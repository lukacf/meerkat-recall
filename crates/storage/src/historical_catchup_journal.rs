//! Distinct payload-free catch-up marker. It never replaces erasure authority.
use crate::durability_io::{atomic_write, io_error, sync_directory};
use crate::erasure_journal::ErasureJournal;
use crate::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read as _;

const NAME: &str = "historical-catchup-pending.json";
const MAX_MARKER_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatchupMarker {
    pub version: u32,
    pub journal_identity: String,
    pub migration_id: String,
    pub source_space_id: String,
    pub space_id: String,
    pub before_snapshot_sha256: String,
    pub after_snapshot_sha256: String,
    pub delta_sha256: String,
    pub original_report_sha256: String,
    pub baseline_ledger_sha256: String,
    pub baseline_native_rows_sha256: String,
    pub accept_deletions: bool,
    pub authority_binding_sha256: String,
    pub batches: u64,
    pub regeneration_plan_sha256: String,
    pub regeneration_steps: u64,
}

pub(super) fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl CatchupMarker {
    // Equal row images in a fresh export still need a new source proof and a
    // durable completion. Only this exact declaration takes the no-write path.
    pub(super) fn is_exact_noop(&self) -> bool {
        self.batches == 0
            && self.regeneration_steps == 0
            && self.before_snapshot_sha256 == self.after_snapshot_sha256
    }

    pub(super) fn validate(&self, journal: &ErasureJournal) -> StorageResult<()> {
        if self.version != 1
            || self.journal_identity != journal.identity()
            || self.source_space_id.is_empty()
            || self.space_id.is_empty()
            || self.space_id.trim() != self.space_id
            || self.space_id.len() > 1024
            || self.batches > 1_000_000
            || self.regeneration_steps > 200_000
            || (self.batches > 0 && self.regeneration_steps == 0)
            || [
                &self.migration_id,
                &self.before_snapshot_sha256,
                &self.after_snapshot_sha256,
                &self.delta_sha256,
                &self.original_report_sha256,
                &self.baseline_ledger_sha256,
                &self.baseline_native_rows_sha256,
                &self.authority_binding_sha256,
                &self.regeneration_plan_sha256,
            ]
            .iter()
            .any(|value| !valid_digest(value))
        {
            return Err(StorageError::DataIntegrityError(
                "invalid historical catch-up marker".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn read(journal: &ErasureJournal) -> StorageResult<Option<CatchupMarker>> {
    let file = match fs::File::open(journal.path().join(NAME)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_MARKER_BYTES {
        return Err(io_error("catch-up marker byte bound"));
    }
    let marker: CatchupMarker = serde_json::from_slice(&bytes).map_err(io_error)?;
    marker.validate(journal)?;
    Ok(Some(marker))
}

pub(super) fn begin(journal: &ErasureJournal, marker: &CatchupMarker) -> StorageResult<()> {
    marker.validate(journal)?;
    if marker.is_exact_noop() {
        return Err(StorageError::Conflict(
            "verified no-op must not create a pending marker".into(),
        ));
    }
    let _writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
        &journal.path().join("historical-catchup-writer"),
    )?;
    if read(journal)?.is_some()
        || journal.transfer_marker()?.is_some()
        || journal.restore_marker()?.is_some()
    {
        return Err(StorageError::Conflict(
            "existing maintenance must be reconciled before catch-up".into(),
        ));
    }
    atomic_write(
        &journal.path().join(NAME),
        &serde_json::to_vec(marker).map_err(io_error)?,
    )
}

pub(super) fn finish(journal: &ErasureJournal, expected: &CatchupMarker) -> StorageResult<()> {
    let _writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
        &journal.path().join("historical-catchup-writer"),
    )?;
    if read(journal)?.as_ref() != Some(expected) {
        return Err(StorageError::Conflict(
            "historical catch-up marker changed".into(),
        ));
    }
    fs::remove_file(journal.path().join(NAME)).map_err(io_error)?;
    sync_directory(journal.path())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    fn marker(journal: &ErasureJournal) -> CatchupMarker {
        CatchupMarker {
            version: 1,
            journal_identity: journal.identity().into(),
            migration_id: "a".repeat(64),
            source_space_id: "default".into(),
            space_id: "space".into(),
            before_snapshot_sha256: "b".repeat(64),
            after_snapshot_sha256: "c".repeat(64),
            delta_sha256: "d".repeat(64),
            original_report_sha256: "a".repeat(64),
            baseline_ledger_sha256: "b".repeat(64),
            baseline_native_rows_sha256: "c".repeat(64),
            accept_deletions: true,
            authority_binding_sha256: "e".repeat(64),
            batches: 1,
            regeneration_plan_sha256: "f".repeat(64),
            regeneration_steps: 2,
        }
    }
    #[test]
    fn marker_survives_reopen_and_keeps_erasure_intents() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path()).expect("journal");
        journal
            .record_intent(
                "space",
                &elephant_types::RecordId::new_v4(elephant_types::TableName::Source),
            )
            .expect("original erase");
        let intents = journal.intents().expect("intents");
        let expected = marker(&journal);
        begin(&journal, &expected).expect("marker");
        let reopened = ErasureJournal::open(dir.path()).expect("reopen");
        assert_eq!(read(&reopened).expect("read"), Some(expected.clone()));
        assert!(begin(&reopened, &expected).is_err());
        let mut foreign = expected.clone();
        foreign.after_snapshot_sha256 = "f".repeat(64);
        assert!(finish(&reopened, &foreign).is_err());
        assert_eq!(journal.intents().expect("intents"), intents);
        finish(&reopened, &expected).expect("original completion");
        assert!(read(&journal).expect("read").is_none());
        assert_eq!(journal.intents().expect("intents"), intents);
    }
    #[test]
    fn exact_noop_requires_same_snapshot_and_never_creates_marker() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path()).expect("journal");
        let mut expected = marker(&journal);
        expected.batches = 0;
        expected.validate(&journal).expect("fresh zero-row cut");
        assert!(!expected.is_exact_noop());
        begin(&journal, &expected).expect("fresh cut must retain pending custody");
        finish(&journal, &expected).expect("fresh cut completion");
        expected.regeneration_steps = 0;
        expected
            .validate(&journal)
            .expect("certified empty fresh cut shape");
        assert!(!expected.is_exact_noop());
        begin(&journal, &expected).expect("empty fresh cut still has custody");
        finish(&journal, &expected).expect("empty fresh cut completion");
        expected.after_snapshot_sha256 = expected.before_snapshot_sha256.clone();
        assert!(expected.is_exact_noop());
        assert!(begin(&journal, &expected).is_err());
        assert!(read(&journal).expect("read").is_none());
        expected.regeneration_steps = 1;
        assert!(
            !expected.is_exact_noop(),
            "source regeneration is never a no-op"
        );
    }

    #[test]
    fn corrupt_oversized_and_foreign_marker_fail_closed() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path()).expect("journal");
        for bytes in [
            b"{".to_vec(),
            vec![b' '; usize::try_from(MAX_MARKER_BYTES + 1).expect("bound")],
        ] {
            fs::write(dir.path().join(NAME), bytes).expect("fixture");
            assert!(read(&journal).is_err());
        }
        let mut wrong = marker(&journal);
        wrong.journal_identity = "foreign".into();
        fs::write(
            dir.path().join(NAME),
            serde_json::to_vec(&wrong).expect("encode"),
        )
        .expect("fixture");
        assert!(read(&journal).is_err());
    }
}
