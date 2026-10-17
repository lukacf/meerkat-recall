//! Payload-free erasure intents and restore markers, stored outside database snapshots.
use crate::durability_io::{atomic_write, io_error, private_directory, sync_directory};
use crate::{StorageError, StorageResult};
use chrono::{DateTime, Utc};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[path = "erasure_reference_journal.rs"]
mod references;

#[path = "erasure_history_journal.rs"]
mod history;

#[path = "erasure_operational_archives.rs"]
mod operational_archives;
pub use operational_archives::{
    ArchiveGenerationState, ArchiveRedactionProof, ArchiveRegistration, ArchiveRegistryEntry,
    ArchiveSealEvidence,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Durable authority to purge one physical source incarnation, without its payload.
pub struct ErasureIntent {
    /// Opaque identity of the accepted erasure intent.
    pub intent_id: String,
    /// Named space owning the source incarnation.
    pub space_id: String,
    /// Physical source incarnation; deliberate recreation must use another ID.
    pub source_id: RecordId,
    /// Time at which the intent was recorded.
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestoreMarker {
    pub snapshot_id: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransferMarker {
    pub source_sha256: String,
    pub started_at: DateTime<Utc>,
}

/// Cloneable journal handle. No source text, external IDs, credentials or provider errors.
#[derive(Debug, Clone)]
pub struct ErasureJournal {
    root: PathBuf,
    identity: String,
    reference_key: references::ReferenceKey,
    writer: Arc<Mutex<()>>,
    pub(crate) backups: Arc<tokio::sync::Mutex<()>>,
}

impl ErasureJournal {
    /// Open or initialize the independent journal, rejecting corrupt committed metadata.
    pub fn open(root: impl AsRef<Path>) -> StorageResult<Self> {
        let root = root.as_ref();
        private_directory(root)?;
        for name in ["erasures", "backups"] {
            private_directory(&root.join(name))?;
        }
        let (identity, reference_key) = {
            let _initialization = crate::exclusive_store_lock::StoreWriterLock::acquire(
                &root.join("journal-initialization"),
            )?;
            references::open_key(root)?
        };
        sync_directory(root)?;
        let parent = root
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)?;
        let journal = Self {
            root: root.canonicalize().map_err(io_error)?,
            identity,
            reference_key,
            writer: Arc::new(Mutex::new(())),
            backups: Arc::new(tokio::sync::Mutex::new(())),
        };
        // A malformed or truncated committed intent must never be silently ignored.
        journal.intents()?;
        journal.restore_marker()?;
        journal.transfer_marker()?;
        journal.validate_reference_memberships()?;
        journal.validate_history_commitments()?;
        journal.validate_operational_archives()?;
        crate::source_history_restore::validate_restored_history(&journal)?;
        Ok(journal)
    }

    /// Canonical directory that must be retained independently of restored data.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Immutable UUID bound into live stores and backup manifests.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Call after a durable Erasing fence accepts the source precondition, before live purge.
    ///
    /// A read-only precheck is insufficient: later source CAS failure cannot retract an intent.
    /// Success means the intent and directory entry have been synced to disk.
    pub fn record_intent(
        &self,
        space_id: &str,
        source_id: &RecordId,
    ) -> StorageResult<ErasureIntent> {
        if space_id.is_empty() || source_id.table() != TableName::Source {
            return Err(StorageError::DataIntegrityError(
                "invalid erasure intent identity".into(),
            ));
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        // Repeating an erase is idempotent and does not grow the journal.
        if let Some(intent) = self
            .intents()?
            .into_iter()
            .find(|i| i.space_id == space_id && i.source_id == *source_id)
        {
            return Ok(intent);
        }
        let intent = ErasureIntent {
            intent_id: uuid::Uuid::new_v4().to_string(),
            space_id: space_id.into(),
            source_id: source_id.clone(),
            recorded_at: Utc::now(),
        };
        let bytes = serde_json::to_vec(&intent).map_err(io_error)?;
        atomic_write(
            &self
                .root
                .join("erasures")
                .join(format!("{}.json", intent.intent_id)),
            &bytes,
        )?;
        Ok(intent)
    }

    /// Load all committed intents, rejecting malformed or mismatched entries.
    pub fn intents(&self) -> StorageResult<Vec<ErasureIntent>> {
        let mut intents = Vec::new();
        for entry in fs::read_dir(self.root.join("erasures")).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".recall-write-") {
                continue;
            }
            if !entry.file_type().map_err(io_error)?.is_file() {
                return Err(io_error("unexpected erasure journal entry"));
            }
            let intent: ErasureIntent =
                serde_json::from_slice(&fs::read(entry.path()).map_err(io_error)?)
                    .map_err(io_error)?;
            if name != format!("{}.json", intent.intent_id).as_str()
                || uuid::Uuid::parse_str(&intent.intent_id).is_err()
                || intent.space_id.is_empty()
                || intent.source_id.table() != TableName::Source
            {
                return Err(io_error("erasure journal identity mismatch"));
            }
            intents.push(intent);
        }
        intents.sort_by(|a, b| a.intent_id.cmp(&b.intent_id));
        Ok(intents)
    }

    pub(crate) fn restore_marker(&self) -> StorageResult<Option<RestoreMarker>> {
        match fs::read(self.root.join("restore-pending.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(io_error),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error(error)),
        }
    }

    pub(crate) fn begin_restore(&self, marker: &RestoreMarker) -> StorageResult<()> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        if let Some(original) = self.restore_marker()? {
            if original != *marker {
                return Err(StorageError::Conflict(
                    "restore must resume its original snapshot and digest".into(),
                ));
            }
            return Ok(());
        }
        if self.transfer_marker()?.is_some() {
            return Err(StorageError::Conflict(
                "incomplete transfer must be resolved before restore".into(),
            ));
        }
        atomic_write(
            &self.root.join("restore-pending.json"),
            &serde_json::to_vec(marker).map_err(io_error)?,
        )
    }

    /// Completion belongs to the caller's original marker.
    ///
    /// Full destructive
    /// restore custody remains with the existing store owner and maintenance
    /// guard; observing an identical marker alone never grants that custody.
    pub(crate) fn finish_restore_checked(&self, expected: &RestoreMarker) -> StorageResult<()> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        if self.restore_marker()?.as_ref() != Some(expected) {
            return Err(StorageError::Conflict(
                "restore completion no longer owns its original marker".into(),
            ));
        }
        fs::remove_file(self.root.join("restore-pending.json")).map_err(io_error)?;
        sync_directory(&self.root)
    }

    pub(crate) fn transfer_marker(&self) -> StorageResult<Option<TransferMarker>> {
        match fs::read(self.root.join("transfer-pending.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(io_error),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error(error)),
        }
    }

    pub(crate) fn begin_transfer(&self, source_sha256: &str) -> StorageResult<()> {
        if self.transfer_marker()?.is_some() || self.restore_marker()?.is_some() {
            return Err(StorageError::Conflict(
                "incomplete target maintenance must be resolved explicitly".into(),
            ));
        }
        let marker = TransferMarker {
            source_sha256: source_sha256.into(),
            started_at: Utc::now(),
        };
        atomic_write(
            &self.root.join("transfer-pending.json"),
            &serde_json::to_vec(&marker).map_err(io_error)?,
        )
    }

    pub(crate) fn finish_transfer(&self) -> StorageResult<()> {
        fs::remove_file(self.root.join("transfer-pending.json")).map_err(io_error)?;
        sync_directory(&self.root)
    }

    pub(crate) fn backups_path(&self) -> PathBuf {
        self.root.join("backups")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn journal_survives_reopen_and_is_payload_free() {
        let directory = tempfile::tempdir().unwrap();
        let source = RecordId::new_v4(TableName::Source);
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let first = journal.record_intent("household", &source).unwrap();
        assert_eq!(first, journal.record_intent("household", &source).unwrap());
        drop(journal);
        let reopened = ErasureJournal::open(directory.path()).unwrap();
        assert_eq!(reopened.intents().unwrap(), vec![first]);
        let bytes = serde_json::to_value(&reopened.intents().unwrap()[0]).unwrap();
        assert_eq!(bytes.as_object().unwrap().len(), 4);
        assert!(bytes.get("payload").is_none());
    }

    fn restore_marker_fixture(byte: u8) -> RestoreMarker {
        RestoreMarker {
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            sha256: hex::encode([byte; 32]),
        }
    }

    #[test]
    fn restore_marker_cas_preserves_original_snapshot_and_digest_across_handles() {
        let directory = tempfile::tempdir().unwrap();
        let first = ErasureJournal::open(directory.path()).unwrap();
        let second = ErasureJournal::open(directory.path()).unwrap();
        let original = restore_marker_fixture(1);
        first.begin_restore(&original).unwrap();
        let bytes = fs::read(directory.path().join("restore-pending.json")).unwrap();
        second.begin_restore(&original).unwrap();
        let mut another = original.clone();
        another.sha256 = hex::encode([2; 32]);
        assert!(second.begin_restore(&another).is_err());
        another = original.clone();
        another.snapshot_id = uuid::Uuid::new_v4().to_string();
        assert!(second.begin_restore(&another).is_err());
        assert_eq!(
            fs::read(directory.path().join("restore-pending.json")).unwrap(),
            bytes
        );
        assert_eq!(second.restore_marker().unwrap(), Some(original));
    }

    #[test]
    fn checked_restore_completion_cannot_remove_a_different_or_missing_barrier() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = restore_marker_fixture(1);
        let another = restore_marker_fixture(2);
        journal.begin_restore(&original).unwrap();
        assert!(journal.finish_restore_checked(&another).is_err());
        assert_eq!(journal.restore_marker().unwrap(), Some(original.clone()));
        journal.finish_restore_checked(&original).unwrap();
        assert!(journal.finish_restore_checked(&original).is_err());
        journal.begin_restore(&another).unwrap();
        assert!(journal.finish_restore_checked(&original).is_err());
        assert_eq!(journal.restore_marker().unwrap(), Some(another));
    }

    #[test]
    fn independently_opened_restore_writers_cannot_replace_the_winning_barrier() {
        let directory = tempfile::tempdir().unwrap();
        let first = ErasureJournal::open(directory.path()).unwrap();
        let second = ErasureJournal::open(directory.path()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a = restore_marker_fixture(1);
        let b = restore_marker_fixture(2);
        let original_a = a.clone();
        let original_b = b.clone();
        let gate = barrier.clone();
        let one = std::thread::spawn(move || {
            gate.wait();
            first.begin_restore(&a)
        });
        let two = std::thread::spawn(move || {
            barrier.wait();
            second.begin_restore(&b)
        });
        let one = one.join().unwrap();
        let two = two.join().unwrap();
        assert_ne!(one.is_ok(), two.is_ok());
        let journal = ErasureJournal::open(directory.path()).unwrap();
        assert_eq!(
            journal.restore_marker().unwrap(),
            Some(if one.is_ok() { original_a } else { original_b })
        );
    }

    #[test]
    fn restore_marker_changes_share_the_original_journal_writer_lock() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let marker = restore_marker_fixture(1);
        let lock = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &directory.path().join("reference-membership"),
        )
        .unwrap();
        assert!(journal.begin_restore(&marker).is_err());
        assert!(journal.restore_marker().unwrap().is_none());
        drop(lock);
        journal.begin_restore(&marker).unwrap();
        let lock = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &directory.path().join("reference-membership"),
        )
        .unwrap();
        assert!(journal.finish_restore_checked(&marker).is_err());
        assert_eq!(journal.restore_marker().unwrap(), Some(marker.clone()));
        drop(lock);
        journal.finish_restore_checked(&marker).unwrap();
    }

    #[test]
    fn damaged_committed_intent_blocks_open() {
        let directory = tempfile::tempdir().unwrap();
        ErasureJournal::open(directory.path()).unwrap();
        fs::write(
            directory.path().join("erasures/broken.json"),
            b"{\"intent_id\":",
        )
        .unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
    }
}
