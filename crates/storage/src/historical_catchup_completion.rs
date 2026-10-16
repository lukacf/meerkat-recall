//! Durable completion outside the database: no new row can perturb the reconciled
//! inventory. Retained artifacts are immutable, content addressed, and journal bound.
use super::{journal, CatchupMarker, HistoricalCatchupSource};
use crate::durability_io::{io_error, sync_directory};
use crate::erasure_journal::ErasureJournal;
use crate::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
const MAX_RECEIPT: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub format: String,
    pub bindings_sha256: String,
    pub bindings_count: usize,
    pub expected_epoch: String,
    pub journal_identity: String,
    pub expected_ledger_sha256: String,
    pub expected_tables: BTreeSet<String>,
    pub native_rows_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration_id: Option<elephant_types::RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_source_id: Option<elephant_types::RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_disposition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_archive: Option<crate::historical_archive_lifecycle::OperationalArchiveReceipt>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub marker: CatchupMarker,
    pub final_epoch: String,
    pub inventory_receipt_sha256: String,
    pub report_template_sha256: String,
    pub report: serde_json::Value,
    pub baseline: Baseline,
}
pub fn file_sha(path: &Path) -> StorageResult<String> {
    let mut file = File::open(path).map_err(io_error)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 65_536];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn conflict() -> StorageError {
    StorageError::Conflict("historical completion binding differs".into())
}
fn directory(journal: &Path) -> PathBuf {
    journal.join("historical-catchup-completed")
}
fn artifact(journal: &Path, hash: &str, suffix: &str) -> StorageResult<PathBuf> {
    if !journal::valid_digest(hash) {
        return Err(conflict());
    }
    Ok(directory(journal).join(format!("{hash}.{suffix}")))
}
/// Publish a fully fsynced temporary file create-only. An interrupted temporary
/// is never mistaken for a complete content-addressed artifact on retry.
fn retain(
    journal: &Path,
    suffix: &str,
    write: impl FnOnce(&mut File) -> StorageResult<()>,
) -> StorageResult<String> {
    let dir = directory(journal);
    crate::durability_io::private_directory(&dir)?;
    sync_directory(journal)?;
    let temporary = dir.join(format!(".pending-{}", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(io_error)?;
    let result = (|| {
        write(&mut file)?;
        file.sync_all().map_err(io_error)?;
        let hash = file_sha(&temporary)?;
        let destination = artifact(journal, &hash, suffix)?;
        match fs::hard_link(&temporary, &destination) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if file_sha(&destination)? != hash {
                    return Err(conflict());
                }
            }
            Err(e) => return Err(io_error(e)),
        }
        sync_directory(&dir)?;
        Ok(hash)
    })();
    drop(file);
    let _ = fs::remove_file(&temporary);
    result
}
pub fn retain_baseline(
    journal: &Path,
    identity: &str,
    ledger: &Path,
    tables: &BTreeSet<String>,
    sources: &[HistoricalCatchupSource],
    epoch: &str,
    native_sha: &str,
) -> StorageResult<Baseline> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = ledger.as_os_str().to_owned();
        sidecar.push(suffix);
        if Path::new(&sidecar).exists() {
            return Err(conflict());
        }
    }
    let original_sha = file_sha(ledger)?;
    let expected_ledger_sha256 = retain(journal, "sqlite", |output| {
        std::io::copy(&mut File::open(ledger).map_err(io_error)?, output).map_err(io_error)?;
        Ok(())
    })?;
    if expected_ledger_sha256 != original_sha || file_sha(ledger)? != original_sha {
        return Err(conflict());
    }
    let mut seen = BTreeSet::new();
    let bindings_sha256 = retain(journal, "jsonl", |output| {
        for source in sources {
            if !seen.insert(&source.source_id) {
                return Err(conflict());
            }
            let bytes = serde_json::to_vec(source).map_err(io_error)?;
            if bytes.len() > 1024 * 1024 {
                return Err(conflict());
            }
            output.write_all(&bytes).map_err(io_error)?;
            output.write_all(b"\n").map_err(io_error)?;
        }
        Ok(())
    })?;
    Ok(Baseline {
        format: "recall-initial-catchup-bindings-v1".into(),
        bindings_sha256,
        bindings_count: sources.len(),
        expected_epoch: epoch.into(),
        journal_identity: identity.into(),
        expected_ledger_sha256,
        expected_tables: tables.clone(),
        native_rows_sha256: native_sha.into(),
        configuration_id: None,
        receipt_source_id: sources.first().map(|s| s.source_id.clone()),
        operational_disposition: None,
        operational_archive: None,
    })
}
pub fn template_sha(template: &serde_json::Value) -> StorageResult<String> {
    let bytes = serde_json::to_vec(template).map_err(io_error)?;
    if bytes.len() as u64 > MAX_RECEIPT / 2 {
        return Err(conflict());
    }
    Ok(hex::encode(Sha256::digest(bytes)))
}
fn validate_raw_modes(json: &serde_json::Value) -> StorageResult<()> {
    let report = json.get("report").ok_or_else(conflict)?;
    crate::historical_catchup_ledger::archive_from_report(report)?;
    let baseline = json.get("baseline").ok_or_else(conflict)?;
    if crate::historical_catchup_ledger::operational_mode(baseline)?
        != crate::historical_catchup_ledger::operational_mode(report)?
        || baseline != &report["catchup_baseline"]
    {
        return Err(conflict());
    }
    Ok(())
}
fn validate(journal: &ErasureJournal, receipt: &Receipt) -> StorageResult<()> {
    receipt.marker.validate(journal)?;
    let baseline = &receipt.baseline;
    let mut template = receipt.report.clone();
    let fields = template.as_object_mut().ok_or_else(conflict)?;
    fields.remove("catchup_baseline");
    fields.remove("destination_native_rows_sha256");
    fields.remove("operational_archive");
    if template_sha(&template)? != receipt.report_template_sha256 {
        return Err(conflict());
    }
    validate_template(&receipt.marker, &template)?;
    let archive = crate::historical_catchup_ledger::archive_from_report(&receipt.report)?;
    if crate::historical_catchup_ledger::operational_mode(&template)?
        != crate::historical_catchup_ledger::operational_mode(
            &serde_json::to_value(baseline).map_err(io_error)?,
        )?
        || archive != baseline.operational_archive
    {
        return Err(conflict());
    }
    match (
        baseline.operational_disposition.as_deref(),
        baseline.operational_archive.as_ref(),
    ) {
        (None | Some("native"), None) => {}
        (Some("managed_archive_v1"), Some(archive))
            if archive.binding.snapshot_sha256 == receipt.marker.after_snapshot_sha256
                && archive.binding.journal_identity == receipt.marker.journal_identity
                && archive.binding.source_space_id == receipt.marker.source_space_id
                && archive.binding.target_space_id == receipt.marker.space_id
                && !archive.seal.redacted
                && receipt.report["operational_archive"]
                    == serde_json::to_value(archive).map_err(io_error)? => {}
        _ => return Err(conflict()),
    }
    if receipt.final_epoch.is_empty()
        || baseline.expected_epoch != receipt.final_epoch
        || baseline.journal_identity != journal.identity()
        || baseline.format != "recall-initial-catchup-bindings-v1"
        || !journal::valid_digest(&receipt.inventory_receipt_sha256)
        || !journal::valid_digest(&receipt.report_template_sha256)
        || !journal::valid_digest(&baseline.native_rows_sha256)
        || receipt.report["catchup_baseline"] != serde_json::to_value(baseline).map_err(io_error)?
        || receipt.report["destination_native_rows_sha256"] != baseline.native_rows_sha256
    {
        return Err(conflict());
    }
    for (hash, suffix) in [
        (&baseline.expected_ledger_sha256, "sqlite"),
        (&baseline.bindings_sha256, "jsonl"),
    ] {
        if file_sha(&artifact(journal.path(), hash, suffix)?)? != *hash {
            return Err(conflict());
        }
    }
    Ok(())
}
fn path(journal: &ErasureJournal, marker: &CatchupMarker) -> StorageResult<PathBuf> {
    marker.validate(journal)?;
    Ok(directory(journal.path()).join(format!(
        "{}.completed.json",
        marker.authority_binding_sha256
    )))
}
pub(super) fn read(
    journal: &ErasureJournal,
    marker: &CatchupMarker,
) -> StorageResult<Option<Receipt>> {
    let file = match File::open(path(journal, marker)?) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error(e)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_RECEIPT + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_RECEIPT {
        return Err(conflict());
    }
    let json = crate::transfer_batch::strict_json(std::str::from_utf8(&bytes).map_err(io_error)?)?;
    validate_raw_modes(&json)?;
    let receipt: Receipt = serde_json::from_value(json).map_err(io_error)?;
    if receipt.marker != *marker {
        return Err(conflict());
    }
    validate(journal, &receipt)?;
    Ok(Some(receipt))
}
pub(super) fn write(journal: &ErasureJournal, receipt: &Receipt) -> StorageResult<()> {
    validate(journal, receipt)?;
    let bytes = serde_json::to_vec(receipt).map_err(io_error)?;
    if bytes.len() as u64 > MAX_RECEIPT {
        return Err(conflict());
    }
    let hash = retain(journal.path(), "completion", |file| {
        file.write_all(&bytes).map_err(io_error)
    })?;
    let source = artifact(journal.path(), &hash, "completion")?;
    let destination = path(journal, &receipt.marker)?;
    match fs::hard_link(&source, &destination) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if file_sha(&destination)? != hash {
                return Err(conflict());
            }
        }
        Err(e) => return Err(io_error(e)),
    }
    let report_hash = hex::encode(Sha256::digest(
        serde_json::to_vec_pretty(&receipt.report).map_err(io_error)?,
    ));
    let baseline_certificate =
        directory(journal.path()).join(format!("{report_hash}.baseline.json"));
    match fs::hard_link(&source, &baseline_certificate) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if file_sha(&baseline_certificate)? != hash {
                return Err(conflict());
            }
        }
        Err(e) => return Err(io_error(e)),
    }
    sync_directory(&directory(journal.path()))
}
pub fn render(
    mut template: serde_json::Value,
    baseline: &Baseline,
) -> StorageResult<serde_json::Value> {
    let object = template.as_object_mut().ok_or_else(conflict)?;
    object.insert(
        "catchup_baseline".into(),
        serde_json::to_value(baseline).map_err(io_error)?,
    );
    if let Some(archive) = &baseline.operational_archive {
        object.insert(
            "operational_archive".into(),
            serde_json::to_value(archive).map_err(io_error)?,
        );
    }
    object.insert(
        "destination_native_rows_sha256".into(),
        baseline.native_rows_sha256.clone().into(),
    );
    Ok(template)
}
pub(super) fn export(
    journal: &ErasureJournal,
    receipt: &Receipt,
    report: &Path,
    ledger: &Path,
    bindings: &Path,
) -> StorageResult<()> {
    validate(journal, receipt)?;
    for (hash, suffix, destination) in [
        (&receipt.baseline.expected_ledger_sha256, "sqlite", ledger),
        (&receipt.baseline.bindings_sha256, "jsonl", bindings),
    ] {
        let source = artifact(journal.path(), hash, suffix)?;
        publish_copy(&source, destination, hash)?;
    }
    let bytes = serde_json::to_vec_pretty(&receipt.report).map_err(io_error)?;
    // The retained report is small; publish a fsynced temporary create-only.
    publish_bytes(&bytes, report)
}
fn publish_copy(source: &Path, destination: &Path, hash: &str) -> StorageResult<()> {
    publish_output(destination, hash, |file| {
        std::io::copy(&mut File::open(source).map_err(io_error)?, file).map_err(io_error)?;
        Ok(())
    })
}
fn publish_bytes(bytes: &[u8], destination: &Path) -> StorageResult<()> {
    publish_output(destination, &hex::encode(Sha256::digest(bytes)), |file| {
        file.write_all(bytes).map_err(io_error)
    })
}
fn publish_output(
    destination: &Path,
    hash: &str,
    write: impl FnOnce(&mut File) -> StorageResult<()>,
) -> StorageResult<()> {
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(".recall-report-{}", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(io_error)?;
    let result = (|| {
        write(&mut file)?;
        file.sync_all().map_err(io_error)?;
        if file_sha(&temporary)? != hash {
            return Err(conflict());
        }
        match fs::hard_link(&temporary, destination) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if file_sha(destination)? != hash {
                    return Err(conflict());
                }
            }
            Err(e) => return Err(io_error(e)),
        }
        sync_directory(parent)
    })();
    drop(file);
    let _ = fs::remove_file(temporary);
    result
}

pub fn retain_noop_baseline(
    journal: &ErasureJournal,
    input: &super::HistoricalCatchupInput,
    report_path: &Path,
    ledger: &Path,
) -> StorageResult<Baseline> {
    if file_sha(report_path)? != input.original_report_sha256
        || file_sha(ledger)? != input.baseline_ledger_sha256
    {
        return Err(conflict());
    }
    let file = File::open(report_path).map_err(io_error)?;
    let mut bytes = Vec::new();
    file.take(MAX_RECEIPT + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_RECEIPT {
        return Err(conflict());
    }
    let report =
        crate::transfer_batch::strict_json(std::str::from_utf8(&bytes).map_err(io_error)?)?;
    crate::historical_catchup_ledger::archive_from_report(&report)?;
    let baseline: Baseline =
        serde_json::from_value(report["catchup_baseline"].clone()).map_err(io_error)?;
    let retained_ledger_matches = baseline.expected_ledger_sha256 == input.baseline_ledger_sha256;
    if !crate::historical_catchup_ledger::valid_baseline_report(&report)
        || baseline.expected_epoch != input.expected_epoch
        || baseline.journal_identity != input.journal_identity
        || !retained_ledger_matches
        || baseline.native_rows_sha256 != input.baseline_native_rows_sha256
        || baseline.bindings_count != input.sources.len()
    {
        return Err(conflict());
    }
    let retained = retain_baseline(
        journal.path(),
        journal.identity(),
        ledger,
        &baseline.expected_tables,
        &input.sources,
        &input.expected_epoch,
        &input.baseline_native_rows_sha256,
    )?;
    if retained.bindings_sha256 != baseline.bindings_sha256
        || file_sha(report_path)? != input.original_report_sha256
    {
        return Err(conflict());
    }
    Ok(Baseline {
        configuration_id: baseline.configuration_id,
        receipt_source_id: baseline.receipt_source_id,
        operational_disposition: baseline.operational_disposition,
        operational_archive: baseline.operational_archive,
        ..retained
    })
}
/// Empty original authority is admitted only by an earlier storage-produced,
/// exact completed S1 certificate, never by an arbitrary empty caller list.
fn read_baseline_certificate(
    journal: &ErasureJournal,
    report_sha256: &str,
) -> StorageResult<Receipt> {
    if !journal::valid_digest(report_sha256) {
        return Err(conflict());
    }
    let path = directory(journal.path()).join(format!("{report_sha256}.baseline.json"));
    let file = File::open(path).map_err(io_error)?;
    let mut bytes = Vec::new();
    file.take(MAX_RECEIPT + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_RECEIPT {
        return Err(conflict());
    }
    let json = crate::transfer_batch::strict_json(std::str::from_utf8(&bytes).map_err(io_error)?)?;
    validate_raw_modes(&json)?;
    let receipt: Receipt = serde_json::from_value(json).map_err(io_error)?;
    validate(journal, &receipt)?;
    let report = serde_json::to_vec_pretty(&receipt.report).map_err(io_error)?;
    if hex::encode(Sha256::digest(report)) != report_sha256 {
        return Err(conflict());
    }
    Ok(receipt)
}

pub fn verify_empty_baseline(
    journal: &ErasureJournal,
    input: &super::HistoricalCatchupInput,
) -> StorageResult<Baseline> {
    let receipt = read_baseline_certificate(journal, &input.original_report_sha256)?;
    if receipt.baseline.bindings_count != 0
        || receipt.marker.after_snapshot_sha256 != input.before_snapshot_sha256
        || receipt.marker.space_id != input.space_id
        || receipt.marker.source_space_id != input.source_space_id
        || receipt.baseline.expected_epoch != input.expected_epoch
        || receipt.baseline.expected_ledger_sha256 != input.baseline_ledger_sha256
        || receipt.baseline.native_rows_sha256 != input.baseline_native_rows_sha256
    {
        return Err(conflict());
    }
    let config = receipt
        .baseline
        .configuration_id
        .as_ref()
        .ok_or_else(conflict)?;
    if config.table() != elephant_types::TableName::SpaceConfig {
        return Err(conflict());
    }
    if receipt
        .baseline
        .receipt_source_id
        .as_ref()
        .is_none_or(|id| id.table() != elephant_types::TableName::Source)
    {
        return Err(conflict());
    }
    Ok(receipt.baseline)
}

/// Check a storage-produced empty population after destination-owned reindexing.
/// This is historical evidence only: the caller must independently verify live
/// activation and the exact current empty imported population under local custody.
pub fn verify_completed_empty_population(
    journal: &ErasureJournal,
    report_sha256: &str,
    report: &serde_json::Value,
    space: &str,
    configuration_id: &elephant_types::RecordId,
) -> StorageResult<()> {
    let receipt = read_baseline_certificate(journal, report_sha256)?;
    if receipt.report != *report
        || receipt.marker.space_id != space
        || configuration_id.table() != elephant_types::TableName::SpaceConfig
        || receipt.baseline.configuration_id.as_ref() != Some(configuration_id)
        || receipt.baseline.bindings_count != 0
        || receipt.baseline.bindings_sha256 != hex::encode(Sha256::digest([]))
        || receipt
            .baseline
            .receipt_source_id
            .as_ref()
            .is_none_or(|id| id.table() != elephant_types::TableName::Source)
    {
        return Err(conflict());
    }
    // validate() already joins final_epoch to the original baseline epoch and
    // verifies all retained artifacts. No current destination epoch is inferred.
    Ok(())
}

pub(super) fn validate_template(
    marker: &CatchupMarker,
    template: &serde_json::Value,
) -> StorageResult<()> {
    if template["format"] != "recall-historical-delta-report-v2"
        || template["migration_id"] != marker.migration_id
        || template["original_report_sha256"] != marker.original_report_sha256
        || template["source_space_id"] != marker.source_space_id
        || template["target_space_id"] != marker.space_id
        || template["before_snapshot_sha256"] != marker.before_snapshot_sha256
        || template["after_snapshot_sha256"] != marker.after_snapshot_sha256
        || template["source_sha256"] != marker.after_snapshot_sha256
        || template["delta_plan_sha256"] != marker.delta_sha256
        || template["batches"] != marker.batches
        || template["regeneration_plan_sha256"] != marker.regeneration_plan_sha256
        || template["regeneration_steps"] != marker.regeneration_steps
        || template["accept_deletions"] != marker.accept_deletions
        || template["physical_erase_authorized"] != false
        || template["semantic_index_certified"] != false
        || template["state"]
            != if marker.is_exact_noop() {
                "verified_unchanged_history"
            } else {
                "history_reconciled_current_embedding_coverage_pending"
            }
        || template.get("operational_archive").is_some()
        || template.get("catchup_baseline").is_some()
        || template.get("destination_native_rows_sha256").is_some()
    {
        return Err(conflict());
    }
    crate::historical_catchup_ledger::operational_mode(template)?;
    template_sha(template)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    fn fixture(journal: &ErasureJournal, root: &Path) -> Receipt {
        let marker = CatchupMarker {
            version: 1,
            journal_identity: journal.identity().into(),
            migration_id: "a".repeat(64),
            source_space_id: "default".into(),
            space_id: "household".into(),
            before_snapshot_sha256: "b".repeat(64),
            after_snapshot_sha256: "c".repeat(64),
            delta_sha256: "d".repeat(64),
            original_report_sha256: "e".repeat(64),
            baseline_ledger_sha256: "f".repeat(64),
            baseline_native_rows_sha256: "a".repeat(64),
            accept_deletions: true,
            authority_binding_sha256: "b".repeat(64),
            batches: 1,
            regeneration_plan_sha256: "c".repeat(64),
            regeneration_steps: 1,
        };
        let ledger = root.join("closed.sqlite");
        let db = rusqlite::Connection::open(&ledger).expect("ledger");
        db.execute_batch(
            "CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k));",
        )
        .expect("schema");
        db.close().expect("closed");
        let baseline = retain_baseline(
            journal.path(),
            journal.identity(),
            &ledger,
            &BTreeSet::from(["doc".into()]),
            &[],
            "final-epoch",
            &"d".repeat(64),
        )
        .expect("retain exact empty S1");
        let baseline = Baseline {
            receipt_source_id: Some(elephant_types::RecordId::new_v4(
                elephant_types::TableName::Source,
            )),
            configuration_id: Some(elephant_types::RecordId::new_v4(
                elephant_types::TableName::SpaceConfig,
            )),
            ..baseline
        };
        let template = serde_json::json!({"format":"recall-historical-delta-report-v2","migration_id":marker.migration_id,"source_space_id":marker.source_space_id,"target_space_id":marker.space_id,
            "original_report_sha256":marker.original_report_sha256,"before_snapshot_sha256":marker.before_snapshot_sha256,"after_snapshot_sha256":marker.after_snapshot_sha256,"source_sha256":marker.after_snapshot_sha256,
            "delta_plan_sha256":marker.delta_sha256,"batches":marker.batches,"regeneration_plan_sha256":marker.regeneration_plan_sha256,"regeneration_steps":marker.regeneration_steps,"accept_deletions":true,
            "physical_erase_authorized":false,"semantic_index_certified":false,"state":"history_reconciled_current_embedding_coverage_pending"});
        Receipt {
            marker,
            final_epoch: "final-epoch".into(),
            inventory_receipt_sha256: "e".repeat(64),
            report_template_sha256: template_sha(&template).expect("template"),
            report: render(template, &baseline).expect("report"),
            baseline,
        }
    }
    #[test]
    fn completion_modes_reject_null_wrong_type_and_cross_layer_switches() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path().join("journal")).expect("journal");
        let receipt = fixture(&journal, dir.path());
        let original = serde_json::to_value(&receipt).expect("receipt");
        validate_raw_modes(&original).expect("legacy absent native");
        for bad in [
            serde_json::Value::Null,
            serde_json::json!(1),
            serde_json::json!("managed_archive_v1"),
        ] {
            for location in ["report", "baseline"] {
                let mut changed = original.clone();
                changed[location]["operational_disposition"] = bad.clone();
                assert!(validate_raw_modes(&changed).is_err());
            }
            let mut changed = original.clone();
            changed["report"]["catchup_baseline"]["operational_disposition"] = bad;
            assert!(validate_raw_modes(&changed).is_err());
        }
        let mut explicit = original;
        explicit["report"]["operational_disposition"] = "native".into();
        explicit["report"]["catchup_baseline"]["operational_disposition"] = "native".into();
        explicit["baseline"]["operational_disposition"] = "native".into();
        validate_raw_modes(&explicit).expect("explicit native");
        explicit["report"]["operational_archive"] = serde_json::Value::Null;
        assert!(validate_raw_modes(&explicit).is_err());
    }
    #[test]
    fn retained_completion_reopens_exports_exactly_and_rejects_tamper_or_output_collision() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path().join("journal")).expect("journal");
        let receipt = fixture(&journal, dir.path());
        write(&journal, &receipt).expect("durable before ready");
        let reopened = ErasureJournal::open(journal.path()).expect("reopen");
        let recovered = read(&reopened, &receipt.marker)
            .expect("read")
            .expect("completion");
        let report = dir.path().join("report.json");
        let ledger = dir.path().join("next.sqlite");
        let bindings = dir.path().join("sources.jsonl");
        export(&reopened, &recovered, &report, &ledger, &bindings)
            .expect("recover missing CLI report");
        let original = fs::read(&report).expect("bytes");
        export(&reopened, &recovered, &report, &ledger, &bindings)
            .expect("exact idempotent publication");
        assert_eq!(fs::read(&report).expect("bytes"), original);
        fs::write(&report, b"unrelated existing output").expect("collision");
        assert!(export(&reopened, &recovered, &report, &ledger, &bindings).is_err());
        assert_eq!(
            fs::read(&report).expect("unchanged"),
            b"unrelated existing output"
        );
        let source = artifact(
            reopened.path(),
            &receipt.baseline.expected_ledger_sha256,
            "sqlite",
        )
        .expect("path");
        fs::write(source, b"changed ledger").expect("tamper");
        assert!(read(&reopened, &receipt.marker).is_err());
    }
    #[test]
    fn empty_population_requires_exact_completed_certificate_and_bound_original_epoch() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path().join("journal")).expect("journal");
        let receipt = fixture(&journal, dir.path());
        let mut input = super::super::HistoricalCatchupInput {
            version: 1,
            journal_identity: journal.identity().into(),
            migration_id: "f".repeat(64),
            source_space_id: "default".into(),
            space_id: "household".into(),
            before_snapshot_sha256: receipt.marker.after_snapshot_sha256.clone(),
            after_snapshot_sha256: "f".repeat(64),
            delta_sha256: "a".repeat(64),
            original_report_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec_pretty(&receipt.report).expect("bytes"),
            )),
            baseline_ledger_sha256: receipt.baseline.expected_ledger_sha256.clone(),
            baseline_native_rows_sha256: receipt.baseline.native_rows_sha256.clone(),
            accept_deletions: false,
            expected_epoch: receipt.final_epoch.clone(),
            batches: 1,
            regeneration_plan_sha256: "e".repeat(64),
            regeneration_steps: 1,
            sources: vec![],
        };
        assert!(
            verify_empty_baseline(&journal, &input).is_err(),
            "arbitrary empty declaration"
        );
        write(&journal, &receipt).expect("complete predecessor");
        verify_empty_baseline(&journal, &input).expect("certified empty predecessor");
        input.expected_epoch = "stale".into();
        assert!(verify_empty_baseline(&journal, &input).is_err());
    }

    #[test]
    fn empty_certification_requires_exact_retained_report_and_original_configuration() {
        let dir = tempfile::tempdir().expect("directory");
        let journal = ErasureJournal::open(dir.path().join("journal")).expect("journal");
        let receipt = fixture(&journal, dir.path());
        let report_sha = hex::encode(Sha256::digest(
            serde_json::to_vec_pretty(&receipt.report).expect("exact report"),
        ));
        let config = receipt.baseline.configuration_id.as_ref().expect("config");
        let verify = |report: &serde_json::Value, space: &str, id: &elephant_types::RecordId| {
            verify_completed_empty_population(&journal, &report_sha, report, space, id)
        };
        assert!(verify(&receipt.report, "household", config).is_err());
        write(&journal, &receipt).expect("storage completion");
        verify(&receipt.report, "household", config).expect("exact original empty certificate");

        let mut forged = receipt.report.clone();
        forged["catchup_baseline"]["expected_epoch"] = "post-reindex-epoch".into();
        assert!(verify(&forged, "household", config).is_err());
        let forged_hash = hex::encode(Sha256::digest(
            serde_json::to_vec_pretty(&forged).expect("forged report"),
        ));
        assert!(verify_completed_empty_population(
            &journal,
            &forged_hash,
            &forged,
            "household",
            config,
        )
        .is_err());
        assert!(verify(&receipt.report, "foreign", config).is_err());
        assert!(verify(
            &receipt.report,
            "household",
            &elephant_types::RecordId::new_v4(elephant_types::TableName::SpaceConfig),
        )
        .is_err());
        let foreign = ErasureJournal::open(dir.path().join("foreign")).expect("other journal");
        assert!(verify_completed_empty_population(
            &foreign,
            &report_sha,
            &receipt.report,
            "household",
            config,
        )
        .is_err());

        // Even internally retained output cannot declare zero while pointing at
        // nonempty binding bytes. Current DB/activation behavior is tested by
        // the CLI owner; this helper only proves original historical evidence.
        let other = tempfile::tempdir().expect("other ledger directory");
        let mut nonempty = fixture(&journal, other.path());
        nonempty.baseline.bindings_sha256 = retain(journal.path(), "jsonl", |file| {
            file.write_all(b"{}\n").map_err(io_error)
        })
        .expect("nonempty bindings");
        nonempty.report["catchup_baseline"] =
            serde_json::to_value(&nonempty.baseline).expect("baseline");
        nonempty.marker.authority_binding_sha256 = "f".repeat(64);
        let nonempty_sha = hex::encode(Sha256::digest(
            serde_json::to_vec_pretty(&nonempty.report).expect("report"),
        ));
        write(&journal, &nonempty).expect("retained inconsistent count fixture");
        assert!(verify_completed_empty_population(
            &journal,
            &nonempty_sha,
            &nonempty.report,
            "household",
            nonempty.baseline.configuration_id.as_ref().expect("config"),
        )
        .is_err());
    }
}

/// Exact predecessor evidence, retained once per live maintenance capability.
/// The read-only transaction keeps the hash-verified SQLite snapshot fixed.
#[derive(Debug)]
pub(super) struct RetainedOperationalBaseline {
    db: std::sync::Mutex<rusqlite::Connection>,
}

/// A native control image verified against a storage-produced predecessor
/// completion. Private fields prevent caller-provided lineage declarations.
pub struct VerifiedHistoricalControl {
    id: surrealdb::sql::Thing,
    image: surrealdb::sql::Value,
}
impl VerifiedHistoricalControl {
    pub(crate) fn matches_work_payload(
        &self,
        id: &elephant_types::RecordId,
        payload: &str,
    ) -> bool {
        self.id
            == crate::source_publication::control_thing(
                "work_producer_origin",
                &id.id().to_string(),
            )
            && matches!(&self.image, surrealdb::sql::Value::Object(fields)
                if fields.get("payload") == Some(&surrealdb::sql::Value::from(payload)))
    }
}

pub(super) fn retained_operational_baseline(
    journal: &ErasureJournal,
    marker: &CatchupMarker,
    original_epoch: &str,
) -> StorageResult<RetainedOperationalBaseline> {
    // Initial reports have no storage completion certificate. An old snapshot
    // mismatch on initial adoption therefore cannot use this successor bridge.
    let receipt = read_baseline_certificate(journal, &marker.original_report_sha256)?;
    let baseline = &receipt.baseline;
    if receipt.marker.after_snapshot_sha256 != marker.before_snapshot_sha256
        || receipt.marker.source_space_id != marker.source_space_id
        || receipt.marker.space_id != marker.space_id
        || baseline.expected_epoch != original_epoch
        || baseline.expected_ledger_sha256 != marker.baseline_ledger_sha256
        || baseline.native_rows_sha256 != marker.baseline_native_rows_sha256
    {
        return Err(conflict());
    }
    let ledger = artifact(journal.path(), &baseline.expected_ledger_sha256, "sqlite")?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = ledger.as_os_str().to_os_string();
        sidecar.push(suffix);
        if Path::new(&sidecar).exists() {
            return Err(conflict());
        }
    }
    let db = rusqlite::Connection::open_with_flags(
        &ledger,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(io_error)?;
    db.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON; BEGIN;")
        .map_err(io_error)?;
    // Establish the read transaction before the final file identity comparison.
    {
        let mut statement = db
            .prepare("SELECT h FROM expected LIMIT 1")
            .map_err(io_error)?;
        let mut rows = statement.query([]).map_err(io_error)?;
        let _ = rows.next().map_err(io_error)?;
    }
    if file_sha(&ledger)? != baseline.expected_ledger_sha256 {
        return Err(conflict());
    }
    Ok(RetainedOperationalBaseline {
        db: std::sync::Mutex::new(db),
    })
}

impl RetainedOperationalBaseline {
    pub(super) async fn verify_control(
        &self,
        client: &crate::StorageClient,
        table: &str,
        key: &str,
        expected: &crate::historical_adoption::catchup::HistoricalControlImage,
    ) -> StorageResult<VerifiedHistoricalControl> {
        use rusqlite::OptionalExtension as _;
        use surrealdb::sql::Value;
        if !matches!(table, "work_producer_origin" | "outbox_checkpoint")
            || client.embedded_store().is_some()
        {
            return Err(conflict());
        }
        let id = crate::source_publication::control_thing(table, key);
        let crate::historical_adoption::catchup::HistoricalControlImage::Native(image) = expected
        else {
            return Err(conflict());
        };
        let original: Option<String> = {
            let db = self.db.lock().map_err(|_| conflict())?;
            db.query_row(
                "SELECT h FROM expected WHERE t=? AND k=?",
                rusqlite::params![table, id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(io_error)?
        };
        let original = original.ok_or_else(conflict)?;
        let mut response = client.query_bind_checked(
            "SELECT $this AS record,crypto::sha256(type::string($this)) AS sha256 FROM ONLY $id",
            crate::bindings! {"id"=>id.clone()},
        ).await?;
        let raw: surrealdb::Value = response.take(0).map_err(io_error)?;
        let Value::Object(mut fields) = raw.into_inner() else {
            return Err(conflict());
        };
        let Some(actual) = fields.remove("record") else {
            return Err(conflict());
        };
        let Some(Value::Strand(hash)) = fields.remove("sha256") else {
            return Err(conflict());
        };
        if !fields.is_empty() || actual != *image || hash.0 != original {
            return Err(conflict());
        }
        Ok(VerifiedHistoricalControl { id, image: actual })
    }
}
