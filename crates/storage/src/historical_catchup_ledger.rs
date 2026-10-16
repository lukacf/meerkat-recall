//! Source-derived native inventory: immutable S0 ledger plus verified DB receipts.
//!
//! A fresh private SQLite copy is rebuilt on every resume. DB commit followed by
//! process death therefore never requires guessing whether a CLI update happened.
use crate::canonical_commit::CanonicalRepository;
use crate::client::{CompletedHistoricalCatchup, PreparedHistoricalCatchupAuthority};
use crate::historical_adoption::HistoricalS1SourceClosure;
use crate::historical_native_effects::VerifiedHistoricalNativeEffects;
use crate::{StorageError, StorageResult};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use surrealdb::sql::Value;

fn invalid() -> StorageError {
    StorageError::Conflict("historical catch-up inventory differs".into())
}
fn io(error: impl std::fmt::Display) -> StorageError {
    StorageError::DataIntegrityError(format!("historical inventory: {error}"))
}
fn file_sha(path: &Path) -> StorageResult<String> {
    let mut file = std::fs::File::open(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut bytes = vec![0_u8; 64 * 1024];
    loop {
        let n = file.read(&mut bytes).map_err(io)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn string<'a>(json: &'a serde_json::Value, key: &str) -> StorageResult<&'a str> {
    json.get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(invalid)
}
/// Legacy absent mode means native; an explicit malformed value never does.
pub(crate) fn operational_mode(value: &serde_json::Value) -> StorageResult<&str> {
    if !value.is_object() {
        return Err(invalid());
    }
    match value.get("operational_disposition") {
        None => Ok("native"),
        Some(serde_json::Value::String(mode))
            if mode == "native" || mode == "managed_archive_v1" =>
        {
            Ok(mode)
        }
        _ => Err(invalid()),
    }
}
pub(crate) fn archive_from_report(
    report: &serde_json::Value,
) -> StorageResult<Option<crate::historical_archive_lifecycle::OperationalArchiveReceipt>> {
    let baseline = report.get("catchup_baseline").ok_or_else(invalid)?;
    let mode = operational_mode(report)?;
    if operational_mode(baseline)? != mode {
        return Err(invalid());
    }
    match mode {
        "native"
            if report.get("operational_archive").is_none()
                && baseline.get("operational_archive").is_none() =>
        {
            Ok(None)
        }
        "managed_archive_v1"
            if report.get("operational_archive").is_some()
                && report.get("operational_archive") == baseline.get("operational_archive") =>
        {
            Ok(Some(
                serde_json::from_value(report["operational_archive"].clone()).map_err(io)?,
            ))
        }
        _ => Err(invalid()),
    }
}
/// Both initial transfer and a storage-completed successor carry the same closed baseline.
#[must_use]
pub fn valid_baseline_report(report: &serde_json::Value) -> bool {
    (report["format"] == "recall-legacy-transfer-v2"
        && report["state"] == "initial_history_reconciled_vector_coverage_pending")
        || (report["format"] == "recall-historical-delta-report-v2"
            && matches!(
                report["state"].as_str(),
                Some(
                    "history_reconciled_current_embedding_coverage_pending"
                        | "verified_unchanged_history"
                )
            ))
}

fn copy_closed_baseline(
    report_path: &Path,
    baseline_path: &Path,
    private_copy: &Path,
    report_sha: &str,
    baseline_sha: &str,
) -> StorageResult<()> {
    // Refuse mutable SQLite sidecars. The original report binds a closed,
    // fsynced main file; neither WAL nor a live backup is substituted here.
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = baseline_path.as_os_str().to_os_string();
        sidecar.push(suffix);
        if Path::new(&sidecar).exists() {
            return Err(invalid());
        }
    }
    let mut source = std::fs::File::open(baseline_path).map_err(io)?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(private_copy)
        .map_err(io)?;
    std::io::copy(&mut source, &mut output).map_err(io)?;
    output.sync_all().map_err(io)?;
    drop(output);
    if file_sha(baseline_path)? != baseline_sha
        || file_sha(private_copy)? != baseline_sha
        || file_sha(report_path)? != report_sha
    {
        return Err(invalid());
    }
    Ok(())
}

/// Owned inventory. There is no API to replace it with a destination capture or
/// accept caller-authored changes; only storage-produced opaque effects apply.
pub struct HistoricalCatchupLedger {
    db: Connection,
    path: std::path::PathBuf,
    tables: BTreeSet<String>,
    binding: String,
    report_sha: String,
    baseline_sha: String,
    batch_count: u64,
    regeneration_count: u64,
    next_batch: u64,
    next_regeneration: u64,
    archive_applied: bool,
    original_archive: Option<crate::historical_archive_lifecycle::OperationalArchiveReceipt>,
}
struct CompletionInventory {
    hash: Sha256,
    native_rows_sha256: String,
    current_archive: Option<crate::historical_archive_lifecycle::OperationalArchiveReceipt>,
    journal_identity: String,
    journal_path: std::path::PathBuf,
}

impl HistoricalCatchupLedger {
    /// Verify the predecessor report and ledger against custody, then create a private copy.
    /// The original closed ledger remains immutable across retries.
    pub fn open(
        authority: &PreparedHistoricalCatchupAuthority,
        report_path: &Path,
        baseline_path: &Path,
        private_copy: &Path,
    ) -> StorageResult<Self> {
        authority.check_custody()?;
        let (binding, report_sha, baseline_sha, native_sha, journal, batches, regeneration) =
            authority.ledger_binding();
        if file_sha(report_path)? != report_sha || file_sha(baseline_path)? != baseline_sha {
            return Err(invalid());
        }
        let file = std::fs::File::open(report_path).map_err(io)?;
        if file.metadata().map_err(io)?.len() > 1024 * 1024 {
            return Err(invalid());
        }
        let mut raw = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut raw)
            .map_err(io)?;
        if raw.len() > 1024 * 1024 {
            return Err(invalid());
        }
        let report = crate::transfer_batch::strict_json(std::str::from_utf8(&raw).map_err(io)?)?;
        let baseline = report.get("catchup_baseline").ok_or_else(invalid)?;
        if string(baseline, "format")? != "recall-initial-catchup-bindings-v1"
            || !valid_baseline_report(&report)
            || string(baseline, "expected_ledger_sha256")? != baseline_sha
            || string(&report, "destination_native_rows_sha256")? != native_sha
            || string(baseline, "native_rows_sha256")? != native_sha
            || string(baseline, "journal_identity")? != journal
        {
            return Err(invalid());
        }
        let original_archive = archive_from_report(&report)?;
        if let Some(archive) = &original_archive {
            if archive.binding.snapshot_sha256 != report["source_sha256"]
                || archive.binding.journal_identity != journal
                || archive.seal.redacted
            {
                return Err(invalid());
            }
        }
        let entries = baseline
            .get("expected_tables")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(invalid)?;
        let mut tables = BTreeSet::new();
        for table in entries {
            let table = table.as_str().ok_or_else(invalid)?;
            if table.is_empty()
                || !table
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || !tables.insert(table.to_owned())
            {
                return Err(invalid());
            }
        }
        copy_closed_baseline(
            report_path,
            baseline_path,
            private_copy,
            report_sha,
            baseline_sha,
        )?;
        let db = Connection::open_with_flags(
            private_copy,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(io)?;
        db.execute_batch(
            "PRAGMA trusted_schema=OFF; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;",
        )
        .map_err(io)?;
        let unexpected: u64 = db
            .query_row(
                "SELECT count(*) FROM expected WHERE t NOT IN (SELECT value FROM json_each(?))",
                [serde_json::to_string(&tables).map_err(io)?],
                |r| r.get(0),
            )
            .map_err(io)?;
        if unexpected != 0 {
            return Err(invalid());
        }
        Ok(Self {
            db,
            path: private_copy.into(),
            tables,
            binding: binding.into(),
            report_sha: report_sha.into(),
            baseline_sha: baseline_sha.into(),
            batch_count: batches,
            regeneration_count: regeneration,
            next_batch: 0,
            next_regeneration: 0,
            archive_applied: false,
            original_archive,
        })
    }
    #[cfg(test)]
    pub(crate) fn fixture_open_retained_baseline(
        path: &Path,
        binding: &str,
    ) -> StorageResult<Self> {
        let db = Connection::open(path).map_err(io)?;
        let tables = {
            let mut statement = db.prepare("SELECT DISTINCT t FROM expected").map_err(io)?;
            let tables = statement
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(io)?
                .collect::<Result<BTreeSet<_>, _>>()
                .map_err(io)?;
            tables
        };
        Ok(Self {
            db,
            path: path.into(),
            tables,
            binding: binding.into(),
            report_sha: "a".repeat(64),
            baseline_sha: file_sha(path)?,
            batch_count: 2,
            regeneration_count: 1,
            next_batch: 0,
            next_regeneration: 0,
            archive_applied: false,
            original_archive: None,
        })
    }
    /// Exact immutable predecessor receipt selected when this ledger was opened.
    /// It grants no mutation authority by itself.
    pub const fn original_archive(
        &self,
    ) -> Option<&crate::historical_archive_lifecycle::OperationalArchiveReceipt> {
        self.original_archive.as_ref()
    }
    pub(crate) fn archive_before_images(
        &self,
        authority: &PreparedHistoricalCatchupAuthority,
        ids: &[(String, surrealdb::sql::Thing)],
    ) -> StorageResult<Vec<Option<String>>> {
        authority.check_custody()?;
        if authority.ledger_binding().0 != self.binding
            || self.original_archive.is_none()
            || self.archive_applied
            || self.next_batch != self.batch_count
            || self.next_regeneration != 0
        {
            return Err(invalid());
        }
        ids.iter()
            .map(|(table, id)| {
                self.db
                    .query_row(
                        "SELECT h FROM expected WHERE t=? AND k=?",
                        params![table, id.to_string()],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(io)
            })
            .collect()
    }
    /// Apply one storage-verified receipt in order using exact before-image comparisons.
    /// Absent after-images remove only their bound inventory entries.
    pub fn apply(&mut self, verified: VerifiedHistoricalNativeEffects) -> StorageResult<()> {
        let VerifiedHistoricalNativeEffects {
            binding: b,
            effects,
        } = verified;
        if b.authority_binding_sha256 != self.binding {
            return Err(invalid());
        }
        match b.phase.as_str() {
            "batch"
                if self.next_regeneration == 0
                    && !self.archive_applied
                    && b.ordinal == self.next_batch
                    && b.ordinal < self.batch_count => {}
            "archive"
                if self.original_archive.is_some()
                    && !self.archive_applied
                    && self.next_batch == self.batch_count
                    && self.next_regeneration == 0
                    && b.ordinal == 0 => {}
            "regeneration"
                if self.archive_applied == self.original_archive.is_some()
                    && self.next_batch == self.batch_count
                    && b.ordinal == self.next_regeneration
                    && b.ordinal < self.regeneration_count => {}
            _ => return Err(invalid()),
        }
        let tx = self.db.transaction().map_err(io)?;
        let mut seen = BTreeSet::new();
        let mut new_tables = BTreeSet::new();
        for effect in &effects {
            if !seen.insert((&effect.table, &effect.id))
                || (effect.before_sha256.is_none() && effect.after_sha256.is_none())
            {
                return Err(invalid());
            }
            let actual: Option<String> = tx
                .query_row(
                    "SELECT h FROM expected WHERE t=? AND k=?",
                    params![effect.table, effect.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(io)?;
            if actual != effect.before_sha256 {
                return Err(invalid());
            }
            if let Some(after) = &effect.after_sha256 {
                if actual.is_some() {
                    if tx
                        .execute(
                            "UPDATE expected SET h=? WHERE t=? AND k=?",
                            params![after, effect.table, effect.id],
                        )
                        .map_err(io)?
                        != 1
                    {
                        return Err(invalid());
                    }
                } else {
                    tx.execute("INSERT INTO expected(t,k,h,seq) VALUES(?,?,?,(SELECT coalesce(max(seq)+1,0) FROM expected WHERE t=?))",params![effect.table,effect.id,after,effect.table]).map_err(io)?;
                }
            } else if tx
                .execute(
                    "DELETE FROM expected WHERE t=? AND k=?",
                    params![effect.table, effect.id],
                )
                .map_err(io)?
                != 1
            {
                return Err(invalid());
            }
            new_tables.insert(effect.table.clone());
        }
        tx.commit().map_err(io)?;
        self.tables.extend(new_tables);
        if b.phase == "batch" {
            self.next_batch += 1;
        } else if b.phase == "archive" {
            self.archive_applied = true;
        } else {
            self.next_regeneration += 1;
        }
        Ok(())
    }
    /// Check the exact ledger after committed regeneration effects have been
    /// replayed, before a fresh zero-row cut can write its next source proof.
    pub(crate) async fn verify_regeneration_prefix(
        &mut self,
        sealed: &crate::client::SealedHistoricalCatchupAuthority,
        epoch: &str,
        committed: u64,
    ) -> StorageResult<()> {
        sealed.check_custody()?;
        let (binding, report, baseline, _, _, batches, groups) = sealed.ledger_binding();
        if binding != self.binding
            || report != self.report_sha
            || baseline != self.baseline_sha
            || self.archive_applied != self.original_archive.is_some()
            || self.next_batch != batches
            || self.next_regeneration != committed
            || committed > groups
        {
            return Err(invalid());
        }
        if batches == 0 {
            self.verify_inventory(sealed.client()?, sealed.space_id(), epoch)
                .await?;
            sealed.check_custody()?;
        }
        Ok(())
    }

    async fn verify_inventory(
        &mut self,
        client: &crate::StorageClient,
        space: &str,
        epoch: &str,
    ) -> StorageResult<Sha256> {
        if client.embedded_store().is_some() {
            return Err(invalid());
        }
        if CanonicalRepository::new(client.clone())
            .observe_epoch(space)
            .await?
            .as_deref()
            != Some(epoch)
        {
            return Err(invalid());
        }
        if client
            .list_tables()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>()
            != self.tables
        {
            return Err(invalid());
        }
        let mut hash = Sha256::new();
        for table in &self.tables {
            Self::verify_table_inventory(&mut self.db, client, table, &mut hash).await?;
        }
        if client
            .list_tables()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>()
            != self.tables
            || CanonicalRepository::new(client.clone())
                .observe_epoch(space)
                .await?
                .as_deref()
                != Some(epoch)
        {
            return Err(invalid());
        }
        client.verify_journal_identity().await?;
        Ok(hash)
    }

    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "The owned catch-up future must remain Send: holding an exclusive borrow across native query awaits requires Connection: Send, while a shared borrow would require unsupported Connection: Sync."
    )]
    async fn verify_table_inventory(
        db: &mut Connection,
        client: &crate::StorageClient,
        table: &str,
        hash: &mut Sha256,
    ) -> StorageResult<()> {
        hash.update(serde_json::to_vec(&serde_json::json!(["table", table])).map_err(io)?);
        hash.update(b"\n");
        let mut cursor: Option<surrealdb::sql::Thing> = None;
        let mut count = 0_u64;
        loop {
            let predicate = if cursor.is_some() {
                " WHERE id > $after"
            } else {
                ""
            };
            let sql = format!("SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM type::table($table){predicate} ORDER BY cursor LIMIT 256");
            let mut response = client
                .query_bind_checked(
                    &sql,
                    BTreeMap::<String, Value>::from([
                        ("table".into(), Value::from(table.to_owned())),
                        (
                            "after".into(),
                            cursor.clone().map_or(Value::None, Value::Thing),
                        ),
                    ]),
                )
                .await?;
            let raw: surrealdb::Value = response.take(0).map_err(io)?;
            let Value::Array(rows) = raw.into_inner() else {
                return Err(invalid());
            };
            if rows.len() > 256 {
                return Err(invalid());
            }
            if rows.is_empty() {
                break;
            }
            for row in rows.0 {
                let Value::Object(mut fields) = row else {
                    return Err(invalid());
                };
                let Some(Value::Thing(next)) = fields.remove("cursor") else {
                    return Err(invalid());
                };
                let Some(Value::Strand(id)) = fields.remove("id") else {
                    return Err(invalid());
                };
                let Some(Value::Strand(sha)) = fields.remove("sha256") else {
                    return Err(invalid());
                };
                if !(fields.is_empty() && next.tb == table && next.to_string() == id.0)
                    || cursor.as_ref().is_some_and(|last| last >= &next)
                {
                    return Err(invalid());
                }
                let expected: String = db
                    .query_row(
                        "SELECT h FROM expected WHERE t=? AND k=?",
                        params![table, id.0],
                        |r| r.get(0),
                    )
                    .map_err(io)?;
                if expected != sha.0 {
                    return Err(invalid());
                }
                hash.update(
                    serde_json::to_vec(&serde_json::json!([table, id.0, sha.0])).map_err(io)?,
                );
                hash.update(b"\n");
                cursor = Some(next);
                count += 1;
            }
        }
        let expected: u64 = db
            .query_row("SELECT count(*) FROM expected WHERE t=?", [table], |r| {
                r.get(0)
            })
            .map_err(io)?;
        if expected != count {
            return Err(invalid());
        }
        Ok(())
    }

    async fn complete_inventory(
        &mut self,
        closure: &HistoricalS1SourceClosure,
        client: &crate::StorageClient,
    ) -> StorageResult<CompletionInventory> {
        let hash = self
            .verify_inventory(client, closure.sealed.space_id(), &closure.final_epoch)
            .await?;
        // This domain is exactly the initial all-table native inventory domain.
        // The completion proof below also includes source closure, separately.
        let native_rows_sha256 = hex::encode(hash.clone().finalize());
        let current_archive =
            crate::historical_archive_lifecycle::current_operational_archive_receipt(
                client,
                closure.sealed.space_id(),
            )
            .await?;
        match (&self.original_archive, &current_archive) {
            (None, None) => {}
            (Some(original), Some(current))
                if current.registration.lineage_id == original.registration.lineage_id
                    && current.binding.journal_identity == original.binding.journal_identity
                    && current.binding.source_space_id == original.binding.source_space_id
                    && current.binding.target_space_id == original.binding.target_space_id
                    && current.binding.snapshot_sha256
                        == closure.sealed.after_snapshot_sha256()
                    && current.binding.parent_generation_sha256.as_ref()
                        == Some(&original.seal.file_sha256)
                    && !current.seal.redacted => {}
            _ => return Err(invalid()),
        }
        let journal = client.erasure_journal()?;
        let journal_identity = journal.identity().to_owned();
        let journal_path = journal.path().to_owned();
        Ok(CompletionInventory {
            hash,
            native_rows_sha256,
            current_archive,
            journal_identity,
            journal_path,
        })
    }

    /// Consume both proofs; compare every actual table and row under the same
    /// maintenance lease. A digest supplied by the CLI is never completion.
    pub async fn complete(
        mut self,
        closure: HistoricalS1SourceClosure,
    ) -> StorageResult<CompletedHistoricalCatchup> {
        closure.sealed.check_custody()?;
        let (binding, report, baseline, _, _, batches, groups) = closure.sealed.ledger_binding();
        if binding != self.binding
            || report != self.report_sha
            || baseline != self.baseline_sha
            || self.archive_applied != self.original_archive.is_some()
            || self.next_batch != batches
            || self.next_regeneration != groups
        {
            return Err(invalid());
        }
        let CompletionInventory {
            mut hash,
            native_rows_sha256,
            current_archive,
            journal_identity,
            journal_path,
        } = self
            .complete_inventory(&closure, closure.sealed.client()?)
            .await?;
        let final_epoch = closure.final_epoch.clone();
        self.db.close().map_err(|(_, error)| io(error))?;
        std::fs::File::open(&self.path)
            .map_err(io)?
            .sync_all()
            .map_err(io)?;
        let mut baseline = crate::client::historical_catchup::completion::retain_baseline(
            &journal_path,
            &journal_identity,
            &self.path,
            &self.tables,
            &closure.source_bindings,
            &final_epoch,
            &native_rows_sha256,
        )?;
        if let Some(archive) = current_archive {
            hash.update(b"recall-catchup-operational-archive-v1\0");
            hash.update(serde_json::to_vec(&archive).map_err(io)?);
            baseline.operational_disposition = Some("managed_archive_v1".into());
            baseline.operational_archive = Some(archive);
        }
        baseline.configuration_id = Some(closure.configuration_id.clone());
        baseline.receipt_source_id = Some(closure.sealed.receipt_source_id()?);
        hash.update(closure.source_receipt_sha256.as_bytes());
        hash.update(closure.regeneration_plan_sha256.as_bytes());
        closure
            .sealed
            .complete_after_inventory(closure.final_epoch, hex::encode(hash.finalize()))
            .map(|mut completed| {
                completed.baseline = Some(baseline);
                completed
            })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::historical_native_effects::{HistoricalNativeEffect, HistoricalNativeEffectBinding};
    fn ledger(path: &Path) -> HistoricalCatchupLedger {
        let db = Connection::open(path).expect("private ledger");
        db.execute_batch("CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));").expect("schema");
        db.execute(
            "INSERT INTO expected VALUES('entity','entity:a',?,0)",
            ["a".repeat(64)],
        )
        .expect("original row");
        HistoricalCatchupLedger {
            db,
            path: path.into(),
            tables: BTreeSet::from(["entity".into()]),
            binding: "b".repeat(64),
            report_sha: "c".repeat(64),
            baseline_sha: "d".repeat(64),
            batch_count: 2,
            regeneration_count: 1,
            next_batch: 0,
            next_regeneration: 0,
            archive_applied: false,
            original_archive: None,
        }
    }
    fn token(
        ordinal: u64,
        effects: Vec<HistoricalNativeEffect>,
    ) -> VerifiedHistoricalNativeEffects {
        VerifiedHistoricalNativeEffects {
            binding: HistoricalNativeEffectBinding {
                version: 1,
                authority_binding_sha256: "b".repeat(64),
                phase: "batch".into(),
                ordinal,
                primary_commit_id: format!("batch-{ordinal}"),
                primary_receipt_sha256: "c".repeat(64),
                inventory_sha256: "d".repeat(64),
                inventory_count: effects.len() as u64,
            },
            effects,
        }
    }
    fn effect(id: &str, before: Option<&str>, after: Option<&str>) -> HistoricalNativeEffect {
        HistoricalNativeEffect {
            table: "entity".into(),
            id: id.into(),
            before_sha256: before.map(str::to_owned),
            after_sha256: after.map(str::to_owned),
        }
    }
    #[test]
    fn ledger_replay_exact_deletion_and_stale_batch_rolls_back_every_effect() {
        let dir = tempfile::tempdir().expect("directory");
        let mut ledger = ledger(&dir.path().join("ledger.sqlite"));
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let c = "c".repeat(64);
        assert!(ledger
            .apply(token(
                0,
                vec![
                    effect("entity:a", Some(&a), Some(&b)),
                    effect("entity:missing", Some(&a), None)
                ]
            ))
            .is_err());
        assert_eq!(ledger.next_batch, 0);
        let actual: String = ledger
            .db
            .query_row("SELECT h FROM expected WHERE k='entity:a'", [], |r| {
                r.get(0)
            })
            .expect("original retained");
        assert_eq!(actual, a);
        ledger
            .apply(token(
                0,
                vec![
                    effect("entity:a", Some(&a), None),
                    effect("entity:b", None, Some(&c)),
                ],
            ))
            .expect("exact removal and create");
        assert!(ledger
            .db
            .query_row::<String, _, _>("SELECT h FROM expected WHERE k='entity:a'", [], |r| r
                .get(0))
            .optional()
            .expect("deleted lookup")
            .is_none());
        assert!(
            ledger
                .apply(token(0, vec![effect("entity:b", Some(&c), None)]))
                .is_err(),
            "duplicate ordinal refused"
        );
        assert_eq!(ledger.next_batch, 1);
    }
    #[test]
    fn restart_rebuilds_immutable_baseline_then_replays_committed_prefix_once() {
        let dir = tempfile::tempdir().expect("directory");
        let baseline = dir.path().join("baseline.sqlite");
        let original = ledger(&baseline);
        drop(original);
        let before = file_sha(&baseline).expect("baseline hash");
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let c = "c".repeat(64);
        let first = dir.path().join("first.sqlite");
        std::fs::copy(&baseline, &first).expect("copy");
        let mut initial = HistoricalCatchupLedger {
            db: Connection::open(&first).expect("first"),
            path: first.clone(),
            ..ledger(&dir.path().join("unused.sqlite"))
        };
        initial
            .apply(token(0, vec![effect("entity:a", Some(&a), Some(&b))]))
            .expect("DB manifest consumed");
        drop(initial); // Simulated CLI process loss; its partial ledger is retained.
        let second = dir.path().join("second.sqlite");
        std::fs::copy(&baseline, &second).expect("new original copy");
        let mut resumed = HistoricalCatchupLedger {
            db: Connection::open(&second).expect("second"),
            path: second,
            ..ledger(&dir.path().join("unused2.sqlite"))
        };
        resumed
            .apply(token(0, vec![effect("entity:a", Some(&a), Some(&b))]))
            .expect("durable prefix replay");
        resumed
            .apply(token(1, vec![effect("entity:a", Some(&b), Some(&c))]))
            .expect("new batch extends prefix");
        let actual: String = resumed
            .db
            .query_row("SELECT h FROM expected", [], |r| r.get(0))
            .expect("final ledger");
        assert_eq!(actual, c);
        assert_eq!(file_sha(&baseline).expect("original hash"), before);
        let old: String = Connection::open(&first)
            .expect("preserved partial")
            .query_row("SELECT h FROM expected", [], |r| r.get(0))
            .expect("old partial hash");
        assert_eq!(old, b);
    }
}
