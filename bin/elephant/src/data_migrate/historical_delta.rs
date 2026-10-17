//! Bounded S0/S1 adapter. Original lexical rows and Python hash domains remain
//! evidence; runtime batches carry separately computed full typed image hashes.
use super::archive_disposition::OperationalDisposition;
use super::source_spool::{file_sha, readonly, typed, SourceSpool, TypedRow};
use anyhow::{ensure, Context};
use elephant_storage::historical_delta::{
    retained_control_fingerprint, runtime_embedding_fingerprint, runtime_row_fingerprint,
    HistoricalChangeKind, HistoricalDeltaBatch, HistoricalDeltaRow, HistoricalEmbeddingDelta,
    HistoricalRetainedControlDelta,
};
use elephant_storage::transfer_batch::{strict_json, MAX_ROW_BYTES};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

const MAX_HEADER: u64 = 1024 * 1024;
const MAX_CHANGE_BYTES: usize = 2 * MAX_ROW_BYTES + 1024 * 1024;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const MAX_BATCH_ROWS: usize = 256;

#[derive(Debug)]
pub(super) struct PreparedDelta {
    path: PathBuf,
    sha256: String,
    before_spool: PathBuf,
    after_spool: PathBuf,
    before_spool_sha256: String,
    after_spool_sha256: String,
    before_snapshot: String,
    after_snapshot: String,
    delta_sha256: String,
    source_space: String,
    target_space: String,
    original_report_sha256: String,
    pub batches: u64,
    pub changes: u64,
    accept_deletions: bool,
    operational_disposition: OperationalDisposition,
    archive_batches: u64,
    archive_changes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlannedChange {
    table: String,
    key: String,
    kind: String,
    before_sha256: Option<String>,
    after_sha256: Option<String>,
    destructive_intent_required: bool,
    physical_erase_authorized: bool,
    disposition: String,
    advisory_metadata: serde_json::Value,
    target_guard: serde_json::Value,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    operation_id: String,
}

fn readonly_uri(path: &Path) -> anyhow::Result<String> {
    let path = path.canonicalize()?;
    let text = path.to_str().context("source spool path is not UTF-8")?;
    let mut uri = String::from("file:");
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
            uri.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(uri, "%{byte:02X}")?;
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}
fn header(path: &Path) -> anyhow::Result<serde_json::Value> {
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= MAX_HEADER,
        "delta/report header exceeds bound"
    );
    let mut raw = String::new();
    file.take(MAX_HEADER + 1).read_to_string(&mut raw)?;
    ensure!(raw.len() as u64 <= MAX_HEADER, "delta/report header grew");
    Ok(strict_json(&raw)?)
}
fn digest(value: &serde_json::Value) -> anyhow::Result<String> {
    let value = value.as_str().context("digest is not a string")?;
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid digest"
    );
    Ok(value.into())
}
fn line(reader: &mut impl BufRead) -> anyhow::Result<Option<Vec<u8>>> {
    bounded_line(reader, MAX_CHANGE_BYTES)
}
fn bounded_line(reader: &mut impl BufRead, maximum: usize) -> anyhow::Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            ensure!(bytes.is_empty(), "unterminated delta line");
            return Ok(None);
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |n| n + 1);
        ensure!(
            bytes.len().checked_add(take).is_some_and(|n| n <= maximum),
            "delta line exceeds bound"
        );
        let done = available[take - 1] == b'\n';
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if done {
            return Ok(Some(bytes));
        }
    }
}
/// Returns exact lexical bytes only after declared length/hash/typed checks.
fn original(
    db: &Connection,
    table: &str,
    key: &str,
    declared: Option<&str>,
    supplied: Option<&serde_json::Value>,
) -> anyhow::Result<Option<String>> {
    let meta:Option<(u64,String,String)>=db.query_row("SELECT length(x.lexical),x.lexical_sha256,r.h FROM rows r JOIN raw_rows x ON x.t=r.t AND x.k=r.k WHERE r.t=? AND r.k=?",params![table,key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    match (meta, declared, supplied) {
        (None, None, None) => Ok(None),
        (Some((size, lexical_sha, python_sha)), Some(declared), Some(supplied)) => {
            ensure!(
                size <= MAX_ROW_BYTES as u64 && python_sha == declared,
                "original delta image size/hash differs"
            );
            let raw: Vec<u8> = db.query_row(
                "SELECT lexical FROM raw_rows WHERE t=? AND k=? AND length(lexical)<=?",
                params![table, key, MAX_ROW_BYTES],
                |r| r.get(0),
            )?;
            ensure!(
                raw.len() as u64 == size && hex::encode(Sha256::digest(&raw)) == lexical_sha,
                "original lexical image changed"
            );
            let raw = String::from_utf8(raw)?;
            ensure!(
                strict_json(&raw)? == *supplied,
                "diff image differs from original source bytes"
            );
            let _ = typed(table, &raw)?;
            Ok(Some(raw))
        }
        _ => anyhow::bail!("delta original presence differs"),
    }
}
/// Resolve a payload-free index reference from its immutable original spool.
/// Length and declared lexical digest are checked before allocating the blob;
/// the bytes are then hashed independently rather than trusting `SQLite` metadata.
fn referenced_original(
    db: &Connection,
    table: &str,
    key: &str,
    expected_hash: Option<&str>,
    expected_size: Option<u64>,
) -> anyhow::Result<Option<Vec<u8>>> {
    let meta: Option<(u64, String)> = db
        .query_row(
            "SELECT length(lexical),lexical_sha256 FROM raw_rows WHERE t=? AND k=?",
            params![table, key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    match (meta, expected_hash, expected_size) {
        (None, None, None) => Ok(None),
        (Some((size, digest)), Some(expected), Some(expected_size)) => {
            ensure!(
                size == expected_size && size <= MAX_ROW_BYTES as u64 && digest == expected,
                "referenced original size or digest differs"
            );
            let raw: Vec<u8> = db.query_row(
                "SELECT lexical FROM raw_rows WHERE t=? AND k=? AND length(lexical)=? AND length(lexical)<=? AND lexical_sha256=?",
                params![table, key, size, MAX_ROW_BYTES, expected], |r| r.get(0),
            )?;
            ensure!(
                raw.len() as u64 == size && hex::encode(Sha256::digest(&raw)) == expected,
                "referenced original bytes changed"
            );
            Ok(Some(raw))
        }
        _ => anyhow::bail!("referenced original presence differs"),
    }
}
fn classify(
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
) -> anyhow::Result<(&'static str, HistoricalChangeKind)> {
    Ok(match (before, after) {
        (None, Some(_)) => ("created", HistoricalChangeKind::Create),
        (Some(_), None) => ("removed", HistoricalChangeKind::Remove),
        (Some(before), Some(after)) if before != after => match (
            before.get("deleted_at").is_some_and(|v| !v.is_null()),
            after.get("deleted_at").is_some_and(|v| !v.is_null()),
        ) {
            (false, true) => ("deleted", HistoricalChangeKind::SoftDelete),
            (true, false) => ("restored", HistoricalChangeKind::Restore),
            _ => ("updated", HistoricalChangeKind::Update),
        },
        _ => anyhow::bail!("empty or unchanged delta image"),
    })
}
fn group_key(table: &str, key: &str) -> anyhow::Result<String> {
    let (prefix, id) = key.split_once(':').context("original record id")?;
    ensure!(
        prefix == table && !id.is_empty(),
        "original table/id differs"
    );
    Ok(if matches!(table, "work_item" | "work_item_archive") {
        format!("work:{id}")
    } else {
        format!("row:{table}:{id}")
    })
}

/// Entirely offline. Complete S0/S1 difference closure uses indexed joins, never
/// a caller's assertion that their changes.jsonl contains every graph-only edit.
pub(super) fn prepare(
    before: &SourceSpool,
    after: &SourceSpool,
    delta_path: &Path,
    report_path: &Path,
    index_path: &Path,
) -> anyhow::Result<PreparedDelta> {
    ensure!(!index_path.exists(), "delta index must be new");
    before.verify_unchanged()?;
    after.verify_unchanged()?;
    ensure!(
        before.space_id == after.space_id && before.target_space_id == after.target_space_id,
        "delta space mapping differs"
    );
    ensure!(
        before.operational_disposition == after.operational_disposition,
        "changing operational disposition during catchup is unsupported"
    );
    let operational_disposition = before.operational_disposition;
    let plan = header(delta_path)?;
    ensure!(
        OperationalDisposition::from_plan(&plan)? == operational_disposition,
        "delta operational disposition differs"
    );
    ensure!(
        plan["format"] == "recall-preservation-delta-v1"
            && plan["state"] == "offline_delta_validation_only"
            && plan["target_cas_verified"] == false
            && plan["after_snapshot_completeness_verified"] == true
            && plan["runtime_blockers"]
                .as_array()
                .is_some_and(Vec::is_empty),
        "delta plan state differs"
    );
    ensure!(
        plan["canonical_hash_contract"]["id"] == "homecore-python-json-v1"
            && plan["before_snapshot_sha256"] == before.source_sha256
            && plan["after_snapshot_sha256"] == after.source_sha256
            && plan["baseline_index_sha256"] == before.inventory_sha256
            && plan["baseline_plan_id"] == before.plan_id
            && plan["space_id"] == before.space_id,
        "delta plan inputs differ"
    );
    let delta_sha256 = file_sha(delta_path)?;
    let report = header(report_path)?;
    ensure!(
        OperationalDisposition::from_plan(&report)? == operational_disposition
            && OperationalDisposition::from_plan(&report["catchup_baseline"])?
                == operational_disposition,
        "baseline operational disposition differs"
    );
    if operational_disposition == OperationalDisposition::ManagedArchiveV1 {
        ensure!(
            report["operational_archive"] == report["catchup_baseline"]["operational_archive"]
                && report["operational_archive"].is_object(),
            "baseline archive receipt absent or divergent"
        );
    } else {
        ensure!(
            report.get("operational_archive").is_none()
                && report["catchup_baseline"]
                    .get("operational_archive")
                    .is_none(),
            "native baseline contains archive metadata"
        );
    }
    ensure!(
        elephant_storage::historical_catchup_ledger::valid_baseline_report(&report)
            && report["source_sha256"] == before.source_sha256
            && report["inventory_sha256"] == before.inventory_sha256
            && report["plan_id"] == before.plan_id
            && report["source_space_id"] == before.space_id
            && report["target_space_id"] == before.target_space_id
            && report["retained_source_controls"]["migration_history"]
                == serde_json::to_value(&before.retained_controls)?,
        "original transfer report differs"
    );
    let original_report_sha256 = file_sha(report_path)?;
    let changes_path = delta_path
        .parent()
        .context("delta parent")?
        .join("changes.jsonl");
    let expected_changes_sha = digest(&plan["output_changes_sha256"])?;
    let source_before = readonly(&before.path)?;
    let source_after = readonly(&after.path)?;
    let mut db = Connection::open(index_path)?;
    db.execute_batch("CREATE TABLE changes(t TEXT,k TEXT,g TEXT,before_h TEXT,after_h TEXT,before_lexical_h TEXT,after_lexical_h TEXT,before_n INTEGER,after_n INTEGER,kind TEXT,destructive INTEGER,archived INTEGER NOT NULL,batch INTEGER,PRIMARY KEY(t,k)); CREATE INDEX grouped_changes ON changes(archived,g,t,k); CREATE INDEX batched_changes ON changes(archived,batch,g,t,k);")?;
    let mut reader = BufReader::new(std::fs::File::open(&changes_path)?);
    let mut sha = Sha256::new();
    let mut count = 0_u64;
    let tx = db.transaction()?;
    while let Some(raw) = line(&mut reader)? {
        sha.update(&raw);
        let value = strict_json(std::str::from_utf8(&raw)?)?;
        let change: PlannedChange = serde_json::from_value(value)?;
        ensure!(
            !change.physical_erase_authorized,
            "delta cannot grant physical erase authority"
        );
        let _ = digest(&serde_json::Value::String(change.operation_id))?;
        let _ = (&change.advisory_metadata, &change.target_guard); // Advisory, never runtime authority.
        ensure!(
            change.disposition
                == if change.table == "migration_history" {
                    "retain_source_control_only"
                } else if operational_disposition.archives(&change.table) {
                    "guarded_archive_row_change"
                } else {
                    "guarded_target_row_change"
                },
            "delta disposition differs"
        );
        let (name, kind) = classify(change.before.as_ref(), change.after.as_ref())?;
        ensure!(
            name == change.kind
                && change.destructive_intent_required
                    == matches!(
                        kind,
                        HistoricalChangeKind::Remove | HistoricalChangeKind::SoftDelete
                    ),
            "delta transition kind differs"
        );
        let old = original(
            &source_before,
            &change.table,
            &change.key,
            change.before_sha256.as_deref(),
            change.before.as_ref(),
        )?;
        let new = original(
            &source_after,
            &change.table,
            &change.key,
            change.after_sha256.as_deref(),
            change.after.as_ref(),
        )?;
        tx.execute(
            "INSERT INTO changes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
            params![
                change.table,
                change.key,
                group_key(&change.table, &change.key)?,
                change.before_sha256,
                change.after_sha256,
                old.as_ref()
                    .map(|x| hex::encode(Sha256::digest(x.as_bytes()))),
                new.as_ref()
                    .map(|x| hex::encode(Sha256::digest(x.as_bytes()))),
                old.as_ref().map(|x| x.len() as u64),
                new.as_ref().map(|x| x.len() as u64),
                name,
                change.destructive_intent_required,
                operational_disposition.archives(&change.table)
            ],
        )?;
        count += 1;
    }
    tx.commit()?;
    ensure!(
        hex::encode(sha.finalize()) == expected_changes_sha
            && plan["changes"].as_u64() == Some(count),
        "delta file/count differs"
    );
    db.execute("ATTACH DATABASE ? AS s0", [readonly_uri(&before.path)?])?;
    db.execute("ATTACH DATABASE ? AS s1", [readonly_uri(&after.path)?])?;
    let uncovered:u64=db.query_row("SELECT count(*) FROM (SELECT a.t,a.k,a.h AS before_h,b.h AS after_h FROM s0.rows a LEFT JOIN s1.rows b ON a.t=b.t AND a.k=b.k WHERE b.h IS NULL OR a.h!=b.h UNION ALL SELECT b.t,b.k,NULL,b.h FROM s1.rows b LEFT JOIN s0.rows a ON a.t=b.t AND a.k=b.k WHERE a.k IS NULL) d LEFT JOIN changes c ON d.t=c.t AND d.k=c.k WHERE c.k IS NULL OR c.before_h IS NOT d.before_h OR c.after_h IS NOT d.after_h",[],|r|r.get(0))?;
    ensure!(uncovered == 0, "S0/S1 diff is incomplete");
    db.execute_batch("DETACH DATABASE s0; DETACH DATABASE s1;")?;
    let batches = partition_batches(&db, false)?;
    let archive_batches = partition_batches(&db, true)?;
    let archive_changes: u64 =
        db.query_row("SELECT count(*) FROM changes WHERE archived=1", [], |row| {
            row.get(0)
        })?;
    drop(db);
    before.verify_unchanged()?;
    after.verify_unchanged()?;
    ensure!(
        file_sha(delta_path)? == delta_sha256 && file_sha(report_path)? == original_report_sha256,
        "bound delta/report changed"
    );
    Ok(PreparedDelta {
        path: index_path.into(),
        sha256: file_sha(index_path)?,
        before_spool: before.path.clone(),
        after_spool: after.path.clone(),
        before_spool_sha256: before.inventory_sha256.clone(),
        after_spool_sha256: after.inventory_sha256.clone(),
        before_snapshot: before.source_sha256.clone(),
        after_snapshot: after.source_sha256.clone(),
        delta_sha256,
        source_space: before.space_id.clone(),
        target_space: before.target_space_id.clone(),
        original_report_sha256,
        batches,
        changes: count,
        accept_deletions: false,
        operational_disposition,
        archive_batches,
        archive_changes,
    })
}

fn partition_batches(db: &Connection, archived: bool) -> anyhow::Result<u64> {
    let mut cursor = String::new();
    let (mut ordinal, mut rows, mut bytes) = (0_u64, 0_u64, 0_u64);
    let max_bytes = if archived {
        elephant_storage::historical_operational_archive::MAX_PAGE_BYTES as u64
    } else {
        MAX_BATCH_BYTES as u64
    };
    let mut any = false;
    loop {
        let group:Option<(String,u64,u64)>=db.query_row("SELECT g,count(*),sum(coalesce(before_n,0)+coalesce(after_n,0)) FROM changes WHERE archived=? AND g>? GROUP BY g ORDER BY g LIMIT 1",params![archived,&cursor],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((key, n, size)) = group else {
            break;
        };
        ensure!(
            n <= MAX_BATCH_ROWS as u64 && size <= max_bytes,
            "indivisible hot/archive group exceeds batch bound"
        );
        if rows > 0 && (rows + n > MAX_BATCH_ROWS as u64 || bytes + size > max_bytes) {
            ordinal += 1;
            rows = 0;
            bytes = 0;
        }
        db.execute(
            "UPDATE changes SET batch=? WHERE archived=? AND g=?",
            params![ordinal, archived, key],
        )?;
        rows += n;
        bytes += size;
        cursor = key;
        any = true;
    }
    Ok(if any { ordinal + 1 } else { 0 })
}

impl PreparedDelta {
    fn declare_deletions(&mut self, accepted: bool) -> anyhow::Result<()> {
        let db = readonly(&self.path)?;
        let required: u64 = db.query_row(
            "SELECT count(*) FROM changes WHERE destructive=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            accepted || required == 0,
            "delta contains deletions; explicit --accept-deletions is required"
        );
        self.accept_deletions = accepted;
        Ok(())
    }

    pub fn verify_unchanged(&self) -> anyhow::Result<()> {
        let db = readonly(&self.path)?;
        drop(db);
        ensure!(
            file_sha(&self.path)? == self.sha256
                && file_sha(&self.before_spool)? == self.before_spool_sha256
                && file_sha(&self.after_spool)? == self.after_spool_sha256,
            "prepared delta or bound source spool changed"
        );
        Ok(())
    }
    /// Build exact source-lexical changes; Python hashes never become runtime CAS hashes.
    fn archive_batch(
        &self,
        ordinal: u64,
    ) -> anyhow::Result<Vec<elephant_storage::historical_operational_archive::ArchiveChange>> {
        use elephant_storage::historical_operational_archive::{ArchiveChange, ArchiveRow};
        ensure!(
            self.operational_disposition == OperationalDisposition::ManagedArchiveV1
                && ordinal < self.archive_batches,
            "archive batch disposition or ordinal differs"
        );
        let db = readonly(&self.path)?;
        let before_db = readonly(&self.before_spool)?;
        let after_db = readonly(&self.after_spool)?;
        let mut query=db.prepare("SELECT t,k,before_lexical_h,after_lexical_h,before_n,after_n,destructive FROM changes WHERE archived=1 AND batch=? ORDER BY g,t,k")?;
        let mut cursor = query.query([ordinal])?;
        let mut result = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = cursor.next()? {
            let table: String = row.get(0)?;
            let id: String = row.get(1)?;
            let before_hash: Option<String> = row.get(2)?;
            let after_hash: Option<String> = row.get(3)?;
            let before_n: Option<u64> = row.get(4)?;
            let after_n: Option<u64> = row.get(5)?;
            let required: bool = row.get(6)?;
            let before =
                referenced_original(&before_db, &table, &id, before_hash.as_deref(), before_n)?;
            let after =
                referenced_original(&after_db, &table, &id, after_hash.as_deref(), after_n)?;
            ensure!(
                !required || self.accept_deletions,
                "archive deletion intent absent"
            );
            bytes = bytes
                .checked_add(before.as_ref().map_or(0, Vec::len))
                .and_then(|n| n.checked_add(after.as_ref().map_or(0, Vec::len)))
                .context("archive batch bytes overflow")?;
            ensure!(
                result.len() < MAX_BATCH_ROWS
                    && bytes <= elephant_storage::historical_operational_archive::MAX_PAGE_BYTES,
                "archive batch bound"
            );
            let before_sha256 = before.as_ref().map(|raw| hex::encode(Sha256::digest(raw)));
            let after = after.map(|lexical| ArchiveRow {
                table: table.clone(),
                id: id.clone(),
                lexical_sha256: hex::encode(Sha256::digest(&lexical)),
                lexical,
            });
            if let Some(row) = &after {
                row.metadata()?;
            }
            result.push(ArchiveChange {
                table,
                id,
                before_sha256,
                after,
                logical_deletion_authorized: required && self.accept_deletions,
            });
        }
        ensure!(!result.is_empty(), "archive batch empty");
        Ok(result)
    }
    fn build_archive_successor(
        &self,
        path: &Path,
        binding: elephant_storage::historical_operational_archive::ArchiveBinding,
        original: &elephant_storage::historical_operational_archive::VerifiedArchive,
        expected_inventory: &str,
    ) -> anyhow::Result<elephant_storage::historical_operational_archive::VerifiedArchive> {
        use elephant_storage::historical_operational_archive::ArchiveBuilder;
        self.verify_unchanged()?;
        let mut builder = ArchiveBuilder::successor(path, binding, original)?;
        for ordinal in 0..self.archive_batches {
            builder.apply(&self.archive_batch(ordinal)?)?;
        }
        let verified = builder.seal()?;
        ensure!(
            verified.seal().ordered_inventory_sha256 == expected_inventory,
            "complete S1 archive differs from independently bound source spool"
        );
        self.verify_unchanged()?;
        Ok(verified)
    }
    pub fn batch(
        &self,
        ordinal: u64,
        migration_id: &str,
        epoch: &str,
        previous: Option<String>,
    ) -> anyhow::Result<HistoricalDeltaBatch> {
        ensure!(ordinal < self.batches, "batch ordinal out of range");
        let db = readonly(&self.path)?;
        let before_db = readonly(&self.before_spool)?;
        let after_db = readonly(&self.after_spool)?;
        let mut stmt=db.prepare("SELECT t,k,before_lexical_h,after_lexical_h,before_n,after_n,kind,destructive FROM changes WHERE archived=0 AND batch=? ORDER BY g,t,k")?;
        let mut cursor = stmt.query([ordinal])?;
        let mut batch = HistoricalDeltaBatch {
            version: 1,
            migration_id: migration_id.into(),
            source_space_id: self.source_space.clone(),
            space_id: self.target_space.clone(),
            before_snapshot_sha256: self.before_snapshot.clone(),
            after_snapshot_sha256: self.after_snapshot.clone(),
            planner_delta_sha256: self.delta_sha256.clone(),
            batch_ordinal: ordinal,
            previous_batch_receipt: previous,
            expected_epoch: epoch.into(),
            rows: Vec::new(),
            embeddings: Vec::new(),
            retained_controls: Vec::new(),
        };
        while let Some(row) = cursor.next()? {
            let table: String = row.get(0)?;
            let id: String = row.get(1)?;
            let before_hash: Option<String> = row.get(2)?;
            let after_hash: Option<String> = row.get(3)?;
            let before_n: Option<u64> = row.get(4)?;
            let after_n: Option<u64> = row.get(5)?;
            let kind: String = row.get(6)?;
            let required: bool = row.get(7)?;
            let before =
                referenced_original(&before_db, &table, &id, before_hash.as_deref(), before_n)?;
            let after =
                referenced_original(&after_db, &table, &id, after_hash.as_deref(), after_n)?;
            ensure!(
                !required || self.accept_deletions,
                "planned deletion requirement is not authorization"
            );
            let destructive = required && self.accept_deletions;
            let before = before
                .as_deref()
                .map(|b| typed(&table, std::str::from_utf8(b)?))
                .transpose()?;
            let after = after
                .as_deref()
                .map(|b| typed(&table, std::str::from_utf8(b)?))
                .transpose()?;
            let kind = match kind.as_str() {
                "created" => HistoricalChangeKind::Create,
                "updated" => HistoricalChangeKind::Update,
                "deleted" => HistoricalChangeKind::SoftDelete,
                "restored" => HistoricalChangeKind::Restore,
                "removed" => HistoricalChangeKind::Remove,
                _ => anyhow::bail!("unknown prepared transition"),
            };
            match (before,after) {
                (None,Some(TypedRow::Ordinary(after)))=>batch.rows.push(ordinary(&id,None,Some(after),kind,destructive)?),
                (Some(TypedRow::Ordinary(before)),None)=>batch.rows.push(ordinary(&id,Some(before),None,kind,destructive)?),
                (Some(TypedRow::Ordinary(before)),Some(TypedRow::Ordinary(after)))=>batch.rows.push(ordinary(&id,Some(before),Some(after),kind,destructive)?),
                (None,Some(TypedRow::Embedding(after)))=>batch.embeddings.push(embedding(&id,None,Some(after),kind,destructive)?),
                (Some(TypedRow::Embedding(before)),None)=>batch.embeddings.push(embedding(&id,Some(before),None,kind,destructive)?),
                (Some(TypedRow::Embedding(before)),Some(TypedRow::Embedding(after)))=>batch.embeddings.push(embedding(&id,Some(before),Some(after),kind,destructive)?),
                (None,Some(TypedRow::Receipt(after)))=>batch.retained_controls.push(retained(None,Some(after))?),
                (Some(TypedRow::Receipt(before)),None)=>batch.retained_controls.push(retained(Some(before),None)?),
                (Some(TypedRow::Receipt(before)),Some(TypedRow::Receipt(after)))=>batch.retained_controls.push(retained(Some(before),Some(after))?),
                _=>anyhow::bail!("delta typed category mismatch or unsupported original space configuration change"),
            }
        }
        Ok(batch)
    }
}
fn ordinary(
    id: &str,
    before: Option<elephant_storage::transfer_batch::TransferRecord>,
    after: Option<elephant_storage::transfer_batch::TransferRecord>,
    kind: HistoricalChangeKind,
    destructive: bool,
) -> anyhow::Result<HistoricalDeltaRow> {
    Ok(HistoricalDeltaRow {
        id: id.parse()?,
        kind,
        before_fingerprint: before.as_ref().map(runtime_row_fingerprint).transpose()?,
        after_fingerprint: after.as_ref().map(runtime_row_fingerprint).transpose()?,
        before,
        after,
        destructive_intent: destructive,
    })
}
fn embedding(
    id: &str,
    before: Option<elephant_types::records::NodeEmbedding>,
    after: Option<elephant_types::records::NodeEmbedding>,
    kind: HistoricalChangeKind,
    destructive: bool,
) -> anyhow::Result<HistoricalEmbeddingDelta> {
    Ok(HistoricalEmbeddingDelta {
        id: id.parse()?,
        kind,
        before_fingerprint: before
            .as_ref()
            .map(runtime_embedding_fingerprint)
            .transpose()?,
        after_fingerprint: after
            .as_ref()
            .map(runtime_embedding_fingerprint)
            .transpose()?,
        before,
        after,
        destructive_intent: destructive,
    })
}
fn retained(
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> anyhow::Result<HistoricalRetainedControlDelta> {
    Ok(HistoricalRetainedControlDelta {
        before_fingerprint: before
            .as_ref()
            .map(retained_control_fingerprint)
            .transpose()?,
        after_fingerprint: after
            .as_ref()
            .map(retained_control_fingerprint)
            .transpose()?,
        before,
        after,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_types::records::{BaseFields, Entity};
    use elephant_types::{RecordId, TableName};
    use std::collections::{BTreeMap, BTreeSet};

    fn spool(path: &Path, rows: &[serde_json::Value], snapshot: &str) -> SourceSpool {
        let db = Connection::open(path).expect("fixture index");
        db.execute_batch("CREATE TABLE rows(t TEXT,k TEXT,h TEXT,PRIMARY KEY(t,k)); CREATE TABLE raw_rows(t TEXT,k TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(t,k));").expect("schema");
        for row in rows {
            let raw = serde_json::to_vec(row).expect("raw");
            let sha = hex::encode(Sha256::digest(&raw));
            let id = row["id"].as_str().expect("id");
            db.execute("INSERT INTO rows VALUES ('entity',?,?)", params![id, sha])
                .expect("index");
            db.execute(
                "INSERT INTO raw_rows VALUES ('entity',?,?,?)",
                params![id, raw, sha],
            )
            .expect("lexical");
        }
        drop(db);
        SourceSpool {
            operational_disposition: OperationalDisposition::Native,
            archive_inventory: None,
            path: path.into(),
            inventory_sha256: file_sha(path).expect("hash"),
            source_sha256: snapshot.repeat(64),
            plan_id: "a".repeat(64),
            space_id: "default".into(),
            target_space_id: "household".into(),
            tables: BTreeMap::from([("entity".into(), rows.len() as u64)]),
            spaces: BTreeSet::from(["default".into()]),
            retained_controls: Vec::new(),
            export_notes: serde_json::json!({}),
            media_preservation: serde_json::json!({}),
            historical_work_counts: BTreeMap::new(),
            runtime_requirements: Vec::new(),
        }
    }
    #[test]
    fn archive_partition_cannot_turn_operational_changes_into_native_writes() -> anyhow::Result<()>
    {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE changes(t TEXT,k TEXT,g TEXT,before_h TEXT,after_h TEXT,before_lexical_h TEXT,after_lexical_h TEXT,before_n INTEGER,after_n INTEGER,kind TEXT,destructive INTEGER,archived INTEGER NOT NULL,batch INTEGER,PRIMARY KEY(t,k)); CREATE INDEX grouped_changes ON changes(archived,g,t,k);")?;
        for (table, key, group, archived) in [
            ("work_item", "work_item:1", "work:1", true),
            ("work_item_archive", "work_item_archive:1", "work:1", true),
            ("entity", "entity:2", "row:entity:2", false),
        ] {
            db.execute(
                "INSERT INTO changes VALUES (?,?,?,NULL,NULL,?,NULL,2,NULL,'removed',1,?,NULL)",
                params![table, key, group, "a".repeat(64), archived],
            )?;
        }
        assert_eq!(partition_batches(&db, false)?, 1);
        assert_eq!(partition_batches(&db, true)?, 1);
        let native: u64 = db.query_row(
            "SELECT count(*) FROM changes WHERE archived=0 AND batch=0",
            [],
            |r| r.get(0),
        )?;
        let archived: u64 = db.query_row(
            "SELECT count(*) FROM changes WHERE archived=1 AND batch=0 AND g='work:1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(native, 1);
        assert_eq!(archived, 2);
        db.execute("DELETE FROM changes WHERE archived=0", [])?;
        assert_eq!(partition_batches(&db, false)?, 0);
        assert_eq!(partition_batches(&db, true)?, 1);
        Ok(())
    }

    fn entity() -> serde_json::Value {
        serde_json::to_value(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "default".into(),
                "original-agent".into(),
            ),
            "person".into(),
            "name".into(),
        ))
        .expect("entity")
    }
    fn change(before: &serde_json::Value, after: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({"table":"entity","key":before["id"],"kind":"updated",
            "before_sha256":hex::encode(Sha256::digest(serde_json::to_vec(before).expect("old"))),
            "after_sha256":hex::encode(Sha256::digest(serde_json::to_vec(after).expect("new"))),
            "destructive_intent_required":false,"physical_erase_authorized":false,"disposition":"guarded_target_row_change",
            "advisory_metadata":{},"target_guard":{},"before":before,"after":after,"operation_id":"e".repeat(64)})
    }
    fn files(
        root: &Path,
        before: &SourceSpool,
        after: &SourceSpool,
        changes: &[serde_json::Value],
    ) -> (PathBuf, PathBuf) {
        let mut bytes = Vec::new();
        for change in changes {
            bytes.extend(serde_json::to_vec(change).expect("change"));
            bytes.push(b'\n');
        }
        std::fs::write(root.join("changes.jsonl"), &bytes).expect("changes");
        let delta = root.join("delta.json");
        std::fs::write(&delta,serde_json::to_vec(&serde_json::json!({
            "format":"recall-preservation-delta-v1","state":"offline_delta_validation_only","target_cas_verified":false,
            "after_snapshot_completeness_verified":true,"runtime_blockers":[],"canonical_hash_contract":{"id":"homecore-python-json-v1"},
            "before_snapshot_sha256":before.source_sha256,"after_snapshot_sha256":after.source_sha256,"baseline_index_sha256":before.inventory_sha256,
            "baseline_plan_id":before.plan_id,"space_id":before.space_id,"output_changes_sha256":hex::encode(Sha256::digest(&bytes)),"changes":changes.len()
        })).expect("plan")).expect("plan file");
        let report = root.join("original.json");
        std::fs::write(&report,serde_json::to_vec(&serde_json::json!({"format":"recall-legacy-transfer-v2","state":"initial_history_reconciled_vector_coverage_pending","source_sha256":before.source_sha256,
            "inventory_sha256":before.inventory_sha256,"plan_id":before.plan_id,"source_space_id":before.space_id,"target_space_id":before.target_space_id,
            "retained_source_controls":{"migration_history":[]}})).expect("report")).expect("report file");
        (delta, report)
    }
    #[test]
    fn graph_only_diff_requires_complete_original_spools_and_indexed_batches() {
        let dir = tempfile::tempdir().expect("dir");
        let old = entity();
        let mut new = old.clone();
        new["display_name"] = "agent-authored change".into();
        let before = spool(
            &dir.path().join("s0.sqlite"),
            std::slice::from_ref(&old),
            "b",
        );
        let after = spool(&dir.path().join("s1.sqlite"), &[new.clone()], "c");
        let (delta, report) = files(dir.path(), &before, &after, &[change(&old, &new)]);
        let prepared = prepare(
            &before,
            &after,
            &delta,
            &report,
            &dir.path().join("delta.sqlite"),
        )
        .expect("complete graph delta");
        assert_eq!((prepared.changes, prepared.batches), (1, 1));
        let batch = prepared
            .batch(0, &"d".repeat(64), "epoch", None)
            .expect("typed batch");
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(
            batch.rows[0].before.as_ref().expect("old").base().space_id,
            "default"
        );
        batch
            .prepare()
            .expect("runtime separately validates full typed images");
        let db = readonly(&prepared.path).expect("closed index");
        let plan:String=db.query_row("EXPLAIN QUERY PLAN SELECT t,k,before_lexical_h,after_lexical_h,before_n,after_n,kind,destructive FROM changes WHERE archived=0 AND batch=? ORDER BY g,t,k",[0],|r|r.get(3)).expect("query plan");
        assert!(plan.contains("batched_changes"), "{plan}");
        assert_eq!(file_sha(&before.path).expect("s0"), before.inventory_sha256);
        assert_eq!(file_sha(&after.path).expect("s1"), after.inventory_sha256);
        let index_bytes = std::fs::read(&prepared.path).expect("payload-free delta index");
        assert!(!index_bytes
            .windows(b"agent-authored change".len())
            .any(|w| w == b"agent-authored change"));
        let columns: Vec<String> = db
            .prepare("PRAGMA table_info(changes)")
            .expect("columns")
            .query_map([], |r| r.get(1))
            .expect("column rows")
            .collect::<rusqlite::Result<_>>()
            .expect("column names");
        assert!(!columns
            .iter()
            .any(|c| c == "before_raw" || c == "after_raw"));
        drop(db);
        // An exact lexical reference must reject changed source bytes even when
        // an attacker leaves the source SQLite digest column unchanged.
        let source = Connection::open(&after.path).expect("tamper source");
        let mut changed = new.clone();
        changed["display_name"] = "agent-authored forgery".into();
        source
            .execute(
                "UPDATE raw_rows SET lexical=? WHERE t='entity'",
                [serde_json::to_vec(&changed).expect("changed")],
            )
            .expect("tamper bytes");
        drop(source);
        assert!(prepared.batch(0, &"d".repeat(64), "epoch", None).is_err());
        assert!(prepared.verify_unchanged().is_err());
    }
    #[test]
    fn omitted_graph_change_and_forged_original_hash_refuse_before_target_open() {
        let dir = tempfile::tempdir().expect("dir");
        let old = entity();
        let mut new = old.clone();
        new["display_name"] = "changed".into();
        let before = spool(
            &dir.path().join("s0.sqlite"),
            std::slice::from_ref(&old),
            "b",
        );
        let after = spool(&dir.path().join("s1.sqlite"), &[new.clone()], "c");
        let (delta, report) = files(dir.path(), &before, &after, &[]);
        assert!(prepare(
            &before,
            &after,
            &delta,
            &report,
            &dir.path().join("missing.sqlite")
        )
        .is_err());
        let mut forged = change(&old, &new);
        forged["before_sha256"] = "0".repeat(64).into();
        let (delta, report) = files(dir.path(), &before, &after, &[forged]);
        assert!(prepare(
            &before,
            &after,
            &delta,
            &report,
            &dir.path().join("forged.sqlite")
        )
        .is_err());
        assert_eq!(file_sha(&before.path).expect("s0"), before.inventory_sha256);
    }
    #[test]
    fn identical_snapshot_noop_binds_config_identity_and_zero_regeneration() {
        use elephant_storage::client::HistoricalCatchupSource;
        use elephant_storage::historical_adoption::{
            HistoricalRegenerationPlan, HistoricalRegenerationStep,
        };
        let dir = tempfile::tempdir().expect("directory");
        let before = spool(&dir.path().join("s0.sqlite"), &[entity()], "b");
        let (delta_path, report_path) = files(dir.path(), &before, &before, &[]);
        let delta = prepare(
            &before,
            &before,
            &delta_path,
            &report_path,
            &dir.path().join("delta.sqlite"),
        )
        .expect("complete identical delta");
        let config = dir.path().join("config.json");
        std::fs::write(&config, b"exact original configuration").expect("config");
        let source = RecordId::new_v4(TableName::Source);
        let sources = vec![HistoricalCatchupSource {
            source_id: source.clone(),
            commit_id: "original".into(),
            receipt_sha256: "c".repeat(64),
            head: elephant_types::publication::SourceHead {
                space_id: "household".into(),
                source_id: source.clone(),
                generation: "generation".into(),
                desired_revision: Some("revision".into()),
                published_revision: Some("revision".into()),
                operation_id: "operation".into(),
                lifecycle: elephant_types::publication::SourceVisibility::Active,
                updated_at: chrono::Utc::now(),
            },
        }];
        let report = serde_json::json!({"adoption":{"configuration_input_sha256":file_sha(&config).expect("hash"),"source_input_sha256":"d".repeat(64)}});
        let mut original = HistoricalRegenerationPlan {
            version: 1,
            source_plan_sha256: "d".repeat(64),
            plan_sha256: "e".repeat(64),
            steps: vec![HistoricalRegenerationStep {
                source_id: source,
                external_source_id: Some("hc-src:original".into()),
            }],
        };
        let mut plan = original.clone();
        bind_verified_noop(
            &before, &before, &delta, &report, &config, &sources, &mut plan,
        )
        .expect("verified no-op");
        assert!(plan.steps.is_empty());
        assert_ne!(plan.plan_sha256, original.plan_sha256);
        let mut altered = original.clone();
        altered.source_plan_sha256 = "f".repeat(64);
        assert!(bind_verified_noop(
            &before,
            &before,
            &delta,
            &report,
            &config,
            &sources,
            &mut altered
        )
        .is_err());
        let mut altered = original.clone();
        altered.steps[0].source_id = RecordId::new_v4(TableName::Source);
        assert!(bind_verified_noop(
            &before,
            &before,
            &delta,
            &report,
            &config,
            &sources,
            &mut altered
        )
        .is_err());
        std::fs::write(&config, b"changed configuration").expect("change");
        assert!(bind_verified_noop(
            &before,
            &before,
            &delta,
            &report,
            &config,
            &sources,
            &mut original
        )
        .is_err());
    }
    #[test]
    fn planner_deletion_requirement_needs_explicit_bound_caller_intent() {
        let dir = tempfile::tempdir().expect("directory");
        let old = entity();
        let before = spool(
            &dir.path().join("s0.sqlite"),
            std::slice::from_ref(&old),
            "b",
        );
        let after = spool(&dir.path().join("s1.sqlite"), &[], "c");
        let deletion = serde_json::json!({"table":"entity","key":old["id"],"kind":"removed","before_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&old).expect("old"))),"after_sha256":null,"destructive_intent_required":true,"physical_erase_authorized":false,"disposition":"guarded_target_row_change","advisory_metadata":{},"target_guard":{},"before":old,"after":null,"operation_id":"e".repeat(64)});
        let (path, report) = files(dir.path(), &before, &after, &[deletion]);
        let mut delta = prepare(
            &before,
            &after,
            &path,
            &report,
            &dir.path().join("delta.sqlite"),
        )
        .expect("closed deletion plan");
        assert!(delta.declare_deletions(false).is_err());
        assert!(delta.batch(0, &"d".repeat(64), "epoch", None).is_err());
        delta
            .declare_deletions(true)
            .expect("explicit caller declaration");
        let batch = delta
            .batch(0, &"d".repeat(64), "epoch", None)
            .expect("authorized exact batch");
        assert!(batch.rows[0].destructive_intent);
        batch.prepare().expect("typed guard");
        let mut reader = BufReader::new(&b"12345\n"[..]);
        assert!(
            bounded_line(&mut reader, 5).is_err(),
            "authority row bound applies before allocation"
        );
    }
    #[test]
    fn archive_group_is_indivisible_and_readonly_uri_cannot_inject_options() {
        let id = RecordId::new_v4(TableName::WorkItem);
        assert_eq!(
            group_key("work_item", &id.to_string()).expect("hot"),
            group_key(
                "work_item_archive",
                &format!("work_item_archive:{}", id.id())
            )
            .expect("archive")
        );
        assert!(group_key("work_item", &format!("entity:{}", id.id())).is_err());
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("spool?mode=rw&x.sqlite");
        std::fs::write(&path, b"fixture").expect("file");
        let uri = readonly_uri(&path).expect("URI");
        assert!(uri.contains("%3Fmode%3Drw%26x.sqlite?mode=ro&immutable=1"));
    }
}

/// Explicit catch-up is separate from fresh-target transfer. Every input remains
/// immutable, and each invocation creates a new private planning/ledger directory.
#[derive(clap::Args, Debug, Clone)]
pub struct DataMigrateDeltaArgs {
    #[arg(long)]
    pub baseline_plan: PathBuf,
    #[arg(long)]
    pub baseline_report: PathBuf,
    #[arg(long)]
    pub baseline_receipts: PathBuf,
    #[arg(long)]
    pub baseline_ledger: PathBuf,
    #[arg(long)]
    pub delta_plan: PathBuf,
    #[arg(long)]
    pub s1_plan: PathBuf,
    #[arg(long)]
    pub historical_config: PathBuf,
    #[arg(long)]
    pub scratch: PathBuf,
    #[arg(long)]
    pub report: PathBuf,
    /// Accept the exact bound S0/S1 logical deletions. Does not authorize erasure.
    #[arg(long)]
    pub accept_deletions: bool,
    #[command(flatten)]
    pub target: super::TransferTarget,
}
fn storage_error(error: impl std::fmt::Display) -> elephant_storage::StorageError {
    elephant_storage::StorageError::DataIntegrityError(format!("historical delta adapter: {error}"))
}
fn original_bindings(
    receipts: &Path,
    report: &serde_json::Value,
) -> anyhow::Result<Vec<elephant_storage::client::HistoricalCatchupSource>> {
    let expected = digest(&report["catchup_baseline"]["bindings_sha256"])?;
    ensure!(
        file_sha(receipts)? == expected,
        "original catchup authority bytes changed"
    );
    let mut reader = BufReader::new(std::fs::File::open(receipts)?);
    let mut result = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    while let Some(bytes) = bounded_line(&mut reader, 1024 * 1024)? {
        ensure!(
            bytes.len() <= 1024 * 1024 && result.len() < 100_000,
            "original authority bound"
        );
        let original: elephant_storage::client::HistoricalCatchupSource =
            serde_json::from_value(strict_json(std::str::from_utf8(&bytes)?)?)?;
        ensure!(
            original.head.space_id == report["target_space_id"]
                && original.head.source_id == original.source_id
                && seen.insert(original.source_id.clone()),
            "original authority scope or duplicate"
        );
        result.push(original);
    }
    ensure!(
        file_sha(receipts)? == expected
            && result.len() as u64
                == report["catchup_baseline"]["bindings_count"]
                    .as_u64()
                    .context("original source count")?,
        "original authority closure differs"
    );
    Ok(result)
}

fn bind_verified_noop(
    before: &SourceSpool,
    after: &SourceSpool,
    delta: &PreparedDelta,
    report: &serde_json::Value,
    configuration: &Path,
    sources: &[elephant_storage::client::HistoricalCatchupSource],
    regeneration: &mut elephant_storage::historical_adoption::HistoricalRegenerationPlan,
) -> anyhow::Result<()> {
    ensure!(
        delta.batches == 0
            && delta.changes == 0
            && before.source_sha256 == after.source_sha256
            && before.plan_id == after.plan_id
            && before.inventory_sha256 == after.inventory_sha256
            && file_sha(configuration)?
                == digest(&report["adoption"]["configuration_input_sha256"])?
            && regeneration.source_plan_sha256
                == digest(&report["adoption"]["source_input_sha256"])?
            && regeneration.steps.len() == sources.len()
            && regeneration
                .steps
                .iter()
                .map(|step| &step.source_id)
                .collect::<std::collections::BTreeSet<_>>()
                == sources
                    .iter()
                    .map(|source| &source.source_id)
                    .collect::<std::collections::BTreeSet<_>>()
            && regeneration
                .steps
                .iter()
                .all(|step| step.external_source_id.is_some()),
        "zero delta requires identical complete source, identity mapping and configuration"
    );
    regeneration.steps.clear();
    regeneration.plan_sha256 =
        hex::encode(Sha256::digest(serde_json::to_vec(&serde_json::json!([
            "historical-verified-noop-v1",
            before.source_sha256,
            before.plan_id,
            before.inventory_sha256,
            delta.original_report_sha256,
            regeneration.source_plan_sha256
        ]))?));
    Ok(())
}

#[derive(Clone, Copy)]
enum DeltaFault {
    #[cfg(test)]
    AfterArchiveCommit,
    #[cfg(test)]
    AfterArchiveDatabaseCommit,
    #[cfg(test)]
    AfterArchiveCandidate,
    None,
    #[cfg(test)]
    AfterDatabaseCommit,
    #[cfg(test)]
    AfterFirstRegenerationCommit,
    #[cfg(test)]
    AfterRuntimeCompletion,
}

pub async fn run(args: DataMigrateDeltaArgs) -> anyhow::Result<()> {
    run_inner(args, None, DeltaFault::None).await
}
async fn run_inner(
    args: DataMigrateDeltaArgs,
    opened: Option<elephant_storage::StorageClient>,
    fault: DeltaFault,
) -> anyhow::Result<()> {
    use elephant_storage::client::HistoricalCatchupInput;
    use elephant_storage::historical_adoption::PreparedHistoricalEffectIndex;
    use elephant_storage::historical_catchup_ledger::HistoricalCatchupLedger;
    let _ = fault;
    ensure!(
        !args.scratch.exists(),
        "scratch path must be new; completed report bytes may only be reproduced exactly"
    );
    let mut directory = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }
    directory.create(&args.scratch)?;
    let before = SourceSpool::open(&args.baseline_plan, &args.scratch.join("s0.sqlite"))?;
    let after = SourceSpool::open(&args.s1_plan, &args.scratch.join("s1.sqlite"))?;
    let mut delta = prepare(
        &before,
        &after,
        &args.delta_plan,
        &args.baseline_report,
        &args.scratch.join("delta.sqlite"),
    )?;
    delta.declare_deletions(args.accept_deletions)?;
    let report = header(&args.baseline_report)?;
    ensure!(
        elephant_storage::historical_catchup_ledger::valid_baseline_report(&report)
            && report["catchup_baseline"]["format"] == "recall-initial-catchup-bindings-v1"
            && report["catchup_baseline"]["native_rows_sha256"]
                == report["destination_native_rows_sha256"],
        "original reconciliation state differs"
    );
    let baseline_sha = digest(&report["catchup_baseline"]["expected_ledger_sha256"])?;
    ensure!(
        file_sha(&args.baseline_ledger)? == baseline_sha,
        "original expected SQLite bytes differ"
    );
    let adoption = super::historical_adopt::prepare_catchup(
        &after,
        &args.s1_plan,
        &args.historical_config,
        &args.scratch.join("adoption.sqlite"),
    )?;
    let sources = original_bindings(&args.baseline_receipts, &report)?;
    let mut regeneration = adoption.catchup_plan(&sources)?;
    // A new export can contain identical rows while carrying a new immutable
    // source identity. It must regenerate and close a new baseline, not reuse S0.
    let verified_noop = delta.changes == 0 && before.source_sha256 == after.source_sha256;
    if verified_noop {
        bind_verified_noop(
            &before,
            &after,
            &delta,
            &report,
            &args.historical_config,
            &sources,
            &mut regeneration,
        )?;
    }
    let migration_id = hex::encode(Sha256::digest(serde_json::to_vec(&serde_json::json!([
        "historical-delta-v1",
        delta.original_report_sha256,
        delta.delta_sha256,
        regeneration.plan_sha256,
        args.accept_deletions
    ]))?));
    let input = HistoricalCatchupInput {
        version: 1,
        journal_identity: report["catchup_baseline"]["journal_identity"]
            .as_str()
            .context("original journal absent")?
            .into(),
        migration_id: migration_id.clone(),
        source_space_id: before.space_id.clone(),
        space_id: before.target_space_id.clone(),
        before_snapshot_sha256: before.source_sha256.clone(),
        after_snapshot_sha256: after.source_sha256.clone(),
        delta_sha256: delta.delta_sha256.clone(),
        original_report_sha256: delta.original_report_sha256.clone(),
        baseline_ledger_sha256: baseline_sha,
        baseline_native_rows_sha256: digest(&report["catchup_baseline"]["native_rows_sha256"])?,
        accept_deletions: args.accept_deletions,
        expected_epoch: report["catchup_baseline"]["expected_epoch"]
            .as_str()
            .context("original epoch absent")?
            .into(),
        batches: delta.batches,
        regeneration_plan_sha256: regeneration.plan_sha256.clone(),
        regeneration_steps: regeneration.steps.len() as u64,
        sources: sources.clone(),
    };
    let summary = serde_json::json!({"format":"recall-historical-delta-report-v2","state":if verified_noop {"verified_unchanged_history"} else {"history_reconciled_current_embedding_coverage_pending"},"migration_id":migration_id,"original_report_sha256":delta.original_report_sha256,"before_snapshot_sha256":before.source_sha256,"after_snapshot_sha256":after.source_sha256,"delta_plan_sha256":delta.delta_sha256,"batches":delta.batches,"changes":delta.changes,"regeneration_plan_sha256":regeneration.plan_sha256,"regeneration_steps":regeneration.steps.len(),"semantic_index_certified":false,"accept_deletions":args.accept_deletions,"physical_erase_authorized":false});
    let mut summary = summary;
    summary["operational_disposition"] = serde_json::to_value(before.operational_disposition)?;
    summary["archived_changes"] = delta.archive_changes.into();
    summary["archive_batches"] = delta.archive_batches.into();
    summary["source_space_id"] = before.space_id.clone().into();
    summary["target_space_id"] = before.target_space_id.clone().into();
    summary["source_sha256"] = after.source_sha256.clone().into();
    summary["inventory_sha256"] = after.inventory_sha256.clone().into();
    summary["plan_id"] = after.plan_id.clone().into();
    let archived_tables = after.archive_tables();
    let native_tables = after
        .tables
        .iter()
        .filter(|(table, _)| {
            table.as_str() != "migration_history" && !after.operational_disposition.archives(table)
        })
        .map(|(table, count)| (table.clone(), *count))
        .collect();
    let global_tables = after
        .tables
        .iter()
        .filter(|(table, _)| table.as_str() == "migration_history")
        .map(|(table, count)| (table.clone(), *count))
        .collect();
    summary["source_tables"] = serde_json::to_value(&after.tables)?;
    summary["source_accounting"] = serde_json::to_value(after.operational_disposition.account(
        &after.tables,
        &native_tables,
        &archived_tables,
        &global_tables,
    )?)?;
    summary["source_accounting_basis"] = "complete immutable S1 spool and all-table diff, consumed by final native inventory plus exact archive census".into();
    summary["retained_source_controls"] =
        serde_json::json!({"migration_history":after.retained_controls});
    summary["adoption"] = serde_json::json!({"configuration_input_sha256":file_sha(&args.historical_config)?,"source_input_sha256":regeneration.source_plan_sha256});
    let original_report = args.baseline_report.clone();
    let original_ledger = args.baseline_ledger.clone();
    let ledger_copy = args.scratch.join("expected.sqlite");
    // Recheck all immutable input bindings even when custody takes the no-op
    // shortcut and therefore never opens the private mutation ledger.
    delta.verify_unchanged()?;
    before.verify_unchanged()?;
    after.verify_unchanged()?;
    ensure!(
        file_sha(&args.baseline_report)? == delta.original_report_sha256
            && file_sha(&args.baseline_ledger)? == input.baseline_ledger_sha256
            && file_sha(&args.baseline_receipts)?
                == digest(&report["catchup_baseline"]["bindings_sha256"])?,
        "original bound inputs changed before custody"
    );
    let owns_connection = opened.is_none();
    let client = match opened {
        Some(client) => client,
        None => args.target.connect().await?,
    };
    let result = async {
    let completed = client
        .with_historical_catchup_report(
            input,
            summary,
            args.baseline_report.clone(),
            args.baseline_ledger.clone(),
            move |mut authority| async move {
                delta.verify_unchanged().map_err(storage_error)?;
                let mut ledger = HistoricalCatchupLedger::open(
                    &authority,
                    &original_report,
                    &original_ledger,
                    &ledger_copy,
                )?;
                let index = PreparedHistoricalEffectIndex::prepare(&authority, &sources).await?;
                let recovered = authority.next_batch().0;
                for ordinal in 0..recovered {
                    let (epoch, previous) = authority.replay_batch_position(ordinal).await?;
                    let batch = delta
                        .batch(ordinal, &migration_id, &epoch, previous)
                        .map_err(storage_error)?
                        .prepare()?;
                    ledger.apply(authority.replay_batch_effects(&index, batch).await?)?;
                }
                while authority.next_batch().0 < delta.batches {
                    let (ordinal, previous, epoch) = authority.next_batch();
                    let batch = delta
                        .batch(ordinal, &migration_id, epoch, previous.map(str::to_owned))
                        .map_err(storage_error)?
                        .prepare()?;
                    let (_, effects) = authority.apply_batch_with_effects(&index, batch).await?;
                    #[cfg(test)]
                    if matches!(fault, DeltaFault::AfterDatabaseCommit) {
                        return Err(storage_error(
                            "injected process interruption after DB commit before ledger",
                        ));
                    }
                    ledger
                        .apply(effects.ok_or_else(|| storage_error("native manifest absent"))?)?;
                }
                delta.verify_unchanged().map_err(storage_error)?;
                before.verify_unchanged().map_err(storage_error)?;
                after.verify_unchanged().map_err(storage_error)?;
                if before.operational_disposition == OperationalDisposition::ManagedArchiveV1 {
                    use elephant_storage::historical_operational_archive::ArchiveBinding;
                    let original = ledger.original_archive().cloned().ok_or_else(|| storage_error("original archive receipt absent"))?;
                    let inventory = after.archive_inventory.as_ref().ok_or_else(|| storage_error("S1 archive source census absent"))?;
                    inventory.validate(&after).map_err(storage_error)?;
                    let binding = ArchiveBinding {
                        version: 1,
                        snapshot_sha256: after.source_sha256.clone(),
                        spool_sha256: after.inventory_sha256.clone(),
                        plan_id: after.plan_id.clone(),
                        source_space_id: after.space_id.clone(),
                        target_space_id: after.target_space_id.clone(),
                        journal_identity: original.binding.journal_identity.clone(),
                        disposition_sha256: ArchiveBinding::disposition_digest(),
                        parent_generation_sha256: Some(original.seal.file_sha256.clone()),
                        original_table_declarations: after.tables.clone(),
                    };
                    if let Some(effects) = authority.replay_archive_effects(&original, &binding).await? {
                        #[cfg(test)]
                        if matches!(fault, DeltaFault::AfterArchiveCommit) { return Err(storage_error("injected interruption after archive DB and registry commit before ledger")); }
                        ledger.apply(effects)?;
                    } else {
                        let candidate = authority.reserve_archive_successor(&original_report, binding.clone()).await?;
                        let verified = match authority.recover_sealed_archive(&candidate)? {
                            Some(verified) => verified,
                            None => delta.build_archive_successor(candidate.path(),candidate.binding().clone(),candidate.original(),&inventory.ordered_inventory_sha256).map_err(storage_error)?,
                        };
                        if verified.seal().ordered_inventory_sha256 != inventory.ordered_inventory_sha256 || verified.seal().table_counts != inventory.tables { return Err(storage_error("sealed S1 archive differs from original source census")); }
                        #[cfg(test)]
                        if matches!(fault, DeltaFault::AfterArchiveCandidate) { return Err(storage_error("injected interruption after candidate payload before registry seal")); }
                        authority.commit_archive_successor(candidate,verified,&mut ledger).await?;
                        #[cfg(test)]
                        if matches!(fault, DeltaFault::AfterArchiveDatabaseCommit) { return Err(storage_error("injected interruption after archive DB commit before registry")); }
                        let effects = authority.replay_archive_effects(&original,&binding).await?.ok_or_else(|| storage_error("committed archive effects absent"))?;
                        #[cfg(test)]
                        if matches!(fault, DeltaFault::AfterArchiveCommit) { return Err(storage_error("injected interruption after archive DB and registry commit before ledger")); }
                        ledger.apply(effects)?;
                    }
                }
                let sealed = authority.seal_for_regeneration().await?;
                let closure = Box::pin(adoption
                    .regenerate(
                        sealed,
                        &mut ledger,
                        #[cfg(test)]
                        matches!(fault, DeltaFault::AfterFirstRegenerationCommit),
                    ))
                    .await
                    .map_err(storage_error)?;
                ledger.complete(closure).await
            },
        )
        .await?;
    #[cfg(test)]
    if matches!(fault, DeltaFault::AfterRuntimeCompletion) {
        anyhow::bail!(
            "injected process interruption after runtime completion before report publication"
        );
    }
    completed.export(
        &args.report,
        &args.report.with_extension("catchup.sqlite"),
        &args.report.with_extension("catchup-bindings.jsonl"),
    )?;
    Ok::<(), anyhow::Error>(())
    }.await;
    if owns_connection {
        super::finish_owned_connection(client, result).await
    } else {
        result
    }
}

#[cfg(all(test, feature = "rocksdb"))]
#[path = "historical_delta_integration_tests.rs"]
pub mod integration_tests;

#[cfg(all(test, feature = "rocksdb"))]
#[path = "legacy_assertion_literal_delta_tests.rs"]
mod legacy_assertion_literal_delta_tests;
