//!
//! Exact original operational rows in a closed, independently verified artifact.
//! This module supplies evidence, never live dispatch, registry or erasure authority.
//! The journal owner must reserve the private path before any payload is written.

use crate::transfer_batch::{TransferRecord, MAX_ROW_BYTES};
use crate::{StorageError, StorageResult};
use elephant_types::records::{BaseFields, WorkItem, WorkRun};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

/// Closed operational class; callers cannot archive arbitrary authority tables.
pub const OPERATIONAL_TABLES: &[&str] = &[
    "audit_log",
    "cost_ledger_daily",
    "work_item",
    "work_run",
    "work_item_archive",
    "work_run_archive",
    "work_artifact",
    "work_edge",
    "work_deadletter",
    "system_event",
];
/// Maximum original rows read or appended per page.
pub const MAX_PAGE_ROWS: usize = 256;
/// Maximum cumulative original bytes accepted in a single append transaction.
pub const MAX_PAGE_BYTES: usize = 128 * 1024 * 1024;

fn invalid(message: &str) -> StorageError {
    StorageError::DataIntegrityError(format!("operational archive: {message}"))
}
fn io(error: impl std::fmt::Display) -> StorageError {
    invalid(&error.to_string())
}
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn file_sha(path: &Path) -> StorageResult<String> {
    let mut file = File::open(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut bytes = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let n = file.read(&mut bytes).map_err(io)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn safe_path(path: &Path, exists: bool) -> StorageResult<()> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(invalid(
            "managed artifact needs an absolute non-traversing path",
        ));
    }
    let mut current = PathBuf::new();
    let count = path.components().count();
    for (i, component) in path.components().enumerate() {
        current.push(component.as_os_str());
        if i + 1 == count && !exists {
            break;
        }
        let meta = std::fs::symlink_metadata(&current).map_err(io)?;
        if meta.file_type().is_symlink()
            || (i + 1 < count && !meta.is_dir())
            || (i + 1 == count && !meta.is_file())
        {
            return Err(invalid("managed path is not a regular private artifact"));
        }
    }
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        match std::fs::symlink_metadata(Path::new(&sidecar)) {
            Ok(_) => return Err(invalid("closed artifact has a SQLite sidecar")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io(error)),
        }
    }
    if exists {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = std::fs::metadata(path).map_err(io)?;
            if meta.mode() & 0o077 != 0 || meta.nlink() != 1 {
                return Err(invalid("archive must be private and have one managed link"));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileIdentity {
    bytes: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    native: (u64, u64, i64, i64),
}
impl FileIdentity {
    fn read(path: &Path) -> StorageResult<Self> {
        safe_path(path, true)?;
        let metadata = std::fs::metadata(path).map_err(io)?;
        Ok(Self {
            bytes: metadata.len(),
            modified: metadata.modified().map_err(io)?,
            #[cfg(unix)]
            native: {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            },
        })
    }
}

/// Immutable original-cut declaration. Original and target namespaces stay distinct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveBinding {
    /// Artifact binding version; currently one.
    pub version: u32,
    /// SHA256 of the complete original source snapshot.
    pub snapshot_sha256: String,
    /// SHA256 of the closed original lexical-row spool.
    pub spool_sha256: String,
    /// Original preservation plan identity.
    pub plan_id: String,
    /// Original selected namespace, never substituted into archived bytes.
    pub source_space_id: String,
    /// Explicit destination namespace projection.
    pub target_space_id: String,
    /// Identity of the independently retained destination journal.
    pub journal_identity: String,
    /// Domain-bound fixed operational-class declaration.
    pub disposition_sha256: String,
    /// Exact predecessor artifact digest, absent for the first cut.
    pub parent_generation_sha256: Option<String>,
    /// Complete original table declarations; absence differs from declared zero.
    pub original_table_declarations: BTreeMap<String, u64>,
}
impl ArchiveBinding {
    /// Digest of the fixed class and archive format; not caller-selected exclusions.
    #[must_use]
    pub fn disposition_digest() -> String {
        let mut hash = Sha256::new();
        hash.update(b"recall-managed-operational-archive-v1\0");
        for table in OPERATIONAL_TABLES {
            hash.update(table.as_bytes());
            hash.update([0]);
        }
        hex::encode(hash.finalize())
    }
    /// Validate original identity and bounded table declarations.
    pub fn validate(&self) -> StorageResult<()> {
        if self.version != 1
            || !valid_sha(&self.snapshot_sha256)
            || !valid_sha(&self.spool_sha256)
            || !valid_sha(&self.plan_id)
            || self.disposition_sha256 != Self::disposition_digest()
            || self
                .parent_generation_sha256
                .as_ref()
                .is_some_and(|h| !valid_sha(h))
            || self.source_space_id.trim().is_empty()
            || self.target_space_id.trim().is_empty()
            || self.journal_identity.trim().is_empty()
            || self.original_table_declarations.len() > 128
            || self.original_table_declarations.keys().any(|name| {
                name.is_empty()
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            })
        {
            return Err(invalid("binding is malformed"));
        }
        Ok(())
    }
    /// Exact table/count partition represented by this archive, including zero.
    #[must_use]
    pub fn archived_tables(&self) -> BTreeMap<String, u64> {
        self.original_table_declarations
            .iter()
            .filter(|(table, _)| OPERATIONAL_TABLES.contains(&table.as_str()))
            .map(|(table, count)| (table.clone(), *count))
            .collect()
    }
    /// Domain-separated immutable declaration identity.
    pub fn digest(&self) -> StorageResult<String> {
        self.validate()?;
        let mut bytes = b"recall-archive-binding-v1\0".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(io)?);
        Ok(sha(&bytes))
    }
}

/// Original per-namespace event population and maximum; no delivery is implied.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEventTail {
    /// Number of retained original events in this namespace.
    pub rows: u64,
    /// Highest nonnegative original sequence; future target allocation must exceed it.
    pub max_seq: i64,
}

/// Exact closed artifact evidence. A seal alone grants no archive access or readiness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSeal {
    /// Exact binding digest.
    pub binding_sha256: String,
    /// SHA256 of every byte in the closed SQLite main file.
    pub file_sha256: String,
    /// Domain-separated ordered original table/id/lexical hash/length digest.
    pub ordered_inventory_sha256: String,
    /// Actual closed file length.
    pub bytes: u64,
    /// Original archived populations, including explicitly declared empty tables.
    pub table_counts: BTreeMap<String, u64>,
    /// Verified original event tails by original namespace.
    pub event_tails: BTreeMap<String, ArchiveEventTail>,
    /// Exact ordered source-change/tombstone receipt digest, with no removed payload.
    pub changes_sha256: String,
    /// True only for an explicit survivor artifact; original declarations remain intact.
    pub redacted: bool,
}

/// One original row. Bytes, namespace and hash refer to the source representation.
#[derive(Clone, Debug)]
pub struct ArchiveRow {
    /// Exact original table.
    pub table: String,
    /// Exact typed original row identifier.
    pub id: String,
    /// Original JSON UTF-8 bytes, never rewritten for the destination namespace.
    pub lexical: Vec<u8>,
    /// SHA256 of these exact bytes.
    pub lexical_sha256: String,
}

/// Verified original row metadata, derived from its strict typed representation.
#[derive(Clone, Debug)]
pub struct ArchiveRowMetadata {
    /// Original row namespace.
    pub original_space: String,
    /// Original soft-delete state.
    pub deleted: bool,
    /// Recognized typed identity links; not a declaration of source ownership.
    pub references: BTreeSet<String>,
    /// Original event sequence, only for a system event.
    pub event_sequence: Option<i64>,
}
fn base_refs(base: &BaseFields, refs: &mut BTreeSet<String>) {
    refs.extend(base.provenance.sources.iter().map(ToString::to_string));
    refs.extend(base.provenance.work_run_id.iter().map(ToString::to_string));
    refs.extend(base.subjects.iter().map(|link| link.id().to_string()));
}
fn work_refs(work: &WorkItem, refs: &mut BTreeSet<String>) {
    base_refs(&work.base, refs);
}
fn run_refs(run: &WorkRun, refs: &mut BTreeSet<String>) {
    base_refs(&run.base, refs);
    refs.insert(run.work_item.id().to_string());
}
impl ArchiveRow {
    /// Validate exact bytes and recognized operational type without a database write.
    pub fn metadata(&self) -> StorageResult<ArchiveRowMetadata> {
        if !OPERATIONAL_TABLES.contains(&self.table.as_str())
            || self.lexical.is_empty()
            || self.lexical.len() > MAX_ROW_BYTES
            || sha(&self.lexical) != self.lexical_sha256
        {
            return Err(invalid("row table/bytes/hash differs"));
        }
        let record = TransferRecord::from_json(
            &self.table,
            std::str::from_utf8(&self.lexical).map_err(io)?,
        )?;
        if record.id().to_string() != self.id {
            return Err(invalid("row identity differs"));
        }
        let mut refs = BTreeSet::new();
        base_refs(record.base(), &mut refs);
        let mut seq = None;
        match &record {
            TransferRecord::AuditLog(row) => {
                refs.insert(row.record.to_string());
            }
            TransferRecord::CostLedgerDaily(_) => {}
            TransferRecord::WorkItem(row) => work_refs(row, &mut refs),
            TransferRecord::WorkRun(row) => run_refs(row, &mut refs),
            TransferRecord::WorkItemArchive(row) => {
                refs.insert(row.original_id.to_string());
                work_refs(&row.record, &mut refs);
            }
            TransferRecord::WorkRunArchive(row) => {
                refs.insert(row.original_id.to_string());
                run_refs(&row.record, &mut refs);
            }
            TransferRecord::WorkArtifact(row) => {
                refs.insert(row.work_run.id().to_string());
            }
            TransferRecord::WorkEdge(row) => {
                refs.insert(row.from.id().to_string());
                refs.insert(row.to.id().to_string());
            }
            TransferRecord::WorkDeadletter(row) => {
                refs.insert(row.work_item.id().to_string());
                refs.extend(row.runs.iter().map(|link| link.id().to_string()));
            }
            TransferRecord::SystemEvent(row) if row.seq >= 0 => seq = Some(row.seq),
            _ => return Err(invalid("unsupported operational row or event sequence")),
        }
        Ok(ArchiveRowMetadata {
            original_space: record.base().space_id.clone(),
            deleted: record.base().deleted_at.is_some(),
            references: refs,
            event_sequence: seq,
        })
    }
}

const SCHEMA: &str = "PRAGMA trusted_schema=OFF; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
CREATE TABLE metadata(k TEXT PRIMARY KEY,v TEXT NOT NULL);
CREATE TABLE rows(t TEXT NOT NULL,k TEXT NOT NULL,space TEXT NOT NULL,lexical BLOB NOT NULL,h TEXT NOT NULL,n INTEGER NOT NULL,PRIMARY KEY(t,k));
CREATE TABLE recognized_refs(t TEXT NOT NULL,k TEXT NOT NULL,r TEXT NOT NULL,PRIMARY KEY(t,k,r));
CREATE INDEX reverse_refs ON recognized_refs(r,t,k);
CREATE TABLE changes(t TEXT NOT NULL,k TEXT NOT NULL,before_hash TEXT,after_hash TEXT,PRIMARY KEY(t,k));";

/// Private candidate builder. The journal owner reserves its path before creation.
pub struct ArchiveBuilder {
    db: Connection,
    path: PathBuf,
    binding: ArchiveBinding,
}
impl ArchiveBuilder {
    /// Create a new private candidate. Never replaces an existing artifact.
    pub fn create(path: &Path, binding: ArchiveBinding) -> StorageResult<Self> {
        binding.validate()?;
        safe_path(path, false)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path).map_err(io)?.sync_all().map_err(io)?;
        let db = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(io)?;
        db.execute_batch(SCHEMA).map_err(io)?;
        db.execute(
            "INSERT INTO metadata VALUES ('binding',?)",
            [serde_json::to_string(&binding).map_err(io)?],
        )
        .map_err(io)?;
        Ok(Self {
            db,
            path: path.into(),
            binding,
        })
    }
    /// Append a bounded create-only page. Any failure rolls back the whole page.
    pub fn append(&mut self, rows: &[ArchiveRow]) -> StorageResult<()> {
        if rows.is_empty() || rows.len() > MAX_PAGE_ROWS {
            return Err(invalid("page row bound"));
        }
        let mut bytes = 0usize;
        let mut prepared = Vec::with_capacity(rows.len());
        for row in rows {
            bytes = bytes
                .checked_add(row.lexical.len())
                .ok_or_else(|| invalid("page byte overflow"))?;
            if bytes > MAX_PAGE_BYTES || !self.binding.archived_tables().contains_key(&row.table) {
                return Err(invalid("page bytes or undeclared table"));
            }
            prepared.push(row.metadata()?);
        }
        let tx = self.db.transaction().map_err(io)?;
        for (row, meta) in rows.iter().zip(&prepared) {
            tx.execute(
                "INSERT INTO rows VALUES (?,?,?,?,?,?)",
                params![
                    row.table,
                    row.id,
                    meta.original_space,
                    row.lexical,
                    row.lexical_sha256,
                    row.lexical.len()
                ],
            )
            .map_err(io)?;
            for reference in &meta.references {
                tx.execute(
                    "INSERT INTO recognized_refs VALUES (?,?,?)",
                    params![row.table, row.id, reference],
                )
                .map_err(io)?;
            }
        }
        tx.commit().map_err(io)
    }
    /// Close, synchronize and fully verify a candidate before registry publication.
    pub fn seal(self) -> StorageResult<VerifiedArchive> {
        let Self { db, path, binding } = self;
        db.close().map_err(|(_, error)| io(error))?;
        File::open(&path).map_err(io)?.sync_all().map_err(io)?;
        crate::durability_io::sync_directory(
            path.parent().ok_or_else(|| invalid("archive parent"))?,
        )?;
        verify_candidate(&path, &binding, None)
    }
}

/// Fully checked immutable artifact evidence. It is not a live access capability.
pub struct VerifiedArchive {
    path: PathBuf,
    binding: ArchiveBinding,
    seal: ArchiveSeal,
    identity: FileIdentity,
}
impl VerifiedArchive {
    /// Verify every original row and exact file against an expected sealed binding.
    pub fn open(path: &Path, binding: &ArchiveBinding, seal: &ArchiveSeal) -> StorageResult<Self> {
        verify_candidate(path, binding, Some(seal))
    }
    /// Borrow the immutable original declaration.
    #[must_use]
    pub const fn binding(&self) -> &ArchiveBinding {
        &self.binding
    }
    /// Borrow verified evidence for registration or exact recovery.
    #[must_use]
    pub const fn seal(&self) -> &ArchiveSeal {
        &self.seal
    }
    /// Borrow the exact private candidate path; callers must enforce journal custody.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Read a bounded original page. A current journal gate is still required by callers.
    pub fn page(
        &self,
        table: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<ArchiveRow>> {
        if limit == 0 || limit > MAX_PAGE_ROWS || !self.seal.table_counts.contains_key(table) {
            return Err(invalid("archive page scope"));
        }
        self.check_identity()?;
        let db = open_readonly(&self.path)?;
        let rows = read_page(&db, table, after, limit)?;
        for row in &rows {
            validate_row(&db, row)?;
        }
        self.check_identity()?;
        Ok(rows)
    }
    /// Read one exact original row without restoring or dispatching it.
    pub fn row(&self, table: &str, id: &str) -> StorageResult<Option<ArchiveRow>> {
        if !self.seal.table_counts.contains_key(table) {
            return Err(invalid("archive lookup scope"));
        }
        self.check_identity()?;
        let db = open_readonly(&self.path)?;
        let row = read_exact(&db, table, id)?;
        if let Some(row) = &row {
            validate_row(&db, row)?;
        }
        self.check_identity()?;
        Ok(row)
    }
    /// Verify the complete immutable predecessor/current diff against retained CAS
    /// receipts. A caller cannot obtain deletion authority by sealing a new file
    /// directly; ordinary removals and newly soft-deleted rows require intent.
    /// This validates preservation, not live journal custody.
    pub fn verify_successor(&self, original: &Self, accept_deletions: bool) -> StorageResult<()> {
        self.check_identity()?;
        original.check_identity()?;
        if self.seal.redacted
            || original.seal.redacted
            || self.binding.parent_generation_sha256.as_ref() != Some(&original.seal.file_sha256)
            || self.binding.journal_identity != original.binding.journal_identity
            || self.binding.source_space_id != original.binding.source_space_id
            || self.binding.target_space_id != original.binding.target_space_id
            || self.binding.disposition_sha256 != original.binding.disposition_sha256
        {
            return Err(invalid("successor predecessor binding differs"));
        }
        let db = open_readonly(&self.path)?;
        db.execute(
            "ATTACH DATABASE ? AS predecessor",
            [original
                .path
                .to_str()
                .ok_or_else(|| invalid("archive path is not UTF-8"))?],
        )
        .map_err(io)?;
        // Indexed joins validate every difference and reject spurious, omitted or
        // stale CAS receipts without materializing the population in memory.
        let bad:u64=db.query_row("SELECT count(*) FROM changes c LEFT JOIN predecessor.rows b ON b.t=c.t AND b.k=c.k LEFT JOIN rows a ON a.t=c.t AND a.k=c.k WHERE c.before_hash IS NOT b.h OR c.after_hash IS NOT a.h OR b.h IS a.h",[],|r|r.get(0)).map_err(io)?;
        let missing:u64=db.query_row("SELECT count(*) FROM (SELECT b.t,b.k FROM predecessor.rows b LEFT JOIN rows a ON a.t=b.t AND a.k=b.k WHERE b.h IS NOT a.h UNION SELECT a.t,a.k FROM rows a LEFT JOIN predecessor.rows b ON b.t=a.t AND b.k=a.k WHERE b.k IS NULL) d LEFT JOIN changes c ON c.t=d.t AND c.k=d.k WHERE c.k IS NULL",[],|r|r.get(0)).map_err(io)?;
        if bad != 0 || missing != 0 {
            return Err(invalid(
                "successor changes do not close the original population",
            ));
        }
        if !accept_deletions {
            let mut statement = db
                .prepare("SELECT t,k,before_hash,after_hash FROM changes ORDER BY t,k")
                .map_err(io)?;
            let mut rows = statement.query([]).map_err(io)?;
            while let Some(row) = rows.next().map_err(io)? {
                let before: Option<String> = row.get(2).map_err(io)?;
                if before.is_none() {
                    continue;
                }
                let after: Option<String> = row.get(3).map_err(io)?;
                if after.is_none() {
                    return Err(invalid("successor removal lacks caller intent"));
                }
                let table: String = row.get(0).map_err(io)?;
                let id: String = row.get(1).map_err(io)?;
                let prior = original
                    .row(&table, &id)?
                    .ok_or_else(|| invalid("successor prior row absent"))?;
                let next = self
                    .row(&table, &id)?
                    .ok_or_else(|| invalid("successor row absent"))?;
                if !prior.metadata()?.deleted && next.metadata()?.deleted {
                    return Err(invalid("successor soft-delete lacks caller intent"));
                }
            }
        }
        db.close().map_err(|(_, e)| io(e))?;
        self.check_identity()?;
        original.check_identity()
    }
    fn check_identity(&self) -> StorageResult<()> {
        if FileIdentity::read(&self.path)? != self.identity {
            return Err(invalid("verified archive file identity changed"));
        }
        Ok(())
    }
}
fn open_readonly(path: &Path) -> StorageResult<Connection> {
    safe_path(path, true)?;
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(io)?;
    db.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON;")
        .map_err(io)?;
    Ok(db)
}
fn read_page(
    db: &Connection,
    table: &str,
    after: Option<&str>,
    limit: usize,
) -> StorageResult<Vec<ArchiveRow>> {
    let sql = if after.is_some() {
        "SELECT k,lexical,h,n FROM rows WHERE t=?1 AND k>?2 ORDER BY k LIMIT ?3"
    } else {
        "SELECT k,lexical,h,n FROM rows WHERE t=?1 AND ?2 IS NULL ORDER BY k LIMIT ?3"
    };
    let mut query = db.prepare(sql).map_err(io)?;
    let mut cursor = query.query(params![table, after, limit]).map_err(io)?;
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = cursor.next().map_err(io)? {
        let n: usize = row.get(3).map_err(io)?;
        if n > MAX_ROW_BYTES {
            return Err(invalid("archive row byte bound"));
        }
        let blob = row.get_ref(1).map_err(io)?.as_blob().map_err(io)?;
        if blob.len() != n {
            return Err(invalid("archive row length differs"));
        }
        if !rows.is_empty() && bytes.checked_add(n).is_none_or(|n| n > MAX_PAGE_BYTES) {
            break;
        }
        bytes = bytes
            .checked_add(n)
            .ok_or_else(|| invalid("archive page overflow"))?;
        rows.push(ArchiveRow {
            table: table.into(),
            id: row.get(0).map_err(io)?,
            lexical: blob.to_vec(),
            lexical_sha256: row.get(2).map_err(io)?,
        });
    }
    Ok(rows)
}
fn verify_candidate(
    path: &Path,
    binding: &ArchiveBinding,
    expected: Option<&ArchiveSeal>,
) -> StorageResult<VerifiedArchive> {
    binding.validate()?;
    safe_path(path, true)?;
    let identity = FileIdentity::read(path)?;
    let original_hash = file_sha(path)?;
    if expected.is_some_and(|seal| seal.file_sha256 != original_hash) {
        return Err(invalid("file hash differs"));
    }
    let db = open_readonly(path)?;
    validate_schema(&db)?;
    let (redacted, populations) = verified_populations(&db, binding)?;
    let mut actual = populations.clone();
    for count in actual.values_mut() {
        *count = 0;
    }
    let mut inventory = ArchiveInventoryDigest::default();
    let mut tails = BTreeMap::<String, ArchiveEventTail>::new();
    for (table, expected_count) in populations.clone() {
        inventory.table(&table, expected_count)?;
        let mut cursor = None;
        loop {
            let rows = read_page(&db, &table, cursor.as_deref(), MAX_PAGE_ROWS)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let meta = validate_row(&db, row)?;
                if let Some(seq) = meta.event_sequence {
                    let tail = tails.entry(meta.original_space).or_default();
                    tail.rows = tail
                        .rows
                        .checked_add(1)
                        .ok_or_else(|| invalid("event count overflow"))?;
                    tail.max_seq = tail.max_seq.max(seq);
                }
                let count = actual
                    .get_mut(&table)
                    .ok_or_else(|| invalid("undeclared archive table"))?;
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| invalid("row count overflow"))?;
                inventory.row(&row.table, &row.id, &row.lexical_sha256, row.lexical.len())?;
            }
            cursor = rows.last().map(|row| row.id.clone());
        }
    }
    let total: u64 = db
        .query_row("SELECT count(*) FROM rows", [], |r| r.get(0))
        .map_err(io)?;
    let expected_total = actual
        .values()
        .try_fold(0u64, |n, v| n.checked_add(*v))
        .ok_or_else(|| invalid("count overflow"))?;
    let extras:u64=db.query_row("SELECT count(*) FROM recognized_refs x WHERE NOT EXISTS(SELECT 1 FROM rows r WHERE r.t=x.t AND r.k=x.k)",[],|r|r.get(0)).map_err(io)?;
    if actual != populations || total != expected_total || extras != 0 {
        return Err(invalid("complete archive population differs"));
    }
    let changes_sha256 = changes_digest(&db)?;
    db.close().map_err(|(_, error)| io(error))?;
    let seal = ArchiveSeal {
        binding_sha256: binding.digest()?,
        file_sha256: original_hash.clone(),
        ordered_inventory_sha256: inventory.finish()?,
        bytes: std::fs::metadata(path).map_err(io)?.len(),
        table_counts: actual,
        event_tails: tails,
        changes_sha256,
        redacted,
    };
    if FileIdentity::read(path)? != identity
        || file_sha(path)? != original_hash
        || expected.is_some_and(|expected| expected != &seal)
    {
        return Err(invalid("sealed artifact changed"));
    }
    Ok(VerifiedArchive {
        path: path.into(),
        binding: binding.clone(),
        seal,
        identity,
    })
}

fn verified_populations(
    db: &Connection,
    binding: &ArchiveBinding,
) -> StorageResult<(bool, BTreeMap<String, u64>)> {
    let metadata_count: u64 = db
        .query_row("SELECT count(*) FROM metadata", [], |r| r.get(0))
        .map_err(io)?;
    let binding_bytes: usize = db
        .query_row(
            "SELECT length(CAST(v AS BLOB)) FROM metadata WHERE k='binding'",
            [],
            |r| r.get(0),
        )
        .map_err(io)?;
    let redaction: Option<String> = db
        .query_row("SELECT v FROM metadata WHERE k='redaction'", [], |r| {
            r.get(0)
        })
        .optional()
        .map_err(io)?;
    let redacted = redaction.as_deref() == Some("true");
    if redaction.as_deref().is_some_and(|value| value != "true")
        || metadata_count != if redacted { 2 } else { 1 }
        || binding_bytes > 64 * 1024
    {
        return Err(invalid("metadata scope or byte bound differs"));
    }
    let stored: String = db
        .query_row("SELECT v FROM metadata WHERE k='binding'", [], |r| r.get(0))
        .map_err(io)?;
    let stored: ArchiveBinding =
        serde_json::from_value(crate::transfer_batch::strict_json(&stored)?).map_err(io)?;
    if &stored != binding {
        return Err(invalid("stored binding differs"));
    }
    let mut populations = binding.archived_tables();
    if redacted {
        if binding.parent_generation_sha256.is_none() {
            return Err(invalid("redacted artifact lacks predecessor"));
        }
        let invalid_changes: u64 = db
            .query_row(
                "SELECT count(*) FROM changes WHERE before_hash IS NULL OR after_hash IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .map_err(io)?;
        if invalid_changes != 0 {
            return Err(invalid("redaction has non-removal changes"));
        }
        for (table, count) in &mut populations {
            let removed: u64 = db
                .query_row("SELECT count(*) FROM changes WHERE t=?", [table], |r| {
                    r.get(0)
                })
                .map_err(io)?;
            *count = count
                .checked_sub(removed)
                .ok_or_else(|| invalid("redaction removes undeclared rows"))?;
        }
    }
    Ok((redacted, populations))
}

fn validate_schema(db: &Connection) -> StorageResult<()> {
    type SchemaRow = (String, String, String, Option<String>);
    fn schema(db: &Connection) -> StorageResult<Vec<SchemaRow>> {
        let mut stmt = db
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
            .map_err(io)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .map_err(io)?
            .collect::<Result<_, _>>()
            .map_err(io);
        rows
    }
    let expected = Connection::open_in_memory().map_err(io)?;
    expected.execute_batch(SCHEMA).map_err(io)?;
    if schema(db)? != schema(&expected)? {
        return Err(invalid("unexpected SQLite schema"));
    }
    Ok(())
}
fn read_exact(db: &Connection, table: &str, id: &str) -> StorageResult<Option<ArchiveRow>> {
    let mut query = db
        .prepare("SELECT lexical,h,n FROM rows WHERE t=? AND k=?")
        .map_err(io)?;
    let mut cursor = query.query(params![table, id]).map_err(io)?;
    let Some(row) = cursor.next().map_err(io)? else {
        return Ok(None);
    };
    let n: usize = row.get(2).map_err(io)?;
    if n == 0 || n > MAX_ROW_BYTES {
        return Err(invalid("archive lookup byte bound"));
    }
    let blob = row.get_ref(0).map_err(io)?.as_blob().map_err(io)?;
    if blob.len() != n {
        return Err(invalid("archive lookup length differs"));
    }
    Ok(Some(ArchiveRow {
        table: table.into(),
        id: id.into(),
        lexical: blob.to_vec(),
        lexical_sha256: row.get(1).map_err(io)?,
    }))
}
fn validate_row(db: &Connection, row: &ArchiveRow) -> StorageResult<ArchiveRowMetadata> {
    let metadata = row.metadata()?;
    let stored_space: String = db
        .query_row(
            "SELECT space FROM rows WHERE t=? AND k=?",
            params![row.table, row.id],
            |r| r.get(0),
        )
        .map_err(io)?;
    if stored_space != metadata.original_space {
        return Err(invalid("stored original namespace differs"));
    }
    let mut statement = db
        .prepare_cached("SELECT r FROM recognized_refs WHERE t=? AND k=? ORDER BY r")
        .map_err(io)?;
    let stored_refs = statement
        .query_map(params![row.table, row.id], |r| r.get::<_, String>(0))
        .map_err(io)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(io)?;
    if stored_refs != metadata.references {
        return Err(invalid("recognized references differ"));
    }
    Ok(metadata)
}

/// Streaming exact source-inventory digest shared by the spool and archive readers.
///
/// Feed tables in lexical order, including declared zero counts, then rows in ID
/// order. The domain and JSON tuple framing match `ArchiveSeal` exactly.
pub struct ArchiveInventoryDigest {
    hash: Sha256,
    table: Option<String>,
    last_id: Option<String>,
    expected: u64,
    rows: u64,
}
impl Default for ArchiveInventoryDigest {
    fn default() -> Self {
        let mut hash = Sha256::new();
        hash.update(b"recall-operational-archive-rows-v1\0");
        Self {
            hash,
            table: None,
            last_id: None,
            expected: 0,
            rows: 0,
        }
    }
}
impl ArchiveInventoryDigest {
    /// Begin the next declared operational table, preserving explicit empty tables.
    pub fn table(&mut self, table: &str, count: u64) -> StorageResult<()> {
        if !OPERATIONAL_TABLES.contains(&table)
            || self.rows != self.expected
            || self.table.as_deref().is_some_and(|last| last >= table)
        {
            return Err(invalid("inventory table order/count differs"));
        }
        self.hash
            .update(serde_json::to_vec(&(table, count)).map_err(io)?);
        self.table = Some(table.into());
        self.last_id = None;
        self.rows = 0;
        self.expected = count;
        Ok(())
    }
    /// Add one independently observed exact lexical identity/hash/byte length.
    pub fn row(&mut self, table: &str, id: &str, hash: &str, bytes: usize) -> StorageResult<()> {
        if self.table.as_deref() != Some(table)
            || self.last_id.as_deref().is_some_and(|last| last >= id)
            || !valid_sha(hash)
            || bytes == 0
            || bytes > MAX_ROW_BYTES
            || self.rows >= self.expected
        {
            return Err(invalid("inventory original row order/hash/count differs"));
        }
        self.hash
            .update(serde_json::to_vec(&(table, id, hash, bytes)).map_err(io)?);
        self.last_id = Some(id.into());
        self.rows += 1;
        Ok(())
    }
    /// Finish only after the declared final population was supplied exactly.
    pub fn finish(self) -> StorageResult<String> {
        if self.rows != self.expected {
            return Err(invalid("incomplete archive inventory"));
        }
        Ok(hex::encode(self.hash.finalize()))
    }
}

/// Exact operation-scoped source change. Intent is supplied separately from the
/// fact that a removal or soft deletion requires it; this grants no physical erase.
#[derive(Clone, Debug)]
pub struct ArchiveChange {
    /// Exact original operational table.
    pub table: String,
    /// Exact original row identifier.
    pub id: String,
    /// Original lexical before-image hash, or absence for an actual creation.
    pub before_sha256: Option<String>,
    /// Complete successor original bytes, or logical removal from the current cut.
    pub after: Option<ArchiveRow>,
    /// Explicit caller declaration covering this logical deletion or soft deletion.
    pub logical_deletion_authorized: bool,
}

fn insert_row(
    db: &Connection,
    row: &ArchiveRow,
    metadata: &ArchiveRowMetadata,
) -> StorageResult<()> {
    db.execute(
        "INSERT INTO rows VALUES (?,?,?,?,?,?)",
        params![
            row.table,
            row.id,
            metadata.original_space,
            row.lexical,
            row.lexical_sha256,
            row.lexical.len()
        ],
    )
    .map_err(io)?;
    let mut refs = db
        .prepare_cached("INSERT INTO recognized_refs VALUES (?,?,?)")
        .map_err(io)?;
    for reference in &metadata.references {
        refs.execute(params![row.table, row.id, reference])
            .map_err(io)?;
    }
    Ok(())
}
impl ArchiveBuilder {
    /// Copy a verified predecessor into a new reserved candidate using bounded pages.
    /// The complete new declaration is checked at seal; only this builder permits
    /// temporary old rows whose table is absent from the successor declaration.
    pub fn successor(
        path: &Path,
        binding: ArchiveBinding,
        original: &VerifiedArchive,
    ) -> StorageResult<Self> {
        if original.seal.redacted {
            return Err(invalid(
                "historical catchup after physical redaction is unsupported",
            ));
        }
        Self::copy_original(path, binding, original)
    }
    fn copy_original(
        path: &Path,
        binding: ArchiveBinding,
        original: &VerifiedArchive,
    ) -> StorageResult<Self> {
        if binding.parent_generation_sha256.as_deref() != Some(original.seal.file_sha256.as_str())
            || binding.source_space_id != original.binding.source_space_id
            || binding.target_space_id != original.binding.target_space_id
            || binding.journal_identity != original.binding.journal_identity
            || binding.disposition_sha256 != original.binding.disposition_sha256
        {
            return Err(invalid(
                "successor does not bind the original managed generation",
            ));
        }
        original.check_identity()?;
        let mut builder = Self::create(path, binding)?;
        for table in original.seal.table_counts.keys() {
            let mut cursor = None;
            loop {
                let rows = original.page(table, cursor.as_deref(), MAX_PAGE_ROWS)?;
                if rows.is_empty() {
                    break;
                }
                let tx = builder.db.transaction().map_err(io)?;
                for row in &rows {
                    insert_row(&tx, row, &row.metadata()?)?;
                }
                tx.commit().map_err(io)?;
                cursor = rows.last().map(|row| row.id.clone());
            }
        }
        original.check_identity()?;
        Ok(builder)
    }
    /// Apply one bounded exact-CAS page. Duplicate changes, stale rows and denied
    /// deletions roll back the whole page; old artifacts remain unchanged.
    pub fn apply(&mut self, changes: &[ArchiveChange]) -> StorageResult<()> {
        if self.binding.parent_generation_sha256.is_none()
            || changes.is_empty()
            || changes.len() > MAX_PAGE_ROWS
        {
            return Err(invalid("successor change page scope"));
        }
        let mut bytes = 0usize;
        for change in changes {
            if !OPERATIONAL_TABLES.contains(&change.table.as_str())
                || change.before_sha256.as_ref().is_some_and(|h| !valid_sha(h))
                || (change.before_sha256.is_none() && change.after.is_none())
            {
                return Err(invalid("source change identity or before-image"));
            }
            if let Some(after) = &change.after {
                if after.table != change.table
                    || after.id != change.id
                    || !self.binding.archived_tables().contains_key(&change.table)
                {
                    return Err(invalid("source after-image identity differs"));
                }
                after.metadata()?;
                bytes = bytes
                    .checked_add(after.lexical.len())
                    .ok_or_else(|| invalid("change byte overflow"))?;
            }
            if bytes > MAX_PAGE_BYTES {
                return Err(invalid("change page byte bound"));
            }
        }
        let redacted: u64 = self
            .db
            .query_row(
                "SELECT count(*) FROM metadata WHERE k='redaction'",
                [],
                |r| r.get(0),
            )
            .map_err(io)?;
        if redacted != 0 {
            return Err(invalid("redacted candidate cannot accept source changes"));
        }
        let tx = self.db.transaction().map_err(io)?;
        for change in changes {
            let original = read_exact(&tx, &change.table, &change.id)?;
            if original.as_ref().map(|row| &row.lexical_sha256) != change.before_sha256.as_ref() {
                return Err(invalid("source change before-image differs"));
            }
            let original_meta = original.as_ref().map(ArchiveRow::metadata).transpose()?;
            let after_meta = change
                .after
                .as_ref()
                .map(ArchiveRow::metadata)
                .transpose()?;
            let deleting = original.is_some()
                && (change.after.is_none()
                    || original_meta.as_ref().is_some_and(|m| !m.deleted)
                        && after_meta.as_ref().is_some_and(|m| m.deleted));
            if deleting && !change.logical_deletion_authorized {
                return Err(invalid("logical deletion intent missing"));
            }
            tx.execute(
                "INSERT INTO changes VALUES (?,?,?,?)",
                params![
                    change.table,
                    change.id,
                    change.before_sha256,
                    change.after.as_ref().map(|r| &r.lexical_sha256)
                ],
            )
            .map_err(io)?;
            tx.execute(
                "DELETE FROM recognized_refs WHERE t=? AND k=?",
                params![change.table, change.id],
            )
            .map_err(io)?;
            tx.execute(
                "DELETE FROM rows WHERE t=? AND k=?",
                params![change.table, change.id],
            )
            .map_err(io)?;
            if let (Some(after), Some(metadata)) = (&change.after, &after_meta) {
                insert_row(&tx, after, metadata)?;
            }
        }
        tx.commit().map_err(io)
    }
    /// Build a fresh survivor-only redaction candidate under separate journal
    /// erasure custody. The callback uses its bound exact selection and receives
    /// original typed metadata; returning true excludes that row before any of its
    /// payload reaches the new SQLite file. This does not publish, erase old files,
    /// or confer physical-erasure authority. All prior tombstones are preserved.
    pub fn redact<F>(
        path: &Path,
        original: &VerifiedArchive,
        mut remove: F,
    ) -> StorageResult<VerifiedArchive>
    where
        F: FnMut(&ArchiveRow, &ArchiveRowMetadata) -> StorageResult<bool>,
    {
        let mut binding = original.binding.clone();
        binding.parent_generation_sha256 = Some(original.seal.file_sha256.clone());
        original.check_identity()?;
        let mut builder = Self::create(path, binding)?;
        builder
            .db
            .execute("INSERT INTO metadata VALUES ('redaction','true')", [])
            .map_err(io)?;
        if original.seal.redacted {
            let source = open_readonly(original.path())?;
            let mut statement = source
                .prepare("SELECT t,k,before_hash FROM changes ORDER BY t,k")
                .map_err(io)?;
            let mut cursor = statement.query([]).map_err(io)?;
            while let Some(row) = cursor.next().map_err(io)? {
                builder
                    .db
                    .execute(
                        "INSERT INTO changes VALUES (?,?,?,NULL)",
                        params![
                            row.get::<_, String>(0).map_err(io)?,
                            row.get::<_, String>(1).map_err(io)?,
                            row.get::<_, String>(2).map_err(io)?
                        ],
                    )
                    .map_err(io)?;
            }
        }
        for table in original.seal.table_counts.keys() {
            let mut cursor = None;
            loop {
                let rows = original.page(table, cursor.as_deref(), MAX_PAGE_ROWS)?;
                if rows.is_empty() {
                    break;
                }
                let tx = builder.db.transaction().map_err(io)?;
                for row in &rows {
                    let metadata = row.metadata()?;
                    if remove(row, &metadata)? {
                        tx.execute(
                            "INSERT INTO changes VALUES (?,?,?,NULL)",
                            params![row.table, row.id, row.lexical_sha256],
                        )
                        .map_err(io)?;
                    } else {
                        insert_row(&tx, row, &metadata)?;
                    }
                }
                tx.commit().map_err(io)?;
                cursor = rows.last().map(|row| row.id.clone());
            }
        }
        original.check_identity()?;
        builder.seal()
    }
}
fn changes_digest(db: &Connection) -> StorageResult<String> {
    let mut hash = Sha256::new();
    hash.update(b"recall-operational-archive-changes-v1\0");
    let mut query = db
        .prepare("SELECT t,k,before_hash,after_hash FROM changes ORDER BY t,k")
        .map_err(io)?;
    let mut cursor = query.query([]).map_err(io)?;
    while let Some(row) = cursor.next().map_err(io)? {
        let table: String = row.get(0).map_err(io)?;
        let id: String = row.get(1).map_err(io)?;
        let before: Option<String> = row.get(2).map_err(io)?;
        let after: Option<String> = row.get(3).map_err(io)?;
        if !OPERATIONAL_TABLES.contains(&table.as_str())
            || before.is_none() && after.is_none()
            || before.as_ref().is_some_and(|h| !valid_sha(h))
            || after.as_ref().is_some_and(|h| !valid_sha(h))
        {
            return Err(invalid("malformed source-change receipt"));
        }
        let current: Option<String> = db
            .query_row(
                "SELECT h FROM rows WHERE t=? AND k=?",
                params![table, id],
                |r| r.get(0),
            )
            .optional()
            .map_err(io)?;
        if current != after {
            return Err(invalid("source-change after-image differs from archive"));
        }
        hash.update(serde_json::to_vec(&(table, id, before, after)).map_err(io)?);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_types::records::{AuditAction, AuditLog};
    use elephant_types::RecordId;

    fn original(id: &str, marker: &str) -> StorageResult<ArchiveRow> {
        let record = AuditLog {
            base: BaseFields::new(
                RecordId::new(
                    elephant_types::TableName::AuditLog,
                    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, id.as_bytes()),
                ),
                "default".into(),
                "archive-fixture".into(),
            ),
            action: AuditAction::Update,
            actor: "fixture".into(),
            record: RecordId::new(elephant_types::TableName::Entity, uuid::Uuid::from_u128(42)),
            before: None,
            after: Some(serde_json::json!({"retained":marker})),
            timestamp: chrono::Utc::now(),
            reason: None,
        };
        let lexical = serde_json::to_vec_pretty(&record).map_err(io)?;
        Ok(ArchiveRow {
            table: "audit_log".into(),
            id: record.base.id.to_string(),
            lexical_sha256: sha(&lexical),
            lexical,
        })
    }
    fn binding(count: u64) -> ArchiveBinding {
        ArchiveBinding {
            version: 1,
            snapshot_sha256: "1".repeat(64),
            spool_sha256: "2".repeat(64),
            plan_id: "3".repeat(64),
            source_space_id: "default".into(),
            target_space_id: "household".into(),
            journal_identity: "fixture-journal".into(),
            disposition_sha256: ArchiveBinding::disposition_digest(),
            parent_generation_sha256: None,
            original_table_declarations: BTreeMap::from([
                ("audit_log".into(), count),
                ("work_edge".into(), 0),
                ("doc".into(), 4),
            ]),
        }
    }
    fn make(path: &Path, rows: &[ArchiveRow]) -> StorageResult<VerifiedArchive> {
        let mut builder = ArchiveBuilder::create(path, binding(rows.len() as u64))?;
        builder.append(rows)?;
        builder.seal()
    }
    #[test]
    fn empty_archive_and_survivor_rewrite_after_logical_delete() -> StorageResult<()> {
        let dir = tempfile::tempdir().map_err(io)?;
        let mut empty_binding = binding(0);
        empty_binding.original_table_declarations.clear();
        let empty =
            ArchiveBuilder::create(&dir.path().join("empty.sqlite"), empty_binding)?.seal()?;
        assert!(empty.seal().table_counts.is_empty());
        let row = original("logical-secret", "unique-logically-removed-secret-447123")?;
        let mut first_binding = binding(1);
        first_binding.original_table_declarations = BTreeMap::from([("audit_log".into(), 1)]);
        let mut writer = ArchiveBuilder::create(&dir.path().join("s0.sqlite"), first_binding)?;
        writer.append(std::slice::from_ref(&row))?;
        let first = writer.seal()?;
        let mut next = first.binding().clone();
        next.snapshot_sha256 = "f".repeat(64);
        next.parent_generation_sha256 = Some(first.seal().file_sha256.clone());
        next.original_table_declarations
            .insert("audit_log".into(), 0);
        let mut writer = ArchiveBuilder::successor(&dir.path().join("s1.sqlite"), next, &first)?;
        writer.apply(&[ArchiveChange {
            table: row.table.clone(),
            id: row.id.clone(),
            before_sha256: Some(row.lexical_sha256),
            after: None,
            logical_deletion_authorized: true,
        }])?;
        let second = writer.seal()?;
        second.verify_successor(&first, true)?;
        assert!(second.verify_successor(&first, false).is_err());
        let clean =
            ArchiveBuilder::redact(&dir.path().join("redacted.sqlite"), &second, |_, _| {
                Ok(false)
            })?;
        let bytes = std::fs::read(clean.path()).map_err(io)?;
        assert!(!bytes
            .windows(b"unique-logically-removed-secret-447123".len())
            .any(|b| b == b"unique-logically-removed-secret-447123"));
        // A sealed direct constructor with the right parent but no change receipt
        // cannot bypass the complete original diff at the storage boundary.
        let forged =
            ArchiveBuilder::create(&dir.path().join("forged.sqlite"), second.binding().clone())?
                .seal()?;
        assert!(forged.verify_successor(&first, true).is_err());
        Ok(())
    }
    #[test]
    fn original_bytes_empty_declarations_and_independent_inventory_are_exact() -> StorageResult<()>
    {
        let dir = tempfile::tempdir().map_err(io)?;
        let row = original("one", "original spaces preserved")?;
        let archive = make(
            &dir.path().join("original.sqlite"),
            std::slice::from_ref(&row),
        )?;
        let read = archive
            .row("audit_log", &row.id)?
            .ok_or_else(|| invalid("fixture row"))?;
        assert_eq!(read.lexical, row.lexical);
        assert_eq!(read.metadata()?.original_space, "default");
        assert_eq!(archive.seal.table_counts.get("work_edge"), Some(&0));
        assert!(!archive.seal.table_counts.contains_key("work_deadletter"));
        let mut inventory = ArchiveInventoryDigest::default();
        inventory.table("audit_log", 1)?;
        inventory.row("audit_log", &row.id, &row.lexical_sha256, row.lexical.len())?;
        inventory.table("work_edge", 0)?;
        assert_eq!(inventory.finish()?, archive.seal.ordered_inventory_sha256);
        assert!(archive.row("doc", "doc:unknown").is_err());
        Ok(())
    }
    #[test]
    fn append_failure_rolls_back_page_and_count_mismatch_refuses_seal() -> StorageResult<()> {
        let dir = tempfile::tempdir().map_err(io)?;
        let row = original("one", "payload")?;
        let mut writer = ArchiveBuilder::create(&dir.path().join("rollback.sqlite"), binding(1))?;
        assert!(writer.append(&[row.clone(), row.clone()]).is_err());
        writer.append(std::slice::from_ref(&row))?;
        writer.seal()?;
        let mut writer = ArchiveBuilder::create(&dir.path().join("missing.sqlite"), binding(2))?;
        writer.append(&[row])?;
        assert!(writer.seal().is_err());
        Ok(())
    }
    #[test]
    fn schema_metadata_space_and_byte_bound_tampering_are_rejected() -> StorageResult<()> {
        for sql in [
            "CREATE TABLE hidden(payload TEXT)",
            "INSERT INTO metadata VALUES('unexpected','true')",
            "UPDATE rows SET space='foreign'",
            "UPDATE rows SET n=67108865",
            "INSERT INTO recognized_refs VALUES('audit_log','audit_log:orphan','entity:extra')",
        ] {
            let dir = tempfile::tempdir().map_err(io)?;
            let row = original("one", "payload")?;
            let archive = make(
                &dir.path().join("tamper.sqlite"),
                std::slice::from_ref(&row),
            )?;
            let db = Connection::open(archive.path()).map_err(io)?;
            db.execute_batch(sql).map_err(io)?;
            db.close().map_err(|(_, e)| io(e))?;
            assert!(archive.row("audit_log", &row.id).is_err());
            assert!(
                VerifiedArchive::open(archive.path(), archive.binding(), archive.seal()).is_err()
            );
            assert!(verify_candidate(archive.path(), archive.binding(), None).is_err());
        }
        Ok(())
    }
    #[test]
    fn successor_exact_cas_requires_intent_and_preserves_original_on_failure() -> StorageResult<()>
    {
        let dir = tempfile::tempdir().map_err(io)?;
        let one = original("one", "old")?;
        let two = original("two", "retained")?;
        let original = make(&dir.path().join("s0.sqlite"), &[one.clone(), two.clone()])?;
        let mut next = binding(1);
        next.snapshot_sha256 = "4".repeat(64);
        next.parent_generation_sha256 = Some(original.seal.file_sha256.clone());
        let mut writer = ArchiveBuilder::successor(&dir.path().join("s1.sqlite"), next, &original)?;
        let change = ArchiveChange {
            table: one.table.clone(),
            id: one.id.clone(),
            before_sha256: Some(one.lexical_sha256.clone()),
            after: None,
            logical_deletion_authorized: false,
        };
        assert!(writer.apply(std::slice::from_ref(&change)).is_err());
        let mut wrong = change.clone();
        wrong.logical_deletion_authorized = true;
        wrong.before_sha256 = Some("f".repeat(64));
        assert!(writer.apply(&[wrong]).is_err());
        let mut allowed = change;
        allowed.logical_deletion_authorized = true;
        writer.apply(&[allowed])?;
        let next = writer.seal()?;
        assert!(next.row("audit_log", &one.id)?.is_none());
        assert_eq!(
            next.row("audit_log", &two.id)?
                .ok_or_else(|| invalid("survivor"))?
                .lexical,
            two.lexical
        );
        assert!(original.row("audit_log", &one.id)?.is_some());
        VerifiedArchive::open(next.path(), next.binding(), next.seal())?;
        Ok(())
    }
    #[test]
    fn redaction_never_writes_removed_payload_and_retains_cumulative_tombstones(
    ) -> StorageResult<()> {
        let dir = tempfile::tempdir().map_err(io)?;
        let secret = "UNIQUE_ERASED_PAYLOAD_cce503412f5649fb8749f9f4bdc0";
        let one = original("one", secret)?;
        let two = original("two", "second original")?;
        let original = make(
            &dir.path().join("original.sqlite"),
            &[one.clone(), two.clone()],
        )?;
        let first =
            ArchiveBuilder::redact(&dir.path().join("redacted.sqlite"), &original, |row, _| {
                Ok(row.id == one.id)
            })?;
        let bytes = std::fs::read(first.path()).map_err(io)?;
        assert!(!bytes
            .windows(secret.len())
            .any(|window| window == secret.as_bytes()));
        assert!(first.seal.redacted);
        assert_eq!(first.binding.original_table_declarations["audit_log"], 2);
        assert_eq!(first.seal.table_counts["audit_log"], 1);
        assert_eq!(
            first
                .row("audit_log", &two.id)?
                .ok_or_else(|| invalid("survivor"))?
                .lexical,
            two.lexical
        );
        let second =
            ArchiveBuilder::redact(&dir.path().join("redacted2.sqlite"), &first, |row, _| {
                Ok(row.id == two.id)
            })?;
        assert_eq!(second.seal.table_counts["audit_log"], 0);
        assert_eq!(second.binding.original_table_declarations["audit_log"], 2);
        let mut next = binding(0);
        next.parent_generation_sha256 = Some(second.seal.file_sha256.clone());
        assert!(
            ArchiveBuilder::successor(&dir.path().join("forbidden.sqlite"), next, &second).is_err()
        );
        VerifiedArchive::open(second.path(), second.binding(), second.seal())?;
        Ok(())
    }
}
