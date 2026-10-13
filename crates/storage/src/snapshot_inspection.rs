//! Bounded offline physical inventory of an already exported named snapshot.
//!
//! This module never opens a database, executes SQL, imports data, or reads the
//! external erasure journal. An unsupported value or exceeded bound produces an
//! explicit incomplete error, never a successful partial inventory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use surrealdb::sql::{Id, Thing, Value};

use crate::snapshot::SnapshotManifest;
use crate::{StorageBackendKind, StorageError, StorageResult};

#[path = "snapshot_inspection_compact.rs"]
mod compact;
#[path = "snapshot_inspection_json.rs"]
mod json;
#[path = "snapshot_inspection_native.rs"]
mod native;

// SourceRevision control payloads serialize Source once as JSON, then the
// physical export quotes that payload again. One raw ASCII control byte can
// become six JSON bytes (\u0001) and seven exported bytes (\\u0001).
// The pinned native QuoteStr uses the same one-extra-backslash bound; direct
// native backspace is five bytes, and unpadded base64 is ceil(4*n/3).
// Reserve 64 KiB for the ordinary control envelope. This is a finite supported
// record envelope, not permission to exceed aggregate input/report bounds.
const MAX_INSPECTION_STRING_BYTES: usize =
    elephant_types::publication::MAX_SOURCE_TEXT_BYTES * 7 + 64 * 1024;

/// Fixed resource limits. Callers may lower limits; values above the supported
/// defaults are refused so a CLI option cannot silently unbound inspection.
#[derive(Clone, Debug)]
pub struct SnapshotInspectionLimits {
    /// Include complete native-tagged physical records and schema SQL.
    pub include_records: bool,
    /// Use the lossless v2 blob dictionary; requires complete record inclusion.
    pub compact_records: bool,
    /// Maximum manifest bytes.
    pub manifest_bytes: usize,
    /// Maximum snapshot payload bytes.
    pub payload_bytes: usize,
    /// Maximum physical rows.
    pub rows: usize,
    /// Maximum top-level native statements.
    pub statements: usize,
    /// Maximum physical tables.
    pub tables: usize,
    /// Maximum nested values/parser depth.
    pub depth: usize,
    /// Maximum visited value nodes.
    pub nodes: usize,
    /// Maximum bytes in one encoded string token, decoded string or byte value.
    /// Native quote delimiters are excluded; escaping and base64 count.
    pub string_bytes: usize,
    /// Maximum reference occurrences.
    pub references: usize,
    /// Maximum serialized report bytes.
    pub report_bytes: usize,
}

impl Default for SnapshotInspectionLimits {
    fn default() -> Self {
        Self {
            include_records: false,
            compact_records: false,
            manifest_bytes: 64 * 1024,
            payload_bytes: 64 * 1024 * 1024,
            rows: 200_000,
            statements: 100_000,
            tables: 256,
            depth: 128,
            nodes: 2_000_000,
            string_bytes: MAX_INSPECTION_STRING_BYTES,
            references: 100_000,
            report_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Lossless tagged representation. Decimal and unsigned integer strings retain
/// exact precision; native floating-point values retain their IEEE-754 bits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum NativeInspectionValue {
    /// Native absent value, distinct from null.
    None,
    /// Explicit null.
    Null,
    /// Boolean.
    Bool(bool),
    /// UTF-8 string; text resembling a record ID remains text.
    String(String),
    /// v2 UTF-8 string stored in the report's SHA-256 blob dictionary.
    StringRef(String),
    /// Signed native integer.
    Int(i64),
    /// Embedded unsigned integer outside i64 range.
    Uint(String),
    /// Native float bits as 16 hexadecimal digits.
    FloatBits(String),
    /// Exact native decimal spelling, including retained scale.
    Decimal(String),
    /// UUID value, distinct from a UUID-looking string.
    Uuid(String),
    /// UTC timestamp with nanosecond precision.
    Datetime(String),
    /// Duration in exact seconds and nanoseconds.
    Duration {
        /// Whole seconds in the native duration.
        seconds: u64,
        /// Additional nanoseconds, less than one second.
        nanoseconds: u32,
    },
    /// Byte string encoded as hexadecimal.
    Bytes(String),
    /// v2 raw byte string stored in the report's SHA-256 blob dictionary.
    BytesRef(String),
    /// Ordered array.
    Array(Vec<Self>),
    /// Object with unique keys in deterministic order.
    Map(BTreeMap<String, Self>),
    /// Native typed record reference, including composite identities.
    Thing(Box<InspectedSnapshotIdentity>),
}

/// Physical record identity, retaining native ID type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InspectedSnapshotIdentity {
    /// Physical table name.
    pub table: String,
    /// Exact native record key.
    pub id: NativeInspectionValue,
}

/// One native typed reference found by structural traversal.
#[derive(Clone, Debug, Serialize)]
pub struct InspectedSnapshotReference {
    /// JSON Pointer-like path; ID-contained values use `/@id`.
    pub path: String,
    /// Exact referenced native identity.
    pub identity: InspectedSnapshotIdentity,
}

/// One physical row. Full payload is present only by explicit operator choice.
#[derive(Clone, Debug, Serialize)]
pub struct InspectedSnapshotRow {
    /// Exact physical identity.
    pub identity: InspectedSnapshotIdentity,
    /// Stored space, when the row carries a string space field.
    pub space_id: Option<String>,
    /// SHA-256 over the lossless tagged record representation.
    pub sha256: String,
    /// Every native reference occurrence, including nested arrays and IDs.
    pub references: Vec<InspectedSnapshotReference>,
    /// Full lossless record; omitted from the default report.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<NativeInspectionValue>,
}

/// Native schema statements can contain defaults or other retained payload.
#[derive(Clone, Debug, Serialize)]
pub struct InspectedSnapshotSchema {
    /// SHA-256 over the original statement bytes.
    pub sha256: String,
    /// Original SQL, never executed; included only with explicit payload opt-in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,
}

/// One v2 lossless blob, keyed by SHA-256 of its exact original bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InspectedSnapshotBlob {
    /// Exact decoded byte length, before UTF-8 or native byte-string encoding.
    pub bytes: u64,
    /// Canonical standard-alphabet base64 without padding.
    pub base64: String,
}

/// Complete bounded physical inventory of this one verified bundle.
#[derive(Clone, Debug, Serialize)]
pub struct SnapshotInspection {
    /// Report schema identifier.
    pub format: String,
    /// Verified original named-snapshot manifest.
    pub manifest: SnapshotManifest,
    /// Counts for every physical table, including declared empty native tables.
    pub table_counts: BTreeMap<String, u64>,
    /// Total physical rows.
    pub row_count: u64,
    /// Total top-level native statements; zero for embedded JSON.
    pub statement_count: u64,
    /// Complete inventory; no row is silently skipped.
    pub rows: Vec<InspectedSnapshotRow>,
    /// Complete native schema statement inventory.
    pub schema_statements: Vec<InspectedSnapshotSchema>,
    /// Present only in v2, including when no values require compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blobs: Option<BTreeMap<String, InspectedSnapshotBlob>>,
}

fn incomplete(reason: &str) -> StorageError {
    StorageError::DataIntegrityError(format!("snapshot_inspection_incomplete:{reason}"))
}

struct Budget<'a> {
    limits: &'a SnapshotInspectionLimits,
    nodes: usize,
    references: usize,
    parsed_rows: usize,
    parsed_tables: usize,
}

impl Budget<'_> {
    fn node(&mut self, depth: usize) -> StorageResult<()> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| incomplete("node_limit"))?;
        if depth > self.limits.depth || self.nodes > self.limits.nodes {
            return Err(incomplete("value_depth_or_node_limit"));
        }
        Ok(())
    }

    fn string(&self, value: &str) -> StorageResult<()> {
        if value.len() > self.limits.string_bytes {
            return Err(incomplete("string_limit"));
        }
        Ok(())
    }
}

struct Inventory<'a> {
    report: SnapshotInspection,
    budget: Budget<'a>,
    identities: BTreeSet<String>,
    approximate_report_bytes: usize,
    compact: compact::Compaction,
}

impl Inventory<'_> {
    fn table(&mut self, table: &str) -> StorageResult<()> {
        self.budget.string(table)?;
        if table.is_empty() {
            return Err(incomplete("empty_table"));
        }
        if !self.report.table_counts.contains_key(table) {
            if self.report.table_counts.len() >= self.budget.limits.tables {
                return Err(incomplete("table_limit"));
            }
            self.report.table_counts.insert(table.to_owned(), 0);
        }
        Ok(())
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "The parser transfers the complete native identity to this inventory ownership boundary."
    )]
    fn row(&mut self, identity: Thing, mut record: NativeInspectionValue) -> StorageResult<()> {
        if self.report.rows.len() >= self.budget.limits.rows {
            return Err(incomplete("row_limit"));
        }
        self.table(&identity.tb)?;
        let identity = native::identity(&identity, &mut self.budget, 0)?;
        let key = serde_json::to_string(&identity).map_err(|_| incomplete("identity_encoding"))?;
        if !self.identities.insert(key) {
            return Err(incomplete("duplicate_identity"));
        }
        let space_id = match &record {
            NativeInspectionValue::Map(fields) => match fields.get("space_id") {
                Some(NativeInspectionValue::String(space)) => Some(space.clone()),
                Some(NativeInspectionValue::Null | NativeInspectionValue::None) | None => None,
                Some(_) => return Err(incomplete("invalid_space_shape")),
            },
            _ => return Err(incomplete("non_object_row")),
        };
        let mut references = Vec::new();
        collect_references(&record, "", &mut self.budget, &mut references)?;
        let mut digest = DigestWriter {
            hash: Sha256::new(),
        };
        serde_json::to_writer(&mut digest, &record).map_err(|_| incomplete("record_encoding"))?;
        if self.budget.limits.compact_records {
            self.compact.record(
                &mut record,
                self.budget.limits,
                &mut self.approximate_report_bytes,
            )?;
        }
        let row = InspectedSnapshotRow {
            identity,
            space_id,
            sha256: hex::encode(digest.hash.finalize()),
            references,
            record: self.budget.limits.include_records.then_some(record),
        };
        self.charge_report(&row)?;
        *self
            .report
            .table_counts
            .get_mut(&row.identity.table)
            .ok_or_else(|| incomplete("table_inventory"))? += 1;
        self.report.row_count += 1;
        self.report.rows.push(row);
        Ok(())
    }

    fn charge_report(&mut self, value: &impl Serialize) -> StorageResult<()> {
        let available = self
            .budget
            .limits
            .report_bytes
            .saturating_sub(self.approximate_report_bytes);
        let mut writer = LimitedWriter {
            remaining: available,
            written: 0,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| incomplete("report_limit"))?;
        self.approximate_report_bytes += writer.written;
        Ok(())
    }
}

fn pointer(path: &str, component: &str) -> String {
    format!("{path}/{}", component.replace('~', "~0").replace('/', "~1"))
}

fn collect_references(
    value: &NativeInspectionValue,
    path: &str,
    budget: &mut Budget<'_>,
    references: &mut Vec<InspectedSnapshotReference>,
) -> StorageResult<()> {
    match value {
        NativeInspectionValue::Thing(identity) => {
            budget.references += 1;
            if budget.references > budget.limits.references {
                return Err(incomplete("reference_limit"));
            }
            references.push(InspectedSnapshotReference {
                path: path.to_owned(),
                identity: (**identity).clone(),
            });
            collect_references(&identity.id, &pointer(path, "@id"), budget, references)?;
        }
        NativeInspectionValue::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                collect_references(
                    value,
                    &pointer(path, &index.to_string()),
                    budget,
                    references,
                )?;
            }
        }
        NativeInspectionValue::Map(values) => {
            for (key, value) in values {
                collect_references(value, &pointer(path, key), budget, references)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct DigestWriter {
    hash: Sha256,
}
impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct LimitedWriter {
    remaining: usize,
    written: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("report limit"));
        }
        self.remaining -= bytes.len();
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_file(path: &Path, limit: usize) -> StorageResult<Vec<u8>> {
    let before = fs::symlink_metadata(path).map_err(|_| incomplete("file_unavailable"))?;
    if !before.file_type().is_file() || before.len() > limit as u64 {
        return Err(incomplete("regular_file_or_size_limit"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Do not follow a replaced leaf symlink or wait on a substituted FIFO
        // before the opened descriptor's identity can be checked.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| incomplete("file_unavailable"))?;
    let opened = file.metadata().map_err(|_| incomplete("file_metadata"))?;
    let after = fs::symlink_metadata(path).map_err(|_| incomplete("file_metadata"))?;
    if !opened.is_file()
        || !after.file_type().is_file()
        || opened.len() != before.len()
        || after.len() != before.len()
    {
        return Err(incomplete("file_changed"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev()
            || before.ino() != opened.ino()
            || after.dev() != opened.dev()
            || after.ino() != opened.ino()
        {
            return Err(incomplete("file_changed"));
        }
    }
    let mut bytes =
        Vec::with_capacity(usize::try_from(before.len()).map_err(|_| incomplete("file_size"))?);
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| incomplete("file_read"))?;
    if bytes.len() > limit || bytes.len() as u64 != before.len() {
        return Err(incomplete("file_changed_or_size_limit"));
    }
    Ok(bytes)
}

fn validate_limits(limits: &SnapshotInspectionLimits) -> StorageResult<()> {
    if limits.compact_records && !limits.include_records {
        return Err(incomplete("compact_requires_records"));
    }
    let maximum = SnapshotInspectionLimits::default();
    for (actual, cap) in [
        (limits.manifest_bytes, maximum.manifest_bytes),
        (limits.payload_bytes, maximum.payload_bytes),
        (limits.rows, maximum.rows),
        (limits.statements, maximum.statements),
        (limits.tables, maximum.tables),
        (limits.depth, maximum.depth),
        (limits.nodes, maximum.nodes),
        (limits.string_bytes, maximum.string_bytes),
        (limits.references, maximum.references),
        (limits.report_bytes, maximum.report_bytes),
    ] {
        if actual == 0 || actual > cap {
            return Err(incomplete("invalid_limits"));
        }
    }
    Ok(())
}

/// Verify and inspect a named bundle without opening storage or executing data.
///
/// # Errors
/// Returns `snapshot_inspection_incomplete:*` for every invalid, unsupported,
/// changed, expired, or over-limit input. No partial report is returned.
pub fn inspect_snapshot(
    input: &Path,
    limits: &SnapshotInspectionLimits,
) -> StorageResult<SnapshotInspection> {
    validate_limits(limits)?;
    if !fs::symlink_metadata(input)
        .map_err(|_| incomplete("bundle_unavailable"))?
        .file_type()
        .is_dir()
    {
        return Err(incomplete("bundle_not_directory"));
    }
    let bytes = bounded_file(&input.join("manifest.json"), limits.manifest_bytes)?;
    let mut budget = Budget {
        limits,
        nodes: 0,
        references: 0,
        parsed_rows: 0,
        parsed_tables: 0,
    };
    let manifest_value = json::parse(&bytes, &mut budget)?;
    let manifest: SnapshotManifest =
        serde_json::from_value(manifest_value).map_err(|_| incomplete("manifest_shape"))?;
    crate::snapshot::validate_manifest(&manifest, Utc::now())
        .map_err(|_| incomplete("manifest_invalid_or_expired"))?;
    if manifest.bytes > limits.payload_bytes as u64
        || manifest.sha256.len() != 64
        || !manifest
            .sha256
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(incomplete("manifest_payload_bound_or_digest"));
    }
    let payload = bounded_file(&input.join("data.snapshot"), limits.payload_bytes)?;
    if payload.len() as u64 != manifest.bytes
        || crate::snapshot::digest(&payload) != manifest.sha256
    {
        return Err(incomplete("payload_integrity"));
    }
    let backend = manifest.backend;
    let mut inventory = Inventory {
        report: SnapshotInspection {
            format: if limits.compact_records {
                "recall-snapshot-inspection-v2"
            } else {
                "recall-snapshot-inspection-v1"
            }
            .into(),
            manifest,
            table_counts: BTreeMap::new(),
            row_count: 0,
            statement_count: 0,
            rows: Vec::new(),
            schema_statements: Vec::new(),
            blobs: None,
        },
        budget,
        identities: BTreeSet::new(),
        approximate_report_bytes: 0,
        compact: compact::Compaction::default(),
    };
    match backend {
        StorageBackendKind::ElephantDb => json::inspect(&payload, &mut inventory)?,
        StorageBackendKind::SurrealDb => native::inspect(&payload, &mut inventory)?,
    }
    if limits.compact_records {
        inventory.report.blobs = Some(inventory.compact.into_blobs());
    }
    let mut writer = LimitedWriter {
        remaining: limits.report_bytes,
        written: 0,
    };
    serde_json::to_writer(&mut writer, &inventory.report)
        .map_err(|_| incomplete("report_limit"))?;
    Ok(inventory.report)
}

#[cfg(test)]
#[path = "snapshot_inspection_tests.rs"]
mod tests;
