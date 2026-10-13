//! Authenticated, payload-free native backup causality and exact import bindings.
//!
//! Causal authority belongs to `source_history_authority`; this module only
//! preserves its proof through an export that cannot preserve transaction IDs.
use crate::durability_io::{io_error, write_private_configuration};
use crate::erasure_journal::ErasureJournal;
use crate::snapshot::SnapshotManifest;
use crate::{StorageClient, StorageResult};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path};
use std::time::Instant;
use surrealdb::kvs::recall_history::{
    backup_origin_authenticator, backup_origin_content_commitment,
    backup_origin_journal_authenticator, NativeHistoryKey, RecallHistoryAuthority,
    RecallHistoryRecord, RecallHistorySnapshot,
};

pub const SIDECAR: &str = "history-origin.json";
const IMPORTED: &str = "imported-history-origins.json";
const BACKUP_KIND: &str = "meerkat-recall-backup-history-origin/v1";
const IMPORTED_KIND: &str = "meerkat-recall-imported-history-origin/v1";
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORDS: usize = 1_000_000;
const MAX_TOKENS: usize = 1_000_000;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OriginMode {
    NativeVersions,
    VersionlessCurrent,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Origin {
    content: String,
    #[serde(rename = "origin_mode")]
    mode: OriginMode,
    original_version: u64,
    original_timestamp: u64,
    causal_sources: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupOrigins {
    kind: String,
    journal_identity: String,
    key_fingerprint: String,
    namespace: String,
    database: String,
    snapshot_id: String,
    snapshot_sha256: String,
    manifest_sha256: String,
    rows: Vec<Origin>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed<T> {
    payload: T,
    authentication: String,
}

/// Construction is private: client import hooks receive only verified proof.
/// Deliberately no Debug implementation, including for opaque memberships.
pub struct VerifiedBackupOrigins(BackupOrigins);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedOrigin {
    origin: Origin,
    imported_version: u64,
    imported_timestamp: u64,
    snapshot_id: String,
    snapshot_sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedOrigins {
    kind: String,
    journal_identity: String,
    key_fingerprint: String,
    scopes: Vec<ImportedScope>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedScope {
    namespace: String,
    database: String,
    pending_import: Option<PendingImport>,
    rows: Vec<ImportedOrigin>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingImport {
    snapshot_id: String,
    snapshot_sha256: String,
    origin_sha256: String,
    max_preimport_version: u64,
}

fn token(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn identity(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

fn scope(namespace: &str, database: &str) -> StorageResult<()> {
    if namespace.is_empty()
        || database.is_empty()
        || namespace.len() > 4096
        || database.len() > 4096
    {
        return Err(io_error("backup origin scope is invalid"));
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(std::io::Error::other("backup origin byte bound exceeded"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded(Vec::new());
    serde_json::to_writer(&mut output, value)
        .map_err(|_| io_error("backup origin encoding exceeds bound"))?;
    Ok(output.0)
}

fn private_path(path: &Path) -> StorageResult<()> {
    let mut current = std::path::PathBuf::new();
    for component in path.components() {
        if component == Component::ParentDir {
            return Err(io_error("backup origin path cannot traverse parents"));
        }
        current.push(component);
        let metadata = fs::symlink_metadata(&current).map_err(io_error)?;
        if metadata.file_type().is_symlink() {
            return Err(io_error("backup origin path cannot contain aliases"));
        }
    }
    Ok(())
}

fn read<T: DeserializeOwned + Serialize>(path: &Path) -> StorageResult<T> {
    private_path(path)?;
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
        return Err(io_error("backup origin file type or length is invalid"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(io_error("backup origin file is not exclusively private"));
        }
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io_error)?
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > MAX_BYTES {
        return Err(io_error("backup origin file exceeded its byte bound"));
    }
    let value: T =
        serde_json::from_slice(&bytes).map_err(|_| io_error("invalid backup origin metadata"))?;
    // Canonical bytes reject duplicate fields/tokens and alternate spellings;
    // a parser must not silently collapse contradictory authenticated input.
    if encode(&value)? != bytes {
        return Err(io_error("backup origin metadata is not canonical"));
    }
    Ok(value)
}

fn authenticate<T: Serialize>(
    journal: &ErasureJournal,
    namespace: &str,
    database: &str,
    payload: &T,
) -> StorageResult<String> {
    backup_origin_authenticator(
        journal.identity(),
        namespace,
        database,
        journal.reference_key_bytes(),
        &encode(payload)?,
    )
    .map_err(|_| io_error("backup origin authentication failed"))
}

fn equal_mac(left: &str, right: &str) -> bool {
    // Length is public and fixed; compare all bytes without an early mismatch.
    token(left)
        && token(right)
        && left
            .bytes()
            .zip(right.bytes())
            .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

fn origin_shape<'a>(rows: impl Iterator<Item = &'a Origin>) -> StorageResult<()> {
    let mut count = 0_usize;
    let mut tokens = 0_usize;
    for row in rows {
        count = count
            .checked_add(1)
            .ok_or_else(|| io_error("backup origin count overflow"))?;
        tokens = tokens
            .checked_add(row.causal_sources.len())
            .ok_or_else(|| io_error("backup origin token count overflow"))?;
        if count > MAX_RECORDS
            || tokens > MAX_TOKENS
            || !token(&row.content)
            || row.causal_sources.iter().any(|value| !token(value))
            || (row.mode == OriginMode::VersionlessCurrent)
                != (row.original_version == 0 && row.original_timestamp == 0)
        {
            return Err(io_error(
                "backup origin record or token bound/shape differs",
            ));
        }
    }
    Ok(())
}

#[expect(
    clippy::suspicious_operation_groupings,
    reason = "BackupOrigins.journal_identity and SnapshotManifest.journal_id name the same retained journal; snapshot_sha256 binds the manifest payload digest."
)]
fn backup_shape(proof: &BackupOrigins, manifest: &SnapshotManifest) -> StorageResult<()> {
    scope(&proof.namespace, &proof.database)?;
    origin_shape(proof.rows.iter())?;
    if proof.kind != BACKUP_KIND
        || !identity(&proof.journal_identity)
        || !token(&proof.key_fingerprint)
        || proof.journal_identity != manifest.journal_id
        || proof.snapshot_id != manifest.snapshot_id
        || !identity(&proof.snapshot_id)
        || proof.snapshot_sha256 != manifest.sha256
        || !token(&proof.snapshot_sha256)
        || proof.manifest_sha256 != crate::snapshot::digest(&encode(manifest)?)
        || proof
            .rows
            .windows(2)
            .any(|rows| rows[0].content >= rows[1].content)
    {
        return Err(io_error(
            "backup origin snapshot binding or ordered record set differs",
        ));
    }
    Ok(())
}

/// Offline shape/hash verification cannot authenticate without the original journal.
pub fn inspect_sidecar(input: &Path, manifest: &SnapshotManifest) -> StorageResult<()> {
    let proof: Signed<BackupOrigins> = read(&input.join(SIDECAR))?;
    backup_shape(&proof.payload, manifest)?;
    if !token(&proof.authentication) {
        return Err(io_error("backup origin authentication shape differs"));
    }
    Ok(())
}

pub fn verify_backup(
    journal: &ErasureJournal,
    input: &Path,
    manifest: &SnapshotManifest,
    namespace: &str,
    database: &str,
) -> StorageResult<VerifiedBackupOrigins> {
    let proof: Signed<BackupOrigins> = read(&input.join(SIDECAR))?;
    backup_shape(&proof.payload, manifest)?;
    if proof.payload.namespace != namespace
        || proof.payload.database != database
        || proof.payload.journal_identity != journal.identity()
        || proof.payload.key_fingerprint != journal.reference_key_fingerprint()
        || !equal_mac(
            &proof.authentication,
            &authenticate(journal, namespace, database, &proof.payload)?,
        )
    {
        return Err(io_error(
            "backup origin original journal, scope or authentication differs",
        ));
    }
    // Validate any existing correspondence BEFORE replacing even the marker.
    if journal.restore_marker()?.is_some_and(|marker| {
        marker.snapshot_id != proof.payload.snapshot_id
            || marker.sha256 != proof.payload.snapshot_sha256
    }) {
        return Err(io_error("another original snapshot restore is pending"));
    }
    let verified = VerifiedBackupOrigins(proof.payload);
    if let Some(state) = load_imported(journal)? {
        if let Some(pending) = state
            .scopes
            .iter()
            .find(|scope| scope.namespace == namespace && scope.database == database)
            .and_then(|scope| scope.pending_import.as_ref())
        {
            verify_pending(pending, &verified)?;
        }
    }
    Ok(verified)
}

pub async fn inventory(
    client: &StorageClient,
    deadline: Instant,
) -> StorageResult<RecallHistorySnapshot> {
    let millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis();
    let millis =
        u64::try_from(millis).map_err(|_| io_error("backup origin deadline exceeds bound"))?;
    if millis == 0 || millis > 300_000 {
        return Err(io_error(
            "backup origin original deadline expired or exceeds bound",
        ));
    }
    let snapshot = client
        .db()
        .ok_or_else(|| io_error("native backup origin backend missing"))?
        .recall_source_history_inventory(
            &client.config().namespace,
            &client.config().database,
            millis,
        )
        .into_owned()
        .await?;
    snapshot
        .verify()
        .map_err(|_| io_error("backup origin inventory is incomplete"))?;
    Ok(snapshot)
}

fn content(
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
    row: &RecallHistoryRecord,
) -> StorageResult<String> {
    backup_origin_content_commitment(
        journal.identity(),
        &snapshot.namespace,
        &snapshot.database,
        journal.reference_key_bytes(),
        &row.key,
        &row.value,
    )
    .map_err(|_| io_error("backup origin native record commitment failed"))
}

fn current_records<'a>(
    journal: &ErasureJournal,
    snapshot: &'a RecallHistorySnapshot,
) -> StorageResult<BTreeMap<String, &'a RecallHistoryRecord>> {
    snapshot
        .verify()
        .map_err(|_| io_error("backup origin inventory is incomplete"))?;
    let mut rows = BTreeMap::new();
    for row in &snapshot.records {
        if !row.current || row.tombstone {
            continue;
        }
        let NativeHistoryKey::Record {
            namespace,
            database,
            identity,
        } = row
            .decoded_key()
            .map_err(|_| io_error("backup origin native key is invalid"))?
        else {
            continue;
        };
        if namespace != snapshot.namespace || database != snapshot.database {
            return Err(io_error("backup origin record is outside requested scope"));
        }
        // Exact same exclusion as StorageClient::snapshot_bytes's table selector.
        if identity.tb == "source_erasure_membership" {
            continue;
        }
        if rows.insert(content(journal, snapshot, row)?, row).is_some() {
            return Err(io_error("backup origin duplicate current native record"));
        }
    }
    Ok(rows)
}

pub fn write_backup(
    journal: &ErasureJournal,
    staging: &Path,
    manifest: &SnapshotManifest,
    snapshot: &RecallHistorySnapshot,
    mut causality: BTreeMap<String, BTreeSet<String>>,
) -> StorageResult<()> {
    // B supplies complete current-record coverage; the production export omits
    // only this original private membership table. No other extra is ignored.
    for row in &snapshot.records {
        if row.current
            && !row.tombstone
            && matches!(row.decoded_key().map_err(|_| io_error("backup origin native key is invalid"))?,
            NativeHistoryKey::Record { identity, .. } if identity.tb == "source_erasure_membership")
        {
            causality.remove(&content(journal, snapshot, row)?);
        }
    }
    let previous = load_imported(journal)?;
    let imported = previous
        .as_ref()
        .and_then(|state| {
            state.scopes.iter().find(|scope| {
                scope.namespace == snapshot.namespace && scope.database == snapshot.database
            })
        })
        .map(|scope| imported_index(&scope.rows))
        .unwrap_or_default();
    let mut rows = Vec::new();
    for (commitment, record) in current_records(journal, snapshot)? {
        let causal_sources = causality
            .remove(&commitment)
            .ok_or_else(|| io_error("backup origin causal coverage is incomplete"))?;
        let previous = imported
            .get(&(commitment.clone(), record.version, record.timestamp))
            .filter(|_| record.version != 0 || record.timestamp != 0);
        let (original_version, original_timestamp) = previous
            .map_or((record.version, record.timestamp), |row| {
                (row.origin.original_version, row.origin.original_timestamp)
            });
        let origin_mode = if original_version == 0 && original_timestamp == 0 {
            OriginMode::VersionlessCurrent
        } else {
            OriginMode::NativeVersions
        };
        rows.push(Origin {
            content: commitment,
            mode: origin_mode,
            original_version,
            original_timestamp,
            causal_sources,
        });
    }
    if !causality.is_empty() {
        return Err(io_error(
            "backup origin causal coverage contains unexported records",
        ));
    }
    let payload = BackupOrigins {
        kind: BACKUP_KIND.into(),
        journal_identity: journal.identity().into(),
        key_fingerprint: journal.reference_key_fingerprint().into(),
        namespace: snapshot.namespace.clone(),
        database: snapshot.database.clone(),
        snapshot_id: manifest.snapshot_id.clone(),
        snapshot_sha256: manifest.sha256.clone(),
        manifest_sha256: crate::snapshot::digest(&encode(manifest)?),
        rows,
    };
    backup_shape(&payload, manifest)?;
    let authentication = authenticate(journal, &payload.namespace, &payload.database, &payload)?;
    write_private_configuration(
        &staging.join(SIDECAR),
        &encode(&Signed {
            payload,
            authentication,
        })?,
    )
    .map_err(io_error)
}

type ImportedKey = (String, u64, u64);
fn imported_index(rows: &[ImportedOrigin]) -> BTreeMap<ImportedKey, &ImportedOrigin> {
    rows.iter()
        .map(|row| {
            (
                (
                    row.origin.content.clone(),
                    row.imported_version,
                    row.imported_timestamp,
                ),
                row,
            )
        })
        .collect()
}

fn authenticate_imported(
    journal: &ErasureJournal,
    proof: &ImportedOrigins,
) -> StorageResult<String> {
    backup_origin_journal_authenticator(
        journal.identity(),
        journal.reference_key_bytes(),
        &encode(proof)?,
    )
    .map_err(|_| io_error("imported history origin authentication failed"))
}

fn load_imported(journal: &ErasureJournal) -> StorageResult<Option<ImportedOrigins>> {
    let path = journal.path().join(IMPORTED);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if journal.restore_origins_required()? {
                return Err(io_error(
                    "acknowledged imported history origins are missing",
                ));
            }
            return Ok(None);
        }
        Err(error) => return Err(io_error(error)),
        Ok(_) => {}
    }
    let signed: Signed<ImportedOrigins> = read(&path)?;
    let proof = &signed.payload;
    origin_shape(
        proof
            .scopes
            .iter()
            .flat_map(|scope| scope.rows.iter().map(|row| &row.origin)),
    )?;
    if proof.kind != IMPORTED_KIND
        || proof.journal_identity != journal.identity()
        || proof.key_fingerprint != journal.reference_key_fingerprint()
        || proof.scopes.is_empty()
        || proof.scopes.len() > 1024
        || proof.scopes.windows(2).any(|pair| {
            (&pair[0].namespace, &pair[0].database) >= (&pair[1].namespace, &pair[1].database)
        })
        || !equal_mac(
            &signed.authentication,
            &authenticate_imported(journal, proof)?,
        )
    {
        return Err(io_error(
            "imported history origin identity, ordering or authentication differs",
        ));
    }
    for scope_rows in &proof.scopes {
        scope(&scope_rows.namespace, &scope_rows.database)?;
        if scope_rows.pending_import.as_ref().is_some_and(|pending| {
            !identity(&pending.snapshot_id)
                || !token(&pending.snapshot_sha256)
                || !token(&pending.origin_sha256)
        }) {
            return Err(io_error("pending native import origin identity differs"));
        }
        if scope_rows.rows.iter().any(|row| {
            !identity(&row.snapshot_id)
                || !token(&row.snapshot_sha256)
                || (row.imported_version == 0 && row.imported_timestamp == 0)
        }) || scope_rows.rows.windows(2).any(|pair| {
            (
                &pair[0].origin.content,
                pair[0].imported_version,
                pair[0].imported_timestamp,
            ) >= (
                &pair[1].origin.content,
                pair[1].imported_version,
                pair[1].imported_timestamp,
            )
        }) {
            return Err(io_error(
                "imported history origin native version ordering or shape differs",
            ));
        }
    }
    Ok(Some(signed.payload))
}

/// Called by the original journal's startup validation; absence after ACK is fatal.
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "RestoreMarker.sha256 and PendingImport.snapshot_sha256 bind the same original snapshot payload under its snapshot_id."
)]
pub fn validate_restored_history(journal: &ErasureJournal) -> StorageResult<()> {
    if let Some(proof) = load_imported(journal)? {
        let marker = journal.restore_marker()?;
        for pending in proof
            .scopes
            .iter()
            .filter_map(|scope| scope.pending_import.as_ref())
        {
            if !marker.as_ref().is_some_and(|marker| {
                marker.snapshot_id == pending.snapshot_id
                    && marker.sha256 == pending.snapshot_sha256
            }) {
                return Err(io_error(
                    "pending imported history lost its original restore barrier",
                ));
            }
        }
    }
    Ok(())
}

/// Client hook immediately after checked native import, before guard refresh or replay.
pub async fn bind_imported_snapshot(
    client: &StorageClient,
    proof: &VerifiedBackupOrigins,
    deadline: Instant,
) -> StorageResult<()> {
    let journal = client.erasure_journal()?;
    let marker = journal
        .restore_marker()?
        .ok_or_else(|| io_error("import origin binding requires the durable restore marker"))?;
    if marker.snapshot_id != proof.0.snapshot_id || marker.sha256 != proof.0.snapshot_sha256 {
        return Err(io_error(
            "import origin binding and original restore marker differ",
        ));
    }
    let snapshot = inventory(client, deadline).await?;
    bind_imported(journal, proof, &snapshot)?;
    if Instant::now() >= deadline {
        return Err(io_error("original import origin deadline expired"));
    }
    Ok(())
}

fn bind_imported(
    journal: &ErasureJournal,
    proof: &VerifiedBackupOrigins,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<()> {
    journal.with_restore_origins_writer(|| bind_imported_unacknowledged(journal, proof, snapshot))
}

// Requires the original writer operation. Kept separate to exercise the exact
// durable-file-before-index crash cut without a production fault hook.
fn bind_imported_unacknowledged(
    journal: &ErasureJournal,
    proof: &VerifiedBackupOrigins,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<()> {
    edit_imported_scope(journal, &snapshot.namespace, &snapshot.database, |scope| {
        let pending = scope
            .pending_import
            .as_ref()
            .ok_or_else(|| io_error("native import lacks its durable original version floor"))?;
        verify_pending(pending, proof)?;
        let actual = current_records(journal, snapshot)?;
        if actual.len() != proof.0.rows.len()
            || proof
                .0
                .rows
                .iter()
                .any(|origin| !actual.contains_key(&origin.content))
        {
            return Err(io_error(
                "imported native key/full-value set differs from authenticated backup",
            ));
        }
        if actual
            .values()
            .any(|row| row.version != 0 && row.version <= pending.max_preimport_version)
        {
            return Err(io_error(
                "current restored record predates its original import floor",
            ));
        }
        merge_post_import_rows(journal, scope, proof, snapshot)?;
        scope.pending_import = None;
        Ok(())
    })
}

fn verify_pending(pending: &PendingImport, proof: &VerifiedBackupOrigins) -> StorageResult<()> {
    if pending.snapshot_id != proof.0.snapshot_id
        || pending.snapshot_sha256 != proof.0.snapshot_sha256
        || pending.origin_sha256 != crate::snapshot::digest(&encode(&proof.0)?)
    {
        return Err(io_error(
            "pending import belongs to another authenticated snapshot",
        ));
    }
    Ok(())
}

// The callback's entire read/union/write is protected by B's original journal
// writer operation. This function intentionally does not reacquire that lock.
fn edit_imported_scope(
    journal: &ErasureJournal,
    namespace: &str,
    database: &str,
    change: impl FnOnce(&mut ImportedScope) -> StorageResult<()>,
) -> StorageResult<()> {
    let mut previous = load_imported(journal)?.unwrap_or_else(|| ImportedOrigins {
        kind: IMPORTED_KIND.into(),
        journal_identity: journal.identity().into(),
        key_fingerprint: journal.reference_key_fingerprint().into(),
        scopes: Vec::new(),
    });
    let position = previous
        .scopes
        .iter()
        .position(|scope| scope.namespace == namespace && scope.database == database);
    let mut scope = position.map_or_else(
        || ImportedScope {
            namespace: namespace.into(),
            database: database.into(),
            pending_import: None,
            rows: Vec::new(),
        },
        |index| previous.scopes.remove(index),
    );
    change(&mut scope)?;
    previous.scopes.push(scope);
    previous
        .scopes
        .sort_by(|a, b| (&a.namespace, &a.database).cmp(&(&b.namespace, &b.database)));
    if previous.scopes.len() > 1024 {
        return Err(io_error("imported history origin scope bound exceeded"));
    }
    origin_shape(
        previous
            .scopes
            .iter()
            .flat_map(|scope| scope.rows.iter().map(|row| &row.origin)),
    )?;
    let authentication = authenticate_imported(journal, &previous)?;
    write_private_configuration(
        &journal.path().join(IMPORTED),
        &encode(&Signed {
            payload: previous,
            authentication,
        })?,
    )
    .map_err(io_error)
}

fn merge_post_import_rows(
    journal: &ErasureJournal,
    scope: &mut ImportedScope,
    proof: &VerifiedBackupOrigins,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<()> {
    if proof.0.namespace != snapshot.namespace || proof.0.database != snapshot.database {
        return Err(io_error("imported history origin scope differs"));
    }
    let pending = scope
        .pending_import
        .as_ref()
        .ok_or_else(|| io_error("native import floor is absent"))?;
    verify_pending(pending, proof)?;
    let expected = proof
        .0
        .rows
        .iter()
        .map(|origin| (origin.content.as_str(), origin))
        .collect::<BTreeMap<_, _>>();
    let mut rows = std::mem::take(&mut scope.rows)
        .into_iter()
        .map(|row| {
            (
                (
                    row.origin.content.clone(),
                    row.imported_version,
                    row.imported_timestamp,
                ),
                row,
            )
        })
        .collect::<BTreeMap<_, _>>();
    for actual in &snapshot.records {
        // Pre-existing equal B values remain outside the import's authority.
        // All retry generations after this SAME floor are captured, including
        // historical rows left by an interrupted previous import.
        if actual.tombstone || actual.version <= pending.max_preimport_version {
            continue;
        }
        let NativeHistoryKey::Record {
            namespace,
            database,
            identity,
        } = actual
            .decoded_key()
            .map_err(|_| io_error("import origin native key is invalid"))?
        else {
            continue;
        };
        if namespace != snapshot.namespace || database != snapshot.database {
            return Err(io_error("import origin native scope differs"));
        }
        if identity.tb == "source_erasure_membership" {
            continue;
        }
        let commitment = content(journal, snapshot, actual)?;
        let origin = expected.get(commitment.as_str()).ok_or_else(|| {
            io_error("post-floor imported record lacks exact authenticated backup value")
        })?;
        let key = (commitment, actual.version, actual.timestamp);
        if let Some(previous) = rows.get(&key) {
            if previous.origin.causal_sources != origin.causal_sources
                || previous.origin.original_version != origin.original_version
                || previous.origin.original_timestamp != origin.original_timestamp
            {
                return Err(io_error(
                    "imported native version has contradictory original causality",
                ));
            }
        } else {
            rows.insert(
                key,
                ImportedOrigin {
                    origin: (*origin).clone(),
                    imported_version: actual.version,
                    imported_timestamp: actual.timestamp,
                    snapshot_id: proof.0.snapshot_id.clone(),
                    snapshot_sha256: proof.0.snapshot_sha256.clone(),
                },
            );
        }
    }
    scope.rows = rows.into_values().collect();
    Ok(())
}

/// Before the first destructive replacement, persist the original scoped native
/// version floor.
///
/// On resume it is immutable; bind all exact partial-import rows
/// beyond it before another replacement can make those rows historical.
pub async fn prepare_import(
    client: &StorageClient,
    proof: &VerifiedBackupOrigins,
    deadline: Instant,
) -> StorageResult<()> {
    let journal = client.erasure_journal()?;
    let marker = journal
        .restore_marker()?
        .ok_or_else(|| io_error("import origin preparation requires the durable restore marker"))?;
    if marker.snapshot_id != proof.0.snapshot_id || marker.sha256 != proof.0.snapshot_sha256 {
        return Err(io_error(
            "import origin preparation and restore marker differ",
        ));
    }
    let snapshot = inventory(client, deadline).await?;
    journal
        .with_restore_origins_writer(|| prepare_import_unacknowledged(journal, proof, &snapshot))?;
    if Instant::now() >= deadline {
        return Err(io_error("original import origin deadline expired"));
    }
    Ok(())
}

fn prepare_import_unacknowledged(
    journal: &ErasureJournal,
    proof: &VerifiedBackupOrigins,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<()> {
    snapshot
        .verify()
        .map_err(|_| io_error("pre-import native inventory is incomplete"))?;
    if snapshot.namespace != proof.0.namespace || snapshot.database != proof.0.database {
        return Err(io_error("pre-import native scope differs"));
    }
    edit_imported_scope(journal, &snapshot.namespace, &snapshot.database, |scope| {
        if let Some(pending) = &scope.pending_import {
            verify_pending(pending, proof)?;
            merge_post_import_rows(journal, scope, proof, snapshot)?;
        } else {
            scope.pending_import = Some(PendingImport {
                snapshot_id: proof.0.snapshot_id.clone(),
                snapshot_sha256: proof.0.snapshot_sha256.clone(),
                origin_sha256: crate::snapshot::digest(&encode(&proof.0)?),
                max_preimport_version: snapshot
                    .records
                    .iter()
                    .map(|row| row.version)
                    .max()
                    .unwrap_or(0),
            });
        }
        Ok(())
    })
}

/// Keys include the actual current/historical marker at this inspection, while
/// lookup requires immutable imported version/time and complete key/value proof.
///
/// Empty token sets still identify imported decisions; B must skip TX inference.
pub fn restored_history_causality(
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
    authority: &RecallHistoryAuthority,
) -> StorageResult<BTreeMap<String, BTreeSet<String>>> {
    snapshot
        .verify()
        .map_err(|_| io_error("restored history inventory is incomplete"))?;
    if authority.namespace() != snapshot.namespace || authority.database() != snapshot.database {
        return Err(io_error("restored history authority scope differs"));
    }
    let Some(proof) = load_imported(journal)? else {
        return Ok(BTreeMap::new());
    };
    let Some(scope) = proof
        .scopes
        .iter()
        .find(|scope| scope.namespace == snapshot.namespace && scope.database == snapshot.database)
    else {
        return Ok(BTreeMap::new());
    };
    let index = imported_index(&scope.rows);
    let mut result = BTreeMap::new();
    for row in &snapshot.records {
        if row.tombstone
            || (row.version == 0 && row.timestamp == 0)
            || !matches!(
                row.decoded_key()
                    .map_err(|_| io_error("restored native key is invalid"))?,
                NativeHistoryKey::Record { .. }
            )
        {
            continue;
        }
        if let Some(origin) =
            index.get(&(content(journal, snapshot, row)?, row.version, row.timestamp))
        {
            result.insert(
                authority
                    .version_commitment(row)
                    .map_err(|_| io_error("restored native version commitment failed"))?,
                origin.origin.causal_sources.clone(),
            );
        }
    }
    Ok(result)
}

#[cfg(test)]
#[path = "source_history_restore_tests.rs"]
mod tests;
