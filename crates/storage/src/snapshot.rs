//! Consistent named backup bundles with finite retention and resumable, gated restore.
use crate::durability_io::{atomic_write, io_error, private_directory, sync_directory};
use crate::erasure_journal::{ErasureJournal, RestoreMarker};
use crate::recovery_barrier::RecoveryPhase;
use crate::{StorageBackendKind, StorageClient, StorageError, StorageResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Default lifetime of an owned backup: seven days.
pub const DEFAULT_RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;
/// Maximum accepted backup lifetime: thirty days.
pub const MAX_RETENTION_SECONDS: u64 = 30 * 24 * 60 * 60;
/// Upper bound on waiting for already admitted work to drain.
pub const MAX_QUIESCE_SECONDS: u64 = 60;
const FORMAT: &str = "recall-snapshot-v1";
const DATA: &str = "data.snapshot";
const MANIFEST: &str = "manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Finite backup retention and pre-export drain budget.
pub struct SnapshotRetention {
    /// Lifetime after creation, bounded by MAX_RETENTION_SECONDS.
    pub retention_seconds: u64,
    /// Maximum wait for exclusive storage admission, at most sixty seconds.
    pub quiesce_timeout_seconds: u64,
}
impl Default for SnapshotRetention {
    fn default() -> Self {
        Self {
            retention_seconds: DEFAULT_RETENTION_SECONDS,
            quiesce_timeout_seconds: 30,
        }
    }
}
impl SnapshotRetention {
    fn validate(&self) -> StorageResult<()> {
        if !(1..=MAX_RETENTION_SECONDS).contains(&self.retention_seconds)
            || !(1..=MAX_QUIESCE_SECONDS).contains(&self.quiesce_timeout_seconds)
        {
            return Err(io_error(
                "retention must be 1..=2592000 seconds; quiesce must be 1..=60 seconds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Non-secret receipt describing one immutable named backup bundle.
pub struct SnapshotManifest {
    /// Manifest schema version, currently one.
    pub version: u32,
    /// Stable bundle-format identifier.
    pub format: String,
    /// Opaque immutable backup-generation UUID.
    pub snapshot_id: String,
    /// Required independently retained erasure-journal UUID.
    pub journal_id: String,
    /// Backend required by the restore target.
    pub backend: StorageBackendKind,
    /// Operator-chosen operational name.
    pub name: String,
    /// Start of backup generation and retention.
    pub created_at: DateTime<Utc>,
    /// Deadline after which restore is refused and cleanup deletes owned files.
    pub expires_at: DateTime<Utc>,
    /// Exact payload byte length.
    pub bytes: u64,
    /// SHA-256 digest of the payload bytes, encoded as lowercase hexadecimal.
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedBackup {
    path: PathBuf,
    snapshot_id: String,
    expires_at: DateTime<Utc>,
    #[serde(default)]
    raw_sha256: Option<String>,
    #[serde(default)]
    deleting: bool,
    #[serde(default)]
    temporary: bool,
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn validate_manifest(
    manifest: &SnapshotManifest,
    now: DateTime<Utc>,
) -> StorageResult<()> {
    let retention = manifest
        .expires_at
        .signed_duration_since(manifest.created_at);
    if manifest.version != 1
        || manifest.format != FORMAT
        || uuid::Uuid::parse_str(&manifest.snapshot_id).is_err()
        || uuid::Uuid::parse_str(&manifest.journal_id).is_err()
        || manifest.name.is_empty()
        || retention.num_seconds() <= 0
        || retention.num_seconds() > i64::try_from(MAX_RETENTION_SECONDS).map_err(io_error)?
        || manifest.created_at > now
        || manifest.expires_at <= now
    {
        return Err(io_error(
            "snapshot version, identity or finite retention is invalid/expired",
        ));
    }
    Ok(())
}

fn read_verified(input: &Path) -> StorageResult<(SnapshotManifest, Vec<u8>)> {
    if fs::symlink_metadata(input)
        .map_err(io_error)?
        .file_type()
        .is_symlink()
    {
        return Err(io_error("snapshot bundle must not be a symlink"));
    }
    for name in [MANIFEST, DATA] {
        if !fs::symlink_metadata(input.join(name))
            .map_err(io_error)?
            .file_type()
            .is_file()
        {
            return Err(io_error("snapshot entries must be regular files"));
        }
    }
    let manifest: SnapshotManifest =
        serde_json::from_slice(&fs::read(input.join(MANIFEST)).map_err(io_error)?)
            .map_err(io_error)?;
    validate_manifest(&manifest, Utc::now())?;
    let bytes = fs::read(input.join(DATA)).map_err(io_error)?;
    if manifest.bytes != bytes.len() as u64 || manifest.sha256 != digest(&bytes) {
        return Err(io_error("snapshot length/hash mismatch"));
    }
    if manifest.backend == StorageBackendKind::ElephantDb {
        elephant_db::ElephantDb::from_snapshot_bytes(&bytes)?;
    } else {
        // Syntax is validated before destructive restore. Execution checks every statement too.
        let sql = std::str::from_utf8(&bytes).map_err(io_error)?;
        surrealdb::sql::parse(sql).map_err(io_error)?;
        crate::source_history_restore::inspect_sidecar(input, &manifest)?;
    }
    Ok((manifest, bytes))
}

/// Read-only verification; rejects expired backups and does not open a database.
#[allow(clippy::unused_async)] // Keep the public snapshot I/O interface uniformly asynchronous.
pub async fn verify(input: &Path) -> StorageResult<SnapshotManifest> {
    Ok(read_verified(input)?.0)
}

/// The runtime pauses admissions/scheduling and drains active work before this final gate.
/// The gate covers every storage query, including workers and retained repository handles.
pub async fn export_named(
    client: &StorageClient,
    output: &Path,
    name: &str,
    retention: &SnapshotRetention,
) -> StorageResult<SnapshotManifest> {
    let client = client.clone();
    let output = output.to_owned();
    let name = name.to_owned();
    let retention = retention.clone();
    tokio::spawn(async move { export_owned(&client, &output, &name, &retention).await })
        .await
        .map_err(io_error)?
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
)]
async fn export_owned(
    client: &StorageClient,
    output: &Path,
    name: &str,
    retention: &SnapshotRetention,
) -> StorageResult<SnapshotManifest> {
    retention.validate()?;
    if name.is_empty() || name.len() > 128 {
        return Err(io_error("snapshot name must contain 1..=128 bytes"));
    }
    require_absent_output(output)?;
    let guard = client
        .begin_maintenance(
            RecoveryPhase::Exporting,
            Duration::from_secs(retention.quiesce_timeout_seconds),
        )
        .await?;
    let privileged = client.maintenance_client_with_guard(&guard);
    require_absent_output(output)?;
    privileged.ensure_quiescent().await?;
    let journal = client.erasure_journal()?;
    cleanup_expired(journal)?;
    let _backup_guard = journal.backups.clone().lock_owned().await;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(io_error)?;
    let snapshot_id = uuid::Uuid::new_v4().to_string();
    let staging = parent.join(format!(".recall-snapshot-{snapshot_id}"));
    private_directory(&staging)?;
    // Register the incomplete generation before materializing payload, so crashes cannot
    // leave indefinitely retained data. Cleanup knows both staging and published names.
    let created_at = Utc::now();
    let expires_at = created_at
        + chrono::Duration::seconds(i64::try_from(retention.retention_seconds).map_err(io_error)?);
    let output = parent.canonicalize().map_err(io_error)?.join(
        output
            .file_name()
            .ok_or_else(|| io_error("snapshot output needs a name"))?,
    );
    register_backup(
        journal,
        &staging.canonicalize().map_err(io_error)?,
        &format!("{snapshot_id}-staging"),
        expires_at,
    )?;
    // One native proof deadline covers inspection, export, causal derivation and
    // verification. The existing admission-drain budget remains separate.
    let origin_deadline = Instant::now() + Duration::from_secs(300);
    // Foreground admissions have drained. The SDK has independent writers;
    // keep them paused across the complete original inventory and sealed export.
    let native_background = privileged.pause_native_background(origin_deadline).await?;
    let native_origins = if client.backend() == StorageBackendKind::SurrealDb {
        let before = crate::source_history_restore::inventory(&privileged, origin_deadline).await?;
        // B layers the original live inverse planner over complete native
        // history, including versionless/current and mixed source support.
        let causality = crate::source_history_authority::backup_versionless_history_causality(
            &privileged,
            journal,
            &before,
        )
        .await?;
        Some((before, causality))
    } else {
        None
    };
    let bytes = privileged.snapshot_bytes(&staging.join(DATA)).await?;
    if let Some((before, _)) = &native_origins {
        let after = crate::source_history_restore::inventory(&privileged, origin_deadline).await?;
        if before.fingerprint != after.fingerprint {
            return Err(io_error(
                "native history changed during original backup export",
            ));
        }
    }
    let manifest = SnapshotManifest {
        version: 1,
        format: FORMAT.into(),
        snapshot_id: snapshot_id.clone(),
        journal_id: journal.identity().into(),
        backend: client.backend(),
        name: name.into(),
        created_at,
        expires_at,
        bytes: bytes.len() as u64,
        sha256: digest(&bytes),
    };
    crate::historical_archive_lifecycle::backup::write(&privileged, &staging, &manifest).await?;
    if let Some((snapshot, causality)) = native_origins {
        crate::source_history_restore::write_backup(
            journal, &staging, &manifest, &snapshot, causality,
        )?;
        if Instant::now() >= origin_deadline {
            return Err(io_error("original backup origin deadline expired"));
        }
    }
    atomic_write(&staging.join(DATA), &bytes)?;
    atomic_write(
        &staging.join(MANIFEST),
        &serde_json::to_vec_pretty(&manifest).map_err(io_error)?,
    )?;
    register_backup(journal, &output, &snapshot_id, expires_at)?;
    fs::rename(&staging, &output).map_err(io_error)?;
    sync_directory(parent)?;
    validate_manifest(&manifest, Utc::now())?;
    drop(privileged);
    drop(native_background);
    guard.finish();
    Ok(manifest)
}

fn require_absent_output(output: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(output) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(StorageError::Conflict(
            "snapshot output already exists".into(),
        )),
    }
}

fn register_backup(
    journal: &ErasureJournal,
    path: &Path,
    snapshot_id: &str,
    expires_at: DateTime<Utc>,
) -> StorageResult<()> {
    let record = RetainedBackup {
        path: path.into(),
        snapshot_id: snapshot_id.into(),
        expires_at,
        raw_sha256: None,
        deleting: false,
        temporary: false,
    };
    atomic_write(
        &journal.backups_path().join(format!("{snapshot_id}.json")),
        &serde_json::to_vec(&record).map_err(io_error)?,
    )
}

/// Delete only registered generations created by Recall, including expired partial exports.
/// Run at startup and every minute while the runtime owns storage.
pub fn cleanup_expired(journal: &ErasureJournal) -> StorageResult<usize> {
    cleanup_at(journal, Utc::now())
}

fn cleanup_at(journal: &ErasureJournal, now: DateTime<Utc>) -> StorageResult<usize> {
    let Ok(_backup_guard) = journal.backups.try_lock() else {
        return Ok(0);
    };
    let mut removed = 0;
    for entry in fs::read_dir(journal.backups_path()).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".recall-write-")
        {
            continue;
        }
        let mut record: RetainedBackup =
            serde_json::from_slice(&fs::read(entry.path()).map_err(io_error)?).map_err(io_error)?;
        if record.expires_at > now {
            continue;
        }
        let retained_metadata = match fs::symlink_metadata(&record.path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(io_error(error)),
        };
        if let Some(metadata) = retained_metadata {
            if let Some(expected) = &record.raw_sha256 {
                if !metadata.file_type().is_file() {
                    return Err(io_error("retained raw snapshot changed type"));
                }
                let owned_temporary = record.temporary
                    && record.path.file_name().is_some_and(|name| {
                        name == format!(".recall-write-{}", record.snapshot_id).as_str()
                    });
                if owned_temporary
                    || digest(&fs::read(&record.path).map_err(io_error)?) == *expected
                {
                    fs::remove_file(&record.path).map_err(io_error)?;
                    if let Some(parent) = record.path.parent() {
                        sync_directory(parent)?;
                    }
                    removed += 1;
                }
                // A newer generation may have replaced the same previous-generation path.
                fs::remove_file(entry.path()).map_err(io_error)?;
                continue;
            }
            if !metadata.file_type().is_dir() {
                return Err(io_error("registered backup directory changed type"));
            }
            if !record.deleting && !record.snapshot_id.ends_with("-staging") {
                let manifest: SnapshotManifest = serde_json::from_slice(
                    &fs::read(record.path.join(MANIFEST)).map_err(io_error)?,
                )
                .map_err(io_error)?;
                if manifest.snapshot_id != record.snapshot_id {
                    return Err(io_error("registered backup ownership mismatch"));
                }
            }
            if !record.deleting {
                record.deleting = true;
                atomic_write(
                    &entry.path(),
                    &serde_json::to_vec(&record).map_err(io_error)?,
                )?;
            }
            for temporary in fs::read_dir(&record.path).map_err(io_error)? {
                let temporary = temporary.map_err(io_error)?;
                let name = temporary.file_name();
                if name
                    .to_str()
                    .and_then(|name| name.strip_prefix(".recall-write-"))
                    .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
                {
                    fs::remove_file(temporary.path()).map_err(io_error)?;
                }
            }
            for name in [
                DATA,
                MANIFEST,
                crate::source_history_restore::SIDECAR,
                crate::historical_archive_lifecycle::backup::SIDECAR,
            ] {
                match fs::remove_file(record.path.join(name)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_error(e)),
                }
            }
            // Unknown files are never recursively deleted.
            fs::remove_dir(&record.path).map_err(io_error)?;
            if let Some(parent) = record.path.parent() {
                sync_directory(parent)?;
            }
            removed += 1;
        }
        fs::remove_file(entry.path()).map_err(io_error)?;
    }
    sync_directory(&journal.backups_path())?;
    Ok(removed)
}

/// Restore replaces the complete dataset. A synced marker precedes any replacement;
/// failure/cancellation keeps all ordinary storage access blocked until this operation resumes.
pub async fn restore_named(
    client: &StorageClient,
    input: &Path,
    journal: &ErasureJournal,
) -> StorageResult<SnapshotManifest> {
    restore_named_with_timeout(client, input, journal, Duration::from_secs(30)).await
}

/// Restore with the runtime's remaining precise quiesce budget. Payload I/O is separate.
pub async fn restore_named_with_timeout(
    client: &StorageClient,
    input: &Path,
    journal: &ErasureJournal,
    remaining: Duration,
) -> StorageResult<SnapshotManifest> {
    if remaining.is_zero() || remaining > Duration::from_secs(MAX_QUIESCE_SECONDS) {
        return Err(StorageError::Conflict(
            "restore quiesce budget must be positive and at most sixty seconds".into(),
        ));
    }
    let client = client.clone();
    let input = input.to_owned();
    let journal = journal.clone();
    tokio::spawn(async move { Box::pin(restore_owned(&client, &input, &journal, remaining)).await })
        .await
        .map_err(io_error)?
}

/// Existing OS ownership primitive held for the entire restore, including
/// matching-marker retries from independently opened journal/client handles.
/// The stable lock inode is never removed on completion.
pub(crate) fn restore_ownership(
    journal: &ErasureJournal,
) -> StorageResult<crate::exclusive_store_lock::StoreWriterLock> {
    crate::exclusive_store_lock::StoreWriterLock::acquire(&journal.path().join("restore-operation"))
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
)]
async fn restore_owned(
    client: &StorageClient,
    input: &Path,
    journal: &ErasureJournal,
    remaining: Duration,
) -> StorageResult<SnapshotManifest> {
    client.ensure_no_incomplete_transfer()?;
    if journal.path() != client.erasure_journal()?.path() {
        return Err(io_error(
            "restore must use this store's independently retained erasure journal",
        ));
    }
    let (manifest, bytes) = read_verified(input)?;
    if manifest.journal_id != journal.identity() {
        return Err(io_error(
            "snapshot requires its original independently retained erasure journal",
        ));
    }
    if manifest.backend != client.backend() {
        return Err(io_error("snapshot backend differs from destination"));
    }
    let _restore_owner = restore_ownership(journal)?;
    // This payload-free sidecar binds the native archive cut to the exact
    // snapshot. Archive files remain in the independently retained original
    // journal; an incompatible cut fails before any destructive replacement.
    crate::historical_archive_lifecycle::backup::preflight(journal, input, &manifest)?;
    // Authenticate the exact native bundle and original journal before the
    // destructive import is admitted. Missing old proof is never synthesized.
    let origin = if manifest.backend == StorageBackendKind::SurrealDb {
        Some(crate::source_history_restore::verify_backup(
            journal,
            input,
            &manifest,
            &client.config().namespace,
            &client.config().database,
        )?)
    } else {
        None
    };
    let guard = client
        .begin_maintenance(RecoveryPhase::Restoring, remaining)
        .await?;
    let privileged = client.maintenance_client_with_guard(&guard);
    // Restore ownership excludes restore peers; only this acquired maintenance
    // barrier excludes a concurrent archive cut or redaction since preflight.
    if let Err(error) =
        crate::historical_archive_lifecycle::backup::preflight(journal, input, &manifest)
    {
        if journal.restore_marker()?.is_none() {
            guard.finish();
        }
        return Err(error);
    }
    // An interrupted replacement may already have removed queue state; the durable marker
    // determines whether resumption is legal, never an inference from a partially empty store.
    if journal.restore_marker()?.is_none() {
        if let Err(error) = privileged.ensure_quiescent().await {
            guard.finish();
            return Err(error);
        }
    }
    let restore_marker = RestoreMarker {
        snapshot_id: manifest.snapshot_id.clone(),
        sha256: manifest.sha256.clone(),
    };
    journal.begin_restore(&restore_marker)?;
    let origin_deadline = Instant::now() + Duration::from_secs(300);
    if let Some(origin) = &origin {
        crate::source_history_restore::prepare_import(&privileged, origin, origin_deadline).await?;
    }
    privileged
        .replace_snapshot_bytes_with_origin(&bytes, origin.as_ref(), origin_deadline)
        .await?;
    privileged.initialize_journal_identity().await?;
    Box::pin(privileged.replay_erasures()).await?;
    privileged.rotate_restored_epochs().await?;
    privileged.persist_restored_state().await?;
    crate::historical_archive_lifecycle::verify_native_archive_heads(&privileged).await?;
    cleanup_expired(journal)?;
    journal.finish_restore_checked(&restore_marker)?;
    drop(privileged);
    guard.finish();
    Ok(manifest)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveSnapshot {
    format: String,
    sha256: String,
    data: String,
    #[serde(default)]
    journal_id: Option<String>,
}

pub(crate) fn decode_live_snapshot(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    let header: serde_json::Value = serde_json::from_slice(bytes).map_err(io_error)?;
    if header.get("format").is_none() {
        return Ok(bytes.to_vec());
    }
    let snapshot: LiveSnapshot = serde_json::from_slice(bytes).map_err(io_error)?;
    if snapshot.format != "recall-live-v1" || snapshot.sha256 != digest(snapshot.data.as_bytes()) {
        return Err(io_error("live snapshot checksum/version mismatch"));
    }
    Ok(snapshot.data.into_bytes())
}

/// Publish a raw backup with finite retention for its current and previous generations.
pub(crate) fn write_live_snapshot(
    path: &Path,
    bytes: &[u8],
    journal: Option<&ErasureJournal>,
) -> StorageResult<()> {
    write_live_snapshot_with_policy(path, bytes, journal, false)
}

pub(crate) fn write_live_snapshot_with_policy(
    path: &Path,
    bytes: &[u8],
    journal: Option<&ErasureJournal>,
    replace_corrupt: bool,
) -> StorageResult<()> {
    let _backup_guard = journal
        .map(|journal| {
            journal
                .backups
                .try_lock()
                .map_err(|_| StorageError::Conflict("backup file publication is busy".into()))
        })
        .transpose()?;
    let data = std::str::from_utf8(bytes).map_err(io_error)?.to_owned();
    let envelope = serde_json::to_vec(&LiveSnapshot {
        format: "recall-live-v1".into(),
        sha256: digest(bytes),
        data,
        journal_id: journal.map(|j| j.identity().to_owned()),
    })
    .map_err(io_error)?;
    if let Some(journal) = journal {
        // Raw exports are retained backups once live persistence has its own
        // private checkpoint protocol. Register before writing the payload so
        // a crash cannot orphan an unbounded current raw export.
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io_error)?;
        let absolute = parent.canonicalize().map_err(io_error)?.join(
            path.file_name()
                .ok_or_else(|| io_error("snapshot needs a filename"))?,
        );
        let record = RetainedBackup {
            path: absolute,
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            expires_at: Utc::now() + chrono::Duration::days(7),
            raw_sha256: Some(digest(&envelope)),
            deleting: false,
            temporary: false,
        };
        atomic_write(
            &journal
                .backups_path()
                .join(format!("{}.json", record.snapshot_id)),
            &serde_json::to_vec(&record).map_err(io_error)?,
        )?;
    }
    if path.exists() {
        let previous_bytes = fs::read(path).map_err(io_error)?;
        // Never preserve corrupt current bytes as a supposedly good previous generation.
        let validated = decode_live_snapshot(&previous_bytes).and_then(|decoded| {
            elephant_db::ElephantDb::from_snapshot_bytes(&decoded)
                .map(|_| ())
                .map_err(Into::into)
        });
        if let Err(error) = validated {
            if !replace_corrupt {
                return Err(error);
            }
            return journal.map_or_else(
                || atomic_write(path, &envelope),
                |journal| write_registered_file(path, &envelope, journal),
            );
        }
        if let Some(journal) = journal {
            let mut previous = path.as_os_str().to_os_string();
            previous.push(".previous");
            let previous = PathBuf::from(previous);
            let parent = previous
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let previous = parent.canonicalize().map_err(io_error)?.join(
                previous
                    .file_name()
                    .ok_or_else(|| io_error("snapshot needs a filename"))?,
            );
            let record = RetainedBackup {
                path: previous.clone(),
                snapshot_id: uuid::Uuid::new_v4().to_string(),
                expires_at: Utc::now() + chrono::Duration::days(7),
                raw_sha256: Some(digest(&previous_bytes)),
                deleting: false,
                temporary: false,
            };
            // Register before the payload write; a crash cannot orphan unbounded backup data.
            atomic_write(
                &journal
                    .backups_path()
                    .join(format!("{}.json", record.snapshot_id)),
                &serde_json::to_vec(&record).map_err(io_error)?,
            )?;
            write_registered_file(&previous, &previous_bytes, journal)?;
        }
    }
    journal.map_or_else(
        || atomic_write(path, &envelope),
        |journal| write_registered_file(path, &envelope, journal),
    )
}

fn write_registered_file(path: &Path, bytes: &[u8], journal: &ErasureJournal) -> StorageResult<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(io_error)?;
    let snapshot_id = uuid::Uuid::new_v4().to_string();
    let temporary = parent
        .canonicalize()
        .map_err(io_error)?
        .join(format!(".recall-write-{snapshot_id}"));
    let record = RetainedBackup {
        path: temporary.clone(),
        snapshot_id: snapshot_id.clone(),
        expires_at: Utc::now() + chrono::Duration::days(7),
        raw_sha256: Some(digest(bytes)),
        deleting: false,
        temporary: true,
    };
    let registry = journal.backups_path().join(format!("{snapshot_id}.json"));
    atomic_write(&registry, &serde_json::to_vec(&record).map_err(io_error)?)?;
    crate::durability_io::atomic_write_at(path, bytes, &temporary)?;
    fs::remove_file(registry).map_err(io_error)?;
    sync_directory(&journal.backups_path())
}

pub(crate) fn validate_live_journal(bytes: &[u8], journal: &ErasureJournal) -> StorageResult<()> {
    let snapshot: LiveSnapshot = serde_json::from_slice(bytes).map_err(io_error)?;
    if snapshot.journal_id.as_deref() != Some(journal.identity()) {
        return Err(io_error(
            "live snapshot requires its independently retained erasure journal",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::StorageConfig;

    async fn client(backend: StorageBackendKind, root: &Path) -> StorageClient {
        StorageClient::new_with_durability(
            StorageConfig {
                backend,
                ..StorageConfig::default()
            },
            None,
            Some(root.into()),
        )
        .await
        .unwrap()
    }

    async fn put(client: &StorageClient, id: &str) {
        if let Some(store) = client.embedded_store() {
            let row = serde_json::json!({"id":format!("backup_probe:{id}"), "space_id":"named-space", "value":id});
            store
                .mutate(|tx| {
                    tx.upsert_extension_row("backup_probe", row)?;
                    Ok(())
                })
                .await
                .unwrap();
        } else {
            client.query_bind_checked("UPSERT type::thing('backup_probe', $key) SET space_id='named-space', value=$key", serde_json::json!({"key":id})).await.unwrap();
        }
    }

    async fn values(client: &StorageClient) -> Vec<String> {
        let mut values = if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| {
                    Ok(tx
                        .scan_extension_rows("backup_probe")?
                        .into_iter()
                        .map(|(_, row)| row["value"].as_str().unwrap().to_owned())
                        .collect::<Vec<_>>())
                })
                .await
                .unwrap()
        } else {
            let mut response = client
                .query_checked("SELECT VALUE value FROM backup_probe")
                .await
                .unwrap();
            response.take::<Vec<String>>(0).unwrap()
        };
        values.sort();
        values
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn erasure_barrier_drains_late_payload_writes_before_purge() {
        for backend in [
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let client = client(backend, &directory.path().join("journal")).await;
            let (entered, entry) = tokio::sync::oneshot::channel();
            let (finish_work, finishing) = tokio::sync::oneshot::channel();
            let worker = client.clone();
            let task = tokio::spawn(async move {
                worker
                    .with_activity(async {
                        entered.send(()).unwrap();
                        finishing.await.unwrap();
                        put(&worker, "late-payload").await;
                    })
                    .await
                    .unwrap();
            });
            entry.await.unwrap();
            let eraser = client.clone();
            let purge = tokio::spawn(async move {
                eraser
                    .with_erasure_barrier(|exclusive| async move {
                        if let Some(store) = exclusive.embedded_store() {
                            let empty = elephant_db::ElephantDb::new().export_snapshot_bytes()?;
                            store.import_snapshot_bytes(&empty).await?;
                        } else {
                            exclusive.query_checked("DELETE backup_probe").await?;
                        }
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
            tokio::time::timeout(Duration::from_secs(2), async {
                while client.recovery_status().ready {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!purge.is_finished());
            finish_work.send(()).unwrap();
            task.await.unwrap();
            purge.await.unwrap();
            assert!(
                values(&client).await.is_empty(),
                "late stage payload must be purged after its worker drains"
            );
            assert!(client.recovery_status().ready);
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn exhausted_restore_budget_refuses_before_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let client = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("journal"),
        )
        .await;
        put(&client, "retained").await;
        let error = restore_named_with_timeout(
            &client,
            &directory.path().join("not-read"),
            client.erasure_journal().unwrap(),
            Duration::ZERO,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StorageError::Conflict(_)));
        assert_eq!(values(&client).await, vec!["retained"]);
        assert!(client.recovery_status().ready);
        assert!(client
            .erasure_journal()
            .unwrap()
            .restore_marker()
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn restore_replaces_complete_state_on_both_backends() {
        for backend in [
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let client = client(backend, &directory.path().join("journal")).await;
            put(&client, "kept").await;
            let backup = directory.path().join("backup");
            let manifest = export_named(
                &client,
                &backup,
                "named-spaces",
                &SnapshotRetention::default(),
            )
            .await
            .unwrap();
            assert_eq!(verify(&backup).await.unwrap().sha256, manifest.sha256);
            put(&client, "stale-extra").await;
            restore_named(&client, &backup, client.erasure_journal().unwrap())
                .await
                .unwrap();
            assert_eq!(values(&client).await, vec!["kept"]);
            assert!(client.recovery_status().ready);
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn expired_or_tampered_backup_refused_without_changing_live_rows() {
        let directory = tempfile::tempdir().unwrap();
        let client = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("journal"),
        )
        .await;
        put(&client, "original").await;
        let backup = directory.path().join("backup");
        let mut manifest = export_named(&client, &backup, "before", &SnapshotRetention::default())
            .await
            .unwrap();
        put(&client, "later").await;
        let data = fs::read(backup.join(DATA)).unwrap();
        fs::write(backup.join(DATA), b"truncated").unwrap();
        assert!(
            restore_named(&client, &backup, client.erasure_journal().unwrap())
                .await
                .is_err()
        );
        fs::write(backup.join(DATA), data).unwrap();
        manifest.created_at = Utc::now() - chrono::Duration::days(2);
        manifest.expires_at = Utc::now() - chrono::Duration::days(1);
        fs::write(
            backup.join(MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert!(verify(&backup).await.is_err());
        assert!(
            restore_named(&client, &backup, client.erasure_journal().unwrap())
                .await
                .is_err()
        );
        assert_eq!(values(&client).await, vec!["later", "original"]);
        assert!(client.recovery_status().ready);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn retained_cleanup_physically_deletes_only_owned_expired_generations() {
        let directory = tempfile::tempdir().unwrap();
        let client = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("journal"),
        )
        .await;
        let backup = directory.path().join("owned");
        let manifest = export_named(&client, &backup, "owned", &SnapshotRetention::default())
            .await
            .unwrap();
        let foreign = directory.path().join("not-owned");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join(DATA), b"do not remove").unwrap();
        assert_eq!(
            cleanup_at(
                client.erasure_journal().unwrap(),
                manifest.expires_at + chrono::Duration::seconds(1)
            )
            .unwrap(),
            1
        );
        assert!(!backup.exists());
        assert!(foreign.join(DATA).exists());
        assert_eq!(
            cleanup_at(
                client.erasure_journal().unwrap(),
                manifest.expires_at + chrono::Duration::seconds(2)
            )
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn interrupted_cleanup_and_partial_live_temporary_are_deleted_idempotently() {
        let directory = tempfile::tempdir().unwrap();
        let client = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("journal"),
        )
        .await;
        let journal = client.erasure_journal().unwrap();
        let backup = directory.path().join("owned");
        let manifest = export_named(
            &client,
            &backup,
            "delete-interrupted",
            &SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let registry = journal
            .backups_path()
            .join(format!("{}.json", manifest.snapshot_id));
        let mut record: RetainedBackup =
            serde_json::from_slice(&fs::read(&registry).unwrap()).unwrap();
        record.deleting = true;
        atomic_write(&registry, &serde_json::to_vec(&record).unwrap()).unwrap();
        fs::remove_file(backup.join(MANIFEST)).unwrap();
        let temporary_id = uuid::Uuid::new_v4().to_string();
        let temporary = directory
            .path()
            .join(format!(".recall-write-{temporary_id}"));
        fs::write(&temporary, b"crashed partial payload").unwrap();
        let partial = RetainedBackup {
            path: temporary.clone(),
            snapshot_id: temporary_id.clone(),
            expires_at: manifest.expires_at,
            raw_sha256: Some(digest(b"full expected payload")),
            deleting: false,
            temporary: true,
        };
        atomic_write(
            &journal.backups_path().join(format!("{temporary_id}.json")),
            &serde_json::to_vec(&partial).unwrap(),
        )
        .unwrap();
        assert_eq!(
            cleanup_at(journal, manifest.expires_at + chrono::Duration::seconds(1)).unwrap(),
            2
        );
        assert!(!temporary.exists());
        assert!(!backup.exists());
        assert_eq!(
            cleanup_at(journal, manifest.expires_at + chrono::Duration::seconds(2)).unwrap(),
            0
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn restore_requires_original_journal_and_keeps_it_outside_backup() {
        let directory = tempfile::tempdir().unwrap();
        let source = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("original-journal"),
        )
        .await;
        let backup = directory.path().join("backup");
        export_named(&source, &backup, "copy", &SnapshotRetention::default())
            .await
            .unwrap();
        let other = client(
            StorageBackendKind::ElephantDb,
            &directory.path().join("empty-journal"),
        )
        .await;
        assert!(
            restore_named(&other, &backup, other.erasure_journal().unwrap())
                .await
                .is_err()
        );
        assert_eq!(fs::read_dir(&backup).unwrap().count(), 2);
        assert!(other.recovery_status().ready);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn interrupted_restore_blocks_restart_until_explicit_resumption() {
        for backend in [
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let journal = directory.path().join("journal");
            let first = client(backend, &journal).await;
            put(&first, "retained").await;
            let backup = directory.path().join("backup");
            let manifest = export_named(
                &first,
                &backup,
                "interruption",
                &SnapshotRetention::default(),
            )
            .await
            .unwrap();
            first
                .erasure_journal()
                .unwrap()
                .begin_restore(&RestoreMarker {
                    snapshot_id: manifest.snapshot_id,
                    sha256: manifest.sha256,
                })
                .unwrap();
            drop(first);
            let reopened = client(backend, &journal).await;
            assert!(!reopened.recovery_status().ready);
            assert!(reopened.health_check().await.is_err());
            if let Some(store) = reopened.embedded_store() {
                assert!(store.is_empty().await.is_err());
            }
            restore_named(&reopened, &backup, reopened.erasure_journal().unwrap())
                .await
                .unwrap();
            assert_eq!(values(&reopened).await, vec!["retained"]);
            assert!(reopened.recovery_status().ready);
            assert!(reopened
                .erasure_journal()
                .unwrap()
                .restore_marker()
                .unwrap()
                .is_none());
        }
    }

    async fn sign_restore_fault_fixture(
        client: &StorageClient,
        backup: &Path,
        manifest: &SnapshotManifest,
    ) {
        let journal = client.erasure_journal().unwrap();
        let native = crate::source_history_restore::inventory(
            client,
            Instant::now() + Duration::from_secs(300),
        )
        .await
        .unwrap();
        let causality = crate::source_history_authority::backup_versionless_history_causality(
            client, journal, &native,
        )
        .await
        .unwrap();
        crate::source_history_restore::write_backup(journal, backup, manifest, &native, causality)
            .unwrap();
    }

    async fn serve_restore_fault_responses(listener: tokio::net::TcpListener) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tokio::time::timeout(Duration::from_secs(60), async move {
            for response in ["false", "true"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0_u8; 1024];
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert!(count > 0, "restore fault request ended before headers");
                    request.extend_from_slice(&chunk[..count]);
                    assert!(request.len() <= 8192, "restore fault request exceeds bound");
                }
                assert!(request.starts_with(b"GET /restore-fault HTTP/1.1\r\n"));
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                );
                stream.write_all(reply.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn partial_surreal_restore_failure_does_not_report_ready() {
        let directory = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let client = StorageClient::new_native_with_test_barrier(
            StorageConfig {
                backend: StorageBackendKind::SurrealDb,
                ..StorageConfig::default()
            },
            directory.path().join("journal"),
            address,
        )
        .await
        .unwrap();
        put(&client, "retained").await;
        let backup = directory.path().join("backup");
        let mut manifest = export_named(
            &client,
            &backup,
            "restore-fault",
            &SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let original = fs::read_to_string(backup.join(DATA)).unwrap();
        let journal = client.erasure_journal().unwrap();
        // Both attempts use identical authenticated bytes and the same marker.
        // Only the owned test server's reply changes; production constructors
        // do not enable this loopback capability.
        let fault = format!(
            "{original}\nLET $resume = http::get('http://{address}/restore-fault'); \
             IF $resume {{ RETURN true; }} \
             ELSE {{ THROW 'injected restore failure'; }};"
        );
        fs::write(backup.join(DATA), &fault).unwrap();
        manifest.bytes = fault.len() as u64;
        manifest.sha256 = digest(fault.as_bytes());
        fs::write(
            backup.join(MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        sign_restore_fault_fixture(&client, &backup, &manifest).await;
        // Only authenticated original values may appear after the import floor.
        // Remove the live value so its return proves the failed import wrote it.
        client.query_checked("DELETE backup_probe").await.unwrap();
        assert!(values(&client).await.is_empty());
        let server = tokio::spawn(serve_restore_fault_responses(listener));
        let error = restore_named(&client, &backup, journal).await.unwrap_err();
        assert!(error.to_string().contains("injected restore failure"));
        assert!(!client.recovery_status().ready);
        assert_eq!(
            journal.restore_marker().unwrap().unwrap().sha256,
            manifest.sha256
        );
        // The raw test handle proves a partial import committed; ordinary
        // callers still cannot query this incomplete replacement.
        let partial: Vec<String> = client
            .db()
            .unwrap()
            .query("SELECT VALUE value FROM backup_probe")
            .await
            .unwrap()
            .check()
            .unwrap()
            .take(0)
            .unwrap();
        assert_eq!(partial, vec!["retained"]);
        assert!(client
            .query_checked("SELECT * FROM backup_probe")
            .await
            .is_err());
        restore_named(&client, &backup, journal).await.unwrap();
        server.await.unwrap();
        assert_eq!(fs::read(backup.join(DATA)).unwrap(), fault.as_bytes());
        assert_eq!(values(&client).await, vec!["retained"]);
        assert!(client.recovery_status().ready);
        assert!(journal.restore_marker().unwrap().is_none());
    }

    #[tokio::test]
    async fn erasure_barrier_persists_final_callback_receipt_before_success() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.snapshot");
        let journal = directory.path().join("journal");
        let config = StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        };
        let client = StorageClient::new_with_durability(
            config.clone(),
            Some(path.clone()),
            Some(journal.clone()),
        )
        .await
        .unwrap();
        client
            .with_erasure_barrier(|exclusive| async move {
                put(&exclusive, "completed-receipt").await;
                Ok(())
            })
            .await
            .unwrap();
        drop(client);
        let reopened = StorageClient::new_with_durability(
            StorageConfig {
                snapshot_load: Some(path.clone()),
                ..config
            },
            Some(path),
            Some(journal),
        )
        .await
        .unwrap();
        assert_eq!(values(&reopened).await, vec!["completed-receipt"]);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn raw_snapshot_and_previous_generation_both_have_finite_retention() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.snapshot");
        let client = StorageClient::new_with_durability(
            StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            Some(directory.path().join("primary.store")),
            Some(directory.path().join("journal")),
        )
        .await
        .unwrap();
        put(&client, "first").await;
        client.export_snapshot(&path).await.unwrap();
        let original = fs::read(&path).unwrap();
        put(&client, "second").await;
        client.export_snapshot(&path).await.unwrap();
        assert_eq!(
            fs::read(directory.path().join("live.snapshot.previous")).unwrap(),
            original
        );
        assert!(decode_live_snapshot(&fs::read(&path).unwrap()).is_ok());
        assert_eq!(
            cleanup_at(
                client.erasure_journal().unwrap(),
                Utc::now() + chrono::Duration::days(8)
            )
            .unwrap(),
            2
        );
        assert!(!path.exists());
        assert!(directory.path().join("primary.store").exists());
        assert!(!directory.path().join("live.snapshot.previous").exists());
    }
}
