//! Managed embedded transaction log and private live checkpoints.
//!
//! Commit frames are synced while the kernel still owns its write lock. These
//! files are recovery state, not user backups: Running leases remain intact.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use elephant_db::{ElephantDb, KernelMutation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::durability_io::{io_error, private_directory, sync_directory};
use crate::erasure_journal::ErasureJournal;
use crate::recovery_barrier::RecoveryBarrier;
use crate::StorageResult;

const CHECKPOINT_FORMAT: &str = "recall-embedded-checkpoint-v1";
const WAL_MAGIC: &[u8; 8] = b"RCLOG001";
const FRAME_MAGIC: &[u8; 8] = b"RCDEL001";
const COMMIT_MAGIC: &[u8; 8] = b"COMMIT01";
const FRAME_HEADER_BYTES: usize = 88;
const FRAME_HEADER: u64 = FRAME_HEADER_BYTES as u64;
const MAX_FRAME_BYTES: u64 = 256 * 1024 * 1024;
const CHECKPOINT_AFTER_BYTES: u64 = 64 * 1024 * 1024;
const HEADER_LIMIT: usize = 4096;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreIdentity {
    version: u32,
    store_id: String,
    journal_id: String,
    initialized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalHeader {
    version: u32,
    store_id: String,
    journal_id: String,
    base_sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCheckpoint {
    format: String,
    store_id: String,
    journal_id: String,
    sequence: u64,
    sha256: String,
    data: String,
}

#[derive(Debug)]
struct WalState {
    file: File,
    sequence: u64,
    bytes: u64,
}

#[derive(Debug)]
pub struct EmbeddedDurability {
    checkpoint_path: PathBuf,
    directory: PathBuf,
    store_id: String,
    journal_id: String,
    state: Mutex<WalState>,
    failed: AtomicBool,
    recovery: Arc<RecoveryBarrier>,
    #[cfg(test)]
    fail_next_append: AtomicBool,
    #[cfg(test)]
    fail_after_append_sync: AtomicBool,
    #[cfg(test)]
    fail_after_checkpoint_publish: AtomicBool,
}

impl EmbeddedDurability {
    /// Called with the physical destination writer lock already held, before
    /// any store handle escapes or ordinary storage admission becomes ready.
    #[allow(
        clippy::too_many_lines,
        reason = "Retain the explicit startup order for checkpoint identity, original journal, replay and durability ownership."
    )]
    pub(crate) fn open(
        path: &Path,
        journal: &ErasureJournal,
        recovery: Arc<RecoveryBarrier>,
    ) -> StorageResult<(ElephantDb, Arc<Self>)> {
        let checkpoint_path = canonical_destination(path)?;
        let mut directory = checkpoint_path.as_os_str().to_os_string();
        directory.push(".durable");
        let directory = PathBuf::from(directory);
        if journal.path().starts_with(&directory) {
            return Err(io_error(
                "independent erasure journal cannot be inside live recovery files",
            ));
        }
        if directory.exists()
            && !fs::symlink_metadata(&directory)
                .map_err(io_error)?
                .file_type()
                .is_dir()
        {
            return Err(io_error("live recovery directory must be a real directory"));
        }
        private_directory(&directory)?;
        sync_directory(parent(&directory))?;
        let identity_path = directory.join("identity.json");
        let mut identity: StoreIdentity = if identity_path.exists() {
            read_json(&identity_path)?
        } else {
            let identity = StoreIdentity {
                version: 1,
                store_id: uuid::Uuid::new_v4().to_string(),
                journal_id: journal.identity().to_owned(),
                initialized: false,
            };
            publish_owned(
                &identity_path,
                &serde_json::to_vec(&identity).map_err(io_error)?,
                &directory.join("identity.next"),
            )?;
            identity
        };
        if identity.version != 1
            || uuid::Uuid::parse_str(&identity.store_id).is_err()
            || identity.journal_id != journal.identity()
        {
            return Err(io_error(
                "live recovery identity requires its original erasure journal",
            ));
        }
        let (db, checkpoint_sequence, private_checkpoint) = if checkpoint_path.exists() {
            ensure_regular(&checkpoint_path)?;
            let bytes = fs::read(&checkpoint_path).map_err(io_error)?;
            let format: serde_json::Value = serde_json::from_slice(&bytes).map_err(io_error)?;
            if format.get("format").and_then(serde_json::Value::as_str) == Some(CHECKPOINT_FORMAT) {
                let checkpoint: LiveCheckpoint =
                    serde_json::from_slice(&bytes).map_err(io_error)?;
                if checkpoint.store_id != identity.store_id
                    || checkpoint.journal_id != identity.journal_id
                    || checkpoint.sha256
                        != checkpoint_checksum(
                            &checkpoint.store_id,
                            &checkpoint.journal_id,
                            checkpoint.sequence,
                            checkpoint.data.as_bytes(),
                        )
                {
                    return Err(io_error("live checkpoint identity/checksum mismatch"));
                }
                (
                    ElephantDb::from_recovery_snapshot_bytes(checkpoint.data.as_bytes())?,
                    checkpoint.sequence,
                    true,
                )
            } else {
                if identity.initialized {
                    return Err(io_error("live checkpoint replaced outside guarded restore"));
                }
                // Retain the existing preview's checked live-snapshot adoption.
                // This is not a generic legacy database migration.
                crate::snapshot::validate_live_journal(&bytes, journal)?;
                let data = crate::snapshot::decode_live_snapshot(&bytes)?;
                (ElephantDb::from_snapshot_bytes(&data)?, 0, false)
            }
        } else {
            if identity.initialized {
                return Err(io_error("live checkpoint missing for initialized store"));
            }
            (ElephantDb::new(), 0, false)
        };
        let wal_path = directory.join("commits.wal");
        if !wal_path.exists() {
            if identity.initialized || private_checkpoint {
                return Err(io_error("live transaction log missing"));
            }
            let header = header_bytes(&identity.store_id, &identity.journal_id, 0)?;
            publish_owned(&wal_path, &header, &directory.join("commits.next"))?;
        }
        ensure_regular(&wal_path)?;
        let mut file = open_log(&wal_path)?;
        let (sequence, bytes) = replay(&mut file, &db, &identity, checkpoint_sequence)?;
        let durability = Arc::new(Self {
            checkpoint_path,
            directory,
            store_id: identity.store_id.clone(),
            journal_id: identity.journal_id.clone(),
            state: Mutex::new(WalState {
                file,
                sequence,
                bytes,
            }),
            failed: AtomicBool::new(false),
            recovery,
            #[cfg(test)]
            fail_next_append: AtomicBool::new(false),
            #[cfg(test)]
            fail_after_append_sync: AtomicBool::new(false),
            #[cfg(test)]
            fail_after_checkpoint_publish: AtomicBool::new(false),
        });
        // Finish interrupted initialization, retire already checkpointed frames,
        // and ensure there is a durable baseline before the first admission.
        if !private_checkpoint || !identity.initialized || sequence == checkpoint_sequence {
            durability.checkpoint(&db.export_snapshot_bytes()?, false)?;
        }
        if !identity.initialized {
            identity.initialized = true;
            publish_owned(
                &identity_path,
                &serde_json::to_vec(&identity).map_err(io_error)?,
                &durability.directory.join("identity.next"),
            )?;
        }
        // Only reserved files inside this store-owned private directory are
        // removed. No failed checkpoint payload survives readiness indefinitely.
        for name in ["checkpoint.next", "commits.next", "identity.next"] {
            remove_owned_temporary(&durability.directory.join(name))?;
        }
        sync_directory(&durability.directory)?;
        Ok((db, durability))
    }

    pub(crate) fn ensure_healthy(&self) -> StorageResult<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io_error(
                "live durability failed; restart and replay before further access",
            ));
        }
        Ok(())
    }

    pub(crate) fn poison(&self) {
        self.failed.store(true, Ordering::Release);
        self.recovery
            .fail_until_restart("live durability failed; restart recovery required".into());
    }

    /// Runs synchronously under the kernel write lock. The caller rolls memory
    /// back on any returned error; an ambiguous durable suffix poisons this run.
    pub(crate) fn append(&self, mutations: &[KernelMutation]) -> StorageResult<()> {
        self.ensure_healthy()?;
        if mutations.is_empty() {
            return Ok(());
        }
        let payload = serde_json::to_vec(mutations).map_err(io_error)?;
        let length = u64::try_from(payload.len()).map_err(io_error)?;
        if length > MAX_FRAME_BYTES {
            return Err(io_error(
                "embedded transaction exceeds 256 MiB live-log frame limit",
            ));
        }
        let result = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|_| io_error("live transaction log lock poisoned"))?;
            self.ensure_healthy()?;
            let sequence = state
                .sequence
                .checked_add(1)
                .ok_or_else(|| io_error("live transaction sequence exhausted"))?;
            let checksum = frame_checksum(sequence, length, &payload);
            #[cfg(test)]
            if self.fail_next_append.swap(false, Ordering::AcqRel) {
                state.file.write_all(&FRAME_MAGIC[..3]).map_err(io_error)?;
                return Err(io_error("injected append failure"));
            }
            state.file.write_all(FRAME_MAGIC).map_err(io_error)?;
            state
                .file
                .write_all(&sequence.to_be_bytes())
                .map_err(io_error)?;
            state
                .file
                .write_all(&length.to_be_bytes())
                .map_err(io_error)?;
            state.file.write_all(&checksum).map_err(io_error)?;
            state
                .file
                .write_all(&frame_header_checksum(sequence, length, &checksum))
                .map_err(io_error)?;
            state.file.write_all(&payload).map_err(io_error)?;
            state.file.write_all(COMMIT_MAGIC).map_err(io_error)?;
            state.file.sync_all().map_err(io_error)?;
            #[cfg(test)]
            if self.fail_after_append_sync.swap(false, Ordering::AcqRel) {
                return Err(io_error("injected failure after durable append"));
            }
            state.sequence = sequence;
            state.bytes = state
                .bytes
                .checked_add(FRAME_HEADER + length + 8)
                .ok_or_else(|| io_error("live log size overflow"))?;
            drop(state);
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }

    pub(crate) fn needs_checkpoint(&self) -> StorageResult<bool> {
        self.ensure_healthy()?;
        Ok(self
            .state
            .lock()
            .map_err(|_| io_error("live transaction log lock poisoned"))?
            .bytes
            >= CHECKPOINT_AFTER_BYTES)
    }

    /// Caller excludes all mutations with the store import barrier.
    ///
    /// Publish the
    /// complete current state before replacing the log; never retain a previous
    /// live checkpoint. Replacement advances the boundary even without deltas.
    pub(crate) fn checkpoint(&self, bytes: &[u8], replacement: bool) -> StorageResult<()> {
        self.ensure_healthy()?;
        let result = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|_| io_error("live transaction log lock poisoned"))?;
            let sequence = if replacement {
                state
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| io_error("live transaction sequence exhausted"))?
            } else {
                state.sequence
            };
            let checkpoint = LiveCheckpoint {
                format: CHECKPOINT_FORMAT.into(),
                store_id: self.store_id.clone(),
                journal_id: self.journal_id.clone(),
                sequence,
                sha256: checkpoint_checksum(&self.store_id, &self.journal_id, sequence, bytes),
                data: std::str::from_utf8(bytes).map_err(io_error)?.to_owned(),
            };
            publish_owned(
                &self.checkpoint_path,
                &serde_json::to_vec(&checkpoint).map_err(io_error)?,
                &self.directory.join("checkpoint.next"),
            )?;
            #[cfg(test)]
            if self
                .fail_after_checkpoint_publish
                .swap(false, Ordering::AcqRel)
            {
                return Err(io_error("injected failure after checkpoint publication"));
            }
            let header = header_bytes(&self.store_id, &self.journal_id, sequence)?;
            let wal_path = self.directory.join("commits.wal");
            publish_owned(&wal_path, &header, &self.directory.join("commits.next"))?;
            state.file = open_log(&wal_path)?;
            state.sequence = sequence;
            state.bytes = u64::try_from(header.len()).map_err(io_error)?;
            drop(state);
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }

    pub(crate) fn owns_destination(&self, path: &Path) -> StorageResult<bool> {
        Ok(canonical_destination(path)? == self.checkpoint_path)
    }
}

fn canonical_destination(path: &Path) -> StorageResult<PathBuf> {
    if path.exists() {
        let canonical = path.canonicalize().map_err(io_error)?;
        ensure_regular(&canonical)?;
        return Ok(canonical);
    }
    let name = path
        .file_name()
        .ok_or_else(|| io_error("live store needs a file name"))?;
    fs::create_dir_all(parent(path)).map_err(io_error)?;
    Ok(parent(path).canonicalize().map_err(io_error)?.join(name))
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn ensure_regular(path: &Path) -> StorageResult<()> {
    if !fs::symlink_metadata(path)
        .map_err(io_error)?
        .file_type()
        .is_file()
    {
        return Err(io_error("live durability path must be a regular file"));
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> StorageResult<T> {
    ensure_regular(path)?;
    serde_json::from_slice(&fs::read(path).map_err(io_error)?).map_err(io_error)
}

fn open_log(path: &Path) -> StorageResult<File> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(io_error)
}

fn remove_owned_temporary(path: &Path) -> StorageResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => fs::remove_file(path).map_err(io_error),
        Ok(_) => Err(io_error(
            "reserved durability temporary is not a regular file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn publish_owned(path: &Path, bytes: &[u8], temporary: &Path) -> StorageResult<()> {
    remove_owned_temporary(temporary)?;
    crate::durability_io::atomic_write_at(path, bytes, temporary)?;
    if parent(path) != parent(temporary) {
        sync_directory(parent(temporary))?;
    }
    Ok(())
}

fn checkpoint_checksum(store_id: &str, journal_id: &str, sequence: u64, bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(CHECKPOINT_FORMAT);
    hash.update([0]);
    hash.update(store_id);
    hash.update([0]);
    hash.update(journal_id);
    hash.update([0]);
    hash.update(sequence.to_be_bytes());
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}

fn header_bytes(store_id: &str, journal_id: &str, base_sequence: u64) -> StorageResult<Vec<u8>> {
    let payload = serde_json::to_vec(&WalHeader {
        version: 1,
        store_id: store_id.into(),
        journal_id: journal_id.into(),
        base_sequence,
    })
    .map_err(io_error)?;
    let length = u32::try_from(payload.len()).map_err(io_error)?;
    let mut bytes = Vec::with_capacity(12 + payload.len() + 32);
    bytes.extend_from_slice(WAL_MAGIC);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&Sha256::digest(&payload));
    Ok(bytes)
}

fn frame_checksum(sequence: u64, length: u64, payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(sequence.to_be_bytes());
    hash.update(length.to_be_bytes());
    hash.update(payload);
    hash.finalize().into()
}

fn frame_header_checksum(sequence: u64, length: u64, payload_checksum: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(FRAME_MAGIC);
    hash.update(sequence.to_be_bytes());
    hash.update(length.to_be_bytes());
    hash.update(payload_checksum);
    hash.finalize().into()
}

fn replay(
    file: &mut File,
    db: &ElephantDb,
    identity: &StoreIdentity,
    checkpoint_sequence: u64,
) -> StorageResult<(u64, u64)> {
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut prefix = [0_u8; 12];
    file.read_exact(&mut prefix).map_err(io_error)?;
    if &prefix[..8] != WAL_MAGIC {
        return Err(io_error("live log header magic mismatch"));
    }
    let length = u32::from_be_bytes(prefix[8..12].try_into().map_err(io_error)?) as usize;
    if length > HEADER_LIMIT {
        return Err(io_error("live log header length invalid"));
    }
    let mut payload = vec![0; length];
    file.read_exact(&mut payload).map_err(io_error)?;
    let mut checksum = [0; 32];
    file.read_exact(&mut checksum).map_err(io_error)?;
    if checksum != <[u8; 32]>::from(Sha256::digest(&payload)) {
        return Err(io_error("live log header checksum mismatch"));
    }
    let header: WalHeader = serde_json::from_slice(&payload).map_err(io_error)?;
    if header.version != 1
        || header.store_id != identity.store_id
        || header.journal_id != identity.journal_id
        || header.base_sequence > checkpoint_sequence
    {
        return Err(io_error("live log identity/checkpoint boundary mismatch"));
    }
    let size = file.metadata().map_err(io_error)?.len();
    let mut offset = file.stream_position().map_err(io_error)?;
    let mut sequence = header.base_sequence;
    while offset < size {
        if size - offset < FRAME_HEADER {
            // Even a torn header must begin with the protocol magic; arbitrary
            // trailing garbage is not treated as an uncommitted transaction.
            let available = usize::try_from((size - offset).min(8)).map_err(io_error)?;
            let mut magic = [0_u8; 8];
            file.read_exact(&mut magic[..available]).map_err(io_error)?;
            if magic[..available] != FRAME_MAGIC[..available] {
                return Err(io_error("incomplete live log frame magic mismatch"));
            }
            break;
        }
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        file.read_exact(&mut header).map_err(io_error)?;
        if &header[..8] != FRAME_MAGIC {
            return Err(io_error("live log frame magic mismatch"));
        }
        let next = u64::from_be_bytes(header[8..16].try_into().map_err(io_error)?);
        let length = u64::from_be_bytes(header[16..24].try_into().map_err(io_error)?);
        let payload_checksum: [u8; 32] = header[24..56].try_into().map_err(io_error)?;
        if header[56..88] != frame_header_checksum(next, length, &payload_checksum) {
            return Err(io_error("live log frame header checksum mismatch"));
        }
        if length > MAX_FRAME_BYTES
            || next
                != sequence
                    .checked_add(1)
                    .ok_or_else(|| io_error("live log sequence exhausted"))?
        {
            return Err(io_error("live log frame length/sequence mismatch"));
        }
        let frame_size = FRAME_HEADER + length + 8;
        if size - offset < frame_size {
            break;
        }
        let mut payload = vec![0; usize::try_from(length).map_err(io_error)?];
        file.read_exact(&mut payload).map_err(io_error)?;
        let mut footer = [0_u8; 8];
        file.read_exact(&mut footer).map_err(io_error)?;
        if &footer != COMMIT_MAGIC || header[24..56] != frame_checksum(next, length, &payload) {
            return Err(io_error("committed live log frame checksum mismatch"));
        }
        let changes: Vec<KernelMutation> = serde_json::from_slice(&payload).map_err(io_error)?;
        if changes.is_empty() {
            return Err(io_error("empty committed transaction frame"));
        }
        if next > checkpoint_sequence {
            db.apply_recovery_batch(&changes)?;
        }
        sequence = next;
        offset += frame_size;
    }
    if offset < size {
        // A trailing incomplete frame was never acknowledged by this protocol.
        // Whole checksum-invalid frames are corruption and fail closed above.
        file.set_len(offset).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
    }
    file.seek(SeekFrom::End(0)).map_err(io_error)?;
    Ok((sequence.max(checkpoint_sequence), offset))
}

#[cfg(test)]
#[path = "embedded_durability_tests.rs"]
mod tests;
