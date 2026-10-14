//! Exact surviving-version copies for an externally owned physical generation
//! transition. This module never promotes or removes a database directory: the
//! caller owns exclusive admission, durable promotion and retirement proof.

use crate::entry::{Entry, Record};
use crate::log::{Aol, Options as LogOptions};
use crate::manifest::Manifest;
use crate::store::Core;
use crate::{Error, Result, Store};
use bytes::BytesMut;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

const CLOCK_FILE: &str = "recall-checkpoint.clock";
const CLOCK_MAGIC: &[u8; 8] = b"SKVCLK01";

/// Logical identity of a checkpoint stream, including the deleted-key clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointSummary {
    /// Number of surviving versions, including retained soft tombstones.
    pub records: u64,
    /// Highest previously allocated transaction version.
    pub high_water: u64,
}

/// One immutable native historical version offered to the database's erasure
/// authority. Engine code cannot infer source ownership from these bytes.
#[derive(Clone, Copy)]
pub struct CheckpointRecord<'a> {
    /// Encoded native database key, including its namespace and database.
    pub key: &'a [u8],
    /// Exact native payload before any decoding or source projection.
    pub value: &'a [u8],
    /// Committed transaction version.
    pub version: u64,
    /// Original record timestamp.
    pub timestamp: u64,
    /// Whether this version hides the key without removing older versions.
    pub tombstone: bool,
    /// This is the exact latest native version, including a soft tombstone.
    pub current: bool,
}

/// Selection is supplied by the database's original source-erasure authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDecision {
    /// Preserve the original encoded key/value/version/timestamp/metadata.
    Keep,
    /// Remove the value while retaining its historical absence boundary.
    Tombstone,
    /// Omit this version, only after proving its key has no independent history.
    Drop,
}

fn failure(message: &str) -> Error {
    Error::CustomError(format!("physical checkpoint: {message}"))
}

fn remaining(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(failure("original deadline expired"));
    }
    Ok(())
}

pub(crate) fn read_clock(directory: &Path) -> Result<u64> {
    let path = directory.join(CLOCK_FILE);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 20 => {}
        Ok(_) => return Err(failure("invalid persisted checkpoint clock")),
    }
    let bytes = fs::read(path)?;
    if bytes.len() != 20 || &bytes[..8] != CLOCK_MAGIC {
        return Err(failure("invalid persisted checkpoint clock"));
    }
    let mut number = [0; 8];
    number.copy_from_slice(&bytes[8..16]);
    let mut checksum = [0; 4];
    checksum.copy_from_slice(&bytes[16..]);
    let high_water = u64::from_be_bytes(number);
    if crc32fast::hash(&bytes[..16]) != u32::from_be_bytes(checksum) || high_water == u64::MAX {
        return Err(failure("checkpoint clock checksum or range differs"));
    }
    Ok(high_water)
}

fn write_clock(directory: &Path, high_water: u64) -> Result<()> {
    let mut bytes = Vec::from(*CLOCK_MAGIC);
    bytes.extend_from_slice(&high_water.to_be_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join(CLOCK_FILE))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

impl Store {
    fn checkpoint_ready(&self, deadline: Instant) -> Result<()> {
        remaining(deadline)?;
        if self.is_closed.load(Ordering::Acquire) || !self.core.opts.disk_persistence {
            return Err(failure("an open persistent store is required"));
        }
        if self.is_compacting.load(Ordering::Acquire) || Arc::strong_count(&self.core) != 1 {
            return Err(failure(
                "all original transactions and compaction must drain",
            ));
        }
        Ok(())
    }

    /// Visit the exact surviving version stream in stable key/version order.
    ///
    /// Encoded native records retain keys, values, version IDs, timestamps and
    /// tombstone metadata. Hard-deleted keys are absent from the live index.
    /// Soft-deleted history is deliberately preserved unless the separate
    /// source-authorized historical selector explicitly transforms it.
    /// No callback may start another transaction on this store.
    pub fn visit_checkpoint_records(
        &mut self,
        deadline: Instant,
        visit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<CheckpointSummary> {
        self.visit_selected_checkpoint_records(deadline, |_| Ok(CheckpointDecision::Keep), visit)
    }

    /// Visit every historical version, including keys absent from current reads.
    /// The owner can prepare an immutable group policy without changing the store.
    pub fn visit_checkpoint_versions(
        &mut self,
        deadline: Instant,
        mut visit: impl FnMut(&CheckpointRecord<'_>) -> Result<()>,
    ) -> Result<CheckpointSummary> {
        self.visit_selected_checkpoint_records(
            deadline,
            |record| {
                visit(record)?;
                Ok(CheckpointDecision::Keep)
            },
            |_| Ok(()),
        )
    }

    /// Visit a source-authorized subset of historical versions. `retain` must
    /// be supplied by the owning database's immutable erasure authority; this
    /// API does not authorize deletion or treat unknown ownership as erased.
    pub fn visit_selected_checkpoint_records(
        &mut self,
        deadline: Instant,
        mut retain: impl FnMut(&CheckpointRecord<'_>) -> Result<CheckpointDecision>,
        mut visit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<CheckpointSummary> {
        self.checkpoint_ready(deadline)?;
        let index = self.core.indexer.read();
        let high_water = index.version().max(self.core.oracle.read_ts());
        if high_water == u64::MAX {
            return Err(failure("transaction clock exhausted"));
        }
        let snapshot = index.snapshot();
        drop(index);
        let mut records = 0u64;
        let mut encoded = BytesMut::new();
        for (key, value, version, timestamp) in snapshot.iter_with_versions() {
            remaining(deadline)?;
            let resolved = value.resolve(&self.core)?;
            let latest = snapshot
                .get(&key.into(), high_water)
                .ok_or_else(|| failure("historical key has no current native version"))?;
            let decision = retain(&CheckpointRecord {
                key,
                value: &resolved,
                version,
                timestamp,
                tombstone: value
                    .metadata()
                    .is_some_and(|metadata| metadata.is_tombstone()),
                current: latest.1 == version && latest.2 == timestamp,
            })?;
            let mut entry = match decision {
                CheckpointDecision::Drop => continue,
                CheckpointDecision::Keep => {
                    let mut entry = Entry::new(key, &resolved);
                    if let Some(metadata) = value.metadata() {
                        entry.set_metadata(metadata.clone());
                    }
                    entry
                }
                CheckpointDecision::Tombstone => {
                    let mut entry = Entry::new(key, &[]);
                    entry.mark_tombstone();
                    entry
                }
            };
            entry.set_ts(timestamp);
            encoded.clear();
            Record::from_entry(entry, version).encode(&mut encoded)?;
            visit(&encoded)?;
            records = records
                .checked_add(1)
                .ok_or_else(|| failure("record count exhausted"))?;
        }
        remaining(deadline)?;
        Ok(CheckpointSummary {
            records,
            high_water,
        })
    }

    /// Write an entirely new native generation without copying retired bytes.
    ///
    /// The supplied directory must exist, be a real empty directory and be
    /// owned by the caller's durable generation transaction. The source store
    /// remains open and unchanged on error. A successful copy is *not* a
    /// physical-erasure certificate: the caller must independently reopen and
    /// compare it, close every original handle, promote it and retire the old
    /// directory before admitting new work.
    pub fn write_checkpoint(
        &mut self,
        directory: &Path,
        deadline: Instant,
    ) -> Result<CheckpointSummary> {
        self.write_selected_checkpoint(directory, deadline, |_| Ok(CheckpointDecision::Keep))
    }

    /// Copy only explicitly retained versions. Unknown or failed authority
    /// must return an error; the source generation remains intact on refusal.
    pub fn write_selected_checkpoint(
        &mut self,
        directory: &Path,
        deadline: Instant,
        retain: impl FnMut(&CheckpointRecord<'_>) -> Result<CheckpointDecision>,
    ) -> Result<CheckpointSummary> {
        self.checkpoint_ready(deadline)?;
        if !fs::symlink_metadata(directory)?.file_type().is_dir()
            || fs::read_dir(directory)?.next().is_some()
        {
            return Err(failure(
                "checkpoint destination must be an empty real directory",
            ));
        }
        let manifest = Core::initialize_manifest(directory)?;
        // Keep the final store options, not a transient staging directory.
        manifest.append(&Manifest::with_update_option_change(&self.core.opts).serialize()?)?;
        manifest.sync()?;
        manifest.close()?;
        drop(manifest);
        let options = LogOptions::default()
            .with_max_file_size(self.core.opts.max_segment_size)
            .with_file_extension("clog".to_string());
        let log = Aol::open(&directory.join("clog"), &options)?;
        let summary = self.visit_selected_checkpoint_records(deadline, retain, |record| {
            log.append(record)?;
            Ok(())
        })?;
        log.sync()?;
        log.close()?;
        drop(log);
        write_clock(directory, summary.high_water)?;
        for path in [
            directory.join("clog"),
            directory.join("manifest"),
            directory.to_path_buf(),
        ] {
            remaining(deadline)?;
            File::open(path)?.sync_all()?;
        }
        remaining(deadline)?;
        Ok(summary)
    }
}

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;
