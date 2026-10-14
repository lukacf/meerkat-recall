//! Physical reclamation for the owned RocksDB engine.
//!
//! Exact live native keys/values are copied into a clean engine generation.
//! The entire old directory, including historical MANIFEST and diagnostic keys,
//! is durably retired before completion. Blocking I/O cannot be hard-cancelled:
//! the owned task and exclusive admission lease survive a cancelled waiter.

use super::source_erasure::{self, PreparedSelection};
use super::{Error, NativeOwner, OptimisticTransactionDB, Options};
use crate::kvs::physical_generation;
use crate::kvs::{RecallHistoryAuthority, RecallHistorySnapshot};
use rocksdb::{FlushOptions, IteratorMode, WaitForCompactOptions, WriteOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const RECEIPT_SUFFIX: &str = ".recall-physical-checkpoint.json";
const PENDING_SUFFIX: &str = ".recall-physical-checkpoint.next";
const MAX_RECEIPT_BYTES: u64 = 16 * 1024 * 1024;

pub(super) struct PhysicalState {
	path: PathBuf,
	_owner: File,
	admission: Arc<RwLock<()>>,
	failed: AtomicBool,
}

impl PhysicalState {
	pub(super) fn new(path: &str) -> Result<Self, Error> {
		let supplied = Path::new(path);
		let parent = supplied
			.parent()
			.filter(|p| !p.as_os_str().is_empty())
			.unwrap_or(Path::new("."));
		let name = supplied
			.file_name()
			.ok_or_else(|| failure("native store needs a final path component"))?;
		ensure_native_parent(parent)?;
		let path = fs::canonicalize(parent).map_err(io_error)?.join(name);
		match fs::symlink_metadata(&path) {
			Ok(metadata) if metadata.is_dir() => {}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
			Ok(_) => return Err(failure("native store is not an owned directory")),
			Err(error) => return Err(io_error(error)),
		}
		let lock_path = sibling_path(&path, ".recall-native-owner.lock")?;
		validate_regular_or_absent(&lock_path)?;
		let mut options = OpenOptions::new();
		options.read(true).write(true).create(true).truncate(false);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(0o600);
		}
		let owner = options.open(lock_path).map_err(io_error)?;
		owner.try_lock().map_err(io_error)?;
		sync_parent(&path)?;
		Ok(Self {
			path,
			_owner: owner,
			admission: Arc::new(RwLock::new(())),
			failed: AtomicBool::new(false),
		})
	}

	pub(super) fn path(&self) -> &Path {
		&self.path
	}

	pub(super) async fn admit(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
		self.healthy()?;
		let lease = self.admission.clone().read_owned().await;
		self.healthy()?;
		Ok(lease)
	}

	fn healthy(&self) -> Result<(), Error> {
		if self.failed.load(Ordering::Acquire) {
			return Err(failure(
				"native physical maintenance failed; restart recovery required",
			));
		}
		Ok(())
	}
}

fn ensure_native_parent(path: &Path) -> Result<(), Error> {
	match fs::symlink_metadata(path) {
		Ok(metadata) if metadata.is_dir() => Ok(()),
		Ok(_) => Err(failure("native parent is not an owned directory")),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			let parent = path
				.parent()
				.filter(|p| !p.as_os_str().is_empty())
				.unwrap_or(Path::new("."));
			ensure_native_parent(parent)?;
			fs::create_dir(path).map_err(io_error)?;
			sync_parent(path)
		}
		Err(error) => Err(io_error(error)),
	}
}

pub(super) fn failure(message: &str) -> Error {
	Error::Ds(format!("Recall physical checkpoint: {message}"))
}

fn io_error(error: impl std::fmt::Display) -> Error {
	Error::Ds(format!("Recall physical checkpoint I/O: {error}"))
}

pub(super) fn remaining(deadline: Instant) -> Result<Duration, Error> {
	deadline
		.checked_duration_since(Instant::now())
		.filter(|value| !value.is_zero())
		.ok_or_else(|| failure("original physical maintenance budget exhausted"))
}

struct MaintenanceOwner {
	state: Arc<PhysicalState>,
	success: bool,
	// Drop the lease only after Drop has poisoned failed/panicked ownership.
	_lease: OwnedRwLockWriteGuard<()>,
}

impl Drop for MaintenanceOwner {
	fn drop(&mut self) {
		if !self.success {
			self.state.failed.store(true, Ordering::Release);
		}
	}
}

pub(super) async fn checkpoint(
	db: NativeOwner,
	options: Arc<Options>,
	state: Arc<PhysicalState>,
	deadline: Instant,
	authorities: Option<Vec<RecallHistoryAuthority>>,
	capabilities: Option<crate::dbs::Capabilities>,
) -> Result<(), Error> {
	// The router/caller can disconnect. Never detach ownership of a started
	// native rebuild from its admission lease or database lifetime.
	tokio::spawn(async move {
		let result = async {
			state.healthy()?;
			let lease =
				tokio::time::timeout(remaining(deadline)?, state.admission.clone().write_owned())
					.await
					.map_err(|_| failure("native transaction drain exceeded original budget"))?;
			state.healthy()?;
			let worker_state = state.clone();
			tokio::task::spawn_blocking(move || {
				let mut owner = MaintenanceOwner {
					state: worker_state.clone(),
					success: false,
					_lease: lease,
				};
				let result = (|| {
					let mut native = db
						.current
						.write()
						.map_err(|_| failure("native generation ownership poisoned"))?;
					perform(
						&mut native,
						&db,
						&options,
						&worker_state.path,
						deadline,
						authorities.as_deref(),
						capabilities.as_ref(),
					)
				})();
				owner.success = result.is_ok();
				result
			})
			.await
			.map_err(|_| failure("owned native checkpoint worker failed"))?
		}
		.await;
		if result.is_err() {
			state.failed.store(true, Ordering::Release);
		}
		result
	})
	.await
	.map_err(|_| failure("owned native checkpoint task failed"))?
}

/// Read-only inventory remains owned if the RPC requester disappears. It never
/// emits payloads in error/debug output and cannot certify an incomplete scope.
pub(super) async fn history_inventory(
	db: NativeOwner,
	state: Arc<PhysicalState>,
	deadline: Instant,
	namespace: String,
	database: String,
) -> Result<RecallHistorySnapshot, Error> {
	tokio::spawn(async move {
		state.healthy()?;
		let lease =
			tokio::time::timeout(remaining(deadline)?, state.admission.clone().write_owned())
				.await
				.map_err(|_| failure("native history inventory drain exceeded original budget"))?;
		state.healthy()?;
		tokio::task::spawn_blocking(move || {
			let _lease = lease;
			let _state = state;
			let native = db
				.current
				.read()
				.map_err(|_| failure("native generation ownership poisoned"))?;
			let native = native
				.as_ref()
				.ok_or_else(|| failure("native generation unavailable"))?;
			source_erasure::capture(native, &namespace, &database, deadline)
		})
		.await
		.map_err(|_| failure("owned native history inventory task failed"))?
	})
	.await
	.map_err(|_| failure("owned native history inventory requester failed"))?
}

/// Checked staging shutdown; unlike ordinary shutdown, no native errors are logged away.
pub(super) async fn shutdown_staging(
	db: NativeOwner,
	state: Arc<PhysicalState>,
	deadline: Instant,
) -> Result<(), Error> {
	let lease = tokio::time::timeout(remaining(deadline)?, state.admission.clone().write_owned())
		.await
		.map_err(|_| failure("staging native transaction drain exceeded original budget"))?;
	let owner = MaintenanceOwner {
		state: state.clone(),
		success: false,
		_lease: lease,
	};
	tokio::task::spawn_blocking(move || {
		// This owner is deliberately never reopened after shutdown.
		let _owner = owner;
		let mut current = db
			.current
			.write()
			.map_err(|_| failure("staging generation ownership poisoned"))?;
		let native = current
			.as_ref()
			.ok_or_else(|| failure("staging generation unavailable"))?;
		flush_native(native, deadline)?;
		let native = current
			.take()
			.ok_or_else(|| failure("staging generation unavailable"))?;
		if let Err((error, native)) = close_unique(native) {
			*current = Some(native);
			return Err(error);
		}
		Ok(())
	})
	.await
	.map_err(|_| failure("owned staging shutdown task failed"))?
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Generation {
	name: String,
	bytes: u64,
	#[cfg(unix)]
	device: u64,
	#[cfg(unix)]
	inode: u64,
}

fn payload_name(name: &str) -> bool {
	let Some((number, extension)) = name.rsplit_once('.') else {
		return false;
	};
	!number.is_empty()
		&& number.bytes().all(|byte| byte.is_ascii_digit())
		&& matches!(extension, "sst" | "ldb" | "blob" | "log" | "dbtmp")
}

fn metadata_name(name: &str) -> bool {
	matches!(name, "CURRENT" | "LOCK" | "IDENTITY" | "LOG")
		|| name
			.strip_prefix("LOG.old.")
			.is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
		|| ["MANIFEST-", "OPTIONS-"].iter().any(|prefix| {
			name.strip_prefix(prefix).is_some_and(|suffix| {
				!suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
			})
		})
}

fn generations(path: &Path, deadline: Instant) -> Result<BTreeMap<String, Generation>, Error> {
	let mut result = BTreeMap::new();
	for directory in [path.to_path_buf(), path.join("archive")] {
		remaining(deadline)?;
		match fs::symlink_metadata(&directory) {
			Ok(metadata) if metadata.file_type().is_dir() => {}
			Err(error) if directory != path && error.kind() == std::io::ErrorKind::NotFound => {
				continue
			}
			Ok(_) => return Err(failure("native payload directory changed type")),
			Err(error) => return Err(io_error(error)),
		}
		for entry in fs::read_dir(&directory).map_err(io_error)? {
			remaining(deadline)?;
			let entry = entry.map_err(io_error)?;
			let name = entry
				.file_name()
				.into_string()
				.map_err(|_| failure("unknown native file name"))?;
			let metadata = fs::symlink_metadata(entry.path()).map_err(io_error)?;
			if directory == path && name == "archive" && metadata.is_dir() {
				continue;
			}
			if !metadata.file_type().is_file() {
				return Err(failure("unknown native directory, symlink or special file"));
			}
			if !payload_name(&name) && !(directory == path && metadata_name(&name)) {
				return Err(failure("unclassified native file generation"));
			}
			let relative = if directory == path {
				name
			} else {
				format!("archive/{name}")
			};
			#[cfg(unix)]
			use std::os::unix::fs::MetadataExt;
			result.insert(
				relative.clone(),
				Generation {
					name: relative,
					bytes: metadata.len(),
					#[cfg(unix)]
					device: metadata.dev(),
					#[cfg(unix)]
					inode: metadata.ino(),
				},
			);
		}
	}
	Ok(result)
}

fn require_zero_property(db: &OptimisticTransactionDB, property: &str) -> Result<(), Error> {
	match db.property_int_value(property)? {
		Some(0) => Ok(()),
		_ => Err(failure(
			"native snapshots or background errors prevent physical completion",
		)),
	}
}

fn wait_for_background(db: &OptimisticTransactionDB, deadline: Instant) -> Result<(), Error> {
	let budget = remaining(deadline)?;
	let mut wait = WaitForCompactOptions::default();
	wait.set_abort_on_pause(true);
	wait.set_timeout(u64::try_from(budget.as_micros()).map_err(io_error)?.max(1));
	db.wait_for_compact(&wait)?;
	remaining(deadline)?;
	Ok(())
}

fn sync_payload_directories(path: &Path) -> Result<(), Error> {
	match fs::symlink_metadata(path.join("archive")) {
		Ok(metadata) if metadata.file_type().is_dir() => {
			File::open(path.join("archive"))
				.map_err(io_error)?
				.sync_all()
				.map_err(io_error)?;
		}
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
		Ok(_) => return Err(failure("native archive changed type")),
		Err(error) => return Err(io_error(error)),
	}
	File::open(path)
		.map_err(io_error)?
		.sync_all()
		.map_err(io_error)
}

#[derive(Debug, Eq, PartialEq)]
struct LiveFingerprint {
	rows: u64,
	sha256: String,
}

fn hash_live_row(digest: &mut Sha256, key: &[u8], value: &[u8]) -> Result<(), Error> {
	digest.update(u64::try_from(key.len()).map_err(io_error)?.to_be_bytes());
	digest.update(key);
	digest.update(u64::try_from(value.len()).map_err(io_error)?.to_be_bytes());
	digest.update(value);
	Ok(())
}

fn live_fingerprint(
	db: &OptimisticTransactionDB,
	deadline: Instant,
) -> Result<LiveFingerprint, Error> {
	let mut digest = Sha256::new();
	digest.update(b"meerkat-recall/native-live-kv/v1\0");
	let mut rows = 0_u64;
	for item in db.iterator(IteratorMode::Start) {
		remaining(deadline)?;
		let (key, value) = item?;
		hash_live_row(&mut digest, &key, &value)?;
		rows = rows
			.checked_add(1)
			.ok_or_else(|| failure("native row count overflow"))?;
	}
	remaining(deadline)?;
	Ok(LiveFingerprint {
		rows,
		sha256: format!("{:x}", digest.finalize()),
	})
}

fn protected_fingerprint(
	db: &OptimisticTransactionDB,
	selection: &PreparedSelection,
	deadline: Instant,
) -> Result<LiveFingerprint, Error> {
	let mut digest = Sha256::new();
	digest.update(b"meerkat-recall/native-protected-kv/v1\0");
	let mut rows = 0u64;
	for item in db.iterator(IteratorMode::Start) {
		remaining(deadline)?;
		let (key, value) = item?;
		if selection.rebuilt_key(&key)? {
			continue;
		}
		hash_live_row(&mut digest, &key, &value)?;
		rows = rows
			.checked_add(1)
			.ok_or_else(|| failure("native protected row count overflow"))?;
	}
	Ok(LiveFingerprint {
		rows,
		sha256: format!("{:x}", digest.finalize()),
	})
}

fn copy_live_native(
	original: &OptimisticTransactionDB,
	replacement: &OptimisticTransactionDB,
	deadline: Instant,
	selection: Option<&PreparedSelection>,
) -> Result<LiveFingerprint, Error> {
	let mut digest = Sha256::new();
	digest.update(b"meerkat-recall/native-live-kv/v1\0");
	let mut rows = 0_u64;
	let mut writes = WriteOptions::default();
	// The replacement is unexposed and is discarded after an interrupted build.
	// Its complete memtables/files are explicitly flushed and synced before seal.
	writes.disable_wal(true);
	for item in original.iterator(IteratorMode::Start) {
		remaining(deadline)?;
		let (key, value) = item?;
		if selection.is_some_and(|selection| selection.drops(&key)) {
			continue;
		}
		replacement.put_opt(&key, &value, &writes)?;
		hash_live_row(&mut digest, &key, &value)?;
		rows = rows
			.checked_add(1)
			.ok_or_else(|| failure("native row count overflow"))?;
	}
	remaining(deadline)?;
	let expected = LiveFingerprint {
		rows,
		sha256: format!("{:x}", digest.finalize()),
	};
	if live_fingerprint(replacement, deadline)? != expected {
		return Err(failure(
			"replacement native key/value stream differs from original",
		));
	}
	Ok(expected)
}

fn flush_native(db: &OptimisticTransactionDB, deadline: Instant) -> Result<(), Error> {
	remaining(deadline)?;
	db.flush_wal(true)?;
	let mut options = FlushOptions::default();
	options.set_wait(true);
	db.flush_opt(&options)?;
	wait_for_background(db, deadline)?;
	require_zero_property(db, "rocksdb.background-errors")?;
	remaining(deadline)?;
	Ok(())
}

fn generation_options(
	options: &Options,
) -> Result<(Options, Option<super::DiskSpaceManager>), Error> {
	let mut options = options.clone();
	let manager = if *super::cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE > 0 {
		Some(super::DiskSpaceManager::new(
			*super::cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE,
			&mut options,
		)?)
	} else {
		None
	};
	Ok((options, manager))
}

fn open_existing(options: &Options, path: &Path) -> Result<OptimisticTransactionDB, Error> {
	let mut options = options.clone();
	options.create_if_missing(false);
	options.create_missing_column_families(false);
	let families = <OptimisticTransactionDB>::list_cf(&options, path)?;
	if families != ["default"] {
		return Err(failure(
			"unexpected native column families prevent exact generation replacement",
		));
	}
	OptimisticTransactionDB::open(&options, path).map_err(Error::from)
}

fn close_unique(native: super::NativeDb) -> Result<(), (Error, super::NativeDb)> {
	let native = std::pin::Pin::into_inner(native);
	match Arc::try_unwrap(native) {
		Ok(native) => {
			drop(native);
			Ok(())
		}
		Err(native) => Err((
			failure("a native handle escaped exclusive generation ownership"),
			std::pin::Pin::new(native),
		)),
	}
}

fn verify_existing(
	options: &Options,
	path: &Path,
	identity: &str,
	expected: &LiveFingerprint,
	deadline: Instant,
) -> Result<(), Error> {
	remaining(deadline)?;
	if engine_identity(path)? != identity {
		return Err(failure(
			"replacement native identity differs from the sealed generation",
		));
	}
	let (verification_options, _space_manager) = generation_options(options)?;
	let native = open_existing(&verification_options, path)?;
	if live_fingerprint(&native, deadline)? != *expected {
		return Err(failure(
			"durably reopened native stream does not match the verified copy",
		));
	}
	flush_native(&native, deadline)?;
	drop(native);
	sync_payload_directories(path)?;
	remaining(deadline)?;
	Ok(())
}

struct PreparedNative {
	plan: physical_generation::Generation,
	pending: Receipt,
}

#[cfg(test)]
fn prepare_replacement(
	original: &OptimisticTransactionDB,
	options: &Options,
	path: &Path,
	deadline: Instant,
) -> Result<PreparedNative, Error> {
	prepare_replacement_selected(original, options, path, deadline, None, None)
}

fn prepare_replacement_selected(
	original: &OptimisticTransactionDB,
	options: &Options,
	path: &Path,
	deadline: Instant,
	authorities: Option<&[RecallHistoryAuthority]>,
	capabilities: Option<&crate::dbs::Capabilities>,
) -> Result<PreparedNative, Error> {
	remaining(deadline)?;
	let selection = authorities
		.map(|authorities| PreparedSelection::prepare(original, authorities, deadline))
		.transpose()?;
	let started_unix_ms = unix_ms()?;
	let previous = read_previous_receipt(path)?;
	let old_identity = engine_identity(path)?;
	if previous
		.as_ref()
		.is_some_and(|old| old.engine_identity != old_identity)
	{
		return Err(failure(
			"previous checkpoint does not identify the current native generation",
		));
	}

	flush_native(original, deadline)?;
	require_zero_property(original, "rocksdb.num-snapshots")?;
	let before = generations(path, deadline)?;
	let plan = physical_generation::Generation::prepare(path).map_err(io_error)?;
	remaining(deadline)?;
	let (build_options, _build_space_manager) = generation_options(options)?;
	let replacement = OptimisticTransactionDB::open(&build_options, plan.staged_path())?;
	let mut expected = copy_live_native(original, &replacement, deadline, selection.as_ref())?;
	let protected = selection
		.as_ref()
		.map(|selection| protected_fingerprint(&replacement, selection, deadline))
		.transpose()?;
	flush_native(&replacement, deadline)?;
	let new_identity = engine_identity(plan.staged_path())?;
	if new_identity == old_identity {
		return Err(failure(
			"replacement engine reused the retired native identity",
		));
	}
	drop(replacement);
	verify_existing(
		options,
		plan.staged_path(),
		&new_identity,
		&expected,
		deadline,
	)?;
	if let Some(selection) = &selection {
		selection.rebuild(
			plan.staged_path(),
			deadline,
			capabilities.ok_or_else(|| failure("original staging capability policy is absent"))?,
		)?;
		let (verification_options, _space_manager) = generation_options(options)?;
		let rebuilt = open_existing(&verification_options, plan.staged_path())?;
		if Some(protected_fingerprint(&rebuilt, selection, deadline)?) != protected {
			return Err(failure(
				"derived reconstruction changed protected native keys or values",
			));
		}
		expected = live_fingerprint(&rebuilt, deadline)?;
		flush_native(&rebuilt, deadline)?;
		drop(rebuilt);
		verify_existing(
			options,
			plan.staged_path(),
			&new_identity,
			&expected,
			deadline,
		)?;
	}
	let after = generations(plan.staged_path(), deadline)?;
	let pending = make_receipt(
		directory_identity(path)?,
		directory_identity(plan.staged_path())?,
		previous,
		old_identity,
		new_identity,
		started_unix_ms,
		before,
		after,
		expected,
	)?;
	publish_pending(path, &pending, deadline)?;
	remaining(deadline)?;
	Ok(PreparedNative { plan, pending })
}

fn perform(
	owner: &mut Option<super::NativeDb>,
	native_owner: &super::NativeGeneration,
	options: &Options,
	path: &Path,
	deadline: Instant,
	authorities: Option<&[RecallHistoryAuthority]>,
	capabilities: Option<&crate::dbs::Capabilities>,
) -> Result<(), Error> {
	let original = owner
		.as_ref()
		.ok_or_else(|| failure("native generation unavailable"))?;
	let PreparedNative { plan, pending } =
		prepare_replacement_selected(original, options, path, deadline, authorities, capabilities)?;
	let original = owner
		.take()
		.ok_or_else(|| failure("native generation disappeared before close"))?;
	if let Err((error, original)) = close_unique(original) {
		*owner = Some(original);
		return Err(error);
	}
	// This owned operation drains even if a caller disconnects. Once the helper
	// records its durable decision, it completes/replays promotion, never rollback.
	plan.promote(deadline, |promoted| {
		if directory_identity(promoted).map_err(std::io::Error::other)?
			!= pending.current.current_directory
		{
			return Err(std::io::Error::other(
				"promoted directory differs from pending native proof",
			));
		}
		verify_existing(
			options,
			promoted,
			&pending.current.engine_identity,
			&pending.current.fingerprint(),
			deadline,
		)
		.map_err(std::io::Error::other)
	})
	.map_err(io_error)?;
	remaining(deadline)?;
	let (final_options, final_space_manager) = generation_options(options)?;
	let replacement = open_existing(&final_options, path)?;
	if live_fingerprint(&replacement, deadline)? != pending.current.fingerprint() {
		return Err(failure(
			"promoted native stream does not match its pending checkpoint",
		));
	}
	flush_native(&replacement, deadline)?;
	*native_owner
		.disk_space_manager
		.write()
		.map_err(|_| failure("native space-manager ownership poisoned"))? = final_space_manager;
	*owner = Some(Arc::pin(replacement));
	complete_pending(path, pending, deadline)?;
	remaining(deadline)?;
	Ok(())
}

#[derive(Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity {
	device: u64,
	inode: u64,
}

fn directory_identity(path: &Path) -> Result<DirectoryIdentity, Error> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		let metadata = fs::symlink_metadata(path).map_err(io_error)?;
		if !metadata.is_dir() {
			return Err(failure("native generation directory changed type"));
		}
		Ok(DirectoryIdentity {
			device: metadata.dev(),
			inode: metadata.ino(),
		})
	}
	#[cfg(not(unix))]
	{
		let _ = path;
		Err(failure("native physical directory identity is unavailable"))
	}
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
	sequence: u64,
	old_directory: DirectoryIdentity,
	current_directory: DirectoryIdentity,
	old_engine_identity: String,
	engine_identity: String,
	rows: u64,
	stream_sha256: String,
	started_unix_ms: u64,
	completed_unix_ms: u64,
	previous_sha256: Option<String>,
	original_payload_generations: Vec<Generation>,
	current_payload_generations: Vec<Generation>,
	native_snapshots: u64,
	background_errors: u64,
	completed: bool,
	hard_cancellation_supported: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
	format: String,
	engine: String,
	wrapper_version: String,
	current: Checkpoint,
	current_sha256: String,
	previous: Option<Checkpoint>,
}

impl Checkpoint {
	fn fingerprint(&self) -> LiveFingerprint {
		LiveFingerprint {
			rows: self.rows,
			sha256: self.stream_sha256.clone(),
		}
	}
}

fn unix_ms() -> Result<u64, Error> {
	u64::try_from(
		SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(io_error)?
			.as_millis(),
	)
	.map_err(io_error)
}

fn checkpoint_digest(checkpoint: &Checkpoint) -> Result<String, Error> {
	Ok(format!(
		"{:x}",
		Sha256::digest(serde_json::to_vec(checkpoint).map_err(io_error)?)
	))
}

fn engine_identity(path: &Path) -> Result<String, Error> {
	let path = path.join("IDENTITY");
	let metadata = fs::symlink_metadata(&path).map_err(io_error)?;
	if !metadata.is_file() || metadata.len() > 128 {
		return Err(failure("invalid native engine identity file"));
	}
	let raw = fs::read_to_string(path).map_err(io_error)?;
	let identity = raw.trim();
	if identity.is_empty()
		|| !identity
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-')
	{
		return Err(failure("invalid native engine identity"));
	}
	Ok(identity.to_owned())
}

fn hash_shape(hash: &str) -> bool {
	hash.len() == 64
		&& hash
			.bytes()
			.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn identity_shape(identity: &str) -> bool {
	!identity.is_empty()
		&& identity.len() <= 128
		&& identity
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn validate_checkpoint(checkpoint: &Checkpoint, completed: bool) -> Result<(), Error> {
	if checkpoint.sequence == 0
		|| checkpoint.completed != completed
		|| checkpoint.native_snapshots != 0
		|| checkpoint.background_errors != 0
		|| checkpoint.hard_cancellation_supported
		|| checkpoint.started_unix_ms > checkpoint.completed_unix_ms
		|| !identity_shape(&checkpoint.engine_identity)
		|| !identity_shape(&checkpoint.old_engine_identity)
		|| checkpoint.engine_identity == checkpoint.old_engine_identity
		|| checkpoint.old_directory == checkpoint.current_directory
		|| checkpoint.old_directory.device != checkpoint.current_directory.device
		|| !hash_shape(&checkpoint.stream_sha256)
		|| checkpoint
			.previous_sha256
			.as_ref()
			.is_some_and(|hash| !hash_shape(hash))
	{
		return Err(failure("invalid physical checkpoint fields"));
	}
	for rows in [
		&checkpoint.original_payload_generations,
		&checkpoint.current_payload_generations,
	] {
		let mut names = std::collections::BTreeSet::new();
		for row in rows {
			let archive_name = row.name.strip_prefix("archive/");
			let allowed = archive_name.map_or_else(
				|| payload_name(&row.name) || metadata_name(&row.name),
				payload_name,
			);
			if !allowed || !names.insert(&row.name) {
				return Err(failure("invalid or duplicate physical generation identity"));
			}
		}
	}
	Ok(())
}

fn validate_receipt(receipt: &Receipt, completed: bool) -> Result<(), Error> {
	validate_checkpoint(&receipt.current, completed)?;
	if receipt.format != "recall-rocksdb-physical-checkpoint/v1"
		|| receipt.engine != "rocksdb"
		|| receipt.wrapper_version != "0.24.0-surreal.5"
		|| checkpoint_digest(&receipt.current)? != receipt.current_sha256
	{
		return Err(failure("invalid physical checkpoint receipt"));
	}
	if let Some(previous) = &receipt.previous {
		validate_checkpoint(previous, true)?;
		if receipt.current.previous_sha256.as_deref() != Some(checkpoint_digest(previous)?.as_str())
			|| previous.sequence.checked_add(1) != Some(receipt.current.sequence)
			|| previous.engine_identity != receipt.current.old_engine_identity
			|| previous.current_directory != receipt.current.old_directory
		{
			return Err(failure("physical checkpoint adjacency is inconsistent"));
		}
	} else if receipt.current.sequence != 1 || receipt.current.previous_sha256.is_some() {
		return Err(failure("physical checkpoint adjacency is incomplete"));
	}
	Ok(())
}

fn read_receipt_file(file: &Path, completed: bool) -> Result<Option<Receipt>, Error> {
	match fs::symlink_metadata(file) {
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
		Ok(metadata) if metadata.is_file() && metadata.len() <= MAX_RECEIPT_BYTES => {}
		Ok(_) => return Err(failure("invalid or oversized physical receipt")),
		Err(error) => return Err(io_error(error)),
	}
	let receipt: Receipt =
		serde_json::from_slice(&fs::read(file).map_err(io_error)?).map_err(io_error)?;
	validate_receipt(&receipt, completed)?;
	Ok(Some(receipt))
}

fn read_previous_receipt(path: &Path) -> Result<Option<Checkpoint>, Error> {
	Ok(
		read_receipt_file(&sibling_path(path, RECEIPT_SUFFIX)?, true)?
			.map(|receipt| receipt.current),
	)
}

fn make_receipt(
	old_directory: DirectoryIdentity,
	current_directory: DirectoryIdentity,
	previous: Option<Checkpoint>,
	old_engine_identity: String,
	engine_identity: String,
	started_unix_ms: u64,
	before: BTreeMap<String, Generation>,
	after: BTreeMap<String, Generation>,
	fingerprint: LiveFingerprint,
) -> Result<Receipt, Error> {
	let current = Checkpoint {
		old_directory,
		current_directory,
		sequence: previous
			.as_ref()
			.map_or(Some(1), |old| old.sequence.checked_add(1))
			.ok_or_else(|| failure("physical checkpoint sequence exhausted"))?,
		old_engine_identity,
		engine_identity,
		rows: fingerprint.rows,
		stream_sha256: fingerprint.sha256,
		started_unix_ms,
		completed_unix_ms: unix_ms()?,
		previous_sha256: previous.as_ref().map(checkpoint_digest).transpose()?,
		original_payload_generations: before.into_values().collect(),
		current_payload_generations: after.into_values().collect(),
		native_snapshots: 0,
		background_errors: 0,
		completed: false,
		hard_cancellation_supported: false,
	};
	let receipt = Receipt {
		format: "recall-rocksdb-physical-checkpoint/v1".into(),
		engine: "rocksdb".into(),
		wrapper_version: "0.24.0-surreal.5".into(),
		current_sha256: checkpoint_digest(&current)?,
		current,
		previous,
	};
	validate_receipt(&receipt, false)?;
	Ok(receipt)
}

fn sibling_path(path: &Path, suffix: &str) -> Result<PathBuf, Error> {
	let mut name = std::ffi::OsString::from(".");
	name.push(
		path.file_name()
			.ok_or_else(|| failure("native path needs a filename"))?,
	);
	name.push(suffix);
	Ok(path
		.parent()
		.ok_or_else(|| failure("native path needs a parent"))?
		.join(name))
}

fn sync_parent(path: &Path) -> Result<(), Error> {
	File::open(
		path.parent()
			.ok_or_else(|| failure("path needs a parent"))?,
	)
	.map_err(io_error)?
	.sync_all()
	.map_err(io_error)
}

fn validate_regular_or_absent(file: &Path) -> Result<(), Error> {
	match fs::symlink_metadata(file) {
		Ok(metadata) if metadata.is_file() => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Ok(_) => Err(failure("owned metadata path changed type")),
		Err(error) => Err(io_error(error)),
	}
}

fn write_receipt_file(file: &Path, receipt: &Receipt, create_new: bool) -> Result<(), Error> {
	validate_regular_or_absent(file)?;
	let raw = serde_json::to_vec(receipt).map_err(io_error)?;
	if u64::try_from(raw.len()).map_err(io_error)? > MAX_RECEIPT_BYTES {
		return Err(failure("physical receipt exceeds its publication bound"));
	}
	let mut options = OpenOptions::new();
	options
		.write(true)
		.create_new(create_new)
		.truncate(!create_new);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		options.mode(0o600);
	}
	let mut output = options.open(file).map_err(io_error)?;
	output.write_all(&raw).map_err(io_error)?;
	output.sync_all().map_err(io_error)?;
	drop(output);
	sync_parent(file)
}

fn remove_owned_temporary(path: &Path) -> Result<(), Error> {
	validate_regular_or_absent(path)?;
	match fs::remove_file(path) {
		Ok(()) => sync_parent(path),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(io_error(error)),
	}
}

fn publish_pending(path: &Path, pending: &Receipt, deadline: Instant) -> Result<(), Error> {
	let temporary = sibling_path(path, ".recall-physical-checkpoint.prepare")?;
	let next = sibling_path(path, PENDING_SUFFIX)?;
	match fs::symlink_metadata(&next) {
		Ok(_) => {
			return Err(failure(
				"unfinished native checkpoint must recover before another attempt",
			))
		}
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
		Err(error) => return Err(io_error(error)),
	}
	remove_owned_temporary(&temporary)?;
	write_receipt_file(&temporary, pending, true)?;
	remaining(deadline)?;
	fs::rename(temporary, next).map_err(io_error)?;
	sync_parent(path)
}

fn complete_pending(path: &Path, mut pending: Receipt, deadline: Instant) -> Result<(), Error> {
	remaining(deadline)?;
	if engine_identity(path)? != pending.current.engine_identity
		|| directory_identity(path)? != pending.current.current_directory
	{
		return Err(failure(
			"completed native identity does not match pending receipt",
		));
	}
	pending.current.current_payload_generations =
		generations(path, deadline)?.into_values().collect();
	pending.current.completed = true;
	pending.current.completed_unix_ms = unix_ms()?;
	pending.current_sha256 = checkpoint_digest(&pending.current)?;
	validate_receipt(&pending, true)?;
	let next = sibling_path(path, PENDING_SUFFIX)?;
	let completed = sibling_path(path, RECEIPT_SUFFIX)?;
	validate_regular_or_absent(&completed)?;
	// The stable pending record is already durable before promotion. A separate
	// publication temp keeps it intact if final receipt serialization tears.
	let temporary = sibling_path(path, ".recall-physical-checkpoint.publish")?;
	validate_regular_or_absent(&temporary)?;
	match fs::remove_file(&temporary) {
		Ok(()) => sync_parent(&temporary)?,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
		Err(error) => return Err(io_error(error)),
	}
	write_receipt_file(&temporary, &pending, true)?;
	remaining(deadline)?;
	fs::rename(temporary, completed).map_err(io_error)?;
	sync_parent(path)?;
	fs::remove_file(next).map_err(io_error)?;
	sync_parent(path)?;
	Ok(())
}

pub(super) fn recover_before_open(
	options: &Options,
	path: &Path,
	deadline: Instant,
) -> Result<(), Error> {
	let pending = read_receipt_file(&sibling_path(path, PENDING_SUFFIX)?, false)?;
	physical_generation::recover(path, |promoted| {
		let pending = pending.as_ref().ok_or_else(|| {
			std::io::Error::other("committed native promotion has no pending stream proof")
		})?;
		if directory_identity(promoted).map_err(std::io::Error::other)?
			!= pending.current.current_directory
		{
			return Err(std::io::Error::other(
				"recovered directory differs from pending native proof",
			));
		}
		verify_existing(
			options,
			promoted,
			&pending.current.engine_identity,
			&pending.current.fingerprint(),
			deadline,
		)
		.map_err(std::io::Error::other)
	})
	.map_err(io_error)
}

/// Called only under stable native ownership, after filesystem promotion recovery,
/// before publishing any native handle or allowing engine/background transactions.
pub(super) fn recover_pending(
	db: &OptimisticTransactionDB,
	path: &Path,
	deadline: Instant,
) -> Result<(), Error> {
	remove_owned_temporary(&sibling_path(path, ".recall-physical-checkpoint.prepare")?)?;
	remove_owned_temporary(&sibling_path(path, ".recall-physical-checkpoint.publish")?)?;
	let Some(pending) = read_receipt_file(&sibling_path(path, PENDING_SUFFIX)?, false)? else {
		if let Some(previous) = read_previous_receipt(path)? {
			if previous.engine_identity != engine_identity(path)?
				|| previous.current_directory != directory_identity(path)?
			{
				return Err(failure(
					"completed receipt does not identify recovered native engine",
				));
			}
		}
		return Ok(());
	};
	let existing = read_previous_receipt(path)?;
	let expected_sequence = pending.current.sequence;
	match &existing {
		Some(existing) if existing.sequence == expected_sequence => {
			if existing.engine_identity != pending.current.engine_identity
				|| existing.old_engine_identity != pending.current.old_engine_identity
				|| existing.stream_sha256 != pending.current.stream_sha256
			{
				return Err(failure(
					"pending checkpoint conflicts with completed sequence",
				));
			}
		}
		Some(existing) if existing.sequence.checked_add(1) == Some(expected_sequence) => {
			if pending.current.previous_sha256.as_deref()
				!= Some(checkpoint_digest(existing)?.as_str())
			{
				return Err(failure(
					"pending checkpoint predecessor differs from durable receipt",
				));
			}
		}
		None if expected_sequence == 1 => {}
		_ => {
			return Err(failure(
				"pending checkpoint sequence has no durable predecessor",
			))
		}
	}
	let identity = engine_identity(path)?;
	if identity == pending.current.old_engine_identity {
		if directory_identity(path)? != pending.current.old_directory {
			return Err(failure(
				"abandoned promotion no longer owns its original directory",
			));
		}
		if existing
			.as_ref()
			.is_some_and(|old| old.sequence == expected_sequence)
		{
			return Err(failure(
				"completed native generation was replaced by its retired predecessor",
			));
		}
		// Helper recovery discarded an uncommitted stage; original journal replay
		// retries erasure. Do not invent a completed checkpoint for this attempt.
		fs::remove_file(sibling_path(path, PENDING_SUFFIX)?).map_err(io_error)?;
		sync_parent(path)?;
		return Ok(());
	}
	if identity != pending.current.engine_identity
		|| directory_identity(path)? != pending.current.current_directory
		|| live_fingerprint(db, deadline)? != pending.current.fingerprint()
	{
		return Err(failure(
			"recovered native generation does not match pending checkpoint",
		));
	}
	flush_native(db, deadline)?;
	complete_pending(path, pending, deadline)
}

#[cfg(test)]
#[path = "physical_checkpoint_tests.rs"]
mod tests;
