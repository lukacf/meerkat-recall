//! SurrealKV generation retirement under original transaction and OS custody.
//! The shared generation helper owns crash-safe directory promotion; this module
//! owns native format validation, historical stream identity and completion.

use crate::dbs::Capabilities;
use crate::err::Error;
use crate::kvs::physical_generation::{self, Generation};
use crate::kvs::recall_history::{
	HistoryDecision, HistorySelection, RecallHistoryAuthority, RecallHistoryRecord,
	RecallHistorySnapshot, MAX_HISTORY_BYTES, MAX_HISTORY_RECORDS,
};
use crate::kvs::recall_index_erasure::{self, RebuildTarget};
use crate::kvs::KeyEncode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use surrealkv::{
	CheckpointDecision, CheckpointRecord, CheckpointSummary, Mode, Options, Store, Transaction,
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const IDENTITY: &str = "recall-engine.identity";
const IDENTITY_PENDING: &str = "recall-engine.identity.next";
const RECEIPT_SUFFIX: &str = ".recall-physical-checkpoint.json";
const PENDING_SUFFIX: &str = ".recall-physical-checkpoint.next";
const MAX_RECEIPT: u64 = 16 * 1024 * 1024;

fn failure(message: impl std::fmt::Display) -> Error {
	Error::Ds(format!("SurrealKV physical checkpoint: {message}"))
}

fn io_failure(error: impl std::fmt::Display) -> io::Error {
	io::Error::other(error.to_string())
}

fn remaining(deadline: Instant) -> Result<Duration, Error> {
	deadline
		.checked_duration_since(Instant::now())
		.filter(|value| !value.is_zero())
		.ok_or_else(|| failure("original checkpoint deadline expired"))
}

fn sibling(path: &Path, suffix: &str) -> Result<PathBuf, Error> {
	let name = path
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| failure("invalid store name"))?;
	Ok(path.with_file_name(format!(".{name}{suffix}")))
}

pub(super) struct PhysicalState {
	path: PathBuf,
	options: Options,
	db: Mutex<Option<Store>>,
	admission: Arc<RwLock<()>>,
	failed: AtomicBool,
	// Stable sibling lock survives generation replacement and owned workers.
	_lock: File,
}

impl PhysicalState {
	#[cfg(test)]
	pub(super) fn open(options: Options) -> Result<Self, Error> {
		Self::open_until(options, Instant::now() + Duration::from_secs(300))
	}

	pub(super) fn open_until(
		mut options: Options,
		recovery_deadline: Instant,
	) -> Result<Self, Error> {
		remaining(recovery_deadline)?;
		let input = if options.dir.is_absolute() {
			options.dir.clone()
		} else {
			std::env::current_dir().map_err(failure)?.join(&options.dir)
		};
		let name = input
			.file_name()
			.ok_or_else(|| failure("missing store name"))?;
		let parent = input
			.parent()
			.ok_or_else(|| failure("missing store parent"))?;
		fs::create_dir_all(parent).map_err(failure)?;
		let path = fs::canonicalize(parent).map_err(failure)?.join(name);
		let lock_path = sibling(&path, ".recall-native-owner.lock")?;
		match fs::symlink_metadata(&lock_path) {
			Ok(meta) if !regular_private_file(&meta) || meta.len() != 0 => {
				return Err(failure("owner lock changed type"))
			}
			Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(failure(error)),
			_ => {}
		}
		let lock = OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&lock_path)
			.map_err(failure)?;
		lock.try_lock().map_err(failure)?;
		options.dir = path.clone();
		remove_owned_temporary(&sibling(&path, ".recall-physical-checkpoint.prepare")?)?;
		let pending = read_receipt(&sibling(&path, PENDING_SUFFIX)?, false)?;
		physical_generation::recover(&path, |live| {
			let pending = pending
				.as_ref()
				.ok_or_else(|| io_failure("promoted generation lacks original pending proof"))?;
			verify_store(live, &options, &pending.current, recovery_deadline).map_err(io_failure)
		})
		.map_err(failure)?;
		if !path.exists() {
			if pending.is_some() {
				return Err(failure("pending generation lost live store"));
			}
			fs::create_dir(&path).map_err(failure)?;
			File::open(path.parent().ok_or_else(|| failure("missing parent"))?)
				.map_err(failure)?
				.sync_all()
				.map_err(failure)?;
		}
		if !fs::symlink_metadata(&path)
			.map_err(failure)?
			.file_type()
			.is_dir()
		{
			return Err(failure("store must be a real directory"));
		}
		if let Some(pending) = pending {
			let identity = read_identity(&path)?;
			if identity == pending.current.engine_identity {
				verify_store(&path, &options, &pending.current, recovery_deadline)?;
				complete_receipt(&path, pending, recovery_deadline)?;
			} else if identity == pending.current.old_engine_identity {
				// No durable promotion decision survived: the journal retries.
				fs::remove_file(sibling(&path, PENDING_SUFFIX)?).map_err(failure)?;
				sync_parent(&path)?;
			} else {
				return Err(failure("pending generation identity differs"));
			}
		}
		ensure_identity(&path)?;
		if let Some(receipt) = read_receipt(&sibling(&path, RECEIPT_SUFFIX)?, true)? {
			if receipt.current.engine_identity != read_identity(&path)? {
				return Err(failure(
					"completed checkpoint belongs to another native generation",
				));
			}
		}
		remaining(recovery_deadline)?;
		let db = Store::new(options.clone()).map_err(failure)?;
		if let Err(error) = remaining(recovery_deadline) {
			db.close().map_err(failure)?;
			return Err(error);
		}
		Ok(Self {
			path,
			options,
			db: Mutex::new(Some(db)),
			admission: Arc::new(RwLock::new(())),
			failed: AtomicBool::new(false),
			_lock: lock,
		})
	}

	fn healthy(&self) -> Result<(), Error> {
		if self.failed.load(Ordering::Acquire) {
			return Err(failure("maintenance failed; recovery required"));
		}
		Ok(())
	}

	pub(super) async fn admit(&self) -> Result<OwnedRwLockReadGuard<()>, Error> {
		self.healthy()?;
		let lease = self.admission.clone().read_owned().await;
		self.healthy()?;
		Ok(lease)
	}

	pub(super) fn begin(&self, mode: Mode) -> Result<Transaction, Error> {
		self.healthy()?;
		self.db
			.lock()
			.map_err(failure)?
			.as_ref()
			.ok_or_else(|| failure("native generation is closed"))?
			.begin_with_mode(mode)
			.map_err(failure)
	}

	pub(super) async fn shutdown(&self) -> Result<(), Error> {
		let _lease = self.admission.write().await;
		self.failed.store(true, Ordering::Release);
		let mut owner = self.db.lock().map_err(failure)?;
		if let Some(db) = owner.as_ref() {
			db.close().map_err(failure)?;
		}
		owner.take();
		self.failed.store(true, Ordering::Release);
		Ok(())
	}
}

struct MaintenanceOwner {
	state: Arc<PhysicalState>,
	success: bool,
	_lease: OwnedRwLockWriteGuard<()>,
}

impl Drop for MaintenanceOwner {
	fn drop(&mut self) {
		if !self.success {
			self.state.failed.store(true, Ordering::Release);
		}
	}
}

fn checkpoint_deadline(timeout_ms: u64) -> Result<Instant, Error> {
	if !(1..=300_000).contains(&timeout_ms) {
		return Err(failure("finite1..=300000ms budget required"));
	}
	Ok(Instant::now() + Duration::from_millis(timeout_ms))
}

pub(super) async fn checkpoint(state: Arc<PhysicalState>, timeout_ms: u64) -> Result<(), Error> {
	checkpoint_until(state, checkpoint_deadline(timeout_ms)?).await
}

pub(super) async fn checkpoint_until(
	state: Arc<PhysicalState>,
	deadline: Instant,
) -> Result<(), Error> {
	checkpoint_request(state, deadline, Vec::new(), None).await
}

pub(super) async fn checkpoint_with_history(
	state: Arc<PhysicalState>,
	timeout_ms: u64,
	authorities: Vec<RecallHistoryAuthority>,
	capabilities: Capabilities,
) -> Result<(), Error> {
	checkpoint_with_history_until(
		state,
		checkpoint_deadline(timeout_ms)?,
		authorities,
		capabilities,
	)
	.await
}

pub(super) async fn checkpoint_with_history_until(
	state: Arc<PhysicalState>,
	deadline: Instant,
	authorities: Vec<RecallHistoryAuthority>,
	capabilities: Capabilities,
) -> Result<(), Error> {
	if authorities.is_empty() || authorities.len() > 4096 {
		return Err(failure(
			"complete historical authority is absent or exceeds bound",
		));
	}
	checkpoint_request(state, deadline, authorities, Some(capabilities)).await
}

async fn checkpoint_request(
	state: Arc<PhysicalState>,
	deadline: Instant,
	authorities: Vec<RecallHistoryAuthority>,
	capabilities: Option<Capabilities>,
) -> Result<(), Error> {
	remaining(deadline)?;
	// A disconnected waiter cannot release ownership of a running operation.
	tokio::spawn(async move {
		let result = async {
			for authority in &authorities {
				remaining(deadline)?;
				authority.validate_bound()?;
			}
			remaining(deadline)?;
			state.healthy()?;
			let lease =
				tokio::time::timeout(remaining(deadline)?, state.admission.clone().write_owned())
					.await
					.map_err(|_| failure("original transactions did not drain in time"))?;
			state.healthy()?;
			let task_state = state.clone();
			tokio::task::spawn_blocking(move || {
				let mut owner = MaintenanceOwner {
					state: task_state.clone(),
					success: false,
					_lease: lease,
				};
				let mut recovery_required = false;
				let result = perform(
					&task_state,
					deadline,
					&authorities,
					capabilities.as_ref(),
					&mut recovery_required,
				);
				// A stale original snapshot is a preparation refusal. No
				// generation was touched, so the caller can inspect and retry.
				owner.success = result.is_ok() || !recovery_required;
				result
			})
			.await
			.map_err(|_| failure("checkpoint worker failed"))?
		}
		.await;
		result
	})
	.await
	.map_err(|_| failure("checkpoint owner failed"))?
}

/// Read-only history inspection owns the same original transaction drain as
/// physical replacement. A dropped RPC waiter cannot release the native owner.
pub(super) async fn history_inventory(
	state: Arc<PhysicalState>,
	timeout_ms: u64,
	namespace: String,
	database: String,
) -> Result<RecallHistorySnapshot, Error> {
	history_inventory_until(state, checkpoint_deadline(timeout_ms)?, namespace, database).await
}

pub(super) async fn history_inventory_until(
	state: Arc<PhysicalState>,
	deadline: Instant,
	namespace: String,
	database: String,
) -> Result<RecallHistorySnapshot, Error> {
	if namespace.is_empty()
		|| database.is_empty()
		|| namespace.len() > 4096
		|| database.len() > 4096
	{
		return Err(failure("invalid bounded history inventory scope"));
	}
	remaining(deadline)?;
	tokio::spawn(async move {
		state.healthy()?;
		let lease =
			tokio::time::timeout(remaining(deadline)?, state.admission.clone().write_owned())
				.await
				.map_err(|_| {
					failure("original transactions did not drain for history inspection")
				})?;
		state.healthy()?;
		tokio::task::spawn_blocking(move || {
			// Explicit aggregate retains the OS owner until after the lease.
			let owner = InventoryOwner {
				_lease: lease,
				state,
			};
			let mut native = owner.state.db.lock().map_err(failure)?;
			let source = native
				.as_mut()
				.ok_or_else(|| failure("native generation closed"))?;
			capture_history(source, deadline, &namespace, &database)
		})
		.await
		.map_err(|_| failure("history inspection worker failed"))?
	})
	.await
	.map_err(|_| failure("history inspection owner failed"))?
}

struct InventoryOwner {
	_lease: OwnedRwLockWriteGuard<()>,
	state: Arc<PhysicalState>,
}

fn capture_history(
	source: &mut Store,
	deadline: Instant,
	namespace: &str,
	database: &str,
) -> Result<RecallHistorySnapshot, Error> {
	let prefix = crate::key::database::all::new(namespace, database).encode()?;
	let mut records = Vec::new();
	let mut bytes = namespace
		.len()
		.checked_add(database.len())
		.ok_or_else(|| failure("history inventory size overflow"))?;
	source
		.visit_checkpoint_versions(deadline, |record| {
			if !record.key.starts_with(&prefix) {
				return Ok(());
			}
			bytes = bytes
				.checked_add(record.key.len())
				.and_then(|size| size.checked_add(record.value.len()))
				.and_then(|size| size.checked_add(64))
				.ok_or_else(|| {
					surrealkv::Error::CustomError("history inventory size overflow".into())
				})?;
			if records.len() >= MAX_HISTORY_RECORDS || bytes > MAX_HISTORY_BYTES {
				return Err(surrealkv::Error::CustomError(
					"complete history inventory exceeds bound".into(),
				));
			}
			records.push(RecallHistoryRecord {
				key: record.key.to_vec(),
				value: record.value.to_vec(),
				version: record.version,
				timestamp: record.timestamp,
				tombstone: record.tombstone,
				current: record.current,
			});
			Ok(())
		})
		.map_err(failure)?;
	remaining(deadline)?;
	let snapshot = RecallHistorySnapshot::new(namespace.to_owned(), database.to_owned(), records)?;
	remaining(deadline)?;
	Ok(snapshot)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity {
	device: u64,
	inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileGeneration {
	name: String,
	bytes: u64,
	device: u64,
	inode: u64,
}

fn directory_identity(path: &Path) -> Result<DirectoryIdentity, Error> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		let meta = fs::symlink_metadata(path).map_err(failure)?;
		if !meta.file_type().is_dir() {
			return Err(failure("directory changed type"));
		}
		Ok(DirectoryIdentity {
			device: meta.dev(),
			inode: meta.ino(),
		})
	}
	#[cfg(not(unix))]
	{
		let _ = path;
		Err(failure("physical generation identity requires Unix"))
	}
}

fn regular_private_file(meta: &fs::Metadata) -> bool {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		meta.file_type().is_file() && meta.nlink() == 1
	}
	#[cfg(not(unix))]
	{
		let _ = meta;
		false
	}
}

fn native_file_name(name: &str) -> bool {
	if matches!(name, IDENTITY | "recall-checkpoint.clock") {
		return true;
	}
	let Some((directory, name)) = name.split_once('/') else {
		return false;
	};
	let extension = match directory {
		"clog" => ".clog",
		"manifest" => ".manifest",
		_ => return false,
	};
	let Some(number) = name.strip_suffix(extension) else {
		return false;
	};
	number.len() == 20
		&& number.bytes().all(|byte| byte.is_ascii_digit())
		&& number.parse::<u64>().is_ok()
}

fn files(path: &Path, deadline: Instant) -> Result<Vec<FileGeneration>, Error> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		let root = directory_identity(path)?;
		let mut pending = vec![path.to_path_buf()];
		let mut result = Vec::new();
		while let Some(directory) = pending.pop() {
			remaining(deadline)?;
			for entry in fs::read_dir(directory).map_err(failure)? {
				remaining(deadline)?;
				let entry = entry.map_err(failure)?;
				let meta = fs::symlink_metadata(entry.path()).map_err(failure)?;
				if meta.dev() != root.device {
					return Err(failure("generation crosses a filesystem"));
				}
				let name = entry
					.path()
					.strip_prefix(path)
					.map_err(failure)?
					.to_str()
					.ok_or_else(|| failure("generation name is not UTF8"))?
					.to_owned();
				if meta.file_type().is_dir() {
					if !matches!(name.as_str(), "clog" | "manifest") {
						return Err(failure("generation contains an unknown native directory"));
					}
					pending.push(entry.path());
				} else if regular_private_file(&meta) {
					if !native_file_name(&name) {
						return Err(failure("generation contains an unknown native file"));
					}
					result.push(FileGeneration {
						name,
						bytes: meta.len(),
						device: meta.dev(),
						inode: meta.ino(),
					});
				} else {
					return Err(failure("generation includes links or special files"));
				}
				if pending.len() + result.len() > 100_000 {
					return Err(failure("generation inventory exceeds bound"));
				}
			}
		}
		result.sort_by(|a, b| a.name.cmp(&b.name));
		Ok(result)
	}
	#[cfg(not(unix))]
	{
		let _ = (path, deadline);
		Err(failure("physical generation inventory requires Unix"))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fingerprint {
	rows: u64,
	high_water: u64,
	sha256: String,
}

fn fingerprint(store: &mut Store, deadline: Instant) -> Result<Fingerprint, Error> {
	selected_fingerprint(store, deadline, |_| Ok(CheckpointDecision::Keep))
}

fn selected_fingerprint(
	store: &mut Store,
	deadline: Instant,
	select: impl FnMut(&CheckpointRecord<'_>) -> surrealkv::Result<CheckpointDecision>,
) -> Result<Fingerprint, Error> {
	let mut hash = Sha256::new();
	hash.update(b"recall-surrealkv-surviving-versions-v1\0");
	let CheckpointSummary {
		records,
		high_water,
	} = store
		.visit_selected_checkpoint_records(deadline, select, |record| {
			hash.update((record.len() as u64).to_be_bytes());
			hash.update(record);
			Ok(())
		})
		.map_err(failure)?;
	hash.update(high_water.to_be_bytes());
	hash.update(records.to_be_bytes());
	Ok(Fingerprint {
		rows: records,
		high_water,
		sha256: format!("{:x}", hash.finalize()),
	})
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
	sequence: u64,
	old_directory: DirectoryIdentity,
	current_directory: DirectoryIdentity,
	old_engine_identity: String,
	engine_identity: String,
	rows: u64,
	high_water: u64,
	stream_sha256: String,
	started_unix_ms: u64,
	completed_unix_ms: u64,
	previous_sha256: Option<String>,
	original_payload_generations: Vec<FileGeneration>,
	current_payload_generations: Vec<FileGeneration>,
	native_snapshots: u64,
	background_errors: u64,
	completed: bool,
	hard_cancellation_supported: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
	format: String,
	engine: String,
	wrapper_version: String,
	current: Checkpoint,
	current_sha256: String,
	previous: Option<Checkpoint>,
}

fn checkpoint_hash(value: &Checkpoint) -> Result<String, Error> {
	Ok(format!(
		"{:x}",
		Sha256::digest(serde_json::to_vec(value).map_err(failure)?)
	))
}

fn unix_ms() -> Result<u64, Error> {
	u64::try_from(
		SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(failure)?
			.as_millis(),
	)
	.map_err(failure)
}

fn read_identity(path: &Path) -> Result<String, Error> {
	let file = path.join(IDENTITY);
	let meta = fs::symlink_metadata(&file).map_err(failure)?;
	if !regular_private_file(&meta) || meta.len() != 36 {
		return Err(failure("native identity shape differs"));
	}
	let identity = fs::read_to_string(file).map_err(failure)?;
	let parsed = uuid::Uuid::parse_str(&identity).map_err(failure)?;
	if parsed.is_nil() || parsed.to_string() != identity {
		return Err(failure("noncanonical native identity"));
	}
	Ok(identity)
}

fn canonical_identity(value: &str) -> bool {
	uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil() && parsed.to_string() == value)
}

fn clear_identity_temporary(path: &Path) -> Result<(), Error> {
	let temporary = path.join(IDENTITY_PENDING);
	match fs::symlink_metadata(&temporary) {
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(failure(error)),
		Ok(meta) if regular_private_file(&meta) && meta.len() <= 36 => {}
		Ok(_) => return Err(failure("invalid native identity scratch file")),
	}
	let raw = fs::read(&temporary).map_err(failure)?;
	if raw.iter().enumerate().any(|(index, byte)| {
		if matches!(index, 8 | 13 | 18 | 23) {
			*byte != b'-'
		} else {
			!(byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
		}
	}) {
		return Err(failure("native identity scratch bytes differ"));
	}
	fs::remove_file(temporary).map_err(failure)?;
	File::open(path)
		.map_err(failure)?
		.sync_all()
		.map_err(failure)
}

fn ensure_identity(path: &Path) -> Result<String, Error> {
	// A partial initialization file never becomes identity authority. The
	// stable OS owner lock protects this exact payload-free scratch filename.
	clear_identity_temporary(path)?;
	match fs::symlink_metadata(path.join(IDENTITY)) {
		Ok(_) => read_identity(path),
		Err(error) if error.kind() == io::ErrorKind::NotFound => {
			let identity = uuid::Uuid::new_v4().to_string();
			let temporary = path.join(IDENTITY_PENDING);
			let mut file = OpenOptions::new()
				.create_new(true)
				.write(true)
				.open(&temporary)
				.map_err(failure)?;
			file.write_all(identity.as_bytes()).map_err(failure)?;
			file.sync_all().map_err(failure)?;
			drop(file);
			fs::rename(temporary, path.join(IDENTITY)).map_err(failure)?;
			File::open(path)
				.map_err(failure)?
				.sync_all()
				.map_err(failure)?;
			Ok(identity)
		}
		Err(error) => Err(failure(error)),
	}
}

fn verify_store(
	path: &Path,
	options: &Options,
	proof: &Checkpoint,
	deadline: Instant,
) -> Result<(), Error> {
	if read_identity(path)? != proof.engine_identity
		|| directory_identity(path)? != proof.current_directory
	{
		return Err(failure("replacement native/directory identity differs"));
	}
	let mut options = options.clone();
	options.dir = path.to_path_buf();
	// Validate every owned filename before the native opener can repair logs.
	files(path, deadline)?;
	let mut store = Store::new(options).map_err(failure)?;
	let observed = fingerprint(&mut store, deadline);
	let closed = store.close().map_err(failure);
	drop(store);
	// Close is attempted even when decoding or comparison preparation fails.
	let observed = observed?;
	closed?;
	if observed
		!= (Fingerprint {
			rows: proof.rows,
			high_water: proof.high_water,
			sha256: proof.stream_sha256.clone(),
		}) {
		return Err(failure("reopened surviving version stream differs"));
	}
	Ok(())
}

fn sha256_shape(value: &str) -> bool {
	value.len() == 64
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_inventory(
	files: &[FileGeneration],
	directory: &DirectoryIdentity,
) -> Result<(), Error> {
	if files.is_empty() || files.len() > 100_000 || directory.device == 0 || directory.inode == 0 {
		return Err(failure("invalid checkpoint inventory"));
	}
	let mut previous = None;
	let mut inodes = std::collections::BTreeSet::new();
	for file in files {
		if !native_file_name(&file.name)
			|| file.device != directory.device
			|| file.inode == 0
			|| !inodes.insert(file.inode)
			|| previous.is_some_and(|name: &str| name >= file.name.as_str())
		{
			return Err(failure("checkpoint native inventory differs"));
		}
		previous = Some(file.name.as_str());
	}
	if !files
		.iter()
		.any(|file| file.name == IDENTITY && file.bytes == 36)
		|| !files.iter().any(|file| file.name.starts_with("clog/"))
		|| !files.iter().any(|file| file.name.starts_with("manifest/"))
	{
		return Err(failure("checkpoint lacks required native files"));
	}
	Ok(())
}

fn validate_checkpoint(current: &Checkpoint, completed: bool) -> Result<(), Error> {
	if current.completed != completed
		|| current.sequence == 0
		|| current.native_snapshots != 0
		|| current.background_errors != 0
		|| current.hard_cancellation_supported
		|| !canonical_identity(&current.old_engine_identity)
		|| !canonical_identity(&current.engine_identity)
		|| current.old_engine_identity == current.engine_identity
		|| current.current_directory == current.old_directory
		|| current.current_directory.device != current.old_directory.device
		|| current.high_water == u64::MAX
		|| current.started_unix_ms == 0
		|| current.started_unix_ms > current.completed_unix_ms
		|| !sha256_shape(&current.stream_sha256)
		|| current
			.previous_sha256
			.as_ref()
			.is_some_and(|value| !sha256_shape(value))
	{
		return Err(failure("invalid checkpoint proof"));
	}
	validate_inventory(
		&current.original_payload_generations,
		&current.old_directory,
	)?;
	validate_inventory(
		&current.current_payload_generations,
		&current.current_directory,
	)?;
	if !current
		.current_payload_generations
		.iter()
		.any(|file| file.name == "recall-checkpoint.clock" && file.bytes == 20)
	{
		return Err(failure("checkpoint lacks durable transaction clock"));
	}
	Ok(())
}

fn read_receipt(path: &Path, completed: bool) -> Result<Option<Receipt>, Error> {
	match fs::symlink_metadata(path) {
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Ok(meta) if regular_private_file(&meta) && meta.len() <= MAX_RECEIPT => {}
		Ok(_) => return Err(failure("invalid receipt file")),
		Err(error) => return Err(failure(error)),
	}
	let value: Receipt =
		serde_json::from_slice(&fs::read(path).map_err(failure)?).map_err(failure)?;
	let current = &value.current;
	if value.format != "recall-surrealkv-physical-checkpoint/v1"
		|| value.engine != "surrealkv"
		|| value.wrapper_version != "0.9.3"
		|| checkpoint_hash(current)? != value.current_sha256
	{
		return Err(failure("invalid checkpoint proof"));
	}
	validate_checkpoint(current, completed)?;
	if let Some(previous) = &value.previous {
		validate_checkpoint(previous, true)?;
	}
	match &value.previous {
		Some(previous)
			if previous.completed
				&& previous.sequence.checked_add(1) == Some(current.sequence)
				&& previous.engine_identity == current.old_engine_identity
				&& previous.current_directory == current.old_directory
				&& current.previous_sha256.as_ref() == Some(&checkpoint_hash(previous)?) => {}
		None if current.sequence == 1 && current.previous_sha256.is_none() => {}
		_ => return Err(failure("checkpoint adjacency differs")),
	}
	Ok(Some(value))
}

fn sync_parent(path: &Path) -> Result<(), Error> {
	File::open(path.parent().ok_or_else(|| failure("missing parent"))?)
		.map_err(failure)?
		.sync_all()
		.map_err(failure)
}

// These two fixed metadata scratch files cannot authorize a promotion. They
// are only written before an atomic rename, under the stable owner lock.
fn remove_owned_temporary(path: &Path) -> Result<(), Error> {
	match fs::symlink_metadata(path) {
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(failure(error)),
		Ok(meta) if regular_private_file(&meta) && meta.len() <= MAX_RECEIPT => {}
		Ok(_) => return Err(failure("invalid checkpoint metadata scratch file")),
	}
	fs::remove_file(path).map_err(failure)?;
	sync_parent(path)
}

fn write_pending(path: &Path, value: &Receipt) -> Result<(), Error> {
	let destination = sibling(path, PENDING_SUFFIX)?;
	if destination.exists() {
		return Err(failure("original pending checkpoint already exists"));
	}
	let temporary = sibling(path, ".recall-physical-checkpoint.prepare")?;
	let raw = serde_json::to_vec(value).map_err(failure)?;
	if raw.len() as u64 > MAX_RECEIPT {
		return Err(failure("checkpoint receipt exceeds bound"));
	}
	let mut file = OpenOptions::new()
		.create_new(true)
		.write(true)
		.open(&temporary)
		.map_err(failure)?;
	file.write_all(&raw).map_err(failure)?;
	file.sync_all().map_err(failure)?;
	fs::rename(temporary, destination).map_err(failure)?;
	sync_parent(path)
}

fn complete_receipt(path: &Path, mut value: Receipt, deadline: Instant) -> Result<(), Error> {
	remaining(deadline)?;
	if let Some(existing) = read_receipt(&sibling(path, RECEIPT_SUFFIX)?, true)? {
		if existing.current.sequence == value.current.sequence {
			if existing.current.engine_identity != value.current.engine_identity
				|| existing.current.stream_sha256 != value.current.stream_sha256
				|| existing.current.old_engine_identity != value.current.old_engine_identity
			{
				return Err(failure("completion conflicts with retained checkpoint"));
			}
			remove_owned_temporary(&sibling(path, ".recall-physical-checkpoint.complete")?)?;
			fs::remove_file(sibling(path, PENDING_SUFFIX)?).map_err(failure)?;
			return sync_parent(path);
		}
		if existing.current.sequence.checked_add(1) != Some(value.current.sequence)
			|| value.current.previous_sha256.as_ref() != Some(&existing.current_sha256)
		{
			return Err(failure(
				"completion no longer follows original prior checkpoint",
			));
		}
	} else if value.current.sequence != 1 {
		return Err(failure("completion lost prior checkpoint"));
	}
	value.current.completed = true;
	value.current.completed_unix_ms = unix_ms()?;
	value.current.current_payload_generations = files(path, deadline)?;
	value.current_sha256 = checkpoint_hash(&value.current)?;
	let destination = sibling(path, RECEIPT_SUFFIX)?;
	let temporary = sibling(path, ".recall-physical-checkpoint.complete")?;
	remove_owned_temporary(&temporary)?;
	let raw = serde_json::to_vec(&value).map_err(failure)?;
	if raw.len() as u64 > MAX_RECEIPT {
		return Err(failure("completed receipt exceeds bound"));
	}
	let mut file = OpenOptions::new()
		.create_new(true)
		.write(true)
		.open(&temporary)
		.map_err(failure)?;
	file.write_all(&raw).map_err(failure)?;
	file.sync_all().map_err(failure)?;
	fs::rename(&temporary, destination).map_err(failure)?;
	sync_parent(path)?;
	fs::remove_file(sibling(path, PENDING_SUFFIX)?).map_err(failure)?;
	sync_parent(path)
}

struct PreparedHistory {
	prefix: Vec<u8>,
	selection: HistorySelection,
	versionless: bool,
	targets: Vec<RebuildTarget>,
	rebuild_prefixes: Vec<Vec<u8>>,
	rebuild_extra_keys: BTreeSet<Vec<u8>>,
}

impl PreparedHistory {
	fn prepare(
		source: &mut Store,
		authorities: &[RecallHistoryAuthority],
		deadline: Instant,
	) -> Result<Option<Self>, Error> {
		let Some(first) = authorities.first() else {
			return Ok(None);
		};
		let snapshot = capture_history(source, deadline, first.namespace(), first.database())?;
		// This verifies every original scoped fingerprint before writing stage.
		let versionless = !source.versions_enabled();
		let selection = if versionless {
			HistorySelection::prepare_versionless_with_deadline(authorities, &snapshot, deadline)?
		} else {
			HistorySelection::prepare_with_deadline(authorities, &snapshot, deadline)?
		};
		remaining(deadline)?;
		let targets = recall_index_erasure::collect_targets_with_deadline(
			&snapshot,
			selection.aggregate_indexes(),
			deadline,
		)?;
		let rebuild_prefixes = selection
			.aggregate_indexes()
			.iter()
			.map(|(table, index)| {
				crate::key::index::all::new(first.namespace(), first.database(), table, index)
					.encode()
			})
			.collect::<Result<Vec<_>, _>>()?;
		let prefix =
			crate::key::database::all::new(first.namespace(), first.database()).encode()?;
		let rebuild_extra_keys = recall_index_erasure::rebuild_extra_keys(&targets)?;
		remaining(deadline)?;
		Ok(Some(Self {
			prefix,
			selection,
			versionless,
			targets,
			rebuild_prefixes,
			rebuild_extra_keys,
		}))
	}

	fn decision(&self, record: &CheckpointRecord<'_>) -> surrealkv::Result<CheckpointDecision> {
		if !record.key.starts_with(&self.prefix) {
			return Ok(CheckpointDecision::Keep);
		}
		// Plain SurrealKV exposes no prior index history. Never first copy a
		// stale current index key and rely on a later soft deletion to erase its
		// physical log bytes; rebuild its selected prefix from current records.
		if self.versionless && self.rebuilt_key(record.key) {
			return Ok(CheckpointDecision::Drop);
		}
		let decision = self
			.selection
			.decision(record.key, record.version, record.timestamp)
			.map_err(|_| {
				surrealkv::Error::CustomError("original historical selection differs".into())
			})?;
		Ok(match decision {
			HistoryDecision::Keep => CheckpointDecision::Keep,
			HistoryDecision::Tombstone => CheckpointDecision::Tombstone,
			HistoryDecision::Drop => CheckpointDecision::Drop,
		})
	}

	// Native builders write selected index prefixes and exact current Search
	// state keys. All other table metadata and global counters stay protected.
	fn rebuilt_key(&self, key: &[u8]) -> bool {
		self.rebuild_prefixes
			.iter()
			.any(|prefix| key.starts_with(prefix))
			|| self.rebuild_extra_keys.contains(key)
	}
}

fn protected_fingerprint(
	source: &mut Store,
	plan: &PreparedHistory,
	deadline: Instant,
	original_high_water: u64,
) -> Result<(u64, String), Error> {
	let mut digest = Sha256::new();
	digest.update(b"recall-surrealkv-unaffected-versions-v1\0");
	let summary = source
		.visit_selected_checkpoint_records(
			deadline,
			|record| {
				if plan.rebuilt_key(record.key) {
					let ordinary_index = matches!(
						crate::kvs::recall_history::decode_history_key(record.key).map_err(
							|_| surrealkv::Error::CustomError(
								"native history key codec differs".into()
							)
						)?,
						crate::kvs::recall_history::NativeHistoryKey::Index { .. }
					);
					// Scalar reconciliation can append current entries but cannot
					// discard any selected original ordinary-index version.
					if record.version > original_high_water || !ordinary_index {
						return Ok(CheckpointDecision::Drop);
					}
				}
				Ok(CheckpointDecision::Keep)
			},
			|record| {
				digest.update((record.len() as u64).to_be_bytes());
				digest.update(record);
				Ok(())
			},
		)
		.map_err(failure)?;
	digest.update(summary.records.to_be_bytes());
	Ok((summary.records, format!("{:x}", digest.finalize())))
}

fn inspect_stage(
	path: &Path,
	options: &Options,
	deadline: Instant,
	plan: Option<&PreparedHistory>,
	original_high_water: u64,
) -> Result<(Fingerprint, Option<(u64, String)>), Error> {
	files(path, deadline)?;
	let mut options = options.clone();
	options.dir = path.to_owned();
	let mut store = Store::new(options).map_err(failure)?;
	let observed: Result<_, Error> = (|| {
		let fingerprint = fingerprint(&mut store, deadline)?;
		let protected = plan
			.map(|plan| protected_fingerprint(&mut store, plan, deadline, original_high_water))
			.transpose()?;
		Ok((fingerprint, protected))
	})();
	let closed = store.close().map_err(failure);
	drop(store);
	let observed = observed?;
	closed?;
	Ok(observed)
}

fn rebuild_stage(
	path: &Path,
	options: &Options,
	plan: &PreparedHistory,
	deadline: Instant,
	capabilities: &Capabilities,
) -> Result<(), Error> {
	if plan.targets.is_empty() {
		return Ok(());
	}
	remaining(deadline)?;
	let scheme = if options.enable_versions {
		"surrealkv+versioned"
	} else {
		"surrealkv"
	};
	let path = path
		.to_str()
		.ok_or_else(|| failure("staging datastore path is not UTF8"))?;
	let url = format!("{scheme}://{path}");
	// Called only from the owned blocking worker, never recursively against the
	// original datastore. No query daemon or background index builder is started.
	tokio::runtime::Handle::current().block_on(async {
		let staging = crate::kvs::Datastore::recall_open_staging(&url, deadline)
			.await?
			.with_capabilities(capabilities.clone());
		let rebuilt =
			recall_index_erasure::rebuild_current(&staging, &plan.targets, deadline).await;
		let closed = staging.recall_shutdown_staging(deadline).await;
		drop(staging);
		rebuilt?;
		closed?;
		remaining(deadline)?;
		Ok(())
	})
}

fn perform(
	state: &PhysicalState,
	deadline: Instant,
	authorities: &[RecallHistoryAuthority],
	capabilities: Option<&Capabilities>,
	recovery_required: &mut bool,
) -> Result<(), Error> {
	remaining(deadline)?;
	let started_unix_ms = unix_ms()?;
	let mut native = state.db.lock().map_err(failure)?;
	let source = native
		.as_mut()
		.ok_or_else(|| failure("native generation closed"))?;
	let plan = PreparedHistory::prepare(source, authorities, deadline)?;
	if plan.is_some() && capabilities.is_none() {
		return Err(failure("original capability policy is absent"));
	}
	let select = |record: &CheckpointRecord<'_>| {
		plan.as_ref()
			.map_or(Ok(CheckpointDecision::Keep), |plan| plan.decision(record))
	};
	let expected_copy = selected_fingerprint(source, deadline, select)?;
	let previous =
		read_receipt(&sibling(&state.path, RECEIPT_SUFFIX)?, true)?.map(|value| value.current);
	let old_identity = read_identity(&state.path)?;
	if previous
		.as_ref()
		.is_some_and(|value| value.engine_identity != old_identity)
	{
		return Err(failure("prior checkpoint belongs to another generation"));
	}
	// From the first directory-journal mutation onward, every failure requires
	// the original recovery decision before new native admission.
	*recovery_required = true;
	let generation = Generation::prepare(&state.path).map_err(failure)?;
	source
		.write_selected_checkpoint(generation.staged_path(), deadline, select)
		.map_err(failure)?;
	source.close().map_err(failure)?;
	let new_identity = ensure_identity(generation.staged_path())?;
	let (copied, protected) = inspect_stage(
		generation.staged_path(),
		&state.options,
		deadline,
		plan.as_ref(),
		expected_copy.high_water,
	)?;
	if copied != expected_copy {
		return Err(failure("selected native history copy differs"));
	}
	if let Some(plan) = &plan {
		rebuild_stage(
			generation.staged_path(),
			&state.options,
			plan,
			deadline,
			capabilities.ok_or_else(|| failure("original capability policy is absent"))?,
		)?;
	}
	let (expected, rebuilt_protected) = inspect_stage(
		generation.staged_path(),
		&state.options,
		deadline,
		plan.as_ref(),
		expected_copy.high_water,
	)?;
	if protected != rebuilt_protected || expected.high_water < expected_copy.high_water {
		return Err(failure(
			"derived reconstruction changed unrelated history or transaction clock",
		));
	}
	let current = Checkpoint {
		sequence: previous
			.as_ref()
			.map_or(Some(1), |value| value.sequence.checked_add(1))
			.ok_or_else(|| failure("sequence exhausted"))?,
		old_engine_identity: old_identity,
		engine_identity: new_identity,
		old_directory: directory_identity(&state.path)?,
		current_directory: directory_identity(generation.staged_path())?,
		rows: expected.rows,
		high_water: expected.high_water,
		stream_sha256: expected.sha256,
		started_unix_ms,
		completed_unix_ms: unix_ms()?,
		previous_sha256: previous.as_ref().map(checkpoint_hash).transpose()?,
		original_payload_generations: files(&state.path, deadline)?,
		current_payload_generations: files(generation.staged_path(), deadline)?,
		native_snapshots: 0,
		background_errors: 0,
		completed: false,
		hard_cancellation_supported: false,
	};
	verify_store(generation.staged_path(), &state.options, &current, deadline)?;
	let receipt = Receipt {
		format: "recall-surrealkv-physical-checkpoint/v1".into(),
		engine: "surrealkv".into(),
		wrapper_version: "0.9.3".into(),
		current_sha256: checkpoint_hash(&current)?,
		current,
		previous,
	};
	write_pending(&state.path, &receipt)?;
	drop(native.take());
	generation
		.promote(deadline, |live| {
			verify_store(live, &state.options, &receipt.current, deadline).map_err(io_failure)
		})
		.map_err(failure)?;
	remaining(deadline)?;
	verify_store(&state.path, &state.options, &receipt.current, deadline)?;
	*native = Some(Store::new(state.options.clone()).map_err(failure)?);
	complete_receipt(&state.path, receipt, deadline)?;
	remaining(deadline)?;
	Ok(())
}

#[cfg(test)]
#[path = "physical_checkpoint_tests.rs"]
mod tests;
