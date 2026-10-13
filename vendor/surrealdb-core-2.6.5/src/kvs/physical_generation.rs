//! Crash recovery for an exclusively owned native database generation.
//!
//! Engine adapters must stop admission, drain transactions/background users and
//! close every old and staged engine handle before promotion. They must hold a
//! stable, never-unlinked OS owner lock before recovery and for the entire
//! datastore lifetime (including blocking work). They must verify
//! that the rebuilt stage preserves the required live/history semantics and can
//! be reopened. This module proves filesystem custody and retirement only.
//! `recover` runs before opening an engine or admitting any request. Errors must
//! withhold readiness and erasure completion; the original erasure journal owns
//! retry. Custody of the parent namespace against concurrent replacement is a
//! caller precondition, not a hostile-filesystem race guarantee.

use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

const OWNER: &[u8] = b"recall-native-generation-v1\n";
const DECISION: &[u8; 8] = b"RCLGEN01";
const STAGED_LOCK: &str = ".staged.recall-native-owner.lock";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
	device: u64,
	inode: u64,
}

#[cfg(unix)]
#[expect(
	clippy::unnecessary_wraps,
	reason = "Keep the same fallible identity API as the non-Unix implementation"
)]
fn identity(metadata: &Metadata) -> io::Result<Identity> {
	use std::os::unix::fs::MetadataExt;
	Ok(Identity {
		device: metadata.dev(),
		inode: metadata.ino(),
	})
}

#[cfg(not(unix))]
fn identity(_: &Metadata) -> io::Result<Identity> {
	Err(io::Error::new(
		io::ErrorKind::Unsupported,
		"native generation identity unavailable",
	))
}

fn invalid(message: &'static str) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidData, message)
}

fn metadata(path: &Path) -> io::Result<Option<Metadata>> {
	match fs::symlink_metadata(path) {
		Ok(value) => Ok(Some(value)),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error),
	}
}

fn directory(path: &Path, device: u64) -> io::Result<Identity> {
	let value = fs::symlink_metadata(path)?;
	let id = identity(&value)?;
	if !value.is_dir() || value.file_type().is_symlink() || id.device != device {
		return Err(invalid(
			"generation directory is aliased or on another device",
		));
	}
	Ok(id)
}

fn regular(path: &Path, device: u64) -> io::Result<Identity> {
	let value = fs::symlink_metadata(path)?;
	let id = identity(&value)?;
	if !value.is_file() || value.file_type().is_symlink() || id.device != device {
		return Err(invalid("generation file is not an owned regular file"));
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		if value.nlink() != 1 {
			return Err(invalid("generation file has an external hard link"));
		}
	}
	Ok(id)
}

fn private_directory(path: &Path) -> io::Result<()> {
	let mut builder = fs::DirBuilder::new();
	#[cfg(unix)]
	{
		use std::os::unix::fs::DirBuilderExt;
		builder.mode(0o700);
	}
	builder.create(path)
}

fn sync_directory(path: &Path) -> io::Result<()> {
	File::open(path)?.sync_all()
}

fn write_marker(path: &Path, bytes: &[u8]) -> io::Result<()> {
	let mut options = OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		options.mode(0o600);
	}
	let mut file = options.open(path)?;
	file.write_all(bytes)?;
	file.sync_all()
}

fn read_marker(path: &Path, device: u64, length: usize) -> io::Result<Vec<u8>> {
	let before = regular(path, device)?;
	let file = File::open(path)?;
	if identity(&file.metadata()?)? != before || file.metadata()?.len() != length as u64 {
		return Err(invalid("generation marker identity or length changed"));
	}
	let mut bytes = Vec::new();
	file.take(length as u64 + 1).read_to_end(&mut bytes)?;
	if bytes.len() != length {
		return Err(invalid("incomplete generation marker"));
	}
	Ok(bytes)
}

#[expect(
	clippy::cast_possible_truncation,
	reason = "The preceding bound proves length fits the expected slice's usize length"
)]
fn discard_marker_prefix(path: &Path, device: u64, expected: &[u8]) -> io::Result<()> {
	regular(path, device)?;
	let length = fs::symlink_metadata(path)?.len();
	if length > expected.len() as u64 {
		return Err(invalid(
			"temporary generation marker exceeds its exact format",
		));
	}
	let bytes = read_marker(path, device, length as usize)?;
	if !expected.starts_with(&bytes) {
		return Err(invalid(
			"temporary generation marker is not an owned write prefix",
		));
	}
	fs::remove_file(path)
}

struct Paths {
	live: PathBuf,
	parent: PathBuf,
	control: PathBuf,
	staged: PathBuf,
	retired: PathBuf,
	owner: Vec<u8>,
	device: u64,
}

impl Paths {
	fn new(live: &Path) -> io::Result<Self> {
		if live.as_os_str().is_empty()
			|| live
				.components()
				.any(|part| matches!(part, Component::ParentDir | Component::CurDir))
		{
			return Err(invalid("generation path must not contain traversal"));
		}
		let live = if live.is_absolute() {
			live.to_path_buf()
		} else {
			std::env::current_dir()?.join(live)
		};
		if live.file_name().is_none() {
			return Err(invalid("generation path must name a database directory"));
		}
		let parent = live
			.parent()
			.ok_or_else(|| invalid("generation parent missing"))?
			.to_path_buf();
		let mut ancestor = PathBuf::new();
		for component in parent.components() {
			ancestor.push(component);
			let value = fs::symlink_metadata(&ancestor)?;
			if !value.is_dir() || value.file_type().is_symlink() {
				return Err(invalid("generation ancestor is not a real directory"));
			}
		}
		let device = identity(&fs::symlink_metadata(&parent)?)?.device;
		if metadata(&live)?.is_some() {
			directory(&live, device)?;
			if fs::canonicalize(&live)? != live {
				return Err(invalid("generation path is not canonical"));
			}
		}
		let hash = format!("{:x}", Sha256::digest(live.as_os_str().as_encoded_bytes()));
		let control = parent.join(format!(".recall-physical-{hash}"));
		let mut owner = OWNER.to_vec();
		owner.extend_from_slice(hash.as_bytes());
		Ok(Self {
			live,
			parent,
			staged: control.join("staged"),
			retired: control.join("retired"),
			control,
			owner,
			device,
		})
	}

	fn private_control(&self) -> io::Result<()> {
		directory(&self.control, self.device)?;
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			if fs::symlink_metadata(&self.control)?.permissions().mode() & 0o077 != 0 {
				return Err(invalid("generation control directory is not private"));
			}
		}
		Ok(())
	}

	fn initialize_control(&self) -> io::Result<()> {
		self.private_control()?;
		if metadata(&self.control.join("owner"))?.is_some() {
			return self.validate_control();
		}
		// Caller holds the stable datastore OS lock. Before ownership is
		// installed, only an empty private directory or an exact bounded prefix
		// of our own initialization write may exist. No stage can precede it.
		directory(&self.live, self.device)?;
		for entry in fs::read_dir(&self.control)? {
			if entry?.file_name() != "owner.next" {
				return Err(invalid(
					"unowned generation directory contains foreign entries",
				));
			}
		}
		let temporary = self.control.join("owner.next");
		if metadata(&temporary)?.is_some() {
			discard_marker_prefix(&temporary, self.device, &self.owner)?;
			sync_directory(&self.control)?;
		}
		write_marker(&temporary, &self.owner)?;
		fs::rename(temporary, self.control.join("owner"))?;
		self.sync_parents()?;
		self.validate_control()
	}

	fn validate_control(&self) -> io::Result<()> {
		self.private_control()?;
		if read_marker(&self.control.join("owner"), self.device, self.owner.len())? != self.owner {
			return Err(invalid("foreign generation control directory"));
		}
		for entry in fs::read_dir(&self.control)? {
			let entry = entry?;
			if ![
				"owner",
				"staged",
				"retired",
				"promote",
				"promote.next",
				STAGED_LOCK,
			]
			.iter()
			.any(|name| entry.file_name() == *name)
			{
				return Err(invalid("unrecognized generation control entry"));
			}
		}
		Ok(())
	}

	fn retire_staged_lock(&self) -> io::Result<()> {
		let path = self.control.join(STAGED_LOCK);
		if metadata(&path)?.is_none() {
			return Ok(());
		}
		let expected = regular(&path, self.device)?;
		let lock = OpenOptions::new().read(true).write(true).open(&path)?;
		if identity(&lock.metadata()?)? != expected || lock.metadata()?.len() != 0 {
			return Err(invalid(
				"staged owner lock identity or empty format differs",
			));
		}
		// The caller's outer lifetime lock owns the whole private namespace.
		// Independently refuse an unclosed staged datastore before removing its
		// one exact empty sibling lock. Never touch the original live owner lock.
		lock.try_lock().map_err(io::Error::other)?;
		if regular(&path, self.device)? != expected {
			return Err(invalid("staged owner lock changed before retirement"));
		}
		fs::remove_file(path)?;
		sync_directory(&self.control)?;
		drop(lock);
		Ok(())
	}

	fn sync_parents(&self) -> io::Result<()> {
		sync_directory(&self.control)?;
		sync_directory(&self.parent)
	}

	fn finish(
		&self,
		mut validate: impl FnMut(&Path) -> io::Result<()>,
		mut checkpoint: impl FnMut(Boundary) -> io::Result<()>,
	) -> io::Result<()> {
		self.validate_control()?;
		self.retire_staged_lock()?;
		if metadata(&self.control.join("promote.next"))?.is_some() {
			return Err(invalid(
				"committed generation unexpectedly retains a decision temporary",
			));
		}
		let decision = Decision::read(self)?;
		let staged = metadata(&self.staged)?.is_some();
		let retired = metadata(&self.retired)?.is_some();
		let live = metadata(&self.live)?.is_some();
		if staged {
			if directory(&self.staged, self.device)? != decision.new {
				return Err(invalid("staged generation no longer has captured identity"));
			}
			if live && !retired {
				if directory(&self.live, self.device)? != decision.old {
					return Err(invalid("old live generation identity changed"));
				}
				// Validate before moving: later retirement must not follow any link.
				tree(&self.live, self.device, None)?;
				fs::rename(&self.live, &self.retired)?;
				checkpoint(Boundary::OldRenamed)?;
				self.sync_parents()?;
				checkpoint(Boundary::OldRenameSynced)?;
			} else if live || !retired {
				return Err(invalid("impossible generation promotion shape"));
			}
			if directory(&self.retired, self.device)? != decision.old {
				return Err(invalid("retired generation identity changed"));
			}
			// Also finish an interrupted old rename's directory durability before
			// exposing the target name. A decision is never rolled back.
			self.sync_parents()?;
			fs::rename(&self.staged, &self.live)?;
			checkpoint(Boundary::NewRenamed)?;
		} else if !live {
			return Err(invalid("committed target generation is missing"));
		}
		if directory(&self.live, self.device)? != decision.new {
			return Err(invalid(
				"promoted live generation is not the captured target",
			));
		}
		self.sync_parents()?;
		checkpoint(Boundary::NewRenameSynced)?;
		// Engine options may depend on the final path. Reopen, independently
		// validate against the caller's durable pending proof, and close it
		// before discarding even the first retired byte. No rollback on error.
		validate(&self.live)?;
		if directory(&self.live, self.device)? != decision.new {
			return Err(invalid("final validation replaced the promoted generation"));
		}
		sync_tree(&self.live, self.device, None)?;
		self.sync_parents()?;
		checkpoint(Boundary::FinalValidated)?;
		if metadata(&self.retired)?.is_some() {
			if directory(&self.retired, self.device)? != decision.old {
				return Err(invalid("retirement refuses an uncaptured directory"));
			}
			remove_tree(&self.retired, self.device)?;
		}
		checkpoint(Boundary::RetiredRemoved)?;
		self.sync_parents()?;
		checkpoint(Boundary::RetirementSynced)?;
		fs::remove_file(self.control.join("promote"))?;
		checkpoint(Boundary::DecisionRemoved)?;
		self.sync_parents()?;
		checkpoint(Boundary::Complete)
	}
}

#[derive(Clone, Copy)]
struct Decision {
	old: Identity,
	new: Identity,
}

impl Decision {
	fn bytes(self) -> Vec<u8> {
		let mut bytes = DECISION.to_vec();
		bytes.extend_from_slice(&self.old.device.to_le_bytes());
		bytes.extend_from_slice(&self.old.inode.to_le_bytes());
		bytes.extend_from_slice(&self.new.inode.to_le_bytes());
		bytes
	}

	fn read(paths: &Paths) -> io::Result<Self> {
		let bytes = read_marker(&paths.control.join("promote"), paths.device, 32)?;
		if &bytes[..8] != DECISION {
			return Err(invalid("unrecognized generation decision"));
		}
		let number = |start| -> io::Result<u64> {
			Ok(u64::from_le_bytes(
				bytes[start..start + 8]
					.try_into()
					.map_err(|_| invalid("invalid generation decision"))?,
			))
		};
		let device = number(8)?;
		let old = Identity {
			device,
			inode: number(16)?,
		};
		let new = Identity {
			device,
			inode: number(24)?,
		};
		if device != paths.device || old == new {
			return Err(invalid(
				"generation decision has invalid filesystem identities",
			));
		}
		Ok(Self { old, new })
	}
}

// The two lists retain exact inode custody and give directories child-first
// ordering when reversed. Never call remove_dir_all on an engine path.
type Entries = Vec<(PathBuf, Identity)>;
fn before(deadline: Option<Instant>) -> io::Result<()> {
	if deadline.is_some_and(|value| Instant::now() >= value) {
		return Err(io::Error::new(
			io::ErrorKind::TimedOut,
			"original generation deadline expired",
		));
	}
	Ok(())
}

fn tree(root: &Path, device: u64, deadline: Option<Instant>) -> io::Result<(Entries, Entries)> {
	let mut files = Vec::new();
	let mut directories = Vec::new();
	let mut pending = vec![root.to_path_buf()];
	while let Some(path) = pending.pop() {
		before(deadline)?;
		directories.push((path.clone(), directory(&path, device)?));
		for entry in fs::read_dir(&path)? {
			before(deadline)?;
			let path = entry?.path();
			let value = fs::symlink_metadata(&path)?;
			if value.is_dir() && !value.file_type().is_symlink() {
				pending.push(path);
			} else {
				let id = regular(&path, device)?;
				files.push((path, id));
			}
		}
	}
	Ok((files, directories))
}

fn sync_tree(root: &Path, device: u64, deadline: Option<Instant>) -> io::Result<()> {
	let (files, directories) = tree(root, device, deadline)?;
	for (path, expected) in files {
		before(deadline)?;
		let file = File::open(&path)?;
		if identity(&file.metadata()?)? != expected {
			return Err(invalid(
				"generation file identity changed during synchronization",
			));
		}
		file.sync_all()?;
	}
	for (path, expected) in directories.into_iter().rev() {
		before(deadline)?;
		if directory(&path, device)? != expected {
			return Err(invalid(
				"generation directory identity changed during synchronization",
			));
		}
		sync_directory(&path)?;
	}
	Ok(())
}

fn remove_tree(root: &Path, device: u64) -> io::Result<()> {
	let (files, directories) = tree(root, device, None)?;
	for (path, expected) in files {
		if regular(&path, device)? != expected {
			return Err(invalid("retired file identity changed"));
		}
		fs::remove_file(path)?;
	}
	for (path, expected) in directories.into_iter().rev() {
		if directory(&path, device)? != expected {
			return Err(invalid("retired directory identity changed"));
		}
		fs::remove_dir(path)?;
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
	TargetSynced,
	DecisionPrepared,
	DecisionRenamed,
	DecisionSynced,
	OldRenamed,
	OldRenameSynced,
	NewRenamed,
	NewRenameSynced,
	FinalValidated,
	RetiredRemoved,
	RetirementSynced,
	DecisionRemoved,
	Complete,
}

/// An empty staged directory under this database's private filesystem custody.
/// Drop deliberately does nothing: only explicit pre-open recovery may abandon
/// an interrupted stage. No destructor can make erasure appear complete.
#[expect(
	clippy::redundant_pub_crate,
	reason = "Keep the explicit crate-only API in production and integration-test inclusions"
)]
pub(crate) struct Generation {
	paths: Paths,
	decision: Decision,
}

impl Generation {
	pub(crate) fn prepare(live: &Path) -> io::Result<Self> {
		let paths = Paths::new(live)?;
		let old = directory(&paths.live, paths.device)?;
		if metadata(&paths.control)?.is_none() {
			private_directory(&paths.control)?;
		}
		paths.initialize_control()?;
		if [
			paths.staged.as_path(),
			paths.retired.as_path(),
			&paths.control.join("promote"),
			&paths.control.join("promote.next"),
		]
		.into_iter()
		.map(metadata)
		.collect::<io::Result<Vec<_>>>()?
		.iter()
		.any(Option::is_some)
		{
			return Err(invalid("generation recovery required before preparation"));
		}
		private_directory(&paths.staged)?;
		let new = directory(&paths.staged, paths.device)?;
		paths.sync_parents()?;
		Ok(Self {
			paths,
			decision: Decision { old, new },
		})
	}

	pub(crate) fn staged_path(&self) -> &Path {
		&self.paths.staged
	}

	/// Irreversibly promote a verified, closed engine generation. Success requires
	/// physical removal of all captured retired entries and durable parent updates.
	/// The original deadline gates preparation and the irreversible decision.
	/// Once decided, mandatory cleanup drains even after expiry; an expired call
	/// still returns `TimedOut` after drainage and cannot authorize readiness.
	pub(crate) fn promote(
		self,
		deadline: Instant,
		validate: impl FnMut(&Path) -> io::Result<()>,
	) -> io::Result<()> {
		self.promote_with(deadline, validate, |_| Ok(()))
	}

	fn promote_with(
		self,
		deadline: Instant,
		validate: impl FnMut(&Path) -> io::Result<()>,
		mut checkpoint: impl FnMut(Boundary) -> io::Result<()>,
	) -> io::Result<()> {
		self.paths.validate_control()?;
		self.paths.retire_staged_lock()?;
		if directory(&self.paths.live, self.paths.device)? != self.decision.old
			|| directory(&self.paths.staged, self.paths.device)? != self.decision.new
			|| metadata(&self.paths.retired)?.is_some()
			|| metadata(&self.paths.control.join("promote"))?.is_some()
			|| metadata(&self.paths.control.join("promote.next"))?.is_some()
		{
			return Err(invalid("generation identity changed before decision"));
		}
		tree(&self.paths.live, self.paths.device, Some(deadline))?;
		sync_tree(&self.paths.staged, self.paths.device, Some(deadline))?;
		self.paths.sync_parents()?;
		checkpoint(Boundary::TargetSynced)?;
		before(Some(deadline))?;
		let temporary = self.paths.control.join("promote.next");
		write_marker(&temporary, &self.decision.bytes())?;
		checkpoint(Boundary::DecisionPrepared)?;
		before(Some(deadline))?;
		fs::rename(temporary, self.paths.control.join("promote"))?;
		checkpoint(Boundary::DecisionRenamed)?;
		self.paths.sync_parents()?;
		checkpoint(Boundary::DecisionSynced)?;
		self.paths.finish(validate, checkpoint)?;
		before(Some(deadline))
	}
}

/// Recover under exclusive startup admission, before opening either engine.
/// A durable decision always rolls forward; an undecided stage is discarded and
/// the original live generation remains for the original journal's retry.
#[expect(
	clippy::redundant_pub_crate,
	reason = "Keep the explicit crate-only API in production and integration-test inclusions"
)]
pub(crate) fn recover(
	live: &Path,
	validate: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
	let paths = match Paths::new(live) {
		Ok(paths) => paths,
		// A missing ancestor cannot contain this database's control directory.
		// Existing ancestors were checked before reaching the missing one. This
		// preserves ordinary engine creation of a new nested database path.
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(error),
	};
	if metadata(&paths.control)?.is_none() {
		return Ok(());
	}
	paths.initialize_control()?;
	if metadata(&paths.control.join("promote"))?.is_some() {
		return paths.finish(validate, |_| Ok(()));
	}
	directory(&paths.live, paths.device)?;
	if metadata(&paths.retired)?.is_some() {
		return Err(invalid(
			"retired generation exists without durable promotion decision",
		));
	}
	paths.retire_staged_lock()?;
	let temporary = paths.control.join("promote.next");
	if metadata(&temporary)?.is_some() {
		let expected = Decision {
			old: directory(&paths.live, paths.device)?,
			new: directory(&paths.staged, paths.device)?,
		}
		.bytes();
		discard_marker_prefix(&temporary, paths.device, &expected)?;
		paths.sync_parents()?;
	}
	if metadata(&paths.staged)?.is_some() {
		remove_tree(&paths.staged, paths.device)?;
	}
	paths.sync_parents()
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;
	use std::os::unix::fs::symlink;

	fn deadline() -> Instant {
		Instant::now() + std::time::Duration::from_secs(30)
	}

	fn fixture() -> io::Result<(tempfile::TempDir, PathBuf, Generation)> {
		let root = tempfile::tempdir()?;
		// macOS tempdir may use /var, whose canonical spelling is /private/var.
		let live = fs::canonicalize(root.path())?.join("database");
		fs::create_dir(&live)?;
		fs::write(live.join("old-payload"), b"erased-source")?;
		let generation = Generation::prepare(&live)?;
		fs::create_dir(generation.staged_path().join("versions"))?;
		fs::write(
			generation.staged_path().join("versions/retained"),
			b"unrelated-history",
		)?;
		Ok((root, live, generation))
	}

	#[test]
	fn promotion_removes_every_retired_file_and_keeps_unrelated_history() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		generation.promote(deadline(), |_| Ok(()))?;
		assert_eq!(
			fs::read(live.join("versions/retained"))?,
			b"unrelated-history"
		);
		assert!(!live.join("old-payload").exists());
		let paths = Paths::new(&live)?;
		assert!(!paths.retired.exists() && !paths.staged.exists());
		assert!(!paths.control.join("promote").exists());
		assert_eq!(fs::read(paths.control.join("owner"))?, paths.owner);
		recover(&live, |_| Ok(()))?;
		recover(&live, |_| Ok(()))
	}

	#[test]
	fn every_committed_cut_recovers_forward_without_resurrecting_old_payload() -> io::Result<()> {
		for cut in [
			Boundary::DecisionRenamed,
			Boundary::DecisionSynced,
			Boundary::OldRenamed,
			Boundary::OldRenameSynced,
			Boundary::NewRenamed,
			Boundary::NewRenameSynced,
			Boundary::FinalValidated,
			Boundary::RetiredRemoved,
			Boundary::RetirementSynced,
			Boundary::DecisionRemoved,
			Boundary::Complete,
		] {
			let (_root, live, generation) = fixture()?;
			assert!(generation
				.promote_with(
					deadline(),
					|_| Ok(()),
					|point| if point == cut {
						Err(io::Error::other("injected crash cut"))
					} else {
						Ok(())
					}
				)
				.is_err());
			recover(&live, |_| Ok(()))?;
			recover(&live, |_| Ok(()))?;
			assert_eq!(
				fs::read(live.join("versions/retained"))?,
				b"unrelated-history",
				"{cut:?}"
			);
			assert!(!live.join("old-payload").exists(), "{cut:?}");
			assert!(!Paths::new(&live)?.retired.exists(), "{cut:?}");
		}
		Ok(())
	}

	#[test]
	fn undecided_stage_is_abandoned_without_losing_old_engine_or_original_journal() -> io::Result<()>
	{
		let (root, live, generation) = fixture()?;
		let journal = root.path().join("source-erasure-journal");
		fs::write(&journal, b"pending-original-intent")?;
		assert!(generation
			.promote_with(
				deadline(),
				|_| Ok(()),
				|point| if point == Boundary::TargetSynced {
					Err(io::Error::other("before decision"))
				} else {
					Ok(())
				}
			)
			.is_err());
		recover(&live, |_| Ok(()))?;
		assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
		assert_eq!(fs::read(journal)?, b"pending-original-intent");
		assert!(!Paths::new(&live)?.staged.exists());
		Ok(())
	}

	#[test]
	fn incomplete_decision_blocks_readiness_and_preserves_both_generations() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		fs::write(generation.paths.control.join("promote"), b"RCL")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
		assert!(generation.staged_path().join("versions/retained").exists());
		Ok(())
	}

	#[test]
	fn partial_and_fully_synced_decision_temporaries_remain_predecision() -> io::Result<()> {
		for length in [0, 3, 8, 17, 31, 32] {
			let (_root, live, generation) = fixture()?;
			let temporary = generation.paths.control.join("promote.next");
			write_marker(&temporary, &generation.decision.bytes()[..length])?;
			recover(&live, |_| {
				Err(io::Error::other("no final-path validation before decision"))
			})?;
			assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
			assert!(!temporary.exists());
			assert!(!generation.staged_path().exists());
		}
		let (_root, live, generation) = fixture()?;
		assert!(generation
			.promote_with(
				deadline(),
				|_| Ok(()),
				|point| {
					if point == Boundary::DecisionPrepared {
						Err(io::Error::other("synced temporary before rename"))
					} else {
						Ok(())
					}
				}
			)
			.is_err());
		recover(&live, |_| Ok(()))?;
		assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
		Ok(())
	}

	#[test]
	fn foreign_temporary_bytes_are_not_removed_as_an_interrupted_decision() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		let temporary = generation.paths.control.join("promote.next");
		fs::write(&temporary, b"foreign")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert_eq!(fs::read(temporary)?, b"foreign");
		assert!(generation.staged_path().join("versions/retained").exists());
		Ok(())
	}

	#[test]
	fn stage_and_retired_symlinks_never_delete_foreign_files() -> io::Result<()> {
		for staged in [true, false] {
			let (root, live, generation) = fixture()?;
			let foreign = root.path().join("foreign");
			fs::create_dir(&foreign)?;
			fs::write(foreign.join("keep"), b"foreign")?;
			if staged {
				symlink(&foreign, generation.staged_path().join("alias"))?;
				assert!(generation.promote(deadline(), |_| Ok(())).is_err());
			} else {
				let control = generation.paths.control.clone();
				assert!(generation
					.promote_with(
						deadline(),
						|_| Ok(()),
						|point| if point == Boundary::NewRenameSynced {
							Err(io::Error::other("before retirement"))
						} else {
							Ok(())
						}
					)
					.is_err());
				symlink(&foreign, control.join("retired/alias"))?;
				assert!(recover(&live, |_| Ok(())).is_err());
				assert!(control.join("promote").exists());
			}
			assert_eq!(fs::read(foreign.join("keep"))?, b"foreign");
		}
		Ok(())
	}

	#[test]
	fn hardlinked_payload_refuses_completion_without_deleting_other_link() -> io::Result<()> {
		let (root, live, generation) = fixture()?;
		let external = root.path().join("external-copy");
		fs::hard_link(live.join("old-payload"), &external)?;
		assert!(generation.promote(deadline(), |_| Ok(())).is_err());
		assert_eq!(fs::read(external)?, b"erased-source");
		assert!(live.join("old-payload").exists());
		Ok(())
	}

	#[test]
	fn replaced_target_inode_never_qualifies_as_promoted_generation() -> io::Result<()> {
		let (root, live, generation) = fixture()?;
		assert!(generation
			.promote_with(
				deadline(),
				|_| Ok(()),
				|point| if point == Boundary::NewRenamed {
					Err(io::Error::other("after new rename"))
				} else {
					Ok(())
				}
			)
			.is_err());
		fs::rename(&live, root.path().join("saved-target"))?;
		fs::create_dir(&live)?;
		fs::write(live.join("other"), b"unrelated-owner")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert_eq!(fs::read(live.join("other"))?, b"unrelated-owner");
		assert!(Paths::new(&live)?.retired.join("old-payload").exists());
		Ok(())
	}

	#[test]
	fn final_path_validation_failure_preserves_retired_engine_and_retry_revalidates(
	) -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		let paths = Paths::new(&live)?;
		let mut calls = 0;
		assert!(generation
			.promote(deadline(), |path| {
				calls += 1;
				assert_eq!(path, live);
				assert_eq!(
					fs::read(path.join("versions/retained"))?,
					b"unrelated-history"
				);
				assert_eq!(
					fs::read(paths.retired.join("old-payload"))?,
					b"erased-source"
				);
				Err(io::Error::other("actual engine reopen rejected"))
			})
			.is_err());
		assert_eq!(calls, 1);
		assert!(paths.control.join("promote").exists());
		assert!(recover(&live, |_| Err(io::Error::other("still cannot verify"))).is_err());
		assert!(paths.retired.join("old-payload").exists());
		recover(&live, |path| {
			calls += 1;
			assert_eq!(path, live);
			assert!(paths.retired.join("old-payload").exists());
			Ok(())
		})?;
		assert_eq!(calls, 2);
		assert!(!paths.retired.exists());
		Ok(())
	}

	#[test]
	fn validation_cannot_replace_live_path_then_retire_original_engine() -> io::Result<()> {
		let (root, live, generation) = fixture()?;
		assert!(generation
			.promote(deadline(), |path| {
				fs::rename(path, root.path().join("validated-but-moved"))?;
				fs::create_dir(path)?;
				Ok(())
			})
			.is_err());
		assert!(Paths::new(&live)?.retired.join("old-payload").exists());
		Ok(())
	}

	#[test]
	fn interrupted_partial_retirement_resumes_with_original_decision() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		fs::write(live.join("old-second"), b"more-erased-source")?;
		assert!(generation
			.promote_with(
				deadline(),
				|_| Ok(()),
				|point| if point == Boundary::NewRenameSynced {
					Err(io::Error::other("before retirement"))
				} else {
					Ok(())
				}
			)
			.is_err());
		let paths = Paths::new(&live)?;
		fs::remove_file(paths.retired.join("old-payload"))?;
		recover(&live, |_| Ok(()))?;
		assert!(!paths.retired.exists());
		assert_eq!(
			fs::read(live.join("versions/retained"))?,
			b"unrelated-history"
		);
		Ok(())
	}

	#[test]
	fn foreign_control_entries_and_parent_aliases_are_refused() -> io::Result<()> {
		let (root, live, generation) = fixture()?;
		fs::write(generation.paths.control.join("foreign"), b"preserve")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert_eq!(
			fs::read(generation.paths.control.join("foreign"))?,
			b"preserve"
		);
		let alias = root.path().join("alias");
		symlink(
			live.parent()
				.ok_or_else(|| invalid("test parent missing"))?,
			&alias,
		)?;
		assert!(recover(&alias.join("database"), |_| Ok(())).is_err());
		assert!(recover(&live.join("../database"), |_| Ok(())).is_err());
		Ok(())
	}

	#[test]
	fn unowned_retired_directory_without_decision_is_never_removed() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		fs::create_dir(&generation.paths.retired)?;
		fs::write(generation.paths.retired.join("foreign"), b"do-not-delete")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert_eq!(
			fs::read(generation.paths.retired.join("foreign"))?,
			b"do-not-delete"
		);
		Ok(())
	}

	#[test]
	fn expired_original_deadline_never_creates_irreversible_decision() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		let paths = Paths::new(&live)?;
		let mut validated = false;
		let result = generation.promote(Instant::now(), |_| {
			validated = true;
			Ok(())
		});
		assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
		assert!(!validated);
		assert!(!paths.control.join("promote").exists());
		assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
		recover(&live, |_| {
			Err(io::Error::other("undecided recovery must not validate"))
		})?;
		assert!(!paths.staged.exists());
		Ok(())
	}

	#[test]
	fn incomplete_initial_owner_is_not_adopted_or_deleted() -> io::Result<()> {
		let root = tempfile::tempdir()?;
		let live = fs::canonicalize(root.path())?.join("database");
		fs::create_dir(&live)?;
		let paths = Paths::new(&live)?;
		private_directory(&paths.control)?;
		fs::write(paths.control.join("owner"), b"incomplete")?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert!(Generation::prepare(&live).is_err());
		assert_eq!(fs::read(paths.control.join("owner"))?, b"incomplete");
		assert!(live.exists());
		Ok(())
	}

	#[test]
	fn interrupted_empty_or_partial_owner_initialization_recovers_under_stable_owner(
	) -> io::Result<()> {
		for length in [None, Some(0), Some(3), Some(26), Some(90)] {
			let root = tempfile::tempdir()?;
			let live = fs::canonicalize(root.path())?.join("database");
			fs::create_dir(&live)?;
			fs::write(live.join("original"), b"original-live")?;
			let paths = Paths::new(&live)?;
			private_directory(&paths.control)?;
			if let Some(length) = length {
				write_marker(
					&paths.control.join("owner.next"),
					&paths.owner[..length.min(paths.owner.len())],
				)?;
			}
			recover(&live, |_| {
				Err(io::Error::other("initialization never promotes"))
			})?;
			assert_eq!(fs::read(paths.control.join("owner"))?, paths.owner);
			assert!(!paths.control.join("owner.next").exists());
			assert_eq!(fs::read(live.join("original"))?, b"original-live");
			assert!(Generation::prepare(&live).is_ok());
		}
		Ok(())
	}

	#[test]
	fn owner_initialization_refuses_foreign_prefix_or_additional_files() -> io::Result<()> {
		for extra in [false, true] {
			let root = tempfile::tempdir()?;
			let live = fs::canonicalize(root.path())?.join("database");
			fs::create_dir(&live)?;
			let paths = Paths::new(&live)?;
			private_directory(&paths.control)?;
			let name = if extra { "foreign" } else { "owner.next" };
			fs::write(paths.control.join(name), b"not-our-prefix")?;
			assert!(recover(&live, |_| Ok(())).is_err());
			assert_eq!(fs::read(paths.control.join(name))?, b"not-our-prefix");
			assert!(!paths.control.join("owner").exists());
		}
		Ok(())
	}

	#[test]
	fn first_open_of_uncreated_nested_database_requires_no_generation_recovery() -> io::Result<()> {
		let root = tempfile::tempdir()?;
		let live = fs::canonicalize(root.path())?.join("new-parent/database");
		recover(&live, |_| {
			Err(io::Error::other("no committed generation exists"))
		})?;
		assert!(!live
			.parent()
			.ok_or_else(|| invalid("test parent missing"))?
			.exists());
		Ok(())
	}

	#[test]
	fn missing_staged_target_cannot_misidentify_original_live_as_completed() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		let paths = Paths::new(&live)?;
		assert!(generation
			.promote_with(
				deadline(),
				|_| Ok(()),
				|point| {
					if point == Boundary::DecisionSynced {
						Err(io::Error::other("after decision"))
					} else {
						Ok(())
					}
				}
			)
			.is_err());
		remove_tree(&paths.staged, paths.device)?;
		assert!(recover(&live, |_| Ok(())).is_err());
		assert!(paths.control.join("promote").exists());
		assert_eq!(fs::read(live.join("old-payload"))?, b"erased-source");
		Ok(())
	}

	#[test]
	fn successful_generation_can_be_rebuilt_again_under_same_original_control() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		generation.promote(deadline(), |_| Ok(()))?;
		let again = Generation::prepare(&live)?;
		fs::write(
			again.staged_path().join("last"),
			b"next-complete-generation",
		)?;
		again.promote(deadline(), |_| Ok(()))?;
		recover(&live, |_| Ok(()))?;
		assert_eq!(fs::read(live.join("last"))?, b"next-complete-generation");
		assert!(!live.join("versions").exists());
		Ok(())
	}

	#[test]
	fn staged_native_lock_is_retired_only_after_actual_lock_owner_releases() -> io::Result<()> {
		let (_root, live, generation) = fixture()?;
		let lock_path = generation.paths.control.join(STAGED_LOCK);
		write_marker(&lock_path, b"")?;
		let held = OpenOptions::new().read(true).write(true).open(&lock_path)?;
		held.try_lock().map_err(io::Error::other)?;
		assert!(generation.promote(deadline(), |_| Ok(())).is_err());
		assert!(lock_path.exists());
		assert!(live.join("old-payload").exists());
		drop(held);
		recover(&live, |_| Ok(()))?;
		assert!(!lock_path.exists());
		assert!(!Paths::new(&live)?.staged.exists());
		let again = Generation::prepare(&live)?;
		write_marker(&again.paths.control.join(STAGED_LOCK), b"")?;
		fs::write(again.staged_path().join("retained"), b"verified-native")?;
		again.promote(deadline(), |_| Ok(()))?;
		assert_eq!(fs::read(live.join("retained"))?, b"verified-native");
		assert!(!lock_path.exists());
		Ok(())
	}

	#[test]
	fn staged_native_lock_rejects_payload_links_and_nonempty_foreign_files() -> io::Result<()> {
		for alias in [false, true] {
			let (root, live, generation) = fixture()?;
			let foreign = root.path().join("foreign-lock");
			fs::write(&foreign, b"preserve-owner-data")?;
			let lock_path = generation.paths.control.join(STAGED_LOCK);
			if alias {
				symlink(&foreign, &lock_path)?;
			} else {
				fs::write(&lock_path, b"not-an-empty-native-lock")?;
			}
			assert!(recover(&live, |_| Ok(())).is_err());
			assert!(lock_path.symlink_metadata().is_ok());
			assert_eq!(fs::read(foreign)?, b"preserve-owner-data");
			assert!(generation.staged_path().join("versions/retained").exists());
		}
		Ok(())
	}
}
