//! Durable local metadata publication. Temporary files never replace good data until synced.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use crate::{StorageError, StorageResult};

pub fn io_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::DataIntegrityError(format!("durability I/O: {error}"))
}

pub fn sync_directory(path: &Path) -> StorageResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}

pub fn private_directory(path: &Path) -> StorageResult<()> {
    fs::create_dir_all(path).map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error)?;
    }
    Ok(())
}

/// Same-directory write/fsync/rename/fsync. A failure before rename preserves the old file.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(io_error)?;
    let temporary = parent.join(format!(".recall-write-{}", uuid::Uuid::new_v4()));
    atomic_write_at(path, bytes, &temporary)
}

pub fn atomic_write_at(path: &Path, bytes: &[u8], temporary: &Path) -> StorageResult<()> {
    atomic_write_classified(path, bytes, temporary, WriteKind::Storage).map_err(io_error)
}

/// Whether a failed configuration save crossed the file replacement boundary.
#[derive(Debug, thiserror::Error)]
pub enum ConfigWriteError {
    /// The destination was not replaced. An existing configuration remains intact.
    #[error("file publication failed before rename: {0}")]
    BeforePublication(#[source] io::Error),
    /// The destination was replaced, but the directory sync did not complete.
    ///
    /// Callers must not claim the old configuration was preserved or continue
    /// serving with a conflicting in-memory configuration. Reopening is required.
    #[error(
        "file rename completed but directory synchronization failed; durability is uncertain: {0}"
    )]
    CommitUncertain(#[source] io::Error),
}

/// Durably replace a private runtime configuration using the storage file writer.
///
/// The same-directory temporary file is created exclusively with mode `0600` on
/// Unix, written, flushed, and synced before rename. The containing directory is
/// then synced. Missing parent directories are created privately and synced;
/// existing directory permissions are never changed. Symlink components,
/// non-regular destinations, and parent traversal are rejected.
///
/// The caller must control the configuration directory and serialize its writers.
/// Path checks do not defend against another actor replacing directory entries
/// concurrently. An error after rename is explicitly [`ConfigWriteError::CommitUncertain`].
pub fn write_private_configuration(path: &Path, bytes: &[u8]) -> Result<(), ConfigWriteError> {
    let path = configuration_path(path).map_err(ConfigWriteError::BeforePublication)?;
    prepare_configuration_destination(&path, true).map_err(ConfigWriteError::BeforePublication)?;
    let parent = path.parent().ok_or_else(|| {
        ConfigWriteError::BeforePublication(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration needs a containing directory",
        ))
    })?;
    let temporary = parent.join(format!(".recall-write-{}", uuid::Uuid::new_v4()));
    atomic_write_classified(&path, bytes, &temporary, WriteKind::Configuration)
}

#[derive(Clone, Copy)]
enum WriteKind {
    Storage,
    Configuration,
}

fn atomic_write_classified(
    path: &Path,
    bytes: &[u8],
    temporary: &Path,
    kind: WriteKind,
) -> Result<(), ConfigWriteError> {
    use ConfigWriteError::{BeforePublication, CommitUncertain};

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    match kind {
        WriteKind::Storage => fs::create_dir_all(parent).map_err(BeforePublication)?,
        WriteKind::Configuration => {
            prepare_configuration_destination(path, false).map_err(BeforePublication)?;
        }
    }
    let mut temporary_created = false;
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(temporary).map_err(BeforePublication)?;
        temporary_created = true;
        #[cfg(any(test, feature = "test-utils"))]
        if matches!(kind, WriteKind::Configuration)
            && configuration_write_test_support::take_fault(
                path,
                configuration_write_test_support::Fault::WriteEnospc,
            )
            .map_err(BeforePublication)?
        {
            // A partial private temporary file exercises the production cleanup
            // path. This is a deterministic fault, not evidence of an OS-full disk.
            let partial = (bytes.len() / 2).max(1).min(bytes.len());
            file.write_all(&bytes[..partial])
                .map_err(BeforePublication)?;
            return Err(BeforePublication(io::Error::from_raw_os_error(28)));
        }
        file.write_all(bytes).map_err(BeforePublication)?;
        file.flush().map_err(BeforePublication)?;
        file.sync_all().map_err(BeforePublication)?;
        if matches!(kind, WriteKind::Configuration) {
            prepare_configuration_destination(path, false).map_err(BeforePublication)?;
            #[cfg(any(test, feature = "test-utils"))]
            if configuration_write_test_support::take_fault(
                path,
                configuration_write_test_support::Fault::BeforeRename,
            )
            .map_err(BeforePublication)?
            {
                return Err(BeforePublication(io::Error::from_raw_os_error(5)));
            }
        }
        fs::rename(temporary, path).map_err(BeforePublication)?;
        temporary_created = false;
        #[cfg(any(test, feature = "test-utils"))]
        if matches!(kind, WriteKind::Configuration)
            && configuration_write_test_support::take_fault(
                path,
                configuration_write_test_support::Fault::AfterRenameDirectorySync,
            )
            .map_err(CommitUncertain)?
        {
            return Err(CommitUncertain(io::Error::from_raw_os_error(5)));
        }
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(CommitUncertain)
    })();
    if result.is_err() && temporary_created {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn configuration_path(path: &Path) -> io::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration path is empty",
        ));
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "configuration path cannot contain parent traversal",
                ));
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    if normalized.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration path must name a file",
        ));
    }
    Ok(normalized)
}

fn prepare_configuration_destination(path: &Path, create_parents: bool) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration parent is missing",
        )
    })?;
    for directory in parent.ancestors().collect::<Vec<_>>().into_iter().rev() {
        match fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                if create_parents {
                    // A previous failed first save may have created this
                    // directory but failed to sync its ancestor. Sync the
                    // checked ancestry on retries as well as on creation.
                    File::open(directory)?.sync_all()?;
                }
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "configuration parent must be a directory without symlinks",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && create_parents => {
                let mut builder = fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder.create(directory)?;
                File::open(directory)?.sync_all()?;
                if let Some(ancestor) = directory.parent() {
                    File::open(ancestor)?.sync_all()?;
                }
            }
            Err(error) => return Err(error),
        }
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration destination must be a regular file without symlinks",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Deterministic, path-scoped configuration publication faults for source tests.
/// These are not evidence of an installed binary encountering a full filesystem.
#[cfg(any(test, feature = "test-utils"))]
pub mod configuration_write_test_support {
    use std::collections::btree_map::Entry;
    use std::collections::BTreeMap;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    /// The production publication boundary at which to inject one I/O failure.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Fault {
        /// Write part of the private temporary file, then return OS error 28.
        WriteEnospc,
        /// Return OS error 5 after the file sync, before replacement.
        BeforeRename,
        /// Return OS error 5 after replacement, before directory synchronization.
        AfterRenameDirectorySync,
    }

    type Faults = BTreeMap<PathBuf, (uuid::Uuid, Fault)>;

    fn faults() -> &'static Mutex<Faults> {
        static FAULTS: OnceLock<Mutex<Faults>> = OnceLock::new();
        FAULTS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    /// Removes an unconsumed injection on drop, including across async awaits.
    #[must_use]
    pub struct FaultGuard {
        path: PathBuf,
        token: uuid::Uuid,
    }

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            if let Ok(mut active) = faults().lock() {
                if active
                    .get(&self.path)
                    .is_some_and(|(token, _)| *token == self.token)
                {
                    active.remove(&self.path);
                }
            }
        }
    }

    /// Inject one fault for exactly this configuration path.
    ///
    /// Other paths and ordinary storage publication are unaffected. A second active injection
    /// for the same path is rejected instead of replacing another test's fault.
    pub fn inject_fault(path: &Path, fault: Fault) -> io::Result<FaultGuard> {
        let path = super::configuration_path(path)?;
        let token = uuid::Uuid::new_v4();
        let mut active = faults()
            .lock()
            .map_err(|_| io::Error::other("configuration fault registry is poisoned"))?;
        match active.entry(path.clone()) {
            Entry::Vacant(entry) => {
                entry.insert((token, fault));
                Ok(FaultGuard { path, token })
            }
            Entry::Occupied(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a fault is already registered for this configuration",
            )),
        }
    }

    pub(super) fn take_fault(path: &Path, expected: Fault) -> io::Result<bool> {
        let mut active = faults()
            .lock()
            .map_err(|_| io::Error::other("configuration fault registry is poisoned"))?;
        let matched = active
            .get(path)
            .is_some_and(|(_, fault)| *fault == expected);
        if matched {
            active.remove(path);
        }
        drop(active);
        Ok(matched)
    }
}

#[cfg(test)]
#[path = "durability_io_tests.rs"]
mod tests;
