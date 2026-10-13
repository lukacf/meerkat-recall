//! Lifetime locks for local persistent stores, shared by all storage adapters.
//!
//! A stable sibling file survives data-store replacement. Never unlink it:
//! unlinking a locked inode permits a second writer to lock a different inode.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::initialization_diagnostic::{InitializationPhase, StorageInitializationError};
use crate::{StorageBackendKind, StorageConfig, StorageError, StorageResult};

/// Exclusive operating-system lock held until the last owning client drops.
#[derive(Debug)]
pub struct StoreWriterLock {
    _file: File,
    path: PathBuf,
}

impl StoreWriterLock {
    /// Acquire a nonblocking exclusive lock before opening a persistent store.
    pub fn acquire(store_path: &Path) -> StorageResult<Self> {
        Self::acquire_diagnosed(store_path, InitializationPhase::WriterLock)
            .map_err(StorageInitializationError::into_storage_error)
    }

    pub(crate) fn acquire_diagnosed(
        store_path: &Path,
        phase: InitializationPhase,
    ) -> Result<Self, StorageInitializationError> {
        let path = lock_path(store_path, phase)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| failure(phase, "writer_file_open", &path, &error))?;
        file.try_lock().map_err(|error| {
            let failure = StorageError::ConnectionError(format!(
                "exclusive store ownership unavailable at {}: {error}",
                path.display()
            ));
            match &error {
                std::fs::TryLockError::Error(cause) => StorageInitializationError::from_io(
                    phase,
                    failure,
                    cause,
                    "writer_lock_acquire",
                ),
                std::fs::TryLockError::WouldBlock => {
                    StorageInitializationError::lock_would_block(phase, failure)
                }
            }
        })?;
        Ok(Self { _file: file, path })
    }

    /// Stable ownership file for diagnostics. It contains no stored payload.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Resolve local persistence without assigning filesystem locks to remote stores.
pub(crate) fn configured_store_path(config: &StorageConfig) -> Option<PathBuf> {
    if config.backend == StorageBackendKind::ElephantDb {
        return config.snapshot_load.clone();
    }
    let (scheme, remainder) = config.url.split_once(':')?;
    if !matches!(scheme, "rocksdb" | "surrealkv" | "file") {
        return None;
    }
    Some(PathBuf::from(
        remainder.strip_prefix("//").unwrap_or(remainder),
    ))
}

fn lock_path(
    store_path: &Path,
    phase: InitializationPhase,
) -> Result<PathBuf, StorageInitializationError> {
    if store_path.as_os_str().is_empty() {
        return Err(StorageInitializationError::new(
            phase,
            StorageError::ConnectionError("persistent store path is empty".into()),
        ));
    }
    // Canonicalize an existing target, or its parent for a new store, so path
    // aliases and symlinked parents cannot independently claim one store.
    let absolute = if store_path.exists() {
        store_path
            .canonicalize()
            .map_err(|error| failure(phase, "writer_path_canonicalize", store_path, &error))?
    } else {
        let parent = store_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|error| failure(phase, "writer_parent_create", parent, &error))?;
        let parent = parent
            .canonicalize()
            .map_err(|error| failure(phase, "writer_parent_canonicalize", parent, &error))?;
        let name = store_path.file_name().ok_or_else(|| {
            StorageInitializationError::new(
                phase,
                StorageError::ConnectionError("persistent store needs a file name".into()),
            )
        })?;
        parent.join(name)
    };
    let name = absolute.file_name().ok_or_else(|| {
        StorageInitializationError::new(
            phase,
            StorageError::ConnectionError("persistent store cannot be a filesystem root".into()),
        )
    })?;
    let mut lock_name = name.to_os_string();
    lock_name.push(".writer.lock");
    Ok(absolute.with_file_name(lock_name))
}

fn failure(
    phase: InitializationPhase,
    operation: &'static str,
    path: &Path,
    error: &std::io::Error,
) -> StorageInitializationError {
    StorageInitializationError::from_io(
        phase,
        StorageError::ConnectionError(format!("store ownership file {}: {error}", path.display())),
        error,
        operation,
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn second_writer_is_refused_until_last_owner_releases() {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("data/store");
        let first = std::sync::Arc::new(StoreWriterLock::acquire(&store).unwrap());
        let clone = first.clone();
        assert!(StoreWriterLock::acquire(&store).is_err());
        drop(first);
        assert!(StoreWriterLock::acquire(&store).is_err());
        drop(clone);
        assert!(StoreWriterLock::acquire(&store).is_ok());
    }

    #[test]
    fn store_replacement_keeps_the_same_lock_inode() {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("store");
        std::fs::create_dir(&store).unwrap();
        let lock = StoreWriterLock::acquire(&store).unwrap();
        std::fs::rename(&store, directory.path().join("old-store")).unwrap();
        std::fs::create_dir(&store).unwrap();
        assert!(lock.path().exists());
        assert!(StoreWriterLock::acquire(&store).is_err());
        drop(lock);
        assert!(StoreWriterLock::acquire(&store).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn parent_alias_cannot_take_second_ownership() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("real")).unwrap();
        std::os::unix::fs::symlink(
            directory.path().join("real"),
            directory.path().join("alias"),
        )
        .unwrap();
        let _first = StoreWriterLock::acquire(&directory.path().join("real/store")).unwrap();
        assert!(StoreWriterLock::acquire(&directory.path().join("alias/store")).is_err());
    }

    #[tokio::test]
    async fn escaped_embedded_store_retains_writer_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("state.snapshot");
        let config = StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        };
        let client = crate::StorageClient::new_with_snapshot_destination(
            config.clone(),
            Some(destination.clone()),
        )
        .await
        .unwrap();
        let escaped = client.embedded_store().unwrap();
        drop(client);
        assert!(crate::StorageClient::new_with_snapshot_destination(
            config.clone(),
            Some(destination.clone())
        )
        .await
        .is_err());
        drop(escaped);
        assert!(
            crate::StorageClient::new_with_snapshot_destination(config, Some(destination))
                .await
                .is_ok()
        );
    }

    #[test]
    fn remote_and_memory_stores_do_not_claim_local_files() {
        for url in ["mem://", "ws://localhost:8000", "https://database.example"] {
            let config = StorageConfig {
                url: url.into(),
                ..StorageConfig::default()
            };
            assert_eq!(configured_store_path(&config), None);
        }
        let config = StorageConfig {
            url: "rocksdb:///var/lib/recall/store".into(),
            ..StorageConfig::default()
        };
        assert_eq!(
            configured_store_path(&config),
            Some(PathBuf::from("/var/lib/recall/store"))
        );
    }
}
