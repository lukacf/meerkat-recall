//! Linux descriptor-relative, no-follow private exchange. No unsafe code.
use super::{refusal, Result, MAX_REQUEST_BYTES, MAX_SELECTIONS};
use rustix::fs::{openat, renameat_with, Dir, Mode, OFlags, RenameFlags, CWD};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};

fn failure(_: impl std::fmt::Display) -> super::EvidenceError {
    refusal("exchange_io_failed")
}
fn directory_at(parent: &File, name: &std::ffi::OsStr) -> Result<File> {
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(failure)
}
fn open_absolute_directory(path: &Path) -> Result<File> {
    if !path.is_absolute() {
        return Err(refusal("exchange_path_invalid"));
    }
    let mut directory = File::from(
        openat(
            CWD,
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(failure)?,
    );
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => directory = directory_at(&directory, name)?,
            _ => return Err(refusal("exchange_path_invalid")),
        }
    }
    private_directory(&directory)?;
    Ok(directory)
}
fn private_directory(directory: &File) -> Result<()> {
    let metadata = directory.metadata().map_err(failure)?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(refusal("exchange_directory_not_private"));
    }
    Ok(())
}
fn read_at(directory: &File, name: &std::ffi::OsStr, maximum: usize) -> Result<Vec<u8>> {
    let mut file = File::from(
        openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(failure)?,
    );
    let before = file.metadata().map_err(failure)?;
    if !before.is_file()
        || before.uid() != rustix::process::getuid().as_raw()
        || before.mode() & 0o077 != 0
        || before.nlink() != 1
        || before.len() > maximum as u64
    {
        return Err(refusal("exchange_file_custody_invalid"));
    }
    let mut raw = Vec::new();
    (&mut file)
        .take((maximum + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(failure)?;
    let after = file.metadata().map_err(failure)?;
    if raw.len() > maximum
        || raw.len() as u64 != before.len()
        || before.len() != after.len()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || after.nlink() != 1
        || after.mode() != before.mode()
    {
        return Err(refusal("exchange_file_changed"));
    }
    Ok(raw)
}
pub(super) fn read_capability(path: &Path) -> Result<Vec<u8>> {
    let parent = path
        .parent()
        .ok_or_else(|| refusal("capability_path_invalid"))?;
    let name = path
        .file_name()
        .ok_or_else(|| refusal("capability_path_invalid"))?;
    read_at(&open_absolute_directory(parent)?, name, MAX_REQUEST_BYTES)
}
fn names(directory: &File, maximum: usize) -> Result<Vec<String>> {
    private_directory(directory)?;
    let mut output = Vec::new();
    for entry in Dir::read_from(directory).map_err(failure)? {
        let entry = entry.map_err(failure)?;
        let name = entry.file_name().to_str().map_err(failure)?;
        if name == "." || name == ".." {
            continue;
        }
        if output.len() == maximum {
            return Err(refusal("exchange_entry_limit"));
        }
        output.push(name.to_owned());
    }
    output.sort();
    Ok(output)
}

pub(super) struct Exchange {
    _root: File,
    requests: File,
    responses: File,
}
impl Exchange {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let root = open_absolute_directory(path)?;
        let requests = directory_at(&root, std::ffi::OsStr::new("requests"))?;
        let responses = directory_at(&root, std::ffi::OsStr::new("responses"))?;
        if !names(&requests, MAX_SELECTIONS)?.is_empty() || !names(&responses, 1)?.is_empty() {
            return Err(refusal("exchange_already_used"));
        }
        Ok(Self {
            _root: root,
            requests,
            responses,
        })
    }
    pub(super) fn next(&self, seen: &BTreeSet<String>) -> Result<Option<(String, Vec<u8>)>> {
        for name in names(&self.requests, MAX_SELECTIONS * 2)? {
            // The collector reserves each original name with one persistent
            // hidden claim directory. Never open or traverse these entries.
            if name
                .strip_prefix('.')
                .and_then(|name| name.strip_suffix(".json.claim"))
                .is_some_and(super::valid_hash)
            {
                continue;
            }
            let id = name
                .strip_suffix(".json")
                .filter(|id| super::valid_hash(id))
                .ok_or_else(|| refusal("exchange_request_name_invalid"))?;
            if !seen.contains(id) {
                return Ok(Some((
                    id.to_owned(),
                    read_at(
                        &self.requests,
                        std::ffi::OsStr::new(&name),
                        MAX_REQUEST_BYTES,
                    )?,
                )));
            }
        }
        Ok(None)
    }
    pub(super) fn publish(&self, name: &str, raw: &[u8]) -> Result<()> {
        #[allow(
            clippy::case_sensitive_file_extension_comparisons,
            reason = "The private evidence protocol accepts only exact lowercase .json names."
        )]
        if raw.len() > super::MAX_PAGE_BYTES || name.contains('/') || !name.ends_with(".json") {
            return Err(refusal("exchange_output_invalid"));
        }
        private_directory(&self.responses)?;
        let temporary = format!(".writing-{name}");
        let mut file = File::from(
            openat(
                &self.responses,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(failure)?,
        );
        file.write_all(raw).map_err(failure)?;
        file.sync_all().map_err(failure)?;
        // renameat2 NOREPLACE makes both the pages and final reply immutable.
        // All page files are synced before the reply becomes visible.
        renameat_with(
            &self.responses,
            temporary.as_str(),
            &self.responses,
            name,
            RenameFlags::NOREPLACE,
        )
        .map_err(failure)?;
        self.responses.sync_all().map_err(failure)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    fn fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["requests", "responses"] {
            let path = dir.path().join(name);
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        dir
    }
    #[test]
    fn descriptor_exchange_refuses_symlink_and_traversal() {
        let dir = fixture();
        symlink(dir.path().join("requests"), dir.path().join("alias")).unwrap();
        assert!(open_absolute_directory(&dir.path().join("alias")).is_err());
        assert!(open_absolute_directory(&dir.path().join("requests/../responses")).is_err());
    }
    #[test]
    fn immutable_publish_never_replaces_original_and_restart_refuses_reuse() {
        let dir = fixture();
        let exchange = Exchange::open(dir.path()).unwrap();
        exchange.publish("original.json", b"original").unwrap();
        assert!(exchange.publish("original.json", b"replacement").is_err());
        assert_eq!(
            std::fs::read(dir.path().join("responses/original.json")).unwrap(),
            b"original"
        );
        assert!(Exchange::open(dir.path()).is_err());
    }
    #[test]
    fn bounded_read_refuses_hardlink_nonprivate_file_and_oversize() {
        let dir = fixture();
        let p = dir.path().join("capability.json");
        std::fs::write(&p, b"1234").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        let parent = open_absolute_directory(dir.path()).unwrap();
        assert!(read_at(&parent, std::ffi::OsStr::new("capability.json"), 3).is_err());
        std::fs::hard_link(&p, dir.path().join("copy")).unwrap();
        assert!(read_capability(&p).is_err());
        std::fs::remove_file(dir.path().join("copy")).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_capability(&p).is_err());
    }
}
