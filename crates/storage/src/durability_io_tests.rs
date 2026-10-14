#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::configuration_write_test_support::{inject_fault, Fault};
use super::*;

fn test_directory() -> tempfile::TempDir {
    let root = fs::canonicalize(std::env::temp_dir()).expect("canonical test root");
    tempfile::tempdir_in(root).expect("temporary directory")
}

fn assert_no_temporary_files(parent: &Path) {
    assert!(fs::read_dir(parent)
        .expect("read directory")
        .all(|entry| !entry
            .expect("directory entry")
            .file_name()
            .to_string_lossy()
            .starts_with(".recall-write-")));
}

#[test]
fn configuration_failures_before_publication_preserve_the_old_bytes() {
    for fault in [Fault::WriteEnospc, Fault::BeforeRename] {
        let directory = test_directory();
        let path = directory.path().join("configuration.toml");
        let original = b"[runtime]\ningest_enabled = false\n";
        write_private_configuration(&path, original).expect("original publication");
        let _fault = inject_fault(&path, fault).expect("inject publication fault");

        let error = write_private_configuration(&path, b"[runtime]\ningest_enabled = true\n")
            .expect_err("injected failure");
        assert!(matches!(error, ConfigWriteError::BeforePublication(_)));
        if let ConfigWriteError::BeforePublication(error) = error {
            assert_eq!(
                error.raw_os_error(),
                Some(if fault == Fault::WriteEnospc { 28 } else { 5 })
            );
        }
        assert_eq!(fs::read(&path).expect("preserved configuration"), original);
        assert_no_temporary_files(directory.path());
    }
}

#[test]
fn directory_sync_failure_reports_replacement_without_claiming_rollback() {
    let directory = test_directory();
    let path = directory.path().join("configuration.toml");
    write_private_configuration(&path, b"old").expect("original publication");
    let _fault =
        inject_fault(&path, Fault::AfterRenameDirectorySync).expect("inject directory sync");

    let error = write_private_configuration(&path, b"new").expect_err("directory sync failure");
    assert!(matches!(error, ConfigWriteError::CommitUncertain(_)));
    assert!(error.to_string().contains("rename completed"));
    assert_eq!(fs::read(&path).expect("replaced destination"), b"new");
    assert_no_temporary_files(directory.path());
}

#[cfg(unix)]
#[test]
fn configuration_files_and_new_directories_are_private_without_chmod_existing_parents() {
    use std::os::unix::fs::PermissionsExt;

    let directory = test_directory();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o750))
        .expect("existing shared directory permissions");
    let path = directory.path().join("new/nested/configuration.toml");
    write_private_configuration(&path, b"private configuration").expect("publication");
    assert_eq!(
        fs::metadata(&path).expect("file").permissions().mode() & 0o777,
        0o600
    );
    for relative in ["new", "new/nested"] {
        assert_eq!(
            fs::metadata(directory.path().join(relative))
                .expect("new directory")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
    assert_eq!(
        fs::metadata(directory.path())
            .expect("shared directory")
            .permissions()
            .mode()
            & 0o777,
        0o750
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("old file mode");
    write_private_configuration(&path, b"replacement").expect("replacement publication");
    assert_eq!(
        fs::metadata(&path).expect("file").permissions().mode() & 0o777,
        0o600
    );
}

#[cfg(unix)]
#[test]
fn configuration_rejects_symlink_destination_and_parent_without_touching_the_target() {
    use std::os::unix::fs::symlink;

    let directory = test_directory();
    let actual = directory.path().join("actual");
    fs::create_dir(&actual).expect("actual directory");
    let target = actual.join("configuration.toml");
    fs::write(&target, b"original").expect("original target");
    let linked_file = directory.path().join("linked.toml");
    let linked_parent = directory.path().join("linked-parent");
    symlink(&target, &linked_file).expect("file symlink");
    symlink(&actual, &linked_parent).expect("directory symlink");

    for path in [
        linked_file.clone(),
        linked_parent.join("configuration.toml"),
    ] {
        assert!(matches!(
            write_private_configuration(&path, b"replacement"),
            Err(ConfigWriteError::BeforePublication(_))
        ));
        assert_eq!(fs::read(&target).expect("untouched target"), b"original");
    }
    assert!(fs::symlink_metadata(linked_file)
        .expect("retained symlink")
        .file_type()
        .is_symlink());
    assert_no_temporary_files(directory.path());
    assert_no_temporary_files(&actual);
}

#[test]
fn configuration_rejects_nonfile_and_parent_traversal_destinations() {
    let directory = test_directory();
    fs::create_dir(directory.path().join("nested")).expect("nested directory");
    for path in [
        PathBuf::new(),
        directory.path().to_owned(),
        directory.path().join("nested/../configuration.toml"),
    ] {
        assert!(matches!(
            write_private_configuration(&path, b"configuration"),
            Err(ConfigWriteError::BeforePublication(_))
        ));
    }
    assert!(!directory.path().join("configuration.toml").exists());
    assert_no_temporary_files(directory.path());
}

#[test]
fn shared_writer_does_not_delete_a_temporary_file_it_did_not_create() {
    let directory = test_directory();
    let path = directory.path().join("checkpoint");
    let temporary = directory.path().join("reserved-temporary");
    fs::write(&path, b"old checkpoint").expect("original checkpoint");
    fs::write(&temporary, b"belongs to another writer").expect("reserved file");

    assert!(atomic_write_at(&path, b"new checkpoint", &temporary).is_err());
    assert_eq!(fs::read(&path).expect("checkpoint"), b"old checkpoint");
    assert_eq!(
        fs::read(&temporary).expect("reserved file"),
        b"belongs to another writer"
    );
}

#[test]
fn shared_writer_keeps_existing_cross_directory_temporary_path_support() {
    let directory = test_directory();
    let temporary = directory.path().join("temporary");
    let path = directory.path().join("checkpoint/state");
    atomic_write_at(&path, b"checkpoint bytes", &temporary).expect("same filesystem publication");
    assert_eq!(fs::read(path).expect("checkpoint"), b"checkpoint bytes");
    assert!(!temporary.exists());
}

#[test]
fn fault_guard_is_one_shot_and_scoped_to_the_exact_configuration_path() {
    let directory = test_directory();
    let path = directory.path().join("configuration.toml");
    let other = directory.path().join("other.toml");
    let fault = inject_fault(&path, Fault::BeforeRename).expect("inject fault");
    assert!(inject_fault(&path, Fault::WriteEnospc).is_err());
    write_private_configuration(&other, b"other config").expect("unrelated configuration");
    // Storage writers never consume configuration-only injections.
    atomic_write(&path, b"storage write").expect("unrelated storage writer");
    assert!(matches!(
        write_private_configuration(&path, b"new config"),
        Err(ConfigWriteError::BeforePublication(_))
    ));
    write_private_configuration(&path, b"retry succeeds").expect("one-shot fault consumed");
    // A consumed guard must not remove a later injection for the same path.
    let next_fault = inject_fault(&path, Fault::BeforeRename).expect("second injection");
    drop(fault);
    assert!(write_private_configuration(&path, b"still fails").is_err());
    drop(next_fault);
    let unused_fault = inject_fault(&path, Fault::WriteEnospc).expect("unused injection");
    drop(unused_fault);
    write_private_configuration(&path, b"guard cleanup").expect("dropped fault removed");
}
