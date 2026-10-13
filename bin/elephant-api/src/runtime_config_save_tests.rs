#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::{load_config_from_path, save_config, ElephantConfig};
use elephant_storage::configuration_write_test_support::{inject_fault, Fault};
use elephant_storage::ConfigWriteError;
use std::fs;
use std::path::PathBuf;

struct ConfigDirectory(PathBuf);

impl ConfigDirectory {
    fn new() -> Self {
        Self(
            fs::canonicalize(std::env::temp_dir())
                .expect("canonical test root")
                .join(format!("recall-config-save-{}", uuid::Uuid::new_v4())),
        )
    }

    fn path(&self) -> PathBuf {
        self.0.join(".elephant/elephant_config.toml")
    }
}

impl Drop for ConfigDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn failed_config_publication_preserves_parseable_previous_configuration() {
    for fault in [Fault::WriteEnospc, Fault::BeforeRename] {
        let directory = ConfigDirectory::new();
        let path = directory.path();
        let mut original = ElephantConfig::default();
        original.runtime.ingest_enabled = false;
        save_config(&path, &original).expect("persist original configuration");
        let bytes = fs::read(&path).expect("original bytes");
        let mut proposed = original.clone();
        proposed.runtime.ingest_enabled = true;
        let _fault = inject_fault(&path, fault).expect("inject publication failure");

        let error = save_config(&path, &proposed).expect_err("save must fail");
        assert!(matches!(
            error.downcast_ref::<ConfigWriteError>(),
            Some(ConfigWriteError::BeforePublication(_))
        ));
        assert_eq!(fs::read(&path).expect("old configuration bytes"), bytes);
        let loaded = load_config_from_path(&path).expect("old configuration still parses");
        assert!(!loaded.runtime.ingest_enabled);
        assert_eq!(
            toml::to_string(&loaded).expect("loaded TOML"),
            toml::to_string(&original).expect("original TOML")
        );
    }
}

#[test]
fn renamed_config_with_failed_directory_sync_retains_typed_uncertain_outcome() {
    let directory = ConfigDirectory::new();
    let path = directory.path();
    let mut original = ElephantConfig::default();
    original.runtime.ingest_enabled = false;
    save_config(&path, &original).expect("persist original configuration");
    let mut proposed = original;
    proposed.runtime.ingest_enabled = true;
    let _fault = inject_fault(&path, Fault::AfterRenameDirectorySync).expect("inject sync failure");

    let error = save_config(&path, &proposed).expect_err("durability must be uncertain");
    assert!(matches!(
        error.downcast_ref::<ConfigWriteError>(),
        Some(ConfigWriteError::CommitUncertain(_))
    ));
    assert!(format!("{error:#}").contains("rename completed"));
    let loaded = load_config_from_path(&path).expect("new visible configuration parses");
    assert!(loaded.runtime.ingest_enabled);
    assert_eq!(
        toml::to_string(&loaded).expect("loaded TOML"),
        toml::to_string(&proposed).expect("proposed TOML")
    );
}

#[test]
fn failed_first_config_write_does_not_publish_partial_initialization() {
    let directory = ConfigDirectory::new();
    let path = directory.path();
    let _fault = inject_fault(&path, Fault::WriteEnospc).expect("inject partial write");

    let error = save_config(&path, &ElephantConfig::default()).expect_err("save failure");
    assert!(matches!(
        error.downcast_ref::<ConfigWriteError>(),
        Some(ConfigWriteError::BeforePublication(_))
    ));
    assert!(!path.exists());
    assert_eq!(
        fs::read_dir(path.parent().expect("parent"))
            .expect("config directory")
            .count(),
        0
    );
}
