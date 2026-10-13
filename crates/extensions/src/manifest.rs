//! Extension manifest parsing.

use std::collections::HashSet;
use std::path::Path;

use elephant_identity::{
    OnMergeBehavior, ReferenceCardinality, ReferenceDeclaration, ReferenceRegistry,
};
use serde::{Deserialize, Serialize};

use crate::error::{ExtensionError, ExtensionResult};
use crate::refs::{is_safe_identifier, Cardinality, OnMerge, RefSpec};

/// An extension manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionManifest {
    /// Extension name (slug).
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Version.
    #[serde(default)]
    pub version: Option<String>,
    /// Extension-owned tables that should be considered during merge safety
    /// checks even when they currently have no rewrite declarations.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Reference specifications for merge rewriting.
    #[serde(default)]
    pub refs: Vec<RefSpec>,
}

impl ExtensionManifest {
    /// Validates the manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or malformed.
    pub fn validate(&self) -> ExtensionResult<()> {
        if self.name.is_empty() {
            return Err(ExtensionError::InvalidManifest(
                "name is required".to_string(),
            ));
        }

        // Validate name is a valid slug
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(ExtensionError::InvalidManifest(
                "name must be alphanumeric with underscores".to_string(),
            ));
        }

        for table in &self.tables {
            if !is_safe_identifier(table) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "table '{table}' must use only ASCII letters, digits, and underscores",
                )));
            }
            if !table.starts_with(&format!("{}_", self.name)) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "table '{table}' must be prefixed with '{}_'",
                    self.name
                )));
            }
        }

        // Validate all ref specs
        for ref_spec in &self.refs {
            ref_spec.validate(&self.name)?;
        }

        Ok(())
    }
}

/// Parses a manifest from JSON.
///
/// # Errors
///
/// Returns an error if the JSON is invalid or the manifest fails validation.
pub fn parse_manifest(json: &str) -> ExtensionResult<ExtensionManifest> {
    let manifest: ExtensionManifest =
        serde_json::from_str(json).map_err(|e| ExtensionError::InvalidManifest(e.to_string()))?;
    manifest.validate()?;
    Ok(manifest)
}

/// Returns the normalized list of enabled extension names from an env-style
/// comma-separated string.
#[must_use]
pub fn enabled_extension_names(value: Option<&str>) -> Vec<String> {
    let mut seen = HashSet::new();
    value
        .into_iter()
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|item| {
            let name = item.to_string();
            if seen.insert(name.clone()) {
                Some(name)
            } else {
                None
            }
        })
        .collect()
}

/// Loads a validated manifest from disk.
///
/// # Errors
///
/// Returns an error when the manifest file is missing, unreadable, or invalid.
pub fn load_extension_manifest(path: impl AsRef<Path>) -> ExtensionResult<ExtensionManifest> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ExtensionError::NotFound(format!("{} ({e})", path.display())))?;
    parse_manifest(&raw)
}

/// Loads a runtime reference registry from the enabled extension manifests.
///
/// # Errors
///
/// Returns an error if any referenced manifest is missing or invalid.
pub fn load_reference_registry(
    extension_names: &[String],
    manifest_dir: impl AsRef<Path>,
) -> ExtensionResult<ReferenceRegistry> {
    let manifest_dir = manifest_dir.as_ref();
    let mut registry = ReferenceRegistry::new();

    for extension_name in extension_names {
        let path = manifest_dir.join(format!("{extension_name}.json"));
        let manifest = load_extension_manifest(&path)?;
        if manifest.name != *extension_name {
            return Err(ExtensionError::InvalidManifest(format!(
                "manifest '{}' declares name '{}' but was loaded for extension '{}'",
                path.display(),
                manifest.name,
                extension_name
            )));
        }
        for table in &manifest.tables {
            registry.register_table(table.clone());
        }
        for ref_spec in manifest.refs {
            let cardinality = match ref_spec.cardinality {
                Cardinality::One => ReferenceCardinality::One,
                Cardinality::Many => ReferenceCardinality::Many,
            };
            let on_merge = match ref_spec.on_merge {
                OnMerge::Rewrite => OnMergeBehavior::Rewrite,
                OnMerge::Canonicalize => OnMergeBehavior::Canonicalize,
            };
            let mut decl = ReferenceDeclaration::new(ref_spec.table, ref_spec.field_path)
                .with_on_merge(on_merge);
            if matches!(cardinality, ReferenceCardinality::Many) {
                decl = decl.with_many();
            }
            if ref_spec.required {
                decl = decl.required();
            }
            registry.register(decl);
        }
    }

    Ok(registry)
}

/// Loads the runtime reference registry from Elephant's environment variables.
///
/// Reads:
/// - `ELEPHANT_EXTENSIONS`
/// - `ELEPHANT_EXTENSION_MANIFEST_DIR`
///
/// # Errors
///
/// Returns an error if any enabled extension manifest is missing or invalid.
pub fn load_runtime_reference_registry_from_env() -> ExtensionResult<ReferenceRegistry> {
    let names = enabled_extension_names(std::env::var("ELEPHANT_EXTENSIONS").ok().as_deref());
    if names.is_empty() {
        return Ok(ReferenceRegistry::new());
    }

    let manifest_dir = runtime_manifest_dir()?;
    load_reference_registry(&names, manifest_dir)
}

fn runtime_manifest_dir() -> ExtensionResult<String> {
    if let Ok(path) = std::env::var("ELEPHANT_EXTENSION_MANIFEST_DIR") {
        return Ok(path);
    }

    if let Ok(repo_root) = std::env::var("REPO_CARGO_REPO_ROOT") {
        let candidate = std::path::Path::new(&repo_root).join("extensions");
        if candidate.is_dir() {
            return Ok(candidate.to_string_lossy().into_owned());
        }
    }

    Err(ExtensionError::InvalidManifest(
        "ELEPHANT_EXTENSIONS is set but no manifests directory is configured; set ELEPHANT_EXTENSION_MANIFEST_DIR or run via repo tooling that exports REPO_CARGO_REPO_ROOT"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_parse_manifest() {
        let json = r#"{
            "name": "sampleapp",
            "description": "Sample app extension",
            "tables": ["sampleapp_todo"],
            "refs": [
                {
                    "table": "sampleapp_todo",
                    "field_path": "related_to",
                    "cardinality": "many",
                    "on_merge": "rewrite"
                }
            ]
        }"#;

        let manifest = parse_manifest(json).ok();
        assert!(manifest.is_some());
        if let Some(manifest) = manifest {
            assert_eq!(manifest.name, "sampleapp");
            assert_eq!(manifest.tables, vec!["sampleapp_todo".to_string()]);
            assert_eq!(manifest.refs.len(), 1);
        }
    }

    #[test]
    fn test_invalid_name() {
        let json = r#"{"name": "invalid-name"}"#;
        assert!(parse_manifest(json).is_err());
    }

    #[test]
    fn test_enabled_extension_names_splits_and_dedupes() {
        assert_eq!(
            enabled_extension_names(Some("sampleapp, sampleapp, goldfish, , ")),
            vec!["sampleapp".to_string(), "goldfish".to_string()]
        );
    }

    #[test]
    fn test_load_reference_registry_builds_declarations() {
        let temp_dir = std::env::temp_dir().join(format!(
            "elephant-extension-manifest-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("sampleapp.json");
        std::fs::write(
            &path,
            r#"{
                "name": "sampleapp",
                "tables": ["sampleapp_misc"],
                "refs": [
                    {
                        "table": "sampleapp_todo",
                        "field_path": "owner",
                        "cardinality": "one",
                        "on_merge": "rewrite"
                    },
                    {
                        "table": "sampleapp_todo",
                        "field_path": "secondary",
                        "cardinality": "many",
                        "on_merge": "rewrite"
                    }
                ]
            }"#,
        )
        .expect("write manifest");

        let registry =
            load_reference_registry(&["sampleapp".to_string()], &temp_dir).expect("load registry");
        let refs = registry.get_table_refs("sampleapp_todo");
        assert_eq!(refs.len(), 2);
        assert!(refs.iter().any(|decl| decl.field_path == "owner"));
        assert!(refs.iter().any(|decl| decl.field_path == "secondary"));
        assert!(registry.owns_table("sampleapp_misc"));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_invalid_table_prefix_in_manifest_tables() {
        let json = r#"{
            "name": "sampleapp",
            "tables": ["todo"]
        }"#;
        assert!(parse_manifest(json).is_err());
    }

    #[test]
    fn test_invalid_table_identifier_in_manifest_tables() {
        let json = r#"{
            "name": "sampleapp",
            "tables": ["sampleapp-bad"]
        }"#;
        assert!(parse_manifest(json).is_err());
    }

    #[test]
    fn test_load_reference_registry_rejects_manifest_name_mismatch() {
        let temp_dir = std::env::temp_dir().join(format!(
            "elephant-extension-manifest-mismatch-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("sampleapp.json");
        std::fs::write(
            &path,
            r#"{
                "name": "goldfish",
                "tables": ["goldfish_misc"]
            }"#,
        )
        .expect("write manifest");

        let error = load_reference_registry(&["sampleapp".to_string()], &temp_dir)
            .expect_err("mismatched manifest name should fail");
        assert!(error
            .to_string()
            .contains("declares name 'goldfish' but was loaded for extension 'sampleapp'"));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_load_runtime_reference_registry_from_env() {
        let temp_dir = std::env::temp_dir().join(format!(
            "elephant-extension-runtime-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");
        let path = temp_dir.join("sampleapp.json");
        std::fs::write(
            &path,
            r#"{
                "name": "sampleapp",
                "refs": [
                    {
                        "table": "sampleapp_todo",
                        "field_path": "owner",
                        "cardinality": "one",
                        "on_merge": "rewrite"
                    }
                ]
            }"#,
        )
        .expect("write manifest");

        std::env::set_var("ELEPHANT_EXTENSIONS", "sampleapp");
        std::env::set_var("ELEPHANT_EXTENSION_MANIFEST_DIR", &temp_dir);
        let registry = load_runtime_reference_registry_from_env().expect("load registry from env");
        std::env::remove_var("ELEPHANT_EXTENSIONS");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");

        assert_eq!(registry.get_table_refs("sampleapp_todo").len(), 1);

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_load_runtime_reference_registry_requires_manifest_dir_or_repo_root() {
        std::env::set_var("ELEPHANT_EXTENSIONS", "sampleapp");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");
        std::env::remove_var("REPO_CARGO_REPO_ROOT");

        let error = load_runtime_reference_registry_from_env().expect_err("missing manifest dir");
        assert!(error
            .to_string()
            .contains("ELEPHANT_EXTENSION_MANIFEST_DIR"));

        std::env::remove_var("ELEPHANT_EXTENSIONS");
    }

    #[test]
    fn test_load_runtime_reference_registry_uses_repo_root_env() {
        let temp_dir = std::env::temp_dir().join(format!(
            "elephant-extension-runtime-repo-root-{}",
            uuid::Uuid::new_v4()
        ));
        let manifest_dir = temp_dir.join("extensions");
        std::fs::create_dir_all(&manifest_dir).expect("create extensions dir");
        let path = manifest_dir.join("sampleapp.json");
        std::fs::write(
            &path,
            r#"{
                "name": "sampleapp",
                "refs": [
                    {
                        "table": "sampleapp_todo",
                        "field_path": "owner",
                        "cardinality": "one",
                        "on_merge": "rewrite"
                    }
                ]
            }"#,
        )
        .expect("write manifest");

        std::env::set_var("ELEPHANT_EXTENSIONS", "sampleapp");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");
        std::env::set_var("REPO_CARGO_REPO_ROOT", &temp_dir);

        let registry =
            load_runtime_reference_registry_from_env().expect("load registry from repo root env");
        assert_eq!(registry.get_table_refs("sampleapp_todo").len(), 1);

        std::env::remove_var("ELEPHANT_EXTENSIONS");
        std::env::remove_var("REPO_CARGO_REPO_ROOT");
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
