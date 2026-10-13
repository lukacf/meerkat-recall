//! Extension manifest parsing and validation for Elephant.
//!
//! This crate provides:
//! - Extension manifest parsing
//! - Ref grammar validation
//! - Merge reference rewriting rules

pub mod error;
pub mod manifest;
pub mod refs;

pub use error::{ExtensionError, ExtensionResult};
pub use manifest::{
    enabled_extension_names, load_extension_manifest, load_reference_registry,
    load_runtime_reference_registry_from_env, ExtensionManifest,
};

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
    fn test_extension_error_display() {
        let err = ExtensionError::InvalidManifest("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
