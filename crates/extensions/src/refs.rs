//! Reference specification for merge rewriting.

use serde::{Deserialize, Serialize};

use crate::error::{ExtensionError, ExtensionResult};

/// Cardinality of a reference field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// Single reference.
    One,
    /// Array of references.
    Many,
}

/// Merge behavior for a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnMerge {
    /// Rewrite references during merge.
    #[default]
    Rewrite,
    /// Canonicalize on read (resolve redirects).
    Canonicalize,
}

/// Reference specification for an extension table field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefSpec {
    /// Table name.
    pub table: String,
    /// Field path (e.g., "`related_to`", "`entities[]`", "`data.refs[].entity`").
    pub field_path: String,
    /// Cardinality.
    pub cardinality: Cardinality,
    /// Merge behavior.
    #[serde(default)]
    pub on_merge: OnMerge,
    /// Whether the field is required.
    #[serde(default)]
    pub required: bool,
}

impl RefSpec {
    /// Validates the ref spec.
    ///
    /// # Errors
    ///
    /// Returns an error if the table prefix or field path is invalid.
    pub fn validate(&self, extension_name: &str) -> ExtensionResult<()> {
        if !is_safe_identifier(&self.table) {
            return Err(ExtensionError::InvalidRefPath(format!(
                "table '{}' must use only ASCII letters, digits, and underscores",
                self.table
            )));
        }

        // Table must be prefixed with extension name
        if !self.table.starts_with(&format!("{extension_name}_")) {
            return Err(ExtensionError::InvalidRefPath(format!(
                "table '{}' must be prefixed with '{}_'",
                self.table, extension_name
            )));
        }

        // Validate field path grammar
        validate_field_path(&self.field_path)?;

        Ok(())
    }
}

pub(crate) fn is_safe_identifier(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validates a field path.
fn validate_field_path(path: &str) -> ExtensionResult<()> {
    if path.is_empty() {
        return Err(ExtensionError::InvalidRefPath(
            "empty field path".to_string(),
        ));
    }

    for segment in path.split('.') {
        if segment.is_empty() {
            return Err(ExtensionError::InvalidRefPath(format!(
                "invalid empty segment in field path '{path}'"
            )));
        }

        if let Some(name) = segment.strip_suffix("[]") {
            if !is_safe_identifier(name) {
                return Err(ExtensionError::InvalidRefPath(format!(
                    "invalid repeated field segment '{segment}' in field path '{path}'"
                )));
            }
            continue;
        }

        if !is_safe_identifier(segment) {
            return Err(ExtensionError::InvalidRefPath(format!(
                "invalid field segment '{segment}' in field path '{path}'"
            )));
        }
    }

    Ok(())
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
    fn test_ref_spec_validation() {
        let spec = RefSpec {
            table: "sampleapp_todo".to_string(),
            field_path: "related_to".to_string(),
            cardinality: Cardinality::Many,
            on_merge: OnMerge::Rewrite,
            required: false,
        };

        assert!(spec.validate("sampleapp").is_ok());
    }

    #[test]
    fn test_invalid_table_prefix() {
        let spec = RefSpec {
            table: "todo".to_string(),
            field_path: "related_to".to_string(),
            cardinality: Cardinality::Many,
            on_merge: OnMerge::Rewrite,
            required: false,
        };

        assert!(spec.validate("sampleapp").is_err());
    }

    #[test]
    fn test_invalid_table_identifier() {
        let spec = RefSpec {
            table: "sampleapp-bad".to_string(),
            field_path: "related_to".to_string(),
            cardinality: Cardinality::Many,
            on_merge: OnMerge::Rewrite,
            required: false,
        };

        assert!(spec.validate("sampleapp").is_err());
    }

    #[test]
    fn test_field_path_validation() {
        assert!(validate_field_path("field").is_ok());
        assert!(validate_field_path("field[]").is_ok());
        assert!(validate_field_path("data.refs[].entity").is_ok());
        assert!(validate_field_path("").is_err());
        assert!(validate_field_path("owner..id").is_err());
        assert!(validate_field_path("refs.[].entity").is_err());
    }
}
