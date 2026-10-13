//! Error types for the types crate.

use thiserror::Error;

/// Errors that can occur when working with Elephant types.
#[derive(Debug, Error)]
pub enum TypeError {
    /// Invalid record ID format.
    #[error("Invalid record ID: {0}")]
    InvalidRecordId(String),

    /// Invalid table name.
    #[error("Invalid table name: {0}")]
    InvalidTableName(String),

    /// Validation error.
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Invalid UUID.
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),

    /// Missing required field.
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Invalid enum variant.
    #[error("Invalid enum variant for {field}: {value}")]
    InvalidEnumVariant {
        /// Name of the field with the invalid variant.
        field: String,
        /// The invalid value encountered.
        value: String,
    },
}

/// Result type alias for type operations.
pub type TypeResult<T> = Result<T, TypeError>;

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
    fn test_error_display() {
        let err = TypeError::InvalidRecordId("bad:id".to_string());
        assert!(err.to_string().contains("bad:id"));
    }
}
