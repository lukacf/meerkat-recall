//! Error types for the storage crate.

use thiserror::Error;

/// Errors that can occur in storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Database connection error.
    #[error("Database connection error: {0}")]
    ConnectionError(String),

    /// Query execution error.
    #[error("Query error: {0}")]
    QueryError(String),

    /// Transaction error.
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Record not found.
    #[error("Record not found: {0}")]
    NotFound(String),

    /// Conflict error (e.g., unique constraint violation).
    #[error("Conflict: {0}")]
    Conflict(String),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Persisted data or a proposed atomic write violates a storage invariant.
    #[error("Data integrity error: {0}")]
    DataIntegrityError(String),

    /// Migration error.
    #[error("Migration error: {0}")]
    MigrationError(String),

    /// Type conversion error.
    #[error("Type error: {0}")]
    TypeError(#[from] elephant_types::TypeError),

    /// `SurrealDB` driver error.
    #[error("SurrealDB error: {0}")]
    Surreal(Box<surrealdb::Error>),
}

impl From<surrealdb::Error> for StorageError {
    fn from(err: surrealdb::Error) -> Self {
        Self::Surreal(Box::new(err))
    }
}

impl StorageError {
    /// Classifies a `SurrealDB` query error message into a more specific storage error.
    ///
    /// `SurrealDB` does not currently expose a stable, structured error type for
    /// constraint violations across all drivers. We conservatively detect common
    /// conflict patterns (e.g., unique index violations, record already exists)
    /// and map them to `StorageError::Conflict` so API layers can return 409s per
    /// the spec.
    #[must_use]
    pub fn classify_query_error(message: String) -> Self {
        if is_conflict_message(&message) {
            return Self::Conflict(message);
        }
        Self::QueryError(message)
    }
}

fn is_conflict_message(message: &str) -> bool {
    let msg = message.to_lowercase();

    // Common SurrealDB patterns for uniqueness/exists conflicts.
    msg.contains("already exists")
        || msg.contains("already contains")
        || msg.contains("duplicate")
        || (msg.contains("unique") && msg.contains("index"))
        || (msg.contains("unique") && msg.contains("constraint"))
        || (msg.contains("conflict") && (msg.contains("index") || msg.contains("record")))
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

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
        let err = StorageError::NotFound("entity:123".to_string());
        assert!(err.to_string().contains("entity:123"));
    }
}
