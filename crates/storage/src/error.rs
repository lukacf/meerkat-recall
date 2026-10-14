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

    /// A durable erasure reservation temporarily blocks writes in this space.
    #[error("Source erasure is in progress; retry after cleanup")]
    ErasureInProgress,

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
        if is_erasure_in_progress(&err) {
            Self::ErasureInProgress
        } else {
            Self::Surreal(Box::new(err))
        }
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
        if is_erasure_reservation_message(&message) {
            return Self::ErasureInProgress;
        }
        if is_conflict_message(&message) {
            return Self::Conflict(message);
        }
        Self::QueryError(message)
    }
}

/// Recognizes only the native typed reservation refusal or its remote query wrapper.
///
/// The remote SDK preserves a `THROW` as an `Api::Query` string. Other error
/// variants and messages containing the sentinel as incidental text remain errors
/// of their original kind; no query text or generic conflict is reclassified.
#[must_use]
pub fn is_erasure_in_progress(error: &surrealdb::Error) -> bool {
    match error {
        surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)) => {
            message == "publication:erasure_space_reserved"
        }
        surrealdb::Error::Api(surrealdb::error::Api::Query(message)) => {
            message == "An error occurred: publication:erasure_space_reserved"
        }
        _ => false,
    }
}

fn is_erasure_reservation_message(message: &str) -> bool {
    matches!(
        message,
        "publication:erasure_space_reserved"
            | "An error occurred: publication:erasure_space_reserved"
    )
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
    fn reservation_classification_preserves_unrelated_errors() {
        for message in [
            "publication:erasure_space_reserved",
            "An error occurred: publication:erasure_space_reserved",
        ] {
            assert!(matches!(
                StorageError::classify_query_error(message.into()),
                StorageError::ErasureInProgress
            ));
        }
        for message in [
            "ordinary failure mentioning publication:erasure_space_reserved",
            "An error occurred: publication:erasure_space_reserved trailing data",
            "publication:erasure_space_reserved\n",
            "Source erasure is in progress; retry after cleanup",
        ] {
            assert!(matches!(
                StorageError::classify_query_error(message.into()),
                StorageError::QueryError(preserved) if preserved == message
            ));
        }
        assert!(matches!(
            StorageError::classify_query_error("record conflict".into()),
            StorageError::Conflict(_)
        ));
    }

    #[test]
    fn typed_and_remote_reservations_convert_without_leaking_details() {
        let native = surrealdb::Error::Db(surrealdb::error::Db::Thrown(
            "publication:erasure_space_reserved".into(),
        ));
        let remote = surrealdb::Error::Api(surrealdb::error::Api::Query(
            "An error occurred: publication:erasure_space_reserved".into(),
        ));
        for error in [native, remote] {
            assert!(is_erasure_in_progress(&error));
            let storage = StorageError::from(error);
            assert!(matches!(storage, StorageError::ErasureInProgress));
            assert_eq!(
                storage.to_string(),
                "Source erasure is in progress; retry after cleanup"
            );
        }
        for error in [
            surrealdb::Error::Db(surrealdb::error::Db::Thrown(
                "An error occurred: publication:erasure_space_reserved".into(),
            )),
            surrealdb::Error::Api(surrealdb::error::Api::Query(
                "publication:erasure_space_reserved".into(),
            )),
            surrealdb::Error::Api(surrealdb::error::Api::Query(
                "unrelated query failed: publication:erasure_space_reserved".into(),
            )),
        ] {
            assert!(!is_erasure_in_progress(&error));
            assert!(matches!(
                StorageError::from(error),
                StorageError::Surreal(_)
            ));
        }
    }

    #[test]
    fn test_error_display() {
        let err = StorageError::NotFound("entity:123".to_string());
        assert!(err.to_string().contains("entity:123"));
    }
}
