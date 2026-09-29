//! Error types for the outbox crate.

use thiserror::Error;

/// Errors in outbox operations.
#[derive(Debug, Error)]
pub enum OutboxError {
    /// Storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] elephant_storage::StorageError),

    /// Sequence allocation error.
    #[error("Sequence allocation error: {0}")]
    SequenceError(String),

    /// Write error.
    #[error("Write error: {0}")]
    WriteError(String),

    /// Invalid cursor.
    #[error("Invalid cursor: {0}")]
    InvalidCursor(String),
}

/// Result type for outbox operations.
pub type OutboxResult<T> = Result<T, OutboxError>;
