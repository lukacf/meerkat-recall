//! Error types for the identity crate.

use thiserror::Error;

/// Errors in identity resolution.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// Identity key conflict.
    #[error("Identity key conflict: {0}")]
    KeyConflict(String),

    /// Merge conflict.
    #[error("Merge conflict: {0}")]
    MergeConflict(String),

    /// Invalid merge strategy.
    #[error("Invalid merge strategy: {0}")]
    InvalidStrategy(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] elephant_storage::StorageError),

    /// Entity not found.
    #[error("Entity not found: {0}")]
    EntityNotFound(String),
}

/// Result type for identity operations.
pub type IdentityResult<T> = Result<T, IdentityError>;
