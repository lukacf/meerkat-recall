//! Error types for the embeddings crate.

use thiserror::Error;

/// Errors in embedding operations.
#[derive(Debug, Error)]
pub enum EmbeddingError {
    /// Provider error.
    #[error("Embedding provider error: {0}")]
    ProviderError(String),

    /// Partition error.
    #[error("Partition error: {0}")]
    PartitionError(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Dimension mismatch.
    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Expected embedding dimension.
        expected: usize,
        /// Actual embedding dimension.
        actual: usize,
    },
}

/// Result type for embedding operations.
pub type EmbeddingResult<T> = Result<T, EmbeddingError>;
