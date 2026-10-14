//! Error types for the embeddings crate.

use thiserror::Error;

/// Errors in embedding operations.
#[derive(Debug, Error)]
pub enum EmbeddingError {
    /// Provider error.
    #[error("Embedding provider error: {0}")]
    ProviderError(String),

    /// The selected representation cannot fit one complete input. No prefix
    /// was embedded and no representation change was made automatically.
    #[error("embedding_input_too_large: {preprocessing} requires a complete input within the model limit; for long documents create a new text-full-v2 embedding profile, reindex and activate it before resubmitting; do not relabel existing vectors")]
    InputTooLarge {
        /// Immutable preprocessing identity selected for this operation.
        preprocessing: &'static str,
    },

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
