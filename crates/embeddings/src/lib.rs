//! Embedding generation and vector storage for Elephant.
//!
//! This crate provides:
//! - Embedding provider abstraction
//! - Dynamic partition provisioning
//! - Vector search routing

pub mod error;
pub mod partition;
pub mod policy;
pub mod provider;

pub use error::{EmbeddingError, EmbeddingResult};
pub use partition::{
    discover_accessible_partition_keys, level_bucket, PartitionKey, PartitionManager,
};
pub use policy::{EmbeddingPolicy, EmbeddingProviderCache};
pub use provider::{
    EmbeddingProvider, EmbeddingVector, FakeEmbeddingProvider, HashedEmbeddingProvider,
    OpenAIConfig, OpenAIEmbeddingProvider,
};

#[cfg(feature = "fastembed")]
pub use provider::{fastembed_model_dimensions, FastEmbedConfig, FastEmbedEmbeddingProvider};

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
    fn test_embedding_error_display() {
        let err = EmbeddingError::ProviderError("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
