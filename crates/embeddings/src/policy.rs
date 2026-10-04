//! Embedding policy configuration and provider caching helpers.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::EmbeddingProvider;

/// Embedding policy for a space.
#[derive(Debug, Clone, Eq)]
pub struct EmbeddingPolicy {
    /// Provider identifier (e.g. "fastembed", "openai", "hashed", "fake").
    pub provider: String,
    /// Embedding model name.
    pub model: String,
    /// Embedding vector dimensions.
    pub dims: usize,
}

impl EmbeddingPolicy {
    /// Create a new embedding policy.
    #[must_use]
    pub fn new(provider: impl Into<String>, model: impl Into<String>, dims: usize) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            dims,
        }
    }

    /// Build a policy from a space config record.
    #[must_use]
    pub fn from_space_config(config: &elephant_types::records::SpaceConfig) -> Self {
        let dims = usize::try_from(config.embedding_dims).unwrap_or(0);
        Self {
            provider: config.embedding_provider.clone(),
            model: config.embedding_model.clone(),
            dims,
        }
    }

    /// Build a policy from environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let provider =
            std::env::var("PIPELINE_EMBED_PROVIDER").unwrap_or_else(|_| default_provider());
        let model =
            std::env::var("PIPELINE_EMBED_MODEL").unwrap_or_else(|_| default_model_for(&provider));
        let dims = std::env::var("PIPELINE_EMBED_DIMS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| default_dims_for(&provider, &model));

        Self {
            provider,
            model,
            dims,
        }
    }
}

impl PartialEq for EmbeddingPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.provider == other.provider && self.model == other.model && self.dims == other.dims
    }
}

impl Hash for EmbeddingPolicy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.provider.hash(state);
        self.model.hash(state);
        self.dims.hash(state);
    }
}

/// Cache of embedding providers keyed by policy.
#[derive(Clone, Default)]
pub struct EmbeddingProviderCache {
    inner: Arc<RwLock<HashMap<EmbeddingPolicy, Arc<dyn EmbeddingProvider>>>>,
}

impl EmbeddingProviderCache {
    /// Create a new empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a cache with an initial provider registered.
    #[must_use]
    pub fn with_provider(policy: EmbeddingPolicy, provider: Arc<dyn EmbeddingProvider>) -> Self {
        let mut map = HashMap::new();
        map.insert(policy, provider);
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    /// Insert or update a provider for the policy.
    pub async fn insert(&self, policy: EmbeddingPolicy, provider: Arc<dyn EmbeddingProvider>) {
        let mut map = self.inner.write().await;
        map.insert(policy, provider);
    }

    /// Returns a cached provider or creates one using `build`.
    pub async fn get_or_create<F>(
        &self,
        policy: EmbeddingPolicy,
        build: F,
    ) -> Arc<dyn EmbeddingProvider>
    where
        F: FnOnce(&EmbeddingPolicy) -> Arc<dyn EmbeddingProvider>,
    {
        if let Some(provider) = self.inner.read().await.get(&policy).cloned() {
            return provider;
        }

        let provider = build(&policy);
        let mut map = self.inner.write().await;
        map.insert(policy, provider.clone());
        provider
    }
}

/// The provider used when neither a space config nor `PIPELINE_EMBED_PROVIDER` names one.
///
/// Never `fake`: that default wrote `FakeEmbeddingProvider` ramps, labelled as
/// the real model, into a production store for its whole life.
#[must_use]
pub fn default_provider() -> String {
    #[cfg(feature = "fastembed")]
    {
        "fastembed".to_string()
    }

    #[cfg(not(feature = "fastembed"))]
    {
        let has_openai_key = std::env::var("OPENAI_API_KEY")
            .map(|key| !key.trim().is_empty())
            .unwrap_or(false);
        if has_openai_key {
            "openai".to_string()
        } else {
            // Builds an UnavailableEmbeddingProvider: embedding fails visibly.
            "unconfigured".to_string()
        }
    }
}

/// The default model for a provider.
#[must_use]
pub fn default_model_for(provider: &str) -> String {
    match provider {
        "fastembed" => "bge-small-en-v1.5".to_string(),
        "hashed" => "hashed-bow-v1".to_string(),
        _ => "text-embedding-3-large".to_string(),
    }
}

/// The default embedding dimensions for a provider and model.
#[must_use]
pub fn default_dims_for(provider: &str, model: &str) -> usize {
    if provider == "fastembed" {
        #[cfg(feature = "fastembed")]
        {
            return crate::fastembed_model_dimensions(model).unwrap_or(384);
        }
        #[cfg(not(feature = "fastembed"))]
        {
            let _ = model; // silence unused warning when fastembed feature is disabled
            return 384;
        }
    }

    if provider == "openai" {
        return 3072;
    }

    if provider == "hashed" {
        return 384;
    }

    3072
}
