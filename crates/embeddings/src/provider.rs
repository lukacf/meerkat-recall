//! Embedding provider abstraction.

use async_trait::async_trait;
use elephant_db::hashed_text_embedding;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};

#[cfg(feature = "fastembed")]
use std::sync::Mutex;

#[cfg(feature = "fastembed")]
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

use crate::error::{EmbeddingError, EmbeddingResult};

/// An embedding vector.
pub type EmbeddingVector = Vec<f32>;

/// Trait for embedding providers.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Returns the model name.
    fn model(&self) -> &str;

    /// Returns the embedding dimensions.
    fn dimensions(&self) -> usize;

    /// Generates embeddings for the given texts.
    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>>;
}

#[async_trait]
impl EmbeddingProvider for Arc<dyn EmbeddingProvider> {
    fn model(&self) -> &str {
        (**self).model()
    }

    fn dimensions(&self) -> usize {
        (**self).dimensions()
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        (**self).embed(texts).await
    }
}

/// A fake embedding provider for testing.
pub struct FakeEmbeddingProvider {
    model: String,
    dimensions: usize,
}

/// A provider that refuses to embed, carrying the reason.
///
/// Used wherever the configured provider cannot be built (missing API key,
/// unknown provider name, fastembed unavailable). Embedding then fails
/// visibly and is retried later, instead of silently writing
/// [`FakeEmbeddingProvider`] vectors under the real model's name.
pub struct UnavailableEmbeddingProvider {
    model: String,
    dimensions: usize,
    reason: String,
}

impl UnavailableEmbeddingProvider {
    /// Creates a provider that fails every call with `reason`.
    #[must_use]
    pub fn new(model: impl Into<String>, dimensions: usize, reason: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            dimensions,
            reason: reason.into(),
        }
    }
}

#[async_trait]
impl EmbeddingProvider for UnavailableEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn embed(&self, _texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        Err(EmbeddingError::ProviderError(format!(
            "embedding provider unavailable: {}",
            self.reason
        )))
    }
}

/// A deterministic bag-of-words style embedding provider suitable for local,
/// embedded, and browser-first execution.
pub struct HashedEmbeddingProvider {
    model: String,
    dimensions: usize,
}

#[cfg(feature = "fastembed")]
/// Configuration for the `fastembed` embedding provider.
#[derive(Debug, Clone)]
pub struct FastEmbedConfig {
    /// Model identifier.
    pub model: String,
    /// Maximum texts per batch.
    pub max_batch_size: usize,
}

#[cfg(feature = "fastembed")]
impl Default for FastEmbedConfig {
    fn default() -> Self {
        Self {
            model: "bge-small-en-v1.5".to_string(),
            max_batch_size: 128,
        }
    }
}

#[cfg(feature = "fastembed")]
impl FastEmbedConfig {
    /// Create config with a specific model name.
    #[must_use]
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            ..Default::default()
        }
    }

    /// Set max batch size.
    #[must_use]
    pub const fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }
}

#[cfg(feature = "fastembed")]
#[derive(Debug, Clone)]
struct FastEmbedModelSpec {
    model: EmbeddingModel,
    dims: usize,
}

#[cfg(feature = "fastembed")]
fn fastembed_model_spec(name: &str) -> EmbeddingResult<FastEmbedModelSpec> {
    let normalized = name.trim().to_ascii_lowercase();
    let (model, dims) = match normalized.as_str() {
        "all-minilm-l6-v2" => (EmbeddingModel::AllMiniLML6V2, 384),
        "all-minilm-l6-v2-q" => (EmbeddingModel::AllMiniLML6V2Q, 384),
        "all-minilm-l12-v2" => (EmbeddingModel::AllMiniLML12V2, 384),
        "all-minilm-l12-v2-q" => (EmbeddingModel::AllMiniLML12V2Q, 384),
        "all-mpnet-base-v2" => (EmbeddingModel::AllMpnetBaseV2, 768),
        "bge-small-en-v1.5" => (EmbeddingModel::BGESmallENV15, 384),
        "bge-small-en-v1.5-q" => (EmbeddingModel::BGESmallENV15Q, 384),
        "bge-base-en-v1.5" => (EmbeddingModel::BGEBaseENV15, 768),
        "bge-base-en-v1.5-q" => (EmbeddingModel::BGEBaseENV15Q, 768),
        "bge-large-en-v1.5" => (EmbeddingModel::BGELargeENV15, 1024),
        "bge-large-en-v1.5-q" => (EmbeddingModel::BGELargeENV15Q, 1024),
        _ => {
            return Err(EmbeddingError::ProviderError(format!(
                "Unsupported fastembed model: {name}"
            )))
        }
    };

    Ok(FastEmbedModelSpec { model, dims })
}

/// Returns the embedding dimensions for a fastembed model (if supported).
#[cfg(feature = "fastembed")]
#[must_use]
pub fn fastembed_model_dimensions(name: &str) -> Option<usize> {
    fastembed_model_spec(name).ok().map(|spec| spec.dims)
}

#[cfg(feature = "fastembed")]
/// Local embedding provider backed by the `fastembed` runtime.
pub struct FastEmbedEmbeddingProvider {
    model_name: String,
    dims: usize,
    max_batch_size: usize,
    inner: Arc<Mutex<TextEmbedding>>,
}

#[cfg(feature = "fastembed")]
impl FastEmbedEmbeddingProvider {
    /// Create a fastembed provider.
    ///
    /// # Errors
    ///
    /// Returns an error if the model is unsupported or initialization fails.
    pub fn new(config: FastEmbedConfig) -> EmbeddingResult<Self> {
        let spec = fastembed_model_spec(&config.model)?;
        let options = InitOptions::new(spec.model);
        let inner = TextEmbedding::try_new(options).map_err(|e| {
            EmbeddingError::ProviderError(format!("Failed to initialize fastembed: {e}"))
        })?;

        Ok(Self {
            model_name: config.model,
            dims: spec.dims,
            max_batch_size: config.max_batch_size,
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

#[cfg(feature = "fastembed")]
#[async_trait]
impl EmbeddingProvider for FastEmbedEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model_name
    }

    fn dimensions(&self) -> usize {
        self.dims
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_embeddings = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(self.max_batch_size) {
            let inputs: Vec<String> = chunk.to_vec();
            let embedder = self.inner.clone();
            let embeddings = tokio::task::spawn_blocking(move || {
                let refs: Vec<&str> = inputs.iter().map(String::as_str).collect();
                let mut guard = embedder.lock().map_err(|_| {
                    EmbeddingError::ProviderError("fastembed lock poisoned".to_string())
                })?;
                guard
                    .embed(refs, None)
                    .map_err(|e| EmbeddingError::ProviderError(format!("fastembed error: {e}")))
            })
            .await
            .map_err(|e| EmbeddingError::ProviderError(format!("fastembed join error: {e}")))??;

            all_embeddings.extend(embeddings);
        }

        Ok(all_embeddings)
    }
}
impl FakeEmbeddingProvider {
    /// Creates a new fake provider.
    #[must_use]
    pub fn new(model: impl Into<String>, dimensions: usize) -> Self {
        Self {
            model: model.into(),
            dimensions,
        }
    }
}

impl Default for FakeEmbeddingProvider {
    fn default() -> Self {
        Self::new("text-embedding-3-large", 3072)
    }
}

impl HashedEmbeddingProvider {
    /// Creates a new hashed embedding provider.
    #[must_use]
    pub fn new(model: impl Into<String>, dimensions: usize) -> Self {
        Self {
            model: model.into(),
            dimensions,
        }
    }
}

impl Default for HashedEmbeddingProvider {
    fn default() -> Self {
        Self::new("hashed-bow-v1", 384)
    }
}

#[async_trait]
impl EmbeddingProvider for HashedEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        Ok(texts
            .iter()
            .map(|text| hashed_text_embedding(text, self.dimensions))
            .collect())
    }
}

#[async_trait]
impl EmbeddingProvider for FakeEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        // Generate deterministic fake embeddings based on text hash
        Ok(texts
            .iter()
            .map(|text| {
                let hash = text
                    .bytes()
                    .fold(0u32, |acc, b| acc.wrapping_add(u32::from(b)));
                let max_u16 = u16::MAX;
                let hash_u16 = u16::try_from(hash % u32::from(max_u16)).unwrap_or(max_u16);
                let base = f32::from(hash_u16) / 1000.0;
                let step = 0.001_f32;
                let max_usize = usize::from(max_u16);
                (0..self.dimensions)
                    .map(|i| {
                        let i_u16 = u16::try_from(i % max_usize).unwrap_or(max_u16);
                        (f32::from(i_u16).mul_add(step, base)) % 1.0
                    })
                    .collect()
            })
            .collect())
    }
}

/// `OpenAI` embedding request.
#[derive(Debug, Serialize)]
struct OpenAIEmbeddingRequest {
    model: String,
    input: Vec<String>,
    dimensions: Option<usize>,
}

/// `OpenAI` embedding response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OpenAIEmbeddingResponse {
    data: Vec<OpenAIEmbeddingData>,
    model: String,
    usage: OpenAIUsage,
}

/// `OpenAI` embedding data.
#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

/// `OpenAI` usage info.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OpenAIUsage {
    prompt_tokens: usize,
    total_tokens: usize,
}

/// `OpenAI` error response.
#[derive(Debug, Deserialize)]
struct OpenAIErrorResponse {
    error: OpenAIErrorDetail,
}

/// `OpenAI` error detail.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OpenAIErrorDetail {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
}

/// Configuration for the `OpenAI` embedding provider.
#[derive(Debug, Clone)]
pub struct OpenAIConfig {
    /// API key for authentication.
    pub api_key: String,
    /// Model to use for embeddings.
    pub model: String,
    /// Embedding dimensions (only for models that support it).
    pub dimensions: usize,
    /// API base URL.
    pub base_url: String,
    /// Maximum texts per request (`OpenAI` limit is 2048).
    pub max_batch_size: usize,
    /// Maximum retries on transient failures.
    pub max_retries: usize,
}

impl Default for OpenAIConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            model: "text-embedding-3-small".to_string(),
            dimensions: 1536,
            base_url: "https://api.openai.com/v1".to_string(),
            max_batch_size: 100,
            max_retries: 3,
        }
    }
}

impl OpenAIConfig {
    /// Creates a new configuration with the given API key.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            ..Default::default()
        }
    }

    /// Creates configuration from environment variables.
    ///
    /// Reads:
    /// - `OPENAI_API_KEY` (required)
    /// - `OPENAI_MODEL` (default: text-embedding-3-small)
    /// - `OPENAI_DIMENSIONS` (default: 1536)
    /// - `OPENAI_BASE_URL` (default: `https://api.openai.com/v1`)
    ///
    /// # Errors
    ///
    /// Returns an error if required environment variables are missing or invalid.
    pub fn from_env() -> EmbeddingResult<Self> {
        let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| {
            EmbeddingError::ProviderError("OPENAI_API_KEY environment variable not set".to_string())
        })?;
        if api_key.trim().is_empty() {
            return Err(EmbeddingError::ProviderError(
                "OPENAI_API_KEY environment variable is empty".to_string(),
            ));
        }

        let model =
            std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "text-embedding-3-small".to_string());
        let dimensions = std::env::var("OPENAI_DIMENSIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1536);
        let base_url = std::env::var("OPENAI_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());

        Ok(Self {
            api_key,
            model,
            dimensions,
            base_url,
            ..Default::default()
        })
    }

    /// Sets the model.
    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Sets the dimensions.
    #[must_use]
    pub const fn with_dimensions(mut self, dimensions: usize) -> Self {
        self.dimensions = dimensions;
        self
    }

    /// Sets the base URL.
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }
}

/// `OpenAI` embedding provider.
///
/// Calls the `OpenAI` embeddings API to generate vector embeddings for text.
/// Supports batching, retry logic, and configurable models.
pub struct OpenAIEmbeddingProvider {
    config: OpenAIConfig,
    client: reqwest::Client,
}

impl OpenAIEmbeddingProvider {
    /// Creates a new `OpenAI` embedding provider.
    #[must_use]
    pub fn new(config: OpenAIConfig) -> Self {
        let client = reqwest::Client::new();
        Self { config, client }
    }

    /// Creates a provider from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment configuration is invalid.
    pub fn from_env() -> EmbeddingResult<Self> {
        let config = OpenAIConfig::from_env()?;
        Ok(Self::new(config))
    }

    /// Embeds a single batch (internal helper).
    async fn embed_batch(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        let url = format!("{}/embeddings", self.config.base_url);

        let request = OpenAIEmbeddingRequest {
            model: self.config.model.clone(),
            input: texts.to_vec(),
            dimensions: Some(self.config.dimensions),
        };

        let mut last_error = None;
        for attempt in 0..self.config.max_retries {
            if attempt > 0 {
                // Exponential backoff: 1s, 2s, 4s...
                let delay = std::time::Duration::from_secs(1 << attempt);
                warn!(
                    "Retrying OpenAI request after {:?} (attempt {})",
                    delay,
                    attempt + 1
                );
                tokio::time::sleep(delay).await;
            }

            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.config.api_key))
                .header("Content-Type", "application/json")
                .json(&request)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let body: OpenAIEmbeddingResponse = resp.json().await.map_err(|e| {
                            EmbeddingError::ProviderError(format!("Failed to parse response: {e}"))
                        })?;

                        debug!(
                            "OpenAI embedding request: {} texts, {} tokens used",
                            texts.len(),
                            body.usage.total_tokens
                        );

                        // Sort by index to ensure correct order
                        let mut data = body.data;
                        data.sort_by_key(|d| d.index);

                        return Ok(data.into_iter().map(|d| d.embedding).collect());
                    } else if status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    {
                        // Retry on 5xx errors and rate limits
                        let body = resp.text().await.unwrap_or_default();
                        last_error = Some(EmbeddingError::ProviderError(format!(
                            "OpenAI API error ({status}): {body}"
                        )));
                    } else {
                        // Client error - don't retry
                        let error_body: Result<OpenAIErrorResponse, _> = resp.json().await;
                        let message = error_body
                            .map_or_else(|_| "Unknown error".to_string(), |e| e.error.message);
                        return Err(EmbeddingError::ProviderError(format!(
                            "OpenAI API error ({status}): {message}"
                        )));
                    }
                }
                Err(e) => {
                    // Network error - retry
                    last_error = Some(EmbeddingError::ProviderError(format!(
                        "Request failed: {e}"
                    )));
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| EmbeddingError::ProviderError("Max retries exceeded".to_string())))
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddingProvider {
    fn model(&self) -> &str {
        &self.config.model
    }

    fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Split into batches if needed
        let mut all_embeddings = Vec::with_capacity(texts.len());

        for chunk in texts.chunks(self.config.max_batch_size) {
            let embeddings = self.embed_batch(chunk).await?;
            all_embeddings.extend(embeddings);
        }

        Ok(all_embeddings)
    }
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

    #[tokio::test]
    async fn test_fake_provider() {
        let provider = FakeEmbeddingProvider::default();
        assert_eq!(provider.dimensions(), 3072);

        let embeddings = provider.embed(&["test text".to_string()]).await;
        assert!(embeddings.is_ok());
        if let Ok(embeddings) = embeddings {
            assert_eq!(embeddings.len(), 1);
            assert_eq!(embeddings[0].len(), 3072);
        }
    }

    #[tokio::test]
    async fn test_deterministic_embeddings() {
        let provider = FakeEmbeddingProvider::default();

        let e1 = provider.embed(&["hello".to_string()]).await;
        let e2 = provider.embed(&["hello".to_string()]).await;
        assert!(e1.is_ok());
        assert!(e2.is_ok());
        if let (Ok(e1), Ok(e2)) = (e1, e2) {
            assert_eq!(e1, e2);
        }
    }

    #[test]
    fn test_openai_config_default() {
        let config = OpenAIConfig::default();
        assert_eq!(config.model, "text-embedding-3-small");
        assert_eq!(config.dimensions, 1536);
        assert_eq!(config.base_url, "https://api.openai.com/v1");
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_openai_config_new() {
        let config = OpenAIConfig::new("test-key");
        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.model, "text-embedding-3-small");
    }

    #[test]
    fn test_openai_config_builder() {
        let config = OpenAIConfig::new("test-key")
            .with_model("text-embedding-3-large")
            .with_dimensions(3072)
            .with_base_url("https://custom.api.com");

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.model, "text-embedding-3-large");
        assert_eq!(config.dimensions, 3072);
        assert_eq!(config.base_url, "https://custom.api.com");
    }

    #[test]
    fn test_openai_provider_creation() {
        let config = OpenAIConfig::new("test-key");
        let provider = OpenAIEmbeddingProvider::new(config);
        assert_eq!(provider.model(), "text-embedding-3-small");
        assert_eq!(provider.dimensions(), 1536);
    }

    #[tokio::test]
    async fn test_openai_provider_empty_input() {
        let config = OpenAIConfig::new("test-key");
        let provider = OpenAIEmbeddingProvider::new(config);

        let result = provider.embed(&[]).await;
        assert!(result.is_ok());
        if let Ok(result) = result {
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_openai_config_from_env_missing_key() {
        // Ensure OPENAI_API_KEY is not set for this test
        std::env::remove_var("OPENAI_API_KEY");

        let result = OpenAIConfig::from_env();
        assert!(result.is_err());
        let err = result.err();
        assert!(matches!(err, Some(err) if err.to_string().contains("OPENAI_API_KEY")));
    }

    #[tokio::test]
    async fn test_openai_provider_invalid_url() {
        let mut config =
            OpenAIConfig::new("test-key").with_base_url("http://localhost:99999/invalid");
        config.max_retries = 1; // Don't waste time on retries in tests
        let provider = OpenAIEmbeddingProvider::new(config);

        let result = provider.embed(&["test".to_string()]).await;
        assert!(result.is_err());
    }

    #[cfg(feature = "fastembed")]
    #[test]
    fn test_fastembed_model_spec() {
        let spec = fastembed_model_spec("bge-small-en-v1.5").unwrap();
        assert_eq!(spec.dims, 384);
    }
}
