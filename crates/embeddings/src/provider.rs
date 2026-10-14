//! Embedding provider abstraction.

use async_trait::async_trait;
use elephant_db::hashed_text_embedding;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};

#[path = "provider_full_input.rs"]
mod full_input;
#[cfg(feature = "fastembed")]
#[path = "provider_local_dispatch.rs"]
mod local_dispatch;
#[cfg(feature = "fastembed")]
use full_input::Segment;
use full_input::{Accumulator, Preprocessing};

#[cfg(test)]
#[path = "provider_full_input_tests.rs"]
mod full_input_tests;
#[cfg(all(test, feature = "fastembed"))]
#[path = "provider_local_input_tests.rs"]
mod local_input_tests;

#[cfg(feature = "fastembed")]
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

use crate::error::{EmbeddingError, EmbeddingResult};

const OPENAI_MAX_TOTAL_HTTP_ATTEMPTS: usize = 3;
const OPENAI_MAX_INPUTS_PER_CALL: usize = 100;
const LOCAL_MAX_INPUTS_PER_CALL: usize = 128;

/// Secret-free execution settings for one resolved, approved vector binding.
/// The caller must supply current authority; this value grants no permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EmbeddingExecutionRecipe {
    /// Original configuration, operation, binding and profile references.
    pub provenance: elephant_types::provider_binding::ModelBindingProvenance,
    /// Exact space/provider/model/preprocessing identity of resulting vectors.
    pub partition: elephant_types::provider_binding::VectorPartition,
    /// Total physical HTTP attempts for one logical request, including the first.
    /// Local inference has no HTTP request and reports `None`.
    pub max_total_http_attempts: Option<usize>,
    /// Maximum complete inputs or fitted segments per physical provider call.
    pub max_inputs_per_call: usize,
    /// Initial full-input segment byte bound; absent for exact v1 input.
    pub initial_segment_bytes: Option<usize>,
    /// Maximum size-error bisections for each initial segment; absent for v1.
    pub max_split_depth: Option<u8>,
}

/// Inspect the exact settings used by the governed embedding factory.
/// This does not load models, resolve credentials, reread environment or authorize.
///
/// # Errors
/// Rejects non-vector operations, unsupported providers or preprocessing.
pub fn embedding_execution_recipe(
    binding: &elephant_types::provider_binding::ResolvedModelBinding,
) -> EmbeddingResult<EmbeddingExecutionRecipe> {
    if !binding.provenance.operation.is_embedding() {
        return Err(full_input::invalid("embedding operation required"));
    }
    binding
        .profile
        .validate_preprocessing(&binding.connection.provider)
        .map_err(|_| {
            full_input::invalid("unsupported embedding preprocessing for provider/model")
        })?;
    let preprocessing = Preprocessing::parse(&binding.profile.preprocessing)?;
    let partition = binding
        .vector_partition()
        .map_err(|_| full_input::invalid("invalid embedding partition"))?;
    let (max_total_http_attempts, max_inputs_per_call) = match binding.connection.provider.as_str()
    {
        "openai" => (
            Some(OPENAI_MAX_TOTAL_HTTP_ATTEMPTS),
            OPENAI_MAX_INPUTS_PER_CALL,
        ),
        "fastembed" => (None, LOCAL_MAX_INPUTS_PER_CALL),
        _ => {
            return Err(full_input::invalid(
                "unsupported governed embedding provider",
            ))
        }
    };
    Ok(EmbeddingExecutionRecipe {
        provenance: binding.provenance.clone(),
        partition,
        max_total_http_attempts,
        max_inputs_per_call,
        initial_segment_bytes: (preprocessing == Preprocessing::Full)
            .then_some(full_input::INITIAL_SEGMENT_BYTES),
        max_split_depth: (preprocessing == Preprocessing::Full)
            .then_some(full_input::MAX_SPLIT_DEPTH),
    })
}

/// Construct the governed remote provider configuration from its inspected recipe.
///
/// # Errors
/// Rejects an unsupported binding or a non-OpenAI provider.
pub fn approved_openai_config(
    binding: &elephant_types::provider_binding::ResolvedModelBinding,
    api_key: String,
) -> EmbeddingResult<OpenAIConfig> {
    let recipe = embedding_execution_recipe(binding)?;
    let max_retries = recipe
        .max_total_http_attempts
        .ok_or_else(|| full_input::invalid("OpenAI binding required"))?;
    Ok(OpenAIConfig {
        api_key,
        model: recipe.partition.model,
        dimensions: recipe.partition.dimensions,
        base_url: binding.connection.endpoint.trim_end_matches('/').into(),
        max_batch_size: recipe.max_inputs_per_call,
        max_retries,
        preprocessing: recipe.partition.preprocessing,
    })
}

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
            max_batch_size: LOCAL_MAX_INPUTS_PER_CALL,
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
pub(crate) struct FastEmbedModelSpec {
    pub(crate) model: EmbeddingModel,
    pub(crate) dims: usize,
}

#[cfg(feature = "fastembed")]
pub(crate) fn fastembed_model_spec(name: &str) -> EmbeddingResult<FastEmbedModelSpec> {
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
#[derive(Clone)]
pub struct FastEmbedEmbeddingProvider {
    model_name: String,
    dims: usize,
    max_batch_size: usize,
    preprocessing: Preprocessing,
    token_limit: Option<usize>,
    dispatch: Option<crate::governed::EmbeddingDispatch>,
    inner: Arc<tokio::sync::Mutex<LocalModelResource>>,
}

#[cfg(feature = "fastembed")]
struct LocalModelResource {
    model: TextEmbedding,
    // The actual model mutex guards cross blocking-thread and cancellation
    // boundaries. Keep capacity here until the last executing owner releases it.
    _capacity: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[cfg(feature = "fastembed")]
impl FastEmbedEmbeddingProvider {
    pub(crate) fn from_verified_model(
        model_name: String,
        dims: usize,
        inner: TextEmbedding,
        capacity: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            model_name,
            dims,
            max_batch_size: LOCAL_MAX_INPUTS_PER_CALL,
            preprocessing: Preprocessing::Exact,
            token_limit: inner
                .tokenizer
                .get_truncation()
                .map(|params| params.max_length),
            dispatch: None,
            inner: Arc::new(tokio::sync::Mutex::new(LocalModelResource {
                model: inner,
                _capacity: capacity,
            })),
        }
    }

    /// Select an explicit vector preprocessing contract before inference.
    ///
    /// # Errors
    /// Rejects unknown preprocessing identities.
    pub fn with_preprocessing(mut self, preprocessing: &str) -> EmbeddingResult<Self> {
        let preprocessing = Preprocessing::parse(preprocessing)?;
        if preprocessing == Preprocessing::BgeEnRetrieval
            && elephant_types::provider_binding::bge_en_retrieval_dimensions(&self.model_name)
                != Some(self.dims)
        {
            return Err(full_input::invalid(
                "BGE retrieval requires an English BGE v1.5 model",
            ));
        }
        self.preprocessing = preprocessing;
        Ok(self)
    }

    /// Check current authority and account for each physical inference batch.
    #[must_use]
    pub fn with_dispatch(mut self, dispatch: crate::governed::EmbeddingDispatch) -> Self {
        self.dispatch = Some(dispatch);
        self
    }

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

        let mut provider = Self::from_verified_model(config.model, spec.dims, inner, None);
        provider.max_batch_size = config.max_batch_size;
        Ok(provider)
    }

    async fn prepare_inputs(&self, texts: &[String]) -> EmbeddingResult<Vec<(Segment, u64)>> {
        let inputs = full_input::local_inputs(
            texts,
            self.preprocessing,
            self.dispatch
                .as_ref()
                .map(crate::governed::EmbeddingDispatch::operation),
        )?;
        let mut guard = self.inner.clone().lock_owned().await;
        let limit = self.token_limit.filter(|limit| *limit > 0).ok_or_else(|| {
            full_input::invalid("local tokenizer has no positive approved input limit")
        })?;
        let preprocessing = self.preprocessing;
        tokio::task::spawn_blocking(move || {
            // Disable truncation on the ACTUAL inference tokenizer. Measuring
            // only a clone would leave a second silent prefix path in embed().
            guard
                .model
                .tokenizer
                .with_truncation(None)
                .map_err(|error| {
                    EmbeddingError::ProviderError(format!(
                        "cannot disable tokenizer truncation: {error}"
                    ))
                })?;
            let mut tokenizer = guard.model.tokenizer.clone();
            tokenizer.with_padding(None);
            full_input::local_segments(&inputs, preprocessing, limit, |text| {
                tokenizer
                    .encode(text, true)
                    .map(|encoding| encoding.len())
                    .map_err(|error| {
                        EmbeddingError::ProviderError(format!("local tokenization failed: {error}"))
                    })
            })
        })
        .await
        .map_err(|error| {
            EmbeddingError::ProviderError(format!("local tokenization join failed: {error}"))
        })?
    }

    async fn embed_local_batch(
        &self,
        batch: &[(Segment, u64)],
    ) -> EmbeddingResult<Vec<EmbeddingVector>> {
        // A busy model may wait arbitrarily long. Approval and durable budget
        // admission belong after that wait, immediately before inference.
        let mut guard = self.inner.clone().lock_owned().await;
        let tokens = batch
            .iter()
            .try_fold(0_u64, |sum, (_, count)| sum.checked_add(*count))
            .ok_or_else(|| full_input::invalid("local tokenizer usage overflow"))?;
        let inputs: Vec<_> = batch
            .iter()
            .map(|(segment, _)| segment.text.clone())
            .collect();
        let vectors = local_dispatch::run(
            self.dispatch.clone(),
            self.model_name.clone(),
            self.dims,
            self.preprocessing,
            tokens,
            move || {
                let refs: Vec<_> = inputs.iter().map(String::as_str).collect();
                // Explicitly one physical batch, including dynamically quantized
                // models; no unobserved internal inference chunks.
                let count = refs.len();
                guard.model.embed(refs, Some(count)).map_err(|error| {
                    EmbeddingError::ProviderError(format!("fastembed error: {error}"))
                })
            },
        )
        .await?;
        full_input::validate_vectors(&vectors, batch.len(), self.dims)?;
        Ok(vectors)
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
        if self.max_batch_size == 0 || self.dims == 0 {
            return Err(full_input::invalid(
                "batch size and dimensions must be positive",
            ));
        }
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let segments = self.prepare_inputs(texts).await?;
        let mut accumulator = if self.preprocessing == Preprocessing::Full {
            Some(Accumulator::new(texts, self.dims)?)
        } else {
            None
        };
        let mut all_embeddings = Vec::with_capacity(texts.len());
        for batch in segments.chunks(self.max_batch_size) {
            let vectors = self.embed_local_batch(batch).await?;
            if let Some(accumulator) = &mut accumulator {
                for ((segment, _), vector) in batch.iter().zip(vectors) {
                    accumulator.add(segment, &vector)?;
                }
            } else {
                all_embeddings.extend(vectors);
            }
        }
        accumulator.map_or(Ok(all_embeddings), Accumulator::finish)
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
    #[serde(rename = "type", default)]
    error_type: Option<String>,
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
    /// Maximum total attempts on transient failures, including the first send.
    pub max_retries: usize,
    /// Explicit vector preprocessing identity; never inferred from input size.
    pub preprocessing: String,
}

impl Default for OpenAIConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            model: "text-embedding-3-small".to_string(),
            dimensions: 1536,
            base_url: "https://api.openai.com/v1".to_string(),
            max_batch_size: OPENAI_MAX_INPUTS_PER_CALL,
            max_retries: OPENAI_MAX_TOTAL_HTTP_ATTEMPTS,
            preprocessing: "text-v1".into(),
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

    /// Select the explicit `text-v1` or `text-full-v2` vector contract.
    #[must_use]
    pub fn with_preprocessing(mut self, preprocessing: impl Into<String>) -> Self {
        self.preprocessing = preprocessing.into();
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
    dispatch: Option<crate::governed::EmbeddingDispatch>,
}

impl OpenAIEmbeddingProvider {
    /// Installs a current-approval check for every batch and HTTP retry.
    ///
    /// # Errors
    /// Fails if a redirect-free HTTP client cannot be constructed.
    pub fn with_dispatch(
        mut self,
        dispatch: crate::governed::EmbeddingDispatch,
    ) -> EmbeddingResult<Self> {
        self.client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                EmbeddingError::ProviderError("cannot initialize approved transport".into())
            })?;
        self.dispatch = Some(dispatch);
        Ok(self)
    }

    /// Creates a new `OpenAI` embedding provider.
    #[must_use]
    pub fn new(config: OpenAIConfig) -> Self {
        let client = reqwest::Client::new();
        Self {
            config,
            client,
            dispatch: None,
        }
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

    /// Account for completed usage and validate the entire ordered response.
    async fn complete_batch(
        &self,
        response: reqwest::Response,
        call_id: Option<&crate::governed::DispatchObservation>,
        input_count: usize,
    ) -> Result<Vec<EmbeddingVector>, BatchError> {
        let body: OpenAIEmbeddingResponse = response.json().await.map_err(|e| {
            BatchError::Provider(EmbeddingError::ProviderError(format!(
                "Failed to parse response: {e}"
            )))
        })?;

        if let Some(dispatch) = &self.dispatch {
            dispatch
                .usage_snapshot(
                    call_id,
                    u64::try_from(body.usage.total_tokens).map_err(|_| {
                        BatchError::Provider(EmbeddingError::ProviderError(
                            "provider usage overflow".into(),
                        ))
                    })?,
                )
                .await
                .map_err(BatchError::Provider)?;
        }
        debug!(
            "OpenAI embedding request: {} texts, {} tokens used",
            input_count, body.usage.total_tokens
        );

        // Sort by index to ensure correct order
        let mut data = body.data;
        data.sort_by_key(|d| d.index);
        if data.len() != input_count
            || data
                .iter()
                .enumerate()
                .any(|(index, row)| row.index != index)
        {
            return Err(BatchError::Provider(full_input::invalid(
                "provider returned missing, duplicate or foreign embedding indices",
            )));
        }
        let vectors: Vec<_> = data.into_iter().map(|row| row.embedding).collect();
        full_input::validate_vectors(&vectors, input_count, self.config.dimensions)
            .map_err(BatchError::Provider)?;
        Ok(vectors)
    }

    /// Classify a non-retryable response after decoding its provider error.
    async fn client_error(response: reqwest::Response, status: reqwest::StatusCode) -> BatchError {
        let error_body: Result<OpenAIErrorResponse, _> = response.json().await;
        let message = error_body.map_or_else(|_| "Unknown error".to_string(), |e| e.error.message);
        if is_request_too_large(&message) {
            return BatchError::RequestTooLarge;
        }
        if is_input_too_long(&message) {
            return BatchError::InputTooLong {
                index: input_index_in_error(&message),
            };
        }
        BatchError::Provider(EmbeddingError::ProviderError(format!(
            "OpenAI API error ({status}): {message}"
        )))
    }

    /// Embeds a single batch (internal helper).
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingVector>, BatchError> {
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

            // Backoff above holds no slot. This permit spans the physical send
            // and full decoder, and drops on every error/cancellation path.
            // Governed authority and receipt checks below run after this wait.
            let _remote_permit = crate::remote_admission::acquire(&self.config.base_url)
                .await
                .map_err(BatchError::Provider)?;
            let (request_url, api_key, call_id) = if let Some(dispatch) = &self.dispatch {
                let (binding, _) = dispatch.authorize().await.map_err(BatchError::Provider)?;
                if binding.profile.model != self.config.model
                    || binding.profile.dimensions != Some(self.config.dimensions)
                    || binding.profile.preprocessing != self.config.preprocessing
                {
                    return Err(BatchError::Provider(EmbeddingError::ProviderError(
                        "approved embedding profile changed".into(),
                    )));
                }
                let call_id = dispatch
                    .dispatch_started(&binding)
                    .await
                    .map_err(BatchError::Provider)?;
                // Receipt persistence can wait on storage; use current approval
                // and rotated credential immediately before the physical send.
                let (binding, credential) =
                    dispatch.authorize().await.map_err(BatchError::Provider)?;
                (
                    format!(
                        "{}/embeddings",
                        binding.connection.endpoint.trim_end_matches('/')
                    ),
                    credential.ok_or_else(|| {
                        BatchError::Provider(EmbeddingError::ProviderError(
                            "approved credential unavailable".into(),
                        ))
                    })?,
                    call_id,
                )
            } else {
                (url.clone(), self.config.api_key.clone(), None)
            };
            let response = self
                .client
                .post(&request_url)
                .header("Authorization", format!("Bearer {api_key}"))
                .header("Content-Type", "application/json")
                .json(&request)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return self
                            .complete_batch(resp, call_id.as_ref(), texts.len())
                            .await;
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
                        return Err(Self::client_error(resp, status).await);
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

        Err(BatchError::Provider(last_error.unwrap_or_else(|| {
            EmbeddingError::ProviderError("Max retries exceeded".to_string())
        })))
    }

    /// Exact text-v1 inputs are never shortened. A rejected whole request may
    /// be split into complete batches, preserving each original input/vector.
    async fn embed_batch_fitting(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        match self.embed_batch(texts).await {
            Ok(embeddings) => Ok(embeddings),
            Err(BatchError::Provider(error)) => Err(error),
            Err(BatchError::RequestTooLarge) if texts.len() > 1 => {
                let (first, second) = texts.split_at(texts.len() / 2);
                let mut embeddings = Box::pin(self.embed_batch_fitting(first)).await?;
                embeddings.extend(Box::pin(self.embed_batch_fitting(second)).await?);
                Ok(embeddings)
            }
            Err(BatchError::RequestTooLarge | BatchError::InputTooLong { .. }) => {
                Err(EmbeddingError::InputTooLarge {
                    preprocessing: "text-v1",
                })
            }
        }
    }

    /// Consume every segment before exposing any aggregate. Failed requests
    /// re-enter the same concrete dispatch/accounting guard for every retry.
    async fn embed_full_inputs(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        use std::collections::VecDeque;
        let mut pending: VecDeque<_> = full_input::segments(texts).into();
        let mut accumulator = Accumulator::new(texts, self.config.dimensions)?;
        while !pending.is_empty() {
            let count = pending.len().min(self.config.max_batch_size);
            let mut batch: Vec<_> = pending.drain(..count).collect();
            loop {
                let inputs: Vec<_> = batch.iter().map(|segment| segment.text.clone()).collect();
                match self.embed_batch(&inputs).await {
                    Ok(vectors) => {
                        for (segment, vector) in batch.iter().zip(vectors) {
                            accumulator.add(segment, &vector)?;
                        }
                        break;
                    }
                    Err(BatchError::Provider(error)) => return Err(error),
                    Err(
                        BatchError::RequestTooLarge | BatchError::InputTooLong { index: None, .. },
                    ) if batch.len() > 1 => {
                        // Isolate unindexed errors rather than guessing which
                        // input was rejected and silently losing another tail.
                        let later = batch.split_off(batch.len() / 2);
                        for segment in later.into_iter().rev() {
                            pending.push_front(segment);
                        }
                    }
                    Err(BatchError::InputTooLong { index, .. }) => {
                        let index = index.unwrap_or(0);
                        if index >= batch.len() {
                            return Err(full_input::invalid(
                                "provider named an invalid input index",
                            ));
                        }
                        let halves = batch.remove(index).split()?;
                        batch.splice(index..index, halves);
                        let retain = self.config.max_batch_size.min(batch.len());
                        for segment in batch.drain(retain..).rev() {
                            pending.push_front(segment);
                        }
                    }
                    Err(BatchError::RequestTooLarge) => {
                        let halves = batch.remove(0).split()?;
                        batch.extend(halves);
                        let retain = self.config.max_batch_size.min(batch.len());
                        for segment in batch.drain(retain..).rev() {
                            pending.push_front(segment);
                        }
                    }
                }
            }
        }
        accumulator.finish()
    }
}

/// Why one `OpenAI` batch request failed.
enum BatchError {
    /// The API rejected an input as longer than the model's token limit.
    InputTooLong {
        /// Index of the rejected input, when the error names one.
        index: Option<usize>,
    },
    /// The API rejected the whole request as over its per-request token limit.
    RequestTooLarge,
    /// Any other failure.
    Provider(EmbeddingError),
}

/// Whether an `OpenAI` error message reports a request over its total token limit.
fn is_request_too_large(message: &str) -> bool {
    message.contains("tokens per request")
}

/// Whether an `OpenAI` error message reports an input over the token limit.
fn is_input_too_long(message: &str) -> bool {
    message.contains("maximum input length") || message.contains("maximum context length")
}

/// The input index named by an error such as `Invalid 'input[53]': ...`.
fn input_index_in_error(message: &str) -> Option<usize> {
    let start = message.find("input[")? + "input[".len();
    let end = start + message[start..].find(']')?;
    message[start..end].parse().ok()
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
        let preprocessing = Preprocessing::parse(&self.config.preprocessing)?;
        if preprocessing == Preprocessing::BgeEnRetrieval {
            return Err(full_input::invalid(
                "BGE retrieval preprocessing requires FastEmbed",
            ));
        }
        if preprocessing == Preprocessing::Full {
            full_input::require_nonempty_inputs(texts)?;
        }
        if self.config.max_batch_size == 0 || self.config.dimensions == 0 {
            return Err(full_input::invalid(
                "batch size and dimensions must be positive",
            ));
        }
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Split into batches if needed
        let mut all_embeddings = Vec::with_capacity(texts.len());

        for chunk in texts.chunks(self.config.max_batch_size) {
            let embeddings = match preprocessing {
                Preprocessing::Exact => self.embed_batch_fitting(chunk).await?,
                Preprocessing::Full => self.embed_full_inputs(chunk).await?,
                Preprocessing::BgeEnRetrieval => {
                    return Err(full_input::invalid(
                        "BGE retrieval preprocessing requires FastEmbed",
                    ));
                }
            };
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

    /// Minimal stand-in for the `OpenAI` embeddings endpoint. Rejects an input
    /// longer than `input_limit` chars, or a request whose inputs total more
    /// than `request_limit` chars, with the API's error messages; otherwise
    /// returns one vector per input. Records each request's input lengths.
    async fn fake_openai(
        input_limit: usize,
        request_limit: usize,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<Vec<usize>>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = calls.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    loop {
                        let mut chunk = vec![0u8; 65536];
                        let Ok(read) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..read]);
                        let Some(header_end) =
                            buffer.windows(4).position(|window| window == b"\r\n\r\n")
                        else {
                            continue;
                        };
                        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .map_or(0, |value| value.trim().parse().unwrap());
                        if buffer.len() < header_end + 4 + length {
                            continue;
                        }
                        let body: serde_json::Value = serde_json::from_slice(
                            &buffer[header_end + 4..header_end + 4 + length],
                        )
                        .unwrap();
                        buffer.drain(..header_end + 4 + length);
                        let lengths: Vec<usize> = body["input"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|input| input.as_str().unwrap().chars().count())
                            .collect();
                        recorded.lock().unwrap().push(lengths.clone());
                        let total: usize = lengths.iter().sum();
                        let too_long = lengths.iter().position(|length| *length > input_limit);
                        let (status, payload) = match (total > request_limit, too_long) {
                            (true, _) => (
                                "400 Bad Request",
                                serde_json::json!({"error": {
                                    "type": "invalid_request_error",
                                    "message": format!(
                                        "Requested {total} tokens, max 300000 tokens per request"
                                    )
                                }}),
                            ),
                            (false, Some(index)) => (
                                "400 Bad Request",
                                serde_json::json!({"error": {
                                    "type": "invalid_request_error",
                                    "message": format!(
                                        "Invalid 'input[{index}]': maximum input length is 8192 tokens."
                                    )
                                }}),
                            ),
                            (false, None) => {
                                let data: Vec<serde_json::Value> = (0..lengths.len())
                                    .map(|index| {
                                        serde_json::json!({"index": index, "embedding": [0.5, 0.5]})
                                    })
                                    .collect();
                                (
                                    "200 OK",
                                    serde_json::json!({
                                        "data": data,
                                        "model": "text-embedding-3-large",
                                        "usage": {"prompt_tokens": total, "total_tokens": total}
                                    }),
                                )
                            }
                        };
                        let payload = payload.to_string();
                        let response = format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}",
                            payload.len()
                        );
                        if socket.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (base_url, calls)
    }

    fn fake_provider_config(base_url: &str) -> OpenAIConfig {
        OpenAIConfig::new("test-key")
            .with_dimensions(2)
            .with_base_url(base_url)
    }

    #[tokio::test]
    async fn test_openai_provider_refuses_oversize_v1_without_prefix_loss() {
        let (base_url, calls) = fake_openai(10_000, usize::MAX).await;
        let provider = OpenAIEmbeddingProvider::new(fake_provider_config(&base_url));
        let texts = vec![
            "short".to_string(),
            "a".repeat(30_000),
            "also short".to_string(),
        ];

        let error = provider.embed(&texts).await.unwrap_err();
        assert!(matches!(
            error,
            EmbeddingError::InputTooLarge {
                preprocessing: "text-v1"
            }
        ));
        assert!(error.to_string().contains("text-full-v2"));
        assert!(error
            .to_string()
            .contains("do not relabel existing vectors"));
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls, vec![vec![5, 30_000, 10]]);
    }

    #[tokio::test]
    async fn test_openai_provider_splits_a_request_over_the_token_limit() {
        let (base_url, calls) = fake_openai(usize::MAX, 25_000).await;
        let provider = OpenAIEmbeddingProvider::new(fake_provider_config(&base_url));
        let texts: Vec<String> = (0..4).map(|_| "b".repeat(10_000)).collect();

        let embeddings = provider.embed(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 4);
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls[0].len(), 4);
        assert!(calls[1..].iter().all(|call| call.len() == 2));
    }

    #[test]
    fn test_input_index_in_error() {
        assert_eq!(
            input_index_in_error("Invalid 'input[53]': maximum input length is 8192 tokens."),
            Some(53)
        );
        assert_eq!(
            input_index_in_error("maximum input length is 8192 tokens."),
            None
        );
    }

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

    #[tokio::test]
    async fn openai_rejects_local_bge_recipe_before_any_transport() {
        let provider = OpenAIEmbeddingProvider::new(
            OpenAIConfig::new("unused")
                .with_base_url("not-a-transport-url")
                .with_preprocessing(elephant_types::provider_binding::BGE_EN_RETRIEVAL_V1),
        );
        let error = provider.embed(&["source text".into()]).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("BGE retrieval preprocessing requires FastEmbed"));
    }

    #[test]
    fn inspected_recipe_constructs_the_actual_governed_remote_configuration() {
        use elephant_types::provider_binding::*;
        let mut binding = ResolvedModelBinding {
            provenance: ModelBindingProvenance {
                config: SpaceConfigToken {
                    space_id: "recipe-space".into(),
                    config_id: "cfg".into(),
                    generation: 7,
                    policy_id: "policy".into(),
                },
                operation: ModelOperation::DocumentEmbedding,
                binding_id: "binding".into(),
                profile_id: "profile".into(),
            },
            connection: ProviderConnection {
                id: "connection".into(),
                provider: "openai".into(),
                account_id: "account".into(),
                endpoint: "https://example.test/v1/".into(),
                credential_ref: Some("RECIPE_SECRET".into()),
                credential_generation: 1,
                revoked: false,
            },
            profile: ModelProfile {
                id: "profile".into(),
                connection_id: "connection".into(),
                model: "test-model".into(),
                model_version: "test-revision".into(),
                dimensions: Some(3),
                preprocessing: "text-full-v2".into(),
            },
        };
        for operation in [
            ModelOperation::DocumentEmbedding,
            ModelOperation::QueryEmbedding,
            ModelOperation::ReindexEmbedding,
        ] {
            binding.provenance.operation = operation;
            let recipe = embedding_execution_recipe(&binding).unwrap();
            let config =
                approved_openai_config(&binding, "fixture-secret-never-serialized".into()).unwrap();
            assert_eq!(recipe.provenance, binding.provenance);
            assert_eq!(recipe.partition, binding.vector_partition().unwrap());
            assert_eq!(recipe.max_total_http_attempts, Some(3));
            assert_eq!(recipe.max_inputs_per_call, 100);
            assert_eq!(recipe.initial_segment_bytes, Some(2048));
            assert_eq!(recipe.max_split_depth, Some(16));
            assert_eq!(config.max_retries, recipe.max_total_http_attempts.unwrap());
            assert_eq!(config.max_batch_size, recipe.max_inputs_per_call);
            assert_eq!(config.preprocessing, recipe.partition.preprocessing);
            assert_eq!(config.model, recipe.partition.model);
            assert_eq!(config.dimensions, recipe.partition.dimensions);
            assert_eq!(config.base_url, "https://example.test/v1");
            let serialized = serde_json::to_string(&recipe).unwrap();
            assert!(!serialized.contains("fixture-secret"));
            assert!(!serialized.contains("RECIPE_SECRET"));
        }
        binding.profile.preprocessing = "text-v1".into();
        let recipe = embedding_execution_recipe(&binding).unwrap();
        assert_eq!(recipe.initial_segment_bytes, None);
        assert_eq!(recipe.max_split_depth, None);
        binding.connection.provider = "fastembed".into();
        assert_eq!(
            embedding_execution_recipe(&binding)
                .unwrap()
                .max_total_http_attempts,
            None
        );
        assert_eq!(
            embedding_execution_recipe(&binding)
                .unwrap()
                .max_inputs_per_call,
            128
        );
        assert!(approved_openai_config(&binding, String::new()).is_err());
        binding.profile.preprocessing = "unknown".into();
        assert!(embedding_execution_recipe(&binding).is_err());
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
