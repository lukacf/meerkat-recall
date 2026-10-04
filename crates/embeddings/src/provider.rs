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
                            BatchError::Provider(EmbeddingError::ProviderError(format!(
                                "Failed to parse response: {e}"
                            )))
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
                        if is_request_too_large(&message) {
                            return Err(BatchError::RequestTooLarge { message });
                        }
                        if is_input_too_long(&message) {
                            return Err(BatchError::InputTooLong {
                                index: input_index_in_error(&message),
                                message,
                            });
                        }
                        return Err(BatchError::Provider(EmbeddingError::ProviderError(
                            format!("OpenAI API error ({status}): {message}"),
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

        Err(BatchError::Provider(last_error.unwrap_or_else(|| {
            EmbeddingError::ProviderError("Max retries exceeded".to_string())
        })))
    }

    /// Embeds one batch, shortening any input the API rejects as too long.
    ///
    /// Inputs are capped at [`MAX_INPUT_CHARS`] up front. If the API still
    /// reports an input over its token limit, that input is halved and the
    /// batch retried, so one oversized node cannot fail every batch it lands
    /// in. Only the beginning of an oversized input is embedded. A request
    /// over the per-request token limit is split in half.
    async fn embed_batch_fitting(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        let mut inputs: Vec<String> = texts
            .iter()
            .map(|text| truncate_chars(text, MAX_INPUT_CHARS).to_string())
            .collect();
        for _ in 0..MAX_SHRINK_ROUNDS {
            match self.embed_batch(&inputs).await {
                Ok(embeddings) => return Ok(embeddings),
                Err(BatchError::Provider(err)) => return Err(err),
                Err(BatchError::RequestTooLarge { message }) => {
                    if inputs.len() < 2 {
                        return Err(EmbeddingError::ProviderError(format!(
                            "OpenAI API error (400 Bad Request): {message}"
                        )));
                    }
                    // Too many tokens in one request: embed the halves separately.
                    let (first, second) = inputs.split_at(inputs.len() / 2);
                    let mut embeddings = Box::pin(self.embed_batch_fitting(first)).await?;
                    embeddings.extend(Box::pin(self.embed_batch_fitting(second)).await?);
                    return Ok(embeddings);
                }
                Err(BatchError::InputTooLong { index, message }) => {
                    let Some(target) = index
                        .filter(|index| *index < inputs.len())
                        .or_else(|| longest_input(&inputs))
                    else {
                        return Err(EmbeddingError::ProviderError(format!(
                            "OpenAI API error (400 Bad Request): {message}"
                        )));
                    };
                    let current = inputs[target].chars().count();
                    if current <= 1 {
                        return Err(EmbeddingError::ProviderError(format!(
                            "OpenAI API error (400 Bad Request): {message}"
                        )));
                    }
                    warn!(
                        "Embedding input {target} exceeds the model limit at {current} chars; \
                         retrying with the first {} chars",
                        current / 2
                    );
                    inputs[target] = truncate_chars(&inputs[target], current / 2).to_string();
                }
            }
        }
        Err(EmbeddingError::ProviderError(
            "OpenAI embedding input still too long after shortening".to_string(),
        ))
    }
}

/// Character cap applied to every `OpenAI` embedding input before sending.
///
/// `text-embedding-3-*` accepts at most 8,192 tokens per input. Ordinary
/// prose runs about four characters per token, so this cap keeps typical
/// inputs inside the limit; dense text (base64, URLs, CJK) can still exceed
/// it and is halved on the API's rejection.
const MAX_INPUT_CHARS: usize = 24_000;

/// Times one batch may be retried with a shortened input.
const MAX_SHRINK_ROUNDS: usize = 16;

/// Why one `OpenAI` batch request failed.
enum BatchError {
    /// The API rejected an input as longer than the model's token limit.
    InputTooLong {
        /// Index of the rejected input, when the error names one.
        index: Option<usize>,
        /// The API's error message.
        message: String,
    },
    /// The API rejected the whole request as over its per-request token limit.
    RequestTooLarge {
        /// The API's error message.
        message: String,
    },
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

/// Index of the longest input, by characters.
fn longest_input(inputs: &[String]) -> Option<usize> {
    inputs
        .iter()
        .enumerate()
        .max_by_key(|(_, input)| input.chars().count())
        .map(|(index, _)| index)
}

/// The first `max_chars` characters of `text`, cut on a char boundary.
fn truncate_chars(text: &str, max_chars: usize) -> &str {
    text.char_indices()
        .nth(max_chars)
        .map_or(text, |(byte_index, _)| &text[..byte_index])
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
            let embeddings = self.embed_batch_fitting(chunk).await?;
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
                        let mut chunk = [0u8; 65536];
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
                        let (status, payload) = if total > request_limit {
                            (
                                "400 Bad Request",
                                serde_json::json!({"error": {"message": format!(
                                    "Requested {total} tokens, max 300000 tokens per request"
                                )}}),
                            )
                        } else if let Some(index) =
                            lengths.iter().position(|length| *length > input_limit)
                        {
                            (
                                "400 Bad Request",
                                serde_json::json!({"error": {"message": format!(
                                    "Invalid 'input[{index}]': maximum input length is 8192 tokens."
                                )}}),
                            )
                        } else {
                            let data: Vec<serde_json::Value> = (0..lengths.len())
                                .map(|index| serde_json::json!({"index": index, "embedding": [0.5, 0.5]}))
                                .collect();
                            (
                                "200 OK",
                                serde_json::json!({"data": data, "usage": {"total_tokens": total}}),
                            )
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
    async fn test_openai_provider_shortens_input_over_the_model_limit() {
        let (base_url, calls) = fake_openai(10_000, usize::MAX).await;
        let provider = OpenAIEmbeddingProvider::new(fake_provider_config(&base_url));
        let texts = vec![
            "short".to_string(),
            "a".repeat(30_000),
            "also short".to_string(),
        ];

        let embeddings = provider.embed(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 3);
        // Capped to 24,000 up front, then halved on each rejection of input 1.
        let calls = calls.lock().unwrap().clone();
        let lengths: Vec<usize> = calls.iter().map(|call| call[1]).collect();
        assert_eq!(lengths, vec![24_000, 12_000, 6_000]);
        assert!(calls.iter().all(|call| call[0] == 5 && call[2] == 10));
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
    fn test_input_index_in_error_and_truncate_chars() {
        assert_eq!(
            input_index_in_error("Invalid 'input[53]': maximum input length is 8192 tokens."),
            Some(53)
        );
        assert_eq!(
            input_index_in_error("maximum input length is 8192 tokens."),
            None
        );
        assert_eq!(truncate_chars("åäö-text", 3), "åäö");
        assert_eq!(truncate_chars("ab", 5), "ab");
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
