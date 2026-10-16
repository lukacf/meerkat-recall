//! Pipeline worker configuration.

use elephant_pipeline::scheduler::DocTreeConfig;
use elephant_storage::StorageBackendKind;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

/// Pipeline worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Database URL.
    #[serde(default = "default_db_url")]
    pub db_url: String,
    /// Database namespace.
    #[serde(default = "default_db_namespace")]
    pub db_namespace: String,
    /// Database name.
    #[serde(default = "default_db_database")]
    pub db_database: String,
    /// Database username.
    #[serde(default = "default_db_user")]
    pub db_user: String,
    /// Database password.
    #[serde(default = "default_db_pass")]
    pub db_pass: String,
    /// Storage backend selector.
    #[serde(default = "default_db_backend")]
    pub db_backend: StorageBackendKind,
    /// Optional snapshot file to load for embedded mode.
    #[serde(default)]
    pub db_snapshot_load: Option<PathBuf>,
    /// Embed worker settings.
    #[serde(default)]
    pub embed: EmbedWorkerConfig,
    /// Promote worker settings.
    #[serde(default)]
    pub promote: PromoteWorkerConfig,
    /// Retention worker settings.
    #[serde(default)]
    pub retention: RetentionWorkerConfig,
    /// Truth maintenance worker settings.
    #[serde(default)]
    pub truth_maintenance: TruthMaintenanceWorkerConfig,
    /// Work queue scheduler settings.
    #[serde(default)]
    pub work_queue: WorkQueueWorkerConfig,
    /// Maintenance worker settings (identity resolution, predicate hygiene, entity type consolidation).
    #[serde(default)]
    pub maintenance: MaintenanceWorkerConfig,
    /// Multimodal render settings for image/video-backed docs.
    #[serde(default)]
    pub multimodal: MultimodalRenderConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            db_url: default_db_url(),
            db_namespace: default_db_namespace(),
            db_database: default_db_database(),
            db_user: default_db_user(),
            db_pass: default_db_pass(),
            db_backend: default_db_backend(),
            db_snapshot_load: None,
            embed: EmbedWorkerConfig::default(),
            promote: PromoteWorkerConfig::default(),
            retention: RetentionWorkerConfig::default(),
            truth_maintenance: TruthMaintenanceWorkerConfig::default(),
            work_queue: WorkQueueWorkerConfig::default(),
            maintenance: MaintenanceWorkerConfig::default(),
            multimodal: MultimodalRenderConfig::default(),
        }
    }
}

impl PipelineConfig {
    /// Load configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when `ELEPHANT_DB` is set to an invalid backend name.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            db_url: env::var("SURREALDB_URL").unwrap_or_else(|_| default_db_url()),
            db_namespace: env::var("SURREALDB_NAMESPACE")
                .unwrap_or_else(|_| default_db_namespace()),
            db_database: env::var("SURREALDB_DATABASE").unwrap_or_else(|_| default_db_database()),
            db_user: env::var("SURREALDB_USER").unwrap_or_else(|_| default_db_user()),
            db_pass: env::var("SURREALDB_PASS").unwrap_or_else(|_| default_db_pass()),
            db_backend: backend_from_env()?,
            db_snapshot_load: env::var("ELEPHANT_DB_SNAPSHOT_LOAD")
                .ok()
                .map(PathBuf::from),
            embed: EmbedWorkerConfig::from_env(),
            promote: PromoteWorkerConfig::from_env(),
            retention: RetentionWorkerConfig::from_env(),
            truth_maintenance: TruthMaintenanceWorkerConfig::from_env(),
            work_queue: WorkQueueWorkerConfig::from_env(),
            maintenance: MaintenanceWorkerConfig::from_env(),
            multimodal: MultimodalRenderConfig::from_env(),
        })
    }
}

fn backend_from_env() -> anyhow::Result<StorageBackendKind> {
    env::var("ELEPHANT_DB").map_or_else(
        |_| Ok(default_db_backend()),
        |value| {
            value
                .parse()
                .map_err(|err| anyhow::anyhow!("Invalid ELEPHANT_DB value `{value}`: {err}"))
        },
    )
}

const fn default_db_backend() -> StorageBackendKind {
    StorageBackendKind::SurrealDb
}

fn default_embed_provider() -> String {
    // Never "fake": see elephant_embeddings::policy::default_provider.
    elephant_embeddings::policy::default_provider()
}

/// Embed worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedWorkerConfig {
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Batch size for embedding requests.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Embedding provider ("fake", "openai", or "fastembed").
    #[serde(default = "default_embed_provider")]
    pub provider: String,
    /// Embedding model name.
    #[serde(default = "default_embed_model")]
    pub model: String,
    /// Embedding dimensions.
    #[serde(default = "default_embed_dims")]
    pub dims: usize,
}

impl Default for EmbedWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: default_poll_interval(),
            batch_size: default_batch_size(),
            provider: default_embed_provider(),
            model: default_embed_model(),
            dims: default_embed_dims(),
        }
    }
}

impl EmbedWorkerConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PIPELINE_EMBED_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_EMBED_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_poll_interval),
            batch_size: env::var("PIPELINE_EMBED_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
            provider: env::var("PIPELINE_EMBED_PROVIDER")
                .unwrap_or_else(|_| default_embed_provider()),
            model: env::var("PIPELINE_EMBED_MODEL").unwrap_or_else(|_| default_embed_model()),
            dims: env::var("PIPELINE_EMBED_DIMS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_embed_dims),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}

/// Promote worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteWorkerConfig {
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Confidence threshold for auto-promotion.
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,
}

impl Default for PromoteWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: default_poll_interval(),
            confidence_threshold: default_confidence_threshold(),
        }
    }
}

#[allow(dead_code)]
impl PromoteWorkerConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PIPELINE_PROMOTE_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_PROMOTE_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_poll_interval),
            confidence_threshold: env::var("PIPELINE_PROMOTE_CONFIDENCE_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_confidence_threshold),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}

/// Retention worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionWorkerConfig {
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds (default: 3600 = 1 hour).
    #[serde(default = "default_retention_poll_interval")]
    pub poll_interval_secs: u64,
    /// Retention period in days (default: 30).
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
    /// Independent work-history poll interval in seconds (default: 60).
    #[serde(default = "default_work_history_poll_interval")]
    pub work_history_poll_interval_secs: u64,
    /// Maximum items and attempts archived per pass (default: 1000 each).
    #[serde(default = "default_work_history_batch_size")]
    pub work_history_batch_size: usize,
}

impl Default for RetentionWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: default_retention_poll_interval(),
            retention_days: default_retention_days(),
            work_history_poll_interval_secs: default_work_history_poll_interval(),
            work_history_batch_size: default_work_history_batch_size(),
        }
    }
}

impl RetentionWorkerConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PIPELINE_RETENTION_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_RETENTION_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_retention_poll_interval),
            retention_days: env::var("PIPELINE_RETENTION_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_retention_days),
            work_history_poll_interval_secs: env::var("PIPELINE_WORK_HISTORY_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_work_history_poll_interval),
            work_history_batch_size: env::var("PIPELINE_WORK_HISTORY_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_work_history_batch_size),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    /// Get retention period as Duration.
    #[must_use]
    pub const fn retention_period(&self) -> Duration {
        Duration::from_secs(self.retention_days * 24 * 3600)
    }

    /// Keep archival polling finite and avoid accidental busy loops.
    #[must_use]
    pub fn work_history_poll_interval(&self) -> Duration {
        Duration::from_secs(self.work_history_poll_interval_secs.clamp(1, 86_400))
    }

    /// Bound each pass even when loaded from an external configuration file.
    #[must_use]
    pub fn work_history_batch_size(&self) -> usize {
        self.work_history_batch_size.clamp(1, 1000)
    }
}

/// Work queue worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueWorkerConfig {
    /// Exact tree recipe retained by this worker and used by each render dispatch.
    /// Capture this instance for diagnostics instead of reconstructing defaults.
    #[serde(default)]
    pub doc_tree: DocTreeConfig,
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Lease duration in seconds.
    #[serde(default = "default_lease_duration_secs")]
    pub lease_duration_secs: i64,
    /// Maximum items to fetch in a single poll.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional override for the runnable candidate scan window. Workers that
    /// claim every kind can set this low (e.g. a few hundred) to keep
    /// deep-queue scans cheap; leave unset for selector-filtered workers.
    #[serde(default)]
    pub scan_limit: Option<usize>,
    /// Optional worker identifier override.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// If non-empty, only work kinds in this list are claimable by this worker.
    #[serde(default)]
    pub allowed_kinds: Vec<String>,
    /// Work kinds that this worker should leave runnable for another worker.
    #[serde(default)]
    pub disabled_kinds: Vec<String>,
    /// Whether LLM disambiguation is enabled for resolution.
    /// Defaults to true per resolution redesign - disambiguation is always tried for multiple candidates.
    #[serde(default = "default_true")]
    pub disambiguation_enabled: bool,
    /// LLM model for disambiguation.
    #[serde(default = "default_disambiguation_model")]
    pub disambiguation_model: String,
    /// Max tokens for disambiguation calls.
    #[serde(default = "default_disambiguation_max_tokens")]
    pub disambiguation_max_tokens: usize,
    /// Max wall time in seconds for disambiguation calls.
    #[serde(default = "default_disambiguation_max_wall_time_secs")]
    pub disambiguation_max_wall_time_secs: u64,
    /// Maximum concurrent work items to process in parallel.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Maximum concurrent `extract_assertions` work items.
    #[serde(default = "default_max_extract_concurrent")]
    pub max_extract_concurrent: usize,
    /// Hard timeout in seconds for the full lifecycle of a claimed work item.
    #[serde(default = "default_work_item_timeout_secs")]
    pub work_item_timeout_secs: u64,
    /// Maximum attempts for transient failures before permanent failure.
    #[serde(default = "default_transient_retry_max_attempts")]
    pub transient_retry_max_attempts: u32,
    /// Base delay for transient retry backoff.
    #[serde(default = "default_transient_retry_base_delay_secs")]
    pub transient_retry_base_delay_secs: i64,
    /// Maximum delay for transient retry backoff.
    #[serde(default = "default_transient_retry_max_delay_secs")]
    pub transient_retry_max_delay_secs: i64,
}

impl Default for WorkQueueWorkerConfig {
    fn default() -> Self {
        Self {
            doc_tree: DocTreeConfig::default(),
            enabled: default_true(),
            poll_interval_secs: default_poll_interval(),
            lease_duration_secs: default_lease_duration_secs(),
            batch_size: default_batch_size(),
            scan_limit: None,
            worker_id: None,
            allowed_kinds: Vec::new(),
            disabled_kinds: Vec::new(),
            max_concurrent: default_max_concurrent(),
            max_extract_concurrent: default_max_extract_concurrent(),
            work_item_timeout_secs: default_work_item_timeout_secs(),
            transient_retry_max_attempts: default_transient_retry_max_attempts(),
            transient_retry_base_delay_secs: default_transient_retry_base_delay_secs(),
            transient_retry_max_delay_secs: default_transient_retry_max_delay_secs(),
            disambiguation_enabled: default_true(), // Changed per resolution redesign
            disambiguation_model: default_disambiguation_model(),
            disambiguation_max_tokens: default_disambiguation_max_tokens(),
            disambiguation_max_wall_time_secs: default_disambiguation_max_wall_time_secs(),
        }
    }
}

impl WorkQueueWorkerConfig {
    /// Load from environment once, including the exact tree recipe.
    #[must_use]
    pub fn from_env() -> Self {
        let mut doc_tree = DocTreeConfig::default();
        if let Ok(model) = env::var("PIPELINE_SUMMARIZER_MODEL") {
            if !model.trim().is_empty() {
                doc_tree.summarizer_model = model;
            }
        }
        Self {
            doc_tree,
            enabled: env::var("PIPELINE_WORK_QUEUE_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_WORK_QUEUE_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_poll_interval),
            lease_duration_secs: env::var("PIPELINE_WORK_QUEUE_LEASE_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_lease_duration_secs),
            batch_size: env::var("PIPELINE_WORK_QUEUE_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
            scan_limit: env::var("PIPELINE_WORK_QUEUE_SCAN_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok()),
            worker_id: env::var("PIPELINE_WORK_QUEUE_WORKER_ID").ok(),
            allowed_kinds: env_list("PIPELINE_WORK_QUEUE_ALLOWED_KINDS"),
            disabled_kinds: env_list("PIPELINE_WORK_QUEUE_DISABLED_KINDS"),
            max_concurrent: env::var("PIPELINE_MAX_CONCURRENT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_concurrent),
            max_extract_concurrent: env::var("PIPELINE_MAX_EXTRACT_CONCURRENT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_extract_concurrent),
            work_item_timeout_secs: env::var("PIPELINE_WORK_ITEM_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_work_item_timeout_secs),
            transient_retry_max_attempts: env::var("PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_transient_retry_max_attempts),
            transient_retry_base_delay_secs: env::var(
                "PIPELINE_WORK_TRANSIENT_RETRY_BASE_DELAY_SECS",
            )
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_transient_retry_base_delay_secs),
            transient_retry_max_delay_secs: env::var(
                "PIPELINE_WORK_TRANSIENT_RETRY_MAX_DELAY_SECS",
            )
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_transient_retry_max_delay_secs),
            disambiguation_enabled: env::var("PIPELINE_RESOLUTION_DISAMBIGUATION_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            disambiguation_model: env::var("PIPELINE_RESOLUTION_DISAMBIGUATION_MODEL")
                .unwrap_or_else(|_| default_disambiguation_model()),
            disambiguation_max_tokens: env::var("PIPELINE_RESOLUTION_DISAMBIGUATION_MAX_TOKENS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_disambiguation_max_tokens),
            disambiguation_max_wall_time_secs: env::var(
                "PIPELINE_RESOLUTION_DISAMBIGUATION_TIMEOUT_SECS",
            )
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(default_disambiguation_max_wall_time_secs),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    /// Return whether this worker should claim a work item of the given kind.
    #[must_use]
    pub fn allows_kind(&self, kind: &str) -> bool {
        if self
            .disabled_kinds
            .iter()
            .any(|candidate| candidate == kind)
        {
            return false;
        }
        self.allowed_kinds.is_empty()
            || self.allowed_kinds.iter().any(|candidate| candidate == kind)
    }
}

/// Truth maintenance worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruthMaintenanceWorkerConfig {
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Maximum events per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl Default for TruthMaintenanceWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: default_poll_interval(),
            batch_size: default_batch_size(),
        }
    }
}

impl TruthMaintenanceWorkerConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PIPELINE_TRUTH_MAINTENANCE_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_TRUTH_MAINTENANCE_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_poll_interval),
            batch_size: env::var("PIPELINE_TRUTH_MAINTENANCE_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}

// Default value functions

fn default_db_url() -> String {
    "ws://localhost:8000".to_string()
}

fn default_db_namespace() -> String {
    "elephant".to_string()
}

fn default_db_database() -> String {
    "test".to_string()
}

fn default_db_user() -> String {
    "root".to_string()
}

fn default_db_pass() -> String {
    "root".to_string()
}

const fn default_true() -> bool {
    true
}

const fn default_poll_interval() -> u64 {
    5
}

fn env_list(name: &str) -> Vec<String> {
    env::var(name).map_or_else(
        |_| Vec::new(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect()
        },
    )
}

fn default_disambiguation_model() -> String {
    "claude-sonnet-5-5".to_string()
}

const fn default_disambiguation_max_tokens() -> usize {
    800
}

const fn default_disambiguation_max_wall_time_secs() -> u64 {
    15
}

const fn default_batch_size() -> usize {
    100
}

const fn default_lease_duration_secs() -> i64 {
    300
}

const fn default_max_concurrent() -> usize {
    8
}

const fn default_max_extract_concurrent() -> usize {
    4
}

const fn default_work_item_timeout_secs() -> u64 {
    420
}

// Concurrent source stages can conflict repeatedly on the same canonical epoch.
// Retain finite retries and the existing capped backoff; explicit operators can
// still select a smaller budget. This does not change semantic repair allowance.
const fn default_transient_retry_max_attempts() -> u32 {
    10
}

const fn default_transient_retry_base_delay_secs() -> i64 {
    10
}

const fn default_transient_retry_max_delay_secs() -> i64 {
    120
}

fn default_embed_model() -> String {
    "text-embedding-3-large".to_string()
}

const fn default_embed_dims() -> usize {
    3072
}

const fn default_confidence_threshold() -> f64 {
    0.90
}

const fn default_retention_poll_interval() -> u64 {
    3600 // 1 hour
}

const fn default_retention_days() -> u64 {
    30
}

const fn default_work_history_poll_interval() -> u64 {
    60
}

const fn default_work_history_batch_size() -> usize {
    1000
}

fn default_maintenance_mcp_url() -> String {
    "http://127.0.0.1:3000/mcp/maintenance".to_string()
}

fn default_multimodal_render_model() -> String {
    "gpt-6.1-sol".to_string()
}

const fn default_multimodal_render_timeout_secs() -> u64 {
    300
}

const fn default_multimodal_render_max_concurrent() -> usize {
    4
}

fn default_multimodal_asset_root() -> PathBuf {
    std::env::temp_dir().join("elephant-media-assets")
}

const fn default_multimodal_inline_image_max_bytes() -> u64 {
    64 * 1024 * 1024
}

const fn default_multimodal_inline_video_max_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_maintenance_model() -> String {
    "gpt-6.1-sol".to_string()
}

const fn default_maintenance_poll_interval() -> u64 {
    300 // 5 minutes
}

const fn default_auto_merge_threshold() -> f64 {
    0.9
}

const fn default_max_candidates_per_sweep() -> usize {
    50
}

/// Maintenance worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceWorkerConfig {
    /// Whether the worker is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Poll interval in seconds.
    #[serde(default = "default_maintenance_poll_interval")]
    pub poll_interval_secs: u64,
    /// MCP URL for the maintenance agent profile.
    #[serde(default = "default_maintenance_mcp_url")]
    pub mcp_url: String,
    /// LLM model for maintenance decisions.
    #[serde(default = "default_maintenance_model")]
    pub model: String,
    /// Confidence threshold for auto-merging entities.
    #[serde(default = "default_auto_merge_threshold")]
    pub auto_merge_threshold: f64,
    /// Maximum candidate pairs per sweep.
    #[serde(default = "default_max_candidates_per_sweep")]
    pub max_candidates_per_sweep: usize,
}

/// Multimodal render configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalRenderConfig {
    /// Model used for multimodal render projections.
    #[serde(default = "default_multimodal_render_model")]
    pub model: String,
    /// Timeout in seconds for a single render attempt.
    #[serde(default = "default_multimodal_render_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum concurrent multimodal render agent sessions.
    #[serde(default = "default_multimodal_render_max_concurrent")]
    pub max_render_concurrent: usize,
    /// Root directory for managed inline media assets.
    #[serde(default = "default_multimodal_asset_root")]
    pub asset_root: PathBuf,
    /// Maximum inline image payload size in bytes.
    #[serde(default = "default_multimodal_inline_image_max_bytes")]
    pub inline_image_max_bytes: u64,
    /// Maximum inline video payload size in bytes.
    #[serde(default = "default_multimodal_inline_video_max_bytes")]
    pub inline_video_max_bytes: u64,
}

impl Default for MultimodalRenderConfig {
    fn default() -> Self {
        Self {
            model: default_multimodal_render_model(),
            timeout_secs: default_multimodal_render_timeout_secs(),
            max_render_concurrent: default_multimodal_render_max_concurrent(),
            asset_root: default_multimodal_asset_root(),
            inline_image_max_bytes: default_multimodal_inline_image_max_bytes(),
            inline_video_max_bytes: default_multimodal_inline_video_max_bytes(),
        }
    }
}

impl MultimodalRenderConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            model: env::var("PIPELINE_MULTIMODAL_RENDER_MODEL")
                .unwrap_or_else(|_| default_multimodal_render_model()),
            timeout_secs: env::var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_multimodal_render_timeout_secs),
            max_render_concurrent: env::var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_multimodal_render_max_concurrent),
            asset_root: env::var("PIPELINE_MULTIMODAL_ASSET_ROOT")
                .map_or_else(|_| default_multimodal_asset_root(), PathBuf::from),
            inline_image_max_bytes: env::var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_multimodal_inline_image_max_bytes),
            inline_video_max_bytes: env::var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_multimodal_inline_video_max_bytes),
        }
    }
}

impl Default for MaintenanceWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: default_maintenance_poll_interval(),
            mcp_url: default_maintenance_mcp_url(),
            model: default_maintenance_model(),
            auto_merge_threshold: default_auto_merge_threshold(),
            max_candidates_per_sweep: default_max_candidates_per_sweep(),
        }
    }
}

impl MaintenanceWorkerConfig {
    /// Load from environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PIPELINE_MAINTENANCE_ENABLED")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(true),
            poll_interval_secs: env::var("PIPELINE_MAINTENANCE_POLL_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_maintenance_poll_interval),
            mcp_url: env::var("PIPELINE_MAINTENANCE_MCP_URL")
                .unwrap_or_else(|_| default_maintenance_mcp_url()),
            model: env::var("PIPELINE_MAINTENANCE_MODEL")
                .unwrap_or_else(|_| default_maintenance_model()),
            auto_merge_threshold: env::var("PIPELINE_MAINTENANCE_AUTO_MERGE_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_auto_merge_threshold),
            max_candidates_per_sweep: env::var("PIPELINE_MAINTENANCE_MAX_CANDIDATES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_candidates_per_sweep),
        }
    }

    /// Get poll interval as Duration.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
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
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn test_default_config() {
        let config = PipelineConfig::default();
        assert!(config.embed.enabled);
        assert!(config.promote.enabled);
        assert_eq!(config.embed.batch_size, 100);
        assert!((config.promote.confidence_threshold - 0.90).abs() < f64::EPSILON);
        assert_eq!(config.multimodal.model, "gpt-6.1-sol");
        assert_eq!(config.maintenance.model, "gpt-6.1-sol");
        assert_eq!(default_disambiguation_model(), "claude-sonnet-5-5");
        assert_eq!(config.embed.model, "text-embedding-3-large");
        assert_eq!(config.embed.dims, 3072);
        assert_eq!(config.multimodal.timeout_secs, 300);
        assert_eq!(config.multimodal.max_render_concurrent, 4);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = PipelineConfig::default();
        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: PipelineConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.db_url, config.db_url);
        assert_eq!(decoded.embed.enabled, config.embed.enabled);
    }

    #[test]
    fn work_history_config_is_backward_compatible_and_bounded() {
        let old = serde_json::json!({ "enabled": true, "poll_interval_secs": 3600, "retention_days": 30 });
        let mut config: RetentionWorkerConfig = serde_json::from_value(old).expect("old config");
        assert_eq!(config.work_history_poll_interval(), Duration::from_secs(60));
        assert_eq!(config.work_history_batch_size(), 1000);
        config.work_history_poll_interval_secs = 0;
        config.work_history_batch_size = usize::MAX;
        assert_eq!(config.work_history_poll_interval(), Duration::from_secs(1));
        assert_eq!(config.work_history_batch_size(), 1000);
        config.work_history_poll_interval_secs = u64::MAX;
        config.work_history_batch_size = 0;
        assert_eq!(
            config.work_history_poll_interval(),
            Duration::from_secs(86_400)
        );
        assert_eq!(config.work_history_batch_size(), 1);
    }

    #[test]
    fn test_work_queue_max_concurrent_default() {
        let config = WorkQueueWorkerConfig::default();
        assert_eq!(config.max_concurrent, 8);
        assert_eq!(config.max_extract_concurrent, 4);
        assert!(
            config.max_concurrent > config.max_extract_concurrent,
            "default queue capacity must leave headroom for lightweight lifecycle work"
        );
        assert_eq!(config.work_item_timeout_secs, 420);
        assert_eq!(config.transient_retry_max_attempts, 10);
        assert_eq!(config.transient_retry_base_delay_secs, 10);
        assert_eq!(config.transient_retry_max_delay_secs, 120);
    }

    #[test]
    fn test_work_queue_max_concurrent_serde() {
        let config = PipelineConfig::default();
        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: PipelineConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.work_queue.max_concurrent, 8);
        assert_eq!(decoded.work_queue.max_extract_concurrent, 4);
        assert_eq!(decoded.work_queue.work_item_timeout_secs, 420);
        assert_eq!(decoded.work_queue.transient_retry_max_attempts, 10);
        assert_eq!(decoded.work_queue.transient_retry_base_delay_secs, 10);
        assert_eq!(decoded.work_queue.transient_retry_max_delay_secs, 120);
    }

    #[test]
    fn transient_retry_default_recovers_contention_but_preserves_explicit_caps() {
        use elephant_pipeline::scheduler::execution_contract::{
            retry_action, FailureClass, RetryAction,
        };
        let config: WorkQueueWorkerConfig =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        for attempt in 1..10 {
            assert!(matches!(
                retry_action(
                    FailureClass::Transient,
                    attempt,
                    config.transient_retry_max_attempts,
                    10,
                    120
                ),
                RetryAction::Retry {
                    delay_secs: 1..=120
                }
            ));
        }
        assert_eq!(
            retry_action(
                FailureClass::Transient,
                10,
                config.transient_retry_max_attempts,
                10,
                120
            ),
            RetryAction::Fail
        );
        let selected: WorkQueueWorkerConfig =
            serde_json::from_value(serde_json::json!({"transient_retry_max_attempts": 2}))
                .expect("explicit cap");
        assert_eq!(
            retry_action(
                FailureClass::Transient,
                2,
                selected.transient_retry_max_attempts,
                10,
                120
            ),
            RetryAction::Fail
        );
        assert_eq!(
            retry_action(
                FailureClass::Terminal,
                1,
                config.transient_retry_max_attempts,
                10,
                120
            ),
            RetryAction::Fail
        );
    }

    #[test]
    fn retained_tree_recipe_roundtrips_without_reconstructing_defaults() {
        let old: WorkQueueWorkerConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(old.doc_tree.max_chunk_size, 512);
        assert!(!old.doc_tree.create_summaries);
        let mut selected = old;
        selected.doc_tree.max_chunk_size = 1024;
        selected.doc_tree.create_paragraphs = false;
        selected.doc_tree.summarizer_model = "explicit-retained-model".into();
        let captured = serde_json::to_value(&selected).unwrap();
        assert_eq!(captured["doc_tree"]["max_chunk_size"], 1024);
        assert_eq!(captured["doc_tree"]["create_paragraphs"], false);
        let restored: WorkQueueWorkerConfig = serde_json::from_value(captured.clone()).unwrap();
        assert_eq!(serde_json::to_value(&restored).unwrap(), captured);
    }

    #[test]
    fn test_work_queue_kind_filters_from_env() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_allowed = env::var("PIPELINE_WORK_QUEUE_ALLOWED_KINDS").ok();
        let prior_disabled = env::var("PIPELINE_WORK_QUEUE_DISABLED_KINDS").ok();

        env::set_var(
            "PIPELINE_WORK_QUEUE_ALLOWED_KINDS",
            "plan_extraction, extract_assertions",
        );
        env::set_var("PIPELINE_WORK_QUEUE_DISABLED_KINDS", "render_doc");

        let config = WorkQueueWorkerConfig::from_env();
        assert!(config.allows_kind("plan_extraction"));
        assert!(config.allows_kind("extract_assertions"));
        assert!(!config.allows_kind("render_doc"));
        assert!(!config.allows_kind("triage_source"));

        match prior_allowed {
            Some(value) => env::set_var("PIPELINE_WORK_QUEUE_ALLOWED_KINDS", value),
            None => env::remove_var("PIPELINE_WORK_QUEUE_ALLOWED_KINDS"),
        }
        match prior_disabled {
            Some(value) => env::set_var("PIPELINE_WORK_QUEUE_DISABLED_KINDS", value),
            None => env::remove_var("PIPELINE_WORK_QUEUE_DISABLED_KINDS"),
        }
    }

    #[test]
    fn test_multimodal_from_env_reads_settings() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_model = env::var("PIPELINE_MULTIMODAL_RENDER_MODEL").ok();
        let prior_timeout = env::var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS").ok();
        let prior_max_concurrent = env::var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT").ok();
        let prior_asset_root = env::var("PIPELINE_MULTIMODAL_ASSET_ROOT").ok();
        let prior_image_limit = env::var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES").ok();
        let prior_video_limit = env::var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES").ok();

        env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", "gemini-3.1-pro-preview");
        env::set_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS", "901");
        env::set_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT", "7");
        env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", "/tmp/elephant-assets");
        env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", "1234");
        env::set_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES", "5678");

        let config = PipelineConfig::from_env().expect("config from env");
        assert_eq!(config.multimodal.model, "gemini-3.1-pro-preview");
        assert_eq!(config.multimodal.timeout_secs, 901);
        assert_eq!(config.multimodal.max_render_concurrent, 7);
        assert_eq!(
            config.multimodal.asset_root,
            PathBuf::from("/tmp/elephant-assets")
        );
        assert_eq!(config.multimodal.inline_image_max_bytes, 1234);
        assert_eq!(config.multimodal.inline_video_max_bytes, 5678);

        match prior_model {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_MODEL"),
        }
        match prior_timeout {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS"),
        }
        match prior_max_concurrent {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT"),
        }
        match prior_asset_root {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_ASSET_ROOT"),
        }
        match prior_image_limit {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES"),
        }
        match prior_video_limit {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES"),
        }
    }

    #[test]
    fn test_backend_from_env_invalid_value_errors() {
        let _guard = env_lock().lock().expect("env lock");
        let prior = env::var("ELEPHANT_DB").ok();
        env::set_var("ELEPHANT_DB", "definitely-not-a-backend");
        let result = backend_from_env();
        match prior {
            Some(value) => env::set_var("ELEPHANT_DB", value),
            None => env::remove_var("ELEPHANT_DB"),
        }
        let error = result.expect_err("invalid backend should error");
        assert!(error
            .to_string()
            .contains("Invalid ELEPHANT_DB value `definitely-not-a-backend`"));
    }
}
