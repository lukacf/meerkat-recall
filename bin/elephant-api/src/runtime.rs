//! Managed runtime configuration and readiness state.

use std::env;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc as StdArc, Mutex as StdMutex};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use elephant_pipeline::scheduler::{BudgetEnforcer, WorkQueue};
use elephant_pipeline_bin::config::PipelineConfig;
use elephant_pipeline_bin::run_pipeline_with_shutdown;
use elephant_storage::{StorageBackendKind, StorageClient, StorageConfig};
use elephant_types::records::ingest::{Budget, DowngradePolicy};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};

#[cfg(feature = "meerkat-extraction")]
use elephant_pipeline::extraction::{AgenticExtractor, AgenticExtractorConfig, Extractor};

const CONFIG_DIR: &str = ".elephant";
const CONFIG_FILE: &str = "elephant_config.toml";
const DEFAULT_SNAPSHOT_PATH: &str = ".elephant/state.snapshot";
// gpt-5.6-sol requires meerkat >= 0.8.11 (first catalog that carries it);
// on older pins agent builds fail with "Cannot infer provider from model".
// crates/pipeline/tests/model_catalog_pin.rs pins that resolution. Vision
// capable (text+image input), 1.05M context, 128k max output - safe for both
// the agentic extractor and the multimodal renderer.
const DEFAULT_AGENTIC_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_MULTIMODAL_RENDER_MODEL: &str = "gpt-5.6-sol";
const DEFAULT_MULTIMODAL_RENDER_TIMEOUT_SECS: u64 = 300;
const DEFAULT_MULTIMODAL_INLINE_IMAGE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MULTIMODAL_INLINE_VIDEO_MAX_BYTES: u64 = 512 * 1024 * 1024;
const INITIAL_EXTRACTOR_RETRY_ATTEMPTS: usize = 5;
const INITIAL_EXTRACTOR_RETRY_DELAY: Duration = Duration::from_secs(1);
const SUSTAINED_EXTRACTOR_RETRY_DELAY: Duration = Duration::from_secs(5);
const RETRY_LOG_INTERVAL: usize = 12;
const ISSUE_SEMANTIC_EXTRACTOR_UNAVAILABLE: &str = "semantic_extractor_unavailable";
const ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE: &str = "semantic_extractor_feature_unavailable";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    LocalMemory,
    SharedMemory,
    ReadOnly,
    Maintenance,
}

impl RuntimeMode {
    const fn is_write_capable(self) -> bool {
        !matches!(self, Self::ReadOnly)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElephantConfig {
    pub runtime: RuntimeSection,
    pub storage: StorageSection,
    pub extractor: ExtractorSection,
    #[serde(default)]
    pub multimodal: MultimodalSection,
    #[serde(default)]
    pub budget: Budget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSection {
    pub mode: RuntimeMode,
    pub ingest_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    pub backend: StorageBackendKind,
    #[serde(default)]
    pub elephant_db: EmbeddedStorageSection,
    #[serde(default)]
    pub surreal_db: SurrealStorageSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedStorageSection {
    pub snapshot_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurrealStorageSection {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
    #[serde(default = "default_database_timeout_secs")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_database_timeout_secs")]
    pub transaction_timeout_secs: u64,
}

const fn default_database_timeout_secs() -> u64 {
    elephant_storage::client::DEFAULT_DATABASE_TIMEOUT_SECS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSection {
    pub provider: String,
    pub required: bool,
    pub mcp_url: String,
    pub model: String,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalSection {
    pub model: String,
    pub timeout_secs: u64,
    pub max_render_concurrent: usize,
    pub asset_root: PathBuf,
    pub inline_image_max_bytes: u64,
    pub inline_video_max_bytes: u64,
}

impl Default for ElephantConfig {
    fn default() -> Self {
        Self {
            runtime: RuntimeSection {
                mode: RuntimeMode::LocalMemory,
                ingest_enabled: true,
            },
            storage: StorageSection {
                backend: StorageBackendKind::ElephantDb,
                elephant_db: EmbeddedStorageSection {
                    snapshot_path: PathBuf::from(DEFAULT_SNAPSHOT_PATH),
                },
                surreal_db: SurrealStorageSection::default(),
            },
            extractor: ExtractorSection::default(),
            multimodal: MultimodalSection::default(),
            budget: Budget::default(),
        }
    }
}

impl Default for EmbeddedStorageSection {
    fn default() -> Self {
        Self {
            snapshot_path: PathBuf::from(DEFAULT_SNAPSHOT_PATH),
        }
    }
}

impl Default for SurrealStorageSection {
    fn default() -> Self {
        Self {
            url: "ws://localhost:8000".to_string(),
            namespace: "elephant".to_string(),
            database: "test".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            query_timeout_secs: default_database_timeout_secs(),
            transaction_timeout_secs: default_database_timeout_secs(),
        }
    }
}

impl Default for ExtractorSection {
    fn default() -> Self {
        Self {
            provider: "meerkat-agentic".to_string(),
            required: true,
            mcp_url: "http://127.0.0.1:3000/mcp/agent".to_string(),
            model: DEFAULT_AGENTIC_MODEL.to_string(),
            timeout_secs: 30,
        }
    }
}

impl Default for MultimodalSection {
    fn default() -> Self {
        Self {
            model: DEFAULT_MULTIMODAL_RENDER_MODEL.to_string(),
            timeout_secs: DEFAULT_MULTIMODAL_RENDER_TIMEOUT_SECS,
            max_render_concurrent: 4,
            asset_root: std::env::temp_dir().join("elephant-media-assets"),
            inline_image_max_bytes: DEFAULT_MULTIMODAL_INLINE_IMAGE_MAX_BYTES,
            inline_video_max_bytes: DEFAULT_MULTIMODAL_INLINE_VIDEO_MAX_BYTES,
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub configured: bool,
    pub bootstrap_mode: bool,
    pub runtime_mode: RuntimeMode,
    pub backend: Option<String>,
    pub storage_ready: bool,
    pub worker_ready: bool,
    pub extractor_ready: bool,
    pub semantic_ingest_ready: bool,
    /// Whether the derivation conveyor can currently make progress.
    ///
    /// `None` before storage is up. A conveyor that has stopped is otherwise
    /// invisible here: readiness stays green because every component is alive,
    /// and throughput is not reported at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conveyor: Option<ConveyorStatus>,
    pub issues: Vec<String>,
}

/// Whether derivation work can actually flow, and if not, why.
///
/// The distinction that matters is between an EMPTY queue and a BLOCKED one.
/// Both produce zero assertions per hour, but only one is a problem, and
/// conflating them has cost real time here: the 2026-04-11 to 2026-07-30 KB gap
/// was investigated as a dead worker when the queue simply had nothing in it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConveyorStatus {
    /// Items runnable right now, saturating at the queue's scan window.
    pub runnable_items: usize,
    /// Remaining euro headroom today for retrieval-mode work (triage, embed).
    pub remaining_euros_retrieval: f64,
    /// Remaining euro headroom today for graph-mode work (extraction).
    pub remaining_euros_graph: f64,
    /// True when there is work waiting that cannot start.
    pub blocked: bool,
    /// Machine-readable cause when `blocked`, e.g. `budget_exhausted:graph`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    /// Embedding progress of the default space, from a background count
    /// refreshed at most every [`EMBEDDING_PROGRESS_TTL`]. `None` until the
    /// first count completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<EmbeddingProgress>,
}

/// How many embeddable nodes are embedded and how many still wait.
///
/// `pending_nodes + embedded_nodes == embeddable_nodes`; `counted_at` says how
/// old the numbers are. Two readings minutes apart show whether embedding is
/// moving.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingProgress {
    /// Space the counts are for.
    pub space_id: String,
    /// Embeddable nodes still waiting for an embedding.
    pub pending_nodes: usize,
    /// Embeddable nodes that have one.
    pub embedded_nodes: usize,
    /// The denominator: pending plus embedded.
    pub embeddable_nodes: usize,
    /// When the counts were taken (RFC 3339, UTC).
    pub counted_at: String,
}

/// How often the embedding progress counts are recomputed.
///
/// Counting walks the whole `doc_node` table, so it runs in a background task
/// and status calls only ever read the last result.
const EMBEDDING_PROGRESS_TTL: Duration = Duration::from_secs(300);

/// Last embedding progress count, plus a flag so only one count runs at a time.
#[derive(Debug, Default)]
struct EmbeddingProgressCache {
    latest: StdArc<StdMutex<Option<(std::time::Instant, EmbeddingProgress)>>>,
    refreshing: StdArc<AtomicBool>,
}

/// Decides whether a stalled conveyor is a fault, and names the cause.
///
/// The judgement that matters is between an EMPTY queue and a BLOCKED one. Both
/// show zero throughput, but only one needs attention, and conflating them is
/// how the 2026-04-11 to 2026-07-30 KB gap got investigated as a dead worker
/// when the queue simply had nothing in it.
///
/// So exhausted budget alone is NOT a fault. A quiet day that happens to have
/// spent its budget is fine; reporting that red would teach everyone to ignore
/// this field, which is the failure mode of every green-when-broken surface we
/// already have. It is only a fault when work is waiting that cannot start.
fn classify_conveyor_block(
    runnable_items: usize,
    remaining_euros_graph: f64,
    remaining_euros_retrieval: f64,
) -> Option<String> {
    if runnable_items == 0 {
        return None;
    }

    match (
        remaining_euros_graph <= 0.0,
        remaining_euros_retrieval <= 0.0,
    ) {
        (true, true) => Some("budget_exhausted:graph+retrieval".to_string()),
        (true, false) => Some("budget_exhausted:graph".to_string()),
        (false, true) => Some("budget_exhausted:retrieval".to_string()),
        (false, false) => None,
    }
}

impl RuntimeStatus {
    fn bootstrap() -> Self {
        Self {
            configured: false,
            bootstrap_mode: true,
            runtime_mode: RuntimeMode::LocalMemory,
            backend: None,
            storage_ready: false,
            worker_ready: false,
            extractor_ready: false,
            semantic_ingest_ready: false,
            conveyor: None,
            issues: vec!["runtime_not_configured".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfigPatch {
    pub runtime_mode: Option<RuntimeMode>,
    pub ingest_enabled: Option<bool>,
    pub backend: Option<StorageBackendKind>,
    pub snapshot_path: Option<PathBuf>,
    pub surreal_url: Option<String>,
    pub surreal_namespace: Option<String>,
    pub surreal_database: Option<String>,
    pub surreal_username: Option<String>,
    pub surreal_password: Option<String>,
    pub extractor_provider: Option<String>,
    pub extractor_required: Option<bool>,
    pub extractor_mcp_url: Option<String>,
    pub extractor_model: Option<String>,
    pub extractor_timeout_secs: Option<u64>,
    pub multimodal_model: Option<String>,
    pub multimodal_timeout_secs: Option<u64>,
    pub multimodal_max_render_concurrent: Option<usize>,
    pub multimodal_asset_root: Option<PathBuf>,
    pub multimodal_inline_image_max_bytes: Option<u64>,
    pub multimodal_inline_video_max_bytes: Option<u64>,
    pub budget_daily_euros_retrieval: Option<f64>,
    pub budget_daily_euros_graph: Option<f64>,
    pub budget_daily_items_retrieval: Option<u32>,
    pub budget_daily_items_graph: Option<u32>,
    pub budget_per_item_tokens: Option<u32>,
    pub budget_per_item_wall_time_secs: Option<u32>,
    pub budget_downgrade_policy: Option<DowngradePolicy>,
}

impl RuntimeConfigPatch {
    #[must_use]
    pub fn apply_to(&self, mut config: ElephantConfig) -> ElephantConfig {
        if let Some(mode) = self.runtime_mode {
            config.runtime.mode = mode;
        }
        if let Some(ingest_enabled) = self.ingest_enabled {
            config.runtime.ingest_enabled = ingest_enabled;
        }
        if let Some(backend) = self.backend {
            config.storage.backend = backend;
        }
        if let Some(snapshot_path) = &self.snapshot_path {
            config
                .storage
                .elephant_db
                .snapshot_path
                .clone_from(snapshot_path);
        }
        if let Some(url) = &self.surreal_url {
            config.storage.surreal_db.url.clone_from(url);
        }
        if let Some(namespace) = &self.surreal_namespace {
            config.storage.surreal_db.namespace.clone_from(namespace);
        }
        if let Some(database) = &self.surreal_database {
            config.storage.surreal_db.database.clone_from(database);
        }
        if let Some(username) = &self.surreal_username {
            config.storage.surreal_db.username.clone_from(username);
        }
        if let Some(password) = &self.surreal_password {
            config.storage.surreal_db.password.clone_from(password);
        }
        if let Some(provider) = &self.extractor_provider {
            config.extractor.provider.clone_from(provider);
        }
        if let Some(required) = self.extractor_required {
            config.extractor.required = required;
        }
        if let Some(mcp_url) = &self.extractor_mcp_url {
            config.extractor.mcp_url.clone_from(mcp_url);
        }
        if let Some(model) = &self.extractor_model {
            config.extractor.model.clone_from(model);
        }
        if let Some(timeout_secs) = self.extractor_timeout_secs {
            config.extractor.timeout_secs = timeout_secs;
        }
        if let Some(model) = &self.multimodal_model {
            config.multimodal.model.clone_from(model);
        }
        if let Some(timeout_secs) = self.multimodal_timeout_secs {
            config.multimodal.timeout_secs = timeout_secs;
        }
        if let Some(max_render_concurrent) = self.multimodal_max_render_concurrent {
            config.multimodal.max_render_concurrent = max_render_concurrent;
        }
        if let Some(asset_root) = &self.multimodal_asset_root {
            config.multimodal.asset_root.clone_from(asset_root);
        }
        if let Some(limit) = self.multimodal_inline_image_max_bytes {
            config.multimodal.inline_image_max_bytes = limit;
        }
        if let Some(limit) = self.multimodal_inline_video_max_bytes {
            config.multimodal.inline_video_max_bytes = limit;
        }
        if let Some(daily_euros_retrieval) = self.budget_daily_euros_retrieval {
            config.budget.daily_euros_retrieval = daily_euros_retrieval;
        }
        if let Some(daily_euros_graph) = self.budget_daily_euros_graph {
            config.budget.daily_euros_graph = daily_euros_graph;
        }
        if let Some(daily_items_retrieval) = self.budget_daily_items_retrieval {
            config.budget.daily_items_retrieval = Some(daily_items_retrieval);
        }
        if let Some(daily_items_graph) = self.budget_daily_items_graph {
            config.budget.daily_items_graph = Some(daily_items_graph);
        }
        if let Some(per_item_tokens) = self.budget_per_item_tokens {
            config.budget.per_item_tokens = Some(per_item_tokens);
        }
        if let Some(per_item_wall_time_secs) = self.budget_per_item_wall_time_secs {
            config.budget.per_item_wall_time_secs = Some(per_item_wall_time_secs);
        }
        if let Some(downgrade_policy) = self.budget_downgrade_policy {
            config.budget.downgrade_policy = downgrade_policy;
        }
        config
    }
}

struct ManagedPipeline {
    shutdown_tx: watch::Sender<bool>,
    handle: thread::JoinHandle<anyhow::Result<()>>,
    stopped: StdArc<AtomicBool>,
    last_error: StdArc<StdMutex<Option<String>>>,
}

impl std::fmt::Debug for ManagedPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedPipeline")
            .field("shutdown_tx", &"watch::Sender<bool>")
            .field("handle", &"std::thread::JoinHandle<anyhow::Result<()>>")
            .field("stopped", &self.stopped.load(Ordering::SeqCst))
            .field(
                "last_error",
                &self.last_error.lock().ok().and_then(|guard| guard.clone()),
            )
            .finish()
    }
}

#[derive(Debug)]
struct PreparedRuntime {
    config: ElephantConfig,
    storage: StorageClient,
    pipeline: Option<ManagedPipeline>,
    status: RuntimeStatus,
}

#[derive(Debug)]
struct RuntimeInner {
    config_path: PathBuf,
    config: Option<ElephantConfig>,
    storage: Option<StorageClient>,
    pipeline: Option<ManagedPipeline>,
    status: RuntimeStatus,
}

#[derive(Debug)]
pub struct RuntimeManager {
    inner: RwLock<RuntimeInner>,
    reconfigure: Mutex<()>,
    readiness_refresh: Mutex<()>,
    /// Last computed conveyor status and when. See [`CONVEYOR_STATUS_TTL`].
    conveyor_cache: Mutex<Option<(std::time::Instant, ConveyorStatus)>>,
    /// Background embedding progress count. See [`EMBEDDING_PROGRESS_TTL`].
    embedding_progress: EmbeddingProgressCache,
}

/// How long a computed conveyor status is reused.
///
/// Computing queue diagnostics can be expensive on a large embedded store.
/// Cache them independently of MCP calls so diagnostic queries cannot repeatedly
/// delay extractor connections or client requests.
const CONVEYOR_STATUS_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractorRefreshOutcome {
    NotApplicable,
    Pending,
    Refreshed,
}

impl RuntimeManager {
    /// Creates a managed runtime from config file or legacy environment.
    ///
    /// # Errors
    ///
    /// Returns an error if config discovery, parsing, storage initialization,
    /// or managed worker startup fails.
    pub async fn new() -> Result<Self> {
        Self::new_with_ingest_enabled(None).await
    }

    /// Creates a managed runtime with an optional in-memory ingest override.
    ///
    /// This is used by the unified `elephant serve --server-only` command to
    /// disable workers without rewriting persisted runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if config discovery, parsing, storage initialization,
    /// or managed worker startup fails.
    pub async fn new_with_ingest_enabled(ingest_enabled: Option<bool>) -> Result<Self> {
        let project_root = find_project_root(env::current_dir().context("current dir")?);
        let config_path = project_root.join(CONFIG_DIR).join(CONFIG_FILE);
        let mut inner = RuntimeInner {
            config_path: config_path.clone(),
            config: None,
            storage: None,
            pipeline: None,
            status: RuntimeStatus::bootstrap(),
        };

        if inner.config_path.exists() {
            let mut config = load_config_from_path(&inner.config_path)?;
            apply_env_overrides_to_loaded_config(&mut config);
            if let Some(ingest_enabled) = ingest_enabled {
                config.runtime.ingest_enabled = ingest_enabled;
            }
            let prepared = prepare_runtime(config, None).await?;
            apply_prepared_runtime(&mut inner, prepared);
        } else if env_runtime_config_present() {
            let mut config = ElephantConfig::from_env();
            if let Some(ingest_enabled) = ingest_enabled {
                config.runtime.ingest_enabled = ingest_enabled;
            }
            let prepared = prepare_runtime(config, None).await?;
            apply_prepared_runtime(&mut inner, prepared);
        }

        Ok(Self {
            inner: RwLock::new(inner),
            reconfigure: Mutex::new(()),
            readiness_refresh: Mutex::new(()),
            conveyor_cache: Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        })
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_preconfigured(storage: StorageClient) -> Self {
        let config_path = PathBuf::from(".elephant/elephant_config.toml");
        Self {
            inner: RwLock::new(RuntimeInner {
                config_path,
                config: Some(ElephantConfig {
                    runtime: RuntimeSection {
                        mode: RuntimeMode::LocalMemory,
                        ingest_enabled: true,
                    },
                    storage: StorageSection {
                        backend: storage.backend(),
                        ..StorageSection {
                            backend: storage.backend(),
                            elephant_db: EmbeddedStorageSection::default(),
                            surreal_db: SurrealStorageSection::default(),
                        }
                    },
                    extractor: ExtractorSection::default(),
                    multimodal: MultimodalSection::default(),
                    budget: Budget::default(),
                }),
                storage: Some(storage.clone()),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(storage.backend().as_str().to_string()),
                    storage_ready: true,
                    worker_ready: false,
                    extractor_ready: true,
                    semantic_ingest_ready: true,
                    conveyor: None,
                    issues: Vec::new(),
                },
            }),
            reconfigure: Mutex::new(()),
            readiness_refresh: Mutex::new(()),
            conveyor_cache: Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        }
    }

    #[cfg(test)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_storage_for_tests(storage: StorageClient) -> Self {
        Self::new_preconfigured(storage)
    }

    /// Readiness only: configured, storage/worker/ingest readiness, mode and
    /// issues. No database work, so it is safe on every request path. This is
    /// what tool gating needs; it never reads the conveyor.
    pub async fn get_readiness(&self) -> RuntimeStatus {
        self.refresh_pipeline_liveness().await;
        self.inner.read().await.status.clone()
    }

    /// Full status: readiness plus the conveyor diagnostic (cached, see
    /// `CONVEYOR_STATUS_TTL`). For status endpoints and startup, never for
    /// per-request gating.
    pub async fn get_status(&self) -> RuntimeStatus {
        let mut status = self.get_readiness().await;

        // Readiness alone cannot report a stopped conveyor: every component
        // stays alive and ready while no work moves, which is exactly how the
        // 2026-07-30 budget stall went unnoticed for the better part of an hour
        // until someone happened to be counting assertions by hand.
        if let Some(mut conveyor) = self.cached_conveyor_status().await {
            if conveyor.blocked {
                let issue = conveyor.blocked_reason.clone().map_or_else(
                    || "conveyor_blocked".to_string(),
                    |reason| format!("conveyor_blocked:{reason}"),
                );
                if !status.issues.iter().any(|existing| existing == &issue) {
                    status.issues.push(issue);
                }
            }
            conveyor.embedding = self.embedding_progress().await;
            status.conveyor = Some(conveyor);
        }

        status
    }

    /// The last embedding progress count, starting a background recount when
    /// it is missing or older than [`EMBEDDING_PROGRESS_TTL`]. Never waits for
    /// the count itself.
    async fn embedding_progress(&self) -> Option<EmbeddingProgress> {
        let cache = &self.embedding_progress;
        let latest = cache.latest.lock().ok().and_then(|guard| guard.clone());
        let stale = latest
            .as_ref()
            .is_none_or(|(at, _)| at.elapsed() >= EMBEDDING_PROGRESS_TTL);
        let progress = latest.map(|(_, progress)| progress);
        if !stale || cache.refreshing.swap(true, Ordering::AcqRel) {
            return progress;
        }
        let storage = self.inner.read().await.storage.clone();
        let Some(storage) = storage else {
            cache.refreshing.store(false, Ordering::Release);
            return progress;
        };
        let slot = StdArc::clone(&cache.latest);
        let refreshing = StdArc::clone(&cache.refreshing);
        let space_id = std::env::var("MCP_SPACE_ID").unwrap_or_else(|_| "default".to_string());
        tokio::spawn(async move {
            let repo = elephant_storage::DocNodeRepository::new(storage);
            match repo.count_embedding_progress(&space_id).await {
                Ok(counts) => {
                    let fresh = EmbeddingProgress {
                        space_id,
                        pending_nodes: counts.pending,
                        embedded_nodes: counts.embedded,
                        embeddable_nodes: counts.pending + counts.embedded,
                        counted_at: chrono::Utc::now().to_rfc3339(),
                    };
                    if let Ok(mut guard) = slot.lock() {
                        *guard = Some((std::time::Instant::now(), fresh));
                    }
                }
                Err(err) => warn!("Embedding progress count failed: {err}"),
            }
            refreshing.store(false, Ordering::Release);
        });
        progress
    }

    /// The conveyor status, recomputed at most once per TTL.
    ///
    /// The cache lock is held across the computation on purpose: concurrent
    /// callers wait for ONE scan instead of each starting their own, which is
    /// what turned a slow count into a pile-up.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the guard is held across the scan on purpose: single-flight"
    )]
    async fn cached_conveyor_status(&self) -> Option<ConveyorStatus> {
        let mut cache = self.conveyor_cache.lock().await;
        if let Some((at, status)) = cache.as_ref() {
            if at.elapsed() < CONVEYOR_STATUS_TTL {
                return Some(status.clone());
            }
        }
        let fresh = self.conveyor_status().await?;
        *cache = Some((std::time::Instant::now(), fresh.clone()));
        Some(fresh)
    }

    /// Reports whether derivation work can flow, and why not when it cannot.
    ///
    /// Returns `None` when storage or config is not up yet, since there is
    /// nothing meaningful to say about the queue then.
    async fn conveyor_status(&self) -> Option<ConveyorStatus> {
        // Release the read guard before touching the queue: counting runnable
        // items hits the database, and holding the lock across it would make a
        // status poll contend with reconfiguration.
        let (storage, budget) = {
            let inner = self.inner.read().await;
            let storage = inner.storage.clone()?;
            let budget = inner.config.as_ref()?.budget.clone();
            drop(inner);
            (storage, budget)
        };
        let space_id = std::env::var("MCP_SPACE_ID").unwrap_or_else(|_| "default".to_string());

        let queue = WorkQueue::with_defaults(storage.clone());
        let runnable_items = queue.count_runnable().await.unwrap_or(0);

        let enforcer = BudgetEnforcer::new(storage, budget, space_id);
        let snapshot = enforcer.create_snapshot().await.ok()?;

        let blocked_reason = classify_conveyor_block(
            runnable_items,
            snapshot.remaining_euros_graph,
            snapshot.remaining_euros_retrieval,
        );

        Some(ConveyorStatus {
            runnable_items,
            remaining_euros_retrieval: snapshot.remaining_euros_retrieval,
            remaining_euros_graph: snapshot.remaining_euros_graph,
            blocked: blocked_reason.is_some(),
            blocked_reason,
            embedding: None,
        })
    }

    pub async fn get_config_redacted(&self) -> Option<toml::Value> {
        let inner = self.inner.read().await;
        inner
            .config
            .clone()
            .and_then(|config| toml::Value::try_from(redact_config(config)).ok())
    }

    pub async fn current_storage(&self) -> Option<StorageClient> {
        self.refresh_pipeline_liveness().await;
        self.inner.read().await.storage.clone()
    }

    /// Initializes a bootstrap runtime and applies the provided patch.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is already configured, if the config
    /// cannot be written, or if storage/worker startup fails.
    pub async fn initialize_runtime(&self, patch: RuntimeConfigPatch) -> Result<RuntimeStatus> {
        let _guard = self.reconfigure.lock().await;
        let config_path = {
            let inner = self.inner.read().await;
            if inner.config.is_some() {
                return Err(anyhow!("runtime is already configured"));
            }
            inner.config_path.clone()
        };

        let config = patch.apply_to(ElephantConfig::default());
        save_config(&config_path, &config)?;
        let prepared = prepare_runtime(config, None).await?;

        let old_pipeline = {
            let mut inner = self.inner.write().await;
            if inner.config.is_some() {
                return Err(anyhow!("runtime is already configured"));
            }
            inner.pipeline.take()
        };
        stop_pipeline_handle(old_pipeline).await;

        let mut inner = self.inner.write().await;
        if inner.config.is_some() {
            return Err(anyhow!("runtime is already configured"));
        }
        apply_prepared_runtime(&mut inner, prepared);
        Ok(inner.status.clone())
    }

    /// Applies a runtime config patch to an already-configured runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not configured, if the config
    /// cannot be written, or if storage/worker restart fails.
    pub async fn configure_runtime(&self, patch: RuntimeConfigPatch) -> Result<RuntimeStatus> {
        let _guard = self.reconfigure.lock().await;
        let (current, current_storage, config_path) = {
            let inner = self.inner.read().await;
            let current = inner
                .config
                .clone()
                .ok_or_else(|| anyhow!("runtime is not configured"))?;
            (current, inner.storage.clone(), inner.config_path.clone())
        };

        let mut config = patch.apply_to(current.clone());
        let requested_storage = resolve_storage_config(&mut config)?;
        if let Some(storage) = &current_storage {
            let active = storage.config();
            if active.backend == StorageBackendKind::SurrealDb
                && requested_storage.backend == StorageBackendKind::SurrealDb
                && active.url == requested_storage.url
                && matches!(
                    active.url.split(':').next(),
                    Some("mem" | "rocksdb" | "surrealkv" | "file")
                )
                && (active.query_timeout != requested_storage.query_timeout
                    || active.transaction_timeout != requested_storage.transaction_timeout)
            {
                return Err(anyhow!(
                    "changing embedded SurrealDB execution deadlines requires a process restart; edit the config file or environment and restart Elephant"
                ));
            }
        }
        save_config(&config_path, &config)?;
        let reused_storage = current_storage.filter(|_| storage_config_matches(&current, &config));
        let prepared = prepare_runtime(config, reused_storage).await?;

        let old_pipeline = {
            let mut inner = self.inner.write().await;
            inner
                .config
                .as_ref()
                .ok_or_else(|| anyhow!("runtime is not configured"))?;
            inner.pipeline.take()
        };
        stop_pipeline_handle(old_pipeline).await;

        let mut inner = self.inner.write().await;
        inner
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("runtime is not configured"))?;
        apply_prepared_runtime(&mut inner, prepared);
        Ok(inner.status.clone())
    }

    pub async fn shutdown(&self) {
        let _guard = self.reconfigure.lock().await;
        let pipeline = {
            let mut inner = self.inner.write().await;
            inner.status.worker_ready = false;
            inner.status.semantic_ingest_ready = false;
            inner
                .status
                .issues
                .retain(|issue| issue != "worker_runtime_unavailable");
            if inner
                .config
                .as_ref()
                .is_some_and(|config| config.runtime.ingest_enabled)
            {
                inner
                    .status
                    .issues
                    .push("worker_runtime_unavailable".to_string());
            }
            inner.pipeline.take()
        };
        stop_pipeline_handle(pipeline).await;
    }

    pub fn schedule_post_bind_readiness_refresh(self: std::sync::Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _ = self.refresh_extractor_readiness_until_ready().await;
        })
    }

    async fn refresh_extractor_readiness_until_ready(&self) -> Result<bool> {
        let _guard = self.readiness_refresh.lock().await;
        let mut attempt = 0usize;
        loop {
            attempt = attempt.saturating_add(1);
            match self
                .try_refresh_extractor_readiness_with(|config| async move {
                    extractor_ready(&config).await
                })
                .await?
            {
                ExtractorRefreshOutcome::NotApplicable => {
                    if attempt == 1 {
                        let issues = self.get_status().await.issues;
                        if issues
                            .iter()
                            .any(|issue| issue == ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE)
                        {
                            info!(
                                "Skipping extractor readiness refresh because this binary was built without meerkat-extraction support"
                            );
                        } else {
                            info!(
                                "Skipping extractor readiness refresh because the current runtime does not require it"
                            );
                        }
                    }
                    return Ok(false);
                }
                ExtractorRefreshOutcome::Refreshed => {
                    info!(attempt, "Extractor readiness refresh succeeded");
                    return Ok(true);
                }
                ExtractorRefreshOutcome::Pending => {
                    if attempt == 1 || attempt.is_multiple_of(RETRY_LOG_INTERVAL) {
                        warn!(
                            attempt,
                            "Extractor is still unavailable; Elephant will keep retrying readiness in the background"
                        );
                    }
                    let delay = if attempt < INITIAL_EXTRACTOR_RETRY_ATTEMPTS {
                        INITIAL_EXTRACTOR_RETRY_DELAY
                    } else {
                        SUSTAINED_EXTRACTOR_RETRY_DELAY
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn try_refresh_extractor_readiness_with<F, Fut>(
        &self,
        checker: F,
    ) -> Result<ExtractorRefreshOutcome>
    where
        F: Fn(ElephantConfig) -> Fut,
        Fut: Future<Output = bool>,
    {
        self.retry_extractor_readiness_with(1, Duration::ZERO, checker)
            .await
    }

    async fn retry_extractor_readiness_with<F, Fut>(
        &self,
        attempts: usize,
        delay: Duration,
        checker: F,
    ) -> Result<ExtractorRefreshOutcome>
    where
        F: Fn(ElephantConfig) -> Fut,
        Fut: Future<Output = bool>,
    {
        for attempt in 0..attempts {
            let config = {
                let inner = self.inner.read().await;
                let Some(config) = inner.config.clone() else {
                    return Ok(ExtractorRefreshOutcome::NotApplicable);
                };
                if inner.status.extractor_ready
                    || !config.runtime.ingest_enabled
                    || !config.extractor.required
                    || config.extractor.provider != "meerkat-agentic"
                    || !agentic_extractor_supported()
                {
                    return Ok(ExtractorRefreshOutcome::NotApplicable);
                }
                drop(inner);
                config
            };

            if checker(config.clone()).await {
                let mut inner = self.inner.write().await;
                if inner
                    .config
                    .as_ref()
                    .is_some_and(|current| extractor_config_matches(current, &config))
                {
                    inner.status.extractor_ready = true;
                    inner.status.semantic_ingest_ready =
                        inner.status.storage_ready && inner.status.worker_ready;
                    inner.status.issues.retain(|issue| {
                        issue != ISSUE_SEMANTIC_EXTRACTOR_UNAVAILABLE
                            && issue != ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE
                    });
                    drop(inner);
                    return Ok(ExtractorRefreshOutcome::Refreshed);
                }
                drop(inner);
                return Ok(ExtractorRefreshOutcome::Pending);
            }

            if attempt + 1 < attempts && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        Ok(ExtractorRefreshOutcome::Pending)
    }

    #[cfg(all(test, feature = "meerkat-extraction"))]
    async fn refresh_extractor_readiness_until_ready_with<F, Fut>(&self, checker: F) -> Result<bool>
    where
        F: Fn(ElephantConfig) -> Fut + Send + Sync,
        Fut: Future<Output = bool> + Send,
    {
        let _guard = self.readiness_refresh.lock().await;
        loop {
            match self.try_refresh_extractor_readiness_with(&checker).await? {
                ExtractorRefreshOutcome::NotApplicable => return Ok(false),
                ExtractorRefreshOutcome::Refreshed => return Ok(true),
                ExtractorRefreshOutcome::Pending => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn refresh_pipeline_liveness(&self) {
        // Every request calls this; take the write lock only when there is
        // something to record, so a healthy runtime serves readers in parallel.
        let stopped = self
            .inner
            .read()
            .await
            .pipeline
            .as_ref()
            .is_some_and(|pipeline| pipeline.stopped.load(Ordering::SeqCst));
        if !stopped {
            return;
        }
        let mut inner = self.inner.write().await;
        let Some(pipeline) = inner.pipeline.as_ref() else {
            return;
        };
        let stopped = pipeline.stopped.load(Ordering::SeqCst);
        let last_error = pipeline
            .last_error
            .lock()
            .ok()
            .and_then(|guard| guard.clone());

        if !stopped {
            return;
        }

        inner.status.worker_ready = false;
        inner.status.semantic_ingest_ready = false;
        if !inner
            .status
            .issues
            .iter()
            .any(|issue| issue == "worker_runtime_unavailable")
        {
            inner
                .status
                .issues
                .push("worker_runtime_unavailable".to_string());
        }
        if let Some(err) = last_error {
            let detailed = format!("worker_runtime_stopped:{err}");
            if !inner.status.issues.iter().any(|issue| issue == &detailed) {
                inner.status.issues.push(detailed);
            }
        }
    }
}

impl ElephantConfig {
    fn from_env() -> Self {
        let backend = env::var("ELEPHANT_DB")
            .ok()
            .and_then(|value| value.parse::<StorageBackendKind>().ok())
            .unwrap_or(StorageBackendKind::SurrealDb);
        let mode = if backend == StorageBackendKind::ElephantDb {
            RuntimeMode::LocalMemory
        } else {
            RuntimeMode::SharedMemory
        };
        Self {
            runtime: RuntimeSection {
                mode,
                ingest_enabled: env::var("PIPELINE_DISABLED")
                    .map(|v| !(v == "true" || v == "1"))
                    .unwrap_or(true),
            },
            storage: StorageSection {
                backend,
                elephant_db: EmbeddedStorageSection {
                    snapshot_path: env::var("ELEPHANT_DB_SNAPSHOT_LOAD")
                        .map_or_else(|_| PathBuf::from(DEFAULT_SNAPSHOT_PATH), PathBuf::from),
                },
                surreal_db: SurrealStorageSection {
                    url: env::var("SURREALDB_URL")
                        .unwrap_or_else(|_| SurrealStorageSection::default().url),
                    namespace: env::var("SURREALDB_NAMESPACE")
                        .unwrap_or_else(|_| SurrealStorageSection::default().namespace),
                    database: env::var("SURREALDB_DATABASE")
                        .unwrap_or_else(|_| SurrealStorageSection::default().database),
                    username: env::var("SURREALDB_USER")
                        .unwrap_or_else(|_| SurrealStorageSection::default().username),
                    password: env::var("SURREALDB_PASS")
                        .unwrap_or_else(|_| SurrealStorageSection::default().password),
                    query_timeout_secs: default_database_timeout_secs(),
                    transaction_timeout_secs: default_database_timeout_secs(),
                },
            },
            extractor: ExtractorSection {
                provider: "meerkat-agentic".to_string(),
                required: true,
                mcp_url: env::var("PIPELINE_AGENTIC_MCP_URL")
                    .unwrap_or_else(|_| ExtractorSection::default().mcp_url),
                model: env::var("PIPELINE_AGENTIC_MODEL")
                    .unwrap_or_else(|_| ExtractorSection::default().model),
                timeout_secs: ExtractorSection::default().timeout_secs,
            },
            multimodal: MultimodalSection {
                model: env::var("PIPELINE_MULTIMODAL_RENDER_MODEL")
                    .unwrap_or_else(|_| MultimodalSection::default().model),
                timeout_secs: env::var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_else(|| MultimodalSection::default().timeout_secs),
                max_render_concurrent: env::var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or_else(|| MultimodalSection::default().max_render_concurrent),
                asset_root: env::var("PIPELINE_MULTIMODAL_ASSET_ROOT")
                    .map_or_else(|_| MultimodalSection::default().asset_root, PathBuf::from),
                inline_image_max_bytes: env::var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_else(|| MultimodalSection::default().inline_image_max_bytes),
                inline_video_max_bytes: env::var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_else(|| MultimodalSection::default().inline_video_max_bytes),
            },
            budget: Budget {
                daily_euros_retrieval: env::var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL")
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or_else(|| Budget::default().daily_euros_retrieval),
                daily_euros_graph: env::var("PIPELINE_BUDGET_DAILY_EUROS_GRAPH")
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or_else(|| Budget::default().daily_euros_graph),
                daily_items_retrieval: env::var("PIPELINE_BUDGET_DAILY_ITEMS_RETRIEVAL")
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    .or_else(|| Budget::default().daily_items_retrieval),
                daily_items_graph: env::var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH")
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    .or_else(|| Budget::default().daily_items_graph),
                per_item_tokens: env::var("PIPELINE_BUDGET_PER_ITEM_TOKENS")
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    .or_else(|| Budget::default().per_item_tokens),
                per_item_wall_time_secs: env::var("PIPELINE_BUDGET_PER_ITEM_WALL_TIME_SECS")
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    .or_else(|| Budget::default().per_item_wall_time_secs),
                concurrent_workers: Budget::default().concurrent_workers,
                downgrade_policy: env::var("PIPELINE_BUDGET_DOWNGRADE_POLICY")
                    .ok()
                    .and_then(|value| value.parse::<DowngradePolicy>().ok())
                    .unwrap_or_else(|| Budget::default().downgrade_policy),
            },
        }
    }
}

fn resolve_storage_config(config: &mut ElephantConfig) -> Result<StorageConfig> {
    let storage_config = storage_config_from_runtime(config).with_env_timeouts()?;
    if config.storage.backend == StorageBackendKind::SurrealDb {
        config.storage.surreal_db.query_timeout_secs = storage_config.query_timeout.as_secs();
        config.storage.surreal_db.transaction_timeout_secs =
            storage_config.transaction_timeout.as_secs();
    }
    Ok(storage_config)
}

async fn prepare_runtime(
    mut config: ElephantConfig,
    existing_storage: Option<StorageClient>,
) -> Result<PreparedRuntime> {
    let storage_config = resolve_storage_config(&mut config)?;
    let existing_storage = existing_storage.filter(|storage| {
        storage_config.backend == StorageBackendKind::ElephantDb
            || (storage.config().query_timeout == storage_config.query_timeout
                && storage.config().transaction_timeout == storage_config.transaction_timeout)
    });
    let storage_client = if let Some(storage) = existing_storage {
        storage
    } else {
        StorageClient::new(storage_config)
            .await
            .context("failed to initialize configured storage")?
    };
    let extractor_ready = extractor_ready(&config).await;
    let pipeline = if config.runtime.mode.is_write_capable() && config.runtime.ingest_enabled {
        Some(start_pipeline(&config, &storage_client)?)
    } else {
        None
    };
    let worker_ready = pipeline.is_some();

    let backend = config.storage.backend.as_str().to_string();
    let semantic_ingest_ready = config.runtime.ingest_enabled && worker_ready && extractor_ready;
    let mut issues = Vec::new();
    if !worker_ready && config.runtime.ingest_enabled {
        issues.push("worker_runtime_unavailable".to_string());
    }
    if let Some(issue) = extractor_issue(&config, extractor_ready) {
        issues.push(issue);
    }

    Ok(PreparedRuntime {
        status: RuntimeStatus {
            configured: true,
            bootstrap_mode: false,
            runtime_mode: config.runtime.mode,
            backend: Some(backend),
            storage_ready: true,
            worker_ready,
            extractor_ready,
            semantic_ingest_ready,
            conveyor: None,
            issues,
        },
        config,
        storage: storage_client,
        pipeline,
    })
}

fn apply_prepared_runtime(inner: &mut RuntimeInner, prepared: PreparedRuntime) {
    inner.status = prepared.status;
    inner.storage = Some(prepared.storage);
    inner.pipeline = prepared.pipeline;
    inner.config = Some(prepared.config);
}

fn start_pipeline(
    config: &ElephantConfig,
    storage_client: &StorageClient,
) -> Result<ManagedPipeline> {
    let pipeline_config = pipeline_config_from_runtime(config);
    let extractor_mcp_url = config.extractor.mcp_url.clone();
    let extractor_model = config.extractor.model.clone();
    let multimodal = config.multimodal.clone();
    let budget = config.budget.clone();
    let worker_threads = managed_pipeline_worker_threads(&pipeline_config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let pipeline_storage = storage_client.clone();
    let stopped = StdArc::new(AtomicBool::new(false));
    let last_error = StdArc::new(StdMutex::new(None));
    let thread_stopped = StdArc::clone(&stopped);
    let thread_error = StdArc::clone(&last_error);
    let handle = thread::Builder::new()
        .name("elephant-managed-pipeline".to_string())
        .spawn(move || {
            env::set_var("PIPELINE_AGENTIC_MCP_URL", &extractor_mcp_url);
            env::set_var("PIPELINE_AGENTIC_MODEL", &extractor_model);
            env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", &multimodal.model);
            env::set_var(
                "PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS",
                multimodal.timeout_secs.to_string(),
            );
            env::set_var(
                "PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT",
                multimodal.max_render_concurrent.to_string(),
            );
            env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", &multimodal.asset_root);
            env::set_var(
                "PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES",
                multimodal.inline_image_max_bytes.to_string(),
            );
            env::set_var(
                "PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES",
                multimodal.inline_video_max_bytes.to_string(),
            );
            env::set_var(
                "PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL",
                budget.daily_euros_retrieval.to_string(),
            );
            env::set_var(
                "PIPELINE_BUDGET_DAILY_EUROS_GRAPH",
                budget.daily_euros_graph.to_string(),
            );
            if let Some(value) = budget.daily_items_retrieval {
                env::set_var("PIPELINE_BUDGET_DAILY_ITEMS_RETRIEVAL", value.to_string());
            } else {
                env::remove_var("PIPELINE_BUDGET_DAILY_ITEMS_RETRIEVAL");
            }
            if let Some(value) = budget.daily_items_graph {
                env::set_var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH", value.to_string());
            } else {
                env::remove_var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH");
            }
            if let Some(value) = budget.per_item_tokens {
                env::set_var("PIPELINE_BUDGET_PER_ITEM_TOKENS", value.to_string());
            } else {
                env::remove_var("PIPELINE_BUDGET_PER_ITEM_TOKENS");
            }
            if let Some(value) = budget.per_item_wall_time_secs {
                env::set_var("PIPELINE_BUDGET_PER_ITEM_WALL_TIME_SECS", value.to_string());
            } else {
                env::remove_var("PIPELINE_BUDGET_PER_ITEM_WALL_TIME_SECS");
            }
            env::set_var(
                "PIPELINE_BUDGET_DOWNGRADE_POLICY",
                budget.downgrade_policy.to_string(),
            );
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .thread_name("elephant-pipeline")
                .enable_all()
                .build()
                .context("build managed pipeline runtime")?;

            let result = runtime.block_on(async move {
                run_pipeline_with_shutdown(pipeline_config, pipeline_storage, shutdown_rx).await
            });
            if let Err(err) = &result {
                if let Ok(mut guard) = thread_error.lock() {
                    *guard = Some(err.to_string());
                }
            }
            thread_stopped.store(true, Ordering::SeqCst);
            result
        })
        .context("spawn managed pipeline thread")?;
    Ok(ManagedPipeline {
        shutdown_tx,
        handle,
        stopped,
        last_error,
    })
}

fn managed_pipeline_worker_threads(config: &PipelineConfig) -> usize {
    let mut workers = config.work_queue.max_concurrent.max(1);

    if config.embed.enabled {
        workers += 1;
    }
    if config.truth_maintenance.enabled {
        workers += 1;
    }
    if config.retention.enabled {
        workers += 1;
    }
    if config.maintenance.enabled {
        workers += 1;
    }

    workers.clamp(4, 16)
}

fn storage_config_matches(left: &ElephantConfig, right: &ElephantConfig) -> bool {
    if left.storage.backend != right.storage.backend {
        return false;
    }

    match left.storage.backend {
        StorageBackendKind::ElephantDb => {
            left.storage.elephant_db.snapshot_path == right.storage.elephant_db.snapshot_path
        }
        StorageBackendKind::SurrealDb => {
            left.storage.surreal_db.url == right.storage.surreal_db.url
                && left.storage.surreal_db.namespace == right.storage.surreal_db.namespace
                && left.storage.surreal_db.database == right.storage.surreal_db.database
                && left.storage.surreal_db.username == right.storage.surreal_db.username
                && left.storage.surreal_db.password == right.storage.surreal_db.password
                && left.storage.surreal_db.query_timeout_secs
                    == right.storage.surreal_db.query_timeout_secs
                && left.storage.surreal_db.transaction_timeout_secs
                    == right.storage.surreal_db.transaction_timeout_secs
        }
    }
}

fn extractor_config_matches(left: &ElephantConfig, right: &ElephantConfig) -> bool {
    left.runtime.ingest_enabled == right.runtime.ingest_enabled
        && left.extractor.provider == right.extractor.provider
        && left.extractor.required == right.extractor.required
        && left.extractor.mcp_url == right.extractor.mcp_url
        && left.extractor.model == right.extractor.model
        && left.extractor.timeout_secs == right.extractor.timeout_secs
}

fn extractor_issue(config: &ElephantConfig, extractor_ready: bool) -> Option<String> {
    if !config.runtime.ingest_enabled || !config.extractor.required || extractor_ready {
        return None;
    }
    if config.extractor.provider == "meerkat-agentic" && !agentic_extractor_supported() {
        return Some(ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE.to_string());
    }
    Some(ISSUE_SEMANTIC_EXTRACTOR_UNAVAILABLE.to_string())
}

#[cfg(feature = "meerkat-extraction")]
const fn agentic_extractor_supported() -> bool {
    true
}

#[cfg(not(feature = "meerkat-extraction"))]
const fn agentic_extractor_supported() -> bool {
    false
}

async fn stop_pipeline_handle(pipeline: Option<ManagedPipeline>) {
    if let Some(pipeline) = pipeline {
        let _ = pipeline.shutdown_tx.send(true);
        let handle = pipeline.handle;
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || handle.join()),
        )
        .await;
    }
}

fn storage_config_from_runtime(config: &ElephantConfig) -> StorageConfig {
    match config.storage.backend {
        StorageBackendKind::ElephantDb => StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            snapshot_load: config
                .storage
                .elephant_db
                .snapshot_path
                .exists()
                .then(|| config.storage.elephant_db.snapshot_path.clone()),
            ..StorageConfig::default()
        },
        StorageBackendKind::SurrealDb => StorageConfig {
            backend: StorageBackendKind::SurrealDb,
            url: config.storage.surreal_db.url.clone(),
            username: config.storage.surreal_db.username.clone(),
            password: config.storage.surreal_db.password.clone(),
            namespace: config.storage.surreal_db.namespace.clone(),
            database: config.storage.surreal_db.database.clone(),
            snapshot_load: None,
            query_timeout: Duration::from_secs(config.storage.surreal_db.query_timeout_secs),
            transaction_timeout: Duration::from_secs(
                config.storage.surreal_db.transaction_timeout_secs,
            ),
        },
    }
}

fn pipeline_config_from_runtime(config: &ElephantConfig) -> PipelineConfig {
    let surreal = (config.storage.backend == StorageBackendKind::SurrealDb)
        .then_some(&config.storage.surreal_db);

    let mut pipeline_config = PipelineConfig::from_env().unwrap_or_else(|err| {
        tracing::warn!("Failed to load pipeline config from environment: {err}; using defaults");
        PipelineConfig::default()
    });
    pipeline_config.db_backend = config.storage.backend;
    pipeline_config.db_url = surreal.map_or_else(String::new, |db| db.url.clone());
    pipeline_config.db_namespace = surreal.map_or_else(String::new, |db| db.namespace.clone());
    pipeline_config.db_database = surreal.map_or_else(String::new, |db| db.database.clone());
    pipeline_config.db_user = surreal.map_or_else(String::new, |db| db.username.clone());
    pipeline_config.db_pass = surreal.map_or_else(String::new, |db| db.password.clone());
    pipeline_config.db_snapshot_load = if config.storage.backend == StorageBackendKind::ElephantDb {
        Some(config.storage.elephant_db.snapshot_path.clone())
    } else {
        None
    };
    pipeline_config.multimodal = elephant_pipeline_bin::config::MultimodalRenderConfig {
        model: config.multimodal.model.clone(),
        timeout_secs: config.multimodal.timeout_secs,
        max_render_concurrent: config.multimodal.max_render_concurrent,
        asset_root: config.multimodal.asset_root.clone(),
        inline_image_max_bytes: config.multimodal.inline_image_max_bytes,
        inline_video_max_bytes: config.multimodal.inline_video_max_bytes,
    };
    pipeline_config
}

#[allow(clippy::unused_async)]
async fn extractor_ready(config: &ElephantConfig) -> bool {
    if !config.runtime.ingest_enabled {
        return false;
    }
    if config.extractor.provider != "meerkat-agentic" {
        return false;
    }
    if !agentic_extractor_supported() {
        return false;
    }
    #[cfg(feature = "meerkat-extraction")]
    {
        let extractor = AgenticExtractor::new(
            AgenticExtractorConfig::new(
                config.extractor.mcp_url.clone(),
                config.extractor.model.clone(),
            )
            .with_connect_on_init(false),
        );
        return extractor.health_check().await.unwrap_or(false);
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    {
        false
    }
}

fn find_project_root(start: PathBuf) -> PathBuf {
    let mut current = start;
    loop {
        if current.join(".git").exists() {
            return current;
        }
        if !current.pop() {
            return env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        }
    }
}

fn save_config(path: &Path, config: &ElephantConfig) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("invalid runtime config path"))?;
    fs::create_dir_all(parent).context("create config dir")?;
    let content = toml::to_string_pretty(config).context("serialize config")?;
    fs::write(path, content).context("write config file")
}

fn load_config_from_path(path: &Path) -> Result<ElephantConfig> {
    let content = fs::read_to_string(path).context("read runtime config")?;
    toml::from_str(&content).context("parse runtime config")
}

fn apply_env_overrides_to_loaded_config(config: &mut ElephantConfig) {
    if let Ok(snapshot_path) = env::var("ELEPHANT_DB_SNAPSHOT_LOAD") {
        if !snapshot_path.trim().is_empty() {
            config.storage.elephant_db.snapshot_path = PathBuf::from(snapshot_path);
        }
    }
}

fn env_runtime_config_present() -> bool {
    [
        "ELEPHANT_DB",
        "SURREALDB_URL",
        "ELEPHANT_DB_SNAPSHOT_LOAD",
        "PIPELINE_AGENTIC_MCP_URL",
        "PIPELINE_AGENTIC_MODEL",
    ]
    .iter()
    .any(|name| env::var_os(name).is_some())
}

fn redact_config(mut config: ElephantConfig) -> ElephantConfig {
    config.storage.surreal_db.password = "***redacted***".to_string();
    config
}

#[cfg(test)]
mod tests {

    #![allow(clippy::expect_used, clippy::unwrap_used)]

    #[test]
    fn empty_queue_is_never_blocked_even_with_no_budget_left() {
        // A quiet day that spent its budget is not a fault. If this reported
        // blocked, the field would be red most nights and stop meaning anything.
        assert_eq!(classify_conveyor_block(0, 0.0, 0.0), None);
        assert_eq!(classify_conveyor_block(0, -50.0, -10.0), None);
    }

    #[test]
    fn waiting_work_with_no_budget_is_blocked_and_names_the_lane() {
        assert_eq!(
            classify_conveyor_block(12, 0.0, 5.0).as_deref(),
            Some("budget_exhausted:graph")
        );
        assert_eq!(
            classify_conveyor_block(12, 5.0, 0.0).as_deref(),
            Some("budget_exhausted:retrieval")
        );
        // Both lanes blew together on 2026-07-30; the reason must say so rather
        // than naming whichever was checked first.
        assert_eq!(
            classify_conveyor_block(12, 0.0, 0.0).as_deref(),
            Some("budget_exhausted:graph+retrieval")
        );
    }

    #[test]
    fn waiting_work_with_budget_left_is_healthy() {
        assert_eq!(classify_conveyor_block(12, 100.0, 50.0), None);
    }

    use super::*;
    use elephant_pipeline::scheduler::ingest_work_kinds::enqueue_triage_source_work_item;
    use elephant_storage::work_item_repository::WorkItemRepository;
    use elephant_storage::{DocRepository, EmbeddedStore, Repository, SourceRepository};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc::Doc;
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::records::work::{WorkItem, WorkItemStatus};
    use elephant_types::RecordId;
    use std::ffi::OsString;
    use std::sync::{Mutex as StdMutex, OnceLock};

    const RUNTIME_CONFIG_ENV_VARS: &[&str] = &[
        "ELEPHANT_DB",
        "SURREALDB_URL",
        "SURREALDB_QUERY_TIMEOUT_SECS",
        "SURREALDB_TRANSACTION_TIMEOUT_SECS",
        "ELEPHANT_DB_SNAPSHOT_LOAD",
        "PIPELINE_AGENTIC_MCP_URL",
        "PIPELINE_AGENTIC_MODEL",
        "PIPELINE_EMBED_ENABLED",
        "PIPELINE_MAX_CONCURRENT",
        "PIPELINE_MAX_EXTRACT_CONCURRENT",
        "PIPELINE_WORK_QUEUE_BATCH_SIZE",
        "PIPELINE_WORK_QUEUE_ALLOWED_KINDS",
        "PIPELINE_WORK_QUEUE_DISABLED_KINDS",
    ];

    struct EnvVarGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvVarGuard {
        fn clear(names: &'static [&'static str]) -> Self {
            let saved = names
                .iter()
                .map(|name| {
                    let value = env::var_os(name);
                    env::remove_var(name);
                    (*name, value)
                })
                .collect();
            Self { saved }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                if let Some(value) = value {
                    env::set_var(name, value);
                } else {
                    env::remove_var(name);
                }
            }
        }
    }

    fn env_lock() -> &'static StdMutex<()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        let unique = format!(
            "elephant-runtime-{}-{}",
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_find_project_root_prefers_nearest_git_ancestor() {
        let _guard = env_lock().lock().expect("env lock");
        let root = temp_test_dir("project-root");
        let nested = root.join("repo").join("nested").join("deeper");
        fs::create_dir_all(&nested).expect("nested dirs");
        fs::create_dir_all(root.join("repo").join(".git")).expect("repo git dir");
        fs::create_dir_all(root.join(".git")).expect("outer git dir");

        let found = find_project_root(nested);
        assert_eq!(found, root.join("repo"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_save_and_load_config_roundtrip() {
        let root = temp_test_dir("config-roundtrip");
        let path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        let config = ElephantConfig::default();

        save_config(&path, &config).expect("save config");
        let loaded = load_config_from_path(&path).expect("load config");

        assert_eq!(loaded.runtime.mode, config.runtime.mode);
        assert_eq!(loaded.runtime.ingest_enabled, config.runtime.ingest_enabled);
        assert_eq!(loaded.storage.backend, config.storage.backend);
        assert_eq!(
            loaded.storage.elephant_db.snapshot_path,
            config.storage.elephant_db.snapshot_path
        );
        assert!(
            (loaded.budget.daily_euros_retrieval - config.budget.daily_euros_retrieval).abs()
                < f64::EPSILON
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_database_timeouts_persist_and_change_storage_identity() {
        let mut config = ElephantConfig::default();
        config.storage.backend = StorageBackendKind::SurrealDb;
        config.storage.surreal_db.query_timeout_secs = 12;
        config.storage.surreal_db.transaction_timeout_secs = 8;
        let serialized = toml::to_string(&config).expect("serialize");
        let loaded: ElephantConfig = toml::from_str(&serialized).expect("deserialize");
        let storage = storage_config_from_runtime(&loaded);
        assert_eq!(storage.query_timeout, Duration::from_secs(12));
        assert_eq!(storage.transaction_timeout, Duration::from_secs(8));
        assert!(storage_config_matches(&config, &loaded));
        config.storage.surreal_db.query_timeout_secs = 9;
        assert!(!storage_config_matches(&config, &loaded));
        config.storage.surreal_db.query_timeout_secs = 12;
        config.storage.surreal_db.transaction_timeout_secs = 9;
        assert!(!storage_config_matches(&config, &loaded));

        let legacy = serialized
            .lines()
            .filter(|line| {
                !line.starts_with("query_timeout_secs =")
                    && !line.starts_with("transaction_timeout_secs =")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let loaded: ElephantConfig = toml::from_str(&legacy).expect("legacy config");
        assert_eq!(loaded.storage.surreal_db.query_timeout_secs, 30);
        assert_eq!(loaded.storage.surreal_db.transaction_timeout_secs, 30);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_database_timeouts_require_restart_before_live_config_is_saved() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let root = temp_test_dir("timeout-reconfigure");
        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        let mut config = ElephantConfig::default();
        config.storage.backend = StorageBackendKind::SurrealDb;
        config.storage.surreal_db.url = "mem://".to_string();
        config.runtime.ingest_enabled = false;
        let storage = StorageClient::new(storage_config_from_runtime(&config))
            .await
            .expect("storage");
        storage
            .query_checked("CREATE timeout_probe:retained CONTENT { value: true };")
            .await
            .expect("seed active database");
        save_config(&config_path, &config).expect("save config");
        let saved_before = fs::read(&config_path).expect("saved config");
        let runtime = RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path: config_path.clone(),
                config: Some(config),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus::bootstrap(),
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        };
        env::set_var("SURREALDB_QUERY_TIMEOUT_SECS", "31");
        let patch: RuntimeConfigPatch = serde_json::from_value(serde_json::json!({
            "extractor_model": "must-not-be-saved"
        }))
        .expect("patch");
        let error = runtime
            .configure_runtime(patch)
            .await
            .expect_err("live deadline change must be rejected");
        assert!(error.to_string().contains("requires a process restart"));
        assert_eq!(fs::read(&config_path).expect("config after"), saved_before);
        let inner = runtime.inner.read().await;
        assert_ne!(
            inner
                .config
                .as_ref()
                .expect("active config")
                .extractor
                .model,
            "must-not-be-saved"
        );
        let active = inner.storage.as_ref().expect("active storage");
        assert_eq!(active.config().query_timeout, Duration::from_secs(30));
        let mut response = active
            .query_checked("SELECT VALUE value FROM timeout_probe:retained;")
            .await
            .expect("old database remains available");
        let values: Vec<bool> = response.take(0).expect("probe value");
        assert_eq!(values, vec![true]);
        drop(inner);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_redact_config_hides_surreal_password() {
        let mut config = ElephantConfig::default();
        config.storage.surreal_db.password = "super-secret".to_string();

        let redacted = redact_config(config);
        assert_eq!(redacted.storage.surreal_db.password, "***redacted***");
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_from_env_uses_elephant_db_defaults() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_backend = env::var("ELEPHANT_DB").ok();
        let prior_snapshot = env::var("ELEPHANT_DB_SNAPSHOT_LOAD").ok();

        env::set_var("ELEPHANT_DB", "elephant-db");
        env::remove_var("ELEPHANT_DB_SNAPSHOT_LOAD");

        let config = ElephantConfig::from_env();
        assert_eq!(config.storage.backend, StorageBackendKind::ElephantDb);
        assert_eq!(config.runtime.mode, RuntimeMode::LocalMemory);
        assert_eq!(
            config.storage.elephant_db.snapshot_path,
            PathBuf::from(DEFAULT_SNAPSHOT_PATH)
        );

        match prior_backend {
            Some(value) => env::set_var("ELEPHANT_DB", value),
            None => env::remove_var("ELEPHANT_DB"),
        }
        match prior_snapshot {
            Some(value) => env::set_var("ELEPHANT_DB_SNAPSHOT_LOAD", value),
            None => env::remove_var("ELEPHANT_DB_SNAPSHOT_LOAD"),
        }
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_initialize_runtime_bootstrap_writes_config_and_reports_status() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let prior_cwd = env::current_dir().expect("cwd");
        let root = temp_test_dir("bootstrap");
        fs::create_dir_all(root.join(".git")).expect("git dir");
        env::set_current_dir(&root).expect("set cwd");

        let runtime = RuntimeManager::new().await.expect("runtime");
        let bootstrap = runtime.get_status().await;
        assert!(!bootstrap.configured);
        assert!(bootstrap.bootstrap_mode);

        let status = runtime
            .initialize_runtime(RuntimeConfigPatch {
                runtime_mode: None,
                ingest_enabled: Some(false),
                backend: Some(StorageBackendKind::ElephantDb),
                snapshot_path: None,
                surreal_url: None,
                surreal_namespace: None,
                surreal_database: None,
                surreal_username: None,
                surreal_password: None,
                extractor_provider: None,
                extractor_required: None,
                extractor_mcp_url: None,
                extractor_model: None,
                extractor_timeout_secs: None,
                multimodal_model: None,
                multimodal_timeout_secs: None,
                multimodal_max_render_concurrent: None,
                multimodal_asset_root: None,
                multimodal_inline_image_max_bytes: None,
                multimodal_inline_video_max_bytes: None,
                budget_daily_euros_retrieval: None,
                budget_daily_euros_graph: None,
                budget_daily_items_retrieval: None,
                budget_daily_items_graph: None,
                budget_per_item_tokens: None,
                budget_per_item_wall_time_secs: None,
                budget_downgrade_policy: None,
            })
            .await
            .expect("initialize runtime");

        assert!(status.configured);
        assert!(!status.bootstrap_mode);
        assert!(status.storage_ready);
        assert!(!status.semantic_ingest_ready);
        assert!(root.join(CONFIG_DIR).join(CONFIG_FILE).exists());

        runtime.shutdown().await;
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_snapshot_load_env_overrides_persisted_embedded_config() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let root = temp_test_dir("snapshot-env-overrides-config");
        let prior_cwd = env::current_dir().expect("current dir");
        fs::create_dir_all(root.join(".git")).expect("create git dir");
        env::set_current_dir(&root).expect("switch cwd");

        let stale_snapshot = root.join("stale.snapshot");
        let fresh_snapshot = root.join("fresh.snapshot");
        let stale_store = EmbeddedStore::new();
        let fresh_store = EmbeddedStore::new();
        let fresh_doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Fresh env-selected snapshot doc".to_string(),
            text: "snapshot override sentinel".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("fresh-env-{}", uuid::Uuid::new_v4()),
            metadata: serde_json::json!({}),
        };
        let fresh_doc_id = fresh_doc.base.id.clone();
        fresh_store
            .create(fresh_doc)
            .await
            .expect("create fresh doc");
        stale_store
            .export_snapshot(&stale_snapshot)
            .await
            .expect("export stale snapshot");
        fresh_store
            .export_snapshot(&fresh_snapshot)
            .await
            .expect("export fresh snapshot");

        let mut config = ElephantConfig::default();
        config.storage.backend = StorageBackendKind::ElephantDb;
        config.storage.elephant_db.snapshot_path = stale_snapshot;
        config.runtime.ingest_enabled = false;
        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        save_config(&config_path, &config).expect("save config");
        env::set_var("ELEPHANT_DB_SNAPSHOT_LOAD", &fresh_snapshot);

        let runtime = RuntimeManager::new().await.expect("runtime manager");
        let storage = runtime.current_storage().await.expect("runtime storage");
        let repo = DocRepository::new(storage);
        let loaded_doc = repo
            .get("default", &fresh_doc_id)
            .await
            .expect("get doc")
            .expect("fresh snapshot doc should load");
        assert_eq!(loaded_doc.title, "Fresh env-selected snapshot doc");

        runtime.shutdown().await;
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_from_env_reads_budget_settings() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_daily_euros_retrieval = env::var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL").ok();
        let prior_daily_items_graph = env::var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH").ok();
        let prior_per_item_tokens = env::var("PIPELINE_BUDGET_PER_ITEM_TOKENS").ok();
        let prior_downgrade_policy = env::var("PIPELINE_BUDGET_DOWNGRADE_POLICY").ok();
        let prior_multimodal_model = env::var("PIPELINE_MULTIMODAL_RENDER_MODEL").ok();
        let prior_multimodal_timeout = env::var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS").ok();
        let prior_multimodal_max_concurrent =
            env::var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT").ok();
        let prior_multimodal_asset_root = env::var("PIPELINE_MULTIMODAL_ASSET_ROOT").ok();
        let prior_inline_image_limit = env::var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES").ok();
        let prior_inline_video_limit = env::var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES").ok();

        env::set_var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL", "23.5");
        env::set_var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH", "42");
        env::set_var("PIPELINE_BUDGET_PER_ITEM_TOKENS", "54321");
        env::set_var("PIPELINE_BUDGET_DOWNGRADE_POLICY", "alert_only");
        env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", "gemini-3.1-pro-preview");
        env::set_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS", "777");
        env::set_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT", "6");
        env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", "/tmp/elephant-mm-assets");
        env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", "1111");
        env::set_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES", "2222");

        let config = ElephantConfig::from_env();
        assert!((config.budget.daily_euros_retrieval - 23.5).abs() < f64::EPSILON);
        assert_eq!(config.budget.daily_items_graph, Some(42));
        assert_eq!(config.budget.per_item_tokens, Some(54_321));
        assert_eq!(config.budget.downgrade_policy, DowngradePolicy::AlertOnly);
        assert_eq!(config.multimodal.model, "gemini-3.1-pro-preview");
        assert_eq!(config.multimodal.timeout_secs, 777);
        assert_eq!(config.multimodal.max_render_concurrent, 6);
        assert_eq!(
            config.multimodal.asset_root,
            PathBuf::from("/tmp/elephant-mm-assets")
        );
        assert_eq!(config.multimodal.inline_image_max_bytes, 1111);
        assert_eq!(config.multimodal.inline_video_max_bytes, 2222);

        match prior_daily_euros_retrieval {
            Some(value) => env::set_var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL", value),
            None => env::remove_var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL"),
        }
        match prior_daily_items_graph {
            Some(value) => env::set_var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH", value),
            None => env::remove_var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH"),
        }
        match prior_per_item_tokens {
            Some(value) => env::set_var("PIPELINE_BUDGET_PER_ITEM_TOKENS", value),
            None => env::remove_var("PIPELINE_BUDGET_PER_ITEM_TOKENS"),
        }
        match prior_downgrade_policy {
            Some(value) => env::set_var("PIPELINE_BUDGET_DOWNGRADE_POLICY", value),
            None => env::remove_var("PIPELINE_BUDGET_DOWNGRADE_POLICY"),
        }
        match prior_multimodal_model {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_MODEL"),
        }
        match prior_multimodal_timeout {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS"),
        }
        match prior_multimodal_max_concurrent {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT"),
        }
        match prior_multimodal_asset_root {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_ASSET_ROOT"),
        }
        match prior_inline_image_limit {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES"),
        }
        match prior_inline_video_limit {
            Some(value) => env::set_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES", value),
            None => env::remove_var("PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES"),
        }
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    #[allow(clippy::too_many_lines)]
    async fn test_configure_runtime_reuses_embedded_storage_when_only_extractor_changes() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_cwd = env::current_dir().expect("cwd");
        let root = temp_test_dir("reconfigure-reuse-storage");
        fs::create_dir_all(root.join(".git")).expect("git dir");
        env::set_current_dir(&root).expect("set cwd");

        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");
        let before = storage.embedded_store().expect("embedded store");
        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        save_config(&config_path, &ElephantConfig::default()).expect("save config");

        let runtime = RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path,
                config: Some(ElephantConfig::default()),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(StorageBackendKind::ElephantDb.as_str().to_string()),
                    storage_ready: true,
                    worker_ready: false,
                    extractor_ready: false,
                    semantic_ingest_ready: false,
                    conveyor: None,
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        };

        runtime
            .configure_runtime(RuntimeConfigPatch {
                runtime_mode: None,
                ingest_enabled: Some(false),
                backend: None,
                snapshot_path: None,
                surreal_url: None,
                surreal_namespace: None,
                surreal_database: None,
                surreal_username: None,
                surreal_password: None,
                extractor_provider: None,
                extractor_required: None,
                extractor_mcp_url: Some("http://127.0.0.1:3000/mcp/agent".to_string()),
                extractor_model: Some("gpt-5.5".to_string()),
                extractor_timeout_secs: None,
                multimodal_model: Some("gemini-3.1-flash-lite-preview".to_string()),
                multimodal_timeout_secs: Some(600),
                multimodal_max_render_concurrent: Some(9),
                multimodal_asset_root: Some(PathBuf::from("/tmp/elephant-multimodal-assets")),
                multimodal_inline_image_max_bytes: Some(4096),
                multimodal_inline_video_max_bytes: Some(8192),
                budget_daily_euros_retrieval: Some(25.0),
                budget_daily_euros_graph: None,
                budget_daily_items_retrieval: None,
                budget_daily_items_graph: None,
                budget_per_item_tokens: None,
                budget_per_item_wall_time_secs: None,
                budget_downgrade_policy: Some(DowngradePolicy::AlertOnly),
            })
            .await
            .expect("configure runtime");

        let after = runtime
            .current_storage()
            .await
            .expect("current storage")
            .embedded_store()
            .expect("embedded store after");
        assert!(std::sync::Arc::ptr_eq(&before, &after));
        let config = runtime.get_config_redacted().await.expect("config");
        let multimodal = config
            .get("multimodal")
            .and_then(toml::Value::as_table)
            .expect("multimodal table");
        assert_eq!(
            multimodal.get("model").and_then(toml::Value::as_str),
            Some("gemini-3.1-flash-lite-preview")
        );
        assert_eq!(
            multimodal
                .get("timeout_secs")
                .and_then(toml::Value::as_integer),
            Some(600)
        );
        assert_eq!(
            multimodal
                .get("max_render_concurrent")
                .and_then(toml::Value::as_integer),
            Some(9)
        );
        let budget = config
            .get("budget")
            .and_then(toml::Value::as_table)
            .expect("budget table");
        assert_eq!(
            budget
                .get("daily_euros_retrieval")
                .and_then(toml::Value::as_float),
            Some(25.0)
        );
        assert_eq!(
            budget.get("downgrade_policy").and_then(toml::Value::as_str),
            Some("alert_only")
        );

        runtime.shutdown().await;
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    async fn test_retry_extractor_readiness_promotes_runtime_when_checker_recovers() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let runtime = RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path: PathBuf::from(".elephant/elephant_config.toml"),
                config: Some(ElephantConfig::default()),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(StorageBackendKind::ElephantDb.as_str().to_string()),
                    storage_ready: true,
                    worker_ready: true,
                    extractor_ready: false,
                    semantic_ingest_ready: false,
                    conveyor: None,
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        };

        let attempts = StdArc::new(AtomicUsize::new(0));
        let checker_attempts = attempts.clone();
        let refreshed = runtime
            .retry_extractor_readiness_with(3, Duration::from_millis(1), move |_config| {
                let checker_attempts = checker_attempts.clone();
                async move { checker_attempts.fetch_add(1, Ordering::SeqCst) >= 1 }
            })
            .await
            .expect("retry readiness");

        assert_eq!(refreshed, ExtractorRefreshOutcome::Refreshed);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let status = runtime.get_status().await;
        assert!(status.extractor_ready);
        assert!(status.semantic_ingest_ready);
        assert!(!status
            .issues
            .iter()
            .any(|issue| issue == "semantic_extractor_unavailable"));
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    async fn test_refresh_extractor_readiness_until_ready_keeps_retrying_until_recovered() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let runtime = RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path: PathBuf::from(".elephant/elephant_config.toml"),
                config: Some(ElephantConfig::default()),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(StorageBackendKind::ElephantDb.as_str().to_string()),
                    storage_ready: true,
                    worker_ready: true,
                    extractor_ready: false,
                    semantic_ingest_ready: false,
                    conveyor: None,
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        };

        let attempts = StdArc::new(AtomicUsize::new(0));
        let checker_attempts = attempts.clone();
        let refreshed = runtime
            .refresh_extractor_readiness_until_ready_with(move |_config| {
                let checker_attempts = checker_attempts.clone();
                async move { checker_attempts.fetch_add(1, Ordering::SeqCst) >= 6 }
            })
            .await
            .expect("refresh readiness");

        assert!(refreshed);
        assert_eq!(attempts.load(Ordering::SeqCst), 7);

        let status = runtime.get_status().await;
        assert!(status.extractor_ready);
        assert!(status.semantic_ingest_ready);
    }

    #[cfg(not(feature = "meerkat-extraction"))]
    #[tokio::test]
    async fn test_prepare_runtime_reports_feature_unavailable_without_meerkat_extraction() {
        let config = ElephantConfig::default();

        let prepared = prepare_runtime(config, None)
            .await
            .expect("prepare runtime");

        assert!(!prepared.status.extractor_ready);
        assert!(!prepared.status.semantic_ingest_ready);
        assert!(prepared
            .status
            .issues
            .iter()
            .any(|issue| issue == ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE));
        assert!(!prepared
            .status
            .issues
            .iter()
            .any(|issue| issue == ISSUE_SEMANTIC_EXTRACTOR_UNAVAILABLE));
    }

    #[cfg(not(feature = "meerkat-extraction"))]
    #[tokio::test]
    async fn test_retry_extractor_readiness_is_not_applicable_without_feature() {
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let runtime = RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path: PathBuf::from(".elephant/elephant_config.toml"),
                config: Some(ElephantConfig::default()),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(StorageBackendKind::ElephantDb.as_str().to_string()),
                    storage_ready: true,
                    worker_ready: true,
                    extractor_ready: false,
                    semantic_ingest_ready: false,
                    conveyor: None,
                    issues: vec![ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE.to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        };

        let refreshed = runtime
            .retry_extractor_readiness_with(
                3,
                Duration::from_millis(1),
                move |_config| async move { true },
            )
            .await
            .expect("retry readiness");

        assert_eq!(refreshed, ExtractorRefreshOutcome::NotApplicable);

        let status = runtime.get_status().await;
        assert!(!status.extractor_ready);
        assert!(!status.semantic_ingest_ready);
        assert!(status
            .issues
            .iter()
            .any(|issue| issue == ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE));
    }

    #[tokio::test]
    async fn test_managed_pipeline_thread_starts_and_stops_on_embedded_storage() {
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;

        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_pipeline_handle(Some(pipeline)).await;
    }

    #[test]
    fn test_managed_pipeline_worker_threads_scales_with_enabled_workers() {
        let mut config = PipelineConfig::default();
        config.work_queue.max_concurrent = 4;
        config.embed.enabled = true;
        config.truth_maintenance.enabled = true;
        config.retention.enabled = true;
        config.maintenance.enabled = true;

        assert_eq!(managed_pipeline_worker_threads(&config), 8);

        let mut minimal = PipelineConfig::default();
        minimal.work_queue.max_concurrent = 1;
        minimal.embed.enabled = false;
        minimal.truth_maintenance.enabled = false;
        minimal.retention.enabled = false;
        minimal.maintenance.enabled = false;

        assert_eq!(managed_pipeline_worker_threads(&minimal), 4);
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_pipeline_config_from_runtime_preserves_pipeline_env_overrides() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        env::set_var("PIPELINE_EMBED_ENABLED", "0");
        env::set_var("PIPELINE_MAX_CONCURRENT", "1");
        env::set_var("PIPELINE_MAX_EXTRACT_CONCURRENT", "1");
        env::set_var("PIPELINE_WORK_QUEUE_BATCH_SIZE", "5");
        env::set_var(
            "PIPELINE_WORK_QUEUE_ALLOWED_KINDS",
            "plan_extraction,extract_assertions",
        );
        env::set_var("PIPELINE_WORK_QUEUE_DISABLED_KINDS", "render_doc");

        let mut config = ElephantConfig::default();
        config.storage.backend = StorageBackendKind::ElephantDb;
        config.storage.elephant_db.snapshot_path = PathBuf::from(".elephant/state.snapshot");

        let pipeline = pipeline_config_from_runtime(&config);

        assert!(!pipeline.embed.enabled);
        assert_eq!(pipeline.work_queue.max_concurrent, 1);
        assert_eq!(pipeline.work_queue.max_extract_concurrent, 1);
        assert_eq!(pipeline.work_queue.batch_size, 5);
        assert_eq!(
            pipeline.work_queue.allowed_kinds,
            vec![
                "plan_extraction".to_string(),
                "extract_assertions".to_string()
            ]
        );
        assert_eq!(pipeline.work_queue.disabled_kinds, vec!["render_doc"]);
        assert_eq!(pipeline.db_backend, StorageBackendKind::ElephantDb);
        assert_eq!(
            pipeline.db_snapshot_load,
            Some(PathBuf::from(".elephant/state.snapshot"))
        );
    }

    #[tokio::test]
    async fn test_managed_pipeline_thread_claims_runnable_work_items_on_embedded_storage() {
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;

        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");

        let repo = WorkItemRepository::new(storage.clone());
        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "default".to_string(),
                "tester".to_string(),
            ),
            kind: "unknown_work_kind".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("runtime-claim-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let item_id = item.base.id.clone();
        repo.create(item).await.expect("create work item");

        let mut transitioned = None;
        for _ in 0..40 {
            let fetched: WorkItem = repo
                .get("default", &item_id)
                .await
                .expect("get work item")
                .expect("work item exists");
            if fetched.attempts > 0 || fetched.status != WorkItemStatus::Runnable {
                transitioned = Some(fetched);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let fetched = transitioned.expect("managed pipeline should claim runnable work item");
        assert!(fetched.attempts > 0, "work item should have been attempted");
        assert_ne!(
            fetched.status,
            WorkItemStatus::Runnable,
            "work item should not remain runnable forever"
        );

        stop_pipeline_handle(Some(pipeline)).await;
    }

    #[tokio::test]
    async fn test_managed_pipeline_thread_claims_runnable_item_with_stale_lease_fields() {
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;

        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");

        let repo = WorkItemRepository::new(storage.clone());
        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "default".to_string(),
                "tester".to_string(),
            ),
            kind: "unknown_work_kind".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("runtime-stale-lease-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: Some(chrono::Utc::now() + chrono::Duration::minutes(10)),
            leased_by: Some("old-worker".to_string()),
            run_after: None,
            last_error: None,
        };
        let item_id = item.base.id.clone();
        repo.create(item).await.expect("create work item");

        let mut transitioned = None;
        for _ in 0..40 {
            let fetched: WorkItem = repo
                .get("default", &item_id)
                .await
                .expect("get work item")
                .expect("work item exists");
            if fetched.attempts > 0 || fetched.status != WorkItemStatus::Runnable {
                transitioned = Some(fetched);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let fetched = transitioned.expect("managed pipeline should claim stale runnable work item");
        assert!(fetched.attempts > 0, "work item should have been attempted");
        assert_ne!(
            fetched.status,
            WorkItemStatus::Runnable,
            "stale leased runnable work item should not remain runnable forever"
        );

        stop_pipeline_handle(Some(pipeline)).await;
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_runtime_manager_from_config_starts_managed_pipeline_on_embedded_storage() {
        let _guard = env_lock().lock().expect("env lock");
        let root = temp_test_dir("runtime-manager-managed-pipeline");
        let prior_cwd = env::current_dir().expect("current dir");
        fs::create_dir_all(&root).expect("create temp root");
        env::set_current_dir(&root).expect("switch cwd");

        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        save_config(&config_path, &ElephantConfig::default()).expect("save config");

        let runtime = RuntimeManager::new().await.expect("runtime manager");
        let storage = runtime.current_storage().await.expect("runtime storage");
        let repo = WorkItemRepository::new(storage);

        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "default".to_string(),
                "tester".to_string(),
            ),
            kind: "unknown_work_kind".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("runtime-manager-claim-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let item_id = item.base.id.clone();
        repo.create(item).await.expect("create work item");

        let mut transitioned = None;
        for _ in 0..40 {
            let fetched: WorkItem = repo
                .get("default", &item_id)
                .await
                .expect("get work item")
                .expect("work item exists");
            if fetched.attempts > 0 || fetched.status != WorkItemStatus::Runnable {
                transitioned = Some(fetched);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let fetched =
            transitioned.expect("runtime-managed pipeline should claim runnable work item");
        assert!(fetched.attempts > 0, "work item should have been attempted");
        assert_ne!(
            fetched.status,
            WorkItemStatus::Runnable,
            "work item should not remain runnable forever"
        );

        runtime.shutdown().await;
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_runtime_manager_claims_real_triage_source_work_item_on_embedded_storage() {
        let _guard = env_lock().lock().expect("env lock");
        let root = temp_test_dir("runtime-manager-triage-claim");
        let prior_cwd = env::current_dir().expect("current dir");
        fs::create_dir_all(&root).expect("create temp root");
        env::set_current_dir(&root).expect("switch cwd");

        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        save_config(&config_path, &ElephantConfig::default()).expect("save config");

        let runtime = RuntimeManager::new().await.expect("runtime manager");
        let storage = runtime.current_storage().await.expect("runtime storage");
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "runtime-triage-claim".to_string(),
        );
        source.uri = Some("example://runtime-manager/triage-claim".to_string());
        let source = SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("create source");

        let work_item_id = enqueue_triage_source_work_item(&storage, &source, "tester", 5)
            .await
            .expect("enqueue triage_source");

        let repo = WorkItemRepository::new(storage);
        let mut transitioned = None;
        for _ in 0..40 {
            let fetched = repo
                .get("default", &work_item_id)
                .await
                .expect("get work item")
                .expect("work item exists");
            if fetched.attempts > 0 || fetched.status != WorkItemStatus::Runnable {
                transitioned = Some(fetched);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let fetched = transitioned.expect("managed pipeline should claim triage_source work item");
        assert!(
            fetched.attempts > 0,
            "triage_source should have been attempted"
        );
        assert_ne!(
            fetched.status,
            WorkItemStatus::Runnable,
            "triage_source should not remain runnable forever"
        );

        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    async fn test_runtime_manager_claims_burst_of_triage_items_within_single_poll_window() {
        let _guard = env_lock().lock().expect("env lock");
        let root = temp_test_dir("runtime-manager-burst-triage-claim");
        let prior_cwd = env::current_dir().expect("current dir");
        fs::create_dir_all(&root).expect("create temp root");
        env::set_current_dir(&root).expect("switch cwd");

        let config_path = root.join(CONFIG_DIR).join(CONFIG_FILE);
        save_config(&config_path, &ElephantConfig::default()).expect("save config");

        let runtime = RuntimeManager::new().await.expect("runtime manager");
        let storage = runtime.current_storage().await.expect("runtime storage");
        let source_repo = SourceRepository::new(storage.clone());
        let work_item_repo = WorkItemRepository::new(storage.clone());

        let mut work_item_ids = Vec::new();
        for idx in 0..4 {
            let mut source = Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                SourceType::Manual,
                format!("runtime-burst-triage-{idx}"),
            );
            source.uri = Some(format!("example://runtime-manager/burst/{idx}"));
            let source = source_repo.create(source).await.expect("create source");
            let work_item_id = enqueue_triage_source_work_item(&storage, &source, "tester", 5)
                .await
                .expect("enqueue triage_source");
            work_item_ids.push(work_item_id);
        }

        let started = tokio::time::Instant::now();
        let mut claimed = vec![false; work_item_ids.len()];
        loop {
            for (idx, work_item_id) in work_item_ids.iter().enumerate() {
                let fetched = work_item_repo
                    .get("default", work_item_id)
                    .await
                    .expect("get work item")
                    .expect("work item exists");
                if fetched.attempts > 0 || fetched.status != WorkItemStatus::Runnable {
                    claimed[idx] = true;
                }
            }
            if claimed.iter().all(|done| *done) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(8),
                "expected all burst triage items to be claimed on the first poll tick, claimed={claimed:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        runtime.shutdown().await;
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    fn embedded_test_runtime(storage: StorageClient) -> RuntimeManager {
        RuntimeManager {
            inner: RwLock::new(RuntimeInner {
                config_path: PathBuf::from(".elephant/elephant_config.toml"),
                config: Some(ElephantConfig::default()),
                storage: Some(storage),
                pipeline: None,
                status: RuntimeStatus {
                    configured: true,
                    bootstrap_mode: false,
                    runtime_mode: RuntimeMode::LocalMemory,
                    backend: Some(StorageBackendKind::ElephantDb.as_str().to_string()),
                    storage_ready: true,
                    worker_ready: true,
                    extractor_ready: true,
                    semantic_ingest_ready: true,
                    conveyor: None,
                    issues: vec![],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            embedding_progress: EmbeddingProgressCache::default(),
        }
    }

    /// 2026-09-23: tool gating ran the full status, whose conveyor count scans
    /// every work item; one call took 14-30 s and every MCP list/call paid it.
    /// Readiness must not compute the conveyor, and the full status must reuse
    /// a recent computation instead of scanning again.
    #[tokio::test]
    async fn readiness_skips_the_conveyor_and_status_reuses_it() {
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");
        let runtime = embedded_test_runtime(storage);

        let readiness = runtime.get_readiness().await;
        assert!(
            readiness.conveyor.is_none(),
            "readiness must not scan the queue"
        );
        assert!(runtime.conveyor_cache.lock().await.is_none());

        let first = runtime.get_status().await;
        assert!(
            first.conveyor.is_some(),
            "full status still reports the conveyor"
        );
        let computed_at = runtime
            .conveyor_cache
            .lock()
            .await
            .as_ref()
            .map(|(at, _)| *at);
        assert!(computed_at.is_some());

        let second = runtime.get_status().await;
        assert!(second.conveyor.is_some());
        let reused_at = runtime
            .conveyor_cache
            .lock()
            .await
            .as_ref()
            .map(|(at, _)| *at);
        assert_eq!(
            computed_at, reused_at,
            "a status within the TTL must not rescan"
        );
    }
}
