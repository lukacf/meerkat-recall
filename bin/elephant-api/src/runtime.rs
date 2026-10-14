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

#[path = "runtime_recipe.rs"]
mod recipe;
use recipe::ManagedWorkerRecipe;

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

/// Budget settings shared by managed workers, enforced independently per space.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeBudgetConfiguration {
    /// Stored runtime budget, before the managed-worker environment projection.
    pub configured: Budget,
    /// Budget the managed worker constructs from the runtime settings.
    pub effective: Budget,
    /// Whether this runtime currently owns a live managed worker.
    pub managed_workers_running: bool,
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
    /// Space selected for MCP readiness discovery, not a grant for other spaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_space_id: Option<String>,
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
            readiness_space_id: None,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup_diagnostics: Vec<crate::setup_diagnostics::SetupDiagnostic>,
}

/// Whether derivation work can actually flow, and if not, why.
///
/// The distinction that matters is between an EMPTY queue and a BLOCKED one.
/// Both produce zero assertions per hour. An empty queue is idle; a blocked
/// queue has pending work that cannot run and needs attention.
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
}

/// Decides whether a stalled conveyor is a fault, and names the cause.
///
/// An EMPTY queue and a BLOCKED queue both show zero throughput, but only a
/// blocked queue needs attention. Exhausted budget alone is not a fault:
/// report a fault only when pending work cannot start.
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
            setup_diagnostics: Vec::new(),
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
    pub extractor_readiness_space_id: Option<String>,
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
        if let Some(space) = &self.extractor_readiness_space_id {
            config.extractor.readiness_space_id = (!space.is_empty()).then(|| space.clone());
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
    recipe: Option<ManagedWorkerRecipe>,
    shutdown_tx: watch::Sender<bool>,
    handle: thread::JoinHandle<anyhow::Result<()>>,
    stopped: StdArc<AtomicBool>,
    last_error: StdArc<StdMutex<Option<String>>>,
}

impl ManagedPipeline {
    /// Early builder errors and unwinding may finish before the normal stop flag.
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst) || self.handle.is_finished()
    }
}

#[expect(
    clippy::missing_fields_in_debug,
    reason = "Preserve the existing runtime diagnostic fields with the worker recipe omitted"
)]
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
    listener_bound: AtomicBool,
    listener_address: RwLock<Option<std::net::SocketAddr>>,
    shutting_down: AtomicBool,
    workers_drained: AtomicBool,
    snapshot_paused: AtomicBool,
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
        Self::new_with_config_path(None, ingest_enabled).await
    }

    /// Load an explicitly selected config, then `ELEPHANT_CONFIG`, otherwise
    /// retain nearest-project discovery and the legacy environment fallback.
    ///
    /// # Errors
    ///
    /// An explicitly selected file must exist and parse; it never silently
    /// selects another database or falls back to bootstrap/environment config.
    pub async fn new_with_config_path(
        config_path: Option<&Path>,
        ingest_enabled: Option<bool>,
    ) -> Result<Self> {
        let (config_path, explicit) = selected_config_path(config_path)?;
        let mut inner = RuntimeInner {
            config_path: config_path.clone(),
            config: None,
            storage: None,
            pipeline: None,
            status: RuntimeStatus::bootstrap(),
        };

        if explicit || inner.config_path.exists() {
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
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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
                        ingest_enabled: false,
                    },
                    storage: StorageSection {
                        backend: storage.backend(),
                        ..StorageSection {
                            backend: storage.backend(),
                            elephant_db: EmbeddedStorageSection {
                                snapshot_path: storage
                                    .effective_snapshot_destination()
                                    .map(Path::to_path_buf)
                                    .unwrap_or_default(),
                            },
                            surreal_db: SurrealStorageSection {
                                url: storage.config().url.clone(),
                                namespace: storage.config().namespace.clone(),
                                database: storage.config().database.clone(),
                                username: storage.config().username.clone(),
                                password: storage.config().password.clone(),
                                query_timeout_secs: storage.config().query_timeout.as_secs(),
                                transaction_timeout_secs: storage
                                    .config()
                                    .transaction_timeout
                                    .as_secs(),
                            },
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
                    extractor_ready: false,
                    semantic_ingest_ready: false,
                    conveyor: None,
                    setup_diagnostics: Vec::new(),
                    issues: Vec::new(),
                },
            }),
            reconfigure: Mutex::new(()),
            readiness_refresh: Mutex::new(()),
            conveyor_cache: Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_storage_for_tests(storage: StorageClient) -> Self {
        let mut runtime = Self::new_preconfigured(storage);
        let inner = runtime.inner.get_mut();
        inner.status.extractor_ready = true;
        inner.status.semantic_ingest_ready = true;
        runtime
    }

    /// Readiness only: configured, storage/worker/ingest readiness, mode and
    /// issues. No database work, so it is safe on every request path. This is
    /// what tool gating needs; it never reads the conveyor.
    pub async fn get_readiness(&self) -> RuntimeStatus {
        self.refresh_pipeline_liveness().await;
        let inner = self.inner.read().await;
        let mut status = inner.status.clone();
        if self.snapshot_paused.load(Ordering::SeqCst) {
            status.storage_ready = false;
            status.semantic_ingest_ready = false;
            status.issues.push("snapshot_quiesce".into());
        }
        if inner
            .storage
            .as_ref()
            .is_some_and(|storage| !storage.recovery_status().ready)
        {
            status.storage_ready = false;
            status.semantic_ingest_ready = false;
            status.issues.push("recovery_barrier".into());
        }
        if status
            .issues
            .iter()
            .any(|issue| issue == ISSUE_SEMANTIC_EXTRACTOR_UNAVAILABLE)
        {
            if let Some(config) = &inner.config {
                status
                    .setup_diagnostics
                    .push(crate::setup_diagnostics::extractor_unavailable(
                        &config.extractor.mcp_url,
                    ));
            }
        }
        status
    }

    pub(crate) async fn record_listener_address(&self, address: std::net::SocketAddr) {
        *self.listener_address.write().await = Some(address);
    }

    /// Local keys and grants can diagnose only this exact service's MCP route.
    /// Remote and reverse-proxied endpoints retain their remote authority.
    pub(crate) async fn extractor_uses_local_authority(&self) -> bool {
        let Some(address) = *self.listener_address.read().await else {
            return false;
        };
        let inner = self.inner.read().await;
        inner.config.as_ref().is_some_and(|config| {
            crate::setup_diagnostics::is_local_agent_endpoint(&config.extractor.mcp_url, address)
        })
    }

    pub async fn workers_required(&self) -> bool {
        self.inner
            .read()
            .await
            .config
            .as_ref()
            .is_some_and(|config| {
                config.runtime.mode.is_write_capable() && config.runtime.ingest_enabled
            })
    }

    #[cfg(test)]
    pub(crate) async fn set_mode_for_tests(&self, mode: RuntimeMode) {
        let mut inner = self.inner.write().await;
        inner.status.runtime_mode = mode;
        if let Some(config) = &mut inner.config {
            config.runtime.mode = mode;
        }
    }

    #[cfg(test)]
    pub(crate) async fn require_workers_for_tests(&self) {
        let mut inner = self.inner.write().await;
        if let Some(config) = &mut inner.config {
            config.runtime.ingest_enabled = true;
        }
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
        if let Some(conveyor) = self.cached_conveyor_status().await {
            if conveyor.blocked {
                let issue = conveyor.blocked_reason.clone().map_or_else(
                    || "conveyor_blocked".to_string(),
                    |reason| format!("conveyor_blocked:{reason}"),
                );
                if !status.issues.iter().any(|existing| existing == &issue) {
                    status.issues.push(issue);
                }
            }
            status.conveyor = Some(conveyor);
        }

        status
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
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
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
    pub async fn get_config_redacted(&self) -> Option<toml::Value> {
        self.refresh_pipeline_liveness().await;
        let inner = self.inner.read().await;
        let config = inner.config.clone()?;
        let mut value = toml::Value::try_from(redact_config(config)).ok()?;
        if let Some(recipe) = inner.pipeline.as_ref().and_then(|pipeline| {
            (!pipeline.is_stopped())
                .then_some(pipeline.recipe.as_ref())
                .flatten()
        }) {
            // Authorized global configuration inspection only. This value is
            // neither persisted configuration nor public health/readiness data.
            value.as_table_mut()?.insert(
                "managed_worker_recipe".into(),
                toml::Value::try_from(recipe).ok()?,
            );
        }
        Some(value)
    }

    /// Effective managed-worker configuration, without credentials or provider calls.
    /// External worker processes may use different settings and are not attested.
    pub async fn budget_configuration(&self) -> Option<RuntimeBudgetConfiguration> {
        let inner = self.inner.read().await;
        let configured = inner.config.as_ref()?.budget.clone();
        let managed_workers_running = inner
            .pipeline
            .as_ref()
            .is_some_and(|worker| !worker.is_stopped());
        drop(inner);
        let defaults = Budget::default();
        // start_pipeline removes absent optional variables; the worker's env
        // loader then retains Budget::default for those fields. It does not
        // load concurrent_workers. Report that existing behavior faithfully.
        let effective = Budget {
            daily_items_graph: configured.daily_items_graph.or(defaults.daily_items_graph),
            daily_items_retrieval: configured
                .daily_items_retrieval
                .or(defaults.daily_items_retrieval),
            per_item_tokens: configured.per_item_tokens.or(defaults.per_item_tokens),
            per_item_wall_time_secs: configured
                .per_item_wall_time_secs
                .or(defaults.per_item_wall_time_secs),
            concurrent_workers: defaults.concurrent_workers,
            ..configured
        };
        Some(RuntimeBudgetConfiguration {
            configured,
            effective,
            managed_workers_running,
        })
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
    pub async fn initialize_runtime(&self, patch: RuntimeConfigPatch) -> Result<RuntimeStatus> {
        let _guard = self.reconfigure.lock().await;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::SeqCst),
            "runtime is draining"
        );
        let config_path = {
            let inner = self.inner.read().await;
            if inner.config.is_some() {
                return Err(anyhow!("runtime is already configured"));
            }
            inner.config_path.clone()
        };

        let config = patch.apply_to(ElephantConfig::default());
        let prepared = prepare_runtime_with_schema(config.clone(), None, true).await?;
        self.persist_configuration_or_drain(&config_path, &config)
            .await?;

        let old_pipeline = {
            let mut inner = self.inner.write().await;
            if inner.config.is_some() {
                return Err(anyhow!("runtime is already configured"));
            }
            inner.pipeline.take()
        };
        if stop_pipeline_handle(old_pipeline).await.is_err() {
            self.drain_after_configuration_failure("configuration_activation_failed", true)
                .await;
            return Err(anyhow!("configuration was saved but the previous workers did not stop; runtime is draining and restart is required"));
        }

        let mut inner = self.inner.write().await;
        if inner.config.is_some() {
            return Err(anyhow!("runtime is already configured"));
        }
        apply_prepared_runtime(&mut inner, prepared);
        if self.listener_bound.load(Ordering::SeqCst) {
            start_prepared_workers(&mut inner)?;
        }
        Ok(inner.status.clone())
    }

    /// Applies a runtime config patch to an already-configured runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not configured, if the config
    /// cannot be written, or if storage/worker restart fails.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
    pub async fn configure_runtime(&self, patch: RuntimeConfigPatch) -> Result<RuntimeStatus> {
        let _guard = self.reconfigure.lock().await;
        anyhow::ensure!(
            !self.shutting_down.load(Ordering::SeqCst),
            "runtime is draining"
        );
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
        let reused_storage = current_storage.filter(|_| storage_config_matches(&current, &config));
        let prepared = prepare_runtime(config.clone(), reused_storage).await?;
        self.persist_configuration_or_drain(&config_path, &config)
            .await?;

        let old_pipeline = {
            let mut inner = self.inner.write().await;
            inner
                .config
                .as_ref()
                .ok_or_else(|| anyhow!("runtime is not configured"))?;
            inner.pipeline.take()
        };
        if stop_pipeline_handle(old_pipeline).await.is_err() {
            self.drain_after_configuration_failure("configuration_activation_failed", true)
                .await;
            return Err(anyhow!("configuration was saved but the previous workers did not stop; runtime is draining and restart is required"));
        }

        let mut inner = self.inner.write().await;
        inner
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("runtime is not configured"))?;
        apply_prepared_runtime(&mut inner, prepared);
        if self.listener_bound.load(Ordering::SeqCst) {
            start_prepared_workers(&mut inner)?;
        }
        Ok(inner.status.clone())
    }

    // The caller already holds reconfigure. Never call shutdown() here:
    // shutdown acquires that same lock.
    async fn persist_configuration_or_drain(
        &self,
        path: &Path,
        config: &ElephantConfig,
    ) -> Result<()> {
        match save_config(path, config) {
            Ok(()) => Ok(()),
            Err(error) => {
                if matches!(
                    error.downcast_ref::<elephant_storage::ConfigWriteError>(),
                    Some(elephant_storage::ConfigWriteError::CommitUncertain(_))
                ) {
                    self.drain_after_configuration_failure(
                        "configuration_persistence_uncertain",
                        false,
                    )
                    .await;
                    return Err(error.context("configuration replacement durability is uncertain; runtime is draining and restart is required"));
                }
                Err(error)
            }
        }
    }

    async fn drain_after_configuration_failure(
        &self,
        issue: &'static str,
        prior_drain_failed: bool,
    ) {
        self.begin_shutdown().await;
        self.workers_drained.store(false, Ordering::SeqCst);
        let pipeline = {
            let mut inner = self.inner.write().await;
            if !inner.status.issues.iter().any(|existing| existing == issue) {
                inner.status.issues.push(issue.into());
            }
            if !inner
                .status
                .issues
                .iter()
                .any(|existing| existing == "configuration_restart_required")
            {
                inner
                    .status
                    .issues
                    .push("configuration_restart_required".into());
            }
            inner.pipeline.take()
        };
        let drained = stop_pipeline_handle(pipeline).await.is_ok() && !prior_drain_failed;
        self.workers_drained.store(drained, Ordering::SeqCst);
        if !drained {
            let mut inner = self.inner.write().await;
            if !inner
                .status
                .issues
                .iter()
                .any(|existing| existing == "worker_drain_failed")
            {
                inner.status.issues.push("worker_drain_failed".into());
            }
        }
    }

    /// Stops accepting writes before waiting for the worker drain.
    pub async fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let mut inner = self.inner.write().await;
        inner.status.semantic_ingest_ready = false;
        inner.status.worker_ready = false;
        inner.status.storage_ready = false;
        if !inner
            .status
            .issues
            .iter()
            .any(|issue| issue == "runtime_draining")
        {
            inner.status.issues.push("runtime_draining".into());
        }
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// Starts workers only after the managed API has bound its listener.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is draining or prepared workers cannot start.
    pub async fn start_workers_after_bind(&self) -> Result<()> {
        let _guard = self.reconfigure.lock().await;
        anyhow::ensure!(!self.is_draining(), "runtime is draining");
        self.listener_bound.store(true, Ordering::SeqCst);
        let mut inner = self.inner.write().await;
        start_prepared_workers(&mut inner)
    }

    /// Online snapshot operations run to completion even if the HTTP caller disconnects.
    /// The storage barrier drains admitted HTTP/MCP and worker activities after workers stop.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid retention, unavailable or draining runtime,
    /// failed worker quiescence, snapshot or restore failure, or worker restart
    /// and snapshot task failures.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
    pub async fn snapshot_operation(
        self: &std::sync::Arc<Self>,
        path: PathBuf,
        name: String,
        retention: elephant_storage::snapshot::SnapshotRetention,
        restore: bool,
    ) -> Result<elephant_storage::snapshot::SnapshotManifest> {
        anyhow::ensure!(
            (1..=60).contains(&retention.quiesce_timeout_seconds),
            "quiesce timeout must be 1..=60 seconds"
        );
        anyhow::ensure!(
            (1..=2_592_000).contains(&retention.retention_seconds),
            "retention must be 1..=2592000 seconds"
        );
        let runtime = self.clone();
        tokio::spawn(async move {
            let _guard = runtime.reconfigure.lock().await;
            anyhow::ensure!(!runtime.is_draining(), "runtime is draining");
            let (storage, pipeline) = {
                let mut inner = runtime.inner.write().await;
                anyhow::ensure!(
                    !inner
                        .status
                        .issues
                        .iter()
                        .any(|issue| issue == "worker_drain_failed"),
                    "previous worker drain failed"
                );
                let storage = inner
                    .storage
                    .clone()
                    .ok_or_else(|| anyhow!("runtime storage unavailable"))?;
                runtime.snapshot_paused.store(true, Ordering::SeqCst);
                (storage, inner.pipeline.take())
            };
            let started = std::time::Instant::now();
            let budget = Duration::from_secs(retention.quiesce_timeout_seconds);
            if let Err(error) = stop_pipeline_handle_with_timeout(pipeline, budget).await {
                runtime
                    .inner
                    .write()
                    .await
                    .status
                    .issues
                    .push("worker_drain_failed".into());
                // Keep admission closed: a timed-out worker might still be running.
                return Err(error);
            }
            let remaining = budget.saturating_sub(started.elapsed());
            let result = if remaining.as_secs() == 0 {
                Err(anyhow!(
                    "snapshot quiesce budget exhausted before storage barrier"
                ))
            } else if restore {
                match storage.erasure_journal() {
                    Ok(journal) => elephant_storage::snapshot::restore_named_with_timeout(
                        &storage, &path, journal, remaining,
                    )
                    .await
                    .map_err(anyhow::Error::from),
                    Err(error) => Err(error.into()),
                }
            } else if started.elapsed() >= budget {
                Err(anyhow!(
                    "snapshot quiesce budget exhausted before storage barrier"
                ))
            } else {
                let remaining = remaining.as_secs();
                let retention = elephant_storage::snapshot::SnapshotRetention {
                    quiesce_timeout_seconds: remaining,
                    ..retention
                };
                elephant_storage::snapshot::export_named(&storage, &path, &name, &retention)
                    .await
                    .map_err(anyhow::Error::from)
            };
            if storage.recovery_status().ready && !runtime.is_draining() {
                let mut inner = runtime.inner.write().await;
                inner.status.storage_ready = true;
                if runtime.listener_bound.load(Ordering::SeqCst) {
                    start_prepared_workers(&mut inner)?;
                }
                runtime.snapshot_paused.store(false, Ordering::SeqCst);
            }
            result
        })
        .await
        .context("snapshot task failed")?
    }

    /// The effective configuration owns both snapshot loading and autosave.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
    )]
    pub async fn snapshot_autosave_path(&self) -> Option<PathBuf> {
        let inner = self.inner.read().await;
        let config = inner.config.as_ref()?;
        (config.storage.backend == StorageBackendKind::ElephantDb
            && !config
                .storage
                .elephant_db
                .snapshot_path
                .as_os_str()
                .is_empty())
        .then(|| config.storage.elephant_db.snapshot_path.clone())
    }

    /// Persists only after a successful worker drain; a timeout is not quiescence.
    ///
    /// # Errors
    ///
    /// Returns an error if workers have not drained, storage is unavailable,
    /// or the configured shutdown snapshot cannot be persisted.
    pub async fn persist_shutdown_snapshot(&self) -> Result<()> {
        anyhow::ensure!(
            self.is_draining() && self.workers_drained.load(Ordering::SeqCst),
            "snapshot requires a completed worker drain"
        );
        let Some(path) = self.snapshot_autosave_path().await else {
            return Ok(());
        };
        let storage = self
            .inner
            .read()
            .await
            .storage
            .clone()
            .ok_or_else(|| anyhow!("runtime storage unavailable"))?;
        storage
            .export_snapshot(path)
            .await
            .context("persist shutdown snapshot")
    }

    /// Stops admission and drains the managed workers.
    ///
    /// # Errors
    ///
    /// Returns an error if a prior worker drain failed or the current workers
    /// cannot be drained successfully.
    pub async fn shutdown(&self) -> Result<()> {
        self.begin_shutdown().await;
        let _guard = self.reconfigure.lock().await;
        let pipeline = {
            let mut inner = self.inner.write().await;
            anyhow::ensure!(
                !inner
                    .status
                    .issues
                    .iter()
                    .any(|issue| issue == "worker_drain_failed"),
                "prior worker drain failed; restart before exporting a snapshot"
            );
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
        if let Err(error) = stop_pipeline_handle(pipeline).await {
            self.inner
                .write()
                .await
                .status
                .issues
                .push("worker_drain_failed".into());
            return Err(error);
        }
        self.workers_drained.store(true, Ordering::SeqCst);
        Ok(())
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
                if self.is_draining()
                    || inner.status.extractor_ready
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
                    inner.status.semantic_ingest_ready = !self.is_draining()
                        && inner.status.storage_ready
                        && inner.status.worker_ready;
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
            .is_some_and(ManagedPipeline::is_stopped);
        if !stopped {
            return;
        }
        let mut inner = self.inner.write().await;
        let Some(pipeline) = inner.pipeline.as_ref() else {
            return;
        };
        let stopped = pipeline.is_stopped();
        let failed = pipeline
            .last_error
            .lock()
            .is_ok_and(|guard| guard.is_some());

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
        // Readiness is available to scoped callers. Detailed worker failures
        // may contain another space's data or provider diagnostics; keep those
        // in the private pipeline handle, never in public status strings.
        if failed
            && !inner
                .status
                .issues
                .iter()
                .any(|issue| issue == "worker_runtime_stopped")
        {
            inner.status.issues.push("worker_runtime_stopped".into());
        }
    }
}

impl ElephantConfig {
    fn from_env() -> Self {
        let backend = env::var("ELEPHANT_DB")
            .ok()
            .and_then(|value| value.parse::<StorageBackendKind>().ok())
            .unwrap_or(StorageBackendKind::SurrealDb);
        let mode = match backend {
            StorageBackendKind::ElephantDb => RuntimeMode::LocalMemory,
            StorageBackendKind::SurrealDb => RuntimeMode::SharedMemory,
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
                readiness_space_id: env::var("PIPELINE_AGENTIC_MCP_SPACE_ID").ok(),
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

/// Resolves the same file/environment storage authority used by serve without
/// opening storage, applying schema, starting workers, or changing config.
///
/// # Errors
///
/// Returns an error when the current directory or configuration cannot be
/// read, runtime configuration is absent, or storage timeouts are invalid.
pub fn resolve_storage_config_for_cli() -> Result<StorageConfig> {
    resolve_storage_config_for_cli_with_path(None)
}

/// Resolve CLI storage from the same explicit config selection as `serve`.
///
/// # Errors
///
/// Explicit paths must exist and parse; storage timeouts must be valid.
pub fn resolve_storage_config_for_cli_with_path(path: Option<&Path>) -> Result<StorageConfig> {
    let (path, explicit) = selected_config_path(path)?;
    let mut config = if explicit || path.exists() {
        let mut config = load_config_from_path(&path)?;
        apply_env_overrides_to_loaded_config(&mut config);
        config
    } else if env_runtime_config_present() {
        ElephantConfig::from_env()
    } else {
        return Err(anyhow!("runtime is unconfigured; initialize a configuration or set explicit storage environment before migration"));
    };
    resolve_storage_config(&mut config)
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
    config: ElephantConfig,
    existing_storage: Option<StorageClient>,
) -> Result<PreparedRuntime> {
    prepare_runtime_with_schema(config, existing_storage, false).await
}

async fn prepare_runtime_with_schema(
    mut config: ElephantConfig,
    existing_storage: Option<StorageClient>,
    initialize_schema: bool,
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
        let destination = (config.storage.backend == StorageBackendKind::ElephantDb)
            .then(|| config.storage.elephant_db.snapshot_path.clone());
        StorageClient::new_with_snapshot_destination_diagnosed(storage_config, destination)
            .await
            .context("failed to initialize configured storage")?
    };
    let recovery_ready = storage_client.recovery_status().ready;
    if recovery_ready && initialize_schema {
        elephant_migrations::apply_schema(&storage_client)
            .await
            .map_err(|error| anyhow!("explicit schema initialization failed: {error}"))?;
    }
    if recovery_ready {
        elephant_migrations::verify_schema(&storage_client)
            .await
            .map_err(|error| {
                anyhow!("schema verification failed; run migrate explicitly: {error}")
            })?;
    }
    // Preparing storage must not start workers or probe an MCP endpoint that
    // cannot accept connections yet. The API explicitly starts both after bind.
    let extractor_ready = !config.extractor.required;
    let pipeline = None;
    let worker_ready = false;

    let backend = config.storage.backend.as_str().to_string();
    let semantic_ingest_ready = config.runtime.ingest_enabled && worker_ready && extractor_ready;
    let mut issues = Vec::new();
    if !recovery_ready {
        issues.push("recovery_barrier".into());
    }
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
            storage_ready: recovery_ready,
            worker_ready,
            extractor_ready,
            semantic_ingest_ready,
            conveyor: None,
            setup_diagnostics: Vec::new(),
            issues,
        },
        config,
        storage: storage_client,
        pipeline,
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
)]
fn start_prepared_workers(inner: &mut RuntimeInner) -> Result<()> {
    if inner
        .storage
        .as_ref()
        .is_some_and(|storage| !storage.recovery_status().ready)
    {
        return Ok(());
    }
    let Some(config) = inner.config.as_ref() else {
        return Ok(());
    };
    if inner.pipeline.is_some()
        || !config.runtime.mode.is_write_capable()
        || !config.runtime.ingest_enabled
    {
        return Ok(());
    }
    let storage = inner
        .storage
        .as_ref()
        .ok_or_else(|| anyhow!("storage unavailable"))?;
    anyhow::ensure!(
        storage.recovery_status().ready,
        "storage recovery barrier is not ready"
    );
    inner.pipeline = Some(start_pipeline(config, storage)?);
    inner.status.worker_ready = true;
    inner.status.semantic_ingest_ready = inner.status.storage_ready && inner.status.extractor_ready;
    inner
        .status
        .issues
        .retain(|issue| issue != "worker_runtime_unavailable");
    Ok(())
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
    let recipe = ManagedWorkerRecipe::capture(&pipeline_config);
    let extractor_mcp_url = config.extractor.mcp_url.clone();
    let extractor_model = config.extractor.model.clone();
    let extractor_readiness_space = config.extractor.readiness_space_id.clone();
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
            if let Some(space) = &extractor_readiness_space {
                env::set_var("PIPELINE_AGENTIC_MCP_SPACE_ID", space);
            } else {
                env::remove_var("PIPELINE_AGENTIC_MCP_SPACE_ID");
            }
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
        recipe: Some(recipe),
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
        && left.extractor.readiness_space_id == right.extractor.readiness_space_id
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

async fn stop_pipeline_handle(pipeline: Option<ManagedPipeline>) -> Result<()> {
    stop_pipeline_handle_with_timeout(pipeline, Duration::from_secs(30)).await
}

async fn stop_pipeline_handle_with_timeout(
    pipeline: Option<ManagedPipeline>,
    timeout: Duration,
) -> Result<()> {
    if let Some(pipeline) = pipeline {
        let _ = pipeline.shutdown_tx.send(true);
        let handle = pipeline.handle;
        tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || handle.join()))
            .await
            .context("worker drain timed out; snapshot is unsafe")?
            .context("worker join failed")?
            .map_err(|_| anyhow!("worker thread panicked"))??;
    }
    Ok(())
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
            .with_connect_on_init(false)
            .with_readiness_space(config.extractor.readiness_space_id.clone()),
        );
        return extractor.health_check().await.unwrap_or(false);
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    {
        false
    }
}

fn selected_config_path(cli: Option<&Path>) -> Result<(PathBuf, bool)> {
    let from_env = env::var_os("ELEPHANT_CONFIG").map(PathBuf::from);
    select_config_path(
        &env::current_dir().context("current dir")?,
        cli.or(from_env.as_deref()),
    )
}

fn select_config_path(cwd: &Path, explicit: Option<&Path>) -> Result<(PathBuf, bool)> {
    if let Some(path) = explicit {
        anyhow::ensure!(
            !path.as_os_str().is_empty(),
            "explicit configuration path is empty"
        );
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        };
        return Ok((path, true));
    }
    Ok((
        find_project_root(cwd.to_path_buf())
            .join(CONFIG_DIR)
            .join(CONFIG_FILE),
        false,
    ))
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
    let content = toml::to_string_pretty(config).context("serialize config")?;
    elephant_storage::write_private_configuration(path, content.as_bytes())
        .context("publish runtime configuration")
}

#[cfg(test)]
#[path = "runtime_config_save_tests.rs"]
mod config_save_tests;

fn load_config_from_path(path: &Path) -> Result<ElephantConfig> {
    let content = fs::read_to_string(path).context("read runtime config")?;
    toml::from_str(&content).context("parse runtime config")
}

fn apply_env_overrides_to_loaded_config(config: &mut ElephantConfig) {
    if let Ok(space) = env::var("PIPELINE_AGENTIC_MCP_SPACE_ID") {
        config.extractor.readiness_space_id = Some(space);
    }
    if let Ok(snapshot_path) = env::var("ELEPHANT_DB_SNAPSHOT_LOAD") {
        if !snapshot_path.trim().is_empty() {
            config.storage.elephant_db.snapshot_path = PathBuf::from(snapshot_path);
        }
    }
}

fn env_runtime_config_present() -> bool {
    [
        "ELEPHANT_CONFIG",
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

    #[tokio::test]
    async fn effective_budget_projection_matches_managed_optional_defaults() -> Result<()> {
        let mut runtime = RuntimeManager::new_preconfigured(StorageClient::new_embedded().await?);
        let config = runtime.inner.get_mut().config.as_mut().expect("config");
        config.budget.daily_euros_graph = 7.5;
        config.budget.daily_items_graph = Some(42);
        config.budget.daily_items_retrieval = None;
        config.budget.per_item_tokens = None;
        config.budget.concurrent_workers.insert("extract".into(), 2);
        let projected = runtime.budget_configuration().await.expect("budget");
        assert!(
            (projected.effective.daily_euros_graph - projected.configured.daily_euros_graph).abs()
                < f64::EPSILON
        );
        assert_eq!(projected.effective.daily_items_graph, Some(42));
        assert_eq!(
            projected.effective.daily_items_retrieval,
            Budget::default().daily_items_retrieval
        );
        assert_eq!(
            projected.effective.per_item_tokens,
            Budget::default().per_item_tokens
        );
        assert!(projected.effective.concurrent_workers.is_empty());
        assert!(!projected.managed_workers_running);
        runtime.inner.get_mut().config = None;
        assert!(runtime.budget_configuration().await.is_none());
        drop(runtime);
        Ok(())
    }

    #[test]
    fn minimal_deployment_toml_retains_defaults_and_explicit_discovery_space() -> Result<()> {
        let config: ElephantConfig = toml::from_str(
            r#"
[runtime]
mode = "local_memory"
ingest_enabled = true
[storage]
backend = "elephant-db"
[storage.elephant_db]
snapshot_path = "/var/lib/recall/state.json"
[extractor]
provider = "meerkat-agentic"
required = true
mcp_url = "http://127.0.0.1:3000/mcp/agent"
readiness_space_id = "alpha"
model = "fixture"
timeout_secs = 30
"#,
        )?;
        assert_eq!(
            config.extractor.readiness_space_id.as_deref(),
            Some("alpha")
        );
        assert_eq!(config.storage.backend, StorageBackendKind::ElephantDb);
        assert_eq!(config.budget, Budget::default());
        Ok(())
    }

    #[test]
    fn explicit_config_path_does_not_follow_nearest_git_directory() -> Result<()> {
        let root = temp_test_dir("explicit-config-selection");
        std::fs::create_dir_all(root.join(".git"))?;
        let cwd = root.join("nested");
        std::fs::create_dir(&cwd)?;
        assert_eq!(
            select_config_path(&cwd, Some(Path::new("custom.toml")))?,
            (cwd.join("custom.toml"), true)
        );
        assert_eq!(
            select_config_path(&cwd, None)?,
            (root.join(CONFIG_DIR).join(CONFIG_FILE), false)
        );
        assert!(select_config_path(&cwd, Some(Path::new(""))).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[serial_test::serial(cwd_env)]
    #[tokio::test]
    async fn explicit_missing_config_fails_before_environment_storage_fallback() -> Result<()> {
        let _env = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let root = temp_test_dir("explicit-config-errors");
        fs::create_dir_all(&root)?;
        env::set_var("ELEPHANT_DB", "elephant-db");
        env::set_var("ELEPHANT_CONFIG", root.join("env.toml"));
        let explicit = root.join("explicit.toml");
        assert_eq!(
            selected_config_path(Some(&explicit))?,
            (explicit.clone(), true)
        );
        assert_eq!(selected_config_path(None)?, (root.join("env.toml"), true));
        assert!(
            RuntimeManager::new_with_config_path(Some(&explicit), Some(false))
                .await
                .is_err()
        );
        assert!(resolve_storage_config_for_cli_with_path(Some(&explicit)).is_err());
        fs::write(&explicit, "[extractor broken TOML")?;
        assert!(
            RuntimeManager::new_with_config_path(Some(&explicit), Some(false))
                .await
                .is_err()
        );
        assert!(resolve_storage_config_for_cli_with_path(Some(&explicit)).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

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
    use elephant_storage::work_item_repository::WorkItemRepository;
    use elephant_storage::{DocRepository, Repository};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc::Doc;
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::records::work::{WorkItem, WorkItemStatus};
    use elephant_types::RecordId;
    use std::ffi::OsString;
    use std::sync::{Mutex as StdMutex, OnceLock};

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn finished_worker_without_stop_flag_cannot_supply_live_recipe() -> Result<()> {
        for unwind in [false, true] {
            let storage = StorageClient::new_embedded().await?;
            let mut runtime = RuntimeManager::new_preconfigured(storage);
            let (release, wait) = std::sync::mpsc::channel();
            let handle = thread::spawn(move || -> anyhow::Result<()> {
                wait.recv()?;
                if unwind {
                    std::panic::resume_unwind(Box::new("PRIVATE WORKER PANIC"));
                }
                Err(anyhow!("PRIVATE EARLY WORKER BUILDER ERROR"))
            });
            let stopped = StdArc::new(AtomicBool::new(false));
            let (shutdown_tx, _) = watch::channel(false);
            let inner = runtime.inner.get_mut();
            inner.status.worker_ready = true;
            inner.status.semantic_ingest_ready = true;
            inner.pipeline = Some(ManagedPipeline {
                recipe: Some(ManagedWorkerRecipe::capture(&PipelineConfig::default())),
                shutdown_tx,
                handle,
                stopped: StdArc::clone(&stopped),
                last_error: StdArc::new(StdMutex::new(None)),
            });
            let live = runtime
                .get_config_redacted()
                .await
                .ok_or_else(|| anyhow!("missing config"))?;
            assert!(live.get("managed_worker_recipe").is_some());
            release.send(())?;
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let finished = runtime
                        .inner
                        .read()
                        .await
                        .pipeline
                        .as_ref()
                        .is_some_and(|pipeline| pipeline.handle.is_finished());
                    if finished {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert!(!stopped.load(Ordering::SeqCst));
            let config = runtime
                .get_config_redacted()
                .await
                .ok_or_else(|| anyhow!("lost config"))?;
            assert!(config.get("managed_worker_recipe").is_none());
            assert!(!config.to_string().contains("PRIVATE"));
            let status = runtime.get_readiness().await;
            assert!(!status.worker_ready && !status.semantic_ingest_ready);
            assert!(status
                .issues
                .iter()
                .any(|issue| issue == "worker_runtime_unavailable"));
            assert!(!serde_json::to_string(&status)?.contains("PRIVATE"));
            let pipeline = runtime
                .inner
                .write()
                .await
                .pipeline
                .take()
                .ok_or_else(|| anyhow!("worker handle lost"))?;
            let joined = pipeline.handle.join();
            if unwind {
                assert!(joined.is_err());
            } else {
                assert!(joined
                    .map_err(|_| anyhow!("unexpected fixture panic"))?
                    .is_err());
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn stopped_worker_readiness_keeps_private_error_details_out_of_public_status(
    ) -> Result<()> {
        let storage = StorageClient::new_embedded().await?;
        let mut runtime = RuntimeManager::new_preconfigured(storage);
        let private_error = "PRIVATE OTHER SPACE CONTENT AND PROVIDER DIAGNOSTIC";
        let last_error = StdArc::new(StdMutex::new(Some(private_error.to_string())));
        let (shutdown_tx, _) = watch::channel(false);
        let inner = runtime.inner.get_mut();
        inner.status.worker_ready = true;
        inner.status.semantic_ingest_ready = true;
        inner.pipeline = Some(ManagedPipeline {
            recipe: None,
            shutdown_tx,
            handle: thread::spawn(|| -> anyhow::Result<()> { Ok(()) }),
            stopped: StdArc::new(AtomicBool::new(true)),
            last_error: StdArc::clone(&last_error),
        });

        let status = runtime.get_readiness().await;
        assert!(!status.worker_ready);
        assert!(!status.semantic_ingest_ready);
        assert!(status
            .issues
            .iter()
            .any(|issue| issue == "worker_runtime_stopped"));
        assert!(!serde_json::to_string(&status)?.contains(private_error));
        assert_eq!(
            last_error
                .lock()
                .map_err(|_| anyhow!("private error lock poisoned"))?
                .as_deref(),
            Some(private_error)
        );
        let repeated = runtime.get_readiness().await;
        assert_eq!(repeated.issues, status.issues);
        let pipeline = runtime
            .inner
            .write()
            .await
            .pipeline
            .take()
            .ok_or_else(|| anyhow!("private worker handle lost"))?;
        pipeline
            .handle
            .join()
            .map_err(|_| anyhow!("fixture worker panicked"))??;
        Ok(())
    }

    const RUNTIME_CONFIG_ENV_VARS: &[&str] = &[
        "ELEPHANT_CONFIG",
        "PIPELINE_AGENTIC_MCP_SPACE_ID",
        "PIPELINE_MULTIMODAL_RENDER_MODEL",
        "PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS",
        "PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT",
        "PIPELINE_MULTIMODAL_ASSET_ROOT",
        "PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES",
        "PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES",
        "PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL",
        "PIPELINE_BUDGET_DAILY_EUROS_GRAPH",
        "PIPELINE_BUDGET_DAILY_ITEMS_RETRIEVAL",
        "PIPELINE_BUDGET_DAILY_ITEMS_GRAPH",
        "PIPELINE_BUDGET_PER_ITEM_TOKENS",
        "PIPELINE_BUDGET_PER_ITEM_WALL_TIME_SECS",
        "PIPELINE_BUDGET_DOWNGRADE_POLICY",
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
        fs::canonicalize(std::env::temp_dir())
            .expect("canonical test root")
            .join(unique)
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
                extractor_readiness_space_id: None,
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

        runtime.shutdown().await.expect("worker shutdown");
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_snapshot_load_env_overrides_persisted_embedded_config() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let root = temp_test_dir("snapshot-env-overrides-config");
        let prior_cwd = env::current_dir().expect("current dir");
        fs::create_dir_all(root.join(".git")).expect("create git dir");
        env::set_current_dir(&root).expect("switch cwd");

        let stale_snapshot = root.join("stale.snapshot");
        let fresh_snapshot = root.join("fresh.snapshot");
        let stale_storage = StorageClient::new_with_snapshot_destination(
            StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            Some(stale_snapshot.clone()),
        )
        .await
        .expect("stale live store and independent journal");
        let fresh_storage = StorageClient::new_with_snapshot_destination(
            StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            Some(fresh_snapshot.clone()),
        )
        .await
        .expect("fresh live store and independent journal");
        elephant_migrations::apply_schema(&stale_storage)
            .await
            .expect("stale canonical schema");
        elephant_migrations::apply_schema(&fresh_storage)
            .await
            .expect("fresh canonical schema");
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
        DocRepository::new(fresh_storage.clone())
            .create(fresh_doc)
            .await
            .expect("create fresh doc");
        stale_storage
            .export_snapshot(&stale_snapshot)
            .await
            .expect("export stale snapshot");
        fresh_storage
            .export_snapshot(&fresh_snapshot)
            .await
            .expect("export fresh snapshot");
        // Reopening must retain the original independent journals and release
        // the original store writer locks, just like a real process restart.
        drop(stale_storage);
        drop(fresh_storage);

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

        runtime.shutdown().await.expect("worker shutdown");
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
                    setup_diagnostics: Vec::new(),
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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
                extractor_readiness_space_id: None,
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

        runtime.shutdown().await.expect("worker shutdown");
        env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
                    setup_diagnostics: Vec::new(),
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
                    setup_diagnostics: Vec::new(),
                    issues: vec!["semantic_extractor_unavailable".to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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
                    setup_diagnostics: Vec::new(),
                    issues: vec![ISSUE_SEMANTIC_EXTRACTOR_FEATURE_UNAVAILABLE.to_string()],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
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

    #[serial_test::serial(cwd_env)]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_managed_pipeline_thread_starts_and_stops_on_embedded_storage() {
        let prior: Vec<_> = RUNTIME_CONFIG_ENV_VARS
            .iter()
            .map(|name| (*name, env::var_os(name)))
            .collect();
        let environment = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let storage = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .expect("embedded storage");

        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;
        config.extractor.readiness_space_id = Some("alpha".into());

        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_pipeline_handle(Some(pipeline))
            .await
            .expect("worker shutdown");
        assert_eq!(
            env::var("PIPELINE_AGENTIC_MCP_SPACE_ID").as_deref(),
            Ok("alpha")
        );
        drop(environment);
        for (name, value) in prior {
            assert_eq!(env::var_os(name), value, "managed worker leaked {name}");
        }
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

    const RUNTIME_TEST_SPACE: &str = "runtime-fixture";

    #[expect(
        clippy::default_trait_access,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn prepare_worker_fixture_space(storage: &StorageClient) {
        elephant_migrations::apply_schema(storage)
            .await
            .expect("explicit fixture schema migration");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    RUNTIME_TEST_SPACE.into(),
                    "tester".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                // The real dispatcher resolves an explicit processing policy
                // even for metadata/unknown work. No model binding is approved.
                processing: Some(elephant_types::provider_binding::SpaceProcessingConfig {
                    generation: 1,
                    index_generation: 0,
                    policy_id: "runtime-fixture-no-model-approval".into(),
                    connections: Default::default(),
                    profiles: Default::default(),
                    bindings: Default::default(),
                    active_embedding_profile_id: None,
                    pending_reindex: None,
                }),
                purpose: None,
                skill: None,
            })
            .await
            .expect("explicit named worker space");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .resolve_space(RUNTIME_TEST_SPACE)
            .await
            .expect("dispatcher can resolve the explicit processing policy");
    }

    fn unknown_worker_fixture(stale_lease: bool) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                RUNTIME_TEST_SPACE.into(),
                "tester".into(),
            ),
            kind: "unknown_work_kind".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("runtime-claim-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: stale_lease
                .then(|| chrono::Utc::now() + chrono::Duration::minutes(10)),
            leased_by: stale_lease.then(|| "old-worker".into()),
            run_after: None,
            last_error: None,
        }
    }

    async fn accepted_metadata_fixture(storage: &StorageClient, name: &str) -> RecordId {
        use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                RUNTIME_TEST_SPACE.into(),
                "tester".into(),
            ),
            SourceType::Manual,
            name.into(),
        );
        source.external_id = Some(name.into());
        source.set_requested_ingest_mode(elephant_types::records::ingest::IngestMode::Metadata);
        let accepted = PreparedSourceIntake::prepare(storage, source, IntakeControl::default())
            .await
            .expect("prepare real accepted metadata")
            .accept(storage)
            .await
            .expect("durable source acceptance");
        let id = elephant_pipeline::source_queue::enqueue_accepted_source(
            storage,
            &accepted.source,
            &accepted.publication,
            "tester",
        )
        .await
        .expect("enqueue original accepted source fence");
        let work = WorkItemRepository::new(storage.clone())
            .get(RUNTIME_TEST_SPACE, &id)
            .await
            .expect("read work")
            .expect("work exists");
        assert_eq!(
            elephant_pipeline::source_queue::publication_from_work(&work).expect("original fence"),
            Some(accepted.publication),
        );
        assert_eq!(work.status, WorkItemStatus::Runnable);
        assert_eq!(work.attempts, 0);
        id
    }

    // Return observations on timeout so callers always join workers before
    // asserting. A failed fixture must not leak background workers to later tests.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn observe_worker_claims(
        storage: &StorageClient,
        ids: &[RecordId],
        timeout: Duration,
    ) -> Result<Vec<Option<WorkItem>>> {
        let repo = WorkItemRepository::new(storage.clone());
        let mut observed = vec![None; ids.len()];
        let started = tokio::time::Instant::now();
        loop {
            for (index, id) in ids.iter().enumerate() {
                let item = repo
                    .get(RUNTIME_TEST_SPACE, id)
                    .await?
                    .ok_or_else(|| anyhow!("fixture work disappeared"))?;
                if item.attempts > 0 && item.status != WorkItemStatus::Runnable {
                    observed[index] = Some(item);
                }
            }
            if observed.iter().all(Option::is_some) || started.elapsed() >= timeout {
                return Ok(observed);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    struct WorkerFixtureDirectory {
        root: PathBuf,
        previous: PathBuf,
    }

    impl WorkerFixtureDirectory {
        fn enter(name: &str) -> Self {
            let previous = env::current_dir().expect("current directory");
            let root = temp_test_dir(name);
            fs::create_dir_all(&root).expect("create fixture root");
            let root = fs::canonicalize(root).expect("canonical owned fixture root");
            env::set_current_dir(&root).expect("enter fixture directory");
            Self { root, previous }
        }
    }

    impl Drop for WorkerFixtureDirectory {
        fn drop(&mut self) {
            env::set_current_dir(&self.previous).expect("restore fixture directory");
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    async fn configured_worker_fixture(root: &Path) -> RuntimeManager {
        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;
        config.extractor.required = false;
        save_config(&root.join(CONFIG_DIR).join(CONFIG_FILE), &config).expect("save config");
        let runtime = RuntimeManager::new().await.expect("runtime manager");
        assert!(runtime.inner.read().await.pipeline.is_none());
        assert!(!runtime.get_readiness().await.worker_ready);
        let storage = runtime.current_storage().await.expect("runtime storage");
        prepare_worker_fixture_space(&storage).await;
        runtime
    }

    async fn bind_worker_fixture(runtime: &RuntimeManager) -> tokio::net::TcpListener {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture listener first");
        runtime
            .start_workers_after_bind()
            .await
            .expect("start after listener bind");
        assert!(runtime.inner.read().await.pipeline.is_some());
        listener
    }

    #[serial_test::serial(cwd_env)]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_managed_pipeline_thread_claims_runnable_work_items_on_embedded_storage() {
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        prepare_worker_fixture_space(&storage).await;
        let item = unknown_worker_fixture(false);
        let id = item.base.id.clone();
        WorkItemRepository::new(storage.clone())
            .create(item)
            .await
            .expect("create work");
        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;
        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");
        let observations = observe_worker_claims(&storage, &[id], Duration::from_secs(10)).await;
        stop_pipeline_handle(Some(pipeline))
            .await
            .expect("worker shutdown");
        let observations = observations.expect("observe claim");
        let fetched = observations[0]
            .as_ref()
            .expect("managed pipeline claimed runnable work");
        assert!(fetched.attempts > 0);
        assert_ne!(fetched.status, WorkItemStatus::Runnable);
    }

    #[serial_test::serial(cwd_env)]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_managed_pipeline_thread_claims_runnable_item_with_stale_lease_fields() {
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        prepare_worker_fixture_space(&storage).await;
        let item = unknown_worker_fixture(true);
        let id = item.base.id.clone();
        WorkItemRepository::new(storage.clone())
            .create(item)
            .await
            .expect("create stale work");
        let mut config = ElephantConfig::default();
        config.runtime.ingest_enabled = true;
        let pipeline = start_pipeline(&config, &storage).expect("start pipeline");
        let observations = observe_worker_claims(&storage, &[id], Duration::from_secs(10)).await;
        stop_pipeline_handle(Some(pipeline))
            .await
            .expect("worker shutdown");
        let observations = observations.expect("observe claim");
        let fetched = observations[0]
            .as_ref()
            .expect("managed pipeline claimed stale runnable work");
        assert!(fetched.attempts > 0);
        assert_ne!(fetched.status, WorkItemStatus::Runnable);
        assert_ne!(fetched.leased_by.as_deref(), Some("old-worker"));
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_runtime_manager_from_config_starts_managed_pipeline_on_embedded_storage() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let directory = WorkerFixtureDirectory::enter("runtime-manager-managed-pipeline");
        let runtime = configured_worker_fixture(&directory.root).await;
        let storage = runtime.current_storage().await.expect("runtime storage");
        let item = unknown_worker_fixture(false);
        let id = item.base.id.clone();
        WorkItemRepository::new(storage.clone())
            .create(item)
            .await
            .expect("create work");
        let _listener = bind_worker_fixture(&runtime).await;
        let observations = observe_worker_claims(&storage, &[id], Duration::from_secs(10)).await;
        runtime.shutdown().await.expect("worker shutdown");
        let observations = observations.expect("observe claim");
        let fetched = observations[0]
            .as_ref()
            .expect("runtime-managed pipeline claimed runnable work");
        assert!(fetched.attempts > 0);
        assert_ne!(fetched.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_runtime_manager_claims_real_triage_source_work_item_on_embedded_storage() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let directory = WorkerFixtureDirectory::enter("runtime-manager-triage-claim");
        let runtime = configured_worker_fixture(&directory.root).await;
        let storage = runtime.current_storage().await.expect("runtime storage");
        let id = accepted_metadata_fixture(&storage, "runtime-triage-claim").await;
        let _listener = bind_worker_fixture(&runtime).await;
        let observations = observe_worker_claims(&storage, &[id], Duration::from_secs(10)).await;
        runtime.shutdown().await.expect("worker shutdown");
        let observations = observations.expect("observe claim");
        let fetched = observations[0]
            .as_ref()
            .expect("runtime-managed pipeline claimed accepted triage");
        assert!(fetched.attempts > 0);
        assert_ne!(fetched.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    #[serial_test::serial(cwd_env)]
    #[allow(clippy::await_holding_lock)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn test_runtime_manager_claims_burst_of_triage_items_within_single_poll_window() {
        let _guard = env_lock().lock().expect("env lock");
        let _env_guard = EnvVarGuard::clear(RUNTIME_CONFIG_ENV_VARS);
        let directory = WorkerFixtureDirectory::enter("runtime-manager-burst-triage-claim");
        let runtime = configured_worker_fixture(&directory.root).await;
        let storage = runtime.current_storage().await.expect("runtime storage");
        let mut ids = Vec::new();
        for index in 0..4 {
            ids.push(accepted_metadata_fixture(&storage, &format!("runtime-burst-{index}")).await);
        }
        let _listener = bind_worker_fixture(&runtime).await;
        let observations = observe_worker_claims(&storage, &ids, Duration::from_secs(8)).await;
        runtime.shutdown().await.expect("worker shutdown");
        let observations = observations.expect("observe burst");
        assert!(
            observations.iter().all(Option::is_some),
            "all four original triage items must be claimed within one poll window: {observations:?}",
        );
        for item in observations.into_iter().flatten() {
            assert!(item.attempts > 0);
            assert_ne!(item.status, WorkItemStatus::Runnable);
        }
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
                    setup_diagnostics: Vec::new(),
                    issues: vec![],
                },
            }),
            reconfigure: tokio::sync::Mutex::new(()),
            readiness_refresh: tokio::sync::Mutex::new(()),
            conveyor_cache: tokio::sync::Mutex::new(None),
            listener_bound: AtomicBool::new(false),
            listener_address: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            workers_drained: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
        }
    }

    /// 2026-09-23: tool gating ran the full status, whose conveyor count scans
    /// every work item; one call took 14-30 s and every MCP list/call paid it.
    /// Readiness must not compute the conveyor, and the full status must reuse
    /// a recent computation instead of scanning again.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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

#[cfg(test)]
mod startup_order_tests {
    use super::*;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn prepare_does_not_start_workers_or_probe_a_self_dial_endpoint() -> Result<()> {
        let config = ElephantConfig::default();
        let prepared = prepare_runtime(config, Some(StorageClient::new_embedded().await?)).await?;
        assert!(prepared.pipeline.is_none());
        assert!(!prepared.status.worker_ready);
        assert!(!prepared.status.semantic_ingest_ready);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn draining_precedes_worker_wait_and_prevents_premature_snapshot() -> Result<()> {
        let runtime =
            RuntimeManager::new_with_storage_for_tests(StorageClient::new_embedded().await?);
        assert!(runtime.persist_shutdown_snapshot().await.is_err());
        runtime.begin_shutdown().await;
        assert!(runtime.is_draining());
        let status = runtime.get_readiness().await;
        assert!(!status.storage_ready);
        assert!(!status.semantic_ingest_ready);
        assert!(runtime.persist_shutdown_snapshot().await.is_err());
        runtime.shutdown().await?;
        assert!(runtime.workers_drained.load(Ordering::SeqCst));
        Ok(())
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn online_snapshot_bounds_do_not_pause_the_runtime_on_invalid_input() -> Result<()> {
        let runtime = std::sync::Arc::new(RuntimeManager::new_with_storage_for_tests(
            StorageClient::new_embedded().await?,
        ));
        let result = runtime
            .snapshot_operation(
                PathBuf::from("unused"),
                "test".into(),
                elephant_storage::snapshot::SnapshotRetention {
                    retention_seconds: 1,
                    quiesce_timeout_seconds: 0,
                },
                false,
            )
            .await;
        assert!(result.is_err());
        assert!(!runtime.snapshot_paused.load(Ordering::SeqCst));
        assert!(runtime.get_readiness().await.storage_ready);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn failed_worker_drain_cannot_be_cleared_by_a_later_export() -> Result<()> {
        let runtime = std::sync::Arc::new(RuntimeManager::new_with_storage_for_tests(
            StorageClient::new_embedded().await?,
        ));
        runtime
            .inner
            .write()
            .await
            .status
            .issues
            .push("worker_drain_failed".into());
        let result = runtime
            .snapshot_operation(
                PathBuf::from("unused"),
                "test".into(),
                elephant_storage::snapshot::SnapshotRetention::default(),
                false,
            )
            .await;
        assert!(result.is_err());
        assert!(!runtime.workers_drained.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn online_named_snapshot_returns_manifest_and_reopens_admission() -> Result<()> {
        let directory =
            std::env::temp_dir().join(format!("recall-online-snapshot-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let storage = StorageClient::new_with_snapshot_destination(
            StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            Some(directory.join("live.snapshot")),
        )
        .await?;
        let runtime = std::sync::Arc::new(RuntimeManager::new_preconfigured(storage));
        assert_eq!(
            runtime.snapshot_autosave_path().await,
            Some(directory.join("live.snapshot"))
        );
        let snapshot = directory.join("backup");
        let manifest = runtime
            .snapshot_operation(
                snapshot.clone(),
                "manual".into(),
                elephant_storage::snapshot::SnapshotRetention::default(),
                false,
            )
            .await?;
        let verified = elephant_storage::snapshot::verify(&snapshot).await?;
        assert_eq!(manifest.sha256, verified.sha256);
        assert_eq!(manifest.name, "manual");
        assert!(!runtime.snapshot_paused.load(Ordering::SeqCst));
        assert!(runtime.get_readiness().await.storage_ready);
        drop(runtime);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn explicit_ephemeral_client_has_no_invented_snapshot_destination() -> Result<()> {
        let runtime = RuntimeManager::new_preconfigured(StorageClient::new_embedded().await?);
        assert!(runtime.snapshot_autosave_path().await.is_none());
        assert!(!runtime.workers_required().await);
        Ok(())
    }
}

#[cfg(test)]
#[path = "runtime_configuration_failure_tests.rs"]
mod configuration_failure_tests;
