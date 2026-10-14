//! Storage client wrapper with `surreal-db` and `elephant-db` backends.

use std::path::PathBuf;
use std::sync::Arc;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

use crate::embedded::EmbeddedStore;
use crate::erasure_journal::ErasureJournal;
use crate::error::{StorageError, StorageResult};
use crate::recovery_barrier::{MaintenanceGuard, RecoveryBarrier, RecoveryPhase, RecoveryStatus};

#[path = "client_erasure_reservation.rs"]
mod erasure_reservation;
#[path = "client_native_restore.rs"]
mod native_restore;
#[path = "client_reference_custody.rs"]
mod reference_custody;
#[path = "client_source_acceptance.rs"]
mod source_acceptance;
#[path = "client_source_acceptance_retry.rs"]
mod source_acceptance_retry;
pub(crate) use erasure_reservation::current_erasure_reservation;

/// Additional attempts after the first try when a request fails with a
/// retryable transaction conflict (4 tries total).
const MAX_CONFLICT_RETRIES: u32 = 3;

/// SurrealDB signals a retryable optimistic-transaction conflict with this
/// message ("Failed to commit transaction due to a read or write conflict.
///
/// This transaction can be retried"). Embedded engines deliver it as a typed
/// `Db` error, the ws protocol only as display text — so detection is on the
/// rendered message in both cases.
fn is_retryable_conflict(message: &str) -> bool {
    message.contains("can be retried") || message.contains("read or write conflict")
}

// Rolled-back statements can precede the statement that actually failed.
// Keep the first real error in statement order; remote drivers may expose the
// rollback marker only as display text rather than a typed database variant.
fn check_query_response(mut response: surrealdb::Response) -> Result<surrealdb::Response, String> {
    let errors: std::collections::BTreeMap<_, _> = response.take_errors().into_iter().collect();
    let mut rollback_marker = None;
    for (_, error) in errors {
        let message = error.to_string();
        if matches!(
            error,
            surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted)
        ) || message == "The query was not executed due to a failed transaction"
        {
            rollback_marker.get_or_insert(message);
        } else {
            return Err(message);
        }
    }
    rollback_marker.map_or_else(|| Ok(response), Err)
}

/// Short exponential backoff between conflict retries (25ms, 50ms, 100ms) —
/// long enough to let the competing writer commit, short enough to keep
/// pipeline latency flat.
fn conflict_backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(25u64 << (attempt.saturating_sub(1).min(4)))
}

/// In-process SurrealDB engines have no auth layer; only remote protocol URLs
/// need root signin.
fn is_local_engine_url(url: &str) -> bool {
    matches!(
        url.split(':').next(),
        Some("mem" | "rocksdb" | "surrealkv" | "file")
    )
}

/// Available storage backends for Elephant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBackendKind {
    /// External or in-memory `SurrealDB`.
    SurrealDb,
    /// Embedded in-memory Elephant store.
    ElephantDb,
}

impl StorageBackendKind {
    /// Returns the stable env/config string for this backend.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SurrealDb => "surreal-db",
            Self::ElephantDb => "elephant-db",
        }
    }
}

impl std::str::FromStr for StorageBackendKind {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "surreal-db" => Ok(Self::SurrealDb),
            "elephant-db" => Ok(Self::ElephantDb),
            other => Err(StorageError::ConnectionError(format!(
                "Unknown storage backend `{other}`"
            ))),
        }
    }
}

/// Configuration for the storage client.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage backend selector.
    pub backend: StorageBackendKind,
    /// Database URL (e.g., `<ws://localhost:8000>` or `<mem://>`).
    pub url: String,
    /// `SurrealDB` username.
    pub username: String,
    /// `SurrealDB` password.
    pub password: String,
    /// Default namespace.
    pub namespace: String,
    /// Default database.
    pub database: String,
    /// Optional snapshot file to load for embedded mode.
    pub snapshot_load: Option<PathBuf>,
    /// Maximum duration of an embedded SurrealDB query batch.
    /// Remote servers must configure their own execution deadlines.
    pub query_timeout: std::time::Duration,
    /// Maximum duration of an embedded SurrealDB transaction.
    /// SurrealDB 2.6.5 only enforces query deadlines, so the shorter of this
    /// and `query_timeout` bounds every batch, including explicit transactions.
    pub transaction_timeout: std::time::Duration,
}

/// Default embedded database execution deadline, in seconds.
pub const DEFAULT_DATABASE_TIMEOUT_SECS: u64 = 30;
const MAX_DATABASE_TIMEOUT_SECS: u64 = 86_400;

impl StorageConfig {
    /// Applies explicit operator timeout overrides without silently accepting
    /// malformed, zero, or effectively unbounded values.
    pub fn with_env_timeouts(mut self) -> StorageResult<Self> {
        for (name, target) in [
            ("SURREALDB_QUERY_TIMEOUT_SECS", &mut self.query_timeout),
            (
                "SURREALDB_TRANSACTION_TIMEOUT_SECS",
                &mut self.transaction_timeout,
            ),
        ] {
            match std::env::var(name) {
                Ok(value) => *target = parse_database_timeout(name, &value)?,
                Err(std::env::VarError::NotPresent) => {}
                Err(error) => {
                    return Err(StorageError::ConnectionError(format!("{name}: {error}")))
                }
            }
        }
        self.validate_timeouts()?;
        Ok(self)
    }

    fn validate_timeouts(&self) -> StorageResult<()> {
        for (name, timeout) in [
            ("query_timeout", self.query_timeout),
            ("transaction_timeout", self.transaction_timeout),
        ] {
            if timeout.is_zero()
                || timeout > std::time::Duration::from_secs(MAX_DATABASE_TIMEOUT_SECS)
            {
                return Err(StorageError::ConnectionError(format!(
                    "{name} must be greater than zero and at most {MAX_DATABASE_TIMEOUT_SECS} seconds"
                )));
            }
        }
        Ok(())
    }
}

fn parse_database_timeout(name: &str, value: &str) -> StorageResult<std::time::Duration> {
    let seconds = value.parse::<u64>().map_err(|_| {
        StorageError::ConnectionError(format!(
            "{name} must be an integer between 1 and {MAX_DATABASE_TIMEOUT_SECS} seconds"
        ))
    })?;
    if !(1..=MAX_DATABASE_TIMEOUT_SECS).contains(&seconds) {
        return Err(StorageError::ConnectionError(format!(
            "{name} must be between 1 and {MAX_DATABASE_TIMEOUT_SECS} seconds"
        )));
    }
    Ok(std::time::Duration::from_secs(seconds))
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::SurrealDb,
            url: "mem://".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            namespace: "test".to_string(),
            database: "test".to_string(),
            snapshot_load: None,
            query_timeout: std::time::Duration::from_secs(DEFAULT_DATABASE_TIMEOUT_SECS),
            transaction_timeout: std::time::Duration::from_secs(DEFAULT_DATABASE_TIMEOUT_SECS),
        }
    }
}

#[derive(Clone, Debug)]
enum StorageClientInner {
    Surreal { db: Arc<Surreal<Any>> },
    Embedded { store: Arc<EmbeddedStore> },
}

/// Storage client wrapper.
#[derive(Clone, Debug)]
pub struct StorageClient {
    local_models: Arc<elephant_embeddings::local_model_cache::VerifiedLocalModelCache>,
    inner: StorageClientInner,
    config: StorageConfig,
    _store_writer: Option<Arc<crate::exclusive_store_lock::StoreWriterLock>>,
    recovery: Arc<RecoveryBarrier>,
    journal: ErasureJournal,
    snapshot_destination: Option<PathBuf>,
    maintenance: bool,
    maintenance_lease: Option<Arc<tokio::sync::OwnedRwLockWriteGuard<()>>>,
    // A transport failure cannot prove that remote native maintenance stopped.
    // Preserve the original exclusive owner and require restart reconciliation.
    physical_failure_lease:
        Arc<std::sync::Mutex<Option<Arc<tokio::sync::OwnedRwLockWriteGuard<()>>>>>,
    native_maintenance_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    native_background_gate: Option<Arc<tokio::sync::RwLock<()>>>,
}

fn physical_remaining_ms(deadline: std::time::Instant) -> StorageResult<u64> {
    let millis = deadline
        .checked_duration_since(std::time::Instant::now())
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .filter(|millis| *millis > 0)
        .ok_or_else(|| {
            StorageError::Conflict("original native maintenance deadline exhausted".into())
        })?;
    if millis > 300_000 {
        return Err(StorageError::Conflict(
            "native maintenance deadline exceeds the original protocol bound".into(),
        ));
    }
    Ok(millis)
}

// This guard lives inside the unexposed SDK task. A cancelled requester cannot
// suppress failure custody when the SDK future panics or exits early.
struct NativePhysicalOwner {
    client: StorageClient,
    complete: bool,
}

impl Drop for NativePhysicalOwner {
    fn drop(&mut self) {
        if !self.complete {
            self.client.retain_failed_physical_maintenance();
        }
    }
}

// The 2.6.5 core stores transaction_timeout but does not enforce it separately.
// Its query deadline bounds explicit BEGIN..COMMIT batches as well.
fn native_engine_config(config: &StorageConfig) -> surrealdb::opt::Config {
    surrealdb::opt::Config::new()
        .query_timeout(config.query_timeout.min(config.transaction_timeout))
        .transaction_timeout(config.transaction_timeout)
}

#[cfg(test)]
#[path = "client_native_test_barrier.rs"]
mod native_test_barrier;

#[cfg(test)]
#[path = "client_native_background_tests.rs"]
mod native_background_tests;

impl StorageClient {
    /// Verified model resources shared by repositories and fresh dispatch handles.
    /// This owner contains no space approval, request input or credentials.
    #[must_use]
    pub fn local_models(
        &self,
    ) -> Arc<elephant_embeddings::local_model_cache::VerifiedLocalModelCache> {
        self.local_models.clone()
    }

    /// Creates a new storage client and connects to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if connecting, authenticating, or selecting the namespace/database fails.
    pub async fn new(config: StorageConfig) -> StorageResult<Self> {
        Box::pin(Self::new_with_snapshot_destination(config, None)).await
    }

    /// Open storage while reserving an embedded snapshot destination, even on
    /// first startup before that destination exists.
    ///
    /// Managed mutations sync to
    /// its sibling transaction log; shutdown export is not required for durability.
    pub async fn new_with_snapshot_destination(
        config: StorageConfig,
        snapshot_destination: Option<PathBuf>,
    ) -> StorageResult<Self> {
        Box::pin(Self::new_with_durability(
            config,
            snapshot_destination,
            None,
        ))
        .await
    }

    /// Open the same storage path while retaining safe initialization phase and
    /// observed OS error metadata. This does not change ownership or recovery.
    pub async fn new_with_snapshot_destination_diagnosed(
        config: StorageConfig,
        snapshot_destination: Option<PathBuf>,
    ) -> Result<Self, crate::initialization_diagnostic::StorageInitializationError> {
        let engine_config = native_engine_config(&config);
        Box::pin(Self::open_with_engine_configuration(
            config,
            snapshot_destination,
            None,
            engine_config,
        ))
        .await
    }

    /// Open a restore target with an explicit independently retained journal directory.
    pub async fn new_with_durability(
        config: StorageConfig,
        snapshot_destination: Option<PathBuf>,
        journal_root: Option<PathBuf>,
    ) -> StorageResult<Self> {
        let engine_config = native_engine_config(&config);
        Box::pin(Self::open_with_engine_configuration(
            config,
            snapshot_destination,
            journal_root,
            engine_config,
        ))
        .await
        .map_err(crate::initialization_diagnostic::StorageInitializationError::into_storage_error)
    }

    #[allow(clippy::too_many_lines)] // Preserve the single durability/recovery initialization path.
    async fn open_with_engine_configuration(
        config: StorageConfig,
        snapshot_destination: Option<PathBuf>,
        journal_root: Option<PathBuf>,
        engine_config: surrealdb::opt::Config,
    ) -> Result<Self, crate::initialization_diagnostic::StorageInitializationError> {
        use crate::initialization_diagnostic::{
            InitializationPhase as Phase, StorageInitializationError as InitError,
        };
        config
            .validate_timeouts()
            .map_err(|error| InitError::new(Phase::Configuration, error))?;
        if snapshot_destination.is_some() && config.backend != StorageBackendKind::ElephantDb {
            return Err(InitError::new(
                Phase::Configuration,
                StorageError::ConnectionError(
                    "snapshot destination is only valid for elephant-db".into(),
                ),
            ));
        }
        let snapshot_destination = snapshot_destination.or_else(|| config.snapshot_load.clone());
        if let (Some(load), Some(save)) = (&config.snapshot_load, &snapshot_destination) {
            let load = std::fs::canonicalize(load).map_err(|error| {
                InitError::from_io(
                    Phase::SnapshotPath,
                    crate::durability_io::io_error(&error),
                    &error,
                    "storage_path_canonicalize",
                )
            })?;
            let save = std::fs::canonicalize(save).map_err(|error| {
                InitError::from_io(
                    Phase::SnapshotPath,
                    crate::durability_io::io_error(&error),
                    &error,
                    "storage_path_canonicalize",
                )
            })?;
            if load != save {
                return Err(InitError::new(
                    Phase::SnapshotPath,
                    StorageError::ConnectionError(
                        "snapshot load and save must resolve to the same path".into(),
                    ),
                ));
            }
        }
        let store_path = snapshot_destination
            .clone()
            .or_else(|| crate::exclusive_store_lock::configured_store_path(&config));
        let store_writer = store_path
            .as_deref()
            .map(|path| {
                crate::exclusive_store_lock::StoreWriterLock::acquire_diagnosed(
                    path,
                    Phase::WriterLock,
                )
            })
            .transpose()?
            .map(Arc::new);
        let journal_path = journal_root
            .or_else(|| std::env::var_os("ELEPHANT_DURABILITY_PATH").map(PathBuf::from))
            .map_or_else(|| durability_path(&config, store_path.as_deref()), Ok)
            .map_err(|error| InitError::new(Phase::JournalPath, error))?;
        let journal = ErasureJournal::open(&journal_path)
            .map_err(|error| InitError::new(Phase::JournalOpen, error))?;
        if let Some(path) = store_path.as_deref() {
            let absolute = if path.exists() {
                path.canonicalize().map_err(|error| {
                    InitError::from_io(
                        Phase::JournalPlacement,
                        crate::durability_io::io_error(&error),
                        &error,
                        "storage_path_canonicalize",
                    )
                })?
            } else {
                let parent = path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| std::path::Path::new("."));
                parent
                    .canonicalize()
                    .map_err(|error| {
                        InitError::from_io(
                            Phase::JournalPlacement,
                            crate::durability_io::io_error(&error),
                            &error,
                            "storage_path_canonicalize",
                        )
                    })?
                    .join(path.file_name().ok_or_else(|| {
                        InitError::new(
                            Phase::JournalPlacement,
                            StorageError::ConnectionError("store path needs a name".into()),
                        )
                    })?)
            };
            if journal.path().starts_with(&absolute) {
                return Err(InitError::new(
                    Phase::JournalPlacement,
                    StorageError::ConnectionError(
                        "erasure journal must be outside the replaceable store path".into(),
                    ),
                ));
            }
        }
        // Remote stores have no local data-file lock. Single-writer deployments share
        // the independent journal volume and lock it before opening the connection.
        let store_writer = if store_writer.is_none()
            && config.backend == StorageBackendKind::SurrealDb
            && !config.url.starts_with("mem:")
        {
            Some(Arc::new(
                crate::exclusive_store_lock::StoreWriterLock::acquire_diagnosed(
                    &journal.path().join("remote-store"),
                    Phase::RemoteWriterLock,
                )?,
            ))
        } else {
            store_writer
        };
        let native_background_gate = (config.backend == StorageBackendKind::SurrealDb
            && is_local_engine_url(&config.url))
        .then(|| Arc::new(tokio::sync::RwLock::new(())));
        let engine_config = match &native_background_gate {
            Some(gate) => engine_config.recall_background_gate(gate.clone()),
            None => engine_config,
        };
        let recovery = Arc::new(RecoveryBarrier::default());
        let client = match config.backend {
            StorageBackendKind::SurrealDb => {
                let db = Surreal::<Any>::init();

                db.connect((config.url.as_str(), engine_config))
                    .await
                    .map_err(|e| {
                        InitError::new(
                            Phase::NativeConnect,
                            StorageError::ConnectionError(e.to_string()),
                        )
                    })?;

                // Embedded engines (in-process datastores) have no auth layer;
                // signing in against them fails. Only remote protocol URLs
                // (ws://, wss://, http://, https://) need root signin.
                if !is_local_engine_url(&config.url) {
                    tracing::warn!(
                        "Remote SurrealDB ignores client execution timeout options; configure a finite server-side query timeout"
                    );
                    db.signin(surrealdb::opt::auth::Root {
                        username: &config.username,
                        password: &config.password,
                    })
                    .await
                    .map_err(|e| {
                        InitError::new(
                            Phase::NativeAuthentication,
                            StorageError::ConnectionError(e.to_string()),
                        )
                    })?;
                }

                db.use_ns(&config.namespace)
                    .use_db(&config.database)
                    .await
                    .map_err(|e| {
                        InitError::new(
                            Phase::NativeNamespace,
                            StorageError::ConnectionError(e.to_string()),
                        )
                    })?;

                Self {
                    local_models: Arc::default(),
                    inner: StorageClientInner::Surreal { db: Arc::new(db) },
                    config,
                    _store_writer: store_writer,
                    recovery,
                    journal,
                    snapshot_destination,
                    maintenance: false,
                    maintenance_lease: None,
                    physical_failure_lease: Arc::new(std::sync::Mutex::new(None)),
                    native_maintenance_deadline: Arc::new(std::sync::Mutex::new(None)),
                    native_background_gate,
                }
            }
            StorageBackendKind::ElephantDb => {
                recovery.set(RecoveryPhase::Replaying, None);
                let store = if let Some(path) = &snapshot_destination {
                    EmbeddedStore::from_durable_path(path, &journal, recovery.clone())
                        .map_err(|error| InitError::new(Phase::EmbeddedReplay, error))?
                } else {
                    EmbeddedStore::new()
                };
                let store = Arc::new(
                    store
                        .with_reference_guard(&journal)
                        .map_err(|error| InitError::new(Phase::ReferenceGuard, error))?
                        .with_store_writer(store_writer.clone())
                        .with_recovery_barrier(recovery.clone()),
                );

                Self {
                    local_models: Arc::default(),
                    inner: StorageClientInner::Embedded { store },
                    config,
                    _store_writer: store_writer,
                    recovery,
                    journal,
                    snapshot_destination,
                    maintenance: false,
                    maintenance_lease: None,
                    physical_failure_lease: Arc::new(std::sync::Mutex::new(None)),
                    native_maintenance_deadline: Arc::new(std::sync::Mutex::new(None)),
                    native_background_gate,
                }
            }
        };
        client
            .maintenance_client()
            .verify_reference_capability()
            .await
            .map_err(|error| InitError::new(Phase::ReferenceCapability, error))?;
        Box::pin(client.initialize_recovery())
            .await
            .map_err(|error| InitError::new(Phase::Recovery, error))?;
        client.start_retention_cleanup();
        Ok(client)
    }

    /// Effective managed embedded destination, including the explicit load-path fallback.
    /// `None` identifies a library/test client without managed embedded persistence.
    #[must_use]
    pub fn effective_snapshot_destination(&self) -> Option<&std::path::Path> {
        self.snapshot_destination.as_deref()
    }

    /// Creates a new in-memory storage client for testing.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory connection fails.
    pub async fn new_memory() -> StorageResult<Self> {
        Box::pin(Self::new(StorageConfig::default())).await
    }

    /// Creates a new embedded `elephant-db` client.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot loading fails.
    pub async fn new_embedded() -> StorageResult<Self> {
        Box::pin(Self::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        }))
        .await
    }

    /// Returns a reference to the underlying database when using the
    /// `surreal-db` backend.
    #[must_use]
    pub(crate) fn db(&self) -> Option<&Surreal<Any>> {
        match &self.inner {
            StorageClientInner::Surreal { db } => Some(db),
            StorageClientInner::Embedded { .. } => None,
        }
    }

    /// Returns the configuration.
    #[must_use]
    pub const fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Returns the configured backend.
    #[must_use]
    pub const fn backend(&self) -> StorageBackendKind {
        self.config.backend
    }

    /// Returns `true` when the client is using the embedded backend.
    #[must_use]
    pub const fn is_embedded(&self) -> bool {
        matches!(self.config.backend, StorageBackendKind::ElephantDb)
    }

    /// Returns the embedded store when using `elephant-db`.
    #[must_use]
    pub fn embedded_store(&self) -> Option<Arc<EmbeddedStore>> {
        match &self.inner {
            StorageClientInner::Embedded { store } => Some(store.clone()),
            StorageClientInner::Surreal { .. } => None,
        }
    }

    /// Exports an embedded snapshot to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend is not embedded or the snapshot write fails.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
    )]
    pub async fn export_snapshot(&self, path: impl Into<PathBuf>) -> StorageResult<()> {
        let path = path.into();
        let guard = self
            .begin_maintenance(RecoveryPhase::Exporting, std::time::Duration::from_secs(30))
            .await?;
        let privileged = self.maintenance_client_with_guard(&guard);
        let store = privileged.embedded_store().ok_or_else(|| {
            StorageError::QueryError("raw snapshot export requires elephant-db".into())
        })?;
        if store.is_live_destination(&path)? {
            privileged.ensure_quiescent().await?;
            store.checkpoint().await?;
        } else {
            let bytes = store.export_snapshot_bytes().await?;
            crate::snapshot::write_live_snapshot(&path, &bytes, Some(&self.journal))?;
        }
        drop(privileged);
        guard.finish();
        Ok(())
    }

    /// Raw snapshot import participates in the same durable restore barrier as named bundles.
    /// New operator workflows use snapshot::restore_named for manifest/expiry verification.
    pub async fn import_snapshot(&self, path: impl Into<PathBuf>) -> StorageResult<()> {
        self.ensure_no_incomplete_transfer()?;
        let bytes = tokio::fs::read(path.into())
            .await
            .map_err(crate::durability_io::io_error)?;
        if !self.is_embedded() {
            return Err(StorageError::QueryError(
                "raw snapshot import requires elephant-db".into(),
            ));
        }
        crate::snapshot::validate_live_journal(&bytes, &self.journal)?;
        let bytes = crate::snapshot::decode_live_snapshot(&bytes)?;
        elephant_db::ElephantDb::from_snapshot_bytes(&bytes)?;
        let sha256 = crate::snapshot::digest(&bytes);
        let client = self.clone();
        // The same owner spans raw import and checked completion even if the
        // requesting future disappears after destructive replacement starts.
        tokio::spawn(async move { Box::pin(client.import_snapshot_owned(&bytes, sha256)).await })
            .await
            .map_err(crate::durability_io::io_error)?
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
    )]
    async fn import_snapshot_owned(&self, bytes: &[u8], sha256: String) -> StorageResult<()> {
        let _restore_owner = crate::snapshot::restore_ownership(&self.journal)?;
        self.ensure_no_incomplete_transfer()?;
        let guard = self
            .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(30))
            .await?;
        let privileged = self.maintenance_client_with_guard(&guard);
        if self.journal.restore_marker()?.is_none() {
            if let Err(error) = privileged.ensure_quiescent().await {
                guard.finish();
                return Err(error);
            }
        }
        let marker = crate::erasure_journal::RestoreMarker {
            snapshot_id: format!("raw-{sha256}"),
            sha256,
        };
        self.journal.begin_restore(&marker)?;
        privileged.replace_snapshot_bytes(bytes).await?;
        Box::pin(privileged.replay_erasures()).await?;
        privileged.rotate_restored_epochs().await?;
        privileged.persist_restored_state().await?;
        crate::snapshot::cleanup_expired(&self.journal)?;
        self.journal.finish_restore_checked(&marker)?;
        drop(privileged);
        guard.finish();
        Ok(())
    }

    /// Executes a checked `SurrealQL` query:
    ///
    /// statement-level errors inside the
    /// response are surfaced with their actual statement failure instead of
    /// silently succeeding, and transient transaction conflicts are retried.
    ///
    /// This is the method repositories should use for every read and mutation.
    /// [`Self::query`] / [`Self::query_bind`] expose raw statement responses for
    /// callers that explicitly inspect each result. Initialization uses checked queries.
    ///
    /// Retry safety: every storage-layer request is either a single statement
    /// (its own atomic transaction) or an explicit BEGIN..COMMIT batch — a
    /// retryable conflict means nothing was committed, so re-running the whole
    /// request is safe. `CREATE $id` retried after an actual commit surfaces as
    /// an `already exists` conflict, which callers already handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails, if any statement in the response
    /// failed, or if the conflict persists after all retries.
    pub async fn query_checked(&self, sql: &str) -> StorageResult<surrealdb::Response> {
        match &self.inner {
            StorageClientInner::Surreal { db } => {
                let mut attempt = 0u32;
                loop {
                    let result = match self.run_query(self.trusted_query(db, sql, None)?).await {
                        Ok(response) => check_query_response(response),
                        Err(StorageError::ErasureInProgress) => {
                            return Err(StorageError::ErasureInProgress)
                        }
                        Err(e) => Err(e.to_string()),
                    };
                    match result {
                        Ok(response) => return Ok(response),
                        Err(msg)
                            if attempt < MAX_CONFLICT_RETRIES && is_retryable_conflict(&msg) =>
                        {
                            attempt += 1;
                            tokio::time::sleep(conflict_backoff(attempt)).await;
                        }
                        Err(msg) => return Err(StorageError::classify_query_error(msg)),
                    }
                }
            }
            StorageClientInner::Embedded { .. } => Err(StorageError::QueryError(
                "Raw queries are not supported for elephant-db".to_string(),
            )),
        }
    }

    /// Checked + retried variant of [`Self::query_bind`]. See
    /// [`Self::query_checked`] for the checking and retry semantics. Bindings
    /// must be `Clone` so the request can be replayed on a transient conflict.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails, if any statement in the response
    /// failed, or if the conflict persists after all retries.
    pub async fn query_bind_checked<T: serde::Serialize + Clone + 'static>(
        &self,
        sql: &str,
        bindings: T,
    ) -> StorageResult<surrealdb::Response> {
        match &self.inner {
            StorageClientInner::Surreal { db } => {
                let mut attempt = 0u32;
                loop {
                    let result = match self
                        .run_query(self.trusted_query(
                            db,
                            sql,
                            Some(reference_custody::binding_value(bindings.clone())?),
                        )?)
                        .await
                    {
                        Ok(response) => check_query_response(response),
                        Err(StorageError::ErasureInProgress) => {
                            return Err(StorageError::ErasureInProgress)
                        }
                        Err(e) => Err(e.to_string()),
                    };
                    match result {
                        Ok(response) => return Ok(response),
                        Err(msg)
                            if attempt < MAX_CONFLICT_RETRIES && is_retryable_conflict(&msg) =>
                        {
                            attempt += 1;
                            tokio::time::sleep(conflict_backoff(attempt)).await;
                        }
                        Err(msg) => return Err(StorageError::classify_query_error(msg)),
                    }
                }
            }
            StorageClientInner::Embedded { .. } => {
                let _ = bindings;
                Err(StorageError::QueryError(
                    "Raw queries are not supported for elephant-db".to_string(),
                ))
            }
        }
    }

    /// Executes a raw `SurrealQL` query.
    ///
    /// NOTE: statement-level errors inside the response are NOT checked here —
    /// callers of this low-level method must inspect each statement. Repositories
    /// should use [`Self::query_checked`] / [`Self::query_bind_checked`].
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query(&self, sql: &str) -> StorageResult<surrealdb::Response> {
        match &self.inner {
            StorageClientInner::Surreal { db } => {
                self.run_query(self.trusted_query(db, sql, None)?).await
            }
            StorageClientInner::Embedded { .. } => Err(StorageError::QueryError(
                "Raw queries are not supported for elephant-db".to_string(),
            )),
        }
    }

    /// Executes a `SurrealQL` query with bound parameters.
    ///
    /// Use `$name` placeholders in your SQL and provide values via the bindings.
    /// This prevents SQL injection attacks.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query_bind<T: serde::Serialize + 'static>(
        &self,
        sql: &str,
        bindings: T,
    ) -> StorageResult<surrealdb::Response> {
        match &self.inner {
            StorageClientInner::Surreal { db } => {
                self.run_query(self.trusted_query(
                    db,
                    sql,
                    Some(reference_custody::binding_value(bindings)?),
                )?)
                .await
            }
            StorageClientInner::Embedded { .. } => {
                let _ = bindings;
                Err(StorageError::QueryError(
                    "Raw queries are not supported for elephant-db".to_string(),
                ))
            }
        }
    }

    /// Health check - verifies the connection is alive.
    ///
    /// # Errors
    ///
    /// Returns an error if the health check query fails.
    pub async fn health_check(&self) -> StorageResult<()> {
        if self.is_embedded() {
            let _guard = self.admit().await?;
        } else {
            self.query_checked("RETURN 1").await?;
        }
        Ok(())
    }

    /// Lists all tables in the current namespace and database.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata query fails or cannot be parsed.
    pub async fn list_tables(&self) -> StorageResult<Vec<String>> {
        match &self.inner {
            StorageClientInner::Surreal { .. } => {
                #[derive(serde::Deserialize)]
                struct InfoResult {
                    tables: std::collections::HashMap<String, String>,
                }

                let mut response = self.query_checked("INFO FOR DB").await?;

                let info: Option<InfoResult> = response.take(0).map_err(|e| {
                    StorageError::QueryError(format!("Failed to parse INFO FOR DB: {e}"))
                })?;

                if let Some(res) = info {
                    Ok(res.tables.keys().cloned().collect())
                } else {
                    Err(StorageError::DataIntegrityError(
                        "missing database table metadata".into(),
                    ))
                }
            }
            StorageClientInner::Embedded { store } => store.table_names().await,
        }
    }
}

impl StorageClient {
    /// Current storage readiness, including incomplete restore/erasure/transfer state.
    #[must_use]
    pub fn recovery_status(&self) -> RecoveryStatus {
        self.recovery.status()
    }

    /// Independently retained payload-free erasure journal for this store.
    pub const fn erasure_journal(&self) -> StorageResult<&ErasureJournal> {
        Ok(&self.journal)
    }

    async fn admit(&self) -> StorageResult<Option<crate::recovery_barrier::StorageAdmission>> {
        if self.maintenance {
            Ok(None)
        } else {
            self.recovery.admit_operation().await.map(Some)
        }
    }

    pub(crate) async fn begin_maintenance(
        &self,
        phase: RecoveryPhase,
        timeout: std::time::Duration,
    ) -> StorageResult<MaintenanceGuard> {
        self.recovery.enter(phase, timeout).await
    }

    pub(crate) fn maintenance_client(&self) -> Self {
        let mut client = self.clone();
        if !self.maintenance {
            client.native_maintenance_deadline = Arc::new(std::sync::Mutex::new(None));
        }
        client.maintenance = true;
        if let StorageClientInner::Embedded { store } = &self.inner {
            client.inner = StorageClientInner::Embedded {
                store: Arc::new(store.maintenance_view()),
            };
        }
        client
    }

    pub(crate) fn maintenance_client_with_guard(&self, guard: &MaintenanceGuard) -> Self {
        let mut client = self.maintenance_client();
        client.maintenance_lease = Some(guard.lease());
        client
    }

    /// After accepted Erasing + journal intent, drain all prior producers before physical purge.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
    )]
    pub(crate) async fn with_erasure_barrier<T, F, Fut>(&self, action: F) -> StorageResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
    {
        if self.maintenance {
            return action(self.clone()).await;
        }
        if self.recovery.in_activity() {
            self.block_on_incomplete_erasure();
            return Err(StorageError::Conflict(
                "erase must run outside an ordinary activity scope".into(),
            ));
        }
        let client = self.clone();
        tokio::spawn(async move {
            let guard = match client
                .begin_maintenance(RecoveryPhase::Replaying, std::time::Duration::from_secs(60))
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    client.block_on_incomplete_erasure();
                    return Err(error);
                }
            };
            let privileged = client.maintenance_client_with_guard(&guard);
            let result = action(privileged.clone()).await;
            match result {
                Ok(value) => {
                    // Include the completion receipt as well as purged data in the
                    // live local generation before returning the accepted result.
                    privileged.persist_restored_state().await?;
                    drop(privileged);
                    guard.finish();
                    Ok(value)
                }
                Err(error) => {
                    client.block_on_incomplete_erasure();
                    Err(error)
                }
            }
        })
        .await
        .map_err(|error| {
            StorageError::QueryError(format!("erasure operation task failed: {error}"))
        })?
    }

    /// Run the bounded backend-transfer command on an empty target. The closure must
    /// reconcile copied rows and inventories before returning success.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
    )]
    pub async fn with_transfer_barrier<T, F, Fut>(
        &self,
        source_digest: &str,
        action: F,
    ) -> StorageResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = StorageResult<T>> + Send + 'static,
    {
        validate_source_digest(source_digest)?;
        if self.recovery.in_activity() {
            return Err(StorageError::Conflict(
                "transfer must run outside ordinary activity".into(),
            ));
        }
        if self.journal.transfer_marker()?.is_some() || self.journal.restore_marker()?.is_some() {
            return Err(StorageError::Conflict(
                "incomplete target maintenance must be resolved explicitly".into(),
            ));
        }
        if !self.journal.intents()?.is_empty() {
            return Err(StorageError::Conflict(
                "backend transfer cannot reuse a target with erasure history; use a fresh target"
                    .into(),
            ));
        }
        let source_digest = source_digest.to_owned();
        let client = self.clone();
        tokio::spawn(async move {
            let guard = client
                .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(60))
                .await?;
            let privileged = client.maintenance_client_with_guard(&guard);
            let empty = if let Some(store) = privileged.embedded_store() {
                store.is_empty().await?
            } else {
                privileged.list_tables().await?.is_empty()
            };
            if !empty {
                guard.finish();
                return Err(StorageError::Conflict(
                    "backend transfer requires a completely fresh target".into(),
                ));
            }
            client.journal.begin_transfer(&source_digest)?;
            let result = action(privileged.clone()).await?;
            privileged.verify_journal_identity().await?;
            privileged.persist_restored_state().await?;
            client.journal.finish_transfer()?;
            drop(privileged);
            guard.finish();
            Ok(result)
        })
        .await
        .map_err(|error| {
            StorageError::QueryError(format!("transfer operation task failed: {error}"))
        })?
    }

    /// Destructive, explicit recovery for a failed transfer. Never touches source input.
    /// The caller must have authorized discarding the target and supply the original digest.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "The maintenance guard must span privileged effects through durable completion; finish consumes it."
    )]
    pub async fn discard_incomplete_transfer(
        &self,
        expected_source_digest: &str,
    ) -> StorageResult<()> {
        validate_source_digest(expected_source_digest)?;
        if self.recovery.in_activity() {
            return Err(StorageError::Conflict(
                "discard must run outside ordinary activity".into(),
            ));
        }
        let marker = self
            .journal
            .transfer_marker()?
            .ok_or_else(|| StorageError::Conflict("no incomplete transfer to discard".into()))?;
        if marker.source_sha256 != expected_source_digest
            || self.journal.restore_marker()?.is_some()
        {
            return Err(StorageError::Conflict(
                "incomplete transfer identity does not match discard request".into(),
            ));
        }
        let client = self.clone();
        tokio::spawn(async move {
            let guard = client
                .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(60))
                .await?;
            let privileged = client.maintenance_client_with_guard(&guard);
            if let Some(store) = privileged.embedded_store() {
                store
                    .import_snapshot_bytes(&elephant_db::ElephantDb::new().export_snapshot_bytes()?)
                    .await?;
                privileged.persist_restored_state().await?;
            } else {
                privileged.clear_surreal_database().await?;
            }
            client.journal.finish_transfer()?;
            drop(privileged);
            guard.finish();
            Ok(())
        })
        .await
        .map_err(|error| {
            StorageError::QueryError(format!("discard operation task failed: {error}"))
        })?
    }

    async fn run_query(
        &self,
        query: surrealdb::method::Query<'static, Any>,
    ) -> StorageResult<surrealdb::Response> {
        let admission = self.admit().await?;
        let owner = self.clone();
        // The SDK router can outlive a cancelled caller. Keep admission and writer
        // ownership in an unexposed task until the actual database request returns.
        tokio::spawn(async move {
            let _owner = owner;
            let _admission = admission;
            let response = query.await.map_err(|error| {
                if reference_custody::is_erasure_reservation_error(&error) {
                    StorageError::ErasureInProgress
                } else {
                    StorageError::from(error)
                }
            })?;
            // Leave all unrelated statement errors and successful positions
            // intact for legacy unchecked callers. A reservation refusal is a
            // typed request failure in every path, never apparent partial success.
            if response
                .errors()
                .any(|(_, error)| reference_custody::is_erasure_reservation_error(error))
            {
                return Err(StorageError::ErasureInProgress);
            }
            Ok(response)
        })
        .await
        .map_err(|error| {
            StorageError::QueryError(format!("database operation task failed: {error}"))
        })?
    }

    /// Cover one complete producer or worker operation, including preparation and publication.
    ///
    /// Inner storage calls reuse this task's admission, avoiding nested-reader deadlocks
    /// when maintenance is queued. Spawned child tasks need their own activity scope.
    pub async fn with_activity<F: std::future::Future>(
        &self,
        future: F,
    ) -> StorageResult<F::Output> {
        self.recovery.activity(future).await
    }

    /// Clones share one admission authority; replacement clients do not.
    pub(crate) fn same_incarnation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.recovery, &other.recovery)
    }

    /// Fail closed after an erasure intent was persisted but live cleanup failed.
    pub fn block_on_incomplete_erasure(&self) {
        self.recovery.set(
            RecoveryPhase::Failed,
            Some("erasure cleanup incomplete; restart to replay durable intents".into()),
        );
    }

    async fn initialize_recovery(&self) -> StorageResult<()> {
        crate::snapshot::cleanup_expired(&self.journal)?;
        if self.journal.transfer_marker()?.is_some() {
            self.recovery.set(RecoveryPhase::Failed, Some("incomplete backend transfer; explicitly discard the partial target before retry".into()));
            return Ok(());
        }
        if self.journal.restore_marker()?.is_some() {
            self.recovery.set(
                RecoveryPhase::Failed,
                Some("interrupted restore; resume a verified snapshot".into()),
            );
            // Retain an administrative handle so an explicit restore can resume. All
            // normal queries and background work fail the shared readiness barrier.
            return Ok(());
        }
        self.maintenance_client()
            .check_journal_identity(false)
            .await?;
        self.recovery.set(RecoveryPhase::Replaying, None);
        let privileged = self.maintenance_client();
        Box::pin(privileged.replay_erasures()).await?;
        if !self.journal.intents()?.is_empty() {
            privileged.persist_restored_state().await?;
        }
        drop(privileged);
        self.recovery.set(RecoveryPhase::Ready, None);
        Ok(())
    }

    /// Explicit initialization only. Serve uses verify_journal_identity without mutation.
    pub async fn initialize_journal_identity(&self) -> StorageResult<()> {
        if self.is_embedded() {
            return Ok(());
        }
        self.query_bind_checked(
            "BEGIN TRANSACTION; LET $stored = SELECT * FROM ONLY recall_durability:journal; IF $stored != NONE AND ($stored.journal_id != $journal_id OR $stored.reference_key_fingerprint != $key_fingerprint) { THROW 'journal identity mismatch'; }; UPSERT recall_durability:journal SET journal_id=$journal_id, reference_key_fingerprint=$key_fingerprint; COMMIT TRANSACTION;",
            serde_json::json!({"journal_id": self.journal.identity(), "key_fingerprint": self.journal.reference_key_fingerprint()}),
        ).await?;
        self.check_journal_identity(true).await
    }

    /// Read-only identity check used alongside schema verification at startup.
    pub async fn verify_journal_identity(&self) -> StorageResult<()> {
        self.check_journal_identity(true).await
    }

    async fn check_journal_identity(&self, required: bool) -> StorageResult<()> {
        #[derive(serde::Deserialize)]
        struct Identity {
            journal_id: String,
            reference_key_fingerprint: Option<String>,
        }

        if self.is_embedded() {
            return Ok(());
        }
        let mut response = self
            .query_checked(
                "SELECT journal_id, reference_key_fingerprint FROM recall_durability:journal",
            )
            .await?;
        let rows: Vec<Identity> = response.take(0)?;
        match rows.as_slice() {
            [] if !required => Ok(()),
            [identity]
                if identity.journal_id == self.journal.identity()
                    && identity.reference_key_fingerprint.as_deref()
                        == Some(self.journal.reference_key_fingerprint()) => Ok(()),
            _ => Err(StorageError::DataIntegrityError("store requires its original independently retained erasure journal; initialize a fresh store explicitly".into())),
        }
    }

    pub(crate) async fn replay_erasures(&self) -> StorageResult<()> {
        // Recovery closes pending reservations and replays every journal intent
        // under one barrier, including intents absent from the restored backup.
        Box::pin(
            crate::source_publication::SourcePublicationRepository::new(self.clone())
                .recover_pending_erasures(),
        )
        .await
    }

    pub(crate) async fn rotate_restored_epochs(&self) -> StorageResult<()> {
        crate::source_publication::SourcePublicationRepository::new(self.clone())
            .rotate_canonical_epochs()
            .await
    }

    pub(crate) async fn ensure_quiescent(&self) -> StorageResult<()> {
        #[derive(serde::Deserialize)]
        struct Count {
            count: u64,
        }

        if let Some(store) = self.embedded_store() {
            if store
                .scan::<elephant_types::records::WorkItem>()
                .await?
                .iter()
                .any(|work| work.status == elephant_types::records::WorkItemStatus::Running)
            {
                return Err(StorageError::Conflict(
                    "snapshot requires all Running work to drain".into(),
                ));
            }
        } else {
            let mut response = self
                .query_checked(
                    "SELECT count() AS count FROM work_item WHERE status = 'running' GROUP ALL",
                )
                .await?;
            let rows: Vec<Count> = response.take(0)?;
            if rows.iter().any(|row| row.count > 0) {
                return Err(StorageError::Conflict(
                    "snapshot requires all Running work to drain".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn pause_native_background(
        &self,
        deadline: std::time::Instant,
    ) -> StorageResult<Option<tokio::sync::OwnedRwLockWriteGuard<()>>> {
        if !self.maintenance || self.maintenance_lease.is_none() {
            return Err(StorageError::Conflict(
                "native background pause requires exclusive maintenance".into(),
            ));
        }
        let Some(gate) = &self.native_background_gate else {
            return Ok(None);
        };
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| crate::durability_io::io_error("original backup deadline exhausted"))?;
        tokio::time::timeout(remaining, gate.clone().write_owned())
            .await
            .map(Some)
            .map_err(|_| {
                crate::durability_io::io_error(
                    "native background drain exceeded original backup deadline",
                )
            })
    }

    pub(crate) async fn snapshot_bytes(
        &self,
        staging_file: &std::path::Path,
    ) -> StorageResult<Vec<u8>> {
        #[derive(serde::Deserialize)]
        struct DatabaseInfo {
            tables: std::collections::BTreeMap<String, String>,
        }

        if let Some(store) = self.embedded_store() {
            return store.export_snapshot_bytes().await;
        }
        let db = self
            .db()
            .ok_or_else(|| StorageError::QueryError("missing backend".into()))?;
        let mut response = self.query_checked("INFO FOR DB").await?;
        let info: Option<DatabaseInfo> = response.take(0)?;
        let info = info.ok_or_else(|| {
            StorageError::DataIntegrityError("database export inventory missing".into())
        })?;
        // Use the native export table selector, not textual rewriting of SQL.
        // Membership is private journal state; the guard/count stays in backup
        // so restore cannot become Ready before replaying the original journal.
        let tables = info
            .tables
            .into_keys()
            .filter(|table| table != "source_erasure_membership")
            .collect::<Vec<_>>();
        db.export(staging_file).with_config().tables(tables).await?;
        tokio::fs::read(staging_file)
            .await
            .map_err(crate::durability_io::io_error)
    }

    pub(crate) fn ensure_no_incomplete_transfer(&self) -> StorageResult<()> {
        if self.journal.transfer_marker()?.is_some() {
            return Err(StorageError::Conflict(
                "explicitly discard the incomplete transfer before restoring".into(),
            ));
        }
        Ok(())
    }

    async fn clear_surreal_database(&self) -> StorageResult<()> {
        let db = self
            .db()
            .ok_or_else(|| StorageError::QueryError("missing SurrealDB backend".into()))?;
        let name = self.config.database.replace('`', "\\`");
        db.query(format!("REMOVE DATABASE IF EXISTS `{name}`;"))
            .await?
            .check()?;
        Ok(())
    }

    pub(crate) async fn replace_snapshot_bytes(&self, bytes: &[u8]) -> StorageResult<()> {
        self.replace_snapshot_bytes_with_origin(
            bytes,
            None,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        )
        .await
    }

    pub(crate) async fn replace_snapshot_bytes_with_origin(
        &self,
        bytes: &[u8],
        origin: Option<&crate::source_history_restore::VerifiedBackupOrigins>,
        deadline: std::time::Instant,
    ) -> StorageResult<()> {
        if let Some(store) = self.embedded_store() {
            return store.replace_for_restore(bytes).await;
        }
        let deadline = self.physical_deadline(Some(deadline))?;
        physical_remaining_ms(deadline)?;
        let sql = std::str::from_utf8(bytes).map_err(crate::durability_io::io_error)?;
        let db = self
            .db()
            .ok_or_else(|| StorageError::QueryError("missing backend".into()))?;
        let restore = self.trusted_restore_query(db, sql)?;
        let name = self.config.database.replace('`', "\\`");
        // Complete database replacement, including tables/definitions absent from
        // the backup. A durable external marker blocks readiness across crashes.
        db.query(format!("REMOVE DATABASE IF EXISTS `{name}`;"))
            .await?
            .check()?;
        check_query_response(self.run_query(restore).await?)
            .map_err(StorageError::classify_query_error)?;
        physical_remaining_ms(deadline)?;
        if let Some(origin) = origin {
            crate::source_history_restore::bind_imported_snapshot(self, origin, deadline).await?;
        }
        physical_remaining_ms(deadline)?;
        self.verify_reference_capability().await?;
        self.refresh_native_reference_guards().await?;
        self.ensure_quiescent().await?;
        physical_remaining_ms(deadline)?;
        Ok(())
    }

    pub(crate) async fn persist_restored_state(&self) -> StorageResult<()> {
        if self.snapshot_destination.is_some() {
            let store = self.embedded_store().ok_or_else(|| {
                StorageError::QueryError("snapshot destination without embedded store".into())
            })?;
            store.checkpoint().await?;
        } else if self.db().is_some() {
            // The SDK administrative operation runs outside a query transaction.
            // The engine drains all native snapshots and retains its own task
            // and admission lease until physical reclamation has stopped.
            self.persist_native_physical_state().await?;
        }
        Ok(())
    }

    fn retain_failed_physical_maintenance(&self) {
        self.recovery.fail_until_restart(
            "native physical cleanup incomplete; restart and replay the original journal".into(),
        );
        if let Some(lease) = &self.maintenance_lease {
            let mut retained = self
                .physical_failure_lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retained.is_none() {
                *retained = Some(lease.clone());
            }
        }
    }

    fn physical_deadline(
        &self,
        supplied: Option<std::time::Instant>,
    ) -> StorageResult<std::time::Instant> {
        if !self.maintenance {
            return Ok(supplied.unwrap_or_else(|| {
                std::time::Instant::now() + std::time::Duration::from_secs(60)
            }));
        }
        let mut original = self.native_maintenance_deadline.lock().map_err(|_| {
            StorageError::DataIntegrityError(
                "native maintenance deadline ownership poisoned".into(),
            )
        })?;
        let deadline = match (*original, supplied) {
            (Some(prior), Some(supplied)) => prior.min(supplied),
            (Some(prior), None) => prior,
            (None, Some(supplied)) => supplied,
            (None, None) => std::time::Instant::now() + std::time::Duration::from_secs(60),
        };
        *original = Some(deadline);
        drop(original);
        Ok(deadline)
    }

    async fn checkpoint_native_history(&self, deadline: std::time::Instant) -> StorageResult<()> {
        let db = self.db().ok_or_else(|| {
            StorageError::DataIntegrityError("native checkpoint lost its backend".into())
        })?;
        let intents = self.journal.intents()?;
        if intents.is_empty() {
            db.recall_physical_checkpoint(physical_remaining_ms(deadline)?)
                .await?;
            physical_remaining_ms(deadline)?;
            return Ok(());
        }
        // An existing global anchor must match. Library stores may instead have
        // the mandatory per-space guards validated by the strict control projector.
        self.check_journal_identity(false).await?;
        physical_remaining_ms(deadline)?;
        let snapshot = db
            .recall_source_history_inventory(
                self.config.namespace.clone(),
                self.config.database.clone(),
                physical_remaining_ms(deadline)?,
            )
            .await?;
        physical_remaining_ms(deadline)?;
        let mut authorities = Vec::with_capacity(intents.len());
        for intent in &intents {
            physical_remaining_ms(deadline)?;
            let authority = surrealdb::kvs::RecallHistoryAuthority::new(
                self.config.namespace.clone(),
                self.config.database.clone(),
                self.journal.identity().into(),
                intent.space_id.clone(),
                *self.journal.reference_key_bytes(),
                crate::reference_fence::TYPED_UUID_TABLES
                    .iter()
                    .map(|table| (*table).into())
                    .collect(),
                self.journal.reference_membership(intent)?,
            )
            .map_err(|_| {
                StorageError::DataIntegrityError(
                    "original native history authority is invalid".into(),
                )
            })?;
            // This validates current controls and fsyncs exact opaque historical
            // commitments through the SAME original journal before physical copying.
            authorities.push(crate::source_history_authority::prepare_history_authority(
                &self.journal,
                intent,
                &snapshot,
                authority,
            )?);
        }
        physical_remaining_ms(deadline)?;
        // Every durable intent is represented; no newest-intent shortcut or
        // silent chunking can certify a partially erased native generation.
        db.recall_physical_checkpoint_with_history(
            self.config.namespace.clone(),
            self.config.database.clone(),
            physical_remaining_ms(deadline)?,
            authorities,
        )
        .await?;
        physical_remaining_ms(deadline)?;
        Ok(())
    }

    async fn persist_native_physical_state(&self) -> StorageResult<()> {
        let client = self.clone();
        let deadline = self.physical_deadline(None)?;
        // Retain the Recall maintenance lease, writer and journal through the
        // actual SDK future. Dropping a requester does not release ownership.
        let result = tokio::spawn(async move {
            let mut owner = NativePhysicalOwner {
                client,
                complete: false,
            };
            let result = owner.client.checkpoint_native_history(deadline).await;
            owner.complete = result.is_ok();
            result
        })
        .await;
        result.unwrap_or_else(|_| {
            self.retain_failed_physical_maintenance();
            Err(StorageError::QueryError(
                "owned native physical checkpoint task failed".into(),
            ))
        })
    }

    fn start_retention_cleanup(&self) {
        let recovery = Arc::downgrade(&self.recovery);
        let journal = self.journal.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let Some(recovery) = recovery.upgrade() else {
                    break;
                };
                if let Err(error) = crate::snapshot::cleanup_expired(&journal) {
                    tracing::error!(%error, "retained snapshot cleanup failed");
                    recovery.set(
                        RecoveryPhase::Failed,
                        Some("retained backup cleanup failed".into()),
                    );
                }
            }
        });
    }
}

fn validate_source_digest(digest: &str) -> StorageResult<()> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StorageError::DataIntegrityError(
            "source digest must be a SHA-256 hex digest".into(),
        ));
    }
    Ok(())
}

fn durability_path(
    config: &StorageConfig,
    store_path: Option<&std::path::Path>,
) -> StorageResult<PathBuf> {
    if let Some(path) = store_path {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let name = path
            .file_name()
            .ok_or_else(|| StorageError::ConnectionError("store path needs a name".into()))?;
        let mut sibling = name.to_os_string();
        sibling.push(".recall-durability");
        return Ok(parent.join(sibling));
    }
    if config.backend == StorageBackendKind::ElephantDb || config.url.starts_with("mem:") {
        return Ok(std::env::temp_dir().join(format!("recall-volatile-{}", uuid::Uuid::new_v4())));
    }
    let identity = crate::snapshot::digest(
        format!("{}\n{}\n{}", config.url, config.namespace, config.database).as_bytes(),
    );
    let root = PathBuf::from(".elephant/durability");
    Ok(root.join(identity))
}

#[cfg(test)]
#[path = "native_physical_admission_tests.rs"]
mod native_physical_admission_tests;

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
    use std::path::PathBuf;

    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{WorkItem, WorkItemStatus};
    use elephant_types::RecordId;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    #[expect(
        clippy::unchecked_time_subtraction,
        reason = "The fixture deliberately supplies an already expired deadline; inability to represent it must fail the test."
    )]
    async fn native_maintenance_clones_preserve_one_deadline_across_restore_and_checkpoints() {
        let client = StorageClient::new_memory().await.unwrap();
        let guard = client
            .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let maintenance = client.maintenance_client_with_guard(&guard);
        let original = std::time::Instant::now() + std::time::Duration::from_secs(240);
        assert_eq!(
            maintenance.physical_deadline(Some(original)).unwrap(),
            original
        );
        let nested = maintenance.maintenance_client();
        assert_eq!(nested.physical_deadline(None).unwrap(), original);
        assert_eq!(
            nested
                .physical_deadline(Some(original + std::time::Duration::from_secs(30)))
                .unwrap(),
            original
        );
        let expired = std::time::Instant::now() - std::time::Duration::from_secs(1);
        assert_eq!(nested.physical_deadline(Some(expired)).unwrap(), expired);
        assert_eq!(maintenance.physical_deadline(None).unwrap(), expired);
        assert!(physical_remaining_ms(maintenance.physical_deadline(None).unwrap()).is_err());
        guard.finish();
        // A later independent maintenance operation gets a new budget. The
        // exhausted budget is shared only within its original owner.
        let next = client.maintenance_client();
        assert!(physical_remaining_ms(next.physical_deadline(None).unwrap()).is_ok());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    #[expect(
        clippy::unchecked_time_subtraction,
        reason = "The fixture deliberately supplies an already expired deadline; inability to represent it must fail the test."
    )]
    async fn expired_native_restore_deadline_refuses_before_database_removal() {
        let client = StorageClient::new_memory().await.unwrap();
        client
            .query_checked("CREATE deadline_probe:retained SET value='independent'")
            .await
            .unwrap();
        let guard = client
            .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let maintenance = client.maintenance_client_with_guard(&guard);
        assert!(maintenance
            .replace_snapshot_bytes_with_origin(
                b"CREATE deadline_probe:replacement SET value='changed';",
                None,
                std::time::Instant::now() - std::time::Duration::from_secs(1),
            )
            .await
            .is_err());
        let mut response = maintenance
            .query_checked("SELECT VALUE value FROM deadline_probe")
            .await
            .unwrap();
        assert_eq!(
            response.take::<Vec<String>>(0).unwrap(),
            vec!["independent"]
        );
        guard.finish();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn raw_import_respects_named_restore_operation_ownership_before_mutation() {
        let client = StorageClient::new_embedded().await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raw.snapshot");
        client.export_snapshot(&path).await.unwrap();
        let original_owner = crate::snapshot::restore_ownership(&client.journal).unwrap();
        assert!(client.import_snapshot(&path).await.is_err());
        assert!(client.journal.restore_marker().unwrap().is_none());
        assert!(client.recovery_status().ready);
        drop(original_owner);
        client.import_snapshot(&path).await.unwrap();
        assert!(client.journal.restore_marker().unwrap().is_none());
        assert!(client.recovery_status().ready);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn cancelled_raw_import_requester_keeps_restore_owner_until_checked_completion() {
        let client = StorageClient::new_embedded().await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raw.snapshot");
        client.export_snapshot(&path).await.unwrap();
        let admission = client.recovery.admit_operation().await.unwrap();
        let requester_client = client.clone();
        let requester = tokio::spawn(async move { requester_client.import_snapshot(path).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while client.recovery_status().ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(crate::snapshot::restore_ownership(&client.journal).is_err());
        requester.abort();
        assert!(requester.await.unwrap_err().is_cancelled());
        assert!(crate::snapshot::restore_ownership(&client.journal).is_err());
        drop(admission);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if client.recovery_status().ready
                    && client.journal.restore_marker().unwrap().is_none()
                    && crate::snapshot::restore_ownership(&client.journal).is_ok()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(client.embedded_store().unwrap().is_empty().await.unwrap());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn native_physical_task_panic_retains_original_maintenance_after_waiter_loss() {
        let client = StorageClient::new_memory().await.unwrap();
        let guard = client
            .begin_maintenance(RecoveryPhase::Replaying, std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let original = guard.lease();
        let owner = NativePhysicalOwner {
            client: client.maintenance_client_with_guard(&guard),
            complete: false,
        };
        // The guard is inside the SDK task, independently of a dropped requester
        // JoinHandle. Unwinding it must retain the same original lease and poison.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _sdk_owner = owner;
            panic!("injected SDK task panic after requester loss");
        }));
        assert!(result.is_err());
        assert!(client
            .physical_failure_lease
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|retained| Arc::ptr_eq(retained, &original)));
        drop(original);
        drop(guard);
        assert!(!client.recovery_status().ready);
        assert!(client
            .recovery
            .exclusive(std::time::Duration::from_millis(5))
            .await
            .is_err());
        assert!(client
            .begin_maintenance(
                RecoveryPhase::Replaying,
                std::time::Duration::from_millis(5)
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_memory_client_creation() -> Result<(), Box<dyn std::error::Error>> {
        let _client = StorageClient::new_memory().await?;
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_embedded_client_creation() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_embedded().await?;
        assert!(client.is_embedded());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_health_check() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        client.health_check().await?;
        Ok(())
    }

    #[test]
    fn database_timeouts_reject_unbounded_or_invalid_configuration() {
        for value in [
            "",
            "0",
            "-1",
            "1.5",
            "none",
            "86401",
            "18446744073709551615",
        ] {
            assert!(parse_database_timeout("SURREALDB_QUERY_TIMEOUT_SECS", value).is_err());
        }
        assert_eq!(
            parse_database_timeout("SURREALDB_QUERY_TIMEOUT_SECS", "45").unwrap(),
            std::time::Duration::from_secs(45)
        );
        let mut config = StorageConfig::default();
        assert!(config.validate_timeouts().is_ok());
        config.query_timeout = std::time::Duration::ZERO;
        assert!(config.validate_timeouts().is_err());
        config.query_timeout = std::time::Duration::from_secs(30);
        config.transaction_timeout = std::time::Duration::MAX;
        assert!(config.validate_timeouts().is_err());
    }

    async fn timeout_test_client(
        query_timeout: std::time::Duration,
        transaction_timeout: std::time::Duration,
    ) -> StorageClient {
        StorageClient::new(StorageConfig {
            query_timeout,
            transaction_timeout,
            ..StorageConfig::default()
        })
        .await
        .expect("connect real memory engine")
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn query_deadline_stops_engine_before_later_statements() {
        let client = timeout_test_client(
            std::time::Duration::from_millis(100),
            std::time::Duration::from_secs(5),
        )
        .await;
        let started = std::time::Instant::now();
        let error = client
            .query_checked("SLEEP 2s; CREATE timeout_probe:late SET committed = true;")
            .await
            .expect_err("the engine must stop at its query deadline");
        assert!(
            error.to_string().contains("exceeded the timeout"),
            "{error}"
        );
        assert!(started.elapsed() >= std::time::Duration::from_millis(80));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let mut result = client
            .query_checked("SELECT VALUE committed FROM timeout_probe:late")
            .await
            .expect("engine remains responsive");
        let rows: Vec<bool> = result.take(0).unwrap();
        assert!(
            rows.is_empty(),
            "statements after the deadline must not run"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn checked_queries_preserve_transaction_cause_and_rollback() {
        let client = StorageClient::new(StorageConfig {
            database: format!("query_failure_{}", uuid::Uuid::new_v4().simple()),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        for bound in [false, true] {
            let error = if bound {
                client.query_bind_checked(
                    "BEGIN; CREATE query_failure_probe:bound SET value = 1; THROW $expected_error; COMMIT;",
                    crate::bindings! { "expected_error" => "publication:actual_bound_cause" },
                ).await.unwrap_err()
            } else {
                client.query_checked(
                    "BEGIN; CREATE query_failure_probe:plain SET value = 1; THROW 'publication:actual_plain_cause'; COMMIT;",
                ).await.unwrap_err()
            };
            let expected = if bound {
                "actual_bound_cause"
            } else {
                "actual_plain_cause"
            };
            assert!(
                error.to_string().contains(expected),
                "hidden statement cause: {error}"
            );
            let mut response = client
                .query_checked("SELECT VALUE id FROM query_failure_probe")
                .await
                .unwrap();
            let rows: Vec<surrealdb::sql::Thing> = response.take(0).unwrap();
            assert!(rows.is_empty(), "failed transaction left partial writes");
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn checked_query_success_preserves_statement_positions() {
        let client = StorageClient::new(StorageConfig::default()).await.unwrap();
        let mut response = client.query_checked("RETURN 11; RETURN 22;").await.unwrap();
        assert_eq!(response.take::<Option<i64>>(0).unwrap(), Some(11));
        assert_eq!(response.take::<Option<i64>>(1).unwrap(), Some(22));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn transaction_deadline_rolls_back_prior_writes() {
        let client = timeout_test_client(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(100),
        )
        .await;
        let started = std::time::Instant::now();
        let mut response = client
            .query(
                "BEGIN; CREATE timeout_probe:transaction SET committed = true; SLEEP 2s; COMMIT;",
            )
            .await
            .expect("receive transaction statement outcomes");
        // Earlier statements may be rewritten to QueryNotExecuted when a
        // later statement times out. Inspect all errors, not just the first
        // rollback marker selected by Response::check().
        let errors = response.take_errors();
        assert!(
            errors.values().any(|error| {
                error.to_string().contains("cancelled transaction")
                    || error.to_string().contains("exceeded the timeout")
            }),
            "transaction must contain the engine timeout cause: {errors:?}"
        );
        assert!(started.elapsed() >= std::time::Duration::from_millis(80));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        let mut result = client
            .query_checked("SELECT VALUE committed FROM timeout_probe:transaction")
            .await
            .unwrap();
        let rows: Vec<bool> = result.take(0).unwrap();
        assert!(rows.is_empty(), "timed-out transaction must roll back");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn abandoned_query_cannot_commit_after_its_engine_deadline() {
        let client = timeout_test_client(
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(5),
        )
        .await;
        // Aborting this caller does not cancel the SDK router's query task.
        // The engine must enforce its own deadline after the caller disappears.
        let query_client = client.clone();
        let caller = tokio::spawn(async move {
            query_client
                .query_checked(
                    "CREATE timeout_probe:started SET committed = true; \
                 BEGIN; CREATE timeout_probe:abandoned SET committed = true; SLEEP 2s; COMMIT;",
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let mut result = client
                    .query_checked("SELECT VALUE committed FROM timeout_probe:started")
                    .await
                    .unwrap();
                let rows: Vec<bool> = result.take(0).unwrap();
                if !rows.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the query must have reached the actual engine");
        assert!(
            !caller.is_finished(),
            "caller must leave during engine work"
        );
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        // Beyond the original SLEEP duration, an unbounded engine would have
        // committed the marker even though the caller had already timed out.
        tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
        let mut result = client
            .query_checked("SELECT VALUE committed FROM timeout_probe:abandoned")
            .await
            .unwrap();
        let rows: Vec<bool> = result.take(0).unwrap();
        assert!(
            rows.is_empty(),
            "abandoned transaction must never commit later"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn cancelled_sdk_caller_retains_snapshot_admission_until_engine_completion() {
        let client = StorageClient::new_memory().await.unwrap();
        let owned = client.clone();
        let caller = tokio::spawn(async move {
            owned
                .query_checked("CREATE cancelled_probe:started SET started=true; SLEEP 2s;")
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let mut result = client
                    .query_checked("SELECT VALUE started FROM cancelled_probe:started")
                    .await
                    .unwrap();
                let started: Vec<bool> = result.take(0).unwrap();
                if !started.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!caller.is_finished());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(client
            .begin_maintenance(
                RecoveryPhase::Exporting,
                std::time::Duration::from_millis(20)
            )
            .await
            .is_err());
        let guard = client
            .begin_maintenance(RecoveryPhase::Exporting, std::time::Duration::from_secs(3))
            .await
            .unwrap();
        guard.finish();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn failed_transfer_stays_blocked_until_explicit_matching_discard() {
        let directory = tempfile::tempdir().unwrap();
        let journal = directory.path().join("journal");
        let config = StorageConfig::default();
        let client =
            StorageClient::new_with_durability(config.clone(), None, Some(journal.clone()))
                .await
                .unwrap();
        let digest = "a".repeat(64);
        let result: StorageResult<()> = client
            .with_transfer_barrier(&digest, |exclusive| async move {
                exclusive
                    .query_checked("CREATE transfer_probe:partial SET value='partial'")
                    .await?;
                Err(StorageError::DataIntegrityError(
                    "injected copy failure".into(),
                ))
            })
            .await;
        assert!(result.is_err());
        assert!(!client.recovery_status().ready);
        assert!(client
            .query_checked("SELECT * FROM transfer_probe")
            .await
            .is_err());
        assert!(client
            .discard_incomplete_transfer(&"b".repeat(64))
            .await
            .is_err());
        assert!(client.journal.transfer_marker().unwrap().is_some());
        let reopened = StorageClient::new_with_durability(config, None, Some(journal))
            .await
            .unwrap();
        assert!(
            !reopened.recovery_status().ready,
            "restart must not serve a partial transfer"
        );
        client.discard_incomplete_transfer(&digest).await.unwrap();
        assert!(client.list_tables().await.unwrap().is_empty());
        assert!(client.journal.transfer_marker().unwrap().is_none());
        assert!(client.recovery_status().ready);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn transfer_completion_requires_reconciliation_callback_and_identity() {
        let client = StorageClient::new_memory().await.unwrap();
        let digest = "c".repeat(64);
        let result = client
            .with_transfer_barrier(&digest, |exclusive| async move {
                exclusive
                    .query_checked("CREATE transfer_probe:complete SET value='kept'")
                    .await?;
                exclusive.initialize_journal_identity().await?;
                let mut response = exclusive
                    .query_checked("SELECT VALUE value FROM transfer_probe:complete")
                    .await?;
                let rows: Vec<String> = response.take(0)?;
                if rows != vec!["kept"] {
                    return Err(StorageError::DataIntegrityError(
                        "inventory mismatch".into(),
                    ));
                }
                Ok(rows.len())
            })
            .await
            .unwrap();
        assert_eq!(result, 1);
        assert!(client.recovery_status().ready);
        assert!(client.journal.transfer_marker().unwrap().is_none());
        assert!(client
            .with_transfer_barrier(&digest, |_| async { Ok(()) })
            .await
            .is_err());
        assert!(
            client.recovery_status().ready,
            "nonempty preflight refusal must not damage readiness"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn interrupted_restore_retains_repair_handle_despite_partial_identity() {
        let client = StorageClient::new_memory().await.unwrap();
        client.initialize_journal_identity().await.unwrap();
        client
            .journal
            .begin_restore(&crate::erasure_journal::RestoreMarker {
                snapshot_id: uuid::Uuid::new_v4().to_string(),
                sha256: "d".repeat(64),
            })
            .unwrap();
        client
            .query_checked("UPDATE recall_durability:journal SET journal_id='partial-import-value'")
            .await
            .unwrap();
        client.initialize_recovery().await.unwrap();
        assert!(!client.recovery_status().ready);
        assert!(client.health_check().await.is_err());
        assert!(client.journal.restore_marker().unwrap().is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn surreal_journal_identity_cannot_be_silently_rebound() {
        let client = StorageClient::new_memory().await.unwrap();
        assert!(client.verify_journal_identity().await.is_err());
        client.initialize_journal_identity().await.unwrap();
        client.verify_journal_identity().await.unwrap();
        client
            .query_bind_checked(
                "UPDATE recall_durability:journal SET journal_id=$other",
                serde_json::json!({"other": uuid::Uuid::new_v4().to_string()}),
            )
            .await
            .unwrap();
        assert!(client.verify_journal_identity().await.is_err());
        assert!(client.initialize_journal_identity().await.is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_embedded_snapshot_import_blocks_running_workers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_embedded().await?;
        let snapshot_source = StorageClient::new_with_durability(
            StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            None,
            Some(client.erasure_journal()?.path().into()),
        )
        .await?;

        let snapshot = std::env::temp_dir().join(format!(
            "elephant-snapshot-{}.json",
            RecordId::new_v4(TableName::Doc)
        ));
        snapshot_source.export_snapshot(&snapshot).await?;

        let store = client.embedded_store().expect("embedded store");
        store
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "render_doc".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 0,
                idempotency_key: "snapshot-running".to_string(),
                attempts: 1,
                lease_expires_at: None,
                leased_by: Some("worker-1".to_string()),
                run_after: None,
                last_error: None,
            })
            .await?;

        let error: StorageError = client
            .import_snapshot(&snapshot)
            .await
            .expect_err("import should be blocked while workers are running");
        assert!(matches!(error, StorageError::Conflict(_)));
        assert!(error.to_string().contains("Running work to drain"));
        let _ = std::fs::remove_file(PathBuf::from(&snapshot));
        Ok(())
    }
}
