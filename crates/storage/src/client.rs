//! Storage client wrapper with `surreal-db` and `elephant-db` backends.

use std::path::PathBuf;
use std::sync::Arc;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

use crate::embedded::EmbeddedStore;
use crate::error::{StorageError, StorageResult};

/// Additional attempts after the first try when a request fails with a
/// retryable transaction conflict (4 tries total).
const MAX_CONFLICT_RETRIES: u32 = 3;

/// SurrealDB signals a retryable optimistic-transaction conflict with this
/// message ("Failed to commit transaction due to a read or write conflict.
/// This transaction can be retried"). Embedded engines deliver it as a typed
/// `Db` error, the ws protocol only as display text — so detection is on the
/// rendered message in both cases.
fn is_retryable_conflict(message: &str) -> bool {
    message.contains("can be retried") || message.contains("read or write conflict")
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
    inner: StorageClientInner,
    config: StorageConfig,
}

impl StorageClient {
    /// Creates a new storage client and connects to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if connecting, authenticating, or selecting the namespace/database fails.
    pub async fn new(config: StorageConfig) -> StorageResult<Self> {
        config.validate_timeouts()?;
        match config.backend {
            StorageBackendKind::SurrealDb => {
                let db = Surreal::<Any>::init();

                // The 2.6.5 core stores transaction_timeout but does not use
                // it. Its query context deadline is authoritative, including
                // cancellation/rollback of explicit BEGIN..COMMIT batches.
                let engine_config = surrealdb::opt::Config::new()
                    .query_timeout(config.query_timeout.min(config.transaction_timeout))
                    .transaction_timeout(config.transaction_timeout);
                db.connect((config.url.as_str(), engine_config))
                    .await
                    .map_err(|e| StorageError::ConnectionError(e.to_string()))?;

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
                    .map_err(|e| StorageError::ConnectionError(e.to_string()))?;
                }

                db.use_ns(&config.namespace)
                    .use_db(&config.database)
                    .await
                    .map_err(|e| StorageError::ConnectionError(e.to_string()))?;

                Ok(Self {
                    inner: StorageClientInner::Surreal { db: Arc::new(db) },
                    config,
                })
            }
            StorageBackendKind::ElephantDb => {
                let store = Arc::new(if let Some(path) = &config.snapshot_load {
                    EmbeddedStore::from_snapshot(path).await?
                } else {
                    EmbeddedStore::new()
                });

                Ok(Self {
                    inner: StorageClientInner::Embedded { store },
                    config,
                })
            }
        }
    }

    /// Creates a new in-memory storage client for testing.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory connection fails.
    pub async fn new_memory() -> StorageResult<Self> {
        Self::new(StorageConfig::default()).await
    }

    /// Creates a new embedded `elephant-db` client.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot loading fails.
    pub async fn new_embedded() -> StorageResult<Self> {
        Self::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
    }

    /// Returns a reference to the underlying database when using the
    /// `surreal-db` backend.
    #[must_use]
    pub fn db(&self) -> Option<&Surreal<Any>> {
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
    pub async fn export_snapshot(&self, path: impl Into<PathBuf>) -> StorageResult<()> {
        let path = path.into();
        let Some(store) = self.embedded_store() else {
            return Err(StorageError::QueryError(
                "Snapshot export is only supported for elephant-db".to_string(),
            ));
        };
        store.export_snapshot(path).await
    }

    /// Imports an embedded snapshot from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend is not embedded or the snapshot load fails.
    pub async fn import_snapshot(&self, path: impl Into<PathBuf>) -> StorageResult<()> {
        let path = path.into();
        let Some(store) = self.embedded_store() else {
            return Err(StorageError::QueryError(
                "Snapshot import is only supported for elephant-db".to_string(),
            ));
        };
        store.import_snapshot(path).await
    }

    /// Executes a checked `SurrealQL` query: statement-level errors inside the
    /// response are surfaced (via [`surrealdb::Response::check`]) instead of
    /// silently succeeding, and transient transaction conflicts are retried.
    ///
    /// This is the method repositories should use for every read and mutation.
    /// [`Self::query`] / [`Self::query_bind`] stay unchecked because the
    /// migration runner deliberately tolerates statement errors when re-applying
    /// schema files to an existing database.
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
                    let result = match db.query(sql).await {
                        Ok(response) => response.check().map_err(|e| e.to_string()),
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
                    let result = match db.query(sql).bind(bindings.clone()).await {
                        Ok(response) => response.check().map_err(|e| e.to_string()),
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
    /// the migration runner relies on that to stay re-runnable. Repositories
    /// should use [`Self::query_checked`] / [`Self::query_bind_checked`].
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn query(&self, sql: &str) -> StorageResult<surrealdb::Response> {
        match &self.inner {
            StorageClientInner::Surreal { db } => db
                .query(sql)
                .await
                .map_err(|e| StorageError::QueryError(e.to_string())),
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
            StorageClientInner::Surreal { db } => db
                .query(sql)
                .bind(bindings)
                .await
                .map_err(|e| StorageError::QueryError(e.to_string())),
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
        match &self.inner {
            StorageClientInner::Surreal { db } => {
                db.query("RETURN 1")
                    .await
                    .and_then(surrealdb::Response::check)
                    .map_err(|e| StorageError::ConnectionError(e.to_string()))?;
                Ok(())
            }
            StorageClientInner::Embedded { .. } => Ok(()),
        }
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

                let mut response = self.query("INFO FOR DB").await?;

                let info: Vec<InfoResult> = response.take(0).map_err(|e| {
                    StorageError::QueryError(format!("Failed to parse INFO FOR DB: {e}"))
                })?;

                if let Some(res) = info.into_iter().next() {
                    Ok(res.tables.keys().cloned().collect())
                } else {
                    Ok(Vec::new())
                }
            }
            StorageClientInner::Embedded { store } => store.table_names().await,
        }
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
    use std::path::PathBuf;

    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{WorkItem, WorkItemStatus};
    use elephant_types::RecordId;

    #[tokio::test]
    async fn test_memory_client_creation() -> Result<(), Box<dyn std::error::Error>> {
        let _client = StorageClient::new_memory().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_embedded_client_creation() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_embedded().await?;
        assert!(client.is_embedded());
        Ok(())
    }

    #[tokio::test]
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
    async fn test_embedded_snapshot_import_blocks_running_workers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_embedded().await?;
        let snapshot_source = StorageClient::new_embedded().await?;

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
        assert!(error
            .to_string()
            .contains("embedded work item(s) are still running"));
        let _ = std::fs::remove_file(PathBuf::from(&snapshot));
        Ok(())
    }
}
