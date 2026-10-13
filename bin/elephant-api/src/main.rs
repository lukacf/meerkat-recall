//! Elephant API Server
//!
//! The main HTTP + MCP API server for the Elephant knowledge system.

use std::env;
#[cfg(test)]
use std::time::{Duration, Instant};

use elephant_api::{
    config::ApiConfig, run_api_with_managed_runtime, runtime::RuntimeManager, AuthConfig,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use elephant_storage::StorageBackendKind;
#[cfg(test)]
use elephant_storage::{StorageClient, StorageConfig};
#[cfg(test)]
use tracing::warn;

#[cfg(test)]
const DB_CONNECT_TIMEOUT: Duration = Duration::from_secs(180);
#[cfg(test)]
const DB_CONNECT_INITIAL_DELAY: Duration = Duration::from_millis(250);
#[cfg(test)]
const DB_CONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[cfg(test)]
async fn connect_storage_with_retry(config: StorageConfig) -> anyhow::Result<StorageClient> {
    let deadline = Instant::now() + DB_CONNECT_TIMEOUT;

    let mut attempt: u32 = 0;
    let mut delay = DB_CONNECT_INITIAL_DELAY;
    loop {
        attempt = attempt.saturating_add(1);
        match StorageClient::new(config.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "Database connection error after {attempt} attempts over {DB_CONNECT_TIMEOUT:?}: {err}"
                    ));
                }

                warn!(
                    attempt,
                    "Database connection failed ({err}); retrying in {:?}", delay
                );
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay.saturating_mul(2), DB_CONNECT_MAX_DELAY);
            }
        }
    }
}

#[cfg(test)]
const fn should_retry_storage_connect(config: &StorageConfig) -> bool {
    !matches!(
        config.backend,
        elephant_storage::StorageBackendKind::ElephantDb
    )
}

#[cfg(test)]
async fn connect_storage(config: StorageConfig) -> anyhow::Result<StorageClient> {
    if should_retry_storage_connect(&config) {
        connect_storage_with_retry(config).await
    } else {
        StorageClient::new(config).await.map_err(Into::into)
    }
}

fn validate_backend_env() -> anyhow::Result<()> {
    if let Ok(value) = env::var("ELEPHANT_DB") {
        value
            .parse::<StorageBackendKind>()
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("Invalid ELEPHANT_DB value `{value}`: {err}"))?;
    }
    Ok(())
}

/// Application entry point.
#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Elephant API server");
    validate_backend_env()?;

    // Load configuration from environment
    let config = ApiConfig::from_env()?;
    info!(
        "Loaded server configuration: host={}, port={}, db_url={}, namespace={}, database={}",
        config.host, config.port, config.db_url, config.db_namespace, config.db_database
    );

    let auth_config = AuthConfig::from_env()?;
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let runtime = RuntimeManager::new().await?;
    let runtime_status = runtime.get_status().await;
    info!(
        configured = runtime_status.configured,
        bootstrap_mode = runtime_status.bootstrap_mode,
        backend = runtime_status.backend.as_deref().unwrap_or("unconfigured"),
        extractor_ready = runtime_status.extractor_ready,
        semantic_ingest_ready = runtime_status.semantic_ingest_ready,
        "Managed runtime initialized"
    );
    run_api_with_managed_runtime(config, auth_config, std::sync::Arc::new(runtime), None).await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use elephant_storage::StorageBackendKind;
    use std::time::Duration;

    #[test]
    fn test_should_not_retry_embedded_storage_connect() {
        let config = StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        };
        assert!(!should_retry_storage_connect(&config));
    }

    #[test]
    fn test_should_retry_surreal_storage_connect() {
        let config = StorageConfig {
            backend: StorageBackendKind::SurrealDb,
            ..StorageConfig::default()
        };
        assert!(should_retry_storage_connect(&config));
    }

    #[tokio::test]
    async fn test_embedded_connect_fails_fast_on_missing_snapshot() {
        let config = StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            snapshot_load: Some(std::env::temp_dir().join(format!(
                "missing-elephant-snapshot-{}.json",
                uuid::Uuid::new_v4()
            ))),
            ..StorageConfig::default()
        };

        let result = tokio::time::timeout(Duration::from_secs(1), connect_storage(config))
            .await
            .expect("embedded connect should fail fast rather than retry");
        assert!(result.is_err(), "missing snapshot should fail");
    }
}
