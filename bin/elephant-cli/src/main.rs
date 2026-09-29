#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Elephant CLI
//!
//! Command-line tool for database schema setup and administration.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use walkdir::WalkDir;

#[derive(Parser)]
#[command(name = "elephant-cli")]
#[command(about = "Elephant Knowledge System Administration CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply database schema
    Schema {
        /// Optional directory containing additional SQL files to apply
        #[arg(short, long)]
        extension_dir: Option<PathBuf>,
    },
    /// Apply database schema (alias for `schema`)
    Migrate {
        /// Optional directory containing additional SQL files to apply
        #[arg(short, long)]
        extension_dir: Option<PathBuf>,
    },
    /// Import or export a live embedded snapshot through the running API
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    /// Export the live embedded snapshot to a file on the Elephant host
    Export { path: PathBuf },
    /// Import a snapshot file into the live embedded store
    Import { path: PathBuf },
}

#[derive(Clone, Copy)]
enum SnapshotAction {
    Export,
    Import,
}

impl SnapshotAction {
    const fn endpoint(self) -> &'static str {
        match self {
            Self::Export => "/v1/system/snapshot/export",
            Self::Import => "/v1/system/snapshot/import",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Export => "export",
            Self::Import => "import",
        }
    }
}

#[derive(serde::Serialize)]
struct SnapshotRequest {
    path: String,
}

#[derive(serde::Deserialize)]
struct SnapshotResponse {
    success: bool,
    backend: String,
    path: String,
    message: String,
}

/// Application entry point.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Schema { extension_dir } | Commands::Migrate { extension_dir } => {
            info!("Applying database schema...");
            if let Err(e) = apply_schema(extension_dir).await {
                error!("Schema application failed: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Snapshot { command } => {
            let result = match command {
                SnapshotCommand::Export { path } => {
                    run_snapshot_command(SnapshotAction::Export, path).await
                }
                SnapshotCommand::Import { path } => {
                    run_snapshot_command(SnapshotAction::Import, path).await
                }
            };

            if let Err(e) = result {
                error!("Snapshot command failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn api_base_url(api_url: Option<&str>, host: Option<&str>, port: Option<&str>) -> String {
    if let Some(url) = api_url {
        let trimmed = url.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }

    let host = match host.unwrap_or("127.0.0.1").trim() {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "[::1]".to_string(),
        value => value.to_string(),
    };
    let port = port.unwrap_or("3000").trim();
    format!("http://{host}:{port}")
}

fn api_base_url_from_env() -> String {
    let api_url = std::env::var("ELEPHANT_API_URL").ok();
    let host = std::env::var("ELEPHANT_HOST").ok();
    let port = std::env::var("ELEPHANT_PORT").ok();
    api_base_url(api_url.as_deref(), host.as_deref(), port.as_deref())
}

async fn run_snapshot_command(action: SnapshotAction, path: PathBuf) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}{}", api_base_url_from_env(), action.endpoint());
    let mut request = client.post(url).json(&SnapshotRequest {
        path: path.to_string_lossy().into_owned(),
    });

    if let Ok(token) = std::env::var("ELEPHANT_API_BEARER_TOKEN") {
        if !token.trim().is_empty() {
            request = request.bearer_auth(token);
        }
    }

    let response = request
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to call snapshot {} endpoint: {e}", action.label()))?;
    let status = response.status();

    if status.is_success() {
        let payload: SnapshotResponse = response.json().await.map_err(|e| {
            anyhow::anyhow!(
                "Snapshot {} succeeded but response decoding failed: {e}",
                action.label()
            )
        })?;
        if !payload.success {
            anyhow::bail!(
                "Snapshot {} reported failure for {}",
                action.label(),
                payload.path
            );
        }
        info!("{} [{}]", payload.message, payload.backend);
        return Ok(());
    }

    let error_body: serde_json::Value = response
        .json()
        .await
        .unwrap_or_else(|_| serde_json::json!({ "message": format!("HTTP {}", status.as_u16()) }));
    let message = error_body
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Snapshot request failed");
    anyhow::bail!(
        "Snapshot {} failed ({}): {}",
        action.label(),
        status,
        message
    );
}

async fn apply_schema(extension_dir: Option<PathBuf>) -> anyhow::Result<()> {
    use elephant_migrations::apply_schema;
    use elephant_storage::{StorageBackendKind, StorageClient, StorageConfig};
    use std::env;
    use std::fs;

    let backend = configured_backend_from_env()?;

    if backend == StorageBackendKind::ElephantDb {
        anyhow::bail!(
            "`elephant-cli schema` and `migrate` are only supported with ELEPHANT_DB=surreal-db"
        );
    }

    let config = StorageConfig {
        backend,
        url: env::var("SURREALDB_URL").unwrap_or_else(|_| "ws://localhost:8000".to_string()),
        username: env::var("SURREALDB_USER").unwrap_or_else(|_| "root".to_string()),
        password: env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".to_string()),
        namespace: env::var("SURREALDB_NAMESPACE").unwrap_or_else(|_| "elephant".to_string()),
        database: env::var("SURREALDB_DATABASE").unwrap_or_else(|_| "main".to_string()),
        snapshot_load: env::var("ELEPHANT_DB_SNAPSHOT_LOAD")
            .ok()
            .map(PathBuf::from),
        ..StorageConfig::default()
    }
    .with_env_timeouts()?;
    let client = StorageClient::new(config).await?;

    // Apply core schema
    apply_schema(&client)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Apply extension SQL files if provided
    if let Some(dir) = extension_dir {
        info!("Applying extension SQL from: {:?}", dir);
        let mut entries: Vec<_> = WalkDir::new(&dir)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "surql"))
            .collect();

        // Sort by filename to ensure deterministic application order
        entries.sort_by_key(|e| e.file_name().to_os_string());

        for entry in entries {
            let path = entry.path();
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            let sql = fs::read_to_string(path)?;
            info!("Applying extension: {name}");
            client
                .query(&sql)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to apply {name}: {e}"))?;
        }
    }

    info!("Schema applied successfully");

    Ok(())
}

fn configured_backend_from_env() -> anyhow::Result<elephant_storage::StorageBackendKind> {
    use elephant_storage::StorageBackendKind;
    use std::env;

    match env::var("ELEPHANT_DB") {
        Ok(value) => value
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid ELEPHANT_DB `{value}`: {e}")),
        Err(env::VarError::NotPresent) => Ok(StorageBackendKind::SurrealDb),
        Err(err) => Err(anyhow::anyhow!("failed to read ELEPHANT_DB: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_url_prefers_explicit_api_url() {
        assert_eq!(
            api_base_url(
                Some("http://example.test:3000/"),
                Some("0.0.0.0"),
                Some("9999")
            ),
            "http://example.test:3000"
        );
    }

    #[test]
    fn api_base_url_rewrites_unspecified_bind_hosts() {
        assert_eq!(
            api_base_url(None, Some("0.0.0.0"), Some("3000")),
            "http://127.0.0.1:3000"
        );
        assert_eq!(
            api_base_url(None, Some("::"), Some("3000")),
            "http://[::1]:3000"
        );
    }

    #[test]
    fn configured_backend_from_env_rejects_invalid_values() {
        std::env::set_var("ELEPHANT_DB", "banana-db");
        let result = configured_backend_from_env();
        std::env::remove_var("ELEPHANT_DB");
        assert!(result
            .as_ref()
            .is_err_and(|err| err.to_string().contains("invalid ELEPHANT_DB `banana-db`")));
    }
}
