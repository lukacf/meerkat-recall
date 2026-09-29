#![allow(missing_docs, clippy::too_many_lines, clippy::cognitive_complexity)]

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use elephant_api::config::ApiConfig;
use elephant_api::runtime::RuntimeManager;
use elephant_api::AuthConfig;
use elephant_pipeline_bin::config::PipelineConfig;
use elephant_storage::{StorageBackendKind, StorageClient, StorageConfig};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use walkdir::WalkDir;

mod data_migrate;
mod wiki_demo;

#[derive(Parser, Debug)]
#[command(name = "elephant")]
#[command(about = "Elephant knowledge system", long_about = None)]
struct Cli {
    #[command(flatten)]
    api: ApiTarget,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Debug, Clone, Default)]
struct ApiTarget {
    /// Base URL for the running Elephant API.
    #[arg(long, global = true)]
    api_url: Option<String>,
    /// Bearer token for admin API calls.
    #[arg(long, global = true)]
    token: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the HTTP + MCP server and managed workers.
    Serve(ServeArgs),
    /// Run the advanced standalone pipeline worker.
    Worker,
    /// Apply the `SurrealDB` schema and optional extension SQL.
    Migrate(MigrateArgs),
    /// Import or export a live embedded snapshot through the running API.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    /// One-time data migration: embedded snapshot -> `SurrealDB` backend.
    DataMigrate(data_migrate::DataMigrateArgs),
    /// Show health and managed runtime status for a running Elephant API.
    Status(StatusArgs),
    /// Launch an embedded, evidence-backed Living Wiki demonstration.
    DemoWiki(DemoWikiArgs),
}

#[derive(Args, Debug, Clone, Default)]
struct ServeArgs {
    /// Run only HTTP/MCP server routes; do not start managed workers.
    #[arg(long)]
    server_only: bool,
}

#[derive(Args, Debug, Clone, Default)]
struct MigrateArgs {
    /// Optional directory containing additional .surql files to apply.
    #[arg(short, long)]
    extension_dir: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum SnapshotCommand {
    /// Export the live embedded snapshot to a file on the Elephant host.
    Export { path: PathBuf },
    /// Import a snapshot file into the live embedded store.
    Import { path: PathBuf },
}

#[derive(Args, Debug, Clone, Default)]
struct StatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct DemoWikiArgs {
    /// HTTP port for the demonstration browser.
    #[arg(long, default_value_t = 3000)]
    port: u16,
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

#[derive(Serialize)]
struct SnapshotRequest {
    path: String,
}

#[derive(Deserialize, Serialize)]
struct SnapshotResponse {
    success: bool,
    backend: String,
    path: String,
    message: String,
}

#[derive(Serialize)]
struct StatusResponse {
    health: serde_json::Value,
    runtime: Option<serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;

    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Serve(args) => run_serve(args).await,
        Commands::Worker => run_worker().await,
        Commands::Migrate(args) => apply_schema(args.extension_dir).await,
        Commands::Snapshot { command } => match command {
            SnapshotCommand::Export { path } => {
                run_snapshot_command(&cli.api, SnapshotAction::Export, path).await
            }
            SnapshotCommand::Import { path } => {
                run_snapshot_command(&cli.api, SnapshotAction::Import, path).await
            }
        },
        Commands::Status(args) => run_status_command(&cli.api, args).await,
        Commands::DataMigrate(args) => data_migrate::run(args).await,
        Commands::DemoWiki(args) => Box::pin(wiki_demo::run(args.port)).await,
    };

    if let Err(err) = result {
        error!("{err:#}");
        std::process::exit(1);
    }

    Ok(())
}

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .context("invalid RUST_LOG filter")?;
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

async fn run_serve(args: ServeArgs) -> Result<()> {
    validate_backend_env()?;
    let config = ApiConfig::from_env()?;
    let auth_config = AuthConfig::from_env()?;
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let runtime = RuntimeManager::new_with_ingest_enabled(Some(!args.server_only)).await?;
    let status = runtime.get_status().await;
    info!(
        configured = status.configured,
        bootstrap_mode = status.bootstrap_mode,
        backend = status.backend.as_deref().unwrap_or("unconfigured"),
        worker_ready = status.worker_ready,
        semantic_ingest_ready = status.semantic_ingest_ready,
        "Starting Elephant server"
    );
    let runtime = Arc::new(runtime);
    let snapshot_autosave_path = snapshot_autosave_path(&config);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let api_runtime = runtime.clone();
    let mut api_task = tokio::spawn(async move {
        elephant_api::run_api_with_managed_runtime(
            config,
            auth_config,
            api_runtime,
            Some(shutdown_rx),
        )
        .await
    });

    tokio::select! {
        result = &mut api_task => {
            result.context("Elephant API task panicked")??;
            Ok(())
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for Ctrl-C")?;
            info!("Shutdown requested; draining Elephant runtime");
            runtime.shutdown().await;
            if let Some(path) = snapshot_autosave_path {
                if let Some(storage) = runtime.current_storage().await {
                    match storage.export_snapshot(path.clone()).await {
                        Ok(()) => info!(path = %path.display(), "Exported embedded snapshot on shutdown"),
                        Err(err) => warn!(path = %path.display(), error = %err, "Failed to export embedded snapshot on shutdown"),
                    }
                }
            }
            let _ = shutdown_tx.send(true);
            api_task.await.context("Elephant API task panicked")??;
            Ok(())
        }
    }
}

fn snapshot_autosave_path(config: &ApiConfig) -> Option<PathBuf> {
    if config.db_backend != StorageBackendKind::ElephantDb {
        return None;
    }

    if env_flag_disabled("ELEPHANT_DB_SNAPSHOT_AUTOSAVE") {
        return None;
    }

    env::var("ELEPHANT_DB_SNAPSHOT_SAVE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| config.db_snapshot_load.clone())
}

fn env_flag_disabled(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "0" | "false" | "FALSE" | "off" | "OFF"))
        .unwrap_or(false)
}

async fn run_worker() -> Result<()> {
    validate_backend_env()?;
    let config = PipelineConfig::from_env()?;
    if config.db_backend == StorageBackendKind::ElephantDb {
        anyhow::bail!(
            "`elephant worker` does not support ELEPHANT_DB=elephant-db; use `elephant serve` for embedded all-in-one mode"
        );
    }

    let storage_config = StorageConfig {
        backend: config.db_backend,
        url: config.db_url.clone(),
        username: config.db_user.clone(),
        password: config.db_pass.clone(),
        namespace: config.db_namespace.clone(),
        database: config.db_database.clone(),
        snapshot_load: config.db_snapshot_load.clone(),
        ..StorageConfig::default()
    }
    .with_env_timeouts()?;
    let storage = StorageClient::new(storage_config)
        .await
        .context("failed to connect to Elephant storage")?;
    elephant_pipeline_bin::run_pipeline_with_client(config, storage).await
}

async fn apply_schema(extension_dir: Option<PathBuf>) -> Result<()> {
    use elephant_migrations::apply_schema;
    use std::fs;

    let backend = configured_backend_from_env()?;
    if backend == StorageBackendKind::ElephantDb {
        anyhow::bail!("`elephant migrate` is only supported with ELEPHANT_DB=surreal-db");
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

    apply_schema(&client)
        .await
        .map_err(|err| anyhow::anyhow!(err))?;

    if let Some(dir) = extension_dir {
        let mut entries: Vec<_> = WalkDir::new(&dir)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "surql"))
            .collect();
        entries.sort_by_key(|entry| entry.file_name().to_os_string());

        for entry in entries {
            let path = entry.path();
            let name = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("unknown");
            let sql = fs::read_to_string(path)?;
            info!("Applying extension: {name}");
            client
                .query(&sql)
                .await
                .map_err(|err| anyhow::anyhow!("failed to apply {name}: {err}"))?;
        }
    }

    info!("Schema applied successfully");
    Ok(())
}

async fn run_snapshot_command(
    target: &ApiTarget,
    action: SnapshotAction,
    path: PathBuf,
) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}{}", api_base_url_from_target(target), action.endpoint());
    let mut request = client.post(url).json(&SnapshotRequest {
        path: path.to_string_lossy().into_owned(),
    });
    if let Some(token) = api_token_from_target(target) {
        request = request.bearer_auth(token);
    }

    let response = request.send().await.map_err(|err| {
        anyhow::anyhow!("failed to call snapshot {} endpoint: {err}", action.label())
    })?;
    let status = response.status();

    if status.is_success() {
        let payload: SnapshotResponse = response.json().await.map_err(|err| {
            anyhow::anyhow!(
                "snapshot {} succeeded but response decoding failed: {err}",
                action.label()
            )
        })?;
        if !payload.success {
            anyhow::bail!(
                "snapshot {} reported failure for {}",
                action.label(),
                payload.path
            );
        }
        println!("{} [{}]", payload.message, payload.backend);
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
        "snapshot {} failed ({}): {}",
        action.label(),
        status,
        message
    );
}

async fn run_status_command(target: &ApiTarget, args: StatusArgs) -> Result<()> {
    let client = reqwest::Client::new();
    let base_url = api_base_url_from_target(target);
    let mut health_request = client.get(format!("{base_url}/v1/health"));
    let mut runtime_request = client.get(format!("{base_url}/v1/runtime/status"));
    if let Some(token) = api_token_from_target(target) {
        health_request = health_request.bearer_auth(token.clone());
        runtime_request = runtime_request.bearer_auth(token);
    }

    let health_response = health_request
        .send()
        .await
        .context("failed to call health endpoint")?;
    let health_status = health_response.status();
    let health: serde_json::Value = health_response
        .json()
        .await
        .map_err(|err| anyhow::anyhow!("failed to decode health response: {err}"))?;

    let runtime = match runtime_request.send().await {
        Ok(response) if response.status().is_success() => response.json().await.ok(),
        _ => None,
    };

    let payload = StatusResponse { health, runtime };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    let health_label = payload
        .health
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| health_status.as_str());
    println!("health: {health_label}");
    if let Some(runtime) = payload.runtime {
        println!(
            "runtime: configured={}, bootstrap={}, backend={}, worker_ready={}, semantic_ingest_ready={}",
            json_bool(&runtime, "configured"),
            json_bool(&runtime, "bootstrap_mode"),
            runtime
                .get("backend")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unconfigured"),
            json_bool(&runtime, "worker_ready"),
            json_bool(&runtime, "semantic_ingest_ready")
        );
    } else {
        println!("runtime: unavailable");
    }
    Ok(())
}

fn api_base_url_from_target(target: &ApiTarget) -> String {
    let env_api_url = env::var("ELEPHANT_API_URL").ok();
    let env_host = env::var("ELEPHANT_HOST").ok();
    let env_port = env::var("ELEPHANT_PORT").ok();
    api_base_url(
        target.api_url.as_deref().or(env_api_url.as_deref()),
        env_host.as_deref(),
        env_port.as_deref(),
    )
}

fn api_token_from_target(target: &ApiTarget) -> Option<String> {
    target
        .token
        .clone()
        .or_else(|| env::var("ELEPHANT_API_BEARER_TOKEN").ok())
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
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

fn json_bool(value: &serde_json::Value, key: &str) -> bool {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn validate_backend_env() -> Result<()> {
    if let Ok(value) = env::var("ELEPHANT_DB") {
        value
            .parse::<StorageBackendKind>()
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("invalid ELEPHANT_DB value `{value}`: {err}"))?;
    }
    Ok(())
}

fn configured_backend_from_env() -> Result<StorageBackendKind> {
    match env::var("ELEPHANT_DB") {
        Ok(value) => value
            .parse()
            .map_err(|err| anyhow::anyhow!("invalid ELEPHANT_DB `{value}`: {err}")),
        Err(env::VarError::NotPresent) => Ok(StorageBackendKind::SurrealDb),
        Err(err) => Err(anyhow::anyhow!("failed to read ELEPHANT_DB: {err}")),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parses_command_tree() {
        Cli::try_parse_from(["elephant", "serve"]).unwrap();
        Cli::try_parse_from(["elephant", "serve", "--server-only"]).unwrap();
        Cli::try_parse_from(["elephant", "demo-wiki", "--port", "3100"]).unwrap();
        Cli::try_parse_from(["elephant", "worker"]).unwrap();
        Cli::try_parse_from(["elephant", "migrate"]).unwrap();
        Cli::try_parse_from(["elephant", "snapshot", "export", "state.json"]).unwrap();
        Cli::try_parse_from(["elephant", "snapshot", "import", "state.json"]).unwrap();
        Cli::try_parse_from(["elephant", "status", "--json"]).unwrap();
    }

    #[test]
    fn api_base_url_prefers_explicit_url() {
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
    fn snapshot_autosave_defaults_to_loaded_embedded_snapshot() {
        let mut config = ApiConfig {
            db_backend: StorageBackendKind::ElephantDb,
            db_snapshot_load: Some(PathBuf::from("state.snapshot")),
            ..ApiConfig::default()
        };
        assert_eq!(
            snapshot_autosave_path(&config),
            Some(PathBuf::from("state.snapshot"))
        );

        config.db_backend = StorageBackendKind::SurrealDb;
        assert_eq!(snapshot_autosave_path(&config), None);
    }
}
