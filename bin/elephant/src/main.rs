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
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use walkdir::WalkDir;

mod data_migrate;
mod service_token;
mod snapshot;
use snapshot::SnapshotCommand;
mod status;
mod wiki_demo;

#[derive(Parser, Debug)]
#[command(name = "meerkat-recall", version)]
#[command(about = "Meerkat Recall knowledge system", long_about = None)]
struct Cli {
    #[command(flatten)]
    api: ApiTarget,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Debug, Clone, Default)]
struct ApiTarget {
    /// Base URL for the running Meerkat Recall API.
    #[arg(long, global = true)]
    api_url: Option<String>,
    /// Bearer token for admin API calls.
    #[arg(long, global = true, conflicts_with = "token_file")]
    token: Option<String>,
    /// Read the bearer from a private file; useful for service probes.
    #[arg(long, global = true)]
    token_file: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the HTTP + MCP server and managed workers.
    Serve(ServeArgs),
    /// Run the advanced standalone pipeline worker.
    Worker,
    /// Apply the `SurrealDB` schema and optional extension SQL.
    Migrate(MigrateArgs),
    /// Export/restore named backups, verify bundles and enforce retention.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    /// One-time data migration: embedded snapshot -> `SurrealDB` backend.
    DataMigrate(data_migrate::DataMigrateArgs),
    /// Explicitly erase a partial target left by a failed data-migrate command.
    DataMigrateDiscard(data_migrate::DataMigrateDiscardArgs),
    /// Show health and managed runtime status for a running Meerkat Recall API.
    Status(StatusArgs),
    /// Provision local signing secrets and explicitly scoped service tokens.
    ServiceToken {
        #[command(subcommand)]
        command: service_token::ServiceTokenCommand,
    },
    /// Launch an embedded, evidence-backed Living Wiki demonstration.
    DemoWiki(DemoWikiArgs),
}

#[derive(Args, Debug, Clone, Default)]
struct ServeArgs {
    /// Explicit runtime TOML file (overrides `ELEPHANT_CONFIG` and project discovery).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Run only HTTP/MCP server routes; do not start managed workers.
    #[arg(long)]
    server_only: bool,
    /// Total shutdown deadline; must exceed the managed worker drain deadline.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(31..=300))]
    shutdown_timeout_seconds: u64,
}

#[derive(Args, Debug, Clone, Default)]
struct MigrateArgs {
    /// Explicit runtime TOML file, using the same storage selection as serve.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Optional directory containing additional .surql files to apply.
    #[arg(short, long)]
    extension_dir: Option<PathBuf>,
}

#[derive(Args, Debug, Clone, Default)]
struct StatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
    /// Exit with a stable readiness category unless the authenticated service is ready.
    #[arg(long)]
    require_ready: bool,
    /// Select one authorized named space for runtime readiness.
    #[arg(long)]
    space: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct DemoWikiArgs {
    /// HTTP port for the demonstration browser.
    #[arg(long, default_value_t = 3000)]
    port: u16,
    /// Explicit approved `SpaceProcessingConfig` JSON for this isolated model-backed demo.
    #[arg(long)]
    processing_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;

    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Serve(args) => run_serve(args).await,
        Commands::Worker => run_worker().await,
        Commands::Migrate(args) => apply_schema(args.extension_dir, args.config.as_deref()).await,
        Commands::Snapshot { command } => snapshot::run(&cli.api, command).await,
        Commands::Status(args) => status::run(&cli.api, args).await,
        Commands::ServiceToken { command } => service_token::run(command),
        Commands::DataMigrate(args) => data_migrate::run(args).await,
        Commands::DataMigrateDiscard(args) => data_migrate::discard(args).await,
        Commands::DemoWiki(args) => {
            Box::pin(wiki_demo::run(args.port, args.processing_config)).await
        }
    };

    if let Err(err) = result {
        error!("{err:#}");
        std::process::exit(
            err.downcast_ref::<status::StatusFailure>()
                .map_or(1, |error| error.0),
        );
    }

    Ok(())
}

fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .context("invalid RUST_LOG filter")?;
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[expect(
    clippy::significant_drop_tightening,
    clippy::single_match_else,
    reason = "Preserve runtime ownership through bounded shutdown, drain, and snapshot persistence."
)]
async fn run_serve(args: ServeArgs) -> Result<()> {
    let mut signals = ShutdownSignals::install()?;
    validate_backend_env()?;
    let config = ApiConfig::from_env()?;
    let auth_config = AuthConfig::from_env()?;
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let runtime =
        RuntimeManager::new_with_config_path(args.config.as_deref(), Some(!args.server_only))
            .await?;
    let status = runtime.get_status().await;
    info!(
        configured = status.configured,
        bootstrap_mode = status.bootstrap_mode,
        backend = status.backend.as_deref().unwrap_or("unconfigured"),
        worker_ready = status.worker_ready,
        semantic_ingest_ready = status.semantic_ingest_ready,
        "Starting Meerkat Recall server"
    );
    let runtime = Arc::new(runtime);
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
            result.context("Meerkat Recall API task panicked")??;
            Ok(())
        }
        signal = signals.wait() => {
            signal?;
            info!("Shutdown requested; withdrawing readiness and draining runtime");
            runtime.begin_shutdown().await;
            let drain = async {
                let workers = runtime.shutdown().await;
                let _ = shutdown_tx.send(true);
                workers?;
                (&mut api_task).await.context("Meerkat Recall API task panicked")??;
                runtime.persist_shutdown_snapshot().await?;
                Ok::<(), anyhow::Error>(())
            };
            match tokio::time::timeout(std::time::Duration::from_secs(args.shutdown_timeout_seconds), drain).await {
                Ok(result) => result,
                Err(_) => {
                    let _ = shutdown_tx.send(true);
                    api_task.abort();
                    anyhow::bail!("shutdown deadline expired; clean drain and persistence were not established")
                }
            }
        }
    }
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt:
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                        .context("install SIGINT handler")?,
                terminate:
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .context("install SIGTERM handler")?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn wait(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => {},
                _ = self.terminate.recv() => {},
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.context("listen for shutdown")
        }
    }
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
        .context("failed to connect to Meerkat Recall storage")?;
    elephant_pipeline_bin::run_pipeline_with_client(config, storage).await
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the storage client alive through the complete explicit migration operation."
)]
async fn apply_schema(
    extension_dir: Option<PathBuf>,
    config_path: Option<&std::path::Path>,
) -> Result<()> {
    use elephant_migrations::apply_schema;
    use std::fs;

    let config = elephant_api::runtime::resolve_storage_config_for_cli_with_path(config_path)?;
    if config.backend == StorageBackendKind::ElephantDb {
        anyhow::bail!("`elephant migrate` is only supported with ELEPHANT_DB=surreal-db");
    }
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
                .query_checked(&sql)
                .await
                .map_err(|err| anyhow::anyhow!("failed to apply {name}: {err}"))?;
        }
    }

    info!("Schema applied successfully");

    // Only with a real provider configured: the purge clears fake vectors so
    // the workers re-embed them, which a fake or missing provider cannot do.
    let policy = elephant_embeddings::EmbeddingPolicy::from_env();
    if matches!(policy.provider.as_str(), "openai" | "fastembed") {
        let report = elephant_storage::fake_embedding_repair::purge_fake_embeddings(&client)
            .await
            .map_err(|err| anyhow::anyhow!("fake embedding purge failed: {err}"))?;
        info!(?report, "Fake embedding purge");
    } else {
        info!(
            provider = %policy.provider,
            "Fake embedding purge skipped: no real embedding provider configured"
        );
    }
    Ok(())
}

/// Bound management responses without trusting Content-Length.
async fn bounded_json(mut response: reqwest::Response) -> Result<serde_json::Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            bytes.len() + chunk.len() <= 1_048_576,
            "management response exceeds 1 MiB"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid management JSON response")
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

fn api_token_from_target(target: &ApiTarget) -> Result<Option<String>> {
    let token = target
        .token
        .clone()
        .or_else(|| env::var("ELEPHANT_API_BEARER_TOKEN").ok())
        .filter(|value| !value.trim().is_empty());
    let path = target
        .token_file
        .clone()
        .or_else(|| env::var_os("ELEPHANT_API_BEARER_TOKEN_FILE").map(PathBuf::from));
    anyhow::ensure!(
        token.is_none() || path.is_none(),
        "provide only one API bearer token source"
    );
    if let Some(path) = path {
        return Ok(Some(elephant_auth::service_token::read_secret_file(&path)?));
    }
    Ok(token.map(|value| value.trim().to_owned()))
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

fn validate_backend_env() -> Result<()> {
    if let Ok(value) = env::var("ELEPHANT_DB") {
        value
            .parse::<StorageBackendKind>()
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("invalid ELEPHANT_DB value `{value}`: {err}"))?;
    }
    Ok(())
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
    fn parses_explicit_configuration_for_serve_and_migrate() {
        let cli = Cli::try_parse_from(["elephant", "serve", "--config", "/etc/recall/config.toml"])
            .unwrap();
        assert!(
            matches!(cli.command, Commands::Serve(args) if args.config.as_deref() == Some(std::path::Path::new("/etc/recall/config.toml")))
        );
        let cli = Cli::try_parse_from(["elephant", "migrate", "--config", "custom.toml"]).unwrap();
        assert!(
            matches!(cli.command, Commands::Migrate(args) if args.config.as_deref() == Some(std::path::Path::new("custom.toml")))
        );
    }

    #[test]
    fn discarding_transfer_requires_separate_explicit_confirmation() {
        let digest = "a".repeat(64);
        assert!(Cli::try_parse_from([
            "elephant",
            "data-migrate-discard",
            "--source-sha256",
            &digest
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["elephant", "data-migrate-discard", "--discard-target"]).is_err()
        );
        Cli::try_parse_from([
            "elephant",
            "data-migrate-discard",
            "--source-sha256",
            &digest,
            "--discard-target",
            "--url",
            "memory",
        ])
        .unwrap();
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
}
