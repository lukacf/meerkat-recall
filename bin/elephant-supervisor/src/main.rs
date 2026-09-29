use anyhow::{Context, Result};
use elephant_api::{config::ApiConfig, AuthConfig};
use elephant_pipeline_bin::{config::PipelineConfig, run_pipeline_with_shutdown};
use elephant_storage::{StorageBackendKind, StorageClient, StorageConfig};
use std::env;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const EMBEDDED_API_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const EMBEDDED_PIPELINE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

enum SurrealPipelineStart {
    Child(Child),
    ApiExited(ExitStatus),
    Shutdown,
}

fn backend_from_env() -> Result<StorageBackendKind> {
    match env::var("ELEPHANT_DB") {
        Ok(value) => value
            .parse()
            .map_err(|err| anyhow::anyhow!("Invalid ELEPHANT_DB value `{value}`: {err}")),
        Err(_) => Ok(StorageBackendKind::SurrealDb),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let pipeline_disabled = env::var("PIPELINE_DISABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let backend = backend_from_env()?;

    if backend == StorageBackendKind::ElephantDb {
        return run_embedded_supervisor(pipeline_disabled).await;
    }

    run_surreal_supervisor(pipeline_disabled).await
}

async fn run_surreal_supervisor(pipeline_disabled: bool) -> Result<()> {
    let mut api = spawn_api()?;
    if pipeline_disabled {
        let status = api.wait().await?;
        std::process::exit(status.code().unwrap_or(1));
    }

    let mut pipeline_backoff = Duration::from_secs(1);
    let mut pipeline =
        match start_surreal_pipeline_with_backoff(&mut api, &mut pipeline_backoff).await? {
            SurrealPipelineStart::Child(child) => child,
            SurrealPipelineStart::ApiExited(status) => {
                std::process::exit(status.code().unwrap_or(1));
            }
            SurrealPipelineStart::Shutdown => {
                terminate_child(&mut api).await;
                std::process::exit(0);
            }
        };

    loop {
        tokio::select! {
            status = api.wait() => {
                let status = status.context("api wait failed")?;
                terminate_child(&mut pipeline).await;
                std::process::exit(status.code().unwrap_or(1));
            }
            status = pipeline.wait() => {
                let status = status.context("pipeline wait failed")?;
                eprintln!("elephant-pipeline exited (code={:?}); restarting", status.code());
                match start_surreal_pipeline_with_backoff(&mut api, &mut pipeline_backoff).await? {
                    SurrealPipelineStart::Child(child) => {
                        pipeline = child;
                    }
                    SurrealPipelineStart::ApiExited(status) => {
                        terminate_child(&mut pipeline).await;
                        std::process::exit(status.code().unwrap_or(1));
                    }
                    SurrealPipelineStart::Shutdown => {
                        terminate_child(&mut api).await;
                        terminate_child(&mut pipeline).await;
                        std::process::exit(0);
                    }
                }
            }
            _ = shutdown_signal() => {
                terminate_child(&mut api).await;
                terminate_child(&mut pipeline).await;
                std::process::exit(0);
            }
        }
    }
}

async fn run_embedded_supervisor(pipeline_disabled: bool) -> Result<()> {
    let api_config = ApiConfig::from_env()?;
    let auth_config = AuthConfig::from_env()?;
    let pipeline_config = PipelineConfig::from_env()?;

    let storage_config = StorageConfig {
        backend: StorageBackendKind::ElephantDb,
        url: api_config.db_url.clone(),
        username: api_config.db_user.clone(),
        password: api_config.db_pass.clone(),
        namespace: api_config.db_namespace.clone(),
        database: api_config.db_database.clone(),
        snapshot_load: api_config
            .db_snapshot_load
            .clone()
            .or(pipeline_config.db_snapshot_load.clone()),
        ..StorageConfig::default()
    }
    .with_env_timeouts()?;
    let storage = StorageClient::new(storage_config)
        .await
        .context("failed to initialize embedded storage")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut api = tokio::spawn(elephant_api::run_api_with_client_and_shutdown(
        api_config,
        auth_config,
        storage.clone(),
        Some(shutdown_rx.clone()),
    ));

    if pipeline_disabled {
        tokio::select! {
            result = &mut api => {
                return result.context("embedded api task join failed")?;
            }
            _ = shutdown_signal() => {
                let _ = shutdown_tx.send(true);
                drain_embedded_api(&mut api).await;
                return Ok(());
            }
        }
    }

    let mut pipeline_backoff = Duration::from_secs(1);
    let mut pipeline = spawn_embedded_pipeline(
        pipeline_config.clone(),
        storage.clone(),
        shutdown_rx.clone(),
    );

    loop {
        tokio::select! {
            result = &mut api => {
                let _ = shutdown_tx.send(true);
                drain_embedded_pipeline(&mut pipeline).await;
                return result.context("embedded api task join failed")?;
            }
            result = &mut pipeline => {
                match result {
                    Ok(Ok(())) => {
                        warn!("embedded elephant-pipeline stopped; restarting");
                    }
                    Ok(Err(err)) => {
                        error!("embedded elephant-pipeline exited with error: {err}; restarting");
                    }
                    Err(err) => {
                        error!("embedded elephant-pipeline task failed: {err}; restarting");
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(pipeline_backoff) => {}
                    result = &mut api => {
                        let _ = shutdown_tx.send(true);
                        return result.context("embedded api task join failed")?;
                    }
                    _ = shutdown_signal() => {
                        info!("embedded supervisor received shutdown signal");
                        let _ = shutdown_tx.send(true);
                        drain_embedded_api(&mut api).await;
                        return Ok(());
                    }
                }
                pipeline = spawn_embedded_pipeline(
                    pipeline_config.clone(),
                    storage.clone(),
                    shutdown_rx.clone(),
                );
                pipeline_backoff = (pipeline_backoff * 2).min(MAX_BACKOFF);
            }
            _ = shutdown_signal() => {
                info!("embedded supervisor received shutdown signal");
                let _ = shutdown_tx.send(true);
                drain_embedded_api(&mut api).await;
                drain_embedded_pipeline(&mut pipeline).await;
                return Ok(());
            }
        }
    }
}

async fn start_surreal_pipeline_with_backoff(
    api: &mut Child,
    backoff: &mut Duration,
) -> Result<SurrealPipelineStart> {
    loop {
        match spawn_pipeline() {
            Ok(child) => {
                *backoff = Duration::from_secs(1);
                return Ok(SurrealPipelineStart::Child(child));
            }
            Err(err) => {
                eprintln!(
                    "failed to start elephant-pipeline ({err}); retrying in {}s",
                    backoff.as_secs()
                );
                tokio::select! {
                    _ = tokio::time::sleep(*backoff) => {}
                    status = api.wait() => {
                        return Ok(SurrealPipelineStart::ApiExited(
                            status.context("api wait failed while retrying pipeline startup")?
                        ));
                    }
                    _ = shutdown_signal() => {
                        return Ok(SurrealPipelineStart::Shutdown);
                    }
                }
                *backoff = (*backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

fn spawn_api() -> Result<Child> {
    let mut cmd = Command::new(local_binary_path("elephant-api")?);
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    cmd.spawn().context("failed to start elephant-api")
}

fn spawn_pipeline() -> Result<Child> {
    let mut cmd = Command::new(local_binary_path("elephant-pipeline")?);
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    cmd.spawn().context("failed to start elephant-pipeline")
}

fn local_binary_path(binary_name: &str) -> Result<PathBuf> {
    let current_exe = env::current_exe().context("failed to resolve supervisor executable")?;
    let Some(bin_dir) = current_exe.parent() else {
        return Ok(PathBuf::from(binary_name));
    };
    let local_path = bin_dir.join(binary_name);
    if local_path.exists() {
        return Ok(local_path);
    }
    Ok(PathBuf::from(binary_name))
}

fn spawn_embedded_pipeline(
    config: PipelineConfig,
    storage: StorageClient,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run_pipeline_with_shutdown(config, storage, shutdown_rx))
}

async fn drain_embedded_pipeline(handle: &mut JoinHandle<anyhow::Result<()>>) {
    match tokio::time::timeout(EMBEDDED_PIPELINE_DRAIN_TIMEOUT, &mut *handle).await {
        Ok(Ok(Ok(()))) => info!("embedded elephant-pipeline drained successfully"),
        Ok(Ok(Err(err))) => warn!("embedded elephant-pipeline stopped with error: {err}"),
        Ok(Err(err)) => warn!("embedded elephant-pipeline join failed during drain: {err}"),
        Err(_) => {
            warn!(
                "embedded elephant-pipeline did not drain within {}s; aborting",
                EMBEDDED_PIPELINE_DRAIN_TIMEOUT.as_secs()
            );
            handle.abort();
            let _ = handle.await;
        }
    }
}

async fn drain_embedded_api(handle: &mut JoinHandle<anyhow::Result<()>>) {
    match tokio::time::timeout(EMBEDDED_API_DRAIN_TIMEOUT, &mut *handle).await {
        Ok(Ok(Ok(()))) => info!("embedded elephant-api drained successfully"),
        Ok(Ok(Err(err))) => warn!("embedded elephant-api stopped with error: {err}"),
        Ok(Err(err)) => warn!("embedded elephant-api join failed during drain: {err}"),
        Err(_) => {
            warn!(
                "embedded elephant-api did not drain within {}s; aborting",
                EMBEDDED_API_DRAIN_TIMEOUT.as_secs()
            );
            handle.abort();
            let _ = handle.await;
        }
    }
}

async fn terminate_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = term.recv() => {},
            _ = int.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::backend_from_env;
    use elephant_storage::StorageBackendKind;
    use std::env;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn test_backend_from_env_invalid_value_returns_error() {
        let _guard = env_lock().lock().expect("env lock");
        let prior = env::var("ELEPHANT_DB").ok();
        env::set_var("ELEPHANT_DB", "definitely-not-a-backend");

        let err = backend_from_env().expect_err("invalid backend should error");
        assert!(err
            .to_string()
            .contains("Invalid ELEPHANT_DB value `definitely-not-a-backend`"));

        match prior {
            Some(value) => env::set_var("ELEPHANT_DB", value),
            None => env::remove_var("ELEPHANT_DB"),
        }
    }

    #[test]
    fn test_backend_from_env_defaults_to_surreal() {
        let _guard = env_lock().lock().expect("env lock");
        let prior = env::var("ELEPHANT_DB").ok();
        env::remove_var("ELEPHANT_DB");

        assert_eq!(
            backend_from_env().expect("default backend should parse"),
            StorageBackendKind::SurrealDb
        );

        if let Some(value) = prior {
            env::set_var("ELEPHANT_DB", value);
        }
    }
}
