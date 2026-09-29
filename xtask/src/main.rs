use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Parser)]
#[command(author, version, about = "Repo automation tasks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build release binaries (api + pipeline).
    Build,
    /// Run the API in dev mode (cargo run).
    Dev {
        /// Bind host (default: ELEPHANT_HOST or 0.0.0.0).
        #[arg(long)]
        host: Option<String>,
        /// Bind port (default: ELEPHANT_PORT or 3000).
        #[arg(long)]
        port: Option<u16>,
        /// Skip auth (default: true for dev unless SKIP_AUTH is set).
        #[arg(long)]
        skip_auth: bool,
    },
    /// Build release binary then run the API.
    DeployDev {
        /// Bind host (default: ELEPHANT_HOST or 0.0.0.0).
        #[arg(long)]
        host: Option<String>,
        /// Bind port (default: ELEPHANT_PORT or 3000).
        #[arg(long)]
        port: Option<u16>,
        /// Skip auth (default: true for dev unless SKIP_AUTH is set).
        #[arg(long)]
        skip_auth: bool,
    },
    /// Run docker-compose for surrealdb + elephant-api (dev convenience).
    DockerDev {
        /// Bind host (default: ELEPHANT_HOST or 0.0.0.0).
        #[arg(long)]
        host: Option<String>,
        /// Bind port (default: ELEPHANT_PORT or 3000).
        #[arg(long)]
        port: Option<u16>,
        /// Build images before start.
        #[arg(long)]
        build: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Build => build_release(),
        Commands::Dev {
            host,
            port,
            skip_auth,
        } => run_dev(host, port, skip_auth),
        Commands::DeployDev {
            host,
            port,
            skip_auth,
        } => deploy_dev(host, port, skip_auth),
        Commands::DockerDev { host, port, build } => docker_dev(host, port, build),
    }
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .context("xtask must live in the workspace root")?;
    Ok(root.to_path_buf())
}

fn build_release() -> Result<()> {
    let root = workspace_root()?;
    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--bin")
        .arg("elephant-api")
        .arg("--bin")
        .arg("elephant-pipeline")
        .current_dir(root)
        .status()
        .context("failed to run cargo build")?;

    if !status.success() {
        bail!("release build failed");
    }
    Ok(())
}

fn run_dev(host: Option<String>, port: Option<u16>, skip_auth: bool) -> Result<()> {
    let root = workspace_root()?;
    let mut cmd = Command::new("cargo");
    cmd.arg("run").arg("-p").arg("elephant-api");
    cmd.current_dir(root);
    apply_env(&mut cmd, host, port, skip_auth)?;
    let status = cmd.status().context("failed to run elephant-api")?;
    if !status.success() {
        bail!("elephant-api exited with non-zero status");
    }
    Ok(())
}

fn deploy_dev(host: Option<String>, port: Option<u16>, skip_auth: bool) -> Result<()> {
    build_release()?;
    let root = workspace_root()?;
    let bin = root.join("target/release/elephant-api");
    if !bin.exists() {
        bail!("release binary not found at {}", bin.display());
    }

    let mut cmd = Command::new(bin);
    cmd.current_dir(root);
    apply_env(&mut cmd, host, port, skip_auth)?;
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = cmd.status().context("failed to run elephant-api")?;
    if !status.success() {
        bail!("elephant-api exited with non-zero status");
    }
    Ok(())
}

fn docker_dev(host: Option<String>, port: Option<u16>, build: bool) -> Result<()> {
    let root = workspace_root()?;
    let host = host
        .or_else(|| env::var("ELEPHANT_HOST").ok())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let port = port
        .or_else(|| env::var("ELEPHANT_PORT").ok().and_then(|v| v.parse().ok()))
        .unwrap_or(3000);

    let mut cmd = Command::new("docker");
    cmd.arg("compose").arg("up").arg("-d");
    if build {
        cmd.arg("--build");
    }
    cmd.arg("surrealdb").arg("elephant-api");
    cmd.current_dir(root);
    cmd.env("ELEPHANT_HOST", host);
    cmd.env("ELEPHANT_PORT", port.to_string());

    let status = cmd.status().context("failed to run docker compose")?;
    if !status.success() {
        bail!("docker compose up failed");
    }
    Ok(())
}

fn apply_env(
    cmd: &mut Command,
    host: Option<String>,
    port: Option<u16>,
    skip_auth: bool,
) -> Result<()> {
    let host = host
        .or_else(|| env::var("ELEPHANT_HOST").ok())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let port = port
        .or_else(|| env::var("ELEPHANT_PORT").ok().and_then(|v| v.parse().ok()))
        .unwrap_or(3000);

    cmd.env("ELEPHANT_HOST", host);
    cmd.env("ELEPHANT_PORT", port.to_string());

    if env::var("RUST_LOG").is_err() {
        cmd.env("RUST_LOG", "info");
    }

    if skip_auth || env::var("SKIP_AUTH").is_ok() {
        cmd.env("SKIP_AUTH", "true");
    }

    // Do not override SurrealDB settings if user already provided them.
    if env::var("SURREALDB_URL").is_err() {
        cmd.env("SURREALDB_URL", "ws://127.0.0.1:8000");
    }
    if env::var("SURREALDB_NAMESPACE").is_err() {
        cmd.env("SURREALDB_NAMESPACE", "elephant");
    }
    if env::var("SURREALDB_DATABASE").is_err() {
        cmd.env("SURREALDB_DATABASE", "main");
    }
    if env::var("SURREALDB_USER").is_err() {
        cmd.env("SURREALDB_USER", "root");
    }
    if env::var("SURREALDB_PASS").is_err() {
        cmd.env("SURREALDB_PASS", "root");
    }

    Ok(())
}
