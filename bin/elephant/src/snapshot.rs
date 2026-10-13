//! Named operational snapshots use the recovery owner's format and barrier.
use crate::{api_base_url_from_target, api_token_from_target, bounded_json, ApiTarget};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum SnapshotCommand {
    /// Quiesce and export a named immutable bundle on the running server.
    Export(ExportArgs),
    /// Restore a verified bundle through the server's durable replay barrier.
    #[command(alias = "import")]
    Restore(InputArgs),
    /// Verify a local bundle's format, retention deadline, length and digest offline.
    Verify {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Inspect physical records in a verified local bundle without opening a database.
    Inspect {
        /// Existing local named snapshot bundle; never a running database endpoint.
        #[arg(long)]
        input: PathBuf,
        /// Return the bounded typed inventory as JSON.
        #[arg(long)]
        json: bool,
        /// Include original record/schema values; capture output in a private file.
        #[arg(long, requires = "json")]
        include_records: bool,
        /// Losslessly encode long values in a bounded private v2 blob dictionary.
        #[arg(long, requires_all = ["include_records", "json"])]
        compact_records: bool,
    },
    /// Apply the durable backup retention registry without opening a data store.
    Prune {
        /// Existing store-specific journal directory (never a data snapshot).
        #[arg(long)]
        journal_directory: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
pub struct ExportArgs {
    /// New bundle directory on the server, not on the invoking client.
    #[arg(long, required_unless_present = "path", conflicts_with = "path")]
    output: Option<PathBuf>,
    /// Compatibility positional server path.
    #[arg(hide = true)]
    path: Option<PathBuf>,
    #[arg(long, default_value = "manual")]
    name: String,
    #[arg(long, default_value_t = 7, value_parser = clap::value_parser!(u64).range(1..=30))]
    retention_days: u64,
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=60))]
    quiesce_timeout_seconds: u64,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
pub struct InputArgs {
    /// Existing bundle directory on the server.
    #[arg(long, required_unless_present = "path", conflicts_with = "path")]
    input: Option<PathBuf>,
    #[arg(hide = true)]
    path: Option<PathBuf>,
    #[arg(long)]
    json: bool,
}

#[derive(Serialize)]
struct Request {
    path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quiesce_timeout_seconds: Option<u64>,
}

pub async fn run(target: &ApiTarget, command: SnapshotCommand) -> Result<()> {
    match command {
        SnapshotCommand::Inspect {
            input,
            json,
            include_records,
            compact_records,
        } => {
            let limits = elephant_storage::snapshot_inspection::SnapshotInspectionLimits {
                include_records,
                compact_records,
                ..Default::default()
            };
            let inspection =
                elephant_storage::snapshot_inspection::inspect_snapshot(&input, &limits)?;
            if json {
                // Compact serialization retains the storage inspector's report
                // byte bound; pretty-printing could expand it substantially.
                println!("{}", serde_json::to_string(&inspection)?);
            } else {
                println!("snapshot inspected; use --json for the bounded physical inventory");
            }
            Ok(())
        }
        SnapshotCommand::Verify { input, json } => {
            let manifest = elephant_storage::snapshot::verify(&input).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&manifest)?);
            } else {
                println!(
                    "verified {} {} sha256={} bytes={}",
                    manifest.format, manifest.name, manifest.sha256, manifest.bytes
                );
            }
            Ok(())
        }
        SnapshotCommand::Prune {
            journal_directory,
            json,
        } => {
            anyhow::ensure!(
                journal_directory.is_dir(),
                "journal directory does not exist"
            );
            let journal =
                elephant_storage::erasure_journal::ErasureJournal::open(journal_directory)?;
            let removed = elephant_storage::snapshot::cleanup_expired(&journal)?;
            if json {
                println!("{}", serde_json::json!({"removed":removed}));
            } else {
                println!("removed {removed} expired backup generations");
            }
            Ok(())
        }
        SnapshotCommand::Export(args) => {
            let path = args
                .output
                .or(args.path)
                .context("snapshot output is required")?;
            call(
                target,
                "export",
                Request {
                    path,
                    name: Some(args.name),
                    retention_seconds: Some(args.retention_days * 86_400),
                    quiesce_timeout_seconds: Some(args.quiesce_timeout_seconds),
                },
                args.json,
            )
            .await
        }
        SnapshotCommand::Restore(args) => {
            let path = args
                .input
                .or(args.path)
                .context("snapshot input is required")?;
            call(
                target,
                "restore",
                Request {
                    path,
                    name: None,
                    retention_seconds: None,
                    quiesce_timeout_seconds: None,
                },
                args.json,
            )
            .await
        }
    }
}

async fn call(target: &ApiTarget, operation: &str, payload: Request, json: bool) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let url = format!(
        "{}/v1/system/snapshot/{operation}",
        api_base_url_from_target(target)
    );
    let mut request = client.post(url).json(&payload);
    if let Some(token) = api_token_from_target(target)? {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .context("snapshot management request failed; inspect server status before retrying")?;
    let status = response.status();
    let body = bounded_json(response).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
    }
    anyhow::ensure!(
        status.is_success(),
        "snapshot {operation} refused (HTTP {})",
        status.as_u16()
    );
    anyhow::ensure!(
        body.get("success").and_then(serde_json::Value::as_bool) == Some(true),
        "snapshot operation did not report success"
    );
    let manifest: elephant_storage::snapshot::SnapshotManifest = serde_json::from_value(
        body.get("manifest")
            .cloned()
            .context("snapshot response has no versioned manifest")?,
    )
    .context("malformed snapshot manifest")?;
    anyhow::ensure!(
        manifest.format == "recall-snapshot-v1"
            && manifest.version == 1
            && manifest.sha256.len() == 64
            && manifest.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "snapshot response has unsupported format/digest"
    );
    if !json {
        println!(
            "{operation} {} [{}] sha256={} expires_at={}",
            manifest.name, manifest.format, manifest.sha256, manifest.expires_at
        );
    }
    Ok(())
}
