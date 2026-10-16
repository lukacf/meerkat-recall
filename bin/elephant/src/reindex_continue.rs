//! Explicit local administrative continuation; no scheduler or extraction path.
use anyhow::{ensure, Context};
use elephant_pipeline::embedding_reindex::{process_embedding_reindex_for_target, ReindexProgress};
use elephant_storage::SpaceConfigRepository;
use elephant_types::{RecordId, TableName};
use std::io::Write;
use std::path::PathBuf;

#[derive(clap::Args, Debug, Clone)]
pub struct ReindexContinueArgs {
    #[command(flatten)]
    pub target: crate::data_migrate::TransferTarget,
    #[arg(long)]
    pub space: String,
    /// Exact already accepted reindex ID. This command never stages or approves it.
    #[arg(long)]
    pub reindex_id: String,
    #[arg(long)]
    pub configuration_id: String,
    /// Bound each page; persisted progress precedes the next page.
    #[arg(long, default_value_t=128, value_parser=clap::value_parser!(u16).range(1..=256))]
    pub page_size: u16,
    /// Stop admitting pages after this duration; an admitted page may finish later.
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..=3600))]
    pub max_elapsed_seconds: u64,
    /// New durable JSONL progress file. Interrupted operations resume from storage.
    #[arg(long)]
    pub progress: PathBuf,
}
fn native_local_url(url: &str) -> anyhow::Result<()> {
    let path = url
        .strip_prefix("rocksdb://")
        .context("administrative continuation requires an explicit local rocksdb:// URL")?;
    ensure!(
        std::path::Path::new(path).is_absolute(),
        "native target path must be absolute"
    );
    Ok(())
}
fn validate_progress(
    expected: &str,
    previous: Option<&ReindexProgress>,
    current: &ReindexProgress,
) -> anyhow::Result<()> {
    ensure!(current.reindex_id == expected, "reindex target changed");
    if let Some(previous) = previous {
        ensure!(
            current.restarted_for_content_change
                || (current.embedded_nodes >= previous.embedded_nodes
                    && current.embedded_entities >= previous.embedded_entities),
            "durable reindex counts regressed without a content restart"
        );
    }
    Ok(())
}
fn record(output: &mut std::fs::File, value: &serde_json::Value) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    Ok(())
}
pub async fn run(mut args: ReindexContinueArgs) -> anyhow::Result<()> {
    let url = args
        .target
        .url
        .clone()
        .or_else(|| std::env::var("SURREALDB_URL").ok())
        .context("explicit native target URL required")?;
    native_local_url(&url)?;
    args.target.url = Some(url);
    let configuration: RecordId = args.configuration_id.parse()?;
    ensure!(
        configuration.table() == TableName::SpaceConfig
            && !args.space.trim().is_empty()
            && !args.reindex_id.trim().is_empty(),
        "invalid original reindex identity"
    );
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let output = options.open(&args.progress)?;
    let started = std::time::Instant::now();
    let client = args.target.clone().connect().await?;
    let result = run_connected(&args, &configuration, &client, output, started).await;
    crate::data_migrate::finish_owned_connection(client, result).await
}

// The caller retains the native writer lock until every outcome has been
// recorded and the shared owned-connection close has completed.
async fn run_connected(
    args: &ReindexContinueArgs,
    configuration: &RecordId,
    client: &elephant_storage::StorageClient,
    mut output: std::fs::File,
    started: std::time::Instant,
) -> anyhow::Result<()> {
    let config = SpaceConfigRepository::new(client.clone())
        .get_by_space_id(&args.space)
        .await?
        .context("space missing")?;
    ensure!(
        config.base.id == *configuration
            && config
                .processing
                .as_ref()
                .and_then(|p| p.pending_reindex.as_ref())
                .is_some_and(|p| p.id == args.reindex_id),
        "original accepted reindex differs"
    );
    record(
        &mut output,
        &serde_json::json!({"format":"recall-reindex-continuation-v1","state":"running","space":args.space,"configuration_id":configuration,"reindex_id":args.reindex_id,"page_size":args.page_size,"max_elapsed_seconds":args.max_elapsed_seconds,"activation_performed":false}),
    )?;
    #[cfg(unix)]
    std::fs::File::open(
        args.progress
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new(".")),
    )?
    .sync_all()?;
    let mut previous = None;
    let mut pages = 0_u64;
    loop {
        if started.elapsed().as_secs() >= args.max_elapsed_seconds {
            record(
                &mut output,
                &serde_json::json!({"state":"page_admission_deadline_reached","pages":pages,"elapsed_seconds":started.elapsed().as_secs_f64(),"activation_performed":false}),
            )?;
            anyhow::bail!(
                "page admission deadline reached; original durable reindex remains resumable"
            );
        }
        let result = process_embedding_reindex_for_target(
            client,
            &args.space,
            usize::from(args.page_size),
            &args.reindex_id,
            configuration,
        )
        .await;
        let progress = match result {
            Ok(progress) => progress,
            Err(error) => {
                record(
                    &mut output,
                    &serde_json::json!({"state":"page_failed","pages":pages,"elapsed_seconds":started.elapsed().as_secs_f64(),"activation_performed":false}),
                )?;
                return Err(error.into());
            }
        };
        validate_progress(&args.reindex_id, previous.as_ref(), &progress)?;
        pages += 1;
        record(
            &mut output,
            &serde_json::json!({"state":if progress.ready_to_activate {"coverage_ready_activation_pending"} else {"page_persisted"},"pages":pages,"elapsed_seconds":started.elapsed().as_secs_f64(),"progress":progress,"activation_performed":false}),
        )?;
        if progress.ready_to_activate {
            return Ok(());
        }
        previous = Some(progress);
        tokio::task::yield_now().await; // Successful pages continue without queue dependency delay.
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_owner_boundary_and_progress_restart_are_explicit() {
        assert!(native_local_url("rocksdb:///tmp/isolated-native").is_ok());
        for bad in [
            "rocksdb://relative",
            "memory",
            "ws://localhost:8000",
            "surrealkv:///tmp/target",
        ] {
            assert!(native_local_url(bad).is_err());
        }
        let progress = ReindexProgress {
            reindex_id: "original".into(),
            embedded_nodes: 256,
            embedded_entities: 0,
            ready_to_activate: false,
            restarted_for_content_change: false,
        };
        let mut next = ReindexProgress {
            reindex_id: "original".into(),
            embedded_nodes: 0,
            embedded_entities: 0,
            ready_to_activate: false,
            restarted_for_content_change: false,
        };
        assert!(validate_progress("original", Some(&progress), &next).is_err());
        next.restarted_for_content_change = true;
        assert!(validate_progress("original", Some(&progress), &next).is_ok());
        next.reindex_id = "replacement".into();
        assert!(validate_progress("original", Some(&progress), &next).is_err());
    }
}

#[cfg(all(test, feature = "rocksdb"))]
#[path = "reindex_continue_close_tests.rs"]
mod close_tests;
