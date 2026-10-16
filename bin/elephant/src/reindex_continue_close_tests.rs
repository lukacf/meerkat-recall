//! Public command closure over a real local datastore without provider dispatch.
use super::{run, ReindexContinueArgs};
use crate::data_migrate::TransferTarget;
use anyhow::{ensure, Context};
use elephant_storage::{Repository, SpaceConfigRepository};
use elephant_types::records::{BaseFields, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

const SPACE: &str = "reindex-close-fixture";

fn configuration() -> anyhow::Result<SpaceConfig> {
    Ok(SpaceConfig {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            SPACE.into(),
            "fixture-owner".into(),
        ),
        embedding_provider: "openai".into(),
        embedding_model: "unused-fixture-model".into(),
        embedding_dims: 2,
        purpose: None,
        skill: None,
        processing: Some(serde_json::from_value(json!({
            "generation":1,"index_generation":0,"policy_id":"default",
            "connections":{"fixture":{"id":"fixture","provider":"openai",
                "account_id":"fixture","endpoint":"https://example.invalid/v1",
                "credential_ref":"UNUSED_REINDEX_CLOSE_FIXTURE","credential_generation":1,"revoked":false}},
            "profiles":{"fixture":{"id":"fixture","connection_id":"fixture",
                "model":"unused-fixture-model","model_version":"1","dimensions":2,"preprocessing":"text-full-v2"}},
            "bindings":{
                "document_embedding":{"id":"document","profile_id":"fixture","revoked":false},
                "query_embedding":{"id":"query","profile_id":"fixture","revoked":false},
                "reindex_embedding":{"id":"reindex","profile_id":"fixture","revoked":false}},
            "active_embedding_profile_id":"fixture","pending_reindex":null
        }))?),
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original native client through full schema and empty progress setup, then the explicit consuming close before returning command arguments."
)]
async fn fixture(root: &Path) -> anyhow::Result<(ReindexContinueArgs, Value)> {
    let target = TransferTarget {
        url: Some(format!("rocksdb://{}", root.join("database").display())),
        namespace: Some("reindex_close".into()),
        database: Some("reindex_close".into()),
    };
    let client = target.clone().connect().await?;
    elephant_migrations::apply_schema(&client)
        .await
        .map_err(anyhow::Error::msg)?;
    let original = SpaceConfigRepository::new(client.clone())
        .create(configuration()?)
        .await?;
    let staged = SpaceConfigRepository::new(client.clone())
        .rebuild_active_embedding_profile(&original, "fixture")
        .await?;
    // This fixture has no documents, nodes or entities. Close both empty phases
    // using the governed progress CAS; completion must not activate the profile.
    let nodes = SpaceConfigRepository::new(client.clone())
        .record_embedding_reindex_page(&staged, None, 0, true)
        .await?;
    let complete = SpaceConfigRepository::new(client.clone())
        .record_embedding_reindex_page(&nodes, None, 0, true)
        .await?;
    let pending = complete
        .processing
        .as_ref()
        .and_then(|p| p.pending_reindex.as_ref())
        .context("staged reindex absent")?;
    ensure!(pending.complete && pending.embedded_nodes == 0 && pending.embedded_entities == 0);
    let args = ReindexContinueArgs {
        target,
        space: SPACE.into(),
        reindex_id: pending.id.clone(),
        configuration_id: complete.base.id.to_string(),
        page_size: 1,
        max_elapsed_seconds: 30,
        progress: root.join("progress.jsonl"),
    };
    let before = serde_json::to_value(complete)?;
    client.close_owned_command_connection(30_000).await?;
    Ok((args, before))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain reopened native client through exact original configuration and no-activation assertions, then consume it with the explicit awaited close."
)]
async fn reopen_unchanged(target: TransferTarget, before: &Value) -> anyhow::Result<()> {
    // Immediate same-runtime open: no retry, sleep, runtime teardown or removed lock.
    let client = target.connect().await?;
    let after = SpaceConfigRepository::new(client.clone())
        .get_by_space_id(SPACE)
        .await?
        .context("original config disappeared")?;
    assert_eq!(&serde_json::to_value(&after)?, before);
    assert!(after
        .processing
        .as_ref()
        .is_some_and(|p| p.completed_embedding_reindex.is_none()
            && p.pending_reindex
                .as_ref()
                .is_some_and(|pending| pending.complete)));
    client.close_owned_command_connection(30_000).await?;
    Ok(())
}

fn records(path: &Path) -> anyhow::Result<Vec<Value>> {
    std::fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

#[tokio::test]
async fn owned_reindex_success_closes_before_immediate_native_reopen() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let (args, before) = fixture(directory.path()).await?;
    let target = args.target.clone();
    let progress = args.progress.clone();
    tokio::time::timeout(Duration::from_secs(60), Box::pin(run(args))).await??;
    let rows = records(&progress)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["state"], "running");
    assert_eq!(rows[1]["state"], "coverage_ready_activation_pending");
    assert_eq!(rows[1]["pages"], 1);
    assert_eq!(rows[1]["activation_performed"], false);
    assert_eq!(rows[1]["progress"]["embedded_nodes"], 0);
    assert_eq!(rows[1]["progress"]["embedded_entities"], 0);
    reopen_unchanged(target, &before).await
}

#[tokio::test]
async fn owned_reindex_preflight_error_closes_and_preserves_original_error() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut args, before) = fixture(directory.path()).await?;
    args.reindex_id = "different-original-target".into();
    let target = args.target.clone();
    let progress = args.progress.clone();
    let result = tokio::time::timeout(Duration::from_secs(60), Box::pin(run(args))).await?;
    let error = result
        .err()
        .context("wrong target unexpectedly succeeded")?;
    assert_eq!(error.to_string(), "original accepted reindex differs");
    assert!(std::fs::read(progress)?.is_empty());
    reopen_unchanged(target, &before).await
}

#[tokio::test]
async fn owned_reindex_deadline_error_closes_after_durable_progress() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut args, before) = fixture(directory.path()).await?;
    // Internal args make the first page admission expire deterministically;
    // this tests a post-running-record exit without dispatching any provider.
    args.max_elapsed_seconds = 0;
    let target = args.target.clone();
    let progress = args.progress.clone();
    let result = tokio::time::timeout(Duration::from_secs(60), Box::pin(run(args))).await?;
    let error = result
        .err()
        .context("expired admission unexpectedly succeeded")?;
    assert_eq!(
        error.to_string(),
        "page admission deadline reached; original durable reindex remains resumable"
    );
    let rows = records(&progress)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["state"], "running");
    assert_eq!(rows[1]["state"], "page_admission_deadline_reached");
    assert_eq!(rows[1]["pages"], 0);
    assert_eq!(rows[1]["activation_performed"], false);
    reopen_unchanged(target, &before).await
}
