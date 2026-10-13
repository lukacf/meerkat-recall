//! Success-only acknowledgment through the same helper used by wiki/TM workers.

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/outbox_support.rs"]
mod outbox_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_pipeline::outbox_delivery::deliver_batch;
use elephant_storage::snapshot::{export_named, restore_named, SnapshotRetention};
use elephant_storage::{
    OutboxCheckpointRepository, Repository, StorageBackendKind, StorageClient, StorageConfig,
    WorkItemRepository,
};

const SPACE: &str = "quiet-outbox-qualification";

#[tokio::test]
async fn callback_failure_after_durable_effect_keeps_checkpoint_and_replays_without_duplicate_work(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let event = outbox_support::seed(&client, SPACE).await?;
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        let failure = outbox_support::deliver(&client, SPACE, true)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("callback failure swallowed"))?;
        anyhow::ensure!(failure
            .to_string()
            .contains("qualification failure after durable queue effect"));
        anyhow::ensure!(checkpoints.load(SPACE, outbox_support::CONSUMER).await? == 0);
        anyhow::ensure!(
            WorkItemRepository::new(client.clone())
                .list(SPACE, 100, None)
                .await?
                .len()
                == 1
        );
        anyhow::ensure!(outbox_support::deliver(&client, SPACE, false).await? == 1);
        anyhow::ensure!(checkpoints.load(SPACE, outbox_support::CONSUMER).await? == event.seq);
        anyhow::ensure!(
            outbox_support::deliver(&client, SPACE, true).await? == 0,
            "empty batch invoked callback"
        );
        anyhow::ensure!(
            WorkItemRepository::new(client.clone())
                .list(SPACE, 100, None)
                .await?
                .len()
                == 1
        );
        anyhow::ensure!(
            checkpoints
                .load("another-named-space", outbox_support::CONSUMER)
                .await?
                == 0
        );
        anyhow::ensure!(checkpoints.load(SPACE, "another-consumer").await? == 0);
    }
    Ok(())
}

#[tokio::test]
async fn restore_cannot_split_consumer_effect_from_its_checkpoint_acknowledgment(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let embedded = matches!(backend, support::Backend::ElephantDb);
        let config = StorageConfig {
            backend: if embedded {
                StorageBackendKind::ElephantDb
            } else {
                StorageBackendKind::SurrealDb
            },
            url: "mem://".into(),
            namespace: format!("outbox_{}", uuid::Uuid::new_v4().simple()),
            database: "qualification".into(),
            ..StorageConfig::default()
        };
        let client = StorageClient::new_with_durability(
            config,
            embedded.then(|| directory.0.join("live.snapshot")),
            Some(directory.0.join("journal")),
        )
        .await?;
        if !embedded {
            elephant_migrations::apply_schema(&client)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        let event = outbox_support::seed(&client, SPACE).await?;
        let backup = directory.0.join("before-consumer-effect");
        export_named(
            &client,
            &backup,
            "before-consumer-effect",
            &SnapshotRetention::default(),
        )
        .await?;
        let (entered_send, entered_recv) = tokio::sync::oneshot::channel();
        let (release_send, release_recv) = tokio::sync::oneshot::channel();
        let delivering = client.clone();
        let delivery = tokio::spawn(async move {
            deliver_batch(
                &delivering,
                SPACE,
                outbox_support::CONSUMER,
                10,
                |after, events| {
                    let delivering = &delivering;
                    async move {
                        anyhow::ensure!(after == 0 && events.len() == 1);
                        outbox_support::enqueue_effect(delivering, SPACE, &events[0]).await?;
                        entered_send
                            .send(())
                            .map_err(|_| anyhow::anyhow!("parent left delivery"))?;
                        release_recv.await?;
                        Ok::<(), anyhow::Error>(())
                    }
                },
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(3), entered_recv).await??;
        let restoring = client.clone();
        let restore = tokio::spawn(async move {
            restore_named(&restoring, &backup, restoring.erasure_journal()?).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while client.recovery_status().ready {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        anyhow::ensure!(
            !restore.is_finished(),
            "restore replaced dataset while consumer effects remained unacknowledged"
        );
        release_send
            .send(())
            .map_err(|_| anyhow::anyhow!("delivery left before release"))?;
        let count = tokio::time::timeout(std::time::Duration::from_secs(3), delivery).await???;
        anyhow::ensure!(count == 1);
        tokio::time::timeout(std::time::Duration::from_secs(3), restore).await???;
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        anyhow::ensure!(client.recovery_status().ready);
        anyhow::ensure!(
            checkpoints.load(SPACE, outbox_support::CONSUMER).await? == 0,
            "old delivery advanced restored cursor without restored effects"
        );
        anyhow::ensure!(WorkItemRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        anyhow::ensure!(outbox_support::deliver(&client, SPACE, false).await? == 1);
        anyhow::ensure!(checkpoints.load(SPACE, outbox_support::CONSUMER).await? == event.seq);
        anyhow::ensure!(
            WorkItemRepository::new(client)
                .list(SPACE, 100, None)
                .await?
                .len()
                == 1
        );
    }
    Ok(())
}
