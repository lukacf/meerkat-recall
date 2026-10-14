//! Actual shared delivery helper and durable queue side effect used by D tests.
#![allow(dead_code)]

use elephant_pipeline::outbox_delivery::deliver_batch;
use elephant_pipeline::source_queue::enqueue_idempotent;
use elephant_storage::{Repository, StorageClient, SystemEventRepository};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{SystemEvent, WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};

pub const CONSUMER: &str = "qualification-success-only";

pub async fn seed(client: &StorageClient, space: &str) -> anyhow::Result<SystemEvent> {
    Ok(SystemEventRepository::new(client.clone())
        .create(SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                space.into(),
                "qualification".into(),
            ),
            seq: 7,
            event_type: "qualification.changed".into(),
            payload: serde_json::json!({"source":"independent fixture"}),
        })
        .await?)
}

pub async fn enqueue_effect(
    client: &StorageClient,
    space: &str,
    event: &SystemEvent,
) -> anyhow::Result<()> {
    anyhow::ensure!(event.base.space_id == space && event.seq > 0);
    enqueue_idempotent(
        client,
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space.into(),
                "qualification".into(),
            ),
            kind: "tm_detect_conflicts".into(),
            payload: serde_json::json!({"event_ids":[event.base.id.to_string()]}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("qualification-delivery-{}", event.seq),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        },
    )
    .await?;
    Ok(())
}

pub async fn deliver(
    client: &StorageClient,
    space: &str,
    fail_after_effect: bool,
) -> anyhow::Result<usize> {
    deliver_batch(client, space, CONSUMER, 10, |_, events| async move {
        for event in events {
            enqueue_effect(client, space, &event).await?;
        }
        anyhow::ensure!(
            !fail_after_effect,
            "qualification failure after durable queue effect"
        );
        Ok(())
    })
    .await
}
