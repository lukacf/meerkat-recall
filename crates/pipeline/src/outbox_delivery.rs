//! Success-aware outbox delivery under the storage restore/erasure barrier.

use std::future::Future;

use elephant_storage::{
    OutboxCheckpointRepository, StorageClient, StorageError, SystemEventRepository,
};
use elephant_types::records::SystemEvent;

/// Deliver one complete batch, then acknowledge its last sequence.
///
/// The activity guard spans checkpoint load, event fetch, all consumer effects,
/// and checkpoint compare-and-swap. Restore cannot replace effects between
/// their execution and acknowledgment. Consumers must make effects idempotent:
/// failed delivery or acknowledgment retries the entire unacknowledged batch.
/// The callback receives the original checkpoint and the ordered event batch.
///
/// # Errors
/// Returns storage, malformed-batch, consumer, or checkpoint conflict errors.
/// No acknowledgment is attempted after a consumer error.
pub async fn deliver_batch<E, F, Fut>(
    storage: &StorageClient,
    space: &str,
    consumer: &str,
    batch_size: usize,
    process: F,
) -> Result<usize, E>
where
    E: From<StorageError>,
    F: FnOnce(i64, Vec<SystemEvent>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    if space.is_empty() || consumer.is_empty() || batch_size == 0 {
        return Err(
            StorageError::DataIntegrityError("invalid outbox delivery scope".into()).into(),
        );
    }
    storage
        .with_activity(async {
            let checkpoints = OutboxCheckpointRepository::new(storage.clone());
            let after = checkpoints.load(space, consumer).await?;
            let events = SystemEventRepository::new(storage.clone())
                .get_after_seq(space, after, batch_size)
                .await?;
            let count = events.len();
            let mut last = after;
            for event in &events {
                if event.base.space_id != space || event.seq <= last {
                    return Err(StorageError::DataIntegrityError(
                        "outbox delivery identity or sequence mismatch".into(),
                    )
                    .into());
                }
                last = event.seq;
            }
            if count > 0 {
                process(after, events).await?;
                checkpoints.advance(space, consumer, after, last).await?;
            }
            Ok(count)
        })
        .await?
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use elephant_storage::{Repository, WorkItemRepository};
    use elephant_types::records::{BaseFields, WorkItem, WorkItemStatus};
    use elephant_types::{RecordId, TableName};

    async fn seed(client: &StorageClient) {
        SystemEventRepository::new(client.clone())
            .create(SystemEvent {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SystemEvent),
                    "quiet".into(),
                    "fixture".into(),
                ),
                seq: 7,
                event_type: "fixture.changed".into(),
                payload: serde_json::json!({}),
            })
            .await
            .expect("event");
    }

    async fn enqueue_effect(client: &StorageClient) -> crate::PipelineResult<()> {
        crate::source_queue::enqueue_idempotent(
            client,
            WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "quiet".into(),
                    "fixture".into(),
                ),
                kind: "tm_detect_conflicts".into(),
                payload: serde_json::json!({"batch": 7}),
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key: "delivery:7".into(),
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

    async fn failed_delivery_replays_once(client: StorageClient) {
        seed(&client).await;
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        let failed: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |after, events| {
                let client = &client;
                async move {
                    assert_eq!(after, 0);
                    assert_eq!(events.len(), 1);
                    enqueue_effect(client).await?;
                    Err(StorageError::QueryError("injected after durable effect".into()).into())
                }
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(checkpoints.load("quiet", "wiki").await.unwrap(), 0);
        let delivered: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |after, events| {
                let client = &client;
                async move {
                    assert_eq!(after, 0);
                    assert_eq!(events[0].seq, 7);
                    enqueue_effect(client).await
                }
            })
            .await;
        assert_eq!(delivered.unwrap(), 1);
        assert_eq!(checkpoints.load("quiet", "wiki").await.unwrap(), 7);
        assert_eq!(
            WorkItemRepository::new(client.clone())
                .list("quiet", 20, None)
                .await
                .unwrap()
                .len(),
            1
        );
        let empty: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |_, _| async {
                Err(StorageError::QueryError("empty callback must not run".into()).into())
            })
            .await;
        assert_eq!(empty.unwrap(), 0);
        assert_eq!(checkpoints.load("other", "wiki").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn embedded_failed_delivery_keeps_cursor_and_replays_idempotently() {
        failed_delivery_replays_once(StorageClient::new_embedded().await.unwrap()).await;
    }

    #[tokio::test]
    async fn surreal_failed_delivery_keeps_cursor_and_replays_idempotently() {
        failed_delivery_replays_once(StorageClient::new_memory().await.unwrap()).await;
    }

    #[tokio::test]
    async fn concurrent_checkpoint_change_fails_delivery_acknowledgment() {
        let client = StorageClient::new_embedded().await.unwrap();
        seed(&client).await;
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        let result: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |_, _| async {
                enqueue_effect(&client).await?;
                // A second acknowledged consumer wins the CAS first.
                checkpoints.advance("quiet", "wiki", 0, 7).await?;
                Ok(())
            })
            .await;
        assert!(
            result.is_err(),
            "the stale consumer cannot claim acknowledgment"
        );
        assert_eq!(checkpoints.load("quiet", "wiki").await.unwrap(), 7);
    }
    #[tokio::test]
    async fn malformed_native_view_dependency_never_advances_delivery_checkpoint() {
        let client = StorageClient::new_memory().await.expect("native memory");
        seed(&client).await;
        client.query_checked("CREATE knowledge_view_dependency:malformed_delivery_fixture SET space_id = 'quiet', dependency_record = 'affected';")
            .await.expect("persist malformed index pointer");
        let views = elephant_storage::knowledge_view_repository::KnowledgeViewRepository::new(
            client.clone(),
        );
        let first: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |_, events| {
                let views = views.clone();
                async move {
                    assert_eq!(events[0].seq, 7);
                    views
                        .mark_dirty_for_dependencies("quiet", &["affected".into()], "event")
                        .await?;
                    Ok(())
                }
            })
            .await;
        assert!(matches!(
            first,
            Err(crate::PipelineError::StorageError(
                StorageError::DataIntegrityError(_)
            ))
        ));
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        assert_eq!(
            checkpoints
                .load("quiet", "wiki")
                .await
                .expect("unacknowledged"),
            0
        );
        client
            .query_checked("DELETE knowledge_view_dependency:malformed_delivery_fixture;")
            .await
            .expect("repair invalid index");
        let replay: crate::PipelineResult<usize> =
            deliver_batch(&client, "quiet", "wiki", 20, |_, events| {
                let views = views.clone();
                async move {
                    assert_eq!(events[0].seq, 7);
                    views
                        .mark_dirty_for_dependencies("quiet", &["affected".into()], "event")
                        .await?;
                    Ok(())
                }
            })
            .await;
        assert_eq!(replay.expect("replay after actual repair"), 1);
        assert_eq!(
            checkpoints
                .load("quiet", "wiki")
                .await
                .expect("acknowledged"),
            7
        );
    }
}
