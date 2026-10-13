//! Dead letter handling for failed work items.
//!
//! Per spec §5.3: Failed work items are moved to a dead letter queue
//! after exhausting all retries.

use chrono::Utc;

use elephant_storage::work_deadletter_repository::WorkDeadletterRepository;
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::work_run_repository::WorkRunRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::work::{
    RetryPolicy, WorkDeadletter, WorkItem, WorkItemStatus, WorkRun,
};
use elephant_types::{Link, RecordId, TableName};

use crate::error::PipelineError;

/// Handler for dead letter operations.
#[derive(Debug, Clone)]
pub struct DeadletterHandler {
    item_repo: WorkItemRepository,
    run_repo: WorkRunRepository,
    deadletter_repo: WorkDeadletterRepository,
    storage: StorageClient,
}

impl DeadletterHandler {
    /// Creates a new deadletter handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            item_repo: WorkItemRepository::new(storage.clone()),
            run_repo: WorkRunRepository::new(storage.clone()),
            deadletter_repo: WorkDeadletterRepository::new(storage.clone()),
            storage,
        }
    }

    /// Checks if an item should be deadlettered based on retry policy.
    #[must_use]
    pub fn should_deadletter(item: &WorkItem, policy: &RetryPolicy) -> bool {
        item.attempts >= policy.max_attempts
    }

    /// Finishes the original claimed attempt before recording its deadletter.
    /// Never reloads a successor's token. If recording fails after completion,
    /// `deadletter` can replay the already-failed outcome without a new lease.
    ///
    /// # Errors
    /// Rejects stale/expired claims and propagates accounting or storage failures.
    pub async fn deadletter_claim(
        &self,
        original: &WorkItem,
        error: &str,
    ) -> Result<WorkDeadletter, PipelineError> {
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(original)?;
        let queue = super::work_queue::WorkQueue::new(
            self.storage.clone(),
            super::work_queue::WorkQueueConfig {
                worker_id: fence.lease_token,
                ..Default::default()
            },
        );
        queue
            .finish_authorized(original, false, Some(error.to_owned()))
            .await?;
        self.deadletter(&original.base.space_id, &original.base.id, error)
            .await
    }

    /// Moves a failed work item to the dead letter queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn deadletter(
        &self,
        space_id: &str,
        item_id: &RecordId,
        error: &str,
    ) -> Result<WorkDeadletter, PipelineError> {
        let item = self
            .item_repo
            .get(space_id, item_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(item_id.to_string()))?;

        if item.status != WorkItemStatus::Failed {
            return Err(PipelineError::InvalidState(
                "Deadletter requires an acknowledged failed attempt; use the original claim to finish Running work".into(),
            ));
        }
        // Stable per-failed-outcome identity makes recovery after a successful
        // queue acknowledgement idempotent, including after archival.
        let identity = format!(
            "{}:{}:{}",
            item.base.id, item.attempts, item.base.updated_at
        );
        let id = RecordId::new(
            TableName::WorkDeadletter,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()),
        );
        if let Some(existing) = self.deadletter_repo.get(space_id, &id).await? {
            return Ok(existing);
        }

        // Get all runs for this item
        let runs = self
            .run_repo
            .find_by_work_item(item_id)
            .await
            .map_err(PipelineError::StorageError)?;

        let run_links: Vec<Link<WorkRun>> =
            runs.iter().map(|r| Link::new(r.base.id.clone())).collect();

        // Create deadletter entry
        let mut base = item.base.clone();
        base.id = id;
        base.created_at = Utc::now();
        base.updated_at = base.created_at;
        let deadletter = WorkDeadletter {
            base,
            work_item: Link::new(item_id.clone()),
            runs: run_links,
            final_error: error.to_string(),
            deadlettered_at: Utc::now(),
            acknowledged: false,
            notes: None,
        };

        let created = self
            .deadletter_repo
            .create(deadletter)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(created)
    }

    /// Lists unacknowledged dead letter entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_unacknowledged(
        &self,
        space_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkDeadletter>, PipelineError> {
        self.deadletter_repo
            .find_unacknowledged(space_id, limit)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Acknowledges a dead letter entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn acknowledge(
        &self,
        space_id: &str,
        deadletter_id: &RecordId,
        notes: Option<String>,
    ) -> Result<WorkDeadletter, PipelineError> {
        let entry = self
            .deadletter_repo
            .get(space_id, deadletter_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(deadletter_id.to_string()))?;

        let mut acked = entry;
        acked.acknowledged = true;
        acked.notes = notes;

        self.deadletter_repo
            .update(space_id, deadletter_id, acked)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Retries a deadlettered item by resetting its status.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn retry(
        &self,
        space_id: &str,
        deadletter_id: &RecordId,
    ) -> Result<WorkItem, PipelineError> {
        let entry = self
            .deadletter_repo
            .get(space_id, deadletter_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(deadletter_id.to_string()))?;

        let item = self
            .item_repo
            .get(space_id, entry.work_item.id())
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(entry.work_item.id().to_string()))?;

        // Preserve retry/failure history and the monotonic attempt generation.
        // The failed pre-state and hot/archive move are checked atomically.
        let updated = self.item_repo.retry_authorized(&item).await?;

        // Delete deadletter entry
        self.deadletter_repo
            .delete(space_id, deadletter_id)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::records::base::BaseFields;

    const TEST_SPACE: &str = "default";

    fn make_work_item(space_id: &str, attempts: u32) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                "test".to_string(),
            ),
            kind: "test_kind".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("idem-{}", uuid::Uuid::new_v4()),
            attempts,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn test_deadletter_after_max_retries() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = DeadletterHandler::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item = make_work_item(TEST_SPACE, 3);
        let created = item_repo.create(item).await.expect("create");
        let created = item_repo
            .claim_with_lease(
                TEST_SPACE,
                &created.base.id,
                "original-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("actual original claim");

        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };

        assert!(DeadletterHandler::should_deadletter(&created, &policy));

        let deadletter = handler
            .deadletter_claim(&created, "Max retries exceeded")
            .await
            .expect("deadletter");

        assert_eq!(deadletter.final_error, "Max retries exceeded");
        assert!(!deadletter.acknowledged);

        // Check item is now failed
        let updated = item_repo
            .get(TEST_SPACE, &created.base.id)
            .await
            .expect("get")
            .unwrap();
        assert_eq!(updated.status, WorkItemStatus::Failed);
    }

    #[tokio::test]
    async fn test_acknowledge_deadletter() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = DeadletterHandler::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item = make_work_item(TEST_SPACE, 3);
        let created = item_repo.create(item).await.expect("create");
        let created = item_repo
            .claim_with_lease(
                TEST_SPACE,
                &created.base.id,
                "original-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("actual original claim");

        let deadletter = handler
            .deadletter_claim(&created, "Error")
            .await
            .expect("deadletter");

        let acked = handler
            .acknowledge(
                TEST_SPACE,
                &deadletter.base.id,
                Some("Reviewed, will fix later".to_string()),
            )
            .await
            .expect("ack");

        assert!(acked.acknowledged);
        assert_eq!(acked.notes, Some("Reviewed, will fix later".to_string()));
    }

    #[tokio::test]
    async fn test_retry_deadlettered_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = DeadletterHandler::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item = make_work_item(TEST_SPACE, 3);
        let created = item_repo.create(item).await.expect("create");
        let created = item_repo
            .claim_with_lease(
                TEST_SPACE,
                &created.base.id,
                "original-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("actual original claim");

        let deadletter = handler
            .deadletter_claim(&created, "Error")
            .await
            .expect("deadletter");

        let retried = handler
            .retry(TEST_SPACE, &deadletter.base.id)
            .await
            .expect("retry");

        assert_eq!(retried.status, WorkItemStatus::Runnable);
        assert_eq!(retried.attempts, created.attempts);
        assert_eq!(retried.last_error.as_deref(), Some("Error"));
    }

    #[tokio::test]
    async fn stale_deadletter_claim_cannot_fail_successor_or_create_a_letter() {
        for storage in [
            StorageClient::new_embedded().await.expect("embedded"),
            StorageClient::new_memory().await.expect("native"),
        ] {
            let handler = DeadletterHandler::new(storage.clone());
            let repo = WorkItemRepository::new(storage.clone());
            let work = repo
                .create(make_work_item(TEST_SPACE, 3))
                .await
                .expect("create");
            let original = repo
                .claim_with_lease(
                    TEST_SPACE,
                    &work.base.id,
                    "same-worker-label",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("claim")
                .expect("original");
            assert!(handler
                .deadletter(TEST_SPACE, &work.base.id, "no original claim supplied")
                .await
                .is_err());
            let fence = elephant_storage::work_attempt_repository::fence_from_claim(&original)
                .expect("fence");
            repo.finish_authorized(
                &original,
                &fence.lease_token,
                WorkItemStatus::Failed,
                Some("original failure".into()),
            )
            .await
            .expect("fail")
            .expect("acknowledged");
            let first = handler
                .deadletter(TEST_SPACE, &work.base.id, "original failure")
                .await
                .expect("record");
            assert_eq!(
                handler
                    .deadletter(TEST_SPACE, &work.base.id, "original failure")
                    .await
                    .expect("replay")
                    .base
                    .id,
                first.base.id
            );
            handler
                .retry(TEST_SPACE, &first.base.id)
                .await
                .expect("retry");
            let successor = repo
                .claim_with_lease(
                    TEST_SPACE,
                    &work.base.id,
                    "same-worker-label",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("claim")
                .expect("successor");
            assert!(handler
                .deadletter_claim(&original, "late worker failure")
                .await
                .is_err());
            assert_eq!(
                repo.get(TEST_SPACE, &work.base.id).await.expect("read"),
                Some(successor)
            );
            assert!(handler
                .list_unacknowledged(TEST_SPACE, 10)
                .await
                .expect("letters")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn test_should_not_deadletter_under_max() {
        let item = make_work_item(TEST_SPACE, 2);
        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };

        assert!(!DeadletterHandler::should_deadletter(&item, &policy));
    }
}
