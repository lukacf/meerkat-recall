//! Dead letter handling for failed work items.
//!
//! Per spec §5.3: Failed work items are moved to a dead letter queue
//! after exhausting all retries.

use chrono::Utc;

use elephant_storage::work_deadletter_repository::WorkDeadletterRepository;
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::work_run_repository::WorkRunRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::base::BaseFields;
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
}

impl DeadletterHandler {
    /// Creates a new deadletter handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            item_repo: WorkItemRepository::new(storage.clone()),
            run_repo: WorkRunRepository::new(storage.clone()),
            deadletter_repo: WorkDeadletterRepository::new(storage),
        }
    }

    /// Checks if an item should be deadlettered based on retry policy.
    #[must_use]
    pub fn should_deadletter(item: &WorkItem, policy: &RetryPolicy) -> bool {
        item.attempts >= policy.max_attempts
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

        // Get all runs for this item
        let runs = self
            .run_repo
            .find_by_work_item(item_id)
            .await
            .map_err(PipelineError::StorageError)?;

        let run_links: Vec<Link<WorkRun>> =
            runs.iter().map(|r| Link::new(r.base.id.clone())).collect();

        // Create deadletter entry
        let deadletter = WorkDeadletter {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkDeadletter),
                item.base.space_id.clone(),
                "deadletter_handler".to_string(),
            ),
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

        // Mark item as failed
        let mut failed = item;
        failed.status = WorkItemStatus::Failed;
        failed.last_error = Some(error.to_string());
        self.item_repo
            .update(space_id, item_id, failed)
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

        // Reset item for retry
        let mut reset = item;
        reset.status = WorkItemStatus::Runnable;
        reset.attempts = 0;
        reset.last_error = None;
        reset.leased_by = None;
        reset.lease_expires_at = None;
        reset.run_after = None;

        let updated = self
            .item_repo
            .update(space_id, entry.work_item.id(), reset)
            .await
            .map_err(PipelineError::StorageError)?;

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
            status: WorkItemStatus::Running,
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

        let policy = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };

        assert!(DeadletterHandler::should_deadletter(&created, &policy));

        let deadletter = handler
            .deadletter(TEST_SPACE, &created.base.id, "Max retries exceeded")
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

        let deadletter = handler
            .deadletter(TEST_SPACE, &created.base.id, "Error")
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

        let deadletter = handler
            .deadletter(TEST_SPACE, &created.base.id, "Error")
            .await
            .expect("deadletter");

        let retried = handler
            .retry(TEST_SPACE, &deadletter.base.id)
            .await
            .expect("retry");

        assert_eq!(retried.status, WorkItemStatus::Runnable);
        assert_eq!(retried.attempts, 0);
        assert!(retried.last_error.is_none());
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
