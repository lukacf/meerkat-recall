//! Failures before a work run begins retain the same typed finite retry policy.
use super::{classify_failure, retry_action, RetryAction};
use crate::PipelineResult;
use elephant_storage::{StorageClient, WorkItemRepository};
use elephant_types::records::work::{WorkItem, WorkItemStatus};

/// Persist a scheduling failure against the original claim, without inventing
/// execution or provider usage. Only prerequisite blockage refunds an attempt.
///
/// # Errors
/// Rejects an invalid original claim and propagates transition failures. A lost
/// lease returns None and cannot change the successor's work.
pub async fn record_preflight_failure(
    storage: &StorageClient,
    item: &WorkItem,
    error: &anyhow::Error,
    max_attempts: u32,
    base_delay: i64,
    max_delay: i64,
) -> PipelineResult<Option<WorkItem>> {
    let class = classify_failure(error);
    let action = retry_action(class, item.attempts, max_attempts, base_delay, max_delay);
    record_preflight_disposition(storage, item, error, class, action, base_delay).await
}

/// Apply a previously selected disposition after its original source receipt
/// has been persisted. This creates no execution or provider usage record.
///
/// # Errors
/// Rejects an invalid original claim or a failed durable queue transition.
pub async fn record_preflight_disposition(
    storage: &StorageClient,
    item: &WorkItem,
    error: &anyhow::Error,
    class: super::FailureClass,
    action: RetryAction,
    base_delay: i64,
) -> PipelineResult<Option<WorkItem>> {
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
    let repository = WorkItemRepository::new(storage.clone());
    let result = match action {
        RetryAction::Blocked { delay_secs } => {
            if class == super::FailureClass::PendingDependencies {
                repository
                    .requeue_pending_dependencies(
                        &fence.space_id,
                        &fence.work_item_id,
                        &fence.lease_token,
                        delay_secs,
                        format!("{class:?}: {error}"),
                    )
                    .await?
            } else {
                repository
                    .requeue_configuration_blocked(
                        &fence.space_id,
                        &fence.work_item_id,
                        &fence.lease_token,
                        delay_secs,
                        format!("{class:?}: {error}"),
                    )
                    .await?
            }
        }
        RetryAction::Retry { delay_secs } => {
            repository
                .requeue_with_delay(
                    &fence.space_id,
                    &fence.work_item_id,
                    &fence.lease_token,
                    delay_secs,
                    Some(format!("{class:?}: {error}")),
                )
                .await?
        }
        RetryAction::Fail => {
            repository
                .update_running_for_worker(
                    &fence.space_id,
                    &fence.work_item_id,
                    &fence.lease_token,
                    WorkItemStatus::Failed,
                    Some(format!("{class:?}: {error}")),
                )
                .await?
        }
        RetryAction::Abandon => None,
        // No execution has begun, so a new bounded original claim can re-read
        // prerequisites. This path grants no source-repair/publication authority.
        RetryAction::RepairInput => {
            repository
                .requeue_with_delay(
                    &fence.space_id,
                    &fence.work_item_id,
                    &fence.lease_token,
                    base_delay.max(1),
                    Some(format!("{class:?}: {error}")),
                )
                .await?
        }
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::too_many_lines)]
    use super::*;
    use crate::PipelineError;
    use elephant_storage::{Repository, StorageError};
    use elephant_types::{records::base::BaseFields, RecordId, TableName};

    async fn claim(repository: &WorkItemRepository, mut item: WorkItem) -> WorkItem {
        item.run_after = None;
        let id = item.base.id.clone();
        repository.update("quiet", &id, item).await.unwrap();
        repository
            .claim_with_lease(
                "quiet",
                &id,
                "fixture",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap()
    }

    async fn contract(storage: StorageClient, failure: StorageError) {
        let repository = WorkItemRepository::new(storage.clone());
        let item = repository
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "quiet".into(),
                    "fixture".into(),
                ),
                kind: "preflight-fixture".into(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key: "preflight-fixture".into(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            })
            .await
            .unwrap();
        let original = claim(&repository, item).await;
        let unavailable: anyhow::Error = PipelineError::StorageError(failure).into();
        let retry = record_preflight_failure(&storage, &original, &unavailable, 2, 1, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.status, WorkItemStatus::Runnable);
        assert_eq!(retry.attempts, 1, "outage consumes an execution retry");
        assert!(retry.run_after.is_some());
        let successor = claim(&repository, retry).await;
        assert_eq!(successor.attempts, 2);
        assert!(
            record_preflight_failure(&storage, &original, &unavailable, 2, 1, 60)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            repository
                .get("quiet", &successor.base.id)
                .await
                .unwrap()
                .unwrap()
                .leased_by,
            successor.leased_by
        );
        let exhausted = record_preflight_failure(&storage, &successor, &unavailable, 2, 1, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exhausted.status, WorkItemStatus::Failed);
        assert_eq!(exhausted.attempts, 2);

        let mut next = exhausted;
        next.base.id = RecordId::new_v4(TableName::WorkItem);
        next.status = WorkItemStatus::Runnable;
        next.idempotency_key = "blocked-fixture".into();
        next.attempts = 0;
        next.payload = serde_json::json!({});
        let next = repository.create(next).await.unwrap();
        let blocked_claim = claim(&repository, next).await;
        let denied: anyhow::Error =
            PipelineError::BudgetExceeded("current daily cap".into()).into();
        let blocked = record_preflight_failure(&storage, &blocked_claim, &denied, 1, 1, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blocked.status, WorkItemStatus::Runnable);
        assert_eq!(
            blocked.attempts, 0,
            "real prerequisite refusal retains recovery opportunity"
        );
        let repaired = claim(&repository, blocked).await;
        let corrupt: anyhow::Error =
            PipelineError::StorageError(StorageError::DataIntegrityError("invalid ledger".into()))
                .into();
        let failed = record_preflight_failure(&storage, &repaired, &corrupt, 9, 1, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, WorkItemStatus::Failed);
        assert_eq!(
            failed.attempts, 1,
            "integrity failure does not poll forever"
        );
        assert!(
            elephant_storage::WorkRunRepository::new(storage)
                .find_by_work_item(&failed.base.id)
                .await
                .unwrap()
                .is_empty(),
            "preflight failure is not fabricated work execution"
        );
    }

    #[tokio::test]
    async fn embedded_preflight_failures_are_typed_bounded_and_fenced() {
        contract(
            StorageClient::new_embedded().await.unwrap(),
            StorageError::ConnectionError("offline".into()),
        )
        .await;
    }
    #[tokio::test]
    async fn surreal_preflight_failures_are_typed_bounded_and_fenced() {
        contract(
            StorageClient::new_memory().await.unwrap(),
            StorageError::ConnectionError("offline".into()),
        )
        .await;
    }
    #[tokio::test]
    async fn embedded_typed_erasure_refusal_uses_bounded_original_claim_retry() {
        contract(
            StorageClient::new_embedded().await.unwrap(),
            StorageError::ErasureInProgress,
        )
        .await;
    }
    #[tokio::test]
    async fn surreal_typed_erasure_refusal_uses_bounded_original_claim_retry() {
        contract(
            StorageClient::new_memory().await.unwrap(),
            StorageError::ErasureInProgress,
        )
        .await;
    }
}
