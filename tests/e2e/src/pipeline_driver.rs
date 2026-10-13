//! Test driver around the production source-first work-kind dispatcher.

use std::time::Duration;

use chrono::Utc;
use elephant_pipeline::extraction::Extractor;
use elephant_pipeline::scheduler::execution_contract::{
    classify_failure, retry_action, FailureClass, RetryAction,
};
use elephant_pipeline::scheduler::DagExecutor;
use elephant_storage::{Repository, StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::ids::TableName;
use elephant_types::records::work::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
use elephant_types::{Link, RecordId};

const PIPELINE_DEADLINE: Duration = Duration::from_secs(300);
const INVENTORY_LIMIT: usize = 10_000;

/// Runs persisted work through the actual dispatcher and fails on incomplete work.
/// Dependency retries use the persisted production delay and refund; no fixture
/// advances clocks, stamps readiness or treats a failed attempt as completion.
pub async fn drive_pipeline(
    storage: &StorageClient,
    space_id: &str,
    extractor: &dyn Extractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        PIPELINE_DEADLINE,
        Box::pin(drive_until_complete(
            storage, space_id, extractor, max_steps,
        )),
    )
    .await
    .map_err(|_| anyhow::anyhow!("pipeline exceeded its 300-second elapsed deadline"))?
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn drive_until_complete(
    storage: &StorageClient,
    space_id: &str,
    extractor: &dyn Extractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    let items = WorkItemRepository::new(storage.clone());
    for _ in 0..max_steps {
        // This repository query returns only work whose real run_after is due.
        let runnable = items
            .find_by_status(space_id, WorkItemStatus::Runnable)
            .await?;
        if !runnable.is_empty() {
            for item in runnable {
                Box::pin(drive_attempt(storage, space_id, extractor, &item)).await?;
            }
            continue;
        }

        // The due-only query cannot distinguish completion from delayed work.
        // Read a bounded complete inventory before declaring either condition.
        let inventory = items.list(space_id, INVENTORY_LIMIT + 1, None).await?;
        anyhow::ensure!(
            inventory.len() <= INVENTORY_LIMIT,
            "pipeline fixture inventory exceeds {INVENTORY_LIMIT} items"
        );
        match next_wait(&inventory, Utc::now())? {
            Some(delay) => tokio::time::sleep(delay).await,
            None => return Ok(()),
        }
    }
    anyhow::bail!("pipeline did not quiesce within {max_steps} passes")
}

fn next_wait(
    inventory: &[WorkItem],
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<Option<Duration>> {
    for status in [WorkItemStatus::Running, WorkItemStatus::Failed] {
        let unfinished: Vec<_> = inventory
            .iter()
            .filter(|item| item.status == status)
            .collect();
        anyhow::ensure!(
            unfinished.is_empty(),
            "pipeline stopped with {status} work: {unfinished:?}"
        );
    }
    if let Some(next) = inventory
        .iter()
        .filter(|item| item.status == WorkItemStatus::Runnable)
        .map(|item| item.run_after.unwrap_or(now))
        .min()
    {
        // A row can become due between the due-only query and this inventory.
        return Ok(Some((next - now).to_std().unwrap_or(Duration::ZERO)));
    }
    let pending: Vec<_> = inventory
        .iter()
        .filter(|item| item.status == WorkItemStatus::Pending)
        .collect();
    anyhow::ensure!(
        pending.is_empty(),
        "pipeline stuck without a runnable predecessor: {pending:?}"
    );
    Ok(None)
}

/// Execute one persisted runnable stage under a genuine claimed attempt. This
/// permits scenario assertions between stages without synthesizing authority.
pub async fn drive_item(
    storage: &StorageClient,
    space_id: &str,
    extractor: &dyn Extractor,
    item: &elephant_types::records::work::WorkItem,
) -> anyhow::Result<RecordId> {
    let (run_id, completed) = Box::pin(drive_attempt(storage, space_id, extractor, item)).await?;
    anyhow::ensure!(completed, "stage attempt {run_id} awaits dependencies; use drive_pipeline to wait for its persisted retry");
    Ok(run_id)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn drive_attempt(
    storage: &StorageClient,
    space_id: &str,
    extractor: &dyn Extractor,
    item: &WorkItem,
) -> anyhow::Result<(RecordId, bool)> {
    anyhow::ensure!(item.base.space_id == space_id, "cross-space fixture work");
    let items = WorkItemRepository::new(storage.clone());
    let runs = WorkRunRepository::new(storage.clone());
    let dag = DagExecutor::new(storage.clone());
    let item = items
        .claim_with_lease(
            space_id,
            &item.base.id,
            "e2e-driver",
            Utc::now() + chrono::Duration::minutes(20),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("work item was not claimable: {}", item.base.id))?;
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&item)?;
    anyhow::ensure!(
        attempt.space_id == space_id && attempt.work_item_id == item.base.id,
        "claim returned authority for different work"
    );
    let lease_owner = attempt.lease_token.clone();
    let fence = serde_json::to_value(&attempt)?;
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let mut base = item.base.clone();
    base.id = run_id.clone();
    base.created_by = "e2e-driver".into();
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    let mut initial_metrics = WorkRunMetrics::default();
    initial_metrics
        .custom
        .insert("attempt_fence".into(), fence.clone());
    let mut run = runs
        .create(WorkRun {
            base,
            work_item: Link::new(item.base.id.clone()),
            attempt: item.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: initial_metrics,
            worker_id: lease_owner.clone(),
            worker_version: "test".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await?;
    let result = Box::pin(elephant_pipeline_bin::runtime::execute_work_item_for_test(
        &item,
        &run_id,
        storage.clone(),
        extractor,
    ))
    .await;
    run.completed_at = Some(Utc::now());
    match result {
        Ok(mut metrics) => {
            metrics.custom.insert("attempt_fence".into(), fence);
            run.metrics = metrics;
            run.success = true;
        }
        Err(error) => {
            return Box::pin(persist_failure(storage, &item, run, &lease_owner, error)).await;
        }
    }
    runs.update(space_id, &run_id, run).await?;
    anyhow::ensure!(
        items
            .update_running_for_worker(
                space_id,
                &item.base.id,
                &lease_owner,
                WorkItemStatus::Done,
                None
            )
            .await?
            .is_some(),
        "work item ownership changed during execution: {}",
        item.base.id
    );
    dag.on_item_completed(space_id, &item.base.id).await?;
    Ok((run_id, true))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn persist_failure(
    storage: &StorageClient,
    item: &WorkItem,
    mut run: WorkRun,
    lease_owner: &str,
    error: anyhow::Error,
) -> anyhow::Result<(RecordId, bool)> {
    let class = classify_failure(&error);
    let run_id = run.base.id.clone();
    run.completed_at = Some(Utc::now());
    run.success = false;
    run.error = Some(error.to_string());
    run.metrics
        .custom
        .insert("failure_class".into(), serde_json::to_value(class)?);
    WorkRunRepository::new(storage.clone())
        .update(&item.base.space_id, &run_id, run)
        .await?;
    let items = WorkItemRepository::new(storage.clone());
    if class == FailureClass::PendingDependencies {
        // Production's blocked policy does not consume the finite transient
        // attempt allowance. Its minimum poll interval is thirty real seconds.
        let RetryAction::Blocked { delay_secs } = retry_action(class, item.attempts, 3, 1, 30)
        else {
            anyhow::bail!("dependency retry policy did not return Blocked");
        };
        anyhow::ensure!(
            items
                .requeue_configuration_blocked(
                    &item.base.space_id,
                    &item.base.id,
                    lease_owner,
                    delay_secs,
                    error.to_string(),
                )
                .await?
                .is_some(),
            "work item ownership changed before dependency requeue: {}",
            item.base.id
        );
        return Ok((run_id, false));
    }
    anyhow::ensure!(
        items
            .update_running_for_worker(
                &item.base.space_id,
                &item.base.id,
                lease_owner,
                WorkItemStatus::Failed,
                Some(error.to_string()),
            )
            .await?
            .is_some(),
        "work item ownership changed before failure recording: {}",
        item.base.id
    );
    anyhow::bail!(
        "{} work item {} failed ({class:?}): {error:#}",
        item.kind,
        item.base.id
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_pipeline::scheduler::execution_contract::WorkFailure;
    use elephant_types::records::base::BaseFields;

    fn original_item() -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "driver-contract".into(),
                "fixture".into(),
            ),
            kind: "driver-contract".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            attempts: 900,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    async fn test_storage() -> anyhow::Result<StorageClient> {
        let storage = StorageClient::new_memory().await?;
        elephant_migrations::apply_schema(&storage)
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(storage)
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle and fixture lifetimes intact."
    )]
    async fn claimed_run(storage: &StorageClient) -> anyhow::Result<(WorkItem, WorkRun, String)> {
        let items = WorkItemRepository::new(storage.clone());
        let item = items.create(original_item()).await?;
        let item = items
            .claim_with_lease(
                "driver-contract",
                &item.base.id,
                "original-driver",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("original claim failed"))?;
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(&item)?;
        let mut base = item.base.clone();
        base.id = RecordId::new_v4(TableName::WorkRun);
        let mut metrics = WorkRunMetrics::default();
        metrics
            .custom
            .insert("attempt_fence".into(), serde_json::to_value(&fence)?);
        let run = WorkRunRepository::new(storage.clone())
            .create(WorkRun {
                base,
                work_item: Link::new(item.base.id.clone()),
                attempt: item.attempts,
                started_at: Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics,
                worker_id: fence.lease_token.clone(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await?;
        Ok((item, run, fence.lease_token))
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle and fixture lifetimes intact."
    )]
    async fn dependency_failure_keeps_original_failed_run_and_refunds_only_retry_count(
    ) -> anyhow::Result<()> {
        let storage = test_storage().await?;
        let (item, run, token) = claimed_run(&storage).await?;
        let original_fence = run.metrics.custom["attempt_fence"].clone();
        let before = Utc::now();
        let (run_id, completed) = persist_failure(
            &storage,
            &item,
            run,
            &token,
            elephant_pipeline::PipelineError::InvalidState(
                "source_publication_incomplete: fixture dependency".into(),
            )
            .into(),
        )
        .await?;
        assert!(!completed);
        let failed_run = WorkRunRepository::new(storage.clone())
            .get("driver-contract", &run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed run lost"))?;
        assert!(!failed_run.success);
        assert!(failed_run.completed_at.is_some());
        assert!(failed_run.error.is_some());
        assert_eq!(failed_run.attempt, 901);
        assert_eq!(failed_run.worker_id, token);
        assert_eq!(failed_run.metrics.custom["attempt_fence"], original_fence);
        assert_eq!(
            failed_run.metrics.custom["failure_class"],
            "pending_dependencies"
        );
        let items = WorkItemRepository::new(storage.clone());
        let requeued = items
            .get("driver-contract", &item.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("requeued work lost"))?;
        assert_eq!(requeued.status, WorkItemStatus::Runnable);
        assert_eq!(requeued.attempts, 900);
        assert!(requeued.leased_by.is_none());
        assert!(requeued.lease_expires_at.is_none());
        assert!(requeued
            .run_after
            .is_some_and(|time| time >= before + chrono::Duration::seconds(30)));
        assert!(items
            .find_by_status("driver-contract", WorkItemStatus::Runnable)
            .await?
            .is_empty());
        assert!(items
            .claim_with_lease(
                "driver-contract",
                &item.base.id,
                "too-early",
                Utc::now() + chrono::Duration::minutes(5)
            )
            .await?
            .is_none());
        assert!(next_wait(&[requeued], Utc::now())?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn other_failure_classes_are_not_hidden_as_dependencies() -> anyhow::Result<()> {
        let storage = test_storage().await?;
        for class in [
            FailureClass::ConfigurationBlocked,
            FailureClass::Transient,
            FailureClass::Terminal,
        ] {
            let (item, run, token) = claimed_run(&storage).await?;
            let run_id = run.base.id.clone();
            assert!(persist_failure(
                &storage,
                &item,
                run,
                &token,
                WorkFailure {
                    class,
                    message: "source_publication_incomplete: incidental words".into(),
                }
                .into()
            )
            .await
            .is_err());
            let stored = WorkItemRepository::new(storage.clone())
                .get("driver-contract", &item.base.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("failed item lost"))?;
            assert_eq!(stored.status, WorkItemStatus::Failed);
            assert_eq!(stored.attempts, 901);
            let run = WorkRunRepository::new(storage.clone())
                .get("driver-contract", &run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("failed run lost"))?;
            assert!(!run.success);
            assert_eq!(run.worker_id, token);
            assert!(run.metrics.custom.contains_key("attempt_fence"));
        }
        Ok(())
    }

    #[test]
    fn delayed_work_is_not_quiescence_and_orphaned_dependencies_fail() -> anyhow::Result<()> {
        let now = Utc::now();
        let mut delayed = original_item();
        delayed.run_after = Some(now + chrono::Duration::seconds(30));
        let mut pending = original_item();
        pending.status = WorkItemStatus::Pending;
        assert_eq!(
            next_wait(&[pending.clone(), delayed], now)?,
            Some(Duration::from_secs(30))
        );
        assert!(next_wait(&[pending], now).is_err());
        assert_eq!(next_wait(&[], now)?, None);
        Ok(())
    }
}
