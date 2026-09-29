//! Test driver around the production source-first work-kind dispatcher.

use chrono::Utc;
use elephant_pipeline::extraction::Extractor;
use elephant_pipeline::scheduler::DagExecutor;
use elephant_storage::{Repository, StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{WorkItemStatus, WorkRun, WorkRunMetrics};
use elephant_types::{Link, RecordId};

/// Runs persisted work through the actual dispatcher and fails on incomplete work.
pub async fn drive_pipeline(
    storage: &StorageClient,
    space_id: &str,
    extractor: &dyn Extractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    let items = WorkItemRepository::new(storage.clone());
    let runs = WorkRunRepository::new(storage.clone());
    let dag = DagExecutor::new(storage.clone());
    for _ in 0..max_steps {
        let runnable = items
            .find_by_status(space_id, WorkItemStatus::Runnable)
            .await?;
        if runnable.is_empty() {
            for status in [
                WorkItemStatus::Pending,
                WorkItemStatus::Running,
                WorkItemStatus::Failed,
            ] {
                let unfinished = items.find_by_status(space_id, status).await?;
                anyhow::ensure!(
                    unfinished.is_empty(),
                    "pipeline stopped with {status} work: {unfinished:?}"
                );
            }
            return Ok(());
        }

        for item in runnable {
            let item = items
                .claim_with_lease(
                    space_id,
                    &item.base.id,
                    "e2e-driver",
                    Utc::now() + chrono::Duration::minutes(20),
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("work item was not claimable: {}", item.base.id))?;
            let run_id = RecordId::new_v4(TableName::WorkRun);
            let mut run = runs
                .create(WorkRun {
                    base: BaseFields::new(
                        run_id.clone(),
                        space_id.to_string(),
                        "e2e-driver".to_string(),
                    ),
                    work_item: Link::new(item.base.id.clone()),
                    attempt: item.attempts,
                    started_at: Utc::now(),
                    completed_at: None,
                    success: false,
                    error: None,
                    metrics: WorkRunMetrics::default(),
                    worker_id: "e2e-driver".to_string(),
                    worker_version: "test".to_string(),
                    model_version: None,
                    prompt_id: None,
                    config_hash: None,
                })
                .await?;
            let result = elephant_pipeline_bin::runtime::execute_work_item_for_test(
                &item,
                &run_id,
                storage.clone(),
                extractor,
            )
            .await;
            run.completed_at = Some(Utc::now());
            match result {
                Ok(metrics) => {
                    run.metrics = metrics;
                    run.success = true;
                }
                Err(error) => {
                    run.error = Some(error.to_string());
                    runs.update(space_id, &run_id, run).await?;
                    items
                        .update_running_for_worker(
                            space_id,
                            &item.base.id,
                            "e2e-driver",
                            WorkItemStatus::Failed,
                            Some(error.to_string()),
                        )
                        .await?;
                    anyhow::bail!("{} work item {} failed: {error:#}", item.kind, item.base.id);
                }
            }
            runs.update(space_id, &run_id, run).await?;
            anyhow::ensure!(
                items
                    .update_running_for_worker(
                        space_id,
                        &item.base.id,
                        "e2e-driver",
                        WorkItemStatus::Done,
                        None
                    )
                    .await?
                    .is_some(),
                "work item ownership changed during execution: {}",
                item.base.id
            );
            dag.on_item_completed(space_id, &item.base.id).await?;
        }
    }
    anyhow::bail!("pipeline did not quiesce within {max_steps} passes")
}
