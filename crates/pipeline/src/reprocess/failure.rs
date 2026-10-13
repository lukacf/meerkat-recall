//! Reconcile a final executed work failure with its public reprocess intent.
//! Queue ownership is closed first by the existing fenced transition. This path
//! grants no lease or graph authority: its control-only commit compares the exact
//! terminal work, original failed run and job, including retained archive rows.

use elephant_storage::canonical_commit::{CanonicalCommit, CanonicalRepository};
use elephant_storage::reprocess_job_repository::verify_reprocess_handoff;
use elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY;
use elephant_storage::{
    Repository, ReprocessJobRepository, StorageClient, WorkItemRepository, WorkRunRepository,
};
use elephant_types::publication::AttemptFence;
use elephant_types::records::reprocess::ReprocessStatus;
use elephant_types::records::{WorkItem, WorkItemStatus, WorkRun};
use elephant_types::RecordId;

use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
use crate::{PipelineError, PipelineResult};

const FAILURE: &str = "reprocess_work_failed";

fn invalid() -> PipelineError {
    PipelineError::InvalidState(
        "Reprocess terminal attempt evidence is unavailable or changed".into(),
    )
}

fn terminal_fence(item: &WorkItem) -> PipelineResult<AttemptFence> {
    let fence: AttemptFence = serde_json::from_value(
        item.payload
            .get(ATTEMPT_PAYLOAD_KEY)
            .cloned()
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    if fence.space_id != item.base.space_id
        || fence.work_item_id != item.base.id
        || fence.generation == 0
        || fence.lease_token.is_empty()
        || item.base.deleted_at.is_some()
        || item.status != WorkItemStatus::Failed
        || item.attempts == 0
        || item.leased_by.is_some()
        || item.lease_expires_at.is_some()
    {
        return Err(invalid());
    }
    Ok(fence)
}

fn failed_run(
    item: &WorkItem,
    fence: &AttemptFence,
    runs: Vec<WorkRun>,
) -> PipelineResult<WorkRun> {
    let value = serde_json::to_value(fence).map_err(|_| invalid())?;
    let mut matching = runs
        .into_iter()
        .filter(|run| run.metrics.custom.get("attempt_fence") == Some(&value));
    let run = matching.next().ok_or_else(invalid)?;
    if matching.next().is_some()
        || run.base.space_id != item.base.space_id
        || run.base.deleted_at.is_some()
        || run.work_item.id() != &item.base.id
        || run.attempt != item.attempts
        || run.success
        || run.error.is_none()
        || run
            .completed_at
            .is_none_or(|end| end < run.started_at || end > item.base.updated_at)
    {
        return Err(invalid());
    }
    Ok(run)
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Keep the original job/work/run snapshots and repository owners together through control preparation."
)]
async fn prepare_failure(
    storage: &StorageClient,
    space: &str,
    work_id: &RecordId,
    original: Option<&AttemptFence>,
) -> PipelineResult<Option<CanonicalCommit>> {
    // Capture before reading any evidence; a concurrent mutation must fail the
    // existing canonical epoch/read-set comparison rather than adopt new input.
    let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space,
        None,
    ))
    .await?;
    let item = WorkItemRepository::new(storage.clone())
        .get(space, work_id)
        .await?
        .ok_or_else(invalid)?;
    if item.kind != "execute_reprocess_job" || item.status != WorkItemStatus::Failed {
        return Ok(None);
    }
    let job_id: RecordId = item
        .payload
        .get("job_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let jobs = ReprocessJobRepository::new(storage.clone());
    let job = jobs.get(space, &job_id).await?.ok_or_else(invalid)?;
    if !matches!(
        job.status,
        ReprocessStatus::Pending | ReprocessStatus::Running
    ) {
        return Ok(None);
    }
    // Existing atomic handoff validation checks the original private producer,
    // full authorization envelope and actual retained (possibly random) work ID.
    // A matching terminal handoff is observed, never reset or re-enqueued.
    if Box::pin(jobs.ensure_work(&job)).await? != item.base.id {
        return Err(invalid());
    }
    verify_reprocess_handoff(&job, &item)?;
    let fence = terminal_fence(&item)?;
    if original.is_some_and(|original| original != &fence) {
        return Err(invalid());
    }
    let runs = WorkRunRepository::new(storage.clone())
        .find_by_work_item(work_id)
        .await?;
    let run = failed_run(&item, &fence, runs)?;
    planning.observe(&item)?;
    planning.observe(&run)?;
    let mut after = job.clone();
    after.status = ReprocessStatus::Failed;
    after.started_at = after.started_at.or(Some(run.started_at));
    after.completed_at = Some(item.base.updated_at);
    // Do not copy arbitrary provider/error text into public job diagnostics.
    // Keep existing progress: a failed job does not establish source totals.
    after.error = Some(FAILURE.into());
    after.base.updated_at = item.base.updated_at;
    planning.stage(Some(job.clone()), after)?;
    Box::pin(planning.audit_pending_effects(FAILURE)).await?;
    let scope = format!("{}:{}:{}", job.base.id, item.base.id, fence.generation);
    let input = serde_json::json!({
        "job": job.base.id, "work_item": item.base.id,
        "generation": fence.generation, "work_run": run.base.id,
    });
    Box::pin(planning.prepare_reprocess_control_request(&job, FAILURE, &scope, &input, &true))
        .await
        .map(Some)
}

/// Publish a final executed reprocess failure, or leave a nonfailed/finished job
/// unchanged. Call after the original queue failure, and during handoff recovery.
/// The immediate caller passes its original claim; recovery passes `None` and
/// proves the same closed attempt from retained work/run evidence. Neither path
/// grants a new attempt or reclassifies a retry as terminal.
///
/// # Errors
/// Rejects missing, mismatched or changed evidence and propagates canonical
/// conflicts. Recovery may retry this bookkeeping after a crash or conflict.
pub async fn reconcile_failed_work(
    storage: &StorageClient,
    space: &str,
    work_id: &RecordId,
    original: Option<&AttemptFence>,
) -> PipelineResult<bool> {
    let Some(request) = Box::pin(prepare_failure(storage, space, work_id, original)).await? else {
        return Ok(false);
    };
    Box::pin(CanonicalRepository::new(storage.clone()).commit(request)).await?;
    Ok(true)
}

#[cfg(test)]
#[path = "failure_tests.rs"]
mod tests;
