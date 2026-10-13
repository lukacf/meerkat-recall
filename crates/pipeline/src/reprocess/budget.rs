//! Explicit currency restrictions cannot be satisfied by abstract token units.
//! This preserves stop-after-recorded-spend semantics; it is not a quoted hard cap.
use super::*;
use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
use elephant_types::records::WorkItem;

fn missing_origin() -> PipelineError {
    PipelineError::ConfigError("Original provider producer authority is missing or changed".into())
}

/// One traversal context crosses source admissions and private work ancestry.
/// Completed branches may converge, but active-path cycles and the total amount
/// of verification remain bounded across all recursive calls.
#[derive(Clone, Copy)]
pub(super) enum ProducerNode {
    Work,
    Source,
}

#[derive(Default)]
pub(super) struct ProducerWalk {
    visited: std::collections::BTreeSet<String>,
    active: std::collections::BTreeSet<String>,
    complete: std::collections::BTreeSet<String>,
    work_nodes: usize,
    source_nodes: usize,
}

impl ProducerWalk {
    pub(super) fn enter(
        &mut self,
        kind: ProducerNode,
        key: String,
    ) -> PipelineResult<Option<String>> {
        if self.active.contains(&key) {
            return Err(PipelineError::ConfigError(
                "Private provider producer chain is cyclic".into(),
            ));
        }
        if self.complete.contains(&key) {
            return Ok(None);
        }
        if self.visited.insert(key.clone()) {
            match kind {
                ProducerNode::Work => self.work_nodes += 1,
                ProducerNode::Source => self.source_nodes += 1,
            }
        }
        // Source intermediates do not consume the established Work depth.
        // There is at most one operation per Work plus an initial Source seed.
        if self.work_nodes > 256 || self.source_nodes > 257 {
            return Err(PipelineError::ConfigError(
                "Private provider producer chain exceeds its original execution bound".into(),
            ));
        }
        self.active.insert(key.clone());
        Ok(Some(key))
    }

    pub(super) fn leave(&mut self, key: &str, successful: bool) {
        self.active.remove(key);
        if successful {
            self.complete.insert(key.into());
        }
    }
}

/// Check the private original producer chain before a physical provider call.
/// Recovery additionally verifies the original live attempt at this boundary.
/// The accounting walk and the shared standing-authority resolver both retain
/// original execution ancestry. Applicable standing ceilings authorize every
/// actual input before dispatch; a parent record's envelope grants no permission.
///
/// # Errors
/// Returns a currency refusal, missing/altered private linkage, or storage error.
pub async fn guard_physical_dispatch(
    storage: &StorageClient,
    original_work: &WorkItem,
) -> PipelineResult<()> {
    use elephant_storage::canonical_commit::CanonicalRepository;
    use elephant_storage::standing_dispatch_inputs::PreparedStandingDispatchInputs;
    use elephant_storage::standing_watch_repository::PreparedStandingSourceAuthority;

    let space = &original_work.base.space_id;
    let canonical = CanonicalRepository::new(storage.clone());
    let observed_epoch = canonical.observe_epoch(space).await?;
    let mut authority =
        PreparedStandingSourceAuthority::capture_for_work(storage, original_work).await?;
    let mut inputs = Vec::new();
    // Independently admitted execution is not restricted by a historical Watch
    // merely because its selected content once came from that Watch.
    if authority.principals().next().is_some() {
        let sources = authority.source_ids().cloned().collect::<Vec<_>>();
        for source in sources {
            let captured = PreparedStandingDispatchInputs::capture(storage, space, &source).await?;
            for record in captured.records() {
                authority.authorize_record(record.clone())?;
            }
            inputs.push(captured);
        }
    }
    guard_physical_dispatch_in_walk(storage, original_work, &mut ProducerWalk::default()).await?;
    // Revalidate the SAME original captures after all accounting and record
    // reads. No current claim, grant, source generation or epoch is adopted.
    for input in &inputs {
        input.verify_current(storage).await?;
    }
    authority.verify_current(storage).await?;
    verify_dispatch_epoch(&canonical, space, observed_epoch).await
}

/// This coarse predicate fence detects concurrent canonical writes, including
/// unrelated records. Refuse this dispatch and let the original work retry its
/// complete authority capture under the existing finite transient policy.
/// Never adopt a newer epoch inside the same dispatch or bypass an input guard.
async fn verify_dispatch_epoch(
    canonical: &elephant_storage::canonical_commit::CanonicalRepository,
    space: &str,
    observed_epoch: Option<String>,
) -> PipelineResult<()> {
    if canonical.observe_epoch(space).await? != observed_epoch {
        return Err(elephant_storage::StorageError::Conflict(
            "Provider dispatch inputs changed; reprepare the original work".into(),
        )
        .into());
    }
    Ok(())
}

pub(super) async fn guard_physical_dispatch_in_walk(
    storage: &StorageClient,
    original_work: &WorkItem,
    walk: &mut ProducerWalk,
) -> PipelineResult<()> {
    use elephant_storage::work_producer_repository::{
        verify_parent_origin, verify_reprocess_origin, WorkProducerOrigin, WorkProducerRepository,
    };
    let repo = elephant_storage::WorkItemRepository::new(storage.clone());
    let origins = WorkProducerRepository::new(storage.clone());
    let mut current = repo
        .get(&original_work.base.space_id, &original_work.base.id)
        .await?
        .ok_or_else(missing_origin)?;
    if &current != original_work {
        return Err(missing_origin());
    }
    let mut entered = Vec::new();
    let result = async {
        loop {
            let key = crate::maintenance_commit::request_fingerprint(&(
                "work",
                &current.base.space_id,
                &current.base.id,
            ))?;
            let Some(key) = walk.enter(ProducerNode::Work, key)? else {
                return Ok(());
            };
            entered.push(key);
            if let Some(publication) = crate::source_queue::publication_from_work(&current)? {
                source_job::guard_source_dispatch_in_walk(
                    storage,
                    &current.base.space_id,
                    &publication,
                    walk,
                )
                .await?;
            }
            let origin = origins.origin_for_work(&current).await?;
            match &origin {
                WorkProducerOrigin::Unknown => return Err(missing_origin()),
                WorkProducerOrigin::Direct => {
                    if current.kind == "execute_reprocess_job" {
                        return Err(missing_origin());
                    }
                    return Ok(());
                }
                WorkProducerOrigin::ReprocessJob {
                    job_id,
                    original_currency_restricted,
                    ..
                } => {
                    let job = ReprocessJobRepository::new(storage.clone())
                        .get(&current.base.space_id, job_id)
                        .await?
                        .ok_or_else(missing_origin)?;
                    verify_reprocess_origin(&origin, &job, &current)?;
                    if let Some(reason) =
                        refusal_with_origin(&job, true, *original_currency_restricted)
                    {
                        return Err(PipelineError::BudgetExceeded(reason.into()));
                    }
                    return guard_job_execution_parent_in_walk(storage, &job, walk).await;
                }
                WorkProducerOrigin::Child {
                    parent_work_item_id,
                    original_parent_work_run_id,
                    ..
                } => {
                    let parent = repo
                        .get(&current.base.space_id, parent_work_item_id)
                        .await?
                        .ok_or_else(missing_origin)?;
                    let run = elephant_storage::WorkRunRepository::new(storage.clone())
                        .get(&current.base.space_id, original_parent_work_run_id)
                        .await?
                        .ok_or_else(missing_origin)?;
                    verify_parent_origin(&origin, &parent, &run)?;
                    current = parent;
                }
            }
        }
    }
    .await;
    for key in entered.into_iter().rev() {
        walk.leave(&key, result.is_ok());
    }
    result
}

/// Follow the private ORIGINAL execution parent after checking the job's own
/// budget. Selecting old source bytes does not create this execution edge.
pub(super) async fn guard_job_execution_parent_in_walk(
    storage: &StorageClient,
    job: &ReprocessJob,
    walk: &mut ProducerWalk,
) -> PipelineResult<()> {
    use elephant_storage::work_producer_repository::{
        verify_reprocess_execution_parent, ReprocessExecutionOrigin, WorkProducerOrigin,
        WorkProducerRepository,
    };
    let origin = WorkProducerRepository::new(storage.clone())
        .origin_for_job(job)
        .await?;
    let WorkProducerOrigin::ReprocessJob {
        execution_origin, ..
    } = origin
    else {
        return Err(missing_origin());
    };
    match &execution_origin {
        ReprocessExecutionOrigin::Independent => Ok(()),
        ReprocessExecutionOrigin::Inherited {
            parent_work_item_id,
            original_parent_work_run_id,
            ..
        } => {
            let parent = elephant_storage::WorkItemRepository::new(storage.clone())
                .get(&job.base.space_id, parent_work_item_id)
                .await?
                .ok_or_else(missing_origin)?;
            let run = elephant_storage::WorkRunRepository::new(storage.clone())
                .get(&job.base.space_id, original_parent_work_run_id)
                .await?
                .ok_or_else(missing_origin)?;
            verify_reprocess_execution_parent(&execution_origin, &parent, &run)?;
            // Preserve the same cycle tracker and total bound across Job/Work/
            // Source edges, including completed historical producer runs.
            Box::pin(guard_physical_dispatch_in_walk(storage, &parent, walk)).await
        }
    }
}

/// Read the immutable restriction captured by the storage owner when this job
/// was created, including the crash gap before an actual queue row exists.
pub(super) async fn original_currency_restricted(
    storage: &StorageClient,
    job: &ReprocessJob,
) -> PipelineResult<bool> {
    use elephant_storage::work_producer_repository::{WorkProducerOrigin, WorkProducerRepository};
    match WorkProducerRepository::new(storage.clone())
        .origin_for_job(job)
        .await?
    {
        WorkProducerOrigin::ReprocessJob {
            job_id,
            original_currency_restricted,
            ..
        } if job_id == job.base.id => Ok(original_currency_restricted),
        _ => Err(missing_origin()),
    }
}

pub(super) async fn job_refusal(
    storage: &StorageClient,
    job: &ReprocessJob,
    may_dispatch: bool,
) -> PipelineResult<Option<&'static str>> {
    let original = original_currency_restricted(storage, job).await?;
    Ok(refusal_with_origin(job, may_dispatch, original))
}

fn refusal_with_origin(
    job: &ReprocessJob,
    may_dispatch: bool,
    original_restricted: bool,
) -> Option<&'static str> {
    if original_restricted && job.budget_limit_euros.is_none() {
        return Some(
            "Original finite currency restriction cannot be removed by changing the job budget",
        );
    }
    refusal(job, may_dispatch)
}

pub(super) const UNPRICED: &str =
    "Currency budget cannot be verified: provider dispatch has no authoritative EUR price";
pub(super) const UNKNOWN_PRIOR: &str =
    "Currency budget cannot be verified: prior monetary usage is unknown";

pub(super) fn validate_limit(limit: Option<f64>) -> PipelineResult<()> {
    if limit.is_some_and(|amount| !amount.is_finite() || amount < 0.0) {
        return Err(PipelineError::ValidationError(
            "budget_limit_euros must be finite and nonnegative".into(),
        ));
    }
    Ok(())
}

/// None is an explicit unrestricted job. A legacy zero is not monetary evidence
/// for prior calls, but a fresh deterministic preparation can prove no calls.
pub(super) fn refusal(job: &ReprocessJob, may_dispatch: bool) -> Option<&'static str> {
    let limit = job.budget_limit_euros?;
    if !limit.is_finite() || limit < 0.0 {
        return Some("Currency budget limit is invalid");
    }
    if !job.progress.cost_spent_euros.is_finite() || job.progress.cost_spent_euros < 0.0 {
        return Some(UNKNOWN_PRIOR);
    }
    if job.progress.cost_spent_euros >= limit {
        return Some("Budget limit exceeded");
    }
    if may_dispatch {
        return Some(UNPRICED);
    }
    // Existing maintenance seeds may have caused adjudication work before the
    // dispatch guard was installed. Do not relabel their legacy zero as free.
    job.started_at.is_some().then_some(UNKNOWN_PRIOR)
}

/// Persist only the job's refusal under the original captured epoch/attempt.
/// Prepared source/graph effects and provider-child identifiers are absent.
pub(super) async fn pause(
    planning: &PlanningTruthMaintenanceStorage,
    job: ReprocessJob,
    config: &ReprocessConfig,
    reason: &str,
) -> PipelineResult<ReprocessResult> {
    let scope = format!(
        "{}:{}",
        job.base.id,
        planning.prepared_plan()?.observed_epoch
    );
    let request = serde_json::json!({
        "job":job.base.id, "selection":job.selection_criteria,
        "stages":job.target_stages, "limit":job.budget_limit_euros,
        "config":config, "reason":reason,
    });
    let paused = config.pause_on_budget_limit;
    let result = ReprocessResult {
        paused,
        continuation_required: paused,
        error: (!paused).then(|| reason.to_owned()),
        ..Default::default()
    };
    let mut after = job.clone();
    after.status = if paused {
        ReprocessStatus::Paused
    } else {
        ReprocessStatus::Failed
    };
    after.error = Some(reason.into());
    if !paused {
        after.completed_at = Some(Utc::now());
    }
    planning.stage(Some(job.clone()), after)?;
    planning
        .audit_pending_effects("reprocess_currency_budget_refusal")
        .await?;
    planning
        .commit_reprocess_control(
            &job,
            "reprocess_currency_budget_refusal",
            &scope,
            &request,
            &result,
        )
        .await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monetary_restriction_rejects_unknowns_without_inventing_prices() {
        let mut job = ReprocessJob {
            base: BaseFields::new(
                RecordId::new_v4(TableName::ReprocessJob),
                "space".into(),
                "test".into(),
            ),
            name: "currency".into(),
            selection_criteria: SelectionCriteria::default(),
            target_stages: vec![ReprocessTargetStage::ReMaintain],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 0,
            budget_limit_euros: None,
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: None,
        };
        assert_eq!(refusal(&job, true), None);
        job.budget_limit_euros = Some(10.0);
        assert_eq!(refusal(&job, true), Some(UNPRICED));
        assert_eq!(refusal(&job, false), None);
        job.started_at = Some(Utc::now());
        assert_eq!(refusal(&job, false), Some(UNKNOWN_PRIOR));
        job.progress.cost_spent_euros = 10.0;
        assert_eq!(refusal(&job, false), Some("Budget limit exceeded"));
        for invalid in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(validate_limit(Some(invalid)).is_err());
        }
        assert!(validate_limit(Some(0.0)).is_ok());
        assert!(validate_limit(None).is_ok());
        job.budget_limit_euros = None;
        assert!(refusal_with_origin(&job, true, true).is_some());
        assert!(refusal_with_origin(&job, false, true).is_some());
        assert_eq!(refusal_with_origin(&job, true, false), None);
    }
}

#[cfg(test)]
#[path = "budget_tests.rs"]
mod dispatch_tests;

#[cfg(test)]
#[path = "watch_budget_tests.rs"]
mod watch_dispatch_tests;

#[cfg(test)]
#[path = "standing_dispatch_tests.rs"]
mod standing_dispatch_tests;

#[cfg(test)]
#[path = "../../../../tests/recall_preview/producer_ancestry_oracles.rs"]
mod independent_producer_ancestry;

#[cfg(test)]
#[path = "dispatch_epoch_tests.rs"]
mod dispatch_epoch_tests;
