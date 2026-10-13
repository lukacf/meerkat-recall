//! Synchronous trusted-library convenience, using actual queue custody and
//! scheduler accounting. It never manufactures an AttemptFence or converts an
//! existing signed recurring delegation into a direct local producer.

use super::*;
use crate::provider_usage::{reconcile_finished_usage, DurableUsageObserver};
use crate::scheduler::execution_contract::AttemptUsage;
use crate::scheduler::{BudgetCheckResult, BudgetEnforcer};
use elephant_auth::standing_watch::StandingWatchAdmission;
use elephant_storage::source_watch_registration::PreparedSourceWatchRegistration;
use elephant_storage::standing_watch_repository::StandingWatchRepository;
use elephant_storage::WorkRunRepository;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::{WorkRun, WorkRunMetrics};

pub(super) async fn check(
    service: &SourceWatchService,
    watch: SourceWatch,
    actor: &str,
    admission: &StandingWatchAdmission,
) -> PipelineResult<SourceWatchRun> {
    validate_watch(&watch)?;
    let timeout_secs = u64::from(service.local_budget.per_item_wall_time_secs.unwrap_or(300));
    if timeout_secs == 0 {
        return Err(PipelineError::BudgetExceeded(
            "local source-watch wall time is zero".into(),
        ));
    }
    let lease_secs = i64::try_from(timeout_secs)
        .ok()
        .and_then(|value| value.checked_add(30))
        .ok_or_else(|| {
            PipelineError::InvalidInput("local watch deadline exceeds lease range".into())
        })?;
    let storage = &service.storage;
    let space = &watch.base.space_id;
    let source = service
        .sources
        .get(space, watch.source.id())
        .await?
        .ok_or_else(|| PipelineError::NotFound(watch.source.id().to_string()))?;
    let records = Box::pin(PreparedSourceWatchRegistration::prepare(
        storage,
        &source,
        Some(&watch),
        watch.clone(),
    ))
    .await?;
    records.authorize(admission.principal())?;
    let standing_repository = StandingWatchRepository::new(storage.clone());
    let initial =
        Box::pin(standing_repository.prepare_local_registration_if_absent(&watch, admission))
            .await?;
    if let Some(initial) = initial {
        // The private row was actually absent and remains absent at commit.
        // Never interpret a failed/corrupt/signed capture as local registration.
        Box::pin(records.clone().register(storage, &initial)).await?;
    }
    let standing =
        Box::pin(standing_repository.capture_for_local_scheduling(&watch, admission)).await?;
    let repository = WorkItemRepository::new(storage.clone());
    let item =
        Box::pin(repository.enqueue_source_watch_refresh(&records, &standing, Some(admission), 0))
            .await?;
    let worker = format!("local-source-watch:{}", uuid::Uuid::new_v4());
    let expires = Utc::now()
        .checked_add_signed(Duration::seconds(lease_secs))
        .ok_or_else(|| PipelineError::InvalidInput("local watch lease overflows".into()))?;
    let claimed = Box::pin(repository.claim_authorized(&item, &worker, expires))
        .await?
        .ok_or_else(|| PipelineError::LeaseConflict(item.base.id.to_string()))?;
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)?;
    let enforcer =
        BudgetEnforcer::new(storage.clone(), service.local_budget.clone(), space.clone());
    let current_running = enforcer
        .current_running_for_kind(REFRESH_SOURCE_WATCH_KIND)
        .await?
        .saturating_sub(1);
    let budget = Box::pin(
        enforcer.check_all_budgets_and_reserve(
            IngestMode::Metadata,
            0.0,
            0,
            u32::try_from(timeout_secs)
                .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
            REFRESH_SOURCE_WATCH_KIND,
            current_running,
        ),
    )
    .await;
    let ledger = match budget {
        Ok(checked) if checked.result == BudgetCheckResult::Allowed => {
            checked.reserved_ledger_id.ok_or_else(|| {
                PipelineError::InvalidState("watch reservation has no ledger".into())
            })?
        }
        result => {
            if let Ok(checked) = &result {
                if let Some(ledger) = &checked.reserved_ledger_id {
                    // The actual claim was refused before physical execution.
                    enforcer.reconcile_cost(ledger, 0.0, 0, false).await?;
                }
            }
            let error = match result {
                Ok(checked) => PipelineError::BudgetExceeded(format!(
                    "watch dispatch refused: {:?}",
                    checked.result
                )),
                Err(error) => error,
            };
            Box::pin(repository.finish_authorized(
                &claimed,
                &attempt.lease_token,
                WorkItemStatus::Failed,
                Some(bounded_error(error.to_string())),
            ))
            .await?;
            return Err(error);
        }
    };
    let started = std::time::Instant::now();
    let started_at = Utc::now();
    let mut base = claimed.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    base.created_by = worker.clone();
    base.created_at = started_at;
    base.updated_at = started_at;
    let mut metrics = WorkRunMetrics::default();
    let reserved = AttemptUsage::reserved(0, 0.0);
    reserved
        .write_metrics(&mut metrics)
        .map_err(receipt_error)?;
    metrics
        .custom
        .insert("cost_ledger_id".into(), serde_json::json!(ledger));
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).map_err(receipt_error)?,
    );
    metrics
        .custom
        .insert("invoker".into(), serde_json::json!(actor));
    let runs = WorkRunRepository::new(storage.clone());
    let run = runs
        .create(WorkRun {
            base,
            work_item: Link::new(claimed.base.id.clone()),
            attempt: claimed.attempts,
            started_at,
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: worker,
            worker_version: env!("CARGO_PKG_VERSION").into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await?;
    let observer =
        DurableUsageObserver::for_attempt(storage.clone(), &run.base.id, attempt.clone());
    let execution = crate::provider_dispatch::with_dispatch_observer(
        observer.clone(),
        Box::pin(service.check_with_attempt(
            watch.clone(),
            &claimed.base.created_by,
            &attempt,
            &run.base.id,
        )),
    );
    let result =
        match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), execution).await {
            Ok(result) => result,
            Err(_) => Err(PipelineError::InvalidState(
                "local source-watch execution timed out".into(),
            )),
        };
    let usage = Box::pin(observer.summarize(reserved)).await?;
    let mut completed = runs
        .get(space, &run.base.id)
        .await?
        .ok_or_else(|| PipelineError::InvalidState("original watch run disappeared".into()))?;
    completed.completed_at = Some(Utc::now());
    completed.success = result.is_ok();
    completed.error = result
        .as_ref()
        .err()
        .map(|error| bounded_error(error.to_string()));
    completed.metrics.wall_time_ms = u64::try_from(started.elapsed().as_millis())
        .map_err(|error| PipelineError::InvalidState(error.to_string()))?;
    usage
        .write_metrics(&mut completed.metrics)
        .map_err(receipt_error)?;
    runs.update(space, &run.base.id, completed).await?;
    Box::pin(reconcile_finished_usage(storage, space, &run.base.id)).await?;
    let status = if result.is_ok() {
        WorkItemStatus::Done
    } else {
        WorkItemStatus::Failed
    };
    Box::pin(
        repository.finish_authorized(
            &claimed,
            &attempt.lease_token,
            status,
            result
                .as_ref()
                .err()
                .map(|error| bounded_error(error.to_string())),
        ),
    )
    .await?
    .ok_or_else(|| PipelineError::LeaseConflict(claimed.base.id.to_string()))?;
    match result? {
        SourceWatchAttemptOutcome::Observed(run) => Ok(run),
        SourceWatchAttemptOutcome::ReceiptOnly(_) => Err(PipelineError::InvalidState(
            "watch source receipt replayed without a new observation; read source operation status"
                .into(),
        )),
    }
}

fn receipt_error(error: serde_json::Error) -> PipelineError {
    PipelineError::ValidationError(error.to_string())
}
