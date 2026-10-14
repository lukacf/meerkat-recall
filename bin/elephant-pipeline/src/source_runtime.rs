//! Dispatcher joins for the original accepted source revision. Authority lives
//! in `SourceHead`, queue attempts, and the sealed publication manifest.

use super::{
    classify_failure, enqueue_idempotent, enqueue_work_successor, failure_context_code,
    failure_diagnostic, list_configured_space_ids, materialize_doc_from_source, retry_action,
    successor_priority, warn, AttemptUsage, Doc, DocNodeRepository, DocRepository,
    DocRevisionRepository, EmbedWorker, FailureClass, FromStr, IngestMode, ModelOperation,
    PipelineError, PlanExtractionPayload, ProviderDispatchContext, RecordId, Repository,
    RetryAction, Source, SpaceConfigRepository, StorageClient, TableName, Utc,
    WorkArtifactRepository, WorkFailure, WorkItem, WorkItemRepository, WorkItemStatus, WorkQueue,
    WorkQueueConfig, WorkQueueWorkerConfig, WorkRunMetrics, WorkRunRepository,
    EXTRACT_ASSERTIONS_KIND, RENDER_DOC_KIND, SOURCE_METADATA_ARTIFACT_TYPE, TM_COMMIT_KIND,
    TM_RECOMPUTE_KIND, TM_RESOLVE_SLOT_KIND, TRIAGE_SOURCE_KIND,
};
use elephant_pipeline::source_queue::{
    configuration_from_work, enqueue_source_plan, pin_source_configuration, publication_from_work,
};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::{
    required_embedding, source_bundle_id_for_plan, source_plan_id, ExpectedSourceBundle,
    SourceDocumentMapping, SourcePublicationManifest, SourcePublicationManifestRepository,
};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{PublicationFence, SourceVisibility};
use elephant_types::records::doc_node::NodeRole;

pub(super) const METADATA_IDS: &str = "_metadata_bundle_ids";
pub(super) const PREDECESSOR: &str = "_predecessor_work_item";

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn accepted_source(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<Option<Source>> {
    let Some(fence) = publication_from_work(item)? else {
        return Ok(None);
    };
    let repo = SourcePublicationRepository::new(storage.clone());
    let head = repo
        .head(&item.base.space_id, &fence.source_id)
        .await?
        .ok_or_else(|| WorkFailure {
            class: FailureClass::StaleInput,
            message: "Accepted source head missing".into(),
        })?;
    if head.generation != fence.generation
        || head.operation_id != fence.operation_id
        || head.desired_revision.as_deref() != Some(fence.revision_id.as_str())
        || head.lifecycle != SourceVisibility::Active
    {
        return Err(WorkFailure {
            class: FailureClass::StaleInput,
            message: "Accepted source revision superseded or withdrawn".into(),
        }
        .into());
    }
    let revision = repo
        .revision(&item.base.space_id, &fence.source_id, &fence.revision_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Accepted immutable source revision missing"))?;
    if revision.source.base.space_id != item.base.space_id
        || revision.source.base.id != fence.source_id
    {
        anyhow::bail!("Accepted source envelope identity mismatch");
    }
    Ok(Some(revision.source))
}

#[expect(
    clippy::significant_drop_tightening,
    clippy::suspicious_operation_groupings,
    reason = "Compare the receipt source generation with its publication fence and retain repository lifetime"
)]
pub(super) async fn begin_operation(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<bool> {
    use elephant_types::publication::OperationState;
    let Some(publication) = publication_from_work(item)? else {
        return Ok(false);
    };
    let repository = SourcePublicationRepository::new(storage.clone());
    let operation = repository
        .operation(&item.base.space_id, &publication.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Accepted operation receipt missing"))?;
    if operation.source_id != publication.source_id
        || operation.source_generation != publication.generation
        || operation.desired_revision.as_deref() != Some(publication.revision_id.as_str())
    {
        anyhow::bail!("Accepted operation identity mismatch");
    }
    let configuration = configuration_from_work(item)?;
    if operation.state == OperationState::Completed {
        // A bootstrap never executes model work. Completed semantic work may
        // replay only the plan that actually published, even after config rotates.
        if configuration.is_none() && item.kind == TRIAGE_SOURCE_KIND {
            return Ok(true);
        }
        let manifest = SourcePublicationManifestRepository::new(storage.clone())
            .get(&item.base.space_id, &publication)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Completed source manifest missing"))?;
        if configuration.as_ref() != Some(&manifest.configuration) {
            return Err(WorkFailure {
                class: FailureClass::StaleInput,
                message: "Original work plan differs from completed publication".into(),
            }
            .into());
        }
        return Ok(true);
    }
    if !matches!(
        operation.state,
        OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
    ) {
        return Err(WorkFailure {
            class: FailureClass::Terminal,
            message: "Source operation no longer accepts stage progress".into(),
        }
        .into());
    }
    let current = SpaceConfigRepository::new(storage.clone())
        .resolve_space(&item.base.space_id)
        .await?
        .token;
    let Some(configuration) = configuration else {
        if item.kind != TRIAGE_SOURCE_KIND {
            anyhow::bail!("Semantic source work lacks captured configuration");
        }
        let source = accepted_source(storage, item)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Accepted bootstrap input missing"))?;
        enqueue_source_plan(
            storage,
            &source,
            &publication,
            &current,
            &item.base.created_by,
        )
        .await?;
        return Ok(true);
    };
    if configuration != current {
        return Err(WorkFailure {
            class: FailureClass::ConfigurationBlocked,
            message: "Original source plan configuration changed".into(),
        }
        .into());
    }
    // A readiness poll is not progress by the child that actually hit the
    // budget. Keep its observable wait until a real stage resumes or this
    // finalizer commits the complete publication.
    if item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
        && operation.state == OperationState::FailedRetryable
        && operation.error_code.as_deref() == Some("awaits_budget")
    {
        return Ok(false);
    }
    repository
        .record_execution_state(
            &item.base.space_id,
            &publication,
            &elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            OperationState::Processing,
            None,
        )
        .await?;
    Ok(false)
}

/// Select one action for both durable source receipt and queue disposition.
async fn failure_disposition(
    storage: &StorageClient,
    item: &WorkItem,
    error: &anyhow::Error,
    config: &WorkQueueWorkerConfig,
) -> anyhow::Result<(FailureClass, RetryAction)> {
    let class = classify_failure(error);
    if class == FailureClass::LostLease {
        return Ok((class, RetryAction::Abandon));
    }
    let limit = match elephant_pipeline::scheduler::extraction_repair::semantic_retry_limit(
        storage,
        item,
        config.transient_retry_max_attempts,
    )
    .await
    {
        Ok(limit) => limit,
        Err(PipelineError::GroundingRepairExhausted) => {
            return Ok((FailureClass::Terminal, RetryAction::Fail))
        }
        Err(error) => return Err(error.into()),
    };
    match elephant_pipeline::scheduler::extraction_repair::consumed_semantic_repair(storage, item)
        .await
    {
        Ok(true) => {
            // Preserve the original cause and genuine fresh-configuration recovery,
            // but never refund a physical repair into another same-plan model call.
            return Ok((class, RetryAction::Fail));
        }
        Ok(false) => {}
        Err(PipelineError::GroundingRepairExhausted) => {
            return Ok((FailureClass::Terminal, RetryAction::Fail));
        }
        Err(error) => return Err(error.into()),
    }
    Ok((
        class,
        retry_action(
            class,
            item.attempts,
            limit,
            config.transient_retry_base_delay_secs,
            config.transient_retry_max_delay_secs,
        ),
    ))
}

/// Pre-execution failures have no fabricated `WorkRun` or usage. Persist the
/// original source disposition first, then apply that identical queue action.
pub(super) async fn record_preflight_failure(
    storage: &StorageClient,
    item: &WorkItem,
    error: &anyhow::Error,
    config: &WorkQueueWorkerConfig,
) -> anyhow::Result<()> {
    let (class, action) = failure_disposition(storage, item, error, config).await?;
    record_operation_failure_with_reason(
        storage,
        item,
        class,
        action,
        source_failure_reason(error),
    )
    .await?;
    elephant_pipeline::scheduler::execution_contract::record_preflight_disposition(
        storage,
        item,
        error,
        class,
        action,
        config.transient_retry_base_delay_secs,
    )
    .await?;
    Ok(())
}

/// Shared production failure bookkeeping for worker execution and stage oracles.
/// Order: failed run/actual usage, settlement, source receipt, then caller applies
/// the returned identical queue action. Original input metadata is preserved.
#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn record_failed_execution(
    storage: &StorageClient,
    item: &WorkItem,
    run_id: &RecordId,
    error: &anyhow::Error,
    usage: &AttemptUsage,
    completed_at: chrono::DateTime<Utc>,
    wall_time_ms: u64,
    config: &WorkQueueWorkerConfig,
) -> anyhow::Result<(FailureClass, RetryAction)> {
    let repository = WorkRunRepository::new(storage.clone());
    let mut run = repository
        .get(&item.base.space_id, run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Failed original run disappeared"))?;
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
    anyhow::ensure!(
        run.work_item.id() == &item.base.id
            && run.metrics.custom.get("attempt_fence") == Some(&serde_json::to_value(&fence)?),
        "Failed original run authority changed"
    );
    let class = classify_failure(error);
    run.metrics.custom.insert(
        "failure_diagnostic".into(),
        failure_diagnostic::FailureDiagnostic::from_error(error).as_json(),
    );
    if let Some(code) = failure_context_code(error) {
        run.metrics
            .custom
            .insert("failure_context_code".into(), serde_json::json!(code));
    }
    usage.write_metrics(&mut run.metrics)?;
    run.completed_at = Some(completed_at);
    run.success = false;
    let diagnostic = failure_diagnostic::FailureDiagnostic::from_error(error);
    run.error = Some(
        if diagnostic.kind == "extraction" || diagnostic.code == "embedding_input_too_large" {
            diagnostic.summary()
        } else {
            error.to_string()
        },
    );
    run.metrics.wall_time_ms = wall_time_ms;
    run.metrics
        .custom
        .insert("failure_class".into(), serde_json::json!(class));
    repository.update(&item.base.space_id, run_id, run).await?;
    elephant_pipeline::provider_usage::reconcile_finished_usage(
        storage,
        &item.base.space_id,
        run_id,
    )
    .await?;
    let disposition = failure_disposition(storage, item, error, config).await?;
    record_operation_failure_with_reason(
        storage,
        item,
        disposition.0,
        disposition.1,
        source_failure_reason(error),
    )
    .await?;
    Ok(disposition)
}

/// Apply the exact source-recorded disposition under the original attempt fence.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn apply_failed_disposition(
    storage: &StorageClient,
    item: &WorkItem,
    error: &anyhow::Error,
    class: FailureClass,
    action: RetryAction,
) -> anyhow::Result<()> {
    let queue = WorkQueue::new(storage.clone(), WorkQueueConfig::default()).for_attempt(item)?;
    if class == FailureClass::ConfigurationBlocked
        && schedule_configuration_recovery(storage, item).await?
    {
        WorkItemRepository::new(storage.clone())
            .update_running_for_worker(
                &item.base.space_id,
                &item.base.id,
                queue.lease_owner(),
                WorkItemStatus::Cancelled,
                Some("original_configuration_replaced_by_fresh_plan".into()),
            )
            .await?;
        return Ok(());
    }
    match action {
        RetryAction::Retry { delay_secs } | RetryAction::Blocked { delay_secs } => {
            let repository = WorkItemRepository::new(storage.clone());
            if class == FailureClass::PendingDependencies {
                repository
                    .requeue_pending_dependencies(
                        &item.base.space_id,
                        &item.base.id,
                        queue.lease_owner(),
                        delay_secs,
                        error.to_string(),
                    )
                    .await?;
            } else if class == FailureClass::ConfigurationBlocked {
                repository
                    .requeue_configuration_blocked(
                        &item.base.space_id,
                        &item.base.id,
                        queue.lease_owner(),
                        delay_secs,
                        error.to_string(),
                    )
                    .await?;
            } else {
                repository
                    .requeue_with_delay(
                        &item.base.space_id,
                        &item.base.id,
                        queue.lease_owner(),
                        delay_secs,
                        Some(format!("{class:?}: {error}")),
                    )
                    .await?;
            }
            return Ok(());
        }
        RetryAction::Abandon => return Ok(()),
        RetryAction::RepairInput => match schedule_input_repair(storage, item).await? {
            InputRepair::Replaced => {
                WorkItemRepository::new(storage.clone())
                    .update_running_for_worker(
                        &item.base.space_id,
                        &item.base.id,
                        queue.lease_owner(),
                        WorkItemStatus::Cancelled,
                        Some("stale_decision_replaced_by_fresh_preparation".into()),
                    )
                    .await?;
                return Ok(());
            }
            InputRepair::RetryOriginal => {
                WorkItemRepository::new(storage.clone())
                    .requeue_with_delay(
                        &item.base.space_id,
                        &item.base.id,
                        queue.lease_owner(),
                        30,
                        Some("stale_input: original revision repair queued".into()),
                    )
                    .await?;
                return Ok(());
            }
            InputRepair::Unavailable => {}
        },
        RetryAction::Fail => {}
    }
    if action != RetryAction::Fail {
        record_operation_failure(storage, item, class, RetryAction::Fail).await?;
    }
    queue
        .fail(&item.base.space_id, &item.base.id, error.to_string())
        .await?;
    if item.kind == "execute_reprocess_job" {
        let original = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
        elephant_pipeline::reprocess::reconcile_failed_work(
            storage,
            &item.base.space_id,
            &item.base.id,
            Some(&original),
        )
        .await?;
    }
    Ok(())
}

/// Record the selected queue disposition without discarding its original cause.
/// A transient classification is terminal when its finite retry budget is spent.
pub(super) async fn record_operation_failure(
    storage: &StorageClient,
    item: &WorkItem,
    class: FailureClass,
    action: RetryAction,
) -> anyhow::Result<()> {
    record_operation_failure_with_reason(storage, item, class, action, None).await
}

/// Only typed causes and exact private sentinels become public payload-free codes.
fn source_failure_reason(error: &anyhow::Error) -> Option<&'static str> {
    for cause in error.chain() {
        match cause.downcast_ref::<PipelineError>() {
            Some(PipelineError::BudgetExceeded(_)) => return Some("awaits_budget"),
            Some(PipelineError::InvalidState(message)) => {
                let reason = message.strip_prefix("Conflict: ").unwrap_or(message);
                match reason {
                    "source_publication_incomplete:expected_bundle_missing"
                    | "source_publication_incomplete:expected_artifact_missing" => {
                        return Some("awaits_source_bundle")
                    }
                    "source_publication_incomplete:required_index_missing" => {
                        return Some("awaits_source_index")
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    None
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the observed repositories through the original fenced operation update."
)]
async fn record_operation_failure_with_reason(
    storage: &StorageClient,
    item: &WorkItem,
    class: FailureClass,
    action: RetryAction,
    reason: Option<&str>,
) -> anyhow::Result<()> {
    use elephant_types::publication::OperationState;
    if class == FailureClass::LostLease {
        return Ok(());
    }
    let Some(publication) = publication_from_work(item)? else {
        return Ok(());
    };
    let source_repository = SourcePublicationRepository::new(storage.clone());
    let operation = source_repository
        .operation(&item.base.space_id, &publication.operation_id)
        .await?;
    if class == FailureClass::PendingDependencies
        && item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
        && operation.as_ref().is_some_and(|operation| {
            operation.state == OperationState::FailedRetryable
                && operation.error_code.as_deref() == Some("awaits_budget")
        })
    {
        return Ok(());
    }
    if !operation.is_some_and(|operation| {
        matches!(
            operation.state,
            OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
        )
    }) {
        // A completed/lifecycle/terminal receipt is immutable to this worker.
        // Its own queue item can still be acknowledged with its original token.
        return Ok(());
    }
    let head = source_repository
        .head(&item.base.space_id, &publication.source_id)
        .await?;
    if !head.is_some_and(|head| {
        head.lifecycle == SourceVisibility::Active
            && head.generation == publication.generation
            && head.operation_id == publication.operation_id
            && head.desired_revision.as_deref() == Some(publication.revision_id.as_str())
    }) {
        return Ok(());
    }
    if let Some(configuration) = configuration_from_work(item)? {
        let current = SpaceConfigRepository::new(storage.clone())
            .resolve_space(&item.base.space_id)
            .await?
            .token;
        if configuration != current {
            // Obsolete stage outcomes cannot overwrite the successor plan's progress.
            return Ok(());
        }
    }
    let state = match (class, action) {
        (FailureClass::Terminal, _) | (_, RetryAction::Fail) => OperationState::FailedTerminal,
        (FailureClass::PendingDependencies, _) => OperationState::Processing,
        _ => OperationState::FailedRetryable,
    };
    let code = reason.unwrap_or(match class {
        FailureClass::Transient => "transient_failure",
        FailureClass::Timeout => "execution_timeout",
        FailureClass::ConfigurationBlocked => "configuration_blocked",
        FailureClass::PendingDependencies => "pending_dependencies",
        FailureClass::StaleInput => "stale_input",
        FailureClass::LostLease => "lost_lease",
        FailureClass::Terminal => "terminal_failure",
    });
    SourcePublicationRepository::new(storage.clone())
        .record_execution_state(
            &item.base.space_id,
            &publication,
            &elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            state,
            Some(code),
        )
        .await?;
    Ok(())
}

pub(super) async fn require_predecessor(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<()> {
    let origin =
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone())
            .origin_for_work(item)
            .await?;
    if let elephant_storage::work_producer_repository::WorkProducerOrigin::Child {
        parent_work_item_id,
        original_parent_work_run_id,
        wait_for_parent_completion: true,
        ..
    } = &origin
    {
        let parent = WorkItemRepository::new(storage.clone())
            .get(&item.base.space_id, parent_work_item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Private original predecessor missing"))?;
        let run = WorkRunRepository::new(storage.clone())
            .get(&item.base.space_id, original_parent_work_run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Private original predecessor receipt missing"))?;
        elephant_storage::work_producer_repository::verify_parent_origin(&origin, &parent, &run)?;
        if parent.status != WorkItemStatus::Done || !run.success || run.completed_at.is_none() {
            return Err(WorkFailure {
                class: FailureClass::PendingDependencies,
                message: "Original predecessor has not completed successfully".into(),
            }
            .into());
        }
        return Ok(());
    }
    let Some(id) = item
        .payload
        .get(PREDECESSOR)
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    let id = RecordId::from_str(id)?;
    if id == item.base.id {
        anyhow::bail!("Work item depends on itself");
    }
    let prior = WorkItemRepository::new(storage.clone())
        .get(&item.base.space_id, &id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Required predecessor missing: {id}"))?;
    match prior.status {
        WorkItemStatus::Done => Ok(()),
        WorkItemStatus::Failed | WorkItemStatus::Cancelled => Err(WorkFailure {
            class: FailureClass::PendingDependencies,
            message: format!("Predecessor {id} needs recovery"),
        }
        .into()),
        _ => Err(WorkFailure {
            class: FailureClass::PendingDependencies,
            message: format!("Predecessor {id} has not completed"),
        }
        .into()),
    }
}

pub(super) async fn require_successful_run(
    storage: &StorageClient,
    space: &str,
    id: &RecordId,
) -> anyhow::Result<()> {
    let run = WorkRunRepository::new(storage.clone())
        .get(space, id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Required predecessor run missing: {id}"))?;
    if !run.success || run.completed_at.is_none() {
        return Err(PipelineError::InvalidInput(
            "predecessor attempt did not complete successfully".into(),
        )
        .into());
    }
    Ok(())
}

pub(super) fn metadata_bundle_id(
    fence: &PublicationFence,
    configuration: &SpaceConfigToken,
) -> RecordId {
    let identity = format!(
        "source-metadata:{}:{}:{}:{}",
        fence.source_id,
        fence.generation,
        fence.revision_id,
        source_plan_id(fence, configuration)
    );
    RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()),
    )
}

/// Metadata is deterministic from immutable source input. Its staged output is
/// still attempt-local: a later run cannot overwrite the earlier producer.
pub(super) fn metadata_stage_id(item: &WorkItem, work_run: &RecordId) -> anyhow::Result<RecordId> {
    let publication = publication_from_work(item)?
        .ok_or_else(|| anyhow::anyhow!("Metadata publication missing"))?;
    let configuration = configuration_from_work(item)?
        .ok_or_else(|| anyhow::anyhow!("Metadata configuration missing"))?;
    if work_run.table() != TableName::WorkRun {
        anyhow::bail!("Invalid metadata producer run");
    }
    let identity = serde_json::to_vec(&(
        "source-metadata-stage-v1",
        &item.base.space_id,
        source_plan_id(&publication, &configuration),
        SOURCE_METADATA_ARTIFACT_TYPE,
        work_run,
    ))?;
    Ok(RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity),
    ))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn persist_metadata_stage(
    storage: &StorageClient,
    item: &WorkItem,
    work_run: &RecordId,
    mut artifact: elephant_types::records::work::WorkArtifact,
) -> anyhow::Result<()> {
    let repository = WorkArtifactRepository::new(storage.clone());
    if publication_from_work(item)?.is_some() {
        let run = WorkRunRepository::new(storage.clone())
            .get(&item.base.space_id, work_run)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Metadata producer run missing"))?;
        if run.work_item.id() != &item.base.id || artifact.work_run.id() != work_run {
            anyhow::bail!("Metadata producer identity changed");
        }
        accepted_source(storage, item)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Metadata original source missing"))?;
        let captured = configuration_from_work(item)?
            .ok_or_else(|| anyhow::anyhow!("Metadata plan missing"))?;
        if SpaceConfigRepository::new(storage.clone())
            .resolve_space(&item.base.space_id)
            .await?
            .token
            != captured
        {
            return Err(WorkFailure {
                class: FailureClass::ConfigurationBlocked,
                message: "Metadata producer configuration changed".into(),
            }
            .into());
        }
        artifact.base.id = metadata_stage_id(item, work_run)?;
        for key in [
            elephant_pipeline::source_queue::PUBLICATION_PAYLOAD_KEY,
            elephant_pipeline::source_queue::SOURCE_CONFIGURATION_KEY,
            elephant_pipeline::source_queue::SOURCE_PLAN_KEY,
        ] {
            artifact.data[key] = item.payload[key].clone();
        }
        if let Some(existing) = repository
            .get(&item.base.space_id, &artifact.base.id)
            .await?
        {
            if existing.data != artifact.data
                || existing.work_run != artifact.work_run
                || existing.artifact_type != artifact.artifact_type
                || existing.base.security != artifact.base.security
                || existing.base.subjects != artifact.base.subjects
                || existing.base.subjects_state != artifact.base.subjects_state
            {
                anyhow::bail!("Metadata stage replay differs from original output");
            }
            return Ok(());
        }
    }
    repository.create(artifact).await?;
    Ok(())
}

pub(super) async fn materialize(
    storage: &StorageClient,
    source: &Source,
    item: &WorkItem,
) -> anyhow::Result<Doc> {
    let Some(fence) = publication_from_work(item)? else {
        return materialize_doc_from_source(storage, source, &item.base.created_by).await;
    };
    let template = source
        .manual_doc_template()?
        .ok_or_else(|| anyhow::anyhow!("Accepted source lacks immutable document input"))?;
    let document_id =
        elephant_pipeline::source_queue::accepted_document_id(&fence, &template.doc_id)?;
    // Public intake, worker materialization and recovery must use the same
    // projection and immutable-input checks for this original acceptance.
    Box::pin(elephant_storage::source_intake::stage_source_revision(
        storage, source, &fence,
    ))
    .await?;
    DocRepository::new(storage.clone())
        .get(&source.base.space_id, &document_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Accepted document was not staged"))
}

fn metadata_ids(item: &WorkItem) -> anyhow::Result<Vec<RecordId>> {
    let ids: Vec<String> = serde_json::from_value(
        item.payload
            .get(METADATA_IDS)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Original metadata bundle IDs missing"))?,
    )?;
    ids.into_iter()
        .map(|id| RecordId::from_str(&id).map_err(Into::into))
        .collect()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn seal_manifest(
    storage: &StorageClient,
    item: &WorkItem,
    mode: IngestMode,
    revision_id: Option<&RecordId>,
    plan: Option<&elephant_pipeline::scheduler::extraction_work_kinds::PlanExtractionResult>,
) -> anyhow::Result<Option<SourcePublicationManifest>> {
    let Some(publication) = publication_from_work(item)? else {
        return Ok(None);
    };
    let configurations = SpaceConfigRepository::new(storage.clone());
    let configuration = configuration_from_work(item)?
        .ok_or_else(|| anyhow::anyhow!("Source planner configuration missing"))?;
    if configurations
        .resolve_space(&item.base.space_id)
        .await?
        .token
        != configuration
    {
        return Err(WorkFailure {
            class: FailureClass::ConfigurationBlocked,
            message: "Source configuration changed before sealing plan".into(),
        }
        .into());
    }
    let mut manifest = SourcePublicationManifest {
        space_id: item.base.space_id.clone(),
        publication: publication.clone(),
        mode,
        configuration: configuration.clone(),
        embedding_profile: None,
        documents: Vec::new(),
        metadata_artifact_ids: metadata_ids(item)?,
        planning_artifact_ids: Vec::new(),
        bundles: Vec::new(),
        embeddings: Vec::new(),
        embedding_work_item_ids: Vec::new(),
    };
    if let Some(revision_id) = revision_id {
        let revision = DocRevisionRepository::new(storage.clone())
            .get(&item.base.space_id, revision_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Required rendered revision missing"))?;
        manifest.embedding_work_item_ids.push(
            elephant_storage::source_publication_manifest::source_embedding_work_id_for_plan(
                &item.base.space_id,
                &publication,
                &configuration,
                revision_id,
            ),
        );
        manifest.documents.push(SourceDocumentMapping {
            space_id: item.base.space_id.clone(),
            doc_id: revision.doc.id().clone(),
            doc_revision_id: revision_id.clone(),
            publication: publication.clone(),
        });
        let binding = configurations
            .authorize_model_operation(&item.base.space_id, ModelOperation::DocumentEmbedding, None)
            .await?;
        let profile = binding.vector_partition()?;
        for node in DocNodeRepository::new(storage.clone())
            .find_by_revision(revision_id)
            .await?
        {
            if matches!(
                node.role,
                NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
            ) {
                manifest
                    .embeddings
                    .push(required_embedding(&profile, &node)?);
            }
        }
        manifest.embedding_profile = Some(profile);
    }
    if let Some(result) = plan {
        let revision_id = revision_id.ok_or_else(|| anyhow::anyhow!("Plan has no revision"))?;
        manifest
            .planning_artifact_ids
            .push(result.artifact_id.clone());
        for selection in &result.plan.node_selections {
            manifest.bundles.push(ExpectedSourceBundle {
                artifact_id: source_bundle_id_for_plan(
                    &publication,
                    &configuration,
                    revision_id,
                    &selection.node_id,
                ),
                node_id: selection.node_id.clone(),
                node_ids: selection.node_ids.clone(),
                revision_id: revision_id.clone(),
                span_start: selection.span_start,
                span_end: selection.span_end,
            });
        }
    }
    let repository = SourcePublicationManifestRepository::new(storage.clone());
    let manifest = match repository.get(&item.base.space_id, &publication).await? {
        Some(previous) if previous.configuration != configuration => {
            repository
                .replace_operation_plan(
                    &item.base.space_id,
                    &publication,
                    &previous.digest()?,
                    manifest,
                    &elephant_storage::work_attempt_repository::fence_from_claim(item)?,
                )
                .await?
        }
        _ => repository.set_operation_plan(manifest).await?,
    };
    enqueue_embeddings(storage, item, &manifest).await?;
    Ok(Some(manifest))
}

pub(super) async fn enqueue_finalizer(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<()> {
    let source = accepted_source(storage, item)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Finalizer requires accepted source"))?;
    let fence =
        publication_from_work(item)?.ok_or_else(|| anyhow::anyhow!("Finalizer fence missing"))?;
    let mut base = source.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    base.deleted_at = None;
    let mut work = WorkItem {
        base,
        kind: elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND.into(),
        payload: serde_json::json!({"_publication":fence}),
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!(
            "finalize_source_revision:{}:{}",
            fence.source_id, fence.generation
        ),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    elephant_pipeline::source_queue::inherit_source_context(item, &mut work)?;
    enqueue_idempotent(storage, work).await?;
    Ok(())
}

/// All-space repair from durable acceptance and closed manifests, even when
/// there is no Runnable work and no request visits the named space.
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn recover_sources(storage: &StorageClient) -> anyhow::Result<()> {
    let source_repo = SourcePublicationRepository::new(storage.clone());
    let configurations = SpaceConfigRepository::new(storage.clone());
    let mut incomplete = false;
    let jobs = elephant_storage::ReprocessJobRepository::new(storage.clone());
    let mut job_cursor = None;
    loop {
        let page = jobs.recoverable_after(256, job_cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        for job in &page {
            let recovery = async {
                let work_id = jobs.ensure_work(job).await?;
                elephant_pipeline::reprocess::reconcile_failed_work(
                    storage,
                    &job.base.space_id,
                    &work_id,
                    None,
                )
                .await?;
                Ok::<(), elephant_pipeline::PipelineError>(())
            }
            .await;
            if let Err(error) = recovery {
                incomplete = true;
                warn!(job = %job.base.id, space = %job.base.space_id, %error,
                    "Reprocess handoff or terminal-state recovery remains pending");
            }
        }
        let next = page.last().map(|job| job.base.id.clone());
        if next == job_cursor {
            anyhow::bail!("Reprocess recovery cursor did not advance");
        }
        job_cursor = next;
        if page.len() < 256 {
            break;
        }
    }
    for space in list_configured_space_ids(&configurations).await? {
        if let Err(error) = elephant_pipeline::provider_usage::recover_usage(storage, &space).await
        {
            incomplete = true;
            warn!(%space, %error, "Usage reconciliation remains pending");
        }
        let reindex: anyhow::Result<()> = async {
            let configuration = configurations
                .get_by_space_id(&space)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Discovered space vanished"))?;
            if configuration
                .processing
                .as_ref()
                .and_then(|processing| processing.pending_reindex.as_ref())
                .is_some_and(|pending| !pending.complete)
            {
                elephant_pipeline::source_queue::enqueue_space_reindex(
                    storage,
                    &configuration,
                    "pipeline.reindex_recovery",
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = reindex {
            incomplete = true;
            warn!(%space, %error, "Reindex enqueue recovery remains pending");
        }
    }
    let mut after: Option<String> = None;
    loop {
        let receipts = source_repo
            .recoverable_operations_after(256, after.as_deref())
            .await?;
        if receipts.is_empty() {
            break;
        }
        let next = receipts.last().map(|receipt| receipt.operation_id.clone());
        for receipt in receipts.iter().cloned() {
            if let Err(error) = recover_operation(storage, &source_repo, receipt).await {
                incomplete = true;
                warn!(%error, "Accepted operation recovery failed; other spaces continue");
            }
        }
        if next == after {
            anyhow::bail!("Accepted operation recovery cursor did not advance");
        }
        after = next;
        if receipts.len() < 256 {
            break;
        }
    }
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    for manifest in manifests.pending_all().await? {
        if let Err(error) = recover_manifest(storage, &source_repo, manifest).await {
            incomplete = true;
            warn!(%error, "Manifest recovery failed; other spaces continue");
        }
    }
    if incomplete {
        anyhow::bail!("Some durable recovery units remain incomplete");
    }

    Ok(())
}

async fn recover_operation(
    storage: &StorageClient,
    source_repo: &SourcePublicationRepository,
    receipt: elephant_types::publication::OperationReceipt,
) -> anyhow::Result<()> {
    let Some(revision_id) = receipt.desired_revision else {
        return Ok(());
    };
    let fence = PublicationFence {
        source_id: receipt.source_id,
        generation: receipt.source_generation,
        revision_id,
        operation_id: receipt.operation_id,
    };
    let head = source_repo
        .head(&receipt.space_id, &fence.source_id)
        .await?;
    if !head.is_some_and(|head| {
        head.lifecycle == SourceVisibility::Active
            && head.generation == fence.generation
            && head.operation_id == fence.operation_id
    }) {
        return Ok(());
    }
    let revision = source_repo
        .revision(&receipt.space_id, &fence.source_id, &fence.revision_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Recoverable accepted operation has no immutable input"))?;
    elephant_storage::source_intake::stage_source_revision(storage, &revision.source, &fence)
        .await?;
    let configuration = SpaceConfigRepository::new(storage.clone())
        .resolve_space(&receipt.space_id)
        .await?
        .token;
    enqueue_source_plan(
        storage,
        &revision.source,
        &fence,
        &configuration,
        "pipeline.source_recovery",
    )
    .await?;
    Ok(())
}

async fn recover_manifest(
    storage: &StorageClient,
    source_repo: &SourcePublicationRepository,
    manifest: SourcePublicationManifest,
) -> anyhow::Result<()> {
    let head = source_repo
        .head(&manifest.space_id, &manifest.publication.source_id)
        .await?;
    if !head.is_some_and(|head| {
        head.lifecycle == SourceVisibility::Active
            && head.generation == manifest.publication.generation
            && head.operation_id == manifest.publication.operation_id
            && head.desired_revision.as_deref() == Some(manifest.publication.revision_id.as_str())
    }) {
        return Ok(());
    }
    let revision = source_repo
        .revision(
            &manifest.space_id,
            &manifest.publication.source_id,
            &manifest.publication.revision_id,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("Manifest immutable input missing"))?;
    let current = SpaceConfigRepository::new(storage.clone())
        .resolve_space(&manifest.space_id)
        .await?
        .token;
    if manifest.configuration != current {
        enqueue_source_plan(
            storage,
            &revision.source,
            &manifest.publication,
            &current,
            "pipeline.configuration_recovery",
        )
        .await?;
        return Ok(());
    }
    // The original planner work remains available through durable archives.
    // Reuse its exact settings and expected selections, never a newer plan.
    for plan_id in &manifest.planning_artifact_ids {
        let plan = WorkArtifactRepository::new(storage.clone())
            .get(&manifest.space_id, plan_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Manifest planner receipt missing"))?;
        let run = WorkRunRepository::new(storage.clone())
            .get(&manifest.space_id, plan.work_run.id())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Manifest planner run missing"))?;
        let parent = WorkItemRepository::new(storage.clone())
            .get(&manifest.space_id, run.work_item.id())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Manifest planner work missing"))?;
        if parent.status != WorkItemStatus::Done || !run.success || run.completed_at.is_none() {
            continue;
        }
        enqueue_manifest_children(storage, &parent, &manifest).await?;
    }
    let mut base = revision.source.base;
    base.id = RecordId::new_v4(TableName::WorkItem);
    let mut seed = WorkItem {
        base,
        kind: "recovery".into(),
        payload: serde_json::json!({"_publication":manifest.publication}),
        status: WorkItemStatus::Done,
        priority: 10,
        idempotency_key: "recovery".into(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    pin_source_configuration(&mut seed, &manifest.configuration)?;
    enqueue_embeddings(storage, &seed, &manifest).await?;
    enqueue_finalizer(storage, &seed).await?;
    Ok(())
}

pub(super) async fn enqueue_manifest_children(
    storage: &StorageClient,
    parent: &WorkItem,
    manifest: &SourcePublicationManifest,
) -> anyhow::Result<()> {
    require_manifest_plan(parent, manifest)?;
    let settings: PlanExtractionPayload = serde_json::from_value(parent.payload.clone())?;
    for bundle in &manifest.bundles {
        if bundle.revision_id.to_string() != settings.revision_id {
            continue;
        }
        let payload = serde_json::json!({"node_id":bundle.node_id.to_string(),"node_ids":bundle.node_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "revision_id":bundle.revision_id.to_string(),"extractor_id":settings.extractor_id,"allow_raw_quotes":settings.allow_raw_quotes,
            "ingest_mode":manifest.mode.to_string(),"source_id":manifest.publication.source_id.to_string(),
            "span_start":bundle.span_start,"span_end":bundle.span_end});
        let mut base = parent.base.clone();
        base.id = RecordId::new_v4(TableName::WorkItem);
        enqueue_work_successor(
            storage,
            parent,
            WorkItem {
                base,
                kind: EXTRACT_ASSERTIONS_KIND.into(),
                payload,
                status: WorkItemStatus::Runnable,
                priority: successor_priority(parent.priority),
                idempotency_key: format!(
                    "{EXTRACT_ASSERTIONS_KIND}:{}:{}",
                    bundle.revision_id, bundle.node_id
                ),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            },
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn enqueue_embeddings(
    storage: &StorageClient,
    item: &WorkItem,
    manifest: &SourcePublicationManifest,
) -> anyhow::Result<()> {
    require_manifest_plan(item, manifest)?;
    for mapping in &manifest.documents {
        let mut base = item.base.clone();
        base.id = RecordId::new_v4(TableName::WorkItem);
        let mut work = WorkItem {
            base,
            kind: "embed_source_revision".into(),
            payload: serde_json::json!({"_publication":manifest.publication,"revision_id":mapping.doc_revision_id.to_string()}),
            status: WorkItemStatus::Runnable,
            priority: successor_priority(item.priority),
            idempotency_key: format!(
                "embed_source_revision:{}:{}:{}:{}",
                manifest.publication.source_id,
                manifest.publication.generation,
                mapping.doc_revision_id,
                source_plan_id(&manifest.publication, &manifest.configuration)
            ),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        pin_source_configuration(&mut work, &manifest.configuration)?;
        let created = enqueue_idempotent(storage, work).await?;
        if !manifest.embedding_work_item_ids.contains(&created.base.id) {
            anyhow::bail!("Embedding work differs from sealed manifest");
        }
    }
    Ok(())
}

pub(super) fn require_manifest_plan(
    item: &WorkItem,
    manifest: &SourcePublicationManifest,
) -> anyhow::Result<()> {
    if publication_from_work(item)?.as_ref() != Some(&manifest.publication)
        || configuration_from_work(item)?.as_ref() != Some(&manifest.configuration)
    {
        return Err(WorkFailure {
            class: FailureClass::StaleInput,
            message: "Work belongs to a different source plan".into(),
        }
        .into());
    }
    Ok(())
}

/// A configuration fix creates new work from the immutable accepted input. It
/// never changes the old work payload, source head, or accepted revision.
pub(super) async fn schedule_configuration_recovery(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<bool> {
    let Some(configuration) = configuration_from_work(item)? else {
        return Ok(false);
    };
    let current = SpaceConfigRepository::new(storage.clone())
        .resolve_space(&item.base.space_id)
        .await?
        .token;
    if current == configuration {
        return Ok(false);
    }
    let source = match accepted_source(storage, item).await {
        Ok(Some(source)) => source,
        Ok(None) => return Ok(false),
        Err(error) if classify_failure(&error) == FailureClass::StaleInput => return Ok(false),
        Err(error) => return Err(error),
    };
    let publication = publication_from_work(item)?
        .ok_or_else(|| anyhow::anyhow!("Original source fence missing"))?;
    let operation = SourcePublicationRepository::new(storage.clone())
        .operation(&item.base.space_id, &publication.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Accepted operation missing"))?;
    if operation.state == elephant_types::publication::OperationState::Completed {
        return Ok(false);
    }
    enqueue_source_plan(
        storage,
        &source,
        &publication,
        &current,
        "pipeline.configuration_recovery",
    )
    .await?;
    Ok(true)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub(super) async fn execute_embeddings(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<WorkRunMetrics> {
    let publication = elephant_pipeline::source_queue::publication_from_work(item)?
        .ok_or_else(|| anyhow::anyhow!("Source embedding lacks original fence"))?;
    let manifest = SourcePublicationManifestRepository::new(storage.clone())
        .get(&item.base.space_id, &publication)
        .await?
        .ok_or_else(|| WorkFailure {
            class: FailureClass::PendingDependencies,
            message: "Source embedding manifest missing".into(),
        })?;
    require_manifest_plan(item, &manifest)?;
    if !manifest.embedding_work_item_ids.contains(&item.base.id) {
        anyhow::bail!("Source embedding work was not declared");
    }
    let revision_id = RecordId::from_str(
        item.payload
            .get("revision_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Source embedding revision missing"))?,
    )?;
    if !manifest
        .documents
        .iter()
        .any(|mapping| mapping.doc_revision_id == revision_id)
    {
        anyhow::bail!("Source embedding revision is not declared");
    }
    let authority = SourcePublicationManifestRepository::new(storage.clone())
        .prepare_embedding_write(
            &item.base.space_id,
            &publication,
            &manifest.configuration,
            &elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            &revision_id,
        )
        .await?;
    let context = ProviderDispatchContext::new(
        SpaceConfigRepository::new(storage.clone()),
        item.base.space_id.clone(),
    );
    let binding = context
        .repository
        .authorize_model_operation(&item.base.space_id, ModelOperation::DocumentEmbedding, None)
        .await?;
    if manifest.embedding_profile.as_ref() != Some(&binding.vector_partition()?) {
        return Err(
            PipelineError::ConfigError("Required source embedding profile changed".into()).into(),
        );
    }
    let (_, provider, provenance) = context.embedding(ModelOperation::DocumentEmbedding).await?;
    if provenance.config != manifest.configuration {
        return Err(WorkFailure {
            class: FailureClass::ConfigurationBlocked,
            message: "Embedding binding differs from original source configuration".into(),
        }
        .into());
    }
    let worker = EmbedWorker::new(
        DocNodeRepository::new(storage.clone()),
        DocRevisionRepository::new(storage.clone()),
        elephant_storage::NodeEmbeddingRepository::new(
            storage.clone(),
            elephant_embeddings::PartitionManager::new(),
        ),
        provider,
    )
    .with_source_authority(authority);
    let nodes: Vec<_> = manifest
        .embeddings
        .iter()
        .filter(|expected| expected.revision_id == revision_id)
        .map(|expected| expected.node_id.clone())
        .collect();
    if nodes.is_empty() {
        worker.embed_exact_nodes(&item.base.space_id, &[]).await?;
    }
    let mut count = 0;
    for batch in nodes.chunks(128) {
        let result = worker.embed_exact_nodes(&item.base.space_id, batch).await?;
        if result.chunks_processed != batch.len() {
            anyhow::bail!("Source embedding batch did not cover its expected set");
        }
        count += result.chunks_processed;
    }
    let mut metrics = WorkRunMetrics::default();
    metrics
        .custom
        .insert("embedded_nodes".into(), serde_json::json!(count));
    Ok(metrics)
}

pub(super) enum InputRepair {
    Unavailable,
    RetryOriginal,
    Replaced,
}

/// Rebuild from original accepted input or replace obsolete plan work. A
/// superseded/retracted source cannot be revived by an old failed child.
pub(super) async fn schedule_input_repair(
    storage: &StorageClient,
    item: &WorkItem,
) -> anyhow::Result<InputRepair> {
    if schedule_configuration_recovery(storage, item).await? {
        return Ok(InputRepair::Replaced);
    }
    let Some(publication) = elephant_pipeline::source_queue::publication_from_work(item)? else {
        if matches!(item.kind.as_str(), TM_COMMIT_KIND | TM_RESOLVE_SLOT_KIND) {
            let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
            let mut base = item.base.clone();
            base.id = RecordId::new_v4(TableName::WorkItem);
            enqueue_idempotent(
                storage,
                WorkItem {
                    base,
                    kind: TM_RECOMPUTE_KIND.into(),
                    payload: serde_json::json!({"priority":"high","max_slots":1000}),
                    status: WorkItemStatus::Runnable,
                    priority: item.priority.saturating_add(1),
                    idempotency_key: format!(
                        "reprepare_tm:{}:{}",
                        item.base.id, attempt.generation
                    ),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                },
            )
            .await?;
            return Ok(InputRepair::Replaced);
        }
        return Ok(InputRepair::Unavailable);
    };
    let source = match accepted_source(storage, item).await {
        Ok(Some(source)) => source,
        Ok(None) => return Ok(InputRepair::Unavailable),
        Err(error) if classify_failure(&error) == FailureClass::StaleInput => {
            return Ok(InputRepair::Unavailable)
        }
        Err(error) => return Err(error),
    };
    let operation = SourcePublicationRepository::new(storage.clone())
        .operation(&item.base.space_id, &publication.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Accepted operation missing"))?;
    if operation.state == elephant_types::publication::OperationState::Completed {
        return Ok(InputRepair::Unavailable);
    }
    let manifest = SourcePublicationManifestRepository::new(storage.clone())
        .get(&item.base.space_id, &publication)
        .await?;
    let doc = materialize(storage, &source, item).await?;
    let mode = manifest
        .as_ref()
        .map_or(IngestMode::Graph, |manifest| manifest.mode);
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
    let mut base = source.base;
    base.id = RecordId::new_v4(TableName::WorkItem);
    let mut payload = serde_json::json!({"_publication":publication,"doc_id":doc.base.id.to_string(),"source_id":publication.source_id.to_string(),"ingest_mode":mode.to_string(),"repair_for":item.base.id.to_string()});
    if let Some(manifest) = &manifest {
        payload[METADATA_IDS] = serde_json::to_value(
            manifest
                .metadata_artifact_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
    } else if let Some(ids) = item.payload.get(METADATA_IDS) {
        payload[METADATA_IDS] = ids.clone();
    }
    let mut repair = WorkItem {
        base,
        kind: RENDER_DOC_KIND.into(),
        payload,
        status: WorkItemStatus::Runnable,
        priority: item.priority.saturating_add(10),
        idempotency_key: format!("repair_input:{}:{}", item.base.id, attempt.generation),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    elephant_pipeline::source_queue::inherit_source_context(item, &mut repair)?;
    enqueue_idempotent(storage, repair).await?;
    Ok(InputRepair::RetryOriginal)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::super::{
        enqueue_reprocess_resolution, run_on_dedicated_stack, Arc, BaseFields,
        ConflictGroupRepository, Duration, Link, ReprocessJobRepository,
    };
    use super::*;
    use elephant_storage::source_publication::AcceptSourceRevision;
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::source::{ManualDocTemplate, SourceType};

    async fn fixture(storage: &StorageClient) -> Source {
        Box::pin(elephant_migrations::apply_schema(storage))
            .await
            .expect("explicit complete source fixture schema");
        let config=serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"quiet-business","created_by":"fixture",
            "created_at":Utc::now(),"updated_at":Utc::now(),"deleted_at":null,
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"fixture","embedding_dims":8,
            "processing":{"generation":1,"index_generation":0,"policy_id":"fixture-no-model","connections":{},"profiles":{},"bindings":{},"active_embedding_profile_id":null,"pending_reindex":null}
        })).expect("space");
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("explicit space");
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "quiet-business".into(),
                "fixture".into(),
            ),
            SourceType::Manual,
            "original".into(),
        );
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "First".into(),
                text: "Immutable original text.".into(),
                sidecar_text: None,
                media_attachments: Vec::new(),
                rendition_key: "text-v1".into(),
                metadata: serde_json::json!({}),
            })
            .expect("template");
        source
    }

    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn accepted(
        storage: &StorageClient,
        source: Source,
        revision: &str,
        expected: SourcePrecondition,
    ) -> (PublicationFence, WorkItem) {
        accepted_with_key(
            storage,
            source,
            revision,
            expected,
            &format!("accept-{revision}"),
        )
        .await
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn accepted_with_key(
        storage: &StorageClient,
        source: Source,
        revision: &str,
        expected: SourcePrecondition,
        key: &str,
    ) -> (PublicationFence, WorkItem) {
        let repository = SourcePublicationRepository::new(storage.clone());
        let authorized = repository
            .authorization_snapshot(&source.base.space_id, &source.base.id)
            .await
            .expect("actual authorized fixture pre-state");
        let receipt = repository
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: revision.into(),
                expected,
                idempotency_key: key.into(),
                request_fingerprint: format!("request-{revision}"),
                reactivate: false,
                recreate_after_erasure: false,
                authorized,
                authorized_document: None,
            })
            .await
            .expect("accept");
        let fence = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: revision.into(),
            operation_id: receipt.operation_id,
        };
        let configuration = SpaceConfigRepository::new(storage.clone())
            .resolve_space("quiet-business")
            .await
            .expect("explicit configuration")
            .token;
        let id = enqueue_source_plan(storage, &source, &fence, &configuration, "fixture")
            .await
            .expect("enqueue");
        let work = WorkItemRepository::new(storage.clone())
            .get("quiet-business", &id)
            .await
            .expect("get")
            .expect("item");
        (fence, work)
    }

    #[test]
    fn source_wait_codes_come_only_from_exact_typed_causes() {
        let budget = anyhow::Error::new(PipelineError::BudgetExceeded("private amount".into()))
            .context("source stage");
        assert_eq!(source_failure_reason(&budget), Some("awaits_budget"));
        let index = anyhow::Error::new(PipelineError::InvalidState(
            "Conflict: source_publication_incomplete:required_index_missing".into(),
        ));
        assert_eq!(source_failure_reason(&index), Some("awaits_source_index"));
        let forged =
            anyhow::anyhow!("source_publication_incomplete:required_index_missing budget exceeded");
        assert_eq!(source_failure_reason(&forged), None);
        let wrong_type = anyhow::Error::new(PipelineError::SecurityViolation(
            "source_publication_incomplete:required_index_missing".into(),
        ));
        assert_eq!(source_failure_reason(&wrong_type), None);
    }

    #[tokio::test]
    async fn typed_budget_wait_is_durable_without_finishing_or_spending_an_attempt() {
        use elephant_types::publication::OperationState;
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.expect("embedded")
            } else {
                StorageClient::new_memory().await.expect("memory")
            };
            let source = Box::pin(fixture(&storage)).await;
            let (fence, item) = Box::pin(accepted(
                &storage,
                source,
                "budget-r1",
                SourcePrecondition::Absent,
            ))
            .await;
            let repository = WorkItemRepository::new(storage.clone());
            let claim = repository
                .claim_with_lease(
                    &item.base.space_id,
                    &item.base.id,
                    "budget-fixture",
                    Utc::now() + Duration::seconds(60),
                )
                .await
                .expect("claim")
                .expect("owned");
            assert!(!Box::pin(begin_operation(&storage, &claim))
                .await
                .expect("begin"));
            let error = anyhow::Error::new(PipelineError::BudgetExceeded(
                "private amount and source detail".into(),
            ))
            .context("triage_source failed");
            Box::pin(record_preflight_failure(
                &storage,
                &claim,
                &error,
                &WorkQueueWorkerConfig::default(),
            ))
            .await
            .expect("record budget refusal");
            let operation = SourcePublicationRepository::new(storage.clone())
                .operation(&item.base.space_id, &fence.operation_id)
                .await
                .expect("read")
                .expect("operation");
            assert_eq!(operation.state, OperationState::FailedRetryable);
            assert_eq!(operation.error_code.as_deref(), Some("awaits_budget"));
            assert!(operation.completed_at.is_none());
            assert!(operation.published_revision.is_none());
            let mut finalizer = item.clone();
            finalizer.base.id = RecordId::new_v4(TableName::WorkItem);
            finalizer.kind =
                elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND.into();
            finalizer.idempotency_key = "budget-wait-finalizer".into();
            let finalizer = repository.create(finalizer).await.expect("finalizer queue");
            let finalizer = repository
                .claim_with_lease(
                    &item.base.space_id,
                    &finalizer.base.id,
                    "finalizer",
                    Utc::now() + Duration::seconds(60),
                )
                .await
                .expect("claim finalizer")
                .expect("owned finalizer");
            assert!(!Box::pin(begin_operation(&storage, &finalizer))
                .await
                .expect("poll does not reset child wait"));
            Box::pin(record_operation_failure_with_reason(
                &storage,
                &finalizer,
                FailureClass::PendingDependencies,
                RetryAction::Blocked { delay_secs: 5 },
                Some("awaits_source_bundle"),
            ))
            .await
            .expect("pending poll");
            assert_eq!(
                SourcePublicationRepository::new(storage.clone())
                    .operation(&item.base.space_id, &fence.operation_id)
                    .await
                    .expect("read retained wait")
                    .expect("wait")
                    .error_code
                    .as_deref(),
                Some("awaits_budget")
            );
            let queued = repository
                .get(&item.base.space_id, &item.base.id)
                .await
                .expect("read queue")
                .expect("queue");
            drop(repository);
            assert_eq!(queued.status, WorkItemStatus::Runnable);
            assert_eq!(queued.attempts, claim.attempts - 1);
            assert!(
                WorkRunRepository::new(storage.clone())
                    .find_by_work_item(&item.base.id)
                    .await
                    .expect("runs")
                    .is_empty(),
                "preflight refusal must not fabricate a provider run"
            );
            assert!(
                Box::pin(record_preflight_failure(
                    &storage,
                    &claim,
                    &error,
                    &WorkQueueWorkerConfig::default()
                ))
                .await
                .is_err(),
                "retired original claim cannot update state again"
            );
            drop(storage);
        }
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn metadata_stage_identity_binds_original_plan_and_producer() {
        let storage = StorageClient::new_embedded().await.expect("embedded");
        let source = fixture(&storage).await;
        let (_, original) = accepted(&storage, source, "r1", SourcePrecondition::Absent).await;
        let run = RecordId::new_v4(TableName::WorkRun);
        let first = metadata_stage_id(&original, &run).expect("stage identity");
        assert_eq!(first, metadata_stage_id(&original, &run).expect("replay"));
        assert_ne!(
            first,
            metadata_stage_id(&original, &RecordId::new_v4(TableName::WorkRun))
                .expect("new attempt")
        );
        let mut later = original.clone();
        later
            .payload
            .as_object_mut()
            .expect("object")
            .remove(elephant_pipeline::source_queue::SOURCE_CONFIGURATION_KEY);
        later
            .payload
            .as_object_mut()
            .expect("object")
            .remove(elephant_pipeline::source_queue::SOURCE_PLAN_KEY);
        let mut configuration = configuration_from_work(&original)
            .expect("capture")
            .expect("present");
        configuration.generation += 1;
        pin_source_configuration(&mut later, &configuration).expect("new plan context");
        assert_ne!(
            first,
            metadata_stage_id(&later, &run).expect("new plan identity")
        );
        assert!(metadata_stage_id(&original, &RecordId::new_v4(TableName::DocRevision)).is_err());
    }

    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn configuration_recovery_contract(storage: StorageClient) {
        let source = fixture(&storage).await;
        let (publication, item) =
            accepted(&storage, source.clone(), "r1", SourcePrecondition::Absent).await;
        let repository = WorkItemRepository::new(storage.clone());
        let claimed = repository
            .claim_with_lease(
                &item.base.space_id,
                &item.base.id,
                "old-plan",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("owned");
        assert!(!begin_operation(&storage, &claimed)
            .await
            .expect("original starts"));
        let configuration_repository = SpaceConfigRepository::new(storage.clone());
        let observed = configuration_repository
            .get_by_space_id(&item.base.space_id)
            .await
            .expect("load")
            .expect("exists");
        let mut successor = observed.clone();
        successor
            .processing
            .as_mut()
            .expect("processing")
            .generation += 1;
        configuration_repository
            .replace_authorized(&observed, successor)
            .await
            .expect("legitimate config change");
        let current = configuration_repository
            .resolve_space(&item.base.space_id)
            .await
            .expect("resolve")
            .token;
        let failure = begin_operation(&storage, &claimed)
            .await
            .expect_err("old plan cannot recapture config");
        assert_eq!(
            classify_failure(&failure),
            FailureClass::ConfigurationBlocked
        );
        // No Runnable work is needed for discovery: only the original Running
        // item existed before the idle recovery pass.
        recover_sources(&storage)
            .await
            .expect("quiet space recovery");
        assert!(schedule_configuration_recovery(&storage, &claimed)
            .await
            .expect("durable replay"));
        let next_id = enqueue_source_plan(&storage, &source, &publication, &current, "fixture")
            .await
            .expect("same successor identity");
        assert_ne!(item.base.id, next_id);
        let next = repository
            .get(&item.base.space_id, &next_id)
            .await
            .expect("get")
            .expect("new chain");
        assert_eq!(configuration_from_work(&next).expect("pin"), Some(current));
        assert_eq!(
            publication_from_work(&next).expect("source"),
            Some(publication.clone())
        );
        assert_eq!(next.status, WorkItemStatus::Runnable);
        assert!(
            next.payload.get(PREDECESSOR).is_none(),
            "new plan is independent of cancelled predecessors"
        );
        let original = repository
            .get(&item.base.space_id, &item.base.id)
            .await
            .expect("get original")
            .expect("original");
        assert_eq!(
            original.payload, claimed.payload,
            "recovery never changes old authority or policy"
        );
        let head = SourcePublicationRepository::new(storage.clone())
            .head(&item.base.space_id, &publication.source_id)
            .await
            .expect("head")
            .expect("accepted");
        assert_eq!(head.generation, publication.generation);
        assert_eq!(head.operation_id, publication.operation_id);
        assert_eq!(head.desired_revision.as_deref(), Some("r1"));
        repository
            .update_running_for_worker(
                &item.base.space_id,
                &item.base.id,
                claimed.leased_by.as_deref().expect("original token"),
                WorkItemStatus::Cancelled,
                Some("configuration replaced".into()),
            )
            .await
            .expect("cancel original");
        let next = repository
            .claim_with_lease(
                &next.base.space_id,
                &next.base.id,
                "new-plan",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim successor")
            .expect("owned");
        assert!(!begin_operation(&storage, &next)
            .await
            .expect("new plan begins"));
        let original_metadata = metadata_bundle_id(
            &publication,
            &configuration_from_work(&claimed)
                .expect("original pin")
                .expect("original"),
        );
        let new_metadata = metadata_bundle_id(
            &publication,
            &configuration_from_work(&next)
                .expect("next pin")
                .expect("next"),
        );
        assert_ne!(original_metadata, new_metadata);
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_quiet_space_recovers_new_configuration_without_reaccepting_source() {
        configuration_recovery_contract(StorageClient::new_embedded().await.expect("embedded"))
            .await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_quiet_space_recovers_new_configuration_without_reaccepting_source() {
        configuration_recovery_contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn unversioned_acceptance_bootstrap_only_enqueues_pinned_work() {
        let storage = StorageClient::new_embedded().await.expect("embedded");
        let source = fixture(&storage).await;
        let (publication, pinned) =
            accepted(&storage, source.clone(), "r1", SourcePrecondition::Absent).await;
        let bootstrap_id = elephant_pipeline::source_queue::enqueue_accepted_source(
            &storage,
            &source,
            &publication,
            "fixture",
        )
        .await
        .expect("bootstrap");
        let repository = WorkItemRepository::new(storage.clone());
        let bootstrap = repository
            .claim_with_lease(
                "quiet-business",
                &bootstrap_id,
                "bootstrap",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("owned");
        assert!(begin_operation(&storage, &bootstrap)
            .await
            .expect("only enqueue"));
        assert!(configuration_from_work(&bootstrap)
            .expect("unchanged original")
            .is_none());
        assert_ne!(bootstrap.base.id, pinned.base.id);
        let operation = SourcePublicationRepository::new(storage)
            .operation("quiet-business", &publication.operation_id)
            .await
            .expect("operation")
            .expect("accepted");
        assert_ne!(
            operation.state,
            elephant_types::publication::OperationState::Completed
        );
    }

    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn obsolete_outcome_contract(storage: StorageClient) {
        let source = fixture(&storage).await;
        let (publication, item) =
            accepted(&storage, source.clone(), "r1", SourcePrecondition::Absent).await;
        let repository = WorkItemRepository::new(storage.clone());
        let claim = repository
            .claim_with_lease(
                &item.base.space_id,
                &item.base.id,
                "old-source",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("owned");
        assert!(!begin_operation(&storage, &claim).await.expect("starts"));
        record_operation_failure(&storage, &claim, FailureClass::Terminal, RetryAction::Fail)
            .await
            .expect("terminal receipt");
        let failure = begin_operation(&storage, &claim)
            .await
            .expect_err("cannot restart terminal source");
        assert_eq!(classify_failure(&failure), FailureClass::Terminal);
        record_operation_failure(&storage, &claim, FailureClass::Terminal, RetryAction::Fail)
            .await
            .expect("terminal bookkeeping does not strand queue");
        let (next_publication, _) = accepted(
            &storage,
            source,
            "r2",
            SourcePrecondition::Generation(publication.generation),
        )
        .await;
        record_operation_failure(
            &storage,
            &claim,
            FailureClass::StaleInput,
            RetryAction::RepairInput,
        )
        .await
        .expect("superseded bookkeeping leaves new outcome untouched");
        repository
            .update_running_for_worker(
                &item.base.space_id,
                &item.base.id,
                claim.leased_by.as_deref().expect("original token"),
                WorkItemStatus::Failed,
                Some("original source no longer accepts work".into()),
            )
            .await
            .expect("obsolete queue item can finish");
        let old_work = repository
            .get(&item.base.space_id, &item.base.id)
            .await
            .expect("get")
            .expect("retained");
        assert_eq!(old_work.status, WorkItemStatus::Failed);
        let next = SourcePublicationRepository::new(storage)
            .operation(&item.base.space_id, &next_publication.operation_id)
            .await
            .expect("new receipt")
            .expect("accepted");
        assert_eq!(
            next.state,
            elephant_types::publication::OperationState::Accepted
        );
        assert!(next.error_code.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_obsolete_source_outcomes_do_not_strand_running_queue_items() {
        obsolete_outcome_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_obsolete_source_outcomes_do_not_strand_running_queue_items() {
        obsolete_outcome_contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn immutable_contract(storage: StorageClient) {
        let source = fixture(&storage).await;
        let (first, work) =
            accepted(&storage, source.clone(), "r1", SourcePrecondition::Absent).await;
        let captured = accepted_source(&storage, &work)
            .await
            .expect("captured")
            .expect("source");
        elephant_storage::source_intake::stage_source_revision(&storage, &captured, &first)
            .await
            .expect("public intake stages original accepted document");
        let old_doc = materialize(&storage, &captured, &work)
            .await
            .expect("original doc");
        let mut replacement = source;
        let mut template = replacement
            .manual_doc_template()
            .expect("template")
            .expect("present");
        template.text = "Replacement text.".into();
        replacement
            .set_manual_doc_template(&template)
            .expect("replacement template");
        replacement.hash = "replacement".into();
        let (_, new_work) = accepted(
            &storage,
            replacement,
            "r2",
            SourcePrecondition::Generation(first.generation),
        )
        .await;
        assert!(
            accepted_source(&storage, &work).await.is_err(),
            "old source fence cannot read newer input"
        );
        let current = accepted_source(&storage, &new_work)
            .await
            .expect("current")
            .expect("source");
        let new_doc = materialize(&storage, &current, &new_work)
            .await
            .expect("replacement doc");
        assert_ne!(old_doc.base.id, new_doc.base.id);
        assert_ne!(old_doc.rendition_key, new_doc.rendition_key);
        let replay = materialize(&storage, &current, &new_work)
            .await
            .expect("same accepted document replay");
        assert_eq!(replay.base.id, new_doc.base.id);
        assert_eq!(replay.rendition_key, new_doc.rendition_key);
        assert_eq!(replay.text, new_doc.text);
        assert_eq!(
            current
                .manual_doc_template()
                .expect("immutable template")
                .expect("present")
                .rendition_key,
            "text-v1",
            "physical rendition does not rewrite immutable accepted input"
        );
        assert_eq!(
            DocRepository::new(storage.clone())
                .get("quiet-business", &old_doc.base.id)
                .await
                .expect("old")
                .expect("retained")
                .text,
            "Immutable original text."
        );
        recover_sources(&storage).await.expect("all-space repair");
        recover_sources(&storage).await.expect("idempotent repair");
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_original_fence_and_immutable_docs_survive_replacement() {
        immutable_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_original_fence_and_immutable_docs_survive_replacement() {
        immutable_contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn aba_materialization_contract(storage: StorageClient) {
        let original = fixture(&storage).await;
        let (first, first_work) = accepted(
            &storage,
            original.clone(),
            "revision-a",
            SourcePrecondition::Absent,
        )
        .await;
        let first_source = accepted_source(&storage, &first_work)
            .await
            .expect("read first")
            .expect("accepted");
        let first_doc = materialize(&storage, &first_source, &first_work)
            .await
            .expect("first physical document");
        let owner = SourcePublicationRepository::new(storage.clone());
        let immutable_a = owner
            .revision("quiet-business", &first.source_id, "revision-a")
            .await
            .expect("revision")
            .expect("immutable input");
        let mut replacement = original.clone();
        let mut template = replacement
            .manual_doc_template()
            .expect("template")
            .expect("manual input");
        template.text = "Intermediate B text.".into();
        replacement
            .set_manual_doc_template(&template)
            .expect("B template");
        replacement.hash = "revision-b-hash".into();
        let (second, second_work) = accepted(
            &storage,
            replacement,
            "revision-b",
            SourcePrecondition::Generation(first.generation.clone()),
        )
        .await;
        let second_source = accepted_source(&storage, &second_work)
            .await
            .expect("read second")
            .expect("accepted");
        let second_doc = materialize(&storage, &second_source, &second_work)
            .await
            .expect("second physical document");
        let (third, third_work) = accepted_with_key(
            &storage,
            original,
            "revision-a",
            SourcePrecondition::Generation(second.generation),
            "explicit-fresh-revert-key",
        )
        .await;
        assert_ne!(third.operation_id, first.operation_id);
        assert_ne!(third.generation, first.generation);
        assert_eq!(third.revision_id, first.revision_id);
        assert_ne!(third_work.base.id, first_work.base.id);
        assert!(accepted_source(&storage, &first_work).await.is_err());
        let third_source = accepted_source(&storage, &third_work)
            .await
            .expect("read explicit revert")
            .expect("accepted");
        elephant_storage::source_intake::stage_source_revision(&storage, &third_source, &third)
            .await
            .expect("public intake stages fresh repeated revision");
        let third_doc = materialize(&storage, &third_source, &third_work)
            .await
            .expect("third physical document");
        assert_ne!(third_doc.base.id, first_doc.base.id);
        assert_ne!(third_doc.base.id, second_doc.base.id);
        assert_ne!(first_doc.rendition_key, second_doc.rendition_key);
        assert_ne!(third_doc.rendition_key, first_doc.rendition_key);
        assert_ne!(third_doc.rendition_key, second_doc.rendition_key);
        let replay = materialize(&storage, &third_source, &third_work)
            .await
            .expect("ABA acceptance replay");
        assert_eq!(replay.base.id, third_doc.base.id);
        assert_eq!(replay.rendition_key, third_doc.rendition_key);
        assert_eq!(third_doc.text, first_doc.text);
        assert_eq!(
            third_doc.metadata["_publication"],
            serde_json::to_value(&third).expect("original fence")
        );
        assert_eq!(
            serde_json::to_value(
                owner
                    .revision("quiet-business", &first.source_id, "revision-a")
                    .await
                    .expect("retained revision")
            )
            .expect("retained immutable revision fields"),
            serde_json::to_value(Some(immutable_a)).expect("original immutable revision fields")
        );
        assert_eq!(
            DocRepository::new(storage.clone())
                .get("quiet-business", &first_doc.base.id)
                .await
                .expect("retained doc")
                .expect("present")
                .text,
            "Immutable original text."
        );
        recover_sources(&storage)
            .await
            .expect("quiet-space ABA recovery");
        assert_eq!(
            owner
                .head("quiet-business", &third.source_id)
                .await
                .expect("head")
                .expect("present")
                .operation_id,
            third.operation_id
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_fresh_aba_acceptance_materializes_distinct_physical_documents() {
        aba_materialization_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_fresh_aba_acceptance_materializes_distinct_physical_documents() {
        aba_materialization_contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    async fn claim_with_original_run(
        storage: &StorageClient,
        item: &WorkItem,
    ) -> (WorkItem, elephant_types::records::WorkRun) {
        let parent = WorkItemRepository::new(storage.clone())
            .claim_with_lease(
                &item.base.space_id,
                &item.base.id,
                "fixture-worker",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("owned");
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(&parent)
            .expect("original attempt");
        let mut metrics = WorkRunMetrics::default();
        metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&fence).expect("fence"),
        );
        let mut base = parent.base.clone();
        base.id = RecordId::new_v4(TableName::WorkRun);
        let run = WorkRunRepository::new(storage.clone())
            .create(elephant_types::records::WorkRun {
                base,
                work_item: Link::new(parent.base.id.clone()),
                attempt: parent.attempts,
                started_at: Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics,
                worker_id: fence.lease_token,
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("original durable execution receipt");
        (parent, run)
    }

    #[allow(
        clippy::too_many_lines,
        clippy::significant_drop_tightening,
        reason = "Exercise the real failure and recovery entry points with retained source, job and attempt owners."
    )]
    async fn reprocess_terminal_failure_contract(storage: StorageClient, archive_gap: bool) {
        use elephant_storage::canonical_commit::{CanonicalRecord, CanonicalRepository};
        use elephant_types::records::reprocess::{
            ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage,
            SelectionCriteria,
        };
        let source = Box::pin(fixture(&storage)).await;
        let source_id = source.base.id.clone();
        Box::pin(accepted(
            &storage,
            source,
            "unpublished",
            SourcePrecondition::Absent,
        ))
        .await;
        let current = elephant_storage::SourceRepository::new(storage.clone())
            .get("quiet-business", &source_id)
            .await
            .expect("source")
            .expect("accepted");
        let canonical = CanonicalRepository::new(storage.clone());
        assert!(
            matches!(
                Box::pin(canonical.capture_maintenance_support(
                    "quiet-business",
                    &[CanonicalRecord::Source(Box::new(current.clone()))],
                )).await,
                Err(elephant_storage::StorageError::Conflict(code)) if code == "maintenance_source_not_published"
            ),
            "unpublished intake is not graph support"
        );
        let jobs = ReprocessJobRepository::new(storage.clone());
        let items = WorkItemRepository::new(storage.clone());
        let mut base = current.base.clone();
        base.id = RecordId::new_v4(TableName::ReprocessJob);
        base.provenance.sources = vec![source_id.clone()];
        let job = jobs
            .create_with_work(ReprocessJob {
                base,
                name: "observed terminal refusal".into(),
                selection_criteria: SelectionCriteria::default(),
                target_stages: vec![ReprocessTargetStage::ReMaintain],
                status: ReprocessStatus::Pending,
                progress: ReprocessProgress::default(),
                priority: 10,
                budget_limit_euros: None,
                max_sources_per_day: None,
                started_at: None,
                completed_at: None,
                error: None,
                reason: None,
            })
            .await
            .expect("job and original handoff");
        let id = jobs.ensure_work(&job).await.expect("work");
        let item = items
            .get("quiet-business", &id)
            .await
            .expect("read")
            .expect("work");
        let (item, mut run) = claim_with_original_run(&storage, &item).await;
        run.completed_at = Some(Utc::now());
        run.error = Some("maintenance_source_not_published".into());
        let run_id = run.base.id.clone();
        WorkRunRepository::new(storage.clone())
            .update("quiet-business", &run_id, run)
            .await
            .expect("actual failed execution persisted before disposition");
        if archive_gap {
            WorkQueue::new(storage.clone(), WorkQueueConfig::default())
                .for_attempt(&item)
                .expect("original lease")
                .fail(
                    "quiet-business",
                    &id,
                    "maintenance_source_not_published".into(),
                )
                .await
                .expect("queue closed before simulated crash");
            assert_eq!(
                jobs.get("quiet-business", &job.base.id)
                    .await
                    .expect("read")
                    .expect("job")
                    .status,
                ReprocessStatus::Pending
            );
            let archived = elephant_storage::work_retention::archive_terminal_work(
                &storage,
                Utc::now() + Duration::seconds(1),
                100,
            )
            .await
            .expect("retained history");
            assert_eq!(archived.items_archived, 1);
            assert_eq!(archived.runs_archived, 1);
            Box::pin(recover_sources(&storage))
                .await
                .expect("real recovery closes archived crash gap");
        } else {
            Box::pin(apply_failed_disposition(
                &storage,
                &item,
                &anyhow::anyhow!("maintenance_source_not_published"),
                FailureClass::Transient,
                RetryAction::Fail,
            ))
            .await
            .expect("real exhausted-retry disposition also closes public job");
        }
        let after = jobs
            .get("quiet-business", &job.base.id)
            .await
            .expect("read")
            .expect("job");
        assert_eq!(after.status, ReprocessStatus::Failed);
        assert_eq!(
            after.progress, job.progress,
            "refusal does not invent corpus progress"
        );
        assert_eq!(after.error.as_deref(), Some("reprocess_work_failed"));
        assert_eq!(
            items
                .get("quiet-business", &id)
                .await
                .expect("read")
                .expect("retained")
                .status,
            WorkItemStatus::Failed
        );
        assert!(
            matches!(
                Box::pin(canonical.capture_maintenance_support(
                    "quiet-business",
                    &[CanonicalRecord::Source(Box::new(current))],
                )).await,
                Err(elephant_storage::StorageError::Conflict(code)) if code == "maintenance_source_not_published"
            ),
            "job bookkeeping cannot certify an unpublished revision"
        );
        let head = SourcePublicationRepository::new(storage.clone())
            .head("quiet-business", &source_id)
            .await
            .expect("head")
            .expect("accepted");
        assert!(head.published_revision.is_none());
        Box::pin(recover_sources(&storage))
            .await
            .expect("idempotent recovery");
        assert_eq!(
            jobs.get("quiet-business", &job.base.id)
                .await
                .expect("read")
                .expect("job"),
            after
        );
    }

    #[tokio::test]
    async fn executed_reprocess_failure_closes_job_without_publishing_source() {
        for embedded in [true, false] {
            Box::pin(reprocess_terminal_failure_contract(
                if embedded {
                    StorageClient::new_embedded().await.expect("embedded")
                } else {
                    StorageClient::new_memory().await.expect("memory")
                },
                false,
            ))
            .await;
        }
    }

    #[tokio::test]
    async fn source_recovery_closes_archived_reprocess_failure_gap() {
        for embedded in [true, false] {
            Box::pin(reprocess_terminal_failure_contract(
                if embedded {
                    StorageClient::new_embedded().await.expect("embedded")
                } else {
                    StorageClient::new_memory().await.expect("memory")
                },
                true,
            ))
            .await;
        }
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn pending_predecessor_cannot_start_successor() {
        let storage = StorageClient::new_embedded().await.expect("embedded");
        let source = fixture(&storage).await;
        let (_, parent) = accepted(&storage, source, "r1", SourcePrecondition::Absent).await;
        let (parent, mut run) = claim_with_original_run(&storage, &parent).await;
        let mut child = parent.clone();
        child.base.id = RecordId::new_v4(TableName::WorkItem);
        child.idempotency_key = "actual-dependent-child".into();
        child.status = WorkItemStatus::Runnable;
        child.attempts = 0;
        child.leased_by = None;
        child.lease_expires_at = None;
        child.run_after = None;
        let child = elephant_storage::work_producer_repository::WorkProducerRepository::new(
            storage.clone(),
        )
        .enqueue_successor(&parent, &run, child, true)
        .await
        .expect("actual private successor");
        let error = require_predecessor(&storage, &child)
            .await
            .expect_err("pending");
        assert_eq!(classify_failure(&error), FailureClass::PendingDependencies);
        WorkItemRepository::new(storage.clone())
            .update_running_for_worker(
                &parent.base.space_id,
                &parent.base.id,
                parent.leased_by.as_deref().expect("original token"),
                WorkItemStatus::Done,
                None,
            )
            .await
            .expect("original fenced completion")
            .expect("still owned");
        assert_eq!(
            classify_failure(
                &require_predecessor(&storage, &child)
                    .await
                    .expect_err("work completion alone cannot manufacture successful run")
            ),
            FailureClass::PendingDependencies
        );
        run.success = true;
        run.completed_at = Some(Utc::now());
        WorkRunRepository::new(storage.clone())
            .update(&run.base.space_id, &run.base.id, run.clone())
            .await
            .expect("persist actual original outcome");
        require_predecessor(&storage, &child)
            .await
            .expect("successful original predecessor");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn reprocess_resolution_runs_while_its_parent_waits_for_progress() {
        use elephant_types::records::reprocess::{
            ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage,
            SelectionCriteria,
        };
        let storage = StorageClient::new_embedded().await.expect("embedded");
        fixture(&storage).await;
        let repository = WorkItemRepository::new(storage.clone());
        let jobs = ReprocessJobRepository::new(storage.clone());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "quiet-business".into(),
            "fixture".into(),
        );
        base.security.level = 2;
        let job = jobs
            .create_with_work(ReprocessJob {
                base,
                name: "actual coordinator".into(),
                selection_criteria: SelectionCriteria::default(),
                target_stages: vec![ReprocessTargetStage::ReMaintain],
                status: ReprocessStatus::Pending,
                progress: ReprocessProgress::default(),
                priority: 10,
                budget_limit_euros: Some(1.0),
                max_sources_per_day: None,
                started_at: None,
                completed_at: None,
                error: None,
                reason: None,
            })
            .await
            .expect("durable original restricted job");
        let id = jobs.ensure_work(&job).await.expect("actual handoff");
        let item = repository
            .get(&job.base.space_id, &id)
            .await
            .expect("read")
            .expect("job work");
        let (parent, run) = claim_with_original_run(&storage, &item).await;
        let mut base = parent.base.clone();
        base.id = RecordId::new_v4(TableName::ConflictGroup);
        let group = ConflictGroupRepository::new(storage.clone())
            .create(elephant_types::records::ConflictGroup {
                base,
                slot_key: "actual-conflict-slot".into(),
                slot_type: "attribute".into(),
                claim_ids: vec![],
                resolved: false,
                resolution_rationale: None,
                detected_at: Utc::now(),
                resolved_at: None,
                adjudicated_claim_set_hash: None,
                adjudicated_at: None,
            })
            .await
            .expect("actual conflict group");
        let child = enqueue_reprocess_resolution(&storage, &parent, &run.base.id, &group.base.id)
            .await
            .expect("durable child");
        let replay = enqueue_reprocess_resolution(&storage, &parent, &run.base.id, &group.base.id)
            .await
            .expect("idempotent replay");
        assert_eq!(child.base.id, replay.base.id);
        assert_eq!(child.base.security, parent.base.security);
        assert!(child.payload.get(PREDECESSOR).is_none());
        assert!(child
            .payload
            .get(elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY)
            .is_none());
        repository
            .requeue_configuration_blocked(
                &parent.base.space_id,
                &parent.base.id,
                parent.leased_by.as_deref().expect("token"),
                30,
                "child progress pending".into(),
            )
            .await
            .expect("parent waits")
            .expect("original ownership");
        require_predecessor(&storage, &child)
            .await
            .expect("child is independent of waiting parent completion");
        assert_eq!(child.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    async fn cancelling_dedicated_stack_drops_its_producer() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = dropped.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_on_dedicated_stack(
            "cancellable-fixture",
            1024 * 1024,
            async move {
                let _drop = Dropped(signal);
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            },
        ));
        started_rx.await.expect("thread entered");
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled producer dropped");
    }
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn operation_outcome_contract(storage: StorageClient) {
        use elephant_types::publication::OperationState;
        let source = fixture(&storage).await;
        let (fence, item) = accepted(&storage, source, "r1", SourcePrecondition::Absent).await;
        let repository = WorkItemRepository::new(storage.clone());
        let claimed = repository
            .claim_with_lease(
                &item.base.space_id,
                &item.base.id,
                "fixture",
                Utc::now() + Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("owned");
        assert!(!begin_operation(&storage, &claimed)
            .await
            .expect("processing"));
        record_operation_failure(
            &storage,
            &claimed,
            FailureClass::ConfigurationBlocked,
            RetryAction::Blocked { delay_secs: 120 },
        )
        .await
        .expect("blocked receipt");
        let operation = SourcePublicationRepository::new(storage.clone())
            .operation(&item.base.space_id, &fence.operation_id)
            .await
            .expect("receipt")
            .expect("operation");
        assert_eq!(operation.state, OperationState::FailedRetryable);
        assert_eq!(
            operation.error_code.as_deref(),
            Some("configuration_blocked")
        );
        assert!(operation.completed_at.is_none());
        assert!(!begin_operation(&storage, &claimed)
            .await
            .expect("resume current original source"));
        repository
            .requeue_configuration_blocked(
                &item.base.space_id,
                &item.base.id,
                claimed.leased_by.as_deref().expect("token"),
                30,
                "blocked".into(),
            )
            .await
            .expect("release");
        assert!(
            record_operation_failure(
                &storage,
                &claimed,
                FailureClass::Terminal,
                RetryAction::Fail
            )
            .await
            .is_err(),
            "lost original attempt cannot overwrite source outcome"
        );
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_operation_blockage_is_durable_and_attempt_fenced() {
        operation_outcome_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_operation_blockage_is_durable_and_attempt_fenced() {
        operation_outcome_contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the regression owner lifetimes and unboxed future shape."
    )]
    async fn exhausted_operation_contract(storage: StorageClient, class: FailureClass) {
        use elephant_types::publication::OperationState;
        let source = fixture(&storage).await;
        let (publication, item) =
            accepted(&storage, source, "r1", SourcePrecondition::Absent).await;
        let repository = WorkItemRepository::new(storage.clone());
        let sources = SourcePublicationRepository::new(storage.clone());
        assert!(matches!(
            class,
            FailureClass::Transient | FailureClass::Timeout
        ));
        let expected_code = if class == FailureClass::Transient {
            "transient_failure"
        } else {
            "execution_timeout"
        };
        let mut old_claim: Option<WorkItem> = None;
        for attempt in 1..=3 {
            let claim = repository
                .claim_with_lease(
                    &item.base.space_id,
                    &item.base.id,
                    "bounded-retry-fixture",
                    Utc::now() + Duration::seconds(60),
                )
                .await
                .expect("claim")
                .expect("fresh attempt");
            assert_eq!(claim.attempts, attempt);
            assert!(!begin_operation(&storage, &claim).await.expect("begin"));
            if let Some(previous) = &old_claim {
                assert!(
                    record_operation_failure(&storage, previous, class, RetryAction::Fail,)
                        .await
                        .is_err(),
                    "a retired claim cannot finalize its successor's operation"
                );
                let current = sources
                    .operation(&item.base.space_id, &publication.operation_id)
                    .await
                    .expect("read current")
                    .expect("current receipt");
                assert_eq!(current.state, OperationState::Processing);
            }
            let action = retry_action(class, claim.attempts, 3, 10, 120);
            record_operation_failure(&storage, &claim, class, action)
                .await
                .expect("record selected disposition");
            let operation = sources
                .operation(&item.base.space_id, &publication.operation_id)
                .await
                .expect("read receipt")
                .expect("receipt");
            assert_eq!(operation.error_code.as_deref(), Some(expected_code));
            if attempt < 3 {
                assert_eq!(operation.state, OperationState::FailedRetryable);
                assert_eq!(
                    action,
                    RetryAction::Retry {
                        delay_secs: if attempt == 1 { 10 } else { 20 },
                    }
                );
                // The fixture checks the actual policy delay above and advances
                // the real queue immediately instead of sleeping for backoff.
                repository
                    .requeue_with_delay(
                        &item.base.space_id,
                        &item.base.id,
                        claim.leased_by.as_deref().expect("claim token"),
                        0,
                        Some(expected_code.into()),
                    )
                    .await
                    .expect("requeue")
                    .expect("original attempt transitioned");
                old_claim = Some(claim);
            } else {
                assert_eq!(action, RetryAction::Fail);
                assert_eq!(operation.state, OperationState::FailedTerminal);
                let error = begin_operation(&storage, &claim)
                    .await
                    .expect_err("exhausted publication cannot restart");
                assert_eq!(classify_failure(&error), FailureClass::Terminal);
                repository
                    .update_running_for_worker(
                        &item.base.space_id,
                        &item.base.id,
                        claim.leased_by.as_deref().expect("claim token"),
                        WorkItemStatus::Failed,
                        Some(expected_code.into()),
                    )
                    .await
                    .expect("terminal queue outcome");
                assert_eq!(
                    repository
                        .get(&item.base.space_id, &item.base.id)
                        .await
                        .expect("read work")
                        .expect("work")
                        .status,
                    WorkItemStatus::Failed
                );
            }
        }
        let head = sources
            .head(&item.base.space_id, &publication.source_id)
            .await
            .expect("head")
            .expect("original head");
        assert_eq!(head.operation_id, publication.operation_id);
        assert_eq!(head.generation, publication.generation);
        assert_eq!(head.desired_revision.as_deref(), Some("r1"));
        assert!(head.published_revision.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_exhausted_transient_receipt_is_terminal_and_fenced() {
        exhausted_operation_contract(
            StorageClient::new_embedded().await.expect("embedded"),
            FailureClass::Transient,
        )
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_exhausted_transient_receipt_is_terminal_and_fenced() {
        exhausted_operation_contract(
            StorageClient::new_memory().await.expect("memory"),
            FailureClass::Transient,
        )
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn embedded_exhausted_timeout_receipt_is_terminal_and_fenced() {
        exhausted_operation_contract(
            StorageClient::new_embedded().await.expect("embedded"),
            FailureClass::Timeout,
        )
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Preserve the original regression future type and execution shape."
    )]
    async fn surreal_exhausted_timeout_receipt_is_terminal_and_fenced() {
        exhausted_operation_contract(
            StorageClient::new_memory().await.expect("memory"),
            FailureClass::Timeout,
        )
        .await;
    }
}
