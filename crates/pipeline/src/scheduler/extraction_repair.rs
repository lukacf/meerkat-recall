//! Governed whole-proposal re-extraction. Existing run/artifact records retain
//! inputs and history; queue attempts, provider receipts and source fences remain
//! authoritative. This module grants no publication or provider permission.
use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use elephant_storage::{
    Repository, StorageClient, WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::publication::AttemptFence;
use elephant_types::records::work::{ArtifactStatus, WorkArtifact, WorkItem, WorkRun};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::execution_contract::{AttemptUsage, UsageBasis};
use super::extraction_work_kinds::{ExtractionArtifactData, KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE};
use super::grounding::{same_authorization_partition, GroundingLineage};
use super::source_stage::SourceStageContext;
use super::staged_commit::{validate_artifacts, StagedCommit};
use crate::extraction::{ExtractionBudget, ExtractionContext, ExtractionRepairContext, Extractor};
use crate::PipelineError;

/// Frozen execution input retained in the existing WorkRun metrics.
pub const EXTRACTION_BUDGET_KEY: &str = "grounded_extraction_budget_v1";
const REJECTION_KEY: &str = "grounded_extraction_rejection_v1";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectedProposal {
    artifact_id: RecordId,
    fingerprint: String,
}

fn rejection_fingerprint(artifact: &WorkArtifact) -> Result<String, PipelineError> {
    let data = serde_json::to_string(&(
        &artifact.base.id,
        &artifact.work_run,
        &artifact.data,
        &artifact.validation_errors,
    ))
    .map_err(|_| exhausted())?;
    Ok(crate::extraction::AssertionValidator::hash_quote(&data))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalBudget {
    input: Value,
    original_run: RecordId,
    started_at: DateTime<Utc>,
    deadline: Option<DateTime<Utc>>,
    budget: Option<ExtractionBudget>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunBudget {
    original: OriginalBudget,
    attempt: AttemptFence,
}

fn exhausted() -> PipelineError {
    PipelineError::GroundingRepairExhausted
}

fn frozen(run: &WorkRun) -> Result<Option<RunBudget>, PipelineError> {
    run.metrics
        .custom
        .get(EXTRACTION_BUDGET_KEY)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| exhausted())
}

fn run_fence(run: &WorkRun, item: &WorkItem) -> Result<AttemptFence, PipelineError> {
    let fence: AttemptFence = serde_json::from_value(
        run.metrics
            .custom
            .get("attempt_fence")
            .cloned()
            .ok_or_else(exhausted)?,
    )
    .map_err(|_| exhausted())?;
    if run.base.space_id != item.base.space_id
        || run.base.is_deleted()
        || run.work_item.id() != &item.base.id
        || run.attempt == 0
        || fence.space_id != item.base.space_id
        || fence.work_item_id != item.base.id
        || fence.generation == 0
        || fence.lease_token.is_empty()
    {
        return Err(exhausted());
    }
    Ok(fence)
}

pub(crate) async fn current_run(
    storage: &StorageClient,
    space: &str,
    run_id: &RecordId,
) -> Result<(WorkRun, WorkItem, AttemptFence), PipelineError> {
    let run = WorkRunRepository::new(storage.clone())
        .get(space, run_id)
        .await?
        .ok_or_else(exhausted)?;
    let item = WorkItemRepository::new(storage.clone())
        .get(space, run.work_item.id())
        .await?
        .ok_or_else(exhausted)?;
    let original = run_fence(&run, &item)?;
    let current = elephant_storage::work_attempt_repository::fence_from_claim(&item)?;
    if original != current
        || run.attempt != item.attempts
        || run.completed_at.is_some()
        || item
            .lease_expires_at
            .is_none_or(|deadline| deadline <= Utc::now())
    {
        return Err(PipelineError::LeaseConflict(
            "Original extraction attempt is no longer live".into(),
        ));
    }
    Ok((run, item, original))
}

async fn history(storage: &StorageClient, item: &WorkItem) -> Result<Vec<WorkRun>, PipelineError> {
    let mut runs = WorkRunRepository::new(storage.clone())
        .find_by_work_item(&item.base.id)
        .await?;
    let mut generations = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for run in &runs {
        let fence = run_fence(run, item)?;
        if !generations.insert(fence.generation) || !ids.insert(run.base.id.clone()) {
            return Err(exhausted());
        }
    }
    runs.sort_by_key(|run| (run.started_at, run.base.id.clone()));
    Ok(runs)
}

fn check_artifact(
    artifact: &WorkArtifact,
    run: &WorkRun,
    lineage: &GroundingLineage,
    window: [usize; 2],
) -> Result<ExtractionArtifactData, PipelineError> {
    let data: ExtractionArtifactData =
        serde_json::from_value(artifact.data.clone()).map_err(|_| exhausted())?;
    if artifact.base.is_deleted()
        || artifact.work_run.id() != &run.base.id
        || artifact.artifact_type != KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
        || !same_authorization_partition(&artifact.base, &lineage.source.base)
        || artifact.data.get("_source_window") != Some(&json!(window))
        || data.source_id.as_deref() != Some(lineage.source.base.id.to_string().as_str())
        || data.node_id != lineage.node.base.id.to_string()
        || data.revision_id != lineage.revision.base.id.to_string()
        || usize::try_from(data.revision_text_len).ok() != Some(lineage.revision.text.len())
        || data.assertions.iter().any(|claim| {
            (claim.evidence_start as usize) < window[0] || (claim.evidence_end as usize) > window[1]
        })
    {
        return Err(exhausted());
    }
    Ok(data)
}

// Strict conservative admission: incomplete usage cannot release another call.
// Reservations remain held by normal settlement; none are presented as usage.
async fn complete_usage(
    storage: &StorageClient,
    item: &WorkItem,
    runs: &[WorkRun],
) -> Result<u64, PipelineError> {
    let repository =
        elephant_storage::provider_usage_repository::ProviderUsageRepository::new(storage.clone());
    let mut total = 0_u64;
    let mut calls = BTreeSet::new();
    for run in runs {
        if run.completed_at.is_none() {
            return Err(exhausted());
        }
        let fence = run_fence(run, item)?;
        let usage = AttemptUsage::from_metrics(&run.metrics)
            .map_err(|_| exhausted())?
            .ok_or_else(exhausted)?;
        let snapshot = frozen(run)?.ok_or_else(exhausted)?;
        let original_config = snapshot
            .original
            .input
            .get("_source_configuration")
            .ok_or_else(exhausted)?;
        let completed = run.completed_at.ok_or_else(exhausted)?;
        if completed < run.started_at {
            return Err(exhausted());
        }
        let receipts = repository
            .for_scope(&item.base.space_id, &run.base.id.to_string())
            .await?;
        let mut measured = 0_u64;
        for receipt in &receipts {
            if receipt.attempt.as_ref() != Some(&fence)
                || !receipt.usage_complete
                || receipt.space_id != item.base.space_id
                || receipt.scope_id != run.base.id.to_string()
                || serde_json::to_value(&receipt.provenance.config).map_err(|_| exhausted())?
                    != *original_config
                || receipt.started_at < run.started_at
                || receipt.updated_at < receipt.started_at
                || receipt.updated_at > completed
                || !calls.insert(receipt.call_id.clone())
            {
                return Err(exhausted());
            }
            measured = measured
                .checked_add(receipt.input_tokens.ok_or_else(exhausted)?)
                .and_then(|n| n.checked_add(receipt.output_tokens?))
                .ok_or_else(exhausted)?;
        }
        let valid = match usage.basis {
            UsageBasis::NotStarted => {
                receipts.is_empty()
                    && usage.measured_tokens.is_none()
                    && usage.estimated_tokens.is_none()
            }
            UsageBasis::Measured => {
                !receipts.is_empty()
                    && usage.measured_tokens == Some(measured)
                    && usage.estimated_tokens.is_none()
            }
            UsageBasis::Unknown | UsageBasis::Estimated => false,
        };
        if !valid {
            return Err(exhausted());
        }
        total = total.checked_add(measured).ok_or_else(exhausted)?;
    }
    Ok(total)
}

fn elapsed_budget(
    original: &OriginalBudget,
    current: &ExtractionBudget,
    used: u64,
) -> Result<ExtractionBudget, PipelineError> {
    let now = Utc::now();
    let hard = original.budget.as_ref().ok_or_else(exhausted)?;
    let expected = original
        .started_at
        .checked_add_signed(chrono::Duration::seconds(i64::from(
            hard.max_wall_time_secs,
        )))
        .ok_or_else(exhausted)?;
    if original.started_at > now || original.deadline != Some(expected) {
        return Err(exhausted());
    }
    let millis = expected.signed_duration_since(now).num_milliseconds();
    // Round down: an integer-second context cannot extend the original deadline.
    let seconds = u32::try_from(millis / 1000)
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(exhausted)?;
    let tokens = u64::from(hard.max_tokens)
        .checked_sub(used)
        .filter(|n| *n > 0)
        .ok_or_else(exhausted)?;
    let remaining = ExtractionBudget {
        max_tokens: u32::try_from(tokens)
            .map_err(|_| exhausted())?
            .min(current.max_tokens),
        max_wall_time_secs: seconds.min(current.max_wall_time_secs),
    };
    if remaining.max_tokens == 0 || remaining.max_wall_time_secs == 0 {
        return Err(exhausted());
    }
    Ok(remaining)
}

/// Bind the actual original budget before dispatch and admit at most one complete
/// re-extraction. Prior proposals are never copied into a successful artifact.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn prepare<E: Extractor + ?Sized>(
    storage: &StorageClient,
    extractor: &E,
    context: &SourceStageContext,
    lineage: &GroundingLineage,
    run_id: &RecordId,
    window: [usize; 2],
    configured_max_attempts: u32,
    ctx: &mut ExtractionContext,
) -> Result<(), PipelineError> {
    let (mut run, item, fence) = current_run(storage, &ctx.space_id, run_id).await?;
    ctx.work_run_id = Some(run_id.clone());
    let mut input = json!({"work_item": item.base.id, "revision": ctx.revision_id,
        "node": ctx.node_id, "window": window, "source_hash": lineage.source.hash,
        "revision_hash": lineage.revision.text_hash, "security": lineage.source.base.security,
        "subjects": lineage.source.base.subjects, "subjects_state": lineage.source.base.subjects_state});
    context.tag(&mut input)?;
    let _ = has_prior_rejection(storage, &item, Some(run_id)).await?;
    if consumed_semantic_repair(storage, &item).await? {
        return Err(exhausted());
    }
    let all = history(storage, &item).await?;
    let prior: Vec<_> = all
        .into_iter()
        .filter(|old| old.base.id != *run_id)
        .collect();
    let mut original: Option<OriginalBudget> = None;
    let mut repair = None;
    let staged = StagedCommit::new(storage.clone());
    for old in &prior {
        let old_fence = run_fence(old, &item)?;
        if old_fence.generation >= fence.generation || old.started_at > run.started_at {
            return Err(exhausted());
        }
        if let Some(snapshot) = frozen(old)? {
            if snapshot.attempt != old_fence
                || snapshot.original.input != input
                || original
                    .as_ref()
                    .is_some_and(|expected| expected != &snapshot.original)
            {
                return Err(exhausted());
            }
            original = Some(snapshot.original);
        }
        let id = context.artifact_id(
            KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
            &ctx.revision_id,
            Some(&ctx.node_id),
            &old.base.id,
        )?;
        if let Some(artifact) = context
            .replay(
                storage,
                &id,
                KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
                &old.base.id,
            )
            .await?
        {
            let _ = check_artifact(&artifact, old, lineage, window)?;
            if artifact.status == ArtifactStatus::Rejected {
                let indices = validate_artifacts::unsupported_proposal_indices(
                    &staged,
                    &ctx.space_id,
                    &artifact,
                )
                .await?
                .ok_or_else(exhausted)?;
                if repair.is_some() {
                    return Err(exhausted());
                }
                let mut proposal = artifact.data["assertions"].clone();
                for claim in proposal.as_array_mut().ok_or_else(exhausted)? {
                    let object = claim.as_object_mut().ok_or_else(exhausted)?;
                    object.remove("support_verification");
                    for field in ["evidence_start", "evidence_end"] {
                        let value = object
                            .get(field)
                            .and_then(Value::as_u64)
                            .ok_or_else(exhausted)?;
                        object.insert(
                            field.into(),
                            json!(value.checked_sub(window[0] as u64).ok_or_else(exhausted)?),
                        );
                    }
                }
                if serde_json::to_vec(&proposal)
                    .map_err(|_| exhausted())?
                    .len()
                    > 250_000
                {
                    return Err(exhausted());
                }
                repair = Some(ExtractionRepairContext {
                    proposal,
                    unsupported_indices: indices,
                });
            }
        }
    }
    let effective = extractor.effective_budget(&ctx.budget);
    let original = original.unwrap_or_else(|| OriginalBudget {
        input: input.clone(),
        original_run: run_id.clone(),
        started_at: run.started_at,
        deadline: effective.as_ref().and_then(|hard| {
            run.started_at
                .checked_add_signed(chrono::Duration::seconds(i64::from(
                    hard.max_wall_time_secs,
                )))
        }),
        budget: effective.clone(),
    });
    if repair.is_some() {
        if item.attempts > configured_max_attempts.min(2)
            || item.attempts < 2
            || prior.iter().any(|old| frozen(old).ok().flatten().is_none())
            || !prior.iter().any(|old| {
                old.base.id == original.original_run && old.started_at == original.started_at
            })
        {
            return Err(exhausted());
        }
        let used = complete_usage(storage, &item, &prior).await?;
        ctx.budget = elapsed_budget(&original, effective.as_ref().ok_or_else(exhausted)?, used)?;
        ctx.repair = repair;
    }
    let snapshot = RunBudget {
        original,
        attempt: fence,
    };
    if frozen(&run)?.is_some_and(|prior| prior != snapshot) {
        return Err(exhausted());
    }
    run.metrics.custom.insert(
        EXTRACTION_BUDGET_KEY.into(),
        serde_json::to_value(snapshot).map_err(|_| exhausted())?,
    );
    context.ensure_current(storage).await?;
    WorkRunRepository::new(storage.clone())
        .update(&ctx.space_id, run_id, run)
        .await?;
    Ok(())
}

/// Validate the exact complete producer batch durably before producer success.
/// Unsupported interpretations remain rejected and use a typed charged failure.
pub(crate) async fn validate_producer(
    storage: &StorageClient,
    space: &str,
    run_id: &RecordId,
    artifact_id: &RecordId,
    configured_max_attempts: u32,
) -> Result<(), PipelineError> {
    let (run, item, _) = current_run(storage, space, run_id).await?;
    let staged = StagedCommit::new(storage.clone());
    let artifacts = validate_artifacts::execute(&staged, space, run_id).await?;
    if artifacts.len() != 1
        || artifacts[0].base.id != *artifact_id
        || artifacts[0].work_run.id() != run_id
        || artifacts[0].artifact_type != KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
    {
        return Err(exhausted());
    }
    let artifact = &artifacts[0];
    let snapshot = frozen(&run)?.ok_or_else(exhausted)?;
    if snapshot.attempt != run_fence(&run, &item)? {
        return Err(exhausted());
    }
    let repairing = has_prior_rejection(storage, &item, Some(run_id)).await?;
    if repairing {
        let hard = snapshot.original.budget.as_ref().ok_or_else(exhausted)?;
        let _ = elapsed_budget(&snapshot.original, hard, 0)?;
    }
    if artifact.status == ArtifactStatus::Validated {
        let data: ExtractionArtifactData =
            serde_json::from_value(artifact.data.clone()).map_err(|_| exhausted())?;
        if data.assertions.is_empty() && repairing {
            return Err(PipelineError::GroundingRepairEmpty);
        }
        return Ok(());
    }
    if validate_artifacts::unsupported_proposal_indices(&staged, space, artifact)
        .await?
        .is_none()
    {
        return Err(exhausted());
    }
    // Persist an exact derived rejection reference as the failed run outcome.
    // It is not a retry counter or alternate artifact authority: a missing or
    // changed referenced artifact is terminal, never a restored ordinary cap.
    let rejected = serde_json::to_value(RejectedProposal {
        artifact_id: artifact.base.id.clone(),
        fingerprint: rejection_fingerprint(artifact)?,
    })
    .map_err(|_| exhausted())?;
    if run
        .metrics
        .custom
        .get(REJECTION_KEY)
        .is_some_and(|old| old != &rejected)
    {
        return Err(exhausted());
    }
    let mut rejected_run = run.clone();
    rejected_run
        .metrics
        .custom
        .insert(REJECTION_KEY.into(), rejected);
    WorkRunRepository::new(storage.clone())
        .update(space, run_id, rejected_run)
        .await?;
    if item.attempts >= configured_max_attempts.min(2) {
        return Err(exhausted());
    }
    let hard = snapshot.original.budget.as_ref().ok_or_else(exhausted)?;
    let _ = elapsed_budget(&snapshot.original, hard, 0)?;
    Err(PipelineError::GroundingRepairRequired)
}

async fn has_prior_rejection(
    storage: &StorageClient,
    item: &WorkItem,
    exclude: Option<&RecordId>,
) -> Result<bool, PipelineError> {
    if item.kind != super::extraction_work_kinds::EXTRACT_ASSERTIONS_KIND
        || item.payload.get("_source_plan").is_none()
    {
        return Ok(false);
    }
    let mut selected = false;
    for run in history(storage, item).await? {
        if exclude == Some(&run.base.id) {
            continue;
        }
        let artifacts = WorkArtifactRepository::new(storage.clone())
            .find_by_work_run(&run.base.id)
            .await?;
        if let Some(reference) = run.metrics.custom.get(REJECTION_KEY) {
            let reference: RejectedProposal =
                serde_json::from_value(reference.clone()).map_err(|_| exhausted())?;
            let artifact = artifacts
                .iter()
                .find(|artifact| artifact.base.id == reference.artifact_id)
                .ok_or_else(exhausted)?;
            if artifact.status != ArtifactStatus::Rejected
                || artifact.base.is_deleted()
                || rejection_fingerprint(artifact)? != reference.fingerprint
            {
                return Err(exhausted());
            }
            selected = true;
        }
        for artifact in artifacts {
            if artifact.base.space_id != item.base.space_id
                || artifact.work_run.id() != &run.base.id
            {
                return Err(exhausted());
            }
            if artifact.artifact_type == KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
                && artifact.status == ArtifactStatus::Rejected
            {
                // Malformed history must not restore a larger ordinary retry cap.
                if artifact.data.get("_source_plan") != item.payload.get("_source_plan")
                    || artifact.data.get("_source_configuration")
                        != item.payload.get("_source_configuration")
                    || artifact.data.get("_publication") != item.payload.get("_publication")
                {
                    return Err(exhausted());
                }
                selected = true;
            }
        }
    }
    Ok(selected)
}

/// Limit only a selected semantic repair sequence. Ordinary transport retries
/// retain their configured policy. Callers use the same result for queue and
/// source-operation disposition, including preflight and timeout failures.
///
/// # Errors
/// Refuses malformed or missing retained rejection history and storage failures.
pub async fn semantic_retry_limit(
    storage: &StorageClient,
    item: &WorkItem,
    configured: u32,
) -> Result<u32, PipelineError> {
    Ok(if has_prior_rejection(storage, item, None).await? {
        configured.min(2)
    } else {
        configured
    })
}

/// Read complete current physical usage for the active extractor's support and
/// fallback budgets. SDK totals alone cannot turn missing usage into capacity.
pub(crate) async fn measured_current_usage(
    storage: &StorageClient,
    space: &str,
    run_id: &RecordId,
) -> Result<u64, PipelineError> {
    let (run, item, fence) = current_run(storage, space, run_id).await?;
    let snapshot = frozen(&run)?.ok_or_else(exhausted)?;
    let config = snapshot
        .original
        .input
        .get("_source_configuration")
        .ok_or_else(exhausted)?;
    let receipts =
        elephant_storage::provider_usage_repository::ProviderUsageRepository::new(storage.clone())
            .for_scope(space, &run_id.to_string())
            .await?;
    let mut total = 0_u64;
    let mut ids = BTreeSet::new();
    for receipt in receipts {
        if receipt.space_id != item.base.space_id
            || receipt.scope_id != run_id.to_string()
            || receipt.attempt.as_ref() != Some(&fence)
            || !receipt.usage_complete
            || serde_json::to_value(&receipt.provenance.config).map_err(|_| exhausted())? != *config
            || receipt.started_at < run.started_at
            || receipt.updated_at < receipt.started_at
            || !ids.insert(receipt.call_id)
        {
            return Err(exhausted());
        }
        total = total
            .checked_add(receipt.input_tokens.ok_or_else(exhausted)?)
            .and_then(|n| n.checked_add(receipt.output_tokens?))
            .ok_or_else(exhausted)?;
    }
    Ok(total)
}

/// Additional budget guard inside the existing durable dispatch observer. It
/// runs before recording/issuing each physical request, including SDK retries.
/// It does not resolve credentials, change bindings or grant source authority.
pub(crate) async fn guard_physical_dispatch(
    storage: &StorageClient,
    item: &WorkItem,
    run_id: &RecordId,
) -> Result<(), PipelineError> {
    if item.kind != super::extraction_work_kinds::EXTRACT_ASSERTIONS_KIND
        || item.payload.get("_source_plan").is_none()
    {
        return Ok(());
    }
    let run = WorkRunRepository::new(storage.clone())
        .get(&item.base.space_id, run_id)
        .await?
        .ok_or_else(exhausted)?;
    let Some(snapshot) = frozen(&run)? else {
        return Ok(());
    };
    let Some(hard) = snapshot.original.budget.as_ref() else {
        return Ok(());
    };
    if !has_prior_rejection(storage, item, Some(run_id)).await? {
        // Original SDK generation/auxiliary dispatch is unchanged. Incomplete
        // historical auxiliary usage disables automatic repair, not this path.
        return Ok(());
    }
    let current = measured_current_usage(storage, &item.base.space_id, run_id).await?;
    {
        let prior: Vec<_> = history(storage, item)
            .await?
            .into_iter()
            .filter(|old| old.base.id != *run_id)
            .collect();
        let used = complete_usage(storage, item, &prior)
            .await?
            .checked_add(current)
            .ok_or_else(exhausted)?;
        let _ = elapsed_budget(&snapshot.original, hard, used)?;
    }
    Ok(())
}

/// Whether an exact generation after the original rejected proposal already
/// issued a physical request. Refundable prerequisite counters cannot lend that
/// consumed repair generation to another call. This derives from existing runs,
/// artifacts and provider receipts; it does not maintain a second attempt ledger.
///
/// # Errors
/// Refuses ambiguous history, changed rejection references and foreign receipts.
pub async fn consumed_semantic_repair(
    storage: &StorageClient,
    item: &WorkItem,
) -> Result<bool, PipelineError> {
    if !has_prior_rejection(storage, item, None).await? {
        return Ok(false);
    }
    let runs = history(storage, item).await?;
    let mut rejected_generations = BTreeSet::new();
    for run in &runs {
        if WorkArtifactRepository::new(storage.clone())
            .find_by_work_run(&run.base.id)
            .await?
            .iter()
            .any(|artifact| {
                artifact.artifact_type == KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
                    && artifact.status == ArtifactStatus::Rejected
            })
        {
            rejected_generations.insert(run_fence(run, item)?.generation);
        }
    }
    let first = *rejected_generations.first().ok_or_else(exhausted)?;
    let repository =
        elephant_storage::provider_usage_repository::ProviderUsageRepository::new(storage.clone());
    for run in runs {
        let fence = run_fence(&run, item)?;
        if fence.generation <= first {
            continue;
        }
        let receipts = repository
            .for_scope(&item.base.space_id, &run.base.id.to_string())
            .await?;
        for receipt in &receipts {
            if receipt.attempt.as_ref() != Some(&fence)
                || receipt.space_id != item.base.space_id
                || receipt.scope_id != run.base.id.to_string()
            {
                return Err(exhausted());
            }
        }
        if !receipts.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}
