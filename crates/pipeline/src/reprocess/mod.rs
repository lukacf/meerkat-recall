//! Reprocessing and backfill workers.
//!
//! Per spec §14/§16, this module handles replay when extractors or semantics improve.
//! Key capabilities:
//! - Job creation with selection criteria validation
//! - Budget-aware pacing for batch processing
//! - Progress tracking with ETA estimation
//! - Supersession of old revisions/assertions/rels/attributes

mod atomic;
mod budget;
mod failure;
mod maintenance_job;
mod source_job;

pub use budget::guard_physical_dispatch;
pub use failure::reconcile_failed_work;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, DocRevisionRepository, RelRepository,
    Repository, ReprocessJobRepository, SourceRepository, StorageClient, WorkRunRepository,
};
#[cfg(test)]
use elephant_types::records::assertion::ResolutionStatus;
use elephant_types::records::base::BaseFields;
#[cfg(test)]
use elephant_types::records::base::{Extractor, Provenance};
#[cfg(test)]
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::DocRevision;
#[cfg(test)]
use elephant_types::records::doc_revision::RevisionStatus;
#[cfg(test)]
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::source::{Source, SourceType};
#[cfg(test)]
use elephant_types::records::work::WorkRun;
use elephant_types::{Link, RecordId, TableName};
use std::str::FromStr;

use crate::error::{PipelineError, PipelineResult};
#[cfg(test)]
use crate::renderer::{RenderContext, RendererRegistry};
use crate::truth_maintenance::{
    execute_tm_recompute, RecomputePriority, TmContext, TruthMaintenanceStorage,
};

// ============================================================================
// Configuration Types
// ============================================================================

/// Configuration for reprocess job execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReprocessConfig {
    /// Batch size for processing sources.
    pub batch_size: usize,

    /// Default budget limit in euros (None = no limit).
    pub default_budget_limit_euros: Option<f64>,

    /// Default max sources per day.
    pub default_max_sources_per_day: Option<u32>,

    /// Whether to pause on budget limit (vs fail).
    pub pause_on_budget_limit: bool,
}

impl Default for ReprocessConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            default_budget_limit_euros: None,
            default_max_sources_per_day: Some(1000),
            pause_on_budget_limit: true,
        }
    }
}

/// Validation error for selection criteria.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    /// Field that failed validation.
    pub field: String,
    /// Reason for validation failure.
    pub reason: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

/// Result of job creation.
#[derive(Debug, Clone)]
pub struct JobCreationResult {
    /// The created job.
    pub job: ReprocessJob,
    /// Sources matched by criteria (estimated).
    pub estimated_sources: u64,
}

/// A read-only snapshot of original accepted source-operation usage. This is
/// neither source completion authority nor a currency budget reservation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReprocessSourceUsage {
    /// Original accepted operations represented in this snapshot.
    pub operation_count: u64,
    /// Distinct physical dispatch receipts across all attempts and plans.
    pub physical_calls: u64,
    /// Complete input token sum, or unknown.
    pub input_tokens: Option<u64>,
    /// Complete output token sum, or unknown.
    pub output_tokens: Option<u64>,
    /// Monetary total backed by actual billing evidence; never inferred from
    /// a missing receipt or a provider/model name.
    pub monetary_euros: Option<f64>,
    /// All included operations and their observed usage are closed. Pricing
    /// may still be unknown even when this is true.
    pub complete: bool,
}

/// Result of reprocess execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReprocessResult {
    /// Number of sources processed.
    pub sources_processed: u64,
    /// Number of new revisions created.
    pub revisions_created: u64,
    /// Number of assertions superseded.
    pub assertions_superseded: u64,
    /// Existing assertions reconsidered without manufacturing extraction evidence.
    #[serde(default)]
    pub assertions_reprocessed: u64,
    /// Events retired while re-resolving their assertion support.
    #[serde(default)]
    pub events_superseded: u64,
    /// New entity identities prepared by assertion re-resolution.
    #[serde(default)]
    pub entities_created: u64,
    /// Number of relationships superseded.
    pub rels_superseded: u64,
    /// Number of attributes superseded.
    pub attributes_superseded: u64,
    /// Retained legacy recorded cost. This field is not proof of measured
    /// source-child spending; consult `source_usage.monetary_euros`.
    pub cost_spent_euros: f64,
    /// Current original-operation accounting for source replacement jobs.
    /// Absent for maintenance-only jobs and before source admission.
    #[serde(default)]
    pub source_usage: Option<ReprocessSourceUsage>,
    /// Whether the job was paused (vs completed).
    pub paused: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Conflict groups the recompute deferred; the caller must enqueue a
    /// tm_resolve_slot work item per group.
    pub pending_conflict_groups: Vec<RecordId>,
    /// Accepted source operations still awaiting canonical publication.
    #[serde(default)]
    pub pending_operations: Vec<String>,
    /// More durable work remains, including sources awaiting admission capacity.
    #[serde(default)]
    pub continuation_required: bool,
}

/// Repository bundle for executing reprocess jobs.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct ReprocessExecutionContext {
    pub storage: StorageClient,
    pub job_repo: ReprocessJobRepository,
    pub source_repo: SourceRepository,
    pub doc_repo: DocRepository,
    pub doc_revision_repo: DocRevisionRepository,
    pub assertion_repo: AssertionRepository,
    pub work_run_repo: WorkRunRepository,
}

/// Result of supersession operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupersessionResult {
    /// ID of the old record that was superseded.
    pub old_id: RecordId,
    /// ID of the new record that supersedes.
    pub new_id: RecordId,
    /// Timestamp of supersession.
    pub superseded_at: DateTime<Utc>,
}

// ============================================================================
// Core Functions - Stubs for TDD
// ============================================================================

/// Validate selection criteria for a reprocess job.
///
/// # Arguments
/// * `criteria` - The selection criteria to validate
///
/// # Errors
///
/// Returns a list of validation errors if:
/// - No criteria are specified
/// - Date range is invalid (date_from > date_to)
pub fn validate_selection_criteria(
    criteria: &SelectionCriteria,
) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    // Check that at least one criterion is specified
    let has_source_types = !criteria.source_types.is_empty();
    let has_predicate_ids = !criteria.predicate_ids.is_empty();
    let has_date_range = criteria.date_from.is_some() || criteria.date_to.is_some();
    let has_extractor_version = criteria.extractor_version_before.is_some();

    if !has_source_types && !has_predicate_ids && !has_date_range && !has_extractor_version {
        errors.push(ValidationError {
            field: "selection_criteria".to_string(),
            reason: "at least one criterion must be specified (source_types, predicate_ids, date_from/date_to, or extractor_version_before)".to_string(),
        });
    }

    // Validate date range if both dates are present
    if let (Some(date_from), Some(date_to)) = (criteria.date_from, criteria.date_to) {
        if date_from > date_to {
            errors.push(ValidationError {
                field: "date_range".to_string(),
                reason: "date_from must be <= date_to".to_string(),
            });
        }
    }

    // Validate source types
    for source_type in &criteria.source_types {
        if SourceType::from_str(source_type).is_err() {
            errors.push(ValidationError {
                field: "source_types".to_string(),
                reason: format!("invalid source type: {source_type}"),
            });
        }
    }

    // Validate extractor version format (semver-like)
    if let Some(version) = criteria.extractor_version_before.as_ref() {
        if parse_semver(version).is_none() {
            errors.push(ValidationError {
                field: "extractor_version_before".to_string(),
                reason: format!("invalid version format: {version}"),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn parse_semver(version: &str) -> Option<(u64, u64, u64)> {
    let trimmed = version.trim().trim_start_matches('v');
    let core = trimmed.split(['-', '+']).next().unwrap_or(trimmed);
    let mut parts = core.split('.').filter(|p| !p.is_empty());
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next().unwrap_or("0").parse::<u64>().ok()?;
    let patch = parts.next().unwrap_or("0").parse::<u64>().ok()?;
    Some((major, minor, patch))
}

fn is_version_before(candidate: &str, threshold: &str) -> bool {
    let Some(candidate_ver) = parse_semver(candidate) else {
        return false;
    };
    let Some(threshold_ver) = parse_semver(threshold) else {
        return false;
    };
    candidate_ver < threshold_ver
}

/// Create a new reprocess job.
///
/// # Arguments
/// * `repository` - Repository for persistence
/// * `name` - Job name/description
/// * `criteria` - Selection criteria for sources
/// * `space_id` - Space to create job in
/// * `created_by` - Actor creating the job
/// * `config` - Optional configuration overrides
///
/// # Errors
///
/// Returns an error if validation fails or storage operations fail.
pub async fn create_reprocess_job(
    repository: &ReprocessJobRepository,
    name: &str,
    criteria: SelectionCriteria,
    target_stages: Vec<ReprocessTargetStage>,
    space_id: &str,
    created_by: &str,
    config: Option<&ReprocessConfig>,
) -> PipelineResult<JobCreationResult> {
    // Validate criteria first
    validate_selection_criteria(&criteria).map_err(|errors| {
        let error_msg = errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        PipelineError::ValidationError(error_msg)
    })?;

    if target_stages.is_empty() {
        return Err(PipelineError::ValidationError(
            "target_stages must include at least one stage".to_string(),
        ));
    }

    // Apply config defaults
    let config = config.cloned().unwrap_or_default();
    budget::validate_limit(config.default_budget_limit_euros)?;

    // Create job record
    let id = RecordId::new_v4(TableName::ReprocessJob);
    let base = BaseFields::new(id, space_id.to_string(), created_by.to_string());

    let job = ReprocessJob {
        base,
        name: name.to_string(),
        selection_criteria: criteria,
        target_stages,
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 0,
        budget_limit_euros: config.default_budget_limit_euros,
        max_sources_per_day: config.default_max_sources_per_day,
        started_at: None,
        completed_at: None,
        error: None,
        reason: None,
    };

    // The internal factory follows the same configured-space envelope and
    // atomic original-producer/initial-handoff contract as public creation.
    let prepared = repository.prepare_creation(job).await?;
    let created = repository.create_prepared_with_work(prepared).await?;

    // Estimated sources is 0 until Phase 10 wires source counting
    Ok(JobCreationResult {
        job: created,
        estimated_sources: 0,
    })
}

/// Execute a reprocess job.
///
/// # Arguments
/// * `repository` - Repository for persistence
/// * `job_id` - ID of the job to execute
/// * `space_id` - Space containing the job
/// * `config` - Execution configuration
///
/// # Errors
///
/// Returns an error if the job is not found, in an invalid state, or storage fails.
///
/// Source replacement dispatches durable accepted operations; resolution and
/// maintenance effects share the job's original captured canonical plan.
pub async fn execute_reprocess_job(
    ctx: &ReprocessExecutionContext,
    job_id: &RecordId,
    work_run_id: &RecordId,
    space_id: &str,
    config: &ReprocessConfig,
) -> PipelineResult<ReprocessResult> {
    Box::pin(execute_reprocess_job_with_attempt(
        ctx,
        job_id,
        work_run_id,
        space_id,
        config,
        None,
    ))
    .await
}

/// Runs retained reprocessing under the original successful queue claim.
/// A pending source operation is not completion; callers must schedule a retry.
#[allow(clippy::too_many_arguments)]
pub async fn execute_reprocess_job_with_attempt(
    ctx: &ReprocessExecutionContext,
    job_id: &RecordId,
    work_run_id: &RecordId,
    space_id: &str,
    config: &ReprocessConfig,
    attempt: Option<elephant_types::publication::AttemptFence>,
) -> PipelineResult<ReprocessResult> {
    let planning = Box::pin(
        crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
            ctx.storage.clone(),
            space_id,
            attempt.clone(),
        ),
    )
    .await?;
    let job = ctx
        .job_repo
        .get(space_id, job_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(format!("ReprocessJob not found: {job_id}")))?;
    planning.observe(&job)?;
    let run = ctx
        .work_run_repo
        .get(space_id, work_run_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(format!("WorkRun not found: {work_run_id}")))?;
    if let Some(fence) = &attempt {
        let saved = run.metrics.custom.get("attempt_fence").and_then(|value| {
            serde_json::from_value::<elephant_types::publication::AttemptFence>(value.clone()).ok()
        });
        if fence.space_id != space_id
            || &fence.work_item_id != run.work_item.id()
            || saved.as_ref() != Some(fence)
            || run.base.deleted_at.is_some()
        {
            return Err(PipelineError::InvalidState(
                "Reprocess run differs from original attempt".into(),
            ));
        }
        planning.observe(&run)?;
    }
    if should_reextract(&job.target_stages) {
        // Both branches have substantial independent state. Keeping their
        // futures out of this frame also bounds callers that select only one.
        return Box::pin(source_job::execute(
            ctx,
            job,
            work_run_id,
            space_id,
            config,
            attempt,
            planning,
        ))
        .await;
    }
    Box::pin(maintenance_job::execute(
        ctx,
        job,
        work_run_id,
        space_id,
        config,
        planning,
    ))
    .await
}

fn remaining_daily_capacity(job: &ReprocessJob, now: DateTime<Utc>) -> Option<u64> {
    let max_per_day = job.max_sources_per_day?;
    let started_at = job.started_at.unwrap_or(now);
    let days_elapsed = now
        .date_naive()
        .signed_duration_since(started_at.date_naive())
        .num_days()
        .max(0);
    let total_allowed = u64::from(max_per_day) * (days_elapsed as u64 + 1);
    if job.progress.processed_sources >= total_allowed {
        Some(0)
    } else {
        Some(total_allowed - job.progress.processed_sources)
    }
}

async fn select_sources_for_reprocess(
    ctx: &ReprocessExecutionContext,
    space_id: &str,
    criteria: &SelectionCriteria,
    limit: usize,
) -> PipelineResult<Vec<Source>> {
    if !criteria.space_ids.is_empty() && !criteria.space_ids.contains(&space_id.to_string()) {
        return Ok(Vec::new());
    }

    let source_types: Vec<SourceType> = criteria
        .source_types
        .iter()
        .filter_map(|s| SourceType::from_str(s).ok())
        .collect();

    let mut selected = Vec::new();
    let mut cursor: Option<RecordId> = None;
    let list_limit = 200;

    while selected.len() < limit {
        let batch = ctx
            .source_repo
            .list(space_id, list_limit, cursor.as_ref())
            .await?;
        if batch.is_empty() {
            break;
        }

        for source in batch.iter() {
            if !matches_source_type(source, &source_types) {
                continue;
            }
            if !matches_date_range(source, criteria.date_from, criteria.date_to) {
                continue;
            }
            if !source_matches_assertion_filters(ctx, source, space_id, criteria).await? {
                continue;
            }

            selected.push(source.clone());
            if selected.len() >= limit {
                break;
            }
        }

        cursor = batch.last().map(|source| source.base.id.clone());
    }

    Ok(selected)
}

fn matches_source_type(source: &Source, allowed: &[SourceType]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    allowed.contains(&source.type_)
}

fn matches_date_range(
    source: &Source,
    date_from: Option<NaiveDate>,
    date_to: Option<NaiveDate>,
) -> bool {
    if date_from.is_none() && date_to.is_none() {
        return true;
    }
    let Some(ts) = source.timestamp else {
        return false;
    };
    let date = ts.date_naive();
    if let Some(from) = date_from {
        if date < from {
            return false;
        }
    }
    if let Some(to) = date_to {
        if date > to {
            return false;
        }
    }
    true
}

async fn source_matches_assertion_filters(
    ctx: &ReprocessExecutionContext,
    source: &Source,
    space_id: &str,
    criteria: &SelectionCriteria,
) -> PipelineResult<bool> {
    if criteria.predicate_ids.is_empty() && criteria.extractor_version_before.is_none() {
        return Ok(true);
    }

    // Selection is a complete predicate over the source, not the first 1,000
    // assertions/documents. Follow matching assertions back to actual records.
    let mut cursor = None;
    loop {
        let page = ctx
            .assertion_repo
            .list(space_id, 256, cursor.as_ref())
            .await?;
        if page.is_empty() {
            return Ok(false);
        }
        let next = page.last().map(|row| row.base.id.clone());
        if next == cursor {
            return Err(PipelineError::InvalidState(
                "Assertion selection did not advance".into(),
            ));
        }
        for assertion in page {
            if !assertion_matches_criteria(&assertion, criteria) {
                continue;
            }
            let Some(revision) = ctx
                .doc_revision_repo
                .get(space_id, assertion.evidence.revision.id())
                .await?
            else {
                continue;
            };
            let Some(doc) = ctx.doc_repo.get(space_id, revision.doc.id()).await? else {
                continue;
            };
            if doc
                .source
                .as_ref()
                .is_some_and(|link| link.id() == &source.base.id)
            {
                return Ok(true);
            }
        }
        cursor = next;
    }
}

fn assertion_matches_criteria(
    assertion: &elephant_types::records::assertion::Assertion,
    criteria: &SelectionCriteria,
) -> bool {
    // Public criteria name predicate IDs. Pending assertions may only have a
    // raw alias, whose retained lookup contract is ASCII case-insensitive.
    let predicate_match = criteria.predicate_ids.is_empty()
        || criteria.predicate_ids.iter().any(|pred| {
            assertion.predicate_id.as_ref() == Some(pred)
                || assertion.predicate_raw.eq_ignore_ascii_case(pred)
        });

    let version_match = match criteria.extractor_version_before.as_ref() {
        None => true,
        Some(version) => assertion
            .base
            .provenance
            .extractor
            .as_ref()
            .and_then(|extractor| extractor.version.as_ref())
            .map_or(false, |candidate| is_version_before(candidate, version)),
    };

    predicate_match && version_match
}

fn should_reextract(target_stages: &[ReprocessTargetStage]) -> bool {
    target_stages.iter().any(|stage| {
        matches!(
            stage,
            ReprocessTargetStage::ReRender
                | ReprocessTargetStage::ReChunk
                | ReprocessTargetStage::ReExtract
        )
    })
}

fn should_reresolve(target_stages: &[ReprocessTargetStage]) -> bool {
    target_stages
        .iter()
        .any(|stage| matches!(stage, ReprocessTargetStage::ReResolve))
}

#[cfg(test)]
async fn build_rerendered_revision(
    registry: &RendererRegistry,
    storage: &StorageClient,
    doc: &Doc,
    space_id: &str,
    work_run: &WorkRun,
) -> PipelineResult<DocRevision> {
    let source_type = doc
        .metadata
        .get("content_type")
        .and_then(|value| value.as_str())
        .or_else(|| {
            doc.media_attachments
                .first()
                .map(|item| item.media_type.as_str())
        })
        .unwrap_or("text/plain");
    let renderer = registry.find_for_source_type(source_type).ok_or_else(|| {
        PipelineError::ConfigError(format!("No renderer for source type: {source_type}"))
    })?;

    let ctx = RenderContext {
        doc: doc.clone(),
        storage: Some(storage.clone()),
        space_id: doc.base.space_id.clone(),
        config: None,
    };
    let render_result = renderer.render(ctx).await?;
    if render_result.status != RevisionStatus::Active {
        return Err(PipelineError::InvalidState(format!(
            "Render failed for doc {}",
            doc.base.id
        )));
    }

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::DocRevision),
        space_id.to_string(),
        doc.base.created_by.clone(),
    );
    base.security = doc.base.security.clone();
    base.provenance = build_reprocess_provenance(&doc.base.provenance, work_run);
    base.subjects = doc.base.subjects.clone();
    base.subjects_state = doc.base.subjects_state;

    Ok(DocRevision {
        base,
        doc: Link::new(doc.base.id.clone()),
        text: render_result.text,
        text_hash: render_result.text_hash,
        renderer: render_result.renderer,
        manifest: render_result.manifest,
        supersedes: None,
        status: render_result.status,
        rendered_at: Utc::now(),
    })
}

#[cfg(test)]
fn build_reprocess_provenance(previous: &Provenance, work_run: &WorkRun) -> Provenance {
    let mut provenance = previous.clone();
    provenance.work_run_id = Some(work_run.base.id.clone());
    provenance.run_id = Some(work_run.base.id.to_string());
    provenance.prompt_id = work_run.prompt_id.clone();
    provenance.config_id = work_run.config_hash.clone();
    provenance.extractor = Some(Extractor {
        name: Some(work_run.worker_id.clone()),
        version: Some(work_run.worker_version.clone()),
        model: work_run.model_version.clone(),
    });
    provenance
}

/// Update progress for a running job.
///
/// # Arguments
/// * `repository` - Repository for persistence
/// * `job_id` - ID of the job
/// * `space_id` - Space containing the job
/// * `processed` - Number of sources processed this batch
/// * `succeeded` - Number that succeeded
/// * `failed` - Number that failed
/// * `skipped` - Number skipped
/// * `cost` - Cost of this batch in euros
///
/// # Errors
///
/// Returns an error if the job is not found or storage fails.
#[allow(clippy::too_many_arguments)] // Progress update requires all metrics
pub async fn update_progress(
    repository: &ReprocessJobRepository,
    job_id: &RecordId,
    space_id: &str,
    processed: u64,
    succeeded: u64,
    failed: u64,
    skipped: u64,
    cost: f64,
) -> PipelineResult<ReprocessProgress> {
    use elephant_storage::Repository;

    // Get current job
    let mut job = repository
        .get(space_id, job_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(format!("ReprocessJob not found: {job_id}")))?;

    // Update progress fields
    job.progress.processed_sources += processed;
    job.progress.succeeded_sources += succeeded;
    job.progress.failed_sources += failed;
    job.progress.skipped_sources += skipped;
    job.progress.cost_spent_euros += cost;

    // Calculate progress percentage
    if job.progress.total_sources > 0 {
        #[allow(clippy::cast_precision_loss)] // Source counts fit in f32 for percentage calc
        {
            job.progress.progress_percent =
                (job.progress.processed_sources as f32 / job.progress.total_sources as f32) * 100.0;
        }
    }

    // Persist update
    let updated = repository.update(space_id, job_id, job).await?;

    Ok(updated.progress)
}

/// Check if job has exceeded its budget limit.
///
/// # Arguments
/// * `job` - The job to check
///
/// # Returns
/// true if budget exceeded, false otherwise
pub fn check_budget_limit(job: &ReprocessJob) -> bool {
    match job.budget_limit_euros {
        None => false, // No limit means never exceeded
        Some(limit) => job.progress.cost_spent_euros >= limit,
    }
}

/// Supersede an old revision with a new one.
///
/// Updates the old revision's status to `Superseded` and sets the `supersedes`
/// link on the new revision to point to the old one.
///
/// # Arguments
/// * `repository` - Document revision repository
/// * `old_revision_id` - ID of the old revision
/// * `new_revision_id` - ID of the new revision
/// * `space_id` - Space containing the revisions
///
/// # Errors
///
/// Returns an error if the revisions are not found or storage fails.
pub async fn supersede_revision(
    repository: &DocRevisionRepository,
    old_revision_id: &RecordId,
    new_revision_id: &RecordId,
    space_id: &str,
) -> PipelineResult<SupersessionResult> {
    atomic::supersede::<DocRevision, _>(
        repository.storage_client(),
        repository,
        old_revision_id,
        new_revision_id,
        space_id,
    )
    .await
}

/// Supersede an old assertion with a new one.
///
/// Updates the old assertion's resolution_status to `Superseded`.
///
/// # Arguments
/// * `repository` - Assertion repository
/// * `old_assertion_id` - ID of the old assertion
/// * `new_assertion_id` - ID of the new assertion
/// * `space_id` - Space containing the assertions
///
/// # Errors
///
/// Returns an error if the assertions are not found or storage fails.
pub async fn supersede_assertion(
    repository: &AssertionRepository,
    old_assertion_id: &RecordId,
    new_assertion_id: &RecordId,
    space_id: &str,
) -> PipelineResult<SupersessionResult> {
    atomic::supersede::<elephant_types::records::Assertion, _>(
        repository.storage_client(),
        repository,
        old_assertion_id,
        new_assertion_id,
        space_id,
    )
    .await
}

/// Mark an assertion as superseded without linking to a replacement.
/// Supersede an old rel with a new one.
///
/// Updates the old rel's status to `Superseded` and sets the temporal_audit.superseded_at.
///
/// # Arguments
/// * `repository` - Rel repository
/// * `old_rel_id` - ID of the old rel
/// * `new_rel_id` - ID of the new rel
/// * `space_id` - Space containing the rels
///
/// # Errors
///
/// Returns an error if the rels are not found or storage fails.
pub async fn supersede_rel(
    repository: &RelRepository,
    old_rel_id: &RecordId,
    new_rel_id: &RecordId,
    space_id: &str,
) -> PipelineResult<SupersessionResult> {
    atomic::supersede::<elephant_types::records::Rel, _>(
        repository.storage_client(),
        repository,
        old_rel_id,
        new_rel_id,
        space_id,
    )
    .await
}

/// Supersede an old attribute with a new one.
///
/// Updates the old attribute's status to `Superseded` and sets the temporal_audit.superseded_at.
///
/// # Arguments
/// * `repository` - Attribute repository
/// * `old_attribute_id` - ID of the old attribute
/// * `new_attribute_id` - ID of the new attribute
/// * `space_id` - Space containing the attributes
///
/// # Errors
///
/// Returns an error if the attributes are not found or storage fails.
pub async fn supersede_attribute(
    repository: &AttributeRepository,
    old_attribute_id: &RecordId,
    new_attribute_id: &RecordId,
    space_id: &str,
) -> PipelineResult<SupersessionResult> {
    atomic::supersede::<elephant_types::records::Attribute, _>(
        repository.storage_client(),
        repository,
        old_attribute_id,
        new_attribute_id,
        space_id,
    )
    .await
}

// ============================================================================
// Work Kind Constants
// ============================================================================

/// Work kind identifier for creating reprocess jobs.
pub const WORK_KIND_CREATE_REPROCESS_JOB: &str = "create_reprocess_job";

/// Work kind identifier for executing reprocess jobs.
pub const WORK_KIND_EXECUTE_REPROCESS_JOB: &str = "execute_reprocess_job";

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]
    use super::*;
    use crate::renderer::default::DefaultTextRenderer;
    use crate::renderer::multimodal::{FakeMediaRender, MultimodalRenderer};
    use crate::renderer::RendererRegistry;
    use chrono::{NaiveDate, TimeZone};
    use elephant_types::records::reprocess::default_reprocess_target_stages;

    #[test]
    fn reprocess_dispatch_future_types_fit_the_default_stack_budget() {
        // Infer actual composed future types without constructing a backend,
        // allocating a large future on the stack, or polling any work.
        fn dispatch_bytes<F: std::future::Future>(
            _: impl FnOnce(ReprocessExecutionContext, RecordId, RecordId, ReprocessConfig) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        fn maintenance_bytes<F: std::future::Future>(
            _: impl FnOnce(
                ReprocessExecutionContext,
                ReprocessJob,
                RecordId,
                ReprocessConfig,
                crate::maintenance_commit::PlanningTruthMaintenanceStorage,
            ) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        for (phase, bytes) in [
            (
                "direct dispatch",
                dispatch_bytes(|ctx, job, run, config| async move {
                    execute_reprocess_job(&ctx, &job, &run, "test_space", &config).await
                }),
            ),
            (
                "attempt dispatch",
                dispatch_bytes(|ctx, job, run, config| async move {
                    execute_reprocess_job_with_attempt(
                        &ctx,
                        &job,
                        &run,
                        "test_space",
                        &config,
                        None,
                    )
                    .await
                }),
            ),
            (
                "retained maintenance",
                maintenance_bytes(|ctx, job, run, config, planning| async move {
                    maintenance_job::execute(&ctx, job, &run, "test_space", &config, planning).await
                }),
            ),
        ] {
            assert!(bytes <= 128 * 1024, "{phase} future is {bytes} bytes");
        }
    }

    // ========================================================================
    // Selection Criteria Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_criteria_empty_rejected() {
        let criteria = SelectionCriteria::default();
        let result = validate_selection_criteria(&criteria);
        assert!(result.is_err(), "Empty criteria should be rejected");
        let errors = result.unwrap_err();
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_validate_criteria_with_source_types_valid() {
        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };
        let result = validate_selection_criteria(&criteria);
        assert!(result.is_ok(), "Criteria with source_types should be valid");
    }

    #[test]
    fn test_validate_criteria_with_predicate_ids_valid() {
        let criteria = SelectionCriteria {
            predicate_ids: vec!["pred:123".to_string()],
            ..Default::default()
        };
        let result = validate_selection_criteria(&criteria);
        assert!(
            result.is_ok(),
            "Criteria with predicate_ids should be valid"
        );
    }

    #[test]
    fn test_validate_criteria_date_range_valid() {
        let criteria = SelectionCriteria {
            date_from: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            date_to: Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()),
            ..Default::default()
        };
        let result = validate_selection_criteria(&criteria);
        assert!(result.is_ok(), "Valid date range should be accepted");
    }

    #[test]
    fn test_validate_criteria_date_range_invalid() {
        let criteria = SelectionCriteria {
            date_from: Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()),
            date_to: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ..Default::default()
        };
        let result = validate_selection_criteria(&criteria);
        assert!(result.is_err(), "date_from > date_to should be rejected");
    }

    #[test]
    fn test_validate_criteria_extractor_version_valid() {
        let criteria = SelectionCriteria {
            extractor_version_before: Some("1.2.3".to_string()),
            ..Default::default()
        };
        let result = validate_selection_criteria(&criteria);
        assert!(result.is_ok(), "Valid semver should be accepted");
    }

    // ========================================================================
    // Budget Check Tests
    // ========================================================================

    #[test]
    fn test_check_budget_no_limit() {
        let job = make_test_job(None, 100.0);
        let exceeded = check_budget_limit(&job);
        assert!(!exceeded, "No limit should never be exceeded");
    }

    #[test]
    fn test_check_budget_under_limit() {
        let job = make_test_job(Some(100.0), 50.0);
        let exceeded = check_budget_limit(&job);
        assert!(!exceeded, "Under limit should not be exceeded");
    }

    #[test]
    fn test_check_budget_at_limit() {
        let job = make_test_job(Some(100.0), 100.0);
        let exceeded = check_budget_limit(&job);
        assert!(exceeded, "At limit should be exceeded");
    }

    #[test]
    fn test_check_budget_over_limit() {
        let job = make_test_job(Some(100.0), 150.0);
        let exceeded = check_budget_limit(&job);
        assert!(exceeded, "Over limit should be exceeded");
    }

    // ========================================================================
    // Progress Update Tests
    // ========================================================================

    #[test]
    fn test_progress_percentage_calculation() {
        let progress = ReprocessProgress {
            total_sources: 100,
            processed_sources: 25,
            succeeded_sources: 20,
            failed_sources: 5,
            skipped_sources: 0,
            progress_percent: 0.0, // Will be calculated
            eta_seconds: None,
            cost_spent_euros: 0.0,
        };
        // Progress percent should be (processed / total) * 100 = 25%
        // This test validates the calculation logic once implemented
        assert!(progress.total_sources > 0);
    }

    // ========================================================================
    // Supersession Tests
    // ========================================================================

    #[test]
    fn test_supersession_result_structure() {
        let now = Utc::now();
        let old_id = RecordId::new_v4(elephant_types::TableName::DocRevision);
        let new_id = RecordId::new_v4(elephant_types::TableName::DocRevision);

        let result = SupersessionResult {
            old_id: old_id.clone(),
            new_id: new_id.clone(),
            superseded_at: now,
        };

        assert_eq!(result.old_id, old_id);
        assert_eq!(result.new_id, new_id);
        assert!(result.superseded_at <= Utc::now());
    }

    // ========================================================================
    // Config Tests
    // ========================================================================

    #[test]
    fn test_reprocess_config_default() {
        let config = ReprocessConfig::default();
        assert_eq!(config.batch_size, 100);
        assert!(config.default_budget_limit_euros.is_none());
        assert_eq!(config.default_max_sources_per_day, Some(1000));
        assert!(config.pause_on_budget_limit);
    }

    #[test]
    fn test_reprocess_config_custom() {
        let config = ReprocessConfig {
            batch_size: 50,
            default_budget_limit_euros: Some(500.0),
            default_max_sources_per_day: Some(500),
            pause_on_budget_limit: false,
        };
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.default_budget_limit_euros, Some(500.0));
    }

    #[test]
    fn test_reprocess_result_default() {
        let result = ReprocessResult::default();
        assert_eq!(result.sources_processed, 0);
        assert_eq!(result.revisions_created, 0);
        assert_eq!(result.assertions_superseded, 0);
        assert!(!result.paused);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError {
            field: "date_range".to_string(),
            reason: "date_from must be <= date_to".to_string(),
        };
        let display = err.to_string();
        assert!(display.contains("date_range"));
        assert!(display.contains("date_from must be <= date_to"));
    }

    #[tokio::test]
    async fn test_build_rerendered_revision_uses_multimodal_renderer_for_media_docs() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(DefaultTextRenderer::new()));
        registry.register(Box::new(MultimodalRenderer::for_tests(vec![
            FakeMediaRender {
                attachment_id: "att-1".to_string(),
                derived_text: "Visible event: Alex joined the channel".to_string(),
            },
        ])));

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(elephant_types::TableName::Doc),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            title: "Observed frame".to_string(),
            text: String::new(),
            sidecar_text: Some("Sidecar note".to_string()),
            source: None,
            media_attachments: vec![elephant_types::records::DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: elephant_types::records::DocMediaKind::Image,
                media_type: "image/png".to_string(),
                location: elephant_types::records::DocMediaLocation::Path {
                    path: "/tmp/frame.png".to_string(),
                },
                label: Some("frame.png".to_string()),
                size_bytes: Some(4),
                duration_ms: None,
                sha256: None,
            }],
            rendition_key: "rerender-media".to_string(),
            metadata: serde_json::json!({ "content_type": "image/png" }),
        };
        let work_run = WorkRun {
            base: BaseFields::new(
                RecordId::new_v4(elephant_types::TableName::WorkRun),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            work_item: Link::new(RecordId::new_v4(elephant_types::TableName::WorkItem)),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "reprocess_worker".to_string(),
            worker_version: "1.0.0".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };

        let revision =
            build_rerendered_revision(&registry, &storage, &doc, "test_space", &work_run)
                .await
                .expect("rerendered revision");

        assert_eq!(revision.renderer.name, "elephant.renderer.multimodal");
        let manifest = revision.manifest.expect("manifest");
        assert_eq!(manifest.attachments.len(), 1);
        assert_eq!(manifest.span_bindings.len(), 1);
    }

    // ========================================================================
    // Integration Tests - Work Kind to Repository Flow
    // ========================================================================

    use elephant_storage::{
        AssertionRepository, DocRepository, DocRevisionRepository, RelRepository, Repository,
        ReprocessJobRepository, SourceRepository, StorageClient, StorageConfig,
        TruthSlotRepository, WorkItemRepository, WorkRunRepository,
    };
    use elephant_types::records::assertion::{Assertion, AssertionRetryState, EvidenceSpan};
    use elephant_types::records::base::{
        BaseFields, Extractor, Provenance, Security, SubjectsState,
    };
    #[cfg(test)]
    use elephant_types::records::doc::Doc;
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata};
    use elephant_types::records::rel::{EvidenceRef, Rel, TimeBasis};
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::records::truth::TruthSlot;
    use elephant_types::records::work::{WorkRun, WorkRunMetrics};
    use elephant_types::Link;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(92000);

    const REPROCESS_EXEC_SCHEMA: &str = r"
        DEFINE TABLE reprocess_job SCHEMALESS;
        DEFINE TABLE source SCHEMALESS;
        DEFINE TABLE doc SCHEMALESS;
        DEFINE TABLE doc_revision SCHEMALESS;
        DEFINE TABLE doc_node SCHEMALESS;
        DEFINE TABLE doc_chunk SCHEMALESS;
        DEFINE TABLE assertion SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
        DEFINE TABLE attribute SCHEMALESS;
        DEFINE TABLE truth_slot SCHEMALESS;
        DEFINE TABLE conflict_group SCHEMALESS;
        DEFINE TABLE predicate_registry SCHEMALESS;
        DEFINE TABLE predicate_alias SCHEMALESS;
        DEFINE TABLE freshness SCHEMALESS;
        DEFINE TABLE audit_log SCHEMALESS;
        DEFINE TABLE work_run SCHEMALESS;
        DEFINE TABLE work_item SCHEMALESS;
        DEFINE TABLE work_edge SCHEMALESS;
        DEFINE TABLE work_artifact SCHEMALESS;
        DEFINE TABLE cost_ledger_daily SCHEMALESS;
        DEFINE TABLE system_event SCHEMALESS;
    ";

    async fn setup_test_repo() -> ReprocessJobRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("reprocess_integration_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        elephant_migrations::apply_schema(&client).await.unwrap();
        elephant_storage::SpaceConfigRepository::new(client.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "test_space".into(),
                    "oracle".into(),
                ),
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 1,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        ReprocessJobRepository::new(client)
    }

    async fn setup_execution_context() -> ReprocessExecutionContext {
        setup_execution_context_with_space_base(BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "test_space".into(),
            "oracle".into(),
        ))
        .await
    }

    async fn setup_execution_context_with_space_base(
        base: BaseFields,
    ) -> ReprocessExecutionContext {
        std::env::remove_var("ANTHROPIC_API_KEY");

        let client = StorageClient::new_memory().await.unwrap();
        elephant_migrations::apply_schema(&client).await.unwrap();
        elephant_storage::SpaceConfigRepository::new(client.clone())
            .create(elephant_types::records::SpaceConfig {
                base,
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 1,
                processing: Some(elephant_types::provider_binding::SpaceProcessingConfig {
                    generation: 1,
                    index_generation: 0,
                    policy_id: "oracle".into(),
                    connections: Default::default(),
                    profiles: Default::default(),
                    bindings: Default::default(),
                    active_embedding_profile_id: None,
                    pending_reindex: None,
                }),
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();

        ReprocessExecutionContext {
            storage: client.clone(),
            job_repo: ReprocessJobRepository::new(client.clone()),
            source_repo: SourceRepository::new(client.clone()),
            doc_repo: DocRepository::new(client.clone()),
            doc_revision_repo: DocRevisionRepository::new(client.clone()),
            assertion_repo: AssertionRepository::new(client.clone()),
            work_run_repo: WorkRunRepository::new(client),
        }
    }

    async fn create_test_work_run(
        ctx: &ReprocessExecutionContext,
        space_id: &str,
        worker_id: &str,
        worker_version: &str,
        model_version: Option<&str>,
    ) -> RecordId {
        let work_run_id = RecordId::new_v4(elephant_types::TableName::WorkRun);
        let work_item_id = RecordId::new_v4(elephant_types::TableName::WorkItem);
        let base = BaseFields::new(
            work_run_id.clone(),
            space_id.to_string(),
            worker_id.to_string(),
        );
        let work_run = WorkRun {
            base,
            work_item: Link::new(work_item_id),
            attempt: 0,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: worker_id.to_string(),
            worker_version: worker_version.to_string(),
            model_version: model_version.map(str::to_string),
            prompt_id: None,
            config_hash: Some("config_hash".to_string()),
        };
        ctx.work_run_repo.create(work_run).await.expect("work_run");
        work_run_id
    }

    async fn persist_test_entity(storage: &StorageClient, space: &str, name: &str) -> RecordId {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space.into(),
            "test_agent".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        elephant_storage::EntityRepository::new(storage.clone())
            .create(Entity::new(
                base,
                elephant_types::records::EntityType::new("person"),
                name.into(),
            ))
            .await
            .expect("actual entity")
            .base
            .id
    }

    fn make_source(
        space_id: &str,
        source_type: SourceType,
        timestamp: Option<DateTime<Utc>>,
    ) -> Source {
        let id = RecordId::new_v4(elephant_types::TableName::Source);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let mut source = Source::new(base, source_type, format!("hash_{}", uuid::Uuid::new_v4()));
        source.timestamp = timestamp;
        source
    }

    fn make_doc(space_id: &str, source_id: &RecordId) -> Doc {
        let id = RecordId::new_v4(elephant_types::TableName::Doc);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Doc {
            base,
            title: "Test Doc".to_string(),
            text: "Alice works at Acme".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source_id.clone())),
            media_attachments: Vec::new(),
            rendition_key: format!("rendition_{}", uuid::Uuid::new_v4()),
            metadata: serde_json::json!({}),
        }
    }

    fn make_doc_revision(space_id: &str, doc_id: &RecordId) -> DocRevision {
        let id = RecordId::new_v4(elephant_types::TableName::DocRevision);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        DocRevision {
            base,
            doc: Link::new(doc_id.clone()),
            text: "Alice works at Acme".to_string(),
            text_hash: crate::extraction::AssertionValidator::hash_quote("Alice works at Acme"),
            renderer: RendererMetadata {
                name: "default".to_string(),
                version: "1.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        }
    }

    fn make_assertion(
        space_id: &str,
        revision_id: &RecordId,
        predicate_raw: &str,
        extractor_version: Option<&str>,
    ) -> Assertion {
        let id = RecordId::new_v4(elephant_types::TableName::Assertion);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance {
            extractor: extractor_version.map(|version| Extractor {
                name: Some("test_extractor".to_string()),
                version: Some(version.to_string()),
                model: None,
            }),
            ..Provenance::default()
        };
        base.subjects_state = SubjectsState::Complete;

        Assertion {
            base,
            subject_text: "Alice".to_string(),
            predicate_raw: predicate_raw.to_string(),
            predicate_id: None,
            object_text: Some("Acme".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: None,
                start: 0,
                end: u32::try_from("Alice works at Acme".len()).expect("fixture span"),
                quote_hash: crate::extraction::AssertionValidator::hash_quote(
                    "Alice works at Acme",
                ),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: format!("assertion_{}", uuid::Uuid::new_v4()),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.9,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        }
    }

    #[test]
    fn predicate_selection_matches_canonical_ids_and_retained_alias_case_semantics() {
        let mut assertion = make_assertion(
            "test_space",
            &RecordId::new_v4(TableName::DocRevision),
            "WorksAt",
            Some("1.0.0"),
        );
        assertion.predicate_id = Some("employment.works_at".into());
        let mut criteria = SelectionCriteria {
            predicate_ids: vec!["worksat".into()],
            extractor_version_before: Some("2.0.0".into()),
            ..Default::default()
        };
        assert!(assertion_matches_criteria(&assertion, &criteria));
        criteria.predicate_ids = vec!["employment.works_at".into()];
        assert!(assertion_matches_criteria(&assertion, &criteria));
        criteria.predicate_ids = vec!["EMPLOYMENT.WORKS_AT".into()];
        assert!(!assertion_matches_criteria(&assertion, &criteria));
        criteria.predicate_ids = vec!["worksat".into()];
        criteria.extractor_version_before = Some("1.0.0".into());
        assert!(!assertion_matches_criteria(&assertion, &criteria));
    }

    fn make_rel(
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        predicate_id: &str,
        security: &Security,
    ) -> Rel {
        let id = RecordId::new_v4(elephant_types::TableName::Rel);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = security.clone();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Rel {
            base,
            subject: Link::new(subject_id.clone()),
            object: Link::new(object_id.clone()),
            rel_type: predicate_id.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.8,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    fn make_truth_slot(
        space_id: &str,
        slot_key: &str,
        subject_id: &RecordId,
        predicate_id: &str,
        security_hash: &str,
        computed_at: DateTime<Utc>,
    ) -> TruthSlot {
        let id = RecordId::new_v4(elephant_types::TableName::TruthSlot);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        TruthSlot {
            base,
            slot_key: slot_key.to_string(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: predicate_id.to_string(),
            current_values: Vec::new(),
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 0,
            alias_version: 0,
            stale: false,
            stale_reason: None,
            computed_at,
            security_hash: security_hash.to_string(),
        }
    }

    /// Integration test: create_reprocess_job -> repository -> get
    #[tokio::test]
    async fn test_create_job_integration() {
        let repo = setup_test_repo().await;

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };

        let result = super::create_reprocess_job(
            &repo,
            "Integration Test Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await;

        assert!(result.is_ok(), "create_reprocess_job should succeed");
        let creation_result = result.unwrap();
        assert_eq!(creation_result.job.name, "Integration Test Job");
        assert_eq!(creation_result.job.status, ReprocessStatus::Pending);

        // Verify job was persisted via repository
        let retrieved = repo
            .get("test_space", &creation_result.job.base.id)
            .await
            .unwrap();
        assert!(retrieved.is_some(), "Job should be persisted");
        assert_eq!(retrieved.unwrap().name, "Integration Test Job");
    }

    #[tokio::test]
    async fn internal_reprocess_factory_uses_current_space_scope_and_initial_handoff() {
        let mut original_base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "test_space".into(),
            "oracle".into(),
        );
        original_base.security.policy_id = "private-space".into();
        original_base.security.level = 7;
        original_base.subjects_state = SubjectsState::Complete;
        original_base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        let ctx = setup_execution_context_with_space_base(original_base).await;
        let configurations = elephant_storage::SpaceConfigRepository::new(ctx.storage.clone());
        let configuration = configurations
            .get_by_space_id("test_space")
            .await
            .unwrap()
            .unwrap();
        let created = super::create_reprocess_job(
            &ctx.job_repo,
            "scoped internal job",
            SelectionCriteria {
                source_types: vec!["manual".into()],
                ..Default::default()
            },
            vec![ReprocessTargetStage::ReMaintain],
            "test_space",
            "original actor",
            None,
        )
        .await
        .unwrap()
        .job;
        assert_eq!(created.base.security, configuration.base.security);
        assert_eq!(created.base.subjects, configuration.base.subjects);
        assert_eq!(
            created.base.subjects_state,
            configuration.base.subjects_state
        );
        assert_eq!(created.base.created_by, "original actor");
        let work = WorkItemRepository::new(ctx.storage.clone())
            .find_by_idempotency_key(
                "test_space",
                &format!("{WORK_KIND_EXECUTE_REPROCESS_JOB}:{}", created.base.id),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(work.base.security, created.base.security);
        assert_eq!(work.base.subjects, created.base.subjects);
        assert_eq!(
            ctx.job_repo.ensure_work(&created).await.unwrap(),
            work.base.id
        );
    }

    /// Integration test: execute_reprocess_job state transitions
    #[tokio::test]
    async fn test_execute_job_integration() {
        let ctx = setup_execution_context().await;

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };

        // Create job first
        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "Execution Test Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();

        let job_id = creation_result.job.base.id.clone();
        let config = super::ReprocessConfig::default();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;

        let source_id = RecordId::new_v4(elephant_types::TableName::Source);
        let mut source_base = BaseFields::new(
            source_id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        source_base.security = Security::default();
        source_base.provenance = Provenance::default();
        source_base.subjects_state = SubjectsState::Complete;
        let source = Source::new(source_base, SourceType::Email, "hash_1".to_string());
        ctx.source_repo.create(source).await.unwrap();

        // Execute job
        let execute_result =
            super::execute_reprocess_job(&ctx, &job_id, &work_run_id, "test_space", &config).await;

        assert!(
            execute_result.is_ok(),
            "execute_reprocess_job should succeed"
        );

        // Acceptance remains pending until the source finalizer publishes.
        let final_job = ctx
            .job_repo
            .get("test_space", &job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            final_job.status,
            ReprocessStatus::Running,
            "Accepted work is not completed publication"
        );
        assert!(final_job.started_at.is_some(), "started_at should be set");
        assert!(
            final_job.completed_at.is_none(),
            "completion needs the finalizer certificate"
        );
    }

    #[tokio::test]
    async fn test_execute_reprocess_job_selects_without_premature_supersession() {
        let ctx = setup_execution_context().await;

        let within_range = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let out_of_range = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        let source_good = make_source("test_space", SourceType::Email, Some(within_range));
        let source_bad = make_source("test_space", SourceType::Web, Some(out_of_range));

        ctx.source_repo.create(source_good.clone()).await.unwrap();
        ctx.source_repo.create(source_bad.clone()).await.unwrap();

        let doc_good = make_doc("test_space", &source_good.base.id);
        let doc_bad = make_doc("test_space", &source_bad.base.id);
        ctx.doc_repo.create(doc_good.clone()).await.unwrap();
        ctx.doc_repo.create(doc_bad.clone()).await.unwrap();

        let rev_good = make_doc_revision("test_space", &doc_good.base.id);
        let rev_bad = make_doc_revision("test_space", &doc_bad.base.id);
        ctx.doc_revision_repo
            .create(rev_good.clone())
            .await
            .unwrap();
        ctx.doc_revision_repo.create(rev_bad.clone()).await.unwrap();

        let assertion_good =
            make_assertion("test_space", &rev_good.base.id, "works_at", Some("1.0.0"));
        let assertion_bad =
            make_assertion("test_space", &rev_bad.base.id, "has_title", Some("3.0.0"));
        ctx.assertion_repo
            .create(assertion_good.clone())
            .await
            .unwrap();
        ctx.assertion_repo
            .create(assertion_bad.clone())
            .await
            .unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            date_from: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            date_to: Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()),
            extractor_version_before: Some("2.0.0".to_string()),
            predicate_ids: vec!["WoRkS_aT".to_string()],
            ..Default::default()
        };

        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "Selection Test Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();

        let job_id = creation_result.job.base.id.clone();
        let config = super::ReprocessConfig::default();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;

        let result =
            super::execute_reprocess_job(&ctx, &job_id, &work_run_id, "test_space", &config).await;
        assert!(result.is_ok(), "execute_reprocess_job should succeed");
        let result = result.unwrap();
        assert!(
            result.error.is_none(),
            "reprocess error: {:?}",
            result.error
        );
        assert_eq!(result.revisions_created, 0);
        assert_eq!(result.pending_operations.len(), 1);
        assert_eq!(result.assertions_superseded, 0);

        let updated_good = ctx
            .assertion_repo
            .get("test_space", &assertion_good.base.id)
            .await
            .unwrap()
            .unwrap();
        let updated_bad = ctx
            .assertion_repo
            .get("test_space", &assertion_bad.base.id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(updated_good, assertion_good);
        assert_eq!(updated_bad.resolution_status, ResolutionStatus::Resolved);

        let revisions = ctx
            .doc_revision_repo
            .find_by_doc(&doc_good.base.id)
            .await
            .unwrap();
        assert_eq!(revisions, vec![rev_good]);
        let publication = elephant_storage::source_publication::SourcePublicationRepository::new(
            ctx.storage.clone(),
        );
        assert!(publication
            .head("test_space", &source_good.base.id)
            .await
            .unwrap()
            .is_some());
        assert!(publication
            .head("test_space", &source_bad.base.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_reprocess_reresolve_preserves_existing_assertion_evidence() {
        let ctx = setup_execution_context().await;

        let security = elephant_storage::SpaceConfigRepository::new(ctx.storage.clone())
            .get_by_space_id("test_space")
            .await
            .unwrap()
            .unwrap()
            .base
            .security;
        let mut source = make_source("test_space", SourceType::Email, Some(Utc::now()));
        source.base.security.clone_from(&security);
        ctx.source_repo.create(source.clone()).await.unwrap();
        let mut doc = make_doc("test_space", &source.base.id);
        doc.base.security.clone_from(&security);
        ctx.doc_repo.create(doc.clone()).await.unwrap();
        let mut revision = make_doc_revision("test_space", &doc.base.id);
        revision.base.security.clone_from(&security);
        ctx.doc_revision_repo
            .create(revision.clone())
            .await
            .unwrap();
        let entities = elephant_storage::EntityRepository::new(ctx.storage.clone());
        let mut subject_base = source.base.clone();
        subject_base.id = RecordId::new_v4(TableName::Entity);
        let subject = entities
            .create(elephant_types::records::Entity::new(
                subject_base,
                elephant_types::records::EntityType::new("person"),
                "Alice".into(),
            ))
            .await
            .unwrap();
        let mut object_base = source.base.clone();
        object_base.id = RecordId::new_v4(TableName::Entity);
        let object = entities
            .create(elephant_types::records::Entity::new(
                object_base,
                elephant_types::records::EntityType::new("organization"),
                "Acme".into(),
            ))
            .await
            .unwrap();
        let mut assertion =
            make_assertion("test_space", &revision.base.id, "works_at", Some("1.0.0"));
        assertion.base.security = security;
        assertion.base.provenance.sources = vec![source.base.id.clone()];
        assertion.subject_entity = Some(Link::new(subject.base.id.clone()));
        assertion.object_entity = Some(Link::new(object.base.id.clone()));
        assertion.base.subjects = vec![Link::new(subject.base.id.clone())];
        let assertion = ctx.assertion_repo.create(assertion).await.unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };

        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "ReResolve Job",
            criteria,
            vec![ReprocessTargetStage::ReResolve],
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();

        let job_id = creation_result.job.base.id.clone();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;

        let result = super::execute_reprocess_job(
            &ctx,
            &job_id,
            &work_run_id,
            "test_space",
            &ReprocessConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.assertions_reprocessed, 1);
        let preserved = ctx
            .assertion_repo
            .get("test_space", &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(preserved.evidence, assertion.evidence);
        assert_eq!(preserved.assertion_key, assertion.assertion_key);
        assert_eq!(preserved.resolution_status, ResolutionStatus::Resolved);
        assert_eq!(preserved.subject_entity, assertion.subject_entity);
        assert_eq!(preserved.object_entity, assertion.object_entity);
    }

    #[tokio::test]
    async fn test_reprocess_triggers_truth_maintenance_recompute() {
        use crate::truth_maintenance::compute_slot_key;

        let ctx = setup_execution_context().await;

        let source = make_source("test_space", SourceType::Email, Some(Utc::now()));
        ctx.source_repo.create(source.clone()).await.unwrap();
        let doc = make_doc("test_space", &source.base.id);
        ctx.doc_repo.create(doc.clone()).await.unwrap();
        let revision = make_doc_revision("test_space", &doc.base.id);
        ctx.doc_revision_repo
            .create(revision.clone())
            .await
            .unwrap();
        let assertion = make_assertion("test_space", &revision.base.id, "works_at", Some("1.0.0"));
        ctx.assertion_repo.create(assertion).await.unwrap();

        let subject_id = persist_test_entity(&ctx.storage, "test_space", "Alice").await;
        let object_id = persist_test_entity(&ctx.storage, "test_space", "Acme").await;
        let security = Security::default();
        let rel = make_rel("test_space", &subject_id, &object_id, "works_at", &security);
        let security_hash = crate::truth_maintenance::compute_record_partition(&rel.base);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );

        let rel_repo = RelRepository::new(ctx.storage.clone());
        let truth_slot_repo = TruthSlotRepository::new(ctx.storage.clone());
        rel_repo.create(rel).await.unwrap();

        let old_computed_at = Utc::now() - chrono::Duration::days(10);
        let slot = make_truth_slot(
            "test_space",
            &slot_key,
            &subject_id,
            "works_at",
            &security_hash,
            old_computed_at,
        );
        truth_slot_repo.create(slot).await.unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };
        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "Truth Slot Recompute Job",
            criteria,
            vec![ReprocessTargetStage::ReMaintain],
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();
        let job_id = creation_result.job.base.id.clone();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;

        super::execute_reprocess_job(
            &ctx,
            &job_id,
            &work_run_id,
            "test_space",
            &ReprocessConfig::default(),
        )
        .await
        .unwrap();

        let updated_slot = truth_slot_repo
            .find_by_slot_key("test_space", &slot_key)
            .await
            .unwrap()
            .unwrap();
        assert!(!updated_slot.stale, "truth_slot should be recomputed");
        assert!(
            updated_slot.computed_at > old_computed_at,
            "computed_at should update after recompute"
        );
    }

    /// Reconvergence defers divergent-value slots out of the recompute lease;
    /// the pending conflict groups must surface through ReprocessResult so the
    /// work-item arm can enqueue tm_resolve_slot items.
    #[tokio::test]
    async fn test_reprocess_surfaces_pending_conflict_groups() {
        use crate::truth_maintenance::compute_slot_key;
        use elephant_types::records::attribute::{Attribute, Literal};

        for limit in [None, Some(10.0)] {
            let ctx = setup_execution_context().await;

            let source = make_source("test_space", SourceType::Email, Some(Utc::now()));
            ctx.source_repo.create(source.clone()).await.unwrap();
            let doc = make_doc("test_space", &source.base.id);
            ctx.doc_repo.create(doc.clone()).await.unwrap();
            let revision = make_doc_revision("test_space", &doc.base.id);
            ctx.doc_revision_repo
                .create(revision.clone())
                .await
                .unwrap();
            let assertion = make_assertion("test_space", &revision.base.id, "age", Some("1.0.0"));
            ctx.assertion_repo.create(assertion).await.unwrap();

            let subject_id = persist_test_entity(&ctx.storage, "test_space", "Alice").await;
            let security = Security::default();

            // Two divergent surviving claims: recompute cannot commit inline.
            let attr_repo = elephant_storage::AttributeRepository::new(ctx.storage.clone());
            for value in [35_i64, 36] {
                let id = RecordId::new_v4(elephant_types::TableName::Attribute);
                let mut base =
                    BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
                base.security = security.clone();
                base.subjects_state = SubjectsState::Complete;
                let attr = Attribute {
                    base,
                    subject: Link::new(subject_id.clone()),
                    attr_type: "age".to_string(),
                    value: Literal::Integer(value),
                    status: ClaimStatus::Candidate,
                    confidence: 0.8,
                    validity_window: None,
                    evidence: vec![],
                    temporal_audit: None,
                };
                attr_repo.create(attr).await.unwrap();
            }

            let before_claims = attr_repo.list("test_space", 100, None).await.unwrap();
            let security_hash = crate::truth_maintenance::compute_record_partition(
                &before_claims.first().expect("actual claim").base,
            );
            let slot_key =
                compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
            let truth_slot_repo = TruthSlotRepository::new(ctx.storage.clone());
            let mut slot = make_truth_slot(
                "test_space",
                &slot_key,
                &subject_id,
                "age",
                &security_hash,
                Utc::now() - chrono::Duration::days(10),
            );
            slot.slot_type = "attribute".to_string();
            let before_slot = truth_slot_repo.create(slot).await.unwrap();

            let criteria = SelectionCriteria {
                source_types: vec!["email".to_string()],
                ..Default::default()
            };
            let config = ReprocessConfig {
                default_budget_limit_euros: limit,
                ..Default::default()
            };
            let creation_result = super::create_reprocess_job(
                &ctx.job_repo,
                "Pending Conflict Groups Job",
                criteria,
                vec![ReprocessTargetStage::ReMaintain],
                "test_space",
                "test_agent",
                Some(&config),
            )
            .await
            .unwrap();
            let job_id = creation_result.job.base.id.clone();
            let work_run_id = create_test_work_run(
                &ctx,
                "test_space",
                "reprocess_worker",
                "1.2.3",
                Some("model_v1"),
            )
            .await;

            let result =
                super::execute_reprocess_job(&ctx, &job_id, &work_run_id, "test_space", &config)
                    .await
                    .unwrap();

            if limit.is_some() {
                assert!(result.paused);
                assert!(result.pending_conflict_groups.is_empty());
                assert_eq!(
                    truth_slot_repo
                        .get("test_space", &before_slot.base.id)
                        .await
                        .unwrap(),
                    Some(before_slot)
                );
                assert_eq!(
                    attr_repo.list("test_space", 100, None).await.unwrap(),
                    before_claims
                );
                assert!(
                    elephant_storage::ConflictGroupRepository::new(ctx.storage.clone())
                        .list("test_space", 100, None)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    elephant_storage::WorkArtifactRepository::new(ctx.storage.clone())
                        .list("test_space", 100, None)
                        .await
                        .unwrap()
                        .is_empty()
                );
                let saved = ctx
                    .job_repo
                    .get("test_space", &job_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(saved.status, ReprocessStatus::Paused);
                assert_eq!(saved.error.as_deref(), Some(budget::UNPRICED));
                continue;
            }
            assert_eq!(
                result.pending_conflict_groups.len(),
                1,
                "divergent slot must surface as a pending conflict group"
            );

            let conflict_repo = elephant_storage::ConflictGroupRepository::new(ctx.storage.clone());
            let group = conflict_repo
                .find_unresolved_by_slot_key_and_type("test_space", &slot_key, "attribute")
                .await
                .unwrap()
                .expect("conflict group upserted");
            assert_eq!(group.base.id, result.pending_conflict_groups[0]);
            assert_eq!(group.claim_ids.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_reprocess_remaintain_runs_recompute_without_supersession() {
        use crate::truth_maintenance::compute_slot_key;

        let ctx = setup_execution_context().await;

        let source = make_source("test_space", SourceType::Email, Some(Utc::now()));
        ctx.source_repo.create(source).await.unwrap();

        let subject_id = persist_test_entity(&ctx.storage, "test_space", "Alice").await;
        let object_id = persist_test_entity(&ctx.storage, "test_space", "Acme").await;
        let security = Security::default();
        let rel = make_rel("test_space", &subject_id, &object_id, "works_at", &security);
        let security_hash = crate::truth_maintenance::compute_record_partition(&rel.base);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );

        let rel_repo = RelRepository::new(ctx.storage.clone());
        let truth_slot_repo = TruthSlotRepository::new(ctx.storage.clone());
        rel_repo.create(rel).await.unwrap();

        let old_computed_at = Utc::now() - chrono::Duration::days(7);
        let slot = make_truth_slot(
            "test_space",
            &slot_key,
            &subject_id,
            "works_at",
            &security_hash,
            old_computed_at,
        );
        truth_slot_repo.create(slot).await.unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };
        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "ReMaintain Job",
            criteria,
            vec![ReprocessTargetStage::ReMaintain],
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();
        let job_id = creation_result.job.base.id.clone();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;

        super::execute_reprocess_job(
            &ctx,
            &job_id,
            &work_run_id,
            "test_space",
            &ReprocessConfig::default(),
        )
        .await
        .unwrap();

        let updated_slot = truth_slot_repo
            .find_by_slot_key("test_space", &slot_key)
            .await
            .unwrap()
            .unwrap();
        assert!(!updated_slot.stale, "truth_slot should be recomputed");
        assert!(
            updated_slot.computed_at > old_computed_at,
            "computed_at should update after recompute"
        );
    }

    #[tokio::test]
    async fn test_reprocess_retains_intent_run_without_fabricating_extraction_provenance() {
        let ctx = setup_execution_context().await;

        let source = make_source("test_space", SourceType::Email, Some(Utc::now()));
        ctx.source_repo.create(source.clone()).await.unwrap();
        let doc = make_doc("test_space", &source.base.id);
        ctx.doc_repo.create(doc.clone()).await.unwrap();
        let revision = make_doc_revision("test_space", &doc.base.id);
        ctx.doc_revision_repo
            .create(revision.clone())
            .await
            .unwrap();
        let assertion = make_assertion("test_space", &revision.base.id, "works_at", Some("1.0.0"));
        ctx.assertion_repo.create(assertion).await.unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };
        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "Provenance Test Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();
        let job_id = creation_result.job.base.id.clone();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "9.9.9",
            Some("model_v2"),
        )
        .await;

        super::execute_reprocess_job(
            &ctx,
            &job_id,
            &work_run_id,
            "test_space",
            &ReprocessConfig::default(),
        )
        .await
        .unwrap();

        let revisions = ctx
            .doc_revision_repo
            .find_by_doc(&doc.base.id)
            .await
            .unwrap();
        assert_eq!(revisions, vec![revision]);
        let intents = source_job::load_source_artifacts(
            &elephant_storage::WorkArtifactRepository::new(ctx.storage.clone()),
            "test_space",
            &job_id,
        )
        .await
        .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].work_run.id(), &work_run_id);
        assert_eq!(intents[0].base.provenance.sources, vec![source.base.id]);
        let stored_run = ctx
            .work_run_repo
            .get("test_space", &work_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_run.worker_version, "9.9.9");
        assert_eq!(stored_run.model_version.as_deref(), Some("model_v2"));
    }

    #[tokio::test]
    async fn test_execute_reprocess_job_respects_daily_limit() {
        let ctx = setup_execution_context().await;

        let source = make_source("test_space", SourceType::Email, Some(Utc::now()));
        ctx.source_repo.create(source).await.unwrap();

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };

        let creation_result = super::create_reprocess_job(
            &ctx.job_repo,
            "Daily Limit Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();

        let job_id = creation_result.job.base.id.clone();
        let mut job = ctx
            .job_repo
            .get("test_space", &job_id)
            .await
            .unwrap()
            .unwrap();
        job.max_sources_per_day = Some(1);
        job.progress.processed_sources = 1;
        job.started_at = Some(Utc::now());
        ctx.job_repo
            .update("test_space", &job_id, job)
            .await
            .unwrap();

        let config = super::ReprocessConfig::default();
        let work_run_id = create_test_work_run(
            &ctx,
            "test_space",
            "reprocess_worker",
            "1.2.3",
            Some("model_v1"),
        )
        .await;
        let result =
            super::execute_reprocess_job(&ctx, &job_id, &work_run_id, "test_space", &config).await;
        assert!(result.is_ok(), "execute_reprocess_job should succeed");
        assert!(
            result.unwrap().paused,
            "job should be paused for daily limit"
        );

        let updated_job = ctx
            .job_repo
            .get("test_space", &job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated_job.status, ReprocessStatus::Paused);
    }

    /// Integration test: update_progress modifies job via repository
    #[tokio::test]
    async fn test_update_progress_integration() {
        let repo = setup_test_repo().await;

        let criteria = SelectionCriteria {
            source_types: vec!["email".to_string()],
            ..Default::default()
        };

        // Create job
        let creation_result = super::create_reprocess_job(
            &repo,
            "Progress Test Job",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await
        .unwrap();

        let job_id = creation_result.job.base.id.clone();

        // Set total_sources for progress tracking
        let mut job = repo.get("test_space", &job_id).await.unwrap().unwrap();
        job.progress.total_sources = 100;
        job.status = ReprocessStatus::Running;
        repo.update("test_space", &job_id, job).await.unwrap();

        // Update progress
        let progress =
            super::update_progress(&repo, &job_id, "test_space", 25, 20, 5, 0, 10.5).await;

        assert!(progress.is_ok(), "update_progress should succeed");
        let progress = progress.unwrap();
        assert_eq!(progress.processed_sources, 25);
        assert_eq!(progress.succeeded_sources, 20);
        assert_eq!(progress.failed_sources, 5);
        assert!((progress.cost_spent_euros - 10.5).abs() < f64::EPSILON);
        assert!((progress.progress_percent - 25.0).abs() < f32::EPSILON);

        // Verify persisted via repository
        let final_job = repo.get("test_space", &job_id).await.unwrap().unwrap();
        assert_eq!(final_job.progress.processed_sources, 25);
    }

    /// Integration test: validation rejection prevents repository write
    #[tokio::test]
    async fn test_create_job_validation_rejects_empty_criteria() {
        let repo = setup_test_repo().await;

        let criteria = SelectionCriteria::default(); // Empty - invalid

        let result = super::create_reprocess_job(
            &repo,
            "Should Fail",
            criteria,
            default_reprocess_target_stages(),
            "test_space",
            "test_agent",
            None,
        )
        .await;

        assert!(result.is_err(), "Empty criteria should be rejected");
        match result.unwrap_err() {
            crate::PipelineError::ValidationError(msg) => {
                assert!(msg.contains("at least one criterion"));
            }
            other => panic!("Expected ValidationError, got {other:?}"),
        }
    }

    // ========================================================================
    // Supersession Integration Tests
    // ========================================================================

    use elephant_storage::AttributeRepository;
    use elephant_types::records::attribute::{Attribute, Literal};
    use elephant_types::records::entity::Entity;

    async fn setup_supersession_repos(
        db_id: u64,
    ) -> (
        DocRevisionRepository,
        AssertionRepository,
        RelRepository,
        AttributeRepository,
        StorageClient,
    ) {
        let config = StorageConfig {
            database: format!("supersession_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        elephant_migrations::apply_schema(&client).await.unwrap();
        elephant_storage::SpaceConfigRepository::new(client.clone())
            .create(elephant_types::records::SpaceConfig {
                base: elephant_types::records::BaseFields::new(
                    RecordId::new_v4(elephant_types::TableName::SpaceConfig),
                    "test_space".into(),
                    "test_agent".into(),
                ),
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 1,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();

        (
            DocRevisionRepository::new(client.clone()),
            AssertionRepository::new(client.clone()),
            RelRepository::new(client.clone()),
            AttributeRepository::new(client.clone()),
            client,
        )
    }

    /// Integration test: supersede_revision updates old revision status
    #[tokio::test]
    async fn test_supersede_revision_integration() {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let (doc_rev_repo, _, _, _, storage) = setup_supersession_repos(db_id).await;

        // Create old revision
        let old_id = RecordId::new_v4(elephant_types::TableName::DocRevision);
        let doc_id = RecordId::new_v4(elephant_types::TableName::Doc);
        DocRepository::new(storage)
            .create(Doc {
                base: elephant_types::records::BaseFields::new(
                    doc_id.clone(),
                    "test_space".into(),
                    "test_agent".into(),
                ),
                title: "authored document".into(),
                text: "Old content".into(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: uuid::Uuid::new_v4().to_string(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let old_revision = DocRevision {
            base: elephant_types::records::base::BaseFields::new(
                old_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            doc: Link::new(doc_id.clone()),
            text: "Old content".to_string(),
            text_hash: "old_hash".to_string(),
            renderer: RendererMetadata {
                name: "default".to_string(),
                version: "1.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: super::RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        doc_rev_repo.create(old_revision).await.unwrap();

        // Create new revision
        let new_id = RecordId::new_v4(elephant_types::TableName::DocRevision);
        let new_revision = DocRevision {
            base: elephant_types::records::base::BaseFields::new(
                new_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            doc: Link::new(doc_id),
            text: "New content".to_string(),
            text_hash: "new_hash".to_string(),
            renderer: RendererMetadata {
                name: "default".to_string(),
                version: "2.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: super::RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        doc_rev_repo.create(new_revision).await.unwrap();

        // Supersede
        let result = super::supersede_revision(&doc_rev_repo, &old_id, &new_id, "test_space").await;

        assert!(
            result.is_ok(),
            "supersede_revision should succeed: {result:?}"
        );
        let supersession = result.unwrap();
        assert_eq!(supersession.old_id, old_id);
        assert_eq!(supersession.new_id, new_id);

        // Verify old revision is superseded
        let old_updated = doc_rev_repo
            .get("test_space", &old_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old_updated.status, super::RevisionStatus::Superseded);

        // Verify new revision has supersedes link
        let new_updated = doc_rev_repo
            .get("test_space", &new_id)
            .await
            .unwrap()
            .unwrap();
        assert!(new_updated.supersedes.is_some());
    }

    /// Integration test: supersede_assertion updates old assertion status
    #[tokio::test]
    async fn test_supersede_assertion_integration() {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let (_, assertion_repo, _, _, _) = setup_supersession_repos(db_id).await;

        let revision_id = RecordId::new_v4(elephant_types::TableName::DocRevision);

        // Create old assertion
        let old_id = RecordId::new_v4(elephant_types::TableName::Assertion);
        let old_assertion = Assertion {
            base: elephant_types::records::base::BaseFields::new(
                old_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject_text: "Alice".to_string(),
            predicate_raw: "works_at".to_string(),
            predicate_id: None,
            object_text: Some("Acme".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: None,
                start: 0,
                end: 10,
                quote_hash: "hash1".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "key1".to_string(),
            resolution_status: super::ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.9,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        assertion_repo.create(old_assertion).await.unwrap();

        // Create new assertion
        let new_id = RecordId::new_v4(elephant_types::TableName::Assertion);
        let new_assertion = Assertion {
            base: elephant_types::records::base::BaseFields::new(
                new_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject_text: "Alice".to_string(),
            predicate_raw: "works_at".to_string(),
            predicate_id: None,
            object_text: Some("Acme Corp".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id),
                node: None,
                start: 0,
                end: 15,
                quote_hash: "hash2".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "key2".to_string(),
            resolution_status: super::ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.95,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        assertion_repo.create(new_assertion).await.unwrap();

        // Supersede
        let result =
            super::supersede_assertion(&assertion_repo, &old_id, &new_id, "test_space").await;

        assert!(
            result.is_ok(),
            "supersede_assertion should succeed: {result:?}"
        );

        // Verify old assertion is superseded
        let old_updated = assertion_repo
            .get("test_space", &old_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            old_updated.resolution_status,
            super::ResolutionStatus::Superseded
        );
    }

    /// Integration test: supersede_rel updates old rel status
    #[tokio::test]
    async fn test_supersede_rel_integration() {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let (_, _, rel_repo, _, storage) = setup_supersession_repos(db_id).await;

        let subject_id = persist_test_entity(&storage, "test_space", "Alice").await;
        let object_id = persist_test_entity(&storage, "test_space", "Acme").await;

        // Create old rel
        let old_id = RecordId::new_v4(elephant_types::TableName::Rel);
        let old_rel = Rel {
            base: elephant_types::records::base::BaseFields::new(
                old_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject: Link::new(subject_id.clone()),
            object: Link::new(object_id.clone()),
            rel_type: "works_at".to_string(),
            status: super::ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        rel_repo.create(old_rel).await.unwrap();

        // Create new rel
        let new_id = RecordId::new_v4(elephant_types::TableName::Rel);
        let new_rel = Rel {
            base: elephant_types::records::base::BaseFields::new(
                new_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: "works_at".to_string(),
            status: super::ClaimStatus::Approved,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        rel_repo.create(new_rel).await.unwrap();

        // Supersede
        let result = super::supersede_rel(&rel_repo, &old_id, &new_id, "test_space").await;

        assert!(result.is_ok(), "supersede_rel should succeed: {result:?}");

        // Verify old rel is superseded
        let old_updated = rel_repo.get("test_space", &old_id).await.unwrap().unwrap();
        assert_eq!(old_updated.status, super::ClaimStatus::Superseded);
    }

    /// Integration test: supersede_attribute updates old attribute status
    #[tokio::test]
    async fn test_supersede_attribute_integration() {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let (_, _, _, attr_repo, storage) = setup_supersession_repos(db_id).await;

        let subject_id = persist_test_entity(&storage, "test_space", "Alice").await;

        // Create old attribute
        let old_id = RecordId::new_v4(elephant_types::TableName::Attribute);
        let old_attr = Attribute {
            base: elephant_types::records::base::BaseFields::new(
                old_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject: Link::new(subject_id.clone()),
            attr_type: "email".to_string(),
            value: Literal::String("old@example.com".to_string()),
            status: super::ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        attr_repo.create(old_attr).await.unwrap();

        // Create new attribute
        let new_id = RecordId::new_v4(elephant_types::TableName::Attribute);
        let new_attr = Attribute {
            base: elephant_types::records::base::BaseFields::new(
                new_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            subject: Link::new(subject_id),
            attr_type: "email".to_string(),
            value: Literal::String("new@example.com".to_string()),
            status: super::ClaimStatus::Approved,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        attr_repo.create(new_attr).await.unwrap();

        // Supersede
        let result = super::supersede_attribute(&attr_repo, &old_id, &new_id, "test_space").await;

        assert!(
            result.is_ok(),
            "supersede_attribute should succeed: {result:?}"
        );

        // Verify old attribute is superseded
        let old_updated = attr_repo.get("test_space", &old_id).await.unwrap().unwrap();
        assert_eq!(old_updated.status, super::ClaimStatus::Superseded);
    }

    // ========================================================================
    // Helper Functions
    // ========================================================================

    fn make_test_job(budget_limit: Option<f64>, cost_spent: f64) -> ReprocessJob {
        use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
        use elephant_types::TableName;

        let id = RecordId::new_v4(TableName::ReprocessJob);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        ReprocessJob {
            base,
            name: "Test Job".to_string(),
            selection_criteria: SelectionCriteria {
                source_types: vec!["email".to_string()],
                ..Default::default()
            },
            target_stages: default_reprocess_target_stages(),
            status: ReprocessStatus::Running,
            progress: ReprocessProgress {
                total_sources: 100,
                processed_sources: 50,
                succeeded_sources: 45,
                failed_sources: 5,
                skipped_sources: 0,
                progress_percent: 50.0,
                eta_seconds: Some(300),
                cost_spent_euros: cost_spent,
            },
            priority: 0,
            budget_limit_euros: budget_limit,
            max_sources_per_day: Some(1000),
            started_at: Some(Utc::now()),
            completed_at: None,
            error: None,
            reason: Some("Test reprocessing".to_string()),
        }
    }
}
