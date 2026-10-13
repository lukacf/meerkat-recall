//! Source reprocessing is a genuine new accepted revision. Durable, partitioned
//! intent artifacts precede acceptance; retries never derive a new recipe from
//! a head advanced by their own earlier attempt.
use super::*;
use crate::maintenance_commit::{request_fingerprint, PlanningTruthMaintenanceStorage};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{SpaceConfigRepository, WorkArtifactRepository};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{
    AttemptFence, OperationReceipt, OperationState, PublicationFence, SourceAuthorizationSnapshot,
    SourcePrecondition, SourceVisibility,
};
use elephant_types::records::{ArtifactStatus, WorkArtifact};

const INDEX_KIND: &str = "reprocess_selection/v1";
const SOURCE_KIND: &str = "reprocess_source_intent/v1";
const MAINTENANCE_KIND: &str = "reprocess_source_maintenance/v1";

#[path = "source_budget.rs"]
mod source_budget;
pub(super) use source_budget::guard_source_dispatch;
pub(super) use source_budget::guard_source_dispatch_in_walk;

#[cfg(test)]
#[path = "source_job_tests.rs"]
mod tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SelectionManifest {
    job_id: RecordId,
    request_fingerprint: String,
    configuration: SpaceConfigToken,
    source_count: usize,
    /// Only a digest is kept in the coordinator partition, never source IDs or
    /// copied source bytes. Each source artifact retains its own full envelope.
    source_artifact_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceIntent {
    job_id: RecordId,
    source: Source,
    authorized: SourceAuthorizationSnapshot,
    expected: SourcePrecondition,
    revision_id: String,
    idempotency_key: String,
    request_fingerprint: String,
    configuration: SpaceConfigToken,
    /// Original accepted capability identity; editable contact metadata alone
    /// never authorizes a fresh structured graph contribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contact_authority: Option<ContactAuthority>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContactAuthority {
    publication: PublicationFence,
    // Earlier selections captured the full seal. Preserve their exact recipe
    // encoding; they require the actual original plan while it remains present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    admission_digest: Option<String>,
}

fn recipe_fingerprint(
    job: &RecordId,
    source: &Source,
    expected: &SourcePrecondition,
    configuration: &SpaceConfigToken,
    stages: &[ReprocessTargetStage],
    contact: Option<&ContactAuthority>,
) -> PipelineResult<String> {
    let original = (job, source, expected, configuration, stages);
    match contact {
        Some(contact) => request_fingerprint(&(original, contact)),
        None => request_fingerprint(&original),
    }
}

async fn capture_contact_authority(
    publication: &SourcePublicationRepository,
    space: &str,
    input: &Source,
    original: Option<&PublicationFence>,
) -> PipelineResult<Option<ContactAuthority>> {
    if input.metadata.get("contact").is_none() {
        return Ok(None);
    }
    let original = original.ok_or_else(|| {
        PipelineError::InvalidState(
            "Reprocess contact input has no accepted original capability".into(),
        )
    })?;
    let Some(contact) = publication
        .accepted_contact_capability_for_publication(space, original)
        .await?
    else {
        // The owner verified the exact accepted original operation. Explicit
        // plain intake stays plain even if its metadata resembles a contact.
        return Ok(None);
    };
    Ok(Some(ContactAuthority {
        publication: contact.publication().clone(),
        seal_fingerprint: None,
        admission_digest: Some(contact.digest().to_owned()),
    }))
}

async fn verify_contact_authority(
    publication: &SourcePublicationRepository,
    space: &str,
    intent: &SourceIntent,
    authority: &ContactAuthority,
) -> PipelineResult<()> {
    let current = capture_contact_authority(
        publication,
        space,
        &intent.source,
        Some(&authority.publication),
    )
    .await?
    .ok_or_else(|| {
        PipelineError::InvalidState("Original contact capability was withdrawn".into())
    })?;
    match (&authority.admission_digest, &authority.seal_fingerprint) {
        (Some(expected), None) if current.admission_digest.as_ref() == Some(expected) => Ok(()),
        (None, Some(expected)) => {
            let plan = publication
                .accepted_contact_for_publication(space, &authority.publication)
                .await?
                .ok_or_else(|| {
                    PipelineError::InvalidState("Original contact seal is unavailable".into())
                })?;
            if request_fingerprint(&plan)? == *expected {
                Ok(())
            } else {
                Err(PipelineError::InvalidState(
                    "Original contact capability changed".into(),
                ))
            }
        }
        _ => Err(PipelineError::InvalidState(
            "Original contact capability changed".into(),
        )),
    }
}

fn require_receipt_only_contact_state(state: OperationState) -> PipelineResult<()> {
    if matches!(
        state,
        OperationState::Completed
            | OperationState::Superseded
            | OperationState::FailedTerminal
            | OperationState::Retracted
            | OperationState::Erased
    ) {
        return Ok(());
    }
    Err(elephant_storage::StorageError::Conflict("accepted_contact_plan_unavailable".into()).into())
}

fn acceptance_request(intent: &SourceIntent) -> AcceptSourceRevision {
    AcceptSourceRevision {
        source: intent.source.clone(),
        revision_id: intent.revision_id.clone(),
        expected: intent.expected.clone(),
        idempotency_key: intent.idempotency_key.clone(),
        request_fingerprint: intent.request_fingerprint.clone(),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: Some(intent.authorized.clone()),
        authorized_document: None,
    }
}

async fn prepare_contact_admission(
    storage: &StorageClient,
    publication: &SourcePublicationRepository,
    space: &str,
    intent: &SourceIntent,
    request: &AcceptSourceRevision,
) -> PipelineResult<Option<elephant_storage::source_contact_intake::PreparedSourceContact>> {
    use elephant_storage::source_contact_intake::PreparedSourceContact;
    use elephant_storage::source_publication::SourceAcceptanceRetry;
    if intent.contact_authority.is_some() && intent.source.metadata.get("contact").is_none() {
        return Err(PipelineError::InvalidState(
            "Saved sealed contact input lost its original metadata".into(),
        ));
    }
    let contact = match publication.source_acceptance_retry(request).await? {
        SourceAcceptanceRetry::Existing { contact, .. } => {
            if contact.is_some() != intent.contact_authority.is_some() {
                return Err(PipelineError::InvalidState(
                    "Accepted retry differs from its original plain or sealed contact authority"
                        .into(),
                ));
            }
            let Some(contact) = contact else {
                return Ok(None);
            };
            contact
        }
        SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => {
            // This helper admits work. A matching unavailable plan grants only
            // read-only receipt access in the recovery branch, never admission.
            return Err(elephant_storage::StorageError::Conflict(
                "accepted_contact_plan_unavailable".into(),
            )
            .into());
        }
        SourceAcceptanceRetry::NewIntent => {
            let Some(authority) = &intent.contact_authority else {
                // No selector/preflight is run for a saved plain admission.
                return Ok(None);
            };
            verify_contact_authority(publication, space, intent, authority).await?;
            // Fresh preflight follows selection commit and uses only saved
            // input. The owner still checks this new epoch in acceptance.
            PreparedSourceContact::prepare(storage, &intent.source)
                .await?
                .ok_or_else(|| {
                    PipelineError::InvalidState("Saved contact input cannot be prepared".into())
                })?
        }
    };
    contact.validate_service_authority(&intent.authorized)?;
    Ok(Some(contact))
}

fn index_id(job_id: &RecordId) -> RecordId {
    RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("reprocess-selection:{job_id}").as_bytes(),
        ),
    )
}
fn source_artifact_id(job_id: &RecordId, source_id: &RecordId) -> RecordId {
    RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("reprocess-source:{job_id}:{source_id}").as_bytes(),
        ),
    )
}
fn encode<T: Serialize>(value: &T) -> PipelineResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|e| PipelineError::InvalidState(e.to_string()))
}
fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> PipelineResult<T> {
    serde_json::from_value(value)
        .map_err(|e| PipelineError::InvalidState(format!("Invalid saved reprocess intent: {e}")))
}
fn artifact(
    mut base: BaseFields,
    id: RecordId,
    run: &RecordId,
    kind: &str,
    data: serde_json::Value,
) -> WorkArtifact {
    base.id = id;
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    base.deleted_at = None;
    WorkArtifact {
        base,
        work_run: Link::new(run.clone()),
        artifact_type: kind.into(),
        data,
        status: ArtifactStatus::Validated,
        validation_errors: None,
        validated_at: Some(Utc::now()),
        committed_at: None,
    }
}
fn request_identity(job: &ReprocessJob, config: &ReprocessConfig) -> PipelineResult<String> {
    request_fingerprint(&(
        job.base.id.clone(),
        &job.selection_criteria,
        &job.target_stages,
        config,
    ))
}
fn artifacts_digest(artifacts: &[WorkArtifact]) -> PipelineResult<String> {
    let mut rows: Vec<_> = artifacts
        .iter()
        .map(|row| {
            Ok((
                row.base.id.to_string(),
                elephant_storage::canonical_commit::source_support_fingerprint(row)?,
            ))
        })
        .collect::<PipelineResult<_>>()?;
    rows.sort();
    request_fingerprint(&rows)
}

pub(super) async fn execute(
    ctx: &ReprocessExecutionContext,
    job: ReprocessJob,
    work_run_id: &RecordId,
    space: &str,
    config: &ReprocessConfig,
    attempt: Option<AttemptFence>,
    planning: PlanningTruthMaintenanceStorage,
) -> PipelineResult<ReprocessResult> {
    let job_id = &job.base.id.clone();
    let request = request_identity(&job, config)?;
    if matches!(
        job.status,
        ReprocessStatus::Cancelled | ReprocessStatus::Failed
    ) {
        return Err(PipelineError::InvalidState(
            "Reprocess job is terminal".into(),
        ));
    }
    if job.status != ReprocessStatus::Completed {
        if let Some(reason) = budget::job_refusal(&ctx.storage, &job, true).await? {
            return budget::pause(&planning, job, config, reason).await;
        }
    }
    let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
    let index = artifacts.get(space, &index_id(job_id)).await?;
    let (manifest, source_artifacts) = if let Some(index) = index {
        if index.artifact_type != INDEX_KIND || index.status != ArtifactStatus::Validated {
            return Err(PipelineError::InvalidState(
                "Reprocess selection manifest is unavailable".into(),
            ));
        }
        let manifest: SelectionManifest = decode(index.data)?;
        if manifest.job_id != *job_id || manifest.request_fingerprint != request {
            return Err(PipelineError::InvalidState(
                "Reprocess job differs from its immutable selection".into(),
            ));
        }
        let rows = load_source_artifacts(&artifacts, space, job_id).await?;
        if rows.len() != manifest.source_count
            || artifacts_digest(&rows)? != manifest.source_artifact_digest
        {
            return Err(PipelineError::InvalidState(
                "Reprocess intent was withdrawn or is incomplete".into(),
            ));
        }
        if job.status == ReprocessStatus::Completed {
            let publication = SourcePublicationRepository::new(ctx.storage.clone());
            let mut operations = Vec::with_capacity(rows.len());
            for row in &rows {
                let intent: SourceIntent = decode(row.data.clone())?;
                let receipt = publication
                    .operation_by_idempotency_key(space, &intent.idempotency_key)
                    .await?
                    .ok_or_else(|| {
                        PipelineError::InvalidState(
                            "Completed reprocess operation receipt is unavailable".into(),
                        )
                    })?;
                operations.push(receipt.operation_id);
            }
            if job
                .target_stages
                .contains(&ReprocessTargetStage::ReMaintain)
            {
                let seed = artifacts
                    .get(space, &maintenance_id(job_id))
                    .await?
                    .ok_or_else(|| {
                        PipelineError::InvalidState(
                            "Completed source reprocess maintenance intent is unavailable".into(),
                        )
                    })?;
                validate_maintenance_seed(&seed, &completion_request(&manifest, &operations))?;
            }
            return Ok(ReprocessResult {
                sources_processed: job.progress.succeeded_sources,
                cost_spent_euros: job.progress.cost_spent_euros,
                source_usage: Some(project_usage(&ctx.storage, space, &operations).await?),
                ..Default::default()
            });
        }
        (manifest, rows)
    } else {
        if matches!(
            job.status,
            ReprocessStatus::Running | ReprocessStatus::Completed
        ) {
            return Err(PipelineError::InvalidState(
                "Running reprocess job has no durable selection".into(),
            ));
        }
        if check_budget_limit(&job)
            || remaining_daily_capacity(&job, Utc::now()) == Some(0)
            || config.batch_size == 0
        {
            let mut paused = job.clone();
            let failed = check_budget_limit(&job) && !config.pause_on_budget_limit;
            paused.status = if failed {
                ReprocessStatus::Failed
            } else {
                ReprocessStatus::Paused
            };
            paused.error = failed.then(|| "Budget limit exceeded".into());
            let output = ReprocessResult {
                paused: !failed,
                continuation_required: !failed,
                error: paused.error.clone(),
                ..Default::default()
            };
            planning.stage(Some(job), paused)?;
            planning
                .commit(
                    "reprocess_pause",
                    &job_id.to_string(),
                    &serde_json::json!({"request":request}),
                    &output,
                )
                .await?;
            return Ok(output);
        }
        let configuration = SpaceConfigRepository::new(ctx.storage.clone())
            .resolve_space(space)
            .await
            .map_err(|error| match error {
                elephant_storage::space_config_repository::SpaceResolutionError::Storage(error) => {
                    PipelineError::StorageError(error)
                }
                error => PipelineError::ConfigError(error.to_string()),
            })?
            .token;
        // Freeze the complete selected set. Batch/daily pacing limits admissions
        // below; it must never truncate the scope and mark a prefix complete.
        let sources =
            select_sources_for_reprocess(ctx, space, &job.selection_criteria, usize::MAX).await?;
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let mut rows = Vec::with_capacity(sources.len());
        for source in sources {
            planning.observe(&source)?;
            let head = publication.head(space, &source.base.id).await?;
            let (input, expected, contact_authority) = if let Some(head) = head {
                if head.lifecycle != SourceVisibility::Active {
                    return Err(PipelineError::InvalidState(
                        "Reprocess selected a withdrawn source".into(),
                    ));
                }
                if head.desired_revision != head.published_revision {
                    return Ok(ReprocessResult {
                        pending_operations: vec![head.operation_id],
                        ..Default::default()
                    });
                }
                let published = head.published_revision.ok_or_else(|| {
                    PipelineError::InvalidState("Source has no published input".into())
                })?;
                let input = publication
                    .revision(space, &source.base.id, &published)
                    .await?
                    .ok_or_else(|| {
                        PipelineError::InvalidState(
                            "Published immutable source is unavailable".into(),
                        )
                    })?
                    .source;
                let original = PublicationFence {
                    source_id: source.base.id.clone(),
                    generation: head.generation.clone(),
                    revision_id: published,
                    operation_id: head.operation_id,
                };
                let contact =
                    capture_contact_authority(&publication, space, &input, Some(&original)).await?;
                (
                    input,
                    SourcePrecondition::Generation(head.generation),
                    contact,
                )
            } else {
                capture_contact_authority(&publication, space, &source, None).await?;
                (source.clone(), SourcePrecondition::Absent, None)
            };
            if !crate::maintenance_commit::contains_authorization(&input.base, &source.base)? {
                return Err(PipelineError::SecurityViolation(
                    "Saved reprocess input would weaken current source authorization".into(),
                ));
            }
            let recipe = recipe_fingerprint(
                job_id,
                &input,
                &expected,
                &configuration,
                &job.target_stages,
                contact_authority.as_ref(),
            )?;
            let intent = SourceIntent {
                job_id: job_id.clone(),
                source: input,
                authorized: SourceAuthorizationSnapshot::from(&source),
                expected,
                revision_id: format!("reprocess-{recipe}"),
                idempotency_key: format!("reprocess:{job_id}:{}", source.base.id),
                request_fingerprint: recipe,
                configuration: configuration.clone(),
                contact_authority,
            };
            let mut base = source.base.clone();
            base.provenance.sources = vec![source.base.id.clone()];
            let row = artifact(
                base,
                source_artifact_id(job_id, &source.base.id),
                work_run_id,
                SOURCE_KIND,
                encode(&intent)?,
            );
            planning.stage(None, row.clone())?;
            rows.push(row);
        }
        let manifest = SelectionManifest {
            job_id: job_id.clone(),
            request_fingerprint: request.clone(),
            configuration,
            source_count: rows.len(),
            source_artifact_digest: artifacts_digest(&rows)?,
        };
        let index = artifact(
            job.base.clone(),
            index_id(job_id),
            work_run_id,
            INDEX_KIND,
            encode(&manifest)?,
        );
        planning.stage(None, index)?;
        let mut running = job.clone();
        running.status = ReprocessStatus::Running;
        running.started_at = Some(Utc::now());
        running.progress.total_sources = rows.len() as u64;
        planning.stage(Some(job), running)?;
        planning
            .commit(
                "reprocess_selection",
                &job_id.to_string(),
                &serde_json::json!({"request":request}),
                &(),
            )
            .await?;
        (manifest, rows)
    };
    Box::pin(admit_and_poll(
        ctx,
        job_id,
        work_run_id,
        space,
        config,
        attempt,
        manifest,
        source_artifacts,
    ))
    .await
}

pub(super) async fn load_source_artifacts(
    repo: &WorkArtifactRepository,
    space: &str,
    job: &RecordId,
) -> PipelineResult<Vec<WorkArtifact>> {
    let mut rows = Vec::new();
    let mut cursor = None;
    loop {
        let page = repo.list(space, 256, cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        let next = page.last().map(|row| row.base.id.clone());
        if next == cursor {
            return Err(PipelineError::InvalidState(
                "Reprocess artifact pagination did not advance".into(),
            ));
        }
        for row in page {
            if row.artifact_type == SOURCE_KIND
                && row.data.get("job_id") == Some(&serde_json::json!(job))
            {
                if row.status != ArtifactStatus::Validated {
                    return Err(PipelineError::InvalidState(
                        "Saved reprocess source intent is no longer valid".into(),
                    ));
                }
                rows.push(row);
            }
        }
        cursor = next;
    }
    rows.sort_by_key(|row| row.base.id.to_string());
    Ok(rows)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the original job, run, attempt, durable selection and source artifacts explicit across admission and receipt polling."
)]
async fn admit_and_poll(
    ctx: &ReprocessExecutionContext,
    job_id: &RecordId,
    work_run_id: &RecordId,
    space: &str,
    config: &ReprocessConfig,
    attempt: Option<AttemptFence>,
    manifest: SelectionManifest,
    source_artifacts: Vec<WorkArtifact>,
) -> PipelineResult<ReprocessResult> {
    let publication = SourcePublicationRepository::new(ctx.storage.clone());
    let admission = Box::pin(PlanningTruthMaintenanceStorage::new(
        ctx.storage.clone(),
        space,
        attempt.clone(),
    ))
    .await?;
    let job = ctx
        .job_repo
        .get(space, job_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(job_id.to_string()))?;
    // A restored selection does not bypass the current explicit restriction.
    // Source acceptance remains owner-controlled; no new operation is admitted
    // once this coordinator observes an unpriced finite-budget request.
    if let Some(reason) = budget::job_refusal(&ctx.storage, &job, true).await? {
        admission.observe(&job)?;
        return budget::pause(&admission, job, config, reason).await;
    }
    let mut intents = Vec::new();
    let mut receipts = Vec::new();
    // Read owner receipt identity before deciding admissions; no duplicate
    // source-operation authority or locally replicated operation-key algorithm.
    for row in source_artifacts {
        let intent: SourceIntent = decode(row.data.clone())?;
        if intent.job_id != *job_id
            || intent.configuration != manifest.configuration
            || intent.source.base.space_id != space
            || row.base.provenance.sources != vec![intent.source.base.id.clone()]
        {
            return Err(PipelineError::InvalidState(
                "Saved reprocess source authority differs".into(),
            ));
        }
        if let Some(receipt) = publication
            .operation_by_idempotency_key(space, &intent.idempotency_key)
            .await?
        {
            let original = PublicationFence {
                source_id: receipt.source_id.clone(),
                generation: receipt.source_generation.clone(),
                revision_id: receipt.desired_revision.clone().ok_or_else(|| {
                    PipelineError::InvalidState("Saved source receipt has no revision".into())
                })?,
                operation_id: receipt.operation_id.clone(),
            };
            let origin = publication
                .source_operation_producer(space, &original)
                .await?;
            let matches_saved = matches!(origin,
                elephant_types::publication::SourceProducerOrigin::Reprocess {
                    job_id: producer_job, selection_artifact_id, source_intent_id,
                    selection_digest, intent_fingerprint, ..
                } if producer_job == *job_id && selection_artifact_id == index_id(job_id)
                    && source_intent_id == row.base.id
                    && selection_digest == manifest.source_artifact_digest
                    && intent_fingerprint == elephant_storage::canonical_commit::source_support_fingerprint(&row)?
            );
            if !matches_saved
                || original.source_id != intent.source.base.id
                || original.revision_id != intent.revision_id
            {
                return Err(PipelineError::InvalidState(
                    "Saved reprocess receipt belongs to different original producer authority"
                        .into(),
                ));
            }
            guard_source_dispatch(&ctx.storage, space, &original).await?;
            let request = acceptance_request(&intent);
            match publication.source_acceptance_retry(&request).await? {
                elephant_storage::source_publication::SourceAcceptanceRetry::Existing {
                    contact,
                    ..
                } if contact.is_some() == intent.contact_authority.is_some() => {
                    if let Some(contact) = contact {
                        contact.validate_service_authority(&intent.authorized)?;
                    }
                }
                elephant_storage::source_publication::SourceAcceptanceRetry::ExistingContactPlanUnavailable {
                    operation, capability,
                } if intent.contact_authority.is_some()
                    && operation == receipt
                    && capability.space_id() == space
                    && capability.publication() == &original => {
                    let live = ctx.source_repo.get(space, &receipt.source_id).await?
                        .ok_or_else(|| PipelineError::NotFound(receipt.source_id.to_string()))?;
                    if !crate::maintenance_commit::contains_authorization(&intent.source.base, &live.base)? {
                        return Err(PipelineError::InvalidState("Retained source receipt exceeds original service authority".into()));
                    }
                    // No fresh contact preparation and no enqueue/requeue for
                    // an unavailable historical plan, even if still pending.
                    require_receipt_only_contact_state(operation.state)?;
                }
                _ => {
                    return Err(PipelineError::InvalidState(
                        "Saved reprocess retry lost its original plain or sealed contact authority"
                            .into(),
                    ))
                }
            }
            receipts.push(receipt);
        } else {
            intents.push(row);
        }
    }
    let daily = cumulative_daily_capacity(&job, Utc::now()).saturating_sub(receipts.len() as u64);
    let admissions = config
        .batch_size
        .min(usize::try_from(daily).unwrap_or(usize::MAX));
    let continuation_required = intents.len() > admissions;
    for selected in intents.into_iter().take(admissions) {
        // Every acceptance advances the canonical epoch. Capture a separate
        // complete original selection read set for each admission, before any
        // reads, instead of reusing the first source's now-stale preparation.
        let admission = Box::pin(PlanningTruthMaintenanceStorage::new(
            ctx.storage.clone(),
            space,
            attempt.clone(),
        ))
        .await?;
        let current_job = ctx
            .job_repo
            .get(space, job_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(job_id.to_string()))?;
        admission.observe(&current_job)?;
        if let Some(reason) = budget::job_refusal(&ctx.storage, &current_job, true).await? {
            return budget::pause(&admission, current_job, config, reason).await;
        }
        let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
        let index = artifacts
            .get(space, &index_id(job_id))
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("Original reprocess selection was withdrawn".into())
            })?;
        let current_manifest: SelectionManifest = decode(index.data.clone())?;
        let rows = load_source_artifacts(&artifacts, space, job_id).await?;
        if index.status != ArtifactStatus::Validated
            || index.artifact_type != INDEX_KIND
            || current_manifest.job_id != *job_id
            || current_manifest.request_fingerprint != request_identity(&current_job, config)?
            || current_manifest.configuration != manifest.configuration
            || rows.len() != current_manifest.source_count
            || artifacts_digest(&rows)? != current_manifest.source_artifact_digest
        {
            return Err(PipelineError::InvalidState(
                "Original reprocess selection changed before admission".into(),
            ));
        }
        let current = rows
            .iter()
            .find(|row| row.base.id == selected.base.id)
            .ok_or_else(|| {
                PipelineError::InvalidState("Original source intent was withdrawn".into())
            })?;
        if elephant_storage::canonical_commit::source_support_fingerprint(current)?
            != elephant_storage::canonical_commit::source_support_fingerprint(&selected)?
        {
            return Err(PipelineError::InvalidState(
                "Original source intent changed before admission".into(),
            ));
        }
        let intent: SourceIntent = decode(current.data.clone())?;
        let producer = elephant_storage::source_producer::PreparedSourceProducer::from_verified_reprocess_selection(
            &admission.prepared_plan()?.observed_epoch, &current_job, &index, current, &rows,
        )?;
        let request = acceptance_request(&intent);
        let contact = Box::pin(prepare_contact_admission(
            &ctx.storage,
            &publication,
            space,
            &intent,
            &request,
        ))
        .await?;
        let receipt = Box::pin(publication.accept_with_source_producer(
            request,
            &producer,
            attempt.as_ref(),
            Some(&intent.configuration),
            contact.as_ref(),
        ))
        .await?;
        receipts.push(receipt);
    }
    for receipt in &receipts {
        if matches!(
            receipt.state,
            OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
        ) {
            let revision = receipt.desired_revision.clone().ok_or_else(|| {
                PipelineError::InvalidState("Reprocess acceptance has no revision".into())
            })?;
            let input = publication
                .revision(space, &receipt.source_id, &revision)
                .await?
                .ok_or_else(|| {
                    PipelineError::InvalidState("Accepted reprocess input was withdrawn".into())
                })?;
            let fence = PublicationFence {
                source_id: receipt.source_id.clone(),
                generation: receipt.source_generation.clone(),
                revision_id: revision,
                operation_id: receipt.operation_id.clone(),
            };
            // Initial execution uses the captured recipe. Only the storage
            // owner's explicit manifest rotation may select a successor plan.
            let current = SourcePublicationManifestRepository::new(ctx.storage.clone())
                .get(space, &fence)
                .await?;
            let configuration = current
                .as_ref()
                .map_or(&manifest.configuration, |current| &current.configuration);
            Box::pin(crate::source_queue::enqueue_source_plan(
                &ctx.storage,
                &input.source,
                &fence,
                configuration,
                &job.base.created_by,
            ))
            .await?;
        }
    }
    Box::pin(finish_from_receipts(
        ctx,
        job_id,
        work_run_id,
        space,
        attempt,
        config,
        &manifest,
        &receipts,
        continuation_required,
    ))
    .await
}

fn cumulative_daily_capacity(job: &ReprocessJob, now: DateTime<Utc>) -> u64 {
    let Some(per_day) = job.max_sources_per_day else {
        return u64::MAX;
    };
    let start = job.started_at.unwrap_or(now);
    let days = now
        .date_naive()
        .signed_duration_since(start.date_naive())
        .num_days()
        .max(0) as u64
        + 1;
    u64::from(per_day).saturating_mul(days)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep original selection, attempt, receipts and continuation authority explicit while completing the same canonical plan."
)]
async fn finish_from_receipts(
    ctx: &ReprocessExecutionContext,
    job_id: &RecordId,
    work_run_id: &RecordId,
    space: &str,
    attempt: Option<AttemptFence>,
    config: &ReprocessConfig,
    selection: &SelectionManifest,
    receipts: &[OperationReceipt],
    continuation_required: bool,
) -> PipelineResult<ReprocessResult> {
    // Re-capture before the authoritative completion reads. Source acceptance
    // is a separate durable intent operation, never a partial canonical publish.
    let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
        ctx.storage.clone(),
        space,
        attempt,
    ))
    .await?;
    let before = ctx
        .job_repo
        .get(space, job_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(job_id.to_string()))?;
    planning.observe(&before)?;
    if request_identity(&before, config)? != selection.request_fingerprint
        || selection.job_id != *job_id
        || matches!(
            before.status,
            ReprocessStatus::Cancelled | ReprocessStatus::Failed
        )
    {
        return Err(PipelineError::InvalidState(
            "Reprocess completion differs from its original selection".into(),
        ));
    }
    if let Some(reason) = Box::pin(budget::job_refusal(&ctx.storage, &before, true)).await? {
        return Box::pin(budget::pause(&planning, before, config, reason)).await;
    }
    // Completion captures the original durable selection again after source
    // admission advanced the epoch. These control bytes are never substituted
    // for accepted immutable source support in the canonical maintenance plan.
    let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
    let index = artifacts
        .get(space, &index_id(job_id))
        .await?
        .ok_or_else(|| {
            PipelineError::InvalidState("Original reprocess selection was withdrawn".into())
        })?;
    let current_selection: SelectionManifest = decode(index.data.clone())?;
    let intents = Box::pin(load_source_artifacts(&artifacts, space, job_id)).await?;
    if index.artifact_type != INDEX_KIND
        || index.status != ArtifactStatus::Validated
        || request_fingerprint(&current_selection)? != request_fingerprint(selection)?
        || intents.len() != selection.source_count
        || artifacts_digest(&intents)? != selection.source_artifact_digest
        || (!continuation_required && receipts.len() != selection.source_count)
    {
        return Err(PipelineError::InvalidState(
            "Original reprocess selection changed before completion".into(),
        ));
    }
    planning.observe(&index)?;
    let original_configuration = &selection.configuration;
    let publication = SourcePublicationRepository::new(ctx.storage.clone());
    let mut output = ReprocessResult {
        continuation_required,
        ..Default::default()
    };
    for expected in receipts {
        let receipt = publication
            .operation(space, &expected.operation_id)
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("Reprocess source operation is unavailable".into())
            })?;
        match receipt.state {
            OperationState::Completed => {
                let fence = PublicationFence {
                    source_id: receipt.source_id.clone(),
                    generation: receipt.source_generation.clone(),
                    revision_id: receipt.desired_revision.clone().ok_or_else(|| {
                        PipelineError::InvalidState("Completed operation has no revision".into())
                    })?,
                    operation_id: receipt.operation_id.clone(),
                };
                let manifests = SourcePublicationManifestRepository::new(ctx.storage.clone());
                let manifest = manifests.get(space, &fence).await?.ok_or_else(|| {
                    PipelineError::InvalidState(
                        "Completed reprocess manifest is unavailable".into(),
                    )
                })?;
                let certificate = manifests.completion(space, &fence).await?.ok_or_else(|| {
                    PipelineError::InvalidState(
                        "Completed reprocess certificate is unavailable".into(),
                    )
                })?;
                if certificate.space_id != space
                    || certificate.publication != fence
                    || certificate.manifest_digest != manifest.digest()?
                {
                    return Err(PipelineError::InvalidState(
                        "Reprocess completion does not certify the current manifest".into(),
                    ));
                }
                let head = publication
                    .head(space, &receipt.source_id)
                    .await?
                    .ok_or_else(|| {
                        PipelineError::InvalidState("Completed source head is unavailable".into())
                    })?;
                if head.lifecycle != SourceVisibility::Active
                    || head.generation != receipt.source_generation
                    || head.published_revision != receipt.desired_revision
                {
                    return Err(PipelineError::InvalidState(
                        "Completed reprocess source is no longer current".into(),
                    ));
                }
                let source = ctx
                    .source_repo
                    .get(space, &receipt.source_id)
                    .await?
                    .ok_or_else(|| {
                        PipelineError::InvalidState("Completed source was withdrawn".into())
                    })?;
                planning.observe(&source)?;
                let mut base = source.base.clone();
                base.provenance.sources = vec![source.base.id.clone()];
                let mut certificate_record = artifact(
                    base,
                    RecordId::new(
                        TableName::WorkArtifact,
                        uuid::Uuid::new_v5(
                            &uuid::Uuid::NAMESPACE_OID,
                            format!("reprocess-completion:{job_id}:{}", source.base.id).as_bytes(),
                        ),
                    ),
                    work_run_id,
                    "reprocess_source_completion/v1",
                    serde_json::json!({
                        "job_id":job_id, "operation_id":receipt.operation_id,
                        "original_configuration":original_configuration,
                        "completed_configuration":manifest.configuration,
                        "original_plan":elephant_storage::source_publication_manifest::source_plan_id(&fence, original_configuration),
                        "completed_plan":elephant_storage::source_publication_manifest::source_plan_id(&fence, &manifest.configuration),
                        "manifest_digest":certificate.manifest_digest,
                        "canonical_commit":certificate.commit_id,
                    }),
                );
                certificate_record.status = ArtifactStatus::Committed;
                certificate_record.committed_at = Some(Utc::now());
                // A maintenance continuation retains the original certificate
                // artifact and its original run; it cannot create it again or
                // refresh its authority using the continuation's WorkRun.
                Box::pin(retain_completion_artifact(
                    ctx,
                    &planning,
                    space,
                    certificate_record,
                ))
                .await?;
                output.sources_processed += 1;
                output.revisions_created += manifest.documents.len() as u64;
            }
            OperationState::Accepted
            | OperationState::Processing
            | OperationState::FailedRetryable => {
                output.pending_operations.push(receipt.operation_id)
            }
            _ => output.error = Some("Reprocess source operation ended without publication".into()),
        }
    }
    let operation_ids = receipts
        .iter()
        .map(|receipt| receipt.operation_id.clone())
        .collect::<Vec<_>>();
    output.source_usage = Some(project_usage(&ctx.storage, space, &operation_ids).await?);
    if let Some(amount) = output
        .source_usage
        .as_ref()
        .and_then(|usage| usage.monetary_euros)
    {
        output.cost_spent_euros = amount;
    }
    if (output.continuation_required || !output.pending_operations.is_empty())
        && output.error.is_none()
    {
        return Ok(output);
    }
    let request = completion_request(selection, &operation_ids);
    Box::pin(prepare_completed_job(
        ctx,
        &planning,
        before,
        work_run_id,
        &request,
        receipts.len(),
        &mut output,
    ))
    .await?;
    // Each continuation is prepared against its own captured epoch. A prior
    // pending-output receipt must not freeze this WorkItem forever.
    let scope = format!("{job_id}:{}", planning.prepared_plan()?.observed_epoch);
    Box::pin(planning.commit("reprocess_completion", &scope, &request, &output)).await?;
    Ok(output)
}

async fn retain_completion_artifact(
    ctx: &ReprocessExecutionContext,
    planning: &PlanningTruthMaintenanceStorage,
    space: &str,
    certificate: WorkArtifact,
) -> PipelineResult<()> {
    let Some(original) = WorkArtifactRepository::new(ctx.storage.clone())
        .get(space, &certificate.base.id)
        .await?
    else {
        planning.stage(None, certificate)?;
        return Ok(());
    };
    if original.artifact_type != certificate.artifact_type
        || original.status != ArtifactStatus::Committed
        || original.data != certificate.data
        || original.base.deleted_at.is_some()
        || original.base.provenance.sources != certificate.base.provenance.sources
        || !crate::maintenance_commit::same_authorization(&original.base, &certificate.base)?
    {
        return Err(PipelineError::InvalidState(
            "Original reprocess completion certificate changed".into(),
        ));
    }
    planning.observe(&original)
}

fn maintenance_id(job: &RecordId) -> RecordId {
    RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("reprocess-source-maintenance:{job}").as_bytes(),
        ),
    )
}

fn completion_request(
    selection: &SelectionManifest,
    operation_ids: &[String],
) -> serde_json::Value {
    // Initial Source selection and restored artifact traversal have different
    // orders. Bind the same original operation multiset without rewriting the
    // caller's receipts or silently removing duplicate identities.
    let mut operation_ids = operation_ids.to_vec();
    operation_ids.sort();
    serde_json::json!({
        "selection": selection.request_fingerprint,
        "selection_digest": selection.source_artifact_digest,
        "configuration": selection.configuration,
        "operations": operation_ids,
    })
}

fn validate_maintenance_seed(
    seed: &WorkArtifact,
    request: &serde_json::Value,
) -> PipelineResult<()> {
    if seed.artifact_type != MAINTENANCE_KIND
        || seed.status != ArtifactStatus::Committed
        || seed.base.deleted_at.is_some()
        || seed.data != serde_json::json!({"request_fingerprint":request_fingerprint(request)?})
    {
        return Err(PipelineError::InvalidState(
            "Original source reprocess maintenance intent changed".into(),
        ));
    }
    Ok(())
}

/// Prepare retained TM and the coordinator transition in the same overlay as
/// the actual source certificates. This helper performs no repository writes.
async fn prepare_completed_job(
    ctx: &ReprocessExecutionContext,
    planning: &PlanningTruthMaintenanceStorage,
    before: ReprocessJob,
    work_run_id: &RecordId,
    request: &serde_json::Value,
    receipt_count: usize,
    output: &mut ReprocessResult,
) -> PipelineResult<()> {
    let space = &before.base.space_id;
    planning.observe(&before)?;
    if output.error.is_none()
        && before
            .target_stages
            .contains(&ReprocessTargetStage::ReMaintain)
    {
        let seed_id = maintenance_id(&before.base.id);
        if let Some(seed) = WorkArtifactRepository::new(ctx.storage.clone())
            .get(space, &seed_id)
            .await?
        {
            validate_maintenance_seed(&seed, request)?;
            planning.observe(&seed)?;
        } else {
            Box::pin(planning.mark_all_slots_stale(space, "reprocess_supersession")).await?;
            let mut seed = artifact(
                before.base.clone(),
                seed_id,
                work_run_id,
                MAINTENANCE_KIND,
                serde_json::json!({"request_fingerprint":request_fingerprint(request)?}),
            );
            seed.status = ArtifactStatus::Committed;
            seed.committed_at = Some(Utc::now());
            planning.stage(None, seed)?;
        }
        let recompute = Box::pin(execute_tm_recompute(
            &TmContext::none(),
            space,
            RecomputePriority::Normal,
            1000,
            planning,
        ))
        .await?;
        output
            .pending_conflict_groups
            .extend(recompute.pending_conflict_groups);
        output.pending_conflict_groups.sort();
        output.pending_conflict_groups.dedup();
        output.continuation_required = recompute.slots_skipped > 0
            || !Box::pin(planning.find_stale_slots(space, usize::MAX))
                .await?
                .is_empty()
            || !output.pending_conflict_groups.is_empty();
    }
    let mut after = before.clone();
    after.progress.processed_sources = receipt_count as u64;
    after.progress.succeeded_sources = output.sources_processed;
    after.progress.failed_sources = (receipt_count as u64).saturating_sub(output.sources_processed);
    if let Some(amount) = output
        .source_usage
        .as_ref()
        .and_then(|usage| usage.monetary_euros)
    {
        after.progress.cost_spent_euros = amount;
    }
    after.error = output.error.clone();
    after.status = if output.error.is_some() {
        ReprocessStatus::Failed
    } else if output.continuation_required {
        ReprocessStatus::Paused
    } else {
        ReprocessStatus::Completed
    };
    if !output.continuation_required || output.error.is_some() {
        after.progress.progress_percent = 100.0;
        after.completed_at = Some(Utc::now());
    } else {
        after.completed_at = None;
    }
    planning.stage(Some(before), after)?;
    Ok(())
}

fn merge_usage(
    summary: &mut ReprocessSourceUsage,
    row: &crate::provider_usage::SourceOperationUsage,
) -> PipelineResult<()> {
    let add = |total: Option<u64>, value: Option<u64>| -> PipelineResult<Option<u64>> {
        match (total, value) {
            (Some(total), Some(value)) => total.checked_add(value).map(Some).ok_or_else(|| {
                PipelineError::InvalidState("Reprocess source usage overflow".into())
            }),
            _ => Ok(None),
        }
    };
    summary.operation_count = summary
        .operation_count
        .checked_add(1)
        .ok_or_else(|| PipelineError::InvalidState("Reprocess operation count overflow".into()))?;
    summary.physical_calls = summary
        .physical_calls
        .checked_add(row.physical_calls)
        .ok_or_else(|| {
            PipelineError::InvalidState("Reprocess physical call count overflow".into())
        })?;
    summary.input_tokens = add(summary.input_tokens, row.input_tokens)?;
    summary.output_tokens = add(summary.output_tokens, row.output_tokens)?;
    summary.monetary_euros = match (summary.monetary_euros, row.monetary_euros) {
        (Some(total), Some(value))
            if value.is_finite() && value >= 0.0 && (total + value).is_finite() =>
        {
            Some(total + value)
        }
        (Some(_), Some(_)) => {
            return Err(PipelineError::InvalidState(
                "Invalid reprocess source monetary evidence".into(),
            ))
        }
        _ => None,
    };
    summary.complete &= row.complete;
    Ok(())
}

fn empty_usage() -> ReprocessSourceUsage {
    ReprocessSourceUsage {
        operation_count: 0,
        physical_calls: 0,
        input_tokens: Some(0),
        output_tokens: Some(0),
        monetary_euros: Some(0.0),
        complete: true,
    }
}

async fn project_usage(
    storage: &StorageClient,
    space: &str,
    operations: &[String],
) -> PipelineResult<ReprocessSourceUsage> {
    if operations
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != operations.len()
    {
        return Err(PipelineError::InvalidState(
            "Duplicate reprocess operation accounting identity".into(),
        ));
    }
    let mut summary = empty_usage();
    for batch in operations.chunks(256) {
        for row in crate::provider_usage::source_operation_usage(storage, space, batch).await? {
            merge_usage(&mut summary, &row)?;
        }
    }
    Ok(summary)
}
