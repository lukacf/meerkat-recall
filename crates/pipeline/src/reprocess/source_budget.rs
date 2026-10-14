//! Resolve the immutable private source-operation producer, then verify its
//! actual saved selection. Missing history cannot become unlimited dispatch.
use super::*;
use elephant_types::publication::SourceProducerOrigin;

fn unavailable(reason: &str) -> PipelineError {
    PipelineError::ConfigError(format!(
        "Original reprocess budget authority is incomplete: {reason}"
    ))
}

pub(crate) async fn guard_source_dispatch(
    storage: &StorageClient,
    space: &str,
    original: &PublicationFence,
) -> PipelineResult<()> {
    guard_source_dispatch_in_walk(
        storage,
        space,
        original,
        &mut budget::ProducerWalk::default(),
    )
    .await
}

pub(in crate::reprocess) async fn guard_source_dispatch_in_walk(
    storage: &StorageClient,
    space: &str,
    original: &PublicationFence,
    walk: &mut budget::ProducerWalk,
) -> PipelineResult<()> {
    let Some(key) = walk.enter(
        budget::ProducerNode::Source,
        request_fingerprint(&("source", space, original))?,
    )?
    else {
        return Ok(());
    };
    let result = guard_source_dispatch_body(storage, space, original, walk).await;
    walk.leave(&key, result.is_ok());
    result
}

async fn guard_source_dispatch_body(
    storage: &StorageClient,
    space: &str,
    original: &PublicationFence,
    walk: &mut budget::ProducerWalk,
) -> PipelineResult<()> {
    let publication = SourcePublicationRepository::new(storage.clone());
    let producer = publication
        .source_operation_producer(space, original)
        .await?;
    let (
        job_id,
        selection_id,
        intent_id,
        selection_digest,
        intent_fingerprint,
        original_currency_restricted,
        original_attempt,
    ) = match producer {
        SourceProducerOrigin::UnknownOriginal => {
            return Err(unavailable("source operation has no saved producer"));
        }
        SourceProducerOrigin::Direct => return Ok(()),
        SourceProducerOrigin::Watch {
            ref work_item_id,
            ref work_run_id,
            ..
        } => {
            let work = elephant_storage::WorkItemRepository::new(storage.clone())
                .get(space, work_item_id)
                .await?
                .ok_or_else(|| unavailable("original watch work item is missing"))?;
            let run = elephant_storage::WorkRunRepository::new(storage.clone())
                .get(space, work_run_id)
                .await?
                .ok_or_else(|| unavailable("original watch work run is missing"))?;
            // The producer has already dispatched; its old lease need not still
            // be live. Exact original intent, run and attempt remain mandatory.
            elephant_storage::source_watch_producer::verify_watch_origin(&producer, &work, &run)?;
            return Box::pin(budget::guard_physical_dispatch_in_walk(
                storage, &work, walk,
            ))
            .await;
        }
        SourceProducerOrigin::Reprocess {
            job_id,
            selection_artifact_id,
            source_intent_id,
            selection_digest,
            intent_fingerprint,
            currency_restricted,
            original_attempt,
        } => (
            job_id,
            selection_artifact_id,
            source_intent_id,
            selection_digest,
            intent_fingerprint,
            currency_restricted,
            original_attempt,
        ),
    };
    let job = ReprocessJobRepository::new(storage.clone())
        .get(space, &job_id)
        .await?
        .ok_or_else(|| unavailable("original reprocess job is missing"))?;
    // The original job restriction was captured before any coordinator claim;
    // a later operation attachment cannot turn that finite intent into None.
    let job_restricted = budget::original_currency_restricted(storage, &job).await?;
    let repo = WorkArtifactRepository::new(storage.clone());
    let index = repo
        .get(space, &selection_id)
        .await?
        .ok_or_else(|| unavailable("original selection index is missing"))?;
    let manifest: SelectionManifest = decode(index.data.clone())?;
    let rows = load_source_artifacts(&repo, space, &job_id).await?;
    if selection_id != index_id(&job_id)
        || index.artifact_type != INDEX_KIND
        || index.status != ArtifactStatus::Validated
        || index.validated_at.is_none()
        || manifest.job_id != job_id
        || manifest.source_artifact_digest != selection_digest
        || rows.len() != manifest.source_count
        || artifacts_digest(&rows)? != selection_digest
        || !crate::maintenance_commit::same_authorization(&index.base, &job.base)?
        || index.base.provenance != job.base.provenance
        || index.base.created_by != job.base.created_by
    {
        return Err(unavailable(
            "selection manifest, digest or job envelope differs",
        ));
    }
    let row = rows
        .iter()
        .find(|row| row.base.id == intent_id)
        .ok_or_else(|| unavailable("original source intent is missing"))?;
    if row.artifact_type != SOURCE_KIND
        || row.status != ArtifactStatus::Validated
        || row.validated_at.is_none()
        || row.work_run != index.work_run
        || elephant_storage::canonical_commit::source_support_fingerprint(row)?
            != intent_fingerprint
    {
        return Err(unavailable("saved source intent artifact differs"));
    }
    let intent: SourceIntent = decode(row.data.clone())?;
    let authority = &intent.authorized.envelope;
    if intent.job_id != job_id
        || intent_id != source_artifact_id(&job_id, &original.source_id)
        || intent.source.base.id != original.source_id
        || intent.revision_id != original.revision_id
        || intent.configuration != manifest.configuration
        || intent.source.base.space_id != space
        || authority.space_id != space
        || authority.source_id != original.source_id
        || row.base.security != authority.security
        || row.base.subjects != authority.subjects
        || row.base.subjects_state != authority.subjects_state
        || row.base.provenance.sources != vec![original.source_id.clone()]
        || intent.request_fingerprint
            != recipe_fingerprint(
                &job_id,
                &intent.source,
                &intent.expected,
                &intent.configuration,
                &job.target_stages,
                intent.contact_authority.as_ref(),
            )?
    {
        return Err(unavailable(
            "source intent, recipe or authorization envelope differs",
        ));
    }
    let receipt = publication
        .operation_by_idempotency_key(space, &intent.idempotency_key)
        .await?
        .ok_or_else(|| unavailable("original admission receipt is missing"))?;
    if receipt.space_id != space
        || receipt.operation_id != original.operation_id
        || receipt.source_id != original.source_id
        || receipt.source_generation != original.generation
        || receipt.desired_revision.as_deref() != Some(original.revision_id.as_str())
    {
        return Err(unavailable("original admission receipt identity differs"));
    }
    let revision = publication
        .revision(space, &original.source_id, &original.revision_id)
        .await?
        .ok_or_else(|| unavailable("accepted immutable revision is missing"))?;
    if elephant_storage::canonical_commit::source_support_fingerprint(&revision.source)?
        != elephant_storage::canonical_commit::source_support_fingerprint(&intent.source)?
    {
        return Err(unavailable(
            "accepted revision differs from selected immutable input",
        ));
    }
    if let Some(attempt) = original_attempt {
        // This is historical accounting evidence, not a fresh lease grant.
        // The physical source dispatcher separately checks its own live attempt.
        let parent = elephant_storage::WorkItemRepository::new(storage.clone())
            .get(space, &attempt.work_item_id)
            .await?
            .ok_or_else(|| unavailable("original admission work item is missing"))?;
        // Selection can commit in an earlier attempt than this source's
        // admission. Find the real admission run by its saved original fence;
        // rebinding it to the selection artifact's run would reject recovery.
        let runs = elephant_storage::WorkRunRepository::new(storage.clone())
            .find_by_work_item(&parent.base.id)
            .await?;
        let matches = runs
            .iter()
            .filter(|run| {
                run.base.space_id == space
                    && run.base.deleted_at.is_none()
                    && run.work_item.id() == &parent.base.id
                    && run
                        .metrics
                        .custom
                        .get("attempt_fence")
                        .and_then(|value| {
                            serde_json::from_value::<AttemptFence>(value.clone()).ok()
                        })
                        .as_ref()
                        == Some(&attempt)
            })
            .count();
        let origins = elephant_storage::work_producer_repository::WorkProducerRepository::new(
            storage.clone(),
        );
        let origin = origins.origin_for_work(&parent).await?;
        elephant_storage::work_producer_repository::verify_reprocess_origin(
            &origin, &job, &parent,
        )?;
        if matches != 1 || attempt.space_id != space {
            return Err(unavailable(
                "original admission attempt has no unique matching run",
            ));
        }
    }
    if (original_currency_restricted || job_restricted) && job.budget_limit_euros.is_none() {
        return Err(PipelineError::BudgetExceeded(
            "Original finite currency restriction cannot be removed by changing the job budget"
                .into(),
        ));
    }
    if let Some(reason) = budget::refusal(&job, true) {
        return Err(PipelineError::BudgetExceeded(reason.into()));
    }
    budget::guard_job_execution_parent_in_walk(storage, &job, walk).await
}
