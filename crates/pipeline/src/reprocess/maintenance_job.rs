//! Re-maintenance shares one captured plan with its job transition and output.
use super::*;
use crate::maintenance_commit::PlanningTruthMaintenanceStorage;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SelectionSummary {
    selected_sources: u64,
    assertions_reprocessed: u64,
    rels_superseded: u64,
    attributes_superseded: u64,
    events_superseded: u64,
    entities_created: u64,
}

pub(super) async fn execute(
    ctx: &ReprocessExecutionContext,
    job: ReprocessJob,
    work_run_id: &RecordId,
    space: &str,
    config: &ReprocessConfig,
    planning: PlanningTruthMaintenanceStorage,
) -> PipelineResult<ReprocessResult> {
    // A continuation is a new preparation against the then-current graph. An
    // old pending-output receipt must not freeze the original work item forever.
    let scope = format!(
        "{}:{}",
        job.base.id,
        planning.prepared_plan()?.observed_epoch
    );
    let request = serde_json::json!({"job":job.base.id,"selection":job.selection_criteria,
        "stages":job.target_stages,"config":config});
    if let Some(previous) =
        Box::pin(planning.replay("reprocess_maintenance", &scope, &request)).await?
    {
        return Ok(previous);
    }
    if matches!(
        job.status,
        ReprocessStatus::Cancelled | ReprocessStatus::Failed | ReprocessStatus::Running
    ) {
        return Err(PipelineError::InvalidState(
            "Reprocess job cannot be claimed in its current state".into(),
        ));
    }
    if job.status == ReprocessStatus::Completed {
        let seed = elephant_storage::WorkArtifactRepository::new(ctx.storage.clone())
            .get(space, &seed_id(&job.base.id))
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("Completed reprocess intent unavailable".into())
            })?;
        let summary = saved_summary(&seed, &request)?;
        return Ok(ReprocessResult {
            sources_processed: job.progress.succeeded_sources,
            assertions_reprocessed: summary.assertions_reprocessed,
            rels_superseded: summary.rels_superseded,
            attributes_superseded: summary.attributes_superseded,
            events_superseded: summary.events_superseded,
            entities_created: summary.entities_created,
            ..Default::default()
        });
    }
    if let Some(reason) = Box::pin(budget::job_refusal(
        &ctx.storage,
        &job,
        should_reresolve(&job.target_stages),
    ))
    .await?
    {
        return Box::pin(budget::pause(&planning, job, config, reason)).await;
    }
    let refusal_plan = job
        .budget_limit_euros
        .map(|_| planning.fork())
        .transpose()?;
    let budget_reached = check_budget_limit(&job);
    let paused = (budget_reached && config.pause_on_budget_limit)
        || remaining_daily_capacity(&job, Utc::now()) == Some(0)
        || config.batch_size == 0;
    let mut result = ReprocessResult {
        paused,
        continuation_required: paused,
        ..Default::default()
    };
    let mut updated = job.clone();
    updated.started_at = updated.started_at.or(Some(Utc::now()));
    if budget_reached && !config.pause_on_budget_limit {
        result.error = Some("Budget limit exceeded".into());
        updated.status = ReprocessStatus::Failed;
        updated.error = result.error.clone();
    } else if paused {
        updated.status = ReprocessStatus::Paused;
    } else {
        let seed_id = seed_id(&job.base.id);
        let seed = elephant_storage::WorkArtifactRepository::new(ctx.storage.clone())
            .get(space, &seed_id)
            .await?;
        let summary: SelectionSummary = if let Some(seed) = seed {
            planning.observe(&seed)?;
            saved_summary(&seed, &request)?
        } else {
            let selected = Box::pin(select_sources_for_reprocess(
                ctx,
                space,
                &job.selection_criteria,
                usize::MAX,
            ))
            .await?;
            for source in &selected {
                planning.observe(source)?;
            }
            let mut summary = SelectionSummary {
                selected_sources: selected.len() as u64,
                ..Default::default()
            };
            if should_reresolve(&job.target_stages) {
                let options = Box::pin(resolution_options(
                    ctx,
                    space,
                    work_run_id,
                    &job.base.created_by,
                ))
                .await?;
                let resolved = Box::pin(crate::resolution::reprocess::prepare_reprocess(
                    &planning,
                    &ctx.storage,
                    space,
                    &selected,
                    &options,
                ))
                .await?;
                summary.assertions_reprocessed = resolved.assertions_reprocessed;
                summary.rels_superseded = resolved.rels_superseded;
                summary.attributes_superseded = resolved.attributes_superseded;
                summary.events_superseded = resolved.events_superseded;
                summary.entities_created = resolved.entities_created;
                result.pending_conflict_groups = resolved.pending_conflict_groups;
            }
            planning
                .mark_all_slots_stale(space, "reprocess_supersession")
                .await?;
            let mut base = job.base.clone();
            base.id = seed_id;
            base.created_at = Utc::now();
            base.updated_at = base.created_at;
            planning.stage(
                None,
                elephant_types::records::WorkArtifact {
                    base,
                    work_run: Link::new(work_run_id.clone()),
                    artifact_type: "reprocess_maintenance_seed/v1".into(),
                    data: serde_json::json!({"request":request,"summary":summary}),
                    status: elephant_types::records::ArtifactStatus::Committed,
                    validation_errors: None,
                    validated_at: Some(Utc::now()),
                    committed_at: Some(Utc::now()),
                },
            )?;
            summary
        };
        result.sources_processed = summary.selected_sources;
        result.assertions_reprocessed = summary.assertions_reprocessed;
        result.rels_superseded = summary.rels_superseded;
        result.attributes_superseded = summary.attributes_superseded;
        result.events_superseded = summary.events_superseded;
        result.entities_created = summary.entities_created;
        let recompute = Box::pin(execute_tm_recompute(
            &TmContext::none(),
            space,
            RecomputePriority::Normal,
            1000,
            &planning,
        ))
        .await?;
        result
            .pending_conflict_groups
            .extend(recompute.pending_conflict_groups);
        result.pending_conflict_groups.sort();
        result.pending_conflict_groups.dedup();
        if !result.pending_conflict_groups.is_empty() {
            if let Some(refusal_plan) = refusal_plan {
                // The fork retains only the original epoch/read set. Neither
                // seed, slot changes nor provider children from this speculative
                // preparation may escape a finite currency refusal.
                return Box::pin(budget::pause(&refusal_plan, job, config, budget::UNPRICED)).await;
            }
        }
        result.continuation_required = recompute.slots_skipped > 0
            || !planning
                .find_stale_slots(space, usize::MAX)
                .await?
                .is_empty()
            || !result.pending_conflict_groups.is_empty();
        // Seed invalidation once; subsequent continuations process only remaining
        // stale slots and never dirty the children they just finished.
        updated.status = if result.continuation_required {
            ReprocessStatus::Paused
        } else {
            ReprocessStatus::Completed
        };
        updated.progress.total_sources = summary.selected_sources;
        if !result.continuation_required {
            updated.progress.processed_sources = summary.selected_sources;
            updated.progress.succeeded_sources = summary.selected_sources;
            updated.progress.progress_percent = 100.0;
            updated.completed_at = Some(Utc::now());
        }
    }
    planning.stage(Some(job), updated)?;
    Box::pin(planning.commit("reprocess_maintenance", &scope, &request, &result)).await?;
    Ok(result)
}

fn seed_id(job: &RecordId) -> RecordId {
    RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("reprocess-maintenance:{job}").as_bytes(),
        ),
    )
}

fn saved_summary(
    seed: &elephant_types::records::WorkArtifact,
    request: &serde_json::Value,
) -> PipelineResult<SelectionSummary> {
    if seed.artifact_type != "reprocess_maintenance_seed/v1"
        || seed.status != elephant_types::records::ArtifactStatus::Committed
        || seed.data.get("request") != Some(request)
    {
        return Err(PipelineError::InvalidState(
            "Reprocess maintenance intent changed".into(),
        ));
    }
    serde_json::from_value(seed.data["summary"].clone())
        .map_err(|error| PipelineError::InvalidState(format!("Invalid reprocess summary: {error}")))
}

async fn resolution_options(
    ctx: &ReprocessExecutionContext,
    space: &str,
    run: &RecordId,
    actor: &str,
) -> PipelineResult<crate::resolution::reprocess::ReprocessResolutionOptions> {
    use elephant_types::provider_binding::ModelOperation;
    let context = crate::provider_dispatch::ProviderDispatchContext::new(
        elephant_storage::SpaceConfigRepository::new(ctx.storage.clone()),
        space,
    );
    let resolved = context
        .repository
        .resolve_space(space)
        .await
        .map_err(|error| match error {
            elephant_storage::space_config_repository::SpaceResolutionError::Storage(error) => {
                PipelineError::StorageError(error)
            }
            error => PipelineError::ConfigError(format!("provider_authorization: {error}")),
        })?;
    let disambiguator = if resolved
        .processing
        .bindings
        .contains_key(&ModelOperation::PromotionDisambiguation)
    {
        #[cfg(feature = "meerkat-extraction")]
        {
            use crate::scheduler::promotion_disambiguation::{
                AgenticPromotionDisambiguator, PromotionDisambiguationConfig,
                PromotionDisambiguator,
            };
            Some(std::sync::Arc::new(
                AgenticPromotionDisambiguator::new(PromotionDisambiguationConfig::default())
                    .with_provider_context(context)
                    .await?,
            ) as std::sync::Arc<dyn PromotionDisambiguator>)
        }
        #[cfg(not(feature = "meerkat-extraction"))]
        return Err(PipelineError::ConfigError(
            "Approved promotion disambiguation requires meerkat-extraction".into(),
        ));
    } else {
        None
    };
    let mut options =
        crate::resolution::reprocess::ReprocessResolutionOptions::new(actor, run.clone());
    options.disambiguator = disambiguator;
    Ok(options)
}
