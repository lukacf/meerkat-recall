//! Real repository assertions for durable intent preparation; no model calls.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use elephant_types::provider_binding::SpaceProcessingConfig;
use elephant_types::records::SpaceConfig;

const SPACE: &str = "reprocess-intent-oracle";

fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), SPACE.into(), "oracle".into())
}

async fn setup(embedded: bool) -> (ReprocessExecutionContext, RecordId) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        elephant_migrations::apply_schema(&storage).await.unwrap();
        storage
    };
    SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id: "oracle-policy".into(),
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
    let ctx = ReprocessExecutionContext {
        storage: storage.clone(),
        job_repo: ReprocessJobRepository::new(storage.clone()),
        source_repo: SourceRepository::new(storage.clone()),
        doc_repo: DocRepository::new(storage.clone()),
        doc_revision_repo: DocRevisionRepository::new(storage.clone()),
        assertion_repo: AssertionRepository::new(storage.clone()),
        work_run_repo: WorkRunRepository::new(storage),
    };
    let run = WorkRun {
        base: base(TableName::WorkRun),
        work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
        attempt: 0,
        started_at: Utc::now(),
        completed_at: None,
        success: false,
        error: None,
        metrics: Default::default(),
        worker_id: "oracle".into(),
        worker_version: "1".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    let id = run.base.id.clone();
    ctx.work_run_repo.create(run).await.unwrap();
    (ctx, id)
}

async fn job(ctx: &ReprocessExecutionContext, config: &ReprocessConfig) -> ReprocessJob {
    job_for_stage(ctx, config, ReprocessTargetStage::ReExtract).await
}

async fn job_for_stage(
    ctx: &ReprocessExecutionContext,
    config: &ReprocessConfig,
    stage: ReprocessTargetStage,
) -> ReprocessJob {
    create_reprocess_job(
        &ctx.job_repo,
        "replacement oracle",
        SelectionCriteria {
            source_types: vec!["manual".into()],
            ..Default::default()
        },
        vec![stage],
        SPACE,
        "oracle",
        Some(config),
    )
    .await
    .unwrap()
    .job
}

async fn source(ctx: &ReprocessExecutionContext) -> Source {
    let mut source = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "original".into(),
    );
    source.metadata = serde_json::json!({"_manual_doc":{"text":"Ada works at the laboratory."}});
    ctx.source_repo.create(source).await.unwrap()
}

#[test]
fn intent_digest_binds_payload_and_envelope_but_ignores_storage_timestamp() {
    let run = RecordId::new_v4(TableName::WorkRun);
    let first = artifact(
        base(TableName::WorkArtifact),
        RecordId::new_v4(TableName::WorkArtifact),
        &run,
        SOURCE_KIND,
        serde_json::json!({"revision":"original"}),
    );
    let expected = artifacts_digest(std::slice::from_ref(&first)).unwrap();
    let mut changed = first.clone();
    changed.data["revision"] = serde_json::json!("replacement");
    assert_ne!(expected, artifacts_digest(&[changed]).unwrap());
    let mut changed = first.clone();
    changed
        .base
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    assert_ne!(expected, artifacts_digest(&[changed]).unwrap());
    let mut timestamp_only = first;
    timestamp_only.base.updated_at += chrono::Duration::seconds(1);
    assert_eq!(expected, artifacts_digest(&[timestamp_only]).unwrap());
}

#[tokio::test]
async fn editable_contact_metadata_without_accepted_capability_never_creates_reprocess_intent() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let mut input = Source::new(
            base(TableName::Source),
            SourceType::Manual,
            "unsealed".into(),
        );
        input.metadata = serde_json::json!({"contact":{"name":"Unsealed Ada","company":"Lab"}});
        let input = ctx.source_repo.create(input).await.unwrap();
        assert!(
            execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
                .await
                .is_err()
        );
        assert!(SourcePublicationRepository::new(ctx.storage.clone())
            .head(SPACE, &input.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(WorkArtifactRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            ctx.job_repo.get(SPACE, &job.base.id).await.unwrap(),
            Some(job)
        );
    }
}

#[tokio::test]
async fn contact_recipe_binds_original_capability_without_changing_plain_source_identity() {
    let (ctx, _) = setup(true).await;
    let input = source(&ctx).await;
    let job = job(&ctx, &ReprocessConfig::default()).await;
    let configuration = SpaceConfigRepository::new(ctx.storage.clone())
        .resolve_space(SPACE)
        .await
        .unwrap()
        .token;
    let expected = SourcePrecondition::Absent;
    let plain = recipe_fingerprint(
        &job.base.id,
        &input,
        &expected,
        &configuration,
        &job.target_stages,
        None,
    )
    .unwrap();
    assert_eq!(
        plain,
        request_fingerprint(&(
            &job.base.id,
            &input,
            &expected,
            &configuration,
            &job.target_stages
        ))
        .unwrap()
    );
    // Unit identity check only: these descriptors are not accepted proof and
    // cannot bypass the owner's real publication/seal lookup at admission.
    let mut authority = ContactAuthority {
        publication: PublicationFence {
            source_id: input.base.id.clone(),
            generation: "original".into(),
            revision_id: "r1".into(),
            operation_id: "operation".into(),
        },
        seal_fingerprint: None,
        admission_digest: Some("original-seal".into()),
    };
    let first = recipe_fingerprint(
        &job.base.id,
        &input,
        &expected,
        &configuration,
        &job.target_stages,
        Some(&authority),
    )
    .unwrap();
    assert_ne!(plain, first);
    authority.admission_digest = Some("changed-seal".into());
    assert_ne!(
        first,
        recipe_fingerprint(
            &job.base.id,
            &input,
            &expected,
            &configuration,
            &job.target_stages,
            Some(&authority)
        )
        .unwrap()
    );
}

#[tokio::test]
async fn accepted_work_is_pending_and_retry_preserves_original_operation() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let source = source(&ctx).await;
        let first = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert_eq!(first.sources_processed, 0);
        assert_eq!(first.pending_operations.len(), 1);
        let usage = first.source_usage.as_ref().unwrap();
        assert_eq!(usage.operation_count, 1);
        assert_eq!(usage.physical_calls, 0);
        assert!(!usage.complete);
        assert_eq!(
            usage.monetary_euros, None,
            "unstarted accepted work is not measured zero"
        );
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let accepted = publication
            .head(SPACE, &source.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.published_revision, None);
        let retry = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert_eq!(retry.pending_operations, first.pending_operations);
        assert_eq!(
            publication.head(SPACE, &source.base.id).await.unwrap(),
            Some(accepted)
        );
        let current_job = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current_job.status, ReprocessStatus::Running);
        assert_eq!(current_job.progress.succeeded_sources, 0);
        assert!(current_job.completed_at.is_none());
    }
}

#[tokio::test]
async fn changed_job_recipe_cannot_reuse_saved_selection() {
    let (ctx, run) = setup(true).await;
    let config = ReprocessConfig::default();
    let job = job(&ctx, &config).await;
    let source = source(&ctx).await;
    execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
        .await
        .unwrap();
    let publication = SourcePublicationRepository::new(ctx.storage.clone());
    let accepted = publication.head(SPACE, &source.base.id).await.unwrap();
    let changed = ReprocessConfig {
        batch_size: config.batch_size + 1,
        ..config
    };
    assert!(
        execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &changed)
            .await
            .is_err()
    );
    assert_eq!(
        publication.head(SPACE, &source.base.id).await.unwrap(),
        accepted
    );
}

#[tokio::test]
async fn erased_intent_is_not_recreated_or_counted_complete() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let source = source(&ctx).await;
        execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        let repo = WorkArtifactRepository::new(ctx.storage.clone());
        let id = source_artifact_id(&job.base.id, &source.base.id);
        repo.delete(SPACE, &id).await.unwrap();
        assert!(
            execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
                .await
                .is_err()
        );
        assert!(repo.get(SPACE, &id).await.unwrap().is_none());
        let current = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(current.status, ReprocessStatus::Completed);
        assert!(current.completed_at.is_none());
    }
}

#[tokio::test]
async fn batch_limit_paces_admission_without_truncating_durable_selection() {
    let (ctx, run) = setup(true).await;
    let config = ReprocessConfig {
        batch_size: 1,
        ..Default::default()
    };
    let job = job(&ctx, &config).await;
    source(&ctx).await;
    source(&ctx).await;
    let output = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
        .await
        .unwrap();
    assert!(output.continuation_required);
    assert_eq!(output.pending_operations.len(), 1);
    let repo = WorkArtifactRepository::new(ctx.storage.clone());
    let index = repo
        .get(SPACE, &index_id(&job.base.id))
        .await
        .unwrap()
        .unwrap();
    let manifest: SelectionManifest = decode(index.data).unwrap();
    assert_eq!(manifest.source_count, 2);
    assert_eq!(
        load_source_artifacts(&repo, SPACE, &job.base.id)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn budget_failure_has_no_partial_intent_or_source_acceptance() {
    let (ctx, run) = setup(true).await;
    let config = ReprocessConfig {
        default_budget_limit_euros: Some(0.0),
        pause_on_budget_limit: false,
        ..Default::default()
    };
    let job = job(&ctx, &config).await;
    let source = source(&ctx).await;
    let output = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
        .await
        .unwrap();
    assert!(!output.paused);
    assert!(output.error.is_some());
    assert_eq!(
        ctx.job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReprocessStatus::Failed
    );
    assert!(SourcePublicationRepository::new(ctx.storage.clone())
        .head(SPACE, &source.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(WorkArtifactRepository::new(ctx.storage.clone())
        .get(SPACE, &index_id(&job.base.id))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn finite_unpriced_jobs_pause_or_fail_before_any_source_or_resolution_effect() {
    for embedded in [true, false] {
        for stage in [
            ReprocessTargetStage::ReExtract,
            ReprocessTargetStage::ReResolve,
        ] {
            for pause in [true, false] {
                let (ctx, run) = setup(embedded).await;
                let config = ReprocessConfig {
                    default_budget_limit_euros: Some(25.0),
                    pause_on_budget_limit: pause,
                    ..Default::default()
                };
                let job = job_for_stage(&ctx, &config, stage).await;
                let source = source(&ctx).await;
                let result = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
                    .await
                    .unwrap();
                assert_eq!(result.paused, pause);
                assert_eq!(result.continuation_required, pause);
                assert!(result.pending_conflict_groups.is_empty());
                assert!(result.pending_operations.is_empty());
                let saved = ctx
                    .job_repo
                    .get(SPACE, &job.base.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    saved.status,
                    if pause {
                        ReprocessStatus::Paused
                    } else {
                        ReprocessStatus::Failed
                    }
                );
                assert_eq!(saved.error.as_deref(), Some(budget::UNPRICED));
                assert!(SourcePublicationRepository::new(ctx.storage.clone())
                    .head(SPACE, &source.base.id)
                    .await
                    .unwrap()
                    .is_none());
                assert!(WorkArtifactRepository::new(ctx.storage.clone())
                    .list(SPACE, 100, None)
                    .await
                    .unwrap()
                    .is_empty());
            }
        }
    }
}

#[tokio::test]
async fn restored_selection_cannot_bypass_a_finite_currency_restriction() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let source = source(&ctx).await;
        let first = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert_eq!(first.pending_operations.len(), 1);
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let head = publication.head(SPACE, &source.base.id).await.unwrap();
        assert!(
            head.as_ref().unwrap().published_revision.is_none(),
            "the refusal must work before source publication"
        );
        let mut current = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        current.budget_limit_euros = Some(5.0);
        ctx.job_repo
            .update(SPACE, &job.base.id, current)
            .await
            .unwrap();
        let retry = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert!(retry.paused);
        assert!(retry.pending_operations.is_empty());
        let paused = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused.status, ReprocessStatus::Paused);
        let audit = elephant_storage::AuditLogRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .into_iter()
            .find(|audit| {
                audit.record == job.base.id
                    && audit.reason.as_deref() == Some("reprocess_currency_budget_refusal")
            })
            .expect("the same transaction records the job refusal");
        assert_eq!(
            audit.after.as_ref().unwrap()["status"],
            serde_json::json!(ReprocessStatus::Paused)
        );
        assert!(
            publication
                .support_for_record(SPACE, &job.base.id)
                .await
                .unwrap()
                .is_empty(),
            "job controls cannot manufacture publication support"
        );
        assert_eq!(
            publication.head(SPACE, &source.base.id).await.unwrap(),
            head
        );
    }
}

#[tokio::test]
async fn physical_source_guard_uses_original_receipt_and_rejects_withdrawn_selection() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let source = source(&ctx).await;
        let result = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let receipt = publication
            .operation(SPACE, &result.pending_operations[0])
            .await
            .unwrap()
            .unwrap();
        let original = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: receipt.desired_revision.unwrap(),
            operation_id: receipt.operation_id,
        };
        guard_source_dispatch(&ctx.storage, SPACE, &original)
            .await
            .unwrap();
        let mut invented = original.clone();
        invented.generation = "later-generation".into();
        assert!(guard_source_dispatch(&ctx.storage, SPACE, &invented)
            .await
            .is_err());
        let mut current = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        current.budget_limit_euros = Some(10.0);
        ctx.job_repo
            .update(SPACE, &job.base.id, current)
            .await
            .unwrap();
        assert!(matches!(
            guard_source_dispatch(&ctx.storage, SPACE, &original).await,
            Err(PipelineError::BudgetExceeded(_))
        ));
        WorkArtifactRepository::new(ctx.storage.clone())
            .delete(SPACE, &source_artifact_id(&job.base.id, &source.base.id))
            .await
            .unwrap();
        assert!(matches!(
            guard_source_dispatch(&ctx.storage, SPACE, &original).await,
            Err(PipelineError::ConfigError(_))
        ));
    }
}

#[tokio::test]
async fn physical_source_guard_cannot_treat_a_deleted_job_as_unbudgeted() {
    let (ctx, run) = setup(true).await;
    let config = ReprocessConfig::default();
    let job = job(&ctx, &config).await;
    source(&ctx).await;
    let result = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
        .await
        .unwrap();
    let publication = SourcePublicationRepository::new(ctx.storage.clone());
    let receipt = publication
        .operation(SPACE, &result.pending_operations[0])
        .await
        .unwrap()
        .unwrap();
    let original = PublicationFence {
        source_id: receipt.source_id,
        generation: receipt.source_generation,
        revision_id: receipt.desired_revision.unwrap(),
        operation_id: receipt.operation_id,
    };
    ctx.job_repo.delete(SPACE, &job.base.id).await.unwrap();
    assert!(matches!(
        guard_source_dispatch(&ctx.storage, SPACE, &original).await,
        Err(PipelineError::ConfigError(_))
    ));
}

#[tokio::test]
async fn stale_capture_rolls_back_selection_job_and_artifacts() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let job = job(&ctx, &config).await;
        let source = source(&ctx).await;
        let planning = PlanningTruthMaintenanceStorage::new(ctx.storage.clone(), SPACE, None)
            .await
            .unwrap();
        planning.observe(&job).unwrap();
        let repo = SpaceConfigRepository::new(ctx.storage.clone());
        let before = repo.get_by_space_id(SPACE).await.unwrap().unwrap();
        let mut changed = before.clone();
        changed.processing.as_mut().unwrap().generation += 1;
        repo.replace_authorized(&before, changed).await.unwrap();
        assert!(
            execute(&ctx, job.clone(), &run, SPACE, &config, None, planning)
                .await
                .is_err()
        );
        assert_eq!(
            ctx.job_repo.get(SPACE, &job.base.id).await.unwrap(),
            Some(job.clone())
        );
        assert!(WorkArtifactRepository::new(ctx.storage.clone())
            .get(SPACE, &index_id(&job.base.id))
            .await
            .unwrap()
            .is_none());
        assert!(SourcePublicationRepository::new(ctx.storage.clone())
            .head(SPACE, &source.base.id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn remaintain_completion_and_seed_are_atomic_and_changed_replay_conflicts() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig {
            default_budget_limit_euros: Some(10.0),
            ..Default::default()
        };
        let job = job_for_stage(&ctx, &config, ReprocessTargetStage::ReMaintain).await;
        let first = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert!(!first.continuation_required);
        assert!(first.pending_conflict_groups.is_empty());
        let completed = ctx
            .job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, ReprocessStatus::Completed);
        assert!(completed.completed_at.is_some());
        let rows = WorkArtifactRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, ArtifactStatus::Committed);
        assert_eq!(rows[0].work_run.id(), &run);
        let retry = execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert!(!retry.continuation_required);
        let changed = ReprocessConfig {
            batch_size: config.batch_size + 1,
            ..config
        };
        assert!(
            execute_reprocess_job(&ctx, &job.base.id, &run, SPACE, &changed)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn remaintain_stale_epoch_leaves_job_and_seed_unmodified() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let mut job = job(&ctx, &config).await;
        job.target_stages = vec![ReprocessTargetStage::ReMaintain];
        let job = ctx
            .job_repo
            .update(SPACE, &job.base.id, job.clone())
            .await
            .unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(ctx.storage.clone(), SPACE, None)
            .await
            .unwrap();
        planning.observe(&job).unwrap();
        let repo = SpaceConfigRepository::new(ctx.storage.clone());
        let before = repo.get_by_space_id(SPACE).await.unwrap().unwrap();
        let mut after = before.clone();
        after.processing.as_mut().unwrap().generation += 1;
        repo.replace_authorized(&before, after).await.unwrap();
        assert!(super::super::maintenance_job::execute(
            &ctx,
            job.clone(),
            &run,
            SPACE,
            &config,
            planning
        )
        .await
        .is_err());
        assert_eq!(
            ctx.job_repo.get(SPACE, &job.base.id).await.unwrap(),
            Some(job)
        );
        assert!(WorkArtifactRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}

#[test]
fn source_usage_unknowns_are_not_laundered_by_known_zero_operations() {
    use crate::provider_usage::SourceOperationUsage;
    let known = SourceOperationUsage {
        operation_id: "known".into(),
        monetary_euros: Some(0.0),
        input_tokens: Some(0),
        output_tokens: Some(0),
        complete: true,
        physical_calls: 0,
    };
    let unknown = SourceOperationUsage {
        operation_id: "unknown".into(),
        monetary_euros: None,
        input_tokens: None,
        output_tokens: Some(12),
        complete: false,
        physical_calls: 1,
    };
    for rows in [[&known, &unknown], [&unknown, &known]] {
        let mut summary = empty_usage();
        for row in rows {
            merge_usage(&mut summary, row).unwrap();
        }
        assert_eq!(summary.operation_count, 2);
        assert_eq!(summary.monetary_euros, None);
        assert_eq!(summary.input_tokens, None);
        assert_eq!(summary.output_tokens, Some(12));
        assert_eq!(summary.physical_calls, 1);
        assert!(!summary.complete);
    }
    let mut summary = empty_usage();
    summary.physical_calls = u64::MAX;
    assert!(merge_usage(&mut summary, &unknown).is_err());
}

#[tokio::test]
async fn original_finite_job_cannot_become_unrestricted_before_first_admission() {
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig {
            default_budget_limit_euros: Some(10.0),
            ..Default::default()
        };
        let original = job(&ctx, &config).await;
        let source = source(&ctx).await;
        let mut changed = original.clone();
        changed.budget_limit_euros = None;
        ctx.job_repo
            .update(SPACE, &changed.base.id.clone(), changed)
            .await
            .unwrap();
        let result = execute_reprocess_job(&ctx, &original.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        assert!(
            result.paused,
            "original finite authority remains restricted"
        );
        assert!(result.pending_operations.is_empty());
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        assert!(publication
            .head(SPACE, &source.base.id)
            .await
            .unwrap()
            .is_none());
        let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
        assert!(artifacts
            .get(SPACE, &index_id(&original.base.id))
            .await
            .unwrap()
            .is_none());
        // A distinct fresh unlimited job is separate explicit authority.
        let unlimited = job(&ctx, &ReprocessConfig::default()).await;
        let admitted = execute_reprocess_job(
            &ctx,
            &unlimited.base.id,
            &run,
            SPACE,
            &ReprocessConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(admitted.pending_operations.len(), 1);
    }
}

#[tokio::test]
async fn private_source_origin_survives_deleted_controls_and_does_not_taint_direct_intake() {
    use elephant_types::publication::SourceProducerOrigin;
    for embedded in [true, false] {
        let (ctx, run) = setup(embedded).await;
        let config = ReprocessConfig::default();
        let original = job(&ctx, &config).await;
        let selected = source(&ctx).await;
        let result = execute_reprocess_job(&ctx, &original.base.id, &run, SPACE, &config)
            .await
            .unwrap();
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let receipt = publication
            .operation(SPACE, &result.pending_operations[0])
            .await
            .unwrap()
            .unwrap();
        let fence = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: receipt.desired_revision.unwrap(),
            operation_id: receipt.operation_id,
        };
        let origin = publication
            .source_operation_producer(SPACE, &fence)
            .await
            .unwrap();
        assert!(matches!(&origin, SourceProducerOrigin::Reprocess {
            job_id, selection_artifact_id, source_intent_id, currency_restricted: false, ..
        } if *job_id == original.base.id && *selection_artifact_id == index_id(&original.base.id)
            && *source_intent_id == source_artifact_id(&original.base.id, &selected.base.id)));
        guard_source_dispatch(&ctx.storage, SPACE, &fence)
            .await
            .unwrap();
        let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
        artifacts
            .delete(
                SPACE,
                &source_artifact_id(&original.base.id, &selected.base.id),
            )
            .await
            .unwrap();
        artifacts
            .delete(SPACE, &index_id(&original.base.id))
            .await
            .unwrap();
        assert!(matches!(
            guard_source_dispatch(&ctx.storage, SPACE, &fence).await,
            Err(PipelineError::ConfigError(_))
        ));
        assert_eq!(
            publication
                .source_operation_producer(SPACE, &fence)
                .await
                .unwrap(),
            origin
        );
        // No all-space artifact scan: another operation's missing controls must
        // neither grant this producer authority nor disable explicit direct work.
        let direct = source(&ctx).await;
        let receipt = publication
            .accept(AcceptSourceRevision {
                source: direct.clone(),
                revision_id: "direct-original".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "direct-original".into(),
                request_fingerprint: "direct-original".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: Some(SourceAuthorizationSnapshot::from(&direct)),
                authorized_document: None,
            })
            .await
            .unwrap();
        let direct = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: receipt.desired_revision.unwrap(),
            operation_id: receipt.operation_id,
        };
        assert_eq!(
            publication
                .source_operation_producer(SPACE, &direct)
                .await
                .unwrap(),
            SourceProducerOrigin::Direct
        );
        guard_source_dispatch(&ctx.storage, SPACE, &direct)
            .await
            .unwrap();
    }
}

async fn claimed_coordinator_run(
    ctx: &ReprocessExecutionContext,
    job: &ReprocessJob,
) -> (elephant_types::records::WorkItem, WorkRun, AttemptFence) {
    let id = ctx.job_repo.ensure_work(job).await.unwrap();
    let item = elephant_storage::WorkItemRepository::new(ctx.storage.clone())
        .claim_with_lease(
            SPACE,
            &id,
            "admission-oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&item).unwrap();
    let mut metrics = elephant_types::records::WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).unwrap(),
    );
    let mut run_base = item.base.clone();
    run_base.id = RecordId::new_v4(TableName::WorkRun);
    let run = ctx
        .work_run_repo
        .create(WorkRun {
            base: run_base,
            work_item: Link::new(item.base.id.clone()),
            attempt: item.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: fence.lease_token.clone(),
            worker_version: "oracle".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    (item, run, fence)
}

#[tokio::test]
async fn paced_admission_recovers_original_selection_under_a_new_real_attempt() {
    for embedded in [true, false] {
        let (ctx, _) = setup(embedded).await;
        let config = ReprocessConfig {
            batch_size: 1,
            ..Default::default()
        };
        let original = job(&ctx, &config).await;
        source(&ctx).await;
        source(&ctx).await;
        let (first_work, first_run, first_attempt) = claimed_coordinator_run(&ctx, &original).await;
        let first = execute_reprocess_job_with_attempt(
            &ctx,
            &original.base.id,
            &first_run.base.id,
            SPACE,
            &config,
            Some(first_attempt.clone()),
        )
        .await
        .unwrap();
        assert_eq!(first.pending_operations.len(), 1);
        let index = WorkArtifactRepository::new(ctx.storage.clone())
            .get(SPACE, &index_id(&original.base.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.work_run.id(), &first_run.base.id);
        elephant_storage::WorkItemRepository::new(ctx.storage.clone())
            .update_running_for_worker(
                SPACE,
                &first_work.base.id,
                &first_attempt.lease_token,
                elephant_types::records::WorkItemStatus::Runnable,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let current = ctx
            .job_repo
            .get(SPACE, &original.base.id)
            .await
            .unwrap()
            .unwrap();
        let (_, second_run, second_attempt) = claimed_coordinator_run(&ctx, &current).await;
        assert_ne!(first_attempt, second_attempt);
        let second = execute_reprocess_job_with_attempt(
            &ctx,
            &original.base.id,
            &second_run.base.id,
            SPACE,
            &config,
            Some(second_attempt.clone()),
        )
        .await
        .unwrap();
        assert_eq!(second.pending_operations.len(), 2);
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let second_operation = second
            .pending_operations
            .iter()
            .find(|id| !first.pending_operations.contains(id))
            .unwrap();
        let receipt = publication
            .operation(SPACE, second_operation)
            .await
            .unwrap()
            .unwrap();
        let fence = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: receipt.desired_revision.unwrap(),
            operation_id: receipt.operation_id,
        };
        assert!(
            matches!(publication.source_operation_producer(SPACE, &fence).await.unwrap(),
            elephant_types::publication::SourceProducerOrigin::Reprocess { original_attempt: Some(attempt), .. }
                if attempt == second_attempt)
        );
        guard_source_dispatch(&ctx.storage, SPACE, &fence)
            .await
            .unwrap();
        assert!(
            execute_reprocess_job_with_attempt(
                &ctx,
                &original.base.id,
                &first_run.base.id,
                SPACE,
                &config,
                Some(second_attempt),
            )
            .await
            .is_err(),
            "a current lease cannot authorize an unrelated older run"
        );
    }
}

#[tokio::test]
async fn verified_plain_contact_metadata_keeps_plain_admission_and_retry() {
    // This tests the real intake/contact boundary. Full published-source
    // reprocessing is exercised by the finalization owner's retained-path oracle.
    for embedded in [true, false] {
        let (ctx, _) = setup(embedded).await;
        let source = source(&ctx).await;
        let mut metadata = source.clone();
        metadata.metadata["contact"] = serde_json::json!({"name":"Ada", "company":"Research"});
        let source = ctx
            .source_repo
            .update(SPACE, &source.base.id, metadata)
            .await
            .unwrap();
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let accepted = publication
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "original-plain".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "original-plain".into(),
                request_fingerprint: "original-plain".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: Some(SourceAuthorizationSnapshot::from(&source)),
                authorized_document: None,
            })
            .await
            .unwrap();
        let original = PublicationFence {
            source_id: accepted.source_id,
            generation: accepted.source_generation,
            revision_id: accepted.desired_revision.unwrap(),
            operation_id: accepted.operation_id,
        };
        assert!(
            capture_contact_authority(&publication, SPACE, &source, Some(&original))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            capture_contact_authority(&publication, SPACE, &source, None)
                .await
                .is_err(),
            "metadata alone is still not original authority"
        );
        let mut foreign = original.clone();
        foreign.generation = "different-original".into();
        assert!(
            capture_contact_authority(&publication, SPACE, &source, Some(&foreign))
                .await
                .is_err()
        );
        let configuration = SpaceConfigRepository::new(ctx.storage.clone())
            .resolve_space(SPACE)
            .await
            .unwrap()
            .token;
        let job_id = RecordId::new_v4(TableName::ReprocessJob);
        let expected = SourcePrecondition::Generation(original.generation.clone());
        let intent = SourceIntent {
            job_id: job_id.clone(),
            source: source.clone(),
            authorized: publication
                .authorization_snapshot(SPACE, &source.base.id)
                .await
                .unwrap()
                .unwrap(),
            request_fingerprint: recipe_fingerprint(
                &job_id,
                &source,
                &expected,
                &configuration,
                &[ReprocessTargetStage::ReExtract],
                None,
            )
            .unwrap(),
            expected,
            revision_id: "plain-successor".into(),
            idempotency_key: "plain-successor".into(),
            configuration,
            contact_authority: None,
        };
        let request = acceptance_request(&intent);
        assert!(
            prepare_contact_admission(&ctx.storage, &publication, SPACE, &intent, &request)
                .await
                .unwrap()
                .is_none()
        );
        publication.accept(request.clone()).await.unwrap();
        // Acceptance advanced the old head, but retry must recover the new
        // operation's original plain class before any fresh selector reads.
        assert!(
            prepare_contact_admission(&ctx.storage, &publication, SPACE, &intent, &request)
                .await
                .unwrap()
                .is_none()
        );
        let mut invented = intent.clone();
        invented.contact_authority = Some(ContactAuthority {
            publication: original,
            seal_fingerprint: None,
            admission_digest: Some("invented".into()),
        });
        assert!(
            prepare_contact_admission(&ctx.storage, &publication, SPACE, &invented, &request)
                .await
                .is_err()
        );
        assert!(elephant_storage::EntityRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(elephant_storage::RelRepository::new(ctx.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}

#[test]
fn unavailable_contact_plan_states_never_authorize_resumed_work() {
    for state in [
        OperationState::Accepted,
        OperationState::Processing,
        OperationState::FailedRetryable,
        OperationState::Erasing,
    ] {
        assert!(
            matches!(require_receipt_only_contact_state(state), Err(PipelineError::StorageError(elephant_storage::StorageError::Conflict(code))) if code == "accepted_contact_plan_unavailable")
        );
    }
    for state in [
        OperationState::Completed,
        OperationState::Superseded,
        OperationState::FailedTerminal,
        OperationState::Retracted,
        OperationState::Erased,
    ] {
        assert!(require_receipt_only_contact_state(state).is_ok());
    }
}

#[test]
fn original_full_seal_selection_encoding_is_preserved_without_minting_a_digest() {
    let original = serde_json::json!({
        "publication": {
            "source_id": RecordId::new_v4(TableName::Source),
            "generation": "original-generation",
            "revision_id": "original-revision",
            "operation_id": "original-operation"
        },
        "seal_fingerprint": "original-full-seal-fingerprint"
    });
    let authority: ContactAuthority = serde_json::from_value(original.clone()).unwrap();
    assert!(authority.admission_digest.is_none());
    assert_eq!(serde_json::to_value(authority).unwrap(), original);
}

async fn nested_source_intent_roundtrip(embedded: bool) {
    let (ctx, run) = Box::pin(setup(embedded)).await;
    let job = Box::pin(job(&ctx, &ReprocessConfig::default())).await;
    let mut input = Box::pin(source(&ctx)).await;
    // Deliberately preserve non-alphabetical insertion order at every level.
    // Native artifact persistence sorts JSON objects; arrays remain ordered.
    let mut contact = serde_json::Map::new();
    contact.insert("name".into(), serde_json::json!("Ada"));
    contact.insert("company".into(), serde_json::json!("Laboratory"));
    let mut nested = serde_json::Map::new();
    nested.insert("z_last".into(), serde_json::json!("last"));
    nested.insert("a_first".into(), serde_json::json!("first"));
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "ordered_steps".into(),
        serde_json::json!([nested, "second"]),
    );
    metadata.insert("contact".into(), serde_json::Value::Object(contact));
    metadata.insert("_manual_doc".into(), input.metadata["_manual_doc"].clone());
    input.metadata = serde_json::Value::Object(metadata);
    let configuration = SpaceConfigRepository::new(ctx.storage.clone())
        .resolve_space(SPACE)
        .await
        .unwrap()
        .token;
    let expected = SourcePrecondition::Absent;
    let fingerprint = recipe_fingerprint(
        &job.base.id,
        &input,
        &expected,
        &configuration,
        &job.target_stages,
        None,
    )
    .unwrap();
    let intent = SourceIntent {
        job_id: job.base.id.clone(),
        authorized: SourceAuthorizationSnapshot::from(&input),
        source: input.clone(),
        expected,
        revision_id: format!("reprocess-{fingerprint}"),
        idempotency_key: "nested-intent-roundtrip".into(),
        request_fingerprint: fingerprint.clone(),
        configuration,
        contact_authority: None,
    };
    let mut base = input.base.clone();
    base.provenance.sources = vec![input.base.id.clone()];
    let original = artifact(
        base,
        source_artifact_id(&job.base.id, &input.base.id),
        &run,
        SOURCE_KIND,
        encode(&intent).unwrap(),
    );
    let artifacts = WorkArtifactRepository::new(ctx.storage.clone());
    artifacts.create(original.clone()).await.unwrap();
    let stored = artifacts
        .get(SPACE, &original.base.id)
        .await
        .unwrap()
        .unwrap();
    let saved: SourceIntent = decode(stored.data.clone()).unwrap();
    assert_eq!(saved.request_fingerprint, fingerprint);
    assert_eq!(
        elephant_storage::canonical_commit::source_support_fingerprint(&stored).unwrap(),
        elephant_storage::canonical_commit::source_support_fingerprint(&original).unwrap()
    );
    let recompute = |source: &Source| {
        recipe_fingerprint(
            &saved.job_id,
            source,
            &saved.expected,
            &saved.configuration,
            &job.target_stages,
            saved.contact_authority.as_ref(),
        )
        .unwrap()
    };
    assert_eq!(recompute(&saved.source), fingerprint);
    let mut reordered = saved.source.clone();
    reordered.metadata["ordered_steps"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert_ne!(
        recompute(&reordered),
        fingerprint,
        "array order remains original intent"
    );
    let mut changed = saved.source.clone();
    changed.metadata["contact"]["company"] = serde_json::json!("Changed laboratory");
    assert_ne!(
        recompute(&changed),
        fingerprint,
        "nested scalar remains original intent"
    );
    let mut changed = saved.source.clone();
    changed.base.security.level += 1;
    assert_ne!(
        recompute(&changed),
        fingerprint,
        "source envelope remains original intent"
    );
    let mut changed = saved.source.clone();
    changed.base.updated_at += chrono::Duration::seconds(1);
    assert_ne!(
        recompute(&changed),
        fingerprint,
        "recipe preserves its original timestamp too"
    );
    // This serialization oracle never admits the metadata as a contact/source
    // operation and never rewrites a persisted fingerprint to make it pass.
    assert!(SourcePublicationRepository::new(ctx.storage)
        .operation_by_idempotency_key(SPACE, &saved.idempotency_key)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn embedded_nested_source_intent_preserves_exact_recipe_after_storage_roundtrip() {
    Box::pin(nested_source_intent_roundtrip(true)).await;
}

#[tokio::test]
async fn native_nested_source_intent_preserves_exact_recipe_after_storage_roundtrip() {
    Box::pin(nested_source_intent_roundtrip(false)).await;
}

#[path = "source_completion_tests.rs"]
mod completion;
