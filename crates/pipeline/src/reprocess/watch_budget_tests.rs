//! Actual source admissions from private watch ancestry; no network/provider work.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_watch_producer::PreparedSourceWatchProducer;
use elephant_storage::work_producer_repository::WorkProducerRepository;
use elephant_storage::{
    SourceWatchRepository, SpaceConfigRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::publication::{
    PublicationFence, SourceAuthorizationSnapshot, SourcePrecondition,
};
use elephant_types::records::{
    SourceWatch, SourceWatchStatus, SpaceConfig, WorkItemStatus, WorkRun, WorkRunMetrics,
};

const SPACE: &str = "watch-private-budget";
fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), SPACE.into(), "oracle".into())
}

async fn register_original_watch(
    storage: &StorageClient,
    source: &Source,
    watch: &SourceWatch,
) -> elephant_storage::standing_watch_repository::StandingWatchIdentity {
    use elephant_auth::standing_watch::StandingWatchAdmission;
    use elephant_storage::source_watch_registration::PreparedSourceWatchRegistration;
    use elephant_storage::standing_watch_repository::StandingWatchRepository;

    let principal = elephant_policy::Principal {
        space_id: SPACE.into(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![elephant_policy::Clearance {
            policy_id: source.base.security.policy_id.clone(),
            max_level: source.base.security.level,
            labels: source.base.security.labels.iter().cloned().collect(),
            handling_allow: source.base.security.handling.iter().cloned().collect(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    let admission = StandingWatchAdmission::trusted_local(&principal)
        .expect("explicit bounded local fixture admission");
    let records = Box::pin(PreparedSourceWatchRegistration::prepare(
        storage,
        source,
        Some(watch),
        watch.clone(),
    ))
    .await
    .unwrap();
    let registration = Box::pin(
        StandingWatchRepository::new(storage.clone()).prepare_registration(watch, &admission),
    )
    .await
    .unwrap();
    let (_, identity) = Box::pin(records.register(storage, &registration))
        .await
        .unwrap();
    identity
}

async fn claim_run(
    storage: &StorageClient,
    id: &RecordId,
) -> (WorkItem, WorkRun, elephant_types::publication::AttemptFence) {
    let item = WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            SPACE,
            id,
            "watch-oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&item).unwrap();
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).unwrap(),
    );
    let run = WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base: base(TableName::WorkRun),
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
async fn accepted_watch_source_keeps_private_budget_after_original_dispatch_completes() {
    for embedded in [true, false] {
        for finite in [false, true] {
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
                    processing: None,
                    purpose: None,
                    skill: None,
                })
                .await
                .unwrap();
            let sources = SourceRepository::new(storage.clone());
            let mut input =
                Source::new(base(TableName::Source), SourceType::Web, "original".into());
            input.uri = Some("https://example.invalid/watch".into());
            // A raw Source row has no standing publication authority. Admit
            // the original input first, then authorize the actual live row.
            let publications = SourcePublicationRepository::new(storage.clone());
            let initial = Box::pin(publications.accept(AcceptSourceRevision {
                source: input,
                revision_id: "original-watch-input".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "original-watch-intent".into(),
                request_fingerprint: "original-watch-input".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            }))
            .await
            .unwrap();
            let source = sources
                .get(SPACE, &initial.source_id)
                .await
                .unwrap()
                .unwrap();
            let watch = SourceWatchRepository::new(storage.clone())
                .create(SourceWatch {
                    base: base(TableName::SourceWatch),
                    source: Link::new(source.base.id.clone()),
                    requested_uri: "https://example.invalid/watch".into(),
                    status: SourceWatchStatus::Enabled,
                    interval_seconds: 300,
                    next_check_at: Utc::now(),
                    last_checked_at: None,
                    last_changed_at: None,
                    etag: None,
                    last_modified: None,
                    observed_hash: None,
                    consecutive_failures: 0,
                    last_error: None,
                    latest_check: None,
                })
                .await
                .unwrap();
            let standing = Box::pin(register_original_watch(&storage, &source, &watch)).await;
            let jobs = ReprocessJobRepository::new(storage.clone());
            let job = jobs
                .create_with_work(ReprocessJob {
                    base: base(TableName::ReprocessJob),
                    name: "watch ancestor".into(),
                    selection_criteria: SelectionCriteria::default(),
                    target_stages: vec![ReprocessTargetStage::ReMaintain],
                    status: ReprocessStatus::Pending,
                    progress: ReprocessProgress::default(),
                    priority: 1,
                    budget_limit_euros: finite.then_some(1.0),
                    max_sources_per_day: None,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    reason: None,
                })
                .await
                .unwrap();
            let parent_id = jobs.ensure_work(&job).await.unwrap();
            let (parent, parent_run, _) = claim_run(&storage, &parent_id).await;
            let mut child_base = parent.base.clone();
            child_base.id = RecordId::new_v4(TableName::WorkItem);
            let mut payload = serde_json::json!({
                "watch_id": watch.base.id.to_string(),
                "watch_intent_fingerprint": elephant_storage::source_watch_producer::watch_intent_fingerprint(&watch, "oracle").unwrap(),
            });
            payload.as_object_mut().unwrap().insert(
                elephant_storage::standing_watch_repository::PAYLOAD_KEY.into(),
                serde_json::to_value(standing).unwrap(),
            );
            let child = WorkProducerRepository::new(storage.clone())
                .enqueue_successor(
                    &parent,
                    &parent_run,
                    WorkItem {
                        base: child_base,
                        kind: "refresh_source_watch".into(),
                        payload,
                        status: WorkItemStatus::Runnable,
                        priority: 1,
                        idempotency_key: uuid::Uuid::new_v4().to_string(),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    },
                    false,
                )
                .await
                .unwrap();
            let (work, mut run, attempt) = claim_run(&storage, &child.base.id).await;
            let producer = PreparedSourceWatchProducer::capture(
                &storage,
                &watch,
                "oracle",
                &attempt,
                &run.base.id,
            )
            .await
            .unwrap();
            let publication = SourcePublicationRepository::new(storage.clone());
            let mut observed = source.clone();
            observed.hash = "new-observation".into();
            // This exercises the storage admission boundary, not a network
            // fetch. The physical guard below must still enforce the ancestor.
            let receipt = publication
                .accept_with_watch_producer(
                    AcceptSourceRevision {
                        source: observed,
                        revision_id: "watched-revision".into(),
                        expected: SourcePrecondition::Generation(initial.source_generation),
                        idempotency_key: "watched-intent".into(),
                        request_fingerprint: "watched-input".into(),
                        reactivate: false,
                        recreate_after_erasure: false,
                        authorized: Some(SourceAuthorizationSnapshot::from(&source)),
                        authorized_document: None,
                    },
                    &producer,
                    None,
                    None,
                )
                .await
                .unwrap();
            let original = PublicationFence {
                source_id: receipt.source_id,
                generation: receipt.source_generation,
                revision_id: receipt.desired_revision.unwrap(),
                operation_id: receipt.operation_id,
            };
            WorkItemRepository::new(storage.clone())
                .update_running_for_worker(
                    SPACE,
                    &work.base.id,
                    &attempt.lease_token,
                    WorkItemStatus::Done,
                    None,
                )
                .await
                .unwrap()
                .expect("complete the original captured lease");
            run.completed_at = Some(Utc::now());
            run.success = true;
            run = WorkRunRepository::new(storage.clone())
                .update(SPACE, &run.base.id, run.clone())
                .await
                .unwrap();
            let mut changed_watch = watch.clone();
            changed_watch.etag = Some("later-observation".into());
            SourceWatchRepository::new(storage.clone())
                .update(SPACE, &watch.base.id, changed_watch)
                .await
                .unwrap();
            let mut current_job = job.clone();
            current_job.budget_limit_euros = None;
            jobs.update(SPACE, &job.base.id, current_job).await.unwrap();
            let guard = source_job::guard_source_dispatch(&storage, SPACE, &original).await;
            if finite {
                assert!(
                    matches!(guard, Err(PipelineError::BudgetExceeded(_))),
                    "{guard:?}"
                );
            } else {
                guard.unwrap();
                let mut restricted = jobs.get(SPACE, &job.base.id).await.unwrap().unwrap();
                restricted.budget_limit_euros = Some(2.0);
                jobs.update(SPACE, &job.base.id, restricted).await.unwrap();
                assert!(matches!(
                    source_job::guard_source_dispatch(&storage, SPACE, &original).await,
                    Err(PipelineError::BudgetExceeded(_))
                ));
            }
            // Accounting must use the actual original run evidence, never the
            // mutable watch's latest observation or a newly adopted lease.
            run.metrics.custom.remove("attempt_fence");
            WorkRunRepository::new(storage.clone())
                .update(SPACE, &run.base.id, run.clone())
                .await
                .unwrap();
            assert!(
                source_job::guard_source_dispatch(&storage, SPACE, &original)
                    .await
                    .is_err()
            );
        }
    }
}
