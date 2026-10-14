//! Durable accepted Watch work survives cancellation without losing its budget origin.
use super::*;
use elephant_pipeline::source_queue::enqueue_accepted_source_from_attempt;
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_watch_producer::{
    watch_intent_fingerprint, PreparedSourceWatchProducer,
};
use elephant_storage::work_producer_repository::{WorkProducerOrigin, WorkProducerRepository};
use elephant_types::publication::{OperationState, SourceProducerOrigin};
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::{SourceWatch, SourceWatchStatus, SubjectsState};

const SPACE: &str = "quiet-watch-accepted-gap";
const ACTOR: &str = "watch-gap-oracle";

async fn claim_run(storage: &StorageClient, work: &WorkItem) -> (WorkItem, WorkRun) {
    let claimed = WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            SPACE,
            &work.base.id,
            ACTOR,
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("actual claim")
        .expect("owned");
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)
        .expect("original fence");
    let mut base = claimed.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).expect("fence"),
    );
    let run = WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(claimed.base.id.clone()),
            attempt: claimed.attempts,
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
        .expect("original durable run");
    (claimed, run)
}

async fn cancel(storage: &StorageClient, work: &WorkItem, mut run: WorkRun) {
    WorkItemRepository::new(storage.clone())
        .update_running_for_worker(
            SPACE,
            &work.base.id,
            work.leased_by.as_deref().expect("original token"),
            WorkItemStatus::Cancelled,
            Some("authorized original cancellation".into()),
        )
        .await
        .expect("cancel")
        .expect("original ownership");
    run.completed_at = Some(Utc::now());
    run.success = false;
    run.error = Some("authorized original cancellation".into());
    WorkRunRepository::new(storage.clone())
        .update(SPACE, &run.base.id, run.clone())
        .await
        .expect("actual cancelled outcome");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn accepted_gap(embedded: bool) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded")
    } else {
        Box::pin(StorageClient::new_memory()).await.expect("native")
    };
    Box::pin(initialize_fixture_schema(&storage)).await;
    Box::pin(ensure_fixture_space(&storage, SPACE, ACTOR)).await;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Source),
        SPACE.into(),
        ACTOR.into(),
    );
    base.subjects_state = SubjectsState::Complete;
    let mut source = Source::new(base.clone(), SourceType::Web, "original-input".into());
    source.uri = Some("https://example.invalid/accepted-gap".into());
    source.external_id = Some("accepted-gap".into());
    // This real supported mode has no model prerequisite. The same original
    // finite producer must still forbid any later unpriced physical dispatch.
    source.set_requested_ingest_mode(IngestMode::Metadata);
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Gate hours".into(),
            text: "The gate closes at six.".into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "watch-gap".into(),
            metadata: serde_json::json!({"content_type":"text/plain"}),
        })
        .expect("immutable original input");
    let first = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        source,
        IntakeControl::default(),
    ))
    .await
    .expect("prepare original")
    .accept(&storage)
    .await
    .expect("accept original");

    let jobs = ReprocessJobRepository::new(storage.clone());
    base.id = RecordId::new_v4(TableName::ReprocessJob);
    let mut job = jobs
        .create_with_work(ReprocessJob {
            base,
            name: "original finite coordinator".into(),
            selection_criteria: SelectionCriteria::default(),
            target_stages: vec![ReprocessTargetStage::ReMaintain],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 5,
            budget_limit_euros: Some(1.0),
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: None,
        })
        .await
        .expect("actual original finite job");
    let items = WorkItemRepository::new(storage.clone());
    let parent_id = jobs.ensure_work(&job).await.expect("durable job handoff");
    let parent = items
        .get(SPACE, &parent_id)
        .await
        .expect("parent")
        .expect("present");
    let (parent, parent_run) = Box::pin(claim_run(&storage, &parent)).await;
    let mut base = first.source.base.clone();
    base.id = RecordId::new_v4(TableName::SourceWatch);
    let watches = SourceWatchRepository::new(storage.clone());
    let watch = watches
        .create(SourceWatch {
            base,
            source: Link::new(first.source.base.id.clone()),
            requested_uri: "https://example.invalid/accepted-gap".into(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some("original-input".into()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        })
        .await
        .expect("actual watch");
    let standing = Box::pin(super::source_watch_test_authority::register_local(
        &storage, &watch,
    ))
    .await;
    let mut base = watch.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    let origins = WorkProducerRepository::new(storage.clone());
    let work = origins.enqueue_successor(&parent, &parent_run, WorkItem {
        base, kind: REFRESH_SOURCE_WATCH_KIND.into(),
        payload: serde_json::json!({"watch_id":watch.base.id,
            "standing_watch_delegation":standing,
            "watch_intent_fingerprint":watch_intent_fingerprint(&watch, ACTOR).expect("original intent")}),
        status: WorkItemStatus::Runnable, priority: 5, idempotency_key: "original-job-watch".into(),
        attempts: 0, lease_expires_at: None, leased_by: None, run_after: None, last_error: None,
    }, false).await.expect("real private watch child");
    let (work, run) = Box::pin(claim_run(&storage, &work)).await;
    let attempt =
        elephant_storage::work_attempt_repository::fence_from_claim(&work).expect("watch fence");
    let producer = Box::pin(PreparedSourceWatchProducer::capture(
        &storage,
        &watch,
        ACTOR,
        &attempt,
        &run.base.id,
    ))
    .await
    .expect("original watch producer");
    let mut changed = first.source.clone();
    changed.hash = "changed-input".into();
    let mut template = changed
        .manual_doc_template()
        .expect("template")
        .expect("present");
    template.text = "The gate now closes at seven.".into();
    changed
        .set_manual_doc_template(&template)
        .expect("changed immutable input");
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        changed,
        IntakeControl {
            revision_id: Some("watch-gap-r2".into()),
            idempotency_key: Some("watch-gap-original-acceptance".into()),
            expected_generation: Some(first.publication.generation.clone()),
        },
    ))
    .await
    .expect("original watch acceptance intent");
    let accepted = Box::pin(prepared.accept_with_watch_producer(&storage, &producer, None))
        .await
        .expect("acceptance is durable before queue handoff");
    let publications = SourcePublicationRepository::new(storage.clone());
    let original_origin = publications
        .source_operation_producer(SPACE, &accepted.publication)
        .await
        .expect("original source producer");
    assert!(matches!(&original_origin, SourceProducerOrigin::Watch {
        work_item_id, work_run_id, original_attempt, ..
    } if work_item_id == &work.base.id && work_run_id == &run.base.id && original_attempt == &attempt));
    Box::pin(cancel(&storage, &work, run)).await;
    Box::pin(cancel(&storage, &parent, parent_run)).await;
    job.status = ReprocessStatus::Cancelled;
    job.budget_limit_euros = None;
    jobs.update(SPACE, &job.base.id, job.clone())
        .await
        .expect("current job policy changes independently");
    let before_queue = items
        .list(SPACE, 100, None)
        .await
        .expect("queue before stale handoff");
    let error = Box::pin(enqueue_accepted_source_from_attempt(
        &storage,
        &accepted.source,
        &accepted.publication,
        ACTOR,
        &producer,
    ))
    .await
    .expect_err("cancelled watch cannot enqueue");
    assert_eq!(
        classify_failure(&anyhow::Error::new(error)),
        FailureClass::LostLease
    );
    assert_eq!(
        items.list(SPACE, 100, None).await.expect("unchanged queue"),
        before_queue
    );
    assert!(items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .expect("quiet space")
        .is_empty());
    assert_eq!(
        publications
            .operation(SPACE, &accepted.operation.operation_id)
            .await
            .expect("receipt"),
        Some(accepted.operation.clone())
    );
    assert_eq!(
        watches.get(SPACE, &watch.base.id).await.expect("watch"),
        Some(watch.clone())
    );
    assert!(watches
        .list_checks(SPACE, &watch.base.id, 100)
        .await
        .expect("checks")
        .is_empty());

    Box::pin(source_runtime::recover_sources(&storage))
        .await
        .expect("actual all-space accepted-gap recovery");
    let recovered = items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .expect("recovered queue");
    assert!(!recovered.is_empty());
    assert!(matches!(
        origins
            .origin_for_job(&job)
            .await
            .expect("original job proof"),
        WorkProducerOrigin::ReprocessJob {
            original_currency_restricted: true,
            ..
        }
    ));
    for item in &recovered {
        assert_eq!(
            elephant_pipeline::source_queue::publication_from_work(item).expect("original fence"),
            Some(accepted.publication.clone())
        );
        let refusal = Box::pin(elephant_pipeline::reprocess::guard_physical_dispatch(
            &storage, item,
        ))
        .await
        .expect_err("recovery cannot erase the original finite budget");
        assert!(matches!(refusal, PipelineError::BudgetExceeded(ref message)
            if message.contains("Original finite currency restriction")));
    }
    // Run supported Metadata publication through the actual dispatcher. No
    // physical provider is needed and no validation/completion state is stamped.
    for _ in 0..30 {
        let ready = items
            .find_by_status(SPACE, WorkItemStatus::Runnable)
            .await
            .expect("ready");
        if ready.is_empty() {
            break;
        }
        for item in ready {
            Box::pin(dispatch_fixture_with_extractor(
                &item,
                &RecordId::new_v4(TableName::WorkRun),
                storage.clone(),
                &WorkQueueWorkerConfig::default(),
                &TmContext::none(),
                None,
            ))
            .await
            .expect("actual recovered source dispatch");
        }
    }
    for status in [
        WorkItemStatus::Runnable,
        WorkItemStatus::Pending,
        WorkItemStatus::Running,
        WorkItemStatus::Failed,
    ] {
        assert!(items
            .find_by_status(SPACE, status)
            .await
            .expect("settled queue")
            .is_empty());
    }
    let completed = publications
        .operation(SPACE, &accepted.operation.operation_id)
        .await
        .expect("original receipt")
        .expect("retained");
    assert_eq!(completed.state, OperationState::Completed);
    assert_eq!(completed.source_generation, accepted.publication.generation);
    assert_eq!(
        publications
            .source_operation_producer(SPACE, &accepted.publication)
            .await
            .expect("retained original source producer"),
        original_origin
    );
    let completion =
        elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(
            storage.clone(),
        )
        .completion(SPACE, &accepted.publication)
        .await
        .expect("completion proof")
        .expect("actual publication");
    assert_eq!(completion.publication, accepted.publication);
    let receipt = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &completion.commit_id)
        .await
        .expect("canonical receipt")
        .expect("actual canonical commit");
    assert_eq!(receipt.source_id, Some(accepted.source.base.id));
    assert_eq!(
        watches
            .get(SPACE, &watch.base.id)
            .await
            .expect("watch unchanged by recovery"),
        Some(watch)
    );
}

#[tokio::test]
async fn embedded_cancelled_watch_acceptance_recovers_with_original_finite_origin() {
    Box::pin(accepted_gap(true)).await;
}

#[tokio::test]
async fn native_cancelled_watch_acceptance_recovers_with_original_finite_origin() {
    Box::pin(accepted_gap(false)).await;
}
