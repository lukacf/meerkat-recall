//! The real accepted-source dispatcher retains its original triage decision.
use super::*;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_types::publication::{AttemptFence, PublicationFence, SourcePrecondition};

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn prepared(embedded: bool) -> (StorageClient, Source, WorkItem) {
    let storage = if embedded {
        StorageClient::new_embedded().await.expect("embedded")
    } else {
        StorageClient::new_memory().await.expect("native")
    };
    if !embedded {
        Box::pin(initialize_fixture_schema(&storage)).await;
    }
    let space = "original-triage-decision";
    let actor = "original-actor";
    ensure_fixture_space(&storage, space, actor).await;
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            actor.into(),
        ),
        SourceType::Manual,
        "original-triage-content".into(),
    );
    source.set_requested_ingest_mode(IngestMode::Metadata);
    let acceptance = SourcePublicationRepository::new(storage.clone())
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "original-triage-revision".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-triage-acceptance".into(),
            request_fingerprint: "original-triage-request".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .expect("actual accepted source revision");
    let publication = PublicationFence {
        source_id: acceptance.source_id,
        generation: acceptance.source_generation,
        revision_id: acceptance.desired_revision.expect("accepted revision"),
        operation_id: acceptance.operation_id,
    };
    let spaces = SpaceConfigRepository::new(storage.clone());
    let configuration = spaces
        .resolve_space(space)
        .await
        .expect("original configuration")
        .token;
    let work_id = elephant_pipeline::source_queue::enqueue_source_plan(
        &storage,
        &source,
        &publication,
        &configuration,
        actor,
    )
    .await
    .expect("actual configured triage work");
    let work = WorkItemRepository::new(storage.clone())
        .get(space, &work_id)
        .await
        .expect("original work read")
        .expect("actual triage work");
    (storage, source, work)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn original_decision(embedded: bool) {
    let (storage, source, work) = Box::pin(prepared(embedded)).await;
    let space = source.base.space_id.as_str();
    let actor = source.base.created_by.as_str();
    let work_id = work.base.id.clone();
    assert_eq!(work.kind, TRIAGE_SOURCE_KIND);
    let run_id = create_work_run(&storage, space, actor, &work_id).await;
    let returned = Box::pin(dispatch_fixture_with_extractor(
        &work,
        &run_id,
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
    .expect("actual accepted-source triage and original completion");
    let decision_id: RecordId = returned.custom["source_ingest_decision_id"]
        .as_str()
        .expect("original decision encoded as a RecordId string")
        .parse()
        .expect("typed original decision identity");
    assert_eq!(decision_id.table(), TableName::IngestDecision);
    let decision = IngestDecisionRepository::new(storage.clone())
        .get(space, &decision_id)
        .await
        .expect("read exact original decision")
        .expect("decision actually persisted by triage");
    assert_eq!(decision.source.id(), &source.base.id);
    assert_eq!(decision.decision, IngestMode::Metadata);
    let run = WorkRunRepository::new(storage.clone())
        .get(space, &run_id)
        .await
        .expect("original run read")
        .expect("original execution receipt");
    assert!(run.success && run.completed_at.is_some());
    assert_eq!(run.work_item.id(), &work_id);
    assert_eq!(
        run.metrics.custom["source_ingest_decision_id"],
        returned.custom["source_ingest_decision_id"]
    );
    let fence: AttemptFence = serde_json::from_value(run.metrics.custom["attempt_fence"].clone())
        .expect("same original attempt proof remains present");
    assert_eq!(fence.work_item_id, work_id);
    assert_eq!(fence.space_id, space);
    assert!(!fence.lease_token.is_empty());
    let items = WorkItemRepository::new(storage.clone());
    let children = items
        .find_by_status_and_kind(space, WorkItemStatus::Runnable, INGEST_METADATA_KIND, 10)
        .await
        .expect("actual metadata handoff");
    assert_eq!(children.len(), 1);
    let child = &children[0];
    let parent = items
        .get(space, &work_id)
        .await
        .expect("original parent")
        .expect("retained parent");
    let producer =
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone());
    let origin = producer
        .origin_for_work(child)
        .await
        .expect("sealed original child");
    assert!(
        matches!(&origin,
            elephant_storage::work_producer_repository::WorkProducerOrigin::Child {
                parent_work_item_id, original_parent_work_run_id, original_parent_attempt,
                wait_for_parent_completion, ..
            } if parent_work_item_id == &work_id && original_parent_work_run_id == &run_id
                && original_parent_attempt == &fence && *wait_for_parent_completion
        ),
        "actual triage handoff must retain original execution: {origin:?}"
    );
    elephant_storage::work_producer_repository::verify_parent_origin(&origin, &parent, &run)
        .expect("historical original triage proof");
    for key in ["_publication", "_source_configuration", "_source_plan"] {
        assert_eq!(child.payload.get(key), work.payload.get(key));
    }
    assert!(child.payload.get("_attempt_fence").is_none());
    assert!(child.payload.get(source_runtime::PREDECESSOR).is_none());
    let metadata_run = create_work_run(&storage, space, actor, &child.base.id).await;
    Box::pin(dispatch_fixture_with_extractor(
        child,
        &metadata_run,
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
    .expect("actual metadata executes under its original source context");
    let artifacts = WorkArtifactRepository::new(storage.clone())
        .find_by_work_run(&metadata_run)
        .await
        .expect("actual metadata artifacts");
    let metadata = artifacts
        .iter()
        .filter(|artifact| artifact.artifact_type == SOURCE_METADATA_ARTIFACT_TYPE)
        .collect::<Vec<_>>();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].work_run.id(), &metadata_run);
    for key in ["_publication", "_source_configuration", "_source_plan"] {
        assert_eq!(metadata[0].data.get(key), work.payload.get(key));
    }
    let current = load_source_record(&storage, space, &source.base.id)
        .await
        .expect("accepted source remains unpublished");
    assert!(current.latest_ingest_decision.is_none());
    Box::pin(original_handoff_guards(embedded)).await;
}

#[expect(
    clippy::semicolon_if_nothing_returned,
    clippy::significant_drop_tightening,
    reason = "Keep original handoff tampering branches and repository scopes through all assertions"
)]
async fn original_handoff_guards(embedded: bool) {
    let (storage, source, proposed) = Box::pin(prepared(embedded)).await;
    let space = source.base.space_id.as_str();
    let items = WorkItemRepository::new(storage.clone());
    let parent = items
        .claim_with_lease(
            space,
            &proposed.base.id,
            "original-handoff-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("actual claim")
        .expect("owned original attempt");
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&parent)
        .expect("original claim fence");
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).expect("original fence"),
    );
    let mut base = parent.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let run = WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(parent.base.id.clone()),
            attempt: parent.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: fence.lease_token.clone(),
            worker_version: "original-handoff".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .expect("actual original execution receipt");
    let mut child = proposed.clone();
    child.base.id = RecordId::new_v4(TableName::WorkItem);
    child.kind = INGEST_METADATA_KIND.into();
    child.idempotency_key = format!("{INGEST_METADATA_KIND}:{}", source.base.id);
    child.payload =
        serde_json::json!({"source_id": source.base.id.to_string(), "target_mode":"metadata"});
    let admitted = Box::pin(enqueue_work_successor(&storage, &parent, child.clone()))
        .await
        .expect("actual source-specific handoff");
    let replay = Box::pin(enqueue_work_successor(&storage, &parent, child.clone()))
        .await
        .expect("same original attempt replay");
    assert_eq!(replay, admitted);
    assert!(
        source_runtime::require_predecessor(&storage, &admitted)
            .await
            .is_err(),
        "metadata cannot run before its original triage execution succeeds"
    );
    inherit_source_context(&parent, &mut child).expect("exact original source context");
    let producer =
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone());
    for field in [
        "_publication",
        "_source_configuration",
        "_source_plan",
        "source_id",
    ] {
        let mut changed = child.clone();
        match field {
            "_publication" => {
                changed.payload[field]["operation_id"] =
                    serde_json::json!("different-original-operation")
            }
            "_source_configuration" => {
                changed.payload[field]["generation"] =
                    serde_json::json!(configuration_generation(&child) + 1)
            }
            "_source_plan" => changed.payload[field] = serde_json::json!("different-original-plan"),
            _ => {
                changed.payload[field] =
                    serde_json::json!(RecordId::new_v4(TableName::Source).to_string())
            }
        }
        assert!(
            producer
                .enqueue_source_metadata(&parent, &run, changed)
                .await
                .is_err(),
            "{field}"
        );
    }
    let mut changed_parent = parent.clone();
    changed_parent.payload["changed_original_intent"] = serde_json::json!(true);
    assert!(Box::pin(enqueue_work_successor(
        &storage,
        &changed_parent,
        child.clone()
    ))
    .await
    .is_err());
    let mut changed_run = run.clone();
    changed_run
        .metrics
        .custom
        .get_mut("attempt_fence")
        .expect("original captured fence")["lease_token"] = serde_json::json!("different-attempt");
    assert!(producer
        .enqueue_source_metadata(&parent, &changed_run, child.clone())
        .await
        .is_err());
    assert_eq!(
        items
            .get(space, &parent.base.id)
            .await
            .expect("original parent"),
        Some(parent.clone())
    );
    assert_eq!(
        WorkRunRepository::new(storage.clone())
            .get(space, &run.base.id)
            .await
            .expect("original run"),
        Some(run.clone())
    );
    // A real retry gets another attempt-owned child, never a reparented old
    // semantic work item whose original execution failed after handoff.
    let original_origin = producer
        .origin_for_work(&admitted)
        .await
        .expect("original private edge");
    let mut failed_run = run.clone();
    failed_run.completed_at = Some(Utc::now());
    failed_run.error = Some("original controlled failure after handoff".into());
    WorkRunRepository::new(storage.clone())
        .update(space, &run.base.id, failed_run)
        .await
        .expect("original failed receipt");
    items
        .requeue_with_delay(
            space,
            &parent.base.id,
            &fence.lease_token,
            0,
            Some("retry original source plan".into()),
        )
        .await
        .expect("actual original retry")
        .expect("original owner");
    let retry = items
        .claim_with_lease(
            space,
            &parent.base.id,
            "recovery-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("actual recovery claim")
        .expect("next original attempt");
    let retry_fence =
        elephant_storage::work_attempt_repository::fence_from_claim(&retry).expect("retry fence");
    assert_ne!(retry_fence, fence);
    let mut retry_run = run.clone();
    retry_run.base.id = RecordId::new_v4(TableName::WorkRun);
    retry_run.attempt = retry.attempts;
    retry_run.started_at = Utc::now();
    retry_run.worker_id = retry_fence.lease_token.clone();
    retry_run.metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&retry_fence).expect("original retry fence"),
    );
    WorkRunRepository::new(storage.clone())
        .create(retry_run.clone())
        .await
        .expect("actual recovery execution");
    let next = Box::pin(enqueue_work_successor(&storage, &retry, child.clone()))
        .await
        .expect("recover original plan with new actual attempt");
    assert_ne!(next.base.id, admitted.base.id);
    assert_eq!(
        producer
            .origin_for_work(&admitted)
            .await
            .expect("unmodified original edge"),
        original_origin
    );
    for key in ["_publication", "_source_configuration", "_source_plan"] {
        assert_eq!(next.payload.get(key), admitted.payload.get(key));
    }
    retry_run.success = true;
    retry_run.completed_at = Some(Utc::now());
    WorkRunRepository::new(storage.clone())
        .update(space, &retry_run.base.id, retry_run.clone())
        .await
        .expect("original recovery receipt completion");
    items
        .finish_authorized(&retry, &retry_fence.lease_token, WorkItemStatus::Done, None)
        .await
        .expect("actual owner completion")
        .expect("original owner");
    source_runtime::require_predecessor(&storage, &next)
        .await
        .expect("successful original retry releases its own child");
    assert!(
        source_runtime::require_predecessor(&storage, &admitted)
            .await
            .is_err(),
        "latest successful retry cannot rescue the failed original child's receipt"
    );
    child.idempotency_key.push_str(":late-handoff");
    assert!(
        Box::pin(enqueue_work_successor(&storage, &parent, child))
            .await
            .is_err(),
        "completed original claim cannot admit later children"
    );
    let remaining = items
        .find_by_status_and_kind(space, WorkItemStatus::Runnable, INGEST_METADATA_KIND, 10)
        .await
        .expect("unchanged metadata work");
    assert_eq!(remaining.len(), 2);
    assert!(remaining.contains(&admitted) && remaining.contains(&next));
}

fn configuration_generation(work: &WorkItem) -> u64 {
    work.payload["_source_configuration"]["generation"]
        .as_u64()
        .expect("actual configured generation")
}

#[tokio::test]
async fn embedded_accepted_triage_retains_original_decision_in_completed_run() {
    Box::pin(original_decision(true)).await;
}

#[tokio::test]
async fn native_accepted_triage_retains_original_decision_in_completed_run() {
    Box::pin(original_decision(false)).await;
}
