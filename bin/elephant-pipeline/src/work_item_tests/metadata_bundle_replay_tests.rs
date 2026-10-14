//! Reclaim the actual metadata producer after its successful run was saved but
//! its queue acknowledgement was lost. Both validators remain original work.

use super::source_finalizer_contention_tests::{accept_sources, ACTOR, SPACE};
use super::*;
use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
use elephant_types::publication::{OperationState, PublicationFence, SourcePrecondition};
use elephant_types::records::work::WorkArtifact;

struct Fixture {
    storage: StorageClient,
    publication: PublicationFence,
    producer: WorkItem,
    runs: [WorkRun; 2],
    validators: [WorkItem; 2],
    candidates: [WorkArtifact; 2],
}

fn workflow_key(producer: &WorkItem, kind: &str, run: &RecordId) -> String {
    let publication = elephant_pipeline::source_queue::publication_from_work(producer)
        .unwrap()
        .unwrap();
    let plan = producer.payload[elephant_pipeline::source_queue::SOURCE_PLAN_KEY]
        .as_str()
        .unwrap();
    format!(
        "{kind}:{run}:publication:{}:{}:plan:{plan}",
        publication.generation, publication.revision_id
    )
}

async fn claim(storage: &StorageClient, item: &WorkItem) -> WorkItem {
    let items = WorkItemRepository::new(storage.clone());
    let mut due = items.get(SPACE, &item.base.id).await.unwrap().unwrap();
    // Advance only the fixture's scheduling time, never an execution counter.
    due.run_after = None;
    items.update(SPACE, &item.base.id, due).await.unwrap();
    items
        .claim_with_lease(
            SPACE,
            &item.base.id,
            ACTOR,
            Utc::now() + Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn expire_original(storage: &StorageClient, original: &WorkItem) {
    let expiry = Utc::now() + Duration::seconds(1);
    let renewed = WorkItemRepository::new(storage.clone())
        .renew_lease(
            SPACE,
            &original.base.id,
            original.leased_by.as_deref().unwrap(),
            expiry,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.leased_by, original.leased_by);
    // Exercise real expiry without changing Running work through a generic
    // repository update. Both queue and attempt carry this same future deadline.
    let remaining = (expiry - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(remaining + std::time::Duration::from_millis(10)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn dispatch(storage: &StorageClient, item: &WorkItem) -> anyhow::Result<WorkRunMetrics> {
    let items = WorkItemRepository::new(storage.clone());
    let mut due = items.get(SPACE, &item.base.id).await?.unwrap();
    due.run_after = None;
    items.update(SPACE, &item.base.id, due).await?;
    Box::pin(dispatch_fixture_with_extractor(
        item,
        &RecordId::new_v4(TableName::WorkRun),
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
}

/// Exact interruption boundary: producer result and successful original run
/// exist, while the queue still owns the live claim. No forged second result.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn save_success_without_ack(storage: &StorageClient, item: &WorkItem) -> WorkRun {
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(item).unwrap();
    let id = create_work_run(storage, SPACE, ACTOR, &item.base.id).await;
    let runs = WorkRunRepository::new(storage.clone());
    let mut run = runs.get(SPACE, &id).await.unwrap().unwrap();
    run.attempt = item.attempts;
    run.metrics
        .custom
        .insert("attempt_fence".into(), serde_json::json!(fence));
    runs.update(SPACE, &id, run.clone()).await.unwrap();
    let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
        storage.clone(),
        &id,
        fence.clone(),
    );
    let mut metrics = storage
        .with_activity(
            elephant_pipeline::provider_dispatch::with_dispatch_observer(
                observer.clone(),
                Box::pin(super::super::execute_work_item_with_extractor(
                    item,
                    &id,
                    storage.clone(),
                    &WorkQueueWorkerConfig::default(),
                    &TmContext::none(),
                    None,
                )),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    observer
        .summarize(AttemptUsage::reserved(0, 0.0))
        .await
        .unwrap()
        .write_metrics(&mut metrics)
        .unwrap();
    metrics
        .custom
        .insert("attempt_fence".into(), serde_json::json!(fence));
    run.metrics = metrics;
    run.success = true;
    run.completed_at = Some(Utc::now());
    runs.update(SPACE, &id, run).await.unwrap()
}

async fn fixture(embedded: bool) -> Fixture {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        Box::pin(initialize_fixture_schema(&storage)).await;
        storage
    };
    let publication = Box::pin(accept_sources(&storage, 1, false)).await.remove(0);
    let items = WorkItemRepository::new(storage.clone());
    let mut metadata = None;
    for _ in 0..6 {
        let ready = items
            .find_by_status(SPACE, WorkItemStatus::Runnable)
            .await
            .unwrap();
        if let Some(item) = ready.iter().find(|item| item.kind == INGEST_METADATA_KIND) {
            metadata = Some(item.clone());
            break;
        }
        assert!(!ready.is_empty());
        for item in ready {
            Box::pin(dispatch(&storage, &item)).await.unwrap();
        }
    }
    let first = claim(&storage, &metadata.unwrap()).await;
    let original = Box::pin(save_success_without_ack(&storage, &first)).await;
    let old_validator = items
        .find_by_idempotency_key(
            SPACE,
            &workflow_key(&first, VALIDATE_ARTIFACTS_KIND, &original.base.id),
        )
        .await
        .unwrap()
        .unwrap();
    let blocked = Box::pin(dispatch(&storage, &old_validator))
        .await
        .unwrap_err();
    assert_eq!(
        classify_failure(&blocked),
        FailureClass::PendingDependencies
    );
    assert_eq!(
        items
            .get(SPACE, &old_validator.base.id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        0
    );
    assert_eq!(
        items
            .get(SPACE, &first.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkItemStatus::Running
    );

    // Expire the real queue lease and invoke the retained recovery path. The
    // original successful WorkRun and its already-enqueued child remain intact.
    expire_original(&storage, &first).await;
    assert_eq!(items.recover_expired_leases_all_spaces().await.unwrap(), 1);
    Box::pin(dispatch(&storage, &first)).await.unwrap();
    let producer = items.get(SPACE, &first.base.id).await.unwrap().unwrap();
    assert_eq!(producer.status, WorkItemStatus::Done);
    assert_eq!(producer.attempts, 2);
    let runs = WorkRunRepository::new(storage.clone());
    let all = runs.find_by_work_item(&producer.base.id).await.unwrap();
    assert_eq!(all.len(), 2);
    let retry = all
        .into_iter()
        .find(|run| run.base.id != original.base.id)
        .unwrap();
    assert!(retry.success && retry.completed_at.is_some());
    assert_eq!(
        runs.get(SPACE, &original.base.id).await.unwrap().unwrap(),
        original
    );
    let retry_validator = items
        .find_by_idempotency_key(
            SPACE,
            &workflow_key(&producer, VALIDATE_ARTIFACTS_KIND, &retry.base.id),
        )
        .await
        .unwrap()
        .unwrap();
    let artifacts = WorkArtifactRepository::new(storage.clone());
    let mut candidates = Vec::new();
    for run in [&original, &retry] {
        let rows = artifacts.find_by_work_run(&run.base.id).await.unwrap();
        assert_eq!(rows.len(), 1);
        candidates.push(rows[0].clone());
    }
    assert_ne!(candidates[0].base.id, candidates[1].base.id);
    assert_eq!(candidates[0].data, candidates[1].data);
    Fixture {
        storage,
        publication,
        producer,
        runs: [original, retry],
        validators: [old_validator, retry_validator],
        candidates: candidates.try_into().unwrap(),
    }
}

async fn selected(fixture: &Fixture) -> WorkArtifact {
    let config = elephant_pipeline::source_queue::configuration_from_work(&fixture.producer)
        .unwrap()
        .unwrap();
    WorkArtifactRepository::new(fixture.storage.clone())
        .get(
            SPACE,
            &source_runtime::metadata_bundle_id(&fixture.publication, &config),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn commit_item(fixture: &Fixture, index: usize) -> WorkItem {
    WorkItemRepository::new(fixture.storage.clone())
        .find_by_idempotency_key(
            SPACE,
            &workflow_key(
                &fixture.producer,
                COMMIT_ARTIFACTS_KIND,
                &fixture.runs[index].base.id,
            ),
        )
        .await
        .unwrap()
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn both_orderings(embedded: bool, first: usize) {
    let fixture = Box::pin(fixture(embedded)).await;
    let other = 1 - first;
    Box::pin(dispatch(&fixture.storage, &fixture.validators[first]))
        .await
        .unwrap();
    let chosen = selected(&fixture).await;
    assert_eq!(chosen.work_run.id(), &fixture.runs[first].base.id);
    assert_eq!(
        chosen.data["source_artifact_id"],
        fixture.candidates[first].base.id.to_string()
    );
    let commit = commit_item(&fixture, first).await;
    Box::pin(dispatch(&fixture.storage, &commit)).await.unwrap();
    let existing_commit = WorkItemRepository::new(fixture.storage.clone())
        .get(SPACE, &commit.base.id)
        .await
        .unwrap()
        .unwrap();
    // The delayed original validator runs after the other attempt's real
    // validation/registration, exactly where source141 reported a false conflict.
    Box::pin(dispatch(&fixture.storage, &fixture.validators[other]))
        .await
        .unwrap();
    assert_eq!(selected(&fixture).await, chosen);
    let items = WorkItemRepository::new(fixture.storage.clone());
    assert_eq!(
        items.get(SPACE, &commit.base.id).await.unwrap().unwrap(),
        existing_commit
    );
    assert!(
        items
            .find_by_idempotency_key(
                SPACE,
                &workflow_key(
                    &fixture.producer,
                    COMMIT_ARTIFACTS_KIND,
                    &fixture.runs[other].base.id
                ),
            )
            .await
            .unwrap()
            .is_none(),
        "no empty wrong-run commit handoff"
    );
    assert_eq!(
        items
            .get(SPACE, &commit.base.id)
            .await
            .unwrap()
            .unwrap()
            .payload["work_run_id"],
        fixture.runs[first].base.id.to_string()
    );
    let ready = items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(
        ready[0].kind,
        elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
    );
    Box::pin(dispatch(&fixture.storage, &ready[0]))
        .await
        .unwrap();
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let receipt = sources
        .operation(SPACE, &fixture.publication.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, OperationState::Completed);
    assert_eq!(receipt.published_revision.as_deref(), Some("r1"));
    let final_bundle = selected(&fixture).await;
    assert_eq!(final_bundle.data, chosen.data);
    assert_eq!(final_bundle.work_run, chosen.work_run);
    assert_eq!(final_bundle.status, ArtifactStatus::Committed);
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone());
    assert_eq!(
        artifacts
            .get(SPACE, &fixture.candidates[first].base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ArtifactStatus::Committed
    );
    assert_eq!(
        artifacts
            .get(SPACE, &fixture.candidates[other].base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ArtifactStatus::Validated
    );
    let mut bundles = 0;
    for run in &fixture.runs {
        bundles += artifacts
            .find_by_work_run(&run.base.id)
            .await
            .unwrap()
            .iter()
            .filter(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
            .count();
        assert_eq!(
            WorkRunRepository::new(fixture.storage.clone())
                .get(SPACE, &run.base.id)
                .await
                .unwrap()
                .unwrap(),
            *run
        );
    }
    assert_eq!(bundles, 1);
}

fn proposal(fixture: &Fixture, candidate: &WorkArtifact, selected: &WorkArtifact) -> WorkArtifact {
    let mut proposed = selected.clone();
    proposed.work_run = candidate.work_run.clone();
    proposed.data["source_artifact_id"] = serde_json::json!(candidate.base.id.to_string());
    let payload: SourceMetadataArtifactData =
        serde_json::from_value(candidate.data.clone()).unwrap();
    proposed.data["payload"]["data"] = serde_json::to_value(payload).unwrap();
    proposed.base.security = candidate.base.security.clone();
    proposed.base.subjects = candidate.base.subjects.clone();
    proposed.base.subjects_state = candidate.base.subjects_state;
    proposed.base.provenance = candidate.base.provenance.clone();
    proposed.base.created_by = fixture.validators[1].base.created_by.clone();
    proposed
}

async fn replay(
    fixture: &Fixture,
    candidate: &WorkArtifact,
    proposed: &WorkArtifact,
    selected: &WorkArtifact,
) -> anyhow::Result<Option<RecordId>> {
    Box::pin(super::super::metadata_bundle_replay::original_run(
        &fixture.storage,
        &fixture.validators[1],
        candidate,
        proposed,
        selected,
    ))
    .await
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn prepared_pair(embedded: bool) -> (Fixture, WorkArtifact, WorkArtifact, WorkArtifact) {
    let fixture = Box::pin(fixture(embedded)).await;
    Box::pin(dispatch(&fixture.storage, &fixture.validators[0]))
        .await
        .unwrap();
    let selected = selected(&fixture).await;
    let staged = StagedCommit::new(fixture.storage.clone());
    let mut validated = Box::pin(
        elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute_source(
            &staged,
            SPACE,
            &fixture.runs[1].base.id,
        ),
    )
    .await
    .unwrap();
    assert_eq!(validated.len(), 1);
    let candidate = validated.remove(0);
    let proposed = proposal(&fixture, &candidate, &selected);
    assert_eq!(
        Box::pin(replay(&fixture, &candidate, &proposed, &selected))
            .await
            .unwrap(),
        Some(fixture.runs[0].base.id.clone())
    );
    (fixture, candidate, proposed, selected)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn create_race_and_states(embedded: bool) {
    let (fixture, candidate, proposed, selected) = Box::pin(prepared_pair(embedded)).await;
    // Both validators may have observed absence. Winner is now durable; invoke
    // the actual create-only continuation for the loser, forcing a real duplicate
    // CREATE on each backend rather than relying on scheduler coincidence.
    let run = Box::pin(super::super::metadata_bundle_replay::create_bundle(
        &fixture.storage,
        &fixture.validators[1],
        &candidate,
        proposed.clone(),
        true,
    ))
    .await
    .unwrap();
    assert_eq!(run, fixture.runs[0].base.id);
    assert_eq!(self::selected(&fixture).await, selected);
    let repo = WorkArtifactRepository::new(fixture.storage.clone());
    let mut committing = selected.clone();
    committing.status = ArtifactStatus::Committing;
    repo.update(SPACE, &selected.base.id, committing.clone())
        .await
        .unwrap();
    let pending = Box::pin(replay(&fixture, &candidate, &proposed, &committing))
        .await
        .unwrap_err();
    assert_eq!(
        classify_failure(&pending),
        FailureClass::PendingDependencies
    );
    assert_eq!(self::selected(&fixture).await, committing);
    let mut committed_candidate = fixture.candidates[0].clone();
    committed_candidate.status = ArtifactStatus::Committed;
    committed_candidate.validated_at = selected.validated_at;
    committed_candidate.committed_at = Some(Utc::now());
    let committed_candidate_id = committed_candidate.base.id.clone();
    repo.update(SPACE, &committed_candidate_id, committed_candidate)
        .await
        .unwrap();
    let mut committed = selected.clone();
    committed.status = ArtifactStatus::Committed;
    committed.committed_at = Some(Utc::now());
    repo.update(SPACE, &selected.base.id, committed.clone())
        .await
        .unwrap();
    assert_eq!(
        Box::pin(replay(&fixture, &candidate, &proposed, &committed))
            .await
            .unwrap(),
        Some(fixture.runs[0].base.id.clone())
    );
    assert_eq!(self::selected(&fixture).await, committed);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn missing_commit_handoff(embedded: bool) {
    let fixture = Box::pin(fixture(embedded)).await;
    let staged = StagedCommit::new(fixture.storage.clone());
    let mut validated = Box::pin(
        elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute_source(
            &staged,
            SPACE,
            &fixture.runs[0].base.id,
        ),
    )
    .await
    .unwrap();
    let candidate = validated.remove(0);
    let configuration = elephant_pipeline::source_queue::configuration_from_work(&fixture.producer)
        .unwrap()
        .unwrap();
    let mut bundle = candidate.clone();
    bundle.base.id = source_runtime::metadata_bundle_id(&fixture.publication, &configuration);
    bundle.artifact_type = COMMIT_BUNDLE_ARTIFACT_TYPE.into();
    bundle.data = serde_json::to_value(CommitBundleArtifactData {
        source_artifact_id: candidate.base.id.to_string(),
        source_artifact_type: SOURCE_METADATA_ARTIFACT_TYPE.into(),
        bundle_kind: CommitBundleKind::SourceMetadata,
        payload: CommitBundlePayload::SourceMetadata(
            serde_json::from_value(candidate.data.clone()).unwrap(),
        ),
    })
    .unwrap();
    bundle.data["publication"] = serde_json::json!(fixture.publication);
    for key in ["_source_plan", "_source_configuration"] {
        bundle.data[key] = fixture.producer.payload[key].clone();
    }
    // Cut immediately after the real create-only bundle persistence. No
    // validator queue completion or commit child has happened for this result.
    Box::pin(super::super::metadata_bundle_replay::create_bundle(
        &fixture.storage,
        &fixture.validators[0],
        &candidate,
        bundle.clone(),
        true,
    ))
    .await
    .unwrap();
    let items = WorkItemRepository::new(fixture.storage.clone());
    assert!(items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .unwrap()
        .iter()
        .all(|item| item.kind != COMMIT_ARTIFACTS_KIND));
    Box::pin(dispatch(&fixture.storage, &fixture.validators[1]))
        .await
        .unwrap();
    assert_eq!(selected(&fixture).await, bundle);
    let commit = commit_item(&fixture, 0).await;
    assert_eq!(
        commit.payload["work_run_id"],
        fixture.runs[0].base.id.to_string()
    );
    assert_eq!(
        commit.payload[source_runtime::PREDECESSOR],
        fixture.validators[1].base.id.to_string()
    );
    assert!(items
        .find_by_idempotency_key(
            SPACE,
            &workflow_key(
                &fixture.producer,
                COMMIT_ARTIFACTS_KIND,
                &fixture.runs[1].base.id
            ),
        )
        .await
        .unwrap()
        .is_none());
    // The first validator now finds a commit child genuinely created by the
    // replaying validator. Keep that child's original predecessor unchanged.
    Box::pin(dispatch(&fixture.storage, &fixture.validators[0]))
        .await
        .unwrap();
    assert_eq!(commit_item(&fixture, 0).await, commit);
    Box::pin(dispatch(&fixture.storage, &commit)).await.unwrap();
    let finalizer = items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .unwrap()
        .into_iter()
        .find(|item| {
            item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
        })
        .unwrap();
    Box::pin(dispatch(&fixture.storage, &finalizer))
        .await
        .unwrap();
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let complete = sources
        .operation(SPACE, &fixture.publication.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(complete.state, OperationState::Completed);
    assert_eq!(complete.published_revision.as_deref(), Some("r1"));
    assert_eq!(
        sources
            .operation(SPACE, &fixture.publication.operation_id)
            .await
            .unwrap()
            .unwrap(),
        complete
    );
    assert_eq!(selected(&fixture).await.data, bundle.data);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn negative_proofs(embedded: bool) {
    let (fixture, candidate, proposed, selected) = Box::pin(prepared_pair(embedded)).await;
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone());
    let child = commit_item(&fixture, 0).await;
    let mut expected_child = child.clone();
    expected_child
        .payload
        .as_object_mut()
        .unwrap()
        .remove(source_runtime::PREDECESSOR);
    for case in ["source", "run", "predecessor", "policy", "plan"] {
        let mut changed = child.clone();
        match case {
            "source" => {
                changed.payload["source_id"] =
                    serde_json::json!(RecordId::new_v4(TableName::Source).to_string());
            }
            "run" => {
                changed.payload["work_run_id"] =
                    serde_json::json!(fixture.runs[1].base.id.to_string());
            }
            "predecessor" => {
                changed.payload[source_runtime::PREDECESSOR] =
                    serde_json::json!(fixture.producer.base.id.to_string());
            }
            "policy" => changed.base.security.policy_id.push_str("-foreign"),
            "plan" => changed.payload["_source_plan"] = serde_json::json!("foreign"),
            _ => unreachable!(),
        }
        assert!(
            !Box::pin(super::super::metadata_bundle_replay::original_commit_child(
                &fixture.storage,
                &expected_child,
                &changed,
            ))
            .await
            .unwrap(),
            "changed child {case}"
        );
    }
    for case in [
        "title",
        "security",
        "policy",
        "labels",
        "handling",
        "subjects",
        "subjects_state",
        "provenance",
        "generation",
        "configuration",
        "plan",
        "nonmetadata",
        "candidate_id",
        "missing_stamp",
        "errors",
    ] {
        let mut bad = candidate.clone();
        match case {
            "title" => bad.data["normalized_title"] = serde_json::json!("Changed semantics"),
            "security" => bad.base.security.level += 1,
            "policy" => bad.base.security.policy_id.push_str("-foreign"),
            "labels" => bad.base.security.labels.push("changed".into()),
            "handling" => bad.base.security.handling.push("changed".into()),
            "subjects" => bad
                .base
                .subjects
                .push(Link::new(RecordId::new_v4(TableName::Entity))),
            "subjects_state" => {
                bad.base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
            }
            "provenance" => bad.base.provenance.run_id = Some("foreign".into()),
            "generation" => bad.data["_publication"]["generation"] = serde_json::json!("foreign"),
            "configuration" => {
                bad.data["_source_configuration"]["generation"] = serde_json::json!(99);
            }
            "plan" => bad.data["_source_plan"] = serde_json::json!("foreign"),
            "nonmetadata" => bad.artifact_type = "knowledge_candidates".into(),
            "candidate_id" => bad.base.id = RecordId::new_v4(TableName::WorkArtifact),
            "missing_stamp" => bad.validated_at = None,
            "errors" => bad.validation_errors = Some(vec!["rejected".into()]),
            _ => unreachable!(),
        }
        let bad_proposed = proposal(&fixture, &bad, &selected);
        assert!(
            Box::pin(replay(&fixture, &bad, &bad_proposed, &selected))
                .await
                .unwrap()
                .is_none(),
            "{case}"
        );
        assert_eq!(self::selected(&fixture).await, selected, "{case}");
    }
    // Equal corruption of both mutable candidates and the selected bundle is
    // not proof: deterministic accepted source input remains the comparison.
    let original = artifacts
        .get(SPACE, &fixture.candidates[0].base.id)
        .await
        .unwrap()
        .unwrap();
    let mut bad_original = original.clone();
    bad_original.data["normalized_title"] = serde_json::json!("Equal forged title");
    artifacts
        .update(SPACE, &original.base.id, bad_original.clone())
        .await
        .unwrap();
    let mut bad = candidate.clone();
    bad.data = bad_original.data;
    let mut bad_selected = selected.clone();
    bad_selected.data["payload"]["data"]["normalized_title"] =
        serde_json::json!("Equal forged title");
    let bad_proposed = proposal(&fixture, &bad, &bad_selected);
    assert!(
        Box::pin(replay(&fixture, &bad, &bad_proposed, &bad_selected))
            .await
            .unwrap()
            .is_none()
    );
    artifacts
        .update(SPACE, &original.base.id, original.clone())
        .await
        .unwrap();
    let runs = WorkRunRepository::new(fixture.storage.clone());
    for case in [
        "foreign_producer",
        "failed",
        "unfinished",
        "missing_fence",
        "foreign_generation",
    ] {
        let mut bad_run = fixture.runs[1].clone();
        match case {
            "foreign_producer" => {
                bad_run.work_item = Link::new(RecordId::new_v4(TableName::WorkItem));
            }
            "failed" => bad_run.success = false,
            "unfinished" => bad_run.completed_at = None,
            "missing_fence" => {
                bad_run.metrics.custom.remove("attempt_fence");
            }
            "foreign_generation" => {
                bad_run.metrics.custom.get_mut("attempt_fence").unwrap()["generation"] =
                    serde_json::json!(999);
            }
            _ => unreachable!(),
        }
        runs.update(SPACE, &bad_run.base.id.clone(), bad_run)
            .await
            .unwrap();
        assert!(
            Box::pin(replay(&fixture, &candidate, &proposed, &selected))
                .await
                .unwrap()
                .is_none(),
            "{case}"
        );
        runs.update(SPACE, &fixture.runs[1].base.id, fixture.runs[1].clone())
            .await
            .unwrap();
    }
    for case in ["stamp", "errors", "run", "kind"] {
        let mut bad_original = original.clone();
        match case {
            "stamp" => bad_original.validated_at = None,
            "errors" => bad_original.validation_errors = Some(vec!["rejected".into()]),
            "run" => bad_original.work_run = candidate.work_run.clone(),
            "kind" => bad_original.artifact_type = "resolution_patch_set".into(),
            _ => unreachable!(),
        }
        artifacts
            .update(SPACE, &original.base.id, bad_original)
            .await
            .unwrap();
        assert!(
            Box::pin(replay(&fixture, &candidate, &proposed, &selected))
                .await
                .unwrap()
                .is_none(),
            "{case}"
        );
        artifacts
            .update(SPACE, &original.base.id, original.clone())
            .await
            .unwrap();
    }
    assert_eq!(self::selected(&fixture).await, selected);
    let mut substituted = selected.clone();
    substituted.data["source_artifact_id"] =
        serde_json::json!(RecordId::new_v4(TableName::WorkArtifact).to_string());
    assert!(
        Box::pin(replay(&fixture, &candidate, &proposed, &substituted))
            .await
            .unwrap()
            .is_none()
    );
    // Deletion is last: repository updates deliberately cannot resurrect a
    // deleted original candidate merely to restore a fixture between cases.
    artifacts.delete(SPACE, &original.base.id).await.unwrap();
    assert!(Box::pin(replay(&fixture, &candidate, &proposed, &selected))
        .await
        .unwrap()
        .is_none());
    let source = SourcePublicationRepository::new(fixture.storage.clone())
        .operation(SPACE, &fixture.publication.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(source.state, OperationState::Completed);
    assert!(source.published_revision.is_none());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn changed_authority_refuses_dispatch(embedded: bool, change: &str) {
    let (fixture, _, _, selected) = Box::pin(prepared_pair(embedded)).await;
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let validator = claim(&fixture.storage, &fixture.validators[1]).await;
    let items = WorkItemRepository::new(fixture.storage.clone());
    let expected = match change {
        "configuration" => {
            let spaces = SpaceConfigRepository::new(fixture.storage.clone());
            let mut configuration = spaces.get_by_space_id(SPACE).await.unwrap().unwrap();
            configuration.processing.as_mut().unwrap().generation += 1;
            spaces
                .update(SPACE, &configuration.base.id.clone(), configuration)
                .await
                .unwrap();
            FailureClass::ConfigurationBlocked
        }
        "generation" => {
            let source = sources
                .revision(SPACE, &fixture.publication.source_id, "r1")
                .await
                .unwrap()
                .unwrap()
                .source;
            let authorized = sources
                .authorization_snapshot(SPACE, &fixture.publication.source_id)
                .await
                .unwrap();
            sources
                .accept(elephant_storage::source_publication::AcceptSourceRevision {
                    source,
                    revision_id: "r2".into(),
                    expected: SourcePrecondition::Generation(
                        fixture.publication.generation.clone(),
                    ),
                    idempotency_key: "supersede-replay".into(),
                    request_fingerprint: "supersede-replay".into(),
                    reactivate: false,
                    recreate_after_erasure: false,
                    authorized,
                    authorized_document: None,
                })
                .await
                .unwrap();
            FailureClass::StaleInput
        }
        "expired" | "successor" => {
            expire_original(&fixture.storage, &validator).await;
            if change == "successor" {
                assert_eq!(items.recover_expired_leases_all_spaces().await.unwrap(), 1);
                let successor = claim(&fixture.storage, &validator).await;
                assert_ne!(successor.leased_by, validator.leased_by);
            }
            FailureClass::LostLease
        }
        _ => unreachable!(),
    };
    let error = Box::pin(super::super::execute_work_item_with_extractor(
        &validator,
        &RecordId::new_v4(TableName::WorkRun),
        fixture.storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
    .unwrap_err();
    assert_eq!(classify_failure(&error), expected, "{change}");
    assert_eq!(self::selected(&fixture).await, selected, "{change}");
    let operation = sources
        .operation(SPACE, &fixture.publication.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(operation.published_revision.is_none(), "{change}");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn erased_input_refuses_replay(embedded: bool) {
    let (fixture, _, _, _) = Box::pin(prepared_pair(embedded)).await;
    let repository = SourcePublicationRepository::new(fixture.storage.clone());
    let head = repository
        .head(SPACE, &fixture.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    let authorized = repository
        .authorization_snapshot(SPACE, &fixture.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    let receipt = Box::pin(repository.erase_live(SourceMutation {
        space_id: SPACE.into(),
        source_id: fixture.publication.source_id.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: "erase-replay-input".into(),
        request_fingerprint: "erase-replay-input".into(),
        actor: ACTOR.into(),
        authorized,
        authorized_document: None,
    }))
    .await
    .unwrap();
    assert_eq!(receipt.state, OperationState::Erased);
    let error = Box::pin(super::super::execute_work_item_with_extractor(
        &fixture.validators[1],
        &RecordId::new_v4(TableName::WorkRun),
        fixture.storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
    .unwrap_err();
    assert_eq!(classify_failure(&error), FailureClass::StaleInput);
    assert!(SourceRepository::new(fixture.storage.clone())
        .get_with_deleted(SPACE, &fixture.publication.source_id)
        .await
        .unwrap()
        .is_none());
    assert!(WorkArtifactRepository::new(fixture.storage.clone())
        .get(SPACE, &fixture.candidates[0].base.id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn metadata_replay_preserves_original_result_in_both_orders() {
    for embedded in [true, false] {
        for first in [0, 1] {
            Box::pin(both_orderings(embedded, first)).await;
        }
    }
}

#[tokio::test]
async fn metadata_replay_create_race_and_selected_states() {
    for embedded in [true, false] {
        Box::pin(create_race_and_states(embedded)).await;
    }
}

#[tokio::test]
async fn metadata_replay_recovers_missing_commit_handoff() {
    for embedded in [true, false] {
        Box::pin(missing_commit_handoff(embedded)).await;
    }
}

#[tokio::test]
async fn metadata_replay_rejects_changed_evidence_and_provenance() {
    for embedded in [true, false] {
        Box::pin(negative_proofs(embedded)).await;
    }
}

#[tokio::test]
async fn metadata_replay_cannot_restore_erased_input() {
    for embedded in [true, false] {
        Box::pin(erased_input_refuses_replay(embedded)).await;
    }
}

#[tokio::test]
async fn metadata_replay_keeps_current_configuration_generation_and_attempt_fences() {
    for embedded in [true, false] {
        for change in ["configuration", "generation", "expired", "successor"] {
            Box::pin(changed_authority_refuses_dispatch(embedded, change)).await;
        }
    }
}

#[test]
fn metadata_duplicate_recovery_does_not_swallow_other_storage_failures() {
    use super::super::metadata_bundle_replay::duplicate_bundle;
    use elephant_storage::StorageError;
    let id = RecordId::new_v4(TableName::WorkArtifact);
    let native = format!(
        "Database record `{}` already exists",
        elephant_storage::surreal::thing_from_record_id(&id)
    );
    assert!(duplicate_bundle(
        &StorageError::Conflict(id.to_string()),
        &id
    ));
    assert!(duplicate_bundle(
        &StorageError::Conflict(native.clone()),
        &id
    ));
    for error in [
        StorageError::ErasureInProgress,
        StorageError::Conflict(format!("stale_attempt: {id}")),
        StorageError::Conflict("source_configuration_changed".into()),
        StorageError::Conflict(RecordId::new_v4(TableName::WorkArtifact).to_string()),
        StorageError::DataIntegrityError(native.clone()),
        StorageError::QueryError(native),
        StorageError::TransactionError("read or write conflict".into()),
    ] {
        assert!(!duplicate_bundle(&error, &id), "{error}");
    }
}
