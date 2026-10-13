//! Exercise the exposed validate -> generated commit dispatcher with real
//! original claims and the maintenance owner's single canonical batch.

use super::*;

#[derive(Clone, Copy)]
pub(super) enum Scenario {
    Commit,
    ChangedDecisionInput,
    ChangedSecondBundle,
}

fn item(space: &str, kind: &str, payload: serde_json::Value) -> WorkItem {
    let id = RecordId::new_v4(TableName::WorkItem);
    WorkItem {
        base: BaseFields::new(id.clone(), space.into(), "resolution-oracle".into()),
        kind: kind.into(),
        payload,
        status: WorkItemStatus::Runnable,
        priority: 5,
        idempotency_key: format!("resolution-oracle:{id}"),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn contract(embedded: bool, scenario: Scenario) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded")
    } else {
        Box::pin(StorageClient::new_memory()).await.expect("native")
    };
    Box::pin(initialize_fixture_schema(&storage)).await;
    let space = "resolution-bundle-dispatch";
    let actor = "resolution-oracle";
    let (revision, node) = Box::pin(create_revision_with_node(
        &storage,
        space,
        actor,
        "Alice works at Acme Corp.",
    ))
    .await;
    let mut assertions = Vec::new();
    for _ in 0..2 {
        assertions.push(
            Box::pin(create_pending_assertion(
                &storage,
                space,
                actor,
                (&revision, &node),
                "Alice",
                "works_at",
                Some("Acme Corp"),
            ))
            .await,
        );
    }
    let resolved = Box::pin(create_entity(&storage, space, actor, "Alice", "person")).await;
    let items = WorkItemRepository::new(storage.clone());
    let producer = items
        .create(item(
            space,
            "resolution_decision_fixture",
            serde_json::json!({}),
        ))
        .await
        .expect("actual producer item");
    let producer = items
        .claim_with_lease(
            space,
            &producer.base.id,
            "resolution-oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("producer claim")
        .expect("original producer");
    let producer_fence = elephant_storage::work_attempt_repository::fence_from_claim(&producer)
        .expect("original producer fence");
    let run_id = Box::pin(create_work_run(&storage, space, actor, &producer.base.id)).await;
    let runs = WorkRunRepository::new(storage.clone());
    let mut run = runs
        .get(space, &run_id)
        .await
        .expect("producer run")
        .expect("present");
    run.attempt = producer.attempts;
    run.metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&producer_fence).expect("original producer fence"),
    );
    runs.update(space, &run_id, run.clone())
        .await
        .expect("record original producer authority");
    let epoch = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
        .epoch(space)
        .await
        .expect("original decision epoch");
    let workflow = ResolutionPatchWorkflow::new(storage.clone());
    let mut source_artifacts = Vec::new();
    for assertion in &assertions {
        source_artifacts.push(
            Box::pin(workflow.stage_resolution_patch(
                &run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    prepared_epoch: Some(epoch.clone()),
                    assertion_id: assertion.base.id.clone(),
                    resolved_subject: Some(resolved.base.id.clone()),
                    resolved_object: None,
                    confidence: 0.91,
                    resolution_method: "graph_context".into(),
                    status: ResolutionStatus::Resolved,
                    abstention_reason: None,
                },
                space,
            ))
            .await
            .expect("stage original decision"),
        );
    }
    run.completed_at = Some(Utc::now());
    run.success = true;
    runs.update(space, &run_id, run)
        .await
        .expect("complete original decision run");
    WorkQueue::new(storage.clone(), WorkQueueConfig::default())
        .for_attempt(&producer)
        .expect("original producer queue token")
        .finish_authorized(&producer, true, None)
        .await
        .expect("acknowledge original producer");

    let validator = item(
        space,
        VALIDATE_ARTIFACTS_KIND,
        serde_json::json!({"work_run_id":run_id.to_string(),"ingest_mode":"graph"}),
    );
    Box::pin(dispatch_fixture_with_extractor(
        &validator,
        &RecordId::new_v4(TableName::WorkRun),
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
    .expect("actual validation and successor dispatch");
    let artifacts = WorkArtifactRepository::new(storage.clone());
    let bundles: Vec<_> = artifacts
        .find_by_work_run(&run_id)
        .await
        .expect("actual staged artifacts")
        .into_iter()
        .filter(|row| row.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
        .collect();
    assert_eq!(bundles.len(), 2);
    for bundle in &bundles {
        assert_eq!(bundle.status, ArtifactStatus::Validated);
        let payload = commit_bundle_payload(bundle).expect("actual generated bundle");
        let original = source_artifacts
            .iter()
            .find(|row| row.base.id.to_string() == payload.source_artifact_id)
            .expect("exact original patch");
        assert_eq!(bundle.base.security, original.base.security);
        assert_eq!(bundle.base.subjects, original.base.subjects);
        assert_eq!(bundle.base.subjects_state, original.base.subjects_state);
        assert_eq!(
            serde_json::to_value(&bundle.base.provenance).expect("bundle provenance"),
            serde_json::to_value(&original.base.provenance).expect("patch provenance")
        );
    }
    assert_eq!(
        elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
            .epoch(space)
            .await
            .expect("epoch before commit"),
        epoch,
        "queue causal bookkeeping cannot replace or invalidate an unchanged decision"
    );
    let commits: Vec<_> = items
        .find_by_status(space, WorkItemStatus::Runnable)
        .await
        .expect("actual successor queue")
        .into_iter()
        .filter(|row| row.kind == COMMIT_ARTIFACTS_KIND)
        .collect();
    assert_eq!(commits.len(), 1, "commit is the real generated child");
    match scenario {
        Scenario::ChangedDecisionInput => {
            let mut changed = resolved.clone();
            changed.display_name = "Alice changed after decision".into();
            EntityRepository::new(storage.clone())
                .update(space, &changed.base.id.clone(), changed)
                .await
                .expect("actual canonical decision input mutation");
        }
        Scenario::ChangedSecondBundle => {
            let mut changed = bundles[1].clone();
            let mut payload = commit_bundle_payload(&changed).expect("bundle payload");
            let CommitBundlePayload::ResolutionPatchSet(patch) = &mut payload.payload else {
                panic!("actual resolution bundle");
            };
            patch.resolved_subject = Some(RecordId::new_v4(TableName::Entity));
            changed.data = serde_json::to_value(payload).expect("modified bundle");
            artifacts
                .update(space, &changed.base.id.clone(), changed)
                .await
                .expect("mutate only second bundle");
        }
        Scenario::Commit => {}
    }
    let events = SystemEventRepository::new(storage.clone());
    let before_events = events
        .get_after_seq(space, 0, 100)
        .await
        .expect("outbox before commit");
    let committed = Box::pin(dispatch_fixture_with_extractor(
        &commits[0],
        &RecordId::new_v4(TableName::WorkRun),
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await;
    if matches!(scenario, Scenario::Commit) {
        committed.expect("atomic resolution bundle dispatch");
    } else {
        assert!(
            committed.is_err(),
            "stale or mismatched batch must be refused"
        );
    }
    if !matches!(scenario, Scenario::Commit) {
        assert_eq!(
            serde_json::to_value(
                events
                    .get_after_seq(space, 0, 100)
                    .await
                    .expect("outbox after refusal")
            )
            .expect("current outbox"),
            serde_json::to_value(before_events).expect("original outbox"),
            "a refused resolution batch cannot advance its canonical outbox"
        );
    }
    for original in &assertions {
        let current = AssertionRepository::new(storage.clone())
            .get(space, &original.base.id)
            .await
            .expect("assertion")
            .expect("present");
        if matches!(scenario, Scenario::Commit) {
            assert_eq!(current.resolution_status, ResolutionStatus::Resolved);
            assert_eq!(
                current.subject_entity.as_ref().map(Link::id),
                Some(&resolved.base.id)
            );
            assert_eq!(current.resolution_method.as_deref(), Some("graph_context"));
            assert_eq!(current.resolution_confidence, Some(0.91));
        } else {
            assert_eq!(
                serde_json::to_value(current).expect("current assertion"),
                serde_json::to_value(original).expect("original assertion"),
                "no earlier patch in a refused batch may commit"
            );
        }
    }
    for original in source_artifacts.iter().chain(&bundles) {
        let current = artifacts
            .get(space, &original.base.id)
            .await
            .expect("artifact status")
            .expect("retained");
        assert_eq!(
            current.status,
            if matches!(scenario, Scenario::Commit) {
                ArtifactStatus::Committed
            } else {
                ArtifactStatus::Validated
            }
        );
        assert_eq!(
            current.committed_at.is_some(),
            matches!(scenario, Scenario::Commit)
        );
    }
}

#[tokio::test]
async fn embedded_dispatch_commits_the_complete_resolution_bundle_batch() {
    Box::pin(contract(true, Scenario::Commit)).await;
}

#[tokio::test]
async fn both_backends_reject_changed_decision_epoch_without_partial_commit() {
    for embedded in [true, false] {
        Box::pin(contract(embedded, Scenario::ChangedDecisionInput)).await;
    }
}

#[tokio::test]
async fn both_backends_reject_second_bundle_mismatch_without_partial_commit() {
    for embedded in [true, false] {
        Box::pin(contract(embedded, Scenario::ChangedSecondBundle)).await;
    }
}

#[test]
fn resolution_dispatch_fixture_future_stays_bounded() {
    fn bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let size = bytes(|| contract(true, Scenario::Commit));
    assert!(
        size <= 64 * 1024,
        "resolution fixture future is {size} bytes"
    );
}
