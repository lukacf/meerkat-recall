//! Original publication progress through real acceptance, producer admission and
//! canonical completion. Direct fixtures do not claim to execute a triage plugin.
use super::*;
use crate::canonical_commit::source_support_fingerprint;
use crate::read_eligibility::ReadEligibility;
use elephant_types::publication::SourceRevision;
use elephant_types::records::ingest::IngestDecision;
use elephant_types::records::source::SourceLifecycleStatus;

async fn project(storage: &StorageClient, source: &RecordId, space: &str) -> Option<Source> {
    let current = crate::SourceRepository::new(storage.clone())
        .get(space, source)
        .await
        .unwrap()
        .unwrap();
    ReadEligibility::new(storage.clone())
        .project(current, &|_| true)
        .await
        .unwrap()
}

async fn overwrite_completion(storage: &StorageClient, completion: &SourcePublicationCompletion) {
    let key = manifest_key(&completion.space_id, &completion.publication);
    overwrite_completion_at(storage, completion, &key).await;
}

async fn overwrite_completion_at(
    storage: &StorageClient,
    completion: &SourcePublicationCompletion,
    key: &str,
) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    COMPLETION_TABLE,
                    key,
                    &completion.space_id,
                    &completion.publication.source_id,
                    completion,
                )
            })
            .await
            .unwrap();
    } else {
        storage.query_bind_checked("UPSERT $id CONTENT $value;", crate::bindings! {
            "id" => control_thing(COMPLETION_TABLE, key),
            "value" => control_value(&completion.space_id, &completion.publication.source_id, completion).unwrap(),
        }).await.unwrap();
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn pending_replacement(storage: &StorageClient, accepted: &SourceRevision) {
    use crate::source_publication::AcceptSourceRevision;
    use elephant_types::publication::SourcePrecondition;
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = publications
        .head(&accepted.space_id, &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    let mut desired = accepted.source.clone();
    desired.hash = "different-pending-content".into();
    desired.metadata["title"] = serde_json::json!("pending title must not leak");
    desired.current_ingest_mode = Some(IngestMode::Graph);
    desired.lifecycle_status = SourceLifecycleStatus::GraphReady;
    desired.latest_ingest_decision = Some(Link::new(RecordId::new_v4(TableName::IngestDecision)));
    let pending = publications
        .accept(AcceptSourceRevision {
            source: desired,
            revision_id: "pending-successor".into(),
            expected: SourcePrecondition::Generation(head.generation),
            idempotency_key: "pending-progress-replacement".into(),
            request_fingerprint: "pending-progress-request".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: publications
                .authorization_snapshot(&accepted.space_id, &accepted.source_id)
                .await
                .unwrap(),
            authorized_document: None,
        })
        .await
        .unwrap();
    assert_eq!(
        pending.published_revision.as_deref(),
        Some(accepted.revision_id.as_str())
    );
    assert_ne!(pending.desired_revision, pending.published_revision);
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn certified_absence(embedded: bool) {
    let fixture = Box::pin(fixture(
        client(embedded).await,
        "published-progress-absence",
        IngestMode::Metadata,
        0,
    ))
    .await;
    let storage = &fixture.storage;
    let manifest = &fixture.manifest;
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    manifests
        .set_operation_plan(manifest.clone())
        .await
        .unwrap();
    let receipt = Box::pin(publish_actual_metadata_acceptance(storage, manifest)).await;
    let completion = manifests
        .completion(&manifest.space_id, &manifest.publication)
        .await
        .unwrap()
        .unwrap();
    let progress = completion
        .published_progress
        .as_ref()
        .expect("actual completion certifies progress");
    assert_eq!(progress.mode, IngestMode::Metadata);
    assert!(progress.latest_ingest_decision.is_none());
    assert!(progress.ingest_decision_fingerprint.is_none());
    assert_eq!(completion.commit_id, receipt.commit_id);
    let accepted = SourcePublicationRepository::new(storage.clone())
        .revision(
            &manifest.space_id,
            &manifest.publication.source_id,
            &manifest.publication.revision_id,
        )
        .await
        .unwrap()
        .unwrap();
    let mut expected = accepted.source.clone();
    expected.current_ingest_mode = Some(IngestMode::Metadata);
    expected.lifecycle_status = SourceLifecycleStatus::MetadataReady;
    expected.latest_ingest_decision = None;
    assert_eq!(
        project(storage, &accepted.source_id, &accepted.space_id).await,
        Some(expected.clone())
    );
    Box::pin(maintenance_source_edit(storage, manifest)).await;
    assert_eq!(
        project(storage, &accepted.source_id, &accepted.space_id).await,
        Some(expected.clone()),
        "a later real canonical Source effect must not move original progress authority"
    );
    pending_replacement(storage, &accepted).await;
    assert_eq!(
        project(storage, &accepted.source_id, &accepted.space_id).await,
        Some(expected.clone()),
        "pending desired progress cannot replace original published progress or content"
    );
    assert_eq!(
        manifests
            .completion(&manifest.space_id, &manifest.publication)
            .await
            .unwrap(),
        Some(completion.clone())
    );
    for mutation in 0..5 {
        let mut changed = completion.clone();
        match mutation {
            0 => changed.published_progress = None,
            1 => changed.published_progress.as_mut().unwrap().mode = IngestMode::Graph,
            2 => {
                changed
                    .published_progress
                    .as_mut()
                    .unwrap()
                    .latest_ingest_decision = Some(RecordId::new_v4(TableName::IngestDecision));
            }
            3 => changed.accepted_source_fingerprint = Some("changed-original-snapshot".into()),
            _ => changed.commit_id.push_str("-another-writer"),
        }
        overwrite_completion(storage, &changed).await;
        assert!(
            project(storage, &accepted.source_id, &accepted.space_id)
                .await
                .is_none(),
            "missing/mismatched original proof must not fabricate readiness, mutation {mutation}"
        );
        overwrite_completion(storage, &completion).await;
        assert_eq!(
            project(storage, &accepted.source_id, &accepted.space_id).await,
            Some(expected.clone())
        );
    }
    let lookup_key = progress::published_completion_key(&accepted.space_id, &accepted.source_id);
    let mut false_lookup = completion.clone();
    false_lookup.published_progress.as_mut().unwrap().mode = IngestMode::Graph;
    overwrite_completion_at(storage, &false_lookup, &lookup_key).await;
    assert!(
        project(storage, &accepted.source_id, &accepted.space_id)
            .await
            .is_none(),
        "lookup alone is not authority"
    );
    overwrite_completion_at(storage, &completion, &lookup_key).await;
    let original_key = manifest_key(&accepted.space_id, &completion.publication);
    for missing_key in [&lookup_key, &original_key] {
        remove_completion(storage, &accepted.space_id, missing_key).await;
        assert!(
            project(storage, &accepted.source_id, &accepted.space_id)
                .await
                .is_none(),
            "missing original or lookup proof refuses"
        );
        overwrite_completion_at(storage, &completion, missing_key).await;
        assert_eq!(
            project(storage, &accepted.source_id, &accepted.space_id).await,
            Some(expected.clone())
        );
    }
    // The derived lookup is source-owned physical inventory, just like the
    // immutable completion. Actual erasure must remove both records.
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = publications
        .head(&accepted.space_id, &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    let erase = crate::source_publication::SourceMutation {
        space_id: accepted.space_id.clone(),
        source_id: accepted.source_id.clone(),
        expected: elephant_types::publication::SourcePrecondition::Generation(head.generation),
        idempotency_key: "erase-progress-controls".into(),
        request_fingerprint: "erase-progress-controls".into(),
        actor: "manifest-fixture".into(),
        authorized_document: None,
        authorized: publications
            .authorization_snapshot(&accepted.space_id, &accepted.source_id)
            .await
            .unwrap()
            .unwrap(),
    };
    Box::pin(publications.erase_live(erase)).await.unwrap();
    assert!(publications
        .read_control_payload(COMPLETION_TABLE, &lookup_key, &accepted.space_id)
        .await
        .unwrap()
        .is_none());
    assert!(manifests
        .completion(&accepted.space_id, &completion.publication)
        .await
        .unwrap()
        .is_none());
    assert!(crate::SourceRepository::new(storage.clone())
        .get(&accepted.space_id, &accepted.source_id)
        .await
        .unwrap()
        .is_none());
}

async fn remove_completion(storage: &StorageClient, space: &str, key: &str) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(
                    COMPLETION_TABLE,
                    &format!("{COMPLETION_TABLE}:{key}"),
                    space,
                )?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "DELETE $id;",
                crate::bindings! { "id" => control_thing(COMPLETION_TABLE, key) },
            )
            .await
            .unwrap();
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn maintenance_source_edit(storage: &StorageClient, manifest: &SourcePublicationManifest) {
    use crate::canonical_commit::{
        CanonicalCommit, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin, CanonicalPlan,
        CanonicalRecord, CanonicalRepository,
    };
    let canonical = CanonicalRepository::new(storage.clone());
    let epoch = canonical.epoch(&manifest.space_id).await.unwrap();
    let publications = SourcePublicationRepository::new(storage.clone());
    let lookup_key =
        progress::published_completion_key(&manifest.space_id, &manifest.publication.source_id);
    let original_completion = publications
        .read_control::<SourcePublicationCompletion>(
            COMPLETION_TABLE,
            &lookup_key,
            &manifest.space_id,
        )
        .await
        .unwrap()
        .expect("original canonical completion lookup");
    let before = crate::SourceRepository::new(storage.clone())
        .get(&manifest.space_id, &manifest.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    let original = CanonicalRecord::Source(Box::new(before.clone()));
    let sources = canonical
        .capture_maintenance_support(&manifest.space_id, std::slice::from_ref(&original))
        .await
        .unwrap();
    let mut after = before;
    after.metadata["maintenance_observation"] =
        serde_json::json!("later actual Source after-image");
    after.base.updated_at += chrono::Duration::seconds(1);
    let mut plan = CanonicalPlan {
        observed_epoch: epoch,
        ..CanonicalPlan::default()
    };
    plan.push(
        Some(original),
        Some(CanonicalRecord::Source(Box::new(after.clone()))),
    )
    .unwrap();
    let receipt = canonical
        .commit(CanonicalCommit {
            space_id: manifest.space_id.clone(),
            commit_id: "later-source-maintenance".into(),
            input_fingerprint: "later-source-maintenance".into(),
            replay_output: None,
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::IndependentMutation,
                attempt: None,
                sources,
            }),
            plan,
        })
        .await
        .unwrap();
    assert!(receipt.effect_ids.contains(&manifest.publication.source_id));
    assert!(receipt.publication_authority.is_none());
    assert_eq!(
        canonical
            .receipt(&manifest.space_id, &receipt.commit_id)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        crate::SourceRepository::new(storage.clone())
            .get(&manifest.space_id, &manifest.publication.source_id)
            .await
            .unwrap(),
        Some(after),
        "the later canonical maintenance really changed the live Source"
    );
    assert_eq!(
        publications
            .read_control::<SourcePublicationCompletion>(
                COMPLETION_TABLE,
                &lookup_key,
                &manifest.space_id
            )
            .await
            .unwrap(),
        Some(original_completion),
        "later canonical Source effects do not replace original progress"
    );
    let supports = publications
        .support_for_record(&manifest.space_id, &manifest.publication.source_id)
        .await
        .unwrap();
    assert!(
        supports
            .iter()
            .all(|support| support.source_id != manifest.publication.source_id),
        "a Source input must not acquire derived support from itself"
    );
}

#[tokio::test]
async fn embedded_published_progress_retains_original_certified_absence_during_pending_revision() {
    Box::pin(certified_absence(true)).await;
    Box::pin(unsealed_predecessor_cannot_certify_absence(true)).await;
}
#[tokio::test]
async fn native_published_progress_retains_original_certified_absence_during_pending_revision() {
    Box::pin(certified_absence(false)).await;
    Box::pin(unsealed_predecessor_cannot_certify_absence(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn unsealed_predecessor_cannot_certify_absence(embedded: bool) {
    let mut fixture = Box::pin(fixture(
        client(embedded).await,
        "unsealed-source-predecessor",
        IngestMode::Metadata,
        0,
    ))
    .await;
    let storage = &fixture.storage;
    let manifest = &fixture.manifest;
    let space = &manifest.space_id;
    let triage = claim_plan_work(storage, manifest, "triage_source", plan_payload(manifest)).await;
    let mut triage_run = run_for_claim(storage, &triage).await;
    let decision = crate::IngestDecisionRepository::new(storage.clone())
        .create(IngestDecision {
            base: base(TableName::IngestDecision, space),
            source: Link::new(manifest.publication.source_id.clone()),
            decision: IngestMode::Metadata,
            reason_codes: vec!["unsealed-predecessor-control".into()],
            confidence: 1.0,
            estimated_cost: 0.0,
            cost_ledger_id: None,
            budget_snapshot: None,
            policy_version: None,
            downgrade_reason: None,
            original_mode: None,
        })
        .await
        .unwrap();
    triage_run.metrics.custom.insert(
        "source_ingest_decision_id".into(),
        serde_json::json!(decision.base.id.to_string()),
    );
    let triage_run = finish_run(storage, &triage, triage_run).await;

    // Reproduce the original direct admission with a public predecessor hint.
    // The real parent, decision and successful runs exist, but no private edge
    // records which original run/attempt produced this metadata work.
    let mut payload = plan_payload(manifest);
    payload["_predecessor_work_item"] = serde_json::json!(triage.base.id.to_string());
    let metadata = claim_plan_work(storage, manifest, "ingest_metadata", payload).await;
    let metadata_run = run_for_claim(storage, &metadata).await;
    let metadata_run = finish_run(storage, &metadata, metadata_run).await;
    let items = crate::WorkItemRepository::new(storage.clone());
    let original_metadata = items.get(space, &metadata.base.id).await.unwrap().unwrap();
    assert_eq!(
        crate::work_producer_repository::WorkProducerRepository::new(storage.clone())
            .origin_for_work(&original_metadata)
            .await
            .unwrap(),
        crate::work_producer_repository::WorkProducerOrigin::Direct
    );
    let (candidate, bundle) = original_metadata_artifacts(manifest, &metadata_run);
    let artifacts = crate::WorkArtifactRepository::new(storage.clone());
    artifacts.create(candidate).await.unwrap();
    artifacts.create(bundle.clone()).await.unwrap();
    register_completed(storage, manifest, &bundle).await;
    fixture.manifest.metadata_artifact_ids = vec![bundle.base.id];
    let manifest = &fixture.manifest;
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    manifests
        .set_operation_plan(manifest.clone())
        .await
        .unwrap();
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = publications
        .head(&manifest.space_id, &manifest.publication.source_id)
        .await
        .unwrap();
    let operation = publications
        .operation(&manifest.space_id, &manifest.publication.operation_id)
        .await
        .unwrap();
    let error = manifests
        .requirements(&manifest.space_id, &manifest.publication)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("source_original_ancestry_unproven"));
    assert_eq!(
        publications
            .head(&manifest.space_id, &manifest.publication.source_id)
            .await
            .unwrap(),
        head
    );
    assert_eq!(
        publications
            .operation(&manifest.space_id, &manifest.publication.operation_id)
            .await
            .unwrap(),
        operation
    );
    assert!(manifests
        .completion(&manifest.space_id, &manifest.publication)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        items
            .get(&manifest.space_id, &metadata.base.id)
            .await
            .unwrap(),
        Some(original_metadata)
    );
    assert_eq!(
        crate::WorkRunRepository::new(storage.clone())
            .get(&manifest.space_id, &triage_run.base.id)
            .await
            .unwrap(),
        Some(triage_run)
    );
}

async fn run_for_claim(storage: &StorageClient, claim: &WorkItem) -> WorkRun {
    let attempt = crate::work_attempt_repository::fence_from_claim(claim).unwrap();
    let mut metrics = elephant_types::records::WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    crate::WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base: base(TableName::WorkRun, &claim.base.space_id),
            work_item: Link::new(claim.base.id.clone()),
            attempt: claim.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: attempt.lease_token,
            worker_version: "original-progress-fixture".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap()
}

async fn finish_run(storage: &StorageClient, claim: &WorkItem, mut run: WorkRun) -> WorkRun {
    run.success = true;
    run.completed_at = Some(Utc::now());
    let completed = crate::WorkRunRepository::new(storage.clone())
        .update(&claim.base.space_id, &run.base.id, run.clone())
        .await
        .unwrap();
    complete_claim(storage, claim).await;
    completed
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn triage_ancestry_fixture(
    embedded: bool,
    direct_metadata: bool,
) -> (Fixture, IngestDecision, WorkRun) {
    let mut fixture = Box::pin(fixture(
        client(embedded).await,
        "original-triage-progress",
        IngestMode::Metadata,
        0,
    ))
    .await;
    let storage = &fixture.storage;
    let manifest = &fixture.manifest;
    let space = &manifest.space_id;
    let claim = claim_plan_work(storage, manifest, "triage_source", plan_payload(manifest)).await;
    let mut run = run_for_claim(storage, &claim).await;
    let decision = crate::IngestDecisionRepository::new(storage.clone())
        .create(IngestDecision {
            base: base(TableName::IngestDecision, space),
            source: Link::new(manifest.publication.source_id.clone()),
            decision: IngestMode::Metadata,
            reason_codes: vec!["original-storage-fixture".into()],
            confidence: 1.0,
            estimated_cost: 0.0,
            cost_ledger_id: None,
            budget_snapshot: None,
            policy_version: None,
            downgrade_reason: None,
            original_mode: None,
        })
        .await
        .unwrap();
    let mut proposed = work_item(space, "ingest_source", plan_payload(manifest));
    proposed.base.security = claim.base.security.clone();
    proposed.base.subjects = claim.base.subjects.clone();
    proposed.base.subjects_state = claim.base.subjects_state;
    proposed.base.provenance = claim.base.provenance.clone();
    let child = if direct_metadata {
        None
    } else {
        Some(
            crate::work_producer_repository::WorkProducerRepository::new(storage.clone())
                .enqueue_successor(&claim, &run, proposed, false)
                .await
                .unwrap(),
        )
    };
    run.metrics.custom.insert(
        "source_ingest_decision_id".into(),
        serde_json::json!(decision.base.id.to_string()),
    );
    let original_run = finish_run(storage, &claim, run).await;
    let metadata_run = if let Some(child) = child {
        let metadata_claim = crate::WorkItemRepository::new(storage.clone())
            .claim_with_lease(
                space,
                &child.base.id,
                "metadata-progress",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let metadata_run = run_for_claim(storage, &metadata_claim).await;
        finish_run(storage, &metadata_claim, metadata_run).await
    } else {
        original_run.clone()
    };
    let (candidate, bundle) = original_metadata_artifacts(manifest, &metadata_run);
    let artifacts = crate::WorkArtifactRepository::new(storage.clone());
    artifacts.create(candidate).await.unwrap();
    artifacts.create(bundle.clone()).await.unwrap();
    register_completed(storage, manifest, &bundle).await;
    fixture.manifest.metadata_artifact_ids = vec![bundle.base.id];
    SourcePublicationManifestRepository::new(storage.clone())
        .set_operation_plan(fixture.manifest.clone())
        .await
        .unwrap();
    (fixture, decision, original_run)
}

fn original_metadata_artifacts(
    manifest: &SourcePublicationManifest,
    run: &WorkRun,
) -> (WorkArtifact, WorkArtifact) {
    // A configuration-scoped original producer requires the same immutable
    // tags on both its candidate and bundle, including the copied payload.
    let plan = source_plan_id(&manifest.publication, &manifest.configuration);
    let mut candidate = artifact(
        &manifest.space_id,
        "source_metadata",
        serde_json::json!({
            "source_id": manifest.publication.source_id.to_string(),
            "_publication": manifest.publication,
            "_source_configuration": manifest.configuration,
            "_source_plan": plan,
        }),
    );
    candidate.work_run = Link::new(run.base.id.clone());
    let mut bundle = artifact(
        &manifest.space_id,
        "commit_bundle",
        serde_json::json!({
            "publication": manifest.publication,
            "_publication": manifest.publication,
            "_source_configuration": manifest.configuration,
            "_source_plan": plan,
            "source_artifact_id": candidate.base.id.to_string(),
            "source_artifact_type": "source_metadata", "bundle_kind": "source_metadata",
            "payload": {"kind": "source_metadata", "data": candidate.data},
        }),
    );
    bundle.work_run = candidate.work_run.clone();
    (candidate, bundle)
}

async fn progress_guard(
    storage: &StorageClient,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let space = &requirements.manifest.space_id;
    if let Some(store) = storage.embedded_store() {
        return store
            .mutate(|tx| {
                tx.upsert_extension_row(
                    "progress_guard_probe",
                    serde_json::json!({"id":"progress_guard_probe:sentinel", "space_id":space}),
                )?;
                progress::check_tx(tx, requirements)
            })
            .await;
    }
    let mut sql = String::from(
        "BEGIN TRANSACTION; CREATE progress_guard_probe:sentinel SET space_id=$sf_space;\n",
    );
    let mut bindings = crate::bindings! { "sf_space" => space.clone() };
    progress::append_sql(&mut sql, &mut bindings, requirements)?;
    sql.push_str("COMMIT TRANSACTION;");
    storage.query_bind_checked(&sql, bindings).await.map(|_| ())
}

async fn probe_exists(storage: &StorageClient) -> bool {
    if let Some(store) = storage.embedded_store() {
        return store
            .mutate(|tx| {
                Ok::<_, StorageError>(!tx.scan_extension_rows("progress_guard_probe")?.is_empty())
            })
            .await
            .unwrap();
    }
    let mut result = storage
        .query_checked("SELECT VALUE <string>id FROM progress_guard_probe:sentinel;")
        .await
        .unwrap();
    let ids: Vec<String> = result.take(0).unwrap();
    !ids.is_empty()
}

async fn remove_probe(storage: &StorageClient, space: &str) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(
                    "progress_guard_probe",
                    "progress_guard_probe:sentinel",
                    space,
                )?;
                Ok::<_, StorageError>(())
            })
            .await
            .unwrap();
    } else {
        storage
            .query_checked("DELETE progress_guard_probe:sentinel;")
            .await
            .unwrap();
    }
}

#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
async fn run_nonce(storage: &StorageClient, run: &WorkRun) -> Option<String> {
    if storage.is_embedded() {
        return None;
    }
    #[derive(Deserialize)]
    struct Nonce {
        canonical_read_nonce: Option<String>,
    }
    let mut response = storage
        .query_bind_checked(
            "SELECT canonical_read_nonce FROM $id;",
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(&run.base.id) },
        )
        .await
        .unwrap();
    let rows: Vec<Nonce> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1);
    rows.into_iter().next().unwrap().canonical_read_nonce
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn original_decision_and_guards(embedded: bool) {
    let (fixture, decision, original_run) =
        Box::pin(triage_ancestry_fixture(embedded, false)).await;
    let storage = &fixture.storage;
    let manifest = &fixture.manifest;
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let requirements = manifests
        .requirements(&manifest.space_id, &manifest.publication)
        .await
        .unwrap();
    assert_eq!(
        requirements
            .published_ingest_decision()
            .unwrap()
            .as_ref()
            .map(Link::id),
        Some(&decision.base.id)
    );
    progress_guard(storage, &requirements).await.unwrap();
    assert!(probe_exists(storage).await);
    remove_probe(storage, &manifest.space_id).await;
    let nonce = run_nonce(storage, &original_run).await;
    let lookup_key =
        progress::published_completion_key(&manifest.space_id, &manifest.publication.source_id);
    let previous_lookup: SourcePublicationCompletion =
        SourcePublicationRepository::new(storage.clone())
            .read_control(COMPLETION_TABLE, &lookup_key, &manifest.space_id)
            .await
            .unwrap()
            .unwrap();
    let mut unpublished_head = SourcePublicationRepository::new(storage.clone())
        .head(&manifest.space_id, &manifest.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    unpublished_head.published_revision = None;
    assert!(
        progress::capture(storage, manifest, &unpublished_head)
            .await
            .is_ok(),
        "an unpublished successor can capture an exact historical completion as its CAS preimage"
    );
    for mutation in 0..5 {
        let decisions = crate::IngestDecisionRepository::new(storage.clone());
        let runs = crate::WorkRunRepository::new(storage.clone());
        match mutation {
            0 => {
                let mut changed = decision.clone();
                changed.policy_version = Some("changed-after-original-capture".into());
                decisions
                    .update(&manifest.space_id, &decision.base.id, changed)
                    .await
                    .unwrap();
            }
            1 => {
                let mut changed = original_run.clone();
                changed.metrics.custom.remove("source_ingest_decision_id");
                runs.update(&manifest.space_id, &original_run.base.id, changed)
                    .await
                    .unwrap();
            }
            2 => {
                let mut changed = original_run.clone();
                changed.metrics.custom.get_mut("attempt_fence").unwrap()["lease_token"] =
                    serde_json::json!("another-original-attempt");
                runs.update(&manifest.space_id, &original_run.base.id, changed)
                    .await
                    .unwrap();
            }
            3 => {
                let foreign_source = RecordId::new_v4(TableName::Source);
                if let Some(store) = storage.embedded_store() {
                    store
                        .mutate(|tx| {
                            write_control_tx(
                                tx,
                                COMPLETION_TABLE,
                                &lookup_key,
                                &manifest.space_id,
                                &foreign_source,
                                &previous_lookup,
                            )
                        })
                        .await
                        .unwrap();
                } else {
                    storage.query_bind_checked("UPDATE $id SET source_id=$source;", crate::bindings! {
                        "id" => control_thing(COMPLETION_TABLE, &lookup_key), "source" => foreign_source.to_string(),
                    }).await.unwrap();
                }
            }
            _ => {
                let mut changed = previous_lookup.clone();
                changed.published_progress = None;
                overwrite_completion_at(storage, &changed, &lookup_key).await;
                assert!(
                    manifests
                        .requirements(&manifest.space_id, &manifest.publication)
                        .await
                        .is_err(),
                    "a new finalization cannot silently repair a malformed original lookup"
                );
                assert!(
                    progress::capture(storage, manifest, &unpublished_head)
                        .await
                        .is_err(),
                    "an unpublished head cannot bypass immutable completion equality"
                );
            }
        }
        let before_nonce = run_nonce(storage, &original_run).await;
        assert!(
            progress_guard(storage, &requirements).await.is_err(),
            "original proof mutation {mutation} accepted"
        );
        assert!(
            !probe_exists(storage).await,
            "guard failure must roll back a preceding write"
        );
        assert_eq!(
            run_nonce(storage, &original_run).await,
            before_nonce,
            "guard read touches must also roll back"
        );
        decisions
            .update(&manifest.space_id, &decision.base.id, decision.clone())
            .await
            .unwrap();
        overwrite_original_run(storage, &original_run).await;
        overwrite_completion_at(storage, &previous_lookup, &lookup_key).await;
    }
    if !embedded {
        assert!(
            nonce.is_some(),
            "positive guard must take a real native write conflict"
        );
    }
    // A successful triage cannot bypass the legitimacy of its own producer.
    // These mutations keep its actual run/decision and immutable work intent.
    let origin_key = original_run.work_item.id().id().to_string();
    let original_origin = SourcePublicationRepository::new(storage.clone())
        .read_control_payload("work_producer_origin", &origin_key, &manifest.space_id)
        .await
        .unwrap()
        .unwrap();
    for origin in [
        crate::work_producer_repository::WorkProducerOrigin::Unknown,
        crate::work_producer_repository::WorkProducerOrigin::ReprocessJob {
            job_id: RecordId::new_v4(TableName::ReprocessJob),
            original_job_intent_fingerprint: "missing-original-job".into(),
            original_currency_restricted: false,
            execution_origin:
                crate::work_producer_repository::ReprocessExecutionOrigin::Independent,
        },
    ] {
        let mut changed: serde_json::Value = serde_json::from_str(&original_origin).unwrap();
        changed["origin"] = serde_json::to_value(origin).unwrap();
        overwrite_origin_payload(
            storage,
            &manifest.space_id,
            &origin_key,
            &serde_json::to_string(&changed).unwrap(),
        )
        .await;
        assert!(
            manifests
                .requirements(&manifest.space_id, &manifest.publication)
                .await
                .is_err(),
            "successful triage must not short-circuit original producer validation"
        );
        assert!(progress_guard(storage, &requirements).await.is_err());
        assert!(!probe_exists(storage).await);
        overwrite_origin_payload(storage, &manifest.space_id, &origin_key, &original_origin).await;
    }
    // Recapture after restoring exact domain before-images, then publish through
    // the actual source transaction (no fabricated completion or head).
    Box::pin(publish_actual_metadata_acceptance(storage, manifest)).await;
    let completion = manifests
        .completion(&manifest.space_id, &manifest.publication)
        .await
        .unwrap()
        .unwrap();
    let certified = completion.published_progress.as_ref().unwrap();
    assert_eq!(
        certified.latest_ingest_decision.as_ref(),
        Some(&decision.base.id)
    );
    assert_eq!(
        certified.ingest_decision_fingerprint.as_ref(),
        Some(&source_support_fingerprint(&decision).unwrap())
    );
    let accepted = SourcePublicationRepository::new(storage.clone())
        .revision(
            &manifest.space_id,
            &manifest.publication.source_id,
            &manifest.publication.revision_id,
        )
        .await
        .unwrap()
        .unwrap();
    let mut expected = accepted.source.clone();
    expected.current_ingest_mode = Some(IngestMode::Metadata);
    expected.lifecycle_status = SourceLifecycleStatus::MetadataReady;
    expected.latest_ingest_decision = Some(Link::new(decision.base.id.clone()));
    assert_eq!(
        project(storage, &accepted.source_id, &accepted.space_id).await,
        Some(expected.clone())
    );
    pending_replacement(storage, &accepted).await;
    assert_eq!(
        project(storage, &accepted.source_id, &accepted.space_id).await,
        Some(expected)
    );
    let mut changed = decision.clone();
    changed.reason_codes.push("later-mutation".into());
    crate::IngestDecisionRepository::new(storage.clone())
        .update(&manifest.space_id, &decision.base.id, changed)
        .await
        .unwrap();
    assert!(
        project(storage, &accepted.source_id, &accepted.space_id)
            .await
            .is_none(),
        "changed certified decision cannot authorize historical progress"
    );
    Box::pin(direct_completed_triage(embedded)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn direct_completed_triage(embedded: bool) {
    let (fixture, decision, run) = Box::pin(triage_ancestry_fixture(embedded, true)).await;
    let storage = &fixture.storage;
    let manifest = &fixture.manifest;
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let work = crate::WorkItemRepository::new(storage.clone())
        .get(&manifest.space_id, run.work_item.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work.status, WorkItemStatus::Done);
    assert!(work.leased_by.is_none() && work.lease_expires_at.is_none());
    assert!(run.success && run.completed_at.is_some());
    let requirements = manifests
        .requirements(&manifest.space_id, &manifest.publication)
        .await
        .unwrap();
    assert_eq!(
        requirements
            .published_ingest_decision()
            .unwrap()
            .as_ref()
            .map(Link::id),
        Some(&decision.base.id)
    );
    for mismatch in 0..3 {
        let mut changed = run.clone();
        match mismatch {
            0 => changed.attempt += 1,
            1 => {
                changed.metrics.custom.get_mut("attempt_fence").unwrap()["lease_token"] =
                    serde_json::json!("different-original-attempt");
            }
            _ => {
                changed.metrics.custom.remove("attempt_fence");
            }
        }
        overwrite_original_run(storage, &changed).await;
        assert!(manifests
            .requirements(&manifest.space_id, &manifest.publication)
            .await
            .is_err());
        assert!(progress_guard(storage, &requirements).await.is_err());
        assert!(!probe_exists(storage).await);
        overwrite_original_run(storage, &run).await;
    }
    Box::pin(publish_actual_metadata_acceptance(storage, manifest)).await;
    let projected = project(storage, &manifest.publication.source_id, &manifest.space_id)
        .await
        .unwrap();
    assert_eq!(
        projected.latest_ingest_decision.as_ref().map(Link::id),
        Some(&decision.base.id)
    );
    assert_eq!(
        crate::WorkItemRepository::new(storage.clone())
            .get(&manifest.space_id, &work.base.id)
            .await
            .unwrap()
            .unwrap(),
        work,
        "historical proof must not synthesize or reclaim a Running work item"
    );
}

async fn overwrite_origin_payload(storage: &StorageClient, space: &str, key: &str, payload: &str) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                let mut row = tx
                    .scan_extension_rows("work_producer_origin")?
                    .into_iter()
                    .find(|(id, _)| id == &format!("work_producer_origin:{key}"))
                    .unwrap()
                    .1;
                assert_eq!(row["space_id"].as_str(), Some(space));
                row["payload"] = serde_json::json!(payload);
                tx.upsert_extension_row("work_producer_origin", row)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        storage.query_bind_checked("UPDATE $id SET payload=$payload WHERE space_id=$space;", crate::bindings! {
            "id" => control_thing("work_producer_origin", key), "space" => space, "payload" => payload,
        }).await.unwrap();
    }
}

async fn overwrite_original_run(storage: &StorageClient, run: &WorkRun) {
    // Restore the exact fixture before-image, including its original timestamp;
    // ordinary WorkRunRepository::update deliberately advances updated_at.
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| tx.replace(run.clone()).map_err(Into::into))
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "UPDATE $id CONTENT $value;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&run.base.id),
                    "value" => crate::WorkRunRepository::work_run_to_value(run).unwrap(),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn embedded_original_triage_decision_is_sealed_guarded_and_retained_during_pending_revision()
{
    Box::pin(original_decision_and_guards(true)).await;
}
#[tokio::test]
async fn native_original_triage_decision_is_sealed_guarded_and_retained_during_pending_revision() {
    Box::pin(original_decision_and_guards(false)).await;
}
