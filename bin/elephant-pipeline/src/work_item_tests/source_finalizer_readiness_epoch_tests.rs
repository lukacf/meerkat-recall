//! Interleave real finalizer readiness with an already prepared canonical write.
//! The ordering is deterministic: no scheduler coincidence or provider is needed.

use super::source_finalizer_contention_tests::{
    dispatch_claimed, stage_finalizers, FinalizerOutcome, ACTOR, SPACE,
};
use super::*;
use elephant_storage::canonical_commit::{
    CanonicalCommit, CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::StorageError;
use elephant_types::publication::OperationState;

fn planned_entity(epoch: &str, label: &str) -> (Entity, CanonicalCommit) {
    let entity = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            ACTOR.into(),
        ),
        EntityType::new("project"),
        label.into(),
    );
    let mut plan = CanonicalPlan {
        observed_epoch: epoch.into(),
        ..CanonicalPlan::default()
    };
    plan.push(
        None,
        Some(CanonicalRecord::Entity(Box::new(entity.clone()))),
    )
    .unwrap();
    let commit = CanonicalCommit {
        space_id: SPACE.into(),
        commit_id: format!("readiness-epoch-{label}"),
        input_fingerprint: format!("original-{label}"),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: None,
        plan,
    };
    (entity, commit)
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep deterministic readiness interleaving, stale-plan refusal and actual source completion together"
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn readiness_does_not_invalidate_prepared_work(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        Box::pin(initialize_fixture_schema(&storage)).await;
        storage
    };
    let (publications, finalizers) = Box::pin(stage_finalizers(&storage, 2, true)).await;
    let queue = WorkItemRepository::new(storage.clone());
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let sources = SourcePublicationRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    let entities = EntityRepository::new(storage.clone());
    let mut claimed = Vec::new();
    let mut requirements = Vec::new();
    let mut original_heads = Vec::new();
    let mut original_operations = Vec::new();
    let mut accepted_sources = Vec::new();
    let mut original_documents = Vec::new();
    for item in finalizers {
        let original = queue
            .claim_with_lease(
                SPACE,
                &item.base.id,
                ACTOR,
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let attempt =
            elephant_storage::work_attempt_repository::fence_from_claim(&original).unwrap();
        let publication = elephant_pipeline::source_queue::publication_from_work(&original)
            .unwrap()
            .unwrap();
        requirements.push(
            manifests
                .requirements_for_attempt(SPACE, &publication, &attempt)
                .await
                .unwrap(),
        );
        original_heads.push(sources.head(SPACE, &publication.source_id).await.unwrap());
        original_operations.push(
            sources
                .operation(SPACE, &publication.operation_id)
                .await
                .unwrap(),
        );
        let accepted = sources
            .revision(SPACE, &publication.source_id, &publication.revision_id)
            .await
            .unwrap()
            .unwrap();
        // Intake scopes the physical document to this immutable acceptance;
        // the template ID is input, not the persisted evidence identity.
        let doc_id = elephant_storage::source_intake::accepted_document_id(
            &publication,
            &accepted
                .source
                .manual_doc_template()
                .unwrap()
                .unwrap()
                .doc_id,
        )
        .unwrap();
        original_documents.push(
            DocRepository::new(storage.clone())
                .get(SPACE, &doc_id)
                .await
                .unwrap()
                .unwrap(),
        );
        accepted_sources.push((accepted.source, publication));
        claimed.push(original);
    }
    let epoch = canonical.epoch(SPACE).await.unwrap();
    let (winner, prepared) = planned_entity(&epoch, "survives-readiness");
    let (stale, stale_plan) = planned_entity(&epoch, "refused-after-real-write");
    assert!(entities
        .get(SPACE, &winner.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(entities.get(SPACE, &stale.base.id).await.unwrap().is_none());

    // Exercise the exact production readiness transaction for two independent
    // pending sources. Each call may refresh private physical read fences, but
    // it cannot invalidate a peer's original semantic planning observation.
    for _ in 0..3 {
        for (requirement, (source, publication)) in requirements.iter().zip(&accepted_sources) {
            // This is the recovery path's real idempotent document staging,
            // using the immutable acceptance rather than the latest Source.
            Box::pin(elephant_storage::source_intake::stage_source_revision(
                &storage,
                source,
                publication,
            ))
            .await
            .unwrap();
            assert_eq!(
                canonical.epoch(SPACE).await.unwrap(),
                epoch,
                "idempotent recovery staging invalidated a peer's prepared work"
            );
            Box::pin(manifests.check_ready(requirement)).await.unwrap();
            assert_eq!(
                canonical.epoch(SPACE).await.unwrap(),
                epoch,
                "readiness bookkeeping invalidated a peer's prepared work"
            );
        }
    }
    for (index, item) in claimed.iter().enumerate() {
        let publication = elephant_pipeline::source_queue::publication_from_work(item)
            .unwrap()
            .unwrap();
        assert_eq!(
            sources.head(SPACE, &publication.source_id).await.unwrap(),
            original_heads[index]
        );
        assert_eq!(
            sources
                .operation(SPACE, &publication.operation_id)
                .await
                .unwrap(),
            original_operations[index]
        );
        assert!(manifests
            .completion(SPACE, &publication)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            queue.get(SPACE, &item.base.id).await.unwrap().unwrap(),
            *item
        );
        assert_eq!(
            DocRepository::new(storage.clone())
                .get(SPACE, &original_documents[index].base.id)
                .await
                .unwrap()
                .unwrap(),
            original_documents[index],
            "idempotent recovery staging changed accepted document content"
        );
    }

    // This is the original plan, not a freshly captured epoch after readiness.
    Box::pin(canonical.commit(prepared)).await.unwrap();
    let changed_epoch = canonical.epoch(SPACE).await.unwrap();
    assert_ne!(
        changed_epoch, epoch,
        "real canonical writes must still invalidate plans"
    );
    let stale_commit_id = stale_plan.commit_id.clone();
    let refused = Box::pin(canonical.commit(stale_plan)).await.unwrap_err();
    assert!(
        matches!(refused, StorageError::Conflict(ref code) if code == "canonical_inputs_changed")
    );
    assert!(entities.get(SPACE, &stale.base.id).await.unwrap().is_none());
    assert!(canonical
        .receipt(SPACE, &stale_commit_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(canonical.epoch(SPACE).await.unwrap(), changed_epoch);

    // Both sources still need the full dispatcher/finalizer transaction under
    // their original claims; successful readiness grants no completion itself.
    for item in &claimed {
        let outcome = Box::pin(dispatch_claimed(&storage, item, None))
            .await
            .unwrap();
        assert!(
            matches!(outcome, FinalizerOutcome::Completed),
            "{outcome:?}"
        );
        let completed = queue.get(SPACE, &item.base.id).await.unwrap().unwrap();
        assert_eq!(completed.status, WorkItemStatus::Done);
        assert_eq!(completed.attempts, item.attempts);
        assert_eq!(
            completed.payload, item.payload,
            "completion replaced original authority"
        );
    }
    for publication in publications {
        let operation = sources
            .operation(SPACE, &publication.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.state, OperationState::Completed);
        let completion = manifests
            .completion(SPACE, &publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completion.publication, publication);
        let receipt = canonical
            .receipt(SPACE, &completion.commit_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.publication_authority.as_ref(), Some(&publication));
        assert_eq!(
            sources
                .head(SPACE, &publication.source_id)
                .await
                .unwrap()
                .unwrap()
                .published_revision
                .as_deref(),
            Some("r1")
        );
    }
}

#[tokio::test]
async fn embedded_finalizer_readiness_preserves_epoch_but_real_writes_refuse_stale_plans() {
    Box::pin(readiness_does_not_invalidate_prepared_work(true)).await;
}

#[tokio::test]
async fn surreal_finalizer_readiness_preserves_epoch_but_real_writes_refuse_stale_plans() {
    Box::pin(readiness_does_not_invalidate_prepared_work(false)).await;
}
