//! Causal merge erasure over two actually published sources. Acceptance, producer
//! completion, source support and inverse receipts all come from real repositories.

use super::*;
use crate::canonical_commit::{
    source_support_fingerprint, CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext,
    CanonicalMaintenanceOrigin, CanonicalPlan, CanonicalRecord, CanonicalRepository,
    PublishedSourceFence,
};
use crate::entity_merge_repository::{EntityMergeRepository, MergeSelection};
use crate::source_decision::{SourceDecisionRepository, DECISION_TABLE};
use crate::source_publication::{AcceptSourceRevision, SourceMutation};
use crate::{EntityRepository, Repository, SourceRepository, WorkArtifactRepository};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::{entity::EntityType, Entity, WorkArtifact};
use std::collections::BTreeSet;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn published_input(storage: &StorageClient, space: &str, label: &str) -> (Source, Entity) {
    let publications = SourcePublicationRepository::new(storage.clone());
    let mut source = Source::new(
        base(TableName::Source, space),
        SourceType::Manual,
        label.into(),
    );
    source.set_requested_ingest_mode(IngestMode::Metadata);
    let accepted = Box::pin(publications.accept(AcceptSourceRevision {
        source: source.clone(),
        revision_id: format!("revision-{label}"),
        expected: SourcePrecondition::Absent,
        idempotency_key: format!("accept-{label}"),
        request_fingerprint: format!("request-{label}"),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    }))
    .await
    .unwrap();
    let configuration = crate::SpaceConfigRepository::new(storage.clone())
        .resolve_space(space)
        .await
        .unwrap()
        .token;
    let manifest = Box::pin(stage_actual_metadata_acceptance(
        storage,
        &accepted,
        configuration,
    ))
    .await;
    Box::pin(publish_actual_metadata_acceptance(storage, &manifest)).await;
    let source = SourceRepository::new(storage.clone())
        .get(space, &source.base.id)
        .await
        .unwrap()
        .unwrap();
    let mut entity = Entity::new(
        base(TableName::Entity, space),
        EntityType::new("person"),
        format!("Original {label}"),
    );
    entity.base.provenance.sources = vec![source.base.id.clone()];
    entity.metadata = serde_json::json!({"original_contribution":label});
    let canonical = CanonicalRepository::new(storage.clone());
    let observed_epoch = canonical.epoch(space).await.unwrap();
    let dependencies = vec![CanonicalRecord::Source(Box::new(source.clone()))];
    let sources = Box::pin(canonical.capture_maintenance_support(space, &dependencies))
        .await
        .unwrap();
    assert!(
        matches!(sources.as_slice(), [PublishedSourceFence::Head {
        source_id,
        generation,
        published_revision,
    }] if source_id == &source.base.id
        && generation == &manifest.publication.generation
        && published_revision == &manifest.publication.revision_id),
        "entity input must capture the actual completed published Head"
    );
    Box::pin(canonical.commit(CanonicalCommit {
        space_id: space.into(),
        commit_id: format!("entity-{label}"),
        input_fingerprint: format!("entity-input-{label}"),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::Derived,
            attempt: None,
            sources,
        }),
        plan: CanonicalPlan {
            observed_epoch,
            dependencies,
            effects: vec![CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::Entity(Box::new(entity.clone()))),
            }],
            extension_effects: vec![],
            outbox: vec![],
        },
    }))
    .await
    .unwrap();
    let entity = EntityRepository::new(storage.clone())
        .get(space, &entity.base.id)
        .await
        .unwrap()
        .unwrap();
    (source, entity)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn inverse_deleted_merge_cannot_be_a_shared_survivor(embedded: bool) {
    let storage = client(embedded).await;
    let space = "mixed-source-merge-erasure";
    crate::SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig, space),
            embedding_provider: "openai".into(),
            embedding_model: "fixture-embedding".into(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
            processing: Some(processing()),
        })
        .await
        .unwrap();
    let (source_a, entity_a) = Box::pin(published_input(&storage, space, "A")).await;
    let (source_b, entity_b) = Box::pin(published_input(&storage, space, "B")).await;
    let entities = EntityRepository::new(storage.clone());
    let independent = entities
        .create(Entity::new(
            base(TableName::Entity, space),
            EntityType::new("person"),
            "Independent record".into(),
        ))
        .await
        .unwrap();
    let publications = SourcePublicationRepository::new(storage.clone());
    let original_b_support = publications
        .support_for_record(space, &entity_b.base.id)
        .await
        .unwrap();
    assert_eq!(original_b_support.len(), 1);
    assert_eq!(original_b_support[0].source_id, source_b.base.id);
    assert!(!original_b_support[0].independent);
    let head_b = publications
        .head(space, &source_b.base.id)
        .await
        .unwrap()
        .unwrap();
    let revision_b = publications
        .revision(
            space,
            &source_b.base.id,
            head_b.published_revision.as_ref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let prepared = Box::pin(EntityMergeRepository::new(storage.clone()).prepare_merge(
        space,
        &entity_a.base.id,
        &entity_b.base.id,
        MergeSelection::KeepB,
        "test",
    ))
    .await
    .unwrap();
    let merged = Box::pin(prepared.commit()).await.unwrap();
    assert_eq!(merged.survivor.base.id, entity_b.base.id);
    assert_eq!(merged.merge_record_ids.len(), 1);
    let merge_id = &merged.merge_record_ids[0];
    let original_merge = CanonicalRecord::load(&storage, space, merge_id)
        .await
        .unwrap()
        .unwrap();
    let supports = publications
        .support_for_record(space, merge_id)
        .await
        .unwrap();
    assert_eq!(
        supports.len(),
        2,
        "real merge creates both published support rows"
    );
    assert_eq!(
        supports
            .iter()
            .map(|support| support.source_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([source_a.base.id.clone(), source_b.base.id.clone()])
    );
    assert!(supports.iter().all(|support| !support.independent
        && support.record_fingerprint == original_merge.fingerprint().unwrap()));
    let decisions: Vec<crate::source_decision::SourceDecisionReceipt> = publications
        .list_controls(DECISION_TABLE, Some(space))
        .await
        .unwrap();
    assert!(decisions.iter().any(|decision| {
        decision.source_ids.contains(&source_a.base.id)
            && decision.source_ids.contains(&source_b.base.id)
            && decision.effects.iter().any(|effect| {
                effect.before.is_none()
                    && effect
                        .after
                        .as_ref()
                        .is_some_and(|record| record.base().id == *merge_id)
            })
    }));
    let invalidation = Box::pin(
        SourceDecisionRepository::new(storage.clone()).invalidation(space, &source_a.base.id),
    )
    .await
    .unwrap();
    assert!(
        invalidation.effects.iter().any(|effect| {
            effect.after.is_none()
                && effect
                    .before
                    .as_ref()
                    .is_some_and(|record| record.base().id == *merge_id)
        }),
        "the actual causal inverse deletes the two-supported merge output"
    );

    // This artifact names only the inverse-deleted output. Its cleanup and late
    // writer rejection must still be seeded by that output's removed identity.
    let artifact = WorkArtifact {
        base: base(TableName::WorkArtifact, space),
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "merge-dependent".into(),
        data: serde_json::json!({"output":merge_id,"private":"erase-dependent-payload"}),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: None,
        committed_at: Some(Utc::now()),
    };
    let artifacts = WorkArtifactRepository::new(storage.clone());
    artifacts.create(artifact.clone()).await.unwrap();
    let head_a = publications
        .head(space, &source_a.base.id)
        .await
        .unwrap()
        .unwrap();
    Box::pin(
        publications.erase_live(SourceMutation {
            space_id: space.into(),
            source_id: source_a.base.id.clone(),
            expected: SourcePrecondition::Generation(head_a.generation),
            idempotency_key: "erase-merge-A".into(),
            request_fingerprint: "erase-merge-A".into(),
            actor: "test".into(),
            authorized: publications
                .authorization_snapshot(space, &source_a.base.id)
                .await
                .unwrap()
                .unwrap(),
            authorized_document: None,
        }),
    )
    .await
    .unwrap();
    assert!(CanonicalRecord::load(&storage, space, merge_id)
        .await
        .unwrap()
        .is_none());
    assert!(publications
        .support_for_record(space, merge_id)
        .await
        .unwrap()
        .is_empty());
    assert!(entities
        .get_with_deleted(space, &entity_a.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(artifacts
        .get_including_deleted(space, &artifact.base.id)
        .await
        .unwrap()
        .is_none());
    let mut fresh_artifact = artifact.clone();
    fresh_artifact.base.id = RecordId::new_v4(TableName::WorkArtifact);
    assert!(
        artifacts.create(artifact).await.is_err(),
        "removed artifact keeps its permanent incarnation fence"
    );
    assert!(
        artifacts.create(fresh_artifact.clone()).await.is_err(),
        "a fresh artifact identity cannot reference the inverse-deleted merge output"
    );
    assert!(artifacts
        .get_including_deleted(space, &fresh_artifact.base.id)
        .await
        .unwrap()
        .is_none());
    let restored = entities
        .get(space, &entity_b.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source_support_fingerprint(&restored).unwrap(),
        source_support_fingerprint(&entity_b).unwrap()
    );
    let mut original_b_fields = serde_json::to_value(&entity_b).unwrap();
    let mut restored_b_fields = serde_json::to_value(&restored).unwrap();
    original_b_fields
        .as_object_mut()
        .unwrap()
        .remove("updated_at");
    restored_b_fields
        .as_object_mut()
        .unwrap()
        .remove("updated_at");
    assert_eq!(restored_b_fields, original_b_fields);
    let surviving_support = publications
        .support_for_record(space, &entity_b.base.id)
        .await
        .unwrap();
    assert_eq!(surviving_support.len(), 1);
    assert_eq!(surviving_support[0].source_id, source_b.base.id);
    assert_eq!(
        surviving_support[0].space_id,
        original_b_support[0].space_id
    );
    assert_eq!(
        surviving_support[0].record_id,
        original_b_support[0].record_id
    );
    assert_eq!(
        surviving_support[0].source_revision,
        original_b_support[0].source_revision
    );
    assert_eq!(
        surviving_support[0].independent,
        original_b_support[0].independent
    );
    assert_eq!(
        surviving_support[0].record_fingerprint,
        source_support_fingerprint(&restored).unwrap()
    );
    assert_eq!(
        entities.get(space, &independent.base.id).await.unwrap(),
        Some(independent)
    );
    assert_eq!(
        publications.head(space, &source_b.base.id).await.unwrap(),
        Some(head_b)
    );
    assert_eq!(
        serde_json::to_value(
            publications
                .revision(space, &source_b.base.id, &revision_b.revision_id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&revision_b).unwrap()
    );
    assert_eq!(
        SourceRepository::new(storage.clone())
            .get(space, &source_b.base.id)
            .await
            .unwrap(),
        Some(source_b)
    );
    let inventory = publications.erasure_inventory_rows(space).await.unwrap();
    assert!(!serde_json::to_string(&inventory)
        .unwrap()
        .contains("erase-dependent-payload"));
}

#[tokio::test]
async fn embedded_inverse_deleted_merge_with_two_published_supports() {
    Box::pin(inverse_deleted_merge_cannot_be_a_shared_survivor(true)).await;
}

#[tokio::test]
async fn surreal_inverse_deleted_merge_with_two_published_supports() {
    Box::pin(inverse_deleted_merge_cannot_be_a_shared_survivor(false)).await;
}
