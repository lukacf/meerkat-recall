//! Actual canonical transactions; neither the private proof nor a decision is
//! fabricated by these oracles.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::canonical_commit::{
    CanonicalEffect, CanonicalFault, CanonicalMaintenanceContext, CanonicalPlan,
};
use crate::{
    EntityRepository, KnowledgeViewDefinitionRepository, SpaceConfigRepository, StorageClient,
    StorageConfig,
};
use chrono::Utc;
use elephant_types::records::{entity::EntityType, Entity, SpaceConfig, SystemEvent};
use elephant_types::{Link, RecordId, TableName};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .expect("backend");
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .expect("schema");
    }
    let space: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id": RecordId::new_v4(TableName::SpaceConfig), "space_id": "wiki-test",
        "created_at": Utc::now(), "updated_at": Utc::now(), "created_by": "test",
        "security": {"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance": {}, "subjects": [], "subjects_state": "complete",
        "embedding_provider":"fake", "embedding_model":"test", "embedding_dims":8
    }))
    .unwrap();
    SpaceConfigRepository::new(client.clone())
        .create(space)
        .await
        .unwrap();
    client
}

fn entity() -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "wiki-test".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "Original inventory".into(),
    )
}

fn definition(entity: &Entity, commit: &str) -> KnowledgeViewDefinition {
    let mut base = input_envelope(&CanonicalRecord::Entity(Box::new(entity.clone()))).unwrap();
    base.id = RecordId::new_v4(TableName::KnowledgeViewDefinition);
    let mut row = KnowledgeViewDefinition {
        base,
        title: "Generated original title".into(),
        publication_status: KnowledgeViewPublicationStatus::Published,
        related_articles: Vec::new(),
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(entity.base.id.clone()),
            relation_types: Vec::new(),
            max_hops: 1,
            max_members: 10,
        },
    };
    bind_definition_decision(&mut row, commit).unwrap();
    row
}

async fn request(
    client: &StorageClient,
    entity: &Entity,
    definitions: &[KnowledgeViewDefinition],
    commit: &str,
) -> CanonicalCommit {
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "wiki-test".into(),
            "test".into(),
        ),
        seq: 0,
        event_type: "wiki.definitions_changed".into(),
        payload: serde_json::json!({"count": definitions.len()}),
    };
    CanonicalCommit {
        space_id: "wiki-test".into(),
        commit_id: commit.into(),
        input_fingerprint: format!("input:{commit}"),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::Derived,
            attempt: None,
            sources: Vec::new(),
        }),
        plan: CanonicalPlan {
            observed_epoch: CanonicalRepository::new(client.clone())
                .epoch("wiki-test")
                .await
                .unwrap(),
            dependencies: vec![CanonicalRecord::Entity(Box::new(entity.clone()))],
            effects: definitions
                .iter()
                .map(|row| CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::KnowledgeViewDefinition(Box::new(
                        row.clone(),
                    ))),
                })
                .collect(),
            extension_effects: Vec::new(),
            outbox: vec![event],
        },
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn actual_bundle(embedded: bool) {
    let client = client(embedded).await;
    let entity = EntityRepository::new(client.clone())
        .create(entity())
        .await
        .unwrap();
    let first = definition(&entity, "definition-bundle");
    let second = definition(&entity, "definition-bundle");
    let canonical = CanonicalRepository::new(client.clone());
    let repo = KnowledgeViewDefinitionRepository::new(client.clone());
    let request = request(
        &client,
        &entity,
        &[first.clone(), second.clone()],
        "definition-bundle",
    )
    .await;
    for fault in [
        CanonicalFault::AfterFirstEffect,
        CanonicalFault::BeforeOutbox,
        CanonicalFault::BeforeReceipt,
    ] {
        assert!(canonical
            .commit_with_fault(request.clone(), fault)
            .await
            .is_err());
        assert!(repo
            .get("wiki-test", &first.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(repo
            .get("wiki-test", &second.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(
            CanonicalRecord::load(&client, "wiki-test", &request.plan.outbox[0].base.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(canonical
            .receipt("wiki-test", "definition-bundle")
            .await
            .unwrap()
            .is_none());
        assert!(SourcePublicationRepository::new(client.clone())
            .read_control::<SourceDecisionReceipt>(
                DECISION_TABLE,
                &stable_key(&["wiki-test", "definition-bundle"]),
                "wiki-test"
            )
            .await
            .unwrap()
            .is_none());
    }
    let receipt = canonical.commit(request.clone()).await.unwrap();
    assert_eq!(receipt.effect_ids.len(), 2);
    assert_eq!(canonical.commit(request).await.unwrap(), receipt);
    assert_eq!(
        repo.get("wiki-test", &first.base.id)
            .await
            .unwrap()
            .as_ref(),
        Some(&first)
    );
    let inputs = canonical
        .definition_decision_inputs(&first)
        .await
        .unwrap()
        .expect("original input proof");
    assert_eq!(inputs.len(), 1);
    let decisions: Vec<SourceDecisionReceipt> = SourcePublicationRepository::new(client.clone())
        .list_controls(DECISION_TABLE, Some("wiki-test"))
        .await
        .unwrap();
    assert_eq!(decisions.len(), 1);
    assert!(decisions[0].source_ids.is_empty());
    assert_eq!(decisions[0].dependency_ids, vec![entity.base.id.clone()]);
    let public = serde_json::to_value(receipt).unwrap();
    assert!(
        public.get(FIELD).is_none(),
        "private input payload is not a wire field"
    );

    let mut tampered = first.clone();
    tampered.title = "Uncommitted title".into();
    repo.upsert(tampered.clone()).await.unwrap();
    assert!(canonical
        .definition_decision_inputs(&tampered)
        .await
        .unwrap()
        .is_none());
    assert!(canonical
        .definition_decision_inputs(&first)
        .await
        .unwrap()
        .is_none());
    repo.upsert(first.clone()).await.unwrap();
    let mut changed = entity.clone();
    changed.display_name = "Changed actual inventory".into();
    EntityRepository::new(client.clone())
        .update("wiki-test", &entity.base.id, changed)
        .await
        .unwrap();
    assert!(canonical
        .definition_decision_inputs(&first)
        .await
        .unwrap()
        .is_none());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn missing_and_stale(embedded: bool) {
    let client = client(embedded).await;
    let entity = EntityRepository::new(client.clone())
        .create(entity())
        .await
        .unwrap();
    let canonical = CanonicalRepository::new(client.clone());
    let repo = KnowledgeViewDefinitionRepository::new(client.clone());
    let fabricated = definition(&entity, "never-committed");
    repo.upsert(fabricated.clone()).await.unwrap();
    assert!(canonical
        .definition_decision_inputs(&fabricated)
        .await
        .unwrap()
        .is_none());
    let row = definition(&entity, "stale-inventory");
    let request = request(
        &client,
        &entity,
        std::slice::from_ref(&row),
        "stale-inventory",
    )
    .await;
    let mut changed = entity.clone();
    changed.base.security.labels.push("new-restriction".into());
    EntityRepository::new(client.clone())
        .update("wiki-test", &entity.base.id, changed)
        .await
        .unwrap();
    assert!(canonical.commit(request).await.is_err());
    assert!(repo.get("wiki-test", &row.base.id).await.unwrap().is_none());
    assert!(canonical
        .receipt("wiki-test", "stale-inventory")
        .await
        .unwrap()
        .is_none());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn hidden_only(embedded: bool) {
    let client = client(embedded).await;
    let entity = EntityRepository::new(client.clone())
        .create(entity())
        .await
        .unwrap();
    let canonical = CanonicalRepository::new(client.clone());
    let row = definition(&entity, "visible-original");
    canonical
        .commit(
            request(
                &client,
                &entity,
                std::slice::from_ref(&row),
                "visible-original",
            )
            .await,
        )
        .await
        .unwrap();
    let mut hidden = row.clone();
    hidden.publication_status = KnowledgeViewPublicationStatus::Hidden;
    hidden.base.updated_at = Utc::now();
    let mut hide = request(&client, &entity, &[], "hide-original").await;
    hide.plan.dependencies.clear();
    hide.plan.effects.push(CanonicalEffect {
        before: Some(CanonicalRecord::KnowledgeViewDefinition(Box::new(row))),
        after: Some(CanonicalRecord::KnowledgeViewDefinition(Box::new(
            hidden.clone(),
        ))),
    });
    let mut wrong = hide.clone();
    if let Some(CanonicalRecord::KnowledgeViewDefinition(row)) = &mut wrong.plan.effects[0].after {
        row.title = "Sneaked fresh content".into();
    }
    assert!(canonical.commit(wrong).await.is_err());
    canonical.commit(hide).await.unwrap();
    assert!(canonical
        .definition_decision_inputs(&hidden)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        KnowledgeViewDefinitionRepository::new(client)
            .get("wiki-test", &hidden.base.id)
            .await
            .unwrap()
            .unwrap()
            .publication_status,
        KnowledgeViewPublicationStatus::Hidden
    );
}

#[tokio::test]
async fn embedded_actual_definition_bundle_and_current_inputs() {
    actual_bundle(true).await;
}
#[tokio::test]
async fn surreal_actual_definition_bundle_and_current_inputs() {
    actual_bundle(false).await;
}
#[tokio::test]
async fn embedded_definition_marker_and_stale_input_refused() {
    missing_and_stale(true).await;
}
#[tokio::test]
async fn surreal_definition_marker_and_stale_input_refused() {
    missing_and_stale(false).await;
}
#[tokio::test]
async fn embedded_definition_hidden_only_cannot_certify_content() {
    hidden_only(true).await;
}
#[tokio::test]
async fn surreal_definition_hidden_only_cannot_certify_content() {
    hidden_only(false).await;
}

#[test]
fn typed_entity_subjects_are_mandatory_without_mutating_raw_input() {
    let row = entity();
    assert!(row.base.subjects.is_empty());
    let scope = input_envelope(&CanonicalRecord::Entity(Box::new(row.clone()))).unwrap();
    assert!(scope
        .subjects
        .iter()
        .any(|subject| subject.id() == &row.base.id));
    assert_eq!(scope.subjects_state, SubjectsState::Complete);
    let mut output = scope.clone();
    output.subjects.clear();
    assert!(!contains_scope(&scope, &output));
    assert!(row.base.subjects.is_empty());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_source_input_is_authority(embedded: bool) {
    use elephant_types::records::{Source, SourceType};
    let client = client(embedded).await;
    let entity = EntityRepository::new(client.clone())
        .create(entity())
        .await
        .unwrap();
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "wiki-test".into(),
            "test".into(),
        ),
        SourceType::Manual,
        "retained-independent-input".into(),
    );
    source.base.subjects_state = SubjectsState::Complete;
    let source = crate::SourceRepository::new(client.clone())
        .create(source)
        .await
        .unwrap();
    // Actual retained pre-head input; no completed publication is manufactured.
    let mut row = definition(&entity, "physical-source-definition");
    let canonical = CanonicalRepository::new(client.clone());
    let mut request = request(
        &client,
        &entity,
        &[row.clone()],
        "physical-source-definition",
    )
    .await;
    request
        .plan
        .dependencies
        .push(CanonicalRecord::Source(Box::new(source.clone())));
    request
        .maintenance
        .as_mut()
        .unwrap()
        .sources
        .push(PublishedSourceFence::Legacy {
            source: Box::new(source.clone()),
        });
    assert!(
        canonical.commit(request.clone()).await.is_err(),
        "physical source itself cannot be omitted from derived provenance"
    );
    assert!(canonical
        .receipt("wiki-test", "physical-source-definition")
        .await
        .unwrap()
        .is_none());
    row.base.provenance.sources.push(source.base.id.clone());
    request.plan.effects[0].after = Some(CanonicalRecord::KnowledgeViewDefinition(Box::new(
        row.clone(),
    )));
    canonical.commit(request).await.unwrap();
    assert_eq!(
        canonical
            .definition_decision_inputs(&row)
            .await
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    crate::SourceRepository::new(client.clone())
        .delete("wiki-test", &source.base.id)
        .await
        .unwrap();
    assert!(canonical
        .definition_decision_inputs(&row)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn embedded_definition_retains_physical_source_input_authority() {
    physical_source_input_is_authority(true).await;
}
#[tokio::test]
async fn surreal_definition_retains_physical_source_input_authority() {
    physical_source_input_is_authority(false).await;
}

fn published_snapshot() -> SourceRevision {
    let mut source = elephant_types::records::Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "wiki-test".into(),
            "test".into(),
        ),
        elephant_types::records::SourceType::Manual,
        "hash-r1".into(),
    );
    source.base.subjects_state = SubjectsState::Complete;
    SourceRevision {
        space_id: source.base.space_id.clone(),
        source_id: source.base.id.clone(),
        revision_id: "r1".into(),
        content_hash: source.hash.clone(),
        accepted_at: Utc::now(),
        source,
    }
}

#[test]
fn pending_desired_content_does_not_replace_published_authorization() {
    let published = published_snapshot();
    let mut desired = published.source.clone();
    desired.hash = "hash-r2".into();
    desired.metadata = serde_json::json!({"pending": "new desired text"});
    desired.base.updated_at = Utc::now();
    assert!(same_source_authorization(&published.source, &desired));
    assert_ne!(published.source.hash, desired.hash);
    assert!(revision_identity(
        &published,
        "wiki-test",
        &desired.base.id,
        "r1"
    ));
    assert!(!revision_identity(
        &published,
        "wiki-test",
        &desired.base.id,
        "r2"
    ));
}

#[test]
fn pending_source_policy_changes_cannot_borrow_original_definition() {
    let original = published_snapshot().source;
    let mut current = original.clone();
    current.base.security.level += 1;
    assert!(!same_source_authorization(&original, &current));
    current = original.clone();
    current.base.subjects_state = SubjectsState::Unknown;
    assert!(!same_source_authorization(&original, &current));
    current = original.clone();
    current.base.deleted_at = Some(Utc::now());
    assert!(!same_source_authorization(&original, &current));
}

#[test]
fn published_revision_identity_binds_original_source_hash_and_incarnation() {
    let mut original = published_snapshot();
    let id = original.source_id.clone();
    assert!(revision_identity(&original, "wiki-test", &id, "r1"));
    original.source.hash = "tampered-immutable-content".into();
    assert!(!revision_identity(&original, "wiki-test", &id, "r1"));
    original.source.hash = original.content_hash.clone();
    original.source.base.id = RecordId::new_v4(TableName::Source);
    assert!(!revision_identity(&original, "wiki-test", &id, "r1"));
}

#[test]
fn definition_must_include_published_and_raw_source_authority() {
    let entity = entity();
    let mut row = definition(&entity, "published-union");
    let mut published = published_snapshot();
    published.source.base.security.level = 4;
    let supporting = RecordId::new_v4(TableName::Source);
    published
        .source
        .base
        .provenance
        .sources
        .push(supporting.clone());
    row.base
        .provenance
        .sources
        .push(published.source_id.clone());
    let input = CanonicalRecord::Source(Box::new(published.source));
    assert!(!input_contained(&input, &row).unwrap());
    row.base.security.level = 4;
    assert!(
        !input_contained(&input, &row).unwrap(),
        "published provenance is material too"
    );
    row.base.provenance.sources.push(supporting);
    assert!(input_contained(&input, &row).unwrap());
}
