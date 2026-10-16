//! Real canonical definition decisions consumed by the public projection seam.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::ReadEligibility;
use crate::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin,
    CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use crate::{
    EntityRepository, KnowledgeViewDefinitionRepository, Repository, StorageClient, StorageConfig,
};
use elephant_types::records::{
    base::BaseFields, entity::EntityType, Entity, KnowledgeViewDefinition,
    KnowledgeViewPublicationStatus, KnowledgeViewSelector, SystemEvent,
};
use elephant_types::{Link, RecordId, TableName};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let client = StorageClient::new(StorageConfig::default()).await.unwrap();
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .unwrap();
        client
    };
    // Actual native canonical commits require an existing named SpaceConfig.
    let space: elephant_types::records::SpaceConfig = serde_json::from_value(serde_json::json!({
        "id": elephant_types::RecordId::new_v4(TableName::SpaceConfig),
        "space_id":"definition-read", "created_at":chrono::Utc::now(),
        "updated_at":chrono::Utc::now(), "created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
    }))
    .unwrap();
    crate::SpaceConfigRepository::new(client.clone())
        .create(space)
        .await
        .unwrap();
    client
}

fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        "definition-read".into(),
        "test".into(),
    )
}
async fn prepare(client: &StorageClient) -> (Entity, KnowledgeViewDefinition, CanonicalCommit) {
    let entity = EntityRepository::new(client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("project"),
            "Original inventory".into(),
        ))
        .await
        .unwrap();
    let mut envelope = crate::canonical_definition::input_envelope(&CanonicalRecord::Entity(
        Box::new(entity.clone()),
    ))
    .unwrap();
    envelope.id = RecordId::new_v4(TableName::KnowledgeViewDefinition);
    let mut definition = KnowledgeViewDefinition {
        base: envelope,
        title: "Original derived definition".into(),
        publication_status: KnowledgeViewPublicationStatus::Published,
        related_articles: vec![],
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(entity.base.id.clone()),
            relation_types: vec![],
            max_hops: 1,
            max_members: 5,
        },
    };
    crate::canonical_definition::bind_definition_decision(&mut definition, "original-definition")
        .unwrap();
    let request = CanonicalCommit {
        space_id: definition.base.space_id.clone(),
        commit_id: "original-definition".into(),
        input_fingerprint: "actual-inventory".into(),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::Derived,
            attempt: None,
            sources: vec![],
        }),
        plan: CanonicalPlan {
            observed_epoch: CanonicalRepository::new(client.clone())
                .epoch(&definition.base.space_id)
                .await
                .unwrap(),
            dependencies: vec![CanonicalRecord::Entity(Box::new(entity.clone()))],
            effects: vec![CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::KnowledgeViewDefinition(Box::new(
                    definition.clone(),
                ))),
            }],
            extension_effects: vec![],
            outbox: vec![SystemEvent {
                base: base(TableName::SystemEvent),
                seq: 0,
                event_type: "wiki.definitions_changed".into(),
                payload: serde_json::json!({"count":1}),
            }],
        },
    };
    (entity, definition, request)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn actual_decision(embedded: bool) {
    let client = client(embedded).await;
    let (entity, definition, request) = prepare(&client).await;
    CanonicalRepository::new(client.clone())
        .commit(request)
        .await
        .unwrap();
    let reader = ReadEligibility::new(client.clone());
    assert_eq!(
        reader.project(definition.clone(), &|_| true).await.unwrap(),
        Some(definition.clone())
    );
    // Entity self is a real implicit subject even when its raw BaseFields has none.
    let original_entity = entity.base.id.clone();
    assert!(reader
        .project(definition.clone(), &|scope| {
            scope.id != original_entity
                || scope
                    .subjects
                    .iter()
                    .any(|subject| subject.id() == &original_entity)
        })
        .await
        .unwrap()
        .is_some());
    assert!(reader
        .project(definition.clone(), &|scope| scope.id != original_entity)
        .await
        .unwrap()
        .is_none());
    let mut changed = entity.clone();
    changed.display_name = "Uncommitted later input".into();
    EntityRepository::new(client.clone())
        .update(&entity.base.space_id, &entity.base.id, changed)
        .await
        .unwrap();
    assert!(reader.verify_current().await.is_err());
    assert!(ReadEligibility::new(client.clone())
        .project(definition, &|_| true)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn exact_definition_decision_authorizes_typed_original_inputs_on_both_backends() {
    for embedded in [true, false] {
        actual_decision(embedded).await;
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn marker_only_or_changed_definition_cannot_gain_read_authority() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let (_, definition, _request) = prepare(&client).await;
        let repo = KnowledgeViewDefinitionRepository::new(client.clone());
        repo.upsert(definition.clone()).await.unwrap();
        assert!(ReadEligibility::new(client.clone())
            .project(definition.clone(), &|_| true)
            .await
            .unwrap()
            .is_none());
        // Use a fresh fixture for real publication; marker-only storage never
        // gets retroactively certified by creating a receipt for its old row.
        let client = self::client(embedded).await;
        let (_, definition, request) = prepare(&client).await;
        let repo = KnowledgeViewDefinitionRepository::new(client.clone());
        CanonicalRepository::new(client.clone())
            .commit(request)
            .await
            .unwrap();
        let mut changed = definition.clone();
        changed.title = "Substituted title".into();
        repo.upsert(changed.clone()).await.unwrap();
        assert!(ReadEligibility::new(client.clone())
            .project(changed, &|_| true)
            .await
            .unwrap()
            .is_none());
        let mut hidden = definition;
        hidden.publication_status = KnowledgeViewPublicationStatus::Hidden;
        repo.upsert(hidden.clone()).await.unwrap();
        assert!(ReadEligibility::new(client)
            .project(hidden, &|_| true)
            .await
            .unwrap()
            .is_none());
    }
}
