//! Publication regressions exercise the existing transaction on both backends.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::canonical_commit::CanonicalRepository;
use crate::{
    EntityRepository, Repository, SpaceConfigRepository, StorageConfig, SystemEventRepository,
};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{CitationKind, SpaceConfig};

#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn fixture(embedded: bool) -> (StorageClient, Entity, KnowledgeView, KnowledgeViewRevision) {
    use elephant_types::records::HasSecurity;
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .unwrap();
    }
    let space = "wiki-guarded";
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":space,
        "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
    }))
    .unwrap();
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await
        .unwrap();
    let root = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space.into(),
            "author".into(),
        ),
        EntityType::new("project"),
        "An independent source-less project".into(),
    );
    EntityRepository::new(client.clone())
        .create(root.clone())
        .await
        .unwrap();
    let now = Utc::now();
    let view_id = RecordId::new_v4(TableName::KnowledgeView);
    let revision_id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    let mut view = KnowledgeView {
        base: BaseFields::new(view_id.clone(), space.into(), "wiki-test".into()),
        key: format!("view:{}", root.base.id),
        kind: KnowledgeViewKind::Entity,
        surface: KnowledgeViewSurface::Inspector,
        publication_status: KnowledgeViewPublicationStatus::Published,
        root_record: root.base.id.to_string(),
        title: root.display_name.clone(),
        slug: "project".into(),
        security_fingerprint: "test-policy".into(),
        current_revision: Some(revision_id.to_string()),
        revision_count: 1,
        state: ProjectionState::Current,
        state_reason: None,
        published_at: Some(now),
    };
    let mut revision = KnowledgeViewRevision {
        base: BaseFields::new(revision_id, space.into(), "wiki-test".into()),
        view_id: view_id.to_string(),
        revision: 1,
        kind: KnowledgeViewKind::Entity,
        root_record: root.base.id.to_string(),
        blocks: vec![],
        rendered_markdown: root.display_name.clone(),
        citations: vec![KnowledgeCitation {
            id: "c1".into(),
            kind: CitationKind::GraphRecord,
            record_id: root.base.id.to_string(),
            assertion_id: None,
            source_id: None,
            evidence_span: None,
        }],
        dependencies: vec![
            KnowledgeDependency {
                record_id: root.base.id.to_string(),
                record_type: "entity".into(),
                role: "root".into(),
                observed_updated_at: Some(root.base.updated_at),
            },
            KnowledgeDependency {
                record_id: "model:test-writer".into(),
                record_type: "model".into(),
                role: "article_writer".into(),
                observed_updated_at: None,
            },
            KnowledgeDependency {
                record_id: "prompt:test-v1".into(),
                record_type: "prompt".into(),
                role: "article_writer_prompt".into(),
                observed_updated_at: None,
            },
        ],
        freshness: ProjectionFreshness {
            state: ProjectionState::Current,
            minimum_claim_score: None,
            stale_truth_slots: 0,
            unresolved_conflicts: 0,
            unsupported_claims: 0,
            expired_claims: 0,
            selection_truncated: false,
            valid_until: None,
        },
        validation: ValidationReceipt {
            citations_resolve: true,
            factual_blocks_cited: true,
            statements_supported: true,
            prose_is_structured: true,
            factual_block_count: 0,
            citation_count: 1,
            statement_count: 0,
            warnings: vec![],
        },
        compiler: CompilerReceipt {
            compiler: "test".into(),
            version: 1,
            dependency_hash: "test-dependency".into(),
            policy_fingerprint: "test-policy".into(),
        },
        compiled_at: now,
        supersedes: None,
    };
    // A derived view includes the typed entity's intrinsic self subject.
    // Publication must validate the same authorization view as compilation.
    view.base.subjects = root.subjects();
    view.base.subjects_state = root.subjects_state();
    revision.base.subjects = root.subjects();
    revision.base.subjects_state = root.subjects_state();
    (client, root, view, revision)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn publish_independent_and_keep_atomic_receipt(embedded: bool) {
    let (client, _, view, revision) = fixture(embedded).await;
    let repository = KnowledgeViewRepository::new(client.clone());
    let before = CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap();
    repository
        .publish_at_epoch(view.clone(), revision.clone(), &before)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_view("wiki-guarded", &view.base.id)
            .await
            .unwrap(),
        Some(view.clone())
    );
    assert_eq!(
        repository
            .get_revision("wiki-guarded", &revision.base.id)
            .await
            .unwrap(),
        Some(revision.clone())
    );
    assert_eq!(
        repository
            .list_dependency_indexes("wiki-guarded", &view.base.id)
            .await
            .unwrap()
            .len(),
        3
    );
    let events = SystemEventRepository::new(client.clone())
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(!events[0]
        .payload
        .to_string()
        .contains(&revision.rendered_markdown));
    assert_ne!(
        CanonicalRepository::new(client)
            .epoch("wiki-guarded")
            .await
            .unwrap(),
        before
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn stale_preparation_rejects_same_timestamp_reclassification(embedded: bool) {
    let (client, mut root, view, revision) = fixture(embedded).await;
    let observed = CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap();
    // A full authorization change must conflict even if a caller preserves time.
    root.base.security.level = 4;
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| tx.replace(root.clone()).map_err(Into::into))
            .await
            .unwrap();
    } else {
        client
            .query_bind_checked(
                "UPDATE $id CONTENT $row",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&root.base.id),
                    "row" => crate::surreal::entity_to_value(&root).unwrap(),
                },
            )
            .await
            .unwrap();
    }
    let repository = KnowledgeViewRepository::new(client.clone());
    assert!(repository
        .publish_at_epoch(view.clone(), revision.clone(), &observed)
        .await
        .is_err());
    assert!(repository
        .get_view("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .get_revision("wiki-guarded", &revision.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_dependency_indexes("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_empty());
    assert!(SystemEventRepository::new(client)
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10
        )
        .await
        .unwrap()
        .is_empty());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn input_policy_and_unknown_metadata_cannot_be_laundered(embedded: bool) {
    let (client, _, mut view, mut revision) = fixture(embedded).await;
    let repository = KnowledgeViewRepository::new(client.clone());
    view.base.security.policy_id = "unrelated-policy".into();
    revision.base.security.policy_id = "unrelated-policy".into();
    let observed = CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap();
    assert!(repository
        .publish_at_epoch(view.clone(), revision.clone(), &observed)
        .await
        .is_err());
    view.base.security.policy_id = "default".into();
    revision.base.security.policy_id = "default".into();
    revision.dependencies.push(KnowledgeDependency {
        record_id: "made-up:metadata".into(),
        record_type: "model".into(),
        role: "article_writer".into(),
        observed_updated_at: None,
    });
    assert!(repository
        .publish_at_epoch(view.clone(), revision, &observed)
        .await
        .is_err());
    assert!(repository
        .get_view("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_none());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn revision_progression_cannot_overwrite_current(embedded: bool) {
    let (client, _, view, revision) = fixture(embedded).await;
    let repository = KnowledgeViewRepository::new(client.clone());
    repository
        .publish(view.clone(), revision.clone())
        .await
        .unwrap();
    let mut competing = revision.clone();
    competing.base.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    let mut competing_view = view.clone();
    competing_view.current_revision = Some(competing.base.id.to_string());
    let observed = CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap();
    assert!(repository
        .publish_at_epoch(competing_view, competing.clone(), &observed)
        .await
        .is_err());
    assert_eq!(
        repository
            .get_view("wiki-guarded", &view.base.id)
            .await
            .unwrap(),
        Some(view)
    );
    assert!(repository
        .get_revision("wiki-guarded", &competing.base.id)
        .await
        .unwrap()
        .is_none());
}

macro_rules! both_backends {
    ($embedded:ident, $surreal:ident, $body:ident) => {
        #[tokio::test]
        async fn $embedded() {
            $body(true).await;
        }
        #[tokio::test]
        async fn $surreal() {
            $body(false).await;
        }
    };
}
both_backends!(
    embedded_wiki_atomic_receipt,
    surreal_wiki_atomic_receipt,
    publish_independent_and_keep_atomic_receipt
);
both_backends!(
    embedded_wiki_stale_epoch,
    surreal_wiki_stale_epoch,
    stale_preparation_rejects_same_timestamp_reclassification
);
both_backends!(
    embedded_wiki_input_authority,
    surreal_wiki_input_authority,
    input_policy_and_unknown_metadata_cannot_be_laundered
);
both_backends!(
    embedded_wiki_revision_cas,
    surreal_wiki_revision_cas,
    revision_progression_cannot_overwrite_current
);

#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep canonical, publication, repository in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn source_retract_during_preparation_refuses_stale_wiki(embedded: bool) {
    use crate::source_publication::{
        AcceptSourceRevision, SourceMutation, SourcePublicationRepository,
    };
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::{Source, SourceType};

    let (client, root, view, mut revision) = fixture(embedded).await;
    let source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "wiki-guarded".into(),
            "author".into(),
        ),
        SourceType::Manual,
        "source-content-hash".into(),
    );
    crate::SourceRepository::new(client.clone())
        .create(source.clone())
        .await
        .unwrap();
    revision.dependencies.push(KnowledgeDependency {
        record_id: source.base.id.to_string(),
        record_type: "source".into(),
        role: "editorial_evidence".into(),
        observed_updated_at: Some(source.base.updated_at),
    });
    revision.citations.push(KnowledgeCitation {
        id: "source-citation".into(),
        kind: CitationKind::GraphRecord,
        record_id: source.base.id.to_string(),
        assertion_id: None,
        source_id: Some(source.base.id.to_string()),
        evidence_span: None,
    });
    let canonical = CanonicalRepository::new(client.clone());
    let before_model = canonical.epoch("wiki-guarded").await.unwrap();
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "retract-wiki-input-v1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "accept-wiki-input".into(),
            request_fingerprint: "wiki-input-v1".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: publication
                .authorization_snapshot("wiki-guarded", &source.base.id)
                .await
                .unwrap(),
            authorized_document: None,
        })
        .await
        .unwrap();
    publication
        .retract(SourceMutation {
            space_id: "wiki-guarded".into(),
            source_id: source.base.id.clone(),
            expected: SourcePrecondition::Generation(accepted.source_generation),
            idempotency_key: "retract-wiki-input".into(),
            request_fingerprint: "retract-wiki-input-v1".into(),
            actor: "test".into(),
            authorized: publication
                .authorization_snapshot("wiki-guarded", &source.base.id)
                .await
                .unwrap()
                .unwrap(),
            authorized_document: None,
        })
        .await
        .unwrap();

    let repository = KnowledgeViewRepository::new(client.clone());
    assert!(repository
        .publish_at_epoch(view.clone(), revision.clone(), &before_model)
        .await
        .is_err());
    // Refreshing just the epoch cannot make a stale dependency current.
    let current = canonical.epoch("wiki-guarded").await.unwrap();
    assert!(repository
        .publish_at_epoch(view.clone(), revision.clone(), &current)
        .await
        .is_err());
    assert!(repository
        .get_view("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .get_revision("wiki-guarded", &revision.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_dependency_indexes("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_empty());
    assert!(SystemEventRepository::new(client.clone())
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        EntityRepository::new(client)
            .get("wiki-guarded", &root.base.id)
            .await
            .unwrap(),
        Some(root)
    );
}
both_backends!(
    embedded_wiki_source_retract_race,
    surreal_wiki_source_retract_race,
    source_retract_during_preparation_refuses_stale_wiki
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn successive_wiki_publication_allocates_distinct_sequences(embedded: bool) {
    let (client, _, mut view, revision) = fixture(embedded).await;
    let repository = KnowledgeViewRepository::new(client.clone());
    repository
        .publish(view.clone(), revision.clone())
        .await
        .unwrap();
    let mut successor = revision.clone();
    successor.base.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    successor.revision = 2;
    successor.supersedes = Some(revision.base.id.to_string());
    view.revision_count = 2;
    view.current_revision = Some(successor.base.id.to_string());
    repository
        .publish(view.clone(), successor.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.get_current_revision(&view).await.unwrap(),
        Some(successor)
    );
    assert_eq!(
        repository
            .get_revision("wiki-guarded", &revision.base.id)
            .await
            .unwrap(),
        Some(revision)
    );
    let mut events = SystemEventRepository::new(client)
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    events.sort_by_key(|event| event.seq);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].seq, events[0].seq + 1);
}
both_backends!(
    embedded_wiki_successive_publication_sequence,
    surreal_wiki_successive_publication_sequence,
    successive_wiki_publication_allocates_distinct_sequences
);

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn typed_root_subject_cannot_be_omitted_from_published_envelope() {
    for embedded in [true, false] {
        let (client, _, mut view, mut revision) = fixture(embedded).await;
        view.base.subjects.clear();
        revision.base.subjects.clear();
        let repository = KnowledgeViewRepository::new(client.clone());
        let epoch = CanonicalRepository::new(client.clone())
            .epoch("wiki-guarded")
            .await
            .unwrap();
        let result = repository
            .publish_at_epoch(view.clone(), revision.clone(), &epoch)
            .await;
        assert!(
            matches!(result, Err(crate::StorageError::Conflict(ref code))
            if code == "knowledge_view_dependency_not_current")
        );
        assert!(repository
            .get_view("wiki-guarded", &view.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(repository
            .get_revision("wiki-guarded", &revision.base.id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            CanonicalRepository::new(client)
                .epoch("wiki-guarded")
                .await
                .unwrap(),
            epoch
        );
    }
}

#[path = "knowledge_view_authority_tests.rs"]
mod authority_tests;

#[path = "knowledge_view_batch_tests.rs"]
mod batch_tests;
