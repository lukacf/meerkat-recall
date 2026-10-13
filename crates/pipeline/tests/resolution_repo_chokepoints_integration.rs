#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use elephant_migrations::apply_schema;
use elephant_storage::surreal::thing_from_record_id;
use elephant_storage::{EntityRepository, RelRepository, Repository, StorageClient};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::Link;
use elephant_types::RecordId;

async fn setup_storage() -> StorageClient {
    let storage = StorageClient::new_memory().await.expect("storage");
    apply_schema(&storage).await.expect("schema");
    storage
}

fn make_entity(space_id: &str, name: &str) -> Entity {
    let id = RecordId::new_v4(TableName::Entity);
    let base = BaseFields::new(id, space_id.to_string(), "test".to_string());
    Entity::new(base, EntityType::new("person"), name.to_string())
}

fn make_rel(space_id: &str, subject: &RecordId, object: &RecordId, rel_type: &str) -> Rel {
    let id = RecordId::new_v4(TableName::Rel);
    let base = BaseFields::new(id, space_id.to_string(), "test".to_string());
    Rel {
        base,
        subject: Link::new(subject.clone()),
        object: Link::new(object.clone()),
        rel_type: rel_type.to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.9,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn test_resolution_bm25_search_entities_returns_hits() {
    let storage = setup_storage().await;
    let entity_repo = EntityRepository::new(storage);

    let mut alice = make_entity("test_space", "Alice");
    alice.aliases = vec!["Alice A.".to_string()];
    alice.summary = Some("Alice is a test user.".to_string());
    let alice = entity_repo.create(alice).await.expect("create Alice");

    let bob = make_entity("test_space", "Bob");
    let _bob = entity_repo.create(bob).await.expect("create Bob");

    let hits = entity_repo
        .bm25_search_entities("test_space", "Alice", 10)
        .await
        .expect("bm25_search_entities");

    assert!(!hits.is_empty(), "expected at least one BM25 hit");
    assert_eq!(hits[0].entity_id, alice.base.id);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn test_resolution_vector_search_entities_returns_hits() {
    let storage = setup_storage().await;
    let entity_repo = EntityRepository::new(storage.clone());

    let alice = entity_repo
        .create(make_entity("test_space", "Alice"))
        .await
        .expect("create Alice");
    let bob = entity_repo
        .create(make_entity("test_space", "Bob"))
        .await
        .expect("create Bob");

    let mut alice_embedding = vec![0.0_f32; 3072];
    alice_embedding[0] = 1.0;
    let mut bob_embedding = vec![0.0_f32; 3072];
    bob_embedding[0] = -1.0;

    storage
        .query_bind(
            "UPDATE $id SET name_embedding = $embedding",
            elephant_storage::bindings! {
                "id" => thing_from_record_id(&alice.base.id),
                "embedding" => alice_embedding.clone(),
            },
        )
        .await
        .expect("update Alice embedding");

    storage
        .query_bind(
            "UPDATE $id SET name_embedding = $embedding",
            elephant_storage::bindings! {
                "id" => thing_from_record_id(&bob.base.id),
                "embedding" => bob_embedding,
            },
        )
        .await
        .expect("update Bob embedding");

    let query_embedding = alice_embedding;
    let hits = entity_repo
        .vector_search_entities("test_space", &query_embedding, 10)
        .await
        .expect("vector_search_entities");

    assert!(!hits.is_empty(), "expected at least one vector hit");
    assert_eq!(hits[0].entity_id, alice.base.id);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn test_resolution_get_entities_within_n_hops_traverses_both_directions() {
    let storage = setup_storage().await;
    let entity_repo = EntityRepository::new(storage.clone());
    let rel_repo = RelRepository::new(storage);

    let a = entity_repo
        .create(make_entity("test_space", "A"))
        .await
        .expect("create A");
    let b = entity_repo
        .create(make_entity("test_space", "B"))
        .await
        .expect("create B");
    let c = entity_repo
        .create(make_entity("test_space", "C"))
        .await
        .expect("create C");
    let d = entity_repo
        .create(make_entity("test_space", "D"))
        .await
        .expect("create D");

    // A -> B -> C (outgoing)
    rel_repo
        .create(make_rel("test_space", &a.base.id, &b.base.id, "knows"))
        .await
        .expect("create rel A->B");
    rel_repo
        .create(make_rel("test_space", &b.base.id, &c.base.id, "works_at"))
        .await
        .expect("create rel B->C");

    // D -> A (incoming to A)
    rel_repo
        .create(make_rel("test_space", &d.base.id, &a.base.id, "reports_to"))
        .await
        .expect("create rel D->A");

    let neighbors = rel_repo
        .get_entities_within_n_hops("test_space", &a.base.id, 2)
        .await
        .expect("get_entities_within_n_hops");

    let mut distance_1: Vec<(RecordId, String)> = neighbors
        .iter()
        .filter(|n| n.distance == 1)
        .map(|n| (n.entity_id.clone(), n.rel_type.clone()))
        .collect();
    distance_1.sort_by(|(a, _), (b, _)| a.to_string().cmp(&b.to_string()));

    assert_eq!(distance_1.len(), 2, "expected 2 neighbors at distance 1");
    assert!(distance_1.contains(&(b.base.id.clone(), "knows".to_string())));
    assert!(distance_1.contains(&(d.base.id.clone(), "reports_to".to_string())));

    let distance_2: Vec<RecordId> = neighbors
        .iter()
        .filter(|n| n.distance == 2)
        .map(|n| n.entity_id.clone())
        .collect();
    assert_eq!(distance_2, vec![c.base.id.clone()]);
}
