#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use elephant_storage::{
    MockEntityRepository, MockRelRepository, Repository, SearchCriteria, SearchRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

#[tokio::test]
async fn scenario_fast_entity_lifecycle() -> anyhow::Result<()> {
    let repo = MockEntityRepository::empty();
    let space_id = "Space_X";

    // 1. Create
    let id = RecordId::new_v4(TableName::Entity);
    let mut base = BaseFields::new(id.clone(), space_id.to_string(), "P_sys".to_string());
    base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    let entity = Entity::new(base, EntityType::new("person"), "Entity 1".to_string());
    repo.create(entity).await?;

    // 2. Retrieve
    let retrieved = repo.get(space_id, &id).await?;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().display_name, "Entity 1");

    // 3. Soft-delete
    repo.delete(space_id, &id).await?;

    // 4. Verify Not Reachable via get
    let retrieved = repo.get(space_id, &id).await?;
    assert!(
        retrieved.is_none(),
        "CRITICAL: Soft-deleted record reachable via standard get!"
    );

    // 5. Reachable via get_with_deleted
    let retrieved_deleted = repo.get_with_deleted(space_id, &id).await?;
    assert!(retrieved_deleted.is_some());
    assert!(retrieved_deleted.unwrap().base.deleted_at.is_some());

    // 6. Restore
    repo.restore(space_id, &id).await?;
    let restored = repo.get(space_id, &id).await?;
    assert!(restored.is_some());
    assert!(restored.unwrap().base.deleted_at.is_none());

    // 7. Verify Uniqueness (Create same ID should fail)
    let mut base_dup = BaseFields::new(id.clone(), space_id.to_string(), "P_sys".to_string());
    base_dup.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let entity_dup = Entity::new(
        base_dup,
        EntityType::new("person"),
        "Entity Duplicate".to_string(),
    );
    let result = repo.create(entity_dup).await;
    assert!(result.is_err());

    // 8. Search
    let criteria = SearchCriteria::new()
        .with_space_id(space_id)
        .with_query("Entity");
    let search_results = repo.search(&criteria).await?;
    assert!(!search_results.results.is_empty());

    Ok(())
}

#[tokio::test]
async fn scenario_fast_rel_crud() -> anyhow::Result<()> {
    let entity_repo = MockEntityRepository::empty();
    let rel_repo = MockRelRepository::empty();
    let space_id = "Space_X";

    // Create Entity A
    let id_a = RecordId::new_v4(TableName::Entity);
    let base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    entity_repo
        .create(Entity::new(
            base_a,
            EntityType::new("person"),
            "Entity A".to_string(),
        ))
        .await?;

    // Create Entity B
    let id_b = RecordId::new_v4(TableName::Entity);
    let base_b = BaseFields::new(id_b.clone(), space_id.to_string(), "P_sys".to_string());
    entity_repo
        .create(Entity::new(
            base_b,
            EntityType::new("person"),
            "Entity B".to_string(),
        ))
        .await?;

    // Create Relationship A -> B
    let id_rel = RecordId::new_v4(TableName::Rel);
    let mut base_rel = BaseFields::new(id_rel.clone(), space_id.to_string(), "P_sys".to_string());
    base_rel.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_rel.provenance = Provenance::default();
    base_rel.subjects_state = SubjectsState::Complete;

    let rel = Rel {
        base: base_rel,
        subject: Link::new(id_a.clone()),
        object: Link::new(id_b.clone()),
        rel_type: "knows".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 1.0,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    rel_repo.create(rel).await?;

    // Retrieve
    let retrieved = rel_repo.get(space_id, &id_rel).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.rel_type, "knows");
    assert_eq!(retrieved.subject.id(), &id_a);
    assert_eq!(retrieved.object.id(), &id_b);

    // Soft delete
    rel_repo.delete(space_id, &id_rel).await?;
    assert!(rel_repo.get(space_id, &id_rel).await?.is_none());

    // Restore
    rel_repo.restore(space_id, &id_rel).await?;
    assert!(rel_repo.get(space_id, &id_rel).await?.is_some());

    // Search
    let criteria = SearchCriteria::new()
        .with_space_id(space_id)
        .with_query("kno");
    let search_results = rel_repo.search(&criteria).await?;
    assert!(!search_results.results.is_empty());

    Ok(())
}
