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
use elephant_storage::{EntityRepository, Repository, SearchCriteria, SearchRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s1_1_entity_lifecycle() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let repo = EntityRepository::new(harness.client.clone());

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
