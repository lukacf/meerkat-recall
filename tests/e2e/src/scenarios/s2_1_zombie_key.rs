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
use elephant_storage::{EntityIdentityKeyRepository, EntityRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::{EntityStatus, EntityType};
use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
use elephant_types::records::Entity;
use elephant_types::Link;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s2_1_zombie_key() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let key_repo = EntityIdentityKeyRepository::new(harness.client.clone());

    let space_id = "Space_X";

    // 1. Create Entity A with Key X
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        labels: vec![],
        handling: vec![],
    };
    base_a.subjects_state = SubjectsState::Complete;
    let mut entity_a = Entity::new(base_a, EntityType::new("person"), "Entity A".to_string());
    entity_a.identity_keys = vec!["email:a@example.com".to_string()];
    entity_repo.create(entity_a).await?;

    // SoT for Key X
    let mut key_x_base = BaseFields::new(
        RecordId::new_v4(TableName::EntityIdentityKey),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    key_x_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        labels: vec![],
        handling: vec![],
    };
    let key_x = EntityIdentityKey {
        base: key_x_base,
        key: "email:a@example.com".to_string(),
        entity: Link::new(id_a.clone()),
        confidence: 1.0,
        first_seen: None,
        last_seen: None,
        evidence: vec![],
        status: IdentityKeyStatus::Active,
    };
    key_repo.create(key_x).await?;

    // 2. Create Entity B with Key Y
    let id_b = RecordId::new_v4(TableName::Entity);
    let mut base_b = BaseFields::new(id_b.clone(), space_id.to_string(), "P_sys".to_string());
    base_b.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        labels: vec![],
        handling: vec![],
    };
    base_b.subjects_state = SubjectsState::Complete;
    let mut entity_b = Entity::new(base_b, EntityType::new("person"), "Entity B".to_string());
    entity_b.identity_keys = vec!["email:b@example.com".to_string()];
    entity_repo.create(entity_b).await?;

    // 3. Merge A into B (Using correct transactional logic)
    let mut survivor = entity_repo.get(space_id, &id_b).await?.unwrap();
    let merged_entity = entity_repo.get(space_id, &id_a).await?.unwrap();

    // Update survivor cache
    survivor
        .identity_keys
        .extend(merged_entity.identity_keys.iter().cloned());
    survivor.identity_keys.sort();
    survivor.identity_keys.dedup();
    entity_repo.update(space_id, &id_b, survivor).await?;

    // Mark A as merged
    let mut tombstone = merged_entity.clone();
    tombstone.status = EntityStatus::Merged;
    tombstone.redirect_to = Some(Link::new(id_b.clone()));
    entity_repo.update(space_id, &id_a, tombstone).await?;

    // REPOINT IN TRANSACTION (The missing part in old API §11.5)
    key_repo.repoint_entity(&id_a, &id_b).await?;

    // 4. Verify Invariant: Key X must point to B
    let resolved_key_x = key_repo
        .find_by_key(space_id, "email:a@example.com")
        .await?;
    assert!(resolved_key_x.is_some(), "Key X should still exist");
    let key_rec = resolved_key_x.unwrap();

    assert_eq!(
        key_rec.entity.id().to_string(),
        id_b.to_string(),
        "CRITICAL: Key X still points to Entity A instead of survivor B!"
    );

    Ok(())
}
