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
use elephant_types::records::entity::EntityType;
use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
use elephant_types::records::Entity;
use elephant_types::Link;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s1_4_identity_key_uniqueness() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let key_repo = EntityIdentityKeyRepository::new(harness.client.clone());

    let space_id = "Space_X";

    // 1. Create Entity A
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_a.subjects_state = SubjectsState::Complete;
    entity_repo
        .create(Entity::new(
            base_a,
            EntityType::new("person"),
            "Entity A".to_string(),
        ))
        .await?;

    // 2. Create Identity Key for A
    let key_val = "email:user@example.com";
    let mut key_base = BaseFields::new(
        RecordId::new_v4(TableName::EntityIdentityKey),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    key_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };

    let key = EntityIdentityKey {
        base: key_base,
        key: key_val.to_string(),
        entity: Link::new(id_a),
        confidence: 1.0,
        first_seen: None,
        last_seen: None,
        evidence: vec![],
        status: IdentityKeyStatus::Active,
    };
    key_repo.create(key).await?;

    // 3. Attempt to create SAME key in SAME space (Should Fail)
    let mut key_base_dup = BaseFields::new(
        RecordId::new_v4(TableName::EntityIdentityKey),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    key_base_dup.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };

    let key_dup = EntityIdentityKey {
        base: key_base_dup,
        key: key_val.to_string(),
        entity: Link::new(RecordId::new_v4(TableName::Entity)),
        confidence: 1.0,
        first_seen: None,
        last_seen: None,
        evidence: vec![],
        status: IdentityKeyStatus::Active,
    };

    let result = key_repo.create(key_dup).await;
    assert!(
        result.is_err(),
        "CRITICAL: Duplicate identity key allowed in same space!"
    );

    // 4. Attempt to create SAME key in DIFFERENT space (Should Succeed - Section 8.2 Index)
    let space_y = "Space_Y";
    let mut key_base_y = BaseFields::new(
        RecordId::new_v4(TableName::EntityIdentityKey),
        space_y.to_string(),
        "P_sys".to_string(),
    );
    key_base_y.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };

    let key_y = EntityIdentityKey {
        base: key_base_y,
        key: key_val.to_string(),
        entity: Link::new(RecordId::new_v4(TableName::Entity)),
        confidence: 1.0,
        first_seen: None,
        last_seen: None,
        evidence: vec![],
        status: IdentityKeyStatus::Active,
    };
    key_repo.create(key_y).await?;

    Ok(())
}
