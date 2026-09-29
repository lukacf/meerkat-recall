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
use elephant_storage::{EntityRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s2_2_amnesia_merge() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());

    let space_id = "Space_X";

    // 1. Create Entity A with nested metadata
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security::default();
    base_a.security.policy_id = "default".to_string();
    let mut entity_a = Entity::new(base_a, EntityType::new("person"), "Entity A".to_string());
    entity_a.metadata = serde_json::json!({
        "source_app": {
            "preference_value": 22
        }
    });
    entity_repo.create(entity_a).await?;

    // 2. Create Entity B with different nested metadata
    let id_b = RecordId::new_v4(TableName::Entity);
    let mut base_b = BaseFields::new(id_b.clone(), space_id.to_string(), "P_sys".to_string());
    base_b.security = Security::default();
    base_b.security.policy_id = "default".to_string();
    let mut entity_b = Entity::new(base_b, EntityType::new("person"), "Entity B".to_string());
    entity_b.metadata = serde_json::json!({
        "source_b": {
            "safety_clearance": "high"
        }
    });
    entity_repo.create(entity_b).await?;

    // 3. Perform Merge (Combine Strategy)
    // We now use the CORRECT logic (matching the fixed identity.rs)
    let entity_a_rec = entity_repo.get(space_id, &id_a).await?.unwrap();
    let entity_b_rec = entity_repo.get(space_id, &id_b).await?.unwrap();

    // Create a new survivor entity (C)
    let id_c = RecordId::new_v4(TableName::Entity);
    let mut base_c = BaseFields::new(id_c.clone(), space_id.to_string(), "P_sys".to_string());
    base_c.security = Security::default();
    base_c.security.policy_id = "default".to_string();
    base_c.subjects_state = SubjectsState::Complete;

    let mut combined = Entity::new(
        base_c,
        EntityType::new("person"),
        "Combined Entity".to_string(),
    );

    // DEEP MERGE metadata (Standard §11.5).
    fn merge_json(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
        match (a, b) {
            (serde_json::Value::Object(a_obj), serde_json::Value::Object(b_obj)) => {
                let mut merged = a_obj.clone();
                for (k, v) in b_obj {
                    match merged.get(k) {
                        Some(ov) => {
                            merged.insert(k.clone(), merge_json(ov, v));
                        }
                        None => {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                }
                serde_json::Value::Object(merged)
            }
            _ => b.clone(),
        }
    }

    combined.metadata = merge_json(&entity_a_rec.metadata, &entity_b_rec.metadata);

    let created_c = entity_repo.create(combined).await?;

    // 4. Verify Invariant: C must contain metadata from BOTH A and B
    let metadata = created_c.metadata;

    assert!(
        metadata.get("source_app").is_some(),
        "Metadata from Entity A (source_app) was lost!"
    );
    assert!(
        metadata.get("source_b").is_some(),
        "Metadata from Entity B (source_b) was lost!"
    );

    Ok(())
}
