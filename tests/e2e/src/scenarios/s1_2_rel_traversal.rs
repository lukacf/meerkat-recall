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
use elephant_storage::{EntityRepository, RelRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::Entity;
use elephant_types::Link;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s1_2_ghost_edges() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let rel_repo = RelRepository::new(harness.client.clone());

    let space_id = "Space_X";

    // 1. Create Entity A
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_a.provenance = Provenance::default();
    base_a.subjects_state = SubjectsState::Complete;
    entity_repo
        .create(Entity::new(
            base_a,
            EntityType::new("person"),
            "Entity A".to_string(),
        ))
        .await?;

    // 2. Create Entity B
    let id_b = RecordId::new_v4(TableName::Entity);
    let mut base_b = BaseFields::new(id_b.clone(), space_id.to_string(), "P_sys".to_string());
    base_b.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_b.provenance = Provenance::default();
    base_b.subjects_state = SubjectsState::Complete;
    entity_repo
        .create(Entity::new(
            base_b,
            EntityType::new("person"),
            "Entity B".to_string(),
        ))
        .await?;

    // 3. Create Relationship A -> B
    let id_rel = RecordId::new_v4(TableName::Rel);
    let mut base_rel = BaseFields::new(id_rel.clone(), space_id.to_string(), "P_sys".to_string());
    base_rel.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };

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

    // 4. Verify Traversal Works
    let context = entity_repo
        .get_with_relations(space_id, &id_a)
        .await?
        .unwrap();
    assert_eq!(context.outgoing_relations.len(), 1);

    // 5. Soft-delete Entity B
    entity_repo.delete(space_id, &id_b).await?;

    // 6. Verify Invariant: Relationship must be hidden from traversal (Section 7.1)
    let context_after = entity_repo
        .get_with_relations(space_id, &id_a)
        .await?
        .unwrap();
    assert_eq!(
        context_after.outgoing_relations.len(),
        0,
        "CRITICAL: Ghost Edge! Traversal returned relationship to a soft-deleted entity."
    );

    Ok(())
}
