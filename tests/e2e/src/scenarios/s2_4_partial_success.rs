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
use elephant_storage::{EntityRepository, RelRepository, Repository, TransactionExt};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s2_4_partial_success_rollback() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());

    let space_id = "Space_X";

    // 0. Create entities
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_repo
        .create(Entity::new(
            base_a,
            EntityType::new("person"),
            "A".to_string(),
        ))
        .await?;

    let id_b = RecordId::new_v4(TableName::Entity);
    let mut base_b = BaseFields::new(id_b.clone(), space_id.to_string(), "P_sys".to_string());
    base_b.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_repo
        .create(Entity::new(
            base_b,
            EntityType::new("person"),
            "B".to_string(),
        ))
        .await?;

    // 1. Start transaction with two relationship creates sharing an idempotency key
    let mut tx = harness.client.transaction();

    let rel_id = RecordId::new_v4(TableName::Rel);
    tx = tx.add_statement(format!(
        "RELATE type::thing('entity', '{}')->rel->type::thing('entity', '{}') CONTENT {{
            id: type::thing('rel', '{}'),
            space_id: 'Space_X',
            created_at: time::now(),
            updated_at: time::now(),
            created_by: 'P_sys',
            security: {{ policy_id: 'default', level: 1, labels: [], handling: [] }},
            provenance: {{ sources: [], chain: [] }},
            subjects_state: 'complete',
            rel_type: 'knows',
            idempotency_key: 'idem_fail'
        }}",
        id_a.id(),
        id_b.id(),
        rel_id.id()
    ));

    // INTENTIONAL FAILURE: Duplicate idempotency key in same transaction
    tx = tx.add_statement(format!(
        "RELATE type::thing('entity', '{}')->rel->type::thing('entity', '{}') CONTENT {{
            id: type::thing('rel', '{}'),
            space_id: 'Space_X',
            created_at: time::now(),
            updated_at: time::now(),
            created_by: 'P_sys',
            security: {{ policy_id: 'default', level: 1, labels: [], handling: [] }},
            provenance: {{ sources: [], chain: [] }},
            subjects_state: 'complete',
            rel_type: 'knows',
            idempotency_key: 'idem_fail'
        }}",
        id_a.id(),
        id_b.id(),
        RecordId::new_v4(TableName::Rel).id()
    ));

    // 2. Execute should fail
    let result = tx.execute().await;
    assert!(
        result.is_err(),
        "CRITICAL: Transaction should have failed due to duplicate idempotency key!"
    );

    Ok(())
}
