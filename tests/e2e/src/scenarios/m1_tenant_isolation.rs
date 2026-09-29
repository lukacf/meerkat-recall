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
use elephant_types::records::base::{BaseFields, Security};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_m1_tenant_isolation_leaks() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let repo = EntityRepository::new(harness.client.clone());

    // 1. Create Entity in Space_X
    let id = RecordId::new_v4(TableName::Entity);
    let mut base_x = BaseFields::new(id.clone(), "Space_X".to_string(), "P_sys".to_string());
    base_x.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    repo.create(Entity::new(
        base_x,
        EntityType::new("person"),
        "Space X User".to_string(),
    ))
    .await?;

    // 2. Attempt to read using Space_Y context.
    // The repository MUST enforce that a record can only be retrieved if the space_id matches.
    let retrieved = repo.get("Space_Y", &id).await?;

    if let Some(entity) = retrieved {
        assert_eq!(
            entity.base.space_id, "Space_X",
            "Sanity check: recorded space_id is correct"
        );

        // This is the core failure condition: If I'm in Space_Y, I should NEVER see Space_X records.
        panic!("CRITICAL: Resource Leak! Retrieved a Space_X record from Space_Y context.");
    }

    // Also verify that it IS reachable from Space_X.
    let reachable = repo.get("Space_X", &id).await?;
    assert!(
        reachable.is_some(),
        "Record should be reachable from its own space"
    );

    Ok(())
}
