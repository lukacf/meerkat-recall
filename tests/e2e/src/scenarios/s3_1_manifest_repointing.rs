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
use serde::Deserialize;

use elephant_identity::resolver::generate_scoped_rewrite_query;
use elephant_identity::{generate_rewrite_query, ReferenceDeclaration};
use elephant_storage::surreal::record_id_from_thing;
use elephant_storage::{EntityRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use crate::Harness;

#[derive(Deserialize)]
struct ExtRow {
    owner: surrealdb::sql::Thing,
    secondary: Vec<surrealdb::sql::Thing>,
}

#[tokio::test]
#[ignore]
async fn scenario_s3_1_manifest_repointing() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let client = harness.client.clone();
    let repo = EntityRepository::new(client.clone());
    let space_id = "Space_X";

    client.query("DEFINE TABLE ext_table SCHEMALESS;").await?;

    // Create entities A and B.
    let id_a = RecordId::new_v4(TableName::Entity);
    let mut base_a = BaseFields::new(id_a.clone(), space_id.to_string(), "P_sys".to_string());
    base_a.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_a.provenance = Provenance::default();
    base_a.subjects_state = SubjectsState::Complete;
    repo.create(Entity::new(
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
    base_b.provenance = Provenance::default();
    base_b.subjects_state = SubjectsState::Complete;
    repo.create(Entity::new(
        base_b,
        EntityType::new("person"),
        "B".to_string(),
    ))
    .await?;

    let unrelated_id = RecordId::new_v4(TableName::Entity);
    // Keep an unrelated reference and collapse an existing survivor duplicate.
    let mut response = client
        .query_bind(
            r#"
            CREATE ext_table CONTENT {
                owner: type::thing('entity', $owner),
                secondary: [
                    type::thing('entity', $owner),
                    type::thing('entity', $secondary),
                    type::thing('entity', $unrelated)
                ]
            };
            "#,
            serde_json::json!({
                "owner": id_a.id().to_string(),
                "secondary": id_b.id().to_string(),
                "unrelated": unrelated_id.id().to_string()
            }),
        )
        .await?;
    let _: surrealdb::Value = response.take(0)?;

    // Rewrite references (A -> B).
    let owner_decl = ReferenceDeclaration::new("ext_table", "owner");
    let secondary_decl = ReferenceDeclaration::new("ext_table", "secondary").with_many();

    let owner_query = generate_rewrite_query(&owner_decl, &id_a, &id_b)?;
    client.query(&owner_query).await?.check()?;
    let secondary_query = generate_rewrite_query(&secondary_decl, &id_a, &id_b)?;
    client.query(&secondary_query).await?.check()?;

    // Verify record references updated.
    let mut response = client
        .query("SELECT owner, secondary FROM ext_table LIMIT 1")
        .await?;
    let rows: Vec<ExtRow> = response.take(0)?;
    let row = rows.first().expect("ext row");

    let owner_id = record_id_from_thing(&row.owner)?;
    assert_eq!(owner_id, id_b);

    let secondary_ids = row
        .secondary
        .iter()
        .map(record_id_from_thing)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(secondary_ids.len(), 2);
    assert!(secondary_ids.contains(&id_b));
    assert!(secondary_ids.contains(&unrelated_id));
    assert!(!secondary_ids.contains(&id_a));

    Ok(())
}

#[tokio::test]
#[ignore]
async fn scenario_s3_1_scoped_manifest_repointing_preserves_other_space() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let client = &harness.client;
    client
        .query("DEFINE TABLE ext_table SCHEMALESS;")
        .await?
        .check()?;
    let old_id = RecordId::new_v4(TableName::Entity);
    let survivor = RecordId::new_v4(TableName::Entity);
    client.query_bind(
        "CREATE ext_table:first SET space_id = 'first', owner = type::thing('entity', $old), secondary = [type::thing('entity', $old)]; CREATE ext_table:second SET space_id = 'second', owner = type::thing('entity', $old), secondary = [type::thing('entity', $old)];",
        serde_json::json!({"old": old_id.id().to_string()}),
    ).await?.check()?;
    for declaration in [
        ReferenceDeclaration::new("ext_table", "owner"),
        ReferenceDeclaration::new("ext_table", "secondary").with_many(),
    ] {
        let query = generate_scoped_rewrite_query(&declaration, "first", &old_id, &survivor)?;
        client.query(&query).await?.check()?;
    }
    for (space_id, expected) in [("first", &survivor), ("second", &old_id)] {
        let mut response = client
            .query_bind(
                "SELECT owner, secondary FROM ext_table WHERE space_id = $space",
                serde_json::json!({"space": space_id}),
            )
            .await?;
        let rows: Vec<ExtRow> = response.take(0)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(&record_id_from_thing(&rows[0].owner)?, expected);
        assert_eq!(rows[0].secondary.len(), 1);
        assert_eq!(&record_id_from_thing(&rows[0].secondary[0])?, expected);
    }

    Ok(())
}
