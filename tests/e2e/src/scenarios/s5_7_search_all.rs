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

use std::collections::HashSet;

use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_policy::{Clearance, Principal};

use crate::Harness;

fn test_principal(space_id: &str) -> Principal {
    let mut scopes = HashSet::new();
    scopes.insert("*".to_string());

    Principal {
        space_id: space_id.to_string(),
        scopes,
        clearances: vec![Clearance {
            policy_id: "default".to_string(),
            max_level: i32::MAX,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

#[tokio::test]
async fn scenario_s5_7_search_all() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let create_entity = dispatcher
        .execute(
            "create_entity_simple",
            serde_json::json!({
                "entity_type": "person",
                "display_name": "Jane Smith",
                "summary": "Test entity"
            }),
        )
        .await?;
    let entity_resp: ToolResponse<serde_json::Value> = serde_json::from_value(create_entity)?;
    assert!(entity_resp.ok, "create_entity_simple should succeed");

    let ingest_doc = dispatcher
        .execute(
            "ingest_doc_simple",
            serde_json::json!({
                "title": "Acme Notes",
                "text": "Jane Smith works at Acme Corp."
            }),
        )
        .await?;
    let doc_resp: ToolResponse<serde_json::Value> = serde_json::from_value(ingest_doc)?;
    assert!(doc_resp.ok, "ingest_doc_simple should succeed");

    let response = dispatcher
        .execute(
            "search_all",
            serde_json::json!({
                "query": "Jane Acme",
                "limit": 10,
                "include": ["entities", "docs"]
            }),
        )
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "search_all should succeed");

    let data = decoded.data.expect("data");
    let entities = data
        .get("entities")
        .and_then(|b| b.get("items"))
        .and_then(|v| v.as_array())
        .expect("entities.items");
    let docs = data
        .get("docs")
        .and_then(|b| b.get("items"))
        .and_then(|v| v.as_array())
        .expect("docs.items");

    assert!(!entities.is_empty(), "expected non-empty entities bucket");
    assert!(!docs.is_empty(), "expected non-empty docs bucket");

    Ok(())
}
