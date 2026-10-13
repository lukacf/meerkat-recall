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
async fn scenario_s5_11_space_management() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let create = dispatcher
        .execute(
            "create_space",
            serde_json::json!({
                "name": "default",
                "config": {
                    "embedding_provider": "fake",
                    "embedding_model": "test-model",
                    "embedding_dims": 3
                }
            }),
        )
        .await?;
    let created: ToolResponse<serde_json::Value> = serde_json::from_value(create)?;
    assert!(created.ok, "create_space should succeed");

    let listed = dispatcher
        .execute("list_spaces", serde_json::json!({ "limit": 1000 }))
        .await?;
    let listed_resp: ToolResponse<serde_json::Value> = serde_json::from_value(listed)?;
    assert!(listed_resp.ok, "list_spaces should succeed");
    let items = listed_resp
        .data
        .as_ref()
        .and_then(|v| v.get("items"))
        .and_then(|v| v.as_array())
        .expect("items array");
    assert!(
        items
            .iter()
            .any(|v| v.get("space_id").and_then(|s| s.as_str()) == Some("default")),
        "expected default to appear in list_spaces"
    );

    let get = dispatcher
        .execute("get_space", serde_json::json!({ "space_id": "default" }))
        .await?;
    let get_resp: ToolResponse<serde_json::Value> = serde_json::from_value(get)?;
    assert!(get_resp.ok, "get_space should succeed");
    let embedding_provider = get_resp
        .data
        .as_ref()
        .and_then(|v| v.get("embedding_provider"))
        .and_then(|v| v.as_str())
        .expect("embedding_provider");
    assert_eq!(embedding_provider, "fake");

    let update = dispatcher
        .execute(
            "update_space",
            serde_json::json!({
                "space_id": "default",
                "config": { "embedding_provider": "openai" }
            }),
        )
        .await?;
    let update_resp: ToolResponse<serde_json::Value> = serde_json::from_value(update)?;
    assert!(update_resp.ok, "update_space should succeed");

    let get2 = dispatcher
        .execute("get_space", serde_json::json!({ "space_id": "default" }))
        .await?;
    let get2_resp: ToolResponse<serde_json::Value> = serde_json::from_value(get2)?;
    assert!(get2_resp.ok, "get_space should succeed after update");
    let provider2 = get2_resp
        .data
        .as_ref()
        .and_then(|v| v.get("embedding_provider"))
        .and_then(|v| v.as_str())
        .expect("embedding_provider");
    assert_eq!(provider2, "openai");

    Ok(())
}
