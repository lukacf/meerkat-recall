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
async fn scenario_s5_9_tool_introspection() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let response = dispatcher
        .execute(
            "get_tool_schema",
            serde_json::json!({ "tool_name": "ingest_doc" }),
        )
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "get_tool_schema should succeed");

    let data = decoded.data.expect("data");
    let schema = data
        .get("input_schema")
        .and_then(|v| v.as_object())
        .expect("input_schema object");
    let required = schema
        .get("required")
        .and_then(|v| v.as_array())
        .expect("required array");

    assert!(
        required.iter().any(|v| v.as_str() == Some("doc")),
        "expected ingest_doc schema.required to include 'doc'"
    );

    Ok(())
}
