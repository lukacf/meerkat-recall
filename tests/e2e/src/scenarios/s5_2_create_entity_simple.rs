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
async fn scenario_s5_2_create_entity_simple() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let params = serde_json::json!({
        "entity_type": "person",
        "display_name": "Alex Chen",
        "summary": "Scenario S5.2"
    });

    let response = dispatcher.execute("create_entity_simple", params).await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;

    assert!(
        decoded.ok,
        "expected create_entity_simple ok=true, got error={:?} details={:?}",
        decoded.error, decoded.details
    );
    assert!(decoded.data.is_some(), "expected response data");

    Ok(())
}
