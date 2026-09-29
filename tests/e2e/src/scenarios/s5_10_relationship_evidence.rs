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
async fn scenario_s5_10_relationship_evidence() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let a = dispatcher
        .execute(
            "create_entity_simple",
            serde_json::json!({
                "entity_type": "person",
                "display_name": "Alice",
                "summary": "S5.10"
            }),
        )
        .await?;
    let a_resp: ToolResponse<serde_json::Value> = serde_json::from_value(a)?;
    assert!(a_resp.ok);
    let a_id = a_resp
        .data
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_str())
        .expect("Alice id")
        .to_string();

    let b = dispatcher
        .execute(
            "create_entity_simple",
            serde_json::json!({
                "entity_type": "org",
                "display_name": "Acme Corp",
                "summary": "S5.10"
            }),
        )
        .await?;
    let b_resp: ToolResponse<serde_json::Value> = serde_json::from_value(b)?;
    assert!(b_resp.ok);
    let b_id = b_resp
        .data
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_str())
        .expect("Acme id")
        .to_string();

    let src = dispatcher
        .execute(
            "create_source_simple",
            serde_json::json!({
                "name": "S5.10 Evidence Source",
                "uri": "file:///tmp/s5_10.txt"
            }),
        )
        .await?;
    let src_resp: ToolResponse<serde_json::Value> = serde_json::from_value(src)?;
    assert!(src_resp.ok);
    let source_id = src_resp
        .data
        .as_ref()
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_str())
        .expect("source id")
        .to_string();
    let source_uuid = source_id
        .split(':')
        .next_back()
        .unwrap_or(source_id.as_str())
        .to_string();

    let evidence_text = "Alice works at Acme Corp.";
    let rel = dispatcher
        .execute(
            "relate_entities",
            serde_json::json!({
                "from_id": a_id,
                "to_id": b_id,
                "rel_type": "works_at",
                "evidence_text": evidence_text,
                "evidence_source": source_id,
            }),
        )
        .await?;
    let rel_resp: ToolResponse<serde_json::Value> = serde_json::from_value(rel)?;
    assert!(rel_resp.ok, "relate_entities should succeed with evidence");

    let listed = dispatcher
        .execute(
            "list_assertions",
            serde_json::json!({
                "source_id": source_uuid,
                "predicate": "works_at",
                "limit": 50
            }),
        )
        .await?;
    let list_resp: ToolResponse<serde_json::Value> = serde_json::from_value(listed)?;
    assert!(list_resp.ok, "list_assertions should succeed");

    let data = list_resp.data.expect("data");
    let items = data.get("items").and_then(|v| v.as_array()).expect("items");
    assert!(
        !items.is_empty(),
        "expected at least one assertion created from relate_entities evidence"
    );

    let evidence = items[0]
        .get("evidence")
        .and_then(|v| v.as_object())
        .expect("evidence object");
    let quote_hash = evidence
        .get("quote_hash")
        .and_then(|v| v.as_str())
        .expect("quote_hash");
    assert!(
        quote_hash.starts_with("sha256:"),
        "expected sha256: quote hash, got {quote_hash}"
    );

    Ok(())
}
