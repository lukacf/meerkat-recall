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
async fn scenario_s5_12_batch_progress() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let response = dispatcher
        .execute(
            "batch_ingest_docs",
            serde_json::json!({
                "docs": [
                    { "title": "S5.12 Doc A", "text": "Hello A" },
                    { "title": "S5.12 Doc B", "text": "Hello B" }
                ]
            }),
        )
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "batch_ingest_docs should succeed");

    let data = decoded.data.expect("data");
    let job_id = data
        .get("job_id")
        .and_then(|v| v.as_str())
        .expect("job_id")
        .to_string();
    assert!(
        job_id.starts_with("work_item:"),
        "expected work_item:* job_id, got {job_id}"
    );

    let status = dispatcher
        .execute("get_job_status", serde_json::json!({ "job_id": job_id }))
        .await?;
    let status_resp: ToolResponse<serde_json::Value> = serde_json::from_value(status)?;
    assert!(status_resp.ok, "get_job_status should succeed");

    let status_data = status_resp.data.expect("data");
    let payload = status_data
        .get("payload")
        .and_then(|v| v.as_object())
        .expect("payload object");
    assert_eq!(payload.get("total").and_then(|v| v.as_u64()), Some(2));

    Ok(())
}
