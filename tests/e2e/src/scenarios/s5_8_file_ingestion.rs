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
use std::str::FromStr;

use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::scheduler::ingest_work_kinds::TRIAGE_SOURCE_KIND;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{DocRepository, Repository, WorkItemRepository};
use elephant_types::RecordId;

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
async fn scenario_s5_8_file_ingestion() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let path = std::env::temp_dir().join(format!("elephant-s5-8-{}.md", uuid::Uuid::new_v4()));
    tokio::fs::write(&path, "# S5.8\n\nAlice works at Acme.\n").await?;

    let response = dispatcher
        .execute(
            "ingest_file",
            serde_json::json!({
                "path": path.to_string_lossy().to_string(),
                "source_type": "text/markdown"
            }),
        )
        .await?;

    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "ingest_file should succeed");

    let doc = decoded
        .data
        .as_ref()
        .and_then(|v| v.get("data"))
        .expect("doc envelope");
    let doc_id = RecordId::from_str(doc.get("id").and_then(|v| v.as_str()).expect("doc id"))?;
    let stored_doc = DocRepository::new(harness.client.clone())
        .get("default", &doc_id)
        .await?
        .expect("stored doc");
    let source_id = stored_doc
        .source
        .as_ref()
        .map(|link| link.id().to_string())
        .expect("source id");

    let repo = WorkItemRepository::new(harness.client.clone());
    let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
    let item = repo
        .find_by_idempotency_key("default", &idempotency_key)
        .await?;

    assert!(item.is_some(), "triage_source work item should be enqueued");

    Ok(())
}
