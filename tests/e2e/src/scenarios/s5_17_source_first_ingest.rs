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
use elephant_storage::{DocRepository, Repository, SourceRepository, WorkItemRepository};
use elephant_types::ids::TableName;
use elephant_types::records::doc::Doc;
use elephant_types::records::SourceLifecycleStatus;
use elephant_types::{BaseFieldsBuilder, RecordId};

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
async fn scenario_s5_17_source_first_ingest() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));
    let doc = Doc {
        base: BaseFieldsBuilder::new("default")
            .for_table(TableName::Doc)
            .build(),
        title: "Launch Notes".to_string(),
        text: "Alice announced Project Phoenix at 09:00.".to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: "source-first-rk".to_string(),
        metadata: serde_json::json!({ "collection": "default" }),
    };

    let response = dispatcher
        .execute(
            "ingest_source_simple",
            serde_json::json!({
                "name": "Quarterly launch notes",
                "uri": "https://example.com/launch-notes",
                "ingest_mode": "graph",
                "doc": doc
            }),
        )
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "expected ingest_source_simple ok=true");
    let output = decoded.data.expect("response data");

    let source_id = output
        .get("source_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("source_id missing"))?;
    let doc_id = output
        .get("doc_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("doc_id missing"))?;
    let work_item_id = output
        .get("work_item_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("work_item_id missing"))?;

    let source_repo = SourceRepository::new(harness.client.clone());
    let doc_repo = DocRepository::new(harness.client.clone());
    let work_item_repo = WorkItemRepository::new(harness.client.clone());

    let source = source_repo
        .get("default", &RecordId::from_str(source_id)?)
        .await?
        .ok_or_else(|| anyhow::anyhow!("persisted source missing"))?;
    assert_eq!(source.lifecycle_status, SourceLifecycleStatus::Discovered);
    assert!(source.manual_doc_template()?.is_some());

    let doc = doc_repo
        .get("default", &RecordId::from_str(doc_id)?)
        .await?
        .ok_or_else(|| anyhow::anyhow!("persisted doc missing"))?;
    assert_eq!(
        doc.source.as_ref().map(|link| link.id().to_string()),
        Some(source.base.id.to_string())
    );

    let triage_item = work_item_repo
        .get("default", &RecordId::from_str(work_item_id)?)
        .await?
        .ok_or_else(|| anyhow::anyhow!("triage work item missing"))?;
    assert_eq!(triage_item.kind, TRIAGE_SOURCE_KIND);

    Ok(())
}
