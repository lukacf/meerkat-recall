//! Pipeline status is checked after real source completion, without test queue writes.
use elephant_mcp::create_dispatcher;

use crate::grounded_source_fixture::{principal, reextract_employment_via_mcp};
use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s5_6_pipeline_status() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space = "pipeline-status";
    let (doc_id, revision_id) = reextract_employment_via_mcp(&harness.client, space).await?;
    let dispatcher = create_dispatcher(harness.client.clone(), principal(space));
    let response = dispatcher
        .execute(
            "get_doc_pipeline_status",
            serde_json::json!({ "id": doc_id.to_string() }),
        )
        .await?;
    anyhow::ensure!(response["ok"] == true, "status failed: {response}");
    let status = &response["data"];
    anyhow::ensure!(
        status["revision"].as_str() == Some(revision_id.to_string().as_str()),
        "status must name exact completed revision"
    );
    anyhow::ensure!(
        status["nodes_count"]
            .as_u64()
            .is_some_and(|count| count > 0),
        "status omitted rendered nodes"
    );
    anyhow::ensure!(
        status["assertions_count"]
            .as_u64()
            .is_some_and(|count| count > 0),
        "status omitted canonical assertions"
    );
    Ok(())
}
