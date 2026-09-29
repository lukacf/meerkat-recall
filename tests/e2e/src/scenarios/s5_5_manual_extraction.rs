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
use std::sync::Arc;

use chrono::Utc;

use elephant_embeddings::FakeEmbeddingProvider;
use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::extraction::DefaultExtractor;
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::extraction_work_kinds::ExtractionArtifactData;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_extract_assertions, execute_plan_extraction,
    promote_assertions_inline, work_kinds, DocTreeConfig, ExtractionPlanConfig,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, Repository, StorageClient,
    WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
use elephant_types::{Link, RecordId};

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

async fn create_work_run(
    storage: StorageClient,
    space_id: &str,
    item_id: &RecordId,
) -> anyhow::Result<RecordId> {
    let repo = WorkRunRepository::new(storage);
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let work_run = WorkRun {
        base: BaseFields::new(
            work_run_id.clone(),
            space_id.to_string(),
            "scenario".to_string(),
        ),
        work_item: Link::new(item_id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "scenario-worker".to_string(),
        worker_version: "test".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    repo.create(work_run).await?;
    Ok(work_run_id)
}

async fn drive_pipeline_for_revision(
    storage: StorageClient,
    target_revision: &RecordId,
    max_steps: usize,
) -> anyhow::Result<()> {
    let space_id = "default";
    let item_repo = WorkItemRepository::new(storage.clone());
    let _doc_repo = DocRepository::new(storage.clone());
    let _assertion_repo = AssertionRepository::new(storage.clone());
    let dag = elephant_pipeline::scheduler::DagExecutor::new(storage.clone());

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));

    let _embedder: Arc<dyn elephant_embeddings::EmbeddingProvider> =
        Arc::new(FakeEmbeddingProvider::new("test-model", 8));

    for _ in 0..max_steps {
        let runnable = item_repo
            .find_by_status(space_id, WorkItemStatus::Runnable)
            .await?;
        if runnable.is_empty() {
            break;
        }

        for item in runnable {
            let item_id = item.base.id.clone();
            let _ = item_repo
                .update_status(space_id, &item_id, WorkItemStatus::Running)
                .await;
            let work_run_id = create_work_run(storage.clone(), space_id, &item_id).await?;

            match item.kind.as_str() {
                work_kinds::TRIAGE_SOURCE_KIND => {
                    // The manual extract_doc path below creates and targets its own revision.
                    // Background source-first ingest work from ingest_doc_simple is not part of
                    // this scenario's assertion.
                }
                work_kinds::RENDER_DOC_KIND => {
                    // Ignore background render_doc work for this scenario.
                }
                work_kinds::PLAN_EXTRACTION_KIND => {
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("plan_extraction missing revision_id"))?;
                    let revision_id = RecordId::from_str(revision_id)?;
                    if &revision_id != target_revision {
                        item_repo
                            .update_status(space_id, &item_id, WorkItemStatus::Done)
                            .await?;
                        continue;
                    }
                    let policy_version = item
                        .payload
                        .get("policy_version")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");

                    let config = ExtractionPlanConfig::default();
                    let result = execute_plan_extraction(
                        &storage,
                        &revision_id,
                        policy_version,
                        &config,
                        &work_run_id,
                        space_id,
                        "scenario",
                    )
                    .await?;

                    for selection in result.plan.node_selections {
                        let extract_item = WorkItem {
                            base: BaseFields::new(
                                RecordId::new_v4(TableName::WorkItem),
                                space_id.to_string(),
                                "scenario".to_string(),
                            ),
                            kind: work_kinds::EXTRACT_ASSERTIONS_KIND.to_string(),
                            payload: serde_json::json!({
                                "node_id": selection.node_id.to_string(),
                                "revision_id": revision_id.to_string(),
                                "extractor_id": "elephant.extractor.default",
                                "allow_raw_quotes": false,
                                "estimated_tokens": selection.estimated_tokens,
                                "estimated_cost_euros": 0.0,
                                "ingest_mode": "graph",
                            }),
                            status: WorkItemStatus::Runnable,
                            priority: item.priority,
                            idempotency_key: format!(
                                "extract_assertions:{}:{}",
                                revision_id, selection.node_id
                            ),
                            attempts: 0,
                            lease_expires_at: None,
                            leased_by: None,
                            run_after: None,
                            last_error: None,
                        };
                        let _ = item_repo.create(extract_item).await?;
                    }
                }
                work_kinds::EXTRACT_ASSERTIONS_KIND => {
                    let node_id = item
                        .payload
                        .get("node_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing node_id"))?;
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing revision_id"))?;
                    let node_id = RecordId::from_str(node_id)?;
                    let revision_id = RecordId::from_str(revision_id)?;
                    if &revision_id != target_revision {
                        item_repo
                            .update_status(space_id, &item_id, WorkItemStatus::Done)
                            .await?;
                        continue;
                    }

                    let extractor = DefaultExtractor::new();
                    let original_key = std::env::var("ANTHROPIC_API_KEY").ok();
                    std::env::remove_var("ANTHROPIC_API_KEY");
                    let result = execute_extract_assertions(
                        &storage,
                        &extractor,
                        &node_id,
                        &revision_id,
                        &work_run_id,
                        space_id,
                        "scenario",
                        None,
                    )
                    .await;
                    if let Some(value) = original_key {
                        std::env::set_var("ANTHROPIC_API_KEY", value);
                    }
                    let result = result?;
                    promote_extraction_artifact(
                        &storage,
                        space_id,
                        &result.artifact_id,
                        &work_run_id,
                    )
                    .await?;
                }
                other => return Err(anyhow::anyhow!("unhandled work kind: {other}")),
            }

            item_repo
                .update_status(space_id, &item_id, WorkItemStatus::Done)
                .await?;
            let _ = dag.on_item_completed(space_id, &item_id).await;
        }
    }

    Ok(())
}

async fn promote_extraction_artifact(
    storage: &StorageClient,
    space_id: &str,
    artifact_id: &RecordId,
    work_run_id: &RecordId,
) -> anyhow::Result<()> {
    let artifact_repo = WorkArtifactRepository::new(storage.clone());
    let artifact = artifact_repo
        .get(space_id, artifact_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extraction artifact not found: {artifact_id}"))?;
    let data: ExtractionArtifactData = serde_json::from_value(artifact.data)?;
    let revision_id = RecordId::from_str(&data.revision_id)?;
    let node_id = RecordId::from_str(&data.node_id)?;
    let assertions = data
        .assertions
        .iter()
        .map(|assertion| assertion.to_raw(&revision_id, &node_id))
        .collect::<Result<Vec<_>, _>>()?;

    promote_assertions_inline(
        storage,
        space_id,
        &assertions,
        work_run_id,
        &node_id,
        &revision_id,
        "scenario",
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn scenario_s5_5_manual_extraction() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let ingest_params = serde_json::json!({
        "title": "S5.5 Doc",
        "text": "Alice works at Acme Corp."
    });
    let response = dispatcher
        .execute("ingest_doc_simple", ingest_params)
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "ingest_doc_simple should succeed");
    let doc_id = decoded
        .data
        .as_ref()
        .and_then(|value| value.get("data"))
        .and_then(|value| value.get("id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("ingest_doc_simple response missing doc id"))?;

    let response = dispatcher
        .execute("extract_doc", serde_json::json!({ "id": doc_id }))
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "extract_doc should succeed");
    let revision_id = decoded
        .data
        .as_ref()
        .and_then(|value| value.get("revision_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("extract_doc response missing revision_id"))?;
    let revision_id = RecordId::from_str(revision_id)?;

    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let revision = revision_repo.get("default", &revision_id).await?;
    assert!(
        revision.is_some(),
        "extract_doc should have created doc_revision {}",
        revision_id
    );

    drive_pipeline_for_revision(harness.client.clone(), &revision_id, 50).await?;

    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let assertions = assertion_repo
        .find_by_revision("default", &revision_id, 50)
        .await?;
    assert!(
        !assertions.is_empty(),
        "expected assertions after manual extract_doc + pipeline run"
    );

    Ok(())
}
