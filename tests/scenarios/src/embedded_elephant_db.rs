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
use std::sync::Arc;

use elephant_embeddings::{HashedEmbeddingProvider, PartitionManager};
use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_render_doc, ingest_work_kinds::TRIAGE_SOURCE_KIND,
    DocTreeConfig,
};
use elephant_pipeline::EmbedWorker;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig, WorkItemRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::space::SpaceConfig;
use elephant_types::records::work::WorkItemStatus;
use elephant_types::RecordId;
use serde_json::json;

fn test_principal(space_id: &str) -> Principal {
    let mut scopes = HashSet::new();
    let _ = scopes.insert("*".to_string());

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

fn space_config(space_id: &str, dims: usize) -> SpaceConfig {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        space_id.to_string(),
        "scenario".to_string(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: Vec::new(),
        handling: Vec::new(),
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    SpaceConfig {
        base,
        embedding_provider: "hashed".to_string(),
        embedding_model: "hashed-bow-v1".to_string(),
        embedding_dims: i32::try_from(dims).unwrap(),
        purpose: Some("Embedded Elephant scenario".to_string()),
        skill: None,
    }
}

async fn embedded_client() -> anyhow::Result<StorageClient> {
    let config = StorageConfig {
        backend: StorageBackendKind::ElephantDb,
        ..StorageConfig::default()
    };
    StorageClient::new(config).await.map_err(Into::into)
}

#[tokio::test]
async fn scenario_embedded_elephant_db_ingests_alice_and_searches_semantically(
) -> anyhow::Result<()> {
    let client = embedded_client().await?;
    let principal = test_principal("default");
    let dispatcher = create_dispatcher(client.clone(), principal);

    let dims = 256;
    let config_repo = SpaceConfigRepository::new(client.clone());
    config_repo.create(space_config("default", dims)).await?;

    let alice_text = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("alice_in_wonderland.txt"),
    )?;

    let alice_response = dispatcher
        .execute(
            "ingest_doc_simple",
            json!({
                "title": "Alice's Adventures in Wonderland",
                "text": alice_text,
                "collection": "fiction"
            }),
        )
        .await?;
    let alice_decoded: ToolResponse<serde_json::Value> = serde_json::from_value(alice_response)?;
    assert!(alice_decoded.ok, "alice ingestion should succeed");

    let control_response = dispatcher
        .execute(
            "ingest_doc_simple",
            json!({
                "title": "Chocolate Cake Recipe",
                "text": "Mix butter, sugar, flour, cocoa powder, eggs, and vanilla. Bake until the cake is firm and fragrant.",
                "collection": "recipes"
            }),
        )
        .await?;
    let control_decoded: ToolResponse<serde_json::Value> =
        serde_json::from_value(control_response)?;
    assert!(control_decoded.ok, "control ingestion should succeed");

    let work_item_repo = WorkItemRepository::new(client.clone());
    let runnable = work_item_repo
        .find_by_status("default", WorkItemStatus::Runnable)
        .await?;
    assert_eq!(
        runnable.len(),
        2,
        "both ingests should enqueue source triage"
    );
    assert!(runnable.iter().all(|item| item.kind == TRIAGE_SOURCE_KIND));

    let doc_repo = DocRepository::new(client.clone());
    let docs = doc_repo.list("default", 10, None).await?;
    assert_eq!(docs.len(), 2);

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let node_repo = DocNodeRepository::new(client.clone());
    let mut chunk_ids = Vec::new();

    for doc in &docs {
        let render = execute_render_doc(&client, &registry, doc, Some("text/plain")).await?;
        execute_build_doc_tree(&client, &render.revision_id, &DocTreeConfig::default()).await?;
        let revision_chunks = node_repo
            .find_chunks_by_revision(&render.revision_id)
            .await?;
        chunk_ids.extend(revision_chunks.into_iter().map(|chunk| chunk.base.id));
    }
    assert!(chunk_ids.len() > 10, "expected chunk nodes for retrieval");

    let embed_worker = EmbedWorker::new(
        node_repo,
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );
    let embed_result = embed_worker.embed_chunks("default", &chunk_ids).await?;
    assert!(
        embed_result.chunks_processed > 10,
        "expected the full Alice document to produce chunk embeddings"
    );

    let search_response = dispatcher
        .execute(
            "search_docs",
            json!({
                "query": "white rabbit rabbit hole wonderland",
                "k": 5
            }),
        )
        .await?;
    let search_decoded: ToolResponse<serde_json::Value> = serde_json::from_value(search_response)?;
    assert!(search_decoded.ok, "semantic search should succeed");

    let items = search_decoded
        .data
        .as_ref()
        .and_then(|value| value.get("items"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(!items.is_empty(), "semantic search should return hits");

    let first_title = items[0]
        .get("doc")
        .and_then(|value| value.get("data"))
        .and_then(|value| value.get("title"))
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        first_title, "Alice's Adventures in Wonderland",
        "Alice should rank above the unrelated control document"
    );

    Ok(())
}
