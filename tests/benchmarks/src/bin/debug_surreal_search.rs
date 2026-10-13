#![allow(missing_docs)]

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use elephant_embeddings::{
    discover_accessible_partition_keys, EmbeddingProvider, HashedEmbeddingProvider, PartitionKey,
    PartitionManager,
};
use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_migrations::apply_schema;
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{execute_build_doc_tree, execute_render_doc, DocTreeConfig};
use elephant_pipeline::EmbedWorker;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SearchRepository, SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::space::SpaceConfig;
use serde_json::json;
use uuid::Uuid;

#[tokio::main]
#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain search repository, dispatcher and embedding worker ownership and drop order"
)]
async fn main() -> Result<()> {
    let namespace = std::env::var("SURREALDB_NAMESPACE")
        .unwrap_or_else(|_| format!("debug_search_{}", Uuid::new_v4().simple()));
    let database = std::env::var("SURREALDB_DATABASE")
        .unwrap_or_else(|_| format!("debug_search_{}", Uuid::new_v4().simple()));

    let client = StorageClient::new(StorageConfig {
        backend: StorageBackendKind::SurrealDb,
        url: std::env::var("SURREALDB_URL").unwrap_or_else(|_| "ws://127.0.0.1:8000".to_string()),
        username: std::env::var("SURREALDB_USER").unwrap_or_else(|_| "root".to_string()),
        password: std::env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".to_string()),
        namespace: namespace.clone(),
        database: database.clone(),
        ..StorageConfig::default()
    })
    .await
    .context("create storage client")?;
    println!("namespace: {namespace}");
    println!("database: {database}");
    apply_schema(&client)
        .await
        .map_err(|err| anyhow::anyhow!("apply schema: {err}"))?;

    let dims = 256;
    let space_config_repo = SpaceConfigRepository::new(client.clone());
    space_config_repo
        .create(space_config("default", dims))
        .await
        .context("create space config")?;

    let principal = test_principal("default");
    let dispatcher = Arc::new(create_dispatcher(client.clone(), principal.clone()));

    ingest_doc_simple(
        &dispatcher,
        "Alice's Adventures in Wonderland",
        "Alice follows the white rabbit into wonderland and falls down the rabbit hole.",
    )
    .await?;
    ingest_doc_simple(
        &dispatcher,
        "Control Document",
        "This document is about boring accounting topics and ledgers.",
    )
    .await?;

    let doc_repo = DocRepository::new(client.clone());
    let docs = doc_repo
        .list("default", 100, None)
        .await
        .context("list docs")?;
    println!("doc count: {}", docs.len());
    for doc in &docs {
        println!("doc: {} {}", doc.base.id, doc.title);
    }

    let raw = doc_repo
        .search(
            &elephant_storage::SearchCriteria::new()
                .with_space_id("default")
                .with_query("white rabbit wonderland")
                .with_limit(5),
        )
        .await
        .context("raw doc search")?;
    println!(
        "raw search titles: {:?}",
        raw.results
            .iter()
            .map(|doc| doc.title.clone())
            .collect::<Vec<_>>()
    );

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let node_repo = DocNodeRepository::new(client.clone());
    let mut chunk_ids = Vec::new();

    for doc in &docs {
        let render = execute_render_doc(&client, &registry, doc, Some("text/plain"))
            .await
            .context("render doc")?;
        execute_build_doc_tree(&client, &render.revision_id, &DocTreeConfig::default())
            .await
            .context("build doc tree")?;
        let chunks = node_repo
            .find_chunks_by_revision(&render.revision_id)
            .await
            .context("find chunks")?;
        println!("chunks for {}: {}", doc.title, chunks.len());
        for chunk in chunks {
            chunk_ids.push(chunk.base.id);
        }
    }

    let embed_worker = EmbedWorker::new(
        node_repo,
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );
    let embed_result = embed_worker
        .embed_chunks("default", &chunk_ids)
        .await
        .context("embed chunks")?;
    println!(
        "embed result: processed={}, chunk_ids={}",
        embed_result.chunks_processed,
        embed_result.chunk_ids.len()
    );

    let tables = client.list_tables().await.context("list tables")?;
    println!("tables: {tables:?}");

    let fetched_policy = space_config_repo
        .get_by_space_id("default")
        .await
        .context("get space config")?;
    println!("space config found: {:?}", fetched_policy.is_some());
    if let Some(config) = fetched_policy {
        println!(
            "space config policy: provider={}, model={}, dims={}",
            config.embedding_provider, config.embedding_model, config.embedding_dims
        );
        let policy = elephant_embeddings::EmbeddingPolicy::from_space_config(&config);
        let clearances: Vec<_> = principal
            .clearances
            .iter()
            .map(|clearance| (clearance.policy_id.clone(), clearance.max_level))
            .collect();
        let keys = discover_accessible_partition_keys(&tables, &policy, &clearances, "default", 0);
        println!(
            "partition keys: {:?}",
            keys.iter()
                .map(PartitionKey::table_name)
                .collect::<Vec<_>>()
        );
        let provider = HashedEmbeddingProvider::new("hashed-bow-v1", dims);
        let vector = provider
            .embed(&["white rabbit wonderland".to_string()])
            .await
            .context("embed query")?
            .into_iter()
            .next()
            .context("missing query vector")?;
        let embedding_repo = NodeEmbeddingRepository::new(client.clone(), PartitionManager::new());
        for key in &keys {
            let hits = embedding_repo
                .search_partition(key, "default", &vector, 10, None, false)
                .await;
            println!("partition {} hits: {:?}", key.table_name(), hits);
        }
    }

    let response = dispatcher
        .execute(
            "search_docs",
            json!({
                "query": "white rabbit wonderland",
                "k": 5,
            }),
        )
        .await
        .context("execute search_docs")?;
    let decoded: ToolResponse<serde_json::Value> =
        serde_json::from_value(response).context("decode search_docs response")?;
    println!(
        "search_docs response: {}",
        serde_json::to_string_pretty(&decoded).context("serialize response")?
    );

    Ok(())
}

async fn ingest_doc_simple(
    dispatcher: &elephant_mcp::ToolDispatcher,
    title: &str,
    text: &str,
) -> Result<()> {
    let response = dispatcher
        .execute(
            "ingest_doc_simple",
            json!({
                "title": title,
                "text": text,
                "collection": "default",
            }),
        )
        .await
        .context("execute ingest_doc_simple")?;
    let decoded: ToolResponse<serde_json::Value> =
        serde_json::from_value(response).context("decode ingest response")?;
    anyhow::ensure!(decoded.ok, "ingest_doc_simple failed");
    Ok(())
}

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
        elephant_types::RecordId::new_v4(TableName::SpaceConfig),
        space_id.to_string(),
        "debug".to_string(),
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
        processing: None,
        base,
        embedding_provider: "hashed".to_string(),
        embedding_model: "hashed-bow-v1".to_string(),
        embedding_dims: i32::try_from(dims).unwrap_or(256),
        purpose: Some("Debug search".to_string()),
        skill: None,
    }
}
