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

use anyhow::Context;
use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::extraction::{
    DefaultExtractor, ExtractionContext, ExtractionResult, Extractor,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::scheduler::ingest_work_kinds::TRIAGE_SOURCE_KIND;
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository, SpaceConfigRepository,
    StorageBackendKind, StorageClient, StorageConfig, WorkItemRepository,
};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::publication::{OperationReceipt, OperationState};
use elephant_types::records::doc_revision::RevisionStatus;
use elephant_types::records::work::{PluginManifest, WorkItemStatus};
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

// Shared controlled HTTP transport only; hashed vectors below retain this
// scenario's original deterministic numerical behavior, not model relevance.
#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario module and its existing independent type identity"
)]
#[path = "../../recall_preview_semantics/provider_spy.rs"]
mod alice_transport;

#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario module and its existing independent type identity"
)]
#[path = "../../e2e/src/pipeline_driver.rs"]
mod alice_pipeline;

/// The original fixture tests document ranking, not generated canonical claims.
struct DocumentOnlyExtractor {
    manifest: PluginManifest,
}
#[async_trait::async_trait]
impl Extractor for DocumentOnlyExtractor {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
    async fn extract(
        &self,
        _context: ExtractionContext,
    ) -> Result<ExtractionResult, elephant_pipeline::PipelineError> {
        Ok(ExtractionResult::empty(
            "alice-document-ranking-fixture".into(),
        ))
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
    let spy = alice_transport::Spy::start().await?;
    spy.hashed_vectors(dims)?;
    alice_transport::configure(&config_repo, "default", &spy.endpoint, dims).await?;
    alice_transport::edit(&config_repo, "default", |processing| {
        processing.bindings.retain(|operation, _| {
            matches!(
                operation,
                ModelOperation::QueryEmbedding | ModelOperation::DocumentEmbedding
            )
        });
        // Historical profiles are immutable. Removing generation bindings
        // removes approval while retaining their recorded profile identities.
        Ok(())
    })
    .await?;

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
    let alice_accepted: OperationReceipt =
        serde_json::from_value(alice_response["operation"].clone())?;
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
    let control_accepted: OperationReceipt =
        serde_json::from_value(control_response["operation"].clone())?;
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

    let query = "white rabbit rabbit hole wonderland";
    let operations = SourcePublicationRepository::new(client.clone());
    for accepted in [&alice_accepted, &control_accepted] {
        assert_ne!(accepted.state, OperationState::Completed);
    }
    let pending_response = with_dispatch_credentials(
        Arc::new(alice_transport::SpyCredentials),
        dispatcher.execute("search_docs", json!({"query": query, "k": 5})),
    )
    .await?;
    let pending: ToolResponse<serde_json::Value> = serde_json::from_value(pending_response)?;
    assert!(pending.ok);
    assert!(
        pending
            .data
            .as_ref()
            .and_then(|value| value["items"].as_array())
            .context("pending document bucket missing")?
            .is_empty(),
        "accepted but unpublished sources must be absent"
    );
    let before_publication = spy.count()?;
    let extractor = DocumentOnlyExtractor {
        manifest: DefaultExtractor::new().manifest().clone(),
    };
    with_dispatch_credentials(
        Arc::new(alice_transport::SpyCredentials),
        Box::pin(alice_pipeline::drive_pipeline(
            &client, "default", &extractor, 300,
        )),
    )
    .await?;
    for accepted in [&alice_accepted, &control_accepted] {
        let receipt = operations
            .operation("default", &accepted.operation_id)
            .await?
            .context("original source operation missing")?;
        assert_eq!(receipt.state, OperationState::Completed);
        assert_eq!(receipt.source_id, accepted.source_id);
        assert_eq!(receipt.source_generation, accepted.source_generation);
        assert_eq!(receipt.published_revision, accepted.desired_revision);
    }
    let node_repo = DocNodeRepository::new(client.clone());
    let revisions = DocRevisionRepository::new(client.clone());
    let mut chunk_count = 0;
    for doc in &docs {
        let active: Vec<_> = revisions
            .find_by_doc(&doc.base.id)
            .await?
            .into_iter()
            .filter(|revision| revision.status == RevisionStatus::Active)
            .collect();
        assert_eq!(
            active.len(),
            1,
            "original document must have one published active revision"
        );
        let revision = &active[0];
        let chunks = node_repo.find_chunks_by_revision(&revision.base.id).await?;
        assert!(
            chunks.iter().all(|chunk| chunk.embedding_model.is_some()),
            "all published chunks must carry their completed embedding profile"
        );
        chunk_count += chunks.len();
    }
    assert!(
        chunk_count > 10,
        "expected the full Alice document to produce chunk embeddings"
    );

    let embedding_calls = spy.count()?;
    assert!(
        embedding_calls > before_publication,
        "document embeddings must reach the approved transport"
    );
    let search_response = with_dispatch_credentials(
        Arc::new(alice_transport::SpyCredentials),
        dispatcher.execute(
            "search_docs",
            json!({
                "query": query,
                "k": 5
            }),
        ),
    )
    .await?;
    let search_decoded: ToolResponse<serde_json::Value> = serde_json::from_value(search_response)?;
    assert!(search_decoded.ok, "semantic search should succeed");
    assert_eq!(
        spy.count()?,
        embedding_calls + 1,
        "query must dispatch through its actual approval"
    );
    let search_mode = search_decoded
        .data
        .as_ref()
        .and_then(|value| value["mode"].as_str())
        .context("retained search mode missing")?;
    assert!(
        matches!(search_mode, "semantic" | "semantic+lexical"),
        "lexical fallback cannot qualify this scenario"
    );

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
