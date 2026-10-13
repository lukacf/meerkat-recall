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
use elephant_mcp::handlers::search::UnifiedSearchOutput;
use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::extraction::{
    DefaultExtractor, ExtractionContext, ExtractionResult, Extractor,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;
use elephant_types::publication::{OperationReceipt, OperationState};
use elephant_types::records::work::PluginManifest;

use crate::Harness;

#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario transport module and its existing independent type identity"
)]
#[path = "../../../recall_preview_semantics/provider_spy.rs"]
mod search_transport;

/// This bucket-contract fixture makes no generated canonical claims. Real
/// source work and finalization still have to publish the original document.
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
        _ctx: ExtractionContext,
    ) -> Result<ExtractionResult, elephant_pipeline::PipelineError> {
        Ok(ExtractionResult::empty("document-bucket-fixture".into()))
    }
}

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
async fn scenario_s5_7_search_all() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let spy = search_transport::Spy::start().await?;
    let configurations = SpaceConfigRepository::new(harness.client.clone());
    search_transport::configure(&configurations, "default", &spy.endpoint, 8).await?;
    search_transport::edit(&configurations, "default", |processing| {
        processing.bindings.retain(|operation, _| {
            matches!(
                operation,
                ModelOperation::QueryEmbedding | ModelOperation::DocumentEmbedding
            )
        });
        Ok(())
    })
    .await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let create_entity = dispatcher
        .execute(
            "create_entity_simple",
            serde_json::json!({
                "entity_type": "person",
                "display_name": "Jane Smith",
                "summary": "Test entity"
            }),
        )
        .await?;
    let entity_resp: ToolResponse<serde_json::Value> = serde_json::from_value(create_entity)?;
    assert!(entity_resp.ok, "create_entity_simple should succeed");

    let ingest_doc = dispatcher
        .execute(
            "ingest_doc_simple",
            serde_json::json!({
                "title": "Acme Notes",
                "text": "Jane Smith works at Acme Corp."
            }),
        )
        .await?;
    let accepted: OperationReceipt = serde_json::from_value(ingest_doc["operation"].clone())?;
    let doc_resp: ToolResponse<serde_json::Value> = serde_json::from_value(ingest_doc)?;
    assert!(doc_resp.ok, "ingest_doc_simple should succeed");
    assert_ne!(accepted.state, OperationState::Completed);

    let query = serde_json::json!({
        "query": "Jane Acme",
        "limit": 10,
        "include": ["entities", "docs"]
    });
    let pending = with_dispatch_credentials(
        Arc::new(search_transport::SpyCredentials),
        dispatcher.execute("search_all", query.clone()),
    )
    .await?;
    let pending: ToolResponse<serde_json::Value> = serde_json::from_value(pending)?;
    assert!(pending.ok, "pending search should succeed");
    let pending_data = pending.data.context("pending search data")?;
    assert!(
        pending_data["docs"]["items"]
            .as_array()
            .context("pending docs.items")?
            .is_empty(),
        "accepted source bytes must not appear before publication"
    );
    assert!(
        !pending_data["entities"]["items"]
            .as_array()
            .context("pending entities.items")?
            .is_empty(),
        "the independently created entity remains visible"
    );

    let before_publication = spy.count()?;
    let extractor = DocumentOnlyExtractor {
        manifest: DefaultExtractor::new().manifest().clone(),
    };
    with_dispatch_credentials(
        Arc::new(search_transport::SpyCredentials),
        Box::pin(crate::pipeline_driver::drive_pipeline(
            &harness.client,
            "default",
            &extractor,
            300,
        )),
    )
    .await?;
    assert!(
        spy.count()? > before_publication,
        "publication must physically embed its document"
    );
    let published = SourcePublicationRepository::new(harness.client.clone())
        .operation("default", &accepted.operation_id)
        .await?
        .context("original source operation disappeared")?;
    assert_eq!(published.state, OperationState::Completed);
    assert_eq!(published.source_id, accepted.source_id);
    assert_eq!(published.source_generation, accepted.source_generation);
    assert_eq!(published.published_revision, accepted.desired_revision);

    let response = with_dispatch_credentials(
        Arc::new(search_transport::SpyCredentials),
        dispatcher.execute("search_all", query),
    )
    .await?;
    let decoded: ToolResponse<UnifiedSearchOutput> = serde_json::from_value(response)?;
    assert!(decoded.ok, "search_all should succeed");

    let data = decoded.data.expect("data");
    let entities = data.entities.context("entities bucket")?.items;
    // Unified search returns RedactionEnvelope<Doc> directly. The separate
    // search_docs tool has a DocSearchResult containing an extra `doc` field.
    let docs = data.docs.context("docs bucket")?.items;

    assert!(!entities.is_empty(), "expected non-empty entities bucket");
    assert!(!docs.is_empty(), "expected non-empty docs bucket");
    assert!(
        docs.iter()
            .any(|row| row.data.title == "Acme Notes"
                && row.data.text == "Jane Smith works at Acme Corp.\n"),
        "published search must return the ingested document with the default renderer's terminal newline"
    );
    assert!(
        spy.requests()?
            .iter()
            .all(|request| request.path.ends_with("/embeddings")),
        "this deterministic bucket fixture must not dispatch generation"
    );

    Ok(())
}
