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

use anyhow::Context;
use elephant_mcp::{create_dispatcher, McpError, ToolResponse};
use elephant_pipeline::extraction::{
    AssertionValidator, DefaultExtractor, ExtractionContext, ExtractionResult, Extractor,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::{
    AssertionRepository, DocRevisionRepository, RelRepository, Repository, StorageError,
    WorkItemRepository,
};
use elephant_types::publication::OperationState;
use elephant_types::records::work::{PluginManifest, WorkItemStatus};

use crate::grounded_source_fixture::{fixture_published_revision, GroundedSourceFixture};
use crate::Harness;

/// This scenario tests explicit manual evidence creation. The real source queue
/// publishes the document without generating a competing extracted assertion.
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
        Ok(ExtractionResult::empty("manual-evidence-document".into()))
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
async fn scenario_s5_10_relationship_evidence() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let fixture = GroundedSourceFixture::new(&harness.client, "default").await?;
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

    let evidence_text = "Alice works at Acme Corp.";
    let accepted =
        Box::pin(fixture.accept_text(&harness.client, "S5.10 Evidence Source", evidence_text))
            .await?;
    let extractor = DocumentOnlyExtractor {
        manifest: DefaultExtractor::new().manifest().clone(),
    };
    let work = WorkItemRepository::new(harness.client.clone());
    let mut rendered = false;
    // Stop at the actual rendering boundary, before extraction/finalization.
    // Every step claims a real queued item and retains its original attempt.
    for _ in 0..20 {
        let runnable = work
            .find_by_status("default", WorkItemStatus::Runnable)
            .await?;
        assert_eq!(runnable.len(), 1, "expected one pre-render source stage");
        let item = &runnable[0];
        assert_ne!(
            item.kind,
            elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND,
            "render must precede publication"
        );
        Box::pin(fixture.run_item(&harness.client, &extractor, item)).await?;
        if item.kind == elephant_pipeline::scheduler::RENDER_DOC_KIND {
            rendered = true;
            break;
        }
    }
    assert!(rendered, "actual source work did not render its document");
    let revision_id = fixture_published_revision(&harness.client, &accepted).await?;
    let rendered_revision = DocRevisionRepository::new(harness.client.clone())
        .get("default", &revision_id)
        .await?
        .context("actual rendered evidence revision")?;
    assert_eq!(
        rendered_revision.text,
        format!("{evidence_text}\n"),
        "publication must certify the renderer's actual normalized bytes"
    );
    let publications = SourcePublicationRepository::new(harness.client.clone());
    let pending = publications
        .operation("default", &accepted.operation.operation_id)
        .await?
        .context("original pending operation")?;
    assert_ne!(pending.state, OperationState::Completed);
    assert!(pending.published_revision.is_none());

    let arguments = serde_json::json!({
        "from_id": a_id,
        "to_id": b_id,
        "rel_type": "works_at",
        "evidence_text": evidence_text,
        "evidence_source": revision_id.to_string(),
    });
    let error = dispatcher
        .execute("relate_entities", arguments.clone())
        .await
        .expect_err("rendered but unpublished source evidence must be refused");
    assert!(
        matches!(error, McpError::StorageError(StorageError::Conflict(ref reason))
            if reason == "maintenance_source_not_published"),
        "expected the real publication barrier, got {error}"
    );
    assert!(AssertionRepository::new(harness.client.clone())
        .list("default", 100, None)
        .await?
        .is_empty());
    assert!(RelRepository::new(harness.client.clone())
        .list("default", 100, None)
        .await?
        .is_empty());

    Box::pin(fixture.drive(&harness.client, &extractor)).await?;
    let published = publications
        .operation("default", &accepted.operation.operation_id)
        .await?
        .context("original completed operation")?;
    assert_eq!(published.state, OperationState::Completed);
    assert_eq!(published.source_id, accepted.operation.source_id);
    assert_eq!(
        published.source_generation,
        accepted.operation.source_generation
    );
    assert_eq!(
        published.published_revision,
        accepted.operation.desired_revision
    );
    assert_eq!(
        fixture_published_revision(&harness.client, &accepted).await?,
        revision_id
    );

    let source_uuid = accepted.source.base.id.id().to_string();
    let rel = dispatcher.execute("relate_entities", arguments).await?;
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
    assert_eq!(
        items.len(),
        1,
        "only the manual relate_entities call must create its evidence assertion"
    );

    let evidence = items[0]
        .get("evidence")
        .and_then(|v| v.as_object())
        .expect("evidence object");
    let quote_hash = evidence
        .get("quote_hash")
        .and_then(|v| v.as_str())
        .expect("quote_hash");
    assert_eq!(
        quote_hash,
        format!("sha256:{}", AssertionValidator::hash_quote(evidence_text))
    );
    assert_eq!(evidence["revision"], revision_id.to_string());
    assert_eq!(evidence["start"], 0);
    assert_eq!(evidence["end"], evidence_text.len());

    Ok(())
}
