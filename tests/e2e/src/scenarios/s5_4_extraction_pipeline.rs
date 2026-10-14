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

use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::extraction::{DefaultExtractor, Extractor};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{AssertionRepository, Repository};
use elephant_types::Link;

use crate::Harness;

#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario transport module and its existing independent type identity"
)]
#[path = "../../../recall_preview_semantics/provider_spy.rs"]
mod extraction_transport;

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

struct FixtureSupport;
#[async_trait::async_trait]
impl elephant_pipeline::extraction::AssertionSupportVerifier for FixtureSupport {
    fn identity(&self) -> &str {
        "s5-4-known-answer-v1"
    }
    async fn verify(
        &self,
        candidates: &[elephant_pipeline::extraction::SupportCandidate],
    ) -> Result<Vec<elephant_pipeline::extraction::SupportVerdict>, elephant_pipeline::PipelineError>
    {
        Ok(candidates
            .iter()
            .map(|candidate| elephant_pipeline::extraction::SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == "Alice works at Acme Corp."
                    && candidate.claim["subject"] == "Alice"
                    && candidate.claim["predicate"] == "works at"
                    && candidate.claim["object"] == "Acme Corp"
                    && candidate.claim["literal"].is_null()
                    && candidate.claim["event_time"].is_null()
                    && candidate.claim["event_title"].is_null(),
            })
            .collect())
    }
}

/// A fixed extractor response exercises orchestration without network calls.
struct FixtureExtractor {
    manifest: elephant_types::records::work::PluginManifest,
}

#[async_trait::async_trait]
impl elephant_pipeline::extraction::Extractor for FixtureExtractor {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        &self.manifest
    }

    async fn extract(
        &self,
        ctx: elephant_pipeline::extraction::ExtractionContext,
    ) -> Result<elephant_pipeline::extraction::ExtractionResult, elephant_pipeline::PipelineError>
    {
        use elephant_pipeline::extraction::{ExtractionResult, RawAssertion};
        use elephant_types::records::assertion::EvidenceSpan;
        use sha2::{Digest, Sha256};
        let quote = "Alice works at Acme Corp.";
        let start = ctx.node_text.find(quote).ok_or_else(|| {
            elephant_pipeline::PipelineError::ExtractionError("fixture text missing".to_string())
        })?;
        let mut assertions = vec![RawAssertion {
            support_verification: None,
            subject_text: "Alice".to_string(),
            predicate_text: "works at".to_string(),
            object_text: Some("Acme Corp".to_string()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(ctx.revision_id),
                node: Some(Link::new(ctx.node_id)),
                start: u32::try_from(start).expect("small fixture"),
                end: u32::try_from(start + quote.len()).expect("small fixture"),
                quote_hash: format!("{:x}", Sha256::digest(quote.as_bytes())),
            },
            confidence: 0.99,
            quote: Some(quote.to_string()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("org".to_string()),
            significance: Some("high".to_string()),
        }];
        elephant_pipeline::extraction::verify_assertion_support(
            &mut assertions,
            &ctx.node_text,
            &FixtureSupport,
        )
        .await?;
        Ok(ExtractionResult::new(
            assertions,
            0,
            "deterministic-fixture".to_string(),
        ))
    }
}

#[tokio::test]
#[ignore]
async fn scenario_s5_4_extraction_pipeline() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space = "grounded-extraction";
    let fixture =
        crate::grounded_source_fixture::GroundedSourceFixture::new(&harness.client, space).await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal(space));

    let ingest_params = serde_json::json!({
        "title": "S5.4 Doc",
        "text": "Alice works at Acme Corp."
    });
    let response = dispatcher
        .execute("ingest_doc_simple", ingest_params)
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "ingest_doc_simple should succeed");

    let extractor = FixtureExtractor {
        manifest: DefaultExtractor::new().manifest().clone(),
    };
    fixture.drive(&harness.client, &extractor).await?;

    let sources = elephant_storage::SourceRepository::new(harness.client.clone())
        .list(space, 10, None)
        .await?;
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].lifecycle_status,
        elephant_types::records::SourceLifecycleStatus::GraphReady
    );
    let assertions = AssertionRepository::new(harness.client.clone())
        .list(space, 10, None)
        .await?;
    assert_eq!(
        assertions.len(),
        1,
        "artifact commit must persist the fixture assertion"
    );
    let events = elephant_storage::SystemEventRepository::new(harness.client.clone())
        .get_after_seq(space, 0, 100)
        .await?;
    assert!(
        !events.is_empty(),
        "artifact commit must emit graph change events"
    );

    let search_params = serde_json::json!({ "query": "Alice", "limit": 10 });
    // drive() scopes credentials only around the real publication pipeline.
    // The subsequent real query has its own current approved binding and must
    // resolve the same loopback credential references in its own task scope.
    let response = with_dispatch_credentials(
        Arc::new(extraction_transport::SpyCredentials),
        Box::pin(dispatcher.execute("search_entities", search_params)),
    )
    .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "search_entities should succeed");
    let items = decoded
        .data
        .as_ref()
        .and_then(|value| value.get("items"))
        .and_then(|value| value.as_array())
        .ok_or_else(|| anyhow::anyhow!("search_entities response missing items"))?;
    assert!(
        !items.is_empty(),
        "expected at least one entity after extraction pipeline"
    );

    Ok(())
}
