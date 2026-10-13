//! Independent fixture inputs for production repository and promotion tests.
//! No model, provider, canonical winner, or successful receipt is fabricated here.

#![allow(dead_code)] // Shared by test targets exercising different fixture subsets.

use elephant_migrations::apply_schema;
use elephant_pipeline::error::PipelineError;
use elephant_pipeline::extraction::{
    verify_assertion_support, AssertionSupportVerifier, RawAssertion, SupportCandidate,
    SupportVerdict,
};
use elephant_pipeline::renderer::{default::PassthroughRenderer, RendererRegistry};
use elephant_pipeline::scheduler::execute_render_doc;
use elephant_pipeline::source_queue::accepted_document_id;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository, SourceRepository,
    SpaceConfigRepository, StorageClient,
};
use elephant_types::publication::{
    OperationReceipt, PublicationFence, SourceAuthorizationSnapshot, SourcePrecondition,
};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::{
    Doc, DocNode, DocRevision, ManualDocTemplate, Source, SourceType, SpaceConfig,
};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug)]
pub enum Backend {
    SurrealMemory,
    ElephantDb,
}

pub const BACKENDS: [Backend; 2] = [Backend::SurrealMemory, Backend::ElephantDb];
pub const SPACE: &str = "qualification-confidential";
pub const TEXT: &str = "Élodie opened the Oriole laboratory on 5 October 2026.";

/// A second fixed evidence input: its only opening claim is beyond byte 1024.
pub fn long_evidence_text() -> String {
    format!(
        "{}{TEXT}",
        "The catalog describes shelf labels and paper covers. ".repeat(23)
    )
}

pub async fn storage(backend: Backend) -> anyhow::Result<StorageClient> {
    let client = match backend {
        Backend::SurrealMemory => StorageClient::new_memory().await?,
        Backend::ElephantDb => StorageClient::new_embedded().await?,
    };
    if matches!(backend, Backend::SurrealMemory) {
        apply_schema(&client).await.map_err(anyhow::Error::msg)?;
    }
    Ok(client)
}

pub fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// MCP success envelopes retain `ok`/`data`; HTTP returns the inner search DTO.
pub fn mcp_success_data(response: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    anyhow::ensure!(
        response["ok"] == true,
        "MCP operation did not succeed: {response}"
    );
    response
        .get("data")
        .filter(|data| data.is_object())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("MCP success data object absent"))
}

/// Capture and evaluate the exact prestate used by a subsequent source mutation.
/// The principal has fixed fixture clearances; it is never derived from the row.
pub async fn authorized_source_snapshot(
    repository: &SourcePublicationRepository,
    space: &str,
    source_id: &RecordId,
) -> anyhow::Result<SourceAuthorizationSnapshot> {
    use elephant_policy::{AbacEngine, Clearance, Principal, RecordAttributes};

    let snapshot = repository
        .authorization_snapshot(space, source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source authorization prestate absent"))?;
    let principal = Principal {
        space_id: space.into(),
        scopes: ["read:source".into(), "write:source".into()].into(),
        clearances: vec![
            Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: Default::default(),
                handling_allow: Default::default(),
            },
            Clearance {
                policy_id: "qualification-policy".into(),
                max_level: 7,
                labels: ["research".into()].into(),
                handling_allow: ["no-external-processing".into()].into(),
            },
        ],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    let envelope = &snapshot.envelope;
    let record = RecordAttributes {
        space_id: envelope.space_id.clone(),
        security: envelope.security.clone(),
        subjects: envelope.subjects.clone(),
        subjects_state: envelope.subjects_state,
    };
    anyhow::ensure!(
        AbacEngine::new()
            .evaluate(&principal, &record, "write:source")
            .is_allowed(),
        "fixed qualification principal cannot authorize source prestate"
    );
    Ok(snapshot)
}

pub fn classified_base(table: TableName, space: &str) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        space.into(),
        "qualification".into(),
    );
    base.security = Security {
        policy_id: "qualification-policy".into(),
        level: 7,
        labels: vec!["research".into()],
        handling: vec!["no-external-processing".into()],
    };
    base.subjects_state = SubjectsState::Complete;
    base.subjects = vec![Link::new(RecordId::new(
        TableName::Entity,
        uuid::Uuid::from_u128(0x5d367e7ce8cd4a30a5245d167acb0401),
    ))];
    base
}

pub struct Lineage {
    pub request: AcceptSourceRevision,
    pub accepted: OperationReceipt,
    pub source: Source,
    pub doc: Doc,
    pub revision: DocRevision,
    pub node: DocNode,
}

pub async fn lineage(client: &StorageClient, space: &str) -> anyhow::Result<Lineage> {
    lineage_with_subjects_state(client, space, SubjectsState::Complete).await
}

pub async fn lineage_with_subjects_state(
    client: &StorageClient,
    space: &str,
    subjects_state: SubjectsState,
) -> anyhow::Result<Lineage> {
    let mut config = serde_json::to_value(classified_base(TableName::SpaceConfig, space))?;
    config["embedding_provider"] = serde_json::json!("qualification-no-dispatch");
    config["embedding_model"] = serde_json::json!("qualification-no-dispatch");
    config["embedding_dims"] = serde_json::json!(3);
    config["purpose"] = serde_json::json!("Independent Oriole grounding fixture");
    config["skill"] = serde_json::Value::Null;
    config["processing"] = serde_json::Value::Null;
    lineage_with_configuration(
        client,
        space,
        subjects_state,
        serde_json::from_value::<SpaceConfig>(config)?,
    )
    .await
}

pub async fn lineage_with_configuration(
    client: &StorageClient,
    space: &str,
    subjects_state: SubjectsState,
    config: SpaceConfig,
) -> anyhow::Result<Lineage> {
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await?;
    lineage_for_registered_space(
        client,
        space,
        subjects_state,
        "qualification/oriole/opening",
        "oriole-revision-1",
    )
    .await
}

/// A distinct corroborating source in a space whose processing profile is already registered.
pub async fn lineage_for_registered_space(
    client: &StorageClient,
    space: &str,
    subjects_state: SubjectsState,
    external_id: &str,
    revision_id: &str,
) -> anyhow::Result<Lineage> {
    lineage_text_for_registered_space(
        client,
        space,
        subjects_state,
        external_id,
        revision_id,
        TEXT,
    )
    .await
}

/// Exact alternate input goes through acceptance and real immutable rendering.
pub async fn lineage_text_for_registered_space(
    client: &StorageClient,
    space: &str,
    subjects_state: SubjectsState,
    external_id: &str,
    revision_id: &str,
    text: &str,
) -> anyhow::Result<Lineage> {
    anyhow::ensure!(
        SpaceConfigRepository::new(client.clone())
            .get_by_space_id(space)
            .await?
            .is_some(),
        "fixture named space absent"
    );
    let mut source = Source::new(
        classified_base(TableName::Source, space),
        SourceType::Manual,
        digest(text),
    );
    source.external_id = Some(external_id.into());
    source.base.subjects_state = subjects_state;
    source.uri = Some("https://example.invalid/oriole/opening".into());
    let doc_id = RecordId::new_v4(TableName::Doc);
    // Each independently accepted source/revision has its own immutable
    // rendition; the database correctly enforces uniqueness within a space.
    let rendition_key = format!("qualification/{external_id}/{revision_id}/text");
    source.set_manual_doc_template(&ManualDocTemplate {
        doc_id: doc_id.to_string(),
        title: "Oriole opening".into(),
        text: text.into(),
        sidecar_text: None,
        media_attachments: vec![],
        rendition_key: rendition_key.clone(),
        metadata: serde_json::json!({}),
    })?;
    let request = AcceptSourceRevision {
        source,
        revision_id: revision_id.into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: format!("qualification-{external_id}-{revision_id}"),
        request_fingerprint: digest(text),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    };
    let receipt = SourcePublicationRepository::new(client.clone())
        .accept(request.clone())
        .await?;
    let source = SourceRepository::new(client.clone())
        .get(space, &receipt.source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted source absent"))?;
    let publication = PublicationFence {
        source_id: receipt.source_id.clone(),
        generation: receipt.source_generation.clone(),
        revision_id: receipt
            .desired_revision
            .clone()
            .ok_or_else(|| anyhow::anyhow!("actual acceptance has no immutable revision"))?,
        operation_id: receipt.operation_id.clone(),
    };
    let mut base = classified_base(TableName::Doc, space);
    // Use the production immutable-document address from the actual accepted
    // operation. A staged template's Doc ID is not its materialized rendition.
    base.id = accepted_document_id(&publication, &doc_id.to_string())?;
    base.subjects_state = subjects_state;
    base.provenance.sources.push(source.base.id.clone());
    let doc = DocRepository::new(client.clone())
        .create(Doc {
            base,
            title: "Oriole opening".into(),
            text: text.into(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: vec![],
            rendition_key,
            metadata: serde_json::json!({"_publication": publication}),
        })
        .await?;
    let mut renderers = RendererRegistry::new();
    renderers.register(Box::new(PassthroughRenderer::new()));
    let rendered = execute_render_doc(client, &renderers, &doc, Some("text/plain")).await?;
    anyhow::ensure!(
        rendered.success,
        "fixture rendering failed: {:?}",
        rendered.error
    );
    let revision = DocRevisionRepository::new(client.clone())
        .get(space, &rendered.revision_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("renderer did not persist immutable revision"))?;
    let mut node_base = classified_base(TableName::DocNode, space);
    node_base.subjects_state = subjects_state;
    let node = DocNodeRepository::new(client.clone())
        .create(DocNode {
            base: node_base,
            revision: Link::new(revision.base.id.clone()),
            // The public lexical route discovers Chunk nodes. This exact
            // original-text leaf is shared with extraction and index coverage.
            role: NodeRole::Chunk,
            span: TextSpan {
                start: 0,
                end: u32::try_from(revision.text.len())?,
            },
            node_key: "qualification/oriole/chunk-0".into(),
            text: Some(revision.text.clone()),
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        })
        .await?;
    Ok(Lineage {
        request,
        accepted: receipt,
        source,
        doc,
        revision,
        node,
    })
}

pub async fn grounded_assertion(input: &Lineage) -> anyhow::Result<RawAssertion> {
    anyhow::ensure!(
        input.revision.text == TEXT || input.revision.text == long_evidence_text(),
        "fixed interpretation fixture only approves its two independently specified inputs"
    );
    let mut assertion = RawAssertion {
        subject_text: "Élodie".into(),
        predicate_text: "opened".into(),
        object_text: Some("Oriole laboratory".into()),
        object_literal: None,
        evidence_span: EvidenceSpan {
            revision: Link::new(input.revision.base.id.clone()),
            node: Some(Link::new(input.node.base.id.clone())),
            start: 0,
            end: u32::try_from(input.revision.text.len())?,
            quote_hash: digest(&input.revision.text),
        },
        confidence: 0.99,
        quote: Some(input.revision.text.clone()),
        source_event_time: None,
        source_event_time_precision: None,
        event_title: Some("Oriole laboratory opened".into()),
        event_lineage_key: Some("qualification/oriole/opening".into()),
        subject_entity_id: None,
        subject_entity_type: Some("person".into()),
        object_entity_id: None,
        object_entity_type: Some("org".into()),
        significance: Some("high".into()),
        support_verification: None,
    };
    verify_assertion_support(
        std::slice::from_mut(&mut assertion),
        &input.revision.text,
        &OrioleVerifier {
            revision: input.revision.base.id.clone(),
            node: input.node.base.id.clone(),
            quote: input.revision.text.clone(),
        },
    )
    .await?;
    anyhow::ensure!(
        assertion.support_verification.is_some(),
        "fixture claim was not independently supported"
    );
    Ok(assertion)
}

/// Fixed known-answer interpretation; no arbitrary candidate is approved.
/// This is a deterministic support-protocol fixture, not a model-quality claim.
struct OrioleVerifier {
    revision: RecordId,
    node: RecordId,
    quote: String,
}

#[async_trait::async_trait]
impl AssertionSupportVerifier for OrioleVerifier {
    fn identity(&self) -> &str {
        "independent-oriole-expectation-v1"
    }

    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        // The v2 verifier input binds exact immutable byte coordinates as well
        // as semantics. These expectations come from our fixed input, never
        // from the candidate; unknown or missing fields still fail equality.
        let expected = serde_json::json!({
            "revision": self.revision.to_string(), "node": self.node.to_string(),
            "quote_hash": digest(&self.quote), "start": 0, "end": self.quote.len(),
            "subject": "Élodie", "predicate": "opened",
            "object": "Oriole laboratory", "literal": null, "subject_type": "person",
            "object_type": "org", "subject_entity": null, "object_entity": null,
            "event_time": null, "event_precision": null,
            "event_title": "Oriole laboratory opened",
            "event_lineage": "qualification/oriole/opening", "confidence": 0.99,
            "significance": "high"
        });
        Ok(candidates
            .iter()
            .map(|candidate| SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == self.quote && candidate.claim == expected,
            })
            .collect())
    }
}
