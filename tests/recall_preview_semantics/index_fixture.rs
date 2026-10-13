//! Actual accepted-source, renderer, EmbedWorker, index and finalizer fixture.
//! Only source text and controlled vectors are supplied; no published head,
//! canonical answer or completion receipt is inserted as an expected winner.
use super::publication_support::PublicationFixture;
use super::spy;
use anyhow::Context;
use chrono::Utc;
use elephant_embeddings::PartitionManager;
use elephant_mcp::handlers::ToolContext;
use elephant_pipeline::embed::EmbedWorker;
use elephant_pipeline::renderer::{
    default::{DefaultTextRenderer, PassthroughRenderer},
    RendererRegistry,
};
use elephant_pipeline::scheduler::evidence_work_kinds::execute_render_doc;
use elephant_pipeline::scheduler::ingest_work_kinds::SourceMetadataArtifactData;
use elephant_pipeline::scheduler::WorkQueue;
use elephant_policy::{AbacEngine, Clearance, Principal, RecordAttributes};
use elephant_storage::source_intake::{accepted_document_id, stage_source_revision};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::{
    required_embedding, source_embedding_work_id_for_plan, source_plan_id, SourceDocumentMapping,
    SourcePublicationManifest, SourcePublicationManifestRepository,
};
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SourceRepository, SpaceConfigRepository, StorageClient, WorkItemRepository, WorkRunRepository,
};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::publication::{PublicationFence, SourcePrecondition};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::{DocNode, ManualDocTemplate, Source, SourceType};
use elephant_types::{Link, RecordId, TableName};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub fn reader(client: &StorageClient, space: &str) -> ToolContext {
    ToolContext::new(
        client.clone(),
        Principal {
            space_id: space.into(),
            scopes: HashSet::from([
                "read:doc:raw".into(),
                "read:entity".into(),
                "read:assertion".into(),
                "write:source".into(),
            ]),
            // Fixed before looking at any candidate row. Hidden policy is absent.
            clearances: [
                ("default", 0),
                ("index-a", 0),
                ("index-b", 0),
                ("synthetic-household", 3),
                ("synthetic-business", 7),
            ]
            .into_iter()
            .map(|(policy_id, max_level)| Clearance {
                policy_id: policy_id.into(),
                max_level,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            })
            .collect(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        },
    )
}

pub fn security(policy: &str, level: i32) -> Security {
    Security {
        policy_id: policy.into(),
        level,
        labels: vec![],
        handling: vec![],
    }
}

async fn begin_run(fixture: &PublicationFixture, claim: &WorkItem) -> anyhow::Result<WorkRun> {
    let mut base = claim.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    // The runtime captures this before dispatch and retains it at completion.
    // Retry counts alone do not identify the durable lease generation.
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(elephant_storage::work_attempt_repository::fence_from_claim(
            claim,
        )?)?,
    );
    Ok(WorkRunRepository::new(fixture.client.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(claim.base.id.clone()),
            attempt: claim.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: "qualification".into(),
            worker_version: "semantic-index-fixture-v1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await?)
}

async fn finish_run(
    fixture: &PublicationFixture,
    claim: &WorkItem,
    mut run: WorkRun,
) -> anyhow::Result<()> {
    let captured = serde_json::to_value(
        elephant_storage::work_attempt_repository::fence_from_claim(claim)?,
    )?;
    anyhow::ensure!(
        run.base.space_id == claim.base.space_id
            && run.work_item.id() == &claim.base.id
            && run.attempt == claim.attempts
            && run.metrics.custom.get("attempt_fence") == Some(&captured)
            && run.completed_at.is_none()
            && !run.success,
        "fixture completion lost its original run/claim identity"
    );
    run.success = true;
    run.completed_at = Some(Utc::now());
    run.base.updated_at = Utc::now();
    let id = run.base.id.clone();
    WorkRunRepository::new(fixture.client.clone())
        .update(&fixture.space, &id, run)
        .await?;
    fixture
        .queue
        .for_attempt(claim)?
        .complete(&fixture.space, &claim.base.id)
        .await?;
    Ok(())
}

/// Split points are original UTF-8 byte offsets, selected by the test before
/// retrieval. Each span is physically embedded by the production worker.
pub async fn publish(
    client: &StorageClient,
    space: &str,
    external: &str,
    revision: &str,
    text: &str,
    security: Security,
    split_points: &[usize],
) -> anyhow::Result<()> {
    publish_with_rendered_input(
        client,
        space,
        external,
        revision,
        text,
        text,
        security,
        split_points,
        false,
    )
    .await
}

/// Actual default renderer with independently supplied expected rendered bytes
/// and selected UTF-8 split points. Neither expectation comes from the renderer.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)] // Coordinate target uses this; other targets share the fixture module.
pub async fn publish_default_text(
    client: &StorageClient,
    space: &str,
    external: &str,
    revision: &str,
    original: &str,
    expected_rendered: &str,
    security: Security,
    split_points: &[usize],
) -> anyhow::Result<()> {
    publish_with_rendered_input(
        client,
        space,
        external,
        revision,
        original,
        expected_rendered,
        security,
        split_points,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn publish_with_rendered_input(
    client: &StorageClient,
    space: &str,
    external: &str,
    revision: &str,
    text: &str,
    expected_rendered: &str,
    security: Security,
    split_points: &[usize],
    default_renderer: bool,
) -> anyhow::Result<()> {
    let configurations = SpaceConfigRepository::new(client.clone());
    let configuration = configurations
        .get_by_space_id(space)
        .await?
        .context("configured space absent")?;
    let resolved = configurations.resolve_space(space).await?;
    let profile = resolved
        .authorize(space, ModelOperation::DocumentEmbedding, None)?
        .vector_partition()?;
    let publications = SourcePublicationRepository::new(client.clone());
    let previous = publications
        .external_head(space, SourceType::Manual, external)
        .await?;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Source),
        space.into(),
        "D-index-fixture".into(),
    );
    base.security = security;
    base.subjects_state = SubjectsState::Complete;
    let (expected, authorized) = if let Some(head) = previous {
        base.id = head.source_id;
        let snapshot = publications
            .authorization_snapshot(space, &base.id)
            .await?
            .context("previous source authorization absent")?;
        let envelope = &snapshot.envelope;
        anyhow::ensure!(
            AbacEngine::new()
                .evaluate(
                    &reader(client, space).principal,
                    &RecordAttributes {
                        space_id: envelope.space_id.clone(),
                        security: envelope.security.clone(),
                        subjects: envelope.subjects.clone(),
                        subjects_state: envelope.subjects_state,
                    },
                    "write:source"
                )
                .is_allowed(),
            "fixed fixture writer cannot replace this source"
        );
        (
            SourcePrecondition::Generation(head.generation),
            Some(snapshot),
        )
    } else {
        (SourcePrecondition::Absent, None)
    };
    let mut source = Source::new(base, SourceType::Manual, digest(text));
    source.external_id = Some(external.into());
    let original_doc = RecordId::new_v4(TableName::Doc);
    source.set_manual_doc_template(&ManualDocTemplate {
        doc_id: original_doc.to_string(),
        title: "Independent index input".into(),
        text: text.into(),
        sidecar_text: None,
        media_attachments: vec![],
        rendition_key: format!("{external}/{revision}"),
        metadata: json!({}),
    })?;
    let request = AcceptSourceRevision {
        source,
        revision_id: revision.into(),
        expected,
        idempotency_key: format!("semantic-input/{external}/{revision}"),
        request_fingerprint: digest(text),
        reactivate: false,
        recreate_after_erasure: false,
        authorized,
        authorized_document: None,
    };
    let accepted = publications.accept(request.clone()).await?;
    let publication = PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: accepted.source_generation.clone(),
        revision_id: revision.into(),
        operation_id: accepted.operation_id.clone(),
    };
    let plan = source_plan_id(&publication, &resolved.token);
    let source_configuration = resolved.token;
    let source = SourceRepository::new(client.clone())
        .get(space, &accepted.source_id)
        .await?
        .context("accepted source missing")?;
    stage_source_revision(client, &source, &publication).await?;
    let doc_id = accepted_document_id(&publication, &original_doc.to_string())?;
    let doc = DocRepository::new(client.clone())
        .get(space, &doc_id)
        .await?
        .context("staged document missing")?;
    let mut renderers = RendererRegistry::new();
    if default_renderer {
        renderers.register(Box::new(DefaultTextRenderer::new()));
    } else {
        renderers.register(Box::new(PassthroughRenderer::new()));
    }
    let rendered = execute_render_doc(client, &renderers, &doc, Some("text/plain")).await?;
    anyhow::ensure!(rendered.success);
    let rendered = DocRevisionRepository::new(client.clone())
        .get(space, &rendered.revision_id)
        .await?
        .context("immutable rendered revision missing")?;
    anyhow::ensure!(
        rendered.text == expected_rendered,
        "fixture renderer differs from independently frozen rendered bytes"
    );
    let mut cuts = vec![0];
    cuts.extend_from_slice(split_points);
    cuts.push(expected_rendered.len());
    let mut nodes = Vec::new();
    for (position, pair) in cuts.windows(2).enumerate() {
        let slice = expected_rendered
            .get(pair[0]..pair[1])
            .filter(|s| !s.is_empty())
            .context("invalid fixture span")?;
        if default_renderer && slice.trim().is_empty() {
            // A separator alone is not a supported evidence candidate. The
            // selected literal interval remains unchanged and is never clipped.
            continue;
        }
        let mut base = rendered.base.clone();
        base.id = RecordId::new_v4(TableName::DocNode);
        let node = DocNodeRepository::new(client.clone())
            .create(DocNode {
                base,
                revision: Link::new(rendered.base.id.clone()),
                // Both retained lexical lookup and vector retrieval consume
                // actual Chunk nodes. A paragraph-only tree cannot exercise
                // the lexical half of the original ABA publication contract.
                role: NodeRole::Chunk,
                span: TextSpan {
                    start: u32::try_from(pair[0])?,
                    end: u32::try_from(pair[1])?,
                },
                node_key: format!("{external}/{revision}/{position}"),
                text: Some(slice.into()),
                parent: None,
                position: i32::try_from(position)?,
                depth: 0,
                embedding_model: None,
                collection: None,
            })
            .await?;
        nodes.push(node);
    }
    let queue = WorkQueue::with_defaults(client.clone());
    let mut finalizer_base = source.base.clone();
    finalizer_base.id = RecordId::new_v4(TableName::WorkItem);
    finalizer_base.provenance.sources = vec![source.base.id.clone()];
    let finalizer = queue
        .enqueue(WorkItem {
            base: finalizer_base,
            kind: "finalize_source_revision".into(),
            payload: json!({"_publication":publication,"_source_plan":plan,
                "_source_configuration":source_configuration}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("semantic-finalize/{}/{plan}", publication.operation_id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let claimed = queue.claim().await?.context("finalizer attempt absent")?;
    anyhow::ensure!(claimed.base.id == finalizer.base.id);
    let metadata = serde_json::to_value(SourceMetadataArtifactData::derive(&source))?;
    let mut candidate_data = metadata.clone();
    candidate_data["_source_plan"] = json!(plan);
    candidate_data["_source_configuration"] = json!(source_configuration);
    let mut base = source.base.clone();
    base.id = RecordId::new_v4(TableName::WorkArtifact);
    base.provenance.sources = vec![source.base.id.clone()];
    let candidate = WorkArtifact {
        base,
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "source_metadata".into(),
        data: candidate_data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let mut bundle = candidate.clone();
    bundle.base.id = RecordId::new_v4(TableName::WorkArtifact);
    bundle.artifact_type = "commit_bundle".into();
    bundle.data = json!({"publication":publication,"_source_plan":plan,"_source_configuration":source_configuration,
        "source_artifact_id":candidate.base.id.to_string(),
        "source_artifact_type":"source_metadata","bundle_kind":"source_metadata","payload":{"kind":"source_metadata","data":metadata}});
    let embedding_work_id = source_embedding_work_id_for_plan(
        space,
        &publication,
        &source_configuration,
        &rendered.base.id,
    );
    let manifest = SourcePublicationManifest {
        space_id: space.into(),
        publication: publication.clone(),
        mode: IngestMode::Retrieval,
        configuration: source_configuration.clone(),
        embedding_profile: Some(profile.clone()),
        documents: vec![SourceDocumentMapping {
            space_id: space.into(),
            doc_id,
            doc_revision_id: rendered.base.id.clone(),
            publication: publication.clone(),
        }],
        metadata_artifact_ids: vec![bundle.base.id.clone()],
        planning_artifact_ids: vec![],
        bundles: vec![],
        embeddings: nodes
            .iter()
            .map(|node| required_embedding(&profile, node))
            .collect::<Result<_, _>>()?,
        embedding_work_item_ids: vec![embedding_work_id.clone()],
    };
    let mut fixture = PublicationFixture {
        client: client.clone(),
        space: space.into(),
        request,
        accepted,
        publication: publication.clone(),
        manifest,
        configuration,
        artifacts: vec![(candidate, bundle)],
        queue,
        claimed,
    };
    fixture.manifest = SourcePublicationManifestRepository::new(client.clone())
        .set_operation_plan(fixture.manifest.clone())
        .await?;
    let stage = fixture.stage_pair_before_queue_completion(0).await?;
    let origin_run = WorkRunRepository::new(client.clone())
        .get(space, fixture.artifacts[0].0.work_run.id())
        .await?
        .context("metadata origin run missing")?;
    let origin = WorkItemRepository::new(client.clone())
        .get(space, origin_run.work_item.id())
        .await?
        .context("metadata origin work missing")?;
    for work in [&origin, &stage] {
        anyhow::ensure!(
            work.payload["_source_plan"] == plan
                && work.payload["_source_configuration"] == json!(source_configuration),
            "metadata fixture helper did not preserve its original plan context"
        );
    }
    fixture
        .queue
        .for_attempt(&stage)?
        .complete(space, &stage.base.id)
        .await?;
    let node_ids: Vec<_> = nodes.iter().map(|node| node.base.id.clone()).collect();
    let work = fixture
        .claim_fixture_work_with_id(
            "embed_source_revision",
            "semantic-index-embedding",
            json!({"_publication":publication,"revision_id":rendered.base.id.to_string(),
                "_source_plan":plan,"_source_configuration":source_configuration}),
            Some(embedding_work_id),
        )
        .await?;
    let run = begin_run(&fixture, &work).await?;
    // Follow the source runtime: capture the declared original claim before
    // inference. Generic embedding is correctly forbidden for managed nodes.
    let authority = SourcePublicationManifestRepository::new(client.clone())
        .prepare_embedding_write(
            space,
            &publication,
            &fixture.manifest.configuration,
            &elephant_storage::work_attempt_repository::fence_from_claim(&work)?,
            &rendered.base.id,
        )
        .await?;
    let (_, provider, _) = spy::context(&configurations, space)
        .embedding(ModelOperation::DocumentEmbedding)
        .await?;
    let worker = EmbedWorker::new(
        DocNodeRepository::new(client.clone()),
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        provider,
    )
    .with_source_authority(authority);
    let embedded = worker.embed_exact_nodes(space, &node_ids).await?;
    anyhow::ensure!(embedded.chunks_processed == node_ids.len());
    finish_run(&fixture, &work, run).await?;
    let finalizer_run = begin_run(&fixture, &fixture.claimed).await?;
    fixture.finalize().await?;
    finish_run(&fixture, &fixture.claimed, finalizer_run).await?;
    let head = publications
        .head(space, &publication.source_id)
        .await?
        .context("published source head absent")?;
    anyhow::ensure!(head.published_revision.as_deref() == Some(revision));
    Ok(())
}
