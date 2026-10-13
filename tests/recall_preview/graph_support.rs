//! Accepted source -> actual rendering/planning -> verified staged interpretation
//! -> physical indexed-vector fixture -> production whole-source publication.
//! Deterministic vectors here test storage completeness, NEVER model relevance.

#![allow(dead_code)]

use chrono::Utc;
use elephant_embeddings::PartitionManager;
use elephant_pipeline::scheduler::extraction_work_kinds::{
    execute_plan_extraction, ExtractionArtifactData, ExtractionPlanConfig, SerializedRawAssertion,
};
use elephant_pipeline::scheduler::ingest_work_kinds::SourceMetadataArtifactData;
use elephant_pipeline::scheduler::{StagedCommit, WorkQueue};
use elephant_storage::source_publication_manifest::{
    required_embedding, source_bundle_id, source_embedding_work_id, ExpectedSourceBundle,
    SourceDocumentMapping, SourcePublicationManifest, SourcePublicationManifestRepository,
};
use elephant_storage::{
    DocNodeRepository, NodeEmbeddingRepository, Repository, SpaceConfigRepository, StorageClient,
    WorkRunRepository,
};
use elephant_types::provider_binding::{
    ModelOperation, ModelProfile, OperationBinding, ProviderConnection, SpaceProcessingConfig,
};
use elephant_types::publication::PublicationFence;
use elephant_types::records::base::SubjectsState;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun,
};
use elephant_types::records::{DocNode, NodeEmbedding, SpaceConfig};
use elephant_types::{Link, RecordId, TableName};
use std::collections::BTreeMap;

use crate::publication_support::PublicationFixture;
use crate::support::{self, Lineage};

pub struct GraphFixture {
    pub publication: PublicationFixture,
    pub lineage: Lineage,
    pub nodes: Vec<DocNode>,
}

pub fn configuration(space: &str) -> SpaceConfig {
    SpaceConfig {
        base: support::classified_base(TableName::SpaceConfig, space),
        embedding_provider: "openai".into(),
        embedding_model: "fixture-storage-vector".into(),
        embedding_dims: 3,
        purpose: Some("Oriole classified graph storage oracle".into()),
        skill: Some("Preserve exact source evidence".into()),
        processing: Some(SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "qualification-policy".into(),
            connections: BTreeMap::from([(
                "fixture-connection".into(),
                ProviderConnection {
                    id: "fixture-connection".into(),
                    provider: "openai".into(),
                    account_id: "no-live-probe".into(),
                    endpoint: "https://qualification.invalid/v1".into(),
                    credential_ref: Some("UNUSED_QUALIFICATION_CREDENTIAL".into()),
                    credential_generation: 1,
                    revoked: false,
                },
            )]),
            profiles: BTreeMap::from([(
                "fixture-profile".into(),
                ModelProfile {
                    id: "fixture-profile".into(),
                    connection_id: "fixture-connection".into(),
                    model: "fixture-storage-vector".into(),
                    model_version: "deterministic-storage-only-v1".into(),
                    dimensions: Some(3),
                    preprocessing: "text-v1".into(),
                },
            )]),
            bindings: [
                ModelOperation::DocumentEmbedding,
                ModelOperation::QueryEmbedding,
            ]
            .into_iter()
            .map(|operation| {
                (
                    operation,
                    OperationBinding {
                        id: format!("fixture-{operation:?}"),
                        profile_id: "fixture-profile".into(),
                        revoked: false,
                    },
                )
            })
            .collect(),
            active_embedding_profile_id: Some("fixture-profile".into()),
            pending_reindex: None,
        }),
    }
}

fn artifact_pair(
    lineage: &Lineage,
    publication: &PublicationFence,
    kind: &str,
    data: serde_json::Value,
    bundle_id: RecordId,
) -> (WorkArtifact, WorkArtifact) {
    let mut base = lineage.source.base.clone();
    base.id = RecordId::new_v4(TableName::WorkArtifact);
    base.provenance.sources = vec![lineage.source.base.id.clone()];
    let candidate = WorkArtifact {
        base,
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: kind.into(),
        data: data.clone(),
        status: ArtifactStatus::Draft,
        validated_at: None,
        validation_errors: None,
        committed_at: None,
    };
    let mut bundle = candidate.clone();
    bundle.base.id = bundle_id;
    bundle.artifact_type = "commit_bundle".into();
    bundle.data = serde_json::json!({"publication": publication, "source_artifact_id": candidate.base.id.to_string(),
        "source_artifact_type": kind, "bundle_kind": kind, "payload":{"kind": kind, "data":data}});
    (candidate, bundle)
}

impl GraphFixture {
    /// Builds all required source work except indexed vectors. No model call.
    pub async fn new(client: StorageClient, space: &str) -> anyhow::Result<Self> {
        let lineage = support::lineage_with_configuration(
            &client,
            space,
            SubjectsState::Complete,
            configuration(space),
        )
        .await?;
        Self::from_lineage(client, space, lineage).await
    }

    /// Continue actual publication for a separately accepted source in the same space.
    pub async fn from_lineage(
        client: StorageClient,
        space: &str,
        lineage: Lineage,
    ) -> anyhow::Result<Self> {
        let configurations = SpaceConfigRepository::new(client.clone());
        let configuration = configurations
            .get_by_space_id(space)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fixture configuration absent"))?;
        let resolved = configurations.resolve_space(space).await?;
        let profile = resolved
            .authorize(space, ModelOperation::DocumentEmbedding, None)?
            .vector_partition()?;
        let publication = PublicationFence {
            source_id: lineage.accepted.source_id.clone(),
            generation: lineage.accepted.source_generation.clone(),
            revision_id: lineage.request.revision_id.clone(),
            operation_id: lineage.accepted.operation_id.clone(),
        };
        let nodes = DocNodeRepository::new(client.clone())
            .find_by_revision(&lineage.revision.base.id)
            .await?;
        anyhow::ensure!(
            nodes.len() == 1 && nodes[0].base.id == lineage.node.base.id,
            "fixture renderer/tree shape changed; update independent selections deliberately"
        );
        let raw = support::grounded_assertion(&lineage).await?;
        let extraction = ExtractionArtifactData {
            source_id: Some(lineage.source.base.id.to_string()),
            node_id: lineage.node.base.id.to_string(),
            revision_id: lineage.revision.base.id.to_string(),
            assertions: vec![SerializedRawAssertion::from_raw(&raw)?],
            tokens_used: 0,
            model: "independent-deterministic-support-fixture".into(),
            revision_text_len: u32::try_from(lineage.revision.text.len())?,
        };
        let knowledge_id = source_bundle_id(
            &publication,
            &lineage.revision.base.id,
            &lineage.node.base.id,
        );
        let artifacts = vec![
            artifact_pair(
                &lineage,
                &publication,
                "source_metadata",
                serde_json::to_value(SourceMetadataArtifactData::derive(&lineage.source))?,
                RecordId::new_v4(TableName::WorkArtifact),
            ),
            artifact_pair(
                &lineage,
                &publication,
                "knowledge_candidates",
                serde_json::to_value(extraction)?,
                knowledge_id.clone(),
            ),
        ];
        let queue = WorkQueue::with_defaults(client.clone());
        let mut base = lineage.source.base.clone();
        base.id = RecordId::new_v4(TableName::WorkItem);
        base.provenance.sources = vec![lineage.source.base.id.clone()];
        queue
            .enqueue(WorkItem {
                base,
                kind: "finalize_source_revision".into(),
                payload: serde_json::json!({"_publication": publication}),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: format!("graph-finalizer-{space}-{}", publication.operation_id),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            })
            .await?;
        let claimed = queue
            .claim()
            .await?
            .ok_or_else(|| anyhow::anyhow!("graph finalizer claim absent"))?;
        let manifest = SourcePublicationManifest {
            space_id: space.into(),
            publication: publication.clone(),
            mode: IngestMode::Graph,
            configuration: resolved.token,
            embedding_profile: Some(profile.clone()),
            documents: vec![SourceDocumentMapping {
                space_id: space.into(),
                doc_id: lineage.doc.base.id.clone(),
                doc_revision_id: lineage.revision.base.id.clone(),
                publication: publication.clone(),
            }],
            metadata_artifact_ids: vec![artifacts[0].1.base.id.clone()],
            planning_artifact_ids: vec![],
            bundles: vec![ExpectedSourceBundle {
                artifact_id: knowledge_id,
                node_id: lineage.node.base.id.clone(),
                node_ids: vec![lineage.node.base.id.clone()],
                revision_id: lineage.revision.base.id.clone(),
                span_start: 0,
                span_end: u32::try_from(lineage.revision.text.len())?,
            }],
            embeddings: nodes
                .iter()
                .map(|node| required_embedding(&profile, node))
                .collect::<Result<Vec<_>, _>>()?,
            embedding_work_item_ids: vec![source_embedding_work_id(
                space,
                &publication,
                &lineage.revision.base.id,
            )],
        };
        let fixture = PublicationFixture {
            client,
            space: space.into(),
            request: lineage.request.clone(),
            accepted: lineage.accepted.clone(),
            publication,
            manifest,
            configuration,
            artifacts,
            queue,
            claimed,
        };
        let mut graph = Self {
            publication: fixture,
            lineage,
            nodes,
        };
        let planning_claim = graph.publication.claim_fixture_work("plan_extraction", "graph-plan", serde_json::json!({"_publication": graph.publication.publication, "revision_id":graph.lineage.revision.base.id.to_string()})).await?;
        let run = graph.start_run(&planning_claim).await?;
        let plan = execute_plan_extraction(
            &graph.publication.client,
            &graph.lineage.revision.base.id,
            "qualification-plan-v1",
            &ExtractionPlanConfig::default(),
            &run.base.id,
            space,
            "qualification",
        )
        .await?;
        anyhow::ensure!(plan.plan.node_selections.len() == 1);
        let selection = &plan.plan.node_selections[0];
        anyhow::ensure!(
            selection.node_id == graph.lineage.node.base.id
                && selection.span_start == 0
                && usize::try_from(selection.span_end)? == graph.lineage.revision.text.len()
        );
        anyhow::ensure!(plan
            .plan
            .coverage
            .as_ref()
            .is_some_and(|coverage| coverage.complete
                && coverage.revision_text_hash == support::digest(&graph.lineage.revision.text)));
        let expected = serde_json::to_value(&plan.plan)?;
        StagedCommit::new(graph.publication.client.clone())
            .validate(space, &plan.artifact_id, move |artifact| {
                if artifact.data == expected {
                    Ok(())
                } else {
                    Err(vec!["persisted extraction plan changed".into()])
                }
            })
            .await?;
        graph.finish_run(run, &planning_claim).await?;
        graph.publication.manifest.planning_artifact_ids = vec![plan.artifact_id];
        graph.publication.manifest =
            SourcePublicationManifestRepository::new(graph.publication.client.clone())
                .set_operation_plan(graph.publication.manifest.clone())
                .await?;
        graph.publication.stage_pair(0).await?;
        graph.publication.stage_pair(1).await?;
        Ok(graph)
    }

    async fn start_run(&self, claim: &WorkItem) -> anyhow::Result<WorkRun> {
        let mut base = claim.base.clone();
        base.id = RecordId::new_v4(TableName::WorkRun);
        Ok(WorkRunRepository::new(self.publication.client.clone())
            .create(WorkRun {
                base,
                work_item: Link::new(claim.base.id.clone()),
                attempt: claim.attempts,
                started_at: Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics: Default::default(),
                worker_id: "qualification".into(),
                worker_version: "fixture-v1".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await?)
    }

    async fn finish_run(&self, mut run: WorkRun, claim: &WorkItem) -> anyhow::Result<()> {
        run.success = true;
        run.completed_at = Some(Utc::now());
        run.base.updated_at = Utc::now();
        let id = run.base.id.clone();
        WorkRunRepository::new(self.publication.client.clone())
            .update(&self.publication.space, &id, run)
            .await?;
        self.publication
            .queue
            .for_attempt(claim)?
            .complete(&self.publication.space, &claim.base.id)
            .await?;
        Ok(())
    }

    /// Persist deterministic finite vectors through the actual index repository,
    /// then finish the exact declared embedding work. This is NOT real-provider
    /// evidence and must never be fed to the semantic acceptance evaluator.
    pub async fn index_fixture_vectors(&self) -> anyhow::Result<()> {
        let fixture = &self.publication;
        let profile = fixture
            .manifest
            .embedding_profile
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("embedding profile missing"))?;
        let claim = fixture.claim_fixture_work_with_id("embed_source_revision", "graph-storage-embedding",
            serde_json::json!({"_publication":fixture.publication,"revision_id":self.lineage.revision.base.id.to_string()}),
            Some(source_embedding_work_id(&fixture.space, &fixture.publication, &self.lineage.revision.base.id))).await?;
        let run = self.start_run(&claim).await?;
        let model = format!("recall:{}", serde_json::to_string(profile)?);
        let embeddings = self
            .nodes
            .iter()
            .map(|node| {
                let mut base = node.base.clone();
                base.id = RecordId::new_v4(TableName::ChunkEmbedding);
                NodeEmbedding {
                    base,
                    node: Link::new(node.base.id.clone()),
                    model: model.clone(),
                    dims: 3,
                    vector: vec![1.0, 0.0, 0.0],
                }
            })
            .collect();
        NodeEmbeddingRepository::new(fixture.client.clone(), PartitionManager::new())
            .store_embeddings_and_mark_nodes(
                &fixture.space,
                &self
                    .nodes
                    .iter()
                    .map(|node| node.base.id.clone())
                    .collect::<Vec<_>>(),
                &model,
                Utc::now(),
                embeddings,
            )
            .await?;
        self.finish_run(run, &claim).await
    }
}
