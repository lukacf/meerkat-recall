//! Real acceptance, staging and finalizer fixtures. No head, attempt or completion
//! receipt is inserted by hand. Metadata mode never claims graph/index coverage.

#![allow(dead_code)]

use std::collections::BTreeMap;

use chrono::Utc;
use elephant_pipeline::scheduler::ingest_work_kinds::SourceMetadataArtifactData;
use elephant_pipeline::scheduler::inline_promotion::PromotionOptions;
use elephant_pipeline::scheduler::{StagedCommit, WorkQueue};
use elephant_pipeline::source_finalization::{finalize_source_revision, register_bundle};
use elephant_storage::canonical_commit::CanonicalCommitReceipt;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::{
    SourcePublicationManifest, SourcePublicationManifestRepository,
};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{
    Repository, SourceRepository, SpaceConfigRepository, StorageClient, WorkRunRepository,
};
use elephant_types::provider_binding::SpaceProcessingConfig;
use elephant_types::publication::{OperationReceipt, PublicationFence, SourcePrecondition};
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun,
};
use elephant_types::records::{ManualDocTemplate, Source, SourceType, SpaceConfig};
use elephant_types::{Link, RecordId, TableName};

pub struct PublicationFixture {
    pub client: StorageClient,
    pub space: String,
    pub request: AcceptSourceRevision,
    pub accepted: OperationReceipt,
    pub publication: PublicationFence,
    pub manifest: SourcePublicationManifest,
    pub configuration: SpaceConfig,
    pub artifacts: Vec<(WorkArtifact, WorkArtifact)>,
    pub queue: WorkQueue,
    pub claimed: WorkItem,
}

pub type MetadataFixture = PublicationFixture;

impl PublicationFixture {
    pub async fn new(
        client: StorageClient,
        space: &str,
        bundle_count: usize,
        stage_count: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(bundle_count > 0 && stage_count <= bundle_count);
        let configuration = SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                "qualification".into(),
            ),
            embedding_provider: "fastembed".into(),
            embedding_model: "qualification-unused".into(),
            embedding_dims: 3,
            purpose: Some(format!("Metadata fixture in {space}")),
            skill: Some(format!("independent-{space}")),
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id: format!("metadata-{space}"),
                connections: BTreeMap::new(),
                profiles: BTreeMap::new(),
                bindings: BTreeMap::new(),
                active_embedding_profile_id: None,
                pending_reindex: None,
            }),
        };
        let configurations = SpaceConfigRepository::new(client.clone());
        let configuration = configurations.create(configuration).await?;
        let text = "The independent metadata source identifies the Oriole laboratory.";
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "qualification".into(),
            ),
            SourceType::Manual,
            crate::support::digest(text),
        );
        source.external_id = Some("qualification-metadata-source".into());
        source.set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Oriole metadata".into(),
            text: text.into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "qualification-metadata".into(),
            metadata: serde_json::json!({}),
        })?;
        let request = AcceptSourceRevision {
            source,
            revision_id: "metadata-r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "metadata-request-1".into(),
            request_fingerprint: crate::support::digest(text),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        };
        let accepted = SourcePublicationRepository::new(client.clone())
            .accept(request.clone())
            .await?;
        let publication = PublicationFence {
            source_id: accepted.source_id.clone(),
            generation: accepted.source_generation.clone(),
            revision_id: request.revision_id.clone(),
            operation_id: accepted.operation_id.clone(),
        };
        let source = SourceRepository::new(client.clone())
            .get(space, &accepted.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted fixture source missing"))?;
        let queue = WorkQueue::with_defaults(client.clone());
        queue
            .enqueue(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    space.into(),
                    "qualification".into(),
                ),
                kind: "finalize_source_revision".into(),
                payload: serde_json::json!({"_publication": publication}),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: format!("metadata-finalize-{space}"),
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
            .ok_or_else(|| anyhow::anyhow!("finalizer fixture claim missing"))?;
        anyhow::ensure!(claimed.base.space_id == space);
        let data = serde_json::to_value(SourceMetadataArtifactData::derive(&source))?;
        anyhow::ensure!(data["normalized_title"] == "Oriole metadata");
        let mut artifacts = Vec::with_capacity(bundle_count);
        for _ in 0..bundle_count {
            let mut base = source.base.clone();
            base.id = RecordId::new_v4(TableName::WorkArtifact);
            base.provenance.sources = vec![source.base.id.clone()];
            let candidate = WorkArtifact {
                base,
                work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
                artifact_type: "source_metadata".into(),
                data: data.clone(),
                status: ArtifactStatus::Draft,
                validation_errors: None,
                validated_at: None,
                committed_at: None,
            };
            let mut bundle = candidate.clone();
            bundle.base.id = RecordId::new_v4(TableName::WorkArtifact);
            bundle.artifact_type = "commit_bundle".into();
            bundle.data = serde_json::json!({
                "source_artifact_id": candidate.base.id.to_string(), "source_artifact_type": "source_metadata",
                "bundle_kind": "source_metadata", "publication": publication,
                "payload": {"kind": "source_metadata", "data": data},
            });
            artifacts.push((candidate, bundle));
        }
        let manifest = SourcePublicationManifest {
            space_id: space.into(),
            publication: publication.clone(),
            mode: IngestMode::Metadata,
            configuration: configurations.resolve_space(space).await?.token,
            embedding_profile: None,
            documents: vec![],
            metadata_artifact_ids: artifacts
                .iter()
                .map(|(_, bundle)| bundle.base.id.clone())
                .collect(),
            planning_artifact_ids: vec![],
            bundles: vec![],
            embeddings: vec![],
            embedding_work_item_ids: vec![],
        };
        let mut fixture = Self {
            client,
            space: space.into(),
            request,
            accepted,
            publication,
            manifest,
            configuration,
            artifacts,
            queue,
            claimed,
        };
        fixture.manifest = SourcePublicationManifestRepository::new(fixture.client.clone())
            .set_operation_plan(fixture.manifest.clone())
            .await?;
        for index in 0..stage_count {
            fixture.stage_pair(index).await?;
        }
        Ok(fixture)
    }

    pub async fn stage_pair(&self, index: usize) -> anyhow::Result<()> {
        let stage = self.stage_pair_before_queue_completion(index).await?;
        self.queue
            .for_attempt(&stage)?
            .complete(&self.space, &stage.base.id)
            .await?;
        Ok(())
    }

    /// Execute deterministic staging and real registration, then leave its queue
    /// completion outstanding. The source must not publish in this interval.
    pub async fn stage_pair_before_queue_completion(
        &self,
        index: usize,
    ) -> anyhow::Result<WorkItem> {
        let (candidate, bundle) = self
            .artifacts
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("unknown fixture artifact"))?;
        let origin_kind = if candidate.artifact_type == "knowledge_candidates" {
            "extract_node"
        } else {
            "parse_source"
        };
        let origin = self
            .claim_fixture_work(
                origin_kind,
                &format!("origin-{index}"),
                serde_json::json!({"_publication": self.publication}),
            )
            .await?;
        let run_repo = WorkRunRepository::new(self.client.clone());
        let mut run_base = candidate.base.clone();
        run_base.id = candidate.work_run.id().clone();
        let mut run = run_repo
            .create(WorkRun {
                base: run_base,
                work_item: Link::new(origin.base.id.clone()),
                attempt: origin.attempts,
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
            .await?;
        let staging = StagedCommit::new(self.client.clone());
        for artifact in [candidate, bundle] {
            staging.stage(artifact.clone()).await?;
            let expected = artifact.data.clone();
            staging
                .validate(&self.space, &artifact.base.id, move |actual| {
                    if actual.data == expected {
                        Ok(())
                    } else {
                        Err(vec!["metadata fixture bytes changed".into()])
                    }
                })
                .await?;
        }
        // Success follows the actual staging/validation writes. These are
        // repository worker transitions, not fabricated source publication.
        run.success = true;
        run.completed_at = Some(Utc::now());
        run.base.updated_at = Utc::now();
        let run_id = run.base.id.clone();
        run_repo.update(&self.space, &run_id, run).await?;
        self.queue
            .for_attempt(&origin)?
            .complete(&self.space, &origin.base.id)
            .await?;
        let stage = self.claim_fixture_work("commit_artifacts", &format!("metadata-register-{index}"), serde_json::json!({
            "_publication": self.publication, "work_run_id": candidate.work_run.id().to_string(),
        })).await?;
        register_bundle(
            &self.client,
            &self.space,
            &self.publication,
            &bundle.base.id,
            &fence_from_claim(&stage)?,
        )
        .await?;
        Ok(stage)
    }

    pub async fn claim_fixture_work(
        &self,
        kind: &str,
        key: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<WorkItem> {
        self.claim_fixture_work_with_id(kind, key, payload, None)
            .await
    }

    pub async fn claim_fixture_work_with_id(
        &self,
        kind: &str,
        key: &str,
        payload: serde_json::Value,
        id: Option<RecordId>,
    ) -> anyhow::Result<WorkItem> {
        let mut base = self.request.source.base.clone();
        base.id = id.unwrap_or_else(|| RecordId::new_v4(TableName::WorkItem));
        base.provenance.sources = vec![self.accepted.source_id.clone()];
        let mut work = WorkItem {
            base,
            kind: kind.into(),
            payload,
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("{key}-{}-{}", self.space, self.publication.operation_id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        // Inherit only the original claim's accepted source/plan, never a fresh
        // configuration read. Production helper also separates successor keys
        // by original publication/plan and strips predecessor attempt authority.
        elephant_pipeline::source_queue::inherit_source_context(&self.claimed, &mut work)?;
        let work = self.queue.enqueue(work).await?;
        let claimed = self
            .queue
            .claim()
            .await?
            .ok_or_else(|| anyhow::anyhow!("fixture predecessor claim missing"))?;
        anyhow::ensure!(
            claimed.base.id == work.base.id && claimed.kind == kind,
            "fixture claimed unrelated work"
        );
        Ok(claimed)
    }

    pub async fn finalize(&self) -> anyhow::Result<CanonicalCommitReceipt> {
        Ok(finalize_source_revision(
            &self.client,
            &self.space,
            &self.publication,
            &fence_from_claim(&self.claimed)?,
            "qualification",
            &PromotionOptions::default(),
        )
        .await?)
    }
}
