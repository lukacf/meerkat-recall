//! Expected-state and fault/restart cases for the actual manifest repository.
//! Vectors below are deterministic storage fixtures, not provider qualification.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::StorageError;
use elephant_embeddings::PartitionManager;
use elephant_types::provider_binding::{
    ModelProfile, OperationBinding, ProviderConnection, SpaceProcessingConfig,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::TextSpan;
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::Link;

#[path = "source_publication_standing_tests.rs"]
mod standing_original_producer;

#[path = "source_publication_native_fence_tests.rs"]
mod native_input_fences;

#[path = "source_publication_inputs_tests.rs"]
mod declared_inputs;

#[path = "source_publication_progress_tests.rs"]
mod original_progress;

#[path = "source_lifecycle_merge_tests.rs"]
mod causal_merge_erasure;

struct Fixture {
    storage: StorageClient,
    manifest: SourcePublicationManifest,
    nodes: Vec<DocNode>,
}

fn base(table: TableName, space: &str) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        space.into(),
        "manifest-fixture".into(),
    )
}

fn processing() -> SpaceProcessingConfig {
    SpaceProcessingConfig {
        generation: 1,
        index_generation: 0,
        policy_id: "fixture-policy".into(),
        connections: BTreeMap::from([(
            "connection".into(),
            ProviderConnection {
                id: "connection".into(),
                provider: "openai".into(),
                account_id: "fixture-account".into(),
                endpoint: "https://example.test/v1".into(),
                credential_ref: Some("UNUSED_FIXTURE_SECRET".into()),
                credential_generation: 1,
                revoked: false,
            },
        )]),
        profiles: BTreeMap::from([(
            "profile".into(),
            ModelProfile {
                id: "profile".into(),
                connection_id: "connection".into(),
                model: "fixture-embedding".into(),
                model_version: "fixture-v1".into(),
                dimensions: Some(3),
                preprocessing: "text-v1".into(),
                summary: None,
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
                    id: format!("{operation:?}"),
                    profile_id: "profile".into(),
                    revoked: false,
                },
            )
        })
        .collect(),
        active_embedding_profile_id: Some("profile".into()),
        pending_reindex: None,
        completed_embedding_reindex: None,
    }
}

async fn client(embedded: bool) -> StorageClient {
    if embedded {
        return StorageClient::new_embedded().await.unwrap();
    }
    let storage = StorageClient::new(crate::StorageConfig {
        database: format!("source_manifest_{}", Uuid::new_v4().simple()),
        ..crate::StorageConfig::default()
    })
    .await
    .unwrap();
    // Install the complete native boundaries explicitly. Ordinary constructors
    // never mutate schema, and source acceptance/finalization needs real control
    // records and the same reference guards used by the retained runtime.
    for migration in [
        concat!(
            include_str!("../../migrations/sql/031_source_publication.surql"),
            "\n",
            include_str!("../../migrations/sql/046_source_support_index_fence.surql")
        ),
        include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
        include_str!("../../migrations/sql/037_source_publication_manifest_history.surql"),
        include_str!("../../migrations/sql/039_private_reference_fence.surql"),
        include_str!("../../migrations/sql/043_native_read_fence.surql"),
    ] {
        storage.query_checked(migration).await.unwrap();
    }
    storage
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn fixture(
    storage: StorageClient,
    space: &str,
    mode: IngestMode,
    node_count: usize,
) -> Fixture {
    fixture_with_registration(storage, space, mode, node_count, true).await
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn fixture_with_registration(
    storage: StorageClient,
    space: &str,
    mode: IngestMode,
    node_count: usize,
    complete_registration: bool,
) -> Fixture {
    fixture_with_embedding_waits(
        storage,
        space,
        mode,
        node_count,
        complete_registration,
        &[],
        true,
    )
    .await
}

// Vector-positive cases retain their actual live claim until the final batch
// is written through the source-authorized repository. Other fault fixtures
// still finish work without vectors to exercise incomplete readiness.
async fn embedding_fixture(
    storage: StorageClient,
    space: &str,
    mode: IngestMode,
    node_count: usize,
) -> Fixture {
    Box::pin(fixture_with_embedding_waits(
        storage,
        space,
        mode,
        node_count,
        true,
        &[],
        false,
    ))
    .await
}

#[derive(Clone, Copy)]
enum EmbeddingWait {
    Dependency,
    Configuration,
}

#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn fixture_with_embedding_waits(
    storage: StorageClient,
    space: &str,
    mode: IngestMode,
    node_count: usize,
    complete_registration: bool,
    embedding_waits: &[EmbeddingWait],
    finish_embedding: bool,
) -> Fixture {
    let configuration = SpaceConfig {
        base: base(TableName::SpaceConfig, space),
        embedding_provider: "openai".into(),
        embedding_model: "fixture-embedding".into(),
        embedding_dims: 3,
        purpose: None,
        skill: None,
        processing: Some(processing()),
    };
    crate::SpaceConfigRepository::new(storage.clone())
        .create(configuration.clone())
        .await
        .unwrap();
    use crate::source_publication::AcceptSourceRevision;
    use elephant_types::publication::SourcePrecondition;
    let mut source = Source::new(
        base(TableName::Source, space),
        SourceType::Manual,
        "fixture-content-hash".into(),
    );
    let publications = SourcePublicationRepository::new(storage.clone());
    let mut previous_source = source.clone();
    previous_source.set_requested_ingest_mode(IngestMode::Metadata);
    let previous = publications
        .accept(AcceptSourceRevision {
            source: previous_source,
            revision_id: "revision-1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "manifest-previous-acceptance".into(),
            request_fingerprint: "manifest-previous-request".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .unwrap();
    let previous_manifest = stage_actual_metadata_acceptance(
        &storage,
        &previous,
        crate::SpaceConfigRepository::new(storage.clone())
            .resolve_space(space)
            .await
            .unwrap()
            .token,
    )
    .await;
    publish_actual_metadata_acceptance(&storage, &previous_manifest).await;
    // Preserve the prior-published-state oracle using actual acceptance and
    // completion, not a head/operation fabricated independently of admission.
    source.set_requested_ingest_mode(mode);
    let accepted = publications
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "revision-2".into(),
            expected: SourcePrecondition::Generation(previous.source_generation),
            idempotency_key: "manifest-current-acceptance".into(),
            request_fingerprint: "manifest-current-request".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: publications
                .authorization_snapshot(space, &source.base.id)
                .await
                .unwrap(),
            authorized_document: None,
        })
        .await
        .unwrap();
    let publication = PublicationFence {
        source_id: accepted.source_id,
        generation: accepted.source_generation,
        revision_id: accepted.desired_revision.unwrap(),
        operation_id: accepted.operation_id,
    };
    assert_eq!(accepted.published_revision.as_deref(), Some("revision-1"));
    let resolved = crate::SpaceConfigRepository::new(storage.clone())
        .resolve_space(space)
        .await
        .unwrap();
    let profile = resolved
        .authorize(space, ModelOperation::DocumentEmbedding, None)
        .unwrap()
        .vector_partition()
        .unwrap();
    let mut manifest = SourcePublicationManifest {
        historical_adoption: None,
        space_id: space.into(),
        publication: publication.clone(),
        mode,
        configuration: resolved.token,
        embedding_profile: (!matches!(mode, IngestMode::Skip | IngestMode::Metadata))
            .then_some(profile.clone()),
        documents: Vec::new(),
        metadata_artifact_ids: Vec::new(),
        planning_artifact_ids: Vec::new(),
        bundles: Vec::new(),
        embeddings: Vec::new(),
        embedding_work_item_ids: Vec::new(),
    };
    let metadata_candidate = artifact(
        space,
        "source_metadata",
        serde_json::json!({ "source_id": source.base.id.to_string() }),
    );
    crate::WorkArtifactRepository::new(storage.clone())
        .create(metadata_candidate.clone())
        .await
        .unwrap();
    let mut metadata = artifact(
        space,
        "commit_bundle",
        serde_json::json!({
            "publication": publication, "source_artifact_id": metadata_candidate.base.id.to_string(),
            "source_artifact_type": "source_metadata", "bundle_kind": "source_metadata",
            "payload": { "kind": "source_metadata", "data": metadata_candidate.data },
        }),
    );
    metadata.work_run = metadata_candidate.work_run;
    manifest
        .metadata_artifact_ids
        .push(metadata.base.id.clone());
    create_successful_origin(&storage, &metadata).await;
    crate::WorkArtifactRepository::new(storage.clone())
        .create(metadata.clone())
        .await
        .unwrap();
    if complete_registration {
        register_completed(&storage, &manifest, &metadata).await;
    } else {
        register_running(&storage, &manifest, &metadata).await;
    }
    let mut nodes = Vec::new();
    if !matches!(mode, IngestMode::Skip | IngestMode::Metadata) {
        let text = vec!["one"; node_count].join(" ");
        let doc = Doc {
            base: base(TableName::Doc, space),
            title: "fixture".into(),
            text: text.clone(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id)),
            media_attachments: Vec::new(),
            rendition_key: Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
        };
        crate::DocRepository::new(storage.clone())
            .create(doc.clone())
            .await
            .unwrap();
        let revision = DocRevision {
            base: base(TableName::DocRevision, space),
            doc: Link::new(doc.base.id.clone()),
            text: text.clone(),
            text_hash: format!("{:x}", Sha256::digest(text.as_bytes())),
            renderer: RendererMetadata {
                name: "fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        crate::DocRevisionRepository::new(storage.clone())
            .create(revision.clone())
            .await
            .unwrap();
        manifest.documents.push(SourceDocumentMapping {
            space_id: space.into(),
            doc_id: doc.base.id,
            doc_revision_id: revision.base.id.clone(),
            publication: publication.clone(),
        });
        manifest
            .embedding_work_item_ids
            .push(source_embedding_work_id(
                space,
                &publication,
                &revision.base.id,
            ));
        complete_embedding_work(
            &storage,
            &manifest,
            &revision.base.id,
            embedding_waits,
            finish_embedding,
        )
        .await;
        for index in 0..node_count {
            let start = u32::try_from(index * 4).unwrap();
            let node = DocNode {
                base: base(TableName::DocNode, space),
                revision: Link::new(revision.base.id.clone()),
                role: NodeRole::Chunk,
                span: TextSpan::new(start, start + 3),
                node_key: Uuid::new_v4().to_string(),
                text: None,
                parent: None,
                position: 0,
                depth: 0,
                embedding_model: None,
                collection: None,
            };
            crate::DocNodeRepository::new(storage.clone())
                .create(node.clone())
                .await
                .unwrap();
            if mode == IngestMode::Graph {
                manifest.bundles.push(ExpectedSourceBundle {
                    artifact_id: source_bundle_id(&publication, &revision.base.id, &node.base.id),
                    node_id: node.base.id.clone(),
                    node_ids: vec![node.base.id.clone()],
                    revision_id: revision.base.id.clone(),
                    span_start: start,
                    span_end: start + 3,
                });
            }
            manifest
                .embeddings
                .push(required_embedding(&profile, &node).unwrap());
            nodes.push(node);
        }
        if mode == IngestMode::Graph {
            let plan = artifact(
                space,
                "extraction_plan",
                serde_json::json!({
                    "revision_id": revision.base.id,
                    "coverage": expected_coverage(&manifest, &revision).unwrap(),
                    "node_selections": manifest.bundles.iter().map(|bundle| serde_json::json!({
                        "node_id": bundle.node_id, "node_ids": bundle.node_ids,
                        "span_start": bundle.span_start, "span_end": bundle.span_end,
                    })).collect::<Vec<_>>(),
                }),
            );
            manifest.planning_artifact_ids.push(plan.base.id.clone());
            create_successful_origin(&storage, &plan).await;
            crate::WorkArtifactRepository::new(storage.clone())
                .create(plan)
                .await
                .unwrap();
        }
    }
    manifest.normalize();
    Fixture {
        storage,
        manifest,
        nodes,
    }
}

fn artifact(space: &str, kind: &str, data: serde_json::Value) -> WorkArtifact {
    WorkArtifact {
        base: base(TableName::WorkArtifact, space),
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: kind.into(),
        data,
        status: ArtifactStatus::Validated,
        validation_errors: None,
        validated_at: Some(Utc::now()),
        committed_at: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn stage_bundle(fixture: &Fixture, index: usize) {
    let expected = &fixture.manifest.bundles[index];
    let candidate = artifact(
        &fixture.manifest.space_id,
        "knowledge_candidates",
        serde_json::json!({
            "source_id": fixture.manifest.publication.source_id.to_string(),
            "node_id": expected.node_id.to_string(), "revision_id": expected.revision_id.to_string(),
            "assertions": [], "tokens_used": 0, "model": "deterministic-fixture", "revision_text_len": fixture.nodes.len() * 4 - 1,
        }),
    );
    let mut bundle = artifact(
        &fixture.manifest.space_id,
        "commit_bundle",
        serde_json::json!({
            "publication": fixture.manifest.publication, "bundle_kind": "knowledge_candidates",
            "source_artifact_id": candidate.base.id.to_string(), "source_artifact_type": "knowledge_candidates",
            "payload": { "kind": "knowledge_candidates", "data": candidate.data },
        }),
    );
    bundle.base.id = expected.artifact_id.clone();
    bundle.work_run = candidate.work_run.clone();
    let repo = crate::WorkArtifactRepository::new(fixture.storage.clone());
    create_successful_origin(&fixture.storage, &bundle).await;
    repo.create(candidate).await.unwrap();
    repo.create(bundle.clone()).await.unwrap();
    register_completed(&fixture.storage, &fixture.manifest, &bundle).await;
}

fn work_item(space: &str, kind: &str, payload: serde_json::Value) -> WorkItem {
    WorkItem {
        base: base(TableName::WorkItem, space),
        kind: kind.into(),
        payload,
        status: WorkItemStatus::Runnable,
        priority: 0,
        idempotency_key: Uuid::new_v4().to_string(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

async fn create_successful_origin(storage: &StorageClient, artifact: &WorkArtifact) -> WorkItem {
    create_successful_origin_with_payload(storage, artifact, serde_json::json!({})).await
}

async fn create_successful_origin_with_payload(
    storage: &StorageClient,
    artifact: &WorkArtifact,
    payload: serde_json::Value,
) -> WorkItem {
    let mut item = work_item(&artifact.base.space_id, "fixture_origin", payload);
    item.status = WorkItemStatus::Done;
    crate::WorkItemRepository::new(storage.clone())
        .create(item.clone())
        .await
        .unwrap();
    let mut run_base = base(TableName::WorkRun, &artifact.base.space_id);
    run_base.id = artifact.work_run.id().clone();
    let run = WorkRun {
        base: run_base,
        work_item: Link::new(item.base.id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: elephant_types::records::work::WorkRunMetrics::default(),
        worker_id: "fixture".into(),
        worker_version: "1".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    crate::WorkRunRepository::new(storage.clone())
        .create(run)
        .await
        .unwrap();
    item
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn complete_embedding_work(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
    revision: &RecordId,
    waits: &[EmbeddingWait],
    finish: bool,
) {
    let mut item = work_item(
        &manifest.space_id,
        "embed_source_revision",
        serde_json::json!({"_publication": manifest.publication, "revision_id": revision.to_string()}),
    );
    item.base.id = source_embedding_work_id(&manifest.space_id, &manifest.publication, revision);
    let items = crate::WorkItemRepository::new(storage.clone());
    let runs = crate::WorkRunRepository::new(storage.clone());
    items.create(item.clone()).await.unwrap();
    for index in 0..=waits.len() {
        let claim = items
            .claim_with_lease(
                &manifest.space_id,
                &item.base.id,
                "fixture-embedding",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let fence = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
        assert_eq!(claim.attempts, 1, "waits refund the retry allowance");
        assert_eq!(fence.generation, u64::try_from(index + 1).unwrap());
        if index == waits.len() && !finish {
            // Keep the original successful attempt live for the source vector
            // transaction; do not invent a completed run before persistence.
            return;
        }
        let mut run = WorkRun {
            base: base(TableName::WorkRun, &manifest.space_id),
            work_item: Link::new(item.base.id.clone()),
            attempt: claim.attempts,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: index == waits.len(),
            error: (index < waits.len()).then(|| "fixture local wait".into()),
            metrics: elephant_types::records::work::WorkRunMetrics::default(),
            worker_id: "fixture".into(),
            worker_version: "1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        run.metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&fence).unwrap(),
        );
        runs.create(run).await.unwrap();
        let Some(wait) = waits.get(index) else {
            complete_claim(storage, &claim).await;
            break;
        };
        let token = claim.leased_by.as_deref().unwrap();
        let mut pending = match wait {
            EmbeddingWait::Dependency => {
                items
                    .requeue_pending_dependencies(
                        &manifest.space_id,
                        &item.base.id,
                        token,
                        5,
                        "fixture dependency".into(),
                    )
                    .await
            }
            EmbeddingWait::Configuration => {
                items
                    .requeue_configuration_blocked(
                        &manifest.space_id,
                        &item.base.id,
                        token,
                        30,
                        "fixture configuration".into(),
                    )
                    .await
            }
        }
        .unwrap()
        .unwrap();
        assert_eq!(pending.attempts, 0);
        assert!(pending.run_after.unwrap() > Utc::now());
        // Advance only the fixture's eligibility timestamp; the real owned
        // requeue above persisted the refund and retained durable generation.
        pending.run_after = Some(Utc::now() - chrono::Duration::seconds(1));
        items
            .update(&manifest.space_id, &item.base.id, pending)
            .await
            .unwrap();
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn register_running(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
    artifact: &WorkArtifact,
) -> WorkItem {
    let repo = crate::WorkItemRepository::new(storage.clone());
    let item = work_item(
        &manifest.space_id,
        "commit_artifacts",
        serde_json::json!({
            "_publication": manifest.publication, "work_run_id": artifact.work_run.id().to_string(),
        }),
    );
    repo.create(item.clone()).await.unwrap();
    let claimed = repo
        .claim_with_lease(
            &manifest.space_id,
            &item.base.id,
            "fixture-commit",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::work_attempt_repository::fence_from_claim(&claimed).unwrap();
    SourcePublicationManifestRepository::new(storage.clone())
        .register_bundle(
            &manifest.space_id,
            &manifest.publication,
            &artifact.base.id,
            &attempt,
        )
        .await
        .unwrap();
    claimed
}

async fn complete_claim(storage: &StorageClient, item: &WorkItem) {
    crate::WorkItemRepository::new(storage.clone())
        .update_running_for_worker(
            &item.base.space_id,
            &item.base.id,
            item.leased_by.as_deref().unwrap(),
            WorkItemStatus::Done,
            None,
        )
        .await
        .unwrap()
        .unwrap();
}

async fn register_completed(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
    artifact: &WorkArtifact,
) -> WorkItem {
    let item = register_running(storage, manifest, artifact).await;
    complete_claim(storage, &item).await;
    item
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original claim and repository owners through vector persistence and completion."
)]
async fn embed(fixture: &Fixture, index: usize, vector: Vec<f32>) {
    let node = &fixture.nodes[index];
    let space = &fixture.manifest.space_id;
    let model = embedding_model(&fixture.manifest).unwrap();
    let items = crate::WorkItemRepository::new(fixture.storage.clone());
    let claim = items
        .get(
            space,
            &fixture
                .manifest
                .embedding_work_id(node.revision.id())
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.status, WorkItemStatus::Running);
    let fence = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
    let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
    let authority = Box::pin(repository.prepare_embedding_write(
        space,
        &fixture.manifest.publication,
        &fixture.manifest.configuration,
        &fence,
        node.revision.id(),
    ))
    .await
    .unwrap();
    let mut embedding_base = node.base.clone();
    embedding_base.id = RecordId::new_v4(TableName::ChunkEmbedding);
    let embedding = NodeEmbedding {
        base: embedding_base,
        node: Link::new(node.base.id.clone()),
        model: model.clone(),
        dims: 3,
        vector,
    };
    crate::NodeEmbeddingRepository::new(fixture.storage.clone(), PartitionManager::new())
        .store_source_embeddings_and_mark_nodes(
            space,
            std::slice::from_ref(&node.base.id),
            &model,
            Utc::now(),
            vec![embedding],
            &authority,
        )
        .await
        .unwrap();
    if index + 1 == fixture.nodes.len() {
        let mut run_base = claim.base.clone();
        run_base.id = RecordId::new_v4(TableName::WorkRun);
        let mut run = WorkRun {
            base: run_base,
            work_item: Link::new(claim.base.id.clone()),
            attempt: claim.attempts,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
            error: None,
            metrics: elephant_types::records::work::WorkRunMetrics::default(),
            worker_id: "fixture".into(),
            worker_version: "1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        run.metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&fence).unwrap(),
        );
        crate::WorkRunRepository::new(fixture.storage.clone())
            .create(run)
            .await
            .unwrap();
        complete_claim(&fixture.storage, &claim).await;
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn finished_subset_cannot_publish_or_shrink_sealed_expected_set() {
    for embedded in [true, false] {
        let fixture = embedding_fixture(
            client(embedded).await,
            "quiet-business",
            IngestMode::Graph,
            2,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        stage_bundle(&fixture, 0).await;
        embed(&fixture, 0, vec![1.0, 0.0, 0.0]).await;
        let requirements = repo
            .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap();
        assert!(repo.check_ready(&requirements).await.is_err());
        let mut truncated = fixture.manifest.clone();
        truncated.bundles.pop();
        truncated.embeddings.pop();
        assert!(repo.set_operation_plan(truncated).await.is_err());
        assert_eq!(
            repo.get(&fixture.manifest.space_id, &fixture.manifest.publication)
                .await
                .unwrap(),
            Some(fixture.manifest.clone())
        );
        assert_eq!(
            SourcePublicationRepository::new(fixture.storage.clone())
                .head(
                    &fixture.manifest.space_id,
                    &fixture.manifest.publication.source_id
                )
                .await
                .unwrap()
                .unwrap()
                .published_revision
                .as_deref(),
            Some("revision-1")
        );
        stage_bundle(&fixture, 1).await;
        assert!(repo.check_ready(&requirements).await.is_err());
        embed(&fixture, 1, vec![0.0, 1.0, 0.0]).await;
        repo.check_ready(&requirements).await.unwrap();
        // Readiness alone neither moves the source head nor commits any bundle.
        assert!(repo
            .completion(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn missing_bundle_and_unvalidated_bundle_have_distinct_readiness_failures() {
    for embedded in [true, false] {
        let fixture = embedding_fixture(
            client(embedded).await,
            "bundle-readiness",
            IngestMode::Graph,
            1,
        )
        .await;
        let manifests = SourcePublicationManifestRepository::new(fixture.storage.clone());
        manifests
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let space = &fixture.manifest.space_id;
        let proof = manifests
            .requirements(space, &fixture.manifest.publication)
            .await
            .unwrap();
        let before = SourcePublicationRepository::new(fixture.storage.clone())
            .head(space, &fixture.manifest.publication.source_id)
            .await
            .unwrap();
        let missing = manifests.check_ready(&proof).await.unwrap_err();
        assert!(
            matches!(missing, StorageError::Conflict(ref reason)
            if reason == "source_publication_incomplete:expected_bundle_missing"),
            "{missing}"
        );

        // These are explicit storage-boundary fixtures, not a claim that the
        // real extraction/validation dispatcher ran. Keep all other required
        // facts valid so each negative isolates one persisted bundle condition.
        stage_bundle(&fixture, 0).await;
        embed(&fixture, 0, vec![1.0, 0.0, 0.0]).await;
        manifests.check_ready(&proof).await.unwrap();
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.bundles[0].artifact_id;
        let original = artifacts.get(space, id).await.unwrap().unwrap();
        for mutation in 0..3 {
            let mut broken = original.clone();
            match mutation {
                0 => broken.validated_at = None,
                1 => broken.status = ArtifactStatus::Draft,
                _ => broken.validation_errors = Some(vec!["fixture rejected evidence".into()]),
            }
            artifacts.update(space, id, broken).await.unwrap();
            let unvalidated = manifests.check_ready(&proof).await.unwrap_err();
            assert!(
                matches!(unvalidated, StorageError::Conflict(ref reason)
                if reason == "source_publication_incomplete:bundle_not_validated"),
                "{unvalidated}"
            );
        }
        artifacts.update(space, id, original).await.unwrap();
        manifests.check_ready(&proof).await.unwrap();
        assert_eq!(
            SourcePublicationRepository::new(fixture.storage.clone())
                .head(space, &fixture.manifest.publication.source_id)
                .await
                .unwrap(),
            before
        );
        assert!(manifests
            .completion(space, &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn metadata_and_explicit_zero_graph_have_distinct_closed_requirements() {
    for embedded in [true, false] {
        let metadata = fixture(
            client(embedded).await,
            "metadata-space",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(metadata.storage.clone());
        repo.set_operation_plan(metadata.manifest.clone())
            .await
            .unwrap();
        repo.check_ready(
            &repo
                .requirements("metadata-space", &metadata.manifest.publication)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let graph = fixture(
            client(embedded).await,
            "empty-graph-space",
            IngestMode::Graph,
            0,
        )
        .await;
        let mut accidental_empty = graph.manifest.clone();
        accidental_empty.planning_artifact_ids.clear();
        assert!(accidental_empty.validate().is_err());
        let repo = SourcePublicationManifestRepository::new(graph.storage.clone());
        repo.set_operation_plan(graph.manifest.clone())
            .await
            .unwrap();
        repo.check_ready(
            &repo
                .requirements("empty-graph-space", &graph.manifest.publication)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn model_marker_without_real_vector_is_incomplete() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "retrieval-space",
            IngestMode::Retrieval,
            1,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let mut node = fixture.nodes[0].clone();
        node.embedding_model = Some(embedding_model(&fixture.manifest).unwrap());
        crate::DocNodeRepository::new(fixture.storage.clone())
            .update("retrieval-space", &node.base.id.clone(), node)
            .await
            .unwrap();
        let requirements = repo
            .requirements("retrieval-space", &fixture.manifest.publication)
            .await
            .unwrap();
        let error = repo.check_ready(&requirements).await.unwrap_err();
        assert!(
            error.to_string().contains("required_embedding_missing"),
            "completed work plus a marker must fail for its missing vector: {error}"
        );
        // The positive control uses a separate actual live source claim. A
        // completed claim from the negative fixture cannot authorize a write.
        let positive = embedding_fixture(
            client(embedded).await,
            "retrieval-vector-present",
            IngestMode::Retrieval,
            1,
        )
        .await;
        let positive_repo = SourcePublicationManifestRepository::new(positive.storage.clone());
        positive_repo
            .set_operation_plan(positive.manifest.clone())
            .await
            .unwrap();
        embed(&positive, 0, vec![1.0, 0.0, 0.0]).await;
        let positive_requirements = positive_repo
            .requirements(&positive.manifest.space_id, &positive.manifest.publication)
            .await
            .unwrap();
        positive_repo
            .check_ready(&positive_requirements)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn missing_enqueue_is_recovered_from_snapshot_in_every_named_space() {
    let directory = tempfile::tempdir().unwrap();
    let journal = directory.path().join("original-journal");
    let snapshot = directory.path().join("source-backup.snapshot");
    let config = crate::StorageConfig {
        backend: crate::StorageBackendKind::ElephantDb,
        ..crate::StorageConfig::default()
    };
    let (first_manifest, second_manifest) = {
        let storage =
            StorageClient::new_with_durability(config.clone(), None, Some(journal.clone()))
                .await
                .unwrap();
        let first = fixture(storage.clone(), "quiet-one", IngestMode::Graph, 1).await;
        let second = fixture(storage.clone(), "quiet-two", IngestMode::Retrieval, 1).await;
        let repo = SourcePublicationManifestRepository::new(storage.clone());
        repo.set_operation_plan(first.manifest.clone())
            .await
            .unwrap();
        repo.set_operation_plan(second.manifest.clone())
            .await
            .unwrap();
        storage.export_snapshot(snapshot.clone()).await.unwrap();
        (first.manifest, second.manifest)
    };
    let restored = StorageClient::new_with_durability(config, None, Some(journal))
        .await
        .unwrap();
    restored.import_snapshot(snapshot).await.unwrap();
    let repo = SourcePublicationManifestRepository::new(restored);
    let pending = repo.pending_all().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].bundles, first_manifest.bundles);
    assert_eq!(pending[1].embeddings, second_manifest.embeddings);
    for manifest in pending {
        assert!(repo
            .check_ready(
                &repo
                    .requirements(&manifest.space_id, &manifest.publication)
                    .await
                    .unwrap()
            )
            .await
            .is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn old_revision_mapping_survives_pending_replacement_and_wrong_space_is_empty() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "mapped-space",
            IngestMode::Retrieval,
            0,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let mapping = &fixture.manifest.documents[0];
        assert_eq!(
            repo.source_revision_for_document("mapped-space", &mapping.doc_id)
                .await
                .unwrap(),
            Some(mapping.clone())
        );
        assert_eq!(
            repo.source_revision_for_document_revision("mapped-space", &mapping.doc_revision_id)
                .await
                .unwrap(),
            Some(mapping.clone())
        );
        assert!(repo
            .source_revision_for_document_revision("other-space", &mapping.doc_revision_id)
            .await
            .unwrap()
            .is_none());
        let mut replacement = fixture.manifest.clone();
        replacement.publication.generation = "new-accepted-generation".into();
        replacement.publication.revision_id = "revision-3".into();
        for map in &mut replacement.documents {
            map.publication = replacement.publication.clone();
        }
        assert!(repo.set_operation_plan(replacement).await.is_err());
        assert_eq!(
            repo.source_revision_for_document_revision("mapped-space", &mapping.doc_revision_id)
                .await
                .unwrap(),
            Some(mapping.clone())
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn source_deletion_and_config_revocation_after_preflight_block_completion() {
    let fixture =
        embedding_fixture(client(true).await, "racing-space", IngestMode::Retrieval, 1).await;
    let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
    repo.set_operation_plan(fixture.manifest.clone())
        .await
        .unwrap();
    embed(&fixture, 0, vec![1.0, 0.0, 0.0]).await;
    let requirements = repo
        .requirements("racing-space", &fixture.manifest.publication)
        .await
        .unwrap();
    repo.check_ready(&requirements).await.unwrap();
    let store = fixture.storage.embedded_store().unwrap();
    store
        .mutate(|tx| {
            let mut config = tx
                .get::<SpaceConfig>(
                    "racing-space",
                    &RecordId::from_str(&fixture.manifest.configuration.config_id)?,
                    false,
                )?
                .unwrap();
            config
                .processing
                .as_mut()
                .unwrap()
                .bindings
                .get_mut(&ModelOperation::QueryEmbedding)
                .unwrap()
                .revoked = true;
            tx.replace(config)?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(repo.check_ready(&requirements).await.is_err());
    crate::SourceRepository::new(fixture.storage.clone())
        .delete("racing-space", &fixture.manifest.publication.source_id)
        .await
        .unwrap();
    assert!(repo.check_ready(&requirements).await.is_err());
    assert!(repo
        .completion("racing-space", &fixture.manifest.publication)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn completion_guard_rejects_a_plan_that_left_required_artifacts_uncommitted() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "incomplete-plan",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let requirements = repo
            .requirements("incomplete-plan", &fixture.manifest.publication)
            .await
            .unwrap();
        repo.check_ready(&requirements).await.unwrap();
        if let Some(store) = fixture.storage.embedded_store() {
            assert!(store
                .mutate(|tx| mark_completed_tx(tx, &requirements, &requirements.commit_id()))
                .await
                .is_err());
        } else {
            let mut sql = String::from("BEGIN TRANSACTION;\n");
            let mut bindings = BTreeMap::new();
            append_ready_sql(
                &mut sql,
                &mut bindings,
                "incomplete-plan",
                &fixture.manifest.publication,
                &requirements,
            )
            .unwrap();
            append_completed_sql(
                &mut sql,
                &mut bindings,
                &requirements,
                &requirements.commit_id(),
            )
            .unwrap();
            sql.push_str("COMMIT TRANSACTION;");
            assert!(fixture
                .storage
                .query_bind_checked(&sql, bindings)
                .await
                .is_err());
        }
        assert!(repo
            .completion("incomplete-plan", &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}

#[test]
fn generation_is_part_of_deterministic_bundle_identity() {
    let mut publication = PublicationFence {
        source_id: RecordId::new_v4(TableName::Source),
        generation: "a".into(),
        revision_id: "identical-content-hash".into(),
        operation_id: "operation".into(),
    };
    let revision = RecordId::new_v4(TableName::DocRevision);
    let node = RecordId::new_v4(TableName::DocNode);
    let first = source_bundle_id(&publication, &revision, &node);
    assert_eq!(first, source_bundle_id(&publication, &revision, &node));
    publication.generation = "b".into();
    assert_ne!(first, source_bundle_id(&publication, &revision, &node));
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn interruption_after_completion_certificate_rolls_back_every_transition() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "fault-space",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let requirements = repo
            .requirements("fault-space", &fixture.manifest.publication)
            .await
            .unwrap();
        let publications = SourcePublicationRepository::new(fixture.storage.clone());
        let lookup_key = progress::published_completion_key(
            "fault-space",
            &fixture.manifest.publication.source_id,
        );
        let original_lookup = publications
            .read_control_payload(COMPLETION_TABLE, &lookup_key, "fault-space")
            .await
            .unwrap();
        assert!(original_lookup.is_some());
        let bundle_id = &fixture.manifest.metadata_artifact_ids[0];
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let bundle = artifacts
            .get("fault-space", bundle_id)
            .await
            .unwrap()
            .unwrap();
        let candidate_id =
            RecordId::from_str(bundle.data["source_artifact_id"].as_str().unwrap()).unwrap();
        if let Some(store) = fixture.storage.embedded_store() {
            let failed: StorageResult<()> = store
                .mutate(|tx| {
                    check_ready_tx(
                        tx,
                        "fault-space",
                        &fixture.manifest.publication,
                        &requirements,
                    )?;
                    for id in [bundle_id, &candidate_id] {
                        let mut artifact =
                            tx.get::<WorkArtifact>("fault-space", id, false)?.unwrap();
                        artifact.status = ArtifactStatus::Committed;
                        artifact.committed_at = Some(Utc::now());
                        tx.replace(artifact)?;
                    }
                    let mut source = tx
                        .get::<Source>(
                            "fault-space",
                            &fixture.manifest.publication.source_id,
                            false,
                        )?
                        .unwrap();
                    source.current_ingest_mode = Some(IngestMode::Metadata);
                    source.lifecycle_status =
                        elephant_types::records::source::SourceLifecycleStatus::MetadataReady;
                    tx.replace(source)?;
                    mark_completed_tx(tx, &requirements, &requirements.commit_id())?;
                    Err(integrity(
                        "injected interruption before outer canonical commit",
                    ))
                })
                .await;
            assert!(failed.is_err());
        } else {
            let mut sql = String::from("BEGIN TRANSACTION;\n");
            let mut bindings = BTreeMap::new();
            append_ready_sql(
                &mut sql,
                &mut bindings,
                "fault-space",
                &fixture.manifest.publication,
                &requirements,
            )
            .unwrap();
            bindings.insert(
                "fixture_bundle".into(),
                crate::surreal::thing_from_record_id(bundle_id).into(),
            );
            bindings.insert(
                "fixture_candidate".into(),
                crate::surreal::thing_from_record_id(&candidate_id).into(),
            );
            sql.push_str("UPDATE $fixture_bundle SET status = 'committed', committed_at = time::now();\n\
                UPDATE $fixture_candidate SET status = 'committed', committed_at = time::now();\n\
                UPDATE $sf_source_id SET current_ingest_mode = 'metadata', lifecycle_status = 'metadata_ready';\n");
            append_completed_sql(
                &mut sql,
                &mut bindings,
                &requirements,
                &requirements.commit_id(),
            )
            .unwrap();
            sql.push_str(
                "THROW 'injected interruption before outer canonical commit';\nCOMMIT TRANSACTION;",
            );
            let failed = fixture
                .storage
                .query_bind_checked(&sql, bindings)
                .await
                .unwrap_err();
            assert!(
                failed.to_string().contains("injected interruption"),
                "unexpected earlier failure: {failed}"
            );
        }
        assert!(repo
            .completion("fault-space", &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            publications
                .read_control_payload(COMPLETION_TABLE, &lookup_key, "fault-space")
                .await
                .unwrap(),
            original_lookup,
            "late failure must retain the earlier published lookup exactly"
        );
        for id in [bundle_id, &candidate_id] {
            assert_eq!(
                artifacts
                    .get("fault-space", id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                ArtifactStatus::Validated
            );
        }
        let source = crate::SourceRepository::new(fixture.storage.clone())
            .get("fault-space", &fixture.manifest.publication.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source.current_ingest_mode, None);
        repo.check_ready(&requirements).await.unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn source_completion_requires_successful_origin_and_completed_registration() {
    for embedded in [true, false] {
        for terminal in [
            WorkItemStatus::Done,
            WorkItemStatus::Failed,
            WorkItemStatus::Cancelled,
        ] {
            let fixture = fixture_with_registration(
                client(embedded).await,
                "predecessor-space",
                IngestMode::Metadata,
                0,
                false,
            )
            .await;
            let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
            repo.set_operation_plan(fixture.manifest.clone())
                .await
                .unwrap();
            let space = &fixture.manifest.space_id;
            let requirements = repo
                .requirements(space, &fixture.manifest.publication)
                .await
                .unwrap();
            let artifact = crate::WorkArtifactRepository::new(fixture.storage.clone())
                .get(space, &fixture.manifest.metadata_artifact_ids[0])
                .await
                .unwrap()
                .unwrap();
            let registration: SourceBundleRegistration =
                SourcePublicationRepository::new(fixture.storage.clone())
                    .read_control(
                        REGISTRATION_TABLE,
                        &registration_key(space, &fixture.manifest.publication, &artifact.base.id),
                        space,
                    )
                    .await
                    .unwrap()
                    .unwrap();
            let items = crate::WorkItemRepository::new(fixture.storage.clone());
            let stage = items
                .get(space, &registration.attempt.work_item_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                repo.check_ready(&requirements).await.is_err(),
                "running registration is incomplete"
            );
            let completed = items
                .update_running_for_worker(
                    space,
                    &stage.base.id,
                    stage.leased_by.as_deref().unwrap(),
                    terminal,
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            if terminal == WorkItemStatus::Done {
                repo.check_ready(&requirements).await.unwrap();
                let runs = crate::WorkRunRepository::new(fixture.storage.clone());
                let mut run = runs
                    .get(space, artifact.work_run.id())
                    .await
                    .unwrap()
                    .unwrap();
                run.success = false;
                runs.update(space, &run.base.id.clone(), run).await.unwrap();
                assert!(
                    repo.check_ready(&requirements).await.is_err(),
                    "failed producer cannot certify completion"
                );
            } else {
                assert!(
                    repo.check_ready(&requirements).await.is_err(),
                    "failed/cancelled registration is incomplete"
                );
                if terminal == WorkItemStatus::Failed {
                    items.retry_authorized(&completed).await.unwrap();
                    let successor = items
                        .claim_with_lease(
                            space,
                            &stage.base.id,
                            "registration-retry",
                            Utc::now() + chrono::Duration::minutes(5),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    complete_claim(&fixture.storage, &successor).await;
                    assert!(
                        repo.check_ready(&requirements).await.is_err(),
                        "a later completed attempt cannot reuse an older registration"
                    );
                }
            }
            assert!(repo
                .completion(space, &fixture.manifest.publication)
                .await
                .unwrap()
                .is_none());
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn completed_predecessors_remain_valid_after_hot_queue_archival() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "archive-source",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let requirements = repo
            .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap();
        repo.check_ready(&requirements).await.unwrap();
        let archived = crate::work_retention::archive_terminal_work(
            &fixture.storage,
            Utc::now() + chrono::Duration::minutes(1),
            100,
        )
        .await
        .unwrap();
        assert!(archived.items_archived >= 2);
        assert!(archived.runs_archived >= 1);
        repo.check_ready(&requirements).await.unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn sealing_an_explicit_empty_plan_validates_its_durable_receipt() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "empty-plan-receipt",
            IngestMode::Graph,
            0,
        )
        .await;
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.planning_artifact_ids[0];
        let mut plan = artifacts
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        plan.status = ArtifactStatus::Draft;
        plan.validated_at = None;
        artifacts
            .update(&fixture.manifest.space_id, id, plan)
            .await
            .unwrap();
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let plan = artifacts
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.status, ArtifactStatus::Validated);
        assert!(plan.validated_at.is_some());
        repo.check_ready(
            &repo
                .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn explicit_skip_preserves_selected_mode_and_metadata_obligations() {
    for embedded in [true, false] {
        let fixture = fixture(client(embedded).await, "explicit-skip", IngestMode::Skip, 0).await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let proof = repo
            .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap();
        assert_eq!(proof.manifest().mode, IngestMode::Skip);
        assert_eq!(completed_lifecycle(IngestMode::Skip), "skipped");
        repo.check_ready(&proof).await.unwrap();
        let mut missing_metadata = fixture.manifest;
        missing_metadata.metadata_artifact_ids.clear();
        assert!(missing_metadata.validate().is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn extraction_evidence_cannot_escape_its_planned_selection() {
    let fixture = fixture(client(true).await, "bounded-evidence", IngestMode::Graph, 1).await;
    let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
    repo.set_operation_plan(fixture.manifest.clone())
        .await
        .unwrap();
    stage_bundle(&fixture, 0).await;
    let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
    let mut bundle = artifacts
        .get(
            &fixture.manifest.space_id,
            &fixture.manifest.bundles[0].artifact_id,
        )
        .await
        .unwrap()
        .unwrap();
    bundle.data["payload"]["data"]["assertions"] =
        serde_json::json!([{ "evidence_start": 0, "evidence_end": 4 }]);
    assert!(check_expected_bundle(&fixture.manifest, &bundle).is_err());
    bundle.data["payload"]["data"]["assertions"] =
        serde_json::json!([{ "evidence_start": 1, "evidence_end": 1 }]);
    assert!(check_expected_bundle(&fixture.manifest, &bundle).is_err());
    bundle.data["payload"]["data"]["assertions"] =
        serde_json::json!([{ "evidence_start": 0, "evidence_end": 3 }]);
    check_expected_bundle(&fixture.manifest, &bundle).unwrap();
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn unreasoned_zero_selection_does_not_publish_nonempty_graph_input() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "unreasoned-zero",
            IngestMode::Graph,
            0,
        )
        .await;
        let revisions = crate::DocRevisionRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.documents[0].doc_revision_id;
        let mut revision = revisions
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        revision.text = "Nonempty source that was never evaluated.".into();
        revision.text_hash = format!("{:x}", Sha256::digest(revision.text.as_bytes()));
        revisions
            .update(&fixture.manifest.space_id, id, revision)
            .await
            .unwrap();
        let error = SourcePublicationManifestRepository::new(fixture.storage.clone())
            .set_operation_plan(fixture.manifest)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("nonempty_graph_plan_missing_coverage"),
            "unexpected rejection: {error}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn planner_retry_adopts_exact_sealed_selections_without_forgiving_failed_origin() {
    for embedded in [true, false] {
        let fixture = fixture(client(embedded).await, "resume-plan", IngestMode::Graph, 0).await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let proof = repo
            .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap();
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let before = artifacts
            .get(
                &fixture.manifest.space_id,
                &fixture.manifest.planning_artifact_ids[0],
            )
            .await
            .unwrap()
            .unwrap();
        let runs = crate::WorkRunRepository::new(fixture.storage.clone());
        let mut original = runs
            .get(&fixture.manifest.space_id, before.work_run.id())
            .await
            .unwrap()
            .unwrap();
        original.success = false;
        runs.update(
            &fixture.manifest.space_id,
            &original.base.id,
            original.clone(),
        )
        .await
        .unwrap();
        let items = crate::WorkItemRepository::new(fixture.storage.clone());
        let mut planner = items
            .get(&fixture.manifest.space_id, original.work_item.id())
            .await
            .unwrap()
            .unwrap();
        planner.kind = "plan_extraction".into();
        planner.payload["_publication"] =
            serde_json::to_value(&fixture.manifest.publication).unwrap();
        planner.status = WorkItemStatus::Runnable;
        items
            .update(
                &fixture.manifest.space_id,
                &planner.base.id,
                planner.clone(),
            )
            .await
            .unwrap();
        let claim = items
            .claim_with_lease(
                &fixture.manifest.space_id,
                &planner.base.id,
                "resumed-planner",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let attempt = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
        let mut resumed = original.clone();
        resumed.base.id = RecordId::new_v4(TableName::WorkRun);
        resumed.attempt = claim.attempts;
        resumed.completed_at = None;
        resumed.success = false;
        runs.create(resumed.clone()).await.unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        repo.resume_plan_attempt(
            &fixture.manifest.space_id,
            &fixture.manifest.publication,
            &attempt,
            &resumed.base.id,
        )
        .await
        .unwrap();
        let after = artifacts
            .get(&fixture.manifest.space_id, &before.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.data, before.data);
        assert_eq!(after.work_run.id(), &resumed.base.id);
        assert_eq!(
            repo.get(&fixture.manifest.space_id, &fixture.manifest.publication)
                .await
                .unwrap()
                .unwrap(),
            fixture.manifest
        );
        assert!(repo.check_ready(&proof).await.is_err());
        resumed.success = true;
        resumed.completed_at = Some(Utc::now());
        runs.update(
            &fixture.manifest.space_id,
            &resumed.base.id,
            resumed.clone(),
        )
        .await
        .unwrap();
        complete_claim(&fixture.storage, &claim).await;
        repo.check_ready(&proof).await.unwrap();
        assert!(
            repo.resume_plan_attempt(
                &fixture.manifest.space_id,
                &fixture.manifest.publication,
                &attempt,
                &resumed.base.id
            )
            .await
            .is_err(),
            "completed lease cannot adopt again"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn real_vectors_cannot_finish_a_pending_or_failed_embedding_job() {
    for embedded in [true, false] {
        let fixture = embedding_fixture(
            client(embedded).await,
            "embedding-job",
            IngestMode::Retrieval,
            1,
        )
        .await;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        embed(&fixture, 0, vec![1.0, 0.0, 0.0]).await;
        let proof = repo
            .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap();
        repo.check_ready(&proof).await.unwrap();
        let items = crate::WorkItemRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.embedding_work_item_ids[0];
        let mut item = items
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        item.status = WorkItemStatus::Runnable;
        items
            .update(&fixture.manifest.space_id, id, item.clone())
            .await
            .unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        item.status = WorkItemStatus::Done;
        items
            .update(&fixture.manifest.space_id, id, item.clone())
            .await
            .unwrap();
        let runs = crate::WorkRunRepository::new(fixture.storage.clone());
        let mut run = runs.find_by_work_item(id).await.unwrap().remove(0);
        run.success = false;
        runs.update(&fixture.manifest.space_id, &run.base.id, run.clone())
            .await
            .unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        run.success = true;
        runs.update(&fixture.manifest.space_id, &run.base.id, run.clone())
            .await
            .unwrap();
        crate::work_retention::archive_terminal_work(
            &fixture.storage,
            Utc::now() + chrono::Duration::minutes(1),
            100,
        )
        .await
        .unwrap();
        repo.check_ready(&proof).await.unwrap();
        let mut incomplete_expected_set = fixture.manifest;
        incomplete_expected_set.embedding_work_item_ids.clear();
        assert!(incomplete_expected_set.validate().is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn embedding_publication_uses_completed_fence_after_refunded_waits() {
    for embedded in [true, false] {
        let fixture = fixture_with_embedding_waits(
            client(embedded).await,
            "embedding-refunded-attempts",
            IngestMode::Retrieval,
            1,
            true,
            &[EmbeddingWait::Dependency, EmbeddingWait::Configuration],
            false,
        )
        .await;
        let space = &fixture.manifest.space_id;
        let repo = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repo.set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        embed(&fixture, 0, vec![1.0, 0.0, 0.0]).await;
        let proof = repo
            .requirements(space, &fixture.manifest.publication)
            .await
            .unwrap();
        let items = crate::WorkItemRepository::new(fixture.storage.clone());
        let runs = crate::WorkRunRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.embedding_work_item_ids[0];
        let completed = items.get(space, id).await.unwrap().unwrap();
        let mut history = runs.find_by_work_item(id).await.unwrap();
        history.sort_by_key(|run| {
            run.metrics.custom["attempt_fence"]["generation"]
                .as_u64()
                .unwrap()
        });
        assert_eq!(history.len(), 3);
        assert!(history.iter().all(|run| run.attempt == 1));
        assert!(!history[0].success && !history[1].success && history[2].success);
        assert_eq!(completed.attempts, 1);
        let current = history[2].clone();
        // Before the repair this fails: three physical claims share retry 1.
        repo.check_ready(&proof).await.unwrap();

        // A successful stale generation cannot stand in for the current one.
        let mut old_success = history[0].clone();
        old_success.success = true;
        runs.update(space, &old_success.base.id, old_success.clone())
            .await
            .unwrap();
        let mut stale_item = completed.clone();
        stale_item.payload["_attempt_fence"] = old_success.metrics.custom["attempt_fence"].clone();
        items.update(space, id, stale_item).await.unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        items.update(space, id, completed.clone()).await.unwrap();

        // Reject malformed, absent, wrong-space/work/token/generation run
        // evidence even when successful vectors and an older success exist.
        for field in [
            "missing",
            "space_id",
            "work_item_id",
            "lease_token",
            "generation",
        ] {
            let mut bad = current.clone();
            match field {
                "missing" => {
                    bad.metrics.custom.remove("attempt_fence");
                }
                "generation" => {
                    bad.metrics.custom.get_mut("attempt_fence").unwrap()[field] =
                        serde_json::json!(1);
                }
                _ => {
                    bad.metrics.custom.get_mut("attempt_fence").unwrap()[field] =
                        serde_json::json!("unrelated");
                }
            }
            runs.update(space, &bad.base.id, bad.clone()).await.unwrap();
            assert!(repo.check_ready(&proof).await.is_err(), "{field}");
        }
        for field in ["attempt", "success", "completion"] {
            let mut bad = current.clone();
            match field {
                "attempt" => bad.attempt += 1,
                "success" => bad.success = false,
                _ => bad.completed_at = None,
            }
            runs.update(space, &bad.base.id, bad.clone()).await.unwrap();
            assert!(repo.check_ready(&proof).await.is_err(), "{field}");
        }
        runs.update(space, &current.base.id, current.clone())
            .await
            .unwrap();
        repo.check_ready(&proof).await.unwrap();

        // A second record claiming this same completed fence is ambiguous;
        // success flags and insertion order never choose a winning record.
        let mut duplicate = current.clone();
        duplicate.base.id = RecordId::new_v4(TableName::WorkRun);
        duplicate.success = false;
        runs.create(duplicate.clone()).await.unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        duplicate.success = true;
        runs.update(space, &duplicate.base.id, duplicate.clone())
            .await
            .unwrap();
        assert!(repo.check_ready(&proof).await.is_err());
        duplicate.base.deleted_at = Some(Utc::now());
        runs.update(space, &duplicate.base.id, duplicate.clone())
            .await
            .unwrap();
        repo.check_ready(&proof).await.unwrap();

        runs.update(space, &history[0].base.id, history[0].clone())
            .await
            .unwrap();

        // A completed-looking item/run cannot override changed durable state.
        for status in ["runnable", "done"] {
            if let Some(store) = fixture.storage.embedded_store() {
                store
                    .mutate(|tx| {
                        let mut row = crate::work_attempt_repository::read_attempt(tx, id)?
                            .ok_or_else(|| integrity("fixture attempt missing"))?;
                        row.status = status.into();
                        tx.upsert_extension_row(
                            crate::work_attempt_repository::ATTEMPT_TABLE,
                            serde_json::to_value(row)
                                .map_err(|error| integrity(&error.to_string()))?,
                        )?;
                        Ok(())
                    })
                    .await
                    .unwrap();
            } else {
                fixture
                    .storage
                    .query_bind_checked(
                        "UPDATE $id SET status = $status;",
                        crate::bindings! {
                            "id" => crate::work_attempt_repository::attempt_id(id),
                            "status" => status,
                        },
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(repo.check_ready(&proof).await.is_ok(), status == "done");
        }

        // Existing archival preserves both refunded failures and exact success;
        // the retained durable claim is still required after hot rows move.
        crate::work_retention::archive_terminal_work(
            &fixture.storage,
            Utc::now() + chrono::Duration::minutes(1),
            100,
        )
        .await
        .unwrap();
        repo.check_ready(&proof).await.unwrap();
        for original in &history {
            let retained = runs.get(space, &original.base.id).await.unwrap().unwrap();
            assert_eq!(
                retained.metrics.custom["attempt_fence"],
                original.metrics.custom["attempt_fence"]
            );
            assert_eq!(retained.success, original.success);
            assert_eq!(retained.attempt, original.attempt);
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn claimed_complete_coverage_cannot_hide_a_budget_truncated_selection() {
    for embedded in [true, false] {
        let mut fixture = fixture(
            client(embedded).await,
            "coverage-budget",
            IngestMode::Graph,
            2,
        )
        .await;
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.planning_artifact_ids[0];
        let mut plan = artifacts
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        let retained_node = fixture.manifest.bundles[0].node_id.to_string();
        fixture.manifest.bundles.truncate(1);
        plan.data["node_selections"]
            .as_array_mut()
            .unwrap()
            .retain(|selection| selection["node_id"] == retained_node);
        // The persisted flag still claims complete. Actual text/ranges disprove it.
        assert_eq!(plan.data["coverage"]["complete"], true);
        artifacts
            .update(&fixture.manifest.space_id, id, plan)
            .await
            .unwrap();
        let error = SourcePublicationManifestRepository::new(fixture.storage.clone())
            .set_operation_plan(fixture.manifest)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source_extraction_coverage_incomplete"),
            "unexpected failure: {error}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn graph_plan_without_coverage_proof_is_not_a_publication_manifest() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "coverage-missing",
            IngestMode::Graph,
            1,
        )
        .await;
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.planning_artifact_ids[0];
        let mut plan = artifacts
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        plan.data.as_object_mut().unwrap().remove("coverage");
        artifacts
            .update(&fixture.manifest.space_id, id, plan)
            .await
            .unwrap();
        let persisted = artifacts
            .get(&fixture.manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            persisted.data.get("coverage").is_none(),
            "artifact replacement must remove the coverage proof before testing publication refusal"
        );
        assert!(
            SourcePublicationManifestRepository::new(fixture.storage.clone())
                .set_operation_plan(fixture.manifest)
                .await
                .is_err()
        );
    }
}

fn plan_payload(manifest: &SourcePublicationManifest) -> serde_json::Value {
    serde_json::json!({
        "_publication": manifest.publication,
        "_source_configuration": manifest.configuration,
        "_source_plan": source_plan_id(&manifest.publication, &manifest.configuration),
    })
}

// The active planner selects configuration-scoped output IDs before fanout.
// Exercise that exact format against the real durable extraction plan on both
// backends; legacy-only fixtures cannot detect a seal-time backend divergence.
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn configuration_scoped_graph_plan_preserves_exact_durable_selections() {
    for embedded in [true, false] {
        let mut fixture = fixture(
            client(embedded).await,
            "versioned-graph-selection",
            IngestMode::Graph,
            2,
        )
        .await;
        let space = fixture.manifest.space_id.clone();
        let publications = SourcePublicationRepository::new(fixture.storage.clone());
        let before = publications
            .head(&space, &fixture.manifest.publication.source_id)
            .await
            .unwrap();
        let original_plan = crate::WorkArtifactRepository::new(fixture.storage.clone())
            .get(&space, &fixture.manifest.planning_artifact_ids[0])
            .await
            .unwrap()
            .unwrap();
        // Existing initial manifests still use their original explicit IDs.
        check_planned_selections(&fixture.manifest, std::slice::from_ref(&original_plan)).unwrap();
        for bundle in &mut fixture.manifest.bundles {
            bundle.artifact_id = source_bundle_id_for_plan(
                &fixture.manifest.publication,
                &fixture.manifest.configuration,
                &bundle.revision_id,
                &bundle.node_id,
            );
        }
        fixture.manifest.normalize();
        fixture.manifest.validate().unwrap();
        check_planned_selections(&fixture.manifest, std::slice::from_ref(&original_plan)).unwrap();

        // IDs must still belong to this exact accepted configuration.
        let mut wrong_ids = fixture.manifest.clone();
        let mut other_configuration = wrong_ids.configuration.clone();
        other_configuration.generation += 1;
        wrong_ids.bundles[0].artifact_id = source_bundle_id_for_plan(
            &wrong_ids.publication,
            &other_configuration,
            &wrong_ids.bundles[0].revision_id,
            &wrong_ids.bundles[0].node_id,
        );
        wrong_ids.normalize();
        assert!(wrong_ids.validate().is_err());
        assert!(matches!(
            check_planned_selections(&wrong_ids, std::slice::from_ref(&original_plan)),
            Err(StorageError::Conflict(reason)) if reason == "source_extraction_expected_set_changed"
        ));

        // No finished subset, duplicate, altered span or coalesced membership
        // may pass simply because its plan uses the correct artifact format.
        for fault in 0..4 {
            let mut changed = original_plan.clone();
            let selections = changed.data["node_selections"].as_array_mut().unwrap();
            match fault {
                0 => {
                    selections.pop();
                }
                1 => {
                    selections.push(selections[0].clone());
                }
                2 => {
                    selections[0]["span_end"] = serde_json::json!(2);
                }
                _ => {
                    selections[0]["node_ids"] = serde_json::json!([]);
                }
            }
            assert!(matches!(
                check_planned_selections(&fixture.manifest, &[changed]),
                Err(StorageError::Conflict(reason)) if reason == "source_extraction_expected_set_changed"
            ));
        }

        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        let sealed = repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        assert_eq!(sealed, fixture.manifest);
        assert_eq!(
            repository.get(&space, &sealed.publication).await.unwrap(),
            Some(sealed.clone())
        );
        assert_eq!(
            repository.set_operation_plan(sealed.clone()).await.unwrap(),
            sealed
        );
        assert_eq!(
            publications
                .head(&space, &sealed.publication.source_id)
                .await
                .unwrap(),
            before,
            "sealing expected outputs cannot publish the pending replacement"
        );
        assert!(repository
            .completion(&space, &sealed.publication)
            .await
            .unwrap()
            .is_none());
        for bundle in &sealed.bundles {
            assert!(crate::WorkArtifactRepository::new(fixture.storage.clone())
                .get(&space, &bundle.artifact_id)
                .await
                .unwrap()
                .is_none());
        }
    }
}

async fn claim_plan_work(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
    kind: &str,
    payload: serde_json::Value,
) -> WorkItem {
    let item = work_item(&manifest.space_id, kind, payload);
    let items = crate::WorkItemRepository::new(storage.clone());
    items.create(item.clone()).await.unwrap();
    items
        .claim_with_lease(
            &manifest.space_id,
            &item.base.id,
            "replan-fixture",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn next_configuration(fixture: &Fixture) -> SourcePublicationManifest {
    let configs = crate::SpaceConfigRepository::new(fixture.storage.clone());
    let config_id = RecordId::from_str(&fixture.manifest.configuration.config_id).unwrap();
    let mut config = configs
        .get(&fixture.manifest.space_id, &config_id)
        .await
        .unwrap()
        .unwrap();
    let processing = config.processing.as_mut().unwrap();
    processing.generation += 1;
    processing
        .connections
        .get_mut("connection")
        .unwrap()
        .credential_generation += 1;
    configs
        .update(&fixture.manifest.space_id, &config_id, config)
        .await
        .unwrap();
    let mut next = fixture.manifest.clone();
    next.configuration = configs
        .resolve_space(&fixture.manifest.space_id)
        .await
        .unwrap()
        .token;
    next.embedding_work_item_ids = next
        .documents
        .iter()
        .map(|mapping| {
            source_embedding_work_id_for_plan(
                &next.space_id,
                &next.publication,
                &next.configuration,
                &mapping.doc_revision_id,
            )
        })
        .collect();
    for bundle in &mut next.bundles {
        bundle.artifact_id = source_bundle_id_for_plan(
            &next.publication,
            &next.configuration,
            &bundle.revision_id,
            &bundle.node_id,
        );
    }
    next
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn new_version_metadata(
    storage: &StorageClient,
    next: &mut SourcePublicationManifest,
) -> WorkItem {
    let space = next.space_id.clone();
    let candidates = crate::WorkArtifactRepository::new(storage.clone());
    let mut candidate = artifact(
        &space,
        "source_metadata",
        serde_json::json!({"source_id": next.publication.source_id.to_string()}),
    );
    let metadata_payload = candidate.data.clone();
    candidate
        .data
        .as_object_mut()
        .unwrap()
        .extend(plan_payload(next).as_object().unwrap().clone());
    let mut bundle = artifact(
        &space,
        "commit_bundle",
        serde_json::json!({
            "publication": next.publication, "source_artifact_id": candidate.base.id.to_string(),
            "source_artifact_type": "source_metadata", "bundle_kind": "source_metadata",
            "payload": {"kind":"source_metadata", "data":metadata_payload},
        }),
    );
    bundle.work_run = candidate.work_run.clone();
    // Admit the intended payload once: private original producer evidence must
    // not be invalidated by rewriting semantic intent after admission.
    create_successful_origin_with_payload(storage, &bundle, plan_payload(next)).await;
    candidates.create(candidate).await.unwrap();
    candidates.create(bundle.clone()).await.unwrap();
    next.metadata_artifact_ids = vec![bundle.base.id.clone()];
    let mut payload = plan_payload(next);
    payload["work_run_id"] = serde_json::json!(bundle.work_run.id().to_string());
    let claim = claim_plan_work(storage, next, "commit_artifacts", payload).await;
    let attempt = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
    SourcePublicationManifestRepository::new(storage.clone())
        .register_bundle(&space, &next.publication, &bundle.base.id, &attempt)
        .await
        .unwrap();
    claim
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn configuration_replan_keeps_acceptance_and_previous_publication_but_fences_old_claims() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "configuration-recovery",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let old_finalizer = claim_plan_work(
            &fixture.storage,
            &fixture.manifest,
            "finalize_source_revision",
            plan_payload(&fixture.manifest),
        )
        .await;
        let old_attempt = crate::work_attempt_repository::fence_from_claim(&old_finalizer).unwrap();
        let mut next = next_configuration(&fixture).await;
        let planner = new_version_metadata(&fixture.storage, &mut next).await;
        let planner_attempt = crate::work_attempt_repository::fence_from_claim(&planner).unwrap();
        let digest = fixture.manifest.digest().unwrap();
        let replaced = repository
            .replace_operation_plan(
                &next.space_id,
                &next.publication,
                &digest,
                next.clone(),
                &planner_attempt,
            )
            .await
            .unwrap();
        assert_eq!(replaced, next);
        assert_eq!(
            repository
                .replace_operation_plan(
                    &next.space_id,
                    &next.publication,
                    &digest,
                    next.clone(),
                    &planner_attempt
                )
                .await
                .unwrap(),
            next,
            "retry preserves one immutable transition"
        );
        complete_claim(&fixture.storage, &planner).await;
        let head = SourcePublicationRepository::new(fixture.storage.clone())
            .head(&next.space_id, &next.publication.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.generation, fixture.manifest.publication.generation);
        assert_eq!(head.operation_id, fixture.manifest.publication.operation_id);
        assert_eq!(head.published_revision.as_deref(), Some("revision-1"));
        assert!(
            repository
                .requirements_for_attempt(&next.space_id, &next.publication, &old_attempt)
                .await
                .is_err(),
            "an original old-plan finalizer cannot adopt the new manifest"
        );
        let unbound = repository
            .requirements(&next.space_id, &next.publication)
            .await
            .unwrap();
        assert!(
            repository.check_ready(&unbound).await.is_err(),
            "replacement needs an original configuration-bound finalizer"
        );
        let finalizer = claim_plan_work(
            &fixture.storage,
            &next,
            "finalize_source_revision",
            plan_payload(&next),
        )
        .await;
        let attempt = crate::work_attempt_repository::fence_from_claim(&finalizer).unwrap();
        let ready = repository
            .requirements_for_attempt(&next.space_id, &next.publication, &attempt)
            .await
            .unwrap();
        assert!(ready.matches_attempt(&attempt));
        assert!(!ready.matches_attempt(&old_attempt));
        repository.check_ready(&ready).await.unwrap();
        let artifacts = crate::WorkArtifactRepository::new(fixture.storage.clone());
        let metadata = artifacts
            .get(&next.space_id, &next.metadata_artifact_ids[0])
            .await
            .unwrap()
            .unwrap();
        let candidate_id =
            RecordId::from_str(metadata.data["source_artifact_id"].as_str().unwrap()).unwrap();
        let mut candidate = artifacts
            .get(&next.space_id, &candidate_id)
            .await
            .unwrap()
            .unwrap();
        candidate.data["_source_configuration"] =
            serde_json::to_value(&fixture.manifest.configuration).unwrap();
        artifacts
            .update(&next.space_id, &candidate_id, candidate)
            .await
            .unwrap();
        assert!(
            repository.check_ready(&ready).await.is_err(),
            "a new producer claim cannot relabel an old-configuration candidate"
        );
        assert!(repository
            .completion(&next.space_id, &next.publication)
            .await
            .unwrap()
            .is_none());
        let history: serde_json::Value = SourcePublicationRepository::new(fixture.storage.clone())
            .read_control(
                replan::HISTORY_TABLE,
                &source_plan_id(&next.publication, &next.configuration),
                &next.space_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            history["previous"],
            serde_json::to_value(&fixture.manifest).unwrap()
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn replacement_manifest_rejects_stale_compare_config_attempt_and_old_outputs_atomically() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "replan-failures",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let mut next = next_configuration(&fixture).await;
        let planner = new_version_metadata(&fixture.storage, &mut next).await;
        let attempt = crate::work_attempt_repository::fence_from_claim(&planner).unwrap();
        let digest = fixture.manifest.digest().unwrap();
        assert!(repository
            .replace_operation_plan(
                &next.space_id,
                &next.publication,
                "wrong-old-set",
                next.clone(),
                &attempt
            )
            .await
            .is_err());
        let mut stolen = attempt.clone();
        stolen.generation += 1;
        assert!(repository
            .replace_operation_plan(
                &next.space_id,
                &next.publication,
                &digest,
                next.clone(),
                &stolen
            )
            .await
            .is_err());
        let mut stale = next.clone();
        stale.configuration.generation += 1;
        assert!(repository
            .replace_operation_plan(&next.space_id, &next.publication, &digest, stale, &attempt)
            .await
            .is_err());
        let mut reused = next.clone();
        reused.metadata_artifact_ids = fixture.manifest.metadata_artifact_ids.clone();
        assert!(repository
            .replace_operation_plan(&next.space_id, &next.publication, &digest, reused, &attempt)
            .await
            .is_err());
        assert_eq!(
            repository
                .get(&next.space_id, &next.publication)
                .await
                .unwrap(),
            Some(fixture.manifest.clone())
        );
        let history: Option<serde_json::Value> =
            SourcePublicationRepository::new(fixture.storage.clone())
                .read_control(
                    replan::HISTORY_TABLE,
                    &source_plan_id(&next.publication, &next.configuration),
                    &next.space_id,
                )
                .await
                .unwrap();
        assert!(history.is_none());
        // A superseding source acceptance also defeats a previously prepared replan.
        let mut head = SourcePublicationRepository::new(fixture.storage.clone())
            .head(&next.space_id, &next.publication.source_id)
            .await
            .unwrap()
            .unwrap();
        head.generation = Uuid::new_v4().to_string();
        if let Some(store) = fixture.storage.embedded_store() {
            store
                .mutate(|tx| {
                    write_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(&next.space_id, &next.publication.source_id),
                        &next.space_id,
                        &next.publication.source_id,
                        &head,
                    )
                })
                .await
                .unwrap();
        } else {
            fixture.storage.query_bind_checked("UPDATE $id CONTENT $value", crate::bindings! { "id" => control_thing(HEAD_TABLE, &head_key(&next.space_id, &next.publication.source_id)), "value" => control_value(&next.space_id, &next.publication.source_id, &head).unwrap() }).await.unwrap();
        }
        assert!(repository
            .replace_operation_plan(
                &next.space_id,
                &next.publication,
                &digest,
                next.clone(),
                &attempt
            )
            .await
            .is_err());
        assert_eq!(
            repository
                .get(&next.space_id, &next.publication)
                .await
                .unwrap(),
            Some(fixture.manifest)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn completed_revision_is_never_rewritten_by_configuration_replan() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "published-replan",
            IngestMode::Metadata,
            0,
        )
        .await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let mut next = next_configuration(&fixture).await;
        let planner = new_version_metadata(&fixture.storage, &mut next).await;
        let attempt = crate::work_attempt_repository::fence_from_claim(&planner).unwrap();
        let requirements = repository
            .requirements(&next.space_id, &next.publication)
            .await
            .unwrap();
        let completion = completion_for(&requirements, &requirements.commit_id()).unwrap();
        if let Some(store) = fixture.storage.embedded_store() {
            store
                .mutate(|tx| {
                    write_control_tx(
                        tx,
                        COMPLETION_TABLE,
                        &manifest_key(&next.space_id, &next.publication),
                        &next.space_id,
                        &next.publication.source_id,
                        &completion,
                    )
                })
                .await
                .unwrap();
        } else {
            fixture.storage.query_bind_checked("CREATE $id CONTENT $value", crate::bindings! { "id" => control_thing(COMPLETION_TABLE, &manifest_key(&next.space_id, &next.publication)), "value" => control_value(&next.space_id, &next.publication.source_id, &completion).unwrap() }).await.unwrap();
        }
        assert!(repository
            .replace_operation_plan(
                &next.space_id,
                &next.publication,
                &fixture.manifest.digest().unwrap(),
                next.clone(),
                &attempt
            )
            .await
            .is_err());
        assert_eq!(
            repository
                .get(&next.space_id, &next.publication)
                .await
                .unwrap(),
            Some(fixture.manifest)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn replacement_expectations_survive_restart_before_any_new_child_enqueue() {
    let directory = tempfile::tempdir().unwrap();
    let config = crate::StorageConfig {
        backend: crate::StorageBackendKind::ElephantDb,
        ..crate::StorageConfig::default()
    };
    let snapshot = directory.path().join("live.snapshot");
    let journal = directory.path().join("journal");
    let next;
    {
        let storage = StorageClient::new_with_durability(
            config.clone(),
            Some(snapshot.clone()),
            Some(journal.clone()),
        )
        .await
        .unwrap();
        let fixture = fixture(storage, "quiet-replan-restart", IngestMode::Metadata, 0).await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let mut replacement = next_configuration(&fixture).await;
        let planner = new_version_metadata(&fixture.storage, &mut replacement).await;
        next = repository
            .replace_operation_plan(
                &replacement.space_id,
                &replacement.publication,
                &fixture.manifest.digest().unwrap(),
                replacement.clone(),
                &crate::work_attempt_repository::fence_from_claim(&planner).unwrap(),
            )
            .await
            .unwrap();
        // No next child/finalizer enqueue or shutdown snapshot is performed.
    }
    let reopened = StorageClient::new_with_durability(config, Some(snapshot), Some(journal))
        .await
        .unwrap();
    let repository = SourcePublicationManifestRepository::new(reopened);
    assert_eq!(repository.pending_all().await.unwrap(), vec![next]);
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn credential_rotation_versions_work_without_changing_vector_identity() {
    let fixture = fixture(
        client(true).await,
        "rotation-identity",
        IngestMode::Retrieval,
        0,
    )
    .await;
    let old_profile = fixture.manifest.embedding_profile.clone().unwrap();
    let next = next_configuration(&fixture).await;
    let resolved = crate::SpaceConfigRepository::new(fixture.storage)
        .resolve_space(&next.space_id)
        .await
        .unwrap();
    assert_eq!(
        resolved
            .authorize(&next.space_id, ModelOperation::DocumentEmbedding, None)
            .unwrap()
            .vector_partition()
            .unwrap(),
        old_profile
    );
    assert_ne!(
        source_plan_id(
            &fixture.manifest.publication,
            &fixture.manifest.configuration
        ),
        source_plan_id(&next.publication, &next.configuration)
    );
    assert_ne!(
        fixture.manifest.embedding_work_item_ids,
        next.embedding_work_item_ids
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn actual_erasure_removes_current_and_superseded_plan_payloads() {
    use crate::source_publication::{AcceptSourceRevision, SourceMutation};
    use elephant_types::publication::{OperationState, SourcePrecondition};
    use elephant_types::records::{entity::EntityType, Entity};

    for embedded in [true, false] {
        let storage = client(embedded).await;
        let space = "erased-plan-history";
        let configs = crate::SpaceConfigRepository::new(storage.clone());
        configs
            .create(SpaceConfig {
                base: base(TableName::SpaceConfig, space),
                embedding_provider: "openai".into(),
                embedding_model: "fixture-embedding".into(),
                embedding_dims: 3,
                purpose: None,
                skill: None,
                processing: Some(processing()),
            })
            .await
            .unwrap();
        let mut source = Source::new(
            base(TableName::Source, space),
            SourceType::Manual,
            "erased-replan-content-hash".into(),
        );
        let canary = "PRIVATE-REPLAN-ERASURE-CANARY";
        source.metadata = serde_json::json!({"_manual_doc":{"text":canary}});
        let publications = SourcePublicationRepository::new(storage.clone());
        let accepted = publications
            .accept(AcceptSourceRevision {
                source,
                revision_id: "immutable-erasure-input".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "accept-erasure-replan".into(),
                request_fingerprint: "accept-erasure-replan".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .unwrap();
        let mut original = SourcePublicationManifest {
            historical_adoption: None,
            space_id: space.into(),
            publication: PublicationFence {
                source_id: accepted.source_id.clone(),
                generation: accepted.source_generation.clone(),
                revision_id: "immutable-erasure-input".into(),
                operation_id: accepted.operation_id.clone(),
            },
            mode: IngestMode::Metadata,
            configuration: configs.resolve_space(space).await.unwrap().token,
            embedding_profile: None,
            documents: Vec::new(),
            metadata_artifact_ids: Vec::new(),
            planning_artifact_ids: Vec::new(),
            bundles: Vec::new(),
            embeddings: Vec::new(),
            embedding_work_item_ids: Vec::new(),
        };
        let original_stage = new_version_metadata(&storage, &mut original).await;
        complete_claim(&storage, &original_stage).await;
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        manifests
            .set_operation_plan(original.clone())
            .await
            .unwrap();
        let old_finalizer = claim_plan_work(
            &storage,
            &original,
            "finalize_source_revision",
            plan_payload(&original),
        )
        .await;
        let fixture = Fixture {
            storage: storage.clone(),
            manifest: original.clone(),
            nodes: Vec::new(),
        };
        let mut next = next_configuration(&fixture).await;
        let replacement_stage = new_version_metadata(&storage, &mut next).await;
        manifests
            .replace_operation_plan(
                space,
                &next.publication,
                &original.digest().unwrap(),
                next.clone(),
                &crate::work_attempt_repository::fence_from_claim(&replacement_stage).unwrap(),
            )
            .await
            .unwrap();
        complete_claim(&storage, &replacement_stage).await;
        let new_finalizer = claim_plan_work(
            &storage,
            &next,
            "finalize_source_revision",
            plan_payload(&next),
        )
        .await;
        let history_key = source_plan_id(&next.publication, &next.configuration);
        assert!(publications
            .read_control::<serde_json::Value>(replan::HISTORY_TABLE, &history_key, space)
            .await
            .unwrap()
            .is_some());

        // Capture actual producer/run and candidate identities before erasure;
        // absence is checked against physical inventory, not eligibility views.
        let artifacts = crate::WorkArtifactRepository::new(storage.clone());
        let runs = crate::WorkRunRepository::new(storage.clone());
        let mut erased_ids = vec![
            original_stage.base.id,
            replacement_stage.base.id,
            old_finalizer.base.id,
            new_finalizer.base.id,
        ];
        for manifest in [&original, &next] {
            let bundle = artifacts
                .get(space, &manifest.metadata_artifact_ids[0])
                .await
                .unwrap()
                .unwrap();
            let candidate_id =
                RecordId::from_str(bundle.data["source_artifact_id"].as_str().unwrap()).unwrap();
            let run = runs
                .get(space, bundle.work_run.id())
                .await
                .unwrap()
                .unwrap();
            erased_ids.extend([
                bundle.base.id,
                candidate_id,
                run.base.id,
                run.work_item.id().clone(),
            ]);
        }
        let contains_physical_id = |rows: &[serde_json::Value], id: &RecordId| {
            let domain_id = serde_json::to_value(id).unwrap();
            let native_id =
                serde_json::Value::String(crate::surreal::thing_from_record_id(id).to_string());
            rows.iter().any(|row| {
                row.get("id")
                    .is_some_and(|value| value == &domain_id || value == &native_id)
            })
        };
        let before_rows = publications.erasure_inventory_rows(space).await.unwrap();
        for id in &erased_ids {
            assert!(
                contains_physical_id(&before_rows, id),
                "fixture physical row absent before erase: {id}"
            );
        }
        let independent = Entity::new(
            base(TableName::Entity, space),
            EntityType::new("project"),
            "independent survivor".into(),
        );
        crate::EntityRepository::new(storage.clone())
            .create(independent.clone())
            .await
            .unwrap();
        let receipt = publications
            .erase_live(SourceMutation {
                space_id: space.into(),
                source_id: accepted.source_id.clone(),
                expected: SourcePrecondition::Generation(accepted.source_generation),
                idempotency_key: "erase-both-plans".into(),
                request_fingerprint: "erase-both-plans".into(),
                actor: "manifest-fixture".into(),
                authorized: publications
                    .authorization_snapshot(space, &accepted.source_id)
                    .await
                    .unwrap()
                    .unwrap(),
                authorized_document: None,
            })
            .await
            .unwrap();
        assert_eq!(receipt.state, OperationState::Erased);
        assert!(crate::SourceRepository::new(storage.clone())
            .get_with_deleted(space, &accepted.source_id)
            .await
            .unwrap()
            .is_none());
        assert!(manifests
            .get(space, &next.publication)
            .await
            .unwrap()
            .is_none());
        assert!(publications
            .read_control::<serde_json::Value>(replan::HISTORY_TABLE, &history_key, space)
            .await
            .unwrap()
            .is_none());
        let rows = publications.erasure_inventory_rows(space).await.unwrap();
        let physical_payload = serde_json::to_string(&rows).unwrap();
        assert!(!physical_payload.contains(canary));
        for id in erased_ids {
            // Payload-free erase events may retain affected opaque IDs. The
            // physical record itself must be gone, including soft-deleted rows.
            let present = contains_physical_id(&rows, &id);
            assert!(!present, "erasure retained physical work/artifact row {id}");
        }
        assert!(crate::EntityRepository::new(storage.clone())
            .get(space, &independent.base.id)
            .await
            .unwrap()
            .is_some());
    }
}

#[test]
fn native_predecessor_history_expression_parses_for_both_archive_tables() {
    for table in ["work_item", "work_run"] {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        super::append_history_sql(&mut sql, "history_probe", "$record_id", table);
        sql.push_str("COMMIT TRANSACTION;");
        surrealdb::sql::parse(&sql)
            .expect("hot/archive predecessor guards must be valid native SQL");
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn native_archive_lookup_preserves_uuid_id_type() {
    let storage = client(false).await;
    for table in [TableName::WorkItem, TableName::WorkRun] {
        let original = RecordId::new_v4(table);
        let expected =
            crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&original));
        let mut response = storage
            .query_bind_checked(
                "RETURN type::thing($archive_table, $original);",
                crate::bindings! {
                    "archive_table" => expected.tb.clone(),
                    "original" => crate::surreal::thing_from_record_id(&original),
                },
            )
            .await
            .unwrap();
        let actual: Option<surrealdb::sql::Thing> = response.take(0).unwrap();
        assert_eq!(actual, Some(expected));
    }
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn embedding_write_fixture(
    embedded: bool,
    space: &str,
) -> (Fixture, WorkItem, SourceEmbeddingWriteAuthority) {
    let mut fixture = fixture(client(embedded).await, space, IngestMode::Retrieval, 1).await;
    let revision = fixture.manifest.documents[0].doc_revision_id.clone();
    let id = source_embedding_work_id_for_plan(
        space,
        &fixture.manifest.publication,
        &fixture.manifest.configuration,
        &revision,
    );
    fixture.manifest.embedding_work_item_ids = vec![id.clone()];
    let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
    repository
        .set_operation_plan(fixture.manifest.clone())
        .await
        .unwrap();
    let mut payload = plan_payload(&fixture.manifest);
    payload["revision_id"] = serde_json::json!(revision.to_string());
    let mut item = work_item(space, "embed_source_revision", payload);
    item.base.id = id;
    let items = crate::WorkItemRepository::new(fixture.storage.clone());
    items.create(item.clone()).await.unwrap();
    let claim = items
        .claim_with_lease(
            space,
            &item.base.id,
            "original-embedding-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let authority = repository
        .prepare_embedding_write(
            space,
            &fixture.manifest.publication,
            &fixture.manifest.configuration,
            &crate::work_attempt_repository::fence_from_claim(&claim).unwrap(),
            &revision,
        )
        .await
        .unwrap();
    (fixture, claim, authority)
}

fn source_vector(fixture: &Fixture, vector: Vec<f32>) -> NodeEmbedding {
    let mut base = fixture.nodes[0].base.clone();
    base.id = RecordId::new_v4(TableName::ChunkEmbedding);
    NodeEmbedding {
        base,
        node: Link::new(fixture.nodes[0].base.id.clone()),
        model: embedding_model(&fixture.manifest).unwrap(),
        dims: 3,
        vector,
    }
}

#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
async fn stored_source_vector(fixture: &Fixture) -> Option<Vec<f32>> {
    let node = &fixture.nodes[0];
    let model = embedding_model(&fixture.manifest).unwrap();
    if let Some(store) = fixture.storage.embedded_store() {
        let id = crate::chunk_embedding_repository::embedding_record_id_for(
            &node.base.id,
            &fixture.manifest.space_id,
            &node.base.security,
            &model,
        );
        return store
            .mutate(|tx| {
                Ok(tx
                    .get::<NodeEmbedding>(&fixture.manifest.space_id, &id, true)?
                    .map(|embedding| embedding.vector))
            })
            .await
            .unwrap();
    }
    #[derive(Deserialize)]
    struct VectorRow {
        vector: Vec<f32>,
    }
    let id = surrealdb::sql::Thing::from((
        fixture.manifest.embeddings[0].partition_table.clone(),
        surrealdb::sql::Id::Uuid(node.base.id.id().into()),
    ));
    let mut response = fixture
        .storage
        .query_bind_checked("SELECT vector FROM ONLY $id", crate::bindings! {"id" => id})
        .await
        .unwrap();
    let row: Option<VectorRow> = response.take(0).unwrap();
    row.map(|row| row.vector)
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn source_vector_write_rejects_original_claim_after_successor_commits() {
    for embedded in [true, false] {
        let (fixture, claim, original) =
            embedding_write_fixture(embedded, "vector-claim-fence").await;
        let space = &fixture.manifest.space_id;
        let ids = vec![fixture.nodes[0].base.id.clone()];
        let model = embedding_model(&fixture.manifest).unwrap();
        let vectors =
            crate::NodeEmbeddingRepository::new(fixture.storage.clone(), PartitionManager::new());
        vectors
            .store_source_embeddings_and_mark_nodes(
                space,
                &ids,
                &model,
                Utc::now(),
                vec![source_vector(&fixture, vec![1.0, 0.0, 0.0])],
                &original,
            )
            .await
            .unwrap();
        let items = crate::WorkItemRepository::new(fixture.storage.clone());
        items
            .requeue_with_delay(
                space,
                &claim.base.id,
                claim.leased_by.as_deref().unwrap(),
                0,
                Some("fixture interrupted before completion".into()),
            )
            .await
            .unwrap()
            .unwrap();
        let successor = items
            .claim_with_lease(
                space,
                &claim.base.id,
                "successor-embedding-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let next = SourcePublicationManifestRepository::new(fixture.storage.clone())
            .prepare_embedding_write(
                space,
                &fixture.manifest.publication,
                &fixture.manifest.configuration,
                &crate::work_attempt_repository::fence_from_claim(&successor).unwrap(),
                &fixture.manifest.documents[0].doc_revision_id,
            )
            .await
            .unwrap();
        vectors
            .store_source_embeddings_and_mark_nodes(
                space,
                &ids,
                &model,
                Utc::now(),
                vec![source_vector(&fixture, vec![0.0, 1.0, 0.0])],
                &next,
            )
            .await
            .unwrap();
        assert!(vectors
            .store_source_embeddings_and_mark_nodes(
                space,
                &ids,
                &model,
                Utc::now(),
                vec![source_vector(&fixture, vec![0.0, 0.0, 1.0])],
                &original
            )
            .await
            .is_err());
        assert!(vectors
            .store_source_embeddings_and_mark_nodes(
                space,
                &[],
                &model,
                Utc::now(),
                vec![],
                &original
            )
            .await
            .is_err());
        assert_eq!(
            stored_source_vector(&fixture).await,
            Some(vec![0.0, 1.0, 0.0])
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn source_vector_write_rechecks_configuration_and_actual_node_input() {
    for embedded in [true, false] {
        for fault in 0..4 {
            let (fixture, _, authority) =
                embedding_write_fixture(embedded, "vector-input-fence").await;
            if fault == 0 {
                next_configuration(&fixture).await;
            } else if fault == 1 {
                let mut node = fixture.nodes[0].clone();
                node.span.end -= 1;
                crate::DocNodeRepository::new(fixture.storage.clone())
                    .update(&fixture.manifest.space_id, &node.base.id.clone(), node)
                    .await
                    .unwrap();
            } else {
                let revisions = crate::DocRevisionRepository::new(fixture.storage.clone());
                let mut revision = revisions
                    .get(&fixture.manifest.space_id, fixture.nodes[0].revision.id())
                    .await
                    .unwrap()
                    .unwrap();
                if fault == 2 {
                    revision.base.security.level += 1;
                } else {
                    revision.renderer.version.push_str("-changed");
                }
                revisions
                    .update(
                        &fixture.manifest.space_id,
                        &revision.base.id.clone(),
                        revision,
                    )
                    .await
                    .unwrap();
            }
            let vectors = crate::NodeEmbeddingRepository::new(
                fixture.storage.clone(),
                PartitionManager::new(),
            );
            assert!(vectors
                .store_source_embeddings_and_mark_nodes(
                    &fixture.manifest.space_id,
                    &[fixture.nodes[0].base.id.clone()],
                    &embedding_model(&fixture.manifest).unwrap(),
                    Utc::now(),
                    vec![source_vector(&fixture, vec![1.0, 0.0, 0.0])],
                    &authority
                )
                .await
                .is_err());
            assert_eq!(stored_source_vector(&fixture).await, None);
            assert_eq!(
                crate::DocNodeRepository::new(fixture.storage.clone())
                    .get(&fixture.manifest.space_id, &fixture.nodes[0].base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .embedding_model,
                None
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn source_vector_write_rejects_foreign_envelope_and_unplanned_nodes_atomically() {
    for embedded in [true, false] {
        let (fixture, _, authority) =
            embedding_write_fixture(embedded, "vector-envelope-fence").await;
        let vectors =
            crate::NodeEmbeddingRepository::new(fixture.storage.clone(), PartitionManager::new());
        let model = embedding_model(&fixture.manifest).unwrap();
        let mut changed = source_vector(&fixture, vec![1.0, 0.0, 0.0]);
        changed
            .base
            .security
            .labels
            .push("not-the-captured-envelope".into());
        assert!(vectors
            .store_source_embeddings_and_mark_nodes(
                &fixture.manifest.space_id,
                &[fixture.nodes[0].base.id.clone()],
                &model,
                Utc::now(),
                vec![changed],
                &authority
            )
            .await
            .is_err());
        let mut other = source_vector(&fixture, vec![1.0, 0.0, 0.0]);
        let other_id = RecordId::new_v4(TableName::DocNode);
        other.node = Link::new(other_id.clone());
        assert!(vectors
            .store_source_embeddings_and_mark_nodes(
                &fixture.manifest.space_id,
                &[other_id],
                &model,
                Utc::now(),
                vec![other],
                &authority
            )
            .await
            .is_err());
        assert_eq!(stored_source_vector(&fixture).await, None);
        assert_eq!(
            crate::DocNodeRepository::new(fixture.storage.clone())
                .get(&fixture.manifest.space_id, &fixture.nodes[0].base.id)
                .await
                .unwrap()
                .unwrap()
                .embedding_model,
            None
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn source_vector_provider_input_must_match_captured_snapshot_before_inference() {
    for embedded in [true, false] {
        let (fixture, _, authority) = embedding_write_fixture(embedded, "provider-input-aba").await;
        let node = &fixture.nodes[0];
        let revision = crate::DocRevisionRepository::new(fixture.storage.clone())
            .get(&fixture.manifest.space_id, node.revision.id())
            .await
            .unwrap()
            .unwrap();
        authority.validate_embedding_input(node, &revision).unwrap();
        let mut transient_node = node.clone();
        transient_node.span.end -= 1;
        assert!(authority
            .validate_embedding_input(&transient_node, &revision)
            .is_err());
        let mut transient_revision = revision.clone();
        transient_revision.text = "transient provider input".into();
        transient_revision.text_hash =
            format!("{:x}", Sha256::digest(transient_revision.text.as_bytes()));
        assert!(authority
            .validate_embedding_input(node, &transient_revision)
            .is_err());
        for fault in 0..3 {
            let mut changed = revision.clone();
            match fault {
                0 => changed.base.security.level += 1,
                1 => changed.renderer.version.push_str("-changed"),
                _ => changed.base.subjects_state = elephant_types::records::SubjectsState::Unknown,
            }
            // Default fixture subjects are already Unknown, so force a distinct
            // completeness value rather than accidentally exercising no change.
            if fault == 2 && changed.base.subjects_state == revision.base.subjects_state {
                changed.base.subjects_state = elephant_types::records::SubjectsState::Complete;
            }
            assert!(authority.validate_embedding_input(node, &changed).is_err());
        }
        // Returning storage to the old value cannot legitimize a provider call
        // with either transient snapshot: it was rejected before inference.
        authority.validate_embedding_input(node, &revision).unwrap();
        let mut previous_batch_marker = node.clone();
        previous_batch_marker.embedding_model = Some(embedding_model(&fixture.manifest).unwrap());
        previous_batch_marker.base.updated_at = Utc::now();
        authority
            .validate_embedding_input(&previous_batch_marker, &revision)
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn reused_published_revision_string_does_not_complete_new_acceptance_plan() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "reaccepted-content-plan",
            IngestMode::Metadata,
            0,
        )
        .await;
        let publications = SourcePublicationRepository::new(fixture.storage.clone());
        let manifest = &fixture.manifest;
        let mut head = publications
            .head(&manifest.space_id, &manifest.publication.source_id)
            .await
            .unwrap()
            .unwrap();
        // This is the pending accepted-instance state after A -> pending B -> A:
        // the published content string matches, but this operation has no certificate.
        head.published_revision = Some(manifest.publication.revision_id.clone());
        if let Some(store) = fixture.storage.embedded_store() {
            store
                .mutate(|tx| {
                    write_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(&manifest.space_id, &manifest.publication.source_id),
                        &manifest.space_id,
                        &manifest.publication.source_id,
                        &head,
                    )
                })
                .await
                .unwrap();
        } else {
            fixture.storage.query_bind_checked("UPDATE $id CONTENT $value", crate::bindings! {
                "id" => control_thing(HEAD_TABLE, &head_key(&manifest.space_id, &manifest.publication.source_id)),
                "value" => control_value(&manifest.space_id, &manifest.publication.source_id, &head).unwrap(),
            }).await.unwrap();
        }
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        repository
            .set_operation_plan(manifest.clone())
            .await
            .unwrap();
        let mut next = next_configuration(&fixture).await;
        let claim = new_version_metadata(&fixture.storage, &mut next).await;
        let replaced = repository
            .replace_operation_plan(
                &manifest.space_id,
                &manifest.publication,
                &manifest.digest().unwrap(),
                next.clone(),
                &crate::work_attempt_repository::fence_from_claim(&claim).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replaced, next);
        assert_eq!(
            publications
                .head(&manifest.space_id, &manifest.publication.source_id)
                .await
                .unwrap()
                .unwrap(),
            head
        );
        assert!(repository
            .completion(&manifest.space_id, &manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}

async fn stage_actual_metadata_acceptance(
    storage: &StorageClient,
    accepted: &elephant_types::publication::OperationReceipt,
    configuration: SpaceConfigToken,
) -> SourcePublicationManifest {
    let mut manifest = SourcePublicationManifest {
        historical_adoption: None,
        space_id: accepted.space_id.clone(),
        publication: PublicationFence {
            source_id: accepted.source_id.clone(),
            generation: accepted.source_generation.clone(),
            revision_id: accepted.desired_revision.clone().unwrap(),
            operation_id: accepted.operation_id.clone(),
        },
        mode: IngestMode::Metadata,
        configuration,
        embedding_profile: None,
        documents: Vec::new(),
        metadata_artifact_ids: Vec::new(),
        planning_artifact_ids: Vec::new(),
        bundles: Vec::new(),
        embeddings: Vec::new(),
        embedding_work_item_ids: Vec::new(),
    };
    let stage = new_version_metadata(storage, &mut manifest).await;
    complete_claim(storage, &stage).await;
    SourcePublicationManifestRepository::new(storage.clone())
        .set_operation_plan(manifest.clone())
        .await
        .unwrap();
    manifest
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn publish_actual_metadata_acceptance(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
) -> crate::canonical_commit::CanonicalCommitReceipt {
    use crate::canonical_commit::{
        CanonicalCommit, CanonicalPublicationContext, CanonicalRecord, CanonicalRepository,
    };
    let claim = claim_plan_work(
        storage,
        manifest,
        "finalize_source_revision",
        plan_payload(manifest),
    )
    .await;
    let attempt = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
    let requirements = SourcePublicationManifestRepository::new(storage.clone())
        .requirements_for_attempt(&manifest.space_id, &manifest.publication, &attempt)
        .await
        .unwrap();
    let canonical = CanonicalRepository::new(storage.clone());
    let mut plan = canonical
        .prepare_source_replacement(&manifest.space_id, &manifest.publication)
        .await
        .unwrap();
    let artifacts = crate::WorkArtifactRepository::new(storage.clone());
    for id in &manifest.metadata_artifact_ids {
        let bundle = artifacts
            .get(&manifest.space_id, id)
            .await
            .unwrap()
            .unwrap();
        let candidate_id =
            RecordId::from_str(bundle.data["source_artifact_id"].as_str().unwrap()).unwrap();
        let candidate = artifacts
            .get(&manifest.space_id, &candidate_id)
            .await
            .unwrap()
            .unwrap();
        for before in [bundle, candidate] {
            let mut after = before.clone();
            after.status = ArtifactStatus::Committed;
            after.committed_at = Some(Utc::now());
            after.base.updated_at = Utc::now();
            plan.push(
                Some(CanonicalRecord::WorkArtifact(Box::new(before))),
                Some(CanonicalRecord::WorkArtifact(Box::new(after))),
            )
            .unwrap();
        }
    }
    let before = crate::SourceRepository::new(storage.clone())
        .get(&manifest.space_id, &manifest.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    let mut after = before.clone();
    after.current_ingest_mode = Some(IngestMode::Metadata);
    after.latest_ingest_decision = requirements.published_ingest_decision().unwrap();
    after.lifecycle_status = elephant_types::records::source::SourceLifecycleStatus::MetadataReady;
    after.base.updated_at = Utc::now();
    plan.push(
        Some(CanonicalRecord::Source(Box::new(before))),
        Some(CanonicalRecord::Source(Box::new(after))),
    )
    .unwrap();
    let receipt = canonical
        .commit(CanonicalCommit {
            space_id: manifest.space_id.clone(),
            commit_id: requirements.commit_id(),
            input_fingerprint: manifest.digest().unwrap(),
            replay_output: None,
            publication: Some(CanonicalPublicationContext {
                source: manifest.publication.clone(),
                attempt,
            }),
            plan,
            finalization: Some(requirements),
            maintenance: None,
        })
        .await
        .unwrap();
    complete_claim(storage, &claim).await;
    receipt
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn actual_published_a_pending_b_reaccepted_a_replans_and_completes_new_instance() {
    use crate::source_publication::AcceptSourceRevision;
    use elephant_types::publication::{OperationState, SourcePrecondition};
    for embedded in [true, false] {
        let storage = client(embedded).await;
        let space = "actual-aba-replan";
        let configs = crate::SpaceConfigRepository::new(storage.clone());
        configs
            .create(SpaceConfig {
                base: base(TableName::SpaceConfig, space),
                embedding_provider: "openai".into(),
                embedding_model: "fixture-embedding".into(),
                embedding_dims: 3,
                purpose: None,
                skill: None,
                processing: Some(processing()),
            })
            .await
            .unwrap();
        let mut source_a = Source::new(
            base(TableName::Source, space),
            SourceType::Manual,
            "immutable-a".into(),
        );
        source_a.external_id = Some("consumer-stable-aba".into());
        source_a.metadata = serde_json::json!({"_manual_doc":{"text":"accepted A"}});
        let publications = SourcePublicationRepository::new(storage.clone());
        let initial_request = AcceptSourceRevision {
            source: source_a.clone(),
            revision_id: "revision-a".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-a".into(),
            request_fingerprint: "request-a".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        };
        let accepted_a = publications.accept(initial_request.clone()).await.unwrap();
        let first = stage_actual_metadata_acceptance(
            &storage,
            &accepted_a,
            configs.resolve_space(space).await.unwrap().token,
        )
        .await;
        let first_receipt = publish_actual_metadata_acceptance(&storage, &first).await;
        assert!(first_receipt.effect_ids.contains(&accepted_a.source_id));
        assert!(
            publications.support_for_record(space, &accepted_a.source_id).await.unwrap().is_empty(),
            "accepted Source input is governed by its head and immutable revision, not a derived self-support row",
        );
        let first_completed = publications
            .operation(space, &accepted_a.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_completed.state, OperationState::Completed);
        let mut source_b = source_a.clone();
        source_b.hash = "immutable-b".into();
        source_b.metadata = serde_json::json!({"_manual_doc":{"text":"pending B"}});
        let b = publications
            .accept(AcceptSourceRevision {
                source: source_b,
                revision_id: "revision-b".into(),
                expected: SourcePrecondition::Generation(accepted_a.source_generation.clone()),
                idempotency_key: "pending-b".into(),
                request_fingerprint: "request-b".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: publications
                    .authorization_snapshot(space, &accepted_a.source_id)
                    .await
                    .unwrap(),
                authorized_document: None,
            })
            .await
            .unwrap();
        let next_a = publications
            .accept(AcceptSourceRevision {
                source: source_a,
                revision_id: "revision-a".into(),
                expected: SourcePrecondition::Generation(b.source_generation.clone()),
                idempotency_key: "new-a-instance".into(),
                request_fingerprint: "request-a".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: publications
                    .authorization_snapshot(space, &accepted_a.source_id)
                    .await
                    .unwrap(),
                authorized_document: None,
            })
            .await
            .unwrap();
        assert_ne!(next_a.operation_id, accepted_a.operation_id);
        assert_ne!(next_a.source_generation, accepted_a.source_generation);
        let head = publications
            .head(space, &next_a.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.desired_revision.as_deref(), Some("revision-a"));
        assert_eq!(head.published_revision.as_deref(), Some("revision-a"));
        assert_eq!(head.operation_id, next_a.operation_id);
        assert_eq!(
            publications.accept(initial_request).await.unwrap(),
            first_completed
        );
        assert_eq!(
            publications
                .head(space, &next_a.source_id)
                .await
                .unwrap()
                .unwrap(),
            head
        );

        let current = stage_actual_metadata_acceptance(
            &storage,
            &next_a,
            configs.resolve_space(space).await.unwrap().token,
        )
        .await;
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        assert!(manifests
            .completion(space, &current.publication)
            .await
            .unwrap()
            .is_none());
        assert!(manifests
            .completion(space, &first.publication)
            .await
            .unwrap()
            .is_some());
        let fixture = Fixture {
            storage: storage.clone(),
            manifest: current.clone(),
            nodes: Vec::new(),
        };
        let mut replanned = next_configuration(&fixture).await;
        let stage = new_version_metadata(&storage, &mut replanned).await;
        let attempt = crate::work_attempt_repository::fence_from_claim(&stage).unwrap();
        manifests
            .replace_operation_plan(
                space,
                &current.publication,
                &current.digest().unwrap(),
                replanned.clone(),
                &attempt,
            )
            .await
            .unwrap();
        complete_claim(&storage, &stage).await;
        assert_eq!(
            publications
                .head(space, &next_a.source_id)
                .await
                .unwrap()
                .unwrap(),
            head
        );
        let final_receipt = publish_actual_metadata_acceptance(&storage, &replanned).await;
        assert_ne!(final_receipt.commit_id, first_receipt.commit_id);
        let completed = publications
            .operation(space, &next_a.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, OperationState::Completed);
        let certificate = manifests
            .completion(space, &replanned.publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(certificate.publication, replanned.publication);
        assert_eq!(certificate.commit_id, final_receipt.commit_id);
        let lookup: SourcePublicationCompletion = publications
            .read_control(
                COMPLETION_TABLE,
                &progress::published_completion_key(space, &next_a.source_id),
                space,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            lookup, certificate,
            "same-revision completion must advance the derived lookup to the actual new acceptance"
        );
        assert_ne!(lookup.publication, first.publication);
        assert_eq!(
            manifests
                .completion(space, &first.publication)
                .await
                .unwrap()
                .unwrap()
                .commit_id,
            first_receipt.commit_id,
            "the original immutable completion remains independently identifiable"
        );
    }
}

#[path = "source_publication_render_tests.rs"]
mod render_proof;

#[path = "../../../tests/recall_preview/source_lifecycle_contact_oracles.rs"]
mod independent_contact_erasure_oracles;

/// A filtered-out twin must never occupy the single readiness probe slot.
/// The exact target envelope/vector guards still run before the index probe.
#[tokio::test]
async fn native_publication_index_probe_ignores_deleted_and_foreign_vector_twins() {
    let storage = client(false).await;
    let target = embedding_fixture(
        storage.clone(),
        "index-target-space",
        IngestMode::Retrieval,
        1,
    )
    .await;
    let repository = SourcePublicationManifestRepository::new(storage.clone());
    repository
        .set_operation_plan(target.manifest.clone())
        .await
        .unwrap();
    let table = &target.manifest.embeddings[0].partition_table;
    let key = PartitionKey::from_table_name(table).unwrap();
    storage
        .query_checked(&PartitionManager::create_partition_ddl(&key))
        .await
        .unwrap();
    // Deliberately malformed foreign row INSIDE the exact target partition.
    // Insert it first so the old KNN k=1 implementation selects it before
    // applying the per-row space predicate. No production input is relabeled.
    storage.query_checked(&format!("CREATE {table}:foreign_twin SET vector=[1,0,0], space_id='foreign-fixture', vector_norm=1")).await.unwrap();
    embed(&target, 0, vec![1.0, 0.0, 0.0]).await;
    let requirements = repository
        .requirements(&target.manifest.space_id, &target.manifest.publication)
        .await
        .unwrap();
    repository
        .check_ready(&requirements)
        .await
        .expect("foreign identical vector cannot hide target index entry");
    storage.query_bind_checked(
        &format!("UPDATE ONLY {table}:foreign_twin SET space_id = $target_space, deleted_at = time::now()"),
        crate::bindings! {"target_space" => target.manifest.space_id.clone()},
    ).await.unwrap();
    let mut observed = storage
        .query_checked(&format!(
            "SELECT VALUE deleted_at != NONE FROM ONLY {table}:foreign_twin"
        ))
        .await
        .unwrap();
    assert_eq!(
        observed.take::<Option<bool>>(0).unwrap(),
        Some(true),
        "the exact indexed twin is now deleted in the target space"
    );
    repository
        .check_ready(&requirements)
        .await
        .expect("deleted identical vector cannot hide target index entry");
    storage
        .query_checked(&format!("REMOVE INDEX {table}_hnsw ON TABLE {table}"))
        .await
        .unwrap();
    drop(target);
    drop(storage);
    assert!(
        repository.check_ready(&requirements).await.is_err(),
        "actual missing index must still refuse publication"
    );
    drop(repository);
}
