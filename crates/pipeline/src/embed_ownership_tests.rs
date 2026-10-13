//! No external provider calls: recording fake vectors cross the real worker and storage seam.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use elephant_embeddings::{FakeEmbeddingProvider, PartitionManager};
use elephant_storage::source_publication::AcceptSourceRevision;
use elephant_storage::{
    DocRepository, Repository, SourcePublicationRepository, SpaceConfigRepository, StorageClient,
};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::{Doc, Source, SourceType, SpaceConfig};
use elephant_types::{RecordId, TableName};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SPACE: &str = "quiet-named-space";
const MODEL: &str = "ownership-test";

async fn accept(client: &StorageClient, source: &RecordId) {
    let mut record = Source::new(
        BaseFields::new(source.clone(), SPACE.into(), "test".into()),
        SourceType::Manual,
        "hash-r1".into(),
    );
    record.metadata = serde_json::json!({"_manual_doc":{"text":"hello"}});
    SourcePublicationRepository::new(client.clone())
        .accept(AcceptSourceRevision {
            source: record,
            revision_id: "r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "ownership-r1".into(),
            request_fingerprint: "ownership-r1".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .expect("real source acceptance");
}

async fn fixture() -> (StorageClient, RecordId, DocNode) {
    let client = StorageClient::new_embedded().await.expect("backend");
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":SPACE,
        "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test","deleted_at":null,
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":MODEL,"embedding_dims":3
    }))
    .unwrap();
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await
        .unwrap();
    let source = RecordId::new_v4(TableName::Source);
    let doc = Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            SPACE.into(),
            "test".into(),
        ),
        title: "test".into(),
        text: "hello".into(),
        sidecar_text: None,
        source: Some(Link::new(source.clone())),
        media_attachments: vec![],
        rendition_key: "test".into(),
        metadata: serde_json::json!({}),
    };
    let revision = DocRevision {
        base: BaseFields::new(
            RecordId::new_v4(TableName::DocRevision),
            SPACE.into(),
            "test".into(),
        ),
        doc: Link::new(doc.base.id.clone()),
        text: "hello".into(),
        text_hash: "hash".into(),
        renderer: RendererMetadata {
            name: "test".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    let node = DocNode {
        base: BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            SPACE.into(),
            "test".into(),
        ),
        revision: Link::new(revision.base.id.clone()),
        role: NodeRole::Chunk,
        span: TextSpan::new(0, 5),
        node_key: "test".into(),
        text: Some("hello".into()),
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    DocRepository::new(client.clone())
        .create(doc)
        .await
        .unwrap();
    DocRevisionRepository::new(client.clone())
        .create(revision)
        .await
        .unwrap();
    DocNodeRepository::new(client.clone())
        .create(node.clone())
        .await
        .unwrap();
    (client, source, node)
}

struct RecordingProvider {
    calls: Arc<AtomicUsize>,
    acquire_source: Option<(StorageClient, RecordId)>,
}
#[async_trait::async_trait]
impl EmbeddingProvider for RecordingProvider {
    fn model(&self) -> &str {
        MODEL
    }
    fn dimensions(&self) -> usize {
        3
    }
    async fn embed(
        &self,
        texts: &[String],
    ) -> elephant_embeddings::EmbeddingResult<Vec<EmbeddingVector>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((client, source)) = &self.acquire_source {
            accept(client, source).await;
        }
        FakeEmbeddingProvider::new(MODEL, 3).embed(texts).await
    }
}
fn worker(
    client: &StorageClient,
    calls: &Arc<AtomicUsize>,
    acquire_source: Option<RecordId>,
) -> EmbedWorker<RecordingProvider> {
    EmbedWorker::new(
        DocNodeRepository::new(client.clone()),
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        RecordingProvider {
            calls: calls.clone(),
            acquire_source: acquire_source.map(|source| (client.clone(), source)),
        },
    )
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain both original owned request slices and zero-dispatch coverage assertions for managed nodes."
)]
async fn managed_nodes_never_dispatch_through_generic_embedding_entrypoints() {
    let (client, source, node) = fixture().await;
    accept(&client, &source).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = worker(&client, &calls, None);
    assert_eq!(
        worker
            .process_pending(SPACE, 1)
            .await
            .unwrap()
            .chunks_processed,
        0
    );
    assert!(worker
        .embed_chunks(SPACE, &[node.base.id.clone()])
        .await
        .is_err());
    assert!(worker
        .embed_exact_nodes(SPACE, &[node.base.id.clone()])
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        DocNodeRepository::new(client)
            .get(SPACE, &node.base.id)
            .await
            .unwrap()
            .unwrap()
            .embedding_model,
        None
    );
}

#[tokio::test]
async fn source_acceptance_during_provider_call_refuses_generic_vector_and_marker() {
    let (client, source, node) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(worker(&client, &calls, Some(source))
        .process_pending(SPACE, 1)
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        DocNodeRepository::new(client.clone())
            .get(SPACE, &node.base.id)
            .await
            .unwrap()
            .unwrap()
            .embedding_model,
        None
    );
    assert!(client
        .embedded_store()
        .unwrap()
        .scan::<NodeEmbedding>()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unmanaged_document_still_embeds_and_leaves_the_pending_queue() {
    let (client, _, node) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = worker(&client, &calls, None);
    assert_eq!(
        worker
            .process_pending(SPACE, 1)
            .await
            .unwrap()
            .chunks_processed,
        1
    );
    assert_eq!(
        worker
            .process_pending(SPACE, 1)
            .await
            .unwrap()
            .chunks_processed,
        0
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        DocNodeRepository::new(client.clone())
            .get(SPACE, &node.base.id)
            .await
            .unwrap()
            .unwrap()
            .embedding_model
            .as_deref(),
        Some(MODEL)
    );
    assert_eq!(
        client
            .embedded_store()
            .unwrap()
            .scan::<NodeEmbedding>()
            .await
            .unwrap()
            .len(),
        1
    );
}
