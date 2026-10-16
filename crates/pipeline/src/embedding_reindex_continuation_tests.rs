//! Governed production page processor with a bounded localhost-only transport.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use elephant_embeddings::governed::CredentialResolver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_storage::{DocRepository, EntityRepository};
use elephant_types::records::doc_node::TextSpan;
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{BaseFields, Doc, DocNode, DocRevision, Entity, SpaceConfig};
use elephant_types::TableName;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SPACE: &str = "continuation-fixture";
#[derive(Debug)]
struct LocalCredentials;
impl CredentialResolver for LocalCredentials {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        if reference == "local-test-key" {
            Ok("synthetic-local-credential".into())
        } else {
            Err(EmbeddingError::ProviderError(
                "unexpected fixture credential".into(),
            ))
        }
    }
}
struct Fixture {
    client: StorageClient,
    configs: SpaceConfigRepository,
    config_id: RecordId,
    reindex_id: String,
    requests: Arc<Mutex<Vec<Vec<String>>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        SPACE.into(),
        "deterministic-local-fixture".into(),
    )
}
async fn request(socket: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = Vec::new();
    let (start, length) = loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0 && bytes.len() + n <= 128 * 1024);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            assert!(headers.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            assert!(end <= 16 * 1024 && length <= 64 * 1024);
            break (end + 4, length);
        }
    };
    while bytes.len() < start + length {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0 && bytes.len() + n <= 128 * 1024);
        bytes.extend_from_slice(&chunk[..n]);
    }
    serde_json::from_slice(&bytes[start..start + length]).unwrap()
}
impl Fixture {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            for _ in 0..32 {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let body = request(&mut socket).await;
                    assert_eq!(body["model"], "fixture-v2");
                    let texts: Vec<String> = serde_json::from_value(body["input"].clone()).unwrap();
                    assert_eq!(texts.len(), 1, "page size is exactly one");
                    captured.lock().unwrap().push(texts.clone());
                    let data: Vec<_> = texts.iter().enumerate().map(|(index, text)| json!({"index":index,"embedding":[1.0, if text.len()%2 == 0 {0.0} else {1.0}]})).collect();
                    let response = serde_json::to_vec(&json!({"data":data,"model":"fixture-v2","usage":{"prompt_tokens":1,"total_tokens":1}})).unwrap();
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).await.unwrap();
                    socket.write_all(&response).await.unwrap();
                }).await.expect("bounded local transport");
            }
        });
        let client = StorageClient::new_embedded().await.unwrap();
        let configs = SpaceConfigRepository::new(client.clone());
        configs.create(SpaceConfig {
            base: base(TableName::SpaceConfig), embedding_provider: "openai".into(),
            embedding_model: "fixture-v1".into(), embedding_dims: 2,
            processing: Some(serde_json::from_value(json!({
                "generation":1,"index_generation":0,"policy_id":"fixture-policy",
                "connections":{"local":{"id":"local","provider":"openai","account_id":"local-fixture","endpoint":endpoint,"credential_ref":"local-test-key","credential_generation":1,"revoked":false}},
                "profiles":{
                    "v1":{"id":"v1","connection_id":"local","model":"fixture-v1","model_version":"1","dimensions":2,"preprocessing":"text-v1"},
                    "v2":{"id":"v2","connection_id":"local","model":"fixture-v2","model_version":"2","dimensions":2,"preprocessing":"text-full-v2"},
                    "v3":{"id":"v3","connection_id":"local","model":"fixture-v3","model_version":"3","dimensions":2,"preprocessing":"text-full-v2"}},
                "bindings":{
                    "query_embedding":{"id":"query","profile_id":"v1","revoked":false},
                    "document_embedding":{"id":"document","profile_id":"v1","revoked":false},
                    "reindex_embedding":{"id":"reindex","profile_id":"v2","revoked":false}},
                "active_embedding_profile_id":"v1","pending_reindex":null
            })).unwrap()), purpose: None, skill: None,
        }).await.unwrap();
        for i in 0..3 {
            let text = format!("exact original node {i}");
            let doc = Doc {
                base: base(TableName::Doc),
                title: format!("doc-{i}"),
                text: text.clone(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: format!("fixture-{i}"),
                metadata: Value::Null,
            };
            let revision = DocRevision {
                base: base(TableName::DocRevision),
                doc: Link::new(doc.base.id.clone()),
                text: text.clone(),
                text_hash: hex::encode(Sha256::digest(text.as_bytes())),
                renderer: RendererMetadata {
                    name: "fixture".into(),
                    version: "1".into(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: doc.base.created_at,
            };
            let node = DocNode {
                base: base(TableName::DocNode),
                revision: Link::new(revision.base.id.clone()),
                role: NodeRole::Chunk,
                span: TextSpan::new(0, u32::try_from(text.len()).unwrap()),
                node_key: format!("node-{i}"),
                text: None,
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
                .create(node)
                .await
                .unwrap();
            EntityRepository::new(client.clone())
                .create(Entity::new(
                    base(TableName::Entity),
                    EntityType::new("person"),
                    format!("entity-{i}"),
                ))
                .await
                .unwrap();
        }
        let original = configs.get_by_space_id(SPACE).await.unwrap().unwrap();
        let pending = configs
            .start_embedding_reindex(&original, "v2")
            .await
            .unwrap();
        let reindex_id = pending
            .processing
            .as_ref()
            .unwrap()
            .pending_reindex
            .as_ref()
            .unwrap()
            .id
            .clone();
        Self {
            client,
            configs,
            config_id: pending.base.id,
            reindex_id,
            requests,
            server,
        }
    }
    async fn page(&self) -> PipelineResult<ReindexProgress> {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::provider_dispatch::with_dispatch_credentials(
                Arc::new(LocalCredentials),
                process_embedding_reindex_for_target(
                    &self.client,
                    SPACE,
                    1,
                    &self.reindex_id,
                    &self.config_id,
                ),
            ),
        )
        .await
        .expect("page deadline")
    }
    async fn config(&self) -> SpaceConfig {
        self.configs.get_by_space_id(SPACE).await.unwrap().unwrap()
    }
    fn calls(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[tokio::test]
async fn administrative_pages_continue_nodes_then_entities_without_activating() {
    let f = Fixture::new().await;
    let mut progress = Vec::new();
    for _ in 0..10 {
        let page = f.page().await.unwrap();
        let complete = page.ready_to_activate;
        progress.push(page);
        if complete {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(progress.len(), 8, "3nodes+end page,3entities+end page");
    assert!(progress.last().unwrap().ready_to_activate);
    assert_eq!(progress.last().unwrap().embedded_nodes, 3);
    assert_eq!(progress.last().unwrap().embedded_entities, 3);
    assert_eq!(f.calls(), 6);
    let requests = f.requests.lock().unwrap().clone();
    let mut nodes: Vec<_> = requests.iter().take(3).map(|v| v[0].clone()).collect();
    nodes.sort();
    assert_eq!(
        nodes,
        vec![
            "exact original node 0",
            "exact original node 1",
            "exact original node 2"
        ]
    );
    let configured = f.config().await;
    assert_eq!(
        configured
            .processing
            .as_ref()
            .unwrap()
            .active_embedding_profile_id
            .as_deref(),
        Some("v1")
    );
    let binding = f
        .configs
        .authorize_model_operation(SPACE, ModelOperation::ReindexEmbedding, None)
        .await
        .unwrap();
    let policy = elephant_embeddings::governed::policy_for_binding(&binding).unwrap();
    let hashes=elephant_storage::entity_profile_embedding_repository::EntityProfileEmbeddingRepository::new(f.client.clone()).text_hashes(SPACE,&policy.model,2).await.unwrap();
    assert_eq!(
        hashes.len(),
        3,
        "successful responses were durably persisted"
    );
}

#[tokio::test]
async fn next_page_refuses_revoked_approval_profile_and_replaced_target_without_send() {
    for change in ["revoked", "profile", "target"] {
        let f = Fixture::new().await;
        assert_eq!(f.page().await.unwrap().embedded_nodes, 1);
        assert_eq!(f.calls(), 1);
        let replaced_configuration = RecordId::new_v4(TableName::SpaceConfig);
        assert!(process_embedding_reindex_for_target(
            &f.client,
            SPACE,
            1,
            &f.reindex_id,
            &replaced_configuration
        )
        .await
        .is_err());
        assert_eq!(
            f.calls(),
            1,
            "configuration identity mismatch refuses before send"
        );
        let original = f.config().await;
        let before_progress =
            serde_json::to_value(&original.processing.as_ref().unwrap().pending_reindex).unwrap();
        let original = if change == "target" {
            f.configs.cancel_embedding_reindex(&original).await.unwrap()
        } else {
            original
        };
        let mut proposed = original.clone();
        let processing = proposed.processing.as_mut().unwrap();
        processing.generation += 1;
        let grant = processing
            .bindings
            .get_mut(&ModelOperation::ReindexEmbedding)
            .unwrap();
        if change == "revoked" {
            grant.revoked = true;
        } else {
            grant.profile_id = "v3".into();
        }
        let changed = f
            .configs
            .replace_authorized(&original, proposed)
            .await
            .unwrap();
        let changed = if change == "target" {
            f.configs
                .start_embedding_reindex(&changed, "v3")
                .await
                .unwrap()
        } else {
            changed
        };
        assert!(f.page().await.is_err(), "{change}");
        assert_eq!(f.calls(), 1, "{change} must refuse before HTTP dispatch");
        let current = f.config().await;
        assert_eq!(
            current, changed,
            "refused page must not mutate configuration/progress"
        );
        if change != "target" {
            assert_eq!(
                serde_json::to_value(&current.processing.as_ref().unwrap().pending_reindex)
                    .unwrap(),
                before_progress
            );
        }
    }
}

#[tokio::test]
async fn content_change_restarts_coverage_before_next_page() {
    let f = Fixture::new().await;
    assert_eq!(f.page().await.unwrap().embedded_nodes, 1);
    let first_request = f.requests.lock().unwrap()[0].clone();
    EntityRepository::new(f.client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "new original content".into(),
        ))
        .await
        .unwrap();
    let result = f.page().await.unwrap();
    assert!(result.restarted_for_content_change);
    assert_eq!(
        result.embedded_nodes, 1,
        "discard old generation progress then persist first current page"
    );
    assert_eq!(result.embedded_entities, 0);
    assert!(!result.ready_to_activate);
    assert_eq!(f.calls(), 2);
    assert_eq!(
        f.requests.lock().unwrap()[1],
        first_request,
        "restart starts at original node boundary"
    );
}
