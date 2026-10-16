//! Real storage/transport fixtures for retained retrieval regression tests.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::panic
)]

use chrono::Utc;
use elephant_embeddings::{governed::CredentialResolver, EmbeddingPolicy};
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, Repository, SpaceConfigRepository,
    StorageClient, StorageResult,
};
use elephant_types::provider_binding::*;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::{Assertion, Doc, DocRevision, SpaceConfig};
use elephant_types::{Link, RecordId, TableName};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Install the production native source-support indexes and validation fence.
/// Embedded fixtures already maintain their support index inside the store.
///
/// # Errors
/// Returns any production schema statement failure.
pub async fn initialize_source_support(storage: &StorageClient) -> StorageResult<()> {
    if !storage.is_embedded() {
        storage
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await?;
    }
    Ok(())
}

/// Assertion fixtures persist the immutable passage they declare instead of
/// relying on references to absent revisions or placeholder hashes.
pub struct FixtureAssertions {
    storage: StorageClient,
    repository: AssertionRepository,
}
impl FixtureAssertions {
    pub fn new(storage: StorageClient) -> Self {
        Self {
            repository: AssertionRepository::new(storage.clone()),
            storage,
        }
    }
    pub async fn create(&self, mut assertion: Assertion) -> StorageResult<Assertion> {
        let text = format!(
            "{} {} {}",
            assertion.subject_text,
            assertion.predicate_raw,
            assertion
                .object_text
                .clone()
                .or_else(|| assertion.object_literal.as_ref().map(ToString::to_string))
                .unwrap_or_default()
        );
        let mut doc_base = assertion.base.clone();
        doc_base.id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: doc_base,
            title: "Declared assertion fixture".into(),
            text: text.clone(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: assertion.base.id.to_string(),
            metadata: json!({}),
        };
        DocRepository::new(self.storage.clone())
            .create(doc.clone())
            .await?;
        let mut revision_base = doc.base.clone();
        revision_base.id = assertion.evidence.revision.id().clone();
        let digest = hex::encode(Sha256::digest(text.as_bytes()));
        let revision = DocRevision {
            base: revision_base,
            doc: Link::new(doc.base.id),
            text: text.clone(),
            text_hash: digest.clone(),
            renderer: RendererMetadata {
                name: "assertion-fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        DocRevisionRepository::new(self.storage.clone())
            .create(revision)
            .await?;
        assertion.evidence.start = 0;
        assertion.evidence.end = u32::try_from(text.len()).expect("bounded test passage");
        assertion.evidence.quote_hash = digest;
        assertion.evidence.node = None;
        self.repository.create(assertion).await
    }
    pub async fn update(
        &self,
        space: &str,
        id: &RecordId,
        assertion: Assertion,
    ) -> StorageResult<Assertion> {
        self.repository.update(space, id, assertion).await
    }
}

#[derive(Debug)]
struct TestCredentials;
impl CredentialResolver for TestCredentials {
    fn resolve(&self, reference: &str) -> elephant_embeddings::EmbeddingResult<String> {
        assert_eq!(reference, "RETRIEVAL_TEST_ONLY");
        Ok("loopback-only-fixture".into())
    }
}

/// Actual loopback `OpenAI` transport, reached only through a persisted approval.
/// Vectors are declared ranking inputs, not a semantic-quality oracle.
pub struct QueryFixture {
    pub policy: EmbeddingPolicy,
    task: tokio::task::JoinHandle<()>,
    physical_requests: Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for QueryFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl QueryFixture {
    pub async fn install(
        storage: &StorageClient,
        space: &str,
        entries: Vec<(&str, Vec<f32>)>,
    ) -> Self {
        Self::install_with_gate(storage, space, entries, None).await
    }
    /// Gate complete localhost responses for queued-fill authorization controls.
    pub async fn install_with_gate(
        storage: &StorageClient,
        space: &str,
        entries: Vec<(&str, Vec<f32>)>,
        response_gate: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Self {
        let entries: BTreeMap<String, Vec<f32>> = entries
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect();
        let dims = entries.values().next().map_or(3, Vec::len);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let physical_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_requests = physical_requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0, "complete fixture request headers");
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                        break end + 4;
                    }
                    assert!(bytes.len() < 65536);
                };
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                assert!(headers.starts_with("POST /v1/embeddings "));
                assert!(headers
                    .to_lowercase()
                    .contains("authorization: bearer loopback-only-fixture"));
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < header_end + length {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let request: serde_json::Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
                assert_eq!(request["model"], "ranking-fixture");
                observed_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let data: Vec<_> = request["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(index, text)| {
                        let text = text.as_str().unwrap();
                        let vector = entries
                            .get(text)
                            .unwrap_or_else(|| panic!("unexpected original query: {text}"));
                        json!({"object":"embedding", "index":index, "embedding":vector})
                    })
                    .collect();
                let body = json!({"object":"list", "model":"ranking-fixture", "data":data, "usage":{"prompt_tokens":1,"total_tokens":1}}).to_string();
                if let Some(gate) = &response_gate {
                    gate.acquire().await.unwrap().forget();
                }
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            space.into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let config = SpaceConfig {
            base,
            embedding_provider: "openai".into(),
            embedding_model: "ranking-fixture".into(),
            embedding_dims: i32::try_from(dims).unwrap(),
            purpose: None,
            skill: None,
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id: "fixture-policy".into(),
                connections: BTreeMap::from([(
                    "connection".into(),
                    ProviderConnection {
                        id: "connection".into(),
                        provider: "openai".into(),
                        account_id: format!("fixture-{space}"),
                        endpoint,
                        credential_ref: Some("RETRIEVAL_TEST_ONLY".into()),
                        credential_generation: 1,
                        revoked: false,
                    },
                )]),
                profiles: BTreeMap::from([(
                    "profile".into(),
                    ModelProfile {
                        id: "profile".into(),
                        connection_id: "connection".into(),
                        model: "ranking-fixture".into(),
                        model_version: "1".into(),
                        dimensions: Some(dims),
                        preprocessing: "text-v1".into(),
                        summary: None,
                    },
                )]),
                bindings: BTreeMap::from([
                    (
                        ModelOperation::QueryEmbedding,
                        OperationBinding {
                            id: "query".into(),
                            profile_id: "profile".into(),
                            revoked: false,
                        },
                    ),
                    // Fixture vectors still pass the real guarded publication
                    // path, which requires document approval for this profile.
                    (
                        ModelOperation::DocumentEmbedding,
                        OperationBinding {
                            id: "document".into(),
                            profile_id: "profile".into(),
                            revoked: false,
                        },
                    ),
                ]),
                active_embedding_profile_id: Some("profile".into()),
                pending_reindex: None,
                completed_embedding_reindex: None,
            }),
        };
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .unwrap();
        let (policy, _, _) = elephant_pipeline::provider_dispatch::ProviderDispatchContext::new(
            SpaceConfigRepository::new(storage.clone()),
            space,
        )
        .with_credentials(Arc::new(TestCredentials))
        .embedding(ModelOperation::QueryEmbedding)
        .await
        .unwrap();
        Self {
            policy,
            task,
            physical_requests,
        }
    }
    /// Complete physical requests observed by the localhost transport.
    pub fn physical_requests(&self) -> usize {
        self.physical_requests
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        elephant_pipeline::provider_dispatch::with_dispatch_credentials(
            Arc::new(TestCredentials),
            future,
        )
        .await
    }
}
