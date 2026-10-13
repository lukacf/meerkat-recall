//! Local physical-request capture for the two operation-specific agent builders.
//! No provider credentials, model responses, external endpoint or live probe.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use elephant_embeddings::governed::CredentialResolver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::BaseFields;
use elephant_types::records::SpaceConfig;
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::provider_dispatch::ProviderDispatchContext;

pub(crate) const SPACE: &str = "governed-reasoning-fixture";

#[derive(Debug)]
struct FixtureCredentials;

impl CredentialResolver for FixtureCredentials {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        if reference == "reasoning-fixture-key" {
            Ok("local-physical-request-fixture".into())
        } else {
            Err(EmbeddingError::ProviderError(
                "Unconfigured fixture key".into(),
            ))
        }
    }
}

pub(crate) struct RequestFixture {
    pub(crate) storage: StorageClient,
    pub(crate) context: ProviderDispatchContext,
    pub(crate) credentials: Arc<dyn CredentialResolver>,
    captured: tokio::task::JoinHandle<Value>,
}

impl RequestFixture {
    pub(crate) async fn new(operation: ModelOperation) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let operation = serde_json::to_value(operation).unwrap();
        let operation = operation.as_str().unwrap();
        let storage = StorageClient::new_embedded().await.unwrap();
        let repository = SpaceConfigRepository::new(storage.clone());
        repository.create(SpaceConfig {
            base: BaseFields::new(RecordId::new_v4(TableName::SpaceConfig), SPACE.into(), "fixture".into()),
            embedding_provider: "openai".into(),
            embedding_model: "unused".into(),
            embedding_dims: 2,
            processing: Some(serde_json::from_value(json!({
                "generation": 1, "index_generation": 0, "policy_id": "fixture-policy",
                "connections": {"account": {"id": "account", "provider": "openai",
                    "account_id": "fixture-account", "endpoint": endpoint,
                    "credential_ref": "reasoning-fixture-key", "credential_generation": 1,
                    "revoked": false}},
                "profiles": {"model": {"id": "model", "connection_id": "account",
                    "model": "gpt-6.1-sol", "model_version": "fixture", "dimensions": null,
                    "preprocessing": "text-v1"}},
                "bindings": {(operation): {"id": operation, "profile_id": "model", "revoked": false}},
                "active_embedding_profile_id": null, "pending_reindex": null
            })).unwrap()),
            purpose: None, skill: None,
        }).await.unwrap();
        let credentials: Arc<dyn CredentialResolver> = Arc::new(FixtureCredentials);
        let context =
            ProviderDispatchContext::new(repository, SPACE).with_credentials(credentials.clone());
        let captured = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let header_end = loop {
                    let mut buffer = [0_u8; 4096];
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0, "SDK request ended before headers");
                    received.extend_from_slice(&buffer[..read]);
                    if let Some(end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                        assert!(end <= 16_384, "fixture header bound");
                        break end + 4;
                    }
                    assert!(received.len() <= 16_384, "fixture header bound");
                };
                let headers = std::str::from_utf8(&received[..header_end]).unwrap();
                assert!(headers.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                let length: usize = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                }).expect("bounded JSON request has a content length");
                assert!(length <= 262_144, "fixture body bound");
                while received.len() < header_end + length {
                    let mut buffer = [0_u8; 4096];
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0, "SDK request ended before its body");
                    received.extend_from_slice(&buffer[..read]);
                    assert!(received.len() <= header_end + length, "unexpected trailing request data");
                }
                let body = serde_json::from_slice(&received[header_end..header_end + length]).unwrap();
                // An explicit non-retryable refusal avoids a synthetic model
                // success or adjudication. Only the actual outgoing JSON counts.
                let denied = br#"{"error":{"message":"local fixture stops after capture","type":"authentication_error"}}"#;
                let response = format!("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", denied.len());
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.write_all(denied).await.unwrap();
                body
            }).await.expect("local request capture deadline")
        });
        Self {
            storage,
            context,
            credentials,
            captured,
        }
    }

    pub(crate) async fn assert_request(self, schema: &str, max_tokens: u32) -> Value {
        let body = self.captured.await.expect("local request capture task");
        assert_eq!(body["model"], "gpt-6.1-sol");
        assert_eq!(body["reasoning"]["effort"], "medium");
        assert_eq!(body["reasoning"]["summary"], "auto");
        assert_eq!(body["max_output_tokens"], max_tokens);
        // The first agent turn carries the compiled output schema in its
        // system instructions. Native text.format is added only on a later
        // extraction turn, which this first-request refusal never reaches.
        let system = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|message| message["role"] == "system")
            .expect("operation system instructions");
        let content = if let Some(text) = system["content"].as_str() {
            text.to_string()
        } else {
            system["content"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| {
                    assert_eq!(part["type"], "input_text");
                    part["text"].as_str().unwrap()
                })
                .collect::<String>()
        };
        let (_, section) = content
            .rsplit_once("<structured_output>\n")
            .expect("compiled operation schema projection");
        let (_, encoded) = section.split_once("\nJSON Schema:\n").unwrap();
        let projected: Value =
            serde_json::from_str(encoded.strip_suffix("\n</structured_output>").unwrap()).unwrap();
        assert_eq!(projected["type"], "object");
        assert_eq!(projected["additionalProperties"], false);
        let mut required: Vec<_> = projected["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap())
            .collect();
        required.sort_unstable();
        match schema {
            "assertion_support" => {
                assert_eq!(required, ["decisions"]);
                let decisions = &projected["properties"]["decisions"];
                assert_eq!(decisions["type"], "array");
                assert_eq!(decisions["items"]["additionalProperties"], false);
                assert_eq!(
                    decisions["items"]["required"],
                    json!(["index", "supported"])
                );
                assert_eq!(decisions["items"]["properties"]["index"]["type"], "integer");
                assert_eq!(
                    decisions["items"]["properties"]["supported"]["type"],
                    "boolean"
                );
            }
            "adjudication_verdict" => {
                assert_eq!(required, ["decision", "equivalent_groups", "rationale"]);
                assert_eq!(projected["properties"].as_object().unwrap().len(), 7);
                assert_eq!(
                    projected["properties"]["decision"]["enum"],
                    json!([
                        "corroborate",
                        "single_winner",
                        "coexist",
                        "supersede",
                        "needs_review"
                    ])
                );
                assert_eq!(
                    projected["properties"]["equivalent_groups"]["type"],
                    "array"
                );
                assert_eq!(projected["properties"]["rationale"]["type"], "string");
            }
            _ => panic!("unknown operation schema in request fixture"),
        }
        assert!(!body["tools"].as_array().is_some_and(|tools| tools
            .iter()
            .any(|tool| tool["type"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("web_search")))));
        body
    }
}

pub(crate) fn run<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    // Same bounded stack allowance used by the existing agent factory tests.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}
