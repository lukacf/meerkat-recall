//! Loopback HTTP transport spies. Responses are deliberately non-semantic.
//! Production storage, binding resolution, credentials and HTTP clients remain active.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use axum::extract::{rejection::JsonRejection, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use elephant_embeddings::governed::CredentialResolver;
use elephant_embeddings::{
    EmbeddingError, EmbeddingProvider, EmbeddingResult, HashedEmbeddingProvider,
};
use elephant_migrations::apply_schema;
use elephant_pipeline::provider_dispatch::ProviderDispatchContext;
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::{ModelOperation, SpaceProcessingConfig};
use elephant_types::records::base::BaseFields;
use elephant_types::records::SpaceConfig;
use elephant_types::{RecordId, TableName};
use futures::StreamExt;
use meerkat::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest, Message, UserMessage};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

pub const SPACES: [&str; 2] = ["semantics-household", "semantics-business"];
pub const GENERATION: [ModelOperation; 11] = [
    ModelOperation::Extraction,
    ModelOperation::Triage,
    ModelOperation::SupportVerification,
    ModelOperation::Summary,
    ModelOperation::PromotionDisambiguation,
    ModelOperation::IdentityDisambiguation,
    ModelOperation::TruthMaintenance,
    ModelOperation::WikiPlanner,
    ModelOperation::WikiWriter,
    ModelOperation::WikiVerifier,
    ModelOperation::MediaRender,
];
pub const EMBEDDING: [ModelOperation; 2] = [
    ModelOperation::QueryEmbedding,
    ModelOperation::DocumentEmbedding,
];

#[derive(Debug, Clone, Copy)]
pub enum Backend {
    SurrealMemory,
    ElephantDb,
}
pub const BACKENDS: [Backend; 2] = [Backend::SurrealMemory, Backend::ElephantDb];

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

/// Complete the controlled bag-of-words transport output for nonempty inputs
/// that contain no tokens (or cancel to zero). This is never model relevance.
#[expect(
    clippy::redundant_pub_crate,
    reason = "Preserve the shared control helper's crate-only visibility in every fixture inclusion."
)]
pub(crate) fn nonzero_controlled_vector(text: &str, vector: &mut [f32]) -> anyhow::Result<()> {
    use sha2::{Digest, Sha256};
    anyhow::ensure!(!text.is_empty(), "controlled embedding input is empty");
    anyhow::ensure!(
        !vector.is_empty() && vector.iter().all(|value| value.is_finite()),
        "controlled embedding vector has invalid dimensions or values"
    );
    if vector.iter().all(|value| *value == 0.0) {
        // Preserve every original byte, including punctuation and whitespace.
        // Word-bearing nonzero vectors retain their original fixture ranking.
        let digest = Sha256::digest(text.as_bytes());
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&digest[..8]);
        let dimensions = u64::try_from(vector.len())?;
        let index = usize::try_from(u64::from_be_bytes(prefix) % dimensions)?;
        vector[index] = 1.0;
    }
    Ok(())
}

#[derive(Debug)]
pub struct SpyCredentials;
impl CredentialResolver for SpyCredentials {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        // Never read environment variables or real deployment secrets.
        if !reference.starts_with("SEMANTICS_SPY_") {
            return Err(EmbeddingError::ProviderError(
                "unknown spy credential".into(),
            ));
        }
        Ok(format!("spy-only-{reference}"))
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub path: String,
    pub credential: String,
    pub body: Value,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Keep the original retry-action enum layout, owned fixtures, and allocation behavior."
)]
pub enum BeforeRetry {
    Revoke(SpaceConfigRepository, String, ModelOperation),
    Rotate(SpaceConfigRepository, String, String),
    ReplaceMedia(
        elephant_storage::MediaAssetRepository,
        elephant_types::records::MediaAsset,
    ),
}

#[derive(Clone)]
enum ControlledVectors {
    Declared(BTreeMap<String, Vec<f32>>),
    // Constructed by the separate Alice regression importing this shared module.
    #[allow(dead_code)]
    Hashed {
        dimensions: usize,
    },
}

#[derive(Clone)]
struct SpyState {
    requests: Arc<Mutex<Vec<Request>>>,
    before_retry: Arc<Mutex<Option<BeforeRetry>>>,
    failure: Arc<Mutex<Option<String>>>,
    structured: Arc<Mutex<Option<Value>>>,
    vectors: Arc<Mutex<Option<ControlledVectors>>>,
}

pub struct Spy {
    pub endpoint: String,
    state: SpyState,
    task: JoinHandle<()>,
}
impl Drop for Spy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Spy {
    pub async fn start() -> anyhow::Result<Self> {
        let state = SpyState {
            requests: Arc::new(Mutex::new(Vec::new())),
            before_retry: Arc::new(Mutex::new(None)),
            failure: Arc::new(Mutex::new(None)),
            structured: Arc::new(Mutex::new(None)),
            vectors: Arc::new(Mutex::new(None)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1", listener.local_addr()?);
        // Observe all requests, including wrong paths/methods/credentials or
        // malformed bodies. Router-generated 404/405/422 must not masquerade
        // as zero unauthorized outbound calls in a negative control.
        let app = Router::new().fallback(dispatch).with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            endpoint,
            state,
            task,
        })
    }
    pub fn requests(&self) -> anyhow::Result<Vec<Request>> {
        if let Some(error) = self
            .state
            .failure
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))?
            .as_ref()
        {
            anyhow::bail!("spy mutation failed: {error}");
        }
        Ok(self
            .state
            .requests
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))?
            .clone())
    }
    pub fn count(&self) -> anyhow::Result<usize> {
        Ok(self.requests()?.len())
    }
    /// Explicit controlled index inputs. These vectors are never model quality
    /// evidence; unknown text is refused instead of falling back to a winner.
    pub fn declared_vectors(&self, vectors: BTreeMap<String, Vec<f32>>) -> anyhow::Result<()> {
        anyhow::ensure!(!vectors.is_empty());
        anyhow::ensure!(vectors
            .values()
            .all(|v| !v.is_empty() && v.iter().all(|x| x.is_finite())));
        *self
            .state
            .vectors
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? =
            Some(ControlledVectors::Declared(vectors));
        Ok(())
    }
    /// Original Alice numerical regression only. Hash the actual request text;
    /// no query-to-answer table or claim about provider relevance is involved.
    #[allow(dead_code)] // The provider-only target does not run the Alice fixture.
    pub fn hashed_vectors(&self, dimensions: usize) -> anyhow::Result<()> {
        anyhow::ensure!(dimensions > 0);
        *self
            .state
            .vectors
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? =
            Some(ControlledVectors::Hashed { dimensions });
        Ok(())
    }
    /// Deliberately unsupported extraction output; the independent scripted
    /// support response rejects this exact counterexample, never grants truth.
    pub fn unsupported_extraction(&self, output: Value) -> anyhow::Result<()> {
        *self
            .state
            .structured
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? = Some(output);
        Ok(())
    }
    /// Controlled low-confidence identity response. It proposes review only;
    /// it is never a provider relevance or canonical identity winner.
    pub fn pending_identity(&self, canonical_name: &str) -> anyhow::Result<()> {
        *self
            .state
            .structured
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? = Some(json!({
            "same_entity":true,"canonical_display_name":canonical_name,"confidence":0.5,
            "reasoning":"Controlled transport spy requests review; no semantic acceptance."
        }));
        Ok(())
    }
    /// Deterministic completion for media lifecycle/accounting controls only.
    /// This text is a transport marker, never a visual-recognition expectation.
    pub fn media_projection(&self) -> anyhow::Result<()> {
        *self
            .state
            .structured
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? = Some(json!({
            "analyses": ["CONTROLLED transport marker; not visual evidence."]
        }));
        Ok(())
    }
    pub fn before_retry(&self, action: BeforeRetry) -> anyhow::Result<()> {
        *self
            .state
            .before_retry
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))? = Some(action);
        Ok(())
    }
}

async fn dispatch(
    State(state): State<SpyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let body = body.map_or(Value::Null, |Json(body)| body);
    match handle_request(&state, method, uri, headers, body).await {
        Ok(response) => response,
        Err(error) => {
            if let Ok(mut failure) = state.failure.lock() {
                *failure = Some(error.to_string());
            }
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":{"message":"spy fixture failure"}})),
            )
                .into_response()
        }
    }
}

#[expect(
    clippy::format_collect,
    clippy::significant_drop_tightening,
    reason = "Keep original retry-action lifetimes and ordered physical response construction."
)]
async fn handle_request(
    state: &SpyState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Value,
) -> anyhow::Result<Response> {
    let credential = headers
        .get("authorization")
        .or_else(|| headers.get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    state
        .requests
        .lock()
        .map_err(|_| anyhow!("spy lock poisoned"))?
        .push(Request {
            path: uri.path().into(),
            credential: credential.clone(),
            body: body.clone(),
        });
    anyhow::ensure!(
        method == Method::POST,
        "unexpected physical HTTP method: {method}"
    );
    anyhow::ensure!(
        matches!(
            uri.path(),
            "/v1/embeddings" | "/v1/messages" | "/v1/responses"
        ),
        "unexpected physical provider path: {}",
        uri.path()
    );
    anyhow::ensure!(
        !credential.is_empty(),
        "production client omitted credential"
    );
    anyhow::ensure!(
        body.is_object(),
        "physical provider body is not a JSON object"
    );
    let action = state
        .before_retry
        .lock()
        .map_err(|_| anyhow!("spy lock poisoned"))?
        .take();
    if let Some(action) = action {
        let controlled_media_retry = matches!(&action, BeforeRetry::ReplaceMedia(..));
        match action {
            BeforeRetry::Revoke(repo, space, operation) => {
                edit(&repo, &space, |config| {
                    config
                        .bindings
                        .get_mut(&operation)
                        .context("missing binding")?
                        .revoked = true;
                    Ok(())
                })
                .await?;
            }
            BeforeRetry::Rotate(repo, space, reference) => {
                edit(&repo, &space, |config| {
                    let connection = config
                        .connections
                        .get_mut("embed")
                        .context("missing connection")?;
                    connection.credential_generation += 1;
                    connection.credential_ref = Some(reference);
                    Ok(())
                })
                .await?;
            }
            BeforeRetry::ReplaceMedia(repo, replacement) => {
                repo.update(
                    &replacement.base.space_id.clone(),
                    &replacement.base.id.clone(),
                    replacement,
                )
                .await?;
            }
        }
        // Revocation/rotation commits before the client receives its 429. No
        // sleeps/race windows are used to synchronize the test with the retry.
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":{"message":"controlled retry"}})),
        )
            .into_response();
        if controlled_media_retry {
            // The real agent SDK otherwise applies its 30-second rate-limit
            // floor, exceeding this fixture's unchanged 10-second deadline.
            // An actual provider hint above its first 500ms (+/-10%) backoff
            // exercises the original retry path without changing that deadline.
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("2"),
            );
        }
        return Ok(response);
    }
    if uri.path().ends_with("embeddings") {
        let dims = usize::try_from(body["dimensions"].as_u64().context("missing dimensions")?)?;
        let input = body["input"]
            .as_array()
            .context("missing embedding inputs")?;
        let controlled = state
            .vectors
            .lock()
            .map_err(|_| anyhow!("spy lock poisoned"))?
            .clone();
        let hashed = if let Some(ControlledVectors::Hashed { dimensions }) = &controlled {
            anyhow::ensure!(*dimensions == dims, "hashed fixture dimensions changed");
            let texts = input
                .iter()
                .map(|text| {
                    text.as_str()
                        .context("embedding input is not text")
                        .map(str::to_owned)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let mut vectors = HashedEmbeddingProvider::new("hashed-bow-v1", dims)
                .embed(&texts)
                .await?;
            anyhow::ensure!(
                vectors.len() == texts.len(),
                "controlled embedding batch count differs"
            );
            for (text, vector) in texts.iter().zip(&mut vectors) {
                anyhow::ensure!(
                    vector.len() == dims,
                    "controlled embedding dimensions differ"
                );
                nonzero_controlled_vector(text, vector)?;
            }
            Some(vectors)
        } else {
            None
        };
        // Fixed nonzero vectors are TRANSPORT SPY output, never semantic evidence.
        let rows: Vec<_> = input
            .iter()
            .enumerate()
            .map(|(index, text)| -> anyhow::Result<Value> {
                let vector = if let Some(ControlledVectors::Declared(vectors)) = &controlled {
                    vectors
                        .get(text.as_str().context("embedding input is not text")?)
                        .context("undeclared controlled embedding input")?
                        .clone()
                } else if let Some(vectors) = &hashed {
                    vectors
                        .get(index)
                        .context("hashed fixture omitted an input")?
                        .clone()
                } else {
                    vec![0.25_f32; dims]
                };
                anyhow::ensure!(
                    vector.len() == dims,
                    "declared vector dimensions differ from approved profile"
                );
                Ok(json!({"index":index, "object":"embedding", "embedding":vector}))
            })
            .collect::<anyhow::Result<_>>()?;
        return Ok(
            Json(json!({"object":"list", "data":rows, "model":body["model"],
            "usage":{"prompt_tokens":7,"total_tokens":7}}))
            .into_response(),
        );
    }
    let structured = state
        .structured
        .lock()
        .map_err(|_| anyhow!("spy lock poisoned"))?
        .clone();
    if let Some(extraction) = structured {
        let payload = if body["model"]
            .as_str()
            .is_some_and(|model| model.ends_with("-independent-support"))
        {
            json!({"decisions":[{"index":0,"supported":false}]})
        } else {
            extraction
        };
        let text = payload.to_string();
        let events = if uri.path().ends_with("messages") {
            vec![
                json!({"type":"message_start","message":{"id":"spy-message","role":"assistant","model":body["model"],"content":[],"usage":{"input_tokens":1,"output_tokens":0}}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}),
                json!({"type":"message_stop"}),
            ]
        } else {
            vec![
                json!({"type":"response.output_text.delta","delta":text}),
                json!({"type":"response.completed","response":{"id":"spy-response","status":"completed","output":[{"id":"spy-message","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1}}}),
            ]
        };
        let stream: String = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        return Ok(([("content-type", "text/event-stream")], stream).into_response());
    }
    // A nonretryable response demonstrates actual outbound dispatch without
    // fabricating a generated claim, structured winner or completion receipt.
    Ok((
        StatusCode::BAD_REQUEST,
        Json(json!({"error":{
            "type":"invalid_request_error", "message":"qualification_spy_stop"
        }})),
    )
        .into_response())
}

pub fn context(repo: &SpaceConfigRepository, space: &str) -> ProviderDispatchContext {
    ProviderDispatchContext::new(repo.clone(), space).with_credentials(Arc::new(SpyCredentials))
}

pub async fn configure(
    repo: &SpaceConfigRepository,
    space: &str,
    endpoint: &str,
    dimensions: usize,
) -> anyhow::Result<SpaceConfig> {
    // JSON is ordinary test input, not a database proxy/custom deserializer.
    // index_generation belongs to the provider owner successor contract.
    let generative_provider = if space == SPACES[0] {
        "anthropic"
    } else {
        "openai"
    };
    let model = if generative_provider == "anthropic" {
        "claude-spy-a"
    } else {
        "gpt-spy-b"
    };
    // SDK 0.8.51 appends /v1/messages to the Anthropic base, while OpenAI
    // appends responses to an explicitly versioned /v1 base.
    let generation_endpoint = if generative_provider == "anthropic" {
        endpoint
            .strip_suffix("/v1")
            .context("spy endpoint is not versioned")?
    } else {
        endpoint
    };
    let mut bindings = BTreeMap::new();
    for operation in GENERATION {
        bindings.insert(operation, json!({"id":format!("{space}-{operation:?}"),"profile_id":format!("{space}-generation"),"revoked":false}));
    }
    for operation in EMBEDDING {
        bindings.insert(operation, json!({"id":format!("{space}-{operation:?}"),"profile_id":format!("{space}-embedding"),"revoked":false}));
    }
    let processing: SpaceProcessingConfig = serde_json::from_value(json!({
        "generation":1, "index_generation":0, "policy_id":format!("policy-{space}"),
        "connections": {
            "embed":{"id":"embed","provider":"openai","account_id":format!("{space}-embedding-account"),"endpoint":endpoint,"credential_ref":format!("SEMANTICS_SPY_{dimensions}_EMBED"),"credential_generation":1,"revoked":false},
            "generate":{"id":"generate","provider":generative_provider,"account_id":format!("{space}-generation-account"),"endpoint":generation_endpoint,"credential_ref":format!("SEMANTICS_SPY_{dimensions}_GENERATE"),"credential_generation":1,"revoked":false}
        },
        "profiles": {
            format!("{space}-embedding"):{"id":format!("{space}-embedding"),"connection_id":"embed","model":format!("text-embedding-spy-{dimensions}"),"model_version":"spy-v1","dimensions":dimensions,"preprocessing":"text-v1"},
            format!("{space}-generation"):{"id":format!("{space}-generation"),"connection_id":"generate","model":model,"model_version":"spy-v1","dimensions":null,"preprocessing":"prompt-v1"}
        },
        "bindings":bindings, "active_embedding_profile_id":format!("{space}-embedding"), "pending_reindex":null
    }))?;
    Ok(repo
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                "D-semantic-spy".into(),
            ),
            embedding_provider: "openai".into(),
            embedding_model: format!("text-embedding-spy-{dimensions}"),
            embedding_dims: i32::try_from(dimensions)?,
            processing: Some(processing),
            purpose: Some(format!("Distinct test-only guidance for {space}")),
            skill: None,
        })
        .await?)
}

pub async fn edit(
    repo: &SpaceConfigRepository,
    space: &str,
    mutation: impl FnOnce(&mut SpaceProcessingConfig) -> anyhow::Result<()>,
) -> anyhow::Result<SpaceConfig> {
    let observed = repo
        .get_by_space_id(space)
        .await?
        .context("missing configured space")?;
    let mut proposed = observed.clone();
    let processing = proposed
        .processing
        .as_mut()
        .context("missing processing policy")?;
    processing.generation += 1;
    mutation(processing)?;
    Ok(repo.replace_authorized(&observed, proposed).await?)
}

pub async fn model_call(
    client: Arc<dyn LlmClient>,
    model: &str,
    marker: &str,
) -> anyhow::Result<()> {
    let mut request = LlmRequest::new(model, vec![Message::User(UserMessage::text(marker))]);
    request.max_tokens = 16;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        require_model_refusal(client.stream(&request)),
    )
    .await?
}

pub async fn require_model_refusal(
    mut stream: impl futures::Stream<Item = Result<LlmEvent, LlmError>> + Unpin,
) -> anyhow::Result<()> {
    while let Some(event) = stream.next().await {
        match event {
            // SDK 0.8.51 can encode a provider failure as a terminal event
            // inside Ok. This is still an explicit failure, never success.
            Err(_)
            | Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Error { .. },
            }) => return Ok(()),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { .. },
            }) => {
                anyhow::bail!("controlled refusal emitted model success");
            }
            Ok(
                LlmEvent::TextDelta { .. }
                | LlmEvent::AssistantOutput { .. }
                | LlmEvent::ToolCallComplete { .. },
            ) => {
                anyhow::bail!("controlled refusal emitted generated output");
            }
            _ => {}
        }
    }
    anyhow::bail!("controlled refusal ended without an explicit error")
}
