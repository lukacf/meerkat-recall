//! Approved per-space embedding dispatch. Authority is checked at the HTTP edge.

use std::sync::Arc;

use async_trait::async_trait;
use elephant_types::provider_binding::{ModelOperation, ResolvedModelBinding};

use crate::{EmbeddingError, EmbeddingPolicy, EmbeddingProvider, EmbeddingResult, EmbeddingVector};

/// Current authority lookup. Implementations must not cache approval decisions.
#[async_trait]
pub trait EmbeddingBindingResolver: Send + Sync {
    /// Reload the selected space's current approval for this operation/profile.
    /// Revoked, missing or replaced bindings must return an error.
    async fn resolve(
        &self,
        space_id: &str,
        operation: ModelOperation,
        pinned_profile: &str,
    ) -> EmbeddingResult<ResolvedModelBinding>;
}

/// Optional durable accounting observer. It owns call IDs/receipts; transports
/// report measured snapshots and never invent usage for a failed request.
#[async_trait]
pub trait ProviderDispatchObserver: Send + Sync + std::fmt::Debug {
    /// Durably admit this actual dispatch before sending data to the provider.
    async fn before_dispatch(
        &self,
        call_id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> EmbeddingResult<()>;
    /// Persist usage reported for this dispatch; `None` denotes unknown usage.
    async fn observe_usage(
        &self,
        call_id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    ) -> EmbeddingResult<()>;

    /// An actual complete provider response was observed. Intermediate token
    /// snapshots alone do not certify final usage. Stateless observers may ignore it.
    async fn dispatch_finished(
        &self,
        _call_id: &str,
        _provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        Ok(())
    }
}

pub(crate) struct DispatchObservation {
    call_id: String,
    provenance: elephant_types::provider_binding::ModelBindingProvenance,
}

/// Deployment secret lookup, separate from persisted provider/profile identity.
pub trait CredentialResolver: Send + Sync + std::fmt::Debug {
    /// Resolve only the explicitly approved secret reference or fail closed.
    ///
    /// # Errors
    /// Fails when the approved reference is invalid, unavailable, or refused.
    fn resolve(&self, reference: &str) -> EmbeddingResult<String>;
}

/// Explicit environment reference lookup; no conventional-key fallback.
#[derive(Debug, Default)]
pub struct EnvironmentCredentials;

impl CredentialResolver for EnvironmentCredentials {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        if reference.is_empty()
            || !reference
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            return Err(blocked(
                "configuration_blocked: invalid credential reference",
            ));
        }
        std::env::var(reference)
            .ok()
            .filter(|secret| !secret.trim().is_empty())
            .ok_or_else(|| blocked("configuration_blocked: approved credential is unavailable"))
    }
}

/// Transport guard retains identity, never a secret or a reusable approval.
#[derive(Clone)]
pub struct EmbeddingDispatch {
    resolver: Arc<dyn EmbeddingBindingResolver>,
    credentials: Arc<dyn CredentialResolver>,
    approved: ResolvedModelBinding,
    observer: Option<Arc<dyn ProviderDispatchObserver>>,
}

impl EmbeddingDispatch {
    /// Role of this request's pinned approval. `authorize` rechecks this exact
    /// operation after model/blocking waits and before every physical dispatch.
    #[cfg(feature = "fastembed")]
    pub(crate) const fn operation(&self) -> ModelOperation {
        self.approved.provenance.operation
    }

    /// Pin the approved vector identity while retaining fresh authority lookup.
    #[must_use]
    pub fn new(
        resolver: Arc<dyn EmbeddingBindingResolver>,
        credentials: Arc<dyn CredentialResolver>,
        approved: ResolvedModelBinding,
    ) -> Self {
        Self {
            resolver,
            credentials,
            approved,
            observer: None,
        }
    }

    /// Attach durable dispatch admission and measured usage accounting.
    #[must_use]
    pub fn with_observer(mut self, observer: Option<Arc<dyn ProviderDispatchObserver>>) -> Self {
        self.observer = observer;
        self
    }

    pub(crate) async fn dispatch_started(
        &self,
        binding: &ResolvedModelBinding,
    ) -> EmbeddingResult<Option<DispatchObservation>> {
        if let Some(observer) = &self.observer {
            let call_id = uuid::Uuid::now_v7().to_string();
            observer
                .before_dispatch(&call_id, &binding.provenance)
                .await?;
            return Ok(Some(DispatchObservation {
                call_id,
                provenance: binding.provenance.clone(),
            }));
        }
        Ok(None)
    }

    pub(crate) async fn usage_snapshot(
        &self,
        observation: Option<&DispatchObservation>,
        input_tokens: u64,
    ) -> EmbeddingResult<()> {
        if let (Some(observer), Some(observation)) = (&self.observer, observation) {
            observer
                .observe_usage(
                    &observation.call_id,
                    &observation.provenance,
                    Some(input_tokens),
                    Some(0),
                )
                .await?;
            observer
                .dispatch_finished(&observation.call_id, &observation.provenance)
                .await?;
        }
        Ok(())
    }

    /// Recheck current authority and then resolve the current credential.
    ///
    /// # Errors
    /// Revocation, profile replacement or absent credentials prevent dispatch.
    pub async fn authorize(&self) -> EmbeddingResult<(ResolvedModelBinding, Option<String>)> {
        let current = self
            .resolver
            .resolve(
                &self.approved.provenance.config.space_id,
                self.approved.provenance.operation,
                &self.approved.profile.id,
            )
            .await?;
        if current.provenance.config.space_id != self.approved.provenance.config.space_id
            || current.provenance.operation != self.approved.provenance.operation
            || current.profile != self.approved.profile
            || current
                .vector_partition()
                .map_err(|_| blocked("invalid embedding profile"))?
                != self
                    .approved
                    .vector_partition()
                    .map_err(|_| blocked("invalid embedding profile"))?
            || current.connection.revoked
        {
            return Err(blocked("approved embedding identity changed"));
        }
        validate_endpoint(&current)?;
        let credential = current
            .connection
            .credential_ref
            .as_ref()
            .map(|reference| self.credentials.resolve(reference))
            .transpose()?;
        Ok((current, credential))
    }
}

/// Validate the approved endpoint before using any credential. Redirects must
/// remain disabled on the HTTP client so an approved host cannot relay secrets.
///
/// # Errors
/// Rejects URL credentials, query strings, fragments and unencrypted remote URLs.
pub fn validate_endpoint(binding: &ResolvedModelBinding) -> EmbeddingResult<()> {
    if binding.connection.provider == "fastembed" {
        return crate::pinned_fastembed::validate_binding_identity(binding);
    }
    let url = reqwest::Url::parse(&binding.connection.endpoint)
        .map_err(|_| blocked("invalid approved endpoint"))?;
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
        || url.host_str().is_none()
    {
        return Err(blocked(
            "approved endpoint must use TLS and contain no credentials, query or fragment",
        ));
    }
    Ok(())
}

/// Stable space/provider/model/version/dimension/preprocessing partition identity.
///
/// # Errors
/// Rejects a non-vector profile or serialization failure.
pub fn policy_for_binding(binding: &ResolvedModelBinding) -> EmbeddingResult<EmbeddingPolicy> {
    let identity = binding
        .vector_partition()
        .map_err(|_| blocked("invalid embedding profile"))?;
    let model =
        serde_json::to_string(&identity).map_err(|_| blocked("invalid partition identity"))?;
    Ok(EmbeddingPolicy::new(
        &identity.provider,
        format!("recall:{model}"),
        identity.dimensions,
    ))
}

/// Provider handle pins vector identity while reauthorizing every transport call.
pub struct GovernedEmbeddingProvider {
    dispatch: EmbeddingDispatch,
    policy: EmbeddingPolicy,
    local: Arc<crate::local_model_cache::VerifiedLocalModelCache>,
}

impl GovernedEmbeddingProvider {
    /// # Errors
    /// Rejects non-embedding operations and malformed profiles.
    pub fn new(dispatch: EmbeddingDispatch) -> EmbeddingResult<Self> {
        if !dispatch.approved.provenance.operation.is_embedding() {
            return Err(blocked("embedding operation required"));
        }
        let policy = policy_for_binding(&dispatch.approved)?;
        Ok(Self {
            dispatch,
            policy,
            local: Arc::default(),
        })
    }
}

impl GovernedEmbeddingProvider {
    /// Reuse verified local model resources owned by the service. This cache
    /// contains no request authority, credentials or accounting observer.
    #[must_use]
    pub fn with_local_models(
        mut self,
        local: Arc<crate::local_model_cache::VerifiedLocalModelCache>,
    ) -> Self {
        self.local = local;
        self
    }

    async fn embed_local(
        &self,
        binding: &ResolvedModelBinding,
        texts: &[String],
    ) -> EmbeddingResult<Vec<EmbeddingVector>> {
        #[cfg(feature = "fastembed")]
        {
            // Authority and accounting belong to this handle, never the cached
            // model. The transport rechecks after model-lock and blocking waits.
            let provider = self
                .local
                .provider(binding)
                .await?
                .with_dispatch(self.dispatch.clone());
            provider.embed(texts).await
        }
        #[cfg(not(feature = "fastembed"))]
        {
            let _ = (binding, texts);
            Err(blocked("this build lacks the approved FastEmbed runtime"))
        }
    }
}

#[async_trait]
impl EmbeddingProvider for GovernedEmbeddingProvider {
    fn model(&self) -> &str {
        &self.policy.model
    }
    fn dimensions(&self) -> usize {
        self.policy.dims
    }

    async fn embed(&self, texts: &[String]) -> EmbeddingResult<Vec<EmbeddingVector>> {
        let (binding, secret) = self.dispatch.authorize().await?;
        let vectors = match binding.connection.provider.as_str() {
            "openai" => {
                let config = crate::provider::approved_openai_config(
                    &binding,
                    secret.ok_or_else(|| blocked("approved credential is unavailable"))?,
                )?;
                crate::OpenAIEmbeddingProvider::new(config)
                    .with_dispatch(self.dispatch.clone())?
                    .embed(texts)
                    .await?
            }
            "fastembed" => self.embed_local(&binding, texts).await?,
            _ => return Err(blocked("approved provider does not support embeddings")),
        };
        if vectors.len() != texts.len()
            || vectors.iter().any(|vector| {
                vector.len() != self.policy.dims || vector.iter().any(|value| !value.is_finite())
            })
        {
            return Err(blocked("provider returned invalid embedding shape"));
        }
        Ok(vectors)
    }
}

fn blocked(message: &str) -> EmbeddingError {
    EmbeddingError::ProviderError(format!("provider_authorization: {message}"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::provider_binding::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    include!("governed_usage_tests.rs");
    include!("governed_full_input_tests.rs");

    #[derive(Debug)]
    struct Approval {
        space: ResolvedSpaceConfig,
        revoked: AtomicBool,
        resolutions: AtomicUsize,
    }
    #[async_trait]
    impl EmbeddingBindingResolver for Approval {
        async fn resolve(
            &self,
            space_id: &str,
            operation: ModelOperation,
            pinned: &str,
        ) -> EmbeddingResult<ResolvedModelBinding> {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            if self.revoked.load(Ordering::SeqCst) {
                return Err(blocked("revoked"));
            }
            self.space
                .authorize(space_id, operation, Some(pinned))
                .map_err(|_| blocked("refused"))
        }
    }
    #[derive(Debug)]
    struct Secret;
    impl CredentialResolver for Secret {
        fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
            assert_eq!(reference, "TEST_ONLY");
            Ok("loopback-fixture-secret".into())
        }
    }
    fn approval(endpoint: String) -> Approval {
        let connection = ProviderConnection {
            id: "c".into(),
            provider: "openai".into(),
            account_id: "account-a".into(),
            endpoint,
            credential_ref: Some("TEST_ONLY".into()),
            credential_generation: 1,
            revoked: false,
        };
        let profile = ModelProfile {
            id: "p".into(),
            connection_id: "c".into(),
            model: "test-embed".into(),
            model_version: "1".into(),
            dimensions: Some(3),
            preprocessing: "text-v1".into(),
            summary: None,
        };
        Approval {
            space: ResolvedSpaceConfig {
                token: SpaceConfigToken {
                    space_id: "a".into(),
                    config_id: "cfg".into(),
                    generation: 1,
                    policy_id: "policy".into(),
                },
                processing: SpaceProcessingConfig {
                    generation: 1,
                    index_generation: 0,
                    policy_id: "policy".into(),
                    connections: BTreeMap::from([("c".into(), connection)]),
                    profiles: BTreeMap::from([("p".into(), profile)]),
                    bindings: BTreeMap::from([(
                        ModelOperation::QueryEmbedding,
                        OperationBinding {
                            id: "b".into(),
                            profile_id: "p".into(),
                            revoked: false,
                        },
                    )]),
                    active_embedding_profile_id: Some("p".into()),
                    pending_reindex: None,
                    completed_embedding_reindex: None,
                },
                purpose: None,
                skill: None,
            },
            revoked: AtomicBool::new(false),
            resolutions: AtomicUsize::new(0),
        }
    }
    fn provider(approval: &Arc<Approval>) -> GovernedEmbeddingProvider {
        let binding = approval
            .space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        GovernedEmbeddingProvider::new(EmbeddingDispatch::new(
            approval.clone(),
            Arc::new(Secret),
            binding,
        ))
        .unwrap()
    }

    #[tokio::test]
    #[cfg_attr(
        feature = "fastembed",
        expect(
            clippy::significant_drop_tightening,
            reason = "Construct the cached handle before revocation and retain it through the no-request interval."
        )
    )]
    async fn revoked_cached_handle_dispatches_no_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let approval = Arc::new(approval(format!(
            "http://{}/v1",
            listener.local_addr().unwrap()
        )));
        let cached = provider(&approval);
        approval.revoked.store(true, Ordering::SeqCst);
        assert!(cached.embed(&["private text".into()]).await.is_err());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[derive(Debug)]
    struct RevokingObserver(Arc<Approval>);
    #[async_trait]
    impl ProviderDispatchObserver for RevokingObserver {
        async fn before_dispatch(
            &self,
            _: &str,
            _: &ModelBindingProvenance,
        ) -> EmbeddingResult<()> {
            self.0.revoked.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn observe_usage(
            &self,
            _: &str,
            _: &ModelBindingProvenance,
            _: Option<u64>,
            _: Option<u64>,
        ) -> EmbeddingResult<()> {
            Err(blocked("no physical request expected"))
        }
    }

    #[tokio::test]
    #[cfg_attr(
        feature = "fastembed",
        expect(
            clippy::significant_drop_tightening,
            reason = "Retain the original governed handle through receipt revocation and the no-request interval."
        )
    )]
    async fn revocation_during_dispatch_receipt_prevents_embedding_send() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let approval = Arc::new(approval(format!(
            "http://{}/v1",
            listener.local_addr().unwrap()
        )));
        let binding = approval
            .space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        let provider = GovernedEmbeddingProvider::new(
            EmbeddingDispatch::new(approval.clone(), Arc::new(Secret), binding)
                .with_observer(Some(Arc::new(RevokingObserver(approval)))),
        )
        .unwrap();
        assert!(provider
            .embed(&["local negative fixture".into()])
            .await
            .is_err());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[cfg_attr(
        feature = "fastembed",
        expect(
            clippy::significant_drop_tightening,
            reason = "Retain the pre-revocation handle through the first response and rejected retry assertions."
        )
    )]
    async fn revocation_after_rate_limit_fences_internal_http_retry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let approval = Arc::new(approval(format!(
            "http://{}/v1",
            listener.local_addr().unwrap()
        )));
        let cached = provider(&approval);
        let current = approval.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0, "request ended before complete body");
                bytes.extend_from_slice(&chunk[..count]);
                assert!(bytes.len() <= 64 * 1024);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let request = String::from_utf8_lossy(&bytes);
            assert!(request.starts_with("POST /v1/embeddings"));
            assert!(request.contains("test-embed"));
            assert!(!request.contains("recall:"));
            current.revoked.store(true, Ordering::SeqCst);
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            listener
        });
        let result = cached
            .embed(&["only approved account sees this".into()])
            .await;
        assert!(result.is_err());
        let listener = server.await.unwrap();
        assert!(approval.resolutions.load(Ordering::SeqCst) >= 3);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn partition_identity_and_endpoint_validation_are_space_exact() {
        let a = approval("https://example.test/v1".into());
        let first = a
            .space
            .authorize("a", ModelOperation::QueryEmbedding, None)
            .unwrap();
        let mut second = first.clone();
        second.provenance.config.space_id = "b".into();
        assert_ne!(
            policy_for_binding(&first).unwrap(),
            policy_for_binding(&second).unwrap()
        );
        second = first.clone();
        second.profile.preprocessing = "text-full-v2".into();
        assert_ne!(
            policy_for_binding(&first).unwrap(),
            policy_for_binding(&second).unwrap()
        );
        second = first.clone();
        second.connection.credential_generation += 1;
        assert_eq!(
            policy_for_binding(&first).unwrap(),
            policy_for_binding(&second).unwrap()
        );
        for endpoint in [
            "https://key@example.test/v1",
            "https://example.test/v1?key=secret",
            "http://example.test/v1",
            "file:///tmp/socket",
        ] {
            second.connection.endpoint = endpoint.into();
            assert!(validate_endpoint(&second).is_err());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "The pending future borrows this provider; retain its owner through refusal, socket absence and shared-slot reuse assertions."
    )]
    async fn queued_embedding_rechecks_approval_after_shared_remote_wait() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let first = crate::remote_admission::acquire(&endpoint).await.unwrap();
        let second = crate::remote_admission::acquire(&endpoint).await.unwrap();
        let current = Arc::new(approval(endpoint.clone()));
        let provider = provider(&current);
        let inputs = ["queued original text".into()];
        let mut pending = Box::pin(provider.embed(&inputs));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
        assert!(current.resolutions.load(Ordering::SeqCst) > 0);
        current.revoked.store(true, Ordering::SeqCst);
        drop(first);
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut pending)
            .await
            .unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("provider_authorization"));
        drop(pending);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
        drop(second);
        // The refused attempt does not leak the shared endpoint slot.
        let _first = crate::remote_admission::acquire(&endpoint).await.unwrap();
        let _second = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            crate::remote_admission::acquire(&endpoint),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "The original provider is moved into the third request task after the first two responses stall; its lifetime must cover that physical request."
    )]
    async fn physical_embedding_capacity_covers_response_body_and_cancellation() {
        async fn read_request(socket: &mut tokio::net::TcpStream) {
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                assert!(bytes.len() <= 64 * 1024);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    assert!(headers.starts_with("POST /v1/embeddings "));
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        return;
                    }
                }
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let current = Arc::new(approval(format!(
            "http://{}/v1",
            listener.local_addr().unwrap()
        )));
        let provider = Arc::new(provider(&current));
        let first_provider = provider.clone();
        let first = tokio::spawn(async move { first_provider.embed(&["first".into()]).await });
        let second_provider = provider.clone();
        let second = tokio::spawn(async move { second_provider.embed(&["second".into()]).await });
        let (mut first_socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let (mut second_socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_request(&mut first_socket),
        )
        .await
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_request(&mut second_socket),
        )
        .await
        .unwrap();
        // Headers alone are not completion: both decoders are still waiting.
        let partial = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{\"data\":[";
        first_socket.write_all(partial).await.unwrap();
        second_socket.write_all(partial).await.unwrap();
        let third = tokio::spawn(async move { provider.embed(&["third".into()]).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let (mut third_socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_request(&mut third_socket),
        )
        .await
        .unwrap();
        let body = r#"{"data":[{"index":0,"embedding":[0.5,0.5,0.5]}],"model":"test-embed","usage":{"prompt_tokens":7,"total_tokens":7}}"#;
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        third_socket.write_all(response.as_bytes()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), third)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![vec![0.5, 0.5, 0.5]]
        );
        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        drop((first_socket, second_socket, third_socket));
    }
}
