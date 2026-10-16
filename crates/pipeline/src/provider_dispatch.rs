//! Space-bound model dispatch shared by workers and public query adapters.

use std::sync::Arc;

use elephant_embeddings::governed::{
    policy_for_binding, CredentialResolver, EmbeddingDispatch, EnvironmentCredentials,
    GovernedEmbeddingProvider,
};
use elephant_embeddings::{EmbeddingPolicy, EmbeddingProvider};
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::{ModelBindingProvenance, ModelOperation};

use crate::{PipelineError, PipelineResult};

tokio::task_local! {
    static DISPATCH_CREDENTIALS: Arc<dyn CredentialResolver>;
    static DISPATCH_OBSERVER: Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>;
}

/// Scope the attempt owner's observer over the current task. Space authority
/// remains explicit; this carries accounting only. Spawned work must clone/install
/// its observer or construct its explicit dispatch context before spawning.
pub async fn with_dispatch_observer<F: std::future::Future>(
    observer: Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>,
    future: F,
) -> F::Output {
    DISPATCH_OBSERVER.scope(observer, future).await
}

/// Scope a deployment secret resolver over a task, without changing provider
/// approval or process-global environment. Spawned tasks explicitly propagate it.
pub async fn with_dispatch_credentials<F: std::future::Future>(
    credentials: Arc<dyn CredentialResolver>,
    future: F,
) -> F::Output {
    DISPATCH_CREDENTIALS.scope(credentials, future).await
}

/// Explicit propagation across dedicated threads and spawned tasks. This carries
/// accounting and secret resolution only; selected-space approval stays explicit.
#[derive(Clone)]
pub struct CapturedDispatchScope {
    credentials: Option<Arc<dyn CredentialResolver>>,
    observer: Option<Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>>,
}

/// Capture task-local accounting and explicit credential resolution for a child task.
#[must_use]
pub fn capture_dispatch_scope() -> CapturedDispatchScope {
    CapturedDispatchScope {
        credentials: DISPATCH_CREDENTIALS.try_with(Arc::clone).ok(),
        observer: DISPATCH_OBSERVER.try_with(Arc::clone).ok(),
    }
}

impl CapturedDispatchScope {
    /// Install the captured task-local context while polling the supplied future.
    pub async fn run<F: std::future::Future>(self, future: F) -> F::Output {
        let with_credentials = async move {
            match self.credentials {
                Some(credentials) => with_dispatch_credentials(credentials, future).await,
                None => future.await,
            }
        };
        match self.observer {
            Some(observer) => with_dispatch_observer(observer, with_credentials).await,
            None => with_credentials.await,
        }
    }
}

/// Reusable lookup context, not an approval cache.
#[derive(Debug, Clone)]
pub struct ProviderDispatchContext {
    /// Repository reloaded at each model operation and concrete provider request.
    pub repository: SpaceConfigRepository,
    /// Explicit named space whose current approvals govern every dispatch.
    pub space_id: String,
    credentials: Arc<dyn CredentialResolver>,
    observer: Option<Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>>,
}

impl ProviderDispatchContext {
    /// Bind a selected space and inherit only task-local credential/accounting context.
    #[must_use]
    pub fn new(repository: SpaceConfigRepository, space_id: impl Into<String>) -> Self {
        Self {
            repository,
            space_id: space_id.into(),
            credentials: DISPATCH_CREDENTIALS
                .try_with(Arc::clone)
                .unwrap_or_else(|_| Arc::new(EnvironmentCredentials)),
            observer: DISPATCH_OBSERVER.try_with(Arc::clone).ok(),
        }
    }

    /// Inject a deployment secret resolver or deterministic transport fixture.
    #[must_use]
    pub fn with_credentials(mut self, credentials: Arc<dyn CredentialResolver>) -> Self {
        self.credentials = credentials;
        self
    }

    /// Install the attempt owner's durable dispatch/usage receipt sink.
    #[must_use]
    pub fn with_observer(
        mut self,
        observer: Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>,
    ) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Add an input guard without replacing the attempt owner's accounting.
    ///
    /// The original observer finishes its durable admission first, then this
    /// guard runs before the concrete request. Further guards run in insertion
    /// order. Only `before_dispatch` is called on the additional observer;
    /// usage and completion remain owned by the original observer. A refusal
    /// never fabricates a completion or measured usage receipt.
    #[must_use]
    pub fn with_additional_observer(
        mut self,
        guard: Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>,
    ) -> Self {
        self.observer = Some(Arc::new(AdditionalDispatchGuard {
            original: self.observer.take(),
            guard,
        }));
        self
    }

    /// Resolve the approved embedding partition and a handle that checks current
    /// authority at every batch/retry. This does not perform a model request.
    pub async fn embedding(
        &self,
        operation: ModelOperation,
    ) -> PipelineResult<(
        EmbeddingPolicy,
        Arc<dyn EmbeddingProvider>,
        ModelBindingProvenance,
    )> {
        let binding = self
            .repository
            .authorize_model_operation(&self.space_id, operation, None)
            .await
            .map_err(resolution_failure)?;
        let policy = policy_for_binding(&binding).map_err(blocked)?;
        let provenance = binding.provenance.clone();
        let dispatch = EmbeddingDispatch::new(
            Arc::new(self.repository.clone()),
            self.credentials.clone(),
            binding,
        )
        .with_observer(self.observer.clone());
        let provider = GovernedEmbeddingProvider::new(dispatch)
            .map_err(blocked)?
            .with_local_models(self.repository.local_models());
        Ok((policy, Arc::new(provider), provenance))
    }
}

#[derive(Debug)]
struct AdditionalDispatchGuard {
    original: Option<Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>>,
    guard: Arc<dyn elephant_embeddings::governed::ProviderDispatchObserver>,
}

#[async_trait::async_trait]
impl elephant_embeddings::governed::ProviderDispatchObserver for AdditionalDispatchGuard {
    async fn before_dispatch(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        if let Some(original) = &self.original {
            original.before_dispatch(call_id, provenance).await?;
        }
        self.guard.before_dispatch(call_id, provenance).await
    }

    async fn observe_usage(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        if let Some(original) = &self.original {
            original
                .observe_usage(call_id, provenance, input_tokens, output_tokens)
                .await?;
        }
        Ok(())
    }

    async fn dispatch_finished(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        if let Some(original) = &self.original {
            original.dispatch_finished(call_id, provenance).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "provider_dispatch_composition_tests.rs"]
mod composition_tests;

fn resolution_failure(
    error: elephant_storage::space_config_repository::SpaceResolutionError,
) -> PipelineError {
    match error {
        elephant_storage::space_config_repository::SpaceResolutionError::Storage(error) => {
            PipelineError::StorageError(error)
        }
        elephant_storage::space_config_repository::SpaceResolutionError::Authorization(
            elephant_types::provider_binding::ProviderAuthorizationError::WrongSpace,
        ) => {
            PipelineError::SecurityViolation("model operation belongs to a different space".into())
        }
        elephant_storage::space_config_repository::SpaceResolutionError::Authorization(error) => {
            blocked(error)
        }
    }
}

fn blocked(error: impl std::fmt::Display) -> PipelineError {
    PipelineError::ConfigError(format!("provider_authorization: {error}"))
}

#[cfg(feature = "meerkat-extraction")]
mod model {
    use std::pin::Pin;

    use async_trait::async_trait;
    use elephant_types::provider_binding::ResolvedModelBinding;
    use futures::{Stream, StreamExt};
    use meerkat::{AgentBuildConfig, LlmClient, LlmError, LlmEvent, LlmRequest, Message};
    use meerkat_core::Provider;

    use super::*;

    fn refusal(error: impl std::fmt::Display) -> LlmError {
        LlmError::InvalidRequest {
            message: format!("provider_authorization: {error}"),
        }
    }

    fn auth_refusal(error: impl std::fmt::Display) -> meerkat_core::AuthError {
        meerkat_core::AuthError::Other(format!("provider_authorization: {error}"))
    }

    /// SDK callback invoked for each concrete HTTP request, including Gemini
    /// file registration/polls and any SDK authorization retry.
    struct ModelHttpGuard {
        context: ProviderDispatchContext,
        approved: ResolvedModelBinding,
        last_call: tokio::sync::Mutex<Option<(String, ModelBindingProvenance)>>,
    }

    impl ModelHttpGuard {
        fn new(context: ProviderDispatchContext, approved: ResolvedModelBinding) -> Self {
            Self {
                context,
                approved,
                last_call: tokio::sync::Mutex::new(None),
            }
        }

        async fn observe_event(&self, event: &LlmEvent) -> Result<(), LlmError> {
            let Some(observer) = &self.context.observer else {
                return Ok(());
            };
            let terminal = matches!(
                event,
                LlmEvent::Done {
                    outcome: meerkat::LlmDoneOutcome::Success { .. }
                }
            );
            if !terminal && !matches!(event, LlmEvent::UsageUpdate { .. }) {
                return Ok(());
            }
            let (call_id, provenance) = self
                .last_call
                .lock()
                .await
                .clone()
                .ok_or_else(|| refusal("usage arrived without a physical dispatch receipt"))?;
            if let LlmEvent::UsageUpdate { usage } = event {
                let mut normalized = meerkat_core::CumulativeUsage::default();
                normalized.add_turn(usage);
                let usage = normalized.into_inner();
                observer
                    .observe_usage(
                        &call_id,
                        &provenance,
                        Some(usage.input_tokens),
                        Some(usage.output_tokens),
                    )
                    .await
                    .map_err(refusal)?;
            }
            if terminal {
                observer
                    .dispatch_finished(&call_id, &provenance)
                    .await
                    .map_err(refusal)?;
            }
            Ok(())
        }

        async fn current(&self) -> Result<ResolvedModelBinding, meerkat_core::AuthError> {
            let binding = self
                .context
                .repository
                .authorize_model_operation(
                    &self.context.space_id,
                    self.approved.provenance.operation,
                    Some(&self.approved.profile.id),
                )
                .await
                .map_err(|error| auth_refusal(error.dispatch_failure_message()))?;
            if binding.profile != self.approved.profile
                || binding.connection.provider != self.approved.connection.provider
                || binding.connection.account_id != self.approved.connection.account_id
                || binding.connection.endpoint != self.approved.connection.endpoint
            {
                return Err(auth_refusal(
                    "configuration_blocked: pinned model/connection identity changed",
                ));
            }
            elephant_embeddings::governed::validate_endpoint(&binding).map_err(auth_refusal)?;
            Ok(binding)
        }
    }

    fn validate_request_url(
        endpoint: &str,
        request_url: &str,
    ) -> Result<(), meerkat_core::AuthError> {
        let base =
            reqwest::Url::parse(endpoint).map_err(|_| auth_refusal("invalid approved endpoint"))?;
        let request = reqwest::Url::parse(request_url)
            .map_err(|_| auth_refusal("invalid provider request URL"))?;
        let base_path = base.path().trim_end_matches('/');
        if request.origin() != base.origin()
            || !request.username().is_empty()
            || request.password().is_some()
            || request.fragment().is_some()
            || !(request.path() == base_path
                || request.path().starts_with(&format!("{base_path}/")))
        {
            return Err(auth_refusal("physical request leaves approved endpoint"));
        }
        Ok(())
    }

    #[async_trait]
    impl meerkat_core::HttpAuthorizer for ModelHttpGuard {
        async fn authorize(
            &self,
            request: &mut meerkat_core::HttpAuthorizationRequest<'_>,
        ) -> Result<(), meerkat_core::AuthError> {
            let binding = self.current().await?;
            validate_request_url(&binding.connection.endpoint, request.url)?;
            let call_id = uuid::Uuid::now_v7().to_string();
            let observed_provenance = binding.provenance.clone();
            if let Some(observer) = &self.context.observer {
                observer
                    .before_dispatch(&call_id, &binding.provenance)
                    .await
                    .map_err(auth_refusal)?;
            }
            // The durable observer may wait on storage. Recheck before attaching
            // credentials so a revocation during that wait still blocks sending.
            let binding = self.current().await?;
            let reference = binding
                .connection
                .credential_ref
                .as_deref()
                .ok_or_else(|| {
                    auth_refusal("configuration_blocked: missing credential reference")
                })?;
            let credential = self
                .context
                .credentials
                .resolve(reference)
                .map_err(auth_refusal)?;
            let (header, value) = match binding.connection.provider.as_str() {
                "anthropic" => ("x-api-key", credential),
                "openai" => ("Authorization", format!("Bearer {credential}")),
                "gemini" => ("x-goog-api-key", credential),
                _ => return Err(auth_refusal("provider cannot generate")),
            };
            request
                .headers
                .retain(|(name, _)| !name.eq_ignore_ascii_case(header));
            request.headers.push((header.into(), value));
            *self.last_call.lock().await = Some((call_id, observed_provenance));
            Ok(())
        }

        fn label(&self) -> &str {
            "recall-current-space-approval"
        }
    }

    fn transport(
        binding: &ResolvedModelBinding,
        guard: Arc<ModelHttpGuard>,
    ) -> Result<Arc<dyn LlmClient>, LlmError> {
        elephant_embeddings::governed::validate_endpoint(binding).map_err(refusal)?;
        let endpoint = binding
            .connection
            .endpoint
            .trim_end_matches('/')
            .to_string();
        // Credentials are injected only by the per-request callback. Empty
        // constructor keys cannot enable an ambient conventional-env fallback.
        match binding.connection.provider.as_str() {
            "anthropic" => Ok(Arc::new(
                meerkat::AnthropicClient::builder(String::new())
                    .base_url(endpoint)
                    .authorizer(guard)
                    .build()?,
            )),
            "openai" => Ok(Arc::new(
                meerkat::OpenAiClient::new_with_base_url(String::new(), endpoint)
                    .with_responses_path("responses")
                    .with_authorizer(guard),
            )),
            "gemini" => Ok(Arc::new(
                meerkat::GeminiClient::new_with_base_url(String::new(), endpoint)
                    .with_authorizer(guard),
            )),
            _ => Err(refusal("approved provider does not support generation")),
        }
    }

    struct GovernedLlmClient {
        context: ProviderDispatchContext,
        approved: ResolvedModelBinding,
        /// Used only for local provider-specific projection/schema handling.
        projection_client: Arc<dyn LlmClient>,
    }

    #[async_trait]
    impl LlmClient for GovernedLlmClient {
        fn project_replay_messages(&self, messages: &[Message]) -> Result<Vec<Message>, LlmError> {
            self.projection_client.project_replay_messages(messages)
        }

        fn compile_schema(
            &self,
            schema: &meerkat_core::OutputSchema,
        ) -> Result<meerkat_core::schema::CompiledSchema, meerkat_core::schema::SchemaError>
        {
            self.projection_client.compile_schema(schema)
        }

        fn output_token_limit(&self) -> Option<u32> {
            self.projection_client.output_token_limit()
        }

        fn request_pressure(
            &self,
            request: &LlmRequest,
        ) -> Result<Option<meerkat_core::ProviderRequestPressure>, LlmError> {
            self.projection_client.request_pressure(request)
        }

        fn stream<'a>(
            &'a self,
            request: &'a LlmRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<LlmEvent, LlmError>> + Send + 'a>> {
            Box::pin(async_stream::try_stream! {
                let binding = self.context.repository.authorize_model_operation(
                    &self.context.space_id, self.approved.provenance.operation, Some(&self.approved.profile.id)
                ).await.map_err(|error| refusal(error.dispatch_failure_message()))?;
                if binding.profile != self.approved.profile
                    || binding.connection.provider != self.approved.connection.provider
                    || binding.connection.account_id != self.approved.connection.account_id
                    || binding.connection.endpoint != self.approved.connection.endpoint
                    || request.model != binding.profile.model {
                    Err(refusal("configuration_blocked: pinned model/connection identity changed"))?;
                }
                // Share endpoint capacity with embedding attempts. Keep the
                // permit inside the stream so completion, error and consumer
                // cancellation release it. SDK HTTP guards below still recheck
                // current approval/credentials after the wait and each retry.
                let _remote_permit = elephant_embeddings::remote_admission::acquire(
                    &binding.connection.endpoint
                ).await.map_err(refusal)?;
                let guard = Arc::new(ModelHttpGuard::new(self.context.clone(), binding.clone()));
                let client = transport(&binding, guard.clone())?;
                let mut response = client.stream(request);
                while let Some(event) = response.next().await {
                    let event = event?;
                    guard.observe_event(&event).await?;
                    yield event;
                }
            })
        }

        fn provider(&self) -> Provider {
            self.projection_client.provider()
        }

        async fn health_check(&self) -> Result<(), LlmError> {
            // Readiness does not spend tokens or call a provider endpoint.
            let binding = self
                .context
                .repository
                .authorize_model_operation(
                    &self.context.space_id,
                    self.approved.provenance.operation,
                    Some(&self.approved.profile.id),
                )
                .await
                .map_err(|error| refusal(error.dispatch_failure_message()))?;
            transport(
                &binding,
                Arc::new(ModelHttpGuard::new(self.context.clone(), binding.clone())),
            )
            .map(|_| ())
        }
    }

    impl ProviderDispatchContext {
        /// Install the exact approved model/account/endpoint and per-turn revocation
        /// check. The caller cannot smuggle a model override through job payloads.
        pub async fn govern_agent_build(
            &self,
            operation: ModelOperation,
            build: &mut AgentBuildConfig,
        ) -> PipelineResult<ModelBindingProvenance> {
            let binding = self
                .repository
                .authorize_model_operation(&self.space_id, operation, None)
                .await
                .map_err(resolution_failure)?;
            let client = transport(
                &binding,
                Arc::new(ModelHttpGuard::new(self.clone(), binding.clone())),
            )
            .map_err(blocked)?;
            build.model.clone_from(&binding.profile.model);
            build.provider = Some(client.provider());
            build.provider_params = None;
            // Recall's source-bound operations do not approve provider-hosted
            // web search. Keep their MCP/function tools and suppress SDK defaults.
            build.override_web_search = meerkat::ToolCategoryOverride::Disable;
            // Reusing a process fallback table could dispatch an unapproved model.
            build.model_fallback = Some(Default::default());
            build.agent_llm_client_override = None;
            build.llm_client_override = Some(Arc::new(GovernedLlmClient {
                context: self.clone(),
                approved: binding.clone(),
                projection_client: client,
            }));
            Ok(binding.provenance)
        }

        /// Approved single-turn transport for summarization and resolution.
        pub async fn model_client(
            &self,
            operation: ModelOperation,
        ) -> PipelineResult<(String, Arc<dyn LlmClient>, ModelBindingProvenance)> {
            let binding = self
                .repository
                .authorize_model_operation(&self.space_id, operation, None)
                .await
                .map_err(resolution_failure)?;
            self.client_for_binding(binding)
        }

        /// Resolve summary limits and transport from the same approved profile.
        ///
        /// # Errors
        /// Missing, revoked, invalid or unavailable model authority is refused.
        pub(crate) async fn summary_client(
            &self,
        ) -> PipelineResult<(
            String,
            Arc<dyn LlmClient>,
            elephant_types::summary_policy::SummaryPolicy,
        )> {
            let binding = self
                .repository
                .authorize_model_operation(&self.space_id, ModelOperation::Summary, None)
                .await
                .map_err(resolution_failure)?;
            let policy = binding.profile.summary.clone().unwrap_or_else(|| {
                elephant_types::summary_policy::SummaryPolicy::for_model(
                    &binding.connection.provider,
                    &binding.profile.model,
                )
            });
            policy
                .validate(&binding.connection.provider, &binding.profile.model)
                .map_err(blocked)?;
            let (model, client, _) = self.client_for_binding(binding)?;
            Ok((model, client, policy))
        }

        fn client_for_binding(
            &self,
            binding: ResolvedModelBinding,
        ) -> PipelineResult<(String, Arc<dyn LlmClient>, ModelBindingProvenance)> {
            let projection_client = transport(
                &binding,
                Arc::new(ModelHttpGuard::new(self.clone(), binding.clone())),
            )
            .map_err(blocked)?;
            let model = binding.profile.model.clone();
            let provenance = binding.provenance.clone();
            Ok((
                model,
                Arc::new(GovernedLlmClient {
                    context: self.clone(),
                    approved: binding,
                    projection_client,
                }),
                provenance,
            ))
        }
    }

    /// Convenience seam for code that already owns a storage client and space.
    pub async fn govern_agent_build(
        repository: SpaceConfigRepository,
        space_id: &str,
        operation: ModelOperation,
        build: &mut AgentBuildConfig,
    ) -> PipelineResult<ModelBindingProvenance> {
        ProviderDispatchContext::new(repository, space_id)
            .govern_agent_build(operation, build)
            .await
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::unwrap_used, clippy::too_many_lines)]
        use super::*;
        use elephant_embeddings::governed::ProviderDispatchObserver;
        use elephant_embeddings::{EmbeddingError, EmbeddingResult};
        use elephant_storage::{Repository, StorageClient};
        use elephant_types::provider_binding::SpaceProcessingConfig;
        use elephant_types::records::{base::BaseFields, SpaceConfig};
        use elephant_types::{RecordId, TableName};
        use std::sync::atomic::{AtomicUsize, Ordering};

        include!("provider_dispatch_gateway_tests.rs");
        include!("provider_dispatch_admission_tests.rs");
        include!("provider_dispatch_summary_policy_tests.rs");

        #[derive(Debug)]
        struct TestCredentials;
        impl CredentialResolver for TestCredentials {
            fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
                assert_eq!(reference, "fixture-key");
                Ok("local-fixture-secret".into())
            }
        }

        #[derive(Debug)]
        struct RevokeBeforeSend {
            repository: SpaceConfigRepository,
            calls: AtomicUsize,
        }
        #[async_trait]
        impl ProviderDispatchObserver for RevokeBeforeSend {
            async fn before_dispatch(
                &self,
                _: &str,
                provenance: &ModelBindingProvenance,
            ) -> EmbeddingResult<()> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let original = self
                    .repository
                    .get_by_space_id(&provenance.config.space_id)
                    .await
                    .unwrap()
                    .unwrap();
                let mut revoked = original.clone();
                let processing = revoked.processing.as_mut().unwrap();
                processing.generation += 1;
                processing
                    .bindings
                    .get_mut(&ModelOperation::Summary)
                    .unwrap()
                    .revoked = true;
                self.repository
                    .replace_authorized(&original, revoked)
                    .await
                    .map_err(|error| EmbeddingError::StorageError(error.to_string()))?;
                Ok(())
            }
            async fn observe_usage(
                &self,
                _: &str,
                _: &ModelBindingProvenance,
                _: Option<u64>,
                _: Option<u64>,
            ) -> EmbeddingResult<()> {
                Err(EmbeddingError::ProviderError(
                    "no request should have been sent".into(),
                ))
            }
        }

        #[tokio::test]
        #[expect(
            clippy::expect_used,
            reason = "Unexpected physical request success must fail the original SDK revocation assertion for every provider."
        )]
        async fn sdk_http_callback_rechecks_revocation_after_durable_observer() {
            for (provider, model, prefix) in [
                ("openai", "gpt-4.1-mini", "/v1"),
                ("anthropic", "claude-sonnet-4-5", ""),
                ("gemini", "gemini-2.5-flash", ""),
            ] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("http://{}{prefix}", listener.local_addr().unwrap());
                let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
                    "generation": 1, "index_generation": 0, "policy_id": "fixture-policy",
                    "connections": {"account": {"id":"account", "provider":provider, "account_id":"fixture-account", "endpoint":endpoint, "credential_ref":"fixture-key", "credential_generation":1, "revoked":false}},
                    "profiles": {"model": {"id":"model", "connection_id":"account", "model":model, "model_version":"fixture", "dimensions":null, "preprocessing":"text-v1"}},
                    "bindings": {"summary": {"id":"summary", "profile_id":"model", "revoked":false}},
                    "active_embedding_profile_id":null, "pending_reindex":null
                })).unwrap();
                let storage = StorageClient::new_embedded().await.unwrap();
                let repository = SpaceConfigRepository::new(storage);
                repository
                    .create(SpaceConfig {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::SpaceConfig),
                            "named-fixture".into(),
                            "test".into(),
                        ),
                        embedding_provider: "openai".into(),
                        embedding_model: "unused".into(),
                        embedding_dims: 2,
                        processing: Some(processing),
                        purpose: None,
                        skill: None,
                    })
                    .await
                    .unwrap();
                let observer = Arc::new(RevokeBeforeSend {
                    repository: repository.clone(),
                    calls: AtomicUsize::new(0),
                });
                let context = ProviderDispatchContext::new(repository, "named-fixture")
                    .with_credentials(Arc::new(TestCredentials))
                    .with_observer(observer.clone());
                let (model, client, _) =
                    context.model_client(ModelOperation::Summary).await.unwrap();
                let result = crate::llm::run_model_prompt(
                    client,
                    model,
                    None,
                    "local authorization fixture".into(),
                    10,
                )
                .await;
                let error = result.expect_err("revocation must stop the physical request");
                assert_eq!(
                    crate::scheduler::execution_contract::classify_failure(&error.into()),
                    crate::scheduler::execution_contract::FailureClass::ConfigurationBlocked,
                    "the real SDK callback must preserve recoverable revocation",
                );
                assert_eq!(
                    observer.calls.load(Ordering::SeqCst),
                    1,
                    "actual SDK must reach its physical-send authorization hook"
                );
                assert!(tokio::time::timeout(
                    std::time::Duration::from_millis(20),
                    listener.accept()
                )
                .await
                .is_err());
            }
        }

        #[test]
        fn concrete_request_must_stay_under_approved_endpoint() {
            assert!(validate_request_url(
                "https://models.example/v1",
                "https://models.example/v1/responses"
            )
            .is_ok());
            assert!(validate_request_url(
                "https://models.example/v1",
                "https://other.example/v1/responses"
            )
            .is_err());
            assert!(validate_request_url(
                "https://models.example/v1",
                "https://models.example/v10/responses"
            )
            .is_err());
            assert!(validate_request_url(
                "https://models.example/v1",
                "https://models.example/outside"
            )
            .is_err());
            assert!(validate_request_url(
                "http://127.0.0.1:8080",
                "http://127.0.0.1:8080/v1beta/models/test:streamGenerateContent?alt=sse"
            )
            .is_ok());
        }
    }
}

#[cfg(feature = "meerkat-extraction")]
pub use model::govern_agent_build;

#[cfg(test)]
mod resolution_error_tests {
    use super::*;
    use crate::scheduler::execution_contract::{classify_failure, FailureClass};
    use elephant_storage::{
        space_config_repository::SpaceResolutionError, StorageClient, StorageError,
    };
    use elephant_types::provider_binding::ProviderAuthorizationError;

    #[tokio::test]
    #[expect(
        clippy::expect_used,
        reason = "Backend setup failure or missing-space resolution success must fail the original two-backend classification control."
    )]
    async fn actual_embedding_resolver_missing_space_keeps_configuration_blocked() {
        use elephant_embeddings::governed::EmbeddingBindingResolver;
        for storage in [
            StorageClient::new_embedded().await.expect("embedded"),
            StorageClient::new_memory().await.expect("native"),
        ] {
            let error = SpaceConfigRepository::new(storage)
                .resolve(
                    "missing-approved-space",
                    ModelOperation::DocumentEmbedding,
                    "original-profile",
                )
                .await
                .expect_err("missing space must never dispatch");
            assert_eq!(
                classify_failure(&PipelineError::EmbeddingError(error).into()),
                FailureClass::ConfigurationBlocked
            );
        }
    }

    #[test]
    fn sdk_wrapping_preserves_typed_authorization_failure_categories() {
        for (error, expected) in [
            (
                SpaceResolutionError::Authorization(ProviderAuthorizationError::Revoked),
                FailureClass::ConfigurationBlocked,
            ),
            (
                SpaceResolutionError::Authorization(ProviderAuthorizationError::WrongSpace),
                FailureClass::Terminal,
            ),
            (
                SpaceResolutionError::Storage(StorageError::ConnectionError(
                    "configuration_blocked: timeout words are diagnostic only".into(),
                )),
                FailureClass::Transient,
            ),
            (
                SpaceResolutionError::Storage(StorageError::ErasureInProgress),
                FailureClass::Transient,
            ),
            (
                SpaceResolutionError::Storage(StorageError::DataIntegrityError(
                    "timeout configuration_blocked: malformed persisted row".into(),
                )),
                FailureClass::Terminal,
            ),
        ] {
            let wrapped = format!(
                "Invalid request: SDK authentication: {}",
                error.dispatch_failure_message()
            );
            assert_eq!(
                classify_failure(&PipelineError::ExtractionError(wrapped.clone()).into()),
                expected
            );
            assert_eq!(
                classify_failure(
                    &PipelineError::EmbeddingError(
                        elephant_embeddings::EmbeddingError::ProviderError(wrapped)
                    )
                    .into()
                ),
                expected
            );
        }
        assert_eq!(
            classify_failure(
                &PipelineError::ExtractionError(
                    "provider_authorization: physical request leaves approved endpoint".into()
                )
                .into()
            ),
            FailureClass::Terminal
        );
    }

    #[test]
    fn typed_resolution_preserves_outages_integrity_and_approval_failures() {
        for (error, expected) in [
            (
                SpaceResolutionError::Storage(StorageError::ConnectionError(
                    "provider_authorization text cannot override typed outage".into(),
                )),
                FailureClass::Transient,
            ),
            (
                SpaceResolutionError::Storage(StorageError::DataIntegrityError(
                    "temporary malformed policy row".into(),
                )),
                FailureClass::Terminal,
            ),
            (
                SpaceResolutionError::Authorization(
                    ProviderAuthorizationError::MissingConfiguration,
                ),
                FailureClass::ConfigurationBlocked,
            ),
        ] {
            assert_eq!(
                classify_failure(&resolution_failure(error).into()),
                expected
            );
        }
    }
}

#[cfg(test)]
#[path = "provider_dispatch_local_resource_tests.rs"]
mod local_resource_tests;
