// Included inside provider_dispatch::model::tests. Real authorization, no HTTP.
#[derive(Debug)]
struct GatewayFixtureCredentials(std::collections::BTreeMap<String, String>);
impl CredentialResolver for GatewayFixtureCredentials {
    fn resolve(&self, reference: &str) -> EmbeddingResult<String> {
        self.0.get(reference).cloned().ok_or_else(|| {
            EmbeddingError::ProviderError(
                "No fixture capability for exact credential reference".into(),
            )
        })
    }
}

async fn gateway_binding_contract(storage: StorageClient, backend: &str) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/recall_preview_semantics/gateway/binding-fixtures.json"
    )))
    .unwrap();
    assert_eq!(fixture["live_ready"], false);
    let repository = SpaceConfigRepository::new(storage);
    let all_bindings = fixture["gateway_bindings"].as_array().unwrap();
    for entry in fixture["space_processing_configurations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["backend"] == backend)
    {
        let space = entry["space_id"].as_str().unwrap();
        let processing: SpaceProcessingConfig =
            serde_json::from_value(entry["processing"].clone()).unwrap();
        processing.validate().unwrap();
        let embedding_id = processing.active_embedding_profile_id.as_ref().unwrap();
        let embedding = &processing.profiles[embedding_id];
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            space.into(),
            "offline-fixture".into(),
        );
        base.security = serde_json::from_value(entry["security"].clone()).unwrap();
        let original = repository
            .create(SpaceConfig {
                base,
                embedding_provider: "openai".into(),
                embedding_model: embedding.model.clone(),
                embedding_dims: i32::try_from(embedding.dimensions.unwrap()).unwrap(),
                processing: Some(processing.clone()),
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        let mut secrets = std::collections::BTreeMap::new();
        for connection in processing.connections.values() {
            let reference = connection.credential_ref.clone().unwrap();
            assert!(reference.starts_with("RECALL_GATEWAY_"));
            assert_ne!(reference, "OPENAI_API_KEY");
            secrets.insert(reference.clone(), format!("fixture-only-{reference}"));
        }
        let credentials = Arc::new(GatewayFixtureCredentials(secrets));
        let context = ProviderDispatchContext::new(repository.clone(), space)
            .with_credentials(credentials.clone());
        let mut embedding_policies = Vec::new();
        for operation in processing.bindings.keys().copied() {
            let approved = repository
                .authorize_model_operation(space, operation, None)
                .await
                .unwrap();
            let manifest = all_bindings
                .iter()
                .find(|binding| {
                    binding["backend"] == backend
                        && binding["space"] == space
                        && binding["profile_id"] == approved.profile.id
                })
                .unwrap();
            assert_eq!(manifest["connection_id"], approved.connection.id);
            assert_eq!(manifest["model"], approved.profile.model);
            assert_eq!(approved.connection.endpoint, "http://127.0.0.1:43129/v1");
            let reference = approved.connection.credential_ref.as_deref().unwrap();
            assert_eq!(
                reference,
                format!("RECALL_GATEWAY_{}", manifest["id"].as_str().unwrap())
            );
            let expected = credentials.resolve(reference).unwrap();
            if operation.is_embedding() {
                let (policy, provider, provenance) = context.embedding(operation).await.unwrap();
                assert_eq!(provenance.operation, operation);
                assert_eq!(provider.dimensions(), approved.profile.dimensions.unwrap());
                embedding_policies.push(policy.model);
                let guard = EmbeddingDispatch::new(
                    Arc::new(repository.clone()),
                    credentials.clone(),
                    approved.clone(),
                );
                // The embedding retry loop invokes this guard again before each
                // request. This test never calls provider.embed.
                for _ in 0..2 {
                    let (binding, capability) = guard.authorize().await.unwrap();
                    assert_eq!(binding.provenance.operation, operation);
                    assert_eq!(binding.connection.endpoint, approved.connection.endpoint);
                    assert_eq!(capability.as_deref(), Some(expected.as_str()));
                }
                assert_eq!(manifest["operation"], "embedding");
                assert_eq!(manifest["dimensions"], approved.profile.dimensions.unwrap());
            } else {
                governed_agent_tool_policy_contract(&context, operation).await;
                let (model_name, _transport, provenance) =
                    context.model_client(operation).await.unwrap();
                assert_eq!(model_name, approved.profile.model);
                assert_eq!(provenance.operation, operation);
                let guard = ModelHttpGuard::new(context.clone(), approved.clone());
                let mut call_ids = Vec::new();
                for _ in 0..2 {
                    let mut headers = vec![("Authorization".into(), "untrusted-old-value".into())];
                    let mut request = meerkat_core::HttpAuthorizationRequest {
                        method: "POST",
                        url: "http://127.0.0.1:43129/v1/responses",
                        headers: &mut headers,
                    };
                    meerkat_core::HttpAuthorizer::authorize(&guard, &mut request)
                        .await
                        .unwrap();
                    assert_eq!(
                        headers,
                        vec![("Authorization".into(), format!("Bearer {expected}"))]
                    );
                    let (call, observed) = guard.last_call.lock().await.clone().unwrap();
                    assert_eq!(observed.operation, operation);
                    assert_eq!(observed.config.space_id, space);
                    call_ids.push(call);
                }
                assert_ne!(
                    call_ids[0], call_ids[1],
                    "physical retries get separate accounting identity"
                );
                for url in [
                    "https://api.openai.com/v1/responses",
                    "http://127.0.0.1:43129/v10/responses",
                ] {
                    let mut headers = Vec::new();
                    let mut request = meerkat_core::HttpAuthorizationRequest {
                        method: "POST",
                        url,
                        headers: &mut headers,
                    };
                    assert!(
                        meerkat_core::HttpAuthorizer::authorize(&guard, &mut request)
                            .await
                            .is_err()
                    );
                    assert!(
                        headers.is_empty(),
                        "denied endpoint never receives a capability"
                    );
                }
            }
        }
        assert_eq!(embedding_policies.len(), 2);
        assert_eq!(
            embedding_policies[0], embedding_policies[1],
            "query/document use one active partition"
        );
        assert!(repository
            .authorize_model_operation(space, ModelOperation::Triage, None)
            .await
            .is_err());
        assert!(repository
            .authorize_model_operation("unconfigured-space", ModelOperation::Extraction, None)
            .await
            .is_err());

        let approved_embedding = repository
            .authorize_model_operation(space, ModelOperation::QueryEmbedding, None)
            .await
            .unwrap();
        let embedding_guard = EmbeddingDispatch::new(
            Arc::new(repository.clone()),
            credentials.clone(),
            approved_embedding,
        );
        let approved_model = repository
            .authorize_model_operation(space, ModelOperation::Extraction, None)
            .await
            .unwrap();
        let model_guard = ModelHttpGuard::new(context.clone(), approved_model);
        let mut revoked = original.clone();
        revoked.processing.as_mut().unwrap().generation += 1;
        for connection in revoked
            .processing
            .as_mut()
            .unwrap()
            .connections
            .values_mut()
        {
            connection.revoked = true;
        }
        repository
            .replace_authorized(&original, revoked)
            .await
            .unwrap();
        assert!(
            embedding_guard.authorize().await.is_err(),
            "query retries must reload revocation"
        );
        let mut headers = Vec::new();
        let mut request = meerkat_core::HttpAuthorizationRequest {
            method: "POST",
            url: "http://127.0.0.1:43129/v1/responses",
            headers: &mut headers,
        };
        assert!(
            meerkat_core::HttpAuthorizer::authorize(&model_guard, &mut request)
                .await
                .is_err()
        );
        assert!(headers.is_empty());
    }
}

#[tokio::test]
async fn embedded_gateway_bindings_preserve_roles_at_physical_authorization() {
    gateway_binding_contract(StorageClient::new_embedded().await.unwrap(), "elephant-db").await;
}

#[tokio::test]
async fn surreal_gateway_bindings_preserve_roles_at_physical_authorization() {
    gateway_binding_contract(StorageClient::new_memory().await.unwrap(), "surreal-db").await;
}

async fn physical_completion_contract(storage: StorageClient) {
    use crate::provider_usage::DurableUsageObserver;
    use crate::scheduler::execution_contract::{AttemptUsage, UsageBasis};
    use meerkat_core::{ProviderTokenAccounting, TurnUsage, Usage};
    let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
        "generation": 1, "index_generation": 0, "policy_id": "fixture-policy",
        "connections": {"account": {"id":"account", "provider":"openai", "account_id":"fixture-account", "endpoint":"http://127.0.0.1:43129/v1", "credential_ref":"fixture-key", "credential_generation":1, "revoked":false}},
        "profiles": {"model": {"id":"model", "connection_id":"account", "model":"gpt-4.1-mini", "model_version":"fixture", "dimensions":null, "preprocessing":"text-v1"}},
        "bindings": {"summary": {"id":"summary", "profile_id":"model", "revoked":false}},
        "active_embedding_profile_id":null, "pending_reindex":null
    })).unwrap();
    let repository = SpaceConfigRepository::new(storage.clone());
    repository
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "completion-fixture".into(),
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
    for outcome in ["drop", "error", "success", "missing-usage"] {
        let observer =
            DurableUsageObserver::for_activity(storage.clone(), "completion-fixture", outcome);
        let context = ProviderDispatchContext::new(repository.clone(), "completion-fixture")
            .with_credentials(Arc::new(TestCredentials))
            .with_observer(observer.clone());
        let binding = repository
            .authorize_model_operation("completion-fixture", ModelOperation::Summary, None)
            .await
            .unwrap();
        let guard = ModelHttpGuard::new(context, binding);
        let terminal = LlmEvent::Done {
            outcome: meerkat::LlmDoneOutcome::Success {
                stop_reason: meerkat::StopReason::EndTurn,
            },
        };
        assert!(
            guard.observe_event(&terminal).await.is_err(),
            "completion requires the original physical dispatch"
        );
        let mut headers = Vec::new();
        let mut request = meerkat_core::HttpAuthorizationRequest {
            method: "POST",
            url: "http://127.0.0.1:43129/v1/responses",
            headers: &mut headers,
        };
        // Exercise the installed SDK authorizer and event accounting callback,
        // without a socket, credential lookup from the environment, or inference.
        meerkat_core::HttpAuthorizer::authorize(&guard, &mut request)
            .await
            .unwrap();
        let reserved = AttemptUsage::reserved(100, 1.0);
        if outcome != "missing-usage" {
            let event = LlmEvent::UsageUpdate {
                usage: TurnUsage::new(
                    Usage {
                        input_tokens: 8,
                        output_tokens: 2,
                        ..Usage::default()
                    },
                    ProviderTokenAccounting::openai("gpt-4.1-mini", 8),
                ),
            };
            guard.observe_event(&event).await.unwrap();
            guard.observe_event(&event).await.unwrap();
            let partial = observer.summarize(reserved.clone()).await.unwrap();
            assert_eq!(partial.basis, UsageBasis::Unknown);
            assert_eq!(partial.measured_tokens, Some(10));
        }
        match outcome {
            "error" => guard
                .observe_event(&LlmEvent::Done {
                    outcome: meerkat::LlmDoneOutcome::Error {
                        error: refusal("truncated fixture stream"),
                    },
                })
                .await
                .unwrap(),
            "success" | "missing-usage" => {
                guard.observe_event(&terminal).await.unwrap();
                guard.observe_event(&terminal).await.unwrap();
            }
            _ => {}
        }
        drop(guard);
        let final_usage = observer.summarize(reserved).await.unwrap();
        assert_eq!(
            final_usage.basis,
            if outcome == "success" {
                UsageBasis::Measured
            } else {
                UsageBasis::Unknown
            }
        );
        assert_eq!(
            final_usage.measured_tokens,
            (outcome != "missing-usage").then_some(10)
        );
    }
}

#[tokio::test]
async fn embedded_physical_stream_usage_requires_terminal_success() {
    physical_completion_contract(StorageClient::new_embedded().await.unwrap()).await;
}

#[tokio::test]
async fn surreal_physical_stream_usage_requires_terminal_success() {
    physical_completion_contract(StorageClient::new_memory().await.unwrap()).await;
}

// This fixture exercises the real Recall approval/build seam and the SDK's
// actual provider-default policy. It is not a model or relevance acceptance.
#[derive(Debug)]
struct GovernedFunctionTools {
    tools: Arc<[Arc<meerkat::ToolDef>]>,
}

#[async_trait]
impl meerkat::AgentToolDispatcher for GovernedFunctionTools {
    fn tools(&self) -> Arc<[Arc<meerkat::ToolDef>]> {
        Arc::clone(&self.tools)
    }

    async fn dispatch(
        &self,
        _call: meerkat_core::ToolCallView<'_>,
    ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat::ToolError> {
        Err(meerkat::ToolError::Other(
            "policy-only fixture must never execute a tool".into(),
        ))
    }
}

async fn governed_agent_tool_policy_contract(
    context: &ProviderDispatchContext,
    operation: ModelOperation,
) {
    use meerkat::ToolCategoryOverride;
    use meerkat_core::lifecycle::run_primitive::ProviderTag;

    let external: Arc<dyn meerkat::AgentToolDispatcher> = Arc::new(GovernedFunctionTools {
        tools: vec![Arc::new(meerkat::ToolDef::new(
            "get_source",
            "Read the source through the supplied authorized function dispatcher",
            serde_json::json!({"type": "object", "properties": {}}),
        ))]
        .into(),
    });
    let factory = meerkat::AgentFactory::minimal();
    let config = meerkat_core::Config::default();
    for requested in [
        ToolCategoryOverride::Inherit,
        ToolCategoryOverride::Enable,
        ToolCategoryOverride::Disable,
    ] {
        let mut build = AgentBuildConfig::new("unapproved-model");
        build.override_web_search = requested;
        build.external_tools = Some(Arc::clone(&external));
        let provenance = context
            .govern_agent_build(operation, &mut build)
            .await
            .unwrap();
        assert_eq!(provenance.operation, operation);
        assert_eq!(build.model, "gpt-6.1-sol");
        assert_eq!(build.override_web_search, ToolCategoryOverride::Disable);
        assert!(Arc::ptr_eq(
            build.external_tools.as_ref().unwrap(),
            &external
        ));
        assert!(Arc::ptr_eq(
            &build.external_tools.as_ref().unwrap().tools()[0],
            &external.tools()[0],
        ));
        assert!(build.llm_client_override.is_some());
        assert!(build.provider_params.is_none());

        let identity = meerkat_core::SessionLlmIdentity {
            model: build.model.clone(),
            provider: build.provider.unwrap(),
            self_hosted_server_id: None,
            provider_params: build.provider_params.clone(),
            auth_binding: None,
        };
        let baseline = factory
            .request_policy_for_llm_identity(&config, &identity, ToolCategoryOverride::Inherit)
            .unwrap();
        let mut expected = baseline
            .provider_tool_defaults
            .and_then(|tag| match tag {
                ProviderTag::OpenAi(tag) => Some(tag),
                _ => None,
            })
            .unwrap();
        assert!(expected.web_search.is_some());
        assert!(expected.prompt_cache_options.is_some());
        expected.web_search = None;
        let governed = factory
            .request_policy_for_llm_identity(&config, &identity, build.override_web_search)
            .unwrap();
        assert_eq!(
            governed.provider_tool_defaults,
            Some(ProviderTag::OpenAi(expected)),
            "only hosted web search is removed; cache and other SDK defaults remain"
        );
    }
    assert!(config.provider_tools.openai.web_search);
    assert_eq!(
        AgentBuildConfig::new("gpt-6.1-sol").override_web_search,
        ToolCategoryOverride::Inherit,
        "Recall's governed build must not mutate defaults for other SDK consumers"
    );
}
