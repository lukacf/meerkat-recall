//! Current named-space capabilities, without provider credentials or global queue data.
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Extension, Json, Router,
};
use elephant_embeddings::governed::{CredentialResolver, EnvironmentCredentials};
use elephant_storage::{SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::ModelOperation;
use serde_json::{json, Value};

use crate::{
    auth::Principal,
    public_error::{ApiFailure, ErrorCode, PublicError},
    runtime::{RuntimeManager, RuntimeMode},
    AuthzService,
};

#[derive(Clone)]
pub struct SpaceStatusState {
    storage: StorageClient,
    runtime: Arc<RuntimeManager>,
    authz: Arc<AuthzService>,
}
impl SpaceStatusState {
    pub const fn new(
        storage: StorageClient,
        runtime: Arc<RuntimeManager>,
        authz: Arc<AuthzService>,
    ) -> Self {
        Self {
            storage,
            runtime,
            authz,
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope; Keep ordered authorization, source lifecycle and configuration checks in their existing operation"
)]
async fn status(
    State(state): State<Arc<SpaceStatusState>>,
    Extension(principal): Extension<Principal>,
    Path(space): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiFailure> {
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:status")?;
    let repository = SpaceConfigRepository::new(state.storage.clone());
    let config = repository
        .get_by_space_id(&space)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::UnknownSpace, "Unknown space"))?;
    state
        .authz
        .authorize_record_read(&principal, &config, "read:status")?;
    let runtime = state.runtime.get_readiness().await;
    let mut bindings = serde_json::Map::new();
    let mut available_operations = serde_json::Map::new();
    // A vector partition contains account/endpoint configuration. Status-only
    // access must not disclose it; use the same record's configuration authority.
    let inspect_recipes = state
        .authz
        .authorize_record_read(&principal, &config, "read:space_config")
        .is_ok();
    let mut embedding_recipes = serde_json::Map::new();
    for (name, operation) in [
        ("query_embedding", ModelOperation::QueryEmbedding),
        ("document_embedding", ModelOperation::DocumentEmbedding),
        ("reindex_embedding", ModelOperation::ReindexEmbedding),
        ("extraction", ModelOperation::Extraction),
        ("support_verification", ModelOperation::SupportVerification),
        ("triage", ModelOperation::Triage),
    ] {
        let (approved, available) = match repository
            .authorize_model_operation(&space, operation, None)
            .await
        {
            Ok(binding) => {
                let token = &binding.provenance.config;
                if token.space_id != space
                    || token.config_id != config.base.id.to_string()
                    || !config.processing.as_ref().is_some_and(|processing| {
                        token.generation == processing.generation
                            && token.policy_id == processing.policy_id
                    })
                {
                    return Err(ApiFailure::new(
                        ErrorCode::NotReady,
                        "Space configuration changed while resolving provider settings; retry",
                    ));
                }
                let recipe = operation
                    .is_embedding()
                    .then(|| elephant_embeddings::provider::embedding_execution_recipe(&binding));
                let executable_recipe = recipe.as_ref().is_none_or(Result::is_ok);
                if inspect_recipes {
                    if let Some(Ok(recipe)) = &recipe {
                        embedding_recipes.insert(name.into(), json!(recipe));
                    }
                }
                let available = if !executable_recipe {
                    false
                } else if binding.connection.provider == "fastembed" {
                    elephant_embeddings::validate_fastembed_assets(&binding)
                        .await
                        .is_ok()
                } else {
                    binding
                        .connection
                        .credential_ref
                        .as_deref()
                        .is_some_and(|reference| EnvironmentCredentials.resolve(reference).is_ok())
                };
                (true, available)
            }
            Err(
                elephant_storage::space_config_repository::SpaceResolutionError::Authorization(_),
            ) => (false, false),
            Err(elephant_storage::space_config_repository::SpaceResolutionError::Storage(
                error,
            )) => return Err(error.into()),
        };
        bindings.insert(name.into(), json!(approved));
        available_operations.insert(name.into(), json!(available));
    }
    // Run the same local asset check used before extraction, without inference
    // or a provider request. Per-document overrides are checked when dispatched.
    let guidance_config = config.clone();
    let guidance_ready = tokio::task::spawn_blocking(move || {
        elephant_pipeline::scheduler::validate_extraction_guidance_assets(
            Some(&guidance_config),
            None,
            None,
        )
        .is_ok()
    })
    .await
    .map_err(|_| {
        ApiFailure::new(
            ErrorCode::Unavailable,
            "Unable to check selected guidance assets",
        )
    })?;
    if repository.get_by_space_id(&space).await?.as_ref() != Some(&config) {
        return Err(ApiFailure::new(
            ErrorCode::NotReady,
            "Space configuration changed while checking readiness; retry",
        ));
    }
    // Public source PUT selects Graph. Deterministic triage makes its model
    // binding optional; independent support verification is a graph dependency.
    let providers_ready = [
        "query_embedding",
        "document_embedding",
        "extraction",
        "support_verification",
    ]
    .iter()
    .all(|operation| available_operations.get(*operation) == Some(&Value::Bool(true)));
    let storage_ready = runtime.configured
        && runtime.storage_ready
        && state.storage.recovery_status().ready
        && !state.runtime.is_draining();
    let semantic_ingest_ready = storage_ready
        && runtime.semantic_ingest_ready
        && providers_ready
        && guidance_ready
        && runtime.runtime_mode != RuntimeMode::ReadOnly;
    let ready = storage_ready && (!state.runtime.workers_required().await || semantic_ingest_ready);
    let mut body = json!({
        "space":space,
        "ready":ready,
        "storage_ready":storage_ready,
        "semantic_ingest_ready":semantic_ingest_ready,
        "runtime_mode":runtime.runtime_mode,
        "processing_generation":config.processing.as_ref().map(|processing|processing.generation),
        "approved_operations":bindings,
        "available_operations":available_operations,
        "providers_ready":providers_ready,
        "guidance_ready":guidance_ready,
        "capabilities":{"source_read":storage_ready,"source_write":storage_ready && runtime.runtime_mode != RuntimeMode::ReadOnly,"lexical_search":storage_ready,"semantic_ingest":semantic_ingest_ready},
        "error":(!ready).then(|| PublicError::new(ErrorCode::NotReady, "Selected space capabilities are not ready")),
    });
    if inspect_recipes {
        body["embedding_execution_recipes"] = Value::Object(embedding_recipes);
    }
    Ok((
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    ))
}

pub fn space_status_routes() -> Router<Arc<SpaceStatusState>> {
    Router::new().route("/v1/spaces/{space}/status", get(status))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_storage::Repository;
    use elephant_types::records::SpaceConfig;
    use elephant_types::{RecordId, TableName};

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope; Keep the complete authorization and publication control sequence in one test"
    )]
    async fn approval_without_local_credentials_is_not_provider_readiness(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let reference = format!("RECALL_TEST_MISSING_{}", uuid::Uuid::new_v4().simple());
        let mut bindings = serde_json::Map::new();
        for operation in [
            "query_embedding",
            "document_embedding",
            "extraction",
            "support_verification",
            "triage",
        ] {
            bindings.insert(
                operation.into(),
                json!({"id":format!("{operation}-approval"),
                "profile_id":if operation.ends_with("embedding") {"embedding"} else {"generation"},
                "revoked":false}),
            );
        }
        let config: SpaceConfig = serde_json::from_value(json!({
            "id":RecordId::new_v4(TableName::SpaceConfig), "space_id":"alpha",
            "created_at":chrono::Utc::now(), "updated_at":chrono::Utc::now(), "created_by":"operator",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{}, "subjects":[], "subjects_state":"complete",
            "embedding_provider":"openai", "embedding_model":"fixture", "embedding_dims":3,
            "purpose":null, "skill":null,
            "processing":{"generation":1,"index_generation":0,"policy_id":"operator-approved",
                "connections":{"remote":{"id":"remote","provider":"openai","account_id":"fixture",
                    "endpoint":"https://example.test/v1","credential_ref":reference,"credential_generation":1,"revoked":false}},
                "profiles":{
                    "embedding":{"id":"embedding","connection_id":"remote","model":"fixture","model_version":"v1","dimensions":3,"preprocessing":"text-v1"},
                    "generation":{"id":"generation","connection_id":"remote","model":"fixture","model_version":"v1","dimensions":null,"preprocessing":"text-v1"}},
                "bindings":bindings,"active_embedding_profile_id":"embedding","pending_reindex":null}
        }))?;
        let repository = SpaceConfigRepository::new(storage.clone());
        repository.create(config.clone()).await?;
        let runtime = Arc::new(RuntimeManager::new_preconfigured(storage.clone()));
        let state = Arc::new(SpaceStatusState::new(
            storage,
            runtime.clone(),
            Arc::new(AuthzService::new()),
        ));
        let principal = Principal {
            subject: "service".into(),
            space_id: Some("alpha".into()),
            bypass_abac: true,
            ..Principal::default()
        };
        let (http_status, Json(body)) = status(
            State(state.clone()),
            Extension(principal.clone()),
            Path("alpha".into()),
        )
        .await?;
        // API-only role is available, while processor availability is honestly false.
        assert_eq!(http_status, StatusCode::OK);
        assert_eq!(body["approved_operations"]["query_embedding"], true);
        assert_eq!(body["available_operations"]["query_embedding"], false);
        assert_eq!(body["approved_operations"]["support_verification"], true);
        assert_eq!(body["providers_ready"], false);
        assert_eq!(body["semantic_ingest_ready"], false);
        assert!(!body.to_string().contains(&reference));
        let recipe = &body["embedding_execution_recipes"]["query_embedding"];
        assert_eq!(recipe["max_total_http_attempts"], 3);
        assert_eq!(recipe["partition"]["space_id"], "alpha");
        assert_eq!(recipe["partition"]["preprocessing"], "text-v1");
        assert_eq!(recipe["provenance"]["config"]["generation"], 1);
        assert_eq!(
            recipe["provenance"]["binding_id"],
            "query_embedding-approval"
        );
        assert!(body["embedding_execution_recipes"]
            .get("reindex_embedding")
            .is_none());

        let mut ordinary = Principal {
            subject: "status-reader".into(),
            space_id: Some("alpha".into()),
            scopes: vec!["read:status".into()],
            bypass_abac: false,
            ..Principal::default()
        };
        let (_, Json(status_only)) = status(
            State(state.clone()),
            Extension(ordinary.clone()),
            Path("alpha".into()),
        )
        .await?;
        assert!(status_only.get("embedding_execution_recipes").is_none());
        assert!(!status_only.to_string().contains("example.test"));
        ordinary.scopes.push("read:space_config".into());
        let (_, Json(authorized)) = status(
            State(state.clone()),
            Extension(ordinary.clone()),
            Path("alpha".into()),
        )
        .await?;
        assert_eq!(
            authorized["embedding_execution_recipes"]["query_embedding"],
            *recipe
        );
        let mut revoked = config.clone();
        let processing = revoked.processing.as_mut().ok_or("missing processing")?;
        processing.generation += 1;
        processing
            .bindings
            .get_mut(&ModelOperation::QueryEmbedding)
            .ok_or("missing query binding")?
            .revoked = true;
        repository.replace_authorized(&config, revoked).await?;
        let (_, Json(current)) = status(
            State(state.clone()),
            Extension(ordinary),
            Path("alpha".into()),
        )
        .await?;
        assert_eq!(current["approved_operations"]["query_embedding"], false);
        assert!(current["embedding_execution_recipes"]
            .get("query_embedding")
            .is_none());
        assert_eq!(
            current["embedding_execution_recipes"]["document_embedding"]["provenance"]["config"]
                ["generation"],
            2
        );
        runtime.begin_shutdown().await;
        let (http_status, Json(body)) =
            status(State(state), Extension(principal), Path("alpha".into())).await?;
        assert_eq!(http_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["capabilities"]["source_write"], false);
        assert_eq!(body["error"]["error_code"], "not_ready");
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_explicit_guidance_blocks_semantic_capability_without_leaking_asset_names(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let missing_skill = format!("recall-uninstalled-{}", uuid::Uuid::new_v4().simple());
        let config: SpaceConfig = serde_json::from_value(json!({
            "id":RecordId::new_v4(TableName::SpaceConfig), "space_id":"alpha",
            "created_at":chrono::Utc::now(), "updated_at":chrono::Utc::now(), "created_by":"operator",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{}, "subjects":[], "subjects_state":"complete",
            "embedding_provider":"openai", "embedding_model":"fixture", "embedding_dims":3,
            "purpose":null, "skill":missing_skill, "processing":null
        }))?;
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await?;
        let runtime = Arc::new(RuntimeManager::new_preconfigured(storage.clone()));
        let state = Arc::new(SpaceStatusState::new(
            storage,
            runtime,
            Arc::new(AuthzService::new()),
        ));
        let principal = Principal {
            subject: "service".into(),
            space_id: Some("alpha".into()),
            bypass_abac: true,
            ..Principal::default()
        };
        let (http_status, Json(body)) =
            status(State(state), Extension(principal), Path("alpha".into())).await?;
        // Durable API admission remains available; processor capability does not.
        assert_eq!(http_status, StatusCode::OK);
        assert_eq!(body["guidance_ready"], false);
        assert_eq!(body["semantic_ingest_ready"], false);
        assert_eq!(body["capabilities"]["source_write"], true);
        assert_eq!(body["capabilities"]["semantic_ingest"], false);
        assert!(!body.to_string().contains(&missing_skill));
        Ok(())
    }
}
