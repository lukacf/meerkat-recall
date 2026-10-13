//! HTTP adapters share the MCP provider-administration implementation.
use crate::{
    auth::Principal,
    public_error::{ApiFailure, ErrorCode},
    AuthzService,
};
use axum::{
    extract::{rejection::JsonRejection, Path, State},
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router,
};
use elephant_mcp::{
    handlers::{
        processing::{ReindexAction, ReindexHandler},
        ToolHandler,
    },
    ToolContext,
};
use elephant_storage::StorageClient;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
pub struct ProcessingState {
    storage: StorageClient,
    authz: Arc<AuthzService>,
}
impl ProcessingState {
    #[expect(
        clippy::must_use_candidate,
        reason = "Retain the existing constructor and builder annotation contract."
    )]
    pub const fn new(storage: StorageClient, authz: Arc<AuthzService>) -> Self {
        Self { storage, authz }
    }
}

async fn invoke(
    state: &ProcessingState,
    principal: &Principal,
    verified: Option<elephant_auth::VerifiedCallerContext>,
    space: String,
    action: ReindexAction,
    mut body: Value,
) -> Result<Json<Value>, ApiFailure> {
    if !body.is_object() {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Expected a JSON object",
        ));
    }
    state
        .authz
        .authorize_space_scope(principal, &space, "read:space_config")?;
    body["space_id"] = json!(space);
    let context = ToolContext::new(
        state.storage.clone(),
        state.authz.to_abac_principal(principal),
    )
    .with_verified_caller(verified);
    let response = ReindexHandler::new(state.storage.clone(), action)
        .execute(&context, body)
        .await
        .map_err(|error| match error {
            elephant_mcp::McpError::Public(error) => ApiFailure(error),
            elephant_mcp::McpError::InvalidParameters(message) => {
                ApiFailure::new(ErrorCode::InvalidRequest, message)
            }
            _ => ApiFailure::new(ErrorCode::Unavailable, "Reindex administration unavailable"),
        })?;
    Ok(Json(response.get("data").cloned().ok_or_else(|| {
        ApiFailure::new(ErrorCode::Unavailable, "Missing reindex response")
    })?))
}
async fn status(
    State(state): State<Arc<ProcessingState>>,
    Extension(principal): Extension<Principal>,
    Path(space): Path<String>,
    verified: Option<Extension<elephant_auth::VerifiedCallerContext>>,
) -> Result<Json<Value>, ApiFailure> {
    invoke(
        &state,
        &principal,
        verified.map(|Extension(caller)| caller),
        space,
        ReindexAction::Status,
        json!({}),
    )
    .await
}
async fn mutate(
    State(state): State<Arc<ProcessingState>>,
    Extension(principal): Extension<Principal>,
    Path((space, action)): Path<(String, String)>,
    verified: Option<Extension<elephant_auth::VerifiedCallerContext>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiFailure> {
    let body = body
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid JSON request"))?
        .0;
    let action = match action.as_str() {
        "start" => ReindexAction::Start,
        "activate" => ReindexAction::Activate,
        "cancel" => ReindexAction::Cancel,
        _ => {
            return Err(ApiFailure::new(
                ErrorCode::NotFound,
                "Unknown reindex action",
            ))
        }
    };
    let status = if matches!(action, ReindexAction::Start) {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        invoke(
            &state,
            &principal,
            verified.map(|Extension(caller)| caller),
            space,
            action,
            body,
        )
        .await?,
    ))
}
pub fn processing_routes() -> Router<Arc<ProcessingState>> {
    Router::new()
        .route("/v1/spaces/{space}/processing/reindex", get(status))
        .route(
            "/v1/spaces/{space}/processing/reindex/{action}",
            post(mutate),
        )
}
