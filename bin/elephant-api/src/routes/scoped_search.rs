//! Exact revision search for one explicitly authorized named space.
use axum::{
    extract::{Path, State},
    routing::post,
    Extension, Json, Router,
};
use elephant_mcp::scoped_search::{self, SearchRequest, SearchResponse};
use elephant_mcp::{authz::AuthzService as McpAuthz, ToolContext};
use elephant_storage::{SpaceConfigRepository, StorageClient};
use std::sync::Arc;

use crate::auth::Principal;
use crate::authz::AuthzState;
use crate::public_error::{ApiFailure, ErrorCode};

/// Dependencies for scoped search. Public writes and runtime gates stay with
/// their existing owners.
#[derive(Clone)]
pub struct SearchState {
    storage: StorageClient,
    authz: AuthzState,
}
impl SearchState {
    /// Construct with the same storage and authorization as retained routes.
    #[must_use]
    pub const fn new(storage: StorageClient, authz: AuthzState) -> Self {
        Self { storage, authz }
    }
}

/// Mount underneath the existing authentication and runtime middleware.
pub fn router(storage: StorageClient, authz: AuthzState) -> Router {
    Router::new()
        .route("/v1/spaces/{space}/search", post(search))
        .with_state(Arc::new(SearchState::new(storage, authz)))
}

/// The actual HTTP handler used by the public router and qualification tests.
///
/// # Errors
///
/// Returns request, authorization or unknown-space errors, and propagates
/// storage and scoped-search failures through the shared public error type.
pub async fn search(
    State(state): State<Arc<SearchState>>,
    Path(space): Path<String>,
    Extension(principal): Extension<Principal>,
    request: Result<Json<SearchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<SearchResponse>, ApiFailure> {
    let Json(request) = request
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid scoped search JSON"))?;
    request.validate().map_err(ApiFailure)?;
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:doc:raw")?;
    let config = SpaceConfigRepository::new(state.storage.clone())
        .get_by_space_id(&space)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::UnknownSpace, "Selected space does not exist"))?;
    state
        .authz
        .authorize_record_read(&principal, &config, "read:doc:raw")?;
    let ctx = ToolContext::with_authz(
        state.storage.clone(),
        Arc::new(McpAuthz::new_with_skip_auth(principal.bypass_abac)),
        state.authz.to_abac_principal(&principal),
    );
    scoped_search::search(&ctx, &request)
        .await
        .map(Json)
        .map_err(ApiFailure)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope"
    )]
    async fn call(
        space: &str,
        principal_space: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let storage = StorageClient::new_embedded().await.unwrap();
        let principal = Principal {
            subject: "reader".into(),
            space_id: Some(principal_space.into()),
            scopes: vec!["read:doc:raw".into()],
            ..Principal::default()
        };
        let app = router(storage, Arc::new(crate::authz::AuthzService::new()))
            .layer(Extension(principal));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/spaces/{space}/search"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn actual_route_returns_typed_validation_and_scope_errors() {
        for body in [
            serde_json::json!({"query":""}),
            serde_json::json!({"query":"valid","limit":51}),
            serde_json::json!({"query":"valid","source_revision_constraints":[]}),
            serde_json::json!({"query":"valid","filters":{"labels":{"initiative":[]}}}),
        ] {
            let (status, body) = call("alpha", "alpha", body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(body.to_string().contains("invalid_request"));
        }
        let (status, body) = call(
            "other",
            "alpha",
            serde_json::json!({"query":"valid","use_vector":false}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.to_string().contains("forbidden_space"));
        let (status, body) = call(
            "alpha",
            "alpha",
            serde_json::json!({"query":"valid","use_vector":false}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.to_string().contains("unknown_space"));
    }
}
