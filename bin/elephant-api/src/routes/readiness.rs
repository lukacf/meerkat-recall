//! Authenticated readiness, separate from process liveness and source coverage.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::get, Extension, Json, Router};
use serde::Serialize;

use crate::auth::Principal;
use crate::public_error::{ApiFailure, ErrorCode, PublicError};
use crate::runtime::{RuntimeManager, RuntimeMode};

#[derive(Debug, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Expose each existing readiness dimension independently on the wire."
)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub configured: bool,
    pub runtime_mode: RuntimeMode,
    pub storage_ready: bool,
    pub worker_ready: bool,
    pub semantic_ingest_ready: bool,
    pub recovery_ready: bool,
    pub draining: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<PublicError>,
}

pub(super) fn authorize_status(principal: &Principal) -> Result<(), ApiFailure> {
    if principal.bypass_abac
        || principal.scopes.iter().any(|scope| {
            matches!(
                scope.as_str(),
                "*" | "read:status" | "read:system" | "read:space_config"
            )
        })
    {
        return Ok(());
    }
    Err(ApiFailure::new(
        ErrorCode::ForbiddenSpace,
        "Readiness requires read:status scope",
    ))
}

async fn readiness(
    State(runtime): State<Arc<RuntimeManager>>,
    Extension(principal): Extension<Principal>,
) -> Result<(StatusCode, Json<ReadinessResponse>), ApiFailure> {
    authorize_status(&principal)?;
    let status = runtime.get_readiness().await;
    let recovery_ready = runtime
        .current_storage()
        .await
        .is_some_and(|storage| storage.recovery_status().ready);
    let draining = runtime.is_draining();
    let ready = status.configured
        && status.storage_ready
        && recovery_ready
        && !draining
        && (!runtime.workers_required().await || status.semantic_ingest_ready);
    Ok((
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadinessResponse {
            ready,
            configured: status.configured,
            runtime_mode: status.runtime_mode,
            storage_ready: status.storage_ready,
            worker_ready: status.worker_ready,
            semantic_ingest_ready: status.semantic_ingest_ready,
            recovery_ready,
            draining,
            error: (!ready).then(|| {
                PublicError::new(
                    ErrorCode::NotReady,
                    "Requested service capabilities are not ready",
                )
            }),
        }),
    ))
}

pub fn readiness_routes() -> Router<Arc<RuntimeManager>> {
    Router::new().route("/v1/readiness", get(readiness))
}

/// Process liveness has no database, worker, provider or authentication dependency.
pub async fn liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"healthy"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn draining_service_stays_live_but_is_not_ready() -> Result<(), Box<dyn std::error::Error>>
    {
        let runtime = Arc::new(RuntimeManager::new_with_storage_for_tests(
            elephant_storage::StorageClient::new_embedded().await?,
        ));
        runtime.begin_shutdown().await;
        let principal = Principal {
            scopes: vec!["read:status".into()],
            ..Principal::default()
        };
        let response = readiness_routes()
            .with_state(runtime)
            .layer(Extension(principal))
            .oneshot(
                Request::builder()
                    .uri("/v1/readiness")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[test]
    fn ingest_scope_does_not_grant_global_status() {
        let principal = Principal {
            scopes: vec!["write:source".into()],
            ..Principal::default()
        };
        assert!(authorize_status(&principal).is_err());
    }
}
