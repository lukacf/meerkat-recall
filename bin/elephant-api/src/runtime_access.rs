//! Runtime capability enforcement shared by the REST data routes.

use std::sync::Arc;

use axum::{
    extract::{MatchedPath, Request, State},
    http::Method,
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::public_error::{ApiFailure, ErrorCode};
use crate::runtime::{RuntimeManager, RuntimeMode};

fn is_query(method: &Method, path: Option<&str>) -> bool {
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return true;
    }
    *method == Method::POST
        && matches!(
            path,
            Some(
                "/v1/entities/search"
                    | "/v1/entities/batch-get"
                    | "/v1/relationships/search"
                    | "/v1/timeline/search"
                    | "/v1/assertions/search"
                    | "/v1/docs/search"
                    | "/v1/spaces/{space}/search"
                    | "/v1/spaces/{space_id}/search"
                    | "/v1/sources/search"
                    | "/v1/system/snapshot/export"
            )
        )
}

#[expect(
    clippy::large_futures,
    clippy::redundant_pub_crate,
    reason = "Keep the crate-scoped middleware boundary and original dispatch future layout."
)]
pub(crate) async fn enforce_runtime_access(
    State(runtime): State<Arc<RuntimeManager>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str);
    let restore = matches!(
        path,
        Some("/v1/system/snapshot/restore" | "/v1/system/snapshot/import")
    );
    let owns_barrier = matches!(
        path,
        Some(
            "/v1/system/snapshot/export"
                | "/v1/system/snapshot/restore"
                | "/v1/system/snapshot/import"
        )
    ) || (*request.method() == Method::POST
        && matches!(path, Some("/v1/spaces/{space}/sources/{external_id}")));
    let status = runtime.get_readiness().await;
    if !status.storage_ready && !restore {
        return ApiFailure::new(
            ErrorCode::NotReady,
            "Storage recovery or runtime drain is in progress",
        )
        .into_response();
    }
    if !is_query(request.method(), path) && runtime.is_draining() {
        return ApiFailure::new(ErrorCode::NotReady, "Runtime is draining").into_response();
    }
    if !is_query(request.method(), path) && status.runtime_mode == RuntimeMode::ReadOnly {
        return ApiFailure::new(
            ErrorCode::CapabilityDisabled,
            "This runtime permits queries only",
        )
        .into_response();
    }
    if owns_barrier {
        return next.run(request).await;
    }
    let Some(storage) = runtime.current_storage().await else {
        return ApiFailure::new(ErrorCode::NotReady, "Storage is not configured").into_response();
    };
    let query = is_query(request.method(), path);
    let read_storage = storage.clone();
    let dispatch = async move {
        if query {
            match elephant_storage::read_eligibility::with_read_fence(
                read_storage,
                next.run(request),
            )
            .await
            {
                Ok(response) => response,
                Err(elephant_storage::StorageError::Conflict(_)) => ApiFailure::new(
                    ErrorCode::NotReady,
                    "Source visibility changed while assembling this response; retry the read",
                )
                .into_response(),
                Err(error) => ApiFailure::from(error).into_response(),
            }
        } else {
            next.run(request).await
        }
    };
    match storage.with_activity(dispatch).await {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(error=%error,"HTTP storage admission refused");
            ApiFailure::new(
                ErrorCode::NotReady,
                "Storage recovery or maintenance is in progress",
            )
            .into_response()
        }
    }
}
