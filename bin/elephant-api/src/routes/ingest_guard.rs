use std::sync::Arc;

use axum::{http::StatusCode, Json};

use crate::routes::entities::ApiError;
use crate::runtime::RuntimeManager;

pub async fn ensure_semantic_ingest_ready(
    runtime: Option<&Arc<RuntimeManager>>,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if let Some(runtime) = runtime {
        let status = runtime.get_readiness().await;
        if !status.semantic_ingest_ready {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiError {
                    code: "RUNTIME_NOT_READY".to_string(),
                    message: "Semantic ingest is not ready; check get_runtime_status".to_string(),
                    details: None,
                }),
            ));
        }
    }
    Ok(())
}
