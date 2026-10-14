//! Durable source admission is separate from worker and search completion readiness.
use crate::{
    routes::entities::ApiError,
    runtime::{RuntimeManager, RuntimeMode},
};
use axum::{http::StatusCode, Json};
use std::sync::Arc;

/// Retained helper name for source adapters. No inline model execution occurs here.
///
/// # Errors
/// Refuses unconfigured/recovering storage and read-only or draining runtimes.
pub async fn ensure_semantic_ingest_ready(
    runtime: Option<&Arc<RuntimeManager>>,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if let Some(runtime) = runtime {
        let status = runtime.get_readiness().await;
        if !status.configured || !status.storage_ready || runtime.is_draining() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiError {
                    code: "RUNTIME_NOT_READY".into(),
                    message: "Source admission is unavailable; check get_runtime_status".into(),
                    details: None,
                }),
            ));
        }
        if status.runtime_mode == RuntimeMode::ReadOnly {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: "RUNTIME_READ_ONLY".into(),
                    message: "This runtime permits queries only".into(),
                    details: None,
                }),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn storage_only_api_admits_durable_sources_without_claiming_completion(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Arc::new(RuntimeManager::new_preconfigured(
            elephant_storage::StorageClient::new_embedded().await?,
        ));
        assert!(!runtime.get_readiness().await.semantic_ingest_ready);
        assert!(ensure_semantic_ingest_ready(Some(&runtime)).await.is_ok());
        runtime.set_mode_for_tests(RuntimeMode::ReadOnly).await;
        assert!(matches!(
            ensure_semantic_ingest_ready(Some(&runtime)).await,
            Err((StatusCode::FORBIDDEN, _))
        ));
        Ok(())
    }
}
