//! Liveness is usable before schema initialization and without credentials.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

#[tokio::test]
async fn liveness_requires_no_configured_dependencies_or_authentication(
) -> Result<(), Box<dyn std::error::Error>> {
    for path in ["/v1/health", "/health"] {
        let response = super::health_routes()
            .oneshot(Request::builder().uri(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024).await?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(value, serde_json::json!({"status":"healthy"}));
    }
    Ok(())
}
