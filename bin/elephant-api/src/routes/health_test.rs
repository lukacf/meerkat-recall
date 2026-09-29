#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
//! Integration tests for health endpoint.

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        clippy::unused_async,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::{self, Next},
        response::IntoResponse,
        Router,
    };
    use tower::ServiceExt;

    use elephant_storage::StorageResult;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::health::{health_routes, HealthState, StorageHealth};

    #[derive(Clone, Debug)]
    struct AlwaysHealthyStorage;

    #[async_trait::async_trait]
    impl StorageHealth for AlwaysHealthyStorage {
        async fn health_check(&self) -> StorageResult<()> {
            Ok(())
        }
    }

    async fn setup_test_app() -> Router {
        let authz = Arc::new(AuthzService::new());
        let storage: Arc<dyn StorageHealth> = Arc::new(AlwaysHealthyStorage);
        let state = Arc::new(HealthState::new_with_storage(storage, authz));
        health_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:entity".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn test_health_response_matches_spec_shape() {
        let app = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("status").is_some());
        assert!(json.get("backend").is_some());
        assert!(json.get("storage").is_some());
        assert!(json.get("surrealdb").is_some());
        assert!(json.get("embedding_provider").is_some());
        assert!(json.get("circuit_breakers").is_some());
        assert!(json["circuit_breakers"].get("storage").is_some());
        assert!(json["circuit_breakers"].get("surrealdb").is_some());
        assert!(json["circuit_breakers"].get("embeddings").is_some());
    }
}
