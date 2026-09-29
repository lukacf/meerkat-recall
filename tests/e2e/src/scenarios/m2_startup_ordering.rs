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
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    Router,
};
use tower::ServiceExt;

use elephant_migrations::apply_schema;
use elephant_storage::{StorageClient, StorageConfig};

use elephant_api::auth::Principal as AuthPrincipal;
use elephant_api::authz::AuthzService;
use elephant_api::routes::health::{health_routes, HealthState};

#[tokio::test]
#[ignore]
async fn scenario_m2_startup_ordering() -> anyhow::Result<()> {
    let config = StorageConfig {
        url: "mem://".to_string(),
        namespace: "ns_startup_ordering".to_string(),
        database: "db_startup_ordering".to_string(),
        ..StorageConfig::default()
    };
    let client = StorageClient::new(config).await?;

    // Apply schema
    apply_schema(&client)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Then start API health
    let authz = Arc::new(AuthzService::new());
    let state = Arc::new(HealthState::new(client, authz));
    let app = health_routes()
        .layer(middleware::from_fn(mock_auth))
        .with_state(state);

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
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(json["status"], "healthy");
    assert_eq!(json["surrealdb"], "connected");

    Ok(())
}

async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
    let mut req = req;
    req.extensions_mut().insert(AuthPrincipal {
        subject: "test-user".to_string(),
        space_id: Some("Space_X".to_string()),
        scopes: vec!["read:entity".to_string()],
        security_level: 10,
        security_labels: vec![],
        ..AuthPrincipal::default()
    });
    next.run(req).await
}
