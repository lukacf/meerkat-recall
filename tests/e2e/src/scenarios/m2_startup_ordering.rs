//! Explicit schema initialization precedes readiness; liveness is independent.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension, Router,
};
use tower::ServiceExt;

use elephant_api::{
    auth::Principal,
    routes::{health::health_routes, readiness::readiness_routes},
    runtime::RuntimeManager,
};
use elephant_migrations::{apply_schema, verify_schema};
use elephant_storage::{StorageClient, StorageConfig};

#[tokio::test]
#[ignore]
async fn scenario_m2_startup_ordering() -> anyhow::Result<()> {
    let client = StorageClient::new(StorageConfig {
        url: "mem://".to_string(),
        namespace: "ns_startup_ordering".to_string(),
        database: "db_startup_ordering".to_string(),
        ..StorageConfig::default()
    })
    .await?;

    // A live listener must not imply storage initialization, nor initialize it.
    assert_eq!(
        get_json(health_routes(), "/v1/health").await?,
        (StatusCode::OK, serde_json::json!({"status":"healthy"}))
    );
    assert!(verify_schema(&client).await.is_err());
    assert!(client.list_tables().await?.is_empty());

    // The explicit migration step establishes the receipts serve later verifies.
    apply_schema(&client).await.map_err(anyhow::Error::msg)?;
    verify_schema(&client).await.map_err(anyhow::Error::msg)?;
    let runtime = Arc::new(RuntimeManager::new_preconfigured(client));
    let readiness = readiness_routes()
        .with_state(runtime.clone())
        .layer(Extension(Principal {
            subject: "startup-status-reader".into(),
            scopes: vec!["read:status".into()],
            ..Principal::default()
        }));
    let (status, body) = get_json(readiness.clone(), "/v1/readiness").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ready"], true);
    assert_eq!(body["storage_ready"], true);
    assert_eq!(body["recovery_ready"], true);
    // This fixture hosts only the API; it must not invent a running worker.
    assert_eq!(body["worker_ready"], false);
    assert_eq!(body["semantic_ingest_ready"], false);

    runtime.begin_shutdown().await;
    let (status, body) = get_json(readiness, "/v1/readiness").await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["ready"], false);
    assert_eq!(body["draining"], true);
    assert_eq!(body["error"]["error_code"], "not_ready");
    assert_eq!(
        get_json(health_routes(), "/v1/health").await?,
        (StatusCode::OK, serde_json::json!({"status":"healthy"}))
    );
    Ok(())
}

async fn get_json(app: Router, path: &str) -> anyhow::Result<(StatusCode, serde_json::Value)> {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty())?)
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4096).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}
