//! Same managed Router before and after real runtime initialization.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    response::Response,
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt as _;

use crate::{auth::Claims, runtime::RuntimeManager, AuthConfig};

const SECRET: &str = "synthetic-only-managed-bootstrap-route-secret";
const ENVIRONMENT: &[&str] = &[
    "ELEPHANT_DB",
    "SURREALDB_URL",
    "SURREALDB_QUERY_TIMEOUT_SECS",
    "SURREALDB_TRANSACTION_TIMEOUT_SECS",
    "ELEPHANT_DB_SNAPSHOT_LOAD",
    "PIPELINE_AGENTIC_MCP_URL",
    "PIPELINE_AGENTIC_MODEL",
    "PIPELINE_EMBED_ENABLED",
    "PIPELINE_MAX_CONCURRENT",
    "PIPELINE_MAX_EXTRACT_CONCURRENT",
    "PIPELINE_WORK_QUEUE_BATCH_SIZE",
    "PIPELINE_WORK_QUEUE_ALLOWED_KINDS",
    "PIPELINE_WORK_QUEUE_DISABLED_KINDS",
    "ELEPHANT_EXTENSIONS",
];

struct Environment {
    cwd: PathBuf,
    root: PathBuf,
    values: Vec<(&'static str, Option<OsString>)>,
}

impl Environment {
    fn new() -> Self {
        let cwd = std::env::current_dir().unwrap();
        let root = std::env::temp_dir().join(format!("managed-bootstrap-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".git")).unwrap();
        let values = ENVIRONMENT
            .iter()
            .map(|&name| {
                let value = std::env::var_os(name);
                std::env::remove_var(name);
                (name, value)
            })
            .collect();
        std::env::set_current_dir(&root).unwrap();
        Self { cwd, root, values }
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.cwd).unwrap();
        for (name, value) in &self.values {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn bootstrap_token(space: &str) -> String {
    let now = chrono::Utc::now().timestamp();
    let claims: Claims = serde_json::from_value(json!({
        "sub": format!("bootstrap-{space}"), "iat": now, "exp": now + 900,
        "space_id": space, "spaces": [space], "bootstrap_space": space,
        "scopes": ["read:space_config", "write:space_config", "manage:grant"],
        "clearances": [{"policy_id": format!("{space}-policy"), "max_level": 3,
                         "labels": [], "handling_allow": []}]
    }))
    .unwrap();
    elephant_auth::service_token::issue_space_bootstrap(SECRET, &claims, now).unwrap()
}

fn space_request(space: &str) -> Value {
    json!({"name": space,
        "security": {"policy_id": format!("{space}-policy"), "level": 3,
                     "labels": [], "handling": []},
        "embedding_provider": "openai", "embedding_model": "unapproved-no-outbound",
        "embedding_dims": 384})
}

async fn request(
    app: &Router,
    method: Method,
    path: &str,
    token: Option<&str>,
    space: &str,
    body: Value,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("Content-Type", "application/json")
        .header("X-Space-Id", space);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    app.clone()
        .oneshot(
            request
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn initialize(runtime: &RuntimeManager, root: &std::path::Path) {
    // This is the exact RuntimeManager transition called by initialize_runtime
    // over MCP. No replacement storage, route reconstruction or process restart.
    let patch = serde_json::from_value(json!({
        "backend": "elephant-db", "ingest_enabled": false, "extractor_required": false,
        "snapshot_path": root.join("state.snapshot"), "multimodal_asset_root": root.join("media")
    }))
    .unwrap();
    let status = runtime.initialize_runtime(patch).await.unwrap();
    assert!(status.configured && status.storage_ready);
    assert!(!status.bootstrap_mode && !status.worker_ready);
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn bootstrap_router_activates_same_process_with_named_authorization() {
    let environment = Environment::new();
    let runtime = Arc::new(
        RuntimeManager::new_with_ingest_enabled(Some(false))
            .await
            .unwrap(),
    );
    let app = crate::managed_api_router(
        AuthConfig {
            secret: SECRET.into(),
            ..AuthConfig::default()
        },
        runtime.clone(),
    )
    .await
    .unwrap();
    let household = bootstrap_token("household");
    let business = bootstrap_token("business");

    for token in [None, Some(household.as_str())] {
        let response = request(
            &app,
            Method::POST,
            "/v1/spaces",
            token,
            "household",
            space_request("household"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    assert!(runtime.current_storage().await.is_none());
    initialize(&runtime, &environment.root).await;

    let denied = request(
        &app,
        Method::POST,
        "/v1/spaces",
        None,
        "household",
        space_request("household"),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let wrong = request(
        &app,
        Method::POST,
        "/v1/spaces",
        Some(&household),
        "business",
        space_request("business"),
    )
    .await;
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);

    for (space, token) in [("household", &household), ("business", &business)] {
        let response = request(
            &app,
            Method::POST,
            "/v1/spaces",
            Some(token),
            space,
            space_request(space),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 32_768).await.unwrap()).unwrap();
        assert_eq!(value["space_id"], space);
        assert_eq!(value["security"]["policy_id"], format!("{space}-policy"));
        let read = request(
            &app,
            Method::GET,
            &format!("/v1/spaces/{space}"),
            Some(token),
            space,
            Value::Null,
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
    }
    let denied = request(
        &app,
        Method::GET,
        "/v1/spaces/business",
        Some(&household),
        "household",
        Value::Null,
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn initialized_router_still_refuses_data_after_runtime_drain() {
    let environment = Environment::new();
    let runtime = Arc::new(
        RuntimeManager::new_with_ingest_enabled(Some(false))
            .await
            .unwrap(),
    );
    let app = crate::managed_api_router(
        AuthConfig {
            secret: SECRET.into(),
            ..AuthConfig::default()
        },
        runtime.clone(),
    )
    .await
    .unwrap();
    initialize(&runtime, &environment.root).await;
    let token = bootstrap_token("household");
    let created = request(
        &app,
        Method::POST,
        "/v1/spaces",
        Some(&token),
        "household",
        space_request("household"),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);
    runtime.shutdown().await.unwrap();
    let response = request(
        &app,
        Method::GET,
        "/v1/spaces/household",
        Some(&token),
        "household",
        Value::Null,
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}
