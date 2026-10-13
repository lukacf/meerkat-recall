//! Installed command contract. Fixtures test CLI behavior, not service readiness.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Value};
use tokio::process::Command;

#[tokio::test]
async fn readiness_exit_codes_preserve_diagnostics_and_ignore_healthy_liveness() {
    for (case, code, body, expected) in [
        ("ready", StatusCode::OK, json!({"ready":true}), 0),
        (
            "restore_barrier",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"ready":false}),
            3,
        ),
        ("draining", StatusCode::OK, json!({"ready":false}), 3),
        (
            "unauthorized",
            StatusCode::UNAUTHORIZED,
            json!({"code":"unauthorized"}),
            5,
        ),
        (
            "forbidden",
            StatusCode::FORBIDDEN,
            json!({"code":"forbidden_space"}),
            5,
        ),
        ("missing_readiness", StatusCode::NOT_FOUND, json!({}), 4),
        ("malformed", StatusCode::OK, json!({"ready":"true"}), 4),
        (
            "inconsistent",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"ready":true}),
            4,
        ),
    ] {
        let app = Router::new()
            .route(
                "/v1/health",
                get(|| async { Json(json!({"status":"healthy"})) }),
            )
            .route(
                "/v1/runtime/status",
                get(|| async { Json(json!({"semantic_ingest_ready":true})) }),
            )
            .route(
                "/v1/readiness",
                get(move || {
                    let body = body.clone();
                    async move { (code, Json(body)) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        for strict in [false, true] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"));
            command
                .env_remove("RUST_LOG")
                .env_remove("ELEPHANT_API_BEARER_TOKEN_FILE")
                .args(["--api-url", &url, "--token", "fixture", "status", "--json"]);
            if strict {
                command.arg("--require-ready");
            }
            let output = command.output().await.unwrap();
            assert_eq!(
                output.status.code(),
                Some(if strict { expected } else { 0 }),
                "{case}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let diagnostic: Value =
                serde_json::from_slice(&output.stdout).expect("JSON survives refusal");
            assert!(diagnostic.get("health").is_some());
            assert!(diagnostic.get("runtime").is_some());
            assert!(diagnostic.get("readiness").is_some());
        }
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn unavailable_server_emits_json_and_exit_four() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let output = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"))
        .env_remove("ELEPHANT_API_BEARER_TOKEN")
        .env_remove("ELEPHANT_API_BEARER_TOKEN_FILE")
        .args(["--api-url", &url, "status", "--json", "--require-ready"])
        .output()
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(4));
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["readiness"]["error"], "service_unavailable");
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_and_legacy_paths_report_the_same_version() {
    let binary = env!("CARGO_BIN_EXE_meerkat-recall");
    let dir = tempfile::tempdir().unwrap();
    let alias = dir.path().join("elephant");
    std::os::unix::fs::symlink(binary, &alias).unwrap();
    let canonical = Command::new(binary)
        .arg("--version")
        .output()
        .await
        .unwrap();
    let legacy = Command::new(alias).arg("--version").output().await.unwrap();
    assert!(canonical.status.success());
    assert!(legacy.status.success());
    assert_eq!(canonical.stdout, legacy.stdout);
    assert_eq!(
        String::from_utf8(canonical.stdout).unwrap().trim(),
        concat!("meerkat-recall ", env!("CARGO_PKG_VERSION"))
    );
}
