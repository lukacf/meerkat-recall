//! Exercise installed CLI readiness and compatibility behavior against loopback fixtures.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::{http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Value};
use tokio::process::Command;

fn runtime() -> Value {
    json!({
        "configured": true, "bootstrap_mode": false, "runtime_mode": "shared_memory",
        "backend": "surreal-db", "storage_ready": true, "worker_ready": true,
        "extractor_ready": true, "semantic_ingest_ready": true, "issues": []
    })
}

#[tokio::test]
async fn strict_readiness_checks_http_and_payload_without_corrupting_json() {
    for case in [
        "ready",
        "unhealthy",
        "health_http",
        "runtime_http",
        "missing_runtime",
        "malformed_runtime",
        "not_ready",
    ] {
        let health_code = if case == "health_http" {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        };
        let runtime_code = if case == "runtime_http" {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::OK
        };
        let health = json!({"status": if case == "unhealthy" { "unhealthy" } else { "healthy" }});
        let mut state = runtime();
        match case {
            "missing_runtime" => state = Value::Null,
            "malformed_runtime" => {
                state["configured"] = json!("true");
            }
            "not_ready" => {
                state["semantic_ingest_ready"] = json!(false);
            }
            _ => {}
        }
        let app = Router::new()
            .route(
                "/v1/health",
                get(move || {
                    let health = health.clone();
                    async move { (health_code, Json(health)) }
                }),
            )
            .route(
                "/v1/runtime/status",
                get(move || {
                    let state = state.clone();
                    async move { (runtime_code, Json(state)) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        for strict in [false, true] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"));
            command.env_remove("RUST_LOG").args([
                "--api-url",
                &url,
                "--token",
                "fixture",
                "status",
                "--json",
            ]);
            if strict {
                command.arg("--require-ready");
            }
            let output = command.output().await.unwrap();
            assert_eq!(
                output.status.success(),
                !strict || case == "ready",
                "case={case}, strict={strict}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let payload: Value = serde_json::from_slice(&output.stdout)
                .expect("stdout must remain diagnostic JSON even on failure");
            assert_eq!(payload.as_object().unwrap().len(), 2);
            assert!(payload.get("health").is_some());
            assert!(payload.get("runtime").is_some());
        }
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn unavailable_server_fails_readiness() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let output = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"))
        .env_remove("RUST_LOG")
        .args(["--api-url", &url, "status", "--json", "--require-ready"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_and_legacy_paths_report_the_same_version() {
    let binary = env!("CARGO_BIN_EXE_meerkat-recall");
    let dir = tempfile::tempdir().unwrap();
    let alias = dir.path().join("elephant");
    std::os::unix::fs::symlink(binary, &alias).unwrap();
    let canonical = Command::new(binary)
        .env_remove("RUST_LOG")
        .arg("--version")
        .output()
        .await
        .unwrap();
    let legacy = Command::new(alias)
        .env_remove("RUST_LOG")
        .arg("--version")
        .output()
        .await
        .unwrap();
    assert!(canonical.status.success());
    assert!(legacy.status.success());
    assert_eq!(canonical.stdout, legacy.stdout);
    assert_eq!(
        String::from_utf8(canonical.stdout).unwrap().trim(),
        concat!("meerkat-recall ", env!("CARGO_PKG_VERSION"))
    );
}
