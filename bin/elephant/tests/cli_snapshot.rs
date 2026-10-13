//! CLI wire tests only; recovery backend consistency has independent storage tests.
#![allow(clippy::unwrap_used)]
use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use tokio::process::Command;

#[tokio::test]
async fn named_export_sends_bounded_quiesce_and_requires_a_manifest() {
    for valid in [true, false] {
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let app = Router::new().route("/v1/system/snapshot/export", post(move |Json(request): Json<Value>| {
            let send = send.clone();
            async move {
                send.send(request).await.unwrap();
                let mut response = json!({"success":true, "backend":"elephant-db", "path":"/backups/nightly", "message":"exported"});
                if valid {
                    response["manifest"] = json!({"version":1,"format":"recall-snapshot-v1", "snapshot_id":uuid::Uuid::new_v4(), "journal_id":uuid::Uuid::new_v4(), "backend":"elephant-db","name":"nightly","created_at":chrono::Utc::now(),"expires_at":chrono::Utc::now()+chrono::Duration::days(7),"bytes":123,"sha256":"a".repeat(64)});
                }
                Json(response)
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let output = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"))
            .env_remove("ELEPHANT_API_BEARER_TOKEN_FILE")
            .args([
                "--api-url",
                &url,
                "--token",
                "fixture",
                "snapshot",
                "export",
                "--output",
                "/backups/nightly",
                "--name",
                "nightly",
                "--retention-days",
                "7",
                "--quiesce-timeout-seconds",
                "20",
                "--json",
            ])
            .output()
            .await
            .unwrap();
        assert_eq!(
            output.status.success(),
            valid,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let request = receive.recv().await.unwrap();
        assert_eq!(
            request,
            json!({"path":"/backups/nightly","name":"nightly","retention_seconds":604_800,"quiesce_timeout_seconds":20})
        );
        let _: Value = serde_json::from_slice(&output.stdout).unwrap();
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn verification_is_offline_and_invalid_retention_is_usage_error() {
    let missing = tempfile::tempdir().unwrap().path().join("absent");
    let output = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"))
        .args([
            "--api-url",
            "http://127.0.0.1:1",
            "snapshot",
            "verify",
            "--input",
        ])
        .arg(missing)
        .output()
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("management request"));
    for days in ["0", "31"] {
        let output = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"))
            .args([
                "snapshot",
                "export",
                "--output",
                "/backups/new",
                "--retention-days",
                days,
            ])
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
    }
}
