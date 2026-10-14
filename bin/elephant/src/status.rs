//! Bounded CLI liveness/readiness diagnostics and stable exit categories.
use anyhow::{Context, Result};
use reqwest::StatusCode;
use serde::Serialize;

use crate::{api_base_url_from_target, api_token_from_target, ApiTarget, StatusArgs};

#[derive(Debug)]
pub struct StatusFailure(pub i32, pub &'static str);
impl std::fmt::Display for StatusFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.1)
    }
}
impl std::error::Error for StatusFailure {}

#[derive(Serialize)]
struct Endpoint {
    http_status: Option<u16>,
    body: Option<serde_json::Value>,
    error: Option<&'static str>,
}

#[derive(Serialize)]
struct StatusResponse {
    health: Endpoint,
    runtime: Endpoint,
    readiness: Endpoint,
}

#[expect(
    clippy::option_if_let_else,
    reason = "Keep bounded response decoding and both redacted failure categories explicit."
)]
async fn endpoint(
    client: &reqwest::Client,
    url: String,
    token: Option<&str>,
    space: Option<&str>,
) -> Endpoint {
    let mut request = client.get(url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(space) = space {
        request = request.header("X-Space-Id", space);
    }
    match request.send().await {
        Ok(response) => {
            let code = response.status().as_u16();
            match crate::bounded_json(response).await {
                Ok(body) => Endpoint {
                    http_status: Some(code),
                    body: Some(body),
                    error: None,
                },
                Err(_) => Endpoint {
                    http_status: Some(code),
                    body: None,
                    error: Some("malformed_or_oversized_response"),
                },
            }
        }
        Err(_) => Endpoint {
            http_status: None,
            body: None,
            error: Some("service_unavailable"),
        },
    }
}

pub async fn run(target: &ApiTarget, args: StatusArgs) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build status client")?;
    let token = api_token_from_target(target)?;
    let base = api_base_url_from_target(target);
    let (health, runtime, readiness) = tokio::join!(
        endpoint(
            &client,
            format!("{base}/v1/health"),
            token.as_deref(),
            args.space.as_deref()
        ),
        endpoint(
            &client,
            format!("{base}/v1/runtime/status"),
            token.as_deref(),
            args.space.as_deref()
        ),
        endpoint(
            &client,
            format!("{base}/v1/readiness"),
            token.as_deref(),
            args.space.as_deref()
        ),
    );
    let payload = StatusResponse {
        health,
        runtime,
        readiness,
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "liveness: {}",
            payload
                .health
                .http_status
                .map_or_else(|| "unavailable".to_string(), |code| code.to_string())
        );
        println!("readiness: {}", readiness_label(&payload.readiness));
        if let Some(runtime) = &payload.runtime.body {
            println!("runtime: {}", serde_json::to_string(runtime)?);
        }
    }
    if args.require_ready {
        require_ready(&payload.readiness)?;
    } else if payload.health.http_status.is_none() {
        return Err(StatusFailure(4, "service is unavailable").into());
    }
    Ok(())
}

fn readiness_label(endpoint: &Endpoint) -> &'static str {
    match require_ready(endpoint) {
        Ok(()) => "ready",
        Err(StatusFailure(3, _)) => "not_ready",
        Err(StatusFailure(5, _)) => "unauthorized",
        Err(_) => "unavailable_or_malformed",
    }
}

fn require_ready(endpoint: &Endpoint) -> std::result::Result<(), StatusFailure> {
    if matches!(endpoint.http_status, Some(401 | 403)) {
        return Err(StatusFailure(5, "readiness access was refused"));
    }
    if endpoint.error.is_some() {
        return Err(StatusFailure(
            4,
            "readiness response is unavailable or malformed",
        ));
    }
    let ready = endpoint
        .body
        .as_ref()
        .and_then(|body| body.get("ready"))
        .and_then(serde_json::Value::as_bool);
    match (endpoint.http_status, ready) {
        (Some(code), Some(true)) if code == StatusCode::OK.as_u16() => Ok(()),
        (Some(200 | 503), Some(false)) => Err(StatusFailure(3, "service is not ready")),
        _ => Err(StatusFailure(
            4,
            "readiness response is unavailable or inconsistent",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn healthy_process_never_substitutes_for_readiness() {
        for (code, ready, expected) in [
            (200, true, 0),
            (200, false, 3),
            (503, false, 3),
            (503, true, 4),
            (401, false, 5),
            (403, false, 5),
            (404, false, 4),
        ] {
            let endpoint = Endpoint {
                http_status: Some(code),
                body: Some(serde_json::json!({"ready":ready})),
                error: None,
            };
            assert_eq!(
                require_ready(&endpoint).err().map_or(0, |error| error.0),
                expected
            );
        }
    }
}
