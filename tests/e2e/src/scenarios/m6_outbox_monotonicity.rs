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
use futures::future::join_all;
use tower::ServiceExt;

use elephant_api::auth::Principal as AuthPrincipal;
use elephant_api::authz::AuthzService;
use elephant_api::routes::events::{events_routes, EventsState, StreamResponse};
use elephant_outbox::{OutboxPayload, OutboxWriter};

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_m6_outbox_monotonicity() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "Space_X";
    let writer = OutboxWriter::new(harness.client.clone());

    // Simulate concurrent writers.
    let mut tasks = Vec::new();
    for i in 0..10 {
        let writer = writer.clone();
        let event_type = format!("event.{i}");
        tasks.push(tokio::spawn(async move {
            let payload = OutboxPayload::new(space_id.to_string(), event_type, "P_sys".to_string());
            writer.write(payload).await
        }));
    }

    let results = join_all(tasks).await;
    for result in results {
        result??;
    }

    let authz = Arc::new(AuthzService::new());
    let state = Arc::new(EventsState::new(harness.client.clone(), authz));
    let app = events_routes()
        .layer(middleware::from_fn(mock_auth))
        .with_state(state);

    // Fetch first page.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/events/stream?limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let first_page: StreamResponse = serde_json::from_slice(&body)?;
    assert_eq!(first_page.events.len(), 5);
    assert_monotonic(&first_page.events.iter().map(|e| e.seq).collect::<Vec<_>>());

    let cursor = first_page
        .next_cursor
        .expect("expected next_cursor for remaining events");

    // Fetch next page.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/events/stream?limit=10&cursor={cursor}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let second_page: StreamResponse = serde_json::from_slice(&body)?;
    assert!(!second_page.events.is_empty());
    assert_monotonic(&second_page.events.iter().map(|e| e.seq).collect::<Vec<_>>());

    // Ensure no overlap between pages.
    let last_first = first_page.events.last().map(|e| e.seq).unwrap_or(0);
    let first_second = second_page.events.first().map(|e| e.seq).unwrap_or(0);
    assert!(
        first_second > last_first,
        "CRITICAL: cursor resume returned overlapping events"
    );

    Ok(())
}

fn assert_monotonic(seqs: &[i64]) {
    for pair in seqs.windows(2) {
        assert!(
            pair[0] < pair[1],
            "CRITICAL: non-monotonic seq ordering: {} then {}",
            pair[0],
            pair[1]
        );
    }
}

async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
    let mut req = req;
    req.extensions_mut().insert(AuthPrincipal {
        subject: "P_sys".to_string(),
        space_id: Some("Space_X".to_string()),
        scopes: vec!["read:event".to_string()],
        security_level: 10,
        security_labels: vec![],
        ..AuthPrincipal::default()
    });
    next.run(req).await
}
