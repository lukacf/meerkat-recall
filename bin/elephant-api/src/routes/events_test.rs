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
//! Integration tests for event streaming endpoints.

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

    use elephant_outbox::StreamCursor;
    use elephant_storage::{MockSystemEventRepository, Repository};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::SystemEvent;
    use elephant_types::RecordId;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::events::{events_routes, EventsState, LatestSeqResponse, StreamResponse};

    async fn setup_test_app() -> (Router, Arc<MockSystemEventRepository>) {
        let authz = Arc::new(AuthzService::new());
        let repo = Arc::new(MockSystemEventRepository::empty());
        let state = Arc::new(EventsState::new_with_repo(repo.clone(), authz));
        (
            events_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state),
            repo,
        )
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;

        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:event".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn make_event(space_id: &str, seq: i64, event_type: &str) -> SystemEvent {
        let id = RecordId::new_v4(TableName::SystemEvent);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        SystemEvent {
            base,
            seq,
            event_type: event_type.to_string(),
            payload: serde_json::json!({ "seq": seq }),
        }
    }

    async fn create_test_events(repo: &MockSystemEventRepository, space_id: &str, count: usize) {
        for i in 0..count {
            let seq = i64::try_from(i + 1).unwrap_or(0);
            let event = make_event(space_id, seq, &format!("test.event.{i}"));
            repo.create(event).await.expect("create event");
        }
    }

    #[tokio::test]
    async fn test_stream_events_returns_200() {
        let (app, repo) = setup_test_app().await;

        // Create some events
        create_test_events(&repo, "test_space", 3).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stream_events_returns_events() {
        let (app, repo) = setup_test_app().await;

        create_test_events(&repo, "test_space", 5).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stream_response: StreamResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(stream_response.events.len(), 5);
        assert!(!stream_response.has_more);
    }

    #[tokio::test]
    async fn test_stream_events_respects_limit() {
        let (app, repo) = setup_test_app().await;

        create_test_events(&repo, "test_space", 10).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stream_response: StreamResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(stream_response.events.len(), 3);
        assert!(stream_response.has_more);
        assert!(stream_response.next_cursor.is_some());
    }

    #[tokio::test]
    async fn test_stream_events_with_cursor() {
        let (app, repo) = setup_test_app().await;

        create_test_events(&repo, "test_space", 6).await;

        // First request - get first 3
        let response1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX)
            .await
            .unwrap();
        let response1: StreamResponse = serde_json::from_slice(&body1).unwrap();

        assert_eq!(response1.events.len(), 3);
        let cursor = response1.next_cursor.expect("should have cursor");

        // Second request - use cursor (URL-encode special chars)
        let encoded_cursor = cursor
            .replace('+', "%2B")
            .replace('/', "%2F")
            .replace('=', "%3D");
        let response2 = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/events/stream?limit=10&cursor={encoded_cursor}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
            .await
            .unwrap();
        let response2: StreamResponse = serde_json::from_slice(&body2).unwrap();

        // Should get remaining events (seq > 3)
        assert_eq!(response2.events.len(), 3);
        assert!(!response2.has_more);
    }

    #[tokio::test]
    async fn test_stream_events_filters_by_space() {
        let (app, repo) = setup_test_app().await;

        create_test_events(&repo, "test_space", 3).await;
        create_test_events(&repo, "other_space", 2).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stream_response: StreamResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(stream_response.events.len(), 3);
    }

    #[tokio::test]
    async fn test_stream_events_invalid_cursor() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream?cursor=invalid_cursor")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_stream_events_cursor_space_mismatch() {
        let (app, _) = setup_test_app().await;

        // Create a cursor for a different space than the principal.
        let cursor = StreamCursor::new("other_space".to_string(), 0);
        let encoded = cursor.encode().expect("encode");

        let encoded_cursor = encoded
            .replace('+', "%2B")
            .replace('/', "%2F")
            .replace('=', "%3D");
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/events/stream?cursor={encoded_cursor}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_latest_seq_returns_200() {
        let (app, repo) = setup_test_app().await;

        create_test_events(&repo, "test_space", 5).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/latest?space_id=test_space")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let latest: LatestSeqResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(latest.space_id, "test_space");
        assert_eq!(latest.seq, 5);
    }

    #[tokio::test]
    async fn test_get_latest_seq_empty_space() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/latest?space_id=test_space")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let latest: LatestSeqResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(latest.seq, 0);
    }

    #[tokio::test]
    async fn test_stream_empty_space() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/events/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stream_response: StreamResponse = serde_json::from_slice(&body).unwrap();

        assert!(stream_response.events.is_empty());
        assert!(!stream_response.has_more);
    }
}
