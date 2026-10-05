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
//! Integration tests for timeline/event endpoints.
//! Tests written FIRST (TDD) to verify endpoints work end-to-end.

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
    use chrono::{Duration, Utc};
    use tower::ServiceExt;
    use uuid::Uuid;

    use async_trait::async_trait;
    use elephant_storage::{
        MockEventRepository, PaginatedResult, SearchCriteria, StorageError, StorageResult,
    };
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::{Event, EventStatus, TimePrecision};
    use elephant_types::Link;
    use elephant_types::RecordId;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::timeline::{timeline_routes, TimelineEventRepo, TimelineState};

    async fn setup_test_app() -> Router {
        let event_repo: Arc<dyn TimelineEventRepo> = Arc::new(MockEventRepository::empty());
        setup_test_app_with_repo(event_repo).await
    }

    async fn setup_test_app_with_repo(event_repo: Arc<dyn TimelineEventRepo>) -> Router {
        let authz = Arc::new(AuthzService::new());
        let state = Arc::new(TimelineState::new_with_repo(event_repo, authz));
        timeline_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state)
    }

    async fn setup_failing_test_app() -> Router {
        let event_repo: Arc<dyn TimelineEventRepo> = Arc::new(FailingTimelineEventRepo);
        setup_test_app_with_repo(event_repo).await
    }

    #[derive(Debug)]
    struct FailingTimelineEventRepo;

    #[async_trait]
    impl TimelineEventRepo for FailingTimelineEventRepo {
        async fn restore_guarded(
            &self,
            _space_id: &str,
            _id: &elephant_types::RecordId,
            _authorized: &Event,
        ) -> StorageResult<Event> {
            Err(StorageError::QueryError(
                "injected restore failure".to_string(),
            ))
        }

        async fn create(&self, _record: Event) -> StorageResult<Event> {
            Err(StorageError::QueryError(
                "injected create failure".to_string(),
            ))
        }

        async fn get(&self, _space_id: &str, _id: &RecordId) -> StorageResult<Option<Event>> {
            Err(StorageError::QueryError("injected get failure".to_string()))
        }

        async fn update(
            &self,
            _space_id: &str,
            _id: &RecordId,
            _record: Event,
        ) -> StorageResult<Event> {
            Err(StorageError::QueryError(
                "injected update failure".to_string(),
            ))
        }

        async fn delete(&self, _space_id: &str, _id: &RecordId) -> StorageResult<()> {
            Err(StorageError::QueryError(
                "injected delete failure".to_string(),
            ))
        }

        async fn search(
            &self,
            _criteria: &SearchCriteria,
        ) -> StorageResult<PaginatedResult<Event>> {
            Err(StorageError::QueryError(
                "injected search failure".to_string(),
            ))
        }

        async fn get_recent(&self, _space_id: &str, _limit: usize) -> StorageResult<Vec<Event>> {
            Err(StorageError::QueryError(
                "injected recent failure".to_string(),
            ))
        }

        async fn get_upcoming(&self, _space_id: &str, _days: u32) -> StorageResult<Vec<Event>> {
            Err(StorageError::QueryError(
                "injected upcoming failure".to_string(),
            ))
        }

        async fn get_with_deleted(
            &self,
            _space_id: &str,
            _id: &RecordId,
        ) -> StorageResult<Option<Event>> {
            Err(StorageError::QueryError(
                "injected get_with_deleted failure".to_string(),
            ))
        }

        async fn restore(&self, _space_id: &str, _id: &RecordId) -> StorageResult<Event> {
            Err(StorageError::QueryError(
                "injected restore failure".to_string(),
            ))
        }
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:event".to_string(), "write:event".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_event(title: &str) -> Event {
        let id = RecordId::new_v4(TableName::Event);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let assertion_ref = Link::new(RecordId::new_v4(TableName::Assertion));
        let mut event =
            Event::new(base, title, vec![assertion_ref], Vec::new()).expect("valid event");
        event.status = EventStatus::Approved;
        event.time_start = Some(Utc::now());
        event.time_end = Some(Utc::now() + Duration::hours(1));
        event.time_precision = TimePrecision::Hour;
        event.confidence = 0.9;
        event.time_confidence = 0.9;
        event.metadata = serde_json::json!({
            "idempotency_key": format!("idem_{}", Uuid::new_v4()),
        });
        event
    }

    #[tokio::test]
    async fn test_create_timeline_event_returns_201() {
        let app = setup_test_app().await;
        let event = create_test_event("Team Meeting");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_get_timeline_event_returns_event() {
        let app = setup_test_app().await;
        let event = create_test_event("Project Review");
        let event_id = event.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/timeline/{event_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_timeline_event_returns_204() {
        let app = setup_test_app().await;
        let event = create_test_event("Old Event");
        let event_id = event.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/timeline/{event_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_restore_deleted_timeline_event() {
        let app = setup_test_app().await;
        let event = create_test_event("Restored Event");
        let event_id = event.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/timeline/{event_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Restore
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/timeline/{event_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_search_timeline_returns_events() {
        let app = setup_test_app().await;

        // Create events
        for title in ["Meeting 1", "Meeting 2", "Meeting 3"] {
            let event = create_test_event(title);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Search
        let search_req = serde_json::json!({
            "space_id": "test_space",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_search_timeline_filters_by_entity_ids_and_mode() {
        let app = setup_test_app().await;

        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        // Create three events: A-only, B-only, and A+B.
        let mut event_a = create_test_event("Event A");
        event_a.base.subjects = vec![Link::new(entity_a.clone())];
        let mut event_b = create_test_event("Event B");
        event_b.base.subjects = vec![Link::new(entity_b.clone())];
        let mut event_ab = create_test_event("Event AB");
        event_ab.base.subjects = vec![Link::new(entity_a.clone()), Link::new(entity_b.clone())];

        for event in [&event_a, &event_b, &event_ab] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        // mode=all should return only the event that contains BOTH entities.
        let search_all = serde_json::json!({
            "entity_ids": [entity_a.to_string(), entity_b.to_string()],
            "mode": "all",
            "limit": 50
        });
        let resp_all = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_all).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_all.status(), StatusCode::OK);
        let body_all = axum::body::to_bytes(resp_all.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_all: serde_json::Value = serde_json::from_slice(&body_all).unwrap();
        let titles_all: Vec<String> = json_all["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["title"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(titles_all, vec!["Event AB".to_string()]);

        // mode=any should return all events that contain either entity.
        let search_any = serde_json::json!({
            "entity_ids": [entity_a.to_string(), entity_b.to_string()],
            "mode": "any",
            "limit": 50
        });
        let resp_any = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_any).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp_any.status(), StatusCode::OK);
        let body_any = axum::body::to_bytes(resp_any.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_any: serde_json::Value = serde_json::from_slice(&body_any).unwrap();
        let titles_any: std::collections::HashSet<String> = json_any["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["title"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            titles_any,
            ["Event A", "Event B", "Event AB"]
                .into_iter()
                .map(String::from)
                .collect()
        );
    }

    #[tokio::test]
    async fn test_search_timeline_excludes_candidates_by_default_and_includes_on_opt_in() {
        let app = setup_test_app().await;

        let mut approved = create_test_event("Search Approved");
        approved.status = EventStatus::Approved;

        let mut candidate = create_test_event("Search Candidate");
        candidate.status = EventStatus::Candidate;

        for event in [&approved, &candidate] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        let default_search_req = serde_json::json!({ "limit": 50 });
        let default_search = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&default_search_req).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(default_search.status(), StatusCode::OK);
        let default_body = axum::body::to_bytes(default_search.into_body(), usize::MAX)
            .await
            .unwrap();
        let default_json: serde_json::Value = serde_json::from_slice(&default_body).unwrap();
        let default_titles: std::collections::HashSet<String> = default_json["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();
        assert!(default_titles.contains("Search Approved"));
        assert!(!default_titles.contains("Search Candidate"));

        let include_search_req = serde_json::json!({
            "limit": 50,
            "include_candidates": true
        });
        let include_search = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&include_search_req).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(include_search.status(), StatusCode::OK);
        let include_body = axum::body::to_bytes(include_search.into_body(), usize::MAX)
            .await
            .unwrap();
        let include_json: serde_json::Value = serde_json::from_slice(&include_body).unwrap();
        let include_titles: std::collections::HashSet<String> = include_json["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();
        assert!(include_titles.contains("Search Approved"));
        assert!(include_titles.contains("Search Candidate"));
    }

    #[tokio::test]
    async fn test_get_recent_timeline() {
        let app = setup_test_app().await;

        // Create events
        for title in ["Recent 1", "Recent 2"] {
            let event = create_test_event(title);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Get recent
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/recent?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_batch_timeline_events() {
        let app = setup_test_app().await;

        let events = vec![
            create_test_event("Batch Event 1"),
            create_test_event("Batch Event 2"),
        ];

        let batch_req = serde_json::json!({
            "items": events
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&batch_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_update_timeline_event() {
        let app = setup_test_app().await;
        let event = create_test_event("Original Title");
        let event_id = event.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Update
        let mut updated_event = event.clone();
        updated_event.title = "Updated Title".to_string();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/timeline/{event_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&updated_event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    fn create_future_event(title: &str, days_ahead: i64) -> Event {
        let id = RecordId::new_v4(TableName::Event);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let assertion_ref = Link::new(RecordId::new_v4(TableName::Assertion));
        let mut event =
            Event::new(base, title, vec![assertion_ref], Vec::new()).expect("valid event");
        event.status = EventStatus::Approved;
        event.time_start = Some(Utc::now() + Duration::days(days_ahead));
        event.time_end = event.time_start.map(|start| start + Duration::hours(1));
        event.time_precision = TimePrecision::Hour;
        event.confidence = 0.9;
        event.time_confidence = 0.9;
        event.metadata = serde_json::json!({
            "idempotency_key": format!("idem_{}", Uuid::new_v4()),
        });
        event
    }

    #[tokio::test]
    async fn test_get_upcoming_timeline() {
        let app = setup_test_app().await;

        // Create future events (upcoming)
        for (title, days) in [("Future Event 1", 1), ("Future Event 2", 7)] {
            let event = create_future_event(title, days);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Get upcoming
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/upcoming?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify response is an array of events
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

        // Should have the 2 future events we created
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_upcoming_timeline_excludes_past_events() {
        let app = setup_test_app().await;

        // Create a past event (using current time, which becomes past immediately)
        let past_event = create_test_event("Past Event");
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&past_event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Create a future event
        let future_event = create_future_event("Future Event", 7);
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&future_event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get upcoming - should only return future event
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/upcoming?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

        assert_eq!(events.len(), 1);
        let titles: std::collections::HashSet<String> = events
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();
        assert!(!titles.contains("Past Event"));
        assert!(titles.contains("Future Event"));
    }

    #[tokio::test]
    async fn test_upcoming_timeline_excludes_candidates_by_default_and_includes_on_opt_in() {
        let app = setup_test_app().await;

        let mut approved_future = create_future_event("Upcoming Approved", 3);
        approved_future.status = EventStatus::Approved;

        let mut candidate_future = create_future_event("Upcoming Candidate", 4);
        candidate_future.status = EventStatus::Candidate;

        for event in [&approved_future, &candidate_future] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let default_upcoming = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/upcoming?days=30")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(default_upcoming.status(), StatusCode::OK);
        let default_body = axum::body::to_bytes(default_upcoming.into_body(), usize::MAX)
            .await
            .unwrap();
        let default_events: Vec<serde_json::Value> = serde_json::from_slice(&default_body).unwrap();
        let default_titles: std::collections::HashSet<String> = default_events
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();
        assert!(default_titles.contains("Upcoming Approved"));
        assert!(!default_titles.contains("Upcoming Candidate"));

        let include_upcoming = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/upcoming?days=30&include_candidates=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(include_upcoming.status(), StatusCode::OK);
        let include_body = axum::body::to_bytes(include_upcoming.into_body(), usize::MAX)
            .await
            .unwrap();
        let include_events: Vec<serde_json::Value> = serde_json::from_slice(&include_body).unwrap();
        let include_titles: std::collections::HashSet<String> = include_events
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();
        assert!(include_titles.contains("Upcoming Approved"));
        assert!(include_titles.contains("Upcoming Candidate"));
    }

    #[tokio::test]
    async fn phase1_red_contract_002_event_payload_to_timeline_returns_event_shape() {
        let app = setup_test_app().await;

        let mut event_payload = create_test_event("Domain Event Title");
        event_payload.summary = Some("Event summary".to_string());
        event_payload.status = EventStatus::Approved;
        event_payload.time_precision = TimePrecision::Hour;
        event_payload.metadata = serde_json::json!({
            "phase": "1"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["title"], "Domain Event Title");
        assert_eq!(json["status"], "approved");
        assert_eq!(json["time_precision"], "hour");
    }

    #[tokio::test]
    async fn phase1_red_contract_004_recent_excludes_candidates_by_default() {
        let app = setup_test_app().await;

        let mut approved = create_test_event("Approved Timeline Item");
        approved.status = EventStatus::Approved;

        let mut candidate = create_test_event("Candidate Timeline Item");
        candidate.status = EventStatus::Candidate;

        for event in [&approved, &candidate] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/timeline")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(event).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let default_recent = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/recent?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(default_recent.status(), StatusCode::OK);

        let default_body = axum::body::to_bytes(default_recent.into_body(), usize::MAX)
            .await
            .unwrap();
        let default_events: Vec<serde_json::Value> = serde_json::from_slice(&default_body).unwrap();
        let default_titles: std::collections::HashSet<String> = default_events
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();

        assert!(default_titles.contains("Approved Timeline Item"));
        assert!(!default_titles.contains("Candidate Timeline Item"));

        let include_candidates_recent = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/timeline/recent?limit=10&include_candidates=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(include_candidates_recent.status(), StatusCode::OK);

        let include_body = axum::body::to_bytes(include_candidates_recent.into_body(), usize::MAX)
            .await
            .unwrap();
        let include_events: Vec<serde_json::Value> = serde_json::from_slice(&include_body).unwrap();
        let include_titles: std::collections::HashSet<String> = include_events
            .iter()
            .map(|event| event["title"].as_str().unwrap().to_string())
            .collect();

        assert!(include_titles.contains("Approved Timeline Item"));
        assert!(include_titles.contains("Candidate Timeline Item"));
    }

    #[tokio::test]
    async fn test_create_timeline_returns_500_when_repo_fails() {
        let app = setup_failing_test_app().await;
        let event = create_test_event("Repo Failure Create");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&event).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_search_timeline_returns_500_when_repo_fails() {
        let app = setup_failing_test_app().await;
        let search_req = serde_json::json!({
            "space_id": "test_space",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/timeline/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    async fn mock_write_only_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "write-only".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["write:event".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn test_restore_event_requires_read_authorization() {
        let authz = Arc::new(AuthzService::new());
        let event_repo: Arc<dyn TimelineEventRepo> = Arc::new(MockEventRepository::empty());
        let state = Arc::new(TimelineState::new_with_repo(event_repo, authz));
        let full = timeline_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state.clone());
        let write_only = timeline_routes()
            .layer(middleware::from_fn(mock_write_only_auth))
            .with_state(state);
        let send = |method: &str, uri: String, body: Body| {
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(body)
                .unwrap()
        };

        let deleted = create_test_event("Private Fixture");
        let deleted_id = deleted.base.id.to_string();
        let active = create_test_event("Private Fixture Active");
        let active_id = active.base.id.to_string();
        for record in [
            serde_json::to_string(&deleted).unwrap(),
            serde_json::to_string(&active).unwrap(),
        ] {
            let created = full
                .clone()
                .oneshot(send("POST", "/v1/timeline".to_string(), Body::from(record)))
                .await
                .unwrap();
            assert!(created.status().is_success());
        }
        let removed = full
            .clone()
            .oneshot(send(
                "DELETE",
                format!("/v1/timeline/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert!(removed.status().is_success());

        // Write scope alone must neither restore nor disclose, deleted or active.
        for id in [&deleted_id, &active_id] {
            let response = write_only
                .clone()
                .oneshot(send(
                    "POST",
                    format!("/v1/timeline/{id}/restore"),
                    Body::empty(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("Private Fixture"));
        }

        // No side effect: the deleted row is still deleted.
        let still_deleted = full
            .clone()
            .oneshot(send(
                "GET",
                format!("/v1/timeline/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(still_deleted.status(), StatusCode::NOT_FOUND);

        // Positive control: read and write together restore it.
        let restored = full
            .oneshot(send(
                "POST",
                format!("/v1/timeline/{deleted_id}/restore"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(restored.status(), StatusCode::OK);
    }
}
