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
//! Integration tests for assertion endpoints.
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
    use chrono::Utc;
    use tower::ServiceExt;

    use elephant_storage::MockAssertionRepository;
    use elephant_types::ids::TableName;
    use elephant_types::records::assertion::{
        Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
    };
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc_revision::DocRevision;
    use elephant_types::{Link, RecordId};

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::assertions::{assertion_routes, AssertionSearchResponse, AssertionsState};

    async fn setup_test_app() -> Router {
        let authz = Arc::new(AuthzService::new());
        let assertion_repo = Arc::new(MockAssertionRepository::empty());
        let state = Arc::new(AssertionsState::new_with_repo(assertion_repo, authz));
        assertion_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:assertion".to_string(), "write:assertion".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_assertion(subject_text: &str, predicate: &str) -> Assertion {
        let id = RecordId::new_v4(TableName::Assertion);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let revision_id = RecordId::new_v4(TableName::DocRevision);

        Assertion {
            base,
            subject_text: subject_text.to_string(),
            predicate_raw: predicate.to_string(),
            predicate_id: None,
            object_text: Some("Test Object".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id),
                node: None,
                start: 0,
                end: 50,
                quote_hash: "hash123".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
            resolution_status: ResolutionStatus::Pending,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.9,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_assertion_returns_201() {
        let app = setup_test_app().await;
        let assertion = create_test_assertion("John Doe", "works_at");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_get_assertion_returns_assertion() {
        let app = setup_test_app().await;
        let assertion = create_test_assertion("Jane Smith", "lives_in");
        let assertion_id = assertion.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/assertions/{assertion_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_assertion_returns_404() {
        let app = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/assertions/assertion:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_resolution_status() {
        let app = setup_test_app().await;
        let assertion = create_test_assertion("Bob Wilson", "member_of");
        let assertion_id = assertion.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Update resolution
        let update_req = serde_json::json!({
            "status": "resolved",
            "rationale": "Manually resolved by operator"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/assertions/{assertion_id}/resolution"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&update_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_then_restore_assertion() {
        let app = setup_test_app().await;
        let assertion = create_test_assertion("Test Fixture", "mentions");
        let assertion_id = assertion.base.id.to_string();
        let send = |method: &str, uri: String, body: Body| {
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(body)
                .unwrap()
        };

        let created = app
            .clone()
            .oneshot(send(
                "POST",
                "/v1/assertions".to_string(),
                Body::from(serde_json::to_string(&assertion).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let deleted = app
            .clone()
            .oneshot(send(
                "DELETE",
                format!("/v1/assertions/{assertion_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        let after_delete = app
            .clone()
            .oneshot(send(
                "GET",
                format!("/v1/assertions/{assertion_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);

        let deleted_again = app
            .clone()
            .oneshot(send(
                "DELETE",
                format!("/v1/assertions/{assertion_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(deleted_again.status(), StatusCode::NOT_FOUND);

        let restored = app
            .clone()
            .oneshot(send(
                "POST",
                format!("/v1/assertions/{assertion_id}/restore"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(restored.status(), StatusCode::OK);

        let after_restore = app
            .oneshot(send(
                "GET",
                format!("/v1/assertions/{assertion_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(after_restore.status(), StatusCode::OK);
    }

    async fn mock_write_only_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "write-only".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["write:assertion".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn test_restore_requires_read_authorization() {
        let authz = Arc::new(AuthzService::new());
        let repo = Arc::new(MockAssertionRepository::empty());
        let state = Arc::new(AssertionsState::new_with_repo(repo, authz));
        let full = assertion_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state.clone());
        let write_only = assertion_routes()
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

        let deleted = create_test_assertion("Deleted Row", "mentions");
        let deleted_id = deleted.base.id.to_string();
        let active = create_test_assertion("Active Row", "mentions");
        let active_id = active.base.id.to_string();
        for assertion in [&deleted, &active] {
            let created = full
                .clone()
                .oneshot(send(
                    "POST",
                    "/v1/assertions".to_string(),
                    Body::from(serde_json::to_string(assertion).unwrap()),
                ))
                .await
                .unwrap();
            assert_eq!(created.status(), StatusCode::CREATED);
        }
        let removed = full
            .clone()
            .oneshot(send(
                "DELETE",
                format!("/v1/assertions/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(removed.status(), StatusCode::NO_CONTENT);

        // Write scope alone must neither restore nor disclose, deleted or active.
        for id in [&deleted_id, &active_id] {
            let response = write_only
                .clone()
                .oneshot(send(
                    "POST",
                    format!("/v1/assertions/{id}/restore"),
                    Body::empty(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let text = String::from_utf8_lossy(&body);
            assert!(!text.contains("Deleted Row") && !text.contains("Active Row"));
        }

        // No side effect: the deleted row is still deleted.
        let still_deleted = full
            .oneshot(send(
                "GET",
                format!("/v1/assertions/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(still_deleted.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_resolution_requires_read_authorization() {
        let authz = Arc::new(AuthzService::new());
        let repo = Arc::new(MockAssertionRepository::empty());
        let state = Arc::new(AssertionsState::new_with_repo(repo, authz));
        let full = assertion_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state.clone());
        let write_only = assertion_routes()
            .layer(middleware::from_fn(mock_write_only_auth))
            .with_state(state);
        let assertion = create_test_assertion("Private Row", "mentions");
        let id = assertion.base.id.to_string();
        let created = full
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let update = serde_json::json!({"status": "resolved", "rationale": "write-only"});
        let response = write_only
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/assertions/{id}/resolution"))
                    .header("content-type", "application/json")
                    .body(Body::from(update.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("Private Row"));

        // No side effect: the stored status is unchanged.
        let stored = full
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/assertions/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stored.status(), StatusCode::OK);
        let body = axum::body::to_bytes(stored.into_body(), usize::MAX)
            .await
            .unwrap();
        let stored: Assertion = serde_json::from_slice(&body).unwrap();
        assert_eq!(stored.resolution_status, ResolutionStatus::Pending);
    }

    #[tokio::test]
    async fn test_search_assertions() {
        let app = setup_test_app().await;

        // Create assertions
        for (subject, predicate) in [("Alice", "knows"), ("Bob", "works_at")] {
            let assertion = create_test_assertion(subject, predicate);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Search
        let search_req = serde_json::json!({
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_search_assertions_filters_by_query() {
        let app = setup_test_app().await;

        for (subject, predicate) in [("Alice", "knows"), ("Bob", "works_at")] {
            let assertion = create_test_assertion(subject, predicate);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "query": "Bob works_at",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.total, 1);
        assert_eq!(decoded.results.len(), 1);
        assert_eq!(decoded.results[0].subject_text, "Bob");
        assert_eq!(decoded.results[0].predicate_raw, "works_at");
    }

    #[tokio::test]
    async fn test_search_assertions_uses_ranked_term_retrieval() {
        let app = setup_test_app().await;

        for (subject, predicate) in [("Acme Corp", "vendor_of"), ("Bob", "works_at")] {
            let assertion = create_test_assertion(subject, predicate);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "query": "vendor supplier contract procurement account relationship alias role",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.total, 1);
        assert_eq!(decoded.results[0].subject_text, "Acme Corp");
        assert_eq!(decoded.results[0].predicate_raw, "vendor_of");
    }

    #[tokio::test]
    async fn test_search_assertions_filters_by_source_id() {
        let app = setup_test_app().await;
        let source_id = RecordId::new_v4(TableName::Source);

        let mut matching = create_test_assertion("Alice", "knows");
        matching.base.provenance.sources.push(source_id.clone());
        let mut unrelated = create_test_assertion("Bob", "knows");
        unrelated
            .base
            .provenance
            .sources
            .push(RecordId::new_v4(TableName::Source));

        for assertion in [matching, unrelated] {
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "source_id": source_id.to_string(),
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.total, 1);
        assert_eq!(decoded.results[0].subject_text, "Alice");
    }

    #[tokio::test]
    async fn test_search_assertions_filters_by_resolved_entity_id() {
        let app = setup_test_app().await;
        let person_id = RecordId::new_v4(TableName::Entity);
        let platform_id = RecordId::new_v4(TableName::Entity);
        let unrelated_id = RecordId::new_v4(TableName::Entity);

        let mut runs_on = create_test_assertion("Discovered service", "runs_on");
        runs_on.subject_entity = Some(Link::new(person_id.clone()));
        runs_on.object_entity = Some(Link::new(platform_id.clone()));
        let mut unrelated = create_test_assertion("Unrelated person", "works_at");
        unrelated.subject_entity = Some(Link::new(unrelated_id));

        for assertion in [runs_on, unrelated] {
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "entity_id": platform_id.to_string(),
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.total, 1);
        assert_eq!(decoded.results[0].subject_text, "Discovered service");
        assert_eq!(
            decoded.results[0].object_entity.as_ref().unwrap().id(),
            &platform_id
        );
    }

    #[tokio::test]
    async fn test_search_assertions_filters_by_subject_and_object_entity() {
        let app = setup_test_app().await;
        let person_id = RecordId::new_v4(TableName::Entity);
        let platform_id = RecordId::new_v4(TableName::Entity);

        let mut runs_on = create_test_assertion("Discovered service", "runs_on");
        runs_on.subject_entity = Some(Link::new(person_id.clone()));
        runs_on.object_entity = Some(Link::new(platform_id.clone()));
        let mut inverse = create_test_assertion("Discovered platform", "hosts");
        inverse.subject_entity = Some(Link::new(platform_id.clone()));
        inverse.object_entity = Some(Link::new(person_id.clone()));

        for assertion in [runs_on, inverse] {
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "subject_entity": person_id.to_string(),
            "object_entity": platform_id.to_string(),
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.total, 1);
        assert_eq!(decoded.results[0].subject_text, "Discovered service");
        assert_eq!(decoded.results[0].predicate_raw, "runs_on");
    }

    #[tokio::test]
    async fn test_search_assertions_preserves_total_beyond_page_size() {
        let app = setup_test_app().await;

        for (subject, predicate) in [("Alice", "knows"), ("Bob", "works_at")] {
            let assertion = create_test_assertion(subject, predicate);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/assertions")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = serde_json::json!({
            "limit": 1
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let decoded: AssertionSearchResponse =
            serde_json::from_slice(&body).expect("decode response");
        assert_eq!(decoded.results.len(), 1);
        assert_eq!(decoded.total, 2);
        assert!(decoded.has_more);
        assert!(decoded.cursor.is_some());
    }

    #[tokio::test]
    async fn test_search_assertions_rejects_invalid_cursor() {
        let app = setup_test_app().await;

        let search_req = serde_json::json!({
            "limit": 1,
            "cursor": "not-an-assertion-id"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_search_assertions_with_status_filter() {
        let app = setup_test_app().await;

        // Create an assertion
        let assertion = create_test_assertion("Charlie", "lives_in");
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&assertion).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Search with status filter
        let search_req = serde_json::json!({
            "status": "pending",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/assertions/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
