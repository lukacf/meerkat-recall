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
//! Integration tests for relationship endpoints.

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

    use elephant_storage::MockRelRepository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::rel::{ClaimStatus, Rel};
    use elephant_types::{Link, RecordId};

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::relationships::{rel_routes, RelationshipsState};

    async fn setup_test_app() -> Router {
        let authz = Arc::new(AuthzService::new());
        let rel_repo = Arc::new(MockRelRepository::empty());
        let state = Arc::new(RelationshipsState::new_with_repo(rel_repo, authz));
        rel_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec![
                "read:relationship".to_string(),
                "write:relationship".to_string(),
                "read:entity".to_string(),
                "write:entity".to_string(),
            ],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_rel(rel_type: &str, subject_id: RecordId, object_id: RecordId) -> Rel {
        let id = RecordId::new_v4(TableName::Rel);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Rel {
            base,
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: rel_type.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.8,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    #[tokio::test]
    async fn test_create_relationship_returns_201() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let rel = create_test_rel("knows", from_id, to_id);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&rel).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        if response.status() != StatusCode::CREATED {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            panic!(
                "expected 201, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }
    }

    #[tokio::test]
    async fn test_delete_relationship_returns_204() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let rel = create_test_rel("works_with", from_id, to_id);
        let rel_id = rel.base.id.to_string();

        // First create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&rel).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then delete
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/relationships/{rel_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_relationship_returns_404() {
        let app = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/relationships/rel:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restore_deleted_relationship() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let rel = create_test_rel("manages", from_id, to_id);
        let rel_id = rel.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&rel).unwrap()))
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
                    .uri(format!("/v1/relationships/{rel_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Restore
        let restore_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/relationships/{rel_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(restore_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upsert_creates_new_relationship() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let rel = create_test_rel("reports_to", from_id, to_id);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&rel).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upsert_updates_existing_relationship() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let mut original = create_test_rel("knows", from_id.clone(), to_id.clone());
        original.confidence = 0.5;
        let original_id = original.base.id.clone();

        // Create the original relationship
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&original).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Upsert with the SAME record ID should update
        let mut updated = create_test_rel("knows", from_id, to_id);
        updated.base.id = original_id.clone();
        updated.confidence = 0.95;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&updated).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Rel = serde_json::from_slice(&body).unwrap();

        assert_eq!(result.base.id, original_id);
        assert!((result.confidence - 0.95).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_batch_relationships() {
        let app = setup_test_app().await;

        let from_id = RecordId::new_v4(TableName::Entity);
        let to_id = RecordId::new_v4(TableName::Entity);

        let rels = vec![
            create_test_rel("knows", from_id.clone(), to_id.clone()),
            create_test_rel("works_with", from_id.clone(), to_id.clone()),
            create_test_rel("manages", from_id, to_id),
        ];

        let batch_req = serde_json::json!({
            "items": rels
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/relationships/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&batch_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn mock_write_only_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "write-only".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["write:relationship".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn test_restore_relationship_requires_read_authorization() {
        let authz = Arc::new(AuthzService::new());
        let state = Arc::new(RelationshipsState::new_with_repo(
            Arc::new(MockRelRepository::empty()),
            authz,
        ));
        let full = rel_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state.clone());
        let write_only = rel_routes()
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

        let deleted = create_test_rel(
            "private_fixture_rel",
            RecordId::new_v4(TableName::Entity),
            RecordId::new_v4(TableName::Entity),
        );
        let deleted_id = deleted.base.id.to_string();
        let active = create_test_rel(
            "private_fixture_rel",
            RecordId::new_v4(TableName::Entity),
            RecordId::new_v4(TableName::Entity),
        );
        let active_id = active.base.id.to_string();
        for record in [
            serde_json::to_string(&deleted).unwrap(),
            serde_json::to_string(&active).unwrap(),
        ] {
            let created = full
                .clone()
                .oneshot(send(
                    "POST",
                    "/v1/relationships".to_string(),
                    Body::from(record),
                ))
                .await
                .unwrap();
            assert!(created.status().is_success());
        }
        let removed = full
            .clone()
            .oneshot(send(
                "DELETE",
                format!("/v1/relationships/{deleted_id}"),
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
                    format!("/v1/relationships/{id}/restore"),
                    Body::empty(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("private_fixture_rel"));
        }

        // No side effect: the deleted row is still deleted.
        let still_deleted = full
            .clone()
            .oneshot(send(
                "GET",
                format!("/v1/relationships/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(still_deleted.status(), StatusCode::NOT_FOUND);

        // Positive control: read and write together restore it.
        let restored = full
            .oneshot(send(
                "POST",
                format!("/v1/relationships/{deleted_id}/restore"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(restored.status(), StatusCode::OK);
    }
}
