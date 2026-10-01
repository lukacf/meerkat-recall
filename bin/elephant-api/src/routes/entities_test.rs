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
//! Integration tests for entity endpoints.

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
    use std::collections::HashSet;
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::{self, Next},
        response::IntoResponse,
        Router,
    };
    use tower::ServiceExt;

    use elephant_storage::{EntityRepository, MockRelRepository, Repository, StorageClient};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::Entity;
    use elephant_types::RecordId;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::entities::{
        entity_routes, AppState as EntitiesState, EntitySearchRequest, EventRepo,
    };

    #[derive(Clone, Debug)]
    struct NoopEventRepo;

    #[async_trait::async_trait]
    impl EventRepo for NoopEventRepo {
        async fn find_by_entity_since(
            &self,
            _space_id: &str,
            _entity_id: &RecordId,
            _since: chrono::DateTime<chrono::Utc>,
            _limit: usize,
        ) -> elephant_storage::StorageResult<Vec<elephant_types::records::SystemEvent>> {
            Ok(Vec::new())
        }
    }

    async fn setup_test_app() -> (Router, Arc<EntityRepository>) {
        let authz = Arc::new(AuthzService::new());
        let entity_repo = Arc::new(EntityRepository::new(
            StorageClient::new_embedded().await.unwrap(),
        ));
        let rel_repo = Arc::new(MockRelRepository::empty());
        let event_repo = Arc::new(NoopEventRepo);
        let state = Arc::new(EntitiesState::new_with_repos(
            entity_repo.clone(),
            rel_repo,
            event_repo,
            authz,
        ));
        let app = entity_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state);
        (app, entity_repo)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec![
                "read:entity".to_string(),
                "write:entity".to_string(),
                "read:relationship".to_string(),
                "write:relationship".to_string(),
                "read:event".to_string(),
                "read:doc:metadata".to_string(),
                "write:doc".to_string(),
                "read:fact".to_string(),
                "write:fact".to_string(),
                "read:source".to_string(),
            ],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_entity(name: &str) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Entity::new(base, EntityType::new("person"), name.to_string())
    }

    async fn setup_security_test_app() -> (
        Router,
        Arc<EntityRepository>,
        Arc<AuthzService>,
        AuthPrincipal,
    ) {
        let authz = Arc::new(AuthzService::new());
        let principal = AuthPrincipal {
            subject: "security-test".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:entity".to_string(), "write:entity".to_string()],
            clearances: ["default", "other"]
                .into_iter()
                .map(|policy_id| elephant_policy::Clearance {
                    policy_id: policy_id.to_string(),
                    max_level: 10,
                    labels: HashSet::from(["restricted".to_string()]),
                    handling_allow: HashSet::from(["no_export".to_string()]),
                })
                .collect(),
            ..AuthPrincipal::default()
        };
        let repo = Arc::new(EntityRepository::new(
            StorageClient::new_embedded().await.unwrap(),
        ));
        let state = Arc::new(EntitiesState::new_with_repos(
            repo.clone(),
            Arc::new(MockRelRepository::empty()),
            Arc::new(NoopEventRepo),
            authz.clone(),
        ));
        let app = entity_routes()
            .layer(axum::Extension(principal.clone()))
            .with_state(state);
        (app, repo, authz, principal)
    }

    async fn send_entity_write(
        app: Router,
        route: &str,
        entity: &Entity,
    ) -> axum::response::Response {
        let (method, path, body) = match route {
            "put" => (
                "PUT",
                format!("/v1/entities/{}", entity.base.id),
                serde_json::to_value(entity).unwrap(),
            ),
            "upsert" => (
                "POST",
                "/v1/entities/upsert".to_string(),
                serde_json::to_value(entity).unwrap(),
            ),
            "batch" => (
                "POST",
                "/v1/entities/batch".to_string(),
                serde_json::json!({"items": [entity]}),
            ),
            _ => panic!("unknown test route"),
        };
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_entity_writes_cannot_change_security_even_with_both_clearances() {
        for route in ["put", "upsert", "batch"] {
            for change in ["level", "policy_id", "labels", "handling"] {
                let (app, repo, authz, principal) = setup_security_test_app().await;
                let mut original = create_test_entity("Protected");
                original.base.security = Security {
                    policy_id: "default".to_string(),
                    level: 5,
                    labels: vec!["restricted".to_string()],
                    handling: vec!["no_export".to_string()],
                };
                repo.create(original.clone()).await.unwrap();
                let mut proposed = original.clone();
                match change {
                    "level" => proposed.base.security.level = 0,
                    "policy_id" => proposed.base.security.policy_id = "other".to_string(),
                    "labels" => proposed.base.security.labels.clear(),
                    "handling" => proposed.base.security.handling.clear(),
                    _ => unreachable!(),
                }
                proposed.display_name = "Must not persist".to_string();
                // Both records independently pass ordinary read/write policy checks.
                for record in [&original, &proposed] {
                    authz
                        .authorize_record_read(&principal, record, "read:entity")
                        .unwrap();
                    authz
                        .authorize_record_write(&principal, record, "write:entity")
                        .unwrap();
                }
                let response = send_entity_write(app, route, &proposed).await;
                let status = response.status();
                let body: serde_json::Value = serde_json::from_slice(
                    &axum::body::to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap(),
                )
                .unwrap();
                if route == "batch" {
                    assert_eq!(status, StatusCode::OK);
                    assert_eq!(body["succeeded"].as_array().unwrap().len(), 0);
                    assert_eq!(body["failed"].as_array().unwrap().len(), 1);
                    assert!(body["failed"][0]["error"]
                        .as_str()
                        .unwrap()
                        .contains("cannot change security"));
                } else {
                    assert_eq!(status, StatusCode::FORBIDDEN, "{route}/{change}: {body}");
                    assert!(body["message"]
                        .as_str()
                        .unwrap()
                        .contains("cannot change security"));
                }
                let stored = repo
                    .get("test_space", &original.base.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(stored.base.security, original.base.security);
                assert_eq!(stored.display_name, original.display_name);
            }
        }
    }

    #[tokio::test]
    async fn test_entity_writes_preserve_security_on_content_edits() {
        for route in ["put", "upsert", "batch"] {
            let (app, repo, _, _) = setup_security_test_app().await;
            let mut original = create_test_entity("Before");
            original.base.security.level = 5;
            repo.create(original.clone()).await.unwrap();
            let mut proposed = original.clone();
            proposed.display_name = "After".to_string();
            let response = send_entity_write(app, route, &proposed).await;
            assert_eq!(response.status(), StatusCode::OK);
            let stored = repo
                .get("test_space", &original.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.display_name, "After");
            assert_eq!(stored.base.security, original.base.security);
        }
    }

    #[tokio::test]
    async fn test_entity_upsert_and_batch_refuse_reclassification_without_restoring_deleted_rows() {
        for route in ["upsert", "batch"] {
            let (app, repo, _, _) = setup_security_test_app().await;
            let mut original = create_test_entity("Deleted protected entity");
            original.base.security.level = 5;
            repo.create(original.clone()).await.unwrap();
            repo.delete("test_space", &original.base.id).await.unwrap();
            let mut proposed = original.clone();
            proposed.base.security.level = 0;
            send_entity_write(app.clone(), route, &proposed).await;
            let stored = repo
                .get_with_deleted("test_space", &original.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.base.security, original.base.security);
            assert!(stored.base.deleted_at.is_some());
            // Keeping the classification permits the intended restore plus content update.
            original.display_name = "Restored".to_string();
            assert_eq!(
                send_entity_write(app, route, &original).await.status(),
                StatusCode::OK
            );
            let stored = repo
                .get("test_space", &original.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.display_name, "Restored");
            assert_eq!(stored.base.security, original.base.security);
        }
    }

    #[tokio::test]
    async fn test_entity_upsert_authorizes_existing_record() {
        let (app, repo, _, _) = setup_security_test_app().await;
        let mut original = create_test_entity("Above caller clearance");
        original.base.security.level = 20;
        repo.create(original.clone()).await.unwrap();
        let mut proposed = original.clone();
        proposed.base.security.level = 0;
        let response = send_entity_write(app, "upsert", &proposed).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(body["message"].as_str().unwrap().contains("exceeds max"));
        assert_eq!(
            repo.get("test_space", &original.base.id)
                .await
                .unwrap()
                .unwrap()
                .base
                .security,
            original.base.security
        );
    }

    #[tokio::test]
    async fn test_create_entity_returns_201() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Alice");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
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
    async fn test_get_entity_returns_entity() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Bob");
        let entity_id = entity.base.id.to_string();

        // First create the entity
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        // Then get it
        let get_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_entity_returns_404() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/entities/entity:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_entity_returns_204() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Carol");
        let entity_id = entity.base.id.to_string();

        // First create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then delete
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_search_entities_returns_results() {
        let (app, _) = setup_test_app().await;

        // Create a few entities
        for name in ["Dave", "Eve", "Frank"] {
            let entity = create_test_entity(name);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/entities")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&entity).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Search
        let search_req = EntitySearchRequest {
            query: None,
            space_id: Some("test_space".to_string()),
            entity_type: None,
            include_deleted: false,
            limit: 50,
            cursor: None,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_search_entities_wildcard_lists_entities() {
        let (app, _) = setup_test_app().await;

        for name in ["Tom", "Lena"] {
            let entity = create_test_entity(name);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/entities")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&entity).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let search_req = EntitySearchRequest {
            query: Some("*".to_string()),
            space_id: Some("test_space".to_string()),
            entity_type: None,
            include_deleted: false,
            limit: 50,
            cursor: None,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["results"].as_array().unwrap().len(), 2);
        assert_eq!(json["total"].as_u64().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_search_entities_includes_authorized_total() {
        let (app, entity_repo) = setup_test_app().await;

        // Insert one readable entity (no labels) and one unreadable (requires label clearance).
        let readable = create_test_entity("Readable");
        let mut unreadable = create_test_entity("Unreadable");
        unreadable.base.security.labels = vec!["comp:secret".to_string()];

        // Insert directly (bypasses API auth) so the unreadable record exists in storage.
        entity_repo.create(readable).await.expect("create readable");
        entity_repo
            .create(unreadable)
            .await
            .expect("create unreadable");

        let req = EntitySearchRequest {
            query: None,
            space_id: None,
            entity_type: None,
            include_deleted: false,
            limit: 50,
            cursor: None,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["results"].as_array().unwrap().len(), 1);
        assert_eq!(json["total"].as_u64().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_create_entity_conflict_returns_409() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Dupe");

        let resp1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp1.status(), StatusCode::CREATED);

        let resp2 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp2.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_deleted_entity_include_deleted_true_returns_200() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Deleted");
        let entity_id = entity.base.id.to_string();

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/entities/{entity_id}?include_deleted=true"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_restore_deleted_entity() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("Grace");
        let entity_id = entity.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
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
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify it's gone
        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);

        // Restore
        let restore_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/entities/{entity_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(restore_response.status(), StatusCode::OK);

        // Verify it's back
        let final_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/entities/{entity_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(final_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_entity_context_returns_context() {
        let (app, _) = setup_test_app().await;
        let entity = create_test_entity("ContextEntity");
        let entity_id = entity.base.id.to_string();

        // Create the entity
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get context
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/entities/{entity_id}/context"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify response structure
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let context: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify expected fields exist
        assert!(context.get("entity").is_some());
        assert!(context.get("outgoing_relations").is_some());
        assert!(context.get("incoming_relations").is_some());
        assert!(context.get("recent_events").is_some());
    }

    #[tokio::test]
    async fn test_get_entity_context_returns_404_for_nonexistent() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/entities/entity:00000000-0000-0000-0000-000000000000/context")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Regression test: verify that `space_id` in request body is overridden by principal's `space_id`.
    /// This prevents records from being created in a space the principal cannot access.
    #[tokio::test]
    async fn test_create_entity_overrides_space_id_with_principal_space() {
        let (app, _) = setup_test_app().await;

        // Create an entity with a DIFFERENT space_id than the principal's ("test_space")
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(
            id,
            "wrong_space".to_string(), // Different from principal's "test_space"
            "test_agent".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let entity = Entity::new(
            base,
            EntityType::new("person"),
            "SpaceTestEntity".to_string(),
        );

        // Create the entity
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&entity).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // Parse the response to verify the space_id was overridden
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: Entity = serde_json::from_slice(&body).unwrap();

        // The space_id should be the principal's space_id, not the one we provided
        assert_eq!(
            created.base.space_id, "test_space",
            "space_id should be overridden to principal's space"
        );

        // Verify the entity can be found via search (proving it's in the correct space)
        let search_req = serde_json::json!({
            "space_id": "test_space",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/entities/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let search_result: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Should find at least one result
        assert!(
            search_result["total"].as_u64().unwrap() >= 1,
            "Should find the created entity via search"
        );
    }
}
