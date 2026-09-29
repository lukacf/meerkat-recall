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
//! Integration tests for predicate endpoints.
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
    use tower::ServiceExt;

    use elephant_storage::{MockPredicateAliasRepository, MockPredicateRegistryRepository};
    use elephant_types::ids::TableName;
    use elephant_types::RecordId;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::predicates::{predicate_routes, PredicatesState};

    async fn setup_test_app() -> Router {
        let authz = Arc::new(AuthzService::new());
        let predicate_repo = Arc::new(MockPredicateRegistryRepository::empty());
        let alias_repo = Arc::new(MockPredicateAliasRepository::empty());
        let state = Arc::new(PredicatesState::new_with_repos(
            predicate_repo,
            alias_repo,
            authz,
        ));
        predicate_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:predicate".to_string(), "write:predicate".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    async fn test_create_predicate_returns_201() {
        let app = setup_test_app().await;

        let create_req = serde_json::json!({
            "predicate_id": "works_at",
            "description": "Employment relationship"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_list_predicates() {
        let app = setup_test_app().await;

        // Create a predicate
        let create_req = serde_json::json!({
            "predicate_id": "knows",
            "description": "Social relationship"
        });

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // List predicates
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/predicates?limit=50&include_aliases=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_predicate_returns_predicate() {
        let app = setup_test_app().await;

        // Create a predicate
        let create_req = serde_json::json!({
            "predicate_id": "lives_in",
            "description": "Residence relationship"
        });

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Extract ID from response
        let body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let predicate_id = created["id"].as_str().unwrap();

        // Get predicate
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/predicates/{predicate_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_predicate_returns_404() {
        let app = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/predicates/predicate_registry:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_propose_alias_returns_201() {
        let app = setup_test_app().await;

        // Create a predicate
        let create_req = serde_json::json!({
            "predicate_id": "employed_by",
            "description": "Employment relationship"
        });

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Extract ID from response
        let body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let predicate_id = created["id"].as_str().unwrap();

        // Propose an alias
        let alias_req = serde_json::json!({
            "alias": "works_for",
            "alias_type": "raw_string",
            "confidence": 0.95,
            "scope": "global"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/predicates/{predicate_id}/aliases"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&alias_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_update_alias_status() {
        let app = setup_test_app().await;

        // Create a predicate
        let create_req = serde_json::json!({
            "predicate_id": "member_of",
            "description": "Membership relationship"
        });

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let predicate_id = created["id"].as_str().unwrap();

        // Propose an alias
        let alias_req = serde_json::json!({
            "alias": "belongs_to",
            "alias_type": "raw_string",
            "confidence": 0.9,
            "scope": "global"
        });

        let alias_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/predicates/{predicate_id}/aliases"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&alias_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(alias_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let alias_created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alias_id = alias_created["id"].as_str().unwrap();

        // Activate the alias
        let update_req = serde_json::json!({
            "status": "active"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/predicates/{predicate_id}/aliases/{alias_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&update_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_deactivate_hygiene_aliases_deprecates_only_generated_active_aliases() {
        let app = setup_test_app().await;

        let create_req = serde_json::json!({
            "predicate_id": "agreed_to",
            "description": "Agreement relationship"
        });

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let predicate_id = created["id"].as_str().unwrap();

        let hygiene_alias_req = serde_json::json!({
            "alias": "works at",
            "alias_type": "raw_string",
            "confidence": 0.98,
            "scope": "global",
            "source_plugin": "predicate_hygiene"
        });
        let manual_alias_req = serde_json::json!({
            "alias": "agrees with",
            "alias_type": "raw_string",
            "confidence": 0.95,
            "scope": "global",
            "source_plugin": "manual_review"
        });

        let hygiene_alias_id =
            create_and_activate_alias(app.clone(), predicate_id, &hygiene_alias_req).await;
        let _manual_alias_id =
            create_and_activate_alias(app.clone(), predicate_id, &manual_alias_req).await;

        let repair_req = serde_json::json!({
            "dry_run": false
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/predicates/aliases/deactivate-hygiene")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&repair_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let repaired: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(repaired["matched"], 1);
        assert_eq!(repaired["deactivated"], 1);

        let alias_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!(
                        "/v1/predicates/{predicate_id}/aliases/{hygiene_alias_id}"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"status":"deprecated"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(alias_response.status(), StatusCode::OK);
    }

    async fn create_and_activate_alias(
        app: Router,
        predicate_id: &str,
        alias_req: &serde_json::Value,
    ) -> String {
        let alias_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/predicates/{predicate_id}/aliases"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(alias_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(alias_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let alias_created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let alias_id = alias_created["id"].as_str().unwrap().to_string();

        let update_req = serde_json::json!({
            "status": "active"
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/predicates/{predicate_id}/aliases/{alias_id}"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&update_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        alias_id
    }
}
