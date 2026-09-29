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
//! Integration tests for system endpoints.

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

    use elephant_storage::{
        MockAuditLogRepository, MockEntityRepository, Repository, StorageClient, StorageError,
        StorageResult,
    };
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::{
        AuditAction, AuditLog, Doc, Entity, IdentityCandidate, Rel, Source, SystemEvent,
    };
    use elephant_types::RecordId;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::marker::PhantomData;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::system::{system_routes, SystemState};

    #[derive(Clone, Debug)]
    struct NoopRepo<T>(PhantomData<T>);

    impl<T> Default for NoopRepo<T> {
        fn default() -> Self {
            Self(PhantomData)
        }
    }

    #[async_trait::async_trait]
    impl<T> Repository<T> for NoopRepo<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        async fn create(&self, _record: T) -> StorageResult<T> {
            Err(StorageError::NotFound("NoopRepo".to_string()))
        }

        async fn get(&self, _space_id: &str, _id: &RecordId) -> StorageResult<Option<T>> {
            Ok(None)
        }

        async fn update(&self, _space_id: &str, _id: &RecordId, _record: T) -> StorageResult<T> {
            Err(StorageError::NotFound("NoopRepo".to_string()))
        }

        async fn delete(&self, _space_id: &str, _id: &RecordId) -> StorageResult<()> {
            Err(StorageError::NotFound("NoopRepo".to_string()))
        }

        async fn list(
            &self,
            _space_id: &str,
            _limit: usize,
            _cursor: Option<&RecordId>,
        ) -> StorageResult<Vec<T>> {
            Ok(Vec::new())
        }
    }

    async fn setup_test_app() -> (
        Router,
        Arc<MockEntityRepository>,
        Arc<MockAuditLogRepository>,
    ) {
        let authz = Arc::new(AuthzService::new());
        let entity_repo = Arc::new(MockEntityRepository::empty());
        let audit_repo = Arc::new(MockAuditLogRepository::empty());

        let state = Arc::new(SystemState::new_with_repos(
            None,
            audit_repo.clone(),
            entity_repo.clone(),
            Arc::new(NoopRepo::<Rel>::default()),
            Arc::new(NoopRepo::<SystemEvent>::default()),
            Arc::new(NoopRepo::<Doc>::default()),
            Arc::new(NoopRepo::<Source>::default()),
            Arc::new(NoopRepo::<IdentityCandidate>::default()),
            authz,
        ));
        (
            system_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state),
            entity_repo,
            audit_repo,
        )
    }

    async fn setup_embedded_test_app() -> (Router, Arc<SystemState>) {
        let authz = Arc::new(AuthzService::new());
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SystemState::new(storage, authz));
        (
            system_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state.clone()),
            state,
        )
    }

    async fn setup_embedded_test_app_without_auth() -> (Router, Arc<SystemState>) {
        let authz = Arc::new(AuthzService::new());
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SystemState::new(storage, authz));
        (system_routes().with_state(state.clone()), state)
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:audit".to_string(), "write:entity".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    async fn mock_auth_without_space(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: None,
            scopes: vec!["read:audit".to_string(), "write:entity".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_entity(display_name: &str) -> Entity {
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

        Entity::new(base, EntityType::new("person"), display_name.to_string())
    }

    #[tokio::test]
    async fn test_get_capabilities_shape_matches_spec() {
        let (app, ..) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("policies").is_some());
        assert!(json.get("extensions").is_some());
        assert!(json.get("features").is_some());
    }

    #[tokio::test]
    async fn test_revert_audit_reverts_entity_update() {
        let (app, entity_repo, audit_repo) = setup_test_app().await;

        let original = create_test_entity("Original");
        let entity_id = original.base.id.clone();
        let created = entity_repo
            .create(original.clone())
            .await
            .expect("create entity");

        let mut updated = created.clone();
        updated.display_name = "Updated".to_string();
        updated.base.updated_at = Utc::now();
        let updated = entity_repo
            .update("test_space", &entity_id, updated.clone())
            .await
            .expect("update entity");

        // Create an audit log entry representing the update.
        let audit_id = RecordId::new_v4(TableName::AuditLog);
        let mut audit_base = BaseFields::new(
            audit_id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        audit_base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        audit_base.provenance = Provenance::default();
        audit_base.subjects_state = SubjectsState::Complete;

        let audit = AuditLog {
            base: audit_base,
            action: AuditAction::Update,
            actor: "test-user".to_string(),
            record: created.base.id.clone(),
            before: Some(serde_json::to_value(&original).unwrap()),
            after: Some(serde_json::to_value(&updated).unwrap()),
            timestamp: Utc::now(),
            reason: Some("test".to_string()),
        };
        let created_audit = audit_repo.create(audit).await.expect("create audit");

        // Revert and verify entity returns to original state.
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/audit/revert/{}", created_audit.base.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            panic!(
                "expected 200 OK, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let reverted = entity_repo
            .get("test_space", &entity_id)
            .await
            .expect("get entity")
            .expect("entity exists");
        assert!(
            (reverted.display_name == "Original"),
            "expected display_name 'Original', got '{}'",
            reverted.display_name
        );
    }

    #[tokio::test]
    async fn test_revert_audit_restores_deleted_entity_in_embedded_mode() {
        let (app, state) = setup_embedded_test_app().await;

        let original = create_test_entity("Alice");
        let entity_id = original.base.id.clone();
        let created = state
            .entity_repo
            .create(original.clone())
            .await
            .expect("create entity");
        state
            .entity_repo
            .delete("test_space", &entity_id)
            .await
            .expect("soft delete entity");

        let audit_id = RecordId::new_v4(TableName::AuditLog);
        let mut audit_base = BaseFields::new(
            audit_id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        audit_base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        audit_base.provenance = Provenance::default();
        audit_base.subjects_state = SubjectsState::Complete;

        let audit = AuditLog {
            base: audit_base,
            action: AuditAction::Delete,
            actor: "test-user".to_string(),
            record: created.base.id.clone(),
            before: Some(serde_json::to_value(&original).expect("serialize before")),
            after: Some(serde_json::to_value(&created).expect("serialize after")),
            timestamp: Utc::now(),
            reason: Some("test".to_string()),
        };
        let created_audit = state.audit_repo.create(audit).await.expect("create audit");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/audit/revert/{}", created_audit.base.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("execute request");

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read error body");
            panic!(
                "expected 200 OK, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let restored = state
            .entity_repo
            .get("test_space", &entity_id)
            .await
            .expect("get entity")
            .expect("entity restored");
        assert!(restored.base.deleted_at.is_none());
        assert_eq!(restored.display_name, "Alice");
    }

    #[tokio::test]
    async fn test_revert_audit_rejects_missing_space_id() {
        let authz = Arc::new(AuthzService::new());
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SystemState::new(storage, authz));
        let app = system_routes()
            .layer(middleware::from_fn(mock_auth_without_space))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/audit/revert/audit_log:550e8400-e29b-41d4-a716-446655440000")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            json.get("code").and_then(|value| value.as_str()),
            Some("ACCESS_DENIED")
        );
    }

    #[tokio::test]
    async fn test_snapshot_export_requires_global_principal() {
        let (app, _) = setup_embedded_test_app_without_auth().await;
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/system/snapshot/export")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"path":"/tmp/test-snapshot.json"}"#))
            .unwrap();
        request.extensions_mut().insert(AuthPrincipal {
            subject: "space-admin".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:system".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["message"]
            .as_str()
            .unwrap_or_default()
            .contains("snapshot operations require a global principal"));
    }
}
