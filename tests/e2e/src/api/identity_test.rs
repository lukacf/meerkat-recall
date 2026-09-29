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
//! Integration tests for identity endpoints.

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
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use std::sync::{Arc, OnceLock};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::{self, Next},
        response::IntoResponse,
        Router,
    };
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use elephant_migrations::apply_schema;
    use elephant_storage::{
        DocRepository, EntityRepository, Repository, StorageClient, StorageConfig,
    };
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::{Doc, Entity};
    use elephant_types::{Link, RecordId};
    use serde::Deserialize;

    use elephant_api::auth::Principal as AuthPrincipal;
    use elephant_api::authz::AuthzService;
    use elephant_api::routes::identity::{identity_routes, IdentityState};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn set_env_var_scoped(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> EnvVarGuard {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        EnvVarGuard { key, previous }
    }

    #[derive(Debug, Deserialize)]
    struct IdentityKeyRow {
        entity: String,
    }

    async fn setup_test_app() -> (Router, StorageClient) {
        let config = StorageConfig {
            database: format!(
                "identity_api_test_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.expect("create client");
        apply_schema(&client).await.expect("schema");

        let authz = Arc::new(AuthzService::new());
        let state = Arc::new(IdentityState::new(client.clone(), authz));
        (
            identity_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state),
            client,
        )
    }

    async fn setup_embedded_test_app() -> (Router, StorageClient) {
        let client = StorageClient::new_embedded()
            .await
            .expect("create embedded client");

        let authz = Arc::new(AuthzService::new());
        let state = Arc::new(IdentityState::new(client.clone(), authz));
        (
            identity_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state),
            client,
        )
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec![
                "read:entity".to_string(),
                "write:entity".to_string(),
                "read:doc".to_string(),
                "write:doc".to_string(),
                "read:relationship".to_string(),
                "write:relationship".to_string(),
                "read:event".to_string(),
                "write:event".to_string(),
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

    fn create_test_entity_with_metadata(name: &str, metadata: serde_json::Value) -> Entity {
        let mut entity = create_test_entity(name);
        entity.metadata = metadata;
        entity
    }

    fn create_test_doc(title: &str, subject_id: &RecordId) -> Doc {
        let id = RecordId::new_v4(TableName::Doc);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects = vec![Link::new(subject_id.clone())];
        base.subjects_state = SubjectsState::Complete;

        Doc {
            base,
            title: title.to_string(),
            text: "test document".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("rendition-{title}"),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_identity_merge_keep_a_creates_tombstone_redirect() {
        let (app, client) = setup_test_app().await;

        let repo = EntityRepository::new(client);
        let entity_a = repo
            .create(create_test_entity("Entity A"))
            .await
            .expect("create a");
        let entity_b = repo
            .create(create_test_entity("Entity B"))
            .await
            .expect("create b");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "keep_a"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
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
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        // Entity B should remain as a tombstone with redirect_to -> A (not hard-deleted).
        let b_after = repo
            .get("test_space", &entity_b.base.id)
            .await
            .expect("get b after")
            .expect("b exists");
        assert!(b_after.is_merged());
        assert_eq!(
            b_after.redirect_to.as_ref().unwrap().id().to_string(),
            entity_a.base.id.to_string()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_identity_merge_keep_a_moves_identity_keys_to_survivor() {
        let (app, client) = setup_test_app().await;

        let repo = EntityRepository::new(client.clone());
        let entity_a = repo
            .create(create_test_entity("Entity A"))
            .await
            .expect("create a");
        let entity_b = repo
            .create(create_test_entity("Entity B"))
            .await
            .expect("create b");

        // Create an entity_identity_key row pointing at entity B (source of truth table).
        let key = "email:b@example.com";
        client
            .query_bind(
                r#"
                CREATE entity_identity_key CONTENT {
                    space_id: "test_space",
                    created_at: time::now(),
                    updated_at: time::now(),
                    created_by: "test_agent",
                    deleted_at: NONE,
                    security: { policy_id: "default", level: 0, labels: [], handling: [] },
                    provenance: { sources: [], extractor: NONE, run_id: NONE, chain: [] },
                    subjects: NONE,
                    subjects_state: "complete",
                    key: $key,
                    entity: type::thing("entity", $entity_id),
                    confidence: 1.0,
                    first_seen: NONE,
                    last_seen: NONE,
                    evidence: [],
                    status: "active"
                };
                "#,
                serde_json::json!({
                    "key": key,
                    "entity_id": entity_b.base.id.id().to_string(),
                }),
            )
            .await
            .expect("create entity_identity_key");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "keep_a"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
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
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let mut query_response = client
            .query_bind(
                r"
                SELECT string::concat(meta::tb(entity), ':', meta::id(entity)) AS entity
                FROM entity_identity_key
                WHERE key = $key AND deleted_at IS NONE;
                ",
                serde_json::json!({ "key": key }),
            )
            .await
            .expect("query entity_identity_key");

        let rows: Vec<IdentityKeyRow> = query_response.take(0).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity, entity_a.base.id.to_string());
    }

    #[tokio::test]
    #[ignore]
    async fn test_identity_merge_combine_preserves_metadata() {
        let (app, client) = setup_test_app().await;

        let repo = EntityRepository::new(client);
        let entity_a = repo
            .create(create_test_entity_with_metadata(
                "Entity A",
                serde_json::json!({"foo": 1}),
            ))
            .await
            .expect("create a");
        let entity_b = repo
            .create(create_test_entity_with_metadata(
                "Entity B",
                serde_json::json!({"bar": 2}),
            ))
            .await
            .expect("create b");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "combine"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
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
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

        assert_eq!(json["entity"]["metadata"]["foo"], serde_json::json!(1));
        assert_eq!(json["entity"]["metadata"]["bar"], serde_json::json!(2));
    }

    #[tokio::test]
    #[ignore]
    async fn test_identity_merge_keep_a_preserves_metadata() {
        let (app, client) = setup_test_app().await;

        let repo = EntityRepository::new(client);
        let entity_a = repo
            .create(create_test_entity_with_metadata(
                "Entity A",
                serde_json::json!({"foo": 1}),
            ))
            .await
            .expect("create a");
        let entity_b = repo
            .create(create_test_entity_with_metadata(
                "Entity B",
                serde_json::json!({"bar": 2}),
            ))
            .await
            .expect("create b");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "keep_a"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&req).unwrap()))
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
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        // Check survivor metadata
        let a_after = repo
            .get("test_space", &entity_a.base.id)
            .await
            .expect("get a after")
            .expect("a exists");
        assert_eq!(a_after.metadata["foo"], serde_json::json!(1));
        assert_eq!(a_after.metadata["bar"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn test_identity_merge_repoints_soft_deleted_docs_in_embedded_mode() {
        let (app, client) = setup_embedded_test_app().await;

        let entity_repo = EntityRepository::new(client.clone());
        let doc_repo = DocRepository::new(client.clone());

        let entity_a = entity_repo
            .create(create_test_entity("Entity A"))
            .await
            .expect("create a");
        let entity_b = entity_repo
            .create(create_test_entity("Entity B"))
            .await
            .expect("create b");

        let doc = doc_repo
            .create(create_test_doc("Merged Doc", &entity_b.base.id))
            .await
            .expect("create doc");
        doc_repo
            .delete("test_space", &doc.base.id)
            .await
            .expect("soft delete doc");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "keep_a"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&req).expect("serialize request"),
                    ))
                    .expect("build request"),
            )
            .await
            .expect("merge response");

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read error body");
            panic!(
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let restored = doc_repo
            .restore("test_space", &doc.base.id)
            .await
            .expect("restore doc");
        assert_eq!(restored.base.subjects.len(), 1);
        assert_eq!(restored.base.subjects[0].id(), &entity_a.base.id);
    }

    #[tokio::test]
    async fn test_identity_merge_rewrites_extension_rows_in_embedded_mode() {
        let _guard = env_lock().lock().await;
        let (app, client) = setup_embedded_test_app().await;
        let entity_repo = EntityRepository::new(client.clone());

        let entity_a = entity_repo
            .create(create_test_entity("Entity A"))
            .await
            .expect("create a");
        let entity_b = entity_repo
            .create(create_test_entity("Entity B"))
            .await
            .expect("create b");

        let manifest_dir =
            std::env::temp_dir().join(format!("elephant-extension-merge-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&manifest_dir).expect("create manifest dir");
        std::fs::write(
            manifest_dir.join("sampleapp.json"),
            r#"{
                "name": "sampleapp",
                "refs": [
                    {
                        "table": "sampleapp_todo",
                        "field_path": "owner",
                        "cardinality": "one",
                        "on_merge": "rewrite"
                    },
                    {
                        "table": "sampleapp_todo",
                        "field_path": "secondary",
                        "cardinality": "many",
                        "on_merge": "rewrite"
                    }
                ]
            }"#,
        )
        .expect("write manifest");

        let _extensions_guard = set_env_var_scoped("ELEPHANT_EXTENSIONS", "sampleapp");
        let _manifest_dir_guard =
            set_env_var_scoped("ELEPHANT_EXTENSION_MANIFEST_DIR", &manifest_dir);

        let store = client.embedded_store().expect("embedded store");
        let row = serde_json::json!({
            "id": "sampleapp_todo:todo-1",
            "space_id": "test_space",
            "owner": entity_b.base.id.to_string(),
            "secondary": [
                entity_b.base.id.to_string(),
                entity_a.base.id.to_string()
            ]
        });
        store
            .mutate(|mutation| {
                mutation.upsert_extension_row("sampleapp_todo", row)?;
                Ok(())
            })
            .await
            .expect("insert extension row");

        let req = serde_json::json!({
            "entity_a": entity_a.base.id.to_string(),
            "entity_b": entity_b.base.id.to_string(),
            "strategy": "keep_a"
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&req).expect("serialize request"),
                    ))
                    .expect("build request"),
            )
            .await
            .expect("merge response");

        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read error body");
            panic!(
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let rows = store
            .mutate(|mutation| {
                mutation
                    .scan_extension_rows("sampleapp_todo")
                    .map_err(Into::into)
            })
            .await
            .expect("scan extension rows");
        let row = &rows[0].1;
        assert_eq!(
            row["owner"],
            serde_json::json!(entity_a.base.id.to_string())
        );
        assert_eq!(
            row["secondary"],
            serde_json::json!([entity_a.base.id.to_string()])
        );
        let _ = std::fs::remove_dir_all(manifest_dir);
    }
}
