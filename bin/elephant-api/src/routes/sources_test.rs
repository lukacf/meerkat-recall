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
//! Integration tests for source endpoints.
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
    use std::str::FromStr;
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::{self, Next},
        response::IntoResponse,
        Router,
    };
    use tower::ServiceExt;

    use elephant_storage::{DocRepository, MockSourceRepository, Repository, StorageClient};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc::Doc;
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::RecordId;

    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use crate::routes::sources::{source_routes, SourcesState};

    async fn setup_test_app() -> (Router, StorageClient) {
        let authz = Arc::new(AuthzService::new());
        let storage = configured_storage().await;
        let state = Arc::new(SourcesState::new(storage.clone(), authz));
        (
            source_routes()
                .layer(middleware::from_fn(mock_auth))
                .with_state(state),
            storage,
        )
    }

    async fn configured_storage() -> StorageClient {
        let storage = StorageClient::new_embedded().await.expect("storage");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "test_space".into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("named space");
        storage
    }

    async fn generation(storage: &StorageClient, id: &str) -> String {
        elephant_storage::SourcePublicationRepository::new(storage.clone())
            .head("test_space", &RecordId::from_str(id).expect("source id"))
            .await
            .expect("source head")
            .expect("accepted source")
            .generation
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec![
                "read:source".to_string(),
                "write:source".to_string(),
                "read:doc".to_string(),
                "write:doc".to_string(),
            ],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_source(hash: &str) -> Source {
        let id = RecordId::new_v4(TableName::Source);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Source {
            base,
            type_: SourceType::Email,
            lifecycle_status: elephant_types::records::SourceLifecycleStatus::Discovered,
            current_ingest_mode: None,
            latest_ingest_decision: None,
            external_id: None,
            uri: None,
            path: None,
            parent: None,
            range: None,
            timestamp: None,
            author: None,
            channel: None,
            hash: hash.to_string(),
            metadata: serde_json::Value::Null,
        }
    }

    #[tokio::test]
    async fn test_create_source_returns_accepted_operation() {
        let (app, _storage) = setup_test_app().await;
        let source = create_test_source("hash123");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&source).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_get_source_returns_source() {
        let (app, storage) = setup_test_app().await;
        let source = create_test_source("hash456");
        let source_id = source.base.id.to_string();

        // An intrinsic source without a pending publication head remains
        // readable. Acceptance/publication visibility is exercised separately.
        let repository = elephant_storage::SourceRepository::new(storage);
        Repository::create(&repository, source).await.unwrap();

        // Get
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/sources/{source_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_source_returns_404() {
        let (app, _storage) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/sources/source:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_source_returns_retraction_receipt() {
        let (app, storage) = setup_test_app().await;
        let source = create_test_source("hash789");
        let source_id = source.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&source).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/v1/sources/{source_id}?expected_generation={}&idempotency_key=delete",
                        generation(&storage, &source_id).await
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_restore_deleted_source() {
        let (app, storage) = setup_test_app().await;
        let source = create_test_source("hashabc");
        let source_id = source.base.id.to_string();

        // Create
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&source).unwrap()))
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
                    .uri(format!(
                        "/v1/sources/{source_id}?expected_generation={}&idempotency_key=delete",
                        generation(&storage, &source_id).await
                    ))
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
                    .uri(format!("/v1/sources/{source_id}/restore?revision_id=restored&expected_generation={}&idempotency_key=restore", generation(&storage, &source_id).await))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_search_sources() {
        let (app, _storage) = setup_test_app().await;

        // Create sources
        for hash in ["searchhash1", "searchhash2"] {
            let source = create_test_source(hash);
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sources")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&source).unwrap()))
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
                    .uri("/v1/sources/search")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&search_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Regression test: verify that `space_id` in request body is overridden by principal's `space_id`.
    /// This prevents records from being created in a space the principal cannot access.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn test_create_source_overrides_space_id_with_principal_space() {
        let (app, storage) = setup_test_app().await;

        // Create a source with a DIFFERENT space_id than the principal's ("test_space")
        let id = RecordId::new_v4(TableName::Source);
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

        let source = Source {
            base,
            type_: SourceType::Email,
            lifecycle_status: elephant_types::records::SourceLifecycleStatus::Discovered,
            current_ingest_mode: None,
            latest_ingest_decision: None,
            external_id: None,
            uri: None,
            path: None,
            parent: None,
            range: None,
            timestamp: None,
            author: None,
            channel: None,
            hash: "space_override_test".to_string(),
            metadata: serde_json::Value::Null,
        };

        // Create the source
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&source).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Parse the response to verify the space_id was overridden
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let created: Source = serde_json::from_value(payload["source"].clone()).unwrap();

        // The space_id should be the principal's space_id, not the one we provided
        assert_eq!(
            created.base.space_id, "test_space",
            "space_id should be overridden to principal's space"
        );

        let authority = elephant_storage::SourcePublicationRepository::new(storage);
        let head = authority
            .head("test_space", &created.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            head.operation_id,
            payload["operation"]["operation_id"].as_str().unwrap()
        );
        assert!(head.published_revision.is_none());
        assert!(authority
            .head("wrong_space", &created.base.id)
            .await
            .unwrap()
            .is_none());

        // Durable acceptance does not publish the source into ordinary search.
        let search_req = serde_json::json!({
            "space_id": "test_space",
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/search")
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

        assert_eq!(search_result["total"], 0);
        assert!(search_result["results"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn test_ingest_source_enqueues_triage_work_item() {
        use elephant_storage::work_item_repository::WorkItemRepository;

        let (app, storage) = setup_test_app().await;
        let source = create_test_source("hash-ingest");
        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            title: "Source first".to_string(),
            text: "Ontology-first ingestion".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            metadata: serde_json::json!({ "collection": "default" }),
            rendition_key: "source-first-rk".to_string(),
        };
        let request = serde_json::json!({
            "source": source,
            "doc": doc,
            "ingest_mode": "graph"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let accepted_doc_id =
            RecordId::from_str(result["doc_id"].as_str().expect("immutable doc ID"))
                .expect("parse doc ID");
        assert_ne!(accepted_doc_id, doc_id);
        assert_eq!(result["lifecycle_status"], "discovered");
        let source_id = result["source_id"].as_str().expect("source id");

        let repo = WorkItemRepository::new(storage.clone());
        let item = repo
            .find_by_idempotency_key(
                "test_space",
                &format!(
                    "triage_source:{source_id}:{}",
                    result["operation"]["source_generation"]
                        .as_str()
                        .expect("generation")
                ),
            )
            .await
            .expect("find triage work item");
        assert!(item.is_some(), "triage_source should be enqueued");

        let doc_repo = DocRepository::new(storage);
        let persisted = doc_repo
            .get("test_space", &accepted_doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(source_id.to_string())
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_graph_without_doc() {
        let (app, _storage) = setup_test_app().await;
        let source = create_test_source("hash-ingest-graph");
        let request = serde_json::json!({
            "source": source,
            "ingest_mode": "graph"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_ingest_source_accepts_normalized_staged_doc_shape() {
        let (app, storage) = setup_test_app().await;

        let request = serde_json::json!({
            "name": "Observed dashboard frame",
            "uri": "https://example.com/frame/1",
            "source_type": "device_frame",
            "title": "Observed frame",
            "text": "Operator reviewed the dashboard",
            "collection": "frames",
            "metadata": { "sensor": "dashboard-camera" },
            "ingest_mode": "graph"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let doc_id = RecordId::from_str(result["doc_id"].as_str().expect("doc id")).unwrap();

        let persisted = DocRepository::new(storage)
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Observed frame");
        assert_eq!(persisted.text, "Operator reviewed the dashboard");
        assert_eq!(
            persisted.metadata.get("collection"),
            Some(&serde_json::Value::String("frames".to_string()))
        );
        assert_eq!(
            persisted.metadata.get("sensor"),
            Some(&serde_json::Value::String("dashboard-camera".to_string()))
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_mixed_legacy_and_normalized_shapes() {
        let (app, _storage) = setup_test_app().await;
        let source = create_test_source("hash-ingest-mixed");
        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: BaseFields::new(doc_id, "test_space".to_string(), "test_agent".to_string()),
            title: "Legacy".to_string(),
            text: "Legacy body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            metadata: serde_json::json!({}),
            rendition_key: "legacy-rk".to_string(),
        };
        let request = serde_json::json!({
            "name": "Mixed source shape",
            "uri": "https://example.com/mixed",
            "source_type": "web",
            "title": "Normalized",
            "text": "Normalized body",
            "source": source,
            "doc": doc,
            "ingest_mode": "graph"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(error["code"], "VALIDATION_ERROR");
        assert!(error["message"]
            .as_str()
            .unwrap()
            .contains("invalid source ingest request"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn test_ingest_source_keeps_prior_unaccepted_doc_tombstone() {
        let (app, storage) = setup_test_app().await;
        let source = create_test_source("hash-ingest-conflict");
        let doc_id = RecordId::new_v4(TableName::Doc);
        let existing = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            title: "Existing".to_string(),
            text: "Deleted doc".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            metadata: serde_json::json!({ "collection": "default" }),
            rendition_key: "existing-doc".to_string(),
        };
        let doc_repo = DocRepository::new(storage.clone());
        doc_repo
            .create(existing)
            .await
            .expect("create conflicting doc");
        doc_repo
            .delete("test_space", &doc_id)
            .await
            .expect("soft-delete conflicting doc");

        let doc = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            title: "Source first".to_string(),
            text: "Ontology-first ingestion".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            metadata: serde_json::json!({ "collection": "default" }),
            rendition_key: "source-first-rk".to_string(),
        };
        let request = serde_json::json!({
            "source": source,
            "doc": doc,
            "ingest_mode": "graph"
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let receipt: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let actual = RecordId::from_str(receipt["doc_id"].as_str().unwrap()).unwrap();
        assert_ne!(actual, doc_id);
        assert!(DocRepository::new(storage.clone())
            .get_with_deleted("test_space", &doc_id)
            .await
            .unwrap()
            .unwrap()
            .base
            .deleted_at
            .is_some());
        let restored = DocRepository::new(storage.clone())
            .get("test_space", &actual)
            .await
            .expect("load restored doc")
            .expect("restored doc");
        assert_eq!(restored.title, "Source first");
        assert!(restored.base.deleted_at.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn test_ingest_source_inherits_doc_security_before_create() {
        let (app, storage) = setup_test_app().await;
        let mut source = create_test_source("hash-ingest-security");
        source.base.security.level = 1;
        let doc_id = RecordId::new_v4(TableName::Doc);
        let mut doc = Doc {
            base: BaseFields::new(doc_id, "test_space".to_string(), "test_agent".to_string()),
            title: "Source first".to_string(),
            text: "Ontology-first ingestion".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            metadata: serde_json::json!({ "collection": "default" }),
            rendition_key: "source-first-rk".to_string(),
        };
        doc.base.security.level = 5;

        let request = serde_json::json!({
            "source": source,
            "doc": doc,
            "ingest_mode": "graph"
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let source_id = result["source_id"].as_str().expect("source id");

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/sources/{source_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Accepted source classification is persisted before queue work,
        // while ordinary GET remains unavailable until publication.
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let repository = elephant_storage::SourceRepository::new(storage);
        let stored = Repository::get(
            &repository,
            "test_space",
            &RecordId::from_str(source_id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stored.base.security.level, 5);
        assert_eq!(stored.base.space_id, "test_space");
    }

    async fn mock_write_only_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "write-only".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["write:source".to_string()],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn test_restore_source_requires_read_authorization() {
        let authz = Arc::new(AuthzService::new());
        let storage = configured_storage().await;
        let state = Arc::new(SourcesState::new(storage.clone(), authz));
        let full = source_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state.clone());
        let write_only = source_routes()
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

        let deleted = create_test_source("privatefixturehash");
        let deleted_id = deleted.base.id.to_string();
        let active = create_test_source("privatefixturehash2");
        let active_id = active.base.id.to_string();
        for record in [
            serde_json::to_string(&deleted).unwrap(),
            serde_json::to_string(&active).unwrap(),
        ] {
            let created = full
                .clone()
                .oneshot(send("POST", "/v1/sources".to_string(), Body::from(record)))
                .await
                .unwrap();
            assert!(created.status().is_success());
        }
        let removed = full
            .clone()
            .oneshot(send(
                "DELETE",
                format!(
                    "/v1/sources/{deleted_id}?expected_generation={}&idempotency_key=delete",
                    generation(&storage, &deleted_id).await
                ),
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
                    format!("/v1/sources/{id}/restore"),
                    Body::empty(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("privatefixturehash"));
        }

        // No side effect: the deleted row is still deleted.
        let still_deleted = full
            .clone()
            .oneshot(send(
                "GET",
                format!("/v1/sources/{deleted_id}"),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(still_deleted.status(), StatusCode::NOT_FOUND);

        // Positive control: read and write together restore it.
        let restored = full
            .oneshot(send(
                "POST",
                format!("/v1/sources/{deleted_id}/restore?revision_id=restored&expected_generation={}&idempotency_key=restore", generation(&storage, &deleted_id).await),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(restored.status(), StatusCode::OK);
    }
}
