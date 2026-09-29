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
//! Integration tests for document endpoints.
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
    use uuid::Uuid;

    use elephant_embeddings::FakeEmbeddingProvider;
    use elephant_migrations::apply_schema;
    use elephant_pipeline::renderer::default::PassthroughRenderer;
    use elephant_pipeline::renderer::RendererRegistry;
    use elephant_pipeline::scheduler::{execute_build_doc_tree, execute_render_doc, DocTreeConfig};
    use elephant_pipeline::EmbedWorker;
    use elephant_storage::{Repository, SpaceConfigRepository, StorageClient, StorageConfig};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::{Doc, SpaceConfig};
    use elephant_types::RecordId;

    use elephant_api::auth::Principal as AuthPrincipal;
    use elephant_api::authz::AuthzService;
    use elephant_api::routes::docs::{doc_routes, DocsState};

    async fn setup_test_app() -> (Router, StorageClient) {
        let config = StorageConfig {
            database: format!(
                "docs_api_test_{}",
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
        let config_repo = SpaceConfigRepository::new(client.clone());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "test_space".to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let cfg = SpaceConfig {
            base,
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3072,
            purpose: None,
            skill: None,
        };
        config_repo.create(cfg).await.expect("space config");

        let state = Arc::new(DocsState::new(&client, authz));
        let app = doc_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state);
        (app, client)
    }

    async fn run_embed_worker(client: &StorageClient) {
        let node_repo = elephant_storage::DocNodeRepository::new(client.clone());
        let revision_repo = elephant_storage::DocRevisionRepository::new(client.clone());
        let partition_manager = elephant_embeddings::PartitionManager::new();
        let embedding_repo =
            elephant_storage::NodeEmbeddingRepository::new(client.clone(), partition_manager);
        let provider = FakeEmbeddingProvider::new("test-model", 3072);
        let embed_worker = EmbedWorker::new(node_repo, revision_repo, embedding_repo, provider);
        embed_worker
            .process_pending("test_space", 10)
            .await
            .expect("process pending embeddings");
    }

    async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
        let mut req = req;
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec![
                "read:doc:metadata".to_string(),
                "write:doc".to_string(),
                "read:source".to_string(),
                "write:source".to_string(),
                "read:entity".to_string(), // for potential subject checks
            ],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_doc(title: &str) -> Doc {
        let id = RecordId::new_v4(TableName::Doc);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Doc {
            base,
            title: title.to_string(),
            text: format!("Content of {title}"),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("doc_{}", Uuid::new_v4()),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_ingest_doc_returns_201() {
        let (app, _client) = setup_test_app().await;
        let doc = create_test_doc("Test Document");

        let ingest_req = serde_json::json!({
            "doc": doc,
            "collection": "default"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
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
    #[ignore]
    async fn test_ingest_doc_creates_chunks_and_embeddings() {
        let (app, client) = setup_test_app().await;
        let mut doc = create_test_doc("Chunked Document");
        doc.text = "a".repeat(2500);
        let doc_id = doc.base.id.to_string();

        let ingest_req = serde_json::json!({
            "doc": doc,
            "collection": "default"
        });

        // Ingest the document
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // Ingestion is asynchronous. Materialize the stored document before
        // embedding, as the source-first worker does after metadata commit.
        let stored_doc = elephant_storage::DocRepository::new(client.clone())
            .get("test_space", &doc_id.parse().expect("doc id"))
            .await
            .expect("load ingested doc")
            .expect("ingested doc exists");
        let mut renderers = RendererRegistry::new();
        renderers.register(Box::new(PassthroughRenderer::new()));
        let rendered = execute_render_doc(&client, &renderers, &stored_doc, Some("text/plain"))
            .await
            .expect("render ingested doc");
        execute_build_doc_tree(&client, &rendered.revision_id, &DocTreeConfig::default())
            .await
            .expect("build document nodes");
        run_embed_worker(&client).await;

        // Now check chunks
        let chunks_resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/docs/{doc_id}/chunks"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        if chunks_resp.status() != StatusCode::OK {
            let status = chunks_resp.status();
            let body = axum::body::to_bytes(chunks_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            panic!(
                "expected 200, got {}: {}",
                status,
                String::from_utf8_lossy(&body)
            );
        }

        let body = axum::body::to_bytes(chunks_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let chunks: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(chunks.as_array().unwrap().len() >= 2);

        // Vector partitions should exist for strict corpora.
        let tables = client.list_tables().await.expect("list tables");
        assert!(
            tables
                .iter()
                .any(|t| t.starts_with("emb__") || t.starts_with("chunk_embedding_")),
            "expected at least one embedding partition table"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_search_docs_respects_k() {
        let (app, _) = setup_test_app().await;

        // Ingest multiple docs.
        for title in ["Search K 1", "Search K 2", "Search K 3"] {
            let doc = create_test_doc(title);
            let ingest_req = serde_json::json!({
                "doc": doc,
                "collection": "default"
            });
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/docs/ingest")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            if resp.status() != StatusCode::CREATED {
                let status = resp.status();
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                panic!(
                    "expected 201, got {}: {}",
                    status,
                    String::from_utf8_lossy(&body)
                );
            }
        }

        // Search with k=1 should return exactly 1 result.
        let search_req = serde_json::json!({
            "query": "Content",
            "k": 1,
            "limit": 50
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/search")
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
        assert_eq!(json["results"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_doc_returns_document() {
        let (app, _) = setup_test_app().await;
        let doc = create_test_doc("Retrievable Doc");
        let doc_id = doc.base.id.to_string();

        let ingest_req = serde_json::json!({
            "doc": doc,
            "collection": "default"
        });

        // Ingest
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Get
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/docs/{doc_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_nonexistent_doc_returns_404() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/docs/doc:00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_doc_returns_204() {
        let (app, _) = setup_test_app().await;
        let doc = create_test_doc("To Delete");
        let doc_id = doc.base.id.to_string();

        let ingest_req = serde_json::json!({
            "doc": doc,
            "collection": "default"
        });

        // Ingest
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/docs/{doc_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    #[ignore]
    async fn test_restore_deleted_doc() {
        let (app, _) = setup_test_app().await;
        let doc = create_test_doc("Restore Me");
        let doc_id = doc.base.id.to_string();

        let ingest_req = serde_json::json!({
            "doc": doc,
            "collection": "default"
        });

        // Ingest
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
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
                    .uri(format!("/v1/docs/{doc_id}"))
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
                    .uri(format!("/v1/docs/{doc_id}/restore"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore]
    async fn test_search_docs() {
        let (app, client) = setup_test_app().await;

        // Create docs
        for title in ["Search Doc 1", "Search Doc 2"] {
            let doc = create_test_doc(title);
            let ingest_req = serde_json::json!({
                "doc": doc,
                "collection": "default"
            });
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/docs/ingest")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&ingest_req).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        run_embed_worker(&client).await;

        // Search (vector path expects k + query)
        let search_req = serde_json::json!({
            "space_id": "test_space",
            "limit": 50,
            "k": 5,
            "query": "Content of Search Doc 1"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/search")
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
        let payload: elephant_api::routes::docs::DocSearchResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(!payload.results.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_collections() {
        let (app, _) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/docs/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore]
    async fn test_batch_docs() {
        let (app, _) = setup_test_app().await;

        let doc1 = create_test_doc("Batch Doc 1");
        let doc2 = create_test_doc("Batch Doc 2");
        let doc3 = create_test_doc("Batch Doc 3");

        let batch_req = serde_json::json!({
            "items": [doc1, doc2, doc3]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&batch_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
