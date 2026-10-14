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
#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario transport module and its existing independent type identity"
)]
#[path = "../../../recall_preview_semantics/provider_spy.rs"]
mod document_transport;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::document_transport;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::{self, Next},
        response::IntoResponse,
        Router,
    };
    use elephant_api::auth::Principal as AuthPrincipal;
    use elephant_api::authz::AuthzService;
    use elephant_api::routes::docs::{doc_routes, DocsState};
    use elephant_migrations::apply_schema;
    use elephant_pipeline::extraction::{
        DefaultExtractor, ExtractionContext, ExtractionResult, Extractor,
    };
    use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
    use elephant_storage::source_publication::SourcePublicationRepository;
    use elephant_storage::{Repository, SpaceConfigRepository, StorageClient, StorageConfig};
    use elephant_types::provider_binding::ModelOperation;
    use elephant_types::publication::{OperationReceipt, OperationState, SourceVisibility};
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::{work::PluginManifest, Doc};
    use elephant_types::{RecordId, TableName};
    use tower::ServiceExt;
    use uuid::Uuid;

    const SPACE: &str = "test_space";

    // These document endpoint oracles make no generated graph claims. The
    // production dispatcher and finalizer still publish every accepted Doc.
    struct DocumentOnlyExtractor {
        manifest: PluginManifest,
    }
    #[async_trait::async_trait]
    impl Extractor for DocumentOnlyExtractor {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }
        async fn extract(
            &self,
            _ctx: ExtractionContext,
        ) -> Result<ExtractionResult, elephant_pipeline::PipelineError> {
            Ok(ExtractionResult::empty("document-api-fixture".into()))
        }
    }

    async fn setup_test_app() -> (Router, StorageClient, document_transport::Spy) {
        let client = StorageClient::new(StorageConfig {
            database: format!("docs_api_test_{}", Uuid::new_v4()),
            ..StorageConfig::default()
        })
        .await
        .expect("client");
        apply_schema(&client).await.expect("schema");
        let spy = document_transport::Spy::start()
            .await
            .expect("loopback fixture");
        let configs = SpaceConfigRepository::new(client.clone());
        document_transport::configure(&configs, SPACE, &spy.endpoint, 3072)
            .await
            .expect("explicit named space");
        document_transport::edit(&configs, SPACE, |processing| {
            processing.bindings.retain(|operation, _| {
                matches!(
                    operation,
                    ModelOperation::QueryEmbedding | ModelOperation::DocumentEmbedding
                )
            });
            Ok(())
        })
        .await
        .expect("only document/query approvals");
        let state = Arc::new(DocsState::new(&client, Arc::new(AuthzService::new())));
        let app = doc_routes()
            .layer(middleware::from_fn(mock_auth))
            .with_state(state);
        (app, client, spy)
    }

    async fn mock_auth(mut req: Request<Body>, next: Next) -> impl IntoResponse {
        req.extensions_mut().insert(AuthPrincipal {
            subject: "test-user".into(),
            space_id: Some(SPACE.into()),
            scopes: vec![
                "read:doc:metadata".into(),
                "read:doc:raw".into(),
                "write:doc".into(),
                "read:source".into(),
                "write:source".into(),
                "read:entity".into(),
            ],
            security_level: 10,
            security_labels: vec![],
            ..AuthPrincipal::default()
        });
        next.run(req).await
    }

    fn create_test_doc(title: &str) -> Doc {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            SPACE.into(),
            "test_agent".into(),
        );
        base.security = Security {
            policy_id: "default".into(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        Doc {
            base,
            title: title.into(),
            text: format!("Content of {title}"),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: format!("doc_{}", Uuid::new_v4()),
            metadata: serde_json::json!({}),
        }
    }

    async fn response_json(
        response: axum::response::Response,
        expected: StatusCode,
    ) -> serde_json::Value {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .expect("bounded response");
        assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    struct AcceptedDocument {
        doc_id: RecordId,
        operation: OperationReceipt,
    }

    async fn ingest(app: &Router, doc: &Doc) -> AcceptedDocument {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"doc":doc,"collection":"default"}))
                            .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = response_json(response, StatusCode::ACCEPTED).await;
        let operation: OperationReceipt =
            serde_json::from_value(result["operation"].clone()).unwrap();
        let doc_id: RecordId = result["doc_id"].as_str().unwrap().parse().unwrap();
        assert_eq!(operation.state, OperationState::Accepted);
        assert_eq!(result["source_id"], operation.source_id.to_string());
        assert_eq!(result["enqueued"], true);
        assert!(result["work_item_id"].as_str().is_some());
        assert_ne!(
            doc_id, doc.base.id,
            "response carries the accepted immutable physical identity"
        );
        AcceptedDocument { doc_id, operation }
    }

    async fn get(app: &Router, doc_id: &RecordId) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/docs/{doc_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn publish(
        client: &StorageClient,
        spy: &document_transport::Spy,
        accepted: &OperationReceipt,
    ) {
        let before = spy.count().expect("spy count");
        let extractor = DocumentOnlyExtractor {
            manifest: DefaultExtractor::new().manifest().clone(),
        };
        with_dispatch_credentials(
            Arc::new(document_transport::SpyCredentials),
            Box::pin(crate::pipeline_driver::drive_pipeline(
                client, SPACE, &extractor, 300,
            )),
        )
        .await
        .expect("actual source dispatcher and finalizer");
        assert!(
            spy.count().unwrap() > before,
            "publication must physically embed the accepted document"
        );
        assert!(spy
            .requests()
            .unwrap()
            .iter()
            .all(|request| request.path.ends_with("/embeddings")));
        let completed = SourcePublicationRepository::new(client.clone())
            .operation(SPACE, &accepted.operation_id)
            .await
            .unwrap()
            .expect("original operation");
        assert_eq!(completed.state, OperationState::Completed);
        assert_eq!(completed.source_id, accepted.source_id);
        assert_eq!(completed.source_generation, accepted.source_generation);
        assert_eq!(completed.published_revision, accepted.desired_revision);
    }

    async fn ingest_published(
        app: &Router,
        client: &StorageClient,
        spy: &document_transport::Spy,
        doc: &Doc,
    ) -> AcceptedDocument {
        let accepted = Box::pin(ingest(app, doc)).await;
        let publications = SourcePublicationRepository::new(client.clone());
        let revision_id = accepted.operation.desired_revision.as_deref().unwrap();
        let raw_revision = publications
            .revision(SPACE, &accepted.operation.source_id, revision_id)
            .await
            .unwrap()
            .expect("original accepted raw revision");
        assert_eq!(raw_revision.source_id, accepted.operation.source_id);
        assert_eq!(raw_revision.revision_id, revision_id);
        assert_eq!(
            raw_revision
                .source
                .manual_doc_template()
                .unwrap()
                .unwrap()
                .text,
            doc.text,
            "acceptance must preserve the exact raw input"
        );
        assert_eq!(
            get(app, &accepted.doc_id).await.status(),
            StatusCode::NOT_FOUND,
            "pending accepted evidence must not be published by fixture setup"
        );
        Box::pin(publish(client, spy, &accepted.operation)).await;
        let stored = elephant_storage::DocRepository::new(client.clone())
            .get(SPACE, &accepted.doc_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.title, doc.title);
        // These fixtures deliberately submit single lines without trailing
        // whitespace. The text renderer 1.0.0 contract appends one LF to them;
        // derive this expectation from the fixture input, never product output.
        assert!(!doc.text.is_empty());
        assert!(!doc.text.contains(['\r', '\n']));
        assert_eq!(doc.text.trim_end(), doc.text);
        assert_eq!(stored.text, format!("{}\n", doc.text));
        assert_eq!(stored.base.security, doc.base.security);
        assert_eq!(stored.base.subjects, doc.base.subjects);
        assert_eq!(stored.base.subjects_state, doc.base.subjects_state);
        let original_after_publication = publications
            .revision(SPACE, &accepted.operation.source_id, revision_id)
            .await
            .unwrap()
            .expect("publication retains original raw revision");
        assert_eq!(
            serde_json::to_value(original_after_publication).unwrap(),
            serde_json::to_value(raw_revision).unwrap(),
            "rendering must not rewrite accepted source input"
        );
        accepted
    }

    async fn retract(app: &Router, accepted: &AcceptedDocument, key: &str) -> OperationReceipt {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/v1/docs/{}?expected_generation={}&idempotency_key={key}",
                        accepted.doc_id, accepted.operation.source_generation
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = response_json(response, StatusCode::ACCEPTED).await;
        let receipt: OperationReceipt =
            serde_json::from_value(result["operation"].clone()).unwrap();
        assert_eq!(receipt.source_id, accepted.operation.source_id);
        assert_ne!(
            receipt.source_generation,
            accepted.operation.source_generation
        );
        receipt
    }

    // Historical test names stay stable for the centrally selected ignored suite.
    #[tokio::test]
    #[ignore]
    async fn test_ingest_doc_returns_201() {
        let (app, client, _spy) = Box::pin(setup_test_app()).await;
        let accepted = Box::pin(ingest(&app, &create_test_doc("Test Document"))).await;
        assert_eq!(
            get(&app, &accepted.doc_id).await.status(),
            StatusCode::NOT_FOUND
        );
        let head = SourcePublicationRepository::new(client)
            .head(SPACE, &accepted.operation.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.operation_id, accepted.operation.operation_id);
        assert_eq!(head.generation, accepted.operation.source_generation);
        assert!(head.published_revision.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn test_ingest_doc_creates_chunks_and_embeddings() {
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        let mut doc = create_test_doc("Chunked Document");
        doc.text = "a".repeat(2500);
        let accepted = Box::pin(ingest_published(&app, &client, &spy, &doc)).await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v1/docs/{}/chunks", accepted.doc_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let chunks = response_json(response, StatusCode::OK).await;
        assert!(chunks.as_array().unwrap().len() >= 2);
        let tables = client.list_tables().await.expect("list tables");
        assert!(
            tables
                .iter()
                .any(|table| table.starts_with("emb__") || table.starts_with("chunk_embedding_")),
            "expected at least one embedding partition table"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_search_docs_respects_k() {
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        for title in ["Search K 1", "Search K 2", "Search K 3"] {
            Box::pin(ingest_published(
                &app,
                &client,
                &spy,
                &create_test_doc(title),
            ))
            .await;
        }
        let before = spy.count().unwrap();
        let response = with_dispatch_credentials(
            Arc::new(document_transport::SpyCredentials),
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"query":"Content","k":1,"limit":50}).to_string(),
                    ))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
        let json = response_json(response, StatusCode::OK).await;
        assert_eq!(json["results"].as_array().unwrap().len(), 1);
        assert_eq!(
            spy.count().unwrap(),
            before + 1,
            "one original governed query embedding"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_doc_returns_document() {
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        let doc = create_test_doc("Retrievable Doc");
        let accepted = Box::pin(ingest_published(&app, &client, &spy, &doc)).await;
        let body = response_json(get(&app, &accepted.doc_id).await, StatusCode::OK).await;
        assert_eq!(body["title"], doc.title);
        assert_eq!(body["text"], "Content of Retrievable Doc\n");
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_nonexistent_doc_returns_404() {
        let (app, _, _spy) = Box::pin(setup_test_app()).await;
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
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        let accepted = Box::pin(ingest_published(
            &app,
            &client,
            &spy,
            &create_test_doc("To Delete"),
        ))
        .await;
        let receipt = Box::pin(retract(&app, &accepted, "delete-document")).await;
        assert_eq!(
            get(&app, &accepted.doc_id).await.status(),
            StatusCode::NOT_FOUND
        );
        let head = SourcePublicationRepository::new(client)
            .head(SPACE, &receipt.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.lifecycle, SourceVisibility::Retracted);
        assert_eq!(head.generation, receipt.source_generation);
    }

    #[tokio::test]
    #[ignore]
    async fn test_restore_deleted_doc() {
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        let doc = create_test_doc("Restore Me");
        let accepted = Box::pin(ingest_published(&app, &client, &spy, &doc)).await;
        let retracted = Box::pin(retract(&app, &accepted, "restore-retraction")).await;
        assert_eq!(
            get(&app, &accepted.doc_id).await.status(),
            StatusCode::NOT_FOUND
        );
        let response = app.clone().oneshot(Request::builder().method("POST")
            .uri(format!("/v1/docs/{}/restore?expected_generation={}&idempotency_key=explicit-restore&revision_id=restored-r1",
                accepted.doc_id, retracted.source_generation)).body(Body::empty()).unwrap()).await.unwrap();
        let restored: Doc =
            serde_json::from_value(response_json(response, StatusCode::OK).await).unwrap();
        assert_eq!(restored.title, doc.title);
        assert_eq!(restored.text, doc.text);
        assert_ne!(restored.base.id, accepted.doc_id);
        assert_eq!(
            restored.source.as_ref().unwrap().id(),
            &accepted.operation.source_id
        );
        let repository = SourcePublicationRepository::new(client.clone());
        let head = repository
            .head(SPACE, &accepted.operation.source_id)
            .await
            .unwrap()
            .unwrap();
        let original_restore = repository
            .operation(SPACE, &head.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_restore.state, OperationState::Accepted);
        assert_eq!(
            original_restore.desired_revision.as_deref(),
            Some("restored-r1")
        );
        assert_eq!(
            get(&app, &restored.base.id).await.status(),
            StatusCode::NOT_FOUND
        );
        Box::pin(publish(&client, &spy, &original_restore)).await;
        let published = response_json(get(&app, &restored.base.id).await, StatusCode::OK).await;
        assert_eq!(published["text"], "Content of Restore Me\n");
    }

    #[tokio::test]
    #[ignore]
    async fn test_search_docs() {
        let (app, client, spy) = Box::pin(setup_test_app()).await;
        for title in ["Search Doc 1", "Search Doc 2"] {
            Box::pin(ingest_published(
                &app,
                &client,
                &spy,
                &create_test_doc(title),
            ))
            .await;
        }
        let before = spy.count().unwrap();
        let response = with_dispatch_credentials(Arc::new(document_transport::SpyCredentials),
            app.oneshot(Request::builder().method("POST").uri("/v1/docs/search").header("content-type", "application/json")
                .body(Body::from(serde_json::json!({"space_id":SPACE,"limit":50,"k":5,"query":"Content of Search Doc 1"}).to_string())).unwrap()))
            .await.unwrap();
        let body = response_json(response, StatusCode::OK).await;
        let payload: elephant_api::routes::docs::DocSearchResponse =
            serde_json::from_value(body).unwrap();
        assert!(!payload.results.is_empty());
        assert_eq!(spy.count().unwrap(), before + 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_collections() {
        let (app, _, _spy) = Box::pin(setup_test_app()).await;
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
        let (app, _, _spy) = Box::pin(setup_test_app()).await;
        let batch = serde_json::json!({"items":[create_test_doc("Batch Doc 1"),create_test_doc("Batch Doc 2"),create_test_doc("Batch Doc 3")]});
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/docs/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(batch.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
