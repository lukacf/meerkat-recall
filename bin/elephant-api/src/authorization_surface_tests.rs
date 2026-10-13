//! Regression tests for the retained public authorization boundaries.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    middleware,
    response::Response,
    Extension, Router,
};
use elephant_storage::{
    DocRepository, RelRepository, Repository, SpaceConfigRepository, StorageClient,
    WorkItemRepository,
};
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::{Doc, Rel, SpaceConfig, WorkItem, WorkItemStatus};
use elephant_types::{Link, RecordId, TableName};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use crate::{AuthzService, Principal};

fn principal() -> Principal {
    Principal {
        subject: "operator".into(),
        space_id: Some("alpha".into()),
        scopes: vec!["*".into()],
        security_level: 5,
        ..Principal::default()
    }
}

fn base(table: TableName) -> BaseFields {
    let mut base = BaseFields::new(RecordId::new_v4(table), "alpha".into(), "fixture".into());
    base.subjects_state = SubjectsState::Complete;
    base
}

fn doc() -> Doc {
    Doc {
        base: base(TableName::Doc),
        title: "Fixture".into(),
        text: "private body".into(),
        sidecar_text: Some("private sidecar".into()),
        source: None,
        media_attachments: vec![elephant_types::records::doc::DocMediaAttachment {
            attachment_id: "attachment".into(),
            kind: elephant_types::records::doc::DocMediaKind::Image,
            media_type: "image/png".into(),
            location: elephant_types::records::doc::DocMediaLocation::Uri {
                uri: "https://example.invalid/private.png".into(),
            },
            label: None,
            size_bytes: None,
            duration_ms: None,
            sha256: None,
        }],
        rendition_key: "fixture".into(),
        metadata: json!({}),
    }
}

fn rel() -> Rel {
    Rel {
        base: base(TableName::Rel),
        subject: Link::new(RecordId::new_v4(TableName::Entity)),
        object: Link::new(RecordId::new_v4(TableName::Entity)),
        rel_type: "related_to".into(),
        status: ClaimStatus::Candidate,
        confidence: 0.8,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn prepare_relationship_inputs(storage: &StorageClient, relationship: &Rel) {
    // Exercise the real canonical boundary with an explicitly migrated named
    // space and live endpoint records, not an unknown-space refusal.
    elephant_migrations::apply_schema(storage).await.unwrap();
    SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            processing: None,
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let ids = std::iter::once(relationship.subject.id().clone())
        .chain(std::iter::once(relationship.object.id().clone()))
        .chain(
            relationship
                .base
                .subjects
                .iter()
                .map(|link| link.id().clone()),
        )
        .collect::<std::collections::BTreeSet<_>>();
    let repository = elephant_storage::EntityRepository::new(storage.clone());
    for id in ids {
        let mut fields = base(TableName::Entity);
        fields.id = id;
        repository
            .create(elephant_types::records::Entity::new(
                fields,
                elephant_types::records::entity::EntityType::new("person"),
                "Live relationship input".into(),
            ))
            .await
            .unwrap();
    }
}

fn work() -> WorkItem {
    WorkItem {
        base: base(TableName::WorkItem),
        kind: "fixture".into(),
        payload: json!({"private": "payload"}),
        status: WorkItemStatus::Failed,
        priority: 0,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 1,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

#[expect(
    clippy::redundant_clone,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
fn app(storage: &StorageClient, principal: Principal) -> Router {
    let authz = Arc::new(AuthzService::new());
    Router::new()
        .merge(crate::routes::space_routes().with_state(Arc::new(
            crate::routes::spaces::SpaceApiState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::doc_routes().with_state(Arc::new(
            crate::routes::docs::DocsState::new(storage, authz.clone()),
        )))
        .merge(crate::routes::rel_routes().with_state(Arc::new(
            crate::routes::relationships::RelationshipsState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::work_item_routes().with_state(Arc::new(
            crate::routes::work_items::WorkItemsState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::entity_routes().with_state(Arc::new(
            crate::routes::entities::AppState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::timeline_routes().with_state(Arc::new(
            crate::routes::timeline::TimelineState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::identity_routes().with_state(Arc::new(
            crate::routes::identity::IdentityState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::system_routes().with_state(Arc::new(
            crate::routes::system::SystemState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::events_routes().with_state(Arc::new(
            crate::routes::events::EventsState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::assertion_routes().with_state(Arc::new(
            crate::routes::assertions::AssertionsState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::predicate_routes().with_state(Arc::new(
            crate::routes::predicates::PredicatesState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::truth_slot_routes().with_state(Arc::new(
            crate::routes::truth_slots::TruthSlotsState::new(storage.clone(), authz.clone()),
        )))
        .merge(crate::routes::knowledge_view_routes().with_state(Arc::new(
            crate::routes::knowledge_views::KnowledgeViewsState::new(
                storage.clone(),
                authz.clone(),
            ),
        )))
        .merge(crate::routes::source_routes().with_state(Arc::new(
            crate::routes::sources::SourcesState::new(storage.clone(), authz.clone()),
        )))
        .layer(Extension(principal))
}

async fn request(app: Router, method: &str, path: &str, body: Value) -> Response {
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

async fn json_body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap()).unwrap()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn space_admin_requires_scope_tenant_and_record_authority() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let repo = SpaceConfigRepository::new(storage.clone());
    for name in ["alpha", "beta"] {
        let mut fields = base(TableName::SpaceConfig);
        fields.space_id = name.into();
        repo.create(SpaceConfig {
            processing: None,
            base: fields,
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    }
    let allowed = app(&storage, principal());
    let rows = json_body(request(allowed.clone(), "GET", "/v1/spaces", Value::Null).await).await;
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["space_id"], "alpha");
    for method in ["GET", "PUT"] {
        assert_eq!(
            request(
                allowed.clone(),
                method,
                "/v1/spaces/beta",
                json!({"purpose":"changed"})
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        request(
            allowed.clone(),
            "POST",
            "/v1/spaces",
            json!({"name":"gamma"})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert!(repo.get_by_space_id("gamma").await.unwrap().is_none());
    let mut no_admin = principal();
    no_admin.scopes = vec!["read:entity".into()];
    assert_eq!(
        request(
            app(&storage, no_admin),
            "PUT",
            "/v1/spaces/alpha",
            json!({"purpose":"changed"})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let response = request(
        allowed.clone(),
        "PUT",
        "/v1/spaces/alpha",
        json!({"purpose":"authorized"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        repo.get_by_space_id("alpha")
            .await
            .unwrap()
            .unwrap()
            .purpose
            .as_deref(),
        Some("authorized")
    );
    let mut config = repo.get_by_space_id("alpha").await.unwrap().unwrap();
    config.base.security.level = 8;
    // Fixture-only classification change: the ordinary repository update
    // correctly refuses relabeling, so it cannot establish this read-denial case.
    storage
        .embedded_store()
        .unwrap()
        .replace(config)
        .await
        .unwrap();
    assert_eq!(
        request(allowed.clone(), "GET", "/v1/spaces/alpha", Value::Null)
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    let rows = json_body(request(allowed, "GET", "/v1/spaces", Value::Null).await).await;
    assert!(rows.as_array().unwrap().is_empty());
    let mut restricted = principal();
    restricted.space_id = Some("gamma".into());
    restricted.subject_allowlist = Some(vec![]);
    assert_eq!(
        request(
            app(&storage, restricted),
            "POST",
            "/v1/spaces",
            json!({"name":"gamma"})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let mut creator = principal();
    creator.space_id = Some("gamma".into());
    assert_eq!(request(app(&storage, creator), "POST", "/v1/spaces", json!({"name":"gamma", "embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3})).await.status(), StatusCode::CREATED);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn relationship_replacements_preserve_protected_live_and_deleted_records() {
    for mode in [
        "update",
        "upsert",
        "batch",
        "deleted_batch",
        "deleted_upsert",
    ] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let repo = RelRepository::new(storage.clone());
        let mut original = rel();
        original.base.security.level = 4;
        prepare_relationship_inputs(&storage, &original).await;
        repo.create(original.clone()).await.unwrap();
        if mode.starts_with("deleted") {
            repo.delete("alpha", &original.base.id).await.unwrap();
        }
        let before = repo
            .get_with_deleted("alpha", &original.base.id)
            .await
            .unwrap()
            .unwrap();
        let mut replacement = original.clone();
        replacement.base.security.level = 0;
        let (method, path, payload) = if mode.ends_with("batch") {
            (
                "POST",
                "/v1/relationships/batch".into(),
                json!({"items":[replacement]}),
            )
        } else if mode.ends_with("upsert") {
            (
                "POST",
                "/v1/relationships/upsert".into(),
                json!(replacement),
            )
        } else {
            (
                "PUT",
                format!("/v1/relationships/{}", original.base.id),
                json!(replacement),
            )
        };
        let response = request(app(&storage, principal()), method, &path, payload).await;
        if mode.ends_with("batch") {
            let body = json_body(response).await;
            assert!(body["succeeded"].as_array().unwrap().is_empty());
            assert_eq!(body["failed"].as_array().unwrap().len(), 1);
        } else {
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        assert_eq!(
            repo.get_with_deleted("alpha", &original.base.id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn authorized_relationship_content_edit_succeeds() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let repo = RelRepository::new(storage.clone());
    let original = rel();
    prepare_relationship_inputs(&storage, &original).await;
    let mut original = repo.create(original).await.unwrap();
    original.confidence = 0.5;
    let response = request(
        app(&storage, principal()),
        "POST",
        "/v1/relationships/upsert",
        json!(original),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        repo.get("alpha", &original.base.id)
            .await
            .unwrap()
            .unwrap()
            .confidence
            .to_bits(),
        0.5_f32.to_bits()
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn document_metadata_routes_do_not_return_raw_payloads() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let doc = DocRepository::new(storage.clone())
        .create(doc())
        .await
        .unwrap();
    let mut metadata = principal();
    metadata.scopes = vec!["read:doc:metadata".into()];
    let paths = [
        format!("/v1/docs/{}", doc.base.id),
        format!("/api/v1/spaces/alpha/documents/{}", doc.base.id),
        "/api/v1/spaces/alpha/documents".into(),
        "/v1/docs/search".into(),
    ];
    for path in &paths {
        let response = request(
            app(&storage, metadata.clone()),
            if path.ends_with("search") {
                "POST"
            } else {
                "GET"
            },
            path,
            json!({}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await.to_string();
        assert!(!body.contains("private body"));
        assert!(!body.contains("private sidecar"));
        assert!(!body.contains("private.png"));
    }
    let response = request(app(&storage, principal()), "GET", &paths[0], Value::Null).await;
    let body = json_body(response).await;
    assert_eq!(body["text"], "private body");
    assert_eq!(body["sidecar_text"], "private sidecar");
    assert_eq!(
        request(
            app(&storage, metadata),
            "GET",
            &format!("/v1/docs/{}/chunks", doc.base.id),
            Value::Null
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn retry_requires_work_scope_clearance_and_returns_authorized_result() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let repo = WorkItemRepository::new(storage.clone());
    let item = repo.create(work()).await.unwrap();
    let path = format!("/v1/work-items/{}/retry", item.base.id);
    for scopes in [
        vec!["read:work_item".into()],
        vec!["write:work_item".into()],
        vec!["read:entity".into(), "write:entity".into()],
    ] {
        let mut caller = principal();
        caller.scopes = scopes;
        assert_eq!(
            request(app(&storage, caller), "POST", &path, Value::Null)
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            repo.get("alpha", &item.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Failed
        );
    }
    let mut protected = item.clone();
    protected.base.security.level = 8;
    repo.update("alpha", &item.base.id, protected)
        .await
        .unwrap();
    assert_eq!(
        request(app(&storage, principal()), "POST", &path, Value::Null)
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    assert!(repo.retry_authorized(&item).await.is_err());
    let mut cleared = principal();
    cleared.security_level = 10;
    let response = request(app(&storage, cleared), "POST", &path, Value::Null).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["status"], "runnable");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn read_only_runtime_rejects_mutations_even_for_bypass_principals() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let runtime = Arc::new(crate::runtime::RuntimeManager::new_preconfigured(
        storage.clone(),
    ));
    runtime
        .set_mode_for_tests(crate::runtime::RuntimeMode::ReadOnly)
        .await;
    let mut caller = principal();
    caller.bypass_abac = true;
    let router = app(&storage, caller).layer(middleware::from_fn_with_state(
        runtime.clone(),
        crate::runtime_access::enforce_runtime_access,
    ));
    for (method, path) in [
        ("POST", "/v1/entities"),
        ("POST", "/v1/entities/upsert"),
        ("POST", "/v1/entities/batch"),
        ("PUT", "/v1/entities/missing"),
        ("DELETE", "/v1/entities/missing"),
        ("POST", "/v1/entities/missing/restore"),
        ("POST", "/v1/relationships"),
        ("POST", "/v1/relationships/upsert"),
        ("POST", "/v1/relationships/batch"),
        ("PUT", "/v1/relationships/missing"),
        ("DELETE", "/v1/relationships/missing"),
        ("POST", "/v1/relationships/missing/restore"),
        ("POST", "/v1/timeline"),
        ("POST", "/v1/timeline/batch"),
        ("PUT", "/v1/timeline/missing"),
        ("DELETE", "/v1/timeline/missing"),
        ("POST", "/v1/timeline/missing/restore"),
        ("POST", "/v1/docs/ingest"),
        ("PUT", "/v1/docs/missing"),
        ("DELETE", "/v1/docs/missing"),
        ("POST", "/v1/docs/missing/restore"),
        ("POST", "/v1/sources/ingest"),
        ("PUT", "/v1/sources/missing"),
        ("DELETE", "/v1/sources/missing"),
        ("POST", "/v1/sources/missing/restore"),
        ("POST", "/v1/docs/batch"),
        ("POST", "/v1/sources"),
        ("POST", "/v1/identity/candidates"),
        ("POST", "/v1/identity/merge"),
        ("POST", "/v1/assertions"),
        ("DELETE", "/v1/assertions/missing"),
        ("POST", "/v1/assertions/missing/restore"),
        ("PATCH", "/v1/assertions/missing/resolution"),
        ("POST", "/v1/predicates"),
        ("POST", "/v1/predicates/aliases/deactivate-hygiene"),
        ("POST", "/v1/predicates/missing/aliases"),
        ("PATCH", "/v1/predicates/missing/aliases/missing"),
        ("POST", "/v1/spaces"),
        ("PUT", "/v1/spaces/alpha"),
        ("POST", "/api/v1/spaces/alpha/sources"),
        ("POST", "/api/v1/spaces/alpha/entities"),
        ("POST", "/api/v1/spaces/alpha/documents"),
        ("POST", "/api/v1/predicates/proposals/missing/approve"),
        ("POST", "/v1/work-items/missing/retry"),
        ("POST", "/v1/audit/revert/missing"),
        ("POST", "/v1/system/snapshot/import"),
    ] {
        let response = request(router.clone(), method, path, json!({})).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {path}");
        let refusal = json_body(response).await;
        assert_eq!(
            refusal["error_code"], "capability_disabled",
            "{method} {path}"
        );
        assert_eq!(refusal["retryable"], false, "{method} {path}");
    }
    assert_eq!(
        request(router.clone(), "POST", "/v1/entities/search", json!({}))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(router.clone(), "GET", "/v1/spaces", Value::Null)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(request(router.clone(), "POST", "/v1/spaces", json!({"name":"alpha", "embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3})).await.status(), StatusCode::FORBIDDEN);
    assert!(SpaceConfigRepository::new(storage.clone())
        .get_by_space_id("alpha")
        .await
        .unwrap()
        .is_none());
    runtime
        .set_mode_for_tests(crate::runtime::RuntimeMode::LocalMemory)
        .await;
    assert_eq!(request(router, "POST", "/v1/spaces", json!({"name":"alpha", "embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3})).await.status(), StatusCode::CREATED);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn document_chunks_authorize_revision_and_individual_nodes() {
    use elephant_storage::{DocNodeRepository, DocRevisionRepository};
    use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    for protected in ["none", "revision", "node"] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let doc = DocRepository::new(storage.clone())
            .create(doc())
            .await
            .unwrap();
        let mut revision_base = base(TableName::DocRevision);
        if protected == "revision" {
            revision_base.security.level = 8;
        }
        let revision = DocRevision {
            base: revision_base,
            doc: Link::new(doc.base.id.clone()),
            text: doc.text.clone(),
            text_hash: format!("{:x}", Sha256::digest(doc.text.as_bytes())),
            renderer: RendererMetadata {
                name: "fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        };
        DocRevisionRepository::new(storage.clone())
            .create(revision.clone())
            .await
            .unwrap();
        let mut node_base = base(TableName::DocNode);
        if protected == "node" {
            node_base.security.level = 8;
        }
        let node = DocNode {
            base: node_base,
            revision: Link::new(revision.base.id),
            role: NodeRole::Paragraph,
            span: TextSpan {
                start: 0,
                end: u32::try_from(doc.text.len()).unwrap(),
            },
            node_key: "fixture".into(),
            text: Some(doc.text.clone()),
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        };
        DocNodeRepository::new(storage.clone())
            .create(node)
            .await
            .unwrap();
        let response = request(
            app(&storage, principal()),
            "GET",
            &format!("/v1/docs/{}/chunks", doc.base.id),
            Value::Null,
        )
        .await;
        if protected == "revision" {
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        } else {
            assert_eq!(response.status(), StatusCode::OK);
            let body = json_body(response).await;
            assert_eq!(
                body.as_array().unwrap().len(),
                usize::from(protected == "none")
            );
            if protected == "none" {
                assert_eq!(body[0]["text"], doc.text);
            }
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn work_item_lists_filter_scope_and_classification_in_hot_and_cold_storage() {
    for cold in [false, true] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let repo = WorkItemRepository::new(storage.clone());
        for protected in [false, true] {
            let mut item = work();
            item.base.security.level = if protected { 8 } else { 0 };
            item.base.updated_at = chrono::Utc::now() - chrono::Duration::days(40);
            repo.create(item).await.unwrap();
        }
        if cold {
            let moved = elephant_storage::work_retention::archive_terminal_work(
                &storage,
                chrono::Utc::now() - chrono::Duration::days(30),
                10,
            )
            .await
            .unwrap();
            assert_eq!(moved.items_archived, 2);
        }
        for path in [
            "/api/v1/work_items",
            "/api/v1/work_items?status=failed",
            "/api/v1/work_items?status=failed&kind=fixture",
            "/v1/work-items",
        ] {
            for (scopes, level, visible) in [
                (vec!["read:entity"], 10, 0),
                (vec!["read:work_item"], 5, 1),
                (vec!["read:work_item"], 10, 2),
            ] {
                let mut caller = principal();
                caller.scopes = scopes.into_iter().map(str::to_owned).collect();
                caller.security_level = level;
                let response = request(app(&storage, caller), "GET", path, Value::Null).await;
                assert_eq!(response.status(), StatusCode::OK);
                let body = json_body(response).await;
                assert_eq!(
                    body["results"].as_array().unwrap().len(),
                    visible,
                    "{path}, cold={cold}"
                );
            }
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn relationship_replacement_requires_both_scopes_and_existing_record_clearance() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let repo = RelRepository::new(storage.clone());
    let mut original = rel();
    original.base.security.level = 4;
    prepare_relationship_inputs(&storage, &original).await;
    let original = repo.create(original).await.unwrap();
    for (scopes, level) in [
        (vec!["read:entity", "read:relationship"], 5),
        (vec!["read:entity", "write:relationship"], 5),
        (
            vec!["read:entity", "read:relationship", "write:relationship"],
            3,
        ),
    ] {
        let mut caller = principal();
        caller.scopes = scopes.into_iter().map(str::to_owned).collect();
        caller.security_level = level;
        for (method, path, payload) in [
            (
                "PUT",
                format!("/v1/relationships/{}", original.base.id),
                json!(original),
            ),
            ("POST", "/v1/relationships/upsert".into(), json!(original)),
            (
                "POST",
                "/v1/relationships/batch".into(),
                json!({"items":[original]}),
            ),
        ] {
            let response = request(app(&storage, caller.clone()), method, &path, payload).await;
            if path.ends_with("batch") {
                let body = json_body(response).await;
                assert!(body["succeeded"].as_array().unwrap().is_empty());
                assert_eq!(body["failed"].as_array().unwrap().len(), 1);
            } else {
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
            }
            assert_eq!(
                repo.get("alpha", &original.base.id).await.unwrap(),
                Some(original.clone())
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_in_scrutinee,
    clippy::significant_drop_tightening,
    reason = "Preserve both original backend fixtures and their loop lifetimes."
)]
async fn equivalent_relationship_envelopes_are_accepted_on_both_backends() {
    for storage in [
        StorageClient::new_embedded().await.unwrap(),
        StorageClient::new_memory().await.unwrap(),
    ] {
        let repo = RelRepository::new(storage.clone());
        let mut original = rel();
        original.base.security.labels = vec!["first".into(), "second".into()];
        original.base.subjects = vec![
            Link::new(RecordId::new_v4(TableName::Entity)),
            Link::new(RecordId::new_v4(TableName::Entity)),
        ];
        prepare_relationship_inputs(&storage, &original).await;
        let original = repo.create(original).await.unwrap();
        let mut replacement = original.clone();
        replacement.base.security.labels.reverse();
        replacement.base.security.labels.push("first".into());
        replacement.base.subjects.reverse();
        replacement
            .base
            .subjects
            .push(original.base.subjects[0].clone());
        replacement.confidence = 0.5;
        let mut caller = principal();
        caller.security_labels = vec!["first".into(), "second".into()];
        let response = request(
            app(&storage, caller),
            "PUT",
            &format!("/v1/relationships/{}", original.base.id),
            json!(replacement),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let saved = repo.get("alpha", &original.base.id).await.unwrap().unwrap();
        assert_eq!(saved.base.security, original.base.security);
        assert_eq!(saved.base.subjects, original.base.subjects);
        assert_eq!(saved.confidence.to_bits(), 0.5_f32.to_bits());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn create_space_requires_read_authority_before_existence_lookup() {
    for exists in [false, true] {
        let storage = StorageClient::new_embedded().await.unwrap();
        if exists {
            SpaceConfigRepository::new(storage.clone())
                .create(SpaceConfig {
                    processing: None,
                    base: base(TableName::SpaceConfig),
                    embedding_provider: "fake".into(),
                    embedding_model: "fixture".into(),
                    embedding_dims: 3,
                    purpose: None,
                    skill: None,
                })
                .await
                .unwrap();
        }
        let mut writer = principal();
        writer.scopes = vec!["write:space_config".into()];
        let response = request(
            app(&storage, writer),
            "POST",
            "/v1/spaces",
            json!({"name":"alpha"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(json_body(response).await["code"], "ACCESS_DENIED");
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn custom_space_security_is_authorized_before_creation_and_preserved() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let mut caller = principal();
    caller.scopes = vec!["read:space_config".into(), "write:space_config".into()];
    let body = json!({
        "name":"alpha", "embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3,
        "security":{"policy_id":"household-v1","level":2,"labels":["audience:owner"],"handling":["no_voice"]}
    });
    assert_eq!(
        request(
            app(&storage, caller.clone()),
            "POST",
            "/v1/spaces",
            body.clone()
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let repo = SpaceConfigRepository::new(storage.clone());
    assert!(repo.get_by_space_id("alpha").await.unwrap().is_none());
    caller.clearances = vec![elephant_policy::Clearance {
        policy_id: "household-v1".into(),
        max_level: 2,
        labels: ["audience:owner".into()].into(),
        handling_allow: ["no_voice".into()].into(),
    }];
    assert_eq!(
        request(
            app(&storage, caller.clone()),
            "POST",
            "/v1/spaces",
            body.clone()
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    let before = repo.get_by_space_id("alpha").await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&before.base.security).unwrap(),
        body["security"]
    );
    // A caller authorized only for a proposed public envelope cannot discover
    // the existing protected space through the create-conflict response.
    let mut public_caller = principal();
    public_caller.scopes = caller.scopes.clone();
    assert_eq!(
        request(
            app(&storage, public_caller),
            "POST",
            "/v1/spaces",
            json!({"name":"alpha"})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert!(request(
        app(&storage, caller),
        "PUT",
        "/v1/spaces/alpha",
        json!({"security":{"policy_id":"default","level":0}})
    )
    .await
    .status()
    .is_client_error());
    let after = repo.get_by_space_id("alpha").await.unwrap().unwrap();
    assert_eq!(after.base.security, before.base.security);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original authorization fixture ownership and drop order."
)]
async fn chunks_authorize_the_document_revision_and_each_node() {
    use chrono::Utc;
    use elephant_storage::{DocNodeRepository, DocRevisionRepository};
    use elephant_types::records::doc_node::{NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
    use elephant_types::records::{DocNode, DocRevision};

    for case in [
        "allowed",
        "doc",
        "revision",
        "node",
        "foreign_revision",
        "foreign_node",
    ] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let mut document = doc();
        if case == "doc" {
            document.base.security.level = 8;
        }
        DocRepository::new(storage.clone())
            .create(document.clone())
            .await
            .unwrap();
        let mut revision = DocRevision {
            base: base(TableName::DocRevision),
            doc: Link::new(document.base.id.clone()),
            text: document.text.clone(),
            text_hash: format!("{:x}", Sha256::digest(document.text.as_bytes())),
            renderer: RendererMetadata {
                name: "fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        if case == "revision" {
            revision.base.security.level = 8;
        }
        if case == "foreign_revision" {
            revision.base.space_id = "beta".into();
        }
        DocRevisionRepository::new(storage.clone())
            .create(revision.clone())
            .await
            .unwrap();
        let mut node = DocNode {
            base: base(TableName::DocNode),
            revision: Link::new(revision.base.id),
            role: NodeRole::Paragraph,
            span: TextSpan { start: 0, end: 12 },
            node_key: "fixture".into(),
            text: Some("private body".into()),
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        };
        if case == "node" {
            node.base.security.level = 8;
        }
        if case == "foreign_node" {
            node.base.space_id = "beta".into();
        }
        DocNodeRepository::new(storage.clone())
            .create(node)
            .await
            .unwrap();
        let response = request(
            app(&storage, principal()),
            "GET",
            &format!("/v1/docs/{}/chunks", document.base.id),
            Value::Null,
        )
        .await;
        match case {
            "doc" | "revision" => assert_eq!(response.status(), StatusCode::FORBIDDEN),
            "foreign_revision" => assert_eq!(response.status(), StatusCode::NOT_FOUND),
            _ => {
                assert_eq!(response.status(), StatusCode::OK);
                let rows = json_body(response).await;
                assert_eq!(
                    rows.as_array().unwrap().len(),
                    usize::from(case == "allowed")
                );
                if case == "allowed" {
                    assert_eq!(rows[0]["text"], document.text);
                }
            }
        }
        let response = request(
            app(&storage, principal()),
            "GET",
            &format!("/v1/docs/{}/pipeline-status", document.base.id),
            Value::Null,
        )
        .await;
        match case {
            "doc" | "revision" => assert_eq!(response.status(), StatusCode::FORBIDDEN),
            "foreign_revision" => assert_eq!(response.status(), StatusCode::NOT_FOUND),
            _ => {
                assert_eq!(response.status(), StatusCode::OK);
                let status = json_body(response).await;
                assert_eq!(status["nodes_count"], usize::from(case == "allowed"));
                assert_eq!(status["chunks"]["total"], usize::from(case == "allowed"));
                assert!(!serde_json::to_string(&status)
                    .unwrap()
                    .contains("private body"));
            }
        }
    }
}
