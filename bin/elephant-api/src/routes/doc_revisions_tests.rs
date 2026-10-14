#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use super::*;
use axum::{body::to_bytes, http::StatusCode};
use elephant_storage::{StorageClient, StorageConfig};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::{Doc, SpaceConfig, SubjectsState};
use elephant_types::Link;
use sha2::{Digest, Sha256};

fn base(table: TableName) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        "revision-read".into(),
        "fixture".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    base
}

fn revision(doc: &Doc) -> DocRevision {
    DocRevision {
        base: base(TableName::DocRevision),
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
    }
}

fn document() -> Doc {
    Doc {
        base: base(TableName::Doc),
        title: "Exact immutable inspection".into(),
        text: "Exact UTF-8 é.\n".into(),
        sidecar_text: None,
        source: None,
        media_attachments: vec![],
        rendition_key: "fixture".into(),
        metadata: serde_json::json!({}),
    }
}

fn principal() -> Principal {
    Principal {
        subject: "reader".into(),
        space_id: Some("revision-read".into()),
        scopes: vec!["read:doc".into(), "read:doc:raw".into()],
        ..Principal::default()
    }
}

#[test]
fn path_identity_cannot_select_a_different_record_table() {
    let doc = RecordId::new_v4(TableName::Doc);
    assert_eq!(typed_id(&doc.to_string(), TableName::Doc).unwrap(), doc);
    assert_eq!(
        typed_id(&doc.to_string(), TableName::DocRevision)
            .unwrap_err()
            .0
            .error_code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        typed_id("invalid", TableName::Doc)
            .unwrap_err()
            .0
            .error_code,
        ErrorCode::InvalidRequest
    );
}

#[test]
fn encoded_revision_is_lossless_and_bounds_escaped_text_plus_full_envelope() {
    let mut revision = revision(&document());
    revision.text = "\0".repeat(MAX_SOURCE_TEXT_BYTES);
    let encoded = encode_revision(&revision).unwrap();
    assert!(encoded.len() > MAX_SOURCE_TEXT_BYTES * 6);
    assert_eq!(
        serde_json::from_slice::<DocRevision>(&encoded).unwrap(),
        revision
    );
    revision.renderer.name = "x".repeat(65_536);
    let error = encode_revision(&revision).unwrap_err();
    assert_eq!(error.0.error_code, ErrorCode::PayloadTooLarge);
    assert!(!error.0.retryable);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture ownership, future layout, and oracle sequence."
)]
async fn intrinsic_revision_read(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        StorageClient::new(StorageConfig {
            database: format!("revision_read_{}", uuid::Uuid::new_v4().simple()),
            ..StorageConfig::default()
        })
        .await
        .unwrap()
    };
    elephant_migrations::apply_schema(&storage).await.unwrap();
    let state = Arc::new(DocsState::new(
        &storage,
        Arc::new(crate::authz::AuthzService::new()),
    ));
    state
        .space_config_repo
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 3,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let doc = state.doc_repo.create(document()).await.unwrap();
    let revision = state.revision_repo.create(revision(&doc)).await.unwrap();
    let path = (doc.base.id.to_string(), revision.base.id.to_string());
    let response = get_doc_revision(
        State(state.clone()),
        Extension(principal()),
        Path(path.clone()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), MAX_REVISION_RESPONSE_BYTES)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<DocRevision>(&body).unwrap(),
        revision
    );
    let mut metadata_only = principal();
    metadata_only.scopes = vec!["read:doc".into()];
    assert_eq!(
        get_doc_revision(
            State(state.clone()),
            Extension(metadata_only),
            Path(path.clone())
        )
        .await
        .unwrap_err()
        .0
        .error_code,
        ErrorCode::ForbiddenSpace
    );
    let mut foreign = principal();
    foreign.space_id = Some("foreign".into());
    let mut foreign_config = state
        .space_config_repo
        .get_by_space_id("revision-read")
        .await
        .unwrap()
        .unwrap();
    foreign_config.base = base(TableName::SpaceConfig);
    foreign_config.base.space_id = "foreign".into();
    state
        .space_config_repo
        .create(foreign_config)
        .await
        .unwrap();
    assert_eq!(
        get_doc_revision(State(state.clone()), Extension(foreign), Path(path.clone()))
            .await
            .unwrap_err()
            .0
            .error_code,
        ErrorCode::NotFound
    );
    for case in ["level", "subjects", "hash", "superseded", "parent"] {
        let mut changed = revision.clone();
        match case {
            "level" => changed.base.security.level = 7,
            "subjects" => changed.base.subjects_state = SubjectsState::Unknown,
            "hash" => changed.text.push_str("tampered"),
            "superseded" => changed.status = RevisionStatus::Superseded,
            _ => changed.doc = Link::new(RecordId::new_v4(TableName::Doc)),
        }
        state
            .revision_repo
            .update("revision-read", &revision.base.id, changed)
            .await
            .unwrap();
        let mut reader = principal();
        if case == "subjects" {
            reader.subject_allowlist = Some(vec![]);
        }
        let result =
            get_doc_revision(State(state.clone()), Extension(reader), Path(path.clone())).await;
        assert!(result.is_err(), "{case} must not expose revision content");
        state
            .revision_repo
            .update("revision-read", &revision.base.id, revision.clone())
            .await
            .unwrap();
    }
    let mut changed_doc = doc.clone();
    changed_doc.base.security.level = 7;
    state
        .doc_repo
        .update("revision-read", &doc.base.id, changed_doc)
        .await
        .unwrap();
    assert_eq!(
        get_doc_revision(State(state), Extension(principal()), Path(path))
            .await
            .unwrap_err()
            .0
            .error_code,
        ErrorCode::ForbiddenSpace
    );
}

#[tokio::test]
async fn embedded_intrinsic_revision_requires_both_original_record_envelopes() {
    intrinsic_revision_read(true).await;
}

#[tokio::test]
async fn native_intrinsic_revision_requires_both_original_record_envelopes() {
    intrinsic_revision_read(false).await;
}
