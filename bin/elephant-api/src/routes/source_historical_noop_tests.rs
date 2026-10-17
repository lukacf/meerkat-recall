//! Actual HTTP adapter controls over a custody-created two-document group.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::super::source_routes;
use super::*;
use axum::body::{to_bytes, Body};
use axum::http::Request as HttpRequest;
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::historical_adoption::*;
use elephant_storage::{DocRevisionRepository, StorageClient, StorageConfig, WorkItemRepository};
use elephant_types::provider_binding::{
    ModelOperation, ModelProfile, OperationBinding, ProviderConnection, SpaceProcessingConfig,
};
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::{Doc, DocRevision, SpaceConfig};
use elephant_types::Link;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const SPACE: &str = "history-test";
const EXTERNAL: &str = "homecore-two-original-documents";
struct JournalDirectory(std::path::PathBuf);
impl Drop for JournalDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}
fn digest<T: serde::Serialize>(value: &T) -> String {
    sha(&serde_json::to_vec(&canonical(serde_json::to_value(value).unwrap())).unwrap())
}
fn jsonl<T: serde::Serialize>(values: impl IntoIterator<Item = T>) -> String {
    let mut hash = Sha256::new();
    for value in values {
        hash.update(serde_json::to_vec(&canonical(serde_json::to_value(value).unwrap())).unwrap());
        hash.update(b"\n");
    }
    hex::encode(hash.finalize())
}
fn config() -> SpaceConfig {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "history-test".into(),
        "fixture".into(),
    );
    base.security.policy_id = "default".into();
    SpaceConfig {
        base,
        embedding_provider: "openai".into(),
        embedding_model: "fixture".into(),
        embedding_dims: 3,
        purpose: None,
        skill: None,
        processing: Some(SpaceProcessingConfig {
            completed_embedding_reindex: None,
            generation: 1,
            index_generation: 0,
            policy_id: "default".into(),
            connections: BTreeMap::from([(
                "fixture".into(),
                ProviderConnection {
                    id: "fixture".into(),
                    provider: "openai".into(),
                    account_id: "unused".into(),
                    endpoint: "https://example.test/v1".into(),
                    credential_ref: Some("UNUSED".into()),
                    credential_generation: 1,
                    revoked: false,
                },
            )]),
            profiles: BTreeMap::from([(
                "embedding".into(),
                ModelProfile {
                    id: "embedding".into(),
                    connection_id: "fixture".into(),
                    model: "fixture".into(),
                    model_version: "fixture-v1".into(),
                    dimensions: Some(3),
                    preprocessing: "text-full-v2".into(),
                    summary: None,
                },
            )]),
            bindings: [
                ModelOperation::DocumentEmbedding,
                ModelOperation::QueryEmbedding,
            ]
            .into_iter()
            .map(|op| {
                (
                    op,
                    OperationBinding {
                        id: format!("{op:?}"),
                        profile_id: "embedding".into(),
                        revoked: false,
                    },
                )
            })
            .collect(),
            active_embedding_profile_id: Some("embedding".into()),
            pending_reindex: None,
        }),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original storage owner through transfer initialization and its consuming move into returned HTTP state; the journal directory remains caller-owned."
)]
async fn fixture() -> (
    JournalDirectory,
    Arc<SourcesState>,
    HistoricalAdoptionReceipt,
) {
    let root =
        std::env::temp_dir().join(format!("recall-http-historical-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let directory = JournalDirectory(root);
    let storage = StorageClient::new_with_durability(
        StorageConfig {
            backend: elephant_storage::client::StorageBackendKind::ElephantDb,
            url: "mem://".into(),
            ..StorageConfig::default()
        },
        None,
        Some(directory.0.join("journal")),
    )
    .await
    .unwrap();
    let snapshot = "b".repeat(64);
    let bootstrap_snapshot = snapshot.clone();
    let receipt=storage.with_transfer_barrier(&snapshot,move |exclusive| async move {
        exclusive.initialize_journal_identity().await?;
        HistoricalAdoptionRepository::new(exclusive.clone()).bootstrap_configuration(&bootstrap_snapshot,config()).await?;
        let mut rows=vec![];
        let mut declarations=vec![];
        for ordinal in 0..2 {
            let mut base=BaseFields::new(RecordId::new_v4(TableName::Doc),SPACE.into(),"original-author".into());
            base.subjects_state=SubjectsState::Complete;
            let doc=Doc {base,title:format!("Original {ordinal}"),text:format!("Original αβ text {ordinal}\n"),sidecar_text:None,source:None,media_attachments:vec![],rendition_key:format!("original-{ordinal}"),metadata:serde_json::json!({})};
            let raw=serde_json::to_value(&doc).unwrap();
            declarations.push(HistoricalDocumentInput {legacy_doc_id:doc.base.id.clone(),external_source_id:EXTERNAL.into(),original_row_sha256:digest(&raw),doc_content_sha256:original_document_content_sha256(&doc)?,original_text_sha256:sha(doc.text.as_bytes()),original_text_utf8_bytes:doc.text.len(),title_sha256:sha(doc.title.as_bytes()),security_sha256:digest(&raw["security"]),provenance_sha256:digest(&raw["provenance"]),accepted_request_fingerprint:None,accepted_revision:None});
            let revision=DocRevision {base:BaseFields::new(RecordId::new_v4(TableName::DocRevision),SPACE.into(),"original-renderer".into()),doc:Link::new(doc.base.id.clone()),text:doc.text.clone(),text_hash:sha(doc.text.as_bytes()),renderer:RendererMetadata{name:"original".into(),version:"retained".into(),config_hash:None},manifest:None,supersedes:None,status:RevisionStatus::Active,rendered_at:chrono::Utc::now()};
            DocRepository::new(exclusive.clone()).create(doc.clone()).await?;
            DocRevisionRepository::new(exclusive.clone()).create(revision.clone()).await?;
            rows.push(HistoricalOriginalDocument {document:doc,original_document:raw,revision,nodes:vec![]});
        }
        declarations.sort_by_key(|doc|doc.legacy_doc_id.to_string());
        let group=HistoricalSourceGroup {external_source_id:EXTERNAL.into(),document_mapping_sha256:document_mapping_sha256(declarations.iter().map(|doc|(&doc.legacy_doc_id,doc.original_row_sha256.as_str())))?,documents:declarations};
        let input=HistoricalAdoptionInput {source_space_id:SPACE.into(),space_id:SPACE.into(),snapshot_sha256:bootstrap_snapshot,idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:jsonl(&group.documents),groups_sha256:jsonl([serde_json::json!({"external_source_id":group.external_source_id,"documents":group.documents.len(),"document_mapping_sha256":group.document_mapping_sha256})]),groups:vec![group],snapshot_groups:vec![]};
        HistoricalAdoptionRepository::new(exclusive).prepare_authority(input)?.adopt_group(EXTERNAL,HistoricalGroupRows {documents:rows,original_sources:vec![],graph:vec![]}).await
    }).await.unwrap();
    let state = Arc::new(SourcesState::new(
        storage,
        Arc::new(crate::authz::AuthzService::new()),
    ));
    (directory, state, receipt)
}
fn principal() -> AuthPrincipal {
    AuthPrincipal {
        subject: "migration-owner".into(),
        space_id: Some(SPACE.into()),
        scopes: [
            "read:source",
            "write:source",
            "read:doc:metadata",
            "write:doc",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        security_level: 10,
        ..AuthPrincipal::default()
    }
}
fn body(receipt: &HistoricalAdoptionReceipt) -> Value {
    serde_json::json!({"external_source_id":receipt.external_source_id,"expected_generation":receipt.operation.source_generation,"accepted_revision":receipt.accepted_revision,"accepted_request_fingerprint":receipt.accepted_request_fingerprint,"original_commit_id":receipt.commit_id,"document_content_sha256":receipt.document_content_sha256})
}
async fn post(
    state: Arc<SourcesState>,
    principal: AuthPrincipal,
    id: &RecordId,
    body: Value,
) -> (StatusCode, Value) {
    let app = source_routes()
        .layer(axum::Extension(principal))
        .with_state(state);
    let response = app
        .oneshot(
            HttpRequest::post(format!("/v1/sources/{id}/historical-unchanged"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn epoch(state: &SourcesState) -> Option<String> {
    CanonicalRepository::new(state.storage.clone())
        .observe_epoch(SPACE)
        .await
        .unwrap()
}
async fn no_work(state: &SourcesState) {
    assert!(WorkItemRepository::new(state.storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn historical_noop_http_exact_group_is_read_only_and_denies_missing_scopes() {
    let (_directory, state, receipt) = fixture().await;
    let before = epoch(&state).await;
    let (status, response) = post(
        state.clone(),
        principal(),
        &receipt.source_id,
        body(&receipt),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["unchanged"], true);
    assert_eq!(
        response["original_adoption"],
        serde_json::to_value(&receipt).unwrap()
    );
    for scope in [
        "read:source",
        "write:source",
        "read:doc:metadata",
        "write:doc",
    ] {
        let mut denied = principal();
        denied.scopes.retain(|s| s != scope);
        assert_eq!(
            post(state.clone(), denied, &receipt.source_id, body(&receipt))
                .await
                .0,
            StatusCode::FORBIDDEN,
            "{scope}"
        );
    }
    let mut foreign = principal();
    foreign.space_id = Some("foreign".into());
    assert_eq!(
        post(state.clone(), foreign, &receipt.source_id, body(&receipt))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            state.clone(),
            principal(),
            &RecordId::new_v4(TableName::Source),
            body(&receipt)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(epoch(&state).await, before);
    no_work(&state).await;
}

#[tokio::test]
async fn historical_noop_http_changed_partial_and_invalid_groups_never_enqueue() {
    let (_directory, state, receipt) = fixture().await;
    let before = epoch(&state).await;
    let mut changed = body(&receipt);
    let first = receipt
        .document_content_sha256
        .keys()
        .next()
        .unwrap()
        .to_string();
    changed["document_content_sha256"][&first] = "0".repeat(64).into();
    assert_eq!(
        post(state.clone(), principal(), &receipt.source_id, changed)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut partial = body(&receipt);
    partial["document_content_sha256"]
        .as_object_mut()
        .unwrap()
        .remove(&first);
    assert_eq!(
        post(state.clone(), principal(), &receipt.source_id, partial)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut invalid = body(&receipt);
    invalid["document_content_sha256"] =
        serde_json::json!({receipt.source_id.to_string():"a".repeat(64)});
    assert_eq!(
        post(state.clone(), principal(), &receipt.source_id, invalid)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut empty = body(&receipt);
    empty["document_content_sha256"] = serde_json::json!({});
    assert_eq!(
        post(state.clone(), principal(), &receipt.source_id, empty)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut forged = body(&receipt);
    forged["accepted_request_fingerprint"] = "forged".into();
    assert_eq!(
        post(state.clone(), principal(), &receipt.source_id, forged)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(epoch(&state).await, before);
    no_work(&state).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original document reader and HTTP fixture through security mutation, recaptured-epoch refusal, route denial and final no-work assertion."
)]
async fn historical_noop_authorized_member_and_epoch_changes_are_rechecked() {
    let (_directory, state, receipt) = fixture().await;
    let request = authorize_request(
        &state,
        &principal(),
        &receipt.source_id.to_string(),
        serde_json::from_value(body(&receipt)).unwrap(),
    )
    .await
    .unwrap();
    let id = receipt.document_content_sha256.keys().next().unwrap();
    let repository = DocRepository::new(state.storage.clone());
    let mut doc = repository.get(SPACE, id).await.unwrap().unwrap();
    doc.base.security.level = 11;
    repository.update(SPACE, id, doc).await.unwrap();
    assert!(HistoricalAdoptionRepository::new(state.storage.clone())
        .verify_unchanged(&request)
        .await
        .is_err());
    // Even a recaptured epoch cannot substitute for the exact previously
    // authorized document image; every member, not a representative, is bound.
    let mut changed_epoch = request.clone();
    changed_epoch.authorization_epoch = epoch(&state).await;
    assert!(HistoricalAdoptionRepository::new(state.storage.clone())
        .verify_unchanged(&changed_epoch)
        .await
        .is_err());
    assert_eq!(
        post(
            state.clone(),
            principal(),
            &receipt.source_id,
            body(&receipt)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    no_work(&state).await;
}

#[tokio::test]
async fn historical_noop_authorization_epoch_rejects_unrelated_canonical_mutation() {
    let (_directory, state, receipt) = fixture().await;
    let request = authorize_request(
        &state,
        &principal(),
        &receipt.source_id.to_string(),
        serde_json::from_value(body(&receipt)).unwrap(),
    )
    .await
    .unwrap();
    let entity = elephant_types::records::Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "owner".into(),
        ),
        elephant_types::records::entity::EntityType::new("person"),
        "Concurrent entity".into(),
    );
    elephant_storage::EntityRepository::new(state.storage.clone())
        .create(entity)
        .await
        .unwrap();
    assert_ne!(epoch(&state).await, request.authorization_epoch);
    assert!(HistoricalAdoptionRepository::new(state.storage.clone())
        .verify_unchanged(&request)
        .await
        .is_err());
    // A fresh authorization against the still-identical group remains valid.
    assert_eq!(
        post(
            state.clone(),
            principal(),
            &receipt.source_id,
            body(&receipt)
        )
        .await
        .0,
        StatusCode::OK
    );
    no_work(&state).await;
}
