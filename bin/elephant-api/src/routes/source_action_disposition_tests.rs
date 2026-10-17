#![allow(clippy::unwrap_used)]
use super::super::assertion_routes;
use super::*;
use axum::body::{to_bytes, Body};
use axum::http::Request as HttpRequest;
use chrono::Utc;
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, EntityRepository, EventRepository,
    Repository, SpaceConfigRepository, StorageClient,
};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{BaseFields, Doc, DocRevision, Entity, Event, SpaceConfig};
use elephant_types::Link;
use sha2::{Digest, Sha256};
use tower::ServiceExt;
const SPACE: &str = "action-test";
async fn fixture() -> (
    StorageClient,
    elephant_types::records::Assertion,
    Event,
    CloseActionInput,
) {
    let storage = StorageClient::new_embedded().await.unwrap();
    let mut cfg = serde_json::to_value(base(TableName::SpaceConfig)).unwrap();
    cfg.as_object_mut().unwrap().extend(serde_json::json!({"embedding_provider":"fake","embedding_model":"unused","embedding_dims":8}).as_object().unwrap().clone());
    SpaceConfigRepository::new(storage.clone())
        .create(serde_json::from_value::<SpaceConfig>(cfg).unwrap())
        .await
        .unwrap();
    let subject = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "Owner".into(),
    );
    EntityRepository::new(storage.clone())
        .create(subject.clone())
        .await
        .unwrap();
    let doc = Doc {
        base: base(TableName::Doc),
        title: "Original owner statement".into(),
        text: "I am closing this action; no further follow-up.".into(),
        sidecar_text: None,
        source: None,
        media_attachments: vec![],
        rendition_key: "owner".into(),
        metadata: serde_json::json!({"recorded_by":"identity:not-authority"}),
    };
    DocRepository::new(storage.clone())
        .create(doc.clone())
        .await
        .unwrap();
    let rev = DocRevision {
        base: base(TableName::DocRevision),
        doc: Link::new(doc.base.id.clone()),
        text: doc.text.clone(),
        text_hash: format!("{:x}", Sha256::digest(doc.text.as_bytes())),
        renderer: RendererMetadata {
            name: "original".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    DocRevisionRepository::new(storage.clone())
        .create(rev.clone())
        .await
        .unwrap();
    let span = EvidenceSpan {
        revision: Link::new(rev.base.id.clone()),
        node: None,
        start: 0,
        end: u32::try_from(doc.text.len()).unwrap(),
        quote_hash: rev.text_hash.clone(),
    };
    let mut raw = serde_json::to_value(base(TableName::Assertion)).unwrap();
    raw.as_object_mut().unwrap().extend(serde_json::json!({"subject_text":"Owner","predicate_raw":"requires_action","object_text":"Original provider request","subject_entity":subject.base.id,"evidence":span,"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"original-request"}).as_object().unwrap().clone());
    let action: elephant_types::records::Assertion = serde_json::from_value(raw).unwrap();
    Repository::create(&AssertionRepository::new(storage.clone()), action.clone())
        .await
        .unwrap();
    let event = Event::new(
        base(TableName::Event),
        "Original request timeline",
        vec![Link::new(action.base.id.clone())],
        vec![],
    )
    .unwrap();
    EventRepository::new(storage.clone())
        .create(event.clone())
        .await
        .unwrap();
    let input = CloseActionInput {
        action_id: action.base.id.clone(),
        subject_entity: subject.base.id,
        owner_statement: span,
        reason: "Closed by explicit owner disposition".into(),
        effective_at: Utc::now(),
    };
    (storage, action, event, input)
}
fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        SPACE.into(),
        "original-author".into(),
    )
}

fn principal() -> AuthPrincipal {
    AuthPrincipal {
        subject: "authenticated-owner".into(),
        space_id: Some(SPACE.into()),
        scopes: [
            "manage:action",
            "read:assertion",
            "write:assertion",
            "read:entity",
            "read:doc:raw",
            "read:source",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        security_level: 10,
        ..Default::default()
    }
}
async fn post(
    state: Arc<AssertionsState>,
    principal: AuthPrincipal,
    input: &CloseActionInput,
    extra: Option<(&str, serde_json::Value)>,
) -> (StatusCode, serde_json::Value) {
    let mut body = serde_json::to_value(input).unwrap();
    body.as_object_mut().unwrap().remove("action_id");
    if let Some((key, value)) = extra {
        body.as_object_mut().unwrap().insert(key.into(), value);
    }
    let response = assertion_routes()
        .layer(axum::Extension(principal))
        .with_state(state)
        .oneshot(
            HttpRequest::post(format!("/v1/assertions/{}/disposition", input.action_id))
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
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}
#[tokio::test]
async fn actual_action_route_requires_current_permissions_and_ignores_recorded_by() {
    let (storage, action, event, input) = fixture().await;
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    let epoch = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
        .observe_epoch(SPACE)
        .await
        .unwrap();
    for scope in [
        "manage:action",
        "read:assertion",
        "write:assertion",
        "read:entity",
        "read:doc:raw",
    ] {
        let mut denied = principal();
        denied.scopes.retain(|value| value != scope);
        assert_ne!(
            post(state.clone(), denied, &input, None).await.0,
            StatusCode::OK,
            "{scope}"
        );
    }
    assert_eq!(
        epoch,
        elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
            .observe_epoch(SPACE)
            .await
            .unwrap()
    );
    assert!(elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    let mut foreign = principal();
    foreign.space_id = Some("foreign".into());
    assert_ne!(
        post(state.clone(), foreign, &input, None).await.0,
        StatusCode::OK
    );
    assert_ne!(
        post(
            state.clone(),
            principal(),
            &input,
            Some(("actor", serde_json::json!("identity:forged")))
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, result) = post(state.clone(), principal(), &input, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["actor"], "authenticated-owner");
    assert_eq!(
        post(state.clone(), principal(), &input, None).await,
        (status, result)
    );
    assert_eq!(
        Repository::get(
            &AssertionRepository::new(storage.clone()),
            SPACE,
            &action.base.id,
        )
        .await
        .unwrap(),
        Some(action)
    );
    assert_eq!(
        EventRepository::new(storage.clone())
            .get(SPACE, &event.base.id)
            .await
            .unwrap(),
        Some(event)
    );
}
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original HTTP state and document repository through parent-security mutation, route denial and final unchanged disposition readback."
)]
async fn actual_action_route_denies_one_owner_evidence_parent() {
    let (storage, action, _, input) = fixture().await;
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    let docs = DocRepository::new(storage.clone());
    let revision = DocRevisionRepository::new(storage.clone())
        .get(SPACE, input.owner_statement.revision.id())
        .await
        .unwrap()
        .unwrap();
    let mut doc = docs.get(SPACE, revision.doc.id()).await.unwrap().unwrap();
    doc.base.security.level = 11;
    docs.update(SPACE, revision.doc.id(), doc).await.unwrap();
    assert_ne!(
        post(state, principal(), &input, None).await.0,
        StatusCode::OK
    );
    assert!(ActionDispositionRepository::new(storage)
        .current(SPACE, &action.base.id, &|_| true)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original action-route state through closure and both denied and authorized event response checks."
)]
async fn event_read_scope_alone_never_reveals_owner_disposition_reason() {
    let (storage, _, event, input) = fixture().await;
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    assert_eq!(
        post(state.clone(), principal(), &input, None).await.0,
        StatusCode::OK
    );
    let mut reader = principal();
    reader.scopes = vec!["read:event".into()];
    let visible = super::super::super::current_read::filter(
        Some(&storage),
        &state.authz,
        &reader,
        vec![event.clone()],
        "read:event",
    )
    .await
    .unwrap();
    assert_eq!(visible.len(), 1);
    assert!(visible[0]
        .metadata
        .get("recall_action_dispositions")
        .is_none());
    reader = principal();
    reader.scopes.push("read:event".into());
    let visible = super::super::super::current_read::filter(
        Some(&storage),
        &state.authz,
        &reader,
        vec![event],
        "read:event",
    )
    .await
    .unwrap();
    assert_eq!(
        visible[0].metadata["recall_action_dispositions"][0]["state"],
        "closed_by_owner"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original HTTP state and canonical epoch reader through stale target refusal and exact no-mutation assertions."
)]
async fn actual_action_close_compares_original_proposed_target_before_any_mutation() {
    let (storage, action, _, input) = fixture().await;
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone());
    let original = elephant_storage::canonical_commit::source_support_fingerprint(&action).unwrap();
    let epoch = canonical.observe_epoch(SPACE).await.unwrap();
    assert_eq!(
        post(
            state.clone(),
            principal(),
            &input,
            Some(("expected_action_fingerprint", serde_json::json!("forged")))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(canonical.observe_epoch(SPACE).await.unwrap(), epoch);
    assert!(elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    let mut changed = action.clone();
    changed.object_text = Some("Different requested task".into());
    let changed = Repository::update(
        &AssertionRepository::new(storage.clone()),
        SPACE,
        &action.base.id,
        changed,
    )
    .await
    .unwrap();
    assert_eq!(
        post(
            state.clone(),
            principal(),
            &input,
            Some(("expected_action_fingerprint", serde_json::json!(original)))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let exact = elephant_storage::canonical_commit::source_support_fingerprint(&changed).unwrap();
    assert_eq!(
        post(
            state,
            principal(),
            &input,
            Some(("expected_action_fingerprint", serde_json::json!(exact)))
        )
        .await
        .0,
        StatusCode::OK
    );
}

async fn get_proposal(
    state: Arc<AssertionsState>,
    principal: AuthPrincipal,
    action: &RecordId,
    artifact: &RecordId,
) -> (StatusCode, serde_json::Value) {
    let response = assertion_routes()
        .layer(axum::Extension(principal))
        .with_state(state)
        .oneshot(
            HttpRequest::get(format!(
                "/v1/assertions/{action}/disposition/proposals/{artifact}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original route state through forged artifact/recorded-by requests and final empty audit/work assertions."
)]
async fn actual_action_proposal_does_not_trust_raw_artifact_metadata_or_recorded_by() {
    let (storage, action, _, input) = fixture().await;
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    let proposal = elephant_pipeline::scheduler::StagedActionDispositionProposal {
        version: 1,
        authority: "untrusted_model_proposal".into(),
        target_action_id: action.base.id.clone(),
        target_fingerprint: elephant_storage::canonical_commit::source_support_fingerprint(&action)
            .unwrap(),
        subject_entity_id: input.subject_entity.clone(),
        owner_statement: input.owner_statement.clone(),
        reason: input.reason.clone(),
    };
    let artifact = elephant_types::records::work::WorkArtifact {
        base: base(TableName::WorkArtifact),
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "knowledge_candidates".into(),
        data: serde_json::json!({"_action_disposition_proposals":[proposal],"recorded_by":"identity:owner","revision_id":input.owner_statement.revision.id()}),
        status: elephant_types::records::work::ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let artifact = elephant_storage::WorkArtifactRepository::new(storage.clone())
        .create(artifact)
        .await
        .unwrap();
    let epoch = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
        .observe_epoch(SPACE)
        .await
        .unwrap();
    assert_ne!(
        get_proposal(
            state.clone(),
            principal(),
            &action.base.id,
            &artifact.base.id
        )
        .await
        .0,
        StatusCode::OK
    );
    for scope in ["read:assertion", "read:doc:raw"] {
        let mut denied = principal();
        denied.scopes.retain(|s| s != scope);
        assert_eq!(
            get_proposal(state.clone(), denied, &action.base.id, &artifact.base.id)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    let mut foreign = principal();
    foreign.space_id = Some("foreign".into());
    assert_ne!(
        get_proposal(state, foreign, &action.base.id, &artifact.base.id)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
            .observe_epoch(SPACE)
            .await
            .unwrap(),
        epoch
    );
    assert!(elephant_storage::AuditLogRepository::new(storage)
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original HTTP state through valid artifact resolution, every dependency/source denial and the existing explicit directory teardown."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered versioned-artifact acceptance and source/dependency denial scenario, including its original teardown."
)]
async fn actual_action_proposal_current_versioned_artifact_then_source_and_dependency_denial() {
    use elephant_storage::historical_adoption::*;
    use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
    use elephant_types::publication::PublicationFence;
    use elephant_types::records::work::{
        ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun,
    };
    use std::collections::BTreeMap;
    fn sorted(v: serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => serde_json::Value::Object(
                m.into_iter()
                    .map(|(k, v)| (k, sorted(v)))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            serde_json::Value::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(sorted).collect())
            }
            v => v,
        }
    }
    fn hash(value: &impl serde::Serialize) -> String {
        hex::encode(Sha256::digest(
            serde_json::to_vec(&sorted(serde_json::to_value(value).unwrap())).unwrap(),
        ))
    }
    let directory =
        std::env::temp_dir().join(format!("action-proposal-http-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let storage = StorageClient::new_with_durability(
        elephant_storage::StorageConfig {
            backend: elephant_storage::client::StorageBackendKind::ElephantDb,
            url: "mem://".into(),
            ..Default::default()
        },
        None,
        Some(directory.join("journal")),
    )
    .await
    .unwrap();
    let (receipt,doc,revision,node)=storage.with_transfer_barrier(&"b".repeat(64),move|exclusive|async move {
        exclusive.initialize_journal_identity().await?;
        let config=SpaceConfig {base:base(TableName::SpaceConfig),embedding_provider:"openai".into(),embedding_model:"fixture".into(),embedding_dims:3,purpose:None,skill:None,processing:Some(serde_json::from_value(serde_json::json!({"generation":1,"index_generation":0,"policy_id":"default","connections":{"fixture":{"id":"fixture","provider":"openai","account_id":"unused","endpoint":"https://example.test/v1","credential_ref":"UNUSED","credential_generation":1,"revoked":false}},"profiles":{"embedding":{"id":"embedding","connection_id":"fixture","model":"fixture","model_version":"fixture-v1","dimensions":3,"preprocessing":"text-full-v2"}},"bindings":{"document_embedding":{"id":"document_embedding","profile_id":"embedding","revoked":false},"query_embedding":{"id":"query_embedding","profile_id":"embedding","revoked":false}},"active_embedding_profile_id":"embedding","pending_reindex":null})).unwrap())};
        HistoricalAdoptionRepository::new(exclusive.clone()).bootstrap_configuration(&"b".repeat(64),config).await?;
        let doc=Doc {base:base(TableName::Doc),title:"Explicit owner statement".into(),text:"I close this exact action.".into(),sidecar_text:None,source:None,media_attachments:vec![],rendition_key:"original".into(),metadata:serde_json::json!({"recorded_by":"not-authority"})};
        let raw=serde_json::to_value(&doc).unwrap();
        let declaration=HistoricalDocumentInput {legacy_doc_id:doc.base.id.clone(),external_source_id:"owner-statement".into(),original_row_sha256:hash(&raw),doc_content_sha256:original_document_content_sha256(&doc)?,original_text_sha256:hex::encode(Sha256::digest(doc.text.as_bytes())),original_text_utf8_bytes:doc.text.len(),title_sha256:hex::encode(Sha256::digest(doc.title.as_bytes())),security_sha256:hash(&raw["security"]),provenance_sha256:hash(&raw["provenance"]),accepted_request_fingerprint:None,accepted_revision:None};
        let group=HistoricalSourceGroup {external_source_id:"owner-statement".into(),document_mapping_sha256:document_mapping_sha256([(&doc.base.id,declaration.original_row_sha256.as_str())])?,documents:vec![declaration]};
        // JSONL canonical hash uses the exact same serialized document/group DTO.
        let jsonl=|v:serde_json::Value| {let mut bytes=serde_json::to_vec(&sorted(v)).unwrap();bytes.push(b'\n');hex::encode(Sha256::digest(bytes))};
        let input=HistoricalAdoptionInput {source_space_id:SPACE.into(),space_id:SPACE.into(),snapshot_sha256:"b".repeat(64),idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:jsonl(serde_json::to_value(&group.documents[0]).unwrap()),groups_sha256:jsonl(serde_json::json!({"external_source_id":group.external_source_id,"documents":1,"document_mapping_sha256":group.document_mapping_sha256})),groups:vec![group],snapshot_groups:vec![]};
        let revision=DocRevision {base:base(TableName::DocRevision),doc:Link::new(doc.base.id.clone()),text:doc.text.clone(),text_hash:hex::encode(Sha256::digest(doc.text.as_bytes())),renderer:RendererMetadata {name:"original".into(),version:"1".into(),config_hash:None},manifest:None,supersedes:None,status:RevisionStatus::Active,rendered_at:Utc::now()};
        let node=elephant_types::records::DocNode {base:base(TableName::DocNode),revision:Link::new(revision.base.id.clone()),role:elephant_types::records::doc_node::NodeRole::Chunk,span:elephant_types::records::doc_node::TextSpan::new(0,u32::try_from(doc.text.len()).unwrap()),node_key:"original".into(),text:None,parent:None,position:0,depth:0,embedding_model:None,collection:None};
        DocRepository::new(exclusive.clone()).create(doc.clone()).await?;DocRevisionRepository::new(exclusive.clone()).create(revision.clone()).await?;elephant_storage::DocNodeRepository::new(exclusive.clone()).create(node.clone()).await?;
        let receipt=HistoricalAdoptionRepository::new(exclusive).prepare_authority(input)?.adopt_group("owner-statement",HistoricalGroupRows {documents:vec![HistoricalOriginalDocument {document:doc.clone(),original_document:raw,revision:revision.clone(),nodes:vec![node.clone()]}],original_sources:vec![],graph:vec![]}).await?;
        Ok((receipt,doc,revision,node))
    }).await.unwrap();
    let subject = EntityRepository::new(storage.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Owner".into(),
        ))
        .await
        .unwrap();
    let mut raw = serde_json::to_value(base(TableName::Assertion)).unwrap();
    raw.as_object_mut().unwrap().extend(serde_json::json!({"subject_text":"Owner","predicate_raw":"requires_action","object_text":"Exact original task","subject_entity":subject.base.id,"evidence":{"revision":RecordId::new_v4(TableName::DocRevision),"start":0,"end":0,"quote_hash":""},"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"original-request"}).as_object().unwrap().clone());
    let action: elephant_types::records::Assertion = serde_json::from_value(raw).unwrap();
    let action = Repository::create(&AssertionRepository::new(storage.clone()), action)
        .await
        .unwrap();
    let publication = PublicationFence {
        source_id: receipt.source_id.clone(),
        generation: receipt.operation.source_generation.clone(),
        revision_id: receipt.accepted_revision.clone(),
        operation_id: receipt.operation.operation_id.clone(),
    };
    let configuration = SpaceConfigRepository::new(storage.clone())
        .resolve_space(SPACE)
        .await
        .unwrap()
        .token;
    let plan =
        elephant_storage::source_publication_manifest::source_plan_id(&publication, &configuration);
    let payload = serde_json::json!({"_publication":publication,"_source_configuration":configuration,"_source_plan":plan});
    let item = WorkItem {
        base: base(TableName::WorkItem),
        kind: "extract_assertions".into(),
        payload: payload.clone(),
        status: WorkItemStatus::Done,
        priority: 0,
        idempotency_key: "proposal-fixture".into(),
        attempts: 1,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let item = elephant_storage::WorkItemRepository::new(storage.clone())
        .create(item)
        .await
        .unwrap();
    let run = WorkRun {
        base: base(TableName::WorkRun),
        work_item: Link::new(item.base.id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: elephant_types::records::WorkRunMetrics::default(),
        worker_id: "fixture".into(),
        worker_version: "1".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    let run = elephant_storage::WorkRunRepository::new(storage.clone())
        .create(run)
        .await
        .unwrap();
    let proposal = elephant_pipeline::scheduler::StagedActionDispositionProposal {
        version: 1,
        authority: "untrusted_model_proposal".into(),
        target_action_id: action.base.id.clone(),
        target_fingerprint: elephant_storage::canonical_commit::source_support_fingerprint(&action)
            .unwrap(),
        subject_entity_id: subject.base.id.clone(),
        owner_statement: EvidenceSpan {
            revision: Link::new(revision.base.id.clone()),
            node: Some(Link::new(node.base.id.clone())),
            start: 0,
            end: u32::try_from(doc.text.len()).unwrap(),
            quote_hash: revision.text_hash.clone(),
        },
        reason: "Owner closes the exact request".into(),
    };
    let id = elephant_pipeline::scheduler::source_stage::source_stage_artifact_id(
        SPACE,
        &plan,
        "knowledge_candidates",
        &revision.base.id,
        Some(&node.base.id),
        &run.base.id,
    )
    .unwrap();
    let mut data = payload;
    data.as_object_mut().unwrap().extend(serde_json::json!({"revision_id":revision.base.id,"node_id":node.base.id,"source_id":receipt.source_id,"_action_disposition_proposals":[proposal]}).as_object().unwrap().clone());
    let mut artifact_base = base(TableName::WorkArtifact);
    artifact_base.id = id.clone();
    artifact_base.provenance.sources = vec![receipt.source_id.clone()];
    let artifact = WorkArtifact {
        base: artifact_base,
        work_run: Link::new(run.base.id.clone()),
        artifact_type: "knowledge_candidates".into(),
        data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    elephant_storage::WorkArtifactRepository::new(storage.clone())
        .create(artifact)
        .await
        .unwrap();
    let state = Arc::new(AssertionsState::new(
        storage.clone(),
        Arc::new(crate::authz::AuthzService::new()),
    ));
    let first = get_proposal(state.clone(), principal(), &action.base.id, &id).await;
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(first.1["authority"], "untrusted_model_proposal");
    for scope in ["read:source", "read:entity"] {
        let mut denied = principal();
        denied.scopes.retain(|s| s != scope);
        assert_ne!(
            get_proposal(state.clone(), denied, &action.base.id, &id)
                .await
                .0,
            StatusCode::OK
        );
    }
    // A matching immutable artifact is insufficient when its actual producer
    // failed, has not completed, or is a different kind of work. These fixture
    // mutations preserve the same captured source/configuration and artifact.
    for (failure, successful, completed, status, kind) in [
        (
            "failed_run",
            false,
            true,
            WorkItemStatus::Done,
            "extract_assertions",
        ),
        (
            "unfinished_run",
            true,
            false,
            WorkItemStatus::Done,
            "extract_assertions",
        ),
        (
            "failed_item",
            true,
            true,
            WorkItemStatus::Failed,
            "extract_assertions",
        ),
        (
            "other_kind",
            true,
            true,
            WorkItemStatus::Done,
            "triage_source",
        ),
    ] {
        storage
            .embedded_store()
            .unwrap()
            .mutate(|tx| {
                let mut altered_run = run.clone();
                let mut altered_item = item.clone();
                altered_run.success = successful;
                if !completed {
                    altered_run.completed_at = None;
                }
                altered_item.status = status;
                altered_item.kind = kind.into();
                tx.replace(altered_run)?;
                tx.replace(altered_item)?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            get_proposal(state.clone(), principal(), &action.base.id, &id)
                .await
                .0,
            StatusCode::CONFLICT,
            "accepted {failure}"
        );
        storage
            .embedded_store()
            .unwrap()
            .mutate(|tx| {
                tx.replace(run.clone())?;
                tx.replace(item.clone())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            get_proposal(state.clone(), principal(), &action.base.id, &id)
                .await
                .0,
            StatusCode::OK
        );
    }
    // Exercise the actual archive lookup, including a corrupt live wrapper
    // carrying a deleted inner producer. Only this fixture injects that corruption.
    let moved = elephant_storage::work_retention::archive_terminal_work(
        &storage,
        Utc::now() + chrono::Duration::seconds(1),
        10,
    )
    .await
    .unwrap();
    assert_eq!((moved.items_archived, moved.runs_archived), (1, 1));
    assert_eq!(
        get_proposal(state.clone(), principal(), &action.base.id, &id)
            .await
            .0,
        StatusCode::OK
    );
    let store = storage.embedded_store().unwrap();
    let run_archive_id = RecordId::new(TableName::WorkRunArchive, run.base.id.id());
    let item_archive_id = RecordId::new(TableName::WorkItemArchive, item.base.id.id());
    let run_archive = store
        .get::<elephant_types::records::WorkRunArchive>(SPACE, &run_archive_id, false)
        .await
        .unwrap()
        .unwrap();
    let item_archive = store
        .get::<elephant_types::records::WorkItemArchive>(SPACE, &item_archive_id, false)
        .await
        .unwrap()
        .unwrap();
    for deleted_run in [true, false] {
        store
            .mutate(|tx| {
                if deleted_run {
                    let mut altered = run_archive.clone();
                    altered.record.base.deleted_at = Some(Utc::now());
                    tx.replace(altered)?;
                } else {
                    let mut altered = item_archive.clone();
                    altered.record.base.deleted_at = Some(Utc::now());
                    tx.replace(altered)?;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_ne!(
            get_proposal(state.clone(), principal(), &action.base.id, &id)
                .await
                .0,
            StatusCode::OK
        );
        store
            .mutate(|tx| {
                tx.replace(run_archive.clone())?;
                tx.replace(item_archive.clone())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            get_proposal(state.clone(), principal(), &action.base.id, &id)
                .await
                .0,
            StatusCode::OK
        );
    }
    // Retire the actual evidence source without changing the original action.
    let source = elephant_storage::SourceRepository::new(storage.clone())
        .get(SPACE, &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    SourcePublicationRepository::new(storage.clone())
        .retract(SourceMutation {
            space_id: SPACE.into(),
            source_id: receipt.source_id.clone(),
            expected: elephant_types::publication::SourcePrecondition::Generation(
                receipt.operation.source_generation,
            ),
            idempotency_key: "proposal-retirement".into(),
            request_fingerprint: "proposal-retirement".into(),
            actor: "fixture".into(),
            authorized_document: None,
            authorized: (&source).into(),
        })
        .await
        .unwrap();
    assert_ne!(
        get_proposal(state, principal(), &action.base.id, &id)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        Repository::get(
            &AssertionRepository::new(storage.clone()),
            SPACE,
            &action.base.id,
        )
        .await
        .unwrap(),
        Some(action)
    );
    assert!(elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    drop(storage);
    let _ = std::fs::remove_dir_all(directory);
}
