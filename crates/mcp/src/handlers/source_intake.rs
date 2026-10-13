//! Authorization and acceptance shared by retained write adapters.

use elephant_pipeline::source_queue::enqueue_accepted_source;
use elephant_storage::source_intake::{AcceptedSourceIntake, IntakeControl, PreparedSourceIntake};
use elephant_storage::{find_existing_doc_for_doc, StorageError};
use elephant_types::publication::PublicationFence;
use elephant_types::records::{Doc, IngestMode, Source};
use serde_json::{json, Value};

use super::ToolContext;
use crate::error::{McpError, McpResult};

#[expect(
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) fn public_source(principal: &elephant_policy::Principal, source: Source) -> Source {
    let mut source = elephant_storage::source_intake::public_source_projection(source);
    if !principal.scopes.contains("read:source:raw") && !principal.scopes.contains("*") {
        source.uri = None;
        source.path = None;
    }
    source
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) fn intake_control(params: &Value) -> McpResult<IntakeControl> {
    IntakeControl::from_params(params)
        .map_err(|error| McpError::InvalidParameters(error.to_string()))
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) fn intake_schema() -> Value {
    json!({"type":"object","required":[],"additionalProperties":false,"properties":{
        "revision_id":{"type":"string","minLength":1,"maxLength":1024},
        "idempotency_key":{"type":"string","minLength":1,"maxLength":1024},
        "expected_generation":{"type":"string","minLength":1,"maxLength":1024}
    }})
}

enum IntakeIntent {
    New,
    Restore,
    Reprocess(PublicationFence),
}

#[expect(
    clippy::large_futures,
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) async fn accept_intake(
    ctx: &ToolContext,
    source: Source,
    doc: Option<&Doc>,
    control: IntakeControl,
    assets: &[elephant_types::records::MediaAsset],
) -> McpResult<AcceptedSourceIntake> {
    accept_intake_mode(ctx, source, doc, control, assets, IntakeIntent::New, None).await
}

#[expect(
    clippy::large_futures,
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) async fn reprocess_intake(
    ctx: &ToolContext,
    mut source: Source,
    document: &Doc,
    original: &PublicationFence,
    control: IntakeControl,
) -> McpResult<AcceptedSourceIntake> {
    source.set_requested_ingest_mode(IngestMode::Graph);
    accept_intake_mode(
        ctx,
        source,
        None,
        control,
        &[],
        IntakeIntent::Reprocess(original.clone()),
        Some(document.clone()),
    )
    .await
}

#[expect(
    clippy::large_futures,
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) async fn restore_intake(
    ctx: &ToolContext,
    mut source: Source,
    control: IntakeControl,
    authorized_document: Option<&Doc>,
) -> McpResult<AcceptedSourceIntake> {
    source.base.deleted_at = None;
    accept_intake_mode(
        ctx,
        source,
        None,
        control,
        &[],
        IntakeIntent::Restore,
        authorized_document.cloned(),
    )
    .await
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep authorization and prepared acceptance in their existing ordered operation"
)]
async fn accept_intake_mode(
    ctx: &ToolContext,
    mut source: Source,
    doc: Option<&Doc>,
    control: IntakeControl,
    assets: &[elephant_types::records::MediaAsset],
    intent: IntakeIntent,
    mut authorized_document: Option<Doc>,
) -> McpResult<AcceptedSourceIntake> {
    let reactivate = matches!(&intent, IntakeIntent::Restore);
    let denied = |error| McpError::ExecutionError(format!("Access denied: {error}"));
    if source.base.space_id != ctx.principal.space_id || ctx.principal.space_id.trim().is_empty() {
        return Err(McpError::InvalidParameters(
            "source must belong to selected space".into(),
        ));
    }
    if let Some(old) = &authorized_document {
        ctx.authz
            .authorize_record_read(&ctx.principal, old, "read:doc:metadata")
            .map_err(denied)?;
        ctx.authz
            .authorize_record_write(&ctx.principal, old, "write:doc")
            .map_err(denied)?;
    }
    if let Some(doc) = doc {
        ctx.authz
            .authorize_record_read(&ctx.principal, doc, "read:doc:metadata")
            .map_err(denied)?;
        ctx.authz
            .authorize_record_write(&ctx.principal, doc, "write:doc")
            .map_err(denied)?;
        if let Some(old) = find_existing_doc_for_doc(&ctx.storage, doc)
            .await
            .map_err(storage_error)?
        {
            ctx.authz
                .authorize_record_read(&ctx.principal, &old, "read:doc:metadata")
                .map_err(denied)?;
            ctx.authz
                .authorize_record_write(&ctx.principal, &old, "write:doc")
                .map_err(denied)?;
            authorized_document = Some(old);
        }
        if source.base.space_id != doc.base.space_id
            || source.base.security != doc.base.security
            || source.base.subjects != doc.base.subjects
            || source.base.subjects_state != doc.base.subjects_state
        {
            return Err(McpError::InvalidParameters(
                "source/document security envelope mismatch".into(),
            ));
        }
        source
            .set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(doc))
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
    }
    if reactivate && control.expected_generation.is_none() {
        return Err(McpError::InvalidParameters(
            "restore requires intake.expected_generation".into(),
        ));
    }
    let prepared = PreparedSourceIntake::prepare_with_assets(&ctx.storage, source, control, assets)
        .await
        .map_err(storage_error)?;
    if let Some(old) = &prepared.previous {
        ctx.authz
            .authorize_record_read(&ctx.principal, old, "read:source")
            .map_err(denied)?;
        ctx.authz
            .authorize_record_write(&ctx.principal, old, "write:source")
            .map_err(denied)?;
    }
    ctx.authz
        .authorize_record_read(&ctx.principal, &prepared.source, "read:source")
        .map_err(denied)?;
    ctx.authz
        .authorize_record_write(&ctx.principal, &prepared.source, "write:source")
        .map_err(denied)?;
    for previous in &prepared.previous_documents {
        ctx.authz
            .authorize_record_read(&ctx.principal, previous, "read:doc:metadata")
            .map_err(denied)?;
        ctx.authz
            .authorize_record_write(&ctx.principal, previous, "write:doc")
            .map_err(denied)?;
    }
    if let Some(doc) = &prepared.document {
        ctx.authz
            .authorize_record_read(&ctx.principal, doc, "read:doc:metadata")
            .map_err(denied)?;
        ctx.authz
            .authorize_record_write(&ctx.principal, doc, "write:doc")
            .map_err(denied)?;
    }
    let prepared = match authorized_document {
        Some(document) => prepared.with_authorized_document(document),
        None => prepared,
    };
    let prepared = if reactivate {
        prepared.reactivate()
    } else {
        prepared
    };
    if let Some(receipt) = prepared
        .read_only_contact_retry(&ctx.storage)
        .await
        .map_err(storage_error)?
    {
        return Ok(receipt);
    }
    let contact = match &intent {
        IntakeIntent::Reprocess(original) => prepared
            .prepare_reprocess_contact(&ctx.storage, original)
            .await
            .map_err(storage_error)?,
        IntakeIntent::Restore => prepared
            .prepare_restore_contact(&ctx.storage)
            .await
            .map_err(storage_error)?,
        IntakeIntent::New => prepared
            .prepare_replacement_contact(&ctx.storage)
            .await
            .map_err(storage_error)?,
    };
    if let Some(contact) = &contact {
        for row in contact.authorizations() {
            ctx.authz
                .authorize_record_read(&ctx.principal, row, row.read_scope())
                .map_err(denied)?;
            ctx.authz
                .authorize_record_write(&ctx.principal, row, row.write_scope())
                .map_err(denied)?;
        }
    }
    match &contact {
        Some(contact) => {
            prepared
                .accept_with_prepared_contact(&ctx.storage, contact)
                .await
        }
        None => prepared.accept(&ctx.storage).await,
    }
    .map_err(storage_error)
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) async fn retract_intake(
    ctx: &ToolContext,
    source: &Source,
    control: IntakeControl,
    authorized_document: Option<&Doc>,
) -> McpResult<elephant_types::publication::OperationReceipt> {
    if source.base.space_id != ctx.principal.space_id {
        return Err(McpError::InvalidParameters(
            "source must belong to selected space".into(),
        ));
    }
    ctx.authz
        .authorize_record_read(&ctx.principal, source, "read:source")
        .map_err(|error| McpError::ExecutionError(format!("Access denied: {error}")))?;
    ctx.authz
        .authorize_record_write(&ctx.principal, source, "write:source")
        .map_err(|error| McpError::ExecutionError(format!("Access denied: {error}")))?;
    let prepared = elephant_storage::source_intake::PreparedSourceRetraction::prepare(
        &ctx.storage,
        source,
        control,
        "mcp.source_intake",
        authorized_document,
    )
    .await
    .map_err(storage_error)?;
    for document in &prepared.documents {
        ctx.authz
            .authorize_record_read(&ctx.principal, document, "read:doc:metadata")
            .map_err(|error| McpError::ExecutionError(format!("Access denied: {error}")))?;
        ctx.authz
            .authorize_record_write(&ctx.principal, document, "write:doc")
            .map_err(|error| McpError::ExecutionError(format!("Access denied: {error}")))?;
    }
    prepared.accept(&ctx.storage).await.map_err(storage_error)
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "Retain the explicit crate-only intake API and its original future layout"
)]
pub(crate) async fn enqueue_intake(
    ctx: &ToolContext,
    accepted: &AcceptedSourceIntake,
) -> Option<elephant_types::RecordId> {
    if !accepted.needs_processing() {
        return None;
    }
    match enqueue_accepted_source(
        &ctx.storage,
        &accepted.source,
        &accepted.publication,
        "mcp.source_intake",
    )
    .await
    {
        Ok(id) => Some(id),
        Err(error) => {
            tracing::warn!(operation_id = %accepted.operation.operation_id, %error,
                "source accepted; queue handoff awaits durable recovery");
            None
        }
    }
}

#[expect(
    clippy::missing_const_for_fn,
    clippy::redundant_pub_crate,
    reason = "Retain the crate-only error conversion API without changing const availability"
)]
pub(crate) fn storage_error(error: StorageError) -> McpError {
    McpError::StorageError(error)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::authz::AuthzService;
    use crate::handlers::{IngestDocHandler, ToolHandler};
    use elephant_storage::{
        DocRepository, Repository, SourcePublicationRepository, SpaceConfigRepository,
        WorkItemRepository,
    };
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::SpaceConfig;
    use elephant_types::{RecordId, TableName};
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn retained_intake_preserves_typed_erasure_refusal() {
        assert!(matches!(
            storage_error(StorageError::ErasureInProgress),
            McpError::StorageError(StorageError::ErasureInProgress)
        ));
        assert!(matches!(
            storage_error(StorageError::Conflict("generation changed".into())),
            McpError::StorageError(StorageError::Conflict(_))
        ));
    }

    async fn context(skip_auth: bool) -> ToolContext {
        let storage = elephant_storage::StorageClient::new_embedded()
            .await
            .expect("store");
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "household".into(),
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
            .expect("space");
        let principal = elephant_policy::Principal {
            space_id: "household".into(),
            scopes: if skip_auth {
                HashSet::from(["*".to_owned()])
            } else {
                HashSet::new()
            },
            clearances: vec![],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(skip_auth)),
            principal,
        )
    }

    fn request(text: &str) -> Value {
        json!({"doc":{
            "id":"doc:11111111-1111-4111-8111-111111111111", "title":"Household note", "text":text,
            "security":{"policy_id":"default","level":3,"labels":["family"],"handling":["private"]},
            "subjects":[],"subjects_state":"partial",
            "metadata":{"extraction_skill":"extraction/household","extraction_purpose":"explicit facts"}
        }})
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn source_write_response_excludes_private_template_and_raw_locations() {
        let mut ctx = context(true).await;
        ctx.principal.scopes = ["read:source", "write:source"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "household".into(),
                "fixture".into(),
            ),
            elephant_types::records::SourceType::Manual,
            "private-input-hash".into(),
        );
        source.uri = Some("https://example.invalid/private".into());
        source.path = Some("/private/input".into());
        source.metadata = json!({"public_note":"retained"});
        source
            .set_manual_doc_template(&elephant_types::records::ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Private input".into(),
                text: "cHJpdmF0ZS1tZWRpYS1ieXRlcw==".into(),
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "private-rendition".into(),
                metadata: json!({"extraction_purpose":"private guidance"}),
            })
            .expect("template");
        let id = source.base.id.clone();
        let handler = crate::handlers::sources::CreateSourceHandler::new(ctx.storage.clone());
        let result = handler
            .execute(&ctx, json!({"source":source}))
            .await
            .expect("accept");
        assert_eq!(result["ok"], true);
        assert!(result["data"]["metadata"].get("_manual_doc").is_none());
        assert_eq!(result["data"]["metadata"]["public_note"], "retained");
        assert!(result["data"]["uri"].is_null());
        assert!(result["data"]["path"].is_null());
        assert!(!result.to_string().contains("cHJpdmF0ZS1tZWRpYS1ieXRlcw=="));
        let stored = elephant_storage::SourceRepository::new(ctx.storage.clone())
            .get("household", &id)
            .await
            .expect("read original")
            .expect("original");
        assert_eq!(
            stored
                .manual_doc_template()
                .expect("template")
                .expect("private input")
                .text,
            "cHJpdmF0ZS1tZWRpYS1ieXRlcw=="
        );
        ctx.principal.scopes.insert("read:source:raw".into());
        let template = stored
            .manual_doc_template()
            .expect("template")
            .expect("input");
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "household".into(),
                "fixture".into(),
            ),
            title: template.title,
            text: template.text.clone(),
            sidecar_text: None,
            source: Some(elephant_types::Link::new(id)),
            media_attachments: vec![],
            rendition_key: template.rendition_key,
            metadata: template.metadata,
        };
        assert!(crate::redaction::redact_doc(&ctx.principal, doc.clone())
            .data
            .text
            .is_empty());
        ctx.principal.scopes.insert("read:doc:raw".into());
        assert_eq!(
            crate::redaction::redact_doc(&ctx.principal, doc).data.text,
            template.text
        );
        let raw_source = public_source(&ctx.principal, stored);
        assert!(raw_source.uri.is_some());
        assert!(raw_source.metadata.get("_manual_doc").is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn retained_ingest_doc_enqueues_original_authority_and_preserves_immutable_history() {
        let ctx = context(true).await;
        let handler = IngestDocHandler::new(ctx.storage.clone());
        let first = handler
            .execute(&ctx, request("First version"))
            .await
            .expect("accept");
        let operation: elephant_types::publication::OperationReceipt =
            serde_json::from_value(first["operation"].clone()).expect("receipt");
        let work_id: RecordId =
            serde_json::from_value(first["work_item_id"].clone()).expect("work ID");
        let work = WorkItemRepository::new(ctx.storage.clone())
            .get("household", &work_id)
            .await
            .expect("work")
            .expect("queued");
        let fence: elephant_types::publication::PublicationFence =
            serde_json::from_value(work.payload["_publication"].clone()).expect("original fence");
        assert_eq!(fence.operation_id, operation.operation_id);
        assert_eq!(fence.generation, operation.source_generation);
        assert_eq!(work.base.security.handling, ["private"]);
        assert_eq!(
            work.base.subjects_state,
            elephant_types::records::base::SubjectsState::Partial
        );
        assert!(work.payload.get("_attempt_fence").is_none());
        let replay = handler
            .execute(&ctx, request("First version"))
            .await
            .expect("retry");
        assert_eq!(first["operation"], replay["operation"]);
        let old_doc: Doc = serde_json::from_value(first["data"]["data"].clone()).expect("old doc");
        let mut next = request("Second version");
        next["intake"] = json!({"revision_id":"second","idempotency_key":"second-key","expected_generation":operation.source_generation});
        let second = handler.execute(&ctx, next).await.expect("replace");
        let next_doc: Doc =
            serde_json::from_value(second["data"]["data"].clone()).expect("new doc");
        assert_ne!(old_doc.base.id, next_doc.base.id);
        let persisted_old = DocRepository::new(ctx.storage.clone())
            .get("household", &old_doc.base.id)
            .await
            .expect("old row")
            .expect("retained");
        assert_eq!(persisted_old.text, "First version");
        assert_eq!(
            next_doc.metadata["extraction_skill"],
            "extraction/household"
        );
    }

    async fn accepted_contact_input(ctx: &ToolContext, sealed: bool) -> AcceptedSourceIntake {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "household".into(),
                "fixture".into(),
            ),
            elephant_types::records::SourceType::Manual,
            "contact-input".into(),
        );
        source.metadata =
            json!({"contact":{"name":"Accepted Person","email":"person@example.invalid"}});
        source.set_requested_ingest_mode(IngestMode::Metadata);
        source
            .set_manual_doc_template(&elephant_types::records::ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Contact input".into(),
                text: "Accepted Person".into(),
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "original-input".into(),
                metadata: json!({"extraction_purpose":"preserve original input"}),
            })
            .unwrap();
        let prepared =
            PreparedSourceIntake::prepare(&ctx.storage, source, IntakeControl::default())
                .await
                .unwrap();
        if sealed {
            let contact = prepared
                .prepare_contact(&ctx.storage)
                .await
                .unwrap()
                .unwrap();
            prepared
                .accept_with_prepared_contact(&ctx.storage, &contact)
                .await
                .unwrap()
        } else {
            prepared.accept(&ctx.storage).await.unwrap()
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn unavailable_original_contact_retry_authorizes_then_returns_without_writes() {
        let mut ctx = context(true).await;
        // Seed actual admission capability, then admit the operation under
        // test through the same retained handler which receives its retry.
        let seed = accepted_contact_input(&ctx, true).await;
        let handler = crate::handlers::sources::CreateSourceHandler::new(ctx.storage.clone());
        let request = json!({"source": seed.source, "intake": {
            "revision_id": "public-original", "idempotency_key": "public-original",
            "expected_generation": seed.publication.generation,
        }});
        let admitted = handler.execute(&ctx, request.clone()).await.unwrap();
        assert_eq!(admitted["ok"], true);
        let first: elephant_types::publication::OperationReceipt =
            serde_json::from_value(admitted["operation"].clone()).unwrap();
        let work_id: RecordId = serde_json::from_value(admitted["work_item_id"].clone()).unwrap();
        let work = WorkItemRepository::new(ctx.storage.clone())
            .get("household", &work_id)
            .await
            .unwrap()
            .unwrap();
        let publication: PublicationFence =
            serde_json::from_value(work.payload["_publication"].clone()).unwrap();
        let repository = SourcePublicationRepository::new(ctx.storage.clone());
        assert!(repository
            .accepted_contact_for_publication("household", &publication)
            .await
            .unwrap()
            .is_some());
        let revision = repository
            .revision(
                "household",
                &publication.source_id,
                &publication.revision_id,
            )
            .await
            .unwrap()
            .unwrap();
        let template = revision.source.manual_doc_template().unwrap().unwrap();
        let doc_id =
            elephant_storage::source_intake::accepted_document_id(&publication, &template.doc_id)
                .unwrap();
        let store = ctx.storage.embedded_store().unwrap();
        store
            .mutate(|tx| {
                let rows: Vec<_> = tx
                    .scan_extension_rows("source_contact_intent")?
                    .into_iter()
                    .filter(|(_, row)| {
                        let payload: Value =
                            serde_json::from_str(row["payload"].as_str().unwrap()).unwrap();
                        payload["publication"]["operation_id"] == first.operation_id
                    })
                    .collect();
                assert_eq!(rows.len(), 1);
                tx.hard_delete_extension_row("source_contact_intent", &rows[0].0, "household")
                    .map_err(Into::into)
            })
            .await
            .unwrap();
        ctx.authz = Arc::new(AuthzService::new_with_skip_auth(false));
        let required = [
            "read:source",
            "write:source",
            "read:doc:metadata",
            "write:doc",
        ];
        ctx.principal.clearances = vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: 0,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }];
        let before: Value =
            serde_json::from_slice(&store.export_snapshot_bytes().await.unwrap()).unwrap();
        for missing in required {
            ctx.principal.scopes = required
                .into_iter()
                .filter(|scope| *scope != missing)
                .map(str::to_owned)
                .collect();
            let response = handler.execute(&ctx, request.clone()).await;
            assert!(
                response.is_err() || response.unwrap()["ok"] == false,
                "receipt bypassed missing scope {missing}"
            );
            let after: Value =
                serde_json::from_slice(&store.export_snapshot_bytes().await.unwrap()).unwrap();
            assert_eq!(before["tables"], after["tables"]);
        }
        ctx.principal.scopes = required.into_iter().map(str::to_owned).collect();
        let docs = DocRepository::new(ctx.storage.clone());
        let mut protected = docs.get("household", &doc_id).await.unwrap().unwrap();
        protected.base.security.level = 1;
        docs.update("household", &doc_id, protected).await.unwrap();
        assert!(
            handler.execute(&ctx, request.clone()).await.is_err(),
            "current Doc ABAC still applies to receipt retries"
        );
        docs.hard_delete("household", &doc_id).await.unwrap();
        let before: Value =
            serde_json::from_slice(&store.export_snapshot_bytes().await.unwrap()).unwrap();
        let response = handler.execute(&ctx, request.clone()).await.unwrap();
        assert_eq!(response["ok"], true);
        assert_eq!(response["operation"], serde_json::to_value(&first).unwrap());
        assert_eq!(
            first.state,
            elephant_types::publication::OperationState::Accepted
        );
        assert!(response["work_item_id"].is_null());
        assert_eq!(response["enqueued"], false);
        let after: Value =
            serde_json::from_slice(&store.export_snapshot_bytes().await.unwrap()).unwrap();
        assert_eq!(
            before["tables"], after["tables"],
            "no Doc restaging, contact recreation or queue mutation"
        );
        let mut changed = request;
        changed["source"]["hash"] = json!("changed-intent");
        assert!(handler.execute(&ctx, changed).await.is_err());
        let after: Value =
            serde_json::from_slice(&store.export_snapshot_bytes().await.unwrap()).unwrap();
        assert_eq!(before["tables"], after["tables"]);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn new_document_preserves_explicit_source_identity_and_capability() {
        for sealed in [false, true] {
            let ctx = context(true).await;
            let first = accepted_contact_input(&ctx, sealed).await;
            let mut doc = first.document.as_ref().unwrap().clone();
            doc.base.id = RecordId::new_v4(TableName::Doc);
            doc.rendition_key = "new-linked-document".into();
            doc.text = "New document on the existing source".into();
            doc.metadata = json!({"extraction_skill":"extraction/home","extraction_purpose":"document-only guidance"});
            let handler = IngestDocHandler::new(ctx.storage.clone());
            let result = handler
                .execute(
                    &ctx,
                    json!({"doc":doc,"intake":{
                        "revision_id":"linked-doc-r2", "idempotency_key":"linked-doc-r2",
                        "expected_generation":first.publication.generation,
                    }}),
                )
                .await
                .unwrap();
            assert_eq!(result["ok"], true);
            let operation: elephant_types::publication::OperationReceipt =
                serde_json::from_value(result["operation"].clone()).unwrap();
            assert_eq!(operation.source_id, first.source.base.id);

            let work: RecordId = serde_json::from_value(result["work_item_id"].clone()).unwrap();
            let work = WorkItemRepository::new(ctx.storage.clone())
                .get("household", &work)
                .await
                .unwrap()
                .unwrap();
            let publication: PublicationFence =
                serde_json::from_value(work.payload["_publication"].clone()).unwrap();
            let mut expected_metadata = doc.metadata.clone();
            expected_metadata["_publication"] = serde_json::to_value(&publication).unwrap();
            assert_eq!(result["data"]["data"]["metadata"], expected_metadata);
            assert_eq!(
                SourcePublicationRepository::new(ctx.storage.clone())
                    .accepted_contact_for_publication("household", &publication)
                    .await
                    .unwrap()
                    .is_some(),
                sealed
            );
            let sources = elephant_storage::SourceRepository::new(ctx.storage.clone());
            let saved = sources
                .get("household", &first.source.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved.metadata["contact"], first.source.metadata["contact"]);
            assert!(saved.metadata.get("extraction_skill").is_none());
            assert_eq!(sources.list("household", 10, None).await.unwrap().len(), 1);
            doc.base.id = RecordId::new_v4(TableName::Doc);
            doc.rendition_key = "unknown-source-document".into();
            doc.source = Some(elephant_types::Link::new(RecordId::new_v4(
                TableName::Source,
            )));
            assert!(handler.execute(&ctx, json!({"doc":doc})).await.is_err());
            assert_eq!(sources.list("household", 10, None).await.unwrap().len(), 1);
            assert_eq!(
                SourcePublicationRepository::new(ctx.storage.clone())
                    .head("household", &first.source.base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .operation_id,
                operation.operation_id
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn retained_reprocess_preserves_actual_capability_and_original_retry_receipt() {
        for sealed in [false, true] {
            let ctx = context(true).await;
            let first = accepted_contact_input(&ctx, sealed).await;
            let original_doc = first.document.as_ref().unwrap();
            let control = IntakeControl {
                revision_id: Some("extract-r2".into()),
                idempotency_key: Some("extract-r2".into()),
                expected_generation: Some(first.publication.generation.clone()),
            };
            let second = reprocess_intake(
                &ctx,
                first.source.clone(),
                original_doc,
                &first.publication,
                control.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                second.source.requested_ingest_mode(),
                Some(IngestMode::Graph)
            );
            assert_ne!(
                second.document.as_ref().unwrap().base.id,
                original_doc.base.id
            );
            let repository = SourcePublicationRepository::new(ctx.storage.clone());
            assert_eq!(
                repository
                    .accepted_contact_for_publication("household", &second.publication)
                    .await
                    .unwrap()
                    .is_some(),
                sealed
            );
            let replay = reprocess_intake(
                &ctx,
                first.source.clone(),
                original_doc,
                &first.publication,
                control,
            )
            .await
            .unwrap();
            assert_eq!(replay.operation, second.operation);
            assert_eq!(replay.publication, second.publication);
            let stale = reprocess_intake(
                &ctx,
                first.source.clone(),
                original_doc,
                &first.publication,
                IntakeControl {
                    revision_id: Some("stale-r3".into()),
                    idempotency_key: Some("stale-r3".into()),
                    expected_generation: Some(second.publication.generation.clone()),
                },
            )
            .await;
            assert!(
                stale.is_err(),
                "a fresh intent cannot upgrade the original document fence"
            );
            assert_eq!(
                repository
                    .head("household", &first.source.base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .operation_id,
                second.operation.operation_id
            );
            let replaced = accept_intake(
                &ctx,
                second.source.clone(),
                None,
                IntakeControl {
                    revision_id: Some("explicit-replacement-r3".into()),
                    idempotency_key: Some("explicit-replacement-r3".into()),
                    expected_generation: Some(second.publication.generation.clone()),
                },
                &[],
            )
            .await
            .unwrap();
            assert_eq!(
                repository
                    .accepted_contact_for_publication("household", &replaced.publication)
                    .await
                    .unwrap()
                    .is_some(),
                sealed
            );
            assert!(elephant_storage::EntityRepository::new(ctx.storage.clone())
                .list("household", 10, None)
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn retained_restore_uses_original_pending_acceptance_after_repeated_retraction() {
        for sealed in [false, true] {
            let ctx = context(true).await;
            let first = accepted_contact_input(&ctx, sealed).await;
            let sources = elephant_storage::SourceRepository::new(ctx.storage.clone());
            let repository = SourcePublicationRepository::new(ctx.storage.clone());
            let mut generation = first.publication.generation.clone();
            for index in 0..2 {
                let source = sources
                    .get_with_deleted("household", &first.source.base.id)
                    .await
                    .unwrap()
                    .unwrap();
                let retracted = retract_intake(
                    &ctx,
                    &source,
                    IntakeControl {
                        expected_generation: Some(generation),
                        idempotency_key: Some(format!("retract-{index}")),
                        revision_id: None,
                    },
                    None,
                )
                .await
                .unwrap();
                generation = retracted.source_generation;
            }
            let head = repository
                .head("household", &first.source.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                repository
                    .publication_before_retraction("household", &head)
                    .await
                    .unwrap(),
                first.publication
            );
            let mut source = sources
                .get_with_deleted("household", &first.source.base.id)
                .await
                .unwrap()
                .unwrap();
            source.base.deleted_at = None;
            let control = IntakeControl {
                revision_id: Some("restore-r2".into()),
                idempotency_key: Some("restore-r2".into()),
                expected_generation: Some(generation),
            };
            let prepared =
                PreparedSourceIntake::prepare(&ctx.storage, source.clone(), control.clone())
                    .await
                    .unwrap();
            assert_eq!(prepared.previous_documents.len(), 1);
            assert_eq!(
                prepared.previous_documents[0].base.id,
                first.document.as_ref().unwrap().base.id
            );
            let restored = restore_intake(&ctx, source.clone(), control.clone(), None)
                .await
                .unwrap();
            assert_eq!(
                repository
                    .accepted_contact_for_publication("household", &restored.publication)
                    .await
                    .unwrap()
                    .is_some(),
                sealed
            );
            let replay = restore_intake(&ctx, source, control, None).await.unwrap();
            assert_eq!(replay.operation, restored.operation);
            assert_eq!(
                replay.document.as_ref().unwrap().base.id,
                restored.document.as_ref().unwrap().base.id
            );
            assert_ne!(
                restored.document.as_ref().unwrap().base.id,
                first.document.as_ref().unwrap().base.id
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn retained_reprocess_requires_contact_scopes_before_new_acceptance() {
        let mut ctx = context(true).await;
        let first = accepted_contact_input(&ctx, true).await;
        ctx.authz = Arc::new(AuthzService::new_with_skip_auth(false));
        ctx.principal.scopes = [
            "read:source",
            "write:source",
            "read:doc:metadata",
            "write:doc",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        ctx.principal.clearances = vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: 0,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }];
        let result = reprocess_intake(
            &ctx,
            first.source.clone(),
            first.document.as_ref().unwrap(),
            &first.publication,
            IntakeControl {
                revision_id: Some("denied-r2".into()),
                idempotency_key: Some("denied-r2".into()),
                expected_generation: Some(first.publication.generation.clone()),
            },
        )
        .await;
        assert!(result.is_err());
        let repository = SourcePublicationRepository::new(ctx.storage.clone());
        assert_eq!(
            repository
                .head("household", &first.source.base.id)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            first.operation.operation_id
        );
        assert!(repository
            .operation_by_idempotency_key("household", "denied-r2")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn retained_ingest_authorizes_current_physical_doc_even_when_source_is_readable() {
        let mut ctx = context(true).await;
        let handler = IngestDocHandler::new(ctx.storage.clone());
        let first = handler.execute(&ctx, request("Original")).await.unwrap();
        let id: RecordId = serde_json::from_value(first["data"]["data"]["id"].clone()).unwrap();
        let documents = DocRepository::new(ctx.storage.clone());
        let mut protected = documents.get("household", &id).await.unwrap().unwrap();
        protected.base.security.level = 5;
        let protected = documents.update("household", &id, protected).await.unwrap();
        ctx.authz = Arc::new(AuthzService::new_with_skip_auth(false));
        ctx.principal.scopes = [
            "read:source",
            "write:source",
            "read:doc:metadata",
            "read:doc:raw",
            "write:doc",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        ctx.principal.clearances = vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: 3,
            labels: HashSet::from(["family".into()]),
            handling_allow: HashSet::from(["private".into()]),
        }];
        let source_id: RecordId =
            serde_json::from_value(first["operation"]["source_id"].clone()).unwrap();
        let current_source = elephant_storage::SourceRepository::new(ctx.storage.clone())
            .get("household", &source_id)
            .await
            .unwrap()
            .unwrap();
        assert!(retract_intake(
            &ctx,
            &current_source,
            IntakeControl {
                revision_id: None,
                idempotency_key: Some("protected-retract".into()),
                expected_generation: Some(
                    first["operation"]["source_generation"]
                        .as_str()
                        .unwrap()
                        .into()
                ),
            },
            None
        )
        .await
        .is_err());
        let mut next = request("Replacement");
        next["intake"] = json!({"revision_id":"protected-r2","idempotency_key":"protected-r2","expected_generation":first["operation"]["source_generation"]});
        assert!(handler.execute(&ctx, next).await.is_err());
        assert_eq!(
            documents.get("household", &id).await.unwrap().unwrap(),
            protected
        );
        let source: RecordId =
            serde_json::from_value(first["operation"]["source_id"].clone()).unwrap();
        assert_eq!(
            SourcePublicationRepository::new(ctx.storage.clone())
                .head("household", &source)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            first["operation"]["operation_id"].as_str().unwrap()
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn missing_read_write_scope_creates_no_source_authority() {
        let ctx = context(false).await;
        let handler = IngestDocHandler::new(ctx.storage.clone());
        let result = handler.execute(&ctx, request("Protected")).await;
        assert!(result.is_err() || result.unwrap()["ok"] == false);
        let head = SourcePublicationRepository::new(ctx.storage.clone())
            .external_head(
                "household",
                elephant_types::records::SourceType::Manual,
                "doc:11111111-1111-4111-8111-111111111111",
            )
            .await
            .expect("head read");
        assert!(head.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn replacement_without_explicit_desired_generation_is_refused() {
        let ctx = context(true).await;
        let handler = IngestDocHandler::new(ctx.storage.clone());
        handler
            .execute(&ctx, request("First"))
            .await
            .expect("first");
        let replacement = handler.execute(&ctx, request("Second")).await;
        assert!(replacement.is_err());
    }
}
