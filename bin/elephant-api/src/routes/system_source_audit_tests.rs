//! Exercise both public audit adapters against a separately protected physical Doc.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::panic
)]

use super::*;
use chrono::Utc;
use elephant_mcp::handlers::system::RevertAuditHandler;
use elephant_mcp::handlers::ToolHandler;
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_intake::{stage_accepted_document, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::SpaceConfigRepository;
use elephant_types::publication::SourceVisibility;
use elephant_types::records::{
    AuditAction, BaseFields, ManualDocTemplate, SourceType, SpaceConfig,
};
use elephant_types::RecordId;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug)]
enum Adapter {
    Http,
    Mcp,
}

#[expect(
    clippy::large_futures,
    reason = "Keep the original fixture ownership, future layout, and oracle sequence."
)]
async fn attempt(
    adapter: Adapter,
    storage: &StorageClient,
    audit_id: &RecordId,
    generation: &str,
    key: &str,
    scopes: Vec<String>,
    level: i32,
) -> bool {
    let control = IntakeControl {
        expected_generation: Some(generation.into()),
        idempotency_key: Some(key.into()),
        revision_id: None,
    };
    match adapter {
        Adapter::Http => {
            let principal = AuthPrincipal {
                subject: "audit-caller".into(),
                space_id: Some("audit-space".into()),
                scopes,
                security_level: level,
                ..AuthPrincipal::default()
            };
            let state = Arc::new(SystemState::new(
                storage.clone(),
                Arc::new(crate::authz::AuthzService::new()),
            ));
            match revert_audit(
                State(state),
                axum::Extension(principal),
                Path(audit_id.to_string()),
                Query(control),
            )
            .await
            {
                Ok(Json(response)) => response.success,
                Err((status, _)) => {
                    assert_eq!(status, StatusCode::FORBIDDEN, "must be an authority denial");
                    false
                }
            }
        }
        Adapter::Mcp => {
            let principal = Principal {
                space_id: "audit-space".into(),
                scopes: scopes.into_iter().collect(),
                clearances: vec![Clearance {
                    policy_id: "default".into(),
                    max_level: level,
                    labels: HashSet::new(),
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };
            let context = ToolContext::with_authz(
                storage.clone(),
                Arc::new(McpAuthzService::new_with_skip_auth(false)),
                principal,
            );
            match RevertAuditHandler::new(storage.clone())
                .execute(
                    &context,
                    serde_json::json!({"audit_id":audit_id,"intake":control}),
                )
                .await
            {
                Ok(value) => value["ok"] == true,
                Err(elephant_mcp::McpError::ExecutionError(reason)) => {
                    assert!(
                        reason.contains("denied")
                            || reason.contains("scope")
                            || reason.contains("exceeds max"),
                        "{reason}"
                    );
                    false
                }
                Err(error) => panic!("expected authority denial, got {error}"),
            }
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture ownership, future layout, and oracle sequence."
)]
async fn source_audit_inverse_authorizes_every_current_physical_document_on_both_adapters() {
    for adapter in [Adapter::Http, Adapter::Mcp] {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "audit-space".into(),
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
            .expect("space configuration");
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "audit-space".into(),
                "fixture".into(),
            ),
            SourceType::Manual,
            "audit-input".into(),
        );
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Audit document".into(),
                text: "Retained source input".into(),
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "audit-rendition".into(),
                metadata: serde_json::json!({}),
            })
            .expect("document template");
        let accepted = PreparedSourceIntake::prepare(&storage, source, IntakeControl::default())
            .await
            .expect("prepare input")
            .accept(&storage)
            .await
            .expect("accept input");
        stage_accepted_document(&storage, &accepted)
            .await
            .expect("stage physical document");
        let docs = DocRepository::new(storage.clone());
        let mut protected = docs
            .get("audit-space", &accepted.document.as_ref().unwrap().base.id)
            .await
            .unwrap()
            .unwrap();
        protected.base.security.level = 5;
        let doc_id = protected.base.id.clone();
        let protected = docs
            .update("audit-space", &doc_id, protected)
            .await
            .expect("independently protected document");
        let current = SourceRepository::new(storage.clone())
            .get("audit-space", &accepted.source.base.id)
            .await
            .unwrap()
            .unwrap();
        let audit = AuditLogRepository::new(storage.clone())
            .create(AuditLog {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::AuditLog),
                    "audit-space".into(),
                    "fixture".into(),
                ),
                action: AuditAction::Create,
                actor: "fixture".into(),
                record: current.base.id.clone(),
                before: None,
                after: Some(serde_json::to_value(&current).unwrap()),
                timestamp: Utc::now(),
                reason: None,
            })
            .await
            .expect("source creation audit");
        let publication = SourcePublicationRepository::new(storage.clone());
        let head_before = publication
            .head("audit-space", &current.base.id)
            .await
            .unwrap()
            .unwrap();
        let all_scopes = [
            "read:audit",
            "read:source",
            "write:source",
            "read:doc:metadata",
            "write:doc",
        ];
        for (index, missing, level) in [
            (0, Some("read:doc:metadata"), 10),
            (1, Some("write:doc"), 10),
            (2, None, 0),
        ] {
            let key = format!("audit-denied-{index}");
            let scopes = all_scopes
                .iter()
                .filter(|scope| Some(**scope) != missing)
                .map(|scope| (*scope).to_owned())
                .collect();
            assert!(
                !attempt(
                    adapter,
                    &storage,
                    &audit.base.id,
                    &accepted.publication.generation,
                    &key,
                    scopes,
                    level
                )
                .await,
                "{adapter:?} admitted unauthorized physical document"
            );
            assert_eq!(
                publication
                    .head("audit-space", &current.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                head_before
            );
            assert!(publication
                .operation_by_idempotency_key("audit-space", &key)
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                docs.get("audit-space", &doc_id).await.unwrap().unwrap(),
                protected
            );
        }
        assert!(
            attempt(
                adapter,
                &storage,
                &audit.base.id,
                &accepted.publication.generation,
                "audit-allowed",
                all_scopes.into_iter().map(str::to_owned).collect(),
                10
            )
            .await,
            "authorized inverse must succeed"
        );
        assert_eq!(
            publication
                .head("audit-space", &current.base.id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            SourceVisibility::Retracted
        );
    }
}
