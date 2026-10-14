//! MCP management surface for durable remote-source freshness observations.

use async_trait::async_trait;
use chrono::Utc;
use elephant_auth::standing_watch::{StandingWatchAdmission, StandingWatchError};
use elephant_storage::source_watch_registration::PreparedSourceWatchRegistration;
use elephant_storage::standing_watch_repository::StandingWatchRepository;
use elephant_storage::{
    parse_id, Repository, SourceRepository, SourceWatchRepository, StorageClient,
};
use elephant_types::public_error::{ErrorCode, PublicError};
use elephant_types::records::{
    Source, SourceType, SourceWatch, SourceWatchCheck, SourceWatchStatus,
};
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::profile::ToolProfile;
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

const MIN_INTERVAL_SECONDS: i64 = 60;
const MAX_INTERVAL_SECONDS: i64 = 31 * 24 * 60 * 60;
const DEFAULT_CHECK_LIMIT: usize = 10;

#[derive(Debug, Deserialize)]
struct SourceWatchIdInput {
    source_id: String,
    #[serde(default = "default_check_limit")]
    check_limit: usize,
}

const fn default_check_limit() -> usize {
    DEFAULT_CHECK_LIMIT
}

#[derive(Debug, Deserialize)]
struct ConfigureSourceWatchInput {
    source_id: String,
    interval_seconds: i64,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

const fn default_enabled() -> bool {
    true
}

/// Read response containing the current schedule and recent receipts.
#[derive(Debug, Clone, Serialize)]
pub struct SourceWatchResponse {
    /// Mutable watch schedule.
    pub watch: SourceWatch,
    /// Most recent immutable checks, newest first.
    pub checks: Vec<SourceWatchCheck>,
}

/// Reads source-watch state without performing network work.
pub struct GetSourceWatchHandler {
    storage: StorageClient,
    watches: SourceWatchRepository,
}

impl GetSourceWatchHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            watches: SourceWatchRepository::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl ToolHandler for GetSourceWatchHandler {
    fn name(&self) -> &'static str {
        "get_source_watch"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get the recurring freshness policy and immutable observation receipts for a source. This does not fetch the remote source.\n\n",
            "Example:\n",
            r#"{ "source_id": "source:550e8400-e29b-41d4-a716-446655440000", "check_limit": 10 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        source_watch_id_schema(true)
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SourceWatchIdInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if !(1..=100).contains(&input.check_limit) {
            return Err(McpError::InvalidParameters(
                "check_limit must be between 1 and 100".to_string(),
            ));
        }
        let source = load_source(ctx, &self.storage, &input.source_id, false).await?;
        let Some(watch) = self
            .watches
            .find_by_source(&ctx.principal.space_id, &source.base.id)
            .await?
        else {
            return Err(watch_error(ErrorCode::NotFound, "Source watch not found"));
        };
        authorize_watch_read(ctx, &watch)?;
        let checks = self
            .watches
            .list_checks(&ctx.principal.space_id, &watch.base.id, input.check_limit)
            .await?;
        for check in &checks {
            ctx.authz
                .authorize_record_read(&ctx.principal, check, "read:source")
                .map_err(|_| watch_forbidden())?;
        }
        response_success(SourceWatchResponse { watch, checks })
    }
}

/// Creates, enables, pauses, or reschedules one source watch.
pub struct ConfigureSourceWatchHandler {
    storage: StorageClient,
    watches: SourceWatchRepository,
}

impl ConfigureSourceWatchHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            watches: SourceWatchRepository::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl ToolHandler for ConfigureSourceWatchHandler {
    fn name(&self) -> &'static str {
        "configure_source_watch"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Configure or pause a recurring, conditional HTTP freshness check for a web source. Configuration never weakens SSRF or response-size policy.\n\n",
            "Example:\n",
            r#"{ "source_id": "source:550e8400-e29b-41d4-a716-446655440000", "interval_seconds": 3600, "enabled": true }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["source_id", "interval_seconds"],
            "properties": {
                "source_id": {"type": "string", "description": "Canonical web source ID"},
                "interval_seconds": {
                    "type": "integer",
                    "minimum": MIN_INTERVAL_SECONDS,
                    "maximum": MAX_INTERVAL_SECONDS,
                    "description": "Normal interval between successful observations"
                },
                "enabled": {
                    "type": "boolean",
                    "default": true,
                    "description": "Enable the schedule or pause it without deleting receipts"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ConfigureSourceWatchInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if !(MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&input.interval_seconds) {
            return Err(McpError::InvalidParameters(format!(
                "interval_seconds must be between {MIN_INTERVAL_SECONDS} and {MAX_INTERVAL_SECONDS}"
            )));
        }
        let source = load_source(ctx, &self.storage, &input.source_id, true).await?;
        let requested_uri = watchable_uri(&source)?;
        let now = Utc::now();
        let status = if input.enabled {
            SourceWatchStatus::Enabled
        } else {
            SourceWatchStatus::Paused
        };
        let admission = verified_watch_admission(ctx, &self.storage).await?;
        let previous = self
            .watches
            .find_by_source(&ctx.principal.space_id, &source.base.id)
            .await?;
        let proposed = if let Some(mut watch) = previous.clone() {
            authorize_watch_read(ctx, &watch)?;
            ctx.authz
                .authorize_record_write(&ctx.principal, &watch, "write:source")
                .map_err(|_| watch_forbidden())?;
            watch.requested_uri = requested_uri;
            watch.interval_seconds = input.interval_seconds;
            watch.status = status;
            watch.last_error = None;
            watch.base.security = source.base.security.clone();
            watch.base.provenance = source.base.provenance.clone();
            watch.base.subjects.clone_from(&source.base.subjects);
            watch.base.subjects_state = source.base.subjects_state;
            if input.enabled {
                watch.next_check_at = now;
            }
            watch.base.updated_at = now;
            authorize_watch_write(ctx, &watch)?;
            watch
        } else {
            let mut base = source.base.clone();
            base.id = deterministic_watch_id(&source.base.space_id, &source.base.id);
            base.created_at = now;
            base.updated_at = now;
            base.created_by = "mcp.configure_source_watch".to_string();
            base.deleted_at = None;
            let proposed = SourceWatch {
                base,
                source: Link::new(source.base.id.clone()),
                requested_uri,
                status,
                interval_seconds: input.interval_seconds,
                next_check_at: now,
                last_checked_at: None,
                last_changed_at: None,
                etag: None,
                last_modified: None,
                observed_hash: Some(source.hash.clone()),
                consecutive_failures: 0,
                last_error: None,
                latest_check: None,
            };
            authorize_watch_write(ctx, &proposed)?;
            proposed
        };
        let prepared = PreparedSourceWatchRegistration::prepare(
            &self.storage,
            &source,
            previous.as_ref(),
            proposed,
        )
        .await?;
        prepared
            .authorize(&ctx.principal)
            .map_err(|_| watch_forbidden())?;
        let standing = StandingWatchRepository::new(self.storage.clone())
            .prepare_registration(prepared.watch(), &admission)
            .await?;
        let (watch, _) = prepared.register(&self.storage, &standing).await?;
        authorize_watch_read(ctx, &watch)?;
        response_success(SourceWatchResponse {
            watch,
            checks: Vec::new(),
        })
    }
}

/// Enqueues one immediate source observation through the durable work queue.
pub struct CheckSourceWatchHandler {
    storage: StorageClient,
    watches: SourceWatchRepository,
}

impl CheckSourceWatchHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            watches: SourceWatchRepository::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl ToolHandler for CheckSourceWatchHandler {
    fn name(&self) -> &'static str {
        "check_source_watch"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Enqueue or reopen the source's idempotent refresh work item for immediate leased execution. Use get_source_watch to inspect the resulting immutable receipt.\n\n",
            "Example:\n",
            r#"{ "source_id": "source:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        source_watch_id_schema(false)
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SourceWatchIdInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let source = load_source(ctx, &self.storage, &input.source_id, true).await?;
        let Some(watch) = self
            .watches
            .find_by_source(&ctx.principal.space_id, &source.base.id)
            .await?
        else {
            return Err(watch_error(ErrorCode::NotFound, "Source watch not found"));
        };
        authorize_watch_write(ctx, &watch)?;
        let caller = verified_watch_admission(ctx, &self.storage).await?;
        let prepared = PreparedSourceWatchRegistration::prepare(
            &self.storage,
            &source,
            Some(&watch),
            watch.clone(),
        )
        .await?;
        prepared
            .authorize(caller.principal())
            .map_err(|_| watch_forbidden())?;
        let standing = StandingWatchRepository::new(self.storage.clone())
            .capture_for_scheduling(&watch)
            .await?;
        prepared
            .authorize(standing.principal())
            .map_err(|_| watch_forbidden())?;
        let work_item_id =
            elephant_pipeline::source_watch::enqueue_source_watch_refresh_authorized(
                &self.storage,
                &prepared,
                &standing,
                Some(&caller),
                "mcp.check_source_watch",
                5,
            )
            .await
            .map_err(McpError::from)?;
        authorize_watch_read(ctx, &watch)?;
        response_success(json!({
            "watch": watch,
            "work_item_id": work_item_id,
        }))
    }
}

fn source_watch_id_schema(include_limit: bool) -> Value {
    let mut properties = serde_json::Map::from_iter([(
        "source_id".to_string(),
        json!({"type": "string", "description": "Canonical source ID"}),
    )]);
    if include_limit {
        properties.insert(
            "check_limit".to_string(),
            json!({
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": DEFAULT_CHECK_LIMIT,
                "description": "Maximum recent immutable check receipts"
            }),
        );
    }
    Value::Object(serde_json::Map::from_iter([
        ("type".to_string(), Value::String("object".to_string())),
        ("required".to_string(), json!(["source_id"])),
        ("properties".to_string(), Value::Object(properties)),
    ]))
}

async fn load_source(
    ctx: &ToolContext,
    storage: &StorageClient,
    raw_id: &str,
    write: bool,
) -> McpResult<Source> {
    if ctx.principal.space_id.is_empty() {
        return Err(watch_forbidden());
    }
    let id = parse_id(raw_id, TableName::Source)
        .ok()
        .filter(|id| id.table() == TableName::Source)
        .ok_or_else(|| watch_error(ErrorCode::InvalidRequest, "Invalid source identifier"))?;
    let source = SourceRepository::new(storage.clone())
        .get(&ctx.principal.space_id, &id)
        .await?
        .ok_or_else(|| watch_error(ErrorCode::NotFound, "Source not found"))?;
    ctx.authz
        .authorize_record_read(&ctx.principal, &source, "read:source")
        .map_err(|_| watch_forbidden())?;
    if write {
        ctx.authz
            .authorize_record_write(&ctx.principal, &source, "write:source")
            .map_err(|_| watch_forbidden())?;
    }
    Ok(source)
}

async fn verified_watch_admission(
    ctx: &ToolContext,
    storage: &StorageClient,
) -> McpResult<StandingWatchAdmission> {
    let caller = ctx.verified_caller().ok_or_else(|| {
        watch_error(
            ErrorCode::Unauthorized,
            "Source watch management requires a verified named-space caller",
        )
    })?;
    let grant =
        elephant_storage::space_grant_repository::SpaceGrantRepository::new(storage.clone())
            .get(&ctx.principal.space_id, &caller.claims().sub)
            .await?
            .ok_or_else(watch_forbidden)?;
    StandingWatchAdmission::verified(caller, &ctx.principal, &grant).map_err(admission_error)
}

fn watchable_uri(source: &Source) -> McpResult<String> {
    if source.type_ != SourceType::Web {
        return Err(McpError::InvalidParameters(
            "only web sources can currently be watched".to_string(),
        ));
    }
    let uri = source
        .uri
        .as_deref()
        .ok_or_else(|| McpError::InvalidParameters("web source has no URI".to_string()))?;
    let parsed = reqwest::Url::parse(uri)
        .map_err(|error| McpError::InvalidParameters(format!("invalid source URI: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(McpError::InvalidParameters(
            "source watch URI must use http or https".to_string(),
        ));
    }
    Ok(parsed.to_string())
}

fn deterministic_watch_id(space_id: &str, source_id: &RecordId) -> RecordId {
    RecordId::new(
        TableName::SourceWatch,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{space_id}|source_watch|{source_id}").as_bytes(),
        ),
    )
}

fn authorize_watch_write(ctx: &ToolContext, watch: &SourceWatch) -> McpResult<()> {
    authorize_watch_read(ctx, watch)?;
    ctx.authz
        .authorize_record_write(&ctx.principal, watch, "write:source")
        .map_err(|_| watch_forbidden())
}

fn authorize_watch_read(ctx: &ToolContext, watch: &SourceWatch) -> McpResult<()> {
    ctx.authz
        .authorize_record_read(&ctx.principal, watch, "read:source")
        .map_err(|_| watch_forbidden())
}

fn response_success<T: Serialize>(data: T) -> McpResult<Value> {
    serde_json::to_value(ToolResponse::success(data))
        .map_err(|error| McpError::ExecutionError(error.to_string()))
}

fn watch_error(code: ErrorCode, message: &'static str) -> McpError {
    McpError::Public(PublicError::new(code, message))
}

fn watch_forbidden() -> McpError {
    watch_error(
        ErrorCode::ForbiddenSpace,
        "Source watch operation is not permitted",
    )
}

fn admission_error(error: StandingWatchError) -> McpError {
    match error {
        StandingWatchError::InvalidCaller => watch_error(
            ErrorCode::Unauthorized,
            "Source watch registration requires a current authenticated caller",
        ),
        StandingWatchError::InvalidGrant
        | StandingWatchError::ExpandedAuthority
        | StandingWatchError::MissingScope => watch_forbidden(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    #[test]
    fn admission_refusals_preserve_typed_public_codes() {
        for (input, code) in [
            (StandingWatchError::InvalidCaller, ErrorCode::Unauthorized),
            (StandingWatchError::InvalidGrant, ErrorCode::ForbiddenSpace),
            (
                StandingWatchError::ExpandedAuthority,
                ErrorCode::ForbiddenSpace,
            ),
            (StandingWatchError::MissingScope, ErrorCode::ForbiddenSpace),
        ] {
            assert!(matches!(admission_error(input), McpError::Public(error)
                if error.error_code == code && !error.retryable));
        }
    }
    use super::*;
    use elephant_policy::{Clearance, Principal};
    use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
    use elephant_storage::{SpaceConfigRepository, WorkItemRepository};
    use elephant_types::records::base::SubjectsState;
    use elephant_types::records::{BaseFields, SpaceConfig};
    use std::{collections::HashSet, sync::Arc};

    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn fixture() -> (ToolContext, Source) {
        let storage = StorageClient::new_embedded().await.unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "watch-auth".into(),
            "fixture".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: base.clone(),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        base.id = RecordId::new_v4(TableName::Source);
        base.security.level = 3;
        base.security.labels = vec!["home".into()];
        base.security.handling = vec!["personal".into()];
        base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        let mut source = Source::new(base, SourceType::Web, "original-watch-source".into());
        source.uri = Some("https://example.invalid/facts".into());
        let mut doc_base = source.base.clone();
        doc_base.id = RecordId::new_v4(TableName::Doc);
        let doc = elephant_types::records::Doc {
            base: doc_base,
            title: "Watch facts".into(),
            text: "Actual original watch source bytes".into(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source.base.id.clone())),
            rendition_key: "watch-original".into(),
            metadata: json!({}),
        };
        source
            .set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(&doc))
            .unwrap();
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            source,
            IntakeControl::default(),
        ))
        .await
        .unwrap();
        let accepted = Box::pin(prepared.accept(&storage)).await.unwrap();
        let source = SourceRepository::new(storage.clone())
            .get("watch-auth", &accepted.source.base.id)
            .await
            .unwrap()
            .unwrap();
        let principal = Principal {
            space_id: "watch-auth".into(),
            scopes: HashSet::from([
                "read:source".into(),
                "write:source".into(),
                "read:doc".into(),
                "write:doc".into(),
            ]),
            clearances: vec![Clearance {
                policy_id: "default".into(),
                max_level: 10,
                labels: HashSet::from(["home".into()]),
                handling_allow: HashSet::from(["personal".into()]),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let now = Utc::now();
        elephant_storage::space_grant_repository::SpaceGrantRepository::new(storage.clone())
            .replace(
                elephant_types::space_grant::SpaceGrant {
                    space_id: principal.space_id.clone(),
                    subject: "watch-owner".into(),
                    generation: String::new(),
                    active: true,
                    scopes: principal.scopes.iter().cloned().collect(),
                    updated_by: "fixture-admin".into(),
                    updated_at: now,
                },
                None,
            )
            .await
            .unwrap();
        let claims = elephant_auth::Claims {
            sub: "watch-owner".into(),
            exp: now.timestamp() + 3600,
            iat: now.timestamp(),
            iss: None,
            aud: None,
            space_id: Some(principal.space_id.clone()),
            spaces: None,
            bootstrap_space: None,
            scopes: principal.scopes.iter().cloned().collect(),
            security_level: 10,
            security_labels: vec!["home".into()],
            clearances: principal.clearances.clone(),
            subject_allowlist: None,
            purpose: Vec::new(),
        };
        let secret = "local-watch-handler-test-secret";
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let caller = elephant_auth::LegacyVerifier {
            secret,
            algorithm: jsonwebtoken::Algorithm::HS256,
            audience: None,
            issuer: None,
            mappings: None,
        }
        .verify(&token)
        .unwrap();
        (
            ToolContext::with_authz(
                storage,
                Arc::new(crate::authz::AuthzService::new_with_skip_auth(false)),
                principal,
            )
            .with_verified_caller(Some(caller)),
            source,
        )
    }

    async fn tables(ctx: &ToolContext) -> Value {
        let bytes = ctx
            .storage
            .embedded_store()
            .unwrap()
            .export_snapshot_bytes()
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()["tables"].clone()
    }

    fn configure(source: &Source, interval: i64) -> Value {
        json!({"source_id":source.base.id,"interval_seconds":interval,"enabled":true})
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn source_read_and_write_denials_precede_watch_creation_update_and_enqueue() {
        let (ctx, source) = Box::pin(fixture()).await;
        let configure_handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        let check_handler = CheckSourceWatchHandler::new(ctx.storage.clone());
        // Each missing scope is independently denied even though the other one
        // is granted and the full record envelope is within clearance.
        for missing in ["read:source", "write:source", "read:doc", "write:doc"] {
            let mut denied = ctx.clone();
            denied.principal.scopes.remove(missing);
            let before = tables(&ctx).await;
            assert!(
                Box::pin(configure_handler.execute(&denied, configure(&source, 60)))
                    .await
                    .is_err()
            );
            assert_eq!(
                tables(&ctx).await,
                before,
                "denied creation must have no side effects"
            );
        }
        assert_eq!(
            Box::pin(configure_handler.execute(&ctx, configure(&source, 60)))
                .await
                .unwrap()["ok"],
            true
        );
        for missing in ["read:source", "write:source", "read:doc", "write:doc"] {
            let mut denied = ctx.clone();
            denied.principal.scopes.remove(missing);
            let before = tables(&ctx).await;
            assert!(
                Box::pin(configure_handler.execute(&denied, configure(&source, 120)))
                    .await
                    .is_err()
            );
            assert_eq!(
                tables(&ctx).await,
                before,
                "denied update must preserve original watch"
            );
            assert!(
                Box::pin(check_handler.execute(&denied, json!({"source_id":source.base.id})))
                    .await
                    .is_err()
            );
            assert_eq!(
                tables(&ctx).await,
                before,
                "denied response read must not enqueue work"
            );
        }
        let mut wrong_space = ctx.clone();
        wrong_space.principal.space_id = "other-space".into();
        let before = tables(&ctx).await;
        assert!(
            Box::pin(check_handler.execute(&wrong_space, json!({"source_id":source.base.id})))
                .await
                .is_err()
        );
        assert_eq!(tables(&ctx).await, before);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn old_watch_restrictions_cannot_be_lowered_before_authorization() {
        let (ctx, source) = Box::pin(fixture()).await;
        let configure_handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        Box::pin(configure_handler.execute(&ctx, configure(&source, 60)))
            .await
            .unwrap();
        let watches = SourceWatchRepository::new(ctx.storage.clone());
        let mut restricted = watches
            .find_by_source("watch-auth", &source.base.id)
            .await
            .unwrap()
            .unwrap();
        restricted.base.security.level = 11;
        let id = restricted.base.id.clone();
        watches
            .update("watch-auth", &id, restricted.clone())
            .await
            .unwrap();
        let before = tables(&ctx).await;
        assert!(
            Box::pin(configure_handler.execute(&ctx, configure(&source, 120)))
                .await
                .is_err()
        );
        assert!(Box::pin(
            CheckSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, json!({"source_id":source.base.id}))
        )
        .await
        .is_err());
        assert_eq!(tables(&ctx).await, before);
        assert_eq!(
            watches.get("watch-auth", &id).await.unwrap(),
            Some(restricted)
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn authorized_watch_updates_and_work_preserve_the_complete_envelope() {
        let (ctx, source) = Box::pin(fixture()).await;
        let configure_handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        Box::pin(configure_handler.execute(&ctx, configure(&source, 60)))
            .await
            .unwrap();
        Box::pin(configure_handler.execute(&ctx, configure(&source, 120)))
            .await
            .unwrap();
        let watch = SourceWatchRepository::new(ctx.storage.clone())
            .find_by_source("watch-auth", &source.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watch.interval_seconds, 120);
        assert_eq!(watch.base.security, source.base.security);
        assert_eq!(watch.base.subjects, source.base.subjects);
        assert_eq!(watch.base.subjects_state, source.base.subjects_state);
        assert_eq!(watch.base.provenance, source.base.provenance);
        let response = Box::pin(
            CheckSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, json!({"source_id":source.base.id})),
        )
        .await
        .unwrap();
        assert_eq!(response["ok"], true);
        let id: RecordId =
            serde_json::from_value(response["data"]["work_item_id"].clone()).unwrap();
        let item = WorkItemRepository::new(ctx.storage.clone())
            .get("watch-auth", &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.base.security, source.base.security);
        assert_eq!(item.base.subjects, source.base.subjects);
        assert_eq!(item.base.subjects_state, source.base.subjects_state);
        assert_eq!(item.base.provenance, source.base.provenance);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn signed_watch_registration_refuses_missing_context_and_stricter_current_doc() {
        let (ctx, source) = Box::pin(fixture()).await;
        let handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        let missing = ctx.clone().with_verified_caller(None);
        let before = tables(&ctx).await;
        assert!(matches!(
            Box::pin(handler.execute(&missing, configure(&source, 60))).await,
            Err(McpError::Public(PublicError {
                error_code: ErrorCode::Unauthorized,
                retryable: false,
                ..
            }))
        ));
        assert_eq!(tables(&ctx).await, before);
        let docs = elephant_storage::DocRepository::new(ctx.storage.clone());
        let mut original = docs
            .find_by_source("watch-auth", &source.base.id, 2)
            .await
            .unwrap()
            .pop()
            .unwrap();
        original.base.security.level = 11;
        docs.update("watch-auth", &original.base.id.clone(), original)
            .await
            .unwrap();
        let before = tables(&ctx).await;
        assert!(matches!(
            Box::pin(handler.execute(&ctx, configure(&source, 60))).await,
            Err(McpError::Public(PublicError {
                error_code: ErrorCode::ForbiddenSpace,
                retryable: false,
                ..
            }))
        ));
        assert_eq!(
            tables(&ctx).await,
            before,
            "current Doc policy must be checked before delegation/watch writes"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn current_grant_revocation_denies_watch_reconfiguration_and_immediate_check() {
        let (ctx, source) = Box::pin(fixture()).await;
        let configure_handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        Box::pin(configure_handler.execute(&ctx, configure(&source, 60)))
            .await
            .unwrap();
        let grants = elephant_storage::space_grant_repository::SpaceGrantRepository::new(
            ctx.storage.clone(),
        );
        let mut grant = grants
            .get("watch-auth", "watch-owner")
            .await
            .unwrap()
            .unwrap();
        let generation = grant.generation.clone();
        grant.active = false;
        grants.replace(grant, Some(&generation)).await.unwrap();
        let before = tables(&ctx).await;
        assert!(
            Box::pin(configure_handler.execute(&ctx, configure(&source, 120)))
                .await
                .is_err()
        );
        assert!(Box::pin(
            CheckSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, json!({"source_id":source.base.id}))
        )
        .await
        .is_err());
        assert_eq!(tables(&ctx).await, before);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn authorized_pause_rotates_registration_but_cannot_dispatch() {
        let (ctx, source) = Box::pin(fixture()).await;
        let handler = ConfigureSourceWatchHandler::new(ctx.storage.clone());
        Box::pin(handler.execute(&ctx, configure(&source, 60)))
            .await
            .unwrap();
        let response = Box::pin(handler.execute(
            &ctx,
            json!({
                "source_id": source.base.id, "interval_seconds": 60, "enabled": false,
            }),
        ))
        .await
        .unwrap();
        assert_eq!(response["ok"], true);
        let paused = SourceWatchRepository::new(ctx.storage.clone())
            .find_by_source("watch-auth", &source.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused.status, SourceWatchStatus::Paused);
        let before = tables(&ctx).await;
        assert!(Box::pin(
            CheckSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, json!({"source_id": source.base.id}))
        )
        .await
        .is_err());
        assert_eq!(
            tables(&ctx).await,
            before,
            "paused admission cannot enqueue work"
        );
        Box::pin(handler.execute(&ctx, configure(&source, 60)))
            .await
            .unwrap();
        let response = Box::pin(
            CheckSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, json!({"source_id": source.base.id})),
        )
        .await
        .unwrap();
        assert_eq!(
            response["ok"], true,
            "explicit authorized resume restores dispatch"
        );
    }

    fn unregistered_watch(source: &Source) -> SourceWatch {
        let mut base = source.base.clone();
        base.id = deterministic_watch_id(&source.base.space_id, &source.base.id);
        SourceWatch {
            base,
            source: Link::new(source.base.id.clone()),
            requested_uri: source.uri.clone().unwrap(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 60,
            next_check_at: Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some(source.hash.clone()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn local_registration_cannot_cross_a_concurrent_signed_registration() {
        let (ctx, source) = Box::pin(fixture()).await;
        let watch = SourceWatchRepository::new(ctx.storage.clone())
            .create(unregistered_watch(&source))
            .await
            .unwrap();
        let local = StandingWatchAdmission::trusted_local(&ctx.principal).unwrap();
        let records = Box::pin(PreparedSourceWatchRegistration::prepare(
            &ctx.storage,
            &source,
            Some(&watch),
            watch.clone(),
        ))
        .await
        .unwrap();
        let local_registration = StandingWatchRepository::new(ctx.storage.clone())
            .prepare_local_registration_if_absent(&watch, &local)
            .await
            .unwrap()
            .unwrap();
        Box::pin(
            ConfigureSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, configure(&source, 60)),
        )
        .await
        .unwrap();
        let signed = tables(&ctx).await;
        assert!(
            Box::pin(records.register(&ctx.storage, &local_registration))
                .await
                .is_err()
        );
        assert_eq!(tables(&ctx).await, signed);
        let service =
            elephant_pipeline::source_watch::SourceWatchService::with_trusted_local_admission(
                ctx.storage.clone(),
                local,
            )
            .unwrap();
        let current = SourceWatchRepository::new(ctx.storage.clone())
            .get("watch-auth", &watch.base.id)
            .await
            .unwrap()
            .unwrap();
        let denied = Box::pin(service.check(current, "unrelated-local-invoker")).await;
        assert!(
            matches!(denied, Err(elephant_pipeline::PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(ref reason)))
            if reason == "standing_watch_requires_registered_dispatch")
        );
        assert_eq!(
            tables(&ctx).await,
            signed,
            "signed registration cannot be adopted or dispatched as local"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn local_convenience_refuses_a_revoked_signed_watch_without_writes() {
        let (ctx, source) = Box::pin(fixture()).await;
        Box::pin(
            ConfigureSourceWatchHandler::new(ctx.storage.clone())
                .execute(&ctx, configure(&source, 60)),
        )
        .await
        .unwrap();
        let grants = elephant_storage::space_grant_repository::SpaceGrantRepository::new(
            ctx.storage.clone(),
        );
        let mut grant = grants
            .get("watch-auth", "watch-owner")
            .await
            .unwrap()
            .unwrap();
        let generation = grant.generation.clone();
        grant.active = false;
        grants.replace(grant, Some(&generation)).await.unwrap();
        let watch = SourceWatchRepository::new(ctx.storage.clone())
            .find_by_source("watch-auth", &source.base.id)
            .await
            .unwrap()
            .unwrap();
        let before = tables(&ctx).await;
        let service =
            elephant_pipeline::source_watch::SourceWatchService::with_trusted_local_admission(
                ctx.storage.clone(),
                StandingWatchAdmission::trusted_local(&ctx.principal).unwrap(),
            )
            .unwrap();
        assert!(
            Box::pin(service.check(watch, "local-cannot-revive-service-grant"))
                .await
                .is_err()
        );
        assert_eq!(tables(&ctx).await, before);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn local_constructor_rejects_verified_service_admission() {
        let (ctx, source) = Box::pin(fixture()).await;
        let signed = verified_watch_admission(&ctx, &ctx.storage).await.unwrap();
        let before = tables(&ctx).await;
        assert!(matches!(
            elephant_pipeline::source_watch::SourceWatchService::with_trusted_local_admission(
                ctx.storage.clone(),
                signed,
            ),
            Err(elephant_pipeline::PipelineError::SecurityViolation(_))
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut watch = unregistered_watch(&source);
        watch.requested_uri = format!(
            "http://{}/no-authorized-fetch",
            listener.local_addr().unwrap()
        );
        let worker_only = elephant_pipeline::source_watch::SourceWatchService::with_fetch_policy(
            ctx.storage.clone(),
            elephant_pipeline::remote_source::RemoteFetchPolicy {
                allow_private_networks: true,
                ..elephant_pipeline::remote_source::RemoteFetchPolicy::web_watch()
            },
        );
        assert!(matches!(
            Box::pin(worker_only.check(watch, "actor-is-not-authority")).await,
            Err(elephant_pipeline::PipelineError::SecurityViolation(_))
        ));
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept()).await.is_err(),
            "missing local admission must make zero requests even when the explicit network policy permits this endpoint");
        assert_eq!(
            tables(&ctx).await,
            before,
            "missing/wrong constructor authority cannot register or enqueue/fetch"
        );
    }
}
