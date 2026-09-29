//! MCP management surface for durable remote-source freshness observations.

use async_trait::async_trait;
use chrono::Utc;
use elephant_pipeline::source_watch::enqueue_source_watch_refresh;
use elephant_storage::{
    parse_id, Repository, SourceRepository, SourceWatchRepository, StorageClient,
};
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
            return response_error(format!(
                "No source watch is configured for {}",
                source.base.id
            ));
        };
        authorize_watch_read(ctx, &watch)?;
        let checks = self
            .watches
            .list_checks(&ctx.principal.space_id, &watch.base.id, input.check_limit)
            .await?;
        for check in &checks {
            ctx.authz
                .authorize_record_read(&ctx.principal, check, "read:source")
                .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
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
        let watch = if let Some(mut watch) = self
            .watches
            .find_by_source(&ctx.principal.space_id, &source.base.id)
            .await?
        {
            ctx.authz
                .authorize_record_write(&ctx.principal, &watch, "write:source")
                .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
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
            let watch_id = watch.base.id.clone();
            self.watches
                .update(&ctx.principal.space_id, &watch_id, watch)
                .await?
        } else {
            let mut base = source.base.clone();
            base.id = deterministic_watch_id(&source.base.space_id, &source.base.id);
            base.created_at = now;
            base.updated_at = now;
            base.created_by = "mcp.configure_source_watch".to_string();
            base.deleted_at = None;
            self.watches
                .create(SourceWatch {
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
                })
                .await?
        };
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
            return response_error(format!(
                "No source watch is configured for {}",
                source.base.id
            ));
        };
        ctx.authz
            .authorize_record_write(&ctx.principal, &watch, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        let work_item_id =
            enqueue_source_watch_refresh(&self.storage, &watch, "mcp.check_source_watch", 5)
                .await
                .map_err(|error| McpError::ExecutionError(error.to_string()))?;
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
        return Err(McpError::ExecutionError(
            "Access denied: missing space_id".to_string(),
        ));
    }
    let id = parse_id(raw_id, TableName::Source)
        .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
    let source = SourceRepository::new(storage.clone())
        .get(&ctx.principal.space_id, &id)
        .await?
        .ok_or_else(|| McpError::ExecutionError(format!("Source not found: {id}")))?;
    let authorization = if write {
        ctx.authz
            .authorize_record_write(&ctx.principal, &source, "write:source")
    } else {
        ctx.authz
            .authorize_record_read(&ctx.principal, &source, "read:source")
    };
    authorization.map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    Ok(source)
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

fn authorize_watch_read(ctx: &ToolContext, watch: &SourceWatch) -> McpResult<()> {
    ctx.authz
        .authorize_record_read(&ctx.principal, watch, "read:source")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))
}

fn response_success<T: Serialize>(data: T) -> McpResult<Value> {
    serde_json::to_value(ToolResponse::success(data))
        .map_err(|error| McpError::ExecutionError(error.to_string()))
}

fn response_error(message: impl Into<String>) -> McpResult<Value> {
    serde_json::to_value(ToolResponse::<()>::error(message))
        .map_err(|error| McpError::ExecutionError(error.to_string()))
}
