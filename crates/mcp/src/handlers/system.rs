#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! System MCP tool handlers.
//!
//! Implements:
//! - `revert_audit`
//! - `stream_events`
//! - `get_capabilities`
//! - `get_health`
//! - `get_tool_schema`
//! - `get_provenance`

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use elephant_extensions::{enabled_extension_names, load_runtime_reference_registry_from_env};
use elephant_pipeline::scheduler::budget::BUDGET_EXHAUSTED_EVENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use elephant_storage::public_mutation::PreparedAuditRevert;
use elephant_storage::source_intake::{
    prepare_source_audit_revert, IntakeControl, SourceAuditRevertOutcome,
};
use elephant_storage::{
    parse_id, AuditLogRepository, DocRepository, Repository, StorageClient, SystemEventRepository,
    WorkItemRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::{AuditLog, SystemEvent};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};
const SNAPSHOT_DIR_ENV: &str = "ELEPHANT_SNAPSHOT_DIR";

const fn write_scope_for_table(table: TableName) -> Option<&'static str> {
    match table {
        TableName::Entity => Some("write:entity"),
        TableName::Rel => Some("write:relationship"),
        TableName::Doc => Some("write:doc"),
        TableName::Source => Some("write:source"),
        TableName::IdentityCandidate | TableName::EntityIdentityKey | TableName::EntityMerge => {
            Some("write:entity")
        }
        // System/internal tables and v0.8 tables - revert not supported
        TableName::SystemEvent
        | TableName::Event
        | TableName::AuditLog
        | TableName::IdMap
        | TableName::RelEvidence
        | TableName::ChunkEmbedding
        | TableName::SpaceConfig
        | TableName::DocRevision
        | TableName::DocNode
        | TableName::Assertion
        | TableName::Attribute
        | TableName::WorkItem
        | TableName::WorkItemArchive
        | TableName::WorkEdge
        | TableName::WorkRun
        | TableName::WorkRunArchive
        | TableName::WorkArtifact
        | TableName::WorkDeadletter
        | TableName::PredicateRegistry
        | TableName::PredicateAlias
        | TableName::ConflictGroup
        | TableName::TruthSlot
        | TableName::IngestDecision
        | TableName::CostLedgerDaily
        | TableName::Freshness
        | TableName::ReprocessJob
        | TableName::KnowledgeView
        | TableName::KnowledgeViewRevision
        | TableName::KnowledgeViewDefinition
        | TableName::KnowledgeViewDependency
        | TableName::SourceWatch
        | TableName::SourceWatchCheck
        | TableName::MediaAsset
        | TableName::DocChunk => None,
    }
}

const REVERT_AUDIT_WRITE_SCOPES: &[&str] = &[
    "write:entity",
    "write:relationship",
    "write:doc",
    "write:source",
];

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `revert_audit` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertAuditInput {
    /// Audit log entry ID.
    pub audit_id: String,
}

/// Output for `revert_audit` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertAuditOutput {
    /// Whether the revert was successful.
    pub success: bool,
    /// Reverted record type.
    pub record_type: String,
    /// Reverted record ID.
    pub record_id: String,
    /// Description of what was reverted.
    pub description: String,
}

/// Input for `stream_events` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventsInput {
    /// Space ID to stream events from.
    #[serde(default = "default_space_id")]
    pub space_id: String,
    /// Cursor for pagination (sequence number).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Maximum events to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_space_id() -> String {
    "default".to_string()
}

const fn default_limit() -> usize {
    100
}

/// Output for `stream_events` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventsOutput {
    /// Events.
    pub events: Vec<Value>,
    /// Next cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more events exist.
    pub has_more: bool,
}

/// Input for `get_tool_schema` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetToolSchemaInput {
    /// Tool name.
    pub tool_name: String,
}

/// Output for `get_tool_schema` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetToolSchemaOutput {
    /// Tool name.
    pub name: String,
    /// Tool description.
    pub description: String,
    /// Tool owner (core or extension name).
    pub owner: String,
    /// Input schema (JSON Schema).
    pub input_schema: Value,
}

/// Input for `get_job_status` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetJobStatusInput {
    /// Job/work item ID.
    pub job_id: String,
}

/// Output for `get_job_status` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetJobStatusOutput {
    /// Job/work item ID.
    pub job_id: String,
    /// Work item status.
    pub status: String,
    /// Work item kind.
    pub kind: String,
    /// Work item payload with progress details.
    pub payload: Value,
    /// Attempts made.
    pub attempts: u32,
    /// Last error message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Output for `get_capabilities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesOutput {
    /// API version.
    pub version: String,
    /// Active storage backend kind.
    pub backend: String,
    /// Available tools.
    pub tools: Vec<String>,
    /// Enabled features.
    pub features: Vec<String>,
    /// Extensions.
    pub extensions: Vec<ExtensionInfo>,
    /// Limits.
    pub limits: CapabilityLimits,
}

/// Extension information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionInfo {
    /// Extension name.
    pub name: String,
    /// Extension version.
    pub version: String,
    /// Whether enabled.
    pub enabled: bool,
}

/// Capability limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityLimits {
    /// Max batch size.
    pub max_batch_size: usize,
    /// Max search results.
    pub max_search_results: usize,
    /// Max event stream batch.
    pub max_event_stream_batch: usize,
}

/// Output for `get_health` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatusOutput {
    /// Overall status.
    pub status: String,
    /// Active storage backend kind.
    pub backend: String,
    /// Generic storage connectivity status.
    pub storage: String,
    /// Legacy compatibility alias for `storage`.
    pub surrealdb: String,
    /// Database health.
    pub db_ok: bool,
    /// Table count.
    pub table_count: usize,
    /// Table names.
    pub tables: Vec<String>,
    /// Timestamp of the check (UTC).
    pub timestamp: String,
    /// Budget exhaustion signal: present when any mode's daily pipeline
    /// budget was exhausted today, absent when healthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<BudgetHealth>,
}

/// Budget section of `get_health`, derived from today's `budget_exhausted`
/// system events (cross-space — `get_health` serves a global principal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetHealth {
    /// Modes whose daily budget was exhausted today (sorted, deduped;
    /// e.g. `["graph", "retrieval"]`).
    pub exhausted_modes: Vec<String>,
    /// RFC 3339 timestamp of the most recent exhaustion event today.
    pub last_exhausted_at: String,
}

/// Input for snapshot import/export tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotOperationInput {
    /// Filesystem path on the Elephant host.
    pub path: String,
    /// Operator-selected snapshot label; omitted labels are generated by the host.
    #[serde(default)]
    pub name: Option<String>,
    /// Backup lifetime, bounded by the snapshot retention policy.
    #[serde(default)]
    pub retention_seconds: Option<u64>,
    /// Maximum worker/storage quiesce time before capture is refused.
    #[serde(default)]
    pub quiesce_timeout_seconds: Option<u64>,
}

/// Output for snapshot import/export tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotOperationOutput {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Active storage backend kind.
    pub backend: String,
    /// Filesystem path on the Elephant host.
    pub path: String,
    /// Human-friendly result message.
    pub message: String,
    /// Verified identity, backend, checksum, timestamps and retention metadata.
    pub manifest: elephant_storage::snapshot::SnapshotManifest,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `revert_audit` tool.
#[allow(clippy::struct_field_names)]
pub struct RevertAuditHandler {
    storage: StorageClient,
    audit_repo: AuditLogRepository,
}

impl RevertAuditHandler {
    /// Creates a new handler.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    #[expect(
        clippy::redundant_clone,
        reason = "Keep both repository/client clones and the constructor input drop timing unchanged."
    )]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            storage: storage.clone(),
            audit_repo: AuditLogRepository::new(storage.clone()),
        }
    }
}

#[async_trait]
impl ToolHandler for RevertAuditHandler {
    fn name(&self) -> &'static str {
        "revert_audit"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Revert changes from an audit log entry.\n\n",
            "Example:\n",
            r#"{ "audit_id": "audit_log:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["audit_id"],
            "properties": {
                "audit_id": {
                    "type": "string",
                    "description": "Audit log entry ID to revert"
                },
                "intake": {"type": "object", "required": [], "properties": {
                    "expected_generation": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "revision_id": {"type": "string"}
                }, "description": "Source/doc inverses require exact generation and retry identity"}
            }
        })
    }

    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Keep prepared inverse scopes and the original unboxed accept future through authorization and commit handling."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let intake = IntakeControl::from_params(&params)?;
        let input: RevertAuditInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.audit_id, TableName::AuditLog)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let audit: AuditLog = match self.audit_repo.get(&space_id, &rid).await {
            Ok(Some(audit)) => audit,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Audit log entry not found: {}", input.audit_id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::StorageError(e)),
        };

        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &audit, "read:audit")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let record_id = audit.record.to_string();
        let record_type = audit.record.table().to_string();
        let required_scope = write_scope_for_table(audit.record.table()).ok_or_else(|| {
            McpError::InvalidParameters("Revert not supported for record type".into())
        })?;
        let actor = ctx
            .verified_caller()
            .map_or("trusted:mcp", |caller| caller.claims().sub.as_str());
        let source_backed = match audit.record.table() {
            TableName::Source => true,
            TableName::Doc => DocRepository::new(self.storage.clone())
                .get_with_deleted(&space_id, &audit.record)
                .await?
                .is_some_and(|doc| doc.source.is_some()),
            _ => false,
        };
        if source_backed {
            let prepared =
                prepare_source_audit_revert(&self.storage, &space_id, &audit, intake).await?;
            for source in
                std::iter::once(&prepared.current_source).chain(prepared.proposed_source.iter())
            {
                ctx.authz
                    .authorize_record_read(&ctx.principal, source, "read:source")
                    .map_err(McpError::ExecutionError)?;
                ctx.authz
                    .authorize_record_write(&ctx.principal, source, "write:source")
                    .map_err(McpError::ExecutionError)?;
            }
            for doc in prepared
                .current_document
                .iter()
                .chain(prepared.proposed_document.iter())
            {
                ctx.authz
                    .authorize_record_read(&ctx.principal, doc, "read:doc")
                    .map_err(McpError::ExecutionError)?;
                ctx.authz
                    .authorize_record_write(&ctx.principal, doc, "write:doc")
                    .map_err(McpError::ExecutionError)?;
            }
            for doc in &prepared.current_documents {
                ctx.authz
                    .authorize_record_read(&ctx.principal, doc, "read:doc:metadata")
                    .map_err(McpError::ExecutionError)?;
                ctx.authz
                    .authorize_record_write(&ctx.principal, doc, "write:doc")
                    .map_err(McpError::ExecutionError)?;
            }
            if let Some(contact) = &prepared.contact {
                for row in contact.authorizations() {
                    ctx.authz
                        .authorize_record_read(&ctx.principal, row, row.read_scope())
                        .map_err(McpError::ExecutionError)?;
                    ctx.authz
                        .authorize_record_write(&ctx.principal, row, row.write_scope())
                        .map_err(McpError::ExecutionError)?;
                }
            }
            if let SourceAuditRevertOutcome::Accepted(accepted) =
                prepared.accept(&self.storage, actor).await?
            {
                if accepted.needs_processing() {
                    elephant_pipeline::source_queue::enqueue_accepted_source(&self.storage, &accepted.source, &accepted.publication, actor).await
                        .map_err(|error| McpError::ExecutionError(format!("Audit inverse accepted as {}; queue handoff pending durable recovery: {error}", accepted.operation.operation_id)))?;
                }
            }
        } else {
            let prepared =
                PreparedAuditRevert::prepare(self.storage.clone(), &space_id, &audit).await?;
            for record in [&prepared.current, &prepared.proposed] {
                let base = record.base();
                let attributes = elephant_policy::RecordAttributes {
                    space_id: base.space_id.clone(),
                    security: base.security.clone(),
                    subjects: base.subjects.clone(),
                    subjects_state: base.subjects_state,
                };
                ctx.authz
                    .evaluate(&ctx.principal, &attributes, required_scope)
                    .map_err(McpError::ExecutionError)?;
            }
            prepared.commit(actor).await?;
        }
        let description = format!(
            "Reversed {:?} on {record_type} with current authority",
            audit.action
        );
        let output = RevertAuditOutput {
            success: true,
            record_type,
            record_id,
            description,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `stream_events` tool.
pub struct StreamEventsHandler {
    repo: SystemEventRepository,
}

impl StreamEventsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SystemEventRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for StreamEventsHandler {
    fn name(&self) -> &'static str {
        "stream_events"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Stream system events from a cursor position.\n\n",
            "Example:\n",
            r#"{ "space_id": "default", "cursor": "0", "limit": 100 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "space_id": {
                    "type": "string",
                    "default": "default",
                    "description": "Space ID to stream events from"
                },
                "cursor": {
                    "type": "string",
                    "description": "Cursor position (sequence number)"
                },
                "limit": {
                    "type": "integer",
                    "default": 100,
                    "maximum": 1000,
                    "description": "Maximum events to return"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: StreamEventsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if input.space_id != space_id {
            let response: ToolResponse<()> =
                ToolResponse::error("Access denied: access to space denied");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);
        if let Err(reason) = ctx
            .authz
            .evaluate(&ctx.principal, &record, "read:system_event")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let after_seq = input
            .cursor
            .as_ref()
            .and_then(|c| c.parse::<i64>().ok())
            .unwrap_or(0);

        let limit = input.limit.clamp(1, 1000);

        let mut collected: Vec<SystemEvent> = Vec::new();
        let mut page_after_seq = after_seq;
        let mut has_more_unfiltered = true;
        let mut safety_limit = 0usize;

        while collected.len() < limit && has_more_unfiltered {
            safety_limit += 1;
            if safety_limit > 1000 {
                return Err(McpError::ExecutionError(
                    "Event stream pagination exceeded safety limit".to_string(),
                ));
            }

            let page = self
                .repo
                .get_after_seq(&space_id, page_after_seq, limit)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;

            has_more_unfiltered = page.len() >= limit;
            if page.is_empty() {
                break;
            }

            page_after_seq = page.last().map_or(page_after_seq, |e| e.seq);

            let filtered = ctx
                .authz
                .filter_records(&ctx.principal, page, "read:system_event");

            for event in filtered {
                if collected.len() >= limit {
                    break;
                }
                collected.push(event);
            }
        }

        let next_cursor = collected.last().map(|e| e.seq.to_string());
        let has_more = collected.len() >= limit;

        let events_json: Vec<Value> = collected
            .into_iter()
            .map(|e| serde_json::to_value(e).unwrap_or_default())
            .collect();

        let output = StreamEventsOutput {
            events: events_json,
            next_cursor,
            has_more,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_tool_schema` tool.
#[derive(Default)]
pub struct GetToolSchemaHandler;

impl GetToolSchemaHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ToolHandler for GetToolSchemaHandler {
    fn name(&self) -> &'static str {
        "get_tool_schema"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Return the full JSON schema for a tool, including required fields and examples.\n\n",
            "Example:\n",
            r#"{ "tool_name": "ingest_doc" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["tool_name"],
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "Tool name to inspect"
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the dispatcher scope through tool metadata lookup and response construction."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetToolSchemaInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let dispatcher = super::create_dispatcher_with_context(ctx.clone());
        let Some(meta) = dispatcher.get_tool_meta(&input.tool_name) else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Tool not found: {}", input.tool_name));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        let output = GetToolSchemaOutput {
            name: meta.name,
            description: meta.description,
            owner: meta.owner,
            input_schema: meta.input_schema,
        };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_job_status` tool.
pub struct GetJobStatusHandler {
    repo: WorkItemRepository,
}

impl GetJobStatusHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetJobStatusHandler {
    fn name(&self) -> &'static str {
        "get_job_status"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get the status and progress payload for a job (work item).\n\n",
            "Example:\n",
            r#"{ "job_id": "work_item:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["job_id"],
            "properties": {
                "job_id": { "type": "string", "description": "Job/work item ID" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetJobStatusInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.job_id, TableName::WorkItem)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let Some(work_item) = self
            .repo
            .get(&space_id, &rid)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?
        else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Job not found: {}", input.job_id));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &work_item, "read:work_item")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let output = GetJobStatusOutput {
            job_id: work_item.base.id.to_string(),
            status: work_item.status.to_string(),
            kind: work_item.kind,
            payload: work_item.payload,
            attempts: work_item.attempts,
            last_error: work_item.last_error,
        };
        let response: ToolResponse<GetJobStatusOutput> = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_capabilities` tool.
pub struct GetCapabilitiesHandler {
    storage: StorageClient,
    advertise_snapshot_tools: bool,
    registered_tools: Vec<String>,
}

impl GetCapabilitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(
        storage: StorageClient,
        advertise_snapshot_tools: bool,
        mut registered_tools: Vec<String>,
    ) -> Self {
        if !registered_tools
            .iter()
            .any(|tool_name| tool_name == "get_capabilities")
        {
            registered_tools.push("get_capabilities".to_string());
        }
        registered_tools.sort();
        registered_tools.dedup();

        Self {
            storage,
            advertise_snapshot_tools,
            registered_tools,
        }
    }

    fn tool_usable_for_principal(&self, ctx: &ToolContext, tool_name: &str) -> bool {
        tool_usable_for_principal(
            ctx,
            tool_name,
            self.advertise_snapshot_tools,
            self.storage.is_embedded(),
        )
    }

    fn features_from_tools(tools: &[String]) -> Vec<String> {
        capability_features_from_tools(tools)
    }

    fn configured_extensions_from_env_value(value: Option<&str>) -> Vec<ExtensionInfo> {
        value
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(|name| ExtensionInfo {
                        name: name.to_string(),
                        version: "configured".to_string(),
                        enabled: true,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn configured_extensions() -> McpResult<Vec<ExtensionInfo>> {
        let value = std::env::var("ELEPHANT_EXTENSIONS").ok();
        let names = enabled_extension_names(value.as_deref());
        if names.is_empty() {
            return Ok(Vec::new());
        }

        load_runtime_reference_registry_from_env().map_err(|e| {
            McpError::ExecutionError(format!("failed to load extension manifests: {e}"))
        })?;
        Ok(Self::configured_extensions_from_env_value(value.as_deref()))
    }
}

/// Returns whether a registered MCP tool should be visible/usable for the
/// current principal under the given dispatcher configuration.
#[must_use]
pub fn tool_usable_for_principal(
    ctx: &ToolContext,
    tool_name: &str,
    advertise_snapshot_tools: bool,
    embedded_snapshots_supported: bool,
) -> bool {
    match tool_name {
        "get_capabilities" => true,
        "create_wiki_from_graph" | "compile_wiki_article" | "stop_managed_wiki" => {
            super::knowledge_views::authorize_wiki_management(ctx).is_ok()
        }
        "get_health" => authorize_global_system_access(ctx, "read:system").is_ok(),
        "export_snapshot" => {
            advertise_snapshot_tools
                && embedded_snapshots_supported
                && authorize_global_system_access(ctx, "read:system").is_ok()
        }
        "import_snapshot" => {
            advertise_snapshot_tools
                && embedded_snapshots_supported
                && authorize_global_system_access(ctx, "write:system").is_ok()
        }
        "revert_audit" => {
            if ctx.principal.space_id.is_empty() {
                return false;
            }

            evaluate_space_scope(ctx, "read:audit")
                && REVERT_AUDIT_WRITE_SCOPES
                    .iter()
                    .any(|scope| evaluate_space_scope(ctx, scope))
        }
        _ => {
            if ctx.principal.space_id.is_empty() {
                return false;
            }

            match capability_scope_requirement(tool_name) {
                ScopeRequirement::None => true,
                ScopeRequirement::All(scopes) => {
                    scopes.iter().all(|scope| evaluate_space_scope(ctx, scope))
                }
                ScopeRequirement::Any(scopes) => {
                    scopes.iter().any(|scope| evaluate_space_scope(ctx, scope))
                }
            }
        }
    }
}

/// Filters a registered tool-name set down to the names that should be visible
/// to the current principal.
#[must_use]
pub fn visible_tool_names_for_principal(
    ctx: &ToolContext,
    registered_tools: &[String],
    advertise_snapshot_tools: bool,
    embedded_snapshots_supported: bool,
) -> Vec<String> {
    let mut tools: Vec<String> = registered_tools
        .iter()
        .filter(|tool_name| {
            tool_usable_for_principal(
                ctx,
                tool_name,
                advertise_snapshot_tools,
                embedded_snapshots_supported,
            )
        })
        .cloned()
        .collect();
    tools.sort();
    tools
}

/// Derives capability feature flags from a sorted list of visible tool names.
#[must_use]
pub fn capability_features_from_tools(tools: &[String]) -> Vec<String> {
    let has = |tool_name: &str| {
        tools
            .binary_search_by(|item| item.as_str().cmp(tool_name))
            .is_ok()
    };
    let has_any = |tool_names: &[&str]| tool_names.iter().any(|tool_name| has(tool_name));
    let mut features = Vec::new();

    if has_any(&[
        "search_entities",
        "get_entity",
        "get_entity_with_relations",
        "update_entity",
        "delete_entity",
        "restore_entity",
        "batch_entities",
        "create_entity_simple",
    ]) {
        features.push("entity_management".to_string());
    }
    if has_any(&[
        "relate_entities",
        "upsert_relationship",
        "update_relationship",
        "delete_relationship",
        "restore_relationship",
        "batch_relationships",
    ]) {
        features.push("relationship_management".to_string());
    }
    if has_any(&[
        "search_docs",
        "ingest_doc",
        "ingest_doc_simple",
        "ingest_file",
        "ingest_directory",
        "ingest_url",
        "extract_doc",
        "get_doc_pipeline_status",
        "list_doc_collections",
        "get_doc",
        "list_doc_chunks",
        "get_doc_chunk",
        "delete_doc",
        "restore_doc",
        "batch_ingest_docs",
    ]) {
        features.push("document_ingestion".to_string());
    }
    if has_any(&[
        "list_assertions",
        "get_assertion",
        "search_assertions",
        "list_truth_slots",
        "get_truth_slot_history",
        "get_provenance",
    ]) {
        features.push("fact_promotion".to_string());
    }
    if has_any(&[
        "get_entity_context_bundle",
        "propose_identity_candidate",
        "list_identity_candidates",
        "merge_entities",
    ]) {
        features.push("identity_resolution".to_string());
    }
    if has_any(&[
        "get_knowledge_view",
        "get_knowledge_view_definition",
        "rebuild_knowledge_view",
        "materialize_knowledge_views",
        "upsert_knowledge_view_definition",
        "get_source_watch",
        "configure_source_watch",
        "check_source_watch",
    ]) {
        features.push("living_wiki".to_string());
    }
    if has_any(&[
        "get_source_watch",
        "configure_source_watch",
        "check_source_watch",
    ]) {
        features.push("source_freshness".to_string());
    }
    if has_any(&[
        "search_sources",
        "get_source",
        "create_source",
        "create_source_simple",
        "update_source",
        "delete_source",
        "restore_source",
    ]) {
        features.push("source_management".to_string());
    }
    if has("revert_audit") {
        features.push("audit_logging".to_string());
    }
    if has_any(&[
        "stream_events",
        "search_events",
        "get_event",
        "list_recent_events",
        "list_upcoming_events",
    ]) {
        features.push("event_streaming".to_string());
    }
    if has("search_all") {
        features.push("unified_search".to_string());
    }
    if has_any(&["batch_entities", "batch_relationships", "batch_ingest_docs"]) {
        features.push("batch_operations".to_string());
    }
    if has_any(&[
        "delete_entity",
        "restore_entity",
        "delete_relationship",
        "restore_relationship",
        "delete_doc",
        "restore_doc",
        "delete_source",
        "restore_source",
    ]) {
        features.push("soft_delete".to_string());
    }
    if has("export_snapshot") || has("import_snapshot") {
        features.push("embedded_snapshots".to_string());
    }

    features
}

enum ScopeRequirement {
    None,
    All(&'static [&'static str]),
    Any(&'static [&'static str]),
}

#[expect(
    clippy::match_same_arms,
    reason = "Keep scoped search and document-reading permissions as explicit independently reviewable rows."
)]
fn capability_scope_requirement(tool_name: &str) -> ScopeRequirement {
    match tool_name {
        "search_entities"
        | "get_entity"
        | "get_entity_with_relations"
        | "get_entity_context_bundle"
        | "list_identity_candidates"
        | "list_truth_slots"
        | "get_knowledge_view_definition" => ScopeRequirement::All(&["read:entity"]),
        "get_knowledge_view" | "materialize_knowledge_views" => {
            ScopeRequirement::Any(&["read:entity", "read:source"])
        }
        "update_entity"
        | "delete_entity"
        | "restore_entity"
        | "batch_entities"
        | "create_entity_simple"
        | "propose_identity_candidate"
        | "upsert_knowledge_view_definition" => ScopeRequirement::All(&["write:entity"]),
        "rebuild_knowledge_view" => ScopeRequirement::Any(&["write:entity", "write:source"]),
        "merge_entities" => ScopeRequirement::All(&["write:entity", "write:relationship"]),
        "relate_entities"
        | "upsert_relationship"
        | "update_relationship"
        | "delete_relationship"
        | "restore_relationship"
        | "batch_relationships" => ScopeRequirement::All(&["write:relationship"]),
        "search_docs"
        | "list_doc_collections"
        | "get_doc"
        | "list_doc_chunks"
        | "get_doc_pipeline_status" => ScopeRequirement::All(&["read:doc:metadata"]),
        "search_scoped" => ScopeRequirement::All(&["read:doc:raw"]),
        "get_doc_chunk" | "extract_doc" => ScopeRequirement::All(&["read:doc:raw"]),
        "ingest_doc" | "ingest_doc_simple" | "delete_doc" | "restore_doc" | "batch_ingest_docs" => {
            ScopeRequirement::All(&["write:doc"])
        }
        "ingest_file" | "ingest_directory" | "ingest_url" => {
            ScopeRequirement::All(&["write:doc", "write:source"])
        }
        "search_sources" | "get_source" | "get_source_watch" => {
            ScopeRequirement::All(&["read:source"])
        }
        "get_provenance" => {
            ScopeRequirement::All(&["read:assertion", "read:doc:metadata", "read:doc:raw"])
        }
        "create_source"
        | "create_source_simple"
        | "update_source"
        | "delete_source"
        | "restore_source"
        | "configure_source_watch"
        | "check_source_watch" => ScopeRequirement::All(&["write:source"]),
        "list_predicates" | "get_predicate" => ScopeRequirement::All(&["read:predicate"]),
        "list_assertions" | "get_assertion" | "search_assertions" => {
            ScopeRequirement::All(&["read:assertion"])
        }
        "get_truth_slot_history" => ScopeRequirement::All(&["read:entity", "read:assertion"]),
        "register_predicate"
        | "propose_predicate_alias"
        | "validate_predicate_proposal"
        | "approve_predicate_proposal" => ScopeRequirement::All(&["write:predicate"]),
        "add_timeline_event" => ScopeRequirement::All(&["write:event"]),
        "search_events" | "get_event" | "list_recent_events" | "list_upcoming_events" => {
            ScopeRequirement::All(&["read:event"])
        }
        "list_work_items" | "list_pending_work_items" | "get_job_status" => {
            ScopeRequirement::All(&["read:work_item"])
        }
        "claim_work_item" | "complete_work_item" | "retry_work_item" => {
            ScopeRequirement::All(&["read:work_item", "write:work_item"])
        }
        "search_all" => ScopeRequirement::Any(&[
            "read:entity",
            "read:relationship",
            "read:doc:metadata",
            "read:source",
        ]),
        "revert_audit" => ScopeRequirement::All(&["read:audit"]),
        "stream_events" => ScopeRequirement::All(&["read:system_event"]),
        "get_source_head" | "get_source_operation" => ScopeRequirement::All(&["read:source"]),
        "retract_source" | "erase_source" => {
            ScopeRequirement::All(&["read:source", "write:source"])
        }
        "create_space" | "update_space" => ScopeRequirement::All(&["write:space_config"]),
        "list_spaces" | "get_space" | "get_embedding_reindex" => {
            ScopeRequirement::All(&["read:space_config"])
        }
        "start_embedding_reindex" | "activate_embedding_reindex" | "cancel_embedding_reindex" => {
            ScopeRequirement::All(&["read:space_config", "write:space_config", "manage:provider"])
        }
        _ => ScopeRequirement::None,
    }
}

fn evaluate_space_scope(ctx: &ToolContext, required_scope: &str) -> bool {
    let space_id = &ctx.principal.space_id;
    if space_id.is_empty() {
        return false;
    }

    let policy_id = ctx.principal.clearances.first().map_or_else(
        || "default".to_string(),
        |clearance| clearance.policy_id.clone(),
    );
    let record = crate::authz::AuthzService::preflight_record(space_id, &policy_id);
    ctx.authz
        .evaluate(&ctx.principal, &record, required_scope)
        .is_ok()
}

#[async_trait]
impl ToolHandler for GetCapabilitiesHandler {
    fn name(&self) -> &'static str {
        "get_capabilities"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get system capabilities and available tools.\n\n",
            "Example:\n",
            r#"{}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {}
        })
    }

    async fn execute(&self, ctx: &ToolContext, _params: Value) -> McpResult<Value> {
        let mut tools: Vec<String> = self
            .registered_tools
            .iter()
            .filter(|tool_name| self.tool_usable_for_principal(ctx, tool_name))
            .cloned()
            .collect();
        tools.sort();

        let features = Self::features_from_tools(&tools);

        let output = CapabilitiesOutput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            backend: self.storage.backend().as_str().to_string(),
            tools,
            features,
            extensions: Self::configured_extensions()?,
            limits: CapabilityLimits {
                max_batch_size: 100,
                max_search_results: 100,
                max_event_stream_batch: 1000,
            },
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_health` tool.
pub struct GetHealthHandler {
    storage: StorageClient,
}

impl GetHealthHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ToolHandler for GetHealthHandler {
    fn name(&self) -> &'static str {
        "get_health"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get lightweight system health status. The response includes a ",
            "`budget` section ({exhausted_modes, last_exhausted_at}) when any ",
            "mode's daily pipeline budget was exhausted today; the section is ",
            "absent when budgets are healthy.\n\n",
            "Example:\n",
            r#"{}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {}
        })
    }

    async fn execute(&self, ctx: &ToolContext, _params: Value) -> McpResult<Value> {
        if let Err(reason) = authorize_global_system_access(ctx, "read:system") {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let db_ok = self.storage.health_check().await.is_ok();
        let tables = if db_ok {
            self.storage.list_tables().await.unwrap_or_default()
        } else {
            Vec::new()
        };
        let budget = if db_ok {
            budget_exhaustion_health(&SystemEventRepository::new(self.storage.clone())).await
        } else {
            None
        };

        let output = HealthStatusOutput {
            status: if db_ok {
                "healthy".to_string()
            } else {
                "unhealthy".to_string()
            },
            backend: self.storage.backend().as_str().to_string(),
            storage: if db_ok {
                "connected".to_string()
            } else {
                "disconnected".to_string()
            },
            surrealdb: if db_ok {
                "connected".to_string()
            } else {
                "disconnected".to_string()
            },
            db_ok,
            table_count: tables.len(),
            tables,
            timestamp: Utc::now().to_rfc3339(),
            budget,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Derives the `get_health` budget section from today's `budget_exhausted`
/// system events. Returns `None` when healthy — the section is then absent
/// from the response entirely. One event-type query; spend is never
/// recomputed here.
async fn budget_exhaustion_health(event_repo: &SystemEventRepository) -> Option<BudgetHealth> {
    let today_start = Utc.from_utc_datetime(&Utc::now().date_naive().and_hms_opt(0, 0, 0)?);
    let events = event_repo
        .find_recent_by_type_since(BUDGET_EXHAUSTED_EVENT_TYPE, today_start, 100)
        .await
        .unwrap_or_default();
    if events.is_empty() {
        return None;
    }

    let mut exhausted_modes: Vec<String> = events
        .iter()
        .filter_map(|event| {
            event
                .payload
                .get("mode")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect();
    exhausted_modes.sort();
    exhausted_modes.dedup();

    let last_exhausted_at = events
        .iter()
        .map(|event| event.base.created_at)
        .max()?
        .to_rfc3339();

    Some(BudgetHealth {
        exhausted_modes,
        last_exhausted_at,
    })
}

fn authorize_global_system_access(ctx: &ToolContext, required_scope: &str) -> Result<(), String> {
    if ctx.authz.is_skip_auth() {
        return Ok(());
    }

    let has_scope =
        ctx.principal.scopes.contains("*") || ctx.principal.scopes.contains(required_scope);
    if !has_scope {
        return Err(format!(
            "Access denied: missing required scope `{required_scope}`"
        ));
    }

    if !ctx.principal.space_id.is_empty() {
        return Err("Access denied: snapshot operations require a global principal".to_string());
    }

    Ok(())
}

fn resolve_snapshot_host_path(requested: &str, for_import: bool) -> Result<PathBuf, String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Err("Snapshot path is required".to_string());
    }

    let configured_root = std::env::var_os(SNAPSHOT_DIR_ENV).map_or_else(
        || {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".elephant-snapshots")
        },
        PathBuf::from,
    );
    std::fs::create_dir_all(&configured_root)
        .map_err(|err| format!("Failed to prepare snapshot directory: {err}"))?;
    let root = configured_root
        .canonicalize()
        .map_err(|err| format!("Failed to resolve snapshot directory: {err}"))?;

    let requested_path = PathBuf::from(requested);
    let candidate = if requested_path.is_absolute() {
        requested_path
    } else {
        root.join(requested_path)
    };

    let resolved = if for_import {
        candidate
            .canonicalize()
            .map_err(|err| format!("Snapshot path is invalid or missing: {err}"))?
    } else {
        let file_name = candidate
            .file_name()
            .ok_or_else(|| "Snapshot path must include a file name".to_string())?;
        let parent = candidate.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|err| format!("Failed to create snapshot parent directory: {err}"))?;
        let parent = parent
            .canonicalize()
            .map_err(|err| format!("Failed to resolve snapshot parent directory: {err}"))?;
        parent.join(file_name)
    };

    if !resolved.starts_with(&root) {
        return Err(format!(
            "Snapshot paths must stay inside {}",
            root.display()
        ));
    }

    Ok(resolved)
}

async fn execute_named_snapshot(
    ctx: &ToolContext,
    storage: &StorageClient,
    input: SnapshotOperationInput,
    restore: bool,
) -> McpResult<Value> {
    use elephant_types::public_error::{ErrorCode, PublicError};
    let error = |code, message: &str| McpError::Public(PublicError::new(code, message));
    authorize_global_system_access(
        ctx,
        if restore {
            "write:system"
        } else {
            "read:system"
        },
    )
    .map_err(|_| {
        error(
            ErrorCode::ForbiddenSpace,
            "Snapshot operations require an authorized global principal",
        )
    })?;
    if ctx
        .verified_caller()
        .is_some_and(|caller| caller.claims().spaces.is_some())
    {
        return Err(error(
            ErrorCode::ForbiddenSpace,
            "Space service tokens cannot operate global snapshots",
        ));
    }
    let path = resolve_snapshot_host_path(&input.path, restore)
        .map_err(|_| error(ErrorCode::InvalidRequest, "Invalid snapshot path"))?;
    let defaults = elephant_storage::snapshot::SnapshotRetention::default();
    let retention = elephant_storage::snapshot::SnapshotRetention {
        retention_seconds: input
            .retention_seconds
            .unwrap_or(defaults.retention_seconds),
        quiesce_timeout_seconds: input
            .quiesce_timeout_seconds
            .unwrap_or(defaults.quiesce_timeout_seconds),
    };
    if !(1..=2_592_000).contains(&retention.retention_seconds)
        || !(1..=60).contains(&retention.quiesce_timeout_seconds)
    {
        return Err(error(
            ErrorCode::InvalidRequest,
            "Snapshot retention or quiesce timeout exceeds its bound",
        ));
    }
    let name = input.name.unwrap_or_else(|| "manual".into());
    let manifest = if let Some(coordinator) = ctx.snapshot_coordinator() {
        coordinator
            .run(path.clone(), name, retention, restore)
            .await?
    } else {
        let result = if restore {
            elephant_storage::snapshot::restore_named_with_timeout(
                storage,
                &path,
                storage
                    .erasure_journal()
                    .map_err(|_| error(ErrorCode::NotReady, "Durable journal unavailable"))?,
                std::time::Duration::from_secs(retention.quiesce_timeout_seconds),
            )
            .await
        } else {
            elephant_storage::snapshot::export_named(storage, &path, &name, &retention).await
        };
        result.map_err(|failure| {
            tracing::warn!(error=%failure,restore,"Native MCP snapshot failed");
            error(
                ErrorCode::Unavailable,
                "Snapshot operation unavailable; drain workers and inspect operator logs",
            )
        })?
    };
    let response = ToolResponse::success(SnapshotOperationOutput {
        success: true,
        backend: storage.backend().as_str().into(),
        path: path.display().to_string(),
        message: if restore {
            "Snapshot restored after erasure replay"
        } else {
            "Consistent snapshot exported with bounded retention"
        }
        .into(),
        manifest,
    });
    serde_json::to_value(response).map_err(|failure| McpError::ExecutionError(failure.to_string()))
}

/// Handler for `export_snapshot` tool.
pub struct ExportSnapshotHandler {
    storage: StorageClient,
}

impl ExportSnapshotHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ToolHandler for ExportSnapshotHandler {
    fn name(&self) -> &'static str {
        "export_snapshot"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Export a consistent named snapshot with a manifest and bounded retention.\n\n",
            "Example:\n",
            r#"{ "path": "/tmp/elephant-snapshot.json" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Filesystem path on the Elephant host"
                },
                "name": {"type":"string"},
                "retention_seconds": {"type":"integer","minimum":1,"maximum":2_592_000},
                "quiesce_timeout_seconds": {"type":"integer","minimum":1,"maximum":60}
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let input: SnapshotOperationInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        execute_named_snapshot(ctx, &self.storage, input, false).await
    }
}

/// Handler for `import_snapshot` tool.
pub struct ImportSnapshotHandler {
    storage: StorageClient,
}

impl ImportSnapshotHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ToolHandler for ImportSnapshotHandler {
    fn name(&self) -> &'static str {
        "import_snapshot"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Restore a named snapshot after durable erasure replay.\n\n",
            "Example:\n",
            r#"{ "path": "/tmp/elephant-snapshot.json" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Filesystem path on the Elephant host"
                },
                "name": {"type":"string"},
                "retention_seconds": {"type":"integer","minimum":1,"maximum":2_592_000},
                "quiesce_timeout_seconds": {"type":"integer","minimum":1,"maximum":60}
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let input: SnapshotOperationInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        execute_named_snapshot(ctx, &self.storage, input, true).await
    }
}

/// Handler for provenance tracing.
///
/// Given a relationship or attribute ID, traces back through assertions
/// to the source document evidence chain.
pub struct GetProvenanceHandler {
    storage: StorageClient,
}

impl GetProvenanceHandler {
    /// Creates a new provenance handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ToolHandler for GetProvenanceHandler {
    fn name(&self) -> &'static str {
        "get_provenance"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Trace provenance for a relationship or attribute back to source evidence.\n\n",
            "Given a rel or attribute ID, returns the chain: claim -> assertion(s) -> doc_node -> doc_revision -> doc.\n",
            "Each evidence link includes the source text span, quote hash, and document metadata.\n\n",
            "Example:\n",
            r#"{ "id": "rel:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "id": {
                    "description": "The relationship or attribute ID to trace provenance for (e.g. rel:uuid or attribute:uuid)",
                    "type": "string"
                }
            },
            "required": ["id"]
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep provenance repository scopes through authorization and evidence response assembly."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        use elephant_storage::{AttributeRepository, RelRepository};

        let params = coerce_params(params, &self.input_schema());
        let id_str = params
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::InvalidParameters("id is required".to_string()))?;

        for scope in ["read:assertion", "read:doc:metadata", "read:doc:raw"] {
            if !evaluate_space_scope(ctx, scope) {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: missing {scope} scope"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        }

        let space_id = ctx.principal.space_id.clone();

        // Determine whether this is a rel or attribute based on the table prefix
        let (record_type, evidence_refs) = if id_str.starts_with("rel:") {
            if !evaluate_space_scope(ctx, "read:relationship") {
                let response: ToolResponse<()> =
                    ToolResponse::error("Access denied: missing read:relationship scope");
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            let rid = parse_id(id_str, TableName::Rel)
                .map_err(|e| McpError::InvalidParameters(format!("Invalid rel ID: {e}")))?;
            let rel_repo = RelRepository::new(self.storage.clone());
            let rel = rel_repo
                .get(&space_id, &rid)
                .await
                .map_err(|e| McpError::ExecutionError(format!("Failed to get rel: {e}")))?
                .ok_or_else(|| McpError::ExecutionError(format!("Rel not found: {id_str}")))?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &rel, "read:relationship")
                .map_err(McpError::ExecutionError)?;
            let rel = crate::current_read::require(ctx, rel, "read:relationship").await?;
            (
                json!({
                    "type": "rel",
                    "id": id_str,
                    "rel_type": rel.rel_type,
                    "subject": rel.subject.id().to_string(),
                    "object": rel.object.id().to_string(),
                    "status": format!("{:?}", rel.status),
                    "confidence": rel.confidence,
                }),
                rel.evidence,
            )
        } else if id_str.starts_with("attribute:") {
            let rid = parse_id(id_str, TableName::Attribute)
                .map_err(|e| McpError::InvalidParameters(format!("Invalid attribute ID: {e}")))?;
            let attr_repo = AttributeRepository::new(self.storage.clone());
            let attr = attr_repo
                .get(&space_id, &rid)
                .await
                .map_err(|e| McpError::ExecutionError(format!("Failed to get attribute: {e}")))?
                .ok_or_else(|| {
                    McpError::ExecutionError(format!("Attribute not found: {id_str}"))
                })?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &attr, "read:entity")
                .map_err(McpError::ExecutionError)?;
            let attr = crate::current_read::require(ctx, attr, "read:entity").await?;
            (
                json!({
                    "type": "attribute",
                    "id": id_str,
                    "attr_type": attr.attr_type,
                    "subject": attr.subject.id().to_string(),
                    "value": format!("{:?}", attr.value),
                    "status": format!("{:?}", attr.status),
                    "confidence": attr.confidence,
                }),
                attr.evidence,
            )
        } else {
            return Err(McpError::InvalidParameters(
                "ID must start with 'rel:' or 'attribute:'".to_string(),
            ));
        };

        let mut chain = Vec::new();
        for evidence_ref in &evidence_refs {
            let resolved = crate::evidence::load_evidence(
                &self.storage,
                &space_id,
                &evidence_ref.span,
                Some(evidence_ref.assertion.id()),
            )
            .await?;
            let assertion = resolved
                .assertion
                .as_ref()
                .ok_or_else(|| McpError::ExecutionError("Evidence unavailable".into()))?;
            ctx.authz
                .authorize_record_read(&ctx.principal, assertion, "read:assertion")
                .map_err(McpError::ExecutionError)?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &resolved.revision, "read:doc:raw")
                .map_err(McpError::ExecutionError)?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &resolved.doc, "read:doc:metadata")
                .map_err(McpError::ExecutionError)?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &resolved.doc, "read:doc:raw")
                .map_err(McpError::ExecutionError)?;
            if let Some(source) = &resolved.source {
                // The citation discloses source content; use the caller's raw
                // document scope with the CURRENT source security envelope.
                ctx.authz
                    .authorize_record_read(&ctx.principal, source, "read:doc:raw")
                    .map_err(McpError::ExecutionError)?;
            }
            if let Some(node) = &resolved.node {
                ctx.authz
                    .authorize_record_read(&ctx.principal, node, "read:doc:raw")
                    .map_err(McpError::ExecutionError)?;
            }
            let mut link = json!({
                "assertion_id": evidence_ref.assertion.id().to_string(),
                "time_basis": format!("{:?}", evidence_ref.time_basis),
                "time_value": evidence_ref.time_value.to_rfc3339(),
                "span": {
                    "revision_id": evidence_ref.span.revision.id().to_string(),
                    "start": evidence_ref.span.start,
                    "end": evidence_ref.span.end,
                    "quote_hash": evidence_ref.span.quote_hash,
                },
                "assertion": {
                    "subject_text": assertion.subject_text,
                    "predicate_raw": assertion.predicate_raw,
                    "object_text": assertion.object_text,
                    "confidence": assertion.confidence,
                    "resolution_status": format!("{:?}", assertion.resolution_status),
                    "observed_at": assertion.observed_at.to_rfc3339(),
                },
                "revision": {
                    "id": resolved.revision.base.id.to_string(),
                    "renderer": resolved.revision.renderer.name,
                    "renderer_version": resolved.revision.renderer.version,
                    "status": format!("{:?}", resolved.revision.status),
                    "rendered_at": resolved.revision.rendered_at.to_rfc3339(),
                },
                "doc": { "id": resolved.doc.base.id.to_string(), "title": resolved.doc.title },
                "quote": resolved.quote,
            });
            if let Some(node) = resolved.node {
                // Expose only the validated source passage, never unvalidated
                // summary prose stored in the node's optional text field.
                link["node"] = json!({ "id": node.base.id.to_string(), "role": node.role.to_string(),
                    "node_key": node.node_key, "text": node.text });
            }
            if let Some(source) = resolved.source {
                link["doc"]["source_id"] = json!(source.base.id.to_string());
            }
            chain.push(link);
        }

        let response = ToolResponse::success(json!({
            "claim": record_type,
            "evidence_count": evidence_refs.len(),
            "chain": chain,
        }));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

#[cfg(test)]
#[path = "provenance_tests.rs"]
mod provenance_tests;

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
    use super::*;
    use chrono::Utc;
    use elephant_policy::{Clearance, Principal};
    use elephant_storage::{AuditLogRepository, EntityRepository, Repository, StorageClient};
    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::work::WorkItemStatus;
    use elephant_types::records::{AuditAction, AuditLog, Entity, WorkItem};
    use std::{collections::HashSet, sync::Arc};

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[serial_test::serial]
    #[test]
    fn test_stream_events_input_defaults() {
        let input: StreamEventsInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 100);
        assert_eq!(input.space_id, "default");
        assert!(input.cursor.is_none());
    }

    #[serial_test::serial]
    #[test]
    fn test_capabilities_output_serialization() {
        let output = CapabilitiesOutput {
            version: "1.0.0".to_string(),
            backend: "surreal-db".to_string(),
            tools: vec!["search_entities".to_string()],
            features: vec!["entity_management".to_string()],
            extensions: vec![],
            limits: CapabilityLimits {
                max_batch_size: 100,
                max_search_results: 100,
                max_event_stream_batch: 1000,
            },
        };
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("version"));
        assert!(json.contains("tools"));
        assert!(json.contains("features"));
    }

    fn capabilities_tool_inventory(include_snapshot_tools: bool) -> Vec<String> {
        let mut tools = vec![
            "get_capabilities".to_string(),
            "get_health".to_string(),
            "search_entities".to_string(),
            "update_entity".to_string(),
        ];
        if include_snapshot_tools {
            tools.push("export_snapshot".to_string());
            tools.push("import_snapshot".to_string());
        }
        tools
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_revert_audit_restores_soft_deleted_entity() {
        let storage = StorageClient::new_memory().await.expect("storage");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("complete native schema before fixture writes");
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"default","created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete","embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        })).expect("space fixture");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
        let entity_repo = EntityRepository::new(storage.clone());
        let audit_repo = AuditLogRepository::new(storage.clone());
        let handler = RevertAuditHandler::new(storage.clone());

        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "default".to_string(),
                "tester".to_string(),
            ),
            EntityType::new("person"),
            "Alice".to_string(),
        );
        entity.base.security = Security::default();
        entity.base.provenance = Provenance::default();
        entity.base.subjects_state = SubjectsState::Complete;
        let entity_id = entity.base.id.clone();
        let created = entity_repo
            .create(entity.clone())
            .await
            .expect("create entity");
        entity_repo
            .delete("default", &entity_id)
            .await
            .expect("delete entity");

        let mut audit_base = BaseFields::new(
            RecordId::new_v4(TableName::AuditLog),
            "default".to_string(),
            "tester".to_string(),
        );
        audit_base.security = Security::default();
        audit_base.provenance = Provenance::default();
        audit_base.subjects_state = SubjectsState::Complete;
        let audit = AuditLog {
            base: audit_base,
            action: AuditAction::Delete,
            actor: "tester".to_string(),
            record: entity_id.clone(),
            before: Some(serde_json::to_value(&created).expect("snapshot")),
            after: None,
            timestamp: Utc::now(),
            reason: Some("test".to_string()),
        };
        let created_audit = audit_repo.create(audit).await.expect("create audit");

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:audit".to_string(), "write:entity".to_string()]),
            clearances: vec![Clearance {
                policy_id: String::new(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({ "audit_id": created_audit.base.id.to_string() }),
            )
            .await
            .expect("execute revert");
        let decoded: ToolResponse<RevertAuditOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(decoded.ok, "unexpected revert response: {decoded:?}");

        let restored = entity_repo
            .get("default", &entity_id)
            .await
            .expect("get entity")
            .expect("restored entity");
        assert!(restored.base.deleted_at.is_none());
        assert_eq!(restored.display_name, "Alice");
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_import_snapshot_rejects_running_embedded_workers() {
        let snapshot_root = temp_dir("elephant-mcp-running-snapshot");
        std::fs::create_dir_all(&snapshot_root).expect("create snapshot root");
        let storage = StorageClient::new_with_snapshot_destination(
            elephant_storage::StorageConfig {
                backend: elephant_storage::StorageBackendKind::ElephantDb,
                ..elephant_storage::StorageConfig::default()
            },
            Some(snapshot_root.join("live.snapshot")),
        )
        .await
        .expect("durable storage");
        let handler = ImportSnapshotHandler::new(storage.clone());
        let work_item_repo = WorkItemRepository::new(storage.clone());
        let snapshot_path = snapshot_root.join("snapshot");
        elephant_storage::snapshot::export_named(
            &storage,
            &snapshot_path,
            "before-work",
            &elephant_storage::snapshot::SnapshotRetention::default(),
        )
        .await
        .expect("valid named snapshot");
        std::env::set_var(SNAPSHOT_DIR_ENV, &snapshot_root);

        let work_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "default".to_string(),
                "tester".to_string(),
            ),
            kind: "render_doc".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Running,
            priority: 0,
            idempotency_key: "snapshot-running".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: Some("worker-1".to_string()),
            run_after: None,
            last_error: None,
        };
        work_item_repo
            .create(work_item.clone())
            .await
            .expect("create work item");

        let principal = Principal {
            space_id: String::new(),
            scopes: HashSet::from(["write:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let error = handler
            .execute(&ctx, serde_json::json!({ "path": "snapshot" }))
            .await
            .expect_err("import should be blocked while workers are running");
        assert!(matches!(
            error,
            McpError::Public(elephant_types::public_error::PublicError {
                error_code: elephant_types::public_error::ErrorCode::Unavailable,
                ..
            })
        ));
        // A refused restore must not discard the currently running work.
        assert!(work_item_repo
            .get("default", &work_item.base.id)
            .await
            .expect("live work remains readable after refused restore")
            .is_some());

        std::env::remove_var(SNAPSHOT_DIR_ENV);
        let _ = std::fs::remove_dir_all(snapshot_root);
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_snapshot_tools_reject_space_scoped_principal() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler = ExportSnapshotHandler::new(storage.clone());

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let error = handler
            .execute(&ctx, serde_json::json!({ "path": "/tmp/ignored.json" }))
            .await
            .expect_err("space-scoped snapshot forbidden");
        assert!(matches!(
            error,
            McpError::Public(elephant_types::public_error::PublicError {
                error_code: elephant_types::public_error::ErrorCode::ForbiddenSpace,
                ..
            })
        ));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_snapshot_tools_for_space_scoped_principal() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler = GetCapabilitiesHandler::new(storage, true, capabilities_tool_inventory(true));

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from([
                "read:system".to_string(),
                "write:system".to_string(),
                "read:entity".to_string(),
            ]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(
            StorageClient::new_embedded().await.expect("ctx storage"),
            principal,
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");
        assert!(!output.tools.iter().any(|tool| tool == "export_snapshot"));
        assert!(!output.tools.iter().any(|tool| tool == "import_snapshot"));
        assert!(!output
            .features
            .iter()
            .any(|feature| feature == "embedded_snapshots"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_expose_snapshot_tools_for_skip_auth_principal() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler =
            GetCapabilitiesHandler::new(storage.clone(), true, capabilities_tool_inventory(true));

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:system".to_string(), "write:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(crate::authz::AuthzService::new_with_skip_auth(true)),
            principal,
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");
        assert!(output.tools.iter().any(|tool| tool == "export_snapshot"));
        assert!(output.tools.iter().any(|tool| tool == "import_snapshot"));
        assert!(output
            .features
            .iter()
            .any(|feature| feature == "embedded_snapshots"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_allow_global_principal_without_space_id() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler =
            GetCapabilitiesHandler::new(storage.clone(), true, capabilities_tool_inventory(true));

        let principal = Principal {
            space_id: String::new(),
            scopes: HashSet::from(["read:system".to_string(), "write:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(crate::authz::AuthzService::new_with_skip_auth(false)),
            principal,
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");
        assert!(output.tools.iter().any(|tool| tool == "export_snapshot"));
        assert!(output.tools.iter().any(|tool| tool == "import_snapshot"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_snapshot_tools_when_not_registered() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler =
            GetCapabilitiesHandler::new(storage.clone(), false, capabilities_tool_inventory(false));

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:system".to_string(), "write:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(crate::authz::AuthzService::new_with_skip_auth(true)),
            principal,
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");
        assert!(!output.tools.iter().any(|tool| tool == "export_snapshot"));
        assert!(!output.tools.iter().any(|tool| tool == "import_snapshot"));
        assert!(!output
            .features
            .iter()
            .any(|feature| feature == "embedded_snapshots"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_work_item_and_predicate_write_tools_without_scopes() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetCapabilitiesHandler::new(
            storage.clone(),
            false,
            vec![
                "list_work_items".to_string(),
                "claim_work_item".to_string(),
                "list_predicates".to_string(),
                "register_predicate".to_string(),
            ],
        );

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from([
                "read:entity".to_string(),
                "read:predicate".to_string(),
                "read:work_item".to_string(),
            ]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");
        assert!(output.tools.iter().any(|tool| tool == "list_work_items"));
        assert!(!output.tools.iter().any(|tool| tool == "claim_work_item"));
        assert!(output.tools.iter().any(|tool| tool == "list_predicates"));
        assert!(!output.tools.iter().any(|tool| tool == "register_predicate"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_work_item_mutation_capabilities_require_read_and_write() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let tools = ["claim_work_item", "complete_work_item", "retry_work_item"];
        let handler = GetCapabilitiesHandler::new(
            storage.clone(),
            false,
            tools.iter().map(|tool| (*tool).to_owned()).collect(),
        );

        for (scopes, allowed) in [
            (vec!["read:work_item"], false),
            (vec!["write:work_item"], false),
            (vec!["read:work_item", "write:work_item"], true),
        ] {
            let ctx = ToolContext::new(
                storage.clone(),
                Principal {
                    space_id: "default".to_string(),
                    scopes: scopes.into_iter().map(str::to_owned).collect(),
                    clearances: vec![Clearance {
                        policy_id: "default".to_string(),
                        max_level: 0,
                        labels: HashSet::new(),
                        handling_allow: HashSet::new(),
                    }],
                    subject_allowlist: None,
                    purpose: HashSet::new(),
                },
            );
            let response = handler
                .execute(&ctx, serde_json::json!({}))
                .await
                .expect("capabilities");
            let decoded: ToolResponse<CapabilitiesOutput> =
                serde_json::from_value(response).expect("decode capabilities");
            assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
            let output = decoded.data.expect("capabilities output");
            for tool in tools {
                assert_eq!(
                    output.tools.iter().any(|candidate| candidate == tool),
                    allowed,
                    "incorrect capability advertisement for {tool}"
                );
            }
        }
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_unusable_tools_for_current_principal() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetCapabilitiesHandler::new(
            storage.clone(),
            false,
            vec![
                "get_capabilities".to_string(),
                "search_entities".to_string(),
                "update_entity".to_string(),
                "get_health".to_string(),
            ],
        );

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:entity".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");

        assert!(output.tools.iter().any(|tool| tool == "get_capabilities"));
        assert!(output.tools.iter().any(|tool| tool == "search_entities"));
        assert!(!output.tools.iter().any(|tool| tool == "update_entity"));
        assert!(!output.tools.iter().any(|tool| tool == "get_health"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_expose_get_health_for_global_principal() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetCapabilitiesHandler::new(
            storage.clone(),
            false,
            vec!["get_capabilities".to_string(), "get_health".to_string()],
        );

        let principal = Principal {
            space_id: String::new(),
            scopes: HashSet::from(["read:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");

        assert!(output.tools.iter().any(|tool| tool == "get_health"));
    }

    fn global_system_ctx(storage: StorageClient) -> ToolContext {
        let principal = Principal {
            space_id: String::new(),
            scopes: HashSet::from(["read:system".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        ToolContext::new(storage, principal)
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_get_health_budget_section_absent_when_healthy() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetHealthHandler::new(storage.clone());
        let ctx = global_system_ctx(storage);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("health");
        // The raw JSON must omit the key entirely when healthy.
        let data = response.get("data").expect("health data");
        assert!(
            data.get("budget").is_none(),
            "healthy response must have no budget section: {data:?}"
        );

        let decoded: ToolResponse<HealthStatusOutput> =
            serde_json::from_value(response).expect("decode health");
        assert!(decoded.ok);
        assert!(decoded.data.expect("health output").budget.is_none());
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_get_health_surfaces_budget_exhaustion() {
        use elephant_pipeline::scheduler::budget::BudgetEnforcer;
        use elephant_types::records::ingest::{Budget, DowngradePolicy, IngestMode};

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query(
                "DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE system_event SCHEMALESS;",
            )
            .await
            .expect("define tables");

        // Exhaust a family space's retrieval budget through the real
        // enforcer: the refusal writes the budget_exhausted system_event.
        let budget = Budget {
            daily_euros_retrieval: 0.5,
            downgrade_policy: DowngradePolicy::Strict,
            ..Budget::default()
        };
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "family-a".to_string());
        let refused = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 1.0)
            .await;
        assert!(refused.is_err(), "reservation over budget must be refused");

        // get_health runs with a GLOBAL principal and must still see the
        // space-scoped signal.
        let handler = GetHealthHandler::new(storage.clone());
        let ctx = global_system_ctx(storage);
        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("health");
        let decoded: ToolResponse<HealthStatusOutput> =
            serde_json::from_value(response).expect("decode health");
        assert!(decoded.ok);
        let budget_health = decoded
            .data
            .expect("health output")
            .budget
            .expect("budget section present after exhaustion");
        assert_eq!(budget_health.exhausted_modes, vec!["retrieval"]);
        assert!(
            !budget_health.last_exhausted_at.is_empty(),
            "last_exhausted_at must carry the event timestamp"
        );
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_get_provenance_without_required_scopes() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler =
            GetCapabilitiesHandler::new(storage.clone(), false, vec!["get_provenance".to_string()]);

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:relationship".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");

        assert!(!output.tools.iter().any(|tool| tool == "get_provenance"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_get_provenance_requires_assertion_and_doc_scopes() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetProvenanceHandler::new(storage.clone());
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:relationship".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({ "id": "rel:00000000-0000-0000-0000-000000000000" }),
            )
            .await
            .expect("tool response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .as_deref()
            .expect("error")
            .contains("read:assertion"));
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_capabilities_hide_revert_audit_without_write_scope() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetCapabilitiesHandler::new(
            storage.clone(),
            false,
            vec!["get_capabilities".to_string(), "revert_audit".to_string()],
        );

        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["read:audit".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage, principal);

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");
        let output = decoded.data.expect("capabilities output");

        assert!(!output.tools.iter().any(|tool| tool == "revert_audit"));
    }

    #[serial_test::serial]
    #[test]
    fn test_configured_extensions_follow_env_configuration() {
        let extensions = GetCapabilitiesHandler::configured_extensions_from_env_value(Some(
            "goldfish,sampleapp",
        ));
        let names: Vec<_> = extensions.iter().map(|ext| ext.name.as_str()).collect();
        assert_eq!(names, vec!["goldfish", "sampleapp"]);
        assert!(extensions.iter().all(|ext| ext.enabled));
    }

    #[serial_test::serial]
    #[test]
    fn test_configured_extensions_surface_manifest_errors() {
        let temp_dir = temp_dir("elephant-mcp-capabilities");
        std::env::set_var("ELEPHANT_EXTENSIONS", "missingext");
        std::env::set_var("ELEPHANT_EXTENSION_MANIFEST_DIR", &temp_dir);

        let error = GetCapabilitiesHandler::configured_extensions()
            .expect_err("missing manifest should fail");
        assert!(error
            .to_string()
            .contains("failed to load extension manifests"));

        std::env::remove_var("ELEPHANT_EXTENSIONS");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");
        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
