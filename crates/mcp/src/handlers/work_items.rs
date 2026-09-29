#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Work item MCP tool handlers.
//!
//! Implements:
//! - `list_work_items` - List work queue items with filtering
//! - `retry_work_item` - Retry a failed work item
//! - `stop_managed_wiki` - Revoke recurring wiki service grants

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_pipeline::error::PipelineError;
use elephant_pipeline::scheduler::work_queue::WorkQueueConfig;
use elephant_pipeline::scheduler::WorkQueue;
use elephant_storage::{parse_id, Repository, StorageClient, WorkItemRepository};
use elephant_types::ids::TableName;
use elephant_types::records::work::{WorkItem, WorkItemStatus};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `list_work_items` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListWorkItemsInput {
    /// Filter by work item kind.
    #[serde(default)]
    pub kind: Option<String>,
    /// Filter by status.
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by minimum age in seconds.
    #[serde(default)]
    pub min_age_secs: Option<u64>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

const fn default_limit() -> usize {
    50
}

const FILTERED_WORK_ITEM_SCAN_BATCH: usize = 100;
const FILTERED_WORK_ITEM_MAX_SCAN: usize = 1_000;

fn authorize_work_item_scope(ctx: &ToolContext, required_scope: &str) -> Result<(), String> {
    let space_id = &ctx.principal.space_id;
    if space_id.is_empty() {
        return Err("Access denied: missing space_id".to_string());
    }

    let policy_id = ctx
        .principal
        .clearances
        .first()
        .map_or("default", |clearance| clearance.policy_id.as_str());
    let record = crate::authz::AuthzService::preflight_record(space_id, policy_id);
    ctx.authz
        .evaluate(&ctx.principal, &record, required_scope)
        .map_err(|reason| format!("Access denied: {reason}"))
}

/// Output for `list_work_items` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListWorkItemsOutput {
    /// Search results.
    pub items: Vec<WorkItemSummary>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
}

/// Summary of a work item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItemSummary {
    /// Work item ID.
    pub id: String,
    /// Work item kind.
    pub kind: String,
    /// Current status.
    pub status: WorkItemStatus,
    /// Retry count.
    pub attempts: u32,
    /// Last error (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Created timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Priority.
    pub priority: i32,
}

/// Input for `retry_work_item` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryWorkItemInput {
    /// Work item ID.
    pub id: String,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `list_work_items` tool.
pub struct ListWorkItemsHandler {
    repo: WorkItemRepository,
}

impl ListWorkItemsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListWorkItemsHandler {
    fn name(&self) -> &'static str {
        "list_work_items"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List work queue items with filtering by kind, status, or age.\n\n",
            "Example:\n",
            r#"{ "status": "failed", "limit": 20 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "kind": {
                    "type": "string",
                    "description": "Filter by work item kind (e.g., extract, embed, promote)"
                },
                "status": {
                    "type": "string",
                    "description": "Filter by status",
                    "enum": ["pending", "runnable", "running", "done", "failed", "cancelled"]
                },
                "min_age_secs": {
                    "type": "integer",
                    "description": "Filter by minimum age in seconds"
                },
                "limit": {
                    "type": "integer",
                    "default": 50,
                    "maximum": 100
                },
                "cursor": {
                    "type": "string",
                    "description": "Pagination cursor"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListWorkItemsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_work_item_scope(ctx, "read:work_item") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        // Parse cursor if provided
        let cursor = if let Some(ref c) = input.cursor {
            Some(
                parse_id(c, TableName::WorkItem)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?,
            )
        } else {
            None
        };

        let now = chrono::Utc::now();
        let has_filter =
            input.kind.is_some() || input.status.is_some() || input.min_age_secs.is_some();
        let page_limit = input.limit.min(100);
        if input.cursor.is_none() && input.min_age_secs.is_none() {
            if let Some(status) = input
                .status
                .as_deref()
                .and_then(parse_work_item_status_filter)
            {
                let items = if let Some(kind) = input.kind.as_deref() {
                    self.repo
                        .find_by_status_and_kind(&space_id, status, kind, page_limit + 1)
                        .await
                } else {
                    self.repo
                        .find_by_status_limited(&space_id, status, page_limit + 1)
                        .await
                }
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                let mut filtered = summarize_work_items(items);
                let has_more = filtered.len() > page_limit;
                if has_more {
                    filtered.truncate(page_limit);
                }
                let next_cursor = if has_more {
                    filtered.last().map(|s| {
                        s.id.strip_prefix("work_item:")
                            .map_or_else(|| s.id.clone(), std::string::ToString::to_string)
                    })
                } else {
                    None
                };
                let output = ListWorkItemsOutput {
                    items: filtered,
                    next_cursor,
                    has_more,
                };
                let response = ToolResponse::success(output);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        }
        let fetch_limit = if has_filter {
            FILTERED_WORK_ITEM_SCAN_BATCH
        } else {
            page_limit.saturating_add(1)
        };
        let mut scan_cursor = cursor;
        let mut filtered = Vec::new();
        let mut scanned = 0usize;
        let mut exhausted = false;

        loop {
            let batch_limit = if has_filter {
                let remaining_scan = FILTERED_WORK_ITEM_MAX_SCAN.saturating_sub(scanned);
                if remaining_scan == 0 {
                    break;
                }
                fetch_limit.min(remaining_scan)
            } else {
                fetch_limit
            };
            let items = self
                .repo
                .list(&space_id, batch_limit, scan_cursor.as_ref())
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            if items.is_empty() {
                exhausted = true;
                break;
            }

            let batch_len = items.len();
            scanned = scanned.saturating_add(batch_len);
            scan_cursor = items.last().map(|item| item.base.id.clone());
            for item in items {
                if !work_item_matches_list_filter(&item, &input, now) {
                    continue;
                }
                filtered.push(WorkItemSummary {
                    id: item.base.id.to_string(),
                    kind: item.kind,
                    status: item.status,
                    attempts: item.attempts,
                    last_error: item.last_error,
                    created_at: item.base.created_at,
                    priority: item.priority,
                });
                if filtered.len() > page_limit {
                    break;
                }
            }

            if batch_len < batch_limit {
                exhausted = true;
            }
            if filtered.len() > page_limit || exhausted || !has_filter {
                break;
            }
        }

        let found_extra = filtered.len() > page_limit;
        if found_extra {
            filtered.truncate(page_limit);
        }
        let scan_window_exhausted = has_filter && !exhausted && scan_cursor.is_some();
        let has_more = found_extra || scan_window_exhausted;

        let next_cursor = if found_extra {
            filtered.last().map(|s| {
                s.id.strip_prefix("work_item:")
                    .map_or_else(|| s.id.clone(), std::string::ToString::to_string)
            })
        } else if scan_window_exhausted {
            scan_cursor.map(|id| {
                id.to_string()
                    .strip_prefix("work_item:")
                    .map_or_else(|| id.to_string(), std::string::ToString::to_string)
            })
        } else {
            None
        };

        let output = ListWorkItemsOutput {
            items: filtered,
            next_cursor,
            has_more,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

fn work_item_matches_list_filter(
    item: &WorkItem,
    input: &ListWorkItemsInput,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    if let Some(ref kind) = input.kind {
        if item.kind != *kind {
            return false;
        }
    }
    if let Some(ref status_str) = input.status {
        let expected = match status_str.as_str() {
            "pending" => WorkItemStatus::Pending,
            "runnable" => WorkItemStatus::Runnable,
            "running" => WorkItemStatus::Running,
            "done" => WorkItemStatus::Done,
            "failed" => WorkItemStatus::Failed,
            "cancelled" => WorkItemStatus::Cancelled,
            _ => return true,
        };
        if item.status != expected {
            return false;
        }
    }
    if let Some(min_age) = input.min_age_secs {
        let age = (now - item.base.created_at).num_seconds();
        if age < i64::try_from(min_age).unwrap_or(i64::MAX) {
            return false;
        }
    }
    true
}

fn parse_work_item_status_filter(status: &str) -> Option<WorkItemStatus> {
    match status {
        "pending" => Some(WorkItemStatus::Pending),
        "runnable" => Some(WorkItemStatus::Runnable),
        "running" => Some(WorkItemStatus::Running),
        "done" => Some(WorkItemStatus::Done),
        "failed" => Some(WorkItemStatus::Failed),
        "cancelled" => Some(WorkItemStatus::Cancelled),
        _ => None,
    }
}

fn summarize_work_items(items: Vec<WorkItem>) -> Vec<WorkItemSummary> {
    items
        .into_iter()
        .map(|item| WorkItemSummary {
            id: item.base.id.to_string(),
            kind: item.kind,
            status: item.status,
            attempts: item.attempts,
            last_error: item.last_error,
            created_at: item.base.created_at,
            priority: item.priority,
        })
        .collect()
}

/// Handler for `retry_work_item` tool.
pub struct RetryWorkItemHandler {
    repo: WorkItemRepository,
}

impl RetryWorkItemHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RetryWorkItemHandler {
    fn name(&self) -> &'static str {
        "retry_work_item"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Retry a failed work item by resetting its status to runnable.\n\n",
            "Example:\n",
            r#"{ "id": "work_item:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Work item ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RetryWorkItemInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_work_item_scope(ctx, "write:work_item") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let rid = parse_id(&input.id, TableName::WorkItem)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        // Get the work item
        let item = match self.repo.get(&space_id, &rid).await {
            Ok(Some(i)) => i,
            Ok(None) => {
                let response: ToolResponse<WorkItem> =
                    ToolResponse::error(format!("Work item not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        // Only allow retrying failed items
        if item.status != WorkItemStatus::Failed {
            let response: ToolResponse<WorkItem> = ToolResponse::error(format!(
                "Work item is not in failed status (current: {:?})",
                item.status
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Update the work item - reset to runnable status so work queue picks it up
        let mut updated = item;
        updated.status = WorkItemStatus::Runnable;
        updated.attempts = 0;
        updated.last_error = None;
        updated.lease_expires_at = None;
        updated.leased_by = None;

        match self.repo.update(&space_id, &rid, updated).await {
            Ok(result) => {
                let response = ToolResponse::success(json!({
                    "id": result.base.id.to_string(),
                    "status": result.status,
                    "retried": true
                }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Revokes recurring wiki authority in the caller's space.
pub struct StopManagedWikiHandler {
    repo: WorkItemRepository,
}

impl StopManagedWikiHandler {
    /// Creates a handler for the durable wiki service registration.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for StopManagedWikiHandler {
    fn name(&self) -> &'static str {
        "stop_managed_wiki"
    }

    fn description(&self) -> &'static str {
        "Revoke all recurring wiki service grants in this space and cancel their queued jobs. Requires explicit manage:wiki with unrestricted subjects. An already running reconciliation may finish its current publication, but cannot schedule more work. Published articles remain available. Creating a managed wiki again grants a new registration.\n\nExample:\n{}"
    }

    fn input_schema(&self) -> Value {
        json!({ "type": "object", "properties": {}, "required": [], "additionalProperties": false })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        if !params.as_object().is_some_and(serde_json::Map::is_empty) {
            return Err(McpError::InvalidParameters(
                "Expected an empty object".to_string(),
            ));
        }
        if let Err(message) = super::knowledge_views::authorize_wiki_management(ctx) {
            return serde_json::to_value(ToolResponse::<()>::error(message))
                .map_err(|error| McpError::ExecutionError(error.to_string()));
        }
        self.repo
            .revoke_managed_wiki_grants(&ctx.principal.space_id)
            .await?;
        serde_json::to_value(ToolResponse::success(json!({
            "stopped": true,
            "space_id": ctx.principal.space_id,
            "running_reconciliation_may_finish": true
        })))
        .map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

/// Input for `claim_work_item` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimWorkItemInput {
    /// Work item ID.
    pub id: String,
    /// Worker identifier claiming the item.
    pub worker_id: String,
    /// Lease duration in seconds.
    #[serde(default = "default_lease_duration")]
    pub lease_duration_secs: u64,
}

const fn default_lease_duration() -> u64 {
    300 // 5 minutes
}

/// Input for `complete_work_item` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteWorkItemInput {
    /// Work item ID.
    pub id: String,
    /// Whether the work item succeeded.
    #[serde(default = "default_success")]
    pub success: bool,
    /// Error message if failed.
    #[serde(default)]
    pub error: Option<String>,
}

const fn default_success() -> bool {
    true
}

/// Handler for `claim_work_item` tool.
pub struct ClaimWorkItemHandler {
    repo: WorkItemRepository,
}

impl ClaimWorkItemHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ClaimWorkItemHandler {
    fn name(&self) -> &'static str {
        "claim_work_item"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Claim a runnable work item with a lease for processing.\n\n",
            "Example:\n",
            r#"{ "id": "work_item:550e8400-e29b-41d4-a716-446655440000", "worker_id": "worker-1", "lease_duration_secs": 300 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "worker_id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Work item ID"
                },
                "worker_id": {
                    "type": "string",
                    "description": "Identifier of the worker claiming the item"
                },
                "lease_duration_secs": {
                    "type": "integer",
                    "default": 300,
                    "description": "Lease duration in seconds"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ClaimWorkItemInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_work_item_scope(ctx, "write:work_item") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let rid = parse_id(&input.id, TableName::WorkItem)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let lease_duration = parse_lease_duration_secs(input.lease_duration_secs)?;

        // Calculate lease expiration
        let lease_expires = chrono::Utc::now() + lease_duration;

        // Try to claim the work item atomically
        match self
            .repo
            .claim_with_lease(&space_id, &rid, &input.worker_id, lease_expires)
            .await
        {
            Ok(Some(claimed)) => {
                let response = ToolResponse::success(json!({
                    "id": claimed.base.id.to_string(),
                    "status": claimed.status,
                    "claimed": true,
                    "worker_id": input.worker_id,
                    "lease_expires_at": lease_expires.to_rfc3339()
                }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<WorkItem> = ToolResponse::error(
                    "Work item could not be claimed (not found, not runnable, or already claimed)",
                );
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

fn parse_lease_duration_secs(lease_duration_secs: u64) -> McpResult<chrono::Duration> {
    let secs = i64::try_from(lease_duration_secs)
        .map_err(|_| McpError::InvalidParameters("lease_duration_secs out of range".to_string()))?;
    chrono::Duration::try_seconds(secs)
        .ok_or_else(|| McpError::InvalidParameters("lease_duration_secs out of range".to_string()))
}

/// Handler for `complete_work_item` tool.
pub struct CompleteWorkItemHandler {
    repo: WorkItemRepository,
    storage: StorageClient,
}

impl CompleteWorkItemHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl ToolHandler for CompleteWorkItemHandler {
    fn name(&self) -> &'static str {
        "complete_work_item"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Mark a work item as completed (success or failure).\n\n",
            "Example:\n",
            r#"{ "id": "work_item:550e8400-e29b-41d4-a716-446655440000", "success": true }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Work item ID"
                },
                "success": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether the work item succeeded"
                },
                "error": {
                    "type": "string",
                    "description": "Error message if failed"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CompleteWorkItemInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_work_item_scope(ctx, "write:work_item") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let rid = parse_id(&input.id, TableName::WorkItem)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        // Get the work item
        let item = match self.repo.get(&space_id, &rid).await {
            Ok(Some(i)) => i,
            Ok(None) => {
                let response: ToolResponse<WorkItem> =
                    ToolResponse::error(format!("Work item not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        // Only allow completing items that are running
        if item.status != WorkItemStatus::Running {
            let response: ToolResponse<WorkItem> = ToolResponse::error(format!(
                "Work item is not in running status (current: {:?})",
                item.status
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let worker_id = match item.leased_by.as_ref() {
            Some(worker) if !worker.is_empty() => worker,
            _ => {
                let response: ToolResponse<WorkItem> =
                    ToolResponse::error("Work item missing lease owner".to_string());
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        };

        let config = WorkQueueConfig {
            worker_id: worker_id.clone(),
            ..WorkQueueConfig::default()
        };
        let work_queue = WorkQueue::new(self.storage.clone(), config);

        let result = if input.success {
            work_queue.complete(&space_id, &rid).await
        } else {
            let error = input
                .error
                .clone()
                .unwrap_or_else(|| "Work item failed".to_string());
            work_queue.fail(&space_id, &rid, error).await
        };

        match result {
            Ok(result) => {
                let response = ToolResponse::success(json!({
                    "id": result.base.id.to_string(),
                    "status": result.status,
                    "completed": true
                }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(PipelineError::LeaseConflict(_)) => {
                let response: ToolResponse<WorkItem> = ToolResponse::error(
                    "Work item could not be completed (not running or lease owner mismatch)",
                );
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Input for `list_pending_work_items` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPendingWorkItemsInput {
    /// Filter by work item kind.
    #[serde(default)]
    pub kind: Option<String>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Handler for `list_pending_work_items` tool.
pub struct ListPendingWorkItemsHandler {
    repo: WorkItemRepository,
}

impl ListPendingWorkItemsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListPendingWorkItemsHandler {
    fn name(&self) -> &'static str {
        "list_pending_work_items"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List work items that are pending or runnable (ready to be claimed).\n\n",
            "Example:\n",
            r#"{ "kind": "extract", "limit": 50 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "kind": {
                    "type": "string",
                    "description": "Filter by work item kind (e.g., extract, embed, promote)"
                },
                "limit": {
                    "type": "integer",
                    "default": 50,
                    "maximum": 100
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListPendingWorkItemsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_work_item_scope(ctx, "read:work_item") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        // Get runnable work items
        let mut items = self
            .repo
            .get_runnable(&space_id, input.limit * 2)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // Also include pending items, using the status-specific repository path
        // so a dashboard read cannot deserialize the whole queue history.
        let pending = if let Some(kind) = input.kind.as_deref() {
            self.repo
                .find_by_status_and_kind(&space_id, WorkItemStatus::Pending, kind, input.limit)
                .await
        } else {
            self.repo
                .find_by_status_limited(&space_id, WorkItemStatus::Pending, input.limit)
                .await
        }
        .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // Merge and deduplicate.
        for item in pending {
            if !items.iter().any(|i| i.base.id == item.base.id) {
                items.push(item);
            }
        }

        // Apply kind filter
        if let Some(ref kind) = input.kind {
            items.retain(|i| i.kind == *kind);
        }

        // Create summaries
        let summaries: Vec<WorkItemSummary> = items
            .into_iter()
            .take(input.limit)
            .map(|item| WorkItemSummary {
                id: item.base.id.to_string(),
                kind: item.kind,
                status: item.status,
                attempts: item.attempts,
                last_error: item.last_error,
                created_at: item.base.created_at,
                priority: item.priority,
            })
            .collect();

        let output = ListWorkItemsOutput {
            items: summaries,
            next_cursor: None,
            has_more: false,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

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
    use crate::handlers::ToolContext;
    use elephant_policy::{Clearance, Principal};
    use elephant_storage::{Repository, StorageClient, WorkItemRepository};
    use elephant_types::records::base::BaseFields;
    use elephant_types::RecordId;
    use std::collections::HashSet;

    fn make_test_principal(space_id: &str, scopes: &[&str]) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn stop_managed_wiki_requires_explicit_space_service_authority() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repo = WorkItemRepository::new(storage.clone());
        let grant = repo
            .create_managed_wiki_grant("space", "manager")
            .await
            .unwrap();
        let other = repo
            .create_managed_wiki_grant("other", "manager")
            .await
            .unwrap();
        let handler = StopManagedWikiHandler::new(storage.clone());
        for scopes in [&["write:work_item"][..], &["*"][..]] {
            let ctx = ToolContext::new(storage.clone(), make_test_principal("space", scopes));
            let response = handler.execute(&ctx, json!({})).await.unwrap();
            assert_eq!(response["ok"], false);
            assert!(repo
                .managed_wiki_grant_is_active("space", &grant)
                .await
                .unwrap());
        }
        let mut principal = make_test_principal("space", &["manage:wiki"]);
        principal.subject_allowlist = Some(HashSet::new());
        let restricted = ToolContext::new(storage.clone(), principal);
        assert_eq!(
            handler.execute(&restricted, json!({})).await.unwrap()["ok"],
            false
        );
        let ctx = ToolContext::new(storage, make_test_principal("space", &["manage:wiki"]));
        assert_eq!(handler.execute(&ctx, json!({})).await.unwrap()["ok"], true);
        assert!(!repo
            .managed_wiki_grant_is_active("space", &grant)
            .await
            .unwrap());
        assert!(repo
            .managed_wiki_grant_is_active("other", &other)
            .await
            .unwrap());
    }

    #[test]
    fn test_list_work_items_input_defaults() {
        let input: ListWorkItemsInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.kind.is_none());
        assert!(input.status.is_none());
    }

    #[test]
    fn test_claim_work_item_input_defaults() {
        let input: ClaimWorkItemInput =
            serde_json::from_str(r#"{"id": "work_item:123", "worker_id": "worker1"}"#).unwrap();
        assert_eq!(input.lease_duration_secs, 300);
    }

    #[test]
    fn test_complete_work_item_input_defaults() {
        let input: CompleteWorkItemInput =
            serde_json::from_str(r#"{"id": "work_item:123"}"#).unwrap();
        assert!(input.success);
        assert!(input.error.is_none());
    }

    #[test]
    fn test_parse_lease_duration_secs_ok() {
        let duration = parse_lease_duration_secs(300).unwrap();
        assert_eq!(duration, chrono::Duration::seconds(300));
    }

    #[test]
    fn test_parse_lease_duration_secs_overflow() {
        let err = parse_lease_duration_secs(u64::MAX).unwrap_err();
        assert!(matches!(err, McpError::InvalidParameters(_)));
    }

    #[test]
    fn test_list_pending_work_items_input_defaults() {
        let input: ListPendingWorkItemsInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.kind.is_none());
    }

    #[tokio::test]
    async fn test_list_work_items_requires_read_work_item_scope() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler = ListWorkItemsHandler::new(storage.clone());
        let ctx = ToolContext::new(storage, make_test_principal("default", &[]));

        let result = handler
            .execute(&ctx, serde_json::json!({ "limit": 1 }))
            .await
            .expect("handler response");
        let decoded: ToolResponse<ListWorkItemsOutput> =
            serde_json::from_value(result).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Access denied"));
    }

    #[tokio::test]
    async fn test_list_work_items_status_filter_uses_limited_status_path() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repo = WorkItemRepository::new(storage.clone());

        for index in 0..50 {
            repo.create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "history".to_string(),
                payload: serde_json::json!({ "index": index }),
                status: WorkItemStatus::Done,
                priority: index,
                idempotency_key: format!("history-{index}"),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create history item");
        }

        let running = repo
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "active".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 100,
                idempotency_key: "active-running".to_string(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: Some("worker".to_string()),
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create running item");

        let handler = ListWorkItemsHandler::new(storage.clone());
        let ctx = ToolContext::new(storage, make_test_principal("default", &["read:work_item"]));
        let result = handler
            .execute(
                &ctx,
                serde_json::json!({ "status": "running", "limit": 10 }),
            )
            .await
            .expect("handler response");
        let decoded: ToolResponse<ListWorkItemsOutput> =
            serde_json::from_value(result).expect("decode");
        assert!(decoded.ok);
        let data = decoded.data.expect("data");
        assert_eq!(data.items.len(), 1);
        assert_eq!(data.items[0].id, running.base.id.to_string());
        assert!(!data.has_more);
    }

    #[tokio::test]
    async fn test_retry_work_item_requires_write_work_item_scope() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repo = WorkItemRepository::new(storage.clone());
        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "default".to_string(),
                "tester".to_string(),
            ),
            kind: "embed".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Failed,
            priority: 0,
            idempotency_key: "retry-auth".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("boom".to_string()),
        };
        let id = item.base.id.to_string();
        repo.create(item).await.expect("create work item");

        let handler = RetryWorkItemHandler::new(storage.clone());
        let ctx = ToolContext::new(storage, make_test_principal("default", &["read:work_item"]));

        let result = handler
            .execute(&ctx, serde_json::json!({ "id": id }))
            .await
            .expect("handler response");
        let decoded: ToolResponse<serde_json::Value> =
            serde_json::from_value(result).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Access denied"));
    }
}
