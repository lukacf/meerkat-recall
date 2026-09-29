#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Reprocess job MCP tool handlers.
//!
//! Implements:
//! - `create_reprocess_job` - Create a reprocessing job
//! - `get_reprocess_job_progress` - Get job status and progress

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_pipeline::reprocess::WORK_KIND_EXECUTE_REPROCESS_JOB;
use elephant_storage::{
    parse_id, Repository, ReprocessJobRepository, StorageClient, WorkItemRepository,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::base::BaseFields;
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::work::{WorkItem, WorkItemStatus};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `create_reprocess_job` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReprocessJobInput {
    /// Job name.
    pub name: String,
    /// Reason for reprocessing.
    #[serde(default)]
    pub reason: Option<String>,
    /// Selection criteria for sources.
    pub selection_criteria: SelectionCriteriaInput,
    /// Target pipeline stages to re-run.
    pub target_stages: Vec<ReprocessTargetStage>,
    /// Priority (higher runs first).
    #[serde(default)]
    pub priority: i32,
    /// Budget limit in euros.
    #[serde(default)]
    pub budget_limit_euros: Option<f64>,
    /// Max sources per day.
    #[serde(default)]
    pub max_sources_per_day: Option<u32>,
}

/// Selection criteria input.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SelectionCriteriaInput {
    /// Source types to include.
    #[serde(default)]
    pub source_types: Vec<String>,
    /// Date range start (YYYY-MM-DD).
    #[serde(default)]
    pub date_from: Option<String>,
    /// Date range end (YYYY-MM-DD).
    #[serde(default)]
    pub date_to: Option<String>,
    /// Extractor version to reprocess (versions before this).
    #[serde(default)]
    pub extractor_version_before: Option<String>,
    /// Specific predicate IDs to reprocess.
    #[serde(default)]
    pub predicate_ids: Vec<String>,
}

/// Input for `get_reprocess_job_progress` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReprocessJobProgressInput {
    /// Reprocess job ID.
    pub id: String,
}

/// Output for `get_reprocess_job_progress` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReprocessJobProgressOutput {
    /// Job ID.
    pub id: String,
    /// Job name.
    pub name: String,
    /// Current status.
    pub status: ReprocessStatus,
    /// Progress details.
    pub progress: ReprocessProgress,
    /// Started timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Completed timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `create_reprocess_job` tool.
pub struct CreateReprocessJobHandler {
    repo: ReprocessJobRepository,
}

impl CreateReprocessJobHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: ReprocessJobRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for CreateReprocessJobHandler {
    fn name(&self) -> &'static str {
        "create_reprocess_job"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a reprocessing job to backfill or re-extract sources.\n\n",
            "Example:\n",
            r#"{ "name": "Backfill extraction", "selection_criteria": { "source_types": ["web"], "date_from": "2026-01-01" }, "target_stages": ["re-extract"], "priority": 1 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["name", "selection_criteria", "target_stages"],
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Job name"
                },
                "reason": {
                    "type": "string",
                    "description": "Reason for reprocessing"
                },
                "selection_criteria": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "source_types": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Source types to include"
                        },
                        "date_from": {
                            "type": "string",
                            "description": "Date range start (YYYY-MM-DD)"
                        },
                        "date_to": {
                            "type": "string",
                            "description": "Date range end (YYYY-MM-DD)"
                        },
                        "extractor_version_before": {
                            "type": "string",
                            "description": "Extractor version to reprocess"
                        },
                        "predicate_ids": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Specific predicate IDs to reprocess"
                        }
                    }
                },
                "target_stages": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["re-render", "re-chunk", "re-extract", "re-resolve", "re-maintain"]
                    },
                    "description": "Target pipeline stages to re-run"
                },
                "priority": {
                    "type": "integer",
                    "default": 0,
                    "description": "Priority (higher runs first)"
                },
                "budget_limit_euros": {
                    "type": "number",
                    "description": "Budget limit in euros"
                },
                "max_sources_per_day": {
                    "type": "integer",
                    "description": "Max sources per day"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CreateReprocessJobInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if input.target_stages.is_empty() {
            let response: ToolResponse<()> =
                ToolResponse::error("target_stages must include at least one stage");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Parse dates if provided
        let date_from = input
            .selection_criteria
            .date_from
            .as_deref()
            .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());
        let date_to = input
            .selection_criteria
            .date_to
            .as_deref()
            .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

        // Create the selection criteria
        let selection_criteria = SelectionCriteria {
            source_types: input.selection_criteria.source_types,
            date_from,
            date_to,
            extractor_version_before: input.selection_criteria.extractor_version_before,
            predicate_ids: input.selection_criteria.predicate_ids,
            space_ids: vec![],
            source_status: None,
            custom_filter: None,
        };

        // Create the job record
        let id = RecordId::new_v4(TableName::ReprocessJob);
        let base = BaseFields::new(id, space_id.clone(), "mcp_tool".to_string());

        let job = ReprocessJob {
            base,
            name: input.name,
            selection_criteria,
            target_stages: input.target_stages,
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: input.priority,
            budget_limit_euros: input.budget_limit_euros,
            max_sources_per_day: input.max_sources_per_day,
            started_at: None,
            completed_at: None,
            error: None,
            reason: input.reason,
        };

        match self.repo.create(job).await {
            Ok(created) => {
                let work_item_repo = WorkItemRepository::new(ctx.storage.clone());
                let idempotency_key =
                    format!("{WORK_KIND_EXECUTE_REPROCESS_JOB}:{}", created.base.id);
                let existing = work_item_repo
                    .find_by_idempotency_key(&space_id, &idempotency_key)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                if existing.is_none() {
                    let work_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            "mcp_tool".to_string(),
                        ),
                        kind: WORK_KIND_EXECUTE_REPROCESS_JOB.to_string(),
                        payload: json!({ "job_id": created.base.id.to_string() }),
                        status: WorkItemStatus::Runnable,
                        priority: created.priority,
                        idempotency_key,
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };
                    work_item_repo
                        .create(work_item)
                        .await
                        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                }

                let response = ToolResponse::success(json!({
                    "id": created.base.id.to_string(),
                    "name": created.name,
                    "status": created.status
                }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `get_reprocess_job_progress` tool.
pub struct GetReprocessJobProgressHandler {
    repo: ReprocessJobRepository,
}

impl GetReprocessJobProgressHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: ReprocessJobRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetReprocessJobProgressHandler {
    fn name(&self) -> &'static str {
        "get_reprocess_job_progress"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get reprocessing job status and progress.\n\n",
            "Example:\n",
            r#"{ "id": "reprocess_job:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Reprocess job ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetReprocessJobProgressInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::ReprocessJob)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        match self.repo.get(&space_id, &rid).await {
            Ok(Some(job)) => {
                let output = ReprocessJobProgressOutput {
                    id: job.base.id.to_string(),
                    name: job.name,
                    status: job.status,
                    progress: job.progress,
                    started_at: job.started_at,
                    completed_at: job.completed_at,
                    error: job.error,
                };
                let response = ToolResponse::success(output);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<ReprocessJobProgressOutput> =
                    ToolResponse::error(format!("Reprocess job not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
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
    use elephant_policy::Principal;
    use elephant_storage::{StorageConfig, WorkItemRepository};
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    const REPROCESS_SCHEMA: &str = "DEFINE TABLE reprocess_job SCHEMALESS;";
    const WORK_ITEM_SCHEMA: &str = "DEFINE TABLE work_item SCHEMALESS;";

    fn make_test_principal(space_id: &str) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    async fn setup_test_storage() -> StorageClient {
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99400);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = StorageConfig {
            database: format!("mcp_reprocess_test_{db_id}"),
            ..StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(REPROCESS_SCHEMA).await.expect("schema");
        storage.query(WORK_ITEM_SCHEMA).await.expect("schema");
        storage
    }

    #[test]
    fn test_create_reprocess_job_input() {
        let input: CreateReprocessJobInput = serde_json::from_str(
            r#"{
                "name": "test",
                "selection_criteria": {"source_types": ["pdf"]},
                "target_stages": ["re-extract"]
            }"#,
        )
        .unwrap();
        assert_eq!(input.name, "test");
        assert_eq!(input.priority, 0);
    }

    #[tokio::test]
    async fn test_create_reprocess_job_enqueues_work_item() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let ctx = ToolContext::new(storage.clone(), principal);
        let handler = CreateReprocessJobHandler::new(storage.clone());

        let params = serde_json::json!({
            "name": "Backfill job",
            "selection_criteria": { "source_types": ["pdf"] },
            "target_stages": ["re-extract"]
        });

        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<serde_json::Value> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let job_id = decoded
            .data
            .as_ref()
            .and_then(|data| data.get("id"))
            .and_then(|value| value.as_str())
            .expect("job id");

        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{WORK_KIND_EXECUTE_REPROCESS_JOB}:{job_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");

        assert!(item.is_some(), "reprocess work item should be enqueued");
    }
}
