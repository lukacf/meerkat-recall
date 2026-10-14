#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Reprocess job MCP tool handlers.
//!
//! Implements:
//! - `create_reprocess_job` - Create a reprocessing job
//! - `get_reprocess_job_progress` - Get job status and progress

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[cfg(test)]
use elephant_pipeline::reprocess::WORK_KIND_EXECUTE_REPROCESS_JOB;
use elephant_storage::{parse_id, Repository, ReprocessJobRepository, StorageClient};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::base::BaseFields;
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};

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

        if let Err(message) = crate::authz::authorize_preflight_scope(
            &ctx.authz,
            &ctx.principal,
            "write:reprocess_job",
        ) {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();
        let parse_date = |value: Option<&str>| {
            value
                .map(|date| {
                    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|error| {
                        McpError::InvalidParameters(format!("Invalid selection date: {error}"))
                    })
                })
                .transpose()
        };
        let date_from = parse_date(input.selection_criteria.date_from.as_deref())?;
        let date_to = parse_date(input.selection_criteria.date_to.as_deref())?;
        if date_from.zip(date_to).is_some_and(|(from, to)| from > to) {
            return Err(McpError::InvalidParameters(
                "date_from must not follow date_to".into(),
            ));
        }
        if input
            .budget_limit_euros
            .is_some_and(|amount| !amount.is_finite() || amount < 0.0)
        {
            return Err(McpError::InvalidParameters(
                "budget_limit_euros must be finite and nonnegative".into(),
            ));
        }

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

        // Resolve the new job's envelope from the actual selected space before
        // authorizing it. The owner rechecks that exact configuration alongside
        // the job, original producer and initial queue insertion.
        let prepared = self.repo.prepare_creation(job).await?;

        if let Err(message) = ctx.authz.authorize_record_write(
            &ctx.principal,
            &prepared.job().base,
            "write:reprocess_job",
        ) {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        // The durable job and its deterministic initial handoff share the
        // storage owner's transaction. No success with a stranded job.
        match self.repo.create_prepared_with_work(prepared).await {
            Ok(created) => {
                let response = ToolResponse::success(json!({
                    "id": created.base.id.to_string(),
                    "name": created.name,
                    "status": created.status
                }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
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

        if let Err(message) = crate::authz::authorize_preflight_scope(
            &ctx.authz,
            &ctx.principal,
            "read:reprocess_job",
        ) {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.get(&space_id, &rid).await {
            Ok(Some(job)) => {
                if let Err(message) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &job.base, "read:reprocess_job")
                {
                    let response: ToolResponse<()> = ToolResponse::error(message);
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
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
            Err(e) => Err(McpError::StorageError(e)),
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
    use elephant_storage::WorkItemRepository;
    use std::collections::HashSet;

    fn make_test_principal(space_id: &str) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: HashSet::from(["write:reprocess_job".into(), "read:reprocess_job".into()]),
            clearances: vec![elephant_policy::Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    async fn setup_test_storage() -> StorageClient {
        setup_test_storage_with_base(BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "test_space".into(),
            "test".into(),
        ))
        .await
    }

    async fn setup_test_storage_with_base(base: BaseFields) -> StorageClient {
        let storage = StorageClient::new_embedded().await.expect("storage");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base,
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 1,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("explicit selected space configuration");
        storage
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn classified_space_creates_and_reads_jobs_without_unrelated_default_clearance() {
        for level in [3, 7] {
            let policy = format!("classified-{level}");
            let mut base = BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "test_space".into(),
                "test".into(),
            );
            base.security.policy_id = policy.clone();
            base.security.level = level;
            base.security.labels = vec!["private".into()];
            base.security.handling = vec!["no-export".into()];
            base.subjects = vec![elephant_types::Link::new(RecordId::new_v4(
                TableName::Entity,
            ))];
            base.subjects_state = elephant_types::records::SubjectsState::Complete;
            // The envelope is original fixture authority. Repository updates
            // deliberately cannot reclassify an existing authorized config.
            let storage = setup_test_storage_with_base(base).await;
            let config = elephant_storage::SpaceConfigRepository::new(storage.clone())
                .get_by_space_id("test_space")
                .await
                .unwrap()
                .unwrap();
            let mut principal = make_test_principal("test_space");
            principal.clearances = vec![elephant_policy::Clearance {
                policy_id: policy,
                max_level: level,
                labels: HashSet::from(["private".into()]),
                handling_allow: HashSet::from(["no-export".into()]),
            }];
            let context = ToolContext::new(storage.clone(), principal.clone());
            let params = json!({"name":"classified job", "selection_criteria":{}, "target_stages":["re-maintain"]});
            let result = CreateReprocessJobHandler::new(storage.clone())
                .execute(&context, params.clone())
                .await
                .unwrap();
            assert_eq!(result["ok"], true, "{result}");
            let id = parse_id(
                result["data"]["id"].as_str().unwrap(),
                TableName::ReprocessJob,
            )
            .unwrap();
            let jobs = ReprocessJobRepository::new(storage.clone());
            let job = jobs.get("test_space", &id).await.unwrap().unwrap();
            assert_eq!(job.base.security, config.base.security);
            assert_eq!(job.base.subjects, config.base.subjects);
            assert_eq!(job.base.subjects_state, config.base.subjects_state);
            let work = WorkItemRepository::new(storage.clone())
                .find_by_idempotency_key(
                    "test_space",
                    &format!("{WORK_KIND_EXECUTE_REPROCESS_JOB}:{id}"),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(work.base.security, job.base.security);
            assert_eq!(work.base.subjects, job.base.subjects);
            let progress = GetReprocessJobProgressHandler::new(storage.clone())
                .execute(&context, json!({"id": id.to_string()}))
                .await
                .unwrap();
            assert_eq!(progress["ok"], true, "{progress}");
            assert_eq!(progress["data"]["name"], "classified job");

            // Revoking this principal's clearance cannot expose progress or
            // create a second job under an unrelated default-policy envelope.
            principal.clearances.clear();
            let revoked = ToolContext::new(storage.clone(), principal);
            let denied = CreateReprocessJobHandler::new(storage.clone())
                .execute(&revoked, params)
                .await
                .unwrap();
            assert_eq!(denied["ok"], false);
            let denied = GetReprocessJobProgressHandler::new(storage.clone())
                .execute(&revoked, json!({"id":id.to_string()}))
                .await
                .unwrap();
            assert_eq!(denied["ok"], false);
            assert!(!denied.to_string().contains("classified job"));
            assert_eq!(jobs.list("test_space", 10, None).await.unwrap(), vec![job]);
            assert_eq!(
                WorkItemRepository::new(storage)
                    .list("test_space", 10, None)
                    .await
                    .unwrap(),
                vec![work]
            );
        }
    }

    #[tokio::test]
    async fn unconfigured_space_never_creates_a_default_policy_job_or_handoff() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let context = ToolContext::new(storage.clone(), make_test_principal("test_space"));
        assert!(CreateReprocessJobHandler::new(storage.clone()).execute(&context, json!({
            "name":"missing configuration", "selection_criteria":{}, "target_stages":["re-maintain"]
        })).await.is_err());
        assert!(ReprocessJobRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .unwrap()
            .is_empty());
        assert!(WorkItemRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .unwrap()
            .is_empty());
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
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
    #[tokio::test]
    async fn create_requires_write_scope_and_complete_job_abac_before_allocation() {
        for restricted in [false, true] {
            let storage = setup_test_storage().await;
            let mut principal = make_test_principal("test_space");
            if restricted {
                principal.subject_allowlist =
                    Some(HashSet::from([RecordId::new_v4(TableName::Entity)]));
            } else {
                principal.scopes.clear();
            }
            let ctx = ToolContext::new(storage.clone(), principal);
            let result = CreateReprocessJobHandler::new(storage.clone())
                .execute(
                    &ctx,
                    json!({
                        "name":"denied", "selection_criteria":{}, "target_stages":["re-extract"]
                    }),
                )
                .await
                .expect("response");
            let response: ToolResponse<Value> = serde_json::from_value(result).expect("decode");
            assert!(!response.ok);
            assert!(ReprocessJobRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .expect("jobs")
                .is_empty());
            assert!(WorkItemRepository::new(storage)
                .list("test_space", 10, None)
                .await
                .expect("work")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn invalid_selection_date_never_broadens_the_job() {
        let storage = setup_test_storage().await;
        let ctx = ToolContext::new(storage.clone(), make_test_principal("test_space"));
        let result = CreateReprocessJobHandler::new(storage.clone()).execute(&ctx, json!({
            "name":"bad date", "selection_criteria":{"date_from":"invalid"}, "target_stages":["re-extract"]
        })).await;
        assert!(matches!(result, Err(McpError::InvalidParameters(_))));
        assert!(ReprocessJobRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("jobs")
            .is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::if_not_else,
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn progress_requires_scope_and_full_job_authority_before_disclosing_details() {
        let storage = setup_test_storage().await;
        let ctx = ToolContext::new(storage.clone(), make_test_principal("test_space"));
        let created = CreateReprocessJobHandler::new(storage.clone()).execute(&ctx, json!({
            "name":"private job detail", "selection_criteria":{}, "target_stages":["re-maintain"]
        })).await.unwrap();
        let id = created["data"]["id"].as_str().unwrap();
        let id = parse_id(id, TableName::ReprocessJob).unwrap();
        let repo = ReprocessJobRepository::new(storage.clone());
        let mut job = repo.get("test_space", &id).await.unwrap().unwrap();
        job.base.security.level = 2;
        job.error = Some("private failure detail".into());
        repo.update("test_space", &id, job).await.unwrap();
        let handler = GetReprocessJobProgressHandler::new(storage.clone());
        for mode in 0..3 {
            let mut principal = make_test_principal("test_space");
            if mode != 1 {
                principal.clearances[0].max_level = 2;
            }
            if mode == 0 {
                principal.scopes.clear();
            }
            let context = ToolContext::new(storage.clone(), principal);
            let output = handler
                .execute(&context, json!({"id":id.to_string()}))
                .await
                .unwrap();
            let response: ToolResponse<Value> = serde_json::from_value(output.clone()).unwrap();
            assert_eq!(response.ok, mode == 2);
            if mode != 2 {
                assert!(!output.to_string().contains("private job detail"));
                assert!(!output.to_string().contains("private failure detail"));
                assert!(response.data.is_none());
            } else {
                assert_eq!(response.data.unwrap()["name"], "private job detail");
            }
        }
    }
}
