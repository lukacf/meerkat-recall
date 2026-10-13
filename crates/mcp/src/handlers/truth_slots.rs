#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Truth slot MCP tool handlers.
//!
//! Implements:
//! - `list_truth_slots` - List truth slots with filtering
//! - `get_truth_slot_history` - Get conflicting assertions for a slot

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_storage::{
    parse_id, AssertionRepository, ConflictGroupRepository, Repository, StorageClient,
    TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::Assertion;
use elephant_types::records::truth::{ConflictGroup, TruthSlot};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `list_truth_slots` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTruthSlotsInput {
    /// Filter by subject entity ID.
    #[serde(default)]
    pub subject_entity_id: Option<String>,
    /// Filter by predicate ID.
    #[serde(default)]
    pub predicate_id: Option<String>,
    /// Filter for stale slots only.
    #[serde(default)]
    pub stale_only: bool,
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

fn authorize_truth_slot_scope(ctx: &ToolContext, required_scope: &str) -> Result<(), String> {
    crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, required_scope)
}

/// Output for `list_truth_slots` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTruthSlotsOutput {
    /// Search results.
    pub items: Vec<TruthSlotSummary>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
}

/// Summary of a truth slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruthSlotSummary {
    /// The truth slot.
    #[serde(flatten)]
    pub slot: TruthSlot,
    /// Number of competing assertions.
    pub conflict_count: usize,
    /// Whether this slot has an active conflict.
    pub has_conflict: bool,
}

/// Input for `get_truth_slot_history` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTruthSlotHistoryInput {
    /// Truth slot ID.
    pub id: String,
}

/// Output for `get_truth_slot_history` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruthSlotHistoryOutput {
    /// The truth slot.
    pub slot: TruthSlot,
    /// Conflict group (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_group: Option<ConflictGroup>,
    /// All assertions competing for this slot.
    pub assertions: Vec<Assertion>,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `list_truth_slots` tool.
pub struct ListTruthSlotsHandler {
    slot_repo: TruthSlotRepository,
    conflict_repo: ConflictGroupRepository,
}

impl ListTruthSlotsHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            slot_repo: TruthSlotRepository::new(storage.clone()),
            conflict_repo: ConflictGroupRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListTruthSlotsHandler {
    fn name(&self) -> &'static str {
        "list_truth_slots"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List truth slots with filtering by subject entity and predicate.\n\n",
            "Example:\n",
            r#"{ "subject_entity_id": "entity:550e8400-e29b-41d4-a716-446655440000", "predicate_id": "works_at", "limit": 25 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "subject_entity_id": {
                    "type": "string",
                    "description": "Filter by subject entity ID"
                },
                "predicate_id": {
                    "type": "string",
                    "description": "Filter by predicate ID"
                },
                "stale_only": {
                    "type": "boolean",
                    "default": false,
                    "description": "Filter for stale slots only"
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
        let input: ListTruthSlotsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_truth_slot_scope(ctx, "read:entity") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Parse cursor if provided
        let cursor = if let Some(ref c) = input.cursor {
            Some(
                parse_id(c, TableName::TruthSlot)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?,
            )
        } else {
            None
        };

        // Get truth slots from repository
        let slots = self
            .slot_repo
            .list(&space_id, input.limit + 1, cursor.as_ref())
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let slots = crate::current_read::filter(ctx, slots, "read:entity").await?;

        // Get all conflict groups for the space
        let conflict_groups = self
            .conflict_repo
            .list(&space_id, 1000, None)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // Apply in-memory filters and create summaries
        let mut filtered: Vec<TruthSlotSummary> = slots
            .into_iter()
            .filter(|s| {
                // Filter by subject entity
                if let Some(ref entity_id) = input.subject_entity_id {
                    if !s.subject_entity.contains(entity_id) {
                        return false;
                    }
                }
                // Filter by predicate
                if let Some(ref pred_id) = input.predicate_id {
                    if s.predicate_id != *pred_id {
                        return false;
                    }
                }
                // Filter by stale
                if input.stale_only && !s.stale {
                    return false;
                }
                true
            })
            .map(|slot| {
                // Find conflict group for this slot
                let conflict = conflict_groups
                    .iter()
                    .find(|cg| cg.slot_key == slot.slot_key);

                let conflict_count = conflict.map_or(0, |cg| cg.claim_ids.len());
                let has_conflict = conflict.is_some_and(|cg| !cg.resolved);

                TruthSlotSummary {
                    slot,
                    conflict_count,
                    has_conflict,
                }
            })
            .collect();

        let has_more = filtered.len() > input.limit;
        if filtered.len() > input.limit {
            filtered.truncate(input.limit);
        }

        let next_cursor = if has_more {
            filtered.last().map(|s| {
                s.slot
                    .base
                    .id
                    .to_string()
                    .strip_prefix("truth_slot:")
                    .map_or_else(
                        || s.slot.base.id.to_string(),
                        std::string::ToString::to_string,
                    )
            })
        } else {
            None
        };

        let output = ListTruthSlotsOutput {
            items: filtered,
            next_cursor,
            has_more,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_truth_slot_history` tool.
#[allow(clippy::struct_field_names)] // Repo suffix is intentional for clarity
pub struct GetTruthSlotHistoryHandler {
    slot_repo: TruthSlotRepository,
    conflict_repo: ConflictGroupRepository,
    assertion_repo: AssertionRepository,
}

impl GetTruthSlotHistoryHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            slot_repo: TruthSlotRepository::new(storage.clone()),
            conflict_repo: ConflictGroupRepository::new(storage.clone()),
            assertion_repo: AssertionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetTruthSlotHistoryHandler {
    fn name(&self) -> &'static str {
        "get_truth_slot_history"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get all assertions competing for a truth slot.\n\n",
            "Example:\n",
            r#"{ "id": "truth_slot:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Truth slot ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetTruthSlotHistoryInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        for scope in ["read:entity", "read:assertion"] {
            if let Err(message) = authorize_truth_slot_scope(ctx, scope) {
                let response: ToolResponse<()> = ToolResponse::error(message);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::TruthSlot)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        // Get the truth slot
        let slot = match self.slot_repo.get(&space_id, &rid).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                let response: ToolResponse<TruthSlotHistoryOutput> =
                    ToolResponse::error(format!("Truth slot not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };
        if let Err(message) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &slot, "read:entity")
        {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Find conflict group by slot key
        let conflict_groups = self
            .conflict_repo
            .list(&space_id, 1000, None)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let conflict_group = conflict_groups
            .into_iter()
            .find(|cg| cg.slot_key == slot.slot_key);

        // Get assertions for this slot's claims
        let mut assertions = Vec::new();

        // First, try to get assertions from conflict group claim_ids
        if let Some(ref cg) = conflict_group {
            for claim_id in &cg.claim_ids {
                // Try to parse as assertion ID
                if let Ok(aid) = parse_id(claim_id, TableName::Assertion) {
                    if let Ok(Some(assertion)) = self.assertion_repo.get(&space_id, &aid).await {
                        if ctx
                            .authz
                            .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
                            .is_ok()
                        {
                            assertions.push(assertion);
                        }
                    }
                }
            }
        }

        // Also get assertions from current_values
        for cv in &slot.current_values {
            if let Ok(aid) = parse_id(&cv.claim_id, TableName::Assertion) {
                if let Ok(Some(assertion)) = self.assertion_repo.get(&space_id, &aid).await {
                    // Avoid duplicates
                    if ctx
                        .authz
                        .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
                        .is_ok()
                        && !assertions.iter().any(|a| a.base.id == assertion.base.id)
                    {
                        assertions.push(assertion);
                    }
                }
            }
        }
        let output = TruthSlotHistoryOutput {
            slot,
            conflict_group,
            assertions,
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
    use chrono::Utc;
    use elephant_storage::Repository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security};

    #[test]
    fn test_list_truth_slots_input_defaults() {
        let input: ListTruthSlotsInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(!input.stale_only);
        assert!(input.subject_entity_id.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_list_truth_slots_requires_read_entity_scope() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = ListTruthSlotsHandler::new(storage.clone());
        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::collections::HashSet::new(),
                clearances: vec![elephant_policy::Clearance {
                    policy_id: "default".to_string(),
                    max_level: 0,
                    labels: std::collections::HashSet::new(),
                    handling_allow: std::collections::HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: std::collections::HashSet::new(),
            },
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("tool response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded.error.expect("error").contains("read:entity"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_list_truth_slots_filters_records_by_security() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = ListTruthSlotsHandler::new(storage.clone());
        let repo = TruthSlotRepository::new(storage.clone());

        let id = elephant_types::RecordId::new_v4(TableName::TruthSlot);
        let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.security = Security {
            policy_id: "default".to_string(),
            level: 5,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        let slot = TruthSlot {
            base,
            slot_key: "entity:alice|works_at".to_string(),
            slot_type: "rel".to_string(),
            subject_entity: "entity:alice".to_string(),
            predicate_id: "works_at".to_string(),
            current_values: Vec::new(),
            conflict_group: None,
            confidence: 0.9,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: "hash".to_string(),
        };
        repo.create(slot).await.expect("create truth slot");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:entity".to_string()).collect(),
                clearances: vec![elephant_policy::Clearance {
                    policy_id: "default".to_string(),
                    max_level: 0,
                    labels: std::collections::HashSet::new(),
                    handling_allow: std::collections::HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: std::collections::HashSet::new(),
            },
        );

        let response = handler
            .execute(&ctx, serde_json::json!({}))
            .await
            .expect("tool response");
        let decoded: ToolResponse<ListTruthSlotsOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok);
        assert!(decoded.data.expect("data").items.is_empty());
    }
}
