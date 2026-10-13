#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Predicate MCP tool handlers.
//!
//! Implements:
//! - `list_predicates` - List predicate definitions with alias mappings
//! - `propose_predicate_alias` - Propose a new alias for a predicate
//! - `validate_predicate_proposal` - Validate a staged predicate proposal
//! - `approve_predicate_proposal` - Approve a predicate alias proposal from work artifacts

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    PredicateAliasRepository, PredicateRegistryRepository, Repository, StorageClient,
    WorkArtifactRepository,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::base::BaseFields;
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasType, PredicateAlias, PredicateRegistry, SemanticsStatus,
};
use elephant_types::records::work::ArtifactStatus;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `list_predicates` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPredicatesInput {
    /// Filter by status (active, deprecated).
    #[serde(default)]
    pub status: Option<String>,
    /// Include alias mappings in output.
    #[serde(default = "default_include_aliases")]
    pub include_aliases: bool,
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

const fn default_include_aliases() -> bool {
    true
}

/// Predicate with its aliases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateWithAliases {
    /// The predicate definition.
    #[serde(flatten)]
    pub predicate: PredicateRegistry,
    /// Associated aliases.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aliases: Option<Vec<PredicateAlias>>,
}

/// Output for `list_predicates` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPredicatesOutput {
    /// Search results.
    pub items: Vec<PredicateWithAliases>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
}

/// Input for `propose_predicate_alias` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposePredAliasInput {
    /// The alias name to propose.
    pub alias_name: String,
    /// The canonical predicate ID this alias maps to.
    pub canonical_predicate_id: String,
    /// Confidence score for the alias (0.0 to 1.0).
    #[serde(default = "default_confidence")]
    pub confidence: f32,
}

const fn default_confidence() -> f32 {
    0.8
}

fn authorize_predicate_scope(ctx: &ToolContext, required_scope: &str) -> Result<(), String> {
    crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, required_scope)
}

fn predicate_denial(message: String) -> McpResult<Value> {
    serde_json::to_value(ToolResponse::<()>::error(message))
        .map_err(|e| McpError::ExecutionError(e.to_string()))
}

fn authorize_predicate_record(
    ctx: &ToolContext,
    record: &impl elephant_types::records::HasSecurity,
) -> Result<(), String> {
    ctx.authz
        .authorize_record_read(&ctx.principal, record, "read:predicate")?;
    ctx.authz
        .authorize_record_write(&ctx.principal, record, "write:predicate")
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `list_predicates` tool.
pub struct ListPredicatesHandler {
    predicate_repo: PredicateRegistryRepository,
    alias_repo: PredicateAliasRepository,
}

impl ListPredicatesHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            predicate_repo: PredicateRegistryRepository::new(storage.clone()),
            alias_repo: PredicateAliasRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListPredicatesHandler {
    fn name(&self) -> &'static str {
        "list_predicates"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List predicate definitions with optional alias mappings.\n\n",
            "Example:\n",
            r#"{ "status": "active", "include_aliases": true, "limit": 50 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter by status (active, deprecated)",
                    "enum": ["active", "deprecated"]
                },
                "include_aliases": {
                    "type": "boolean",
                    "default": true,
                    "description": "Include alias mappings in output"
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
        let input: ListPredicatesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "read:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        // Parse cursor if provided
        let cursor = if let Some(ref c) = input.cursor {
            Some(
                elephant_storage::parse_id(c, TableName::PredicateRegistry)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?,
            )
        } else {
            None
        };

        // Get predicates from repository
        let predicates = self
            .predicate_repo
            .list(&space_id, input.limit + 1, cursor.as_ref())
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let predicates = ctx
            .authz
            .filter_records(&ctx.principal, predicates, "read:predicate");

        // Filter by status
        let mut filtered: Vec<PredicateRegistry> = if let Some(ref status) = input.status {
            predicates
                .into_iter()
                .filter(|p| p.status == *status)
                .collect()
        } else {
            predicates
        };

        let has_more = filtered.len() > input.limit;
        if filtered.len() > input.limit {
            filtered.truncate(input.limit);
        }

        // Fetch aliases if requested
        let mut items: Vec<PredicateWithAliases> = Vec::with_capacity(filtered.len());

        if input.include_aliases {
            // Get all aliases for the space
            let all_aliases = self
                .alias_repo
                .list(&space_id, 1000, None)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            let all_aliases =
                ctx.authz
                    .filter_records(&ctx.principal, all_aliases, "read:predicate");

            for predicate in filtered {
                let pred_id = &predicate.predicate_id;
                let aliases: Vec<PredicateAlias> = all_aliases
                    .iter()
                    .filter(|a| a.predicate_id == *pred_id)
                    .cloned()
                    .collect();

                items.push(PredicateWithAliases {
                    predicate,
                    aliases: if aliases.is_empty() {
                        None
                    } else {
                        Some(aliases)
                    },
                });
            }
        } else {
            for predicate in filtered {
                items.push(PredicateWithAliases {
                    predicate,
                    aliases: None,
                });
            }
        }

        let next_cursor = if has_more {
            items.last().map(|p| {
                p.predicate
                    .base
                    .id
                    .to_string()
                    .strip_prefix("predicate_registry:")
                    .map_or_else(
                        || p.predicate.base.id.to_string(),
                        std::string::ToString::to_string,
                    )
            })
        } else {
            None
        };

        let output = ListPredicatesOutput {
            items,
            next_cursor,
            has_more,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `propose_predicate_alias` tool.
pub struct ProposePredAliasHandler;

impl ProposePredAliasHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self
    }
}

#[async_trait]
impl ToolHandler for ProposePredAliasHandler {
    fn name(&self) -> &'static str {
        "propose_predicate_alias"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Propose a new alias mapping for a predicate.\n\n",
            "Example:\n",
            r#"{ "alias_name": "employed_by", "canonical_predicate_id": "works_at", "confidence": 0.9 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["alias_name", "canonical_predicate_id"],
            "properties": {
                "alias_name": {
                    "type": "string",
                    "description": "The alias name to propose"
                },
                "canonical_predicate_id": {
                    "type": "string",
                    "description": "The canonical predicate ID this alias maps to"
                },
                "confidence": {
                    "type": "number",
                    "default": 0.8,
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "Confidence score for the alias"
                }
            }
        })
    }

    #[expect(
        clippy::assigning_clones,
        reason = "Keep the original owned field replacement and mutation ordering."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ProposePredAliasInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "write:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id)
            .await
            .map_err(McpError::StorageError)?;
        let predicate = PredicateRegistryRepository::new(ctx.storage.clone())
            .find_by_predicate_id(&space_id, &input.canonical_predicate_id)
            .await
            .map_err(McpError::StorageError)?
            .ok_or_else(|| McpError::ExecutionError("Canonical predicate not found".into()))?;
        if let Err(message) = authorize_predicate_record(ctx, &predicate) {
            return predicate_denial(message);
        }
        mutation.depend_on(CanonicalRecord::PredicateRegistry(Box::new(
            predicate.clone(),
        )));
        // Create the alias record
        let id = RecordId::new_v4(TableName::PredicateAlias);
        let mut base = BaseFields::new(id, space_id, "mcp_tool".to_string());
        base.security.policy_id = predicate.base.security.policy_id.clone();
        inherit_public_envelope(&mut base, &predicate.base)
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let alias = PredicateAlias {
            base,
            alias: input.alias_name,
            alias_type: AliasType::RawString,
            source_plugin: Some("mcp_propose".to_string()),
            confidence: input.confidence,
            status: AliasStatus::Proposed,
            scope: AliasScope::Global,
            predicate_id: input.canonical_predicate_id,
            confidence_version: 1,
            usage_count: 0,
        };

        if let Err(message) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &alias, "write:predicate")
        {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match mutation.create(alias, "mcp_tool").await {
            Ok(created) => {
                let response = ToolResponse::success(created);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

/// Input for `get_predicate` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPredicateInput {
    /// Predicate ID (the `predicate_id` field, not the record ID).
    pub predicate_id: String,
    /// Include alias mappings in output.
    #[serde(default = "default_include_aliases")]
    pub include_aliases: bool,
}

/// Handler for `get_predicate` tool.
pub struct GetPredicateHandler {
    predicate_repo: PredicateRegistryRepository,
    alias_repo: PredicateAliasRepository,
}

impl GetPredicateHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            predicate_repo: PredicateRegistryRepository::new(storage.clone()),
            alias_repo: PredicateAliasRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetPredicateHandler {
    fn name(&self) -> &'static str {
        "get_predicate"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a single predicate by its predicate_id with optional alias mappings.\n\n",
            "Example:\n",
            r#"{ "predicate_id": "works_at", "include_aliases": true }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["predicate_id"],
            "properties": {
                "predicate_id": {
                    "type": "string",
                    "description": "The predicate identifier (e.g., 'works_at', 'located_in')"
                },
                "include_aliases": {
                    "type": "boolean",
                    "default": true,
                    "description": "Include alias mappings in output"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetPredicateInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "read:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        // Get all predicates and find by predicate_id
        let predicates = self
            .predicate_repo
            .list(&space_id, 1000, None)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let predicates = ctx
            .authz
            .filter_records(&ctx.principal, predicates, "read:predicate");

        let predicate = predicates
            .into_iter()
            .find(|p| p.predicate_id == input.predicate_id);

        if let Some(pred) = predicate {
            let aliases = if input.include_aliases {
                let all_aliases = self
                    .alias_repo
                    .list(&space_id, 1000, None)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                let all_aliases =
                    ctx.authz
                        .filter_records(&ctx.principal, all_aliases, "read:predicate");

                let matching: Vec<PredicateAlias> = all_aliases
                    .into_iter()
                    .filter(|a| a.predicate_id == input.predicate_id)
                    .collect();

                if matching.is_empty() {
                    None
                } else {
                    Some(matching)
                }
            } else {
                None
            };

            let output = PredicateWithAliases {
                predicate: pred,
                aliases,
            };

            let response = ToolResponse::success(output);
            serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
        } else {
            let response: ToolResponse<PredicateWithAliases> =
                ToolResponse::error(format!("Predicate not found: {}", input.predicate_id));
            serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
        }
    }
}

/// Input for `register_predicate` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPredicateInput {
    /// The predicate identifier (e.g., `works_at`, `located_in`).
    pub predicate_id: String,
    /// Description of the predicate's semantics.
    #[serde(default)]
    pub description: Option<String>,
}

/// Handler for `register_predicate` tool.
pub struct RegisterPredicateHandler {
    predicate_repo: PredicateRegistryRepository,
}

impl RegisterPredicateHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            predicate_repo: PredicateRegistryRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RegisterPredicateHandler {
    fn name(&self) -> &'static str {
        "register_predicate"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Register a new predicate in the predicate registry.\n\n",
            "Example:\n",
            r#"{ "predicate_id": "works_at", "description": "Subject works for or is employed by object" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["predicate_id"],
            "properties": {
                "predicate_id": {
                    "type": "string",
                    "description": "Unique predicate identifier (e.g., 'works_at')"
                },
                "description": {
                    "type": "string",
                    "description": "Description of the predicate's semantics"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RegisterPredicateInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "write:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id)
            .await
            .map_err(McpError::StorageError)?;
        if let Some(existing) = self
            .predicate_repo
            .find_by_predicate_id(&space_id, &input.predicate_id)
            .await
            .map_err(McpError::StorageError)?
        {
            if let Err(message) = authorize_predicate_record(ctx, &existing) {
                return predicate_denial(message);
            }
            return predicate_denial(format!("Predicate already exists: {}", input.predicate_id));
        }

        // Create the predicate registry record
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, space_id, "mcp_tool".to_string());

        let predicate = PredicateRegistry {
            base,
            predicate_id: input.predicate_id,
            description: input.description,
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::default(),
            first_seen_at: Some(chrono::Utc::now()),
            usage_count: 0,
            last_used_at: None,
        };

        if let Err(message) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &predicate, "write:predicate")
        {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match mutation.create(predicate, "mcp_tool").await {
            Ok(created) => {
                let response = ToolResponse::success(created);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

// ============================================================================
// Approve Predicate Proposal
// ============================================================================

/// Input for `approve_predicate_proposal` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovePredicateProposalInput {
    /// The `work_artifact` ID of the proposal to approve.
    pub proposal_id: String,
    /// Override confidence score for the created alias. Defaults to 0.95.
    #[serde(default = "default_approval_confidence")]
    pub confidence: f32,
}

const fn default_approval_confidence() -> f32 {
    0.95
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PredicateProposalData {
    predicate_id: String,
    proposed_aliases: Vec<ProposedAlias>,
    #[allow(dead_code)]
    confidence: f32,
    space_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProposedAlias {
    alias: String,
    source: String,
    confidence: f32,
}

// ============================================================================
// Validate Predicate Proposal
// ============================================================================

/// Input for `validate_predicate_proposal` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatePredicateProposalInput {
    /// The `work_artifact` ID of the proposal to validate.
    pub proposal_id: String,
}

/// Handler for `validate_predicate_proposal` tool.
#[allow(clippy::struct_field_names)]
pub struct ValidatePredicateProposalHandler {
    artifact_repo: WorkArtifactRepository,
    alias_repo: PredicateAliasRepository,
    predicate_repo: PredicateRegistryRepository,
}

impl ValidatePredicateProposalHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            artifact_repo: WorkArtifactRepository::new(storage.clone()),
            alias_repo: PredicateAliasRepository::new(storage.clone()),
            predicate_repo: PredicateRegistryRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ValidatePredicateProposalHandler {
    fn name(&self) -> &'static str {
        "validate_predicate_proposal"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Validate a predicate proposal work artifact.\n\n",
            "Example:\n",
            r#"{ "proposal_id": "work_artifact:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["proposal_id"],
            "properties": {
                "proposal_id": {
                    "type": "string",
                    "description": "The work_artifact ID of the proposal to validate"
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ValidatePredicateProposalInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "write:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        let artifact_id =
            elephant_storage::parse_id(&input.proposal_id, TableName::WorkArtifact)
                .map_err(|e| McpError::InvalidParameters(format!("Invalid proposal_id: {e}")))?;

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id)
            .await
            .map_err(McpError::StorageError)?;
        let artifact = self
            .artifact_repo
            .get(&space_id, &artifact_id)
            .await
            .map_err(McpError::StorageError)?
            .ok_or_else(|| {
                McpError::ExecutionError(format!("Proposal not found: {}", input.proposal_id))
            })?;

        if let Err(message) = authorize_predicate_record(ctx, &artifact.base) {
            return predicate_denial(message);
        }

        if artifact.artifact_type != "predicate_proposal" {
            let response: ToolResponse<()> = ToolResponse::error(format!(
                "Invalid artifact type: expected 'predicate_proposal', got '{}'",
                artifact.artifact_type
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if artifact.status != ArtifactStatus::Draft {
            let response: ToolResponse<()> = ToolResponse::error(format!(
                "Invalid artifact status: expected 'draft', got '{}'",
                artifact.status
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let proposal: PredicateProposalData = serde_json::from_value(artifact.data.clone())
            .map_err(|e| McpError::ExecutionError(format!("Invalid proposal data: {e}")))?;

        if proposal.space_id != space_id {
            return Err(McpError::ExecutionError(
                "Proposal space_id does not match request space_id".to_string(),
            ));
        }

        let mut validation_errors = Vec::new();

        if let Some(predicate) = self
            .predicate_repo
            .find_by_predicate_id(&space_id, &proposal.predicate_id)
            .await
            .map_err(McpError::StorageError)?
        {
            if let Err(message) = authorize_predicate_record(ctx, &predicate) {
                return predicate_denial(message);
            }
            mutation.depend_on(CanonicalRecord::PredicateRegistry(Box::new(predicate)));
        } else {
            validation_errors.push(format!(
                "Canonical predicate '{}' does not exist in registry",
                proposal.predicate_id
            ));
        }
        if proposal.proposed_aliases.is_empty() || proposal.predicate_id.trim().is_empty() {
            validation_errors.push("Proposal must name a predicate and at least one alias".into());
        }
        let mut proposed_names = std::collections::BTreeSet::new();
        for proposed_alias in &proposal.proposed_aliases {
            if proposed_alias.alias.trim().is_empty()
                || !proposed_names.insert(proposed_alias.alias.to_ascii_lowercase())
            {
                validation_errors.push("Proposal contains an empty or duplicate alias".into());
            }
            if let Some(alias) = self
                .alias_repo
                .find_active_by_raw(&space_id, &proposed_alias.alias)
                .await
                .map_err(McpError::StorageError)?
            {
                if let Err(message) = authorize_predicate_record(ctx, &alias) {
                    return predicate_denial(message);
                }
                mutation.depend_on(CanonicalRecord::PredicateAlias(Box::new(alias)));
                validation_errors.push(format!(
                    "Active alias already exists for '{}'",
                    proposed_alias.alias
                ));
            }
        }

        let mut updated = artifact.clone();
        if validation_errors.is_empty() {
            updated.status = ArtifactStatus::Validated;
            updated.validated_at = Some(chrono::Utc::now());
            updated.validation_errors = None;
        } else {
            updated.status = ArtifactStatus::Rejected;
            updated.validation_errors = Some(validation_errors);
        }

        if let Err(message) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &updated.base, "write:predicate")
        {
            return predicate_denial(message);
        }
        let updated = mutation
            .replace(&artifact, updated, false, "mcp_tool")
            .await
            .map_err(McpError::StorageError)?;

        let response = ToolResponse::success(updated);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `approve_predicate_proposal` tool.
pub struct ApprovePredicateProposalHandler {
    artifact_repo: WorkArtifactRepository,
}

impl ApprovePredicateProposalHandler {
    /// Creates a new handler.
    #[must_use]
    #[expect(
        clippy::missing_const_for_fn,
        reason = "Preserve the existing runtime constructor signature."
    )]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            artifact_repo: WorkArtifactRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ApprovePredicateProposalHandler {
    fn name(&self) -> &'static str {
        "approve_predicate_proposal"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Approve a predicate alias proposal from a work artifact, creating an active alias.\n\n",
            "Example:\n",
            r#"{ "proposal_id": "work_artifact:550e8400-e29b-41d4-a716-446655440000", "confidence": 0.95 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["proposal_id"],
            "properties": {
                "proposal_id": {
                    "type": "string",
                    "description": "The work_artifact ID of the proposal to approve"
                },
                "confidence": {
                    "type": "number",
                    "default": 0.95,
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "Override confidence score for the created alias"
                }
            }
        })
    }

    #[expect(
        clippy::items_after_statements,
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ApprovePredicateProposalInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) = authorize_predicate_scope(ctx, "write:predicate") {
            let response: ToolResponse<()> = ToolResponse::error(message);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let space_id = ctx.principal.space_id.clone();

        // Parse the proposal ID
        let artifact_id =
            elephant_storage::parse_id(&input.proposal_id, TableName::WorkArtifact)
                .map_err(|e| McpError::InvalidParameters(format!("Invalid proposal_id: {e}")))?;

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id)
            .await
            .map_err(McpError::StorageError)?;
        // Find the work artifact
        let artifact = self
            .artifact_repo
            .get(&space_id, &artifact_id)
            .await
            .map_err(McpError::StorageError)?
            .ok_or_else(|| {
                McpError::ExecutionError(format!("Proposal not found: {}", input.proposal_id))
            })?;

        if let Err(message) = authorize_predicate_record(ctx, &artifact.base) {
            return predicate_denial(message);
        }
        // Verify artifact type is predicate_proposal
        if artifact.artifact_type != "predicate_proposal" {
            let response: ToolResponse<()> = ToolResponse::error(format!(
                "Invalid artifact type: expected 'predicate_proposal', got '{}'",
                artifact.artifact_type
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Verify artifact status is Validated
        if artifact.status != ArtifactStatus::Validated {
            let response: ToolResponse<()> = ToolResponse::error(format!(
                "Invalid artifact status: expected 'validated', got '{}'",
                artifact.status
            ));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        use elephant_pipeline::public_predicate_proposal::{
            PublicPredicateProposal, PublicProposalOptions,
        };
        let observed = PublicPredicateProposal::load(
            ctx.storage.clone(),
            mutation,
            artifact,
            PublicProposalOptions {
                allow_draft: false,
                ensure_registry: false,
                check_work_run_budget: false,
                confidence: Some(input.confidence),
            },
        )
        .await
        .map_err(McpError::from)?;
        if let Some(registry) = &observed.registry {
            if let Err(message) = authorize_predicate_record(ctx, registry) {
                return predicate_denial(message);
            }
        }
        for alias in &observed.aliases {
            if let Err(message) = authorize_predicate_record(ctx, alias) {
                return predicate_denial(message);
            }
        }
        let prepared = observed.prepare("mcp_tool").map_err(McpError::from)?;
        for base in std::iter::once(&prepared.artifact.base)
            .chain(prepared.aliases.iter().map(|alias| &alias.base))
            .chain(std::iter::once(&prepared.reprocess_job.base))
        {
            if let Err(message) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, base, "write:predicate")
            {
                return predicate_denial(message);
            }
        }
        let created_alias = prepared.commit("mcp_tool").await.map_err(McpError::from)?;

        let response = ToolResponse::success(created_alias);
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

    #[test]
    fn test_list_predicates_input_defaults() {
        let input: ListPredicatesInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.include_aliases);
        assert!(input.status.is_none());
    }

    #[test]
    fn test_propose_alias_input_defaults() {
        let input: ProposePredAliasInput =
            serde_json::from_str(r#"{"alias_name": "test", "canonical_predicate_id": "works_at"}"#)
                .unwrap();
        assert!((input.confidence - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn test_get_predicate_input_defaults() {
        let input: GetPredicateInput =
            serde_json::from_str(r#"{"predicate_id": "works_at"}"#).unwrap();
        assert!(input.include_aliases);
    }

    #[test]
    fn test_register_predicate_input_defaults() {
        let input: RegisterPredicateInput =
            serde_json::from_str(r#"{"predicate_id": "new_pred"}"#).unwrap();
        assert!(input.description.is_none());
    }

    #[test]
    fn test_approve_predicate_proposal_input_schema() {
        // Test that proposal_id is required
        let result: Result<ApprovePredicateProposalInput, _> = serde_json::from_str("{}");
        assert!(result.is_err(), "proposal_id should be required");

        // Test with valid input and default confidence
        let input: ApprovePredicateProposalInput =
            serde_json::from_str(r#"{"proposal_id": "work_artifact:abc123"}"#).unwrap();
        assert_eq!(input.proposal_id, "work_artifact:abc123");
        assert!(
            (input.confidence - 0.95).abs() < f32::EPSILON,
            "default confidence should be 0.95"
        );

        // Test with custom confidence
        let input: ApprovePredicateProposalInput =
            serde_json::from_str(r#"{"proposal_id": "test-id", "confidence": 0.85}"#).unwrap();
        assert_eq!(input.proposal_id, "test-id");
        assert!((input.confidence - 0.85).abs() < f32::EPSILON);
    }

    #[test]
    fn test_validate_predicate_proposal_input_schema() {
        let result: Result<ValidatePredicateProposalInput, _> = serde_json::from_str("{}");
        assert!(result.is_err(), "proposal_id should be required");

        let input: ValidatePredicateProposalInput =
            serde_json::from_str(r#"{"proposal_id": "work_artifact:abc123"}"#).unwrap();
        assert_eq!(input.proposal_id, "work_artifact:abc123");
    }

    use elephant_policy::{Clearance, Principal};
    use elephant_storage::{
        PredicateRegistryRepository, StorageClient, StorageConfig, WorkArtifactRepository,
    };
    use elephant_types::records::base::Security;
    use elephant_types::records::work::{ArtifactStatus as WaStatus, WorkArtifact};
    use elephant_types::Link;
    use std::collections::HashSet;

    fn make_test_principal(space_id: &str) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: HashSet::from(["read:predicate".to_string(), "write:predicate".to_string()]),
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

    async fn setup_test_storage() -> StorageClient {
        use std::sync::atomic::{AtomicU64, Ordering};
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99100);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = StorageConfig {
            database: format!("mcp_predicate_test_{db_id}"),
            ..StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("complete native schema before fixture writes");
        seed_predicate_test_space(&storage).await;
        storage
    }

    async fn seed_predicate_test_space(storage: &StorageClient) {
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"test_space",
            "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        }))
        .expect("space config");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
    }

    fn create_test_work_artifact(
        space_id: &str,
        raw_pred: &str,
        canonical: &str,
        status: WaStatus,
    ) -> WorkArtifact {
        use elephant_types::TableName;

        let id = RecordId::new_v4(TableName::WorkArtifact);
        let base = BaseFields::new(id, space_id.to_string(), "test".to_string());
        let work_run_id = RecordId::new_v4(TableName::WorkRun);

        WorkArtifact {
            base,
            work_run: Link::new(work_run_id),
            artifact_type: "predicate_proposal".to_string(),
            data: serde_json::json!({
                "predicate_id": canonical,
                "proposed_aliases": [{
                    "alias": raw_pred,
                    "source": "canonicalization",
                    "confidence": 0.72
                }],
                "confidence": 0.72,
                "space_id": space_id
            }),
            status,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_approve_predicate_proposal_creates_active_alias() {
        use crate::handlers::ToolContext;
        use elephant_storage::Repository;

        let storage = setup_test_storage().await;
        let space_id = "test_space";
        PredicateRegistryRepository::new(storage.clone())
            .create(PredicateRegistry {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::PredicateRegistry),
                    space_id.into(),
                    "test".into(),
                ),
                predicate_id: "works_at".into(),
                description: None,
                status: "active".into(),
                semantics: None,
                semantics_status: SemanticsStatus::Default,
                first_seen_at: None,
                usage_count: 0,
                last_used_at: None,
            })
            .await
            .expect("registry");

        // Create a predicate proposal work artifact with Validated status
        let artifact =
            create_test_work_artifact(space_id, "worksAt", "works_at", WaStatus::Validated);
        let artifact_id = artifact.base.id.clone();

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(artifact)
            .await
            .expect("create artifact");

        // Create handler and context
        let handler = ApprovePredicateProposalHandler::new(storage.clone());
        let principal = make_test_principal(space_id);
        let ctx = ToolContext::new(storage.clone(), principal);

        // Execute the approval
        let params = serde_json::json!({
            "proposal_id": artifact_id.to_string()
        });
        let result = handler.execute(&ctx, params).await.expect("execute");

        // Verify the response contains success and the created alias
        let response: crate::tools::ToolResponse<PredicateAlias> =
            serde_json::from_value(result).expect("deserialize response");
        assert!(response.ok, "Response should be successful");

        let alias = response.data.expect("alias should exist");
        assert_eq!(alias.alias, "worksAt");
        assert_eq!(alias.predicate_id, "works_at");
        assert_eq!(alias.status, AliasStatus::Active);
        assert!((alias.confidence - 0.95).abs() < f32::EPSILON);

        // Verify the artifact was updated to Committed status
        let updated_artifact = artifact_repo
            .get(space_id, &artifact_id)
            .await
            .expect("get artifact")
            .expect("artifact should exist");
        assert_eq!(updated_artifact.status, WaStatus::Committed);
        assert!(updated_artifact.committed_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_validate_predicate_proposal_accepts_valid_draft() {
        use crate::handlers::ToolContext;
        use elephant_storage::Repository;

        let storage = setup_test_storage().await;
        let space_id = "test_space";

        let registry_repo = PredicateRegistryRepository::new(storage.clone());
        let registry_id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(registry_id, space_id.to_string(), "test".to_string());
        let predicate = PredicateRegistry {
            base,
            predicate_id: "works_at".to_string(),
            description: None,
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::default(),
            first_seen_at: Some(chrono::Utc::now()),
            usage_count: 0,
            last_used_at: None,
        };
        registry_repo
            .create(predicate)
            .await
            .expect("create predicate");

        let artifact = create_test_work_artifact(space_id, "worksAt", "works_at", WaStatus::Draft);
        let artifact_id = artifact.base.id.clone();

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(artifact)
            .await
            .expect("create artifact");

        let handler = ValidatePredicateProposalHandler::new(storage.clone());
        let principal = make_test_principal(space_id);
        let ctx = ToolContext::new(storage.clone(), principal);

        let params = serde_json::json!({
            "proposal_id": artifact_id.to_string()
        });
        let result = handler.execute(&ctx, params).await.expect("execute");

        let response: crate::tools::ToolResponse<WorkArtifact> =
            serde_json::from_value(result).expect("deserialize response");
        assert!(response.ok, "Response should be successful");
        let validated = response.data.expect("artifact should exist");
        assert_eq!(validated.status, WaStatus::Validated);

        let updated = artifact_repo
            .get(space_id, &artifact_id)
            .await
            .expect("get artifact")
            .expect("artifact should exist");
        assert_eq!(updated.status, WaStatus::Validated);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_validate_predicate_proposal_rejects_missing_predicate() {
        use crate::handlers::ToolContext;
        use elephant_storage::Repository;

        let storage = setup_test_storage().await;
        let space_id = "test_space";

        let artifact = create_test_work_artifact(space_id, "worksAt", "works_at", WaStatus::Draft);
        let artifact_id = artifact.base.id.clone();

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(artifact)
            .await
            .expect("create artifact");

        let handler = ValidatePredicateProposalHandler::new(storage.clone());
        let principal = make_test_principal(space_id);
        let ctx = ToolContext::new(storage.clone(), principal);

        let params = serde_json::json!({
            "proposal_id": artifact_id.to_string()
        });
        let result = handler.execute(&ctx, params).await.expect("execute");

        let response: crate::tools::ToolResponse<WorkArtifact> =
            serde_json::from_value(result).expect("deserialize response");
        assert!(response.ok, "Response should be successful");
        let validated = response.data.expect("artifact should exist");
        assert_eq!(validated.status, WaStatus::Rejected);
        assert!(validated.validation_errors.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_approve_predicate_proposal_rejects_non_validated() {
        use crate::handlers::ToolContext;
        use elephant_storage::Repository;

        let storage = setup_test_storage().await;
        let space_id = "test_space";

        // Create a predicate proposal with Draft status (not Validated)
        let artifact = create_test_work_artifact(space_id, "worksAt", "works_at", WaStatus::Draft);
        let artifact_id = artifact.base.id.clone();

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(artifact)
            .await
            .expect("create artifact");

        // Create handler and context
        let handler = ApprovePredicateProposalHandler::new(storage.clone());
        let principal = make_test_principal(space_id);
        let ctx = ToolContext::new(storage.clone(), principal);

        // Execute the approval - should fail
        let params = serde_json::json!({
            "proposal_id": artifact_id.to_string()
        });
        let result = handler.execute(&ctx, params).await.expect("execute");

        // Verify the response contains an error
        let response: crate::tools::ToolResponse<PredicateAlias> =
            serde_json::from_value(result).expect("deserialize response");
        assert!(
            !response.ok,
            "Response should fail for non-validated artifact"
        );
        assert!(response.error.as_ref().unwrap().contains("validated"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_approve_predicate_proposal_rejects_wrong_type() {
        use crate::handlers::ToolContext;
        use elephant_storage::Repository;
        use elephant_types::TableName;

        let storage = setup_test_storage().await;
        let space_id = "test_space";

        // Create an artifact with wrong type
        let id = RecordId::new_v4(TableName::WorkArtifact);
        let base = BaseFields::new(id.clone(), space_id.to_string(), "test".to_string());
        let work_run_id = RecordId::new_v4(TableName::WorkRun);

        let artifact = WorkArtifact {
            base,
            work_run: Link::new(work_run_id),
            artifact_type: "knowledge_candidates".to_string(), // Wrong type
            data: serde_json::json!({}),
            status: WaStatus::Validated,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(artifact)
            .await
            .expect("create artifact");

        // Create handler and context
        let handler = ApprovePredicateProposalHandler::new(storage.clone());
        let principal = make_test_principal(space_id);
        let ctx = ToolContext::new(storage.clone(), principal);

        // Execute the approval - should fail
        let params = serde_json::json!({
            "proposal_id": id.to_string()
        });
        let result = handler.execute(&ctx, params).await.expect("execute");

        // Verify the response contains an error
        let response: crate::tools::ToolResponse<PredicateAlias> =
            serde_json::from_value(result).expect("deserialize response");
        assert!(!response.ok, "Response should fail for wrong artifact type");
        assert!(response
            .error
            .as_ref()
            .unwrap()
            .contains("predicate_proposal"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_register_predicate_requires_write_predicate_scope() {
        use crate::handlers::ToolContext;

        let storage = setup_test_storage().await;
        let principal = Principal {
            space_id: "test_space".to_string(),
            scopes: HashSet::from(["read:predicate".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage.clone(), principal);
        let handler = RegisterPredicateHandler::new(storage);

        let result = handler
            .execute(
                &ctx,
                serde_json::json!({"predicate_id": "works_at", "description": "test"}),
            )
            .await
            .expect("handler response");
        let decoded: crate::tools::ToolResponse<PredicateRegistry> =
            serde_json::from_value(result).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Access denied"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_get_predicate_filters_records_by_security() {
        use elephant_storage::Repository;

        let storage = setup_test_storage().await;
        let space_id = "test_space";
        let repo = PredicateRegistryRepository::new(storage.clone());
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let mut base = BaseFields::new(id, space_id.to_string(), "test".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 5,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        let predicate = PredicateRegistry {
            base,
            predicate_id: "secret_predicate".to_string(),
            description: None,
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::default(),
            first_seen_at: Some(chrono::Utc::now()),
            usage_count: 0,
            last_used_at: None,
        };
        repo.create(predicate).await.expect("create predicate");

        let principal = Principal {
            space_id: space_id.to_string(),
            scopes: HashSet::from(["read:predicate".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage.clone(), principal);
        let handler = GetPredicateHandler::new(storage);

        let result = handler
            .execute(
                &ctx,
                serde_json::json!({"predicate_id": "secret_predicate"}),
            )
            .await
            .expect("handler response");
        let decoded: crate::tools::ToolResponse<PredicateWithAliases> =
            serde_json::from_value(result).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Predicate not found"));
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn validation_authorizes_original_classification_before_changing_artifact() {
        for embedded in [true, false] {
            let storage = if embedded {
                let storage = StorageClient::new_embedded().await.expect("embedded");
                seed_predicate_test_space(&storage).await;
                storage
            } else {
                setup_test_storage().await
            };
            let artifact_repo = WorkArtifactRepository::new(storage.clone());
            let mut artifact = create_test_work_artifact(
                "test_space",
                "private alias",
                "private predicate",
                WaStatus::Draft,
            );
            artifact.base.security.level = 5;
            artifact_repo
                .create(artifact.clone())
                .await
                .expect("artifact");
            let handler = ValidatePredicateProposalHandler::new(storage.clone());
            let mut ctx = ToolContext::new(storage.clone(), make_test_principal("test_space"));
            let input = json!({"proposal_id":artifact.base.id.to_string()});
            let response: ToolResponse<WorkArtifact> = serde_json::from_value(
                handler
                    .execute(&ctx, input.clone())
                    .await
                    .expect("response"),
            )
            .expect("decode");
            assert!(!response.ok);
            let unchanged = artifact_repo
                .get("test_space", &artifact.base.id)
                .await
                .expect("read")
                .expect("artifact");
            assert_eq!(
                serde_json::to_value(&artifact).expect("before"),
                serde_json::to_value(unchanged).expect("after")
            );
            assert!(elephant_storage::AuditLogRepository::new(storage.clone())
                .list("test_space", 100, None)
                .await
                .expect("audit")
                .is_empty());
            ctx.principal.clearances[0].max_level = 5;
            let response: ToolResponse<WorkArtifact> = serde_json::from_value(
                handler
                    .execute(&ctx, input)
                    .await
                    .expect("authorized response"),
            )
            .expect("decode");
            assert!(response.ok);
            let rejected = response.data.expect("artifact");
            assert_eq!(rejected.status, WaStatus::Rejected);
            assert_eq!(rejected.base.security.level, 5);
        }
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn approval_authorizes_original_artifact_and_preserves_classification() {
        for embedded in [true, false] {
            let storage = if embedded {
                let storage = StorageClient::new_embedded().await.expect("embedded");
                seed_predicate_test_space(&storage).await;
                storage
            } else {
                setup_test_storage().await
            };
            let artifacts = WorkArtifactRepository::new(storage.clone());
            let mut artifact = create_test_work_artifact(
                "test_space",
                "confidential alias",
                "works_at",
                WaStatus::Validated,
            );
            artifact.base.security.level = 5;
            artifacts.create(artifact.clone()).await.expect("artifact");
            let mut base = artifact.base.clone();
            base.id = RecordId::new_v4(TableName::PredicateRegistry);
            PredicateRegistryRepository::new(storage.clone())
                .create(PredicateRegistry {
                    base,
                    predicate_id: "works_at".into(),
                    description: None,
                    status: "active".into(),
                    semantics: None,
                    semantics_status: SemanticsStatus::Default,
                    first_seen_at: None,
                    usage_count: 0,
                    last_used_at: None,
                })
                .await
                .expect("registry");
            let handler = ApprovePredicateProposalHandler::new(storage.clone());
            let mut ctx = ToolContext::new(storage.clone(), make_test_principal("test_space"));
            let input = json!({"proposal_id":artifact.base.id.to_string()});
            let response: ToolResponse<PredicateAlias> = serde_json::from_value(
                handler
                    .execute(&ctx, input.clone())
                    .await
                    .expect("response"),
            )
            .expect("decode");
            assert!(!response.ok);
            assert_eq!(
                artifacts
                    .get("test_space", &artifact.base.id)
                    .await
                    .expect("read")
                    .expect("artifact")
                    .status,
                WaStatus::Validated
            );
            assert!(PredicateAliasRepository::new(storage.clone())
                .list("test_space", 100, None)
                .await
                .expect("aliases")
                .is_empty());
            assert!(
                elephant_storage::ReprocessJobRepository::new(storage.clone())
                    .list("test_space", 100, None)
                    .await
                    .expect("jobs")
                    .is_empty()
            );
            ctx.principal.clearances[0].max_level = 5;
            let response: ToolResponse<PredicateAlias> = serde_json::from_value(
                handler
                    .execute(&ctx, input)
                    .await
                    .expect("authorized response"),
            )
            .expect("decode");
            assert!(response.ok);
            let alias = response.data.expect("alias");
            assert_eq!(alias.status, AliasStatus::Active);
            assert_eq!(alias.base.security.level, 5);
            let jobs = elephant_storage::ReprocessJobRepository::new(storage.clone())
                .list("test_space", 100, None)
                .await
                .expect("jobs");
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].base.security.level, 5);
        }
    }

    #[test]
    fn approval_pipeline_error_preserves_typed_erasure_barrier() {
        let error = McpError::from(elephant_pipeline::PipelineError::StorageError(
            elephant_storage::StorageError::ErasureInProgress,
        ));
        assert!(matches!(
            error,
            McpError::StorageError(elephant_storage::StorageError::ErasureInProgress)
        ));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn registry_handler_returns_typed_erasure_barrier_and_rolls_back() {
        let storage = setup_test_storage().await;
        storage.query_checked("DEFINE EVENT test_erasure_barrier ON TABLE predicate_registry WHEN $event = 'CREATE' THEN { THROW 'publication:erasure_space_reserved'; };").await.expect("barrier event");
        let ctx = ToolContext::new(storage.clone(), make_test_principal("test_space"));
        let handler = RegisterPredicateHandler::new(storage.clone());
        let result = handler
            .execute(&ctx, json!({"predicate_id":"retained-predicate"}))
            .await;
        assert!(matches!(
            result,
            Err(McpError::StorageError(
                elephant_storage::StorageError::ErasureInProgress
            ))
        ));
        assert!(PredicateRegistryRepository::new(storage.clone())
            .list("test_space", 100, None)
            .await
            .expect("registry")
            .is_empty());
        assert!(elephant_storage::AuditLogRepository::new(storage.clone())
            .list("test_space", 100, None)
            .await
            .expect("audits")
            .is_empty());
        assert!(elephant_storage::SystemEventRepository::new(storage)
            .get_after_seq("test_space", 0, 100)
            .await
            .expect("outbox")
            .is_empty());
    }
}
