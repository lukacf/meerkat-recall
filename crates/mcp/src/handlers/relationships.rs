#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Relationship MCP tool handlers.
//!
//! Implements:
//! - `relate_entities`
//! - `upsert_relationship`
//! - `update_relationship`
//! - `delete_relationship`
//! - `restore_relationship`
//! - `batch_relationships`

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use elephant_storage::{
    parse_id, AssertionRepository, PredicateAliasRepository, PredicateRegistryRepository,
    RelRepository, Repository, StorageClient, StorageError,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::predicate::{PredicateRegistry, SemanticsStatus};
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::rel::{EvidenceRef, TimeBasis};
use elephant_types::Link;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{BatchError, ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `relate_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelateEntitiesInput {
    /// Source entity ID.
    pub from_id: String,
    /// Target entity ID.
    pub to_id: String,
    /// Relationship type.
    pub rel_type: String,
    /// Relationship weight (0.0 to 1.0).
    #[serde(default)]
    pub weight: Option<f64>,
    /// Valid from timestamp.
    #[serde(default)]
    pub valid_from: Option<String>,
    /// Valid to timestamp.
    #[serde(default)]
    pub valid_to: Option<String>,
    /// Relationship description.
    #[serde(default)]
    pub description: Option<String>,
    /// Additional properties.
    #[serde(default)]
    pub properties: Option<Value>,
    /// Evidence quote text supporting this relationship (optional).
    #[serde(default)]
    pub evidence_text: Option<String>,
    /// Evidence source record ID (e.g., `source:...`) or external reference (optional).
    #[serde(default)]
    pub evidence_source: Option<String>,
    /// Space ID.
    #[serde(default = "default_space")]
    pub space_id: String,
}

fn default_space() -> String {
    "default".to_string()
}

fn sha256_hex_prefixed(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

async fn canonicalize_and_register_rel_type(
    storage: &StorageClient,
    space_id: &str,
    rel_type: &str,
) -> McpResult<String> {
    let trimmed = rel_type.trim();
    if trimmed.is_empty() {
        return Err(McpError::InvalidParameters(
            "rel_type must not be empty".to_string(),
        ));
    }

    let alias_repo = PredicateAliasRepository::new(storage.clone());
    let canonical = alias_repo
        .find_active_by_raw(space_id, trimmed)
        .await
        .map_err(|e| McpError::ExecutionError(e.to_string()))?
        .map_or_else(|| trimmed.to_string(), |alias| alias.predicate_id);

    ensure_predicate_registered(storage, space_id, &canonical).await?;
    Ok(canonical)
}

async fn ensure_predicate_registered(
    storage: &StorageClient,
    space_id: &str,
    predicate_id: &str,
) -> McpResult<()> {
    let predicate_repo = PredicateRegistryRepository::new(storage.clone());
    if predicate_repo
        .find_by_predicate_id(space_id, predicate_id)
        .await
        .map_err(|e| McpError::ExecutionError(e.to_string()))?
        .is_some()
    {
        return Ok(());
    }

    let now = Utc::now();
    let predicate = PredicateRegistry {
        base: BaseFields::new(
            RecordId::new_v4(TableName::PredicateRegistry),
            space_id.to_string(),
            "mcp_relationship_write".to_string(),
        ),
        predicate_id: predicate_id.to_string(),
        description: None,
        status: "active".to_string(),
        semantics: None,
        semantics_status: SemanticsStatus::Default,
        first_seen_at: Some(now),
        usage_count: 0,
        last_used_at: Some(now),
    };

    match predicate_repo.create(predicate).await {
        Ok(_) | Err(StorageError::Conflict(_)) => Ok(()),
        Err(e) => Err(McpError::ExecutionError(e.to_string())),
    }
}

/// Input for `upsert_relationship` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertRelationshipInput {
    /// Source entity ID.
    pub from_id: String,
    /// Target entity ID.
    pub to_id: String,
    /// Relationship type.
    pub rel_type: String,
    /// Fields to set/update.
    #[serde(default)]
    pub fields: Option<Value>,
}

/// Input for `update_relationship` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRelationshipInput {
    /// Relationship ID.
    pub id: String,
    /// Fields to update.
    pub fields: Value,
}

/// Input for delete/restore tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelIdInput {
    /// Relationship ID.
    pub id: String,
}

/// Input for `batch_relationships` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRelationshipsInput {
    /// Items to process.
    pub items: Vec<RelateEntitiesInput>,
}

/// Output for batch operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRelOutput {
    /// Successfully processed items.
    pub succeeded: Vec<Rel>,
    /// Failed items.
    pub failed: Vec<BatchError>,
}

/// Helper to create a Link from an entity ID string.
fn make_entity_link(id: &str) -> Link<elephant_types::records::Entity> {
    let record_id = id
        .parse::<RecordId>()
        .or_else(|_| uuid::Uuid::parse_str(id).map(|uuid| RecordId::new(TableName::Entity, uuid)))
        .unwrap_or_else(|_| RecordId::new_v4(TableName::Entity));
    Link::new(record_id)
}

fn revision_id_for_evidence_source(source: Option<&str>) -> RecordId {
    let Some(source) = source else {
        return RecordId::new_v4(TableName::DocRevision);
    };

    source
        .parse::<RecordId>()
        .ok()
        .filter(|rid| rid.table() == TableName::DocRevision)
        .unwrap_or_else(|| RecordId::new_v4(TableName::DocRevision))
}

fn evidence_span_for_manual_quote(
    evidence_source: Option<&str>,
    evidence_text: &str,
) -> McpResult<EvidenceSpan> {
    let end: u32 = evidence_text
        .len()
        .try_into()
        .map_err(|_| McpError::InvalidParameters("evidence_text too large".to_string()))?;

    Ok(EvidenceSpan {
        revision: Link::new(revision_id_for_evidence_source(evidence_source)),
        node: None,
        start: 0,
        end,
        quote_hash: sha256_hex_prefixed(evidence_text),
    })
}

#[allow(clippy::cast_possible_truncation)]
async fn create_evidence_assertion(
    repo: &AssertionRepository,
    space_id: &str,
    input: &RelateEntitiesInput,
) -> McpResult<Assertion> {
    let now = Utc::now();

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Assertion),
        space_id.to_string(),
        "mcp_tool".to_string(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance {
        sources: input
            .evidence_source
            .as_deref()
            .and_then(|s| s.parse::<RecordId>().ok())
            .map(|rid| vec![rid])
            .unwrap_or_default(),
        extractor: None,
        work_run_id: None,
        run_id: None,
        prompt_id: None,
        config_id: None,
        chain: Vec::new(),
    };
    base.subjects_state = SubjectsState::Unknown;

    let evidence_text = input
        .evidence_text
        .as_deref()
        .ok_or_else(|| McpError::InvalidParameters("evidence_text required".to_string()))?;
    let evidence = evidence_span_for_manual_quote(input.evidence_source.as_deref(), evidence_text)?;

    let key_material = format!(
        "manual_rel:{}:{}:{}:{}:{}",
        input.from_id,
        input.rel_type,
        input.to_id,
        input.evidence_source.as_deref().unwrap_or(""),
        evidence.quote_hash
    );

    let assertion = Assertion {
        base,
        subject_text: input.from_id.clone(),
        predicate_raw: input.rel_type.clone(),
        predicate_id: None,
        object_text: Some(input.to_id.clone()),
        object_literal: None,
        evidence,
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: sha256_hex_prefixed(&key_material),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: Some("manual".to_string()),
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(make_entity_link(&input.from_id)),
        object_entity: Some(make_entity_link(&input.to_id)),
        confidence: input.weight.unwrap_or(0.8) as f32,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };

    repo.create(assertion)
        .await
        .map_err(|e| McpError::ExecutionError(e.to_string()))
}

/// Helper to create a Rel from input.
#[allow(clippy::cast_possible_truncation)]
fn create_rel_from_input(input: &RelateEntitiesInput, evidence: Vec<EvidenceRef>) -> Rel {
    let id = RecordId::new_v4(TableName::Rel);
    let mut base = BaseFields::new(id, input.space_id.clone(), "mcp_tool".to_string());
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Unknown;

    Rel {
        base,
        subject: make_entity_link(&input.from_id),
        object: make_entity_link(&input.to_id),
        rel_type: input.rel_type.clone(),
        status: ClaimStatus::Candidate,
        confidence: input.weight.unwrap_or(0.8) as f32,
        validity_window: None,
        evidence,
        temporal_audit: None,
    }
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `relate_entities` tool.
pub struct RelateEntitiesHandler {
    repo: RelRepository,
}

impl RelateEntitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RelateEntitiesHandler {
    fn name(&self) -> &'static str {
        "relate_entities"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a relationship between two entities.\n\n",
            "Example:\n",
            r#"{ "from_id": "entity:550e8400-e29b-41d4-a716-446655440000", "to_id": "entity:550e8400-e29b-41d4-a716-446655440001", "rel_type": "works_at", "weight": 0.9 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["from_id", "to_id", "rel_type"],
            "properties": {
                "from_id": {
                    "type": "string",
                    "description": "Source entity ID"
                },
                "to_id": {
                    "type": "string",
                    "description": "Target entity ID"
                },
                "rel_type": {
                    "type": "string",
                    "description": "Relationship type (e.g., 'knows', 'works_at')"
                },
                "weight": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "Relationship strength"
                },
                "valid_from": {
                    "type": "string",
                    "format": "date-time",
                    "description": "When the relationship became valid"
                },
                "valid_to": {
                    "type": "string",
                    "format": "date-time",
                    "description": "When the relationship ended"
                },
                "description": {
                    "type": "string",
                    "description": "Description of the relationship"
                },
                "properties": {
                    "type": "object",
                    "required": [],
                    "description": "Additional properties"
                },
                "evidence_text": {
                    "type": "string",
                    "description": "Optional evidence quote supporting the relationship"
                },
                "evidence_source": {
                    "type": "string",
                    "description": "Optional evidence source record ID (e.g., source:..., doc:...)"
                },
                "space_id": {
                    "type": "string",
                    "default": "default",
                    "description": "Space/tenant ID (defaults to your current space)"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let mut input: RelateEntitiesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if input.space_id == "default" {
            input.space_id.clone_from(&ctx.principal.space_id);
        }
        if input.space_id != ctx.principal.space_id {
            let response: ToolResponse<()> =
                ToolResponse::error("Access denied: access to space denied");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        input.rel_type =
            canonicalize_and_register_rel_type(&ctx.storage, &input.space_id, &input.rel_type)
                .await?;

        let assertion_repo = AssertionRepository::new(ctx.storage.clone());
        let evidence = if input
            .evidence_text
            .as_deref()
            .is_some_and(|t| !t.trim().is_empty())
        {
            let assertion =
                create_evidence_assertion(&assertion_repo, &input.space_id, &input).await?;
            vec![EvidenceRef {
                assertion: Link::new(assertion.base.id.clone()),
                span: assertion.evidence.clone(),
                time_basis: TimeBasis::ObservedAt,
                time_value: assertion.observed_at,
            }]
        } else {
            Vec::new()
        };

        let rel = create_rel_from_input(&input, evidence);

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &rel, "write:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &rel, "read:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.create(rel).await {
            Ok(created) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &created, "read:relationship")
                {
                    let response: ToolResponse<Rel> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(created);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `upsert_relationship` tool.
pub struct UpsertRelationshipHandler {
    repo: RelRepository,
}

impl UpsertRelationshipHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UpsertRelationshipHandler {
    fn name(&self) -> &'static str {
        "upsert_relationship"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create or update a relationship based on from/to/type.\n\n",
            "Example:\n",
            r#"{ "from_id": "entity:550e8400-e29b-41d4-a716-446655440000", "to_id": "entity:550e8400-e29b-41d4-a716-446655440001", "rel_type": "works_at", "fields": { "description": "Employment relationship" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["from_id", "to_id", "rel_type"],
            "properties": {
                "from_id": {
                    "type": "string",
                    "description": "Source entity ID"
                },
                "to_id": {
                    "type": "string",
                    "description": "Target entity ID"
                },
                "rel_type": {
                    "type": "string",
                    "description": "Relationship type"
                },
                "fields": {
                    "type": "object",
                    "required": [],
                    "description": "Fields to set/update"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let mut input: UpsertRelationshipInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid_from = parse_id(&input.from_id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let rid_to = parse_id(&input.to_id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        input.rel_type =
            canonicalize_and_register_rel_type(&ctx.storage, &space_id, &input.rel_type).await?;

        // Try to find existing relationship using find_between
        let existing = self
            .repo
            .find_between(&space_id, &rid_from, &rid_to)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // Filter by rel_type
        let matching_rel = existing.into_iter().find(|r| r.rel_type == input.rel_type);

        if let Some(rel) = matching_rel {
            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &rel, "write:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            if let Err(reason) =
                ctx.authz
                    .authorize_record_read(&ctx.principal, &rel, "read:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }

            // Update existing
            let mut rel_value =
                serde_json::to_value(&rel).map_err(|e| McpError::ExecutionError(e.to_string()))?;

            if let (Value::Object(ref mut map), Some(Value::Object(fields))) =
                (&mut rel_value, input.fields)
            {
                for (k, v) in fields {
                    map.insert(k, v);
                }
            }

            let updated: Rel = serde_json::from_value(rel_value)
                .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
            let mut updated = updated;
            updated.rel_type =
                canonicalize_and_register_rel_type(&ctx.storage, &space_id, &updated.rel_type)
                    .await?;

            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &updated, "write:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            if let Err(reason) =
                ctx.authz
                    .authorize_record_read(&ctx.principal, &updated, "read:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }

            let rid = rel.base.id.clone();
            match self.repo.update(&space_id, &rid, updated).await {
                Ok(rel) => {
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_read(&ctx.principal, &rel, "read:relationship")
                    {
                        let response: ToolResponse<Rel> =
                            ToolResponse::error(format!("Access denied: {reason}"));
                        return serde_json::to_value(response)
                            .map_err(|e| McpError::ExecutionError(e.to_string()));
                    }
                    let response = ToolResponse::success(rel);
                    serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()))
                }
                Err(e) => Err(McpError::ExecutionError(e.to_string())),
            }
        } else {
            // Create new
            let rel_input = RelateEntitiesInput {
                from_id: input.from_id,
                to_id: input.to_id,
                rel_type: input.rel_type,
                weight: None,
                valid_from: None,
                valid_to: None,
                description: None,
                properties: input.fields,
                evidence_text: None,
                evidence_source: None,
                space_id: ctx.principal.space_id.clone(),
            };
            let rel = create_rel_from_input(&rel_input, Vec::new());

            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &rel, "write:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            if let Err(reason) =
                ctx.authz
                    .authorize_record_read(&ctx.principal, &rel, "read:relationship")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }

            match self.repo.create(rel).await {
                Ok(created) => {
                    if let Err(reason) = ctx.authz.authorize_record_read(
                        &ctx.principal,
                        &created,
                        "read:relationship",
                    ) {
                        let response: ToolResponse<Rel> =
                            ToolResponse::error(format!("Access denied: {reason}"));
                        return serde_json::to_value(response)
                            .map_err(|e| McpError::ExecutionError(e.to_string()));
                    }
                    let response = ToolResponse::success(created);
                    serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()))
                }
                Err(e) => Err(McpError::ExecutionError(e.to_string())),
            }
        }
    }
}

/// Handler for `update_relationship` tool.
pub struct UpdateRelationshipHandler {
    repo: RelRepository,
}

impl UpdateRelationshipHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UpdateRelationshipHandler {
    fn name(&self) -> &'static str {
        "update_relationship"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Update a relationship's fields.\n\n",
            "Example:\n",
            r#"{ "id": "rel:550e8400-e29b-41d4-a716-446655440000", "fields": { "description": "Updated description" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "fields"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Relationship ID"
                },
                "fields": {
                    "type": "object",
                    "required": [],
                    "description": "Fields to update"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: UpdateRelationshipInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Rel)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        // Get existing relationship
        let existing = match self.repo.get(&space_id, &rid).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Relationship not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &existing, "write:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &existing, "read:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Merge fields
        let mut rel_value =
            serde_json::to_value(&existing).map_err(|e| McpError::ExecutionError(e.to_string()))?;

        if let Value::Object(ref mut map) = rel_value {
            if let Value::Object(fields) = input.fields {
                for (k, v) in fields {
                    map.insert(k, v);
                }
            }
        }

        let updated: Rel = serde_json::from_value(rel_value)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let mut updated = updated;
        updated.rel_type =
            canonicalize_and_register_rel_type(&ctx.storage, &space_id, &updated.rel_type).await?;

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &updated, "write:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &updated, "read:relationship")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.update(&space_id, &rid, updated).await {
            Ok(rel) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &rel, "read:relationship")
                {
                    let response: ToolResponse<Rel> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(rel);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `delete_relationship` tool.
pub struct DeleteRelationshipHandler {
    repo: RelRepository,
}

impl DeleteRelationshipHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for DeleteRelationshipHandler {
    fn name(&self) -> &'static str {
        "delete_relationship"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Soft delete a relationship.\n\n",
            "Example:\n",
            r#"{ "id": "rel:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Relationship ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RelIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Rel)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        // Check if relationship exists
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(rel)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &rel, "write:relationship")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                self.repo
                    .delete(&space_id, &rid)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;

                let response = ToolResponse::success(json!({ "deleted": true, "id": input.id }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Relationship not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `restore_relationship` tool.
pub struct RestoreRelationshipHandler {
    repo: RelRepository,
}

impl RestoreRelationshipHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RestoreRelationshipHandler {
    fn name(&self) -> &'static str {
        "restore_relationship"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Restore a soft-deleted relationship.\n\n",
            "Example:\n",
            r#"{ "id": "rel:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Relationship ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RelIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Rel)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(rel)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &rel, "write:relationship")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &rel, "read:relationship")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                let restored = self
                    .repo
                    .restore(&space_id, &rid)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;

                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &restored, "read:relationship")
                {
                    let response: ToolResponse<Rel> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                let response = ToolResponse::success(restored);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Relationship not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `batch_relationships` tool.
pub struct BatchRelationshipsHandler {
    repo: RelRepository,
}

impl BatchRelationshipsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for BatchRelationshipsHandler {
    fn name(&self) -> &'static str {
        "batch_relationships"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Batch create relationships.\n\n",
            "Example:\n",
            r#"{ "items": [ { "from_id": "entity:550e8400-e29b-41d4-a716-446655440000", "to_id": "entity:550e8400-e29b-41d4-a716-446655440001", "rel_type": "works_at", "weight": 0.9 } ] }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["items"],
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["from_id", "to_id", "rel_type"],
                        "properties": {
                            "from_id": { "type": "string" },
                            "to_id": { "type": "string" },
                            "rel_type": { "type": "string" },
                            "space_id": {
                                "type": "string",
                                "default": "default",
                                "description": "Space ID (defaults to \"default\")"
                            },
                            "weight": { "type": "number" },
                            "description": { "type": "string" }
                        }
                    },
                    "description": "Relationships to create"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: BatchRelationshipsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        for (index, mut item) in input.items.into_iter().enumerate() {
            if item.space_id == "default" {
                item.space_id.clone_from(&ctx.principal.space_id);
            }
            if item.space_id != ctx.principal.space_id {
                failed.push(BatchError {
                    index,
                    error: "Access denied: access to space denied".to_string(),
                });
                continue;
            }
            match canonicalize_and_register_rel_type(&ctx.storage, &item.space_id, &item.rel_type)
                .await
            {
                Ok(canonical) => item.rel_type = canonical,
                Err(e) => {
                    failed.push(BatchError {
                        index,
                        error: e.to_string(),
                    });
                    continue;
                }
            }
            let rel = create_rel_from_input(&item, Vec::new());

            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &rel, "write:relationship")
            {
                failed.push(BatchError {
                    index,
                    error: format!("Access denied: {reason}"),
                });
                continue;
            }
            if let Err(reason) =
                ctx.authz
                    .authorize_record_read(&ctx.principal, &rel, "read:relationship")
            {
                failed.push(BatchError {
                    index,
                    error: format!("Access denied: {reason}"),
                });
                continue;
            }

            match self.repo.create(rel).await {
                Ok(created) => {
                    if let Err(reason) = ctx.authz.authorize_record_read(
                        &ctx.principal,
                        &created,
                        "read:relationship",
                    ) {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                    } else {
                        succeeded.push(created);
                    }
                }
                Err(e) => failed.push(BatchError {
                    index,
                    error: e.to_string(),
                }),
            }
        }

        let output = BatchRelOutput { succeeded, failed };
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

    #[test]
    fn test_relate_entities_input_defaults() {
        let json = r#"{"from_id": "a", "to_id": "b", "rel_type": "knows"}"#;
        let input: RelateEntitiesInput = serde_json::from_str(json).unwrap();
        assert!(input.weight.is_none());
        assert!(input.valid_from.is_none());
        assert_eq!(input.space_id, "default");
    }

    #[test]
    fn test_make_entity_link() {
        let link = make_entity_link("entity:550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(link.id().table(), TableName::Entity);
    }

    #[tokio::test]
    async fn test_canonicalize_and_register_rel_type_uses_active_alias() {
        use elephant_storage::PredicateAliasRepository;
        use elephant_types::records::predicate::{AliasScope, AliasStatus, AliasType};

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query(
                r"
                DEFINE TABLE predicate_registry SCHEMALESS;
                DEFINE TABLE predicate_alias SCHEMALESS;
                ",
            )
            .await
            .expect("schema");

        let alias_repo = PredicateAliasRepository::new(storage.clone());
        alias_repo
            .create(elephant_types::records::predicate::PredicateAlias {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::PredicateAlias),
                    "test_space".to_string(),
                    "relationship_test".to_string(),
                ),
                alias: "employed by".to_string(),
                alias_type: AliasType::RawString,
                source_plugin: None,
                confidence: 0.99,
                status: AliasStatus::Active,
                scope: AliasScope::Global,
                predicate_id: "works_at".to_string(),
                confidence_version: 1,
                usage_count: 0,
            })
            .await
            .expect("create alias");

        let canonical = canonicalize_and_register_rel_type(&storage, "test_space", " employed by ")
            .await
            .expect("canonical rel type");

        assert_eq!(canonical, "works_at");
        assert!(PredicateRegistryRepository::new(storage.clone())
            .find_by_predicate_id("test_space", "works_at")
            .await
            .expect("find predicate")
            .is_some());
        assert!(PredicateRegistryRepository::new(storage)
            .find_by_predicate_id("test_space", "employed by")
            .await
            .expect("find raw predicate")
            .is_none());
    }
}
