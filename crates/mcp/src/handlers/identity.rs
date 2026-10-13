#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Identity and entity context MCP tool handlers.
//!
//! Implements:
//! - `get_entity_context_bundle`
//! - `propose_identity_candidate`
//! - `list_identity_candidates`
//! - `merge_entities`

use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_extensions::load_runtime_reference_registry_from_env;
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    parse_id, AssertionRepository, AttributeRepository, EntityRepository, EventRepository,
    FreshnessRepository, IdentityCandidateRepository, Repository, StorageClient,
    TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::{
    Assertion, Attribute, Entity, Event, Freshness, IdentityCandidate, Rel, TruthSlot,
};
use elephant_types::RecordId;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `get_entity_context_bundle` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetEntityContextBundleInput {
    /// Entity ID.
    pub id: String,
    /// Time window in days.
    #[serde(default = "default_window")]
    pub window: i64,
    /// Maximum items per section.
    #[serde(default = "default_section_limit")]
    pub limit: usize,
}

const fn default_window() -> i64 {
    30
}

const fn default_section_limit() -> usize {
    10
}

const fn default_limit() -> usize {
    50
}

/// Output for `get_entity_context_bundle` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityContextBundle {
    /// The entity.
    pub entity: Entity,
    /// Outgoing relationships.
    pub outgoing_relations: Vec<Rel>,
    /// Incoming relationships.
    pub incoming_relations: Vec<Rel>,
    /// Entity-to-literal graph claims.
    #[serde(default)]
    pub attributes: Vec<Attribute>,
    /// Current truth slots for this entity.
    #[serde(default)]
    pub truth_slots: Vec<TruthSlot>,
    /// First-class domain events involving this entity.
    #[serde(default)]
    pub events: Vec<Event>,
    /// Recent raw assertions involving this entity.
    #[serde(default)]
    pub assertions: Vec<Assertion>,
    /// Freshness metadata for returned records.
    #[serde(default)]
    pub freshness: Vec<Freshness>,
    /// Summary statistics.
    pub stats: ContextStats,
}

/// Context statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextStats {
    /// Total relationships.
    pub total_relations: usize,
    /// Total attributes after authorization and window filtering.
    pub total_attributes: usize,
    /// Total truth slots after authorization.
    pub total_truth_slots: usize,
    /// Total events after authorization and window filtering.
    pub total_events: usize,
    /// Total assertions after authorization and window filtering.
    pub total_assertions: usize,
    /// Number of returned freshness records.
    pub total_freshness_records: usize,
    /// Effective historical window in days.
    pub window_days: i64,
}

/// Input for `propose_identity_candidate` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeIdentityCandidateInput {
    /// Candidate details.
    pub candidate: IdentityCandidateProposal,
}

/// Identity candidate proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityCandidateProposal {
    /// Entity A ID.
    pub entity_a: String,
    /// Entity B ID.
    pub entity_b: String,
    /// Confidence score.
    pub confidence: f64,
    /// Evidence/reasoning.
    #[serde(default)]
    pub evidence: Option<String>,
    /// Signal type (how the match was detected).
    #[serde(default)]
    pub signal_type: Option<String>,
}

/// Input for `list_identity_candidates` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListIdentityCandidatesInput {
    /// Filters.
    #[serde(default)]
    pub filters: Option<IdentityCandidateFilters>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Identity candidate filters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IdentityCandidateFilters {
    /// Filter by status.
    #[serde(default)]
    pub status: Option<String>,
    /// Minimum confidence.
    #[serde(default)]
    pub min_confidence: Option<f64>,
    /// Entity ID filter.
    #[serde(default)]
    pub entity_id: Option<String>,
}

/// Input for `merge_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEntitiesInput {
    /// Entity A ID.
    pub entity_a: String,
    /// Entity B ID.
    pub entity_b: String,
    /// Merge strategy.
    #[serde(default = "default_strategy")]
    pub strategy: String,
}

fn default_strategy() -> String {
    "prefer_newer".to_string()
}

/// Output for `merge_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEntitiesOutput {
    /// Survivor entity.
    pub survivor: Entity,
    /// Merged (tombstone) entity ID.
    pub merged_id: String,
    /// Number of relationships updated.
    pub relations_updated: usize,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `get_entity_context_bundle` tool.
pub struct GetEntityContextBundleHandler {
    entities: EntityRepository,
    attributes: AttributeRepository,
    events: EventRepository,
    truth_slots: TruthSlotRepository,
    assertions: AssertionRepository,
    freshness: FreshnessRepository,
}

impl GetEntityContextBundleHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entities: EntityRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            events: EventRepository::new(storage.clone()),
            truth_slots: TruthSlotRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage.clone()),
            freshness: FreshnessRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetEntityContextBundleHandler {
    fn name(&self) -> &'static str {
        "get_entity_context_bundle"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get comprehensive context bundle for an entity.\n\n",
            "Example:\n",
            r#"{ "id": "entity:550e8400-e29b-41d4-a716-446655440000", "window": 30, "limit": 10 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Entity ID"
                },
                "window": {
                    "type": "integer",
                    "default": 30,
                    "description": "Time window in days for context"
                },
                "limit": {
                    "type": "integer",
                    "default": 10,
                    "description": "Maximum items per section"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetEntityContextBundleInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if !(0..=36_500).contains(&input.window) {
            return Err(McpError::InvalidParameters(
                "window must be between 0 and 36500 days".to_string(),
            ));
        }
        if !(1..=100).contains(&input.limit) {
            return Err(McpError::InvalidParameters(
                "limit must be between 1 and 100".to_string(),
            ));
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        // Get entity with relations
        let mut entity_with_rels = match self.entities.get_with_relations(&space_id, &rid).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &entity_with_rels.entity, "read:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        entity_with_rels.entity =
            crate::current_read::require(ctx, entity_with_rels.entity, "read:entity").await?;
        let cutoff = Utc::now() - Duration::days(input.window);
        let mut outgoing_relations = crate::current_read::filter(
            ctx,
            entity_with_rels.outgoing_relations,
            "read:relationship",
        )
        .await?;
        let mut incoming_relations = crate::current_read::filter(
            ctx,
            entity_with_rels.incoming_relations,
            "read:relationship",
        )
        .await?;
        outgoing_relations.retain(|relation| {
            relation
                .validity_window
                .as_ref()
                .and_then(|window| window.end)
                .is_none_or(|end| end >= cutoff)
        });
        incoming_relations.retain(|relation| {
            relation
                .validity_window
                .as_ref()
                .and_then(|window| window.end)
                .is_none_or(|end| end >= cutoff)
        });
        outgoing_relations.truncate(input.limit);
        incoming_relations.truncate(input.limit);

        let mut attributes = self
            .attributes
            .find_by_subject_entity(&space_id, &rid)
            .await
            .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        attributes = crate::current_read::filter(ctx, attributes, "read:relationship").await?;
        attributes.retain(|attribute| {
            attribute
                .validity_window
                .as_ref()
                .and_then(|window| window.end)
                .is_none_or(|end| end >= cutoff)
        });
        attributes.truncate(input.limit);

        let mut truth_slots = self
            .truth_slots
            .find_by_subject_entity(&space_id, &rid.to_string())
            .await
            .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        truth_slots = crate::current_read::filter(ctx, truth_slots, "read:relationship").await?;
        truth_slots.truncate(input.limit);

        let mut events = self
            .events
            .find_by_subject_entity(&space_id, &rid, input.limit.saturating_mul(2))
            .await
            .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        events = crate::current_read::filter(ctx, events, "read:event").await?;
        events.retain(|event| event.time_start.unwrap_or(event.base.created_at) >= cutoff);
        events.truncate(input.limit);

        let mut assertions = self
            .assertions
            .find_by_resolved_entity(&space_id, &rid, input.limit.saturating_mul(2))
            .await
            .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        assertions = crate::current_read::filter(ctx, assertions, "read:assertion").await?;
        assertions.retain(|assertion| {
            assertion.source_event_time.unwrap_or(assertion.observed_at) >= cutoff
        });
        assertions.truncate(input.limit);

        let freshness_targets = outgoing_relations
            .iter()
            .map(|record| record.base.id.to_string())
            .chain(
                incoming_relations
                    .iter()
                    .map(|record| record.base.id.to_string()),
            )
            .chain(attributes.iter().map(|record| record.base.id.to_string()))
            .chain(truth_slots.iter().map(|record| record.base.id.to_string()))
            .chain(events.iter().map(|record| record.base.id.to_string()))
            .chain(std::iter::once(rid.to_string()))
            .collect::<std::collections::BTreeSet<_>>();
        let mut freshness = Vec::new();
        for target in freshness_targets {
            if let Some(record) = self
                .freshness
                .find_by_target(&space_id, &target)
                .await
                .map_err(|error| McpError::ExecutionError(error.to_string()))?
            {
                if ctx
                    .authz
                    .authorize_record_read(&ctx.principal, &record, "read:relationship")
                    .is_ok()
                {
                    freshness.push(record);
                }
            }
        }

        let stats = ContextStats {
            total_relations: outgoing_relations.len() + incoming_relations.len(),
            total_attributes: attributes.len(),
            total_truth_slots: truth_slots.len(),
            total_events: events.len(),
            total_assertions: assertions.len(),
            total_freshness_records: freshness.len(),
            window_days: input.window,
        };

        let bundle = EntityContextBundle {
            entity: entity_with_rels.entity,
            outgoing_relations,
            incoming_relations,
            attributes,
            truth_slots,
            events,
            assertions,
            freshness,
            stats,
        };

        let response = ToolResponse::success(bundle);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `propose_identity_candidate` tool.
pub struct ProposeIdentityCandidateHandler {}

impl ProposeIdentityCandidateHandler {
    /// Creates a new handler. Request context supplies the canonical client.
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self {}
    }
}

#[async_trait]
impl ToolHandler for ProposeIdentityCandidateHandler {
    fn name(&self) -> &'static str {
        "propose_identity_candidate"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Propose two entities as potential duplicates.\n\n",
            "Example:\n",
            r#"{ "candidate": { "entity_a": "entity:550e8400-e29b-41d4-a716-446655440000", "entity_b": "entity:550e8400-e29b-41d4-a716-446655440001", "confidence": 0.92, "evidence": "Same name + email", "signal_type": "heuristic" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["candidate"],
            "properties": {
                "candidate": {
                    "type": "object",
                    "required": ["entity_a", "entity_b", "confidence"],
                    "properties": {
                        "entity_a": {
                            "type": "string",
                            "description": "First entity ID"
                        },
                        "entity_b": {
                            "type": "string",
                            "description": "Second entity ID"
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0,
                            "description": "Confidence in match"
                        },
                        "evidence": {
                            "type": "string",
                            "description": "Evidence or reasoning"
                        },
                        "signal_type": {
                            "type": "string",
                            "description": "How the match was detected"
                        }
                    }
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the identity mutation plan through candidate authorization, commit and response construction."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        use elephant_types::records::IdentityCandidateStatus;
        use elephant_types::Link;
        use std::str::FromStr;

        let params = coerce_params(params, &self.input_schema());
        let input: ProposeIdentityCandidateInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        // Create identity candidate record
        let id = RecordId::new_v4(TableName::IdentityCandidate);
        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let mut base = BaseFields::new(id, space_id.clone(), "mcp_tool".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        // Parse entity IDs
        let first_id_string = if input.candidate.entity_a.starts_with("entity:") {
            input.candidate.entity_a.clone()
        } else {
            format!("entity:{}", input.candidate.entity_a)
        };
        let rid_a = RecordId::from_str(&first_id_string)
            .map_err(|e| McpError::InvalidParameters(format!("Invalid entity_a ID: {e}")))?;

        let second_id_string = if input.candidate.entity_b.starts_with("entity:") {
            input.candidate.entity_b.clone()
        } else {
            format!("entity:{}", input.candidate.entity_b)
        };
        let rid_b = RecordId::from_str(&second_id_string)
            .map_err(|e| McpError::InvalidParameters(format!("Invalid entity_b ID: {e}")))?;

        for (index, entity_id) in [&rid_a, &rid_b].into_iter().enumerate() {
            if entity_id.table() != TableName::Entity {
                return Err(McpError::InvalidParameters(
                    "identity target must be an entity".into(),
                ));
            }
            let entity = EntityRepository::new(ctx.storage.clone())
                .get(&space_id, entity_id)
                .await?
                .ok_or_else(|| {
                    McpError::InvalidParameters(
                        "identity target does not exist in selected space".into(),
                    )
                })?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &entity, "read:entity")
                .map_err(McpError::ExecutionError)?;
            if index == 0 {
                base.security = entity.base.security.clone();
            }
            inherit_public_envelope(&mut base, &entity.base)?;
            mutation.depend_on(CanonicalRecord::Entity(Box::new(entity)));
        }
        let candidate = IdentityCandidate {
            base,
            entity_a: Link::new(rid_a),
            entity_b: Link::new(rid_b),
            signals: json!({
                "evidence": input.candidate.evidence,
                "signal_type": input.candidate.signal_type,
            }),
            confidence: input.candidate.confidence,
            status: IdentityCandidateStatus::Pending,
            proposed_by: "mcp_tool".to_string(),
            decided_by: None,
            decided_at: None,
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &candidate, "write:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &candidate, "read:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let actor = ctx
            .verified_caller()
            .map_or("trusted:mcp", |caller| caller.claims().sub.as_str());
        let created = mutation
            .create(candidate, actor)
            .await
            .map_err(McpError::StorageError)?;

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &created, "read:entity")
        {
            let response: ToolResponse<IdentityCandidate> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let response = ToolResponse::success(created);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `list_identity_candidates` tool.
pub struct ListIdentityCandidatesHandler {
    identity: IdentityCandidateRepository,
}

impl ListIdentityCandidatesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            identity: IdentityCandidateRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListIdentityCandidatesHandler {
    fn name(&self) -> &'static str {
        "list_identity_candidates"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List pending entity merge candidates.\n\n",
            "Example:\n",
            r#"{ "filters": { "status": "pending", "min_confidence": 0.85 }, "limit": 50 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "filters": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["pending", "approved", "rejected"]
                        },
                        "min_confidence": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0
                        },
                        "entity_id": {
                            "type": "string",
                            "description": "Filter by entity involvement"
                        }
                    }
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
        let input: ListIdentityCandidatesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);
        if let Err(reason) = ctx.authz.evaluate(&ctx.principal, &record, "read:entity") {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let candidates = self
            .identity
            .get_pending(&space_id, input.limit)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // Apply filters
        let mut items = ctx
            .authz
            .filter_records(&ctx.principal, candidates, "read:entity");
        if let Some(filters) = &input.filters {
            if let Some(min_conf) = filters.min_confidence {
                items.retain(|c| c.confidence >= min_conf);
            }
        }
        let items: Vec<IdentityCandidate> = items.into_iter().take(input.limit).collect();

        let response = ToolResponse::success(items);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `merge_entities` tool.
pub struct MergeEntitiesHandler {
    storage: StorageClient,
}

impl MergeEntitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ToolHandler for MergeEntitiesHandler {
    fn name(&self) -> &'static str {
        "merge_entities"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Merge two entities into one.\n\n",
            "Example:\n",
            r#"{ "entity_a": "entity:550e8400-e29b-41d4-a716-446655440000", "entity_b": "entity:550e8400-e29b-41d4-a716-446655440001", "strategy": "prefer_newer" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["entity_a", "entity_b"],
            "properties": {
                "entity_a": {
                    "type": "string",
                    "description": "First entity ID"
                },
                "entity_b": {
                    "type": "string",
                    "description": "Second entity ID"
                },
                "strategy": {
                    "type": "string",
                    "enum": ["prefer_newer", "prefer_older", "prefer_a", "prefer_b"],
                    "default": "prefer_newer",
                    "description": "Merge strategy for conflicting fields"
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the prepared merge through all dependent-record authorizations and commit handling."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        use elephant_storage::entity_merge_repository::MergeSelection;
        let params = coerce_params(params, &self.input_schema());
        let input: MergeEntitiesInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if ctx.principal.space_id.trim().is_empty() {
            return Err(McpError::InvalidParameters(
                "selected space is required".into(),
            ));
        }
        let a = parse_id(&input.entity_a, TableName::Entity)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let b = parse_id(&input.entity_b, TableName::Entity)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let selection = match input.strategy.as_str() {
            "prefer_a" => MergeSelection::KeepA,
            "prefer_b" => MergeSelection::KeepB,
            "prefer_older" => MergeSelection::PreferOlder,
            "prefer_newer" => MergeSelection::PreferNewer,
            _ => return Err(McpError::InvalidParameters("unknown merge strategy".into())),
        };
        let registry = load_runtime_reference_registry_from_env()
            .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        let prepared = elephant_identity::prepare_entity_merge(
            &self.storage,
            &ctx.principal.space_id,
            &a,
            &b,
            selection,
            "mcp.merge_entities",
            &registry,
        )
        .await
        .map_err(|error| match error {
            elephant_identity::IdentityError::StorageError(error) => {
                super::source_intake::storage_error(error)
            }
            other => McpError::ExecutionError(other.to_string()),
        })?;
        for envelope in prepared
            .authorizations()
            .map_err(super::source_intake::storage_error)?
        {
            let authorization = ctx
                .authz
                .authorize_record_read(&ctx.principal, &envelope, envelope.read_scope)
                .and_then(|()| {
                    ctx.authz.authorize_record_write(
                        &ctx.principal,
                        &envelope,
                        envelope.write_scope,
                    )
                });
            if let Err(reason) = authorization {
                return serde_json::to_value(ToolResponse::<()>::error(format!(
                    "Access denied: {reason}"
                )))
                .map_err(|error| McpError::ExecutionError(error.to_string()));
            }
        }
        let outcome = prepared
            .commit()
            .await
            .map_err(super::source_intake::storage_error)?;
        let merged_id = outcome
            .merged
            .first()
            .ok_or_else(|| McpError::ExecutionError("merge has no redirected input".into()))?
            .to_string();
        let output = MergeEntitiesOutput {
            survivor: outcome.survivor,
            merged_id,
            relations_updated: outcome.relationships_repointed,
        };
        serde_json::to_value(ToolResponse::success(output))
            .map_err(|error| McpError::ExecutionError(error.to_string()))
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
    use elephant_storage::RelRepository;
    use std::sync::OnceLock;

    use elephant_policy::{Clearance, Principal};
    use elephant_storage::{EntityRepository, Repository, StorageClient};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::Entity;
    use elephant_types::RecordId;
    use tokio::sync::Mutex;

    async fn register_merge_space(storage: &StorageClient, space: &str) {
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: elephant_types::records::base::BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    space.into(),
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
            .unwrap();
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn create_test_entity(space_id: &str, name: &str) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Entity::new(base, EntityType::new("person"), name.to_string())
    }

    fn test_principal(space_id: &str) -> Principal {
        let mut scopes = std::collections::HashSet::new();
        scopes.insert("*".to_string());
        Principal {
            space_id: space_id.to_string(),
            scopes,
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: i32::MAX,
                labels: std::collections::HashSet::new(),
                handling_allow: std::collections::HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: std::collections::HashSet::new(),
        }
    }

    #[test]
    fn test_get_entity_context_bundle_input_defaults() {
        let json = r#"{"id": "entity:123"}"#;
        let input: GetEntityContextBundleInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.window, 30);
        assert_eq!(input.limit, 10);
    }

    #[test]
    fn test_merge_entities_input_defaults() {
        let json = r#"{"entity_a": "a", "entity_b": "b"}"#;
        let input: MergeEntitiesInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.strategy, "prefer_newer");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_merge_entities_rewrites_extension_rows_in_embedded_mode() {
        let _guard = env_lock().lock().await;
        let storage = StorageClient::new_embedded().await.expect("storage");
        register_merge_space(&storage, "default").await;
        let entity_repo = EntityRepository::new(storage.clone());

        let entity_a = entity_repo
            .create(create_test_entity("default", "Entity A"))
            .await
            .expect("create a");
        let entity_b = entity_repo
            .create(create_test_entity("default", "Entity B"))
            .await
            .expect("create b");

        let manifest_dir =
            std::env::temp_dir().join(format!("elephant-mcp-extension-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&manifest_dir).expect("create manifest dir");
        std::fs::write(
            manifest_dir.join("sampleapp.json"),
            r#"{
                "name": "sampleapp",
                "refs": [
                    {
                        "table": "sampleapp_todo",
                        "field_path": "owner",
                        "cardinality": "one",
                        "on_merge": "rewrite"
                    },
                    {
                        "table": "sampleapp_todo",
                        "field_path": "secondary",
                        "cardinality": "many",
                        "on_merge": "rewrite"
                    }
                ]
            }"#,
        )
        .expect("write manifest");

        std::env::set_var("ELEPHANT_EXTENSIONS", "sampleapp");
        std::env::set_var("ELEPHANT_EXTENSION_MANIFEST_DIR", &manifest_dir);

        let store = storage.embedded_store().expect("embedded store");
        let row = serde_json::json!({
            "id": "sampleapp_todo:todo-1",
            "space_id": "default",
            "security": entity_b.base.security,
            "subjects": [],
            "subjects_state": "complete",
            "owner": entity_b.base.id.to_string(),
            "secondary": [
                entity_b.base.id.to_string(),
                entity_a.base.id.to_string()
            ]
        });
        store
            .mutate(|mutation| {
                mutation.upsert_extension_row("sampleapp_todo", row)?;
                Ok(())
            })
            .await
            .expect("insert extension row");

        let handler = MergeEntitiesHandler::new(storage.clone());
        let ctx = ToolContext::new(storage, test_principal("default"));
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "entity_a": entity_a.base.id.to_string(),
                    "entity_b": entity_b.base.id.to_string(),
                    "strategy": "prefer_a"
                }),
            )
            .await
            .expect("merge response");
        let decoded: ToolResponse<MergeEntitiesOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(decoded.ok, "unexpected merge response: {decoded:?}");

        let rows = store
            .mutate(|mutation| {
                mutation
                    .scan_extension_rows("sampleapp_todo")
                    .map_err(Into::into)
            })
            .await
            .expect("scan extension rows");
        let row = &rows[0].1;
        assert_eq!(
            row["owner"],
            serde_json::json!(entity_a.base.id.to_string())
        );
        assert_eq!(
            row["secondary"],
            serde_json::json!([entity_a.base.id.to_string()])
        );

        std::env::remove_var("ELEPHANT_EXTENSIONS");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");
        let _ = std::fs::remove_dir_all(manifest_dir);
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn merge_denied_dependent_record_leaves_all_inputs_unchanged() {
        let _guard = env_lock().lock().await;
        let storage = StorageClient::new_embedded().await.unwrap();
        register_merge_space(&storage, "default").await;
        let entities = EntityRepository::new(storage.clone());
        let a = entities
            .create(create_test_entity("default", "A"))
            .await
            .unwrap();
        let b = entities
            .create(create_test_entity("default", "B"))
            .await
            .unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "default".into(),
            "test".into(),
        );
        base.security.level = 5;
        let rel = Rel {
            base,
            subject: elephant_types::Link::new(b.base.id.clone()),
            object: elephant_types::Link::new(a.base.id.clone()),
            rel_type: "knows".into(),
            status: elephant_types::records::rel::ClaimStatus::Approved,
            confidence: 1.0,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        let rel = RelRepository::new(storage.clone())
            .create(rel)
            .await
            .unwrap();
        let mut principal = test_principal("default");
        principal.clearances[0].max_level = 0;
        let context = ToolContext::new(storage.clone(), principal);
        let response = MergeEntitiesHandler::new(storage.clone())
            .execute(
                &context,
                json!({
                    "entity_a": a.base.id, "entity_b":b.base.id, "strategy":"prefer_a"
                }),
            )
            .await
            .unwrap();
        let response: ToolResponse<()> = serde_json::from_value(response).unwrap();
        assert!(!response.ok);
        assert_eq!(
            entities.get("default", &a.base.id).await.unwrap().unwrap(),
            a
        );
        assert_eq!(
            entities.get("default", &b.base.id).await.unwrap().unwrap(),
            b
        );
        assert_eq!(
            RelRepository::new(storage.clone())
                .get("default", &rel.base.id)
                .await
                .unwrap()
                .unwrap(),
            rel
        );
        assert!(
            elephant_storage::SystemEventRepository::new(storage.clone())
                .list("default", 100, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(elephant_storage::EntityMergeRepository::new(storage)
            .list("default", 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}
