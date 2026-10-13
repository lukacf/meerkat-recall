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

use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    parse_id, PredicateAliasRepository, PredicateRegistryRepository, RelRepository, Repository,
    StorageClient,
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

fn mutation_actor(ctx: &ToolContext) -> &str {
    ctx.verified_caller()
        .map_or("trusted:mcp", |caller| caller.claims().sub.as_str())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original handler ownership, future shape, and call ordering."
)]
async fn authorize_relationship_endpoints(
    ctx: &ToolContext,
    mutation: &mut PublicMutation,
    rel: &mut Rel,
    inherit: bool,
) -> McpResult<()> {
    let original = rel.base.clone();
    let mut candidate = rel.base.clone();
    let repo = elephant_storage::EntityRepository::new(ctx.storage.clone());
    for (index, id) in [rel.subject.id(), rel.object.id()].into_iter().enumerate() {
        let entity = repo
            .get(&ctx.principal.space_id, id)
            .await?
            .ok_or_else(|| {
                McpError::InvalidParameters(
                    "relationship endpoint is not live in selected space".into(),
                )
            })?;
        ctx.authz
            .authorize_record_read(&ctx.principal, &entity, "read:entity")
            .map_err(McpError::ExecutionError)?;
        if inherit && index == 0 && rel.evidence.is_empty() {
            candidate.security = entity.base.security.clone();
        }
        inherit_public_envelope(&mut candidate, &entity.base)?;
        mutation.depend_on(CanonicalRecord::Entity(Box::new(entity)));
    }
    for evidence in &rel.evidence {
        let assertion = if let Some(assertion) = mutation.staged_assertion(evidence.assertion.id())
        {
            assertion.clone()
        } else {
            let assertion = elephant_storage::AssertionRepository::new(ctx.storage.clone())
                .get(&ctx.principal.space_id, evidence.assertion.id())
                .await?
                .ok_or_else(|| {
                    McpError::InvalidParameters(
                        "relationship assertion is not live in selected space".into(),
                    )
                })?;
            mutation.depend_on(CanonicalRecord::Assertion(Box::new(assertion.clone())));
            assertion
        };
        ctx.authz
            .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
            .map_err(McpError::ExecutionError)?;
        if assertion.evidence != evidence.span {
            return Err(McpError::InvalidParameters(
                "relationship span differs from referenced assertion".into(),
            ));
        }
        mutation
            .observe_relationship_assertion(rel, &assertion)
            .await
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let (doc, revision, node) = mutation.observe_evidence(&evidence.span).await?;
        ctx.authz
            .authorize_record_read(&ctx.principal, &doc, "read:doc:raw")
            .map_err(McpError::ExecutionError)?;
        ctx.authz
            .authorize_record_read(&ctx.principal, &revision, "read:doc:raw")
            .map_err(McpError::ExecutionError)?;
        inherit_public_envelope(&mut candidate, &assertion.base)?;
        inherit_public_envelope(&mut candidate, &doc.base)?;
        inherit_public_envelope(&mut candidate, &revision.base)?;
        if let Some(node) = &node {
            ctx.authz
                .authorize_record_read(&ctx.principal, node, "read:doc:raw")
                .map_err(McpError::ExecutionError)?;
            inherit_public_envelope(&mut candidate, &node.base)?;
        }
        if let Some(source) = &doc.source {
            if !candidate.provenance.sources.contains(source.id()) {
                candidate.provenance.sources.push(source.id().clone());
            }
        }
    }
    if inherit {
        rel.base = candidate;
    } else if original.security != candidate.security
        || original.subjects != candidate.subjects
        || original.subjects_state != candidate.subjects_state
    {
        return Err(McpError::InvalidParameters(
            "relationship envelope does not cover endpoint restrictions".into(),
        ));
    }
    ensure_predicate_registered(
        &ctx.storage,
        &ctx.principal.space_id,
        &rel.rel_type,
        &rel.base,
        mutation,
    )
    .await
}

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
    /// Immutable document revision ID containing the exact evidence quote (optional).
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

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original handler ownership, future shape, and call ordering."
)]
async fn canonicalize_rel_type(
    storage: &StorageClient,
    space_id: &str,
    rel_type: &str,
    mutation: &mut PublicMutation,
) -> McpResult<String> {
    let trimmed = rel_type.trim();
    if trimmed.is_empty() {
        return Err(McpError::InvalidParameters(
            "rel_type must not be empty".to_string(),
        ));
    }

    let alias_repo = PredicateAliasRepository::new(storage.clone());
    let alias = alias_repo
        .find_active_by_raw(space_id, trimmed)
        .await
        .map_err(McpError::StorageError)?;
    if let Some(alias) = alias {
        let canonical = alias.predicate_id.clone();
        mutation.depend_on(CanonicalRecord::PredicateAlias(Box::new(alias)));
        Ok(canonical)
    } else {
        Ok(trimmed.to_owned())
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original handler ownership, future shape, and call ordering."
)]
async fn ensure_predicate_registered(
    storage: &StorageClient,
    space_id: &str,
    predicate_id: &str,
    envelope: &BaseFields,
    mutation: &mut PublicMutation,
) -> McpResult<()> {
    let predicate_repo = PredicateRegistryRepository::new(storage.clone());
    if let Some(predicate) = predicate_repo
        .find_by_predicate_id(space_id, predicate_id)
        .await
        .map_err(McpError::StorageError)?
    {
        mutation.depend_on(CanonicalRecord::PredicateRegistry(Box::new(predicate)));
        return Ok(());
    }

    let now = Utc::now();
    let mut base = envelope.clone();
    base.id = RecordId::new_v4(TableName::PredicateRegistry);
    base.created_at = now;
    base.updated_at = now;
    base.deleted_at = None;
    let predicate = PredicateRegistry {
        base,
        predicate_id: predicate_id.to_string(),
        description: None,
        status: "active".to_string(),
        semantics: None,
        semantics_status: SemanticsStatus::Default,
        first_seen_at: Some(now),
        usage_count: 0,
        last_used_at: Some(now),
    };

    mutation.stage_create(CanonicalRecord::PredicateRegistry(Box::new(predicate)))?;
    Ok(())
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

async fn evidence_span_for_manual_quote(
    ctx: &ToolContext,
    mutation: &mut PublicMutation,
    evidence_source: Option<&str>,
    evidence_text: &str,
) -> McpResult<(EvidenceSpan, BaseFields)> {
    let source = evidence_source.ok_or_else(|| {
        McpError::InvalidParameters(
            "evidence_source must identify an immutable document revision".into(),
        )
    })?;
    let revision_id = parse_id(source, TableName::DocRevision)
        .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
    let revision = elephant_storage::DocRevisionRepository::new(ctx.storage.clone())
        .get(&ctx.principal.space_id, &revision_id)
        .await?
        .ok_or_else(|| {
            McpError::InvalidParameters("evidence revision not found in selected space".into())
        })?;
    let doc = elephant_storage::DocRepository::new(ctx.storage.clone())
        .get(&ctx.principal.space_id, revision.doc.id())
        .await?
        .ok_or_else(|| McpError::InvalidParameters("evidence document is not live".into()))?;
    ctx.authz
        .authorize_record_read(&ctx.principal, &revision, "read:doc:raw")
        .map_err(McpError::ExecutionError)?;
    ctx.authz
        .authorize_record_read(&ctx.principal, &doc, "read:doc:raw")
        .map_err(McpError::ExecutionError)?;
    if evidence_text.is_empty() {
        return Err(McpError::InvalidParameters(
            "evidence quote must be nonempty".into(),
        ));
    }
    let mut matches = revision.text.match_indices(evidence_text);
    let (start, _) = matches.next().ok_or_else(|| {
        McpError::InvalidParameters("quote is absent from immutable evidence".into())
    })?;
    if matches.next().is_some() {
        return Err(McpError::InvalidParameters(
            "quote is ambiguous; provide a unique immutable passage".into(),
        ));
    }
    let end = start
        .checked_add(evidence_text.len())
        .ok_or_else(|| McpError::InvalidParameters("evidence span overflow".into()))?;
    let mut base = revision.base.clone();
    inherit_public_envelope(&mut base, &doc.base)?;
    if let Some(source) = &doc.source {
        if !base.provenance.sources.contains(source.id()) {
            base.provenance.sources.push(source.id().clone());
        }
    }
    mutation.depend_on(CanonicalRecord::Doc(Box::new(doc)));
    mutation.depend_on(CanonicalRecord::DocRevision(Box::new(revision)));
    Ok((
        EvidenceSpan {
            revision: Link::new(revision_id),
            node: None,
            start: start
                .try_into()
                .map_err(|_| McpError::InvalidParameters("evidence offset too large".into()))?,
            end: end
                .try_into()
                .map_err(|_| McpError::InvalidParameters("evidence offset too large".into()))?,
            quote_hash: sha256_hex_prefixed(evidence_text),
        },
        base,
    ))
}

#[allow(clippy::cast_possible_truncation)]
#[expect(
    clippy::assigning_clones,
    reason = "Keep the original owned field replacement and mutation ordering."
)]
async fn create_evidence_assertion(
    ctx: &ToolContext,
    mutation: &mut PublicMutation,
    space_id: &str,
    input: &RelateEntitiesInput,
) -> McpResult<Assertion> {
    let now = Utc::now();

    let evidence_text = input
        .evidence_text
        .as_deref()
        .ok_or_else(|| McpError::InvalidParameters("evidence_text required".into()))?;
    let (evidence, mut base) = evidence_span_for_manual_quote(
        ctx,
        mutation,
        input.evidence_source.as_deref(),
        evidence_text,
    )
    .await?;
    if base.space_id != space_id {
        return Err(McpError::InvalidParameters(
            "evidence belongs to another space".into(),
        ));
    }
    base.id = RecordId::new_v4(TableName::Assertion);
    base.created_at = now;
    base.updated_at = now;
    base.created_by = mutation_actor(ctx).to_owned();
    base.deleted_at = None;

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

    ctx.authz
        .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
        .map_err(McpError::ExecutionError)?;
    ctx.authz
        .authorize_record_write(&ctx.principal, &assertion, "write:assertion")
        .map_err(McpError::ExecutionError)?;
    mutation.stage_create(CanonicalRecord::Assertion(Box::new(assertion.clone())))?;
    Ok(assertion)
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
pub struct RelateEntitiesHandler {}

impl RelateEntitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self {}
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
                    "description": "Immutable doc_revision:... containing the exact evidence_text quote"
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
        parse_id(&input.from_id, TableName::Entity)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        parse_id(&input.to_id, TableName::Entity)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &input.space_id).await?;
        input.rel_type = canonicalize_rel_type(
            &ctx.storage,
            &input.space_id,
            &input.rel_type,
            &mut mutation,
        )
        .await?;

        let mut evidence_base = None;
        let evidence = if input
            .evidence_text
            .as_deref()
            .is_some_and(|t| !t.trim().is_empty())
        {
            let assertion =
                create_evidence_assertion(ctx, &mut mutation, &input.space_id, &input).await?;
            evidence_base = Some(assertion.base.clone());
            vec![EvidenceRef {
                assertion: Link::new(assertion.base.id.clone()),
                span: assertion.evidence.clone(),
                time_basis: TimeBasis::ObservedAt,
                time_value: assertion.observed_at,
            }]
        } else {
            Vec::new()
        };

        let mut rel = create_rel_from_input(&input, evidence);
        if let Some(base) = evidence_base {
            rel.base.security = base.security;
            rel.base.subjects = base.subjects;
            rel.base.subjects_state = base.subjects_state;
            rel.base.provenance = base.provenance;
        }

        authorize_relationship_endpoints(ctx, &mut mutation, &mut rel, true).await?;
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

        match mutation.create(rel, mutation_actor(ctx)).await {
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
            Err(e) => Err(McpError::StorageError(e)),
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

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let rid_from = parse_id(&input.from_id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let rid_to = parse_id(&input.to_id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        input.rel_type =
            canonicalize_rel_type(&ctx.storage, &space_id, &input.rel_type, &mut mutation).await?;

        // Try to find existing relationship using find_between
        let existing = self
            .repo
            .find_between(&space_id, &rid_from, &rid_to)
            .await
            .map_err(McpError::StorageError)?;

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
                canonicalize_rel_type(&ctx.storage, &space_id, &updated.rel_type, &mut mutation)
                    .await?;

            authorize_relationship_endpoints(ctx, &mut mutation, &mut updated, false).await?;
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

            match mutation
                .replace(&rel, updated, false, mutation_actor(ctx))
                .await
            {
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
                Err(e) => Err(McpError::StorageError(e)),
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
            let mut rel = create_rel_from_input(&rel_input, Vec::new());
            authorize_relationship_endpoints(ctx, &mut mutation, &mut rel, true).await?;

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

            match mutation.create(rel, mutation_actor(ctx)).await {
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
                Err(e) => Err(McpError::StorageError(e)),
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

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
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
            Err(e) => return Err(McpError::StorageError(e)),
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
            canonicalize_rel_type(&ctx.storage, &space_id, &updated.rel_type, &mut mutation)
                .await?;

        authorize_relationship_endpoints(ctx, &mut mutation, &mut updated, false).await?;
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

        match mutation
            .replace(&existing, updated, false, mutation_actor(ctx))
            .await
        {
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
            Err(e) => Err(McpError::StorageError(e)),
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

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
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

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
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
                mutation
                    .delete(&rel, mutation_actor(ctx))
                    .await
                    .map_err(McpError::StorageError)?;

                let response = ToolResponse::success(json!({ "deleted": true, "id": input.id }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Relationship not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
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

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
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

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let rid = parse_id(&input.id, TableName::Rel)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(rel)) => {
                let mut restored_candidate = rel.clone();
                authorize_relationship_endpoints(
                    ctx,
                    &mut mutation,
                    &mut restored_candidate,
                    false,
                )
                .await?;
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

                let restored = mutation
                    .restore(&rel, mutation_actor(ctx))
                    .await
                    .map_err(McpError::StorageError)?;

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
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

/// Handler for `batch_relationships` tool.
pub struct BatchRelationshipsHandler {}

impl BatchRelationshipsHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self {}
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
            if let Err(error) = parse_id(&item.from_id, TableName::Entity)
                .and_then(|_| parse_id(&item.to_id, TableName::Entity))
            {
                failed.push(BatchError {
                    index,
                    error: error.to_string(),
                });
                continue;
            }
            let mut mutation = PublicMutation::begin(ctx.storage.clone(), &item.space_id).await?;
            match canonicalize_rel_type(&ctx.storage, &item.space_id, &item.rel_type, &mut mutation)
                .await
            {
                Ok(canonical) => item.rel_type = canonical,
                Err(
                    error @ McpError::StorageError(
                        elephant_storage::StorageError::ErasureInProgress,
                    ),
                ) => {
                    return Err(error);
                }
                Err(e) => {
                    failed.push(BatchError {
                        index,
                        error: e.to_string(),
                    });
                    continue;
                }
            }
            let mut rel = create_rel_from_input(&item, Vec::new());
            if let Err(error) =
                authorize_relationship_endpoints(ctx, &mut mutation, &mut rel, true).await
            {
                if matches!(
                    error,
                    McpError::StorageError(elephant_storage::StorageError::ErasureInProgress)
                ) {
                    return Err(error);
                }
                failed.push(BatchError {
                    index,
                    error: error.to_string(),
                });
                continue;
            }

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

            match mutation.create(rel, mutation_actor(ctx)).await {
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
                Err(error @ elephant_storage::StorageError::ErasureInProgress) => {
                    return Err(McpError::StorageError(error));
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn test_canonicalize_rel_type_uses_active_alias() {
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

        storage
            .query_checked(include_str!(
                "../../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .expect("canonical schema");
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id": RecordId::new_v4(TableName::SpaceConfig), "space_id":"test_space", "created_at":Utc::now(), "updated_at":Utc::now(), "created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]}, "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        })).expect("space fixture");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
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

        let mut mutation = PublicMutation::begin(storage.clone(), "test_space")
            .await
            .expect("mutation");
        let canonical =
            canonicalize_rel_type(&storage, "test_space", " employed by ", &mut mutation)
                .await
                .expect("canonical rel type");

        assert_eq!(canonical, "works_at");
        assert!(PredicateRegistryRepository::new(storage.clone())
            .find_by_predicate_id("test_space", "works_at")
            .await
            .expect("find predicate")
            .is_none());
        let entity = elephant_types::records::Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".into(),
                "test".into(),
            ),
            elephant_types::records::entity::EntityType::new("topic"),
            "staged write".into(),
        );
        ensure_predicate_registered(
            &storage,
            "test_space",
            &canonical,
            &entity.base,
            &mut mutation,
        )
        .await
        .expect("stage predicate under final envelope");
        mutation
            .create(entity, "test")
            .await
            .expect("atomic registry and entity");
        assert!(PredicateRegistryRepository::new(storage.clone())
            .find_by_predicate_id("test_space", "works_at")
            .await
            .expect("predicate after commit")
            .is_some());
        assert!(PredicateRegistryRepository::new(storage)
            .find_by_predicate_id("test_space", "employed by")
            .await
            .expect("find raw predicate")
            .is_none());
    }
}
