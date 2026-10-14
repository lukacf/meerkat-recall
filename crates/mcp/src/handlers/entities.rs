#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Entity MCP tool handlers.
//!
//! Implements:
//! - `search_entities`
//! - `get_entity`
//! - `get_entity_with_relations`
//! - `update_entity`
//! - `delete_entity`
//! - `restore_entity`
//! - `batch_entities`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;

use elephant_embeddings::EmbeddingProviderCache;
use elephant_storage::public_mutation::PublicMutation;
use elephant_storage::{
    is_source_inventory_entity, parse_id, EntityRepository, EntityVectorCache, Repository,
    SearchCriteria, SearchRepository, SpaceConfigRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::mcp_simple::CreateEntitySimpleInput;
use elephant_types::records::entity::{EntityStatus, EntityType};
use elephant_types::records::Entity;
use elephant_types::BaseFieldsBuilder;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::assertions::entity_passes_attribution;
use super::docs::resolve_query_embedding;
use super::{merge_base_fields, ToolContext, ToolHandler};

/// Minimum cosine similarity for a semantic hit to participate in ranking.
/// Vector search always returns the nearest neighbors whether or not they
/// are relevant; this floor keeps unrelated entities from being added to
/// results just for being the least-distant strangers.
const MIN_SEMANTIC_SCORE: f32 = 0.30;

/// An approval change after binding resolution must remain an authorization
/// error, even when no vector would otherwise contribute to lexical ranking.
fn entity_vector_availability(
    result: elephant_storage::StorageResult<Vec<(elephant_types::RecordId, Vec<f32>)>>,
) -> McpResult<bool> {
    match result {
        Ok(rows) => Ok(!rows.is_empty()),
        Err(error) if error.to_string().contains("provider_authorization") => Err(
            McpError::Public(elephant_types::public_error::PublicError::new(
                elephant_types::public_error::ErrorCode::ProviderUnapproved,
                "Approved query embedding is unavailable",
            )),
        ),
        Err(error) => {
            warn!("Semantic entity ranking unavailable (vector availability): {error}");
            Ok(false)
        }
    }
}

fn entity_payload_schema() -> Value {
    json!({
        "type": "object",
        "description": "Entity fields. Base fields (id, space_id, created_at, security, etc.) are optional and will be auto-generated if omitted.",
        "required": ["type", "display_name"],
        "properties": {
            "type": { "type": "string", "description": "Entity type (e.g., person, org, domain)" },
            "display_name": { "type": "string", "description": "Entity display name" },
            "summary": { "type": "string", "description": "Optional summary" },
            "metadata": {
                "type": "object",
                "required": [],
                "description": "Arbitrary metadata.",
                "additionalProperties": true
            }
        },
        "additionalProperties": true
    })
}

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `search_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEntitiesInput {
    /// Free-text query.
    #[serde(default)]
    pub query: Option<String>,
    /// Filters.
    #[serde(default)]
    pub filters: Option<SearchFilters>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Search filters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchFilters {
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Filter by entity type.
    #[serde(default)]
    pub entity_type: Option<String>,
    /// Filter by attribution: entities whose `metadata.angel_protectees`
    /// contains this value, plus entities with no `angel_protectees` key.
    #[serde(default)]
    pub attributed_to: Option<String>,
    /// Include soft-deleted entities.
    #[serde(default)]
    pub include_deleted: bool,
}

const fn default_limit() -> usize {
    50
}

/// Output for `search_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEntitiesOutput {
    /// Search results.
    pub items: Vec<Entity>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

fn normalize_entity_query(query: Option<String>) -> Option<String> {
    query.and_then(|query| {
        let trimmed = query.trim();
        if trimmed.is_empty() || trimmed == "*" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Input for `get_entity` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetEntityInput {
    /// Entity ID.
    pub id: String,
}

/// Input for `update_entity` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEntityInput {
    /// Entity ID.
    pub id: String,
    /// Fields to update.
    pub fields: Value,
}

/// Input for delete/restore tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityIdInput {
    /// Entity ID.
    pub id: String,
}

/// Input for `batch_entities` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEntitiesInput {
    /// Items to process.
    pub items: Vec<Entity>,
}

/// Output for batch operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOutput<T> {
    /// Successfully processed items.
    pub succeeded: Vec<T>,
    /// Failed items.
    pub failed: Vec<BatchError>,
}

/// Batch error details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchError {
    /// Index of failed item.
    pub index: usize,
    /// Error message.
    pub error: String,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `search_entities` tool.
pub struct SearchEntitiesHandler {
    repo: EntityRepository,
    space_config_repo: SpaceConfigRepository,
    _provider_cache: EmbeddingProviderCache,
    entity_vectors: EntityVectorCache,
}

impl SearchEntitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self::with_provider_cache(storage, EmbeddingProviderCache::new())
    }

    /// Retained constructor compatibility. Query dispatch resolves the current
    /// approved space binding; cached providers do not grant model authority.
    #[must_use]
    pub fn with_provider_cache(
        storage: StorageClient,
        provider_cache: EmbeddingProviderCache,
    ) -> Self {
        Self {
            repo: EntityRepository::new(storage.clone()),
            space_config_repo: SpaceConfigRepository::new(storage),
            _provider_cache: provider_cache,
            entity_vectors: EntityVectorCache::new(),
        }
    }

    /// Semantic hits for a query: embeds the query text (a read-only
    /// provider call — entity embeddings themselves are only ever written by
    /// the pipeline's maintenance sweep), ranks stored entity embeddings by
    /// cosine similarity, and returns fetched, filter-checked entities with
    /// scores. Returns `None` whenever semantic ranking cannot contribute
    /// (provider unavailable or failing, no stored vectors, nothing above
    /// the similarity floor) so the caller falls back to today's lexical
    /// behavior unchanged.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository handle scope while retaining the existing vector reload and provider authorization order."
    )]
    pub(crate) async fn semantic_entity_hits(
        &self,
        ctx: &ToolContext,
        space_id: &str,
        query: &str,
        entity_type: Option<&EntityType>,
        attributed_to: Option<&str>,
        limit: usize,
    ) -> McpResult<Option<Vec<(Entity, f32)>>> {
        let (policy, provider) = resolve_query_embedding(&self.space_config_repo, space_id).await?;

        // A query vector cannot contribute when this exact approved profile has
        // no live entity vectors. Check current storage before spending a
        // provider call. Discard this read before awaiting inference: ranking
        // below reloads current rows, retaining erasure/restore and input guards.
        let entity_vectors = elephant_storage::entity_profile_embedding_repository::EntityProfileEmbeddingRepository::new(self.repo.storage_client().clone());
        if !entity_vector_availability(
            entity_vectors
                .load(space_id, &policy.model, policy.dims)
                .await,
        )? {
            return Ok(None);
        }

        let vectors = match provider.embed(&[query.to_string()]).await {
            Ok(vectors) => vectors,
            Err(err) => {
                if err.to_string().contains("provider_authorization") {
                    return Err(McpError::Public(
                        elephant_types::public_error::PublicError::new(
                            elephant_types::public_error::ErrorCode::ProviderUnapproved,
                            "Approved query embedding is unavailable",
                        ),
                    ));
                }
                warn!("Semantic entity ranking unavailable (embed): {err}");
                return Ok(None);
            }
        };
        let Some(vector) = vectors.into_iter().next() else {
            return Ok(None);
        };

        let hits = match self
            .repo
            .vector_search_entities_for_policy(
                &self.entity_vectors,
                space_id,
                &policy,
                &vector,
                limit.saturating_mul(2).clamp(10, 50),
            )
            .await
        {
            Ok(hits) => hits,
            Err(err) => {
                warn!("Semantic entity ranking unavailable (vector search): {err}");
                return Ok(None);
            }
        };

        let mut scored = Vec::new();
        for hit in hits {
            if hit.score < MIN_SEMANTIC_SCORE {
                continue;
            }
            let entity = match Repository::get(&self.repo, space_id, &hit.entity_id).await {
                Ok(Some(entity)) => entity,
                Ok(None) => continue,
                Err(err) => {
                    warn!("Semantic hit fetch failed for {}: {err}", hit.entity_id);
                    continue;
                }
            };
            // Vector search ranks by similarity only; re-apply the same
            // visibility and filter rules the lexical path enforces.
            if entity.status == EntityStatus::Merged || is_source_inventory_entity(&entity) {
                continue;
            }
            if let Some(entity_type) = entity_type {
                if &entity.entity_type != entity_type {
                    continue;
                }
            }
            if !entity_passes_attribution(&entity, attributed_to) {
                continue;
            }
            scored.push((entity, hit.score));
        }

        if scored.is_empty() {
            return Ok(None);
        }

        // Row-level authz, preserving scores.
        let score_by_id: std::collections::HashMap<String, f32> = scored
            .iter()
            .map(|(entity, score)| (entity.base.id.to_string(), *score))
            .collect();
        let entities: Vec<Entity> = scored.into_iter().map(|(entity, _)| entity).collect();
        let visible = crate::current_read::filter(ctx, entities, "read:entity").await?;
        let visible: Vec<(Entity, f32)> = visible
            .into_iter()
            .filter_map(|entity| {
                let score = score_by_id.get(&entity.base.id.to_string()).copied()?;
                Some((entity, score))
            })
            .collect();

        Ok((!visible.is_empty()).then_some(visible))
    }
}

/// Ranking key for the blended result list: exact display-name/alias matches
/// keep absolute priority (the query IS the entity's name), everything else
/// ranks by the better of its semantic similarity and a lexical affinity
/// tier, with deterministic name/id tie-breaks.
fn entity_blend_rank(entity: &Entity, query_lower: &str, semantic_score: f32) -> (bool, f32) {
    let name_lower = entity.display_name.to_lowercase();
    let exact = name_lower == query_lower
        || entity
            .aliases
            .iter()
            .any(|alias| alias.to_lowercase() == query_lower);
    let lexical: f32 = if exact {
        1.0
    } else if name_lower.starts_with(query_lower) {
        0.7
    } else if name_lower.contains(query_lower)
        || entity
            .aliases
            .iter()
            .any(|alias| alias.to_lowercase().contains(query_lower))
    {
        0.5
    } else {
        0.0
    };
    (exact, lexical.max(semantic_score))
}

#[async_trait]
impl ToolHandler for SearchEntitiesHandler {
    fn name(&self) -> &'static str {
        "search_entities"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search for entities by query and filters. Text queries rank ",
            "semantically when entity embeddings are available (exact name ",
            "matches always rank first); without embeddings the lexical ",
            "match behavior is unchanged.\n\n",
            "Example:\n",
            r#"{ "query": "Alex Chen", "filters": { "entity_type": "person" }, "limit": 10 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "use_vector": { "type": "boolean", "default": true },
                "query": {
                    "type": "string",
                    "description": "Free-text search query"
                },
                "filters": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "space_id": { "type": "string" },
                        "entity_type": { "type": "string" },
                        "attributed_to": {
                            "type": "string",
                            "description": "Return entities whose metadata.angel_protectees contains this value; entities without an angel_protectees key are also included"
                        },
                        "include_deleted": { "type": "boolean", "default": false }
                    }
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
        let use_vector = params
            .get("use_vector")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let input: SearchEntitiesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let mut criteria = SearchCriteria::new().with_limit(input.limit);

        if let Some(q) = normalize_entity_query(input.query) {
            criteria = criteria.with_query(q);
        }

        if let Some(filters) = input.filters {
            if filters.include_deleted {
                criteria = criteria.include_deleted();
            }
            if let Some(entity_type) = filters.entity_type {
                criteria = criteria.with_entity_type(EntityType::new(&entity_type));
            }
            if let Some(attributed_to) = filters.attributed_to {
                criteria = criteria.with_attributed_to(attributed_to);
            }
            if let Some(space) = filters.space_id {
                if space != ctx.principal.space_id {
                    let response: ToolResponse<()> =
                        ToolResponse::error("Access denied: access to space denied");
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
            }
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        criteria = criteria.with_space_id(space_id.clone());

        let first_page = input.cursor.is_none();
        if let Some(cursor) = input.cursor {
            criteria = criteria.with_cursor(cursor);
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

        let mut collected: Vec<Entity> = Vec::new();
        let mut page_cursor = criteria.cursor.clone();
        let mut has_more = true;
        let mut safety_limit = 0usize;

        while collected.len() < input.limit && has_more {
            safety_limit += 1;
            if safety_limit > 1000 {
                return Err(McpError::ExecutionError(
                    "Search pagination exceeded safety limit".to_string(),
                ));
            }

            let mut page_criteria = criteria.clone();
            page_criteria.cursor.clone_from(&page_cursor);

            let page = self
                .repo
                .search(&page_criteria)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;

            has_more = page.has_more;
            page_cursor.clone_from(&page.cursor);

            if page.results.is_empty() {
                break;
            }

            let filtered = crate::current_read::filter(ctx, page.results, "read:entity").await?;

            for entity in filtered {
                if collected.len() >= input.limit {
                    break;
                }
                collected.push(entity);
            }

            if has_more && page_cursor.is_none() {
                break;
            }
        }

        // The cursor stays a lexical id-ordered cursor: it is computed from
        // the lexical result stream before any semantic re-ranking below.
        let next_cursor = if has_more && collected.len() >= input.limit {
            collected.last().map(|e| {
                e.base
                    .id
                    .to_string()
                    .strip_prefix("entity:")
                    .map_or_else(|| e.base.id.to_string(), std::string::ToString::to_string)
            })
        } else {
            None
        };
        let has_more_output = next_cursor.is_some();

        // Semantic ranking blend (first page of a text query only). When
        // embeddings are absent or the provider is unavailable this is None
        // and the lexical results pass through untouched — identical to the
        // pre-embedding behavior.
        let semantic = if first_page && use_vector {
            match criteria.query.as_deref() {
                Some(query) => {
                    self.semantic_entity_hits(
                        ctx,
                        &space_id,
                        query,
                        criteria.entity_type.as_ref(),
                        criteria.attributed_to.as_deref(),
                        input.limit,
                    )
                    .await?
                }
                None => None,
            }
        } else {
            None
        };

        let items = if let Some(semantic) = semantic {
            let query_lower = criteria.query.as_deref().unwrap_or_default().to_lowercase();
            let semantic_scores: std::collections::HashMap<String, f32> = semantic
                .iter()
                .map(|(entity, score)| (entity.base.id.to_string(), *score))
                .collect();
            let mut seen: std::collections::HashSet<String> = collected
                .iter()
                .map(|entity| entity.base.id.to_string())
                .collect();
            let mut items = collected;
            // Semantic-only hits (e.g. summary matches with no substring
            // overlap) join the candidate set.
            for (entity, _) in semantic {
                if seen.insert(entity.base.id.to_string()) {
                    items.push(entity);
                }
            }
            items.sort_by(|left, right| {
                let left_score = semantic_scores
                    .get(&left.base.id.to_string())
                    .copied()
                    .unwrap_or(0.0);
                let right_score = semantic_scores
                    .get(&right.base.id.to_string())
                    .copied()
                    .unwrap_or(0.0);
                let (left_exact, left_rank) = entity_blend_rank(left, &query_lower, left_score);
                let (right_exact, right_rank) = entity_blend_rank(right, &query_lower, right_score);
                right_exact
                    .cmp(&left_exact)
                    .then_with(|| {
                        right_rank
                            .partial_cmp(&left_rank)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .then_with(|| left.display_name.cmp(&right.display_name))
                    .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
            });
            items.truncate(input.limit);
            items
        } else {
            collected
        };

        let output = SearchEntitiesOutput {
            items,
            next_cursor,
            has_more: has_more_output,
            total: None,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_entity` tool.
pub struct GetEntityHandler {
    repo: EntityRepository,
}

impl GetEntityHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetEntityHandler {
    fn name(&self) -> &'static str {
        "get_entity"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a single entity by ID.\n\n",
            "Example:\n",
            r#"{ "id": "entity:f47ac10b-58cc-4372-a567-0e02b2c3d479" }"#,
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
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetEntityInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(entity)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &entity, "read:entity")
                {
                    let response: ToolResponse<Entity> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let entity = crate::current_read::require(ctx, entity, "read:entity").await?;
                let response = ToolResponse::success(entity);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<Entity> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `get_entity_with_relations` tool.
pub struct GetEntityWithRelationsHandler {
    repo: EntityRepository,
}

impl GetEntityWithRelationsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetEntityWithRelationsHandler {
    fn name(&self) -> &'static str {
        "get_entity_with_relations"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get an entity with its relationships.\n\n",
            "Example:\n",
            r#"{ "id": "entity:f47ac10b-58cc-4372-a567-0e02b2c3d479" }"#,
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
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetEntityInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_relations(&space_id, &rid).await {
            Ok(Some(mut entity)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &entity.entity, "read:entity")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                entity.entity =
                    crate::current_read::require(ctx, entity.entity, "read:entity").await?;

                let outgoing_relations = crate::current_read::filter(
                    ctx,
                    entity.outgoing_relations,
                    "read:relationship",
                )
                .await?;
                let incoming_relations = crate::current_read::filter(
                    ctx,
                    entity.incoming_relations,
                    "read:relationship",
                )
                .await?;

                let entity = elephant_storage::EntityWithRelations {
                    entity: entity.entity,
                    outgoing_relations,
                    incoming_relations,
                };

                let response = ToolResponse::success(entity);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

fn mutation_actor(ctx: &ToolContext) -> &str {
    ctx.verified_caller()
        .map_or("trusted:mcp", |caller| caller.claims().sub.as_str())
}

/// Handler for `update_entity` tool.
pub struct UpdateEntityHandler {
    repo: EntityRepository,
}

impl UpdateEntityHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UpdateEntityHandler {
    fn name(&self) -> &'static str {
        "update_entity"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Update an entity's fields while preserving its security classification. ",
            "Security fields cannot be changed through this tool.\n\n",
            "Example:\n",
            r#"{ "id": "entity:f47ac10b-58cc-4372-a567-0e02b2c3d479", "fields": { "summary": "Founder of NeuralFlow" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "fields"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Entity ID"
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
        let input: UpdateEntityInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        // Get existing entity first
        let existing = match self.repo.get(&space_id, &rid).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::StorageError(e)),
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &existing, "write:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &existing, "read:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Merge fields into existing entity
        let mut entity_value =
            serde_json::to_value(&existing).map_err(|e| McpError::ExecutionError(e.to_string()))?;

        if let Value::Object(ref mut map) = entity_value {
            if let Value::Object(fields) = input.fields {
                for (k, v) in fields {
                    map.insert(k, v);
                }
            }
        }

        let updated: Entity = serde_json::from_value(entity_value)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if updated.base.security != existing.base.security {
            let response: ToolResponse<()> = ToolResponse::error(
                "Access denied: update_entity cannot change an entity's security classification",
            );
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if let Err(reason) =
            elephant_policy::authorize_ordinary_update(&existing.base, &updated.base)
        {
            return serde_json::to_value(ToolResponse::<()>::error(format!(
                "Access denied: {reason}"
            )))
            .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &updated, "write:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &updated, "read:entity")
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
            Ok(entity) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &entity, "read:entity")
                {
                    let response: ToolResponse<Entity> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(entity);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

/// Handler for `delete_entity` tool.
pub struct DeleteEntityHandler {
    repo: EntityRepository,
}

impl DeleteEntityHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for DeleteEntityHandler {
    fn name(&self) -> &'static str {
        "delete_entity"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Soft delete an entity.\n\n",
            "Example:\n",
            r#"{ "id": "entity:f47ac10b-58cc-4372-a567-0e02b2c3d479" }"#,
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
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the mutation plan scope through authorization, commit and response construction."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: EntityIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        // Check if entity exists
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(entity)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &entity, "write:entity")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                mutation
                    .delete(&entity, mutation_actor(ctx))
                    .await
                    .map_err(McpError::StorageError)?;

                let response = ToolResponse::success(json!({ "deleted": true, "id": input.id }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

/// Handler for `restore_entity` tool.
pub struct RestoreEntityHandler {
    repo: EntityRepository,
}

impl RestoreEntityHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RestoreEntityHandler {
    fn name(&self) -> &'static str {
        "restore_entity"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Restore a soft-deleted entity.\n\n",
            "Example:\n",
            r#"{ "id": "entity:f47ac10b-58cc-4372-a567-0e02b2c3d479" }"#,
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
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the mutation plan scope through authorization, commit and response construction."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: EntityIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let rid = parse_id(&input.id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(entity)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &entity, "write:entity")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &entity, "read:entity")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                // Guarded: restores only the row authorized above. A row changed
                // in between is a conflict, not acted on under the old decision.
                let restored = match mutation.restore(&entity, mutation_actor(ctx)).await {
                    Ok(restored) => restored,
                    Err(elephant_storage::StorageError::Conflict(_)) => {
                        let response: ToolResponse<()> = ToolResponse::error(
                            "Entity changed since it was authorized; retry".to_string(),
                        );
                        return serde_json::to_value(response)
                            .map_err(|e| McpError::ExecutionError(e.to_string()));
                    }
                    Err(e) => return Err(McpError::StorageError(e)),
                };

                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &restored, "read:entity")
                {
                    let response: ToolResponse<Entity> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                let response = ToolResponse::success(restored);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Entity not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::StorageError(e)),
        }
    }
}

/// Handler for `batch_entities` tool.
pub struct BatchEntitiesHandler {
    repo: EntityRepository,
}

impl BatchEntitiesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EntityRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for BatchEntitiesHandler {
    fn name(&self) -> &'static str {
        "batch_entities"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Batch create or update entities. Existing entities, including deleted entities, ",
            "must retain their complete security classification. Include unchanged security ",
            "fields when updating an existing entity.\n\n",
            "Example:\n",
            r#"{
  "items": [
    {
      "type": "person",
      "display_name": "Alex Chen",
      "summary": "Security researcher"
    }
  ]
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["items"],
            "properties": {
                "items": {
                    "type": "array",
                    "items": entity_payload_schema(),
                    "description": "Entities to create/update"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(Value::Array(items)) = params.get_mut("items") {
            for item in items {
                let Some(obj) = item.as_object_mut() else {
                    continue;
                };
                merge_base_fields(obj, TableName::Entity, ctx)?;
            }
        }
        let input: BatchEntitiesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();

        for (index, entity) in input.items.into_iter().enumerate() {
            let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
            let rid = entity.base.id.clone();

            // Try to get existing entity
            let result = match self.repo.get_with_deleted(&space_id, &rid).await {
                Ok(Some(existing)) => {
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_write(&ctx.principal, &existing, "write:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_read(&ctx.principal, &existing, "read:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_write(&ctx.principal, &entity, "write:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_read(&ctx.principal, &entity, "read:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }

                    if entity.base.security != existing.base.security {
                        failed.push(BatchError {
                            index,
                            error: "Access denied: batch_entities cannot change an entity's security classification"
                                .to_string(),
                        });
                        continue;
                    }

                    if let Err(reason) =
                        elephant_policy::authorize_ordinary_update(&existing.base, &entity.base)
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }

                    // Update existing
                    mutation
                        .replace(&existing, entity, true, mutation_actor(ctx))
                        .await
                }
                Ok(None) => {
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_write(&ctx.principal, &entity, "write:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_read(&ctx.principal, &entity, "read:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                        continue;
                    }
                    // Create new
                    mutation.create(entity, mutation_actor(ctx)).await
                }
                Err(e) => Err(e),
            };

            match result {
                Ok(entity) => {
                    if let Err(reason) =
                        ctx.authz
                            .authorize_record_read(&ctx.principal, &entity, "read:entity")
                    {
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                    } else {
                        succeeded.push(entity);
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

        let output = BatchOutput { succeeded, failed };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `create_entity_simple` tool.
pub struct CreateEntitySimpleHandler {}

impl CreateEntitySimpleHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self {}
    }
}

#[async_trait]
impl ToolHandler for CreateEntitySimpleHandler {
    fn name(&self) -> &'static str {
        "create_entity_simple"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create an entity with only type + display_name; all base fields are auto-generated.\n\n",
            "Example:\n",
            r#"{ "entity_type": "person", "display_name": "Alex Chen", "summary": "Founder of NeuralFlow" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["entity_type", "display_name"],
            "properties": {
                "entity_type": { "type": "string", "description": "Entity type (e.g. person, org, topic)" },
                "display_name": { "type": "string", "description": "Primary display name" },
                "summary": { "type": "string", "description": "Optional brief summary" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CreateEntitySimpleInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let entity_type = EntityType::new(&input.entity_type);

        let base = BaseFieldsBuilder::new(space_id)
            .for_table(TableName::Entity)
            .build();
        let mut entity = Entity::new(base, entity_type, input.display_name);
        entity.summary = input.summary;

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &entity, "write:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &entity, "read:entity")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match mutation.create(entity, mutation_actor(ctx)).await {
            Ok(entity) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &entity, "read:entity")
                {
                    let response: ToolResponse<Entity> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(entity);
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
    use elephant_types::records::base::Security;
    use elephant_types::records::entity::EntityStatus;

    fn security_test_context(storage: StorageClient) -> ToolContext {
        use elephant_policy::{Clearance, Principal};
        use std::collections::HashSet;

        ToolContext::new(
            storage,
            Principal {
                space_id: "test_space".to_string(),
                scopes: HashSet::from(["read:entity".to_string(), "write:entity".to_string()]),
                clearances: ["restricted", "alternative"]
                    .into_iter()
                    .map(|policy_id| Clearance {
                        policy_id: policy_id.to_string(),
                        max_level: 3,
                        labels: HashSet::from(["comp:private".to_string()]),
                        handling_allow: HashSet::from(["handling:trusted_context".to_string()]),
                    })
                    .collect(),
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        )
    }

    async fn canonical_test_space(storage: &StorageClient, space: &str) {
        if !storage.is_embedded() {
            storage
                .query_checked(include_str!(
                    "../../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .expect("canonical schema");
        }
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id":elephant_types::RecordId::new_v4(TableName::SpaceConfig),"space_id":space,"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete","embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        })).expect("space fixture");
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
    }

    fn security_test_entity() -> Entity {
        let mut entity = semantic_test_entity("Classified entity", Some("Original summary"));
        entity.base.security = Security {
            policy_id: "restricted".to_string(),
            level: 2,
            labels: vec!["comp:private".to_string()],
            handling: vec!["handling:trusted_context".to_string()],
        };
        entity
    }

    fn changed_security_envelopes(original: &Security) -> Vec<Security> {
        vec![
            Security {
                level: 0,
                ..original.clone()
            },
            Security {
                level: 3,
                ..original.clone()
            },
            Security {
                policy_id: "alternative".to_string(),
                ..original.clone()
            },
            Security {
                labels: vec![],
                ..original.clone()
            },
            Security {
                handling: vec![],
                ..original.clone()
            },
        ]
    }

    fn assert_security_test_access(ctx: &ToolContext, entity: &Entity) {
        assert!(!ctx.authz.is_skip_auth());
        assert!(ctx
            .authz
            .authorize_record_read(&ctx.principal, entity, "read:entity")
            .is_ok());
        assert!(ctx
            .authz
            .authorize_record_write(&ctx.principal, entity, "write:entity")
            .is_ok());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_update_entity_cannot_change_security_with_clearance_for_both() {
        let storage = StorageClient::new_embedded().await.unwrap();
        canonical_test_space(&storage, "test_space").await;
        let repo = EntityRepository::new(storage.clone());
        let existing = repo.create(security_test_entity()).await.unwrap();
        let ctx = security_test_context(storage.clone());
        let handler = UpdateEntityHandler::new(storage);
        assert_security_test_access(&ctx, &existing);

        for security in changed_security_envelopes(&existing.base.security) {
            let mut proposed = existing.clone();
            proposed.base.security = security.clone();
            assert_security_test_access(&ctx, &proposed);

            let response = handler
                .execute(
                    &ctx,
                    json!({
                        "id": existing.base.id,
                        "fields": {"security": security, "summary": "Unauthorized change"}
                    }),
                )
                .await
                .unwrap();
            assert_eq!(response["ok"], false);
            assert!(response["error"]
                .as_str()
                .unwrap()
                .starts_with("Access denied: update_entity cannot change"));
            let stored = repo
                .get("test_space", &existing.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(stored).unwrap(),
                serde_json::to_value(&existing).unwrap()
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_update_entity_content_preserves_security() {
        let storage = StorageClient::new_embedded().await.unwrap();
        canonical_test_space(&storage, "test_space").await;
        let repo = EntityRepository::new(storage.clone());
        let existing = repo.create(security_test_entity()).await.unwrap();
        let ctx = security_test_context(storage.clone());
        let handler = UpdateEntityHandler::new(storage);

        for fields in [
            json!({"summary": "Updated summary"}),
            json!({"display_name": "Updated name", "security": existing.base.security}),
        ] {
            let response = handler
                .execute(&ctx, json!({"id": existing.base.id, "fields": fields}))
                .await
                .unwrap();
            assert_eq!(response["ok"], true, "{response}");
        }
        let stored = repo
            .get("test_space", &existing.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.summary.as_deref(), Some("Updated summary"));
        assert_eq!(stored.display_name, "Updated name");
        assert_eq!(stored.base.security, existing.base.security);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_batch_entities_cannot_change_security_or_restore_deleted_rows() {
        let storage = StorageClient::new_embedded().await.unwrap();
        canonical_test_space(&storage, "test_space").await;
        let repo = EntityRepository::new(storage.clone());
        let ctx = security_test_context(storage.clone());
        let handler = BatchEntitiesHandler::new(storage);

        for deleted in [false, true] {
            let created = repo.create(security_test_entity()).await.unwrap();
            if deleted {
                repo.delete("test_space", &created.base.id).await.unwrap();
            }
            let existing = repo
                .get_with_deleted("test_space", &created.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_security_test_access(&ctx, &existing);

            for security in changed_security_envelopes(&existing.base.security) {
                let mut proposed = existing.clone();
                proposed.base.security = security;
                proposed.summary = Some("Unauthorized change".to_string());
                proposed.base.deleted_at = None;
                assert_security_test_access(&ctx, &proposed);

                let value = handler
                    .execute(&ctx, json!({"items": [proposed]}))
                    .await
                    .unwrap();
                let response: ToolResponse<BatchOutput<Entity>> =
                    serde_json::from_value(value).unwrap();
                let output = response.data.unwrap();
                assert!(output.succeeded.is_empty());
                assert_eq!(output.failed.len(), 1);
                assert_eq!(output.failed[0].index, 0);
                assert!(output.failed[0]
                    .error
                    .starts_with("Access denied: batch_entities cannot change"));
                let stored = repo
                    .get_with_deleted("test_space", &existing.base.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(stored).unwrap(),
                    serde_json::to_value(&existing).unwrap()
                );
                assert_eq!(
                    repo.get("test_space", &existing.base.id)
                        .await
                        .unwrap()
                        .is_none(),
                    deleted
                );
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_batch_entities_preserves_security_on_content_update_and_restore() {
        let storage = StorageClient::new_embedded().await.unwrap();
        canonical_test_space(&storage, "test_space").await;
        let repo = EntityRepository::new(storage.clone());
        let ctx = security_test_context(storage.clone());
        let handler = BatchEntitiesHandler::new(storage);

        for deleted in [false, true] {
            let mut existing = repo.create(security_test_entity()).await.unwrap();
            if deleted {
                repo.delete("test_space", &existing.base.id).await.unwrap();
                existing = repo
                    .get_with_deleted("test_space", &existing.base.id)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let mut proposed = existing.clone();
            proposed.summary = Some("Updated summary".to_string());
            let value = handler
                .execute(&ctx, json!({"items": [proposed]}))
                .await
                .unwrap();
            let response: ToolResponse<BatchOutput<Entity>> =
                serde_json::from_value(value).unwrap();
            let output = response.data.unwrap();
            assert!(output.failed.is_empty(), "{:?}", output.failed);
            assert_eq!(output.succeeded.len(), 1);
            let stored = repo
                .get("test_space", &existing.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.summary.as_deref(), Some("Updated summary"));
            assert_eq!(stored.base.security, existing.base.security);
            assert!(stored.base.deleted_at.is_none());
        }
    }

    #[test]
    fn test_search_entities_input_defaults() {
        let input: SearchEntitiesInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.query.is_none());
    }

    #[test]
    fn test_search_filters_defaults() {
        let filters: SearchFilters = serde_json::from_str("{}").unwrap();
        assert!(!filters.include_deleted);
        assert!(filters.space_id.is_none());
    }

    #[test]
    fn test_search_entities_wildcard_query_lists_all() {
        assert_eq!(normalize_entity_query(Some("*".to_string())), None);
        assert_eq!(normalize_entity_query(Some("  *  ".to_string())), None);
        assert_eq!(
            normalize_entity_query(Some("Tom".to_string())),
            Some("Tom".to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_search_entities_applies_entity_type_filter() {
        use std::collections::HashSet;
        use std::sync::Arc;

        use crate::authz::AuthzService;
        use crate::handlers::ToolContext;
        use crate::tools::ToolResponse;
        use elephant_policy::Principal;
        use elephant_storage::{Repository, StorageClient};
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE entity SCHEMALESS;")
            .await
            .expect("schema");
        let repo = EntityRepository::new(storage.clone());

        repo.create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Jane Smith".to_string(),
        ))
        .await
        .expect("person entity");
        repo.create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("org"),
            "Acme Corp".to_string(),
        ))
        .await
        .expect("org entity");

        let principal = Principal {
            space_id: "test_space".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(true)),
            principal,
        );
        let handler = SearchEntitiesHandler::new(ctx.storage.clone());

        let value = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "use_vector": false, "query": "*",
                    "filters": {"entity_type": "person"},
                    "limit": 10
                }),
            )
            .await
            .expect("search");
        let response: ToolResponse<SearchEntitiesOutput> =
            serde_json::from_value(value).expect("response");
        let output = response.data.expect("search output");

        assert_eq!(output.items.len(), 1);
        assert_eq!(output.items[0].display_name, "Jane Smith");
        assert_eq!(output.items[0].entity_type, EntityType::new("person"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_search_entities_hides_merged_redirects_by_default() {
        use std::collections::HashSet;
        use std::sync::Arc;

        use crate::authz::AuthzService;
        use crate::handlers::ToolContext;
        use crate::tools::ToolResponse;
        use elephant_policy::Principal;
        use elephant_storage::{Repository, StorageClient};
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;
        use elephant_types::Link;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE entity SCHEMALESS;")
            .await
            .expect("schema");
        let repo = EntityRepository::new(storage.clone());

        let canonical_id = RecordId::new_v4(TableName::Entity);
        repo.create(Entity::new(
            BaseFields::new(
                canonical_id.clone(),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Riley Example".to_string(),
        ))
        .await
        .expect("canonical entity");

        let mut merged = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Ada".to_string(),
        );
        merged.status = EntityStatus::Merged;
        merged.redirect_to = Some(Link::new(canonical_id));
        repo.create(merged).await.expect("merged entity");

        let principal = Principal {
            space_id: "test_space".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(true)),
            principal,
        );
        let handler = SearchEntitiesHandler::new(ctx.storage.clone());

        let value = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "use_vector": false, "query": "Riley",
                    "limit": 10
                }),
            )
            .await
            .expect("search");
        let response: ToolResponse<SearchEntitiesOutput> =
            serde_json::from_value(value).expect("response");
        let output = response.data.expect("search output");

        assert_eq!(output.items.len(), 1);
        assert_eq!(output.items[0].display_name, "Riley Example");
        assert_eq!(output.items[0].status, EntityStatus::Active);
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage ownership and cleanup timing through construction of the ordinary-update context."
    )]
    async fn ordinary_update_fixture(embedded: bool) -> (ToolContext, EntityRepository, Entity) {
        use elephant_policy::{Clearance, Principal};
        use elephant_storage::Repository;
        use elephant_types::records::base::{BaseFields, SubjectsState};
        use elephant_types::{Link, RecordId};
        use std::collections::HashSet;

        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .expect("storage");
        canonical_test_space(&storage, "test_space").await;
        if !embedded {
            storage
                .query("DEFINE TABLE entity SCHEMALESS;")
                .await
                .expect("schema");
        }
        let repo = EntityRepository::new(storage.clone());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "test_space".into(),
            "fixture".into(),
        );
        base.security.policy_id = "default".into();
        base.security.level = 2;
        base.security.labels = vec!["private".into()];
        base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        base.subjects_state = SubjectsState::Complete;
        let entity = repo
            .create(Entity::new(
                base,
                EntityType::new("person"),
                "Before".into(),
            ))
            .await
            .expect("seed");
        let ctx = ToolContext::new(
            storage,
            Principal {
                space_id: "test_space".into(),
                scopes: HashSet::from(["*".into()]),
                clearances: vec![Clearance {
                    policy_id: "default".into(),
                    max_level: 5,
                    labels: HashSet::from(["private".into()]),
                    handling_allow: HashSet::from(["handling:subjectless_ok".into()]),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );
        (ctx, repo, entity)
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn ordinary_update_refuses_relabel_on_both_storage_backends() {
        use elephant_storage::Repository;
        for embedded in [false, true] {
            let (ctx, repo, entity) = ordinary_update_fixture(embedded).await;
            let handler = UpdateEntityHandler::new(ctx.storage.clone());
            let patches = [
                json!({"security": {"policy_id": "default", "level": 0, "labels": ["private"], "handling": []}}),
                json!({"security": {"policy_id": "default", "level": 2, "labels": [], "handling": ["handling:subjectless_ok"]}}),
                json!({"subjects": []}),
                json!({"subjects_state": "unknown"}),
            ];
            for fields in patches {
                let expected_reason = if fields.get("security").is_some() {
                    "update_entity cannot change"
                } else {
                    "separately authorized relabel"
                };
                let response = handler
                    .execute(
                        &ctx,
                        json!({"id": entity.base.id.to_string(), "fields": fields}),
                    )
                    .await
                    .expect("dispatch");
                assert_eq!(response["ok"], false, "{response}");
                assert!(response["error"]
                    .as_str()
                    .expect("error")
                    .contains(expected_reason));
                let stored = repo
                    .get("test_space", &entity.base.id)
                    .await
                    .expect("read")
                    .expect("entity");
                assert_eq!(
                    serde_json::to_value(stored).expect("json"),
                    serde_json::to_value(&entity).expect("json")
                );
            }
            let response = handler
                .execute(
                    &ctx,
                    json!({"id": entity.base.id.to_string(), "fields": {"display_name": "After"}}),
                )
                .await
                .expect("update");
            assert_eq!(response["ok"], true, "{response}");
            assert_eq!(
                repo.get("test_space", &entity.base.id)
                    .await
                    .expect("read")
                    .expect("entity")
                    .display_name,
                "After"
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn ordinary_batch_refuses_relabel_before_restoring_deleted_entity() {
        use elephant_storage::Repository;
        for embedded in [false, true] {
            let (ctx, repo, entity) = ordinary_update_fixture(embedded).await;
            repo.delete("test_space", &entity.base.id)
                .await
                .expect("delete");
            let before = repo
                .get_with_deleted("test_space", &entity.base.id)
                .await
                .expect("read")
                .expect("entity");
            let mut replacement = before.clone();
            replacement.base.security.level = 0;
            let handler = BatchEntitiesHandler::new(ctx.storage.clone());
            let response = handler
                .execute(&ctx, json!({"items": [replacement]}))
                .await
                .expect("batch");
            assert_eq!(
                response["data"]["succeeded"]
                    .as_array()
                    .expect("succeeded")
                    .len(),
                0,
                "{response}"
            );
            assert_eq!(
                response["data"]["failed"].as_array().expect("failed").len(),
                1
            );
            let after = repo
                .get_with_deleted("test_space", &entity.base.id)
                .await
                .expect("read")
                .expect("entity");
            assert_eq!(
                serde_json::to_value(after).expect("json"),
                serde_json::to_value(&before).expect("json")
            );
            let response = handler
                .execute(&ctx, json!({"items": [before]}))
                .await
                .expect("batch same envelope");
            assert_eq!(
                response["data"]["failed"].as_array().expect("failed").len(),
                0,
                "{response}"
            );
            assert!(repo
                .get("test_space", &entity.base.id)
                .await
                .expect("read")
                .is_some());
        }
    }

    #[tokio::test]
    async fn authorized_replacement_refuses_concurrent_envelope_change() {
        use elephant_storage::Repository;
        for embedded in [false, true] {
            let (_, repo, observed) = ordinary_update_fixture(embedded).await;
            let mut reclassified = observed.clone();
            reclassified.base.security.level = 4;
            let current = repo
                .update("test_space", &observed.base.id, reclassified)
                .await
                .expect("trusted reclassification");
            let mut stale_update = observed.clone();
            stale_update.display_name = "Stale".into();
            assert!(matches!(
                repo.replace_authorized(&observed, stale_update, false)
                    .await,
                Err(elephant_storage::StorageError::Conflict(_))
            ));
            let after = repo
                .get("test_space", &observed.base.id)
                .await
                .expect("read")
                .expect("entity");
            assert_eq!(
                serde_json::to_value(after).expect("json"),
                serde_json::to_value(current).expect("json")
            );
        }
    }

    async fn semantic_test_setup(
        provider_entries: Vec<(&str, Vec<f32>)>,
    ) -> (
        StorageClient,
        EntityRepository,
        SearchEntitiesHandler,
        crate::retrieval_test_support::QueryFixture,
    ) {
        use std::sync::Arc;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE entity SCHEMALESS;")
            .await
            .expect("schema");
        let repo = EntityRepository::new(storage.clone());
        let fixture = crate::retrieval_test_support::QueryFixture::install(
            &storage,
            "test_space",
            provider_entries,
        )
        .await;
        let handler = SearchEntitiesHandler::new(storage.clone());
        (storage, repo, handler, fixture)
    }

    fn semantic_test_ctx(storage: StorageClient) -> ToolContext {
        use crate::authz::AuthzService;
        use elephant_policy::Principal;
        use std::collections::HashSet;
        use std::sync::Arc;

        ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(true)),
            Principal {
                space_id: "test_space".to_string(),
                scopes: HashSet::new(),
                clearances: Vec::new(),
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        )
    }

    fn semantic_test_entity(name: &str, summary: Option<&str>) -> Entity {
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;

        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            name.to_string(),
        );
        entity.summary = summary.map(ToString::to_string);
        entity
    }

    async fn run_search(
        handler: &SearchEntitiesHandler,
        ctx: &ToolContext,
        query: &str,
        fixture: &crate::retrieval_test_support::QueryFixture,
    ) -> SearchEntitiesOutput {
        let value = fixture
            .run(handler.execute(ctx, serde_json::json!({ "query": query, "limit": 10 })))
            .await
            .expect("search");
        let response: ToolResponse<SearchEntitiesOutput> =
            serde_json::from_value(value).expect("response");
        response.data.expect("search output")
    }

    #[tokio::test]
    async fn test_search_entities_semantic_match_beats_substring_miss() {
        use elephant_storage::Repository;

        let (storage, repo, handler, fixture) =
            semantic_test_setup(vec![("machine learning", vec![1.0, 0.0, 0.0])]).await;

        // "neural networks researcher" shares no substring with the query —
        // today's lexical search cannot find this entity.
        let researcher = semantic_test_entity("Riley Kim", Some("neural networks researcher"));
        let researcher_id = researcher.base.id.clone();
        let researcher_hash =
            elephant_storage::entity_profile_embedding_repository::entity_input_hash(&researcher);
        repo.create(researcher).await.expect("create researcher");
        // Pipeline-written embedding, close to the query vector.
        repo.set_name_embedding(
            "test_space",
            &researcher_id,
            &[0.95, 0.05, 0.0],
            &fixture.policy.model,
            &researcher_hash,
        )
        .await
        .expect("store embedding");

        let ctx = semantic_test_ctx(storage);
        let output = run_search(&handler, &ctx, "machine learning", &fixture).await;

        assert!(
            output
                .items
                .iter()
                .any(|entity| entity.display_name == "Riley Kim"),
            "semantic hit must surface despite zero substring overlap: {:?}",
            output
                .items
                .iter()
                .map(|entity| entity.display_name.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_search_entities_exact_name_still_wins_over_semantic() {
        use elephant_storage::Repository;

        let (storage, repo, handler, fixture) =
            semantic_test_setup(vec![("machine learning", vec![1.0, 0.0, 0.0])]).await;

        let researcher = semantic_test_entity("Riley Kim", Some("neural networks researcher"));
        let researcher_id = researcher.base.id.clone();
        let researcher_hash =
            elephant_storage::entity_profile_embedding_repository::entity_input_hash(&researcher);
        repo.create(researcher).await.expect("create researcher");
        repo.set_name_embedding(
            "test_space",
            &researcher_id,
            &[1.0, 0.0, 0.0],
            &fixture.policy.model,
            &researcher_hash,
        )
        .await
        .expect("store embedding");

        // The literal-named entity has NO embedding — exactness alone must
        // put it above the perfect-cosine semantic hit.
        let exact = semantic_test_entity("Machine Learning", None);
        repo.create(exact).await.expect("create exact");

        let ctx = semantic_test_ctx(storage);
        let output = run_search(&handler, &ctx, "machine learning", &fixture).await;

        assert_eq!(
            output
                .items
                .first()
                .map(|entity| entity.display_name.clone()),
            Some("Machine Learning".to_string()),
            "exact name match ranks first: {:?}",
            output
                .items
                .iter()
                .map(|entity| entity.display_name.clone())
                .collect::<Vec<_>>()
        );
        assert!(output
            .items
            .iter()
            .any(|entity| entity.display_name == "Riley Kim"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_search_entities_without_embeddings_matches_plain_handler() {
        use elephant_storage::Repository;

        // Provider present, but NO entity embeddings stored anywhere: the
        // provider-equipped handler must return byte-identical results to
        // the plain handler (today's behavior).
        let (storage, repo, handler, fixture) =
            semantic_test_setup(vec![("Riley", vec![1.0, 0.0, 0.0])]).await;

        repo.create(semantic_test_entity(
            "Riley Kim",
            Some("neural networks researcher"),
        ))
        .await
        .expect("create researcher");
        repo.create(semantic_test_entity("Riley Chen", None))
            .await
            .expect("create second");
        repo.create(semantic_test_entity("Morgan Lee", None))
            .await
            .expect("create third");

        let ctx = semantic_test_ctx(storage.clone());
        let plain_handler = SearchEntitiesHandler::new(storage);

        let params = serde_json::json!({ "query": "Riley", "limit": 10 });
        let blended = fixture
            .run(handler.execute(&ctx, params.clone()))
            .await
            .expect("blended search");
        let plain = plain_handler
            .execute(
                &ctx,
                serde_json::json!({"query":"Riley", "limit":10, "use_vector":false}),
            )
            .await
            .expect("plain search");

        assert_eq!(
            blended, plain,
            "absent embeddings: fallback must be identical to today's output"
        );
    }

    #[tokio::test]
    async fn empty_entity_vectors_skip_dispatch_until_live_vectors_exist_and_after_erasure() {
        let (storage, repo, handler, fixture) =
            semantic_test_setup(vec![("related subject", vec![1.0, 0.0, 0.0])]).await;
        let entity = semantic_test_entity("Riley Kim", Some("A research subject"));
        let id = entity.base.id.clone();
        let hash =
            elephant_storage::entity_profile_embedding_repository::entity_input_hash(&entity);
        repo.create(entity).await.expect("entity");
        let ctx = semantic_test_ctx(storage);
        let empty = run_search(&handler, &ctx, "related subject", &fixture).await;
        assert!(empty.items.is_empty());
        assert_eq!(fixture.physical_requests(), 0);

        repo.set_name_embedding(
            "test_space",
            &id,
            &[1.0, 0.0, 0.0],
            &fixture.policy.model,
            &hash,
        )
        .await
        .expect("publish current vector");
        let ranked = run_search(&handler, &ctx, "related subject", &fixture).await;
        assert!(ranked.items.iter().any(|row| row.base.id == id));
        assert_eq!(fixture.physical_requests(), 1);

        repo.hard_delete("test_space", &id)
            .await
            .expect("erase entity");
        let erased = run_search(&handler, &ctx, "related subject", &fixture).await;
        assert!(erased.items.is_empty());
        assert_eq!(
            fixture.physical_requests(),
            1,
            "erased vectors must not trigger inference"
        );
    }

    #[tokio::test]
    async fn stale_entity_vector_skips_dispatch_without_losing_lexical_results() {
        use elephant_storage::entity_profile_embedding_repository::{
            entity_input_hash, EntityProfileEmbeddingRepository,
        };

        for embedded in [false, true] {
            let entries = vec![
                ("Riley", vec![1.0, 0.0, 0.0]),
                ("related subject", vec![1.0, 0.0, 0.0]),
            ];
            let (storage, repo, handler, fixture) = if embedded {
                let storage = StorageClient::new_embedded()
                    .await
                    .expect("embedded storage");
                let repo = EntityRepository::new(storage.clone());
                let fixture = crate::retrieval_test_support::QueryFixture::install(
                    &storage,
                    "test_space",
                    entries,
                )
                .await;
                let handler = SearchEntitiesHandler::new(storage.clone());
                (storage, repo, handler, fixture)
            } else {
                semantic_test_setup(entries).await
            };
            let entity = semantic_test_entity("Riley Kim", Some("Old description"));
            let id = entity.base.id.clone();
            let hash = entity_input_hash(&entity);
            let mut entity = repo.create(entity).await.expect("entity");
            let original_version = entity.base.updated_at;
            repo.set_name_embedding(
                "test_space",
                &id,
                &[1.0, 0.0, 0.0],
                &fixture.policy.model,
                &hash,
            )
            .await
            .expect("publish vector");
            entity.summary = Some("Changed description invalidates the stored vector".into());
            let current = repo
                .update("test_space", &id, entity)
                .await
                .expect("change entity input");
            let current_hash = entity_input_hash(&current);
            assert_ne!(current_hash, hash, "the stored preimage really changed");
            if !embedded {
                assert_eq!(
                    current.base.updated_at, original_version,
                    "Surreal trusted update retains the supplied timestamp"
                );
            }
            assert!(EntityProfileEmbeddingRepository::new(storage.clone())
                .load("test_space", &fixture.policy.model, fixture.policy.dims)
                .await
                .expect("current vector read")
                .is_empty());
            let ctx = semantic_test_ctx(storage);
            let output = run_search(&handler, &ctx, "Riley", &fixture).await;
            assert!(output.items.iter().any(|row| row.base.id == id));
            assert_eq!(fixture.physical_requests(), 0);
            let stale_semantic = run_search(&handler, &ctx, "related subject", &fixture).await;
            assert!(stale_semantic.items.is_empty());
            assert_eq!(fixture.physical_requests(), 0);

            repo.set_name_embedding(
                "test_space",
                &id,
                &[1.0, 0.0, 0.0],
                &fixture.policy.model,
                &current_hash,
            )
            .await
            .expect("publish vector for current exact text");
            let refreshed = run_search(&handler, &ctx, "related subject", &fixture).await;
            assert!(refreshed.items.iter().any(|row| row.base.id == id));
            assert_eq!(fixture.physical_requests(), 1);
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn empty_entity_vectors_do_not_bypass_revoked_query_approval() {
        use elephant_types::provider_binding::ModelOperation;
        let (storage, _, handler, fixture) = semantic_test_setup(vec![]).await;
        let configs = SpaceConfigRepository::new(storage.clone());
        let before = configs
            .get_by_space_id("test_space")
            .await
            .expect("read config")
            .expect("config");
        let mut revoked = before.clone();
        let processing = revoked.processing.as_mut().expect("processing");
        processing.generation += 1;
        processing
            .bindings
            .get_mut(&ModelOperation::QueryEmbedding)
            .expect("binding")
            .revoked = true;
        configs
            .replace_authorized(&before, revoked)
            .await
            .expect("revoke binding");
        let ctx = semantic_test_ctx(storage);
        let result = fixture
            .run(handler.execute(&ctx, serde_json::json!({"query":"Riley", "limit":10})))
            .await;
        assert!(matches!(result, Err(McpError::Public(_))));
        assert_eq!(fixture.physical_requests(), 0);
    }

    #[test]
    fn entity_vector_availability_preserves_late_authorization_failures() {
        use elephant_storage::StorageError;
        use elephant_types::public_error::ErrorCode;
        for cause in [
            "provider_authorization: query approval changed",
            "provider_authorization: entity query approval missing",
            "provider_authorization: stale query profile",
            "provider_authorization: binding revoked",
        ] {
            let error = entity_vector_availability(Err(StorageError::Conflict(cause.into())))
                .expect_err("late authorization change must not fall back");
            assert!(
                matches!(error, McpError::Public(error) if error.error_code == ErrorCode::ProviderUnapproved)
            );
        }
        assert!(!entity_vector_availability(Ok(vec![])).expect("empty index"));
        assert!(!entity_vector_availability(Err(StorageError::QueryError(
            "storage unavailable".into()
        )))
        .expect("existing ordinary storage fallback"));
    }
}
