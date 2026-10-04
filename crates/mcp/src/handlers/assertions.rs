#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Assertion MCP tool handlers.
//!
//! Implements:
//! - `list_assertions` - List assertions with filtering
//! - `get_assertion` - Get single assertion by ID

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use tracing::warn;

use elephant_embeddings::EmbeddingProviderCache;
use elephant_storage::{
    parse_id, AssertionRepository, EntityRepository, Repository, SearchCriteria, SearchRepository,
    SpaceConfigRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{Assertion, ResolutionStatus};
use elephant_types::records::entity::Entity;
use elephant_types::{Link, RecordId};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `list_assertions` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAssertionsInput {
    /// Filter by resolution status.
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by source ID.
    #[serde(default)]
    pub source_id: Option<String>,
    /// Filter by predicate (raw string).
    #[serde(default)]
    pub predicate: Option<String>,
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

/// Output for `list_assertions` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAssertionsOutput {
    /// Search results.
    pub items: Vec<Assertion>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
}

/// Input for `get_assertion` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAssertionInput {
    /// Assertion ID.
    pub id: String,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `list_assertions` tool.
pub struct ListAssertionsHandler {
    repo: AssertionRepository,
}

impl ListAssertionsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: AssertionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListAssertionsHandler {
    fn name(&self) -> &'static str {
        "list_assertions"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List assertions with optional filtering by status, source, or predicate.\n\n",
            "Example:\n",
            r#"{ "status": "pending", "limit": 10 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter by resolution status (pending, resolved, failed, skipped)",
                    "enum": ["pending", "resolved", "partial", "abstained", "failed", "superseded"]
                },
                "source_id": {
                    "type": "string",
                    "description": "Filter by source ID"
                },
                "predicate": {
                    "type": "string",
                    "description": "Filter by predicate (raw string)"
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
        let input: ListAssertionsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) =
            crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:assertion")
        {
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
                parse_id(c, TableName::Assertion)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?,
            )
        } else {
            None
        };

        let page_limit = input.limit.min(100);
        let fetch_limit = page_limit.saturating_add(1);
        let mut scan_cursor = cursor;
        let mut filtered = Vec::new();

        loop {
            let items = self
                .repo
                .list(&space_id, fetch_limit, scan_cursor.as_ref())
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            if items.is_empty() {
                break;
            }

            let batch_len = items.len();
            scan_cursor = items.last().map(|a| a.base.id.clone());
            let items = ctx
                .authz
                .filter_records(&ctx.principal, items, "read:assertion");

            for assertion in items {
                if !assertion_matches_list_filter(&assertion, &input) {
                    continue;
                }
                if is_source_inventory_metadata_assertion(&assertion) {
                    continue;
                }
                filtered.push(assertion);
                if filtered.len() > page_limit {
                    break;
                }
            }

            if filtered.len() > page_limit || batch_len < fetch_limit {
                break;
            }
        }

        let has_more = filtered.len() > page_limit;
        if filtered.len() > page_limit {
            filtered.truncate(page_limit);
        }
        let next_cursor = if has_more {
            filtered.last().map(|a| {
                a.base
                    .id
                    .to_string()
                    .strip_prefix("assertion:")
                    .map_or_else(|| a.base.id.to_string(), std::string::ToString::to_string)
            })
        } else {
            None
        };

        let output = ListAssertionsOutput {
            items: filtered,
            next_cursor,
            has_more,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

fn assertion_matches_list_filter(assertion: &Assertion, input: &ListAssertionsInput) -> bool {
    if let Some(ref status) = input.status {
        let expected = match status.as_str() {
            "pending" => ResolutionStatus::Pending,
            "resolved" => ResolutionStatus::Resolved,
            "failed" => ResolutionStatus::Failed,
            "abstained" => ResolutionStatus::Abstained,
            "partial" => ResolutionStatus::Partial,
            "superseded" => ResolutionStatus::Superseded,
            _ => return true,
        };
        if assertion.resolution_status != expected {
            return false;
        }
    }
    if let Some(ref predicate) = input.predicate {
        if !assertion.predicate_raw.contains(predicate) {
            return false;
        }
    }
    if let Some(ref source_id) = input.source_id {
        let has_source = assertion
            .base
            .provenance
            .sources
            .iter()
            .any(|s| s.id().to_string().contains(source_id));
        if !has_source {
            return false;
        }
    }
    true
}

/// Handler for `get_assertion` tool.
pub struct GetAssertionHandler {
    repo: AssertionRepository,
}

impl GetAssertionHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: AssertionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetAssertionHandler {
    fn name(&self) -> &'static str {
        "get_assertion"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a single assertion by ID with related source and predicate info.\n\n",
            "Example:\n",
            r#"{ "id": "assertion:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Assertion ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetAssertionInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) =
            crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:assertion")
        {
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

        let rid = parse_id(&input.id, TableName::Assertion)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        match self.repo.get(&space_id, &rid).await {
            Ok(Some(assertion)) => {
                if let Err(message) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
                {
                    let response: ToolResponse<()> = ToolResponse::error(message);
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(assertion);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<Assertion> =
                    ToolResponse::error(format!("Assertion not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Input for `search_assertions` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchAssertionsInput {
    /// Free-text search query (searches `subject_text`, `predicate_raw`, `object_text`).
    /// Optional when `entity_id` is given.
    #[serde(default)]
    pub query: Option<String>,
    /// Return assertions whose resolved subject or object is this entity —
    /// a single indexed-style `WHERE` query instead of the bounded text scan.
    /// Callers that already know the entity (profile panels, graph views)
    /// should always prefer this: the text scan pages the assertion table in
    /// batches and is orders of magnitude slower on populated spaces.
    #[serde(default)]
    pub entity_id: Option<String>,
    /// Filter by resolution status.
    #[serde(default)]
    pub status: Option<String>,
    /// Filters.
    #[serde(default)]
    pub filters: Option<AssertionSearchFilters>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Search filters for assertions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AssertionSearchFilters {
    /// Filter by attribution. Assertions carry no attribution themselves —
    /// they filter via their linked entities: an assertion passes when its
    /// resolved subject or object entity has `metadata.angel_protectees`
    /// containing this value, or has no `angel_protectees` key at all
    /// (legacy/family-wide). Assertions with no resolved entity links are
    /// unattributed and pass every filter, mirroring the entity union rule.
    #[serde(default)]
    pub attributed_to: Option<String>,
}

/// Output for `search_assertions` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchAssertionsOutput {
    /// Search results.
    pub items: Vec<Assertion>,
    /// Total matches found.
    pub total: usize,
}

/// Handler for `search_assertions` tool.
pub struct SearchAssertionsHandler {
    repo: AssertionRepository,
    entity_repo: EntityRepository,
    space_config_repo: SpaceConfigRepository,
    provider_cache: EmbeddingProviderCache,
    /// Rows the fallback table scan reads at most.
    scan_window: usize,
}

/// Default bound on the fallback table scan.
const DEFAULT_SCAN_WINDOW: usize = 2_000;

/// Ranked candidates taken from the assertion search index per query.
const MAX_INDEX_CANDIDATES: usize = 500;

impl SearchAssertionsHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self::with_provider_cache(storage, EmbeddingProviderCache::new())
    }

    /// Creates a handler with a pre-seeded embedding provider cache (tests
    /// inject controlled providers here).
    #[must_use]
    pub fn with_provider_cache(
        storage: StorageClient,
        provider_cache: EmbeddingProviderCache,
    ) -> Self {
        Self {
            repo: AssertionRepository::new(storage.clone()),
            entity_repo: EntityRepository::new(storage.clone()),
            space_config_repo: SpaceConfigRepository::new(storage),
            provider_cache,
            scan_window: DEFAULT_SCAN_WINDOW,
        }
    }

    /// Bounds the fallback table scan (tests shrink it).
    #[cfg(test)]
    const fn with_scan_window(mut self, scan_window: usize) -> Self {
        self.scan_window = scan_window;
        self
    }

    /// Entities semantically close to the text query, best first. Assertions
    /// carry no embeddings of their own — semantic recall works through
    /// their resolved subject/object entities. Returns `None` whenever
    /// semantic ranking cannot contribute (provider unavailable or failing,
    /// no stored entity vectors), leaving the text path's behavior
    /// unchanged.
    async fn semantic_entity_anchors(
        &self,
        space_id: &str,
        query: &str,
    ) -> Option<Vec<(RecordId, f32)>> {
        let policy =
            match super::docs::resolve_embedding_policy(&self.space_config_repo, space_id).await {
                Ok(policy) => policy,
                Err(err) => {
                    warn!("Semantic assertion recall unavailable (policy): {err}");
                    return None;
                }
            };
        let provider = self
            .provider_cache
            .get_or_create(policy, super::docs::build_embedding_provider)
            .await;
        let vectors = match provider.embed(&[query.to_string()]).await {
            Ok(vectors) => vectors,
            Err(err) => {
                warn!("Semantic assertion recall unavailable (embed): {err}");
                return None;
            }
        };
        let vector = vectors.into_iter().next()?;
        let hits = match self
            .entity_repo
            .vector_search_entities(space_id, &vector, SEMANTIC_ANCHOR_ENTITIES)
            .await
        {
            Ok(hits) => hits,
            Err(err) => {
                warn!("Semantic assertion recall unavailable (vector search): {err}");
                return None;
            }
        };
        let anchors: Vec<(RecordId, f32)> = hits
            .into_iter()
            .filter(|hit| hit.score >= MIN_SEMANTIC_ANCHOR_SCORE)
            .map(|hit| (hit.entity_id, hit.score))
            .collect();
        (!anchors.is_empty()).then_some(anchors)
    }
}

/// Maximum entities used as semantic anchors for assertion recall.
const SEMANTIC_ANCHOR_ENTITIES: usize = 3;
/// Minimum cosine similarity for an entity to anchor assertion recall —
/// vector search always returns the nearest neighbors whether or not they
/// are relevant.
///
/// Measured on a live household space 2026-07-30: at 0.30 this floor was low
/// enough that a deliberate nonsense query (`zzzqqqxyzzyplugh`) still returned
/// four assertions — `#general-2 | observed_at`, `Discord | has_state` and
/// similar — because an arbitrary string embeds to *something* and modern
/// embeddings put unrelated text comfortably above 0.3 cosine. The text path
/// is correctly gated on `score > 0`, so those hits arrived purely through
/// semantic anchoring, and the tool looked like it was ignoring the query
/// entirely. That is worse than returning nothing: an agent cannot tell a
/// confident wrong answer from no answer, and it made every hit count from
/// this tool untrustworthy while auditing the knowledge base.
///
/// 0.62 keeps genuine paraphrase recall ("payment due" -> "invoice deadline")
/// while rejecting unrelated neighbours. If this is ever lowered again, add a
/// test that a nonsense query returns zero results FIRST.
const MIN_SEMANTIC_ANCHOR_SCORE: f32 = 0.62;

#[async_trait]
impl ToolHandler for SearchAssertionsHandler {
    fn name(&self) -> &'static str {
        "search_assertions"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search assertions by text query across subject, predicate, and object fields, ",
            "or fetch assertions linked to a resolved entity via entity_id (much faster when ",
            "the entity is already known — prefer it over query whenever you have the id). ",
            "filters.attributed_to scopes results via the linked entities' ",
            "metadata.angel_protectees (assertions carry no attribution of their own). ",
            "Text queries also recall assertions whose linked entities match the query ",
            "semantically when entity embeddings are available; text matches rank first.\n\n",
            "Example:\n",
            r#"{ "query": "works at", "limit": 10 }"#,
            "\n",
            r#"{ "entity_id": "entity:abc123", "limit": 10, "filters": { "attributed_to": "child-a" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Free-text search query (optional when entity_id is given)"
                },
                "entity_id": {
                    "type": "string",
                    "description": "Return assertions whose resolved subject or object is this entity — prefer this over query when the entity is known (single indexed query vs bounded table scan)"
                },
                "status": {
                    "type": "string",
                    "description": "Filter by resolution status",
                    "enum": ["pending", "resolved", "partial", "abstained", "failed", "superseded"]
                },
                "filters": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "attributed_to": {
                            "type": "string",
                            "description": "Assertions filter via their linked entities: return assertions whose resolved subject or object entity has metadata.angel_protectees containing this value; entities without an angel_protectees key — and assertions with no resolved entity links — are also included"
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
        let input: SearchAssertionsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(message) =
            crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:assertion")
        {
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

        let output_limit = input.limit.min(100);
        let attributed_to = input
            .filters
            .as_ref()
            .and_then(|filters| filters.attributed_to.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());

        // Fast path: the caller already knows the entity. One WHERE query on
        // resolved subject/object links — no text scan over the table.
        if let Some(entity_id_raw) = input
            .entity_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let entity_id = parse_id(entity_id_raw, TableName::Entity)
                .map_err(|e| McpError::InvalidParameters(format!("invalid entity_id: {e}")))?;
            // Over-fetch a little so status filtering can't starve the page.
            let fetch_limit = output_limit.saturating_mul(4).clamp(50, 400);
            let assertions = self
                .repo
                .find_by_resolved_entity(&space_id, &entity_id, fetch_limit)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            let assertions = ctx
                .authz
                .filter_records(&ctx.principal, assertions, "read:assertion");
            let mut entity_link_cache: HashMap<RecordId, CachedEntityLink> = HashMap::new();
            let mut items = Vec::new();
            for assertion in assertions {
                if !assertion_matches_search_status(&assertion, input.status.as_deref()) {
                    continue;
                }
                let Some(assertion) = self
                    .canonicalize_and_filter_assertion(
                        &space_id,
                        assertion,
                        attributed_to,
                        &mut entity_link_cache,
                    )
                    .await?
                else {
                    continue;
                };
                items.push(assertion);
            }
            let total = items.len();
            items.truncate(output_limit);
            let output = SearchAssertionsOutput { items, total };
            let response = ToolResponse::success(output);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let Some(query) = input
            .query
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Err(McpError::InvalidParameters(
                "provide `query` (text search) or `entity_id` (assertions linked to an entity)"
                    .to_string(),
            ));
        };
        let query_lower = query.to_lowercase();
        let query_terms = assertion_search_terms(&query_lower);

        let candidate_target = output_limit.saturating_mul(10).clamp(50, 300);
        let mut scored: Vec<(usize, Assertion)> = Vec::new();
        let mut considered: HashSet<String> = HashSet::new();
        let mut entity_link_cache: HashMap<RecordId, CachedEntityLink> = HashMap::new();

        // Candidates come from the BM25 index over subject, predicate and
        // object text: one ranked page, re-scored below. Scanning the table in
        // id order capped recall at the first `scan_window` rows, so most of a
        // large space was unsearchable and every query paid for the full scan.
        // One page rather than several: the ranked query re-scores every match
        // per page and the repository adds a count query to each.
        let criteria = SearchCriteria::new()
            .with_query(query)
            .with_space_id(space_id.clone())
            .with_limit(MAX_INDEX_CANDIDATES);
        let index_failed = match self.repo.search(&criteria).await {
            Ok(page) => {
                let items =
                    ctx.authz
                        .filter_records(&ctx.principal, page.results, "read:assertion");
                for assertion in items {
                    if !considered.insert(assertion.base.id.to_string()) {
                        continue;
                    }
                    if let Some(hit) = self
                        .score_search_candidate(
                            &space_id,
                            assertion,
                            &query_lower,
                            &query_terms,
                            input.status.as_deref(),
                            attributed_to,
                            &mut entity_link_cache,
                        )
                        .await?
                    {
                        scored.push(hit);
                    }
                }
                false
            }
            Err(err) => {
                // A space without the search indexes still gets the bounded
                // scan below rather than an error.
                warn!("Assertion index search failed, falling back to scan: {err}");
                true
            }
        };

        // The index does not cover `object_literal`. Literal lookups (dates,
        // amounts, phone numbers) carry digits, and their tokens often hit
        // unrelated text, so they also get the bounded scan. Word queries
        // with no index match return empty without paying for it.
        let literal_shaped = query.chars().any(|ch| ch.is_ascii_digit());
        if index_failed || literal_shaped {
            let max_scan_records = self.scan_window;
            let fetch_limit = max_scan_records.clamp(1, 500);
            let mut scan_cursor: Option<RecordId> = None;
            let mut scanned = 0usize;
            loop {
                let page = self
                    .repo
                    .list(&space_id, fetch_limit, scan_cursor.as_ref())
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                if page.is_empty() {
                    break;
                }

                let batch_len = page.len();
                scanned = scanned.saturating_add(batch_len);
                scan_cursor = page.last().map(|assertion| assertion.base.id.clone());
                let items = ctx
                    .authz
                    .filter_records(&ctx.principal, page, "read:assertion");

                for assertion in items {
                    if !considered.insert(assertion.base.id.to_string()) {
                        continue;
                    }
                    if let Some(hit) = self
                        .score_search_candidate(
                            &space_id,
                            assertion,
                            &query_lower,
                            &query_terms,
                            input.status.as_deref(),
                            attributed_to,
                            &mut entity_link_cache,
                        )
                        .await?
                    {
                        scored.push(hit);
                    }
                }

                if batch_len < fetch_limit
                    || scanned >= max_scan_records
                    || scored.len() >= candidate_target
                {
                    break;
                }
            }
        }
        scored.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.base.created_at.cmp(&left.base.created_at))
        });
        let mut seen = HashSet::new();
        let deduped: Vec<(usize, Assertion)> = scored
            .into_iter()
            .filter(|(_, assertion)| seen.insert(assertion_search_result_key(assertion)))
            .collect();
        let mut total = deduped.len();
        let mut items: Vec<Assertion> = deduped
            .into_iter()
            .map(|(_, assertion)| assertion)
            .take(output_limit)
            .collect();

        // Semantic recall through entity embeddings: entities close to the
        // query pull in their linked assertions via the same indexed lookup
        // the entity_id fast path uses. Text-ranked hits keep their order and
        // precedence — semantic hits only fill the remaining page, ordered by
        // entity similarity. With no stored embeddings (or provider down)
        // `semantic_entity_anchors` is None and this block is a no-op,
        // leaving today's behavior untouched.
        if items.len() < output_limit {
            if let Some(anchors) = self.semantic_entity_anchors(&space_id, query).await {
                let mut present: HashSet<String> = items
                    .iter()
                    .map(|assertion| assertion.base.id.to_string())
                    .collect();
                'anchors: for (entity_id, _) in anchors {
                    let linked = match self
                        .repo
                        .find_by_resolved_entity(&space_id, &entity_id, SEMANTIC_LINK_FETCH_LIMIT)
                        .await
                    {
                        Ok(linked) => linked,
                        Err(err) => {
                            warn!("Semantic assertion recall failed for {entity_id}: {err}");
                            continue;
                        }
                    };
                    let linked = ctx
                        .authz
                        .filter_records(&ctx.principal, linked, "read:assertion");
                    for assertion in linked {
                        if !present.insert(assertion.base.id.to_string()) {
                            continue;
                        }
                        if !assertion_matches_search_status(&assertion, input.status.as_deref()) {
                            continue;
                        }
                        if is_source_inventory_metadata_assertion(&assertion) {
                            continue;
                        }
                        let Some(assertion) = self
                            .canonicalize_and_filter_assertion(
                                &space_id,
                                assertion,
                                attributed_to,
                                &mut entity_link_cache,
                            )
                            .await?
                        else {
                            continue;
                        };
                        if !seen.insert(assertion_search_result_key(&assertion)) {
                            continue;
                        }
                        total += 1;
                        items.push(assertion);
                        if items.len() >= output_limit {
                            break 'anchors;
                        }
                    }
                }
            }
        }

        let output = SearchAssertionsOutput { items, total };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Linked assertions fetched per semantic entity anchor.
const SEMANTIC_LINK_FETCH_LIMIT: usize = 500;

/// Per-call cache entry for a resolved assertion entity link: the merge
/// redirect (if any) plus whether the canonical entity passes the requested
/// `attributed_to` filter. One entity fetch per distinct link id per call.
#[derive(Clone)]
struct CachedEntityLink {
    /// Survivor id when the linked entity is merged into a live survivor.
    redirect: Option<RecordId>,
    /// Whether the canonical (post-redirect) entity passes the
    /// `attributed_to` filter. Always `true` when no filter was requested.
    attributed: bool,
}

impl SearchAssertionsHandler {
    /// Scores one text-search candidate and applies the status, inventory,
    /// canonicalisation and `attributed_to` filters. Returns `None` when the
    /// candidate does not match or is filtered out.
    #[allow(clippy::too_many_arguments)]
    async fn score_search_candidate(
        &self,
        space_id: &str,
        assertion: Assertion,
        query_lower: &str,
        query_terms: &[String],
        status: Option<&str>,
        attributed_to: Option<&str>,
        entity_link_cache: &mut HashMap<RecordId, CachedEntityLink>,
    ) -> McpResult<Option<(usize, Assertion)>> {
        let Some(score) = search_assertion_score_if_match(&assertion, query_lower, query_terms)
        else {
            return Ok(None);
        };
        if !assertion_matches_search_status(&assertion, status) {
            return Ok(None);
        }
        Ok(self
            .canonicalize_and_filter_assertion(
                space_id,
                assertion,
                attributed_to,
                entity_link_cache,
            )
            .await?
            .map(|assertion| (score, assertion)))
    }

    /// Canonicalizes both entity links and applies the `attributed_to`
    /// filter. Returns `None` when a filter is present and no linked entity
    /// passes it. An assertion with no resolved entity links is unattributed
    /// and passes every filter — the same union semantics as an entity
    /// without an `angel_protectees` key.
    async fn canonicalize_and_filter_assertion(
        &self,
        space_id: &str,
        mut assertion: Assertion,
        attributed_to: Option<&str>,
        entity_link_cache: &mut HashMap<RecordId, CachedEntityLink>,
    ) -> McpResult<Option<Assertion>> {
        let subject = self
            .resolve_entity_link(
                space_id,
                assertion.subject_entity.as_ref(),
                attributed_to,
                entity_link_cache,
            )
            .await?;
        let object = self
            .resolve_entity_link(
                space_id,
                assertion.object_entity.as_ref(),
                attributed_to,
                entity_link_cache,
            )
            .await?;

        if attributed_to.is_some() && (subject.is_some() || object.is_some()) {
            let passes = subject.as_ref().is_some_and(|(_, attributed)| *attributed)
                || object.as_ref().is_some_and(|(_, attributed)| *attributed);
            if !passes {
                return Ok(None);
            }
        }

        assertion.subject_entity = subject.map(|(link, _)| link);
        assertion.object_entity = object.map(|(link, _)| link);
        Ok(Some(assertion))
    }

    /// Resolves a link to its canonical (post-merge) form and evaluates the
    /// `attributed_to` filter against the canonical entity.
    async fn resolve_entity_link(
        &self,
        space_id: &str,
        link: Option<&Link<Entity>>,
        attributed_to: Option<&str>,
        entity_link_cache: &mut HashMap<RecordId, CachedEntityLink>,
    ) -> McpResult<Option<(Link<Entity>, bool)>> {
        let Some(link) = link else {
            return Ok(None);
        };
        if let Some(cached) = entity_link_cache.get(link.id()) {
            let canonical = cached
                .redirect
                .as_ref()
                .map_or_else(|| link.clone(), |entity_id| Link::new(entity_id.clone()));
            return Ok(Some((canonical, cached.attributed)));
        }
        let entity = self
            .entity_repo
            .get(space_id, link.id())
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let Some(entity) = entity else {
            // Dangling link: nothing to evaluate attribution against, so it
            // stays visible (unattributed), matching the entity union rule.
            entity_link_cache.insert(
                link.id().clone(),
                CachedEntityLink {
                    redirect: None,
                    attributed: true,
                },
            );
            return Ok(Some((link.clone(), true)));
        };
        if entity.is_merged() {
            if let Some(redirect_to) = entity.redirect_to.as_ref() {
                if let Some(survivor_entity) = self
                    .entity_repo
                    .get(space_id, redirect_to.id())
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?
                {
                    // Attribution follows the survivor — that is the entity
                    // the returned link points at.
                    let attributed = entity_passes_attribution(&survivor_entity, attributed_to);
                    let survivor = redirect_to.id().clone();
                    entity_link_cache.insert(
                        link.id().clone(),
                        CachedEntityLink {
                            redirect: Some(survivor.clone()),
                            attributed,
                        },
                    );
                    return Ok(Some((Link::new(survivor), attributed)));
                }
            }
        }
        let attributed = entity_passes_attribution(&entity, attributed_to);
        entity_link_cache.insert(
            link.id().clone(),
            CachedEntityLink {
                redirect: None,
                attributed,
            },
        );
        Ok(Some((link.clone(), attributed)))
    }
}

/// Mirrors the storage-side entity `attributed_to` union rule: entities with
/// no `angel_protectees` metadata key are family-wide and pass every filter;
/// an explicit list must contain the id (an empty list matches none).
pub(crate) fn entity_passes_attribution(entity: &Entity, attributed_to: Option<&str>) -> bool {
    let Some(attributed_to) = attributed_to else {
        return true;
    };
    entity
        .metadata
        .get("angel_protectees")
        .and_then(serde_json::Value::as_array)
        .is_none_or(|protectees| {
            protectees
                .iter()
                .any(|value| value.as_str() == Some(attributed_to))
        })
}

fn search_assertion_score_if_match(
    assertion: &Assertion,
    query_lower: &str,
    query_terms: &[String],
) -> Option<usize> {
    if is_source_inventory_metadata_assertion(assertion) {
        return None;
    }
    // Text search across the whole assertion. Agent queries often combine
    // subject + predicate + object terms, e.g. "Ada reading homework";
    // matching field-by-field hides facts that are present but split across
    // those fields.
    let text = assertion_search_text(assertion);
    let score = assertion_search_score(&text, query_lower, query_terms);
    (score > 0).then_some(score)
}

fn assertion_search_text(assertion: &Assertion) -> String {
    let mut parts = vec![
        assertion.subject_text.as_str(),
        assertion.predicate_raw.as_str(),
    ];
    if let Some(object_text) = assertion.object_text.as_deref() {
        parts.push(object_text);
    }
    let object_literal = assertion.object_literal.as_ref().map(Value::to_string);
    if let Some(object_literal) = object_literal.as_deref() {
        parts.push(object_literal);
    }
    parts.join(" ").to_lowercase()
}

fn assertion_matches_search_status(assertion: &Assertion, status: Option<&str>) -> bool {
    let Some(status) = status else {
        return true;
    };
    let expected = match status {
        "pending" => ResolutionStatus::Pending,
        "resolved" => ResolutionStatus::Resolved,
        "failed" => ResolutionStatus::Failed,
        "abstained" => ResolutionStatus::Abstained,
        "partial" => ResolutionStatus::Partial,
        "superseded" => ResolutionStatus::Superseded,
        _ => return true,
    };
    assertion.resolution_status == expected
}

fn normalize_assertion_label(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn is_source_inventory_subject(subject: &str) -> bool {
    let normalized = normalize_assertion_label(subject);
    [
        "email message ",
        "mail message ",
        "calendar event ",
        "source document ",
        "source excerpt ",
        "source file ",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
}

fn is_provider_catalog_predicate(predicate: &str) -> bool {
    let normalized = normalize_assertion_label(predicate).replace('_', " ");
    matches!(
        normalized.as_str(),
        "account"
            | "collection"
            | "created"
            | "created at"
            | "created time"
            | "exported"
            | "exported at"
            | "external id"
            | "file id"
            | "id"
            | "ingested"
            | "ingested at"
            | "last modified"
            | "live search query"
            | "mime type"
            | "modified"
            | "modified at"
            | "modified time"
            | "raw metadata"
            | "received"
            | "received at"
            | "recorded"
            | "recorded at"
            | "ref"
            | "source id"
            | "source type"
            | "timestamp"
            | "title"
            | "web link"
            | "web view link"
            | "url"
    )
}

fn is_source_inventory_metadata_assertion(assertion: &Assertion) -> bool {
    is_source_inventory_subject(&assertion.subject_text)
        && is_provider_catalog_predicate(&assertion.predicate_raw)
}

fn assertion_search_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| term.len() > 1)
        .map(str::to_string)
        .collect()
}

fn assertion_search_score(text: &str, query: &str, terms: &[String]) -> usize {
    if query.is_empty() {
        return 0;
    }
    if text.contains(query) {
        return terms.len().saturating_add(1).max(1) * 10;
    }
    terms
        .iter()
        .filter(|term| text.contains(term.as_str()))
        .count()
}

fn assertion_search_result_key(assertion: &Assertion) -> String {
    [
        assertion.subject_entity.as_ref().map_or_else(
            || normalize_assertion_label(&assertion.subject_text),
            |entity| entity.id().to_string(),
        ),
        normalize_predicate_key(
            assertion
                .predicate_id
                .as_deref()
                .unwrap_or(&assertion.predicate_raw),
        ),
        assertion
            .object_entity
            .as_ref()
            .map(|entity| entity.id().to_string())
            .or_else(|| {
                assertion
                    .object_text
                    .as_deref()
                    .map(normalize_assertion_label)
            })
            .unwrap_or_else(|| {
                assertion
                    .object_literal
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_default()
            }),
        format!("{:?}", assertion.resolution_status),
    ]
    .join("\u{1f}")
}

fn normalize_predicate_key(value: &str) -> String {
    normalize_assertion_label(&value.replace(['_', '-'], " "))
        .split_whitespace()
        .filter(|term| !matches!(*term, "is" | "a" | "an" | "the" | "of" | "or" | "to"))
        .collect::<Vec<_>>()
        .join(" ")
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
    use elephant_types::records::assertion::{AssertionRetryState, EvidenceSpan};
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::{EntityStatus, EntityType};
    use elephant_types::{Link, RecordId};
    use std::str::FromStr;

    #[test]
    fn test_list_assertions_input_defaults() {
        let input: ListAssertionsInput = serde_json::from_str("{}").unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.status.is_none());
        assert!(input.source_id.is_none());
    }

    #[test]
    fn test_search_assertions_input_defaults() {
        let input: SearchAssertionsInput = serde_json::from_str(r#"{"query": "test"}"#).unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.status.is_none());
        assert!(input.entity_id.is_none());
    }

    #[test]
    fn test_search_assertions_input_entity_id_only() {
        let input: SearchAssertionsInput =
            serde_json::from_str(r#"{"entity_id": "entity:abc"}"#).unwrap();
        assert!(input.query.is_none());
        assert_eq!(input.entity_id.as_deref(), Some("entity:abc"));
        assert_eq!(input.limit, 50);
        assert!(input.filters.is_none());
    }

    #[test]
    fn test_search_assertions_input_parses_attribution_filter() {
        let input: SearchAssertionsInput = serde_json::from_str(
            r#"{"entity_id": "entity:abc", "filters": {"attributed_to": "child-a"}}"#,
        )
        .unwrap();
        assert_eq!(
            input
                .filters
                .and_then(|filters| filters.attributed_to)
                .as_deref(),
            Some("child-a")
        );
    }

    /// In-memory storage with the production assertion search indexes
    /// (migration 012), so text-path tests run through the index path.
    async fn indexed_memory_storage() -> StorageClient {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query(concat!(
                "DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase, ascii;\n",
                include_str!("../../../migrations/sql/012_assertion_search.surql")
            ))
            .await
            .expect("define assertion search indexes");
        storage
    }

    async fn create_test_assertion(
        repo: &AssertionRepository,
        id: &str,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Assertion {
        let id = RecordId::from_str(id).expect("valid assertion id");
        let mut base = BaseFields::new(id.clone(), "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let assertion = Assertion {
            base,
            subject_text: subject.to_string(),
            predicate_raw: predicate.to_string(),
            predicate_id: None,
            object_text: Some(object.to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: format!("assertion-{id}"),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        repo.create(assertion).await.expect("create assertion")
    }

    fn create_test_entity(id: &str, name: &str) -> Entity {
        let id = RecordId::from_str(id).expect("valid entity id");
        let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        Entity::new(base, EntityType::new("person"), name.to_string())
    }

    #[test]
    fn test_assertion_search_scores_matching_terms_for_person_query() {
        let terms = assertion_search_terms("riley example");

        assert!(
            assertion_search_score("morgan example owns folder", "riley example", &terms,) > 0,
            "partial matches should remain discoverable for broad agent queries"
        );
        assert!(assertion_search_score("riley example owns folder", "riley example", &terms,) > 0);
    }

    #[test]
    fn test_source_inventory_metadata_assertions_are_hidden_from_default_reads() {
        let inventory = Assertion {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Assertion),
                "default".to_string(),
                "tester".to_string(),
            ),
            subject_text: "Source document Ada Example".to_string(),
            predicate_raw: "last modified".to_string(),
            predicate_id: None,
            object_text: None,
            object_literal: Some(json!("2026-05-17")),
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "inventory".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        let mut ordinary = inventory.clone();
        ordinary.subject_text = "Ada Example".to_string();

        assert!(is_source_inventory_metadata_assertion(&inventory));
        assert!(!is_source_inventory_metadata_assertion(&ordinary));
    }

    #[tokio::test]
    async fn test_list_assertions_requires_read_assertion_scope() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = ListAssertionsHandler::new(storage.clone());
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
        assert!(decoded.error.expect("error").contains("read:assertion"));
    }

    #[tokio::test]
    #[allow(clippy::items_after_statements)]
    async fn test_list_assertions_filter_scans_past_first_page() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = ListAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        async fn create_assertion(
            repo: &AssertionRepository,
            id: &str,
            predicate: &str,
        ) -> Assertion {
            let id = RecordId::from_str(id).expect("valid assertion id");
            let assertion_key = format!("assertion-{predicate}-{id}");
            let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
            base.provenance = Provenance::default();
            base.subjects_state = SubjectsState::Complete;
            let assertion = Assertion {
                base,
                subject_text: "Subject".to_string(),
                predicate_raw: predicate.to_string(),
                predicate_id: None,
                object_text: Some("Object".to_string()),
                object_literal: None,
                evidence: EvidenceSpan {
                    revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                    node: None,
                    start: 0,
                    end: 1,
                    quote_hash: "hash".to_string(),
                },
                source_event_time: None,
                observed_at: Utc::now(),
                asserted_at: Utc::now(),
                assertion_key,
                resolution_status: ResolutionStatus::Resolved,
                resolution_method: None,
                resolution_confidence: None,
                retry_state: AssertionRetryState::default(),
                subject_entity: None,
                object_entity: None,
                confidence: 1.0,
                subject_entity_type: None,
                object_entity_type: None,
                significance: None,
                lifecycle_tier: "active".to_string(),
            };
            repo.create(assertion).await.expect("create assertion")
        }

        for index in 0..110 {
            create_assertion(
                &repo,
                &format!("assertion:00000000-0000-0000-0000-{index:012x}"),
                "unrelated",
            )
            .await;
        }
        create_assertion(
            &repo,
            "assertion:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "depends_on",
        )
        .await;

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "predicate": "depends_on",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<ListAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.items.len(), 1);
        assert_eq!(data.items[0].predicate_raw, "depends_on");
    }

    #[tokio::test]
    async fn test_search_assertions_preserves_total_beyond_page_size() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        for subject in ["Alice", "Bob"] {
            let id = RecordId::new_v4(TableName::Assertion);
            let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
            base.provenance = Provenance::default();
            base.subjects_state = SubjectsState::Complete;
            let assertion = Assertion {
                base,
                subject_text: subject.to_string(),
                predicate_raw: "knows".to_string(),
                predicate_id: None,
                object_text: Some("Wonderland".to_string()),
                object_literal: None,
                evidence: EvidenceSpan {
                    revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                    node: None,
                    start: 0,
                    end: 1,
                    quote_hash: "hash".to_string(),
                },
                source_event_time: None,
                observed_at: Utc::now(),
                asserted_at: Utc::now(),
                assertion_key: format!("assertion-{subject}"),
                resolution_status: ResolutionStatus::Pending,
                resolution_method: None,
                resolution_confidence: None,
                retry_state: AssertionRetryState::default(),
                subject_entity: None,
                object_entity: None,
                confidence: 1.0,
                subject_entity_type: None,
                object_entity_type: None,
                significance: None,
                lifecycle_tier: "active".to_string(),
            };
            repo.create(assertion).await.expect("create assertion");
        }

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "knows",
                    "limit": 1
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.items.len(), 1);
        assert_eq!(data.total, 2);
    }

    #[tokio::test]
    async fn test_search_assertions_scans_past_first_repository_page() {
        // No search indexes here: the index query fails and the bounded scan
        // still serves the query.
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        for index in 0..1000 {
            create_test_assertion(
                &repo,
                &format!("assertion:00000000-0000-0000-0000-{index:012x}"),
                &format!("Unrelated Subject {index}"),
                "mentions",
                "background fact",
            )
            .await;
        }
        create_test_assertion(
            &repo,
            "assertion:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "Needle Person",
            "owns",
            "Needle Folder",
        )
        .await;

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Needle Folder",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 1);
        assert_eq!(data.items[0].subject_text, "Needle Person");
        assert_eq!(data.items[0].object_text.as_deref(), Some("Needle Folder"));
    }

    #[tokio::test]
    async fn test_search_assertions_finds_matches_past_the_scan_window() {
        let storage = indexed_memory_storage().await;
        // More rows than the scan window ahead of the only match: recall
        // must come from the index, not the id-order scan.
        let handler = SearchAssertionsHandler::new(storage.clone()).with_scan_window(10);
        let repo = AssertionRepository::new(storage.clone());

        for index in 0..30 {
            create_test_assertion(
                &repo,
                &format!("assertion:00000000-0000-0000-0000-{index:012x}"),
                &format!("Unrelated Subject {index}"),
                "mentions",
                "background fact",
            )
            .await;
        }
        create_test_assertion(
            &repo,
            "assertion:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "Needle Person",
            "owns",
            "Needle Folder",
        )
        .await;

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Needle Folder",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 1);
        assert_eq!(data.items[0].subject_text, "Needle Person");
    }

    #[tokio::test]
    async fn test_search_assertions_matches_terms_across_subject_predicate_and_literal() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        let id = RecordId::new_v4(TableName::Assertion);
        let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let assertion = Assertion {
            base,
            subject_text: "Ada".to_string(),
            predicate_raw: "has reading homework due".to_string(),
            predicate_id: None,
            object_text: None,
            object_literal: Some(serde_json::json!("kapitel 27")),
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "assertion-ada-homework".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Ada reading homework kapitel",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 1);
        assert_eq!(data.items[0].subject_text, "Ada");
    }

    #[tokio::test]
    async fn test_search_assertions_literal_lookup_survives_unrelated_index_hits() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        // An unrelated row whose text carries the number: the index returns
        // it, so before the literal-shaped scan the literal-only row was lost.
        create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000001",
            "Room 152",
            "is booked for",
            "Friday",
        )
        .await;

        let id = RecordId::new_v4(TableName::Assertion);
        let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let assertion = Assertion {
            base,
            subject_text: "Ada".to_string(),
            predicate_raw: "height cm".to_string(),
            predicate_id: None,
            object_text: None,
            object_literal: Some(serde_json::json!(152)),
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "assertion-ada-height".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "152",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 2);
        assert!(data.items.iter().any(|item| item.subject_text == "Ada"));
    }

    #[tokio::test]
    async fn test_search_assertions_matches_partial_agent_query_terms() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        let id = RecordId::new_v4(TableName::Assertion);
        let mut base = BaseFields::new(id, "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let assertion = Assertion {
            base,
            subject_text: "Example Group".to_string(),
            predicate_raw: "member_of".to_string(),
            predicate_id: None,
            object_text: Some("Example Organization".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "assertion-example-group-organization".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Example Organization group member",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 1);
        assert_eq!(data.items[0].subject_text, "Example Group");
    }

    #[tokio::test]
    async fn test_search_assertions_does_not_drop_broad_discovery_queries() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000101",
            "Service A",
            "depends_on",
            "Platform B",
        )
        .await;
        create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000103",
            "Service A",
            "depends on",
            "Platform B",
        )
        .await;
        create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000102",
            "Worker C",
            "uses",
            "Queue D",
        )
        .await;

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Service Platform Worker Queue depends uses durable discovery context",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 2);
        let mut subjects = data
            .items
            .iter()
            .map(|item| item.subject_text.as_str())
            .collect::<Vec<_>>();
        subjects.sort_unstable();
        assert_eq!(subjects, vec!["Service A", "Worker C"]);
    }

    #[tokio::test]
    async fn test_search_assertions_canonicalizes_merged_entity_links_before_dedup() {
        let storage = indexed_memory_storage().await;
        let handler = SearchAssertionsHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        let canonical_platform_id =
            RecordId::from_str("entity:00000000-0000-0000-0000-000000000201")
                .expect("valid canonical platform id");
        let merged_platform_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000202")
            .expect("valid merged platform id");

        let canonical_platform =
            create_test_entity(&canonical_platform_id.to_string(), "Platform B");
        let mut merged_platform = create_test_entity(&merged_platform_id.to_string(), "Platform");
        merged_platform.status = EntityStatus::Merged;
        merged_platform.redirect_to = Some(Link::new(canonical_platform_id.clone()));
        entity_repo
            .create(canonical_platform)
            .await
            .expect("create canonical platform");
        entity_repo
            .create(merged_platform)
            .await
            .expect("create merged platform");

        let mut merged_link_assertion = create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000301",
            "Service A",
            "depends_on",
            "Platform B",
        )
        .await;
        merged_link_assertion.object_entity = Some(Link::new(merged_platform_id));
        let merged_assertion_id = merged_link_assertion.base.id.clone();
        repo.update("default", &merged_assertion_id, merged_link_assertion)
            .await
            .expect("update merged assertion");

        let mut canonical_link_assertion = create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000302",
            "Service A",
            "depends on",
            "Platform B",
        )
        .await;
        canonical_link_assertion.object_entity = Some(Link::new(canonical_platform_id.clone()));
        let canonical_assertion_id = canonical_link_assertion.base.id.clone();
        repo.update("default", &canonical_assertion_id, canonical_link_assertion)
            .await
            .expect("update canonical assertion");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(
                &ctx,
                serde_json::json!({
                    "query": "Service Platform depends",
                    "status": "resolved",
                    "limit": 10
                }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");

        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(data.total, 1);
        assert_eq!(
            data.items[0].object_entity.as_ref().map(Link::id),
            Some(&canonical_platform_id)
        );
    }

    fn read_assertion_ctx(storage: StorageClient) -> ToolContext {
        ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
                clearances: vec![elephant_policy::Clearance {
                    policy_id: "default".to_string(),
                    max_level: 0,
                    labels: std::collections::HashSet::new(),
                    handling_allow: std::collections::HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: std::collections::HashSet::new(),
            },
        )
    }

    /// Fixture for the attribution filter tests. Entities:
    /// riley (`angel_protectees: [child-a]`), morgan (`[child-b]`),
    /// alpha (`[child-a]`), beta (`[child-b]`), club (no key = legacy).
    /// Assertions (subject → predicate → object):
    /// riley → "owns alpha" → alpha, morgan → "owns beta" → beta,
    /// riley → "member of club" → club, riley → "feeds beta" → beta,
    /// and "likes chess" with no entity links at all.
    /// Returns (storage, riley's entity id).
    async fn setup_attribution_fixture() -> (StorageClient, RecordId) {
        let storage = indexed_memory_storage().await;
        let repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        let riley_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000401")
            .expect("valid riley id");
        let alpha_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000402")
            .expect("valid alpha id");
        let beta_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000403")
            .expect("valid beta id");
        let club_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000404")
            .expect("valid club id");
        let morgan_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000405")
            .expect("valid morgan id");

        let mut riley = create_test_entity(&riley_id.to_string(), "Riley Example");
        riley.metadata = serde_json::json!({ "angel_protectees": ["child-a"] });
        let mut morgan = create_test_entity(&morgan_id.to_string(), "Morgan Example");
        morgan.metadata = serde_json::json!({ "angel_protectees": ["child-b"] });
        let mut alpha = create_test_entity(&alpha_id.to_string(), "Goldfish Alpha");
        alpha.metadata = serde_json::json!({ "angel_protectees": ["child-a"] });
        let mut beta = create_test_entity(&beta_id.to_string(), "Goldfish Beta");
        beta.metadata = serde_json::json!({ "angel_protectees": ["child-b"] });
        // No angel_protectees key: legacy/family-wide.
        let club = create_test_entity(&club_id.to_string(), "Chess Club");
        for entity in [riley, morgan, alpha, beta, club] {
            entity_repo.create(entity).await.expect("create entity");
        }

        let specs: [(&str, &str, Option<(&RecordId, &RecordId)>); 5] = [
            (
                "assertion:00000000-0000-0000-0000-000000000501",
                "owns alpha",
                Some((&riley_id, &alpha_id)),
            ),
            (
                "assertion:00000000-0000-0000-0000-000000000502",
                "owns beta",
                Some((&morgan_id, &beta_id)),
            ),
            (
                "assertion:00000000-0000-0000-0000-000000000503",
                "member of club",
                Some((&riley_id, &club_id)),
            ),
            (
                "assertion:00000000-0000-0000-0000-000000000504",
                "feeds beta",
                Some((&riley_id, &beta_id)),
            ),
            (
                "assertion:00000000-0000-0000-0000-000000000505",
                "likes chess",
                None,
            ),
        ];
        for (assertion_id, predicate, links) in specs {
            let mut assertion = create_test_assertion(
                &repo,
                assertion_id,
                "Family Example",
                predicate,
                "attribution fixture",
            )
            .await;
            // The "likes chess" assertion keeps no entity links; the rest
            // resolve both subject and object.
            if let Some((subject_id, object_id)) = links {
                assertion.subject_entity = Some(Link::new(subject_id.clone()));
                assertion.object_entity = Some(Link::new(object_id.clone()));
                let id = assertion.base.id.clone();
                repo.update("default", &id, assertion)
                    .await
                    .expect("update assertion links");
            }
        }

        (storage, riley_id)
    }

    fn assertion_predicates(output: &SearchAssertionsOutput) -> Vec<String> {
        let mut predicates: Vec<String> = output
            .items
            .iter()
            .map(|assertion| assertion.predicate_raw.clone())
            .collect();
        predicates.sort();
        predicates
    }

    async fn search_with_params(storage: StorageClient, params: Value) -> SearchAssertionsOutput {
        let handler = SearchAssertionsHandler::new(storage.clone());
        let ctx = read_assertion_ctx(storage);
        let response = handler.execute(&ctx, params).await.expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(response.ok, "search should succeed: {:?}", response.error);
        response.data.expect("tool data")
    }

    #[tokio::test]
    async fn test_search_assertions_attributed_to_filters_entity_id_fast_path() {
        let (storage, riley_id) = setup_attribution_fixture().await;

        // Riley links three assertions: owns alpha, member of club, feeds
        // beta. In child-a scope all pass (riley is child-a's).
        let output = search_with_params(
            storage.clone(),
            serde_json::json!({
                "entity_id": riley_id.to_string(),
                "filters": { "attributed_to": "child-a" },
                "limit": 10
            }),
        )
        .await;
        assert_eq!(
            assertion_predicates(&output),
            vec!["feeds beta", "member of club", "owns alpha"],
            "child-a scope keeps every riley-linked assertion"
        );

        // child-b scope: "owns alpha" is excluded (riley and alpha are both
        // child-a's); "member of club" passes via the legacy club object;
        // "feeds beta" passes via the child-b object.
        let output = search_with_params(
            storage.clone(),
            serde_json::json!({
                "entity_id": riley_id.to_string(),
                "filters": { "attributed_to": "child-b" },
                "limit": 10
            }),
        )
        .await;
        assert_eq!(
            assertion_predicates(&output),
            vec!["feeds beta", "member of club"],
            "child-b scope excludes the fully child-a-scoped assertion"
        );

        // No filter: every linked assertion comes back.
        let output = search_with_params(
            storage,
            serde_json::json!({
                "entity_id": riley_id.to_string(),
                "limit": 10
            }),
        )
        .await;
        assert_eq!(output.total, 3, "unfiltered fast path returns all linked");
    }

    #[tokio::test]
    async fn test_search_assertions_attributed_to_union_keeps_mixed_assertions() {
        let (storage, _riley_id) = setup_attribution_fixture().await;

        // "feeds beta" mixes scopes (riley: child-a, beta: child-b). Under
        // the union rule it is visible in BOTH scopes — via the subject in
        // child-a's, via the object in child-b's.
        for scope in ["child-a", "child-b"] {
            let output = search_with_params(
                storage.clone(),
                serde_json::json!({
                    "query": "attribution fixture",
                    "filters": { "attributed_to": scope },
                    "limit": 10
                }),
            )
            .await;
            let predicates = assertion_predicates(&output);
            assert!(
                predicates.contains(&"feeds beta".to_string()),
                "mixed-scope assertion visible in {scope}: {predicates:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_search_assertions_attributed_to_filters_text_path() {
        let (storage, _riley_id) = setup_attribution_fixture().await;

        let output = search_with_params(
            storage,
            serde_json::json!({
                "query": "attribution fixture",
                "filters": { "attributed_to": "child-a" },
                "limit": 10
            }),
        )
        .await;
        let predicates = assertion_predicates(&output);
        assert!(
            predicates.contains(&"owns alpha".to_string()),
            "attributed assertion visible in matching scope: {predicates:?}"
        );
        assert!(
            !predicates.contains(&"owns beta".to_string()),
            "foreign-scoped entities' assertion excluded: {predicates:?}"
        );
        assert!(
            predicates.contains(&"member of club".to_string()),
            "legacy entity's assertion visible in every scope: {predicates:?}"
        );
        assert!(
            predicates.contains(&"likes chess".to_string()),
            "assertion with no entity links visible: {predicates:?}"
        );
    }

    #[tokio::test]
    async fn test_get_assertion_enforces_record_security() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let handler = GetAssertionHandler::new(storage.clone());
        let repo = AssertionRepository::new(storage.clone());

        let id = RecordId::new_v4(TableName::Assertion);
        let mut base = BaseFields::new(id.clone(), "default".to_string(), "tester".to_string());
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        base.security = Security {
            policy_id: "default".to_string(),
            level: 5,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        let assertion = Assertion {
            base,
            subject_text: "Alice".to_string(),
            predicate_raw: "knows".to_string(),
            predicate_id: None,
            object_text: Some("Wonderland".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "secure-assertion".to_string(),
            resolution_status: ResolutionStatus::Pending,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage,
            elephant_policy::Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:assertion".to_string()).collect(),
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
            .execute(&ctx, serde_json::json!({"id": id.to_string()}))
            .await
            .expect("tool response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded.error.is_some());
    }

    /// Deterministic provider for semantic recall tests: known texts map to
    /// fixed vectors, everything else embeds to a zero vector.
    struct ControlledProvider {
        entries: HashMap<String, Vec<f32>>,
        dims: usize,
    }

    impl ControlledProvider {
        fn new(entries: Vec<(&str, Vec<f32>)>) -> Self {
            let dims = entries.first().map_or(3, |(_, vector)| vector.len());
            Self {
                entries: entries
                    .into_iter()
                    .map(|(text, vector)| (text.to_string(), vector))
                    .collect(),
                dims,
            }
        }
    }

    #[async_trait]
    impl elephant_embeddings::EmbeddingProvider for ControlledProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn model(&self) -> &str {
            "controlled-assertions-test"
        }

        fn dimensions(&self) -> usize {
            self.dims
        }

        async fn embed(
            &self,
            texts: &[String],
        ) -> elephant_embeddings::EmbeddingResult<Vec<Vec<f32>>> {
            Ok(texts
                .iter()
                .map(|text| {
                    self.entries
                        .get(text)
                        .cloned()
                        .unwrap_or_else(|| vec![0.0; self.dims])
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn test_search_assertions_semantic_entity_recall_without_text_match() {
        use std::sync::Arc;

        let storage = indexed_memory_storage().await;
        let repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        // Riley's summary is semantically related to the query but shares no
        // substring with it — today's text scan finds nothing.
        let riley_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000601")
            .expect("valid riley id");
        let mut riley = create_test_entity(&riley_id.to_string(), "Riley Kim");
        riley.summary = Some("neural networks researcher".to_string());
        entity_repo.create(riley).await.expect("create riley");
        entity_repo
            .set_name_embedding(
                "default",
                &riley_id,
                &[0.95, 0.05, 0.0],
                "controlled-assertions-test",
                "hash-riley",
            )
            .await
            .expect("store embedding");

        let mut assertion = create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000601",
            "Riley",
            "owns",
            "Goldfish Alpha",
        )
        .await;
        assertion.subject_entity = Some(Link::new(riley_id.clone()));
        let assertion_id = assertion.base.id.clone();
        repo.update("default", &assertion_id, assertion)
            .await
            .expect("link assertion");

        let cache = elephant_embeddings::EmbeddingProviderCache::with_provider(
            elephant_embeddings::EmbeddingPolicy::from_env(),
            Arc::new(ControlledProvider::new(vec![(
                "machine learning",
                vec![1.0, 0.0, 0.0],
            )])),
        );
        let handler = SearchAssertionsHandler::with_provider_cache(storage.clone(), cache);
        let ctx = read_assertion_ctx(storage);

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({ "query": "machine learning", "limit": 10 }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert_eq!(
            data.items
                .iter()
                .map(|assertion| assertion.base.id.to_string())
                .collect::<Vec<_>>(),
            vec![assertion_id.to_string()],
            "entity-anchored assertion surfaces with zero text overlap"
        );
    }

    #[tokio::test]
    async fn test_search_assertions_without_embeddings_matches_plain_handler() {
        use std::sync::Arc;

        let storage = indexed_memory_storage().await;
        let repo = AssertionRepository::new(storage.clone());
        create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000701",
            "Riley",
            "owns",
            "Goldfish Alpha",
        )
        .await;

        // Provider present, but no entity embeddings stored anywhere: output
        // must be byte-identical to the plain handler (today's behavior).
        let cache = elephant_embeddings::EmbeddingProviderCache::with_provider(
            elephant_embeddings::EmbeddingPolicy::from_env(),
            Arc::new(ControlledProvider::new(vec![("owns", vec![1.0, 0.0, 0.0])])),
        );
        let blended_handler = SearchAssertionsHandler::with_provider_cache(storage.clone(), cache);
        let plain_handler = SearchAssertionsHandler::new(storage.clone());
        let ctx = read_assertion_ctx(storage);

        let params = serde_json::json!({ "query": "owns", "limit": 10 });
        let blended = blended_handler
            .execute(&ctx, params.clone())
            .await
            .expect("blended search");
        let plain = plain_handler
            .execute(&ctx, params)
            .await
            .expect("plain search");

        assert_eq!(
            blended, plain,
            "absent embeddings: fallback must be identical to today's output"
        );
    }
    /// A weakly-similar entity must NOT anchor assertion recall.
    ///
    /// The query vector is [1,0,0] and the entity vector [0.5, 0.866, 0] — a
    /// cosine of exactly 0.5, deliberately chosen to sit ABOVE the old 0.30
    /// floor and BELOW the current one. Under 0.30 this returned the linked
    /// assertion despite zero lexical overlap, which is how a nonsense query
    /// came back with four confident-looking hits on the live household space.
    #[tokio::test]
    async fn test_search_assertions_rejects_weak_semantic_anchor() {
        use std::sync::Arc;

        let storage = indexed_memory_storage().await;
        let repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        let weak_id = RecordId::from_str("entity:00000000-0000-0000-0000-000000000901")
            .expect("valid weak id");
        let mut weak = create_test_entity(&weak_id.to_string(), "Unrelated Topic");
        weak.summary = Some("nothing to do with the query".to_string());
        entity_repo.create(weak).await.expect("create weak entity");
        entity_repo
            .set_name_embedding(
                "default",
                &weak_id,
                &[0.5, 0.866, 0.0],
                "controlled-assertions-test",
                "hash-weak",
            )
            .await
            .expect("store embedding");

        let mut assertion = create_test_assertion(
            &repo,
            "assertion:00000000-0000-0000-0000-000000000901",
            "Unrelated Topic",
            "observed_at",
            "Somewhere",
        )
        .await;
        assertion.subject_entity = Some(Link::new(weak_id.clone()));
        let assertion_id = assertion.base.id.clone();
        repo.update("default", &assertion_id, assertion)
            .await
            .expect("link assertion");

        let cache = elephant_embeddings::EmbeddingProviderCache::with_provider(
            elephant_embeddings::EmbeddingPolicy::from_env(),
            Arc::new(ControlledProvider::new(vec![(
                "zzzqqqxyzzyplugh",
                vec![1.0, 0.0, 0.0],
            )])),
        );
        let handler = SearchAssertionsHandler::with_provider_cache(storage.clone(), cache);
        let ctx = read_assertion_ctx(storage);

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({ "query": "zzzqqqxyzzyplugh", "limit": 10 }),
            )
            .await
            .expect("tool response");
        let response: ToolResponse<SearchAssertionsOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(response.ok);
        let data = response.data.expect("tool data");
        assert!(
            data.items.is_empty(),
            "a nonsense query must return NOTHING, not the nearest neighbour; got {:?}",
            data.items
                .iter()
                .map(|a| a.subject_text.clone())
                .collect::<Vec<_>>()
        );
    }
}
