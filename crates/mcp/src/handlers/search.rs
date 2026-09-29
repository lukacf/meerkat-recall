//! Unified search MCP tool handler.
//!
//! Implements:
//! - `search_all`

use std::collections::HashSet;
use std::str::FromStr;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_policy::RedactionEnvelope;
use elephant_storage::{
    DocRepository, EntityRepository, RelRepository, SearchCriteria, SearchRepository,
    SourceRepository, StorageClient,
};
use elephant_types::records::{Doc, Entity, Rel, Source};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::handlers::entities::SearchEntitiesHandler;
use crate::redaction::redact_doc;
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

fn query_terms(query: &str) -> Vec<String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut seen = HashSet::new();
    let mut terms = Vec::new();

    if seen.insert(trimmed.to_string()) {
        terms.push(trimmed.to_string());
    }

    for token in trimmed.split(|c: char| !c.is_alphanumeric()) {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if token.len() < 2 {
            continue;
        }
        if seen.insert(token.to_string()) {
            terms.push(token.to_string());
        }
    }

    terms
}

/// How well an entity matches the ORIGINAL query, not the single term that found it.
///
/// Entity lookup uses substring matching. Score the complete query so a mid-word
/// coincidence cannot outrank an exact name or a word-boundary match merely by
/// arriving first. The regression fixtures use synthetic names and organizations.
fn entity_relevance(entity: &Entity, query: &str, terms: &[String]) -> i32 {
    let name = entity.display_name.to_lowercase();
    let query_lower = query.trim().to_lowercase();
    if name == query_lower {
        return 1000;
    }

    let mut score = 0;
    for term in terms {
        let term_lower = term.to_lowercase();
        if term_lower == query_lower && terms.len() > 1 {
            // The whole-query term; its own contribution is covered below.
            continue;
        }
        if name == term_lower {
            score += 200;
        } else if word_boundary_hit(&name, &term_lower) {
            score += 60;
        } else if name.contains(&term_lower) {
            // A mid-word coincidence: "mia" inside "acamia". Worth almost nothing.
            score += 1;
        } else if entity
            .aliases
            .iter()
            .any(|alias| word_boundary_hit(&alias.to_lowercase(), &term_lower))
        {
            score += 40;
        }
    }

    // Reward covering the whole query rather than one lucky term.
    //
    // NOTE `query_terms` returns the ENTIRE trimmed query as its first element and the
    // individual tokens after it. Comparing against `terms.len()` therefore demanded
    // that the name contain the whole query string as well, which for any multi-word
    // query is almost never true - the bonus was very nearly dead code, and a mutation
    // that deleted it did not fail a single test. Count coverage over the TOKENS only.
    let tokens: Vec<&String> = terms
        .iter()
        .filter(|term| !term.trim().eq_ignore_ascii_case(query.trim()))
        .collect();
    if tokens.len() > 1 {
        let covered = tokens
            .iter()
            .filter(|term| name.contains(&term.to_lowercase()))
            .count();
        if covered == tokens.len() {
            score += 300;
        }
    }
    score
}

/// Whether `needle` appears in `haystack` at a word boundary on both sides.
/// Cosine similarity as a small integer band.
///
/// Banded rather than scaled-and-cast so there is no float-to-int conversion to reason
/// about (NaN, truncation, overflow) and so the contribution is legible next to the
/// lexical scores: a strong semantic hit lands near an exact-name match's neighbourhood
/// without ever overtaking the 1000 an exact name earns, and a weak one sits above a
/// mid-word substring's 1. Anything below the handler's similarity floor never reaches
/// here at all.
fn semantic_band(score: f32) -> i32 {
    if !score.is_finite() {
        return 0;
    }
    if score >= 0.90 {
        180
    } else if score >= 0.70 {
        140
    } else if score >= 0.50 {
        100
    } else if score > 0.0 {
        60
    } else {
        0
    }
}

fn word_boundary_hit(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let bytes = haystack.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        let after_ok = end == bytes.len() || !bytes[end].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
        if from >= haystack.len() {
            break;
        }
    }
    false
}

// ============================================================================
// Input/Output Types
// ============================================================================

/// Search target types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SearchTarget {
    /// Entity search.
    Entities,
    /// Document search.
    Docs,
    /// Source search.
    Sources,
    /// Relationship search.
    Relationships,
}

/// Input for `search_all` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedSearchInput {
    /// Search query.
    pub query: String,
    /// Maximum results per type.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Include soft-deleted records.
    #[serde(default)]
    pub include_deleted: bool,
    /// Restrict search targets.
    #[serde(default)]
    pub include: Option<Vec<SearchTarget>>,
}

const fn default_limit() -> usize {
    10
}

/// Output for a single search target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchBucket<T> {
    /// Search results.
    pub items: Vec<T>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

/// Per-target error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedSearchError {
    /// Target that failed.
    pub target: String,
    /// Error message.
    pub error: String,
}

/// Output for `search_all` tool.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UnifiedSearchOutput {
    /// Entity results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entities: Option<SearchBucket<Entity>>,
    /// Doc results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<SearchBucket<RedactionEnvelope<Doc>>>,
    /// Source results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<SearchBucket<Source>>,
    /// Relationship results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relationships: Option<SearchBucket<Rel>>,
    /// Per-target errors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<UnifiedSearchError>,
}

// ============================================================================
// Tool Handler
// ============================================================================

/// Handler for `search_all` tool.
#[allow(clippy::struct_field_names)]
pub struct UnifiedSearchHandler {
    entity_repo: EntityRepository,
    /// Delegate that owns the embedding provider cache and space config, so unified
    /// search can use the SAME semantic path `search_entities` already uses instead of
    /// re-implementing it. See the call site for why substring-only was the bug.
    entity_search: SearchEntitiesHandler,
    doc_repo: DocRepository,
    source_repo: SourceRepository,
    rel_repo: RelRepository,
}

impl UnifiedSearchHandler {
    /// Creates a handler with a pre-seeded embedding provider cache, so tests can
    /// inject a controlled provider exactly as the entities handler allows.
    #[must_use]
    pub fn with_provider_cache(
        storage: StorageClient,
        provider_cache: elephant_embeddings::EmbeddingProviderCache,
    ) -> Self {
        Self {
            entity_repo: EntityRepository::new(storage.clone()),
            entity_search: SearchEntitiesHandler::with_provider_cache(
                storage.clone(),
                provider_cache,
            ),
            doc_repo: DocRepository::new(storage.clone()),
            source_repo: SourceRepository::new(storage.clone()),
            rel_repo: RelRepository::new(storage),
        }
    }

    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entity_repo: EntityRepository::new(storage.clone()),
            entity_search: SearchEntitiesHandler::new(storage.clone()),
            doc_repo: DocRepository::new(storage.clone()),
            source_repo: SourceRepository::new(storage.clone()),
            rel_repo: RelRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UnifiedSearchHandler {
    fn name(&self) -> &'static str {
        "search_all"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search across entities, docs, sources, and relationships.\n\n",
            "Example:\n",
            r#"{ "query": "Alex Chen Acme", "limit": 10, "include": ["entities", "docs", "relationships"] }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query text"
                },
                "limit": {
                    "type": "integer",
                    "default": 10,
                    "maximum": 100
                },
                "include_deleted": {
                    "type": "boolean",
                    "default": false
                },
                "include": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["entities", "docs", "sources", "relationships"]
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: UnifiedSearchInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let terms = query_terms(&input.query);
        let targets: HashSet<SearchTarget> = input
            .include
            .unwrap_or_else(|| {
                vec![
                    SearchTarget::Entities,
                    SearchTarget::Docs,
                    SearchTarget::Sources,
                    SearchTarget::Relationships,
                ]
            })
            .into_iter()
            .collect();

        let mut base_criteria = SearchCriteria::new()
            .with_limit(input.limit)
            .with_space_id(space_id.clone());
        if input.include_deleted {
            base_criteria = base_criteria.include_deleted();
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);

        let mut output = UnifiedSearchOutput::default();

        if targets.contains(&SearchTarget::Entities) {
            match ctx.authz.evaluate(&ctx.principal, &record, "read:entity") {
                Ok(()) => {
                    let mut items: Vec<Entity> = Vec::new();
                    let mut seen_ids: HashSet<String> = HashSet::new();

                    // Collect across ALL terms first, then RANK. Taking the first
                    // `limit` in term order is what let "Acamia AB" outrank an exact
                    // "Mia Example" - the junk simply arrived first, because "Mia"
                    // is term one and a substring of "Acamia". See entity_relevance.
                    for term in &terms {
                        let criteria = base_criteria.clone().with_query(term.clone());
                        let page = self
                            .entity_repo
                            .search(&criteria)
                            .await
                            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                        let filtered =
                            ctx.authz
                                .filter_records(&ctx.principal, page.results, "read:entity");

                        for entity in filtered {
                            if seen_ids.insert(entity.base.id.to_string()) {
                                items.push(entity);
                            }
                        }
                        if items.len() >= input.limit.saturating_mul(8) {
                            break;
                        }
                    }

                    // SEMANTIC FIRST. Vector search over stored name embeddings is the
                    // right primary signal and it already exists - `search_entities`
                    // has used it all along (entities.rs). Unified search never got
                    // it, so the tool the household agents actually reach was
                    // substring-only, which is how "Mia Example principal" returned
                    // "Acamia AB". Substring is the fallback, not the design.
                    //
                    // Degrades exactly as the entities handler does: when no provider,
                    // no stored vectors, or nothing above the similarity floor, this
                    // returns None and the lexical results stand unchanged.
                    let semantic = self
                        .entity_search
                        .semantic_entity_hits(ctx, &space_id, &input.query, None, None, input.limit)
                        .await;
                    let mut semantic_scores: std::collections::HashMap<String, f32> =
                        std::collections::HashMap::new();
                    if let Some(hits) = semantic {
                        for (entity, score) in hits {
                            let id = entity.base.id.to_string();
                            semantic_scores.insert(id.clone(), score);
                            if seen_ids.insert(id) {
                                items.push(entity);
                            }
                        }
                    }

                    // RANK, then truncate. Stable sort on a descending score keeps
                    // ties in discovery order, so results stay deterministic.
                    let mut ranked: Vec<(i32, Entity)> = items
                        .into_iter()
                        .map(|entity| {
                            let lexical = entity_relevance(&entity, &input.query, &terms);
                            // A cosine hit above the floor is worth more than a bare
                            // substring coincidence and less than an exact name match,
                            // so semantic ranks the middle band rather than overriding
                            // a name the user typed exactly.
                            // Cosine similarity is bounded to [-1, 1] and the floor
                            // keeps it positive, so this scales into a small integer
                            // band. Clamped and rounded explicitly rather than cast,
                            // so a NaN or an out-of-range score can never wrap into a
                            // huge rank.
                            let semantic = semantic_scores
                                .get(&entity.base.id.to_string())
                                .map_or(0, |score| semantic_band(*score));
                            (lexical + semantic, entity)
                        })
                        .collect();
                    ranked.sort_by(|lhs, rhs| rhs.0.cmp(&lhs.0));
                    let mut items: Vec<Entity> =
                        ranked.into_iter().map(|(_, entity)| entity).collect();
                    items.truncate(input.limit);

                    output.entities = Some(SearchBucket {
                        items,
                        next_cursor: None,
                        has_more: false,
                        total: None,
                    });
                }
                Err(reason) => output.errors.push(UnifiedSearchError {
                    target: "entities".to_string(),
                    error: reason,
                }),
            }
        }

        if targets.contains(&SearchTarget::Docs) {
            match ctx
                .authz
                .evaluate(&ctx.principal, &record, "read:doc:metadata")
            {
                Ok(()) => {
                    let mut items: Vec<RedactionEnvelope<Doc>> = Vec::new();
                    let mut seen_ids: HashSet<String> = HashSet::new();

                    for term in &terms {
                        let criteria = base_criteria.clone().with_query(term.clone());
                        let page = self
                            .doc_repo
                            .search(&criteria)
                            .await
                            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                        let filtered = ctx.authz.filter_records(
                            &ctx.principal,
                            page.results,
                            "read:doc:metadata",
                        );

                        for doc in filtered {
                            if items.len() >= input.limit {
                                break;
                            }
                            if seen_ids.insert(doc.base.id.to_string()) {
                                items.push(redact_doc(&ctx.principal, doc));
                            }
                        }
                        if items.len() >= input.limit {
                            break;
                        }
                    }

                    output.docs = Some(SearchBucket {
                        items,
                        next_cursor: None,
                        has_more: false,
                        total: None,
                    });
                }
                Err(reason) => output.errors.push(UnifiedSearchError {
                    target: "docs".to_string(),
                    error: reason,
                }),
            }
        }

        if targets.contains(&SearchTarget::Sources) {
            match ctx.authz.evaluate(&ctx.principal, &record, "read:source") {
                Ok(()) => {
                    let mut items: Vec<Source> = Vec::new();
                    let mut seen_ids: HashSet<String> = HashSet::new();

                    for term in &terms {
                        let criteria = base_criteria.clone().with_query(term.clone());
                        let page = self
                            .source_repo
                            .search(&criteria)
                            .await
                            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                        let filtered =
                            ctx.authz
                                .filter_records(&ctx.principal, page.results, "read:source");

                        for source in filtered {
                            if items.len() >= input.limit {
                                break;
                            }
                            if seen_ids.insert(source.base.id.to_string()) {
                                items.push(source);
                            }
                        }
                        if items.len() >= input.limit {
                            break;
                        }
                    }

                    output.sources = Some(SearchBucket {
                        items,
                        next_cursor: None,
                        has_more: false,
                        total: None,
                    });
                }
                Err(reason) => output.errors.push(UnifiedSearchError {
                    target: "sources".to_string(),
                    error: reason,
                }),
            }
        }

        if targets.contains(&SearchTarget::Relationships) {
            match ctx
                .authz
                .evaluate(&ctx.principal, &record, "read:relationship")
            {
                Ok(()) => {
                    let mut items: Vec<Rel> = Vec::new();
                    let mut seen_ids: HashSet<String> = HashSet::new();

                    // Search by rel_type (predicate)
                    for term in &terms {
                        let criteria = base_criteria.clone().with_query(term.clone());
                        let page = self
                            .rel_repo
                            .search(&criteria)
                            .await
                            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                        let filtered = ctx.authz.filter_records(
                            &ctx.principal,
                            page.results,
                            "read:relationship",
                        );

                        for rel in filtered {
                            if items.len() >= input.limit {
                                break;
                            }
                            if seen_ids.insert(rel.base.id.to_string()) {
                                items.push(rel);
                            }
                        }
                        if items.len() >= input.limit {
                            break;
                        }
                    }

                    // Also search by entity names (find relationships involving matching entities)
                    if items.len() < input.limit {
                        let mut entity_ids: HashSet<String> = HashSet::new();
                        for term in &terms {
                            let criteria = base_criteria.clone().with_query(term.clone());
                            if let Ok(page) = self.entity_repo.search(&criteria).await {
                                for entity in page.results {
                                    entity_ids.insert(entity.base.id.to_string());
                                }
                            }
                        }

                        for entity_id_str in entity_ids {
                            if items.len() >= input.limit {
                                break;
                            }
                            if let Ok(entity_id) =
                                elephant_types::RecordId::from_str(&entity_id_str)
                            {
                                // Find outgoing relationships
                                if let Ok(outgoing) =
                                    self.rel_repo.find_outgoing(&space_id, &entity_id).await
                                {
                                    let filtered = ctx.authz.filter_records(
                                        &ctx.principal,
                                        outgoing,
                                        "read:relationship",
                                    );
                                    for rel in filtered {
                                        if items.len() >= input.limit {
                                            break;
                                        }
                                        if seen_ids.insert(rel.base.id.to_string()) {
                                            items.push(rel);
                                        }
                                    }
                                }

                                // Find incoming relationships
                                if let Ok(incoming) =
                                    self.rel_repo.find_incoming(&space_id, &entity_id).await
                                {
                                    let filtered = ctx.authz.filter_records(
                                        &ctx.principal,
                                        incoming,
                                        "read:relationship",
                                    );
                                    for rel in filtered {
                                        if items.len() >= input.limit {
                                            break;
                                        }
                                        if seen_ids.insert(rel.base.id.to_string()) {
                                            items.push(rel);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    output.relationships = Some(SearchBucket {
                        items,
                        next_cursor: None,
                        has_more: false,
                        total: None,
                    });
                }
                Err(reason) => output.errors.push(UnifiedSearchError {
                    target: "relationships".to_string(),
                    error: reason,
                }),
            }
        }

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    use elephant_types::prelude::{BaseFields, EntityType, RecordId, TableName};

    fn scoring_entity(display_name: &str) -> Entity {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "test_space".to_string(),
            "test".to_string(),
        );
        Entity::new(base, EntityType::new("person"), display_name.to_string())
    }

    struct UnifiedControlledProvider {
        entries: std::collections::HashMap<String, Vec<f32>>,
    }

    #[async_trait]
    impl elephant_embeddings::EmbeddingProvider for UnifiedControlledProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn model(&self) -> &str {
            "controlled-unified-test"
        }

        fn dimensions(&self) -> usize {
            3
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
                        .unwrap_or_else(|| vec![0.0; 3])
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn test_search_all_uses_the_semantic_path_not_just_substring() {
        // THE STRUCTURAL GAP behind the household incident. Vector search over stored
        // name embeddings already existed and `search_entities` used it - but
        // `search_all`, which is the tool the household agents actually reach, was
        // substring-only. So the knowledge base could not find anything a query did
        // not literally spell, and answered "Mia Example principal" with "Acamia AB".
        //
        // Mutation: delete the semantic block from the unified handler - executed, red.
        use std::collections::HashSet;
        use std::sync::Arc;

        use elephant_storage::{EntityRepository, Repository, StorageClient};
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::{Entity, EntityType};

        use crate::authz::AuthzService;
        use crate::handlers::ToolContext;
        use crate::tools::ToolResponse;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE entity SCHEMALESS;")
            .await
            .expect("schema");
        let repo = EntityRepository::new(storage.clone());

        // Shares NO substring with the query: only the semantic path can find it.
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Riley Kim".to_string(),
        );
        let entity_id = entity.base.id.clone();
        repo.create(entity).await.expect("create");
        repo.set_name_embedding(
            "test_space",
            &entity_id,
            &[0.95, 0.05, 0.0],
            "controlled-unified-test",
            "hash-riley",
        )
        .await
        .expect("embedding");

        let cache = elephant_embeddings::EmbeddingProviderCache::with_provider(
            elephant_embeddings::EmbeddingPolicy::from_env(),
            Arc::new(UnifiedControlledProvider {
                entries: std::iter::once(("machine learning".to_string(), vec![1.0, 0.0, 0.0]))
                    .collect(),
            }),
        );
        let handler = UnifiedSearchHandler::with_provider_cache(storage.clone(), cache);
        let ctx = ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(true)),
            elephant_policy::Principal {
                space_id: "test_space".to_string(),
                scopes: HashSet::new(),
                clearances: Vec::new(),
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );

        let result = handler
            .execute(
                &ctx,
                serde_json::json!({"query": "machine learning", "include": ["entities"]}),
            )
            .await
            .expect("search_all");
        let response: ToolResponse<UnifiedSearchOutput> =
            serde_json::from_value(result).expect("deserialize");
        let names: Vec<String> = response
            .data
            .expect("data")
            .entities
            .expect("entities bucket")
            .items
            .iter()
            .map(|entity| entity.display_name.clone())
            .collect();

        assert!(
            names.iter().any(|name| name == "Riley Kim"),
            "unified search must use the semantic path: a query sharing no substring \
             with the name found nothing before this change; got {names:?}"
        );
    }

    #[test]
    fn test_exact_name_outranks_midword_substring() {
        // Synthetic regression: the first query term occurs inside an unrelated
        // organization name. Arrival order must not override name relevance.
        let query = "Mia Example principal";
        let terms = query_terms(query);

        let real = entity_relevance(&scoring_entity("Mia Example"), query, &terms);
        let acamia = entity_relevance(&scoring_entity("Acamia AB"), query, &terms);
        let acamia_zone = entity_relevance(&scoring_entity("Acamia Portal"), query, &terms);

        assert!(
            real > acamia && real > acamia_zone,
            "an exact name match must outrank a mid-word coincidence: \
             Mia Example={real}, Acamia AB={acamia}, Acamia Portal={acamia_zone}"
        );
    }

    #[test]
    fn test_midword_substring_scores_far_below_a_word_hit() {
        // Isolates the mid-word penalty with ONE matching term on each side, because
        // my first version of this test was VACUOUS: "Mia Example" earned two
        // boundary hits and still won even when mid-word scoring was mutated up to
        // match. Recorded rather than quietly fixed.
        //
        // Mutation: score a mid-word substring like a word-boundary hit - executed,
        // red here.
        let query = "Mia";
        let terms = query_terms(query);

        let boundary = entity_relevance(&scoring_entity("Mia Sample"), query, &terms);
        let midword = entity_relevance(&scoring_entity("Acamia AB"), query, &terms);

        assert!(
            boundary >= midword * 10,
            "a word-boundary hit must dominate a mid-word coincidence, not merely \
             edge it out: boundary={boundary}, midword={midword}"
        );
    }

    #[test]
    fn test_covering_the_whole_query_beats_one_lucky_term() {
        // "Companies Registry" was returned for "who is the
        // principal at Academy Northbank" on one lucky term. An entity matching every
        // term should win.
        //
        // Mutation: drop the all-terms-covered bonus - executed, red.
        let query = "Academy Northbank";
        let terms = query_terms(query);

        // Deliberately NOT the exact query string: an exact name match short-circuits
        // to 1000 and would mask whether the bonus fires at all. My first version of
        // this test did exactly that and a mutation deleting the bonus passed it.
        let both = entity_relevance(&scoring_entity("Northbank Academy Primary"), query, &terms);
        let one = entity_relevance(&scoring_entity("Northbank Sailing Club"), query, &terms);

        assert!(
            both > one,
            "covering both terms must beat covering one: {both} vs {one}"
        );
        assert!(
            both - one >= 300,
            "the all-tokens bonus must actually fire; it counted against terms.len(), \
             which includes the whole-query term, so it was near-dead code and a \
             mutation deleting it failed nothing: both={both}, one={one}"
        );
    }

    #[test]
    fn test_word_boundary_hit_is_not_fooled_by_substrings() {
        // Mutation: return `haystack.contains(needle)` - executed, red.
        assert!(word_boundary_hit("mia example", "mia"));
        assert!(word_boundary_hit("example, mia", "mia"));
        assert!(!word_boundary_hit("acamia ab", "mia"), "the whole point");
        assert!(!word_boundary_hit("acamia portal", "mia"));
        // Non-ASCII neighbours must not be treated as boundaries.
        assert!(word_boundary_hit("morgan sjöberg", "morgan"));
        assert!(!word_boundary_hit("sjöbergsson", "sjöberg"));
    }

    #[test]
    fn test_unified_search_defaults() {
        let req: UnifiedSearchInput =
            serde_json::from_str(r#"{ "query": "hello" }"#).expect("deserialize");
        assert_eq!(req.limit, 10);
        assert!(!req.include_deleted);
        assert!(req.include.is_none());
    }

    const DOC_SCHEMA: &str = "DEFINE TABLE doc SCHEMALESS;";
    const ENTITY_SCHEMA: &str = "DEFINE TABLE entity SCHEMALESS;";

    #[tokio::test]
    async fn test_search_all_ranks_the_real_match_above_a_midword_coincidence() {
        // END-TO-END through execute(), because the scoring unit tests do not cover
        // the SORT: a mutation removing `ranked.sort_by` entirely passed all of them.
        // Synthetic regression: substring-only results must not displace a full name.
        //
        // Mutation: delete the sort and keep discovery order - executed, red here.
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        use elephant_storage::{EntityRepository, Repository, StorageClient, StorageConfig};
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::{Entity, EntityType};

        use crate::authz::AuthzService;
        use crate::handlers::{create_dispatcher_with_context, ToolContext};
        use crate::tools::ToolResponse;

        static RANK_DB_COUNTER: AtomicU64 = AtomicU64::new(60500);
        let db_id = RANK_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("mcp_search_rank_test_{db_id}"),
            ..StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(ENTITY_SCHEMA).await.expect("schema");

        let principal = elephant_policy::Principal {
            space_id: "test_space".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let dispatcher = create_dispatcher_with_context(ctx);

        let entity_repo = EntityRepository::new(storage.clone());
        // Insert the decoys FIRST so discovery order favours them, exactly as in
        // production where "Mia" is term one and matches "Acamia" before anything else.
        for name in [
            "Acamia AB",
            "Acamia Portal",
            "Acamia Support",
            "Mia Example",
        ] {
            let entity = Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "test_space".to_string(),
                    "test".to_string(),
                ),
                EntityType::new("person"),
                name.to_string(),
            );
            entity_repo.create(entity).await.expect("create entity");
        }

        // limit 2 against THREE decoys inserted first: without collecting past the
        // limit before ranking, the loop fills with decoys and breaks before it ever
        // reaches the real entity, so no amount of ranking can save it. A mutation
        // reverting the widening while keeping the sort passed a limit-10 version of
        // this test, so the small limit is the point.
        let params = serde_json::json!({
            "query": "Mia Example principal",
            "include": ["entities"],
            "limit": 2
        });
        let result = dispatcher
            .execute("search_all", params)
            .await
            .expect("execute search_all");
        let response: ToolResponse<UnifiedSearchOutput> =
            serde_json::from_value(result).expect("deserialize");
        let data = response.data.expect("data");
        let entities = data.entities.expect("entities bucket");
        let names: Vec<String> = entities
            .items
            .iter()
            .map(|entity| entity.display_name.clone())
            .collect();

        assert!(!names.is_empty(), "the real entity must be found at all");
        assert_eq!(
            names[0], "Mia Example",
            "the exact name must rank first, ahead of mid-word matches; got {names:?}"
        );
    }

    #[tokio::test]
    async fn test_search_all_returns_mixed_results_for_multi_term_query() {
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        use elephant_storage::{
            DocRepository, EntityRepository, Repository, StorageClient, StorageConfig,
        };
        use elephant_types::ids::{RecordId, TableName};
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::{Doc, Entity, EntityType};

        use crate::authz::AuthzService;
        use crate::handlers::{create_dispatcher_with_context, ToolContext};
        use crate::tools::ToolResponse;

        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(60100);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = StorageConfig {
            database: format!("mcp_search_all_test_{db_id}"),
            ..StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(DOC_SCHEMA).await.expect("schema");
        storage.query(ENTITY_SCHEMA).await.expect("schema");

        let principal = elephant_policy::Principal {
            space_id: "test_space".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let dispatcher = create_dispatcher_with_context(ctx);

        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "test_space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Jane Smith".to_string(),
        );
        let entity_repo = EntityRepository::new(storage.clone());
        let _entity = entity_repo.create(entity).await.expect("create entity");

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "test_space".to_string(),
                "test".to_string(),
            ),
            title: "Acme Notes".to_string(),
            text: "Jane Smith works at Acme Corp.".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "rk-1".to_string(),
            metadata: serde_json::json!({}),
        };
        let doc_repo = DocRepository::new(storage.clone());
        let _ = doc_repo.create(doc).await.expect("create doc");

        let params = serde_json::json!({
            "query": "Jane Acme",
            "limit": 10,
            "include": ["entities", "docs"]
        });
        let response = dispatcher
            .execute("search_all", params)
            .await
            .expect("execute search_all");

        let decoded: ToolResponse<UnifiedSearchOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "search_all should succeed");

        let data = decoded.data.expect("data");
        assert!(data.entities.as_ref().is_some_and(|b| !b.items.is_empty()));
        assert!(data.docs.as_ref().is_some_and(|b| !b.items.is_empty()));
    }
}
