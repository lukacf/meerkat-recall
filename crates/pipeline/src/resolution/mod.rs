//! Resolution work kinds for entity resolution and synthesis (Phase 6).
//!
//! Per spec Section 9, these work kinds manage:
//! - Match key extraction from assertions
//! - Candidate entity generation (identity keys, BM25, vectors)
//! - Resolution decisions (auto-resolve, abstain)
//! - Predicate canonicalization
//! - Rel/Attribute synthesis from resolved assertions
//! - Retry mechanics for abstained assertions

pub(crate) mod reprocess;

#[cfg(feature = "meerkat-extraction")]
use std::str::FromStr;
use std::sync::Arc;
#[cfg(feature = "meerkat-extraction")]
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elephant_embeddings::EmbeddingProvider;
use elephant_storage::{
    AssertionRepository, EntityIdentityKeyRepository, EntityRepository, EntitySearchHit,
    GraphNeighbor, PredicateAliasRepository, PredicateRegistryRepository, RelRepository,
    Repository, StorageClient, WorkArtifactRepository, WorkRunRepository,
};
#[cfg(feature = "meerkat-extraction")]
use elephant_storage::{EntityWithRelations, SearchCriteria, SearchRepository};
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::ingest::{Budget, IngestMode};
use elephant_types::records::predicate::{AliasThresholds, PredicateAlias, PredicateRegistry};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};

use crate::error::PipelineError;
use crate::maintenance_commit::{same_partition, PlanningTruthMaintenanceStorage};
use crate::predicate::{can_use_for_truth, PredicateProposalData, ProposedAlias};
use crate::provenance::apply_work_run_provenance;
use crate::scheduler::budget::{BudgetCheckResult, BudgetEnforcer};
use crate::scheduler::staged_commit::StagedCommit;
use crate::truth_maintenance::TruthMaintenanceStorage;
use elephant_types::publication::AttemptFence;

// ============================================================================
// Match Keys
// ============================================================================

/// Match keys extracted from an assertion for candidate generation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchKeys {
    /// Identity keys found in the assertion text (emails, handles, etc.).
    pub identity_keys: Vec<String>,
    /// Raw name mentions for fuzzy matching.
    pub name_mentions: Vec<String>,
    /// Any explicit entity hints from extraction.
    pub entity_hints: Vec<String>,
}

/// Match keys produced by a plugin for an assertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchKeyOutput {
    /// Match keys for the subject mention.
    pub subject: MatchKeys,
    /// Match keys for the object mention, if any.
    pub object: Option<MatchKeys>,
}

impl MatchKeyOutput {
    fn empty() -> Self {
        Self {
            subject: MatchKeys::default(),
            object: None,
        }
    }
}

/// Work artifact payload for match key extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchKeyArtifact {
    /// Assertion identifier the keys belong to.
    pub assertion_id: RecordId,
    /// Match keys for the subject mention.
    pub subject: MatchKeys,
    /// Match keys for the object mention, if any.
    pub object: Option<MatchKeys>,
    /// Source plugin identifier (if available).
    pub source_plugin: Option<String>,
}

/// Optional plugin stage for extracting match keys.
#[async_trait]
pub trait MatchKeyExtractor: Send + Sync {
    /// Extracts match keys for a given assertion.
    async fn extract(&self, assertion: &Assertion) -> Result<MatchKeyOutput, PipelineError>;
    /// Stable identifier for observability.
    fn id(&self) -> &'static str;
}

/// Work artifact type for match key extraction.
pub const MATCH_KEY_ARTIFACT_TYPE: &str = "match_keys";

/// Result of match key extraction.
#[derive(Debug, Clone)]
pub struct ExtractMatchKeysResult {
    /// Number of assertions processed.
    pub assertions_processed: usize,
    /// Number of artifacts created.
    pub artifacts_created: usize,
}

/// Extracts match keys from committed work artifacts.
#[derive(Debug, Clone)]
pub struct WorkArtifactMatchKeyExtractor {
    repo: WorkArtifactRepository,
}

impl WorkArtifactMatchKeyExtractor {
    /// Creates a new extractor backed by the work artifact repository.
    #[must_use]
    pub const fn new(repo: WorkArtifactRepository) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl MatchKeyExtractor for WorkArtifactMatchKeyExtractor {
    async fn extract(&self, assertion: &Assertion) -> Result<MatchKeyOutput, PipelineError> {
        let artifact = self
            .repo
            .find_latest_by_type_and_assertion(
                &assertion.base.space_id,
                MATCH_KEY_ARTIFACT_TYPE,
                &assertion.base.id,
            )
            .await?;

        let Some(artifact) = artifact else {
            return Ok(MatchKeyOutput::empty());
        };

        let payload: MatchKeyArtifact = serde_json::from_value(artifact.data).map_err(|e| {
            PipelineError::InvalidInput(format!("Invalid match key artifact payload: {e}"))
        })?;

        if payload.assertion_id != assertion.base.id {
            return Err(PipelineError::InvalidInput(
                "Match key artifact assertion_id mismatch".to_string(),
            ));
        }

        Ok(MatchKeyOutput {
            subject: payload.subject,
            object: payload.object,
        })
    }

    fn id(&self) -> &'static str {
        "work_artifact_match_keys"
    }
}

/// Budget constraints for LLM disambiguation.
#[derive(Debug, Clone)]
pub struct DisambiguationBudget {
    /// Maximum tokens for disambiguation call.
    pub max_tokens: usize,
    /// Maximum wall time in seconds for disambiguation call.
    pub max_wall_time_secs: u64,
}

impl Default for DisambiguationBudget {
    fn default() -> Self {
        Self {
            max_tokens: 800,
            max_wall_time_secs: 15,
        }
    }
}

/// Context for disambiguation decisions.
#[derive(Debug, Clone)]
pub struct DisambiguationContext {
    /// Assertion being resolved.
    pub assertion: Assertion,
    /// Ranked candidate scores.
    pub candidates: Vec<CandidateScore>,
    /// Evidence span for the assertion.
    pub evidence: EvidenceSpan,
    /// Candidate summaries if available.
    pub candidate_summaries: Vec<CandidateSummary>,
    /// Budget constraints for disambiguation.
    pub budget: DisambiguationBudget,
}

/// Short summary for a candidate entity.
#[derive(Debug, Clone)]
pub struct CandidateSummary {
    /// Entity identifier.
    pub entity_id: RecordId,
    /// Display name for the entity.
    pub display_name: String,
    /// Optional summary text.
    pub summary: Option<String>,
    /// Alternate names/aliases.
    pub aliases: Vec<String>,
}

/// Disambiguator for ambiguous resolution cases.
#[async_trait]
pub trait ResolutionDisambiguator: Send + Sync + std::fmt::Debug {
    /// Selects the best candidate entity if possible.
    async fn disambiguate(
        &self,
        ctx: DisambiguationContext,
    ) -> Result<Option<RecordId>, PipelineError>;

    /// Identifier for the disambiguator.
    fn id(&self) -> &'static str;
}

/// Agentic disambiguator using Meerkat LLM with tool-enabled reasoning.
///
/// This disambiguator can make tool calls to gather additional context about
/// candidate entities before making a decision. Available tools:
/// - `get_entity` - Get full entity details
/// - `search_assertions` - Find other assertions mentioning an entity
/// - `get_entity_with_relations` - Get entity relationships
#[cfg(feature = "meerkat-extraction")]
#[derive(Debug)]
pub struct AgenticDisambiguator {
    storage: StorageClient,
    /// Maximum tool call iterations before forcing a decision.
    max_iterations: usize,
    /// Maximum assertions returned by repository-backed context tools.
    assertion_limit: usize,
    /// Maximum relations returned per direction by repository-backed context tools.
    relation_limit: usize,
}

#[cfg(feature = "meerkat-extraction")]
impl AgenticDisambiguator {
    /// Creates a new agentic disambiguator. The legacy model hint is retained
    /// for caller compatibility; the current space binding selects the model.
    #[must_use]
    pub fn new(_model: impl Into<String>, storage: StorageClient) -> Self {
        Self {
            storage,
            max_iterations: 3,
            assertion_limit: 5,
            relation_limit: 5,
        }
    }

    /// Sets the maximum number of tool call iterations.
    #[must_use]
    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    /// Sets bounded repository-backed context limits for tool use.
    #[must_use]
    pub fn with_context_limits(mut self, assertion_limit: usize, relation_limit: usize) -> Self {
        self.assertion_limit = assertion_limit.max(1);
        self.relation_limit = relation_limit.max(1);
        self
    }

    fn build_prompt(&self, ctx: &DisambiguationContext) -> String {
        let mut prompt = String::new();
        prompt.push_str("You are an entity resolution expert. Your task is to choose the best entity match for an assertion.\n\n");
        prompt.push_str("## Assertion to Resolve\n\n");
        prompt.push_str(&format!(
            "- **Subject**: {}\n- **Predicate**: {}\n- **Object**: {}\n\n",
            ctx.assertion.subject_text,
            ctx.assertion.predicate_raw,
            ctx.assertion
                .object_text
                .clone()
                .unwrap_or_else(|| "(none)".to_string())
        ));
        prompt.push_str(&format!(
            "**Evidence**: span [{}, {}), quote_hash: `{}`\n\n",
            ctx.evidence.start, ctx.evidence.end, ctx.evidence.quote_hash
        ));

        prompt.push_str("## Candidate Entities with Scores\n\n");
        prompt.push_str("Each candidate has a composite score and feature breakdown:\n\n");

        for (i, candidate) in ctx.candidates.iter().enumerate() {
            prompt.push_str(&format!(
                "### Candidate {} - `{}`\n",
                i + 1,
                candidate.entity_id
            ));
            prompt.push_str(&format!("- **Composite Score**: {:.3}\n", candidate.score));
            prompt.push_str("- **Feature Scores**:\n");
            for (name, score) in &candidate.features {
                prompt.push_str(&format!("  - {}: {:.3}\n", name, score));
            }

            // Add summary info if available
            if let Some(summary) = ctx
                .candidate_summaries
                .iter()
                .find(|s| s.entity_id == candidate.entity_id)
            {
                prompt.push_str(&format!("- **Display Name**: {}\n", summary.display_name));
                if !summary.aliases.is_empty() {
                    prompt.push_str(&format!("- **Aliases**: {}\n", summary.aliases.join(", ")));
                }
                if let Some(ref sum) = summary.summary {
                    prompt.push_str(&format!("- **Summary**: {}\n", sum));
                }
            }
            prompt.push('\n');
        }

        prompt.push_str("## Instructions\n\n");
        prompt.push_str("1. Review the candidate scores and summaries above.\n");
        prompt.push_str("2. If scores are close or you need more context, use the available tools to investigate.\n");
        prompt.push_str("3. Once confident, respond with ONLY the chosen entity ID (e.g., `entity:abc123`).\n\n");
        prompt.push_str("**Available Tools**:\n");
        prompt.push_str("- `get_entity`: Get full entity details including attributes\n");
        prompt.push_str("- `search_assertions`: Find other assertions mentioning this entity\n");
        prompt.push_str("- `get_entity_with_relations`: Get entity with its relationships\n\n");
        prompt.push_str("When you have enough information, respond with just the entity ID.\n");
        prompt
    }

    fn tool_definitions() -> Vec<DisambiguatorToolDef> {
        vec![
            DisambiguatorToolDef {
                name: "get_entity".to_string(),
                description: "Get full details of an entity by ID.".to_string(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "Entity ID (e.g., entity:abc123)"
                        }
                    }
                }),
            },
            DisambiguatorToolDef {
                name: "search_assertions".to_string(),
                description: "Search assertions by text query.".to_string(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Free-text search query"
                        },
                        "limit": {
                            "type": "integer",
                            "default": 10,
                            "description": "Maximum results to return"
                        }
                    }
                }),
            },
            DisambiguatorToolDef {
                name: "get_entity_with_relations".to_string(),
                description: "Get an entity with its relationships.".to_string(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": {
                            "type": "string",
                            "description": "Entity ID (e.g., entity:abc123)"
                        }
                    }
                }),
            },
        ]
    }
}

/// Tool definition for the disambiguator.
#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Clone)]
struct DisambiguatorToolDef {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

/// Tool dispatcher for disambiguation over the in-memory candidate set.
/// Uses bounded repository-backed context so the disambiguator reasons over the
/// existing world model instead of just the local candidate set.
#[cfg(feature = "meerkat-extraction")]
struct DisambiguatorToolDispatcher {
    storage: StorageClient,
    space_id: String,
    /// Candidate summaries for quick lookup.
    candidate_summaries: Vec<CandidateSummary>,
    assertion_limit: usize,
    relation_limit: usize,
}

#[cfg(feature = "meerkat-extraction")]
impl DisambiguatorToolDispatcher {
    fn new(
        storage: StorageClient,
        space_id: impl Into<String>,
        candidate_summaries: Vec<CandidateSummary>,
        assertion_limit: usize,
        relation_limit: usize,
    ) -> Self {
        Self {
            storage,
            space_id: space_id.into(),
            candidate_summaries,
            assertion_limit: assertion_limit.max(1),
            relation_limit: relation_limit.max(1),
        }
    }

    async fn dispatch(&self, name: &str, args: &serde_json::Value) -> serde_json::Value {
        match name {
            "get_entity" => {
                let id = args.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let Ok(rid) = RecordId::from_str(id) else {
                    return serde_json::json!({
                        "success": false,
                        "error": format!("Invalid entity id: {}", id)
                    });
                };

                match EntityRepository::new(self.storage.clone())
                    .get(&self.space_id, &rid)
                    .await
                {
                    Ok(Some(entity)) => serde_json::json!({
                        "success": true,
                        "data": summarize_entity(&entity)
                    }),
                    Ok(None) => {
                        if let Some(summary) = self
                            .candidate_summaries
                            .iter()
                            .find(|s| s.entity_id == rid || s.entity_id.id().to_string() == id)
                        {
                            serde_json::json!({
                                "success": true,
                                "data": {
                                    "id": summary.entity_id.to_string(),
                                    "display_name": summary.display_name,
                                    "aliases": summary.aliases,
                                    "summary": summary.summary
                                }
                            })
                        } else {
                            serde_json::json!({
                                "success": false,
                                "error": format!("Entity not found: {}", id)
                            })
                        }
                    }
                    Err(error) => serde_json::json!({
                        "success": false,
                        "error": error.to_string()
                    }),
                }
            }
            "search_assertions" => {
                let query = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let limit = args
                    .get("limit")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .map_or(self.assertion_limit, |value| {
                        value.min(self.assertion_limit)
                    });
                let criteria = SearchCriteria::new()
                    .with_space_id(self.space_id.clone())
                    .with_query(query.to_string())
                    .with_limit(limit.max(1));
                match AssertionRepository::new(self.storage.clone())
                    .search(&criteria)
                    .await
                {
                    Ok(results) => serde_json::json!({
                        "success": true,
                        "data": {
                            "items": results.results.iter().map(summarize_assertion).collect::<Vec<_>>(),
                            "total": results.total,
                            "has_more": results.has_more
                        }
                    }),
                    Err(error) => serde_json::json!({
                        "success": false,
                        "error": error.to_string()
                    }),
                }
            }
            "get_entity_with_relations" => {
                let id = args.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let Ok(rid) = RecordId::from_str(id) else {
                    return serde_json::json!({
                        "success": false,
                        "error": format!("Invalid entity id: {}", id)
                    });
                };
                match EntityRepository::new(self.storage.clone())
                    .get_with_relations(&self.space_id, &rid)
                    .await
                {
                    Ok(Some(entity)) => serde_json::json!({
                        "success": true,
                        "data": summarize_entity_with_relations(entity, self.relation_limit)
                    }),
                    Ok(None) => serde_json::json!({
                        "success": false,
                        "error": format!("Entity not found: {}", id)
                    }),
                    Err(error) => serde_json::json!({
                        "success": false,
                        "error": error.to_string()
                    }),
                }
            }
            _ => {
                serde_json::json!({
                    "success": false,
                    "error": format!("Unknown tool: {}", name)
                })
            }
        }
    }
}

#[cfg(feature = "meerkat-extraction")]
async fn collect_disambiguation_turn(
    mut stream: impl futures::Stream<Item = Result<meerkat::LlmEvent, meerkat::LlmError>> + Unpin,
) -> Result<(String, Vec<meerkat::ToolCall>), PipelineError> {
    use futures::StreamExt;
    use meerkat::{LlmDoneOutcome, LlmEvent};

    let mut content = String::new();
    let mut tool_calls = Vec::new();
    while let Some(result) = stream.next().await {
        match result
            .map_err(|error| PipelineError::ExtractionError(format!("LLM stream error: {error}")))?
        {
            LlmEvent::TextDelta { delta, .. } => content.push_str(&delta),
            LlmEvent::ToolCallComplete { id, name, args, .. } => {
                tool_calls.push(meerkat::ToolCall::new(id, name, args));
            }
            LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { .. },
            } => {
                return Ok((content, tool_calls));
            }
            LlmEvent::Done {
                outcome: LlmDoneOutcome::Error { error },
            } => {
                return Err(PipelineError::ExtractionError(format!(
                    "LLM stream error: {error}"
                )));
            }
            _ => {}
        }
    }
    Err(PipelineError::ExtractionError(
        "LLM stream ended without a completion outcome".into(),
    ))
}

#[cfg(feature = "meerkat-extraction")]
#[async_trait]
impl ResolutionDisambiguator for AgenticDisambiguator {
    async fn disambiguate(
        &self,
        ctx: DisambiguationContext,
    ) -> Result<Option<RecordId>, PipelineError> {
        use meerkat::{
            AssistantBlock, BlockAssistantMessage, LlmRequest, Message, SystemMessage, ToolDef,
            UserMessage,
        };
        use serde_json::value::RawValue;

        if ctx.budget.max_tokens == 0 {
            return Ok(None);
        }

        let context = crate::provider_dispatch::ProviderDispatchContext::new(
            elephant_storage::SpaceConfigRepository::new(self.storage.clone()),
            &ctx.assertion.base.space_id,
        );
        let (model, client, _provenance) = context
            .model_client(elephant_types::provider_binding::ModelOperation::IdentityDisambiguation)
            .await?;

        // Build tool definitions
        let tool_defs: Vec<Arc<ToolDef>> = Self::tool_definitions()
            .into_iter()
            .map(|t| Arc::new(ToolDef::new(t.name, t.description, t.input_schema)))
            .collect();

        // Create tool dispatcher with candidate context
        let candidate_ids: Vec<RecordId> =
            ctx.candidates.iter().map(|c| c.entity_id.clone()).collect();
        let dispatcher = DisambiguatorToolDispatcher::new(
            self.storage.clone(),
            ctx.assertion.base.space_id.clone(),
            ctx.candidate_summaries.clone(),
            self.assertion_limit,
            self.relation_limit,
        );

        let prompt = self.build_prompt(&ctx);
        let system_prompt = "You are a careful entity resolver. Analyze candidates and their scores, use tools if needed to gather more context, then select the most likely entity ID. When confident, respond with ONLY the entity ID.";

        let max_tokens = u32::try_from(ctx.budget.max_tokens)
            .unwrap_or(u32::MAX)
            .max(256);

        let timeout = Duration::from_secs(ctx.budget.max_wall_time_secs);
        let start = std::time::Instant::now();

        let mut messages = vec![
            Message::System(SystemMessage::new(system_prompt)),
            Message::User(UserMessage::text(prompt)),
        ];

        // Agentic loop
        for iteration in 0..self.max_iterations {
            if start.elapsed() > timeout {
                tracing::warn!("Disambiguation timed out after {} iterations", iteration);
                break;
            }

            let request = LlmRequest {
                model: model.clone(),
                messages: messages.clone(),
                tools: tool_defs.clone(),
                tool_choice: Default::default(),
                max_tokens,
                temperature: Some(0.0),
                stop_sequences: None,
                provider_params: None,
            };

            let (content, tool_calls) =
                collect_disambiguation_turn(client.stream(&request)).await?;

            // If no tool calls, try to parse the response as an entity ID
            if tool_calls.is_empty() {
                let text = content.trim();
                tracing::debug!(
                    iteration,
                    response = %text,
                    "Disambiguation response (no tool calls)"
                );

                // Try to extract entity ID from the response
                if let Some(entity_id) = extract_entity_id(text, &candidate_ids) {
                    return Ok(Some(entity_id));
                }

                // If we got a response but couldn't parse it, and this isn't the last iteration,
                // ask the model to clarify
                if iteration < self.max_iterations - 1 {
                    messages.push(Message::BlockAssistant(BlockAssistantMessage::new(
                        vec![AssistantBlock::Text {
                            text: content.clone(),
                            meta: None,
                        }],
                        meerkat::StopReason::EndTurn,
                    )));
                    messages.push(Message::User(UserMessage::text(
                        "Please respond with ONLY the entity ID (e.g., entity:abc123).".to_string(),
                    )));
                    continue;
                }

                // Last iteration - return None if we couldn't parse
                return Ok(None);
            }

            // Process tool calls
            tracing::debug!(
                iteration,
                tool_count = tool_calls.len(),
                "Processing tool calls"
            );

            // Add assistant message with tool calls
            let mut assistant_blocks = Vec::new();
            if !content.is_empty() {
                assistant_blocks.push(AssistantBlock::Text {
                    text: content.clone(),
                    meta: None,
                });
            }
            for tool_call in &tool_calls {
                let args = RawValue::from_string(
                    serde_json::to_string(&tool_call.args).unwrap_or_else(|_| "{}".to_string()),
                )
                .map_err(|err| {
                    PipelineError::ExtractionError(format!(
                        "failed to encode tool call args for transcript: {err}"
                    ))
                })?;
                assistant_blocks.push(AssistantBlock::ToolUse {
                    id: tool_call.id.clone(),
                    name: tool_call.name.clone(),
                    args,
                    meta: None,
                });
            }
            messages.push(Message::BlockAssistant(BlockAssistantMessage::new(
                assistant_blocks,
                meerkat::StopReason::ToolUse,
            )));

            // Execute tools and add results
            let mut tool_results = Vec::with_capacity(tool_calls.len());
            for tc in &tool_calls {
                let result = dispatcher.dispatch(&tc.name, &tc.args).await;
                tool_results.push(meerkat::ToolResult::new(
                    tc.id.clone(),
                    serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string()),
                    false,
                ));
            }
            messages.push(Message::tool_results(tool_results));
        }

        tracing::warn!(
            max_iterations = self.max_iterations,
            "Disambiguation reached max iterations without decision"
        );
        Ok(None)
    }

    fn id(&self) -> &'static str {
        "agentic"
    }
}

/// Extracts an entity ID from text response, validating against candidates.
#[cfg(feature = "meerkat-extraction")]
fn extract_entity_id(text: &str, candidates: &[RecordId]) -> Option<RecordId> {
    // Clean up the text - look for a line containing entity: or a candidate UUID
    let cleaned = text
        .lines()
        .find(|line| {
            line.contains("entity:")
                || candidates
                    .iter()
                    .any(|c| line.contains(&c.id().to_string()))
        })
        .unwrap_or(text)
        .trim()
        .trim_matches('`')
        .trim();

    // Try to parse as-is
    if let Ok(rid) = RecordId::from_str(cleaned) {
        if candidates.iter().any(|c| c == &rid) {
            return Some(rid);
        }
    }

    // Try adding entity: prefix
    if !cleaned.contains(':') && !cleaned.is_empty() {
        if let Ok(rid) = RecordId::from_str(&format!("entity:{cleaned}")) {
            if candidates.iter().any(|c| c == &rid) {
                return Some(rid);
            }
        }
    }

    // Look for entity ID pattern in the text
    for candidate in candidates {
        let id_str = candidate.to_string();
        let uuid_str = candidate.id().to_string();
        if cleaned.contains(&id_str) || cleaned.contains(&uuid_str) {
            return Some(candidate.clone());
        }
    }

    None
}

/// Entity creation for resolution (when no candidates found).
#[async_trait]
pub trait EntityCreator: Send + Sync {
    /// Creates or retrieves an entity with the given display name and type.
    /// Returns existing entity if one with the same name+type already exists (idempotent).
    async fn create_or_get_entity(
        &self,
        space_id: &str,
        actor: &str,
        entity_type: EntityType,
        display_name: &str,
    ) -> Result<RecordId, PipelineError>;
}

/// External dependencies for resolution.
pub struct ResolutionContext<'a> {
    /// Lookup for identity keys.
    pub identity_lookup: &'a dyn IdentityKeyLookup,
    /// Entity search repository for BM25 and vector lookup.
    pub entity_search: &'a dyn EntitySearch,
    /// Graph lookup for relationship priors.
    pub graph_lookup: &'a dyn GraphLookup,
    /// Optional plugin extractor for match keys.
    pub match_key_extractor: Option<&'a dyn MatchKeyExtractor>,
    /// Embedding provider for vector search.
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    /// Optional disambiguator for ambiguous cases.
    pub disambiguator: Option<&'a dyn ResolutionDisambiguator>,
    /// Entity creator for auto-creating entities when no candidates found.
    pub entity_creator: Option<&'a dyn EntityCreator>,
    /// Budget for disambiguation calls.
    pub disambiguation_budget: DisambiguationBudget,
    /// Candidate limit per query.
    pub candidate_limit: usize,
    /// Maximum graph hops for priors.
    pub graph_hops: usize,
}

/// Identity key lookup for resolution.
#[async_trait]
pub trait IdentityKeyLookup: Send + Sync {
    /// Finds entity and confidence for an identity key.
    async fn find_entity_by_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> Result<Option<(RecordId, f32)>, PipelineError>;
}

/// Entity search operations for resolution.
#[async_trait]
pub trait EntitySearch: Send + Sync {
    /// BM25 search for entities.
    async fn bm25_search(
        &self,
        space_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EntitySearchHit>, PipelineError>;

    /// Vector search for entities.
    async fn vector_search(
        &self,
        space_id: &str,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<EntitySearchHit>, PipelineError>;

    /// Fetches entity details for summaries.
    async fn get_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> Result<Option<Entity>, PipelineError>;
}

/// Graph lookup for relationship priors.
#[async_trait]
pub trait GraphLookup: Send + Sync {
    /// Returns entities within N hops of a start entity.
    async fn neighbors_within_hops(
        &self,
        space_id: &str,
        start: &RecordId,
        hops: usize,
    ) -> Result<Vec<GraphNeighbor>, PipelineError>;
}

#[async_trait]
impl IdentityKeyLookup for EntityIdentityKeyRepository {
    async fn find_entity_by_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> Result<Option<(RecordId, f32)>, PipelineError> {
        let record = self
            .find_by_key(space_id, key)
            .await
            .map_err(PipelineError::StorageError)?;
        Ok(record.map(|item| (item.entity.id().clone(), item.confidence as f32)))
    }
}

#[async_trait]
impl EntitySearch for EntityRepository {
    async fn bm25_search(
        &self,
        space_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EntitySearchHit>, PipelineError> {
        self.bm25_search_entities(space_id, query, limit)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn vector_search(
        &self,
        space_id: &str,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<EntitySearchHit>, PipelineError> {
        self.vector_search_entities(space_id, embedding, limit)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> Result<Option<Entity>, PipelineError> {
        Repository::get(self, space_id, entity_id)
            .await
            .map_err(PipelineError::StorageError)
    }
}

#[async_trait]
impl GraphLookup for RelRepository {
    async fn neighbors_within_hops(
        &self,
        space_id: &str,
        start: &RecordId,
        hops: usize,
    ) -> Result<Vec<GraphNeighbor>, PipelineError> {
        self.get_entities_within_n_hops(space_id, start, hops)
            .await
            .map_err(PipelineError::StorageError)
    }
}

#[async_trait]
impl EntityCreator for EntityRepository {
    async fn create_or_get_entity(
        &self,
        space_id: &str,
        actor: &str,
        entity_type: EntityType,
        display_name: &str,
    ) -> Result<RecordId, PipelineError> {
        // Idempotency: check for existing entity with same name
        if let Some(existing) = self
            .find_by_display_name(space_id, display_name)
            .await
            .map_err(PipelineError::StorageError)?
        {
            return Ok(existing.base.id);
        }

        // Create new entity
        let entity_id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(entity_id, space_id.to_string(), actor.to_string());
        let entity = Entity::new(base, entity_type, display_name.to_string());

        let created = self
            .create(entity)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(created.base.id)
    }
}

/// Extracts match keys from an assertion for candidate generation.
///
/// Per spec §9: extracts identity hints, name mentions, and other
/// signals that can be used to find candidate entities.
pub fn extract_match_keys(assertion: &Assertion) -> MatchKeys {
    let mut identity_keys = Vec::new();
    let mut name_mentions = Vec::new();

    // Extract from subject text
    extract_keys_from_text(
        &assertion.subject_text,
        &mut identity_keys,
        &mut name_mentions,
    );

    // Extract from object text if present
    if let Some(ref object_text) = assertion.object_text {
        extract_keys_from_text(object_text, &mut identity_keys, &mut name_mentions);
    }

    MatchKeys {
        identity_keys,
        name_mentions,
        entity_hints: Vec::new(), // Explicit hints would come from extraction metadata
    }
}

/// Extracts match keys from a single text span.
#[must_use]
pub fn extract_match_keys_from_text(text: &str) -> MatchKeys {
    let mut identity_keys = Vec::new();
    let mut name_mentions = Vec::new();
    extract_keys_from_text(text, &mut identity_keys, &mut name_mentions);
    MatchKeys {
        identity_keys,
        name_mentions,
        entity_hints: Vec::new(),
    }
}

/// Extracts identity keys and name mentions from text.
///
/// Per spec §3.1: Core does NOT implement typed pattern recognition (email, handle, etc.).
/// Identity key detection is delegated to plugins via `IdentityKeyExtractor` trait.
/// This function treats all text as name mentions; identity keys must be provided
/// out-of-band by plugins and are not inferred from the raw mention text.
fn extract_keys_from_text(
    text: &str,
    _identity_keys: &mut [String],
    name_mentions: &mut Vec<String>,
) {
    if !text.is_empty() {
        name_mentions.push(text.to_string());
    }
}

// ============================================================================
// Candidate Generation
// ============================================================================

/// Source of a candidate entity match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateSource {
    /// Matched via identity key (email, handle, etc.).
    IdentityKey,
    /// Matched via BM25 text similarity.
    Bm25,
    /// Matched via vector embedding similarity.
    Vector,
    /// Matched via graph priors (relationship-based inference).
    GraphPrior,
}

/// A candidate entity for resolution.
#[derive(Debug, Clone)]
pub struct CandidateEntity {
    /// The entity ID.
    pub entity_id: RecordId,
    /// Raw score from the matching method.
    pub raw_score: f32,
    /// Source of the match.
    pub source: CandidateSource,
}

/// Weighted score for a candidate entity.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateScore {
    /// The entity ID.
    pub entity_id: RecordId,
    /// Final weighted score (0.0 to 1.0).
    pub score: f32,
    /// Individual feature scores for debugging.
    pub features: Vec<(String, f32)>,
}

/// Configuration for candidate scoring weights.
#[derive(Debug, Clone)]
pub struct ScoringWeights {
    /// Weight for identity key matches.
    pub identity_key: f32,
    /// Weight for BM25 matches.
    pub bm25: f32,
    /// Weight for vector matches.
    pub vector: f32,
    /// Weight for graph prior matches.
    pub graph_prior: f32,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            identity_key: 1.0,
            bm25: 1.3,
            vector: 0.3,
            graph_prior: 0.4, // Between BM25 and vector in relevance
        }
    }
}

/// Generates candidate entities from identity key matches.
///
/// Per spec §9: exact identity key matches (email, handle) are highest confidence.
pub async fn generate_candidates_from_identity_keys(
    match_keys: &MatchKeys,
    space_id: &str,
    lookup: &dyn IdentityKeyLookup,
) -> Result<Vec<CandidateEntity>, PipelineError> {
    let mut candidates = Vec::new();
    let mut seen: std::collections::HashMap<RecordId, f32> = std::collections::HashMap::new();

    let possible_keys = match_keys
        .identity_keys
        .iter()
        .chain(match_keys.name_mentions.iter());

    for raw_key in possible_keys {
        let key = raw_key.trim();
        if key.is_empty() {
            continue;
        }

        if let Some((entity_id, confidence)) = lookup.find_entity_by_key(space_id, key).await? {
            let score = confidence.clamp(0.0, 1.0);
            seen.entry(entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }
    }

    for (entity_id, score) in seen {
        candidates.push(CandidateEntity {
            entity_id,
            raw_score: score,
            source: CandidateSource::IdentityKey,
        });
    }

    Ok(candidates)
}

/// Generates candidate entities from BM25 text similarity.
///
/// Per spec §9: BM25 provides lexical matching for name mentions.
///
/// Uses repository BM25 search against mention text and merges top hits.
pub async fn generate_candidates_from_bm25(
    match_keys: &MatchKeys,
    space_id: &str,
    repo: &dyn EntitySearch,
    limit: usize,
) -> Result<Vec<CandidateEntity>, PipelineError> {
    let mut candidates = Vec::new();
    let mut merged: std::collections::HashMap<RecordId, f32> = std::collections::HashMap::new();

    for mention in &match_keys.name_mentions {
        if mention.trim().is_empty() {
            continue;
        }
        let hits = repo.bm25_search(space_id, mention, limit).await?;
        for hit in hits {
            let score = hit.score.clamp(0.0, 1.0);
            merged
                .entry(hit.entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }
    }

    for (entity_id, score) in merged {
        candidates.push(CandidateEntity {
            entity_id,
            raw_score: score,
            source: CandidateSource::Bm25,
        });
    }

    Ok(candidates)
}

/// Generates candidate entities from vector embedding similarity.
///
/// Per spec §9: vector search provides semantic matching.
///
/// Uses embedding similarity search for mention text and merges top hits.
pub async fn generate_candidates_from_vectors(
    match_keys: &MatchKeys,
    space_id: &str,
    repo: &dyn EntitySearch,
    embedder: Arc<dyn EmbeddingProvider>,
    limit: usize,
) -> Result<Vec<CandidateEntity>, PipelineError> {
    let mut candidates = Vec::new();
    let mentions: Vec<String> = match_keys
        .name_mentions
        .iter()
        .filter(|m| !m.trim().is_empty())
        .cloned()
        .collect();

    if mentions.is_empty() {
        return Ok(candidates);
    }

    let embeddings = embedder
        .embed(&mentions)
        .await
        .map_err(PipelineError::EmbeddingError)?;

    for embedding in embeddings {
        let hits = repo.vector_search(space_id, &embedding, limit).await?;
        for hit in hits {
            let score = normalize_vector_score(hit.score);
            candidates.push(CandidateEntity {
                entity_id: hit.entity_id,
                raw_score: score,
                source: CandidateSource::Vector,
            });
        }
    }

    Ok(candidates)
}

/// Generates candidate entities from graph priors (relationship-based).
///
/// Per spec §9: uses existing relationships to infer likely matches,
/// without introducing predicate-level semantics in core logic.
pub async fn generate_candidates_from_graph_priors(
    seed_entities: &[RecordId],
    space_id: &str,
    graph: &dyn GraphLookup,
    max_hops: usize,
) -> Result<Vec<CandidateEntity>, PipelineError> {
    let mut candidates = Vec::new();
    let mut scored: std::collections::HashMap<RecordId, f32> = std::collections::HashMap::new();

    if seed_entities.is_empty() || max_hops == 0 {
        return Ok(candidates);
    }

    for seed in seed_entities {
        let neighbors = graph
            .neighbors_within_hops(space_id, seed, max_hops)
            .await?;
        for neighbor in neighbors {
            let distance = neighbor.distance.max(1) as f32;
            let type_weight = graph_prior_type_weight(&neighbor.rel_type);
            let score = ((1.0 / distance) * type_weight).clamp(0.0, 1.0);
            scored
                .entry(neighbor.entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }
    }

    for (entity_id, score) in scored {
        candidates.push(CandidateEntity {
            entity_id,
            raw_score: score,
            source: CandidateSource::GraphPrior,
        });
    }

    Ok(candidates)
}

fn graph_prior_type_weight(rel_type: &str) -> f32 {
    if rel_type.trim().is_empty() {
        return 0.85;
    }

    let mut hash: u64 = 0;
    for byte in rel_type.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(u64::from(*byte));
    }

    let bucket = (hash % 100) as f32 / 100.0;
    (0.7 + (0.3 * bucket)).clamp(0.7, 1.0)
}

fn normalize_vector_score(score: f32) -> f32 {
    if score.is_nan() {
        0.0
    } else {
        score.clamp(0.0, 1.0)
    }
}

#[cfg(feature = "meerkat-extraction")]
fn summarize_entity(entity: &Entity) -> serde_json::Value {
    serde_json::json!({
        "id": entity.base.id.to_string(),
        "display_name": entity.display_name,
        "entity_type": entity.entity_type,
        "aliases": entity.aliases,
        "summary": entity.summary,
        "status": entity.status,
    })
}

#[cfg(feature = "meerkat-extraction")]
fn summarize_assertion(assertion: &Assertion) -> serde_json::Value {
    serde_json::json!({
        "id": assertion.base.id.to_string(),
        "subject_text": assertion.subject_text,
        "predicate_raw": assertion.predicate_raw,
        "object_text": assertion.object_text,
        "resolution_status": assertion.resolution_status,
        "confidence": assertion.confidence,
        "subject_entity": assertion.subject_entity.as_ref().map(|link| link.id().to_string()),
        "object_entity": assertion.object_entity.as_ref().map(|link| link.id().to_string()),
        "evidence": {
            "start": assertion.evidence.start,
            "end": assertion.evidence.end,
            "quote_hash": assertion.evidence.quote_hash,
        }
    })
}

#[cfg(feature = "meerkat-extraction")]
fn summarize_rel(rel: &elephant_types::records::rel::Rel) -> serde_json::Value {
    serde_json::json!({
        "id": rel.base.id.to_string(),
        "rel_type": rel.rel_type,
        "subject": rel.subject.id().to_string(),
        "object": rel.object.id().to_string(),
        "status": rel.status,
        "confidence": rel.confidence,
    })
}

#[cfg(feature = "meerkat-extraction")]
fn summarize_entity_with_relations(
    entity: EntityWithRelations,
    relation_limit: usize,
) -> serde_json::Value {
    let outgoing_relations = entity
        .outgoing_relations
        .iter()
        .take(relation_limit)
        .map(summarize_rel)
        .collect::<Vec<_>>();
    let incoming_relations = entity
        .incoming_relations
        .iter()
        .take(relation_limit)
        .map(summarize_rel)
        .collect::<Vec<_>>();

    serde_json::json!({
        "entity": summarize_entity(&entity.entity),
        "outgoing_relations": outgoing_relations,
        "incoming_relations": incoming_relations,
        "total_outgoing_relations": entity.outgoing_relations.len(),
        "total_incoming_relations": entity.incoming_relations.len(),
    })
}

/// Scores and ranks candidate entities.
///
/// Combines candidates from multiple sources with weighted scoring.
/// Groups candidates by entity_id and aggregates their scores.
pub fn score_candidates(
    candidates: &[CandidateEntity],
    weights: &ScoringWeights,
) -> Vec<CandidateScore> {
    use std::collections::HashMap;

    // Group candidates by entity_id and collect their weighted scores
    let mut entity_scores: HashMap<RecordId, Vec<(String, f32)>> = HashMap::new();

    for candidate in candidates {
        let (weight, feature_name) = match candidate.source {
            CandidateSource::IdentityKey => (weights.identity_key, "identity_key"),
            CandidateSource::Bm25 => (weights.bm25, "bm25"),
            CandidateSource::Vector => (weights.vector, "vector"),
            CandidateSource::GraphPrior => (weights.graph_prior, "graph_prior"),
        };

        let weighted_score = candidate.raw_score * weight;
        entity_scores
            .entry(candidate.entity_id.clone())
            .or_default()
            .push((feature_name.to_string(), weighted_score));
    }

    // Convert to CandidateScore, computing final score as sum of weighted features
    let mut scores: Vec<CandidateScore> = entity_scores
        .into_iter()
        .map(|(entity_id, features)| {
            let total_score: f32 = features.iter().map(|(_, s)| s).sum();
            // Normalize to [0, 1] by clamping (weights could exceed 1.0 in aggregate)
            let normalized_score = total_score.clamp(0.0, 1.0);

            CandidateScore {
                entity_id,
                score: normalized_score,
                features,
            }
        })
        .collect();

    // Sort by score descending (best candidates first)
    scores.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    scores
}

#[allow(missing_docs)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolutionPatch {
    /// Epoch captured before decision/candidate/provider reads. Background
    /// decisions must retain it through staging, validation and commit.
    #[serde(default)]
    pub prepared_epoch: Option<String>,
    pub assertion_id: RecordId,
    pub resolved_subject: Option<RecordId>,
    pub resolved_object: Option<RecordId>,
    pub confidence: f32,
    pub resolution_method: String,
    pub status: ResolutionStatus,
    pub abstention_reason: Option<String>,
}

/// Artifact type used for staged assertion resolution patches.
pub const RESOLUTION_PATCH_ARTIFACT_TYPE: &str = "resolution_patch";

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub base_delay_secs: i64,
    pub max_delay_secs: i64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_delay_secs: 300,
            max_delay_secs: 86_400,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct RetryState {
    attempts: u32,
    last_at: Option<DateTime<Utc>>,
    reason: Option<String>,
}

impl From<&AssertionRetryState> for RetryState {
    fn from(state: &AssertionRetryState) -> Self {
        Self {
            attempts: state.resolution_attempts,
            last_at: state.last_resolution_at,
            reason: state.abstention_reason.clone(),
        }
    }
}

fn update_retry_state(state: &mut RetryState, reason: Option<String>, _backoff: &BackoffConfig) {
    state.attempts += 1;
    state.last_at = Some(Utc::now());
    state.reason = reason;
}

fn apply_resolution_patch_to_assertion(
    assertion: Assertion,
    patch: &ResolutionPatch,
    backoff_config: &BackoffConfig,
    max_attempts: Option<u32>,
) -> Assertion {
    let mut updated = assertion;
    updated.resolution_status = patch.status;
    updated.resolution_method = Some(patch.resolution_method.clone());
    updated.resolution_confidence = Some(patch.confidence);
    updated.subject_entity = patch.resolved_subject.clone().map(Link::new);
    updated.object_entity = patch.resolved_object.clone().map(Link::new);

    if patch.status == ResolutionStatus::Abstained {
        let mut retry_state = RetryState::from(&updated.retry_state);
        update_retry_state(
            &mut retry_state,
            patch.abstention_reason.clone(),
            backoff_config,
        );
        updated.retry_state = AssertionRetryState {
            resolution_attempts: retry_state.attempts,
            last_resolution_at: retry_state.last_at,
            abstention_reason: retry_state.reason,
        };
        if let Some(max_attempts) = max_attempts {
            if retry_state.attempts >= max_attempts {
                updated.resolution_status = ResolutionStatus::Failed;
            }
        }
    } else if patch.status == ResolutionStatus::Resolved {
        updated.retry_state = AssertionRetryState::default();
    }

    updated
}

/// Validates a resolution patch against the current repository-backed world model.
///
/// # Errors
///
/// Returns a pipeline/storage error if repository access fails while computing validation errors.
pub async fn validate_resolution_patch_payload(
    storage: &StorageClient,
    space_id: &str,
    patch: &ResolutionPatch,
) -> Result<Vec<String>, PipelineError> {
    let assertion_repo = AssertionRepository::new(storage.clone());
    let entity_repo = EntityRepository::new(storage.clone());
    let mut errors = Vec::new();

    if !patch.confidence.is_finite() || !(0.0..=1.0).contains(&patch.confidence) {
        errors.push("Resolution patch confidence must be between 0.0 and 1.0".to_string());
    }
    if patch.resolution_method.trim().is_empty() {
        errors.push("Resolution patch method cannot be empty".to_string());
    }
    if patch.status == ResolutionStatus::Abstained
        && patch
            .abstention_reason
            .as_deref()
            .is_none_or(|reason| reason.trim().is_empty())
    {
        errors.push("Abstained resolution patches require an abstention_reason".to_string());
    }
    if patch.status == ResolutionStatus::Resolved
        && patch.resolved_subject.is_none()
        && patch.resolved_object.is_none()
    {
        errors.push(
            "Resolved resolution patches must provide at least one resolved entity".to_string(),
        );
    }

    let assertion = assertion_repo.get(space_id, &patch.assertion_id).await?;
    let Some(assertion) = assertion else {
        errors.push(format!("Assertion not found: {}", patch.assertion_id));
        return Ok(errors);
    };
    if assertion.resolution_status != ResolutionStatus::Pending {
        errors.push(format!(
            "Resolution patch target assertion is no longer pending: {}",
            patch.assertion_id
        ));
    }

    if let Some(entity_id) = patch.resolved_subject.as_ref() {
        if entity_repo.get(space_id, entity_id).await?.is_none() {
            errors.push(format!("Resolved subject entity not found: {entity_id}"));
        }
    }
    if let Some(entity_id) = patch.resolved_object.as_ref() {
        if entity_repo.get(space_id, entity_id).await?.is_none() {
            errors.push(format!("Resolved object entity not found: {entity_id}"));
        }
    }

    Ok(errors)
}

/// Applies a validated resolution patch directly to the canonical assertion.
///
/// # Errors
///
/// Returns an error if the target assertion is missing or the update fails.
pub async fn apply_resolution_patch(
    storage: &StorageClient,
    space_id: &str,
    patch: &ResolutionPatch,
    backoff_config: &BackoffConfig,
    max_attempts: Option<u32>,
) -> Result<(), PipelineError> {
    apply_resolution_patches_with_attempt(
        storage,
        space_id,
        &patch.assertion_id.to_string(),
        std::slice::from_ref(patch),
        backoff_config,
        max_attempts,
        None,
    )
    .await
}

/// Apply every resolution patch in one canonical transaction with audit/outbox.
/// The stable commit identity is normally the bundle artifact ID. Background
/// callers must pass the original claimed attempt, never reconstruct a new one.
#[allow(clippy::too_many_arguments)]
pub async fn apply_resolution_patches_with_attempt(
    storage: &StorageClient,
    space_id: &str,
    commit_id: &str,
    patches: &[ResolutionPatch],
    backoff_config: &BackoffConfig,
    max_attempts: Option<u32>,
    attempt: Option<AttemptFence>,
) -> Result<(), PipelineError> {
    let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, attempt).await?;
    let request = resolution_request(patches, backoff_config, max_attempts);
    if planning
        .replay::<()>("resolution", commit_id, &request)
        .await?
        .is_some()
    {
        return Ok(());
    }
    prepare_resolution_patches(&planning, space_id, patches, backoff_config, max_attempts).await?;
    planning
        .commit("resolution", commit_id, &request, &())
        .await?;
    Ok(())
}

fn resolution_request(
    patches: &[ResolutionPatch],
    backoff: &BackoffConfig,
    max_attempts: Option<u32>,
) -> serde_json::Value {
    serde_json::json!({"patches": patches, "base_delay_secs": backoff.base_delay_secs,
        "max_delay_secs": backoff.max_delay_secs, "max_attempts": max_attempts})
}

/// Prepare patches against the same epoch/read set as their surrounding bundle.
/// No live mutation occurs if a later patch, evidence check or precondition fails.
pub async fn prepare_resolution_patches(
    planning: &PlanningTruthMaintenanceStorage,
    space_id: &str,
    patches: &[ResolutionPatch],
    backoff: &BackoffConfig,
    max_attempts: Option<u32>,
) -> Result<(), PipelineError> {
    let mut seen = std::collections::HashSet::new();
    for patch in patches {
        if let Some(expected) = &patch.prepared_epoch {
            if planning.prepared_epoch()?.as_ref() != Some(expected) {
                return Err(PipelineError::InvalidState(
                    "resolution decision inputs changed; reprepare".to_owned(),
                ));
            }
        } else if planning.has_attempt() {
            return Err(PipelineError::InvalidState(
                "background resolution lacks its original decision epoch".to_owned(),
            ));
        }

        if !seen.insert(patch.assertion_id.clone()) {
            return Err(PipelineError::InvalidInput(
                "resolution batch contains duplicate assertion targets".to_owned(),
            ));
        }
        if !patch.confidence.is_finite()
            || !(0.0..=1.0).contains(&patch.confidence)
            || patch.resolution_method.trim().is_empty()
            || (patch.status == ResolutionStatus::Abstained
                && patch
                    .abstention_reason
                    .as_deref()
                    .is_none_or(|s| s.trim().is_empty()))
            || (patch.status == ResolutionStatus::Resolved
                && patch.resolved_subject.is_none()
                && patch.resolved_object.is_none())
        {
            return Err(PipelineError::InvalidInput(
                "invalid resolution patch payload".to_owned(),
            ));
        }
        let before = planning
            .get_assertion(space_id, &patch.assertion_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(patch.assertion_id.to_string()))?;
        if before.resolution_status != ResolutionStatus::Pending {
            return Err(PipelineError::InvalidState(format!(
                "assertion {} is no longer pending",
                patch.assertion_id
            )));
        }
        planning.validate_assertion_evidence(&before).await?;
        for id in patch
            .resolved_subject
            .iter()
            .chain(patch.resolved_object.iter())
        {
            let entity = planning
                .get_entity(space_id, id)
                .await?
                .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
            if !same_partition(&before.base, &entity.base)? {
                return Err(PipelineError::InvalidState(
                    "resolved entity crosses the assertion support/security partition".to_owned(),
                ));
            }
        }
        let mut after =
            apply_resolution_patch_to_assertion(before.clone(), patch, backoff, max_attempts);
        after.base.updated_at = Utc::now();
        planning.stage_assertion(before.clone(), after.clone())?;
        let now = Utc::now();
        planning
            .create_audit_log(elephant_types::records::AuditLog {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::AuditLog),
                    space_id.to_owned(),
                    "resolution".to_owned(),
                ),
                action: elephant_types::records::AuditAction::Update,
                actor: "resolution".to_owned(),
                record: patch.assertion_id.clone(),
                before: Some(
                    serde_json::to_value(&before)
                        .map_err(|e| PipelineError::InvalidInput(e.to_string()))?,
                ),
                after: Some(
                    serde_json::to_value(&after)
                        .map_err(|e| PipelineError::InvalidInput(e.to_string()))?,
                ),
                timestamp: now,
                reason: Some("resolution_patch".to_owned()),
            })
            .await?;
    }
    Ok(())
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ResolutionPatchWorkflow {
    attempt: Option<AttemptFence>,
    staged_commit: StagedCommit,
    storage: StorageClient,
}

impl ResolutionPatchWorkflow {
    /// Creates a workflow instance backed by the provided storage client.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            attempt: None,
            staged_commit: StagedCommit::new(storage.clone()),
            storage,
        }
    }

    /// Bind compatibility dispatch to the original currently claimed attempt.
    #[must_use]
    pub fn with_attempt(mut self, attempt: AttemptFence) -> Self {
        self.attempt = Some(attempt);
        self
    }

    /// Stages a draft resolution patch artifact for later validation and commit.
    pub async fn stage_resolution_patch(
        &self,
        work_run_id: &RecordId,
        patch: ResolutionPatch,
        space_id: &str,
    ) -> Result<WorkArtifact, PipelineError> {
        let assertion = AssertionRepository::new(self.storage.clone())
            .get(space_id, &patch.assertion_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(patch.assertion_id.to_string()))?;
        let run = WorkRunRepository::new(self.storage.clone())
            .get(space_id, work_run_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(work_run_id.to_string()))?;
        let mut base = assertion.base.clone();
        elephant_storage::public_mutation::inherit_public_envelope(&mut base, &run.base)?;
        base.id = RecordId::new_v4(TableName::WorkArtifact);
        base.created_at = Utc::now();
        base.updated_at = base.created_at;
        base.created_by = "resolution_patch_workflow".into();
        base.deleted_at = None;
        apply_work_run_provenance(&self.storage, work_run_id, space_id, &mut base.provenance)
            .await?;

        let artifact = WorkArtifact {
            base,
            work_run: Link::new(work_run_id.clone()),
            artifact_type: RESOLUTION_PATCH_ARTIFACT_TYPE.to_string(),
            data: serde_json::to_value(&patch).map_err(|e| {
                PipelineError::InvalidInput(format!("Failed to serialize resolution patch: {e}"))
            })?,
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };
        self.staged_commit.stage(artifact).await
    }

    /// Validates a staged resolution patch artifact.
    pub async fn validate_resolution_patch(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
    ) -> Result<WorkArtifact, PipelineError> {
        let artifact_repo = WorkArtifactRepository::new(self.storage.clone());
        let artifact = artifact_repo
            .get(space_id, artifact_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;
        if artifact.artifact_type != RESOLUTION_PATCH_ARTIFACT_TYPE {
            return Err(PipelineError::InvalidInput(format!(
                "Invalid artifact type: {}",
                artifact.artifact_type
            )));
        }
        let patch: ResolutionPatch =
            serde_json::from_value(artifact.data.clone()).map_err(|e| {
                PipelineError::InvalidInput(format!("Invalid resolution patch data: {e}"))
            })?;
        let errors = validate_resolution_patch_payload(&self.storage, space_id, &patch).await?;
        self.staged_commit
            .validate(space_id, artifact_id, move |_artifact| {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors)
                }
            })
            .await
    }

    /// Commits a validated resolution patch with its artifact and propagation.
    pub async fn commit_resolution_patch(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
        backoff_config: &BackoffConfig,
        max_attempts: Option<u32>,
    ) -> Result<WorkArtifact, PipelineError> {
        self.commit_resolution_patch_batch(
            space_id,
            std::slice::from_ref(artifact_id),
            backoff_config,
            max_attempts,
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| PipelineError::InvalidState("resolution batch returned no artifact".into()))
    }

    /// Apply a complete exposed batch, including all artifact statuses, atomically.
    pub async fn commit_resolution_patch_batch(
        &self,
        space_id: &str,
        artifact_ids: &[RecordId],
        backoff_config: &BackoffConfig,
        max_attempts: Option<u32>,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        self.commit_resolution_artifact_batch(
            space_id,
            artifact_ids,
            backoff_config,
            max_attempts,
            false,
        )
        .await
    }

    /// Atomically applies validated resolution bundles and their exact source
    /// artifacts. Every bundle must retain the original decision epoch and run.
    pub async fn commit_resolution_bundle_batch(
        &self,
        space_id: &str,
        artifact_ids: &[RecordId],
        backoff_config: &BackoffConfig,
        max_attempts: Option<u32>,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        self.commit_resolution_artifact_batch(
            space_id,
            artifact_ids,
            backoff_config,
            max_attempts,
            true,
        )
        .await
    }

    async fn commit_resolution_artifact_batch(
        &self,
        space_id: &str,
        artifact_ids: &[RecordId],
        backoff_config: &BackoffConfig,
        max_attempts: Option<u32>,
        bundles: bool,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        use crate::scheduler::staged_commit::{
            CommitBundleArtifactData, CommitBundleKind, CommitBundlePayload,
            COMMIT_BUNDLE_ARTIFACT_TYPE,
        };
        if artifact_ids.is_empty() {
            return Ok(Vec::new());
        }
        let planning = PlanningTruthMaintenanceStorage::new(
            self.storage.clone(),
            space_id,
            self.attempt.clone(),
        )
        .await?;
        let artifact_repo = WorkArtifactRepository::new(self.storage.clone());
        let mut ids = artifact_ids.to_vec();
        ids.sort();
        ids.dedup();
        if ids.len() != artifact_ids.len() {
            return Err(PipelineError::InvalidInput(
                "duplicate resolution artifact".into(),
            ));
        }
        let mut artifacts = Vec::new();
        let mut patches = Vec::new();
        let mut source_artifacts = Vec::new();
        let mut source_ids = std::collections::HashSet::new();
        for id in &ids {
            let artifact = artifact_repo
                .get(space_id, id)
                .await?
                .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
            let patch = if bundles {
                if artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE {
                    return Err(PipelineError::InvalidInput(
                        "not a resolution bundle".into(),
                    ));
                }
                let bundle: CommitBundleArtifactData =
                    serde_json::from_value(artifact.data.clone()).map_err(|e| {
                        PipelineError::InvalidInput(format!("invalid resolution bundle: {e}"))
                    })?;
                let CommitBundlePayload::ResolutionPatchSet(patch) = bundle.payload else {
                    return Err(PipelineError::InvalidInput(
                        "not a resolution bundle payload".into(),
                    ));
                };
                if bundle.bundle_kind != CommitBundleKind::ResolutionPatchSet
                    || bundle.source_artifact_type != RESOLUTION_PATCH_ARTIFACT_TYPE
                    || patch.prepared_epoch.is_none()
                {
                    return Err(PipelineError::InvalidState(
                        "resolution bundle lacks original decision authority".into(),
                    ));
                }
                let source_id = bundle.source_artifact_id.parse::<RecordId>().map_err(|e| {
                    PipelineError::InvalidInput(format!("invalid resolution source artifact: {e}"))
                })?;
                if !source_ids.insert(source_id.clone()) {
                    return Err(PipelineError::InvalidInput(
                        "duplicate resolution source artifact".into(),
                    ));
                }
                let source = artifact_repo
                    .get(space_id, &source_id)
                    .await?
                    .ok_or_else(|| PipelineError::NotFound(source_id.to_string()))?;
                let encoded_patch = serde_json::to_value(&patch).map_err(|e| {
                    PipelineError::InvalidInput(format!("invalid resolution patch: {e}"))
                })?;
                if source.artifact_type != RESOLUTION_PATCH_ARTIFACT_TYPE
                    || source.work_run != artifact.work_run
                    || !same_partition(&source.base, &artifact.base)?
                    || source.data != encoded_patch
                {
                    return Err(PipelineError::InvalidState(
                        "resolution bundle differs from its original source artifact".into(),
                    ));
                }
                source_artifacts.push(source);
                patch
            } else {
                if artifact.artifact_type != RESOLUTION_PATCH_ARTIFACT_TYPE {
                    return Err(PipelineError::InvalidInput(
                        "not a resolution patch artifact".into(),
                    ));
                }
                serde_json::from_value(artifact.data.clone()).map_err(|e| {
                    PipelineError::InvalidInput(format!("invalid resolution patch: {e}"))
                })?
            };
            artifacts.push(artifact);
            patches.push(patch);
        }
        let patch_request = resolution_request(&patches, backoff_config, max_attempts);
        let kind = if bundles {
            "resolution_bundles"
        } else {
            "resolution_artifacts"
        };
        let request = if bundles {
            // Lifecycle timestamps/statuses change on success; immutable request
            // identity and original authority must remain bound on replay.
            let original_artifacts: Vec<_> = artifacts
                .iter()
                .chain(&source_artifacts)
                .map(|row| {
                    serde_json::json!({"id":row.base.id,"data":row.data,"work_run":row.work_run,
                    "security":row.base.security,"subjects":row.base.subjects,
                    "subjects_state":row.base.subjects_state,"provenance":row.base.provenance})
                })
                .collect();
            serde_json::json!({"resolution":patch_request,"artifacts":original_artifacts})
        } else {
            patch_request
        };
        let scope = crate::maintenance_commit::request_fingerprint(&ids)?;
        if let Some(previous) = planning.replay(kind, &scope, &request).await? {
            return Ok(previous);
        }
        if artifacts
            .iter()
            .chain(&source_artifacts)
            .any(|row| row.status != ArtifactStatus::Validated)
        {
            return Err(PipelineError::InvalidState(
                "resolution batch has an artifact without a matching complete receipt".into(),
            ));
        }
        let mut runs = std::collections::HashSet::new();
        let mut estimated_cost = 0.0;
        for artifact in &artifacts {
            if runs.insert(artifact.work_run.id().clone()) {
                let run = WorkRunRepository::new(self.storage.clone())
                    .get(space_id, artifact.work_run.id())
                    .await?
                    .ok_or_else(|| PipelineError::NotFound(artifact.work_run.id().to_string()))?;
                if !run.metrics.estimated_cost_euros.is_finite()
                    || run.metrics.estimated_cost_euros < 0.0
                {
                    return Err(PipelineError::InvalidState(
                        "resolution run cost is invalid".into(),
                    ));
                }
                planning.observe(&run)?;
                if bundles {
                    for member in artifacts
                        .iter()
                        .filter(|member| member.work_run == artifact.work_run)
                    {
                        if !crate::maintenance_commit::contains_authorization(
                            &member.base,
                            &run.base,
                        )? || run
                            .base
                            .provenance
                            .sources
                            .iter()
                            .any(|id| !member.base.provenance.sources.contains(id))
                        {
                            return Err(PipelineError::InvalidState(
                                "resolution artifact weakened its run authority".into(),
                            ));
                        }
                    }
                }
                estimated_cost += run.metrics.estimated_cost_euros;
            }
        }
        if !estimated_cost.is_finite() {
            return Err(PipelineError::InvalidState(
                "resolution batch cost overflowed".into(),
            ));
        }
        let budget =
            BudgetEnforcer::new(self.storage.clone(), Budget::default(), space_id.to_owned());
        match budget
            .check_budget_at_commit(IngestMode::Graph, estimated_cost)
            .await?
        {
            BudgetCheckResult::Allowed => {}
            other => {
                return Err(PipelineError::BudgetExceeded(format!(
                    "resolution commit budget refused: {other:?}"
                )))
            }
        }
        if bundles {
            for (artifact, patch) in artifacts.iter().zip(&patches) {
                let assertion = AssertionRepository::new(self.storage.clone())
                    .get(space_id, &patch.assertion_id)
                    .await?
                    .ok_or_else(|| PipelineError::NotFound(patch.assertion_id.to_string()))?;
                planning.observe(&assertion)?;
                if !crate::maintenance_commit::contains_authorization(
                    &artifact.base,
                    &assertion.base,
                )? || assertion
                    .base
                    .provenance
                    .sources
                    .iter()
                    .any(|id| !artifact.base.provenance.sources.contains(id))
                {
                    return Err(PipelineError::InvalidState(
                        "resolution artifact weakened its assertion authority".into(),
                    ));
                }
            }
        }
        prepare_resolution_patches(&planning, space_id, &patches, backoff_config, max_attempts)
            .await?;
        for artifact in source_artifacts {
            let mut after = artifact.clone();
            after.status = ArtifactStatus::Committed;
            after.committed_at = Some(Utc::now());
            after.base.updated_at = Utc::now();
            planning.stage_artifact(artifact, after)?;
        }
        let mut committed = Vec::new();
        for artifact in artifacts {
            let mut after = artifact.clone();
            after.status = ArtifactStatus::Committed;
            after.committed_at = Some(Utc::now());
            after.base.updated_at = Utc::now();
            planning.stage_artifact(artifact, after.clone())?;
            committed.push(after);
        }
        planning.commit(kind, &scope, &request, &committed).await?;
        Ok(committed)
    }
}

#[allow(missing_docs)]
pub async fn stage_resolution_patches(
    workflow: &ResolutionPatchWorkflow,
    work_run_id: &RecordId,
    space_id: &str,
    patches: &[ResolutionPatch],
) -> Result<Vec<WorkArtifact>, PipelineError> {
    let mut artifacts = Vec::with_capacity(patches.len());
    for patch in patches {
        let staged = workflow
            .stage_resolution_patch(work_run_id, patch.clone(), space_id)
            .await?;
        artifacts.push(staged);
    }
    Ok(artifacts)
}

#[allow(missing_docs)]
pub mod validate_resolution_patches {
    use super::{PipelineError, RecordId, ResolutionPatchWorkflow, WorkArtifactRepository};
    use elephant_types::records::work::ArtifactStatus;

    pub const KIND: &str = "validate_resolution_patches";

    pub async fn execute(
        workflow: &ResolutionPatchWorkflow,
        space_id: &str,
        work_run_id: &RecordId,
    ) -> Result<Vec<elephant_types::records::work::WorkArtifact>, PipelineError> {
        let repo = WorkArtifactRepository::new(workflow.storage.clone());
        let artifacts = repo
            .find_by_work_run(work_run_id)
            .await
            .map_err(PipelineError::StorageError)?;
        let mut results = Vec::new();
        for artifact in artifacts {
            if artifact.status == ArtifactStatus::Draft {
                let validated = workflow
                    .validate_resolution_patch(space_id, &artifact.base.id)
                    .await?;
                results.push(validated);
            }
        }
        Ok(results)
    }
}

#[allow(missing_docs)]
pub mod commit_resolution_patches {
    use super::{
        BackoffConfig, PipelineError, RecordId, ResolutionPatchWorkflow, WorkArtifactRepository,
    };
    use elephant_types::records::work::ArtifactStatus;

    pub const KIND: &str = "commit_resolution_patches";

    pub async fn execute(
        workflow: &ResolutionPatchWorkflow,
        space_id: &str,
        work_run_id: &RecordId,
        backoff_config: &BackoffConfig,
        max_attempts: Option<u32>,
    ) -> Result<Vec<elephant_types::records::work::WorkArtifact>, PipelineError> {
        let repo = WorkArtifactRepository::new(workflow.storage.clone());
        let artifacts = repo
            .find_by_work_run(work_run_id)
            .await
            .map_err(PipelineError::StorageError)?;
        let ids = artifacts
            .into_iter()
            .filter(|row| {
                row.artifact_type == super::RESOLUTION_PATCH_ARTIFACT_TYPE
                    && matches!(
                        row.status,
                        ArtifactStatus::Validated | ArtifactStatus::Committed
                    )
            })
            .map(|row| row.base.id)
            .collect::<Vec<_>>();
        workflow
            .commit_resolution_patch_batch(space_id, &ids, backoff_config, max_attempts)
            .await
    }
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    pub auto_resolve_threshold: f32,
    pub abstain_threshold: f32,
    pub max_attempts: u32,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            auto_resolve_threshold: 0.9,
            abstain_threshold: 0.5,
            max_attempts: 5,
        }
    }
}

// ============================================================================
// Canonicalization
// ============================================================================

/// Outcome of predicate canonicalization.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum CanonicalizationOutcome {
    /// Canonical predicate resolved from the registry.
    Canonical(PredicateRegistry),
    /// Canonicalization failed; a predicate proposal should be staged.
    ProposalNeeded(PredicateProposalData),
}

fn build_predicate_proposal_data(
    predicate_raw: &str,
    predicate_id: String,
    space_id: &str,
    confidence: f32,
) -> PredicateProposalData {
    let alias = predicate_raw.trim().to_string();
    PredicateProposalData {
        predicate_id,
        proposed_aliases: vec![ProposedAlias {
            alias,
            source: "canonicalization".to_string(),
            confidence,
        }],
        confidence,
        space_id: space_id.to_string(),
    }
}

/// Canonicalizes a raw predicate to its canonical form using alias mappings.
///
/// Per spec §10.2a: uses `can_use_for_truth()` from the predicate module
/// to gate truth maintenance. Only returns a canonical predicate if the
/// alias meets the confidence threshold and is in Active status.
///
/// Integration: This function uses `can_use_for_truth()` from Phase 5
/// (`crates/pipeline/src/predicate/mod.rs`) to properly check alias eligibility.
pub fn canonicalize_predicate_with_aliases(
    predicate_raw: &str,
    aliases: &[PredicateAlias],
    thresholds: &AliasThresholds,
) -> Option<String> {
    // Find matching alias (case-insensitive)
    let predicate_lower = predicate_raw.to_lowercase();

    for alias in aliases {
        if alias.alias.to_lowercase() == predicate_lower {
            // Use can_use_for_truth() from predicate module to check eligibility
            // This ensures consistency with Phase 5 predicate governance
            if can_use_for_truth(alias, thresholds) {
                return Some(alias.predicate_id.clone());
            }
        }
    }

    None
}

/// Canonicalizes a raw predicate to its canonical form using DB-backed aliases.
///
/// Per spec §10.3: looks up active aliases in predicate_alias and ensures the
/// predicate exists in predicate_registry before returning a canonical ID.
pub async fn canonicalize_predicate_from_db(
    predicate_raw: &str,
    space_id: &str,
    thresholds: &AliasThresholds,
    alias_repo: &PredicateAliasRepository,
    registry_repo: &PredicateRegistryRepository,
) -> Result<CanonicalizationOutcome, PipelineError> {
    let alias = alias_repo
        .find_active_by_raw(space_id, predicate_raw)
        .await?;

    let predicate_raw_trimmed = predicate_raw.trim();

    let Some(alias) = alias else {
        let predicate = registry_repo
            .find_by_predicate_id(space_id, predicate_raw_trimmed)
            .await?;
        let predicate_id = predicate
            .map(|registry| registry.predicate_id)
            .unwrap_or_else(|| predicate_raw_trimmed.to_string());

        return Ok(CanonicalizationOutcome::ProposalNeeded(
            build_predicate_proposal_data(predicate_raw_trimmed, predicate_id, space_id, 0.0),
        ));
    };

    let predicate = registry_repo
        .find_by_predicate_id(space_id, &alias.predicate_id)
        .await?;
    let Some(predicate) = predicate else {
        return Ok(CanonicalizationOutcome::ProposalNeeded(
            build_predicate_proposal_data(
                predicate_raw_trimmed,
                alias.predicate_id.clone(),
                space_id,
                alias.confidence,
            ),
        ));
    };

    let effective_thresholds = predicate
        .semantics
        .as_ref()
        .map(|semantics| &semantics.alias_thresholds)
        .unwrap_or(thresholds);

    if !can_use_for_truth(&alias, effective_thresholds) {
        return Ok(CanonicalizationOutcome::ProposalNeeded(
            build_predicate_proposal_data(
                predicate_raw_trimmed,
                alias.predicate_id.clone(),
                space_id,
                alias.confidence,
            ),
        ));
    }

    Ok(CanonicalizationOutcome::Canonical(predicate))
}

#[cfg(all(test, feature = "meerkat-extraction"))]
mod agentic_resolution_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::Utc;
    use elephant_storage::StorageClient;
    use elephant_types::records::assertion::{Assertion, AssertionRetryState, EvidenceSpan};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::rel::{ClaimStatus, Rel};
    use elephant_types::{Link, TableName};

    const TEST_SPACE: &str = "resolution-test-space";

    fn make_entity(display_name: &str) -> Entity {
        Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                TEST_SPACE.to_string(),
                "resolution-test".to_string(),
            ),
            EntityType::new("person"),
            display_name.to_string(),
        )
    }

    fn make_assertion(subject_entity: &RecordId, object_entity: &RecordId) -> Assertion {
        let now = Utc::now();
        Assertion {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Assertion),
                TEST_SPACE.to_string(),
                "resolution-test".to_string(),
            ),
            subject_text: "Alice".to_string(),
            predicate_raw: "works_at".to_string(),
            predicate_id: None,
            object_text: Some("Acme Corp".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 12,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: now,
            asserted_at: now,
            assertion_key: "assertion-key".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: Some("test".to_string()),
            resolution_confidence: Some(0.92),
            retry_state: AssertionRetryState::default(),
            subject_entity: Some(Link::new(subject_entity.clone())),
            object_entity: Some(Link::new(object_entity.clone())),
            confidence: 0.92,
            subject_entity_type: Some("person".to_string()),
            object_entity_type: Some("organization".to_string()),
            significance: Some("medium".to_string()),
            lifecycle_tier: "active".to_string(),
        }
    }

    fn make_rel(subject: &RecordId, object: &RecordId) -> Rel {
        Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                TEST_SPACE.to_string(),
                "resolution-test".to_string(),
            ),
            subject: Link::new(subject.clone()),
            object: Link::new(object.clone()),
            rel_type: "works_at".to_string(),
            status: ClaimStatus::Approved,
            confidence: 0.95,
            validity_window: None,
            evidence: Vec::new(),
            temporal_audit: None,
        }
    }

    #[tokio::test]
    async fn test_disambiguator_search_assertions_uses_repository_context() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let entity_repo = EntityRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());

        let subject = entity_repo
            .create(make_entity("Alice"))
            .await
            .expect("subject");
        let object = entity_repo
            .create(make_entity("Acme Corp"))
            .await
            .expect("object");
        assertion_repo
            .create(make_assertion(&subject.base.id, &object.base.id))
            .await
            .expect("assertion");

        let dispatcher = DisambiguatorToolDispatcher::new(storage, TEST_SPACE, Vec::new(), 5, 5);

        let response = dispatcher
            .dispatch(
                "search_assertions",
                &serde_json::json!({"query": "Alice", "limit": 5}),
            )
            .await;

        assert_eq!(response["success"], serde_json::Value::Bool(true));
        assert_eq!(response["data"]["total"], serde_json::Value::from(1));
        let items = response["data"]["items"]
            .as_array()
            .expect("items should be an array");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["subject_text"], serde_json::Value::from("Alice"));
    }

    #[tokio::test]
    async fn test_disambiguator_get_entity_with_relations_uses_repository_context() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let entity_repo = EntityRepository::new(storage.clone());
        let rel_repo = RelRepository::new(storage.clone());

        let subject = entity_repo
            .create(make_entity("Alice"))
            .await
            .expect("subject");
        let object = entity_repo
            .create(make_entity("Acme Corp"))
            .await
            .expect("object");
        rel_repo
            .create(make_rel(&subject.base.id, &object.base.id))
            .await
            .expect("rel");

        let dispatcher = DisambiguatorToolDispatcher::new(storage, TEST_SPACE, Vec::new(), 5, 5);

        let response = dispatcher
            .dispatch(
                "get_entity_with_relations",
                &serde_json::json!({"id": subject.base.id.to_string()}),
            )
            .await;

        assert_eq!(response["success"], serde_json::Value::Bool(true));
        assert_eq!(
            response["data"]["entity"]["display_name"],
            serde_json::Value::from("Alice")
        );
        assert_eq!(
            response["data"]["total_outgoing_relations"],
            serde_json::Value::from(1)
        );
        let outgoing = response["data"]["outgoing_relations"]
            .as_array()
            .expect("outgoing relations should be an array");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0]["rel_type"], serde_json::Value::from("works_at"));
    }
    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    async fn disambiguation_requires_successful_terminal_outcome() {
        use meerkat::{LlmDoneOutcome, LlmError, LlmEvent, StopReason};
        let text = || {
            Ok(LlmEvent::TextDelta {
                delta: "entity:chosen".into(),
                meta: None,
            })
        };
        for events in [
            vec![
                text(),
                Ok(LlmEvent::Done {
                    outcome: LlmDoneOutcome::Error {
                        error: LlmError::ServerOverloaded,
                    },
                }),
            ],
            vec![text(), Err(LlmError::ServerOverloaded)],
            vec![text()],
        ] {
            assert!(matches!(
                collect_disambiguation_turn(futures::stream::iter(events)).await,
                Err(PipelineError::ExtractionError(_))
            ));
        }
        let (content, calls) = collect_disambiguation_turn(futures::stream::iter([
            text(),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success {
                    stop_reason: StopReason::EndTurn,
                },
            }),
        ]))
        .await
        .unwrap();
        assert_eq!(content, "entity:chosen");
        assert!(calls.is_empty());
    }
}
