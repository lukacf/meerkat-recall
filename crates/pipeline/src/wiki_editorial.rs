//! LLM editorial compilation of evidence-backed wiki articles.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
#[cfg(feature = "meerkat-extraction")]
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elephant_knowledge_views::{security_fingerprint, GraphViewCompiler};
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocNodeRepository, DocRevisionRepository,
    EntityRepository, EventRepository, KnowledgeViewDefinitionRepository, KnowledgeViewRepository,
    PredicateRegistryRepository, RelRepository, Repository, StorageClient, TruthSlotRepository,
    WorkItemRepository,
};
use elephant_types::records::base::{
    BaseFields, Extractor, Provenance, ProvenanceStep, Security, SubjectsState,
};
use elephant_types::records::predicate::SemanticsStatus;
use elephant_types::records::{
    CitationKind, ClaimStatus, CompilerReceipt, Entity, EventStatus, EvidenceSpan, HasSecurity,
    KnowledgeCitation, KnowledgeDependency, KnowledgeStatement, KnowledgeView, KnowledgeViewBlock,
    KnowledgeViewDefinition, KnowledgeViewKind, KnowledgeViewNavigation,
    KnowledgeViewPublicationStatus, KnowledgeViewRevision, KnowledgeViewSelector,
    KnowledgeViewSurface, Literal, ProjectionState, ResolutionStatus, StatementDerivation,
    StatementVerification, ValidationReceipt, ViewBlockKind, WorkItem, WorkItemStatus,
};
use elephant_types::{Link, RecordId, TableName};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(any(feature = "meerkat-extraction", test))]
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{PipelineError, PipelineResult};

/// Stable identity of the LLM article compiler.
pub const ARTICLE_COMPILER_NAME: &str = "elephant.llm_article";
/// Contract version of the LLM article compiler.
pub const ARTICLE_COMPILER_VERSION: u32 = 15;
const WRITER_PROMPT_VERSION: &str = "wiki-writer-v12";
const VERIFIER_PROMPT_VERSION: &str = "wiki-verifier-v4";
const PLANNER_PROMPT_VERSION: &str = "wiki-planner-v12";
const ORGANIZER_PROMPT_VERSION: &str = "wiki-organizer-v6";
const NO_PUBLISHABLE_RELATIONS: &str = "__elephant_wiki_no_publishable_relations__";
const FULL_WIKI_STRUCTURE_MAX_OUTPUT_TOKENS: u32 = 64_000;
const FULL_WIKI_STRUCTURE_MAX_CONTEXT_CHARS: usize = 1_000_000;
const FULL_WIKI_ORGANIZER_MIN_TIMEOUT_SECS: u64 = 600;
const WIKI_PLANNING_MAX_ATTEMPTS: usize = 8;
const MAX_WIKI_PLANNING_PARTITION_ENTITIES: usize = 30;
const DEFAULT_WIKI_ARTICLE_CONCURRENCY: usize = 4;
const MAX_WIKI_ARTICLE_CONCURRENCY: usize = 32;

const fn full_wiki_structure_output_token_budget(configured: u32) -> u32 {
    if configured > FULL_WIKI_STRUCTURE_MAX_OUTPUT_TOKENS {
        configured
    } else {
        FULL_WIKI_STRUCTURE_MAX_OUTPUT_TOKENS
    }
}

const fn full_wiki_structure_context_char_budget(configured: usize) -> usize {
    if configured > FULL_WIKI_STRUCTURE_MAX_CONTEXT_CHARS {
        configured
    } else {
        FULL_WIKI_STRUCTURE_MAX_CONTEXT_CHARS
    }
}

const fn full_wiki_organizer_timeout_secs(configured: u64) -> u64 {
    if configured > FULL_WIKI_ORGANIZER_MIN_TIMEOUT_SECS {
        configured
    } else {
        FULL_WIKI_ORGANIZER_MIN_TIMEOUT_SECS
    }
}

async fn wait_before_wiki_llm_retry(attempt: usize) {
    if cfg!(test) {
        return;
    }
    let exponent = u32::try_from(attempt.saturating_sub(1).min(3)).unwrap_or(3);
    tokio::time::sleep(std::time::Duration::from_secs(1_u64 << exponent)).await;
}
/// Durable work kind for recurring full-graph wiki reconciliation.
pub const RECONCILE_LIVING_WIKI_KIND: &str = "reconcile_living_wiki";

/// Runtime bounds and model choices for editorial compilation.
#[derive(Debug, Clone)]
pub struct WikiEditorialConfig {
    /// Model that writes structured article prose.
    pub writer_model: String,
    /// Model that independently checks every statement against its citations.
    pub verifier_model: String,
    /// Maximum output tokens for either model call.
    pub max_output_tokens: u32,
    /// In-session structured-output repairs before returning a malformed response.
    pub structured_output_retries: u32,
    /// Wall-clock timeout for each model call.
    pub timeout_secs: u64,
    /// Maximum serialized evidence-catalog characters supplied to the models.
    pub max_context_chars: usize,
    /// Maximum writer-verifier cycles before compilation fails closed.
    pub max_editorial_attempts: usize,
}

impl Default for WikiEditorialConfig {
    fn default() -> Self {
        let writer_model = std::env::var("ELEPHANT_WIKI_WRITER_MODEL")
            .or_else(|_| std::env::var("PIPELINE_AGENTIC_MODEL"))
            .unwrap_or_else(|_| "gpt-5.6-sol".to_string());
        let verifier_model =
            std::env::var("ELEPHANT_WIKI_VERIFIER_MODEL").unwrap_or_else(|_| writer_model.clone());
        let max_context_chars = std::env::var("ELEPHANT_WIKI_MAX_CONTEXT_CHARS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(400_000)
            .clamp(50_000, FULL_WIKI_STRUCTURE_MAX_CONTEXT_CHARS);
        let max_editorial_attempts = std::env::var("ELEPHANT_WIKI_MAX_EDITORIAL_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(6)
            .clamp(1, 12);
        let max_output_tokens = std::env::var("ELEPHANT_WIKI_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(32_000)
            .clamp(4_000, FULL_WIKI_STRUCTURE_MAX_OUTPUT_TOKENS);
        let structured_output_retries = std::env::var("ELEPHANT_WIKI_STRUCTURED_OUTPUT_RETRIES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(5)
            .clamp(1, 8);
        Self {
            writer_model,
            verifier_model,
            max_output_tokens,
            structured_output_retries,
            timeout_secs: 180,
            max_context_chars,
            max_editorial_attempts,
        }
    }
}

/// One bounded item supplied to the writer and verifier.
#[derive(Debug, Clone, Serialize)]
pub struct EditorialEvidenceItem {
    /// Citation ID that article statements may reference.
    pub citation_id: String,
    /// Canonical support kind.
    pub kind: CitationKind,
    /// Canonical record directly addressed by the citation.
    pub record_id: String,
    /// Deterministic graph statements already supported by this citation.
    pub canonical_statements: Vec<String>,
    /// Exact immutable source quote, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_quote: Option<String>,
    /// Original source ID, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
}

/// One claim-bearing sentence or short paragraph returned by the writer.
#[derive(Debug, Clone, Serialize)]
pub struct EditorialStatement {
    /// Stable article-local statement ID.
    #[serde(default)]
    pub id: String,
    /// Human-readable prose. Citation markers are added by Elephant.
    pub text: String,
    /// Citation IDs from the supplied evidence catalog.
    pub citation_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EditorialStatementWire {
    Structured {
        #[serde(default)]
        id: String,
        text: String,
        citation_ids: Vec<String>,
    },
    CitedText(String),
}

impl<'de> Deserialize<'de> for EditorialStatement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match EditorialStatementWire::deserialize(deserializer)? {
            EditorialStatementWire::Structured {
                id,
                text,
                citation_ids,
            } => Ok(Self {
                id,
                text,
                citation_ids,
            }),
            EditorialStatementWire::CitedText(value) => {
                let mut words = value.split_whitespace().collect::<Vec<_>>();
                let mut citation_ids = VecDeque::new();
                while let Some(citation_id) = words
                    .last()
                    .and_then(|word| parse_inline_editorial_citation(word))
                {
                    words.pop();
                    citation_ids.push_front(citation_id);
                }
                Ok(Self {
                    id: String::new(),
                    text: words.join(" "),
                    citation_ids: citation_ids.into(),
                })
            }
        }
    }
}

fn parse_inline_editorial_citation(value: &str) -> Option<String> {
    let inner = value.strip_prefix('[')?.strip_suffix(']')?;
    let citation_id = inner.strip_prefix('^').unwrap_or(inner);
    let digits = citation_id.strip_prefix('c')?;
    (!digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit()))
        .then(|| citation_id.to_string())
}

/// One semantic article section returned by the writer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EditorialSection {
    /// Stable section ID.
    #[serde(default)]
    pub id: String,
    /// Human-readable heading selected for this subject.
    #[serde(default = "default_editorial_section_heading")]
    pub heading: String,
    /// Semantic section class.
    #[serde(default = "default_editorial_section_kind")]
    pub kind: String,
    /// Claim-bearing prose in display order.
    pub statements: Vec<EditorialStatement>,
}

fn default_editorial_section_heading() -> String {
    "Overview".to_string()
}

fn default_editorial_section_kind() -> String {
    "overview".to_string()
}

/// Structured article returned by the writer model.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EditorialDraft {
    /// Required output language. The production schema only permits English.
    pub language: String,
    /// Human-readable article title.
    pub title: String,
    /// Wikipedia-style lead explaining identity and local relevance.
    pub lead: EditorialStatement,
    /// Subject-appropriate semantic sections.
    pub sections: Vec<EditorialSection>,
}

#[derive(Deserialize)]
struct EditorialBodyDraftWire {
    language: String,
    title: String,
    lead: String,
    sections: Vec<EditorialBodySectionWire>,
    #[serde(default)]
    statements: Vec<String>,
}

#[derive(Deserialize)]
struct EditorialBodySectionWire {
    heading: String,
    body: String,
}

#[derive(Deserialize)]
struct EditorialArticleTextWire {
    language: String,
    title: String,
    article: String,
}

/// One verifier judgment.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatementVerificationDecision {
    /// Article-local statement ID.
    pub statement_id: String,
    /// Whether the cited catalog items entail the complete statement.
    pub entailed: bool,
    /// Whether the statement is useful knowledge about the article subject.
    pub relevant: bool,
    /// Concise audit reason.
    pub reason: String,
}

/// Structured output returned by the verifier model.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EditorialVerification {
    /// One exact decision for every writer statement.
    pub decisions: Vec<StatementVerificationDecision>,
    /// Whether the result reads as a useful article rather than a fact dump.
    pub article_useful: bool,
    /// Whether the lead and sections form a coherent account of one subject.
    pub article_coherent: bool,
    /// Whether old and current evidence are framed without temporal distortion.
    pub temporal_framing_sound: bool,
    /// Important supported dimensions omitted from the draft, if any.
    #[serde(default)]
    pub missing_major_topics: Vec<String>,
    /// Concise whole-article audit reason.
    pub article_reason: String,
}

#[derive(Deserialize)]
struct EditorialVerificationWire {
    statement_decisions: Vec<StatementVerificationWireDecision>,
    #[serde(alias = "whole_article_judgment")]
    article_judgment: ArticleVerificationWireJudgment,
}

#[derive(Deserialize)]
struct StatementVerificationWireDecision {
    id: String,
    entailed: bool,
    relevant: bool,
    #[serde(default)]
    reason: String,
}

#[derive(Deserialize)]
struct ArticleVerificationWireJudgment {
    article_useful: bool,
    article_coherent: bool,
    temporal_framing_sound: bool,
    missing_major_topics: Vec<String>,
}

/// Bounded semantic entity inventory supplied to the wiki planner.
#[derive(Debug, Clone, Serialize)]
pub struct WikiPlanningEntity {
    /// Canonical entity ID that the planner must return verbatim.
    pub entity_id: String,
    /// Canonical semantic entity type.
    pub entity_type: String,
    /// Human-readable canonical name.
    pub display_name: String,
    /// Existing canonical summary, when one is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Governed, approved graph degree used only as an editorial importance signal.
    pub graph_degree: usize,
    /// Resolved source-backed assertions available for editorial evidence.
    pub evidence_assertions: usize,
    /// Governed, approved entity-to-literal facts.
    pub attribute_facts: usize,
    /// Approved first-class events that name this entity as a participant.
    pub events: usize,
    /// Stale or conflicting truth slots that require reader-visible caution.
    pub knowledge_issues: usize,
    /// Earliest semantic source or event time known for this subject.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earliest_evidence_at: Option<DateTime<Utc>>,
    /// Latest semantic source or event time known for this subject. This is a
    /// recency signal, not a claim that every old subject is unimportant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_evidence_at: Option<DateTime<Utc>>,
    /// Person records that a conservative name matcher considers possible
    /// identity duplicates. These are review hints, not identity evidence. The
    /// LLM may consolidate a well-supported match, but uncertainty must retain
    /// separate articles rather than silently deleting or merging records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub possible_duplicate_entity_ids: Vec<String>,
    /// Names of directly connected candidate entities and their predicates.
    pub connections: Vec<WikiPlanningConnection>,
    /// Entity co-mentions supported by resolved source assertions.
    pub evidence_connections: Vec<WikiPlanningEvidenceConnection>,
}

/// One graph-native connection exposed to the planner.
#[derive(Debug, Clone, Serialize)]
pub struct WikiPlanningConnection {
    /// Approved relationship predicate.
    pub predicate: String,
    /// Canonical neighboring entity ID.
    pub entity_id: String,
    /// Canonical neighboring entity name.
    pub display_name: String,
}

/// Source-backed entity co-mention available for navigation, not graph truth.
#[derive(Debug, Clone, Serialize)]
pub struct WikiPlanningEvidenceConnection {
    /// Canonical neighboring entity ID.
    pub entity_id: String,
    /// Canonical neighboring entity name.
    pub display_name: String,
    /// Number of resolved assertions connecting the two entities in evidence.
    pub assertion_count: usize,
}

/// One article selected by the LLM wiki planner.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlannedWikiArticle {
    /// Canonical entity that anchors the article.
    pub entity_id: String,
    /// Concise English article title.
    pub title: String,
    /// Why this entity merits a reader-facing article. This is an audit note,
    /// not reader prose.
    pub editorial_reason: String,
    /// Other inventory entities synthesized into this page rather than
    /// published as duplicate or context-free standalone pages.
    #[serde(default)]
    pub supporting_entity_ids: Vec<String>,
    /// Whole-wiki editorial importance from 1 (deep reference) to 5 (core).
    pub importance: u8,
    /// Semantic role such as core, overview, topic, reference, or archive.
    pub page_role: String,
    /// Reader-facing temporal framing: current, enduring, mixed, or historical.
    pub temporal_relevance: String,
    /// Other selected article entities that should be linked from this page.
    #[serde(default)]
    pub related_entity_ids: Vec<String>,
}

/// Structured English wiki plan returned by the planner model.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WikiPlan {
    /// Required output language. The production schema only permits English.
    pub language: String,
    /// Curated article set in intended index order.
    pub articles: Vec<PlannedWikiArticle>,
}

/// One reader-facing section selected by the LLM organizer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlannedWikiSection {
    /// Stable organizer-local identifier.
    pub id: String,
    /// Concise English section title.
    pub title: String,
    /// Short explanation of the reader lens, displayed on the wiki index.
    pub description: String,
    /// Selected article entities assigned to this section in reader order.
    pub article_entity_ids: Vec<String>,
}

/// Global reader navigation generated after graph-sharded article selection.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WikiOrganization {
    /// Required output language.
    pub language: String,
    /// Reader-facing wiki title.
    pub title: String,
    /// Concise statement of what this knowledge space is for, inferred from
    /// the complete planned corpus rather than configured by a demo fixture.
    pub purpose: String,
    /// Small global set of defining pages shown on the compact front page.
    pub featured_entity_ids: Vec<String>,
    /// Ordered, exhaustive article sections.
    pub sections: Vec<PlannedWikiSection>,
}

/// Injection seam for the writer and verifier model calls.
#[async_trait]
pub trait WikiEditorialLlm: Send + Sync {
    /// Selects a curated, interlinked article set from a bounded graph inventory.
    async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan>;
    /// Organizes every selected article into a small reader-facing section map.
    async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization>;
    /// Produces a structured article from the bounded evidence prompt.
    async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft>;
    /// Checks every proposed statement against its cited evidence.
    async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification>;
    /// Durable writer model identifier.
    fn writer_model(&self) -> &str;
    /// Durable verifier model identifier.
    fn verifier_model(&self) -> &str;
}

/// Meerkat-backed structured-output implementation.
#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Clone)]
pub struct MeerkatWikiEditorialLlm {
    config: WikiEditorialConfig,
}

#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Clone, Copy)]
struct WikiModelCallBudget {
    max_output_tokens: u32,
    timeout_secs: u64,
}

#[cfg(feature = "meerkat-extraction")]
impl MeerkatWikiEditorialLlm {
    /// Creates a model adapter using Elephant's existing Meerkat runtime.
    #[must_use]
    pub const fn new(config: WikiEditorialConfig) -> Self {
        Self { config }
    }

    async fn run_structured(
        &self,
        model: &str,
        system_prompt: &str,
        prompt: String,
        schema_name: &str,
        schema: Value,
        budget: WikiModelCallBudget,
    ) -> PipelineResult<Value> {
        use std::path::PathBuf;

        use meerkat::{
            AgentBuildConfig, AgentFactory, BudgetLimits, OutputSchema, ToolCategoryOverride,
        };
        use meerkat_core::types::ContentInput;
        use meerkat_core::Config;

        let output_schema = OutputSchema::new(schema)
            .map_err(|error| {
                PipelineError::ConfigError(format!("Invalid wiki output schema: {error}"))
            })?
            .with_name(schema_name)
            .strict();
        let limits = BudgetLimits::default()
            .with_max_tokens(u64::from(budget.max_output_tokens).saturating_mul(4))
            .with_max_duration(Duration::from_secs(budget.timeout_secs));
        let mut build = AgentBuildConfig::new(model.to_string());
        build.system_prompt = meerkat_core::SystemPromptOverride::Set(system_prompt.to_string());
        build.max_tokens = Some(budget.max_output_tokens);
        build.budget_limits = Some(limits);
        build.output_schema = Some(output_schema);
        build.structured_output_retries = Some(self.config.structured_output_retries);
        build.override_shell = ToolCategoryOverride::Disable;
        build.override_mob = ToolCategoryOverride::Disable;

        let factory = AgentFactory::new(PathBuf::from("/tmp/elephant-wiki-editorial"));
        let call = async {
            let mut agent = factory
                .build_agent(build, &Config::default())
                .await
                .map_err(|error| {
                    PipelineError::ExtractionError(format!(
                        "Wiki editorial agent build failed: {error}"
                    ))
                })?;
            let output = agent
                .run(ContentInput::Text(prompt))
                .await
                .map_err(|error| {
                    PipelineError::ExtractionError(format!(
                        "Wiki editorial model call failed: {error}"
                    ))
                })?;
            recover_editorial_output(output.structured_output, &output.text)
        };
        tokio::time::timeout(Duration::from_secs(budget.timeout_secs), call)
            .await
            .map_err(|_| {
                PipelineError::BudgetExceeded(format!(
                    "Wiki editorial model exceeded {} seconds",
                    budget.timeout_secs
                ))
            })?
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn recover_editorial_output(structured: Option<Value>, text: &str) -> PipelineResult<Value> {
    if let Some(structured) = structured {
        return Ok(structured);
    }
    let trimmed = text.trim();
    let json_text = exact_json_code_fence(trimmed).unwrap_or(trimmed);
    serde_json::from_str(json_text).map_err(|error| {
        PipelineError::ValidationError(format!(
            "Wiki editorial model returned neither structured output nor a complete JSON response ({} characters): {error}",
            text.chars().count()
        ))
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn exact_json_code_fence(text: &str) -> Option<&str> {
    let fenced = text.strip_prefix("```json")?;
    let fenced = fenced
        .strip_prefix("\r\n")
        .or_else(|| fenced.strip_prefix('\n'))?;
    let fenced = fenced.strip_suffix("```")?;
    Some(fenced.trim())
}

#[cfg(feature = "meerkat-extraction")]
#[async_trait]
impl WikiEditorialLlm for MeerkatWikiEditorialLlm {
    async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan> {
        let value = self
            .run_structured(
                &self.config.writer_model,
                planner_system_prompt(),
                prompt,
                "elephant_wiki_plan",
                wiki_plan_schema(),
                WikiModelCallBudget {
                    max_output_tokens: full_wiki_structure_output_token_budget(
                        self.config.max_output_tokens,
                    ),
                    timeout_secs: self.config.timeout_secs,
                },
            )
            .await?;
        serde_json::from_value(value).map_err(|error| {
            PipelineError::ValidationError(format!(
                "Planner output did not match the wiki-plan schema: {error}"
            ))
        })
    }

    async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization> {
        let value = self
            .run_structured(
                &self.config.writer_model,
                organizer_system_prompt(),
                prompt,
                "elephant_wiki_organization",
                wiki_organization_schema(),
                WikiModelCallBudget {
                    max_output_tokens: full_wiki_structure_output_token_budget(
                        self.config.max_output_tokens,
                    ),
                    timeout_secs: full_wiki_organizer_timeout_secs(self.config.timeout_secs),
                },
            )
            .await?;
        serde_json::from_value(value).map_err(|error| {
            PipelineError::ValidationError(format!(
                "Organizer output did not match the wiki-organization schema: {error}"
            ))
        })
    }

    async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft> {
        let value = self
            .run_structured(
                &self.config.writer_model,
                writer_system_prompt(),
                prompt,
                "elephant_wiki_article",
                article_schema(),
                WikiModelCallBudget {
                    max_output_tokens: self.config.max_output_tokens,
                    timeout_secs: self.config.timeout_secs,
                },
            )
            .await?;
        let mut draft = decode_editorial_draft(value)?;
        assign_missing_statement_ids(&mut draft);
        Ok(draft)
    }

    async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification> {
        let value = self
            .run_structured(
                &self.config.verifier_model,
                verifier_system_prompt(),
                prompt,
                "elephant_wiki_verification",
                verification_schema(),
                WikiModelCallBudget {
                    max_output_tokens: self.config.max_output_tokens,
                    timeout_secs: self.config.timeout_secs,
                },
            )
            .await?;
        decode_editorial_verification(value)
    }

    fn writer_model(&self) -> &str {
        &self.config.writer_model
    }

    fn verifier_model(&self) -> &str {
        &self.config.verifier_model
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn decode_editorial_verification(value: Value) -> PipelineResult<EditorialVerification> {
    let wire_shape = describe_editorial_wire_shape(&value, 0);
    match serde_json::from_value::<EditorialVerification>(value.clone()) {
        Ok(verification) => Ok(verification),
        Err(structured_error) => {
            let wire = serde_json::from_value::<EditorialVerificationWire>(value).map_err(
                |alternate_error| {
                    PipelineError::ValidationError(format!(
                        "Verifier output did not match the verification schema: {structured_error}; alternate verifier schema: {alternate_error}; wire shape: {wire_shape}"
                    ))
                },
            )?;
            let ArticleVerificationWireJudgment {
                article_useful,
                article_coherent,
                temporal_framing_sound,
                missing_major_topics,
            } = wire.article_judgment;
            let article_reason = if article_useful
                && article_coherent
                && temporal_framing_sound
                && missing_major_topics.is_empty()
            {
                "Verifier returned a positive whole-article judgment.".to_string()
            } else {
                "Verifier returned a negative whole-article judgment.".to_string()
            };
            Ok(EditorialVerification {
                decisions: wire
                    .statement_decisions
                    .into_iter()
                    .map(|decision| StatementVerificationDecision {
                        statement_id: decision.id,
                        entailed: decision.entailed,
                        relevant: decision.relevant,
                        reason: if decision.reason.trim().is_empty() {
                            format!(
                                "Verifier returned entailed={} and relevant={} for this statement.",
                                decision.entailed, decision.relevant
                            )
                        } else {
                            decision.reason
                        },
                    })
                    .collect(),
                article_useful,
                article_coherent,
                temporal_framing_sound,
                missing_major_topics,
                article_reason,
            })
        }
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn decode_editorial_draft(value: Value) -> PipelineResult<EditorialDraft> {
    let wire_shape = describe_editorial_wire_shape(&value, 0);
    match serde_json::from_value::<EditorialDraft>(value.clone()) {
        Ok(draft) => Ok(draft),
        Err(structured_error) => {
            match serde_json::from_value::<EditorialBodyDraftWire>(value.clone()) {
                Ok(body_draft) => decode_editorial_body_draft(body_draft),
                Err(body_error) => {
                    let article_text = serde_json::from_value::<EditorialArticleTextWire>(value)
                        .map_err(|article_error| {
                            PipelineError::ValidationError(format!(
                                "Writer output did not match the article schema: {structured_error}; alternate body schema: {body_error}; alternate article-text schema: {article_error}; wire shape: {wire_shape}"
                            ))
                        })?;
                    decode_editorial_article_text(article_text)
                }
            }
        }
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn decode_editorial_article_text(wire: EditorialArticleTextWire) -> PipelineResult<EditorialDraft> {
    let article = wire.article.replace("\r\n", "\n");
    let mut lead_lines = Vec::new();
    let mut section_heading: Option<String> = None;
    let mut section_lines = Vec::new();
    let mut raw_sections = Vec::new();

    for line in article.lines() {
        if let Some(heading) = line.strip_prefix("## ") {
            if let Some(previous_heading) = section_heading.replace(heading.trim().to_string()) {
                raw_sections.push((previous_heading, std::mem::take(&mut section_lines)));
            }
        } else if line.starts_with("# ") && section_heading.is_none() && lead_lines.is_empty() {
            continue;
        } else if section_heading.is_some() {
            section_lines.push(line.to_string());
        } else {
            lead_lines.push(line.to_string());
        }
    }
    if let Some(previous_heading) = section_heading {
        raw_sections.push((previous_heading, section_lines));
    }

    let mut lead_paragraphs = split_editorial_paragraphs(&lead_lines.join("\n"));
    if raw_sections.is_empty() && lead_paragraphs.len() > 1 {
        let section_paragraphs = lead_paragraphs.split_off(1);
        raw_sections.push(("Overview".to_string(), section_paragraphs));
    }
    let lead_text = lead_paragraphs.join(" ");
    if lead_text.trim().is_empty() {
        return Err(PipelineError::ValidationError(
            "Alternate article-text output did not contain lead prose".to_string(),
        ));
    }

    let sections = raw_sections
        .into_iter()
        .map(|(heading, lines)| {
            let statements = split_editorial_paragraphs(&lines.join("\n"))
                .into_iter()
                .map(|paragraph| decode_cited_editorial_text(&paragraph))
                .collect::<Vec<_>>();
            EditorialSection {
                id: String::new(),
                heading,
                kind: default_editorial_section_kind(),
                statements,
            }
        })
        .collect::<Vec<_>>();

    Ok(EditorialDraft {
        language: wire.language,
        title: wire.title,
        lead: decode_cited_editorial_text(&lead_text),
        sections,
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn split_editorial_paragraphs(value: &str) -> Vec<String> {
    value
        .split("\n\n")
        .map(str::trim)
        .filter(|paragraph| !paragraph.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn decode_editorial_body_draft(wire: EditorialBodyDraftWire) -> PipelineResult<EditorialDraft> {
    let lead = decode_cited_editorial_text(&wire.lead);
    let ledger = wire
        .statements
        .iter()
        .map(|statement| decode_cited_editorial_text(statement))
        .collect::<Vec<_>>();
    let mut matched_ledger = vec![false; ledger.len()];
    let normalized_lead = normalize_editorial_wire_text(&lead.text);
    for (index, statement) in ledger.iter().enumerate() {
        let normalized_statement = normalize_editorial_wire_text(&statement.text);
        matched_ledger[index] = !normalized_statement.is_empty()
            && normalized_lead.contains(normalized_statement.as_str());
    }

    let mut sections = Vec::with_capacity(wire.sections.len().saturating_add(1));
    for section in wire.sections {
        let mut statement = decode_cited_editorial_text(&section.body);
        let normalized_body = normalize_editorial_wire_text(&statement.text);
        for (index, ledger_statement) in ledger.iter().enumerate() {
            let normalized_statement = normalize_editorial_wire_text(&ledger_statement.text);
            if !normalized_statement.is_empty()
                && normalized_body.contains(normalized_statement.as_str())
            {
                matched_ledger[index] = true;
                statement
                    .citation_ids
                    .extend(ledger_statement.citation_ids.iter().cloned());
            }
        }
        statement.citation_ids.sort();
        statement.citation_ids.dedup();
        sections.push(EditorialSection {
            id: String::new(),
            heading: section.heading,
            kind: default_editorial_section_kind(),
            statements: vec![statement],
        });
    }

    let unmatched = ledger
        .into_iter()
        .zip(matched_ledger)
        .filter_map(|(statement, matched)| (!matched).then_some(statement))
        .collect::<Vec<_>>();
    if !unmatched.is_empty() {
        sections.push(EditorialSection {
            id: String::new(),
            heading: "Additional context".to_string(),
            kind: default_editorial_section_kind(),
            statements: unmatched,
        });
    }

    Ok(EditorialDraft {
        language: wire.language,
        title: wire.title,
        lead,
        sections,
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn decode_cited_editorial_text(value: &str) -> EditorialStatement {
    let mut text = Vec::new();
    let mut citation_ids = Vec::new();
    for word in value.split_whitespace() {
        if let Some(citation_id) = parse_inline_editorial_citation(word) {
            citation_ids.push(citation_id);
        } else {
            text.push(word);
        }
    }
    citation_ids.sort();
    citation_ids.dedup();
    EditorialStatement {
        id: String::new(),
        text: text.join(" "),
        citation_ids,
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn normalize_editorial_wire_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn describe_editorial_wire_shape(value: &Value, depth: usize) -> String {
    if depth >= 4 {
        return "...".to_string();
    }
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(values) => values.first().map_or_else(
            || "array(len=0)".to_string(),
            |first| {
                format!(
                    "array(len={}, first={})",
                    values.len(),
                    describe_editorial_wire_shape(first, depth.saturating_add(1))
                )
            },
        ),
        Value::Object(values) => {
            let fields = values
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{key}:{}",
                        describe_editorial_wire_shape(value, depth.saturating_add(1))
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("object({fields})")
        }
    }
}

/// Result of compiling and optionally publishing one LLM-authored article.
#[derive(Debug, Clone, Serialize)]
pub struct CompiledWikiArticle {
    /// Stable article record.
    pub view: KnowledgeView,
    /// Current immutable article revision.
    pub revision: KnowledgeViewRevision,
    /// Whether a new LLM-authored revision was stored.
    pub changed: bool,
}

/// Bounds for creating a curated wiki from an existing Elephant graph.
#[derive(Debug, Clone)]
pub struct WikiCreationConfig {
    /// Maximum article entities the planner may publish. Zero uses exhaustive
    /// automatic coverage of the eligible graph, up to the safety ceiling.
    pub max_articles: usize,
    /// Maximum explicit article links on one planned page.
    pub max_related_articles: usize,
    /// Graph traversal depth used by each generated topic definition.
    pub article_max_hops: u8,
    /// Maximum graph members supplied to each article writer.
    pub article_max_members: u16,
    /// Publish verified articles instead of retaining drafts.
    pub publish: bool,
}

/// Explicit durable delegation to the wiki service, granted by `manage:wiki`.
/// The space binding prevents a queued payload from acquiring another space.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedWikiAuthority {
    /// Permission to maintain all wiki articles in this one space.
    Space {
        /// Space covered by this delegation.
        space_id: String,
        /// Durable registration that fences revoked or replaced chains.
        grant_id: RecordId,
    },
}

impl ManagedWikiAuthority {
    fn grant_id(&self) -> &RecordId {
        match self {
            Self::Space { grant_id, .. } => grant_id,
        }
    }

    /// Returns whether this exact service grant still authorizes future work.
    pub async fn is_active(&self, storage: &StorageClient, space_id: &str) -> PipelineResult<bool> {
        self.validate(space_id)?;
        Ok(WorkItemRepository::new(storage.clone())
            .managed_wiki_grant_is_active(space_id, self.grant_id())
            .await?)
    }

    async fn ensure_active(&self, storage: &StorageClient, space_id: &str) -> PipelineResult<()> {
        if self.is_active(storage, space_id).await? {
            Ok(())
        } else {
            Err(PipelineError::ValidationError(
                "Managed wiki service grant has been revoked".to_string(),
            ))
        }
    }

    fn validate(&self, space_id: &str) -> PipelineResult<()> {
        match self {
            Self::Space {
                space_id: granted, ..
            } if !space_id.trim().is_empty() && granted == space_id => Ok(()),
            Self::Space { .. } => Err(PipelineError::ValidationError(
                "Managed wiki authority does not match the target space".to_string(),
            )),
        }
    }
}

/// Durable configuration carried by recurring Living Wiki work items.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManagedWikiPayload {
    /// Required service grant. Legacy jobs without it fail closed.
    pub authority: ManagedWikiAuthority,
    /// Global article cap across all security envelopes.
    pub max_articles: usize,
    /// Graph hops supplied to each article compiler.
    pub article_max_hops: u8,
    /// Maximum graph members supplied to each article compiler.
    pub article_max_members: u16,
    /// Minutes between complete automatic reconciliation runs.
    pub refresh_interval_minutes: u32,
    /// Exact planner and writer model.
    pub writer_model: String,
    /// Exact independent verifier model.
    pub verifier_model: String,
    /// Last complete graph fingerprint successfully reconciled by this
    /// managed chain. Unchanged graphs reuse the current published wiki
    /// without spending planner, writer, or verifier calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_graph_fingerprint: Option<String>,
}

impl ManagedWikiPayload {
    /// Converts the durable payload into one publication configuration.
    #[must_use]
    pub const fn creation_config(&self) -> WikiCreationConfig {
        WikiCreationConfig {
            max_articles: self.max_articles,
            max_related_articles: 12,
            article_max_hops: self.article_max_hops,
            article_max_members: self.article_max_members,
            publish: true,
        }
    }
}

impl Default for WikiCreationConfig {
    fn default() -> Self {
        Self {
            max_articles: 0,
            max_related_articles: 12,
            article_max_hops: 1,
            article_max_members: 250,
            publish: false,
        }
    }
}

/// One article that could not pass planning, context, or publication gates.
#[derive(Debug, Clone, Serialize)]
pub struct WikiCreationFailure {
    /// Canonical entity selected by the planner.
    pub entity_id: String,
    /// Fail-closed reason.
    pub error: String,
}

/// Result of planning and compiling a wiki from an existing graph inventory.
#[derive(Debug, Clone, Serialize)]
pub struct CreatedWiki {
    /// Auditable proof that curation signals were computed from the complete
    /// security-scoped graph rather than a caller-selected prefix.
    pub receipt: WikiPlanningReceipt,
    /// Exact structured plan returned by the LLM and validated by Elephant.
    pub plan: WikiPlan,
    /// LLM-authored global reader organization for every planned article.
    pub organization: WikiOrganization,
    /// Articles that passed writing, verification, and publication gates.
    pub articles: Vec<CompiledWikiArticle>,
    /// Per-article failures. A bad graph neighborhood does not suppress clean
    /// articles elsewhere in the plan.
    pub failures: Vec<WikiCreationFailure>,
}

/// Full-graph scan and reduction receipt for one security-scoped wiki plan.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WikiPlanningReceipt {
    /// Active canonical entities considered before graph-quality filtering.
    pub entities_scanned: usize,
    /// Connected groups of person records whose names may identify the same
    /// person. Retained under its original serialized field name for receipt
    /// compatibility; candidates are now sent to the LLM as non-binding
    /// identity-review hints.
    pub ambiguous_person_groups_withheld: usize,
    /// Identity-ambiguous person entities silently withheld. This is zero in
    /// the exhaustive planner because heuristic identity deletion is forbidden.
    pub ambiguous_person_entities_withheld: usize,
    /// Complete relationship table rows scanned for curation signals.
    pub relationships_scanned: usize,
    /// Approved relationship rows withheld because their predicate has not
    /// passed explicit semantics governance.
    pub relationships_withheld_by_predicate_governance: usize,
    /// Complete assertion table rows scanned for source-backed salience.
    pub assertions_scanned: usize,
    /// Complete attribute table rows scanned for governed literal facts.
    pub attributes_scanned: usize,
    /// Approved attribute rows withheld because their predicate has not
    /// passed explicit semantics governance.
    pub attributes_withheld_by_predicate_governance: usize,
    /// Complete domain-event table rows scanned for timeline salience.
    pub events_scanned: usize,
    /// Complete truth-slot table rows scanned for current/conflicting state.
    pub truth_slots_scanned: usize,
    /// Entities with at least one meaningful summary, governed edge,
    /// resolved source-backed assertion, governed attribute, approved event,
    /// or current knowledge issue.
    pub eligible_entities: usize,
    /// Eligible entities offered across all bounded planner prompts.
    pub planner_candidates: usize,
    /// Bounded graph-neighborhood prompts used to cover the full inventory.
    pub planning_partitions: usize,
    /// Hash of every security-scoped curation input read by the full scan.
    pub graph_fingerprint: String,
}

/// Result of one complete automatic reconciliation across every security
/// envelope represented in an authorized graph.
#[derive(Debug, Clone, Serialize)]
pub struct ReconciledWiki {
    /// Combined fingerprint of every security-scoped graph inventory.
    pub graph_fingerprint: String,
    /// Whether this run observed a graph change and executed LLM curation.
    pub changed: bool,
    /// Exact security envelopes observed in the full entity scan.
    pub security_envelopes_scanned: usize,
    /// Envelopes allocated at least one article by the automatic global cap.
    pub security_envelopes_planned: usize,
    /// Total article subjects selected by all planner calls.
    pub selected_articles: usize,
    /// Articles that passed writing and verification.
    pub compiled_articles: usize,
    /// Selected articles that failed closed.
    pub failed_articles: usize,
    /// Previously managed articles hidden because they were not successfully
    /// retained by this complete reconciliation.
    pub hidden_articles: usize,
    /// Full scan receipts produced before any unchanged-graph cache decision.
    pub scan_receipts: Vec<WikiPlanningReceipt>,
    /// Security-scoped plans and their full-graph receipts.
    pub wikis: Vec<CreatedWiki>,
}

/// Uses an LLM to curate article identity and navigation, then delegates every
/// article to the evidence-verified editorial compiler.
#[derive(Clone)]
pub struct WikiGraphPlanner {
    storage: StorageClient,
    definitions: KnowledgeViewDefinitionRepository,
    views: KnowledgeViewRepository,
    assertions: AssertionRepository,
    attributes: AttributeRepository,
    events: EventRepository,
    relations: RelRepository,
    predicates: PredicateRegistryRepository,
    truth_slots: TruthSlotRepository,
    llm: Arc<dyn WikiEditorialLlm>,
    editorial_config: WikiEditorialConfig,
}

impl std::fmt::Debug for WikiGraphPlanner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WikiGraphPlanner")
            .field("editorial_config", &self.editorial_config)
            .finish_non_exhaustive()
    }
}

impl WikiGraphPlanner {
    /// Creates a planner with an injectable LLM seam.
    #[must_use]
    pub fn new(
        storage: StorageClient,
        llm: Arc<dyn WikiEditorialLlm>,
        editorial_config: WikiEditorialConfig,
    ) -> Self {
        Self {
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            views: KnowledgeViewRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            events: EventRepository::new(storage.clone()),
            relations: RelRepository::new(storage.clone()),
            predicates: PredicateRegistryRepository::new(storage.clone()),
            truth_slots: TruthSlotRepository::new(storage.clone()),
            storage,
            llm,
            editorial_config,
        }
    }

    /// Curates and compiles a wiki from caller-authorized graph entities.
    ///
    /// Candidates must share one exact security envelope. This keeps a single
    /// planning prompt from mixing knowledge across policy boundaries. Callers
    /// with several envelopes run one plan per envelope.
    pub async fn create_from_entities(
        &self,
        space_id: &str,
        candidates: Vec<Entity>,
        config: WikiCreationConfig,
    ) -> PipelineResult<CreatedWiki> {
        validate_creation_config(&config)?;
        let prepared = self.build_planning_inventory(space_id, &candidates).await?;
        if prepared.entities.is_empty() {
            return Err(PipelineError::ValidationError(
                "Wiki planning requires at least one authorized semantic entity".to_string(),
            ));
        }
        let (plan, organization, resolved_config) = self
            .plan_complete_inventory(&prepared.entities, &config)
            .await?;
        let navigation = organization_navigation(&organization, &plan)?;

        let entities = candidates
            .into_iter()
            .map(|entity| (entity.base.id.to_string(), entity))
            .collect::<BTreeMap<_, _>>();
        let compiler = WikiEditorialCompiler::new(
            self.storage.clone(),
            Arc::clone(&self.llm),
            self.editorial_config.clone(),
        );
        let mut compilation_jobs = Vec::new();
        for (index, planned) in plan.articles.iter().enumerate() {
            let Some(entity) = entities.get(&planned.entity_id) else {
                return Err(PipelineError::ValidationError(format!(
                    "Planner selected unknown entity {}",
                    planned.entity_id
                )));
            };
            let mut definition = planned_definition(
                space_id,
                entity,
                &entities,
                planned,
                PlannedDefinitionPresentation {
                    relation_types: publishable_relation_types(planned, &prepared.entities),
                    navigation: navigation.get(&planned.entity_id).cloned(),
                },
                &resolved_config,
                self.llm.writer_model(),
            )?;
            if let Some(scope) = &prepared.subject_scope {
                merge_subject_scope(&mut definition.base, scope);
            }
            let definition_id = definition.base.id.clone();
            let existing = self.definitions.get(space_id, &definition_id).await?;
            let definition_changed = existing.as_ref().is_none_or(|existing| {
                existing.title != definition.title
                    || existing.publication_status != definition.publication_status
                    || existing.related_articles != definition.related_articles
                    || existing.navigation != definition.navigation
                    || existing.selector != definition.selector
                    || existing.base.security != definition.base.security
                    || existing.base.subjects != definition.base.subjects
                    || existing.base.subjects_state != definition.base.subjects_state
                    || existing.base.provenance.extractor != definition.base.provenance.extractor
                    || existing.base.provenance.prompt_id != definition.base.provenance.prompt_id
                    || existing.base.provenance.config_id != definition.base.provenance.config_id
            });
            if let Some(existing) = existing {
                definition.base.id = existing.base.id;
                definition.base.created_at = existing.base.created_at;
                if definition_changed {
                    definition.base.updated_at = Utc::now();
                } else {
                    definition.base.updated_at = existing.base.updated_at;
                }
            }
            if definition_changed {
                self.definitions.upsert(definition).await?;
                self.views
                    .mark_dirty_for_dependencies(
                        space_id,
                        &[definition_id.to_string()],
                        "wiki plan or authorization scope changed",
                    )
                    .await?;
            }
            compilation_jobs.push((index, planned.entity_id.clone(), definition_id));
        }
        let publish = resolved_config.publish;
        let concurrency = wiki_article_concurrency();
        let mut compiled = stream::iter(compilation_jobs.into_iter().map(
            |(index, entity_id, definition_id)| {
                let compiler = compiler.clone();
                async move {
                    let result = compiler.compile(space_id, &definition_id, publish).await;
                    (index, entity_id, result)
                }
            },
        ))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
        compiled.sort_by_key(|(index, _, _)| *index);
        let mut articles = Vec::new();
        let mut failures = Vec::new();
        for (_, entity_id, result) in compiled {
            match result {
                Ok(article) => articles.push(article),
                Err(error) => failures.push(WikiCreationFailure {
                    entity_id,
                    error: error.to_string(),
                }),
            }
        }
        Ok(CreatedWiki {
            receipt: prepared.receipt,
            plan,
            organization,
            articles,
            failures,
        })
    }

    async fn plan_complete_inventory(
        &self,
        inventory: &[WikiPlanningEntity],
        config: &WikiCreationConfig,
    ) -> PipelineResult<(WikiPlan, WikiOrganization, WikiCreationConfig)> {
        let partitions = planning_partitions(inventory);
        let article_budget =
            resolve_article_budget(config.max_articles, inventory.len(), partitions.len())?;
        let quotas = allocate_partition_quotas(&partitions, article_budget);
        let mut combined = WikiPlan {
            language: "en".to_string(),
            articles: Vec::new(),
        };
        let planning_jobs = partitions
            .into_iter()
            .zip(quotas)
            .enumerate()
            .filter(|(_, (_, quota))| *quota > 0)
            .map(|(index, (partition, quota))| {
                let planner = self.clone();
                let config = config.clone();
                async move {
                    planner
                        .plan_partition(index, partition, quota, config)
                        .await
                }
            });
        let mut planned = stream::iter(planning_jobs)
            .buffer_unordered(wiki_planner_concurrency())
            .collect::<Vec<_>>()
            .await;
        planned.sort_by_key(|result| match result {
            Ok((index, _)) => *index,
            Err(_) => usize::MAX,
        });
        for result in planned {
            let (_, plan) = result?;
            combined.articles.extend(plan.articles);
        }
        let mut resolved = config.clone();
        resolved.max_articles = article_budget;
        augment_plan_navigation(&mut combined, inventory, resolved.max_related_articles);
        validate_wiki_plan(&combined, inventory, &resolved)?;

        let organization = self
            .organize_complete_inventory(&combined, inventory)
            .await?;
        Ok((combined, organization, resolved))
    }

    async fn organize_complete_inventory(
        &self,
        plan: &WikiPlan,
        inventory: &[WikiPlanningEntity],
    ) -> PipelineResult<WikiOrganization> {
        let organizer_json = organizer_inventory_json(plan, inventory)?;
        let organizer_context_budget =
            full_wiki_structure_context_char_budget(self.editorial_config.max_context_chars);
        if organizer_json.chars().count() > organizer_context_budget {
            return Err(PipelineError::BudgetExceeded(format!(
                "Wiki organizer inventory exceeds {} characters",
                organizer_context_budget
            )));
        }
        let base_prompt = render_organizer_prompt(&organizer_json);
        let mut prompt = base_prompt.clone();
        let mut last_error = None;
        for attempt in 1..=WIKI_PLANNING_MAX_ATTEMPTS {
            match self.llm.organize_wiki(prompt.clone()).await {
                Ok(candidate) => match validate_wiki_taxonomy(&candidate, plan) {
                    Ok(()) => {
                        return self.assign_wiki_taxonomy(candidate, plan, inventory).await;
                    }
                    Err(error) => {
                        prompt =
                            render_organizer_repair_prompt(&organizer_json, &candidate, &error)?;
                        last_error = Some(error);
                    }
                },
                Err(error) => {
                    prompt.clone_from(&base_prompt);
                    last_error = Some(error);
                }
            }
            if attempt < WIKI_PLANNING_MAX_ATTEMPTS {
                wait_before_wiki_llm_retry(attempt).await;
            }
        }
        Err(PipelineError::ValidationError(format!(
            "Wiki taxonomy organizer failed after {WIKI_PLANNING_MAX_ATTEMPTS} attempts: {}",
            last_error.map_or_else(
                || "no organizer result was returned".to_string(),
                |error| error.to_string()
            )
        )))
    }

    async fn assign_wiki_taxonomy(
        &self,
        taxonomy: WikiOrganization,
        plan: &WikiPlan,
        inventory: &[WikiPlanningEntity],
    ) -> PipelineResult<WikiOrganization> {
        let taxonomy_json = organizer_taxonomy_json(&taxonomy)?;
        let allowed_section_ids = taxonomy
            .sections
            .iter()
            .map(|section| section.id.clone())
            .collect::<BTreeSet<_>>();
        let section_anchors = taxonomy
            .sections
            .iter()
            .filter_map(|section| {
                section
                    .article_entity_ids
                    .first()
                    .map(|entity_id| (section.id.clone(), entity_id.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        let mut jobs = Vec::new();
        for (index, articles) in plan
            .articles
            .chunks(MAX_WIKI_PLANNING_PARTITION_ENTITIES)
            .enumerate()
        {
            let partition = WikiPlan {
                language: "en".to_string(),
                articles: articles.to_vec(),
            };
            let inventory_json = organizer_inventory_json(&partition, inventory)?;
            let planner = self.clone();
            let taxonomy_json = taxonomy_json.clone();
            let allowed_section_ids = allowed_section_ids.clone();
            jobs.push(async move {
                planner
                    .assign_wiki_taxonomy_partition(
                        index,
                        partition,
                        taxonomy_json,
                        inventory_json,
                        allowed_section_ids,
                    )
                    .await
            });
        }
        let mut assignments = stream::iter(jobs)
            .buffer_unordered(wiki_planner_concurrency())
            .collect::<Vec<_>>()
            .await;
        assignments.sort_by_key(|result| match result {
            Ok((index, _)) => *index,
            Err(_) => usize::MAX,
        });

        let mut organization = taxonomy;
        for section in &mut organization.sections {
            section.article_entity_ids.clear();
        }
        let section_index = organization
            .sections
            .iter()
            .enumerate()
            .map(|(index, section)| (section.id.clone(), index))
            .collect::<BTreeMap<_, _>>();
        for result in assignments {
            let (_, assignment) = result?;
            for section in assignment.sections {
                let Some(index) = section_index.get(&section.id).copied() else {
                    return Err(PipelineError::ValidationError(format!(
                        "Wiki assignment returned unknown section {}",
                        section.id
                    )));
                };
                organization.sections[index]
                    .article_entity_ids
                    .extend(section.article_entity_ids);
            }
        }
        stabilize_wiki_organization(&mut organization, plan, &section_anchors)?;
        validate_wiki_organization(&organization, plan)?;
        Ok(organization)
    }

    async fn assign_wiki_taxonomy_partition(
        &self,
        index: usize,
        partition: WikiPlan,
        taxonomy_json: String,
        inventory_json: String,
        allowed_section_ids: BTreeSet<String>,
    ) -> PipelineResult<(usize, WikiOrganization)> {
        let base_prompt = render_organizer_assignment_prompt(&taxonomy_json, &inventory_json);
        let mut prompt = base_prompt.clone();
        let mut last_error = None;
        for attempt in 1..=WIKI_PLANNING_MAX_ATTEMPTS {
            match self.llm.organize_wiki(prompt.clone()).await {
                Ok(candidate) => {
                    match validate_wiki_assignment(&candidate, &partition, &allowed_section_ids) {
                        Ok(()) => return Ok((index, candidate)),
                        Err(error) => {
                            prompt = render_organizer_assignment_repair_prompt(
                                &taxonomy_json,
                                &inventory_json,
                                &candidate,
                                &error,
                            )?;
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    prompt.clone_from(&base_prompt);
                    last_error = Some(error);
                }
            }
            if attempt < WIKI_PLANNING_MAX_ATTEMPTS {
                wait_before_wiki_llm_retry(attempt).await;
            }
        }
        Err(PipelineError::ValidationError(format!(
            "Wiki section assignment partition {index} failed after {WIKI_PLANNING_MAX_ATTEMPTS} attempts: {}",
            last_error.map_or_else(
                || "no assignment result was returned".to_string(),
                |error| error.to_string()
            )
        )))
    }

    async fn plan_partition(
        &self,
        index: usize,
        partition: Vec<WikiPlanningEntity>,
        quota: usize,
        config: WikiCreationConfig,
    ) -> PipelineResult<(usize, WikiPlan)> {
        let inventory_json = serde_json::to_string_pretty(&partition).map_err(|error| {
            PipelineError::ValidationError(format!(
                "Could not serialize wiki planning partition: {error}"
            ))
        })?;
        if inventory_json.chars().count() > self.editorial_config.max_context_chars {
            return Err(PipelineError::BudgetExceeded(format!(
                "Wiki planning partition exceeds {} characters",
                self.editorial_config.max_context_chars
            )));
        }
        let mut scoped = config;
        scoped.max_articles = quota;
        let prompt = render_planner_prompt(&inventory_json, &scoped);
        let mut last_validation_error = None;
        for attempt in 1..=WIKI_PLANNING_MAX_ATTEMPTS {
            let mut plan = match self.llm.plan_wiki(prompt.clone()).await {
                Ok(plan) => plan,
                Err(error) => {
                    last_validation_error = Some(error);
                    if attempt < WIKI_PLANNING_MAX_ATTEMPTS {
                        wait_before_wiki_llm_retry(attempt).await;
                    }
                    continue;
                }
            };
            augment_plan_navigation(&mut plan, &partition, scoped.max_related_articles);
            let validation = validate_wiki_plan(&plan, &partition, &scoped);
            match validation {
                Ok(()) => return Ok((index, plan)),
                Err(error) => {
                    last_validation_error = Some(error);
                    if attempt < WIKI_PLANNING_MAX_ATTEMPTS {
                        wait_before_wiki_llm_retry(attempt).await;
                    }
                }
            }
        }
        Err(PipelineError::ValidationError(format!(
            "Wiki planner partition {index} failed after {WIKI_PLANNING_MAX_ATTEMPTS} attempts: {}",
            last_validation_error.map_or_else(
                || "no planner result was returned".to_string(),
                |error| error.to_string()
            )
        )))
    }

    /// Automatically partitions a complete graph scan by exact security
    /// envelope, allocates the global article budget, runs each LLM plan, and
    /// reconciles the reader-visible article set.
    pub async fn reconcile_full_graph(
        &self,
        space_id: &str,
        candidates: Vec<Entity>,
        config: WikiCreationConfig,
    ) -> PipelineResult<ReconciledWiki> {
        self.reconcile_full_graph_if_changed(space_id, candidates, config, None)
            .await
    }

    /// Reconciles the complete graph unless its canonical fingerprint matches
    /// the last successfully published managed run.
    pub async fn reconcile_full_graph_if_changed(
        &self,
        space_id: &str,
        candidates: Vec<Entity>,
        config: WikiCreationConfig,
        previous_graph_fingerprint: Option<&str>,
    ) -> PipelineResult<ReconciledWiki> {
        validate_creation_config(&config)?;
        let mut groups = BTreeMap::<String, Vec<Entity>>::new();
        for entity in candidates {
            if entity.base.space_id != space_id || entity.base.deleted_at.is_some() {
                return Err(PipelineError::ValidationError(
                    "Full-graph wiki reconciliation received an out-of-space or deleted entity"
                        .to_string(),
                ));
            }
            let key = serde_json::to_string(&entity.base.security).map_err(|error| {
                PipelineError::ValidationError(format!(
                    "Could not group wiki candidates by security: {error}"
                ))
            })?;
            groups.entry(key).or_default().push(entity);
        }
        if groups.is_empty() {
            return Err(PipelineError::ValidationError(
                "Full-graph wiki reconciliation found no active semantic entities".to_string(),
            ));
        }
        let security_envelopes_scanned = groups.len();
        let mut fingerprint = Sha256::new();
        hash_wiki_editorial_contract(&mut fingerprint);
        let mut scan_receipts = Vec::new();
        let mut eligible_by_security = BTreeMap::new();
        for (security_key, entities) in &groups {
            let prepared = self.build_planning_inventory(space_id, entities).await?;
            hash_wiki_curation_input(
                &mut fingerprint,
                &[
                    security_key.clone(),
                    prepared.receipt.graph_fingerprint.clone(),
                    prepared.receipt.eligible_entities.to_string(),
                    prepared.receipt.planning_partitions.to_string(),
                ],
            );
            eligible_by_security.insert(security_key.clone(), prepared.receipt.eligible_entities);
            scan_receipts.push(prepared.receipt);
        }
        let graph_fingerprint = format!("sha256:{}", hex::encode(fingerprint.finalize()));
        let published_views = self
            .views
            .list_views_for_surface(
                space_id,
                KnowledgeViewSurface::Wiki,
                KnowledgeViewPublicationStatus::Published,
                250,
            )
            .await?;
        let published_by_root = published_views
            .iter()
            .filter(|view| view.base.created_by == ARTICLE_COMPILER_NAME)
            .map(|view| (view.root_record.as_str(), view))
            .collect::<BTreeMap<_, _>>();
        let mut managed_definitions_current = true;
        let mut definition_cursor = None;
        let mut published_definition_ids = BTreeSet::new();
        loop {
            let page = self
                .definitions
                .list_page(space_id, 250, definition_cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            let last_page = page.len() < 250;
            definition_cursor = page.last().map(|definition| definition.base.id.clone());
            for definition in page.into_iter().filter(|definition| {
                definition.base.created_by == "elephant.wiki_planner"
                    && definition.publication_status == KnowledgeViewPublicationStatus::Published
            }) {
                let id = definition.base.id.to_string();
                published_definition_ids.insert(id.clone());
                let Some(view) = published_by_root.get(id.as_str()) else {
                    managed_definitions_current = false;
                    continue;
                };
                if definition.navigation.is_none() || view.state == ProjectionState::Dirty {
                    managed_definitions_current = false;
                }
            }
            if last_page {
                break;
            }
        }
        if published_by_root
            .keys()
            .any(|root| !published_definition_ids.contains(*root))
        {
            managed_definitions_current = false;
        }
        if scan_receipts
            .iter()
            .any(|receipt| receipt.eligible_entities > 0)
            && published_definition_ids.is_empty()
        {
            managed_definitions_current = false;
        }
        if previous_graph_fingerprint == Some(graph_fingerprint.as_str())
            && managed_definitions_current
        {
            return Ok(ReconciledWiki {
                graph_fingerprint,
                changed: false,
                security_envelopes_scanned,
                security_envelopes_planned: 0,
                selected_articles: 0,
                compiled_articles: 0,
                failed_articles: 0,
                hidden_articles: 0,
                scan_receipts,
                wikis: Vec::new(),
            });
        }
        let minimum_planning_partitions = scan_receipts
            .iter()
            .map(|receipt| receipt.planning_partitions)
            .sum::<usize>();
        let total_candidates = scan_receipts
            .iter()
            .map(|receipt| receipt.eligible_entities)
            .sum::<usize>();
        let article_budget = resolve_article_budget(
            config.max_articles,
            total_candidates,
            minimum_planning_partitions,
        )?;
        let quotas = allocate_article_quotas(&eligible_by_security, article_budget);
        let mut wikis = Vec::new();
        for (security_key, candidates) in groups {
            let quota = quotas.get(&security_key).copied().unwrap_or_default();
            if quota == 0 {
                continue;
            }
            let mut scoped_config = config.clone();
            scoped_config.max_articles = quota;
            wikis.push(
                self.create_from_entities(space_id, candidates, scoped_config)
                    .await?,
            );
        }

        let retained_definition_ids = wikis
            .iter()
            .flat_map(|wiki| &wiki.articles)
            .map(|article| article.view.root_record.clone())
            .collect::<BTreeSet<_>>();
        let hidden_articles = if config.publish {
            self.reconcile_managed_articles(space_id, &retained_definition_ids)
                .await?
        } else {
            0
        };
        Ok(ReconciledWiki {
            graph_fingerprint,
            changed: true,
            security_envelopes_scanned,
            security_envelopes_planned: quotas.len(),
            selected_articles: wikis.iter().map(|wiki| wiki.plan.articles.len()).sum(),
            compiled_articles: wikis.iter().map(|wiki| wiki.articles.len()).sum(),
            failed_articles: wikis.iter().map(|wiki| wiki.failures.len()).sum(),
            hidden_articles,
            scan_receipts,
            wikis,
        })
    }

    /// Reconciles the reader-visible article set after a complete automatic
    /// planning run. Planner-owned definitions that were not successfully
    /// republished are hidden, while their immutable history is retained.
    pub async fn reconcile_managed_articles(
        &self,
        space_id: &str,
        retained_definition_ids: &BTreeSet<String>,
    ) -> PipelineResult<usize> {
        let mut cursor = None;
        let mut hidden = 0_usize;
        loop {
            let page = self
                .definitions
                .list_page(space_id, 250, cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|definition| definition.base.id.clone());
            let last_page = page.len() < 250;
            for mut definition in page.into_iter().filter(|definition| {
                definition.base.created_by == "elephant.wiki_planner"
                    && !retained_definition_ids.contains(&definition.base.id.to_string())
            }) {
                if definition.publication_status != KnowledgeViewPublicationStatus::Hidden {
                    definition.publication_status = KnowledgeViewPublicationStatus::Hidden;
                    definition.base.updated_at = Utc::now();
                    self.definitions.upsert(definition.clone()).await?;
                }
                hidden = hidden.saturating_add(
                    self.views
                        .set_publication_status_for_root_record(
                            space_id,
                            &definition.base.id.to_string(),
                            KnowledgeViewPublicationStatus::Hidden,
                        )
                        .await?,
                );
            }
            if last_page {
                break;
            }
        }
        Ok(hidden)
    }

    async fn build_planning_inventory(
        &self,
        space_id: &str,
        candidates: &[Entity],
    ) -> PipelineResult<PreparedWikiInventory> {
        const MAX_CONNECTIONS_PER_ENTITY: usize = 8;

        let Some(first) = candidates.first() else {
            return Ok(PreparedWikiInventory::default());
        };
        if candidates.iter().any(|entity| {
            entity.base.space_id != space_id
                || entity.base.security != first.base.security
                || entity.base.deleted_at.is_some()
        }) {
            return Err(PipelineError::ValidationError(
                "Wiki planning candidates must be active entities in one space with one exact security envelope"
                    .to_string(),
            ));
        }
        let (possible_person_duplicates, possible_person_duplicate_groups) =
            possible_person_duplicate_records(candidates);
        let by_id = candidates
            .iter()
            .map(|entity| (entity.base.id.clone(), entity))
            .collect::<BTreeMap<_, _>>();
        let mut subject_scope = first.base.clone();
        subject_scope.subjects.clear();
        subject_scope.subjects_state = SubjectsState::Complete;
        for entity in candidates {
            merge_record_subject_scope(&mut subject_scope, entity);
        }
        let mut connections = BTreeMap::<RecordId, Vec<WikiPlanningConnection>>::new();
        let mut graph_degree = BTreeMap::<RecordId, usize>::new();
        let mut predicate_publishability = BTreeMap::<String, bool>::new();
        let mut fingerprint = Sha256::new();
        for entity in candidates {
            hash_wiki_curation_input(
                &mut fingerprint,
                &[serde_json::to_string(entity).map_err(|error| {
                    PipelineError::ValidationError(format!("Could not fingerprint entity: {error}"))
                })?],
            );
        }
        let mut cursor = None;
        let mut scanned = 0_usize;
        let mut relationships_withheld_by_predicate_governance = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .relations
                .list(space_id, page_limit, cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            scanned = scanned.saturating_add(page.len());
            cursor = page.last().map(|relation| relation.base.id.clone());
            let last_page = page.len() < page_limit;
            for relation in page {
                hash_wiki_curation_input(
                    &mut fingerprint,
                    &[serde_json::to_string(&relation).map_err(|error| {
                        PipelineError::ValidationError(format!(
                            "Could not fingerprint relationship: {error}"
                        ))
                    })?],
                );
                if relation.status != ClaimStatus::Approved
                    || relation.base.security != first.base.security
                {
                    continue;
                }
                let publishable =
                    if let Some(publishable) = predicate_publishability.get(&relation.rel_type) {
                        *publishable
                    } else {
                        let registry = self
                            .predicates
                            .find_by_predicate_id(space_id, &relation.rel_type)
                            .await?;
                        if let Some(registry) = &registry {
                            hash_wiki_curation_input(
                                &mut fingerprint,
                                &[serde_json::to_string(registry).map_err(|error| {
                                    PipelineError::ValidationError(format!(
                                        "Could not fingerprint predicate registry: {error}"
                                    ))
                                })?],
                            );
                        }
                        let publishable = registry.as_ref().is_some_and(predicate_is_publishable);
                        predicate_publishability.insert(relation.rel_type.clone(), publishable);
                        publishable
                    };
                if !publishable {
                    relationships_withheld_by_predicate_governance =
                        relationships_withheld_by_predicate_governance.saturating_add(1);
                    continue;
                }
                merge_record_subject_scope(&mut subject_scope, &relation);
                let Some(subject) = by_id.get(relation.subject.id()) else {
                    continue;
                };
                let Some(object) = by_id.get(relation.object.id()) else {
                    continue;
                };
                let subject_degree = graph_degree.entry(subject.base.id.clone()).or_default();
                *subject_degree = subject_degree.saturating_add(1);
                let object_degree = graph_degree.entry(object.base.id.clone()).or_default();
                *object_degree = object_degree.saturating_add(1);
                connections
                    .entry(subject.base.id.clone())
                    .or_default()
                    .push(WikiPlanningConnection {
                        predicate: relation.rel_type.clone(),
                        entity_id: object.base.id.to_string(),
                        display_name: object.display_name.clone(),
                    });
                connections.entry(object.base.id.clone()).or_default().push(
                    WikiPlanningConnection {
                        predicate: relation.rel_type,
                        entity_id: subject.base.id.to_string(),
                        display_name: subject.display_name.clone(),
                    },
                );
            }
            if last_page {
                break;
            }
        }

        let mut evidence_assertions = BTreeMap::<RecordId, usize>::new();
        let mut earliest_evidence_at = BTreeMap::<RecordId, DateTime<Utc>>::new();
        let mut latest_evidence_at = BTreeMap::<RecordId, DateTime<Utc>>::new();
        let mut evidence_connections = BTreeMap::<RecordId, BTreeMap<RecordId, usize>>::new();
        let mut assertion_cursor = None;
        let mut assertions_scanned = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .assertions
                .list(space_id, page_limit, assertion_cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            assertions_scanned = assertions_scanned.saturating_add(page.len());
            assertion_cursor = page.last().map(|assertion| assertion.base.id.clone());
            let last_page = page.len() < page_limit;
            for assertion in page {
                hash_wiki_curation_input(
                    &mut fingerprint,
                    &[serde_json::to_string(&assertion).map_err(|error| {
                        PipelineError::ValidationError(format!(
                            "Could not fingerprint assertion: {error}"
                        ))
                    })?],
                );
                if assertion.base.security != first.base.security
                    || assertion.resolution_status != ResolutionStatus::Resolved
                    || assertion.lifecycle_tier != "active"
                    || assertion
                        .significance
                        .as_deref()
                        .is_some_and(|significance| significance == "skip")
                {
                    continue;
                }
                merge_record_subject_scope(&mut subject_scope, &assertion);
                for entity_id in assertion
                    .subject_entity
                    .iter()
                    .chain(assertion.object_entity.iter())
                    .map(Link::id)
                    .filter(|entity_id| by_id.contains_key(*entity_id))
                {
                    let count = evidence_assertions.entry(entity_id.clone()).or_default();
                    *count = count.saturating_add(1);
                    let semantic_time =
                        assertion.source_event_time.unwrap_or(assertion.observed_at);
                    earliest_evidence_at
                        .entry(entity_id.clone())
                        .and_modify(|current| *current = (*current).min(semantic_time))
                        .or_insert(semantic_time);
                    latest_evidence_at
                        .entry(entity_id.clone())
                        .and_modify(|current| *current = (*current).max(semantic_time))
                        .or_insert(semantic_time);
                }
                if let (Some(subject), Some(object)) = (
                    assertion.subject_entity.as_ref().map(Link::id),
                    assertion.object_entity.as_ref().map(Link::id),
                ) {
                    if subject != object
                        && by_id.contains_key(subject)
                        && by_id.contains_key(object)
                    {
                        let forward = evidence_connections
                            .entry(subject.clone())
                            .or_default()
                            .entry(object.clone())
                            .or_default();
                        *forward = forward.saturating_add(1);
                        let reverse = evidence_connections
                            .entry(object.clone())
                            .or_default()
                            .entry(subject.clone())
                            .or_default();
                        *reverse = reverse.saturating_add(1);
                    }
                }
            }
            if last_page {
                break;
            }
        }

        let mut attribute_facts = BTreeMap::<RecordId, usize>::new();
        let mut attribute_cursor = None;
        let mut attributes_scanned = 0_usize;
        let mut attributes_withheld_by_predicate_governance = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .attributes
                .list(space_id, page_limit, attribute_cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            attributes_scanned = attributes_scanned.saturating_add(page.len());
            attribute_cursor = page.last().map(|attribute| attribute.base.id.clone());
            let last_page = page.len() < page_limit;
            for attribute in page {
                hash_wiki_curation_input(
                    &mut fingerprint,
                    &[serde_json::to_string(&attribute).map_err(|error| {
                        PipelineError::ValidationError(format!(
                            "Could not fingerprint attribute: {error}"
                        ))
                    })?],
                );
                if attribute.base.security != first.base.security
                    || attribute.status != ClaimStatus::Approved
                    || !by_id.contains_key(attribute.subject.id())
                {
                    continue;
                }
                let publishable =
                    if let Some(publishable) = predicate_publishability.get(&attribute.attr_type) {
                        *publishable
                    } else {
                        let registry = self
                            .predicates
                            .find_by_predicate_id(space_id, &attribute.attr_type)
                            .await?;
                        if let Some(registry) = &registry {
                            hash_wiki_curation_input(
                                &mut fingerprint,
                                &[serde_json::to_string(registry).map_err(|error| {
                                    PipelineError::ValidationError(format!(
                                        "Could not fingerprint predicate registry: {error}"
                                    ))
                                })?],
                            );
                        }
                        let publishable = registry.as_ref().is_some_and(predicate_is_publishable);
                        predicate_publishability.insert(attribute.attr_type.clone(), publishable);
                        publishable
                    };
                if publishable {
                    merge_record_subject_scope(&mut subject_scope, &attribute);
                    let count = attribute_facts
                        .entry(attribute.subject.id().clone())
                        .or_default();
                    *count = count.saturating_add(1);
                } else {
                    attributes_withheld_by_predicate_governance =
                        attributes_withheld_by_predicate_governance.saturating_add(1);
                }
            }
            if last_page {
                break;
            }
        }

        let mut entity_events = BTreeMap::<RecordId, usize>::new();
        let mut event_cursor = None;
        let mut events_scanned = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .events
                .list(space_id, page_limit, event_cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            events_scanned = events_scanned.saturating_add(page.len());
            event_cursor = page.last().map(|event| event.base.id.clone());
            let last_page = page.len() < page_limit;
            for event in page {
                hash_wiki_curation_input(
                    &mut fingerprint,
                    &[serde_json::to_string(&event).map_err(|error| {
                        PipelineError::ValidationError(format!(
                            "Could not fingerprint event: {error}"
                        ))
                    })?],
                );
                if event.base.security != first.base.security
                    || event.status != EventStatus::Approved
                {
                    continue;
                }
                merge_record_subject_scope(&mut subject_scope, &event);
                for subject in &event.base.subjects {
                    if by_id.contains_key(subject.id()) {
                        let count = entity_events.entry(subject.id().clone()).or_default();
                        *count = count.saturating_add(1);
                        let semantic_time = event
                            .time_end
                            .or(event.time_start)
                            .unwrap_or(event.base.updated_at);
                        earliest_evidence_at
                            .entry(subject.id().clone())
                            .and_modify(|current| *current = (*current).min(semantic_time))
                            .or_insert(semantic_time);
                        latest_evidence_at
                            .entry(subject.id().clone())
                            .and_modify(|current| *current = (*current).max(semantic_time))
                            .or_insert(semantic_time);
                    }
                }
            }
            if last_page {
                break;
            }
        }

        let mut knowledge_issues = BTreeMap::<RecordId, usize>::new();
        let mut truth_slot_cursor = None;
        let mut truth_slots_scanned = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .truth_slots
                .list(space_id, page_limit, truth_slot_cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            truth_slots_scanned = truth_slots_scanned.saturating_add(page.len());
            truth_slot_cursor = page.last().map(|slot| slot.base.id.clone());
            let last_page = page.len() < page_limit;
            for slot in page {
                hash_wiki_curation_input(
                    &mut fingerprint,
                    &[serde_json::to_string(&slot).map_err(|error| {
                        PipelineError::ValidationError(format!(
                            "Could not fingerprint truth slot: {error}"
                        ))
                    })?],
                );
                if slot.base.security != first.base.security
                    || (!slot.stale && slot.conflict_group.is_none())
                {
                    continue;
                }
                merge_record_subject_scope(&mut subject_scope, &slot);
                if let Ok(entity_id) = slot.subject_entity.parse::<RecordId>() {
                    if by_id.contains_key(&entity_id) {
                        let count = knowledge_issues.entry(entity_id).or_default();
                        *count = count.saturating_add(1);
                    }
                }
            }
            if last_page {
                break;
            }
        }

        let mut inventory = by_id
            .values()
            .copied()
            .map(|entity| WikiPlanningEntity {
                entity_id: entity.base.id.to_string(),
                entity_type: entity.entity_type.to_string(),
                display_name: entity.display_name.clone(),
                summary: entity.summary.clone(),
                graph_degree: graph_degree.remove(&entity.base.id).unwrap_or_default(),
                evidence_assertions: evidence_assertions
                    .remove(&entity.base.id)
                    .unwrap_or_default(),
                attribute_facts: attribute_facts.remove(&entity.base.id).unwrap_or_default(),
                events: entity_events.remove(&entity.base.id).unwrap_or_default(),
                knowledge_issues: knowledge_issues.remove(&entity.base.id).unwrap_or_default(),
                earliest_evidence_at: earliest_evidence_at.remove(&entity.base.id),
                latest_evidence_at: latest_evidence_at.remove(&entity.base.id),
                possible_duplicate_entity_ids: possible_person_duplicates
                    .get(&entity.base.id)
                    .into_iter()
                    .flatten()
                    .map(ToString::to_string)
                    .collect(),
                connections: {
                    let mut entity_connections =
                        connections.remove(&entity.base.id).unwrap_or_default();
                    entity_connections.sort_by(|left, right| {
                        left.predicate
                            .cmp(&right.predicate)
                            .then_with(|| left.display_name.cmp(&right.display_name))
                            .then_with(|| left.entity_id.cmp(&right.entity_id))
                    });
                    entity_connections.dedup_by(|left, right| {
                        left.predicate == right.predicate && left.entity_id == right.entity_id
                    });
                    entity_connections.truncate(MAX_CONNECTIONS_PER_ENTITY);
                    entity_connections
                },
                evidence_connections: {
                    let mut entity_connections = evidence_connections
                        .remove(&entity.base.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|(entity_id, assertion_count)| {
                            by_id
                                .get(&entity_id)
                                .map(|neighbor| WikiPlanningEvidenceConnection {
                                    entity_id: entity_id.to_string(),
                                    display_name: neighbor.display_name.clone(),
                                    assertion_count,
                                })
                        })
                        .collect::<Vec<_>>();
                    entity_connections.sort_by(|left, right| {
                        right
                            .assertion_count
                            .cmp(&left.assertion_count)
                            .then_with(|| left.display_name.cmp(&right.display_name))
                            .then_with(|| left.entity_id.cmp(&right.entity_id))
                    });
                    entity_connections.truncate(MAX_CONNECTIONS_PER_ENTITY);
                    entity_connections
                },
            })
            .collect::<Vec<_>>();
        inventory.retain(|entity| {
            entity
                .summary
                .as_deref()
                .is_some_and(|summary| !summary.trim().is_empty())
                || !entity.connections.is_empty()
                || entity.evidence_assertions > 0
                || entity.attribute_facts > 0
                || entity.events > 0
                || entity.knowledge_issues > 0
        });
        inventory.sort_by(|left, right| {
            right
                .graph_degree
                .cmp(&left.graph_degree)
                .then_with(|| right.evidence_assertions.cmp(&left.evidence_assertions))
                .then_with(|| right.attribute_facts.cmp(&left.attribute_facts))
                .then_with(|| right.events.cmp(&left.events))
                .then_with(|| right.connections.len().cmp(&left.connections.len()))
                .then_with(|| left.display_name.cmp(&right.display_name))
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        let eligible_entities = inventory.len();
        let planner_candidates = inventory.len();
        let planning_partitions = planning_partitions(&inventory).len();
        Ok(PreparedWikiInventory {
            subject_scope: Some(subject_scope),
            entities: inventory,
            receipt: WikiPlanningReceipt {
                entities_scanned: candidates.len(),
                ambiguous_person_groups_withheld: possible_person_duplicate_groups,
                ambiguous_person_entities_withheld: 0,
                relationships_scanned: scanned,
                relationships_withheld_by_predicate_governance,
                assertions_scanned,
                attributes_scanned,
                attributes_withheld_by_predicate_governance,
                events_scanned,
                truth_slots_scanned,
                eligible_entities,
                planner_candidates,
                planning_partitions,
                graph_fingerprint: format!("sha256:{}", hex::encode(fingerprint.finalize())),
            },
        })
    }
}

/// Loads every active semantic entity in a space using stable ID pagination.
/// There is deliberately no caller-tunable scan ceiling: boundedness is
/// applied only after the complete graph has contributed curation signals.
pub async fn load_full_graph_entities(
    storage: &StorageClient,
    space_id: &str,
) -> PipelineResult<Vec<Entity>> {
    let repository = EntityRepository::new(storage.clone());
    let mut entities = Vec::new();
    let mut cursor = None;
    loop {
        let page = repository.list(space_id, 500, cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|entity| entity.base.id.clone());
        let last_page = page.len() < 500;
        entities.extend(page);
        if last_page {
            break;
        }
    }
    Ok(entities)
}

/// Executes one trusted recurring reconciliation over the complete graph.
#[cfg(feature = "meerkat-extraction")]
pub async fn execute_managed_wiki_reconciliation(
    storage: StorageClient,
    space_id: &str,
    payload: &ManagedWikiPayload,
) -> PipelineResult<ReconciledWiki> {
    payload.authority.ensure_active(&storage, space_id).await?;
    if !(5..=10_080).contains(&payload.refresh_interval_minutes) {
        return Err(PipelineError::ValidationError(
            "Managed wiki refresh interval must be 5-10080 minutes".to_string(),
        ));
    }
    let candidates = load_full_graph_entities(&storage, space_id).await?;
    let editorial_config = WikiEditorialConfig {
        writer_model: payload.writer_model.clone(),
        verifier_model: payload.verifier_model.clone(),
        ..WikiEditorialConfig::default()
    };
    WikiGraphPlanner::with_meerkat(storage, editorial_config)
        .reconcile_full_graph_if_changed(
            space_id,
            candidates,
            payload.creation_config(),
            payload.last_graph_fingerprint.as_deref(),
        )
        .await
}

/// Ensures one future automatic reconciliation exists for the exact interval
/// bucket. This is idempotent across retries and process restarts.
pub async fn enqueue_managed_wiki_reconciliation(
    storage: &StorageClient,
    space_id: &str,
    actor: &str,
    payload: &ManagedWikiPayload,
    run_after: chrono::DateTime<Utc>,
) -> PipelineResult<WorkItem> {
    let interval_seconds = i64::from(payload.refresh_interval_minutes).saturating_mul(60);
    enqueue_managed_wiki_reconciliation_scoped(
        storage,
        space_id,
        actor,
        payload,
        run_after,
        "periodic",
        interval_seconds,
    )
    .await
}

/// Schedules the next recurring reconciliation with identity tied to the
/// completed predecessor. This avoids colliding with a cancelled periodic
/// item when a graph-change run finishes inside the same wall-clock bucket.
pub async fn enqueue_managed_wiki_successor(
    storage: &StorageClient,
    space_id: &str,
    actor: &str,
    predecessor_id: &RecordId,
    payload: &ManagedWikiPayload,
    run_after: chrono::DateTime<Utc>,
) -> PipelineResult<WorkItem> {
    let interval_seconds = i64::from(payload.refresh_interval_minutes).saturating_mul(60);
    let schedule_scope = format!("successor:{predecessor_id}");
    enqueue_managed_wiki_reconciliation_scoped(
        storage,
        space_id,
        actor,
        payload,
        run_after,
        &schedule_scope,
        interval_seconds,
    )
    .await
}

/// Debounces a semantic graph change into the existing managed wiki chain.
/// The replacement cancels older queued periodic work, while a running
/// reconciliation retains its lease and the replacement waits its turn.
pub async fn enqueue_managed_wiki_graph_change(
    storage: &StorageClient,
    space_id: &str,
    actor: &str,
    change_observed_at: chrono::DateTime<Utc>,
    run_after: chrono::DateTime<Utc>,
) -> PipelineResult<Option<WorkItem>> {
    const CHANGE_BUCKET_SECONDS: i64 = 300;
    let repository = WorkItemRepository::new(storage.clone());
    let Some(existing) = repository.find_latest_managed_wiki_work(space_id).await? else {
        return Ok(None);
    };
    if change_observed_at <= existing.base.created_at {
        return Ok(None);
    }
    let mut payload =
        serde_json::from_value::<ManagedWikiPayload>(existing.payload).map_err(|error| {
            PipelineError::ValidationError(format!(
                "Managed wiki work item carries invalid configuration: {error}"
            ))
        })?;
    payload.authority.validate(space_id)?;
    if !repository
        .managed_wiki_grant_is_active(space_id, payload.authority.grant_id())
        .await?
    {
        return Ok(None);
    }
    // The outbox is authoritative that some reader-relevant graph table
    // changed. Clear the periodic cache key so records not represented in the
    // curation inventory (for example events or attributes) cannot be skipped.
    payload.last_graph_fingerprint = None;
    let replacement = enqueue_managed_wiki_reconciliation_scoped(
        storage,
        space_id,
        actor,
        &payload,
        run_after,
        "graph_change",
        CHANGE_BUCKET_SECONDS,
    )
    .await?;
    repository
        .cancel_queued_managed_wiki_except(
            space_id,
            payload.authority.grant_id(),
            &replacement.base.id,
        )
        .await?;
    Ok(Some(replacement))
}

async fn enqueue_managed_wiki_reconciliation_scoped(
    storage: &StorageClient,
    space_id: &str,
    actor: &str,
    payload: &ManagedWikiPayload,
    run_after: chrono::DateTime<Utc>,
    schedule_scope: &str,
    bucket_seconds: i64,
) -> PipelineResult<WorkItem> {
    payload.authority.ensure_active(storage, space_id).await?;
    if !(5..=10_080).contains(&payload.refresh_interval_minutes) {
        return Err(PipelineError::ValidationError(
            "Managed wiki refresh interval must be 5-10080 minutes".to_string(),
        ));
    }
    validate_creation_config(&payload.creation_config())?;
    let payload_json = serde_json::to_value(payload).map_err(|error| {
        PipelineError::ValidationError(format!(
            "Could not serialize managed wiki configuration: {error}"
        ))
    })?;
    let payload_hash = hex::encode(Sha256::digest(serde_json::to_vec(&payload_json).map_err(
        |error| {
            PipelineError::ValidationError(format!(
                "Could not fingerprint managed wiki configuration: {error}"
            ))
        },
    )?));
    let bucket = run_after.timestamp().div_euclid(bucket_seconds.max(1));
    let base_key =
        format!("{RECONCILE_LIVING_WIKI_KIND}:{schedule_scope}:{space_id}:{payload_hash}:{bucket}");
    let mut idempotency_key = base_key.clone();
    let repository = WorkItemRepository::new(storage.clone());
    while let Some(existing) = repository
        .find_by_idempotency_key(space_id, &idempotency_key)
        .await?
    {
        if schedule_scope != "graph_change"
            || matches!(
                existing.status,
                WorkItemStatus::Runnable | WorkItemStatus::Pending
            )
        {
            return Ok(existing);
        }
        // A running or completed job cannot absorb a newly observed change.
        // Keep retries stable while allocating a fresh job in the same bucket.
        idempotency_key = format!("{base_key}:after:{}", existing.base.id);
    }
    repository
        .create_with_managed_wiki_grant(
            WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                kind: RECONCILE_LIVING_WIKI_KIND.to_string(),
                payload: payload_json,
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key,
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: Some(run_after),
                last_error: None,
            },
            payload.authority.grant_id(),
        )
        .await
        .map_err(PipelineError::StorageError)
}

#[derive(Debug, Clone, Default)]
struct PreparedWikiInventory {
    subject_scope: Option<BaseFields>,
    entities: Vec<WikiPlanningEntity>,
    receipt: WikiPlanningReceipt,
}

fn hash_wiki_curation_input(hasher: &mut Sha256, fields: &[String]) {
    for field in fields {
        hasher.update(field.len().to_string().as_bytes());
        hasher.update([0]);
        hasher.update(field.as_bytes());
        hasher.update([0]);
    }
}

fn hash_wiki_editorial_contract(hasher: &mut Sha256) {
    hash_wiki_curation_input(
        hasher,
        &[
            ARTICLE_COMPILER_VERSION.to_string(),
            WRITER_PROMPT_VERSION.to_string(),
            VERIFIER_PROMPT_VERSION.to_string(),
            PLANNER_PROMPT_VERSION.to_string(),
            ORGANIZER_PROMPT_VERSION.to_string(),
            MAX_WIKI_PLANNING_PARTITION_ENTITIES.to_string(),
            FULL_WIKI_STRUCTURE_MAX_CONTEXT_CHARS.to_string(),
            FULL_WIKI_STRUCTURE_MAX_OUTPUT_TOKENS.to_string(),
            FULL_WIKI_ORGANIZER_MIN_TIMEOUT_SECS.to_string(),
        ],
    );
}

fn wiki_article_concurrency() -> usize {
    bounded_wiki_article_concurrency(
        std::env::var("ELEPHANT_WIKI_ARTICLE_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WIKI_ARTICLE_CONCURRENCY),
    )
}

fn bounded_wiki_article_concurrency(requested: usize) -> usize {
    requested.clamp(1, MAX_WIKI_ARTICLE_CONCURRENCY)
}

fn wiki_planner_concurrency() -> usize {
    std::env::var("ELEPHANT_WIKI_PLANNER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .clamp(1, 8)
}

fn possible_person_duplicate_records(
    candidates: &[Entity],
) -> (BTreeMap<RecordId, Vec<RecordId>>, usize) {
    let people = candidates
        .iter()
        .filter(|entity| entity.entity_type.as_str() == "person")
        .filter_map(|entity| {
            let tokens = person_name_tokens(&entity.display_name);
            (!tokens.is_empty()).then_some((entity, tokens))
        })
        .collect::<Vec<_>>();
    let mut adjacency = vec![Vec::<usize>::new(); people.len()];
    for left in 0..people.len() {
        for right in (left + 1)..people.len() {
            if person_names_may_duplicate(&people[left].1, &people[right].1) {
                adjacency[left].push(right);
                adjacency[right].push(left);
            }
        }
    }

    let mut visited = vec![false; people.len()];
    let mut possible_duplicates = BTreeMap::<RecordId, Vec<RecordId>>::new();
    let mut groups = 0_usize;
    for start in 0..people.len() {
        if visited[start] || adjacency[start].is_empty() {
            continue;
        }
        groups = groups.saturating_add(1);
        let mut pending = vec![start];
        let mut group = Vec::new();
        visited[start] = true;
        while let Some(index) = pending.pop() {
            group.push(people[index].0.base.id.clone());
            for neighbor in &adjacency[index] {
                if !visited[*neighbor] {
                    visited[*neighbor] = true;
                    pending.push(*neighbor);
                }
            }
        }
        for entity_id in &group {
            possible_duplicates.insert(
                entity_id.clone(),
                group
                    .iter()
                    .filter(|candidate| *candidate != entity_id)
                    .cloned()
                    .collect(),
            );
        }
    }
    (possible_duplicates, groups)
}

fn person_name_tokens(name: &str) -> Vec<String> {
    name.to_lowercase()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn person_names_may_duplicate(left: &[String], right: &[String]) -> bool {
    if left == right {
        return true;
    }
    if left.len() == 1 && right.len() >= 2 {
        return left[0] == right[0];
    }
    if right.len() == 1 && left.len() >= 2 {
        return right[0] == left[0];
    }
    left.len() >= 2
        && right.len() >= 2
        && left.last() == right.last()
        && given_names_compatible(&left[0], &right[0])
}

fn given_names_compatible(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let (shorter, longer) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    shorter.chars().count() >= 3 && longer.starts_with(shorter)
}

fn allocate_article_quotas(
    eligible_by_security: &BTreeMap<String, usize>,
    max_articles: usize,
) -> BTreeMap<String, usize> {
    let mut quotas = eligible_by_security
        .iter()
        .filter(|(_, eligible)| **eligible > 0)
        .map(|(key, eligible)| {
            (
                key.clone(),
                eligible
                    .div_ceil(MAX_WIKI_PLANNING_PARTITION_ENTITIES)
                    .max(1),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let allocated = quotas.values().copied().sum::<usize>();
    for _ in allocated..max_articles {
        let next = eligible_by_security
            .iter()
            .filter(|(key, eligible)| quotas.get(*key).copied().unwrap_or_default() < **eligible)
            .max_by(|(left_key, left_eligible), (right_key, right_eligible)| {
                let left_quota = quotas.get(*left_key).copied().unwrap_or_default();
                let right_quota = quotas.get(*right_key).copied().unwrap_or_default();
                let left_score = u128::try_from(**left_eligible)
                    .unwrap_or(u128::MAX)
                    .saturating_mul(
                        u128::try_from(right_quota.saturating_add(1)).unwrap_or(u128::MAX),
                    );
                let right_score = u128::try_from(**right_eligible)
                    .unwrap_or(u128::MAX)
                    .saturating_mul(
                        u128::try_from(left_quota.saturating_add(1)).unwrap_or(u128::MAX),
                    );
                left_score
                    .cmp(&right_score)
                    .then_with(|| right_key.cmp(left_key))
            })
            .map(|(key, _)| key.clone());
        let Some(next) = next else {
            break;
        };
        if let Some(quota) = quotas.get_mut(&next) {
            *quota = quota.saturating_add(1);
        }
    }
    quotas.retain(|_, quota| *quota > 0);
    quotas
}

fn planning_partitions(inventory: &[WikiPlanningEntity]) -> Vec<Vec<WikiPlanningEntity>> {
    const MIN_PACKED_ENTITIES: usize = 12;

    let rank = inventory
        .iter()
        .enumerate()
        .map(|(index, entity)| (entity.entity_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let by_id = inventory
        .iter()
        .map(|entity| (entity.entity_id.as_str(), entity))
        .collect::<BTreeMap<_, _>>();
    let mut remaining = inventory
        .iter()
        .map(|entity| entity.entity_id.clone())
        .collect::<BTreeSet<_>>();
    let mut partitions = Vec::new();
    while let Some(seed) = inventory
        .iter()
        .find(|entity| remaining.contains(&entity.entity_id))
    {
        let seed_type = seed.entity_type.clone();
        let mut queue = VecDeque::from([seed.entity_id.clone()]);
        let mut queued = BTreeSet::from([seed.entity_id.clone()]);
        let mut partition = Vec::new();
        while partition.len() < MAX_WIKI_PLANNING_PARTITION_ENTITIES {
            let Some(entity_id) = queue.pop_front() else {
                break;
            };
            if !remaining.remove(&entity_id) {
                continue;
            }
            let Some(entity) = by_id.get(entity_id.as_str()) else {
                continue;
            };
            partition.push((*entity).clone());
            let mut neighbors = entity
                .connections
                .iter()
                .map(|connection| connection.entity_id.as_str())
                .chain(
                    entity
                        .evidence_connections
                        .iter()
                        .map(|connection| connection.entity_id.as_str()),
                )
                .chain(
                    entity
                        .possible_duplicate_entity_ids
                        .iter()
                        .map(String::as_str),
                )
                .filter(|neighbor| remaining.contains(*neighbor))
                .collect::<Vec<_>>();
            neighbors.sort_by_key(|neighbor| rank.get(neighbor).copied().unwrap_or(usize::MAX));
            neighbors.dedup();
            for neighbor in neighbors {
                if queued.insert(neighbor.to_string()) {
                    queue.push_back(neighbor.to_string());
                }
            }
        }
        if partition.len() < MIN_PACKED_ENTITIES {
            let fillers = inventory
                .iter()
                .filter(|entity| {
                    entity.entity_type == seed_type && remaining.contains(&entity.entity_id)
                })
                .map(|entity| entity.entity_id.clone())
                .take(MAX_WIKI_PLANNING_PARTITION_ENTITIES.saturating_sub(partition.len()))
                .collect::<Vec<_>>();
            for entity_id in fillers {
                if remaining.remove(&entity_id) {
                    if let Some(entity) = by_id.get(entity_id.as_str()) {
                        partition.push((*entity).clone());
                    }
                }
            }
        }
        let partition_ids = partition
            .iter()
            .map(|entity| entity.entity_id.clone())
            .collect::<BTreeSet<_>>();
        for entity in &mut partition {
            entity
                .possible_duplicate_entity_ids
                .retain(|candidate| partition_ids.contains(candidate));
        }
        partitions.push(partition);
    }
    partitions
}

fn resolve_article_budget(
    requested: usize,
    eligible_entities: usize,
    partition_count: usize,
) -> PipelineResult<usize> {
    const MAX_WIKI_ARTICLES: usize = 2_000;
    if eligible_entities == 0 {
        return Ok(0);
    }
    if requested > 0 {
        let minimum = eligible_entities.saturating_mul(7).div_ceil(10);
        if requested < minimum {
            return Err(PipelineError::ValidationError(format!(
                "Explicit wiki article cap {requested} would discard most of the {eligible_entities} eligible subjects; use automatic budgeting or a cap of at least {minimum}"
            )));
        }
        return Ok(requested.min(eligible_entities));
    }
    let _ = partition_count;
    Ok(eligible_entities.min(MAX_WIKI_ARTICLES))
}

fn allocate_partition_quotas(
    partitions: &[Vec<WikiPlanningEntity>],
    article_budget: usize,
) -> Vec<usize> {
    let mut quotas = vec![0_usize; partitions.len()];
    let mut remaining = article_budget;
    for (index, partition) in partitions.iter().enumerate() {
        if remaining == 0 {
            break;
        }
        if !partition.is_empty() {
            quotas[index] = 1;
            remaining = remaining.saturating_sub(1);
        }
    }
    while remaining > 0 {
        let next = partitions
            .iter()
            .enumerate()
            .filter(|(index, partition)| quotas[*index] < partition.len())
            .max_by(|(left_index, left), (right_index, right)| {
                let left_score = left
                    .len()
                    .saturating_mul(quotas[*right_index].saturating_add(1));
                let right_score = right
                    .len()
                    .saturating_mul(quotas[*left_index].saturating_add(1));
                left_score
                    .cmp(&right_score)
                    .then_with(|| right_index.cmp(left_index))
            })
            .map(|(index, _)| index);
        let Some(next) = next else {
            break;
        };
        quotas[next] = quotas[next].saturating_add(1);
        remaining = remaining.saturating_sub(1);
    }
    quotas
}

fn organizer_inventory_json(
    plan: &WikiPlan,
    inventory: &[WikiPlanningEntity],
) -> PipelineResult<String> {
    let by_id = inventory
        .iter()
        .map(|entity| (entity.entity_id.as_str(), entity))
        .collect::<BTreeMap<_, _>>();
    let rows = plan
        .articles
        .iter()
        .map(|article| {
            let entity = by_id.get(article.entity_id.as_str()).ok_or_else(|| {
                PipelineError::ValidationError(format!(
                    "Organizer input references unknown entity {}",
                    article.entity_id
                ))
            })?;
            Ok(json!([
                article.entity_id,
                article.title,
                entity.entity_type,
                article.importance,
                article.page_role,
                article.temporal_relevance,
                entity.latest_evidence_at,
            ]))
        })
        .collect::<PipelineResult<Vec<_>>>()?;
    let payload = json!({
        "columns": [
            "entity_id",
            "article_title",
            "entity_type",
            "importance",
            "page_role",
            "temporal_relevance",
            "latest_evidence_at"
        ],
        "rows": rows,
    });
    serde_json::to_string(&payload).map_err(|error| {
        PipelineError::ValidationError(format!(
            "Could not serialize wiki organizer inventory: {error}"
        ))
    })
}

fn organizer_taxonomy_json(organization: &WikiOrganization) -> PipelineResult<String> {
    let value = json!({
        "language": organization.language,
        "title": organization.title,
        "purpose": organization.purpose,
        "featured_entity_ids": organization.featured_entity_ids,
        "sections": organization.sections.iter().map(|section| json!({
            "id": section.id,
            "title": section.title,
            "description": section.description,
            "representative_entity_ids": section.article_entity_ids,
        })).collect::<Vec<_>>(),
    });
    serde_json::to_string(&value).map_err(|error| {
        PipelineError::ValidationError(format!("Could not serialize wiki taxonomy: {error}"))
    })
}

fn validate_wiki_taxonomy(organization: &WikiOrganization, plan: &WikiPlan) -> PipelineResult<()> {
    let minimum_sections = if plan.articles.len() > 60 { 3 } else { 1 };
    if organization.language != "en"
        || organization.title.trim().is_empty()
        || organization.title.len() > 200
        || organization.title.contains(['\n', '\r', '`', '<', '>'])
        || organization.purpose.trim().is_empty()
        || organization.purpose.len() > 1_000
        || organization.sections.len() < minimum_sections
        || organization.sections.len() > 24
    {
        return Err(PipelineError::ValidationError(format!(
            "Wiki taxonomy requires an English title, purpose, and {minimum_sections}-24 semantic sections"
        )));
    }
    let selected = plan
        .articles
        .iter()
        .map(|article| article.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    let archive = plan
        .articles
        .iter()
        .filter(|article| article.page_role == "archive")
        .map(|article| article.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    let featured = organization
        .featured_entity_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if featured.is_empty()
        || featured.len() != organization.featured_entity_ids.len()
        || featured.len() > 48
        || featured
            .iter()
            .any(|entity_id| !selected.contains(entity_id) || archive.contains(entity_id))
    {
        return Err(PipelineError::ValidationError(
            "Wiki taxonomy requires 1-48 unique, known, non-archive featured articles".to_string(),
        ));
    }
    let mut section_ids = BTreeSet::new();
    let mut representatives = BTreeSet::new();
    for section in &organization.sections {
        if section.id.trim().is_empty()
            || section.id.len() > 100
            || section.id.contains(['\n', '\r', '`', '<', '>'])
            || section.title.trim().is_empty()
            || section.title.len() > 200
            || section.title.contains(['\n', '\r', '`', '<', '>'])
            || section.description.trim().is_empty()
            || section.description.len() > 500
            || section.description.contains(['\n', '\r', '`', '<', '>'])
            || section.article_entity_ids.is_empty()
            || !section_ids.insert(section.id.as_str())
        {
            return Err(PipelineError::ValidationError(
                "Wiki taxonomy contains an invalid semantic section".to_string(),
            ));
        }
        if section.article_entity_ids.iter().any(|entity_id| {
            !selected.contains(entity_id.as_str()) || !representatives.insert(entity_id.as_str())
        }) {
            return Err(PipelineError::ValidationError(
                "Wiki taxonomy representatives must be unique selected article IDs".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_wiki_assignment(
    assignment: &WikiOrganization,
    partition: &WikiPlan,
    allowed_section_ids: &BTreeSet<String>,
) -> PipelineResult<()> {
    let expected = partition
        .articles
        .iter()
        .map(|article| article.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut section_ids = BTreeSet::new();
    for section in &assignment.sections {
        if section.article_entity_ids.is_empty()
            || !allowed_section_ids.contains(&section.id)
            || !section_ids.insert(section.id.as_str())
        {
            return Err(PipelineError::ValidationError(
                "Wiki assignment must use unique non-empty sections from the supplied taxonomy"
                    .to_string(),
            ));
        }
        for entity_id in &section.article_entity_ids {
            if !expected.contains(entity_id.as_str()) || !actual.insert(entity_id.as_str()) {
                return Err(PipelineError::ValidationError(format!(
                    "Wiki assignment contains an unknown or duplicate article {entity_id}"
                )));
            }
        }
    }
    if actual != expected {
        return Err(PipelineError::ValidationError(
            "Wiki assignment must place every supplied article exactly once".to_string(),
        ));
    }
    Ok(())
}

fn stabilize_wiki_organization(
    organization: &mut WikiOrganization,
    plan: &WikiPlan,
    section_anchors: &BTreeMap<String, String>,
) -> PipelineResult<()> {
    for target in 0..organization.sections.len() {
        if !organization.sections[target].article_entity_ids.is_empty() {
            continue;
        }
        let anchor = section_anchors
            .get(&organization.sections[target].id)
            .ok_or_else(|| {
                PipelineError::ValidationError(format!(
                    "Wiki taxonomy section {} has no representative anchor",
                    organization.sections[target].id
                ))
            })?;
        let source = organization
            .sections
            .iter()
            .position(|section| {
                section.article_entity_ids.len() > 1
                    && section
                        .article_entity_ids
                        .iter()
                        .any(|entity_id| entity_id == anchor)
            })
            .ok_or_else(|| {
                PipelineError::ValidationError(format!(
                    "Wiki assignment left section {} empty and could not restore its LLM-selected representative",
                    organization.sections[target].id
                ))
            })?;
        let position = organization.sections[source]
            .article_entity_ids
            .iter()
            .position(|entity_id| entity_id == anchor)
            .ok_or_else(|| PipelineError::NotFound(anchor.clone()))?;
        let entity_id = organization.sections[source]
            .article_entity_ids
            .remove(position);
        organization.sections[target]
            .article_entity_ids
            .push(entity_id);
    }

    let by_id = plan
        .articles
        .iter()
        .enumerate()
        .map(|(index, article)| (article.entity_id.as_str(), (index, article)))
        .collect::<BTreeMap<_, _>>();
    let featured = organization
        .featured_entity_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for section in &mut organization.sections {
        section.article_entity_ids.sort_by(|left, right| {
            let (left_index, left_article) = by_id[left.as_str()];
            let (right_index, right_article) = by_id[right.as_str()];
            featured
                .contains(right.as_str())
                .cmp(&featured.contains(left.as_str()))
                .then_with(|| right_article.importance.cmp(&left_article.importance))
                .then_with(|| {
                    temporal_relevance_rank(&left_article.temporal_relevance)
                        .cmp(&temporal_relevance_rank(&right_article.temporal_relevance))
                })
                .then_with(|| left_index.cmp(&right_index))
        });
    }
    if organization.sections.first().is_some_and(|section| {
        !section
            .article_entity_ids
            .iter()
            .any(|entity_id| featured.contains(entity_id.as_str()))
    }) {
        let featured_section = organization
            .sections
            .iter()
            .position(|section| {
                section
                    .article_entity_ids
                    .iter()
                    .any(|entity_id| featured.contains(entity_id.as_str()))
            })
            .ok_or_else(|| {
                PipelineError::ValidationError(
                    "Wiki assignment did not place any featured article".to_string(),
                )
            })?;
        let section = organization.sections.remove(featured_section);
        organization.sections.insert(0, section);
    }
    Ok(())
}

fn temporal_relevance_rank(value: &str) -> u8 {
    match value {
        "current" => 0,
        "enduring" => 1,
        "mixed" => 2,
        "historical" => 3,
        _ => 4,
    }
}

fn validate_wiki_organization(
    organization: &WikiOrganization,
    plan: &WikiPlan,
) -> PipelineResult<()> {
    if organization.language != "en"
        || organization.title.trim().is_empty()
        || organization.title.len() > 200
        || organization.title.contains(['\n', '\r', '`', '<', '>'])
        || organization.sections.is_empty()
        || organization.sections.len() > 24
    {
        return Err(PipelineError::ValidationError(
            "Wiki organization requires an English title and 1-24 sections".to_string(),
        ));
    }
    let selected = plan
        .articles
        .iter()
        .map(|article| article.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    let featured = organization
        .featured_entity_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if organization.purpose.trim().is_empty()
        || organization.purpose.len() > 1_000
        || featured.is_empty()
        || featured.len() != organization.featured_entity_ids.len()
        || featured.len() > 48
        || featured
            .iter()
            .any(|entity_id| !selected.contains(entity_id))
    {
        return Err(PipelineError::ValidationError(
            "Wiki organization requires an inferred purpose and 1-48 unique featured articles"
                .to_string(),
        ));
    }
    if plan.articles.iter().any(|article| {
        featured.contains(article.entity_id.as_str()) && article.page_role == "archive"
    }) || organization.sections.first().is_none_or(|section| {
        !section
            .article_entity_ids
            .iter()
            .any(|entity_id| featured.contains(entity_id.as_str()))
    }) {
        return Err(PipelineError::ValidationError(
            "Featured pages cannot be archive pages and the first reader lens must contain a featured page"
                .to_string(),
        ));
    }
    let mut section_ids = BTreeSet::new();
    let mut assigned = BTreeSet::new();
    for section in &organization.sections {
        if section.id.trim().is_empty()
            || section.id.len() > 100
            || section.id.contains(['\n', '\r', '`', '<', '>'])
            || section.title.trim().is_empty()
            || section.title.len() > 200
            || section.title.contains(['\n', '\r', '`', '<', '>'])
            || section.description.trim().is_empty()
            || section.description.len() > 500
            || section.description.contains(['\n', '\r', '`', '<', '>'])
            || section.article_entity_ids.is_empty()
            || !section_ids.insert(section.id.as_str())
        {
            return Err(PipelineError::ValidationError(
                "Wiki organization contains an invalid section".to_string(),
            ));
        }
        for entity_id in &section.article_entity_ids {
            if !selected.contains(entity_id.as_str()) || !assigned.insert(entity_id.as_str()) {
                return Err(PipelineError::ValidationError(format!(
                    "Wiki organization contains an unknown or duplicate article {entity_id}"
                )));
            }
        }
    }
    if assigned != selected {
        return Err(PipelineError::ValidationError(
            "Wiki organization must assign every selected article exactly once".to_string(),
        ));
    }
    Ok(())
}

fn organization_navigation(
    organization: &WikiOrganization,
    plan: &WikiPlan,
) -> PipelineResult<BTreeMap<String, KnowledgeViewNavigation>> {
    let articles = plan
        .articles
        .iter()
        .map(|article| (article.entity_id.as_str(), article))
        .collect::<BTreeMap<_, _>>();
    let mut navigation = BTreeMap::new();
    for (section_index, section) in organization.sections.iter().enumerate() {
        let section_order = u16::try_from(section_index).map_err(|error| {
            PipelineError::ValidationError(format!("Wiki section order overflow: {error}"))
        })?;
        for (article_index, entity_id) in section.article_entity_ids.iter().enumerate() {
            let article_order = u16::try_from(article_index).map_err(|error| {
                PipelineError::ValidationError(format!("Wiki article order overflow: {error}"))
            })?;
            let article = articles.get(entity_id.as_str()).ok_or_else(|| {
                PipelineError::ValidationError(format!(
                    "Wiki organization references unknown article {entity_id}"
                ))
            })?;
            navigation.insert(
                entity_id.clone(),
                KnowledgeViewNavigation {
                    wiki_title: organization.title.trim().to_string(),
                    wiki_purpose: organization.purpose.trim().to_string(),
                    section_id: section.id.trim().to_string(),
                    section_title: section.title.trim().to_string(),
                    section_order,
                    article_order,
                    section_description: section.description.trim().to_string(),
                    article_importance: article.importance,
                    page_role: article.page_role.clone(),
                    featured: organization
                        .featured_entity_ids
                        .iter()
                        .any(|featured| featured == entity_id),
                },
            );
        }
    }
    Ok(navigation)
}

#[cfg(feature = "meerkat-extraction")]
impl WikiGraphPlanner {
    /// Creates the production graph-to-wiki workflow using Meerkat model
    /// resolution for planning, writing, and verification.
    #[must_use]
    pub fn with_meerkat(storage: StorageClient, config: WikiEditorialConfig) -> Self {
        let llm = Arc::new(MeerkatWikiEditorialLlm::new(config.clone()));
        Self::new(storage, llm, config)
    }
}

/// Compiles bounded graph context into LLM-authored, verified article prose.
#[derive(Clone)]
pub struct WikiEditorialCompiler {
    graph: GraphViewCompiler,
    definitions: KnowledgeViewDefinitionRepository,
    views: KnowledgeViewRepository,
    revisions: DocRevisionRepository,
    nodes: DocNodeRepository,
    assertions: AssertionRepository,
    attributes: AttributeRepository,
    events: EventRepository,
    relations: RelRepository,
    predicates: PredicateRegistryRepository,
    truth_slots: TruthSlotRepository,
    llm: Arc<dyn WikiEditorialLlm>,
    config: WikiEditorialConfig,
}

impl std::fmt::Debug for WikiEditorialCompiler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WikiEditorialCompiler")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl WikiEditorialCompiler {
    /// Creates an editorial compiler with an injectable writer/verifier.
    #[must_use]
    pub fn new(
        storage: StorageClient,
        llm: Arc<dyn WikiEditorialLlm>,
        config: WikiEditorialConfig,
    ) -> Self {
        Self {
            graph: GraphViewCompiler::new(storage.clone()),
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            views: KnowledgeViewRepository::new(storage.clone()),
            revisions: DocRevisionRepository::new(storage.clone()),
            nodes: DocNodeRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            events: EventRepository::new(storage.clone()),
            relations: RelRepository::new(storage.clone()),
            predicates: PredicateRegistryRepository::new(storage.clone()),
            truth_slots: TruthSlotRepository::new(storage.clone()),
            llm,
            config,
        }
    }

    /// Runs the writer and independent verifier, then persists an article.
    ///
    /// `publish` requires an explicitly approved context definition and a
    /// clean bounded context. Draft compilation still runs the same verifier.
    pub async fn compile(
        &self,
        space_id: &str,
        definition_id: &RecordId,
        publish: bool,
    ) -> PipelineResult<CompiledWikiArticle> {
        let definition = self
            .definitions
            .get(space_id, definition_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(definition_id.to_string()))?;
        if publish && definition.publication_status != KnowledgeViewPublicationStatus::Published {
            return Err(PipelineError::ValidationError(
                "Article publication requires an explicitly published context plan".to_string(),
            ));
        }

        let mut context = self.graph.compile(space_id, definition_id, false).await?;
        if publish {
            validate_publishable_context(&context.revision)?;
            self.validate_publishable_predicates(space_id, &context.revision)
                .await?;
        }
        self.augment_with_entity_source_evidence(space_id, &definition, &mut context.revision)
            .await?;
        self.augment_with_root_semantic_records(space_id, &definition, &mut context.revision)
            .await?;
        let catalog = self
            .build_evidence_catalog(space_id, &mut context.revision)
            .await?;
        if catalog.is_empty() {
            return Err(PipelineError::ValidationError(
                "The editorial context contains no supported graph facts or source evidence"
                    .to_string(),
            ));
        }
        let catalog_json = serde_json::to_string_pretty(&catalog).map_err(|error| {
            PipelineError::ValidationError(format!("Could not serialize evidence catalog: {error}"))
        })?;
        if catalog_json.chars().count() > self.config.max_context_chars {
            return Err(PipelineError::BudgetExceeded(format!(
                "Wiki editorial context exceeds {} characters; narrow the definition",
                self.config.max_context_chars
            )));
        }

        let dependency_hash = editorial_dependency_hash(
            &context.revision,
            self.llm.writer_model(),
            self.llm.verifier_model(),
            publish,
        )?;
        let security_fingerprint = security_fingerprint(&context.revision.base.security)?;
        let key = format!("article:{definition_id}:{security_fingerprint}");
        let existing = self.views.find_by_key(space_id, &key).await?;
        if let Some(existing_view) = existing.as_ref() {
            if let Some(current) = self.views.get_current_revision(existing_view).await? {
                if existing_view.state != ProjectionState::Dirty
                    && current.compiler.version == ARTICLE_COMPILER_VERSION
                    && current.compiler.dependency_hash == dependency_hash
                {
                    return Ok(CompiledWikiArticle {
                        view: existing_view.clone(),
                        revision: current,
                        changed: false,
                    });
                }
            }
        }

        let base_writer_prompt = render_writer_prompt(
            &definition.title,
            definition.navigation.as_ref(),
            &catalog_json,
        );
        let mut writer_prompt = base_writer_prompt.clone();
        let max_attempts = self.config.max_editorial_attempts.max(1);
        let mut attempt = 0_usize;
        let draft = loop {
            attempt = attempt.saturating_add(1);
            let candidate = match self.llm.write_article(writer_prompt.clone()).await {
                Ok(candidate) => candidate,
                Err(error) if attempt < max_attempts => {
                    writer_prompt =
                        render_format_recovery_prompt(&base_writer_prompt, attempt, &error);
                    wait_before_wiki_llm_retry(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Err(error) = validate_editorial_draft(&candidate, &catalog) {
                if attempt >= max_attempts {
                    return Err(error);
                }
                writer_prompt = render_repair_prompt(
                    &definition.title,
                    &catalog_json,
                    &candidate,
                    None,
                    &error,
                )?;
                continue;
            }
            let verification_prompt = render_verifier_prompt(&candidate, &catalog_json)?;
            let mut verification = None;
            let mut verification_error = None;
            for verifier_attempt in 1..=3 {
                match self.llm.verify_article(verification_prompt.clone()).await {
                    Ok(candidate) => {
                        verification = Some(candidate);
                        break;
                    }
                    Err(error) => {
                        verification_error = Some(error);
                        if verifier_attempt < 3 {
                            wait_before_wiki_llm_retry(verifier_attempt).await;
                        }
                    }
                }
            }
            let verification = verification.ok_or_else(|| {
                verification_error.unwrap_or_else(|| {
                    PipelineError::ValidationError(
                        "Wiki verifier exhausted its bounded retries".to_string(),
                    )
                })
            })?;
            if let Err(error) = validate_verification(&candidate, &verification) {
                if attempt >= max_attempts {
                    return Err(error);
                }
                writer_prompt = render_repair_prompt(
                    &definition.title,
                    &catalog_json,
                    &candidate,
                    Some(&verification),
                    &error,
                )?;
                continue;
            }
            break candidate;
        };

        let (blocks, citations) = article_blocks(&draft, &context.revision.citations)?;
        let dependencies = article_dependencies(
            &context.view,
            &context.revision,
            self.llm.writer_model(),
            self.llm.verifier_model(),
        );
        let validation = article_validation(&blocks, &citations, &dependencies);
        if !validation.citations_resolve
            || !validation.factual_blocks_cited
            || !validation.statements_supported
            || !validation.prose_is_structured
        {
            return Err(PipelineError::ValidationError(format!(
                "Article validation failed: {}",
                validation.warnings.join("; ")
            )));
        }
        let now = Utc::now();
        let view_id = existing.as_ref().map_or_else(
            || {
                RecordId::new(
                    TableName::KnowledgeView,
                    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()),
                )
            },
            |view| view.base.id.clone(),
        );
        let revision_number = existing
            .as_ref()
            .map_or(1, |view| view.revision_count.saturating_add(1));
        let revision_id = RecordId::new_v7(TableName::KnowledgeViewRevision);
        let mut view_base = existing.as_ref().map_or_else(
            || {
                BaseFields::new(
                    view_id.clone(),
                    space_id.to_string(),
                    ARTICLE_COMPILER_NAME.to_string(),
                )
            },
            |view| view.base.clone(),
        );
        view_base.updated_at = now;
        view_base.deleted_at = None;
        view_base
            .security
            .clone_from(&context.revision.base.security);
        view_base
            .subjects
            .clone_from(&context.revision.base.subjects);
        view_base.subjects_state = context.revision.base.subjects_state;
        view_base.provenance = article_provenance(
            &context.view.base.provenance,
            &context.revision,
            &view_id,
            self.llm.writer_model(),
            self.llm.verifier_model(),
            &dependency_hash,
        );
        let publication_status = if publish {
            KnowledgeViewPublicationStatus::Published
        } else {
            KnowledgeViewPublicationStatus::Draft
        };
        let view = KnowledgeView {
            base: view_base,
            key,
            kind: KnowledgeViewKind::Article,
            surface: KnowledgeViewSurface::Wiki,
            publication_status,
            root_record: definition_id.to_string(),
            title: draft.title.clone(),
            slug: article_slug(&draft.title, definition_id),
            security_fingerprint: security_fingerprint.clone(),
            current_revision: Some(revision_id.to_string()),
            revision_count: revision_number,
            state: context.revision.freshness.state,
            state_reason: context.view.state_reason.clone(),
            published_at: publish.then_some(now),
        };
        let mut revision_base = BaseFields::new(
            revision_id,
            space_id.to_string(),
            ARTICLE_COMPILER_NAME.to_string(),
        );
        revision_base
            .security
            .clone_from(&context.revision.base.security);
        revision_base
            .subjects
            .clone_from(&context.revision.base.subjects);
        revision_base.subjects_state = context.revision.base.subjects_state;
        revision_base.provenance = article_provenance(
            &context.view.base.provenance,
            &context.revision,
            &revision_base.id,
            self.llm.writer_model(),
            self.llm.verifier_model(),
            &dependency_hash,
        );
        let revision = KnowledgeViewRevision {
            base: revision_base,
            view_id: view.base.id.to_string(),
            revision: revision_number,
            kind: KnowledgeViewKind::Article,
            root_record: definition_id.to_string(),
            rendered_markdown: render_article_markdown(&draft.title, &blocks),
            blocks,
            citations,
            dependencies,
            freshness: context.revision.freshness,
            validation,
            compiler: CompilerReceipt {
                compiler: format!(
                    "{ARTICLE_COMPILER_NAME}:writer={}:verifier={}",
                    self.llm.writer_model(),
                    self.llm.verifier_model()
                ),
                version: ARTICLE_COMPILER_VERSION,
                dependency_hash,
                policy_fingerprint: security_fingerprint,
            },
            compiled_at: now,
            supersedes: existing.and_then(|view| view.current_revision),
        };
        let (view, revision) = self.views.publish(view, revision).await?;
        Ok(CompiledWikiArticle {
            view,
            revision,
            changed: true,
        })
    }

    async fn augment_with_entity_source_evidence(
        &self,
        space_id: &str,
        definition: &KnowledgeViewDefinition,
        context: &mut KnowledgeViewRevision,
    ) -> PipelineResult<()> {
        let max_entity_assertions = definition.navigation.as_ref().map_or(36, |navigation| {
            match navigation.article_importance {
                5 => 96,
                4 => 64,
                3 => 48,
                _ => 32,
            }
        });
        let roots = definition_roots(definition);
        if roots.is_empty() {
            return Ok(());
        }
        let mut seen_spans = BTreeSet::new();
        for topic in &roots {
            let assertions = self
                .assertions
                .find_by_resolved_entity(space_id, topic.id(), max_entity_assertions)
                .await?;
            for assertion in assertions {
                if assertion.base.security != definition.base.security
                    || assertion.resolution_status != ResolutionStatus::Resolved
                    || assertion.lifecycle_tier != "active"
                    || assertion
                        .significance
                        .as_deref()
                        .is_some_and(|significance| significance == "skip")
                {
                    continue;
                }
                let revision = self
                    .revisions
                    .get(space_id, assertion.evidence.revision.id())
                    .await?
                    .ok_or_else(|| {
                        PipelineError::NotFound(assertion.evidence.revision.id().to_string())
                    })?;
                let Some(joined_security) =
                    policy_aware_security_join(&context.base.security, &revision.base.security)
                else {
                    continue;
                };
                context.base.security = joined_security;
                merge_record_subject_scope(&mut context.base, &assertion);
                merge_subject_scope(&mut context.base, &revision.base);
                let node = if let Some(node_link) = assertion.evidence.node.as_ref() {
                    self.nodes.get(space_id, node_link.id()).await?
                } else {
                    None
                };
                let mut candidate_span = assertion.evidence.clone();
                if candidate_span.start >= candidate_span.end {
                    let Some(node) = node.as_ref() else {
                        continue;
                    };
                    if node.revision.id() != assertion.evidence.revision.id() {
                        continue;
                    }
                    let Some(joined_security) =
                        policy_aware_security_join(&context.base.security, &node.base.security)
                    else {
                        continue;
                    };
                    context.base.security = joined_security;
                    merge_subject_scope(&mut context.base, &node.base);
                    candidate_span.start = node.span.start;
                    candidate_span.end = node.span.end;
                }
                let span_key = format!(
                    "{}:{}:{}",
                    candidate_span.revision.id(),
                    candidate_span.start,
                    candidate_span.end
                );
                if !seen_spans.insert(span_key) {
                    continue;
                }
                let Some(evidence_span) = reconciled_evidence_span(&revision.text, &candidate_span)
                else {
                    continue;
                };
                if evidence_span.quote_hash != assertion.evidence.quote_hash {
                    context.validation.warnings.push(format!(
                    "Assertion {} had a stale evidence hash; the article citation uses the hash of the immutable revision slice at the recorded offsets",
                    assertion.base.id
                ));
                }
                let citation_id = format!("c{}", context.citations.len().saturating_add(1));
                let source_id = assertion
                    .base
                    .provenance
                    .sources
                    .first()
                    .or_else(|| revision.base.provenance.sources.first())
                    .map(ToString::to_string);
                context.citations.push(KnowledgeCitation {
                    id: citation_id,
                    kind: CitationKind::EvidenceSpan,
                    record_id: assertion.base.id.to_string(),
                    assertion_id: Some(assertion.base.id.to_string()),
                    source_id: source_id.clone(),
                    evidence_span: Some(evidence_span),
                });
                context.dependencies.extend([
                    KnowledgeDependency {
                        record_id: assertion.base.id.to_string(),
                        record_type: "assertion".to_string(),
                        role: "entity_source_evidence".to_string(),
                        observed_updated_at: Some(assertion.base.updated_at),
                    },
                    KnowledgeDependency {
                        record_id: revision.base.id.to_string(),
                        record_type: "doc_revision".to_string(),
                        role: "entity_source_evidence".to_string(),
                        observed_updated_at: Some(revision.base.updated_at),
                    },
                ]);
                if let Some(node) = node {
                    context.dependencies.push(KnowledgeDependency {
                        record_id: node.base.id.to_string(),
                        record_type: "doc_node".to_string(),
                        role: "entity_source_evidence".to_string(),
                        observed_updated_at: Some(node.base.updated_at),
                    });
                }
                if let Some(source_id) = source_id {
                    context.dependencies.push(KnowledgeDependency {
                        record_id: source_id,
                        record_type: "source".to_string(),
                        role: "entity_source_evidence".to_string(),
                        observed_updated_at: None,
                    });
                }
            }
        }
        Ok(())
    }

    async fn augment_with_root_semantic_records(
        &self,
        space_id: &str,
        definition: &KnowledgeViewDefinition,
        context: &mut KnowledgeViewRevision,
    ) -> PipelineResult<()> {
        const MAX_ROOT_ATTRIBUTES: usize = 50;
        const MAX_ROOT_EVENTS: usize = 25;
        let roots = definition_roots(definition);
        if roots.is_empty() {
            return Ok(());
        }

        let mut predicate_publishability = BTreeMap::<String, bool>::new();
        let mut fact_statements = Vec::new();
        let mut root_attributes = Vec::new();
        for topic in &roots {
            root_attributes.extend(
                self.attributes
                    .find_by_subject_entity(space_id, topic.id())
                    .await?,
            );
        }
        for attribute in root_attributes
            .into_iter()
            .filter(|attribute| {
                attribute.base.security == definition.base.security
                    && attribute.status == ClaimStatus::Approved
            })
            .take(MAX_ROOT_ATTRIBUTES)
        {
            let publishable =
                if let Some(value) = predicate_publishability.get(&attribute.attr_type) {
                    *value
                } else {
                    let registry = self
                        .predicates
                        .find_by_predicate_id(space_id, &attribute.attr_type)
                        .await?;
                    let value = registry.as_ref().is_some_and(predicate_is_publishable);
                    predicate_publishability.insert(attribute.attr_type.clone(), value);
                    value
                };
            if !publishable {
                continue;
            }
            merge_record_subject_scope(&mut context.base, &attribute);
            let citation_id = push_context_graph_citation(context, &attribute.base.id);
            context.dependencies.push(KnowledgeDependency {
                record_id: attribute.base.id.to_string(),
                record_type: "attribute".to_string(),
                role: "root_current_fact".to_string(),
                observed_updated_at: Some(attribute.base.updated_at),
            });
            fact_statements.push(KnowledgeStatement {
                id: format!("root-fact-{}", fact_statements.len().saturating_add(1)),
                text: format!(
                    "{} - {}: {}.",
                    definition.title,
                    humanize_predicate(&attribute.attr_type),
                    wiki_literal_text(&attribute.value)
                ),
                derivation: StatementDerivation::Direct,
                verification: StatementVerification::DeterministicRecordRendering,
                citations: vec![citation_id],
            });
        }
        append_context_block(
            context,
            "root-current-facts",
            ViewBlockKind::CurrentFacts,
            "Current facts",
            fact_statements,
        );

        let mut event_statements = Vec::new();
        let mut root_events = Vec::new();
        for topic in &roots {
            root_events.extend(
                self.events
                    .find_by_subject_entity(space_id, topic.id(), MAX_ROOT_EVENTS.saturating_mul(2))
                    .await?,
            );
        }
        for event in root_events
            .into_iter()
            .filter(|event| {
                event.base.security == definition.base.security
                    && event.status == EventStatus::Approved
            })
            .take(MAX_ROOT_EVENTS)
        {
            merge_record_subject_scope(&mut context.base, &event);
            let citation_id = push_context_graph_citation(context, &event.base.id);
            context.dependencies.push(KnowledgeDependency {
                record_id: event.base.id.to_string(),
                record_type: "event".to_string(),
                role: "root_timeline".to_string(),
                observed_updated_at: Some(event.base.updated_at),
            });
            let date = event.time_start.as_ref().map_or_else(
                || "Date unknown".to_string(),
                |value| value.format("%Y-%m-%d").to_string(),
            );
            let summary = event
                .summary
                .as_deref()
                .filter(|summary| !summary.trim().is_empty())
                .map(|summary| format!(" {}", bounded_text(summary, 500)))
                .unwrap_or_default();
            event_statements.push(KnowledgeStatement {
                id: format!("root-event-{}", event_statements.len().saturating_add(1)),
                text: format!("{date}: {}.{summary}", event.title.trim()),
                derivation: StatementDerivation::Direct,
                verification: StatementVerification::DeterministicRecordRendering,
                citations: vec![citation_id],
            });
        }
        append_context_block(
            context,
            "root-timeline",
            ViewBlockKind::Timeline,
            "Timeline",
            event_statements,
        );

        let mut issue_statements = Vec::new();
        let mut root_slots = Vec::new();
        for topic in &roots {
            root_slots.extend(
                self.truth_slots
                    .find_by_subject_entity(space_id, &topic.id().to_string())
                    .await?,
            );
        }
        for slot in root_slots.into_iter().filter(|slot| {
            slot.base.security == definition.base.security
                && (slot.stale || slot.conflict_group.is_some())
        }) {
            merge_record_subject_scope(&mut context.base, &slot);
            let citation_id = push_context_graph_citation(context, &slot.base.id);
            context.dependencies.push(KnowledgeDependency {
                record_id: slot.base.id.to_string(),
                record_type: "truth_slot".to_string(),
                role: "root_knowledge_issue".to_string(),
                observed_updated_at: Some(slot.base.updated_at),
            });
            let state = match (slot.stale, slot.conflict_group.is_some()) {
                (true, true) => "may be stale and has conflicting current information",
                (true, false) => "may be stale",
                (false, true) => "has conflicting current information",
                (false, false) => continue,
            };
            issue_statements.push(KnowledgeStatement {
                id: format!("root-issue-{}", issue_statements.len().saturating_add(1)),
                text: format!(
                    "Elephant's knowledge about {} and {} {}.",
                    definition.title,
                    humanize_predicate(&slot.predicate_id),
                    state
                ),
                derivation: StatementDerivation::Direct,
                verification: StatementVerification::DeterministicRecordRendering,
                citations: vec![citation_id],
            });
        }
        append_context_block(
            context,
            "root-knowledge-issues",
            ViewBlockKind::Conflicts,
            "Uncertainty",
            issue_statements,
        );
        Ok(())
    }

    async fn build_evidence_catalog(
        &self,
        space_id: &str,
        context: &mut KnowledgeViewRevision,
    ) -> PipelineResult<Vec<EditorialEvidenceItem>> {
        let mut canonical = BTreeMap::<String, BTreeSet<String>>::new();
        for statement in context
            .blocks
            .iter()
            .filter(|block| {
                !matches!(
                    block.kind,
                    ViewBlockKind::SelectionReceipt | ViewBlockKind::EvidenceStatus
                )
            })
            .flat_map(|block| block.statements.iter())
        {
            for citation_id in &statement.citations {
                canonical
                    .entry(citation_id.clone())
                    .or_default()
                    .insert(statement.text.clone());
            }
        }
        let mut catalog = Vec::new();
        for citation in context.citations.clone() {
            if citation.kind == CitationKind::ClaimRecord
                || citation.record_id.starts_with("knowledge_view_definition:")
            {
                continue;
            }
            let source_quote = if let Some(span) = citation.evidence_span.as_ref() {
                let revision = self
                    .revisions
                    .get(space_id, span.revision.id())
                    .await?
                    .ok_or_else(|| {
                        PipelineError::NotFound(format!("Evidence revision {}", span.revision.id()))
                    })?;
                context.base.security =
                    policy_aware_security_join(&context.base.security, &revision.base.security)
                        .ok_or_else(|| {
                            PipelineError::ValidationError(
                                "Evidence revision has an incompatible security policy".to_string(),
                            )
                        })?;
                merge_subject_scope(&mut context.base, &revision.base);
                if !context
                    .dependencies
                    .iter()
                    .any(|dependency| dependency.record_id == revision.base.id.to_string())
                {
                    context.dependencies.push(KnowledgeDependency {
                        record_id: revision.base.id.to_string(),
                        record_type: "doc_revision".to_string(),
                        role: "editorial_evidence".to_string(),
                        observed_updated_at: Some(revision.base.updated_at),
                    });
                }
                Some(validated_quote(&revision.text, span)?)
            } else {
                None
            };
            let canonical_statements = canonical
                .remove(&citation.id)
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            if canonical_statements.is_empty() && source_quote.is_none() {
                continue;
            }
            catalog.push(EditorialEvidenceItem {
                citation_id: citation.id.clone(),
                kind: citation.kind,
                record_id: citation.record_id.clone(),
                canonical_statements,
                source_quote,
                source_id: citation.source_id.clone(),
            });
        }
        Ok(catalog)
    }

    async fn validate_publishable_predicates(
        &self,
        space_id: &str,
        context: &KnowledgeViewRevision,
    ) -> PipelineResult<()> {
        let mut problems = BTreeSet::new();
        for dependency in context
            .dependencies
            .iter()
            .filter(|dependency| dependency.record_type == "rel")
        {
            let relation_id = dependency.record_id.parse::<RecordId>().map_err(|error| {
                PipelineError::ValidationError(format!(
                    "Invalid relationship dependency {}: {error}",
                    dependency.record_id
                ))
            })?;
            let relation = self
                .relations
                .get(space_id, &relation_id)
                .await?
                .ok_or_else(|| PipelineError::NotFound(relation_id.to_string()))?;
            let registry = self
                .predicates
                .find_by_predicate_id(space_id, &relation.rel_type)
                .await?;
            if registry
                .as_ref()
                .is_none_or(|predicate| !predicate_is_publishable(predicate))
            {
                problems.insert(relation.rel_type);
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(PipelineError::ValidationError(format!(
                "Article publication blocked by ungoverned or review-pending predicates: {}",
                problems.into_iter().collect::<Vec<_>>().join(", ")
            )))
        }
    }
}

fn validate_creation_config(config: &WikiCreationConfig) -> PipelineResult<()> {
    if config.max_articles > 2_000
        || config.max_related_articles > 24
        || !(1..=4).contains(&config.article_max_hops)
        || !(1..=250).contains(&config.article_max_members)
    {
        return Err(PipelineError::ValidationError(
            "Wiki creation requires an automatic or 1-2000 article budget, at most 24 links, 1-4 hops, and 1-250 context members"
                .to_string(),
        ));
    }
    Ok(())
}

fn augment_plan_navigation(
    plan: &mut WikiPlan,
    inventory: &[WikiPlanningEntity],
    max_related_articles: usize,
) {
    let selected = plan
        .articles
        .iter()
        .map(|article| article.entity_id.clone())
        .collect::<BTreeSet<_>>();
    for article in &mut plan.articles {
        let Some(entity) = inventory
            .iter()
            .find(|entity| entity.entity_id == article.entity_id)
        else {
            continue;
        };
        let candidates = entity
            .connections
            .iter()
            .map(|connection| &connection.entity_id)
            .chain(
                entity
                    .evidence_connections
                    .iter()
                    .map(|connection| &connection.entity_id),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        article.related_entity_ids.retain(|entity_id| {
            entity_id != &article.entity_id
                && selected.contains(entity_id)
                && candidates.contains(entity_id)
                && seen.insert(entity_id.clone())
        });
        article.related_entity_ids.truncate(max_related_articles);
        for entity_id in candidates {
            if article.related_entity_ids.len() >= max_related_articles {
                break;
            }
            if entity_id != article.entity_id
                && selected.contains(&entity_id)
                && seen.insert(entity_id.clone())
            {
                article.related_entity_ids.push(entity_id);
            }
        }
    }
}

fn validate_wiki_plan(
    plan: &WikiPlan,
    inventory: &[WikiPlanningEntity],
    config: &WikiCreationConfig,
) -> PipelineResult<()> {
    if plan.language != "en" {
        return Err(PipelineError::ValidationError(
            "Wiki planner must produce an English plan".to_string(),
        ));
    }
    let article_limit = if config.max_articles == 0 {
        resolve_article_budget(0, inventory.len(), usize::from(!inventory.is_empty()))?
    } else {
        config.max_articles
    };
    if plan.articles.is_empty() || plan.articles.len() > article_limit {
        return Err(PipelineError::ValidationError(format!(
            "Wiki planner must select 1-{} articles",
            article_limit
        )));
    }
    let allowed = inventory
        .iter()
        .map(|entity| entity.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    let selected = plan
        .articles
        .iter()
        .map(|article| article.entity_id.as_str())
        .collect::<BTreeSet<_>>();
    if selected.len() != plan.articles.len()
        || selected
            .iter()
            .any(|entity_id| !allowed.contains(entity_id))
    {
        return Err(PipelineError::ValidationError(
            "Wiki planner selected an unknown or duplicate article entity".to_string(),
        ));
    }
    let minimum_articles = if inventory.len() <= 60 {
        inventory.len().div_ceil(2)
    } else {
        inventory.len().saturating_mul(7).div_ceil(10)
    };
    if plan.articles.len() < minimum_articles {
        return Err(PipelineError::ValidationError(format!(
            "Wiki planner collapsed {} eligible subjects into only {} articles; at least {minimum_articles} are required for full-corpus coverage",
            inventory.len(),
            plan.articles.len(),
        )));
    }
    let possible_duplicates = inventory
        .iter()
        .map(|entity| {
            (
                entity.entity_id.as_str(),
                entity
                    .possible_duplicate_entity_ids
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut accounted = BTreeSet::new();
    for article in &plan.articles {
        if article.title.trim().is_empty()
            || article.title.chars().count() > 200
            || article.title.contains(['\n', '\r', '`', '<', '>'])
            || article.editorial_reason.trim().is_empty()
            || !(1..=5).contains(&article.importance)
            || !matches!(
                article.page_role.as_str(),
                "core" | "overview" | "topic" | "reference" | "archive"
            )
            || !matches!(
                article.temporal_relevance.as_str(),
                "current" | "enduring" | "mixed" | "historical"
            )
        {
            return Err(PipelineError::ValidationError(format!(
                "Planned article {} needs a concise plain-English title and editorial reason",
                article.entity_id
            )));
        }
        if !accounted.insert(article.entity_id.as_str()) {
            return Err(PipelineError::ValidationError(format!(
                "Wiki planner accounted for {} more than once",
                article.entity_id
            )));
        }
        let supporting = article
            .supporting_entity_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if supporting.len() != article.supporting_entity_ids.len()
            || supporting.contains(article.entity_id.as_str())
            || supporting.iter().any(|entity_id| {
                !allowed.contains(entity_id)
                    || !possible_duplicates
                        .get(article.entity_id.as_str())
                        .is_some_and(|candidates| candidates.contains(entity_id))
                    || !accounted.insert(entity_id)
            })
        {
            return Err(PipelineError::ValidationError(format!(
                "Supporting subjects for {} must be unique, unassigned possible identity duplicates",
                article.entity_id
            )));
        }
        let links = article
            .related_entity_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if links.len() != article.related_entity_ids.len()
            || links.len() > config.max_related_articles
            || links.contains(article.entity_id.as_str())
            || links.iter().any(|entity_id| !selected.contains(entity_id))
        {
            return Err(PipelineError::ValidationError(format!(
                "Planned links for {} must be unique targets in the selected article set",
                article.entity_id
            )));
        }
        let available_neighbors = inventory
            .iter()
            .find(|entity| entity.entity_id == article.entity_id)
            .into_iter()
            .flat_map(|entity| {
                entity
                    .connections
                    .iter()
                    .map(|connection| connection.entity_id.as_str())
                    .chain(
                        entity
                            .evidence_connections
                            .iter()
                            .map(|connection| connection.entity_id.as_str()),
                    )
            })
            .collect::<BTreeSet<_>>();
        if links
            .iter()
            .any(|entity_id| !available_neighbors.contains(entity_id))
        {
            return Err(PipelineError::ValidationError(format!(
                "Planned links for {} must follow a governed graph connection or resolved source-evidence co-mention",
                article.entity_id
            )));
        }
    }
    if accounted != allowed {
        return Err(PipelineError::ValidationError(
            "Wiki planner must account for every eligible entity exactly once as an article or an explicit possible-duplicate supporting subject"
                .to_string(),
        ));
    }
    Ok(())
}

fn definition_roots(definition: &KnowledgeViewDefinition) -> Vec<Link<Entity>> {
    match &definition.selector {
        KnowledgeViewSelector::Topic { topic, .. } => vec![topic.clone()],
        KnowledgeViewSelector::Community { seeds, .. } => seeds.clone(),
        KnowledgeViewSelector::Query { .. } => Vec::new(),
    }
}

struct PlannedDefinitionPresentation {
    relation_types: Vec<String>,
    navigation: Option<KnowledgeViewNavigation>,
}

fn planned_definition(
    space_id: &str,
    entity: &Entity,
    entities: &BTreeMap<String, Entity>,
    planned: &PlannedWikiArticle,
    presentation: PlannedDefinitionPresentation,
    config: &WikiCreationConfig,
    planner_model: &str,
) -> PipelineResult<KnowledgeViewDefinition> {
    let PlannedDefinitionPresentation {
        relation_types,
        navigation,
    } = presentation;
    let definition_id = wiki_article_definition_id(space_id, &entity.base.id.to_string());
    let mut base = BaseFields::new(
        definition_id,
        space_id.to_string(),
        "elephant.wiki_planner".to_string(),
    );
    base.security.clone_from(&entity.base.security);
    let mut subject_entities = vec![entity];
    for supporting_id in &planned.supporting_entity_ids {
        let supporting = entities.get(supporting_id).ok_or_else(|| {
            PipelineError::ValidationError(format!(
                "Planner attached unknown supporting entity {supporting_id}"
            ))
        })?;
        subject_entities.push(supporting);
    }
    let scope_entities = if navigation.is_some() {
        entities.values().collect::<Vec<_>>()
    } else {
        subject_entities.clone()
    };
    base.subjects = scope_entities
        .iter()
        .flat_map(|entity| entity.subjects())
        .collect::<Vec<_>>();
    base.subjects
        .sort_by(|left, right| left.id().cmp(right.id()));
    base.subjects
        .dedup_by(|left, right| left.id() == right.id());
    base.subjects_state = if scope_entities
        .iter()
        .all(|entity| entity.subjects_state() == SubjectsState::Complete)
    {
        SubjectsState::Complete
    } else {
        SubjectsState::Unknown
    };
    let plan_hash_input = serde_json::to_vec(&json!({
        "article": planned,
        "organizer_prompt": ORGANIZER_PROMPT_VERSION,
        "config": {
            "publish": config.publish,
            "max_hops": config.article_max_hops,
            "max_members": config.article_max_members,
            "relation_types": &relation_types,
        }
    }))
    .map_err(|error| {
        PipelineError::ValidationError(format!("Could not hash planned article: {error}"))
    })?;
    let plan_hash = hex::encode(Sha256::digest(plan_hash_input));
    base.provenance.sources = subject_entities
        .iter()
        .flat_map(|subject| subject.base.provenance.sources.iter().cloned())
        .map(|source| (source.id(), source))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    base.provenance.extractor = Some(Extractor {
        name: Some("elephant.wiki_planner".to_string()),
        version: Some("10".to_string()),
        model: Some(planner_model.to_string()),
    });
    base.provenance.prompt_id = Some(PLANNER_PROMPT_VERSION.to_string());
    base.provenance.config_id = Some(format!("sha256:{plan_hash}"));
    base.provenance.chain.push(ProvenanceStep {
        step: "wiki_article_planning".to_string(),
        input: entity.base.id.to_string(),
        output: base.id.to_string(),
    });
    Ok(KnowledgeViewDefinition {
        base,
        title: planned.title.trim().to_string(),
        publication_status: if config.publish {
            KnowledgeViewPublicationStatus::Published
        } else {
            KnowledgeViewPublicationStatus::Draft
        },
        related_articles: planned
            .related_entity_ids
            .iter()
            .map(|id| {
                id.parse::<RecordId>().map(Link::new).map_err(|error| {
                    PipelineError::ValidationError(format!(
                        "Planner returned invalid related entity ID {id}: {error}"
                    ))
                })
            })
            .collect::<PipelineResult<Vec<_>>>()?,
        navigation,
        selector: if subject_entities.len() == 1 {
            KnowledgeViewSelector::Topic {
                topic: Link::new(entity.base.id.clone()),
                relation_types,
                max_hops: config.article_max_hops,
                max_members: config.article_max_members,
            }
        } else {
            KnowledgeViewSelector::Community {
                seeds: subject_entities
                    .iter()
                    .map(|subject| Link::new(subject.base.id.clone()))
                    .collect(),
                relation_types,
                max_hops: config.article_max_hops,
                max_members: config.article_max_members,
            }
        },
    })
}

fn wiki_article_definition_id(space_id: &str, entity_id: &str) -> RecordId {
    let definition_key = format!("wiki-article-definition:{space_id}:{entity_id}");
    RecordId::new(
        TableName::KnowledgeViewDefinition,
        Uuid::new_v5(&Uuid::NAMESPACE_URL, definition_key.as_bytes()),
    )
}

fn publishable_relation_types(
    planned: &PlannedWikiArticle,
    inventory: &[WikiPlanningEntity],
) -> Vec<String> {
    let mut predicates = inventory
        .iter()
        .filter(|entity| {
            entity.entity_id == planned.entity_id
                || planned.supporting_entity_ids.contains(&entity.entity_id)
        })
        .flat_map(|entity| &entity.connections)
        .map(|connection| connection.predicate.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        predicates.push(NO_PUBLISHABLE_RELATIONS.to_string());
    }
    predicates
}

fn predicate_is_publishable(predicate: &elephant_types::records::PredicateRegistry) -> bool {
    predicate.status == "active"
        && predicate.semantics.is_some()
        && predicate.semantics_status == SemanticsStatus::Configured
}

fn restrictive_security_join(left: &Security, right: &Security) -> Security {
    let mut labels = left
        .labels
        .iter()
        .chain(&right.labels)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut handling = left
        .handling
        .iter()
        .chain(&right.handling)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    // This flag grants an exception, unlike restrictive handling caveats.
    // It cannot be inherited from only one contributor.
    handling.retain(|flag| {
        flag != "handling:subjectless_ok"
            || (left.handling.contains(flag) && right.handling.contains(flag))
    });
    labels.sort();
    handling.sort();
    Security {
        policy_id: left.policy_id.clone(),
        level: left.level.max(right.level),
        labels,
        handling,
    }
}

fn policy_aware_security_join(left: &Security, right: &Security) -> Option<Security> {
    if left.policy_id == right.policy_id {
        return Some(restrictive_security_join(left, right));
    }
    if left.policy_id == "default" {
        let mut promoted = left.clone();
        promoted.policy_id.clone_from(&right.policy_id);
        return Some(restrictive_security_join(&promoted, right));
    }
    if right.policy_id == "default" {
        let mut promoted = right.clone();
        promoted.policy_id.clone_from(&left.policy_id);
        return Some(restrictive_security_join(left, &promoted));
    }
    None
}

fn merge_record_subject_scope(target: &mut BaseFields, source: &impl HasSecurity) {
    let mut base = target.clone();
    base.subjects = source.subjects();
    base.subjects_state = source.subjects_state();
    merge_subject_scope(target, &base);
}

fn merge_subject_scope(target: &mut BaseFields, source: &BaseFields) {
    target.subjects.extend(source.subjects.iter().cloned());
    target
        .subjects
        .sort_by(|left, right| left.id().cmp(right.id()));
    target
        .subjects
        .dedup_by(|left, right| left.id() == right.id());
    if target.subjects_state != SubjectsState::Complete
        || source.subjects_state != SubjectsState::Complete
        || source.subjects.is_empty()
    {
        target.subjects_state = SubjectsState::Unknown;
    }
}

fn push_context_graph_citation(
    context: &mut KnowledgeViewRevision,
    record_id: &RecordId,
) -> String {
    let id = format!("c{}", context.citations.len().saturating_add(1));
    context.citations.push(KnowledgeCitation {
        id: id.clone(),
        kind: CitationKind::GraphRecord,
        record_id: record_id.to_string(),
        assertion_id: None,
        source_id: None,
        evidence_span: None,
    });
    id
}

fn append_context_block(
    context: &mut KnowledgeViewRevision,
    id: &str,
    kind: ViewBlockKind,
    heading: &str,
    statements: Vec<KnowledgeStatement>,
) {
    if statements.is_empty() {
        return;
    }
    let citations = statements
        .iter()
        .flat_map(|statement| statement.citations.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let markdown = statements
        .iter()
        .map(|statement| statement.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    context.blocks.push(KnowledgeViewBlock {
        id: id.to_string(),
        kind,
        heading: heading.to_string(),
        markdown,
        statements,
        citations,
    });
}

fn humanize_predicate(value: &str) -> String {
    value
        .trim()
        .replace(['_', '-'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn wiki_literal_text(value: &Literal) -> String {
    match value {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => bounded_text(value, 500),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Json(value) => bounded_text(&value.to_string(), 500),
    }
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.trim().chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}...")
    } else {
        bounded
    }
}

#[cfg(feature = "meerkat-extraction")]
impl WikiEditorialCompiler {
    /// Creates the production compiler using Meerkat model resolution.
    #[must_use]
    pub fn with_meerkat(storage: StorageClient, config: WikiEditorialConfig) -> Self {
        let llm = Arc::new(MeerkatWikiEditorialLlm::new(config.clone()));
        Self::new(storage, llm, config)
    }
}

fn validate_publishable_context(context: &KnowledgeViewRevision) -> PipelineResult<()> {
    let freshness = &context.freshness;
    if freshness.unsupported_claims > 0
        || freshness.unresolved_conflicts > 0
        || freshness.stale_truth_slots > 0
        || freshness.expired_claims > 0
        || freshness.selection_truncated
    {
        return Err(PipelineError::ValidationError(format!(
            "Context is not publishable: unsupported={}, conflicts={}, stale={}, expired={}, truncated={}",
            freshness.unsupported_claims,
            freshness.unresolved_conflicts,
            freshness.stale_truth_slots,
            freshness.expired_claims,
            freshness.selection_truncated
        )));
    }
    Ok(())
}

fn validated_quote(text: &str, span: &EvidenceSpan) -> PipelineResult<String> {
    let start = usize::try_from(span.start)
        .map_err(|_| PipelineError::ValidationError("Evidence start is invalid".to_string()))?;
    let end = usize::try_from(span.end)
        .map_err(|_| PipelineError::ValidationError("Evidence end is invalid".to_string()))?;
    let quote = text.get(start..end).ok_or_else(|| {
        PipelineError::ValidationError(format!(
            "Evidence span {}-{} is not a valid UTF-8 slice",
            span.start, span.end
        ))
    })?;
    let hash = format!("sha256:{}", hex::encode(Sha256::digest(quote.as_bytes())));
    if hash != span.quote_hash {
        return Err(PipelineError::ValidationError(
            "Evidence quote hash does not match the immutable revision".to_string(),
        ));
    }
    Ok(quote.to_string())
}

fn reconciled_evidence_span(text: &str, span: &EvidenceSpan) -> Option<EvidenceSpan> {
    let start = usize::try_from(span.start).ok()?;
    let end = usize::try_from(span.end).ok()?;
    let quote = text.get(start..end)?;
    let mut reconciled = span.clone();
    reconciled.quote_hash = format!("sha256:{}", hex::encode(Sha256::digest(quote.as_bytes())));
    Some(reconciled)
}

fn validate_editorial_draft(
    draft: &EditorialDraft,
    catalog: &[EditorialEvidenceItem],
) -> PipelineResult<()> {
    if draft.language != "en" {
        return Err(PipelineError::ValidationError(
            "Article writer must produce English prose".to_string(),
        ));
    }
    if draft.title.trim().is_empty() || draft.title.contains(['\n', '\r']) {
        return Err(PipelineError::ValidationError(
            "Article title must be one non-empty line".to_string(),
        ));
    }
    if draft.sections.is_empty() {
        return Err(PipelineError::ValidationError(
            "Article must contain at least one semantic section".to_string(),
        ));
    }
    validate_meaningful_lead(&draft.lead.text)?;
    let allowed_citations = catalog
        .iter()
        .map(|item| item.citation_id.as_str())
        .collect::<BTreeSet<_>>();
    let meaningful_citations = catalog
        .iter()
        .filter(|item| evidence_item_is_meaningful(item))
        .map(|item| item.citation_id.as_str())
        .collect::<BTreeSet<_>>();
    let source_citations = catalog
        .iter()
        .filter(|item| item.kind == CitationKind::EvidenceSpan)
        .map(|item| item.citation_id.as_str())
        .collect::<BTreeSet<_>>();
    if !source_citations.is_empty()
        && draft
            .lead
            .citation_ids
            .iter()
            .all(|citation_id| !source_citations.contains(citation_id.as_str()))
    {
        return Err(PipelineError::ValidationError(
            "Source-backed article lead must cite at least one supplied EvidenceSpan item"
                .to_string(),
        ));
    }
    if draft
        .lead
        .citation_ids
        .iter()
        .all(|citation_id| !meaningful_citations.contains(citation_id.as_str()))
    {
        return Err(PipelineError::ValidationError(
            "Article lead must cite a concrete summary, relationship, event, assertion, or source passage rather than identity metadata alone"
                .to_string(),
        ));
    }
    let mut statement_ids = BTreeSet::new();
    for statement in std::iter::once(&draft.lead).chain(
        draft
            .sections
            .iter()
            .flat_map(|section| &section.statements),
    ) {
        if statement.id.trim().is_empty() || !statement_ids.insert(statement.id.as_str()) {
            return Err(PipelineError::ValidationError(
                "Article statement IDs must be non-empty and unique".to_string(),
            ));
        }
        if statement.text.trim().is_empty()
            || statement.text.chars().count() > 2_000
            || statement.text.contains("[^")
            || statement.text.contains(['`', '<', '>'])
        {
            return Err(PipelineError::ValidationError(format!(
                "Statement {} is empty, too long, or contains model-authored citation markers",
                statement.id
            )));
        }
        validate_reader_facing_prose(&statement.id, &statement.text)?;
        if statement.citation_ids.is_empty()
            || statement
                .citation_ids
                .iter()
                .any(|id| !allowed_citations.contains(id.as_str()))
        {
            return Err(PipelineError::ValidationError(format!(
                "Statement {} lacks valid supplied citations",
                statement.id
            )));
        }
    }
    for section in &draft.sections {
        if section.id.trim().is_empty()
            || section.heading.trim().is_empty()
            || section.heading.contains(['\n', '\r'])
            || section.statements.is_empty()
        {
            return Err(PipelineError::ValidationError(
                "Article sections need an ID, one-line heading, and statements".to_string(),
            ));
        }
        parse_block_kind(&section.kind)?;
    }
    Ok(())
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn assign_missing_statement_ids(draft: &mut EditorialDraft) {
    let mut used_section_ids = draft
        .sections
        .iter()
        .filter_map(|section| {
            let id = section.id.trim();
            (!id.is_empty()).then(|| id.to_string())
        })
        .collect::<BTreeSet<_>>();
    let mut used = std::iter::once(&draft.lead)
        .chain(
            draft
                .sections
                .iter()
                .flat_map(|section| &section.statements),
        )
        .filter_map(|statement| {
            let id = statement.id.trim();
            (!id.is_empty()).then(|| id.to_string())
        })
        .collect::<BTreeSet<_>>();
    if draft.lead.id.trim().is_empty() {
        draft.lead.id = reserve_article_local_id("lead", &mut used);
    }
    for (section_index, section) in draft.sections.iter_mut().enumerate() {
        if section.id.trim().is_empty() {
            let preferred = format!("section-{}", section_index.saturating_add(1));
            section.id = reserve_article_local_id(&preferred, &mut used_section_ids);
        }
        if parse_block_kind(&section.kind).is_err() {
            section.kind = default_editorial_section_kind();
        }
        for (statement_index, statement) in section.statements.iter_mut().enumerate() {
            if statement.id.trim().is_empty() {
                let preferred = format!(
                    "section-{}-statement-{}",
                    section_index.saturating_add(1),
                    statement_index.saturating_add(1)
                );
                statement.id = reserve_article_local_id(&preferred, &mut used);
            }
        }
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn reserve_article_local_id(preferred: &str, used: &mut BTreeSet<String>) -> String {
    if used.insert(preferred.to_string()) {
        return preferred.to_string();
    }
    for suffix in 2_u32.. {
        let candidate = format!("{preferred}-{suffix}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("an unbounded numeric suffix always yields a unique statement ID")
}

fn validate_meaningful_lead(lead: &str) -> PipelineResult<()> {
    let trimmed = lead.trim();
    if trimmed.chars().count() < 32 || trimmed.split_whitespace().count() < 6 {
        return Err(PipelineError::ValidationError(
            "Article lead is too generic to explain what the subject is and why it matters"
                .to_string(),
        ));
    }
    let normalized = trimmed
        .trim_end_matches(['.', '!', '?'])
        .trim()
        .to_lowercase();
    const TYPE_ONLY_ENDINGS: [&str; 13] = [
        " is a person",
        " is an organization",
        " is a company",
        " is a platform",
        " is a project",
        " is a topic",
        " is a concept",
        " is a place",
        " is a product",
        " is an event",
        " is a device",
        " is a school",
        " is a band",
    ];
    if TYPE_ONLY_ENDINGS
        .iter()
        .any(|ending| normalized.ends_with(ending))
    {
        return Err(PipelineError::ValidationError(
            "Article lead merely repeats the subject type instead of conveying knowledge"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_reader_facing_prose(statement_id: &str, text: &str) -> PipelineResult<()> {
    let normalized = text.to_lowercase();
    const IMPLEMENTATION_PHRASES: [&str; 10] = [
        "editorial context",
        "central to this context",
        "at the center of this",
        "central topic in this",
        "supplied context",
        "evidence catalog",
        "entity inventory",
        "knowledge graph",
        "in this graph",
        "in elephant's data",
    ];
    if IMPLEMENTATION_PHRASES
        .iter()
        .any(|phrase| normalized.contains(phrase))
    {
        return Err(PipelineError::ValidationError(format!(
            "Statement {statement_id} contains implementation-facing filler instead of reader-facing knowledge"
        )));
    }
    Ok(())
}

fn evidence_item_is_meaningful(item: &EditorialEvidenceItem) -> bool {
    if item
        .source_quote
        .as_deref()
        .is_some_and(|quote| !quote.trim().is_empty())
    {
        return true;
    }
    item.canonical_statements.iter().any(|statement| {
        let normalized = statement.to_lowercase();
        ![
            "has canonical type",
            "at the center of this editorial context",
            "is central to this context",
            "matches this context's maintained criteria",
            "no canonical entities matched",
        ]
        .iter()
        .any(|boilerplate| normalized.contains(boilerplate))
    })
}

fn validate_verification(
    draft: &EditorialDraft,
    verification: &EditorialVerification,
) -> PipelineResult<()> {
    let expected = std::iter::once(&draft.lead)
        .chain(
            draft
                .sections
                .iter()
                .flat_map(|section| &section.statements),
        )
        .map(|statement| statement.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut rejected = Vec::new();
    for decision in &verification.decisions {
        if !actual.insert(decision.statement_id.as_str()) {
            return Err(PipelineError::ValidationError(format!(
                "Verifier repeated statement {}",
                decision.statement_id
            )));
        }
        if !decision.entailed || !decision.relevant {
            rejected.push(format!("{}: {}", decision.statement_id, decision.reason));
        }
    }
    if actual != expected {
        return Err(PipelineError::ValidationError(
            "Verifier must return exactly one decision for every statement".to_string(),
        ));
    }
    if !rejected.is_empty() {
        return Err(PipelineError::ValidationError(format!(
            "Article contains unsupported prose: {}",
            rejected.join("; ")
        )));
    }
    if !verification.article_useful
        || !verification.article_coherent
        || !verification.temporal_framing_sound
        || !verification.missing_major_topics.is_empty()
    {
        return Err(PipelineError::ValidationError(format!(
            "Article fails whole-page editorial review: {}; missing major topics: {}",
            verification.article_reason,
            verification.missing_major_topics.join(", ")
        )));
    }
    Ok(())
}

fn article_blocks(
    draft: &EditorialDraft,
    context_citations: &[KnowledgeCitation],
) -> PipelineResult<(Vec<KnowledgeViewBlock>, Vec<KnowledgeCitation>)> {
    let used_ids = std::iter::once(&draft.lead)
        .chain(
            draft
                .sections
                .iter()
                .flat_map(|section| &section.statements),
        )
        .flat_map(|statement| statement.citation_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let citations = context_citations
        .iter()
        .filter(|citation| used_ids.contains(&citation.id))
        .cloned()
        .collect::<Vec<_>>();
    let mut blocks = vec![editorial_block(
        "lead",
        "Overview",
        ViewBlockKind::Overview,
        std::slice::from_ref(&draft.lead),
    )];
    for section in &draft.sections {
        blocks.push(editorial_block(
            &section.id,
            &section.heading,
            parse_block_kind(&section.kind)?,
            &section.statements,
        ));
    }
    Ok((blocks, citations))
}

fn editorial_block(
    id: &str,
    heading: &str,
    kind: ViewBlockKind,
    statements: &[EditorialStatement],
) -> KnowledgeViewBlock {
    let statements = statements
        .iter()
        .map(|statement| KnowledgeStatement {
            id: statement.id.clone(),
            text: statement.text.trim().to_string(),
            derivation: StatementDerivation::Synthesized,
            verification: StatementVerification::EvidenceEntailment,
            citations: statement.citation_ids.clone(),
        })
        .collect::<Vec<_>>();
    let markdown = statements
        .iter()
        .map(|statement| {
            let marks = statement
                .citations
                .iter()
                .map(|id| format!(" [^{id}]"))
                .collect::<String>();
            format!("{}{marks}", statement.text)
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let citations = statements
        .iter()
        .flat_map(|statement| statement.citations.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    KnowledgeViewBlock {
        id: id.to_string(),
        kind,
        heading: heading.to_string(),
        markdown,
        statements,
        citations,
    }
}

fn parse_block_kind(kind: &str) -> PipelineResult<ViewBlockKind> {
    match kind {
        "overview" => Ok(ViewBlockKind::Overview),
        "current_facts" => Ok(ViewBlockKind::CurrentFacts),
        "relationships" => Ok(ViewBlockKind::Relationships),
        "timeline" => Ok(ViewBlockKind::Timeline),
        "conflicts" => Ok(ViewBlockKind::Conflicts),
        _ => Err(PipelineError::ValidationError(format!(
            "Unsupported editorial section kind: {kind}"
        ))),
    }
}

fn article_dependencies(
    context_view: &KnowledgeView,
    context: &KnowledgeViewRevision,
    writer_model: &str,
    verifier_model: &str,
) -> Vec<KnowledgeDependency> {
    let mut dependencies = context.dependencies.clone();
    dependencies.extend([
        KnowledgeDependency {
            record_id: context_view.base.id.to_string(),
            record_type: "knowledge_view".to_string(),
            role: "editorial_context".to_string(),
            observed_updated_at: Some(context_view.base.updated_at),
        },
        KnowledgeDependency {
            record_id: context.base.id.to_string(),
            record_type: "knowledge_view_revision".to_string(),
            role: "editorial_context_revision".to_string(),
            observed_updated_at: Some(context.base.updated_at),
        },
        KnowledgeDependency {
            record_id: format!("model:{writer_model}"),
            record_type: "model".to_string(),
            role: "article_writer".to_string(),
            observed_updated_at: None,
        },
        KnowledgeDependency {
            record_id: format!("model:{verifier_model}"),
            record_type: "model".to_string(),
            role: "article_verifier".to_string(),
            observed_updated_at: None,
        },
        KnowledgeDependency {
            record_id: format!("prompt:{WRITER_PROMPT_VERSION}"),
            record_type: "prompt".to_string(),
            role: "article_writer_prompt".to_string(),
            observed_updated_at: None,
        },
        KnowledgeDependency {
            record_id: format!("prompt:{VERIFIER_PROMPT_VERSION}"),
            record_type: "prompt".to_string(),
            role: "article_verifier_prompt".to_string(),
            observed_updated_at: None,
        },
    ]);
    let mut unique = BTreeMap::<String, KnowledgeDependency>::new();
    for dependency in dependencies {
        if let Some(existing) = unique.get_mut(&dependency.record_id) {
            let roles = existing
                .role
                .split('+')
                .chain(dependency.role.split('+'))
                .map(str::to_string)
                .collect::<BTreeSet<_>>();
            existing.role = roles.into_iter().collect::<Vec<_>>().join("+");
            existing.observed_updated_at = existing
                .observed_updated_at
                .max(dependency.observed_updated_at);
        } else {
            unique.insert(dependency.record_id.clone(), dependency);
        }
    }
    unique.into_values().collect()
}

fn article_provenance(
    context: &Provenance,
    context_revision: &KnowledgeViewRevision,
    output: &RecordId,
    writer_model: &str,
    verifier_model: &str,
    dependency_hash: &str,
) -> Provenance {
    let mut provenance = context.clone();
    provenance.sources.extend(
        context_revision
            .citations
            .iter()
            .filter_map(|citation| citation.source_id.as_deref())
            .filter_map(|source| source.parse::<RecordId>().ok()),
    );
    provenance.sources.sort();
    provenance.sources.dedup();
    provenance.extractor = Some(Extractor {
        name: Some(ARTICLE_COMPILER_NAME.to_string()),
        version: Some(ARTICLE_COMPILER_VERSION.to_string()),
        model: Some(format!("writer={writer_model}; verifier={verifier_model}")),
    });
    provenance.prompt_id = Some(format!("{WRITER_PROMPT_VERSION}+{VERIFIER_PROMPT_VERSION}"));
    provenance.config_id = Some(format!("sha256:{dependency_hash}"));
    provenance.chain.push(ProvenanceStep {
        step: "wiki_editorial_compilation".to_string(),
        input: context_revision.base.id.to_string(),
        output: output.to_string(),
    });
    provenance
}

fn article_validation(
    blocks: &[KnowledgeViewBlock],
    citations: &[KnowledgeCitation],
    dependencies: &[KnowledgeDependency],
) -> ValidationReceipt {
    let citation_ids = citations
        .iter()
        .map(|citation| citation.id.as_str())
        .collect::<BTreeSet<_>>();
    let dependency_ids = dependencies
        .iter()
        .map(|dependency| dependency.record_id.as_str())
        .collect::<BTreeSet<_>>();
    let statements = blocks
        .iter()
        .flat_map(|block| block.statements.iter())
        .collect::<Vec<_>>();
    let citations_resolve = citations.iter().all(|citation| {
        dependency_ids.contains(citation.record_id.as_str())
            || citation.evidence_span.as_ref().is_some_and(|span| {
                dependency_ids.contains(span.revision.id().to_string().as_str())
            })
    });
    let factual_blocks_cited = blocks
        .iter()
        .all(|block| !block.statements.is_empty() && !block.citations.is_empty());
    let statements_supported = statements.iter().all(|statement| {
        !statement.citations.is_empty()
            && statement
                .citations
                .iter()
                .all(|id| citation_ids.contains(id.as_str()))
            && statement.verification == StatementVerification::EvidenceEntailment
    });
    let prose_is_structured = blocks.iter().all(|block| {
        let rendered = block
            .statements
            .iter()
            .map(|statement| {
                let marks = statement
                    .citations
                    .iter()
                    .map(|id| format!(" [^{id}]"))
                    .collect::<String>();
                format!("{}{marks}", statement.text)
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        rendered == block.markdown
    });
    let mut warnings = Vec::new();
    if !citations_resolve {
        warnings.push("Article citation does not resolve to a declared dependency".to_string());
    }
    if !factual_blocks_cited {
        warnings.push("Article contains an uncited factual block".to_string());
    }
    if !statements_supported {
        warnings.push("Article contains a statement without verified support".to_string());
    }
    if !prose_is_structured {
        warnings.push("Article Markdown diverges from its statement ledger".to_string());
    }
    ValidationReceipt {
        citations_resolve,
        factual_blocks_cited,
        statements_supported,
        prose_is_structured,
        factual_block_count: u32::try_from(blocks.len()).unwrap_or(u32::MAX),
        citation_count: u32::try_from(citations.len()).unwrap_or(u32::MAX),
        statement_count: u32::try_from(statements.len()).unwrap_or(u32::MAX),
        warnings,
    }
}

fn editorial_dependency_hash(
    context: &KnowledgeViewRevision,
    writer_model: &str,
    verifier_model: &str,
    publish: bool,
) -> PipelineResult<String> {
    let value = json!({
        "context_revision": context.base.id.to_string(),
        "context_dependency_hash": context.compiler.dependency_hash,
        "source_evidence_dependencies": context.dependencies,
        "source_evidence_citations": context.citations,
        "security": context.base.security,
        "subjects": context.base.subjects,
        "subjects_state": context.base.subjects_state,
        "writer_model": writer_model,
        "verifier_model": verifier_model,
        "writer_prompt": WRITER_PROMPT_VERSION,
        "verifier_prompt": VERIFIER_PROMPT_VERSION,
        "publish": publish,
    });
    let bytes = serde_json::to_vec(&value).map_err(|error| {
        PipelineError::ValidationError(format!("Could not hash editorial inputs: {error}"))
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn render_article_markdown(title: &str, blocks: &[KnowledgeViewBlock]) -> String {
    let mut markdown = format!("# {}\n", title.trim());
    for block in blocks {
        markdown.push_str("\n## ");
        markdown.push_str(&block.heading);
        markdown.push_str("\n\n");
        markdown.push_str(&block.markdown);
        markdown.push('\n');
    }
    markdown
}

fn article_slug(title: &str, definition_id: &RecordId) -> String {
    let slug = title
        .chars()
        .flat_map(char::to_lowercase)
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        format!("article-{}", definition_id.id().simple())
    } else {
        slug
    }
}

fn render_writer_prompt(
    title: &str,
    navigation: Option<&KnowledgeViewNavigation>,
    catalog_json: &str,
) -> String {
    let editorial_context = navigation.map_or_else(
        || "No global editorial placement is available.".to_string(),
        |navigation| {
            format!(
                "This page belongs in the {:?} lens ({}) with role {:?} and whole-wiki importance {}/5.",
                navigation.section_title,
                navigation.section_description,
                navigation.page_role,
                navigation.article_importance,
            )
        },
    );
    format!(
        "Write a useful English private-wiki article titled {title:?}. Set language to en. {editorial_context} Use only the evidence catalog below. The lead must convey concrete supported identity and local relevance, not repeat the title or entity type. Synthesize across the evidence: explain what matters, how facts relate, what is current, and what changed over time. A list of disconnected extracted facts is not an article. For core and overview pages, cover the major supported dimensions of the subject and use meaningful sections. For reference and archive pages, be concise and explain why the subject occurs in this knowledge space. A supported family relationship, trip, purchase, message, decision, or event can be the entire local significance of a peripheral person, place, organization, product, or concept; do not add general encyclopedia facts to make it look important. When the catalog contains evidence_span items, the lead must cite at least one. Never describe a subject as central merely because it appears in an inventory or graph. Prefer current state, meaningful relationships, events, decisions, constraints, and useful history. Clearly frame old evidence as historical and never imply it is current. Omit ingestion metadata and graph machinery. Do not add general world knowledge unless it appears in the catalog. Every sentence must help explain this article subject or its supported connection to this private knowledge space. Exclude unrelated co-mentioned facts. Return plain prose without Markdown syntax. Every statement must cite catalog IDs supporting the complete statement. Sparse evidence calls for a short, honest article with one concrete local-relevance fact, not padding or invented synthesis.\n\nEVIDENCE CATALOG:\n{catalog_json}"
    )
}

fn render_format_recovery_prompt(
    base_prompt: &str,
    failed_attempt: usize,
    error: &PipelineError,
) -> String {
    format!(
        "{base_prompt}\n\nFORMAT RECOVERY: The previous article response (attempt {failed_attempt}) could not be decoded as the required structured article: {error}. Return the complete article again through the required output schema. Do not return Markdown, a code fence, an explanation, a refusal, or introductory text. Preserve the evidence-only and citation requirements above."
    )
}

fn render_planner_prompt(inventory_json: &str, config: &WikiCreationConfig) -> String {
    format!(
        "Create an English private-wiki plan from the bounded canonical entity inventory below. Set language to en. This is exhaustive full-corpus planning, not representative sampling: account for every supplied entity_id exactly once. Normally create one article per semantic entity. possible_duplicate_entity_ids are conservative name-match review hints, not proof that records share an identity. You may place an entity in another article's supporting_entity_ids only when it appears in that anchor's possible_duplicate_entity_ids and the supplied inventory contains concrete compatible evidence that the records plausibly describe the same real identity. Otherwise retain separate articles so a dirty graph cannot erase a person. Never use supporting_entity_ids merely to suppress an old, peripheral, sparse, ambiguous, or inconvenient subject. Return no more than {} articles and at least 70 percent as many articles as supplied entities. Every candidate has meaningful graph or evidence support. Assign importance from 1 to 5 based on usefulness to the whole knowledge space, not raw record count. A current household member or active household matter should outrank a decade-old peripheral business reference in a family graph. Use latest_evidence_at and earliest_evidence_at to distinguish current, enduring, mixed, and historical subjects, while recognizing that enduring family relationships remain important. Use page_role core only for defining subjects of the entire space, overview for navigational synthesis, topic for substantive ordinary pages, reference for small supporting subjects, and archive for mostly historical subjects. Choose human titles and explain the actual editorial reason. Link useful selected neighbors found in connections or evidence_connections. An evidence_connection is navigation context, not proof of a relationship. Use canonical IDs exactly. Do not write article prose.\n\nENTITY INVENTORY:\n{inventory_json}",
        config.max_articles
    )
}

fn render_organizer_prompt(selected_articles_json: &str) -> String {
    format!(
        "Design the global taxonomy for a coherent private wiki from the complete selected-article inventory below. The compact payload contains a columns array and rows whose values use those column positions. Set language to en. Infer and state the purpose of this knowledge space, then choose a concise reader-facing title. Select 1-48 featured_entity_ids that best define that purpose. Feature people and active/enduring household matters over old peripheral professional references when the corpus is a family system. Create 3-24 semantic reader lenses. This is information architecture, not an entity-type dump and not proportional representation of source volume. Put defining, current, and enduring subjects first. Put historical or peripheral material under appropriately narrow history, archive, or work lenses, never on equal footing with the people and active matters that define the space. Write a one-sentence description for every section. In each section's article_entity_ids, include 1-12 unique representative inventory IDs that anchor the lens; this is a taxonomy proposal, not the exhaustive assignment. Do not write article prose or mention graph/database machinery.\n\nSELECTED ARTICLES:\n{selected_articles_json}"
    )
}

fn render_organizer_repair_prompt(
    selected_articles_json: &str,
    rejected: &WikiOrganization,
    error: &PipelineError,
) -> PipelineResult<String> {
    let rejected_json = serde_json::to_string(rejected).map_err(|serialize_error| {
        PipelineError::ValidationError(format!(
            "Could not serialize rejected wiki organization: {serialize_error}"
        ))
    })?;
    Ok(format!(
        "Correct the rejected English private-wiki taxonomy below. Return a complete replacement, not a patch or commentary. Preserve good semantic taxonomy choices, but fix the exact validation failure. The compact inventory contains a columns array and rows whose values use those column positions. Use only inventory IDs. Keep 3-24 semantic reader lenses and 1-48 unique non-archive featured_entity_ids. In each section's article_entity_ids, include 1-12 unique representative IDs that anchor that lens; exhaustive assignment happens separately.\n\nORGANIZATION REJECTION:\n{error}\n\nREJECTED ORGANIZATION:\n{rejected_json}\n\nFULL SELECTED ARTICLE INVENTORY:\n{selected_articles_json}"
    ))
}

fn render_organizer_assignment_prompt(taxonomy_json: &str, inventory_json: &str) -> String {
    format!(
        "Assign every article in the bounded partition below to exactly one semantic lens from the fixed wiki taxonomy. Return a complete WikiOrganization object. Echo language, title, purpose, section id, section title, and section description from the taxonomy. Include only non-empty sections. article_entity_ids must contain every partition entity_id exactly once, with no outside or duplicate IDs. featured_entity_ids must contain 1-12 partition IDs for schema completeness; the global featured set remains canonical. Make semantic editorial assignments from the title, type, importance, role, temporal relevance, and taxonomy descriptions.\n\nFIXED WIKI TAXONOMY:\n{taxonomy_json}\n\nARTICLE ASSIGNMENT PARTITION:\n{inventory_json}"
    )
}

fn render_organizer_assignment_repair_prompt(
    taxonomy_json: &str,
    inventory_json: &str,
    rejected: &WikiOrganization,
    error: &PipelineError,
) -> PipelineResult<String> {
    let rejected_json = serde_json::to_string(rejected).map_err(|serialize_error| {
        PipelineError::ValidationError(format!(
            "Could not serialize rejected wiki assignment: {serialize_error}"
        ))
    })?;
    Ok(format!(
        "Correct the rejected bounded wiki-lens assignment below. Return a complete replacement, not a patch or commentary. Use only fixed taxonomy section IDs. Assign every partition entity_id exactly once with no outside or duplicate IDs.\n\nASSIGNMENT REJECTION:\n{error}\n\nREJECTED ASSIGNMENT:\n{rejected_json}\n\nFIXED WIKI TAXONOMY:\n{taxonomy_json}\n\nARTICLE ASSIGNMENT PARTITION:\n{inventory_json}"
    ))
}

fn render_repair_prompt(
    title: &str,
    catalog_json: &str,
    rejected: &EditorialDraft,
    verification: Option<&EditorialVerification>,
    error: &PipelineError,
) -> PipelineResult<String> {
    let rejected_json = serde_json::to_string_pretty(rejected).map_err(|serialize_error| {
        PipelineError::ValidationError(format!(
            "Could not serialize rejected article draft: {serialize_error}"
        ))
    })?;
    let verification_json = verification.map_or_else(
        || Ok("No verifier output: the draft failed deterministic validation.".to_string()),
        |output| {
            serde_json::to_string_pretty(output).map_err(|serialize_error| {
                PipelineError::ValidationError(format!(
                    "Could not serialize article verification: {serialize_error}"
                ))
            })
        },
    )?;
    Ok(format!(
        "Rewrite the rejected private-wiki article titled {title:?}. Preserve supported useful content, but remove or narrow every unsupported, embellished, generic, technical, or uncited claim identified below. The lead must contain a concrete supported fact, not merely the entity type or a claim that the subject is central to some context. Local relevance is enough for a peripheral subject: explain its supported connection to a family person, event, trip, purchase, decision, project, or source context without inventing general encyclopedia facts. If the catalog is sparse, return one short, honest lead and one semantic section rather than padding. If the catalog contains kind=evidence_span items, cite at least one of their citation_id values in the lead. Use only the evidence catalog. Return a complete replacement article, not commentary about the repair.\n\nREJECTION:\n{error}\n\nREJECTED DRAFT:\n{rejected_json}\n\nVERIFIER OUTPUT:\n{verification_json}\n\nEVIDENCE CATALOG:\n{catalog_json}"
    ))
}

fn render_verifier_prompt(draft: &EditorialDraft, catalog_json: &str) -> PipelineResult<String> {
    let draft_json = serde_json::to_string_pretty(draft).map_err(|error| {
        PipelineError::ValidationError(format!("Could not serialize article draft: {error}"))
    })?;
    Ok(format!(
        "Independently verify every article statement against only its cited evidence-catalog items. Mark entailed=false if any part is unsupported, embellished, temporally misleading, or broader than its citations. This is a private wiki, not a general encyclopedia. Mark relevant=true when an entailed statement either explains the named subject itself or explains why the subject occurs in this private knowledge space through a supported family relationship, trip, purchase, message, decision, project, event, or other meaningful local connection. Do not reject a family-specific fact merely because it would not belong in a general encyclopedia article. Still mark unrelated co-mentions and bare technical/source metadata relevant=false. Then judge the article as a whole. For a rich catalog, article_useful requires human-useful synthesis rather than disconnected facts or filler. For a sparse catalog, one concrete, entailed local-relevance fact is sufficient for a short useful article; never demand facts or synthesis absent from the catalog. article_coherent requires a lead and sections about one subject or its local connection. temporal_framing_sound requires old evidence to be framed historically rather than as current. missing_major_topics must list only major, well-supported subject dimensions in the evidence items, never catalog labels, editorial-centrality metadata, or facts absent from the catalog. Return exactly one decision per statement ID plus the whole-article judgment.\n\nARTICLE DRAFT:\n{draft_json}\n\nEVIDENCE CATALOG:\n{catalog_json}"
    ))
}

#[cfg(feature = "meerkat-extraction")]
const fn writer_system_prompt() -> &'static str {
    "You are Elephant's private-wiki editor. Write only English. Produce concise, structured prose from supplied canonical facts and source evidence. Every lead must communicate concrete supported knowledge. Never invent facts, restate only an entity type, pad sparse knowledge, call a subject central to a context, expose storage or context-selection mechanics, or cite evidence that does not support the complete statement."
}

#[cfg(feature = "meerkat-extraction")]
const fn planner_system_prompt() -> &'static str {
    "You are Elephant's private-wiki curator. Build a useful English article map over the supplied semantic graph shard. Select meaningful subjects and useful graph-backed navigation. Never turn raw sources, database metadata, or weak fragments into articles."
}

#[cfg(feature = "meerkat-extraction")]
const fn organizer_system_prompt() -> &'static str {
    "You are Elephant's private-wiki information architect. Organize every selected semantic article into a concise, coherent English navigation map. Use the subjects and their editorial reasons, not storage metadata. Do not omit, duplicate, or invent article IDs."
}

#[cfg(feature = "meerkat-extraction")]
const fn verifier_system_prompt() -> &'static str {
    "You are Elephant's strict private-wiki publication verifier. Judge entailment only from the supplied catalog. Relevance includes either explaining the named subject or its supported connection to this private knowledge space; do not demand general encyclopedia information from a peripheral subject. Reject plausible but unsupported prose, unrelated co-mentions, technical metadata, and filler. Accept one concise concrete local-relevance fact when that is all the catalog supports. Completeness and relevance are more important than fluency."
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn statement_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["id", "text", "citation_ids"],
        "properties": {
            "id": {"type": "string"},
            "text": {"type": "string"},
            "citation_ids": {"type": "array", "minItems": 1, "items": {"type": "string"}}
        }
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn article_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["language", "title", "lead", "sections"],
        "properties": {
            "language": {"type": "string", "enum": ["en"]},
            "title": {"type": "string"},
            "lead": statement_schema(),
            "sections": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["id", "heading", "kind", "statements"],
                    "properties": {
                        "id": {"type": "string"},
                        "heading": {"type": "string"},
                        "kind": {
                            "type": "string",
                            "enum": ["overview", "current_facts", "relationships", "timeline", "conflicts"]
                        },
                        "statements": {"type": "array", "minItems": 1, "items": statement_schema()}
                    }
                }
            }
        }
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn wiki_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["language", "articles"],
        "properties": {
            "language": {"type": "string", "enum": ["en"]},
            "articles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "entity_id", "title", "editorial_reason",
                        "supporting_entity_ids", "importance", "page_role",
                        "temporal_relevance", "related_entity_ids"
                    ],
                    "properties": {
                        "entity_id": {"type": "string"},
                        "title": {"type": "string"},
                        "editorial_reason": {"type": "string"},
                        "supporting_entity_ids": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "importance": {"type": "integer", "minimum": 1, "maximum": 5},
                        "page_role": {
                            "type": "string",
                            "enum": ["core", "overview", "topic", "reference", "archive"]
                        },
                        "temporal_relevance": {
                            "type": "string",
                            "enum": ["current", "enduring", "mixed", "historical"]
                        },
                        "related_entity_ids": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            }
        }
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[cfg(feature = "meerkat-extraction")]
fn wiki_organization_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["language", "title", "purpose", "featured_entity_ids", "sections"],
        "properties": {
            "language": {"type": "string", "enum": ["en"]},
            "title": {"type": "string"},
            "purpose": {"type": "string"},
            "featured_entity_ids": {
                "type": "array",
                "minItems": 1,
                "maxItems": 48,
                "items": {"type": "string"}
            },
            "sections": {
                "type": "array",
                "minItems": 1,
                "maxItems": 24,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["id", "title", "description", "article_entity_ids"],
                    "properties": {
                        "id": {"type": "string"},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "article_entity_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string"}
                        }
                    }
                }
            }
        }
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[cfg(feature = "meerkat-extraction")]
fn verification_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "decisions", "article_useful", "article_coherent",
            "temporal_framing_sound", "missing_major_topics", "article_reason"
        ],
        "properties": {
            "article_useful": {"type": "boolean"},
            "article_coherent": {"type": "boolean"},
            "temporal_framing_sound": {"type": "boolean"},
            "missing_major_topics": {
                "type": "array",
                "items": {"type": "string"}
            },
            "article_reason": {"type": "string"},
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["statement_id", "entailed", "relevant", "reason"],
                    "properties": {
                        "statement_id": {"type": "string"},
                        "entailed": {"type": "boolean"},
                        "relevant": {"type": "boolean"},
                        "reason": {"type": "string"}
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use elephant_storage::{EntityRepository, EventRepository, KnowledgeViewDefinitionRepository};
    use elephant_types::records::{
        Entity, EntityType, Event, KnowledgeViewDefinition, KnowledgeViewSelector,
    };
    use elephant_types::Link;

    use super::*;

    #[test]
    fn wiki_article_concurrency_supports_large_graph_compilation() {
        assert_eq!(bounded_wiki_article_concurrency(0), 1);
        assert_eq!(bounded_wiki_article_concurrency(8), 8);
        assert_eq!(bounded_wiki_article_concurrency(32), 32);
        assert_eq!(bounded_wiki_article_concurrency(usize::MAX), 32);
    }

    #[derive(Debug, Default)]
    struct MockEditorialLlm {
        planner_calls: AtomicUsize,
        organizer_calls: AtomicUsize,
        writer_calls: AtomicUsize,
        verifier_calls: AtomicUsize,
        planner_failures_remaining: AtomicUsize,
        organizer_failures_remaining: AtomicUsize,
        organizer_invalid_results_remaining: AtomicUsize,
        organizer_repair_prompts: AtomicUsize,
        reject_first: bool,
    }

    #[async_trait]
    impl WikiEditorialLlm for MockEditorialLlm {
        async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan> {
            self.planner_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .planner_failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(PipelineError::ConfigError(
                    "transient planner stream failure".to_string(),
                ));
            }
            let inventory = prompt
                .split_once("ENTITY INVENTORY:\n")
                .and_then(|(_, inventory)| serde_json::from_str::<Vec<Value>>(inventory).ok())
                .unwrap_or_default();
            Ok(WikiPlan {
                language: "en".to_string(),
                articles: inventory
                    .into_iter()
                    .filter_map(|entity| {
                        Some(PlannedWikiArticle {
                            entity_id: entity.get("entity_id")?.as_str()?.to_string(),
                            title: entity.get("display_name")?.as_str()?.to_string(),
                            editorial_reason: "Best-supported subject in this graph partition"
                                .to_string(),
                            supporting_entity_ids: Vec::new(),
                            importance: 3,
                            page_role: "topic".to_string(),
                            temporal_relevance: "mixed".to_string(),
                            related_entity_ids: Vec::new(),
                        })
                    })
                    .collect(),
            })
        }

        async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization> {
            self.organizer_calls.fetch_add(1, Ordering::SeqCst);
            if prompt.contains("ORGANIZATION REJECTION:") {
                self.organizer_repair_prompts.fetch_add(1, Ordering::SeqCst);
            }
            if self
                .organizer_failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(PipelineError::ConfigError(
                    "transient organizer stream failure".to_string(),
                ));
            }
            let invalid = self
                .organizer_invalid_results_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            let articles = prompt
                .split_once("SELECTED ARTICLES:\n")
                .or_else(|| prompt.split_once("FULL SELECTED ARTICLE INVENTORY:\n"))
                .or_else(|| prompt.split_once("ARTICLE ASSIGNMENT PARTITION:\n"))
                .and_then(|(_, articles)| serde_json::from_str::<Value>(articles).ok())
                .and_then(|payload| payload.get("rows").and_then(Value::as_array).cloned())
                .unwrap_or_default();
            Ok(WikiOrganization {
                language: "en".to_string(),
                title: "Household knowledge".to_string(),
                purpose: "Knowledge about the household and the people in it.".to_string(),
                featured_entity_ids: if invalid {
                    Vec::new()
                } else {
                    articles
                        .iter()
                        .take(12)
                        .filter_map(|article| article.get(0)?.as_str())
                        .map(ToString::to_string)
                        .collect()
                },
                sections: vec![PlannedWikiSection {
                    id: "household".to_string(),
                    title: "Household".to_string(),
                    description: "People, places, and matters in the household.".to_string(),
                    article_entity_ids: articles
                        .iter()
                        .filter_map(|article| article.get(0)?.as_str())
                        .map(ToString::to_string)
                        .collect(),
                }],
            })
        }

        async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft> {
            let call = self.writer_calls.fetch_add(1, Ordering::SeqCst);
            assert!(!prompt.contains("traverses at most"));
            let title = prompt
                .split_once("titled \"")
                .and_then(|(_, suffix)| suffix.split_once("\"."))
                .map_or("Eve", |(title, _)| title);
            let is_eve = prompt.contains("Eve is Noah's daughter");
            let catalog = prompt
                .rsplit_once("EVIDENCE CATALOG:\n")
                .and_then(|(_, catalog)| serde_json::from_str::<Vec<Value>>(catalog).ok())
                .unwrap_or_default();
            let citation_id = catalog
                .iter()
                .find(|item| item.get("kind").and_then(Value::as_str) == Some("evidence_span"))
                .or_else(|| catalog.first())
                .and_then(|item| item.get("citation_id"))
                .and_then(Value::as_str)
                .unwrap_or("c1")
                .to_string();
            let lead = if self.reject_first && call == 0 && is_eve {
                "Eve is Noah's daughter and therefore matters more than everyone else.".to_string()
            } else if is_eve {
                "Eve is Noah's daughter and an immediate member of Noah's family.".to_string()
            } else {
                format!("{title} is recorded as an unrelated supported topic.")
            };
            Ok(EditorialDraft {
                language: "en".to_string(),
                title: title.to_string(),
                lead: EditorialStatement {
                    id: "lead-1".to_string(),
                    text: lead,
                    citation_ids: vec![citation_id.clone()],
                },
                sections: vec![EditorialSection {
                    id: "family".to_string(),
                    heading: "Family context".to_string(),
                    kind: "relationships".to_string(),
                    statements: vec![EditorialStatement {
                        id: "family-1".to_string(),
                        text: if is_eve {
                            "Eve's recorded family relationship identifies Noah as her parent."
                                .to_string()
                        } else {
                            format!("{title} is described as an unrelated supported topic.")
                        },
                        citation_ids: vec![citation_id],
                    }],
                }],
            })
        }

        async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification> {
            self.verifier_calls.fetch_add(1, Ordering::SeqCst);
            assert!(prompt.contains("ARTICLE DRAFT"));
            assert!(prompt.contains("EVIDENCE CATALOG"));
            let unsupported = prompt.contains("matters more than everyone else");
            Ok(EditorialVerification {
                decisions: vec![
                    StatementVerificationDecision {
                        statement_id: "lead-1".to_string(),
                        entailed: !unsupported,
                        relevant: !unsupported,
                        reason: if unsupported {
                            "The comparison is not present in the evidence.".to_string()
                        } else {
                            "The entity summary supports the statement.".to_string()
                        },
                    },
                    StatementVerificationDecision {
                        statement_id: "family-1".to_string(),
                        entailed: true,
                        relevant: true,
                        reason: "The entity summary supports the statement.".to_string(),
                    },
                ],
                article_useful: !unsupported,
                article_coherent: !unsupported,
                temporal_framing_sound: true,
                missing_major_topics: Vec::new(),
                article_reason: if unsupported {
                    "The draft contains an unsupported comparison.".to_string()
                } else {
                    "The draft is supported and useful.".to_string()
                },
            })
        }

        fn writer_model(&self) -> &str {
            "mock-writer"
        }

        fn verifier_model(&self) -> &str {
            "mock-verifier"
        }
    }

    async fn seeded_definition(storage: &StorageClient) -> RecordId {
        let mut eve = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Eve".to_string(),
        );
        eve.summary = Some("Eve is Noah's daughter and central to the family.".to_string());
        let ada_id = eve.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(eve)
            .await
            .expect("entity");

        let definition_id = RecordId::new_v4(TableName::KnowledgeViewDefinition);
        KnowledgeViewDefinitionRepository::new(storage.clone())
            .upsert(KnowledgeViewDefinition {
                base: BaseFields::new(
                    definition_id.clone(),
                    "space".to_string(),
                    "test".to_string(),
                ),
                title: "Eve".to_string(),
                publication_status: KnowledgeViewPublicationStatus::Published,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Topic {
                    topic: Link::new(ada_id),
                    relation_types: Vec::new(),
                    max_hops: 1,
                    max_members: 10,
                },
            })
            .await
            .expect("definition");
        definition_id
    }

    fn planning_entity(name: &str) -> Entity {
        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            name.to_string(),
        );
        entity.summary = Some(format!("{name} has source-backed household context."));
        entity
    }

    #[tokio::test]
    async fn organizer_scope_includes_disconnected_inventory_subjects() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let eve = planning_entity("Eve");
        let unrelated = planning_entity("Secret project");
        let ids = [eve.base.id.clone(), unrelated.base.id.clone()];
        for entity in [&eve, &unrelated] {
            EntityRepository::new(storage.clone())
                .create(entity.clone())
                .await
                .expect("entity");
        }
        let planner = WikiGraphPlanner::new(
            storage.clone(),
            Arc::new(MockEditorialLlm::default()),
            WikiEditorialConfig::default(),
        );
        let result = planner
            .create_from_entities(
                "space",
                vec![eve, unrelated],
                WikiCreationConfig {
                    max_articles: 2,
                    publish: true,
                    ..WikiCreationConfig::default()
                },
            )
            .await
            .expect("wiki");
        assert_eq!(result.articles.len(), 2);
        for article in result.articles {
            let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
                .get("space", &article.view.root_record.parse().expect("id"))
                .await
                .expect("lookup")
                .expect("definition");
            assert!(definition.navigation.is_some());
            assert_eq!(definition_roots(&definition).len(), 1);
            for id in &ids {
                assert!(definition
                    .base
                    .subjects
                    .iter()
                    .any(|subject| subject.id() == id));
                assert!(article
                    .revision
                    .base
                    .subjects
                    .iter()
                    .any(|subject| subject.id() == id));
            }
        }
    }

    #[tokio::test]
    async fn evidence_catalog_joins_revision_security_and_subjects_without_root_assertions() {
        use elephant_types::records::doc_revision::RendererMetadata;
        use elephant_types::records::{DocRevision, RevisionStatus};
        let storage = StorageClient::new_embedded().await.expect("storage");
        let definition_id = seeded_definition(&storage).await;
        let mut context = GraphViewCompiler::new(storage.clone())
            .compile("space", &definition_id, true)
            .await
            .expect("context")
            .revision;
        let forbidden_subject = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::DocRevision),
            "space".to_string(),
            "test".to_string(),
        );
        base.security.level = 3;
        base.subjects = vec![Link::new(forbidden_subject.clone())];
        base.subjects_state = SubjectsState::Complete;
        let revision_id = base.id.clone();
        DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base,
                doc: Link::new(RecordId::new_v4(TableName::Doc)),
                text: "Secret evidence".to_string(),
                text_hash: "sha256:test".to_string(),
                renderer: RendererMetadata {
                    name: "test".to_string(),
                    version: "1".to_string(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: Utc::now(),
            })
            .await
            .expect("revision");
        let span = reconciled_evidence_span(
            "Secret evidence",
            &EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: None,
                start: 0,
                end: 15,
                quote_hash: String::new(),
            },
        )
        .expect("span");
        context.citations.push(KnowledgeCitation {
            id: "secret".to_string(),
            kind: CitationKind::EvidenceSpan,
            record_id: RecordId::new_v4(TableName::Rel).to_string(),
            assertion_id: None,
            source_id: None,
            evidence_span: Some(span),
        });
        let compiler = WikiEditorialCompiler::new(
            storage,
            Arc::new(MockEditorialLlm::default()),
            WikiEditorialConfig::default(),
        );
        let catalog = compiler
            .build_evidence_catalog("space", &mut context)
            .await
            .expect("catalog");
        assert!(catalog
            .iter()
            .any(|item| item.source_quote.as_deref() == Some("Secret evidence")));
        assert_eq!(context.base.security.level, 3);
        assert!(context
            .base
            .subjects
            .iter()
            .any(|subject| subject.id() == &forbidden_subject));
        assert!(context
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == revision_id.to_string()));
    }

    #[tokio::test]
    async fn root_truth_slot_preserves_additional_and_unknown_subject_requirements() {
        use elephant_types::records::TruthSlot;
        let storage = StorageClient::new_embedded().await.expect("storage");
        let definition_id = seeded_definition(&storage).await;
        let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
            .get("space", &definition_id)
            .await
            .expect("lookup")
            .expect("definition");
        let root = definition_roots(&definition)[0].id().clone();
        let restricted = RecordId::new_v4(TableName::Entity);
        let mut context = GraphViewCompiler::new(storage.clone())
            .compile("space", &definition_id, true)
            .await
            .expect("context")
            .revision;
        context.base.subjects_state = SubjectsState::Complete;
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::TruthSlot),
            "space".to_string(),
            "test".to_string(),
        );
        base.subjects = vec![Link::new(root.clone()), Link::new(restricted.clone())];
        base.subjects_state = SubjectsState::Unknown;
        TruthSlotRepository::new(storage.clone())
            .create(TruthSlot {
                base,
                slot_key: "test-slot".to_string(),
                slot_type: "attribute".to_string(),
                subject_entity: root.to_string(),
                predicate_id: "test_predicate".to_string(),
                current_values: Vec::new(),
                conflict_group: None,
                confidence: 1.0,
                rationale: None,
                semantics_version: 1,
                alias_version: 1,
                stale: true,
                stale_reason: None,
                computed_at: Utc::now(),
                security_hash: "test".to_string(),
            })
            .await
            .expect("slot");
        WikiEditorialCompiler::new(
            storage,
            Arc::new(MockEditorialLlm::default()),
            WikiEditorialConfig::default(),
        )
        .augment_with_root_semantic_records("space", &definition, &mut context)
        .await
        .expect("augmentation");
        assert!(context
            .blocks
            .iter()
            .any(|block| block.id == "root-knowledge-issues"));
        assert!(context
            .base
            .subjects
            .iter()
            .any(|subject| subject.id() == &restricted));
        assert_eq!(context.base.subjects_state, SubjectsState::Unknown);
    }

    #[tokio::test]
    async fn revoked_wiki_cannot_resume_from_completed_history_or_after_restart() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repository = WorkItemRepository::new(storage.clone());
        let payload = ManagedWikiPayload {
            authority: ManagedWikiAuthority::Space {
                space_id: "space".to_string(),
                grant_id: repository
                    .create_managed_wiki_grant("space", "test")
                    .await
                    .expect("grant"),
            },
            max_articles: 0,
            article_max_hops: 1,
            article_max_members: 250,
            refresh_interval_minutes: 360,
            writer_model: "mock".to_string(),
            verifier_model: "mock".to_string(),
            last_graph_fingerprint: None,
        };
        let now = Utc::now();
        let mut predecessor =
            enqueue_managed_wiki_reconciliation(&storage, "space", "test", &payload, now)
                .await
                .expect("predecessor");
        predecessor.status = WorkItemStatus::Done;
        repository
            .update("space", &predecessor.base.id, predecessor.clone())
            .await
            .expect("complete");
        enqueue_managed_wiki_successor(
            &storage,
            "space",
            "test",
            &predecessor.base.id,
            &payload,
            now + chrono::Duration::hours(6),
        )
        .await
        .expect("successor");
        repository
            .revoke_managed_wiki_grants("space")
            .await
            .expect("stop");
        let resumed = enqueue_managed_wiki_graph_change(
            &storage,
            "space",
            "test",
            now + chrono::Duration::minutes(1),
            now + chrono::Duration::minutes(5),
        )
        .await
        .expect("graph event");
        assert!(resumed.is_none());
        repository
            .create_managed_wiki_grant("space", "test")
            .await
            .expect("restart");
        assert!(enqueue_managed_wiki_successor(
            &storage,
            "space",
            "test",
            &predecessor.base.id,
            &payload,
            now + chrono::Duration::hours(6)
        )
        .await
        .is_err());
    }

    #[test]
    fn security_join_does_not_transfer_subjectless_permission() {
        let permission = Security {
            handling: vec!["handling:subjectless_ok".to_string()],
            ..Security::default()
        };
        let restricted = Security::default();
        for (left, right) in [(&permission, &restricted), (&restricted, &permission)] {
            assert!(!restrictive_security_join(left, right)
                .handling
                .iter()
                .any(|flag| flag == "handling:subjectless_ok"));
        }
        assert!(restrictive_security_join(&permission, &permission)
            .handling
            .iter()
            .any(|flag| flag == "handling:subjectless_ok"));
    }

    #[test]
    fn editorial_output_recovers_complete_json_text_only() {
        let recovered =
            recover_editorial_output(None, "  {\"answer\":42}  ").expect("complete JSON response");
        assert_eq!(recovered, serde_json::json!({"answer": 42}));
        assert!(recover_editorial_output(None, "preface {\"answer\":42}").is_err());
        let fenced = recover_editorial_output(None, "```json\n{\"answer\":42}\n```")
            .expect("whole-response JSON fence");
        assert_eq!(fenced, serde_json::json!({"answer": 42}));
        assert!(recover_editorial_output(None, "preface\n```json\n{\"answer\":42}\n```").is_err());
    }

    #[test]
    fn editorial_output_prefers_native_structured_value() {
        let native = serde_json::json!({"source": "native"});
        let recovered = recover_editorial_output(Some(native.clone()), "not json")
            .expect("native structured output");
        assert_eq!(recovered, native);
    }

    #[test]
    fn editorial_wire_shape_reports_structure_without_values() {
        let shape = describe_editorial_wire_shape(
            &serde_json::json!({
                "title": "Private title",
                "sections": [{"body": "Private prose"}]
            }),
            0,
        );

        assert_eq!(
            shape,
            "object(title:string,sections:array(len=1, first=object(body:string)))"
        );
        assert!(!shape.contains("Private"));
    }

    #[test]
    fn alternate_body_article_wire_shape_is_decoded_without_losing_prose() {
        let draft = decode_editorial_draft(serde_json::json!({
            "language": "en",
            "title": "Noah",
            "lens": "family",
            "role": "core",
            "importance": "high",
            "lead": "Noah is a member of the Example family. [c1]",
            "sections": [{
                "heading": "Family",
                "body": "Noah is Eve's father."
            }],
            "statements": [
                "Noah is Eve's father. [c2]",
                "Noah worked on Example Labs. [c3]"
            ]
        }))
        .expect("alternate article wire shape");

        assert_eq!(draft.lead.text, "Noah is a member of the Example family.");
        assert_eq!(draft.lead.citation_ids, ["c1"]);
        assert_eq!(draft.sections[0].heading, "Family");
        assert_eq!(
            draft.sections[0].statements[0].text,
            "Noah is Eve's father."
        );
        assert_eq!(draft.sections[0].statements[0].citation_ids, ["c2"]);
        assert_eq!(draft.sections[1].heading, "Additional context");
        assert_eq!(
            draft.sections[1].statements[0].text,
            "Noah worked on Example Labs."
        );
        assert_eq!(draft.sections[1].statements[0].citation_ids, ["c3"]);
    }

    #[test]
    fn alternate_body_article_wire_shape_does_not_require_a_redundant_ledger() {
        let draft = decode_editorial_draft(serde_json::json!({
            "language": "en",
            "title": "Noah",
            "lead": "Noah is a member of the family. [c1]",
            "sections": [{
                "heading": "Family",
                "body": "Noah is Eve's father. [c2]"
            }]
        }))
        .expect("alternate body article without ledger");

        assert_eq!(draft.lead.citation_ids, ["c1"]);
        assert_eq!(draft.sections[0].statements[0].citation_ids, ["c2"]);
    }

    #[test]
    fn alternate_article_text_wire_shape_preserves_markdown_structure() {
        let draft = decode_editorial_draft(serde_json::json!({
            "language": "en",
            "title": "Noah",
            "lens": "family",
            "role": "core",
            "importance": "high",
            "article": "# Noah\n\nNoah is a member of the family. [c1]\n\n## Family\n\nNoah is Eve's father. [c2]\n\nNoah is Oliver's father. [^c3]\n\n## Work\n\nNoah worked on Example Labs. [c4]"
        }))
        .expect("alternate article-text wire shape");

        assert_eq!(draft.lead.text, "Noah is a member of the family.");
        assert_eq!(draft.lead.citation_ids, ["c1"]);
        assert_eq!(draft.sections.len(), 2);
        assert_eq!(draft.sections[0].heading, "Family");
        assert_eq!(draft.sections[0].statements.len(), 2);
        assert_eq!(draft.sections[0].statements[1].citation_ids, ["c3"]);
        assert_eq!(draft.sections[1].heading, "Work");
        assert_eq!(
            draft.sections[1].statements[0].text,
            "Noah worked on Example Labs."
        );
    }

    #[test]
    fn article_format_failure_changes_the_next_writer_prompt() {
        let prompt = render_format_recovery_prompt(
            "Write the evidence-backed article.",
            2,
            &PipelineError::ValidationError("model returned prose".to_string()),
        );
        assert!(prompt.contains("attempt 2"));
        assert!(prompt.contains("required output schema"));
        assert!(prompt.contains("Do not return Markdown"));
        assert!(prompt.contains("Write the evidence-backed article."));
    }

    #[test]
    fn missing_statement_ids_are_assigned_without_changing_article_content() {
        let mut draft = EditorialDraft {
            language: "en".to_string(),
            title: "Eve".to_string(),
            lead: EditorialStatement {
                id: String::new(),
                text: "Eve is a member of the family.".to_string(),
                citation_ids: vec!["c1".to_string()],
            },
            sections: vec![EditorialSection {
                id: String::new(),
                heading: "Family".to_string(),
                kind: "relationships".to_string(),
                statements: vec![EditorialStatement {
                    id: String::new(),
                    text: "Noah is Eve's father.".to_string(),
                    citation_ids: vec!["c2".to_string()],
                }],
            }],
        };

        assign_missing_statement_ids(&mut draft);

        assert_eq!(draft.lead.id, "lead");
        assert_eq!(draft.sections[0].id, "section-1");
        assert_eq!(draft.sections[0].statements[0].id, "section-1-statement-1");
        assert_eq!(draft.lead.text, "Eve is a member of the family.");
        assert_eq!(draft.sections[0].statements[0].citation_ids, ["c2"]);
    }

    #[test]
    fn cited_text_statement_wire_shape_preserves_prose_and_citations() {
        let statement = serde_json::from_value::<EditorialStatement>(serde_json::json!(
            "Noah is Eve's father. [c9] [^c12]"
        ))
        .expect("cited text statement");

        assert!(statement.id.is_empty());
        assert_eq!(statement.text, "Noah is Eve's father.");
        assert_eq!(statement.citation_ids, ["c9", "c12"]);
    }

    #[test]
    fn missing_section_id_wire_shape_gets_a_structural_id() {
        let mut draft = serde_json::from_value::<EditorialDraft>(serde_json::json!({
            "language": "en",
            "title": "Noah",
            "lead": {
                "id": "lead",
                "text": "Noah is a member of the Example family.",
                "citation_ids": ["c1"]
            },
            "sections": [{
                "heading": "Family",
                "kind": "relationships",
                "statements": [{
                    "id": "family-1",
                    "text": "Noah is Eve's father.",
                    "citation_ids": ["c2"]
                }]
            }]
        }))
        .expect("draft with missing section id");

        assign_missing_statement_ids(&mut draft);

        assert_eq!(draft.sections[0].id, "section-1");
        assert_eq!(draft.sections[0].heading, "Family");
        assert_eq!(
            draft.sections[0].statements[0].text,
            "Noah is Eve's father."
        );
    }

    #[test]
    fn missing_section_presentation_wire_shape_uses_neutral_defaults() {
        let draft = serde_json::from_value::<EditorialDraft>(serde_json::json!({
            "language": "en",
            "title": "Noah",
            "lead": {
                "id": "lead",
                "text": "Noah is a member of the Example family.",
                "citation_ids": ["c1"]
            },
            "sections": [{
                "id": "family",
                "statements": [{
                    "id": "family-1",
                    "text": "Noah is Eve's father.",
                    "citation_ids": ["c2"]
                }]
            }]
        }))
        .expect("draft with missing section kind");

        assert_eq!(draft.sections[0].kind, "overview");
        assert_eq!(draft.sections[0].heading, "Overview");
        assert_eq!(draft.sections[0].statements[0].citation_ids, ["c2"]);
    }

    #[test]
    fn unknown_section_presentation_kind_preserves_heading_and_prose() {
        let mut draft = EditorialDraft {
            language: "en".to_string(),
            title: "Noah".to_string(),
            lead: EditorialStatement {
                id: "lead".to_string(),
                text: "Noah is a member of the Example family.".to_string(),
                citation_ids: vec!["c1".to_string()],
            },
            sections: vec![EditorialSection {
                id: "career".to_string(),
                heading: "Career".to_string(),
                kind: "career".to_string(),
                statements: vec![EditorialStatement {
                    id: "career-1".to_string(),
                    text: "Noah worked on Example Labs.".to_string(),
                    citation_ids: vec!["c2".to_string()],
                }],
            }],
        };

        assign_missing_statement_ids(&mut draft);

        assert_eq!(draft.sections[0].kind, "overview");
        assert_eq!(draft.sections[0].heading, "Career");
        assert_eq!(
            draft.sections[0].statements[0].text,
            "Noah worked on Example Labs."
        );
    }

    #[tokio::test]
    async fn wiki_planning_receipt_covers_complete_security_scoped_input() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner = WikiGraphPlanner::new(storage, llm, WikiEditorialConfig::default());
        let candidates = vec![
            planning_entity("Eve"),
            planning_entity("Duplicate Person"),
            planning_entity("Duplicate Person"),
            planning_entity("Noah"),
            planning_entity("Noah Example"),
        ];
        let prepared = planner
            .build_planning_inventory("space", &candidates)
            .await
            .expect("inventory");

        assert_eq!(prepared.receipt.entities_scanned, 5);
        assert_eq!(prepared.receipt.ambiguous_person_groups_withheld, 2);
        assert_eq!(prepared.receipt.ambiguous_person_entities_withheld, 0);
        assert_eq!(prepared.receipt.relationships_scanned, 0);
        assert_eq!(
            prepared
                .receipt
                .relationships_withheld_by_predicate_governance,
            0
        );
        assert_eq!(prepared.receipt.assertions_scanned, 0);
        assert_eq!(prepared.receipt.attributes_scanned, 0);
        assert_eq!(
            prepared.receipt.attributes_withheld_by_predicate_governance,
            0
        );
        assert_eq!(prepared.receipt.events_scanned, 0);
        assert_eq!(prepared.receipt.truth_slots_scanned, 0);
        assert_eq!(prepared.receipt.eligible_entities, 5);
        assert_eq!(prepared.receipt.planner_candidates, 5);
        assert!(prepared.receipt.graph_fingerprint.starts_with("sha256:"));
        assert_eq!(
            prepared
                .entities
                .iter()
                .find(|entity| entity.display_name == "Duplicate Person")
                .expect("possible duplicate")
                .possible_duplicate_entity_ids
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn wiki_retries_transient_planner_and_organizer_stream_failures() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm {
            planner_failures_remaining: AtomicUsize::new(7),
            organizer_failures_remaining: AtomicUsize::new(7),
            ..MockEditorialLlm::default()
        });
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default());
        let eve = planning_entity("Eve");
        EntityRepository::new(storage)
            .create(eve.clone())
            .await
            .expect("entity");

        let wiki = planner
            .create_from_entities(
                "space",
                vec![eve],
                WikiCreationConfig {
                    max_articles: 1,
                    ..WikiCreationConfig::default()
                },
            )
            .await
            .expect("transient failures retried");

        assert_eq!(wiki.plan.articles.len(), 1);
        assert_eq!(llm.planner_calls.load(Ordering::SeqCst), 8);
        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 9);
    }

    #[tokio::test]
    async fn wiki_organizer_retries_with_exact_validation_feedback() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm {
            organizer_invalid_results_remaining: AtomicUsize::new(1),
            ..MockEditorialLlm::default()
        });
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default());
        let eve = planning_entity("Eve");
        EntityRepository::new(storage)
            .create(eve.clone())
            .await
            .expect("entity");

        planner
            .create_from_entities(
                "space",
                vec![eve],
                WikiCreationConfig {
                    max_articles: 1,
                    ..WikiCreationConfig::default()
                },
            )
            .await
            .expect("organizer correction");

        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 3);
        assert_eq!(llm.organizer_repair_prompts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wiki_taxonomy_assignment_is_bounded_and_exhaustive() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner = WikiGraphPlanner::new(storage, llm.clone(), WikiEditorialConfig::default());
        let inventory = (0..61)
            .map(|index| WikiPlanningEntity {
                entity_id: format!("entity:{index:032x}"),
                entity_type: "topic".to_string(),
                display_name: format!("Topic {index}"),
                summary: Some(format!("Supported topic {index}")),
                graph_degree: 1,
                evidence_assertions: 1,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: Vec::new(),
                connections: Vec::new(),
                evidence_connections: Vec::new(),
            })
            .collect::<Vec<_>>();
        let plan = WikiPlan {
            language: "en".to_string(),
            articles: inventory
                .iter()
                .map(|entity| PlannedWikiArticle {
                    entity_id: entity.entity_id.clone(),
                    title: entity.display_name.clone(),
                    editorial_reason: "Supported topic".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 3,
                    page_role: "topic".to_string(),
                    temporal_relevance: "mixed".to_string(),
                    related_entity_ids: Vec::new(),
                })
                .collect(),
        };
        let taxonomy = WikiOrganization {
            language: "en".to_string(),
            title: "Household knowledge".to_string(),
            purpose: "Knowledge about the household.".to_string(),
            featured_entity_ids: vec![inventory[0].entity_id.clone()],
            sections: vec![PlannedWikiSection {
                id: "household".to_string(),
                title: "Household".to_string(),
                description: "Household people and matters.".to_string(),
                article_entity_ids: vec![inventory[0].entity_id.clone()],
            }],
        };

        let organization = planner
            .assign_wiki_taxonomy(taxonomy, &plan, &inventory)
            .await
            .expect("sharded assignment");

        assert_eq!(organization.sections[0].article_entity_ids.len(), 61);
        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn wiki_planning_includes_entities_supported_only_by_approved_events() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner = WikiGraphPlanner::new(storage.clone(), llm, WikiEditorialConfig::default());
        let mut entity = planning_entity("Rome trip");
        entity.summary = None;
        let mut event = Event::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Event),
                "space".to_string(),
                "test".to_string(),
            ),
            "Family trip to Rome",
            Vec::new(),
            vec![Link::new(RecordId::new_v4(TableName::Source))],
        )
        .expect("event");
        event.status = EventStatus::Approved;
        event.base.subjects = vec![Link::new(entity.base.id.clone())];
        EntityRepository::new(storage.clone())
            .create(entity.clone())
            .await
            .expect("store entity");
        EventRepository::new(storage)
            .create(event)
            .await
            .expect("store event");

        let prepared = planner
            .build_planning_inventory("space", &[entity])
            .await
            .expect("inventory");
        assert_eq!(prepared.receipt.events_scanned, 1);
        assert_eq!(prepared.receipt.eligible_entities, 1);
        assert_eq!(prepared.entities[0].events, 1);
        assert!(prepared.entities[0].latest_evidence_at.is_some());
    }

    #[tokio::test]
    async fn wiki_full_graph_loader_has_no_first_page_ceiling() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repository = EntityRepository::new(storage.clone());
        for index in 0..525 {
            let mut entity = planning_entity(&format!("Entity {index:03}"));
            entity.entity_type = EntityType::new("topic");
            repository.create(entity).await.expect("entity");
        }

        let loaded = load_full_graph_entities(&storage, "space")
            .await
            .expect("full graph");
        assert_eq!(loaded.len(), 525);
    }

    #[test]
    fn wiki_planning_partitions_cover_every_eligible_entity_once() {
        let mut inventory = (0..125)
            .map(|index| WikiPlanningEntity {
                entity_id: format!("entity:{index:032x}"),
                entity_type: "topic".to_string(),
                display_name: format!("Topic {index}"),
                summary: Some(format!("Supported topic {index}")),
                graph_degree: 1,
                evidence_assertions: 1,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: Vec::new(),
                connections: Vec::new(),
                evidence_connections: Vec::new(),
            })
            .collect::<Vec<_>>();
        inventory[0].possible_duplicate_entity_ids = inventory
            .iter()
            .skip(1)
            .map(|entity| entity.entity_id.clone())
            .collect();
        let partitions = planning_partitions(&inventory);
        assert_eq!(partitions.len(), 5);
        assert!(partitions
            .iter()
            .all(|partition| partition.len() <= MAX_WIKI_PLANNING_PARTITION_ENTITIES));
        let selected = partitions
            .iter()
            .flatten()
            .map(|entity| entity.entity_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(selected.len(), inventory.len());
        assert!(partitions.iter().all(|partition| {
            let ids = partition
                .iter()
                .map(|entity| entity.entity_id.as_str())
                .collect::<BTreeSet<_>>();
            partition.iter().all(|entity| {
                entity
                    .possible_duplicate_entity_ids
                    .iter()
                    .all(|candidate| ids.contains(candidate.as_str()))
            })
        }));
    }

    #[test]
    fn wiki_automatic_budget_scales_with_the_complete_graph() {
        assert_eq!(resolve_article_budget(0, 796, 14).expect("budget"), 796);
        assert_eq!(resolve_article_budget(0, 5, 1).expect("small budget"), 5);
        assert!(resolve_article_budget(556, 796, 14).is_err());
        assert_eq!(
            resolve_article_budget(558, 796, 14).expect("70 percent"),
            558
        );
    }

    #[test]
    fn full_wiki_structure_calls_have_room_for_large_corpora() {
        assert_eq!(full_wiki_structure_output_token_budget(12_000), 64_000);
        assert_eq!(full_wiki_structure_output_token_budget(96_000), 96_000);
        assert_eq!(full_wiki_structure_context_char_budget(200_000), 1_000_000);
        assert_eq!(
            full_wiki_structure_context_char_budget(1_500_000),
            1_500_000
        );
        assert_eq!(full_wiki_organizer_timeout_secs(180), 600);
        assert_eq!(full_wiki_organizer_timeout_secs(900), 900);
    }

    #[test]
    fn compact_organizer_inventory_fits_the_supported_full_graph_cap() {
        let inventory = (0..2_000)
            .map(|index| WikiPlanningEntity {
                entity_id: format!("entity:{index:032x}"),
                entity_type: "topic".to_string(),
                display_name: format!("Topic {index}"),
                summary: Some("Supported subject".to_string()),
                graph_degree: 1,
                evidence_assertions: 1,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: Vec::new(),
                connections: Vec::new(),
                evidence_connections: Vec::new(),
            })
            .collect::<Vec<_>>();
        let plan = WikiPlan {
            language: "en".to_string(),
            articles: inventory
                .iter()
                .map(|entity| PlannedWikiArticle {
                    entity_id: entity.entity_id.clone(),
                    title: format!("{} {}", entity.display_name, "x".repeat(180)),
                    editorial_reason: "Supported subject".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 1,
                    page_role: "reference".to_string(),
                    temporal_relevance: "historical".to_string(),
                    related_entity_ids: Vec::new(),
                })
                .collect(),
        };
        let serialized = organizer_inventory_json(&plan, &inventory).expect("organizer inventory");
        assert!(
            serialized.chars().count()
                < full_wiki_structure_context_char_budget(
                    WikiEditorialConfig::default().max_context_chars
                )
        );
        let parsed = serde_json::from_str::<Value>(&serialized).expect("compact payload");
        assert_eq!(parsed["rows"].as_array().map(Vec::len), Some(2_000));
    }

    #[test]
    fn wiki_identity_candidates_may_be_consolidated_but_uncertainty_stays_visible() {
        let first_id = "entity:11111111-1111-4111-8111-111111111111".to_string();
        let second_id = "entity:22222222-2222-4222-8222-222222222222".to_string();
        let make_entity =
            |entity_id: String, duplicate_id: String, display_name: &str| WikiPlanningEntity {
                entity_id,
                entity_type: "person".to_string(),
                display_name: display_name.to_string(),
                summary: Some(format!(
                    "{display_name} is supported by household evidence."
                )),
                graph_degree: 1,
                evidence_assertions: 1,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: vec![duplicate_id],
                connections: Vec::new(),
                evidence_connections: Vec::new(),
            };
        let inventory = vec![
            make_entity(first_id.clone(), second_id.clone(), "Noah"),
            make_entity(second_id.clone(), first_id.clone(), "Noah Example"),
        ];
        let consolidated = WikiPlan {
            language: "en".to_string(),
            articles: vec![PlannedWikiArticle {
                entity_id: first_id.clone(),
                title: "Noah Example".to_string(),
                editorial_reason: "Two candidate records plausibly describe one person."
                    .to_string(),
                supporting_entity_ids: vec![second_id.clone()],
                importance: 5,
                page_role: "core".to_string(),
                temporal_relevance: "enduring".to_string(),
                related_entity_ids: Vec::new(),
            }],
        };
        validate_wiki_plan(&consolidated, &inventory, &WikiCreationConfig::default())
            .expect("explicit identity consolidation");

        let unresolved = WikiPlan {
            language: "en".to_string(),
            articles: inventory
                .iter()
                .map(|entity| PlannedWikiArticle {
                    entity_id: entity.entity_id.clone(),
                    title: entity.display_name.clone(),
                    editorial_reason: "Separate page".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 5,
                    page_role: "core".to_string(),
                    temporal_relevance: "enduring".to_string(),
                    related_entity_ids: Vec::new(),
                })
                .collect(),
        };
        validate_wiki_plan(&unresolved, &inventory, &WikiCreationConfig::default())
            .expect("unadjudicated identity candidates remain separate");
    }

    #[test]
    fn wiki_organization_must_assign_every_article_exactly_once() {
        let plan = WikiPlan {
            language: "en".to_string(),
            articles: vec![
                PlannedWikiArticle {
                    entity_id: "entity:a".to_string(),
                    title: "Eve".to_string(),
                    editorial_reason: "Family context".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 5,
                    page_role: "core".to_string(),
                    temporal_relevance: "enduring".to_string(),
                    related_entity_ids: Vec::new(),
                },
                PlannedWikiArticle {
                    entity_id: "entity:b".to_string(),
                    title: "Home".to_string(),
                    editorial_reason: "Household context".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 5,
                    page_role: "core".to_string(),
                    temporal_relevance: "current".to_string(),
                    related_entity_ids: Vec::new(),
                },
            ],
        };
        let valid = WikiOrganization {
            language: "en".to_string(),
            title: "Household knowledge".to_string(),
            purpose: "Knowledge about this household.".to_string(),
            featured_entity_ids: vec!["entity:a".to_string(), "entity:b".to_string()],
            sections: vec![PlannedWikiSection {
                id: "family-home".to_string(),
                title: "Family and home".to_string(),
                description: "The people and places that define the household.".to_string(),
                article_entity_ids: vec!["entity:a".to_string(), "entity:b".to_string()],
            }],
        };
        validate_wiki_organization(&valid, &plan).expect("complete organization");
        let navigation = organization_navigation(&valid, &plan).expect("navigation");
        assert!(navigation["entity:a"].featured);
        let mut invalid = valid;
        invalid.sections[0].article_entity_ids = vec!["entity:a".to_string()];
        assert!(validate_wiki_organization(&invalid, &plan).is_err());
    }

    #[tokio::test]
    async fn wiki_unchanged_fingerprint_skips_every_llm_call() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default());
        let mut eve = planning_entity("Eve");
        eve.summary = Some("Eve is Noah's daughter and an immediate family member.".to_string());
        EntityRepository::new(storage)
            .create(eve.clone())
            .await
            .expect("entity");
        let config = WikiCreationConfig {
            max_articles: 1,
            publish: true,
            ..WikiCreationConfig::default()
        };
        planner
            .create_from_entities("space", vec![eve.clone()], config.clone())
            .await
            .expect("initial wiki");
        llm.planner_calls.store(0, Ordering::SeqCst);
        llm.organizer_calls.store(0, Ordering::SeqCst);
        llm.writer_calls.store(0, Ordering::SeqCst);
        llm.verifier_calls.store(0, Ordering::SeqCst);
        let candidates = vec![eve];
        let prepared = planner
            .build_planning_inventory("space", &candidates)
            .await
            .expect("inventory");
        let security_key =
            serde_json::to_string(&candidates[0].base.security).expect("security key");
        let mut fingerprint = Sha256::new();
        hash_wiki_editorial_contract(&mut fingerprint);
        hash_wiki_curation_input(
            &mut fingerprint,
            &[
                security_key,
                prepared.receipt.graph_fingerprint,
                prepared.receipt.eligible_entities.to_string(),
                prepared.receipt.planning_partitions.to_string(),
            ],
        );
        let previous = format!("sha256:{}", hex::encode(fingerprint.finalize()));
        let result = planner
            .reconcile_full_graph_if_changed("space", candidates, config, Some(&previous))
            .await
            .expect("unchanged reconciliation");

        assert!(!result.changed);
        assert_eq!(llm.planner_calls.load(Ordering::SeqCst), 0);
        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wiki_graph_change_replans_globally_but_keeps_unchanged_articles_hot() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default());
        let mut eve = planning_entity("Eve");
        eve.summary = Some("Eve is Noah's daughter and an immediate family member.".to_string());
        let mut unrelated = planning_entity("Zebra topic");
        unrelated.summary = Some("An unrelated supported topic.".to_string());
        let repository = EntityRepository::new(storage);
        repository.create(eve.clone()).await.expect("Eve entity");
        repository
            .create(unrelated.clone())
            .await
            .expect("unrelated entity");
        let config = WikiCreationConfig {
            max_articles: 2,
            publish: true,
            ..WikiCreationConfig::default()
        };
        planner
            .create_from_entities(
                "space",
                vec![eve.clone(), unrelated.clone()],
                config.clone(),
            )
            .await
            .expect("initial wiki");
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 2);

        unrelated.summary = Some("The unrelated topic has changed.".to_string());
        planner
            .create_from_entities("space", vec![eve, unrelated], config)
            .await
            .expect("replanned wiki");

        assert_eq!(llm.planner_calls.load(Ordering::SeqCst), 2);
        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 4);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn managed_wiki_rejects_legacy_or_cross_space_authority() {
        let payload_json = json!({
            "max_articles": 24, "article_max_hops": 1, "article_max_members": 250,
            "refresh_interval_minutes": 360, "writer_model": "mock", "verifier_model": "mock"
        });
        assert!(serde_json::from_value::<ManagedWikiPayload>(payload_json.clone()).is_err());
        let mut granted = payload_json;
        granted["authority"] = json!({"kind": "space", "space_id": "authorized-space", "grant_id": RecordId::new_v4(TableName::WorkItem)});
        let payload: ManagedWikiPayload = serde_json::from_value(granted).expect("payload");
        let storage = StorageClient::new_embedded().await.expect("storage");
        let denied = enqueue_managed_wiki_reconciliation(
            &storage,
            "other-space",
            "test",
            &payload,
            Utc::now(),
        )
        .await;
        assert!(matches!(denied, Err(PipelineError::ValidationError(_))));
        assert!(WorkItemRepository::new(storage)
            .find_latest_by_kind("other-space", RECONCILE_LIVING_WIKI_KIND)
            .await
            .expect("queue")
            .is_none());
    }

    #[tokio::test]
    async fn wiki_managed_reconciliation_schedule_is_idempotent_per_interval_bucket() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let payload = ManagedWikiPayload {
            authority: ManagedWikiAuthority::Space {
                space_id: "space".to_string(),
                grant_id: WorkItemRepository::new(storage.clone())
                    .create_managed_wiki_grant("space", "test")
                    .await
                    .expect("grant"),
            },
            max_articles: 24,
            article_max_hops: 1,
            article_max_members: 250,
            refresh_interval_minutes: 360,
            writer_model: "mock-writer".to_string(),
            verifier_model: "mock-verifier".to_string(),
            last_graph_fingerprint: None,
        };
        let run_after = Utc::now() + chrono::Duration::hours(6);
        let first =
            enqueue_managed_wiki_reconciliation(&storage, "space", "test", &payload, run_after)
                .await
                .expect("first schedule");
        let second =
            enqueue_managed_wiki_reconciliation(&storage, "space", "test", &payload, run_after)
                .await
                .expect("second schedule");

        assert_eq!(first.base.id, second.base.id);
        assert_eq!(first.kind, RECONCILE_LIVING_WIKI_KIND);
        assert_eq!(first.run_after, Some(run_after));
    }

    #[tokio::test]
    async fn wiki_successor_does_not_reuse_cancelled_periodic_item_in_same_bucket() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let payload = ManagedWikiPayload {
            authority: ManagedWikiAuthority::Space {
                space_id: "space".to_string(),
                grant_id: WorkItemRepository::new(storage.clone())
                    .create_managed_wiki_grant("space", "test")
                    .await
                    .expect("grant"),
            },
            max_articles: 24,
            article_max_hops: 1,
            article_max_members: 250,
            refresh_interval_minutes: 360,
            writer_model: "mock-writer".to_string(),
            verifier_model: "mock-verifier".to_string(),
            last_graph_fingerprint: None,
        };
        let run_after = Utc::now() + chrono::Duration::hours(5);
        let periodic =
            enqueue_managed_wiki_reconciliation(&storage, "space", "test", &payload, run_after)
                .await
                .expect("periodic schedule");
        WorkItemRepository::new(storage.clone())
            .cancel_queued_by_kind_except(
                "space",
                RECONCILE_LIVING_WIKI_KIND,
                &RecordId::new_v4(TableName::WorkItem),
            )
            .await
            .expect("cancel periodic");
        let predecessor = RecordId::new_v4(TableName::WorkItem);
        let successor = enqueue_managed_wiki_successor(
            &storage,
            "space",
            "test",
            &predecessor,
            &payload,
            run_after,
        )
        .await
        .expect("successor schedule");
        let repeated = enqueue_managed_wiki_successor(
            &storage,
            "space",
            "test",
            &predecessor,
            &payload,
            run_after,
        )
        .await
        .expect("repeated successor schedule");

        assert_ne!(successor.base.id, periodic.base.id);
        assert_eq!(successor.base.id, repeated.base.id);
        assert_eq!(successor.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    async fn wiki_graph_change_replaces_periodic_work_and_forces_reconciliation() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let payload = ManagedWikiPayload {
            authority: ManagedWikiAuthority::Space {
                space_id: "space".to_string(),
                grant_id: WorkItemRepository::new(storage.clone())
                    .create_managed_wiki_grant("space", "test")
                    .await
                    .expect("grant"),
            },
            max_articles: 0,
            article_max_hops: 1,
            article_max_members: 250,
            refresh_interval_minutes: 360,
            writer_model: "mock-writer".to_string(),
            verifier_model: "mock-verifier".to_string(),
            last_graph_fingerprint: Some("sha256:old".to_string()),
        };
        let periodic = enqueue_managed_wiki_reconciliation(
            &storage,
            "space",
            "test",
            &payload,
            Utc::now() + chrono::Duration::hours(6),
        )
        .await
        .expect("periodic work");
        let ignored = enqueue_managed_wiki_graph_change(
            &storage,
            "space",
            "test",
            periodic.base.created_at - chrono::Duration::seconds(1),
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("historical outbox event");
        assert!(ignored.is_none());

        let replacement = enqueue_managed_wiki_graph_change(
            &storage,
            "space",
            "test",
            periodic.base.created_at + chrono::Duration::seconds(1),
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("new graph event")
        .expect("replacement work");
        let replacement_payload =
            serde_json::from_value::<ManagedWikiPayload>(replacement.payload.clone())
                .expect("replacement payload");
        assert!(replacement_payload.last_graph_fingerprint.is_none());
        assert_eq!(replacement_payload.writer_model, payload.writer_model);
        let original = WorkItemRepository::new(storage)
            .get("space", &periodic.base.id)
            .await
            .expect("original lookup")
            .expect("original work");
        assert_eq!(original.status, WorkItemStatus::Cancelled);
    }

    #[tokio::test]
    async fn second_graph_change_in_same_bucket_keeps_a_runnable_reconciliation() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repository = WorkItemRepository::new(storage.clone());
        let payload = ManagedWikiPayload {
            authority: ManagedWikiAuthority::Space {
                space_id: "space".to_string(),
                grant_id: repository
                    .create_managed_wiki_grant("space", "test")
                    .await
                    .expect("grant"),
            },
            max_articles: 0,
            article_max_hops: 1,
            article_max_members: 250,
            refresh_interval_minutes: 360,
            writer_model: "mock".to_string(),
            verifier_model: "mock".to_string(),
            last_graph_fingerprint: None,
        };
        let now = Utc::now();
        let deadline = now + chrono::Duration::seconds(30);
        let periodic = enqueue_managed_wiki_reconciliation(
            &storage,
            "space",
            "test",
            &payload,
            now + chrono::Duration::hours(6),
        )
        .await
        .expect("periodic");
        let mut first = enqueue_managed_wiki_graph_change(
            &storage,
            "space",
            "test",
            periodic.base.created_at + chrono::Duration::seconds(1),
            deadline,
        )
        .await
        .expect("change")
        .expect("first");
        let future = enqueue_managed_wiki_successor(
            &storage,
            "space",
            "test",
            &first.base.id,
            &payload,
            now + chrono::Duration::hours(6),
        )
        .await
        .expect("successor");
        first.status = WorkItemStatus::Done;
        first.base.updated_at = future.base.updated_at + chrono::Duration::seconds(1);
        repository
            .update("space", &first.base.id, first.clone())
            .await
            .expect("complete");
        let replacement = enqueue_managed_wiki_graph_change(
            &storage,
            "space",
            "test",
            first.base.created_at + chrono::Duration::seconds(2),
            deadline,
        )
        .await
        .expect("second change")
        .expect("replacement");
        assert_eq!(replacement.status, WorkItemStatus::Runnable);
        assert_ne!(replacement.base.id, first.base.id);
        assert_eq!(
            repository
                .get("space", &replacement.base.id)
                .await
                .expect("lookup")
                .expect("work")
                .status,
            WorkItemStatus::Runnable
        );
    }

    #[tokio::test]
    async fn llm_article_is_verified_published_and_memoized() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let definition_id = seeded_definition(&storage).await;

        let llm = Arc::new(MockEditorialLlm::default());
        let compiler = WikiEditorialCompiler::new(
            storage.clone(),
            llm.clone(),
            WikiEditorialConfig {
                writer_model: "mock-writer".to_string(),
                verifier_model: "mock-verifier".to_string(),
                ..WikiEditorialConfig::default()
            },
        );
        let first = compiler
            .compile("space", &definition_id, true)
            .await
            .expect("first compile");
        assert!(first.changed);
        assert_eq!(first.view.kind, KnowledgeViewKind::Article);
        assert_eq!(first.view.surface, KnowledgeViewSurface::Wiki);
        assert_eq!(
            first.view.publication_status,
            KnowledgeViewPublicationStatus::Published
        );
        assert_eq!(
            first
                .view
                .base
                .provenance
                .extractor
                .as_ref()
                .and_then(|extractor| extractor.name.as_deref()),
            Some(ARTICLE_COMPILER_NAME)
        );
        assert!(first
            .revision
            .blocks
            .iter()
            .flat_map(|block| &block.statements)
            .all(|statement| {
                statement.derivation == StatementDerivation::Synthesized
                    && statement.verification == StatementVerification::EvidenceEntailment
            }));

        let second = compiler
            .compile("space", &definition_id, true)
            .await
            .expect("memoized compile");
        assert!(!second.changed);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 1);
        let listed = KnowledgeViewRepository::new(storage)
            .list_views_for_surface(
                "space",
                KnowledgeViewSurface::Wiki,
                KnowledgeViewPublicationStatus::Published,
                10,
            )
            .await
            .expect("wiki list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].base.id, first.view.base.id);
    }

    #[tokio::test]
    async fn wiki_planner_persists_llm_organized_reader_navigation() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let mut eve = planning_entity("Eve");
        eve.summary = Some("Eve is Noah's daughter and an immediate family member.".to_string());
        EntityRepository::new(storage.clone())
            .create(eve.clone())
            .await
            .expect("entity");
        let llm = Arc::new(MockEditorialLlm::default());
        let planner = WikiGraphPlanner::new(storage.clone(), llm, WikiEditorialConfig::default());
        let result = planner
            .create_from_entities(
                "space",
                vec![eve],
                WikiCreationConfig {
                    max_articles: 1,
                    publish: true,
                    ..WikiCreationConfig::default()
                },
            )
            .await
            .expect("created wiki");
        assert_eq!(result.articles.len(), 1);
        let definition_id = result.articles[0]
            .view
            .root_record
            .parse::<RecordId>()
            .expect("definition id");
        let definition = KnowledgeViewDefinitionRepository::new(storage)
            .get("space", &definition_id)
            .await
            .expect("definition lookup")
            .expect("definition");
        let navigation = definition.navigation.expect("organized navigation");
        assert_eq!(navigation.wiki_title, "Household knowledge");
        assert_eq!(navigation.section_title, "Household");
        assert_eq!(navigation.section_order, 0);
        assert_eq!(navigation.article_order, 0);
    }

    #[tokio::test]
    async fn verifier_rejection_is_repaired_before_publication() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let definition_id = seeded_definition(&storage).await;
        let llm = Arc::new(MockEditorialLlm {
            reject_first: true,
            ..MockEditorialLlm::default()
        });
        let compiler = WikiEditorialCompiler::new(
            storage,
            llm.clone(),
            WikiEditorialConfig {
                writer_model: "mock-writer".to_string(),
                verifier_model: "mock-verifier".to_string(),
                max_editorial_attempts: 2,
                ..WikiEditorialConfig::default()
            },
        );
        let result = compiler
            .compile("space", &definition_id, true)
            .await
            .expect("repaired article");

        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 2);
        assert!(result
            .revision
            .blocks
            .iter()
            .flat_map(|block| &block.statements)
            .all(|statement| !statement.text.contains("matters more")));
    }

    #[test]
    fn alternate_verifier_wire_shape_preserves_every_judgment() {
        let verification = decode_editorial_verification(serde_json::json!({
            "statement_decisions": [{
                "id": "lead",
                "entailed": false,
                "relevant": true
            }],
            "whole_article_judgment": {
                "article_useful": true,
                "article_coherent": true,
                "temporal_framing_sound": false,
                "missing_major_topics": ["Current family context"]
            }
        }))
        .expect("alternate verifier wire shape");

        assert_eq!(verification.decisions.len(), 1);
        assert_eq!(verification.decisions[0].statement_id, "lead");
        assert!(!verification.decisions[0].entailed);
        assert!(verification.decisions[0].relevant);
        assert_eq!(
            verification.decisions[0].reason,
            "Verifier returned entailed=false and relevant=true for this statement."
        );
        assert!(verification.article_useful);
        assert!(verification.article_coherent);
        assert!(!verification.temporal_framing_sound);
        assert_eq!(
            verification.missing_major_topics,
            ["Current family context"]
        );
        assert_eq!(
            verification.article_reason,
            "Verifier returned a negative whole-article judgment."
        );
    }

    #[test]
    fn verifier_rejects_missing_and_unsupported_decisions() {
        let draft = EditorialDraft {
            language: "en".to_string(),
            title: "Eve".to_string(),
            lead: EditorialStatement {
                id: "lead-1".to_string(),
                text: "Eve is a person.".to_string(),
                citation_ids: vec!["c1".to_string()],
            },
            sections: vec![EditorialSection {
                id: "current".to_string(),
                heading: "Current".to_string(),
                kind: "current_facts".to_string(),
                statements: vec![EditorialStatement {
                    id: "current-1".to_string(),
                    text: "Eve attends school.".to_string(),
                    citation_ids: vec!["c2".to_string()],
                }],
            }],
        };
        let missing = EditorialVerification {
            decisions: vec![StatementVerificationDecision {
                statement_id: "lead-1".to_string(),
                entailed: true,
                relevant: true,
                reason: "Supported".to_string(),
            }],
            article_useful: true,
            article_coherent: true,
            temporal_framing_sound: true,
            missing_major_topics: Vec::new(),
            article_reason: "Useful".to_string(),
        };
        assert!(validate_verification(&draft, &missing).is_err());

        let rejected = EditorialVerification {
            decisions: vec![
                StatementVerificationDecision {
                    statement_id: "lead-1".to_string(),
                    entailed: true,
                    relevant: true,
                    reason: "Supported".to_string(),
                },
                StatementVerificationDecision {
                    statement_id: "current-1".to_string(),
                    entailed: false,
                    relevant: true,
                    reason: "No current date".to_string(),
                },
            ],
            article_useful: true,
            article_coherent: true,
            temporal_framing_sound: true,
            missing_major_topics: Vec::new(),
            article_reason: "Useful".to_string(),
        };
        assert!(validate_verification(&draft, &rejected).is_err());

        let irrelevant = EditorialVerification {
            decisions: vec![
                StatementVerificationDecision {
                    statement_id: "lead-1".to_string(),
                    entailed: true,
                    relevant: true,
                    reason: "Supported and on topic".to_string(),
                },
                StatementVerificationDecision {
                    statement_id: "current-1".to_string(),
                    entailed: true,
                    relevant: false,
                    reason: "Supported but unrelated to Eve".to_string(),
                },
            ],
            article_useful: true,
            article_coherent: true,
            temporal_framing_sound: true,
            missing_major_topics: Vec::new(),
            article_reason: "Useful".to_string(),
        };
        assert!(validate_verification(&draft, &irrelevant).is_err());
    }

    #[test]
    fn editorial_validation_rejects_type_only_and_context_filler_leads() {
        assert!(validate_meaningful_lead("Alice Example is a person.").is_err());
        assert!(validate_reader_facing_prose(
            "lead-1",
            "Flash Attention is a central topic in this editorial context."
        )
        .is_err());
        assert!(validate_meaningful_lead(
            "Eve is Noah's daughter and an immediate member of Noah's family."
        )
        .is_ok());
        assert!(validate_reader_facing_prose(
            "uncertainty",
            "Available information does not establish whether Atlas is a person or a project."
        )
        .is_ok());

        let identity_only = EditorialEvidenceItem {
            citation_id: "c1".to_string(),
            kind: CitationKind::GraphRecord,
            record_id: "entity:eve".to_string(),
            canonical_statements: vec![
                "**Eve** is Person at the center of this editorial context.".to_string(),
            ],
            source_quote: None,
            source_id: None,
        };
        let relationship = EditorialEvidenceItem {
            citation_id: "c2".to_string(),
            kind: CitationKind::GraphRecord,
            record_id: "rel:parent".to_string(),
            canonical_statements: vec!["Eve is Noah's daughter.".to_string()],
            source_quote: None,
            source_id: None,
        };
        assert!(!evidence_item_is_meaningful(&identity_only));
        assert!(evidence_item_is_meaningful(&relationship));

        let stale_span = EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 9,
            quote_hash: "sha256:stale".to_string(),
        };
        let reconciled =
            reconciled_evidence_span("Eve likes music.", &stale_span).expect("valid span");
        assert_ne!(reconciled.quote_hash, stale_span.quote_hash);
        assert_eq!(
            validated_quote("Eve likes music.", &reconciled).expect("validated quote"),
            "Eve likes"
        );

        let joined = restrictive_security_join(
            &Security {
                policy_id: "family".to_string(),
                level: 1,
                labels: vec!["home".to_string()],
                handling: Vec::new(),
            },
            &Security {
                policy_id: "family".to_string(),
                level: 3,
                labels: vec!["school".to_string()],
                handling: vec!["private".to_string()],
            },
        );
        assert_eq!(joined.level, 3);
        assert_eq!(joined.labels, vec!["home", "school"]);
        assert_eq!(joined.handling, vec!["private"]);

        let promoted = policy_aware_security_join(
            &Security {
                policy_id: "default".to_string(),
                ..Security::default()
            },
            &Security {
                policy_id: "household-v1".to_string(),
                labels: vec!["audience:household".to_string()],
                ..Security::default()
            },
        )
        .expect("default policy promotion");
        assert_eq!(promoted.policy_id, "household-v1");
        assert_eq!(promoted.labels, vec!["audience:household"]);
        assert!(policy_aware_security_join(
            &Security {
                policy_id: "household-v1".to_string(),
                ..Security::default()
            },
            &Security {
                policy_id: "corporate-v1".to_string(),
                ..Security::default()
            }
        )
        .is_none());
    }

    #[test]
    fn verifier_treats_sparse_local_relevance_as_useful_private_wiki_content() {
        let draft = EditorialDraft {
            language: "en".to_string(),
            title: "Radiohead".to_string(),
            lead: EditorialStatement {
                id: "lead".to_string(),
                text: "Anna likes Radiohead.".to_string(),
                citation_ids: vec!["c1".to_string()],
            },
            sections: vec![EditorialSection {
                id: "local-relevance".to_string(),
                heading: "In our world".to_string(),
                kind: "relationships".to_string(),
                statements: vec![EditorialStatement {
                    id: "anna".to_string(),
                    text: "Anna has said that Radiohead is her favourite band.".to_string(),
                    citation_ids: vec!["c1".to_string()],
                }],
            }],
        };
        let prompt = render_verifier_prompt(&draft, "[]").expect("verifier prompt");
        assert!(prompt.contains("This is a private wiki, not a general encyclopedia"));
        assert!(prompt.contains("one concrete, entailed local-relevance fact is sufficient"));
    }

    #[test]
    fn writer_schema_requires_statement_citations() {
        let schema = article_schema();
        assert_eq!(
            schema["properties"]["lead"]["required"],
            json!(["id", "text", "citation_ids"])
        );
        assert_eq!(schema["properties"]["sections"]["minItems"], 1);
        assert_eq!(
            schema["properties"]["sections"]["items"]["properties"]["statements"]["minItems"],
            1
        );
    }

    #[test]
    fn planner_schema_requires_english_and_canonical_subjects() {
        let schema = wiki_plan_schema();
        assert_eq!(schema["properties"]["language"]["enum"], json!(["en"]));
        assert_eq!(
            schema["properties"]["articles"]["items"]["required"],
            json!([
                "entity_id",
                "title",
                "editorial_reason",
                "supporting_entity_ids",
                "importance",
                "page_role",
                "temporal_relevance",
                "related_entity_ids"
            ])
        );
    }

    #[test]
    fn planner_treats_omitted_navigation_links_as_empty() {
        let plan: WikiPlan = serde_json::from_value(json!({
            "language": "en",
            "articles": [{
                "entity_id": "entity:11111111-1111-4111-8111-111111111111",
                "title": "Eve",
                "editorial_reason": "Central household context",
                "importance": 5,
                "page_role": "core",
                "temporal_relevance": "enduring"
            }]
        }))
        .expect("plan without optional navigation links");
        assert!(plan.articles[0].related_entity_ids.is_empty());
    }

    #[test]
    fn wiki_plan_links_only_selected_canonical_entities() {
        let inventory = vec![
            WikiPlanningEntity {
                entity_id: "entity:11111111-1111-4111-8111-111111111111".to_string(),
                entity_type: "person".to_string(),
                display_name: "Eve".to_string(),
                summary: Some("Eve is a member of the family.".to_string()),
                graph_degree: 3,
                evidence_assertions: 5,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: Vec::new(),
                connections: vec![WikiPlanningConnection {
                    predicate: "lives_in".to_string(),
                    entity_id: "entity:22222222-2222-4222-8222-222222222222".to_string(),
                    display_name: "Lakeside".to_string(),
                }],
                evidence_connections: Vec::new(),
            },
            WikiPlanningEntity {
                entity_id: "entity:22222222-2222-4222-8222-222222222222".to_string(),
                entity_type: "place".to_string(),
                display_name: "Lakeside".to_string(),
                summary: Some("Lakeside is connected to Eve.".to_string()),
                graph_degree: 1,
                evidence_assertions: 1,
                attribute_facts: 0,
                events: 0,
                knowledge_issues: 0,
                earliest_evidence_at: None,
                latest_evidence_at: None,
                possible_duplicate_entity_ids: Vec::new(),
                connections: Vec::new(),
                evidence_connections: vec![WikiPlanningEvidenceConnection {
                    entity_id: "entity:11111111-1111-4111-8111-111111111111".to_string(),
                    display_name: "Eve".to_string(),
                    assertion_count: 2,
                }],
            },
        ];
        let mut plan = WikiPlan {
            language: "en".to_string(),
            articles: vec![
                PlannedWikiArticle {
                    entity_id: inventory[0].entity_id.clone(),
                    title: "Eve".to_string(),
                    editorial_reason: "Central person".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 5,
                    page_role: "core".to_string(),
                    temporal_relevance: "enduring".to_string(),
                    related_entity_ids: Vec::new(),
                },
                PlannedWikiArticle {
                    entity_id: inventory[1].entity_id.clone(),
                    title: "Lakeside".to_string(),
                    editorial_reason: "Relevant place".to_string(),
                    supporting_entity_ids: Vec::new(),
                    importance: 4,
                    page_role: "topic".to_string(),
                    temporal_relevance: "enduring".to_string(),
                    related_entity_ids: Vec::new(),
                },
            ],
        };
        augment_plan_navigation(&mut plan, &inventory, 12);
        assert_eq!(
            plan.articles[0].related_entity_ids,
            vec![inventory[1].entity_id.clone()]
        );
        assert_eq!(
            plan.articles[1].related_entity_ids,
            vec![inventory[0].entity_id.clone()]
        );
        assert!(validate_wiki_plan(&plan, &inventory, &WikiCreationConfig::default()).is_ok());
        assert_eq!(
            publishable_relation_types(&plan.articles[0], &inventory),
            vec!["lives_in".to_string()]
        );
        assert_eq!(
            publishable_relation_types(&plan.articles[1], &inventory),
            vec![NO_PUBLISHABLE_RELATIONS.to_string()]
        );

        plan.articles[0].related_entity_ids =
            vec!["entity:33333333-3333-4333-8333-333333333333".to_string()];
        assert!(validate_wiki_plan(&plan, &inventory, &WikiCreationConfig::default()).is_err());
    }
}
