//! LLM slot adjudication for truth maintenance conflicts.
//!
//! The adjudicator sees the actual claim values (unlike the old structural
//! reasoner) and returns a structured verdict: corroborate, single winner,
//! coexist, supersede, or needs review. It is invoked through the
//! [`SlotAdjudicator`] trait so tests inject fakes and keyless deployments
//! fall back to deterministic policies.

#[cfg(any(feature = "meerkat-extraction", test))]
use std::collections::HashSet;
#[cfg(any(feature = "meerkat-extraction", test))]
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(any(feature = "meerkat-extraction", test))]
use chrono::{NaiveDate, NaiveDateTime};
use elephant_types::records::attribute::Literal;
use elephant_types::records::predicate::{Cardinality, PredicateSemantics};
use elephant_types::records::temporal::TemporalPersonality;
use elephant_types::records::truth::ConflictPolicy;
#[cfg(any(feature = "meerkat-extraction", test))]
use serde::Deserialize;

use crate::error::PipelineError;

const ADJUDICATION_PROMPT_TEMPLATE: &str = include_str!("../../prompts/tm_adjudication.txt");

#[cfg(feature = "meerkat-extraction")]
const ADJUDICATION_MIN_BUDGET_TOKENS: u64 = 16_000;

/// The value carried by a claim, rendered for the adjudicator.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimValue {
    /// A literal attribute value.
    Literal(Literal),
    /// A reference to an object entity (rel claims).
    EntityRef {
        /// Entity record id.
        id: String,
        /// Human-readable display name.
        display: String,
    },
    /// An event claim.
    EventClaim {
        /// Event title.
        title: String,
        /// Event start time, if known.
        time_start: Option<DateTime<Utc>>,
        /// Participant display names.
        participants: Vec<String>,
        /// Event status label.
        status: String,
    },
}

/// A single claim presented to the adjudicator.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjudicationClaim {
    /// Claim record id.
    pub claim_id: String,
    /// The claim's value.
    pub value: ClaimValue,
    /// Claim confidence (0.0 to 1.0).
    pub confidence: f32,
    /// Number of supporting evidence references.
    pub evidence_count: usize,
    /// Recency timestamp for the claim.
    pub timestamp: DateTime<Utc>,
    /// Human label of the recency basis behind `timestamp`.
    pub time_basis: String,
    /// Source identifiers backing the claim.
    pub sources: Vec<String>,
    /// Rendered evidence assertions backing the claim (what the sources said).
    pub evidence_notes: Vec<String>,
}

/// Pure-data input for one adjudication call.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjudicationInput {
    /// Space the slot belongs to.
    pub space_id: String,
    /// Slot key of the conflict group.
    pub slot_key: String,
    /// Slot type: `"rel"`, `"attribute"`, `"event"`, or `"rel_set"`.
    pub slot_type: String,
    /// Display name of the subject entity.
    pub subject_display: String,
    /// Canonical predicate id.
    pub predicate_id: String,
    /// Registered predicate semantics, if any.
    pub semantics: Option<PredicateSemantics>,
    /// The competing claims.
    pub claims: Vec<AdjudicationClaim>,
}

/// Decision returned by the adjudicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjudicationDecision {
    /// All claims state the same fact; merge evidence.
    Corroborate,
    /// Exactly one claim is correct; the others are wrong.
    SingleWinner,
    /// Multiple values are legitimately true simultaneously.
    Coexist,
    /// Older claims were true; the winner is now current.
    Supersede,
    /// Genuinely undecidable; flag for review.
    NeedsReview,
}

/// A claim marked as superseded by the verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersededClaim {
    /// Claim record id.
    pub claim_id: String,
    /// When the claim stopped being true, if the values state it.
    pub valid_until: Option<DateTime<Utc>>,
}

/// Predicate semantics proposed by the adjudicator when none are registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposedSemantics {
    /// Proposed cardinality.
    pub cardinality: Cardinality,
    /// Proposed temporal model.
    pub temporal_model: TemporalPersonality,
    /// Proposed conflict policy (`split_by_context` is not allowed).
    pub conflict_policy: ConflictPolicy,
}

/// Validated adjudication verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjudicationVerdict {
    /// The decision.
    pub decision: AdjudicationDecision,
    /// Winner claim id (required for `SingleWinner` and `Supersede`).
    pub winner_id: Option<String>,
    /// Partition of claim ids into value-equivalent groups (may be empty).
    pub equivalent_groups: Vec<Vec<String>>,
    /// Claims that are no longer current.
    pub superseded: Vec<SupersededClaim>,
    /// When the winner became true, if the values state it.
    pub winner_valid_from: Option<DateTime<Utc>>,
    /// Semantics proposal (required when the input has no semantics).
    pub proposed_semantics: Option<ProposedSemantics>,
    /// Short explanation of the verdict.
    pub rationale: String,
}

/// Configuration for LLM adjudication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjudicationConfig {
    /// Model identifier.
    pub model: String,
    /// Maximum output tokens per call.
    pub max_tokens: u32,
    /// Wall-clock budget per call in seconds.
    pub max_wall_time_secs: u64,
}

impl Default for AdjudicationConfig {
    fn default() -> Self {
        Self {
            model: "claude-sonnet-4-5".to_string(),
            max_tokens: 2000,
            max_wall_time_secs: 120,
        }
    }
}

/// Adjudicates competing claims for a truth slot.
#[async_trait]
pub trait SlotAdjudicator: Send + Sync + std::fmt::Debug {
    /// Adjudicates the claims in `input`.
    ///
    /// Returns `Ok(None)` when no verdict is available (provider failure,
    /// timeout, or malformed/invalid output); the caller then falls back to
    /// the deterministic policy.
    ///
    /// # Errors
    ///
    /// Returns an error only for failures unrelated to the LLM call itself;
    /// implementations must not stall the pipeline on LLM failures.
    async fn adjudicate(
        &self,
        input: &AdjudicationInput,
    ) -> Result<Option<AdjudicationVerdict>, PipelineError>;

    /// Identifier for the adjudicator implementation.
    fn id(&self) -> &'static str;
}

/// Renders the adjudication prompt for the given input.
///
/// Public so the calibration harness can inspect and score prompt versions.
#[must_use]
pub fn render_adjudication_prompt(input: &AdjudicationInput) -> String {
    let semantics = input.semantics.as_ref().map_or_else(
        || "(none registered)".to_string(),
        |semantics| {
            format!(
                "cardinality={}, temporal_model={}, conflict_policy={}",
                semantics.cardinality, semantics.temporal_model, semantics.conflict_policy
            )
        },
    );
    let semantics_requirement = if input.semantics.is_some() {
        "- proposed_semantics: omit it unless the claim values clearly contradict the \
         registered semantics shown above."
    } else {
        "- proposed_semantics is REQUIRED because this predicate has no registered semantics: \
         propose cardinality (\"one\" or \"many\"), temporal_model (\"persistent\", \
         \"state_over_time\", or \"event_like\"), and conflict_policy (\"allow_parallel\", \
         \"supersede_by_recency\", or \"require_more_evidence\"). \"split_by_context\" is \
         not allowed."
    };
    let claims = input
        .claims
        .iter()
        .map(render_claim)
        .collect::<Vec<_>>()
        .join("\n");

    ADJUDICATION_PROMPT_TEMPLATE
        .replace("{subject_display}", &input.subject_display)
        .replace("{predicate_id}", &input.predicate_id)
        .replace("{slot_type}", &input.slot_type)
        .replace("{semantics}", &semantics)
        .replace("{claims}", &claims)
        .replace("{semantics_requirement}", semantics_requirement)
}

/// Bases that describe when a fact actually held; every other basis is an
/// ingestion/processing artifact.
const FACT_TIME_BASES: [&str; 2] = ["source_event_time", "event_start"];

fn render_claim(claim: &AdjudicationClaim) -> String {
    let sources = if claim.sources.is_empty() {
        "(none)".to_string()
    } else {
        claim.sources.join(", ")
    };
    let evidence = if claim.evidence_notes.is_empty() {
        "(none)".to_string()
    } else {
        claim
            .evidence_notes
            .iter()
            .map(|note| format!("\n    * {note}"))
            .collect::<String>()
    };
    // Ingestion/processing timestamps are withheld entirely: models reliably
    // misread them as fact recency no matter how the rules qualify them.
    let fact_time = if FACT_TIME_BASES.contains(&claim.time_basis.as_str()) {
        format!(
            "{} (basis: {})",
            claim.timestamp.to_rfc3339(),
            claim.time_basis
        )
    } else {
        "unknown (this claim carries no fact-time evidence; do not assume it is \
         recent or current)"
            .to_string()
    };
    format!(
        "- claim_id: {}\n  value: {}\n  confidence: {:.2}\n  evidence_count: {}\n  \
         fact_time: {}\n  sources: {}\n  evidence: {}",
        claim.claim_id,
        render_claim_value(&claim.value),
        claim.confidence,
        claim.evidence_count,
        fact_time,
        sources,
        evidence,
    )
}

fn render_claim_value(value: &ClaimValue) -> String {
    match value {
        ClaimValue::Literal(literal) => render_literal(literal),
        ClaimValue::EntityRef { id, display } => format!("entity \"{display}\" (id: {id})"),
        ClaimValue::EventClaim {
            title,
            time_start,
            participants,
            status,
        } => {
            let time = time_start.map_or_else(|| "(unknown)".to_string(), |t| t.to_rfc3339());
            let participants = if participants.is_empty() {
                "(none)".to_string()
            } else {
                participants.join(", ")
            };
            format!(
                "event \"{title}\" | time_start: {time} | participants: {participants} | status: {status}"
            )
        }
    }
}

fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::String(v) => format!("string \"{v}\""),
        Literal::Integer(v) => format!("integer {v}"),
        Literal::Float(v) => format!("float {v}"),
        Literal::Boolean(v) => format!("boolean {v}"),
        Literal::Date(v) => format!("date {v}"),
        Literal::DateTime(v) => format!("datetime {v}"),
        Literal::Duration(v) => format!("duration {v}"),
        Literal::Json(v) => format!("json {v}"),
    }
}

/// JSON schema for the adjudication verdict (meerkat strict structured output).
///
/// Public so the calibration harness can reuse the exact production schema.
#[must_use]
pub fn verdict_json_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "decision": {
                "type": "string",
                "enum": ["corroborate", "single_winner", "coexist", "supersede", "needs_review"]
            },
            "winner_id": { "type": "string" },
            "equivalent_groups": {
                "type": "array",
                "items": { "type": "array", "items": { "type": "string" } }
            },
            "superseded": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "claim_id": { "type": "string" },
                        "valid_until": { "type": "string" }
                    },
                    "required": ["claim_id"],
                    "additionalProperties": false
                }
            },
            "winner_valid_from": { "type": "string" },
            "proposed_semantics": {
                "type": "object",
                "properties": {
                    "cardinality": { "type": "string", "enum": ["one", "many"] },
                    "temporal_model": {
                        "type": "string",
                        "enum": ["persistent", "state_over_time", "event_like"]
                    },
                    "conflict_policy": {
                        "type": "string",
                        "enum": ["allow_parallel", "supersede_by_recency", "require_more_evidence"]
                    }
                },
                "required": ["cardinality", "temporal_model", "conflict_policy"],
                "additionalProperties": false
            },
            "rationale": { "type": "string" }
        },
        "required": ["decision", "rationale", "equivalent_groups"],
        "additionalProperties": false
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[derive(Debug, Deserialize)]
struct RawVerdict {
    decision: String,
    #[serde(default)]
    winner_id: Option<String>,
    #[serde(default)]
    equivalent_groups: Vec<Vec<String>>,
    #[serde(default)]
    superseded: Vec<RawSupersededClaim>,
    #[serde(default)]
    winner_valid_from: Option<String>,
    #[serde(default)]
    proposed_semantics: Option<RawProposedSemantics>,
    rationale: String,
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[derive(Debug, Deserialize)]
struct RawSupersededClaim {
    claim_id: String,
    #[serde(default)]
    valid_until: Option<String>,
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[derive(Debug, Deserialize)]
struct RawProposedSemantics {
    cardinality: String,
    temporal_model: String,
    conflict_policy: String,
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_decision(value: &str) -> Option<AdjudicationDecision> {
    match value.trim().to_ascii_lowercase().as_str() {
        "corroborate" => Some(AdjudicationDecision::Corroborate),
        "single_winner" => Some(AdjudicationDecision::SingleWinner),
        "coexist" => Some(AdjudicationDecision::Coexist),
        "supersede" => Some(AdjudicationDecision::Supersede),
        "needs_review" => Some(AdjudicationDecision::NeedsReview),
        _ => None,
    }
}

/// Parses an ISO-8601 date or datetime; date-only values become midnight UTC
/// and bare years become January 1st midnight UTC.
#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_verdict_datetime(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Some(naive.and_utc());
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return date.and_hms_opt(0, 0, 0).map(|naive| naive.and_utc());
    }
    if trimmed.len() == 4 && trimmed.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(year) = trimmed.parse::<i32>() {
            return NaiveDate::from_ymd_opt(year, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|naive| naive.and_utc());
        }
    }
    None
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_optional_datetime(
    value: Option<&str>,
    field: &str,
) -> Result<Option<DateTime<Utc>>, String> {
    value.map_or(Ok(None), |raw| {
        parse_verdict_datetime(raw).map_or_else(
            || {
                Err(format!(
                    "{field} '{raw}' is not an ISO-8601 date or datetime"
                ))
            },
            |parsed| Ok(Some(parsed)),
        )
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn convert_proposed_semantics(raw: &RawProposedSemantics) -> Result<ProposedSemantics, String> {
    let cardinality = Cardinality::from_str(raw.cardinality.trim())
        .map_err(|e| format!("invalid proposed cardinality: {e}"))?;
    let temporal_model = TemporalPersonality::from_str(raw.temporal_model.trim())
        .map_err(|e| format!("invalid proposed temporal_model: {e}"))?;
    let conflict_policy = ConflictPolicy::from_str(raw.conflict_policy.trim())
        .map_err(|e| format!("invalid proposed conflict_policy: {e}"))?;
    if conflict_policy == ConflictPolicy::SplitByContext {
        return Err("proposed conflict_policy split_by_context is not allowed".to_string());
    }
    Ok(ProposedSemantics {
        cardinality,
        temporal_model,
        conflict_policy,
    })
}

/// Normalizes `equivalent_groups` into a full partition of the claim ids.
///
/// Empty inner groups are dropped. A non-empty set of groups must contain no
/// duplicates and no unknown ids; claim ids the model did not mention are
/// appended as singleton groups.
#[cfg(any(feature = "meerkat-extraction", test))]
fn normalize_equivalent_groups(
    groups: Vec<Vec<String>>,
    input: &AdjudicationInput,
) -> Result<Vec<Vec<String>>, String> {
    let groups: Vec<Vec<String>> = groups.into_iter().filter(|g| !g.is_empty()).collect();
    if groups.is_empty() {
        return Ok(Vec::new());
    }

    let known: HashSet<&str> = input.claims.iter().map(|c| c.claim_id.as_str()).collect();
    let mut seen: HashSet<String> = HashSet::new();
    for group in &groups {
        for id in group {
            if !known.contains(id.as_str()) {
                return Err(format!(
                    "equivalent_groups references unknown claim_id '{id}'"
                ));
            }
            if !seen.insert(id.clone()) {
                return Err(format!(
                    "claim_id '{id}' appears in more than one equivalence group"
                ));
            }
        }
    }

    let mut normalized = groups;
    for claim in &input.claims {
        if !seen.contains(&claim.claim_id) {
            normalized.push(vec![claim.claim_id.clone()]);
        }
    }
    Ok(normalized)
}

/// Validates a raw verdict against the input claims and converts it.
#[cfg(any(feature = "meerkat-extraction", test))]
fn validate_verdict(
    raw: RawVerdict,
    input: &AdjudicationInput,
) -> Result<AdjudicationVerdict, String> {
    let decision = parse_decision(&raw.decision)
        .ok_or_else(|| format!("unknown decision '{}'", raw.decision))?;

    let known: HashSet<&str> = input.claims.iter().map(|c| c.claim_id.as_str()).collect();

    if let Some(winner) = raw.winner_id.as_deref() {
        if !known.contains(winner) {
            return Err(format!("winner_id '{winner}' is not a claim in the group"));
        }
    }
    if matches!(
        decision,
        AdjudicationDecision::SingleWinner | AdjudicationDecision::Supersede
    ) && raw.winner_id.is_none()
    {
        return Err(format!("decision '{}' requires winner_id", raw.decision));
    }

    let equivalent_groups = normalize_equivalent_groups(raw.equivalent_groups, input)?;

    // A corroborate decision asserts every claim states the same fact; a
    // partition with more than one group contradicts it (the merge-all
    // mapping would silently delete a competing value).
    if decision == AdjudicationDecision::Corroborate && equivalent_groups.len() > 1 {
        return Err(format!(
            "corroborate decision contradicts equivalent_groups: {} distinct groups",
            equivalent_groups.len()
        ));
    }

    let mut superseded = Vec::with_capacity(raw.superseded.len());
    for entry in raw.superseded {
        if !known.contains(entry.claim_id.as_str()) {
            return Err(format!(
                "superseded claim_id '{}' is not a claim in the group",
                entry.claim_id
            ));
        }
        let valid_until = parse_optional_datetime(entry.valid_until.as_deref(), "valid_until")?;
        superseded.push(SupersededClaim {
            claim_id: entry.claim_id,
            valid_until,
        });
    }

    // The winner (and its equivalence group, whose members merge into the
    // winner) cannot simultaneously be superseded.
    if matches!(
        decision,
        AdjudicationDecision::SingleWinner | AdjudicationDecision::Supersede
    ) {
        if let Some(winner) = raw.winner_id.as_deref() {
            let winner_group: Vec<&str> = equivalent_groups
                .iter()
                .find(|group| group.iter().any(|id| id == winner))
                .map(|group| group.iter().map(String::as_str).collect())
                .unwrap_or_else(|| vec![winner]);
            if let Some(conflicted) = superseded
                .iter()
                .find(|entry| winner_group.contains(&entry.claim_id.as_str()))
            {
                return Err(format!(
                    "superseded claim_id '{}' is the winner or merges into the winner",
                    conflicted.claim_id
                ));
            }
        }
    }

    let winner_valid_from =
        parse_optional_datetime(raw.winner_valid_from.as_deref(), "winner_valid_from")?;

    let proposed_semantics = raw
        .proposed_semantics
        .as_ref()
        .map(convert_proposed_semantics)
        .transpose()?;
    if input.semantics.is_none() && proposed_semantics.is_none() {
        return Err(
            "predicate has no registered semantics but the verdict omitted proposed_semantics"
                .to_string(),
        );
    }

    Ok(AdjudicationVerdict {
        decision,
        winner_id: raw.winner_id,
        equivalent_groups,
        superseded,
        winner_valid_from,
        proposed_semantics,
        rationale: raw.rationale,
    })
}

/// Parses a raw verdict from the run result: structured output first, then
/// the text body (stripping markdown fences). No substring rescue heuristics.
#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_raw_verdict(
    structured_output: Option<&serde_json::Value>,
    text: &str,
) -> Result<RawVerdict, String> {
    if let Some(structured) = structured_output {
        match serde_json::from_value::<RawVerdict>(structured.clone()) {
            Ok(raw) => return Ok(raw),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Structured adjudication output did not match the verdict shape; trying text"
                );
            }
        }
    }

    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("adjudicator returned empty structured and text output".to_string());
    }

    let mut first_error: Option<String> = None;
    for candidate in verdict_json_candidates(trimmed) {
        match serde_json::from_str::<RawVerdict>(candidate) {
            Ok(raw) => return Ok(raw),
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err.to_string());
                }
            }
        }
    }

    Err(format!(
        "failed to parse adjudication verdict from text: {}",
        first_error.unwrap_or_else(|| "no JSON object found".to_string())
    ))
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn verdict_json_candidates(text: &str) -> Vec<&str> {
    let mut candidates = vec![text];
    if let Some(fenced) = extract_fenced_json(text) {
        candidates.push(fenced);
    }
    if let (Some(start), Some(end)) = (text.find('{'), text.rfind('}')) {
        if start < end {
            candidates.push(&text[start..=end]);
        }
    }
    candidates
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn extract_fenced_json(text: &str) -> Option<&str> {
    let fence_start = text.find("```")?;
    let after_start = &text[fence_start + 3..];
    let after_language = after_start
        .strip_prefix("json")
        .or_else(|| after_start.strip_prefix("JSON"))
        .unwrap_or(after_start);
    let after_newline = after_language
        .strip_prefix('\n')
        .or_else(|| after_language.strip_prefix("\r\n"))
        .unwrap_or(after_language);
    let fence_end = after_newline.find("```")?;
    Some(after_newline[..fence_end].trim())
}

#[cfg(feature = "meerkat-extraction")]
const ADJUDICATION_SYSTEM_PROMPT: &str =
    "You are a truth maintenance adjudicator for a knowledge graph. \
     You compare competing claim values for a single subject and predicate and decide \
     whether they corroborate each other, conflict, supersede each other over time, \
     or legitimately coexist. Use only the claims provided in the prompt. \
     Return only a JSON object matching the requested schema.";

#[cfg(feature = "meerkat-extraction")]
fn adjudication_provider_params(
    model: &str,
) -> Option<meerkat_core::lifecycle::run_primitive::ProviderParamsOverride> {
    use meerkat_core::lifecycle::run_primitive::{
        OpenAiProviderTag, ProviderParamsOverride, ProviderTag, ReasoningEffort,
    };

    if model.starts_with("gpt-") || model.starts_with('o') {
        return Some(ProviderParamsOverride {
            provider_tag: Some(ProviderTag::OpenAi(OpenAiProviderTag {
                reasoning_effort: Some(ReasoningEffort::Low),
                ..Default::default()
            })),
            ..Default::default()
        });
    }
    None
}

/// Agentic adjudicator calling the LLM through meerkat structured output.
///
/// Provider credentials are resolved by meerkat itself; availability is the
/// caller's concern (an unavailable provider yields `Ok(None)` with a warn).
#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Clone)]
pub struct AgenticSlotAdjudicator {
    config: AdjudicationConfig,
}

#[cfg(feature = "meerkat-extraction")]
impl AgenticSlotAdjudicator {
    /// Creates a new agentic adjudicator with the given configuration.
    #[must_use]
    pub const fn new(config: AdjudicationConfig) -> Self {
        Self { config }
    }
}

#[cfg(feature = "meerkat-extraction")]
#[async_trait]
impl SlotAdjudicator for AgenticSlotAdjudicator {
    async fn adjudicate(
        &self,
        input: &AdjudicationInput,
    ) -> Result<Option<AdjudicationVerdict>, PipelineError> {
        use std::path::PathBuf;
        use std::time::Duration;

        use meerkat::{
            AgentBuildConfig, AgentFactory, BudgetLimits, OutputSchema, ToolCategoryOverride,
        };
        use meerkat_core::types::ContentInput;
        use meerkat_core::Config;

        if input.claims.is_empty() {
            return Ok(None);
        }

        let output_schema = match OutputSchema::new(verdict_json_schema()) {
            Ok(schema) => schema.with_name("adjudication_verdict").strict(),
            Err(err) => {
                tracing::warn!(error = %err, "Invalid adjudication verdict schema; skipping LLM adjudication");
                return Ok(None);
            }
        };

        let budget_tokens = u64::from(self.config.max_tokens)
            .saturating_mul(4)
            .max(ADJUDICATION_MIN_BUDGET_TOKENS);
        let budget_limits = BudgetLimits::default()
            .with_max_tokens(budget_tokens)
            .with_max_duration(Duration::from_secs(self.config.max_wall_time_secs));

        let mut build = AgentBuildConfig::new(self.config.model.clone());
        build.system_prompt =
            meerkat_core::SystemPromptOverride::Set(ADJUDICATION_SYSTEM_PROMPT.to_string());
        build.max_tokens = Some(self.config.max_tokens);
        build.budget_limits = Some(budget_limits);
        build.output_schema = Some(output_schema);
        build.structured_output_retries = Some(2);
        build.override_shell = ToolCategoryOverride::Disable;
        build.override_mob = ToolCategoryOverride::Disable;
        build.provider_params = adjudication_provider_params(&self.config.model);

        let prompt = render_adjudication_prompt(input);
        let factory = AgentFactory::new(PathBuf::from("/tmp/elephant-tm-adjudication"));
        let meerkat_config = Config::default();

        let llm_call = async {
            let mut agent = factory
                .build_agent(build, &meerkat_config)
                .await
                .map_err(|e| {
                    PipelineError::ExtractionError(format!("Adjudication agent build failed: {e}"))
                })?;
            let run_result = agent.run(ContentInput::Text(prompt)).await.map_err(|e| {
                PipelineError::ExtractionError(format!("Adjudication agent failed: {e}"))
            })?;
            Ok::<_, PipelineError>((run_result.structured_output, run_result.text))
        };

        let timeout = Duration::from_secs(self.config.max_wall_time_secs);
        let (structured, text) = match tokio::time::timeout(timeout, llm_call).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                tracing::warn!(
                    slot_key = %input.slot_key,
                    error = %err,
                    "LLM adjudication failed; falling back to deterministic policy"
                );
                return Ok(None);
            }
            Err(_) => {
                tracing::warn!(
                    slot_key = %input.slot_key,
                    timeout_secs = self.config.max_wall_time_secs,
                    "LLM adjudication timed out; falling back to deterministic policy"
                );
                return Ok(None);
            }
        };

        let raw = match parse_raw_verdict(structured.as_ref(), &text) {
            Ok(raw) => raw,
            Err(reason) => {
                tracing::warn!(
                    slot_key = %input.slot_key,
                    reason = %reason,
                    "Unparseable adjudication verdict; falling back to deterministic policy"
                );
                return Ok(None);
            }
        };

        match validate_verdict(raw, input) {
            Ok(verdict) => Ok(Some(verdict)),
            Err(reason) => {
                tracing::warn!(
                    slot_key = %input.slot_key,
                    reason = %reason,
                    "Invalid adjudication verdict; falling back to deterministic policy"
                );
                Ok(None)
            }
        }
    }

    fn id(&self) -> &'static str {
        "agentic_adjudicator"
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn claim(id: &str, value: ClaimValue) -> AdjudicationClaim {
        AdjudicationClaim {
            claim_id: id.to_string(),
            value,
            confidence: 0.9,
            evidence_count: 2,
            timestamp: Utc.with_ymd_and_hms(2024, 5, 1, 12, 0, 0).unwrap(),
            time_basis: "asserted_at".to_string(),
            sources: vec!["source:mail-1".to_string()],
            evidence_notes: vec![
                "\"Alice works_at King\" (stated fact time 2024-05-01)".to_string()
            ],
        }
    }

    fn input_with(
        claims: Vec<AdjudicationClaim>,
        semantics: Option<PredicateSemantics>,
    ) -> AdjudicationInput {
        AdjudicationInput {
            space_id: "space:test".to_string(),
            slot_key: "entity:alice::works_at::abc".to_string(),
            slot_type: "rel".to_string(),
            subject_display: "Alice Smith".to_string(),
            predicate_id: "works_at".to_string(),
            semantics,
            claims,
        }
    }

    fn raw_verdict(value: serde_json::Value) -> RawVerdict {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn prompt_renders_all_claim_value_kinds() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim(
                    "c2",
                    ClaimValue::EntityRef {
                        id: "entity:king".to_string(),
                        display: "King".to_string(),
                    },
                ),
                claim(
                    "c3",
                    ClaimValue::EventClaim {
                        title: "Board meeting".to_string(),
                        time_start: Some(Utc.with_ymd_and_hms(2024, 6, 1, 9, 0, 0).unwrap()),
                        participants: vec!["Alice".to_string(), "Bob".to_string()],
                        status: "scheduled".to_string(),
                    },
                ),
            ],
            None,
        );

        let prompt = render_adjudication_prompt(&input);

        assert!(prompt.contains("Alice Smith"));
        assert!(prompt.contains("works_at"));
        assert!(prompt.contains("Slot type: rel"));
        assert!(prompt.contains("claim_id: c1"));
        assert!(prompt.contains("integer 35"));
        assert!(prompt.contains("claim_id: c2"));
        assert!(prompt.contains("entity \"King\" (id: entity:king)"));
        assert!(prompt.contains("claim_id: c3"));
        assert!(prompt.contains("event \"Board meeting\""));
        assert!(prompt.contains("participants: Alice, Bob"));
        assert!(prompt.contains("status: scheduled"));
        assert!(prompt.contains("confidence: 0.90"));
        assert!(prompt.contains("evidence_count: 2"));
        // asserted_at is an ingestion artifact: the timestamp must be withheld.
        assert!(prompt.contains("fact_time: unknown"));
        assert!(!prompt.contains("2024-05-01T12:00:00"));
        assert!(prompt.contains("source:mail-1"));
    }

    #[test]
    fn prompt_renders_registered_semantics() {
        let semantics = PredicateSemantics {
            cardinality: Cardinality::One,
            ..Default::default()
        };
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            Some(semantics),
        );

        let prompt = render_adjudication_prompt(&input);

        assert!(prompt.contains("cardinality=one"));
        assert!(prompt.contains("temporal_model=state_over_time"));
        assert!(prompt.contains("conflict_policy=allow_parallel"));
        assert!(!prompt.contains("proposed_semantics is REQUIRED"));
    }

    #[test]
    fn prompt_requires_proposed_semantics_when_none_registered() {
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            None,
        );

        let prompt = render_adjudication_prompt(&input);

        assert!(prompt.contains("(none registered)"));
        assert!(prompt.contains("proposed_semantics is REQUIRED"));
    }

    #[test]
    fn verdict_schema_requires_minimal_fields() {
        let schema = verdict_json_schema();
        assert_eq!(
            schema["required"],
            json!(["decision", "rationale", "equivalent_groups"])
        );
        assert_eq!(schema["additionalProperties"], json!(false));
    }

    #[test]
    fn verdict_parse_and_validate_roundtrip() {
        let input = input_with(
            vec![
                claim(
                    "c1",
                    ClaimValue::EntityRef {
                        id: "entity:king".to_string(),
                        display: "King".to_string(),
                    },
                ),
                claim(
                    "c2",
                    ClaimValue::EntityRef {
                        id: "entity:blizzard".to_string(),
                        display: "Blizzard".to_string(),
                    },
                ),
            ],
            None,
        );
        let structured = json!({
            "decision": "supersede",
            "winner_id": "c2",
            "equivalent_groups": [["c1"], ["c2"]],
            "superseded": [{ "claim_id": "c1", "valid_until": "2024-01-15" }],
            "winner_valid_from": "2024-01-15T09:30:00Z",
            "proposed_semantics": {
                "cardinality": "one",
                "temporal_model": "state_over_time",
                "conflict_policy": "supersede_by_recency"
            },
            "rationale": "Alice left King and joined Blizzard in 2024."
        });

        let raw = parse_raw_verdict(Some(&structured), "").unwrap();
        let verdict = validate_verdict(raw, &input).unwrap();

        assert_eq!(verdict.decision, AdjudicationDecision::Supersede);
        assert_eq!(verdict.winner_id.as_deref(), Some("c2"));
        assert_eq!(
            verdict.equivalent_groups,
            vec![vec!["c1".to_string()], vec!["c2".to_string()]]
        );
        assert_eq!(verdict.superseded.len(), 1);
        assert_eq!(verdict.superseded[0].claim_id, "c1");
        assert_eq!(
            verdict.superseded[0].valid_until,
            Some(Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap())
        );
        assert_eq!(
            verdict.winner_valid_from,
            Some(Utc.with_ymd_and_hms(2024, 1, 15, 9, 30, 0).unwrap())
        );
        let proposed = verdict.proposed_semantics.unwrap();
        assert_eq!(proposed.cardinality, Cardinality::One);
        assert_eq!(proposed.temporal_model, TemporalPersonality::StateOverTime);
        assert_eq!(proposed.conflict_policy, ConflictPolicy::SupersedeByRecency);
        assert_eq!(
            verdict.rationale,
            "Alice left King and joined Blizzard in 2024."
        );
    }

    #[test]
    fn parse_falls_back_to_fenced_text() {
        let text = "Here is my verdict:\n```json\n{\"decision\": \"corroborate\", \
                    \"rationale\": \"Same value.\", \"equivalent_groups\": [[\"c1\", \"c2\"]]}\n```";

        let raw = parse_raw_verdict(None, text).unwrap();

        assert_eq!(raw.decision, "corroborate");
        assert_eq!(raw.equivalent_groups, vec![vec!["c1", "c2"]]);
    }

    #[test]
    fn parse_rejects_empty_output() {
        assert!(parse_raw_verdict(None, "   ").is_err());
    }

    #[test]
    fn validator_rejects_unknown_claim_ids() {
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            Some(PredicateSemantics::default()),
        );

        let unknown_winner = raw_verdict(json!({
            "decision": "single_winner",
            "winner_id": "ghost",
            "equivalent_groups": [],
            "rationale": "r"
        }));
        assert!(validate_verdict(unknown_winner, &input)
            .unwrap_err()
            .contains("winner_id"));

        let unknown_group_member = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [["c1", "ghost"]],
            "rationale": "r"
        }));
        assert!(validate_verdict(unknown_group_member, &input)
            .unwrap_err()
            .contains("unknown claim_id"));

        let unknown_superseded = raw_verdict(json!({
            "decision": "coexist",
            "superseded": [{ "claim_id": "ghost" }],
            "equivalent_groups": [],
            "rationale": "r"
        }));
        assert!(validate_verdict(unknown_superseded, &input)
            .unwrap_err()
            .contains("superseded claim_id"));
    }

    #[test]
    fn validator_requires_winner_for_single_winner_and_supersede() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );

        for decision in ["single_winner", "supersede"] {
            let raw = raw_verdict(json!({
                "decision": decision,
                "equivalent_groups": [],
                "rationale": "r"
            }));
            assert!(validate_verdict(raw, &input)
                .unwrap_err()
                .contains("requires winner_id"));
        }
    }

    #[test]
    fn validator_requires_proposed_semantics_when_input_has_none() {
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            None,
        );
        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [],
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("proposed_semantics"));
    }

    #[test]
    fn validator_rejects_split_by_context_proposal() {
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            None,
        );
        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [],
            "proposed_semantics": {
                "cardinality": "many",
                "temporal_model": "persistent",
                "conflict_policy": "split_by_context"
            },
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("split_by_context"));
    }

    #[test]
    fn validator_rejects_unknown_decision() {
        let input = input_with(
            vec![claim("c1", ClaimValue::Literal(Literal::Integer(35)))],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "winner_takes_all",
            "equivalent_groups": [],
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("unknown decision"));
    }

    #[test]
    fn partition_normalization_fills_singletons() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(35))),
                claim("c3", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "coexist",
            "equivalent_groups": [["c1", "c2"]],
            "rationale": "r"
        }));

        let verdict = validate_verdict(raw, &input).unwrap();

        assert_eq!(
            verdict.equivalent_groups,
            vec![
                vec!["c1".to_string(), "c2".to_string()],
                vec!["c3".to_string()]
            ]
        );
    }

    #[test]
    fn partition_rejects_duplicate_membership() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "coexist",
            "equivalent_groups": [["c1"], ["c1", "c2"]],
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("more than one equivalence group"));
    }

    #[test]
    fn empty_equivalent_groups_stay_empty() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "coexist",
            "equivalent_groups": [],
            "rationale": "r"
        }));

        let verdict = validate_verdict(raw, &input).unwrap();

        assert!(verdict.equivalent_groups.is_empty());
    }

    #[test]
    fn date_parsing_handles_date_and_datetime() {
        assert_eq!(
            parse_verdict_datetime("2024-06-01"),
            Some(Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap())
        );
        assert_eq!(
            parse_verdict_datetime("2024-06-01T12:30:00Z"),
            Some(Utc.with_ymd_and_hms(2024, 6, 1, 12, 30, 0).unwrap())
        );
        assert_eq!(
            parse_verdict_datetime("2024-06-01T12:30:00+02:00"),
            Some(Utc.with_ymd_and_hms(2024, 6, 1, 10, 30, 0).unwrap())
        );
        assert_eq!(
            parse_verdict_datetime("2024-06-01T12:30:00"),
            Some(Utc.with_ymd_and_hms(2024, 6, 1, 12, 30, 0).unwrap())
        );
        assert_eq!(
            parse_verdict_datetime("2024"),
            Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
        );
        assert_eq!(parse_verdict_datetime("sometime in June"), None);
        assert_eq!(parse_verdict_datetime("324"), None);
    }

    #[test]
    fn validator_rejects_unparseable_dates() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "supersede",
            "winner_id": "c2",
            "equivalent_groups": [],
            "superseded": [{ "claim_id": "c1", "valid_until": "last spring" }],
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("valid_until"));
    }

    #[test]
    fn default_config_matches_contract() {
        let config = AdjudicationConfig::default();
        assert_eq!(config.model, "claude-sonnet-4-5");
        assert_eq!(config.max_tokens, 2000);
        assert_eq!(config.max_wall_time_secs, 120);
    }

    #[test]
    fn validator_rejects_corroborate_with_multiple_value_groups() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(35))),
                claim("c3", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );

        // Mislabeled corroborate with an honest 2-group partition.
        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [["c1", "c2"], ["c3"]],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("contradicts"));

        // Partial partition normalizes to 2 groups: equally contradictory.
        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [["c1", "c2"]],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("contradicts"));

        // One full group or no groups is consistent with corroborate.
        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [["c1", "c2", "c3"]],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input).is_ok());

        let raw = raw_verdict(json!({
            "decision": "corroborate",
            "equivalent_groups": [],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input).is_ok());
    }

    #[test]
    fn validator_rejects_winner_in_superseded() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );
        let raw = raw_verdict(json!({
            "decision": "single_winner",
            "winner_id": "c1",
            "equivalent_groups": [],
            "superseded": [{ "claim_id": "c1" }],
            "rationale": "r"
        }));

        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("winner"));
    }

    #[test]
    fn validator_rejects_winner_group_member_in_superseded() {
        let input = input_with(
            vec![
                claim("c1", ClaimValue::Literal(Literal::Integer(35))),
                claim("c2", ClaimValue::Literal(Literal::Integer(35))),
                claim("c3", ClaimValue::Literal(Literal::Integer(36))),
            ],
            Some(PredicateSemantics::default()),
        );

        // c2 merges into the winner c1; superseding it is contradictory.
        let raw = raw_verdict(json!({
            "decision": "supersede",
            "winner_id": "c1",
            "equivalent_groups": [["c1", "c2"], ["c3"]],
            "superseded": [{ "claim_id": "c2" }],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input)
            .unwrap_err()
            .contains("merges into the winner"));

        // Superseding the genuinely competing claim is fine.
        let raw = raw_verdict(json!({
            "decision": "supersede",
            "winner_id": "c1",
            "equivalent_groups": [["c1", "c2"], ["c3"]],
            "superseded": [{ "claim_id": "c3" }],
            "rationale": "r"
        }));
        assert!(validate_verdict(raw, &input).is_ok());
    }
}
