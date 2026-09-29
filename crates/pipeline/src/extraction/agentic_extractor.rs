//! Agentic extractor using MCP-based LLM agents.
//!
//! Per spec: The AgenticExtractor uses an LLM agent (via MCP) to perform
//! multi-step extraction with tool access to the Elephant knowledge graph.
//! This enables context-aware extraction that can resolve ambiguities by
//! querying existing entities and predicates.

use async_trait::async_trait;
#[cfg(feature = "meerkat-extraction")]
use base64::Engine;
use chrono::{DateTime, NaiveDate, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::{Duration, Instant};

use elephant_types::ids::TableName;
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::attribute::Literal;
use elephant_types::records::doc_revision::DocRevision;
use elephant_types::records::work::{Determinism, PluginManifest, RetryPolicy};
use elephant_types::{Link, RecordId};

use super::{ExtractionBudget, ExtractionContext, ExtractionResult, Extractor, RawAssertion};
use crate::error::PipelineError;
#[cfg(feature = "meerkat-extraction")]
use crate::media::load_media_bytes;

use meerkat::ToolDispatcher as McpToolDispatcher;
use meerkat::{
    AgentError, AgentLlmClient, AgentToolDispatcher, AnthropicClient, BudgetLimits, LlmClient,
    LlmDoneOutcome, LlmEvent, LlmRequest, LlmStreamResult, McpConnection, McpRouter,
    McpServerConfig, Message, OutputSchema, StopReason, ToolCategoryOverride, ToolDef, Usage,
};
use meerkat_core::error::{LlmFailureReason, LlmProviderError, LlmProviderErrorKind};
use meerkat_core::lifecycle::run_primitive::{
    OpenAiProviderTag, ProviderParamsOverride, ProviderTag, ReasoningEffort,
};
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ContentInput, ToolCallView, ToolResult};
use meerkat_core::AssistantBlock;
use serde_json::{json, Value};

fn reasoning_effort_from_str(value: &str) -> Option<ReasoningEffort> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Some(ReasoningEffort::None),
        "low" => Some(ReasoningEffort::Low),
        "medium" => Some(ReasoningEffort::Medium),
        "high" => Some(ReasoningEffort::High),
        "xhigh" => Some(ReasoningEffort::XHigh),
        _ => None,
    }
}

/// Default reasoning effort for OpenAI extraction models when
/// EXTRACTION_REASONING_EFFORT is unset. Measured 2026-08-01 on live A/B
/// (same docs, fresh stores): sol at default(medium) burned 76k tokens/60s on
/// a trivial 3-sentence doc vs 19k/20s at low, and on a fact-dense board
/// minutes doc low kept full assertion coverage (20 vs 21, including the
/// decision facts gpt-5.6-terra dropped entirely) at 28.8k/69s vs 33.8k/99s.
/// Reasoning above low buys nothing measurable for structured extraction.
const DEFAULT_EXTRACTION_REASONING_EFFORT: &str = "low";

/// Effort override for extraction agents: the env var when set (operator's
/// explicit choice, applied to any model), otherwise the low default for
/// models the OpenAI reasoning knob applies to. None for other providers -
/// an OpenAi provider tag must not be forced onto anthropic/gemini builds.
fn extraction_effort_params(model: &str) -> Option<ProviderParamsOverride> {
    let env_effort = std::env::var("EXTRACTION_REASONING_EFFORT").ok();
    effort_params_for(model, env_effort.as_deref())
}

fn effort_params_for(model: &str, env_effort: Option<&str>) -> Option<ProviderParamsOverride> {
    if let Some(effort) = env_effort {
        return openai_reasoning_provider_params(effort);
    }
    if model.starts_with("gpt-") || model.starts_with('o') {
        return openai_reasoning_provider_params(DEFAULT_EXTRACTION_REASONING_EFFORT);
    }
    None
}

fn openai_reasoning_provider_params(effort: &str) -> Option<ProviderParamsOverride> {
    Some(ProviderParamsOverride {
        provider_tag: Some(ProviderTag::OpenAi(OpenAiProviderTag {
            reasoning_effort: reasoning_effort_from_str(effort),
            ..Default::default()
        })),
        ..Default::default()
    })
}

/// Configuration for the agentic extractor.
#[derive(Debug, Clone)]
pub struct AgenticExtractorConfig {
    /// URL of the Elephant MCP server with agent profile.
    pub mcp_url: String,
    /// LLM model identifier (e.g., "gpt-4o", "claude-3-opus").
    pub model: String,
    /// Budget constraints for extraction operations.
    pub budget: ExtractionBudget,
    /// Whether to connect to MCP during initialization.
    pub connect_on_init: bool,
}

impl AgenticExtractorConfig {
    /// Creates a new agentic extractor configuration.
    #[must_use]
    pub fn new(mcp_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            mcp_url: mcp_url.into(),
            model: model.into(),
            budget: ExtractionBudget::default(),
            connect_on_init: true,
        }
    }

    /// Sets the budget for this configuration.
    #[must_use]
    pub fn with_budget(mut self, budget: ExtractionBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Controls whether the extractor connects to MCP during initialization.
    #[must_use]
    pub fn with_connect_on_init(mut self, connect_on_init: bool) -> Self {
        self.connect_on_init = connect_on_init;
        self
    }
}

impl Default for AgenticExtractorConfig {
    fn default() -> Self {
        Self::new("http://localhost:3000/mcp/agent", "gpt-5.6-sol")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LLM Output Parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Evidence span as returned by the LLM.
#[derive(Debug, Clone, Deserialize)]
struct LlmEvidence {
    start: u32,
    end: u32,
    quote: String,
}

/// Literal value as returned by the LLM.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum LlmLiteral {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Date(String),
    #[serde(rename = "datetime")]
    DateTime(String),
}

impl From<LlmLiteral> for Literal {
    fn from(llm: LlmLiteral) -> Self {
        match llm {
            LlmLiteral::String(s) => Literal::String(s),
            LlmLiteral::Integer(i) => Literal::Integer(i),
            LlmLiteral::Float(f) => Literal::Float(f),
            LlmLiteral::Boolean(b) => Literal::Boolean(b),
            LlmLiteral::Date(d) => Literal::Date(d),
            LlmLiteral::DateTime(dt) => Literal::DateTime(dt),
        }
    }
}

/// A single assertion as returned by the LLM.
#[derive(Debug, Clone, Deserialize)]
struct LlmAssertion {
    subject_text: String,
    predicate_text: String,
    #[serde(default)]
    object_text: Option<String>,
    #[serde(default)]
    object_literal: Option<LlmLiteral>,
    /// Evidence span — optional for the agentic path to maximize extraction coverage.
    #[serde(default)]
    evidence: Option<LlmEvidence>,
    confidence: f64,
    #[serde(default)]
    source_event_time: Option<String>,
    #[serde(default)]
    source_event_time_precision: Option<String>,
    #[serde(default)]
    event_title: Option<String>,
    /// Existing entity ID if the LLM found a match via search, null for new entities.
    #[serde(default)]
    subject_entity_id: Option<String>,
    /// Entity type classification for the subject.
    #[serde(default)]
    subject_entity_type: Option<String>,
    /// Existing entity ID if the LLM found a match via search, null for new entities.
    #[serde(default)]
    object_entity_id: Option<String>,
    /// Entity type classification for the object.
    #[serde(default)]
    object_entity_type: Option<String>,
    /// Significance assessment given the space purpose and graph state.
    #[serde(default)]
    significance: Option<String>,
}

/// The top-level response structure from the LLM.
#[derive(Debug, Clone, Deserialize)]
struct LlmResponse {
    assertions: Vec<LlmAssertion>,
}

fn parse_source_event_time(raw: Option<String>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }

    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
        }
    }

    if trimmed.len() == 7
        && trimmed.as_bytes().get(4) == Some(&b'-')
        && trimmed.chars().all(|c| c.is_ascii_digit() || c == '-')
    {
        let month_form = format!("{trimmed}-01");
        if let Ok(date) = NaiveDate::parse_from_str(&month_form, "%Y-%m-%d") {
            if let Some(dt) = date.and_hms_opt(0, 0, 0) {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
            }
        }
    }

    if trimmed.len() == 4 && trimmed.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(year) = trimmed.parse::<i32>() {
            if let Some(date) = NaiveDate::from_ymd_opt(year, 1, 1) {
                if let Some(dt) = date.and_hms_opt(0, 0, 0) {
                    return Some(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
                }
            }
        }
    }

    tracing::debug!("Ignoring non-parseable source_event_time: {trimmed}");
    None
}

fn parse_time_precision(raw: Option<String>) -> Option<elephant_types::records::TimePrecision> {
    use elephant_types::records::TimePrecision;

    let raw = raw?;
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    // Handle LLM-specific aliases not in the canonical FromStr impl.
    match normalized.as_str() {
        "date" => return Some(TimePrecision::Day),
        "datetime" => return Some(TimePrecision::Exact),
        _ => {}
    }

    // Delegate to the canonical FromStr implementation.
    match normalized.parse::<TimePrecision>() {
        Ok(tp) => Some(tp),
        Err(_) => {
            tracing::debug!("Ignoring non-parseable source_event_time_precision: {normalized}");
            None
        }
    }
}

fn normalize_extraction_label(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn is_source_inventory_subject(subject: &str) -> bool {
    let normalized = normalize_extraction_label(subject);
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
    let normalized = normalize_extraction_label(predicate).replace('_', " ");
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

fn is_source_inventory_metadata_assertion(subject: &str, predicate: &str) -> bool {
    is_source_inventory_subject(subject) && is_provider_catalog_predicate(predicate)
}

fn is_scalar_value_label(label: &str) -> bool {
    let normalized = normalize_extraction_label(label);
    if normalized.is_empty() {
        return false;
    }
    if matches!(
        normalized.as_str(),
        "true"
            | "false"
            | "yes"
            | "no"
            | "ja"
            | "nej"
            | "active"
            | "inactive"
            | "approved"
            | "candidate"
            | "confirmed"
            | "inferred"
            | "merged"
            | "pending"
            | "projected"
            | "resolved"
            | "superseded"
            | "uncertain"
            | "unknown"
    ) {
        return true;
    }
    if is_clock_time_like(&normalized) || is_ipv4_like(&normalized) || is_iso_date_like(&normalized)
    {
        return true;
    }
    if is_semver_like(&normalized) {
        return true;
    }
    if normalized
        .chars()
        .all(|ch| ch.is_ascii_digit() || matches!(ch, ' ' | '-' | '+' | '(' | ')' | '.'))
        && normalized.chars().any(|ch| ch.is_ascii_digit())
    {
        return true;
    }
    if normalized.chars().any(|ch| ch.is_ascii_digit())
        && ["kr", "sek", "usd", "eur", "gbp", "$", "€", "£"]
            .iter()
            .any(|token| normalized.contains(token))
    {
        return true;
    }
    let compact_hex = normalized
        .chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .collect::<String>();
    compact_hex.len() >= 16
        && compact_hex.len() == normalized.chars().filter(|ch| !ch.is_whitespace()).count()
}

fn is_clock_time_like(value: &str) -> bool {
    let Some((hours, minutes)) = value.split_once(':') else {
        return false;
    };
    !hours.is_empty()
        && hours.len() <= 2
        && minutes.len() == 2
        && hours.chars().all(|ch| ch.is_ascii_digit())
        && minutes.chars().all(|ch| ch.is_ascii_digit())
        && hours.parse::<u8>().is_ok_and(|value| value <= 23)
        && minutes.parse::<u8>().is_ok_and(|value| value <= 59)
}

fn is_ipv4_like(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 4
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.len() <= 3
                && part.chars().all(|ch| ch.is_ascii_digit())
                && part.parse::<u8>().is_ok()
        })
}

fn is_iso_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn is_semver_like(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() >= 2
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fn is_non_entity_composite_label(label: &str) -> bool {
    let lower = label.to_ascii_lowercase();
    lower.contains("<>")
        || lower.contains("->")
        || lower.contains("=>")
        || lower.contains(" from ")
            && lower.contains(" to ")
            && label.chars().any(|ch| ch == ':' || ch == '<' || ch == '>')
}

fn should_materialize_entity_label(label: &str, _entity_type: Option<&str>) -> bool {
    let trimmed = label.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.chars().count() > 160 {
        return false;
    }
    if is_non_entity_composite_label(trimmed) || is_scalar_value_label(trimmed) {
        return false;
    }
    true
}

/// Computes a SHA-256 hash of a quote string.
fn compute_quote_hash(quote: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(quote.as_bytes());
    hex::encode(hasher.finalize())
}

/// Parses LLM output JSON into a vector of `RawAssertion`s.
///
/// The expected JSON format:
/// ```json
/// {
///   "assertions": [
///     {
///       "subject_text": "Alice",
///       "predicate_text": "works at",
///       "object_text": "Acme Corp",
///       "object_literal": null,
///       "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
///       "confidence": 0.95
///     }
///   ]
/// }
/// ```
///
/// # Errors
///
/// Returns `PipelineError::ConfigError` for invalid JSON syntax.
/// Returns `PipelineError::InvalidInput` for missing required fields.
pub fn parse_llm_output(output: &str) -> Result<Vec<RawAssertion>, PipelineError> {
    // Parse the JSON first, then deserialize the assertion rows leniently.
    // A single malformed row should not throw away the rest of a document's facts.
    let value: Value = serde_json::from_str(output)
        .map_err(|e| PipelineError::ConfigError(format!("Invalid LLM output JSON: {e}")))?;
    let response = deserialize_llm_response(value, "LLM output JSON")?;

    // Convert each LlmAssertion to RawAssertion
    let mut raw_assertions = Vec::with_capacity(response.assertions.len());

    for (idx, llm_assertion) in response.assertions.into_iter().enumerate() {
        // Validate required fields
        if llm_assertion.subject_text.is_empty() {
            return Err(PipelineError::InvalidInput(format!(
                "Assertion {idx}: subject_text is required and cannot be empty"
            )));
        }
        if llm_assertion.predicate_text.is_empty() {
            return Err(PipelineError::InvalidInput(format!(
                "Assertion {idx}: predicate_text is required and cannot be empty"
            )));
        }

        // Must have either object_text or object_literal (skip if neither — don't fail the batch)
        if llm_assertion.object_text.is_none() && llm_assertion.object_literal.is_none() {
            tracing::warn!("Assertion {idx}: skipping — no object_text or object_literal");
            continue;
        }
        if is_source_inventory_metadata_assertion(
            &llm_assertion.subject_text,
            &llm_assertion.predicate_text,
        ) {
            tracing::debug!(
                subject = %llm_assertion.subject_text,
                predicate = %llm_assertion.predicate_text,
                "Assertion {idx}: skipping provider catalog metadata assertion"
            );
            continue;
        }
        if !should_materialize_entity_label(
            &llm_assertion.subject_text,
            llm_assertion.subject_entity_type.as_deref(),
        ) {
            tracing::debug!(
                subject = %llm_assertion.subject_text,
                "Assertion {idx}: skipping non-entity subject label"
            );
            continue;
        }
        if llm_assertion.object_literal.is_none()
            && llm_assertion.object_text.as_deref().is_some_and(|object| {
                !should_materialize_entity_label(
                    object,
                    llm_assertion.object_entity_type.as_deref(),
                )
            })
        {
            tracing::debug!(
                object = ?llm_assertion.object_text,
                "Assertion {idx}: skipping non-entity object label"
            );
            continue;
        }

        // Clamp confidence to valid range
        let confidence = llm_assertion.confidence.clamp(0.0, 1.0);

        // Build evidence span — use provided evidence or create a zero-span placeholder
        let placeholder_revision =
            Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision));

        let (evidence_span, quote) = if let Some(ev) = llm_assertion.evidence {
            (
                EvidenceSpan {
                    revision: placeholder_revision,
                    node: None,
                    start: ev.start,
                    end: ev.end,
                    quote_hash: compute_quote_hash(&ev.quote),
                },
                Some(ev.quote),
            )
        } else {
            // No evidence — zero-span placeholder. Assertion still gets created
            // as an ungrounded claim that can be evidence-linked later.
            (
                EvidenceSpan {
                    revision: placeholder_revision,
                    node: None,
                    start: 0,
                    end: 0,
                    quote_hash: String::new(),
                },
                None,
            )
        };

        let raw_assertion = RawAssertion {
            subject_text: llm_assertion.subject_text,
            predicate_text: llm_assertion.predicate_text,
            object_text: llm_assertion.object_text,
            object_literal: llm_assertion.object_literal.map(Into::into),
            evidence_span,
            confidence,
            quote,
            source_event_time: parse_source_event_time(llm_assertion.source_event_time),
            source_event_time_precision: parse_time_precision(
                llm_assertion.source_event_time_precision,
            ),
            event_title: llm_assertion.event_title,
            event_lineage_key: None,
            subject_entity_id: llm_assertion.subject_entity_id,
            subject_entity_type: llm_assertion.subject_entity_type,
            object_entity_id: llm_assertion.object_entity_id,
            object_entity_type: llm_assertion.object_entity_type,
            significance: llm_assertion.significance,
        };

        raw_assertions.push(raw_assertion);
    }

    Ok(raw_assertions)
}

fn deserialize_llm_response(value: Value, context: &str) -> Result<LlmResponse, PipelineError> {
    match serde_json::from_value::<LlmResponse>(value.clone()) {
        Ok(response) => Ok(response),
        Err(primary_err) => {
            let Some(assertions) = value.get("assertions").and_then(Value::as_array) else {
                return Err(PipelineError::ConfigError(format!(
                    "Invalid {context}: {primary_err}"
                )));
            };

            let mut parsed = Vec::with_capacity(assertions.len());
            let mut skipped = 0usize;
            for (idx, assertion) in assertions.iter().enumerate() {
                match serde_json::from_value::<LlmAssertion>(assertion.clone()) {
                    Ok(assertion) => parsed.push(assertion),
                    Err(err) => {
                        skipped += 1;
                        tracing::warn!(
                            assertion_index = idx,
                            error = %err,
                            "Skipping malformed extracted assertion row"
                        );
                    }
                }
            }

            if parsed.is_empty() && !assertions.is_empty() {
                return Err(PipelineError::ConfigError(format!(
                    "Invalid {context}: {primary_err}"
                )));
            }

            if skipped > 0 {
                tracing::warn!(
                    skipped,
                    kept = parsed.len(),
                    "Recovered valid assertions from partially malformed extraction output"
                );
            }

            Ok(LlmResponse { assertions: parsed })
        }
    }
}

const EXTRACTION_PROMPT_TEMPLATE: &str = include_str!("../../prompts/extraction.txt");
const AGENTIC_EXTRACTION_PROMPT_TEMPLATE: &str =
    include_str!("../../prompts/agentic_extraction.txt");

/// JSON Schema for the expected LLM output structure.
///
/// This schema is used with meerkat's `OutputSchema` to guarantee structured
/// JSON responses from the LLM. After the agentic loop completes, meerkat
/// performs an extraction turn that forces the LLM to produce validated JSON
/// matching this schema.
#[must_use]
pub fn extraction_output_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "assertions": {
                "type": "array",
                "description": "List of extracted factual assertions from the text",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "subject_text": {
                            "type": "string",
                            "description": "Exact text mention of the subject entity (never a pronoun)"
                        },
                        "predicate_text": {
                            "type": "string",
                            "description": "Natural language description of the relationship"
                        },
                        "object_text": {
                            "type": ["string", "null"],
                            "description": "Exact text mention of object entity, or null if object_literal is used"
                        },
                        "object_literal": {
                            "type": ["object", "null"],
                            "description": "Literal value for entity-to-literal relationships",
                            "additionalProperties": false,
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["string", "integer", "float", "boolean", "date", "datetime"]
                                },
                                "value": {
                                    "type": ["string", "number", "boolean"],
                                    "description": "The literal value matching the specified type"
                                }
                            },
                            "required": ["type", "value"]
                        },
                        "evidence": {
                            "type": ["object", "null"],
                            "additionalProperties": false,
                            "description": "Optional evidence span. Include when you can provide exact character offsets; omit or set to null otherwise.",
                            "properties": {
                                "start": {
                                    "type": "integer",
                                    "description": "Character offset where supporting text begins (0-indexed)"
                                },
                                "end": {
                                    "type": "integer",
                                    "description": "Character offset where supporting text ends (exclusive)"
                                },
                                "quote": {
                                    "type": "string",
                                    "description": "Verbatim quote from source text"
                                }
                            },
                            "required": ["start", "end", "quote"]
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Extraction confidence score between 0.0 and 1.0"
                        },
                        "source_event_time": {
                            "type": ["string", "null"],
                            "description": "ISO 8601 datetime when the event occurred (if mentioned in text)"
                        },
                        "source_event_time_precision": {
                            // Nullable enums must be anyOf-encoded: Anthropic's
                            // structured-outputs API rejects `enum` combined
                            // with a `type` union ("Enum value 'exact' does not
                            // match declared type '['string', 'null']'").
                            "anyOf": [
                                { "type": "string", "enum": ["exact", "minute", "hour", "day", "month", "year", "unknown"] },
                                { "type": "null" }
                            ],
                            "description": "Precision of source_event_time; set to null if no event time is present"
                        },
                        "event_title": {
                            "type": ["string", "null"],
                            "description": "Optional concise timeline title when this assertion describes an event"
                        },
                        "subject_entity_id": {
                            "type": ["string", "null"],
                            "description": "Existing entity ID if found via search, null for new entities"
                        },
                        "subject_entity_type": {
                            "type": ["string", "null"],
                            "description": "Entity type classification (freeform, e.g. person, org, topic, device, location, place, group, platform, etc.)"
                        },
                        "object_entity_id": {
                            "type": ["string", "null"],
                            "description": "Existing entity ID if found via search, null for new entities"
                        },
                        "object_entity_type": {
                            "type": ["string", "null"],
                            "description": "Entity type classification (freeform, e.g. person, org, topic, device, location, place, group, platform, etc.)"
                        },
                        "significance": {
                            "type": ["string", "null"],
                            "description": "How significant is this claim given the space purpose and graph state"
                        }
                    },
                    "required": [
                        "subject_text", "predicate_text", "object_text",
                        "object_literal", "evidence", "confidence",
                        "source_event_time", "source_event_time_precision",
                        "event_title", "subject_entity_id",
                        "subject_entity_type", "object_entity_id",
                        "object_entity_type", "significance"
                    ]
                }
            }
        },
        "required": ["assertions"]
    })
}

/// Strips or expands a `{{#if TAG}}...{{/if}}` conditional block in a template.
///
/// If `value` is `Some`, the block is kept (markers removed, TAG replaced with value).
/// If `value` is `None`, the entire block including content is removed.
fn resolve_conditional(template: &mut String, tag: &str, value: Option<&str>) {
    let open = format!("{{{{#if {tag}}}}}");
    let close = "{{/if}}";

    match value {
        Some(val) => {
            // Find the specific {{#if TAG}} and its matching {{/if}}
            if let Some(start) = template.find(&open) {
                let after_open = start + open.len();
                if let Some(close_offset) = template[after_open..].find(close) {
                    let close_start = after_open + close_offset;
                    let close_end = close_start + close.len();
                    // Remove trailing newline after {{/if}} if present
                    let close_end = if template.as_bytes().get(close_end) == Some(&b'\n') {
                        close_end + 1
                    } else {
                        close_end
                    };
                    // Remove {{/if}} first (higher index)
                    template.replace_range(close_start..close_end, "");
                    // Remove {{#if TAG}}
                    template.replace_range(start..after_open, "");
                }
                let placeholder = format!("{{{{{tag}}}}}");
                *template = template.replace(&placeholder, val);
            }
        }
        None => {
            if let Some(start) = template.find(&open) {
                let after_open = start + open.len();
                if let Some(close_offset) = template[after_open..].find(close) {
                    let close_start = after_open + close_offset;
                    let end = close_start + close.len();
                    let end = if template.as_bytes().get(end) == Some(&b'\n') {
                        end + 1
                    } else {
                        end
                    };
                    template.replace_range(start..end, "");
                }
            }
        }
    }
}

/// Formats known entities as a markdown table for injection into the extraction prompt.
fn format_known_entities(entities: &[crate::extraction::KnownEntity]) -> String {
    if entities.is_empty() {
        return String::new();
    }
    let mut out = String::from("| ID | Name | Type |\n|---|---|---|\n");
    for e in entities {
        out.push_str(&format!(
            "| {} | {} | {} |\n",
            e.id, e.display_name, e.entity_type
        ));
    }
    out
}

fn format_media_context_with_source_timestamp(
    attachments: &[crate::extraction::ExtractionMediaAttachment],
    source_timestamp: Option<&DateTime<Utc>>,
) -> String {
    if attachments.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    for attachment in attachments {
        let kind = match attachment.kind {
            elephant_types::records::doc::DocMediaKind::Image => "image",
            elephant_types::records::doc::DocMediaKind::Video => "video",
        };
        out.push_str("- Attachment id: ");
        out.push_str(&attachment.attachment_id);
        out.push('\n');
        out.push_str("  Kind: ");
        out.push_str(kind);
        out.push('\n');
        out.push_str("  Media type: ");
        out.push_str(&attachment.media_type);
        out.push('\n');
        if let Some(label) = attachment.label.as_deref() {
            out.push_str("  Label: ");
            out.push_str(label);
            out.push('\n');
        }
        if let Some(duration_ms) = attachment.duration_ms {
            out.push_str("  Duration ms: ");
            out.push_str(&duration_ms.to_string());
            out.push('\n');
        }
        if let Some(time_range) = &attachment.time_range {
            out.push_str("  Relevant time range ms: ");
            out.push_str(
                &time_range
                    .start_ms
                    .map_or_else(|| "?".to_string(), |value| value.to_string()),
            );
            out.push_str(" -> ");
            out.push_str(
                &time_range
                    .end_ms
                    .map_or_else(|| "?".to_string(), |value| value.to_string()),
            );
            out.push('\n');
            if let Some(source_timestamp) = source_timestamp {
                let absolute_start = time_range.start_ms.and_then(|offset| {
                    chrono::Duration::milliseconds(i64::try_from(offset).ok()?)
                        .checked_add(&chrono::Duration::zero())
                        .and_then(|delta| source_timestamp.checked_add_signed(delta))
                });
                let absolute_end = time_range.end_ms.and_then(|offset| {
                    chrono::Duration::milliseconds(i64::try_from(offset).ok()?)
                        .checked_add(&chrono::Duration::zero())
                        .and_then(|delta| source_timestamp.checked_add_signed(delta))
                });
                if absolute_start.is_some() || absolute_end.is_some() {
                    out.push_str("  Absolute time range: ");
                    out.push_str(
                        &absolute_start.map_or_else(|| "?".to_string(), |value| value.to_rfc3339()),
                    );
                    out.push_str(" -> ");
                    out.push_str(
                        &absolute_end.map_or_else(|| "?".to_string(), |value| value.to_rfc3339()),
                    );
                    out.push('\n');
                }
            }
        }
    }
    out
}

/// Builds the extraction prompt with all context: skill, purpose, known entities, and text.
pub(crate) fn build_extraction_prompt(
    text: &str,
    purpose: Option<&str>,
    skill_body: Option<&str>,
    known_entities: &[crate::extraction::KnownEntity],
) -> String {
    let mut template = EXTRACTION_PROMPT_TEMPLATE.to_string();

    resolve_conditional(&mut template, "EXTRACTION_SKILL", skill_body);
    resolve_conditional(&mut template, "SPACE_PURPOSE", purpose);

    let entities_text = format_known_entities(known_entities);
    let entities_value = if entities_text.is_empty() {
        None
    } else {
        Some(entities_text.as_str())
    };
    resolve_conditional(&mut template, "KNOWN_ENTITIES", entities_value);

    template.replace("{{TEXT}}", text)
}

/// Context size threshold (in chars) for deciding inline vs tool-based text access.
/// ~50K tokens * 4 chars/token = 200K chars. Above this, use tool-based reading.
const INLINE_TEXT_THRESHOLD: usize = 200_000;

struct AgenticPromptInput<'a> {
    text: &'a str,
    doc_id: Option<&'a str>,
    doc_title: Option<&'a str>,
    purpose: Option<&'a str>,
    skill_body: Option<&'a str>,
    known_entities: &'a [crate::extraction::KnownEntity],
    sidecar_text: Option<&'a str>,
    source_timestamp: Option<&'a DateTime<Utc>>,
    media_attachments: &'a [crate::extraction::ExtractionMediaAttachment],
}

/// Builds the agentic extraction prompt.
///
/// For texts that fit in context, includes the text inline.
/// For larger texts, instructs the agent to read via `list_doc_chunks`/`get_doc_chunk`.
fn build_agentic_extraction_prompt(input: AgenticPromptInput<'_>) -> String {
    let AgenticPromptInput {
        text,
        doc_id,
        doc_title,
        purpose,
        skill_body,
        known_entities,
        sidecar_text,
        source_timestamp,
        media_attachments,
    } = input;
    let mut template = AGENTIC_EXTRACTION_PROMPT_TEMPLATE.to_string();

    resolve_conditional(&mut template, "EXTRACTION_SKILL", skill_body);
    resolve_conditional(&mut template, "SPACE_PURPOSE", purpose);

    let entities_text = format_known_entities(known_entities);
    let entities_value = if entities_text.is_empty() {
        None
    } else {
        Some(entities_text.as_str())
    };
    resolve_conditional(&mut template, "KNOWN_ENTITIES", entities_value);

    // Decide inline vs tool-based text access
    if text.len() <= INLINE_TEXT_THRESHOLD {
        resolve_conditional(&mut template, "TEXT_INLINE", Some(text));
        resolve_conditional(&mut template, "TEXT_VIA_TOOLS", None);
    } else {
        resolve_conditional(&mut template, "TEXT_INLINE", None);
        let doc_ref = doc_id.unwrap_or("(unknown)");
        resolve_conditional(&mut template, "TEXT_VIA_TOOLS", Some("true"));
        template = template.replace("{{DOC_ID}}", doc_ref);
    }

    if let Some(title) = doc_title.filter(|value| !value.trim().is_empty()) {
        template.push_str("\n\nDocument title:\n");
        template.push_str(title.trim());
    }

    if let Some(sidecar) = sidecar_text.filter(|value| !value.trim().is_empty()) {
        template.push_str("\n\nCaller sidecar text:\n");
        template.push_str(sidecar.trim());
    }

    if let Some(source_timestamp) = source_timestamp {
        template.push_str("\n\nSource timestamp:\n");
        template.push_str(&source_timestamp.to_rfc3339());
    }

    if !media_attachments.is_empty() {
        template.push_str(
            "\n\nAttached media relevant to this node:\n\
             Inspect the attached image/video content directly. Prefer concrete visible names, \
             handles, apps, sites, groups, visible text, and actions from the media over \
             scaffolding phrases in the rendered transcript.\n\
             For video-backed evidence, sequence and time are first-class. If a source timestamp \
             and clip-relative time range are provided, convert them into absolute wall-clock \
             time and populate `source_event_time`, `source_event_time_precision`, and \
             `event_title` for event-like assertions whenever the evidence supports it. Prefer \
             absolute timestamps over relative offsets.\n",
        );
        template.push_str(&format_media_context_with_source_timestamp(
            media_attachments,
            source_timestamp,
        ));
    }

    template
}

#[cfg(feature = "meerkat-extraction")]
async fn build_agentic_content_input(
    prompt: String,
    media_attachments: &[crate::extraction::ExtractionMediaAttachment],
    storage: Option<&elephant_storage::StorageClient>,
    space_id: &str,
) -> Result<ContentInput, PipelineError> {
    use meerkat_core::{ContentBlock, ImageData, VideoData};

    if media_attachments.is_empty() {
        return Ok(ContentInput::Text(prompt));
    }

    let mut blocks = Vec::with_capacity(media_attachments.len() + 1);
    blocks.push(ContentBlock::Text { text: prompt });

    for attachment in media_attachments {
        let (bytes, resolved_duration_ms) = load_media_bytes(
            storage,
            space_id,
            &elephant_types::records::doc::DocMediaAttachment {
                attachment_id: attachment.attachment_id.clone(),
                kind: attachment.kind,
                media_type: attachment.media_type.clone(),
                location: attachment.location.clone(),
                label: attachment.label.clone(),
                size_bytes: None,
                duration_ms: attachment.duration_ms,
                sha256: None,
            },
        )
        .await?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        match attachment.kind {
            elephant_types::records::doc::DocMediaKind::Image => blocks.push(ContentBlock::Image {
                media_type: attachment.media_type.clone(),
                data: ImageData::Inline { data: encoded },
            }),
            elephant_types::records::doc::DocMediaKind::Video => blocks.push(ContentBlock::Video {
                media_type: attachment.media_type.clone(),
                duration_ms: resolved_duration_ms,
                data: VideoData::Inline { data: encoded },
            }),
        }
    }

    Ok(ContentInput::Blocks(blocks))
}

fn extract_json_block(output: &str) -> Option<String> {
    let trimmed = output.trim();
    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if end >= start {
            return Some(trimmed[start..=end].to_string());
        }
    }
    None
}

fn parse_agent_run_output(
    model: &str,
    structured_output: Option<&Value>,
    text: &str,
) -> Result<Vec<RawAssertion>, PipelineError> {
    if let Some(structured) = structured_output {
        return match parse_structured_output(structured) {
            Ok(assertions) => Ok(assertions),
            Err(e) => {
                eprintln!(
                    "[agentic] structured parse failed for model {model}: {e}\n  value preview: {}",
                    &structured.to_string()[..structured.to_string().len().min(500)]
                );
                eprintln!(
                    "[agentic] text preview for model {model}: {}",
                    &text[..text.len().min(500)]
                );
                if text.trim().is_empty() {
                    Err(PipelineError::ExtractionError(format!(
                        "Model {model} produced unusable extraction output: structured output was present but could not be deserialized, and fallback text output was empty"
                    )))
                } else {
                    parse_llm_output_with_repair(text)
                }
            }
        };
    }

    if text.trim().is_empty() {
        return Err(PipelineError::ExtractionError(format!(
            "Model {model} produced unusable extraction output: structured output was absent and text output was empty"
        )));
    }

    parse_llm_output_with_repair(text)
}

fn is_empty_agent_output_error(err: &PipelineError) -> bool {
    matches!(
        err,
        PipelineError::ExtractionError(message)
            if message.contains("structured output was absent and text output was empty")
    )
}

pub(crate) fn parse_llm_output_with_repair(
    output: &str,
) -> Result<Vec<RawAssertion>, PipelineError> {
    match parse_llm_output(output) {
        Ok(assertions) => Ok(assertions),
        Err(primary_err) => {
            if let Some(candidate) = extract_json_block(output) {
                match parse_llm_output(&candidate) {
                    Ok(assertions) => return Ok(assertions),
                    Err(_) => {
                        // Try repairing common LLM JSON issues (e.g. "true" instead of true)
                        if let Ok(mut value) = serde_json::from_str::<Value>(&candidate) {
                            repair_json_literals(&mut value);
                            let repaired = value.to_string();
                            return parse_llm_output(&repaired);
                        }
                    }
                }
            }
            Err(primary_err)
        }
    }
}

/// Recursively walks a JSON value tree and repairs common LLM output issues:
/// - Converts string `"null"` to actual JSON `null`.
/// - Converts string `"true"`/`"false"` to actual booleans in `object_literal` fields
///   where `type` is `"boolean"`.
/// - Converts string-encoded numbers where `type` is `"integer"` or `"float"`.
fn repair_json_literals(value: &mut Value) {
    match value {
        Value::String(s) if s == "null" => {
            *value = Value::Null;
        }
        Value::Object(map) => {
            // First pass: convert any "null" string values to actual null
            for val in map.values_mut() {
                if val.as_str() == Some("null") {
                    *val = Value::Null;
                }
            }

            // Repair tagged literal objects: {"type": "boolean", "value": "true"}
            if let Some(type_val) = map.get("type").and_then(|v| v.as_str()).map(String::from) {
                // Normalize "number" → "integer" or "float" based on the value
                if type_val == "number" {
                    let is_float = map.get("value").map_or(false, |v| {
                        v.is_f64() || v.as_str().map_or(false, |s| s.contains('.'))
                    });
                    let new_type = if is_float { "float" } else { "integer" };
                    map.insert("type".to_string(), Value::String(new_type.to_string()));
                }

                let type_val = map
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(val) = map.get_mut("value") {
                    match type_val.as_str() {
                        "boolean" => {
                            if let Some(s) = val.as_str() {
                                match s {
                                    "true" => *val = Value::Bool(true),
                                    "false" => *val = Value::Bool(false),
                                    _ => {}
                                }
                            }
                        }
                        "integer" => {
                            if let Some(s) = val.as_str() {
                                if let Ok(n) = s.parse::<i64>() {
                                    *val = Value::Number(n.into());
                                }
                            }
                        }
                        "float" => {
                            if let Some(s) = val.as_str() {
                                if let Ok(n) = s.parse::<f64>() {
                                    if let Some(num) = serde_json::Number::from_f64(n) {
                                        *val = Value::Number(num);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            for val in map.values_mut() {
                repair_json_literals(val);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                repair_json_literals(item);
            }
        }
        _ => {}
    }
}

/// Parse structured output from meerkat's validated JSON response.
///
/// Unlike `parse_llm_output_with_repair`, this function expects already-validated
/// JSON that conforms to `extraction_output_schema()`. It converts the Value
/// directly without needing repair or JSON extraction.
fn parse_structured_output(output: &Value) -> Result<Vec<RawAssertion>, PipelineError> {
    // Repair common LLM quirks (e.g. Gemini returns "null" strings instead of null)
    let mut repaired = output.clone();
    repair_json_literals(&mut repaired);

    let llm_output = deserialize_llm_response(repaired, "structured output").map_err(|e| {
        PipelineError::ConfigError(format!("Failed to deserialize structured output: {e}"))
    })?;

    let mut raw_assertions = Vec::with_capacity(llm_output.assertions.len());

    for assertion in llm_output.assertions {
        // Skip assertions with no object
        if assertion.object_text.is_none() && assertion.object_literal.is_none() {
            continue;
        }
        if is_source_inventory_metadata_assertion(
            &assertion.subject_text,
            &assertion.predicate_text,
        ) {
            tracing::debug!(
                subject = %assertion.subject_text,
                predicate = %assertion.predicate_text,
                "skipping provider catalog metadata assertion"
            );
            continue;
        }
        if !should_materialize_entity_label(
            &assertion.subject_text,
            assertion.subject_entity_type.as_deref(),
        ) {
            tracing::debug!(
                subject = %assertion.subject_text,
                "skipping non-entity subject label"
            );
            continue;
        }
        if assertion.object_literal.is_none()
            && assertion.object_text.as_deref().is_some_and(|object| {
                !should_materialize_entity_label(object, assertion.object_entity_type.as_deref())
            })
        {
            tracing::debug!(
                object = ?assertion.object_text,
                "skipping non-entity object label"
            );
            continue;
        }

        let object_literal = assertion.object_literal.map(Into::into);
        let placeholder_revision =
            Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision));

        let (evidence_span, quote) = if let Some(ev) = assertion.evidence {
            (
                EvidenceSpan {
                    revision: placeholder_revision,
                    node: None,
                    start: ev.start,
                    end: ev.end,
                    quote_hash: compute_quote_hash(&ev.quote),
                },
                Some(ev.quote),
            )
        } else {
            (
                EvidenceSpan {
                    revision: placeholder_revision,
                    node: None,
                    start: 0,
                    end: 0,
                    quote_hash: String::new(),
                },
                None,
            )
        };

        let raw_assertion = RawAssertion {
            subject_text: assertion.subject_text,
            predicate_text: assertion.predicate_text,
            object_text: assertion.object_text,
            object_literal,
            evidence_span,
            quote,
            confidence: assertion.confidence.clamp(0.0, 1.0),
            source_event_time: parse_source_event_time(assertion.source_event_time),
            source_event_time_precision: parse_time_precision(
                assertion.source_event_time_precision,
            ),
            event_title: assertion.event_title,
            event_lineage_key: None,
            subject_entity_id: assertion.subject_entity_id,
            subject_entity_type: assertion.subject_entity_type,
            object_entity_id: assertion.object_entity_id,
            object_entity_type: assertion.object_entity_type,
            significance: assertion.significance,
        };

        raw_assertions.push(raw_assertion);
    }

    Ok(raw_assertions)
}

fn resolve_budget(config: &ExtractionBudget, ctx: &ExtractionBudget) -> ExtractionBudget {
    ExtractionBudget {
        max_tokens: config.max_tokens.min(ctx.max_tokens),
        max_wall_time_secs: config.max_wall_time_secs.min(ctx.max_wall_time_secs),
    }
}

fn agentic_max_tool_calls() -> usize {
    std::env::var("PIPELINE_MAX_EXTRACT_TOOL_CALLS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(24)
}

fn agentic_budget_limits(budget: &ExtractionBudget) -> BudgetLimits {
    BudgetLimits::default()
        .with_max_tokens(u64::from(budget.max_tokens))
        .with_max_duration(Duration::from_secs(u64::from(budget.max_wall_time_secs)))
        .with_max_tool_calls(agentic_max_tool_calls())
}

fn estimated_extraction_text_tokens(text: &str) -> u32 {
    let tokens = text.len().div_ceil(4);
    u32::try_from(tokens).unwrap_or(u32::MAX)
}

fn direct_extraction_threshold_tokens() -> u32 {
    std::env::var("PIPELINE_AGENTIC_DIRECT_EXTRACTION_THRESHOLD_TOKENS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3_000)
}

fn extract_profile_enabled() -> bool {
    std::env::var("PIPELINE_EXTRACT_PROFILE")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes"
        })
        .unwrap_or(false)
}

pub(crate) fn normalize_evidence_span(
    assertion: &mut RawAssertion,
    text: &str,
) -> Result<(), PipelineError> {
    let text_len = text.len();
    let start = assertion.evidence_span.start as usize;
    let end = assertion.evidence_span.end as usize;

    let span_valid = end <= text_len && start <= end;
    match assertion.quote.as_deref() {
        Some(quote) => {
            if span_valid && text.get(start..end) == Some(quote) {
                return Ok(());
            }
            if let Some(found) = text.find(quote) {
                assertion.evidence_span.start = found as u32;
                assertion.evidence_span.end = (found + quote.len()) as u32;
                return Ok(());
            }
            Err(PipelineError::InvalidInput(
                "Evidence quote could not be located in source text".to_string(),
            ))
        }
        None => {
            if span_valid {
                Ok(())
            } else {
                Err(PipelineError::InvalidInput(
                    "Evidence span is out of bounds for source text".to_string(),
                ))
            }
        }
    }
}

fn literal_search_text(literal: &Literal) -> Option<String> {
    match literal {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => Some(value.clone()),
        Literal::Integer(value) => Some(value.to_string()),
        Literal::Float(value) => Some(value.to_string()),
        Literal::Boolean(value) => Some(value.to_string()),
        Literal::Json(value) => value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| Some(value.to_string())),
    }
}

fn recover_evidence_span_from_text(assertion: &mut RawAssertion, text: &str) -> bool {
    if assertion.quote.is_some() {
        return false;
    }

    let subject = assertion.subject_text.trim();
    let object = assertion
        .object_text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            assertion
                .object_literal
                .as_ref()
                .and_then(literal_search_text)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        });

    if subject.is_empty() || object.is_none() {
        return false;
    }
    let Some(object) = object else {
        return false;
    };
    let subject_lower = subject.to_ascii_lowercase();
    let object_lower = object.to_ascii_lowercase();

    let mut offset = 0usize;
    let mut best_quote: Option<&str> = None;
    let mut best_start = 0usize;
    let mut best_len = usize::MAX;

    for line in text.lines() {
        let raw_line_start = offset;
        let raw_line_end = raw_line_start + line.len();
        offset = raw_line_end + 1;

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let trimmed_lower = trimmed.to_ascii_lowercase();
        if !trimmed_lower.contains(&subject_lower) || !trimmed_lower.contains(&object_lower) {
            continue;
        }

        if trimmed.len() >= best_len {
            continue;
        }

        if let Some(relative_start) = line.find(trimmed) {
            best_quote = Some(trimmed);
            best_start = raw_line_start + relative_start;
            best_len = trimmed.len();
        }
    }

    if let Some(quote) = best_quote {
        assertion.quote = Some(quote.to_string());
        assertion.evidence_span.start = best_start as u32;
        assertion.evidence_span.end = (best_start + quote.len()) as u32;
        assertion.evidence_span.quote_hash = compute_quote_hash(quote);
        return true;
    }

    false
}

pub(crate) fn mcp_headers_from_env() -> HashMap<String, String> {
    let mut headers = HashMap::new();
    if let Ok(token) = std::env::var("MCP_BEARER_TOKEN") {
        if !token.trim().is_empty() {
            headers.insert("Authorization".to_string(), format!("Bearer {token}"));
        }
    }
    headers
}

fn llm_provider_error(message: impl Into<String>) -> LlmFailureReason {
    LlmFailureReason::ProviderError(LlmProviderError::non_retryable(
        LlmProviderErrorKind::Unknown,
        json!({ "message": message.into() }),
    ))
}

fn connect_mcp_on_init(config: &AgenticExtractorConfig) -> Result<(), PipelineError> {
    let headers = mcp_headers_from_env();
    let server_config = McpServerConfig::streamable_http(
        "elephant-mcp-agent-init",
        config.mcp_url.clone(),
        headers,
    );
    let init_timeout = mcp_init_timeout();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PipelineError::ExtractionError(format!("MCP runtime init failed: {e}")))?;
        rt.block_on(async move {
            tokio::time::timeout(init_timeout, async {
                let connection = McpConnection::connect(&server_config).await.map_err(|e| {
                    PipelineError::ExtractionError(format!("MCP connection failed: {e}"))
                })?;
                connection
                    .list_tools("elephant-mcp-agent-init")
                    .await
                    .map_err(|e| {
                        PipelineError::ExtractionError(format!("MCP list_tools failed: {e}"))
                    })?;
                connection.close().await.map_err(|e| {
                    PipelineError::ExtractionError(format!("MCP connection close failed: {e}"))
                })?;
                Ok(())
            })
            .await
            .map_err(|_| {
                PipelineError::ExtractionError(format!(
                    "MCP init timed out after {}s",
                    init_timeout.as_secs()
                ))
            })?
        })
    })
    .join()
    .map_err(|_| PipelineError::ExtractionError("MCP init thread panicked".to_string()))?
}

fn mcp_init_timeout() -> Duration {
    let default_secs = if cfg!(test) { 1 } else { 5 };
    let secs = std::env::var("PIPELINE_AGENTIC_MCP_INIT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

// ─────────────────────────────────────────────────────────────────────────────
// UTF-8 boundary helpers (stable equivalents of nightly floor/ceil_char_boundary)
// ─────────────────────────────────────────────────────────────────────────────

/// Rounds a byte offset down to the nearest UTF-8 char boundary.
fn floor_char_boundary(s: &str, mut idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

/// Rounds a byte offset up to the nearest UTF-8 char boundary.
fn ceil_char_boundary(s: &str, mut idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadTextRangeDispatcher — injects a `read_text_range` tool into any dispatcher
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps an existing `AgentToolDispatcher` and adds a `read_text_range` tool
/// that reads a character range from the source text.
///
/// This allows the extraction agent to verify evidence quotes against the
/// actual source text, ensuring evidence spans match verbatim.
struct ReadTextRangeDispatcher {
    inner: Arc<dyn AgentToolDispatcher>,
    text: String,
    tool_defs: Arc<[Arc<ToolDef>]>,
}

impl ReadTextRangeDispatcher {
    fn new(inner: Arc<dyn AgentToolDispatcher>, text: String) -> Self {
        let read_text_tool = Arc::new(ToolDef::new(
            "read_text_range",
            "Read a character range from the source document. Use this to get exact \
             verbatim quotes for evidence spans. Returns the text between the start \
             and end character offsets.",
            json!({
                "type": "object",
                "properties": {
                    "start": {
                        "type": "integer",
                        "description": "Start character offset (0-indexed, inclusive)"
                    },
                    "end": {
                        "type": "integer",
                        "description": "End character offset (exclusive)"
                    }
                },
                "required": ["start", "end"]
            }),
        ));

        // Combine inner tools with read_text_range
        let mut all_tools: Vec<Arc<ToolDef>> = inner.tools().to_vec();
        all_tools.push(read_text_tool);
        let tool_defs: Arc<[Arc<ToolDef>]> = all_tools.into();

        Self {
            inner,
            text,
            tool_defs,
        }
    }

    fn handle_read_text_range(&self, call: &ToolCallView<'_>) -> ToolResult {
        let args: Value =
            serde_json::from_str(call.args.get()).unwrap_or(Value::Object(Default::default()));

        let start = args.get("start").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let end = args.get("end").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let text_len = self.text.len();
        if start > text_len || end > text_len || start > end {
            return ToolResult::new(
                call.id.to_string(),
                format!(
                    "Invalid range [{start}..{end}): document is {text_len} chars. \
                     Ensure 0 <= start <= end <= {text_len}."
                ),
                true,
            );
        }

        // Ensure we don't slice in the middle of a multi-byte UTF-8 char
        let safe_start = floor_char_boundary(&self.text, start);
        let safe_end = ceil_char_boundary(&self.text, end);

        let slice = &self.text[safe_start..safe_end];
        let result = json!({
            "text": slice,
            "start": safe_start,
            "end": safe_end,
            "length": safe_end - safe_start,
        });

        ToolResult::new(call.id.to_string(), result.to_string(), false)
    }
}

#[async_trait]
impl AgentToolDispatcher for ReadTextRangeDispatcher {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        self.tool_defs.clone()
    }

    async fn dispatch(
        &self,
        call: ToolCallView<'_>,
    ) -> Result<ToolDispatchOutcome, meerkat_core::error::ToolError> {
        if call.name == "read_text_range" {
            Ok(self.handle_read_text_range(&call).into())
        } else {
            self.inner.dispatch(call).await
        }
    }
}

fn normalize_extracted_assertions(
    raw_assertions: Vec<RawAssertion>,
    ctx: &ExtractionContext,
) -> Vec<RawAssertion> {
    let mut valid_assertions = Vec::new();
    for mut assertion in raw_assertions {
        assertion.evidence_span.revision = Link::new(ctx.revision_id.clone());
        assertion.evidence_span.node = Some(Link::new(ctx.node_id.clone()));

        // Only validate evidence if the assertion has a non-empty span.
        let has_evidence = assertion.evidence_span.start != 0
            || assertion.evidence_span.end != 0
            || assertion.quote.is_some();

        if has_evidence {
            match normalize_evidence_span(&mut assertion, &ctx.node_text) {
                Ok(()) => valid_assertions.push(assertion),
                Err(e) => {
                    // Evidence didn't match — keep the assertion but clear evidence.
                    tracing::debug!(
                        "Evidence mismatch for '{}' → '{}': {e} — keeping assertion without evidence",
                        assertion.subject_text,
                        assertion.predicate_text,
                    );
                    assertion.evidence_span.start = 0;
                    assertion.evidence_span.end = 0;
                    assertion.evidence_span.quote_hash = String::new();
                    assertion.quote = None;
                    valid_assertions.push(assertion);
                }
            }
        } else {
            let recovered = recover_evidence_span_from_text(&mut assertion, &ctx.node_text);
            if recovered && normalize_evidence_span(&mut assertion, &ctx.node_text).is_ok() {
                valid_assertions.push(assertion);
            } else {
                // No evidence provided — accept as ungrounded assertion.
                valid_assertions.push(assertion);
            }
        }
    }
    valid_assertions
}

fn require_completed_extraction_run(result: &meerkat::RunResult) -> Result<(), PipelineError> {
    if let Some(cause) = result.terminal_cause_kind {
        return Err(PipelineError::ExtractionError(format!(
            "Extraction agent terminated: {cause:?}"
        )));
    }
    if let Some(error) = &result.extraction_error {
        return Err(PipelineError::ExtractionError(format!(
            "Structured extraction failed after {} attempts: {}",
            error.attempts, error.reason
        )));
    }
    Ok(())
}

/// Agentic extractor that uses MCP-connected LLM agents.
///
/// This extractor leverages an LLM agent with access to Elephant's
/// knowledge graph tools to perform context-aware assertion extraction.
/// The agent can query existing entities, predicates, and relationships
/// to resolve ambiguities and maintain consistency.
#[derive(Debug)]
pub struct AgenticExtractor {
    config: AgenticExtractorConfig,
    manifest: PluginManifest,
}

impl AgenticExtractor {
    /// Creates a new agentic extractor with the given configuration.
    #[must_use]
    pub fn new(config: AgenticExtractorConfig) -> Self {
        let manifest = PluginManifest {
            plugin_id: "elephant.extractor.agentic".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "extractor".to_string(),
            required_scopes: vec![
                "read:docs".to_string(),
                "read:entities".to_string(),
                "read:predicates".to_string(),
                "write:assertions".to_string(),
            ],
            // Agent profile tools for knowledge graph access + read_text_range
            tool_access: vec![
                "search_entities".to_string(),
                "get_entity".to_string(),
                "get_entity_context_bundle".to_string(),
                "list_predicates".to_string(),
                "get_predicate".to_string(),
                "search_docs".to_string(),
                "get_doc".to_string(),
                "list_doc_chunks".to_string(),
                "get_doc_chunk".to_string(),
                "search_all".to_string(),
                "list_assertions".to_string(),
                "search_assertions".to_string(),
                "read_text_range".to_string(),
            ],
            work_kinds: vec!["extract_assertions".to_string()],
            determinism: Determinism::AgenticLoop,
            max_tokens: Some(config.budget.max_tokens),
            max_wall_time_secs: Some(config.budget.max_wall_time_secs),
            input_types: vec!["doc_node".to_string()],
            output_types: vec!["knowledge_candidates".to_string()],
            validator_profile: Some("assertion_validator".to_string()),
            config_schema: None,
            health_check: Some("/health".to_string()),
            retry_policy: RetryPolicy {
                max_attempts: 3,
                initial_backoff_secs: 1,
                max_backoff_secs: 10,
                backoff_multiplier: 2.0,
                retryable_errors: vec![],
            },
        };

        if config.connect_on_init {
            if let Err(err) = connect_mcp_on_init(&config) {
                tracing::warn!(error = %err, "AgenticExtractor MCP init connection failed");
            }
        }

        Self { config, manifest }
    }

    /// Returns the MCP URL for the agent.
    #[must_use]
    pub fn mcp_url(&self) -> &str {
        &self.config.mcp_url
    }

    /// Returns the model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.config.model
    }

    /// Returns the extraction budget.
    #[must_use]
    pub fn budget(&self) -> &ExtractionBudget {
        &self.config.budget
    }

    /// Runs the agentic extraction loop with the given tool dispatcher and LLM client.
    ///
    /// Uses `AgentFactory` to build a constrained extraction agent with only the
    /// domain-specific Elephant tools needed for evidence-aware extraction.
    pub async fn run_agent(
        &self,
        ctx: ExtractionContext,
        dispatcher: Arc<dyn AgentToolDispatcher>,
        #[allow(unused_variables)] llm: Arc<dyn AgentLlmClient>,
    ) -> Result<ExtractionResult, PipelineError> {
        use meerkat::{AgentBuildConfig, AgentFactory, DynAgent};
        use meerkat_core::Config;
        use std::path::PathBuf;

        let budget = resolve_budget(&self.config.budget, &ctx.budget);
        let overall_started = Instant::now();

        let prompt_started = Instant::now();
        let prompt = build_agentic_extraction_prompt(AgenticPromptInput {
            text: &ctx.node_text,
            doc_id: ctx.doc_id.as_deref(),
            doc_title: ctx.doc_title.as_deref(),
            purpose: ctx.purpose.as_deref(),
            skill_body: ctx.skill_body.as_deref(),
            known_entities: &ctx.known_entities,
            sidecar_text: ctx.sidecar_text.as_deref(),
            source_timestamp: ctx.source_timestamp.as_ref(),
            media_attachments: &ctx.media_attachments,
        });
        let prompt_ms = prompt_started.elapsed().as_millis();

        // Wrap the dispatcher with a ReadTextRange tool for evidence verification
        let external: Arc<dyn AgentToolDispatcher> = Arc::new(ReadTextRangeDispatcher::new(
            dispatcher,
            ctx.node_text.clone(),
        ));

        // Build a minimal extraction agent with no sub-agent surface; extraction
        // must complete in a single bounded run and produce structured output.
        let factory = AgentFactory::new(PathBuf::from("/tmp/elephant-extraction"));
        let meerkat_config = Config::default();

        let budget_limits = agentic_budget_limits(&budget);

        let output_schema = OutputSchema::new(extraction_output_schema())
            .map_err(|e| PipelineError::ExtractionError(format!("Invalid output schema: {e}")))?
            .with_name("extraction_output")
            .strict();

        let estimated_context_tokens = estimated_extraction_text_tokens(&ctx.node_text);
        let direct_threshold_tokens = direct_extraction_threshold_tokens();
        if estimated_context_tokens >= direct_threshold_tokens {
            tracing::info!(
                node_id = %ctx.node_id,
                revision_id = %ctx.revision_id,
                model = %self.config.model,
                estimated_context_tokens,
                direct_threshold_tokens,
                "Using direct structured extraction for large text span"
            );
            let direct_prompt = build_extraction_prompt(
                &ctx.node_text,
                ctx.purpose.as_deref(),
                ctx.skill_body.as_deref(),
                &ctx.known_entities,
            );
            let mut direct_build = AgentBuildConfig::new(self.config.model.clone());
            #[cfg(test)]
            {
                direct_build.agent_llm_client_override = Some(llm.clone());
            }
            direct_build.output_schema = Some(output_schema.clone());
            direct_build.structured_output_retries = Some(3);
            direct_build.budget_limits = Some(budget_limits.clone());
            direct_build.max_tokens = Some(64_000);
            direct_build.override_shell = ToolCategoryOverride::Disable;
            direct_build.override_mob = ToolCategoryOverride::Disable;
            direct_build.provider_params = extraction_effort_params(&self.config.model);

            let agent_build_started = Instant::now();
            let mut direct_agent: DynAgent = factory
                .build_agent(direct_build, &meerkat_config)
                .await
                .map_err(|e| {
                    PipelineError::ExtractionError(format!(
                        "Direct extraction agent build failed: {e}"
                    ))
                })?;
            let agent_build_ms = agent_build_started.elapsed().as_millis();

            let run_started = Instant::now();
            let direct_result = direct_agent
                .run(ContentInput::Text(direct_prompt))
                .await
                .map_err(|e| {
                    PipelineError::ExtractionError(format!(
                        "Direct extraction agent run failed: {e}"
                    ))
                })?;
            let run_ms = run_started.elapsed().as_millis();
            require_completed_extraction_run(&direct_result)?;

            let parse_started = Instant::now();
            let raw_assertions = parse_agent_run_output(
                &self.config.model,
                direct_result.structured_output.as_ref(),
                &direct_result.text,
            )?;
            let parse_ms = parse_started.elapsed().as_millis();

            let normalize_started = Instant::now();
            let raw_assertions = normalize_extracted_assertions(raw_assertions, &ctx);
            let normalize_ms = normalize_started.elapsed().as_millis();

            let tokens_used = direct_result.usage.total_tokens();
            let total_ms = overall_started.elapsed().as_millis();
            if extract_profile_enabled() {
                tracing::info!(
                    node_id = %ctx.node_id,
                    revision_id = %ctx.revision_id,
                    model = %self.config.model,
                    known_entities = ctx.known_entities.len(),
                    prompt_chars = ctx.node_text.len(),
                    media_attachments = ctx.media_attachments.len(),
                    prompt_ms,
                    agent_build_ms,
                    run_ms,
                    parse_ms,
                    normalize_ms,
                    total_ms,
                    turns = direct_result.turns,
                    tool_calls = direct_result.tool_calls,
                    tokens = tokens_used,
                    assertions = raw_assertions.len(),
                    structured = direct_result.structured_output.is_some(),
                    "Direct structured extraction profile"
                );
            }

            return Ok(ExtractionResult::new(
                raw_assertions,
                u32::try_from(tokens_used).unwrap_or(u32::MAX),
                self.config.model.clone(),
            ));
        }

        // Let AgentFactory resolve the LLM client from model name + env API keys.
        // Provider is inferred: claude-* → Anthropic, gpt-* → OpenAI, gemini-* → Google.
        let mut build = AgentBuildConfig::new(self.config.model.clone());
        // Unit tests exercise the real factory, agent loop, tool dispatch, and
        // budget enforcement with deterministic model responses. Production
        // builds retain the factory's provider and credential resolution.
        #[cfg(test)]
        {
            build.agent_llm_client_override = Some(llm.clone());
        }
        // No hardcoded system_prompt — domain-specific instructions come from
        // the extraction skill and agentic_extraction.txt, both injected into
        // the user prompt by build_agentic_extraction_prompt().
        build.output_schema = Some(output_schema.clone());
        build.structured_output_retries = Some(2);
        build.budget_limits = Some(budget_limits.clone());
        build.max_tokens = Some(64_000);
        build.external_tools = Some(external);
        build.override_shell = ToolCategoryOverride::Disable;
        build.override_mob = ToolCategoryOverride::Disable;

        // Provider-specific params: reasoning effort for OpenAI models
        // (low by default, EXTRACTION_REASONING_EFFORT overrides).
        build.provider_params = extraction_effort_params(&self.config.model);

        let agent_build_started = Instant::now();
        let mut agent: DynAgent = factory
            .build_agent(build, &meerkat_config)
            .await
            .map_err(|e| PipelineError::ExtractionError(format!("Agent build failed: {e}")))?;
        let agent_build_ms = agent_build_started.elapsed().as_millis();

        let content_input = build_agentic_content_input(
            prompt,
            &ctx.media_attachments,
            ctx.storage.as_ref(),
            &ctx.space_id,
        )
        .await?;
        let run_started = Instant::now();
        let run_result = agent
            .run(content_input)
            .await
            .map_err(|e| PipelineError::ExtractionError(format!("Agent run failed: {e}")))?;
        let run_ms = run_started.elapsed().as_millis();
        // Budget exhaustion is an Ok result with a machine-owned terminal
        // cause in Meerkat. Never retry it through the empty-output fallback.
        require_completed_extraction_run(&run_result)?;

        eprintln!(
            "[agentic] completed: turns={}, tool_calls={}, tokens={}, structured={}, text_len={}",
            run_result.turns,
            run_result.tool_calls,
            run_result.usage.total_tokens(),
            run_result.structured_output.is_some(),
            run_result.text.len(),
        );

        // Use structured_output if available, otherwise fall back to parsing text
        let parse_started = Instant::now();
        let mut fallback_tokens: u64 = 0;
        let raw_assertions = match parse_agent_run_output(
            &self.config.model,
            run_result.structured_output.as_ref(),
            &run_result.text,
        ) {
            Ok(assertions) => assertions,
            Err(err) if is_empty_agent_output_error(&err) => {
                tracing::warn!(
                    node_id = %ctx.node_id,
                    revision_id = %ctx.revision_id,
                    model = %self.config.model,
                    "Agentic extraction returned empty structured/text output; retrying once with direct structured extraction prompt"
                );
                let fallback_prompt = build_extraction_prompt(
                    &ctx.node_text,
                    ctx.purpose.as_deref(),
                    ctx.skill_body.as_deref(),
                    &ctx.known_entities,
                );
                let mut fallback_build = AgentBuildConfig::new(self.config.model.clone());
                #[cfg(test)]
                {
                    fallback_build.agent_llm_client_override = Some(llm.clone());
                }
                fallback_build.output_schema = Some(output_schema);
                fallback_build.structured_output_retries = Some(3);
                fallback_build.budget_limits = Some(budget_limits.clone());
                fallback_build.max_tokens = Some(64_000);
                fallback_build.override_shell = ToolCategoryOverride::Disable;
                fallback_build.override_mob = ToolCategoryOverride::Disable;
                if let Ok(effort) = std::env::var("EXTRACTION_REASONING_EFFORT") {
                    fallback_build.provider_params = openai_reasoning_provider_params(&effort);
                }

                let mut fallback_agent: DynAgent = factory
                    .build_agent(fallback_build, &meerkat_config)
                    .await
                    .map_err(|build_err| {
                        PipelineError::ExtractionError(format!(
                            "{err}; fallback agent build failed: {build_err}"
                        ))
                    })?;
                let fallback_result = fallback_agent
                    .run(ContentInput::Text(fallback_prompt))
                    .await
                    .map_err(|run_err| {
                        PipelineError::ExtractionError(format!(
                            "{err}; fallback agent run failed: {run_err}"
                        ))
                    })?;
                require_completed_extraction_run(&fallback_result)?;
                fallback_tokens =
                    fallback_tokens.saturating_add(fallback_result.usage.total_tokens());
                parse_agent_run_output(
                    &self.config.model,
                    fallback_result.structured_output.as_ref(),
                    &fallback_result.text,
                )
                .map_err(|fallback_err| {
                    PipelineError::ExtractionError(format!(
                        "{err}; fallback extraction also failed: {fallback_err}"
                    ))
                })?
            }
            Err(err) => return Err(err),
        };
        let parse_ms = parse_started.elapsed().as_millis();

        let normalize_started = Instant::now();
        let raw_assertions = normalize_extracted_assertions(raw_assertions, &ctx);
        let normalize_ms = normalize_started.elapsed().as_millis();

        let tokens_used = run_result
            .usage
            .total_tokens()
            .saturating_add(fallback_tokens);
        let total_ms = overall_started.elapsed().as_millis();
        if extract_profile_enabled() {
            tracing::info!(
                node_id = %ctx.node_id,
                revision_id = %ctx.revision_id,
                model = %self.config.model,
                known_entities = ctx.known_entities.len(),
                prompt_chars = ctx.node_text.len(),
                media_attachments = ctx.media_attachments.len(),
                prompt_ms,
                agent_build_ms,
                run_ms,
                parse_ms,
                normalize_ms,
                total_ms,
                turns = run_result.turns,
                tool_calls = run_result.tool_calls,
                tokens = tokens_used,
                assertions = raw_assertions.len(),
                structured = run_result.structured_output.is_some(),
                "Agentic extraction profile"
            );
        }
        // Budget enforcement is handled by meerkat's agent loop internally.
        // We just report the actual usage.

        Ok(ExtractionResult::new(
            raw_assertions,
            u32::try_from(tokens_used).unwrap_or(u32::MAX),
            self.config.model.clone(),
        ))
    }
}

/// Adapter that implements `AgentLlmClient` using Anthropic's API via meerkat.
pub struct AnthropicLlmAdapter {
    client: Arc<AnthropicClient>,
    model: String,
}

impl AnthropicLlmAdapter {
    /// Creates a new adapter with the given API key and model.
    #[allow(clippy::result_large_err)]
    pub fn new(api_key: String, model: String) -> Result<Self, AgentError> {
        let client = AnthropicClient::new(api_key).map_err(|e| {
            AgentError::llm(
                "anthropic",
                llm_provider_error(e.to_string()),
                e.to_string(),
            )
        })?;
        Ok(Self {
            client: Arc::new(client),
            model,
        })
    }
}

#[async_trait]
impl AgentLlmClient for AnthropicLlmAdapter {
    async fn stream_response(
        &self,
        messages: &[Message],
        tools: &[Arc<ToolDef>],
        max_tokens: u32,
        temperature: Option<f32>,
        provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<LlmStreamResult, AgentError> {
        let request = LlmRequest {
            model: self.model.clone(),
            messages: messages.to_vec(),
            tools: tools.to_vec(),
            max_tokens,
            temperature,
            stop_sequences: None,
            provider_params: provider_params.and_then(|params| params.provider_tag.clone()),
        };

        let mut stream = self.client.stream(&request);

        let mut content = String::new();
        let mut blocks: Vec<AssistantBlock> = Vec::new();
        let mut stop_reason = StopReason::EndTurn;
        let mut usage = Usage::default();

        while let Some(result) = stream.next().await {
            match result {
                Ok(event) => match event {
                    LlmEvent::TextDelta { delta, .. } => {
                        content.push_str(&delta);
                    }
                    LlmEvent::ToolCallComplete {
                        id,
                        name,
                        args,
                        meta,
                    } => {
                        let raw = serde_json::value::to_raw_value(&args)
                            .or_else(|_| serde_json::value::to_raw_value(&json!({})))
                            .map_err(|e| {
                                AgentError::ConfigError(format!(
                                    "Failed to serialize tool args: {e}"
                                ))
                            })?;
                        blocks.push(AssistantBlock::ToolUse {
                            id,
                            name,
                            args: raw,
                            meta,
                        });
                    }
                    LlmEvent::UsageUpdate { usage: updated } => {
                        usage = updated.into_inner();
                    }
                    LlmEvent::Done { outcome } => match outcome {
                        LlmDoneOutcome::Success {
                            stop_reason: reason,
                        } => {
                            stop_reason = reason;
                        }
                        LlmDoneOutcome::Error { error } => {
                            return Err(AgentError::llm(
                                "anthropic",
                                llm_provider_error(error.to_string()),
                                error.to_string(),
                            ));
                        }
                    },
                    _ => {}
                },
                Err(e) => {
                    return Err(AgentError::llm(
                        "anthropic",
                        llm_provider_error(e.to_string()),
                        e.to_string(),
                    ));
                }
            }
        }

        // Insert text block at front if any text was accumulated
        if !content.is_empty() {
            blocks.insert(
                0,
                AssistantBlock::Text {
                    text: content,
                    meta: None,
                },
            );
        }

        Ok(LlmStreamResult::new(blocks, stop_reason, usage))
    }

    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::Anthropic
    }

    fn model(&self) -> &str {
        &self.model
    }
}

/// Placeholder LLM client for agentic extraction entrypoints that delegate
/// provider/model resolution to Meerkat's `AgentFactory`.
///
/// `AgenticExtractor::run_agent` does not call this client directly today; the
/// factory builds its own provider-specific client from the configured model.
pub(crate) struct NoopAgentLlmClient;

#[async_trait]
impl AgentLlmClient for NoopAgentLlmClient {
    async fn stream_response(
        &self,
        _messages: &[Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<LlmStreamResult, AgentError> {
        Err(AgentError::ConfigError(
            "NoopAgentLlmClient should never be invoked; AgentFactory is expected to resolve the model provider directly".to_string(),
        ))
    }

    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::from_name("noop")
    }

    fn model(&self) -> &str {
        "noop"
    }
}

#[async_trait]
impl Extractor for AgenticExtractor {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        // meerkat 0.7.x McpRouter::new() fails closed (every lifecycle
        // mutation is rejected until a generated surface authority is bound).
        // The extractor drives the router as a standalone host, so bind an
        // ephemeral DSL authority — the same path meerkat's fixtures use.
        let mut router = McpRouter::new_with_surface_handle(Arc::new(
            meerkat_runtime::handles::RuntimeExternalToolSurfaceHandle::ephemeral(),
        ));
        let headers = mcp_headers_from_env();
        let server_config = McpServerConfig::streamable_http(
            "elephant-mcp-agent",
            self.config.mcp_url.clone(),
            headers,
        );

        router
            .add_server(server_config)
            .await
            .map_err(|e| PipelineError::ExtractionError(format!("MCP connection failed: {e}")))?;

        let router = Arc::new(router);

        let tool_dispatcher =
            McpToolDispatcher::new(router.clone()).with_timeout(Duration::from_secs(30));

        let dispatcher: Arc<dyn AgentToolDispatcher> = Arc::new(tool_dispatcher);
        let llm: Arc<dyn AgentLlmClient> = Arc::new(NoopAgentLlmClient);

        self.run_agent(ctx, dispatcher, llm).await
    }

    async fn health_check(&self) -> Result<bool, PipelineError> {
        let headers = mcp_headers_from_env();
        let server_config = McpServerConfig::streamable_http(
            "elephant-mcp-health",
            self.config.mcp_url.clone(),
            headers,
        );

        let connection = match McpConnection::connect(&server_config).await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::debug!(
                    mcp_url = %self.config.mcp_url,
                    error = %err,
                    "AgenticExtractor health check failed to connect"
                );
                return Ok(false);
            }
        };

        let healthy = connection.list_tools("elephant-mcp-health").await.is_ok();
        if let Err(err) = connection.close().await {
            tracing::debug!(
                mcp_url = %self.config.mcp_url,
                error = %err,
                "AgenticExtractor health check failed to close MCP connection"
            );
        }

        Ok(healthy)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]

    #[test]
    fn extraction_effort_defaults_low_for_openai_only() {
        use meerkat_core::lifecycle::run_primitive::{ProviderTag, ReasoningEffort};

        // OpenAI models get the low default (the A/B-measured sweet spot).
        let params = effort_params_for("gpt-5.6-sol", None).expect("openai default");
        match params.provider_tag {
            Some(ProviderTag::OpenAi(tag)) => {
                assert_eq!(tag.reasoning_effort, Some(ReasoningEffort::Low));
            }
            other => panic!("expected OpenAi tag, got {other:?}"),
        }

        // Non-OpenAI models must NOT get an OpenAi provider tag forced on.
        assert!(effort_params_for("claude-opus-4-8", None).is_none());
        assert!(effort_params_for("gemini-3.5-flash", None).is_none());

        // The env override is the operator's explicit choice, any model.
        let params = effort_params_for("claude-opus-4-8", Some("high")).expect("override");
        match params.provider_tag {
            Some(ProviderTag::OpenAi(tag)) => {
                assert_eq!(tag.reasoning_effort, Some(ReasoningEffort::High));
            }
            other => panic!("expected OpenAi tag, got {other:?}"),
        }
    }

    use super::*;
    use chrono::Datelike;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::Security;
    use elephant_types::RecordId;
    use serde_json::json;

    fn run_factory_test<F, Fut>(test: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()>,
    {
        // The generated Meerkat factory future exceeds the default test stack
        // in unoptimized builds. Keep this local to these runtime tests.
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime")
                    .block_on(test());
            })
            .expect("test thread")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    }

    struct MockLlmClient {
        responses: Mutex<Vec<LlmStreamResult>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl MockLlmClient {
        fn new(responses: Vec<LlmStreamResult>) -> Self {
            Self {
                responses: Mutex::new(responses),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AgentLlmClient for MockLlmClient {
        async fn stream_response(
            &self,
            _messages: &[Message],
            _tools: &[Arc<ToolDef>],
            _max_tokens: u32,
            _temperature: Option<f32>,
            _provider_params: Option<&ProviderParamsOverride>,
        ) -> Result<LlmStreamResult, AgentError> {
            let mut responses = self
                .responses
                .lock()
                .map_err(|_| AgentError::ConfigError("MockLlmClient lock poisoned".to_string()))?;
            if responses.is_empty() {
                return Err(AgentError::ConfigError(
                    "MockLlmClient exhausted responses".to_string(),
                ));
            }
            let (blocks, stop_reason, usage) = responses.remove(0).into_parts();
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let usage =
                meerkat_core::TurnUsage::host_declared(self.provider(), self.model(), usage)
                    .into_inner();
            Ok(LlmStreamResult::new(blocks, stop_reason, usage))
        }

        fn provider(&self) -> meerkat::Provider {
            meerkat::Provider::Other
        }

        fn model(&self) -> &str {
            "mock-model"
        }
    }

    #[derive(Debug)]
    struct RecordingDispatcher {
        calls: Arc<Mutex<Vec<String>>>,
        tools: Arc<[Arc<ToolDef>]>,
    }

    impl RecordingDispatcher {
        fn new(calls: Arc<Mutex<Vec<String>>>, tools: Vec<ToolDef>) -> Self {
            let tools = tools.into_iter().map(Arc::new).collect::<Vec<_>>();
            Self {
                calls,
                tools: tools.into(),
            }
        }
    }

    #[async_trait]
    impl AgentToolDispatcher for RecordingDispatcher {
        fn tools(&self) -> Arc<[Arc<ToolDef>]> {
            Arc::clone(&self.tools)
        }

        async fn dispatch(
            &self,
            call: meerkat_core::ToolCallView<'_>,
        ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat::ToolError> {
            let mut calls = self
                .calls
                .lock()
                .map_err(|_| meerkat::ToolError::Other("Dispatcher lock poisoned".to_string()))?;
            calls.push(call.name.to_string());
            Ok(meerkat_core::ToolResult::new(
                call.id.to_string(),
                r#"{"ok": true}"#.to_string(),
                false,
            )
            .into())
        }
    }

    fn make_config() -> AgenticExtractorConfig {
        AgenticExtractorConfig::new("http://localhost:3001/mcp", "gpt-4o")
            .with_connect_on_init(false)
    }

    fn make_context() -> ExtractionContext {
        ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            "Alice works at Acme Corp as a software engineer.".to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        )
    }

    #[test]
    fn test_provider_catalog_metadata_assertions_are_skipped() {
        let output = r#"
{
  "assertions": [
    {
      "subject_text": "Source document Ada Example",
      "predicate_text": "last modified",
      "object_text": null,
      "object_literal": {"type": "date", "value": "2026-05-17"},
      "evidence": {"start": 1, "end": 44, "quote": "Source document Ada Example last modified 2026-05-17"},
      "confidence": 1.0,
      "source_event_time": null,
      "source_event_time_precision": null,
      "event_title": null,
      "subject_entity_id": null,
      "subject_entity_type": "topic",
      "object_entity_id": null,
      "object_entity_type": null,
      "significance": "high"
    },
    {
      "subject_text": "Ada Example",
      "predicate_text": "last modified",
      "object_text": null,
      "object_literal": {"type": "date", "value": "2026-05-17"},
      "evidence": {"start": 45, "end": 80, "quote": "Ada Example last modified 2026-05-17"},
      "confidence": 0.9,
      "source_event_time": null,
      "source_event_time_precision": null,
      "event_title": null,
      "subject_entity_id": null,
      "subject_entity_type": "person",
      "object_entity_id": null,
      "object_entity_type": null,
      "significance": "medium"
    }
  ]
}
"#;

        let assertions = parse_llm_output(output).expect("valid output");

        assert_eq!(
            assertions.len(),
            1,
            "provider catalog metadata should be skipped without suppressing ordinary subjects"
        );
        assert_eq!(assertions[0].subject_text, "Ada Example");
    }

    #[test]
    fn test_core_materialization_only_blocks_structural_and_scalar_labels() {
        assert!(should_materialize_entity_label(
            "Alex, Riley, Morgan and Taylor",
            Some("group")
        ));
        assert!(should_materialize_entity_label(
            "No workshop due to review meeting",
            Some("topic")
        ));
        assert!(!should_materialize_entity_label(
            "Video call: ProjectA <> ProjectB",
            Some("topic")
        ));
        assert!(should_materialize_entity_label(
            "week 46 and week 47",
            Some("topic")
        ));
        assert!(should_materialize_entity_label(
            "Example Organization, class A1/Blue",
            Some("org")
        ));
        assert!(should_materialize_entity_label(
            "Example Organization, class 4B",
            Some("topic")
        ));
        assert!(!should_materialize_entity_label("15:30", Some("topic")));
        assert!(!should_materialize_entity_label("true", Some("topic")));
        assert!(!should_materialize_entity_label("confirmed", Some("topic")));
        assert!(!should_materialize_entity_label(
            "2026-05-20T16:00:00Z",
            Some("topic")
        ));
        assert!(!should_materialize_entity_label(
            "123-45-6789",
            Some("person")
        ));
        assert!(!should_materialize_entity_label(
            "66.40.184.65",
            Some("topic")
        ));
        assert!(should_materialize_entity_label(
            "Portland, Oregon",
            Some("location")
        ));
        assert!(should_materialize_entity_label(
            "1200 Market Street, Springfield, IL",
            Some("location")
        ));
        assert!(should_materialize_entity_label(
            "Research, Development and Operations",
            Some("org")
        ));
    }

    #[test]
    fn test_parse_llm_output_skips_assertions_with_non_entity_labels() {
        let output = r#"
{
  "assertions": [
    {
      "subject_text": "Video call: ProjectA <> ProjectB",
      "predicate_text": "mentions",
      "object_text": "Example Group",
      "object_literal": null,
      "evidence": {"start": 1, "end": 36, "quote": "Video call: ProjectA <> ProjectB"},
      "confidence": 0.9,
      "source_event_time": null,
      "source_event_time_precision": null,
      "event_title": null,
      "subject_entity_id": null,
      "subject_entity_type": "topic",
      "object_entity_id": null,
      "object_entity_type": "group",
      "significance": "medium"
    },
    {
      "subject_text": "Ada Example",
      "predicate_text": "starts_at",
      "object_text": "15:30",
      "object_literal": null,
      "evidence": {"start": 45, "end": 50, "quote": "15:30"},
      "confidence": 0.9,
      "source_event_time": null,
      "source_event_time_precision": null,
      "event_title": null,
      "subject_entity_id": null,
      "subject_entity_type": "person",
      "object_entity_id": null,
      "object_entity_type": "topic",
      "significance": "medium"
    },
    {
      "subject_text": "Ada Example",
      "predicate_text": "member_of",
      "object_text": "Example Organization",
      "object_literal": null,
      "evidence": {"start": 91, "end": 117, "quote": "Ada Example is a member of Example Organization"},
      "confidence": 0.95,
      "source_event_time": null,
      "source_event_time_precision": null,
      "event_title": null,
      "subject_entity_id": null,
      "subject_entity_type": "person",
      "object_entity_id": null,
      "object_entity_type": "org",
      "significance": "high"
    }
  ]
}
"#;

        let assertions = parse_llm_output(output).expect("valid output");

        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].subject_text, "Ada Example");
        assert_eq!(
            assertions[0].object_text.as_deref(),
            Some("Example Organization")
        );
    }

    #[test]
    fn test_config_new() {
        let config = AgenticExtractorConfig::new("http://localhost:3001/mcp", "gpt-4o");
        assert_eq!(config.mcp_url, "http://localhost:3001/mcp");
        assert_eq!(config.model, "gpt-4o");
        assert_eq!(config.budget.max_tokens, 500_000);
    }

    #[test]
    fn test_config_with_budget() {
        let budget = ExtractionBudget {
            max_tokens: 50_000,
            max_wall_time_secs: 300,
        };
        let config = AgenticExtractorConfig::new("http://localhost:3001/mcp", "claude-3-opus")
            .with_budget(budget);

        assert_eq!(config.budget.max_tokens, 50_000);
        assert_eq!(config.budget.max_wall_time_secs, 300);
    }

    #[test]
    fn test_extractor_manifest() {
        let extractor = AgenticExtractor::new(make_config());
        let manifest = extractor.manifest();

        assert_eq!(manifest.plugin_id, "elephant.extractor.agentic");
        assert_eq!(manifest.plugin_type, "extractor");
        assert_eq!(manifest.determinism, Determinism::AgenticLoop);
        assert!(manifest
            .tool_access
            .contains(&"search_entities".to_string()));
        assert!(manifest.tool_access.contains(&"get_entity".to_string()));
        assert!(manifest
            .tool_access
            .contains(&"list_predicates".to_string()));
    }

    #[test]
    fn test_extractor_accessors() {
        let config = AgenticExtractorConfig::new("http://localhost:3001/mcp", "gpt-4o")
            .with_budget(ExtractionBudget {
                max_tokens: 20_000,
                max_wall_time_secs: 120,
            })
            .with_connect_on_init(false);
        let extractor = AgenticExtractor::new(config);

        assert_eq!(extractor.mcp_url(), "http://localhost:3001/mcp");
        assert_eq!(extractor.model(), "gpt-4o");
        assert_eq!(extractor.budget().max_tokens, 20_000);
    }

    #[tokio::test]
    async fn test_extract_no_longer_requires_anthropic_api_key() {
        let extractor = AgenticExtractor::new(make_config());
        let ctx = make_context();

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await;
        assert!(
            matches!(result, Err(PipelineError::ExtractionError(ref message)) if message.contains("MCP connection failed")),
            "expected MCP connectivity failure without provider gating, got: {result:?}"
        );

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[tokio::test]
    async fn test_health_check_unreachable() {
        let mut config = make_config();
        config.mcp_url = "http://127.0.0.1:1/mcp".to_string();
        let extractor = AgenticExtractor::new(config);
        let healthy = extractor.health_check().await.expect("health check");
        assert!(!healthy);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // parse_llm_output tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_llm_output_valid() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert_eq!(a.subject_text, "Alice");
        assert_eq!(a.predicate_text, "works at");
        assert_eq!(a.object_text, Some("Acme Corp".to_string()));
        assert!(a.object_literal.is_none());
        assert_eq!(a.evidence_span.start, 0);
        assert_eq!(a.evidence_span.end, 25);
        assert_eq!(a.confidence, 0.95);
        assert_eq!(a.quote, Some("Alice works at Acme Corp".to_string()));
    }

    #[test]
    fn test_parse_llm_output_with_literal() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "age",
                    "object_text": null,
                    "object_literal": { "type": "integer", "value": 30 },
                    "evidence": { "start": 10, "end": 25, "quote": "Alice is 30 years old" },
                    "confidence": 0.9
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert_eq!(a.subject_text, "Alice");
        assert_eq!(a.predicate_text, "age");
        assert!(a.object_text.is_none());
        assert!(matches!(
            a.object_literal,
            Some(elephant_types::records::attribute::Literal::Integer(30))
        ));
    }

    #[test]
    fn test_parse_llm_output_multiple_assertions() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                },
                {
                    "subject_text": "Bob",
                    "predicate_text": "manages",
                    "object_text": "Alice",
                    "evidence": { "start": 26, "end": 45, "quote": "Bob manages Alice" },
                    "confidence": 0.85
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 2);
        assert_eq!(assertions[0].subject_text, "Alice");
        assert_eq!(assertions[1].subject_text, "Bob");
    }

    #[test]
    fn test_parse_llm_output_empty_assertions() {
        let json = r#"{ "assertions": [] }"#;
        let assertions = parse_llm_output(json).expect("should parse");
        assert!(assertions.is_empty());
    }

    #[test]
    fn test_parse_llm_output_invalid_json() {
        let json = "not valid json";
        let err = parse_llm_output(json).expect_err("should fail");
        assert!(matches!(err, PipelineError::ConfigError(_)));

        if let PipelineError::ConfigError(msg) = err {
            assert!(msg.contains("Invalid LLM output JSON"));
        }
    }

    #[test]
    fn test_parse_llm_output_salvages_valid_rows_from_malformed_batch() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                },
                {
                    "predicate_text": "has role",
                    "object_text": "Principal",
                    "confidence": 0.8
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("valid rows should be salvaged");
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].subject_text, "Alice");
        assert_eq!(assertions[0].predicate_text, "works at");
    }

    #[test]
    fn test_parse_llm_output_missing_assertions_field() {
        let json = r#"{ "results": [] }"#;
        let err = parse_llm_output(json).expect_err("should fail");
        assert!(matches!(err, PipelineError::ConfigError(_)));
    }

    #[test]
    fn test_parse_llm_output_empty_subject() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "works at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let err = parse_llm_output(json).expect_err("should fail");
        assert!(matches!(err, PipelineError::InvalidInput(_)));

        if let PipelineError::InvalidInput(msg) = err {
            assert!(msg.contains("subject_text"));
        }
    }

    #[test]
    fn test_parse_llm_output_empty_predicate() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let err = parse_llm_output(json).expect_err("should fail");
        assert!(matches!(err, PipelineError::InvalidInput(_)));

        if let PipelineError::InvalidInput(msg) = err {
            assert!(msg.contains("predicate_text"));
        }
    }

    #[test]
    fn test_parse_llm_output_missing_object() {
        // Assertions with neither object_text nor object_literal are silently skipped
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": null,
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert!(
            assertions.is_empty(),
            "assertion with no object should be skipped"
        );
    }

    #[test]
    fn test_parse_llm_output_invalid_confidence() {
        // Confidence > 1.0 is clamped to 1.0 (not rejected)
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 1.5
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);
        assert!((assertions[0].confidence - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_llm_output_negative_confidence() {
        // Negative confidence is clamped to 0.0 (not rejected)
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": -0.1
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);
        assert!((assertions[0].confidence - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_llm_output_quote_hash_computed() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        let hash = &assertions[0].evidence_span.quote_hash;

        // Verify the hash is a valid hex string (64 chars for SHA-256)
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Verify same input produces same hash
        let expected = compute_quote_hash("Alice works at Acme Corp");
        assert_eq!(hash, &expected);
    }

    #[test]
    fn test_recover_evidence_span_from_text_for_entity_object() {
        let placeholder_revision =
            Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision));
        let mut assertion = RawAssertion {
            subject_text: "Alex".to_string(),
            predicate_text: "uses".to_string(),
            object_text: Some("codemob".to_string()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: placeholder_revision,
                node: None,
                start: 0,
                end: 0,
                quote_hash: String::new(),
            },
            confidence: 0.9,
            quote: None,
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("platform".to_string()),
            significance: Some("high".to_string()),
        };

        let text = "Observed evidence:\n- Alex uses codemob MCP tool.\n- Another line.";
        assert!(recover_evidence_span_from_text(&mut assertion, text));
        normalize_evidence_span(&mut assertion, text).expect("normalized");
        assert_eq!(
            assertion.quote.as_deref(),
            Some("- Alex uses codemob MCP tool.")
        );
        assert!(assertion.evidence_span.end > assertion.evidence_span.start);
        assert!(!assertion.evidence_span.quote_hash.is_empty());
    }

    #[test]
    fn test_recover_evidence_span_from_text_for_literal_object() {
        let placeholder_revision =
            Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision));
        let mut assertion = RawAssertion {
            subject_text: "ELEPHANT!".to_string(),
            predicate_text: "has_state".to_string(),
            object_text: None,
            object_literal: Some(Literal::String("Ready to merge".to_string())),
            evidence_span: EvidenceSpan {
                revision: placeholder_revision,
                node: None,
                start: 0,
                end: 0,
                quote_hash: String::new(),
            },
            confidence: 1.0,
            quote: None,
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("topic".to_string()),
            object_entity_id: None,
            object_entity_type: None,
            significance: Some("high".to_string()),
        };

        let text = "Status board:\nELEPHANT! Ready to merge\nMore notes.";
        assert!(recover_evidence_span_from_text(&mut assertion, text));
        normalize_evidence_span(&mut assertion, text).expect("normalized");
        assert_eq!(assertion.quote.as_deref(), Some("ELEPHANT! Ready to merge"));
        assert!(assertion.evidence_span.end > assertion.evidence_span.start);
    }

    #[test]
    fn test_estimated_extraction_text_tokens_uses_ceil_four_chars_per_token() {
        assert_eq!(estimated_extraction_text_tokens(""), 0);
        assert_eq!(estimated_extraction_text_tokens("abc"), 1);
        assert_eq!(estimated_extraction_text_tokens("abcd"), 1);
        assert_eq!(estimated_extraction_text_tokens("abcde"), 2);
    }

    #[test]
    fn test_parse_llm_output_literal_types() {
        // Test string literal
        let json = r#"{
            "assertions": [{
                "subject_text": "Alice",
                "predicate_text": "nickname",
                "object_literal": { "type": "string", "value": "Ali" },
                "evidence": { "start": 0, "end": 10, "quote": "Alice (Ali)" },
                "confidence": 0.9
            }]
        }"#;
        let assertions = parse_llm_output(json).expect("should parse string");
        assert!(matches!(
            assertions[0].object_literal,
            Some(elephant_types::records::attribute::Literal::String(_))
        ));

        // Test boolean literal
        let json = r#"{
            "assertions": [{
                "subject_text": "Alice",
                "predicate_text": "is_active",
                "object_literal": { "type": "boolean", "value": true },
                "evidence": { "start": 0, "end": 10, "quote": "Alice is active" },
                "confidence": 0.9
            }]
        }"#;
        let assertions = parse_llm_output(json).expect("should parse boolean");
        assert!(matches!(
            assertions[0].object_literal,
            Some(elephant_types::records::attribute::Literal::Boolean(true))
        ));

        // Test float literal
        let json = r#"{
            "assertions": [{
                "subject_text": "Product",
                "predicate_text": "price",
                "object_literal": { "type": "float", "value": 19.99 },
                "evidence": { "start": 0, "end": 10, "quote": "costs $19.99" },
                "confidence": 0.9
            }]
        }"#;
        let assertions = parse_llm_output(json).expect("should parse float");
        if let Some(elephant_types::records::attribute::Literal::Float(f)) =
            assertions[0].object_literal
        {
            assert!((f - 19.99).abs() < 0.001);
        } else {
            panic!("Expected Float literal");
        }

        // Test date literal
        let json = r#"{
            "assertions": [{
                "subject_text": "Alice",
                "predicate_text": "birth_date",
                "object_literal": { "type": "date", "value": "1990-01-15" },
                "evidence": { "start": 0, "end": 10, "quote": "born Jan 15, 1990" },
                "confidence": 0.9
            }]
        }"#;
        let assertions = parse_llm_output(json).expect("should parse date");
        assert!(matches!(
            assertions[0].object_literal,
            Some(elephant_types::records::attribute::Literal::Date(_))
        ));
    }

    #[test]
    fn test_agentic_extractor_logs_search_entities_tool_call() {
        run_factory_test(|| async {
            let mut config = make_config();
            config.model = "mock-model".to_string();
            let extractor = AgenticExtractor::new(config);
            let ctx = make_context();

            let calls = Arc::new(Mutex::new(Vec::new()));
            let tools = vec![ToolDef::new(
                "search_entities",
                "Search entities",
                json!({
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                    "additionalProperties": false
                }),
            )];
            let dispatcher = Arc::new(RecordingDispatcher::new(calls.clone(), tools));

            let first = LlmStreamResult::new(
                vec![AssistantBlock::ToolUse {
                    id: "call-1".to_string(),
                    name: "search_entities".to_string(),
                    args: serde_json::value::to_raw_value(&json!({"query": "Alice"})).expect("raw"),
                    meta: None,
                }],
                StopReason::ToolUse,
                Usage {
                    input_tokens: 5,
                    output_tokens: 5,
                    cache_creation_tokens: None,
                    cache_read_tokens: None,
                    ..Usage::default()
                },
            );

            let response_json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 23, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95,
                    "source_event_time": null,
                    "source_event_time_precision": null,
                    "event_title": null,
                    "subject_entity_id": null,
                    "subject_entity_type": "person",
                    "object_entity_id": null,
                    "object_entity_type": "org",
                    "significance": null
                }
            ]
        }"#;

            let second = LlmStreamResult::new(
                vec![AssistantBlock::Text {
                    text: response_json.to_string(),
                    meta: None,
                }],
                StopReason::EndTurn,
                Usage {
                    input_tokens: 5,
                    output_tokens: 5,
                    cache_creation_tokens: None,
                    cache_read_tokens: None,
                    ..Usage::default()
                },
            );

            // The schema-complete final turn is accepted without another call.
            let llm = Arc::new(MockLlmClient::new(vec![first, second]));

            let result = extractor
                .run_agent(ctx, dispatcher, llm.clone())
                .await
                .expect("extract");

            assert_eq!(result.assertion_count(), 1);
            assert_eq!(result.tokens_used, 20);
            assert_eq!(llm.call_count(), 2);
            let recorded = calls.lock().expect("lock calls");
            assert_eq!(*recorded, ["search_entities"]);
        });
    }

    #[test]
    fn test_agentic_extractor_budget_enforced() {
        run_factory_test(|| async {
            let mut config = make_config();
            config.model = "mock-model".to_string();
            let extractor = AgenticExtractor::new(config);
            let ctx = make_context().with_budget(ExtractionBudget {
                max_tokens: 1,
                max_wall_time_secs: 60,
            });

            let dispatcher = Arc::new(RecordingDispatcher::new(
                Arc::new(Mutex::new(Vec::new())),
                Vec::new(),
            ));

            let response_json = r#"{"assertions": []}"#;

            let response = LlmStreamResult::new(
                vec![AssistantBlock::Text {
                    text: response_json.to_string(),
                    meta: None,
                }],
                StopReason::EndTurn,
                Usage {
                    input_tokens: 10,
                    output_tokens: 10,
                    cache_creation_tokens: None,
                    cache_read_tokens: None,
                    ..Usage::default()
                },
            );

            // A fallback response is available to prove the budget stops the
            // run before another model call can consume it.
            let extraction_turn = LlmStreamResult::new(
                vec![AssistantBlock::Text {
                    text: response_json.to_string(),
                    meta: None,
                }],
                StopReason::EndTurn,
                Usage {
                    input_tokens: 10,
                    output_tokens: 10,
                    cache_creation_tokens: None,
                    cache_read_tokens: None,
                    ..Usage::default()
                },
            );

            let llm = Arc::new(MockLlmClient::new(vec![response, extraction_turn]));
            let result = extractor.run_agent(ctx, dispatcher, llm.clone()).await;
            assert!(matches!(
                result,
                Err(PipelineError::ExtractionError(ref message)) if message.contains("BudgetExhausted")
            ));
            assert_eq!(llm.call_count(), 1, "budget must stop a second model call");
        });
    }

    #[test]
    fn test_agentic_extractor_rejects_exhausted_structured_extraction() {
        run_factory_test(|| async {
            let mut config = make_config();
            config.model = "mock-model".to_string();
            let extractor = AgenticExtractor::new(config);
            let dispatcher = Arc::new(RecordingDispatcher::new(
                Arc::new(Mutex::new(Vec::new())),
                Vec::new(),
            ));

            // The legacy text parser accepts omitted nullable fields, while
            // the authoritative structured-output schema requires them.
            let primary_output = r#"{
                "assertions": [{
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": {
                        "start": 0,
                        "end": 23,
                        "quote": "Alice works at Acme Corp"
                    },
                    "confidence": 0.95
                }]
            }"#;
            assert_eq!(
                parse_llm_output(primary_output)
                    .expect("the permissive parser accepts the primary output")
                    .len(),
                1
            );

            // One main turn plus the initial extraction and two validation
            // retries. Meerkat returns the original text with extraction_error
            // after these attempts, and Elephant must preserve that failure.
            let responses = [primary_output, "invalid 1", "invalid 2", "invalid 3"]
                .into_iter()
                .map(|text| {
                    LlmStreamResult::new(
                        vec![AssistantBlock::Text {
                            text: text.to_string(),
                            meta: None,
                        }],
                        StopReason::EndTurn,
                        Usage {
                            input_tokens: 5,
                            output_tokens: 5,
                            ..Usage::default()
                        },
                    )
                })
                .collect();
            let llm = Arc::new(MockLlmClient::new(responses));
            let result = extractor
                .run_agent(make_context(), dispatcher, llm.clone())
                .await;

            assert!(matches!(
                result,
                Err(PipelineError::ExtractionError(ref message))
                    if message.contains("Structured extraction failed after 3 attempts")
            ));
            assert_eq!(llm.call_count(), 4, "all schema retries must be exhausted");
        });
    }

    #[test]
    fn test_parse_llm_output_with_source_event_time() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "joined",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 30, "quote": "Alice joined Acme in 2023" },
                    "confidence": 0.95,
                    "source_event_time": "2023-01-15T10:30:00Z"
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert_eq!(a.subject_text, "Alice");
        assert_eq!(a.predicate_text, "joined");
        assert!(a.source_event_time.is_some());

        let event_time = a.source_event_time.unwrap();
        assert_eq!(event_time.year(), 2023);
        assert_eq!(event_time.month(), 1);
        assert_eq!(event_time.day(), 15);
    }

    #[test]
    fn test_parse_llm_output_without_source_event_time() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95,
                    "source_event_time": null
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert!(a.source_event_time.is_none());
    }

    #[test]
    fn test_parse_llm_output_source_event_time_defaults_to_none() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert!(a.source_event_time.is_none());
    }

    #[test]
    fn test_parse_llm_output_source_event_time_accepts_date_only() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "joined",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 30, "quote": "Alice joined Acme in March" },
                    "confidence": 0.95,
                    "source_event_time": "2023-03-01",
                    "source_event_time_precision": "month"
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        let event_time = a.source_event_time.expect("event time should be parsed");
        assert_eq!(event_time.year(), 2023);
        assert_eq!(event_time.month(), 3);
        assert_eq!(event_time.day(), 1);
        assert_eq!(
            a.source_event_time_precision,
            Some(elephant_types::records::TimePrecision::Month)
        );
    }

    #[test]
    fn test_parse_llm_output_source_event_time_invalid_does_not_fail_batch() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "met",
                    "object_text": "Bob",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 16, "quote": "Alice met Bob" },
                    "confidence": 0.8,
                    "source_event_time": "2026-03-",
                    "source_event_time_precision": "weird_precision"
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert!(a.source_event_time.is_none());
        assert!(a.source_event_time_precision.is_none());
    }

    #[test]
    fn test_parse_agent_run_output_rejects_empty_unstructured_result() {
        let err = parse_agent_run_output("gemini-3.1-flash-lite-preview", None, "")
            .expect_err("empty unstructured result should fail");
        match err {
            PipelineError::ExtractionError(message) => {
                assert!(message.contains("gemini-3.1-flash-lite-preview"));
                assert!(message.contains("structured output was absent"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_parse_agent_run_output_rejects_unparseable_structured_without_fallback_text() {
        let err = parse_agent_run_output(
            "gemini-3.1-flash-lite-preview",
            Some(&json!({"assertions":[{"subject_text":"Alice"}]})),
            "   ",
        )
        .expect_err("unparseable structured result should fail when fallback text is empty");
        match err {
            PipelineError::ExtractionError(message) => {
                assert!(message.contains("gemini-3.1-flash-lite-preview"));
                assert!(message.contains("could not be deserialized"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_extraction_output_schema_structure() {
        let schema = extraction_output_schema();

        // Verify top-level structure
        assert_eq!(schema["type"], "object");
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("assertions")));

        // Verify assertions array structure
        let assertions_schema = &schema["properties"]["assertions"];
        assert_eq!(assertions_schema["type"], "array");

        // Verify assertion item structure
        let item_schema = &assertions_schema["items"];
        assert_eq!(item_schema["type"], "object");

        // All properties must be in required (OpenAI strict mode demands this)
        let required = item_schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("subject_text")));
        assert!(required.contains(&json!("predicate_text")));
        assert!(required.contains(&json!("confidence")));
        assert!(required.contains(&json!("object_text")));
        assert!(required.contains(&json!("evidence")));
        assert!(required.contains(&json!("significance")));
        assert!(required.contains(&json!("source_event_time_precision")));
        assert!(required.contains(&json!("event_title")));

        let property_names = item_schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let required_names = required
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            property_names, required_names,
            "OpenAI/strict structured outputs require every property to appear in required"
        );

        // Verify evidence substructure (nullable object)
        let evidence_schema = &item_schema["properties"]["evidence"];
        let evidence_type = &evidence_schema["type"];
        assert!(
            evidence_type
                .as_array()
                .map_or(false, |arr| arr.contains(&json!("null"))),
            "evidence type should include null"
        );

        // Verify confidence field exists and is a number type
        // Note: minimum/maximum constraints were removed for LLM schema compatibility
        let confidence_schema = &item_schema["properties"]["confidence"];
        assert_eq!(confidence_schema["type"], "number");
    }

    #[test]
    fn test_agentic_budget_limits_use_real_budget() {
        let budget = ExtractionBudget {
            max_tokens: 20_000,
            max_wall_time_secs: 120,
        };

        let limits = agentic_budget_limits(&budget);

        assert_eq!(limits.max_tokens, Some(20_000));
        assert_eq!(limits.max_duration, Some(Duration::from_secs(120)));
        assert_eq!(limits.max_tool_calls, Some(24));
    }

    #[test]
    fn test_build_extraction_prompt() {
        let prompt = build_extraction_prompt(
            "some text",
            Some("Track project relationships and architecture decisions"),
            None,
            &[],
        );

        // Purpose section should be present
        assert!(
            prompt.contains("Track project relationships and architecture decisions"),
            "Prompt should contain the purpose text"
        );
        assert!(
            prompt.contains("Knowledge System Purpose"),
            "Prompt should contain the purpose header"
        );
        assert!(
            prompt.contains("Only extract assertions that are relevant to this purpose"),
            "Prompt should contain purpose guidance"
        );

        // The text replacement should still work
        assert!(
            prompt.contains("some text"),
            "Prompt should contain the input text"
        );

        // Template markers should be gone
        assert!(
            !prompt.contains("{{#if SPACE_PURPOSE}}"),
            "Template if-marker should be removed"
        );
        assert!(
            !prompt.contains("{{/if}}"),
            "Template endif-marker should be removed"
        );
        assert!(
            !prompt.contains("{{SPACE_PURPOSE}}"),
            "Template variable should be replaced"
        );
    }

    #[test]
    fn test_build_extraction_prompt_without_purpose() {
        let prompt = build_extraction_prompt("some text", None, None, &[]);

        // Purpose section should be absent
        assert!(
            !prompt.contains("Knowledge System Purpose"),
            "Prompt should NOT contain purpose header when no purpose given"
        );
        assert!(
            !prompt.contains("Only extract assertions that are relevant to this purpose"),
            "Prompt should NOT contain purpose guidance when no purpose given"
        );

        // Template markers should be gone
        assert!(
            !prompt.contains("{{#if SPACE_PURPOSE}}"),
            "Template if-marker should be removed"
        );
        assert!(
            !prompt.contains("{{/if}}"),
            "Template endif-marker should be removed"
        );

        // The text replacement should still work
        assert!(
            prompt.contains("some text"),
            "Prompt should contain the input text"
        );

        // Core extraction instructions should still be present
        assert!(
            prompt.contains("You are an information extraction system"),
            "Prompt should contain core instructions"
        );
    }

    #[test]
    fn test_build_extraction_prompt_with_known_entities() {
        let entities = vec![
            crate::extraction::KnownEntity {
                id: "entity:abc-123".to_string(),
                display_name: "Alice Chen".to_string(),
                entity_type: "person".to_string(),
            },
            crate::extraction::KnownEntity {
                id: "entity:def-456".to_string(),
                display_name: "NeuralFlow".to_string(),
                entity_type: "org".to_string(),
            },
        ];

        let prompt = build_extraction_prompt("some text", None, None, &entities);

        assert!(
            prompt.contains("Known Entities From This Document"),
            "Prompt should contain known entities header"
        );
        assert!(
            prompt.contains("Alice Chen"),
            "Prompt should contain entity name"
        );
        assert!(
            prompt.contains("entity:abc-123"),
            "Prompt should contain entity ID"
        );
        assert!(
            prompt.contains("NeuralFlow"),
            "Prompt should contain second entity"
        );
        assert!(
            prompt.contains("reuse their IDs"),
            "Prompt should instruct LLM to reuse IDs"
        );
        assert!(
            !prompt.contains("{{#if KNOWN_ENTITIES}}"),
            "Template markers should be removed"
        );
    }

    #[test]
    fn test_build_extraction_prompt_without_known_entities() {
        let prompt = build_extraction_prompt("some text", None, None, &[]);

        assert!(
            !prompt.contains("Known Entities From This Document"),
            "Prompt should NOT contain known entities header when empty"
        );
        assert!(
            !prompt.contains("{{KNOWN_ENTITIES}}"),
            "Template variable should be removed"
        );
    }

    #[test]
    fn test_build_extraction_prompt_with_skill() {
        let skill_body =
            "## Entity Types\n- Character -> person\n## Key Predicates\nknows, related_to";
        let prompt = build_extraction_prompt("some text", None, Some(skill_body), &[]);

        assert!(
            prompt.contains("Extraction Strategy"),
            "Prompt should contain extraction strategy header"
        );
        assert!(
            prompt.contains("Character -> person"),
            "Prompt should contain skill body content"
        );
        assert!(
            !prompt.contains("{{EXTRACTION_SKILL}}"),
            "Template variable should be replaced"
        );
    }

    #[test]
    fn test_build_extraction_prompt_with_skill_and_purpose() {
        let skill_body = "## Entity Types\n- Character -> person";
        let prompt = build_extraction_prompt(
            "some text",
            Some("Track fiction characters"),
            Some(skill_body),
            &[],
        );

        // Both should be present
        assert!(prompt.contains("Extraction Strategy"));
        assert!(prompt.contains("Character -> person"));
        assert!(prompt.contains("Track fiction characters"));
    }

    #[test]
    fn test_build_extraction_prompt_without_skill() {
        let prompt = build_extraction_prompt("some text", None, None, &[]);

        assert!(
            !prompt.contains("Extraction Strategy"),
            "Prompt should NOT contain strategy header when no skill"
        );
    }

    #[test]
    fn test_build_agentic_extraction_prompt_includes_multimodal_context() {
        let prompt = build_agentic_extraction_prompt(AgenticPromptInput {
            text: "Rendered projection text",
            doc_id: Some("doc:123"),
            doc_title: Some("Observation frame"),
            purpose: Some("extract observational evidence"),
            skill_body: Some("Prefer concrete people, apps, and sites"),
            known_entities: &[],
            sidecar_text: Some("User says this is a risky coding session"),
            source_timestamp: Some(
                &chrono::DateTime::parse_from_rfc3339("2026-04-10T14:00:00Z")
                    .expect("valid timestamp")
                    .with_timezone(&Utc),
            ),
            media_attachments: &[crate::extraction::ExtractionMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: elephant_types::records::doc::DocMediaKind::Video,
                media_type: "video/mp4".to_string(),
                location: elephant_types::records::doc::DocMediaLocation::Path {
                    path: "/tmp/test.mp4".to_string(),
                },
                label: Some("segment".to_string()),
                duration_ms: Some(90_000),
                time_range: Some(
                    elephant_types::records::doc_revision::RenderManifestTimeRange {
                        start_ms: Some(12_000),
                        end_ms: Some(24_000),
                    },
                ),
            }],
        });

        assert!(prompt.contains("Document title:\nObservation frame"));
        assert!(prompt.contains("Caller sidecar text:\nUser says this is a risky coding session"));
        assert!(prompt.contains("Source timestamp:\n2026-04-10T14:00:00+00:00"));
        assert!(prompt.contains("Attached media relevant to this node"));
        assert!(prompt.contains("Attachment id: att-1"));
        assert!(prompt.contains("Relevant time range ms: 12000 -> 24000"));
        assert!(prompt.contains(
            "Absolute time range: 2026-04-10T14:00:12+00:00 -> 2026-04-10T14:00:24+00:00"
        ));
        assert!(prompt.contains("convert them into absolute wall-clock time"));
        assert!(prompt.contains("source_event_time"));
        assert!(prompt.contains("Rendered projection text"));
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    async fn test_build_agentic_content_input_uses_multimodal_blocks() {
        use meerkat_core::{ContentBlock, ImageData};

        let mut media_path = std::env::temp_dir();
        media_path.push(format!(
            "elephant-agentic-image-{}-{}.jpg",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::write(&media_path, [0_u8, 1, 2, 3]).expect("write temp image");

        let input = build_agentic_content_input(
            "Inspect attached media".to_string(),
            &[crate::extraction::ExtractionMediaAttachment {
                attachment_id: "att-image".to_string(),
                kind: elephant_types::records::doc::DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: elephant_types::records::doc::DocMediaLocation::Path {
                    path: media_path.to_string_lossy().to_string(),
                },
                label: Some("frame".to_string()),
                duration_ms: None,
                time_range: None,
            }],
            None,
            "default",
        )
        .await
        .expect("build multimodal content input");

        match input {
            ContentInput::Blocks(blocks) => {
                assert_eq!(blocks.len(), 2);
                match &blocks[0] {
                    ContentBlock::Text { text } => assert_eq!(text, "Inspect attached media"),
                    other => panic!("expected text block, got {other:?}"),
                }
                match &blocks[1] {
                    ContentBlock::Image { media_type, data } => {
                        assert_eq!(media_type, "image/jpeg");
                        match data {
                            ImageData::Inline { data } => assert!(!data.is_empty()),
                            other => panic!("expected inline image data, got {other:?}"),
                        }
                    }
                    other => panic!("expected image block, got {other:?}"),
                }
            }
            other => panic!("expected block input, got {other:?}"),
        }

        let _ = std::fs::remove_file(media_path);
    }

    #[test]
    fn test_extraction_output_schema_includes_inline_promotion_fields() {
        let schema = extraction_output_schema();
        let item_props = &schema["properties"]["assertions"]["items"]["properties"];

        // Verify new inline promotion fields exist
        assert!(
            item_props.get("subject_entity_id").is_some(),
            "Schema should include subject_entity_id"
        );
        assert!(
            item_props.get("subject_entity_type").is_some(),
            "Schema should include subject_entity_type"
        );
        assert!(
            item_props.get("object_entity_id").is_some(),
            "Schema should include object_entity_id"
        );
        assert!(
            item_props.get("object_entity_type").is_some(),
            "Schema should include object_entity_type"
        );
        assert!(
            item_props.get("significance").is_some(),
            "Schema should include significance"
        );

        // Verify entity type is freeform string (nullable for OpenAI strict mode)
        let stype = &item_props["subject_entity_type"]["type"];
        assert!(
            stype.as_str() == Some("string")
                || stype
                    .as_array()
                    .map_or(false, |a| a.contains(&json!("string"))),
            "subject_entity_type should include string type"
        );
        assert!(
            item_props["subject_entity_type"].get("enum").is_none(),
            "subject_entity_type should not have an enum constraint"
        );

        // Verify significance is nullable string (no enum constraint for cross-provider compat)
        let sig_type = &item_props["significance"]["type"];
        assert!(
            sig_type
                .as_array()
                .map_or(false, |a| a.contains(&json!("string"))),
            "significance should be nullable string"
        );
    }

    #[test]
    fn test_parse_llm_output_with_inline_promotion_fields() {
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "object_literal": null,
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95,
                    "subject_entity_id": "entity:abc-123",
                    "subject_entity_type": "person",
                    "object_entity_id": null,
                    "object_entity_type": "org",
                    "significance": "high"
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert_eq!(a.subject_entity_id, Some("entity:abc-123".to_string()));
        assert_eq!(a.subject_entity_type, Some("person".to_string()));
        assert_eq!(a.object_entity_id, None);
        assert_eq!(a.object_entity_type, Some("org".to_string()));
        assert_eq!(a.significance, Some("high".to_string()));
    }

    #[test]
    fn test_parse_llm_output_without_inline_promotion_fields_backward_compat() {
        // Old-format output without the new fields should still parse
        let json = r#"{
            "assertions": [
                {
                    "subject_text": "Alice",
                    "predicate_text": "works at",
                    "object_text": "Acme Corp",
                    "evidence": { "start": 0, "end": 25, "quote": "Alice works at Acme Corp" },
                    "confidence": 0.95
                }
            ]
        }"#;

        let assertions = parse_llm_output(json).expect("should parse old format");
        assert_eq!(assertions.len(), 1);

        let a = &assertions[0];
        assert_eq!(a.subject_entity_id, None);
        assert_eq!(a.subject_entity_type, None);
        assert_eq!(a.object_entity_id, None);
        assert_eq!(a.object_entity_type, None);
        assert_eq!(a.significance, None);
    }
}
