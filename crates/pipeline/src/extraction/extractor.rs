//! Extractor trait and types for assertion extraction.
//!
//! Per spec: Extractors transform document nodes into raw assertions
//! which are unresolved claims with mention strings.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elephant_storage::StorageClient;

use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::attribute::Literal;
use elephant_types::records::base::Security;
use elephant_types::records::doc::{DocMediaKind, DocMediaLocation};
use elephant_types::records::doc_node::DocNode;
use elephant_types::records::doc_revision::{
    DocRevision, RenderManifestAttachment, RenderManifestTimeRange,
};
use elephant_types::records::event::TimePrecision;
use elephant_types::records::work::{Determinism, PluginManifest, RetryPolicy};
use elephant_types::{Link, RecordId};
use sha2::{Digest, Sha256};

use crate::error::PipelineError;

#[cfg(feature = "meerkat-extraction")]
use crate::extraction::{
    build_extraction_prompt, normalize_evidence_span, parse_llm_output_with_repair,
};
#[cfg(feature = "meerkat-extraction")]
use crate::llm::run_anthropic_prompt;
#[cfg(feature = "meerkat-extraction")]
use std::future::Future;
#[cfg(feature = "meerkat-extraction")]
use std::time::Duration;

/// Budget constraints for an extraction operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionBudget {
    /// Maximum tokens to use.
    pub max_tokens: u32,
    /// Maximum wall time in seconds.
    pub max_wall_time_secs: u32,
}

impl Default for ExtractionBudget {
    fn default() -> Self {
        Self {
            // Total token budget (input + output) for the meerkat agent.
            // Must accommodate: document text + prompt + skill body + output JSON.
            max_tokens: 500_000,
            max_wall_time_secs: 600,
        }
    }
}

/// An entity already known from earlier chunks of the same document.
///
/// Passed to the extractor so the LLM can reuse existing entity IDs
/// instead of creating duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownEntity {
    /// Entity record ID (e.g., "entity:35101236-...").
    pub id: String,
    /// Display name.
    pub display_name: String,
    /// Entity type (e.g., "person", "org").
    pub entity_type: String,
}

/// Media attachment context resolved for the node currently being extracted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionMediaAttachment {
    /// Attachment identifier from the parent document.
    pub attachment_id: String,
    /// Media kind.
    pub kind: DocMediaKind,
    /// MIME type.
    pub media_type: String,
    /// Attachment location.
    pub location: DocMediaLocation,
    /// Optional original label.
    pub label: Option<String>,
    /// Optional attachment duration.
    pub duration_ms: Option<u64>,
    /// Optional manifest-derived time range relevant to the current node span.
    pub time_range: Option<RenderManifestTimeRange>,
}

impl ExtractionMediaAttachment {
    /// Creates extraction context from an immutable revision manifest snapshot.
    #[must_use]
    pub fn from_manifest_attachment(
        attachment: &RenderManifestAttachment,
        time_range: Option<RenderManifestTimeRange>,
    ) -> Self {
        Self {
            attachment_id: attachment.attachment_id.clone(),
            kind: attachment.kind,
            media_type: attachment.media_type.clone(),
            location: attachment.location.clone(),
            label: attachment.label.clone(),
            duration_ms: attachment.duration_ms,
            time_range,
        }
    }
}

/// Context provided to extractors during execution.
#[derive(Debug, Clone)]
pub struct ExtractionContext {
    /// The node being extracted from.
    pub node_id: RecordId,
    /// The text content of the node.
    pub node_text: String,
    /// The revision containing the node.
    pub revision_id: RecordId,
    /// Space ID for storage-backed media lookups.
    pub space_id: String,
    /// Storage client for managed asset reads.
    pub storage: Option<StorageClient>,
    /// Security context for the extraction.
    pub security: Security,
    /// Budget constraints for this extraction.
    pub budget: ExtractionBudget,
    /// Optional space purpose to guide extraction relevance.
    pub purpose: Option<String>,
    /// Entities already identified from earlier chunks of this document.
    pub known_entities: Vec<KnownEntity>,
    /// Optional extraction skill body (loaded from SKILL.md via FilesystemSkillSource).
    pub skill_body: Option<String>,
    /// Optional document ID for tool-based text access (agentic extractor).
    pub doc_id: Option<String>,
    /// Optional document title for multimodal extraction instructions.
    pub doc_title: Option<String>,
    /// Optional caller-provided sidecar text preserved separately from rendered text.
    pub sidecar_text: Option<String>,
    /// Optional source timestamp for the evidence item backing this extraction.
    pub source_timestamp: Option<DateTime<Utc>>,
    /// Media attachments relevant to this node span.
    pub media_attachments: Vec<ExtractionMediaAttachment>,
}

impl ExtractionContext {
    /// Creates a new extraction context.
    #[must_use]
    pub fn new(
        node_id: RecordId,
        node_text: String,
        revision_id: RecordId,
        space_id: String,
        security: Security,
    ) -> Self {
        Self {
            node_id,
            node_text,
            revision_id,
            space_id,
            storage: None,
            security,
            budget: ExtractionBudget::default(),
            purpose: None,
            known_entities: Vec::new(),
            skill_body: None,
            doc_id: None,
            doc_title: None,
            sidecar_text: None,
            source_timestamp: None,
            media_attachments: Vec::new(),
        }
    }

    /// Sets the storage client for managed asset lookups.
    pub fn with_storage(mut self, storage: Option<StorageClient>) -> Self {
        self.storage = storage;
        self
    }

    /// Sets the budget for this context.
    pub fn with_budget(mut self, budget: ExtractionBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Sets the space purpose for this context.
    pub fn with_purpose(mut self, purpose: Option<String>) -> Self {
        self.purpose = purpose;
        self
    }

    /// Sets known entities from earlier chunks of this document.
    pub fn with_known_entities(mut self, known_entities: Vec<KnownEntity>) -> Self {
        self.known_entities = known_entities;
        self
    }

    /// Sets the extraction skill body.
    pub fn with_skill_body(mut self, skill_body: Option<String>) -> Self {
        self.skill_body = skill_body;
        self
    }

    /// Sets the document identifier for tool-based text access.
    pub fn with_doc_id(mut self, doc_id: Option<String>) -> Self {
        self.doc_id = doc_id;
        self
    }

    /// Sets the document title for multimodal extraction prompts.
    pub fn with_doc_title(mut self, doc_title: Option<String>) -> Self {
        self.doc_title = doc_title;
        self
    }

    /// Sets caller-provided sidecar text for multimodal extraction.
    pub fn with_sidecar_text(mut self, sidecar_text: Option<String>) -> Self {
        self.sidecar_text = sidecar_text;
        self
    }

    /// Sets source timestamp context for multimodal extraction.
    pub fn with_source_timestamp(mut self, source_timestamp: Option<DateTime<Utc>>) -> Self {
        self.source_timestamp = source_timestamp;
        self
    }

    /// Sets media attachments relevant to the node currently being extracted.
    pub fn with_media_attachments(
        mut self,
        media_attachments: Vec<ExtractionMediaAttachment>,
    ) -> Self {
        self.media_attachments = media_attachments;
        self
    }
}

/// A raw assertion extracted from a document node.
///
/// This is pre-resolution data - the mention strings have not yet
/// been resolved to entity IDs.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)] // Contains f64
pub struct RawAssertion {
    /// Raw subject mention text.
    pub subject_text: String,
    /// Raw predicate text (uncanonicalized).
    pub predicate_text: String,
    /// Raw object mention text (for entity-to-entity claims).
    pub object_text: Option<String>,
    /// Literal value (for entity-to-literal claims).
    pub object_literal: Option<Literal>,
    /// Evidence span pointing to source location.
    pub evidence_span: EvidenceSpan,
    /// Confidence score from extraction (0.0 to 1.0).
    pub confidence: f64,
    /// Optional direct quote from the source text.
    pub quote: Option<String>,
    /// Optional extractor-provided event time (when the claim occurred in real world).
    /// Per spec §2.3 and §12.1: allows extractors to specify when an event occurred.
    pub source_event_time: Option<DateTime<Utc>>,
    /// Optional precision for `source_event_time`.
    pub source_event_time_precision: Option<TimePrecision>,
    /// Optional event title hint from extraction output.
    pub event_title: Option<String>,
    /// Optional stable lineage key for event identity/refinement.
    pub event_lineage_key: Option<String>,
    /// Existing entity ID if the LLM found a match via search, null for new entities.
    pub subject_entity_id: Option<String>,
    /// Entity type classification for the subject (e.g. "person", "org", "topic").
    pub subject_entity_type: Option<String>,
    /// Existing entity ID if the LLM found a match via search, null for new entities.
    pub object_entity_id: Option<String>,
    /// Entity type classification for the object (e.g. "person", "org", "topic").
    pub object_entity_type: Option<String>,
    /// Significance assessment: "skip", "low", "medium", or "high".
    pub significance: Option<String>,
}

/// Result of an extraction operation.
#[derive(Debug, Clone)]
pub struct ExtractionResult {
    /// Raw assertions extracted from the node.
    pub raw_assertions: Vec<RawAssertion>,
    /// Tokens used during extraction.
    pub tokens_used: u32,
    /// Model used for extraction.
    pub model: String,
}

impl ExtractionResult {
    /// Creates a new extraction result.
    #[must_use]
    pub fn new(raw_assertions: Vec<RawAssertion>, tokens_used: u32, model: String) -> Self {
        Self {
            raw_assertions,
            tokens_used,
            model,
        }
    }

    /// Creates an empty extraction result.
    #[must_use]
    pub fn empty(model: String) -> Self {
        Self {
            raw_assertions: Vec::new(),
            tokens_used: 0,
            model,
        }
    }

    /// Returns the number of assertions extracted.
    #[must_use]
    pub fn assertion_count(&self) -> usize {
        self.raw_assertions.len()
    }
}

/// Trait for document extractors.
///
/// Per spec: Extractors declare a manifest and transform document nodes
/// into raw assertions for storage and later resolution.
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Returns the extractor's manifest.
    fn manifest(&self) -> &PluginManifest;

    /// Extracts assertions from a document node.
    ///
    /// # Errors
    ///
    /// Returns an error if extraction fails.
    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError>;

    /// Optional health check.
    async fn health_check(&self) -> Result<bool, PipelineError> {
        Ok(true)
    }
}

/// Registry of extractors.
#[derive(Default)]
pub struct ExtractorRegistry {
    extractors: Vec<Box<dyn Extractor>>,
}

impl ExtractorRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an extractor.
    pub fn register(&mut self, extractor: Box<dyn Extractor>) {
        self.extractors.push(extractor);
    }

    /// Returns all registered extractors.
    #[must_use]
    pub fn all(&self) -> &[Box<dyn Extractor>] {
        &self.extractors
    }

    /// Returns the number of registered extractors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.extractors.len()
    }

    /// Returns true if no extractors are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.extractors.is_empty()
    }

    /// Finds an extractor by plugin ID.
    #[must_use]
    pub fn find_by_id(&self, plugin_id: &str) -> Option<&dyn Extractor> {
        self.extractors
            .iter()
            .find(|e| e.manifest().plugin_id == plugin_id)
            .map(|e| e.as_ref())
    }

    /// Creates a registry with all available extractors registered.
    ///
    /// The registry will include:
    /// - `DefaultExtractor` (legacy/test-only; not used by the active runtime)
    /// - `AgenticExtractor` (when `meerkat-extraction` feature is enabled)
    #[must_use]
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(Box::new(DefaultExtractor::new()));

        #[cfg(feature = "meerkat-extraction")]
        {
            use super::agentic_extractor::{AgenticExtractor, AgenticExtractorConfig};
            registry.register(Box::new(AgenticExtractor::new(
                AgenticExtractorConfig::default(),
            )));
        }

        registry
    }

    /// Creates a registry with the agentic extractor using custom configuration.
    ///
    /// # Arguments
    ///
    /// * `mcp_url` - URL of the Elephant MCP server (agent profile endpoint)
    /// * `model` - LLM model identifier
    #[cfg(feature = "meerkat-extraction")]
    #[must_use]
    pub fn with_agentic(mcp_url: impl Into<String>, model: impl Into<String>) -> Self {
        use super::agentic_extractor::{AgenticExtractor, AgenticExtractorConfig};
        let mut registry = Self::new();
        registry.register(Box::new(DefaultExtractor::new()));
        registry.register(Box::new(AgenticExtractor::new(
            AgenticExtractorConfig::new(mcp_url, model),
        )));
        registry
    }
}

/// Default extractor implementation for testing and fallback.
#[derive(Debug, Clone)]
/// Legacy/test-only extractor retained for non-runtime unit tests.
///
/// The active Elephant runtime no longer uses this as a semantic fallback.
pub struct DefaultExtractor {
    manifest: PluginManifest,
    model: String,
}

impl Default for DefaultExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultExtractor {
    /// Creates a new default extractor.
    #[must_use]
    pub fn new() -> Self {
        let model = "gpt-5.2".to_string();
        let manifest = PluginManifest {
            plugin_id: "elephant.extractor.default".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "extractor".to_string(),
            required_scopes: vec!["read:docs".to_string(), "write:assertions".to_string()],
            tool_access: vec![
                "fetch_node_text".to_string(),
                "list_revision_nodes".to_string(),
                "search_entities".to_string(),
                "fetch_entity_context".to_string(),
                "fetch_predicate_registry".to_string(),
            ],
            work_kinds: vec!["extract_assertions".to_string()],
            determinism: Determinism::LlmSingleShot,
            max_tokens: Some(10_000),
            max_wall_time_secs: Some(120),
            input_types: vec!["doc_node".to_string()],
            output_types: vec!["knowledge_candidates".to_string()],
            validator_profile: Some("assertion_validator".to_string()),
            config_schema: None,
            health_check: None,
            retry_policy: RetryPolicy::default(),
        };

        Self { manifest, model }
    }

    fn fallback_extract(&self, ctx: &ExtractionContext) -> ExtractionResult {
        let mut assertions = Vec::new();
        assertions.extend(extract_meeting_notes(ctx));
        assertions.extend(extract_joined_as(ctx));
        assertions.extend(extract_has_title(ctx));
        assertions.extend(extract_mentors(ctx));
        if let Some(assertion) = extract_works_at(ctx) {
            assertions.push(assertion);
        }
        ExtractionResult::new(assertions, 0, self.model.clone())
    }

    #[cfg(feature = "meerkat-extraction")]
    async fn run_with_wall_time_budget<T>(
        &self,
        max_wall_time_secs: u32,
        call: impl Future<Output = Result<T, PipelineError>>,
    ) -> Result<T, PipelineError> {
        if max_wall_time_secs == 0 {
            return Err(PipelineError::ExtractionError(
                "Extraction wall-time budget exceeded: max_wall_time_secs=0".to_string(),
            ));
        }

        let timeout = Duration::from_secs(max_wall_time_secs as u64);
        match tokio::time::timeout(timeout, call).await {
            Ok(result) => result,
            Err(_) => Err(PipelineError::ExtractionError(format!(
                "Extraction wall-time budget exceeded: max_wall_time_secs={max_wall_time_secs}"
            ))),
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    async fn llm_extract(
        &self,
        ctx: &ExtractionContext,
        api_key: String,
    ) -> Result<ExtractionResult, PipelineError> {
        let prompt = build_extraction_prompt(
            &ctx.node_text,
            ctx.purpose.as_deref(),
            ctx.skill_body.as_deref(),
            &ctx.known_entities,
        );
        // Output token limit — cap at 64K to match the agentic path.
        // ctx.budget.max_tokens is the total budget (input+output), not the output limit.
        let max_tokens = ctx.budget.max_tokens.clamp(1, 64_000);
        let run_result = self
            .run_with_wall_time_budget(
                ctx.budget.max_wall_time_secs,
                run_anthropic_prompt(
                    api_key,
                    self.model.clone(),
                    Some("You are an information extraction system. Follow the user instructions and return only valid JSON."),
                    prompt,
                    max_tokens,
                    None,
                ),
            )
            .await?;

        let parsed_assertions = parse_llm_output_with_repair(&run_result.text)?;
        let mut raw_assertions = Vec::new();
        for mut assertion in parsed_assertions {
            assertion.evidence_span.revision = Link::new(ctx.revision_id.clone());
            assertion.evidence_span.node = Some(Link::new(ctx.node_id.clone()));
            match normalize_evidence_span(&mut assertion, &ctx.node_text) {
                Ok(()) => raw_assertions.push(assertion),
                Err(e) => {
                    tracing::warn!(
                        "Dropping assertion '{}' → '{}': {e}",
                        assertion.subject_text,
                        assertion.predicate_text,
                    );
                }
            }
        }

        let tokens_used = run_result.usage.total_tokens();
        if tokens_used > ctx.budget.max_tokens as u64 {
            return Err(PipelineError::ExtractionError(format!(
                "Extraction budget exceeded: used {tokens_used} > max {}",
                ctx.budget.max_tokens
            )));
        }

        Ok(ExtractionResult::new(
            raw_assertions,
            u32::try_from(tokens_used).unwrap_or(u32::MAX),
            self.model.clone(),
        ))
    }
}

#[async_trait]
impl Extractor for DefaultExtractor {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        #[cfg(feature = "meerkat-extraction")]
        {
            match std::env::var("ANTHROPIC_API_KEY")
                .ok()
                .map(|key| key.trim().to_string())
            {
                Some(api_key) if !api_key.is_empty() => match self.llm_extract(&ctx, api_key).await
                {
                    Ok(result) => {
                        if !result.raw_assertions.is_empty() {
                            return Ok(result);
                        }
                        tracing::warn!(
                            "DefaultExtractor LLM returned 0 assertions; falling back to heuristic extraction"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "DefaultExtractor LLM call failed, falling back to heuristic extraction"
                        );
                    }
                },
                _ => {
                    tracing::warn!(
                        "ANTHROPIC_API_KEY not set, falling back to heuristic extraction"
                    );
                }
            }
        }

        Ok(self.fallback_extract(&ctx))
    }
}

fn compute_quote_hash(quote: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(quote.as_bytes());
    hex::encode(hasher.finalize())
}

fn trim_object_text(text: &str) -> &str {
    text.trim().trim_end_matches(['.', ',', ';']).trim()
}

fn quote_span_for_text(ctx: &ExtractionContext, quote: &str) -> Option<(EvidenceSpan, String)> {
    let haystack = ctx.node_text.as_str();
    let quote_start = haystack.find(quote)?;
    let quote_end = quote_start.checked_add(quote.len())?;
    Some((
        EvidenceSpan {
            revision: Link::<DocRevision>::new(ctx.revision_id.clone()),
            node: Some(Link::<DocNode>::new(ctx.node_id.clone())),
            start: quote_start as u32,
            end: quote_end as u32,
            quote_hash: compute_quote_hash(quote),
        },
        quote.to_string(),
    ))
}

fn extract_meeting_notes(ctx: &ExtractionContext) -> Vec<RawAssertion> {
    let mut assertions = Vec::new();

    let mut active_section: Option<MeetingSection> = None;
    let mut subject_meeting: Option<String> = None;

    for (line, start, end) in lines_with_offsets(&ctx.node_text) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if matches!(
                active_section,
                Some(MeetingSection::Decisions | MeetingSection::ActionItems)
            ) {
                active_section = None;
            }
            continue;
        }

        if trimmed.eq_ignore_ascii_case("decisions:") {
            active_section = Some(MeetingSection::Decisions);
            continue;
        }
        if trimmed.eq_ignore_ascii_case("action items:") {
            active_section = Some(MeetingSection::ActionItems);
            continue;
        }

        if subject_meeting.is_none() && !trimmed.contains(':') {
            // Treat the first non-header line as the "meeting subject" for decision assertions.
            subject_meeting = Some(trimmed.to_string());
        }

        if let Some(rest) = trimmed.strip_prefix("Attendees:") {
            let attendees = rest
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty());
            for name in attendees {
                assertions.push(make_simple_assertion(
                    ctx,
                    name.to_string(),
                    "attended".to_string(),
                    None,
                    None,
                    start,
                    end,
                ));
            }
            continue;
        }

        match active_section {
            Some(MeetingSection::Decisions) if trimmed.starts_with("- ") => {
                let decision = trimmed.trim_start_matches("- ").trim();
                if decision.is_empty() {
                    continue;
                }
                let subject = subject_meeting
                    .as_deref()
                    .unwrap_or("Meeting Notes")
                    .to_string();
                assertions.push(make_simple_assertion(
                    ctx,
                    subject,
                    "decision".to_string(),
                    Some(decision.to_string()),
                    None,
                    start,
                    end,
                ));
            }
            Some(MeetingSection::ActionItems) if trimmed.starts_with("- ") => {
                let item = trimmed.trim_start_matches("- ").trim();
                if item.is_empty() {
                    continue;
                }
                let (owner, details) = match item.split_once(':') {
                    Some((owner, rest)) => (owner.trim(), rest.trim()),
                    None => continue,
                };
                if owner.is_empty() || details.is_empty() {
                    continue;
                }

                assertions.push(make_simple_assertion(
                    ctx,
                    owner.to_string(),
                    "action item".to_string(),
                    Some(details.to_string()),
                    None,
                    start,
                    end,
                ));

                if let Some(date) = find_iso_date(details) {
                    assertions.push(make_simple_assertion(
                        ctx,
                        owner.to_string(),
                        "action item due".to_string(),
                        None,
                        Some(Literal::Date(date)),
                        start,
                        end,
                    ));
                }
            }
            _ => {}
        }
    }

    assertions
}

fn extract_has_title(ctx: &ExtractionContext) -> Vec<RawAssertion> {
    let text = ctx.node_text.trim();
    let needle = " is ";
    let match_start = match text.find(needle) {
        Some(idx) => idx,
        None => return Vec::new(),
    };
    let match_end = match_start.saturating_add(needle.len());
    if match_end >= text.len() {
        return Vec::new();
    }

    let subject = text.get(..match_start).unwrap_or("").trim();
    let title_raw = text.get(match_end..).unwrap_or("").trim();
    let title = trim_object_text(title_raw);
    if subject.is_empty() || title.is_empty() {
        return Vec::new();
    }

    let quote = text.to_string();
    let (evidence_span, quote) = match quote_span_for_text(ctx, quote.as_str()) {
        Some(values) => values,
        None => {
            return Vec::new();
        }
    };

    vec![RawAssertion {
        subject_text: subject.to_string(),
        predicate_text: "has title".to_string(),
        object_text: None,
        object_literal: Some(Literal::String(title.to_string())),
        evidence_span,
        confidence: 0.9,
        quote: Some(quote),
        source_event_time: None,
        source_event_time_precision: None,
        event_title: None,
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: None,
        object_entity_id: None,
        object_entity_type: None,
        significance: None,
    }]
}

fn extract_joined_as(ctx: &ExtractionContext) -> Vec<RawAssertion> {
    let text = ctx.node_text.trim();
    let needle = " joined ";
    let match_start = match text.find(needle) {
        Some(idx) => idx,
        None => return Vec::new(),
    };
    let match_end = match_start.saturating_add(needle.len());
    if match_end >= text.len() {
        return Vec::new();
    }

    let subject = text.get(..match_start).unwrap_or("").trim();
    let rest = text.get(match_end..).unwrap_or("").trim();
    let (org_raw, title_raw) = match rest.split_once(" as ") {
        Some((org, title)) => (org.trim(), title.trim()),
        None => return Vec::new(),
    };
    let org = trim_object_text(org_raw);
    if subject.is_empty() || org.is_empty() {
        return Vec::new();
    }

    let title_segment = match title_raw.split_once(" in ") {
        Some((title, _)) => title.trim(),
        None => title_raw,
    };
    let title = trim_object_text(title_segment);
    if title.is_empty() {
        return Vec::new();
    }

    let quote = text.to_string();
    let (evidence_span, quote) = match quote_span_for_text(ctx, quote.as_str()) {
        Some(values) => values,
        None => return Vec::new(),
    };

    vec![
        RawAssertion {
            subject_text: subject.to_string(),
            predicate_text: "works at".to_string(),
            object_text: Some(org.to_string()),
            object_literal: None,
            evidence_span: evidence_span.clone(),
            confidence: 0.9,
            quote: Some(quote.clone()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: None,
            object_entity_id: None,
            object_entity_type: None,
            significance: None,
        },
        RawAssertion {
            subject_text: subject.to_string(),
            predicate_text: "has title".to_string(),
            object_text: None,
            object_literal: Some(Literal::String(title.to_string())),
            evidence_span,
            confidence: 0.9,
            quote: Some(quote),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: None,
            object_entity_id: None,
            object_entity_type: None,
            significance: None,
        },
    ]
}

fn extract_mentors(ctx: &ExtractionContext) -> Vec<RawAssertion> {
    let text = ctx.node_text.trim();
    let needle = " mentors ";
    let match_start = match text.find(needle) {
        Some(idx) => idx,
        None => return Vec::new(),
    };
    let match_end = match_start.saturating_add(needle.len());
    if match_end >= text.len() {
        return Vec::new();
    }

    let subject = text.get(..match_start).unwrap_or("").trim();
    let rest = text.get(match_end..).unwrap_or("").trim();
    let object_segment = match rest.split_once(" in ") {
        Some((obj, _)) => obj.trim(),
        None => rest,
    };
    let object = trim_object_text(object_segment);
    if subject.is_empty() || object.is_empty() {
        return Vec::new();
    }

    let quote = text.to_string();
    let (evidence_span, quote) = match quote_span_for_text(ctx, quote.as_str()) {
        Some(values) => values,
        None => return Vec::new(),
    };

    vec![RawAssertion {
        subject_text: subject.to_string(),
        predicate_text: "mentors".to_string(),
        object_text: Some(object.to_string()),
        object_literal: None,
        evidence_span,
        confidence: 0.9,
        quote: Some(quote),
        source_event_time: None,
        source_event_time_precision: None,
        event_title: None,
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: None,
        object_entity_id: None,
        object_entity_type: None,
        significance: None,
    }]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeetingSection {
    Decisions,
    ActionItems,
}

fn make_simple_assertion(
    ctx: &ExtractionContext,
    subject_text: String,
    predicate_text: String,
    object_text: Option<String>,
    object_literal: Option<Literal>,
    start: usize,
    end: usize,
) -> RawAssertion {
    let start_u32 = (start.min(u32::MAX as usize)) as u32;
    let end_u32 = (end.min(u32::MAX as usize)) as u32;

    // Heuristic fallback can still drive timeline/event surfaces by
    // promoting explicit due dates into event metadata.
    let mut source_event_time = None;
    let mut source_event_time_precision = None;
    let mut event_title = None;
    if predicate_text == "action item due" {
        if let Some(Literal::Date(date_text)) = object_literal.as_ref() {
            if let Ok(parsed) = chrono::NaiveDate::parse_from_str(date_text, "%Y-%m-%d") {
                if let Some(naive_dt) = parsed.and_hms_opt(0, 0, 0) {
                    source_event_time =
                        Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc));
                    source_event_time_precision = Some(TimePrecision::Day);
                    event_title = Some(format!("{subject_text} action item due"));
                }
            }
        }
    }

    RawAssertion {
        subject_text,
        predicate_text,
        object_text,
        object_literal,
        evidence_span: EvidenceSpan {
            revision: Link::<DocRevision>::new(ctx.revision_id.clone()),
            node: Some(Link::<DocNode>::new(ctx.node_id.clone())),
            start: start_u32,
            end: end_u32,
            quote_hash: String::new(),
        },
        confidence: 0.9,
        quote: None,
        source_event_time,
        source_event_time_precision,
        event_title,
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: None,
        object_entity_id: None,
        object_entity_type: None,
        significance: None,
    }
}

fn lines_with_offsets(text: &str) -> impl Iterator<Item = (&str, usize, usize)> {
    text.split('\n').scan(0usize, |offset, line| {
        let start = *offset;
        let end = start.saturating_add(line.len());
        *offset = end.saturating_add(1);
        Some((line, start, end))
    })
}

fn find_iso_date(text: &str) -> Option<String> {
    for raw in text.split_whitespace() {
        let token = raw
            .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '-')
            .trim();
        if is_iso_date(token) {
            return Some(token.to_string());
        }
    }
    None
}

fn is_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 {
        return false;
    }
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, b)| idx == 4 || idx == 7 || b.is_ascii_digit())
}

fn extract_works_at(ctx: &ExtractionContext) -> Option<RawAssertion> {
    let text = ctx.node_text.as_str();
    let needle = "works at";
    let match_start = text.find(needle)?;
    let match_end = match_start.checked_add(needle.len())?;

    let subject_raw = text.get(..match_start)?;
    let object_raw = text.get(match_end..)?;
    let subject = subject_raw.trim();
    let object = trim_object_text(object_raw);

    if subject.is_empty() || object.is_empty() {
        return None;
    }

    let predicate = text
        .get(match_start..match_end)
        .unwrap_or(needle)
        .trim()
        .to_string();
    let quote = text.trim().to_string();
    let quote_start = text.find(quote.as_str()).unwrap_or(0);
    let quote_end = quote_start.checked_add(quote.len())?;

    let evidence_span = EvidenceSpan {
        revision: Link::<DocRevision>::new(ctx.revision_id.clone()),
        node: Some(Link::<DocNode>::new(ctx.node_id.clone())),
        start: quote_start as u32,
        end: quote_end as u32,
        quote_hash: compute_quote_hash(&quote),
    };

    Some(RawAssertion {
        subject_text: subject.to_string(),
        predicate_text: predicate,
        object_text: Some(object.to_string()),
        object_literal: None,
        evidence_span,
        confidence: 0.9,
        quote: Some(quote),
        source_event_time: None,
        source_event_time_precision: None,
        event_title: None,
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: None,
        object_entity_id: None,
        object_entity_type: None,
        significance: None,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use elephant_types::ids::TableName;
    #[cfg(feature = "meerkat-extraction")]
    use std::time::Duration;

    fn make_context() -> ExtractionContext {
        ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            "Alice works at Acme Corp.".to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        )
    }

    #[test]
    fn test_extraction_budget_default() {
        let budget = ExtractionBudget::default();
        assert_eq!(budget.max_tokens, 500_000);
        assert_eq!(budget.max_wall_time_secs, 600);
    }

    #[test]
    fn test_extraction_context_with_budget() {
        let ctx = make_context().with_budget(ExtractionBudget {
            max_tokens: 5_000,
            max_wall_time_secs: 30,
        });
        assert_eq!(ctx.budget.max_tokens, 5_000);
        assert_eq!(ctx.budget.max_wall_time_secs, 30);
    }

    #[test]
    fn test_extraction_result_empty() {
        let result = ExtractionResult::empty("test-model".to_string());
        assert_eq!(result.assertion_count(), 0);
        assert_eq!(result.tokens_used, 0);
        assert_eq!(result.model, "test-model");
    }

    #[test]
    fn test_extractor_registry() {
        let mut registry = ExtractorRegistry::new();
        assert!(registry.is_empty());

        registry.register(Box::new(DefaultExtractor::new()));
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());

        let found = registry.find_by_id("elephant.extractor.default");
        assert!(found.is_some());
        assert_eq!(
            found.unwrap().manifest().plugin_id,
            "elephant.extractor.default"
        );

        let not_found = registry.find_by_id("nonexistent");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_default_extractor_manifest() {
        let extractor = DefaultExtractor::new();
        let manifest = extractor.manifest();

        assert_eq!(manifest.plugin_id, "elephant.extractor.default");
        assert_eq!(manifest.plugin_type, "extractor");
        assert_eq!(manifest.determinism, Determinism::LlmSingleShot);
        assert!(manifest
            .tool_access
            .contains(&"fetch_node_text".to_string()));
    }

    #[tokio::test]
    async fn test_default_extractor_extract() {
        let extractor = DefaultExtractor::new();
        let ctx = make_context();

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await.expect("extract");
        assert_eq!(result.assertion_count(), 1);
        let assertion = result.raw_assertions.first().expect("assertion");
        assert_eq!(assertion.subject_text, "Alice");
        assert_eq!(assertion.predicate_text, "works at");
        assert_eq!(assertion.object_text.as_deref(), Some("Acme Corp"));
        assert_eq!(result.tokens_used, 0);
        assert_eq!(result.model, "gpt-5.2");

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[tokio::test]
    async fn test_default_extractor_extract_meeting_notes() {
        let extractor = DefaultExtractor::new();
        let notes = r#"Meeting Notes

Attendees: Alice Johnson, Bob Smith, Carol Diaz

Decisions:
- Adopt Q1 OKRs
- Use vendor XYZ for hosting

Action Items:
- Alice Johnson: Draft proposal by 2025-02-15
- Bob Smith: Prepare budget due 2025-02-20
"#;

        let ctx = ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            notes.to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        );

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await.expect("extract");
        assert!(
            result.assertion_count() >= 7,
            "expected >=7 assertions, got {}",
            result.assertion_count()
        );

        let attended = result
            .raw_assertions
            .iter()
            .filter(|a| a.predicate_text == "attended")
            .count();
        assert!(
            attended >= 3,
            "expected >=3 attended assertions, got {attended}"
        );

        let due_dates: Vec<&RawAssertion> = result
            .raw_assertions
            .iter()
            .filter(|a| a.predicate_text == "action item due")
            .collect();
        assert!(
            due_dates.len() >= 2,
            "expected >=2 action item due assertions, got {}",
            due_dates.len()
        );
        assert!(due_dates
            .iter()
            .all(|a| matches!(a.object_literal, Some(Literal::Date(_)))));

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[tokio::test]
    async fn test_default_extractor_extract_title_statement() {
        let extractor = DefaultExtractor::new();
        let ctx = ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            "Bob Smith is CTO.".to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        );

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await.expect("extract");
        assert_eq!(result.assertion_count(), 1);
        let assertion = result.raw_assertions.first().expect("assertion");
        assert_eq!(assertion.subject_text, "Bob Smith");
        assert_eq!(assertion.predicate_text, "has title");
        assert!(assertion.object_text.is_none());
        assert!(matches!(
            assertion.object_literal,
            Some(Literal::String(ref value)) if value == "CTO"
        ));

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[tokio::test]
    async fn test_default_extractor_extract_joined_as_statement() {
        let extractor = DefaultExtractor::new();
        let ctx = ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            "Dr. Jane Smith joined Acme Corp as Chief Scientist in March 2024.".to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        );

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await.expect("extract");
        assert_eq!(result.assertion_count(), 2);
        assert!(
            result.raw_assertions.iter().any(|a| {
                a.subject_text == "Dr. Jane Smith"
                    && a.predicate_text == "works at"
                    && a.object_text.as_deref() == Some("Acme Corp")
            }),
            "missing works-at assertion"
        );
        assert!(
            result.raw_assertions.iter().any(|a| {
                a.subject_text == "Dr. Jane Smith"
                    && a.predicate_text == "has title"
                    && matches!(a.object_literal, Some(Literal::String(ref value)) if value == "Chief Scientist")
            }),
            "missing title assertion"
        );

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[tokio::test]
    async fn test_default_extractor_extract_mentors_statement() {
        let extractor = DefaultExtractor::new();
        let ctx = ExtractionContext::new(
            RecordId::new_v4(TableName::DocNode),
            "Alice Mentor mentors Bob Student in machine learning.".to_string(),
            RecordId::new_v4(TableName::DocRevision),
            "default".to_string(),
            Security::default(),
        );

        let original = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");

        let result = extractor.extract(ctx).await.expect("extract");
        assert_eq!(result.assertion_count(), 1);
        let assertion = result.raw_assertions.first().expect("assertion");
        assert_eq!(assertion.subject_text, "Alice Mentor");
        assert_eq!(assertion.predicate_text, "mentors");
        assert_eq!(assertion.object_text.as_deref(), Some("Bob Student"));

        if let Some(value) = original {
            std::env::set_var("ANTHROPIC_API_KEY", value);
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    async fn test_default_extractor_wall_time_budget_enforced() {
        let extractor = DefaultExtractor::new();
        let result = extractor
            .run_with_wall_time_budget(0, async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok::<(), PipelineError>(())
            })
            .await;

        assert!(matches!(result, Err(PipelineError::ExtractionError(_))));
    }

    #[test]
    fn test_raw_assertion_with_source_event_time() {
        use chrono::TimeZone;

        let event_time = Utc.with_ymd_and_hms(2023, 1, 15, 10, 30, 0).unwrap();
        let quote = "Alice joined Acme on Jan 15, 2023";
        let quote_hash = compute_quote_hash(quote);

        let evidence_span = EvidenceSpan {
            revision: Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision)),
            node: Some(Link::<DocNode>::new(RecordId::new_v4(TableName::DocNode))),
            start: 0,
            end: quote.len() as u32,
            quote_hash,
        };

        let raw_assertion = RawAssertion {
            subject_text: "Alice".to_string(),
            predicate_text: "joined".to_string(),
            object_text: Some("Acme".to_string()),
            object_literal: None,
            evidence_span,
            confidence: 0.95,
            quote: Some(quote.to_string()),
            source_event_time: Some(event_time),
            source_event_time_precision: Some(TimePrecision::Day),
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: None,
            object_entity_id: None,
            object_entity_type: None,
            significance: None,
        };

        assert_eq!(raw_assertion.subject_text, "Alice");
        assert_eq!(raw_assertion.source_event_time, Some(event_time));
    }

    #[test]
    fn test_raw_assertion_without_source_event_time() {
        let quote = "Alice works at Acme";
        let quote_hash = compute_quote_hash(quote);

        let evidence_span = EvidenceSpan {
            revision: Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision)),
            node: Some(Link::<DocNode>::new(RecordId::new_v4(TableName::DocNode))),
            start: 0,
            end: quote.len() as u32,
            quote_hash,
        };

        let raw_assertion = RawAssertion {
            subject_text: "Alice".to_string(),
            predicate_text: "works at".to_string(),
            object_text: Some("Acme".to_string()),
            object_literal: None,
            evidence_span,
            confidence: 0.95,
            quote: Some(quote.to_string()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: None,
            object_entity_id: None,
            object_entity_type: None,
            significance: None,
        };

        assert_eq!(raw_assertion.subject_text, "Alice");
        assert!(raw_assertion.source_event_time.is_none());
    }
}
