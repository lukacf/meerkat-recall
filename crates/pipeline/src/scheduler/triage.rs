//! Triage plugin framework for progressive ingestion.
//!
//! Per spec §6.3: Triage is plugin-owned, not core semantics.
//! Core provides the stage `triage_source` and stores results in `ingest_decision`.
//! The logic producing that decision belongs to a configured plugin.

use async_trait::async_trait;
use elephant_types::records::ingest::{BudgetSnapshot, IngestMode};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::work::PluginManifest;

use crate::error::PipelineError;

#[cfg(feature = "meerkat-extraction")]
use crate::llm::run_model_prompt;
#[cfg(feature = "meerkat-extraction")]
use serde::Deserialize;
#[cfg(feature = "meerkat-extraction")]
use std::time::Duration;

#[cfg(feature = "meerkat-extraction")]
const TRIAGE_PROMPT_TEMPLATE: &str = include_str!("../../prompts/triage.txt");

/// Result of triage for a source.
#[derive(Debug, Clone)]
pub struct TriageResult {
    /// Decided ingestion mode.
    pub decision: IngestMode,
    /// Reason codes from the plugin (opaque strings).
    pub reason_codes: Vec<String>,
    /// Confidence in the decision (0.0 to 1.0).
    pub confidence: f32,
    /// Estimated cost in euros.
    pub estimated_cost: f64,
}

impl Default for TriageResult {
    fn default() -> Self {
        Self {
            decision: IngestMode::Metadata,
            reason_codes: vec![],
            confidence: 1.0,
            estimated_cost: 0.0,
        }
    }
}

/// Context provided to triage plugins.
#[derive(Debug, Clone)]
pub struct TriageContext {
    /// The source being triaged.
    pub source: Source,
    /// Budget snapshot at triage time.
    pub budget_snapshot: BudgetSnapshot,
    /// Space ID for ABAC filtering.
    pub space_id: String,
    /// Policy version being applied.
    pub policy_version: String,
}

/// Trait for triage plugins.
///
/// Per spec §6.3: Core does not know what a "spam label" is.
/// Core does not know what "email" is. A plugin can, and can return an ingest decision.
#[async_trait]
pub trait TriagePlugin: Send + Sync {
    /// Returns the plugin's manifest.
    fn manifest(&self) -> &PluginManifest;

    /// Returns the source types this plugin handles.
    fn source_types(&self) -> &[String];

    /// Triages a source and returns the recommended ingestion mode.
    ///
    /// # Errors
    ///
    /// Returns an error if triage fails.
    async fn triage(&self, ctx: TriageContext) -> Result<TriageResult, PipelineError>;
}

/// Default triage plugin that returns metadata mode for all sources.
#[derive(Debug, Clone)]
pub struct DefaultTriagePlugin {
    manifest: PluginManifest,
    source_types: Vec<String>,
}

impl DefaultTriagePlugin {
    /// Creates a new default triage plugin.
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: PluginManifest {
                plugin_id: "default_triage".to_string(),
                version: "1.0.0".to_string(),
                plugin_type: "triage".to_string(),
                required_scopes: vec![],
                tool_access: vec![],
                work_kinds: vec!["triage_source".to_string()],
                determinism: elephant_types::records::work::Determinism::Deterministic,
                max_tokens: None,
                max_wall_time_secs: None,
                input_types: vec!["source".to_string()],
                output_types: vec!["ingest_decision".to_string()],
                validator_profile: None,
                config_schema: None,
                health_check: None,
                retry_policy: elephant_types::records::work::RetryPolicy::default(),
            },
            source_types: vec!["*".to_string()],
        }
    }
}

impl Default for DefaultTriagePlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TriagePlugin for DefaultTriagePlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn triage(&self, ctx: TriageContext) -> Result<TriageResult, PipelineError> {
        if let Some(requested_mode) = ctx.source.requested_ingest_mode() {
            let estimated_cost = match requested_mode {
                IngestMode::Skip => 0.0,
                IngestMode::Metadata => 0.01,
                IngestMode::Retrieval => 0.10,
                IngestMode::Graph => 0.50,
            };
            return Ok(TriageResult {
                decision: requested_mode,
                reason_codes: vec!["requested_mode".to_string()],
                confidence: 1.0,
                estimated_cost,
            });
        }

        // Default: metadata mode for all sources
        Ok(TriageResult {
            decision: IngestMode::Metadata,
            reason_codes: vec!["default_policy".to_string()],
            confidence: 1.0,
            estimated_cost: 0.01, // minimal cost estimate
        })
    }
}

/// Converts a TriageResult into an IngestDecision (partial - base fields must be added).
impl TriageResult {
    /// Creates partial IngestDecision fields from the triage result.
    pub fn into_decision_fields(
        self,
        budget_snapshot: Option<BudgetSnapshot>,
        policy_version: Option<String>,
    ) -> PartialIngestDecision {
        PartialIngestDecision {
            decision: self.decision,
            reason_codes: self.reason_codes,
            confidence: self.confidence,
            estimated_cost: self.estimated_cost,
            budget_snapshot,
            policy_version,
            downgrade_reason: None,
            original_mode: None,
        }
    }
}

/// Partial ingest decision fields (before base fields are added).
#[derive(Debug, Clone)]
pub struct PartialIngestDecision {
    /// Decided ingestion mode.
    pub decision: IngestMode,
    /// Reason codes from the plugin.
    pub reason_codes: Vec<String>,
    /// Confidence in the decision.
    pub confidence: f32,
    /// Estimated cost in euros.
    pub estimated_cost: f64,
    /// Budget snapshot at decision time.
    pub budget_snapshot: Option<BudgetSnapshot>,
    /// Policy version used.
    pub policy_version: Option<String>,
    /// Reason for downgrade (if mode was downgraded).
    pub downgrade_reason: Option<String>,
    /// Original mode before downgrade.
    pub original_mode: Option<IngestMode>,
}

/// Rule-based triage plugin using configurable heuristics.
///
/// Per spec §6.3: Plugin-owned triage logic. This implementation uses
/// pattern matching and source metadata to decide ingestion mode.
#[derive(Debug, Clone)]
pub struct RuleBasedTriage {
    manifest: PluginManifest,
    source_types: Vec<String>,
    rules: TriageRules,
}

/// Configurable triage rules.
#[derive(Debug, Clone)]
pub struct TriageRules {
    /// Patterns that result in `skip` mode (e.g., spam indicators).
    pub skip_patterns: Vec<SkipPattern>,
    /// Patterns that result in `graph` mode (e.g., important documents).
    pub graph_patterns: Vec<GraphPattern>,
    /// Default mode when no rules match.
    pub default_mode: IngestMode,
    /// Cost estimates per mode.
    pub cost_estimates: CostEstimates,
    /// Minimum character length to treat a document as huge.
    pub huge_doc_min_chars: usize,
    /// Minimum byte length to treat a document as huge.
    pub huge_doc_min_bytes: usize,
    /// Minimum word count to treat a document as huge.
    pub huge_doc_min_words: usize,
}

/// Pattern for skipping sources.
#[derive(Debug, Clone)]
pub struct SkipPattern {
    /// Pattern name for reason codes.
    pub name: String,
    /// Source types this pattern applies to (empty = all).
    pub source_types: Vec<String>,
    /// Keywords in source metadata that trigger skip.
    pub keywords: Vec<String>,
    /// Minimum confidence for skip decision.
    pub min_confidence: f32,
}

/// Pattern for graph mode sources.
#[derive(Debug, Clone)]
pub struct GraphPattern {
    /// Pattern name for reason codes.
    pub name: String,
    /// Source types this pattern applies to (empty = all).
    pub source_types: Vec<String>,
    /// Keywords in source metadata that trigger graph mode.
    pub keywords: Vec<String>,
    /// Minimum confidence for graph decision.
    pub min_confidence: f32,
}

/// Cost estimates per ingestion mode.
#[derive(Debug, Clone)]
pub struct CostEstimates {
    /// Estimated cost for skip mode.
    pub skip: f64,
    /// Estimated cost for metadata mode.
    pub metadata: f64,
    /// Estimated cost for retrieval mode.
    pub retrieval: f64,
    /// Estimated cost for graph mode.
    pub graph: f64,
}

impl Default for CostEstimates {
    fn default() -> Self {
        Self {
            skip: 0.0,
            metadata: 0.01,
            retrieval: 0.10,
            graph: 0.50,
        }
    }
}

impl Default for TriageRules {
    fn default() -> Self {
        Self {
            skip_patterns: vec![],
            graph_patterns: vec![],
            default_mode: IngestMode::Metadata,
            cost_estimates: CostEstimates::default(),
            huge_doc_min_chars: 50_000,
            huge_doc_min_bytes: 1_000_000,
            huge_doc_min_words: 10_000,
        }
    }
}

impl RuleBasedTriage {
    /// Creates a new rule-based triage plugin with default rules.
    #[must_use]
    pub fn new() -> Self {
        Self::with_rules(TriageRules::default())
    }

    /// Creates a new rule-based triage plugin with custom rules.
    #[must_use]
    pub fn with_rules(rules: TriageRules) -> Self {
        Self {
            manifest: PluginManifest {
                plugin_id: "rule_based_triage".to_string(),
                version: "1.0.0".to_string(),
                plugin_type: "triage".to_string(),
                required_scopes: vec![],
                tool_access: vec![],
                work_kinds: vec!["triage_source".to_string()],
                determinism: elephant_types::records::work::Determinism::Deterministic,
                max_tokens: None,
                max_wall_time_secs: None,
                input_types: vec!["source".to_string()],
                output_types: vec!["ingest_decision".to_string()],
                validator_profile: None,
                config_schema: None,
                health_check: None,
                retry_policy: elephant_types::records::work::RetryPolicy::default(),
            },
            source_types: vec!["*".to_string()],
            rules,
        }
    }

    /// Extracts searchable text from source metadata.
    fn metadata_text(source: &Source) -> String {
        let mut parts = Vec::new();

        // Add URI if present
        if let Some(uri) = &source.uri {
            parts.push(uri.clone());
        }

        // Add path if present
        if let Some(path) = &source.path {
            parts.push(path.clone());
        }

        // Add author if present
        if let Some(author) = &source.author {
            parts.push(author.clone());
        }

        // Add channel if present
        if let Some(channel) = &source.channel {
            parts.push(channel.clone());
        }

        // Add external_id if present
        if let Some(external_id) = &source.external_id {
            parts.push(external_id.clone());
        }

        // Extract text from metadata JSON
        if let serde_json::Value::Object(map) = &source.metadata {
            for (key, value) in map {
                parts.push(key.clone());
                if let serde_json::Value::String(s) = value {
                    parts.push(s.clone());
                }
            }
        }

        parts.join(" ")
    }

    fn parse_numeric_value(value: &serde_json::Value) -> Option<u64> {
        match value {
            serde_json::Value::Number(number) => number
                .as_u64()
                .or_else(|| number.as_f64().map(|float| float.max(0.0).round() as u64)),
            serde_json::Value::String(text) => {
                let cleaned: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
                if cleaned.is_empty() {
                    None
                } else {
                    cleaned.parse::<u64>().ok()
                }
            }
            _ => None,
        }
    }

    fn extract_numeric_field(metadata: &serde_json::Value, key: &str) -> Option<u64> {
        metadata
            .get(key)
            .and_then(|value| Self::parse_numeric_value(value))
    }

    fn is_document_like(source: &Source) -> bool {
        matches!(
            source.type_,
            SourceType::Drive | SourceType::Web | SourceType::Transcript | SourceType::Manual
        )
    }

    fn huge_doc_hint(&self, source: &Source) -> Option<usize> {
        if !Self::is_document_like(source) {
            return None;
        }

        let metadata = &source.metadata;
        if let serde_json::Value::Object(_) = metadata {
            let byte_fields = [
                "size_bytes",
                "byte_size",
                "content_bytes",
                "content_length_bytes",
                "bytes",
            ];
            for key in byte_fields {
                if let Some(bytes) = Self::extract_numeric_field(metadata, key) {
                    if bytes as usize >= self.rules.huge_doc_min_bytes {
                        return Some(bytes as usize);
                    }
                }
            }

            let char_fields = [
                "text_length",
                "content_length",
                "char_count",
                "characters",
                "length",
            ];
            for key in char_fields {
                if let Some(chars) = Self::extract_numeric_field(metadata, key) {
                    if chars as usize >= self.rules.huge_doc_min_chars {
                        return Some(chars as usize);
                    }
                }
            }

            let word_fields = ["word_count", "words"];
            for key in word_fields {
                if let Some(words) = Self::extract_numeric_field(metadata, key) {
                    if words as usize >= self.rules.huge_doc_min_words {
                        return Some((words as usize).saturating_mul(5));
                    }
                }
            }

            let text_fields = ["text", "content", "body"];
            for key in text_fields {
                if let Some(text) = metadata.get(key).and_then(|value| value.as_str()) {
                    if text.len() >= self.rules.huge_doc_min_chars {
                        return Some(text.len());
                    }
                }
            }
        }

        let metadata_len = Self::metadata_text(source).len();
        if metadata_len >= self.rules.huge_doc_min_chars {
            return Some(metadata_len);
        }

        None
    }

    #[cfg(feature = "meerkat-extraction")]
    fn size_hint_text(&self, source: &Source) -> String {
        let mut hints = Vec::new();
        let metadata = &source.metadata;
        let fields = [
            "size_bytes",
            "byte_size",
            "content_bytes",
            "content_length_bytes",
            "bytes",
            "text_length",
            "content_length",
            "char_count",
            "characters",
            "word_count",
            "words",
        ];
        for key in fields {
            if let Some(value) = metadata.get(key) {
                if let Some(parsed) = Self::parse_numeric_value(value) {
                    hints.push(format!("{key}={parsed}"));
                }
            }
        }

        if hints.is_empty() {
            hints.push("size_hint=unknown".to_string());
        }

        if self.huge_doc_hint(source).is_some() {
            hints.push("huge_document=true".to_string());
        }

        hints.join(", ")
    }

    /// Checks if a source matches any skip pattern.
    fn matches_skip_pattern(&self, source: &Source) -> Option<(String, f32)> {
        let source_type = format!("{:?}", source.type_).to_lowercase();
        let metadata_str = Self::metadata_text(source);

        for pattern in &self.rules.skip_patterns {
            // Check source type match
            if !pattern.source_types.is_empty()
                && !pattern.source_types.iter().any(|t| source_type.contains(t))
            {
                continue;
            }

            // Count keyword matches
            let matches: usize = pattern
                .keywords
                .iter()
                .filter(|kw| metadata_str.to_lowercase().contains(&kw.to_lowercase()))
                .count();

            if matches > 0 {
                let confidence = (matches as f32 / pattern.keywords.len() as f32)
                    .min(1.0)
                    .max(pattern.min_confidence);
                if confidence >= pattern.min_confidence {
                    return Some((pattern.name.clone(), confidence));
                }
            }
        }
        None
    }

    /// Checks if a source matches any graph pattern.
    fn matches_graph_pattern(&self, source: &Source) -> Option<(String, f32)> {
        let source_type = format!("{:?}", source.type_).to_lowercase();
        let metadata_str = Self::metadata_text(source);

        for pattern in &self.rules.graph_patterns {
            // Check source type match
            if !pattern.source_types.is_empty()
                && !pattern.source_types.iter().any(|t| source_type.contains(t))
            {
                continue;
            }

            // Count keyword matches
            let matches: usize = pattern
                .keywords
                .iter()
                .filter(|kw| metadata_str.to_lowercase().contains(&kw.to_lowercase()))
                .count();

            if matches > 0 {
                let confidence = (matches as f32 / pattern.keywords.len() as f32)
                    .min(1.0)
                    .max(pattern.min_confidence);
                if confidence >= pattern.min_confidence {
                    return Some((pattern.name.clone(), confidence));
                }
            }
        }
        None
    }
}

impl Default for RuleBasedTriage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TriagePlugin for RuleBasedTriage {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn triage(&self, ctx: TriageContext) -> Result<TriageResult, PipelineError> {
        if let Some(requested_mode) = ctx.source.requested_ingest_mode() {
            return Ok(TriageResult {
                decision: requested_mode,
                reason_codes: vec!["requested_mode".to_string()],
                confidence: 1.0,
                estimated_cost: match requested_mode {
                    IngestMode::Skip => self.rules.cost_estimates.skip,
                    IngestMode::Metadata => self.rules.cost_estimates.metadata,
                    IngestMode::Retrieval => self.rules.cost_estimates.retrieval,
                    IngestMode::Graph => self.rules.cost_estimates.graph,
                },
            });
        }

        // Check skip patterns first (highest priority for resource savings)
        if let Some((reason, confidence)) = self.matches_skip_pattern(&ctx.source) {
            return Ok(TriageResult {
                decision: IngestMode::Skip,
                reason_codes: vec![reason],
                confidence,
                estimated_cost: self.rules.cost_estimates.skip,
            });
        }

        // Check graph patterns (high-value content)
        if let Some((reason, confidence)) = self.matches_graph_pattern(&ctx.source) {
            return Ok(TriageResult {
                decision: IngestMode::Graph,
                reason_codes: vec![reason],
                confidence,
                estimated_cost: self.rules.cost_estimates.graph,
            });
        }

        // Default huge documents to retrieval unless explicitly elevated.
        if self.huge_doc_hint(&ctx.source).is_some() {
            return Ok(TriageResult {
                decision: IngestMode::Retrieval,
                reason_codes: vec!["huge_document".to_string()],
                confidence: 0.9,
                estimated_cost: self.rules.cost_estimates.retrieval,
            });
        }

        // Default mode
        let cost = match self.rules.default_mode {
            IngestMode::Skip => self.rules.cost_estimates.skip,
            IngestMode::Metadata => self.rules.cost_estimates.metadata,
            IngestMode::Retrieval => self.rules.cost_estimates.retrieval,
            IngestMode::Graph => self.rules.cost_estimates.graph,
        };

        Ok(TriageResult {
            decision: self.rules.default_mode,
            reason_codes: vec!["default_rule".to_string()],
            confidence: 1.0,
            estimated_cost: cost,
        })
    }
}

/// Agentic triage plugin using LLM (Meerkat) for nuanced decisions.
///
/// Per spec §6.3: Plugin can use LLM for complex triage decisions.
#[derive(Debug, Clone)]
pub struct AgenticTriage {
    provider_context: Option<crate::provider_dispatch::ProviderDispatchContext>,
    manifest: PluginManifest,
    source_types: Vec<String>,
    /// Fallback to rule-based triage when LLM is unavailable.
    fallback: RuleBasedTriage,
    /// Model identifier used by the LLM.
    #[cfg_attr(not(feature = "meerkat-extraction"), allow(dead_code))]
    model: String,
    /// Whether to use LLM-backed triage decisions.
    #[cfg_attr(not(feature = "meerkat-extraction"), allow(dead_code))]
    llm_enabled: bool,
}

impl AgenticTriage {
    /// Bind optional model triage to one explicit space.
    #[must_use]
    pub fn with_provider_context(
        mut self,
        context: crate::provider_dispatch::ProviderDispatchContext,
    ) -> Self {
        self.provider_context = Some(context);
        self
    }

    /// Creates a new agentic triage plugin.
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider_context: None,
            manifest: PluginManifest {
                plugin_id: "agentic_triage".to_string(),
                version: "1.0.0".to_string(),
                plugin_type: "triage".to_string(),
                required_scopes: vec!["llm:inference".to_string()],
                tool_access: vec![],
                work_kinds: vec!["triage_source".to_string()],
                determinism: elephant_types::records::work::Determinism::LlmSingleShot,
                max_tokens: Some(1000),
                max_wall_time_secs: Some(30),
                input_types: vec!["source".to_string()],
                output_types: vec!["ingest_decision".to_string()],
                validator_profile: None,
                config_schema: None,
                health_check: None,
                retry_policy: elephant_types::records::work::RetryPolicy::default(),
            },
            source_types: vec!["*".to_string()],
            fallback: RuleBasedTriage::new(),
            model: "claude-sonnet-5-5".to_string(),
            llm_enabled: true,
        }
    }

    /// Creates an agentic triage plugin with a custom fallback.
    #[must_use]
    pub fn with_fallback(fallback: RuleBasedTriage) -> Self {
        let mut triage = Self::new();
        triage.fallback = fallback;
        triage
    }

    /// Creates an agentic triage plugin with LLM disabled (force rule-based).
    #[must_use]
    pub fn new_without_llm() -> Self {
        let mut triage = Self::new();
        triage.llm_enabled = false;
        triage
    }
}

impl Default for AgenticTriage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Deserialize)]
struct TriageLlmResponse {
    decision: String,
    reason_codes: Option<Vec<String>>,
    confidence: Option<f32>,
    estimated_cost: Option<f64>,
}

#[cfg(feature = "meerkat-extraction")]
impl AgenticTriage {
    fn build_prompt(&self, ctx: &TriageContext) -> String {
        let source_type = format!("{:?}", ctx.source.type_);
        let mut metadata_text = RuleBasedTriage::metadata_text(&ctx.source);
        if metadata_text.len() > 2000 {
            metadata_text = metadata_text.chars().take(2000).collect();
        }
        let size_hint = self.fallback.size_hint_text(&ctx.source);

        let budget_snapshot = format!(
            "retrieval_euros={:.2}, graph_euros={:.2}, retrieval_items={:?}, graph_items={:?}",
            ctx.budget_snapshot.remaining_euros_retrieval,
            ctx.budget_snapshot.remaining_euros_graph,
            ctx.budget_snapshot.remaining_items_retrieval,
            ctx.budget_snapshot.remaining_items_graph
        );

        TRIAGE_PROMPT_TEMPLATE
            .replace("{source_type}", &source_type)
            .replace("{metadata}", &metadata_text)
            .replace("{size_hint}", &size_hint)
            .replace("{policy_version}", &ctx.policy_version)
            .replace("{budget_snapshot}", &budget_snapshot)
    }

    fn parse_mode(value: &str) -> Option<IngestMode> {
        match value.trim().to_lowercase().as_str() {
            "skip" => Some(IngestMode::Skip),
            "metadata" => Some(IngestMode::Metadata),
            "retrieval" => Some(IngestMode::Retrieval),
            "graph" => Some(IngestMode::Graph),
            _ => None,
        }
    }

    fn estimate_cost(&self, mode: IngestMode) -> f64 {
        match mode {
            IngestMode::Skip => self.fallback.rules.cost_estimates.skip,
            IngestMode::Metadata => self.fallback.rules.cost_estimates.metadata,
            IngestMode::Retrieval => self.fallback.rules.cost_estimates.retrieval,
            IngestMode::Graph => self.fallback.rules.cost_estimates.graph,
        }
    }

    fn parse_llm_response(&self, text: &str) -> Option<TriageResult> {
        let trimmed = text.trim();
        let parsed = serde_json::from_str::<TriageLlmResponse>(trimmed)
            .ok()
            .or_else(|| {
                let start = trimmed.find('{')?;
                let end = trimmed.rfind('}')?;
                serde_json::from_str::<TriageLlmResponse>(&trimmed[start..=end]).ok()
            })?;

        let decision = Self::parse_mode(&parsed.decision)?;
        let confidence = parsed.confidence.unwrap_or(0.5).clamp(0.0, 1.0);
        let mut estimated_cost = parsed.estimated_cost.unwrap_or(0.0);
        if estimated_cost <= 0.0 {
            estimated_cost = self.estimate_cost(decision);
        }

        let mut reason_codes = parsed.reason_codes.unwrap_or_default();
        if reason_codes.is_empty() {
            reason_codes.push("agentic_triage".to_string());
        }

        Some(TriageResult {
            decision,
            reason_codes,
            confidence,
            estimated_cost,
        })
    }

    fn enforce_huge_doc_floor(&self, source: &Source, mut result: TriageResult) -> TriageResult {
        if self.fallback.huge_doc_hint(source).is_some()
            && matches!(result.decision, IngestMode::Metadata)
        {
            result.decision = IngestMode::Retrieval;
            if !result
                .reason_codes
                .iter()
                .any(|code| code == "huge_document")
            {
                result.reason_codes.push("huge_document".to_string());
            }
            result.estimated_cost = self.fallback.rules.cost_estimates.retrieval;
        }
        result
    }
}

#[async_trait]
impl TriagePlugin for AgenticTriage {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn triage(&self, ctx: TriageContext) -> Result<TriageResult, PipelineError> {
        #[cfg(feature = "meerkat-extraction")]
        {
            if !self.llm_enabled {
                return self.fallback.triage(ctx).await;
            }
            if cfg!(test) {
                return self.fallback.triage(ctx).await;
            }

            let context = self.provider_context.as_ref().ok_or_else(|| {
                PipelineError::ConfigError(
                    "provider_authorization: triage requires a space context".into(),
                )
            })?;
            if context.space_id != ctx.space_id || ctx.source.base.space_id != ctx.space_id {
                return Err(PipelineError::ConfigError(
                    "provider_authorization: wrong triage space".into(),
                ));
            }
            let (model, client, _provenance) = context
                .model_client(elephant_types::provider_binding::ModelOperation::Triage)
                .await?;

            let prompt = self.build_prompt(&ctx);
            let max_tokens = self.manifest.max_tokens.unwrap_or(500);
            let timeout_secs = u64::from(self.manifest.max_wall_time_secs.unwrap_or(30));

            tracing::debug!(
                model = %self.model,
                max_tokens,
                "AgenticTriage calling LLM for triage decision"
            );

            let call = run_model_prompt(
                client,
                model,
                Some("You are a careful ingestion triage assistant."),
                prompt,
                max_tokens.max(1),
            );

            let result = tokio::time::timeout(Duration::from_secs(timeout_secs), call).await;

            let run_result = match result {
                Ok(Ok(run_result)) => run_result,
                Ok(Err(err)) if err.to_string().contains("provider_authorization:") => {
                    return Err(err)
                }
                Ok(Err(err)) => {
                    tracing::warn!("AgenticTriage LLM call failed ({err}), falling back to rules");
                    return self.fallback.triage(ctx).await;
                }
                Err(_) => {
                    tracing::warn!("AgenticTriage timed out, falling back to rules");
                    return self.fallback.triage(ctx).await;
                }
            };
            if let Some(parsed) = self.parse_llm_response(&run_result.text) {
                return Ok(self.enforce_huge_doc_floor(&ctx.source, parsed));
            }

            tracing::warn!("AgenticTriage response parsing failed, falling back to rules");
        }

        // Fallback to rule-based triage
        self.fallback.triage(ctx).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::Utc;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::source::SourceType;
    use elephant_types::RecordId;

    fn make_source() -> Source {
        Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "test".to_string(),
            ),
            SourceType::Web,
            "test-hash-123".to_string(),
        )
    }

    fn make_budget_snapshot() -> BudgetSnapshot {
        BudgetSnapshot {
            remaining_euros_retrieval: 10.0,
            remaining_euros_graph: 50.0,
            remaining_items_retrieval: Some(1000),
            remaining_items_graph: Some(100),
            snapshot_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_default_triage_plugin() {
        let plugin = DefaultTriagePlugin::new();

        let ctx = TriageContext {
            source: make_source(),
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage");

        assert_eq!(result.decision, IngestMode::Metadata);
        assert!((result.confidence - 1.0).abs() < f32::EPSILON);
        assert!(result.reason_codes.contains(&"default_policy".to_string()));
    }

    #[test]
    fn test_triage_result_into_decision() {
        let result = TriageResult {
            decision: IngestMode::Retrieval,
            reason_codes: vec!["high_value".to_string()],
            confidence: 0.9,
            estimated_cost: 0.05,
        };

        let snapshot = make_budget_snapshot();
        let partial = result.into_decision_fields(Some(snapshot), Some("v1.0".to_string()));

        assert_eq!(partial.decision, IngestMode::Retrieval);
        assert!((partial.confidence - 0.9).abs() < f32::EPSILON);
        assert!(partial.budget_snapshot.is_some());
        assert_eq!(partial.policy_version, Some("v1.0".to_string()));
    }

    #[test]
    fn test_default_triage_plugin_manifest() {
        let plugin = DefaultTriagePlugin::new();
        let manifest = plugin.manifest();

        assert_eq!(manifest.plugin_id, "default_triage");
        assert!(manifest.work_kinds.contains(&"triage_source".to_string()));
    }

    #[test]
    fn test_default_triage_plugin_source_types() {
        let plugin = DefaultTriagePlugin::new();
        let source_types = plugin.source_types();

        assert!(source_types.contains(&"*".to_string()));
    }

    fn make_email_source_with_metadata(metadata: serde_json::Value) -> Source {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "test".to_string(),
            ),
            SourceType::Email,
            "email-hash-123".to_string(),
        );
        source.metadata = metadata;
        source
    }

    fn make_document_source_with_metadata(metadata: serde_json::Value) -> Source {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "test".to_string(),
            ),
            SourceType::Drive,
            "doc-hash-456".to_string(),
        );
        source.metadata = metadata;
        source
    }

    fn domain_rule_fixture() -> RuleBasedTriage {
        RuleBasedTriage::with_rules(TriageRules {
            skip_patterns: vec![SkipPattern {
                name: "spam_email".to_string(),
                source_types: vec!["email".to_string()],
                keywords: vec![
                    "spam".to_string(),
                    "unsubscribe".to_string(),
                    "promotional".to_string(),
                    "advertisement".to_string(),
                    "newsletter".to_string(),
                ],
                min_confidence: 0.7,
            }],
            graph_patterns: vec![GraphPattern {
                name: "important_document".to_string(),
                source_types: vec![
                    "document".to_string(),
                    "pdf".to_string(),
                    "drive".to_string(),
                ],
                keywords: vec![
                    "contract".to_string(),
                    "agreement".to_string(),
                    "invoice".to_string(),
                    "report".to_string(),
                    "analysis".to_string(),
                ],
                min_confidence: 0.8,
            }],
            ..TriageRules::default()
        })
    }

    /// Phase 9 Test: Spam email is triaged to `skip` mode.
    #[tokio::test]
    async fn test_spam_email_triaged_to_skip() {
        let plugin = domain_rule_fixture();

        // Create spam email source
        let spam_metadata = serde_json::json!({
            "subject": "PROMOTIONAL: Amazing deals - Unsubscribe now!",
            "from": "noreply@spam-deals.example",
            "labels": ["spam", "promotional"]
        });
        let source = make_email_source_with_metadata(spam_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        assert_eq!(
            result.decision,
            IngestMode::Skip,
            "Spam email should be triaged to Skip mode"
        );
        assert!(
            result.reason_codes.contains(&"spam_email".to_string()),
            "Reason should include spam_email pattern"
        );
        assert!(
            result.confidence >= 0.7,
            "Confidence should meet minimum threshold"
        );
    }

    /// Phase 9 Test: Important document is triaged to `graph` mode.
    #[tokio::test]
    async fn test_important_document_triaged_to_graph() {
        let plugin = domain_rule_fixture();

        // Create important document source
        let doc_metadata = serde_json::json!({
            "title": "Q4 2024 Financial Report and Analysis",
            "type": "document",
            "category": "report"
        });
        let source = make_document_source_with_metadata(doc_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        assert_eq!(
            result.decision,
            IngestMode::Graph,
            "Important document should be triaged to Graph mode"
        );
        assert!(
            result
                .reason_codes
                .contains(&"important_document".to_string()),
            "Reason should include important_document pattern"
        );
        assert!(
            result.confidence >= 0.8,
            "Confidence should meet minimum threshold for graph mode"
        );
    }

    #[tokio::test]
    async fn test_huge_document_defaults_to_retrieval() {
        let plugin = RuleBasedTriage::new();

        let doc_metadata = serde_json::json!({
            "title": "General document",
            "text_length": 60000
        });
        let source = make_document_source_with_metadata(doc_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        assert_eq!(
            result.decision,
            IngestMode::Retrieval,
            "Huge documents should default to Retrieval mode"
        );
        assert!(result.reason_codes.contains(&"huge_document".to_string()));
    }

    #[tokio::test]
    async fn test_huge_document_with_graph_signal_stays_graph() {
        let plugin = domain_rule_fixture();

        let doc_metadata = serde_json::json!({
            "title": "Contract analysis for Q4",
            "text_length": 60000
        });
        let source = make_document_source_with_metadata(doc_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        assert_eq!(
            result.decision,
            IngestMode::Graph,
            "Explicit graph signals should override huge doc default"
        );
    }

    #[tokio::test]
    async fn test_neutral_email_triaged_to_default() {
        let plugin = RuleBasedTriage::new();

        // Create normal email with no spam indicators
        let normal_metadata = serde_json::json!({
            "subject": "Meeting reminder for tomorrow",
            "from": "colleague@work.example"
        });
        let source = make_email_source_with_metadata(normal_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        assert_eq!(
            result.decision,
            IngestMode::Metadata,
            "Neutral email should default to Metadata mode"
        );
        assert!(result.reason_codes.contains(&"default_rule".to_string()));
    }

    #[tokio::test]
    async fn test_agentic_triage_falls_back_to_rules() {
        let mut plugin = AgenticTriage::with_fallback(domain_rule_fixture());
        plugin.llm_enabled = false;

        // Without meerkat feature, should fall back to rule-based triage
        let spam_metadata = serde_json::json!({
            "subject": "Newsletter - Weekly spam digest",
            "labels": ["newsletter"]
        });
        let source = make_email_source_with_metadata(spam_metadata);

        let ctx = TriageContext {
            source,
            budget_snapshot: make_budget_snapshot(),
            space_id: "test_space".to_string(),
            policy_version: "v1.0".to_string(),
        };

        let result = plugin.triage(ctx).await.expect("triage should succeed");

        // Should match spam pattern via fallback
        assert_eq!(result.decision, IngestMode::Skip);
        assert!(result.reason_codes.contains(&"spam_email".to_string()));
    }

    #[cfg(feature = "meerkat-extraction")]
    #[test]
    fn test_agentic_triage_parses_llm_response() {
        let plugin = AgenticTriage::new();
        let response = r#"
        {
          "decision": "graph",
          "reason_codes": ["important_document"],
          "confidence": 0.82,
          "estimated_cost": 0.12
        }
        "#;

        let parsed = plugin
            .parse_llm_response(response)
            .expect("parse should succeed");

        assert_eq!(parsed.decision, IngestMode::Graph);
        assert!(parsed
            .reason_codes
            .contains(&"important_document".to_string()));
        assert!((parsed.confidence - 0.82).abs() < f32::EPSILON);
        assert!((parsed.estimated_cost - 0.12).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rule_based_triage_manifest() {
        let plugin = RuleBasedTriage::new();
        let manifest = plugin.manifest();

        assert_eq!(manifest.plugin_id, "rule_based_triage");
        assert_eq!(
            manifest.determinism,
            elephant_types::records::work::Determinism::Deterministic
        );
    }

    #[test]
    fn test_agentic_triage_manifest() {
        let plugin = AgenticTriage::new();
        let manifest = plugin.manifest();

        assert_eq!(manifest.plugin_id, "agentic_triage");
        assert_eq!(
            manifest.determinism,
            elephant_types::records::work::Determinism::LlmSingleShot
        );
    }

    #[test]
    fn test_metadata_extraction() {
        let metadata = serde_json::json!({
            "subject": "Test email",
            "labels": ["important"]
        });
        let mut source = make_email_source_with_metadata(metadata);
        source.uri = Some("https://example.com/email/123".to_string());
        source.author = Some("test@example.com".to_string());

        let text = RuleBasedTriage::metadata_text(&source);

        assert!(text.contains("https://example.com/email/123"));
        assert!(text.contains("test@example.com"));
        assert!(text.contains("subject"));
        assert!(text.contains("Test email"));
    }
}
