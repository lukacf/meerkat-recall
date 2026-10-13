//! LLM disambiguation seam for entity reuse during inline promotion.
//!
//! When an incoming mention matches stored entities by identity key but the
//! match is ambiguous (multiple compatible candidates) or type-conflicting
//! (only incompatible candidates), an optional [`PromotionDisambiguator`]
//! decides between reusing a candidate and creating a new entity. Without a
//! disambiguator the caller falls back to deterministic rules (see
//! `inline_promotion`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::PipelineError;

#[cfg(any(feature = "meerkat-extraction", test))]
const PROMOTION_DISAMBIGUATION_PROMPT_TEMPLATE: &str =
    include_str!("../../prompts/promotion_disambiguation.txt");

/// A stored entity offered to the disambiguator as a reuse candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateEntity {
    /// Entity record id, as a string.
    pub id: String,
    /// Current display name.
    pub display_name: String,
    /// Stored entity type (may be wrong or the generic "topic" bucket).
    pub entity_type: String,
    /// Entity summary, when one has been generated.
    pub summary: Option<String>,
}

/// Input for a single disambiguation call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisambiguationRequest {
    /// The mention text as extracted from the document.
    pub mention: String,
    /// The incoming entity type after `normalize_entity_type`.
    pub normalized_type: String,
    /// Source-document context around the mention.
    pub context_snippet: String,
    /// Stored entities sharing the mention's identity key.
    pub candidates: Vec<CandidateEntity>,
}

/// Outcome of a disambiguation call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionDisambiguationDecision {
    /// Reuse the stored entity with this id (must be one of the candidates).
    Reuse {
        /// Id of the chosen candidate.
        entity_id: String,
    },
    /// The mention refers to a distinct entity; create a new record.
    CreateNew,
}

/// Decides whether an ambiguous identity-key match should reuse a stored
/// entity or create a new one.
///
/// Implementations return `Ok(None)` when they cannot produce a usable
/// decision; the caller then applies deterministic fallback rules.
#[async_trait]
pub trait PromotionDisambiguator: Send + Sync + std::fmt::Debug {
    /// Returns a decision, or `None` when no usable decision was produced.
    ///
    /// # Errors
    ///
    /// Implementations may surface infrastructure errors; callers treat them
    /// as an abort: only `Ok(None)` permits deterministic fallback.
    async fn disambiguate(
        &self,
        input: &DisambiguationRequest,
    ) -> Result<Option<PromotionDisambiguationDecision>, PipelineError>;
}

/// Configuration for the agentic disambiguator.
#[derive(Debug, Clone)]
pub struct PromotionDisambiguationConfig {
    /// Model identifier passed to meerkat.
    pub model: String,
}

impl Default for PromotionDisambiguationConfig {
    fn default() -> Self {
        Self {
            model: "claude-sonnet-4-5".to_string(),
        }
    }
}

impl PromotionDisambiguationConfig {
    /// Builds a config from an optional model override (e.g. from
    /// `PIPELINE_PROMOTION_DISAMBIGUATION_MODEL`, read by the binary).
    #[must_use]
    pub fn with_model_override(model: Option<String>) -> Self {
        model
            .filter(|value| !value.trim().is_empty())
            .map_or_else(Self::default, |model| Self { model })
    }
}

/// Constructs the production disambiguator when an API key is available.
///
/// Pure over its inputs: callers (the binary) read `ANTHROPIC_API_KEY` and
/// `PIPELINE_PROMOTION_DISAMBIGUATION_MODEL` from the environment and pass the
/// results here. Returns `None` when no key is present or the
/// `meerkat-extraction` feature is disabled.
#[must_use]
pub fn promotion_disambiguator_from(
    key_present: bool,
    model: Option<String>,
) -> Option<Arc<dyn PromotionDisambiguator>> {
    #[cfg(feature = "meerkat-extraction")]
    {
        if key_present {
            let config = PromotionDisambiguationConfig::with_model_override(model);
            return Some(Arc::new(AgenticPromotionDisambiguator::new(config)));
        }
        None
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    {
        let _ = (key_present, model);
        None
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn render_candidates(candidates: &[CandidateEntity]) -> String {
    candidates
        .iter()
        .map(|candidate| {
            format!(
                "- id: {} | name: {} | type: {} | summary: {}",
                candidate.id,
                candidate.display_name,
                candidate.entity_type,
                candidate.summary.as_deref().unwrap_or("(none)"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn render_disambiguation_prompt(input: &DisambiguationRequest) -> String {
    PROMOTION_DISAMBIGUATION_PROMPT_TEMPLATE
        .replace("{mention}", &input.mention)
        .replace("{normalized_type}", &input.normalized_type)
        .replace("{context_snippet}", &input.context_snippet)
        .replace("{candidates}", &render_candidates(&input.candidates))
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[derive(Debug, serde::Deserialize)]
struct RawDisambiguationDecision {
    action: String,
    #[serde(default)]
    entity_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    reasoning: Option<String>,
}

/// Validates a raw LLM decision against the offered candidates.
///
/// Returns `None` for unknown actions or a `reuse` pointing at an id that was
/// not among the candidates.
#[cfg(any(feature = "meerkat-extraction", test))]
fn validate_decision(
    raw: &RawDisambiguationDecision,
    candidates: &[CandidateEntity],
) -> Option<PromotionDisambiguationDecision> {
    match raw.action.trim().to_ascii_lowercase().as_str() {
        "create_new" => Some(PromotionDisambiguationDecision::CreateNew),
        "reuse" => {
            let entity_id = raw.entity_id.as_deref().map(str::trim).unwrap_or_default();
            candidates
                .iter()
                .find(|candidate| candidate.id == entity_id)
                .map(|candidate| PromotionDisambiguationDecision::Reuse {
                    entity_id: candidate.id.clone(),
                })
        }
        _ => None,
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_raw_decision(
    structured_output: Option<&serde_json::Value>,
    text: &str,
) -> Result<RawDisambiguationDecision, PipelineError> {
    if let Some(structured) = structured_output {
        return serde_json::from_value(structured.clone()).map_err(|e| {
            PipelineError::ExtractionError(format!(
                "Failed to parse promotion disambiguation structured output: {e}"
            ))
        });
    }
    serde_json::from_str(text).map_err(|e| {
        PipelineError::ExtractionError(format!(
            "Failed to parse promotion disambiguation text output: {e}"
        ))
    })
}

#[cfg(feature = "meerkat-extraction")]
const PROMOTION_LLM_MAX_OUTPUT_TOKENS: u32 = 1_000;
#[cfg(feature = "meerkat-extraction")]
const PROMOTION_LLM_BUDGET_TOKENS: u64 = 8_000;

#[cfg(feature = "meerkat-extraction")]
fn disambiguation_decision_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "action": { "type": "string", "enum": ["reuse", "create_new"] },
            "entity_id": { "type": "string" },
            "reasoning": { "type": "string" }
        },
        "required": ["action", "entity_id", "reasoning"],
        "additionalProperties": false
    })
}

#[cfg(feature = "meerkat-extraction")]
fn disambiguation_system_prompt() -> &'static str {
    "You are an entity-resolution assistant for a knowledge graph. \
     You decide whether a newly extracted mention refers to one of the \
     provided stored candidate entities or to a new, distinct entity. \
     Use only the provided mention, context, and candidates. \
     Return only a JSON object matching the requested schema."
}

/// Meerkat-backed disambiguator using structured output.
///
/// All infrastructure and parse failures degrade to `Ok(None)` with a warning
/// so promotion falls back to deterministic rules instead of failing.
#[cfg(feature = "meerkat-extraction")]
#[derive(Debug, Clone)]
pub struct AgenticPromotionDisambiguator {
    config: PromotionDisambiguationConfig,
    provider_context: Option<crate::provider_dispatch::ProviderDispatchContext>,
}

#[cfg(feature = "meerkat-extraction")]
impl AgenticPromotionDisambiguator {
    /// Resolve this adapter for one explicit space; retries recheck current approval.
    pub async fn with_provider_context(
        mut self,
        context: crate::provider_dispatch::ProviderDispatchContext,
    ) -> crate::PipelineResult<Self> {
        let binding = context
            .repository
            .authorize_model_operation(
                &context.space_id,
                elephant_types::provider_binding::ModelOperation::PromotionDisambiguation,
                None,
            )
            .await
            .map_err(|error| {
                PipelineError::ConfigError(format!("provider_authorization: {error}"))
            })?;
        self.config.model = binding.profile.model;
        self.provider_context = Some(context);
        Ok(self)
    }

    /// Creates a disambiguator with the given config.
    #[must_use]
    pub const fn new(config: PromotionDisambiguationConfig) -> Self {
        Self {
            config,
            provider_context: None,
        }
    }

    async fn run_agent(
        &self,
        input: &DisambiguationRequest,
    ) -> Result<Option<PromotionDisambiguationDecision>, PipelineError> {
        use std::path::PathBuf;
        use std::time::Duration;

        use meerkat::{
            AgentBuildConfig, AgentFactory, BudgetLimits, OutputSchema, ToolCategoryOverride,
        };
        use meerkat_core::types::ContentInput;
        use meerkat_core::Config;

        let prompt = render_disambiguation_prompt(input);

        let budget_limits = BudgetLimits::default()
            .with_max_tokens(PROMOTION_LLM_BUDGET_TOKENS)
            .with_max_duration(Duration::from_secs(60));

        let output_schema = OutputSchema::new(disambiguation_decision_schema())
            .map_err(|e| {
                PipelineError::ExtractionError(format!(
                    "Invalid promotion disambiguation schema: {e}"
                ))
            })?
            .with_name("promotion_disambiguation_decision")
            .strict();

        let factory = AgentFactory::new(PathBuf::from("/tmp/elephant-promotion-disambiguation"));
        let meerkat_config = Config::default();
        let mut build = AgentBuildConfig::new(self.config.model.clone());
        self.provider_context
            .as_ref()
            .ok_or_else(|| {
                PipelineError::ConfigError(
                    "provider_authorization: model adapter requires a space context".into(),
                )
            })?
            .govern_agent_build(
                elephant_types::provider_binding::ModelOperation::PromotionDisambiguation,
                &mut build,
            )
            .await?;

        build.system_prompt =
            meerkat_core::SystemPromptOverride::Set(disambiguation_system_prompt().to_string());
        build.max_tokens = Some(PROMOTION_LLM_MAX_OUTPUT_TOKENS);
        build.budget_limits = Some(budget_limits);
        build.output_schema = Some(output_schema);
        build.structured_output_retries = Some(2);
        build.override_shell = ToolCategoryOverride::Disable;
        build.override_mob = ToolCategoryOverride::Disable;

        let mut agent = factory
            .build_agent(build, &meerkat_config)
            .await
            .map_err(|e| {
                PipelineError::ExtractionError(format!(
                    "Promotion disambiguation agent build failed: {e}"
                ))
            })?;

        let run_result = agent.run(ContentInput::Text(prompt)).await.map_err(|e| {
            PipelineError::ExtractionError(format!("Promotion disambiguation agent failed: {e}"))
        })?;

        let raw = parse_raw_decision(
            run_result.structured_output.as_ref(),
            run_result.text.trim(),
        )?;
        let decision = validate_decision(&raw, &input.candidates);
        if decision.is_none() {
            tracing::warn!(
                action = %raw.action,
                entity_id = raw.entity_id.as_deref().unwrap_or(""),
                mention = %input.mention,
                "promotion disambiguation decision invalid (unknown action or entity id); ignoring"
            );
        }
        Ok(decision)
    }
}

#[cfg(feature = "meerkat-extraction")]
#[async_trait]
impl PromotionDisambiguator for AgenticPromotionDisambiguator {
    async fn disambiguate(
        &self,
        input: &DisambiguationRequest,
    ) -> Result<Option<PromotionDisambiguationDecision>, PipelineError> {
        match self.run_agent(input).await {
            Ok(decision) => Ok(decision),
            Err(error) if error.to_string().contains("provider_authorization:") => Err(error),
            Err(error) => {
                tracing::warn!(
                    %error,
                    mention = %input.mention,
                    "promotion disambiguation agent failed; falling back to deterministic rules"
                );
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn sample_request() -> DisambiguationRequest {
        DisambiguationRequest {
            mention: "Mercury".to_string(),
            normalized_type: "topic".to_string(),
            context_snippet: "Mercury is the closest planet to the Sun.".to_string(),
            candidates: vec![
                CandidateEntity {
                    id: "entity:planet".to_string(),
                    display_name: "Mercury".to_string(),
                    entity_type: "location".to_string(),
                    summary: Some("The innermost planet.".to_string()),
                },
                CandidateEntity {
                    id: "entity:element".to_string(),
                    display_name: "Mercury".to_string(),
                    entity_type: "topic".to_string(),
                    summary: None,
                },
            ],
        }
    }

    #[test]
    fn test_prompt_render_fills_all_placeholders() {
        let request = sample_request();
        let prompt = render_disambiguation_prompt(&request);

        assert!(prompt.contains("Mention: Mercury"));
        assert!(prompt.contains("Mention entity type (normalized): topic"));
        assert!(prompt.contains("Mercury is the closest planet to the Sun."));
        assert!(prompt.contains(
            "- id: entity:planet | name: Mercury | type: location | summary: The innermost planet."
        ));
        assert!(
            prompt.contains("- id: entity:element | name: Mercury | type: topic | summary: (none)")
        );

        for placeholder in [
            "{mention}",
            "{normalized_type}",
            "{context_snippet}",
            "{candidates}",
        ] {
            assert!(
                !prompt.contains(placeholder),
                "placeholder {placeholder} left unrendered"
            );
        }
    }

    #[test]
    fn test_validate_decision_reuse_known_id() {
        let request = sample_request();
        let raw = RawDisambiguationDecision {
            action: "reuse".to_string(),
            entity_id: Some("entity:element".to_string()),
            reasoning: None,
        };
        assert_eq!(
            validate_decision(&raw, &request.candidates),
            Some(PromotionDisambiguationDecision::Reuse {
                entity_id: "entity:element".to_string()
            })
        );
    }

    #[test]
    fn test_validate_decision_reuse_unknown_id_is_none() {
        let request = sample_request();
        let raw = RawDisambiguationDecision {
            action: "reuse".to_string(),
            entity_id: Some("entity:not-a-candidate".to_string()),
            reasoning: None,
        };
        assert_eq!(validate_decision(&raw, &request.candidates), None);
    }

    #[test]
    fn test_validate_decision_create_new_and_unknown_action() {
        let request = sample_request();
        let create = RawDisambiguationDecision {
            action: "create_new".to_string(),
            entity_id: Some(String::new()),
            reasoning: None,
        };
        assert_eq!(
            validate_decision(&create, &request.candidates),
            Some(PromotionDisambiguationDecision::CreateNew)
        );

        let unknown = RawDisambiguationDecision {
            action: "merge".to_string(),
            entity_id: None,
            reasoning: None,
        };
        assert_eq!(validate_decision(&unknown, &request.candidates), None);
    }

    #[test]
    fn test_parse_raw_decision_prefers_structured_output() {
        let structured = serde_json::json!({
            "action": "reuse",
            "entity_id": "entity:planet",
            "reasoning": "planet context"
        });
        let raw = parse_raw_decision(Some(&structured), "").unwrap();
        assert_eq!(raw.action, "reuse");
        assert_eq!(raw.entity_id.as_deref(), Some("entity:planet"));

        let from_text =
            parse_raw_decision(None, r#"{"action": "create_new", "entity_id": ""}"#).unwrap();
        assert_eq!(from_text.action, "create_new");

        assert!(parse_raw_decision(None, "not json").is_err());
    }

    #[test]
    fn test_config_with_model_override() {
        assert_eq!(
            PromotionDisambiguationConfig::with_model_override(None).model,
            "claude-sonnet-4-5"
        );
        assert_eq!(
            PromotionDisambiguationConfig::with_model_override(Some("   ".to_string())).model,
            "claude-sonnet-4-5"
        );
        assert_eq!(
            PromotionDisambiguationConfig::with_model_override(Some("claude-opus-4-8".to_string()))
                .model,
            "claude-opus-4-8"
        );
    }

    #[test]
    fn test_promotion_disambiguator_from_key_absent_is_none() {
        assert!(promotion_disambiguator_from(false, None).is_none());
        assert!(
            promotion_disambiguator_from(false, Some("claude-sonnet-4-5".to_string())).is_none()
        );
    }

    #[test]
    fn test_promotion_disambiguator_from_key_present() {
        let disambiguator = promotion_disambiguator_from(true, None);
        #[cfg(feature = "meerkat-extraction")]
        assert!(disambiguator.is_some());
        #[cfg(not(feature = "meerkat-extraction"))]
        assert!(disambiguator.is_none());
    }
}
