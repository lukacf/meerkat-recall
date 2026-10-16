//! Document summarization for hierarchical extraction.
//!
//! Per spec §8.4.3: Summary nodes enable extract-from-summaries-first strategy.

use async_trait::async_trait;

use crate::error::PipelineError;

#[cfg(feature = "meerkat-extraction")]
use crate::llm::{run_summary_prompt, LlmRunResult};

mod metrics;
use metrics::{record, SummaryEvent};
pub use metrics::{summary_metrics_snapshot, SummaryMetrics};

#[cfg(feature = "meerkat-extraction")]
const SUMMARIZATION_PROMPT_TEMPLATE: &str = include_str!("../../prompts/summarization.txt");

/// Budget constraints for a summarization operation.
#[derive(Debug, Clone)]
pub struct SummarizationBudget {
    /// Maximum output tokens allowed for the summary.
    pub max_output_tokens: u32,
    /// Maximum summary length in characters.
    pub max_summary_length: usize,
    /// Estimated tokens per character ratio.
    pub tokens_per_char: f64,
}

impl Default for SummarizationBudget {
    fn default() -> Self {
        Self {
            max_output_tokens: 4096,
            max_summary_length: 12_000,
            tokens_per_char: 0.25,
        }
    }
}

/// Options controlling summarization behavior.
#[derive(Debug, Clone)]
pub struct SummarizationOptions {
    /// Minimum input length to trigger summarization.
    pub min_text_length: usize,
    /// Model identifier for agentic summarization.
    pub model: String,
}

impl Default for SummarizationOptions {
    fn default() -> Self {
        Self {
            min_text_length: 1000,
            model: "claude-sonnet-5-5".to_string(),
        }
    }
}

/// Context for a summarization request.
#[derive(Debug, Clone)]
pub struct SummarizationContext {
    /// The source text to summarize.
    pub text: String,
    /// Budget constraints for the summary.
    pub budget: SummarizationBudget,
    /// Summarization options.
    pub options: SummarizationOptions,
}

impl SummarizationContext {
    /// Creates a new summarization context.
    #[must_use]
    pub fn new(text: String, budget: SummarizationBudget, options: SummarizationOptions) -> Self {
        Self {
            text,
            budget,
            options,
        }
    }
}

/// Result of a summarization run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryResult {
    /// The generated summary text.
    pub text: String,
    /// Estimated token count for the summary text.
    pub token_count: u32,
}

/// Trait for document summarization.
#[async_trait]
pub trait Summarizer: Send + Sync + std::fmt::Debug {
    /// Summarizes the provided context.
    async fn summarize(&self, ctx: &SummarizationContext) -> Result<SummaryResult, PipelineError>;

    /// Returns the summarizer identifier.
    fn id(&self) -> &'static str;

    /// Returns true if the provided context should be summarized.
    fn should_summarize(&self, ctx: &SummarizationContext) -> bool {
        ctx.text.chars().count() >= ctx.options.min_text_length
    }
}

/// No-op summarizer that truncates input for deterministic testing.
#[derive(Debug, Clone, Default)]
pub struct NoOpSummarizer;

impl NoOpSummarizer {
    /// Creates a new no-op summarizer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Summarizer for NoOpSummarizer {
    async fn summarize(&self, ctx: &SummarizationContext) -> Result<SummaryResult, PipelineError> {
        let max_chars = max_summary_chars(&ctx.budget);
        let summary_text = truncate_visible(&ctx.text, max_chars);
        let token_count = estimate_tokens(&summary_text, ctx.budget.tokens_per_char);
        Ok(SummaryResult {
            text: summary_text,
            token_count,
        })
    }

    fn id(&self) -> &'static str {
        "noop"
    }

    fn should_summarize(&self, ctx: &SummarizationContext) -> bool {
        ctx.text.chars().count() >= ctx.options.min_text_length
    }
}

/// Agentic summarizer using LLM (Meerkat).
#[derive(Debug, Default)]
pub struct AgenticSummarizer {
    provider_context: Option<crate::provider_dispatch::ProviderDispatchContext>,
}

impl AgenticSummarizer {
    /// Bind summaries to current approvals for one explicit named space.
    #[must_use]
    pub fn with_provider_context(
        mut self,
        context: crate::provider_dispatch::ProviderDispatchContext,
    ) -> Self {
        self.provider_context = Some(context);
        self
    }

    /// Creates a new agentic summarizer instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn fallback_with_budget(
        &self,
        ctx: &SummarizationContext,
        budget: &SummarizationBudget,
    ) -> Result<SummaryResult, PipelineError> {
        let summary_text = truncate_visible(&ctx.text, max_summary_chars(budget));
        let token_count = estimate_tokens(&summary_text, budget.tokens_per_char);
        Ok(SummaryResult {
            text: summary_text,
            token_count,
        })
    }

    #[cfg(feature = "meerkat-extraction")]
    fn finish_response(
        &self,
        ctx: &SummarizationContext,
        budget: &SummarizationBudget,
        result: Result<LlmRunResult, PipelineError>,
    ) -> Result<SummaryResult, PipelineError> {
        match result {
            Err(error) if error.to_string().contains("provider_authorization:") => Err(error),
            Err(_) => {
                record(SummaryEvent::ProviderError);
                self.fallback_with_budget(ctx, budget)
            }
            Ok(result) => {
                if result.text.trim().is_empty() {
                    record(SummaryEvent::Empty);
                    if result.stop_reason != meerkat::StopReason::EndTurn {
                        record(SummaryEvent::Incomplete);
                    }
                    return self.fallback_with_budget(ctx, budget);
                }
                if result.stop_reason != meerkat::StopReason::EndTurn {
                    record(SummaryEvent::Incomplete);
                    return self.fallback_with_budget(ctx, budget);
                }
                let text = truncate_visible(result.text.trim(), max_summary_chars(budget));
                let token_count = estimate_tokens(&text, budget.tokens_per_char);
                tracing::debug!(
                    summary_len = text.chars().count(),
                    tokens_used = result.usage.total_tokens(),
                    "AgenticSummarizer LLM summarization complete"
                );
                Ok(SummaryResult { text, token_count })
            }
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    fn build_prompt(&self, ctx: &SummarizationContext, budget: &SummarizationBudget) -> String {
        SUMMARIZATION_PROMPT_TEMPLATE
            .replace("{max_chars}", &max_summary_chars(budget).to_string())
            .replace("{max_tokens}", &budget.max_output_tokens.to_string())
            .replace("{text}", &ctx.text)
    }
}
#[async_trait]
impl Summarizer for AgenticSummarizer {
    async fn summarize(&self, ctx: &SummarizationContext) -> Result<SummaryResult, PipelineError> {
        #[cfg(feature = "meerkat-extraction")]
        {
            let context = self.provider_context.as_ref().ok_or_else(|| {
                PipelineError::ConfigError(
                    "provider_authorization: summary requires a space context".into(),
                )
            })?;
            // Resolve policy and model together, even for the no-request short-input path.
            let (model, client, policy) = context.summary_client().await?;
            let budget = SummarizationBudget {
                max_output_tokens: policy.max_output_tokens,
                max_summary_length: policy.max_summary_length,
                tokens_per_char: ctx.budget.tokens_per_char,
            };
            if !self.should_summarize(ctx) {
                return self.fallback_with_budget(ctx, &budget);
            }
            let prompt = self.build_prompt(ctx, &budget);
            let result = run_summary_prompt(client, model, prompt, &policy).await;
            self.finish_response(ctx, &budget, result)
        }
        #[cfg(not(feature = "meerkat-extraction"))]
        {
            tracing::warn!(
                "AgenticSummarizer requires `meerkat-extraction`; falling back to truncation"
            );
            self.fallback_with_budget(ctx, &ctx.budget)
        }
    }

    fn id(&self) -> &'static str {
        "agentic"
    }

    fn should_summarize(&self, ctx: &SummarizationContext) -> bool {
        ctx.text.chars().count() >= ctx.options.min_text_length
    }
}

fn estimate_tokens(text: &str, tokens_per_char: f64) -> u32 {
    if tokens_per_char <= 0.0 {
        return 0;
    }
    ((text.chars().count() as f64) * tokens_per_char).ceil() as u32
}

fn max_summary_chars(budget: &SummarizationBudget) -> usize {
    if budget.max_output_tokens == 0 || budget.tokens_per_char <= 0.0 {
        return budget.max_summary_length;
    }

    let max_by_tokens = (budget.max_output_tokens as f64 / budget.tokens_per_char).floor() as usize;
    std::cmp::min(budget.max_summary_length, max_by_tokens)
}

fn truncate_visible(text: &str, max_chars: usize) -> String {
    if text.chars().count() > max_chars {
        record(SummaryEvent::Truncated);
    }
    smart_truncate(text, max_chars)
}

fn smart_truncate(text: &str, max_chars: usize) -> String {
    let Some((end, _)) = text.char_indices().nth(max_chars) else {
        return text.to_string();
    };
    let prefix = &text[..end];
    if let Some(end) = prefix.rfind("\n\n").filter(|end| *end > 0) {
        return prefix[..end].trim().to_string();
    }
    for separator in [". ", "! ", "? "] {
        if let Some(end) = prefix.rfind(separator) {
            return prefix[..end + 1].trim().to_string();
        }
    }
    if max_chars <= 3 {
        return prefix.to_string();
    }
    let reserved = prefix
        .char_indices()
        .nth(max_chars - 3)
        .map_or(prefix.len(), |(end, _)| end);
    let prefix = &prefix[..reserved];
    let end = prefix
        .rfind(char::is_whitespace)
        .filter(|end| *end > 0)
        .unwrap_or(prefix.len());
    format!("{}...", prefix[..end].trim())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn test_noop_summarizer_short_text_passthrough() {
        let summarizer = NoOpSummarizer::new();
        let ctx = SummarizationContext::new(
            "Short text".to_string(),
            SummarizationBudget::default(),
            SummarizationOptions::default(),
        );
        let result = summarizer.summarize(&ctx).await.unwrap();
        assert_eq!(result.text, "Short text");
    }

    #[tokio::test]
    async fn test_noop_summarizer_truncates_long_text() {
        let summarizer = NoOpSummarizer::new();
        let budget = SummarizationBudget {
            max_output_tokens: 50,
            max_summary_length: 20,
            tokens_per_char: 0.25,
        };
        let ctx = SummarizationContext::new(
            "This is a longer text that should be truncated".to_string(),
            budget,
            SummarizationOptions {
                min_text_length: 10,
                model: "".to_string(),
            },
        );

        let result = summarizer.summarize(&ctx).await.unwrap();
        assert!(result.text.len() <= 20);
    }

    #[test]
    fn test_should_summarize() {
        let summarizer = NoOpSummarizer::new();
        let ctx = SummarizationContext::new(
            "short".to_string(),
            SummarizationBudget::default(),
            SummarizationOptions {
                min_text_length: 100,
                model: "".to_string(),
            },
        );
        assert!(!summarizer.should_summarize(&ctx));
    }

    #[test]
    fn test_max_summary_chars_respects_token_budget() {
        let budget = SummarizationBudget {
            max_output_tokens: 10,
            max_summary_length: 100,
            tokens_per_char: 0.5,
        };

        assert_eq!(max_summary_chars(&budget), 20);
    }
}

#[cfg(test)]
mod agentic_tests {
    use super::*;

    #[test]
    fn test_agentic_summarizer_creation() {
        let summarizer = AgenticSummarizer::new();
        assert_eq!(summarizer.id(), "agentic");
    }
}

#[cfg(all(test, feature = "meerkat-extraction"))]
#[path = "policy_tests.rs"]
mod policy_tests;
