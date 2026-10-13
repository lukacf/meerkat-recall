//! Document summarization for hierarchical extraction.
//!
//! Per spec §8.4.3: Summary nodes enable extract-from-summaries-first strategy.

use async_trait::async_trait;

use crate::error::PipelineError;

#[cfg(feature = "meerkat-extraction")]
use crate::llm::run_model_prompt;

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
            max_output_tokens: 300,
            max_summary_length: 500,
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
            model: "claude-sonnet-4-5".to_string(),
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
        ctx.text.len() >= ctx.options.min_text_length
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
        let summary_text = smart_truncate(&ctx.text, max_chars);
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
        ctx.text.len() >= ctx.options.min_text_length
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

    fn fallback(&self, ctx: &SummarizationContext) -> Result<SummaryResult, PipelineError> {
        let summary_text = smart_truncate(&ctx.text, max_summary_chars(&ctx.budget));
        let token_count = estimate_tokens(&summary_text, ctx.budget.tokens_per_char);
        Ok(SummaryResult {
            text: summary_text,
            token_count,
        })
    }

    #[cfg(feature = "meerkat-extraction")]
    fn build_prompt(&self, ctx: &SummarizationContext, max_chars: usize) -> String {
        SUMMARIZATION_PROMPT_TEMPLATE
            .replace("{max_chars}", &max_chars.to_string())
            .replace("{max_tokens}", &ctx.budget.max_output_tokens.to_string())
            .replace("{text}", &ctx.text)
    }
}
#[async_trait]
impl Summarizer for AgenticSummarizer {
    async fn summarize(&self, ctx: &SummarizationContext) -> Result<SummaryResult, PipelineError> {
        if ctx.text.len() <= max_summary_chars(&ctx.budget) {
            let token_count = estimate_tokens(&ctx.text, ctx.budget.tokens_per_char);
            return Ok(SummaryResult {
                text: ctx.text.clone(),
                token_count,
            });
        }

        #[cfg(feature = "meerkat-extraction")]
        {
            let context = self.provider_context.as_ref().ok_or_else(|| {
                PipelineError::ConfigError(
                    "provider_authorization: summary requires a space context".into(),
                )
            })?;
            let (model, client, _provenance) = context
                .model_client(elephant_types::provider_binding::ModelOperation::Summary)
                .await?;

            let max_chars = max_summary_chars(&ctx.budget);
            let prompt = self.build_prompt(ctx, max_chars);

            tracing::debug!(
                model = %ctx.options.model,
                text_len = ctx.text.len(),
                max_summary_len = max_chars,
                "AgenticSummarizer calling LLM for summarization"
            );

            let max_tokens = ctx.budget.max_output_tokens.max(1);
            let result = run_model_prompt(
                client,
                model,
                Some("You are a concise summarizer. Produce clear, accurate summaries."),
                prompt,
                max_tokens,
            )
            .await;

            return match result {
                Err(error) if error.to_string().contains("provider_authorization:") => Err(error),
                Ok(run_result) => {
                    let summary = run_result.text.trim().to_string();
                    let mut summary_text = summary.clone();
                    if summary_text.len() > max_chars {
                        summary_text = smart_truncate(&summary_text, max_chars);
                    }

                    let mut token_count =
                        estimate_tokens(&summary_text, ctx.budget.tokens_per_char);
                    if ctx.budget.max_output_tokens > 0
                        && token_count > ctx.budget.max_output_tokens
                    {
                        let max_chars = max_summary_chars(&SummarizationBudget {
                            max_output_tokens: ctx.budget.max_output_tokens,
                            max_summary_length: ctx.budget.max_summary_length,
                            tokens_per_char: ctx.budget.tokens_per_char,
                        });
                        summary_text = smart_truncate(&summary_text, max_chars);
                        token_count = estimate_tokens(&summary_text, ctx.budget.tokens_per_char);
                    }

                    tracing::debug!(
                        summary_len = summary_text.len(),
                        tokens_used = run_result.usage.total_tokens(),
                        "AgenticSummarizer LLM summarization complete"
                    );

                    Ok(SummaryResult {
                        text: summary_text,
                        token_count,
                    })
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AgenticSummarizer LLM call failed, falling back");
                    self.fallback(ctx)
                }
            };
        }

        #[cfg(not(feature = "meerkat-extraction"))]
        {
            tracing::warn!(
                "AgenticSummarizer requires `meerkat-extraction`; falling back to truncation"
            );
            return self.fallback(ctx);
        }
    }

    fn id(&self) -> &'static str {
        "agentic"
    }

    fn should_summarize(&self, ctx: &SummarizationContext) -> bool {
        ctx.text.len() >= ctx.options.min_text_length
    }
}

fn estimate_tokens(text: &str, tokens_per_char: f64) -> u32 {
    if tokens_per_char <= 0.0 {
        return 0;
    }
    ((text.len() as f64) * tokens_per_char).ceil() as u32
}

fn max_summary_chars(budget: &SummarizationBudget) -> usize {
    if budget.max_output_tokens == 0 || budget.tokens_per_char <= 0.0 {
        return budget.max_summary_length;
    }

    let max_by_tokens = (budget.max_output_tokens as f64 / budget.tokens_per_char).floor() as usize;
    std::cmp::min(budget.max_summary_length, max_by_tokens)
}

fn smart_truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }

    // Try to find a paragraph break
    if let Some(para_end) = text[..max_len].rfind("\n\n") {
        return text[..para_end].trim().to_string();
    }

    // Try to find a sentence break
    for end_char in [". ", "! ", "? "] {
        if let Some(sent_end) = text[..max_len].rfind(end_char) {
            return text[..sent_end + 1].trim().to_string();
        }
    }

    // Fall back to word boundary
    let mut end = max_len;
    while end > 0 && !text.as_bytes()[end].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        end = max_len;
    }
    format!("{}...", text[..end].trim())
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
