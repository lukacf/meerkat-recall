use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use elephant_types::summary_policy::SummaryPolicy;
use futures::Stream;
use meerkat::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest, StopReason};
use meerkat_core::lifecycle::run_primitive::{
    AnthropicEffort, AnthropicThinkingConfig, ProviderTag,
};
use meerkat_core::Provider;

use super::{
    summary_metrics_snapshot, AgenticSummarizer, SummarizationBudget, SummarizationContext,
    SummarizationOptions, Summarizer,
};
use crate::error::PipelineError;
use crate::llm::run_summary_prompt;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct ResponseClient {
    model: &'static str,
    text: &'static str,
    reason: StopReason,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl LlmClient for ResponseClient {
    fn stream<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<LlmEvent, LlmError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(request.model, self.model);
        assert_eq!(request.max_tokens, 4096);
        assert!(request.tools.is_empty());
        assert!(request.temperature.is_none());
        assert_eq!(request.tool_choice, meerkat_core::ToolChoice::Auto);
        let expected = if self.model == "claude-sonnet-5-5" {
            AnthropicThinkingConfig::BetweenTools
        } else {
            AnthropicThinkingConfig::Adaptive
        };
        assert!(
            matches!(&request.provider_params, Some(ProviderTag::Anthropic(tag))
            if tag.thinking.as_ref() == Some(&expected) && tag.effort == Some(AnthropicEffort::Low))
        );
        Box::pin(futures::stream::iter([
            Ok(LlmEvent::TextDelta {
                delta: self.text.into(),
                meta: None,
            }),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success {
                    stop_reason: self.reason,
                },
            }),
        ]))
    }
    fn provider(&self) -> Provider {
        Provider::Anthropic
    }
    async fn health_check(&self) -> Result<(), LlmError> {
        Ok(())
    }
}

fn context() -> SummarizationContext {
    SummarizationContext::new(
        "retained source text. ".repeat(1000),
        SummarizationBudget::default(),
        SummarizationOptions::default(),
    )
}

#[tokio::test]
async fn summary_empty_and_token_limited_completions_fall_back_visibly() -> TestResult {
    let summarizer = AgenticSummarizer::new();
    let ctx = context();
    let before = summary_metrics_snapshot();
    for (text, reason) in [
        ("", StopReason::EndTurn),
        (" \n", StopReason::MaxTokens),
        ("partial text", StopReason::MaxTokens),
    ] {
        let client = Arc::new(ResponseClient {
            model: "claude-haiku-5-5",
            text,
            reason,
            calls: AtomicUsize::new(0),
        });
        let policy = SummaryPolicy::for_model("anthropic", client.model);
        let response = run_summary_prompt(
            client.clone(),
            client.model.into(),
            "fixture prompt".into(),
            &policy,
        )
        .await;
        let result = summarizer.finish_response(&ctx, &ctx.budget, response)?;
        assert!(!result.text.is_empty());
        assert!(ctx.text.starts_with(result.text.trim_end_matches("...")));
        assert!(result.text.chars().count() <= policy.max_summary_length);
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
    }
    let after = summary_metrics_snapshot();
    assert!(after.empty_output_fallback >= before.empty_output_fallback + 2);
    assert!(after.incomplete_output_fallback >= before.incomplete_output_fallback + 2);
    assert!(after.local_truncation >= before.local_truncation + 3);
    Ok(())
}

#[tokio::test]
async fn summary_model_policies_keep_complete_text_and_reject_unsupported_pairs() -> TestResult {
    let ctx = context();
    let summarizer = AgenticSummarizer::new();
    for model in ["claude-sonnet-5-5", "claude-haiku-5-5"] {
        let client = Arc::new(ResponseClient {
            model,
            text: "Complete å 🐾 summary.",
            reason: StopReason::EndTurn,
            calls: AtomicUsize::new(0),
        });
        let policy = SummaryPolicy::for_model("anthropic", model);
        let response = run_summary_prompt(
            client.clone(),
            model.into(),
            "fixture prompt".into(),
            &policy,
        )
        .await;
        let result = summarizer.finish_response(&ctx, &ctx.budget, response)?;
        assert_eq!(result.text, "Complete å 🐾 summary.");
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
        let refused = run_summary_prompt(
            client.clone(),
            "unsupported-model".into(),
            "fixture".into(),
            &policy,
        )
        .await;
        assert!(refused.is_err());
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
    }
    Ok(())
}

#[test]
fn summary_unicode_truncation_and_authority_failure_keep_explicit_outcomes() -> TestResult {
    let summarizer = AgenticSummarizer::new();
    let mut ctx = context();
    ctx.text = "🐾å ".repeat(100);
    ctx.budget.max_summary_length = 17;
    let before = summary_metrics_snapshot();
    let fallback = summarizer.finish_response(
        &ctx,
        &ctx.budget,
        Err(PipelineError::ExtractionError("fixture unavailable".into())),
    )?;
    assert!(fallback.text.chars().count() <= 17);
    assert!(!fallback.text.is_empty());
    let after = summary_metrics_snapshot();
    assert!(after.provider_error_fallback > before.provider_error_fallback);
    assert!(after.local_truncation > before.local_truncation);
    let refused = summarizer.finish_response(
        &ctx,
        &ctx.budget,
        Err(PipelineError::ConfigError(
            "provider_authorization: revoked".into(),
        )),
    );
    assert!(matches!(refused, Err(PipelineError::ConfigError(_))));
    for bound in 0..=17 {
        assert!(super::smart_truncate(&ctx.text, bound).chars().count() <= bound);
    }
    Ok(())
}

#[test]
fn summary_short_input_keeps_trigger_separate_from_output_cap() -> TestResult {
    let summarizer = AgenticSummarizer::new();
    let mut ctx = context();
    ctx.text = "🐾".repeat(999);
    ctx.budget.max_summary_length = 17;
    assert!(!summarizer.should_summarize(&ctx));
    let before = summary_metrics_snapshot();
    let result = summarizer.fallback_with_budget(&ctx, &ctx.budget)?;
    assert!(result.text.chars().count() <= 17);
    assert!(summary_metrics_snapshot().local_truncation > before.local_truncation);
    ctx.text.push('🐾');
    assert!(summarizer.should_summarize(&ctx));
    ctx.budget.max_summary_length = 100_000;
    assert!(summarizer.should_summarize(&ctx));
    Ok(())
}

#[test]
fn summary_complete_output_over_visible_cap_is_counted() -> TestResult {
    let summarizer = AgenticSummarizer::new();
    let mut ctx = context();
    ctx.budget.max_summary_length = 17;
    let before = summary_metrics_snapshot();
    let response = crate::llm::LlmRunResult {
        text: "🐾å ".repeat(100),
        usage: meerkat::Usage::default(),
        stop_reason: StopReason::EndTurn,
    };
    let result = summarizer.finish_response(&ctx, &ctx.budget, Ok(response))?;
    assert!(!result.text.is_empty());
    assert!(result.text.chars().count() <= 17);
    assert!(summary_metrics_snapshot().local_truncation > before.local_truncation);
    Ok(())
}
