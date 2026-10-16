//! LLM helper utilities for single-shot prompts.

use futures::StreamExt;

use crate::error::PipelineError;
use meerkat::{
    LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest, Message, SystemMessage, Usage,
    UserMessage,
};

/// Result from a single-shot LLM call.
#[derive(Debug, Clone)]
pub struct LlmRunResult {
    /// Text content produced by the model.
    pub text: String,
    /// Normalized total usage for the prompt, including cached input.
    pub usage: Usage,
    /// Exact terminal reason, retained for callers that require complete text.
    pub stop_reason: meerkat::StopReason,
}

/// Run a single prompt using the injected, governed provider client.
pub async fn run_model_prompt(
    client: std::sync::Arc<dyn LlmClient>,
    model: String,
    system_prompt: Option<&str>,
    prompt: String,
    max_tokens: u32,
) -> Result<LlmRunResult, PipelineError> {
    let mut messages = Vec::new();
    if let Some(system_prompt) = system_prompt {
        messages.push(Message::System(SystemMessage::new(system_prompt)));
    }
    messages.push(Message::User(UserMessage::text(prompt)));

    let mut request = LlmRequest::new(&model, messages);
    request.max_tokens = max_tokens;

    collect_prompt_response(client.stream(&request)).await
}

/// Run a tool-free summary with its approved immutable limits and thinking policy.
pub(crate) async fn run_summary_prompt(
    client: std::sync::Arc<dyn LlmClient>,
    model: String,
    prompt: String,
    policy: &elephant_types::summary_policy::SummaryPolicy,
) -> Result<LlmRunResult, PipelineError> {
    use elephant_types::summary_policy::SummaryThinking;
    use meerkat_core::lifecycle::run_primitive::{
        AnthropicEffort, AnthropicProviderTag, AnthropicThinkingConfig, ProviderTag,
    };

    policy
        .validate(client.provider().as_str(), &model)
        .map_err(|error| PipelineError::ConfigError(format!("provider_authorization: {error}")))?;
    let mut request = LlmRequest::new(
        &model,
        vec![
            Message::System(SystemMessage::new(
                "You are a concise summarizer. Produce clear, accurate summaries.",
            )),
            Message::User(UserMessage::text(prompt)),
        ],
    );
    request.max_tokens = policy.max_output_tokens;
    let thinking = match policy.thinking {
        SummaryThinking::ProviderDefault => None,
        SummaryThinking::AdaptiveLow => Some(AnthropicThinkingConfig::Adaptive),
        SummaryThinking::BetweenToolsLow => Some(AnthropicThinkingConfig::BetweenTools),
    };
    if let Some(thinking) = thinking {
        request.provider_params = Some(ProviderTag::Anthropic(AnthropicProviderTag {
            thinking: Some(thinking),
            effort: Some(AnthropicEffort::Low),
            ..Default::default()
        }));
    }
    collect_prompt_response(client.stream(&request)).await
}

async fn collect_prompt_response(
    mut stream: impl futures::Stream<Item = Result<LlmEvent, LlmError>> + Unpin,
) -> Result<LlmRunResult, PipelineError> {
    let mut content = String::new();
    let mut usage = Usage::default();

    while let Some(result) = stream.next().await {
        let event = result.map_err(|e| PipelineError::ExtractionError(e.to_string()))?;
        match event {
            LlmEvent::TextDelta { delta, .. } => content.push_str(&delta),
            LlmEvent::UsageUpdate { usage: updated } => {
                // Providers emit snapshots of one turn. Replace the snapshot and
                // normalize cached input before downstream budget checks.
                let mut total = meerkat_core::CumulativeUsage::default();
                total.add_turn(&updated);
                usage = total.into_inner();
            }
            LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { stop_reason },
            } => {
                return Ok(LlmRunResult {
                    text: content,
                    usage,
                    stop_reason,
                });
            }
            LlmEvent::Done {
                outcome: LlmDoneOutcome::Error { error },
            } => return Err(PipelineError::ExtractionError(error.to_string())),
            _ => {}
        }
    }

    Err(PipelineError::ExtractionError(
        "LLM stream ended without a completion outcome".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerkat::StopReason;
    use meerkat_core::{ProviderTokenAccounting, TurnUsage};

    fn usage_snapshot(output_tokens: u64) -> LlmEvent {
        LlmEvent::UsageUpdate {
            usage: TurnUsage::new(
                Usage {
                    input_tokens: 10,
                    output_tokens,
                    cache_creation_tokens: Some(20),
                    cache_read_tokens: Some(70),
                    reasoning_tokens: Some(output_tokens.min(2)),
                    ..Usage::default()
                },
                ProviderTokenAccounting::anthropic("claude-sonnet-4-5", 10, 20, 70),
            ),
        }
    }

    #[tokio::test]
    async fn prompt_usage_normalizes_cached_tokens_and_replaces_snapshots(
    ) -> Result<(), PipelineError> {
        let result = collect_prompt_response(futures::stream::iter([
            Ok(usage_snapshot(0)),
            Ok(LlmEvent::TextDelta {
                delta: "answer".to_string(),
                meta: None,
            }),
            Ok(usage_snapshot(5)),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success {
                    stop_reason: StopReason::EndTurn,
                },
            }),
        ]))
        .await?;

        assert_eq!(result.text, "answer");
        assert_eq!(result.usage.total_tokens(), 105);
        assert_eq!(result.usage.cache_creation_tokens, Some(20));
        assert_eq!(result.usage.cache_read_tokens, Some(70));
        assert_eq!(result.usage.reasoning_tokens, Some(2));
        assert!(result.usage.provider_accounting.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn prompt_rejects_terminal_error_after_partial_text() {
        let result = collect_prompt_response(futures::stream::iter([
            Ok(LlmEvent::TextDelta {
                delta: "partial answer".to_string(),
                meta: None,
            }),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::Error {
                    error: LlmError::ServerOverloaded,
                },
            }),
        ]))
        .await;

        assert!(matches!(result, Err(PipelineError::ExtractionError(_))));
    }

    #[tokio::test]
    async fn prompt_rejects_stream_without_terminal_outcome() {
        let result = collect_prompt_response(futures::stream::iter([Ok(LlmEvent::TextDelta {
            delta: "partial answer".to_string(),
            meta: None,
        })]))
        .await;

        assert!(matches!(result, Err(PipelineError::ExtractionError(_))));
    }
}
