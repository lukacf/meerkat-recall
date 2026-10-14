//! Safe cause codes from typed SDK errors, separate from retry classification.

use meerkat::{AgentError, BuildAgentError};
use meerkat_core::error::{LlmFailureReason, LlmProviderErrorKind};

use crate::PipelineError;

pub(super) fn run(stage: &'static str, prefix: &str, error: &AgentError) -> PipelineError {
    PipelineError::ExtractionFailure {
        stage,
        code: run_code(error),
        detail: format!("{prefix}: {error}"),
    }
}

pub(super) fn build(stage: &'static str, prefix: &str, error: &BuildAgentError) -> PipelineError {
    let code = match error {
        BuildAgentError::UnknownProvider { .. } => "provider_model_invalid",
        BuildAgentError::LlmClient(_) => "provider_client_setup",
        BuildAgentError::McpSetup(_) => "mcp_setup",
        BuildAgentError::ToolDispatcher(_) | BuildAgentError::DeclaredToolUnknown(_) => {
            "tool_setup"
        }
        BuildAgentError::Config(_) => "agent_configuration",
        _ => "agent_build",
    };
    PipelineError::ExtractionFailure {
        stage,
        code,
        detail: format!("{prefix}: {error}"),
    }
}

fn run_code(error: &AgentError) -> &'static str {
    match error.primary_error() {
        AgentError::Llm { reason, .. } => llm_code(reason),
        AgentError::TokenBudgetExceeded { .. } => "token_budget_exceeded",
        AgentError::TimeBudgetExceeded { .. } => "time_budget_exceeded",
        AgentError::ToolCallBudgetExceeded { .. } => "tool_budget_exceeded",
        AgentError::MaxTokensReached { .. } => "output_token_limit",
        AgentError::MaxTurnsReached { .. } => "turn_limit",
        AgentError::StructuredOutputValidationFailed { .. } => "structured_output_invalid",
        AgentError::InvalidOutputSchema(_) => "output_schema_invalid",
        AgentError::McpError(_) => "mcp_transport",
        AgentError::Tool { .. } => "tool_execution",
        AgentError::AuthReauthRequired { .. } => "provider_authentication",
        AgentError::ContentFiltered { .. } => "content_filtered",
        AgentError::Cancelled => "cancelled",
        AgentError::TerminalFailure { cause_kind, .. } => terminal_code(*cause_kind),
        _ => "agent_run",
    }
}

fn llm_code(reason: &LlmFailureReason) -> &'static str {
    match reason {
        LlmFailureReason::RateLimited { .. } => "provider_rate_limit",
        LlmFailureReason::ContextExceeded { .. } => "provider_context_limit",
        LlmFailureReason::AuthError => "provider_authentication",
        LlmFailureReason::InvalidModel(_) => "provider_model_invalid",
        LlmFailureReason::NetworkTimeout { .. } => "provider_network_timeout",
        LlmFailureReason::CallTimeout { .. } => "provider_call_timeout",
        LlmFailureReason::StreamStalled { .. } => "provider_stream_stalled",
        LlmFailureReason::ProviderError(error) => match error.kind {
            LlmProviderErrorKind::QuotaExhausted => "provider_quota_exhausted",
            LlmProviderErrorKind::RequestTooLarge => "provider_request_too_large",
            LlmProviderErrorKind::InvalidRequest => "provider_request_invalid",
            LlmProviderErrorKind::OperationRefused => "provider_operation_refused",
            LlmProviderErrorKind::OperationAuthorizationUnavailable => {
                "provider_authority_unavailable"
            }
            LlmProviderErrorKind::OperationObservationUnavailable => {
                "provider_observation_unavailable"
            }
            LlmProviderErrorKind::AuthorizationRouteChanged => "provider_route_changed",
            LlmProviderErrorKind::ContentFiltered => "content_filtered",
            LlmProviderErrorKind::PolicyStop => "provider_policy_stop",
            LlmProviderErrorKind::ServerError | LlmProviderErrorKind::ServerOverloaded => {
                "provider_server_error"
            }
            LlmProviderErrorKind::ConnectionReset => "provider_connection_reset",
            LlmProviderErrorKind::StreamParseError => "provider_stream_invalid",
            LlmProviderErrorKind::IncompleteResponse => "provider_response_incomplete",
            LlmProviderErrorKind::Unknown => "provider_error",
        },
        _ => "provider_error",
    }
}

pub(super) const fn terminal_code(
    cause: meerkat_core::turn_execution_authority::TurnTerminalCauseKind,
) -> &'static str {
    use meerkat_core::turn_execution_authority::TurnTerminalCauseKind as Cause;
    match cause {
        Cause::BudgetExhausted => "extraction_budget_exceeded",
        Cause::TimeBudgetExceeded => "time_budget_exceeded",
        Cause::TurnLimitReached => "turn_limit",
        Cause::StructuredOutputValidationFailed => "structured_output_invalid",
        Cause::RetryExhausted => "provider_retries_exhausted",
        Cause::LlmFailure => "provider_error",
        Cause::ToolFailure => "tool_execution",
        Cause::HookDenied => "hook_denied",
        Cause::HookFailure => "hook_failure",
        Cause::RuntimeApplyFailure => "runtime_apply_failure",
        Cause::FatalFailure => "agent_terminal_failure",
        Cause::Unknown => "agent_terminal_unknown",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    use super::*;
    use crate::scheduler::execution_contract::classify_failure;

    #[test]
    fn provider_quota_and_build_failures_use_typed_causes_not_private_text() {
        let error = AgentError::llm(
            "test",
            LlmFailureReason::ProviderError(meerkat_core::error::LlmProviderError::non_retryable(
                LlmProviderErrorKind::QuotaExhausted,
                serde_json::json!({"private": "PRIVATE_SOURCE"}),
            )),
            "misleading timeout SYNTHETIC_SECRET",
        );
        assert_eq!(run_code(&error), "provider_quota_exhausted");
        let error = BuildAgentError::UnknownProvider {
            model: "SYNTHETIC_SECRET".into(),
        };
        assert!(matches!(
            build("agent_build", "Agent build failed", &error),
            PipelineError::ExtractionFailure {
                stage: "agent_build",
                code: "provider_model_invalid",
                ..
            }
        ));
    }

    #[test]
    fn sdk_causes_survive_context_without_changing_retry_classification() {
        let cases = [
            (
                AgentError::MaxTokensReached {
                    turn: 1,
                    partial: "SYNTHETIC_SECRET".into(),
                },
                "output_token_limit",
            ),
            (
                AgentError::llm(
                    "test",
                    LlmFailureReason::ContextExceeded {
                        max: 8192,
                        requested: 9000,
                    },
                    "SYNTHETIC_SECRET",
                ),
                "provider_context_limit",
            ),
            (
                AgentError::llm(
                    "test",
                    LlmFailureReason::RateLimited { retry_after: None },
                    "rate limit SYNTHETIC_SECRET",
                ),
                "provider_rate_limit",
            ),
            (
                AgentError::TimeBudgetExceeded {
                    elapsed_secs: 301,
                    limit_secs: 300,
                },
                "time_budget_exceeded",
            ),
            (
                AgentError::StructuredOutputValidationFailed {
                    attempts: 3,
                    reason: "SYNTHETIC_SECRET".into(),
                    last_output: "PRIVATE_SOURCE".into(),
                },
                "structured_output_invalid",
            ),
        ];
        for (error, expected) in cases {
            let original = anyhow::Error::new(PipelineError::ExtractionError(format!(
                "Agent run failed: {error}"
            )))
            .context("extract_assertions failed");
            let repaired = run("agent_run", "Agent run failed", &error);
            let PipelineError::ExtractionFailure { code, stage, .. } = &repaired else {
                panic!("typed cause missing")
            };
            assert_eq!(*code, expected);
            assert_eq!(*stage, "agent_run");
            assert!(!code.contains("SYNTHETIC_SECRET"));
            let repaired = anyhow::Error::new(repaired).context("extract_assertions failed");
            assert_eq!(classify_failure(&repaired), classify_failure(&original));
        }
    }
}
