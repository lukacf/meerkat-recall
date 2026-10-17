//! Immutable, bounded policy for one approved summary model.

use serde::{Deserialize, Serialize};

use crate::provider_binding::ProviderAuthorizationError;

/// Supported summary thinking settings; no arbitrary provider parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryThinking {
    /// Send no explicit thinking or effort override.
    ProviderDefault,
    /// Haiku 5.5 adaptive thinking with low effort and the total output bound.
    AdaptiveLow,
    /// Sonnet 5.5 between-tools thinking with low effort on a tool-free call.
    BetweenToolsLow,
}

/// Output and thinking limits attached to an immutable summary profile.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryPolicy {
    /// Total generated tokens, including any thinking; 1 through 65,536.
    pub max_output_tokens: u32,
    /// Maximum visible Unicode characters; 1 through 262,144.
    pub max_summary_length: usize,
    /// Exact supported thinking policy for the selected provider and model.
    pub thinking: SummaryThinking,
}

impl SummaryPolicy {
    /// Derive runtime defaults without rewriting an existing profile.
    #[must_use]
    pub fn for_model(provider: &str, model: &str) -> Self {
        let thinking = match (provider, model) {
            ("anthropic", "claude-haiku-5-5") => SummaryThinking::AdaptiveLow,
            ("anthropic", "claude-sonnet-5-5") => SummaryThinking::BetweenToolsLow,
            _ => SummaryThinking::ProviderDefault,
        };
        Self {
            max_output_tokens: 4096,
            max_summary_length: 12_000,
            thinking,
        }
    }

    /// Validate bounds and the exact supported provider/model policy pair.
    ///
    /// # Errors
    /// Invalid limits or unsupported explicit thinking settings are refused.
    pub fn validate(&self, provider: &str, model: &str) -> Result<(), ProviderAuthorizationError> {
        let valid_thinking = match self.thinking {
            SummaryThinking::ProviderDefault => true,
            SummaryThinking::AdaptiveLow => provider == "anthropic" && model == "claude-haiku-5-5",
            SummaryThinking::BetweenToolsLow => {
                provider == "anthropic" && model == "claude-sonnet-5-5"
            }
        };
        if !(1..=65_536).contains(&self.max_output_tokens)
            || !(1..=262_144).contains(&self.max_summary_length)
            || !valid_thinking
        {
            return Err(ProviderAuthorizationError::InvalidConfiguration(
                "unsupported summary limits or thinking policy".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "summary_policy_tests.rs"]
mod tests;
