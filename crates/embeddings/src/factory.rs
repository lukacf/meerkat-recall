//! The one place a production embedding provider is built from a policy.

use std::sync::Arc;

use tracing::warn;

use crate::policy::EmbeddingPolicy;
use crate::provider::{
    EmbeddingProvider, FakeEmbeddingProvider, HashedEmbeddingProvider, OpenAIConfig,
    OpenAIEmbeddingProvider, UnavailableEmbeddingProvider,
};

/// Builds the provider a policy names.
///
/// `fake` is used only when it is named explicitly (tests, offline demos).
/// Anything that cannot be built becomes an [`UnavailableEmbeddingProvider`]:
/// the embedding attempt fails and is retried, and searches fall back to
/// text. A silent fake here is how a production store came to hold nothing
/// but `FakeEmbeddingProvider` ramps labelled `text-embedding-3-large`.
#[must_use]
pub fn provider_for_policy(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    match policy.provider.as_str() {
        "openai" => match OpenAIConfig::from_env() {
            Ok(config) => Arc::new(OpenAIEmbeddingProvider::new(
                config
                    .with_model(policy.model.clone())
                    .with_dimensions(policy.dims),
            )),
            Err(err) => unavailable(policy, &format!("OpenAI configuration invalid ({err})")),
        },
        "hashed" => Arc::new(HashedEmbeddingProvider::new(
            policy.model.clone(),
            policy.dims,
        )),
        "fake" => Arc::new(FakeEmbeddingProvider::new(
            policy.model.clone(),
            policy.dims,
        )),
        "fastembed" => fastembed(policy),
        other => unavailable(policy, &format!("unknown embedding provider `{other}`")),
    }
}

fn unavailable(policy: &EmbeddingPolicy, reason: &str) -> Arc<dyn EmbeddingProvider> {
    warn!(
        provider = %policy.provider,
        model = %policy.model,
        "{reason}; embeddings are unavailable until it is fixed"
    );
    Arc::new(UnavailableEmbeddingProvider::new(
        policy.model.clone(),
        policy.dims,
        reason,
    ))
}

#[cfg(feature = "fastembed")]
fn fastembed(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    use crate::provider::{
        fastembed_model_dimensions, FastEmbedConfig, FastEmbedEmbeddingProvider,
    };

    if let Some(expected) = fastembed_model_dimensions(&policy.model) {
        if expected != policy.dims {
            warn!(
                "Fastembed dims mismatch for model {}: expected {}, got {}",
                policy.model, expected, policy.dims
            );
        }
    }
    match FastEmbedEmbeddingProvider::new(FastEmbedConfig::new(policy.model.clone())) {
        Ok(provider) => Arc::new(provider),
        Err(err) => unavailable(policy, &format!("fastembed init failed ({err})")),
    }
}

#[cfg(not(feature = "fastembed"))]
fn fastembed(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    unavailable(
        policy,
        "fastembed requested but this build lacks `--features fastembed`",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn policy(provider: &str) -> EmbeddingPolicy {
        EmbeddingPolicy {
            provider: provider.to_string(),
            model: "text-embedding-3-large".to_string(),
            dims: 8,
        }
    }

    #[tokio::test]
    async fn unknown_and_misconfigured_providers_fail_instead_of_faking() {
        for name in ["", "openai-compatible", "Fake"] {
            let provider = provider_for_policy(&policy(name));
            assert!(provider.embed(&["x".to_string()]).await.is_err(), "{name}");
        }
    }

    #[tokio::test]
    async fn fake_is_only_used_when_named() {
        let provider = provider_for_policy(&policy("fake"));
        assert_eq!(
            provider
                .embed(&["x".to_string()])
                .await
                .expect("fake embeds")
                .len(),
            1
        );
    }
}
