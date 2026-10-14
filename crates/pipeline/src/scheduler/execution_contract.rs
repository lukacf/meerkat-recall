//! Retry and usage outcomes shared by normal errors, deadlines and recovery.

use elephant_storage::StorageError;
use elephant_types::records::work::WorkRunMetrics;
use serde::{Deserialize, Serialize};

use crate::PipelineError;

#[path = "preflight_failure.rs"]
mod preflight_failure;
pub use preflight_failure::{record_preflight_disposition, record_preflight_failure};

/// Machine-readable failure policy, independent of human-facing error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// Temporary execution failure eligible for bounded retry.
    Transient,
    /// Execution exceeded its configured deadline.
    Timeout,
    /// Execution requires a configuration or approval repair.
    ConfigurationBlocked,
    /// Required durable predecessor or publication effects are incomplete.
    PendingDependencies,
    /// Captured input must be repaired before executing again.
    StaleInput,
    /// The original worker no longer owns this attempt.
    LostLease,
    /// Failure that cannot be automatically retried.
    Terminal,
}

/// Explicit failure for dispatcher/provider boundaries.
#[derive(Debug, thiserror::Error)]
#[error("{class:?}: {message}")]
pub struct WorkFailure {
    /// Machine-readable retry and completion policy.
    pub class: FailureClass,
    /// Diagnostic detail; public source receipts use separate safe codes.
    pub message: String,
}

impl WorkFailure {
    /// Construct a typed execution deadline failure.
    #[must_use]
    pub fn timeout(seconds: u64) -> Self {
        Self {
            class: FailureClass::Timeout,
            message: format!("execution deadline exceeded after {seconds}s"),
        }
    }
}

/// Configuration blockage is polled at a bounded cadence until configuration
/// changes. Timeouts share the same finite retry budget as transient failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAction {
    /// Retry within the finite execution-attempt budget.
    Retry {
        /// Backoff before another claim.
        delay_secs: i64,
    },
    /// Poll blocked work without consuming the execution retry budget.
    Blocked {
        /// Bounded interval before checking prerequisites again.
        delay_secs: i64,
    },
    /// Rebuild the original accepted input under a new claim.
    RepairInput,
    /// Leave completion to the current owner; never alter its lease.
    Abandon,
    /// Persist a failed outcome without automatic retry.
    Fail,
}

/// Choose bounded retry, prerequisite polling, or terminal handling.
#[must_use]
pub fn retry_action(
    class: FailureClass,
    attempt: u32,
    max_attempts: u32,
    base: i64,
    cap: i64,
) -> RetryAction {
    let base = base.max(1);
    let delay_secs = base
        .saturating_mul(1_i64 << attempt.saturating_sub(1).min(10))
        .min(cap.max(base));
    match class {
        // A local prerequisite may finish on the next worker poll. Its wait is
        // not a provider failure: using the provider backoff cap here imposed a
        // default 120-second earliest retry even after a child finished sooner.
        FailureClass::PendingDependencies => RetryAction::Blocked { delay_secs: 5 },
        FailureClass::ConfigurationBlocked => RetryAction::Blocked {
            delay_secs: cap.max(base).clamp(30, 3600),
        },
        FailureClass::Transient | FailureClass::Timeout if attempt < max_attempts => {
            RetryAction::Retry { delay_secs }
        }
        FailureClass::StaleInput if attempt < max_attempts => RetryAction::RepairInput,
        FailureClass::LostLease => RetryAction::Abandon,
        _ => RetryAction::Fail,
    }
}

/// Text parsing is confined to adapters whose dependencies lack typed errors.
/// A typed terminal/security/input error always wins over incidental words.
#[must_use]
pub fn classify_failure(error: &anyhow::Error) -> FailureClass {
    for cause in error.chain() {
        if let Some(failure) = cause.downcast_ref::<WorkFailure>() {
            return failure.class;
        }
        if let Some(error) = cause.downcast_ref::<PipelineError>() {
            return match error {
                PipelineError::ConfigError(_) | PipelineError::BudgetExceeded(_) => {
                    FailureClass::ConfigurationBlocked
                }
                PipelineError::InvalidState(message)
                    if message == "resolution decision inputs changed; reprepare"
                        || message
                            == "maintenance decision source/configuration changed; reprepare" =>
                {
                    FailureClass::StaleInput
                }
                PipelineError::InvalidState(message)
                    if message.contains("source_publication_incomplete:") =>
                {
                    FailureClass::PendingDependencies
                }
                PipelineError::GroundingRepairRequired => FailureClass::Transient,
                PipelineError::GroundingRepairExhausted => FailureClass::Terminal,
                PipelineError::LeaseConflict(_) => FailureClass::LostLease,
                PipelineError::StorageError(error) => classify_storage(error),
                PipelineError::EmbeddingError(
                    elephant_embeddings::EmbeddingError::ProviderError(message)
                    | elephant_embeddings::EmbeddingError::StorageError(message),
                ) => classify_untyped(message),
                PipelineError::InvalidInput(message) if message.starts_with("stale_input:") => {
                    FailureClass::StaleInput
                }
                PipelineError::ExtractionError(message)
                | PipelineError::ExtractionFailure {
                    detail: message, ..
                }
                | PipelineError::IngestionError(message) => classify_untyped(message),
                _ => FailureClass::Terminal,
            };
        }
        if let Some(error) =
            cause.downcast_ref::<elephant_storage::space_config_repository::SpaceResolutionError>()
        {
            return match error {
                elephant_storage::space_config_repository::SpaceResolutionError::Authorization(
                    elephant_types::provider_binding::ProviderAuthorizationError::WrongSpace,
                ) => FailureClass::Terminal,
                elephant_storage::space_config_repository::SpaceResolutionError::Authorization(
                    _,
                ) => FailureClass::ConfigurationBlocked,
                elephant_storage::space_config_repository::SpaceResolutionError::Storage(error) => {
                    classify_storage(error)
                }
            };
        }
        if let Some(error) = cause.downcast_ref::<StorageError>() {
            return classify_storage(error);
        }
    }
    classify_untyped(&error.to_string())
}

fn classify_storage(error: &StorageError) -> FailureClass {
    match error {
        StorageError::Conflict(message)
            if [
                "source_configuration_changed",
                "source_embedding_profile_changed",
                "source_embedding_profile_inactive",
                "source_embedding_profile_revoked",
            ]
            .iter()
            .any(|code| message.contains(code)) =>
        {
            FailureClass::ConfigurationBlocked
        }
        StorageError::Conflict(message) if message.contains("stale_source_publication") => {
            FailureClass::StaleInput
        }
        StorageError::Conflict(message) if message.contains("stale_attempt") => {
            FailureClass::LostLease
        }
        StorageError::ErasureInProgress
        | StorageError::ConnectionError(_)
        | StorageError::TransactionError(_) => FailureClass::Transient,
        StorageError::Conflict(_) => FailureClass::Transient,
        // The embedded standing-authority fence returns Conflict, which already
        // receives bounded retries. Preserve that policy for the two exact native
        // query shapes from preparation and vector persistence. Other authority
        // text must not become retryable merely by containing this sentinel.
        StorageError::QueryError(message)
            if matches!(
                message.as_str(),
                "An error occurred: standing_watch_authority_changed_or_missing"
                    | "DocNode embedding transaction failed: An error occurred: standing_watch_authority_changed_or_missing"
            ) =>
        {
            FailureClass::Transient
        }
        StorageError::QueryError(message) => classify_untyped(message),
        _ => FailureClass::Terminal,
    }
}

fn classify_untyped(message: &str) -> FailureClass {
    let text = message.to_ascii_lowercase();
    // The typed local resolver emitted these before an SDK erased its type.
    // Check terminal/transient first: incidental words in a storage diagnostic
    // must not turn corruption into a prerequisite refund or an infinite poll.
    if text.contains("recall_dispatch_terminal:") {
        return FailureClass::Terminal;
    }
    if text.contains("recall_dispatch_transient:") {
        return FailureClass::Transient;
    }
    if text.contains("recall_dispatch_timeout:") {
        return FailureClass::Timeout;
    }
    if text.contains("recall_dispatch_stale_input:") {
        return FailureClass::StaleInput;
    }
    if text.contains("stale_attempt") {
        return FailureClass::LostLease;
    }
    if text.contains("source_publication_incomplete:") {
        return FailureClass::PendingDependencies;
    }
    // Physical SDK authorizer failures are wrapped by the dependency, so the
    // explicit local prerequisite marker need not be the outer display prefix.
    if text.contains("configuration_blocked:") {
        return FailureClass::ConfigurationBlocked;
    }
    if ["semantic_extractor_unavailable:", "unknown extractor_id"]
        .iter()
        .any(|prefix| text.starts_with(prefix))
    {
        return FailureClass::ConfigurationBlocked;
    }
    if ["timeout", "timed out", "deadline exceeded"]
        .iter()
        .any(|term| text.contains(term))
    {
        return FailureClass::Timeout;
    }
    if [
        "read or write conflict",
        "transaction can be retried",
        "temporarily unavailable",
        "rate limit",
        "too many requests",
        "connection reset",
        "broken pipe",
    ]
    .iter()
    .any(|term| text.contains(term))
    {
        return FailureClass::Transient;
    }
    FailureClass::Terminal
}

/// Quantity provenance. A reservation is always separate from consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageBasis {
    /// All dispatched physical requests have complete measured totals.
    Measured,
    /// Consumption is estimated and remains distinct from measurement.
    Estimated,
    /// At least one dispatched request has incomplete usage.
    Unknown,
    /// No physical provider request was dispatched.
    NotStarted,
}

/// Stored in WorkRunMetrics.custom["usage"]. No invented currency conversion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptUsage {
    /// Provenance of the consumed-quantity fields.
    pub basis: UsageBasis,
    /// Planned token reservation, separate from measured consumption.
    pub reserved_tokens: u32,
    /// Abstract budget reservation held before dispatch.
    pub reserved_budget_units: f64,
    /// Known cumulative consumption, possibly partial when basis is Unknown.
    pub measured_tokens: Option<u64>,
    /// Explicitly estimated consumption, separate from reservation and measurement.
    pub estimated_tokens: Option<u64>,
    /// Budget policy unit, abstract unless concrete pricing is recorded.
    pub budget_unit: String,
    /// Provider identity when a single applicable identity is known.
    pub provider: Option<String>,
    /// Model identity when a single applicable identity is known.
    pub model: Option<String>,
    /// Versioned pricing basis, absent when invoice calculation is unavailable.
    pub pricing_version: Option<String>,
    /// Currency only when justified by a concrete pricing basis.
    pub currency: Option<String>,
}

impl AttemptUsage {
    /// Create an unconsumed reservation before physical dispatch.
    #[must_use]
    pub fn reserved(tokens: u32, units: f64) -> Self {
        Self {
            basis: UsageBasis::NotStarted,
            reserved_tokens: tokens,
            reserved_budget_units: units,
            measured_tokens: None,
            estimated_tokens: None,
            budget_unit: "abstract".to_string(),
            provider: None,
            model: None,
            pricing_version: None,
            currency: None,
        }
    }

    /// Dispatch without a provider receipt remains unknown, including failures
    /// and cancellations. This never turns the reservation into used tokens.
    pub fn dispatched(&mut self) {
        self.basis = UsageBasis::Unknown;
    }

    /// Record a complete measured token total.
    pub fn measured(&mut self, tokens: u64) {
        self.basis = UsageBasis::Measured;
        self.measured_tokens = Some(tokens);
    }

    /// Record estimated consumption without claiming a measurement.
    pub fn estimated(&mut self, tokens: u64) {
        self.basis = UsageBasis::Estimated;
        self.estimated_tokens = Some(tokens);
    }

    /// Persist provenance and bounded legacy counters without a fabricated EUR value.
    ///
    /// # Errors
    /// Returns a receipt serialization error.
    pub fn write_metrics(&self, metrics: &mut WorkRunMetrics) -> Result<(), serde_json::Error> {
        metrics.tokens_used = self
            .measured_tokens
            .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
            .unwrap_or(0);
        // Historical field retained on the wire; never label abstract units EUR.
        metrics.estimated_cost_euros = 0.0;
        metrics
            .custom
            .insert("usage".to_string(), serde_json::to_value(self)?);
        Ok(())
    }

    /// Read typed usage while preserving absence in historical receipts.
    ///
    /// # Errors
    /// Returns an error for malformed stored usage.
    pub fn from_metrics(metrics: &WorkRunMetrics) -> Result<Option<Self>, serde_json::Error> {
        metrics
            .custom
            .get("usage")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
    }

    /// Budget policy uses explicit abstract units for measured tokens only.
    /// Unknown usage is visible but cannot fabricate a provider invoice.
    #[must_use]
    pub fn accounted_units(&self) -> f64 {
        self.measured_tokens
            .map_or(0.0, |tokens| tokens as f64 * 0.000_001)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::float_cmp)]
    use super::*;

    #[test]
    fn typed_space_authorization_blocks_but_outage_remains_transient() {
        use elephant_storage::space_config_repository::SpaceResolutionError;
        use elephant_types::provider_binding::ProviderAuthorizationError;
        let denied = anyhow::Error::new(SpaceResolutionError::Authorization(
            ProviderAuthorizationError::MissingSpace,
        ));
        assert_eq!(
            classify_failure(&denied),
            FailureClass::ConfigurationBlocked
        );
        let outage = anyhow::Error::new(SpaceResolutionError::Storage(
            StorageError::ConnectionError("offline".into()),
        ));
        assert_eq!(classify_failure(&outage), FailureClass::Transient);
    }

    #[test]
    fn typed_erasure_reservation_is_transient_directly_and_through_pipeline() {
        assert_eq!(
            classify_failure(&anyhow::Error::new(StorageError::ErasureInProgress)),
            FailureClass::Transient
        );
        assert_eq!(
            classify_failure(&anyhow::Error::new(PipelineError::StorageError(
                StorageError::ErasureInProgress
            ))),
            FailureClass::Transient
        );
        assert_eq!(
            classify_failure(&anyhow::Error::new(PipelineError::InvalidInput(
                "erasure temporarily unavailable".into()
            ))),
            FailureClass::Terminal
        );
    }

    #[test]
    fn pending_dependencies_recheck_without_provider_backoff() {
        // Completed local children must become eligible for a fresh fenced
        // claim after a short bounded wait, even after repeated waits or
        // when the provider retry policy allows a long backoff.
        for attempt in [1, 3, 100, u32::MAX] {
            for (base, cap) in [
                (-1, 0),
                (1, 1),
                (10, 120),
                (120, 3600),
                (i64::MAX, i64::MAX),
            ] {
                assert_eq!(
                    retry_action(FailureClass::PendingDependencies, attempt, 3, base, cap),
                    RetryAction::Blocked { delay_secs: 5 },
                );
            }
        }
        // Configuration blocks retain their separate recovery interval; this
        // change does not accelerate outbound provider retry or renew authority.
        assert_eq!(
            retry_action(FailureClass::ConfigurationBlocked, 100, 3, 10, 120),
            RetryAction::Blocked { delay_secs: 120 },
        );
        assert_eq!(
            retry_action(FailureClass::Transient, 2, 3, 10, 120),
            RetryAction::Retry { delay_secs: 20 },
        );
        assert_eq!(
            retry_action(FailureClass::Timeout, 3, 3, 10, 120),
            RetryAction::Fail,
        );
    }

    #[test]
    fn exact_native_standing_fence_failures_keep_bounded_retry_parity() {
        let sentinel = "standing_watch_authority_changed_or_missing";
        let embedded = anyhow::Error::new(StorageError::Conflict(sentinel.into()));
        assert_eq!(classify_failure(&embedded), FailureClass::Transient);
        for message in [
            format!("An error occurred: {sentinel}"),
            format!("DocNode embedding transaction failed: An error occurred: {sentinel}"),
        ] {
            let direct = anyhow::Error::new(StorageError::QueryError(message.clone()));
            let wrapped = anyhow::Error::new(PipelineError::StorageError(
                StorageError::QueryError(message.clone()),
            ));
            for error in [&direct, &wrapped] {
                let class = classify_failure(error);
                assert_eq!(class, FailureClass::Transient);
                assert_eq!(
                    retry_action(class, 1, 3, 10, 120),
                    RetryAction::Retry { delay_secs: 10 },
                );
                assert_eq!(retry_action(class, 3, 3, 10, 120), RetryAction::Fail);
            }
            // Typed denial/corruption and incidental query text do not inherit
            // the native guard's retry policy. No source authority is renewed.
            for error in [
                anyhow::Error::new(PipelineError::SecurityViolation(message.clone())),
                anyhow::Error::new(StorageError::DataIntegrityError(message.clone())),
                anyhow::Error::new(StorageError::QueryError(format!("input: {message}"))),
                anyhow::Error::new(StorageError::QueryError(format!("{message}; denied"))),
            ] {
                assert_eq!(classify_failure(&error), FailureClass::Terminal);
            }
        }
        assert_eq!(
            classify_failure(&anyhow::Error::new(StorageError::QueryError(
                sentinel.into()
            ))),
            FailureClass::Terminal,
        );
    }

    #[test]
    fn deadline_and_transient_share_bounded_retry_policy() {
        assert_eq!(
            retry_action(FailureClass::Timeout, 1, 3, 5, 60),
            RetryAction::Retry { delay_secs: 5 }
        );
        assert_eq!(
            retry_action(FailureClass::Timeout, 3, 3, 5, 60),
            RetryAction::Fail
        );
        assert_eq!(
            retry_action(FailureClass::ConfigurationBlocked, 100, 3, 5, 60),
            RetryAction::Blocked { delay_secs: 60 }
        );
        assert_eq!(
            retry_action(FailureClass::LostLease, 1, 3, 5, 60),
            RetryAction::Abandon
        );
    }

    #[test]
    fn typed_security_error_is_not_retried_due_to_words_in_message() {
        let error = anyhow::Error::new(PipelineError::SecurityViolation(
            "timeout while checking denied scope".to_string(),
        ));
        assert_eq!(classify_failure(&error), FailureClass::Terminal);
    }

    #[test]
    fn wrapped_dispatch_budget_refusal_remains_recoverable_but_typed_denial_wins() {
        let message = "Invalid request: provider_authorization: Embedding provider error: configuration_blocked: unpriced finite job";
        let callback = anyhow::Error::new(PipelineError::ExtractionError(message.into()));
        assert_eq!(
            classify_failure(&callback),
            FailureClass::ConfigurationBlocked
        );
        let input = anyhow::Error::new(PipelineError::InvalidInput(message.into()));
        assert_eq!(classify_failure(&input), FailureClass::Terminal);
    }

    #[test]
    fn reserved_and_unknown_tokens_never_become_consumption() {
        let mut usage = AttemptUsage::reserved(400_000, 2.0);
        let mut metrics = WorkRunMetrics::default();
        usage.write_metrics(&mut metrics).expect("initial receipt");
        assert_eq!(metrics.tokens_used, 0);
        usage.dispatched();
        usage
            .write_metrics(&mut metrics)
            .expect("failed or cancelled receipt");
        assert_eq!(metrics.tokens_used, 0);
        assert_eq!(usage.basis, UsageBasis::Unknown);
        assert_eq!(usage.accounted_units(), 0.0);
        usage.measured(12);
        usage.write_metrics(&mut metrics).expect("provider receipt");
        assert_eq!(metrics.tokens_used, 12);
        assert_eq!(metrics.estimated_cost_euros, 0.0);
        assert_eq!(
            AttemptUsage::from_metrics(&metrics).expect("decode"),
            Some(usage)
        );
    }
    #[test]
    fn storage_decorated_incomplete_proof_keeps_pending_without_retry_exhaustion() {
        let error = anyhow::Error::new(PipelineError::InvalidState(
            "Conflict: source_publication_incomplete:embedding_work_pending".into(),
        ));
        let class = classify_failure(&error);
        assert_eq!(class, FailureClass::PendingDependencies);
        assert!(matches!(
            retry_action(class, 900, 3, 5, 60),
            RetryAction::Blocked { .. }
        ));
        let blocked = anyhow::Error::new(PipelineError::ConfigError("revoked binding".into()))
            .context("render_doc failed");
        assert_eq!(
            classify_failure(&blocked),
            FailureClass::ConfigurationBlocked
        );
    }
}
