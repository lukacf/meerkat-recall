//! Process-local summary outcome counters, exported into work-run metrics.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

static EMPTY: AtomicU64 = AtomicU64::new(0);
static INCOMPLETE: AtomicU64 = AtomicU64::new(0);
static PROVIDER_ERROR: AtomicU64 = AtomicU64::new(0);
static TRUNCATED: AtomicU64 = AtomicU64::new(0);

/// Monotonic totals since process start; these are not per-work attribution.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SummaryMetrics {
    /// Successful provider completion without visible text.
    pub empty_output_fallback: u64,
    /// Provider completion stopped before a complete answer.
    pub incomplete_output_fallback: u64,
    /// Non-authority provider errors handled by local fallback.
    pub provider_error_fallback: u64,
    /// Visible text shortened to the selected summary bound.
    pub local_truncation: u64,
}

/// Read current counters without resetting them.
#[must_use]
pub fn summary_metrics_snapshot() -> SummaryMetrics {
    SummaryMetrics {
        empty_output_fallback: EMPTY.load(Ordering::Relaxed),
        incomplete_output_fallback: INCOMPLETE.load(Ordering::Relaxed),
        provider_error_fallback: PROVIDER_ERROR.load(Ordering::Relaxed),
        local_truncation: TRUNCATED.load(Ordering::Relaxed),
    }
}

#[derive(Clone, Copy)]
pub(super) enum SummaryEvent {
    #[cfg(feature = "meerkat-extraction")]
    Empty,
    #[cfg(feature = "meerkat-extraction")]
    Incomplete,
    #[cfg(feature = "meerkat-extraction")]
    ProviderError,
    Truncated,
}

pub(super) fn record(event: SummaryEvent) {
    let (counter, reason) = match event {
        #[cfg(feature = "meerkat-extraction")]
        SummaryEvent::Empty => (&EMPTY, "empty_output"),
        #[cfg(feature = "meerkat-extraction")]
        SummaryEvent::Incomplete => (&INCOMPLETE, "incomplete_output"),
        #[cfg(feature = "meerkat-extraction")]
        SummaryEvent::ProviderError => (&PROVIDER_ERROR, "provider_error"),
        SummaryEvent::Truncated => (&TRUNCATED, "local_truncation"),
    };
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add(1))
    });
    tracing::warn!(
        summary_reason = reason,
        "Summary fallback or truncation applied"
    );
}
