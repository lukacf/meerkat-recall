//! Pure admission policy and controller shared verbatim with pipeline tests.
//! This state is local to one acceptance call, never a persisted work attempt.

use std::future::Future;
use std::time::Duration;

const MAX_ATTEMPTS: u32 = 16;
const RETRY_START_BUDGET: Duration = Duration::from_secs(2);
const RETRYABLE_ABORT: &str =
    "Failed to commit transaction due to a read or write conflict. This transaction can be retried";
const FAILED_TRANSACTION: &str = "The query was not executed due to a failed transaction";

/// Produced only by matching the native error variant or remote Query wrapper.
pub(super) enum ErrorEvidence<'a> {
    NativeRetryable,
    NativeFailedDetail(&'a str),
    RemoteQuery(&'a str),
    RollbackMarker,
    Other,
}

pub(super) fn proves_retryable_abort<'a>(errors: impl Iterator<Item = ErrorEvidence<'a>>) -> bool {
    let mut conflict = false;
    for error in errors {
        match error {
            ErrorEvidence::NativeRetryable => conflict = true,
            ErrorEvidence::NativeFailedDetail(message) if message == RETRYABLE_ABORT => {
                conflict = true;
            }
            ErrorEvidence::RemoteQuery(message)
                if message == RETRYABLE_ABORT
                    || message == format!("{FAILED_TRANSACTION}. {RETRYABLE_ABORT}") =>
            {
                conflict = true;
            }
            ErrorEvidence::RollbackMarker => {}
            ErrorEvidence::RemoteQuery(message) if message == FAILED_TRANSACTION => {}
            _ => return false,
        }
    }
    conflict
}

fn retry_delay(completed_attempts: u32, entropy: u8) -> Duration {
    let base_ms = 2_u64 << completed_attempts.saturating_sub(1).min(5);
    Duration::from_millis(base_ms + u64::from(entropy & 31))
}

fn retry_fits(completed_attempts: u32, elapsed: Duration, delay: Duration) -> bool {
    completed_attempts < MAX_ATTEMPTS
        && elapsed
            .checked_add(delay)
            .is_some_and(|next| next < RETRY_START_BUDGET)
}

/// Keep the last actual failure; admission expiry never invents a new error.
pub(super) async fn retry_aborted<T, E, F, Fut>(
    mut operation: F,
    mut entropy: impl FnMut() -> u8,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, (E, bool)>>,
{
    let started = tokio::time::Instant::now();
    let mut completed_attempts = 0;
    loop {
        let (error, aborted) = match operation().await {
            Ok(value) => return Ok(value),
            Err(failure) => failure,
        };
        completed_attempts += 1;
        if !aborted {
            return Err(error);
        }
        let delay = retry_delay(completed_attempts, entropy());
        if !retry_fits(completed_attempts, started.elapsed(), delay) {
            return Err(error);
        }
        tokio::time::sleep(delay).await;
        // Oversleep does not authorize starting a query beyond the budget.
        if started.elapsed() >= RETRY_START_BUDGET {
            return Err(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn exact_abort_evidence_and_mixed_errors() {
        let wrapped = format!("{FAILED_TRANSACTION}. {RETRYABLE_ABORT}");
        for error in [
            ErrorEvidence::NativeRetryable,
            ErrorEvidence::NativeFailedDetail(RETRYABLE_ABORT),
            ErrorEvidence::RemoteQuery(RETRYABLE_ABORT),
            ErrorEvidence::RemoteQuery(&wrapped),
        ] {
            assert!(proves_retryable_abort(
                [ErrorEvidence::RollbackMarker, error].into_iter()
            ));
        }
        for message in [
            "publication:conflict_generation".to_owned(),
            "publication:erasure_space_reserved".to_owned(),
            "connection acknowledgement lost".to_owned(),
            format!("An error occurred: {RETRYABLE_ABORT}"),
            format!("{RETRYABLE_ABORT} trailing data"),
            "some conflict can be retried".to_owned(),
        ] {
            assert!(!proves_retryable_abort(
                [ErrorEvidence::RemoteQuery(&message)].into_iter()
            ));
            assert!(!proves_retryable_abort(
                [ErrorEvidence::NativeFailedDetail(&message)].into_iter()
            ));
        }
        assert!(!proves_retryable_abort(
            [ErrorEvidence::RollbackMarker].into_iter()
        ));
        assert!(!proves_retryable_abort(std::iter::empty()));
        for errors in [
            [ErrorEvidence::NativeRetryable, ErrorEvidence::Other],
            [ErrorEvidence::Other, ErrorEvidence::NativeRetryable],
        ] {
            assert!(!proves_retryable_abort(errors.into_iter()));
        }
    }

    #[test]
    fn retry_admission_and_backoff_are_finite() {
        assert!(retry_fits(15, Duration::ZERO, Duration::from_millis(95)));
        assert!(!retry_fits(16, Duration::ZERO, Duration::ZERO));
        assert!(!retry_fits(
            1,
            Duration::from_millis(1999),
            Duration::from_millis(1)
        ));
        assert!(!retry_fits(1, Duration::MAX, Duration::from_millis(1)));
        assert_eq!(retry_delay(1, 0), Duration::from_millis(2));
        assert_eq!(retry_delay(16, 255), Duration::from_millis(95));
        assert_ne!(retry_delay(1, 0), retry_delay(1, 31));
    }

    #[tokio::test(start_paused = true)]
    async fn conflict_then_success_keeps_one_returned_value() {
        let attempts = AtomicUsize::new(0);
        let result = retry_aborted(
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 5 {
                        Err((attempt, true))
                    } else {
                        Ok("original receipt")
                    }
                }
            },
            || 0,
        )
        .await;
        assert_eq!(result, Ok("original receipt"));
        assert_eq!(attempts.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_conflicts_return_sixteenth_original_error() {
        let attempts = AtomicUsize::new(0);
        let result = retry_aborted(
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Err::<(), _>((attempt, true)) }
            },
            || 31,
        )
        .await;
        assert_eq!(result, Err(16));
        assert_eq!(attempts.load(Ordering::SeqCst), 16);
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_query_prevents_another_attempt_without_cancelling_it() {
        let attempts = AtomicUsize::new(0);
        let result = retry_aborted(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    Err::<(), _>(("last definite abort", true))
                }
            },
            || 0,
        )
        .await;
        assert_eq!(result, Err("last definite abort"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn scheduler_oversleep_does_not_start_another_attempt() {
        let attempts = AtomicUsize::new(0);
        let retry = retry_aborted(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(("original abort before sleep", true)) }
            },
            || 0,
        );
        let oversleep = async {
            while attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(3)).await;
        };
        let (result, ()) = tokio::join!(retry, oversleep);
        assert_eq!(result, Err("original abort before sleep"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_after_conflict_keeps_last_error_and_stops() {
        let attempts = AtomicUsize::new(0);
        let result = retry_aborted(
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err::<(), _>(("definite abort", true))
                    } else {
                        Err(("unknown commit outcome", false))
                    }
                }
            },
            || 0,
        )
        .await;
        assert_eq!(result, Err("unknown commit outcome"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
