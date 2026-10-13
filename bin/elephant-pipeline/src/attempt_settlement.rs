//! Bounded continuation of one captured execution result, never work execution.
//!
//! Database queries already retry atomic transaction conflicts. If those retries
//! are exhausted, retain the original result while retrying its bookkeeping.
//! One deadline covers every phase and never extends the original lease.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use elephant_pipeline::PipelineError;
use elephant_storage::{Repository, StorageClient, StorageError, WorkItemRepository};
use elephant_types::publication::AttemptFence;
use elephant_types::records::WorkItem;
use tokio::time::Instant;

const MAX_ATTEMPTS: usize = 8;
const MAX_CONTINUATION: Duration = Duration::from_secs(15);

pub(super) struct AttemptSettlement<'a> {
    storage: &'a StorageClient,
    original: &'a WorkItem,
    fence: AttemptFence,
    deadline: Instant,
}

impl<'a> AttemptSettlement<'a> {
    pub(super) fn new(storage: &'a StorageClient, original: &'a WorkItem) -> anyhow::Result<Self> {
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(original)?;
        let remaining = original
            .lease_expires_at
            .ok_or_else(stale_attempt)?
            .signed_duration_since(Utc::now())
            .to_std()
            .map_err(|_| stale_attempt())?;
        if remaining.is_zero() {
            return Err(stale_attempt());
        }
        Ok(Self {
            storage,
            original,
            fence,
            deadline: Instant::now() + remaining.min(MAX_CONTINUATION),
        })
    }

    /// Retry only persistence of the same result. The caller must not put work
    /// execution, provider dispatch, a new reservation or lease renewal here.
    pub(super) async fn persist<T, F, Fut>(
        &self,
        phase: &'static str,
        operation: F,
    ) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        retry_persistence(
            self.deadline,
            || self.check_original_claim(),
            operation,
            |attempt| Duration::from_millis(100_u64 << attempt.min(4)),
        )
        .await
        .inspect_err(|error| {
            tracing::warn!(
                work_item = %self.original.base.id,
                attempt_generation = self.fence.generation,
                phase,
                %error,
                "Original attempt persistence continuation stopped"
            );
        })
    }

    async fn check_original_claim(&self) -> anyhow::Result<()> {
        let current = WorkItemRepository::new(self.storage.clone())
            .get(&self.original.base.space_id, &self.original.base.id)
            .await?
            .ok_or_else(stale_attempt)?;
        if !same_live_claim(self.original, &self.fence, &current, Utc::now()) {
            return Err(stale_attempt());
        }
        Ok(())
    }
}

fn same_live_claim(
    original: &WorkItem,
    fence: &AttemptFence,
    current: &WorkItem,
    now: DateTime<Utc>,
) -> bool {
    original.lease_expires_at.is_some_and(|expiry| expiry > now)
        && current.lease_expires_at.is_some_and(|expiry| expiry > now)
        && current.base.deleted_at.is_none()
        && current.base.id == original.base.id
        && current.base.space_id == original.base.space_id
        && elephant_storage::work_attempt_repository::fence_from_claim(current)
            .is_ok_and(|observed| observed == *fence)
}

fn stale_attempt() -> anyhow::Error {
    PipelineError::LeaseConflict("Original settlement attempt expired or changed".into()).into()
}

// Match the storage adapter's actual transaction-conflict diagnostic only.
// Typed security, erasure, stale-input and integrity errors cannot opt into
// retry merely by containing these words. No untyped top-level fallback.
fn retryable_conflict(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        let storage = if let Some(error) = cause.downcast_ref::<StorageError>() {
            error
        } else if let Some(error) = cause.downcast_ref::<PipelineError>() {
            let PipelineError::StorageError(storage) = error else {
                return false;
            };
            storage
        } else if cause.is::<elephant_pipeline::scheduler::execution_contract::WorkFailure>() {
            return false;
        } else {
            continue;
        };
        return match storage {
            StorageError::QueryError(message) => {
                let message = message.to_ascii_lowercase();
                message.contains("failed to commit transaction due to a read or write conflict")
                    && message.contains("this transaction can be retried")
            }
            _ => false,
        };
    }
    false
}

async fn retry_persistence<T, G, Guard, F, Fut, D>(
    deadline: Instant,
    mut guard: G,
    mut operation: F,
    delay: D,
) -> anyhow::Result<T>
where
    G: FnMut() -> Guard,
    Guard: Future<Output = anyhow::Result<()>>,
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
    D: Fn(usize) -> Duration,
{
    for attempt in 0..MAX_ATTEMPTS {
        if Instant::now() >= deadline {
            anyhow::bail!("attempt_settlement_deadline: original continuation budget exhausted");
        }
        // A timeout never grants a retry: a cancelled database operation may
        // already have committed. Existing durable recovery resolves that case.
        let result = tokio::time::timeout_at(deadline, async {
            guard().await?;
            operation().await
        })
        .await
        .map_err(|_| anyhow::anyhow!("attempt_settlement_deadline: persistence interrupted"))?;
        match result {
            Ok(value) => return Ok(value),
            Err(error) if attempt + 1 < MAX_ATTEMPTS && retryable_conflict(&error) => {
                let next = Instant::now() + delay(attempt);
                if next >= deadline {
                    return Err(error);
                }
                tokio::time::sleep_until(next).await;
            }
            Err(error) => return Err(error),
        }
    }
    anyhow::bail!("attempt_settlement_attempts_exhausted")
}

#[cfg(test)]
#[path = "attempt_settlement_tests.rs"]
mod tests;
