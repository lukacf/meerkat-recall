//! Priority queue with lease semantics for work items.
//!
//! Per spec §5.1: The work queue provides durable, priority-ordered job scheduling
//! with lease-based concurrency control.

use chrono::{DateTime, Duration, Utc};
use elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository;
use elephant_storage::ingest_decision_repository::IngestDecisionRepository;
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::work_run_repository::WorkRunRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::ingest::Budget;
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::RecordId;
use serde_json::Value;
use std::str::FromStr;

use crate::error::PipelineError;
use crate::scheduler::budget::BudgetEnforcer;

/// Default space ID for single-space deployments.
#[allow(dead_code)]
const DEFAULT_SPACE_ID: &str = "default";

/// Default lease duration in seconds.
const DEFAULT_LEASE_DURATION_SECS: i64 = 300;
const SELECTOR_SCAN_MULTIPLIER: usize = 20;
const SELECTOR_SCAN_MIN_LIMIT: usize = 10_000;

/// Configuration for the work queue.
#[derive(Debug, Clone)]
pub struct WorkQueueConfig {
    /// How long a lease lasts before expiring.
    pub lease_duration_secs: i64,
    /// Maximum items to fetch in a single poll.
    pub batch_size: usize,
    /// Worker identifier for lease tracking.
    pub worker_id: String,
    /// Optional override for the runnable candidate scan window. The default
    /// window is large (10k) so selector-filtered workers can find eligible
    /// work behind a backlog of other kinds; workers that claim every kind can
    /// set this much lower to keep deep-queue scans cheap on the database.
    pub scan_limit: Option<usize>,
}

impl Default for WorkQueueConfig {
    fn default() -> Self {
        Self {
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            batch_size: 10,
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            scan_limit: None,
        }
    }
}

/// Priority queue with lease semantics.
#[derive(Debug, Clone)]
pub struct WorkQueue {
    repo: WorkItemRepository,
    config: WorkQueueConfig,
    storage: StorageClient,
}

impl WorkQueue {
    /// Creates a new work queue.
    #[must_use]
    pub fn new(storage: StorageClient, config: WorkQueueConfig) -> Self {
        Self {
            repo: WorkItemRepository::new(storage.clone()),
            config,
            storage,
        }
    }

    /// Creates a work queue with default configuration.
    #[must_use]
    pub fn with_defaults(storage: StorageClient) -> Self {
        Self::new(storage, WorkQueueConfig::default())
    }

    /// Enqueues a new work item.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be persisted.
    pub async fn enqueue(&self, item: WorkItem) -> Result<WorkItem, PipelineError> {
        self.repo
            .create(item)
            .await
            .map_err(|e| PipelineError::StorageError(e))
    }

    /// Claims the next available work item.
    ///
    /// Returns `None` if no runnable items are available.
    ///
    /// # Errors
    ///
    /// Returns an error if the claim fails.
    pub async fn claim(&self) -> Result<Option<WorkItem>, PipelineError> {
        self.claim_matching(|_| true).await
    }

    /// Claims the next available work item that matches a caller-provided selector.
    ///
    /// Returns `None` if no runnable items are available for the selector.
    ///
    /// # Errors
    ///
    /// Returns an error if the claim fails.
    pub async fn claim_matching<F>(&self, selector: F) -> Result<Option<WorkItem>, PipelineError>
    where
        F: Fn(&WorkItem) -> bool,
    {
        Ok(self
            .claim_batch_matching(1, selector)
            .await?
            .into_iter()
            .next())
    }

    /// Claims up to `max_total` runnable work items that match the selector.
    ///
    /// The candidate scan and expired-lease recovery run once per call, not
    /// once per claimed item, so filling a worker's free capacity costs a
    /// single queue round instead of `max_total` of them.
    ///
    /// # Errors
    ///
    /// Returns an error if the candidate scan fails. Individual claim
    /// conflicts (another worker won the item) are skipped, not errors.
    pub async fn claim_batch_matching<F>(
        &self,
        max_total: usize,
        selector: F,
    ) -> Result<Vec<WorkItem>, PipelineError>
    where
        F: Fn(&WorkItem) -> bool,
    {
        if max_total == 0 {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let lease_expires = now + Duration::seconds(self.config.lease_duration_secs);

        // Find runnable items across all spaces, ordered by priority
        let mut candidates = self.find_runnable(&now).await?;

        // Recover expired leases for spaces that have candidates, plus the
        // default space. Re-fetch only when recovery actually released items.
        let mut recovery_spaces: std::collections::HashSet<String> =
            candidates.iter().map(|c| c.base.space_id.clone()).collect();
        recovery_spaces.insert(DEFAULT_SPACE_ID.to_string());
        if self.recover_leases_for_spaces(&recovery_spaces).await {
            candidates = self.find_runnable(&now).await?;
        }

        let mut claimed = Vec::new();
        for candidate in candidates {
            if claimed.len() >= max_total {
                break;
            }
            if !selector(&candidate) {
                continue;
            }
            if let Ok(item) = self.try_claim(&candidate, &lease_expires).await {
                claimed.push(item);
            }
            // Claim conflict: another worker beat us, try next candidate
        }

        Ok(claimed)
    }

    /// Claims a runnable item of `preferred_kind` before falling back to the normal queue.
    ///
    /// This is used for stage-aware scheduling where a scarce execution lane
    /// is available and should not sit idle behind a large backlog of other
    /// runnable work kinds.
    ///
    /// # Errors
    ///
    /// Returns an error if the claim fails.
    pub async fn claim_prefer_kind<F>(
        &self,
        preferred_kind: &str,
        fallback_selector: F,
    ) -> Result<Option<WorkItem>, PipelineError>
    where
        F: Fn(&WorkItem) -> bool,
    {
        Ok(self
            .claim_batch_prefer_kind(preferred_kind, 1, 1, fallback_selector)
            .await?
            .into_iter()
            .next())
    }

    /// Claims up to `max_total` runnable items, giving items of
    /// `preferred_kind` first pick of at most `max_preferred` slots.
    ///
    /// `max_preferred` models a scarce execution lane (e.g. extract slots):
    /// at most that many `preferred_kind` items are claimed, and with
    /// `max_preferred == 0` the kind is skipped entirely. All other kinds
    /// compete for the remaining capacity in priority order.
    ///
    /// # Errors
    ///
    /// Returns an error if a candidate scan fails. Individual claim conflicts
    /// are skipped, not errors.
    pub async fn claim_batch_prefer_kind<F>(
        &self,
        preferred_kind: &str,
        max_preferred: usize,
        max_total: usize,
        fallback_selector: F,
    ) -> Result<Vec<WorkItem>, PipelineError>
    where
        F: Fn(&WorkItem) -> bool,
    {
        if max_total == 0 {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let lease_expires = now + Duration::seconds(self.config.lease_duration_secs);
        let preferred_quota = max_preferred.min(max_total);

        let mut claimed: Vec<WorkItem> = Vec::new();
        let mut claimed_ids: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
        let mut recovered_spaces: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Phase 1: fill the scarce preferred lane from a cheap by-kind scan.
        if preferred_quota > 0 {
            let scan_limit = preferred_quota.max(self.config.batch_size);
            let mut preferred = self
                .repo
                .find_runnable_by_kind_all_spaces(preferred_kind, scan_limit)
                .await
                .map_err(PipelineError::StorageError)?;

            let spaces: std::collections::HashSet<String> = preferred
                .iter()
                .map(|c| c.base.space_id.clone())
                .filter(|space| !recovered_spaces.contains(space))
                .collect();
            recovered_spaces.extend(spaces.iter().cloned());
            if self.recover_leases_for_spaces(&spaces).await {
                preferred = self
                    .repo
                    .find_runnable_by_kind_all_spaces(preferred_kind, scan_limit)
                    .await
                    .map_err(PipelineError::StorageError)?;
            }

            for candidate in preferred {
                if claimed.len() >= preferred_quota {
                    break;
                }
                if !fallback_selector(&candidate) {
                    continue;
                }
                if let Ok(item) = self.try_claim(&candidate, &lease_expires).await {
                    claimed_ids.insert(item.base.id.clone());
                    claimed.push(item);
                }
            }
        }

        // Phase 2: fill remaining capacity from the general queue in priority
        // order, respecting the preferred lane's remaining quota.
        if claimed.len() < max_total {
            let mut preferred_claimed = claimed.len();
            let mut candidates = self.find_runnable(&now).await?;

            let spaces: std::collections::HashSet<String> = candidates
                .iter()
                .map(|c| c.base.space_id.clone())
                .chain(std::iter::once(DEFAULT_SPACE_ID.to_string()))
                .filter(|space| !recovered_spaces.contains(space))
                .collect();
            recovered_spaces.extend(spaces.iter().cloned());
            if self.recover_leases_for_spaces(&spaces).await {
                candidates = self.find_runnable(&now).await?;
            }

            for candidate in candidates {
                if claimed.len() >= max_total {
                    break;
                }
                if claimed_ids.contains(&candidate.base.id) {
                    continue;
                }
                if candidate.kind == preferred_kind && preferred_claimed >= preferred_quota {
                    continue;
                }
                if !fallback_selector(&candidate) {
                    continue;
                }
                if let Ok(item) = self.try_claim(&candidate, &lease_expires).await {
                    if item.kind == preferred_kind {
                        preferred_claimed += 1;
                    }
                    claimed_ids.insert(item.base.id.clone());
                    claimed.push(item);
                }
            }
        }

        Ok(claimed)
    }

    /// Recovers expired leases for the given spaces; returns true when any
    /// lease was actually recovered (i.e. new items may now be runnable).
    async fn recover_leases_for_spaces(&self, spaces: &std::collections::HashSet<String>) -> bool {
        let mut any_recovered = false;
        for space_id in spaces {
            if let Ok(recovered) = self.recover_expired_leases(space_id).await {
                if !recovered.is_empty() {
                    any_recovered = true;
                }
            }
        }
        any_recovered
    }

    /// Counts work items that are runnable right now.
    ///
    /// Health surfaces use this count to distinguish an empty queue from a
    /// blocked queue, since both states show zero throughput.
    ///
    /// Uses an aggregate across all spaces, independent of the candidate scan
    /// window used when claiming work.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate query fails.
    pub async fn count_runnable(&self) -> Result<usize, PipelineError> {
        self.repo
            .count_runnable()
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_runnable(&self, now: &DateTime<Utc>) -> Result<Vec<WorkItem>, PipelineError> {
        let scan_limit = self
            .config
            .scan_limit
            .unwrap_or_else(|| {
                self.config
                    .batch_size
                    .saturating_mul(SELECTOR_SCAN_MULTIPLIER)
                    .max(self.config.batch_size)
                    // Workers can be configured to claim only a subset of kinds. If the
                    // queue has a large older backlog of other kinds, a small initial
                    // scan window can starve the eligible work indefinitely.
                    .max(SELECTOR_SCAN_MIN_LIMIT)
            })
            .max(1);
        let items = self
            .repo
            .find_runnable_all_spaces(scan_limit)
            .await
            .map_err(PipelineError::StorageError)?;

        // Filter by run_after and sort by priority
        let mut runnable: Vec<_> = items
            .into_iter()
            .filter(|item| item.run_after.map_or(true, |run_after| run_after <= *now))
            .collect();

        runnable.sort_by(|a, b| b.priority.cmp(&a.priority));

        Ok(runnable)
    }

    /// Attempts to claim a specific work item.
    ///
    /// Uses optimistic locking via atomic update_status_conditional to handle concurrent claims.
    async fn try_claim(
        &self,
        item: &WorkItem,
        lease_expires: &DateTime<Utc>,
    ) -> Result<WorkItem, PipelineError> {
        let space_id = &item.base.space_id;
        let id = &item.base.id;

        // Use atomic conditional update: only claim if still in Runnable state
        let claimed = self
            .repo
            .claim_with_lease(space_id, id, &self.config.worker_id, *lease_expires)
            .await
            .map_err(PipelineError::StorageError)?;

        match claimed {
            Some(item) => Ok(item),
            None => Err(PipelineError::LeaseConflict(id.to_string())),
        }
    }

    /// Releases a lease, returning the item to runnable state.
    ///
    /// # Errors
    ///
    /// Returns an error if the release fails.
    pub async fn release(&self, space_id: &str, id: &RecordId) -> Result<WorkItem, PipelineError> {
        let released = self
            .repo
            .release_expired_lease(space_id, id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::LeaseConflict(id.to_string()))?;

        Ok(released)
    }

    /// Extends the current worker's lease for a long-running work item.
    ///
    /// # Errors
    ///
    /// Returns a lease conflict if ownership has already been lost, or a
    /// storage error if the conditional update fails.
    pub async fn renew_lease(
        &self,
        space_id: &str,
        id: &RecordId,
        lease_duration_secs: u64,
    ) -> Result<WorkItem, PipelineError> {
        let lease_seconds = i64::try_from(lease_duration_secs).map_err(|error| {
            PipelineError::ValidationError(format!(
                "Invalid lease duration {lease_duration_secs}: {error}"
            ))
        })?;
        let lease_duration = Duration::try_seconds(lease_seconds).ok_or_else(|| {
            PipelineError::ValidationError(format!(
                "Invalid lease duration {lease_duration_secs}: out of range"
            ))
        })?;
        self.repo
            .renew_lease(
                space_id,
                id,
                &self.config.worker_id,
                Utc::now() + lease_duration,
            )
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::LeaseConflict(id.to_string()))
    }

    /// Marks a work item as completed successfully.
    ///
    /// If the lease was lost (e.g. recovered by another worker's expired-lease sweep),
    /// attempts a force-complete since the work was already done.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn complete(&self, space_id: &str, id: &RecordId) -> Result<WorkItem, PipelineError> {
        let result = self
            .repo
            .update_running_for_worker(
                space_id,
                id,
                &self.config.worker_id,
                WorkItemStatus::Done,
                None,
            )
            .await
            .map_err(PipelineError::StorageError)?;

        if let Some(completed) = result {
            self.reconcile_cost_if_needed(&completed, true).await?;
            return Ok(completed);
        }

        // Lease lost — the item may have been released back to runnable.
        // Force-complete it since the work was actually done.
        tracing::warn!(work_item = %id, "Lease lost during completion, attempting force-complete");
        let force_completed = self
            .repo
            .force_complete(space_id, id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::LeaseConflict(id.to_string()))?;

        self.reconcile_cost_if_needed(&force_completed, true)
            .await?;
        Ok(force_completed)
    }

    /// Marks a work item as failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn fail(
        &self,
        space_id: &str,
        id: &RecordId,
        error: String,
    ) -> Result<WorkItem, PipelineError> {
        let failed = self
            .repo
            .update_running_for_worker(
                space_id,
                id,
                &self.config.worker_id,
                WorkItemStatus::Failed,
                Some(error),
            )
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::LeaseConflict(id.to_string()))?;

        self.reconcile_cost_if_needed(&failed, false).await?;

        Ok(failed)
    }

    async fn reconcile_cost_if_needed(
        &self,
        item: &WorkItem,
        success: bool,
    ) -> Result<(), PipelineError> {
        let Some(ledger_id) = self
            .resolve_cost_ledger_id(item)
            .await
            .map_err(PipelineError::StorageError)?
        else {
            return Ok(());
        };

        let ledger_repo = CostLedgerDailyRepository::new(self.storage.clone());
        let Some(ledger) = ledger_repo
            .get(&item.base.space_id, &ledger_id)
            .await
            .map_err(PipelineError::StorageError)?
        else {
            return Ok(());
        };

        if ledger.reconciled_euros > 0.0 || ledger.reserved_euros <= 0.0 {
            return Ok(());
        }

        let (mut actual_cost, actual_tokens) = self.estimate_actuals(item, success).await?;
        if actual_cost <= 0.0 && actual_tokens == 0 {
            // Banking the reservation for an item that recorded no usage is only
            // defensible when the work actually ran: a completed item whose
            // metrics went missing did consume something we failed to measure. A
            // FAILED item with no metrics is the opposite — most failures here
            // are fail-fast (a model the vendored catalog cannot resolve, a
            // rejected guard), so charging the full reservation bills the budget
            // for work that never reached a provider. That failure mode is
            // self-amplifying: the cheapest failures charge the most relative to
            // what they used, so a single misconfiguration drains the whole
            // day's ceiling and stops the pipeline.
            //
            // Charge zero rather than returning early — reconcile_cost is what
            // releases the reservation, both on the ledger row and on the guard
            // that enforces the ceiling. Skipping it would leave the reservation
            // held for the rest of the day, which is the very thing being fixed.
            actual_cost = if success {
                ledger.reserved_euros.max(0.0)
            } else {
                0.0
            };
        }

        let budget_enforcer = BudgetEnforcer::new(
            self.storage.clone(),
            Budget::default(),
            item.base.space_id.clone(),
        );

        budget_enforcer
            .reconcile_cost(&ledger_id, actual_cost, actual_tokens, success)
            .await?;

        Ok(())
    }

    async fn estimate_actuals(
        &self,
        item: &WorkItem,
        success: bool,
    ) -> Result<(f64, u64), PipelineError> {
        let run_repo = WorkRunRepository::new(self.storage.clone());
        let runs = run_repo
            .find_by_work_item(&item.base.id)
            .await
            .map_err(PipelineError::StorageError)?;

        let latest_metrics = runs.last().map(|run| run.metrics.clone());
        let mut tokens_used = latest_metrics
            .as_ref()
            .map(|metrics| metrics.tokens_used)
            .unwrap_or(0);

        // The payload's token count is what the extraction plan INTENDED to
        // send, not what was sent. Treating it as consumption is a fair proxy
        // for completed work whose metrics went missing, but for a failed item
        // it invents usage that may never have happened — so only successful
        // work may fall back to the plan. A failure that genuinely burned tokens
        // is still charged, via the recorded metrics above; the residual gap is
        // a failure that spent tokens and recorded none, which we under-charge
        // deliberately rather than over-charge every fail-fast item.
        if tokens_used == 0 && success {
            tokens_used = estimated_tokens_from_payload(&item.payload).unwrap_or(0);
        }

        let mut actual_cost = latest_metrics
            .as_ref()
            .map(|metrics| metrics.estimated_cost_euros)
            .unwrap_or(0.0);

        if actual_cost <= 0.0 && tokens_used > 0 {
            actual_cost = estimate_cost_from_tokens(tokens_used);
        }

        Ok((actual_cost, u64::from(tokens_used)))
    }

    async fn resolve_cost_ledger_id(
        &self,
        item: &WorkItem,
    ) -> Result<Option<RecordId>, elephant_storage::error::StorageError> {
        if let Some(id) = record_id_from_payload(&item.payload, &["cost_ledger_id", "ledger_id"]) {
            return Ok(Some(id));
        }

        if let Some(decision_id) =
            record_id_from_payload(&item.payload, &["ingest_decision_id", "decision_id"])
        {
            let decision_repo = IngestDecisionRepository::new(self.storage.clone());
            let decision = decision_repo.get(&item.base.space_id, &decision_id).await?;
            return Ok(decision.and_then(|d| d.cost_ledger_id));
        }

        if let Some(source_id) = record_id_from_payload(&item.payload, &["source_id"]) {
            let decision_repo = IngestDecisionRepository::new(self.storage.clone());
            let decision = decision_repo
                .find_latest_by_source(&item.base.space_id, &source_id)
                .await?;
            return Ok(decision.and_then(|d| d.cost_ledger_id));
        }

        Ok(None)
    }

    /// Recovers expired leases (from crashed workers).
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub async fn recover_expired_leases(
        &self,
        space_id: &str,
    ) -> Result<Vec<WorkItem>, PipelineError> {
        let now = Utc::now();

        let running = self
            .repo
            .find_by_status(space_id, WorkItemStatus::Running)
            .await
            .map_err(PipelineError::StorageError)?;

        // Grace period prevents reclaiming leases that only just expired,
        // avoiding race conditions with workers about to complete.
        let grace_period = Duration::seconds(60);
        let mut recovered = Vec::new();
        for item in running {
            if let Some(lease_expires) = item.lease_expires_at {
                if lease_expires + grace_period < now {
                    if let Some(released) = self
                        .repo
                        .release_expired_lease(space_id, &item.base.id)
                        .await
                        .map_err(PipelineError::StorageError)?
                    {
                        recovered.push(released);
                    }
                }
            }
        }

        Ok(recovered)
    }

    /// Returns the underlying repository for direct access.
    #[must_use]
    pub const fn repository(&self) -> &WorkItemRepository {
        &self.repo
    }
}

fn record_id_from_payload(payload: &Value, keys: &[&str]) -> Option<RecordId> {
    let obj = payload.as_object()?;
    for key in keys {
        if let Some(value) = obj.get(*key).and_then(|v| v.as_str()) {
            if let Ok(record_id) = RecordId::from_str(value) {
                return Some(record_id);
            }
        }
    }
    None
}

fn estimated_tokens_from_payload(payload: &Value) -> Option<u32> {
    let obj = payload.as_object()?;
    obj.get("estimated_tokens")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
}

fn estimate_cost_from_tokens(tokens: u32) -> f64 {
    f64::from(tokens) * 0.000_001
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::{Duration, Utc};
    use elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository;
    use elephant_storage::ingest_decision_repository::IngestDecisionRepository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::ingest::{CostLedgerDaily, IngestDecision, IngestMode};
    use elephant_types::Link;

    const TEST_SPACE: &str = "default";

    fn make_work_item(kind: &str, priority: i32) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            kind: kind.to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority,
            idempotency_key: format!("idem-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn test_enqueue_and_claim() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let item = make_work_item("test_kind", 10);
        let enqueued = queue.enqueue(item).await.expect("enqueue");
        assert_eq!(enqueued.status, WorkItemStatus::Runnable);

        let claimed = queue.claim().await.expect("claim");
        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.status, WorkItemStatus::Running);
        assert!(claimed.leased_by.is_some());
        assert_eq!(claimed.attempts, 1);
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        // Enqueue low priority first
        let low = make_work_item("low", 1);
        queue.enqueue(low).await.expect("enqueue low");

        // Enqueue high priority second
        let high = make_work_item("high", 100);
        queue.enqueue(high).await.expect("enqueue high");

        // High priority should be claimed first
        let claimed = queue.claim().await.expect("claim");
        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.kind, "high");
    }

    #[tokio::test]
    async fn test_claim_recovers_expired_leases() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let config = WorkQueueConfig {
            worker_id: "worker-1".to_string(),
            ..WorkQueueConfig::default()
        };
        let queue = WorkQueue::new(storage.clone(), config);

        let item = make_work_item("expired_lease", 10);
        let enqueued = queue.enqueue(item).await.expect("enqueue");
        let claimed = queue.claim().await.expect("claim").expect("claimed item");
        assert_eq!(claimed.base.id, enqueued.base.id);
        assert_eq!(claimed.leased_by.as_deref(), Some("worker-1"));

        let mut crashed = claimed.clone();
        crashed.lease_expires_at = Some(Utc::now() - Duration::seconds(60));
        crashed.status = WorkItemStatus::Running;
        let crashed_id = crashed.base.id.clone();
        queue
            .repository()
            .update(TEST_SPACE, &crashed_id, crashed)
            .await
            .expect("update crashed item");

        let config2 = WorkQueueConfig {
            worker_id: "worker-2".to_string(),
            ..WorkQueueConfig::default()
        };
        let queue2 = WorkQueue::new(storage, config2);

        let reclaimed = queue2
            .claim()
            .await
            .expect("reclaim")
            .expect("reclaimed item");
        assert_eq!(reclaimed.base.id, claimed.base.id);
        assert_eq!(reclaimed.status, WorkItemStatus::Running);
        assert_eq!(reclaimed.leased_by.as_deref(), Some("worker-2"));
        assert!(reclaimed.lease_expires_at.is_some());
    }

    #[tokio::test]
    async fn test_complete_and_fail() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let item = make_work_item("test", 10);
        let enqueued = queue.enqueue(item).await.expect("enqueue");

        let claimed = queue.claim().await.expect("claim").unwrap();
        assert_eq!(claimed.status, WorkItemStatus::Running);

        let completed = queue
            .complete(TEST_SPACE, &enqueued.base.id)
            .await
            .expect("complete");
        assert_eq!(completed.status, WorkItemStatus::Done);
        assert!(completed.lease_expires_at.is_none());
    }

    #[tokio::test]
    async fn test_reconcile_cost_on_complete_updates_ledger() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage.clone());

        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let ledger_repo = CostLedgerDailyRepository::new(storage.clone());
        let ledger_id = RecordId::new_v4(TableName::CostLedgerDaily);
        let ledger = CostLedgerDaily {
            base: BaseFields::new(
                ledger_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            date: Utc::now().date_naive(),
            mode: IngestMode::Retrieval,
            worker_kind: "triage".to_string(),
            total_euros: 0.0,
            total_items: 0,
            total_tokens: 0,
            total_runs: 0,
            total_failures: 0,
            reserved_euros: 3.0,
            reconciled_euros: 0.0,
        };
        ledger_repo.create(ledger).await.expect("create ledger");

        let mut item = make_work_item("test", 10);
        item.payload = serde_json::json!({
            "cost_ledger_id": ledger_id.to_string()
        });
        let enqueued = queue.enqueue(item).await.expect("enqueue");

        let _claimed = queue.claim().await.expect("claim").unwrap();
        queue
            .complete(TEST_SPACE, &enqueued.base.id)
            .await
            .expect("complete");

        let updated = ledger_repo
            .get(TEST_SPACE, &ledger_id)
            .await
            .expect("get ledger")
            .expect("ledger");

        assert_eq!(updated.reserved_euros, 0.0);
        assert!(updated.reconciled_euros > 0.0);
    }

    #[tokio::test]
    async fn test_reconcile_cost_on_complete_updates_ledger_via_ingest_decision() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage.clone());

        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE ingest_decision SCHEMALESS; DEFINE TABLE work_item SCHEMALESS;")
            .await
            .expect("define tables");

        let ledger_repo = CostLedgerDailyRepository::new(storage.clone());
        let ledger_id = RecordId::new_v4(TableName::CostLedgerDaily);
        let ledger = CostLedgerDaily {
            base: BaseFields::new(
                ledger_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            date: Utc::now().date_naive(),
            mode: IngestMode::Retrieval,
            worker_kind: "triage".to_string(),
            total_euros: 0.0,
            total_items: 0,
            total_tokens: 0,
            total_runs: 0,
            total_failures: 0,
            reserved_euros: 4.0,
            reconciled_euros: 0.0,
        };
        ledger_repo.create(ledger).await.expect("create ledger");

        let decision_repo = IngestDecisionRepository::new(storage.clone());
        let decision_id = RecordId::new_v4(TableName::IngestDecision);
        let decision = IngestDecision {
            base: BaseFields::new(
                decision_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            source: Link::new(RecordId::new_v4(TableName::Source)),
            decision: IngestMode::Retrieval,
            reason_codes: Vec::new(),
            confidence: 1.0,
            estimated_cost: 4.0,
            cost_ledger_id: Some(ledger_id.clone()),
            budget_snapshot: None,
            policy_version: None,
            downgrade_reason: None,
            original_mode: None,
        };
        decision_repo
            .create(decision)
            .await
            .expect("create decision");

        let mut item = make_work_item("test", 10);
        item.payload = serde_json::json!({
            "ingest_decision_id": decision_id.to_string()
        });
        let enqueued = queue.enqueue(item).await.expect("enqueue");

        let _claimed = queue.claim().await.expect("claim").unwrap();
        queue
            .complete(TEST_SPACE, &enqueued.base.id)
            .await
            .expect("complete");

        let updated = ledger_repo
            .get(TEST_SPACE, &ledger_id)
            .await
            .expect("get ledger")
            .expect("ledger");

        assert_eq!(updated.reserved_euros, 0.0);
        assert!(updated.reconciled_euros > 0.0);
    }

    #[tokio::test]
    async fn test_reconcile_cost_on_fail_updates_ledger() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage.clone());

        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let ledger_repo = CostLedgerDailyRepository::new(storage.clone());
        let ledger_id = RecordId::new_v4(TableName::CostLedgerDaily);
        let ledger = CostLedgerDaily {
            base: BaseFields::new(
                ledger_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            date: Utc::now().date_naive(),
            mode: IngestMode::Retrieval,
            worker_kind: "triage".to_string(),
            total_euros: 0.0,
            total_items: 0,
            total_tokens: 0,
            total_runs: 0,
            total_failures: 0,
            reserved_euros: 2.0,
            reconciled_euros: 0.0,
        };
        ledger_repo.create(ledger).await.expect("create ledger");

        let mut item = make_work_item("test", 10);
        item.payload = serde_json::json!({
            "cost_ledger_id": ledger_id.to_string()
        });
        let enqueued = queue.enqueue(item).await.expect("enqueue");

        let _claimed = queue.claim().await.expect("claim").unwrap();
        queue
            .fail(TEST_SPACE, &enqueued.base.id, "boom".to_string())
            .await
            .expect("fail");

        let updated = ledger_repo
            .get(TEST_SPACE, &ledger_id)
            .await
            .expect("get ledger")
            .expect("ledger");

        // The reservation is RELEASED (so it stops occupying the daily ceiling)
        // but nothing is billed: this item failed without any recorded token
        // usage, so there is no evidence a provider was ever reached. Charging
        // it here is what let 434 fail-fast extraction failures exhaust the
        // daily graph budget on 2026-07-30 and halt the pipeline.
        assert_eq!(updated.reserved_euros, 0.0, "reservation must be released");
        assert_eq!(
            updated.reconciled_euros, 0.0,
            "a failure with no recorded usage must not be billed"
        );
        assert_eq!(updated.total_euros, 0.0);
        assert_eq!(updated.total_failures, 1, "but it is still counted");
    }

    /// The planned-token fallback is what turned fail-fast failures into
    /// full-price charges. It must apply to completed work only.
    ///
    /// This tests estimate_actuals directly because that is where the gate sits:
    /// recorded run metrics are read BEFORE the fallback, so a failure that
    /// genuinely burned tokens is still billed from its metrics regardless.
    #[tokio::test]
    async fn test_planned_tokens_are_only_charged_for_completed_work() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage.clone());

        let mut item = make_work_item("test", 10);
        item.payload = serde_json::json!({ "estimated_tokens": 50_000 });

        // Completed: the plan is a fair proxy for usage we failed to measure.
        let (cost, tokens) = queue
            .estimate_actuals(&item, true)
            .await
            .expect("estimate for completed item");
        assert_eq!(tokens, 50_000);
        assert!(
            cost > 0.0,
            "completed work with no metrics should still be charged, got {cost}"
        );

        // Failed: nothing recorded, so there is no evidence a provider was
        // reached. The plan describes what we INTENDED to send, not what we sent.
        let (cost, tokens) = queue
            .estimate_actuals(&item, false)
            .await
            .expect("estimate for failed item");
        assert_eq!(
            tokens, 0,
            "planned tokens must not be counted as usage on failure"
        );
        assert_eq!(cost, 0.0, "a fail-fast item must not be billed");
    }

    #[tokio::test]
    async fn test_claim_empty_queue() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let claimed = queue.claim().await.expect("claim");
        assert!(claimed.is_none());
    }

    #[tokio::test]
    async fn test_enqueue_and_claim_embedded() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let item = make_work_item("test_kind", 10);
        let enqueued = queue.enqueue(item).await.expect("enqueue");
        assert_eq!(enqueued.status, WorkItemStatus::Runnable);

        let claimed = queue.claim().await.expect("claim");
        assert!(claimed.is_some());
        let claimed = claimed.expect("claimed item");
        assert_eq!(claimed.status, WorkItemStatus::Running);
        assert!(claimed.leased_by.is_some());
        assert_eq!(claimed.attempts, 1);
    }

    #[tokio::test]
    async fn test_claim_matching_can_skip_heavy_work_items() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let mut extract = make_work_item("extract_assertions", 100);
        extract.base.space_id = "default".to_string();
        queue.enqueue(extract).await.expect("enqueue extract");

        let mut light = make_work_item("triage_source", 10);
        light.base.space_id = "default".to_string();
        let light = queue.enqueue(light).await.expect("enqueue light");

        let claimed = queue
            .claim_matching(|item| item.kind != "extract_assertions")
            .await
            .expect("claim matching")
            .expect("matched item");

        assert_eq!(claimed.base.id, light.base.id);
        assert_eq!(claimed.kind, "triage_source");
    }

    #[tokio::test]
    async fn test_claim_matching_scans_past_initial_batch_of_skipped_items() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let config = WorkQueueConfig {
            batch_size: 1,
            ..WorkQueueConfig::default()
        };
        let queue = WorkQueue::new(storage, config);

        for _ in 0..(SELECTOR_SCAN_MULTIPLIER + 5) {
            let mut skipped = make_work_item("render_doc", 100);
            skipped.base.space_id = "default".to_string();
            queue.enqueue(skipped).await.expect("enqueue skipped");
        }

        let mut allowed = make_work_item("plan_extraction", 1);
        allowed.base.space_id = "default".to_string();
        let allowed = queue.enqueue(allowed).await.expect("enqueue allowed");

        let claimed = queue
            .claim_matching(|item| item.kind != "render_doc")
            .await
            .expect("claim matching")
            .expect("matched item");

        assert_eq!(claimed.base.id, allowed.base.id);
        assert_eq!(claimed.kind, "plan_extraction");
    }

    #[tokio::test]
    async fn test_claim_prefer_kind_claims_extract_before_render_backlog() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let config = WorkQueueConfig {
            batch_size: 2,
            ..WorkQueueConfig::default()
        };
        let queue = WorkQueue::new(storage, config);

        for index in 0..8 {
            let mut render = make_work_item("render_doc", 50);
            render.base.space_id = "default".to_string();
            render.idempotency_key = format!("render-{index}");
            queue.enqueue(render).await.expect("enqueue render");
        }

        let mut extract = make_work_item("extract_assertions", 50);
        extract.base.space_id = "default".to_string();
        let extract = queue.enqueue(extract).await.expect("enqueue extract");

        let claimed = queue
            .claim_prefer_kind("extract_assertions", |_| true)
            .await
            .expect("claim preferred")
            .expect("preferred item");

        assert_eq!(claimed.base.id, extract.base.id);
        assert_eq!(claimed.kind, "extract_assertions");
    }

    #[tokio::test]
    async fn test_claim_batch_matching_fills_capacity_in_one_call() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        for index in 0..5 {
            let mut item = make_work_item("triage_source", 10 - index);
            item.base.space_id = "default".to_string();
            queue.enqueue(item).await.expect("enqueue");
        }

        let claimed = queue
            .claim_batch_matching(3, |_| true)
            .await
            .expect("claim batch");
        assert_eq!(claimed.len(), 3);
        for item in &claimed {
            assert_eq!(item.status, WorkItemStatus::Running);
            assert!(item.leased_by.is_some());
        }
        // Highest priority items claimed first.
        assert_eq!(claimed[0].priority, 10);
        assert_eq!(claimed[2].priority, 8);

        let rest = queue
            .claim_batch_matching(10, |_| true)
            .await
            .expect("claim rest");
        assert_eq!(rest.len(), 2);

        let empty = queue
            .claim_batch_matching(10, |_| true)
            .await
            .expect("claim empty");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_claim_batch_prefer_kind_respects_preferred_lane_quota() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        for _ in 0..3 {
            let mut extract = make_work_item("extract_assertions", 100);
            extract.base.space_id = "default".to_string();
            queue.enqueue(extract).await.expect("enqueue extract");
        }
        for _ in 0..3 {
            let mut triage = make_work_item("triage_source", 10);
            triage.base.space_id = "default".to_string();
            queue.enqueue(triage).await.expect("enqueue triage");
        }

        let claimed = queue
            .claim_batch_prefer_kind("extract_assertions", 1, 4, |_| true)
            .await
            .expect("claim batch");

        assert_eq!(claimed.len(), 4);
        let extract_count = claimed
            .iter()
            .filter(|item| item.kind == "extract_assertions")
            .count();
        assert_eq!(
            extract_count, 1,
            "preferred lane quota must cap extract claims even though extract outranks triage"
        );
        // The scarce lane is filled first.
        assert_eq!(claimed[0].kind, "extract_assertions");
    }

    #[tokio::test]
    async fn test_claim_batch_prefer_kind_zero_quota_skips_kind_entirely() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);

        let mut extract = make_work_item("extract_assertions", 100);
        extract.base.space_id = "default".to_string();
        queue.enqueue(extract).await.expect("enqueue extract");

        let mut triage = make_work_item("triage_source", 10);
        triage.base.space_id = "default".to_string();
        let triage = queue.enqueue(triage).await.expect("enqueue triage");

        let claimed = queue
            .claim_batch_prefer_kind("extract_assertions", 0, 4, |_| true)
            .await
            .expect("claim batch");

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].base.id, triage.base.id);
        assert_eq!(claimed[0].kind, "triage_source");
    }

    #[tokio::test]
    async fn test_scan_limit_override_caps_candidate_window() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let config = WorkQueueConfig {
            scan_limit: Some(2),
            ..WorkQueueConfig::default()
        };
        let queue = WorkQueue::new(storage, config);

        for _ in 0..5 {
            let mut item = make_work_item("triage_source", 0);
            item.base.space_id = "default".to_string();
            queue.enqueue(item).await.expect("enqueue");
        }

        assert_eq!(
            queue.count_runnable().await.expect("exact queue depth"),
            5,
            "queue depth must not be capped by the two-item claim window"
        );

        // Only the scan window is capped, not correctness: each round claims
        // up to the window, and repeated rounds drain the queue.
        let first = queue
            .claim_batch_matching(10, |_| true)
            .await
            .expect("claim batch");
        assert_eq!(first.len(), 2);

        let mut total = first.len();
        while total < 5 {
            let next = queue
                .claim_batch_matching(10, |_| true)
                .await
                .expect("claim batch");
            assert!(!next.is_empty(), "remaining items must stay claimable");
            total += next.len();
        }
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn test_successor_stage_items_claim_before_same_priority_roots() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // A tight scan window on purpose: candidates are sorted by priority
        // before the window applies, so batched claims see high-priority
        // successors first even when the window is smaller than the queue.
        let config = WorkQueueConfig {
            scan_limit: Some(2),
            ..WorkQueueConfig::default()
        };
        let queue = WorkQueue::new(storage, config);

        // Two new-arrival roots at the base priority, enqueued first (older).
        let mut old_root = make_work_item("triage_source", 5);
        old_root.base.space_id = "default".to_string();
        let old_root = queue.enqueue(old_root).await.expect("enqueue old root");
        let mut new_root = make_work_item("triage_source", 5);
        new_root.base.space_id = "default".to_string();
        queue.enqueue(new_root).await.expect("enqueue new root");

        // A successor-stage item created last (youngest) at parent priority + 1.
        let mut successor = make_work_item("ingest_metadata", 6);
        successor.base.space_id = "default".to_string();
        let successor = queue.enqueue(successor).await.expect("enqueue successor");

        let claimed = queue
            .claim_batch_matching(2, |_| true)
            .await
            .expect("claim batch");
        assert_eq!(claimed.len(), 2);
        assert_eq!(
            claimed[0].base.id, successor.base.id,
            "the in-flight document's next stage goes first despite being youngest"
        );
        assert_eq!(claimed[1].base.id, old_root.base.id);
    }

    #[tokio::test]
    async fn test_concurrent_batch_claims_never_double_claim() {
        // Uses the embedded (elephant-db) backend: its transactional mutate is
        // the claim path in embedded all-in-one deployments. The surreal mem
        // backend interleaves concurrent single-statement UPDATEs in-process
        // and cannot make this guarantee under tokio::join! (pre-existing).
        let storage = StorageClient::new_embedded().await.expect("storage");
        let config1 = WorkQueueConfig {
            worker_id: "worker-1".to_string(),
            ..Default::default()
        };
        let config2 = WorkQueueConfig {
            worker_id: "worker-2".to_string(),
            ..Default::default()
        };
        let queue1 = WorkQueue::new(storage.clone(), config1);
        let queue2 = WorkQueue::new(storage.clone(), config2);

        for _ in 0..10 {
            queue1
                .enqueue(make_work_item("contested", 10))
                .await
                .expect("enqueue");
        }

        let (claim1, claim2) = tokio::join!(
            queue1.claim_batch_matching(10, |_| true),
            queue2.claim_batch_matching(10, |_| true),
        );
        let claim1 = claim1.expect("claim1");
        let claim2 = claim2.expect("claim2");

        let mut seen = std::collections::HashSet::new();
        for item in claim1.iter().chain(claim2.iter()) {
            assert!(
                seen.insert(item.base.id.clone()),
                "work item {} claimed by both workers",
                item.base.id
            );
        }
        assert_eq!(claim1.len() + claim2.len(), 10);
    }

    #[tokio::test]
    async fn test_concurrent_claims_resolve() {
        let storage = StorageClient::new_memory().await.expect("storage");

        // Create two queues with different worker IDs
        let config1 = WorkQueueConfig {
            worker_id: "worker-1".to_string(),
            ..Default::default()
        };
        let config2 = WorkQueueConfig {
            worker_id: "worker-2".to_string(),
            ..Default::default()
        };
        let queue1 = WorkQueue::new(storage.clone(), config1);
        let queue2 = WorkQueue::new(storage.clone(), config2);

        // Enqueue a single item
        let item = make_work_item("contested", 10);
        queue1.enqueue(item).await.expect("enqueue");

        // Both try to claim - only one should succeed
        let claim1 = queue1.claim().await.expect("claim1");
        let claim2 = queue2.claim().await.expect("claim2");

        // Exactly one should have claimed
        assert_eq!([claim1, claim2].into_iter().flatten().count(), 1);
    }
}
