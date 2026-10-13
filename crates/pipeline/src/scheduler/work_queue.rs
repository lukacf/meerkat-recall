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

/// Default lease duration in seconds.
const DEFAULT_LEASE_DURATION_SECS: i64 = 300;
const SELECTOR_SCAN_MULTIPLIER: usize = 20;
const SELECTOR_SCAN_MIN_LIMIT: usize = 10_000;

fn lease_deadline(now: DateTime<Utc>, seconds: i64) -> Result<DateTime<Utc>, PipelineError> {
    let invalid =
        || PipelineError::ConfigError("Lease duration must be positive and fit a timestamp".into());
    if seconds <= 0 {
        return Err(invalid());
    }
    let duration = Duration::try_seconds(seconds).ok_or_else(invalid)?;
    now.checked_add_signed(duration).ok_or_else(invalid)
}

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
        let lease_expires = lease_deadline(now, self.config.lease_duration_secs)?;

        // Recovery is independent of runnable candidates and the request space.
        // A quiet named space can contain only an expired Running item.
        self.recover_all_expired_leases().await?;
        let candidates = self.find_runnable(&now).await?;

        let mut claimed = Vec::new();
        for candidate in candidates {
            if claimed.len() >= max_total {
                break;
            }
            if !selector(&candidate) {
                continue;
            }
            match self.try_claim(&candidate, &lease_expires).await {
                Ok(item) => claimed.push(item),
                Err(PipelineError::LeaseConflict(_)) => {}
                Err(error) => return Err(error),
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
        let lease_expires = lease_deadline(now, self.config.lease_duration_secs)?;
        let preferred_quota = max_preferred.min(max_total);

        let mut claimed: Vec<WorkItem> = Vec::new();
        let mut claimed_ids: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
        self.recover_all_expired_leases().await?;

        // Phase 1: fill the scarce preferred lane from a cheap by-kind scan.
        if preferred_quota > 0 {
            let scan_limit = preferred_quota.max(self.config.batch_size);
            let preferred = self
                .repo
                .find_runnable_by_kind_all_spaces(preferred_kind, scan_limit)
                .await
                .map_err(PipelineError::StorageError)?;

            for candidate in preferred {
                if claimed.len() >= preferred_quota {
                    break;
                }
                if !fallback_selector(&candidate) {
                    continue;
                }
                match self.try_claim(&candidate, &lease_expires).await {
                    Ok(item) => {
                        claimed_ids.insert(item.base.id.clone());
                        claimed.push(item);
                    }
                    Err(PipelineError::LeaseConflict(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        // Phase 2: fill remaining capacity from the general queue in priority
        // order, respecting the preferred lane's remaining quota.
        if claimed.len() < max_total {
            let mut preferred_claimed = claimed.len();
            let candidates = self.find_runnable(&now).await?;

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
                match self.try_claim(&candidate, &lease_expires).await {
                    Ok(item) => {
                        if item.kind == preferred_kind {
                            preferred_claimed += 1;
                        }
                        claimed_ids.insert(item.base.id.clone());
                        claimed.push(item);
                    }
                    Err(PipelineError::LeaseConflict(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        Ok(claimed)
    }

    /// Recovers expired or malformed Running leases across every durable space.
    /// Errors propagate so a failed sweep never looks like an empty queue.
    pub async fn recover_all_expired_leases(&self) -> Result<usize, PipelineError> {
        self.repo
            .recover_expired_leases_all_spaces()
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Binds mutations to the unique owner of a claimed attempt. Never create
    /// this handle by rereading a work item after execution: keep the claim.
    pub fn for_attempt(&self, item: &WorkItem) -> Result<Self, PipelineError> {
        if item.status != WorkItemStatus::Running {
            return Err(PipelineError::LeaseConflict(item.base.id.to_string()));
        }
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(item)
            .map_err(PipelineError::StorageError)?;
        let owner = &fence.lease_token;
        let mut config = self.config.clone();
        config.worker_id.clone_from(owner);
        Ok(Self::new(self.storage.clone(), config))
    }

    /// Unique lease owner of this attempt-scoped queue handle.
    #[must_use]
    pub fn lease_owner(&self) -> &str {
        &self.config.worker_id
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

        // Every claim gets a fresh owner, including a retry in the same process.
        // The retry counter may reset administratively; this fence never repeats.
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
            .requeue_with_delay(space_id, id, &self.config.worker_id, 0, None)
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
            PipelineError::ConfigError(format!(
                "Invalid lease duration {lease_duration_secs}: {error}"
            ))
        })?;
        let expires = lease_deadline(Utc::now(), lease_seconds)?;
        self.repo
            .renew_lease(space_id, id, &self.config.worker_id, expires)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::LeaseConflict(id.to_string()))
    }

    /// Marks a work item as completed successfully.
    ///
    /// Requires this exact claim to still own an unexpired lease. A stale
    /// attempt cannot complete a newer attempt or promote its successors.
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

        Err(PipelineError::LeaseConflict(id.to_string()))
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

    /// Finish the exact public caller-authorized work snapshot using this
    /// handle's originally supplied lease token, then reconcile durable usage.
    ///
    /// # Errors
    /// Returns a lease conflict for replaced authority or a lost original claim.
    pub async fn finish_authorized(
        &self,
        observed: &WorkItem,
        success: bool,
        error: Option<String>,
    ) -> Result<WorkItem, PipelineError> {
        let status = if success {
            WorkItemStatus::Done
        } else {
            WorkItemStatus::Failed
        };
        let result = self
            .repo
            .finish_authorized(observed, &self.config.worker_id, status, error)
            .await?
            .ok_or_else(|| PipelineError::LeaseConflict(observed.base.id.to_string()))?;
        self.reconcile_cost_if_needed(&result, success).await?;
        Ok(result)
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

        if ledger.total_runs > 0 {
            return Ok(());
        }

        let Some((actual_cost, actual_tokens)) = self.estimate_actuals(item).await? else {
            return Ok(());
        };

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

    async fn estimate_actuals(&self, item: &WorkItem) -> Result<Option<(f64, u64)>, PipelineError> {
        let runs = WorkRunRepository::new(self.storage.clone())
            .find_by_work_item(&item.base.id)
            .await
            .map_err(PipelineError::StorageError)?;
        let latest = runs
            .iter()
            .filter(|run| {
                run.base.space_id == item.base.space_id
                    && (item.payload.get("_attempt_fence").is_none()
                        || run.metrics.custom.get("attempt_fence")
                            == item.payload.get("_attempt_fence"))
            })
            .max_by_key(|run| run.started_at);
        let Some(run) = latest.filter(|run| run.completed_at.is_some()) else {
            return Ok(None);
        };
        let Some(usage) =
            crate::scheduler::execution_contract::AttemptUsage::from_metrics(&run.metrics)
                .map_err(|error| PipelineError::ValidationError(error.to_string()))?
        else {
            return Ok(None);
        };
        let Some(captured) = run.metrics.custom.get("attempt_fence") else {
            return Ok(None);
        };
        let attempt: elephant_types::publication::AttemptFence =
            serde_json::from_value(captured.clone())
                .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
        if attempt.space_id != item.base.space_id || attempt.work_item_id != item.base.id {
            return Err(PipelineError::InvalidState(
                "Accounting attempt identity mismatch".into(),
            ));
        }
        let usage = crate::provider_usage::DurableUsageObserver::for_attempt(
            self.storage.clone(),
            &run.base.id,
            attempt,
        )
        .summarize(usage)
        .await?;
        if matches!(
            usage.basis,
            crate::scheduler::execution_contract::UsageBasis::Unknown
                | crate::scheduler::execution_contract::UsageBasis::Estimated
        ) {
            return Ok(None);
        }
        Ok(Some((
            usage.accounted_units(),
            usage.measured_tokens.unwrap_or(0),
        )))
    }

    async fn resolve_cost_ledger_id(
        &self,
        item: &WorkItem,
    ) -> Result<Option<RecordId>, elephant_storage::error::StorageError> {
        let runs = WorkRunRepository::new(self.storage.clone())
            .find_by_work_item(&item.base.id)
            .await?;
        for run in runs.into_iter().filter(|run| {
            run.base.space_id == item.base.space_id
                && run.metrics.custom.get("attempt_fence") == item.payload.get("_attempt_fence")
        }) {
            if let Some(value) = run
                .metrics
                .custom
                .get("cost_ledger_id")
                .and_then(serde_json::Value::as_str)
            {
                return RecordId::from_str(value).map(Some).map_err(|error| {
                    elephant_storage::StorageError::DataIntegrityError(error.to_string())
                });
            }
        }

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
                if lease_expires
                    .checked_add_signed(grace_period)
                    .is_some_and(|deadline| deadline < now)
                {
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
    async fn invalid_lease_deadlines_never_claim_or_replace_an_existing_lease() {
        use crate::scheduler::execution_contract::{classify_failure, FailureClass};
        for storage in [
            StorageClient::new_embedded().await.expect("embedded"),
            StorageClient::new_memory().await.expect("native"),
        ] {
            let queue = WorkQueue::with_defaults(storage.clone());
            let item = queue
                .enqueue(make_work_item("deadline", 1))
                .await
                .expect("enqueue");
            for seconds in [-1, 0, i64::MAX, 100_000_000_000_000] {
                let invalid = WorkQueue::new(
                    storage.clone(),
                    WorkQueueConfig {
                        lease_duration_secs: seconds,
                        ..Default::default()
                    },
                );
                let error = invalid
                    .claim()
                    .await
                    .expect_err("unrepresentable claim deadline");
                assert_eq!(
                    classify_failure(&error.into()),
                    FailureClass::ConfigurationBlocked
                );
                assert!(invalid
                    .claim_prefer_kind("deadline", |_| true)
                    .await
                    .is_err());
                assert_eq!(
                    queue
                        .repository()
                        .get(TEST_SPACE, &item.base.id)
                        .await
                        .expect("read"),
                    Some(item.clone())
                );
            }
            let original = queue.claim().await.expect("valid claim").expect("original");
            let owner = queue.for_attempt(&original).expect("owner");
            for seconds in [0, u64::MAX, 100_000_000_000_000] {
                let error = owner
                    .renew_lease(TEST_SPACE, &item.base.id, seconds)
                    .await
                    .expect_err("unrepresentable renewal");
                assert_eq!(
                    classify_failure(&error.into()),
                    FailureClass::ConfigurationBlocked
                );
                assert_eq!(
                    queue
                        .repository()
                        .get(TEST_SPACE, &item.base.id)
                        .await
                        .expect("read"),
                    Some(original.clone())
                );
            }
            assert!(owner
                .renew_lease(TEST_SPACE, &item.base.id, 300)
                .await
                .is_ok());
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
        let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)
            .expect("original fence");
        assert!(fence.lease_token.starts_with("worker-1:"));
        queue
            .repository()
            .renew_lease(
                TEST_SPACE,
                &claimed.base.id,
                &fence.lease_token,
                Utc::now() + Duration::milliseconds(40),
            )
            .await
            .expect("owner short renewal")
            .expect("owned");
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

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
        assert!(reclaimed
            .leased_by
            .as_deref()
            .is_some_and(|owner| owner.starts_with("worker-2:")));
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
            .for_attempt(&claimed)
            .expect("attempt handle")
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
            "cost_ledger_id": RecordId::new_v4(TableName::CostLedgerDaily).to_string()
        }); // Original WorkRun's real ledger must win over this older reference.
        let enqueued = queue.enqueue(item).await.expect("enqueue");

        let claimed = queue.claim().await.expect("claim").unwrap();
        let queue = queue.for_attempt(&claimed).expect("attempt handle");
        let run_id = RecordId::new_v4(TableName::WorkRun);
        let mut metrics = elephant_types::records::work::WorkRunMetrics::default();
        crate::scheduler::execution_contract::AttemptUsage::reserved(1000, 3.0)
            .write_metrics(&mut metrics)
            .expect("no-dispatch evidence");
        metrics.custom.insert(
            "attempt_fence".into(),
            claimed.payload["_attempt_fence"].clone(),
        );
        metrics.custom.insert(
            "cost_ledger_id".into(),
            serde_json::json!(ledger_id.to_string()),
        );
        WorkRunRepository::new(storage.clone())
            .create(elephant_types::records::work::WorkRun {
                base: BaseFields::new(run_id, TEST_SPACE.into(), "fixture".into()),
                work_item: Link::new(claimed.base.id.clone()),
                attempt: claimed.attempts,
                started_at: Utc::now(),
                completed_at: Some(Utc::now()),
                success: true,
                error: None,
                metrics,
                worker_id: "fixture".into(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("closed no-dispatch attempt");
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
        assert_eq!(
            updated.reconciled_euros, 0.0,
            "reservation is not measured usage"
        );
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

        let claimed = queue.claim().await.expect("claim").unwrap();
        let queue = queue.for_attempt(&claimed).expect("attempt handle");
        queue
            .complete(TEST_SPACE, &enqueued.base.id)
            .await
            .expect("complete");

        let updated = ledger_repo
            .get(TEST_SPACE, &ledger_id)
            .await
            .expect("get ledger")
            .expect("ledger");

        assert_eq!(
            updated.reserved_euros, 4.0,
            "missing execution evidence retains reservation"
        );
        assert_eq!(
            updated.reconciled_euros, 0.0,
            "reservation is not measured usage"
        );
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

        let claimed = queue.claim().await.expect("claim").unwrap();
        let queue = queue.for_attempt(&claimed).expect("attempt handle");
        queue
            .fail(TEST_SPACE, &enqueued.base.id, "boom".to_string())
            .await
            .expect("fail");

        let updated = ledger_repo
            .get(TEST_SPACE, &ledger_id)
            .await
            .expect("get ledger")
            .expect("ledger");

        assert_eq!(
            updated.reserved_euros, 2.0,
            "missing execution evidence retains reservation"
        );
        assert_eq!(updated.reconciled_euros, 0.0, "unknown usage is not billed");
        assert_eq!(updated.total_euros, 0.0);
        assert_eq!(
            updated.total_failures, 0,
            "no accounting settlement was fabricated"
        );
    }

    /// Reservations describe intent, never evidence of provider consumption.
    #[tokio::test]
    async fn test_planned_tokens_never_become_measured_consumption() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let queue = WorkQueue::with_defaults(storage);
        let mut item = make_work_item("test", 10);
        item.payload = serde_json::json!({ "estimated_tokens": 50_000 });
        for status in [WorkItemStatus::Done, WorkItemStatus::Failed] {
            item.status = status;
            let actuals = queue.estimate_actuals(&item).await.expect("read receipts");
            assert_eq!(
                actuals, None,
                "missing execution evidence is unknown, not measured zero"
            );
        }
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
    async fn all_space_attempt_contract(storage: StorageClient) {
        use elephant_storage::work_attempt_repository::fence_from_claim;
        let queue = WorkQueue::with_defaults(storage.clone());
        let mut orphan = make_work_item("quiet_work", 1);
        orphan.base.space_id = "quiet_named_space".to_string();
        orphan.status = WorkItemStatus::Running;
        orphan.leased_by = Some("crashed_process".to_string());
        orphan.lease_expires_at = Some(Utc::now() - Duration::minutes(2));
        let orphan = queue.enqueue(orphan).await.expect("orphan");
        // The only row in this database is Running in a non-default space.
        let first = queue
            .claim_prefer_kind("quiet_work", |_| true)
            .await
            .expect("recover")
            .expect("first claim");
        assert_eq!(first.base.id, orphan.base.id);
        let first_fence = fence_from_claim(&first).expect("fence");
        let first_owner = queue.for_attempt(&first).expect("first owner");
        queue
            .repository()
            .renew_lease(
                &first.base.space_id,
                &first.base.id,
                &first_fence.lease_token,
                Utc::now() + Duration::milliseconds(40),
            )
            .await
            .expect("owner short renewal")
            .expect("owned");
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let second = queue
            .claim()
            .await
            .expect("recover second")
            .expect("second claim");
        let second_fence = fence_from_claim(&second).expect("second fence");
        assert!(second_fence.generation > first_fence.generation);
        assert_ne!(second_fence.lease_token, first_fence.lease_token);
        assert!(first_owner
            .complete(&first.base.space_id, &first.base.id)
            .await
            .is_err());
        assert!(first_owner
            .renew_lease(&first.base.space_id, &first.base.id, 60)
            .await
            .is_err());
        assert!(first_owner
            .fail(
                &first.base.space_id,
                &first.base.id,
                "late failure".to_string()
            )
            .await
            .is_err());
        let second_owner = queue.for_attempt(&second).expect("second owner");
        let failed = second_owner
            .fail(
                &second.base.space_id,
                &second.base.id,
                "fixture".to_string(),
            )
            .await
            .expect("fail current");
        // Archive/retry and an administrative retry-counter reset cannot reuse
        // the durable generation or revive either previous token.
        let archived = elephant_storage::work_retention::archive_terminal_work(
            &storage,
            Utc::now() + Duration::seconds(1),
            10,
        )
        .await
        .expect("archive");
        assert_eq!(archived.items_archived, 1);
        let mut retry = failed.clone();
        retry.status = WorkItemStatus::Runnable;
        retry.attempts = 0;
        queue
            .repository()
            .update(&retry.base.space_id.clone(), &retry.base.id.clone(), retry)
            .await
            .expect("restore retry");
        let third = queue.claim().await.expect("claim third").expect("third");
        let third_fence = fence_from_claim(&third).expect("third fence");
        assert!(third_fence.generation > second_fence.generation);
        assert_eq!(third.attempts, 1);
        assert!(second_owner
            .complete(&third.base.space_id, &third.base.id)
            .await
            .is_err());
        assert!(queue.claim().await.expect("no duplicate").is_none());
        queue
            .for_attempt(&third)
            .expect("third owner")
            .complete(&third.base.space_id, &third.base.id)
            .await
            .expect("complete latest");
    }

    #[tokio::test]
    async fn embedded_quiet_space_and_attempt_fencing_survive_archive_retry() {
        all_space_attempt_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }

    #[tokio::test]
    async fn surreal_quiet_space_and_attempt_fencing_survive_archive_retry() {
        all_space_attempt_contract(StorageClient::new_memory().await.expect("surreal")).await;
    }

    #[tokio::test]
    async fn configuration_blockage_preserves_execution_retry_allowance() {
        let storage = StorageClient::new_embedded().await.expect("store");
        let queue = WorkQueue::with_defaults(storage);
        queue
            .enqueue(make_work_item("configured_work", 1))
            .await
            .expect("enqueue");
        let claim = queue.claim().await.expect("claim").expect("work");
        let fence =
            elephant_storage::work_attempt_repository::fence_from_claim(&claim).expect("fence");
        let blocked = queue
            .repository()
            .requeue_configuration_blocked(
                &claim.base.space_id,
                &claim.base.id,
                &fence.lease_token,
                30,
                "binding revoked".to_string(),
            )
            .await
            .expect("block")
            .expect("owned");
        assert_eq!(blocked.attempts, 0);
        assert!(blocked.run_after.is_some());
        assert!(blocked
            .last_error
            .expect("reason")
            .starts_with("configuration_blocked:"));
        assert!(queue.claim().await.expect("not due").is_none());
    }
}
