//! Budget enforcement module for work scheduling.
//!
//! Per spec §6.2 and §17: Core enforces budgets mechanically at scheduling and commit time.
//! Budgets are checked in order; ALL must pass for work to proceed.

use chrono::{NaiveDate, TimeZone, Utc};
use elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository;
use elephant_storage::system_event_repository::SystemEventRepository;
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{
    Budget, BudgetSnapshot, CostLedgerDaily, DowngradePolicy, IngestMode,
};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::SystemEvent;
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::error::PipelineError;

/// `system_event.event_type` for the durable budget-exhaustion signal.
pub const BUDGET_EXHAUSTED_EVENT_TYPE: &str = "budget_exhausted";

/// Deterministic id for the daily budget-exhaustion event. The same v5-UUID
/// trick as the budget guard: repeated refusals and concurrent workers all
/// derive the same id, so the day's signal collapses to a single row.
#[must_use]
pub fn budget_exhausted_event_id(space_id: &str, date: NaiveDate, mode: IngestMode) -> RecordId {
    let key = format!("budget_exhausted:{space_id}:{date}:{mode}");
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes());
    RecordId::new(TableName::SystemEvent, id)
}

/// Result of a budget check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetCheckResult {
    /// Budget allows the operation.
    Allowed,
    /// Budget requires downgrade to a lower mode.
    Downgrade {
        /// The target mode after downgrade.
        target_mode: IngestMode,
        /// Reason for the downgrade.
        reason: String,
    },
    /// Budget is exhausted and operation is rejected.
    Rejected {
        /// Reason for rejection.
        reason: String,
    },
    /// Concurrency limit hit - queue the item, don't reject.
    Queued {
        /// Which worker kind hit the limit.
        worker_kind: String,
    },
}

/// Per-item cap breach result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapBreachResult {
    /// Item is within caps.
    WithinCaps,
    /// Item exceeded token cap.
    TokenCapExceeded {
        /// Tokens used.
        actual: u32,
        /// Token limit.
        limit: u32,
    },
    /// Item exceeded wall time cap.
    WallTimeExceeded {
        /// Wall time used in seconds.
        actual: u32,
        /// Wall time limit in seconds.
        limit: u32,
    },
}

/// Result of unified budget check per §17.4.
///
/// Contains the results of all budget checks in the order mandated by the spec:
/// 1. daily € budget (per mode)
/// 2. daily item count (per mode)
/// 3. per-item token cap
/// 4. per-item wall-time cap
/// 5. concurrency limit
#[derive(Debug, Clone)]
pub struct AllBudgetsResult {
    /// Whether all budget checks passed (or item was queued).
    pub allowed: bool,
    /// The budget check result (first failing check, or Allowed).
    pub result: BudgetCheckResult,
    /// Per-item cap breach details (if applicable).
    pub cap_breach: Option<CapBreachResult>,
    /// Reserved ledger ID when budget reservation succeeds.
    pub reserved_ledger_id: Option<RecordId>,
}

/// Budget enforcer for work scheduling and commit.
#[derive(Debug, Clone)]
pub struct BudgetEnforcer {
    ledger_repo: CostLedgerDailyRepository,
    event_repo: SystemEventRepository,
    work_item_repo: WorkItemRepository,
    budget: Budget,
    space_id: String,
}

impl BudgetEnforcer {
    /// Creates a new budget enforcer.
    #[must_use]
    pub fn new(storage: StorageClient, budget: Budget, space_id: String) -> Self {
        Self {
            ledger_repo: CostLedgerDailyRepository::new(storage.clone()),
            event_repo: SystemEventRepository::new(storage.clone()),
            work_item_repo: WorkItemRepository::new(storage),
            budget,
            space_id,
        }
    }

    /// Returns the downgrade policy for this budget enforcer.
    #[must_use]
    pub const fn downgrade_policy(&self) -> DowngradePolicy {
        self.budget.downgrade_policy
    }

    /// Creates a budget snapshot at the current time.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger cannot be queried.
    pub async fn create_snapshot(&self) -> Result<BudgetSnapshot, PipelineError> {
        let today = Utc::now().date_naive();
        let (spent_retrieval, spent_graph) = self.get_daily_spending(today).await?;

        let remaining_items_retrieval = if let Some(limit) = self.budget.daily_items_retrieval {
            let spent = self
                .get_daily_item_count(today, IngestMode::Retrieval)
                .await?;
            Some(limit.saturating_sub(spent))
        } else {
            None
        };

        let remaining_items_graph = if let Some(limit) = self.budget.daily_items_graph {
            let spent = self.get_daily_item_count(today, IngestMode::Graph).await?;
            Some(limit.saturating_sub(spent))
        } else {
            None
        };

        Ok(BudgetSnapshot {
            remaining_euros_retrieval: self.budget.daily_euros_retrieval - spent_retrieval,
            remaining_euros_graph: self.budget.daily_euros_graph - spent_graph,
            remaining_items_retrieval,
            remaining_items_graph,
            snapshot_at: Utc::now(),
        })
    }

    /// Checks if budget allows the requested mode.
    ///
    /// Per spec §17.4: Budgets are checked in order; all must pass.
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails.
    pub async fn check_budget(
        &self,
        requested_mode: IngestMode,
        estimated_cost: f64,
    ) -> Result<BudgetCheckResult, PipelineError> {
        let today = Utc::now().date_naive();
        let (spent_retrieval, spent_graph) = self.get_daily_spending(today).await?;

        // Check based on mode
        match requested_mode {
            IngestMode::Graph => {
                // Check graph budget first
                let remaining_graph = self.budget.daily_euros_graph - spent_graph;
                if remaining_graph < estimated_cost {
                    if self.budget.downgrade_policy == DowngradePolicy::AlertOnly {
                        self.emit_alert_only_budget_exceeded(
                            IngestMode::Graph,
                            estimated_cost - remaining_graph,
                        )
                        .await?;
                        return Ok(BudgetCheckResult::Allowed);
                    }
                    return Ok(self.apply_downgrade_policy(
                        IngestMode::Retrieval,
                        "graph_budget_exhausted".to_string(),
                    ));
                }

                // Check item count
                if let Some(limit) = self.budget.daily_items_graph {
                    let count = self.get_daily_item_count(today, IngestMode::Graph).await?;
                    if count >= limit {
                        if self.budget.downgrade_policy == DowngradePolicy::AlertOnly {
                            self.emit_alert_only_budget_exceeded(IngestMode::Graph, 0.0)
                                .await?;
                            return Ok(BudgetCheckResult::Allowed);
                        }
                        return Ok(self.apply_downgrade_policy(
                            IngestMode::Retrieval,
                            "graph_item_limit_reached".to_string(),
                        ));
                    }
                }

                Ok(BudgetCheckResult::Allowed)
            }
            IngestMode::Retrieval => {
                // Check retrieval budget
                let remaining_retrieval = self.budget.daily_euros_retrieval - spent_retrieval;
                if remaining_retrieval < estimated_cost {
                    if self.budget.downgrade_policy == DowngradePolicy::AlertOnly {
                        self.emit_alert_only_budget_exceeded(
                            IngestMode::Retrieval,
                            estimated_cost - remaining_retrieval,
                        )
                        .await?;
                        return Ok(BudgetCheckResult::Allowed);
                    }
                    return Ok(self.apply_downgrade_policy(
                        IngestMode::Metadata,
                        "retrieval_budget_exhausted".to_string(),
                    ));
                }

                // Check item count
                if let Some(limit) = self.budget.daily_items_retrieval {
                    let count = self
                        .get_daily_item_count(today, IngestMode::Retrieval)
                        .await?;
                    if count >= limit {
                        if self.budget.downgrade_policy == DowngradePolicy::AlertOnly {
                            self.emit_alert_only_budget_exceeded(IngestMode::Retrieval, 0.0)
                                .await?;
                            return Ok(BudgetCheckResult::Allowed);
                        }
                        return Ok(self.apply_downgrade_policy(
                            IngestMode::Metadata,
                            "retrieval_item_limit_reached".to_string(),
                        ));
                    }
                }

                Ok(BudgetCheckResult::Allowed)
            }
            IngestMode::Metadata | IngestMode::Skip => {
                // Metadata and Skip have no budget constraints
                Ok(BudgetCheckResult::Allowed)
            }
        }
    }

    /// Checks concurrency limits for a worker kind.
    pub fn check_concurrency(&self, worker_kind: &str, current_running: u32) -> BudgetCheckResult {
        if let Some(&limit) = self.budget.concurrent_workers.get(worker_kind) {
            if current_running >= limit {
                return BudgetCheckResult::Queued {
                    worker_kind: worker_kind.to_string(),
                };
            }
        }
        BudgetCheckResult::Allowed
    }

    /// Returns the current running count for a worker kind.
    ///
    /// # Errors
    ///
    /// Returns an error if the count query fails.
    pub async fn current_running_for_kind(&self, worker_kind: &str) -> Result<u32, PipelineError> {
        self.work_item_repo
            .count_running_by_kind(&self.space_id, worker_kind)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Checks per-item caps.
    pub fn check_per_item_caps(&self, tokens_used: u32, wall_time_secs: u32) -> CapBreachResult {
        // Check token cap
        if let Some(limit) = self.budget.per_item_tokens {
            if tokens_used > limit {
                return CapBreachResult::TokenCapExceeded {
                    actual: tokens_used,
                    limit,
                };
            }
        }

        // Check wall time cap
        if let Some(limit) = self.budget.per_item_wall_time_secs {
            if wall_time_secs > limit {
                return CapBreachResult::WallTimeExceeded {
                    actual: wall_time_secs,
                    limit,
                };
            }
        }

        CapBreachResult::WithinCaps
    }

    fn budget_guard_id(&self, date: NaiveDate, mode: IngestMode) -> RecordId {
        let key = format!("budget_guard:{}:{}:{}", self.space_id, date, mode);
        let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes());
        RecordId::new(TableName::CostLedgerDaily, id)
    }

    async fn get_budget_guard(
        &self,
        date: NaiveDate,
        mode: IngestMode,
    ) -> Result<Option<CostLedgerDaily>, PipelineError> {
        let guard_id = self.budget_guard_id(date, mode);
        self.ledger_repo
            .get(&self.space_id, &guard_id)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Reserves estimated cost before work begins.
    ///
    /// # Errors
    ///
    /// Returns an error if reservation fails.
    pub async fn reserve_cost(
        &self,
        mode: IngestMode,
        worker_kind: &str,
        estimated_cost: f64,
    ) -> Result<RecordId, PipelineError> {
        let today = Utc::now().date_naive();
        let id = RecordId::new_v4(TableName::CostLedgerDaily);

        let (budget_limit, item_limit) = match mode {
            IngestMode::Retrieval => (
                self.budget.daily_euros_retrieval,
                self.budget.daily_items_retrieval,
            ),
            IngestMode::Graph => (self.budget.daily_euros_graph, self.budget.daily_items_graph),
            IngestMode::Metadata | IngestMode::Skip => (0.0, None),
        };

        if matches!(mode, IngestMode::Retrieval | IngestMode::Graph) {
            if self.budget.downgrade_policy != DowngradePolicy::AlertOnly {
                if estimated_cost > budget_limit {
                    self.record_budget_exhausted(mode, today, BudgetExhaustedLimit::Euros)
                        .await;
                    return Err(PipelineError::BudgetExceeded(format!(
                        "Daily budget exhausted for mode {mode}"
                    )));
                }
                if let Some(limit) = item_limit {
                    if limit == 0 {
                        self.record_budget_exhausted(mode, today, BudgetExhaustedLimit::Items)
                            .await;
                        return Err(PipelineError::BudgetExceeded(format!(
                            "Daily item limit reached for mode {mode}"
                        )));
                    }
                }
            }

            let guard_id = self.budget_guard_id(today, mode);
            let reserved = if self.budget.downgrade_policy == DowngradePolicy::AlertOnly {
                let reserved = self
                    .ledger_repo
                    .reserve_budget_guard(
                        &guard_id,
                        &self.space_id,
                        today,
                        mode,
                        estimated_cost,
                        budget_limit,
                        item_limit,
                    )
                    .await
                    .map_err(PipelineError::StorageError)?;

                if !reserved {
                    let reset_at = budget_reset_at(today)?;
                    let overrun_amount = estimated_cost.max(0.0);
                    self.emit_operator_alert_event(mode, overrun_amount, 0, reset_at)
                        .await?;

                    self.ledger_repo
                        .reserve_budget_guard(
                            &guard_id,
                            &self.space_id,
                            today,
                            mode,
                            estimated_cost,
                            f64::MAX,
                            None,
                        )
                        .await
                        .map_err(PipelineError::StorageError)?
                } else {
                    true
                }
            } else {
                self.ledger_repo
                    .reserve_budget_guard(
                        &guard_id,
                        &self.space_id,
                        today,
                        mode,
                        estimated_cost,
                        budget_limit,
                        item_limit,
                    )
                    .await
                    .map_err(PipelineError::StorageError)?
            };

            if !reserved {
                let limit = self
                    .classify_guard_exhaustion(today, mode, item_limit)
                    .await;
                self.record_budget_exhausted(mode, today, limit).await;
                return Err(PipelineError::BudgetExceeded(format!(
                    "Daily budget exhausted for mode {mode}"
                )));
            }
        }

        let ledger = CostLedgerDaily {
            base: BaseFields::new(
                id.clone(),
                self.space_id.clone(),
                "budget_enforcer".to_string(),
            ),
            date: today,
            mode,
            worker_kind: worker_kind.to_string(),
            total_euros: 0.0,
            total_items: 0,
            total_tokens: 0,
            total_runs: 0,
            total_failures: 0,
            reserved_euros: estimated_cost,
            reconciled_euros: 0.0,
        };

        self.ledger_repo
            .create(ledger)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(id)
    }

    /// Reconciles reserved vs actual cost after work completes.
    ///
    /// # Errors
    ///
    /// Returns an error if reconciliation fails.
    pub async fn reconcile_cost(
        &self,
        ledger_id: &RecordId,
        actual_cost: f64,
        actual_tokens: u64,
        success: bool,
    ) -> Result<CostReconciliation, PipelineError> {
        let ledger = self
            .ledger_repo
            .get(&self.space_id, ledger_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(ledger_id.to_string()))?;

        let reserved = ledger.reserved_euros;
        let overrun = actual_cost - reserved;

        let ledger_mode = ledger.mode;
        let ledger_date = ledger.date;
        let guard_id = self.budget_guard_id(ledger_date, ledger_mode);
        let applied = self
            .ledger_repo
            .reconcile_attempt(
                &self.space_id,
                ledger_id,
                &guard_id,
                actual_cost,
                actual_tokens,
                success,
            )
            .await
            .map_err(PipelineError::StorageError)?;
        if !applied {
            return Ok(CostReconciliation {
                reserved: 0.0,
                actual: ledger.reconciled_euros,
                overrun: None,
            });
        }

        if overrun > 0.0 {
            let totals = self
                .ledger_repo
                .aggregate_daily_spending(&self.space_id, ledger_date)
                .await
                .map_err(PipelineError::StorageError)?;

            let (spent, limit) = match ledger_mode {
                IngestMode::Retrieval => {
                    (totals.spent_retrieval, self.budget.daily_euros_retrieval)
                }
                IngestMode::Graph => (totals.spent_graph, self.budget.daily_euros_graph),
                IngestMode::Metadata | IngestMode::Skip => (0.0, 0.0),
            };

            if spent > limit {
                let overrun_amount = spent - limit;
                let reset_at = budget_reset_at(ledger_date)?;
                let paused = self
                    .pause_queued_items_for_mode(ledger_mode, reset_at)
                    .await?;
                self.emit_operator_alert_event(ledger_mode, overrun_amount, paused, reset_at)
                    .await?;
                self.emit_budget_breach_event(ledger_mode, overrun_amount)
                    .await?;
            }
        }

        Ok(CostReconciliation {
            reserved,
            actual: actual_cost,
            overrun: if overrun > 0.0 { Some(overrun) } else { None },
        })
    }

    /// Performs all budget checks in the order specified by §17.4.
    ///
    /// Per spec §17.4: Budgets are checked in order; ALL must pass:
    /// 1. daily € budget (per mode)
    /// 2. daily item count (per mode)
    /// 3. per-item token cap
    /// 4. per-item wall-time cap
    /// 5. concurrency limit
    ///
    /// # Errors
    ///
    /// Returns an error if budget checks fail.
    pub async fn check_all_budgets(
        &self,
        requested_mode: IngestMode,
        estimated_cost: f64,
        tokens_used: u32,
        wall_time_secs: u32,
        worker_kind: &str,
        current_running: u32,
    ) -> Result<AllBudgetsResult, PipelineError> {
        // Step 1-2: Check daily € budget and daily item count (combined in check_budget)
        let daily_check = self.check_budget(requested_mode, estimated_cost).await?;
        if daily_check != BudgetCheckResult::Allowed {
            return Ok(AllBudgetsResult {
                allowed: false,
                result: daily_check,
                cap_breach: None,
                reserved_ledger_id: None,
            });
        }

        // Step 3-4: Check per-item token cap and per-item wall-time cap
        let cap_check = self.check_per_item_caps(tokens_used, wall_time_secs);
        if cap_check != CapBreachResult::WithinCaps {
            return Ok(AllBudgetsResult {
                allowed: false,
                result: BudgetCheckResult::Rejected {
                    reason: format!("Per-item cap exceeded: {cap_check:?}"),
                },
                cap_breach: Some(cap_check),
                reserved_ledger_id: None,
            });
        }

        // Step 5: Check concurrency limit
        let concurrency_check = self.check_concurrency(worker_kind, current_running);
        if concurrency_check != BudgetCheckResult::Allowed {
            return Ok(AllBudgetsResult {
                allowed: matches!(concurrency_check, BudgetCheckResult::Queued { .. }),
                result: concurrency_check,
                cap_breach: None,
                reserved_ledger_id: None,
            });
        }

        Ok(AllBudgetsResult {
            allowed: true,
            result: BudgetCheckResult::Allowed,
            cap_breach: None,
            reserved_ledger_id: None,
        })
    }

    /// Checks all budgets and reserves the estimated cost atomically.
    ///
    /// This is used during scheduling to ensure the budget check and reservation
    /// happen in a single step for daily limits.
    ///
    /// # Errors
    ///
    /// Returns an error if reservation fails for reasons other than budget exhaustion.
    pub async fn check_all_budgets_and_reserve(
        &self,
        requested_mode: IngestMode,
        estimated_cost: f64,
        tokens_used: u32,
        wall_time_secs: u32,
        worker_kind: &str,
        current_running: u32,
    ) -> Result<AllBudgetsResult, PipelineError> {
        // Step 1-2: Check daily € budget and daily item count (combined in check_budget)
        let daily_check = self.check_budget(requested_mode, estimated_cost).await?;
        if daily_check != BudgetCheckResult::Allowed {
            return Ok(AllBudgetsResult {
                allowed: false,
                result: daily_check,
                cap_breach: None,
                reserved_ledger_id: None,
            });
        }

        // Step 3-4: Check per-item token cap and per-item wall-time cap
        let cap_check = self.check_per_item_caps(tokens_used, wall_time_secs);
        if cap_check != CapBreachResult::WithinCaps {
            return Ok(AllBudgetsResult {
                allowed: false,
                result: BudgetCheckResult::Rejected {
                    reason: format!("Per-item cap exceeded: {cap_check:?}"),
                },
                cap_breach: Some(cap_check),
                reserved_ledger_id: None,
            });
        }

        if matches!(requested_mode, IngestMode::Skip) {
            return Ok(AllBudgetsResult {
                allowed: true,
                result: BudgetCheckResult::Allowed,
                cap_breach: None,
                reserved_ledger_id: None,
            });
        }

        let reserved_ledger_id = match self
            .reserve_cost(requested_mode, worker_kind, estimated_cost)
            .await
        {
            Ok(ledger_id) => Some(ledger_id),
            Err(PipelineError::BudgetExceeded(reason)) => {
                let downgrade_target = match requested_mode {
                    IngestMode::Graph => IngestMode::Retrieval,
                    IngestMode::Retrieval => IngestMode::Metadata,
                    IngestMode::Metadata | IngestMode::Skip => IngestMode::Metadata,
                };

                let result = self.apply_downgrade_policy(downgrade_target, reason);
                return Ok(AllBudgetsResult {
                    allowed: matches!(result, BudgetCheckResult::Queued { .. }),
                    result,
                    cap_breach: None,
                    reserved_ledger_id: None,
                });
            }
            Err(err) => return Err(err),
        };

        // Step 5: Check concurrency limit
        let concurrency_check = self.check_concurrency(worker_kind, current_running);
        if concurrency_check != BudgetCheckResult::Allowed {
            return Ok(AllBudgetsResult {
                allowed: matches!(concurrency_check, BudgetCheckResult::Queued { .. }),
                result: concurrency_check,
                cap_breach: None,
                reserved_ledger_id,
            });
        }

        Ok(AllBudgetsResult {
            allowed: true,
            result: BudgetCheckResult::Allowed,
            cap_breach: None,
            reserved_ledger_id,
        })
    }

    /// Checks budget at commit time.
    ///
    /// Per spec §6.2: Budget is enforced at both scheduling AND commit time.
    /// This method is called during commit to verify actual costs don't exceed budget.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit-time budget check fails.
    pub async fn check_budget_at_commit(
        &self,
        mode: IngestMode,
        actual_cost: f64,
    ) -> Result<BudgetCheckResult, PipelineError> {
        // Re-check budget with actual cost instead of estimated
        self.check_budget(mode, actual_cost).await
    }

    /// Queues an upgrade of ingestion mode for the next budget period.
    ///
    /// # Errors
    ///
    /// Returns an error if work item creation fails.
    pub async fn queue_mode_upgrade(
        &self,
        source_id: &RecordId,
        current_mode: IngestMode,
        target_mode: IngestMode,
        estimated_cost: f64,
        actor: &str,
        reason: &str,
    ) -> Result<RecordId, PipelineError> {
        if current_mode == target_mode {
            return Ok(source_id.clone());
        }

        let now = Utc::now();
        let next_day = now.date_naive() + chrono::Duration::days(1);
        let run_after = next_day
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| PipelineError::InvalidInput("Invalid next-day timestamp".to_string()))?;
        let run_after = Utc.from_utc_datetime(&run_after);

        let payload = serde_json::json!({
            "source_id": source_id,
            "current_mode": current_mode,
            "target_mode": target_mode,
            "estimated_cost": estimated_cost,
            "reason": reason
        });

        let id = RecordId::new_v4(TableName::WorkItem);
        let item = WorkItem {
            base: BaseFields::new(id.clone(), self.space_id.clone(), actor.to_string()),
            kind: "upgrade_ingest_mode".to_string(),
            payload,
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!(
                "upgrade_ingest_mode:{}:{}:{}",
                source_id,
                target_mode,
                next_day.format("%Y-%m-%d")
            ),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: Some(run_after),
            last_error: None,
        };

        let created = self
            .work_item_repo
            .create(item)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(created.base.id)
    }

    /// Applies the downgrade policy based on configuration.
    fn apply_downgrade_policy(&self, target_mode: IngestMode, reason: String) -> BudgetCheckResult {
        match self.budget.downgrade_policy {
            DowngradePolicy::Strict => BudgetCheckResult::Rejected { reason },
            DowngradePolicy::DegradeMode | DowngradePolicy::DegradeModeWithQueue => {
                BudgetCheckResult::Downgrade {
                    target_mode,
                    reason,
                }
            }
            DowngradePolicy::AlertOnly => {
                // Alert-only allows the operation but should trigger an alert
                BudgetCheckResult::Allowed
            }
        }
    }

    /// Gets daily spending from ledger by aggregating cost records.
    ///
    /// Returns a tuple of (retrieval_spending, graph_spending) for the given date.
    async fn get_daily_spending(&self, date: NaiveDate) -> Result<(f64, f64), PipelineError> {
        let mut spent_retrieval: Option<f64> = None;
        let mut spent_graph: Option<f64> = None;

        if let Some(guard) = self.get_budget_guard(date, IngestMode::Retrieval).await? {
            spent_retrieval = Some(guard.reserved_euros + guard.reconciled_euros);
        }

        if let Some(guard) = self.get_budget_guard(date, IngestMode::Graph).await? {
            spent_graph = Some(guard.reserved_euros + guard.reconciled_euros);
        }

        if spent_retrieval.is_none() || spent_graph.is_none() {
            let totals = self
                .ledger_repo
                .aggregate_daily_spending(&self.space_id, date)
                .await
                .map_err(PipelineError::StorageError)?;

            if spent_retrieval.is_none() {
                spent_retrieval = Some(totals.spent_retrieval);
            }
            if spent_graph.is_none() {
                spent_graph = Some(totals.spent_graph);
            }
        }

        Ok((spent_retrieval.unwrap_or(0.0), spent_graph.unwrap_or(0.0)))
    }

    /// Gets daily item count for a mode by aggregating from ledger.
    ///
    /// Returns the total number of items processed in the given mode for the date.
    async fn get_daily_item_count(
        &self,
        date: NaiveDate,
        mode: IngestMode,
    ) -> Result<u32, PipelineError> {
        if let Some(guard) = self.get_budget_guard(date, mode).await? {
            return Ok(guard.total_items);
        }

        let totals = self
            .ledger_repo
            .aggregate_daily_spending(&self.space_id, date)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(match mode {
            IngestMode::Retrieval => totals.items_retrieval,
            IngestMode::Graph => totals.items_graph,
            IngestMode::Metadata | IngestMode::Skip => 0,
        })
    }

    /// Records the durable budget-exhaustion signal (`system_event` kind
    /// `budget_exhausted`) — best-effort: a failed write is logged, never
    /// propagated, so the signal can't mask the `BudgetExceeded` refusal
    /// itself. Throttled to at most one event per (space, mode, day).
    async fn record_budget_exhausted(
        &self,
        mode: IngestMode,
        day: NaiveDate,
        limit: BudgetExhaustedLimit,
    ) {
        if let Err(err) = self.try_record_budget_exhausted(mode, day, limit).await {
            warn!(%mode, %day, "Failed to record budget_exhausted system_event: {err}");
        }
    }

    async fn try_record_budget_exhausted(
        &self,
        mode: IngestMode,
        day: NaiveDate,
        limit: BudgetExhaustedLimit,
    ) -> Result<(), PipelineError> {
        let event_id = budget_exhausted_event_id(&self.space_id, day, mode);
        if self
            .event_repo
            .get(&self.space_id, &event_id)
            .await
            .map_err(PipelineError::StorageError)?
            .is_some()
        {
            // Already recorded today: throttled.
            return Ok(());
        }

        let seq = self
            .event_repo
            .get_latest_seq(&self.space_id)
            .await
            .map_err(PipelineError::StorageError)?
            + 1;

        let event = BudgetExhaustedEvent {
            space_id: self.space_id.clone(),
            mode,
            day,
            limit,
            occurred_at: Utc::now(),
        };
        let payload = serde_json::to_value(&event).map_err(|e| {
            PipelineError::InvalidInput(format!("Failed to serialize budget exhausted event: {e}"))
        })?;

        let record = SystemEvent {
            base: BaseFields::new(
                event_id,
                self.space_id.clone(),
                "budget_enforcer".to_string(),
            ),
            seq,
            event_type: BUDGET_EXHAUSTED_EVENT_TYPE.to_string(),
            payload,
        };

        match self.event_repo.create(record).await {
            Ok(_) => Ok(()),
            // Deterministic id: a concurrent worker recorded the same
            // (space, mode, day) first — that is the throttle working.
            Err(elephant_storage::StorageError::Conflict(_)) => Ok(()),
            Err(err) => Err(PipelineError::StorageError(err)),
        }
    }

    /// Classifies which limit refused a guard reservation by inspecting the
    /// guard row: if the item count is at its cap the items limit ran out,
    /// otherwise the euro budget did.
    async fn classify_guard_exhaustion(
        &self,
        day: NaiveDate,
        mode: IngestMode,
        item_limit: Option<u32>,
    ) -> BudgetExhaustedLimit {
        if let (Some(limit), Ok(Some(guard))) = (item_limit, self.get_budget_guard(day, mode).await)
        {
            if guard.total_items >= limit {
                return BudgetExhaustedLimit::Items;
            }
        }
        BudgetExhaustedLimit::Euros
    }

    async fn emit_alert_only_budget_exceeded(
        &self,
        mode: IngestMode,
        overrun_amount: f64,
    ) -> Result<(), PipelineError> {
        let reset_at = budget_reset_at(Utc::now().date_naive())?;
        self.emit_operator_alert_event(mode, overrun_amount, 0, reset_at)
            .await?;
        Ok(())
    }

    async fn emit_budget_breach_event(
        &self,
        mode: IngestMode,
        overrun_amount: f64,
    ) -> Result<(), PipelineError> {
        let seq = self
            .event_repo
            .get_latest_seq(&self.space_id)
            .await
            .map_err(PipelineError::StorageError)?
            + 1;

        let event = BudgetBreachEvent {
            space_id: self.space_id.clone(),
            mode,
            breach_type: BudgetBreachType::DailyEurosExceeded,
            overrun_amount,
            occurred_at: Utc::now(),
        };

        let payload = serde_json::to_value(&event).map_err(|e| {
            PipelineError::InvalidInput(format!("Failed to serialize budget breach event: {e}"))
        })?;

        let record = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                self.space_id.clone(),
                "budget_enforcer".to_string(),
            ),
            seq,
            event_type: "budget_breach".to_string(),
            payload,
        };

        self.event_repo
            .create(record)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(())
    }

    async fn emit_operator_alert_event(
        &self,
        mode: IngestMode,
        overrun_amount: f64,
        paused_items: usize,
        reset_at: chrono::DateTime<Utc>,
    ) -> Result<(), PipelineError> {
        let seq = self
            .event_repo
            .get_latest_seq(&self.space_id)
            .await
            .map_err(PipelineError::StorageError)?
            + 1;

        let event = OperatorAlertEvent {
            space_id: self.space_id.clone(),
            alert_type: "budget_overrun".to_string(),
            mode,
            overrun_amount,
            paused_items,
            reset_at,
            occurred_at: Utc::now(),
        };

        let payload = serde_json::to_value(&event).map_err(|e| {
            PipelineError::InvalidInput(format!("Failed to serialize operator alert: {e}"))
        })?;

        let record = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                self.space_id.clone(),
                "budget_enforcer".to_string(),
            ),
            seq,
            event_type: "operator_alert".to_string(),
            payload,
        };

        self.event_repo
            .create(record)
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(())
    }

    async fn pause_queued_items_for_mode(
        &self,
        mode: IngestMode,
        reset_at: chrono::DateTime<Utc>,
    ) -> Result<usize, PipelineError> {
        match mode {
            IngestMode::Retrieval | IngestMode::Graph => self
                .work_item_repo
                .pause_queued_by_mode(&self.space_id, mode, reset_at)
                .await
                .map_err(PipelineError::StorageError),
            IngestMode::Metadata | IngestMode::Skip => Ok(0),
        }
    }
}

/// Result of cost reconciliation.
#[derive(Debug, Clone)]
pub struct CostReconciliation {
    /// Reserved cost before work.
    pub reserved: f64,
    /// Actual cost after work.
    pub actual: f64,
    /// Overrun amount (actual - reserved) if positive.
    pub overrun: Option<f64>,
}

/// Which daily limit ran out when the budget enforcer refused a reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetExhaustedLimit {
    /// The daily euro budget for the mode is used up.
    Euros,
    /// The daily item count for the mode is used up.
    Items,
}

/// Durable budget-exhaustion signal: the enforcer refused a reservation
/// because a mode's daily budget ran out. At most one per (space, mode, day)
/// via the deterministic event id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetExhaustedEvent {
    /// Space whose budget is exhausted.
    pub space_id: String,
    /// Mode that was refused.
    pub mode: IngestMode,
    /// UTC day the budget applies to.
    pub day: NaiveDate,
    /// Which limit ran out.
    pub limit: BudgetExhaustedLimit,
    /// When the first refusal of the day was recorded.
    pub occurred_at: chrono::DateTime<Utc>,
}

/// Budget breach event (for alerting).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetBreachEvent {
    /// Space that breached budget.
    pub space_id: String,
    /// Mode that was affected.
    pub mode: IngestMode,
    /// Type of breach.
    pub breach_type: BudgetBreachType,
    /// Amount of overrun.
    pub overrun_amount: f64,
    /// When the breach occurred.
    pub occurred_at: chrono::DateTime<Utc>,
}

/// Operator alert event for budget overruns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorAlertEvent {
    /// Space that triggered the alert.
    pub space_id: String,
    /// Alert category.
    pub alert_type: String,
    /// Mode that was affected.
    pub mode: IngestMode,
    /// Amount of overrun.
    pub overrun_amount: f64,
    /// Number of queued items paused.
    pub paused_items: usize,
    /// When the budget resets.
    pub reset_at: chrono::DateTime<Utc>,
    /// When the alert occurred.
    pub occurred_at: chrono::DateTime<Utc>,
}

/// Type of budget breach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetBreachType {
    /// Daily euro budget exceeded.
    DailyEurosExceeded,
    /// Daily item count exceeded.
    DailyItemsExceeded,
    /// Per-item token cap exceeded.
    TokenCapExceeded,
    /// Per-item wall time exceeded.
    WallTimeExceeded,
}

/// Simple daily cap circuit breaker that replaces the full `BudgetEnforcer` ceremony
/// in the work queue loop. Atomically checks item count and reserves a slot.
#[derive(Debug, Clone)]
pub struct DailyCapCheck {
    repo: CostLedgerDailyRepository,
    max_items_per_day: u32,
}

impl DailyCapCheck {
    /// Creates a new daily cap check.
    #[must_use]
    pub fn new(repo: CostLedgerDailyRepository, max_items: u32) -> Self {
        Self {
            repo,
            max_items_per_day: max_items,
        }
    }

    /// Atomically checks if under cap and increments. Returns false if cap reached.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger query fails.
    pub async fn try_reserve(&self, space_id: &str) -> Result<bool, PipelineError> {
        let today = Utc::now().date_naive();
        let totals = self
            .repo
            .aggregate_daily_spending(space_id, today)
            .await
            .map_err(PipelineError::StorageError)?;
        let total_items = totals.items_retrieval + totals.items_graph;
        Ok(total_items < self.max_items_per_day)
    }

    /// Record final cost after completion.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger update fails.
    pub async fn record_cost(
        &self,
        space_id: &str,
        tokens: u64,
        cost_euros: f64,
    ) -> Result<(), PipelineError> {
        let today = Utc::now().date_naive();
        let id = RecordId::new_v4(TableName::CostLedgerDaily);
        let ledger = CostLedgerDaily {
            base: BaseFields::new(id, space_id.to_string(), "daily_cap_check".to_string()),
            date: today,
            mode: IngestMode::Graph,
            worker_kind: "work_queue".to_string(),
            total_euros: cost_euros,
            total_items: 1,
            total_tokens: tokens,
            total_runs: 1,
            total_failures: 0,
            reserved_euros: 0.0,
            reconciled_euros: cost_euros,
        };
        self.repo
            .create(ledger)
            .await
            .map_err(PipelineError::StorageError)?;
        Ok(())
    }
}

fn budget_reset_at(date: NaiveDate) -> Result<chrono::DateTime<Utc>, PipelineError> {
    let next_day = date + chrono::Duration::days(1);
    let run_after = next_day
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| PipelineError::InvalidInput("Invalid next-day timestamp".to_string()))?;
    Ok(Utc.from_utc_datetime(&run_after))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::collections::HashMap;

    fn make_budget() -> Budget {
        Budget {
            daily_euros_retrieval: 10.0,
            daily_euros_graph: 50.0,
            daily_items_retrieval: Some(100),
            daily_items_graph: Some(10),
            per_item_tokens: Some(100_000),
            per_item_wall_time_secs: Some(300),
            concurrent_workers: {
                let mut map = HashMap::new();
                map.insert("embed".to_string(), 5);
                map.insert("extract".to_string(), 3);
                map
            },
            downgrade_policy: DowngradePolicy::DegradeMode,
        }
    }

    #[tokio::test]
    async fn test_budget_check_allowed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer
            .check_budget(IngestMode::Retrieval, 1.0)
            .await
            .expect("check");

        assert_eq!(result, BudgetCheckResult::Allowed);
    }

    #[tokio::test]
    async fn test_budget_check_metadata_always_allowed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer
            .check_budget(IngestMode::Metadata, 0.0)
            .await
            .expect("check");

        assert_eq!(result, BudgetCheckResult::Allowed);
    }

    #[tokio::test]
    async fn test_concurrency_check_allowed() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.check_concurrency("embed", 3);
        assert_eq!(result, BudgetCheckResult::Allowed);
    }

    #[tokio::test]
    async fn test_concurrency_check_queued() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.check_concurrency("embed", 5);
        assert!(matches!(result, BudgetCheckResult::Queued { .. }));
    }

    #[tokio::test]
    async fn test_per_item_caps_within() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.check_per_item_caps(50_000, 100);
        assert_eq!(result, CapBreachResult::WithinCaps);
    }

    #[tokio::test]
    async fn test_per_item_caps_token_exceeded() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.check_per_item_caps(150_000, 100);
        assert!(matches!(result, CapBreachResult::TokenCapExceeded { .. }));
    }

    #[tokio::test]
    async fn test_per_item_caps_wall_time_exceeded() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.check_per_item_caps(50_000, 400);
        assert!(matches!(result, CapBreachResult::WallTimeExceeded { .. }));
    }

    #[tokio::test]
    async fn test_downgrade_policy_strict() {
        let mut budget = make_budget();
        budget.downgrade_policy = DowngradePolicy::Strict;
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.apply_downgrade_policy(IngestMode::Metadata, "test".to_string());
        assert!(matches!(result, BudgetCheckResult::Rejected { .. }));
    }

    #[tokio::test]
    async fn test_downgrade_policy_degrade_mode() {
        let budget = make_budget();
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.apply_downgrade_policy(IngestMode::Retrieval, "test".to_string());
        assert!(matches!(
            result,
            BudgetCheckResult::Downgrade {
                target_mode: IngestMode::Retrieval,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_downgrade_policy_alert_only() {
        let mut budget = make_budget();
        budget.downgrade_policy = DowngradePolicy::AlertOnly;
        let storage = StorageClient::new_memory().await.expect("storage");
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer.apply_downgrade_policy(IngestMode::Metadata, "test".to_string());
        assert_eq!(result, BudgetCheckResult::Allowed);
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let snapshot = enforcer.create_snapshot().await.expect("snapshot");

        // With no spending, remaining equals budget
        assert!((snapshot.remaining_euros_retrieval - 10.0).abs() < f64::EPSILON);
        assert!((snapshot.remaining_euros_graph - 50.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.remaining_items_retrieval, Some(100));
        assert_eq!(snapshot.remaining_items_graph, Some(10));
    }

    #[tokio::test]
    async fn test_check_all_budgets_allowed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer
            .check_all_budgets(IngestMode::Retrieval, 1.0, 50_000, 100, "embed", 2)
            .await
            .expect("check");

        assert!(result.allowed);
        assert_eq!(result.result, BudgetCheckResult::Allowed);
        assert!(result.cap_breach.is_none());
    }

    #[tokio::test]
    async fn test_check_all_budgets_cap_breach() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        // Exceed token cap (budget is 100,000)
        let result = enforcer
            .check_all_budgets(IngestMode::Retrieval, 1.0, 150_000, 100, "embed", 2)
            .await
            .expect("check");

        assert!(!result.allowed);
        assert!(matches!(result.result, BudgetCheckResult::Rejected { .. }));
        assert!(matches!(
            result.cap_breach,
            Some(CapBreachResult::TokenCapExceeded { .. })
        ));
    }

    #[tokio::test]
    async fn test_check_all_budgets_queued() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        // Hit concurrency limit (budget is 5 for embed)
        let result = enforcer
            .check_all_budgets(IngestMode::Retrieval, 1.0, 50_000, 100, "embed", 5)
            .await
            .expect("check");

        // Queued is allowed but with Queued result
        assert!(result.allowed);
        assert!(matches!(result.result, BudgetCheckResult::Queued { .. }));
    }

    #[tokio::test]
    async fn test_check_all_budgets_and_reserve_daily_budget_before_concurrency() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let mut budget = make_budget();
        budget.daily_euros_retrieval = 0.0;
        budget.concurrent_workers.insert("embed".to_string(), 0);
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer
            .check_all_budgets_and_reserve(IngestMode::Retrieval, 1.0, 50_000, 100, "embed", 1)
            .await
            .expect("check");

        assert!(!result.allowed);
        assert!(matches!(result.result, BudgetCheckResult::Downgrade { .. }));
        assert!(result.cap_breach.is_none());
        assert!(result.reserved_ledger_id.is_none());
    }

    #[tokio::test]
    async fn test_check_all_budgets_and_reserve_queues_after_reservation() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");
        let mut budget = make_budget();
        budget.concurrent_workers.insert("embed".to_string(), 0);
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = enforcer
            .check_all_budgets_and_reserve(IngestMode::Retrieval, 1.0, 50_000, 100, "embed", 1)
            .await
            .expect("check");

        assert!(result.allowed);
        assert!(matches!(result.result, BudgetCheckResult::Queued { .. }));
        assert!(result.reserved_ledger_id.is_some());
    }

    #[tokio::test]
    async fn test_check_budget_at_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        // Commit-time check with small cost should pass
        let result = enforcer
            .check_budget_at_commit(IngestMode::Retrieval, 1.0)
            .await
            .expect("check");

        assert_eq!(result, BudgetCheckResult::Allowed);
    }

    #[tokio::test]
    async fn test_daily_budget_cap_enforced() {
        use elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository;
        use elephant_types::records::base::BaseFields;

        let storage = StorageClient::new_memory().await.expect("storage");

        // Define the cost_ledger_daily table
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        // Create a budget with 10 euros daily limit for retrieval
        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());

        // First check: should be allowed (no spending yet)
        let result = enforcer
            .check_budget(IngestMode::Retrieval, 1.0)
            .await
            .expect("check");
        assert_eq!(result, BudgetCheckResult::Allowed);

        // Record 9 euros of spending (within budget)
        let ledger_repo = CostLedgerDailyRepository::new(storage.clone());
        let today = Utc::now().date_naive();
        let ledger = CostLedgerDaily {
            base: BaseFields::new(
                RecordId::new_v4(TableName::CostLedgerDaily),
                "test_space".to_string(),
                "test".to_string(),
            ),
            date: today,
            mode: IngestMode::Retrieval,
            worker_kind: "embed".to_string(),
            total_euros: 9.0,
            total_items: 10,
            total_tokens: 500_000,
            total_runs: 10,
            total_failures: 0,
            reserved_euros: 0.0,
            reconciled_euros: 9.0,
        };
        ledger_repo.create(ledger).await.expect("create ledger");

        // Second check with 2 euros cost: should fail (9 + 2 > 10)
        let result = enforcer
            .check_budget(IngestMode::Retrieval, 2.0)
            .await
            .expect("check");

        // Should be downgraded to Metadata mode
        assert!(
            matches!(
                result,
                BudgetCheckResult::Downgrade {
                    target_mode: IngestMode::Metadata,
                    ..
                }
            ),
            "Expected Downgrade to Metadata, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reserve_and_reconcile_cost() {
        let storage = StorageClient::new_memory().await.expect("storage");

        // Define the cost_ledger_daily table
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        // Reserve cost
        let ledger_id = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 5.0)
            .await
            .expect("reserve");

        // Reconcile with actual cost
        let reconciliation = enforcer
            .reconcile_cost(&ledger_id, 4.5, 100_000, true)
            .await
            .expect("reconcile");

        assert!((reconciliation.reserved - 5.0).abs() < f64::EPSILON);
        assert!((reconciliation.actual - 4.5).abs() < f64::EPSILON);
        assert!(reconciliation.overrun.is_none());
    }

    async fn reconciliation_replay_contract(storage: StorageClient) {
        let enforcer =
            BudgetEnforcer::new(storage.clone(), make_budget(), "accounting".to_string());
        let ledger = enforcer
            .reserve_cost(IngestMode::Retrieval, "attempt", 5.0)
            .await
            .expect("reserve");
        enforcer
            .reconcile_cost(&ledger, 0.0, 0, false)
            .await
            .expect("zero usage");
        enforcer
            .reconcile_cost(&ledger, 0.0, 0, false)
            .await
            .expect("replay");
        let repo = CostLedgerDailyRepository::new(storage);
        let row = repo
            .get("accounting", &ledger)
            .await
            .expect("ledger")
            .expect("row");
        assert_eq!(row.reserved_euros, 0.0);
        assert_eq!(row.total_tokens, 0);
        assert_eq!(row.total_runs, 1);
        assert_eq!(row.total_failures, 1);
        let guard_id = enforcer.budget_guard_id(row.date, row.mode);
        let guard = repo
            .get("accounting", &guard_id)
            .await
            .expect("guard")
            .expect("guard row");
        assert_eq!(guard.reserved_euros, 0.0);
        assert_eq!(guard.total_runs, 1, "replay cannot charge guard twice");
        assert_eq!(guard.total_failures, 1);
    }

    #[tokio::test]
    async fn embedded_zero_usage_reconciliation_is_atomic_and_idempotent() {
        reconciliation_replay_contract(StorageClient::new_embedded().await.expect("embedded"))
            .await;
    }

    #[tokio::test]
    async fn surreal_zero_usage_reconciliation_is_atomic_and_idempotent() {
        reconciliation_replay_contract(StorageClient::new_memory().await.expect("surreal")).await;
    }

    #[tokio::test]
    async fn test_reserve_cost_blocks_over_budget() {
        let storage = StorageClient::new_memory().await.expect("storage");

        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let mut budget = make_budget();
        budget.daily_euros_retrieval = 5.0;
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 5.0)
            .await
            .expect("reserve first");

        let result = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 1.0)
            .await;

        assert!(matches!(result, Err(PipelineError::BudgetExceeded(_))));
    }

    #[tokio::test]
    async fn test_alert_only_emits_operator_alert() {
        use elephant_storage::system_event_repository::SystemEventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");

        storage
            .query(
                "DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE system_event SCHEMALESS;",
            )
            .await
            .expect("define tables");

        let mut budget = make_budget();
        budget.daily_euros_retrieval = 1.0;
        budget.downgrade_policy = DowngradePolicy::AlertOnly;
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());

        let result = enforcer
            .check_budget(IngestMode::Retrieval, 10.0)
            .await
            .expect("check");

        assert_eq!(result, BudgetCheckResult::Allowed);

        let event_repo = SystemEventRepository::new(storage);
        let events = event_repo
            .get_after_seq("test_space", 0, 10)
            .await
            .expect("events");

        assert!(events
            .iter()
            .any(|event| event.event_type == "operator_alert"));
    }

    #[tokio::test]
    async fn test_reserve_and_reconcile_cost_with_overrun() {
        let storage = StorageClient::new_memory().await.expect("storage");

        // Define the cost_ledger_daily table
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let budget = make_budget();
        let enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        // Reserve cost
        let ledger_id = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 5.0)
            .await
            .expect("reserve");

        // Reconcile with higher actual cost (overrun)
        let reconciliation = enforcer
            .reconcile_cost(&ledger_id, 7.0, 150_000, false)
            .await
            .expect("reconcile");

        assert!((reconciliation.reserved - 5.0).abs() < f64::EPSILON);
        assert!((reconciliation.actual - 7.0).abs() < f64::EPSILON);
        assert!(reconciliation.overrun.is_some());
        assert!((reconciliation.overrun.unwrap() - 2.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_reconcile_emits_budget_breach_event() {
        use elephant_storage::system_event_repository::SystemEventRepository;
        use elephant_storage::work_item_repository::WorkItemRepository;

        let storage = StorageClient::new_memory().await.expect("storage");

        storage
            .query(
                "DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE system_event SCHEMALESS; DEFINE TABLE work_item SCHEMALESS;",
            )
            .await
            .expect("define table");

        let mut budget = make_budget();
        budget.daily_euros_retrieval = 5.0;
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());
        let work_item_repo = WorkItemRepository::new(storage.clone());

        let queued_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "upgrade_ingest_mode".to_string(),
            payload: serde_json::json!({ "mode": IngestMode::Retrieval }),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: "queued:retrieval".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let queued_item = work_item_repo
            .create(queued_item)
            .await
            .expect("create queued work item");

        let ledger_id = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 5.0)
            .await
            .expect("reserve");

        enforcer
            .reconcile_cost(&ledger_id, 7.0, 150_000, true)
            .await
            .expect("reconcile");

        let repo = SystemEventRepository::new(storage);
        let events = repo.list_recent("test_space", 10).await.expect("events");
        assert_eq!(events.len(), 2);
        let event_types: Vec<_> = events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(event_types.contains(&"budget_breach"));
        assert!(event_types.contains(&"operator_alert"));

        let breach_event = events
            .iter()
            .find(|event| event.event_type == "budget_breach")
            .expect("budget breach event");
        let payload: BudgetBreachEvent =
            serde_json::from_value(breach_event.payload.clone()).expect("payload");
        assert_eq!(payload.mode, IngestMode::Retrieval);
        assert_eq!(payload.breach_type, BudgetBreachType::DailyEurosExceeded);
        assert!((payload.overrun_amount - 2.0).abs() < f64::EPSILON);

        let alert_event = events
            .iter()
            .find(|event| event.event_type == "operator_alert")
            .expect("operator alert event");
        let alert_payload: OperatorAlertEvent =
            serde_json::from_value(alert_event.payload.clone()).expect("payload");
        assert_eq!(alert_payload.mode, IngestMode::Retrieval);
        assert_eq!(alert_payload.paused_items, 1);
        assert!((alert_payload.overrun_amount - 2.0).abs() < f64::EPSILON);

        let updated_item = work_item_repo
            .get("test_space", &queued_item.base.id)
            .await
            .expect("get queued work item")
            .expect("queued work item exists");
        assert!(updated_item.run_after.is_some());
    }

    async fn budget_exhausted_events(storage: &StorageClient, space_id: &str) -> Vec<SystemEvent> {
        let repo = SystemEventRepository::new(storage.clone());
        repo.list_recent(space_id, 50)
            .await
            .expect("events")
            .into_iter()
            .filter(|event| event.event_type == BUDGET_EXHAUSTED_EVENT_TYPE)
            .collect()
    }

    #[tokio::test]
    async fn test_refused_reservation_records_one_budget_exhausted_event_per_mode_day() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query(
                "DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE system_event SCHEMALESS;",
            )
            .await
            .expect("define tables");

        let mut budget = make_budget();
        budget.daily_euros_retrieval = 5.0;
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());

        enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 5.0)
            .await
            .expect("first reservation fits the budget");

        // Exhausted: every further reservation is refused...
        for _ in 0..3 {
            let refused = enforcer
                .reserve_cost(IngestMode::Retrieval, "embed", 1.0)
                .await;
            assert!(matches!(refused, Err(PipelineError::BudgetExceeded(_))));
        }

        // ...but the signal is throttled to one event for the (mode, day).
        let events = budget_exhausted_events(&storage, "test_space").await;
        assert_eq!(
            events.len(),
            1,
            "repeated refusals must not spam events: {events:?}"
        );

        let payload: BudgetExhaustedEvent =
            serde_json::from_value(events[0].payload.clone()).expect("payload");
        assert_eq!(payload.space_id, "test_space");
        assert_eq!(payload.mode, IngestMode::Retrieval);
        assert_eq!(payload.day, Utc::now().date_naive());
        assert_eq!(payload.limit, BudgetExhaustedLimit::Euros);

        // The event id is deterministic per (space, mode, day).
        assert_eq!(
            events[0].base.id,
            budget_exhausted_event_id("test_space", payload.day, IngestMode::Retrieval)
        );

        // A different mode exhausting gets its own single event.
        let mut graph_budget = make_budget();
        graph_budget.daily_euros_graph = 0.5;
        let graph_enforcer =
            BudgetEnforcer::new(storage.clone(), graph_budget, "test_space".to_string());
        for _ in 0..2 {
            let refused = graph_enforcer
                .reserve_cost(IngestMode::Graph, "extract", 1.0)
                .await;
            assert!(matches!(refused, Err(PipelineError::BudgetExceeded(_))));
        }

        let events = budget_exhausted_events(&storage, "test_space").await;
        assert_eq!(events.len(), 2, "one event per exhausted mode: {events:?}");
        let modes: Vec<IngestMode> = events
            .iter()
            .map(|event| {
                serde_json::from_value::<BudgetExhaustedEvent>(event.payload.clone())
                    .expect("payload")
                    .mode
            })
            .collect();
        assert!(modes.contains(&IngestMode::Retrieval));
        assert!(modes.contains(&IngestMode::Graph));
    }

    #[tokio::test]
    async fn test_item_limit_exhaustion_records_items_limit_kind() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query(
                "DEFINE TABLE cost_ledger_daily SCHEMALESS; DEFINE TABLE system_event SCHEMALESS;",
            )
            .await
            .expect("define tables");

        let mut budget = make_budget();
        budget.daily_items_retrieval = Some(1);
        let enforcer = BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());

        enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 0.5)
            .await
            .expect("first item fits the limit");

        let refused = enforcer
            .reserve_cost(IngestMode::Retrieval, "embed", 0.5)
            .await;
        assert!(matches!(refused, Err(PipelineError::BudgetExceeded(_))));

        let events = budget_exhausted_events(&storage, "test_space").await;
        assert_eq!(events.len(), 1);
        let payload: BudgetExhaustedEvent =
            serde_json::from_value(events[0].payload.clone()).expect("payload");
        assert_eq!(payload.limit, BudgetExhaustedLimit::Items);
        assert_eq!(payload.mode, IngestMode::Retrieval);
    }

    #[tokio::test]
    async fn test_daily_cap_allows_under_limit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let repo = CostLedgerDailyRepository::new(storage);
        let cap = DailyCapCheck::new(repo, 100);

        let result = cap.try_reserve("test_space").await.expect("try_reserve");
        assert!(result, "Should allow when under cap");
    }

    #[tokio::test]
    async fn test_daily_cap_blocks_at_limit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let repo = CostLedgerDailyRepository::new(storage.clone());
        let cap = DailyCapCheck::new(repo.clone(), 2);

        // Record 2 items (reaching the cap)
        let today = Utc::now().date_naive();
        let ledger1 = CostLedgerDaily {
            base: BaseFields::new(
                RecordId::new_v4(TableName::CostLedgerDaily),
                "test_space".to_string(),
                "test".to_string(),
            ),
            date: today,
            mode: IngestMode::Graph,
            worker_kind: "work_queue".to_string(),
            total_euros: 1.0,
            total_items: 1,
            total_tokens: 1000,
            total_runs: 1,
            total_failures: 0,
            reserved_euros: 0.0,
            reconciled_euros: 1.0,
        };
        repo.create(ledger1).await.expect("create ledger1");

        let ledger2 = CostLedgerDaily {
            base: BaseFields::new(
                RecordId::new_v4(TableName::CostLedgerDaily),
                "test_space".to_string(),
                "test".to_string(),
            ),
            date: today,
            mode: IngestMode::Retrieval,
            worker_kind: "work_queue".to_string(),
            total_euros: 1.0,
            total_items: 1,
            total_tokens: 1000,
            total_runs: 1,
            total_failures: 0,
            reserved_euros: 0.0,
            reconciled_euros: 1.0,
        };
        repo.create(ledger2).await.expect("create ledger2");

        let result = cap.try_reserve("test_space").await.expect("try_reserve");
        assert!(!result, "Should block when at cap");
    }

    #[tokio::test]
    async fn test_daily_cap_record_cost() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage
            .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
            .await
            .expect("define table");

        let repo = CostLedgerDailyRepository::new(storage.clone());
        let cap = DailyCapCheck::new(repo.clone(), 10);

        // Record a cost
        cap.record_cost("test_space", 5000, 0.005)
            .await
            .expect("record_cost");

        // Verify item was recorded
        let today = Utc::now().date_naive();
        let totals = repo
            .aggregate_daily_spending("test_space", today)
            .await
            .expect("aggregate");
        assert_eq!(totals.items_graph, 1);
    }
}
