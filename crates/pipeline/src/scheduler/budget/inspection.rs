//! Read-only projection of the same daily guards used for scheduling.

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;

use super::{budget_reset_at, BudgetEnforcer, IngestMode, PipelineError};

/// One mode's daily budget, including outstanding reservations.
#[derive(Debug, Clone, Serialize)]
pub struct ModeBudgetInspection {
    /// Abstract accounting units; legacy configuration calls these euros.
    pub limit_units: f64,
    /// Outstanding reservations plus reconciled units.
    pub used_units: f64,
    /// Available units, clamped to zero on overrun.
    pub remaining_units: f64,
    /// Daily work-item limit, if configured.
    pub limit_items: Option<u32>,
    /// Admitted work items, including outstanding reservations.
    pub used_items: u32,
    /// Remaining items, saturated at zero; absent for an unlimited mode.
    pub remaining_items: Option<u32>,
    /// `budget_guard`, or `ledger` when no guard exists yet.
    pub usage_source: &'static str,
}

impl ModeBudgetInspection {
    fn new(
        limit_units: f64,
        limit_items: Option<u32>,
        used_units: f64,
        used_items: u32,
        usage_source: &'static str,
    ) -> Result<Self, PipelineError> {
        if !limit_units.is_finite() || limit_units < 0.0 {
            return Err(PipelineError::ConfigError(
                "invalid daily budget limit".into(),
            ));
        }
        if !used_units.is_finite() || used_units < 0.0 {
            return Err(PipelineError::ValidationError(
                "invalid daily budget usage".into(),
            ));
        }
        Ok(Self {
            limit_units,
            used_units,
            remaining_units: (limit_units - used_units).max(0.0),
            limit_items,
            used_items,
            remaining_items: limit_items.map(|limit| limit.saturating_sub(used_items)),
            usage_source,
        })
    }
}

/// Advisory daily headroom, not a reservation or atomic cross-mode snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct BudgetInspection {
    /// UTC date whose ledger and guards were read.
    pub date: NaiveDate,
    /// Start of this advisory observation.
    pub sampled_at: DateTime<Utc>,
    /// Beginning of the next UTC budget day.
    pub resets_at: DateTime<Utc>,
    /// Retrieval-mode usage and limits.
    pub retrieval: ModeBudgetInspection,
    /// Graph-mode usage and limits.
    pub graph: ModeBudgetInspection,
}

impl BudgetEnforcer {
    /// Inspect the effective limits and current daily guard usage without writes.
    ///
    /// Existing reservations count toward usage. A mode without a guard uses
    /// the same ledger fallback as admission; guard and ledger are never added.
    /// Concurrent work may change headroom after any read.
    ///
    /// # Errors
    /// Returns storage errors or invalid non-finite/negative limits or usage.
    pub async fn inspect(&self) -> Result<BudgetInspection, PipelineError> {
        let sampled_at = Utc::now();
        let date = sampled_at.date_naive();
        let retrieval = self.get_budget_guard(date, IngestMode::Retrieval).await?;
        let graph = self.get_budget_guard(date, IngestMode::Graph).await?;
        let fallback = if retrieval.is_none() || graph.is_none() {
            Some(
                self.ledger_repo
                    .aggregate_daily_spending(&self.space_id, date)
                    .await?,
            )
        } else {
            None
        };
        let mode = |guard: Option<super::CostLedgerDaily>, graph_mode: bool| {
            let (used_units, used_items, source) = if let Some(guard) = guard {
                (
                    guard.reserved_euros + guard.reconciled_euros,
                    guard.total_items,
                    "budget_guard",
                )
            } else if let Some(totals) = &fallback {
                if graph_mode {
                    (totals.spent_graph, totals.items_graph, "ledger")
                } else {
                    (totals.spent_retrieval, totals.items_retrieval, "ledger")
                }
            } else {
                return Err(PipelineError::ValidationError(
                    "missing daily budget observation".into(),
                ));
            };
            let (limit_units, limit_items) = if graph_mode {
                (self.budget.daily_euros_graph, self.budget.daily_items_graph)
            } else {
                (
                    self.budget.daily_euros_retrieval,
                    self.budget.daily_items_retrieval,
                )
            };
            ModeBudgetInspection::new(limit_units, limit_items, used_units, used_items, source)
        };
        Ok(BudgetInspection {
            date,
            sampled_at,
            resets_at: budget_reset_at(date)?,
            retrieval: mode(retrieval, false)?,
            graph: mode(graph, true)?,
        })
    }
}

#[cfg(test)]
mod tests;
