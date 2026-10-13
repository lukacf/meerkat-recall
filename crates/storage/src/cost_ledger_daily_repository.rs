//! Cost ledger daily repository implementation for `SurrealDB`.
//!
//! Per spec §17, cost ledgers track daily spending by space, mode, and worker kind.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::ingest::{CostLedgerDaily, IngestMode, BUDGET_GUARD_WORKER_KIND};
use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

const COST_LEDGER_DAILY_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    date,
    mode,
    worker_kind,
    total_euros,
    total_items,
    total_tokens,
    total_runs,
    total_failures,
    reserved_euros,
    reconciled_euros
";

const BUDGET_GUARD_WORKER_KIND_LITERAL: &str = BUDGET_GUARD_WORKER_KIND;

const RETRIEVAL_DAILY_SPENDING_QUERY: &str = r"SELECT
                    count() AS row_count,
                    math::sum(reconciled_euros) AS reconciled_euros,
                    math::sum(reserved_euros) AS reserved_euros,
                    math::sum(total_items) AS total_items,
                    math::sum(total_runs) AS total_runs,
                    math::sum(total_failures) AS total_failures
                FROM cost_ledger_daily
                WHERE space_id = $space_id
                    AND date = $date
                    AND mode = 'retrieval'
                    AND worker_kind != $guard_kind
                    AND deleted_at IS NONE
                GROUP ALL";

const GRAPH_DAILY_SPENDING_QUERY: &str = r"SELECT
                    count() AS row_count,
                    math::sum(reconciled_euros) AS reconciled_euros,
                    math::sum(reserved_euros) AS reserved_euros,
                    math::sum(total_items) AS total_items,
                    math::sum(total_runs) AS total_runs,
                    math::sum(total_failures) AS total_failures
                FROM cost_ledger_daily
                WHERE space_id = $space_id
                    AND date = $date
                    AND mode = 'graph'
                    AND worker_kind != $guard_kind
                    AND deleted_at IS NONE
                GROUP ALL";

fn cost_ledger_daily_lookup_query() -> String {
    format!("SELECT {COST_LEDGER_DAILY_PROJECTION} FROM $id WHERE space_id = $space_id AND deleted_at IS NONE")
}

fn date_to_datetime(date: NaiveDate) -> StorageResult<chrono::DateTime<Utc>> {
    date.and_hms_opt(0, 0, 0)
        .map(|naive| chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        .ok_or_else(|| StorageError::QueryError("Invalid ledger date".to_string()))
}

fn date_to_surreal(date: NaiveDate) -> StorageResult<surrealdb::sql::Datetime> {
    Ok(crate::surreal::datetime_from_chrono(date_to_datetime(
        date,
    )?))
}

/// Database-only proxy for CostLedgerDaily.
#[derive(Deserialize)]
struct CostLedgerDailyDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    date: Option<chrono::DateTime<Utc>>,
    mode: Option<IngestMode>,
    worker_kind: Option<String>,
    total_euros: Option<f64>,
    total_items: Option<u32>,
    total_tokens: Option<u64>,
    total_runs: Option<u32>,
    total_failures: Option<u32>,
    reserved_euros: Option<f64>,
    reconciled_euros: Option<f64>,
}

impl TryFrom<CostLedgerDailyDb> for CostLedgerDaily {
    type Error = StorageError;

    fn try_from(db: CostLedgerDailyDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            date: db
                .date
                .ok_or_else(|| StorageError::QueryError("Missing date".to_string()))?
                .date_naive(),
            mode: required_field(db.mode, "mode")?,
            worker_kind: db
                .worker_kind
                .ok_or_else(|| StorageError::QueryError("Missing worker_kind".to_string()))?,
            total_euros: required_field(db.total_euros, "total_euros")?,
            total_items: required_field(db.total_items, "total_items")?,
            total_tokens: required_field(db.total_tokens, "total_tokens")?,
            total_runs: required_field(db.total_runs, "total_runs")?,
            total_failures: required_field(db.total_failures, "total_failures")?,
            reserved_euros: required_field(db.reserved_euros, "reserved_euros")?,
            reconciled_euros: required_field(db.reconciled_euros, "reconciled_euros")?,
        })
    }
}

/// Cost ledger daily repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct CostLedgerDailyRepository {
    client: StorageClient,
}

/// Daily spending totals aggregated from ledger entries.
#[derive(Debug, Clone, Default)]
pub struct DailySpendingTotals {
    /// Total euros spent on retrieval mode.
    pub spent_retrieval: f64,
    /// Total euros spent on graph mode.
    pub spent_graph: f64,
    /// Total items processed in retrieval mode.
    pub items_retrieval: u32,
    /// Total items processed in graph mode.
    pub items_graph: u32,
    /// Total runs in retrieval mode.
    pub runs_retrieval: u32,
    /// Total runs in graph mode.
    pub runs_graph: u32,
    /// Total failures in retrieval mode.
    pub failures_retrieval: u32,
    /// Total failures in graph mode.
    pub failures_graph: u32,
}

impl CostLedgerDailyRepository {
    /// Creates a new cost ledger daily repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Aggregates daily spending totals for a specific date.
    ///
    /// Returns the total euros spent and items processed for each mode
    /// (retrieval vs graph) on the given date.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[allow(clippy::too_many_lines)]
    pub async fn aggregate_daily_spending(
        &self,
        space_id: &str,
        date: NaiveDate,
    ) -> StorageResult<DailySpendingTotals> {
        if let Some(store) = self.client.embedded_store() {
            let items = store.scan::<CostLedgerDaily>().await?;
            let mut totals = DailySpendingTotals::default();

            for item in items {
                if item.base.space_id != space_id
                    || item.base.deleted_at.is_some()
                    || item.date != date
                    || item.worker_kind == BUDGET_GUARD_WORKER_KIND_LITERAL
                {
                    continue;
                }

                let spent = item.reconciled_euros + item.reserved_euros;
                match item.mode {
                    IngestMode::Retrieval => {
                        totals.spent_retrieval += spent;
                        totals.items_retrieval += item.total_items;
                        totals.runs_retrieval += item.total_runs;
                        totals.failures_retrieval += item.total_failures;
                    }
                    IngestMode::Graph => {
                        totals.spent_graph += spent;
                        totals.items_graph += item.total_items;
                        totals.runs_graph += item.total_runs;
                        totals.failures_graph += item.total_failures;
                    }
                    IngestMode::Skip | IngestMode::Metadata => {}
                }
            }

            return Ok(totals);
        }

        // Query for retrieval mode spending
        #[derive(serde::Deserialize)]
        struct AggResult {
            row_count: Option<u64>,
            reconciled_euros: Option<f64>,
            reserved_euros: Option<f64>,
            total_items: Option<u32>,
            total_runs: Option<u32>,
            total_failures: Option<u32>,
        }

        struct ModeTotals {
            reconciled_euros: f64,
            reserved_euros: f64,
            total_items: u32,
            total_runs: u32,
            total_failures: u32,
        }

        impl ModeTotals {
            const fn zero() -> Self {
                Self {
                    reconciled_euros: 0.0,
                    reserved_euros: 0.0,
                    total_items: 0,
                    total_runs: 0,
                    total_failures: 0,
                }
            }
        }

        fn to_totals(mode: &str, row: Option<AggResult>) -> StorageResult<ModeTotals> {
            let Some(row) = row else {
                return Ok(ModeTotals::zero());
            };

            let row_count = required_field(row.row_count, &format!("{mode} row_count"))?;
            if row_count == 0 {
                return Ok(ModeTotals::zero());
            }

            Ok(ModeTotals {
                reconciled_euros: required_field(
                    row.reconciled_euros,
                    &format!("{mode} reconciled_euros"),
                )?,
                reserved_euros: required_field(
                    row.reserved_euros,
                    &format!("{mode} reserved_euros"),
                )?,
                total_items: required_field(row.total_items, &format!("{mode} total_items"))?,
                total_runs: required_field(row.total_runs, &format!("{mode} total_runs"))?,
                total_failures: required_field(
                    row.total_failures,
                    &format!("{mode} total_failures"),
                )?,
            })
        }

        let date_value = date_to_surreal(date)?;

        // Query for retrieval mode spending
        let mut response = self
            .client
            .query_bind_checked(
                RETRIEVAL_DAILY_SPENDING_QUERY,
                crate::bindings! {
                    "space_id" => space_id,
                    "date" => date_value.clone(),
                    "guard_kind" => BUDGET_GUARD_WORKER_KIND_LITERAL
                },
            )
            .await?;

        let retrieval_result: Vec<AggResult> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse retrieval totals: {e}"))
        })?;
        let retrieval = to_totals("retrieval", retrieval_result.into_iter().next())?;

        // Query for graph mode spending
        let mut response = self
            .client
            .query_bind_checked(
                GRAPH_DAILY_SPENDING_QUERY,
                crate::bindings! {
                    "space_id" => space_id,
                    "date" => date_value,
                    "guard_kind" => BUDGET_GUARD_WORKER_KIND_LITERAL
                },
            )
            .await?;

        let graph_result: Vec<AggResult> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse graph totals: {e}")))?;
        let graph = to_totals("graph", graph_result.into_iter().next())?;

        let retrieval_total = retrieval.reconciled_euros + retrieval.reserved_euros;
        let graph_total = graph.reconciled_euros + graph.reserved_euros;

        Ok(DailySpendingTotals {
            spent_retrieval: retrieval_total,
            spent_graph: graph_total,
            items_retrieval: retrieval.total_items,
            items_graph: graph.total_items,
            runs_retrieval: retrieval.total_runs,
            runs_graph: graph.total_runs,
            failures_retrieval: retrieval.total_failures,
            failures_graph: graph.total_failures,
        })
    }

    /// Reserves daily budget in the guard ledger record.
    ///
    /// Returns true if reservation succeeded, false if limits were exceeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub async fn reserve_budget_guard(
        &self,
        guard_id: &RecordId,
        space_id: &str,
        date: NaiveDate,
        mode: IngestMode,
        estimated_cost: f64,
        budget_limit: f64,
        item_limit: Option<u32>,
    ) -> StorageResult<bool> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let now = Utc::now();
                    let mut guard = mutation
                        .get::<CostLedgerDaily>(space_id, guard_id, true)?
                        .unwrap_or_else(|| {
                            Self::new_budget_guard_record(guard_id, space_id, date, mode, now)
                        });

                    let current_total = guard.reserved_euros + guard.reconciled_euros;
                    if current_total + estimated_cost > budget_limit {
                        return Ok(false);
                    }

                    if item_limit.is_some_and(|limit| guard.total_items.saturating_add(1) > limit) {
                        return Ok(false);
                    }

                    let exists = mutation
                        .get::<CostLedgerDaily>(space_id, guard_id, true)?
                        .is_some();
                    guard.base.deleted_at = None;
                    guard.base.updated_at = now;
                    guard.date = date;
                    guard.mode = mode;
                    guard.worker_kind = BUDGET_GUARD_WORKER_KIND_LITERAL.to_string();
                    guard.total_items = guard.total_items.saturating_add(1);
                    guard.reserved_euros += estimated_cost;

                    if exists {
                        mutation.replace(guard)?;
                    } else {
                        mutation.create(guard)?;
                    }

                    Ok(true)
                })
                .await;
        }

        use surrealdb::sql::{Array, Value};

        let date_value = date_to_surreal(date)?;
        let now = Utc::now();

        let mut conditions = vec![String::from(
            "(IF reserved_euros IS NONE THEN 0.0 ELSE reserved_euros END)
                + (IF reconciled_euros IS NONE THEN 0.0 ELSE reconciled_euros END)
                + $cost <= $budget_limit",
        )];

        if item_limit.is_some() {
            conditions.push(String::from(
                "(IF total_items IS NONE THEN 0 ELSE total_items END) + 1 <= $item_limit",
            ));
        }

        let where_clause = conditions.join(" AND ");

        let sql = format!(
            "UPSERT $id SET
                space_id = $space_id,
                created_at = IF created_at IS NONE THEN $created_at ELSE created_at END,
                updated_at = $updated_at,
                created_by = IF created_by IS NONE THEN $created_by ELSE created_by END,
                deleted_at = NONE,
                security = IF security IS NONE THEN $security ELSE security END,
                provenance = IF provenance IS NONE THEN $provenance ELSE provenance END,
                subjects = IF subjects IS NONE THEN $subjects ELSE subjects END,
                subjects_state = IF subjects_state IS NONE THEN $subjects_state ELSE subjects_state END,
                date = $date,
                mode = $mode,
                worker_kind = $worker_kind,
                total_euros = IF total_euros IS NONE THEN 0.0 ELSE total_euros END,
                total_tokens = IF total_tokens IS NONE THEN 0 ELSE total_tokens END,
                total_runs = IF total_runs IS NONE THEN 0 ELSE total_runs END,
                total_failures = IF total_failures IS NONE THEN 0 ELSE total_failures END,
                total_items = IF total_items IS NONE THEN 1 ELSE total_items + 1 END,
                reserved_euros = IF reserved_euros IS NONE THEN $cost ELSE reserved_euros + $cost END,
                reconciled_euros = IF reconciled_euros IS NONE THEN 0.0 ELSE reconciled_euros END
            WHERE {where_clause}
            RETURN AFTER;"
        );

        // Checked + retried: every concurrent worker read-modify-writes the
        // same deterministic guard record, making this the most conflict-prone
        // single-statement upsert in the system. The conflict retry absorbs
        // transient "read or write conflict" failures instead of surfacing
        // them to the scheduler.
        let mut response = self
            .client
            .query_bind_checked(
                &sql,
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(guard_id),
                    "space_id" => space_id,
                    "created_at" => crate::surreal::datetime_from_chrono(now),
                    "updated_at" => crate::surreal::datetime_from_chrono(now),
                    "created_by" => "budget_guard",
                    "security" => crate::surreal::security_to_value(&Security::default()),
                    "provenance" => crate::surreal::provenance_to_value(&Provenance::default())?,
                    "subjects" => Value::Array(Array::from(Vec::<Value>::new())),
                    "subjects_state" => crate::surreal::enum_to_surreal(&SubjectsState::Unknown)?,
                    "date" => date_value,
                    "mode" => crate::surreal::enum_to_surreal(&mode)?,
                    "worker_kind" => BUDGET_GUARD_WORKER_KIND_LITERAL,
                    "cost" => estimated_cost,
                    "budget_limit" => budget_limit,
                    "item_limit" => item_limit.map_or(Value::None, Value::from),
                },
            )
            .await?;

        let items: Vec<CostLedgerDailyDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse guard reserve: {e}")))?;

        Ok(!items.is_empty())
    }

    /// Reconciles reserved vs actual cost in the guard ledger record.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn reconcile_budget_guard(
        &self,
        guard_id: &RecordId,
        space_id: &str,
        date: NaiveDate,
        mode: IngestMode,
        reserved_euros: f64,
        actual_euros: f64,
        success: bool,
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let now = Utc::now();
                    let mut guard = mutation
                        .get::<CostLedgerDaily>(space_id, guard_id, true)?
                        .unwrap_or_else(|| {
                            Self::new_budget_guard_record(guard_id, space_id, date, mode, now)
                        });

                    let exists = mutation
                        .get::<CostLedgerDaily>(space_id, guard_id, true)?
                        .is_some();
                    guard.base.deleted_at = None;
                    guard.base.updated_at = now;
                    guard.date = date;
                    guard.mode = mode;
                    guard.worker_kind = BUDGET_GUARD_WORKER_KIND_LITERAL.to_string();
                    guard.total_euros += actual_euros;
                    guard.total_runs = guard.total_runs.saturating_add(1);
                    if !success {
                        guard.total_failures = guard.total_failures.saturating_add(1);
                    }
                    guard.reserved_euros = (guard.reserved_euros - reserved_euros).max(0.0);
                    guard.reconciled_euros += actual_euros;

                    if exists {
                        mutation.replace(guard)?;
                    } else {
                        mutation.create(guard)?;
                    }

                    Ok(())
                })
                .await;
        }

        use surrealdb::sql::{Array, Value};

        let date_value = date_to_surreal(date)?;
        let now = Utc::now();
        let failures = i32::from(!success);

        let sql = "UPSERT $id SET
                space_id = $space_id,
                created_at = IF created_at IS NONE THEN $created_at ELSE created_at END,
                updated_at = $updated_at,
                created_by = IF created_by IS NONE THEN $created_by ELSE created_by END,
                deleted_at = NONE,
                security = IF security IS NONE THEN $security ELSE security END,
                provenance = IF provenance IS NONE THEN $provenance ELSE provenance END,
                subjects = IF subjects IS NONE THEN $subjects ELSE subjects END,
                subjects_state = IF subjects_state IS NONE THEN $subjects_state ELSE subjects_state END,
                date = $date,
                mode = $mode,
                worker_kind = $worker_kind,
                total_euros = (IF total_euros IS NONE THEN 0.0 ELSE total_euros END) + $actual,
                total_tokens = IF total_tokens IS NONE THEN 0 ELSE total_tokens END,
                total_runs = (IF total_runs IS NONE THEN 0 ELSE total_runs END) + 1,
                total_failures = (IF total_failures IS NONE THEN 0 ELSE total_failures END) + $failures,
                total_items = IF total_items IS NONE THEN 0 ELSE total_items END,
                reserved_euros = math::max([(IF reserved_euros IS NONE THEN 0.0 ELSE reserved_euros END) - $reserved, 0.0]),
                reconciled_euros = (IF reconciled_euros IS NONE THEN 0.0 ELSE reconciled_euros END) + $actual;";

        // Checked + retried for the same reason as reserve_budget_guard: the
        // guard record is a deterministic per-space/day/mode singleton that
        // concurrent workers contend on.
        let mut response = self
            .client
            .query_bind_checked(
                sql,
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(guard_id),
                    "space_id" => space_id,
                    "created_at" => crate::surreal::datetime_from_chrono(now),
                    "updated_at" => crate::surreal::datetime_from_chrono(now),
                    "created_by" => "budget_guard",
                    "security" => crate::surreal::security_to_value(&Security::default()),
                    "provenance" => crate::surreal::provenance_to_value(&Provenance::default())?,
                    "subjects" => Value::Array(Array::from(Vec::<Value>::new())),
                    "subjects_state" => crate::surreal::enum_to_surreal(&SubjectsState::Unknown)?,
                    "date" => date_value,
                    "mode" => crate::surreal::enum_to_surreal(&mode)?,
                    "worker_kind" => BUDGET_GUARD_WORKER_KIND_LITERAL,
                    "reserved" => reserved_euros,
                    "actual" => actual_euros,
                    "failures" => failures,
                },
            )
            .await?;

        response
            .take::<Vec<CostLedgerDailyDb>>(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to reconcile guard: {e}")))?;

        Ok(())
    }

    /// Reconciles a reservation and its shared budget guard atomically, once.
    /// Zero measured usage still releases the reservation and records a run.
    pub async fn reconcile_attempt(
        &self,
        space: &str,
        ledger_id: &RecordId,
        guard_id: &RecordId,
        actual_units: f64,
        tokens: u64,
        success: bool,
    ) -> StorageResult<bool> {
        if !actual_units.is_finite() || actual_units < 0.0 || ledger_id == guard_id {
            return Err(StorageError::DataIntegrityError(
                "Invalid attempt accounting".to_string(),
            ));
        }
        let ledger = self
            .get(space, ledger_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(ledger_id.to_string()))?;
        let initial_guard =
            Self::new_budget_guard_record(guard_id, space, ledger.date, ledger.mode, Utc::now());
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let mut ledger = tx
                        .get::<CostLedgerDaily>(space, ledger_id, false)?
                        .ok_or_else(|| StorageError::NotFound(ledger_id.to_string()))?;
                    if ledger.total_runs > 0 {
                        return Ok(false);
                    }
                    let guard = tx.get::<CostLedgerDaily>(space, guard_id, false)?;
                    let existed = guard.is_some();
                    let mut guard = guard.unwrap_or(initial_guard);
                    guard.reserved_euros = (guard.reserved_euros - ledger.reserved_euros).max(0.0);
                    guard.reconciled_euros += actual_units;
                    guard.total_euros += actual_units;
                    guard.total_tokens = guard.total_tokens.saturating_add(tokens);
                    guard.total_runs = guard.total_runs.saturating_add(1);
                    guard.total_failures = guard.total_failures.saturating_add(u32::from(!success));
                    guard.base.updated_at = Utc::now();
                    ledger.reserved_euros = 0.0;
                    ledger.reconciled_euros = actual_units;
                    ledger.total_euros = actual_units;
                    ledger.total_tokens = tokens;
                    ledger.total_runs = 1;
                    ledger.total_items = 1;
                    ledger.total_failures = u32::from(!success);
                    ledger.base.updated_at = Utc::now();
                    tx.replace(ledger)?;
                    if existed {
                        tx.replace(guard)?;
                    } else {
                        tx.create(guard)?;
                    }
                    Ok(true)
                })
                .await;
        }
        let mut response = self.client.query_bind_checked(
            "BEGIN TRANSACTION;
             LET $ledger = SELECT * FROM ONLY $id WHERE space_id = $space AND deleted_at IS NONE;
             IF $ledger IS NONE { THROW 'accounting_ledger_missing'; };
             LET $applied = IF $ledger.total_runs = 0 {
               LET $guard = SELECT * FROM ONLY $guard_id;
               IF $guard IS NONE { CREATE $guard_id CONTENT $initial_guard; };
               IF $guard IS NOT NONE AND $guard.space_id != $space { THROW 'accounting_guard_space_mismatch'; };
               UPDATE $guard_id SET reserved_euros = math::max([reserved_euros - $ledger.reserved_euros, 0.0]), reconciled_euros += $actual, total_euros += $actual, total_tokens += $tokens, total_runs += 1, total_failures += $failures, updated_at = time::now();
               UPDATE $id SET reserved_euros = 0.0, reconciled_euros = $actual, total_euros = $actual, total_tokens = $tokens, total_runs = 1, total_items = 1, total_failures = $failures, updated_at = time::now();
               RETURN true;
             } ELSE { RETURN false; };
             RETURN $applied; COMMIT TRANSACTION;",
            crate::bindings! {
                "id" => crate::surreal::thing_from_record_id(ledger_id), "space" => space,
                "guard_id" => crate::surreal::thing_from_record_id(guard_id),
                "initial_guard" => Self::cost_ledger_daily_to_value(&initial_guard)?,
                "actual" => actual_units, "tokens" => tokens, "failures" => u32::from(!success),
            },
        ).await?;
        response
            .take::<Option<bool>>(response.num_statements() - 1)
            .map_err(|error| {
                StorageError::DataIntegrityError(format!("Invalid accounting receipt: {error}"))
            })?
            .ok_or_else(|| {
                StorageError::DataIntegrityError("Missing accounting receipt".to_string())
            })
    }

    /// Converts a CostLedgerDaily to a SurrealDB value for insertion.
    fn cost_ledger_daily_to_value(item: &CostLedgerDaily) -> StorageResult<surrealdb::sql::Value> {
        use std::collections::BTreeMap;
        use surrealdb::sql::{Object, Value};

        let date_time = item
            .date
            .and_hms_opt(0, 0, 0)
            .map(|naive| chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
            .ok_or_else(|| StorageError::QueryError("Invalid ledger date".to_string()))?;

        let mut map: BTreeMap<String, Value> = BTreeMap::new();
        map.insert("space_id".to_string(), item.base.space_id.clone().into());
        map.insert(
            "created_at".to_string(),
            Value::from(crate::surreal::datetime_from_chrono(item.base.created_at)),
        );
        map.insert(
            "updated_at".to_string(),
            Value::from(crate::surreal::datetime_from_chrono(item.base.updated_at)),
        );
        map.insert(
            "created_by".to_string(),
            item.base.created_by.clone().into(),
        );
        map.insert(
            "deleted_at".to_string(),
            item.base.deleted_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        map.insert(
            "security".to_string(),
            crate::surreal::security_to_value(&item.base.security),
        );
        map.insert(
            "provenance".to_string(),
            crate::surreal::provenance_to_value(&item.base.provenance)?,
        );
        map.insert(
            "subjects".to_string(),
            Value::Array(crate::surreal::things_from_links(
                item.base.subjects.clone(),
            )),
        );
        map.insert(
            "subjects_state".to_string(),
            crate::surreal::enum_to_surreal(&item.base.subjects_state)?,
        );
        map.insert(
            "date".to_string(),
            Value::from(crate::surreal::datetime_from_chrono(date_time)),
        );
        map.insert(
            "mode".to_string(),
            crate::surreal::enum_to_surreal(&item.mode)?,
        );
        map.insert("worker_kind".to_string(), item.worker_kind.clone().into());
        map.insert("total_euros".to_string(), item.total_euros.into());
        map.insert("total_items".to_string(), item.total_items.into());
        map.insert("total_tokens".to_string(), item.total_tokens.into());
        map.insert("total_runs".to_string(), item.total_runs.into());
        map.insert("total_failures".to_string(), item.total_failures.into());
        map.insert("reserved_euros".to_string(), item.reserved_euros.into());
        map.insert("reconciled_euros".to_string(), item.reconciled_euros.into());

        Ok(Value::Object(Object::from(map)))
    }

    fn new_budget_guard_record(
        guard_id: &RecordId,
        space_id: &str,
        date: NaiveDate,
        mode: IngestMode,
        now: chrono::DateTime<Utc>,
    ) -> CostLedgerDaily {
        let mut base = BaseFields::new(
            guard_id.clone(),
            space_id.to_string(),
            "budget_guard".to_string(),
        );
        base.created_at = now;
        base.updated_at = now;
        base.deleted_at = None;
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects = Vec::new();
        base.subjects_state = SubjectsState::Unknown;

        CostLedgerDaily {
            base,
            date,
            mode,
            worker_kind: BUDGET_GUARD_WORKER_KIND_LITERAL.to_string(),
            total_euros: 0.0,
            total_items: 0,
            total_tokens: 0,
            total_runs: 0,
            total_failures: 0,
            reserved_euros: 0.0,
            reconciled_euros: 0.0,
        }
    }
}

#[async_trait]
impl Repository<CostLedgerDaily> for CostLedgerDailyRepository {
    async fn create(&self, record: CostLedgerDaily) -> StorageResult<CostLedgerDaily> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::cost_ledger_daily_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {COST_LEDGER_DAILY_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<CostLedgerDailyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created cost_ledger_daily: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but cost_ledger_daily not found".to_string())
        })?;

        CostLedgerDaily::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<CostLedgerDaily>> {
        // A direct record target must retain the former fixed-table boundary.
        if id.table() != TableName::CostLedgerDaily {
            return Ok(None);
        }
        if let Some(store) = self.client.embedded_store() {
            return store.get::<CostLedgerDaily>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &cost_ledger_daily_lookup_query(),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<CostLedgerDailyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse cost_ledger_daily: {e}"))
        })?;

        Ok(items
            .into_iter()
            .next()
            .map(CostLedgerDaily::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: CostLedgerDaily,
    ) -> StorageResult<CostLedgerDaily> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {COST_LEDGER_DAILY_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::cost_ledger_daily_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<CostLedgerDailyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated cost_ledger_daily: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("CostLedgerDaily not found in space: {id}"))
        })?;

        CostLedgerDaily::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<CostLedgerDaily>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<CostLedgerDaily>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<CostLedgerDaily>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {COST_LEDGER_DAILY_PROJECTION} FROM cost_ledger_daily WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {COST_LEDGER_DAILY_PROJECTION} FROM cost_ledger_daily WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind_checked(
                &query,
                crate::bindings! {
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                    "space_id" => space_id
                },
            )
            .await?;

        let items: Vec<CostLedgerDailyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse cost_ledger_dailies: {e}"))
        })?;

        items.into_iter().map(CostLedgerDaily::try_from).collect()
    }
}

#[cfg(test)]
#[path = "cost_ledger_daily_index_tests.rs"]
mod index_tests;

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;
    use crate::StorageConfig;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81500);

    const COST_LEDGER_DAILY_SCHEMA: &str = r"
        DEFINE TABLE cost_ledger_daily SCHEMALESS;
    ";

    async fn setup_test_repo() -> CostLedgerDailyRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("cost_ledger_daily_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client
            .query_checked(COST_LEDGER_DAILY_SCHEMA)
            .await
            .unwrap();

        CostLedgerDailyRepository::new(client)
    }

    fn create_test_cost_ledger_daily() -> CostLedgerDaily {
        let id = RecordId::new_v4(TableName::CostLedgerDaily);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        CostLedgerDaily {
            base,
            date: NaiveDate::from_ymd_opt(2024, 7, 15).unwrap(),
            mode: IngestMode::Retrieval,
            worker_kind: "embed".to_string(),
            total_euros: 5.25,
            total_items: 100,
            total_tokens: 500_000,
            total_runs: 100,
            total_failures: 2,
            reserved_euros: 1.5,
            reconciled_euros: 3.75,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_cost_ledger_daily() {
        let repo = setup_test_repo().await;
        let item = create_test_cost_ledger_daily();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.worker_kind, "embed");
        assert_eq!(created.mode, IngestMode::Retrieval);
        assert!((created.total_euros - 5.25).abs() < f64::EPSILON);
        assert_eq!(created.total_runs, 100);
        assert_eq!(created.total_failures, 2);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_cost_ledger_daily() {
        let repo = setup_test_repo().await;
        let item = create_test_cost_ledger_daily();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_cost_ledger_daily_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_cost_ledger_daily();
        item.total_euros = 12.75;
        item.total_items = 250;
        item.total_tokens = 1_250_000;
        item.total_runs = 250;
        item.total_failures = 7;
        item.reserved_euros = 2.5;
        item.reconciled_euros = 10.25;

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert!((created.total_euros - 12.75).abs() < f64::EPSILON);
        assert_eq!(created.total_items, 250);
        assert_eq!(created.total_tokens, 1_250_000);
        assert_eq!(created.total_runs, 250);
        assert_eq!(created.total_failures, 7);
        assert!((created.reserved_euros - 2.5).abs() < f64::EPSILON);
        assert!((created.reconciled_euros - 10.25).abs() < f64::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_cost_ledger_daily() {
        let repo = setup_test_repo().await;
        let item = create_test_cost_ledger_daily();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.total_euros = 10.0;
        updated.total_items = 200;
        updated.total_runs = 200;
        updated.total_failures = 3;
        updated.reconciled_euros = 10.0;
        updated.reserved_euros = 0.0;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!((result.total_euros - 10.0).abs() < f64::EPSILON);
        assert_eq!(result.total_items, 200);
        assert_eq!(result.total_runs, 200);
        assert_eq!(result.total_failures, 3);
        assert!((result.reconciled_euros - 10.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_cost_ledger_daily() {
        let repo = setup_test_repo().await;
        let item = create_test_cost_ledger_daily();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_aggregate_daily_spending_empty() {
        let repo = setup_test_repo().await;
        let date = NaiveDate::from_ymd_opt(2024, 7, 15).unwrap();

        let totals = repo
            .aggregate_daily_spending("test_space", date)
            .await
            .unwrap();

        assert!((totals.spent_retrieval - 0.0).abs() < f64::EPSILON);
        assert!((totals.spent_graph - 0.0).abs() < f64::EPSILON);
        assert_eq!(totals.items_retrieval, 0);
        assert_eq!(totals.items_graph, 0);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_aggregate_daily_spending_with_data() {
        let repo = setup_test_repo().await;
        let date = NaiveDate::from_ymd_opt(2024, 7, 15).unwrap();

        // Create retrieval mode entries
        let mut item1 = create_test_cost_ledger_daily();
        item1.mode = IngestMode::Retrieval;
        item1.reconciled_euros = 5.0;
        item1.reserved_euros = 0.5;
        item1.total_items = 10;
        repo.create(item1).await.unwrap();

        let mut item2 = create_test_cost_ledger_daily();
        item2.base.id = RecordId::new_v4(TableName::CostLedgerDaily);
        item2.mode = IngestMode::Retrieval;
        item2.reconciled_euros = 3.0;
        item2.reserved_euros = 0.25;
        item2.total_items = 5;
        repo.create(item2).await.unwrap();

        // Create graph mode entries
        let mut item3 = create_test_cost_ledger_daily();
        item3.base.id = RecordId::new_v4(TableName::CostLedgerDaily);
        item3.mode = IngestMode::Graph;
        item3.reconciled_euros = 20.0;
        item3.reserved_euros = 1.0;
        item3.total_items = 2;
        repo.create(item3).await.unwrap();

        let totals = repo
            .aggregate_daily_spending("test_space", date)
            .await
            .unwrap();

        // Retrieval: (5.0 + 0.5) + (3.0 + 0.25) = 8.75 euros, 10 + 5 = 15 items
        assert!((totals.spent_retrieval - 8.75).abs() < f64::EPSILON);
        assert_eq!(totals.items_retrieval, 15);

        // Graph: 20.0 + 1.0 = 21.0 euros, 2 items
        assert!((totals.spent_graph - 21.0).abs() < f64::EPSILON);
        assert_eq!(totals.items_graph, 2);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_aggregate_daily_spending_different_dates() {
        let repo = setup_test_repo().await;
        let date1 = NaiveDate::from_ymd_opt(2024, 7, 15).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 7, 16).unwrap();

        // Create entry for date1
        let mut item1 = create_test_cost_ledger_daily();
        item1.date = date1;
        item1.mode = IngestMode::Retrieval;
        item1.reconciled_euros = 10.0;
        item1.reserved_euros = 2.0;
        item1.total_items = 20;
        repo.create(item1).await.unwrap();

        // Create entry for date2
        let mut item2 = create_test_cost_ledger_daily();
        item2.base.id = RecordId::new_v4(TableName::CostLedgerDaily);
        item2.date = date2;
        item2.mode = IngestMode::Retrieval;
        item2.reconciled_euros = 15.0;
        item2.reserved_euros = 4.0;
        item2.total_items = 30;
        repo.create(item2).await.unwrap();

        // Query date1 only
        let totals = repo
            .aggregate_daily_spending("test_space", date1)
            .await
            .unwrap();
        assert!((totals.spent_retrieval - 12.0).abs() < f64::EPSILON);
        assert_eq!(totals.items_retrieval, 20);

        // Query date2 only
        let totals = repo
            .aggregate_daily_spending("test_space", date2)
            .await
            .unwrap();
        assert!((totals.spent_retrieval - 19.0).abs() < f64::EPSILON);
        assert_eq!(totals.items_retrieval, 30);
    }
}
