#![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

use super::*;
use crate::scheduler::budget::CostLedgerDaily;
use elephant_storage::{Repository, StorageClient};
use elephant_types::{
    records::{base::BaseFields, ingest::Budget},
    RecordId, TableName,
};

fn row(space: &str, mode: IngestMode, date: NaiveDate, units: f64) -> CostLedgerDaily {
    CostLedgerDaily {
        base: BaseFields::new(
            RecordId::new_v4(TableName::CostLedgerDaily),
            space.into(),
            "fixture".into(),
        ),
        date,
        mode,
        worker_kind: "fixture".into(),
        total_euros: units,
        total_items: 2,
        total_tokens: 0,
        total_runs: 0,
        total_failures: 0,
        reserved_euros: units,
        reconciled_euros: 0.0,
    }
}

#[tokio::test]
async fn inspection_uses_guard_once_and_isolates_space_and_day() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let enforcer = BudgetEnforcer::new(storage, Budget::default(), "alpha".into());
    let today = Utc::now().date_naive();
    for (space, mode, date, units) in [
        ("alpha", IngestMode::Graph, today, 31.0),
        ("alpha", IngestMode::Retrieval, today, 3.0),
        ("beta", IngestMode::Graph, today, 900.0),
        ("alpha", IngestMode::Graph, today.pred_opt().unwrap(), 800.0),
    ] {
        enforcer
            .ledger_repo
            .create(row(space, mode, date, units))
            .await
            .unwrap();
    }
    let mut guard = row("alpha", IngestMode::Graph, today, 4.0);
    guard.base.id = enforcer.budget_guard_id(today, IngestMode::Graph);
    guard.worker_kind = "__budget_guard__".into();
    guard.reconciled_euros = 2.0;
    guard.total_items = 7;
    enforcer.ledger_repo.create(guard).await.unwrap();
    let before = enforcer
        .ledger_repo
        .get("alpha", &enforcer.budget_guard_id(today, IngestMode::Graph))
        .await
        .unwrap()
        .unwrap();
    let result = enforcer.inspect().await.unwrap();
    assert_eq!(result.graph.usage_source, "budget_guard");
    assert_eq!(result.graph.used_units, 6.0);
    assert_eq!(result.graph.remaining_units, 44.0);
    assert_eq!(result.graph.used_items, 7);
    assert_eq!(result.graph.remaining_items, Some(9993));
    assert_eq!(result.retrieval.usage_source, "ledger");
    assert_eq!(result.retrieval.used_units, 3.0);
    assert_eq!(result.retrieval.remaining_items, Some(998));
    assert_eq!(result.resets_at.date_naive(), today.succ_opt().unwrap());
    let after = enforcer
        .ledger_repo
        .get("alpha", &before.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
}

#[tokio::test]
async fn inspection_reports_real_reservations_and_reconciliation() {
    let storage = StorageClient::new_memory().await.unwrap();
    storage
        .query("DEFINE TABLE cost_ledger_daily SCHEMALESS;")
        .await
        .unwrap();
    let enforcer = BudgetEnforcer::new(storage, Budget::default(), "alpha".into());
    let ledger = enforcer
        .reserve_cost(IngestMode::Graph, "fixture", 4.0)
        .await
        .unwrap();
    assert_eq!(enforcer.inspect().await.unwrap().graph.used_units, 4.0);
    enforcer
        .reconcile_cost(&ledger, 2.0, 2_000_000, true)
        .await
        .unwrap();
    let result = enforcer.inspect().await.unwrap();
    assert_eq!(result.graph.used_units, 2.0);
    assert_eq!(result.graph.remaining_units, 48.0);
    assert_eq!(result.graph.used_items, 1);
}

#[test]
fn inspection_headroom_saturates_and_invalid_values_fail_closed() {
    let result = ModeBudgetInspection::new(1.0, Some(2), 3.0, 4, "ledger").unwrap();
    assert_eq!(result.remaining_units, 0.0);
    assert_eq!(result.remaining_items, Some(0));
    assert_eq!(
        ModeBudgetInspection::new(0.0, None, 0.0, 0, "ledger")
            .unwrap()
            .remaining_items,
        None
    );
    for value in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(ModeBudgetInspection::new(value, Some(2), 0.0, 0, "ledger").is_err());
        assert!(ModeBudgetInspection::new(1.0, Some(2), value, 0, "ledger").is_err());
    }
}
