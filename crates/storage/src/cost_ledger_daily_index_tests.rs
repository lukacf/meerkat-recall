//! Real-engine plans and isolation checks for the budget read path.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::time::{Duration, Instant};

use super::*;

const INDEXES: &str = include_str!("../../migrations/sql/030_cost_ledger_daily_indexes.surql");

fn day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 28).expect("fixture day")
}

fn ledger(sequence: u128, mode: IngestMode) -> CostLedgerDaily {
    CostLedgerDaily {
        base: BaseFields::new(
            RecordId::new(TableName::CostLedgerDaily, uuid::Uuid::from_u128(sequence)),
            "budget-space".to_string(),
            "budget-test".to_string(),
        ),
        date: day(),
        mode,
        worker_kind: "embed".to_string(),
        total_euros: 3.0,
        total_items: 3,
        total_tokens: 100,
        total_runs: 4,
        total_failures: 1,
        reserved_euros: 1.0,
        reconciled_euros: 2.0,
    }
}

async fn insert_rows(client: &StorageClient, rows: Vec<CostLedgerDaily>) {
    let rows = rows
        .into_iter()
        .map(|record| {
            let surrealdb::sql::Value::Object(mut row) =
                CostLedgerDailyRepository::cost_ledger_daily_to_value(&record)
                    .expect("native ledger row")
            else {
                panic!("ledger must be an object");
            };
            row.insert(
                "id".to_string(),
                crate::surreal::thing_from_record_id(&record.base.id).into(),
            );
            surrealdb::sql::Value::Object(row)
        })
        .collect::<Vec<_>>();
    client
        .query_bind_checked(
            "INSERT INTO cost_ledger_daily $rows RETURN NONE",
            crate::bindings! { "rows" => surrealdb::sql::Value::Array(rows.into()) },
        )
        .await
        .expect("insert native ledgers");
}

#[derive(Debug, Deserialize)]
struct ExplainStep {
    operation: String,
    detail: ExplainDetail,
}

#[derive(Debug, Deserialize)]
struct ExplainDetail {
    plan: Option<ExplainIndex>,
}

#[derive(Debug, Deserialize)]
struct ExplainIndex {
    index: String,
}

async fn assert_budget_plans(client: &StorageClient, id: &RecordId) {
    for (label, query, point_lookup) in [
        ("guard", cost_ledger_daily_lookup_query(), true),
        (
            "retrieval totals",
            RETRIEVAL_DAILY_SPENDING_QUERY.to_string(),
            false,
        ),
        (
            "graph totals",
            GRAPH_DAILY_SPENDING_QUERY.to_string(),
            false,
        ),
    ] {
        let mut response = client
            .query_bind_checked(
                &format!("{query} EXPLAIN"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => "budget-space",
                    "date" => date_to_surreal(day()).expect("native date"),
                    "guard_kind" => BUDGET_GUARD_WORKER_KIND,
                },
            )
            .await
            .expect("budget EXPLAIN");
        let steps: Vec<ExplainStep> = response.take(0).expect("budget plan");
        println!("budget plan {label}: {steps:?}");
        assert!(
            !steps
                .iter()
                .any(|step| step.operation.starts_with("Iterate Table")),
            "{label} scans historical ledgers: {steps:?}"
        );
        if point_lookup {
            assert!(steps.iter().any(|step| step.operation == "Iterate Thing"));
        } else {
            assert!(
                steps.iter().any(|step| {
                    step.operation == "Iterate Index"
                        && step
                            .detail
                            .plan
                            .as_ref()
                            .is_some_and(|plan| plan.index == "cost_ledger_daily_space_date_mode")
                }),
                "{label} must use the daily ledger index: {steps:?}"
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn direct_ledger_reads_preserve_table_space_deletion_and_integrity_boundaries() {
    let client = StorageClient::new_memory().await.expect("Surreal memory");
    let repo = CostLedgerDailyRepository::new(client.clone());
    let row = ledger(1, IngestMode::Retrieval);
    let id = row.base.id.clone();
    repo.create(row.clone()).await.expect("ledger");
    assert_eq!(
        repo.get("budget-space", &id)
            .await
            .expect("point read")
            .expect("present")
            .base
            .id,
        id
    );
    assert!(repo
        .get("other-space", &id)
        .await
        .expect("wrong space")
        .is_none());
    assert!(repo
        .get("budget-space", &ledger(2, IngestMode::Retrieval).base.id)
        .await
        .expect("absent record")
        .is_none());

    // A different table can contain a structurally identical row. It must not
    // become reachable when the query changes from a table to a record target.
    let other_id = RecordId::new(TableName::Entity, id.id());
    client
        .query_bind_checked(
            "CREATE $id CONTENT $row RETURN NONE",
            crate::bindings! {
                "id" => crate::surreal::thing_from_record_id(&other_id),
                "row" => CostLedgerDailyRepository::cost_ledger_daily_to_value(&row).expect("row"),
            },
        )
        .await
        .expect("cross-table lookalike");
    assert!(repo
        .get("budget-space", &other_id)
        .await
        .expect("wrong table")
        .is_none());

    client
        .query_bind_checked(
            "UPDATE $id UNSET total_items RETURN NONE",
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(&id) },
        )
        .await
        .expect("malformed persisted row");
    assert!(repo.get("budget-space", &id).await.is_err());
    repo.delete("budget-space", &id).await.expect("soft delete");
    assert!(repo
        .get("budget-space", &id)
        .await
        .expect("deleted")
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn daily_ledger_index_backfills_and_preserves_exact_aggregate_filters() {
    let client = StorageClient::new_memory().await.expect("Surreal memory");
    let repo = CostLedgerDailyRepository::new(client.clone());
    let mut second = ledger(2, IngestMode::Retrieval);
    second.reserved_euros = 0.5;
    second.reconciled_euros = 4.25;
    second.total_items = 2;
    second.total_runs = 6;
    second.total_failures = 3;
    let mut guard = ledger(4, IngestMode::Retrieval);
    guard.worker_kind = BUDGET_GUARD_WORKER_KIND.to_string();
    let mut deleted = ledger(5, IngestMode::Retrieval);
    deleted.base.deleted_at = Some(Utc::now());
    let mut other_space = ledger(6, IngestMode::Retrieval);
    other_space.base.space_id = "other-space".to_string();
    let mut yesterday = ledger(7, IngestMode::Retrieval);
    yesterday.date = day().pred_opt().expect("previous day");
    let mut tomorrow = ledger(8, IngestMode::Retrieval);
    tomorrow.date = day().succ_opt().expect("next day");
    let mut graph_guard = ledger(11, IngestMode::Graph);
    graph_guard.worker_kind = BUDGET_GUARD_WORKER_KIND.to_string();
    insert_rows(
        &client,
        vec![
            ledger(1, IngestMode::Retrieval),
            second,
            ledger(3, IngestMode::Graph),
            guard,
            deleted,
            other_space,
            yesterday,
            tomorrow,
            ledger(9, IngestMode::Metadata),
            ledger(10, IngestMode::Skip),
            graph_guard,
        ],
    )
    .await;
    // SQL NULL is different from NONE; preserve the existing soft-delete filter.
    let null_deleted = ledger(12, IngestMode::Retrieval);
    repo.create(null_deleted.clone())
        .await
        .expect("null fixture");
    client
        .query_bind_checked(
            "UPDATE $id SET deleted_at = NULL RETURN NONE",
            crate::bindings! {
                "id" => crate::surreal::thing_from_record_id(&null_deleted.base.id),
            },
        )
        .await
        .expect("explicit null");

    client
        .query_checked(INDEXES)
        .await
        .expect("backfill populated ledger");
    client
        .query_checked(INDEXES)
        .await
        .expect("idempotent index migration");
    let totals = repo
        .aggregate_daily_spending("budget-space", day())
        .await
        .expect("indexed totals");
    assert!((totals.spent_retrieval - 7.75).abs() < f64::EPSILON);
    assert_eq!(
        (
            totals.items_retrieval,
            totals.runs_retrieval,
            totals.failures_retrieval
        ),
        (5, 10, 4)
    );
    assert!((totals.spent_graph - 3.0).abs() < f64::EPSILON);
    assert_eq!(
        (totals.items_graph, totals.runs_graph, totals.failures_graph),
        (3, 4, 1)
    );
    let absent = repo
        .aggregate_daily_spending("missing-space", day())
        .await
        .expect("empty indexed bucket");
    assert_eq!((absent.items_retrieval, absent.items_graph), (0, 0));
    assert_budget_plans(&client, &ledger(4, IngestMode::Retrieval).base.id).await;
    assert_budget_plans(&client, &ledger(100, IngestMode::Retrieval).base.id).await;
}

async fn measure_budget_reads(repo: &CostLedgerDailyRepository, history: usize) -> [Duration; 3] {
    let mut timings = [Vec::new(), Vec::new(), Vec::new()];
    for _ in 0..5 {
        let start = Instant::now();
        assert!(repo
            .get("budget-space", &ledger(1, IngestMode::Retrieval).base.id)
            .await
            .expect("present guard")
            .is_some());
        timings[0].push(start.elapsed());
        let start = Instant::now();
        assert!(repo
            .get("budget-space", &ledger(3, IngestMode::Graph).base.id)
            .await
            .expect("missing guard")
            .is_none());
        timings[1].push(start.elapsed());
        let start = Instant::now();
        let totals = repo
            .aggregate_daily_spending("budget-space", day())
            .await
            .expect("daily totals");
        assert_eq!((totals.items_retrieval, totals.items_graph), (0, 3));
        timings[2].push(start.elapsed());
    }
    let medians = timings.map(|mut values| {
        values.sort_unstable();
        values[values.len() / 2]
    });
    println!("ledger timings history={history}: {medians:?}");
    medians
}

#[tokio::test]
#[ignore = "explicit 50k-history budget ledger scale lane"]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn budget_reads_stay_indexed_past_fifty_thousand_historical_ledgers() {
    let client = StorageClient::new_memory().await.expect("Surreal memory");
    client.query_checked(INDEXES).await.expect("ledger index");
    let repo = CostLedgerDailyRepository::new(client.clone());
    let mut guard = ledger(1, IngestMode::Retrieval);
    guard.worker_kind = BUDGET_GUARD_WORKER_KIND.to_string();
    insert_rows(&client, vec![guard, ledger(2, IngestMode::Graph)]).await;
    let before = measure_budget_reads(&repo, 0).await;
    let start = Instant::now();
    for batch in 0_u128..100 {
        let rows = (0..500)
            .map(|offset| {
                let sequence = 1000 + batch * 500 + offset;
                let mut row = ledger(sequence, IngestMode::Retrieval);
                match sequence % 3 {
                    0 => row.date = day().pred_opt().expect("history date"),
                    1 => row.base.space_id = "other-space".to_string(),
                    _ => row.mode = IngestMode::Metadata,
                }
                row
            })
            .collect();
        insert_rows(&client, rows).await;
    }
    println!("inserted 50000 historical ledgers in {:?}", start.elapsed());
    let after = measure_budget_reads(&repo, 50_000).await;
    for (label, (before, after)) in ["present guard", "absent guard", "daily totals"]
        .into_iter()
        .zip(before.into_iter().zip(after))
    {
        assert!(
            after < Duration::from_secs(2),
            "{label} exceeds two seconds: {after:?}"
        );
        assert!(
            after <= before * 10 + Duration::from_millis(30),
            "{label} grows with unrelated history: {before:?} -> {after:?}"
        );
    }
    assert_budget_plans(&client, &ledger(1, IngestMode::Retrieval).base.id).await;
    assert_budget_plans(&client, &ledger(3, IngestMode::Graph).base.id).await;
}
