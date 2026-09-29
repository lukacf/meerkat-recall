//! Real-engine queue plans and an explicitly opt-in growing-history regression.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::time::{Duration, Instant};

use super::*;
use elephant_types::TableName;
use elephant_types::{records::WorkEdge, records::WorkRun, records::WorkRunMetrics, Link};
use serde_json::json;

const INDEXES: &str = include_str!("../../migrations/sql/028_work_queue_indexes.surql");
const ARCHIVE_INDEXES: &str = include_str!("../../migrations/sql/029_work_history_archives.surql");

async fn indexed_client() -> StorageClient {
    let client = StorageClient::new_memory().await.expect("Surreal memory");
    client.query_checked(INDEXES).await.expect("queue indexes");
    client
        .query_checked(INDEXES)
        .await
        .expect("idempotent queue indexes");
    client
        .query_checked(ARCHIVE_INDEXES)
        .await
        .expect("archive indexes");
    client
}

fn item(sequence: u128, status: WorkItemStatus) -> WorkItem {
    let id = RecordId::new(TableName::WorkItem, uuid::Uuid::from_u128(sequence));
    WorkItem {
        base: BaseFields::new(id, "queue-space".to_string(), "queue-test".to_string()),
        kind: "extract_assertions".to_string(),
        payload: json!({"source_id": "source:00000000-0000-0000-0000-000000000001"}),
        status,
        priority: 1,
        idempotency_key: format!("queue-{sequence}"),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

async fn insert_items(client: &StorageClient, items: Vec<WorkItem>) {
    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        let surrealdb::sql::Value::Object(mut row) =
            WorkItemRepository::work_item_to_value(&item).expect("work value")
        else {
            panic!("work value must be an object");
        };
        row.insert(
            "id".to_string(),
            crate::surreal::thing_from_record_id(&item.base.id).into(),
        );
        rows.push(surrealdb::sql::Value::Object(row));
    }
    client
        .query_bind_checked(
            "INSERT INTO work_item $rows RETURN NONE",
            crate::bindings! { "rows" => surrealdb::sql::Value::Array(rows.into()) },
        )
        .await
        .expect("insert fixture");
}

#[derive(Debug, Deserialize)]
struct ExplainStep {
    operation: String,
    detail: ExplainDetail,
}

#[derive(Debug, Deserialize)]
struct ExplainDetail {
    plan: Option<ExplainIndex>,
    #[serde(rename = "reason")]
    _reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExplainIndex {
    index: String,
}

async fn assert_plan(client: &StorageClient, name: &str, query: &str, indexes: &[&str]) {
    let mut result = client
        .query_bind(
            &format!("{query} EXPLAIN"),
            crate::bindings! {
                "space_id" => "queue-space",
                "space" => "queue-space",
                "running_status" => "running",
                "failed_status" => "failed",
                "open_work" => Vec::<String>::new(),
                "status" => "runnable",
                "kind" => "extract_assertions",
                "key" => "queue-1",
                "source_id" => "source:00000000-0000-0000-0000-000000000001",
                "statuses" => vec!["runnable", "running"],
                "kinds" => vec!["extract_assertions"],
                "cutoff" => crate::surreal::datetime_from_chrono(Utc::now()),
                "work_item_id" => crate::surreal::thing_from_record_id(&item(1, WorkItemStatus::Done).base.id),
                "item_id" => crate::surreal::thing_from_record_id(&item(1, WorkItemStatus::Done).base.id),
            },
        )
        .await
        .expect("explain queue query");
    let plan: Vec<ExplainStep> = result.take(0).expect("explain projection");
    println!("queue plan {name}: {plan:?}");
    assert!(
        !plan.iter().any(|step| step.operation == "Iterate Table"),
        "{name} scans a table: {plan:?}"
    );
    assert!(
        plan.iter().any(|step| {
            step.operation == "Iterate Index"
                && step
                    .detail
                    .plan
                    .as_ref()
                    .is_some_and(|plan| indexes.contains(&plan.index.as_str()))
        }),
        "{name} must use one of {indexes:?}: {plan:?}"
    );
}

async fn assert_queue_plans(client: &StorageClient) {
    let queries = [
        (
            "global claim",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE {RUNNABLE_WORK_FILTER} ORDER BY priority DESC, created_at ASC LIMIT 10000"),
            vec!["work_item_status"],
        ),
        (
            "space status kind",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $status AND kind = $kind AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC LIMIT 100"),
            vec!["work_item_space_status_kind"],
        ),
        (
            "global preferred kind",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE status = $status AND kind = $kind AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC LIMIT 100"),
            vec!["work_item_status_kind"],
        ),
        (
            "space status",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC LIMIT 100"),
            vec!["work_item_space_status_kind"],
        ),
        (
            "expired lease recovery",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $running_status AND deleted_at IS NONE ORDER BY priority DESC, created_at ASC"),
            vec!["work_item_space_status_kind"],
        ),
        (
            "failed recovery hot selector",
            "SELECT * FROM work_item WHERE space_id = $space AND status = $failed_status AND deleted_at IS NONE AND kind = $kind ORDER BY priority DESC, created_at ASC LIMIT 100".to_string(),
            vec!["work_item_space_status_kind"],
        ),
        (
            "failed recovery cold selector",
            "SELECT * FROM work_item_archive WHERE space_id = $space AND record.status = $failed_status AND deleted_at IS NONE AND record.kind = $kind ORDER BY record.priority DESC, record.created_at ASC LIMIT 100".to_string(),
            vec!["work_item_archive_space_status_kind"],
        ),
        (
            "idempotency",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND idempotency_key = $key AND deleted_at IS NONE LIMIT 1"),
            vec!["work_item_space_idempotency"],
        ),
        (
            "source lookup",
            format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status INSIDE $statuses AND kind INSIDE $kinds AND payload.source_id = $source_id AND deleted_at IS NONE AND array::any([status != 'runnable', run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC"),
            vec!["work_item_space_source_status_kind"],
        ),
        ("running count", RUNNING_COUNT_QUERY.to_string(), vec!["work_item_space_status_kind"]),
        ("runnable count", RUNNABLE_COUNT_QUERY.to_string(), vec!["work_item_status"]),
        ("run receipts", "SELECT * FROM work_run WHERE work_item = $work_item_id AND deleted_at IS NONE ORDER BY attempt ASC".to_string(), vec!["work_run_item"]),
        ("run retention", "SELECT * FROM work_run WHERE completed_at < $cutoff AND deleted_at IS NONE ORDER BY completed_at, id LIMIT 100".to_string(), vec!["work_run_completed"]),
        ("item retention", format!("SELECT {WORK_ITEM_PROJECTION} FROM work_item WITH INDEX work_item_updated WHERE status IN ['done','failed','cancelled'] AND updated_at < $cutoff AND deleted_at IS NONE AND leased_by IS NONE AND lease_expires_at IS NONE AND kind NOT IN ['managed_wiki_registration','managed_wiki_registration_lock'] AND array::all([id NOT IN $open_work, IF id IS NONE THEN true ELSE string::concat(meta::tb(id), ':', meta::id(id)) NOT IN $open_work END]) ORDER BY updated_at, id LIMIT 100"), vec!["work_item_updated"]),
        ("open work attempts", "SELECT VALUE work_item FROM work_run WHERE completed_at IS NONE".to_string(), vec!["work_run_completed"]),
        ("incoming dependencies", "SELECT * FROM work_edge WHERE to_item = $item_id AND deleted_at IS NONE".to_string(), vec!["work_edge_to_item"]),
        ("outgoing dependencies", "SELECT * FROM work_edge WHERE from_item = $item_id AND deleted_at IS NONE".to_string(), vec!["work_edge_from_item"]),
    ];
    for (name, query, indexes) in queries {
        assert_plan(client, name, &query, &indexes).await;
    }
}

#[tokio::test]
async fn production_queue_queries_use_indexes() {
    let client = StorageClient::new_memory().await.expect("Surreal memory");
    // Existing duplicate keys must not prevent this additive migration.
    let first = item(1, WorkItemStatus::Runnable);
    let mut duplicate = item(2, WorkItemStatus::Runnable);
    duplicate.idempotency_key.clone_from(&first.idempotency_key);
    insert_items(&client, vec![first, duplicate]).await;
    client
        .query_checked(INDEXES)
        .await
        .expect("migration with duplicates");
    client
        .query_checked(INDEXES)
        .await
        .expect("repeat additive migration");
    client
        .query_checked(ARCHIVE_INDEXES)
        .await
        .expect("archive migration for recovery selectors");
    assert_queue_plans(&client).await;
}

#[tokio::test]
async fn indexed_due_predicate_preserves_missing_null_and_scheduled_semantics() {
    let client = indexed_client().await;
    let repo = WorkItemRepository::new(client.clone());
    let now = Utc::now();
    let mut fixtures = (1..=5)
        .map(|n| item(n, WorkItemStatus::Runnable))
        .collect::<Vec<_>>();
    fixtures[2].run_after = Some(now - chrono::Duration::hours(1));
    fixtures[3].run_after = Some(now + chrono::Duration::hours(1));
    fixtures[4].base.deleted_at = Some(now);
    insert_items(&client, fixtures).await;
    client.query_bind_checked(
        "UPDATE $id SET run_after = NULL",
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(&item(2, WorkItemStatus::Runnable).base.id) },
    ).await.expect("legacy explicit null schedule");

    let mut original = client.query(
        "SELECT count() AS count FROM work_item WHERE status = 'runnable' AND deleted_at IS NONE AND (run_after IS NONE OR run_after <= time::now()) GROUP ALL",
    ).await.expect("original predicate");
    let original: Vec<WorkCount> = original.take(0).expect("original count");
    let count = repo.count_runnable().await.expect("indexed count");
    assert_eq!(u64::try_from(count).expect("count"), original[0].count);
    assert_eq!(
        repo.find_runnable_all_spaces(10)
            .await
            .expect("claim")
            .len(),
        count
    );
}

async fn exercise_aggregate_counts(client: StorageClient) {
    let repo = WorkItemRepository::new(client);
    assert_eq!(repo.count_runnable().await.expect("empty runnable"), 0);
    assert_eq!(
        repo.count_running_by_kind("queue-space", "extract_assertions")
            .await
            .expect("empty running"),
        0
    );
    let now = Utc::now();
    let mut fixtures = vec![
        item(1, WorkItemStatus::Runnable),
        item(2, WorkItemStatus::Runnable),
        item(3, WorkItemStatus::Runnable),
        item(4, WorkItemStatus::Running),
        item(5, WorkItemStatus::Running),
        item(6, WorkItemStatus::Running),
        item(7, WorkItemStatus::Done),
        item(8, WorkItemStatus::Running),
        item(9, WorkItemStatus::Runnable),
    ];
    fixtures[1].run_after = Some(now + chrono::Duration::hours(1));
    fixtures[2].base.deleted_at = Some(now);
    fixtures[4].kind = "render_doc".to_string();
    fixtures[5].base.space_id = "other-space".to_string();
    fixtures[7].base.deleted_at = Some(now);
    fixtures[8].base.space_id = "other-space".to_string();
    for fixture in fixtures {
        repo.create(fixture).await.expect("create count fixture");
    }
    assert_eq!(repo.count_runnable().await.expect("runnable count"), 2);
    assert_eq!(
        repo.count_running_by_kind("queue-space", "extract_assertions")
            .await
            .expect("running count"),
        1
    );
}

#[tokio::test]
async fn aggregate_queue_counts_preserve_backend_filters() {
    exercise_aggregate_counts(indexed_client().await).await;
    exercise_aggregate_counts(StorageClient::new_embedded().await.expect("embedded")).await;
}

#[derive(Debug)]
struct QueueTimings {
    claim: Duration,
    idempotency: Duration,
    runnable_count: Duration,
}

fn assert_bounded_growth(name: &str, before: Duration, after: Duration) {
    // Explicit scale lanes allow noisy concurrent builds, but reject history scans.
    assert!(
        after <= before * 10 + Duration::from_millis(100),
        "{name} grew with unrelated history: before={before:?} after={after:?}"
    );
}

async fn measure_queue_operations(repo: &WorkItemRepository, history: usize) -> QueueTimings {
    let mut claim = Vec::new();
    let mut enqueue_lookup = Vec::new();
    let mut status = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        assert_eq!(
            repo.find_runnable_all_spaces(10000)
                .await
                .expect("claim candidates")
                .len(),
            32
        );
        claim.push(started.elapsed());
        let started = Instant::now();
        assert!(repo
            .find_by_idempotency_key("queue-space", "queue-1")
            .await
            .expect("enqueue lookup")
            .is_some());
        enqueue_lookup.push(started.elapsed());
        let started = Instant::now();
        assert_eq!(repo.count_runnable().await.expect("status count"), 32);
        status.push(started.elapsed());
    }
    fn median(values: &mut [Duration]) -> Duration {
        values.sort_unstable();
        values[values.len() / 2]
    }
    let timings = QueueTimings {
        claim: median(&mut claim),
        idempotency: median(&mut enqueue_lookup),
        runnable_count: median(&mut status),
    };
    println!("queue timings history={history}: {timings:?}");
    timings
}

/// Explicit expensive lane: more than 70,000 records, never part of unit gates.
#[tokio::test]
#[ignore = "explicit 50k-history queue scale lane"]
async fn queue_indexes_scale_past_fifty_thousand_and_counts_are_exact() {
    let client = indexed_client().await;
    let repo = WorkItemRepository::new(client.clone());
    insert_items(
        &client,
        (1..=32)
            .map(|n| item(n, WorkItemStatus::Runnable))
            .collect(),
    )
    .await;
    let before = measure_queue_operations(&repo, 0).await;
    let started = Instant::now();
    for batch in 0_u128..100 {
        insert_items(
            &client,
            (0..500)
                .map(|n| {
                    let mut historical = item(1000 + batch * 500 + n, WorkItemStatus::Done);
                    historical.kind = "completed-history".to_string();
                    historical.payload = json!({});
                    historical
                })
                .collect(),
        )
        .await;
    }
    println!(
        "queue fixture inserted 50000 terminal rows in {:?}",
        started.elapsed()
    );
    let after = measure_queue_operations(&repo, 50_000).await;
    assert_bounded_growth("claim candidates", before.claim, after.claim);
    assert_bounded_growth("enqueue idempotency", before.idempotency, after.idempotency);
    assert_bounded_growth(
        "runnable aggregate",
        before.runnable_count,
        after.runnable_count,
    );
    assert_queue_plans(&client).await;
    for (base, status) in [
        (100_000_u128, WorkItemStatus::Runnable),
        (200_000_u128, WorkItemStatus::Running),
    ] {
        for batch in 0..21 {
            let start = batch * 500;
            let end = ((batch + 1) * 500).min(10_001);
            insert_items(
                &client,
                (start..end).map(|n| item(base + n, status)).collect(),
            )
            .await;
        }
    }
    assert_eq!(
        repo.count_runnable().await.expect("exact >10k runnable"),
        10_033
    );
    assert_eq!(
        repo.count_running_by_kind("queue-space", "extract_assertions")
            .await
            .expect("exact >10k running"),
        10_001
    );
    println!("queue exact counts: runnable=10033 running=10001 total_fixture=70034");
}

async fn insert_archived_items(client: &StorageClient, items: Vec<WorkItem>) {
    use surrealdb::sql::{Object, Value};

    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        let Value::Object(mut record) =
            WorkItemRepository::work_item_to_value(&item).expect("historical work value")
        else {
            panic!("work value must be an object");
        };
        let original = crate::surreal::thing_from_record_id(&item.base.id);
        record.insert("id".to_string(), original.clone().into());
        let mut row = Object::default();
        row.insert(
            "id".to_string(),
            crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&item.base.id))
                .into(),
        );
        row.insert("space_id".to_string(), item.base.space_id.into());
        row.insert("original_id".to_string(), original.into());
        row.insert(
            "archived_at".to_string(),
            crate::surreal::datetime_from_chrono(Utc::now()).into(),
        );
        row.insert("record".to_string(), record.into());
        rows.push(Value::Object(row));
    }
    client
        .query_bind_checked(
            "INSERT INTO work_item_archive $rows RETURN NONE",
            crate::bindings! { "rows" => Value::Array(rows.into()) },
        )
        .await
        .expect("insert archived fixture");
}

async fn dependent_child(client: &StorageClient, sequence: u128, parent: &RecordId) -> WorkItem {
    let child = WorkItemRepository::new(client.clone())
        .create(item(sequence, WorkItemStatus::Pending))
        .await
        .expect("pending child");
    crate::WorkEdgeRepository::new(client.clone())
        .create(WorkEdge {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkEdge),
                "queue-space".to_string(),
                "queue-test".to_string(),
            ),
            from: Link::new(parent.clone()),
            to: Link::new(child.base.id.clone()),
            edge_type: "depends_on".to_string(),
        })
        .await
        .expect("dependency edge");
    child
}

async fn measure_dependency_promotion(
    client: &StorageClient,
    parent: &RecordId,
    history: usize,
    sequence: u128,
) -> Duration {
    let repo = WorkItemRepository::new(client.clone());
    let mut elapsed = Vec::new();
    let mut idempotency = Vec::new();
    for offset in 0..5 {
        let mut child = dependent_child(client, sequence + offset, parent).await;
        child.status = WorkItemStatus::Runnable;
        let id = child.base.id.clone();
        let started = Instant::now();
        assert!(repo
            .update_if_status_and_dependencies_done(
                "queue-space",
                &id,
                child,
                WorkItemStatus::Pending,
            )
            .await
            .expect("actual production dependency promotion")
            .is_some());
        elapsed.push(started.elapsed());
        let started = Instant::now();
        assert_eq!(
            repo.find_by_idempotency_key("queue-space", "queue-900000")
                .await
                .expect("actual hot and cold idempotency lookup")
                .expect("archived parent remains discoverable")
                .base
                .id,
            *parent
        );
        idempotency.push(started.elapsed());
    }
    elapsed.sort_unstable();
    idempotency.sort_unstable();
    println!(
        "dependency promotion history={history}: median={:?} samples={elapsed:?}",
        elapsed[2]
    );
    println!(
        "cold idempotency history={history}: median={:?} samples={idempotency:?}",
        idempotency[2]
    );
    assert!(
        idempotency[2] < Duration::from_millis(250),
        "cold idempotency lookup scans unrelated history: {idempotency:?}"
    );
    elapsed[2]
}

async fn prepare_failed_recovery_fixture(client: &StorageClient) -> [RecordId; 2] {
    let repo = WorkItemRepository::new(client.clone());
    let mut cold = item(900_100, WorkItemStatus::Failed);
    cold.base.created_at = Utc::now() - chrono::Duration::days(40);
    cold.base.updated_at = cold.base.created_at;
    cold.priority = 20;
    let cold_id = cold.base.id.clone();
    repo.create(cold).await.expect("failed archive candidate");
    assert_eq!(
        crate::work_retention::archive_terminal_work(
            client,
            Utc::now() - chrono::Duration::days(30),
            1,
        )
        .await
        .expect("archive actual failed work")
        .items_archived,
        1
    );
    let mut hot = item(900_101, WorkItemStatus::Failed);
    hot.priority = 10;
    let hot_id = hot.base.id.clone();
    let mut wrong_kind = item(900_102, WorkItemStatus::Failed);
    wrong_kind.kind = "unrelated-failure".to_string();
    insert_items(client, vec![hot, wrong_kind]).await;
    let mut other_space = item(900_103, WorkItemStatus::Failed);
    other_space.base.space_id = "other-space".to_string();
    let mut wrong_kind = item(900_104, WorkItemStatus::Failed);
    wrong_kind.kind = "unrelated-failure".to_string();
    insert_archived_items(client, vec![other_space, wrong_kind]).await;
    [cold_id, hot_id]
}

async fn measure_failed_recovery(
    client: &StorageClient,
    expected: &[RecordId; 2],
    history: usize,
) -> Duration {
    let repo = WorkItemRepository::new(client.clone());
    let mut elapsed = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        let rows = repo
            .find_by_status_and_kind(
                "queue-space",
                WorkItemStatus::Failed,
                "extract_assertions",
                100,
            )
            .await
            .expect("actual maintenance recovery selector");
        elapsed.push(started.elapsed());
        let ids: Vec<_> = rows.into_iter().map(|row| row.base.id).collect();
        assert_eq!(ids.as_slice(), expected.as_slice());
    }
    let limited = repo
        .find_by_status_and_kind(
            "queue-space",
            WorkItemStatus::Failed,
            "extract_assertions",
            1,
        )
        .await
        .expect("bounded recovery selector");
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].base.id, expected[0]);
    elapsed.sort_unstable();
    println!(
        "failed recovery history={history}: median={:?} samples={elapsed:?}",
        elapsed[2]
    );
    elapsed[2]
}

/// Exercises the production correlated promotion query, not a simplified proxy.
#[tokio::test]
#[ignore = "explicit 50k-cold-history dependency scale lane"]
async fn dependency_promotion_stays_bounded_with_fifty_thousand_cold_items() {
    let client = indexed_client().await;
    client
        .query_checked(include_str!(
            "../../migrations/sql/029_work_history_archives.surql"
        ))
        .await
        .expect("archive indexes");
    let repo = WorkItemRepository::new(client.clone());
    let mut parent = item(900_000, WorkItemStatus::Done);
    parent.base.created_at = Utc::now() - chrono::Duration::days(40);
    parent.base.updated_at = parent.base.created_at;
    repo.create(parent.clone()).await.expect("completed parent");
    assert_eq!(
        crate::work_retention::archive_terminal_work(
            &client,
            Utc::now() - chrono::Duration::days(30),
            1,
        )
        .await
        .expect("archive actual parent")
        .items_archived,
        1
    );
    let failed_ids = prepare_failed_recovery_fixture(&client).await;
    let recovery_before = measure_failed_recovery(&client, &failed_ids, 0).await;
    let before = measure_dependency_promotion(&client, &parent.base.id, 0, 900_010).await;
    for batch in 0_u128..100 {
        insert_archived_items(
            &client,
            (0..500)
                .map(|n| item(1000 + batch * 500 + n, WorkItemStatus::Done))
                .collect(),
        )
        .await;
    }
    let recovery_after = measure_failed_recovery(&client, &failed_ids, 50_000).await;
    assert_bounded_growth("failed-work recovery", recovery_before, recovery_after);
    let after = measure_dependency_promotion(&client, &parent.base.id, 50_000, 900_020).await;
    assert!(
        after <= before * 10 + Duration::from_millis(100),
        "point dependency lookup grew with unrelated cold history: before={before:?} after={after:?}"
    );
    let mut missing =
        dependent_child(&client, 900_030, &RecordId::new_v4(TableName::WorkItem)).await;
    missing.status = WorkItemStatus::Runnable;
    let id = missing.base.id.clone();
    assert!(repo
        .update_if_status_and_dependencies_done(
            "queue-space",
            &id,
            missing,
            WorkItemStatus::Pending,
        )
        .await
        .expect("missing parent barrier")
        .is_none());
}

async fn insert_recent_completed_runs(client: &StorageClient, first: u128) {
    use surrealdb::sql::Value;

    let mut rows = Vec::with_capacity(500);
    for n in first..first + 500 {
        let run = WorkRun {
            base: BaseFields::new(
                RecordId::new(TableName::WorkRun, uuid::Uuid::from_u128(n)),
                "queue-space".to_string(),
                "queue-test".to_string(),
            ),
            work_item: Link::new(item(n, WorkItemStatus::Done).base.id),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "queue-test".to_string(),
            worker_version: "1".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        let Value::Object(mut row) =
            crate::WorkRunRepository::work_run_to_value(&run).expect("run value")
        else {
            panic!("run value must be object");
        };
        row.insert(
            "id".to_string(),
            crate::surreal::thing_from_record_id(&run.base.id).into(),
        );
        rows.push(Value::Object(row));
    }
    client
        .query_bind_checked(
            "INSERT INTO work_run $rows RETURN NONE",
            crate::bindings! { "rows" => Value::Array(rows.into()) },
        )
        .await
        .expect("insert completed runs");
}

async fn measure_retention_pass(client: &StorageClient, first: u128, runs: usize) -> Duration {
    let mut samples = Vec::new();
    for sequence in first..first + 5 {
        let mut old = item(sequence, WorkItemStatus::Done);
        old.base.created_at = Utc::now() - chrono::Duration::days(40);
        old.base.updated_at = old.base.created_at;
        WorkItemRepository::new(client.clone())
            .create(old)
            .await
            .expect("old eligible item");
        let started = Instant::now();
        let archived = crate::work_retention::archive_terminal_work(
            client,
            Utc::now() - chrono::Duration::days(30),
            1,
        )
        .await
        .expect("actual bounded archival pass");
        samples.push(started.elapsed());
        assert_eq!(archived.items_archived, 1);
        assert_eq!(archived.runs_archived, 0);
    }
    samples.sort_unstable();
    println!(
        "retention pass unrelated_completed_runs={runs}: median={:?} samples={samples:?}",
        samples[2]
    );
    samples[2]
}

#[tokio::test]
#[ignore = "explicit 50k-completed-attempt retention scale lane"]
async fn retention_pass_stays_bounded_with_fifty_thousand_completed_attempts() {
    let client = indexed_client().await;
    client
        .query_checked(include_str!(
            "../../migrations/sql/029_work_history_archives.surql"
        ))
        .await
        .expect("archive indexes");
    let before = measure_retention_pass(&client, 900_000, 0).await;
    for batch in 0..100 {
        insert_recent_completed_runs(&client, 1000 + batch * 500).await;
    }
    let after = measure_retention_pass(&client, 900_010, 50_000).await;
    assert!(
        after <= before * 10 + Duration::from_millis(100),
        "archival cost grew with unrelated closed attempts: before={before:?} after={after:?}"
    );
}
