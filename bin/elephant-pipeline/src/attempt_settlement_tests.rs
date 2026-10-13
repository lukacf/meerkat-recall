//! Faults are injected into persistence only; the production controller and
//! original-claim guard are used unchanged. No model/provider is invoked.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::large_futures)]

use super::*;
use std::cell::Cell;

use elephant_pipeline::scheduler::execution_contract::{AttemptUsage, FailureClass, WorkFailure};
use elephant_types::records::{BaseFields, WorkItemStatus};
use elephant_types::{RecordId, TableName};

const CONFLICT: &str = "The query was not executed due to a failed transaction. Failed to commit transaction due to a read or write conflict. This transaction can be retried";

fn conflict() -> anyhow::Error {
    StorageError::QueryError(CONFLICT.into()).into()
}

fn distant_deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

#[tokio::test]
async fn conflicts_retry_same_captured_outcome_and_unknown_usage() {
    let attempts = Cell::new(0);
    let original = serde_json::json!({
        "run": "original-run", "success": false, "error": "original provider failure",
        "completed_at": "2026-10-13T11:22:50Z",
        "failure_class": "pending_dependencies", "action": {"blocked": {"delay_secs": 5}},
        "usage": {"basis": "unknown", "reserved_tokens": 1234, "measured_tokens": null}
    });
    let returned = retry_persistence(
        distant_deadline(),
        || async { Ok(()) },
        || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 3 {
                return Err(conflict());
            }
            Ok(original.clone())
        },
        |_| Duration::ZERO,
    )
    .await
    .expect("same outcome eventually persists");
    assert_eq!(attempts.get(), 3);
    assert_eq!(
        returned, original,
        "no new outcome or estimated consumption"
    );
}

#[tokio::test]
async fn persistent_conflicts_stop_without_an_execution_retry() {
    let attempts = Cell::new(0);
    let result = retry_persistence(
        distant_deadline(),
        || async { Ok(()) },
        || async {
            attempts.set(attempts.get() + 1);
            Err::<(), _>(conflict())
        },
        |_| Duration::ZERO,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(attempts.get(), MAX_ATTEMPTS);
}

#[tokio::test]
async fn changed_authority_stops_before_second_persistence() {
    let guard_calls = Cell::new(0);
    let writes = Cell::new(0);
    let result = retry_persistence(
        distant_deadline(),
        || async {
            guard_calls.set(guard_calls.get() + 1);
            if guard_calls.get() > 1 {
                return Err(stale_attempt());
            }
            Ok(())
        },
        || async {
            writes.set(writes.get() + 1);
            Err::<(), _>(conflict())
        },
        |_| Duration::ZERO,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(writes.get(), 1, "a successor cannot inherit continuation");
}

#[tokio::test]
async fn expired_shared_deadline_does_not_start_another_phase() {
    let writes = Cell::new(0);
    let deadline = Instant::now();
    for _ in 0..2 {
        assert!(retry_persistence(
            deadline,
            || async { Ok(()) },
            || async {
                writes.set(writes.get() + 1);
                Ok(())
            },
            |_| Duration::ZERO,
        )
        .await
        .is_err());
    }
    assert_eq!(writes.get(), 0);
}

#[tokio::test]
async fn nonretryable_errors_and_conflict_words_fail_closed() {
    let errors = vec![
        StorageError::ErasureInProgress.into(),
        StorageError::DataIntegrityError(CONFLICT.into()).into(),
        StorageError::Conflict("stale_attempt".into()).into(),
        StorageError::ConnectionError(CONFLICT.into()).into(),
        PipelineError::InvalidInput(CONFLICT.into()).into(),
        WorkFailure {
            class: FailureClass::Terminal,
            message: CONFLICT.into(),
        }
        .into(),
        WorkFailure {
            class: FailureClass::StaleInput,
            message: CONFLICT.into(),
        }
        .into(),
        anyhow::anyhow!(CONFLICT),
    ];
    for error in errors {
        assert!(!retryable_conflict(&error));
    }
    let writes = Cell::new(0);
    assert!(retry_persistence(
        distant_deadline(),
        || async { Ok(()) },
        || async {
            writes.set(writes.get() + 1);
            Err::<(), _>(StorageError::ErasureInProgress.into())
        },
        |_| Duration::ZERO,
    )
    .await
    .is_err());
    assert_eq!(writes.get(), 1);
    assert!(retryable_conflict(
        &PipelineError::StorageError(StorageError::QueryError(CONFLICT.into())).into()
    ));
}

async fn claimed(storage: &StorageClient) -> WorkItem {
    let items = WorkItemRepository::new(storage.clone());
    let item = items
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "settlement".into(),
                "oracle".into(),
            ),
            kind: "ingest_metadata".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .expect("original work");
    items
        .claim_with_lease(
            "settlement",
            &item.base.id,
            "original",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim query")
        .expect("claim")
}

#[tokio::test]
async fn original_expiry_is_not_extended_by_a_newer_lease_snapshot() {
    let storage = StorageClient::new_embedded()
        .await
        .expect("embedded fixture");
    let original = claimed(&storage).await;
    let fence =
        elephant_storage::work_attempt_repository::fence_from_claim(&original).expect("fence");
    let after_original_expiry =
        original.lease_expires_at.expect("expiry") + chrono::Duration::seconds(1);
    let mut renewed = original.clone();
    renewed.lease_expires_at = Some(after_original_expiry + chrono::Duration::minutes(5));
    assert!(!same_live_claim(
        &original,
        &fence,
        &renewed,
        after_original_expiry
    ));
    assert!(same_live_claim(&original, &fence, &original, Utc::now()));
}

#[tokio::test]
async fn real_queue_successor_cannot_receive_original_settlement() {
    let storage = StorageClient::new_embedded()
        .await
        .expect("embedded fixture");
    let original = claimed(&storage).await;
    let continuation = AttemptSettlement::new(&storage, &original).expect("original continuation");
    let items = WorkItemRepository::new(storage.clone());
    items
        .requeue_with_delay(
            "settlement",
            &original.base.id,
            original.leased_by.as_deref().expect("owner"),
            0,
            None,
        )
        .await
        .expect("release original")
        .expect("original owner");
    let successor = items
        .claim_with_lease(
            "settlement",
            &original.base.id,
            "successor",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("successor query")
        .expect("successor claim");
    let writes = Cell::new(0);
    assert!(continuation
        .persist("test_original_outcome", || async {
            writes.set(writes.get() + 1);
            Ok(())
        })
        .await
        .is_err());
    assert_eq!(writes.get(), 0);
    assert_eq!(
        items
            .get("settlement", &original.base.id)
            .await
            .expect("current")
            .expect("item"),
        successor
    );
}

#[tokio::test]
async fn one_continuation_deadline_covers_every_persistence_phase() {
    let storage = StorageClient::new_embedded()
        .await
        .expect("embedded fixture");
    let original = claimed(&storage).await;
    let mut continuation = AttemptSettlement::new(&storage, &original).expect("continuation");
    let writes = Cell::new(0);
    continuation
        .persist("first_phase", || async {
            writes.set(writes.get() + 1);
            Ok(())
        })
        .await
        .expect("first phase");
    // Exhaust this exact controller's budget without a wall-clock latency test.
    // A second phase must use the retained deadline, not construct a new one.
    continuation.deadline = Instant::now();
    assert!(continuation
        .persist("second_phase", || async {
            writes.set(writes.get() + 1);
            Ok(())
        })
        .await
        .is_err());
    assert_eq!(writes.get(), 1);
}

#[tokio::test]
async fn successful_run_survives_settlement_conflicts_and_completes_once() {
    use elephant_pipeline::scheduler::work_queue::WorkQueueConfig;
    use elephant_pipeline::scheduler::{BudgetEnforcer, WorkQueue};
    use elephant_storage::{
        cost_ledger_daily_repository::CostLedgerDailyRepository, WorkRunRepository,
    };
    use elephant_types::records::ingest::{Budget, IngestMode};
    use elephant_types::records::{WorkRun, WorkRunMetrics};
    use elephant_types::Link;

    let storage = StorageClient::new_embedded()
        .await
        .expect("embedded fixture");
    let original = claimed(&storage).await;
    let ledger = BudgetEnforcer::new(storage.clone(), Budget::default(), "settlement".into())
        .reserve_cost(IngestMode::Metadata, "ingest_metadata", 0.0)
        .await
        .expect("reservation");
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let mut metrics = WorkRunMetrics::default();
    AttemptUsage::reserved(0, 0.0)
        .write_metrics(&mut metrics)
        .expect("original usage");
    metrics.custom.insert(
        "attempt_fence".into(),
        original.payload["_attempt_fence"].clone(),
    );
    metrics.custom.insert(
        "cost_ledger_id".into(),
        serde_json::json!(ledger.to_string()),
    );
    let runs = WorkRunRepository::new(storage.clone());
    runs.create(WorkRun {
        base: BaseFields::new(run_id.clone(), "settlement".into(), "oracle".into()),
        work_item: Link::new(original.base.id.clone()),
        attempt: original.attempts,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics,
        worker_id: "original".into(),
        worker_version: "oracle".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    })
    .await
    .expect("durable successful outcome before settlement");
    let continuation = AttemptSettlement::new(&storage, &original).expect("continuation");
    let attempts = Cell::new(0);
    continuation
        .persist("settle_successful_usage", || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 3 {
                return Err(conflict());
            }
            Ok(elephant_pipeline::provider_usage::reconcile_finished_usage(
                &storage,
                "settlement",
                &run_id,
            )
            .await?)
        })
        .await
        .expect("same successful receipt settles");
    continuation
        .persist("settle_already_recorded_usage", || async {
            Ok(elephant_pipeline::provider_usage::reconcile_finished_usage(
                &storage,
                "settlement",
                &run_id,
            )
            .await?)
        })
        .await
        .expect("idempotent settlement");
    WorkQueue::new(storage.clone(), WorkQueueConfig::default())
        .for_attempt(&original)
        .expect("original owner")
        .complete("settlement", &original.base.id)
        .await
        .expect("original completion");
    assert_eq!(attempts.get(), 3);
    assert_eq!(
        runs.find_by_work_item(&original.base.id)
            .await
            .expect("runs")
            .len(),
        1
    );
    let total = CostLedgerDailyRepository::new(storage.clone())
        .get("settlement", &ledger)
        .await
        .expect("ledger")
        .expect("reservation");
    assert_eq!(total.total_runs, 1);
    assert_eq!(total.total_failures, 0);
    assert_eq!(
        WorkItemRepository::new(storage)
            .get("settlement", &original.base.id)
            .await
            .expect("item")
            .expect("work")
            .status,
        WorkItemStatus::Done
    );
}

#[tokio::test]
async fn failed_bookkeeping_retries_keep_original_cause_time_and_disposition() {
    use elephant_pipeline::scheduler::execution_contract::RetryAction;
    use elephant_pipeline::scheduler::BudgetEnforcer;
    use elephant_storage::{
        cost_ledger_daily_repository::CostLedgerDailyRepository, WorkRunRepository,
    };
    use elephant_types::records::ingest::{Budget, IngestMode};
    use elephant_types::records::{WorkRun, WorkRunMetrics};
    use elephant_types::Link;

    let storage = StorageClient::new_embedded()
        .await
        .expect("embedded fixture");
    let original = claimed(&storage).await;
    let ledger = BudgetEnforcer::new(storage.clone(), Budget::default(), "settlement".into())
        .reserve_cost(IngestMode::Metadata, "ingest_metadata", 0.0)
        .await
        .expect("reservation");
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let mut metrics = WorkRunMetrics::default();
    let usage = AttemptUsage::reserved(0, 0.0);
    usage.write_metrics(&mut metrics).expect("usage");
    metrics.custom.insert(
        "attempt_fence".into(),
        original.payload["_attempt_fence"].clone(),
    );
    metrics.custom.insert(
        "cost_ledger_id".into(),
        serde_json::json!(ledger.to_string()),
    );
    let completed_at = Utc::now();
    let runs = WorkRunRepository::new(storage.clone());
    runs.create(WorkRun {
        base: BaseFields::new(run_id.clone(), "settlement".into(), "oracle".into()),
        work_item: Link::new(original.base.id.clone()),
        attempt: original.attempts,
        started_at: completed_at,
        completed_at: None,
        success: false,
        error: None,
        metrics,
        worker_id: "original".into(),
        worker_version: "oracle".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    })
    .await
    .expect("original run");
    let error = anyhow::Error::new(WorkFailure {
        class: FailureClass::PendingDependencies,
        message: "original predecessor is unfinished".into(),
    });
    let config = super::super::config::WorkQueueWorkerConfig::default();
    let continuation = AttemptSettlement::new(&storage, &original).expect("continuation");
    let attempts = Cell::new(0);
    let (class, action) = continuation
        .persist("record_failed_outcome_and_usage", || async {
            attempts.set(attempts.get() + 1);
            let disposition = super::super::source_runtime::record_failed_execution(
                &storage,
                &original,
                &run_id,
                &error,
                &usage,
                completed_at,
                123,
                &config,
            )
            .await?;
            // Model a later persistence conflict after the receipt/ledger commit.
            if attempts.get() < 3 {
                return Err(conflict());
            }
            Ok(disposition)
        })
        .await
        .expect("failed outcome continuation");
    assert_eq!(class, FailureClass::PendingDependencies);
    assert_eq!(action, RetryAction::Blocked { delay_secs: 5 });
    let recorded = runs
        .get("settlement", &run_id)
        .await
        .expect("receipt")
        .expect("run");
    assert_eq!(recorded.completed_at, Some(completed_at));
    assert_eq!(recorded.error.as_deref(), Some(error.to_string().as_str()));
    assert_eq!(recorded.metrics.wall_time_ms, 123);
    assert_eq!(
        recorded.metrics.custom["failure_class"],
        serde_json::json!("pending_dependencies")
    );
    assert_eq!(
        AttemptUsage::from_metrics(&recorded.metrics)
            .expect("usage shape")
            .expect("usage"),
        usage
    );
    super::super::source_runtime::apply_failed_disposition(
        &storage, &original, &error, class, action,
    )
    .await
    .expect("original queue disposition");
    let item = WorkItemRepository::new(storage.clone())
        .get("settlement", &original.base.id)
        .await
        .expect("queue")
        .expect("item");
    assert_eq!(item.status, WorkItemStatus::Runnable);
    assert_eq!(
        item.attempts, 0,
        "dependency polling refunds only original attempt"
    );
    assert_eq!(
        runs.find_by_work_item(&original.base.id)
            .await
            .expect("runs")
            .len(),
        1
    );
    let total = CostLedgerDailyRepository::new(storage)
        .get("settlement", &ledger)
        .await
        .expect("ledger")
        .expect("reservation");
    assert_eq!(total.total_runs, 1);
    assert_eq!(total.total_failures, 1);
    assert_eq!(attempts.get(), 3);
}
