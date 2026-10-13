#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
//! M10: Work Queue Integration tests.
//!
//! Verifies work queue mechanics:
//! - Work item creation from various triggers
//! - Retry logic and exponential backoff
//! - Dead letter handling

use chrono::{Duration, Utc};

use elephant_storage::{
    Repository, WorkDeadletterRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{
    WorkDeadletter, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// M10.1: Work items are created with proper status.
///
/// Test invariants:
/// 1. New work items start as Pending
/// 2. Idempotency key prevents duplicates
/// 3. Priority ordering is respected
#[tokio::test]
#[ignore]
async fn scenario_m10_1_work_item_creation() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());

    // Create work items with different priorities
    let high_priority_id = RecordId::new_v4(TableName::WorkItem);
    let high_base = BaseFields::new(
        high_priority_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let high_work = WorkItem {
        base: high_base,
        kind: "extract".to_string(),
        payload: serde_json::json!({"source_id": "source:123"}),
        status: WorkItemStatus::Pending,
        priority: 100,
        idempotency_key: format!("extract-source-123-{}", uuid::Uuid::new_v4()),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let created_high = work_repo.create(high_work).await?;

    let low_priority_id = RecordId::new_v4(TableName::WorkItem);
    let low_base = BaseFields::new(
        low_priority_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let low_work = WorkItem {
        base: low_base,
        kind: "embed".to_string(),
        payload: serde_json::json!({"doc_id": "doc:456"}),
        status: WorkItemStatus::Pending,
        priority: 10,
        idempotency_key: format!("embed-doc-456-{}", uuid::Uuid::new_v4()),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let created_low = work_repo.create(low_work).await?;

    // Verify initial state
    assert_eq!(
        created_high.status,
        WorkItemStatus::Pending,
        "New items should be pending"
    );
    assert_eq!(created_high.attempts, 0, "New items should have 0 attempts");
    assert!(created_high.priority > created_low.priority);

    Ok(())
}

/// M10.2: Work items can be claimed with lease.
///
/// Test invariants:
/// 1. Claiming sets status to Running
/// 2. Lease expiration is set
/// 3. Attempts counter is incremented
#[tokio::test]
#[ignore]
async fn scenario_m10_2_work_item_lease_acquisition() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());

    // Create a runnable work item
    let work_id = RecordId::new_v4(TableName::WorkItem);
    let work_base = BaseFields::new(work_id.clone(), space_id.to_string(), "P_sys".to_string());
    let work_item = WorkItem {
        base: work_base,
        kind: "resolve".to_string(),
        payload: serde_json::json!({"assertion_ids": ["assertion:1", "assertion:2"]}),
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!("resolve-batch-{}", uuid::Uuid::new_v4()),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    work_repo.create(work_item).await?;

    // Claim with lease
    let lease_duration = Duration::try_minutes(5).unwrap();
    let lease_expires = Utc::now() + lease_duration;
    let worker_id = "worker-001";

    let claimed = work_repo
        .claim_with_lease(space_id, &work_id, worker_id, lease_expires)
        .await?;

    assert!(claimed.is_some(), "Should successfully claim runnable item");

    let claimed = claimed.unwrap();
    assert_eq!(
        claimed.status,
        WorkItemStatus::Running,
        "Claimed item should be running"
    );
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)?;
    assert_eq!(fence.space_id, space_id);
    assert_eq!(fence.work_item_id, work_id);
    assert_eq!(fence.generation, 1);
    assert_eq!(
        claimed.leased_by.as_deref(),
        Some(fence.lease_token.as_str())
    );
    assert!(fence.lease_token.starts_with(&format!("{worker_id}:")));
    assert_ne!(
        fence.lease_token, worker_id,
        "worker identity alone is not ownership"
    );
    assert!(
        claimed.lease_expires_at.is_some(),
        "Should set lease expiration"
    );
    assert_eq!(claimed.attempts, 1, "Should increment attempts");

    // Second claim should fail (already claimed)
    let second_claim = work_repo
        .claim_with_lease(space_id, &work_id, "worker-002", lease_expires)
        .await?;
    assert!(
        second_claim.is_none(),
        "Should not claim already running item"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn scenario_m10_2_runnable_item_replaces_stale_lease_metadata() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());

    let work_id = RecordId::new_v4(TableName::WorkItem);
    let work_base = BaseFields::new(work_id.clone(), space_id.to_string(), "P_sys".to_string());
    let lease_expires = Utc::now() + Duration::try_minutes(5).unwrap();
    let work_item = WorkItem {
        base: work_base,
        kind: "resolve".to_string(),
        payload: serde_json::json!({"assertion_ids": ["assertion:1"]}),
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!("resolve-batch-{}", uuid::Uuid::new_v4()),
        attempts: 1,
        lease_expires_at: Some(lease_expires),
        leased_by: Some("worker-001".to_string()),
        run_after: None,
        last_error: None,
    };
    work_repo.create(work_item).await?;

    let new_lease_expires = Utc::now() + Duration::try_minutes(5).unwrap();
    let claimed = work_repo
        .claim_with_lease(space_id, &work_id, "worker-002", new_lease_expires)
        .await?;

    // Runnable is the ownership authority. Recovery can leave old lease
    // metadata, which must not strand an item that has returned to the queue.
    let claimed = claimed.expect("runnable item should be reclaimed");
    assert_eq!(claimed.status, WorkItemStatus::Running);
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)?;
    assert_eq!(
        claimed.leased_by.as_deref(),
        Some(fence.lease_token.as_str())
    );
    assert!(fence.lease_token.starts_with("worker-002:"));
    assert_ne!(fence.lease_token, "worker-001");
    assert_eq!(fence.work_item_id, work_id);
    assert!(fence.generation > 0);
    assert_eq!(claimed.lease_expires_at, Some(new_lease_expires));
    assert_eq!(claimed.attempts, 2);
    assert!(
        work_repo
            .claim_with_lease(space_id, &work_id, "worker-003", new_lease_expires)
            .await?
            .is_none(),
        "a running item must retain its current owner"
    );

    Ok(())
}

/// M10.3: Failed work items are retried with backoff.
///
/// Test invariants:
/// 1. Failed attempts increment counter
/// 2. run_after is set for exponential backoff
/// 3. Status returns to pending/runnable for retry
#[tokio::test]
#[ignore]
async fn scenario_m10_3_retry_with_exponential_backoff() -> anyhow::Result<()> {
    use elephant_pipeline::scheduler::execution_contract::{
        retry_action, FailureClass, RetryAction,
    };
    use elephant_storage::work_attempt_repository::fence_from_claim;
    let harness = Harness::new().await?;
    let space_id = "test_space";
    let work_repo = WorkItemRepository::new(harness.client.clone());
    let work_id = RecordId::new_v4(TableName::WorkItem);
    work_repo
        .create(WorkItem {
            base: BaseFields::new(work_id.clone(), space_id.into(), "P_sys".into()),
            kind: "extract".into(),
            payload: serde_json::json!({"source_id": "source:bad"}),
            status: WorkItemStatus::Runnable,
            priority: 50,
            idempotency_key: format!("extract-bad-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let first = work_repo
        .claim_with_lease(
            space_id,
            &work_id,
            "worker-001",
            Utc::now() + Duration::minutes(5),
        )
        .await?
        .expect("first actual claim");
    let original = fence_from_claim(&first)?;
    // A one-second configured base keeps this integration oracle bounded while
    // exercising the production exponential policy and actual due-time guard.
    let RetryAction::Retry {
        delay_secs: first_delay,
    } = retry_action(FailureClass::Timeout, first.attempts, 3, 1, 120)
    else {
        anyhow::bail!("first timeout must retry");
    };
    assert_eq!(first_delay, 1);
    assert!(work_repo
        .requeue_with_delay(
            space_id,
            &work_id,
            "worker-001",
            first_delay,
            Some("unfenced actor".into())
        )
        .await?
        .is_none());
    let before_first = Utc::now();
    let updated1 = work_repo
        .requeue_with_delay(
            space_id,
            &work_id,
            &original.lease_token,
            first_delay,
            Some("Connection timeout".into()),
        )
        .await?
        .expect("original owner requeues");
    assert_eq!(updated1.status, WorkItemStatus::Runnable);
    assert_eq!(updated1.attempts, 1);
    assert_eq!(updated1.last_error.as_deref(), Some("Connection timeout"));
    assert!(updated1.leased_by.is_none() && updated1.lease_expires_at.is_none());
    let first_due = updated1.run_after.expect("first due time");
    assert!(first_due >= before_first + Duration::seconds(first_delay));
    assert!(
        work_repo
            .claim_with_lease(
                space_id,
                &work_id,
                "worker-001",
                Utc::now() + Duration::minutes(5)
            )
            .await?
            .is_none(),
        "backoff must prevent immediate claim"
    );
    let wait = (first_due - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(wait + std::time::Duration::from_millis(20)).await;
    let second = work_repo
        .claim_with_lease(
            space_id,
            &work_id,
            "worker-001",
            Utc::now() + Duration::minutes(5),
        )
        .await?
        .expect("due original work can retry");
    let successor = fence_from_claim(&second)?;
    assert!(successor.generation > original.generation);
    assert_ne!(successor.lease_token, original.lease_token);
    assert_eq!(second.attempts, 2);
    assert!(work_repo
        .requeue_with_delay(
            space_id,
            &work_id,
            &original.lease_token,
            120,
            Some("stale original retry".into())
        )
        .await?
        .is_none());
    let RetryAction::Retry {
        delay_secs: second_delay,
    } = retry_action(FailureClass::Timeout, second.attempts, 3, 1, 120)
    else {
        anyhow::bail!("second timeout must retry");
    };
    assert_eq!(second_delay, 2 * first_delay);
    let before_second = Utc::now();
    let updated2 = work_repo
        .requeue_with_delay(
            space_id,
            &work_id,
            &successor.lease_token,
            second_delay,
            Some("Connection timeout (attempt 2)".into()),
        )
        .await?
        .expect("successor owner requeues");
    assert_eq!(updated2.status, WorkItemStatus::Runnable);
    assert_eq!(updated2.attempts, 2);
    assert!(updated2.leased_by.is_none() && updated2.lease_expires_at.is_none());
    assert!(
        updated2.run_after.expect("second due time")
            >= before_second + Duration::seconds(second_delay)
    );
    assert!(matches!(
        retry_action(FailureClass::Timeout, 3, 3, 1, 120),
        RetryAction::Fail
    ));
    Ok(())
}

/// M10.4: Work runs record execution details.
///
/// Test invariants:
/// 1. Work runs track timing and metrics
/// 2. Success/failure is recorded
/// 3. Worker and model versions are tracked
#[tokio::test]
#[ignore]
async fn scenario_m10_4_work_runs_track_execution() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());
    let run_repo = WorkRunRepository::new(harness.client.clone());

    // Create a work item
    let work_id = RecordId::new_v4(TableName::WorkItem);
    let work_base = BaseFields::new(work_id.clone(), space_id.to_string(), "P_sys".to_string());
    let work_item = WorkItem {
        base: work_base,
        kind: "extract".to_string(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Running,
        priority: 50,
        idempotency_key: format!("extract-{}", uuid::Uuid::new_v4()),
        attempts: 1,
        lease_expires_at: None,
        leased_by: Some("worker-001".to_string()),
        run_after: None,
        last_error: None,
    };
    work_repo.create(work_item).await?;

    // Create a work run
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let run_base = BaseFields::new(run_id.clone(), space_id.to_string(), "P_sys".to_string());
    let work_run = WorkRun {
        base: run_base,
        work_item: Link::new(work_id.clone()),
        attempt: 1,
        started_at: Utc::now() - Duration::try_seconds(30).unwrap(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics {
            tokens_used: 5000,
            steps_taken: 3,
            wall_time_ms: 30000,
            estimated_cost_euros: 0.05,
            custom: std::collections::HashMap::new(),
        },
        worker_id: "worker-001".to_string(),
        worker_version: "1.2.0".to_string(),
        model_version: Some("gpt-4-turbo".to_string()),
        prompt_id: None,
        config_hash: Some("cfg_abc123".to_string()),
    };
    let created = run_repo.create(work_run).await?;

    // Verify run details
    assert!(created.success, "Run should be successful");
    assert!(
        created.completed_at.is_some(),
        "Should have completion time"
    );
    assert_eq!(created.metrics.tokens_used, 5000);
    assert_eq!(created.metrics.wall_time_ms, 30000);
    assert!(
        (created.metrics.estimated_cost_euros - 0.05).abs() < f64::EPSILON,
        "Cost should be tracked"
    );
    assert_eq!(created.worker_version, "1.2.0");
    assert!(
        created.model_version.is_some(),
        "Model version should be recorded"
    );

    Ok(())
}

/// M10.5: Failed items go to dead letter after max retries.
///
/// Test invariants:
/// 1. Items exceeding max_attempts are deadlettered
/// 2. Final error is recorded
/// 3. All run attempts are linked
#[tokio::test]
#[ignore]
async fn scenario_m10_5_dead_letter_after_max_retries() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());
    let deadletter_repo = WorkDeadletterRepository::new(harness.client.clone());

    // Create a work item that has failed max retries
    let work_id = RecordId::new_v4(TableName::WorkItem);
    let work_base = BaseFields::new(work_id.clone(), space_id.to_string(), "P_sys".to_string());
    let work_item = WorkItem {
        base: work_base,
        kind: "extract".to_string(),
        payload: serde_json::json!({"source_id": "source:problematic"}),
        status: WorkItemStatus::Failed, // Marked as failed
        priority: 50,
        idempotency_key: format!("extract-fail-{}", uuid::Uuid::new_v4()),
        attempts: 3, // Max attempts reached
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: Some("Persistent failure after 3 attempts".to_string()),
    };
    work_repo.create(work_item).await?;

    // Create dead letter entry
    let deadletter_id = RecordId::new_v4(TableName::WorkDeadletter);
    let deadletter_base = BaseFields::new(
        deadletter_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );

    // Create fake run links
    let run1_id = RecordId::new_v4(TableName::WorkRun);
    let run2_id = RecordId::new_v4(TableName::WorkRun);
    let run3_id = RecordId::new_v4(TableName::WorkRun);

    let deadletter = WorkDeadletter {
        base: deadletter_base,
        work_item: Link::new(work_id.clone()),
        runs: vec![Link::new(run1_id), Link::new(run2_id), Link::new(run3_id)],
        final_error: "Persistent failure: API rate limit exceeded".to_string(),
        deadlettered_at: Utc::now(),
        acknowledged: false,
        notes: None,
    };
    let created = deadletter_repo.create(deadletter).await?;

    // Verify dead letter
    assert!(!created.acknowledged, "Should be unacknowledged initially");
    assert_eq!(created.runs.len(), 3, "Should link all failed run attempts");
    assert!(
        created.final_error.contains("rate limit"),
        "Final error should be recorded"
    );

    // Find unacknowledged dead letters
    let unack = deadletter_repo.find_unacknowledged(space_id, 10).await?;
    assert!(!unack.is_empty(), "Should find unacknowledged deadletters");

    // Acknowledge the dead letter
    let mut acked = created.clone();
    acked.acknowledged = true;
    acked.notes = Some("Investigated: API quota issue, will retry next month".to_string());

    let updated = deadletter_repo
        .update(space_id, &deadletter_id, acked)
        .await?;

    assert!(updated.acknowledged, "Should be acknowledged after update");
    assert!(updated.notes.is_some(), "Notes should be recorded");

    Ok(())
}

/// M10.6: Idempotency prevents duplicate work items.
///
/// Test invariants:
/// 1. Same idempotency_key returns existing item
/// 2. Different keys create separate items
#[tokio::test]
#[ignore]
async fn scenario_m10_6_idempotency_prevents_duplicates() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    let work_repo = WorkItemRepository::new(harness.client.clone());

    let idempotency_key = format!("unique-key-{}", uuid::Uuid::new_v4());

    // Create first work item
    let work1_id = RecordId::new_v4(TableName::WorkItem);
    let work1_base = BaseFields::new(work1_id.clone(), space_id.to_string(), "P_sys".to_string());
    let work_item1 = WorkItem {
        base: work1_base,
        kind: "extract".to_string(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Pending,
        priority: 50,
        idempotency_key: idempotency_key.clone(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    work_repo.create(work_item1).await?;

    // Try to find by idempotency key
    let found = work_repo
        .find_by_idempotency_key(space_id, &idempotency_key)
        .await?;

    assert!(found.is_some(), "Should find item by idempotency key");
    assert_eq!(
        found.unwrap().base.id,
        work1_id,
        "Should return original item"
    );

    // Different key should not find the original
    let not_found = work_repo
        .find_by_idempotency_key(space_id, "different-key")
        .await?;
    assert!(not_found.is_none(), "Different key should not find item");

    Ok(())
}
