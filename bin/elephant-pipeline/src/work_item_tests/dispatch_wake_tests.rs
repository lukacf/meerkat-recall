//! Controlled completion wakeups and real queue due-time/permit boundaries.
//! No wall-clock latency thresholds, model, provider or qualification cap changes.
use super::*;
use tokio::sync::oneshot;
use tokio_test::{assert_pending, task};

const DISTANT_POLL: std::time::Duration = std::time::Duration::from_secs(3600);

async fn completed_task(tasks: &mut JoinSet<()>) {
    let (sent, received) = oneshot::channel();
    tasks.spawn(async move {
        sent.send(()).expect("completion notification");
        // No await after the notification: this task returns in the same poll.
    });
    received.await.expect("owned task reached its return");
}

#[tokio::test]
async fn completion_wakes_before_fallback_and_empty_set_stays_pending() {
    let mut tasks = JoinSet::new();
    let poll = tokio::time::sleep(DISTANT_POLL);
    tokio::pin!(poll);
    {
        let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
        assert_pending!(wake.poll());
    }
    completed_task(&mut tasks).await;
    assert_eq!(
        wait_for_work_queue_wake(&mut tasks, poll.as_mut()).await,
        WorkQueueWake::Completion
    );
    assert!(tasks.is_empty());
    let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
    assert_pending!(wake.poll());
}

#[tokio::test]
async fn long_running_task_waits_but_poll_fallback_still_runs() {
    let mut tasks = JoinSet::new();
    let (release, wait) = oneshot::channel::<()>();
    tasks.spawn(async move {
        let _ = wait.await;
    });
    let poll = tokio::time::sleep(DISTANT_POLL);
    tokio::pin!(poll);
    {
        let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
        assert_pending!(wake.poll());
    }
    // Model a due configured fallback without a real long sleep.
    poll.as_mut().reset(tokio::time::Instant::now());
    assert_eq!(
        wait_for_work_queue_wake(&mut tasks, poll.as_mut()).await,
        WorkQueueWake::Poll
    );
    assert_eq!(tasks.len(), 1, "fallback must not cancel active work");
    release.send(()).expect("release long-running work");
    tasks
        .join_next()
        .await
        .expect("owned task")
        .expect("task success");
}

#[tokio::test]
async fn due_poll_is_not_starved_or_reset_by_ready_completions() {
    let mut tasks = JoinSet::new();
    completed_task(&mut tasks).await;
    let poll = tokio::time::sleep(std::time::Duration::ZERO);
    tokio::pin!(poll);
    poll.as_mut().await;
    assert_eq!(
        wait_for_work_queue_wake(&mut tasks, poll.as_mut()).await,
        WorkQueueWake::Poll
    );
    assert_eq!(
        tasks.len(),
        1,
        "timer wake retains completed task for reaping"
    );
    poll.as_mut()
        .reset(tokio::time::Instant::now() + DISTANT_POLL);
    let deadline = poll.deadline();
    assert_eq!(
        wait_for_work_queue_wake(&mut tasks, poll.as_mut()).await,
        WorkQueueWake::Completion
    );
    assert_eq!(
        poll.deadline(),
        deadline,
        "completion must not defer fallback"
    );
}

#[tokio::test]
async fn cancelled_wait_retains_owned_task_for_shutdown_drain() {
    let mut tasks = JoinSet::new();
    let (release, wait) = oneshot::channel::<()>();
    tasks.spawn(async move {
        let _ = wait.await;
    });
    let poll = tokio::time::sleep(DISTANT_POLL);
    tokio::pin!(poll);
    {
        let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
        assert_pending!(wake.poll());
        // The outer worker shutdown select drops this pending future.
    }
    assert_eq!(tasks.len(), 1);
    release.send(()).expect("release work during drain");
    tasks
        .join_next()
        .await
        .expect("original owned task")
        .expect("drained");
    assert!(tasks.is_empty());
}

fn child(space: &str, name: &str, run_after: Option<chrono::DateTime<Utc>>) -> WorkItem {
    WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            space.into(),
            "wake-fixture".into(),
        ),
        kind: "dispatch-wake-fixture".into(),
        payload: serde_json::json!({"original_child": name}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: name.into(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after,
        last_error: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository owners alive through the original regression assertion scope."
)]
async fn completion_claims_due_children_only(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded()
            .await
            .expect("embedded storage")
    } else {
        StorageClient::new_memory().await.expect("memory storage")
    };
    initialize_fixture_schema(&storage).await;
    let space = "dispatch-completion-wake";
    let items = WorkItemRepository::new(storage.clone());
    let ready = child(space, "ready-child", None);
    let delayed = child(
        space,
        "delayed-child",
        Some(Utc::now() + Duration::hours(1)),
    );
    let ready_id = ready.base.id.clone();
    let delayed_id = delayed.base.id.clone();
    let delayed_snapshot = delayed.clone();
    let permits = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&permits)
        .acquire_owned()
        .await
        .expect("parent permit");
    let mut tasks = JoinSet::new();
    let (release, wait) = oneshot::channel::<()>();
    let parent_items = items.clone();
    tasks.spawn(async move {
        let _permit = permit;
        wait.await.expect("parent released");
        parent_items
            .create(ready)
            .await
            .expect("persist ready child");
        parent_items
            .create(delayed)
            .await
            .expect("persist delayed child");
    });
    assert_eq!(permits.available_permits(), 0);
    let poll = tokio::time::sleep(DISTANT_POLL);
    tokio::pin!(poll);
    {
        let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
        assert_pending!(wake.poll());
    }
    release.send(()).expect("parent can publish children");
    assert_eq!(
        wait_for_work_queue_wake(&mut tasks, poll.as_mut()).await,
        WorkQueueWake::Completion
    );
    assert_eq!(permits.available_permits(), 1);
    let queue = WorkQueue::with_defaults(storage.clone());
    let claimed = queue
        .claim_batch_prefer_kind(
            EXTRACT_ASSERTIONS_KIND,
            1,
            permits.available_permits(),
            |_| true,
        )
        .await
        .expect("same actual worker claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].base.id, ready_id);
    assert_eq!(claimed[0].attempts, 1);
    assert!(
        queue.for_attempt(&claimed[0]).is_ok(),
        "claim retains an attempt fence"
    );
    let still_delayed = items
        .get(space, &delayed_id)
        .await
        .expect("delayed row")
        .expect("retained");
    assert_eq!(still_delayed.run_after, delayed_snapshot.run_after);
    assert_eq!(still_delayed.attempts, 0);
    assert_eq!(still_delayed.status, WorkItemStatus::Runnable);
    assert!(queue
        .claim_batch_prefer_kind(EXTRACT_ASSERTIONS_KIND, 1, 1, |_| true,)
        .await
        .expect("blocked claim pass")
        .is_empty());
    // Only future-due work remains: empty JoinSet must not spin ready forever.
    let mut wake = task::spawn(wait_for_work_queue_wake(&mut tasks, poll.as_mut()));
    assert_pending!(wake.poll());
}

#[tokio::test]
async fn completion_claims_only_due_children_surreal() {
    completion_claims_due_children_only(false).await;
}

#[tokio::test]
async fn completion_claims_only_due_children_embedded() {
    completion_claims_due_children_only(true).await;
}
