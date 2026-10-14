//! Regression oracles for recovery observing an already owned queue handoff.
#![allow(clippy::expect_used)]
use super::tests::{job, prepare_storage};
use super::*;
use crate::{Repository, StorageClient};
use surrealdb::sql::{Id, Thing, Value};

fn bindings(job: &ReprocessJob, work: &RecordId) -> std::collections::BTreeMap<String, Value> {
    crate::bindings! {
        "job" => crate::surreal::thing_from_record_id(&job.base.id),
        "work" => crate::surreal::thing_from_record_id(work),
        "archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(work)),
        "origin" => Thing::from(("work_producer_origin", Id::String(work.id().to_string()))),
        "attempt" => crate::work_attempt_repository::attempt_id(work),
        "epoch" => Thing::from(("canonical_epoch", Id::String(job.base.space_id.clone()))),
        "references" => Thing::from(("source_erasure_guard", Id::String(job.base.space_id.clone()))),
    }
}

// Native values preserve record IDs, timestamps, NONE/NULL and every field.
// Compare complete persisted rows, not just the nonce currently written by the
// implementation: recovery must not create a different hidden write instead.
async fn snapshot(storage: &StorageClient, job: &ReprocessJob, work: &RecordId) -> Vec<Value> {
    let mut response = storage
        .query_bind_checked(
            "SELECT * FROM ONLY $job; SELECT * FROM ONLY $work;
             SELECT * FROM ONLY $archive; SELECT * FROM ONLY $origin;
             SELECT * FROM ONLY $attempt; SELECT * FROM ONLY $epoch;
             SELECT * FROM ONLY $references;",
            bindings(job, work),
        )
        .await
        .expect("actual complete authority rows");
    (0..7)
        .map(|index| {
            let value: surrealdb::Value = response.take(index).expect("native row");
            value.into_inner()
        })
        .collect()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep jobs in the original fixture scope through assertions and cleanup."
)]
async fn assert_observational(storage: &StorageClient, job: &ReprocessJob, work: &RecordId) {
    let before = snapshot(storage, job, work).await;
    let jobs = ReprocessJobRepository::new(storage.clone());
    for _ in 0..3 {
        assert_eq!(
            jobs.ensure_work(job).await.expect("existing handoff"),
            *work
        );
    }
    assert_eq!(snapshot(storage, job, work).await, before);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep items in the original fixture scope through assertions and cleanup."
)]
async fn fail_work(storage: &StorageClient, job: &ReprocessJob, id: &RecordId) {
    let items = WorkItemRepository::new(storage.clone());
    let claimed = items
        .claim_with_lease(
            &job.base.space_id,
            id,
            "actual-owner",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("original owner");
    assert_observational(storage, job, id).await;
    let fence = crate::work_attempt_repository::fence_from_claim(&claimed).expect("original fence");
    items
        .update_running_for_worker(
            &job.base.space_id,
            id,
            &fence.lease_token,
            WorkItemStatus::Failed,
            Some("regression fixture".into()),
        )
        .await
        .expect("terminal transition")
        .expect("owned transition");
}

async fn archive_work(storage: &StorageClient) {
    crate::work_retention::archive_terminal_work(
        storage,
        chrono::Utc::now() + chrono::Duration::seconds(1),
        100,
    )
    .await
    .expect("actual archive transition");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs in the original fixture scope through assertions and cleanup."
)]
async fn surreal_reprocess_existing_handoffs_leave_all_authority_rows_unchanged() {
    let storage = StorageClient::new_memory().await.expect("memory");
    prepare_storage(&storage).await;
    let jobs = ReprocessJobRepository::new(storage.clone());
    let mut original = jobs.create_with_work(job()).await.expect("atomic creation");
    let work = intended_work(&original).expect("identity").base.id;
    assert_observational(&storage, &original, &work).await;

    // A Running job remains discoverable, and recovery is equally observational.
    original.status = ReprocessStatus::Running;
    let space = original.base.space_id.clone();
    let job_id = original.base.id.clone();
    original = jobs
        .update(&space, &job_id, original)
        .await
        .expect("job status advance");
    assert_observational(&storage, &original, &work).await;
    // Includes the actual claim token, attempt generation, lease and payload.
    fail_work(&storage, &original, &work).await;
    assert_observational(&storage, &original, &work).await;
    archive_work(&storage).await;
    let archived = snapshot(&storage, &original, &work).await;
    assert_eq!(archived[1], Value::None, "hot handoff physically archived");
    assert!(matches!(archived[2], Value::Object(_)), "archive exists");
    assert_observational(&storage, &original, &work).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs in the original fixture scope through assertions and cleanup."
)]
async fn surreal_reprocess_missing_handoff_still_writes_both_recovery_fences() {
    let storage = StorageClient::new_memory().await.expect("memory");
    prepare_storage(&storage).await;
    let jobs = ReprocessJobRepository::new(storage.clone());
    // This is the real retained crash gap: original intent + producer committed,
    // with no queue record. No fabricated producer or execution token is used.
    let original = jobs.create(job()).await.expect("durable original intent");
    let work = intended_work(&original).expect("identity").base.id;
    let before = snapshot(&storage, &original, &work).await;
    assert_eq!(before[1], Value::None);
    assert!(
        matches!(before[3], Value::Object(_)),
        "private origin exists"
    );
    assert!(
        matches!(before[5], Value::Object(_)),
        "canonical epoch exists"
    );
    assert_eq!(jobs.ensure_work(&original).await.expect("repair"), work);
    let after = snapshot(&storage, &original, &work).await;
    assert_eq!(after[0], before[0], "repair does not rewrite the job");
    assert!(
        matches!(after[1], Value::Object(_)),
        "real queue work created"
    );
    for (index, nonce_field) in [(3, "read_nonce"), (5, "queue_read_nonce")] {
        assert_ne!(
            after[index], before[index],
            "repair still fences original evidence"
        );
        let fields = match &after[index] {
            Value::Object(fields) => Some(fields),
            _ => None,
        }
        .expect("fenced row remains an object");
        assert!(matches!(fields.get(nonce_field), Some(Value::Strand(_))));
    }
    assert_observational(&storage, &original, &work).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs in the original fixture scope through assertions and cleanup."
)]
async fn surreal_reprocess_observation_rejects_changed_job_work_origin_and_archive() {
    let storage = StorageClient::new_memory().await.expect("memory");
    prepare_storage(&storage).await;
    let jobs = ReprocessJobRepository::new(storage.clone());
    for (archived, mutation) in [
        (false, "UPDATE $job SET name='changed after observation';"),
        (false, "UPDATE $work SET security.level=0;"),
        (false, "UPDATE $work SET payload.job_id='different-job';"),
        (false, "UPDATE $origin SET payload='{}';"),
        (false, "UPDATE $origin SET space_id='different-space';"),
        (false, "DELETE $origin;"),
        (true, "UPDATE $archive SET original_id=$job;"),
        (true, "UPDATE $archive SET space_id='different-space';"),
        (true, "UPDATE $archive SET deleted_at=time::now();"),
        (true, "UPDATE $archive SET record.security.level=0;"),
    ] {
        let original = jobs.create_with_work(job()).await.expect("actual job");
        let work = intended_work(&original).expect("identity").base.id;
        if archived {
            fail_work(&storage, &original, &work).await;
            archive_work(&storage).await;
        }
        // Bypass the public APIs only to model corrupted durable evidence.
        storage
            .query_bind_checked(mutation, bindings(&original, &work))
            .await
            .expect("install malformed evidence fixture");
        let before = snapshot(&storage, &original, &work).await;
        assert!(jobs.ensure_work(&original).await.is_err(), "{mutation}");
        assert_eq!(
            snapshot(&storage, &original, &work).await,
            before,
            "{mutation}"
        );
    }
}
