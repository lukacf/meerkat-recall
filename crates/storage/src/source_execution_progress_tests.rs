//! Deterministic receipt read/commit races. These controls never call a provider
//! or claim a replacement queue attempt to make a stale transaction succeed.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::{Repository, WorkItemRepository};
use elephant_types::publication::{AttemptFence, PublicationFence};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{WorkItem, WorkItemStatus};
use serde_json::json;
use std::cell::Cell;

const SPACE: &str = "business-a";

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the shared client and work repository in the fixture scope until its original claim is captured."
)]
async fn fixture(embedded: bool) -> (SourcePublicationRepository, PublicationFence, AttemptFence) {
    let client = tests::client(embedded).await;
    if !embedded {
        client
            .query_checked(include_str!("../../migrations/sql/032_work_recovery.surql"))
            .await
            .expect("attempt schema");
    }
    let repository = SourcePublicationRepository::new(client.clone());
    let source_id = RecordId::new_v4(TableName::Source);
    let accepted = repository
        .accept(tests::request(&source_id, "r1", SourcePrecondition::Absent))
        .await
        .expect("accepted source");
    let publication = PublicationFence {
        source_id,
        generation: accepted.source_generation,
        revision_id: "r1".into(),
        operation_id: accepted.operation_id,
    };
    let work = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            SPACE.into(),
            "test".into(),
        ),
        kind: "embed_source_revision".into(),
        payload: json!({"_publication": publication}),
        status: WorkItemStatus::Runnable,
        priority: 0,
        idempotency_key: "progress-race".into(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let work_repository = WorkItemRepository::new(client);
    let work = work_repository.create(work).await.expect("queue work");
    let claimed = work_repository
        .claim_with_lease(
            SPACE,
            &work.base.id,
            "original-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("original claim");
    let attempt = crate::work_attempt_repository::fence_from_claim(&claimed).expect("fence");
    (repository, publication, attempt)
}

async fn operation_payload(
    repository: &SourcePublicationRepository,
    publication: &PublicationFence,
) -> serde_json::Value {
    repository
        .read_control(OPERATION_TABLE, &publication.operation_id, SPACE)
        .await
        .expect("read operation")
        .expect("operation exists")
}

async fn write_payload(
    repository: &SourcePublicationRepository,
    publication: &PublicationFence,
    table: &str,
    key: &str,
    value: &serde_json::Value,
) {
    if let Some(store) = repository.client.embedded_store() {
        store
            .mutate(|tx| write_control_tx(tx, table, key, SPACE, &publication.source_id, value))
            .await
            .expect("injected embedded competitor");
    } else {
        repository
            .client
            .query_bind_checked(
                "UPDATE $row CONTENT $record",
                crate::bindings! {
                    "row" => control_thing(table, key),
                    "record" => control_value(SPACE, &publication.source_id, value).expect("record"),
                },
            )
            .await
            .expect("injected native competitor");
    }
}

async fn assert_original_claim(repository: &SourcePublicationRepository, attempt: &AttemptFence) {
    let current = WorkItemRepository::new(repository.client.clone())
        .get(SPACE, &attempt.work_item_id)
        .await
        .expect("read work")
        .expect("original work");
    assert_eq!(current.attempts, 1, "no claim/refund/retry-budget mutation");
    assert_eq!(current.status, WorkItemStatus::Running);
    assert_eq!(
        crate::work_attempt_repository::fence_from_claim(&current).expect("current fence"),
        *attempt,
    );
}

async fn progress_retains_unknown_fields(embedded: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (repository, publication, attempt) = Box::pin(fixture(embedded)).await;
    let mut original = operation_payload(&repository, &publication).await;
    original["future_authority"] = json!({"ceiling": ["private"], "version": 9});
    original["receipt"]["future_receipt_field"] = json!({"must": "survive"});
    write_payload(
        &repository,
        &publication,
        OPERATION_TABLE,
        &publication.operation_id,
        &original,
    )
    .await;
    let calls = AtomicUsize::new(0);
    repository
        .record_execution_state_with(
            SPACE,
            &publication,
            &attempt,
            OperationState::Processing,
            None,
            |preparation| {
                calls.fetch_add(1, Ordering::Relaxed);
                let repository = &repository;
                let publication = &publication;
                async move {
                    if preparation == 0 {
                        // Exactly the gap after the native read/prepare and
                        // before its whole-payload CAS; embedded reads in tx.
                        let mut current = operation_payload(repository, publication).await;
                        current["receipt"]["state"] = json!("failed_retryable");
                        current["receipt"]["error_code"] = json!("peer_progress");
                        write_payload(
                            repository,
                            publication,
                            OPERATION_TABLE,
                            &publication.operation_id,
                            &current,
                        )
                        .await;
                    }
                    Ok(())
                }
            },
        )
        .await
        .expect("same original authority can record progress");
    assert_eq!(calls.load(Ordering::Relaxed), if embedded { 1 } else { 2 });
    let current = operation_payload(&repository, &publication).await;
    original["receipt"]["state"] = json!("processing");
    assert_eq!(current, original, "only requested live progress changed");
    assert_original_claim(&repository, &attempt).await;
}

#[tokio::test]
async fn embedded_execution_progress_preserves_unknown_authority_fields() {
    Box::pin(progress_retains_unknown_fields(true)).await;
}

#[tokio::test]
async fn native_execution_progress_reconciles_only_live_receipt_race() {
    Box::pin(progress_retains_unknown_fields(false)).await;
}

#[tokio::test]
async fn native_execution_progress_race_exhausts_exactly_four_preparations() {
    let (repository, publication, attempt) = Box::pin(fixture(false)).await;
    let calls = Cell::new(0);
    let result = repository
        .record_execution_state_with(
            SPACE,
            &publication,
            &attempt,
            OperationState::Processing,
            None,
            |preparation| {
                calls.set(calls.get() + 1);
                let repository = &repository;
                let publication = &publication;
                async move {
                    let mut current = operation_payload(repository, publication).await;
                    current["receipt"]["state"] = json!("failed_retryable");
                    current["receipt"]["error_code"] = json!(format!("peer_{preparation}"));
                    write_payload(
                        repository,
                        publication,
                        OPERATION_TABLE,
                        &publication.operation_id,
                        &current,
                    )
                    .await;
                    Ok(())
                }
            },
        )
        .await;
    assert!(
        matches!(result, Err(StorageError::Conflict(code)) if code == "stale_source_execution")
    );
    assert_eq!(calls.get(), 4, "finite storage-only preparation budget");
    let current = operation_payload(&repository, &publication).await;
    assert_eq!(current["receipt"]["state"], "failed_retryable");
    assert_eq!(current["receipt"]["error_code"], "peer_3");
    assert_original_claim(&repository, &attempt).await;
}

#[tokio::test]
async fn native_execution_progress_never_adopts_changed_operation_authority() {
    for (pointer, replacement) in [
        ("/input_fingerprint", json!("changed-input")),
        ("/request_fingerprint", json!("changed-request")),
        ("/producer_origin", json!({"kind": "unknown_original"})),
        ("/authorization/security/level", json!(255)),
        ("/receipt/operation_id", json!("changed-operation")),
        ("/receipt/source_generation", json!("changed-generation")),
        ("/receipt/desired_revision", json!("r2")),
        ("/receipt/published_revision", json!("r1")),
        ("/receipt/state", json!("completed")),
        ("/receipt/completed_at", json!(Utc::now())),
    ] {
        let (repository, publication, attempt) = Box::pin(fixture(false)).await;
        let calls = Cell::new(0);
        let result = repository
            .record_execution_state_with(
                SPACE,
                &publication,
                &attempt,
                OperationState::Processing,
                None,
                |_| {
                    calls.set(calls.get() + 1);
                    let repository = &repository;
                    let publication = &publication;
                    let replacement = replacement.clone();
                    async move {
                        let mut current = operation_payload(repository, publication).await;
                        *current.pointer_mut(pointer).expect("original field") = replacement;
                        write_payload(
                            repository,
                            publication,
                            OPERATION_TABLE,
                            &publication.operation_id,
                            &current,
                        )
                        .await;
                        Ok(())
                    }
                },
            )
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(code)) if code == "stale_source_execution"),
            "{pointer}"
        );
        assert_eq!(
            calls.get(),
            1,
            "changed authority must not reprepare: {pointer}"
        );
        assert_original_claim(&repository, &attempt).await;
    }
}

#[tokio::test]
async fn native_execution_progress_unknown_authority_and_head_changes_refuse() {
    for change_head in [false, true] {
        let (repository, publication, attempt) = Box::pin(fixture(false)).await;
        let calls = Cell::new(0);
        let result = repository
            .record_execution_state_with(
                SPACE,
                &publication,
                &attempt,
                OperationState::Processing,
                None,
                |_| {
                    calls.set(calls.get() + 1);
                    let repository = &repository;
                    let publication = &publication;
                    async move {
                        let (table, key) = if change_head {
                            (HEAD_TABLE, head_key(SPACE, &publication.source_id))
                        } else {
                            (OPERATION_TABLE, publication.operation_id.clone())
                        };
                        let mut current: serde_json::Value = repository
                            .read_control(table, &key, SPACE)
                            .await?
                            .expect("control");
                        current["future_authority"] = json!("changed");
                        write_payload(repository, publication, table, &key, &current).await;
                        Ok(())
                    }
                },
            )
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(code)) if code == "stale_source_execution")
        );
        assert_eq!(calls.get(), 1);
        assert_original_claim(&repository, &attempt).await;
    }
}

#[tokio::test]
async fn native_execution_progress_rechecks_original_lease_after_live_reconciliation() {
    let (repository, publication, attempt) = Box::pin(fixture(false)).await;
    let calls = Cell::new(0);
    let result = repository
        .record_execution_state_with(
            SPACE,
            &publication,
            &attempt,
            OperationState::Processing,
            None,
            |preparation| {
                calls.set(calls.get() + 1);
                let repository = &repository;
                let publication = &publication;
                let attempt = &attempt;
                async move {
                    if preparation == 0 {
                        let mut current = operation_payload(repository, publication).await;
                        current["receipt"]["state"] = json!("failed_retryable");
                        current["receipt"]["error_code"] = json!("peer_progress");
                        write_payload(repository, publication, OPERATION_TABLE, &publication.operation_id, &current).await;
                    } else {
                        // Inject expiry directly: ordinary repository updates
                        // deliberately refuse mutations of a Running claim.
                        repository.client.query_bind_checked(
                            "UPDATE $work SET lease_expires_at = $expired",
                            crate::bindings! {
                                "work" => crate::surreal::thing_from_record_id(&attempt.work_item_id),
                                "expired" => Utc::now() - chrono::Duration::seconds(1),
                            },
                        ).await?;
                    }
                    Ok(())
                }
            },
        )
        .await;
    assert!(matches!(result, Err(StorageError::Conflict(code)) if code == "stale_attempt"));
    assert_eq!(
        calls.get(),
        2,
        "a lost lease cannot spend another storage retry"
    );
    let current = operation_payload(&repository, &publication).await;
    assert_eq!(current["receipt"]["state"], "failed_retryable");
    assert_eq!(current["receipt"]["error_code"], "peer_progress");
}

#[test]
fn execution_progress_reconciliation_requires_exact_native_conflict() {
    let display = "An error occurred: publication:stale_source_execution";
    for error in [
        StorageError::QueryError(display.into()),
        StorageError::Surreal(Box::new(surrealdb::Error::Db(
            surrealdb::error::Db::Thrown("publication:stale_source_execution".into()),
        ))),
        StorageError::Surreal(Box::new(surrealdb::Error::Api(
            surrealdb::error::Api::Query(display.into()),
        ))),
    ] {
        assert!(native_execution_progress_conflict(&error));
    }
    for error in [
        StorageError::Conflict("stale_source_execution".into()),
        StorageError::TransactionError(display.into()),
        StorageError::QueryError("publication:stale_source_execution".into()),
        StorageError::QueryError(format!("{display} trailing diagnostic")),
        StorageError::QueryError(format!("{display}\n")),
        StorageError::QueryError(format!("query text mentions {display}")),
        StorageError::QueryError("An error occurred: stale_attempt".into()),
    ] {
        assert!(!native_execution_progress_conflict(&error));
    }
}

#[tokio::test]
async fn execution_progress_rejects_live_state_with_terminal_timestamp_on_both_backends() {
    for embedded in [true, false] {
        let (repository, publication, attempt) = Box::pin(fixture(embedded)).await;
        let mut current = operation_payload(&repository, &publication).await;
        current["receipt"]["completed_at"] = json!(Utc::now());
        write_payload(
            &repository,
            &publication,
            OPERATION_TABLE,
            &publication.operation_id,
            &current,
        )
        .await;
        let result = repository
            .record_execution_state(
                SPACE,
                &publication,
                &attempt,
                OperationState::Processing,
                None,
            )
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(code)) if code == "stale_source_execution")
        );
        assert_eq!(operation_payload(&repository, &publication).await, current);
        assert_original_claim(&repository, &attempt).await;
    }
}
