//! Original external cursors survive real source-event retirement and recovery.
//!
//! Native controls replace an actual current snapshot under the recovery barrier;
//! the embedded control separately drops and reopens the managed durable store.

use super::*;
use crate::{OutboxCheckpointRepository, StorageBackendKind, SystemEventRepository};

const CONSUMER: &str = "actual-lifecycle-consumer";
const ERASED_EVENT_MARKER: &str = "ERASE-ME-original-event-tail";

struct OriginalHistory {
    accepted: OperationReceipt,
    independent: SystemEvent,
    erased: Vec<SystemEvent>,
    other_space: SystemEvent,
    cursor: i64,
    acknowledged: bool,
}

fn event(space: &str, seq: i64, source: Option<&RecordId>) -> SystemEvent {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SystemEvent),
        space.into(),
        "original-event-producer".into(),
    );
    if space == "erase-a" {
        base.created_at = Utc::now() - chrono::Duration::days(2);
        base.updated_at = base.created_at;
    }
    let payload = if let Some(source) = source {
        base.provenance.sources.push(source.clone());
        serde_json::json!({"source_id": source, "text": ERASED_EVENT_MARKER})
    } else {
        serde_json::json!({"independent": true})
    };
    SystemEvent {
        base,
        seq,
        event_type: "original.cursor.event".into(),
        payload,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn original_history(storage: &StorageClient, acknowledged: bool) -> OriginalHistory {
    let publication = SourcePublicationRepository::new(storage.clone());
    let accepted = publication
        .accept(request("erase-a", "sequence-history"))
        .await
        .unwrap();
    let events = SystemEventRepository::new(storage.clone());
    let independent = events.create(event("erase-a", 7, None)).await.unwrap();
    let mut erased = Vec::new();
    for seq in [99, 100] {
        erased.push(
            events
                .create(event("erase-a", seq, Some(&accepted.source_id)))
                .await
                .unwrap(),
        );
    }
    let other_space = events.create(event("erase-b", 1_000, None)).await.unwrap();
    // Expected ordering comes from original actual rows, never from the new floor.
    let cursor = events.get_latest_seq("erase-a").await.unwrap();
    assert_eq!(cursor, erased.last().unwrap().seq);
    assert!(events
        .get_after_seq("erase-a", cursor, 10)
        .await
        .unwrap()
        .is_empty());
    let checkpoints = OutboxCheckpointRepository::new(storage.clone());
    assert_eq!(checkpoints.load("erase-a", CONSUMER).await.unwrap(), 0);
    if acknowledged {
        checkpoints
            .advance("erase-a", CONSUMER, 0, cursor)
            .await
            .unwrap();
    }
    OriginalHistory {
        accepted,
        independent,
        erased,
        other_space,
        cursor,
        acknowledged,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn assert_purged_without_completion(storage: &StorageClient, original: &OriginalHistory) {
    let events = SystemEventRepository::new(storage.clone());
    for erased in &original.erased {
        assert!(events.get_by_id(&erased.base.id).await.unwrap().is_none());
    }
    assert!(SourceRepository::new(storage.clone())
        .get_with_deleted("erase-a", &original.accepted.source_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        events
            .get_by_id(&original.independent.base.id)
            .await
            .unwrap(),
        Some(original.independent.clone())
    );
    assert!(events
        .get_after_seq("erase-a", original.cursor, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        OutboxCheckpointRepository::new(storage.clone())
            .load("erase-a", CONSUMER)
            .await
            .unwrap(),
        if original.acknowledged {
            original.cursor
        } else {
            0
        }
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn assert_recovered_completion(
    storage: &StorageClient,
    original: &OriginalHistory,
) -> SystemEvent {
    assert!(storage.recovery_status().ready);
    let events = SystemEventRepository::new(storage.clone());
    let visible = events
        .get_after_seq("erase-a", original.cursor, 10)
        .await
        .unwrap();
    assert_eq!(
        visible.len(),
        1,
        "the original cursor must see the completion"
    );
    let completed = visible[0].clone();
    assert_eq!(completed.event_type, "source.erased");
    assert_eq!(completed.seq, original.cursor + 1);
    assert_eq!(
        completed.payload["source_id"],
        serde_json::json!(original.accepted.source_id)
    );
    assert!(
        completed.seq < original.other_space.seq,
        "foreign-space history must not raise this cursor"
    );
    assert_eq!(
        events
            .get_by_id(&original.other_space.base.id)
            .await
            .unwrap(),
        Some(original.other_space.clone())
    );
    assert_eq!(
        events
            .get_by_id(&original.independent.base.id)
            .await
            .unwrap(),
        Some(original.independent.clone())
    );
    for erased in &original.erased {
        assert!(events.get_by_id(&erased.base.id).await.unwrap().is_none());
    }
    assert!(SourceRepository::new(storage.clone())
        .get_with_deleted("erase-a", &original.accepted.source_id)
        .await
        .unwrap()
        .is_none());
    let publication = SourcePublicationRepository::new(storage.clone());
    assert!(
        !publication
            .inventory("erase-a")
            .await
            .unwrap()
            .iter()
            .any(|row| { row.value.to_string().contains(ERASED_EVENT_MARKER) }),
        "the sequence proof must not retain erased event payloads"
    );
    let checkpoints = OutboxCheckpointRepository::new(storage.clone());
    assert_eq!(
        checkpoints.load("erase-a", CONSUMER).await.unwrap(),
        if original.acknowledged {
            original.cursor
        } else {
            0
        },
        "producer preservation never acknowledges a real consumer"
    );
    publication.recover_pending_erasures().await.unwrap();
    assert_eq!(
        events
            .get_after_seq("erase-a", original.cursor, 10)
            .await
            .unwrap(),
        vec![completed.clone()],
        "recovery replay must not append or renumber completion"
    );
    completed
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn snapshot_fault_recovery(embedded: bool, acknowledged: bool) {
    for fault in [ErasureFault::AfterPurge, ErasureFault::AfterPersistence] {
        let storage = client(embedded).await;
        let original = Box::pin(original_history(&storage, acknowledged)).await;
        let publication = SourcePublicationRepository::new(storage.clone());
        let erase = mutation(&publication, &original.accepted, "erase-cursor-history").await;
        assert!(publication
            .erase_live_with_fault(erase, fault)
            .await
            .is_err());
        assert!(!storage.recovery_status().ready);
        let guard = storage
            .begin_maintenance(
                crate::recovery_barrier::RecoveryPhase::Replaying,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        let privileged = storage.maintenance_client_with_guard(&guard);
        Box::pin(assert_purged_without_completion(&privileged, &original)).await;
        let directory = tempfile::tempdir().unwrap();
        let snapshot = privileged
            .snapshot_bytes(&directory.path().join("post-purge-current.snapshot"))
            .await
            .unwrap();
        assert!(!snapshot
            .windows(ERASED_EVENT_MARKER.len())
            .any(|part| part == ERASED_EVENT_MARKER.as_bytes()));
        // Replacing the physical state proves the floor is in the real snapshot;
        // keeping the independently retained original journal proves real replay.
        privileged.replace_snapshot_bytes(&snapshot).await.unwrap();
        Box::pin(assert_purged_without_completion(&privileged, &original)).await;
        SourcePublicationRepository::new(privileged.clone())
            .recover_pending_erasures()
            .await
            .unwrap();
        privileged.persist_restored_state().await.unwrap();
        drop(privileged);
        guard.finish();
        Box::pin(assert_recovered_completion(&storage, &original)).await;
    }
}

#[tokio::test]
async fn embedded_erasure_completion_survives_current_snapshot_with_original_cursor() {
    Box::pin(snapshot_fault_recovery(true, true)).await;
}

#[tokio::test]
async fn native_erasure_completion_survives_current_snapshot_with_original_cursor() {
    Box::pin(snapshot_fault_recovery(false, true)).await;
}

#[tokio::test]
async fn embedded_erasure_completion_preserves_consumerless_external_cursor() {
    Box::pin(snapshot_fault_recovery(true, false)).await;
}

#[tokio::test]
async fn native_erasure_completion_preserves_consumerless_external_cursor() {
    Box::pin(snapshot_fault_recovery(false, false)).await;
}

async fn durable_client(root: &std::path::Path) -> StorageClient {
    StorageClient::new_with_durability(
        StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        },
        Some(root.join("live.snapshot")),
        Some(root.join("original-journal")),
    )
    .await
    .unwrap()
}

async fn initialize_spaces(storage: &StorageClient) {
    for space in ["erase-a", "erase-b"] {
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id": RecordId::new_v4(TableName::SpaceConfig), "space_id": space,
            "created_at": Utc::now(), "updated_at": Utc::now(), "created_by": "test",
            "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
            "provenance": {}, "subjects": [], "subjects_state": "complete",
            "embedding_provider": "fake", "embedding_model": "test", "embedding_dims": 8
        }))
        .unwrap();
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn embedded_erasure_floor_reopens_from_original_durable_store_after_both_faults() {
    for fault in [ErasureFault::AfterPurge, ErasureFault::AfterPersistence] {
        let directory = tempfile::tempdir().unwrap();
        let original = {
            let storage = durable_client(directory.path()).await;
            initialize_spaces(&storage).await;
            let original = Box::pin(original_history(&storage, true)).await;
            let publication = SourcePublicationRepository::new(storage.clone());
            let erase = mutation(&publication, &original.accepted, "durable-cursor-erase").await;
            assert!(publication
                .erase_live_with_fault(erase, fault)
                .await
                .is_err());
            assert!(!storage.recovery_status().ready);
            // Drop all store handles: no shutdown snapshot or manual floor write.
            original
        };
        let reopened = durable_client(directory.path()).await;
        Box::pin(assert_recovered_completion(&reopened, &original)).await;
    }
}

async fn original_checkpoint_rows(storage: &StorageClient) -> Vec<(String, serde_json::Value)> {
    let mut rows: Vec<_> = SourcePublicationRepository::new(storage.clone())
        .inventory("erase-a")
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.table == "outbox_checkpoint")
        .map(|row| (row.key, row.value))
        .collect();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn failed_purge_rolls_back_floor_and_history(embedded: bool) {
    let storage = client(embedded).await;
    let original = Box::pin(original_history(&storage, true)).await;
    let checkpoints_before = original_checkpoint_rows(&storage).await;
    let publication = SourcePublicationRepository::new(storage.clone());
    let erase = mutation(&publication, &original.accepted, "rollback-original-tail").await;
    assert!(publication
        .erase_live_with_fault(erase, ErasureFault::DuringPurge)
        .await
        .is_err());
    assert!(!storage.recovery_status().ready);
    let guard = storage
        .begin_maintenance(
            crate::recovery_barrier::RecoveryPhase::Replaying,
            std::time::Duration::from_secs(30),
        )
        .await
        .unwrap();
    let privileged = storage.maintenance_client_with_guard(&guard);
    assert_eq!(
        original_checkpoint_rows(&privileged).await,
        checkpoints_before,
        "late destructive transaction failure must roll back its floor write"
    );
    let events = SystemEventRepository::new(privileged.clone());
    for original_event in &original.erased {
        assert_eq!(
            events.get_by_id(&original_event.base.id).await.unwrap(),
            Some(original_event.clone()),
            "the same failed transaction must preserve every original event"
        );
    }
    assert!(events
        .get_after_seq("erase-a", original.cursor, 10)
        .await
        .unwrap()
        .is_empty());
    SourcePublicationRepository::new(privileged.clone())
        .recover_pending_erasures()
        .await
        .unwrap();
    drop(events);
    drop(privileged);
    guard.finish();
    Box::pin(assert_recovered_completion(&storage, &original)).await;
}

#[tokio::test]
async fn embedded_failed_purge_rolls_back_original_event_floor_and_rows() {
    Box::pin(failed_purge_rolls_back_floor_and_history(true)).await;
}

#[tokio::test]
async fn native_failed_purge_rolls_back_original_event_floor_and_rows() {
    Box::pin(failed_purge_rolls_back_floor_and_history(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn retention_snapshot_keeps_consumerless_cursor(embedded: bool) {
    let storage = client(embedded).await;
    let original = Box::pin(original_history(&storage, false)).await;
    assert_eq!(
        crate::event_sequence::retire_before(&storage, Utc::now() - chrono::Duration::days(1),)
            .await
            .unwrap(),
        original.erased.len() + 1
    );
    let events = SystemEventRepository::new(storage.clone());
    assert!(events
        .get_after_seq("erase-a", 0, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        events
            .get_by_id(&original.other_space.base.id)
            .await
            .unwrap(),
        Some(original.other_space.clone())
    );
    for retired in std::iter::once(&original.independent).chain(&original.erased) {
        assert!(events.get_by_id(&retired.base.id).await.unwrap().is_none());
    }
    let guard = storage
        .begin_maintenance(
            crate::recovery_barrier::RecoveryPhase::Replaying,
            std::time::Duration::from_secs(30),
        )
        .await
        .unwrap();
    let privileged = storage.maintenance_client_with_guard(&guard);
    let directory = tempfile::tempdir().unwrap();
    let snapshot = privileged
        .snapshot_bytes(&directory.path().join("current-after-retention.snapshot"))
        .await
        .unwrap();
    privileged.replace_snapshot_bytes(&snapshot).await.unwrap();
    privileged.persist_restored_state().await.unwrap();
    drop(privileged);
    guard.finish();
    let publication = SourcePublicationRepository::new(storage.clone());
    let retracted = publication
        .retract(mutation(&publication, &original.accepted, "retract-after-retention").await)
        .await
        .unwrap();
    assert_eq!(retracted.state, OperationState::Retracted);
    let following = events
        .get_after_seq("erase-a", original.cursor, 10)
        .await
        .unwrap();
    assert_eq!(following.len(), 1);
    assert_eq!(following[0].event_type, "source.retracted");
    assert_eq!(following[0].seq, original.cursor + 1);
    assert_eq!(
        OutboxCheckpointRepository::new(storage.clone())
            .load("erase-a", CONSUMER)
            .await
            .unwrap(),
        0,
        "external cursor survival must not rely on a real consumer acknowledgment"
    );
    assert_eq!(
        events
            .get_by_id(&original.other_space.base.id)
            .await
            .unwrap(),
        Some(original.other_space.clone())
    );
    assert!(!following[0]
        .payload
        .to_string()
        .contains(ERASED_EVENT_MARKER));
}

#[tokio::test]
async fn embedded_actual_retention_current_snapshot_keeps_consumerless_lifecycle_cursor() {
    Box::pin(retention_snapshot_keeps_consumerless_cursor(true)).await;
}

#[tokio::test]
async fn native_actual_retention_current_snapshot_keeps_consumerless_lifecycle_cursor() {
    Box::pin(retention_snapshot_keeps_consumerless_cursor(false)).await;
}
