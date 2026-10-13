//! The complete durable erasure journal shares one logical-before-physical batch.
//!
//! These controls use actual reservations, repositories, fault boundaries and
//! named snapshots. No replacement checkpoint or simulated database is used.

use super::*;
use crate::recovery_barrier::RecoveryPhase;

async fn reserve_only(
    repository: &SourcePublicationRepository,
    accepted: &OperationReceipt,
    key: &str,
) -> OperationReceipt {
    let request = mutation(repository, accepted, key).await;
    let reserved = repository
        .prepare_lifecycle(&request, MutationKind::Erase)
        .await
        .unwrap();
    repository
        .commit_lifecycle_reservation(&request, &reserved, None, None)
        .await
        .unwrap();
    reserved.operation.receipt
}

async fn assert_pending(repository: &SourcePublicationRepository, original: &OperationReceipt) {
    let receipt = repository
        .operation(&original.space_id, &original.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.operation_id, original.operation_id);
    assert_eq!(receipt.source_generation, original.source_generation);
    assert_eq!(receipt.accepted_at, original.accepted_at);
    assert_eq!(receipt.state, OperationState::Erasing);
    assert!(receipt.completed_at.is_none());
    assert_eq!(
        repository
            .head(&original.space_id, &original.source_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        SourceVisibility::Erasing
    );
}

async fn assert_absent(storage: &StorageClient, receipt: &OperationReceipt) {
    assert!(SourceRepository::new(storage.clone())
        .get_with_deleted(&receipt.space_id, &receipt.source_id)
        .await
        .unwrap()
        .is_none());
    assert!(SourcePublicationRepository::new(storage.clone())
        .revision(&receipt.space_id, &receipt.source_id, "r1")
        .await
        .unwrap()
        .is_none());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn original_batch_faults(embedded: bool) {
    for fault in [ErasureFault::AfterPurge, ErasureFault::AfterPersistence] {
        let storage = client(embedded).await;
        let repository = SourcePublicationRepository::new(storage.clone());
        let mut accepted = Vec::new();
        for (space, external) in [
            ("erase-a", "batch-first-gap"),
            ("erase-a", "batch-original-admitted"),
            ("erase-b", "batch-foreign-gap"),
        ] {
            accepted.push(repository.accept(request(space, external)).await.unwrap());
        }
        let survivor = repository
            .accept(request("erase-b", "batch-independent-survivor"))
            .await
            .unwrap();
        let survivor_source = SourceRepository::new(storage.clone())
            .get("erase-b", &survivor.source_id)
            .await
            .unwrap()
            .unwrap();
        let mut reserved = Vec::new();
        for (index, receipt) in accepted.iter().enumerate() {
            reserved
                .push(reserve_only(&repository, receipt, &format!("batch-erase-{index}")).await);
        }
        let original = storage
            .erasure_journal()
            .unwrap()
            .record_intent(&accepted[1].space_id, &accepted[1].source_id)
            .unwrap();
        assert_eq!(
            storage.erasure_journal().unwrap().intents().unwrap(),
            vec![original.clone()]
        );
        let target = original.clone();
        let result: StorageResult<()> = storage
            .with_erasure_barrier(move |exclusive| async move {
                // The interrupted owner is the second accepted source in the
                // same Space; processing the other intent first must refuse.
                exclusive
                    .with_erasure_reservation(&target, async {
                        crate::reference_fence::reserve_reference_guard(
                            &exclusive,
                            &target.space_id,
                            &target,
                        )
                        .await
                    })
                    .await?;
                SourcePublicationRepository::new(exclusive)
                    .purge_all_reserved_sources(Some((&target, fault)))
                    .await
            })
            .await;
        assert!(result.is_err());
        assert!(!storage.recovery_status().ready);
        let guard = storage
            .begin_maintenance(RecoveryPhase::Replaying, std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let exclusive = storage.maintenance_client_with_guard(&guard);
        let replay = SourcePublicationRepository::new(exclusive.clone());
        let intents = exclusive.erasure_journal().unwrap().intents().unwrap();
        assert_eq!(
            intents.len(),
            3,
            "all accepted head-to-journal gaps close before purge"
        );
        assert!(
            intents.contains(&original),
            "original intent identity never changes"
        );
        for receipt in &reserved {
            assert_pending(&replay, receipt).await;
        }
        assert_absent(&exclusive, &accepted[1]).await;
        for receipt in [&accepted[0], &accepted[2]] {
            if fault == ErasureFault::AfterPersistence {
                assert_absent(&exclusive, receipt).await;
            } else {
                assert!(
                    SourceRepository::new(exclusive.clone())
                        .get_with_deleted(&receipt.space_id, &receipt.source_id)
                        .await
                        .unwrap()
                        .is_some(),
                    "AfterPurge remains the target's original pre-checkpoint boundary"
                );
            }
        }
        assert!(crate::reference_fence::reference_reservations(&exclusive)
            .await
            .unwrap()
            .is_empty());
        for space in ["erase-a", "erase-b"] {
            assert!(!crate::SystemEventRepository::new(exclusive.clone())
                .get_after_seq(space, 0, 100)
                .await
                .unwrap()
                .iter()
                .any(|event| event.event_type == "source.erased"));
        }
        Box::pin(replay.recover_pending_erasures()).await.unwrap();
        let mut completed = Vec::new();
        for receipt in &reserved {
            assert_absent(&exclusive, receipt).await;
            let actual = replay
                .operation(&receipt.space_id, &receipt.operation_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(actual.state, OperationState::Erased);
            assert_eq!(actual.source_generation, receipt.source_generation);
            assert_eq!(actual.accepted_at, receipt.accepted_at);
            assert!(actual.completed_at.is_some());
            completed.push(actual);
        }
        let events_before = crate::SystemEventRepository::new(exclusive.clone())
            .get_after_seq("erase-a", 0, 100)
            .await
            .unwrap();
        assert_eq!(
            events_before
                .iter()
                .filter(|event| event.event_type == "source.erased")
                .count(),
            2
        );
        Box::pin(replay.recover_pending_erasures()).await.unwrap();
        for receipt in completed {
            assert_eq!(
                replay
                    .operation(&receipt.space_id, &receipt.operation_id)
                    .await
                    .unwrap(),
                Some(receipt)
            );
        }
        assert_eq!(
            crate::SystemEventRepository::new(exclusive.clone())
                .get_after_seq("erase-a", 0, 100)
                .await
                .unwrap(),
            events_before
        );
        assert_eq!(
            SourceRepository::new(exclusive.clone())
                .get("erase-b", &survivor.source_id)
                .await
                .unwrap(),
            Some(survivor_source)
        );
        assert_eq!(
            exclusive.erasure_journal().unwrap().intents().unwrap(),
            intents
        );
        exclusive.persist_restored_state().await.unwrap();
        drop(replay);
        drop(exclusive);
        guard.finish();
        assert!(storage.recovery_status().ready);
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn live_batch_preserves_fresh_incarnation(embedded: bool) {
    let storage = client(embedded).await;
    let repository = SourcePublicationRepository::new(storage.clone());
    let original_request = request("erase-a", "batch-recreated-external-key");
    let old = repository.accept(original_request.clone()).await.unwrap();
    repository
        .erase_live(mutation(&repository, &old, "batch-old-erase").await)
        .await
        .unwrap();
    let old_intent = storage
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == old.source_id)
        .unwrap();
    let old_head = repository
        .head("erase-a", &old.source_id)
        .await
        .unwrap()
        .unwrap();
    let mut fresh_request = original_request;
    fresh_request.source.base.id = RecordId::new_v4(TableName::Source);
    fresh_request.revision_id = "r2".into();
    fresh_request.idempotency_key = "batch-fresh-accept".into();
    fresh_request.request_fingerprint = "batch-fresh-bytes".into();
    fresh_request.expected = SourcePrecondition::Generation(old_head.generation);
    fresh_request.recreate_after_erasure = true;
    fresh_request.authorized = repository
        .authorization_snapshot("erase-a", &old.source_id)
        .await
        .unwrap();
    let fresh = repository.accept(fresh_request).await.unwrap();
    let fresh_source = SourceRepository::new(storage.clone())
        .get("erase-a", &fresh.source_id)
        .await
        .unwrap()
        .unwrap();
    let fresh_head = repository
        .head("erase-a", &fresh.source_id)
        .await
        .unwrap()
        .unwrap();
    let gap = repository
        .accept(request("erase-a", "batch-live-gap"))
        .await
        .unwrap();
    let gap_receipt = reserve_only(&repository, &gap, "batch-live-gap-erase").await;
    assert!(storage
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .iter()
        .all(|intent| intent.source_id != gap.source_id));
    let live = repository
        .accept(request("erase-b", "batch-live-target"))
        .await
        .unwrap();
    let erased = repository
        .erase_live(mutation(&repository, &live, "batch-live-erase").await)
        .await
        .unwrap();
    assert_eq!(erased.state, OperationState::Erased);
    assert_eq!(
        repository
            .operation("erase-a", &gap_receipt.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Erased,
        "live target also owns other already accepted gaps"
    );
    assert_absent(&storage, &gap).await;
    assert_absent(&storage, &live).await;
    let before = storage.erasure_journal().unwrap().intents().unwrap();
    repository
        .replay_erasure("erase-a", &old.source_id, &old_intent.intent_id)
        .await
        .unwrap();
    assert_eq!(
        storage.erasure_journal().unwrap().intents().unwrap(),
        before
    );
    assert_eq!(
        SourceRepository::new(storage.clone())
            .get("erase-a", &fresh.source_id)
            .await
            .unwrap(),
        Some(fresh_source)
    );
    assert_eq!(
        repository.head("erase-a", &fresh.source_id).await.unwrap(),
        Some(fresh_head)
    );
    assert_eq!(
        repository
            .operation("erase-b", &erased.operation_id)
            .await
            .unwrap(),
        Some(erased)
    );
    assert!(storage.recovery_status().ready);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn old_snapshot_replays_multiple_original_intents(embedded: bool) {
    let storage = client(embedded).await;
    let repository = SourcePublicationRepository::new(storage.clone());
    let mut accepted = Vec::new();
    for (space, external) in [
        ("erase-a", "batch-backup-first"),
        ("erase-b", "batch-backup-second"),
    ] {
        accepted.push(repository.accept(request(space, external)).await.unwrap());
    }
    let survivor = repository
        .accept(request("erase-a", "batch-backup-survivor"))
        .await
        .unwrap();
    let survivor_source = SourceRepository::new(storage.clone())
        .get("erase-a", &survivor.source_id)
        .await
        .unwrap()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("original-batch-backup");
    crate::snapshot::export_named(
        &storage,
        &backup,
        "original-batch-backup",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    for (index, receipt) in accepted.iter().enumerate() {
        repository
            .erase_live(
                mutation(&repository, receipt, &format!("batch-backup-erase-{index}")).await,
            )
            .await
            .unwrap();
    }
    let intents = storage.erasure_journal().unwrap().intents().unwrap();
    assert_eq!(intents.len(), 2);
    crate::snapshot::restore_named(&storage, &backup, storage.erasure_journal().unwrap())
        .await
        .expect(
            "all restored original payloads must be removed before the first physical checkpoint",
        );
    assert!(storage.recovery_status().ready);
    for receipt in &accepted {
        assert_absent(&storage, receipt).await;
        assert_eq!(
            repository
                .head(&receipt.space_id, &receipt.source_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            SourceVisibility::Erased
        );
        assert_eq!(
            repository
                .operation(&receipt.space_id, &receipt.operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            OperationState::Superseded,
            "restore cannot synthesize a later erase receipt"
        );
    }
    assert_eq!(
        SourceRepository::new(storage.clone())
            .get("erase-a", &survivor.source_id)
            .await
            .unwrap(),
        Some(survivor_source)
    );
    assert_eq!(
        storage.erasure_journal().unwrap().intents().unwrap(),
        intents
    );
}

#[tokio::test]
async fn embedded_batch_faults_keep_all_original_receipts_pending_until_checkpoint() {
    Box::pin(original_batch_faults(true)).await;
}
#[tokio::test]
async fn native_batch_faults_keep_all_original_receipts_pending_until_checkpoint() {
    Box::pin(original_batch_faults(false)).await;
}
#[tokio::test]
async fn embedded_live_batch_discovers_reserved_gap_and_preserves_fresh_incarnation() {
    Box::pin(live_batch_preserves_fresh_incarnation(true)).await;
}
#[tokio::test]
async fn native_live_batch_discovers_reserved_gap_and_preserves_fresh_incarnation() {
    Box::pin(live_batch_preserves_fresh_incarnation(false)).await;
}
#[tokio::test]
async fn embedded_old_snapshot_replays_all_original_erasures_before_ready() {
    Box::pin(old_snapshot_replays_multiple_original_intents(true)).await;
}
#[tokio::test]
async fn native_old_snapshot_replays_all_original_erasures_before_ready() {
    Box::pin(old_snapshot_replays_multiple_original_intents(false)).await;
}
