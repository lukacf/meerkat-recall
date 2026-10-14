//! Real filesystem recovery through the managed storage entrypoint.
//! Process-kill qualification is independently owned by scenarios/crash_support.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::{Repository, StorageBackendKind, StorageClient, StorageConfig};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::{WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};

fn config() -> StorageConfig {
    StorageConfig {
        backend: StorageBackendKind::ElephantDb,
        ..StorageConfig::default()
    }
}

async fn open(root: &Path) -> StorageClient {
    StorageClient::new_with_durability(
        config(),
        Some(root.join("live.snapshot")),
        Some(root.join("journal")),
    )
    .await
    .unwrap()
}

fn entity(name: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "quiet-named-space".into(),
            "fixture".into(),
        ),
        EntityType::new("project"),
        name.into(),
    )
}

fn wal_path(root: &Path) -> PathBuf {
    root.join("live.snapshot.durable/commits.wal")
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn acknowledged_mutations_reopen_without_export_or_shutdown_save() {
    let directory = tempfile::tempdir().unwrap();
    let first = entity("accepted row");
    let second = entity("same transaction");
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        store.create(first.clone()).await.unwrap();
        store.mutate(|tx| {
            tx.create(second.clone())?;
            tx.upsert_extension_row("durability_probe", serde_json::json!({"id":"durability_probe:receipt", "space_id":"quiet-named-space", "value":"atomic receipt"}))?;
            Ok(())
        }).await.unwrap();
        // The initial live checkpoint still predates these writes: WAL is the
        // only recovery authority for their acknowledged transactions.
        let checkpoint: LiveCheckpoint =
            read_json(&directory.path().join("live.snapshot")).unwrap();
        assert_eq!(checkpoint.sequence, 0);
        assert!(!checkpoint.data.contains("accepted row"));
    }
    let client = open(directory.path()).await;
    assert!(client.recovery_status().ready);
    let store = client.embedded_store().unwrap();
    assert_eq!(
        store
            .get::<Entity>("quiet-named-space", &first.base.id, false)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        store
            .get::<Entity>("quiet-named-space", &second.base.id, false)
            .await
            .unwrap(),
        Some(second)
    );
    let receipts = store
        .mutate(|tx| Ok(tx.scan_extension_rows("durability_probe")?))
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn live_checkpoint_preserves_exact_running_claim_but_backup_refuses_it() {
    let directory = tempfile::tempdir().unwrap();
    let claim;
    {
        let client = open(directory.path()).await;
        let work = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "quiet-named-space".into(),
                "fixture".into(),
            ),
            kind: "fixture".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "durable-claim".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let repo = crate::WorkItemRepository::new(client.clone());
        repo.create(work.clone()).await.unwrap();
        claim = repo
            .claim_with_lease(
                &work.base.space_id,
                &work.base.id,
                "original-worker",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        client.embedded_store().unwrap().checkpoint().await.unwrap();
        assert!(crate::snapshot::export_named(
            &client,
            &directory.path().join("backup"),
            "running",
            &crate::snapshot::SnapshotRetention::default()
        )
        .await
        .is_err());
        assert!(client.recovery_status().ready);
    }
    let reopened = open(directory.path()).await;
    let item = crate::WorkItemRepository::new(reopened)
        .get(&claim.base.space_id, &claim.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item, claim);
    assert_eq!(
        crate::work_attempt_repository::fence_from_claim(&item).unwrap(),
        crate::work_attempt_repository::fence_from_claim(&claim).unwrap()
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn append_failure_blocks_escaped_and_maintenance_handles_until_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let accepted = entity("first accepted");
    let rejected = entity("not committed");
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        let privileged = store.maintenance_view();
        store.create(accepted.clone()).await.unwrap();
        let durability = store.durability_for_test().unwrap();
        durability.fail_next_append.store(true, Ordering::Release);
        client
            .with_activity(async {
                assert!(store.create(rejected.clone()).await.is_err());
                assert!(
                    store.scan::<Entity>().await.is_err(),
                    "existing activity cannot bypass poison"
                );
            })
            .await
            .unwrap();
        assert!(!client.recovery_status().ready);
        assert!(privileged.scan::<Entity>().await.is_err());
        assert!(
            client
                .begin_maintenance(
                    crate::recovery_barrier::RecoveryPhase::Restoring,
                    std::time::Duration::from_secs(1)
                )
                .await
                .is_err(),
            "a restore guard cannot reset sticky readiness after ambiguous I/O"
        );
        assert!(!client.recovery_status().ready);
        assert!(store.create(entity("later write")).await.is_err());
    }
    let client = open(directory.path()).await;
    let store = client.embedded_store().unwrap();
    assert!(store
        .get::<Entity>("quiet-named-space", &accepted.base.id, false)
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get::<Entity>("quiet-named-space", &rejected.base.id, false)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn ambiguous_synced_frame_replays_atomically_after_poisoned_process() {
    let directory = tempfile::tempdir().unwrap();
    let first = entity("uncertain transaction first");
    let second = entity("uncertain transaction second");
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        store
            .durability_for_test()
            .unwrap()
            .fail_after_append_sync
            .store(true, Ordering::Release);
        assert!(store
            .mutate(|tx| {
                tx.create(first.clone())?;
                tx.create(second.clone())?;
                Ok(())
            })
            .await
            .is_err());
        assert!(!client.recovery_status().ready);
        assert!(store.scan::<Entity>().await.is_err());
    }
    // A failure after fsync is an uncertain result, never permission to continue
    // from rolled-back memory. Restart resolves it as one complete transaction.
    let reopened = open(directory.path()).await;
    let rows = reopened
        .embedded_store()
        .unwrap()
        .scan::<Entity>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.base.id == first.base.id));
    assert!(rows.iter().any(|row| row.base.id == second.base.id));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn incomplete_tail_is_removed_but_complete_frame_corruption_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let row = entity("survives torn suffix");
    {
        let client = open(directory.path()).await;
        client
            .embedded_store()
            .unwrap()
            .create(row.clone())
            .await
            .unwrap();
    }
    {
        let mut file = open_log(&wal_path(directory.path())).unwrap();
        file.write_all(&FRAME_MAGIC[..4]).unwrap();
        file.sync_all().unwrap();
    }
    {
        let reopened = open(directory.path()).await;
        assert!(reopened
            .embedded_store()
            .unwrap()
            .get::<Entity>("quiet-named-space", &row.base.id, false)
            .await
            .unwrap()
            .is_some());
    }
    let path = wal_path(directory.path());
    let mut bytes = fs::read(&path).unwrap();
    let last_payload_byte = bytes.len() - COMMIT_MAGIC.len() - 1;
    bytes[last_payload_byte] ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(StorageClient::new_with_durability(
        config(),
        Some(directory.path().join("live.snapshot")),
        Some(directory.path().join("journal"))
    )
    .await
    .is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn forced_purge_checkpoint_removes_payload_from_live_files() {
    let directory = tempfile::tempdir().unwrap();
    let private = entity("ERASURE_LIVE_FILE_CANARY_98173");
    let sentinel = entity("independent sentinel");
    let client = open(directory.path()).await;
    let store = client.embedded_store().unwrap();
    store.create(private.clone()).await.unwrap();
    store.create(sentinel.clone()).await.unwrap();
    store
        .hard_delete::<Entity>("quiet-named-space", &private.base.id)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&fs::read(wal_path(directory.path())).unwrap())
            .contains("ERASURE_LIVE_FILE_CANARY_98173")
    );
    store.checkpoint().await.unwrap();
    assert_no_live_canary(directory.path(), "ERASURE_LIVE_FILE_CANARY_98173");
    assert!(store
        .get::<Entity>("quiet-named-space", &sentinel.base.id, false)
        .await
        .unwrap()
        .is_some());
    assert!(!directory.path().join("live.snapshot.previous").exists());
}

fn assert_no_live_canary(root: &Path, canary: &str) {
    assert!(
        !String::from_utf8_lossy(&fs::read(root.join("live.snapshot")).unwrap()).contains(canary)
    );
    for entry in fs::read_dir(root.join("live.snapshot.durable")).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        assert!(
            !String::from_utf8_lossy(&fs::read(entry.path()).unwrap()).contains(canary),
            "retained payload in {}",
            entry.path().display()
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn crash_boundary_after_checkpoint_before_log_reset_cannot_resurrect_purged_rows() {
    let directory = tempfile::tempdir().unwrap();
    let private = entity("CHECKPOINT_BOUNDARY_CANARY_54012");
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        store.create(private.clone()).await.unwrap();
        store
            .hard_delete::<Entity>("quiet-named-space", &private.base.id)
            .await
            .unwrap();
        store
            .durability_for_test()
            .unwrap()
            .fail_after_checkpoint_publish
            .store(true, Ordering::Release);
        assert!(store.checkpoint().await.is_err());
        assert!(!client.recovery_status().ready);
    }
    let reopened = open(directory.path()).await;
    assert!(reopened
        .embedded_store()
        .unwrap()
        .get::<Entity>("quiet-named-space", &private.base.id, false)
        .await
        .unwrap()
        .is_none());
    assert_no_live_canary(directory.path(), "CHECKPOINT_BOUNDARY_CANARY_54012");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn concurrent_mutations_and_checkpoints_capture_one_exact_sequence_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let expected: std::collections::BTreeSet<_>;
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        let rows: Vec<_> = (0..20)
            .map(|index| entity(&format!("concurrent {index}")))
            .collect();
        expected = rows.iter().map(|row| row.base.id.clone()).collect();
        let mut tasks = Vec::new();
        for row in rows {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store.create(row).await.unwrap();
            }));
        }
        for _ in 0..5 {
            store.checkpoint().await.unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
    let reopened = open(directory.path()).await;
    let actual: std::collections::BTreeSet<_> = reopened
        .embedded_store()
        .unwrap()
        .scan::<Entity>()
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.base.id)
        .collect();
    assert_eq!(expected, actual);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn guarded_restore_replaces_the_old_live_log_generation() {
    let directory = tempfile::tempdir().unwrap();
    let before = entity("chosen backup state");
    let later = entity("must not replay after restore");
    {
        let client = open(directory.path()).await;
        let store = client.embedded_store().unwrap();
        store.create(before.clone()).await.unwrap();
        let backup = directory.path().join("chosen-backup");
        crate::snapshot::export_named(
            &client,
            &backup,
            "chosen",
            &crate::snapshot::SnapshotRetention::default(),
        )
        .await
        .unwrap();
        store.create(later.clone()).await.unwrap();
        crate::snapshot::restore_named(&client, &backup, client.erasure_journal().unwrap())
            .await
            .unwrap();
        assert!(store
            .get::<Entity>("quiet-named-space", &later.base.id, false)
            .await
            .unwrap()
            .is_none());
    }
    let reopened = open(directory.path()).await;
    let store = reopened.embedded_store().unwrap();
    assert!(store
        .get::<Entity>("quiet-named-space", &before.base.id, false)
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get::<Entity>("quiet-named-space", &later.base.id, false)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn missing_log_or_foreign_journal_never_opens_an_empty_replacement_store() {
    let directory = tempfile::tempdir().unwrap();
    {
        let client = open(directory.path()).await;
        client
            .embedded_store()
            .unwrap()
            .create(entity("acknowledged"))
            .await
            .unwrap();
    }
    assert!(StorageClient::new_with_durability(
        config(),
        Some(directory.path().join("live.snapshot")),
        Some(directory.path().join("foreign-journal"))
    )
    .await
    .is_err());
    fs::remove_file(wal_path(directory.path())).unwrap();
    assert!(StorageClient::new_with_durability(
        config(),
        Some(directory.path().join("live.snapshot")),
        Some(directory.path().join("journal"))
    )
    .await
    .is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn corrupted_frame_length_cannot_be_misclassified_as_an_incomplete_tail() {
    let directory = tempfile::tempdir().unwrap();
    {
        let client = open(directory.path()).await;
        client
            .embedded_store()
            .unwrap()
            .create(entity("acknowledged frame"))
            .await
            .unwrap();
    }
    let path = wal_path(directory.path());
    let mut bytes = fs::read(&path).unwrap();
    let header_length = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let first_frame = 12 + header_length + 32;
    // Inflate the length of a complete acknowledged transaction. The separately
    // checked header checksum must reject this before treating it as a torn tail.
    let length_start = first_frame + 16;
    bytes[length_start + 6] ^= 0x10;
    fs::write(&path, bytes).unwrap();
    assert!(StorageClient::new_with_durability(
        config(),
        Some(directory.path().join("live.snapshot")),
        Some(directory.path().join("journal"))
    )
    .await
    .is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn checkpoint_sequence_corruption_cannot_skip_acknowledged_log_frames() {
    let directory = tempfile::tempdir().unwrap();
    {
        let client = open(directory.path()).await;
        assert_eq!(
            client.effective_snapshot_destination(),
            Some(directory.path().join("live.snapshot").as_path())
        );
        client
            .embedded_store()
            .unwrap()
            .create(entity("must not be skipped"))
            .await
            .unwrap();
    }
    let path = directory.path().join("live.snapshot");
    let mut checkpoint: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    checkpoint["sequence"] = serde_json::json!(1);
    fs::write(&path, serde_json::to_vec(&checkpoint).unwrap()).unwrap();
    assert!(StorageClient::new_with_durability(
        config(),
        Some(path),
        Some(directory.path().join("journal"))
    )
    .await
    .is_err());
}
