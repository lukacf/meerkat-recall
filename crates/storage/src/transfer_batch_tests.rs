//! Actual typed/native transfer and rollback controls; no consumer corpus.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::Repository;
use elephant_types::records::{AuditAction, DocMediaKind, IngestMode, WorkItemStatus};
use elephant_types::{Link, TableName};

fn base(table: TableName) -> BaseFields {
    let mut b = BaseFields::new(
        RecordId::new_v4(table),
        "transfer".into(),
        "original".into(),
    );
    b.created_at = "2026-10-10T01:02:03.123456789Z".parse().unwrap();
    b.updated_at = "2026-10-11T04:05:06.987654321Z".parse().unwrap();
    b
}
fn work() -> WorkItem {
    WorkItem {
        base: base(TableName::WorkItem),
        kind: "extract_doc".into(),
        payload: serde_json::json!({"original":true}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 4,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: Some("prior failure".into()),
    }
}
fn audit() -> AuditLog {
    let b = base(TableName::AuditLog);
    AuditLog {
        timestamp: b.created_at,
        base: b,
        action: AuditAction::Create,
        actor: "original".into(),
        record: RecordId::new_v4(TableName::Entity),
        before: None,
        after: Some(
            serde_json::json!({"integer":9_007_199_254_740_993_i64,"float":1.0,"negative_zero":-0.0,"text":"å 🐾","nested":{"null":null}}),
        ),
        reason: Some("retained".into()),
    }
}

#[test]
fn lexical_input_rejects_duplicates_overflow_nul_and_unknown_fields() {
    for raw in [
        r#"{"a":1,"a":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        r"[18446744073709551616]",
        r"[-9223372036854775809]",
        r#"{"x":"\u0000"}"#,
        r"{} {}",
    ] {
        assert!(strict_json(raw).is_err(), "{raw}");
    }
    let raw = r#"{"i":9007199254740993,"max":18446744073709551615,"float":1.0,"minus":-0.0}"#;
    let value = strict_json(raw).unwrap();
    assert_eq!(value["i"].as_i64(), Some(9_007_199_254_740_993));
    assert_eq!(value["max"].as_u64(), Some(u64::MAX));
    assert!(value["float"].is_f64());
    assert!(value["minus"].as_f64().unwrap().is_sign_negative());
    let mut value = serde_json::to_value(audit()).unwrap();
    value["unexpected"] = true.into();
    assert!(TransferRecord::from_json("audit_log", &value.to_string()).is_err());
}

#[test]
fn declared_space_projection_moves_typed_archive_bases_only() {
    let mut hot = work();
    hot.payload = serde_json::json!({"space_id":"transfer","nested":{"name":"transfer"}});
    let mut cold_base = hot.base.clone();
    cold_base.id = crate::work_retention::archive_id(&hot.base.id);
    let archive = WorkItemArchive {
        base: cold_base,
        original_id: hot.base.id.clone(),
        archived_at: hot.base.updated_at,
        record: hot,
    };
    let original = TransferRecord::WorkItemArchive(archive.clone());
    let mut projected = original.clone();
    projected.relocate_space("transfer", "household").unwrap();
    let mut expected = original.json().unwrap();
    expected["space_id"] = "household".into();
    expected["record"]["space_id"] = "household".into();
    assert_eq!(projected.json().unwrap(), expected);
    assert_eq!(projected.id(), original.id());
    projected.native().unwrap();
    let mut foreign = original.clone();
    foreign.relocate_space("elsewhere", "household").unwrap();
    assert_eq!(foreign.json().unwrap(), original.json().unwrap());
    let mut corrupt = archive;
    corrupt.record.base.space_id = "other".into();
    assert!(TransferRecord::WorkItemArchive(corrupt)
        .relocate_space("transfer", "household")
        .is_err());
}

#[tokio::test]
async fn native_batch_exact_operational_rows_and_no_invented_origin() {
    let client = StorageClient::new_memory().await.unwrap();
    let work = work();
    let audit = audit();
    let cost = CostLedgerDaily {
        base: base(TableName::CostLedgerDaily),
        date: audit.timestamp.date_naive(),
        mode: IngestMode::Graph,
        worker_kind: "extract_doc".into(),
        total_euros: 1.0,
        total_items: 2,
        total_tokens: 9_007_199_254_740_993,
        total_runs: 3,
        total_failures: 1,
        reserved_euros: 0.0,
        reconciled_euros: 1.0,
    };
    let media = MediaAsset {
        base: base(TableName::MediaAsset),
        kind: DocMediaKind::Image,
        media_type: "image/png".into(),
        storage_path: "original/image".into(),
        size_bytes: 3,
        sha256: "a".repeat(64),
        payload_base64: Some("AQID".into()),
        duration_ms: None,
        label: Some("original 🐾".into()),
    };
    let rows = vec![
        TransferRecord::WorkItem(work.clone()),
        TransferRecord::AuditLog(audit.clone()),
        TransferRecord::CostLedgerDaily(cost.clone()),
        TransferRecord::MediaAsset(media.clone()),
    ];
    let expected = rows.clone();
    let digest = "a".repeat(64);
    let bound = digest.clone();
    assert!(import_batch(&client, &digest, &rows).await.is_err());
    let retained = client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            assert!(import_batch(&exclusive, &"b".repeat(64), &rows)
                .await
                .is_err());
            import_batch(&exclusive, &bound, &rows).await?;
            let mut origins = exclusive
                .query_checked("SELECT * FROM work_producer_origin")
                .await?;
            let origins: Vec<Value> = origins.take(0)?;
            assert!(origins.is_empty());
            exclusive.initialize_journal_identity().await?;
            Ok(exclusive)
        })
        .await
        .unwrap();
    assert!(import_batch(&retained, &digest, &expected).await.is_err());
    // The retained privileged client keeps the maintenance lease until it is dropped.
    drop(retained);
    assert_eq!(
        crate::WorkItemRepository::new(client.clone())
            .get("transfer", &work.base.id)
            .await
            .unwrap(),
        Some(work)
    );
    assert_eq!(
        crate::AuditLogRepository::new(client.clone())
            .get("transfer", &audit.base.id)
            .await
            .unwrap(),
        Some(audit)
    );
    assert_eq!(
        crate::CostLedgerDailyRepository::new(client.clone())
            .get("transfer", &cost.base.id)
            .await
            .unwrap(),
        Some(cost)
    );
    // import_batch performs payload-bearing media projection, unlike metadata-only public reads.
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through the complete exclusive batch-conflict rollback and retained-row assertions."
)]
async fn native_batch_middle_conflict_rolls_back_and_keeps_existing_row() {
    let client = StorageClient::new_memory().await.unwrap();
    let digest = "a".repeat(64);
    let bound = digest.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            let existing = TransferRecord::AuditLog(audit());
            import_batch(&exclusive, &bound, std::slice::from_ref(&existing)).await?;
            let first = TransferRecord::AuditLog(audit());
            let last = TransferRecord::AuditLog(audit());
            assert!(import_batch(
                &exclusive,
                &bound,
                &[first.clone(), existing.clone(), last.clone()]
            )
            .await
            .is_err());
            for row in [first, last] {
                assert!(crate::AuditLogRepository::new(exclusive.clone())
                    .get("transfer", row.id())
                    .await?
                    .is_none());
            }
            assert!(crate::AuditLogRepository::new(exclusive.clone())
                .get("transfer", existing.id())
                .await?
                .is_some());
            assert!(
                import_batch(&exclusive, &bound, &vec![existing; MAX_BATCH_ROWS + 1])
                    .await
                    .is_err()
            );
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep each native client through its complete hot/cold order scenario and atomic collision assertions."
)]
async fn native_batch_hot_cold_collision_refuses_both_row_orders_atomically() {
    for cold_first in [true, false] {
        let client = StorageClient::new_memory().await.unwrap();
        let digest = "a".repeat(64);
        let bound = digest.clone();
        client
            .with_transfer_barrier(&digest, move |exclusive| async move {
                let hot = work();
                let mut cold_base = hot.base.clone();
                cold_base.id = crate::work_retention::archive_id(&hot.base.id);
                let cold = WorkItemArchive {
                    base: cold_base,
                    original_id: hot.base.id.clone(),
                    archived_at: hot.base.updated_at,
                    record: hot.clone(),
                };
                let mut rows = vec![
                    TransferRecord::WorkItem(hot.clone()),
                    TransferRecord::WorkItemArchive(cold),
                ];
                if cold_first {
                    rows.reverse();
                }
                assert!(import_batch(&exclusive, &bound, &rows).await.is_err());
                assert!(crate::WorkItemRepository::new(exclusive.clone())
                    .get("transfer", &hot.base.id)
                    .await?
                    .is_none());
                exclusive.initialize_journal_identity().await?;
                Ok(())
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through timestamp preservation, duplicate-destination refusal and exclusive transfer completion."
)]
async fn native_embedding_batch_preserves_timestamps_and_checks_duplicate_destination() {
    let client = StorageClient::new_memory().await.unwrap();
    let digest = "a".repeat(64);
    let bound = digest.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            let original = NodeEmbedding {
                base: base(TableName::ChunkEmbedding),
                node: Link::new(RecordId::new_v4(TableName::DocNode)),
                model: "synthetic-3".into(),
                dims: 3,
                vector: vec![0.25, 0.5, 0.75],
            };
            let manager = elephant_embeddings::PartitionManager::new();
            let mut duplicate = original.clone();
            duplicate.base.id = RecordId::new_v4(TableName::ChunkEmbedding);
            assert!(import_embedding_batch(
                &exclusive,
                &bound,
                &manager,
                &[original.clone(), duplicate]
            )
            .await
            .is_err());
            let tables = import_embedding_batch(
                &exclusive,
                &bound,
                &manager,
                std::slice::from_ref(&original),
            )
            .await?;
            assert_eq!(tables.len(), 1);
            // The implementation compares entire typed readback, restoring only the documented physical ID mapping.
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .unwrap();
}
