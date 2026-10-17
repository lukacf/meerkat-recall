#![allow(clippy::expect_used)]
use super::*;
use crate::WorkItemRepository;
use chrono::{Duration, Utc};
use elephant_types::records::{BaseFields, WorkItemStatus};

async fn install_schema(client: &StorageClient) {
    if client.embedded_store().is_none() {
        for schema in [
            include_str!("../../migrations/sql/028_work_queue_indexes.surql"),
            include_str!("../../migrations/sql/029_work_history_archives.surql"),
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            client.query_checked(schema).await.expect("schema");
        }
    }
}

async fn storage(embedded: bool) -> StorageClient {
    let client = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("storage");
    install_schema(&client).await;
    client
}

fn item() -> WorkItem {
    WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "historical".into(),
            "original-actor".into(),
        ),
        kind: "extract_doc".into(),
        payload: serde_json::json!({"original": "untouched"}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 4,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: Some("original failure".into()),
    }
}

async fn raw_create(client: &StorageClient, record: WorkItem) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| tx.create(record).map_err(StorageError::from))
            .await
            .expect("original row");
    } else {
        client
            .query_bind_checked(
                "CREATE $id CONTENT $record",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => WorkItemRepository::work_item_to_value(&record).expect("row"),
                },
            )
            .await
            .expect("original row");
    }
}

async fn attempt_rows(client: &StorageClient) -> serde_json::Value {
    if let Some(store) = client.embedded_store() {
        let rows = store
            .mutate(|tx| {
                tx.scan_extension_rows("work_attempt")
                    .map_err(StorageError::from)
            })
            .await
            .expect("attempts");
        serde_json::to_value(rows).expect("attempt rows")
    } else {
        let mut response = client
            .query_checked("SELECT * FROM work_attempt ORDER BY id")
            .await
            .expect("attempts");
        let rows: Vec<surrealdb::sql::Value> = response.take(0).expect("attempt rows");
        serde_json::to_value(rows).expect("attempt rows")
    }
}

async fn expire(client: &StorageClient, id: &RecordId) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                let mut row = tx.get::<WorkItem>("historical", id, false)?.expect("work");
                row.lease_expires_at = Some(Utc::now() - Duration::hours(1));
                tx.replace(row)?;
                Ok::<(), StorageError>(())
            })
            .await
            .expect("expire known lease");
    } else {
        client.query_bind_checked("UPDATE $id SET lease_expires_at = $expired", crate::bindings! {
            "id" => crate::surreal::thing_from_record_id(id),
            "expired" => crate::surreal::datetime_from_chrono(Utc::now() - Duration::hours(1)),
        }).await.expect("expire known lease");
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Fixture repositories retain the store through all assertions."
)]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
async fn hold_contract(embedded: bool, unknown_marker: bool) {
    let client = storage(embedded).await;
    let repo = WorkItemRepository::new(client.clone());
    let runnable = item();
    let mut running = item();
    running.status = WorkItemStatus::Running;
    running.leased_by = Some("old-host-lease".into());
    running.lease_expires_at = Some(Utc::now() - Duration::hours(1));
    for record in [&runnable, &running] {
        raw_create(&client, record.clone()).await;
        if unknown_marker {
            let envelope =
                OriginEnvelope::new(record, WorkProducerOrigin::Unknown, &[]).expect("unknown");
            if let Some(store) = client.embedded_store() {
                store
                    .mutate(|tx| insert_tx(tx, &envelope))
                    .await
                    .expect("durable unknown");
            } else {
                let mut sql = "BEGIN TRANSACTION;".to_string();
                let mut bindings = BTreeMap::new();
                append_insert(&mut sql, &mut bindings, "old", &envelope).expect("binding");
                sql.push_str("COMMIT TRANSACTION;");
                client
                    .query_bind_checked(&sql, bindings)
                    .await
                    .expect("durable unknown");
            }
        }
    }
    let original_attempts = attempt_rows(&client).await;
    assert!(repo
        .claim_with_lease(
            "historical",
            &runnable.base.id,
            "new-worker",
            Utc::now() + Duration::minutes(1)
        )
        .await
        .expect("refusal")
        .is_none());
    assert!(repo
        .release_expired_lease("historical", &running.base.id)
        .await
        .expect("hold")
        .is_none());
    assert_eq!(
        repo.recover_expired_leases_all_spaces()
            .await
            .expect("recovery"),
        0
    );
    for record in [&runnable, &running] {
        assert_eq!(
            repo.get("historical", &record.base.id)
                .await
                .expect("read")
                .expect("retained"),
            *record
        );
    }
    assert_eq!(attempt_rows(&client).await, original_attempts);
    // Held history cannot prevent unrelated known-producer work from executing.
    let fresh = repo.create(item()).await.expect("new direct producer");
    let claimed = repo
        .claim_with_lease(
            "historical",
            &fresh.base.id,
            "worker",
            Utc::now() + Duration::minutes(1),
        )
        .await
        .expect("claim")
        .expect("known producer");
    assert_eq!(claimed.attempts, fresh.attempts + 1);
    assert_eq!(claimed.status, WorkItemStatus::Running);
    expire(&client, &fresh.base.id).await;
    assert_eq!(
        repo.recover_expired_leases_all_spaces()
            .await
            .expect("known recovery"),
        1
    );
    let recovered = repo
        .get("historical", &fresh.base.id)
        .await
        .expect("read")
        .expect("known work");
    assert_eq!(recovered.status, WorkItemStatus::Runnable);
    assert_eq!(recovered.attempts, claimed.attempts);
    assert!(recovered.leased_by.is_none());
    assert!(repo
        .claim_with_lease(
            "historical",
            &fresh.base.id,
            "next",
            Utc::now() + Duration::minutes(1)
        )
        .await
        .expect("reclaim")
        .is_some());
    assert_eq!(
        repo.get("historical", &running.base.id)
            .await
            .expect("read")
            .expect("retained"),
        running
    );
}

#[tokio::test]
async fn missing_origin_holds_runnable_and_expired_history_embedded() {
    hold_contract(true, false).await;
}

#[tokio::test]
async fn missing_origin_holds_runnable_and_expired_history_native() {
    hold_contract(false, false).await;
}

#[tokio::test]
async fn unknown_origin_holds_runnable_and_expired_history_embedded() {
    hold_contract(true, true).await;
}

#[tokio::test]
async fn unknown_origin_holds_runnable_and_expired_history_native() {
    hold_contract(false, true).await;
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Fixture store retained for all queue observations."
)]
async fn held_page_fairness(embedded: bool) {
    let client = storage(embedded).await;
    let repo = WorkItemRepository::new(client.clone());
    for _ in 0..257 {
        let mut held = item();
        held.priority = 100;
        raw_create(&client, held).await;
    }
    let mut held_running = item();
    held_running.status = WorkItemStatus::Running;
    held_running.leased_by = Some("prior-host".into());
    held_running.lease_expires_at = Some(Utc::now() + Duration::days(1));
    raw_create(&client, held_running).await;
    let fresh = repo.create(item()).await.expect("known producer");
    assert_eq!(
        repo.get_runnable("historical", 1)
            .await
            .expect("space page"),
        vec![fresh.clone()]
    );
    assert_eq!(
        repo.find_runnable_all_spaces(1).await.expect("all page"),
        vec![fresh.clone()]
    );
    assert_eq!(
        repo.find_runnable_by_kind_all_spaces("extract_doc", 1)
            .await
            .expect("kind page"),
        vec![fresh.clone()]
    );
    assert!(repo
        .find_runnable_by_kind_all_spaces("other", 1)
        .await
        .expect("kind isolation")
        .is_empty());
    assert_eq!(repo.count_runnable().await.expect("queue count"), 1);
    assert_eq!(
        repo.count_running_by_kind("historical", "extract_doc")
            .await
            .expect("concurrency"),
        0
    );
    repo.claim_with_lease(
        "historical",
        &fresh.base.id,
        "worker",
        Utc::now() + Duration::minutes(1),
    )
    .await
    .expect("claim")
    .expect("known");
    assert_eq!(
        repo.count_running_by_kind("historical", "extract_doc")
            .await
            .expect("concurrency"),
        1
    );
    assert_eq!(repo.count_runnable().await.expect("queue count"), 0);
}

#[tokio::test]
async fn historical_hold_does_not_starve_next_page_embedded() {
    held_page_fairness(true).await;
}
#[tokio::test]
async fn historical_hold_does_not_starve_next_page_native() {
    held_page_fairness(false).await;
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Retained maintenance client is a required post-completion negative control."
)]
async fn registration_contract(embedded: bool) {
    let client = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("empty client");
    let original = item();
    let digest = "a".repeat(64);
    assert!(WorkProducerRepository::new(client.clone())
        .register_historical_hold(&original, &digest, &"b".repeat(64))
        .await
        .is_err());
    let expected = original.clone();
    let source_digest = digest.clone();
    let (retained, payload) = client.with_transfer_barrier(&digest, move |exclusive| async move {
        install_schema(&exclusive).await;
        raw_create(&exclusive, original.clone()).await;
        let producer = WorkProducerRepository::new(exclusive.clone());
        assert!(producer.register_historical_hold(&original, &"c".repeat(64), &"b".repeat(64)).await.is_err());
        let mut changed = original.clone(); changed.payload = serde_json::json!({"changed":true});
        assert!(producer.register_historical_hold(&changed, &source_digest, &"b".repeat(64)).await.is_err());
        if !embedded {
            exclusive.query_bind_checked("UPDATE $id SET unexpected_original_field=true", crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&original.base.id)}).await?;
            assert!(producer.register_historical_hold(&original, &source_digest, &"b".repeat(64)).await.is_err());
            exclusive.query_bind_checked("UPDATE $id UNSET unexpected_original_field", crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&original.base.id)}).await?;
        }
        let payload = producer.register_historical_hold(&original, &source_digest, &"b".repeat(64)).await?;
        assert_eq!(producer.register_historical_hold(&original, &source_digest, &"b".repeat(64)).await?, payload);
        assert!(producer.register_historical_hold(&original, &source_digest, &"c".repeat(64)).await.is_err());
        exclusive.initialize_journal_identity().await?;
        Ok((exclusive, payload))
    }).await.expect("reconciled transfer");
    assert_eq!(payload.len(), 64);
    assert!(client.recovery_status().ready);
    assert!(WorkProducerRepository::new(retained)
        .register_historical_hold(&expected, &digest, &"b".repeat(64))
        .await
        .is_err());
    let repo = WorkItemRepository::new(client.clone());
    assert_eq!(
        repo.get("historical", &expected.base.id)
            .await
            .expect("get")
            .expect("original"),
        expected
    );
    assert!(repo
        .claim_with_lease(
            "historical",
            &expected.base.id,
            "worker",
            Utc::now() + Duration::minutes(1)
        )
        .await
        .expect("held")
        .is_none());
    let envelope = WorkProducerRepository::new(client)
        .read_envelope(&expected)
        .await
        .expect("private history")
        .expect("marker");
    assert_eq!(envelope.origin, WorkProducerOrigin::Unknown);
    assert!(envelope.historical_transfer.is_some());
    assert_eq!(
        verify_original_control_payload(&expected, &envelope.payload().expect("payload"))
            .expect("restore validation"),
        WorkProducerOrigin::Unknown
    );
}
#[tokio::test]
async fn historical_marker_custody_and_completion_embedded() {
    registration_contract(true).await;
}
#[tokio::test]
async fn historical_marker_custody_and_completion_native() {
    registration_contract(false).await;
}

#[test]
fn historical_proof_is_optional_and_never_dispatch_authority() {
    let work = item();
    let ordinary = OriginEnvelope::new(&work, WorkProducerOrigin::Direct, &[]).expect("direct");
    let payload = ordinary.payload().expect("payload");
    assert!(!payload.contains("historical_transfer"));
    assert_eq!(
        serde_json::from_str::<OriginEnvelope>(&payload).expect("legacy decode"),
        ordinary
    );
    let mut historical =
        OriginEnvelope::new(&work, WorkProducerOrigin::Unknown, &[]).expect("unknown");
    historical.historical_transfer = Some(HistoricalTransferProof {
        version: 1,
        previous_hold_sha256: None,
        snapshot_sha256: "a".repeat(64),
        original_row_sha256: "b".repeat(64),
        hash_domain: "homecore-python-json-v1".into(),
        source_space_id: work.base.space_id.clone(),
        target_space_id: work.base.space_id.clone(),
    });
    historical.verify(&work).expect("inert proof");
    historical.origin = WorkProducerOrigin::Direct;
    assert!(historical.verify(&work).is_err());
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Compare original work/attempts before and after all autonomous transitions."
)]
async fn held_pending_failed_and_archive(embedded: bool) {
    let client = storage(embedded).await;
    let repo = WorkItemRepository::new(client.clone());
    let attempts = attempt_rows(&client).await;
    for status in [WorkItemStatus::Pending, WorkItemStatus::Failed] {
        let mut original = item();
        original.status = status;
        original.base.updated_at = Utc::now() - Duration::days(2);
        raw_create(&client, original.clone()).await;
        let mut reset = original.clone();
        reset.status = WorkItemStatus::Runnable;
        reset.attempts = 0;
        reset.last_error = None;
        assert!(repo
            .update_if_status("historical", &original.base.id, reset.clone(), status)
            .await
            .expect("held reset")
            .is_none());
        assert!(repo
            .update_if_status_and_dependencies_done("historical", &original.base.id, reset, status)
            .await
            .expect("held promotion")
            .is_none());
        assert_eq!(
            repo.get("historical", &original.base.id)
                .await
                .expect("get")
                .expect("original"),
            original
        );
        if status == WorkItemStatus::Failed {
            let archived = crate::work_retention::archive_terminal_work(
                &client,
                Utc::now() - Duration::days(1),
                10,
            )
            .await
            .expect("archive");
            assert_eq!(archived.items_archived, 1);
            assert_eq!(
                repo.get("historical", &original.base.id)
                    .await
                    .expect("cold get")
                    .expect("original"),
                original
            );
            let mut reset = original.clone();
            reset.status = WorkItemStatus::Runnable;
            reset.attempts = 0;
            assert!(repo
                .update_if_status("historical", &original.base.id, reset, status)
                .await
                .expect("cold held reset")
                .is_none());
            assert_eq!(
                repo.get("historical", &original.base.id)
                    .await
                    .expect("cold get")
                    .expect("original"),
                original
            );
        }
        let mut ordinary = item();
        ordinary.status = status;
        let ordinary = repo.create(ordinary).await.expect("known producer");
        let mut next = ordinary.clone();
        next.status = WorkItemStatus::Runnable;
        assert!(repo
            .update_if_status_and_dependencies_done("historical", &ordinary.base.id, next, status)
            .await
            .expect("known promotion")
            .is_some());
    }
    assert_eq!(attempt_rows(&client).await, attempts);
}
#[tokio::test]
async fn historical_pending_failed_archive_remain_inert_embedded() {
    held_pending_failed_and_archive(true).await;
}
#[tokio::test]
async fn historical_pending_failed_archive_remain_inert_native() {
    held_pending_failed_and_archive(false).await;
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Retain all original row and store comparisons."
)]
async fn budget_pause_preserves_history(embedded: bool) {
    use elephant_types::records::ingest::IngestMode;
    let client = storage(embedded).await;
    let repo = WorkItemRepository::new(client.clone());
    let mode = IngestMode::Graph;
    let mut originals = Vec::new();
    for status in [WorkItemStatus::Runnable, WorkItemStatus::Pending] {
        let mut work = item();
        work.status = status;
        work.payload = serde_json::json!({"mode":mode.to_string()});
        raw_create(&client, work.clone()).await;
        originals.push(work);
    }
    let mut ordinary = item();
    ordinary.payload = serde_json::json!({"mode":mode.to_string()});
    let ordinary = repo.create(ordinary).await.expect("known");
    let resume = Utc::now() + Duration::hours(1);
    assert_eq!(
        repo.pause_queued_by_mode("historical", mode, resume)
            .await
            .expect("pause"),
        1
    );
    for original in originals {
        assert_eq!(
            repo.get("historical", &original.base.id)
                .await
                .expect("get")
                .expect("work"),
            original
        );
    }
    assert_eq!(
        repo.get("historical", &ordinary.base.id)
            .await
            .expect("get")
            .expect("work")
            .run_after,
        Some(resume)
    );
}
#[tokio::test]
async fn historical_budget_pause_preserves_history_embedded() {
    budget_pause_preserves_history(true).await;
}
#[tokio::test]
async fn historical_budget_pause_preserves_history_native() {
    budget_pause_preserves_history(false).await;
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original transfer and producer custody through hot/archive rollback, completion and final inert-work assertions."
)]
async fn batched_hot_and_archive_registration(embedded: bool) {
    use crate::transfer_batch::TransferRecord;
    use elephant_types::records::WorkItemArchive;
    let client = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("empty target");
    let digest = "a".repeat(64);
    let expected_digest = digest.clone();
    let hot = item();
    let mut cold = item();
    cold.status = WorkItemStatus::Failed;
    let mut archive_base = cold.base.clone();
    archive_base.id = crate::work_retention::archive_id(&cold.base.id);
    let archive = WorkItemArchive {
        base: archive_base,
        original_id: cold.base.id.clone(),
        archived_at: Utc::now(),
        record: cold.clone(),
    };
    let expected_hot = hot.clone();
    let expected_cold = cold.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            install_schema(&exclusive).await;
            raw_create(&exclusive, hot.clone()).await;
            if let Some(store) = exclusive.embedded_store() {
                store
                    .mutate(|tx| tx.create(archive.clone()).map_err(StorageError::from))
                    .await?;
            } else {
                exclusive
                    .query_bind_checked(
                        "CREATE $id CONTENT $record",
                        crate::bindings! {
                            "id"=>crate::surreal::thing_from_record_id(&archive.base.id),
                            "record"=>TransferRecord::WorkItemArchive(archive.clone()).native()?,
                        },
                    )
                    .await?;
            }
            let repository = WorkProducerRepository::new(exclusive.clone());
            let mut original_hot = hot.clone();
            original_hot.base.space_id = "default".into();
            let mut original_archive = archive.clone();
            original_archive.base.space_id = "default".into();
            original_archive.record.base.space_id = "default".into();
            let originals = vec![
                HistoricalWorkHoldInput {
                    original: TransferRecord::WorkItem(original_hot),
                    original_row_sha256: "b".repeat(64),
                    source_space_id: "default".into(),
                    target_space_id: "historical".into(),
                },
                HistoricalWorkHoldInput {
                    original: TransferRecord::WorkItemArchive(original_archive),
                    original_row_sha256: "c".repeat(64),
                    source_space_id: "default".into(),
                    target_space_id: "historical".into(),
                },
            ];
            let mut wrong_mapping = originals.clone();
            wrong_mapping[0].source_space_id = "invented".into();
            assert!(repository
                .register_historical_holds(&expected_digest, &wrong_mapping)
                .await
                .is_err());
            let mut wrong = originals.clone();
            if let TransferRecord::WorkItemArchive(row) = &mut wrong[1].original {
                row.record.payload = serde_json::json!({"changed":true});
            }
            assert!(repository
                .register_historical_holds(&expected_digest, &wrong)
                .await
                .is_err());
            assert!(repository.read_origin_envelope(&hot).await?.is_none());
            assert!(repository.read_origin_envelope(&cold).await?.is_none());
            assert!(repository
                .register_historical_holds(
                    &expected_digest,
                    &[originals[0].clone(), originals[0].clone()]
                )
                .await
                .is_err());
            let hashes = repository
                .register_historical_holds(&expected_digest, &originals)
                .await?;
            assert_eq!(hashes.payload_sha256.len(), 2);
            assert_eq!(hashes.native_effects.len(), if embedded { 0 } else { 3 });
            assert!(hashes
                .native_effects
                .iter()
                .filter(|effect| effect.table == "work_producer_origin")
                .all(|effect| effect.before_sha256.is_none()));
            let replay = repository
                .register_historical_holds(&expected_digest, &originals)
                .await?;
            assert_eq!(replay.payload_sha256, hashes.payload_sha256);
            for effect in &replay.native_effects {
                assert_eq!(
                    effect.before_sha256.as_ref(),
                    Some(&effect.after_sha256),
                    "exact replay must not increment guard or rewrite origin"
                );
            }
            if !embedded {
                let guard = hashes
                    .native_effects
                    .iter()
                    .find(|effect| effect.table == "source_erasure_guard")
                    .expect("bounded guard effect");
                assert_ne!(guard.before_sha256.as_ref(), Some(&guard.after_sha256));
                let mut response = exclusive
                    .query_checked("SELECT revision FROM ONLY source_erasure_guard:historical;")
                    .await?;
                let before: Option<serde_json::Value> = response.take(0).expect("guard revision");
                // A poisoned reservation cannot be absorbed into the inventory.
                exclusive
                    .query_checked(
                        "UPDATE source_erasure_guard:historical SET erasure_reservation='foreign';",
                    )
                    .await?;
                assert!(repository
                    .register_historical_holds(&expected_digest, &originals)
                    .await
                    .is_err());
                exclusive
                    .query_checked(
                        "UPDATE source_erasure_guard:historical SET erasure_reservation=NONE;",
                    )
                    .await?;
                let mut response = exclusive
                    .query_checked("SELECT revision FROM ONLY source_erasure_guard:historical;")
                    .await?;
                let after: Option<serde_json::Value> = response.take(0).expect("guard revision");
                assert_eq!(
                    before, after,
                    "refused replay must not change generated revision"
                );
            }
            for work in [&hot, &cold] {
                let control = repository
                    .read_origin_envelope(work)
                    .await?
                    .expect("private hold");
                assert_eq!(control.origin, WorkProducerOrigin::Unknown);
                let proof = control.historical_transfer.as_ref().expect("mapping proof");
                assert_eq!(proof.source_space_id, "default");
                assert_eq!(proof.target_space_id, "historical");
                assert_eq!(
                    verify_original_control_payload(work, &control.payload()?)?,
                    WorkProducerOrigin::Unknown
                );
            }
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .expect("batch transfer");
    let repository = WorkItemRepository::new(client.clone());
    assert_eq!(
        repository
            .get("historical", &expected_hot.base.id)
            .await
            .expect("read")
            .expect("hot"),
        expected_hot
    );
    assert_eq!(
        repository
            .get("historical", &expected_cold.base.id)
            .await
            .expect("read")
            .expect("archive"),
        expected_cold
    );
    assert!(repository
        .claim_with_lease(
            "historical",
            &expected_hot.base.id,
            "worker",
            Utc::now() + Duration::minutes(1)
        )
        .await
        .expect("held")
        .is_none());
}

#[tokio::test]
async fn historical_hold_batch_hot_archive_atomic_embedded() {
    batched_hot_and_archive_registration(true).await;
}
#[tokio::test]
async fn historical_hold_batch_hot_archive_atomic_native() {
    batched_hot_and_archive_registration(false).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the native client and producer through the original depth failure, whole-page rollback, mixed replay and journal initialization."
)]
async fn historical_hold_full_page_depth_atomicity_and_mixed_replay_native() {
    use crate::transfer_batch::TransferRecord;

    async fn inventory(client: &StorageClient) -> StorageResult<(String, String)> {
        let mut response = client
            .query_checked(
                "SELECT * FROM work_producer_origin ORDER BY id; \
                 SELECT * FROM source_erasure_guard ORDER BY id;",
            )
            .await?;
        let origins: surrealdb::Value = response.take(0).expect("native origins");
        let guards: surrealdb::Value = response.take(1).expect("native guards");
        Ok((
            origins.into_inner().to_string(),
            guards.into_inner().to_string(),
        ))
    }

    let client = Box::pin(StorageClient::new_memory())
        .await
        .expect("empty target");
    // The prior left-associated expression really exceeds the unchanged engine limit.
    let old_sum = std::iter::repeat_n("(IF true THEN 1 ELSE 0 END)", 256)
        .collect::<Vec<_>>()
        .join(" + ");
    let error = client
        .query_checked(&format!("RETURN {old_sum};"))
        .await
        .expect_err("predecessor expression must hit the depth guard");
    assert!(error
        .to_string()
        .contains("Reached excessive computation depth"));

    let digest = "a".repeat(64);
    let expected_digest = digest.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            install_schema(&exclusive).await;
            let mut inputs = Vec::new();
            for _ in 0..384 {
                let work = item();
                raw_create(&exclusive, work.clone()).await;
                let mut original = work;
                original.base.space_id = "default".into();
                inputs.push(HistoricalWorkHoldInput {
                    original: TransferRecord::WorkItem(original),
                    original_row_sha256: "b".repeat(64),
                    source_space_id: "default".into(),
                    target_space_id: "historical".into(),
                });
            }
            let repository = WorkProducerRepository::new(exclusive.clone());
            let before = inventory(&exclusive).await?;
            let mut wrong = inputs[..256].to_vec();
            if let TransferRecord::WorkItem(work) = &mut wrong[255].original {
                work.payload = serde_json::json!({"changed": true});
            }
            assert!(repository
                .register_historical_holds(&expected_digest, &wrong)
                .await
                .is_err());
            assert_eq!(inventory(&exclusive).await?, before, "whole batch rollback");

            let first = repository
                .register_historical_holds(&expected_digest, &inputs[..256])
                .await?;
            assert_eq!(first.payload_sha256.len(), 256);
            assert_eq!(first.native_effects.len(), 257);
            assert!(first.native_effects[..256]
                .iter()
                .all(|effect| effect.before_sha256.is_none()));
            let replay = repository
                .register_historical_holds(&expected_digest, &inputs[..256])
                .await?;
            assert_eq!(replay.payload_sha256, first.payload_sha256);
            assert!(replay
                .native_effects
                .iter()
                .all(|effect| effect.before_sha256.as_ref() == Some(&effect.after_sha256)));

            let mixed = repository
                .register_historical_holds(&expected_digest, &inputs[128..])
                .await?;
            assert_eq!(mixed.native_effects.len(), 257);
            assert!(mixed.native_effects[..128]
                .iter()
                .all(|effect| effect.before_sha256.as_ref() == Some(&effect.after_sha256)));
            assert!(mixed.native_effects[128..256]
                .iter()
                .all(|effect| effect.before_sha256.is_none()));
            let guard = &mixed.native_effects[256];
            assert_eq!(guard.table, "source_erasure_guard");
            assert_ne!(guard.before_sha256.as_ref(), Some(&guard.after_sha256));
            let replay = repository
                .register_historical_holds(&expected_digest, &inputs[128..])
                .await?;
            assert_eq!(replay.payload_sha256, mixed.payload_sha256);
            assert!(replay
                .native_effects
                .iter()
                .all(|effect| effect.before_sha256.as_ref() == Some(&effect.after_sha256)));
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .expect("bounded full-page transfer without raising native depth");
}

#[test]
fn catchup_hold_keeps_s0_proof_chain_and_refuses_forged_original() {
    use sha2::Digest as _;
    let before = item();
    let mut after = before.clone();
    after.payload = serde_json::json!({"original":"S1 captured work"});
    let mut original =
        OriginEnvelope::new(&before, WorkProducerOrigin::Unknown, &[]).expect("envelope");
    original.historical_transfer = Some(HistoricalTransferProof {
        version: 1,
        snapshot_sha256: "b".repeat(64),
        original_row_sha256: "a".repeat(64),
        hash_domain: "homecore-python-json-v1".into(),
        source_space_id: "default".into(),
        target_space_id: "historical".into(),
        previous_hold_sha256: None,
    });
    let payload = original.payload().expect("payload");
    let (next, _) = historical_catchup_envelope(
        Some(&before),
        Some(&after),
        Some(&payload),
        ("default", "historical"),
        (&"b".repeat(64), &"c".repeat(64)),
        Some(&"d".repeat(64)),
    )
    .expect("exact S1")
    .expect("new hold");
    let saved: OriginEnvelope = serde_json::from_str(&next).expect("saved");
    saved.verify_identity(&after).expect("identity");
    assert_eq!(saved.origin, WorkProducerOrigin::Unknown);
    let proof = saved.historical_transfer.expect("proof");
    assert_eq!(proof.version, 2);
    assert_eq!(proof.snapshot_sha256, "c".repeat(64));
    assert_eq!(
        proof.previous_hold_sha256,
        Some(hex::encode(sha2::Sha256::digest(payload.as_bytes())))
    );
    assert!(historical_catchup_envelope(
        Some(&after),
        Some(&after),
        Some(&payload),
        ("default", "historical"),
        (&"b".repeat(64), &"c".repeat(64)),
        Some(&"d".repeat(64))
    )
    .is_err());
    assert!(historical_catchup_envelope(
        Some(&before),
        Some(&after),
        Some(&payload),
        ("default", "historical"),
        (&"e".repeat(64), &"c".repeat(64)),
        Some(&"d".repeat(64))
    )
    .is_err());
}

// The public transfer page bound is 256. Exercise that boundary with the real
// native origin/erasure events, including mixed replay and whole-page rollback.
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain native transfer/producer owners through full-page effects/replay and final held-claim plus exact-work readbacks."
)]
async fn historical_full_page_native_guard_count_replay_and_rollback() {
    use crate::transfer_batch::TransferRecord;
    let client = Box::pin(StorageClient::new_memory())
        .await
        .expect("empty target");
    let digest = "d".repeat(64);
    let source_digest = digest.clone();
    let original: Vec<_> = (0..256).map(|_| item()).collect();
    let expected = original.clone();
    client.with_transfer_barrier(&digest, move |exclusive| async move {
        install_schema(&exclusive).await;
        for work in &original {
            raw_create(&exclusive, work.clone()).await;
        }
        let rows = original.iter().map(|work| {
            let mut source = work.clone();
            source.base.space_id = "default".into();
            HistoricalWorkHoldInput {
                original: TransferRecord::WorkItem(source),
                original_row_sha256: "e".repeat(64),
                source_space_id: "default".into(),
                target_space_id: "historical".into(),
            }
        }).collect::<Vec<_>>();
        let producer = WorkProducerRepository::new(exclusive.clone());
        let mut response = exclusive.query_checked("SELECT VALUE type::string($this) FROM work_item").await?;
        let mut work_before: Vec<String> = response.take(0).expect("native original rows");
        work_before.sort();
        assert_eq!(work_before.len(), 256);
        let mut response = exclusive.query_checked("SELECT VALUE type::string($this) FROM source_erasure_guard; SELECT VALUE revision FROM ONLY source_erasure_guard:historical;").await?;
        let guards_before: Vec<String> = response.take(0).expect("original native guards");
        let revision_before: Option<i64> = response.take(1).expect("original guard revision");
        let revision_before = revision_before.expect("work writes created guard");

        // A stale final row must roll back all preceding tentative origins and
        // their generated guard revisions, not leave a partially held page.
        let mut stale = rows.clone();
        if let TransferRecord::WorkItem(last) = &mut stale[255].original {
            last.payload = serde_json::json!({"stale":true});
        }
        assert!(producer.register_historical_holds(&source_digest, &stale).await.is_err());
        let mut response = exclusive.query_checked("SELECT VALUE type::string($this) FROM work_producer_origin; SELECT VALUE type::string($this) FROM source_erasure_guard;").await?;
        let origins: Vec<String> = response.take(0).expect("no partial origins");
        let guards: Vec<String> = response.take(1).expect("no partial guard");
        assert!(origins.is_empty());
        assert_eq!(guards, guards_before, "failed page must preserve original guard bytes");

        // Preexisting exact holds count zero; the remaining 192 count one each.
        producer.register_historical_holds(&source_digest, &rows[..64]).await?;
        let full = producer.register_historical_holds(&source_digest, &rows).await?;
        assert_eq!(full.payload_sha256.len(), 256);
        assert_eq!(full.native_effects.len(), 257);
        assert_eq!(full.native_effects[..64].iter().filter(|e| e.before_sha256.as_ref() == Some(&e.after_sha256)).count(), 64);
        assert!(full.native_effects[64..256].iter().all(|e| e.before_sha256.is_none()));
        let mut response = exclusive.query_checked("SELECT VALUE revision FROM ONLY source_erasure_guard:historical").await?;
        let revision: Option<i64> = response.take(0).expect("guard revision");
        assert_eq!(revision, Some(revision_before + 256));
        let replay = producer.register_historical_holds(&source_digest, &rows).await?;
        assert_eq!(replay.payload_sha256, full.payload_sha256);
        assert_eq!(replay.native_effects.len(), 257);
        assert!(replay.native_effects.iter().all(|e| e.before_sha256.as_ref() == Some(&e.after_sha256)));
        let mut response = exclusive.query_checked("SELECT VALUE type::string($this) FROM work_item").await?;
        let mut work_after: Vec<String> = response.take(0).expect("native retained rows");
        work_after.sort();
        assert_eq!(work_after, work_before);
        exclusive.initialize_journal_identity().await?;
        Ok(())
    }).await.expect("full bounded native hold page");
    let repository = WorkItemRepository::new(client.clone());
    for original in [&expected[0], &expected[255]] {
        assert!(repository
            .claim_with_lease(
                "historical",
                &original.base.id,
                "must-stay-inert",
                Utc::now() + Duration::minutes(1)
            )
            .await
            .expect("held claim")
            .is_none());
        assert_eq!(
            repository
                .get("historical", &original.base.id)
                .await
                .expect("retained work"),
            Some(original.clone())
        );
    }
}
