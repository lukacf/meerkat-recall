//! Imported event history stays undelivered; future live events remain deliverable.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::Repository;
use elephant_types::records::{BaseFields, SystemEvent};
use elephant_types::{RecordId, TableName};

fn event(seq: i64) -> SystemEvent {
    SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "historical".into(),
            "original".into(),
        ),
        seq,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"original":true}),
    }
}
async fn raw_event(client: &StorageClient, row: SystemEvent) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.create(row)?;
                Ok(())
            })
            .await
    } else {
        client.query_bind_checked("CREATE $id CONTENT $row",crate::bindings!{"id"=>crate::surreal::thing_from_record_id(&row.base.id),"row"=>crate::SystemEventRepository::system_event_to_value(&row)?}).await.map(|_|())
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original client and restart checkpoint/event readers through historical preservation and every future-delivery advance/readback assertion."
)]
async fn baseline_contract(embedded: bool) {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new_memory().await
    }
    .unwrap();
    let digest = "a".repeat(64);
    let bound = digest.clone();
    let original = event(17);
    let check = original.clone();
    assert!(OutboxCheckpointRepository::new(client.clone())
        .preserve_historical_events("historical", &digest)
        .await
        .is_err());
    let retained =
        client
            .with_transfer_barrier(&digest, move |exclusive| async move {
                raw_event(&exclusive, original.clone()).await?;
                let repo = OutboxCheckpointRepository::new(exclusive.clone());
                assert!(repo
                    .preserve_historical_events("historical", &"b".repeat(64))
                    .await
                    .is_err());
                let registration = repo
                    .preserve_historical_events_with_effects("historical", &bound)
                    .await?;
                assert_eq!(registration.native_effects.len(), if embedded { 0 } else { 3 });
                assert!(registration.native_effects.iter().all(|e|e.before_sha256.is_none() && e.table=="outbox_checkpoint"));
                for effect in &registration.native_effects {
                    let mut response=exclusive.query_bind_checked("SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY type::thing($id)",crate::bindings!{"id"=>effect.id.clone()}).await?;
                    let hash:Option<String> = response.take(0).map_err(|e|crate::StorageError::DataIntegrityError(e.to_string()))?;
                    assert_eq!(hash.as_ref(),Some(&effect.after_sha256));
                }
                let proof = registration.baselines;
                assert_eq!(proof.len(), 2);
                assert!(proof.iter().all(|p| p.through_seq == 17
                    && p.disposition == "historical_preserved_not_delivered"));
                assert!(repo
                    .preserve_historical_events("historical", &bound)
                    .await
                    .is_err());
                assert_eq!(
                    crate::SystemEventRepository::new(exclusive.clone())
                        .get("historical", &original.base.id)
                        .await?,
                    Some(original)
                );
                exclusive.initialize_journal_identity().await?;
                Ok(exclusive)
            })
            .await
            .unwrap();
    assert!(OutboxCheckpointRepository::new(retained)
        .preserve_historical_events("historical", &digest)
        .await
        .is_err());
    let restarted = OutboxCheckpointRepository::new(client.clone());
    let events = crate::SystemEventRepository::new(client.clone());
    for consumer in HISTORICAL_CONSUMERS {
        let cursor = restarted.load("historical", consumer).await.unwrap();
        assert_eq!(cursor, 17);
        assert!(events
            .get_after_seq("historical", cursor, 100)
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(
        restarted
            .load("historical", "unrelated-consumer")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        events.get("historical", &check.base.id).await.unwrap(),
        Some(check)
    );
    let next = crate::event_sequence::append_system_event(&client, event(0))
        .await
        .unwrap();
    assert_eq!(next.seq, 18);
    for consumer in HISTORICAL_CONSUMERS {
        let delivered = events
            .get_after_seq(
                "historical",
                restarted.load("historical", consumer).await.unwrap(),
                100,
            )
            .await
            .unwrap();
        assert_eq!(delivered, vec![next.clone()]);
        restarted
            .advance("historical", consumer, 17, 18)
            .await
            .unwrap();
        assert_eq!(restarted.load("historical", consumer).await.unwrap(), 18);
        let row = restarted
            .read_row("historical", consumer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.historical_baseline.unwrap().through_seq, 17);
    }
}
#[tokio::test]
async fn historical_event_baseline_embedded() {
    baseline_contract(true).await;
}
#[tokio::test]
async fn historical_event_baseline_native() {
    baseline_contract(false).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original native client and checkpoint reader through same-sequence replacement, CAS refusal and final cursor check."
)]
async fn native_baseline_compare_and_swap_refuses_same_sequence_replacement() {
    let client = StorageClient::new_memory().await.unwrap();
    let digest = "a".repeat(64);
    let bound = digest.clone();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            raw_event(&exclusive, event(17)).await?;
            OutboxCheckpointRepository::new(exclusive.clone())
                .preserve_historical_events("historical", &bound)
                .await?;
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .unwrap();
    let repo = OutboxCheckpointRepository::new(client.clone());
    let consumer = HISTORICAL_CONSUMERS[0];
    let proof = repo
        .read_row("historical", consumer)
        .await
        .unwrap()
        .unwrap()
        .historical_baseline
        .unwrap();
    let baseline = crate::surreal::json_to_surreal(serde_json::to_value(&proof).unwrap()).unwrap();
    client.query_bind_checked("UPDATE $id SET historical_baseline.snapshot_sha256=$changed",crate::bindings!{"id"=>surrealdb::sql::Thing::from((TABLE.to_owned(),key("historical",consumer))),"changed"=>"b".repeat(64)}).await.unwrap();
    assert!(repo
        .advance_native("historical", consumer, 17, 18, baseline)
        .await
        .is_err());
    assert_eq!(repo.load("historical", consumer).await.unwrap(), 17);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original client move into its checkpoint repository and retain that owner through the wrong-consumer refusal."
)]
async fn embedded_advance_refuses_baseline_attached_to_other_consumer() {
    let client = StorageClient::new_embedded().await.unwrap();
    let journal = client.erasure_journal().unwrap().identity().to_owned();
    let proof = HistoricalEventBaseline {
        version: 1,
        snapshot_sha256: "a".repeat(64),
        journal_id: journal,
        through_seq: 3,
        disposition: "historical_preserved_not_delivered".into(),
    };
    client.embedded_store().unwrap().mutate(|tx|tx.upsert_extension_row(TABLE,serde_json::json!({"id":format!("{TABLE}:{}",key("historical","other")),"space_id":"historical","consumer":"other","seq":3,"historical_baseline":proof})).map_err(crate::StorageError::from)).await.unwrap();
    let repo = OutboxCheckpointRepository::new(client);
    assert!(repo.advance("historical", "other", 3, 4).await.is_err());
}
