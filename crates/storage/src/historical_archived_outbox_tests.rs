//! Archived events remain byte-exact history while future native events advance.
use super::*;
use crate::historical_archive_lifecycle::{
    reserve_initial_archive, SealedOperationalArchive, RESERVED_ARCHIVE_CONSUMER,
};
use crate::historical_operational_archive::{ArchiveBinding, ArchiveBuilder, ArchiveRow};
use crate::Repository;
use elephant_types::records::{BaseFields, SystemEvent};
use elephant_types::{RecordId, TableName};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn event(space: &str, seq: i64) -> SystemEvent {
    SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            space.into(),
            "original".into(),
        ),
        seq,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"original_space":"default","unchanged":true}),
    }
}
fn sealed(client: &StorageClient, rows: &[SystemEvent]) -> StorageResult<SealedOperationalArchive> {
    let binding = ArchiveBinding {
        version: 1,
        snapshot_sha256: "a".repeat(64),
        spool_sha256: "b".repeat(64),
        plan_id: "c".repeat(64),
        source_space_id: "default".into(),
        target_space_id: "household".into(),
        journal_identity: client.erasure_journal()?.identity().into(),
        disposition_sha256: ArchiveBinding::disposition_digest(),
        parent_generation_sha256: None,
        original_table_declarations: BTreeMap::from([
            ("system_event".into(), rows.len() as u64),
            ("work_edge".into(), 0),
        ]),
    };
    let pending = reserve_initial_archive(client, &binding)?;
    let mut writer = ArchiveBuilder::create(pending.path(), binding)?;
    let rows = rows
        .iter()
        .map(|row| {
            let lexical = serde_json::to_vec_pretty(row)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            Ok(ArchiveRow {
                table: "system_event".into(),
                id: row.base.id.to_string(),
                lexical_sha256: hex::encode(Sha256::digest(&lexical)),
                lexical,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    writer.append(&rows)?;
    pending.seal(client, writer.seal()?)
}
async fn initialize(client: &StorageClient) -> StorageResult<()> {
    client.initialize_journal_identity().await?;
    client
        .query_checked("CREATE canonical_epoch:household SET epoch='original-epoch';")
        .await?;
    Ok(())
}
async fn count(client: &StorageClient, table: &str) -> StorageResult<usize> {
    #[derive(serde::Deserialize)]
    struct Count {
        n: usize,
    }
    let mut response = client
        .query_bind_checked(
            "SELECT count() AS n FROM type::table($table) GROUP ALL",
            crate::bindings! {"table"=>table},
        )
        .await?;
    let rows: Vec<Count> = response.take(0)?;
    Ok(rows.first().map_or(0, |row| row.n))
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain transfer custody and original event/checkpoint readers through namespace preservation, reserved-consumer refusal and future-delivery readbacks."
)]
async fn managed_archive_event_floors_preserve_namespace_originals_and_future_delivery(
) -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    let digest = "a".repeat(64);
    let original = vec![event("default", 17), event("foreign", 29)];
    let (archive,retained)=client.with_transfer_barrier(&digest,move |exclusive| async move {
        initialize(&exclusive).await?;
        let archive=sealed(&exclusive,&original)?;
        let repo=OutboxCheckpointRepository::new(exclusive.clone());
        for (space,tail) in [("household",17),("foreign",29),("quiet",0)] {
            let registration=repo.preserve_archived_events_with_effects(space,&archive).await?;
            assert_eq!(registration.native_effects.len(),3);
            assert!(registration.baselines.iter().all(|proof|proof.through_seq==tail && proof.disposition=="historical_preserved_not_delivered"));
            for effect in registration.native_effects {
                assert!(effect.before_sha256.is_none());
                let mut result=exclusive.query_bind_checked("SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY type::thing($id)",crate::bindings!{"id"=>effect.id}).await?;
                let actual:Option<String>=result.take(0)?;
                assert_eq!(actual.as_deref(),Some(effect.after_sha256.as_str()));
            }
            assert!(repo.preserve_archived_events_with_effects(space,&archive).await.is_err());
        }
        assert_eq!(count(&exclusive,"system_event").await?,0);
        assert_eq!(archive.seal().event_tails["default"].max_seq,17);
        assert_eq!(archive.seal().event_tails["foreign"].max_seq,29);
        archive.commit(&exclusive,"original-epoch").await?;
        for reserved in [RESERVED_ARCHIVE_CONSUMER,crate::event_sequence::RESERVED_CONSUMER] {
            assert!(repo.load("household",reserved).await.is_err());
            assert!(repo.advance("household",reserved,0,1).await.is_err());
        }
        exclusive.verify_journal_identity().await?;
        Ok((archive,exclusive))
    }).await?;
    assert!(OutboxCheckpointRepository::new(retained.clone())
        .preserve_archived_events_with_effects("household", &archive)
        .await
        .is_err());
    drop(retained);
    let repo = OutboxCheckpointRepository::new(client.clone());
    let events = crate::SystemEventRepository::new(client.clone());
    for (space, tail) in [("household", 17), ("foreign", 29), ("quiet", 0)] {
        for consumer in HISTORICAL_CONSUMERS {
            assert_eq!(repo.load(space, consumer).await?, tail);
            assert!(events.get_after_seq(space, tail, 100).await?.is_empty());
        }
        let next = crate::event_sequence::append_system_event(&client, event(space, 0)).await?;
        assert_eq!(next.seq, tail + 1);
        for consumer in HISTORICAL_CONSUMERS {
            assert_eq!(
                events
                    .get_after_seq(space, repo.load(space, consumer).await?, 100)
                    .await?,
                vec![next.clone()]
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original exclusive client and checkpoint reader through live-row refusal, alias rollback and archive commit."
)]
async fn managed_archive_event_floors_refuse_live_rows_and_checkpoint_alias_atomically(
) -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    client.with_transfer_barrier(&"a".repeat(64),move |exclusive| async move {
        initialize(&exclusive).await?;
        let archive=sealed(&exclusive,&[event("default",17)])?;
        let repo=OutboxCheckpointRepository::new(exclusive.clone());
        let live=event("household",3);
        exclusive.query_bind_checked("CREATE $id CONTENT $row",crate::bindings!{
            "id"=>crate::surreal::thing_from_record_id(&live.base.id),"row"=>crate::SystemEventRepository::system_event_to_value(&live)?
        }).await?;
        assert!(repo.preserve_archived_events_with_effects("household",&archive).await.is_err());
        assert_eq!(count(&exclusive,"outbox_checkpoint").await?,0);
        assert_eq!(crate::SystemEventRepository::new(exclusive.clone()).get("household",&live.base.id).await?,Some(live.clone()));
        exclusive.query_bind_checked("DELETE $id",crate::bindings!{"id"=>crate::surreal::thing_from_record_id(&live.base.id)}).await?;
        exclusive.query_checked("CREATE outbox_checkpoint:alias SET space_id='household',consumer='knowledge_views',seq=0;").await?;
        assert!(repo.preserve_archived_events_with_effects("household",&archive).await.is_err());
        assert_eq!(count(&exclusive,"outbox_checkpoint").await?,1);
        assert!(repo.load("household",crate::event_sequence::RESERVED_CONSUMER).await.is_err());
        exclusive.query_checked("DELETE outbox_checkpoint:alias").await?;
        let result=repo.preserve_archived_events_with_effects("household",&archive).await?;
        assert_eq!(result.native_effects.len(),3);
        archive.commit(&exclusive,"original-epoch").await?;
        Ok(())
    }).await?;
    Ok(())
}
