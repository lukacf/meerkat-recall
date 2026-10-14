//! Original transactions exercise reservation rollback and corrupted authority.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::{OutboxCheckpointRepository, StorageConfig, SystemEventRepository};
use elephant_types::records::BaseFields;
use elephant_types::{RecordId, TableName};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .expect("storage");
    if !embedded {
        client.query_bind_checked(
            "DEFINE TABLE system_event SCHEMALESS; DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id,seq UNIQUE; DEFINE TABLE outbox_checkpoint SCHEMALESS; DEFINE INDEX outbox_checkpoint_consumer ON TABLE outbox_checkpoint FIELDS space_id,consumer UNIQUE;",
            crate::bindings! {},
        ).await.expect("actual schema");
    }
    client
}

fn event() -> SystemEvent {
    SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "floor-test".into(),
            "floor-control".into(),
        ),
        seq: 999,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"actual":"event"}),
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn producer_floor_is_not_a_delivered_consumer_checkpoint() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let actual = append_system_event(&client, event())
            .await
            .expect("real event");
        assert_eq!(actual.seq, 1);
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        assert_eq!(checkpoints.load("floor-test", "wiki").await.unwrap(), 0);
        assert!(checkpoints
            .load("floor-test", RESERVED_CONSUMER)
            .await
            .is_err());
        assert!(checkpoints
            .advance("floor-test", RESERVED_CONSUMER, 1, 20)
            .await
            .is_err());
        assert!(checkpoints
            .advance("another-space", RESERVED_CONSUMER, 0, 20)
            .await
            .is_err());
        checkpoints
            .advance("floor-test", "wiki", 0, actual.seq)
            .await
            .unwrap();
        let next = append_system_event(&client, event())
            .await
            .expect("next actual event");
        assert_eq!(next.seq, 2);
        assert_eq!(checkpoints.load("floor-test", "wiki").await.unwrap(), 1);
    }
}

#[tokio::test]
#[expect(
    clippy::format_push_string,
    reason = "Preserve the exact assembled rollback query and the original infallible String append in this fixture."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn event_creation_fault_rolls_back_reserved_floor_on_both_backends() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let first = append_system_event(&client, event())
            .await
            .expect("original event");
        let failed = event();
        let result: StorageResult<()> = if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| {
                    let mut row = failed.clone();
                    row.seq = reserve_tx(tx, "floor-test", 0, 1)? + 1;
                    tx.create(row)?;
                    Err(invalid("injected event floor rollback"))
                })
                .await
        } else {
            let mut sql = String::from("BEGIN TRANSACTION;\n");
            let mut bindings = crate::bindings! {
                "failed_id" => crate::surreal::thing_from_record_id(&failed.base.id),
                "failed_record" => SystemEventRepository::system_event_to_value(&failed).unwrap(),
            };
            let base =
                append_reserve_sql(&mut sql, &mut bindings, "floor-test", "fault_floor", 0, 1)
                    .unwrap();
            sql.push_str(&format!("CREATE $failed_id CONTENT object::from_entries(array::concat(object::entries($failed_record), [['seq', {base} + 1]])); THROW 'injected event floor rollback'; COMMIT TRANSACTION;"));
            client.query_bind_checked(&sql, bindings).await.map(|_| ())
        };
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected event floor rollback"));
        let events = SystemEventRepository::new(client.clone())
            .get_after_seq("floor-test", 0, 10)
            .await
            .unwrap();
        assert_eq!(events, vec![first]);
        let next = append_system_event(&client, event())
            .await
            .expect("original floor after rollback");
        assert_eq!(next.seq, 2);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn corrupt_reserved_floor_refuses_new_events_instead_of_reconstructing_authority() {
    for embedded in [true, false] {
        for (field, value) in [
            ("seq", serde_json::json!(-1)),
            ("seq", serde_json::json!(0.5)),
            ("seq", serde_json::json!("1")),
            ("space_id", serde_json::json!("different-space")),
            ("consumer", serde_json::json!("wiki")),
            ("payload", serde_json::json!({"forbidden":"source content"})),
        ] {
            let client = client(embedded).await;
            let original = append_system_event(&client, event())
                .await
                .expect("original actual event");
            let id = format!("{TABLE}:{}", key("floor-test"));
            if let Some(store) = client.embedded_store() {
                store
                    .mutate(|tx| {
                        let mut row = tx
                            .scan_extension_rows(TABLE)?
                            .into_iter()
                            .find(|(physical, _)| physical == &id)
                            .expect("original floor")
                            .1;
                        row[field] = value.clone();
                        tx.upsert_extension_row(TABLE, row)?;
                        Ok(())
                    })
                    .await
                    .unwrap();
            } else {
                client.query_bind_checked(
                    &format!("UPDATE $id SET {field} = $value"),
                    crate::bindings! {
                        "id" => surrealdb::sql::Thing::from((TABLE, key("floor-test").as_str())),
                        "value" => crate::surreal::json_to_surreal(value.clone()).unwrap(),
                    },
                ).await.unwrap();
            }
            assert!(
                append_system_event(&client, event()).await.is_err(),
                "{field}={value}"
            );
            assert_eq!(
                SystemEventRepository::new(client.clone())
                    .get_after_seq("floor-test", 0, 10)
                    .await
                    .unwrap(),
                vec![original]
            );
        }
    }
}
