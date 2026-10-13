#![allow(clippy::expect_used)]
use super::*;

const TABLE: &str = "readonly_control_fixture";
const KEY: &str = "original-physical-key";

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn physical_absence(embedded: bool) {
    let client = if embedded {
        Box::pin(crate::StorageClient::new_embedded()).await
    } else {
        Box::pin(crate::StorageClient::new_memory()).await
    }
    .expect("actual storage");
    if !embedded {
        client
            .query_checked("DEFINE TABLE readonly_control_fixture SCHEMALESS; DEFINE TABLE IF NOT EXISTS source_head SCHEMALESS;")
            .await
            .expect("explicit fixture schema");
    }
    // A source-head absence decision must observe the physical key even if a
    // malformed/foreign row cannot be returned through scoped head decoding.
    let sources = crate::source_publication::SourcePublicationRepository::new(client.clone());
    let source_id = elephant_types::RecordId::new_v4(elephant_types::TableName::Source);
    let head_key = crate::source_publication::head_key("requesting-space", &source_id);
    assert!(!sources
        .head_exists_globally("requesting-space", &source_id)
        .await
        .expect("actual absent head"));
    assert!(sources.head_exists_globally("", &source_id).await.is_err());
    assert!(sources
        .head_exists_globally(
            "requesting-space",
            &elephant_types::RecordId::new_v4(elephant_types::TableName::Doc)
        )
        .await
        .is_err());
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row(
                    "source_head",
                    serde_json::json!({
                        "id":format!("source_head:{head_key}"), "space_id":"foreign-space",
                        "payload":"invalid original control is still physically present",
                    }),
                )?;
                Ok(())
            })
            .await
            .expect("deliberate foreign/malformed physical head fixture");
    } else {
        client.query_bind_checked(
            "CREATE $head SET space_id='foreign-space',payload='invalid original control is still physically present';",
            crate::bindings! {"head"=>crate::source_publication::control_thing("source_head", &head_key)},
        ).await.expect("deliberate foreign/malformed physical head fixture");
    }
    assert!(sources
        .head("requesting-space", &source_id)
        .await
        .expect("foreign head is not scoped data")
        .is_none());
    for _ in 0..2 {
        assert!(sources
            .head_exists_globally("requesting-space", &source_id)
            .await
            .expect("foreign/malformed head cannot become legacy absence"));
    }

    // Reading a genuinely absent physical key must not create it or a table row.
    for _ in 0..2 {
        assert!(!global_exists(&client, TABLE, KEY)
            .await
            .expect("actual absence"));
    }
    let value = serde_json::json!({
        "id":format!("{TABLE}:{KEY}"), "space_id":"foreign-space",
        "payload":"original immutable foreign control",
    });
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row(TABLE, value.clone())?;
                Ok(())
            })
            .await
            .expect("actual foreign-space control writer");
    } else {
        client
            .query_bind_checked(
                "CREATE $control SET space_id=$space,payload=$payload;",
                crate::bindings! {
                    "control"=>crate::source_publication::control_thing(TABLE,KEY),
                    "space"=>"foreign-space", "payload"=>"original immutable foreign control",
                },
            )
            .await
            .expect("actual foreign-space control writer");
    }
    for _ in 0..2 {
        assert!(global_exists(&client, TABLE, KEY)
            .await
            .expect("global physical identity"));
        assert!(!global_exists(&client, TABLE, &format!("{KEY}0"))
            .await
            .expect("distinct adjacent key"));
        assert!(!global_exists(&client, TABLE, "original-physical-ke")
            .await
            .expect("distinct prefix"));
        if let Some(store) = client.embedded_store() {
            assert!(row(&store, TABLE, "requesting-space", KEY)
                .await
                .expect("scoped lookup")
                .is_none());
            assert_eq!(
                row(&store, TABLE, "foreign-space", KEY)
                    .await
                    .expect("actual owner-scoped lookup"),
                Some(value.clone())
            );
            assert!(store
                .extension_row_by_key(TABLE, KEY)
                .await
                .expect("kernel API requires fully qualified key")
                .is_none());
        }
    }
    // Check the physical original after repeated absence/existence observations;
    // no nonce, payload or authority rewrite accompanies the read.
    if let Some(store) = client.embedded_store() {
        assert_eq!(
            store
                .extension_row_by_key(TABLE, &format!("{TABLE}:{KEY}"))
                .await
                .expect("exact original row"),
            Some(value)
        );
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(TABLE, &format!("{TABLE}:{KEY}"), "foreign-space")?;
                Ok(())
            })
            .await
            .expect("actual physical deletion");
    } else {
        #[derive(serde::Deserialize)]
        struct OriginalRow {
            space_id: String,
            payload: String,
        }
        let mut response = client
            .query_bind_checked(
                "SELECT space_id,payload FROM ONLY $control;",
                crate::bindings! {"control"=>crate::source_publication::control_thing(TABLE,KEY)},
            )
            .await
            .expect("exact original projection");
        let current: Option<OriginalRow> = response.take(0).expect("typed original control");
        let current = current.expect("original still present");
        assert_eq!(current.space_id, "foreign-space");
        assert_eq!(current.payload, "original immutable foreign control");
        client
            .query_bind_checked(
                "DELETE $control;",
                crate::bindings! {
                    "control"=>crate::source_publication::control_thing(TABLE,KEY),
                },
            )
            .await
            .expect("actual physical deletion");
    }
    assert!(!global_exists(&client, TABLE, KEY)
        .await
        .expect("deleted key absent"));
}

#[tokio::test]
async fn embedded_global_control_presence_cannot_be_laundered_as_scoped_absence() {
    Box::pin(physical_absence(true)).await;
}

#[tokio::test]
async fn native_global_control_presence_cannot_be_laundered_as_scoped_absence() {
    Box::pin(physical_absence(false)).await;
}
