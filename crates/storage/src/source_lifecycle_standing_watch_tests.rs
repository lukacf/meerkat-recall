//! Actual registered standing descriptors participate in complete physical erase.
use super::*;
use crate::standing_watch_repository::StandingWatchRepository;
use crate::SourceWatchRepository;
use elephant_auth::standing_watch::StandingWatchAdmission;
use elephant_policy::{Clearance, Principal};
use elephant_types::records::{SourceWatch, SourceWatchStatus};
use elephant_types::Link;
use std::collections::{BTreeMap, HashSet};

fn watch(receipt: &OperationReceipt) -> SourceWatch {
    SourceWatch {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SourceWatch),
            receipt.space_id.clone(),
            "explicit-standing-owner".into(),
        ),
        source: Link::new(receipt.source_id.clone()),
        requested_uri: "https://watch.invalid/private-source".into(),
        status: SourceWatchStatus::Enabled,
        interval_seconds: 3600,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: None,
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    }
}

async fn register(client: &StorageClient, watch: &SourceWatch) {
    let principal = Principal {
        space_id: watch.base.space_id.clone(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![Clearance {
            policy_id: "default".into(),
            max_level: 0,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::from(["original-private-standing-purpose".into()]),
    };
    let admission = StandingWatchAdmission::trusted_local(&principal).unwrap();
    let prepared = StandingWatchRepository::new(client.clone())
        .prepare_registration(watch, &admission)
        .await
        .unwrap();
    // Actual storage registration splice, with the private constructor's own
    // snapshots and persistence guard. No handcrafted descriptor or signature.
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                prepared.persist_tx(tx)?;
                tx.create(watch.clone())?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        prepared
            .append_persist_sql(&mut sql, &mut bindings)
            .unwrap();
        bindings.insert(
            "original_watch".into(),
            SourceWatchRepository::watch_to_value(watch).unwrap(),
        );
        sql.push_str("CREATE $standing_watch_watch CONTENT $original_watch; COMMIT TRANSACTION;");
        client.query_bind_checked(&sql, bindings).await.unwrap();
    }
}

fn descriptor_id(watch: &SourceWatch) -> Thing {
    Thing::from((
        "watch_delegation",
        surrealdb::sql::Id::String(watch.base.id.id().to_string()),
    ))
}

// Select the literal physical table directly, independently of erasure's
// inventory selector and public eligibility paths. Preserve complete native
// values, or the complete embedded JSON row, for exact unrelated-B equality.
async fn physical_descriptor(client: &StorageClient, watch: &SourceWatch) -> Option<Value> {
    if let Some(store) = client.embedded_store() {
        let rows = store
            .extension_rows_page("watch_delegation", &watch.base.space_id, None, 101)
            .await
            .unwrap();
        assert!(rows.len() < 101, "actual small fixture must be complete");
        let key = format!("watch_delegation:{}", watch.base.id.id());
        return rows
            .into_iter()
            .find(|(id, _)| id == &key)
            .map(|(_, row)| crate::surreal::json_to_surreal(row).unwrap());
    }
    let mut response = client
        .query_bind_checked(
            "SELECT * FROM ONLY $id;",
            crate::bindings! { "id" => descriptor_id(watch) },
        )
        .await
        .unwrap();
    let row: surrealdb::Value = response.take(0).unwrap();
    let row = row.into_inner();
    (!matches!(row, Value::None | Value::Null)).then_some(row)
}

async fn insert_physical(client: &StorageClient, id: &Thing, row: Value) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row("watch_delegation", row.into_json())?;
                Ok(())
            })
            .await
    } else {
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row;",
                crate::bindings! { "id" => id.clone(), "row" => row },
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[expect(
    clippy::panic,
    reason = "A missing physical descriptor is a hard failure of this conformance oracle."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn original_standing_descriptor_is_physically_erased_and_late_references_refuse() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        if !embedded {
            // Use the shipped new migration, including its own erased-reference
            // event. Installing a test-only replacement event would hide a gap.
            client
                .query_checked(include_str!(
                    "../../migrations/sql/042_watch_delegation.surql"
                ))
                .await
                .unwrap();
        }
        let publication = SourcePublicationRepository::new(client.clone());
        let a = publication
            .accept(request("erase-a", "standing-a"))
            .await
            .unwrap();
        let b = publication
            .accept(request("erase-b", "standing-b"))
            .await
            .unwrap();
        let watch_a = watch(&a);
        let watch_b = watch(&b);
        register(&client, &watch_a).await;
        register(&client, &watch_b).await;
        let original_a = physical_descriptor(&client, &watch_a).await.unwrap();
        let original_b = physical_descriptor(&client, &watch_b).await.unwrap();
        let original_watch_b = SourceWatchRepository::new(client.clone())
            .get("erase-b", &watch_b.base.id)
            .await
            .unwrap()
            .unwrap();
        let original_source_b = SourceRepository::new(client.clone())
            .get("erase-b", &b.source_id)
            .await
            .unwrap()
            .unwrap();
        publication
            .erase_live(mutation(&publication, &a, "erase-standing-a").await)
            .await
            .unwrap();
        assert!(physical_descriptor(&client, &watch_a).await.is_none());
        assert!(SourceWatchRepository::new(client.clone())
            .get("erase-a", &watch_a.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(SourceRepository::new(client.clone())
            .get("erase-a", &a.source_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            physical_descriptor(&client, &watch_b).await,
            Some(original_b.clone())
        );
        assert_eq!(
            SourceWatchRepository::new(client.clone())
                .get("erase-b", &watch_b.base.id)
                .await
                .unwrap(),
            Some(original_watch_b)
        );
        assert_eq!(
            SourceRepository::new(client.clone())
                .get("erase-b", &b.source_id)
                .await
                .unwrap(),
            Some(original_source_b)
        );

        // Both direct original Source/Watch fields and a reference concealed in
        // the serialized private payload remain fenced after physical deletion.
        assert!(
            insert_physical(&client, &descriptor_id(&watch_a), original_a)
                .await
                .is_err()
        );
        let new_id = Thing::from((
            "watch_delegation".to_string(),
            uuid::Uuid::new_v4().to_string(),
        ));
        let Value::Object(mut hidden_reference) = original_b.clone() else {
            panic!("physical descriptor");
        };
        hidden_reference.insert(
            "id".into(),
            if embedded {
                Value::from(format!("watch_delegation:{}", new_id.id))
            } else {
                Value::Thing(new_id.clone())
            },
        );
        hidden_reference.insert("space_id".into(), Value::from("erase-a"));
        hidden_reference.insert(
            "payload".into(),
            Value::from(
                serde_json::to_string(
                    &serde_json::json!({"nested":{"original_erased_source":a.source_id}}),
                )
                .unwrap(),
            ),
        );
        assert!(
            insert_physical(&client, &new_id, Value::Object(hidden_reference))
                .await
                .is_err()
        );
        assert_eq!(
            physical_descriptor(&client, &watch_b).await,
            Some(original_b)
        );
    }
}
