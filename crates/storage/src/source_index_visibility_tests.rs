//! Real-backend coverage invalidation and nonce/rollback regression source.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::{Repository, SpaceConfigRepository, StorageClient, StorageConfig};
use elephant_types::records::SpaceConfig;
use elephant_types::{RecordId, TableName};
use surrealdb::sql::{Thing, Value};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .expect("backend");
    if !embedded {
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .expect("publication schema");
        client
            .query_checked(include_str!(
                "../../migrations/sql/038_source_index_visibility.surql"
            ))
            .await
            .expect("visibility schema");
        client
            .query_checked(include_str!(
                "../../migrations/sql/048_historical_coverage_visibility.surql"
            ))
            .await
            .expect("completion visibility schema");
    }
    let config:SpaceConfig=serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"visibility-a","created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8,
        "processing":{"generation":1,"index_generation":0,"policy_id":"test","connections":{},"profiles":{},"bindings":{},"active_embedding_profile_id":null,"pending_reindex":null}
    })).expect("space fixture");
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await
        .expect("space");
    client
}

async fn generation(client: &StorageClient) -> u64 {
    SpaceConfigRepository::new(client.clone())
        .get_by_space_id("visibility-a")
        .await
        .expect("config")
        .expect("present")
        .processing
        .expect("processing")
        .index_generation
}

async fn write(
    client: &StorageClient,
    table: &str,
    row: serde_json::Value,
    fail: bool,
) -> crate::StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                tx.upsert_extension_row(table, row.clone())?;
                if fail {
                    return Err(crate::source_publication::integrity(
                        "injected visibility failure",
                    ));
                }
                Ok(())
            })
            .await;
    }
    let mut native = crate::surreal::json_to_surreal(row)?;
    if let Value::Object(object) = &mut native {
        object.remove("id");
    }
    let fault = if fail {
        "THROW 'injected visibility failure';"
    } else {
        ""
    };
    client
        .query_bind_checked(
            &format!("BEGIN TRANSACTION; UPSERT $id CONTENT $row; {fault} COMMIT TRANSACTION;"),
            crate::bindings! {"id"=>Thing::from((table,"control")),"row"=>native},
        )
        .await
        .map(|_| ())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn visibility_controls_invalidate_coverage(embedded: bool) {
    let client = client(embedded).await;
    for table in [
        "source_head",
        "source_effect_support",
        "source_revision_mapping",
        "source_publication_completion",
        "source_publication_manifest",
        "source_publication_manifest_history",
    ] {
        let before = generation(&client).await;
        let mut row = serde_json::json!({"id":format!("{table}:control"),"space_id":"visibility-a","payload":"accepted original state","state":{"published":"r1"}});
        write(&client, table, row.clone(), false)
            .await
            .expect("visibility control create");
        let accepted = generation(&client).await;
        assert!(accepted > before, "{table} controls read eligibility");
        row["fence_nonce"] = serde_json::json!("transaction-only-nonce");
        write(&client, table, row.clone(), false)
            .await
            .expect("nonce guard");
        assert_eq!(
            generation(&client).await,
            accepted,
            "{table} nonce must not invalidate own vector page"
        );
        row["payload"] = serde_json::json!("new visibility");
        row["state"] = serde_json::json!({"published":"r2"});
        assert!(write(&client, table, row.clone(), true).await.is_err());
        assert_eq!(
            generation(&client).await,
            accepted,
            "failed {table} publication must roll back coverage invalidation"
        );
        write(&client, table, row, false)
            .await
            .expect("changed publication");
        let changed = generation(&client).await;
        assert!(
            changed > accepted,
            "staged coverage from r1 cannot activate after r2 publication"
        );
        if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| {
                    tx.hard_delete_extension_row(
                        table,
                        &format!("{table}:control"),
                        "visibility-a",
                    )?;
                    Ok(())
                })
                .await
                .expect("withdraw control");
        } else {
            client
                .query_bind_checked(
                    "DELETE $id",
                    crate::bindings! {"id"=>Thing::from((table,"control"))},
                )
                .await
                .expect("withdraw control");
        }
        assert!(
            generation(&client).await > changed,
            "{table} withdrawal changes eligible coverage"
        );
    }
}

#[tokio::test]
async fn embedded_visibility_changes_fence_reindex_without_nonce_self_invalidation() {
    visibility_controls_invalidate_coverage(true).await;
}
#[tokio::test]
async fn surreal_visibility_changes_fence_reindex_without_nonce_self_invalidation() {
    visibility_controls_invalidate_coverage(false).await;
}
