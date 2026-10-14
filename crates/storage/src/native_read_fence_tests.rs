//! Actual typed-key bucket writes and writer event parity; no fabricated payload.
use super::*;
use crate::StorageConfig;

async fn client() -> StorageClient {
    let client = StorageClient::new(StorageConfig::default()).await.unwrap();
    install_native_reference_guards(&client, &[]).await.unwrap();
    client
}
async fn bucket(client: &StorageClient, id: &Thing) -> Value {
    let mut response = client.query_bind_checked(
        "SELECT * FROM ONLY type::thing('native_read_fence',string::concat(meta::tb($record),':',string::slice(crypto::sha256(type::string($record)),0,3)));",
        crate::bindings! {"record" => id.clone()},
    ).await.unwrap();
    let raw: surrealdb::Value = response.take(0).unwrap();
    raw.into_inner()
}
#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
fn nonce(row: &Value) -> String {
    let Value::Object(fields) = row else {
        panic!("native bucket required");
    };
    assert_eq!(
        fields.len(),
        3,
        "only id, version and nonce may be retained"
    );
    assert_eq!(fields.get("version"), Some(&Value::from(1_i64)));
    let Some(Value::Strand(nonce)) = fields.get("nonce") else {
        panic!("private nonce required");
    };
    uuid::Uuid::parse_str(&nonce.0).unwrap();
    nonce.0.clone()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn same_typed_physical_identity_touches_same_bucket_across_spaces() {
    let client = client().await;
    for table in ["doc", "source_head", "source_publication_manifest"] {
        let id = Thing::from((table, surrealdb::sql::Id::Uuid(uuid::Uuid::new_v4().into())));
        assert_eq!(bucket(&client, &id).await, Value::None);
        // A reader's original absent observation receives an actual private
        // write without inserting a placeholder in the observed domain table.
        client.query_bind_checked("BEGIN TRANSACTION; IF (SELECT * FROM ONLY $record) != NONE { THROW 'fixture_expected_absent'; }; fn::recall_physical_read_fence($record); COMMIT TRANSACTION;",crate::bindings!{"record"=>id.clone()}).await.unwrap();
        let absent_guard = nonce(&bucket(&client, &id).await);
        let mut response = client
            .query_bind_checked(
                "SELECT * FROM ONLY $record;",
                crate::bindings! {"record"=>id.clone()},
            )
            .await
            .unwrap();
        let row: surrealdb::Value = response.take(0).unwrap();
        assert_eq!(row.into_inner(), Value::None);
        // Actual writers in a different space touch the SAME physical bucket.
        client
            .query_bind_checked(
                "CREATE $record SET space_id='foreign-space',payload='original';",
                crate::bindings! {"record"=>id.clone()},
            )
            .await
            .unwrap();
        let created = nonce(&bucket(&client, &id).await);
        assert_ne!(created, absent_guard);
        client
            .query_bind_checked(
                "UPDATE $record SET space_id='another-space',payload='updated';",
                crate::bindings! {"record"=>id.clone()},
            )
            .await
            .unwrap();
        let updated = nonce(&bucket(&client, &id).await);
        assert_ne!(updated, created);
        client
            .query_bind_checked("DELETE $record;", crate::bindings! {"record"=>id.clone()})
            .await
            .unwrap();
        let deleted = nonce(&bucket(&client, &id).await);
        assert_ne!(deleted, updated);
    }
    assert!(client
        .query_checked("fn::recall_physical_read_fence(entity:unsupported);")
        .await
        .is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn dynamic_restore_refresh_retains_head_metadata_rule_and_payload_free_bucket() {
    let client = client().await;
    let head = Thing::from(("source_head", "original-control"));
    // Simulate historical saved event definitions, then run the actual refresh
    // path. The private bucket must never receive a space/reference event.
    client.query_checked("DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE native_read_fence WHEN true THEN { THROW 'old_unusable_bucket_event'; }; DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_head WHEN true THEN { THROW 'old_unusable_head_event'; };").await.unwrap();
    install_native_reference_guards(
        &client,
        &[
            "native_read_fence".into(),
            "source_head".into(),
            "doc".into(),
            "source_publication_manifest".into(),
        ],
    )
    .await
    .unwrap();
    client
        .query_bind_checked(
            "CREATE $record SET space_id='metadata-space',payload='retained metadata';",
            crate::bindings! {"record"=>head.clone()},
        )
        .await
        .unwrap();
    nonce(&bucket(&client, &head).await);
    let mut response = client
        .query_checked("SELECT VALUE revision FROM ONLY source_erasure_guard:`metadata-space`;")
        .await
        .unwrap();
    let revision: Option<u64> = response.take(0).unwrap();
    assert!(revision.is_some_and(|revision| revision > 0));
    let sql = native_reference_event("source_head");
    assert!(sql.contains("fn::recall_reference_guard($after,false)"));
    assert!(!sql.contains("$after,true"));
    assert!(reference_guard_exempt("native_read_fence"));
    assert!(crate::canonical_extension::validate_extension_table("native_read_fence").is_err());
}

async fn physical_fence_rows(client: &StorageClient) -> Value {
    let mut response = client
        .query_checked("SELECT * FROM native_read_fence ORDER BY id;")
        .await
        .unwrap();
    let rows: surrealdb::Value = response.take(0).unwrap();
    rows.into_inner()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn ordinary_bucket_writes_refuse_hidden_payload_and_invalid_identity() {
    let client = client().await;
    let record = Thing::from(("doc", "shape-original"));
    client
        .query_bind_checked(
            "fn::recall_physical_read_fence($record);",
            crate::bindings! {"record"=>record.clone()},
        )
        .await
        .unwrap();
    let original = physical_fence_rows(&client).await;
    for invalid in [
        "UPDATE native_read_fence SET hidden_source_payload='must never persist';",
        "UPDATE native_read_fence SET version='1';",
        "UPDATE native_read_fence SET nonce='invalid-nonce';",
        "CREATE native_read_fence:`doc:ABC` SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`source:000` SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:42 SET version=1,nonce=<string>rand::uuid();",
    ] {
        assert!(client.query_checked(invalid).await.is_err(), "{invalid}");
        assert_eq!(physical_fence_rows(&client).await, original);
    }
    // The complete permitted identity space is bounded by three names and
    // exactly three lowercase hexadecimal digits, including both endpoints.
    for table in ["doc", "source_head", "source_publication_manifest"] {
        for suffix in ["000", "fff"] {
            client
                .query_bind_checked(
                    "UPSERT $bucket SET version=1,nonce=<string>rand::uuid();",
                    crate::bindings! {"bucket"=>Thing::from(("native_read_fence".to_string(),format!("{table}:{suffix}")))},
                )
                .await
                .unwrap();
        }
    }
    validate_native_read_fence_rows(&client).await.unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn restore_refuses_malformed_bucket_without_cleaning_original_rows() {
    let client = client().await;
    // Reproduce historical native inventory before current event installation.
    client
        .query_checked("REMOVE EVENT physical_read_fence_shape ON TABLE native_read_fence;")
        .await
        .unwrap();
    for invalid in [
        "CREATE native_read_fence:`doc:000` SET version=1,nonce=<string>rand::uuid(),hidden_source_payload='retained canary';",
        "CREATE native_read_fence:`source:000` SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`doc:ABC` SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`doc:0000` SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:42 SET version=1,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`doc:000` SET version='1',nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`doc:000` SET version=2,nonce=<string>rand::uuid();",
        "CREATE native_read_fence:`doc:000` SET version=1,nonce='invalid';",
        "CREATE native_read_fence:`doc:000` SET version=1,nonce=rand::uuid();",
        "CREATE native_read_fence:`doc:000` SET version=1,nonce='AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA';",
        "CREATE native_read_fence:`doc:000` SET version=1,nonce='00000000000040008000000000000000';",
    ] {
        client.query_checked(invalid).await.unwrap();
        let original = physical_fence_rows(&client).await;
        assert!(install_native_reference_guards(&client, &["native_read_fence".into()])
            .await
            .is_err(), "{invalid}");
        assert_eq!(physical_fence_rows(&client).await, original);
        // Explicitly remove ONLY this test's injected row, preserving the real
        // validator's refusal and proving that it did not silently scrub data.
        client.query_checked("DELETE native_read_fence;").await.unwrap();
    }
    install_native_reference_guards(&client, &["native_read_fence".into()])
        .await
        .unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_guard_refuses_preexisting_payload_before_changing_nonce() {
    let client = client().await;
    let record = Thing::from(("doc", "guard-original"));
    client
        .query_bind_checked(
            "fn::recall_physical_read_fence($record);",
            crate::bindings! {"record"=>record.clone()},
        )
        .await
        .unwrap();
    client.query_checked("REMOVE EVENT physical_read_fence_shape ON TABLE native_read_fence; UPDATE native_read_fence SET hidden_source_payload='original must remain on refusal';").await.unwrap();
    let original = physical_fence_rows(&client).await;
    assert!(client
        .query_bind_checked(
            "fn::recall_physical_read_fence($record);",
            crate::bindings! {"record"=>record},
        )
        .await
        .is_err());
    assert_eq!(physical_fence_rows(&client).await, original);
}

async fn guard_revision(client: &StorageClient, space: &str) -> Option<u64> {
    let mut response = client
        .query_bind_checked(
            "SELECT VALUE revision FROM ONLY $guard;",
            crate::bindings! {"guard"=>Thing::from(("source_erasure_guard",space))},
        )
        .await
        .unwrap();
    response.take(0).unwrap()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn present_mapping_and_registration_writers_touch_original_space_revision() {
    let client = client().await;
    client
        .query_checked(include_str!(
            "../../migrations/sql/035_source_publication_manifest.surql"
        ))
        .await
        .unwrap();
    for table in ["source_document_mapping", "source_bundle_registration"] {
        let id = Thing::from((table, "original"));
        let first = format!("{table}-original");
        let second = format!("{table}-moved");
        assert_eq!(guard_revision(&client, &first).await, None);
        client
            .query_bind_checked(
                "CREATE $record SET space_id=$space,source_id='source:fixture',payload='{}',state={};",
                crate::bindings! {"record"=>id.clone(),"space"=>first.clone()},
            )
            .await
            .unwrap();
        let original = guard_revision(&client, &first).await.unwrap();
        assert!(original > 0);
        client
            .query_bind_checked(
                "UPDATE $record SET space_id=$space;",
                crate::bindings! {"record"=>id.clone(),"space"=>second.clone()},
            )
            .await
            .unwrap();
        assert!(guard_revision(&client, &first).await.unwrap() > original);
        let moved = guard_revision(&client, &second).await.unwrap();
        assert!(moved > 0);
        client
            .query_bind_checked("DELETE $record;", crate::bindings! {"record"=>id})
            .await
            .unwrap();
        assert!(guard_revision(&client, &second).await.unwrap() > moved);
    }
}
