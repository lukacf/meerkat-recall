//! Actual kernel/native transaction oracles for private membership enforcement.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use surrealdb::sql::Id;

const JOURNAL: &str = "5811e486-2a21-45cf-8f33-12c63397869a";
const KEY: [u8; 32] = [71; 32];

fn erased_identity() -> Thing {
    Thing::from(("private_reference_fixture", "42"))
}

fn membership() -> BTreeSet<String> {
    reference_tokens(JOURNAL, "named-a", &KEY, &Value::Thing(erased_identity())).expect("tokens")
}

#[test]
fn private_tokens_preserve_type_scope_and_complete_reference_boundaries() {
    let token = membership();
    assert_eq!(token.len(), 1);
    for value in [
        Value::Thing(Thing::from(("private_reference_fixture", Id::Number(42)))),
        Value::from(format!(
            "unrelated prose around {} trailing text",
            erased_identity()
        )),
        Value::from(format!("{}-suffix", erased_identity())),
    ] {
        assert!(reference_tokens(JOURNAL, "named-a", &KEY, &value)
            .expect("normalized")
            .is_disjoint(&token));
    }
    let encoded =
        serde_json::to_string(&serde_json::json!({"nested":[erased_identity().to_string()]}))
            .expect("encoded");
    assert_eq!(
        reference_tokens(JOURNAL, "named-a", &KEY, &Value::from(encoded)).expect("encoded JSON"),
        token
    );
    assert_ne!(
        reference_tokens(JOURNAL, "named-b", &KEY, &Value::Thing(erased_identity()))
            .expect("other space"),
        token
    );
    assert_ne!(
        reference_tokens(
            JOURNAL,
            "named-a",
            &[72; 32],
            &Value::Thing(erased_identity())
        )
        .expect("other key"),
        token
    );
}

#[test]
#[expect(
    clippy::items_after_statements,
    reason = "Keep the local fixture helper beside the original scenario phase that uses it."
)]
fn embedded_private_membership_rejects_atomically_and_requires_original_runtime_key() {
    let db = elephant_db::ElephantDb::new();
    install_kernel_reference_guard(&db, JOURNAL.into(), KEY).expect("runtime");
    db.tx::<_, StorageError, _>(|tx| {
        project_reference_membership_tx(
            tx,
            "named-a",
            JOURNAL,
            &reference_key_fingerprint(JOURNAL, &KEY)?,
            &membership(),
        )
    })
    .expect("journal membership projection");
    let failed = db.tx::<_,StorageError,_>(|tx| {
        tx.upsert_extension_row("private_reference_fixture",serde_json::json!({"id":"first","space_id":"named-a","text":"first effect"}))?;
        tx.upsert_extension_row("private_reference_fixture",serde_json::json!({"id":"late","space_id":"named-a","reference":erased_identity().to_string()}))?;
        Ok(())
    });
    assert!(failed.is_err());
    assert!(db
        .scan_extension_rows("private_reference_fixture")
        .expect("physical scan")
        .is_empty());
    db.tx::<_,StorageError,_>(|tx| {
        tx.upsert_extension_row("private_reference_fixture",serde_json::json!({"id":42,"space_id":"named-a","text":"native integer is independent"}))?;
        Ok(())
    }).expect("distinct physical integer");
    let backup = db.export_backup_snapshot_bytes().expect("ordinary backup");
    let backup_json: serde_json::Value = serde_json::from_slice(&backup).expect("backup JSON");
    assert!(backup_json["tables"].get(MEMBERSHIP_TABLE).is_none());
    let backup_db = elephant_db::ElephantDb::from_snapshot_bytes(&backup).expect("backup load");
    install_kernel_reference_guard(&backup_db, JOURNAL.into(), KEY).expect("original key alone");
    assert!(backup_db
        .tx::<_, StorageError, _>(|tx| {
            tx.upsert_extension_row(
                "private_reference_fixture",
                serde_json::json!({"id":"fresh","space_id":"named-a"}),
            )?;
            Ok(())
        })
        .is_err());
    backup_db
        .tx::<_, StorageError, _>(|tx| {
            project_reference_membership_tx(
                tx,
                "named-a",
                JOURNAL,
                &reference_key_fingerprint(JOURNAL, &KEY)?,
                &membership(),
            )
        })
        .expect("independent manifest replay");
    assert!(backup_db.tx::<_,StorageError,_>(|tx| {
        tx.upsert_extension_row("private_reference_fixture",serde_json::json!({"id":"late","space_id":"named-a","reference":erased_identity().to_string()}))?;
        Ok(())
    }).is_err());
    let snapshot = db.export_snapshot_bytes().expect("private recovery image");
    let snapshot_json: serde_json::Value =
        serde_json::from_slice(&snapshot).expect("recovery image JSON");
    fn contains_key_material(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Array(values) => {
                (values.len() == KEY.len()
                    && values
                        .iter()
                        .zip(KEY)
                        .all(|(value, byte)| value.as_u64() == Some(u64::from(byte))))
                    || values.iter().any(contains_key_material)
            }
            serde_json::Value::Object(fields) => {
                fields.contains_key("__recall_reference_key")
                    || fields.values().any(contains_key_material)
            }
            _ => false,
        }
    }
    assert!(!contains_key_material(&snapshot_json));
    let restored = elephant_db::ElephantDb::from_snapshot_bytes(&snapshot).expect("load state");
    assert!(restored
        .tx::<_, StorageError, _>(|tx| {
            tx.upsert_extension_row(
                "private_reference_fixture",
                serde_json::json!({"id":"fresh","space_id":"named-a"}),
            )?;
            Ok(())
        })
        .is_err());
    assert!(install_kernel_reference_guard(&restored, JOURNAL.into(), [72; 32]).is_err());
    install_kernel_reference_guard(&restored, JOURNAL.into(), KEY).expect("original key");
    restored
        .tx::<_, StorageError, _>(|tx| {
            tx.upsert_extension_row(
                "private_reference_fixture",
                serde_json::json!({"id":"fresh","space_id":"named-a"}),
            )?;
            Ok(())
        })
        .expect("independent late write");
}

#[tokio::test]
async fn native_private_event_checks_evaluated_partial_update_and_rolls_back_prior_statement() {
    let db = surrealdb::Surreal::new::<surrealdb::engine::local::Mem>(())
        .await
        .expect("native");
    db.use_ns("reference-test")
        .use_db("reference-test")
        .await
        .expect("namespace");
    db.query(include_str!(
        "../../migrations/sql/039_private_reference_fence.surql"
    ))
    .await
    .expect("schema response")
    .check()
    .expect("schema");
    db.query(native_reference_event("private_reference_fixture"))
        .await
        .expect("extension response")
        .check()
        .expect("extension event");
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let mut bindings = BTreeMap::new();
    append_reference_membership_sql(
        &mut sql,
        &mut bindings,
        "named-a",
        JOURNAL,
        &reference_key_fingerprint(JOURNAL, &KEY).expect("fingerprint"),
        &membership(),
    )
    .expect("membership SQL");
    sql.push_str("COMMIT TRANSACTION;");
    db.query(sql)
        .bind(bindings)
        .await
        .expect("projection response")
        .check()
        .expect("projection");
    let key_value = Value::Bytes(KEY.to_vec().into());
    db.query("CREATE private_reference_fixture:current SET space_id='named-a', reference=NONE;")
        .bind(("__recall_reference_key", key_value.clone()))
        .bind(("__recall_journal_identity", JOURNAL))
        .await
        .expect("initial response")
        .check()
        .expect("initial row");
    let response = db.query("BEGIN TRANSACTION; CREATE private_reference_fixture:rollback SET space_id='named-a'; UPDATE private_reference_fixture:current SET reference=type::thing('private_reference_fixture', <string>(40+2)); COMMIT TRANSACTION;")
        .bind(("__recall_reference_key", key_value.clone())).bind(("__recall_journal_identity", JOURNAL))
        .await.expect("native response");
    assert!(response.check().is_err());
    let mut response = db
        .query("SELECT VALUE id FROM private_reference_fixture:rollback;")
        .await
        .expect("read")
        .check()
        .expect("read response");
    let rows: Vec<Thing> = response.take(0).expect("ids");
    assert!(rows.is_empty());
    // Same numeric text with the native integer identity remains independent.
    db.query("UPDATE private_reference_fixture:current SET reference=type::thing('private_reference_fixture',40+2);")
        .bind(("__recall_reference_key", key_value)).bind(("__recall_journal_identity", JOURNAL))
        .await.expect("integer response").check().expect("integer independent");
    assert!(db
        .query("UPDATE private_reference_fixture:current SET text='missing runtime custody';")
        .await
        .expect("missing key response")
        .check()
        .is_err());
    let owner = "3b876b22-56ac-43dd-ab6a-8e7b1c8ee6d7";
    db.query(
        "UPDATE type::thing('source_erasure_guard','named-a') SET erasure_reservation=$owner;",
    )
    .bind(("owner", owner))
    .await
    .expect("reserve fixture")
    .check()
    .expect("reserved");
    assert!(db
        .query("UPDATE private_reference_fixture:current SET text='ordinary remote writer';")
        .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
        .bind(("__recall_journal_identity", JOURNAL))
        .await
        .expect("reserved response")
        .check()
        .is_err());
    db.query("UPDATE private_reference_fixture:current SET text='original intent maintenance';")
        .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
        .bind(("__recall_journal_identity", JOURNAL))
        .bind(("__recall_erasure_reservation", owner))
        .await
        .expect("owner response")
        .check()
        .expect("owner context inherited by event function");
}

#[test]
fn embedded_inventory_revision_fences_queue_writes_and_rolls_back_with_them() {
    use elephant_types::records::{ArtifactStatus, BaseFields, WorkArtifact};
    use elephant_types::{Link, RecordId, TableName};
    let db = elephant_db::ElephantDb::new();
    install_kernel_reference_guard(&db, JOURNAL.into(), KEY).expect("runtime");
    let observed = ReferenceGuardRevision {
        space: "named-a".into(),
        revision: None,
    };
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "named-a".into(),
            "worker".into(),
        ),
        work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "knowledge_candidates".into(),
        data: serde_json::json!({"text":"late unrelated payload"}),
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    db.create(artifact.clone()).expect("ordinary queue writer");
    assert!(db
        .scan_extension_rows("canonical_epoch")
        .expect("canonical epochs")
        .is_empty());
    assert!(db
        .tx::<_, StorageError, _>(|tx| check_reference_guard_revision_tx(tx, &observed))
        .is_err());
    let revision = db
        .tx::<_, StorageError, _>(|tx| reference_revision_tx(tx, "named-a"))
        .expect("revision");
    assert_eq!(revision, Some(1));
    let rollback = db.tx::<(), StorageError, _>(|tx| {
        let mut changed = artifact.clone();
        changed.data = serde_json::json!({"text":"uncommitted"});
        tx.replace(changed)?;
        Err(integrity("injected later failure"))
    });
    assert!(rollback.is_err());
    assert_eq!(
        db.tx::<_, StorageError, _>(|tx| reference_revision_tx(tx, "named-a"))
            .expect("revision after rollback"),
        revision
    );
    let observed = ReferenceGuardRevision {
        space: "named-a".into(),
        revision,
    };
    db.hard_delete::<WorkArtifact>("named-a", &artifact.base.id)
        .expect("ordinary queue deletion");
    assert!(db
        .tx::<_, StorageError, _>(|tx| check_reference_guard_revision_tx(tx, &observed))
        .is_err());
    db.tx::<_, StorageError, _>(|tx| {
        let (_, mut anchor) = tx
            .scan_extension_rows(GUARD_TABLE)?
            .into_iter()
            .next()
            .expect("anchor");
        anchor["revision"] = serde_json::Value::from(i64::MAX);
        tx.upsert_extension_row(GUARD_TABLE, anchor)?;
        Ok(())
    })
    .expect("exhausted counter fixture");
    assert!(db.create(artifact).is_err());
    assert!(db
        .scan::<WorkArtifact>()
        .expect("no overflow write")
        .is_empty());
}

#[path = "native_read_fence_tests.rs"]
mod native_read_fence_tests;

#[path = "reference_guard_evaluation_tests.rs"]
mod reference_guard_evaluation_tests;

#[path = "native_parent_snapshot_tests.rs"]
mod native_parent_snapshot_tests;
