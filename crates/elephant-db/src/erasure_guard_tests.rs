//! Kernel admission failures remain atomic even when a mutation error is caught.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::{ElephantDb, ElephantDbError, ReferenceTokenNormalizer, Result};
use elephant_types::records::{entity::EntityType, BaseFields, Entity};
use elephant_types::{RecordId, TableName};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Guard {
    owner: Arc<Mutex<Option<String>>>,
    captures: Arc<AtomicUsize>,
}
impl ReferenceTokenNormalizer for Guard {
    fn journal_identity(&self) -> &'static str {
        "fixture-journal"
    }
    fn key_fingerprint(&self) -> &'static str {
        "fixture-key-commitment"
    }
    fn erasure_reservation(&self) -> Option<String> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.owner.lock().expect("owner").clone()
    }
    fn reference_tokens(
        &self,
        _space: &str,
        _table: &str,
        _row: &serde_json::Value,
    ) -> Result<BTreeSet<String>> {
        Ok(BTreeSet::new())
    }
}
fn entity(space: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space.into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "original".into(),
    )
}
fn tables(db: &ElephantDb) -> serde_json::Value {
    serde_json::from_slice::<serde_json::Value>(&db.export_snapshot_bytes().expect("snapshot"))
        .expect("JSON")["tables"]
        .clone()
}
fn set_guard(db: &ElephantDb, space: &str, exhausted: bool, owner: Option<&str>) {
    db.tx::<_,ElephantDbError,_>(|tx| {
        let mut row=serde_json::json!({"id":{"tb":"source_erasure_guard","id":space},"space_id":space,"journal_id":"fixture-journal","key_fingerprint":"fixture-key-commitment","version":1,"membership_count":0,"revision":if exhausted {i64::MAX}else{1}});
        if let Some(owner)=owner { row["erasure_reservation"]=owner.into(); }
        tx.upsert_extension_row("source_erasure_guard",row)?;
        Ok(())
    }).expect("guard fixture");
}

#[test]
fn caught_guard_error_cannot_commit_any_payload_epoch_or_index_change() {
    for exhausted in [false, true] {
        for operation in [
            "create",
            "replace",
            "move",
            "move_out",
            "extension_move_out",
            "extension_upsert_out",
            "delete",
            "raw_delete",
            "extension_create",
            "extension_replace",
            "extension_delete",
        ] {
            let db = ElephantDb::new();
            db.install_reference_token_normalizer(Arc::new(Guard {
                owner: Arc::new(Mutex::new(None)),
                captures: Arc::new(AtomicUsize::new(0)),
            }))
            .expect("install");
            let original = db
                .create(entity(if operation == "move" {
                    "original-space"
                } else {
                    "blocked-space"
                }))
                .expect("seed");
            db.tx::<_, ElephantDbError, _>(|tx| {
                tx.upsert_extension_row(
                    "guard_fixture",
                    serde_json::json!({"id":"row","space_id":"blocked-space","value":"original"}),
                )?;
                Ok(())
            })
            .expect("extension seed");
            set_guard(
                &db,
                "blocked-space",
                exhausted,
                (!exhausted).then_some("reserved-intent"),
            );
            let before = tables(&db);
            let result=db.tx::<_,ElephantDbError,_>(|tx| {
                // A different space's earlier successful effect must also roll back.
                tx.create(entity("independent-space"))?;
                let attempted = match operation {
                    "create"=>tx.create(entity("blocked-space")).map(|_|()),
                    "replace"|"move"|"move_out"=>{
                        let mut updated=original.clone();
                        updated.base.space_id=if operation=="move_out" {"independent-space"}else{"blocked-space"}.into();updated.display_name="changed".into();
                        tx.replace(updated).map(|_|())
                    },
                    "delete"=>tx.hard_delete::<Entity>("blocked-space",&original.base.id),
                    "raw_delete"=>tx.hard_delete_raw_row(TableName::Entity,"blocked-space",&original.base.id),
                    "extension_create"=>tx.upsert_extension_row("guard_fixture",serde_json::json!({"id":"new","space_id":"blocked-space"})).map(|_|()),
                    "extension_move_out"=>tx.replace_extension_row("guard_fixture","guard_fixture:row",serde_json::json!({"id":"row","space_id":"independent-space","value":"changed"})),
                    "extension_upsert_out"=>tx.upsert_extension_row("guard_fixture",serde_json::json!({"id":"row","space_id":"independent-space","value":"changed"})).map(|_|()),
                    "extension_replace"=>tx.replace_extension_row("guard_fixture","guard_fixture:row",serde_json::json!({"id":"row","space_id":"blocked-space","value":"changed"})),
                    _=>tx.hard_delete_extension_row("guard_fixture","guard_fixture:row","blocked-space"),
                };
                assert_guard_refusal(&attempted, exhausted, operation);
                Ok(()) // The caller deliberately catches the failed mutation.
            });
            assert_guard_refusal(&result, exhausted, operation);
            assert_eq!(
                tables(&db),
                before,
                "partial mutation: {operation}, exhausted={exhausted}"
            );
            assert_eq!(
                db.get::<Entity>(&original.base.space_id, &original.base.id, false)
                    .expect("typed index read"),
                Some(original)
            );
        }
    }
}

#[test]
fn original_journal_permit_is_captured_once_per_transaction() {
    let owner = Arc::new(Mutex::new(Some("original-intent".into())));
    let captures = Arc::new(AtomicUsize::new(0));
    let db = ElephantDb::new();
    db.install_reference_token_normalizer(Arc::new(Guard {
        owner: owner.clone(),
        captures: captures.clone(),
    }))
    .expect("install");
    set_guard(&db, "reserved-space", false, Some("original-intent"));
    let before = captures.load(Ordering::SeqCst);
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.create(entity("reserved-space"))?;
        *owner.lock().expect("owner") = Some("different-intent".into());
        tx.create(entity("reserved-space"))?;
        Ok(())
    })
    .expect("original admitted permit remains sealed");
    assert_eq!(captures.load(Ordering::SeqCst), before + 1);
    assert!(db.create(entity("reserved-space")).is_err());
    assert_eq!(db.scan::<Entity>().expect("entities").len(), 2);
}

fn assert_guard_refusal(result: &Result<()>, exhausted: bool, operation: &str) {
    assert!(
        (!exhausted && matches!(result, Err(ElephantDbError::ErasureInProgress)))
            || (exhausted
                && matches!(result, Err(ElephantDbError::DataIntegrity(message))
                    if message == "private erasure revision invalid or exhausted")),
        "unexpected refusal for {operation}, exhausted={exhausted}: {result:?}"
    );
}

#[test]
fn an_erasure_event_name_is_not_a_reference_bypass_capability() {
    use elephant_types::records::SystemEvent;
    let db = ElephantDb::new();
    let erased = RecordId::new_v4(TableName::Source);
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row("source_tombstone_ref", serde_json::json!({"id":"old", "space_id":"event-space", "token":erased.id().to_string()}))?;
        Ok(())
    }).expect("legacy erasure fixture");
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "event-space".into(),
            "ordinary-producer".into(),
        ),
        seq: 1,
        event_type: "source.erased".into(),
        payload: serde_json::json!({"source_id":erased,"content":"forged late payload"}),
    };
    let before = tables(&db);
    assert!(
        matches!(db.create(event),Err(ElephantDbError::Conflict(message)) if message.starts_with("source_erased:"))
    );
    assert_eq!(tables(&db), before);
}
