//! Regression source for the intrinsic publication and erasure fences.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::{ArtifactStatus, WorkArtifact};
use elephant_types::{Link, RecordId, TableName};

use crate::{ElephantDb, ElephantDbError};

fn entity(space: &str, name: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space.into(),
            "test".into(),
        ),
        EntityType::new("project"),
        name.into(),
    )
}

fn epoch(db: &ElephantDb, space: &str) -> Option<String> {
    db.scan_extension_rows("canonical_epoch")
        .expect("epochs")
        .into_iter()
        .find(|(_, row)| row["space_id"] == space)
        .map(|(_, row)| row["epoch"].as_str().expect("epoch string").to_owned())
}

#[test]
fn direct_writers_and_transactional_writers_invalidate_absent_match_observation() {
    let db = ElephantDb::new();
    let first = entity("a", "first");
    db.create(first).expect("create direct");
    let observed = epoch(&db, "a");
    db.create(entity("a", "a newly matching identity"))
        .expect("concurrent direct create");
    assert_ne!(epoch(&db, "a"), observed);
    let observed = epoch(&db, "a");
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.create(entity("a", "transactional match"))?;
        Ok(())
    })
    .expect("transactional create");
    assert_ne!(epoch(&db, "a"), observed);
    assert_eq!(epoch(&db, "foreign"), None);
}

#[test]
fn failed_transaction_rolls_back_predicate_epoch_and_data() {
    let db = ElephantDb::new();
    let original = db.create(entity("a", "original")).expect("create");
    let observed = epoch(&db, "a");
    let result = db.tx::<(), ElephantDbError, _>(|tx| {
        tx.create(entity("a", "must roll back"))?;
        tx.hard_delete::<Entity>("a", &original.base.id)?;
        Err(ElephantDbError::DataIntegrity("injected failure".into()))
    });
    assert!(result.is_err());
    assert_eq!(epoch(&db, "a"), observed);
    assert_eq!(db.scan::<Entity>().expect("scan"), vec![original]);
}

#[test]
fn direct_space_move_fences_both_old_and_new_selector_sets() {
    let db = ElephantDb::new();
    let mut moved = db.create(entity("a", "moving")).expect("create a");
    db.create(entity("b", "existing b")).expect("create b");
    let old_a = epoch(&db, "a");
    let old_b = epoch(&db, "b");
    moved.base.space_id = "b".into();
    db.replace(moved).expect("trusted move");
    assert_ne!(epoch(&db, "a"), old_a);
    assert_ne!(epoch(&db, "b"), old_b);
}

#[test]
fn erased_run_rejects_late_artifact_and_keeps_other_space_independent() {
    let db = ElephantDb::new();
    let erased_source = RecordId::new_v4(TableName::Source);
    let erased_run = RecordId::new_v4(TableName::WorkRun);
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row(
            "source_tombstone_ref",
            serde_json::json!({
                "id": "source_tombstone_ref:old-run", "space_id": "a", "source_id": erased_source,
                "token": erased_run.id().to_string()
            }),
        )?;
        Ok(())
    })
    .expect("permanent source tombstone");
    let mut late = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "a".into(),
            "worker".into(),
        ),
        work_run: Link::new(erased_run),
        artifact_type: "knowledge_candidates".into(),
        data: serde_json::json!({"text":"erased source payload must not reappear"}),
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    assert!(db.create(late.clone()).is_err());
    assert!(db
        .tx::<_, ElephantDbError, _>(|tx| tx.create(late.clone()))
        .is_err());
    assert!(db
        .scan::<WorkArtifact>()
        .expect("scan artifacts")
        .is_empty());
    late.base.space_id = "b".into();
    db.create(late)
        .expect("same opaque reference in independent space is not erased");
}

#[test]
fn support_control_mutation_invalidates_prepared_plan_and_rolls_back() {
    let db = ElephantDb::new();
    db.create(entity("a", "baseline")).expect("baseline");
    let observed = epoch(&db, "a");
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row("source_effect_support", serde_json::json!({"id":"source_effect_support:support", "space_id":"a", "payload":"{}"}))?;
        Ok(())
    }).expect("support insert");
    assert_ne!(epoch(&db, "a"), observed);
    let observed = epoch(&db, "a");
    let result = db.tx::<(), ElephantDbError, _>(|tx| {
        tx.hard_delete_extension_row(
            "source_effect_support",
            "source_effect_support:support",
            "a",
        )?;
        Err(ElephantDbError::DataIntegrity("injected failure".into()))
    });
    assert!(result.is_err());
    assert_eq!(epoch(&db, "a"), observed);
    assert_eq!(
        db.scan_extension_rows("source_effect_support")
            .expect("supports")
            .len(),
        1
    );
}

#[test]
fn manifest_and_declared_extension_mutations_fence_read_epochs_and_erased_refs() {
    let db = ElephantDb::new();
    let erased = RecordId::new_v4(TableName::Source);
    db.tx::<_,ElephantDbError,_>(|tx| {tx.upsert_extension_row("source_tombstone_ref",serde_json::json!({"id":"source_tombstone_ref:erased","space_id":"a","source_id":erased,"token":erased.id().to_string()}))?;Ok(())}).expect("tombstone");
    for table in [
        "source_revision_mapping",
        "source_publication_completion",
        "source_bundle_registration",
        "declared_merge_metadata",
    ] {
        let before = epoch(&db, "a");
        db.tx::<_, ElephantDbError, _>(|tx| {
            tx.upsert_extension_row(
                table,
                serde_json::json!({"id":format!("{table}:safe"),"space_id":"a","value":"safe"}),
            )?;
            Ok(())
        })
        .expect("write");
        assert_ne!(
            epoch(&db, "a"),
            before,
            "{table} must fence absent and changed read inputs"
        );
        let observed = epoch(&db, "a");
        assert!(db.tx::<_,ElephantDbError,_>(|tx| {tx.upsert_extension_row(table,serde_json::json!({"id":format!("{table}:late"),"space_id":"a","source":erased}))?;Ok(())}).is_err(),"{table} must reject late payload after erasure");
        assert_eq!(
            epoch(&db, "a"),
            observed,
            "rejected late writes roll back their epoch"
        );
        assert_eq!(db.scan_extension_rows(table).expect("inventory").len(), 1);
    }
}

#[test]
fn native_string_and_integer_extension_keys_survive_snapshot_and_durable_replay_distinctly() {
    let db = ElephantDb::new();
    let mut delta = Vec::new();
    db.tx_with_commit::<_,ElephantDbError,_,_>(|tx| {
        tx.upsert_extension_row("consumer_note",serde_json::json!({"id":"42","space_id":"a","meaning":"string-key"}))?;
        tx.upsert_extension_row("consumer_note",serde_json::json!({"id":{"tb":"consumer_note","id":42},"space_id":"a","meaning":"integer-key"}))?;
        tx.upsert_extension_row("consumer_note",serde_json::json!({"id":{"table":"consumer_note","id":"42"},"space_id":"a","meaning":"string-key-updated"}))?;
        Ok(())
    },|changes|{delta.extend_from_slice(changes);Ok(())}).expect("distinct native key transaction");
    let rows = db.scan_extension_rows("consumer_note").expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .any(|(key, row)| key == "consumer_note:`42`" && row["meaning"] == "string-key-updated"));
    assert!(rows
        .iter()
        .any(|(key, row)| key == "consumer_note:42" && row["meaning"] == "integer-key"));
    let snapshot = ElephantDb::from_snapshot_bytes(&db.export_snapshot_bytes().expect("snapshot"))
        .expect("reopen snapshot");
    let replay = ElephantDb::new();
    replay
        .apply_recovery_batch(&delta)
        .expect("reopen durable delta");
    for restored in [snapshot, replay] {
        assert_eq!(
            restored
                .scan_extension_rows("consumer_note")
                .expect("both keys")
                .len(),
            2
        );
        restored
            .tx::<_, ElephantDbError, _>(|tx| {
                tx.hard_delete_extension_row("consumer_note", "consumer_note:`42`", "a")
            })
            .expect("delete string only");
        let survivors = restored
            .scan_extension_rows("consumer_note")
            .expect("integer remains");
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, "consumer_note:42");
        assert_eq!(survivors[0].1["meaning"], "integer-key");
    }
}

#[test]
fn saved_reprocess_membership_changes_fence_canonical_reads() {
    for transactional in [false, true] {
        let db = ElephantDb::new();
        db.create(entity("a", "epoch baseline")).unwrap();
        db.create(entity("b", "move baseline")).unwrap();
        let mut artifact = WorkArtifact {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkArtifact),
                "a".into(),
                "test".into(),
            ),
            work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
            artifact_type: "ordinary_staged_candidates".into(),
            data: serde_json::json!({}),
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };
        let before = epoch(&db, "a");
        if transactional {
            db.tx::<_, ElephantDbError, _>(|tx| tx.create(artifact.clone()))
                .unwrap();
        } else {
            db.create(artifact.clone()).unwrap();
        }
        assert_eq!(epoch(&db, "a"), before, "ordinary staging stays excluded");
        artifact.artifact_type = "reprocess_source_intent/v1".into();
        if transactional {
            db.tx::<_, ElephantDbError, _>(|tx| tx.replace(artifact.clone()))
                .unwrap();
        } else {
            db.replace(artifact.clone()).unwrap();
        }
        assert_ne!(epoch(&db, "a"), before);
        let before_a = epoch(&db, "a");
        let before_b = epoch(&db, "b");
        artifact.base.space_id = "b".into();
        artifact.artifact_type = "ordinary_staged_candidates".into();
        if transactional {
            db.tx::<_, ElephantDbError, _>(|tx| tx.replace(artifact.clone()))
                .unwrap();
        } else {
            db.replace(artifact.clone()).unwrap();
        }
        assert_ne!(
            epoch(&db, "a"),
            before_a,
            "old selected membership remains fenced on kind-out move"
        );
        assert_ne!(epoch(&db, "b"), before_b);
        artifact.artifact_type = "reprocess_selection/v1".into();
        db.replace(artifact.clone()).unwrap();
        let before = epoch(&db, "b");
        let rollback = db.tx::<(), ElephantDbError, _>(|tx| {
            tx.hard_delete_raw_row(TableName::WorkArtifact, "b", &artifact.base.id)?;
            Err(ElephantDbError::DataIntegrity(
                "injected membership rollback".into(),
            ))
        });
        assert!(rollback.is_err());
        assert_eq!(epoch(&db, "b"), before);
        if transactional {
            db.tx::<_, ElephantDbError, _>(|tx| {
                tx.hard_delete::<WorkArtifact>("b", &artifact.base.id)
            })
            .unwrap();
        } else {
            db.hard_delete::<WorkArtifact>("b", &artifact.base.id)
                .unwrap();
        }
        assert_ne!(epoch(&db, "b"), before);
    }
}

#[test]
fn private_producer_registration_and_read_nonces_preserve_parent_epoch() {
    let db = ElephantDb::new();
    db.create(entity("a", "prepared parent")).unwrap();
    db.create(entity("b", "other space")).unwrap();
    let before = epoch(&db, "a");
    let mut row = serde_json::json!({"id":"work_producer_origin:original", "space_id":"a", "payload":"original immutable producer"});
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row("work_producer_origin", row.clone())
    })
    .unwrap();
    assert_eq!(epoch(&db, "a"), before);
    row["read_nonce"] = "first read".into();
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.replace_extension_row(
            "work_producer_origin",
            "work_producer_origin:original",
            row.clone(),
        )
    })
    .unwrap();
    assert_eq!(epoch(&db, "a"), before);
    row["read_nonce"] = "second read".into();
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row("work_producer_origin", row.clone())
    })
    .unwrap();
    assert_eq!(epoch(&db, "a"), before);
    row["payload"] = "changed semantic producer".into();
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.replace_extension_row(
            "work_producer_origin",
            "work_producer_origin:original",
            row.clone(),
        )
    })
    .unwrap();
    assert_ne!(epoch(&db, "a"), before);
    let before_a = epoch(&db, "a");
    let before_b = epoch(&db, "b");
    row["space_id"] = "b".into();
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.upsert_extension_row("work_producer_origin", row.clone())
    })
    .unwrap();
    assert_ne!(epoch(&db, "a"), before_a);
    assert_ne!(epoch(&db, "b"), before_b);
    let before = epoch(&db, "b");
    db.tx::<_, ElephantDbError, _>(|tx| {
        tx.hard_delete_extension_row("work_producer_origin", "work_producer_origin:original", "b")
    })
    .unwrap();
    assert_ne!(epoch(&db, "b"), before);
}
