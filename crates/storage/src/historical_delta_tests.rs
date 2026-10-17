#![allow(clippy::expect_used)]
use super::*;
use elephant_types::{
    records::{BaseFields, Entity},
    TableName,
};

fn record(name: &str) -> TransferRecord {
    TransferRecord::Entity(Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "household".into(),
            "original-agent".into(),
        ),
        "person".into(),
        name.into(),
    ))
}

fn changed(before: &TransferRecord, name: &str) -> TransferRecord {
    let mut after = before.clone();
    if let TransferRecord::Entity(entity) = &mut after {
        entity.display_name = name.into();
    }
    after
}

fn row(
    before: Option<TransferRecord>,
    after: Option<TransferRecord>,
    kind: HistoricalChangeKind,
) -> HistoricalDeltaRow {
    HistoricalDeltaRow {
        id: before
            .as_ref()
            .or(after.as_ref())
            .expect("row")
            .base()
            .id
            .clone(),
        kind,
        before_fingerprint: before
            .as_ref()
            .map(runtime_row_fingerprint)
            .transpose()
            .expect("before"),
        after_fingerprint: after
            .as_ref()
            .map(runtime_row_fingerprint)
            .transpose()
            .expect("after"),
        before,
        after,
        destructive_intent: false,
    }
}

fn batch(rows: Vec<HistoricalDeltaRow>) -> HistoricalDeltaBatch {
    HistoricalDeltaBatch {
        version: 1,
        migration_id: "1".repeat(64),
        source_space_id: "household".into(),
        space_id: "household".into(),
        before_snapshot_sha256: "2".repeat(64),
        after_snapshot_sha256: "3".repeat(64),
        planner_delta_sha256: "4".repeat(64),
        batch_ordinal: 0,
        previous_batch_receipt: None,
        expected_epoch: "actual-observed-epoch".into(),
        rows,
        embeddings: Vec::new(),
        retained_controls: Vec::new(),
    }
}

#[test]
fn graph_only_preparation_binds_full_before_image_and_batch_identity() {
    let before = record("original");
    let change = row(
        Some(before.clone()),
        Some(changed(&before, "manual edit")),
        HistoricalChangeKind::Update,
    );
    change.validate_target(Some(&before)).expect("exact target");
    assert!(change
        .validate_target(Some(&changed(&before, "concurrent edit")))
        .is_err());
    assert!(change.validate_target(None).is_err());
    let input = batch(vec![change]);
    let first = input.clone().prepare().expect("prepare");
    assert_eq!(
        first.digest,
        input.clone().prepare().expect("replay").digest
    );
    let mut changed_epoch = input;
    changed_epoch.expected_epoch = "another".into();
    assert_ne!(
        first.digest,
        changed_epoch
            .prepare()
            .expect("prepare distinct input")
            .digest
    );
}

#[test]
fn duplicate_cross_space_forged_before_and_reordered_chain_refuse() {
    let before = record("original");
    let change = row(
        Some(before.clone()),
        Some(changed(&before, "edit")),
        HistoricalChangeKind::Update,
    );
    assert!(batch(vec![change.clone(), change.clone()])
        .prepare()
        .is_err());
    let mut forged = change.clone();
    forged.before_fingerprint = Some("0".repeat(64));
    assert!(batch(vec![forged]).prepare().is_err());
    let mut foreign = batch(vec![change.clone()]);
    foreign.source_space_id = "other".into();
    assert!(foreign.prepare().is_err());
    let mut missing_receipt = batch(vec![change]);
    missing_receipt.batch_ordinal = 1;
    assert!(missing_receipt.prepare().is_err());
}

#[test]
fn timestamp_change_is_not_lost_and_removal_needs_explicit_intent() {
    let before = record("original");
    let mut after = before.clone();
    if let TransferRecord::Entity(entity) = &mut after {
        entity.base.updated_at += chrono::Duration::seconds(1);
    }
    assert_ne!(
        runtime_row_fingerprint(&before).expect("hash"),
        runtime_row_fingerprint(&after).expect("hash")
    );
    batch(vec![row(
        Some(before.clone()),
        Some(after),
        HistoricalChangeKind::Update,
    )])
    .prepare()
    .expect("timestamp update");
    let mut removal = row(Some(before), None, HistoricalChangeKind::Remove);
    assert!(batch(vec![removal.clone()]).prepare().is_err());
    removal.destructive_intent = true;
    batch(vec![removal])
        .prepare()
        .expect("declaration only, no erase authority");
}

#[test]
fn unknown_typed_tables_cannot_enter_batch() {
    let bad = serde_json::json!({"table":"canonical_epoch","record":{}});
    assert!(serde_json::from_value::<TransferRecord>(bad).is_err());
}

#[test]
fn soft_delete_restore_and_create_guards_remain_distinct() {
    let original = record("original");
    let mut deleted = original.clone();
    if let TransferRecord::Entity(entity) = &mut deleted {
        entity.base.deleted_at = Some(chrono::Utc::now());
    }
    let mut deletion = row(
        Some(original.clone()),
        Some(deleted.clone()),
        HistoricalChangeKind::SoftDelete,
    );
    assert!(batch(vec![deletion.clone()]).prepare().is_err());
    deletion.destructive_intent = true;
    batch(vec![deletion])
        .prepare()
        .expect("explicit tombstone declaration");
    batch(vec![row(
        Some(deleted),
        Some(original.clone()),
        HistoricalChangeKind::Restore,
    )])
    .prepare()
    .expect("restore declaration");
    let create = row(None, Some(original.clone()), HistoricalChangeKind::Create);
    create.validate_target(None).expect("absence");
    assert!(create.validate_target(Some(&original)).is_err());
    let mut wrong = original;
    if let TransferRecord::Entity(entity) = &mut wrong {
        entity.base.id = RecordId::new_v4(TableName::Assertion);
    }
    assert!(runtime_row_fingerprint(&wrong).is_err());
}

#[test]
fn operational_and_media_records_use_full_shared_transfer_type() {
    use elephant_types::records::{WorkItem, WorkItemStatus};
    let work = TransferRecord::WorkItem(WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "household".into(),
            "original".into(),
        ),
        kind: "historical".into(),
        payload: serde_json::json!({"exact": "history"}),
        status: WorkItemStatus::Failed,
        priority: 1,
        idempotency_key: "original-key".into(),
        attempts: 8,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: Some("original cause".into()),
    });
    let mut after = work.clone();
    if let TransferRecord::WorkItem(work) = &mut after {
        work.last_error = Some("snapshot correction".into());
    }
    let change = row(
        Some(work.clone()),
        Some(after),
        HistoricalChangeKind::Update,
    );
    change
        .validate_target(Some(&work))
        .expect("exact operational original");
    batch(vec![change])
        .prepare()
        .expect("shared operational variant");
}

#[test]
fn explicit_namespace_projection_preserves_original_hash_domain() {
    let before = record("original");
    let change = row(
        Some(before.clone()),
        Some(changed(&before, "edit")),
        HistoricalChangeKind::Update,
    );
    let fingerprint = change.before_fingerprint.clone();
    let mut input = batch(vec![change]);
    input.space_id = "new-target".into();
    let prepared = input.prepare().expect("explicit target declaration");
    assert_eq!(prepared.input.rows[0].before_fingerprint, fingerprint);
    assert_eq!(
        prepared.input.rows[0]
            .before
            .as_ref()
            .expect("original")
            .base()
            .space_id,
        "household"
    );
}
