#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::canonical_commit::CanonicalEffect;
use chrono::Utc;
use elephant_types::records::{BaseFields, Entity, EntityType};
use elephant_types::{RecordId, TableName};
use surrealdb::kvs::recall_history::record_key;

fn fixture() -> (
    tempfile::TempDir,
    ErasureJournal,
    ErasureIntent,
    RecallHistoryAuthority,
) {
    let directory = tempfile::tempdir().unwrap();
    let journal = ErasureJournal::open(directory.path()).unwrap();
    let intent = journal
        .record_intent("named-space", &RecordId::new_v4(TableName::Source))
        .unwrap();
    let authority = RecallHistoryAuthority::for_backup_inspection(
        "ns".into(),
        "db".into(),
        journal.identity().into(),
        intent.space_id.clone(),
        *journal.reference_key_bytes(),
        vec![],
    )
    .unwrap();
    (directory, journal, intent, authority)
}

fn entity() -> CanonicalRecord {
    CanonicalRecord::Entity(Box::new(Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "named-space".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "identical independently useful content".into(),
    )))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Keep the fixture-owned native value and authority construction interface unchanged."
)]
fn native(
    identity: &Thing,
    value: Value,
    version: u64,
    timestamp: u64,
    current: bool,
) -> RecallHistoryRecord {
    RecallHistoryRecord {
        key: record_key("ns", "db", &identity.tb, &identity.id).unwrap(),
        value: RecallHistoryRecord::encode_value(&value).unwrap(),
        version,
        timestamp,
        current,
        tombstone: false,
    }
}

fn image(row: &CanonicalRecord, version: u64, current: bool) -> RecallHistoryRecord {
    let (id, mut value) = native_afterimage(row).unwrap();
    // Canonical CONTENT fields omit the identity that the native document
    // writer supplies. Historical fixtures model the complete persisted row.
    let Value::Object(fields) = &mut value else {
        unreachable!()
    };
    fields.insert("id".into(), Value::Thing(id.clone()));
    native(&id, value, version, version * 100, current)
}

fn nonce(
    row: &RecallHistoryRecord,
    version: u64,
    timestamp: u64,
    current: bool,
) -> RecallHistoryRecord {
    let mut value = row.decoded_value().unwrap();
    let Value::Object(fields) = &mut value else {
        unreachable!()
    };
    fields.insert(
        "canonical_read_nonce".into(),
        Value::from(uuid::Uuid::new_v4().to_string()),
    );
    let mut copy = row.clone();
    copy.value = RecallHistoryRecord::encode_value(&value).unwrap();
    copy.version = version;
    copy.timestamp = timestamp;
    copy.current = current;
    copy
}

fn receipt(
    intent: &ErasureIntent,
    commit: &str,
    sequence: u64,
    row: &CanonicalRecord,
) -> SourceDecisionReceipt {
    SourceDecisionReceipt {
        space_id: intent.space_id.clone(),
        commit_id: commit.into(),
        sequence,
        source_ids: vec![intent.source_id.clone()],
        dependency_ids: vec![],
        effects: vec![CanonicalEffect {
            before: None,
            after: Some(row.clone()),
        }],
        extension_effects: vec![],
        outbox_ids: vec![],
        committed_at: Utc::now(),
    }
}

fn decision(receipt: &SourceDecisionReceipt, version: u64) -> RecallHistoryRecord {
    let id = control_thing(
        DECISION_TABLE,
        &stable_key(&[&receipt.space_id, &receipt.commit_id]),
    );
    let mut payload = receipt.clone();
    payload.sequence = 0;
    let value = Value::Object(
        [
            ("id".into(), Value::Thing(id.clone())),
            ("space_id".into(), Value::from(receipt.space_id.clone())),
            (
                "payload".into(),
                Value::from(serde_json::to_string(&payload).unwrap()),
            ),
            (
                "sequence".into(),
                Value::from(i64::try_from(receipt.sequence).unwrap()),
            ),
        ]
        .into_iter()
        .collect(),
    );
    native(&id, value, version, version * 100, true)
}

fn selected(
    intent: &ErasureIntent,
    authority: &RecallHistoryAuthority,
    rows: Vec<RecallHistoryRecord>,
) -> BTreeSet<String> {
    let snapshot = RecallHistorySnapshot::new("ns".into(), "db".into(), rows).unwrap();
    project(
        &records(&snapshot).unwrap(),
        &intent.space_id,
        Some(&intent.source_id),
        authority,
        &BTreeMap::new(),
        "unused-import-token",
    )
    .unwrap()
}

fn token(authority: &RecallHistoryAuthority, row: &RecallHistoryRecord) -> String {
    authority.version_commitment(row).unwrap()
}

#[test]
fn history_causality_seeds_exact_transaction_and_preserves_identical_earlier_version() {
    let (_directory, _journal, intent, authority) = fixture();
    let row = entity();
    let old = image(&row, 7, false);
    let affected = image(&row, 11, true);
    let proofs = selected(
        &intent,
        &authority,
        vec![
            old.clone(),
            affected.clone(),
            decision(&receipt(&intent, "decision-a", 1, &row), 11),
        ],
    );
    assert_eq!(proofs, BTreeSet::from([token(&authority, &affected)]));
    assert!(!proofs.contains(&token(&authority, &old)));
}

#[test]
fn history_causality_follows_nonce_copies_in_native_order_despite_explicit_timestamps() {
    let (_directory, _journal, intent, authority) = fixture();
    let row = entity();
    let seed = image(&row, 11, false);
    let copy = nonce(&seed, 12, 5, false);
    let current = nonce(&copy, 13, 2, true);
    assert_eq!(
        selected(
            &intent,
            &authority,
            vec![
                seed.clone(),
                copy.clone(),
                current.clone(),
                decision(&receipt(&intent, "decision-a", 1, &row), 11)
            ]
        ),
        [seed, copy, current]
            .iter()
            .map(|row| token(&authority, row))
            .collect()
    );
}

#[test]
fn history_causality_never_jumps_semantic_change_or_tombstone() {
    for tombstone in [false, true] {
        let (_directory, _journal, intent, authority) = fixture();
        let row = entity();
        let seed = image(&row, 11, false);
        let mut boundary = nonce(&seed, 12, 1200, false);
        if tombstone {
            boundary.tombstone = true;
            boundary.value.clear();
        } else {
            let Value::Object(mut value) = boundary.decoded_value().unwrap() else {
                unreachable!()
            };
            value.insert("name".into(), Value::from("independent semantic change"));
            boundary.value = RecallHistoryRecord::encode_value(&Value::Object(value)).unwrap();
        }
        let current = nonce(&seed, 13, 1300, true);
        assert_eq!(
            selected(
                &intent,
                &authority,
                vec![
                    seed.clone(),
                    boundary,
                    current,
                    decision(&receipt(&intent, "decision-a", 1, &row), 11)
                ]
            ),
            BTreeSet::from([token(&authority, &seed)])
        );
    }
}

fn support(
    intent: &ErasureIntent,
    row: &CanonicalRecord,
    source: RecordId,
    commit: Option<&str>,
    version: u64,
) -> RecallHistoryRecord {
    let support = SourceEffectSupport {
        space_id: intent.space_id.clone(),
        source_id: source,
        source_revision: "original-revision".into(),
        record_id: row.base().id.clone(),
        historical_unknown: None,
        independent: true,
        record_fingerprint: row.fingerprint().unwrap(),
        last_commit_id: commit.map(str::to_owned),
    };
    let id = control_thing(
        "source_effect_support",
        &crate::canonical_commit::support_key(
            &support.space_id,
            &support.source_id,
            &support.source_revision,
            &support.record_id,
        ),
    );
    let value = Value::Object(
        [
            ("id".into(), Value::Thing(id.clone())),
            ("space_id".into(), Value::from(support.space_id.clone())),
            (
                "source_id".into(),
                Value::from(support.source_id.to_string()),
            ),
            (
                "source_revision".into(),
                Value::from(support.source_revision.clone()),
            ),
            (
                "record_id".into(),
                Value::from(support.record_id.to_string()),
            ),
            ("independent".into(), Value::Bool(true)),
            (
                "payload".into(),
                Value::from(serde_json::to_string(&support).unwrap()),
            ),
        ]
        .into_iter()
        .collect(),
    );
    native(&id, value, version, version * 100, true)
}

#[test]
fn history_causality_stops_at_identical_independent_support_and_unknown_pointer() {
    for commit in [Some("decision-b"), None] {
        let (_directory, _journal, intent, authority) = fixture();
        let row = entity();
        let seed = image(&row, 11, false);
        let current = nonce(&seed, 13, 1300, true);
        let independent = support(
            &intent,
            &row,
            RecordId::new_v4(TableName::Source),
            commit,
            12,
        );
        assert_eq!(
            selected(
                &intent,
                &authority,
                vec![
                    seed.clone(),
                    current,
                    independent,
                    decision(&receipt(&intent, "decision-a", 1, &row), 11)
                ]
            ),
            BTreeSet::from([token(&authority, &seed)])
        );
    }
}

#[test]
fn history_causality_does_not_treat_invalidated_support_as_independent() {
    let (_directory, _journal, intent, authority) = fixture();
    let row = entity();
    let seed = image(&row, 11, false);
    let current = nonce(&seed, 13, 1300, true);
    let dependent = support(
        &intent,
        &row,
        RecordId::new_v4(TableName::Source),
        Some("decision-a"),
        12,
    );
    assert_eq!(
        selected(
            &intent,
            &authority,
            vec![
                seed.clone(),
                current.clone(),
                dependent,
                decision(&receipt(&intent, "decision-a", 1, &row), 11)
            ]
        ),
        [seed, current]
            .iter()
            .map(|row| token(&authority, row))
            .collect()
    );
}

#[test]
fn history_causality_requires_native_seed_and_ignores_noop_decisions() {
    let (_directory, _journal, intent, authority) = fixture();
    let row = entity();
    let later = image(&row, 20, true);
    assert!(selected(
        &intent,
        &authority,
        vec![
            later,
            decision(&receipt(&intent, "decision-a", 1, &row), 11)
        ]
    )
    .is_empty());
    let mut no_op = receipt(&intent, "decision-a", 1, &row);
    no_op.effects[0].before = Some(row.clone());
    assert!(selected(
        &intent,
        &authority,
        vec![image(&row, 11, true), decision(&no_op, 11)]
    )
    .is_empty());
}

#[test]
fn history_causality_uses_original_transitive_decision_closure_and_native_sequence() {
    let (_directory, _journal, intent, authority) = fixture();
    let parent = entity();
    let child = entity();
    let seed = image(&parent, 11, true);
    let derived = image(&child, 12, true);
    let mut dependent = receipt(&intent, "decision-dependent", 2, &child);
    dependent.source_ids.clear();
    dependent.dependency_ids.push(parent.base().id.clone());
    assert_eq!(
        selected(
            &intent,
            &authority,
            vec![
                decision(&dependent, 12),
                derived.clone(),
                decision(&receipt(&intent, "decision-a", 1, &parent), 11),
                seed.clone()
            ]
        ),
        [seed, derived]
            .iter()
            .map(|row| token(&authority, row))
            .collect()
    );
}

#[test]
fn history_causality_rejects_mismatched_native_document_identity_and_receipt_scope() {
    let (_directory, _journal, intent, authority) = fixture();
    let row = entity();
    let mut bad = image(&row, 11, true);
    let Value::Object(mut value) = bad.decoded_value().unwrap() else {
        unreachable!()
    };
    value.insert("id".into(), Value::Thing(Thing::from(("entity", "wrong"))));
    bad.value = RecallHistoryRecord::encode_value(&Value::Object(value)).unwrap();
    let snapshot = RecallHistorySnapshot::new("ns".into(), "db".into(), vec![bad]).unwrap();
    assert!(records(&snapshot).is_err());
    let mut bad_receipt = receipt(&intent, "decision-a", 1, &row);
    bad_receipt.effects[0]
        .after
        .as_mut()
        .unwrap()
        .base_mut()
        .space_id = "foreign-space".into();
    let snapshot =
        RecallHistorySnapshot::new("ns".into(), "db".into(), vec![decision(&bad_receipt, 11)])
            .unwrap();
    assert!(project(
        &records(&snapshot).unwrap(),
        &intent.space_id,
        Some(&intent.source_id),
        &authority,
        &BTreeMap::new(),
        "unused-import-token"
    )
    .is_err());
}

#[test]
fn history_causality_never_ignores_unknown_nonce_shapes_or_extra_payload_changes() {
    for invalid_nonce in [true, false] {
        let (_directory, _journal, intent, authority) = fixture();
        let row = entity();
        let seed = image(&row, 11, false);
        let mut current = nonce(&seed, 12, 1200, true);
        let Value::Object(mut value) = current.decoded_value().unwrap() else {
            unreachable!()
        };
        if invalid_nonce {
            value.insert(
                "canonical_read_nonce".into(),
                Value::from("arbitrary payload"),
            );
        } else {
            value.insert(
                "additional_payload".into(),
                Value::from("different native field"),
            );
        }
        current.value = RecallHistoryRecord::encode_value(&Value::Object(value)).unwrap();
        assert_eq!(
            selected(
                &intent,
                &authority,
                vec![
                    seed.clone(),
                    current,
                    decision(&receipt(&intent, "decision-a", 1, &row), 11)
                ]
            ),
            BTreeSet::from([token(&authority, &seed)])
        );
    }
}

#[test]
fn history_causality_authenticated_import_survives_rebatched_transactions_and_later_nonce() {
    let (_directory, journal, intent, authority) = fixture();
    let row = entity();
    let imported = nonce(&image(&row, 11, false), 900, 500, false);
    let later = nonce(&imported, 901, 1, true);
    let imported_decision = decision(&receipt(&intent, "decision-a", 1, &row), 800);
    let snapshot = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![imported.clone(), later.clone(), imported_decision.clone()],
    )
    .unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    let proofs = BTreeMap::from([
        (
            token(&authority, &imported),
            BTreeSet::from([source.clone()]),
        ),
        (token(&authority, &imported_decision), BTreeSet::new()),
    ]);
    let erased = project(
        &records(&snapshot).unwrap(),
        &intent.space_id,
        Some(&intent.source_id),
        &authority,
        &proofs,
        &source,
    )
    .unwrap();
    assert_eq!(
        erased,
        [imported, later]
            .iter()
            .map(|row| token(&authority, row))
            .collect()
    );
}

#[test]
fn history_causality_explicit_empty_import_coverage_prevents_false_same_batch_ownership() {
    let (_directory, journal, intent, authority) = fixture();
    let row = entity();
    let independent = image(&row, 900, true);
    let imported_decision = decision(&receipt(&intent, "decision-a", 1, &row), 900);
    let snapshot = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![independent.clone(), imported_decision.clone()],
    )
    .unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    let proofs = BTreeMap::from([
        (token(&authority, &independent), BTreeSet::new()),
        (token(&authority, &imported_decision), BTreeSet::new()),
    ]);
    assert!(project(
        &records(&snapshot).unwrap(),
        &intent.space_id,
        Some(&intent.source_id),
        &authority,
        &proofs,
        &source
    )
    .unwrap()
    .is_empty());
}

#[test]
fn history_causality_imported_decision_still_invalidates_new_dependent_native_decision() {
    let (_directory, journal, intent, authority) = fixture();
    let parent = entity();
    let child = entity();
    let imported = image(&parent, 900, true);
    let imported_decision = decision(&receipt(&intent, "decision-a", 1, &parent), 800);
    let mut dependent = receipt(&intent, "new-dependent", 2, &child);
    dependent.source_ids.clear();
    dependent.dependency_ids.push(parent.base().id.clone());
    let derived = image(&child, 950, true);
    let snapshot = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![
            imported.clone(),
            imported_decision.clone(),
            derived.clone(),
            decision(&dependent, 950),
        ],
    )
    .unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    let proofs = BTreeMap::from([
        (
            token(&authority, &imported),
            BTreeSet::from([source.clone()]),
        ),
        (token(&authority, &imported_decision), BTreeSet::new()),
    ]);
    assert_eq!(
        project(
            &records(&snapshot).unwrap(),
            &intent.space_id,
            Some(&intent.source_id),
            &authority,
            &proofs,
            &source
        )
        .unwrap(),
        [imported, derived]
            .iter()
            .map(|row| token(&authority, row))
            .collect()
    );
}

#[test]
fn backup_causality_covers_future_intents_and_every_current_record_including_empty_causes() {
    let directory = tempfile::tempdir().unwrap();
    let journal = ErasureJournal::open(directory.path()).unwrap();
    let future = ErasureIntent {
        intent_id: uuid::Uuid::new_v4().to_string(),
        space_id: "named-space".into(),
        source_id: RecordId::new_v4(TableName::Source),
        recorded_at: Utc::now(),
    };
    let row = entity();
    let independent = entity();
    let seed = image(&row, 11, false);
    let current = nonce(&seed, 12, 1, true);
    let baseline = image(&independent, 7, true);
    let original_decision = decision(&receipt(&future, "decision-a", 1, &row), 11);
    let snapshot = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![
            seed,
            current.clone(),
            baseline.clone(),
            original_decision.clone(),
        ],
    )
    .unwrap();
    assert!(journal.intents().unwrap().is_empty());
    let proof = backup_history_causality(&journal, &snapshot).unwrap();
    let content = |row: &RecallHistoryRecord| {
        surrealdb::kvs::recall_history::backup_origin_content_commitment(
            journal.identity(),
            "ns",
            "db",
            journal.reference_key_bytes(),
            &row.key,
            &row.value,
        )
        .unwrap()
    };
    assert_eq!(proof.len(), 3);
    assert_eq!(
        proof[&content(&current)],
        BTreeSet::from([
            source_token(&journal, &snapshot, &future.space_id, &future.source_id).unwrap()
        ])
    );
    assert!(proof[&content(&baseline)].is_empty());
    assert!(proof[&content(&original_decision)].is_empty());
    let accepted_later = journal
        .record_intent(&future.space_id, &future.source_id)
        .unwrap();
    assert!(proof[&content(&current)].contains(
        &source_token(
            &journal,
            &snapshot,
            &accepted_later.space_id,
            &accepted_later.source_id
        )
        .unwrap()
    ));
}

#[test]
fn versionless_current_rows_never_claim_native_transaction_coincidence() {
    let (_directory, journal, intent, authority) = fixture();
    let row = entity();
    let current = image(&row, 0, true);
    let original_decision = decision(&receipt(&intent, "decision-a", 1, &row), 0);
    let snapshot =
        RecallHistorySnapshot::new("ns".into(), "db".into(), vec![current, original_decision])
            .unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    assert!(project(
        &records(&snapshot).unwrap(),
        &intent.space_id,
        Some(&intent.source_id),
        &authority,
        &BTreeMap::new(),
        &source
    )
    .unwrap()
    .is_empty());
    let proof = backup_history_causality(&journal, &snapshot).unwrap();
    assert_eq!(proof.len(), 2);
    assert!(proof.values().all(BTreeSet::is_empty));
}

#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "The oracle compares native keys without mutating them; regex cache internals do not change key identity."
)]
fn versionless_live_inverse_projection_binds_full_current_bytes_and_rejects_changed_capture() {
    let (_directory, journal, intent, _authority) = fixture();
    let row = entity();
    let current = image(&row, 0, true);
    let snapshot =
        RecallHistorySnapshot::new("ns".into(), "db".into(), vec![current.clone()]).unwrap();
    let rows = records(&snapshot).unwrap();
    let by_id = rows
        .iter()
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let (identity, expected) = native_afterimage(&row).unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    let mut output = backup_history_causality(&journal, &snapshot).unwrap();
    record_live_inverse(
        &by_id,
        &mut output,
        &journal,
        &snapshot,
        &intent.space_id,
        &identity,
        &expected,
        &source,
    )
    .unwrap();
    let content = surrealdb::kvs::recall_history::backup_origin_content_commitment(
        journal.identity(),
        "ns",
        "db",
        journal.reference_key_bytes(),
        &current.key,
        &current.value,
    )
    .unwrap();
    assert_eq!(output[&content], BTreeSet::from([source.clone()]));
    let Value::Object(mut changed) = expected else {
        unreachable!()
    };
    changed.insert("name".into(), Value::from("capture changed after planning"));
    assert!(record_live_inverse(
        &by_id,
        &mut output,
        &journal,
        &snapshot,
        &intent.space_id,
        &identity,
        &Value::Object(changed),
        &source
    )
    .is_err());
}

#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "The oracle compares native keys without mutating them; regex cache internals do not change key identity."
)]
fn versionless_live_inverse_projection_refuses_missing_inventory_or_wrong_space() {
    let (_directory, journal, intent, _authority) = fixture();
    let row = entity();
    let snapshot =
        RecallHistorySnapshot::new("ns".into(), "db".into(), vec![image(&row, 0, true)]).unwrap();
    let rows = records(&snapshot).unwrap();
    let by_id = rows
        .iter()
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let (identity, expected) = native_afterimage(&row).unwrap();
    let source = source_token(&journal, &snapshot, &intent.space_id, &intent.source_id).unwrap();
    let mut output = backup_history_causality(&journal, &snapshot).unwrap();
    assert!(record_live_inverse(
        &BTreeMap::new(),
        &mut output,
        &journal,
        &snapshot,
        &intent.space_id,
        &identity,
        &expected,
        &source
    )
    .is_err());
    assert!(record_live_inverse(
        &by_id,
        &mut output,
        &journal,
        &snapshot,
        "foreign",
        &identity,
        &expected,
        &source
    )
    .is_err());
}

#[path = "source_history_support_format_tests.rs"]
mod support_format;

#[test]
fn native_current_closure_diagnostic_preserves_refusal_and_exact_retention() {
    let (_directory, journal, intent, _) = fixture();
    let source_membership = crate::reference_fence::reference_identity_token(
        journal.identity(),
        &intent.space_id,
        journal.reference_key_bytes(),
        &crate::surreal::thing_from_record_id(&intent.source_id),
    )
    .unwrap();
    let authority = RecallHistoryAuthority::new(
        "ns".into(),
        "db".into(),
        journal.identity().into(),
        intent.space_id.clone(),
        *journal.reference_key_bytes(),
        crate::reference_fence::TYPED_UUID_TABLES
            .iter()
            .map(|table| (*table).into())
            .collect(),
        BTreeSet::from([source_membership]),
    )
    .unwrap();
    let mut row = entity();
    row.base_mut()
        .provenance
        .sources
        .push(intent.source_id.clone());
    let current = image(&row, 4, true);
    let snapshot =
        RecallHistorySnapshot::new("ns".into(), "db".into(), vec![current.clone()]).unwrap();
    let rows = records(&snapshot).unwrap();
    let error = check_current_record_closure(&rows, &authority, &BTreeSet::new()).unwrap_err();
    assert!(matches!(error, StorageError::Surreal(_)));
    let message = error.to_string();
    assert!(message.contains("current source-bearing record survived the original purge"));
    assert!(message.contains("table class: entity"));
    assert!(!message.contains(&intent.source_id.to_string()));
    assert!(!message.contains("identical independently useful content"));
    let retained = BTreeSet::from([authority.version_commitment(&current).unwrap()]);
    assert!(check_current_record_closure(&rows, &authority, &retained).is_ok());
    let historical = image(&row, 3, false);
    // A complete native inventory must include one current version per key.
    // Keep the source-bearing historical image, with its now-independent head.
    row.base_mut().provenance.sources.clear();
    let independent_current = image(&row, 4, true);
    let snapshot = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![historical, independent_current],
    )
    .unwrap();
    assert!(check_current_record_closure(
        &records(&snapshot).unwrap(),
        &authority,
        &BTreeSet::new()
    )
    .is_ok());
}

#[test]
fn native_current_closure_diagnostic_never_echoes_dynamic_table_names() {
    assert_eq!(closure_table_class("private-tenant-canary"), "other");
    assert_eq!(closure_table_class("rel"), "rel");
    assert_eq!(
        closure_table_class("source_effect_support"),
        "source_effect_support"
    );
}
