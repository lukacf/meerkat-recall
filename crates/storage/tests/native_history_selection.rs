//! Controlled native-version fixtures for the active history selector.
//! These are source-policy regressions, not installed erasure acceptance.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use surrealdb::kvs::recall_history::{
    backup_origin_authenticator, backup_origin_journal_authenticator,
    backup_origin_source_commitment, ordinary_index_key, record_key, HistoryDecision,
    HistorySelection, RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot,
};
use surrealdb::sql::{Array, Id, Thing, Value};

const JOURNAL: &str = "f7f0934e-fc5f-4c4c-bb2f-0674a22b7a7a";
const KEY: [u8; 32] = [91; 32];

#[expect(
    clippy::iter_on_single_items,
    reason = "Retain the original singleton erased-token collection used to construct the fixed history authority fixture."
)]
fn authority() -> RecallHistoryAuthority {
    let source = Thing::from(("source", "erased"));
    let token = surrealdb::recall_reference_identity_token(JOURNAL, "home", &KEY, &[], &source)
        .expect("fixed source identity");
    RecallHistoryAuthority::new(
        "ns".into(),
        "db".into(),
        JOURNAL.into(),
        "home".into(),
        KEY,
        vec![],
        [token].into_iter().collect(),
    )
    .expect("original journal authority")
}

fn row(owner: &str, version: u64, current: bool) -> RecallHistoryRecord {
    let value = Value::Object(
        [
            ("space_id".into(), Value::from("home")),
            (
                "source_id".into(),
                Value::Thing(Thing::from(("source", owner))),
            ),
            (
                "text".into(),
                Value::from("Independent content may be byte-identical."),
            ),
        ]
        .into_iter()
        .collect(),
    );
    RecallHistoryRecord {
        key: record_key("ns", "db", "entity", &Id::String("shared".into())).unwrap(),
        value: RecallHistoryRecord::encode_value(&value).unwrap(),
        version,
        timestamp: version * 100,
        current,
        tombstone: false,
    }
}

fn snapshot(rows: Vec<RecallHistoryRecord>) -> RecallHistorySnapshot {
    RecallHistorySnapshot::new("ns".into(), "db".into(), rows).unwrap()
}

fn bind(snapshot: &RecallHistorySnapshot, erased: BTreeSet<String>) -> RecallHistoryAuthority {
    authority()
        .bind_history(snapshot.fingerprint.clone(), erased, BTreeSet::new())
        .unwrap()
}

#[test]
fn native_history_preserves_independent_versions_around_erased_value() {
    let old = row("independent", 1, false);
    let affected = row("erased", 2, false);
    let current = row("independent", 3, true);
    let snapshot = snapshot(vec![current.clone(), affected.clone(), old.clone()]);
    let selected =
        HistorySelection::prepare(&[bind(&snapshot, BTreeSet::new())], &snapshot).unwrap();
    for independent in [&old, &current] {
        assert_eq!(
            selected
                .decision(&independent.key, independent.version, independent.timestamp)
                .unwrap(),
            HistoryDecision::Keep
        );
    }
    assert_eq!(
        selected
            .decision(&affected.key, affected.version, affected.timestamp)
            .unwrap(),
        HistoryDecision::Tombstone
    );
    assert_eq!(snapshot.records[0].value, old.value);
    assert_eq!(snapshot.records[2].value, current.value);
}

#[test]
#[expect(
    clippy::iter_on_single_items,
    reason = "Retain the original singleton causal-proof collection and version-binding assertions."
)]
fn native_history_causal_proof_binds_version_even_when_content_matches() {
    let old = row("independent", 1, false);
    let affected = row("independent", 2, false);
    let current = row("independent", 3, true);
    assert_eq!(old.value, affected.value);
    let proof = authority().version_commitment(&affected).unwrap();
    let snapshot = snapshot(vec![old.clone(), affected.clone(), current.clone()]);
    let selected =
        HistorySelection::prepare(&[bind(&snapshot, [proof].into_iter().collect())], &snapshot)
            .unwrap();
    assert_eq!(
        selected
            .decision(&old.key, old.version, old.timestamp)
            .unwrap(),
        HistoryDecision::Keep
    );
    assert_eq!(
        selected
            .decision(&affected.key, affected.version, affected.timestamp)
            .unwrap(),
        HistoryDecision::Tombstone
    );
    assert_eq!(
        selected
            .decision(&current.key, current.version, current.timestamp)
            .unwrap(),
        HistoryDecision::Keep
    );
}

#[test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned original authority clone and exact compare-and-swap and uninspected-version refusal oracle."
)]
fn native_history_cas_and_uninspected_version_refuse() {
    let record = row("independent", 1, true);
    let original = snapshot(vec![record.clone()]);
    let authority = bind(&original, BTreeSet::new());
    let selected = HistorySelection::prepare(&[authority.clone()], &original).unwrap();
    assert!(selected.decision(&record.key, 2, 200).is_err());
    let mut changed = record;
    changed.timestamp += 1;
    let changed = snapshot(vec![changed]);
    assert!(HistorySelection::prepare(&[authority], &changed).is_err());
}

#[test]
fn native_history_hidden_source_versions_and_identity_are_removed() {
    let old = row("erased", 1, false);
    let deleted = RecallHistoryRecord {
        key: old.key.clone(),
        value: vec![],
        version: 2,
        timestamp: 200,
        current: true,
        tombstone: true,
    };
    let snapshot = snapshot(vec![old.clone(), deleted.clone()]);
    let selected =
        HistorySelection::prepare(&[bind(&snapshot, BTreeSet::new())], &snapshot).unwrap();
    for record in [&old, &deleted] {
        assert_eq!(
            selected
                .decision(&record.key, record.version, record.timestamp)
                .unwrap(),
            HistoryDecision::Drop
        );
    }
}

#[test]
fn native_history_transport_rejects_unbound_authority_and_preserves_u64() {
    assert!(RecallHistoryAuthority::from_rpc_value(authority().to_rpc_value()).is_err());
    let mut record = row("independent", 1, true);
    record.version = u64::MAX - 1;
    record.timestamp = u64::MAX;
    let original = snapshot(vec![record]);
    let restored = RecallHistorySnapshot::from_rpc_value(original.to_rpc_value()).unwrap();
    assert_eq!(restored.records, original.records);
    assert_eq!(restored.fingerprint, original.fingerprint);
    let authority = bind(&original, BTreeSet::new());
    let restored = RecallHistoryAuthority::from_rpc_value(authority.to_rpc_value()).unwrap();
    assert_eq!(restored.namespace(), "ns");
    assert_eq!(restored.database(), "db");
    assert!(!format!("{restored:?}").contains(JOURNAL));
}

#[test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    clippy::iter_on_single_items,
    reason = "Retain the original owned authority clone and singleton index collection in the versionless stage-write oracle."
)]
fn native_history_versionless_indexes_are_omitted_before_stage_write() {
    let fields = Array::from(vec![Value::from("stale source-bearing field")]);
    let key = ordinary_index_key("ns", "db", "entity", "name", &fields, None).unwrap();
    // An ownerless unique-index deletion still has a source-bearing physical key.
    // Nonversioned SKV keeps native clocks; these are deliberately not zero.
    let index = RecallHistoryRecord {
        key: key.clone(),
        value: vec![],
        version: 27,
        timestamp: 9001,
        tombstone: true,
        current: true,
    };
    let current = row("independent", 26, true);
    let original = snapshot(vec![index, current.clone()]);
    let authority = bind(&original, BTreeSet::new());
    assert!(HistorySelection::prepare(&[authority.clone()], &original).is_err());
    let selected = HistorySelection::prepare_versionless(&[authority], &original).unwrap();
    assert_eq!(
        selected.decision(&key, 27, 9001).unwrap(),
        HistoryDecision::Drop
    );
    assert_eq!(
        selected.aggregate_indexes(),
        &[("entity".into(), "name".into())].into_iter().collect()
    );
    assert_eq!(
        selected
            .decision(&current.key, current.version, current.timestamp)
            .unwrap(),
        HistoryDecision::Keep
    );
}

#[test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned original authority clone in the tombstone-only membership comparison."
)]
fn native_history_tombstone_only_identity_obeys_original_membership() {
    let erased = record_key("ns", "db", "source", &Id::String("erased".into())).unwrap();
    let independent = record_key("ns", "db", "source", &Id::String("independent".into())).unwrap();
    let records = [erased.clone(), independent.clone()]
        .into_iter()
        .map(|key| RecallHistoryRecord {
            key,
            value: vec![],
            version: 29,
            timestamp: 9007,
            tombstone: true,
            current: true,
        })
        .collect();
    let original = snapshot(records);
    let authority = bind(&original, BTreeSet::new());
    for selected in [
        HistorySelection::prepare(&[authority.clone()], &original).unwrap(),
        HistorySelection::prepare_versionless(&[authority], &original).unwrap(),
    ] {
        assert_eq!(
            selected.decision(&erased, 29, 9007).unwrap(),
            HistoryDecision::Drop
        );
        assert_eq!(
            selected.decision(&independent, 29, 9007).unwrap(),
            HistoryDecision::Keep
        );
    }
}

#[test]
fn native_history_backup_source_tokens_bind_identity_scope_key_and_domain() {
    let source = "source:11b33d5b-83d2-44bb-a23b-57816b1b775e";
    let token = backup_origin_source_commitment(JOURNAL, "ns", "db", &KEY, "home", source).unwrap();
    assert_eq!(token.len(), 64);
    for (journal, ns, db, key, space, source) in [
        (
            "f7f0934e-fc5f-4c4c-bb2f-0674a22b7a7b",
            "ns",
            "db",
            KEY,
            "home",
            source,
        ),
        (JOURNAL, "other", "db", KEY, "home", source),
        (JOURNAL, "ns", "other", KEY, "home", source),
        (JOURNAL, "ns", "db", [92; 32], "home", source),
        (JOURNAL, "ns", "db", KEY, "business", source),
        (
            JOURNAL,
            "ns",
            "db",
            KEY,
            "home",
            "source:11b33d5b-83d2-44bb-a23b-57816b1b775f",
        ),
    ] {
        assert_ne!(
            backup_origin_source_commitment(journal, ns, db, &key, space, source).unwrap(),
            token
        );
    }
    // Even identical length-delimited payload bytes cannot cross the MAC domains.
    let mut metadata = (4_u64).to_be_bytes().to_vec();
    metadata.extend_from_slice(b"home");
    metadata.extend_from_slice(&(source.len() as u64).to_be_bytes());
    metadata.extend_from_slice(source.as_bytes());
    let scoped = backup_origin_authenticator(JOURNAL, "ns", "db", &KEY, &metadata).unwrap();
    assert_ne!(scoped, token);
    let whole = backup_origin_journal_authenticator(JOURNAL, &KEY, &metadata).unwrap();
    assert_ne!(whole, scoped);
    assert_ne!(whole, token);
    assert_ne!(
        whole,
        backup_origin_journal_authenticator(JOURNAL, &[92; 32], &metadata).unwrap()
    );
    assert_ne!(
        whole,
        backup_origin_journal_authenticator(
            "f7f0934e-fc5f-4c4c-bb2f-0674a22b7a7b",
            &KEY,
            &metadata
        )
        .unwrap()
    );
    metadata.pop();
    assert_ne!(
        whole,
        backup_origin_journal_authenticator(JOURNAL, &KEY, &metadata).unwrap()
    );
    assert!(backup_origin_journal_authenticator("not-a-journal", &KEY, &metadata).is_err());
    for invalid in [
        "entity:11b33d5b-83d2-44bb-a23b-57816b1b775e",
        "source:11B33D5B-83D2-44BB-A23B-57816B1B775E",
        "source:untyped",
    ] {
        assert!(
            backup_origin_source_commitment(JOURNAL, "ns", "db", &KEY, "home", invalid).is_err()
        );
    }
    assert!(backup_origin_source_commitment(JOURNAL, "ns", "db", &KEY, "", source).is_err());
}

#[test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the original owned authority clone for every unknown-scope-key refusal case."
)]
fn native_history_unknown_scope_key_never_inherits_metadata_retention() {
    for key in [
        b"/*ns\0*db\0*entity\0?unknown".to_vec(),
        b"/*ns\0*db\0!zzunknown\0".to_vec(),
        b"/*ns\0*db\0*entity\0!ixvalid\0trailing".to_vec(),
    ] {
        let original = snapshot(vec![RecallHistoryRecord {
            key,
            value: b"unclassified source payload".to_vec(),
            version: 1,
            timestamp: 100,
            tombstone: false,
            current: true,
        }]);
        let authority = bind(&original, BTreeSet::new());
        assert!(HistorySelection::prepare(&[authority.clone()], &original).is_err());
        assert!(HistorySelection::prepare_versionless(&[authority], &original).is_err());
    }
    let key = b"/*ns\0*db\0*entity\0!ixby_name\0".to_vec();
    let original = snapshot(vec![RecallHistoryRecord {
        key: key.clone(),
        value: b"native index definition retained byte-for-byte".to_vec(),
        version: 1,
        timestamp: 100,
        tombstone: false,
        current: true,
    }]);
    let selected =
        HistorySelection::prepare(&[bind(&original, BTreeSet::new())], &original).unwrap();
    assert_eq!(
        selected.decision(&key, 1, 100).unwrap(),
        HistoryDecision::Keep
    );
}

#[test]
fn native_history_backup_inspection_cannot_become_physical_authority() {
    let inspect = RecallHistoryAuthority::for_backup_inspection(
        "ns".into(),
        "db".into(),
        JOURNAL.into(),
        "home".into(),
        KEY,
        vec![],
    )
    .unwrap();
    let record = row("independent", 7, true);
    assert_eq!(
        inspect.version_commitment(&record).unwrap(),
        authority().version_commitment(&record).unwrap()
    );
    assert!(inspect.validate_bound().is_err());
    let snapshot = snapshot(vec![record]);
    assert!(inspect
        .clone()
        .bind_history(
            snapshot.fingerprint.clone(),
            BTreeSet::new(),
            BTreeSet::new()
        )
        .is_err());
    assert!(RecallHistoryAuthority::from_rpc_value(inspect.to_rpc_value()).is_err());
    let Value::Object(mut wire) = inspect.to_rpc_value() else {
        unreachable!()
    };
    wire.insert(
        "history_fingerprint".into(),
        Value::from(snapshot.fingerprint),
    );
    assert!(RecallHistoryAuthority::from_rpc_value(Value::Object(wire)).is_err());
    assert!(RecallHistoryAuthority::new(
        "ns".into(),
        "db".into(),
        JOURNAL.into(),
        "home".into(),
        KEY,
        vec![],
        BTreeSet::new()
    )
    .is_err());
    assert!(RecallHistoryAuthority::for_backup_inspection(
        "ns".into(),
        "db".into(),
        "invalid journal".into(),
        "home".into(),
        KEY,
        vec![]
    )
    .is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original datastore lifetime through native full-text inventory, source purge and all final history-selection assertions."
)]
async fn native_history_actual_full_text_state_is_dropped_and_requires_rebuild() {
    use surrealdb::dbs::Session;
    use surrealdb::kvs::recall_history::NativeHistoryKey;
    use surrealdb::kvs::Datastore;

    let ds = Datastore::new("memory").await.unwrap();
    for result in ds.execute(
        "DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase; \
         CREATE entity:erased SET space_id='home', source_id=source:erased, text='privateword'; \
         CREATE entity:independent SET space_id='home', source_id=source:independent, text='retainedword'; \
         DEFINE INDEX search ON entity FIELDS text SEARCH ANALYZER words BM25;",
        &Session::owner().with_ns("ns").with_db("db"),
        None,
    ).await.unwrap() {
        result.result.unwrap();
    }
    let before_purge = ds
        .recall_history_inventory(5_000, "ns", "db")
        .await
        .unwrap();
    let error = HistorySelection::prepare_versionless(
        &[bind(&before_purge, BTreeSet::new())],
        &before_purge,
    )
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("current source-bearing record survived the original purge"));
    // Physical selection follows the ordinary live purge. It cannot authorize
    // deleting a still-visible source-bearing document on its own.
    for result in ds
        .execute(
            "DELETE entity:erased;",
            &Session::owner().with_ns("ns").with_db("db"),
            None,
        )
        .await
        .unwrap()
    {
        result.result.unwrap();
    }
    let original = ds
        .recall_history_inventory(5_000, "ns", "db")
        .await
        .unwrap();
    // Golden bytes from the retained native Bs writer, independently of the
    // history decoder. Require the real full-text engine to have emitted it.
    let state_key = b"/*ns\0*db\0*entity\0!bssearch\0";
    let state = original
        .records
        .iter()
        .find(|row| row.key.as_slice() == state_key)
        .unwrap();
    assert!(state.current && !state.tombstone && !state.value.is_empty());
    assert_eq!(
        state.decoded_key().unwrap(),
        NativeHistoryKey::IndexInternal {
            namespace: "ns".into(),
            database: "db".into(),
            table: "entity".into(),
            index: "search".into(),
        }
    );
    let selected =
        HistorySelection::prepare_versionless(&[bind(&original, BTreeSet::new())], &original)
            .unwrap();
    assert_eq!(
        selected
            .decision(&state.key, state.version, state.timestamp)
            .unwrap(),
        HistoryDecision::Drop
    );
    assert!(selected
        .aggregate_indexes()
        .contains(&("entity".into(), "search".into())));
    let independent = record_key("ns", "db", "entity", &Id::String("independent".into())).unwrap();
    let independent = original
        .records
        .iter()
        .find(|row| row.key == independent)
        .unwrap();
    assert_eq!(
        selected
            .decision(&independent.key, independent.version, independent.timestamp)
            .unwrap(),
        HistoryDecision::Keep
    );
    let erased = record_key("ns", "db", "entity", &Id::String("erased".into())).unwrap();
    assert!(original.records.iter().all(|row| row.key != erased));
    assert!(selected.decision(&erased, 0, 0).is_err());
}
