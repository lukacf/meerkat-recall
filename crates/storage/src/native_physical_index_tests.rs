//! Workspace controls for the public native physical-history checkpoint.
//!
//! Fixtures use real SQL indexes and native transactions to represent persisted
//! deferred lag. No test calls the private reconstruction helper or implements
//! an index. The engine owners retain the independent physical-file oracle.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;
use surrealdb::dbs::Session;
use surrealdb::kvs::recall_history::{
    NativeHistoryKey, RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot,
};
use surrealdb::kvs::{Datastore, LockType, TransactionType};
use surrealdb::sql::{Thing, Value};

const NS: &str = "physical-index";
const DB: &str = "controls";
const JOURNAL: &str = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
const KEY: [u8; 32] = [37; 32];
const TIMEOUT_MS: u64 = 60_000;

#[derive(Clone, Copy)]
enum Engine {
    #[cfg(feature = "surrealkv")]
    Versioned,
    #[cfg(feature = "rocksdb")]
    Rocks,
}

impl Engine {
    const fn scheme(self) -> &'static str {
        match self {
            #[cfg(feature = "surrealkv")]
            Self::Versioned => "surrealkv+versioned",
            #[cfg(feature = "rocksdb")]
            Self::Rocks => "rocksdb",
        }
    }

    fn versioned(self) -> bool {
        self.scheme() == "surrealkv+versioned"
    }

    async fn open(self, root: &Path) -> Datastore {
        let datastore = Datastore::new(&format!(
            "{}://{}",
            self.scheme(),
            root.join("store").display()
        ))
        .await
        .expect("actual persistent native engine must be available");
        // Native shutdown removes this datastore's registered cluster node.
        // Match real startup on both initial open and reopen; shutdown errors
        // must remain failures instead of masking an uninitialized fixture.
        datastore
            .bootstrap()
            .await
            .expect("native fixture startup must register its node and restart deferred indexes");
        datastore
    }
}

async fn execute(ds: &Datastore, sql: &str) -> Vec<Value> {
    ds.execute(sql, &Session::owner().with_ns(NS).with_db(DB), None)
        .await
        .unwrap()
        .into_iter()
        .map(|response| response.result.unwrap())
        .collect()
}

fn value(sql: &str) -> Value {
    surrealdb::syn::value(sql).unwrap()
}

async fn inventory(ds: &Datastore) -> RecallHistorySnapshot {
    ds.recall_history_inventory(TIMEOUT_MS, NS, DB)
        .await
        .unwrap()
}

fn authority(snapshot: &RecallHistorySnapshot) -> RecallHistoryAuthority {
    // The original journal authenticates both source and derived record
    // identities. The latter are necessary when a versionless engine has
    // already deleted the corresponding canonical record.
    let membership = [
        Thing::from(("source", "erased")),
        Thing::from(("corpus", "a")),
        Thing::from(("scalar", "erased")),
    ]
    .iter()
    .map(|identity| {
        surrealdb::recall_reference_identity_token(JOURNAL, "home", &KEY, &[], identity).unwrap()
    })
    .collect();
    RecallHistoryAuthority::new(
        NS.into(),
        DB.into(),
        JOURNAL.into(),
        "home".into(),
        KEY,
        Vec::new(),
        membership,
    )
    .unwrap()
    .bind_history(
        snapshot.fingerprint.clone(),
        BTreeSet::new(),
        BTreeSet::new(),
    )
    .unwrap()
}

async fn checkpoint(ds: &Datastore, snapshot: &RecallHistorySnapshot) {
    ds.recall_physical_checkpoint_with_history(TIMEOUT_MS, vec![authority(snapshot)])
        .await
        .expect("source-authorized physical checkpoint including real index reconstruction");
}

fn record_key(table: &str, id: &str) -> Vec<u8> {
    surrealdb::kvs::recall_history::record_key(NS, DB, table, &id.into()).unwrap()
}

fn previous_queue_key(index: &str, id: &str) -> Vec<u8> {
    // Fixed fixture encoding from native key/index/ip.rs's own pinned codec
    // example: scope/name zero terminators, !ip, then native Id::String tag.
    // The production decoder independently validates the complete byte shape.
    format!("/*{NS}\0*{DB}\0*scalar\0+{index}\0!ip\0\0\0\x01{id}\0").into_bytes()
}

fn ordinary_index(row: &RecallHistoryRecord, table: &str, index: &str) -> bool {
    matches!(row.decoded_key().unwrap(), NativeHistoryKey::Index {
        table: found_table, index: found_index, ..
    } if found_table == table && found_index == index)
}

fn same_native_version(a: &RecallHistoryRecord, b: &RecallHistoryRecord) -> bool {
    // A newly reconciled scalar version changes the old row's CURRENT status,
    // but must not rewrite its native key/value/version/timestamp/tombstone.
    a.key == b.key
        && a.value == b.value
        && a.version == b.version
        && a.timestamp == b.timestamp
        && a.tombstone == b.tombstone
}

fn no_payload(snapshot: &RecallHistorySnapshot, markers: &[&str]) {
    for row in &snapshot.records {
        for marker in markers {
            for bytes in [&row.key, &row.value] {
                assert!(!bytes
                    .windows(marker.len())
                    .any(|part| part == marker.as_bytes()));
            }
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn put_record(ds: &Datastore, table: &str, id: &str, document: Value) {
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(
        record_key(table, id),
        RecallHistoryRecord::encode_value(&document).unwrap(),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn wait_for(ds: &Datastore, query: &str, expected: Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if execute(ds, query).await == vec![expected.clone()] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("deferred fixture must actually finish its initial/queued build");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn mixed_search_vector_history(engine: Engine) {
    let root = tempfile::tempdir().unwrap();
    let ds = engine.open(root.path()).await;
    execute(
        &ds,
        "DEFINE TABLE corpus SCHEMALESS; DEFINE TABLE unrelated SCHEMALESS;
        DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;",
    )
    .await;
    if engine.versioned() {
        // This native version supports explicit historical INSERT, whose
        // versioned put writes the complete supplied record at that timestamp.
        // UPSERT has no VERSION clause, and an id-only duplicate-key INSERT
        // would not carry the original field values into that historical write.
        execute(&ds, "
            CREATE corpus:b SET space_id='home',source_id=source:surviving,text='keep old',point=[10,10] VERSION d'2020-01-01T00:00:00Z';
            INSERT INTO corpus {id:corpus:b,space_id:'home',source_id:source:erased,text:'mixedprivate721953',point:[2,2]} VERSION d'2021-01-01T00:00:00Z';
            INSERT INTO corpus {id:corpus:b,space_id:'home',source_id:source:surviving,text:'keep current',point:[20,20]} VERSION d'2022-01-01T00:00:00Z';
            CREATE unrelated:c SET space_id='elsewhere',source_id=source:other,text='unrelated old' VERSION d'2020-01-01T00:00:00Z';
            INSERT INTO unrelated {id:unrelated:c,space_id:'elsewhere',source_id:source:other,text:'unrelated current'} VERSION d'2022-01-01T00:00:00Z';
        ").await;
    } else {
        execute(&ds, "
            CREATE corpus:b SET space_id='home',source_id=source:surviving,text='keep current',point=[20,20];
            CREATE unrelated:c SET space_id='elsewhere',source_id=source:other,text='unrelated current';
        ").await;
    }
    execute(&ds, "
        CREATE corpus:a SET space_id='home',source_id=source:erased,text='aggregateprivate916272',point=[1,1];
        DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25 HIGHLIGHTS;
        DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F64;
        DEFINE INDEX exact_points_idx ON corpus FIELDS point MTREE DIMENSION 2 DIST EUCLIDEAN TYPE F64;
        DEFINE INDEX size_idx ON corpus COUNT;
    ").await;
    // Populate the original live HNSW cache with the soon-to-be-erased point.
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [1,1]"
        )
        .await,
        vec![value("[corpus:a]")]
    );
    execute(&ds, "DELETE corpus:a").await;
    let before = inventory(&ds).await;
    let retained: Vec<_> = before
        .records
        .iter()
        .filter(|row| {
            row.key == record_key("unrelated", "c")
                || (row.key == record_key("corpus", "b")
                    && !row
                        .value
                        .windows(18)
                        .any(|bytes| bytes == b"mixedprivate721953"))
        })
        .cloned()
        .collect();
    assert_eq!(retained.len(), if engine.versioned() { 4 } else { 2 });
    assert!(before.records.iter().any(|row| matches!(
        row.decoded_key().unwrap(),
        NativeHistoryKey::IndexInternal { .. }
    )));
    checkpoint(&ds, &before).await;
    let after = inventory(&ds).await;
    for row in &retained {
        assert!(
            after.records.contains(row),
            "independent native record version changed"
        );
    }
    no_payload(&after, &["aggregateprivate916272", "mixedprivate721953"]);
    if engine.versioned() {
        assert_eq!(
            execute(
                &ds,
                "SELECT text FROM corpus:b VERSION d'2020-01-01T00:00:00Z';
            SELECT text FROM corpus:b VERSION d'2021-01-01T00:00:00Z';
            SELECT text FROM corpus:b VERSION d'2022-01-01T00:00:00Z';"
            )
            .await,
            vec![
                value("[{text:'keep old'}]"),
                value("[]"),
                value("[{text:'keep current'}]")
            ]
        );
    }
    let queries = "SELECT VALUE id FROM corpus WHERE text @@ 'keep';
        SELECT VALUE id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [1,1];
        SELECT VALUE id FROM corpus WITH INDEX exact_points_idx WHERE point <|1|> [1,1];
        SELECT count() AS count FROM corpus GROUP ALL;
        SELECT VALUE id FROM corpus WHERE text @@ 'aggregateprivate916272';";
    let expected = vec![
        value("[corpus:b]"),
        value("[corpus:b]"),
        value("[corpus:b]"),
        value("[{count:1}]"),
        value("[]"),
    ];
    assert_eq!(execute(&ds, queries).await, expected);
    if engine.versioned() {
        assert_eq!(
            execute(
                &ds,
                "SELECT VALUE id FROM corpus WHERE text @@ 'keep' VERSION d'2020-01-01T00:00:00Z'"
            )
            .await,
            vec![value("[corpus:b]")]
        );
    }
    ds.shutdown().await.unwrap();
    drop(ds);
    let reopened = engine.open(root.path()).await;
    assert_eq!(execute(&reopened, queries).await, expected);
    execute(&reopened, "CREATE corpus:d SET space_id='home',source_id=source:surviving,text='future retained',point=[3,3]").await;
    assert_eq!(
        execute(
            &reopened,
            "SELECT VALUE id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [1,1]"
        )
        .await,
        vec![value("[corpus:d]")]
    );
    no_payload(
        &inventory(&reopened).await,
        &["aggregateprivate916272", "mixedprivate721953"],
    );
    reopened.shutdown().await.unwrap();
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn scalar_lag_and_kind_transition(engine: Engine) {
    let root = tempfile::tempdir().unwrap();
    let ds = engine.open(root.path()).await;
    // Commit the original scalar definitions before producing their tuples.
    // A populated-table builder commits in separate native transactions, so
    // its first tuples can otherwise predate the definition's own commit.
    execute(
        &ds,
        "
        DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;
        DEFINE INDEX name_idx ON scalar FIELDS name DEFER;
        DEFINE INDEX unique_name_idx ON scalar FIELDS name UNIQUE;
        DEFINE INDEX transition_idx ON transition FIELDS text;
    ",
    )
    .await;
    execute(
        &ds,
        "
        CREATE scalar:b SET space_id='home',source_id=source:surviving,name='old name';
        CREATE scalar:erased SET space_id='home',source_id=source:erased,name='scalarprivate871641';
        CREATE scalar:mixed SET space_id='home',source_id=source:erased,name='delayedprivate416982';
        CREATE transition:b SET space_id='home',source_id=source:surviving,text='legacy retained';
    ",
    )
    .await;
    wait_for(
        &ds,
        "SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='old name'",
        value("[scalar:b]"),
    )
    .await;
    let original = inventory(&ds).await;
    if engine.versioned() {
        for (table, index) in [
            ("scalar", "name_idx"),
            ("scalar", "unique_name_idx"),
            ("transition", "transition_idx"),
        ] {
            // Golden Ix bytes from the pinned native key/table/ix.rs codec.
            // Inspect the actual snapshot instead of assuming SQL statement
            // order alone establishes the required native version ordering.
            let definition_key = format!("/*{NS}\0*{DB}\0*{table}\0!ix{index}\0").into_bytes();
            let definitions: Vec<_> = original
                .records
                .iter()
                .filter(|row| row.key == definition_key && !row.tombstone)
                .collect();
            assert_eq!(definitions.len(), 1, "initial definition {table}/{index}");
            assert!(definitions[0].current);
            let tuples: Vec<_> = original
                .records
                .iter()
                .filter(|row| !row.tombstone && ordinary_index(row, table, index))
                .collect();
            assert!(!tuples.is_empty(), "real initial tuples {table}/{index}");
            for tuple in tuples {
                assert!(
                    definitions[0].version <= tuple.version,
                    "initial {table}/{index} tuple version {} predates definition version {}",
                    tuple.version,
                    definitions[0].version
                );
            }
        }
    }
    let stale = original.records.iter().find(|row| {
        row.current && !row.tombstone && ordinary_index(row, "scalar", "name_idx") &&
            matches!(row.decoded_key().unwrap(), NativeHistoryKey::Index { fields, .. } if fields == surrealdb::sql::Array::from(vec![Value::from("old name")]))
    }).unwrap().clone();
    let delayed = original.records.iter().find(|row| {
        row.current && !row.tombstone && ordinary_index(row, "scalar", "name_idx") &&
            matches!(row.decoded_key().unwrap(), NativeHistoryKey::Index { fields, .. } if fields == surrealdb::sql::Array::from(vec![Value::from("delayedprivate416982")]))
    }).unwrap().clone();
    // Native UNIQUE and DEFER are mutually exclusive. Exercise both supported
    // definitions on the same retained records and inject the same persisted
    // lag into both, retaining the unique-value handoff to C as well as the
    // real deferred-schema/daemon assertions below.
    let unique_stale = original.records.iter().find(|row| {
        row.current && !row.tombstone && ordinary_index(row, "scalar", "unique_name_idx") &&
            matches!(row.decoded_key().unwrap(), NativeHistoryKey::Index { fields, .. } if fields == surrealdb::sql::Array::from(vec![Value::from("old name")]))
    }).unwrap().clone();
    let unique_delayed = original.records.iter().find(|row| {
        row.current && !row.tombstone && ordinary_index(row, "scalar", "unique_name_idx") &&
            matches!(row.decoded_key().unwrap(), NativeHistoryKey::Index { fields, .. } if fields == surrealdb::sql::Array::from(vec![Value::from("delayedprivate416982")]))
    }).unwrap().clone();
    execute(&ds, "UPDATE scalar:b SET name='new name'; DELETE scalar:erased;
        UPDATE scalar:mixed SET source_id=source:surviving,name='independent replacement';
        DEFINE INDEX OVERWRITE transition_idx ON transition FIELDS text SEARCH ANALYZER words BM25;").await;
    wait_for(
        &ds,
        "SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='new name'",
        value("[scalar:b]"),
    )
    .await;
    wait_for(
        &ds,
        "SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='independent replacement'",
        value("[scalar:mixed]"),
    )
    .await;
    // Reproduce a persisted deferred rename: B's current document is renamed,
    // its native old entry remains, and C has claimed that now-free old value.
    // These writes intentionally bypass ordinary indexing only to form the
    // pre-existing lag state; the action under test is the public checkpoint.
    put_record(
        &ds,
        "scalar",
        "c",
        value("{id:scalar:c,space_id:'home',source_id:source:surviving,name:'old name'}"),
    )
    .await;
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(stale.key.clone(), stale.value.clone(), None)
        .await
        .unwrap();
    // IndexBuilder writes captured Appending fields in a later transaction,
    // without the original document version. Reproduce that exact byte shape:
    // A's original scalar entry is committed after B became the current record.
    // Choosing the latest record <= this index timestamp would wrongly retain A.
    tx.set(delayed.key.clone(), delayed.value.clone(), None)
        .await
        .unwrap();
    tx.set(unique_stale.key.clone(), unique_stale.value.clone(), None)
        .await
        .unwrap();
    tx.set(
        unique_delayed.key.clone(),
        unique_delayed.value.clone(),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    // A soft-deleted old queue entry is invisible to the daemon, yet its native
    // historical payload must be removed. The selector treats this as opaque
    // builder state and never trusts it as record ownership evidence.
    let queue_key = previous_queue_key("name_idx", "erased");
    // An orphan marker also makes the unique companion a reconstruction
    // obligation. It is never consumed by a daemon; the public checkpoint
    // must drop its complete history and reconcile current unique entries.
    let unique_queue_key = previous_queue_key("unique_name_idx", "erased");
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(queue_key.clone(), b"queueprivate194861".to_vec(), None)
        .await
        .unwrap();
    tx.set(
        unique_queue_key.clone(),
        b"queueprivate194861".to_vec(),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.del(queue_key.clone()).await.unwrap();
    tx.del(unique_queue_key.clone()).await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let before = inventory(&ds).await;
    let ordinary: Vec<_> = before
        .records
        .iter()
        .filter(|row| {
            ordinary_index(row, "transition", "transition_idx")
                || (ordinary_index(row, "scalar", "name_idx") && row.key == stale.key)
                || (ordinary_index(row, "scalar", "unique_name_idx") && row.key == unique_stale.key)
        })
        .cloned()
        .collect();
    if engine.versioned() {
        assert!(ordinary
            .iter()
            .any(|row| ordinary_index(row, "transition", "transition_idx")));
        assert!(before
            .records
            .iter()
            .any(|row| row.key == queue_key && !row.tombstone));
        assert!(before
            .records
            .iter()
            .any(|row| row.key == unique_queue_key && !row.tombstone));
    }
    let retained_mixed = before
        .records
        .iter()
        .find(|row| row.current && row.key == record_key("scalar", "mixed"))
        .unwrap()
        .clone();
    let delayed_current = before
        .records
        .iter()
        .find(|row| row.current && row.key == delayed.key)
        .unwrap();
    if engine.versioned() {
        assert!(
            delayed_current.version > retained_mixed.version,
            "regression requires a real deferred index timestamp after the independent record"
        );
        let unique_delayed_current = before
            .records
            .iter()
            .find(|row| row.current && row.key == unique_delayed.key)
            .unwrap();
        assert!(
            unique_delayed_current.version > retained_mixed.version,
            "the unique companion must reproduce the same delayed contribution"
        );
    }
    checkpoint(&ds, &before).await;
    let after = inventory(&ds).await;
    if engine.versioned() {
        for row in &ordinary {
            assert!(
                after
                    .records
                    .iter()
                    .any(|new| same_native_version(row, new)),
                "authorized ordinary index history was lost"
            );
        }
    }
    assert!(!after.records.iter().any(|row| row.key == queue_key));
    assert!(!after.records.iter().any(|row| row.key == unique_queue_key));
    assert!(after.records.contains(&retained_mixed));
    assert!(
        !after.records.iter().any(|row| row.key == delayed.key),
        "all delayed A field key versions must be absent, including current tombstones"
    );
    assert!(
        !after
            .records
            .iter()
            .any(|row| row.key == unique_delayed.key),
        "all delayed A unique-key versions must also be absent"
    );
    no_payload(
        &after,
        &[
            "scalarprivate871641",
            "queueprivate194861",
            "delayedprivate416982",
        ],
    );
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='old name';
        SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='new name';
        SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='scalarprivate871641';
        SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='delayedprivate416982';
        SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='independent replacement';
        SELECT VALUE id FROM transition WHERE text @@ 'retained';"
        )
        .await,
        vec![
            value("[scalar:c]"),
            value("[scalar:b]"),
            value("[]"),
            value("[]"),
            value("[scalar:mixed]"),
            value("[transition:b]")
        ]
    );
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM scalar WITH INDEX unique_name_idx WHERE name='old name';
        SELECT VALUE id FROM scalar WITH INDEX unique_name_idx WHERE name='new name';
        SELECT VALUE id FROM scalar WITH INDEX unique_name_idx WHERE name='scalarprivate871641';
        SELECT VALUE id FROM scalar WITH INDEX unique_name_idx WHERE name='delayedprivate416982';
        SELECT VALUE id FROM scalar WITH INDEX unique_name_idx WHERE name='independent replacement';"
        )
        .await,
        vec![
            value("[scalar:c]"),
            value("[scalar:b]"),
            value("[]"),
            value("[]"),
            value("[scalar:mixed]")
        ]
    );
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    assert!(
        tx.get_tb_index(NS, DB, "scalar", "name_idx")
            .await
            .unwrap()
            .defer
    );
    tx.cancel().await.unwrap();
    drop(tx);
    execute(
        &ds,
        "CREATE scalar:future SET space_id='home',source_id=source:surviving,name='after restart'",
    )
    .await;
    wait_for(
        &ds,
        "SELECT VALUE id FROM scalar WITH INDEX name_idx WHERE name='after restart'",
        value("[scalar:future]"),
    )
    .await;
    // Explicitly retire the daemon before closing this fixture, while retaining
    // the previous assertions about the actual deferred schema and new writes.
    execute(&ds, "REMOVE INDEX name_idx ON scalar").await;
    let final_snapshot = inventory(&ds).await;
    checkpoint(&ds, &final_snapshot).await;
    ds.shutdown().await.unwrap();
}

// These controls enter through the production public physical checkpoint, so
// the workspace lane executes the exact guard regressions even though it does
// not build the private tests of the vendored core dependency.
#[cfg(feature = "surrealkv")]
#[expect(
    clippy::mutable_key_type,
    reason = "The oracle compares native keys without mutating them; regex cache internals do not change key identity."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn scalar_mapping_preflight_controls() {
    const MAPPED: &str = "[{a:[{name:'one'},{name:'two'}]},{a:[{name:'discarded'}]}]";
    const DISCARDED: &str =
        "[{a:[{name:'one'},{name:'two'}]},{a:[source:remote,<future> { RETURN 'unused'; }]}]";
    const REMOTE: &str = "[{a:[{name:'safe'},source:remote]}]";
    const DYNAMIC: &str = "[{a:[{name:'safe'},<future> { RETURN {name:'dynamic'}; }]}]";
    const LOCAL_IDS: &str = "[{id:{part:'local'}}]";
    const RECORD_IDS: &str = "[{id:source:{part:'local'}}]";
    for (path, initial_items, stored_items, expected_fields, rejection) in [
        (
            "items.a[0].name",
            MAPPED,
            DISCARDED,
            vec!["one", "two"],
            None,
        ),
        (
            "items.a.*[0].name",
            MAPPED,
            DISCARDED,
            vec!["one", "two"],
            None,
        ),
        (
            "items.a[0].name",
            MAPPED,
            REMOTE,
            vec!["one", "two"],
            Some("record dereference"),
        ),
        (
            "items.a.*[0].name",
            MAPPED,
            REMOTE,
            vec!["one", "two"],
            Some("record dereference"),
        ),
        (
            "items.a[0].name",
            MAPPED,
            DYNAMIC,
            vec!["one", "two"],
            Some("dynamic value"),
        ),
        (
            "items.a.*[0].name",
            MAPPED,
            DYNAMIC,
            vec!["one", "two"],
            Some("dynamic value"),
        ),
        (
            "items.id.part",
            LOCAL_IDS,
            RECORD_IDS,
            vec!["local"],
            Some("record dereference"),
        ),
        (
            "items[0].id.part",
            LOCAL_IDS,
            RECORD_IDS,
            vec!["local"],
            None,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let ds = Engine::Versioned.open(root.path()).await;
        // An independent named space also tests preservation of unrelated
        // corpus. Its Future bypasses this journal's source-reference scanner,
        // so the expected negative must reach the scalar projector itself.
        // Finish the native definition before creating indexed records. The
        // populated-table builder can otherwise commit tuples before its
        // caller commits the definition used to inspect their fields.
        execute(
            &ds,
            &format!("DEFINE INDEX navigation_idx ON navigation FIELDS {path};"),
        )
        .await;
        execute(
            &ds,
            &format!("CREATE navigation:b SET space_id='elsewhere',items={initial_items};"),
        )
        .await;
        let original = inventory(&ds).await;
        let entries: Vec<_> = original
            .records
            .iter()
            .filter(|row| {
                row.current && !row.tombstone && ordinary_index(row, "navigation", "navigation_idx")
            })
            .cloned()
            .collect();
        let actual_fields: BTreeSet<_> = entries
            .iter()
            .map(|row| match row.decoded_key().unwrap() {
                NativeHistoryKey::Index { fields, .. } => fields,
                _ => panic!("native ordinary index fixture"),
            })
            .collect();
        assert_eq!(
            actual_fields,
            expected_fields
                .iter()
                .map(|field| surrealdb::sql::Array::from(vec![Value::from(*field)]))
                .collect(),
            "actual native mapping must select the expected complete tuple set: {path}"
        );
        assert_eq!(entries.len(), expected_fields.len());
        // Independently inspect the actual native version precondition before
        // replaying any index bytes or introducing the adversarial document.
        let definition_key =
            format!("/*{NS}\0*{DB}\0*navigation\0!ixnavigation_idx\0").into_bytes();
        let definitions: Vec<_> = original
            .records
            .iter()
            .filter(|row| row.key == definition_key && !row.tombstone)
            .collect();
        assert_eq!(definitions.len(), 1, "initial mapping definition: {path}");
        assert!(definitions[0].current, "current mapping definition: {path}");
        for tuple in original
            .records
            .iter()
            .filter(|row| !row.tombstone && ordinary_index(row, "navigation", "navigation_idx"))
        {
            assert!(
                definitions[0].version <= tuple.version,
                "initial mapping {path} tuple version {} predates definition version {}",
                tuple.version,
                definitions[0].version
            );
        }
        // Store literal Future/Thing shapes without asking SQL to evaluate them.
        // Replaying bytes actually written by IndexOperation preserves its
        // native owner codec; no fixture index implementation is substituted.
        put_record(
            &ds,
            "navigation",
            "b",
            value(&format!(
                "{{id:navigation:b,space_id:'elsewhere',items:{stored_items}}}"
            )),
        )
        .await;
        let tx = ds
            .transaction(TransactionType::Write, LockType::Optimistic)
            .await
            .unwrap();
        for row in &entries {
            tx.set(row.key.clone(), row.value.clone(), None)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        drop(tx);
        let before = inventory(&ds).await;
        let retained_document = before
            .records
            .iter()
            .find(|row| row.current && row.key == record_key("navigation", "b"))
            .unwrap()
            .clone();
        let retained_entries: Vec<_> = before
            .records
            .iter()
            .filter(|row| ordinary_index(row, "navigation", "navigation_idx"))
            .cloned()
            .collect();
        assert!(
            retained_entries
                .iter()
                .any(|row| { row.current && row.version > retained_document.version }),
            "projection must inspect the rewritten document before the replayed index: {path}"
        );
        let result = ds
            .recall_physical_checkpoint_with_history(TIMEOUT_MS, vec![authority(&before)])
            .await;
        if let Some(rejection) = rejection {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("Recall historical scalar projection") && error.contains(rejection),
                "must refuse in the preflight before native dereference/evaluation: {path}: {error}"
            );
            assert!(ds
                .transaction(TransactionType::Read, LockType::Optimistic)
                .await
                .is_err());
        } else {
            result.unwrap();
            let after = inventory(&ds).await;
            assert!(after.records.contains(&retained_document));
            for row in &retained_entries {
                assert!(
                    after
                        .records
                        .iter()
                        .any(|new| same_native_version(row, new)),
                    "independent native scalar evidence changed: {path}"
                );
            }
            ds.shutdown().await.unwrap();
            drop(ds);
            let reopened = Engine::Versioned.open(root.path()).await;
            let reopened_history = inventory(&reopened).await;
            assert!(reopened_history.records.contains(&retained_document));
            for row in &retained_entries {
                assert!(
                    reopened_history
                        .records
                        .iter()
                        .any(|new| same_native_version(row, new)),
                    "independent native scalar evidence lost on reopen: {path}"
                );
            }
            reopened.shutdown().await.unwrap();
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn original_analyzer_policy_refuses(engine: Engine) {
    use surrealdb::dbs::capabilities::{Capabilities, FuncTarget, Targets};
    let root = tempfile::tempdir().unwrap();
    let ds = engine.open(root.path()).await;
    execute(
        &ds,
        "
        DEFINE FUNCTION fn::retained_text($text: string) { RETURN $text; };
        DEFINE ANALYZER guarded FUNCTION fn::retained_text TOKENIZERS blank,class FILTERS lowercase;
        CREATE corpus:b SET space_id='home',source_id=source:surviving,text='independent retained';
        DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER guarded BM25;
    ",
    )
    .await;
    let capabilities = Capabilities::default().without_functions(Targets::Some(
        std::iter::once(FuncTarget("fn".into(), Some("retained_text".into()))).collect(),
    ));
    let ds = ds.with_capabilities(capabilities);
    let snapshot = inventory(&ds).await;
    let error = ds
        .recall_physical_checkpoint_with_history(TIMEOUT_MS, vec![authority(&snapshot)])
        .await
        .unwrap_err();
    assert!(
        matches!(error, surrealdb::err::Error::FunctionNotAllowed(ref function) if function == "fn::retained_text"),
        "the original analyzer policy must reach staged native computation: {error}"
    );
    assert!(ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .is_err());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn duplicate_unique_refuses(engine: Engine) {
    original_analyzer_policy_refuses(engine).await;
    let root = tempfile::tempdir().unwrap();
    let ds = engine.open(root.path()).await;
    // Establish the retained definition before producing the tuple. Building
    // an index over existing data uses separate native transactions, so its
    // first tuple may predate the schema commit and correctly lack historical
    // field evidence. This fixture must reach reconstruction's uniqueness check.
    execute(
        &ds,
        "DEFINE INDEX name_idx ON scalar FIELDS name UNIQUE;
        CREATE scalar:b SET space_id='home',source_id=source:surviving,name='duplicate';",
    )
    .await;
    put_record(
        &ds,
        "scalar",
        "c",
        value("{id:scalar:c,space_id:'home',source_id:source:surviving,name:'duplicate'}"),
    )
    .await;
    // An orphan builder marker makes the scalar definition a reconstruction
    // obligation on both engines. No queue worker can consume this fixture.
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(
        previous_queue_key("name_idx", "b"),
        b"old-queue".to_vec(),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let before = inventory(&ds).await;
    let error = ds
        .recall_physical_checkpoint_with_history(TIMEOUT_MS, vec![authority(&before)])
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Database index `name_idx` already contains"),
        "must reach the real uniqueness check: {error}"
    );
    // The public owner must never acknowledge or reopen ordinary query admission
    // after a failed staged reconstruction. Recovery belongs to the native owner.
    assert!(ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .is_err());
    assert!(ds
        .recall_physical_checkpoint_with_history(TIMEOUT_MS, vec![authority(&before)])
        .await
        .is_err());
}

#[cfg(feature = "surrealkv")]
#[tokio::test]
async fn surrealkv_public_physical_indexes_preserve_mixed_history_and_reopen() {
    mixed_search_vector_history(Engine::Versioned).await;
}
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocks_public_physical_indexes_erase_cached_source_and_reopen() {
    mixed_search_vector_history(Engine::Rocks).await;
}
#[cfg(feature = "surrealkv")]
#[tokio::test]
async fn surrealkv_public_physical_scalar_lag_preserves_kind_transition_history() {
    scalar_lag_and_kind_transition(Engine::Versioned).await;
    scalar_mapping_preflight_controls().await;
}
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocks_public_physical_scalar_deferred_rename_and_deletion() {
    scalar_lag_and_kind_transition(Engine::Rocks).await;
}
#[cfg(feature = "surrealkv")]
#[tokio::test]
async fn surrealkv_public_physical_duplicate_unique_refuses_completion() {
    duplicate_unique_refuses(Engine::Versioned).await;
}
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocks_public_physical_duplicate_unique_refuses_completion() {
    duplicate_unique_refuses(Engine::Rocks).await;
}

#[test]
fn native_deferred_marker_requires_exact_codec_and_reconstruction() {
    use surrealdb::kvs::recall_history::{HistoryDecision, HistorySelection};

    // Golden bytes from the native All constructor: IndexBuilder writes false
    // at this bare key before its tagged deferred state. No payload decoding
    // or broad unknown-key fallback may turn it into ordinary retained data.
    let key = format!("/*{NS}\0*{DB}\0*scalar\0+name_idx\0").into_bytes();
    let row = RecallHistoryRecord {
        key: key.clone(),
        value: vec![0],
        version: 1,
        timestamp: 100,
        tombstone: false,
        current: true,
    };
    assert_eq!(
        row.decoded_key().unwrap(),
        NativeHistoryKey::IndexInternal {
            namespace: NS.into(),
            database: DB.into(),
            table: "scalar".into(),
            index: "name_idx".into(),
        }
    );
    let snapshot = RecallHistorySnapshot::new(NS.into(), DB.into(), vec![row.clone()]).unwrap();
    let selection = HistorySelection::prepare(&[authority(&snapshot)], &snapshot).unwrap();
    assert_eq!(
        selection.decision(&key, 1, 100).unwrap(),
        HistoryDecision::Drop
    );
    assert_eq!(
        selection.aggregate_indexes(),
        &std::iter::once(("scalar".into(), "name_idx".into())).collect()
    );
    for suffix in [b"\0".as_slice(), b"!", b"!bu", b"!zzopaque", b"\xff"] {
        let mut malformed = row.clone();
        malformed.key.extend_from_slice(suffix);
        assert!(malformed.decoded_key().is_err());
    }
    let mut truncated = row;
    truncated.key.pop();
    assert!(truncated.decoded_key().is_err());
}
