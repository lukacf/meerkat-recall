//! Actual core transaction and query controls, included in the storage workspace tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::{sync::Arc, time::Duration};
use surrealdb::{
    dbs::Session,
    kvs::{Datastore, LockType::Optimistic, TransactionType::Read},
    sql::Value,
};

async fn execute(ds: &Datastore, sql: &str) -> Vec<Value> {
    ds.execute(
        sql,
        &Session::owner().with_ns("physical").with_db("test"),
        None,
    )
    .await
    .unwrap()
    .into_iter()
    .map(|response| response.result.unwrap())
    .collect()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn cancelled_transaction_retains_core_physical_admission_until_actual_drop() {
    let ds = Arc::new(Datastore::new("memory").await.unwrap());
    let tx = ds.transaction(Read, Optimistic).await.unwrap();
    tx.cancel().await.unwrap();
    let owner = ds.clone();
    let mut checkpoint = tokio::spawn(async move { owner.recall_physical_checkpoint(5_000).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut checkpoint)
            .await
            .is_err()
    );
    drop(tx);
    tokio::time::timeout(Duration::from_secs(5), checkpoint)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tx = ds.transaction(Read, Optimistic).await.unwrap();
    tx.cancel().await.unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn escaped_native_transactor_retains_core_physical_admission() {
    let ds = Arc::new(Datastore::new("memory").await.unwrap());
    let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
    tx.cancel().await.unwrap();
    let owner = ds.clone();
    let mut checkpoint = tokio::spawn(async move { owner.recall_physical_checkpoint(5_000).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut checkpoint)
            .await
            .is_err()
    );
    drop(tx);
    tokio::time::timeout(Duration::from_secs(5), checkpoint)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn cancelled_checkpoint_waiter_does_not_cancel_owned_timeout_or_reopen_queries() {
    let ds = Arc::new(Datastore::new("memory").await.unwrap());
    let tx = ds.transaction(Read, Optimistic).await.unwrap();
    let owner = ds.clone();
    let mut requester = tokio::spawn(async move { owner.recall_physical_checkpoint(100).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut requester)
            .await
            .is_err()
    );
    requester.abort();
    assert!(requester.await.unwrap_err().is_cancelled());
    // The independent task must still reach its original deadline with tx alive.
    tokio::time::sleep(Duration::from_millis(150)).await;
    tx.cancel().await.unwrap();
    drop(tx);
    assert!(ds.transaction(Read, Optimistic).await.is_err());
    assert!(ds.recall_physical_checkpoint(1_000).await.is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_checkpoint_preserves_cached_hnsw_queries_and_subsequent_writes() {
    let ds = Datastore::new("memory").await.unwrap();
    execute(&ds, "CREATE points:private SET vector=[1,0]; CREATE points:independent SET vector=[9,0]; DEFINE INDEX knn ON points FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN;").await;
    let nearest = "SELECT VALUE id FROM points WHERE vector <|1,100|> [1,0]";
    assert_eq!(
        execute(&ds, nearest).await,
        vec![surrealdb::syn::value("[points:private]").unwrap()]
    );
    execute(&ds, "DELETE points:private;").await;
    ds.recall_physical_checkpoint(5_000).await.unwrap();
    assert_eq!(
        execute(&ds, nearest).await,
        vec![surrealdb::syn::value("[points:independent]").unwrap()]
    );
    execute(&ds, "CREATE points:successor SET vector=[2,0];").await;
    assert_eq!(
        execute(&ds, nearest).await,
        vec![surrealdb::syn::value("[points:successor]").unwrap()]
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_checkpoint_restarts_deferred_worker_and_indexes_new_writes() {
    let ds = Datastore::new("memory").await.unwrap();
    execute(&ds, "DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase; DEFINE INDEX search ON entries FIELDS text SEARCH ANALYZER words BM25 DEFER; CREATE entries:before SET text='before';").await;
    ds.recall_physical_checkpoint(5_000).await.unwrap();
    execute(&ds, "CREATE entries:after SET text='after';").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let rows = execute(&ds, "SELECT VALUE id FROM entries WHERE text @0@ 'after';").await;
            if rows == vec![surrealdb::syn::value("[entries:after]").unwrap()] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // A second stop/drain/restart must also work, including an idle running daemon.
    ds.recall_physical_checkpoint(5_000).await.unwrap();
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_checkpoint_drains_removed_concurrent_index_worker() {
    let ds = Datastore::new("memory").await.unwrap();
    execute(&ds, "CREATE entries:one SET value='independent'; DEFINE INDEX old ON entries FIELDS value CONCURRENTLY; REMOVE INDEX old ON entries;").await;
    ds.recall_physical_checkpoint(5_000).await.unwrap();
    execute(&ds, "DEFINE INDEX old ON entries FIELDS value;").await;
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM entries WHERE value='independent';"
        )
        .await,
        vec![surrealdb::syn::value("[entries:one]").unwrap()]
    );
    ds.recall_physical_checkpoint(5_000).await.unwrap();
}

fn history_authority(
    snapshot: &surrealdb::kvs::RecallHistorySnapshot,
) -> surrealdb::kvs::RecallHistoryAuthority {
    let journal = "00000000-0000-0000-0000-000000000017";
    let key = [17_u8; 32];
    let identity = surrealdb::sql::Thing::from(("points", "private"));
    let membership =
        surrealdb::recall_reference_identity_token(journal, "s", &key, &[], &identity).unwrap();
    surrealdb::kvs::RecallHistoryAuthority::new(
        snapshot.namespace.clone(),
        snapshot.database.clone(),
        journal.into(),
        "s".into(),
        key,
        Vec::new(),
        std::collections::BTreeSet::from([membership]),
    )
    .unwrap()
    .bind_history(
        snapshot.fingerprint.clone(),
        std::collections::BTreeSet::default(),
        std::collections::BTreeSet::default(),
    )
    .unwrap()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn memory_history_inventory_reads_actual_native_versions_after_replace_and_delete() {
    let ds = Datastore::new("memory").await.unwrap();
    execute(&ds, "CREATE points:private SET space_id='s', text='original'; UPDATE points:private SET text='replacement'; CREATE points:gone SET space_id='s', text='deleted'; DELETE points:gone;").await;
    let snapshot = ds
        .recall_history_inventory(5_000, "physical", "test")
        .await
        .unwrap();
    snapshot.verify().unwrap();
    let private = surrealdb::kvs::recall_history::record_key(
        "physical",
        "test",
        "points",
        &surrealdb::sql::Id::String("private".into()),
    )
    .unwrap();
    let gone = surrealdb::kvs::recall_history::record_key(
        "physical",
        "test",
        "points",
        &surrealdb::sql::Id::String("gone".into()),
    )
    .unwrap();
    let rows = snapshot
        .records
        .iter()
        .filter(|row| row.key == private)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].current && !rows[0].tombstone);
    assert_eq!((rows[0].version, rows[0].timestamp), (0, 0));
    let Value::Object(value) = rows[0].decoded_value().unwrap() else {
        panic!("native document");
    };
    assert_eq!(value.get("text"), Some(&Value::from("replacement")));
    assert!(!snapshot.records.iter().any(|row| row.key == gone));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn memory_history_inventory_includes_unknown_high_suffix_and_refuses_erasure() {
    use surrealdb::kvs::TransactionType::Write;
    let ds = Datastore::new("memory").await.unwrap();
    // Exact pinned database::All codec, followed by an unknown in-scope suffix
    // beyond the former appended 0xff bound. No private key module is exposed.
    let key = b"/*physical\0*test\0\xff\x01".to_vec();
    let value = b"unknown native format".to_vec();
    let tx = ds.transaction(Write, Optimistic).await.unwrap();
    tx.set(key.clone(), value.clone(), None).await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let snapshot = ds
        .recall_history_inventory(5_000, "physical", "test")
        .await
        .unwrap();
    let row = snapshot.records.iter().find(|row| row.key == key).unwrap();
    assert_eq!(row.value, value);
    assert!(row.current && !row.tombstone);
    assert!(ds
        .recall_physical_checkpoint_with_history(5_000, vec![history_authority(&snapshot)])
        .await
        .is_err());
    assert!(ds.transaction(Read, Optimistic).await.is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn memory_authorized_physical_copy_rebuilds_search_and_vector_from_independent_records() {
    let ds = Datastore::new("memory").await.unwrap();
    execute(&ds, "DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase; CREATE points:private SET space_id='s', text='sensitiveword', vector=[1,0]; CREATE points:independent SET space_id='s', text='independentword', vector=[9,0]; DEFINE INDEX search ON points FIELDS text SEARCH ANALYZER words BM25; DEFINE INDEX knn ON points FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN; DEFINE INDEX scalar ON points FIELDS text; DELETE points:private;").await;
    let snapshot = ds
        .recall_history_inventory(5_000, "physical", "test")
        .await
        .unwrap();
    ds.recall_physical_checkpoint_with_history(5_000, vec![history_authority(&snapshot)])
        .await
        .unwrap();
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM points WHERE text @0@ 'sensitiveword';"
        )
        .await,
        vec![surrealdb::syn::value("[]").unwrap()]
    );
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM points WHERE text @0@ 'independentword';"
        )
        .await,
        vec![surrealdb::syn::value("[points:independent]").unwrap()]
    );
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM points WHERE vector <|1,100|> [1,0];"
        )
        .await,
        vec![surrealdb::syn::value("[points:independent]").unwrap()]
    );
    assert_eq!(
        execute(
            &ds,
            "SELECT VALUE id FROM points WHERE text='independentword';"
        )
        .await,
        vec![surrealdb::syn::value("[points:independent]").unwrap()]
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn stale_memory_history_scope_refuses_physical_success_and_requires_restart() {
    let ds = Datastore::new("memory").await.unwrap();
    let snapshot = ds
        .recall_history_inventory(5_000, "physical", "test")
        .await
        .unwrap();
    execute(
        &ds,
        "CREATE points:independent SET space_id='s', text='after inspection';",
    )
    .await;
    assert!(ds
        .recall_physical_checkpoint_with_history(5_000, vec![history_authority(&snapshot)])
        .await
        .is_err());
    assert!(ds.transaction(Read, Optimistic).await.is_err());
}
