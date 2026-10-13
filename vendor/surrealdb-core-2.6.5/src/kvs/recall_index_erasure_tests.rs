use super::*;
use crate::kvs::recall_history::RecallHistoryRecord;
use crate::sql::statements::DefineStatement;
use crate::sql::Statement;
use crate::syn;

fn inventory_row(index: &DefineIndexStatement) -> RecallHistoryRecord {
    RecallHistoryRecord {
        key: crate::key::table::ix::new("ns", "db", &index.what, &index.name)
            .encode()
            .unwrap(),
        value: revision::to_vec(index).unwrap(),
        version: 1,
        timestamp: 1,
        tombstone: false,
        current: true,
    }
}

#[test]
fn extra_builder_keys_are_exact_current_search_state_only() {
    let search =
        definition("DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25");
    let scalar = definition("DEFINE INDEX former_search ON corpus FIELDS text");
    let vector = definition("DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2");
    let inventory = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        vec![
            inventory_row(&search),
            inventory_row(&scalar),
            inventory_row(&vector),
        ],
    )
    .unwrap();
    let requested = ["words_idx", "former_search", "points_idx", "removed_idx"]
        .into_iter()
        .map(|index| ("corpus".into(), index.into()))
        .collect();
    let targets = collect_targets(&inventory, &requested).unwrap();
    let expected = b"/*ns\0*db\0*corpus\0!bswords_idx\0".to_vec();
    let extras = rebuild_extra_keys(&targets).unwrap();
    assert_eq!(extras, [expected.clone()].into_iter().collect());
    let reversed = targets.into_iter().rev().collect::<Vec<_>>();
    assert_eq!(rebuild_extra_keys(&reversed).unwrap(), extras);
    assert!(rebuild_extra_keys(&[]).unwrap().is_empty());
    for (ns, db, table, index) in [
        ("foreign", "db", "corpus", "words_idx"),
        ("ns", "foreign", "corpus", "words_idx"),
        ("ns", "db", "foreign", "words_idx"),
        ("ns", "db", "corpus", "former_search"),
        ("ns", "db", "corpus", "points_idx"),
        ("ns", "db", "corpus", "removed_idx"),
    ] {
        assert!(!extras.contains(&Bs::new(ns, db, table, index).encode().unwrap()));
    }
    for suffix in [b"\0".as_slice(), b"trailing", b"\xff"] {
        let mut malformed = expected.clone();
        malformed.extend_from_slice(suffix);
        assert!(!extras.contains(&malformed));
    }
    assert!(!extras.contains(&expected[..expected.len() - 1]));
}

#[test]
fn target_discovery_uses_current_native_schema_and_is_order_independent() {
    let first =
        definition("DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25");
    let second = definition("DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2");
    let records = vec![inventory_row(&first), inventory_row(&second)];
    let inventory = RecallHistorySnapshot::new("ns".into(), "db".into(), records.clone()).unwrap();
    let reversed = RecallHistorySnapshot::new(
        "ns".into(),
        "db".into(),
        records.into_iter().rev().collect(),
    )
    .unwrap();
    let requested = [
        ("corpus".into(), "words_idx".into()),
        ("corpus".into(), "points_idx".into()),
        ("corpus".into(), "removed_idx".into()),
    ]
    .into_iter()
    .collect();
    let targets = collect_targets(&inventory, &requested).unwrap();
    assert_eq!(targets.len(), 2);
    assert_eq!(collect_targets(&reversed, &requested).unwrap(), targets);
    assert_eq!(
        collect_targets_with_deadline(
            &inventory,
            &requested,
            Instant::now() + Duration::from_secs(60)
        )
        .unwrap(),
        targets
    );
    assert!(collect_targets_with_deadline(&inventory, &requested, Instant::now()).is_err());
    assert!(targets.iter().all(|(ns, db, _)| ns == "ns" && db == "db"));
}

#[test]
fn target_discovery_rejects_misbound_or_trailing_schema_and_accepts_scalar_reconciliation() {
    let index =
        definition("DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25");
    let requested = [("corpus".into(), "words_idx".into())]
        .into_iter()
        .collect();
    let mut wrong_name = inventory_row(&index);
    let mut wrong_definition = index.as_ref().clone();
    wrong_definition.what = "unrelated".into();
    wrong_name.value = revision::to_vec(&wrong_definition).unwrap();
    let mut trailing = inventory_row(&index);
    trailing.value.push(0xff);
    let mut scalar = inventory_row(&index);
    scalar.value = revision::to_vec(
        definition("DEFINE INDEX words_idx ON corpus FIELDS text UNIQUE").as_ref(),
    )
    .unwrap();
    for row in [wrong_name, trailing] {
        let inventory = RecallHistorySnapshot::new("ns".into(), "db".into(), vec![row]).unwrap();
        assert!(collect_targets(&inventory, &requested).is_err());
    }
    let inventory = RecallHistorySnapshot::new("ns".into(), "db".into(), vec![scalar]).unwrap();
    let targets = collect_targets(&inventory, &requested).unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].2.index, Index::Uniq);
}

async fn execute(ds: &Datastore, sql: &str) -> Vec<Value> {
    ds.execute(sql, &Session::owner().with_ns("ns").with_db("db"), None)
        .await
        .unwrap()
        .into_iter()
        .map(|response| response.result.unwrap())
        .collect()
}

fn definition(sql: &str) -> Arc<DefineIndexStatement> {
    let query = syn::parse(sql).unwrap();
    let Statement::Define(DefineStatement::Index(index)) = &query.0 .0[0] else {
        panic!("fixture must be an index definition")
    };
    Arc::new(index.clone())
}

#[cfg(feature = "kv-surrealkv")]
async fn fixture() -> (tempfile::TempDir, Datastore, Vec<RebuildTarget>) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stage");
    let ds = Datastore::new(&format!("surrealkv+versioned://{}", path.display()))
        .await
        .unwrap();
    execute(
        &ds,
        "
		DEFINE TABLE corpus SCHEMALESS;
		DEFINE TABLE unrelated SCHEMALESS;
		DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;
		CREATE corpus:b SET text = 'keep old', point = [10,10] VERSION d'2020-01-01T00:00:00Z';
		UPSERT corpus:b SET text = 'keep current', point = [20,20] VERSION d'2021-01-01T00:00:00Z';
		CREATE unrelated:c SET text = 'independent old' VERSION d'2020-01-01T00:00:00Z';
		UPSERT unrelated:c SET text = 'independent current' VERSION d'2021-01-01T00:00:00Z';
	",
    )
    .await;
    let definitions = [
		definition("DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25 HIGHLIGHTS"),
		definition("DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F64"),
		definition("DEFINE INDEX exact_points_idx ON corpus FIELDS point MTREE DIMENSION 2 DIST EUCLIDEAN TYPE F64"),
		definition("DEFINE INDEX size_idx ON corpus COUNT"),
	];
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    for index in &definitions {
        // The native source-authorized copy retains schema and record history,
        // and omits the entire old aggregate keyspace before reconstruction.
        tx.set(
            crate::key::table::ix::new("ns", "db", &index.what, &index.name),
            revision::to_vec(index.as_ref()).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    drop(tx);
    let targets = definitions
        .into_iter()
        .map(|index| ("ns".into(), "db".into(), index))
        .collect();
    ds.shutdown().await.unwrap();
    drop(ds);
    // The real owner opens a fresh private Datastore after copying the schema
    // and records. Do not retain this fixture writer's empty-index caches.
    let ds = Datastore::new(&format!("surrealkv+versioned://{}", path.display()))
        .await
        .unwrap();
    (directory, ds, targets)
}

#[cfg(feature = "kv-surrealkv")]
async fn record_history(ds: &Datastore, table: &str) -> Vec<(Vec<u8>, Vec<u8>, u64, bool)> {
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    let records = tx
        .batch_keys_vals_versions(
            thing::prefix("ns", "db", table).unwrap()..thing::suffix("ns", "db", table).unwrap(),
            1024,
        )
        .await
        .unwrap();
    assert!(records.next.is_none());
    tx.cancel().await.unwrap();
    records.result
}

#[tokio::test]
#[cfg(feature = "kv-surrealkv")]
async fn rebuilt_search_vector_count_preserve_unrelated_record_versions_after_reopen() {
    let (directory, ds, targets) = fixture().await;
    let retained = record_history(&ds, "corpus").await;
    let unrelated = record_history(&ds, "unrelated").await;
    assert_eq!(retained.len(), 2);
    assert_eq!(unrelated.len(), 2);
    let historical = execute(
        &ds,
        "SELECT text FROM corpus:b VERSION d'2020-01-01T00:00:00Z';
		SELECT text FROM corpus VERSION d'2020-01-01T00:00:00Z';",
    )
    .await;
    rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(record_history(&ds, "corpus").await, retained);
    assert_eq!(record_history(&ds, "unrelated").await, unrelated);
    assert_eq!(
        execute(
            &ds,
            "SELECT text FROM corpus:b VERSION d'2020-01-01T00:00:00Z';
		SELECT text FROM corpus VERSION d'2020-01-01T00:00:00Z';"
        )
        .await,
        historical
    );
    let queries = "SELECT id,text FROM corpus WHERE text @@ 'keep';
		SELECT id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [20,20];
		SELECT id FROM corpus WITH INDEX exact_points_idx WHERE point <|1|> [20,20];
		SELECT count() AS count FROM corpus GROUP ALL;
		SELECT id FROM corpus WHERE text @@ 'erasedmarker';";
    let actual = execute(&ds, queries).await;
    assert_eq!(
        actual[0],
        syn::value("[{id:corpus:b,text:'keep current'}]").unwrap()
    );
    assert_eq!(actual[1], syn::value("[{id:corpus:b}]").unwrap());
    assert_eq!(actual[2], actual[1]);
    assert_eq!(actual[3], syn::value("[{count:1}]").unwrap());
    assert_eq!(actual[4], Value::Array(Default::default()));
    // Indexed VERSION is current-only in this native release; preserve that
    // existing observable behavior while direct/table historical reads survive.
    assert_eq!(
        execute(
            &ds,
            "SELECT id,text FROM corpus WHERE text @@ 'keep'
		VERSION d'2020-01-01T00:00:00Z';"
        )
        .await[0],
        actual[0]
    );
    ds.shutdown().await.unwrap();
    drop(ds);
    let reopened = Datastore::new(&format!(
        "surrealkv+versioned://{}",
        directory.path().join("stage").display()
    ))
    .await
    .unwrap();
    assert_eq!(record_history(&reopened, "corpus").await, retained);
    assert_eq!(record_history(&reopened, "unrelated").await, unrelated);
    assert_eq!(execute(&reopened, queries).await, actual);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "kv-surrealkv")]
async fn original_orphan_and_soft_deleted_index_history_refuse_rebuild() {
    let (_directory, ds, targets) = fixture().await;
    let mut orphan = prefix_range("ns", "db", &targets[0].2).unwrap().start;
    orphan.extend_from_slice(b"!orphan");
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(orphan.clone(), b"erasedmarker".to_vec(), Some(1))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    assert!(
        rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
            .await
            .is_err()
    );
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.del(orphan.clone()).await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    assert!(tx.get(orphan.clone(), None).await.unwrap().is_none());
    assert!(!tx
        .batch_keys_vals_versions(prefix_range("ns", "db", &targets[0].2).unwrap(), 10)
        .await
        .unwrap()
        .result
        .is_empty());
    tx.cancel().await.unwrap();
    drop(tx);
    assert!(
        rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
            .await
            .is_err()
    );
    // Only the owner-authorized complete-prefix filtering can clear this
    // condition; current-only absence must never be accepted as erasure.
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.clr(orphan).await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
        .await
        .unwrap();
    ds.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "kv-surrealkv")]
async fn changed_schema_duplicate_target_and_expired_budget_publish_no_index() {
    let (_directory, ds, targets) = fixture().await;
    let mut changed = targets.clone();
    let mut definition = changed[1].2.as_ref().clone();
    definition.comment = Some("changed".into());
    changed[1].2 = Arc::new(definition);
    assert!(
        rebuild_current(&ds, &changed, Instant::now() + Duration::from_secs(60))
            .await
            .is_err()
    );
    let mut duplicated = targets.clone();
    duplicated.push(targets[0].clone());
    assert!(
        rebuild_current(&ds, &duplicated, Instant::now() + Duration::from_secs(60))
            .await
            .is_err()
    );
    assert!(rebuild_current(&ds, &targets, Instant::now())
        .await
        .is_err());
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    for (ns, db, index) in &targets {
        assert!(tx
            .batch_keys_vals_versions(prefix_range(ns, db, index).unwrap(), 1)
            .await
            .unwrap()
            .result
            .is_empty());
    }
    tx.cancel().await.unwrap();
    drop(tx);
    ds.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "kv-surrealkv")]
async fn deferred_index_rebuild_retains_schema_without_background_builder() {
    let (_directory, ds, mut targets) = fixture().await;
    let mut index = targets[0].2.as_ref().clone();
    index.defer = true;
    targets[0].2 = Arc::new(index);
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(
        crate::key::table::ix::new("ns", "db", &targets[0].2.what, &targets[0].2.name),
        revision::to_vec(targets[0].2.as_ref()).unwrap(),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
        .await
        .unwrap();
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    assert_eq!(
        tx.get_tb_index("ns", "db", "corpus", "words_idx")
            .await
            .unwrap(),
        targets[0].2
    );
    let marker = tx
        .get(Df::new("ns", "db", "corpus", "words_idx"), None)
        .await
        .unwrap()
        .unwrap();
    assert!(revision::from_slice::<bool>(&marker).unwrap());
    tx.cancel().await.unwrap();
    drop(tx);
    assert_eq!(
        execute(&ds, "SELECT id FROM corpus WHERE text @@ 'keep'").await[0],
        syn::value("[{id:corpus:b}]").unwrap()
    );
    ds.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "kv-surrealkv")]
async fn scalar_reconciliation_preserves_old_versions_removes_stale_keys_and_rolls_back_conflicts()
{
    let (_directory, ds, _) = fixture().await;
    let index = definition("DEFINE INDEX scalar_idx ON corpus FIELDS text UNIQUE DEFER");
    let target = ("ns".to_owned(), "db".to_owned(), index.clone());
    let fields = crate::sql::Array::from(vec![Value::from("keep current")]);
    let stale_fields = crate::sql::Array::from(vec![Value::from("stale queued value")]);
    let entry = crate::key::index::Index::new("ns", "db", "corpus", "scalar_idx", &fields, None)
        .encode()
        .unwrap();
    let stale =
        crate::key::index::Index::new("ns", "db", "corpus", "scalar_idx", &stale_fields, None)
            .encode()
            .unwrap();
    let rid = Thing {
        tb: "corpus".into(),
        id: "b".into(),
    };
    let owner = revision::to_vec(&rid).unwrap();
    for timestamp in [1, 2] {
        let tx = ds
            .transaction(TransactionType::Write, LockType::Optimistic)
            .await
            .unwrap();
        tx.set(
            crate::key::table::ix::new("ns", "db", "corpus", "scalar_idx"),
            revision::to_vec(index.as_ref()).unwrap(),
            None,
        )
        .await
        .unwrap();
        tx.set(entry.clone(), owner.clone(), Some(timestamp))
            .await
            .unwrap();
        tx.set(stale.clone(), owner.clone(), Some(timestamp))
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    let original = tx
        .batch_keys_vals_versions(prefix_range("ns", "db", &index).unwrap(), 100)
        .await
        .unwrap()
        .result;
    assert_eq!(original.len(), 4);
    tx.cancel().await.unwrap();
    drop(tx);
    let record_bytes = record_history(&ds, "corpus").await;
    rebuild_current(
        &ds,
        &[target.clone()],
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .unwrap();
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    let reconciled = tx
        .batch_keys_vals_versions(prefix_range("ns", "db", &index).unwrap(), 100)
        .await
        .unwrap()
        .result;
    for row in &original {
        assert!(reconciled.contains(row));
    }
    assert_eq!(tx.get(entry.clone(), None).await.unwrap(), Some(owner));
    assert!(tx.get(stale, None).await.unwrap().is_none());
    tx.cancel().await.unwrap();
    drop(tx);
    assert_eq!(record_history(&ds, "corpus").await, record_bytes);
    assert_eq!(
        execute(
            &ds,
            "SELECT id FROM corpus WITH INDEX scalar_idx WHERE text = 'keep current'"
        )
        .await[0],
        syn::value("[{id:corpus:b}]").unwrap()
    );

    // An actual duplicate among retained current records must still fail, and
    // cancellation must restore the entire pre-rebuild scalar index state.
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    tx.set(
        thing::new("ns", "db", "corpus", &"c".into()),
        revision::to_vec(&syn::value("{id:corpus:c,text:'keep current'}").unwrap()).unwrap(),
        None,
    )
    .await
    .unwrap();
    // Remove only the completed marker so the selected private stage has no
    // builder internals, exactly as the owner filters before reconciliation.
    tx.clr(Df::new("ns", "db", "corpus", "scalar_idx"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    let before_failure = tx
        .batch_keys_vals_versions(prefix_range("ns", "db", &index).unwrap(), 100)
        .await
        .unwrap()
        .result;
    tx.cancel().await.unwrap();
    drop(tx);
    assert!(
        rebuild_current(&ds, &[target], Instant::now() + Duration::from_secs(60))
            .await
            .is_err()
    );
    let tx = ds
        .transaction(TransactionType::Read, LockType::Optimistic)
        .await
        .unwrap();
    assert_eq!(
        tx.batch_keys_vals_versions(prefix_range("ns", "db", &index).unwrap(), 100)
            .await
            .unwrap()
            .result,
        before_failure
    );
    tx.cancel().await.unwrap();
    drop(tx);
    ds.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "kv-rocksdb")]
async fn rocks_staging_uses_the_same_search_and_vector_reconstruction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stage");
    let ds = Datastore::new(&format!("rocksdb://{}", path.display()))
        .await
        .unwrap();
    execute(
        &ds,
        "DEFINE TABLE corpus SCHEMALESS;
        DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;
        CREATE corpus:b SET text = 'retained record', point = [20,20];",
    )
    .await;
    let definitions = [
        definition("DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25 HIGHLIGHTS"),
        definition("DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F64"),
    ];
    let tx = ds
        .transaction(TransactionType::Write, LockType::Optimistic)
        .await
        .unwrap();
    for index in &definitions {
        tx.set(
            crate::key::table::ix::new("ns", "db", &index.what, &index.name),
            revision::to_vec(index.as_ref()).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    drop(tx);
    ds.shutdown().await.unwrap();
    drop(ds);
    let ds = Datastore::new(&format!("rocksdb://{}", path.display()))
        .await
        .unwrap();
    let targets = definitions
        .into_iter()
        .map(|index| ("ns".into(), "db".into(), index))
        .collect::<Vec<_>>();
    let before = execute(&ds, "SELECT * FROM corpus:b").await;
    rebuild_current(&ds, &targets, Instant::now() + Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(execute(&ds, "SELECT * FROM corpus:b").await, before);
    let result = execute(
        &ds,
        "SELECT id FROM corpus WHERE text @@ 'retained';
        SELECT id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [20,20];",
    )
    .await;
    assert_eq!(result[0], syn::value("[{id:corpus:b}]").unwrap());
    assert_eq!(result[1], result[0]);
    ds.shutdown().await.unwrap();
}
