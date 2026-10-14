use super::*;
use crate::kvs::api::Transaction as NativeTransaction;
use tempfile::TempDir;

fn options(path: &Path) -> Options {
	let mut options = Options::new();
	options.dir = path.to_owned();
	options.enable_versions = true;
	options
}

fn put(state: &PhysicalState, key: &[u8], value: &[u8], timestamp: u64) -> Result<(), Error> {
	let mut tx = state.begin(Mode::ReadWrite)?;
	tx.set_at_ts(key, value, timestamp).map_err(failure)?;
	tx.commit().map_err(failure)
}

#[tokio::test]
async fn retirement_preserves_exact_history_and_reopens_new_directory() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	put(&state, b"keep", b"old independent version", 1)?;
	put(&state, b"keep", b"new independent version", 2)?;
	put(&state, b"erase", b"erased-only-value-aa1098", 3)?;
	{
		let mut tx = state.begin(Mode::ReadWrite)?;
		tx.delete(b"erase").map_err(failure)?;
		tx.commit().map_err(failure)?;
	}
	let old = directory_identity(&path)?;
	checkpoint(state.clone(), 10_000).await?;
	assert_ne!(old, directory_identity(&path)?);
	{
		let mut tx = state.begin(Mode::ReadOnly)?;
		assert_eq!(tx.get(b"erase").map_err(failure)?, None);
		assert_eq!(tx.get_all_versions(b"keep").map_err(failure)?.len(), 2);
	}
	let proof = read_receipt(&sibling(&path, RECEIPT_SUFFIX)?, true)?
		.ok_or_else(|| failure("receipt missing"))?;
	assert_eq!(proof.current.old_directory, old);
	assert_eq!(proof.current.current_directory, directory_identity(&path)?);
	assert_ne!(
		proof.current.old_engine_identity,
		proof.current.engine_identity
	);
	assert!(!sibling(&path, PENDING_SUFFIX)?.exists());
	state.shutdown().await?;
	drop(state);
	let reopened = PhysicalState::open(options(&path))?;
	assert_eq!(
		reopened
			.begin(Mode::ReadOnly)?
			.get_all_versions(b"keep")
			.map_err(failure)?
			.len(),
		2
	);
	Ok(())
}

#[tokio::test]
async fn retained_snapshot_timeout_leaves_untouched_generation_available_for_retry(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let state = Arc::new(PhysicalState::open(options(
		&root.path().join("database.db"),
	))?);
	let lease = state.admit().await?;
	let tx = state.begin(Mode::ReadOnly)?;
	assert!(checkpoint(state.clone(), 1).await.is_err());
	drop(tx);
	drop(lease);
	assert!(state.admit().await.is_ok());
	assert!(state.begin(Mode::ReadOnly).is_ok());
	Ok(())
}

#[tokio::test]
async fn dropped_checkpoint_waiter_keeps_exclusive_native_owner() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	put(&state, b"key", b"surviving", 1)?;
	let lease = state.admit().await?;
	let task = tokio::spawn(checkpoint(state.clone(), 10_000));
	// Poll the actual outer future into its owned checkpoint task before abort.
	for _ in 0..16 {
		tokio::task::yield_now().await;
	}
	task.abort();
	let _ = task.await;
	drop(lease);
	tokio::time::timeout(Duration::from_secs(10), async {
		loop {
			if sibling(&path, RECEIPT_SUFFIX).is_ok_and(|p| p.exists()) {
				break;
			}
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await
	.map_err(failure)?;
	let _lease = state.admit().await?;
	assert_eq!(
		state.begin(Mode::ReadOnly)?.get(b"key").map_err(failure)?,
		Some(b"surviving".to_vec())
	);
	Ok(())
}

#[test]
fn second_owner_cannot_recover_or_open_a_live_generation() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let first = PhysicalState::open(options(&path))?;
	assert!(PhysicalState::open(options(&path)).is_err());
	assert!(first.begin(Mode::ReadOnly).is_ok());
	Ok(())
}

#[tokio::test]
async fn repeated_checkpoint_links_original_and_new_identities() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	put(&state, b"key", b"one", 1)?;
	checkpoint(state.clone(), 10_000).await?;
	put(&state, b"key", b"two", 2)?;
	checkpoint(state.clone(), 10_000).await?;
	let proof = read_receipt(&sibling(&path, RECEIPT_SUFFIX)?, true)?
		.ok_or_else(|| failure("receipt missing"))?;
	let previous = proof
		.previous
		.ok_or_else(|| failure("prior checkpoint missing"))?;
	assert_eq!(proof.current.sequence, 2);
	assert_eq!(previous.engine_identity, proof.current.old_engine_identity);
	assert_eq!(
		proof.current.previous_sha256,
		Some(checkpoint_hash(&previous)?)
	);
	assert_eq!(
		state
			.begin(Mode::ReadOnly)?
			.get_all_versions(b"key")
			.map_err(failure)?
			.len(),
		2
	);
	Ok(())
}

#[tokio::test]
async fn committed_adapter_lease_outlives_transaction_until_explicit_drop() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let datastore =
		super::super::Datastore::new(path.to_str().ok_or_else(|| failure("path"))?, true).await?;
	let mut transaction = datastore.transaction(true, false).await?;
	transaction
		.set(b"key".to_vec(), b"value".to_vec(), None)
		.await?;
	transaction.commit().await?;
	assert!(datastore
		.physical
		.admission
		.clone()
		.try_write_owned()
		.is_err());
	drop(transaction);
	assert!(datastore
		.physical
		.admission
		.clone()
		.try_write_owned()
		.is_ok());
	Ok(())
}

#[tokio::test]
async fn transaction_retains_os_custody_after_datastore_is_dropped() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let datastore =
		super::super::Datastore::new(path.to_str().ok_or_else(|| failure("path"))?, true).await?;
	let mut transaction = datastore.transaction(false, false).await?;
	drop(datastore);
	assert!(PhysicalState::open(options(&path)).is_err());
	transaction.cancel().await?;
	// Cancel does not itself drop the original native snapshot.
	assert!(PhysicalState::open(options(&path)).is_err());
	drop(transaction);
	let reopened = PhysicalState::open(options(&path))?;
	reopened.shutdown().await?;
	Ok(())
}

#[tokio::test]
async fn detached_commit_owner_retains_custody_through_unwind() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let datastore =
		super::super::Datastore::new(path.to_str().ok_or_else(|| failure("path"))?, true).await?;
	let mut transaction = datastore.transaction(true, false).await?;
	let owner = super::super::CommitOwner {
		inner: transaction
			.inner
			.take()
			.ok_or_else(|| failure("missing transaction"))?,
		_admission: transaction._physical_admission.clone(),
		_physical_owner: transaction._physical_owner.clone(),
	};
	transaction.done = true;
	drop(transaction);
	drop(datastore);
	assert!(PhysicalState::open(options(&path)).is_err());
	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
		let _owner = owner;
		std::panic::resume_unwind(Box::new(()));
	}));
	assert!(result.is_err());
	let reopened = PhysicalState::open(options(&path))?;
	reopened.shutdown().await?;
	Ok(())
}

#[test]
fn incomplete_identity_initialization_recovers_without_rewriting_authoritative_identity(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	fs::write(root.path().join(IDENTITY_PENDING), b"82aaf204-4b").map_err(failure)?;
	let first = ensure_identity(root.path())?;
	assert!(canonical_identity(&first));
	assert!(!root.path().join(IDENTITY_PENDING).exists());
	fs::write(root.path().join(IDENTITY_PENDING), b"710abc").map_err(failure)?;
	assert_eq!(ensure_identity(root.path())?, first);
	fs::write(root.path().join(IDENTITY), b"broken").map_err(failure)?;
	assert!(ensure_identity(root.path()).is_err());
	assert_eq!(
		fs::read(root.path().join(IDENTITY)).map_err(failure)?,
		b"broken"
	);
	Ok(())
}

#[tokio::test]
async fn source_bearing_unknown_filename_never_enters_receipt_or_retirement() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	put(&state, b"keep", b"surviving", 1)?;
	let source_name = "personal-source-payload-file";
	fs::write(path.join(source_name), b"foreign").map_err(failure)?;
	assert!(checkpoint(state.clone(), 10_000).await.is_err());
	assert!(path.join(source_name).exists());
	assert!(!sibling(&path, RECEIPT_SUFFIX)?.exists());
	assert!(!sibling(&path, PENDING_SUFFIX)?.exists());
	Ok(())
}

#[tokio::test]
async fn receipt_rejects_rehashed_unknown_inventory_names() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	put(&state, b"keep", b"surviving", 1)?;
	checkpoint(state.clone(), 10_000).await?;
	let receipt_path = sibling(&path, RECEIPT_SUFFIX)?;
	let mut proof = read_receipt(&receipt_path, true)?.ok_or_else(|| failure("receipt missing"))?;
	proof.current.original_payload_generations[0].name = "source-bearing-filename".into();
	proof.current_sha256 = checkpoint_hash(&proof.current)?;
	fs::write(&receipt_path, serde_json::to_vec(&proof).map_err(failure)?).map_err(failure)?;
	assert!(read_receipt(&receipt_path, true).is_err());
	Ok(())
}

#[tokio::test]
async fn inventory_includes_hidden_versions_and_ignores_other_native_scopes() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let state = Arc::new(PhysicalState::open(options(
		&root.path().join("database.db"),
	))?);
	let id = crate::sql::Id::from("shared");
	let key = crate::key::thing::new("ns", "db", "entity", &id).encode()?;
	put(&state, &key, b"older independent version", 10)?;
	put(&state, &key, b"historical erased contribution", 20)?;
	{
		let mut tx = state.begin(Mode::ReadWrite)?;
		tx.soft_delete(&key).map_err(failure)?;
		tx.commit().map_err(failure)?;
	}
	let before = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	assert_eq!(before.records.len(), 3);
	assert_eq!(before.records.iter().filter(|row| row.current).count(), 1);
	assert!(before
		.records
		.iter()
		.any(|row| row.current && row.tombstone));
	put(&state, b"global-heartbeat", b"unrelated activity", 30)?;
	let unchanged = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	assert_eq!(before.fingerprint, unchanged.fingerprint);
	put(&state, &key, b"concurrent scoped change", 40)?;
	let changed = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	assert_ne!(before.fingerprint, changed.fingerprint);
	Ok(())
}

#[tokio::test]
async fn inventory_timeout_is_read_only_and_does_not_poison_original_owner() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let state = Arc::new(PhysicalState::open(options(
		&root.path().join("database.db"),
	))?);
	let lease = state.admit().await?;
	assert!(
		history_inventory(state.clone(), 1, "ns".into(), "db".into())
			.await
			.is_err()
	);
	drop(lease);
	let _lease = state.admit().await?;
	assert!(state.begin(Mode::ReadOnly).is_ok());
	Ok(())
}

fn history_authority(
	snapshot: &RecallHistorySnapshot,
) -> Result<crate::kvs::recall_history::RecallHistoryAuthority, Error> {
	let journal = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
	let key = [37; 32];
	let source = crate::sql::Thing::from(("source", "erased"));
	let token =
		crate::fnc::recall::recall_reference_identity_token(journal, "home", &key, &[], &source)?;
	crate::kvs::recall_history::RecallHistoryAuthority::new(
		"ns".into(),
		"db".into(),
		journal.into(),
		"home".into(),
		key,
		Vec::new(),
		[token].into_iter().collect(),
	)?
	.bind_history(
		snapshot.fingerprint.clone(),
		Default::default(),
		Default::default(),
	)
}

fn document(source: &str, text: &str) -> Result<Vec<u8>, Error> {
	let value = crate::sql::Value::Object(
		[
			("space_id".into(), crate::sql::Value::from("home")),
			(
				"source_id".into(),
				crate::sql::Value::Thing(crate::sql::Thing::from(("source", source))),
			),
			("text".into(), crate::sql::Value::from(text)),
		]
		.into_iter()
		.collect(),
	);
	revision::to_vec(&value).map_err(failure)
}

#[tokio::test]
async fn trusted_history_copy_erases_only_source_versions_and_preserves_independent_past(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	let shared_id = crate::sql::Id::from("shared");
	let hidden_id = crate::sql::Id::from("historical-only");
	let shared = crate::key::thing::new("ns", "db", "entity", &shared_id).encode()?;
	let hidden = crate::key::thing::new("ns", "db", "entity", &hidden_id).encode()?;
	let independent_old = document("surviving", "B independent old")?;
	let independent_current = document("surviving", "B independent current")?;
	put(&state, &shared, &independent_old, 10)?;
	put(
		&state,
		&shared,
		&document("erased", "A mixed erased payload 15927")?,
		20,
	)?;
	put(&state, &shared, &independent_current, 30)?;
	put(
		&state,
		&hidden,
		&document("erased", "A hidden erased payload 6872")?,
		40,
	)?;
	{
		let mut tx = state.begin(Mode::ReadWrite)?;
		tx.soft_delete(&hidden).map_err(failure)?;
		tx.commit().map_err(failure)?;
	}
	let snapshot = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	checkpoint_with_history(
		state.clone(),
		10_000,
		vec![history_authority(&snapshot)?],
		Capabilities::default(),
	)
	.await?;
	{
		let tx = state.begin(Mode::ReadOnly)?;
		assert_eq!(
			tx.get_at_version(&shared, 10).map_err(failure)?,
			Some(independent_old)
		);
		assert_eq!(tx.get_at_version(&shared, 20).map_err(failure)?, None);
		assert_eq!(
			tx.get_at_version(&shared, 30).map_err(failure)?,
			Some(independent_current)
		);
		assert!(tx.get_all_versions(&hidden).map_err(failure)?.is_empty());
	}
	for entry in fs::read_dir(path.join("clog")).map_err(failure)? {
		let bytes = fs::read(entry.map_err(failure)?.path()).map_err(failure)?;
		for erased in [
			b"A mixed erased payload 15927".as_slice(),
			b"A hidden erased payload 6872",
		] {
			assert!(!bytes.windows(erased.len()).any(|window| window == erased));
		}
	}
	assert!(read_receipt(&sibling(&path, RECEIPT_SUFFIX)?, true)?.is_some());
	Ok(())
}

#[tokio::test]
async fn stale_history_authority_refuses_before_mutation_and_can_reprepare() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let state = Arc::new(PhysicalState::open(options(&path))?);
	let id = crate::sql::Id::from("independent");
	let key = crate::key::thing::new("ns", "db", "entity", &id).encode()?;
	put(&state, &key, &document("surviving", "old")?, 10)?;
	let snapshot = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	put(&state, &key, &document("surviving", "new")?, 20)?;
	let original = directory_identity(&path)?;
	assert!(checkpoint_with_history(
		state.clone(),
		10_000,
		vec![history_authority(&snapshot)?],
		Capabilities::default()
	)
	.await
	.is_err());
	assert_eq!(directory_identity(&path)?, original);
	assert!(!sibling(&path, RECEIPT_SUFFIX)?.exists());
	assert!(!sibling(&path, PENDING_SUFFIX)?.exists());
	let fresh = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	checkpoint_with_history(
		state.clone(),
		10_000,
		vec![history_authority(&fresh)?],
		Capabilities::default(),
	)
	.await?;
	assert_eq!(
		state
			.begin(Mode::ReadOnly)?
			.get_all_versions(&key)
			.map_err(failure)?
			.len(),
		2
	);
	Ok(())
}

async fn execute_native(
	ds: &crate::kvs::Datastore,
	sql: &str,
) -> Result<Vec<crate::sql::Value>, Error> {
	ds.execute(
		sql,
		&crate::dbs::Session::owner().with_ns("ns").with_db("db"),
		None,
	)
	.await?
	.into_iter()
	.map(|response| response.result)
	.collect()
}

#[tokio::test]
async fn physical_history_rebuilds_search_and_vector_indexes_before_original_retirement(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let url = format!("surrealkv+versioned://{}", path.display());
	let original = crate::kvs::Datastore::new(&url).await?;
	execute_native(&original, "
        DEFINE TABLE corpus SCHEMALESS;
        DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;
        CREATE corpus:b SET space_id='home',source_id=source:surviving,text='keep old',point=[10,10]
            VERSION d'2020-01-01T00:00:00Z';
        UPSERT corpus:b SET space_id='home',source_id=source:surviving,text='keep current',point=[20,20]
            VERSION d'2021-01-01T00:00:00Z';
        CREATE corpus:a SET space_id='home',source_id=source:erased,text='erasedmarker916272',point=[1,1];
        DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25;
        DEFINE INDEX points_idx ON corpus FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F64;
        DELETE corpus:a;
    ").await?;
	original
		.recall_shutdown_staging(Instant::now() + Duration::from_secs(60))
		.await?;
	drop(original);
	let state = Arc::new(PhysicalState::open(options(&path))?);
	let retained_key =
		crate::key::thing::new("ns", "db", "corpus", &crate::sql::Id::from("b")).encode()?;
	let retained = state
		.begin(Mode::ReadOnly)?
		.get_all_versions(&retained_key)
		.map_err(failure)?;
	assert_eq!(retained.len(), 2);
	let snapshot = history_inventory(state.clone(), 60_000, "ns".into(), "db".into()).await?;
	let old_directory = directory_identity(&path)?;
	checkpoint_with_history(
		state.clone(),
		60_000,
		vec![history_authority(&snapshot)?],
		Capabilities::default(),
	)
	.await?;
	assert_ne!(directory_identity(&path)?, old_directory);
	assert_eq!(
		state
			.begin(Mode::ReadOnly)?
			.get_all_versions(&retained_key)
			.map_err(failure)?,
		retained
	);
	state.shutdown().await?;
	drop(state);
	let reopened = crate::kvs::Datastore::new(&url).await?;
	let rows = execute_native(
		&reopened,
		"
        SELECT id,text FROM corpus WHERE text @@ 'keep';
        SELECT id FROM corpus WITH INDEX points_idx WHERE point <|1,100|> [20,20];
        SELECT id FROM corpus WHERE text @@ 'erasedmarker916272';
    ",
	)
	.await?;
	assert_eq!(
		rows[0],
		crate::syn::value("[{id:corpus:b,text:'keep current'}]")?
	);
	assert_eq!(rows[1], crate::syn::value("[{id:corpus:b}]")?);
	assert_eq!(rows[2], crate::sql::Value::Array(Default::default()));
	reopened
		.recall_shutdown_staging(Instant::now() + Duration::from_secs(60))
		.await?;
	drop(reopened);
	for entry in fs::read_dir(path.join("clog")).map_err(failure)? {
		let bytes = fs::read(entry.map_err(failure)?.path()).map_err(failure)?;
		let erased = b"erasedmarker916272";
		assert!(!bytes.windows(erased.len()).any(|window| window == erased));
	}
	Ok(())
}

#[tokio::test]
async fn expired_staging_open_refuses_before_creating_native_files() -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("unpublished");
	assert!(super::super::Datastore::new_until(
		path.to_str().ok_or_else(|| failure("path"))?,
		true,
		Instant::now()
	)
	.await
	.is_err());
	assert!(!path.exists());
	assert!(fs::read_dir(root.path()).map_err(failure)?.next().is_none());
	Ok(())
}

#[tokio::test]
async fn plain_surrealkv_retirement_erases_deleted_log_and_preserves_native_current_version(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let mut native_options = options(&path);
	native_options.enable_versions = false;
	let state = Arc::new(PhysicalState::open(native_options)?);
	let source_id = crate::sql::Id::from("erased");
	let key = crate::key::thing::new("ns", "db", "source", &source_id).encode()?;
	put(
		&state,
		&key,
		&document("erased", "private source bytes")?,
		10,
	)?;
	let retained_key =
		crate::key::thing::new("ns", "db", "source", &crate::sql::Id::from("surviving"))
			.encode()?;
	put(
		&state,
		&retained_key,
		&document("surviving", "independent bytes")?,
		20,
	)?;
	{
		let mut tx = state.begin(Mode::ReadWrite)?;
		tx.soft_delete(&key).map_err(failure)?;
		tx.commit().map_err(failure)?;
	}
	// The actual nonversioned commit removes tombstones from its index. The
	// original payload still exists in the native log until physical retirement.
	state.shutdown().await?;
	drop(state);
	let mut native_options = options(&path);
	native_options.enable_versions = false;
	let state = Arc::new(PhysicalState::open(native_options)?);
	let erased = b"private source bytes";
	let mut original_payload_seen = false;
	for entry in fs::read_dir(path.join("clog")).map_err(failure)? {
		let bytes = fs::read(entry.map_err(failure)?.path()).map_err(failure)?;
		original_payload_seen |= bytes.windows(erased.len()).any(|window| window == erased);
	}
	assert!(original_payload_seen);
	let snapshot = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	assert_eq!(snapshot.records.len(), 1);
	assert_eq!(snapshot.records[0].key, retained_key);
	assert!(snapshot.records[0].current && !snapshot.records[0].tombstone);
	assert!(snapshot.records[0].version > 0);
	checkpoint_with_history(
		state.clone(),
		10_000,
		vec![history_authority(&snapshot)?],
		Capabilities::default(),
	)
	.await?;
	let after = history_inventory(state.clone(), 10_000, "ns".into(), "db".into()).await?;
	assert_eq!(snapshot.fingerprint, after.fingerprint);
	state.shutdown().await?;
	drop(state);
	for entry in fs::read_dir(path.join("clog")).map_err(failure)? {
		let bytes = fs::read(entry.map_err(failure)?.path()).map_err(failure)?;
		assert!(!bytes.windows(erased.len()).any(|window| window == erased));
	}
	Ok(())
}

#[tokio::test]
async fn staged_analyzer_uses_original_denied_function_policy_before_retirement(
) -> Result<(), Error> {
	let root = TempDir::new().map_err(failure)?;
	let path = root.path().join("database.db");
	let url = format!("surrealkv+versioned://{}", path.display());
	let original = crate::kvs::Datastore::new(&url).await?;
	execute_native(
		&original,
		"
        DEFINE TABLE corpus SCHEMALESS;
        DEFINE FUNCTION fn::retained_text($text: string) { RETURN $text; };
        DEFINE ANALYZER words FUNCTION fn::retained_text TOKENIZERS blank,class;
        CREATE corpus:b SET space_id='home',source_id=source:surviving,text='keep current';
        DEFINE INDEX words_idx ON corpus FIELDS text SEARCH ANALYZER words BM25;
    ",
	)
	.await?;
	original
		.recall_shutdown_staging(Instant::now() + Duration::from_secs(60))
		.await?;
	drop(original);
	let state = Arc::new(PhysicalState::open(options(&path))?);
	let snapshot = history_inventory(state.clone(), 60_000, "ns".into(), "db".into()).await?;
	let identity = directory_identity(&path)?;
	let capabilities =
		Capabilities::default().without_functions(crate::dbs::capabilities::Targets::Some(
			[crate::dbs::capabilities::FuncTarget(
				"fn".into(),
				Some("retained_text".into()),
			)]
			.into_iter()
			.collect(),
		));
	let error = checkpoint_with_history(
		state.clone(),
		60_000,
		vec![history_authority(&snapshot)?],
		capabilities,
	)
	.await
	.expect_err("the original function restriction must reject staged computation");
	assert!(matches!(error, Error::FunctionNotAllowed(_)));
	assert_eq!(directory_identity(&path)?, identity);
	assert!(!sibling(&path, RECEIPT_SUFFIX)?.exists());
	assert!(!sibling(&path, PENDING_SUFFIX)?.exists());
	assert!(state.admit().await.is_err());
	drop(state);
	// Failure after staging keeps admission closed until original recovery.
	let recovered = PhysicalState::open(options(&path))?;
	let key = crate::key::thing::new("ns", "db", "corpus", &crate::sql::Id::from("b")).encode()?;
	assert!(recovered
		.begin(Mode::ReadOnly)?
		.get(&key)
		.map_err(failure)?
		.is_some());
	Ok(())
}
