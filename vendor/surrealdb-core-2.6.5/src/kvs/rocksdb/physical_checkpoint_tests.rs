//! Native engine controls; these must run with kv-rocksdb, not the memory backend.
use super::super::Datastore;
use super::*;
use crate::kvs::api::Transaction as _;
use temp_dir::TempDir;

struct TestDirectory {
	path: PathBuf,
	_root: TempDir,
}

impl TestDirectory {
	fn new() -> Self {
		let root = TempDir::new().unwrap();
		let path = root.path().join("database");
		fs::create_dir(&path).unwrap();
		Self { path, _root: root }
	}
	fn path(&self) -> &Path {
		&self.path
	}
}

#[tokio::test]
async fn cancelled_read_transaction_keeps_physical_admission_until_drop() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let mut transaction = store.transaction(false, false).await.unwrap();
	transaction.cancel().await.unwrap();
	assert!(store.physical.admission.clone().try_write_owned().is_err());
	drop(transaction);
	assert!(store.physical.admission.clone().try_write_owned().is_ok());
}

#[tokio::test]
async fn committed_write_transaction_keeps_physical_admission_until_drop() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let mut transaction = store.transaction(true, false).await.unwrap();
	transaction
		.set(b"key".to_vec(), b"value".to_vec(), None)
		.await
		.unwrap();
	transaction.commit().await.unwrap();
	assert!(store.physical.admission.clone().try_write_owned().is_err());
	drop(transaction);
	assert!(store.physical.admission.clone().try_write_owned().is_ok());
}

#[tokio::test]
async fn original_drain_timeout_poison_refuses_new_native_transactions() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let transaction = store.transaction(false, false).await.unwrap();
	let result = store.recall_physical_checkpoint(1).await;
	assert!(result.is_err());
	drop(transaction);
	assert!(store.transaction(false, false).await.is_err());
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
}

#[tokio::test]
async fn cancelled_waiter_does_not_cancel_owned_checkpoint() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let transaction = store.transaction(false, false).await.unwrap();
	let db = store.db.clone();
	let state = store.physical.clone();
	let options = store.options.clone();
	let deadline = Instant::now() + Duration::from_secs(60);
	let waiter = tokio::spawn(async move {
		checkpoint(db, options, state, deadline, None, None).await
	});
	// Wait until the checkpoint has joined the fair lock's writer queue.
	tokio::time::timeout(Duration::from_secs(5), async {
		loop {
			if store.physical.admission.clone().try_read_owned().is_err() {
				break;
			}
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	waiter.abort();
	assert!(waiter.await.unwrap_err().is_cancelled());
	assert!(store.physical.admission.clone().try_write_owned().is_err());
	drop(transaction);
	tokio::time::timeout(Duration::from_secs(60), async {
		loop {
			if sibling_path(directory.path(), RECEIPT_SUFFIX)
				.unwrap()
				.exists()
			{
				break;
			}
			assert!(!store.physical.failed.load(Ordering::Acquire));
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	})
	.await
	.unwrap();
	let transaction = store.transaction(false, false).await.unwrap();
	drop(transaction);
	assert!(read_previous_receipt(directory.path()).unwrap().is_some());
}

#[test]
fn panic_owner_marks_failure_before_releasing_native_lease() {
	let directory = TestDirectory::new();
	let state = Arc::new(PhysicalState::new(directory.path().to_str().unwrap()).unwrap());
	let lease = state.admission.clone().try_write_owned().unwrap();
	let owner = MaintenanceOwner {
		state: state.clone(),
		success: false,
		_lease: lease,
	};
	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
		let _owned = owner;
		panic!("injected native worker panic");
	}));
	assert!(result.is_err());
	assert!(state.healthy().is_err());
	assert!(state.admission.clone().try_write_owned().is_ok());
}

#[test]
fn native_inventory_includes_archived_wal_and_refuses_unknown_payload_directory() {
	let directory = TestDirectory::new();
	fs::create_dir(directory.path().join("archive")).unwrap();
	fs::write(directory.path().join("archive/000001.log"), b"old WAL").unwrap();
	let rows = generations(directory.path(), Instant::now() + Duration::from_secs(5)).unwrap();
	assert_eq!(rows["archive/000001.log"].bytes, 7);
	fs::create_dir(directory.path().join("unclassified-history")).unwrap();
	assert!(generations(directory.path(), Instant::now() + Duration::from_secs(5)).is_err());
}

#[tokio::test]
async fn unknown_generation_refuses_receipt_and_poison_closes_admission() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	fs::write(
		directory.path().join("unclassified-payload.backup"),
		b"historical payload",
	)
	.unwrap();
	assert!(store.recall_physical_checkpoint(60_000).await.is_err());
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
	assert!(store.transaction(false, false).await.is_err());
}

#[tokio::test]
async fn actual_blob_sst_and_wal_retire_twice_while_independent_value_survives() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let private = vec![0x9a; 16 * 1024];
	let private_key = b"SOURCE_INDEX_BOUNDARY_PRIVATE_89341".to_vec();
	let independent = vec![0x6b; 16 * 1024];
	{
		let mut tx = store.transaction(true, false).await.unwrap();
		tx.set(private_key.clone(), private.clone(), None)
			.await
			.unwrap();
		tx.set(b"independent".to_vec(), independent.clone(), None)
			.await
			.unwrap();
		tx.commit().await.unwrap();
	}
	let mut flush = FlushOptions::default();
	flush.set_wait(true);
	store
		.db
		.current
		.read()
		.unwrap()
		.as_ref()
		.unwrap()
		.flush_opt(&flush)
		.unwrap();
	let before = generations(directory.path(), Instant::now() + Duration::from_secs(5)).unwrap();
	assert!(before.values().any(|row| row.name.ends_with(".sst")));
	assert!(before.values().any(|row| row.name.ends_with(".blob")));
	assert!(before
		.values()
		.filter(|row| row.name.starts_with("MANIFEST-"))
		.any(|row| fs::read(directory.path().join(&row.name))
			.unwrap()
			.windows(private_key.len())
			.any(|window| window == private_key.as_slice())));
	fs::write(directory.path().join("LOG.old.123456"), &private_key).unwrap();
	{
		let mut tx = store.transaction(true, false).await.unwrap();
		tx.del(private_key.clone()).await.unwrap();
		tx.commit().await.unwrap();
	}
	store.recall_physical_checkpoint(60_000).await.unwrap();
	{
		let mut tx = store.transaction(true, false).await.unwrap();
		tx.set(b"completion".to_vec(), b"erased".to_vec(), None)
			.await
			.unwrap();
		tx.commit().await.unwrap();
	}
	store.recall_physical_checkpoint(60_000).await.unwrap();
	let receipt: Receipt = serde_json::from_slice(
		&fs::read(sibling_path(directory.path(), RECEIPT_SUFFIX).unwrap()).unwrap(),
	)
	.unwrap();
	assert_eq!(receipt.current.sequence, 2);
	let previous = receipt.previous.as_ref().unwrap();
	assert_eq!(previous.sequence, 1);
	assert_eq!(
		receipt.current.previous_sha256.as_ref(),
		Some(&checkpoint_digest(previous).unwrap())
	);
	assert_ne!(previous.old_engine_identity, previous.engine_identity);
	assert_eq!(
		previous.engine_identity,
		receipt.current.old_engine_identity
	);
	assert_ne!(
		receipt.current.old_engine_identity,
		receipt.current.engine_identity
	);
	assert_eq!(
		engine_identity(directory.path()).unwrap(),
		receipt.current.engine_identity
	);
	// Native filenames may legitimately restart in a fresh engine. Generation
	// identity plus complete old-directory retirement, not basename reuse, proves absence.
	assert!(previous
		.original_payload_generations
		.iter()
		.any(|row| row.name.starts_with("MANIFEST-")));
	assert!(previous
		.original_payload_generations
		.iter()
		.any(|row| row.name == "LOG"));
	assert!(previous
		.original_payload_generations
		.iter()
		.any(|row| row.name == "LOG.old.123456"));
	for row in generations(directory.path(), Instant::now() + Duration::from_secs(5))
		.unwrap()
		.values()
	{
		let raw = fs::read(directory.path().join(&row.name)).unwrap();
		assert!(
			!raw.windows(private_key.len())
				.any(|window| window == private_key.as_slice()),
			"deleted indexed key remains in {}",
			row.name
		);
	}
	let mut tx = store.transaction(false, false).await.unwrap();
	assert_eq!(tx.get(private_key.clone(), None).await.unwrap(), None);
	assert_eq!(
		tx.get(b"independent".to_vec(), None).await.unwrap(),
		Some(independent)
	);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn queued_checkpoint_rechecks_sticky_failure_after_exclusive_admission() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let held = store.physical.admission.clone().write_owned().await;
	let db = store.db.clone();
	let options = store.options.clone();
	let state = store.physical.clone();
	let deadline = Instant::now() + Duration::from_secs(60);
	let queued = tokio::spawn(async move {
		checkpoint(db, options, state, deadline, None, None).await
	});
	tokio::task::yield_now().await;
	store.physical.failed.store(true, Ordering::Release);
	drop(held);
	assert!(queued.await.unwrap().is_err());
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
}

#[tokio::test]
async fn datastore_drop_cannot_release_stable_owner_before_last_native_transaction() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let transaction = store.transaction(false, false).await.unwrap();
	drop(store);
	assert!(Datastore::new(directory.path().to_str().unwrap())
		.await
		.is_err());
	drop(transaction);
	let reopened = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	drop(reopened);
}

#[tokio::test]
async fn escaped_native_handle_refuses_retirement_and_keeps_original_engine() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let escaped = store.db.current.read().unwrap().as_ref().unwrap().clone();
	let identity = engine_identity(directory.path()).unwrap();
	assert!(store.recall_physical_checkpoint(60_000).await.is_err());
	assert_eq!(engine_identity(directory.path()).unwrap(), identity);
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
	assert!(sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	drop(escaped);
	drop(store);
	// Recovery discards the uncommitted replacement and preserves original data.
	let reopened = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	assert_eq!(engine_identity(directory.path()).unwrap(), identity);
	assert!(!sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	drop(reopened);
}

#[tokio::test]
async fn pending_receipt_rejects_extra_payload_and_invalid_generation_paths() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	store.recall_physical_checkpoint(60_000).await.unwrap();
	let path = sibling_path(directory.path(), RECEIPT_SUFFIX).unwrap();
	let raw = fs::read(&path).unwrap();
	let mut value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
	value["current"]["source_payload"] = "must not be adopted into metadata".into();
	fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
	assert!(read_previous_receipt(directory.path()).is_err());
	let mut receipt: Receipt = serde_json::from_slice(&raw).unwrap();
	receipt.current.current_payload_generations[0].name = "../foreign-payload".into();
	receipt.current_sha256 = checkpoint_digest(&receipt.current).unwrap();
	assert!(validate_receipt(&receipt, true).is_err());
}

#[tokio::test]
async fn crash_after_promotion_before_receipt_reconciles_original_pending_stream() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	{
		let mut tx = store.transaction(true, false).await.unwrap();
		tx.set(b"independent".to_vec(), b"survives crash".to_vec(), None)
			.await
			.unwrap();
		tx.commit().await.unwrap();
	}
	let lease = store.physical.admission.clone().write_owned().await;
	let deadline = Instant::now() + Duration::from_secs(60);
	let PreparedNative { plan, pending } = {
		let owner = store.db.current.read().unwrap();
		prepare_replacement(
			owner.as_ref().unwrap(),
			&store.options,
			directory.path(),
			deadline,
		)
		.unwrap()
	};
	let original = store.db.current.write().unwrap().take().unwrap();
	assert!(close_unique(original).is_ok());
	plan.promote(deadline, |promoted| {
		verify_existing(
			&store.options,
			promoted,
			&pending.current.engine_identity,
			&pending.current.fingerprint(),
			deadline,
		)
		.map_err(std::io::Error::other)
	})
	.unwrap();
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
	assert!(sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	drop(lease);
	drop(store);
	let reopened = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let receipt = read_previous_receipt(directory.path()).unwrap().unwrap();
	assert_eq!(receipt.sequence, pending.current.sequence);
	assert_eq!(receipt.engine_identity, pending.current.engine_identity);
	assert!(!sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	let mut tx = reopened.transaction(false, false).await.unwrap();
	assert_eq!(
		tx.get(b"independent".to_vec(), None).await.unwrap(),
		Some(b"survives crash".to_vec())
	);
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn failed_final_native_validation_preserves_retired_generation_and_replay_proof() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let lease = store.physical.admission.clone().write_owned().await;
	let deadline = Instant::now() + Duration::from_secs(60);
	let PreparedNative { plan, pending } = {
		let owner = store.db.current.read().unwrap();
		prepare_replacement(
			owner.as_ref().unwrap(),
			&store.options,
			directory.path(),
			deadline,
		)
		.unwrap()
	};
	let original = store.db.current.write().unwrap().take().unwrap();
	assert!(close_unique(original).is_ok());
	assert!(plan
		.promote(deadline, |_| Err(std::io::Error::other(
			"injected native validation failure"
		)))
		.is_err());
	assert!(!sibling_path(directory.path(), RECEIPT_SUFFIX)
		.unwrap()
		.exists());
	assert!(sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	drop(lease);
	drop(store);
	// Startup validates the exact pending stream before the helper unlinks the
	// retained original, then finalizes that original checkpoint sequence.
	let reopened = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	assert_eq!(
		read_previous_receipt(directory.path())
			.unwrap()
			.unwrap()
			.engine_identity,
		pending.current.engine_identity
	);
	assert!(!sibling_path(directory.path(), PENDING_SUFFIX)
		.unwrap()
		.exists());
	drop(reopened);
}

#[tokio::test]
async fn fresh_nested_endpoint_creates_owned_parents_before_stable_lock() {
	let root = TempDir::new().unwrap();
	let path = root.path().join("nested/parent/database");
	let store = Datastore::new(path.to_str().unwrap()).await.unwrap();
	assert!(path.join("IDENTITY").is_file());
	store.recall_physical_checkpoint(60_000).await.unwrap();
	assert!(sibling_path(&path, RECEIPT_SUFFIX).unwrap().is_file());
}

#[tokio::test]
async fn enabled_background_flush_follows_replacement_and_releases_last_owner() {
	let directory = TestDirectory::new();
	let store = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	let worker = super::super::spawn_background_flush(&store.db, Duration::from_millis(1), true);
	let original = engine_identity(directory.path()).unwrap();
	store.recall_physical_checkpoint(60_000).await.unwrap();
	assert_ne!(engine_identity(directory.path()).unwrap(), original);
	drop(store);
	worker.join().unwrap();
	let reopened = Datastore::new(directory.path().to_str().unwrap())
		.await
		.unwrap();
	assert!(read_previous_receipt(directory.path()).unwrap().is_some());
	drop(reopened);
}
