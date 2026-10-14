//! Private implementation controls, additional to the real storage workspace tests.
use super::*;

#[tokio::test]
async fn worker_restart_recovers_metadata_without_reopening_ordinary_transactions() {
	let ds = Arc::new(Datastore::new("memory").await.unwrap());
	let session = Session::owner().with_ns("physical").with_db("test");
	for response in ds
		.execute(
			"DEFINE INDEX deferred ON entries FIELDS value DEFER;",
			&session,
			None,
		)
		.await
		.unwrap()
	{
		response.result.unwrap();
	}
	let unfinished = ds.index_builder.pause_for_physical().await.unwrap();
	let exclusive = ds
		.transaction_factory
		.recall_physical
		.transactions
		.clone()
		.write_owned()
		.await;
	let restart_view = ds.transaction_factory.physical_restart_view(&exclusive);
	let query_ds = ds.clone();
	let mut query = tokio::spawn(async move {
		let tx = query_ds.transaction(Read, Optimistic).await?;
		tx.cancel().await
	});
	assert!(tokio::time::timeout(Duration::from_millis(10), &mut query)
		.await
		.is_err());
	let context = ds.setup_ctx().unwrap().freeze();
	let options = ds.setup_options(&Session::owner());
	tokio::time::timeout(
		Duration::from_secs(1),
		ds.index_builder
			.resume_after_physical(&context, &options, unfinished, &restart_view),
	)
	.await
	.unwrap()
	.unwrap();
	// Metadata reads completed under exclusive ownership, while both queued
	// ordinary transactions and the restarted worker still use the original gate.
	assert!(tokio::time::timeout(Duration::from_millis(10), &mut query)
		.await
		.is_err());
	drop(restart_view);
	drop(exclusive);
	tokio::time::timeout(Duration::from_secs(1), query)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
	ds.recall_physical_checkpoint(1_000).await.unwrap();
}

#[tokio::test]
async fn hnsw_cache_generation_changes_only_after_old_transaction_drops() {
	let ds = Arc::new(Datastore::new("memory").await.unwrap());
	let session = Session::owner().with_ns("physical").with_db("test");
	for response in ds.execute("CREATE points:one SET vector=[1,0]; DEFINE INDEX knn ON points FIELDS vector HNSW DIMENSION 2;", &session, None).await.unwrap() {
		response.result.unwrap();
	}
	let tx = Arc::new(ds.transaction(Read, Optimistic).await.unwrap());
	let definition = tx
		.get_tb_index("physical", "test", "points", "knn")
		.await
		.unwrap();
	let Index::Hnsw(parameters) = &definition.index else {
		panic!("expected HNSW");
	};
	let mut context = ds.setup_ctx().unwrap();
	context.set_transaction(tx.clone());
	let context = context.freeze();
	let options = ds.setup_options(&session);
	let old = ds
		.index_stores
		.get_index_hnsw(&context, &options, &definition, parameters)
		.await
		.unwrap();
	tx.cancel().await.unwrap();
	let owner = ds.clone();
	let mut checkpoint = tokio::spawn(async move { owner.recall_physical_checkpoint(5_000).await });
	assert!(
		tokio::time::timeout(Duration::from_millis(10), &mut checkpoint)
			.await
			.is_err()
	);
	let still_old = ds
		.index_stores
		.get_index_hnsw(&context, &options, &definition, parameters)
		.await
		.unwrap();
	assert!(Arc::ptr_eq(&old, &still_old));
	drop(still_old);
	drop(context);
	drop(tx);
	checkpoint.await.unwrap().unwrap();
	let tx = Arc::new(ds.transaction(Read, Optimistic).await.unwrap());
	let mut context = ds.setup_ctx().unwrap();
	context.set_transaction(tx.clone());
	let fresh = ds
		.index_stores
		.get_index_hnsw(&context.freeze(), &options, &definition, parameters)
		.await
		.unwrap();
	assert!(!Arc::ptr_eq(&old, &fresh));
	tx.cancel().await.unwrap();
}

#[tokio::test]
async fn dropping_checkpoint_waiter_keeps_exact_core_owner_until_deadline() {
	let ds = Arc::new(Datastore::new("memory").await.unwrap());
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let owner = ds.clone();
	let request = tokio::spawn(async move { owner.recall_physical_checkpoint(100).await });
	tokio::time::timeout(Duration::from_secs(1), async {
		while ds
			.transaction_factory
			.recall_physical
			.owner
			.try_lock()
			.is_ok()
		{
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	request.abort();
	assert!(request.await.unwrap_err().is_cancelled());
	tokio::time::timeout(Duration::from_secs(1), async {
		while !ds
			.transaction_factory
			.recall_physical
			.failed
			.load(Ordering::Acquire)
		{
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	tx.cancel().await.unwrap();
	drop(tx);
	assert!(ds.transaction(Read, Optimistic).await.is_err());
}

#[tokio::test]
async fn expired_staging_constructor_refuses_before_creating_native_owner_or_directory() {
	let root = temp_dir::TempDir::new().unwrap();
	let path = root.path().join("never-opened");
	let error = Datastore::recall_open_staging(
		&format!("rocksdb:{}", path.display()),
		Instant::now() - Duration::from_millis(1),
	)
	.await
	.err()
	.unwrap();
	assert!(error
		.to_string()
		.contains("original physical maintenance budget exhausted"));
	assert!(!path.exists());
	assert!(!root
		.path()
		.join(".never-opened.recall-native-owner.lock")
		.exists());
}
