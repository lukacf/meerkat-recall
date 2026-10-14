//! Actual runtime transitions around configuration publication failures.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use super::*;
use elephant_storage::configuration_write_test_support::{inject_fault, Fault};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("recall-config-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        // Darwin may expose the system temp directory through a symlink.
        // Resolve only our owned fixture root; production still rejects links.
        Self(fs::canonicalize(path).unwrap())
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn patch(mode: RuntimeMode) -> RuntimeConfigPatch {
    serde_json::from_value(serde_json::json!({
        "runtime_mode": mode, "ingest_enabled": false, "extractor_required": false
    }))
    .unwrap()
}

async fn configured(root: &Path) -> RuntimeManager {
    let storage = StorageClient::new_embedded().await.unwrap();
    elephant_migrations::apply_schema(&storage).await.unwrap();
    let runtime = RuntimeManager::new_preconfigured(storage);
    {
        let mut inner = runtime.inner.write().await;
        inner.config_path = root.join("config/runtime.toml");
        let config = inner.config.as_mut().unwrap();
        config.extractor.required = false;
        config.runtime.mode = RuntimeMode::LocalMemory;
        save_config(&inner.config_path, inner.config.as_ref().unwrap()).unwrap();
    }
    runtime
}

fn awaiting_shutdown_pipeline() -> (ManagedPipeline, StdArc<AtomicBool>) {
    let (shutdown_tx, mut receiver) = watch::channel(false);
    let stopped = StdArc::new(AtomicBool::new(false));
    let observed = stopped.clone();
    let handle = thread::spawn(move || -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), receiver.changed()).await??;
            anyhow::ensure!(*receiver.borrow(), "worker did not receive shutdown");
            Ok::<(), anyhow::Error>(())
        })?;
        observed.store(true, Ordering::SeqCst);
        Ok(())
    });
    (
        ManagedPipeline {
            recipe: None,
            shutdown_tx,
            handle,
            stopped: stopped.clone(),
            last_error: StdArc::new(StdMutex::new(None)),
        },
        stopped,
    )
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn prepublication_failures_preserve_config_bytes_and_active_runtime() {
    for fault in [Fault::WriteEnospc, Fault::BeforeRename] {
        let root = TestDirectory::new();
        let runtime = configured(&root.0).await;
        let path = runtime.inner.read().await.config_path.clone();
        let original = fs::read(&path).unwrap();
        let storage = runtime.current_storage().await.unwrap();
        let original_store = storage.embedded_store().unwrap();
        let _fault = inject_fault(&path, fault).unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            runtime.configure_runtime(patch(RuntimeMode::SharedMemory)),
        )
        .await
        .expect("configuration attempt completed without deadlock")
        .expect_err("injected file failure");
        assert!(matches!(
            error.downcast_ref::<elephant_storage::ConfigWriteError>(),
            Some(elephant_storage::ConfigWriteError::BeforePublication(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(
            load_config_from_path(&path).unwrap().runtime.mode,
            RuntimeMode::LocalMemory
        );
        assert_eq!(
            runtime
                .inner
                .read()
                .await
                .config
                .as_ref()
                .unwrap()
                .runtime
                .mode,
            RuntimeMode::LocalMemory
        );
        assert!(!runtime.is_draining());
        assert!(runtime.get_readiness().await.storage_ready);
        assert!(StdArc::ptr_eq(
            &original_store,
            &runtime
                .current_storage()
                .await
                .unwrap()
                .embedded_store()
                .unwrap()
        ));
    }
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn uncertain_publication_stops_workers_and_requires_restart_without_rollback_claim() {
    let root = TestDirectory::new();
    let runtime = configured(&root.0).await;
    let path = runtime.inner.read().await.config_path.clone();
    let (pipeline, stopped) = awaiting_shutdown_pipeline();
    runtime.inner.write().await.pipeline = Some(pipeline);
    let _fault = inject_fault(&path, Fault::AfterRenameDirectorySync).unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        runtime.configure_runtime(patch(RuntimeMode::SharedMemory)),
    )
    .await
    .expect("held reconfigure lock did not deadlock worker drain")
    .expect_err("directory synchronization was uncertain");
    assert!(matches!(
        error.downcast_ref::<elephant_storage::ConfigWriteError>(),
        Some(elephant_storage::ConfigWriteError::CommitUncertain(_))
    ));
    assert_eq!(
        load_config_from_path(&path).unwrap().runtime.mode,
        RuntimeMode::SharedMemory
    );
    assert_eq!(
        runtime
            .inner
            .read()
            .await
            .config
            .as_ref()
            .unwrap()
            .runtime
            .mode,
        RuntimeMode::LocalMemory
    );
    assert!(runtime.is_draining());
    assert!(stopped.load(Ordering::SeqCst));
    assert!(runtime.workers_drained.load(Ordering::SeqCst));
    let status = runtime.get_readiness().await;
    assert!(!status.storage_ready && !status.worker_ready && !status.semantic_ingest_ready);
    assert!(status
        .issues
        .iter()
        .any(|issue| issue == "configuration_persistence_uncertain"));
    assert!(status
        .issues
        .iter()
        .any(|issue| issue == "configuration_restart_required"));
    assert!(runtime
        .configure_runtime(patch(RuntimeMode::LocalMemory))
        .await
        .is_err());
    assert!(runtime.start_workers_after_bind().await.is_err());
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn saved_configuration_with_failed_old_worker_stop_remains_non_ready() {
    let root = TestDirectory::new();
    let runtime = configured(&root.0).await;
    let path = runtime.inner.read().await.config_path.clone();
    let (shutdown_tx, _receiver) = watch::channel(false);
    runtime.inner.write().await.pipeline = Some(ManagedPipeline {
        recipe: None,
        shutdown_tx,
        handle: thread::spawn(|| Err(anyhow!("PRIVATE WORKER FAILURE CANARY"))),
        stopped: StdArc::new(AtomicBool::new(false)),
        last_error: StdArc::new(StdMutex::new(None)),
    });
    let error = runtime
        .configure_runtime(patch(RuntimeMode::SharedMemory))
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("PRIVATE"));
    assert_eq!(
        load_config_from_path(&path).unwrap().runtime.mode,
        RuntimeMode::SharedMemory
    );
    assert_eq!(
        runtime
            .inner
            .read()
            .await
            .config
            .as_ref()
            .unwrap()
            .runtime
            .mode,
        RuntimeMode::LocalMemory
    );
    assert!(runtime.is_draining());
    assert!(!runtime.workers_drained.load(Ordering::SeqCst));
    let status = runtime.get_readiness().await;
    assert!(!status.storage_ready && !status.worker_ready && !status.semantic_ingest_ready);
    assert!(status
        .issues
        .iter()
        .any(|issue| issue == "worker_drain_failed"));
    assert!(status
        .issues
        .iter()
        .any(|issue| issue == "configuration_activation_failed"));
    assert!(runtime.persist_shutdown_snapshot().await.is_err());
}

#[tokio::test]
#[serial_test::serial(cwd_env)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
)]
async fn bootstrap_publication_failure_never_activates_new_configuration() {
    for (fault, uncertain) in [
        (Fault::BeforeRename, false),
        (Fault::AfterRenameDirectorySync, true),
    ] {
        let root = TestDirectory::new();
        let runtime =
            RuntimeManager::new_preconfigured(StorageClient::new_embedded().await.unwrap());
        let path = root.0.join("config/runtime.toml");
        {
            let mut inner = runtime.inner.write().await;
            inner.config_path = path.clone();
            inner.config = None;
            inner.storage = None;
            inner.status = RuntimeStatus::bootstrap();
        }
        let mut request = patch(RuntimeMode::SharedMemory);
        request.backend = Some(StorageBackendKind::ElephantDb);
        request.snapshot_path = Some(root.0.join("bootstrap.snapshot"));
        let _fault = inject_fault(&path, fault).unwrap();
        runtime
            .initialize_runtime(request)
            .await
            .expect_err("injected publication failure");
        assert!(runtime.inner.read().await.config.is_none());
        assert!(!runtime.get_readiness().await.storage_ready);
        assert_eq!(runtime.is_draining(), uncertain);
        assert_eq!(path.exists(), uncertain);
        if uncertain {
            assert_eq!(
                load_config_from_path(&path).unwrap().runtime.mode,
                RuntimeMode::SharedMemory
            );
        }
    }
}
