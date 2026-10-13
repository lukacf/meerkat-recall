//! The private exporter is a service-lifetime task, never a public route.
use elephant_storage::{private_primary_evidence::PrivatePrimaryEvidence, StorageClient};
use std::{future::Future, sync::Arc, time::Duration};

#[expect(
    clippy::large_enum_variant,
    clippy::redundant_pub_crate,
    reason = "Keep the private provider boundary and existing inline storage layout."
)]
pub(super) enum StorageProvider {
    Managed(Arc<crate::runtime::RuntimeManager>),
    Fixed(StorageClient),
}

#[expect(
    clippy::redundant_pub_crate,
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
pub(super) async fn serve<F>(
    worker: Option<PrivatePrimaryEvidence>,
    storage: StorageProvider,
    server: F,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    let Some(mut worker) = worker else {
        return server.await;
    };
    let evidence = async move {
        loop {
            let current = match &storage {
                StorageProvider::Managed(runtime) => runtime.current_storage().await,
                StorageProvider::Fixed(client) => Some(client.clone()),
            };
            if let Some(client) = current {
                worker.poll(&client).await?;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };
    supervise(server, evidence).await
}

/// A qualification capture failure retires its original worker, not the API.
/// Keep the server future and its original result; never recreate the exporter
/// or lend its consumed capability to a replacement task.
async fn supervise<F, E>(server: F, evidence: E) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
    E: Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(server);
    let evidence_result = {
        tokio::pin!(evidence);
        tokio::select! {
            result = &mut server => return result,
            result = &mut evidence => result,
        }
    };
    // The completed evidence future (and its capability/storage handle) has
    // been dropped before continuing to serve. Its immutable incomplete reply
    // still determines capture failure for the qualification consumer.
    match evidence_result {
        Ok(()) => tracing::warn!("Private qualification evidence worker stopped"),
        Err(error) => tracing::warn!(%error, "Private qualification evidence worker failed"),
    }
    server.await
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::supervise;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::oneshot;

    struct WorkerLifetime(Arc<AtomicUsize>);

    impl Drop for WorkerLifetime {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn failed_capture_retires_worker_and_preserves_running_server_result() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let lifetime = WorkerLifetime(drops.clone());
        let worker_attempts = attempts.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let evidence = async move {
            let _lifetime = lifetime;
            worker_attempts.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("private evidence: capture_incomplete"))
        };
        let server = async move {
            let _ = started_tx.send(());
            finish_rx.await?;
            Err(anyhow::anyhow!("original_server_failure"))
        };
        let task = tokio::spawn(supervise(server, evidence));
        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx)
            .await
            .expect("server remains available")
            .expect("server started");
        // The server may be polled before the failed worker. Let the select
        // finish handling that original failure, without advancing a clock.
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        finish_tx.send(()).expect("server was not cancelled");
        let error = task
            .await
            .expect("supervisor task joined")
            .expect_err("original server result is preserved");
        assert_eq!(error.to_string(), "original_server_failure");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn server_completion_cancels_the_original_private_worker() {
        let drops = Arc::new(AtomicUsize::new(0));
        let lifetime = WorkerLifetime(drops.clone());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let evidence = async move {
            let _lifetime = lifetime;
            let _ = started_tx.send(());
            std::future::pending::<anyhow::Result<()>>().await
        };
        let server = async move {
            finish_rx.await?;
            Ok(())
        };
        let task = tokio::spawn(supervise(server, evidence));
        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx)
            .await
            .expect("original worker is polled")
            .expect("worker started");
        finish_tx.send(()).expect("server still running");
        task.await
            .expect("supervisor task joined")
            .expect("original server success");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
