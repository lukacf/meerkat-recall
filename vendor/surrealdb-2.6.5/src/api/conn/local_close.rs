//! Per-connection local owner drain. This is never a remote shutdown protocol.
use crate::api::{err::Error, Result};
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::watch;
use tokio_util::task::TaskTracker;

// This test-only observer identifies the actual router-owned datastore without
// making the SDK response/error type graph contain a concrete datastore and its
// transaction admission guards. The erased observer still owns only a Weak;
// upgrading it never extends the datastore lifetime beyond the entered test.
#[cfg(feature = "recall-close-test-utils")]
trait TestDatastoreObserver: std::fmt::Debug + Send + Sync {
    fn upgrade(&self) -> Option<Arc<surrealdb_core::kvs::Datastore>>;
}

#[cfg(feature = "recall-close-test-utils")]
impl TestDatastoreObserver for std::sync::Weak<surrealdb_core::kvs::Datastore> {
    fn upgrade(&self) -> Option<Arc<surrealdb_core::kvs::Datastore>> {
        std::sync::Weak::upgrade(self)
    }
}

#[derive(Debug)]
pub(crate) struct LocalClose {
    owners: TaskTracker,
    #[cfg(feature = "recall-close-test-utils")]
    datastore: std::sync::Mutex<Option<Box<dyn TestDatastoreObserver>>>,
    #[cfg(feature = "recall-close-test-utils")]
    datastore_ready: tokio::sync::Notify,
    owner_failed: AtomicBool,
    completion: watch::Sender<Option<std::result::Result<(), String>>>,
}
impl LocalClose {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            owners: TaskTracker::new(),
            #[cfg(feature = "recall-close-test-utils")]
            datastore: std::sync::Mutex::new(None),
            #[cfg(feature = "recall-close-test-utils")]
            datastore_ready: tokio::sync::Notify::new(),
            owner_failed: AtomicBool::new(false),
            completion: watch::channel(None).0,
        })
    }
    #[cfg(feature = "recall-close-test-utils")]
    pub(crate) fn attach_datastore(&self, datastore: &Arc<surrealdb_core::kvs::Datastore>) {
        *self.datastore.lock().unwrap() = Some(Box::new(Arc::downgrade(datastore)));
        self.datastore_ready.notify_waiters();
    }
    #[cfg(feature = "recall-close-test-utils")]
    pub(crate) async fn enter_write(
        self: &Arc<Self>,
    ) -> Result<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<Result<()>>,
    )> {
        let datastore = loop {
            let ready = self.datastore_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(datastore) = self
                .datastore
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|observer| observer.upgrade())
            {
                break datastore;
            }
            ready.await;
        };
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        self.spawn(async move {
            let result = async {
                let txn = datastore
                    .transaction(
                        surrealdb_core::kvs::TransactionType::Write,
                        surrealdb_core::kvs::LockType::Optimistic,
                    )
                    .await?;
                let _ = entered_tx.send(());
                // Dropping the test release channel still finishes by rollback.
                if release_rx.await.is_ok() {
                    txn.commit().await?;
                } else {
                    txn.cancel().await?;
                }
                Ok(())
            }
            .await;
            let _ = finished_tx.send(result);
        });
        entered_rx
            .await
            .map_err(|_| Error::InternalError("test write did not enter".into()))?;
        Ok((release_tx, finished_rx))
    }
    // Descendants enter the same tracker before their parent owner leaves it.
    // Unlike JoinSet, completed owners release memory without a growing result list.
    pub(crate) fn spawn(self: &Arc<Self>, future: impl Future<Output = ()> + Send + 'static) {
        let state = self.clone();
        self.owners.spawn(async move {
            if AssertUnwindSafe(future).catch_unwind().await.is_err() {
                state.owner_failed.store(true, Ordering::Release);
            }
        });
    }
    pub(crate) async fn drain(&self) -> Result<()> {
        self.owners.close();
        self.owners.wait().await;
        if self.owner_failed.load(Ordering::Acquire) {
            return Err(Error::InternalError(
                "local connection owner panicked during drain".into(),
            )
            .into());
        }
        Ok(())
    }
    pub(crate) fn complete(&self, result: Result<()>) {
        let outcome = result.map_err(|error| error.to_string());
        if let Err(error) = &outcome {
            tracing::error!(%error, "local connection shutdown failed");
        }
        self.completion.send_replace(Some(outcome));
    }
    pub(crate) async fn wait(&self) -> Result<()> {
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result.map_err(|error| Error::InternalError(error).into());
            }
            completion
                .changed()
                .await
                .map_err(|_| Error::InternalError("local shutdown owner disappeared".into()))?;
        }
    }
}
