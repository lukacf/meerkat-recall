//! One storage admission barrier shared by repositories, background work and maintenance.
use crate::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Storage admission phase shared by every adapter and background worker.
pub enum RecoveryPhase {
    /// Normal operations may acquire admission.
    Ready,
    /// Erasure replay or exclusive physical erasure is in progress.
    Replaying,
    /// Snapshot quiescence or export is in progress.
    Exporting,
    /// Explicit restore or bounded transfer is in progress.
    Restoring,
    /// Recovery must complete before ordinary operations resume.
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Payload-free storage readiness detail.
pub struct RecoveryStatus {
    /// Whether ordinary storage admission is currently allowed.
    pub ready: bool,
    /// Current maintenance or recovery phase.
    pub phase: RecoveryPhase,
    /// Bounded operational explanation, without source payload or provider errors.
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RecoveryBarrier {
    gate: Arc<RwLock<()>>,
    status: Mutex<RecoveryStatus>,
    restart_required: AtomicBool,
}

impl Default for RecoveryBarrier {
    fn default() -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            restart_required: AtomicBool::new(false),
            status: Mutex::new(RecoveryStatus {
                ready: true,
                phase: RecoveryPhase::Ready,
                last_error: None,
            }),
        }
    }
}

impl RecoveryBarrier {
    pub(crate) fn status(&self) -> RecoveryStatus {
        self.status.lock().map_or_else(
            |_| RecoveryStatus {
                ready: false,
                phase: RecoveryPhase::Failed,
                last_error: Some("storage readiness lock poisoned".into()),
            },
            |status| status.clone(),
        )
    }

    pub(crate) fn set(&self, phase: RecoveryPhase, last_error: Option<String>) {
        if let Ok(mut status) = self.status.lock() {
            if self.restart_required.load(Ordering::Acquire) && phase != RecoveryPhase::Failed {
                return;
            }
            *status = RecoveryStatus {
                ready: phase == RecoveryPhase::Ready,
                phase,
                last_error,
            };
        }
    }

    /// An ambiguous filesystem commit cannot be reconciled by same-process restore.
    /// Only constructing a new barrier after exclusive durable replay clears this state.
    pub(crate) fn fail_until_restart(&self, message: String) {
        self.restart_required.store(true, Ordering::Release);
        self.set(RecoveryPhase::Failed, Some(message));
    }

    #[allow(clippy::unused_async)] // Async admission interface, with immediate maintenance refusal.
    pub(crate) async fn admit(&self) -> StorageResult<OwnedRwLockReadGuard<()>> {
        if !self.status().ready {
            return Err(StorageError::Conflict(
                "storage recovery/maintenance is incomplete".into(),
            ));
        }
        let guard = self
            .gate
            .clone()
            .try_read_owned()
            .map_err(|_| StorageError::Conflict("storage maintenance is pending".into()))?;
        if !self.status().ready {
            return Err(StorageError::Conflict(
                "storage recovery/maintenance is incomplete".into(),
            ));
        }
        Ok(guard)
    }

    pub(crate) async fn exclusive(
        &self,
        timeout: Duration,
    ) -> StorageResult<OwnedRwLockWriteGuard<()>> {
        tokio::time::timeout(timeout, self.gate.clone().write_owned())
            .await
            .map_err(|_| StorageError::Conflict("storage quiesce deadline exceeded".into()))
    }
}

/// Owns the exclusive gate through serialization/replay and handles task cancellation.
pub(crate) struct MaintenanceGuard {
    barrier: Arc<RecoveryBarrier>,
    guard: Arc<OwnedRwLockWriteGuard<()>>,
    restore: bool,
    complete: bool,
}

impl RecoveryBarrier {
    pub(crate) async fn enter(
        self: &Arc<Self>,
        phase: RecoveryPhase,
        timeout: Duration,
    ) -> StorageResult<MaintenanceGuard> {
        let previous = {
            let mut status = self
                .status
                .lock()
                .map_err(|_| StorageError::Conflict("storage readiness lock poisoned".into()))?;
            if self.restart_required.load(Ordering::Acquire) {
                return Err(StorageError::Conflict(
                    "storage durability requires restart recovery".into(),
                ));
            }
            if !(status.ready
                || matches!(phase, RecoveryPhase::Restoring | RecoveryPhase::Replaying)
                    && status.phase == RecoveryPhase::Failed)
            {
                return Err(StorageError::Conflict(
                    "storage recovery/maintenance is incomplete".into(),
                ));
            }
            let previous = status.clone();
            *status = RecoveryStatus {
                ready: false,
                phase,
                last_error: None,
            };
            previous
        };
        // Publish nonreadiness before draining. Existing task-local activities can
        // finish their inner operations; new admissions receive an immediate refusal.
        let mut pending = PendingMaintenance {
            barrier: self.clone(),
            previous,
            phase,
            acquired: false,
        };
        let guard = self.exclusive(timeout).await?;
        pending.acquired = true;
        Ok(MaintenanceGuard {
            barrier: self.clone(),
            guard: Arc::new(guard),
            restore: matches!(phase, RecoveryPhase::Restoring | RecoveryPhase::Replaying),
            complete: false,
        })
    }
}

struct PendingMaintenance {
    barrier: Arc<RecoveryBarrier>,
    previous: RecoveryStatus,
    phase: RecoveryPhase,
    acquired: bool,
}

impl Drop for PendingMaintenance {
    fn drop(&mut self) {
        if !self.acquired {
            if let Ok(mut status) = self.barrier.status.lock() {
                if status.phase == self.phase {
                    *status = self.previous.clone();
                }
            }
        }
    }
}

impl MaintenanceGuard {
    pub(crate) fn lease(&self) -> Arc<OwnedRwLockWriteGuard<()>> {
        self.guard.clone()
    }

    pub(crate) fn finish(mut self) {
        if matches!(
            self.barrier.status().phase,
            RecoveryPhase::Exporting | RecoveryPhase::Restoring | RecoveryPhase::Replaying
        ) {
            self.barrier.set(RecoveryPhase::Ready, None);
        }
        self.complete = true;
    }
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        if !self.complete {
            if self.restore {
                self.barrier.set(
                    RecoveryPhase::Failed,
                    Some("restore/erasure interrupted; complete recovery before serving".into()),
                );
            } else if self.barrier.status().phase == RecoveryPhase::Exporting {
                self.barrier.set(RecoveryPhase::Ready, None);
            }
        }
    }
}

tokio::task_local! {
    static ACTIVITY: ActivityLease;
}

#[derive(Clone)]
struct ActivityLease {
    barrier: Arc<RecoveryBarrier>,
    guard: Arc<OwnedRwLockReadGuard<()>>,
}

/// Owned admission survives cancelled callers while an SDK operation is still executing.
pub(crate) struct StorageAdmission {
    guard: Arc<OwnedRwLockReadGuard<()>>,
}

impl RecoveryBarrier {
    pub(crate) fn in_activity(self: &Arc<Self>) -> bool {
        ACTIVITY
            .try_with(|activity| Arc::ptr_eq(&activity.barrier, self))
            .unwrap_or(false)
    }

    pub(crate) async fn admit_operation(self: &Arc<Self>) -> StorageResult<StorageAdmission> {
        if self.restart_required.load(Ordering::Acquire) {
            return Err(StorageError::Conflict(
                "storage durability requires restart recovery".into(),
            ));
        }
        if let Ok(Some(guard)) = ACTIVITY.try_with(|activity| {
            Arc::ptr_eq(&activity.barrier, self).then(|| activity.guard.clone())
        }) {
            return Ok(StorageAdmission { guard });
        }
        Ok(StorageAdmission {
            guard: Arc::new(self.admit().await?),
        })
    }

    pub(crate) async fn activity<F: std::future::Future>(
        self: &Arc<Self>,
        future: F,
    ) -> StorageResult<F::Output> {
        let admission = self.admit_operation().await?;
        let activity = ActivityLease {
            barrier: self.clone(),
            guard: admission.guard,
        };
        Ok(ACTIVITY.scope(activity, future).await)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn whole_operation_drains_without_nested_read_deadlock() {
        let barrier = Arc::new(RecoveryBarrier::default());
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (continue_work, continuing) = tokio::sync::oneshot::channel();
        let worker_barrier = barrier.clone();
        let operation = tokio::spawn(async move {
            worker_barrier
                .activity(async {
                    entered.send(()).unwrap();
                    continuing.await.unwrap();
                    let _nested = worker_barrier.admit_operation().await.unwrap();
                })
                .await
                .unwrap();
        });
        entry.await.unwrap();
        let writer_barrier = barrier.clone();
        let maintenance = tokio::spawn(async move {
            writer_barrier
                .enter(RecoveryPhase::Exporting, Duration::from_secs(2))
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while barrier.status().ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!maintenance.is_finished());
        assert!(!barrier.status().ready, "quiescing is already unavailable");
        assert!(
            barrier.admit_operation().await.is_err(),
            "new work must fail promptly while draining"
        );
        continue_work.send(()).unwrap();
        operation.await.unwrap();
        let exclusive = maintenance.await.unwrap();
        assert!(!barrier.status().ready);
        exclusive.finish();
        assert!(barrier.status().ready);
    }

    #[tokio::test]
    async fn interrupted_restore_keeps_storage_closed() {
        let barrier = Arc::new(RecoveryBarrier::default());
        let guard = barrier
            .enter(RecoveryPhase::Restoring, Duration::from_secs(1))
            .await
            .unwrap();
        drop(guard);
        assert!(!barrier.status().ready);
        assert!(barrier.admit_operation().await.is_err());
    }
}
