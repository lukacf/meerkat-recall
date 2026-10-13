//! Per-result diagnostics for the existing storage initialization path.
//!
//! Only fixed phase/kind labels and observed OS error numbers are exported.
//! Original storage errors and their classifications are retained unchanged.

use std::io;

use serde::Serialize;

use crate::StorageError;

/// Phase reached by the original initialization attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitializationPhase {
    /// Validate storage configuration.
    Configuration,
    /// Resolve snapshot load and destination identity.
    SnapshotPath,
    /// Acquire exclusive ownership of the local store.
    WriterLock,
    /// Resolve the independent durability journal.
    JournalPath,
    /// Open and validate the erasure journal.
    JournalOpen,
    /// Check journal separation from replaceable storage.
    JournalPlacement,
    /// Acquire remote store ownership through the journal.
    RemoteWriterLock,
    /// Connect to the native SurrealDB engine.
    NativeConnect,
    /// Authenticate the native connection.
    NativeAuthentication,
    /// Select the native namespace and database.
    NativeNamespace,
    /// Open and replay embedded durable state.
    EmbeddedReplay,
    /// Attach the immutable source reference guard.
    ReferenceGuard,
    /// Verify native reference capabilities.
    ReferenceCapability,
    /// Apply existing recovery and restore barriers.
    Recovery,
}

/// Existing storage error classification, without its private text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageFailureKind {
    /// Preserved connection error category.
    Connection,
    /// Preserved query error category.
    Query,
    /// Preserved transaction error category.
    Transaction,
    /// Preserved record-not-found error category.
    NotFound,
    /// Preserved conflict error category.
    Conflict,
    /// Preserved erasure reservation error category.
    ErasureInProgress,
    /// Preserved serialization error category.
    Serialization,
    /// Preserved data integrity error category.
    DataIntegrity,
    /// Preserved migration error category.
    Migration,
    /// Preserved type error category.
    Type,
    /// Preserved SurrealDB driver error category.
    Surreal,
}

/// Safe diagnostic metadata. No path, URL, query, payload or error text is kept
/// in this representation. A missing errno means none was observed, not zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageInitializationDiagnostic {
    /// Storage phase containing the returned failure.
    pub phase: InitializationPhase,
    /// Original typed storage error category.
    pub storage_error_kind: StorageFailureKind,
    /// Fixed operation label for a directly propagated I/O error.
    pub io_operation: Option<&'static str>,
    /// Fixed standard I/O error-kind label when retained.
    pub io_error_kind: Option<&'static str>,
    /// Actual OS errno when the original I/O error carries one.
    pub raw_os_error: Option<i32>,
}

/// Original initialization failure with separately serializable safe metadata.
#[derive(Debug, thiserror::Error)]
#[error("storage initialization failed")]
pub struct StorageInitializationError {
    diagnostic: StorageInitializationDiagnostic,
    #[source]
    source: StorageError,
}

impl StorageInitializationError {
    /// Return metadata suitable for an operator-facing error response.
    #[must_use]
    pub const fn diagnostic(&self) -> &StorageInitializationDiagnostic {
        &self.diagnostic
    }

    pub(crate) const fn new(phase: InitializationPhase, source: StorageError) -> Self {
        Self {
            diagnostic: StorageInitializationDiagnostic {
                phase,
                storage_error_kind: error_kind(&source),
                io_operation: None,
                io_error_kind: None,
                raw_os_error: None,
            },
            source,
        }
    }

    // Construct this only at the boundary returning this exact I/O failure.
    // The metadata travels with the Err itself: no ambient state or comparison
    // with private error text can associate it with another failure.
    pub(crate) fn from_io(
        phase: InitializationPhase,
        source: StorageError,
        cause: &io::Error,
        operation: &'static str,
    ) -> Self {
        let mut error = Self::new(phase, source);
        error.diagnostic.io_operation = Some(operation);
        error.diagnostic.io_error_kind = Some(io_kind(cause.kind()));
        error.diagnostic.raw_os_error = cause.raw_os_error();
        error
    }

    pub(crate) const fn lock_would_block(phase: InitializationPhase, source: StorageError) -> Self {
        let mut error = Self::new(phase, source);
        // std::fs::TryLockError::WouldBlock has no underlying errno value.
        error.diagnostic.io_operation = Some("writer_lock_acquire");
        error.diagnostic.io_error_kind = Some("would_block");
        error
    }

    pub(crate) fn into_storage_error(self) -> StorageError {
        self.source
    }
}

const fn io_kind(kind: io::ErrorKind) -> &'static str {
    match kind {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::AlreadyExists => "already_exists",
        io::ErrorKind::WouldBlock => "would_block",
        io::ErrorKind::InvalidInput => "invalid_input",
        io::ErrorKind::InvalidData => "invalid_data",
        io::ErrorKind::Interrupted => "interrupted",
        io::ErrorKind::TimedOut => "timed_out",
        io::ErrorKind::WriteZero => "write_zero",
        io::ErrorKind::UnexpectedEof => "unexpected_eof",
        _ => "other",
    }
}

const fn error_kind(error: &StorageError) -> StorageFailureKind {
    match error {
        StorageError::ConnectionError(_) => StorageFailureKind::Connection,
        StorageError::QueryError(_) => StorageFailureKind::Query,
        StorageError::TransactionError(_) => StorageFailureKind::Transaction,
        StorageError::NotFound(_) => StorageFailureKind::NotFound,
        StorageError::Conflict(_) => StorageFailureKind::Conflict,
        StorageError::ErasureInProgress => StorageFailureKind::ErasureInProgress,
        StorageError::SerializationError(_) => StorageFailureKind::Serialization,
        StorageError::DataIntegrityError(_) => StorageFailureKind::DataIntegrity,
        StorageError::MigrationError(_) => StorageFailureKind::Migration,
        StorageError::TypeError(_) => StorageFailureKind::Type,
        StorageError::Surreal(_) => StorageFailureKind::Surreal,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    fn original_failure() -> StorageError {
        StorageError::ConnectionError("identical private failure".into())
    }

    fn observed(phase: InitializationPhase, errno: i32) -> StorageInitializationError {
        StorageInitializationError::from_io(
            phase,
            original_failure(),
            &io::Error::from_raw_os_error(errno),
            "test_io",
        )
    }

    #[tokio::test]
    async fn explicit_results_isolate_lookalike_nested_concurrent_and_cancelled_failures() {
        let barrier = tokio::sync::Barrier::new(2);
        let one = |phase, errno| {
            let barrier = &barrier;
            async move {
                let result = Err::<(), _>(observed(phase, errno));
                barrier.wait().await;
                tokio::task::yield_now().await;
                result
            }
        };
        let (first, second) = tokio::join!(
            one(InitializationPhase::WriterLock, 2),
            one(InitializationPhase::JournalOpen, 13),
        );
        let first = first.expect_err("first original failure");
        let second = second.expect_err("second original failure");
        assert_eq!(first.diagnostic().phase, InitializationPhase::WriterLock);
        assert_eq!(first.diagnostic().raw_os_error, Some(2));
        assert_eq!(second.diagnostic().phase, InitializationPhase::JournalOpen);
        assert_eq!(second.diagnostic().raw_os_error, Some(13));

        // Identical Display text is deliberately not an error identity.
        let caught = observed(InitializationPhase::WriterLock, 13);
        let same_text = caught.source.to_string();
        drop(caught);
        let distinct =
            StorageInitializationError::new(InitializationPhase::WriterLock, original_failure());
        assert_eq!(distinct.source.to_string(), same_text);
        assert!(distinct.diagnostic().raw_os_error.is_none());
        assert!(distinct.diagnostic().io_operation.is_none());

        let outer = observed(InitializationPhase::SnapshotPath, 2);
        let inner = async {
            tokio::task::yield_now().await;
            observed(InitializationPhase::RemoteWriterLock, 13)
        }
        .await;
        drop(inner);
        assert_eq!(outer.diagnostic().phase, InitializationPhase::SnapshotPath);
        assert_eq!(outer.diagnostic().raw_os_error, Some(2));

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let cancelled = tokio::spawn(async move {
            let held = observed(InitializationPhase::WriterLock, 13);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
            held
        });
        started_rx
            .await
            .expect("cancelled future reached its failure");
        cancelled.abort();
        assert!(cancelled
            .await
            .expect_err("future cancelled")
            .is_cancelled());
        let after =
            StorageInitializationError::new(InitializationPhase::Recovery, original_failure());
        assert!(after.diagnostic().raw_os_error.is_none());
        assert!(matches!(
            after.into_storage_error(),
            StorageError::ConnectionError(_)
        ));
    }
}
