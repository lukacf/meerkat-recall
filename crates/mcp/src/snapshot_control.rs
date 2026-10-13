//! Optional host runtime coordination for online MCP snapshots.
use crate::McpResult;
use async_trait::async_trait;
use elephant_storage::snapshot::{SnapshotManifest, SnapshotRetention};
use std::path::PathBuf;

/// Hosting runtimes pause admissions and drain their workers before storage capture.
///
/// Native embedders without a coordinator use the storage barrier directly; active
/// workers then cause an explicit conflict instead of an inconsistent export.
#[async_trait]
pub trait SnapshotCoordinator: Send + Sync {
    /// Drain and export a named backup, or restore it and replay the erasure journal.
    /// A failure keeps unsafe storage closed; the returned manifest proves the
    /// checked snapshot identity, not service or source-index readiness.
    async fn run(
        &self,
        path: PathBuf,
        name: String,
        retention: SnapshotRetention,
        restore: bool,
    ) -> McpResult<SnapshotManifest>;
}
