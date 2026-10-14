//! Exercise the SDK's real periodic database writer across snapshot maintenance.
#![allow(clippy::expect_used)]

use super::*;
use std::time::{Duration, Instant};

#[tokio::test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "The fixture retains both maintenance leases across the full export observation."
)]
async fn native_background_pause_preserves_export_inventory_and_resumes_writes() {
    let root = tempfile::tempdir().expect("test directory");
    let config = StorageConfig::default();
    // This invokes the actual SDK changefeed worker, including its timestamp
    // writes even when the database has no configured changefeed.
    let engine = native_engine_config(&config).changefeed_gc_interval(Duration::from_millis(20));
    let client = StorageClient::open_with_engine_configuration(
        config,
        None,
        Some(root.path().join("journal")),
        engine,
    )
    .await
    .expect("isolated native engine");
    client
        .query_checked("CREATE snapshot_pause_probe:first SET value = 1;")
        .await
        .expect("real database row");
    let deadline = Instant::now() + Duration::from_secs(10);
    assert!(client.pause_native_background(deadline).await.is_err());
    let foreground = client
        .begin_maintenance(RecoveryPhase::Exporting, Duration::from_secs(2))
        .await
        .expect("drain foreground first");
    let privileged = client.maintenance_client_with_guard(&foreground);
    let background = privileged
        .pause_native_background(deadline)
        .await
        .expect("drain SDK writers")
        .expect("native engine has a shared gate");
    let before = crate::source_history_restore::inventory(&privileged, deadline)
        .await
        .expect("original inventory");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let bytes = privileged
        .snapshot_bytes(&root.path().join("probe.snapshot"))
        .await
        .expect("actual native export while periodic ticks are pending");
    assert!(!bytes.is_empty());
    let after = crate::source_history_restore::inventory(&privileged, deadline)
        .await
        .expect("post-export inventory");
    assert_eq!(before.fingerprint, after.fingerprint);
    drop(background);
    // Only the SDK worker can write now: foreground maintenance is still held.
    // Prove it resumes, rather than accidentally disabling maintenance forever.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let resumed = crate::source_history_restore::inventory(&privileged, deadline)
                .await
                .expect("inventory after releasing SDK gate");
            if resumed.fingerprint != after.fingerprint {
                break;
            }
        }
    })
    .await
    .expect("the real SDK changefeed writer must resume");
    drop(privileged);
    foreground.finish();
    client
        .query_checked("UPDATE snapshot_pause_probe:first SET value = 2;")
        .await
        .expect("ordinary writes resume after maintenance");
    crate::snapshot::export_named(
        &client,
        &root.path().join("named"),
        "background-maintenance",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .expect("normal named export uses the same SDK gate");
}
