//! Deterministic actual router ownership and same-runtime native reopen controls.
#![cfg(feature = "test-utils")]

use elephant_storage::StorageClient;
#[cfg(feature = "rocksdb")]
use elephant_storage::StorageConfig;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn expected_failure<T, E>(result: Result<T, E>) -> Result<E, std::io::Error> {
    result
        .err()
        .ok_or_else(|| std::io::Error::other("expected operation to fail"))
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original client and clone alive through entered-write drainage, clone rejection and idempotent close; dropping them early would weaken the close contract."
)]
async fn local_close_waits_for_real_entered_write_and_rejects_all_clones() -> TestResult {
    let client = StorageClient::new_memory().await?;
    let (release, finished) =
        elephant_storage::test_utils::enter_local_close_write(&client).await?;
    let old = client.clone();
    let mut close = std::pin::pin!(client.close_owned_command_connection(10_000));
    tokio_test::assert_pending!(tokio_test::task::spawn(close.as_mut()).poll());
    assert!(old.query_checked("RETURN true").await.is_err());
    release
        .send(())
        .map_err(|()| std::io::Error::other("write owner already closed"))?;
    close.await?;
    finished.await??;
    old.close_owned_command_connection(10_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original client alive through timeout, cancelled waiter and actual writer completion; close must drain while the caller still owns its client."
)]
async fn close_timeout_and_cancelled_waiter_preserve_actual_write_owner() -> TestResult {
    let client = StorageClient::new_memory().await?;
    let (release, finished) =
        elephant_storage::test_utils::enter_local_close_write(&client).await?;
    let error = expected_failure(client.clone().close_owned_command_connection(1).await)?;
    assert!(error.to_string().contains("shutdown remains incomplete"));
    let mut cancelled = Box::pin(client.clone().close_owned_command_connection(10_000));
    tokio_test::assert_pending!(tokio_test::task::spawn(cancelled.as_mut()).poll());
    drop(cancelled);
    release
        .send(())
        .map_err(|()| std::io::Error::other("write owner already closed"))?;
    client.close_owned_command_connection(10_000).await?;
    finished.await??;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original client through exact query-error verification and explicit owned close; do not substitute implicit drop cleanup."
)]
async fn rejected_query_still_closes_without_rewriting_its_error() -> TestResult {
    let client = StorageClient::new_memory().await?;
    assert!(expected_failure(
        client
            .query_checked("THROW 'deliberate close control';")
            .await
    )?
    .to_string()
    .contains("deliberate close control"));
    client.close_owned_command_connection(10_000).await?;
    Ok(())
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original client, old SDK handle and reopened client through immediate reopen and retained-row assertions; close must release native ownership without dropping old handles."
)]
async fn local_close_releases_native_database_before_same_runtime_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = StorageConfig {
        url: format!("rocksdb://{}", directory.path().join("database").display()),
        ..StorageConfig::default()
    };
    let journal = directory.path().join("journal");
    let client =
        StorageClient::new_with_durability(config.clone(), None, Some(journal.clone())).await?;
    client.initialize_journal_identity().await?;
    client
        .query_checked("CREATE close_control:original SET value='retained';")
        .await?;
    let old = elephant_storage::test_utils::local_close_sdk_handle(&client)?;
    let (release, finished) =
        elephant_storage::test_utils::enter_local_close_write(&client).await?;
    let mut close = Box::pin(client.close_owned_command_connection(30_000));
    tokio_test::assert_pending!(tokio_test::task::spawn(close.as_mut()).poll());
    release
        .send(())
        .map_err(|()| std::io::Error::other("write owner already closed"))?;
    close.await?;
    finished.await??;
    // No retry, delay, runtime teardown, or removal of the old SDK handle.
    let reopened = StorageClient::new_with_durability(config, None, Some(journal)).await?;
    let mut response = reopened
        .query_checked("SELECT VALUE value FROM ONLY close_control:original;")
        .await?;
    assert_eq!(
        response.take::<Option<String>>(0)?.as_deref(),
        Some("retained")
    );
    assert!(old.query("RETURN true").await.is_err());
    reopened.close_owned_command_connection(30_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain client and SDK owners while the unread export blocks shutdown; only explicit stream release permits the later close."
)]
async fn local_close_does_not_acknowledge_backpressured_export_descendants() -> TestResult {
    let client = StorageClient::new_memory().await?;
    client
        .query_checked("CREATE close_export:original SET value='retained';")
        .await?;
    let sdk = elephant_storage::test_utils::local_close_sdk_handle(&client)?;
    // Export emits multiple header/schema/data chunks into bounded channels.
    // Keeping the stream unread prevents descendant completion deterministically.
    let backup = sdk.export(()).await?;
    let error = expected_failure(client.clone().close_owned_command_connection(1).await)?;
    assert!(error.to_string().contains("shutdown remains incomplete"));
    drop(backup);
    client.close_owned_command_connection(10_000).await?;
    Ok(())
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original and reopened clients through recovery failure diagnosis, immediate reopen and journal verification; teardown order is part of the regression."
)]
async fn failed_native_recovery_initialization_drains_before_immediate_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = StorageConfig {
        url: format!("rocksdb://{}", directory.path().join("database").display()),
        ..StorageConfig::default()
    };
    let original_journal = directory.path().join("original-journal");
    let client =
        StorageClient::new_with_durability(config.clone(), None, Some(original_journal.clone()))
            .await?;
    client.initialize_journal_identity().await?;
    client.close_owned_command_connection(30_000).await?;
    // The diagnosed constructor chooses its ordinary sibling journal, which
    // intentionally differs from the independently retained original above.
    let error = expected_failure(
        StorageClient::new_with_snapshot_destination_diagnosed(config.clone(), None).await,
    )?;
    assert_eq!(
        error.diagnostic().phase,
        elephant_storage::initialization_diagnostic::InitializationPhase::Recovery
    );
    assert!(std::error::Error::source(&error)
        .ok_or_else(|| std::io::Error::other("initialization error lost its original cause"))?
        .to_string()
        .contains("independently retained erasure journal"));
    let reopened = StorageClient::new_with_durability(config, None, Some(original_journal)).await?;
    reopened.verify_journal_identity().await?;
    reopened.close_owned_command_connection(30_000).await?;
    Ok(())
}
