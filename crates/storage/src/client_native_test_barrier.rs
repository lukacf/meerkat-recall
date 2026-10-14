//! Private test constructor for a deterministic in-transaction loopback barrier.
//! No production constructor can select this capability configuration.

#![allow(clippy::expect_used)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::*;

impl StorageClient {
    /// Open an isolated native memory engine with exactly one loopback target.
    ///
    /// The caller owns the bound ephemeral listener and its bounded lifecycle.
    /// Original journal/reference/recovery initialization is shared with normal
    /// clients; this method does not fabricate readiness or an accepted proof.
    pub(crate) async fn new_native_with_test_barrier(
        config: StorageConfig,
        journal_root: PathBuf,
        barrier: SocketAddr,
    ) -> StorageResult<Self> {
        if config.backend != StorageBackendKind::SurrealDb
            || config.url != "mem://"
            || config.snapshot_load.is_some()
            || barrier.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST)
            || barrier.port() == 0
        {
            return Err(StorageError::ConnectionError(
                "native test barrier requires mem:// and one bound 127.0.0.1 port".into(),
            ));
        }
        config.validate_timeouts()?;
        // SDK defaults deny every network target. A host plus an explicit port
        // produces one exact Host target, never an IP network or wildcard.
        let capabilities = surrealdb::opt::capabilities::Capabilities::default()
            .with_net_target_allowed(barrier.to_string())
            .map_err(|error| StorageError::ConnectionError(error.to_string()))?;
        let engine_config = native_engine_config(&config).capabilities(capabilities);
        Self::open_with_engine_configuration(config, None, Some(journal_root), engine_config)
            .await
            .map_err(
                crate::initialization_diagnostic::StorageInitializationError::into_storage_error,
            )
    }
}

#[tokio::test]
async fn native_barrier_rejects_nonexact_target_or_backend_before_journal_io() {
    let directory = tempfile::tempdir().expect("test directory");
    let journal = directory.path().join("must-not-exist");
    for address in [
        "0.0.0.0:49152",
        "127.0.0.2:49152",
        "127.0.0.1:0",
        "[::1]:49152",
    ] {
        let error = StorageClient::new_native_with_test_barrier(
            StorageConfig::default(),
            journal.clone(),
            address.parse().expect("literal socket address"),
        )
        .await
        .expect_err("only exact nonzero IPv4 loopback may be allowed");
        assert!(matches!(error, StorageError::ConnectionError(_)));
        assert!(!journal.exists());
    }
    for config in [
        StorageConfig {
            url: "ws://127.0.0.1:49152".into(),
            ..StorageConfig::default()
        },
        StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        },
        StorageConfig {
            snapshot_load: Some(directory.path().join("not-a-snapshot")),
            ..StorageConfig::default()
        },
    ] {
        assert!(StorageClient::new_native_with_test_barrier(
            config,
            journal.clone(),
            "127.0.0.1:49152".parse().expect("literal socket address"),
        )
        .await
        .is_err());
        assert!(!journal.exists());
    }
}

#[tokio::test]
async fn native_barrier_capability_keeps_other_ports_and_default_client_denied() {
    let directory = tempfile::tempdir().expect("test directory");
    let allowed = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("owned ephemeral listener");
    let denied = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("distinct owned ephemeral listener");
    let allowed_address = allowed.local_addr().expect("owned address");
    let denied_address = denied.local_addr().expect("owned address");
    assert_ne!(allowed_address, denied_address);
    let config = StorageConfig {
        database: format!(
            "native_barrier_capability_{}",
            uuid::Uuid::new_v4().simple()
        ),
        query_timeout: std::time::Duration::from_secs(2),
        transaction_timeout: std::time::Duration::from_secs(2),
        ..StorageConfig::default()
    };
    let scoped = StorageClient::new_native_with_test_barrier(
        config.clone(),
        directory.path().join("scoped-journal"),
        allowed_address,
    )
    .await
    .expect("isolated scoped native engine");
    let ordinary = StorageClient::new_with_durability(
        config,
        None,
        Some(directory.path().join("ordinary-journal")),
    )
    .await
    .expect("ordinary native engine");
    for (client, address) in [(&scoped, denied_address), (&ordinary, allowed_address)] {
        let error = client
            .query_bind_checked(
                "RETURN http::get($url);",
                crate::bindings! { "url" => format!("http://{address}/owned-barrier") },
            )
            .await
            .expect_err("unapproved endpoint must be refused before connect");
        assert!(error.to_string().contains("not allowed"), "{error}");
    }
    // Nothing serves these listeners. A capability bypass must fail the error
    // assertion above rather than count a connection timeout as authorization.
    // The positive approved in-TX call belongs to the independent race oracle.
}
