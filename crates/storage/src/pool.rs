//! Connection pooling with per-server pools and LRU session management.
//!
//! This module provides:
//! - Per-server connection pools for multi-tenant deployments
//! - LRU-based (namespace, database) session caching
//! - Automatic session expiration and cleanup

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::client::{StorageClient, StorageConfig};
use crate::error::{StorageError, StorageResult};

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of different servers (URLs) the pool can connect to.
    /// This limits the total number of server pools, not connections per server.
    pub max_server_pools: usize,
    /// Maximum cached sessions per server pool.
    /// Each session represents a (namespace, database) combination.
    pub max_sessions_per_server: usize,
    /// Session idle timeout before eviction.
    pub session_idle_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_server_pools: 10,
            max_sessions_per_server: 100,
            session_idle_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Key for session cache lookup.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SessionKey {
    /// Namespace.
    pub namespace: String,
    /// Database.
    pub database: String,
}

impl SessionKey {
    /// Creates a new session key.
    #[must_use]
    pub fn new(namespace: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            database: database.into(),
        }
    }
}

/// A cached session entry with LRU tracking.
#[derive(Debug)]
struct SessionEntry {
    /// The storage client for this session.
    client: StorageClient,
    /// Last access time for LRU eviction.
    last_access: Instant,
}

/// Per-server connection pool with LRU session cache.
#[derive(Debug)]
struct ServerPool {
    /// LRU cache of (namespace, database) -> client sessions.
    sessions: HashMap<SessionKey, SessionEntry>,
    /// Pool configuration.
    pool_config: PoolConfig,
}

impl ServerPool {
    /// Creates a new server pool.
    fn new(pool_config: PoolConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            pool_config,
        }
    }

    /// Checks if a valid session exists for the key.
    fn get_cached_session(&mut self, key: &SessionKey) -> Option<StorageClient> {
        if let Some(entry) = self.sessions.get_mut(key) {
            if entry.last_access.elapsed() < self.pool_config.session_idle_timeout {
                entry.last_access = Instant::now();
                return Some(entry.client.clone());
            }
            // Session expired, remove it
            self.sessions.remove(key);
        }
        None
    }

    /// Prepares room for a new session (evicts if necessary).
    /// Returns true if there's room, false if already at capacity and couldn't evict.
    fn prepare_for_new_session(&mut self) -> bool {
        // Evict oldest sessions if at capacity
        while self.sessions.len() >= self.pool_config.max_sessions_per_server {
            if !self.evict_oldest_session() {
                return false;
            }
        }
        true
    }

    /// Inserts a new session entry.
    fn insert_session(&mut self, key: SessionKey, client: StorageClient) {
        self.sessions.insert(
            key,
            SessionEntry {
                client,
                last_access: Instant::now(),
            },
        );
    }

    /// Evicts the oldest (least recently used) session.
    /// Returns true if a session was evicted, false if there were no sessions to evict.
    fn evict_oldest_session(&mut self) -> bool {
        if let Some(oldest_key) = self
            .sessions
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(k, _)| k.clone())
        {
            self.sessions.remove(&oldest_key);
            true
        } else {
            false
        }
    }

    /// Removes expired sessions.
    fn cleanup_expired(&mut self) {
        let timeout = self.pool_config.session_idle_timeout;
        self.sessions
            .retain(|_, entry| entry.last_access.elapsed() < timeout);
    }
}

/// Multi-server connection pool manager.
///
/// Manages connection pools for multiple `SurrealDB` servers with LRU-based
/// session caching per (namespace, database) combination.
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    /// Per-server pools keyed by server URL.
    pools: Arc<RwLock<HashMap<String, ServerPool>>>,
    /// Pool configuration.
    config: PoolConfig,
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new(PoolConfig::default())
    }
}

impl ConnectionPool {
    /// Creates a new connection pool with the given configuration.
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Gets or creates a client for the given server and session.
    ///
    /// This method maintains per-server pools with LRU session caching.
    /// Sessions are reused when possible and evicted based on idle timeout.
    ///
    /// # Concurrency Notes
    ///
    /// This implementation uses a three-phase approach to minimize lock contention:
    /// 1. First, try to get a cached session and check capacity limits
    /// 2. If cache miss, create client outside the lock (expensive I/O)
    /// 3. Re-acquire lock and insert the session
    ///
    /// Concurrent requests for the same uncached session may both create clients,
    /// with one being discarded. This is acceptable for correctness and avoids
    /// holding locks during I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if the server pool limit is reached or if connecting
    /// to the database fails.
    pub async fn get_client(
        &self,
        server_config: &StorageConfig,
        session_key: &SessionKey,
    ) -> StorageResult<StorageClient> {
        let server_url = &server_config.url;

        // Phase 1: Try to get cached session and verify capacity (quick lock)
        {
            let mut pools = self.pools.write().await;
            if let Some(pool) = pools.get_mut(server_url) {
                if let Some(client) = pool.get_cached_session(session_key) {
                    return Ok(client);
                }
                // Cache miss - prepare room but don't create client under lock
                pool.prepare_for_new_session();
            } else {
                // Server pool doesn't exist - check if we can create one
                if pools.len() >= self.config.max_server_pools {
                    return Err(StorageError::ConnectionError(
                        "Maximum server pool limit reached".to_string(),
                    ));
                }
            }
        } // Lock released

        // Phase 2: Create new client outside the lock (expensive I/O)
        let mut config = server_config.clone();
        config.namespace.clone_from(&session_key.namespace);
        config.database.clone_from(&session_key.database);

        let client = Box::pin(StorageClient::new(config)).await?;

        // Phase 3: Insert client into pool (re-acquire lock)
        {
            let mut pools = self.pools.write().await;

            // Create server pool if needed (re-check limit in case of concurrent creation)
            if !pools.contains_key(server_url) {
                if pools.len() >= self.config.max_server_pools {
                    // Another task filled capacity while we were creating client
                    // Return the client anyway - it's usable, just not cached
                    return Ok(client);
                }
                pools.insert(server_url.clone(), ServerPool::new(self.config.clone()));
            }

            // Insert the session (may overwrite if another task created one concurrently)
            if let Some(pool) = pools.get_mut(server_url) {
                pool.insert_session(session_key.clone(), client.clone());
            }
        }

        Ok(client)
    }

    /// Removes a specific session from the pool.
    pub async fn remove_session(&self, server_url: &str, session_key: &SessionKey) {
        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(server_url) {
            pool.sessions.remove(session_key);
        }
    }

    /// Cleans up expired sessions across all pools.
    pub async fn cleanup(&self) {
        let mut pools = self.pools.write().await;
        for pool in pools.values_mut() {
            pool.cleanup_expired();
        }
    }

    /// Returns statistics about the current pool state.
    pub async fn stats(&self) -> PoolStats {
        let pools = self.pools.read().await;
        let mut total_sessions = 0;
        let mut server_stats = Vec::new();

        for (url, pool) in pools.iter() {
            let session_count = pool.sessions.len();
            total_sessions += session_count;
            server_stats.push(ServerStats {
                url: url.clone(),
                session_count,
            });
        }

        PoolStats {
            server_count: pools.len(),
            total_sessions,
            servers: server_stats,
        }
    }
}

/// Statistics for a single server pool.
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// Server URL.
    pub url: String,
    /// Number of cached sessions.
    pub session_count: usize,
}

/// Overall pool statistics.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Number of server pools.
    pub server_count: usize,
    /// Total cached sessions across all servers.
    pub total_sessions: usize,
    /// Per-server statistics.
    pub servers: Vec<ServerStats>,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_session_key_equality() {
        let key1 = SessionKey::new("ns1", "db1");
        let key2 = SessionKey::new("ns1", "db1");
        let key3 = SessionKey::new("ns1", "db2");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_server_pools, 10);
        assert_eq!(config.max_sessions_per_server, 100);
        assert_eq!(config.session_idle_timeout, Duration::from_secs(300));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_connection_pool_creation() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let stats = pool.stats().await;
        assert_eq!(stats.server_count, 0);
        assert_eq!(stats.total_sessions, 0);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_connection_pool_get_client() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let config = StorageConfig::default(); // Uses mem://
        let session_key = SessionKey::new("test_ns", "test_db");

        let result = pool.get_client(&config, &session_key).await;
        assert!(result.is_ok());

        let stats = pool.stats().await;
        assert_eq!(stats.server_count, 1);
        assert_eq!(stats.total_sessions, 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_connection_pool_session_reuse() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let config = StorageConfig::default();
        let session_key = SessionKey::new("test_ns", "test_db");

        // Get client twice with same key
        let client1 = pool.get_client(&config, &session_key).await.unwrap();
        let client2 = pool.get_client(&config, &session_key).await.unwrap();

        // Should have same session
        let stats = pool.stats().await;
        assert_eq!(stats.total_sessions, 1);

        // Verify both clients work
        assert!(client1.health_check().await.is_ok());
        assert!(client2.health_check().await.is_ok());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_connection_pool_multiple_sessions() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let config = StorageConfig::default();

        let key1 = SessionKey::new("ns1", "db1");
        let key2 = SessionKey::new("ns1", "db2");
        let key3 = SessionKey::new("ns2", "db1");

        pool.get_client(&config, &key1).await.unwrap();
        pool.get_client(&config, &key2).await.unwrap();
        pool.get_client(&config, &key3).await.unwrap();

        let stats = pool.stats().await;
        assert_eq!(stats.server_count, 1);
        assert_eq!(stats.total_sessions, 3);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn test_connection_pool_remove_session() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let config = StorageConfig::default();
        let session_key = SessionKey::new("test_ns", "test_db");

        pool.get_client(&config, &session_key).await.unwrap();

        let stats = pool.stats().await;
        assert_eq!(stats.total_sessions, 1);

        pool.remove_session(&config.url, &session_key).await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_sessions, 0);
    }
}
