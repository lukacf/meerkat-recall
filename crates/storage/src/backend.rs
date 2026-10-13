//! Storage backend abstraction.
//!
//! This trait exists to decouple repositories and higher-level services from a
//! concrete database client. The production implementation delegates to
//! `StorageClient` (SurrealDB), while tests can supply lightweight backends.

use async_trait::async_trait;

use crate::client::StorageClient;
use crate::embedded::EmbeddedStore;
use crate::error::{StorageError, StorageResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Minimal storage interface used by repositories.
///
/// This is intentionally close to `StorageClient`'s query surface so existing
/// repositories can be parameterized without being rewritten.
#[async_trait]
pub trait StorageBackend: Clone + Send + Sync + 'static {
    /// Returns the embedded store when the underlying client is using `elephant-db`.
    fn embedded_store(&self) -> Option<Arc<EmbeddedStore>> {
        None
    }

    /// Executes a raw `SurrealQL` query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query(&self, sql: &str) -> StorageResult<surrealdb::Response>;

    /// Executes a `SurrealQL` query with bound parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query_bind<T>(&self, sql: &str, bindings: T) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Send + Sync + 'static;

    /// Checked + retried variant of [`StorageBackend::query_bind`]:
    ///
    /// statement-level errors inside the response are surfaced (via
    /// [`surrealdb::Response::check`]) instead of silently succeeding, and
    /// transient transaction conflicts are retried. Bindings must be `Clone`
    /// so the request can be replayed on a conflict.
    ///
    /// Repositories should use this for every mutation; see
    /// `StorageClient::query_bind_checked` for the checking and retry
    /// semantics.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails, if any statement in the response
    /// failed, or if the conflict persists after all retries.
    async fn query_bind_checked<T>(
        &self,
        sql: &str,
        bindings: T,
    ) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Clone + Send + Sync + 'static;
}

/// Production backend backed by a real `SurrealDB` connection.
#[derive(Clone, Debug)]
pub struct SurrealStorageBackend {
    client: StorageClient,
}

impl SurrealStorageBackend {
    /// Wrap an existing [`StorageClient`].
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Access the underlying [`StorageClient`].
    #[must_use]
    pub const fn client(&self) -> &StorageClient {
        &self.client
    }
}

#[async_trait]
impl StorageBackend for SurrealStorageBackend {
    fn embedded_store(&self) -> Option<Arc<EmbeddedStore>> {
        self.client.embedded_store()
    }

    async fn query(&self, sql: &str) -> StorageResult<surrealdb::Response> {
        self.client.query(sql).await
    }

    async fn query_bind<T>(&self, sql: &str, bindings: T) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Send + Sync + 'static,
    {
        self.client.query_bind(sql, bindings).await
    }

    async fn query_bind_checked<T>(
        &self,
        sql: &str,
        bindings: T,
    ) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Clone + Send + Sync + 'static,
    {
        // Delegates to the client's checked path, which surfaces
        // statement-level errors and retries transient conflicts with the
        // shared backoff logic.
        self.client.query_bind_checked(sql, bindings).await
    }
}

/// Test-friendly backend backed by an in-memory `HashMap`.
///
/// This is **not** a SurrealQL interpreter; use it directly or via mock
/// repositories in unit/integration tests where a real DB would be too slow.
#[derive(Clone, Debug, Default)]
pub struct MockStorageBackend {
    records: Arc<RwLock<HashMap<elephant_types::RecordId, serde_json::Value>>>,
}

impl MockStorageBackend {
    /// Create an empty mock backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a record.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails (should be rare for `Value`).
    pub async fn create_json(
        &self,
        id: elephant_types::RecordId,
        record: serde_json::Value,
    ) -> StorageResult<()> {
        let mut guard = self.records.write().await;
        if guard.contains_key(&id) {
            return Err(StorageError::Conflict(format!(
                "Record already exists: {id}"
            )));
        }
        guard.insert(id, record);
        drop(guard);
        Ok(())
    }

    /// Fetch a record by id.
    #[must_use]
    pub async fn get_json(&self, id: &elephant_types::RecordId) -> Option<serde_json::Value> {
        self.records.read().await.get(id).cloned()
    }

    /// Update an existing record.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the record does not exist.
    pub async fn update_json(
        &self,
        id: &elephant_types::RecordId,
        record: serde_json::Value,
    ) -> StorageResult<()> {
        let mut guard = self.records.write().await;
        if !guard.contains_key(id) {
            return Err(StorageError::NotFound(format!("Record not found: {id}")));
        }
        guard.insert(id.clone(), record);
        drop(guard);
        Ok(())
    }

    /// Delete a record.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the record does not exist.
    pub async fn delete(&self, id: &elephant_types::RecordId) -> StorageResult<()> {
        let removed = {
            let mut guard = self.records.write().await;
            guard.remove(id)
        };
        if removed.is_none() {
            return Err(StorageError::NotFound(format!("Record not found: {id}")));
        }
        Ok(())
    }

    /// List all records currently stored.
    #[must_use]
    pub async fn list_all_json(&self) -> Vec<serde_json::Value> {
        self.records.read().await.values().cloned().collect()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MockStorageBackend {
    /// Start building a [`MockStorageBackend`] pre-populated with records.
    #[must_use]
    pub fn builder() -> MockStorageBackendBuilder {
        MockStorageBackendBuilder {
            records: HashMap::new(),
        }
    }
}

/// Builder for [`MockStorageBackend`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct MockStorageBackendBuilder {
    records: HashMap<elephant_types::RecordId, serde_json::Value>,
}

#[cfg(any(test, feature = "test-utils"))]
impl MockStorageBackendBuilder {
    /// Add an Entity record.
    ///
    /// # Panics
    ///
    /// Panics if the entity cannot be serialized to JSON (should not happen for
    /// Elephant record types).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn with_entity(mut self, entity: elephant_types::records::Entity) -> Self {
        self.records.insert(
            entity.base.id.clone(),
            serde_json::to_value(entity).expect("serialize Entity"),
        );
        self
    }

    /// Build the backend.
    #[must_use]
    pub fn build(self) -> MockStorageBackend {
        MockStorageBackend {
            records: Arc::new(RwLock::new(self.records)),
        }
    }
}

#[async_trait]
impl StorageBackend for MockStorageBackend {
    async fn query(&self, _sql: &str) -> StorageResult<surrealdb::Response> {
        Err(StorageError::QueryError(
            "MockStorageBackend does not support SurrealQL; use mock repositories".to_string(),
        ))
    }

    async fn query_bind<T>(&self, _sql: &str, _bindings: T) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Send + Sync + 'static,
    {
        Err(StorageError::QueryError(
            "MockStorageBackend does not support SurrealQL; use mock repositories".to_string(),
        ))
    }

    async fn query_bind_checked<T>(
        &self,
        _sql: &str,
        _bindings: T,
    ) -> StorageResult<surrealdb::Response>
    where
        T: serde::Serialize + Clone + Send + Sync + 'static,
    {
        Err(StorageError::QueryError(
            "MockStorageBackend does not support SurrealQL; use mock repositories".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::MockStorageBackend;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::Entity;
    use elephant_types::RecordId;

    #[tokio::test]
    async fn mock_storage_backend_basic_crud() {
        let backend = MockStorageBackend::new();
        let id = RecordId::new_v4(TableName::Entity);

        let v1 = serde_json::json!({"id": id.to_string(), "display_name": "Alice"});
        backend
            .create_json(id.clone(), v1.clone())
            .await
            .expect("create");
        assert_eq!(backend.get_json(&id).await, Some(v1));

        let v2 = serde_json::json!({"id": id.to_string(), "display_name": "Bob"});
        backend.update_json(&id, v2.clone()).await.expect("update");
        assert_eq!(backend.get_json(&id).await, Some(v2));

        backend.delete(&id).await.expect("delete");
        assert_eq!(backend.get_json(&id).await, None);
    }

    #[test]
    fn mock_storage_backend_builder_populates_records() {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(
            id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        let entity = Entity::new(base, EntityType::new("person"), "Alice".to_string());

        let backend = MockStorageBackend::builder().with_entity(entity).build();
        let contains = {
            let stored = backend.records.try_read().expect("read lock");
            stored.contains_key(&id)
        };
        assert!(contains);
    }
}
