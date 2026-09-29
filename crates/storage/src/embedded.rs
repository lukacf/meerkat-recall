#![allow(clippy::unused_async)]
//! Async wrapper over the sync `elephant-db` kernel.

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use elephant_db::{
    ElephantDb, ElephantDbError, EntityRelations, GraphNeighbor, NodeEmbeddingSearchHit,
    SnapshotImportOptions,
};
use elephant_types::records::{Assertion, DocNode};
use elephant_types::records::{WorkItem, WorkItemStatus};
use elephant_types::RecordId;
use tokio::sync::RwLock;
use tracing::info;

use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, SearchCriteria};

/// Embedded record trait re-export.
pub use elephant_db::ElephantDbRecord as EmbeddedRecord;
pub use elephant_db::{cosine_similarity, hashed_text_embedding, lexical_score};

/// Embedded mutation view re-export.
pub type EmbeddedMutation<'a> = elephant_db::ElephantDbTx<'a>;

/// Shared in-memory store backing the embedded `elephant-db` backend.
#[derive(Debug, Clone, Default)]
pub struct EmbeddedStore {
    db: Arc<ElephantDb>,
    import_barrier: Arc<RwLock<()>>,
}

impl EmbeddedStore {
    async fn with_import_read_guard<R, F>(&self, f: F) -> StorageResult<R>
    where
        F: FnOnce(&ElephantDb) -> StorageResult<R>,
    {
        let _guard = self.import_barrier.read().await;
        f(&self.db)
    }

    fn ensure_snapshot_operation_quiescent(&self, operation: &str) -> StorageResult<()> {
        let running = self
            .db
            .scan::<WorkItem>()?
            .into_iter()
            .filter(|item| item.base.deleted_at.is_none() && item.status == WorkItemStatus::Running)
            .count();
        if running > 0 {
            return Err(StorageError::Conflict(format!(
                "Cannot {operation} snapshot while {running} embedded work item(s) are still running; pause or drain workers first"
            )));
        }
        Ok(())
    }

    fn ensure_snapshot_payload_has_no_running_work(db: &ElephantDb) -> StorageResult<()> {
        let running = db
            .scan::<WorkItem>()?
            .into_iter()
            .filter(|item| item.base.deleted_at.is_none() && item.status == WorkItemStatus::Running)
            .count();
        if running > 0 {
            return Err(StorageError::Conflict(format!(
                "Cannot import snapshot containing {running} running work item(s); drain workers or export after leases complete"
            )));
        }
        Ok(())
    }

    fn parse_importable_snapshot(
        bytes: &[u8],
        options: SnapshotImportOptions,
    ) -> StorageResult<ElephantDb> {
        let db = ElephantDb::from_snapshot_bytes_with_options(bytes, options)?;
        Self::ensure_snapshot_payload_has_no_running_work(&db)?;
        Ok(db)
    }

    /// Creates an empty embedded store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            db: Arc::new(ElephantDb::new()),
            import_barrier: Arc::new(RwLock::new(())),
        }
    }

    /// Creates a store from a snapshot file.
    pub async fn from_snapshot(path: impl AsRef<Path>) -> StorageResult<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading embedded Elephant snapshot");
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|err| StorageError::SerializationError(err.to_string()))?;
        info!(
            path = %path.display(),
            bytes = bytes.len(),
            "Parsing embedded Elephant snapshot"
        );
        let db = Self::parse_importable_snapshot(&bytes, snapshot_import_options_from_env())?;
        info!(path = %path.display(), "Loaded embedded Elephant snapshot");
        Ok(Self {
            db: Arc::new(db),
            import_barrier: Arc::new(RwLock::new(())),
        })
    }

    /// Exports the current in-memory state to a JSON snapshot.
    pub async fn export_snapshot(&self, path: impl AsRef<Path>) -> StorageResult<()> {
        let bytes = self.export_snapshot_bytes().await?;
        tokio::fs::write(path, bytes)
            .await
            .map_err(|err| StorageError::SerializationError(err.to_string()))
    }

    /// Imports a snapshot into the live in-memory store, replacing all state.
    pub async fn import_snapshot(&self, path: impl AsRef<Path>) -> StorageResult<()> {
        let _guard = self.import_barrier.write().await;
        self.ensure_snapshot_operation_quiescent("import")?;
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|err| StorageError::SerializationError(err.to_string()))?;
        let validated = Self::parse_importable_snapshot(&bytes, SnapshotImportOptions::default())?;
        self.db.import_snapshot_db(validated)?;
        Ok(())
    }

    /// Exports the current in-memory state to JSON bytes.
    pub async fn export_snapshot_bytes(&self) -> StorageResult<Vec<u8>> {
        let _guard = self.import_barrier.write().await;
        {
            let db = &self.db;
            let bytes = db.export_snapshot_bytes()?;
            sanitize_running_work_items_for_snapshot_export(&bytes)
        }
    }

    /// Imports a snapshot from JSON bytes, replacing all state.
    pub async fn import_snapshot_bytes(&self, bytes: &[u8]) -> StorageResult<()> {
        let _guard = self.import_barrier.write().await;
        self.ensure_snapshot_operation_quiescent("import")?;
        let validated = Self::parse_importable_snapshot(bytes, SnapshotImportOptions::default())?;
        self.db.import_snapshot_db(validated).map_err(Into::into)
    }

    /// Returns the set of logical table names with at least one stored record.
    pub async fn table_names(&self) -> StorageResult<Vec<String>> {
        self.with_import_read_guard(|db| db.table_names().map_err(Into::into))
            .await
    }

    /// Applies a set of in-memory mutations atomically.
    pub async fn mutate<R, F>(&self, f: F) -> StorageResult<R>
    where
        F: FnOnce(&mut EmbeddedMutation<'_>) -> StorageResult<R> + Send,
    {
        let _guard = self.import_barrier.read().await;
        self.db.tx(f)
    }

    /// Returns `true` when the store contains no records.
    pub async fn is_empty(&self) -> StorageResult<bool> {
        self.with_import_read_guard(|db| db.is_empty().map_err(Into::into))
            .await
    }

    /// Creates a record in the embedded store.
    pub async fn create<T>(&self, record: T) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        self.db.create(record).map_err(Into::into)
    }

    /// Replaces an existing record.
    pub async fn replace<T>(&self, record: T) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        self.db.replace(record).map_err(Into::into)
    }

    /// Replaces an existing record after validating the target row inside one
    /// embedded transaction. The target `space_id` and `id` always win over any
    /// payload values carried by `record`.
    pub async fn update_existing<T>(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: T,
    ) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        record.base_mut().space_id.clone_from(&target_space_id);
        record.base_mut().id.clone_from(&target_id);

        self.db.tx(move |tx| {
            let Some(_existing) = tx.get::<T>(&target_space_id, &target_id, false)? else {
                return Err(StorageError::NotFound(target_id.to_string()));
            };
            tx.replace(record).map_err(Into::into)
        })
    }

    /// Replaces an existing record in any lifecycle state inside one embedded
    /// transaction. Unlike `update_existing`, this permits replacing a
    /// soft-deleted row with an exact snapshot that may remain deleted.
    pub async fn replace_existing_any_state<T>(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: T,
    ) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        record.base_mut().space_id.clone_from(&target_space_id);
        record.base_mut().id.clone_from(&target_id);

        self.db.tx(move |tx| {
            let Some(_existing) = tx.get::<T>(&target_space_id, &target_id, true)? else {
                return Err(StorageError::NotFound(target_id.to_string()));
            };
            tx.replace(record).map_err(Into::into)
        })
    }

    /// Replaces an existing record and clears any soft-delete flag inside one
    /// embedded transaction. The target `space_id` and `id` always win over any
    /// payload values carried by `record`.
    pub async fn restore_and_replace_existing<T>(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: T,
    ) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        record.base_mut().space_id.clone_from(&target_space_id);
        record.base_mut().id.clone_from(&target_id);
        record.base_mut().deleted_at = None;

        self.db.tx(move |tx| {
            let Some(_existing) = tx.get::<T>(&target_space_id, &target_id, true)? else {
                return Err(StorageError::NotFound(target_id.to_string()));
            };
            tx.replace(record).map_err(Into::into)
        })
    }

    /// Gets a record by ID, optionally including soft-deleted rows.
    pub async fn get<T>(
        &self,
        space_id: &str,
        id: &RecordId,
        include_deleted: bool,
    ) -> StorageResult<Option<T>>
    where
        T: EmbeddedRecord,
    {
        self.with_import_read_guard(|db| db.get(space_id, id, include_deleted).map_err(Into::into))
            .await
    }

    /// Gets a record by globally unique ID, optionally including soft-deleted rows.
    pub async fn get_by_id<T>(
        &self,
        id: &RecordId,
        include_deleted: bool,
    ) -> StorageResult<Option<T>>
    where
        T: EmbeddedRecord,
    {
        self.with_import_read_guard(|db| db.get_by_id(id, include_deleted).map_err(Into::into))
            .await
    }

    /// Finds an assertion by its unique assertion key using the embedded index.
    pub async fn assertion_by_key(
        &self,
        space_id: &str,
        assertion_key: &str,
    ) -> StorageResult<Option<Assertion>> {
        self.with_import_read_guard(|db| {
            db.assertion_by_key(space_id, assertion_key)
                .map_err(Into::into)
        })
        .await
    }

    /// Gets multiple records by ID while preserving the requested order.
    pub async fn get_many<T>(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> StorageResult<Vec<T>>
    where
        T: EmbeddedRecord,
    {
        self.with_import_read_guard(|db| {
            db.get_many(space_id, ids, include_deleted)
                .map_err(Into::into)
        })
        .await
    }

    /// Hard deletes a record.
    pub async fn hard_delete<T>(&self, space_id: &str, id: &RecordId) -> StorageResult<()>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        match self.db.hard_delete::<T>(space_id, id) {
            Ok(()) | Err(ElephantDbError::NotFound(_)) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Soft deletes a record by setting `deleted_at`.
    pub async fn soft_delete<T>(&self, space_id: &str, id: &RecordId) -> StorageResult<()>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        self.db.tx(move |tx| {
            let Some(mut record) = tx.get::<T>(&target_space_id, &target_id, true)? else {
                return Ok(());
            };
            let now = Utc::now();
            record.base_mut().deleted_at = Some(now);
            record.base_mut().updated_at = now;
            let _ = tx.replace(record)?;
            Ok(())
        })
    }

    /// Restores a soft-deleted record.
    pub async fn restore<T>(&self, space_id: &str, id: &RecordId) -> StorageResult<T>
    where
        T: EmbeddedRecord,
    {
        let _guard = self.import_barrier.read().await;
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        self.db.tx(move |tx| {
            let Some(mut record) = tx.get::<T>(&target_space_id, &target_id, true)? else {
                return Err(StorageError::NotFound(target_id.to_string()));
            };
            record.base_mut().deleted_at = None;
            record.base_mut().updated_at = Utc::now();
            tx.replace(record).map_err(Into::into)
        })
    }

    /// Lists records by table and `space_id`.
    pub async fn list<T>(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
        include_deleted: bool,
    ) -> StorageResult<Vec<T>>
    where
        T: EmbeddedRecord,
    {
        let mut rows = self.scan::<T>().await?;
        rows.retain(|row| row.base().space_id == space_id);
        if !include_deleted {
            rows.retain(|row| row.base().deleted_at.is_none());
        }
        rows.sort_by_key(|row| row.base().id.to_string());
        if let Some(cursor) = cursor {
            let cursor_value = cursor.to_string();
            rows.retain(|row| row.base().id.to_string() > cursor_value);
        }
        if rows.len() > limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    /// Scans an entire logical table.
    pub async fn scan<T>(&self) -> StorageResult<Vec<T>>
    where
        T: EmbeddedRecord,
    {
        self.with_import_read_guard(|db| db.scan::<T>().map_err(Into::into))
            .await
    }

    /// Lists work items by status using the embedded kernel's JSON prefilter.
    pub async fn work_items_by_status(
        &self,
        space_id: &str,
        status: WorkItemStatus,
        kind: Option<&str>,
        limit: usize,
        runnable_at: Option<DateTime<Utc>>,
    ) -> StorageResult<Vec<WorkItem>> {
        self.with_import_read_guard(|db| {
            db.work_items_by_status(space_id, &status.to_string(), kind, limit, runnable_at)
                .map_err(Into::into)
        })
        .await
    }

    /// Returns matching document IDs in stable order using the cached doc index.
    pub async fn search_doc_ids(
        &self,
        space_id: &str,
        query: Option<&str>,
        include_deleted: bool,
    ) -> StorageResult<Vec<RecordId>> {
        self.with_import_read_guard(|db| {
            db.search_doc_ids(space_id, query, include_deleted)
                .map_err(Into::into)
        })
        .await
    }

    /// Returns a limited page of matching document IDs plus the total match count.
    pub async fn search_doc_ids_page(
        &self,
        space_id: &str,
        query: Option<&str>,
        include_deleted: bool,
        cursor: Option<&RecordId>,
        limit: usize,
    ) -> StorageResult<(Vec<RecordId>, usize)> {
        self.with_import_read_guard(|db| {
            db.search_doc_ids_page(space_id, query, include_deleted, cursor, limit)
                .map_err(Into::into)
        })
        .await
    }

    /// Searches a logical table using a caller-provided predicate.
    pub async fn search<T, F>(
        &self,
        criteria: &SearchCriteria,
        predicate: F,
    ) -> StorageResult<PaginatedResult<T>>
    where
        T: EmbeddedRecord,
        F: Fn(&T, &SearchCriteria) -> bool,
    {
        let mut rows = self.scan::<T>().await?;
        if let Some(space_id) = &criteria.space_id {
            rows.retain(|row| &row.base().space_id == space_id);
        }
        if !criteria.include_deleted {
            rows.retain(|row| row.base().deleted_at.is_none());
        }
        rows.retain(|row| predicate(row, criteria));
        rows.sort_by_key(|row| row.base().id.to_string());

        let total = rows.len();

        if let Some(cursor) = &criteria.cursor {
            let cursor_id = cursor_with_table(T::TABLE, cursor);
            rows.retain(|row| row.base().id.to_string() > cursor_id);
        }

        if criteria.limit == 0 {
            return Ok(PaginatedResult::new(Vec::new(), None, false, total));
        }
        let mut page = rows;
        let has_more = page.len() > criteria.limit;
        if has_more {
            page.truncate(criteria.limit + 1);
        }

        let next_cursor = if page.len() > criteria.limit {
            let _ = page.pop();
            page.last().map(|row| {
                let id_str = row.base().id.to_string();
                id_str
                    .split(':')
                    .nth(1)
                    .map_or_else(|| id_str.clone(), str::to_string)
            })
        } else {
            None
        };

        Ok(PaginatedResult::new(page, next_cursor, has_more, total))
    }

    /// Graph traversal over indexed relationships.
    pub async fn graph_neighbors(
        &self,
        space_id: &str,
        start: &RecordId,
        hops: usize,
    ) -> StorageResult<Vec<GraphNeighbor>> {
        self.with_import_read_guard(|db| {
            db.graph_neighbors(space_id, start, hops)
                .map_err(Into::into)
        })
        .await
    }

    /// Indexed outgoing and incoming relations for an entity.
    pub async fn entity_relations(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> StorageResult<EntityRelations> {
        self.with_import_read_guard(|db| {
            db.entity_relations(space_id, entity_id).map_err(Into::into)
        })
        .await
    }

    /// Indexed doc-node lookup by revision.
    pub async fn doc_nodes_for_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
    ) -> StorageResult<Vec<DocNode>> {
        self.with_import_read_guard(|db| {
            db.doc_nodes_for_revision(space_id, revision_id)
                .map_err(Into::into)
        })
        .await
    }

    /// Indexed chunk-node lookup by revision.
    pub async fn doc_chunks_for_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
    ) -> StorageResult<Vec<DocNode>> {
        self.with_import_read_guard(|db| {
            db.doc_chunks_for_revision(space_id, revision_id)
                .map_err(Into::into)
        })
        .await
    }

    /// Indexed doc parent chain walk.
    pub async fn doc_parent_chain(
        &self,
        space_id: &str,
        node_id: &RecordId,
    ) -> StorageResult<Vec<DocNode>> {
        self.with_import_read_guard(|db| db.doc_parent_chain(space_id, node_id).map_err(Into::into))
            .await
    }

    /// Indexed vector search over node embeddings.
    pub async fn search_node_embeddings(
        &self,
        partition_name: &str,
        space_id: &str,
        vector: &[f32],
        limit: usize,
        collection: Option<&str>,
        include_deleted: bool,
    ) -> StorageResult<Vec<NodeEmbeddingSearchHit>> {
        self.with_import_read_guard(|db| {
            db.search_node_embeddings(
                partition_name,
                space_id,
                vector,
                limit,
                collection,
                include_deleted,
            )
            .map_err(Into::into)
        })
        .await
    }
}

fn snapshot_import_options_from_env() -> SnapshotImportOptions {
    let rebuild_vector_indexes = std::env::var("ELEPHANT_DB_REBUILD_VECTOR_INDEXES_ON_LOAD")
        .map(|value| {
            !matches!(
                value.as_str(),
                "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF"
            )
        })
        .unwrap_or(true);
    SnapshotImportOptions {
        rebuild_vector_indexes,
    }
}

fn sanitize_running_work_items_for_snapshot_export(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    let mut payload: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|err| StorageError::SerializationError(err.to_string()))?;
    let Some(work_items) = payload
        .get_mut("tables")
        .and_then(|tables| tables.get_mut("work_item"))
        .and_then(serde_json::Value::as_array_mut)
    else {
        return Ok(bytes.to_vec());
    };

    for item in work_items {
        let Some(object) = item.as_object_mut() else {
            continue;
        };
        if object
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status == "running")
        {
            object.insert(
                "status".to_string(),
                serde_json::Value::String("runnable".to_string()),
            );
            object.insert("leased_by".to_string(), serde_json::Value::Null);
            object.insert("lease_expires_at".to_string(), serde_json::Value::Null);
        }
    }

    serde_json::to_vec(&payload).map_err(|err| StorageError::SerializationError(err.to_string()))
}

fn cursor_with_table(table: elephant_types::TableName, cursor: &str) -> String {
    if cursor.contains(':') {
        cursor.to_string()
    } else {
        format!("{}:{cursor}", table.as_str())
    }
}

impl From<ElephantDbError> for StorageError {
    fn from(err: ElephantDbError) -> Self {
        match err {
            ElephantDbError::Serialization(message) => Self::SerializationError(message),
            ElephantDbError::NotFound(message) => Self::NotFound(message),
            ElephantDbError::Conflict(message) => Self::Conflict(message),
            ElephantDbError::Snapshot(message) | ElephantDbError::DataIntegrity(message) => {
                Self::QueryError(message)
            }
            ElephantDbError::LockPoisoned => {
                Self::TransactionError("embedded kernel lock poisoned".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::Doc;
    use std::str::FromStr;

    fn test_doc(id: &str) -> Doc {
        Doc {
            base: BaseFields::new(
                RecordId::from_str(id).expect("valid test record id"),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Alice".to_string(),
            text: "Wonderland".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("rendition-{id}"),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn test_import_snapshot_bytes_blocks_running_workers() {
        let store = EmbeddedStore::new();
        let snapshot_source = EmbeddedStore::new();
        let bytes = snapshot_source
            .export_snapshot_bytes()
            .await
            .expect("export snapshot bytes");

        store
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "render_doc".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 0,
                idempotency_key: "snapshot-running".to_string(),
                attempts: 1,
                lease_expires_at: None,
                leased_by: Some("worker-1".to_string()),
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create work item");

        let error = store
            .import_snapshot_bytes(&bytes)
            .await
            .expect_err("import should be blocked while workers are running");
        assert!(matches!(error, StorageError::Conflict(_)));
        assert!(error
            .to_string()
            .contains("embedded work item(s) are still running"));
    }

    #[tokio::test]
    async fn test_export_snapshot_bytes_requeues_running_workers() {
        let store = EmbeddedStore::new();
        store
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "render_doc".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 0,
                idempotency_key: "snapshot-export-running".to_string(),
                attempts: 1,
                lease_expires_at: None,
                leased_by: Some("worker-1".to_string()),
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create running work item");

        let bytes = store
            .export_snapshot_bytes()
            .await
            .expect("export should produce an importable recovery snapshot");
        let restored = EmbeddedStore::new();
        restored
            .import_snapshot_bytes(&bytes)
            .await
            .expect("import recovery snapshot");
        let work_items = restored.scan::<WorkItem>().await.expect("scan work items");
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].status, WorkItemStatus::Runnable);
        assert!(work_items[0].leased_by.is_none());
        assert!(work_items[0].lease_expires_at.is_none());
    }

    #[tokio::test]
    async fn test_import_snapshot_bytes_rejects_snapshots_with_running_work_items() {
        let store = EmbeddedStore::new();
        let snapshot_source = EmbeddedStore::new();

        snapshot_source
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "render_doc".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 0,
                idempotency_key: "snapshot-payload-running".to_string(),
                attempts: 1,
                lease_expires_at: None,
                leased_by: Some("worker-1".to_string()),
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create running work item in snapshot source");

        let bytes = snapshot_source
            .db
            .export_snapshot_bytes()
            .expect("export raw snapshot bytes");

        let error = store
            .import_snapshot_bytes(&bytes)
            .await
            .expect_err("snapshot with running work should be rejected");
        assert!(matches!(error, StorageError::QueryError(_)));
        assert!(error
            .to_string()
            .contains("snapshot containing 1 running work item"));
    }

    #[tokio::test]
    async fn test_from_snapshot_rejects_snapshots_with_running_work_items() {
        let snapshot_source = EmbeddedStore::new();
        snapshot_source
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                kind: "render_doc".to_string(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Running,
                priority: 0,
                idempotency_key: "snapshot-file-running".to_string(),
                attempts: 1,
                lease_expires_at: None,
                leased_by: Some("worker-2".to_string()),
                run_after: None,
                last_error: None,
            })
            .await
            .expect("create running work item in snapshot file source");

        let bytes = snapshot_source
            .db
            .export_snapshot_bytes()
            .expect("export raw snapshot bytes");
        let mut path = std::env::temp_dir();
        path.push(format!(
            "elephant-embedded-snapshot-{}.json",
            Utc::now().timestamp_nanos_opt().expect("timestamp nanos")
        ));
        tokio::fs::write(&path, bytes)
            .await
            .expect("write snapshot file");

        let error = EmbeddedStore::from_snapshot(&path)
            .await
            .expect_err("snapshot with running work should be rejected");
        assert!(matches!(error, StorageError::QueryError(_)));
        assert!(error
            .to_string()
            .contains("snapshot containing 1 running work item"));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_soft_delete_and_restore_round_trip() {
        let store = EmbeddedStore::new();
        let doc = test_doc("doc:00000000-0000-0000-0000-000000000001");
        let id = doc.base.id.clone();

        store.create(doc).await.expect("create doc");
        store
            .soft_delete::<Doc>("default", &id)
            .await
            .expect("soft delete doc");

        let hidden = store
            .get::<Doc>("default", &id, false)
            .await
            .expect("fetch visible doc");
        assert!(hidden.is_none());

        let restored = store
            .restore::<Doc>("default", &id)
            .await
            .expect("restore doc");
        assert_eq!(restored.base.id, id);
        assert!(restored.base.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_soft_delete_missing_is_idempotent() {
        let store = EmbeddedStore::new();
        let id = RecordId::from_str("doc:00000000-0000-0000-0000-000000000099")
            .expect("valid test record id");

        store
            .soft_delete::<Doc>("default", &id)
            .await
            .expect("missing delete should be a no-op");
    }

    #[tokio::test]
    async fn test_hard_delete_missing_is_idempotent() {
        let store = EmbeddedStore::new();
        let id = RecordId::from_str("doc:00000000-0000-0000-0000-000000000098")
            .expect("valid test record id");

        store
            .hard_delete::<Doc>("default", &id)
            .await
            .expect("missing hard delete should be a no-op");
    }

    #[tokio::test]
    async fn test_restore_and_replace_existing_restores_soft_deleted_row() {
        let store = EmbeddedStore::new();
        let mut doc = test_doc("doc:00000000-0000-0000-0000-000000000002");
        let id = doc.base.id.clone();

        store.create(doc.clone()).await.expect("create doc");
        store
            .soft_delete::<Doc>("default", &id)
            .await
            .expect("soft delete doc");

        doc.title = "Restored".to_string();
        let restored = store
            .restore_and_replace_existing("default", &id, doc)
            .await
            .expect("restore and replace doc");

        assert_eq!(restored.title, "Restored");
        assert!(restored.base.deleted_at.is_none());
        let visible = store
            .get::<Doc>("default", &id, false)
            .await
            .expect("fetch visible doc")
            .expect("doc visible again");
        assert_eq!(visible.title, "Restored");
        assert!(visible.base.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_restore_and_replace_existing_rejects_duplicate_doc_rendition_key() {
        let store = EmbeddedStore::new();
        let mut deleted = test_doc("doc:00000000-0000-0000-0000-000000000004");
        let deleted_id = deleted.base.id.clone();
        let live = test_doc("doc:00000000-0000-0000-0000-000000000005");

        store
            .create(deleted.clone())
            .await
            .expect("create deleted doc");
        store.create(live.clone()).await.expect("create live doc");
        store
            .soft_delete::<Doc>("default", &deleted_id)
            .await
            .expect("soft delete conflicting doc");

        deleted.rendition_key = live.rendition_key.clone();
        let error = store
            .restore_and_replace_existing("default", &deleted_id, deleted)
            .await
            .expect_err("duplicate rendition_key restore should conflict");

        assert!(matches!(error, StorageError::Conflict(_)));
        let hidden = store
            .get::<Doc>("default", &deleted_id, false)
            .await
            .expect("fetch visible doc after failed restore");
        assert!(hidden.is_none());
    }

    #[tokio::test]
    async fn test_update_existing_rejects_soft_deleted_rows() {
        let store = EmbeddedStore::new();
        let mut doc = test_doc("doc:00000000-0000-0000-0000-000000000003");
        let id = doc.base.id.clone();

        store.create(doc.clone()).await.expect("create doc");
        store
            .soft_delete::<Doc>("default", &id)
            .await
            .expect("soft delete doc");

        doc.title = "Updated while soft deleted".to_string();
        let error = store
            .update_existing("default", &id, doc)
            .await
            .expect_err("soft-deleted rows should not be updated through update_existing");
        assert!(matches!(error, StorageError::NotFound(_)));

        let hidden = store
            .get::<Doc>("default", &id, false)
            .await
            .expect("fetch visible doc");
        assert!(hidden.is_none());
    }

    #[tokio::test]
    async fn test_search_zero_limit_reports_no_more_without_cursor() {
        let store = EmbeddedStore::new();
        store
            .create(test_doc("doc:00000000-0000-0000-0000-000000000010"))
            .await
            .expect("create doc");

        let result = store
            .search::<Doc, _>(
                &SearchCriteria::new().with_space_id("default").with_limit(0),
                |_, _| true,
            )
            .await
            .expect("search docs");

        assert!(result.results.is_empty());
        assert!(!result.has_more);
        assert!(result.cursor.is_none());
        assert_eq!(result.total, 1);
    }

    #[tokio::test]
    async fn test_search_total_is_stable_across_cursor_pages() {
        let store = EmbeddedStore::new();
        for id in 11..=13 {
            store
                .create(test_doc(&format!(
                    "doc:00000000-0000-0000-0000-0000000000{id}"
                )))
                .await
                .expect("create doc");
        }

        let page_one = store
            .search::<Doc, _>(
                &SearchCriteria::new().with_space_id("default").with_limit(2),
                |_, _| true,
            )
            .await
            .expect("search first page");
        assert_eq!(page_one.total, 3);
        assert!(page_one.has_more);
        let cursor = page_one.cursor.clone().expect("cursor for page two");

        let page_two = store
            .search::<Doc, _>(
                &SearchCriteria::new()
                    .with_space_id("default")
                    .with_limit(2)
                    .with_cursor(cursor),
                |_, _| true,
            )
            .await
            .expect("search second page");
        assert_eq!(page_two.total, 3);
        assert!(!page_two.has_more);
    }
}
