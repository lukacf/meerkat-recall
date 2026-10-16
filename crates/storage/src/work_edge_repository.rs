//! Work edge repository implementation for `SurrealDB`.
//!
//! Per spec §5, work edges represent dependencies between work items.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::work::WorkEdge;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

pub(crate) const WORK_EDGE_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    from_item,
    to_item,
    edge_type
";

/// Database-only proxy for WorkEdge.
#[derive(Deserialize)]
pub(crate) struct WorkEdgeDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    from_item: Option<surrealdb::sql::Thing>,
    to_item: Option<surrealdb::sql::Thing>,
    #[serde(default = "default_edge_type")]
    edge_type: String,
}

fn default_edge_type() -> String {
    "depends_on".to_string()
}

impl TryFrom<WorkEdgeDb> for WorkEdge {
    type Error = StorageError;

    fn try_from(db: WorkEdgeDb) -> Result<Self, Self::Error> {
        let from_thing = db
            .from_item
            .ok_or_else(|| StorageError::QueryError("Missing from_item".to_string()))?;
        let from_id = crate::surreal::record_id_from_thing(&from_thing)
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

        let to_thing = db
            .to_item
            .ok_or_else(|| StorageError::QueryError("Missing to_item".to_string()))?;
        let to_id = crate::surreal::record_id_from_thing(&to_thing)
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            from: Link::new(from_id),
            to: Link::new(to_id),
            edge_type: db.edge_type,
        })
    }
}

/// Work edge repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct WorkEdgeRepository {
    client: StorageClient,
}

impl WorkEdgeRepository {
    /// Creates a new work edge repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a WorkEdge to a SurrealDB value for insertion.
    pub(crate) fn work_edge_to_value(item: &WorkEdge) -> StorageResult<surrealdb::sql::Value> {
        use std::collections::BTreeMap;
        use surrealdb::sql::{Array, Object, Value};

        let mut obj: BTreeMap<String, Value> = BTreeMap::new();
        obj.insert("space_id".to_string(), item.base.space_id.clone().into());
        obj.insert(
            "created_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.created_at).into(),
        );
        obj.insert(
            "updated_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.updated_at).into(),
        );
        obj.insert(
            "created_by".to_string(),
            item.base.created_by.clone().into(),
        );
        obj.insert(
            "deleted_at".to_string(),
            item.base.deleted_at.map_or(Value::None, |dt| {
                crate::surreal::datetime_from_chrono(dt).into()
            }),
        );
        obj.insert(
            "provenance".to_string(),
            crate::surreal::json_to_surreal(serde_json::json!(item.base.provenance))?,
        );
        obj.insert(
            "security".to_string(),
            crate::surreal::security_to_value(&item.base.security),
        );
        obj.insert(
            "subjects".to_string(),
            Value::Array(Array::from(
                item.base
                    .subjects
                    .iter()
                    .map(|l| Value::from(crate::surreal::thing_from_record_id(l.id())))
                    .collect::<Vec<Value>>(),
            )),
        );
        obj.insert(
            "subjects_state".to_string(),
            crate::surreal::json_to_surreal(serde_json::json!(item.base.subjects_state))?,
        );
        // Convert Links to native Things for proper query matching
        obj.insert(
            "from_item".to_string(),
            crate::surreal::thing_from_record_id(item.from.id()).into(),
        );
        obj.insert(
            "to_item".to_string(),
            crate::surreal::thing_from_record_id(item.to.id()).into(),
        );
        obj.insert("edge_type".to_string(), item.edge_type.clone().into());

        Ok(Value::Object(Object::from(obj)))
    }
}

#[async_trait]
impl Repository<WorkEdge> for WorkEdgeRepository {
    async fn create(&self, record: WorkEdge) -> StorageResult<WorkEdge> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::work_edge_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {WORK_EDGE_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created work_edge: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but work_edge not found".to_string())
        })?;

        WorkEdge::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkEdge>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<WorkEdge>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_EDGE_PROJECTION} FROM work_edge WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_edge: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(WorkEdge::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkEdge,
    ) -> StorageResult<WorkEdge> {
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let record_value = Self::work_edge_to_value(&normalized)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {WORK_EDGE_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated work_edge: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("WorkEdge not found in space: {id}")))?;

        WorkEdge::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<WorkEdge>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<WorkEdge>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<WorkEdge>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {WORK_EDGE_PROJECTION} FROM work_edge WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {WORK_EDGE_PROJECTION} FROM work_edge WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                    "space_id" => space_id
                },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_edges: {e}")))?;

        items.into_iter().map(WorkEdge::try_from).collect()
    }
}

/// Additional work-edge-specific operations.
impl WorkEdgeRepository {
    /// Finds all dependencies for a work item (edges where `to_item` is the given item).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_dependencies(&self, item_id: &RecordId) -> StorageResult<Vec<WorkEdge>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<WorkEdge>()
                .await?
                .into_iter()
                .filter(|edge| edge.base.deleted_at.is_none() && edge.to.id() == item_id)
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_EDGE_PROJECTION} FROM work_edge WHERE to_item = $item_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "item_id" => crate::surreal::thing_from_record_id(item_id) },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_edges: {e}")))?;

        items.into_iter().map(WorkEdge::try_from).collect()
    }

    /// Finds all dependents of a work item (edges where `from_item` is the given item).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_dependents(&self, item_id: &RecordId) -> StorageResult<Vec<WorkEdge>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<WorkEdge>()
                .await?
                .into_iter()
                .filter(|edge| edge.base.deleted_at.is_none() && edge.from.id() == item_id)
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_EDGE_PROJECTION} FROM work_edge WHERE from_item = $item_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "item_id" => crate::surreal::thing_from_record_id(item_id) },
            )
            .await?;

        let items: Vec<WorkEdgeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_edges: {e}")))?;

        items.into_iter().map(WorkEdge::try_from).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;
    use crate::StorageConfig;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80500);

    const WORK_EDGE_SCHEMA: &str = r"
        DEFINE TABLE work_edge SCHEMALESS;
    ";

    async fn setup_test_repo() -> WorkEdgeRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("work_edge_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(WORK_EDGE_SCHEMA).await.unwrap();

        WorkEdgeRepository::new(client)
    }

    fn create_test_work_edge() -> WorkEdge {
        let id = RecordId::new_v4(TableName::WorkEdge);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let from_id = RecordId::new_v4(TableName::WorkItem);
        let to_id = RecordId::new_v4(TableName::WorkItem);

        WorkEdge {
            base,
            from: Link::new(from_id),
            to: Link::new(to_id),
            edge_type: "depends_on".to_string(),
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_work_edge() {
        let repo = setup_test_repo().await;
        let item = create_test_work_edge();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.edge_type, "depends_on");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_work_edge() {
        let repo = setup_test_repo().await;
        let item = create_test_work_edge();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_work_edge_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_edge();
        item.edge_type = "blocks".to_string();

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.edge_type, "blocks");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_work_edge() {
        let repo = setup_test_repo().await;
        let item = create_test_work_edge();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
