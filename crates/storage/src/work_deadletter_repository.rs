//! Work deadletter repository implementation for `SurrealDB`.
//!
//! Per spec §5, work deadletters are failed work items after max retries.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::work::{WorkDeadletter, WorkItem, WorkRun};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

pub(crate) const WORK_DEADLETTER_PROJECTION: &str = r"
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
    work_item,
    runs,
    final_error,
    deadlettered_at,
    acknowledged,
    notes
";

/// Database-only proxy for WorkDeadletter.
#[derive(Deserialize)]
pub(crate) struct WorkDeadletterDb {
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
    work_item: Option<Link<WorkItem>>,
    #[serde(default)]
    runs: Vec<Link<WorkRun>>,
    final_error: Option<String>,
    deadlettered_at: Option<chrono::DateTime<Utc>>,
    #[serde(default)]
    acknowledged: bool,
    notes: Option<String>,
}

impl TryFrom<WorkDeadletterDb> for WorkDeadletter {
    type Error = StorageError;

    fn try_from(db: WorkDeadletterDb) -> Result<Self, Self::Error> {
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
            work_item: db
                .work_item
                .ok_or_else(|| StorageError::QueryError("Missing work_item".to_string()))?,
            runs: db.runs,
            final_error: db
                .final_error
                .ok_or_else(|| StorageError::QueryError("Missing final_error".to_string()))?,
            deadlettered_at: db
                .deadlettered_at
                .ok_or_else(|| StorageError::QueryError("Missing deadlettered_at".to_string()))?,
            acknowledged: db.acknowledged,
            notes: db.notes,
        })
    }
}

/// Work deadletter repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct WorkDeadletterRepository {
    client: StorageClient,
}

impl WorkDeadletterRepository {
    /// Creates a new work deadletter repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a WorkDeadletter to a SurrealDB value for insertion.
    pub(crate) fn work_deadletter_to_value(
        item: &WorkDeadletter,
    ) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;
        let mut object = crate::surreal::base_fields_to_object(&item.base)?;
        object.insert(
            "work_item".into(),
            crate::surreal::thing_from_record_id(item.work_item.id()).into(),
        );
        object.insert(
            "runs".into(),
            Value::Array(crate::surreal::things_from_links(item.runs.clone())),
        );
        object.insert("final_error".into(), Value::from(item.final_error.clone()));
        object.insert(
            "deadlettered_at".into(),
            crate::surreal::datetime_from_chrono(item.deadlettered_at).into(),
        );
        object.insert("acknowledged".into(), Value::from(item.acknowledged));
        object.insert(
            "notes".into(),
            item.notes
                .as_ref()
                .map_or(Value::None, |value| Value::from(value.clone())),
        );
        Ok(Value::Object(object))
    }
}

#[async_trait]
impl Repository<WorkDeadletter> for WorkDeadletterRepository {
    async fn create(&self, record: WorkDeadletter) -> StorageResult<WorkDeadletter> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {WORK_DEADLETTER_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => Self::work_deadletter_to_value(&record)?,
                },
            )
            .await?;

        let items: Vec<WorkDeadletterDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created work_deadletter: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but work_deadletter not found".to_string())
        })?;

        WorkDeadletter::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkDeadletter>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<WorkDeadletter>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_DEADLETTER_PROJECTION} FROM work_deadletter WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkDeadletterDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_deadletter: {e}"))
        })?;

        Ok(items
            .into_iter()
            .next()
            .map(WorkDeadletter::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkDeadletter,
    ) -> StorageResult<WorkDeadletter> {
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {WORK_DEADLETTER_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::work_deadletter_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<WorkDeadletterDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated work_deadletter: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("WorkDeadletter not found in space: {id}"))
        })?;

        WorkDeadletter::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<WorkDeadletter>(space_id, id).await;
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
    ) -> StorageResult<Vec<WorkDeadletter>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<WorkDeadletter>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {WORK_DEADLETTER_PROJECTION} FROM work_deadletter WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {WORK_DEADLETTER_PROJECTION} FROM work_deadletter WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<WorkDeadletterDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_deadletters: {e}"))
        })?;

        items.into_iter().map(WorkDeadletter::try_from).collect()
    }
}

/// Additional work-deadletter-specific operations.
impl WorkDeadletterRepository {
    /// Finds unacknowledged deadletter entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_unacknowledged(
        &self,
        space_id: &str,
        limit: u32,
    ) -> StorageResult<Vec<WorkDeadletter>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<WorkDeadletter>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && !item.acknowledged
            });
            items.sort_by_key(|item| {
                (
                    std::cmp::Reverse(item.deadlettered_at),
                    item.base.id.to_string(),
                )
            });
            if items.len() > limit as usize {
                items.truncate(limit as usize);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_DEADLETTER_PROJECTION} FROM work_deadletter WHERE space_id = $space_id AND acknowledged = false AND deleted_at IS NONE ORDER BY deadlettered_at DESC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkDeadletterDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_deadletters: {e}"))
        })?;

        items.into_iter().map(WorkDeadletter::try_from).collect()
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80800);

    const WORK_DEADLETTER_SCHEMA: &str = r"
        DEFINE TABLE work_deadletter SCHEMALESS;
    ";

    async fn setup_test_repo() -> WorkDeadletterRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("work_deadletter_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(WORK_DEADLETTER_SCHEMA).await.unwrap();

        WorkDeadletterRepository::new(client)
    }

    fn create_test_work_deadletter() -> WorkDeadletter {
        let id = RecordId::new_v4(TableName::WorkDeadletter);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let work_item_id = RecordId::new_v4(TableName::WorkItem);

        WorkDeadletter {
            base,
            work_item: Link::new(work_item_id),
            runs: vec![],
            final_error: "Max retries exceeded".to_string(),
            deadlettered_at: Utc::now(),
            acknowledged: false,
            notes: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_work_deadletter() {
        let repo = setup_test_repo().await;
        let item = create_test_work_deadletter();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.final_error, "Max retries exceeded");
        assert!(!created.acknowledged);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_work_deadletter() {
        let repo = setup_test_repo().await;
        let item = create_test_work_deadletter();
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
    async fn test_work_deadletter_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_deadletter();
        item.notes = Some("Operator note".to_string());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.notes, Some("Operator note".to_string()));
        assert_eq!(created.final_error, "Max retries exceeded");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_work_deadletter() {
        let repo = setup_test_repo().await;
        let item = create_test_work_deadletter();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.acknowledged = true;
        updated.notes = Some("Acknowledged by admin".to_string());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!(result.acknowledged);
        assert_eq!(result.notes, Some("Acknowledged by admin".to_string()));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_work_deadletter() {
        let repo = setup_test_repo().await;
        let item = create_test_work_deadletter();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
