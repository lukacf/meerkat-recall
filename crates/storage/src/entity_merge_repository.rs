//! Entity merge repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::{EntityMerge, MergeStrategy};

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;

const ENTITY_MERGE_PROJECTION: &str = r"
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
    `from`,
    `into`,
    strategy,
    decided_by,
    decided_at,
    run_id
";

/// Database-only proxy for `EntityMerge` to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct EntityMergeDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<surrealdb::sql::Thing>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    from: Option<surrealdb::sql::Thing>,
    into: Option<surrealdb::sql::Thing>,
    strategy: Option<MergeStrategy>,
    decided_by: Option<String>,
    decided_at: Option<chrono::DateTime<Utc>>,
    run_id: Option<String>,
}

impl TryFrom<EntityMergeDb> for EntityMerge {
    type Error = StorageError;

    fn try_from(db: EntityMergeDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
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
                subjects: required_field(db.subjects, "subjects")?
                    .into_iter()
                    .map(|t| crate::surreal::record_id_from_thing(&t))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StorageError::QueryError(e.to_string()))?
                    .into_iter()
                    .map(Link::new)
                    .collect(),
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            from: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.from
                        .ok_or_else(|| StorageError::QueryError("Missing from link".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            into: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.into
                        .ok_or_else(|| StorageError::QueryError("Missing into link".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            strategy: required_field(db.strategy, "strategy")?,
            decided_by: required_field(db.decided_by, "decided_by")?,
            decided_at: db
                .decided_at
                .ok_or_else(|| StorageError::QueryError("Missing decided_at".to_string()))?,
            run_id: db.run_id,
        })
    }
}

/// Entity merge repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct EntityMergeRepository {
    client: StorageClient,
}

impl EntityMergeRepository {
    /// Creates a new entity merge repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Repository<EntityMerge> for EntityMergeRepository {
    async fn create(&self, record: EntityMerge) -> StorageResult<EntityMerge> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {ENTITY_MERGE_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::entity_merge_to_value(&record)?,
                },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created entity_merge: {e}"))
        })?;

        let merge_db = merges.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but entity_merge not found".to_string())
        })?;

        EntityMerge::try_from(merge_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<EntityMerge>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merge: {e}")))?;

        merges
            .into_iter()
            .next()
            .map(EntityMerge::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: EntityMerge,
    ) -> StorageResult<EntityMerge> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ENTITY_MERGE_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::entity_merge_to_value(&normalized)?,
                },
            )
            .await?;

        let merges: Vec<EntityMergeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated entity_merge: {e}"))
        })?;

        let merge_db = merges.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Entity merge not found in space: {id}"))
        })?;

        EntityMerge::try_from(merge_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<EntityMerge>(space_id, id).await;
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
    ) -> StorageResult<Vec<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<EntityMerge>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let merges: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merges: {e}")))?;

        merges.into_iter().map(EntityMerge::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<EntityMerge> for EntityMergeRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<EntityMerge>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search::<EntityMerge, _>(criteria, |_merge, _criteria| true)
                .await;
        }

        #[derive(serde::Deserialize)]
        struct CountResult {
            total: usize,
        }

        let mut conditions = Vec::new();
        let mut bindings = std::collections::BTreeMap::new();

        if !criteria.include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
        }

        if let Some(ref space_id) = criteria.space_id {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::EntityMerge)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            conditions.push("id > $cursor".to_string());
            bindings.insert(
                "cursor".to_string(),
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&rid)),
            );
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {ENTITY_MERGE_PROJECTION} FROM entity_merge {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let merges_db: Vec<EntityMergeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity_merges: {e}")))?;

        let mut merges: Vec<EntityMerge> = merges_db
            .into_iter()
            .map(EntityMerge::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = merges.len() > criteria.limit;
        if has_more {
            merges.pop();
        }

        let next_cursor = if has_more {
            merges.last().map(|m| {
                m.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&m.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM entity_merge {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse entity merge count")?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(merges, next_cursor, has_more, total))
    }
}
