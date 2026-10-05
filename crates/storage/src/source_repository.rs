//! Source repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::entity::Entity;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{WorkItem, WorkItemArchive, WorkItemStatus};
use elephant_types::records::{Source, SourceLifecycleStatus, SourceType};

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;

const SOURCE_PROJECTION: &str = r"
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
    type,
    lifecycle_status,
    current_ingest_mode,
    latest_ingest_decision,
    uri,
    path,
    external_id,
    hash,
    author,
    channel,
    timestamp,
    parent,
    range,
    metadata
";

/// Database-only proxy for Source to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct SourceDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    #[serde(rename = "type")]
    source_type: Option<SourceType>,
    #[serde(default)]
    lifecycle_status: Option<SourceLifecycleStatus>,
    #[serde(default)]
    current_ingest_mode: Option<elephant_types::records::ingest::IngestMode>,
    #[serde(default)]
    latest_ingest_decision: Option<surrealdb::sql::Thing>,
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    external_id: Option<String>,
    hash: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    timestamp: Option<chrono::DateTime<Utc>>,
    #[serde(default)]
    parent: Option<surrealdb::sql::Thing>,
    #[serde(default)]
    range: Option<elephant_types::records::source::SourceRange>,
    #[serde(default = "default_metadata")]
    metadata: serde_json::Value,
}

impl TryFrom<SourceDb> for Source {
    type Error = StorageError;

    fn try_from(db: SourceDb) -> Result<Self, Self::Error> {
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
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            type_: db
                .source_type
                .ok_or_else(|| StorageError::QueryError("Missing type".to_string()))?,
            // Source-first lifecycle fields were added after existing source
            // records already existed in deployed namespaces. Treat missing
            // lifecycle state as `Discovered` during that schema transition.
            lifecycle_status: db.lifecycle_status.unwrap_or_default(),
            current_ingest_mode: db.current_ingest_mode,
            latest_ingest_decision: db
                .latest_ingest_decision
                .map(|t| crate::surreal::record_id_from_thing(&t).map(Link::new))
                .transpose()
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            uri: db.uri,
            path: db.path,
            external_id: db.external_id,
            hash: required_field(db.hash, "hash")?,
            author: db.author,
            channel: db.channel,
            timestamp: db.timestamp,
            parent: db
                .parent
                .map(|t| crate::surreal::record_id_from_thing(&t).map(Link::new))
                .transpose()
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            range: db.range,
            metadata: db.metadata,
        })
    }
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Source repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct SourceRepository {
    client: StorageClient,
}

impl SourceRepository {
    /// Restores a source only if it is unchanged since `authorized`, the row
    /// the caller's authorization was decided on.
    ///
    /// See [`crate::lifecycle_guard`]: the stored security, subjects, subjects
    /// state and exact lifecycle must still equal `authorized` in the same
    /// mutation that restores.
    ///
    /// # Errors
    ///
    /// Returns a conflict if the row is missing or changed, or an error if
    /// storage access fails.
    pub async fn restore_guarded(
        &self,
        space_id: &str,
        id: &RecordId,
        authorized: &Source,
    ) -> StorageResult<Source> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(mut current) = mutation.get::<Source>(space_id, id, true)? else {
                        return Err(crate::lifecycle_guard::guard_conflict("Source", id));
                    };
                    if !crate::lifecycle_guard::base_envelope_unchanged(
                        &current.base,
                        &authorized.base,
                    ) {
                        return Err(crate::lifecycle_guard::guard_conflict("Source", id));
                    }
                    current.base.deleted_at = None;
                    current.base.updated_at = chrono::Utc::now();
                    Ok(mutation.replace(current)?)
                })
                .await;
        }

        let (condition, mut bindings) =
            crate::lifecycle_guard::base_envelope_condition(&authorized.base)?;
        bindings.insert(
            "id".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id)),
        );
        bindings.insert(
            "space_id".to_string(),
            surrealdb::sql::Value::from(space_id),
        );
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() \
                     WHERE space_id = $space_id AND {condition} RETURN {SOURCE_PROJECTION}"
                ),
                bindings,
            )
            .await?;
        let rows: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse guarded source restore: {e}"))
        })?;
        Source::try_from(
            rows.into_iter()
                .next()
                .ok_or_else(|| crate::lifecycle_guard::guard_conflict("Source", id))?,
        )
    }

    const fn pending_graph_statuses() -> [WorkItemStatus; 4] {
        [
            WorkItemStatus::Pending,
            WorkItemStatus::Runnable,
            WorkItemStatus::Running,
            WorkItemStatus::Failed,
        ]
    }

    fn work_item_is_active_for_current_graph_run(
        item: &WorkItem,
        source_updated_at: chrono::DateTime<Utc>,
        now: chrono::DateTime<Utc>,
    ) -> bool {
        match item.status {
            WorkItemStatus::Runnable => item.run_after.is_none_or(|run_after| run_after <= now),
            WorkItemStatus::Failed => item.base.updated_at >= source_updated_at,
            WorkItemStatus::Pending | WorkItemStatus::Running => true,
            _ => false,
        }
    }

    fn work_item_blocks_graph_ready(
        item: &WorkItem,
        space_id: &str,
        source_id: &str,
        source_updated_at: chrono::DateTime<Utc>,
        current_work_item_id: &RecordId,
        work_kinds: &[String],
        now: chrono::DateTime<Utc>,
    ) -> bool {
        item.base.space_id == space_id
            && item.base.deleted_at.is_none()
            && Self::pending_graph_statuses().contains(&item.status)
            && work_kinds.iter().any(|kind| kind == &item.kind)
            && item.base.id != *current_work_item_id
            && item
                .payload
                .as_object()
                .and_then(|object| object.get("source_id"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == source_id)
            && Self::work_item_is_active_for_current_graph_run(item, source_updated_at, now)
    }

    async fn mark_graph_ready_if_quiescent_embedded(
        &self,
        store: std::sync::Arc<crate::embedded::EmbeddedStore>,
        space_id: &str,
        source_id: &RecordId,
        current_work_item_id: &RecordId,
        work_kinds: &[&str],
    ) -> StorageResult<bool> {
        let target_space_id = space_id.to_string();
        let target_source_id = source_id.clone();
        let source_id_string = source_id.to_string();
        let current_work_item_id = current_work_item_id.clone();
        let work_kinds = work_kinds
            .iter()
            .map(|kind| (*kind).to_string())
            .collect::<Vec<_>>();

        store
            .mutate(move |tx| {
                let Some(mut source) =
                    tx.get::<Source>(&target_space_id, &target_source_id, false)?
                else {
                    return Err(StorageError::NotFound(target_source_id.to_string()));
                };
                let now = Utc::now();
                let source_updated_at = source.base.updated_at;
                let cold_items = tx
                    .scan::<WorkItemArchive>()?
                    .into_iter()
                    .filter(|archive| archive.base.deleted_at.is_none())
                    .map(|archive| archive.record);
                let has_pending_graph_work = tx
                    .scan::<WorkItem>()?
                    .into_iter()
                    .chain(cold_items)
                    .any(|item| {
                        Self::work_item_blocks_graph_ready(
                            &item,
                            &target_space_id,
                            &source_id_string,
                            source_updated_at,
                            &current_work_item_id,
                            &work_kinds,
                            now,
                        )
                    });
                if has_pending_graph_work {
                    return Ok(false);
                }

                source.lifecycle_status = SourceLifecycleStatus::GraphReady;
                source.current_ingest_mode = Some(IngestMode::Graph);
                source.base.updated_at = now;
                let _ = tx.replace(source)?;
                Ok(true)
            })
            .await
    }

    async fn mark_graph_ready_if_quiescent_surreal(
        &self,
        space_id: &str,
        source_id: &RecordId,
        current_work_item_id: &RecordId,
        work_kinds: &[&str],
    ) -> StorageResult<bool> {
        let kinds = work_kinds
            .iter()
            .map(|kind| (*kind).to_string())
            .collect::<Vec<_>>();
        let statuses = Self::pending_graph_statuses()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "BEGIN TRANSACTION; \
                     LET $source_updated_at = (SELECT VALUE updated_at FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE); \
                     UPDATE $id SET lifecycle_status = $lifecycle_status, current_ingest_mode = $current_ingest_mode, updated_at = time::now() \
                     WHERE space_id = $space_id \
                       AND deleted_at IS NONE \
                       AND array::len((SELECT id FROM work_item \
                                       WHERE space_id = $space_id \
                                         AND deleted_at IS NONE \
                                         AND payload.source_id = $source_id \
                                         AND status INSIDE $statuses \
                                         AND kind INSIDE $kinds \
                                         AND id != $current_work_item_id \
                                         AND array::any([(status = 'runnable' AND array::any([run_after IS NONE, run_after <= time::now()])), \
                                              status = 'pending', status = 'running', \
                                              (status = 'failed' AND updated_at >= $source_updated_at)]) \
                                       LIMIT 1)) = 0 \
                       AND array::len((SELECT id FROM work_item_archive \
                                       WHERE space_id = $space_id \
                                         AND deleted_at IS NONE AND record.deleted_at IS NONE \
                                         AND record.payload.source_id = $source_id \
                                         AND record.status = 'failed' \
                                         AND record.kind INSIDE $kinds \
                                         AND original_id != $current_work_item_id \
                                         AND record.updated_at >= $source_updated_at \
                                       LIMIT 1)) = 0 \
                     RETURN {SOURCE_PROJECTION}; COMMIT TRANSACTION;"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(source_id),
                    "space_id" => space_id,
                    "source_id" => source_id.to_string(),
                    "statuses" => statuses,
                    "kinds" => kinds,
                    "current_work_item_id" => crate::surreal::thing_from_record_id(current_work_item_id),
                    "lifecycle_status" => SourceLifecycleStatus::GraphReady.to_string(),
                    "current_ingest_mode" => IngestMode::Graph.to_string(),
                },
            )
            .await?;
        let sources: Vec<SourceDb> = response.take(1).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse graph-ready source update: {e}"))
        })?;
        if let Some(source) = sources.into_iter().next() {
            let _ = Source::try_from(source)?;
            return Ok(true);
        }

        if self.get(space_id, source_id).await?.is_some() {
            Ok(false)
        } else {
            Err(StorageError::NotFound(source_id.to_string()))
        }
    }

    /// Creates a new source repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Repository<Source> for SourceRepository {
    async fn create(&self, record: Source) -> StorageResult<Source> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        // Checked single round-trip: a duplicate id now surfaces as
        // StorageError::Conflict instead of silently returning the
        // pre-existing row.
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {SOURCE_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::source_to_value(&record)?,
                },
            )
            .await?;

        let sources: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created source: {e}"))
        })?;

        let source_db = sources.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but source not found".to_string())
        })?;

        Source::try_from(source_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Source>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Source>(space_id, id, false).await;
        }

        // Record access (`FROM ONLY $id`): the planner has no id-equality
        // fast path, so the `FROM source WHERE id = $id` form scanned the
        // whole table on each point lookup (issue #34). The space_id/
        // deleted_at WHERE clause preserves space isolation.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SOURCE_PROJECTION} FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let source: Option<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse source: {e}")))?;

        Ok(source.map(Source::try_from).transpose()?)
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Source) -> StorageResult<Source> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {SOURCE_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::source_to_value(&normalized)?,
                },
            )
            .await?;

        let sources: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated source: {e}"))
        })?;

        let source_db = sources
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Source not found in space: {id}")))?;

        Source::try_from(source_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Source>(space_id, id).await;
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
    ) -> StorageResult<Vec<Source>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<Source>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {SOURCE_PROJECTION} FROM source WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {SOURCE_PROJECTION} FROM source WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let sources: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse sources: {e}")))?;

        sources.into_iter().map(Source::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<Source> for SourceRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Source>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search(criteria, |source: &Source, criteria| {
                    criteria.query.as_ref().is_none_or(|query| {
                        let query = query.to_ascii_lowercase();
                        source
                            .uri
                            .as_ref()
                            .is_some_and(|uri| uri.to_ascii_lowercase().contains(&query))
                            || source
                                .path
                                .as_ref()
                                .is_some_and(|path| path.to_ascii_lowercase().contains(&query))
                            || source.external_id.as_ref().is_some_and(|external_id| {
                                external_id.to_ascii_lowercase().contains(&query)
                            })
                    })
                })
                .await;
        }

        #[derive(Deserialize)]
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

        if let Some(ref query) = criteria.query {
            conditions.push(
                "(string::lowercase(IF uri IS NONE THEN \"\" ELSE uri END) CONTAINS string::lowercase($query) OR string::lowercase(IF path IS NONE THEN \"\" ELSE path END) CONTAINS string::lowercase($query) OR string::lowercase(IF external_id IS NONE THEN \"\" ELSE external_id END) CONTAINS string::lowercase($query))"
                    .to_string(),
            );
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::Source)
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
            "SELECT {SOURCE_PROJECTION} FROM source {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let sources_db: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse sources: {e}")))?;

        let mut sources: Vec<Source> = sources_db
            .into_iter()
            .map(Source::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = criteria.limit > 0 && sources.len() > criteria.limit;
        if has_more {
            sources.pop();
        } else if criteria.limit == 0 {
            sources.clear();
        }

        let next_cursor = if has_more {
            sources.last().map(|s| {
                s.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&s.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query = format!("SELECT count() as total FROM source {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse source count")?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(sources, next_cursor, has_more, total))
    }
}

/// Additional source-specific operations.
impl SourceRepository {
    /// Gets a source including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Source>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Source>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {SOURCE_PROJECTION} FROM source WHERE id = $id AND space_id = $space_id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let sources: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse source: {e}")))?;

        sources.into_iter().next().map(Source::try_from).transpose()
    }

    /// Restores a soft-deleted source.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or the source is not found.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Source> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<Source>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {SOURCE_PROJECTION}"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let sources: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored source: {e}"))
        })?;

        let source_db = sources
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Source not found: {id}")))?;

        Source::try_from(source_db)
    }

    /// Replaces an existing source and clears any soft-delete flag in one
    /// repository operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails or the source is not found.
    pub async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Source,
    ) -> StorageResult<Source> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .restore_and_replace_existing(space_id, id, record)
                .await;
        }

        record.base.id.clone_from(id);
        record.base.space_id = space_id.to_string();
        record.base.deleted_at = None;

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id REPLACE $record WHERE space_id = $space_id RETURN {SOURCE_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::source_to_value(&record)?,
                },
            )
            .await?;

        let sources: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored-and-replaced source: {e}"))
        })?;

        let source_db = sources.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Source not found after restore-and-replace: {id}"))
        })?;

        Source::try_from(source_db)
    }

    /// Replaces an existing source with the exact provided snapshot, even if
    /// that snapshot remains soft-deleted.
    pub async fn replace_existing_snapshot(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Source,
    ) -> StorageResult<Source> {
        if let Some(store) = self.client.embedded_store() {
            return store.replace_existing_any_state(space_id, id, record).await;
        }

        record.base.id.clone_from(id);
        record.base.space_id = space_id.to_string();

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id REPLACE $record WHERE space_id = $space_id RETURN {SOURCE_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::source_to_value(&record)?,
                },
            )
            .await?;

        let sources: Vec<SourceDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse snapshot-replaced source: {e}"))
        })?;

        let source_db = sources.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Source not found after snapshot replace: {id}"))
        })?;

        Source::try_from(source_db)
    }

    /// Hard deletes a source (permanent).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete query fails.
    pub async fn hard_delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.hard_delete::<Source>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "DELETE FROM $id WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    /// Finds all live sources by hash within a space (for deduplication).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_all_by_hash_in_space(
        &self,
        space_id: &str,
        hash: &str,
    ) -> StorageResult<Vec<Source>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store
                .scan::<Source>()
                .await?
                .into_iter()
                .filter(|source| {
                    source.base.space_id == space_id
                        && source.base.deleted_at.is_none()
                        && source.hash == hash
                })
                .collect::<Vec<_>>();
            items.sort_by_key(|source| source.base.id.to_string());
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SOURCE_PROJECTION} FROM source \
                     WHERE space_id = $space_id AND hash = $hash AND deleted_at IS NONE \
                     ORDER BY id ASC"
                ),
                crate::bindings! { "space_id" => space_id, "hash" => hash },
            )
            .await?;

        let sources: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse source: {e}")))?;

        sources
            .into_iter()
            .map(Source::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Finds all sources by hash within a space, including soft-deleted rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_all_by_hash_in_space_with_deleted(
        &self,
        space_id: &str,
        hash: &str,
    ) -> StorageResult<Vec<Source>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store
                .scan::<Source>()
                .await?
                .into_iter()
                .filter(|source| source.base.space_id == space_id && source.hash == hash)
                .collect::<Vec<_>>();
            items.sort_by_key(|source| source.base.id.to_string());
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SOURCE_PROJECTION} FROM source \
                     WHERE space_id = $space_id AND hash = $hash \
                     ORDER BY id ASC"
                ),
                crate::bindings! { "space_id" => space_id, "hash" => hash },
            )
            .await?;

        let sources: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse source: {e}")))?;

        sources
            .into_iter()
            .map(Source::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Finds sources whose `uri` starts with the given prefix.
    ///
    /// Used by attribution re-stamping to resolve doc linkage through source
    /// uris (e.g. Angel's `angel://session/{id}/` prefix).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_uri_prefix(
        &self,
        space_id: &str,
        prefix: &str,
        limit: usize,
    ) -> StorageResult<Vec<Source>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store
                .scan::<Source>()
                .await?
                .into_iter()
                .filter(|source| {
                    source.base.space_id == space_id
                        && source.base.deleted_at.is_none()
                        && source
                            .uri
                            .as_deref()
                            .is_some_and(|uri| uri.starts_with(prefix))
                })
                .collect::<Vec<_>>();
            items.sort_by_key(|source| source.base.id.to_string());
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SOURCE_PROJECTION} FROM source \
                     WHERE space_id = $space_id AND deleted_at IS NONE \
                     AND uri IS NOT NONE AND string::starts_with(uri, $prefix) \
                     ORDER BY id ASC \
                     LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id, "prefix" => prefix },
            )
            .await?;

        let sources: Vec<SourceDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse source: {e}")))?;

        sources
            .into_iter()
            .map(Source::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Finds a source by hash within a space (for deduplication).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_hash_in_space(
        &self,
        space_id: &str,
        hash: &str,
    ) -> StorageResult<Option<Source>> {
        self.find_all_by_hash_in_space(space_id, hash)
            .await
            .map(|items| items.into_iter().next())
    }

    /// Marks a source graph-ready only when no pending graph work items remain
    /// for that source.
    ///
    /// Returns `true` when the source was updated, `false` when other graph work
    /// is still pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the source does not exist or the update fails.
    pub async fn mark_graph_ready_if_quiescent(
        &self,
        space_id: &str,
        source_id: &RecordId,
        current_work_item_id: &RecordId,
        work_kinds: &[&str],
    ) -> StorageResult<bool> {
        if work_kinds.is_empty() {
            return Ok(false);
        }

        if let Some(store) = self.client.embedded_store() {
            return self
                .mark_graph_ready_if_quiescent_embedded(
                    store,
                    space_id,
                    source_id,
                    current_work_item_id,
                    work_kinds,
                )
                .await;
        }
        self.mark_graph_ready_if_quiescent_surreal(
            space_id,
            source_id,
            current_work_item_id,
            work_kinds,
        )
        .await
    }
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
    use crate::StorageConfig;

    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::work::WorkItem;
    use elephant_types::records::SourceType;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(70000);

    const SOURCE_SCHEMA: &str = r"
        DEFINE TABLE source SCHEMALESS;
    ";

    async fn setup_test_repo() -> SourceRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("source_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(SOURCE_SCHEMA).await.unwrap();

        SourceRepository::new(client)
    }

    async fn setup_embedded_test_repo() -> SourceRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        SourceRepository::new(client)
    }

    fn create_test_source(source_type: SourceType, hash: &str) -> Source {
        let id = RecordId::new_v4(TableName::Source);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Source::new(base, source_type, hash.to_string())
    }

    #[tokio::test]
    async fn test_create_source() {
        let repo = setup_test_repo().await;
        let source = create_test_source(SourceType::Email, "abc123");

        let created = repo.create(source.clone()).await.unwrap();

        assert_eq!(created.type_, SourceType::Email);
        assert_eq!(created.hash, "abc123");
    }

    async fn seed_uri_prefix_sources(repo: &SourceRepository) {
        let mut in_session_a = create_test_source(SourceType::Manual, "hash-a");
        in_session_a.uri = Some("angel://session/42/frame/1".to_string());
        repo.create(in_session_a).await.unwrap();

        let mut in_session_b = create_test_source(SourceType::Manual, "hash-b");
        in_session_b.uri = Some("angel://session/42/frame/2".to_string());
        repo.create(in_session_b).await.unwrap();

        let mut other_session = create_test_source(SourceType::Manual, "hash-c");
        other_session.uri = Some("angel://session/7/frame/1".to_string());
        repo.create(other_session).await.unwrap();

        // No uri at all: must not match (and must not error the query).
        repo.create(create_test_source(SourceType::Manual, "hash-d"))
            .await
            .unwrap();
    }

    async fn assert_find_by_uri_prefix(repo: &SourceRepository) {
        let matched = repo
            .find_by_uri_prefix("test_space", "angel://session/42/", 100)
            .await
            .unwrap();
        assert_eq!(matched.len(), 2);
        assert!(matched.iter().all(|source| {
            source
                .uri
                .as_deref()
                .is_some_and(|uri| uri.starts_with("angel://session/42/"))
        }));

        let missing = repo
            .find_by_uri_prefix("test_space", "angel://session/404/", 100)
            .await
            .unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_find_by_uri_prefix() {
        let repo = setup_test_repo().await;
        seed_uri_prefix_sources(&repo).await;
        assert_find_by_uri_prefix(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_find_by_uri_prefix() {
        let repo = setup_embedded_test_repo().await;
        seed_uri_prefix_sources(&repo).await;
        assert_find_by_uri_prefix(&repo).await;
    }

    #[tokio::test]
    async fn test_get_source() {
        let repo = setup_test_repo().await;
        let source = create_test_source(SourceType::Calendar, "def456");
        let id = source.base.id.clone();

        repo.create(source).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().type_, SourceType::Calendar);
    }

    #[tokio::test]
    async fn test_missing_hash_is_error() {
        let repo = setup_test_repo().await;
        let source = create_test_source(SourceType::Manual, "hash123");
        let id = source.base.id.clone();

        let created = repo.create(source).await.unwrap();

        repo.client
            .query_bind(
                "UPDATE $id SET hash = NONE;",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&created.base.id) },
            )
            .await
            .unwrap();

        let err = repo.get("test_space", &id).await.unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("Missing hash"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    async fn test_mark_graph_ready_if_quiescent_requires_no_pending_graph_work() {
        let repo = setup_embedded_test_repo().await;
        let source = create_test_source(SourceType::Manual, "graph-ready-hash");
        let source_id = source.base.id.clone();
        repo.create(source).await.unwrap();

        let pending_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "render_doc".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "render-doc:test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let current_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "commit_artifacts".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Running,
            priority: 5,
            idempotency_key: "commit-artifacts:test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        repo.client
            .embedded_store()
            .unwrap()
            .create(pending_item.clone())
            .await
            .unwrap();
        repo.client
            .embedded_store()
            .unwrap()
            .create(current_item.clone())
            .await
            .unwrap();

        let changed = repo
            .mark_graph_ready_if_quiescent(
                "test_space",
                &source_id,
                &current_item.base.id,
                &["render_doc", "commit_artifacts"],
            )
            .await
            .unwrap();
        assert!(!changed);
        let still_triaged = repo.get("test_space", &source_id).await.unwrap().unwrap();
        assert_eq!(
            still_triaged.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );

        repo.client
            .embedded_store()
            .unwrap()
            .hard_delete::<WorkItem>("test_space", &pending_item.base.id)
            .await
            .unwrap();

        let changed = repo
            .mark_graph_ready_if_quiescent(
                "test_space",
                &source_id,
                &current_item.base.id,
                &["render_doc", "commit_artifacts"],
            )
            .await
            .unwrap();
        assert!(changed);
        let graph_ready = repo.get("test_space", &source_id).await.unwrap().unwrap();
        assert_eq!(
            graph_ready.lifecycle_status,
            SourceLifecycleStatus::GraphReady
        );
        assert_eq!(graph_ready.current_ingest_mode, Some(IngestMode::Graph));
    }

    #[tokio::test]
    async fn test_mark_graph_ready_if_quiescent_ignores_historical_failed_graph_work() {
        let repo = setup_embedded_test_repo().await;
        let source = create_test_source(SourceType::Manual, "graph-ready-failed-hash");
        let source_id = source.base.id.clone();
        repo.create(source).await.unwrap();

        let failed_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "render_doc".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Failed,
            priority: 5,
            idempotency_key: "render-doc:failed".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("failed earlier".to_string()),
        };
        let current_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "commit_artifacts".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Running,
            priority: 5,
            idempotency_key: "commit-artifacts:current".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        repo.client
            .embedded_store()
            .unwrap()
            .create(failed_item)
            .await
            .unwrap();
        repo.client
            .embedded_store()
            .unwrap()
            .create(current_item.clone())
            .await
            .unwrap();

        let mut refreshed_source = repo.get("test_space", &source_id).await.unwrap().unwrap();
        refreshed_source.lifecycle_status = SourceLifecycleStatus::Discovered;
        refreshed_source.current_ingest_mode = None;
        refreshed_source.base.updated_at =
            current_item.base.updated_at + chrono::Duration::seconds(1);
        repo.update("test_space", &source_id, refreshed_source)
            .await
            .unwrap();

        let changed = repo
            .mark_graph_ready_if_quiescent(
                "test_space",
                &source_id,
                &current_item.base.id,
                &["render_doc", "commit_artifacts"],
            )
            .await
            .unwrap();
        assert!(changed);

        let graph_ready = repo.get("test_space", &source_id).await.unwrap().unwrap();
        assert_eq!(
            graph_ready.lifecycle_status,
            SourceLifecycleStatus::GraphReady
        );
    }

    #[tokio::test]
    async fn test_mark_graph_ready_if_quiescent_blocks_current_failed_graph_work() {
        let repo = setup_embedded_test_repo().await;
        let mut source = create_test_source(SourceType::Manual, "graph-ready-current-failed-hash");
        source.base.updated_at = Utc::now();
        let source_id = source.base.id.clone();
        repo.create(source.clone()).await.unwrap();

        let failed_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "extract_assertions".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Failed,
            priority: 5,
            idempotency_key: "extract-assertions:failed".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("failed in current run".to_string()),
        };
        let current_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            kind: "commit_artifacts".to_string(),
            payload: serde_json::json!({ "source_id": source_id.to_string() }),
            status: WorkItemStatus::Running,
            priority: 5,
            idempotency_key: "commit-artifacts:current".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        repo.client
            .embedded_store()
            .unwrap()
            .create(failed_item)
            .await
            .unwrap();
        repo.client
            .embedded_store()
            .unwrap()
            .create(current_item.clone())
            .await
            .unwrap();

        let changed = repo
            .mark_graph_ready_if_quiescent(
                "test_space",
                &source_id,
                &current_item.base.id,
                &["extract_assertions", "commit_artifacts"],
            )
            .await
            .unwrap();
        assert!(!changed);

        let persisted = repo.get("test_space", &source_id).await.unwrap().unwrap();
        assert_eq!(persisted.lifecycle_status, source.lifecycle_status);
    }
    async fn assert_archived_failure_blocks_graph_ready(repo: SourceRepository) {
        let cutoff = Utc::now() - chrono::Duration::days(30);
        let mut source = create_test_source(SourceType::Manual, "archive-failure");
        source.base.created_at = cutoff - chrono::Duration::days(3);
        source.base.updated_at = source.base.created_at;
        let source_id = source.base.id.clone();
        repo.create(source).await.unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "test_space".into(),
            "tester".into(),
        );
        base.created_at = cutoff - chrono::Duration::days(2);
        base.updated_at = base.created_at;
        let failed = WorkItem {
            base,
            kind: "extract_assertions".into(),
            payload: serde_json::json!({"source_id": source_id.to_string()}),
            status: WorkItemStatus::Failed,
            priority: 0,
            idempotency_key: "archived-current-failure".into(),
            attempts: 3,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("failed in current source run".into()),
        };
        let current = RecordId::new_v4(TableName::WorkItem);
        crate::WorkItemRepository::new(repo.client.clone())
            .create(failed)
            .await
            .unwrap();
        assert!(
            !repo
                .mark_graph_ready_if_quiescent(
                    "test_space",
                    &source_id,
                    &current,
                    &["extract_assertions"],
                )
                .await
                .unwrap(),
            "hot failure must block readiness"
        );
        let result = crate::work_retention::archive_terminal_work(&repo.client, cutoff, 100)
            .await
            .unwrap();
        assert_eq!(result.items_archived, 1);
        assert!(
            !repo
                .mark_graph_ready_if_quiescent(
                    "test_space",
                    &source_id,
                    &current,
                    &["extract_assertions"],
                )
                .await
                .unwrap(),
            "archival must not turn failure into success"
        );

        // A new source revision supersedes the old failed run.
        let mut refreshed = repo.get("test_space", &source_id).await.unwrap().unwrap();
        refreshed.base.updated_at = Utc::now();
        repo.update("test_space", &source_id, refreshed)
            .await
            .unwrap();
        assert!(
            repo.mark_graph_ready_if_quiescent(
                "test_space",
                &source_id,
                &current,
                &["extract_assertions"],
            )
            .await
            .unwrap(),
            "historical failure must not block a new source run"
        );
    }

    #[tokio::test]
    async fn test_archived_failure_blocks_graph_ready_surreal() {
        assert_archived_failure_blocks_graph_ready(setup_test_repo().await).await;
    }

    #[tokio::test]
    async fn test_archived_failure_blocks_graph_ready_embedded() {
        assert_archived_failure_blocks_graph_ready(setup_embedded_test_repo().await).await;
    }

    /// Guarded restore acts only on the snapshot the caller was authorized on.
    async fn assert_restore_guarded_lifecycle(
        repo: &SourceRepository,
        space: &str,
        record: Source,
    ) {
        let id = record.base.id.clone();
        repo.create(record).await.unwrap();

        // A fresh tombstone snapshot restores.
        repo.delete(space, &id).await.unwrap();
        let tombstone = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let restored = repo.restore_guarded(space, &id, &tombstone).await.unwrap();
        assert!(restored.base.deleted_at.is_none());

        // Tombstone ABA: snapshot D1, restored and deleted again as D2; the
        // stale restore from D1 conflicts and the row stays deleted.
        repo.delete(space, &id).await.unwrap();
        let tombstone_d1 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        repo.restore(space, &id).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        repo.delete(space, &id).await.unwrap();
        let result = repo.restore_guarded(space, &id, &tombstone_d1).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        assert!(repo.get(space, &id).await.unwrap().is_none());

        // Another space never matches.
        let tombstone_d2 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let result = repo
            .restore_guarded("other_space", &id, &tombstone_d2)
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_surreal() {
        let repo = setup_test_repo().await;
        assert_restore_guarded_lifecycle(
            &repo,
            "test_space",
            create_test_source(SourceType::Manual, "guarded-hash"),
        )
        .await;
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_embedded() {
        let repo = setup_embedded_test_repo().await;
        assert_restore_guarded_lifecycle(
            &repo,
            "test_space",
            create_test_source(SourceType::Manual, "guarded-hash"),
        )
        .await;
    }
}
