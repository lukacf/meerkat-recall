//! Work item repository implementation for `SurrealDB`.
//!
//! Per spec §5, work items represent durable jobs in the scheduler DAG.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

#[path = "managed_wiki_registration.rs"]
mod managed_wiki_registration;

#[path = "work_item_history.rs"]
mod work_item_history;

#[cfg(test)]
#[path = "work_queue_index_tests.rs"]
mod queue_index_tests;

// An OR expression forces SurrealDB 2.6.5 to scan the whole table even
// with an indexed status predicate. array::any preserves the same boolean
// condition while letting the planner use the selective queue index.
const RUNNABLE_WORK_FILTER: &str = "status = $status AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()])";
const RUNNING_COUNT_QUERY: &str = "SELECT count() AS count FROM work_item WHERE space_id = $space_id AND kind = $kind AND status = 'running' AND deleted_at IS NONE GROUP ALL";
const RUNNABLE_COUNT_QUERY: &str = "SELECT count() AS count FROM work_item WHERE status = 'runnable' AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) GROUP ALL";

#[derive(Deserialize)]
struct WorkCount {
    count: u64,
}

pub(crate) const WORK_ITEM_PROJECTION: &str = r"
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
    kind,
    payload,
    status,
    priority,
    idempotency_key,
    attempts,
    lease_expires_at,
    leased_by,
    run_after,
    last_error
";

/// Database-only proxy for WorkItem to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct WorkItemDb {
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
    kind: Option<String>,
    payload: Option<serde_json::Value>,
    status: Option<WorkItemStatus>,
    priority: Option<i32>,
    idempotency_key: Option<String>,
    attempts: Option<u32>,
    lease_expires_at: Option<chrono::DateTime<Utc>>,
    leased_by: Option<String>,
    run_after: Option<chrono::DateTime<Utc>>,
    last_error: Option<String>,
}

impl TryFrom<WorkItemDb> for WorkItem {
    type Error = StorageError;

    fn try_from(db: WorkItemDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: db
                    .space_id
                    .ok_or_else(|| StorageError::QueryError("Missing space_id".to_string()))?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: db
                    .created_by
                    .ok_or_else(|| StorageError::QueryError("Missing created_by".to_string()))?,
                deleted_at: db.deleted_at,
                provenance: db
                    .provenance
                    .ok_or_else(|| StorageError::QueryError("Missing provenance".to_string()))?,
                security: db
                    .security
                    .ok_or_else(|| StorageError::QueryError("Missing security".to_string()))?,
                subjects: db
                    .subjects
                    .ok_or_else(|| StorageError::QueryError("Missing subjects".to_string()))?,
                subjects_state: db.subjects_state.ok_or_else(|| {
                    StorageError::QueryError("Missing subjects_state".to_string())
                })?,
            },
            kind: db
                .kind
                .ok_or_else(|| StorageError::QueryError("Missing kind".to_string()))?,
            payload: db
                .payload
                .ok_or_else(|| StorageError::QueryError("Missing payload".to_string()))?,
            status: db
                .status
                .ok_or_else(|| StorageError::QueryError("Missing status".to_string()))?,
            priority: db
                .priority
                .ok_or_else(|| StorageError::QueryError("Missing priority".to_string()))?,
            idempotency_key: db
                .idempotency_key
                .ok_or_else(|| StorageError::QueryError("Missing idempotency_key".to_string()))?,
            attempts: db
                .attempts
                .ok_or_else(|| StorageError::QueryError("Missing attempts".to_string()))?,
            lease_expires_at: db.lease_expires_at,
            leased_by: db.leased_by,
            run_after: db.run_after,
            last_error: db.last_error,
        })
    }
}

/// Work item repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct WorkItemRepository {
    client: StorageClient,
}

impl WorkItemRepository {
    /// Creates a new work item repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a WorkItem to a SurrealDB value for insertion.
    pub(crate) fn work_item_to_value(item: &WorkItem) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::{Object, Value};

        let mut map = std::collections::BTreeMap::new();
        map.insert("space_id".to_string(), item.base.space_id.clone().into());
        map.insert(
            "created_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.created_at).into(),
        );
        map.insert(
            "updated_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.updated_at).into(),
        );
        map.insert(
            "created_by".to_string(),
            item.base.created_by.clone().into(),
        );
        map.insert(
            "deleted_at".to_string(),
            item.base.deleted_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        map.insert(
            "provenance".to_string(),
            crate::surreal::provenance_to_value(&item.base.provenance)?,
        );
        map.insert(
            "security".to_string(),
            crate::surreal::security_to_value(&item.base.security),
        );
        map.insert(
            "subjects".to_string(),
            Value::Array(crate::surreal::things_from_links(
                item.base.subjects.clone(),
            )),
        );
        map.insert(
            "subjects_state".to_string(),
            crate::surreal::enum_to_surreal(&item.base.subjects_state)?,
        );
        map.insert("kind".to_string(), item.kind.clone().into());
        map.insert(
            "payload".to_string(),
            crate::surreal::json_to_surreal(item.payload.clone())?,
        );
        map.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );
        map.insert("priority".to_string(), item.priority.into());
        map.insert(
            "idempotency_key".to_string(),
            item.idempotency_key.clone().into(),
        );
        map.insert("attempts".to_string(), item.attempts.into());
        map.insert(
            "lease_expires_at".to_string(),
            item.lease_expires_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        map.insert(
            "leased_by".to_string(),
            item.leased_by
                .as_ref()
                .map_or(Value::None, |id| Value::from(id.clone())),
        );
        map.insert(
            "run_after".to_string(),
            item.run_after.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        map.insert(
            "last_error".to_string(),
            item.last_error
                .as_ref()
                .map_or(Value::None, |err| Value::from(err.clone())),
        );

        Ok(Value::Object(Object::from(map)))
    }
}

#[async_trait]
impl Repository<WorkItem> for WorkItemRepository {
    async fn create(&self, record: WorkItem) -> StorageResult<WorkItem> {
        crate::work_retention::create_item(&self.client, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkItem>> {
        crate::work_retention::get_item(&self.client, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkItem,
    ) -> StorageResult<WorkItem> {
        crate::work_retention::update_item(&self.client, space_id, id, record, None)
            .await?
            .ok_or_else(|| StorageError::NotFound(id.to_string()))
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(mut item) = self.get(space_id, id).await? {
            item.base.deleted_at = Some(Utc::now());
            self.update(space_id, id, item).await?;
        }
        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<WorkItem>> {
        crate::work_retention::list_items(&self.client, space_id, limit, cursor).await
    }
}

/// Additional work-item-specific operations.
impl WorkItemRepository {
    fn is_runnable_now(item: &WorkItem, now: DateTime<Utc>) -> bool {
        item.status == WorkItemStatus::Runnable
            && item.run_after.is_none_or(|run_after| run_after <= now)
    }

    fn payload_source_id(payload: &serde_json::Value) -> Option<&str> {
        payload
            .as_object()
            .and_then(|object| object.get("source_id"))
            .and_then(serde_json::Value::as_str)
    }

    /// Gets runnable work items (pending with all dependencies met).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_runnable(&self, space_id: &str, limit: usize) -> StorageResult<Vec<WorkItem>> {
        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .work_items_by_status(space_id, WorkItemStatus::Runnable, None, limit, Some(now))
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = 'runnable' AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Updates the status of a work item.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn update_status(
        &self,
        space_id: &str,
        id: &RecordId,
        status: WorkItemStatus,
    ) -> StorageResult<()> {
        if let Some(mut item) = self.get(space_id, id).await? {
            let expected = item.status;
            item.status = status;
            self.update_if_status(space_id, id, item, expected).await?;
        }
        Ok(())
    }

    /// Retries exactly the failed work item authorized by a request boundary.
    /// The guard and hot/cold reactivation are one transaction.
    ///
    /// # Errors
    ///
    /// Returns a conflict if the authorized pre-state changed or is not failed.
    pub async fn retry_authorized(&self, observed: &WorkItem) -> StorageResult<WorkItem> {
        let conflict = || crate::lifecycle_guard::guard_conflict("WorkItem", &observed.base.id);
        if observed.status != WorkItemStatus::Failed || observed.base.deleted_at.is_some() {
            return Err(conflict());
        }
        let mut retry = observed.clone();
        retry.status = WorkItemStatus::Runnable;
        retry.leased_by = None;
        retry.lease_expires_at = None;
        retry.run_after = None;
        crate::work_retention::update_item_guarded(
            &self.client,
            &observed.base.space_id,
            &observed.base.id,
            retry,
            Some(WorkItemStatus::Failed),
            Some(observed),
        )
        .await?
        .ok_or_else(conflict)
    }

    /// Finds a work item by idempotency key.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_idempotency_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_retention::find_item_key(&self.client, space_id, key).await
    }

    /// Finds work items by status.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_status_limited(
        &self,
        space_id: &str,
        status: WorkItemStatus,
        limit: usize,
    ) -> StorageResult<Vec<WorkItem>> {
        if matches!(
            status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            return crate::work_retention::terminal_items(
                &self.client,
                space_id,
                status,
                None,
                Some(limit),
            )
            .await;
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .work_items_by_status(
                    space_id,
                    status,
                    None,
                    limit,
                    (status == WorkItemStatus::Runnable).then_some(now),
                )
                .await;
        }

        let run_after_clause = if status == WorkItemStatus::Runnable {
            " AND array::any([run_after IS NONE, run_after <= time::now()])"
        } else {
            ""
        };
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE{run_after_clause} ORDER BY priority DESC, created_at ASC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id, "status" => status.to_string() },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Finds all work items by status.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_status(
        &self,
        space_id: &str,
        status: WorkItemStatus,
    ) -> StorageResult<Vec<WorkItem>> {
        if matches!(
            status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            return crate::work_retention::terminal_items(
                &self.client,
                space_id,
                status,
                None,
                None,
            )
            .await;
        }
        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .work_items_by_status(
                    space_id,
                    status,
                    None,
                    usize::MAX,
                    (status == WorkItemStatus::Runnable).then_some(now),
                )
                .await;
        }

        let run_after_clause = if status == WorkItemStatus::Runnable {
            " AND array::any([run_after IS NONE, run_after <= time::now()])"
        } else {
            ""
        };
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE{run_after_clause} ORDER BY priority DESC, created_at ASC"
                ),
                crate::bindings! { "space_id" => space_id, "status" => status.to_string() },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Finds runnable work items across all spaces, ordered by priority.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_runnable_all_spaces(&self, limit: usize) -> StorageResult<Vec<WorkItem>> {
        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| item.base.deleted_at.is_none() && Self::is_runnable_now(item, now))
                .collect();
            sort_work_items(&mut items);
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE {RUNNABLE_WORK_FILTER} ORDER BY priority DESC, created_at ASC LIMIT {limit}"
                ),
                crate::bindings! { "status" => WorkItemStatus::Runnable.to_string() },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Finds runnable work items for a specific kind across all spaces, ordered by priority.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_runnable_by_kind_all_spaces(
        &self,
        kind: &str,
        limit: usize,
    ) -> StorageResult<Vec<WorkItem>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.deleted_at.is_none()
                        && item.kind == kind
                        && Self::is_runnable_now(item, now)
                })
                .collect();
            sort_work_items(&mut items);
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE status = $status AND kind = $kind AND deleted_at IS NONE AND array::any([run_after IS NONE, run_after <= time::now()]) ORDER BY priority DESC, created_at ASC LIMIT {limit}"
                ),
                crate::bindings! {
                    "status" => WorkItemStatus::Runnable.to_string(),
                    "kind" => kind,
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Finds work items by status and kind.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_status_and_kind(
        &self,
        space_id: &str,
        status: WorkItemStatus,
        kind: &str,
        limit: usize,
    ) -> StorageResult<Vec<WorkItem>> {
        if matches!(
            status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            return crate::work_retention::terminal_items(
                &self.client,
                space_id,
                status,
                Some(kind),
                Some(limit),
            )
            .await;
        }
        if limit == 0 {
            return Ok(Vec::new());
        }

        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .work_items_by_status(
                    space_id,
                    status,
                    Some(kind),
                    limit,
                    (status == WorkItemStatus::Runnable).then_some(now),
                )
                .await;
        }

        let run_after_clause = if status == WorkItemStatus::Runnable {
            " AND array::any([run_after IS NONE, run_after <= time::now()])"
        } else {
            ""
        };
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND status = $status AND kind = $kind AND deleted_at IS NONE{run_after_clause} ORDER BY priority DESC, created_at ASC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "status" => status.to_string(),
                    "kind" => kind,
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Finds work items for a specific source payload, filtered by status and kind.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_source_id_and_statuses_and_kinds(
        &self,
        space_id: &str,
        source_id: &RecordId,
        statuses: &[WorkItemStatus],
        kinds: &[&str],
    ) -> StorageResult<Vec<WorkItem>> {
        if statuses.is_empty() || kinds.is_empty() {
            return Ok(Vec::new());
        }

        let now = Utc::now();
        let source_id_string = source_id.to_string();
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && statuses.contains(&item.status)
                        && kinds.contains(&item.kind.as_str())
                        && Self::payload_source_id(&item.payload)
                            .is_some_and(|value| value == source_id_string)
                        && (item.status != WorkItemStatus::Runnable
                            || item.run_after.is_none_or(|run_after| run_after <= now))
                })
                .collect();
            sort_work_items(&mut items);
            return Ok(items);
        }

        let statuses = statuses.iter().map(ToString::to_string).collect::<Vec<_>>();
        let kinds = kinds.iter().map(ToString::to_string).collect::<Vec<_>>();
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item \
                     WHERE space_id = $space_id \
                       AND status INSIDE $statuses \
                       AND kind INSIDE $kinds \
                       AND payload.source_id = $source_id \
                       AND deleted_at IS NONE \
                       AND array::any([status != 'runnable', run_after IS NONE, run_after <= time::now()]) \
                     ORDER BY priority DESC, created_at ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "statuses" => statuses,
                    "kinds" => kinds,
                    "source_id" => source_id_string,
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_items: {e}")))?;

        items.into_iter().map(WorkItem::try_from).collect()
    }

    /// Pauses queued work items for a given ingestion mode until the resume time.
    ///
    /// Returns the number of items updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or deserialization fails.
    pub async fn pause_queued_by_mode(
        &self,
        space_id: &str,
        mode: IngestMode,
        resume_at: DateTime<Utc>,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let mut count = 0usize;
                    for mut item in mutation.scan::<WorkItem>()? {
                        if item.base.space_id != space_id
                            || item.base.deleted_at.is_some()
                            || !matches!(
                                item.status,
                                WorkItemStatus::Runnable | WorkItemStatus::Pending
                            )
                            || !payload_matches_mode(&item.payload, mode)
                        {
                            continue;
                        }

                        item.run_after = Some(resume_at);
                        item.base.updated_at = Utc::now();
                        let _ = mutation.replace(item)?;
                        count += 1;
                    }
                    Ok(count)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                r"UPDATE work_item SET run_after = $run_after, updated_at = time::now()
                  WHERE space_id = $space_id
                    AND deleted_at IS NONE
                    AND (status = 'runnable' OR status = 'pending')
                    AND (
                        payload.mode = $mode
                        OR payload.target_mode = $mode
                        OR payload.ingest_mode = $mode
                        OR payload.decided_mode = $mode
                    )
                  RETURN AFTER",
                crate::bindings! {
                    "space_id" => space_id,
                    "run_after" => resume_at.to_rfc3339(),
                    "mode" => mode.to_string(),
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse paused work_items: {e}"))
        })?;

        Ok(items.len())
    }

    /// Returns the most recently updated active work item of one kind,
    /// including future runnable items.
    ///
    /// This lets event-driven maintenance
    /// reuse the durable configuration carried by an existing managed chain.
    pub async fn find_latest_by_kind(
        &self,
        space_id: &str,
        kind: &str,
    ) -> StorageResult<Option<WorkItem>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.kind == kind
                        && item.status != WorkItemStatus::Cancelled
                        && item.status != WorkItemStatus::Failed
                })
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .base
                    .updated_at
                    .cmp(&left.base.updated_at)
                    .then_with(|| right.base.id.cmp(&left.base.id))
            });
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "SELECT {WORK_ITEM_PROJECTION} FROM work_item WHERE space_id = $space_id AND kind = $kind AND status != 'cancelled' AND status != 'failed' AND deleted_at IS NONE ORDER BY updated_at DESC, id DESC LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "kind" => kind },
            )
            .await?;
        let items: Vec<WorkItemDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse latest work item: {error}"))
        })?;
        items.into_iter().next().map(WorkItem::try_from).transpose()
    }

    /// Cancels queued items of one kind except the replacement item.
    /// Running work retains its lease and is never cancelled here.
    pub async fn cancel_queued_by_kind_except(
        &self,
        space_id: &str,
        kind: &str,
        keep_id: &RecordId,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let mut count = 0_usize;
                    for mut item in mutation.scan::<WorkItem>()? {
                        if item.base.space_id != space_id
                            || item.base.deleted_at.is_some()
                            || item.kind != kind
                            || item.base.id == *keep_id
                            || !matches!(
                                item.status,
                                WorkItemStatus::Runnable | WorkItemStatus::Pending
                            )
                        {
                            continue;
                        }
                        item.status = WorkItemStatus::Cancelled;
                        item.last_error = Some("superseded_by_graph_change".to_string());
                        item.base.updated_at = Utc::now();
                        let _ = mutation.replace(item)?;
                        count = count.saturating_add(1);
                    }
                    Ok(count)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE work_item SET status = 'cancelled', last_error = 'superseded_by_graph_change', updated_at = time::now() WHERE space_id = $space_id AND kind = $kind AND id != $keep_id AND (status = 'runnable' OR status = 'pending') AND deleted_at IS NONE RETURN {WORK_ITEM_PROJECTION}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "kind" => kind,
                    "keep_id" => crate::surreal::thing_from_record_id(keep_id),
                },
            )
            .await?;
        let items: Vec<WorkItemDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse cancelled work items: {error}"))
        })?;
        Ok(items.len())
    }

    /// Counts running work items for a specific kind.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or the count is missing.
    pub async fn count_running_by_kind(&self, space_id: &str, kind: &str) -> StorageResult<u32> {
        if let Some(store) = self.client.embedded_store() {
            let count = store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.kind == kind
                        && item.status == WorkItemStatus::Running
                })
                .count();
            return u32::try_from(count).map_err(|_| {
                StorageError::QueryError("Running count exceeds u32 range".to_string())
            });
        }

        let mut response = self
            .client
            .query_bind_checked(
                RUNNING_COUNT_QUERY,
                crate::bindings! { "space_id" => space_id, "kind" => kind },
            )
            .await?;

        let rows: Vec<WorkCount> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_item count: {e}"))
        })?;

        let Some(row) = rows.into_iter().next() else {
            return Ok(0);
        };

        u32::try_from(row.count)
            .map_err(|_| StorageError::QueryError("Running count exceeds u32 range".to_string()))
    }

    /// Counts all currently runnable work without the claim candidate limit.
    /// Future scheduled and soft-deleted items are excluded on both backends.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or the count exceeds `usize`.
    pub async fn count_runnable(&self) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let now = Utc::now();
            return Ok(store
                .scan::<WorkItem>()
                .await?
                .into_iter()
                .filter(|item| item.base.deleted_at.is_none() && Self::is_runnable_now(item, now))
                .count());
        }

        let mut response = self.client.query_checked(RUNNABLE_COUNT_QUERY).await?;
        let rows: Vec<WorkCount> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse runnable work count: {error}"))
        })?;
        let count = rows.first().map_or(0, |row| row.count);
        usize::try_from(count)
            .map_err(|_| StorageError::QueryError("Runnable count exceeds usize range".to_string()))
    }

    /// Updates a work item only if it matches the expected status.
    ///
    /// Returns `Ok(None)` when the status does not match, indicating another
    /// worker has already transitioned the work item.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn update_if_status(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkItem,
        expected_status: WorkItemStatus,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_retention::update_item(
            &self.client,
            space_id,
            id,
            record,
            Some(expected_status),
        )
        .await
    }

    /// Updates a work item only if it matches the expected status and all dependencies are done.
    ///
    /// Returns `Ok(None)` when the status does not match or dependencies are not satisfied.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn update_if_status_and_dependencies_done(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: WorkItem,
        expected_status: WorkItemStatus,
    ) -> StorageResult<Option<WorkItem>> {
        if expected_status == WorkItemStatus::Running || record.status == WorkItemStatus::Running {
            return Err(StorageError::Conflict(
                "work_attempt_required: dependency promotion cannot claim or finish a lease".into(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(existing) = mutation.get::<WorkItem>(space_id, id, false)? else {
                        return Ok(None);
                    };
                    if existing.status != expected_status
                        || has_pending_dependencies(mutation, space_id, id)?
                    {
                        return Ok(None);
                    }

                    record.base.id = id.clone();
                    record.base.space_id = space_id.to_string();
                    record.base.updated_at = Utc::now();
                    Ok(Some(mutation.replace(record)?))
                })
                .await;
        }

        record.base.space_id = space_id.to_string();
        record.base.id.clone_from(id);
        record.base.updated_at = Utc::now();

        // Resolve only this item's predecessors, with exact record targets. A
        // correlated WHERE on the archive table scans cold history in SurrealDB.
        // Pass the whole Thing to type::thing so native UUID identity survives;
        // meta::id would convert the UUID to a different string record ID.
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "BEGIN TRANSACTION;
                     LET $parents = SELECT VALUE from_item FROM work_edge
                         WHERE space_id = $space_id AND to_item = $id AND deleted_at IS NONE;
                     LET $archives = SELECT VALUE type::thing('work_item_archive', from_item)
                         FROM work_edge
                         WHERE space_id = $space_id AND to_item = $id AND deleted_at IS NONE;
                     LET $done = SELECT VALUE id FROM $parents
                         WHERE space_id = $space_id AND status = $dependency_status
                         AND deleted_at IS NONE;
                     LET $cold_done = SELECT VALUE original_id FROM $archives
                         WHERE space_id = $space_id AND record.status = $dependency_status
                         AND deleted_at IS NONE;
                     LET $updated = UPDATE $id MERGE $record
                         WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE
                         AND array::len(array::complement($parents, array::concat($done, $cold_done))) = 0
                         RETURN {WORK_ITEM_PROJECTION};
                     IF array::len($updated) > 0 {{ {retire_archive} }};
                     RETURN $updated;
                     COMMIT TRANSACTION;",
                    retire_archive = crate::work_retention::RETIRE_ARCHIVE_SQL
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "space" => space_id,
                    "archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(id)),
                    "status" => expected_status.to_string(),
                    "dependency_status" => WorkItemStatus::Done.to_string(),
                    "record" => Self::work_item_to_value(&record)?,
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response.take(response.num_statements() - 1).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_item update: {e}"))
        })?;
        if items.is_empty() {
            return Ok(None);
        }

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Empty result after successful update".to_string())
        })?;

        Ok(Some(WorkItem::try_from(item_db)?))
    }

    /// Acquires a lease on a work item for processing.
    ///
    /// Uses atomic update with WHERE clause to prevent race conditions.
    /// Increments the attempts counter on successful lease acquisition.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn acquire_lease(
        &self,
        space_id: &str,
        id: &RecordId,
        worker_id: &str,
        lease_duration_secs: u64,
    ) -> StorageResult<bool> {
        let duration = i64::try_from(lease_duration_secs)
            .ok()
            .and_then(chrono::Duration::try_seconds)
            .ok_or_else(|| {
                StorageError::DataIntegrityError("Invalid lease duration".to_string())
            })?;
        Ok(self
            .claim_with_lease(space_id, id, worker_id, Utc::now() + duration)
            .await?
            .is_some())
    }

    /// Extends a running work item's lease while preserving ownership.
    ///
    /// Returns `None` if the item is no longer running or is owned by another
    /// worker. The caller chooses the absolute expiry so long-running work can
    /// align its lease with its execution timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the conditional update fails.
    pub async fn renew_lease(
        &self,
        space_id: &str,
        id: &RecordId,
        worker_id: &str,
        lease_expires: chrono::DateTime<Utc>,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_attempt_repository::transition(
            &self.client,
            space_id,
            id,
            worker_id,
            WorkItemStatus::Running,
            Some(lease_expires),
            None,
            None,
            false,
        )
        .await
    }

    /// Claims a work item with a lease using optimistic locking.
    ///
    /// Only succeeds if the item is still in Runnable status, preventing race conditions.
    /// Returns the updated item if successful, None if the item was already claimed.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails (not just if the item was claimed).
    pub async fn claim_with_lease(
        &self,
        space_id: &str,
        id: &RecordId,
        worker_id: &str,
        lease_expires: chrono::DateTime<chrono::Utc>,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_attempt_repository::claim(&self.client, space_id, id, worker_id, lease_expires)
            .await
    }

    /// Claim only the exact work envelope already authorized by the caller.
    ///
    /// # Errors
    /// Propagates storage errors; changed authority or unavailable work returns None.
    pub async fn claim_authorized(
        &self,
        observed: &WorkItem,
        worker: &str,
        expires: chrono::DateTime<chrono::Utc>,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_attempt_repository::claim_observed(
            &self.client,
            &observed.base.space_id,
            &observed.base.id,
            worker,
            expires,
            Some(observed),
        )
        .await
    }

    /// Finish the authorized observed envelope with the caller's original token.
    ///
    /// # Errors
    /// Rejects nonterminal outcomes; changed authority or lost lease returns None.
    pub async fn finish_authorized(
        &self,
        observed: &WorkItem,
        original_token: &str,
        status: WorkItemStatus,
        error: Option<String>,
    ) -> StorageResult<Option<WorkItem>> {
        if !matches!(status, WorkItemStatus::Done | WorkItemStatus::Failed) {
            return Err(StorageError::DataIntegrityError(
                "Authorized completion requires a terminal work outcome".into(),
            ));
        }
        crate::work_attempt_repository::transition_observed(
            &self.client,
            &observed.base.space_id,
            &observed.base.id,
            original_token,
            status,
            None,
            None,
            error,
            false,
            Some(observed),
        )
        .await
    }

    /// Updates a running work item owned by the worker, clearing its lease.
    ///
    /// Returns None if the work item is not leased by the worker or not running.
    pub async fn update_running_for_worker(
        &self,
        space_id: &str,
        id: &RecordId,
        worker_id: &str,
        status: WorkItemStatus,
        last_error: Option<String>,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_attempt_repository::transition(
            &self.client,
            space_id,
            id,
            worker_id,
            status,
            None,
            None,
            last_error,
            false,
        )
        .await
    }

    /// Requeues a running work item with a delay, clearing its lease.
    ///
    /// Returns None if the work item is not leased by the worker or not running.
    pub async fn requeue_with_delay(
        &self,
        space_id: &str,
        id: &RecordId,
        worker_id: &str,
        delay_secs: i64,
        last_error: Option<String>,
    ) -> StorageResult<Option<WorkItem>> {
        let delay = chrono::Duration::try_seconds(delay_secs.max(0)).ok_or_else(|| {
            StorageError::DataIntegrityError(format!("Invalid retry delay: {delay_secs}"))
        })?;
        crate::work_attempt_repository::transition(
            &self.client,
            space_id,
            id,
            worker_id,
            WorkItemStatus::Runnable,
            None,
            Some(Utc::now() + delay),
            last_error,
            false,
        )
        .await
    }

    /// Recoverable configuration failures do not consume the transient retry
    /// allowance. The durable attempt generation remains monotonic.
    pub async fn requeue_configuration_blocked(
        &self,
        space_id: &str,
        id: &RecordId,
        token: &str,
        delay_secs: i64,
        message: String,
    ) -> StorageResult<Option<WorkItem>> {
        let delay = chrono::Duration::try_seconds(delay_secs.clamp(30, 3600)).ok_or_else(|| {
            StorageError::DataIntegrityError("Invalid configuration retry delay".to_string())
        })?;
        self.requeue_without_attempt_charge(
            space_id,
            id,
            token,
            delay,
            format!("configuration_blocked: {message}"),
        )
        .await
    }

    /// Rechecks unfinished local prerequisites without charging a provider retry.
    ///
    /// The original live attempt must own the transition. Dependency polling has
    /// a one-second floor, independent of configuration-repair backoff.
    pub async fn requeue_pending_dependencies(
        &self,
        space_id: &str,
        id: &RecordId,
        token: &str,
        delay_secs: i64,
        message: String,
    ) -> StorageResult<Option<WorkItem>> {
        let delay = chrono::Duration::try_seconds(delay_secs.clamp(1, 3600)).ok_or_else(|| {
            StorageError::DataIntegrityError("Invalid dependency retry delay".to_string())
        })?;
        self.requeue_without_attempt_charge(
            space_id,
            id,
            token,
            delay,
            format!("pending_dependencies: {message}"),
        )
        .await
    }

    async fn requeue_without_attempt_charge(
        &self,
        space_id: &str,
        id: &RecordId,
        token: &str,
        delay: chrono::Duration,
        message: String,
    ) -> StorageResult<Option<WorkItem>> {
        crate::work_attempt_repository::transition(
            &self.client,
            space_id,
            id,
            token,
            WorkItemStatus::Runnable,
            None,
            Some(Utc::now() + delay),
            Some(message),
            true,
        )
        .await
    }

    /// Atomically releases every expired Running lease, independent of space
    /// activity. Missing expiry is malformed ownership and is recoverable too.
    pub async fn recover_expired_leases_all_spaces(&self) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let now = Utc::now();
                    let mut recovered = 0;
                    for item in mutation.scan::<WorkItem>()? {
                        if item.base.deleted_at.is_some()
                            || item.status != WorkItemStatus::Running
                            || item.lease_expires_at.is_some_and(|expiry| expiry > now)
                        {
                            continue;
                        }
                        crate::work_attempt_repository::recover_embedded(mutation, item, now)?;
                        recovered += 1;
                    }
                    Ok(recovered)
                })
                .await;
        }
        let mut response = self.client.query_checked(
            "BEGIN TRANSACTION;
             LET $expired = SELECT VALUE id FROM work_item WHERE status = 'running' AND deleted_at IS NONE AND array::any([lease_expires_at IS NONE, lease_expires_at <= time::now()]);
             UPDATE work_attempt SET status = 'runnable', lease_expires_at = NONE WHERE work_item_id IN $expired;
             UPDATE $expired SET status = 'runnable', leased_by = NONE, lease_expires_at = NONE, last_error = 'lease_expired: recovered abandoned attempt', updated_at = time::now();
             RETURN $expired; COMMIT TRANSACTION;"
        ).await?;
        let recovered: Vec<surrealdb::sql::Thing> = response
            .take(response.num_statements() - 1)
            .map_err(|error| {
                StorageError::DataIntegrityError(format!("Invalid lease recovery result: {error}"))
            })?;
        Ok(recovered.len())
    }

    /// Releases an expired lease, returning the item to runnable state.
    ///
    /// Returns None if the lease is not expired or the item is not running.
    pub async fn release_expired_lease(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<WorkItem>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(item) = mutation.get::<WorkItem>(space_id, id, false)? else {
                        return Ok(None);
                    };
                    let lease_expired = item
                        .lease_expires_at
                        .is_none_or(|expires| expires <= Utc::now());
                    if item.status != WorkItemStatus::Running || !lease_expired {
                        return Ok(None);
                    }

                    Ok(Some(crate::work_attempt_repository::recover_embedded(
                        mutation,
                        item,
                        Utc::now(),
                    )?))
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("BEGIN TRANSACTION;
                LET $updated = UPDATE $id SET status = 'runnable', leased_by = NONE, lease_expires_at = NONE, updated_at = time::now() WHERE space_id = $space_id AND status = 'running' AND deleted_at IS NONE AND array::any([lease_expires_at IS NONE, lease_expires_at <= time::now()]) RETURN {WORK_ITEM_PROJECTION};
                IF array::len($updated) > 0 {{ UPDATE $attempt_id SET status = 'runnable', lease_expires_at = NONE; }};
                RETURN $updated; COMMIT TRANSACTION;"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "attempt_id" => crate::work_attempt_repository::attempt_id(id),
                },
            )
            .await?;

        let items: Vec<WorkItemDb> = response.take(response.num_statements() - 1).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_item lease release: {e}"))
        })?;
        if items.is_empty() {
            return Ok(None);
        }

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Empty result after successful update".to_string())
        })?;

        Ok(Some(WorkItem::try_from(item_db)?))
    }
}

fn sort_work_items(items: &mut [WorkItem]) {
    items.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.base.created_at.cmp(&right.base.created_at))
    });
}

fn payload_matches_mode(payload: &serde_json::Value, mode: IngestMode) -> bool {
    let mode_value = mode.to_string();
    ["mode", "target_mode", "ingest_mode", "decided_mode"]
        .iter()
        .any(|key| {
            payload.get(key).and_then(serde_json::Value::as_str) == Some(mode_value.as_str())
        })
}

fn has_pending_dependencies(
    mutation: &crate::embedded::EmbeddedMutation<'_>,
    space_id: &str,
    item_id: &RecordId,
) -> StorageResult<bool> {
    let edges = mutation.scan::<elephant_types::records::work::WorkEdge>()?;
    for edge in edges {
        if edge.base.space_id != space_id
            || edge.base.deleted_at.is_some()
            || edge.to.id() != item_id
        {
            continue;
        }
        let hot = mutation.get::<WorkItem>(&edge.base.space_id, edge.from.id(), false)?;
        let cold = mutation.get::<elephant_types::records::WorkItemArchive>(
            &edge.base.space_id,
            &crate::work_retention::archive_id(edge.from.id()),
            false,
        )?;
        if hot
            .as_ref()
            .or_else(|| cold.as_ref().map(|v| &v.record))
            .is_none_or(|v| v.status != WorkItemStatus::Done)
        {
            return Ok(true);
        }
    }
    Ok(false)
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
    use crate::WorkEdgeRepository;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::work::WorkEdge;
    use elephant_types::Link;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80000);

    const WORK_ITEM_SCHEMA: &str = r"
        DEFINE TABLE work_item SCHEMALESS;
    ";
    const WORK_EDGE_SCHEMA: &str = r"
        DEFINE TABLE work_edge SCHEMALESS;
    ";

    async fn setup_test_client() -> StorageClient {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("work_item_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query_checked(WORK_ITEM_SCHEMA).await.unwrap();
        client.query_checked(WORK_EDGE_SCHEMA).await.unwrap();

        client
    }

    async fn setup_test_repo() -> WorkItemRepository {
        let client = setup_test_client().await;
        WorkItemRepository::new(client)
    }

    async fn setup_embedded_test_repo() -> WorkItemRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        WorkItemRepository::new(client)
    }

    async fn setup_test_repos() -> (WorkItemRepository, WorkEdgeRepository) {
        let client = setup_test_client().await;
        (
            WorkItemRepository::new(client.clone()),
            WorkEdgeRepository::new(client),
        )
    }

    fn create_test_work_item(kind: &str) -> WorkItem {
        let id = RecordId::new_v4(TableName::WorkItem);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        WorkItem {
            base,
            kind: kind.to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Pending,
            priority: 0,
            idempotency_key: format!("test-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    fn make_work_item_db() -> WorkItemDb {
        WorkItemDb {
            id: surrealdb::sql::Thing::from(("work_item", "item_1")),
            space_id: Some("test_space".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("test_agent".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            kind: Some("extract".to_string()),
            payload: Some(serde_json::json!({"doc_id": "doc:1"})),
            status: Some(WorkItemStatus::Pending),
            priority: Some(0),
            idempotency_key: Some("key-1".to_string()),
            attempts: Some(0),
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[test]
    fn test_work_item_missing_payload_errors() {
        let mut db = make_work_item_db();
        db.payload = None;

        let result = WorkItem::try_from(db);
        assert!(result.is_err(), "Missing payload should error");
    }

    #[test]
    fn test_work_item_missing_priority_errors() {
        let mut db = make_work_item_db();
        db.priority = None;

        let result = WorkItem::try_from(db);
        assert!(result.is_err(), "Missing priority should error");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_work_item() {
        let repo = setup_test_repo().await;
        let item = create_test_work_item("extract");

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.kind, "extract");
        assert_eq!(created.status, WorkItemStatus::Pending);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_work_item() {
        let repo = setup_test_repo().await;
        let item = create_test_work_item("embed");
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().kind, "embed");
    }

    #[tokio::test]
    async fn test_update_if_status_requires_dependencies_done() {
        let (item_repo, edge_repo) = setup_test_repos().await;
        let item_a = create_test_work_item("a");
        let item_b = create_test_work_item("b");

        let a = item_repo.create(item_a).await.unwrap();
        let b = item_repo.create(item_b).await.unwrap();

        let edge = WorkEdge {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkEdge),
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            from: Link::new(a.base.id.clone()),
            to: Link::new(b.base.id.clone()),
            edge_type: "depends_on".to_string(),
        };
        edge_repo.create(edge).await.unwrap();

        let mut b_updated = b.clone();
        b_updated.status = WorkItemStatus::Runnable;
        let blocked = item_repo
            .update_if_status_and_dependencies_done(
                "test_space",
                &b.base.id,
                b_updated.clone(),
                WorkItemStatus::Pending,
            )
            .await
            .unwrap();
        assert!(blocked.is_none());

        let mut a_done = a.clone();
        a_done.status = WorkItemStatus::Done;
        item_repo
            .update("test_space", &a.base.id, a_done)
            .await
            .unwrap();

        let deps = edge_repo.find_dependencies(&b.base.id).await.unwrap();
        assert_eq!(deps.len(), 1);
        let dep_item = item_repo
            .get("test_space", deps[0].from.id())
            .await
            .unwrap()
            .expect("dependency item");
        assert_eq!(dep_item.status, WorkItemStatus::Done);

        let promoted = item_repo
            .update_if_status_and_dependencies_done(
                "test_space",
                &b.base.id,
                b_updated,
                WorkItemStatus::Pending,
            )
            .await
            .unwrap();
        assert!(promoted.is_some());
        assert_eq!(promoted.unwrap().status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_work_item_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_item("promote");
        item.priority = 10;
        item.payload = serde_json::json!({
            "source_id": "source:abc123",
            "options": {"verbose": true}
        });

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.kind, "promote");
        assert_eq!(created.priority, 10);
        assert_eq!(created.payload["options"]["verbose"], true);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_enforces_expected_status() {
        let repo = setup_test_repo().await;
        let item = create_test_work_item("transition");
        let created = repo.create(item).await.expect("create");

        let mut runnable = created.clone();
        runnable.status = WorkItemStatus::Runnable;

        let updated = repo
            .update_if_status(
                "test_space",
                &created.base.id,
                runnable.clone(),
                WorkItemStatus::Pending,
            )
            .await
            .expect("update");
        assert!(updated.is_some());
        assert_eq!(updated.unwrap().status, WorkItemStatus::Runnable);

        let second = repo
            .update_if_status(
                "test_space",
                &created.base.id,
                runnable,
                WorkItemStatus::Pending,
            )
            .await
            .expect("second update");
        assert!(second.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_uses_target_id_not_payload_id() {
        let repo = setup_test_repo().await;
        let target = repo
            .create(create_test_work_item("target"))
            .await
            .expect("create target");
        let other = repo
            .create(create_test_work_item("other"))
            .await
            .expect("create other");

        let mut payload = other.clone();
        payload.status = WorkItemStatus::Runnable;

        let updated = repo
            .update_if_status(
                "test_space",
                &target.base.id,
                payload,
                WorkItemStatus::Pending,
            )
            .await
            .expect("update target")
            .expect("status match");

        assert_eq!(updated.base.id, target.base.id);

        let fetched_target = repo
            .get("test_space", &target.base.id)
            .await
            .expect("get target")
            .expect("target item");
        assert_eq!(fetched_target.status, WorkItemStatus::Runnable);

        let fetched_other = repo
            .get("test_space", &other.base.id)
            .await
            .expect("get other")
            .expect("other item");
        assert_eq!(fetched_other.status, WorkItemStatus::Pending);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_requeue_with_delay() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_item("requeue");
        item.status = WorkItemStatus::Runnable;
        let created = repo.create(item).await.expect("create");
        let claimed = repo
            .claim_with_lease(
                "test_space",
                &created.base.id,
                "worker-1",
                Utc::now() + chrono::Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("claimed");
        let token = claimed.leased_by.as_deref().expect("token");
        let updated = repo
            .requeue_with_delay(
                "test_space",
                &created.base.id,
                token,
                30,
                Some("queued".to_string()),
            )
            .await
            .expect("requeue");

        let updated = updated.expect("requeued item");
        assert_eq!(updated.status, WorkItemStatus::Runnable);
        assert!(updated.run_after.is_some());
        assert!(updated.leased_by.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_release_expired_lease_does_not_release_unexpired_lease() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_item("lease");
        item.status = WorkItemStatus::Runnable;

        let created = repo.create(item).await.expect("create");
        let acquired = repo
            .acquire_lease("test_space", &created.base.id, "worker-1", 60)
            .await
            .expect("acquire lease");
        assert!(acquired);

        let released = repo
            .release_expired_lease("test_space", &created.base.id)
            .await
            .expect("release expired lease");
        assert!(released.is_none(), "unexpired lease must not be released");

        let fetched = repo
            .get("test_space", &created.base.id)
            .await
            .expect("get")
            .expect("work item");
        assert_eq!(fetched.status, WorkItemStatus::Running);
        assert!(fetched
            .leased_by
            .as_deref()
            .is_some_and(|token| token.starts_with("worker-1:")));
        assert!(fetched.lease_expires_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_renew_lease_preserves_worker_ownership() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_item("renew-lease");
        item.status = WorkItemStatus::Runnable;
        let created = repo.create(item).await.expect("create");
        let claimed = repo
            .claim_with_lease(
                "test_space",
                &created.base.id,
                "worker-1",
                Utc::now() + chrono::Duration::seconds(60),
            )
            .await
            .expect("claim")
            .expect("claimed");
        let token = claimed.leased_by.as_deref().expect("token");

        let extended_until = Utc::now() + chrono::Duration::hours(4);
        let renewed = repo
            .renew_lease("test_space", &created.base.id, token, extended_until)
            .await
            .expect("renew lease")
            .expect("owned running item");
        assert_eq!(renewed.leased_by.as_deref(), Some(token));
        assert_eq!(renewed.lease_expires_at, Some(extended_until));

        let rejected = repo
            .renew_lease("test_space", &created.base.id, "worker-2", extended_until)
            .await
            .expect("reject foreign lease renewal");
        assert!(rejected.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_runnable_excludes_future_run_after() {
        let repo = setup_test_repo().await;

        let mut delayed = create_test_work_item("delayed");
        delayed.status = WorkItemStatus::Runnable;
        delayed.priority = 100;
        delayed.run_after = Some(Utc::now() + chrono::Duration::minutes(5));
        repo.create(delayed).await.expect("create delayed");

        let mut ready = create_test_work_item("ready");
        ready.status = WorkItemStatus::Runnable;
        ready.priority = 1;
        let ready = repo.create(ready).await.expect("create ready");

        let runnable = repo
            .get_runnable("test_space", 10)
            .await
            .expect("get runnable");
        assert_eq!(runnable.len(), 1);
        assert_eq!(runnable[0].base.id, ready.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_find_by_status_limited_uses_status_filter() {
        let repo = setup_embedded_test_repo().await;

        for index in 0..50 {
            let mut done = create_test_work_item("historical");
            done.status = WorkItemStatus::Done;
            done.priority = index;
            repo.create(done).await.expect("create done item");
        }

        let mut running = create_test_work_item("active");
        running.status = WorkItemStatus::Running;
        running.priority = 500;
        let running = repo.create(running).await.expect("create running item");

        let found = repo
            .find_by_status_limited("test_space", WorkItemStatus::Running, 10)
            .await
            .expect("find running");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].base.id, running.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_find_by_status_and_kind_excludes_delayed_runnable_items() {
        let repo = setup_embedded_test_repo().await;

        let mut delayed = create_test_work_item("target");
        delayed.status = WorkItemStatus::Runnable;
        delayed.priority = 1000;
        delayed.run_after = Some(Utc::now() + chrono::Duration::minutes(5));
        repo.create(delayed).await.expect("create delayed");

        let mut wrong_kind = create_test_work_item("other");
        wrong_kind.status = WorkItemStatus::Runnable;
        wrong_kind.priority = 900;
        repo.create(wrong_kind).await.expect("create wrong kind");

        let mut ready = create_test_work_item("target");
        ready.status = WorkItemStatus::Runnable;
        ready.priority = 1;
        let ready = repo.create(ready).await.expect("create ready");

        let found = repo
            .find_by_status_and_kind("test_space", WorkItemStatus::Runnable, "target", 10)
            .await
            .expect("find runnable target");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].base.id, ready.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_latest_by_kind_includes_future_managed_work() {
        let repo = setup_embedded_test_repo().await;
        let mut future = create_test_work_item("managed");
        future.status = WorkItemStatus::Runnable;
        future.run_after = Some(Utc::now() + chrono::Duration::hours(6));
        let future = repo.create(future).await.expect("create future work");

        let latest = repo
            .find_latest_by_kind("test_space", "managed")
            .await
            .expect("find latest")
            .expect("managed work");
        assert_eq!(latest.base.id, future.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_cancel_queued_by_kind_except_preserves_replacement_and_running_work() {
        let repo = setup_embedded_test_repo().await;
        let mut old = create_test_work_item("managed");
        old.status = WorkItemStatus::Runnable;
        let old = repo.create(old).await.expect("create old work");
        let mut running = create_test_work_item("managed");
        running.status = WorkItemStatus::Running;
        let running = repo.create(running).await.expect("create running work");
        let mut replacement = create_test_work_item("managed");
        replacement.status = WorkItemStatus::Runnable;
        let replacement = repo.create(replacement).await.expect("create replacement");

        let cancelled = repo
            .cancel_queued_by_kind_except("test_space", "managed", &replacement.base.id)
            .await
            .expect("cancel old queued work");
        assert_eq!(cancelled, 1);
        assert_eq!(
            repo.get("test_space", &old.base.id)
                .await
                .expect("old lookup")
                .expect("old item")
                .status,
            WorkItemStatus::Cancelled
        );
        assert_eq!(
            repo.get("test_space", &running.base.id)
                .await
                .expect("running lookup")
                .expect("running item")
                .status,
            WorkItemStatus::Running
        );
        assert_eq!(
            repo.get("test_space", &replacement.base.id)
                .await
                .expect("replacement lookup")
                .expect("replacement item")
                .status,
            WorkItemStatus::Runnable
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_runnable_all_spaces_does_not_truncate_ready_items_behind_delayed_ones() {
        let repo = setup_test_repo().await;

        for priority in [100, 90, 80] {
            let mut delayed = create_test_work_item("delayed");
            delayed.status = WorkItemStatus::Runnable;
            delayed.priority = priority;
            delayed.run_after = Some(Utc::now() + chrono::Duration::minutes(5));
            repo.create(delayed).await.expect("create delayed");
        }

        let mut ready = create_test_work_item("ready");
        ready.status = WorkItemStatus::Runnable;
        ready.priority = 1;
        let ready = repo.create(ready).await.expect("create ready");

        let runnable = repo
            .find_runnable_all_spaces(2)
            .await
            .expect("find runnable");
        assert_eq!(runnable.len(), 1);
        assert_eq!(runnable[0].base.id, ready.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_claim_with_lease_ignores_stale_lease_fields_on_runnable_item() {
        let repo = setup_test_repo().await;

        let mut item = create_test_work_item("stale-lease");
        item.status = WorkItemStatus::Runnable;
        item.leased_by = Some("old-worker".to_string());
        item.lease_expires_at = Some(Utc::now() + chrono::Duration::minutes(10));
        let created = repo.create(item).await.expect("create stale runnable item");

        let claimed = repo
            .claim_with_lease(
                "test_space",
                &created.base.id,
                "worker-1",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim work item")
            .expect("runnable item should be claimable");

        assert_eq!(claimed.status, WorkItemStatus::Running);
        assert!(claimed
            .leased_by
            .as_deref()
            .is_some_and(|token| token.starts_with("worker-1:")));
        assert_eq!(claimed.attempts, 1);
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn generic_updates_cannot_replace_attempt_authority(client: StorageClient) {
        let repo = WorkItemRepository::new(client);
        let mut item = create_test_work_item("authority-boundary");
        item.status = WorkItemStatus::Runnable;
        let item = repo.create(item).await.expect("create");
        let mut forged = item.clone();
        forged.status = WorkItemStatus::Running;
        assert!(repo
            .update("test_space", &item.base.id, forged.clone())
            .await
            .is_err());
        assert!(repo
            .update_if_status_and_dependencies_done(
                "test_space",
                &item.base.id,
                forged,
                WorkItemStatus::Runnable
            )
            .await
            .is_err());
        let claimed = repo
            .claim_with_lease(
                "test_space",
                &item.base.id,
                "worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("runnable");
        let attempt =
            crate::work_attempt_repository::fence_from_claim(&claimed).expect("authority");
        let mut forged_done = claimed.clone();
        forged_done.status = WorkItemStatus::Done;
        assert!(repo
            .update("test_space", &item.base.id, forged_done.clone())
            .await
            .is_err());
        assert!(repo
            .update_if_status(
                "test_space",
                &item.base.id,
                forged_done.clone(),
                WorkItemStatus::Running
            )
            .await
            .is_err());
        assert!(repo
            .update_if_status_and_dependencies_done(
                "test_space",
                &item.base.id,
                forged_done,
                WorkItemStatus::Running
            )
            .await
            .is_err());
        let untouched = repo
            .get("test_space", &item.base.id)
            .await
            .expect("lookup")
            .expect("live");
        assert_eq!(untouched.status, WorkItemStatus::Running);
        assert_eq!(
            crate::work_attempt_repository::fence_from_claim(&untouched).expect("fence"),
            attempt
        );
        let completed = repo
            .update_running_for_worker(
                "test_space",
                &item.base.id,
                &attempt.lease_token,
                WorkItemStatus::Done,
                None,
            )
            .await
            .expect("finish")
            .expect("owned");
        assert_eq!(completed.status, WorkItemStatus::Done);
    }

    async fn requeue_cadence(
        repo: &WorkItemRepository,
        id: &RecordId,
        token: &str,
        dependency: bool,
        seconds: i64,
    ) -> StorageResult<Option<WorkItem>> {
        if dependency {
            repo.requeue_pending_dependencies(
                "test_space",
                id,
                token,
                seconds,
                "original prerequisite".into(),
            )
            .await
        } else {
            repo.requeue_configuration_blocked(
                "test_space",
                id,
                token,
                seconds,
                "original prerequisite".into(),
            )
            .await
        }
    }

    async fn charged_cadence_claim(repo: &WorkItemRepository, kind: &str) -> WorkItem {
        let mut item = create_test_work_item(kind);
        item.status = WorkItemStatus::Runnable;
        let original = repo.create(item).await.expect("create original work");
        // Retain two actual charged attempts. A local wait refunds only its
        // own unused attempt, not prior execution or claim generations.
        for _ in 0..2 {
            let claim = repo
                .claim_with_lease(
                    "test_space",
                    &original.base.id,
                    "cadence-worker",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("claim")
                .expect("runnable original");
            repo.requeue_with_delay(
                "test_space",
                &original.base.id,
                claim.leased_by.as_deref().expect("original token"),
                0,
                None,
            )
            .await
            .expect("charged retry")
            .expect("owned original");
        }
        repo.claim_with_lease(
            "test_space",
            &original.base.id,
            "cadence-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim local wait")
        .expect("owned original")
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn persisted_retry_cadence_contract(client: StorageClient, dependency: bool) {
        let repo = WorkItemRepository::new(client);
        let cases = if dependency {
            [(i64::MIN, 1), (5, 5), (i64::MAX, 3600)]
        } else {
            [(i64::MIN, 30), (5, 30), (i64::MAX, 3600)]
        };
        for (requested, expected) in cases {
            let claim =
                charged_cadence_claim(&repo, &format!("cadence-{dependency}-{requested}")).await;
            let fence = crate::work_attempt_repository::fence_from_claim(&claim)
                .expect("original durable fence");
            assert_eq!(claim.attempts, 3);
            assert_eq!(fence.generation, 3);
            assert!(requeue_cadence(
                &repo,
                &claim.base.id,
                "foreign-token",
                dependency,
                requested
            )
            .await
            .expect("foreign owner refused")
            .is_none());
            assert_eq!(
                repo.get("test_space", &claim.base.id).await.expect("read"),
                Some(claim.clone())
            );

            let before = Utc::now();
            let requeued = requeue_cadence(
                &repo,
                &claim.base.id,
                &fence.lease_token,
                dependency,
                requested,
            )
            .await
            .expect("persist original delay")
            .expect("original attempt transitioned");
            let after = Utc::now();
            let due = requeued.run_after.expect("persisted deadline");
            assert!(due >= before + chrono::Duration::seconds(expected));
            assert!(due <= after + chrono::Duration::seconds(expected));
            let prefix = if dependency {
                "pending_dependencies"
            } else {
                "configuration_blocked"
            };
            let mut expected_item = claim.clone();
            expected_item.status = WorkItemStatus::Runnable;
            expected_item.attempts = 2;
            expected_item.leased_by = None;
            expected_item.lease_expires_at = None;
            expected_item.run_after = Some(due);
            expected_item.last_error = Some(format!("{prefix}: original prerequisite"));
            expected_item.base.updated_at = requeued.base.updated_at;
            assert_eq!(
                requeued, expected_item,
                "no work input or durable claim envelope changed"
            );
            assert_eq!(
                repo.get("test_space", &claim.base.id)
                    .await
                    .expect("persisted read"),
                Some(requeued.clone())
            );
            assert!(requeue_cadence(
                &repo,
                &claim.base.id,
                &fence.lease_token,
                dependency,
                requested
            )
            .await
            .expect("retired owner refused")
            .is_none());
            assert_eq!(
                repo.get("test_space", &claim.base.id)
                    .await
                    .expect("unchanged read"),
                Some(requeued)
            );
            if expected == 3600 {
                assert!(repo
                    .claim_with_lease(
                        "test_space",
                        &claim.base.id,
                        "premature-worker",
                        Utc::now() + chrono::Duration::minutes(5),
                    )
                    .await
                    .expect("future work stays deferred")
                    .is_none());
            }
        }
    }

    #[tokio::test]
    async fn embedded_pending_dependencies_keep_short_cadence_and_attempt_fence() {
        persisted_retry_cadence_contract(
            StorageClient::new_embedded().await.expect("embedded"),
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn surreal_pending_dependencies_keep_short_cadence_and_attempt_fence() {
        persisted_retry_cadence_contract(StorageClient::new_memory().await.expect("memory"), true)
            .await;
    }

    #[tokio::test]
    async fn embedded_configuration_backoff_keeps_floor_and_attempt_fence() {
        persisted_retry_cadence_contract(
            StorageClient::new_embedded().await.expect("embedded"),
            false,
        )
        .await;
    }

    #[tokio::test]
    async fn surreal_configuration_backoff_keeps_floor_and_attempt_fence() {
        persisted_retry_cadence_contract(StorageClient::new_memory().await.expect("memory"), false)
            .await;
    }

    #[tokio::test]
    async fn embedded_generic_updates_cannot_replace_attempt_authority() {
        generic_updates_cannot_replace_attempt_authority(
            StorageClient::new_embedded().await.unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn surreal_generic_updates_cannot_replace_attempt_authority() {
        generic_updates_cannot_replace_attempt_authority(
            StorageClient::new_memory().await.unwrap(),
        )
        .await;
    }
}

#[path = "standing_watch_queue.rs"]
mod standing_watch_queue;
