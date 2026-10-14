//! System event repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use elephant_types::records::entity::Entity;
use elephant_types::records::SystemEvent;
use elephant_types::{Link, RecordId};
use serde::Deserialize;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;
use elephant_types::ids::TableName;

const SYSTEM_EVENT_PROJECTION: &str = r"
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
    IF subjects IS NOT NONE THEN array::map(subjects, |$s| IF type::is::record($s) THEN string::concat(meta::tb($s), ':', meta::id($s)) ELSE $s END) ELSE [] END AS subjects,
    subjects_state,
    seq,
    event_type,
    payload
";

/// Database-only proxy for `SystemEvent` to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct SystemEventDb {
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
    seq: Option<i64>,
    event_type: Option<String>,
    payload: serde_json::Value,
}

impl TryFrom<SystemEventDb> for SystemEvent {
    type Error = StorageError;

    fn try_from(db: SystemEventDb) -> Result<Self, Self::Error> {
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
            seq: required_field(db.seq, "seq")?,
            event_type: required_field(db.event_type, "event_type")?,
            payload: db.payload,
        })
    }
}

/// System event repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct SystemEventRepository {
    client: StorageClient,
}

impl SystemEventRepository {
    /// Creates a new system event repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    pub(crate) fn system_event_to_value(
        event: &SystemEvent,
    ) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;

        let mut obj = crate::surreal::base_fields_to_object(&event.base)?;
        obj.insert("seq".to_string(), event.seq.into());
        obj.insert("event_type".to_string(), event.event_type.clone().into());
        obj.insert(
            "payload".to_string(),
            crate::surreal::json_to_surreal(event.payload.clone())?,
        );
        Ok(Value::Object(obj))
    }

    /// Gets events after a sequence number for a space.
    ///
    /// Returns events in sequence order for streaming.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_after_seq(
        &self,
        space_id: &str,
        after_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| event.base.space_id == space_id && event.seq > after_seq)
                .collect();
            events.sort_by_key(|event| event.seq);
            if events.len() > limit {
                events.truncate(limit);
            }
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE space_id = $space_id AND seq > $after_seq ORDER BY seq ASC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "after_seq" => after_seq,
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Gets events before a sequence number for a space.
    ///
    /// Returns events in reverse sequence order for pagination/backfill.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_before_seq(
        &self,
        space_id: &str,
        before_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| event.base.space_id == space_id && event.seq < before_seq)
                .collect();
            events.sort_by_key(|event| std::cmp::Reverse(event.seq));
            if events.len() > limit {
                events.truncate(limit);
            }
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE space_id = $space_id AND seq < $before_seq ORDER BY seq DESC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "before_seq" => before_seq,
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Gets the latest sequence number for a space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_latest_seq(&self, space_id: &str) -> StorageResult<i64> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| event.base.space_id == space_id)
                .map(|event| event.seq)
                .max()
                .unwrap_or(0));
        }

        #[derive(serde::Deserialize, Default)]
        struct MaxSeq {
            max_seq: Option<i64>,
        }

        let mut response = self
            .client
            .query_bind(
                "SELECT math::max(seq) as max_seq FROM system_event WHERE space_id = $space_id GROUP ALL",
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let rows: Vec<MaxSeq> =
            crate::response::take_first(&mut response, "Failed to parse system event max seq")?;
        let max = rows.first().and_then(|r| r.max_seq).unwrap_or(0);
        Ok(max)
    }

    /// Gets all unique space IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_all_spaces(&self) -> StorageResult<Vec<String>> {
        if let Some(store) = self.client.embedded_store() {
            let mut spaces: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .map(|event| event.base.space_id)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            spaces.sort();
            return Ok(spaces);
        }

        #[derive(Deserialize)]
        struct SpaceRow {
            space_id: Option<String>,
        }

        let mut response = self
            .client
            .query("SELECT space_id FROM system_event GROUP BY space_id")
            .await?;

        let rows: Vec<SpaceRow> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to get spaces: {e}")))?;

        Ok(rows.into_iter().filter_map(|row| row.space_id).collect())
    }

    /// Gets a single event by ID (without space filtering).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_by_id(&self, id: &RecordId) -> StorageResult<Option<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .find(|event| event.base.id == *id));
        }

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id) },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system event: {e}")))?;

        events
            .into_iter()
            .next()
            .map(SystemEvent::try_from)
            .transpose()
    }

    /// Lists recent events for a space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn list_recent(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| event.base.space_id == space_id)
                .collect();
            events.sort_by_key(|event| std::cmp::Reverse(event.seq));
            if events.len() > limit {
                events.truncate(limit);
            }
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE space_id = $space_id ORDER BY seq DESC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Gets recent events for a space (alias for list_recent).
    pub async fn get_recent(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        self.list_recent(space_id, limit).await
    }

    /// Finds events of a given type created at or after `since`, across ALL
    /// spaces, newest first.
    ///
    /// Used by global health surfaces (`get_health`
    /// runs with a global principal, so it must see every space's signals).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn find_recent_by_type_since(
        &self,
        event_type: &str,
        since: chrono::DateTime<Utc>,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| {
                    event.base.deleted_at.is_none()
                        && event.event_type == event_type
                        && event.base.created_at >= since
                })
                .collect();
            events.sort_by_key(|event| std::cmp::Reverse(event.base.created_at));
            if events.len() > limit {
                events.truncate(limit);
            }
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event \
                     WHERE event_type = $event_type AND created_at >= $since AND deleted_at IS NONE \
                     ORDER BY created_at DESC LIMIT {limit}"
                ),
                crate::bindings! {
                    "event_type" => event_type,
                    "since" => crate::surreal::datetime_from_chrono(since)
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Gets upcoming events for a space.
    ///
    /// System events don't have a future schedule, so this returns recent events.
    pub async fn get_upcoming(
        &self,
        space_id: &str,
        _days: u32,
    ) -> StorageResult<Vec<SystemEvent>> {
        self.list_recent(space_id, 50).await
    }

    /// Finds events referencing an entity via subjects.
    pub async fn find_by_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| {
                    event.base.space_id == space_id
                        && event.base.deleted_at.is_none()
                        && event
                            .base
                            .subjects
                            .iter()
                            .any(|subject| subject.id() == entity_id)
                })
                .collect();
            events.sort_by_key(|event| std::cmp::Reverse(event.seq));
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event \
                     WHERE space_id = $space_id AND deleted_at IS NONE \
                     AND subjects CONTAINS $entity \
                     ORDER BY seq DESC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity" => crate::surreal::thing_from_record_id(entity_id),
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Finds events referencing an entity since a timestamp.
    pub async fn find_by_entity_since(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        since: chrono::DateTime<Utc>,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            let mut events: Vec<_> = store
                .scan::<SystemEvent>()
                .await?
                .into_iter()
                .filter(|event| {
                    event.base.space_id == space_id
                        && event.base.deleted_at.is_none()
                        && event.base.created_at >= since
                        && event
                            .base
                            .subjects
                            .iter()
                            .any(|subject| subject.id() == entity_id)
                })
                .collect();
            events.sort_by_key(|event| std::cmp::Reverse(event.base.created_at));
            if events.len() > limit {
                events.truncate(limit);
            }
            return Ok(events);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event \
                     WHERE space_id = $space_id AND deleted_at IS NONE \
                     AND created_at >= $since AND subjects CONTAINS $entity \
                     ORDER BY created_at DESC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity" => crate::surreal::thing_from_record_id(entity_id),
                    "since" => since,
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        events.into_iter().map(SystemEvent::try_from).collect()
    }

    /// Gets a record including soft-deleted items.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SystemEvent>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system event: {e}")))?;

        events
            .into_iter()
            .next()
            .map(SystemEvent::try_from)
            .transpose()
    }

    /// Restores a soft-deleted record.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<SystemEvent> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<SystemEvent>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE WHERE space_id = $space_id RETURN {SYSTEM_EVENT_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let events: Vec<SystemEventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored system event: {e}"))
        })?;
        let event_db = events.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("SystemEvent not found in space: {id}"))
        })?;
        SystemEvent::try_from(event_db)
    }
}

#[async_trait]
impl Repository<SystemEvent> for SystemEventRepository {
    async fn create(&self, record: SystemEvent) -> StorageResult<SystemEvent> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation.scan::<SystemEvent>()?.into_iter().any(|event| {
                        event.base.space_id == record.base.space_id && event.seq == record.seq
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate system_event sequence for space {}: {}",
                            record.base.space_id, record.seq
                        )));
                    }
                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        let response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => Self::system_event_to_value(&record)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to create system_event: {e}"))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create system_event: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let events: Vec<SystemEventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created system_event: {e}"))
        })?;
        let event_db = events.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but system_event not found".to_string())
        })?;
        SystemEvent::try_from(event_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SystemEvent>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let items: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system event: {e}")))?;

        items
            .into_iter()
            .next()
            .map(SystemEvent::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: SystemEvent,
    ) -> StorageResult<SystemEvent> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(existing) =
                        mutation.get::<SystemEvent>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate = mutation.scan::<SystemEvent>()?.into_iter().any(|event| {
                        event.base.space_id == target_space_id
                            && event.seq == normalized.seq
                            && event.base.id != target_id
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate system_event sequence {} in space {target_space_id}",
                            normalized.seq
                        )));
                    }

                    if normalized.seq < existing.seq {
                        crate::event_sequence::preserve_tx(mutation, &target_space_id)?;
                    }

                    Ok(mutation.replace(normalized.clone())?)
                })
                .await;
        }

        let mut bindings = crate::bindings! {
            "id" => crate::surreal::thing_from_record_id(id),
            "space_id" => space_id,
            "record" => Self::system_event_to_value(&normalized)?,
            "updated_sequence" => normalized.seq,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\nLET $original_event = SELECT * FROM ONLY $id;\nIF $original_event != NONE AND $original_event.space_id = $space_id AND $original_event.deleted_at = NONE {\nIF $updated_sequence < $original_event.seq {\n");
        crate::event_sequence::append_preserve_sql(
            &mut sql,
            &mut bindings,
            space_id,
            "event_update_floor",
        )?;
        sql.push_str("};\nUPDATE $id CONTENT $record WHERE space_id = $space_id AND deleted_at IS NONE;\n};\nCOMMIT TRANSACTION;\n");
        let response = self.client.query_bind(&sql, bindings).await.map_err(|e| {
            StorageError::classify_query_error(format!("Failed to update system_event: {e}"))
        })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to update system_event: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;
        let events: Vec<SystemEventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated system_event: {e}"))
        })?;
        let event_db = events.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("SystemEvent not found in space: {id}"))
        })?;
        SystemEvent::try_from(event_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<SystemEvent>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now() WHERE space_id = $space_id",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;
        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<SystemEvent>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                },
            )
            .await?;

        let items: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;
        items.into_iter().map(SystemEvent::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<SystemEvent> for SystemEventRepository {
    #[allow(clippy::too_many_lines)]
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search(criteria, |event: &SystemEvent, criteria| {
                    if let Some(query) = &criteria.query {
                        if !event
                            .event_type
                            .to_ascii_lowercase()
                            .contains(&query.to_ascii_lowercase())
                        {
                            return false;
                        }
                    }

                    if let Some(entities) = &criteria.entities {
                        if !entities.is_empty() {
                            let mode = criteria.mode.as_deref().unwrap_or("any");
                            let matches = entities
                                .iter()
                                .filter_map(|entity| {
                                    crate::parse_id(entity, TableName::Entity).ok()
                                })
                                .filter(|entity_id| {
                                    event
                                        .base
                                        .subjects
                                        .iter()
                                        .any(|subject| subject.id() == entity_id)
                                })
                                .count();

                            if (mode == "all" && matches != entities.len())
                                || (mode != "all" && matches == 0)
                            {
                                return false;
                            }
                        }
                    }

                    true
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
                "string::lowercase(event_type) CONTAINS string::lowercase($query)".to_string(),
            );
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(ref entities) = criteria.entities {
            if !entities.is_empty() {
                let mode = criteria.mode.as_deref().unwrap_or("any").to_string();
                let mut entity_conditions = Vec::new();
                for (idx, entity) in entities.iter().enumerate() {
                    let rid = crate::parse_id(entity, TableName::Entity)
                        .map_err(|e| StorageError::QueryError(e.to_string()))?;
                    let key = format!("entity_{idx}");
                    bindings.insert(
                        key.clone(),
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&rid)),
                    );
                    entity_conditions.push(format!("subjects CONTAINS ${key}"));
                }

                let joiner = if mode == "all" { " AND " } else { " OR " };
                conditions.push(format!("({})", entity_conditions.join(joiner)));
            }
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::SystemEvent)
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
            "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let items_db: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system events: {e}")))?;

        let mut items: Vec<SystemEvent> = items_db
            .into_iter()
            .map(SystemEvent::try_from)
            .collect::<Result<_, _>>()?;

        let has_more = criteria.limit > 0 && items.len() > criteria.limit;
        if has_more {
            items.truncate(criteria.limit);
        } else if criteria.limit == 0 {
            items.clear();
        }

        let next_cursor = if has_more {
            items.last().map(|e| {
                let id_str = e.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM system_event {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;
        let counts: Vec<CountResult> = count_response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse system event count: {e}"))
        })?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(items, next_cursor, has_more, total))
    }
}

impl SystemEventRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<SystemEvent>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SystemEvent>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SYSTEM_EVENT_PROJECTION} FROM system_event WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let items: Vec<SystemEventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse system event: {e}")))?;

        items
            .into_iter()
            .next()
            .map(SystemEvent::try_from)
            .transpose()
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

    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(90000);

    const SCHEMA: &str = r"
        DEFINE TABLE system_event SCHEMALESS;
        DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id, seq UNIQUE;
    ";

    async fn setup_test_repo() -> (SystemEventRepository, StorageClient) {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("system_event_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        client.query(SCHEMA).await.unwrap();
        (SystemEventRepository::new(client.clone()), client)
    }

    async fn setup_embedded_test_repo() -> SystemEventRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        SystemEventRepository::new(client)
    }

    fn make_test_event(space_id: &str, seq: i64, event_type: &str) -> SystemEvent {
        let id = RecordId::new_v4(TableName::SystemEvent);
        let mut base = BaseFields::new(id, space_id.to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        SystemEvent {
            base,
            seq,
            event_type: event_type.to_string(),
            payload: serde_json::json!({"test": true}),
        }
    }

    async fn create_test_event(
        client: &StorageClient,
        space_id: &str,
        seq: i64,
        event_type: &str,
    ) -> SystemEvent {
        let event = make_test_event(space_id, seq, event_type);
        let id_str = event.base.id.to_string();
        let _uuid = id_str.split(':').nth(1).unwrap_or("");

        // Insert directly
        client
            .query_bind(
                r"CREATE $id CONTENT {
                        space_id: $space_id,
                        created_at: $created_at,
                        updated_at: $updated_at,
                        created_by: $created_by,
                        security: $security,
                        provenance: $provenance,
                        subjects: $subjects,
                        subjects_state: $subjects_state,
                        seq: $seq,
                        event_type: $event_type,
                        payload: $payload
                    }",
                crate::bindings! {
                    "id" => surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&event.base.id)),
                    "space_id" => space_id,
                    "created_at" => surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(event.base.created_at)),
                    "updated_at" => surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(event.base.updated_at)),
                    "created_by" => event.base.created_by.clone(),
                    "security" => crate::surreal::json_to_surreal(
                        serde_json::to_value(&event.base.security)
                            .expect("serialize security"),
                    )
                    .expect("convert security"),
                    "provenance" => crate::surreal::json_to_surreal(
                        serde_json::to_value(&event.base.provenance)
                            .expect("serialize provenance"),
                    )
                    .expect("convert provenance"),
                    "subjects" => crate::surreal::things_from_links(event.base.subjects.clone()),
                    "subjects_state" => crate::surreal::enum_to_surreal(&event.base.subjects_state)
                        .expect("subjects_state enum"),
                    "seq" => seq,
                    "event_type" => event_type,
                    "payload" => crate::surreal::json_to_surreal(event.payload.clone())
                        .expect("payload json"),
                },
            )
            .await
            .unwrap();

        event
    }

    #[tokio::test]
    async fn test_missing_seq_errors() {
        let (repo, client) = setup_test_repo().await;

        let event = create_test_event(&client, "space_a", 1, "event_a").await;
        client
            .query_bind(
                "UPDATE $id SET seq = NONE",
                crate::bindings! {
                    "id" => surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&event.base.id)),
                },
            )
            .await
            .unwrap();

        let result = repo.get("space_a", &event.base.id).await;
        assert!(matches!(
            result,
            Err(StorageError::QueryError(msg)) if msg.contains("Missing seq")
        ));
    }

    #[tokio::test]
    async fn test_missing_event_type_errors() {
        let (repo, client) = setup_test_repo().await;

        let event = create_test_event(&client, "space_a", 2, "event_b").await;
        client
            .query_bind(
                "UPDATE $id SET event_type = NONE",
                crate::bindings! {
                    "id" => surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&event.base.id)),
                },
            )
            .await
            .unwrap();

        let result = repo.get("space_a", &event.base.id).await;
        assert!(matches!(
            result,
            Err(StorageError::QueryError(msg)) if msg.contains("Missing event_type")
        ));
    }

    #[tokio::test]
    async fn test_get_after_seq() {
        let (repo, client) = setup_test_repo().await;

        // Create events with sequential seq numbers
        for seq in 1..=5 {
            create_test_event(&client, "space_a", seq, &format!("event_{seq}")).await;
        }

        // Get events after seq 2
        let events = repo.get_after_seq("space_a", 2, 10).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].seq, 3);
        assert_eq!(events[1].seq, 4);
        assert_eq!(events[2].seq, 5);
    }

    #[tokio::test]
    async fn test_get_after_seq_empty() {
        let (repo, client) = setup_test_repo().await;

        create_test_event(&client, "space_a", 1, "event_1").await;

        let events = repo.get_after_seq("space_a", 10, 10).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_get_after_seq_respects_space() {
        let (repo, client) = setup_test_repo().await;

        create_test_event(&client, "space_a", 1, "event_a").await;
        create_test_event(&client, "space_b", 1, "event_b").await;
        create_test_event(&client, "space_a", 2, "event_a2").await;

        let events_a = repo.get_after_seq("space_a", 0, 10).await.unwrap();
        let events_b = repo.get_after_seq("space_b", 0, 10).await.unwrap();

        assert_eq!(events_a.len(), 2);
        assert_eq!(events_b.len(), 1);
    }

    #[tokio::test]
    async fn test_get_latest_seq() {
        let (repo, client) = setup_test_repo().await;

        create_test_event(&client, "space_a", 1, "event_1").await;
        create_test_event(&client, "space_a", 5, "event_5").await;
        create_test_event(&client, "space_a", 3, "event_3").await;

        let latest = repo.get_latest_seq("space_a").await.unwrap();
        assert_eq!(latest, 5);
    }

    #[tokio::test]
    async fn test_get_latest_seq_empty() {
        let (repo, _) = setup_test_repo().await;

        let latest = repo.get_latest_seq("nonexistent").await.unwrap();
        assert_eq!(latest, 0);
    }

    #[tokio::test]
    async fn test_list_recent() {
        let (repo, client) = setup_test_repo().await;

        for seq in 1..=5 {
            create_test_event(&client, "space_a", seq, &format!("event_{seq}")).await;
        }

        let events = repo.list_recent("space_a", 3).await.unwrap();
        assert_eq!(events.len(), 3);
        // Should be in descending seq order
        assert_eq!(events[0].seq, 5);
        assert_eq!(events[1].seq, 4);
        assert_eq!(events[2].seq, 3);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_get_after_seq_includes_soft_deleted_events() {
        let repo = setup_embedded_test_repo().await;
        let event = make_test_event("space_a", 3, "event_3");
        let id = event.base.id.clone();
        repo.create(event).await.unwrap();
        repo.delete("space_a", &id).await.unwrap();

        let events = repo.get_after_seq("space_a", 2, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 3);
        assert!(events[0].base.deleted_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_list_recent_includes_soft_deleted_events() {
        let repo = setup_embedded_test_repo().await;
        let event = make_test_event("space_a", 7, "event_7");
        let id = event.base.id.clone();
        repo.create(event).await.unwrap();
        repo.delete("space_a", &id).await.unwrap();

        let events = repo.list_recent("space_a", 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 7);
        assert!(events[0].base.deleted_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_duplicate_seq() {
        let repo = setup_embedded_test_repo().await;
        let first = make_test_event("space_a", 9, "event_9");
        let second = make_test_event("space_a", 9, "event_9_dup");

        repo.create(first).await.unwrap();
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate seq should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate system_event sequence"))
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_update_rejects_duplicate_seq() {
        let repo = setup_embedded_test_repo().await;
        let first = repo
            .create(make_test_event("space_a", 10, "event_10"))
            .await
            .unwrap();
        let second = repo
            .create(make_test_event("space_a", 11, "event_11"))
            .await
            .unwrap();

        let mut updated = second.clone();
        updated.seq = first.seq;

        let err = repo
            .update("space_a", &second.base.id, updated)
            .await
            .expect_err("duplicate seq should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate system_event sequence"))
        );
    }

    async fn assert_find_recent_by_type_since(repo: &SystemEventRepository) {
        repo.create(make_test_event("space_a", 1, "budget_exhausted"))
            .await
            .unwrap();
        repo.create(make_test_event("space_b", 2, "budget_exhausted"))
            .await
            .unwrap();
        repo.create(make_test_event("space_a", 3, "operator_alert"))
            .await
            .unwrap();

        // Matches the type across ALL spaces (global health surface).
        let since = Utc::now() - chrono::Duration::hours(1);
        let events = repo
            .find_recent_by_type_since("budget_exhausted", since, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "both spaces' events: {events:?}");
        assert!(events
            .iter()
            .all(|event| event.event_type == "budget_exhausted"));
        let spaces: Vec<&str> = events
            .iter()
            .map(|event| event.base.space_id.as_str())
            .collect();
        assert!(spaces.contains(&"space_a") && spaces.contains(&"space_b"));

        // A cutoff after the events excludes them.
        let future = Utc::now() + chrono::Duration::hours(1);
        let events = repo
            .find_recent_by_type_since("budget_exhausted", future, 10)
            .await
            .unwrap();
        assert!(events.is_empty(), "future cutoff excludes today's events");
    }

    #[tokio::test]
    async fn test_find_recent_by_type_since() {
        let (repo, _client) = setup_test_repo().await;
        assert_find_recent_by_type_since(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_find_recent_by_type_since() {
        let repo = setup_embedded_test_repo().await;
        assert_find_recent_by_type_since(&repo).await;
    }
}
