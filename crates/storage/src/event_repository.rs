//! Event repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use elephant_types::records::assertion::Assertion;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::source::Source;
use elephant_types::records::{Event, EventStatus, TimePrecision};
use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;
use crate::EntityRepository;

pub(crate) const EVENT_PROJECTION: &str = r"
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
        work_run_id: IF provenance.work_run_id IS NOT NONE THEN (IF type::is::record(provenance.work_run_id) THEN string::concat(meta::tb(provenance.work_run_id), ':', meta::id(provenance.work_run_id)) ELSE provenance.work_run_id END) ELSE NONE END,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    IF subjects IS NOT NONE THEN array::map(subjects, |$s| IF type::is::record($s) THEN string::concat(meta::tb($s), ':', meta::id($s)) ELSE $s END) ELSE [] END AS subjects,
    subjects_state,
    title,
    summary,
    status,
    time_start,
    time_end,
    time_precision,
    confidence,
    time_confidence,
    IF assertion_refs IS NOT NONE THEN array::map(assertion_refs, |$r| IF type::is::record($r) THEN string::concat(meta::tb($r), ':', meta::id($r)) ELSE $r END) ELSE [] END AS assertion_refs,
    IF source_refs IS NOT NONE THEN array::map(source_refs, |$r| IF type::is::record($r) THEN string::concat(meta::tb($r), ':', meta::id($r)) ELSE $r END) ELSE [] END AS source_refs,
    metadata,
    metadata IS NOT NONE AS metadata_present
";

/// Default (and maximum) page size for upcoming-event listings.
pub const DEFAULT_UPCOMING_LIMIT: usize = 50;

/// Upper bound on rows scanned for an upcoming-event listing before caller-side filtering
/// (authorization, candidate status) and paging.
pub const UPCOMING_SCAN_LIMIT: usize = 1000;

/// How far back (in hours) a day-precision event stays "upcoming" after 00:00Z of its date.
const UPCOMING_DAY_PRECISION_GRACE_HOURS: i64 = 24;

/// Returns whether `event` belongs in an upcoming listing (see `get_upcoming_at`).
fn is_upcoming(
    event: &Event,
    now: DateTime<Utc>,
    horizon: DateTime<Utc>,
    day_floor: DateTime<Utc>,
) -> bool {
    let Some(time_start) = event.time_start else {
        return false;
    };
    if time_start > horizon {
        return false;
    }
    time_start >= now
        || event.time_end.is_some_and(|time_end| time_end >= now)
        || (event.time_precision == TimePrecision::Day && time_start >= day_floor)
}

/// Window query for explicit time-range and status filtering.
#[derive(Debug, Clone)]
pub struct EventWindowQuery {
    /// Inclusive lower bound on `time_start`.
    pub start: Option<DateTime<Utc>>,
    /// Inclusive upper bound on `time_start`.
    pub end: Option<DateTime<Utc>>,
    /// Optional statuses to include.
    pub statuses: Option<Vec<EventStatus>>,
    /// Maximum records to return.
    pub limit: usize,
}

impl Default for EventWindowQuery {
    fn default() -> Self {
        Self {
            start: None,
            end: None,
            statuses: None,
            limit: 50,
        }
    }
}

/// Database-only proxy for Event to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct EventDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    title: Option<String>,
    summary: Option<String>,
    status: Option<EventStatus>,
    time_start: Option<DateTime<Utc>>,
    time_end: Option<DateTime<Utc>>,
    time_precision: Option<TimePrecision>,
    confidence: Option<f32>,
    time_confidence: Option<f32>,
    assertion_refs: Option<Vec<Link<Assertion>>>,
    source_refs: Option<Vec<Link<Source>>>,
    metadata: Option<serde_json::Value>,
    // Option alone conflates explicit JSON null with an absent native field.
    // This computed projection is never read from a same-named stored value.
    metadata_present: Option<bool>,
}

impl TryFrom<EventDb> for Event {
    type Error = StorageError;

    fn try_from(db: EventDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: required_field(db.created_at, "created_at")?,
                updated_at: required_field(db.updated_at, "updated_at")?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            title: required_field(db.title, "title")?,
            summary: db.summary,
            status: required_field(db.status, "status")?,
            time_start: db.time_start,
            time_end: db.time_end,
            time_precision: required_field(db.time_precision, "time_precision")?,
            confidence: required_field(db.confidence, "confidence")?,
            time_confidence: required_field(db.time_confidence, "time_confidence")?,
            assertion_refs: required_field(db.assertion_refs, "assertion_refs")?,
            source_refs: required_field(db.source_refs, "source_refs")?,
            metadata: if required_field(db.metadata_present, "metadata presence")? {
                db.metadata.unwrap_or(serde_json::Value::Null)
            } else {
                return Err(StorageError::QueryError("Missing metadata".into()));
            },
        })
    }
}

/// Event repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct EventRepository {
    client: StorageClient,
}

impl EventRepository {
    /// Pins a replacement event to its space and ID and normalizes its subject
    /// links, as every update does before writing.
    async fn prepare_replacement(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Event,
    ) -> StorageResult<Event> {
        record.base.space_id = space_id.to_string();
        record.base.id.clone_from(id);
        record.base.updated_at = Utc::now();
        let original_subject_count = record.base.subjects.len();
        let (subjects, removed_subjects) = self
            .normalize_subject_links_in_space(space_id, &record.base.subjects)
            .await?;
        record.base.subjects = subjects;
        if removed_subjects > 0 || record.base.subjects.len() < original_subject_count {
            record.base.subjects_state = if record.base.subjects.is_empty() {
                SubjectsState::Unknown
            } else {
                SubjectsState::Partial
            };
        }
        record.validate()?;
        self.validate_subject_links_in_space(space_id, &record.base.subjects)
            .await?;
        Ok(record)
    }

    /// Replaces an active event without changing its security metadata.
    ///
    /// For ordinary event edits, after authorizing the existing and proposed
    /// records. The stored security must equal the replacement's security in
    /// the same mutation that writes it, so a stale authorization cannot
    /// overwrite a newer classification, and no edit can declassify. Trusted
    /// maintenance that manages security uses [`Repository::update`].
    ///
    /// # Errors
    ///
    /// Returns a conflict if the row is missing, deleted, or its security
    /// differs from the replacement's, or an error if storage access fails.
    pub async fn update_preserving_security(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Event,
    ) -> StorageResult<Event> {
        let record = self.prepare_replacement(space_id, id, record).await?;
        let conflict = || {
            StorageError::Conflict(format!(
                "Event is missing, deleted, or its security has changed: {id}"
            ))
        };
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(existing) = mutation.get::<Event>(space_id, id, false)? else {
                        return Err(conflict());
                    };
                    if existing.base.security != record.base.security {
                        return Err(conflict());
                    }
                    Ok(mutation.replace(record)?)
                })
                .await;
        }

        // The comparison sits in the UPDATE predicate rather than in a separate
        // read whose result could be stale by the time of the write.
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE \
                     AND security = $security RETURN {EVENT_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::event_to_value(&record)?,
                    "security" => crate::surreal::security_to_value(&record.base.security),
                },
            )
            .await?;
        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse updated event: {e}")))?;
        Event::try_from(rows.into_iter().next().ok_or_else(conflict)?)
    }

    /// Restores a event only if it is unchanged since `authorized`, the row
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
        authorized: &Event,
    ) -> StorageResult<Event> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(mut current) = mutation.get::<Event>(space_id, id, true)? else {
                        return Err(crate::lifecycle_guard::guard_conflict("Event", id));
                    };
                    if !crate::lifecycle_guard::base_envelope_unchanged(
                        &current.base,
                        &authorized.base,
                    ) {
                        return Err(crate::lifecycle_guard::guard_conflict("Event", id));
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
                     WHERE space_id = $space_id AND {condition} RETURN {EVENT_PROJECTION}"
                ),
                bindings,
            )
            .await?;
        let rows: Vec<EventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse guarded event restore: {e}"))
        })?;
        Event::try_from(
            rows.into_iter()
                .next()
                .ok_or_else(|| crate::lifecycle_guard::guard_conflict("Event", id))?,
        )
    }

    /// Finds active events that name an entity as a participant.
    pub async fn find_by_subject_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows: Vec<_> = store
                .scan::<Event>()
                .await?
                .into_iter()
                .filter(|row| {
                    row.base.space_id == space_id
                        && row.base.deleted_at.is_none()
                        && row
                            .base
                            .subjects
                            .iter()
                            .any(|subject| subject.id() == entity_id)
                })
                .collect();
            rows.sort_by(|left, right| {
                right
                    .time_start
                    .cmp(&left.time_start)
                    .then_with(|| right.base.created_at.cmp(&left.base.created_at))
                    .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
            });
            rows.truncate(limit);
            return Ok(rows);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event WHERE space_id = $space_id AND deleted_at IS NONE AND subjects CONTAINS $entity_id ORDER BY time_start DESC, created_at DESC, id ASC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity_id" => crate::surreal::thing_from_record_id(entity_id),
                },
            )
            .await?;
        let rows: Vec<EventDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse entity events: {error}"))
        })?;
        rows.into_iter().map(Event::try_from).collect()
    }

    /// Creates a new event repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    async fn validate_subject_links_in_space(
        &self,
        space_id: &str,
        subjects: &[Link<Entity>],
    ) -> StorageResult<()> {
        let entity_repo = EntityRepository::new(self.client.clone());
        for subject in subjects {
            if entity_repo.get(space_id, subject.id()).await?.is_none() {
                return Err(StorageError::QueryError(format!(
                    "Event subject {} is not a valid entity in space {}",
                    subject.id(),
                    space_id
                )));
            }
        }
        Ok(())
    }

    async fn normalize_subject_links_in_space(
        &self,
        space_id: &str,
        subjects: &[Link<Entity>],
    ) -> StorageResult<(Vec<Link<Entity>>, usize)> {
        let entity_repo = EntityRepository::new(self.client.clone());
        let mut normalized = Vec::with_capacity(subjects.len());
        let mut removed = 0usize;

        for subject in subjects {
            let Some(entity) = entity_repo.get(space_id, subject.id()).await? else {
                removed += 1;
                continue;
            };

            if entity.is_merged() {
                if let Some(redirect_to) = entity.redirect_to.as_ref() {
                    if entity_repo.get(space_id, redirect_to.id()).await?.is_some() {
                        let replacement = Link::new(redirect_to.id().clone());
                        if !normalized
                            .iter()
                            .any(|link: &Link<Entity>| link == &replacement)
                        {
                            normalized.push(replacement);
                        }
                        continue;
                    }
                }
            }

            if !normalized.iter().any(|link| link.id() == subject.id()) {
                normalized.push(subject.clone());
            }
        }

        Ok((normalized, removed))
    }

    pub(crate) fn event_to_value(item: &Event) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;

        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("title".to_string(), item.title.clone().into());
        obj.insert(
            "summary".to_string(),
            item.summary
                .as_ref()
                .map_or(Value::None, |summary| Value::from(summary.clone())),
        );
        obj.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );
        obj.insert(
            "time_start".to_string(),
            item.time_start.map_or(Value::None, |time| {
                Value::from(crate::surreal::datetime_from_chrono(time))
            }),
        );
        obj.insert(
            "time_end".to_string(),
            item.time_end.map_or(Value::None, |time| {
                Value::from(crate::surreal::datetime_from_chrono(time))
            }),
        );
        obj.insert(
            "time_precision".to_string(),
            crate::surreal::enum_to_surreal(&item.time_precision)?,
        );
        obj.insert("confidence".to_string(), item.confidence.into());
        obj.insert("time_confidence".to_string(), item.time_confidence.into());
        obj.insert(
            "assertion_refs".to_string(),
            surrealdb::sql::Value::Array(crate::surreal::things_from_links(
                item.assertion_refs.clone(),
            )),
        );
        obj.insert(
            "source_refs".to_string(),
            surrealdb::sql::Value::Array(crate::surreal::things_from_links(
                item.source_refs.clone(),
            )),
        );
        obj.insert(
            "metadata".to_string(),
            crate::surreal::json_to_surreal(item.metadata.clone())?,
        );
        Ok(Value::Object(obj))
    }

    /// Lists recent events ordered by event time descending.
    pub async fn list_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<Event>().await?;
            rows.retain(|row| row.base.space_id == space_id && row.base.deleted_at.is_none());
            rows.sort_by(|lhs, rhs| {
                rhs.time_start
                    .cmp(&lhs.time_start)
                    .then_with(|| rhs.base.created_at.cmp(&lhs.base.created_at))
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            if rows.len() > limit {
                rows.truncate(limit);
            }
            return Ok(rows);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY time_start DESC, created_at DESC LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse events: {e}")))?;
        rows.into_iter().map(Event::try_from).collect()
    }

    /// Gets recent events for a space (alias for `list_recent`).
    pub async fn get_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>> {
        self.list_recent(space_id, limit).await
    }

    /// Finds an event by lineage key stored in `metadata.lineage_key`.
    pub async fn find_by_lineage_key(
        &self,
        space_id: &str,
        lineage_key: &str,
    ) -> StorageResult<Option<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<Event>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && row
                        .metadata
                        .get("lineage_key")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| value == lineage_key)
            });
            rows.sort_by(|lhs, rhs| {
                rhs.base
                    .updated_at
                    .cmp(&lhs.base.updated_at)
                    .then_with(|| rhs.base.created_at.cmp(&lhs.base.created_at))
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            return Ok(rows.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event \
                     WHERE space_id = $space_id \
                       AND deleted_at IS NONE \
                       AND metadata.lineage_key = $lineage_key \
                     ORDER BY updated_at DESC, created_at DESC, id ASC \
                     LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "lineage_key" => lineage_key,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse event lineage search: {e}"))
        })?;
        rows.into_iter().next().map(Event::try_from).transpose()
    }

    /// Finds all events by lineage key stored in `metadata.lineage_key`.
    pub async fn find_all_by_lineage_key(
        &self,
        space_id: &str,
        lineage_key: &str,
    ) -> StorageResult<Vec<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<Event>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && row
                        .metadata
                        .get("lineage_key")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| value == lineage_key)
            });
            rows.sort_by(|lhs, rhs| {
                rhs.base
                    .updated_at
                    .cmp(&lhs.base.updated_at)
                    .then_with(|| rhs.base.created_at.cmp(&lhs.base.created_at))
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            return Ok(rows);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event \
                     WHERE space_id = $space_id \
                       AND deleted_at IS NONE \
                       AND metadata.lineage_key = $lineage_key \
                     ORDER BY updated_at DESC, created_at DESC, id ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "lineage_key" => lineage_key,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse event lineage search: {e}"))
        })?;
        rows.into_iter().map(Event::try_from).collect()
    }

    /// Gets upcoming events in the next N days, capped at the default page size.
    ///
    /// See [`Self::get_upcoming_at`] for the inclusion rules.
    pub async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<Event>> {
        self.get_upcoming_at(space_id, days, Utc::now(), DEFAULT_UPCOMING_LIMIT)
            .await
    }

    /// Gets upcoming events relative to `now`, ordered by `time_start`, at most `max_rows`.
    ///
    /// An event is included when its `time_start` is at or before `now + days` and:
    /// - it starts at or after `now`; or
    /// - it is still in progress (`time_end` is set and at or after `now`); or
    /// - it has day precision and starts no earlier than `now - 24h`. Day-precision events
    ///   are stored at 00:00Z of their date and Elephant has no household timezone, so this
    ///   conservative bound keeps "today" visible in every timezone; callers filter the exact
    ///   local day.
    pub async fn get_upcoming_at(
        &self,
        space_id: &str,
        days: u32,
        now: DateTime<Utc>,
        max_rows: usize,
    ) -> StorageResult<Vec<Event>> {
        let horizon = now + Duration::days(i64::from(days));
        let day_floor = now - Duration::hours(UPCOMING_DAY_PRECISION_GRACE_HOURS);

        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<Event>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && is_upcoming(row, now, horizon, day_floor)
            });
            rows.sort_by(|lhs, rhs| {
                lhs.time_start
                    .cmp(&rhs.time_start)
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            rows.truncate(max_rows);
            return Ok(rows);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event \
                     WHERE space_id = $space_id AND deleted_at IS NONE \
                     AND time_start IS NOT NONE AND time_start <= $horizon \
                     AND (time_start >= $now \
                          OR (time_end IS NOT NONE AND time_end >= $now) \
                          OR (time_precision = 'day' AND time_start >= $day_floor)) \
                     ORDER BY time_start ASC LIMIT $max_rows"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "now" => now,
                    "horizon" => horizon,
                    "day_floor" => day_floor,
                    "max_rows" => i64::try_from(max_rows).unwrap_or(i64::MAX),
                },
            )
            .await?;

        let rows: Vec<EventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse upcoming events: {e}"))
        })?;
        rows.into_iter().map(Event::try_from).collect()
    }

    /// Gets an event including soft-deleted rows.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Event>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Event>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse event: {e}")))?;
        rows.into_iter().next().map(Event::try_from).transpose()
    }

    /// Restores a soft-deleted event.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Event> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<Event>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {EVENT_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored event: {e}"))
        })?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Event not found in space: {id}")))?;
        Event::try_from(row)
    }

    /// Queries by optional time window and status filters.
    pub async fn query_window(
        &self,
        space_id: &str,
        query: &EventWindowQuery,
    ) -> StorageResult<Vec<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<Event>().await?;
            rows.retain(|row| row.base.space_id == space_id && row.base.deleted_at.is_none());
            if let Some(start) = query.start {
                rows.retain(|row| row.time_start.is_some_and(|time_start| time_start >= start));
            }
            if let Some(end) = query.end {
                rows.retain(|row| row.time_start.is_some_and(|time_start| time_start <= end));
            }
            if let Some(statuses) = query.statuses.as_ref() {
                rows.retain(|row| statuses.contains(&row.status));
            }
            rows.sort_by(|lhs, rhs| {
                lhs.time_start
                    .cmp(&rhs.time_start)
                    .then_with(|| lhs.base.created_at.cmp(&rhs.base.created_at))
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            if rows.len() > query.limit {
                rows.truncate(query.limit);
            }
            return Ok(rows);
        }

        let mut conditions = vec![
            "space_id = $space_id".to_string(),
            "deleted_at IS NONE".to_string(),
        ];
        let mut bindings = std::collections::BTreeMap::new();
        bindings.insert(
            "space_id".to_string(),
            surrealdb::sql::Value::from(space_id.to_string()),
        );

        if let Some(start) = query.start {
            conditions.push("time_start IS NOT NONE".to_string());
            conditions.push("time_start >= $start".to_string());
            bindings.insert("start".to_string(), surrealdb::sql::Value::from(start));
        }

        if let Some(end) = query.end {
            conditions.push("time_start IS NOT NONE".to_string());
            conditions.push("time_start <= $end".to_string());
            bindings.insert("end".to_string(), surrealdb::sql::Value::from(end));
        }

        if let Some(statuses) = query.statuses.as_ref() {
            if !statuses.is_empty() {
                let mut status_conditions = Vec::with_capacity(statuses.len());
                for (idx, status) in statuses.iter().enumerate() {
                    let key = format!("status_{idx}");
                    status_conditions.push(format!("status = ${key}"));
                    bindings.insert(key, surrealdb::sql::Value::from(status.to_string()));
                }
                conditions.push(format!("({})", status_conditions.join(" OR ")));
            }
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_sql = format!(
            "SELECT {EVENT_PROJECTION} FROM event {where_clause} ORDER BY time_start ASC, created_at ASC LIMIT {}",
            query.limit
        );

        let mut response = self.client.query_bind(&query_sql, bindings).await?;
        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse window events: {e}")))?;
        rows.into_iter().map(Event::try_from).collect()
    }
}

#[async_trait]
impl Repository<Event> for EventRepository {
    async fn create(&self, record: Event) -> StorageResult<Event> {
        record.validate()?;
        self.validate_subject_links_in_space(&record.base.space_id, &record.base.subjects)
            .await?;

        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {EVENT_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => Self::event_to_value(&record)?,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse created event: {e}")))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but event not found".to_string())
        })?;
        Event::try_from(row)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Event>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Event>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {EVENT_PROJECTION} FROM event WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse event: {e}")))?;
        rows.into_iter().next().map(Event::try_from).transpose()
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Event) -> StorageResult<Event> {
        let record = self.prepare_replacement(space_id, id, record).await?;

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, record).await;
        }

        if self.get(space_id, id).await?.is_none() {
            return Err(StorageError::NotFound(format!(
                "Event not found in space: {id}"
            )));
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {EVENT_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::event_to_value(&record)?,
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse updated event: {e}")))?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Event not found in space: {id}")))?;
        Event::try_from(row)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Event>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
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
    ) -> StorageResult<Vec<Event>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<Event>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {EVENT_PROJECTION} FROM event WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {EVENT_PROJECTION} FROM event WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |id| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id))
                    }),
                },
            )
            .await?;

        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse events: {e}")))?;
        rows.into_iter().map(Event::try_from).collect()
    }
}

impl EventRepository {
    /// Searches events with optional status filtering pushed down to SurrealDB.
    ///
    /// This is used by timeline/MCP query paths so candidate gating can happen
    /// at the database boundary rather than after fetching.
    #[allow(clippy::too_many_lines)]
    pub async fn search_filtered(
        &self,
        criteria: &SearchCriteria,
        statuses: Option<&[EventStatus]>,
    ) -> StorageResult<PaginatedResult<Event>> {
        if let Some(store) = self.client.embedded_store() {
            let statuses = statuses.map(<[elephant_types::records::EventStatus]>::to_vec);
            return store
                .search::<Event, _>(criteria, |event, criteria| {
                    if let Some(query) = criteria.query.as_ref() {
                        let query = query.to_ascii_lowercase();
                        let title = event.title.to_ascii_lowercase();
                        let summary = event.summary.as_deref().unwrap_or("").to_ascii_lowercase();
                        if !title.contains(&query) && !summary.contains(&query) {
                            return false;
                        }
                    }

                    if let Some(entities) = criteria.entities.as_ref() {
                        if !entities.is_empty() {
                            let mode = criteria.mode.as_deref().unwrap_or("any");
                            let contains = |entity: &String| {
                                event
                                    .base
                                    .subjects
                                    .iter()
                                    .any(|subject| subject.id().to_string() == *entity)
                            };
                            let matched = if mode == "all" {
                                entities.iter().all(contains)
                            } else {
                                entities.iter().any(contains)
                            };
                            if !matched {
                                return false;
                            }
                        }
                    }

                    statuses
                        .as_ref()
                        .is_none_or(|statuses| statuses.contains(&event.status))
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

        if let Some(space_id) = criteria.space_id.as_ref() {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        if let Some(query) = criteria.query.as_ref() {
            conditions.push(
                "(string::lowercase(title) CONTAINS string::lowercase($query) \
                 OR string::lowercase(IF summary IS NONE THEN '' ELSE summary END) CONTAINS string::lowercase($query))"
                    .to_string(),
            );
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(entities) = criteria.entities.as_ref() {
            if !entities.is_empty() {
                let mode = criteria.mode.as_deref().unwrap_or("any");
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

        if let Some(statuses) = statuses {
            if !statuses.is_empty() {
                let mut status_conditions = Vec::with_capacity(statuses.len());
                for (idx, status) in statuses.iter().enumerate() {
                    let key = format!("status_{idx}");
                    status_conditions.push(format!("status = ${key}"));
                    bindings.insert(key, surrealdb::sql::Value::from(status.to_string()));
                }
                conditions.push(format!("({})", status_conditions.join(" OR ")));
            }
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::Event)
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
            "SELECT {EVENT_PROJECTION} FROM event {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;
        let rows: Vec<EventDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse event search: {e}")))?;
        let mut events: Vec<Event> = rows
            .into_iter()
            .map(Event::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = events.len() > criteria.limit;
        if has_more {
            events.truncate(criteria.limit);
        }

        let next_cursor = if has_more {
            events.last().map(|event| {
                let id_str = event.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        let count_query = format!("SELECT count() as total FROM event {where_clause} GROUP ALL");
        let mut count_response = self.client.query_bind(&count_query, bindings).await?;
        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse event count")?;
        let total = counts.first().map_or(0, |count| count.total);

        Ok(PaginatedResult::new(events, next_cursor, has_more, total))
    }
}

#[async_trait]
impl SearchRepository<Event> for EventRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Event>> {
        self.search_filtered(criteria, None).await
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
    use crate::EntityRepository;
    use crate::StorageConfig;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(92000);

    const SCHEMA: &str = r"
        DEFINE TABLE event SCHEMALESS;
        DEFINE INDEX event_space_time ON TABLE event FIELDS space_id, time_start;
    ";

    async fn setup_test_repo() -> EventRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("event_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        client.query(SCHEMA).await.unwrap();
        EventRepository::new(client)
    }

    const MIGRATION_001: &str = include_str!("../../migrations/sql/001_init.surql");
    const MIGRATION_002: &str = include_str!("../../migrations/sql/002_indexes.surql");
    const MIGRATION_003: &str =
        include_str!("../../migrations/sql/003_flexible_nested_objects.surql");
    const MIGRATION_004: &str = include_str!("../../migrations/sql/004_space_config.surql");
    const MIGRATION_005: &str = include_str!("../../migrations/sql/005_entity_embedding.surql");
    const MIGRATION_006: &str = include_str!("../../migrations/sql/006_add_space_purpose.surql");
    const MIGRATION_007: &str = include_str!("../../migrations/sql/007_drop_doc_chunk.surql");
    const MIGRATION_008: &str = include_str!("../../migrations/sql/008_add_space_skill.surql");
    const MIGRATION_009: &str = include_str!("../../migrations/sql/009_freeform_entity_type.surql");
    const MIGRATION_010: &str = include_str!("../../migrations/sql/010_add_event.surql");

    async fn setup_migrated_test_repo() -> (EventRepository, StorageClient) {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("event_repo_migrated_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        for sql in [
            MIGRATION_001,
            MIGRATION_002,
            MIGRATION_003,
            MIGRATION_004,
            MIGRATION_005,
            MIGRATION_006,
            MIGRATION_007,
            MIGRATION_008,
            MIGRATION_009,
            MIGRATION_010,
        ] {
            client.query(sql).await.unwrap();
        }

        (EventRepository::new(client.clone()), client)
    }

    fn make_event(
        space_id: &str,
        title: &str,
        status: EventStatus,
        time_start: Option<DateTime<Utc>>,
    ) -> Event {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Event),
            space_id.to_string(),
            "test_agent".to_string(),
        );

        let assertion_ref = Link::new(RecordId::new_v4(TableName::Assertion));
        let mut event = Event::new(base, title.to_string(), vec![assertion_ref], Vec::new())
            .expect("valid event");
        event.status = status;
        event.time_start = time_start;
        event.time_end = time_start.map(|start| start + Duration::hours(1));
        event.time_precision = TimePrecision::Hour;
        event.confidence = 0.9;
        event.time_confidence = 0.85;
        event.metadata = serde_json::json!({"fixture": true});
        event
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep entity_repo in the original fixture scope through assertions and cleanup."
    )]
    async fn create_test_entity(
        client: &StorageClient,
        space_id: &str,
        display_name: &str,
    ) -> RecordId {
        use elephant_types::records::entity::{Entity, EntityStatus, EntityType};

        let entity_repo = EntityRepository::new(client.clone());
        let id = RecordId::new_v4(TableName::Entity);
        let entity = Entity {
            base: BaseFields::new(id.clone(), space_id.to_string(), "test_agent".to_string()),
            display_name: display_name.to_string(),
            entity_type: EntityType::new("person"),
            aliases: Vec::new(),
            summary: None,
            handles: Vec::new(),
            identity_keys: Vec::new(),
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::json!({}),
        };
        entity_repo.create(entity).await.expect("create entity");
        id
    }

    /// Ordinary event edits keep the stored security, atomically, on any backend.
    async fn assert_update_preserves_security(repo: &EventRepository) {
        let space_id = "space_a";
        let mut event = make_event(space_id, "Classified", EventStatus::Approved, None);
        event.base.security = Security {
            policy_id: "default".to_string(),
            level: 3,
            labels: vec!["private".to_string()],
            handling: vec!["no-share".to_string()],
        };
        let id = event.base.id.clone();
        repo.create(event).await.unwrap();
        let stored = repo.get(space_id, &id).await.unwrap().expect("event");

        // Declassifying edits conflict and leave the row untouched.
        let mut lowered = stored.clone();
        lowered.base.security.level = 0;
        let mut unlabeled = stored.clone();
        unlabeled.base.security.labels.clear();
        let mut unhandled = stored.clone();
        unhandled.base.security.handling.clear();
        let mut other_policy = stored.clone();
        other_policy.base.security.policy_id = "other_policy".to_string();
        for attempt in [lowered, unlabeled, unhandled, other_policy] {
            let result = repo
                .update_preserving_security(space_id, &id, attempt)
                .await;
            assert!(
                matches!(result, Err(StorageError::Conflict(_))),
                "{result:?}"
            );
        }
        let after = repo.get(space_id, &id).await.unwrap().expect("event");
        assert_eq!(after.base.security, stored.base.security);
        assert_eq!(after.title, "Classified");

        // An edit that keeps the security succeeds and keeps it exactly.
        let mut edit = stored.clone();
        edit.title = "Classified, renamed".to_string();
        let saved = repo
            .update_preserving_security(space_id, &id, edit)
            .await
            .unwrap();
        assert_eq!(saved.title, "Classified, renamed");
        assert_eq!(saved.base.security, stored.base.security);

        // Stale authorization: another writer restricts the row after this
        // caller's snapshot; the late write conflicts and the restriction stays.
        let snapshot = repo.get(space_id, &id).await.unwrap().expect("event");
        let mut stricter = snapshot.clone();
        stricter.base.security.level = 5;
        stricter.title = "Restricted".to_string();
        repo.update(space_id, &id, stricter).await.unwrap();
        let mut late = snapshot.clone();
        late.title = "Late edit".to_string();
        let result = repo.update_preserving_security(space_id, &id, late).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        let current = repo.get(space_id, &id).await.unwrap().expect("event");
        assert_eq!(current.base.security.level, 5);
        assert_eq!(current.title, "Restricted");

        // A deleted row is not revived by an ordinary edit.
        repo.delete(space_id, &id).await.unwrap();
        let mut revive = current.clone();
        revive.title = "Revived".to_string();
        let result = repo.update_preserving_security(space_id, &id, revive).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        assert!(repo.get(space_id, &id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_update_preserving_security_surreal() {
        let repo = setup_test_repo().await;
        Box::pin(assert_update_preserves_security(&repo)).await;
    }

    #[tokio::test]
    async fn test_update_preserving_security_embedded() {
        let repo = EventRepository::new(StorageClient::new_embedded().await.unwrap());
        Box::pin(assert_update_preserves_security(&repo)).await;
    }

    async fn assert_legacy_metadata_roundtrip(repo: &EventRepository) {
        let space = "legacy-event-metadata";
        let mut original = make_event(space, "Original", EventStatus::Candidate, None);
        original.metadata = serde_json::Value::Null;
        let mut saved = repo.create(original).await.unwrap();
        assert_eq!(saved.metadata, serde_json::Value::Null);
        for metadata in [
            serde_json::Value::Null,
            serde_json::json!("original scalar"),
            serde_json::json!([null, "original", 7, {"nested": [false, 0.125]}]),
            serde_json::json!({"nullable": null, "empty": {}, "array": []}),
            serde_json::json!(false),
            serde_json::json!(0.125),
        ] {
            let mut edit = saved.clone();
            edit.metadata = metadata.clone();
            saved = repo
                .update_preserving_security(space, &edit.base.id, edit.clone())
                .await
                .unwrap();
            assert_eq!(saved.metadata, metadata);
            assert_eq!(saved.base.security, edit.base.security);
            assert_eq!(
                repo.get(space, &saved.base.id).await.unwrap(),
                Some(saved.clone())
            );
            assert_eq!(
                repo.list_recent(space, 10).await.unwrap(),
                vec![saved.clone()]
            );
            assert_eq!(
                repo.list(space, 10, None).await.unwrap(),
                vec![saved.clone()]
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original native client and repository custody alive through every corruption/refusal/readback assertion and final scenario teardown."
    )]
    async fn legacy_event_metadata_native_exact_json_and_missing_field_refusal() {
        let repo = setup_test_repo().await;
        Box::pin(assert_legacy_metadata_roundtrip(&repo)).await;
        let space = "legacy-event-metadata";
        let saved = repo.list(space, 10, None).await.unwrap().remove(0);
        // A stored projection alias cannot manufacture presence for an absent
        // metadata value; the query computes this bit from the actual field.
        repo.client
            .query_bind_checked(
                "UPDATE $id SET metadata = NONE, metadata_present = true;",
                ("id", crate::surreal::thing_from_record_id(&saved.base.id)),
            )
            .await
            .unwrap();
        assert!(matches!(
            repo.get(space, &saved.base.id).await,
            Err(StorageError::QueryError(message)) if message == "Missing metadata"
        ));
        assert!(repo.list_recent(space, 10).await.is_err());
        assert!(repo.list(space, 10, None).await.is_err());
        repo.client
            .query_bind_checked(
                "UPDATE $id SET metadata = NULL, metadata_present = false;",
                ("id", crate::surreal::thing_from_record_id(&saved.base.id)),
            )
            .await
            .unwrap();
        assert_eq!(
            repo.get(space, &saved.base.id)
                .await
                .unwrap()
                .unwrap()
                .metadata,
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn legacy_event_metadata_embedded_exact_json_roundtrip() {
        let repo = EventRepository::new(StorageClient::new_embedded().await.unwrap());
        Box::pin(assert_legacy_metadata_roundtrip(&repo)).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_event_crud_roundtrip() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";

        let event = make_event(
            space_id,
            "Launch window",
            EventStatus::Candidate,
            Some(Utc::now() + Duration::days(1)),
        );
        let id = event.base.id.clone();

        let created = repo.create(event.clone()).await.unwrap();
        assert_eq!(created.base.id, id);
        assert_eq!(created.title, "Launch window");

        let fetched = repo
            .get(space_id, &id)
            .await
            .unwrap()
            .expect("event exists");
        assert_eq!(fetched.base.id, id);

        let mut updated = fetched.clone();
        updated.title = "Revised launch window".to_string();
        updated.status = EventStatus::Approved;
        updated.summary = Some("Promoted timeline event".to_string());

        let updated_saved = repo.update(space_id, &id, updated.clone()).await.unwrap();
        assert_eq!(updated_saved.title, updated.title);
        assert_eq!(updated_saved.status, EventStatus::Approved);

        repo.delete(space_id, &id).await.unwrap();
        let missing = repo.get(space_id, &id).await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_event_list_with_cursor() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";

        let _ = repo
            .create(make_event(
                space_id,
                "Event 1",
                EventStatus::Candidate,
                None,
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(space_id, "Event 2", EventStatus::Approved, None))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(space_id, "Event 3", EventStatus::Rejected, None))
            .await
            .unwrap();

        let page1 = repo.list(space_id, 2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        let cursor = page1.last().expect("cursor row").base.id.clone();

        let page2 = repo.list(space_id, 10, Some(&cursor)).await.unwrap();
        assert!(page2
            .iter()
            .all(|item| item.base.id.to_string() > cursor.to_string()));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_event_search_by_query_and_entities() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";
        let entity_a = create_test_entity(&repo.client, space_id, "Entity A").await;
        let entity_b = create_test_entity(&repo.client, space_id, "Entity B").await;

        let mut event_a = make_event(
            space_id,
            "Quarterly launch review",
            EventStatus::Approved,
            Some(Utc::now()),
        );
        event_a.base.subjects = vec![Link::new(entity_a.clone())];
        event_a.summary = Some("Launch board meeting".to_string());
        let _ = repo.create(event_a).await.unwrap();

        let mut event_b = make_event(
            space_id,
            "Daily standup",
            EventStatus::Candidate,
            Some(Utc::now()),
        );
        event_b.base.subjects = vec![Link::new(entity_b.clone())];
        let _ = repo.create(event_b).await.unwrap();

        let criteria = SearchCriteria::new()
            .with_space_id(space_id.to_string())
            .with_query("launch")
            .with_limit(10);
        let search = repo.search(&criteria).await.unwrap();
        assert_eq!(search.results.len(), 1);
        assert_eq!(search.results[0].title, "Quarterly launch review");

        let criteria = SearchCriteria::new()
            .with_space_id(space_id.to_string())
            .with_entities(vec![entity_a.to_string(), entity_b.to_string()])
            .with_mode("all")
            .with_limit(10);
        let search = repo.search(&criteria).await.unwrap();
        assert!(search.results.is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep entity_repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_prunes_missing_subject_links() {
        let (repo, _) = setup_migrated_test_repo().await;
        let space_id = "space_a";
        let entity_repo = EntityRepository::new(repo.client.clone());
        let entity = create_test_entity(&repo.client, space_id, "Entity A").await;

        let mut event = make_event(
            space_id,
            "Quarterly launch review",
            EventStatus::Candidate,
            Some(Utc::now()),
        );
        event.base.subjects = vec![Link::new(entity.clone())];
        event.base.subjects_state = SubjectsState::Complete;
        let mut event = repo.create(event).await.unwrap();

        entity_repo.delete(space_id, &entity).await.unwrap();
        event.status = EventStatus::Approved;

        let updated = repo
            .update(space_id, &event.base.id.clone(), event)
            .await
            .unwrap();
        assert!(updated.base.subjects.is_empty());
        assert_eq!(updated.base.subjects_state, SubjectsState::Unknown);
        assert_eq!(updated.status, EventStatus::Approved);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep entity_repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_rewrites_merged_subject_links() {
        use elephant_types::records::entity::{Entity, EntityStatus, EntityType};

        let (repo, _) = setup_migrated_test_repo().await;
        let space_id = "space_a";
        let entity_repo = EntityRepository::new(repo.client.clone());
        let active = create_test_entity(&repo.client, space_id, "Entity A").await;

        let merged_id = RecordId::new_v4(TableName::Entity);
        let mut merged = Entity::new(
            BaseFields::new(
                merged_id.clone(),
                space_id.to_string(),
                "test_agent".to_string(),
            ),
            EntityType::new("person"),
            "Entity A Old".to_string(),
        );
        merged.status = EntityStatus::Merged;
        merged.redirect_to = Some(Link::new(active.clone()));
        entity_repo.create(merged).await.unwrap();

        let mut event = make_event(
            space_id,
            "Quarterly launch review",
            EventStatus::Candidate,
            Some(Utc::now()),
        );
        event.base.subjects = vec![Link::new(merged_id)];
        event.base.subjects_state = SubjectsState::Complete;
        let mut event = repo.create(event).await.unwrap();
        event.status = EventStatus::Approved;

        let updated = repo
            .update(space_id, &event.base.id.clone(), event)
            .await
            .unwrap();
        assert_eq!(updated.base.subjects, vec![Link::new(active)]);
        assert_eq!(updated.base.subjects_state, SubjectsState::Complete);
        assert_eq!(updated.status, EventStatus::Approved);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_event_search_filtered_by_status_in_db_query() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";
        let entity = create_test_entity(&repo.client, space_id, "Entity A").await;

        let mut approved = make_event(
            space_id,
            "Quarterly launch review",
            EventStatus::Approved,
            Some(Utc::now()),
        );
        approved.base.subjects = vec![Link::new(entity.clone())];
        repo.create(approved).await.unwrap();

        let mut candidate = make_event(
            space_id,
            "Quarterly launch review",
            EventStatus::Candidate,
            Some(Utc::now()),
        );
        candidate.base.subjects = vec![Link::new(entity)];
        repo.create(candidate).await.unwrap();

        let criteria = SearchCriteria::new()
            .with_space_id(space_id.to_string())
            .with_query("launch")
            .with_limit(10);

        let filtered = repo
            .search_filtered(&criteria, Some(&[EventStatus::Approved]))
            .await
            .unwrap();
        assert_eq!(filtered.results.len(), 1);
        assert_eq!(filtered.results[0].status, EventStatus::Approved);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_event_recent_and_upcoming_queries() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";
        let now = Utc::now();

        let _ = repo
            .create(make_event(
                space_id,
                "Past event",
                EventStatus::Approved,
                Some(now - Duration::days(2)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Soon event",
                EventStatus::Approved,
                Some(now + Duration::days(2)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Far event",
                EventStatus::Approved,
                Some(now + Duration::days(14)),
            ))
            .await
            .unwrap();

        let recent = repo.get_recent(space_id, 3).await.unwrap();
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].title, "Far event");

        let upcoming = repo.get_upcoming(space_id, 7).await.unwrap();
        let titles: HashSet<String> = upcoming.into_iter().map(|event| event.title).collect();
        assert!(titles.contains("Soon event"));
        assert!(!titles.contains("Past event"));
        assert!(!titles.contains("Far event"));
    }

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn make_timed_event(
        space_id: &str,
        title: &str,
        precision: TimePrecision,
        time_start: &str,
        time_end: Option<&str>,
    ) -> Event {
        let mut event = make_event(
            space_id,
            title,
            EventStatus::Approved,
            Some(utc(time_start)),
        );
        event.time_end = time_end.map(utc);
        event.time_precision = precision;
        event
    }

    async fn upcoming_titles_at(repo: &EventRepository, space_id: &str, now: &str) -> Vec<String> {
        repo.get_upcoming_at(space_id, 7, utc(now), DEFAULT_UPCOMING_LIMIT)
            .await
            .unwrap()
            .into_iter()
            .map(|event| event.title)
            .collect()
    }

    /// iss-20261011-75020ed6: day-precision and in-progress events stay upcoming.
    async fn assert_upcoming_precision_semantics(repo: &EventRepository) {
        let space_id = "space_upcoming";
        for event in [
            make_timed_event(
                space_id,
                "Today (day)",
                TimePrecision::Day,
                "2026-10-11T00:00:00Z",
                None,
            ),
            make_timed_event(
                space_id,
                "Yesterday (day)",
                TimePrecision::Day,
                "2026-10-10T00:00:00Z",
                None,
            ),
            make_timed_event(
                space_id,
                "In progress",
                TimePrecision::Minute,
                "2026-10-11T09:00:00Z",
                Some("2026-10-11T11:00:00Z"),
            ),
            make_timed_event(
                space_id,
                "Ended",
                TimePrecision::Minute,
                "2026-10-11T08:00:00Z",
                Some("2026-10-11T09:00:00Z"),
            ),
            make_timed_event(
                space_id,
                "Started, no end",
                TimePrecision::Exact,
                "2026-10-11T09:30:00Z",
                None,
            ),
            make_timed_event(
                space_id,
                "Later today",
                TimePrecision::Minute,
                "2026-10-11T15:00:00Z",
                Some("2026-10-11T16:00:00Z"),
            ),
            make_timed_event(
                space_id,
                "Beyond horizon",
                TimePrecision::Day,
                "2026-10-30T00:00:00Z",
                None,
            ),
        ] {
            repo.create(event).await.unwrap();
        }

        // 10:00Z (12:00 in Stockholm): today's day event, the in-progress meeting and the
        // later meeting are upcoming, ordered by time_start.
        let titles = upcoming_titles_at(repo, space_id, "2026-10-11T10:00:00Z").await;
        assert_eq!(titles, vec!["Today (day)", "In progress", "Later today"]);

        // 23:30Z: today's day event is still within 24h of its 00:00Z anchor; yesterday's
        // (47.5h old) is not.
        let titles = upcoming_titles_at(repo, space_id, "2026-10-11T23:30:00Z").await;
        assert_eq!(titles, vec!["Today (day)"]);

        // Yesterday's day event was still upcoming at 23:30Z on its own date.
        let titles = upcoming_titles_at(repo, space_id, "2026-10-10T23:30:00Z").await;
        assert_eq!(
            titles,
            vec![
                "Yesterday (day)",
                "Today (day)",
                "Ended",
                "In progress",
                "Started, no end",
                "Later today"
            ]
        );

        // max_rows caps the ordered result.
        let capped = repo
            .get_upcoming_at(space_id, 7, utc("2026-10-11T10:00:00Z"), 2)
            .await
            .unwrap();
        assert_eq!(capped.len(), 2);
        assert_eq!(capped[0].title, "Today (day)");
    }

    #[tokio::test]
    async fn test_upcoming_includes_day_precision_and_in_progress_events_surreal() {
        let repo = setup_test_repo().await;
        assert_upcoming_precision_semantics(&repo).await;
    }

    #[tokio::test]
    async fn test_upcoming_includes_day_precision_and_in_progress_events_embedded() {
        let repo = EventRepository::new(StorageClient::new_embedded().await.unwrap());
        assert_upcoming_precision_semantics(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_update_enforce_event_invariants() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";

        let mut invalid = make_event(space_id, "Valid title", EventStatus::Candidate, None);
        invalid.title = "  ".to_string();
        let create_err = repo.create(invalid).await.expect_err("must fail");
        assert!(matches!(create_err, StorageError::TypeError(_)));

        let created = repo
            .create(make_event(
                space_id,
                "Valid event",
                EventStatus::Candidate,
                None,
            ))
            .await
            .unwrap();

        let mut invalid_update = created.clone();
        invalid_update.assertion_refs.clear();
        invalid_update.source_refs.clear();

        let update_err = repo
            .update(space_id, &created.base.id, invalid_update)
            .await
            .expect_err("must fail");
        assert!(matches!(update_err, StorageError::TypeError(_)));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_wrong_space_does_not_leak_existing_event() {
        let repo = setup_test_repo().await;
        let source_space = "space_a";
        let wrong_space = "space_b";

        let created = repo
            .create(make_event(
                source_space,
                "Original event",
                EventStatus::Approved,
                Some(Utc::now() + Duration::hours(1)),
            ))
            .await
            .unwrap();

        let mut wrong_space_record = make_event(
            wrong_space,
            "Tampered title",
            EventStatus::Rejected,
            Some(Utc::now() + Duration::hours(2)),
        );
        wrong_space_record.base.id = created.base.id.clone();

        let err = repo
            .update(wrong_space, &created.base.id, wrong_space_record)
            .await
            .expect_err("cross-space update must fail");
        assert!(matches!(err, StorageError::NotFound(_)));

        let fetched_source = repo
            .get(source_space, &created.base.id)
            .await
            .unwrap()
            .expect("source-space row exists");
        assert_eq!(fetched_source.title, "Original event");
        assert_eq!(fetched_source.status, EventStatus::Approved);

        let fetched_wrong_space = repo.get(wrong_space, &created.base.id).await.unwrap();
        assert!(fetched_wrong_space.is_none());
    }

    #[tokio::test]
    async fn test_migrated_schema_event_create_read_roundtrip() {
        let (repo, _client) = setup_migrated_test_repo().await;
        let space_id = "migrated_space";

        let event = make_event(
            space_id,
            "Migrated table event",
            EventStatus::Approved,
            Some(Utc::now() + Duration::days(1)),
        );
        let id = event.base.id.clone();

        let created = repo.create(event).await.unwrap();
        assert_eq!(created.base.id, id);

        let fetched = repo
            .get(space_id, &id)
            .await
            .unwrap()
            .expect("event must round-trip on migrated schema");
        assert_eq!(fetched.title, "Migrated table event");
        assert_eq!(fetched.status, EventStatus::Approved);
    }

    #[tokio::test]
    async fn test_migrated_schema_rejects_invalid_writes() {
        let (repo, client) = setup_migrated_test_repo().await;
        let space_id = "migrated_space";

        // Repository-level validation rejections.
        let mut invalid_title = make_event(space_id, "Valid", EventStatus::Approved, None);
        invalid_title.title = "   ".to_string();
        let title_err = repo
            .create(invalid_title)
            .await
            .expect_err("empty title must fail");
        assert!(matches!(title_err, StorageError::TypeError(_)));

        let mut invalid_confidence = make_event(space_id, "Valid", EventStatus::Approved, None);
        invalid_confidence.confidence = 1.5;
        let confidence_err = repo
            .create(invalid_confidence)
            .await
            .expect_err("out-of-range confidence must fail");
        assert!(matches!(confidence_err, StorageError::TypeError(_)));

        let now = Utc::now();
        let mut invalid_range = make_event(space_id, "Valid", EventStatus::Approved, Some(now));
        invalid_range.time_end = Some(now - Duration::minutes(1));
        let range_err = repo
            .create(invalid_range)
            .await
            .expect_err("invalid time range must fail");
        assert!(matches!(range_err, StorageError::TypeError(_)));

        // Schema-level invariants on migrated SCHEMAFULL table (bypassing repository).
        let created = repo
            .create(make_event(
                space_id,
                "Schema invariant target",
                EventStatus::Approved,
                Some(Utc::now() + Duration::hours(1)),
            ))
            .await
            .unwrap();

        let response = client
            .query_bind(
                "UPDATE $id SET title = $title;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&created.base.id),
                    "title" => "   ",
                },
            )
            .await
            .unwrap();
        assert!(
            response.check().is_err(),
            "schema must reject empty/whitespace title"
        );

        let response = client
            .query_bind(
                "UPDATE $id SET confidence = $confidence;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&created.base.id),
                    "confidence" => 1.2_f64,
                },
            )
            .await
            .unwrap();
        assert!(
            response.check().is_err(),
            "schema must reject confidence outside [0,1]"
        );

        let time_start = Utc::now() + Duration::hours(3);
        let time_end = time_start - Duration::hours(1);
        let response = client
            .query_bind(
                "UPDATE $id SET time_start = $time_start, time_end = $time_end;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&created.base.id),
                    "time_start" => time_start,
                    "time_end" => time_end,
                },
            )
            .await
            .unwrap();
        assert!(
            response.check().is_err(),
            "schema must reject time_end < time_start"
        );
    }

    #[tokio::test]
    async fn test_migrated_schema_contract_methods_list_search_recent_upcoming() {
        let (repo, _client) = setup_migrated_test_repo().await;
        let space_id = "migrated_space";
        let now = Utc::now();

        let entity_a = create_test_entity(&repo.client, space_id, "Entity A").await;
        let entity_b = create_test_entity(&repo.client, space_id, "Entity B").await;

        let mut launch_a = make_event(
            space_id,
            "Launch A",
            EventStatus::Approved,
            Some(now + Duration::days(2)),
        );
        launch_a.base.subjects = vec![Link::new(entity_a.clone())];
        let created_launch_a = repo.create(launch_a).await.unwrap();

        let mut planning_b = make_event(
            space_id,
            "Planning B",
            EventStatus::Candidate,
            Some(now + Duration::days(3)),
        );
        planning_b.base.subjects = vec![Link::new(entity_b.clone())];
        let _ = repo.create(planning_b).await.unwrap();

        let mut joint_launch = make_event(
            space_id,
            "Joint Launch",
            EventStatus::Approved,
            Some(now + Duration::days(4)),
        );
        joint_launch.base.subjects = vec![Link::new(entity_a.clone()), Link::new(entity_b.clone())];
        let _ = repo.create(joint_launch).await.unwrap();

        let mut past_review = make_event(
            space_id,
            "Past Review",
            EventStatus::Approved,
            Some(now - Duration::days(1)),
        );
        past_review.base.subjects = vec![Link::new(entity_a.clone())];
        let _ = repo.create(past_review).await.unwrap();

        let mut far_future = make_event(
            space_id,
            "Far Future",
            EventStatus::Approved,
            Some(now + Duration::days(15)),
        );
        far_future.base.subjects = vec![Link::new(entity_a)];
        let _ = repo.create(far_future).await.unwrap();

        // list + cursor behavior
        let page1 = repo.list(space_id, 2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        let cursor = page1.last().expect("cursor row").base.id.clone();
        let page2 = repo.list(space_id, 10, Some(&cursor)).await.unwrap();
        assert!(!page2.is_empty());
        assert!(page2
            .iter()
            .all(|item| item.base.id.to_string() > cursor.to_string()));

        // search by query
        let query_result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id(space_id.to_string())
                    .with_query("launch")
                    .with_limit(10),
            )
            .await
            .unwrap();
        let query_titles: HashSet<String> = query_result
            .results
            .iter()
            .map(|event| event.title.clone())
            .collect();
        assert!(query_titles.contains("Launch A"));
        assert!(query_titles.contains("Joint Launch"));

        // search by entities with mode=all
        let entity_result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id(space_id.to_string())
                    .with_entities(vec![
                        created_launch_a.base.subjects[0].id().to_string(),
                        entity_b.to_string(),
                    ])
                    .with_mode("all")
                    .with_limit(10),
            )
            .await
            .unwrap();
        assert_eq!(entity_result.results.len(), 1);
        assert_eq!(entity_result.results[0].title, "Joint Launch");

        // get_recent behavior
        let recent = repo.get_recent(space_id, 3).await.unwrap();
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].title, "Far Future");

        // get_upcoming behavior
        let upcoming = repo.get_upcoming(space_id, 7).await.unwrap();
        let upcoming_titles: HashSet<String> =
            upcoming.into_iter().map(|event| event.title).collect();
        assert!(upcoming_titles.contains("Launch A"));
        assert!(upcoming_titles.contains("Planning B"));
        assert!(upcoming_titles.contains("Joint Launch"));
        assert!(!upcoming_titles.contains("Past Review"));
        assert!(!upcoming_titles.contains("Far Future"));
    }

    #[tokio::test]
    #[ignore = "boundary-mode: validates time range behavior for event queries"]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn boundary_mode_time_range_query_respects_window() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";
        let now = Utc::now();

        let _ = repo
            .create(make_event(
                space_id,
                "Before window",
                EventStatus::Approved,
                Some(now - Duration::days(2)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Inside window",
                EventStatus::Approved,
                Some(now + Duration::days(1)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "After window",
                EventStatus::Approved,
                Some(now + Duration::days(10)),
            ))
            .await
            .unwrap();

        let query = EventWindowQuery {
            start: Some(now),
            end: Some(now + Duration::days(3)),
            statuses: None,
            limit: 50,
        };
        let rows = repo.query_window(space_id, &query).await.unwrap();
        let titles: Vec<&str> = rows.iter().map(|event| event.title.as_str()).collect();
        assert_eq!(titles, vec!["Inside window"]);
    }

    #[tokio::test]
    #[ignore = "boundary-mode: validates time range behavior for event queries on migrated schema"]
    async fn boundary_mode_time_range_query_respects_window_on_migrated_schema() {
        let (repo, _client) = setup_migrated_test_repo().await;
        let space_id = "space_a";
        let now = Utc::now();

        let _ = repo
            .create(make_event(
                space_id,
                "Before window migrated",
                EventStatus::Approved,
                Some(now - Duration::days(2)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Inside window migrated",
                EventStatus::Approved,
                Some(now + Duration::days(1)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "After window migrated",
                EventStatus::Approved,
                Some(now + Duration::days(10)),
            ))
            .await
            .unwrap();

        let query = EventWindowQuery {
            start: Some(now),
            end: Some(now + Duration::days(3)),
            statuses: None,
            limit: 50,
        };
        let rows = repo.query_window(space_id, &query).await.unwrap();
        let titles: Vec<&str> = rows.iter().map(|event| event.title.as_str()).collect();
        assert_eq!(titles, vec!["Inside window migrated"]);
    }

    #[tokio::test]
    #[ignore = "boundary-mode: validates status filter behavior for event queries"]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn boundary_mode_status_filter_query_respects_statuses() {
        let repo = setup_test_repo().await;
        let space_id = "space_a";
        let now = Utc::now();

        let _ = repo
            .create(make_event(
                space_id,
                "Candidate event",
                EventStatus::Candidate,
                Some(now + Duration::hours(2)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Approved event",
                EventStatus::Approved,
                Some(now + Duration::hours(3)),
            ))
            .await
            .unwrap();
        let _ = repo
            .create(make_event(
                space_id,
                "Disputed event",
                EventStatus::Rejected,
                Some(now + Duration::hours(4)),
            ))
            .await
            .unwrap();

        let query = EventWindowQuery {
            start: Some(now),
            end: Some(now + Duration::days(1)),
            statuses: Some(vec![EventStatus::Approved]),
            limit: 50,
        };
        let rows = repo.query_window(space_id, &query).await.unwrap();
        let titles: Vec<&str> = rows.iter().map(|event| event.title.as_str()).collect();
        assert_eq!(titles, vec!["Approved event"]);
    }

    /// Guarded restore acts only on the snapshot the caller was authorized on.
    async fn assert_restore_guarded_lifecycle(repo: &EventRepository, space: &str, record: Event) {
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

        // Reclassified after an active snapshot: the guarded restore conflicts
        // and the stricter row is untouched.
        repo.restore(space, &id).await.unwrap();
        let snapshot = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let mut stricter = snapshot.clone();
        stricter.base.security.level = 5;
        repo.update(space, &id, stricter).await.unwrap();
        let result = repo.restore_guarded(space, &id, &snapshot).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        let current = repo.get(space, &id).await.unwrap().expect("active");
        assert_eq!(current.base.security.level, 5);
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_surreal() {
        let repo = setup_test_repo().await;
        assert_restore_guarded_lifecycle(
            &repo,
            "space_a",
            make_event("space_a", "Guarded", EventStatus::Approved, None),
        )
        .await;
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_embedded() {
        let repo = EventRepository::new(StorageClient::new_embedded().await.unwrap());
        assert_restore_guarded_lifecycle(
            &repo,
            "space_a",
            make_event("space_a", "Guarded", EventStatus::Approved, None),
        )
        .await;
    }
}
