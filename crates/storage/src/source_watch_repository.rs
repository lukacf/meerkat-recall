//! Storage for recurring source watches and immutable observation receipts.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::{
    Source, SourceWatch, SourceWatchCheck, SourceWatchOutcome, SourceWatchStatus,
};
use elephant_types::{Link, RecordId, TableName};
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

/// Original input observations for a check which finds already accepted bytes.
/// This is read-set evidence only and carries no source publication authority.
#[derive(Debug, Clone)]
pub struct PreparedSourceWatchObservation {
    source: Source,
    watch: SourceWatch,
    head_payload: String,
}

impl PreparedSourceWatchObservation {
    /// Preserve the exact original raw head before fetching. A changed head is
    /// refused rather than substituted for the caller's earlier observation.
    pub async fn capture(
        storage: &StorageClient,
        watch: &SourceWatch,
        source: &Source,
        head: &elephant_types::publication::SourceHead,
    ) -> StorageResult<Self> {
        use crate::source_publication::{head_key, SourcePublicationRepository, HEAD_TABLE};
        if source.base.deleted_at.is_some()
            || watch.base.deleted_at.is_some()
            || source.base.space_id != watch.base.space_id
            || source.base.id != *watch.source.id()
            || head.space_id != source.base.space_id
            || head.source_id != source.base.id
            || head.lifecycle != elephant_types::publication::SourceVisibility::Active
            || head.desired_revision.is_none()
        {
            return Err(StorageError::Conflict(
                "watch_original_input_changed".into(),
            ));
        }
        let payload = SourcePublicationRepository::new(storage.clone())
            .read_control_payload(
                HEAD_TABLE,
                &head_key(&head.space_id, &head.source_id),
                &head.space_id,
            )
            .await?
            .ok_or_else(|| StorageError::Conflict("watch_original_head_missing".into()))?;
        let actual: elephant_types::publication::SourceHead =
            crate::source_publication::decode_payload(&payload)?;
        if actual != *head {
            return Err(StorageError::Conflict("watch_original_head_changed".into()));
        }
        Ok(Self {
            source: source.clone(),
            watch: watch.clone(),
            head_payload: payload,
        })
    }

    fn check_tx(&self, tx: &crate::EmbeddedMutation<'_>) -> StorageResult<()> {
        use crate::source_publication::{head_key, read_control_payload_tx, HEAD_TABLE};
        let space = &self.source.base.space_id;
        if tx
            .get::<Source>(space, &self.source.base.id, false)?
            .as_ref()
            != Some(&self.source)
            || tx
                .get::<SourceWatch>(space, &self.watch.base.id, false)?
                .as_ref()
                != Some(&self.watch)
            || read_control_payload_tx(
                tx,
                HEAD_TABLE,
                &head_key(space, &self.source.base.id),
                space,
            )?
            .as_deref()
                != Some(self.head_payload.as_str())
        {
            return Err(StorageError::Conflict(
                "watch_original_input_changed".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut std::collections::BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        use crate::canonical_commit::{append_row_guard, native_expected_fields, CanonicalRecord};
        use crate::source_publication::{control_thing, head_key, HEAD_TABLE};
        bindings.extend(crate::bindings! {
            "space" => self.source.base.space_id.clone(),
            "watch_input_head" => control_thing(HEAD_TABLE, &head_key(&self.source.base.space_id, &self.source.base.id)),
            "watch_input_head_payload" => self.head_payload.clone(),
            "watch_input_watch" => crate::surreal::thing_from_record_id(&self.watch.base.id),
            "watch_input_watch_snapshot" => native_expected_fields(SourceWatchRepository::watch_to_value(&self.watch)?),
        });
        sql.push_str("IF (SELECT VALUE payload FROM ONLY $watch_input_head) != $watch_input_head_payload { THROW 'publication:watch_original_input_changed'; };\nUPDATE $watch_input_head SET canonical_read_nonce=<string>rand::uuid();\nLET $watch_input_watch_current = SELECT * FROM ONLY $watch_input_watch;\nIF $watch_input_watch_current = NONE { THROW 'publication:watch_original_input_changed'; };\nFOR $entry IN object::entries($watch_input_watch_snapshot) { IF $watch_input_watch_current[$entry[0]] != $entry[1] { THROW 'publication:watch_original_input_changed'; }; };\n");
        let source = CanonicalRecord::Source(Box::new(self.source.clone()));
        append_row_guard(sql, bindings, "watch_input_source", &source, Some(&source))?;
        // SurrealDB elides unchanged row updates. Use the actual shared Source
        // writer revision, preserving every domain field and erasure barrier.
        sql.push_str(
            "fn::recall_reference_guard({id:$watch_input_source_id,space_id:$space},false);\n",
        );
        Ok(())
    }
}

const WATCH_PROJECTION: &str = r"
    id, space_id, created_at, updated_at, created_by, deleted_at, security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor, run_id: provenance.run_id,
        work_run_id: provenance.work_run_id, prompt_id: provenance.prompt_id,
        config_id: provenance.config_id, chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects, subjects_state, source, requested_uri, status, interval_seconds,
    next_check_at, last_checked_at, last_changed_at, etag, last_modified,
    observed_hash, consecutive_failures, last_error, latest_check
";

const CHECK_PROJECTION: &str = r"
    id, space_id, created_at, updated_at, created_by, deleted_at, security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor, run_id: provenance.run_id,
        work_run_id: provenance.work_run_id, prompt_id: provenance.prompt_id,
        config_id: provenance.config_id, chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects, subjects_state, watch, source, outcome, started_at, completed_at,
    requested_uri, final_uri, content_type, etag, last_modified, prior_hash,
    observed_hash, content_length, error
";

#[derive(Deserialize)]
struct SourceWatchDb {
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
    source: Option<Link<Source>>,
    requested_uri: Option<String>,
    status: Option<SourceWatchStatus>,
    interval_seconds: Option<i64>,
    next_check_at: Option<DateTime<Utc>>,
    last_checked_at: Option<DateTime<Utc>>,
    last_changed_at: Option<DateTime<Utc>>,
    etag: Option<String>,
    last_modified: Option<String>,
    observed_hash: Option<String>,
    consecutive_failures: Option<u32>,
    last_error: Option<String>,
    latest_check: Option<surrealdb::sql::Thing>,
}

#[derive(Deserialize)]
struct SourceWatchCheckDb {
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
    watch: Option<Link<SourceWatch>>,
    source: Option<Link<Source>>,
    outcome: Option<SourceWatchOutcome>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    requested_uri: Option<String>,
    final_uri: Option<String>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    prior_hash: Option<String>,
    observed_hash: Option<String>,
    content_length: Option<u64>,
    error: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn decode_base(
    id: &surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
) -> StorageResult<BaseFields> {
    Ok(BaseFields {
        id: crate::surreal::record_id_from_thing(id)
            .map_err(|error| StorageError::QueryError(error.to_string()))?,
        space_id: required_field(space_id, "space_id")?,
        created_at: required_field(created_at, "created_at")?,
        updated_at: required_field(updated_at, "updated_at")?,
        created_by: required_field(created_by, "created_by")?,
        deleted_at,
        provenance: required_field(provenance, "provenance")?,
        security: required_field(security, "security")?,
        subjects: required_field(subjects, "subjects")?,
        subjects_state: required_field(subjects_state, "subjects_state")?,
    })
}

impl TryFrom<SourceWatchDb> for SourceWatch {
    type Error = StorageError;

    fn try_from(row: SourceWatchDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: decode_base(
                &row.id,
                row.space_id,
                row.created_at,
                row.updated_at,
                row.created_by,
                row.deleted_at,
                row.provenance,
                row.security,
                row.subjects,
                row.subjects_state,
            )?,
            source: required_field(row.source, "source")?,
            requested_uri: required_field(row.requested_uri, "requested_uri")?,
            status: required_field(row.status, "status")?,
            interval_seconds: required_field(row.interval_seconds, "interval_seconds")?,
            next_check_at: required_field(row.next_check_at, "next_check_at")?,
            last_checked_at: row.last_checked_at,
            last_changed_at: row.last_changed_at,
            etag: row.etag,
            last_modified: row.last_modified,
            observed_hash: row.observed_hash,
            consecutive_failures: required_field(row.consecutive_failures, "consecutive_failures")?,
            last_error: row.last_error,
            latest_check: row
                .latest_check
                .as_ref()
                .map(crate::surreal::record_id_from_thing)
                .transpose()
                .map_err(|error| StorageError::QueryError(error.to_string()))?,
        })
    }
}

impl TryFrom<SourceWatchCheckDb> for SourceWatchCheck {
    type Error = StorageError;

    fn try_from(row: SourceWatchCheckDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: decode_base(
                &row.id,
                row.space_id,
                row.created_at,
                row.updated_at,
                row.created_by,
                row.deleted_at,
                row.provenance,
                row.security,
                row.subjects,
                row.subjects_state,
            )?,
            watch: required_field(row.watch, "watch")?,
            source: required_field(row.source, "source")?,
            outcome: required_field(row.outcome, "outcome")?,
            started_at: required_field(row.started_at, "started_at")?,
            completed_at: required_field(row.completed_at, "completed_at")?,
            requested_uri: required_field(row.requested_uri, "requested_uri")?,
            final_uri: row.final_uri,
            content_type: row.content_type,
            etag: row.etag,
            last_modified: row.last_modified,
            prior_hash: row.prior_hash,
            observed_hash: row.observed_hash,
            content_length: row.content_length,
            error: row.error,
        })
    }
}

/// Repository for source watch schedules and receipts.
#[derive(Clone, Debug)]
pub struct SourceWatchRepository {
    client: StorageClient,
}

impl SourceWatchRepository {
    /// Creates a repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    pub(crate) fn watch_to_value(watch: &SourceWatch) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&watch.base)?;
        object.insert(
            "source".to_string(),
            crate::surreal::thing_from_record_id(watch.source.id()).into(),
        );
        object.insert(
            "requested_uri".to_string(),
            watch.requested_uri.clone().into(),
        );
        object.insert("status".to_string(), watch.status.to_string().into());
        object.insert(
            "interval_seconds".to_string(),
            watch.interval_seconds.into(),
        );
        object.insert(
            "next_check_at".to_string(),
            crate::surreal::datetime_from_chrono(watch.next_check_at).into(),
        );
        for (name, value) in [
            ("last_checked_at", watch.last_checked_at),
            ("last_changed_at", watch.last_changed_at),
        ] {
            object.insert(
                name.to_string(),
                value
                    .map(crate::surreal::datetime_from_chrono)
                    .map_or(Value::None, Value::from),
            );
        }
        for (name, value) in [
            ("etag", &watch.etag),
            ("last_modified", &watch.last_modified),
            ("observed_hash", &watch.observed_hash),
            ("last_error", &watch.last_error),
        ] {
            object.insert(
                name.to_string(),
                value.clone().map_or(Value::None, Value::from),
            );
        }
        object.insert(
            "consecutive_failures".to_string(),
            i64::from(watch.consecutive_failures).into(),
        );
        object.insert(
            "latest_check".to_string(),
            watch.latest_check.as_ref().map_or(Value::None, |id| {
                crate::surreal::thing_from_record_id(id).into()
            }),
        );
        Ok(Value::Object(object))
    }

    fn check_to_value(check: &SourceWatchCheck) -> StorageResult<Value> {
        let mut object = crate::surreal::base_fields_to_object(&check.base)?;
        object.insert(
            "watch".to_string(),
            crate::surreal::thing_from_record_id(check.watch.id()).into(),
        );
        object.insert(
            "source".to_string(),
            crate::surreal::thing_from_record_id(check.source.id()).into(),
        );
        object.insert("outcome".to_string(), check.outcome.to_string().into());
        object.insert(
            "started_at".to_string(),
            crate::surreal::datetime_from_chrono(check.started_at).into(),
        );
        object.insert(
            "completed_at".to_string(),
            crate::surreal::datetime_from_chrono(check.completed_at).into(),
        );
        object.insert(
            "requested_uri".to_string(),
            check.requested_uri.clone().into(),
        );
        for (name, value) in [
            ("final_uri", &check.final_uri),
            ("content_type", &check.content_type),
            ("etag", &check.etag),
            ("last_modified", &check.last_modified),
            ("prior_hash", &check.prior_hash),
            ("observed_hash", &check.observed_hash),
            ("error", &check.error),
        ] {
            object.insert(
                name.to_string(),
                value.clone().map_or(Value::None, Value::from),
            );
        }
        let content_length = check
            .content_length
            .map(i64::try_from)
            .transpose()
            .map_err(|_| {
                StorageError::SerializationError("content_length exceeds i64".to_string())
            })?;
        object.insert(
            "content_length".to_string(),
            content_length.map_or(Value::None, Value::from),
        );
        Ok(Value::Object(object))
    }

    /// Finds the unique watch for a source.
    pub async fn find_by_source(
        &self,
        space_id: &str,
        source_id: &RecordId,
    ) -> StorageResult<Option<SourceWatch>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<SourceWatch>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && row.source.id() == source_id
            });
            rows.sort_by_key(|row| row.base.id.to_string());
            return Ok(rows.into_iter().next());
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {WATCH_PROJECTION} FROM source_watch WHERE space_id = $space_id AND source = $source AND deleted_at IS NONE LIMIT 1"),
            crate::bindings! {
                "space_id" => space_id,
                "source" => crate::surreal::thing_from_record_id(source_id),
            },
        ).await?;
        let rows: Vec<SourceWatchDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse source watch: {error}"))
        })?;
        rows.into_iter()
            .next()
            .map(SourceWatch::try_from)
            .transpose()
    }

    /// Lists enabled or retryable watches that are due.
    pub async fn list_due(
        &self,
        space_id: &str,
        now: DateTime<Utc>,
        limit: usize,
    ) -> StorageResult<Vec<SourceWatch>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<SourceWatch>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && matches!(
                        row.status,
                        SourceWatchStatus::Enabled | SourceWatchStatus::Error
                    )
                    && row.next_check_at <= now
            });
            rows.sort_by_key(|row| (row.next_check_at, row.base.id.to_string()));
            rows.truncate(limit);
            return Ok(rows);
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {WATCH_PROJECTION} FROM source_watch WHERE space_id = $space_id AND status IN ['enabled', 'error'] AND next_check_at <= $now AND deleted_at IS NONE ORDER BY next_check_at, id LIMIT {limit}"),
            crate::bindings! { "space_id" => space_id, "now" => now },
        ).await?;
        let rows: Vec<SourceWatchDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse due source watches: {error}"))
        })?;
        rows.into_iter().map(SourceWatch::try_from).collect()
    }

    /// Atomically appends an immutable check and advances the mutable watch.
    pub async fn record_check(
        &self,
        watch: SourceWatch,
        check: SourceWatchCheck,
    ) -> StorageResult<(SourceWatch, SourceWatchCheck)> {
        self.record_check_mode(watch, check, None, None).await
    }

    /// Advance an observation only under its original watch and live execution.
    ///
    /// The exact guard is shared with source acceptance; a replaced attempt or
    /// edited watch cannot append a receipt or overwrite its successor's state.
    ///
    /// # Errors
    /// Refuses mismatched scope, changed original observations or a stale lease.
    pub async fn record_check_with_watch_producer(
        &self,
        watch: SourceWatch,
        check: SourceWatchCheck,
        producer: &crate::source_watch_producer::PreparedSourceWatchProducer,
    ) -> StorageResult<(SourceWatch, SourceWatchCheck)> {
        let original = producer.original_watch();
        if producer.watch_id() != &watch.base.id
            || producer.space_id() != watch.base.space_id
            || original.base != watch.base
            || original.source != watch.source
            || original.requested_uri != watch.requested_uri
            || original.interval_seconds != watch.interval_seconds
            || check.base.id.table() != TableName::SourceWatchCheck
            || check.base.security != original.base.security
            || check.base.subjects != original.base.subjects
            || check.base.subjects_state != original.base.subjects_state
            || check.base.provenance != original.base.provenance
            || check.requested_uri != original.requested_uri
        {
            return Err(StorageError::DataIntegrityError(
                "watch observation differs from original producer scope".into(),
            ));
        }
        self.record_check_mode(watch, check, Some(producer), None)
            .await
    }

    /// Record an observation of bytes already accepted as the desired input.
    ///
    /// The original Source/raw head and Watch are checked in this same write,
    /// together with the original live attempt when called by a worker.
    pub async fn record_check_with_input_observation(
        &self,
        watch: SourceWatch,
        check: SourceWatchCheck,
        producer: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
        observation: &PreparedSourceWatchObservation,
    ) -> StorageResult<(SourceWatch, SourceWatchCheck)> {
        let original = &observation.watch;
        if producer.is_some_and(|producer| {
            producer.watch_id() != &watch.base.id || producer.space_id() != watch.base.space_id
        }) || check.base.id.table() != TableName::SourceWatchCheck
            || watch.base != original.base
            || watch.source != original.source
            || watch.requested_uri != original.requested_uri
            || watch.interval_seconds != original.interval_seconds
            || check.outcome != SourceWatchOutcome::HashUnchanged
            || check.base.security != original.base.security
            || check.base.subjects != original.base.subjects
            || check.base.subjects_state != original.base.subjects_state
            || check.base.provenance != original.base.provenance
            || check.requested_uri != original.requested_uri
        {
            return Err(StorageError::DataIntegrityError(
                "watch input observation scope changed".into(),
            ));
        }
        self.record_check_mode(watch, check, producer, Some(observation))
            .await
    }

    async fn record_check_mode(
        &self,
        mut watch: SourceWatch,
        check: SourceWatchCheck,
        producer: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
        observation: Option<&PreparedSourceWatchObservation>,
    ) -> StorageResult<(SourceWatch, SourceWatchCheck)> {
        if check.watch.id() != &watch.base.id || check.source.id() != watch.source.id() {
            return Err(StorageError::DataIntegrityError(
                "source watch check links do not match the watch".to_string(),
            ));
        }
        if check.base.space_id != watch.base.space_id {
            return Err(StorageError::DataIntegrityError(
                "source watch check crosses spaces".to_string(),
            ));
        }
        let standing_records = if let Some(producer) = producer {
            let source = crate::SourceRepository::new(self.client.clone())
                .get(producer.space_id(), producer.original_watch().source.id())
                .await?
                .ok_or_else(|| StorageError::Conflict("standing_watch_source_missing".into()))?;
            let records =
                crate::source_watch_registration::PreparedSourceWatchRegistration::prepare(
                    &self.client,
                    &source,
                    Some(producer.original_watch()),
                    producer.original_watch().clone(),
                )
                .await?;
            records.authorize(producer.principal())?;
            Some(records)
        } else {
            None
        };
        watch.latest_check = Some(check.base.id.clone());
        watch.base.updated_at = check.completed_at;

        if let Some(store) = self.client.embedded_store() {
            let space_id = watch.base.space_id.clone();
            let watch_id = watch.base.id.clone();
            let watch_result = watch.clone();
            let check_result = check.clone();
            return store
                .mutate(|mutation| {
                    if let Some(producer) = producer {
                        producer.check_tx(mutation)?;
                    }
                    if let Some(observation) = observation {
                        observation.check_tx(mutation)?;
                    }
                    if let Some(records) = &standing_records {
                        records.check_tx(mutation)?;
                    }
                    mutation
                        .get::<SourceWatch>(&space_id, &watch_id, false)?
                        .ok_or_else(|| {
                            elephant_db::ElephantDbError::NotFound(watch_id.to_string())
                        })?;
                    mutation.create(check.clone())?;
                    mutation.replace(watch.clone())?;
                    Ok((watch_result.clone(), check_result.clone()))
                })
                .await;
        }

        let mut bindings = crate::bindings! {
                "check_id" => crate::surreal::thing_from_record_id(&check.base.id),
                "check" => Self::check_to_value(&check)?,
                "watch_id" => crate::surreal::thing_from_record_id(&watch.base.id),
                "watch" => Self::watch_to_value(&watch)?,
                "space_id" => watch.base.space_id.clone(),
        };
        let mut guard = String::new();
        if let Some(producer) = producer {
            producer.append_guard_sql(&mut guard, &mut bindings)?;
        }
        if let Some(observation) = observation {
            observation.append_guard_sql(&mut guard, &mut bindings)?;
        }
        if let Some(records) = &standing_records {
            records.append_guard_sql(&mut guard, &mut bindings)?;
        }
        self.client.query_bind_checked(
            &format!("BEGIN TRANSACTION; {guard} CREATE $check_id CONTENT $check; UPDATE $watch_id CONTENT $watch WHERE space_id = $space_id AND deleted_at IS NONE; COMMIT TRANSACTION;"),
            bindings,
        ).await.map_err(crate::source_publication::publication_error)?;
        Ok((watch, check))
    }

    /// Reads one immutable observation receipt under the current space boundary.
    pub async fn get_check(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<SourceWatchCheck>> {
        if id.table() != TableName::SourceWatchCheck {
            return Err(StorageError::TypeError(
                elephant_types::TypeError::InvalidRecordId(id.to_string()),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SourceWatchCheck>(space_id, id, false).await;
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {CHECK_PROJECTION} FROM $id WHERE space_id = $space_id AND deleted_at IS NONE"),
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
        ).await?;
        let rows: Vec<SourceWatchCheckDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse source watch receipt: {error}"))
        })?;
        rows.into_iter()
            .next()
            .map(SourceWatchCheck::try_from)
            .transpose()
    }

    /// Returns recent immutable receipts for a watch.
    pub async fn list_checks(
        &self,
        space_id: &str,
        watch_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<SourceWatchCheck>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rows = store.scan::<SourceWatchCheck>().await?;
            rows.retain(|row| {
                row.base.space_id == space_id
                    && row.base.deleted_at.is_none()
                    && row.watch.id() == watch_id
            });
            rows.sort_by_key(|row| (std::cmp::Reverse(row.completed_at), row.base.id.to_string()));
            rows.truncate(limit);
            return Ok(rows);
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {CHECK_PROJECTION} FROM source_watch_check WHERE space_id = $space_id AND watch = $watch AND deleted_at IS NONE ORDER BY completed_at DESC, id LIMIT {limit}"),
            crate::bindings! {
                "space_id" => space_id,
                "watch" => crate::surreal::thing_from_record_id(watch_id),
            },
        ).await?;
        let rows: Vec<SourceWatchCheckDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse source watch checks: {error}"))
        })?;
        rows.into_iter().map(SourceWatchCheck::try_from).collect()
    }
}

#[async_trait]
impl Repository<SourceWatch> for SourceWatchRepository {
    async fn create(&self, watch: SourceWatch) -> StorageResult<SourceWatch> {
        if watch.interval_seconds <= 0 {
            return Err(StorageError::DataIntegrityError(
                "source watch interval_seconds must be positive".to_string(),
            ));
        }
        if self
            .find_by_source(&watch.base.space_id, watch.source.id())
            .await?
            .is_some()
        {
            return Err(StorageError::Conflict(format!(
                "source watch already exists for {}",
                watch.source.id()
            )));
        }
        if let Some(store) = self.client.embedded_store() {
            return store.create(watch).await;
        }
        self.client
            .query_bind_checked(
                "CREATE $id CONTENT $record",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&watch.base.id),
                    "record" => Self::watch_to_value(&watch)?,
                },
            )
            .await?;
        Ok(watch)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<SourceWatch>> {
        if id.table() != TableName::SourceWatch {
            return Err(StorageError::TypeError(
                elephant_types::TypeError::InvalidRecordId(id.to_string()),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SourceWatch>(space_id, id, false).await;
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {WATCH_PROJECTION} FROM source_watch WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"),
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
        ).await?;
        let rows: Vec<SourceWatchDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .next()
            .map(SourceWatch::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut watch: SourceWatch,
    ) -> StorageResult<SourceWatch> {
        if watch.interval_seconds <= 0 {
            return Err(StorageError::DataIntegrityError(
                "source watch interval_seconds must be positive".to_string(),
            ));
        }
        watch.base.space_id = space_id.to_string();
        watch.base.id = id.clone();
        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, watch).await;
        }
        let mut response = self.client.query_bind_checked(
            &format!("UPDATE $id CONTENT $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {WATCH_PROJECTION}"),
            crate::bindings! {
                "id" => crate::surreal::thing_from_record_id(id),
                "record" => Self::watch_to_value(&watch)?,
                "space_id" => space_id,
            },
        ).await?;
        let rows: Vec<SourceWatchDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(id.to_string()))
            .and_then(SourceWatch::try_from)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<SourceWatch>(space_id, id).await;
        }
        self.client.query_bind_checked(
            "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
        ).await?;
        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SourceWatch>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<SourceWatch>(space_id, limit, cursor, false)
                .await;
        }
        let cursor_clause = if cursor.is_some() {
            "AND id > $cursor"
        } else {
            ""
        };
        let mut response = self.client.query_bind(
            &format!("SELECT {WATCH_PROJECTION} FROM source_watch WHERE space_id = $space_id AND deleted_at IS NONE {cursor_clause} ORDER BY id LIMIT {limit}"),
            crate::bindings! {
                "space_id" => space_id,
                "cursor" => cursor.map_or(Value::None, |id| crate::surreal::thing_from_record_id(id).into()),
            },
        ).await?;
        let rows: Vec<SourceWatchDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter().map(SourceWatch::try_from).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use elephant_types::records::base::BaseFields;

    fn test_watch(space_id: &str) -> SourceWatch {
        let now = Utc::now();
        SourceWatch {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SourceWatch),
                space_id.to_string(),
                "tester".to_string(),
            ),
            source: Link::new(RecordId::new_v4(TableName::Source)),
            requested_uri: "https://example.com/facts.txt".to_string(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: now,
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some("old-hash".to_string()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        }
    }

    fn test_check(watch: &SourceWatch) -> SourceWatchCheck {
        let now = Utc::now();
        let mut base = watch.base.clone();
        base.id = RecordId::new_v4(TableName::SourceWatchCheck);
        SourceWatchCheck {
            base,
            watch: Link::new(watch.base.id.clone()),
            source: watch.source.clone(),
            outcome: SourceWatchOutcome::NotModified,
            started_at: now,
            completed_at: now,
            requested_uri: watch.requested_uri.clone(),
            final_uri: Some(watch.requested_uri.clone()),
            content_type: None,
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            prior_hash: watch.observed_hash.clone(),
            observed_hash: watch.observed_hash.clone(),
            content_length: None,
            error: None,
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn exercise_repository(client: StorageClient) {
        let repository = SourceWatchRepository::new(client);
        let watch = repository
            .create(test_watch("watch-space"))
            .await
            .expect("create watch");
        let found = repository
            .find_by_source("watch-space", watch.source.id())
            .await
            .expect("find watch")
            .expect("watch exists");
        assert_eq!(found.base.id, watch.base.id);
        let due = repository
            .list_due("watch-space", Utc::now(), 10)
            .await
            .expect("list due");
        assert_eq!(due.len(), 1);

        let check = test_check(&watch);
        let (updated, persisted_check) = repository
            .record_check(watch, check)
            .await
            .expect("record check");
        assert_eq!(
            updated.latest_check.as_ref(),
            Some(&persisted_check.base.id)
        );
        let checks = repository
            .list_checks("watch-space", &updated.base.id, 10)
            .await
            .expect("list checks");
        assert_eq!(checks, vec![persisted_check]);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn embedded_watch_and_receipt_roundtrip() {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        Box::pin(exercise_repository(client)).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn surreal_watch_and_receipt_roundtrip() {
        let client = StorageClient::new_memory().await.expect("memory storage");
        client
            .query(include_str!(
                "../../migrations/sql/022_source_watches.surql"
            ))
            .await
            .expect("apply source-watch schema");
        Box::pin(exercise_repository(client)).await;
    }
}
