//! Assertion repository implementation for `SurrealDB`.
//!
//! Per spec §9.5, assertions are unresolved claims extracted from documents.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::TableName;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

const ASSERTION_PROJECTION: &str = r"
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
	subject_text,
	predicate_raw,
	predicate_id,
	object_text,
	object_literal,
    IF evidence IS NOT NONE THEN {
        revision: IF type::is::record(evidence.revision) THEN string::concat(meta::tb(evidence.revision), ':', meta::id(evidence.revision)) ELSE evidence.revision END,
        node: IF evidence.node IS NOT NONE THEN (IF type::is::record(evidence.node) THEN string::concat(meta::tb(evidence.node), ':', meta::id(evidence.node)) ELSE evidence.node END) ELSE NONE END,
        start: evidence.start,
        end: evidence.end,
        quote_hash: evidence.quote_hash
    } ELSE NONE END AS evidence,
    source_event_time,
    observed_at,
    asserted_at,
    assertion_key,
    resolution_status,
    resolution_method,
    resolution_confidence,
    retry_state,
    IF subject_entity IS NONE THEN NONE ELSE IF type::is::record(subject_entity) THEN string::concat(meta::tb(subject_entity), ':', meta::id(subject_entity)) ELSE subject_entity END AS subject_entity,
    IF object_entity IS NONE THEN NONE ELSE IF type::is::record(object_entity) THEN string::concat(meta::tb(object_entity), ':', meta::id(object_entity)) ELSE object_entity END AS object_entity,
    confidence,
    subject_entity_type,
    object_entity_type,
    significance,
    lifecycle_tier
";

/// Database-only proxy for Assertion to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct AssertionDb {
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
    subject_text: Option<String>,
    predicate_raw: Option<String>,
    predicate_id: Option<String>,
    object_text: Option<String>,
    object_literal: Option<ObjectLiteralDb>,
    evidence: Option<EvidenceSpan>,
    source_event_time: Option<chrono::DateTime<Utc>>,
    observed_at: Option<chrono::DateTime<Utc>>,
    asserted_at: Option<chrono::DateTime<Utc>>,
    assertion_key: Option<String>,
    resolution_status: Option<ResolutionStatus>,
    resolution_method: Option<String>,
    resolution_confidence: Option<f32>,
    retry_state: Option<AssertionRetryState>,
    subject_entity: Option<Link<Entity>>,
    object_entity: Option<Link<Entity>>,
    confidence: Option<f32>,
    subject_entity_type: Option<String>,
    object_entity_type: Option<String>,
    significance: Option<String>,
    lifecycle_tier: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ObjectLiteralDb {
    Surreal(surrealdb::sql::Value),
    Json(serde_json::Value),
}

fn is_reserved_object_literal_wrapper_shape(value: &serde_json::Value) -> bool {
    let serde_json::Value::Object(obj) = value else {
        return false;
    };

    if obj.len() != 2 {
        return false;
    }

    let Some(serde_json::Value::String(kind)) = obj.get("type") else {
        return false;
    };
    if !obj.contains_key("value") {
        return false;
    }

    matches!(
        kind.as_str(),
        "integer" | "float" | "boolean" | "string" | "json"
    )
}

fn unwrap_object_literal(value: serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(obj) = &value else {
        return value;
    };

    if obj.len() != 2 {
        return value;
    }

    let Some(serde_json::Value::String(kind)) = obj.get("type") else {
        return value;
    };
    let Some(inner) = obj.get("value") else {
        return value;
    };

    match kind.as_str() {
        "integer" | "float" | "boolean" | "string" | "json" => inner.clone(),
        _ => value,
    }
}

fn lexical_query_tokens(query: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut tokens = Vec::new();
    for token in elephant_db::tokenize(query) {
        if seen.insert(token.clone()) {
            tokens.push(token);
        }
    }
    tokens
}

fn ranked_search_offset(cursor: Option<&str>) -> StorageResult<usize> {
    cursor.map_or(Ok(0), |value| {
        value.parse::<usize>().map_err(|error| {
            StorageError::QueryError(format!("Invalid ranked search cursor `{value}`: {error}"))
        })
    })
}

impl TryFrom<AssertionDb> for Assertion {
    type Error = StorageError;

    fn try_from(db: AssertionDb) -> Result<Self, Self::Error> {
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
            subject_text: db
                .subject_text
                .ok_or_else(|| StorageError::QueryError("Missing subject_text".to_string()))?,
            predicate_raw: db
                .predicate_raw
                .ok_or_else(|| StorageError::QueryError("Missing predicate_raw".to_string()))?,
            predicate_id: db.predicate_id,
            object_text: db.object_text,
            object_literal: db
                .object_literal
                .map(|value| match value {
                    ObjectLiteralDb::Json(json) => Ok(json),
                    ObjectLiteralDb::Surreal(val) => crate::surreal::surreal_to_json(val),
                })
                .transpose()
                .map_err(|e| StorageError::QueryError(e.to_string()))?
                .map(unwrap_object_literal),
            evidence: db
                .evidence
                .ok_or_else(|| StorageError::QueryError("Missing evidence".to_string()))?,
            source_event_time: db.source_event_time,
            observed_at: db
                .observed_at
                .ok_or_else(|| StorageError::QueryError("Missing observed_at".to_string()))?,
            asserted_at: db
                .asserted_at
                .ok_or_else(|| StorageError::QueryError("Missing asserted_at".to_string()))?,
            assertion_key: db
                .assertion_key
                .ok_or_else(|| StorageError::QueryError("Missing assertion_key".to_string()))?,
            resolution_status: db
                .resolution_status
                .ok_or_else(|| StorageError::QueryError("Missing resolution_status".to_string()))?,
            resolution_method: db.resolution_method,
            resolution_confidence: db.resolution_confidence,
            retry_state: db
                .retry_state
                .ok_or_else(|| StorageError::QueryError("Missing retry_state".to_string()))?,
            subject_entity: db.subject_entity,
            object_entity: db.object_entity,
            confidence: db
                .confidence
                .ok_or_else(|| StorageError::QueryError("Missing confidence".to_string()))?,
            subject_entity_type: db.subject_entity_type,
            object_entity_type: db.object_entity_type,
            significance: db.significance,
            lifecycle_tier: db.lifecycle_tier.unwrap_or_else(|| "active".to_string()),
        })
    }
}

/// Assertion repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct AssertionRepository {
    client: StorageClient,
}

impl AssertionRepository {
    /// Creates a new assertion repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Gets an assertion including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Assertion>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;
        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertion: {e}")))?;
        items
            .into_iter()
            .next()
            .map(Assertion::try_from)
            .transpose()
    }

    /// Restores a soft-deleted assertion.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or the assertion is not found in
    /// the space.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Assertion> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<Assertion>(space_id, id).await;
        }

        // Returns only rows it updated, so a foreign-space id cannot surface
        // as a successful restore.
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {ASSERTION_PROJECTION}"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;
        let items: Vec<AssertionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored assertion: {e}"))
        })?;
        let item = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Assertion not found in space: {id}")))?;
        Assertion::try_from(item)
    }

    /// Converts an Assertion to a SurrealDB value for insertion.
    #[allow(clippy::too_many_lines)]
    fn assertion_to_value(item: &Assertion) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::{Object, Value};

        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("subject_text".to_string(), item.subject_text.clone().into());
        obj.insert(
            "predicate_raw".to_string(),
            item.predicate_raw.clone().into(),
        );
        obj.insert(
            "predicate_id".to_string(),
            item.predicate_id.clone().map_or(Value::None, Value::from),
        );
        obj.insert(
            "object_text".to_string(),
            item.object_text.clone().map_or(Value::None, Value::from),
        );
        obj.insert(
            "object_literal".to_string(),
            match &item.object_literal {
                None | Some(serde_json::Value::Null) => Value::None,
                Some(value @ serde_json::Value::Object(_)) => {
                    if is_reserved_object_literal_wrapper_shape(value) {
                        return Err(StorageError::SerializationError(
                            "Assertion.object_literal object shape {type,value} is reserved; supply a JSON literal (number/bool/string/array) instead".to_string(),
                        ));
                    }
                    crate::surreal::json_to_surreal(value.clone())?
                }
                Some(value @ serde_json::Value::Number(n)) => {
                    let kind = if n.is_i64() || n.is_u64() {
                        "integer"
                    } else {
                        "float"
                    };
                    crate::surreal::json_to_surreal(serde_json::json!({ "type": kind, "value": value }))?
                }
                Some(value @ serde_json::Value::Bool(_)) => {
                    crate::surreal::json_to_surreal(serde_json::json!({ "type": "boolean", "value": value }))?
                }
                Some(value @ serde_json::Value::String(_)) => {
                    crate::surreal::json_to_surreal(serde_json::json!({ "type": "string", "value": value }))?
                }
                Some(value @ serde_json::Value::Array(_)) => {
                    crate::surreal::json_to_surreal(serde_json::json!({ "type": "json", "value": value }))?
                }
            },
        );
        let evidence_json = serde_json::to_value(&item.evidence).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize evidence: {e}"))
        })?;
        obj.insert(
            "evidence".to_string(),
            crate::surreal::json_to_surreal(evidence_json)?,
        );
        obj.insert(
            "source_event_time".to_string(),
            item.source_event_time.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        obj.insert(
            "observed_at".to_string(),
            crate::surreal::datetime_from_chrono(item.observed_at).into(),
        );
        obj.insert(
            "asserted_at".to_string(),
            crate::surreal::datetime_from_chrono(item.asserted_at).into(),
        );
        obj.insert(
            "assertion_key".to_string(),
            item.assertion_key.clone().into(),
        );
        obj.insert(
            "resolution_status".to_string(),
            crate::surreal::enum_to_surreal(&item.resolution_status)?,
        );
        obj.insert(
            "resolution_method".to_string(),
            item.resolution_method
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "resolution_confidence".to_string(),
            item.resolution_confidence.map_or(Value::None, Value::from),
        );
        let mut retry_map = std::collections::BTreeMap::new();
        retry_map.insert(
            "resolution_attempts".to_string(),
            item.retry_state.resolution_attempts.into(),
        );
        retry_map.insert(
            "last_resolution_at".to_string(),
            item.retry_state
                .last_resolution_at
                .map_or(Value::None, |dt| {
                    Value::from(crate::surreal::datetime_from_chrono(dt))
                }),
        );
        retry_map.insert(
            "abstention_reason".to_string(),
            item.retry_state
                .abstention_reason
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "retry_state".to_string(),
            Value::Object(Object::from(retry_map)),
        );
        obj.insert(
            "subject_entity".to_string(),
            item.subject_entity.as_ref().map_or(Value::None, |link| {
                Value::from(crate::surreal::thing_from_record_id(link.id()))
            }),
        );
        obj.insert(
            "object_entity".to_string(),
            item.object_entity.as_ref().map_or(Value::None, |link| {
                Value::from(crate::surreal::thing_from_record_id(link.id()))
            }),
        );
        obj.insert("confidence".to_string(), item.confidence.into());
        obj.insert(
            "subject_entity_type".to_string(),
            item.subject_entity_type
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "object_entity_type".to_string(),
            item.object_entity_type
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "significance".to_string(),
            item.significance.clone().map_or(Value::None, Value::from),
        );
        obj.insert(
            "lifecycle_tier".to_string(),
            item.lifecycle_tier.clone().into(),
        );

        Ok(Value::Object(obj))
    }
}

#[async_trait]
impl Repository<Assertion> for AssertionRepository {
    async fn create(&self, record: Assertion) -> StorageResult<Assertion> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::assertion_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {ASSERTION_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created assertion: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map_or_else(|| Ok(record), Assertion::try_from)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Assertion>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertion: {e}")))?;

        items
            .into_iter()
            .next()
            .map(Assertion::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Assertion,
    ) -> StorageResult<Assertion> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .update_existing(&target_space_id, &target_id, normalized)
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ASSERTION_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::assertion_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated assertion: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Assertion not found in space: {id}")))?;

        Assertion::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Assertion>(space_id, id).await;
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
    ) -> StorageResult<Vec<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<Assertion>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {ASSERTION_PROJECTION} FROM assertion WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {ASSERTION_PROJECTION} FROM assertion WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl SearchRepository<Assertion> for AssertionRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            let Some(space_id) = &criteria.space_id else {
                return Ok(PaginatedResult::empty());
            };

            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| item.base.space_id == *space_id)
                .filter(|item| criteria.include_deleted || item.base.deleted_at.is_none())
                .filter_map(|item| {
                    let object_text = item.object_text.as_deref().unwrap_or_default();
                    let score = criteria.query.as_ref().map_or(1.0, |query| {
                        crate::embedded::lexical_score(
                            query,
                            &[
                                (item.subject_text.as_str(), 1.0),
                                (item.predicate_raw.as_str(), 0.8),
                                (object_text, 1.0),
                            ],
                        )
                    });
                    (score > 0.0).then_some((item, score))
                })
                .collect();

            items.sort_by(|left, right| {
                right
                    .1
                    .partial_cmp(&left.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.0.base.id.to_string().cmp(&right.0.base.id.to_string()))
            });

            let total = items.len();
            if criteria.query.is_some() {
                let offset = ranked_search_offset(criteria.cursor.as_deref())?;
                let has_more = items.len() > offset.saturating_add(criteria.limit);
                let page: Vec<_> = items
                    .into_iter()
                    .skip(offset)
                    .take(criteria.limit)
                    .map(|(item, _)| item)
                    .collect();
                let next_cursor = has_more.then(|| (offset + criteria.limit).to_string());

                return Ok(PaginatedResult::new(page, next_cursor, has_more, total));
            }

            let cursor_id = criteria
                .cursor
                .as_ref()
                .map(|cursor| {
                    crate::parse_id(cursor, TableName::Assertion)
                        .map_err(|e| StorageError::QueryError(e.to_string()))
                })
                .transpose()?;

            let total = items.len();
            let filtered: Vec<_> = items
                .into_iter()
                .filter(|(item, _)| {
                    cursor_id
                        .as_ref()
                        .is_none_or(|cursor| item.base.id.to_string() > cursor.to_string())
                })
                .collect();

            let has_more = filtered.len() > criteria.limit;
            let mut page = filtered;
            if page.len() > criteria.limit {
                page.truncate(criteria.limit);
            }
            let next_cursor = if has_more {
                page.last().map(|(item, _)| {
                    let id_str = item.base.id.to_string();
                    id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
                })
            } else {
                None
            };

            return Ok(PaginatedResult::new(
                page.into_iter().map(|(item, _)| item).collect(),
                next_cursor,
                has_more,
                total,
            ));
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

        let where_prefix = if conditions.is_empty() {
            String::new()
        } else {
            format!("{} AND ", conditions.join(" AND "))
        };

        if let Some(ref query) = criteria.query {
            let tokens = lexical_query_tokens(query);
            if tokens.is_empty() {
                return Ok(PaginatedResult::empty());
            }

            for (index, token) in tokens.iter().enumerate() {
                bindings.insert(
                    format!("query_{index}"),
                    surrealdb::sql::Value::from(token.as_str()),
                );
            }

            let mut predicate_parts = Vec::with_capacity(tokens.len());
            let mut score_parts = Vec::with_capacity(tokens.len() * 3);
            for (index, _) in tokens.iter().enumerate() {
                let subject_slot = index * 3;
                let predicate_slot = subject_slot + 1;
                let object_slot = subject_slot + 2;
                let binding = format!("$query_{index}");
                predicate_parts.push(format!(
                    "(subject_text @{subject_slot}@ {binding} OR predicate_raw @{predicate_slot}@ {binding} OR object_text @{object_slot}@ {binding})"
                ));
                score_parts.push(format!("search::score({subject_slot}) * 1.0"));
                score_parts.push(format!("search::score({predicate_slot}) * 0.8"));
                score_parts.push(format!("search::score({object_slot}) * 1.0"));
            }

            let search_predicate = predicate_parts.join(" OR ");
            let score_expression = score_parts.join(" + ");
            // Parenthesised: AND binds tighter than OR, so without them every
            // term after the first escaped the space and deleted filters.
            let where_clause = format!("WHERE {where_prefix}({search_predicate})");
            let offset = ranked_search_offset(criteria.cursor.as_deref())?;

            let query = format!(
                "SELECT {ASSERTION_PROJECTION}, ({score_expression}) AS lexical_score \
                 FROM assertion {where_clause} \
                 ORDER BY lexical_score DESC, id ASC START {offset} LIMIT {}",
                criteria.limit + 1
            );

            let mut response = self.client.query_bind(&query, bindings.clone()).await?;
            let assertions_db: Vec<AssertionDb> = response.take(0).map_err(|e| {
                StorageError::QueryError(format!("Failed to parse assertions: {e}"))
            })?;

            let mut assertions: Vec<Assertion> = assertions_db
                .into_iter()
                .map(Assertion::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            let has_more = criteria.limit > 0 && assertions.len() > criteria.limit;
            if has_more {
                assertions.pop();
            } else if criteria.limit == 0 {
                assertions.clear();
            }

            let next_cursor = has_more.then(|| (offset + criteria.limit).to_string());

            let count_query =
                format!("SELECT count() as total FROM assertion {where_clause} GROUP ALL");
            let mut count_response = self.client.query_bind(&count_query, bindings).await?;
            let counts: Vec<CountResult> = crate::response::take_first(
                &mut count_response,
                "Failed to parse assertion count",
            )?;
            let total = counts.first().map_or(0, |count| count.total);

            return Ok(PaginatedResult::new(
                assertions,
                next_cursor,
                has_more,
                total,
            ));
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::Assertion)
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
            "SELECT {ASSERTION_PROJECTION} FROM assertion {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;
        let assertions_db: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;
        let mut assertions: Vec<Assertion> = assertions_db
            .into_iter()
            .map(Assertion::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = criteria.limit > 0 && assertions.len() > criteria.limit;
        if has_more {
            assertions.pop();
        } else if criteria.limit == 0 {
            assertions.clear();
        }
        let next_cursor = if has_more {
            assertions.last().map(|item| {
                let id_str = item.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM assertion {where_clause} GROUP ALL");
        let mut count_response = self.client.query_bind(&count_query, bindings).await?;
        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse assertion count")?;
        let total = counts.first().map_or(0, |count| count.total);

        Ok(PaginatedResult::new(
            assertions,
            next_cursor,
            has_more,
            total,
        ))
    }
}

/// Additional assertion-specific operations.
impl AssertionRepository {
    /// Updates an assertion only if it still has the expected resolution status.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn update_if_resolution_status(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Assertion,
        expected_status: ResolutionStatus,
    ) -> StorageResult<Option<Assertion>> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(existing) =
                        mutation.get::<Assertion>(&target_space_id, &target_id, false)?
                    else {
                        return Ok(None);
                    };
                    if existing.resolution_status != expected_status {
                        return Ok(None);
                    }

                    let duplicate = mutation.scan::<Assertion>()?.into_iter().any(|item| {
                        item.base.space_id == target_space_id
                            && item.assertion_key == normalized.assertion_key
                            && item.base.id != target_id
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate assertion assertion_key in space {target_space_id}"
                        )));
                    }

                    Ok(Some(mutation.replace(normalized.clone())?))
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind(
                "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE AND resolution_status = $expected_status RETURN AFTER",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "expected_status" => expected_status.to_string(),
                    "record" => Self::assertion_to_value(&normalized)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to update assertion: {e}"))
            })?;

        let items: Vec<AssertionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated assertion: {e}"))
        })?;
        let Some(item_db) = items.into_iter().next() else {
            return Ok(None);
        };

        Ok(Some(Assertion::try_from(item_db)?))
    }

    /// Finds abstained assertions ready for retry.
    ///
    /// Returns assertions with `resolution_status = Abstained` that haven't exceeded
    /// the maximum number of attempts.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_abstained_for_retry(
        &self,
        space_id: &str,
        max_attempts: u32,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.resolution_status == ResolutionStatus::Abstained
                        && item.retry_state.resolution_attempts < max_attempts
                })
                .collect();
            items.sort_by_key(|item| item.base.created_at);
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let query = format!(
            "SELECT {ASSERTION_PROJECTION} FROM assertion \
             WHERE space_id = $space_id \
             AND resolution_status = 'abstained' \
             AND deleted_at IS NONE \
             AND (retry_state.resolution_attempts IS NONE OR retry_state.resolution_attempts < $max_attempts) \
             ORDER BY created_at \
             LIMIT {limit}"
        );

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "max_attempts" => max_attempts
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
    }

    /// Finds assertions by assertion key.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_assertion_key(
        &self,
        space_id: &str,
        assertion_key: &str,
    ) -> StorageResult<Option<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            return store.assertion_by_key(space_id, assertion_key).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion WHERE space_id = $space_id AND assertion_key = $key AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "key" => assertion_key },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertion: {e}")))?;

        items
            .into_iter()
            .next()
            .map(Assertion::try_from)
            .transpose()
    }

    /// Archives old resolved assertions by setting `lifecycle_tier = "archived"`.
    ///
    /// Targets assertions where `resolution_status = "resolved"` and
    /// `created_at` is older than `older_than_days` days ago.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn archive_old_resolved(
        &self,
        space_id: &str,
        older_than_days: i64,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(older_than_days);
            return store
                .mutate(|mutation| {
                    let mut count = 0usize;
                    for mut item in mutation.scan::<Assertion>()? {
                        if item.base.space_id != space_id
                            || item.base.deleted_at.is_some()
                            || item.resolution_status != ResolutionStatus::Resolved
                            || item.lifecycle_tier != "active"
                            || item.base.created_at >= cutoff
                        {
                            continue;
                        }

                        item.lifecycle_tier = "archived".to_string();
                        item.base.updated_at = Utc::now();
                        let _ = mutation.replace(item)?;
                        count += 1;
                    }
                    Ok(count)
                })
                .await;
        }

        let cutoff = chrono::Utc::now() - chrono::Duration::days(older_than_days);
        let cutoff_surreal = crate::surreal::datetime_from_chrono(cutoff);

        // Archive and return the affected ids so the count reflects the rows
        // actually updated (no separate, racy pre-count).
        let mut response = self
            .client
            .query_bind_checked(
                "UPDATE assertion SET lifecycle_tier = 'archived', updated_at = time::now() \
                 WHERE space_id = $space_id \
                 AND resolution_status = 'resolved' \
                 AND lifecycle_tier = 'active' \
                 AND created_at < $cutoff \
                 AND deleted_at IS NONE \
                 RETURN VALUE id",
                crate::bindings! {
                    "space_id" => space_id,
                    "cutoff" => cutoff_surreal
                },
            )
            .await?;

        let archived_ids: Vec<surrealdb::sql::Thing> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse archived assertion ids: {e}"))
        })?;

        Ok(archived_ids.len())
    }

    /// Finds assertions by document revision ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.evidence.revision.id() == revision_id
                })
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion \
                     WHERE space_id = $space_id \
                     AND deleted_at IS NONE \
                     AND evidence.revision = $revision_id \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "revision_id" => revision_id.to_string()
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
    }

    /// Finds assertions where a given entity appears in the subjects array.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[allow(clippy::similar_names)]
    pub async fn find_by_subject_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item
                            .base
                            .subjects
                            .iter()
                            .any(|subject| subject.id() == entity_id)
                })
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion \
                     WHERE space_id = $space_id \
                     AND deleted_at IS NONE \
                     AND subjects CONTAINS $entity_id \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity_id" => crate::surreal::thing_from_record_id(entity_id)
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
    }

    /// Finds assertions where a resolved entity appears as subject or object.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_resolved_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && (item
                            .subject_entity
                            .as_ref()
                            .is_some_and(|link| link.id() == entity_id)
                            || item
                                .object_entity
                                .as_ref()
                                .is_some_and(|link| link.id() == entity_id))
                })
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion \
                     WHERE space_id = $space_id \
                     AND deleted_at IS NONE \
                     AND (subject_entity = $entity_id OR object_entity = $entity_id) \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity_id" => crate::surreal::thing_from_record_id(entity_id)
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
    }

    /// Finds assertions where resolved subject and/or object entity filters match.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    #[allow(clippy::similar_names)]
    pub async fn find_by_resolved_entity_pair(
        &self,
        space_id: &str,
        subject_entity: Option<&RecordId>,
        object_entity: Option<&RecordId>,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        if subject_entity.is_none() && object_entity.is_none() {
            return Ok(Vec::new());
        }
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Assertion>()
                .await?
                .into_iter()
                .filter(|item| {
                    if item.base.space_id != space_id || item.base.deleted_at.is_some() {
                        return false;
                    }
                    if let Some(entity_id) = subject_entity {
                        if item
                            .subject_entity
                            .as_ref()
                            .is_none_or(|link| link.id() != entity_id)
                        {
                            return false;
                        }
                    }
                    if let Some(entity_id) = object_entity {
                        if item
                            .object_entity
                            .as_ref()
                            .is_none_or(|link| link.id() != entity_id)
                        {
                            return false;
                        }
                    }
                    true
                })
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut clauses = vec!["space_id = $space_id", "deleted_at IS NONE"];
        if subject_entity.is_some() {
            clauses.push("subject_entity = $subject_entity");
        }
        if object_entity.is_some() {
            clauses.push("object_entity = $object_entity");
        }
        let where_clause = clauses.join(" AND ");
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ASSERTION_PROJECTION} FROM assertion \
                     WHERE {where_clause} \
                     ORDER BY created_at DESC \
                     LIMIT {limit}"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_entity" => subject_entity.map_or(surrealdb::sql::Value::None, |id| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id))
                    }),
                    "object_entity" => object_entity.map_or(surrealdb::sql::Value::None, |id| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id))
                    })
                },
            )
            .await?;

        let items: Vec<AssertionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse assertions: {e}")))?;

        items.into_iter().map(Assertion::try_from).collect()
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
    use elephant_types::records::doc_revision::DocRevision;
    use elephant_types::TableName;

    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80300);

    const ASSERTION_SCHEMA: &str = r"
        DEFINE TABLE assertion SCHEMALESS;
        DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
        DEFINE INDEX assertion_subject_text_search ON TABLE assertion FIELDS subject_text SEARCH ANALYZER simple BM25;
        DEFINE INDEX assertion_predicate_raw_search ON TABLE assertion FIELDS predicate_raw SEARCH ANALYZER simple BM25;
        DEFINE INDEX assertion_object_text_search ON TABLE assertion FIELDS object_text SEARCH ANALYZER simple BM25;
    ";

    async fn setup_test_repo() -> AssertionRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("assertion_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(ASSERTION_SCHEMA).await.unwrap();

        AssertionRepository::new(client)
    }

    async fn setup_embedded_repo() -> AssertionRepository {
        let client = crate::StorageClient::new_embedded().await.unwrap();
        AssertionRepository::new(client)
    }

    fn create_test_assertion() -> Assertion {
        let id = RecordId::new_v4(TableName::Assertion);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        Assertion {
            base,
            subject_text: "John Doe".to_string(),
            predicate_raw: "works_at".to_string(),
            predicate_id: None,
            object_text: Some("Acme Corp".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id),
                node: None,
                start: 0,
                end: 50,
                quote_hash: "hash123".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
            resolution_status: ResolutionStatus::Pending,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.9,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        }
    }

    fn create_test_assertion_with_id(
        id: &str,
        subject_text: &str,
        predicate_raw: &str,
        object_text: Option<&str>,
    ) -> Assertion {
        let mut item = create_test_assertion();
        item.base.id = RecordId::from_str(id).expect("valid test record id");
        item.subject_text = subject_text.to_string();
        item.predicate_raw = predicate_raw.to_string();
        item.object_text = object_text.map(str::to_string);
        item
    }

    #[tokio::test]
    async fn test_create_assertion() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.subject_text, "John Doe");
        assert_eq!(created.predicate_raw, "works_at");
    }

    #[tokio::test]
    async fn test_get_assertion() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().predicate_raw, "works_at");
    }

    #[tokio::test]
    async fn test_delete_and_restore_assertion() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();
        let id = item.base.id.clone();
        repo.create(item).await.unwrap();

        repo.delete("test_space", &id).await.unwrap();
        assert!(repo.get("test_space", &id).await.unwrap().is_none());
        assert!(repo
            .get_with_deleted("test_space", &id)
            .await
            .unwrap()
            .is_some_and(|assertion| assertion.base.deleted_at.is_some()));
        assert!(repo.restore("other_space", &id).await.is_err());

        let restored = repo.restore("test_space", &id).await.unwrap();
        assert!(restored.base.deleted_at.is_none());
        assert!(repo.get("test_space", &id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_ranked_search_scopes_every_query_term() {
        let repo = setup_test_repo().await;

        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000001",
            "Alice",
            "met_in",
            Some("Paris"),
        ))
        .await
        .unwrap();
        let mut other_space = create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000002",
            "Mallory",
            "met_in",
            Some("Berlin"),
        );
        other_space.base.space_id = "other_space".to_string();
        repo.create(other_space).await.unwrap();
        let deleted_id = RecordId::from_str("assertion:00000000-0000-0000-0000-000000000003")
            .expect("valid test record id");
        repo.create(create_test_assertion_with_id(
            &deleted_id.to_string(),
            "Mallory",
            "met_in",
            Some("Rome"),
        ))
        .await
        .unwrap();
        repo.delete("test_space", &deleted_id).await.unwrap();

        // The space and deleted filters must hold for the second term too.
        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("alice mallory")
                    .with_limit(10),
            )
            .await
            .unwrap();

        let subjects: Vec<&str> = result
            .results
            .iter()
            .map(|assertion| assertion.subject_text.as_str())
            .collect();
        assert_eq!(subjects, vec!["Alice"]);
        assert_eq!(result.total, 1);
    }

    #[tokio::test]
    async fn test_ranked_search_cursor_uses_offset_pagination() {
        let repo = setup_test_repo().await;

        repo.create(create_test_assertion_with_id(
            "assertion:ffffffff-ffff-ffff-ffff-ffffffffffff",
            "Alice",
            "met_in",
            Some("Wonderland white rabbit"),
        ))
        .await
        .unwrap();
        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000001",
            "White Rabbit",
            "appears_in",
            Some("Wonderland"),
        ))
        .await
        .unwrap();
        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000002",
            "Alice",
            "saw",
            Some("rabbit"),
        ))
        .await
        .unwrap();

        let full = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("white rabbit wonderland")
                    .with_limit(10),
            )
            .await
            .unwrap();
        assert_eq!(full.results.len(), 3);

        let page_one = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("white rabbit wonderland")
                    .with_limit(1),
            )
            .await
            .unwrap();
        assert_eq!(page_one.results.len(), 1);
        assert_eq!(page_one.results[0].base.id, full.results[0].base.id);
        assert_eq!(page_one.cursor.as_deref(), Some("1"));

        let page_two = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("white rabbit wonderland")
                    .with_limit(1)
                    .with_cursor("1"),
            )
            .await
            .unwrap();
        assert_eq!(page_two.results.len(), 1);
        assert_eq!(page_two.results[0].base.id, full.results[1].base.id);
        assert_eq!(page_two.cursor.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn test_embedded_non_ranked_cursor_total_is_stable_across_cursor_pages() {
        let repo = setup_embedded_repo().await;

        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000001",
            "Alice",
            "likes",
            Some("Tea"),
        ))
        .await
        .unwrap();
        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000002",
            "Rabbit",
            "likes",
            Some("Watch"),
        ))
        .await
        .unwrap();
        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000003",
            "Queen",
            "likes",
            Some("Croquet"),
        ))
        .await
        .unwrap();

        let first_page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_limit(10),
            )
            .await
            .unwrap();
        assert_eq!(first_page.total, 3);

        let page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_limit(10)
                    .with_cursor("00000000-0000-0000-0000-000000000001"),
            )
            .await
            .unwrap();

        assert_eq!(page.results.len(), 2);
        assert_eq!(page.total, 3);
    }

    #[tokio::test]
    async fn test_ranked_search_zero_limit_has_no_cursor() {
        let repo = setup_test_repo().await;

        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000011",
            "Alice",
            "saw",
            Some("White Rabbit"),
        ))
        .await
        .unwrap();
        repo.create(create_test_assertion_with_id(
            "assertion:00000000-0000-0000-0000-000000000012",
            "White Rabbit",
            "appears_in",
            Some("Wonderland"),
        ))
        .await
        .unwrap();

        let page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("white rabbit")
                    .with_limit(0),
            )
            .await
            .unwrap();

        assert!(page.results.is_empty());
        assert!(!page.has_more);
        assert!(page.cursor.is_none());
        assert_eq!(page.total, 2);
    }

    #[tokio::test]
    async fn test_assertion_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_assertion();
        item.confidence = 0.85;
        item.object_literal = Some(serde_json::json!(42));
        item.object_text = None;

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.subject_text, "John Doe");
        assert!((created.confidence - 0.85).abs() < f32::EPSILON);
        assert_eq!(created.object_literal, Some(serde_json::json!(42)));
        assert!(created.object_text.is_none());
    }

    #[tokio::test]
    async fn test_update_assertion() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.resolution_status = ResolutionStatus::Resolved;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.resolution_status, ResolutionStatus::Resolved);
    }

    #[tokio::test]
    async fn test_update_if_resolution_status_returns_none_when_status_already_changed() {
        let repo = setup_test_repo().await;
        let created = repo.create(create_test_assertion()).await.unwrap();
        let id = created.base.id.clone();

        let mut resolved = created.clone();
        resolved.resolution_status = ResolutionStatus::Resolved;
        let _ = repo
            .update_if_resolution_status(
                "test_space",
                &id,
                resolved.clone(),
                ResolutionStatus::Pending,
            )
            .await
            .expect("first compare-and-swap update should succeed");

        let mut competing = resolved.clone();
        competing.resolution_method = Some("competing_patch".to_string());
        competing.resolution_confidence = Some(0.42);

        let result = repo
            .update_if_resolution_status("test_space", &id, competing, ResolutionStatus::Pending)
            .await
            .expect("second compare-and-swap update should not error");

        assert!(
            result.is_none(),
            "stale expected status must not report success"
        );
    }

    #[tokio::test]
    async fn test_embedded_create_rejects_duplicate_assertion_key() {
        let repo = setup_embedded_repo().await;
        let first = create_test_assertion();
        let mut second = create_test_assertion();
        second.assertion_key = first.assertion_key.clone();

        repo.create(first).await.unwrap();
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate assertion_key should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate assertion assertion_key"))
        );
    }

    #[tokio::test]
    async fn test_embedded_update_rejects_duplicate_assertion_key() {
        let repo = setup_embedded_repo().await;
        let first = repo.create(create_test_assertion()).await.unwrap();
        let second = repo.create(create_test_assertion()).await.unwrap();

        let mut updated = second.clone();
        updated.assertion_key = first.assertion_key.clone();

        let err = repo
            .update("test_space", &second.base.id, updated)
            .await
            .expect_err("duplicate assertion_key should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate assertion assertion_key"))
        );
    }

    #[tokio::test]
    async fn test_delete_assertion() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_find_by_assertion_key() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();
        let key = item.assertion_key.clone();

        repo.create(item).await.unwrap();

        let found = repo
            .find_by_assertion_key("test_space", &key)
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().assertion_key, key);
    }

    #[tokio::test]
    async fn test_assertion_lifecycle_defaults_to_active() {
        let repo = setup_test_repo().await;
        let item = create_test_assertion();

        let created = repo.create(item).await.unwrap();
        assert_eq!(created.lifecycle_tier, "active");
    }

    #[tokio::test]
    async fn test_archive_old_resolved_assertions() {
        let repo = setup_test_repo().await;

        // Create a resolved assertion with old created_at
        let mut item = create_test_assertion();
        item.resolution_status = ResolutionStatus::Resolved;
        let id = item.base.id.clone();
        let created = repo.create(item).await.unwrap();
        assert_eq!(created.lifecycle_tier, "active");

        // Archiving with 0 days threshold should archive it (it was just created
        // but the cutoff is "now", so created_at < now might be true since there's
        // a tiny time difference). Use -1 to ensure the cutoff is in the future.
        let archived = repo.archive_old_resolved("test_space", -1).await.unwrap();
        assert_eq!(archived, 1, "should archive one resolved assertion");

        // Verify the assertion is now archived
        let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(retrieved.lifecycle_tier, "archived");

        // Calling archive again should not re-archive (already archived)
        let archived_again = repo.archive_old_resolved("test_space", -1).await.unwrap();
        assert_eq!(
            archived_again, 0,
            "already archived, should not archive again"
        );
    }

    #[tokio::test]
    async fn test_archive_skips_pending_assertions() {
        let repo = setup_test_repo().await;

        // Create a pending assertion (not resolved)
        let item = create_test_assertion();
        assert_eq!(item.resolution_status, ResolutionStatus::Pending);
        repo.create(item).await.unwrap();

        // Archive should skip it
        let archived = repo.archive_old_resolved("test_space", -1).await.unwrap();
        assert_eq!(archived, 0, "should not archive pending assertions");
    }
}
