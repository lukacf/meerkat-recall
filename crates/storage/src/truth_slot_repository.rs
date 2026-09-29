//! Truth slot repository implementation for `SurrealDB`.
//!
//! Per spec §12, truth slots store the current best answer for queries.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::truth::{ConflictGroup, CurrentValue, TruthSlot};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

const TRUTH_SLOT_PROJECTION: &str = r"
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
    slot_key,
    slot_type,
    subject_entity,
    predicate_id,
    current_values,
    IF conflict_group IS NOT NONE THEN (IF type::is::record(conflict_group) THEN string::concat(meta::tb(conflict_group), ':', meta::id(conflict_group)) ELSE conflict_group END) ELSE NONE END AS conflict_group,
    confidence,
    rationale,
    semantics_version,
    alias_version,
    stale,
    stale_reason,
    computed_at,
    security_hash
";

/// Database-only proxy for TruthSlot.
#[derive(Deserialize)]
struct TruthSlotDb {
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
    slot_key: Option<String>,
    slot_type: Option<String>,
    subject_entity: Option<String>,
    predicate_id: Option<String>,
    current_values: Option<Vec<CurrentValue>>,
    conflict_group: Option<Link<ConflictGroup>>,
    confidence: Option<f32>,
    rationale: Option<String>,
    semantics_version: Option<u32>,
    alias_version: Option<u32>,
    stale: Option<bool>,
    stale_reason: Option<String>,
    computed_at: Option<chrono::DateTime<Utc>>,
    security_hash: Option<String>,
}

impl TryFrom<TruthSlotDb> for TruthSlot {
    type Error = StorageError;

    fn try_from(db: TruthSlotDb) -> Result<Self, Self::Error> {
        fn required<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
            value.ok_or_else(|| StorageError::QueryError(format!("Missing {field}")))
        }

        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required(db.provenance, "provenance")?,
                security: required(db.security, "security")?,
                subjects: required(db.subjects, "subjects")?,
                subjects_state: required(db.subjects_state, "subjects_state")?,
            },
            slot_key: db
                .slot_key
                .ok_or_else(|| StorageError::QueryError("Missing slot_key".to_string()))?,
            slot_type: db
                .slot_type
                .ok_or_else(|| StorageError::QueryError("Missing slot_type".to_string()))?,
            subject_entity: db
                .subject_entity
                .ok_or_else(|| StorageError::QueryError("Missing subject_entity".to_string()))?,
            predicate_id: db
                .predicate_id
                .ok_or_else(|| StorageError::QueryError("Missing predicate_id".to_string()))?,
            current_values: required(db.current_values, "current_values")?,
            conflict_group: db.conflict_group,
            confidence: required(db.confidence, "confidence")?,
            rationale: db.rationale,
            semantics_version: required(db.semantics_version, "semantics_version")?,
            alias_version: required(db.alias_version, "alias_version")?,
            stale: required(db.stale, "stale")?,
            stale_reason: db.stale_reason,
            computed_at: db
                .computed_at
                .ok_or_else(|| StorageError::QueryError("Missing computed_at".to_string()))?,
            security_hash: db
                .security_hash
                .ok_or_else(|| StorageError::QueryError("Missing security_hash".to_string()))?,
        })
    }
}

/// Truth slot repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct TruthSlotRepository {
    client: StorageClient,
}

impl TruthSlotRepository {
    /// Creates a new truth slot repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds a truth slot by its slot key.
    ///
    /// Per spec §12.3: slot_key uniquely identifies a truth slot.
    pub async fn find_by_slot_key(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> StorageResult<Option<TruthSlot>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<TruthSlot>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.slot_key == slot_key
            });
            items.sort_by_key(|item| item.base.id.to_string());
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE space_id = $space_id AND slot_key = $slot_key AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "slot_key" => slot_key },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse truth_slot: {e}")))?;

        items
            .into_iter()
            .next()
            .map(TruthSlot::try_from)
            .transpose()
    }

    /// Finds all truth slots for a given subject entity.
    pub async fn find_by_subject_entity(
        &self,
        space_id: &str,
        subject_entity: &str,
    ) -> StorageResult<Vec<TruthSlot>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<TruthSlot>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.subject_entity == subject_entity
            });
            items.sort_by_key(|item| {
                (
                    std::cmp::Reverse(item.computed_at),
                    item.base.id.to_string(),
                )
            });
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE space_id = $space_id \
                     AND subject_entity = $subject_entity AND deleted_at IS NONE ORDER BY computed_at DESC"
                ),
                crate::bindings! { "space_id" => space_id, "subject_entity" => subject_entity },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse truth_slots: {e}")))?;

        items.into_iter().map(TruthSlot::try_from).collect()
    }

    /// Finds all stale truth slots for recomputation.
    ///
    /// Per spec §11.5: stale slots need to be recomputed.
    pub async fn find_stale(&self, space_id: &str, limit: usize) -> StorageResult<Vec<TruthSlot>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<TruthSlot>().await?;
            items.retain(|item| {
                item.base.space_id == space_id && item.base.deleted_at.is_none() && item.stale
            });
            items.sort_by_key(|item| (item.computed_at, item.base.id.to_string()));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE space_id = $space_id AND stale = true AND deleted_at IS NONE ORDER BY computed_at ASC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse truth_slots: {e}")))?;

        items.into_iter().map(TruthSlot::try_from).collect()
    }

    /// Marks all slots for a predicate as stale.
    pub async fn mark_stale_by_predicate(
        &self,
        space_id: &str,
        predicate_id: &str,
        reason: &str,
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let slots = mutation.scan::<TruthSlot>()?;
                    let now = Utc::now();
                    for mut slot in slots {
                        if slot.base.space_id != space_id
                            || slot.base.deleted_at.is_some()
                            || slot.predicate_id != predicate_id
                        {
                            continue;
                        }
                        slot.stale = true;
                        slot.stale_reason = Some(reason.to_string());
                        slot.base.updated_at = now;
                        mutation.replace(slot)?;
                    }
                    Ok(())
                })
                .await;
        }

        self.client
            .query_bind_checked(
                "UPDATE truth_slot SET stale = true, stale_reason = $reason, updated_at = time::now() \
                 WHERE space_id = $space_id AND predicate_id = $predicate_id AND deleted_at IS NONE;",
                crate::bindings! {
                    "space_id" => space_id,
                    "predicate_id" => predicate_id,
                    "reason" => reason,
                },
            )
            .await?;
        Ok(())
    }

    /// Marks all slots related to an entity as stale.
    pub async fn mark_stale_by_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        reason: &str,
    ) -> StorageResult<()> {
        let entity_str = entity_id.to_string();

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let slots = mutation.scan::<TruthSlot>()?;
                    let now = Utc::now();
                    for mut slot in slots {
                        if slot.base.space_id != space_id || slot.base.deleted_at.is_some() {
                            continue;
                        }
                        if slot.subject_entity != entity_str && !slot.slot_key.contains(&entity_str)
                        {
                            continue;
                        }
                        slot.stale = true;
                        slot.stale_reason = Some(reason.to_string());
                        slot.base.updated_at = now;
                        mutation.replace(slot)?;
                    }
                    Ok(())
                })
                .await;
        }

        self.client
            .query_bind_checked(
                "UPDATE truth_slot SET stale = true, stale_reason = $reason, updated_at = time::now() \
                 WHERE space_id = $space_id AND deleted_at IS NONE AND \
                 (subject_entity = $entity_id OR string::contains(slot_key, $entity_id));",
                crate::bindings! {
                    "space_id" => space_id,
                    "entity_id" => entity_str,
                    "reason" => reason,
                },
            )
            .await?;
        Ok(())
    }

    /// Marks all slots in a space as stale.
    pub async fn mark_all_stale(&self, space_id: &str, reason: &str) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let slots = mutation.scan::<TruthSlot>()?;
                    let now = Utc::now();
                    for mut slot in slots {
                        if slot.base.space_id != space_id || slot.base.deleted_at.is_some() {
                            continue;
                        }
                        slot.stale = true;
                        slot.stale_reason = Some(reason.to_string());
                        slot.base.updated_at = now;
                        mutation.replace(slot)?;
                    }
                    Ok(())
                })
                .await;
        }

        self.client
            .query_bind_checked(
                "UPDATE truth_slot SET stale = true, stale_reason = $reason, updated_at = time::now() \
                 WHERE space_id = $space_id AND deleted_at IS NONE;",
                crate::bindings! {
                    "space_id" => space_id,
                    "reason" => reason,
                },
            )
            .await?;
        Ok(())
    }

    /// Converts a TruthSlot to a SurrealDB value for insertion.
    fn truth_slot_to_value(item: &TruthSlot) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;

        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("slot_key".to_string(), item.slot_key.clone().into());
        obj.insert("slot_type".to_string(), item.slot_type.clone().into());
        obj.insert(
            "subject_entity".to_string(),
            item.subject_entity.clone().into(),
        );
        obj.insert("predicate_id".to_string(), item.predicate_id.clone().into());
        let current_values_json = serde_json::to_value(&item.current_values).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize current_values: {e}"))
        })?;
        obj.insert(
            "current_values".to_string(),
            crate::surreal::json_to_surreal(current_values_json)?,
        );
        obj.insert(
            "conflict_group".to_string(),
            item.conflict_group.as_ref().map_or(Value::None, |link| {
                Value::from(crate::surreal::thing_from_record_id(link.id()))
            }),
        );
        obj.insert("confidence".to_string(), item.confidence.into());
        obj.insert(
            "rationale".to_string(),
            item.rationale.clone().map_or(Value::None, Value::from),
        );
        obj.insert(
            "semantics_version".to_string(),
            Value::from(i64::from(item.semantics_version)),
        );
        obj.insert(
            "alias_version".to_string(),
            Value::from(i64::from(item.alias_version)),
        );
        obj.insert("stale".to_string(), item.stale.into());
        obj.insert(
            "stale_reason".to_string(),
            item.stale_reason.clone().map_or(Value::None, Value::from),
        );
        obj.insert(
            "computed_at".to_string(),
            crate::surreal::datetime_from_chrono(item.computed_at).into(),
        );
        obj.insert(
            "security_hash".to_string(),
            item.security_hash.clone().into(),
        );

        Ok(Value::Object(obj))
    }

    /// Upserts a truth slot by ID.
    ///
    /// Ensures concurrent upserts for the same ID do not create duplicates.
    pub async fn upsert(&self, record: TruthSlot) -> StorageResult<TruthSlot> {
        let mut record = record;
        record.base.updated_at = Utc::now();
        let record_value = Self::truth_slot_to_value(&record)?;

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation.scan::<TruthSlot>()?.into_iter().any(|existing| {
                        existing.base.id != record.base.id
                            && existing.base.space_id == record.base.space_id
                            && existing.slot_key == record.slot_key
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate truth_slot slot_key for space {}: {}",
                            record.base.space_id, record.slot_key
                        )));
                    }
                    if mutation
                        .get::<TruthSlot>(&record.base.space_id, &record.base.id, true)?
                        .is_some()
                    {
                        Ok(mutation.replace(record.clone())?)
                    } else {
                        Ok(mutation.create(record.clone())?)
                    }
                })
                .await;
        }

        let response = self
            .client
            .query_bind(
                "UPSERT $id MERGE $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to upsert truth_slot: {e}"))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to upsert truth_slot: {e}"))
        })?;

        Ok(record)
    }
}

#[async_trait]
impl Repository<TruthSlot> for TruthSlotRepository {
    async fn create(&self, record: TruthSlot) -> StorageResult<TruthSlot> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation.scan::<TruthSlot>()?.into_iter().any(|existing| {
                        existing.base.space_id == record.base.space_id
                            && existing.slot_key == record.slot_key
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate truth_slot slot_key for space {}: {}",
                            record.base.space_id, record.slot_key
                        )));
                    }
                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        let response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => Self::truth_slot_to_value(&record)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to create truth_slot: {e}"))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create truth_slot: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created truth_slot: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but truth_slot not found".to_string())
        })?;

        TruthSlot::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<TruthSlot>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<TruthSlot>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse truth_slot: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(TruthSlot::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: TruthSlot,
    ) -> StorageResult<TruthSlot> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<TruthSlot>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate = mutation.scan::<TruthSlot>()?.into_iter().any(|existing| {
                        existing.base.id != target_id
                            && existing.base.space_id == target_space_id
                            && existing.slot_key == normalized.slot_key
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate truth_slot slot_key for space {}: {}",
                            target_space_id, normalized.slot_key
                        )));
                    }

                    Ok(mutation.replace(normalized.clone())?)
                })
                .await;
        }

        let response = self
            .client
            .query_bind(
                "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::truth_slot_to_value(&normalized)?,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!("Failed to update truth_slot: {e}"))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to update truth_slot: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let items: Vec<TruthSlotDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated truth_slot: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("TruthSlot not found in space: {id}")))?;

        TruthSlot::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<TruthSlot>(space_id, id).await;
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
    ) -> StorageResult<Vec<TruthSlot>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<TruthSlot>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {TRUTH_SLOT_PROJECTION} FROM truth_slot WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<TruthSlotDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse truth_slots: {e}")))?;

        items.into_iter().map(TruthSlot::try_from).collect()
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
    use elephant_types::records::rel::ClaimStatus;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81200);

    const TRUTH_SLOT_SCHEMA: &str = r"
        DEFINE TABLE truth_slot SCHEMALESS;
    ";

    async fn setup_test_repo() -> TruthSlotRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("truth_slot_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(TRUTH_SLOT_SCHEMA).await.unwrap();

        TruthSlotRepository::new(client)
    }

    async fn setup_embedded_test_repo() -> TruthSlotRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        TruthSlotRepository::new(client)
    }

    fn create_test_truth_slot() -> TruthSlot {
        let id = RecordId::new_v4(TableName::TruthSlot);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        TruthSlot {
            base,
            slot_key: "entity:123:works_at:hash".to_string(),
            slot_type: "rel".to_string(),
            subject_entity: "entity:123".to_string(),
            predicate_id: "works_at".to_string(),
            current_values: vec![CurrentValue {
                claim_id: "rel:claim1".to_string(),
                confidence: 0.9,
                status: ClaimStatus::Approved,
                evidence: vec![],
            }],
            conflict_group: None,
            confidence: 0.9,
            rationale: Some("Single claim with high confidence".to_string()),
            semantics_version: 1,
            alias_version: 1,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: "abcd1234".to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_truth_slot() {
        let repo = setup_test_repo().await;
        let item = create_test_truth_slot();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.slot_key, "entity:123:works_at:hash");
        assert_eq!(created.predicate_id, "works_at");
        assert!(!created.stale);
    }

    #[tokio::test]
    async fn test_get_truth_slot() {
        let repo = setup_test_repo().await;
        let item = create_test_truth_slot();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_truth_slot_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_truth_slot();
        item.current_values = vec![
            CurrentValue {
                claim_id: "claim1".to_string(),
                confidence: 0.9,
                status: ClaimStatus::Approved,
                evidence: vec![],
            },
            CurrentValue {
                claim_id: "claim2".to_string(),
                confidence: 0.7,
                status: ClaimStatus::Disputed,
                evidence: vec![],
            },
        ];
        item.semantics_version = 5;
        item.alias_version = 3;

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.current_values.len(), 2);
        assert_eq!(created.semantics_version, 5);
        assert_eq!(created.alias_version, 3);
    }

    #[tokio::test]
    async fn test_update_truth_slot() {
        let repo = setup_test_repo().await;
        let item = create_test_truth_slot();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.stale = true;
        updated.stale_reason = Some("Semantics version changed".to_string());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!(result.stale);
        assert_eq!(
            result.stale_reason,
            Some("Semantics version changed".to_string())
        );
    }

    #[tokio::test]
    async fn test_delete_truth_slot() {
        let repo = setup_test_repo().await;
        let item = create_test_truth_slot();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_embedded_create_rejects_duplicate_slot_key() {
        let repo = setup_embedded_test_repo().await;
        let first = create_test_truth_slot();
        let mut second = create_test_truth_slot();
        second.base.id = RecordId::new_v4(TableName::TruthSlot);

        repo.create(first).await.unwrap();
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate slot key should conflict");
        assert!(matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate truth_slot")));
    }

    #[tokio::test]
    async fn test_embedded_upsert_rejects_duplicate_slot_key_for_other_id() {
        let repo = setup_embedded_test_repo().await;
        let first = create_test_truth_slot();
        let mut second = create_test_truth_slot();
        second.base.id = RecordId::new_v4(TableName::TruthSlot);

        repo.create(first).await.unwrap();
        let err = repo
            .upsert(second)
            .await
            .expect_err("duplicate slot key should conflict");
        assert!(matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate truth_slot")));
    }
}
