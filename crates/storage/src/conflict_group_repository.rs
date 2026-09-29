//! Conflict group repository implementation for `SurrealDB`.
//!
//! Per spec §12, conflict groups track competing claims for truth slots.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::truth::ConflictGroup;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

const CONFLICT_GROUP_PROJECTION: &str = r"
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
    slot_key,
    slot_type,
    claim_ids,
    resolved,
    resolution_rationale,
    detected_at,
    resolved_at,
    adjudicated_claim_set_hash,
    adjudicated_at
";

/// Database-only proxy for ConflictGroup.
#[derive(Deserialize)]
struct ConflictGroupDb {
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
    claim_ids: Option<Vec<String>>,
    resolved: Option<bool>,
    resolution_rationale: Option<String>,
    detected_at: Option<chrono::DateTime<Utc>>,
    resolved_at: Option<chrono::DateTime<Utc>>,
    adjudicated_claim_set_hash: Option<String>,
    adjudicated_at: Option<chrono::DateTime<Utc>>,
}

impl TryFrom<ConflictGroupDb> for ConflictGroup {
    type Error = StorageError;

    fn try_from(db: ConflictGroupDb) -> Result<Self, Self::Error> {
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
            slot_key: db
                .slot_key
                .ok_or_else(|| StorageError::QueryError("Missing slot_key".to_string()))?,
            slot_type: db
                .slot_type
                .ok_or_else(|| StorageError::QueryError("Missing slot_type".to_string()))?,
            claim_ids: required_field(db.claim_ids, "claim_ids")?,
            resolved: required_field(db.resolved, "resolved")?,
            resolution_rationale: db.resolution_rationale,
            detected_at: db
                .detected_at
                .ok_or_else(|| StorageError::QueryError("Missing detected_at".to_string()))?,
            resolved_at: db.resolved_at,
            adjudicated_claim_set_hash: db.adjudicated_claim_set_hash,
            adjudicated_at: db.adjudicated_at,
        })
    }
}

/// Conflict group repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct ConflictGroupRepository {
    client: StorageClient,
}

impl ConflictGroupRepository {
    /// Creates a new conflict group repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds an unresolved conflict group by its slot key.
    ///
    /// Per spec §12: unresolved conflicts need resolution.
    pub async fn find_unresolved_by_slot_key(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> StorageResult<Option<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<ConflictGroup>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && !item.resolved
                    && item.slot_key == slot_key
            });
            items.sort_by_key(|item| item.base.id.to_string());
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE space_id = $space_id AND slot_key = $slot_key AND resolved = false AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "slot_key" => slot_key },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_group: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(ConflictGroup::try_from)
            .transpose()
    }

    /// Finds an unresolved conflict group by its slot key and slot type.
    ///
    /// Like [`Self::find_unresolved_by_slot_key`] but additionally filters on
    /// `slot_type` as defense in depth against slot-key ambiguity.
    pub async fn find_unresolved_by_slot_key_and_type(
        &self,
        space_id: &str,
        slot_key: &str,
        slot_type: &str,
    ) -> StorageResult<Option<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<ConflictGroup>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && !item.resolved
                    && item.slot_key == slot_key
                    && item.slot_type == slot_type
            });
            items.sort_by_key(|item| item.base.id.to_string());
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE space_id = $space_id AND slot_key = $slot_key AND slot_type = $slot_type AND resolved = false AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "slot_key" => slot_key, "slot_type" => slot_type },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_group: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(ConflictGroup::try_from)
            .transpose()
    }

    /// Finds all unresolved conflict groups for a space.
    ///
    /// Per spec §12: unresolved conflicts need resolution.
    pub async fn find_all_unresolved(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<ConflictGroup>().await?;
            items.retain(|item| {
                item.base.space_id == space_id && item.base.deleted_at.is_none() && !item.resolved
            });
            items.sort_by_key(|item| (item.detected_at, item.base.id.to_string()));
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE space_id = $space_id AND resolved = false AND deleted_at IS NONE ORDER BY detected_at ASC LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_groups: {e}"))
        })?;

        items.into_iter().map(ConflictGroup::try_from).collect()
    }

    /// Converts a ConflictGroup to a SurrealDB value for insertion.
    fn conflict_group_to_value(item: &ConflictGroup) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;

        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("slot_key".to_string(), item.slot_key.clone().into());
        obj.insert("slot_type".to_string(), item.slot_type.clone().into());
        let claims_json = serde_json::to_value(&item.claim_ids).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize claim_ids: {e}"))
        })?;
        obj.insert(
            "claim_ids".to_string(),
            crate::surreal::json_to_surreal(claims_json)?,
        );
        obj.insert("resolved".to_string(), item.resolved.into());
        obj.insert(
            "resolution_rationale".to_string(),
            item.resolution_rationale
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "detected_at".to_string(),
            crate::surreal::datetime_from_chrono(item.detected_at).into(),
        );
        obj.insert(
            "resolved_at".to_string(),
            item.resolved_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        obj.insert(
            "adjudicated_claim_set_hash".to_string(),
            item.adjudicated_claim_set_hash
                .clone()
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "adjudicated_at".to_string(),
            item.adjudicated_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );

        Ok(Value::Object(obj))
    }

    /// Upserts a conflict group by ID.
    ///
    /// Ensures concurrent upserts for the same ID do not create duplicates.
    pub async fn upsert(&self, record: ConflictGroup) -> StorageResult<ConflictGroup> {
        use surrealdb::sql::{Array, Value};

        let mut record = record;
        record.base.updated_at = Utc::now();

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    if mutation
                        .get::<ConflictGroup>(&record.base.space_id, &record.base.id, true)?
                        .is_some()
                    {
                        Ok(mutation.replace(record.clone())?)
                    } else {
                        Ok(mutation.create(record.clone())?)
                    }
                })
                .await;
        }

        self.client
            .query_bind_checked(
                "UPSERT $id SET
                    space_id = $space_id,
                    created_at = $created_at,
                    updated_at = $updated_at,
                    created_by = $created_by,
                    deleted_at = $deleted_at,
                    security = $security,
                    provenance = $provenance,
                    subjects = $subjects,
                    subjects_state = $subjects_state,
                    slot_key = $slot_key,
                    slot_type = $slot_type,
                    claim_ids = $claim_ids,
                    resolved = $resolved,
                    resolution_rationale = $resolution_rationale,
                    detected_at = $detected_at,
                    resolved_at = $resolved_at,
                    adjudicated_claim_set_hash = $adjudicated_claim_set_hash,
                    adjudicated_at = $adjudicated_at;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "space_id" => record.base.space_id.clone(),
                    "created_at" => crate::surreal::datetime_from_chrono(record.base.created_at),
                    "updated_at" => crate::surreal::datetime_from_chrono(record.base.updated_at),
                    "created_by" => record.base.created_by.clone(),
                    "deleted_at" => record.base.deleted_at.map_or(Value::None, |value| {
                        Value::from(crate::surreal::datetime_from_chrono(value))
                    }),
                    "security" => crate::surreal::security_to_value(&record.base.security),
                    "provenance" => crate::surreal::provenance_to_value(&record.base.provenance)?,
                    "subjects" => Value::Array(crate::surreal::things_from_links(record.base.subjects.clone())),
                    "subjects_state" => crate::surreal::enum_to_surreal(&record.base.subjects_state)?,
                    "slot_key" => record.slot_key.clone(),
                    "slot_type" => record.slot_type.clone(),
                    "claim_ids" => Value::Array(Array::from(record.claim_ids.clone())),
                    "resolved" => record.resolved,
                    "resolution_rationale" => record
                        .resolution_rationale
                        .as_ref()
                        .map_or(Value::None, |value| Value::from(value.clone())),
                    "detected_at" => crate::surreal::datetime_from_chrono(record.detected_at),
                    "resolved_at" => record
                        .resolved_at
                        .map(crate::surreal::datetime_from_chrono)
                        .map_or(Value::None, Value::from),
                    "adjudicated_claim_set_hash" => record
                        .adjudicated_claim_set_hash
                        .as_ref()
                        .map_or(Value::None, |value| Value::from(value.clone())),
                    "adjudicated_at" => record
                        .adjudicated_at
                        .map(crate::surreal::datetime_from_chrono)
                        .map_or(Value::None, Value::from),
                },
            )
            .await?;

        Ok(record)
    }

    /// Merges claim IDs into an unresolved conflict group.
    ///
    /// Returns `None` if the group is already resolved or missing.
    pub async fn merge_unresolved_claim_ids(
        &self,
        space_id: &str,
        id: &RecordId,
        claim_ids: Vec<String>,
    ) -> StorageResult<Option<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(mut item) = mutation.get::<ConflictGroup>(space_id, id, false)? else {
                        return Ok(None);
                    };
                    if item.resolved {
                        return Ok(None);
                    }

                    for claim_id in claim_ids {
                        if !item.claim_ids.contains(&claim_id) {
                            item.claim_ids.push(claim_id);
                        }
                    }
                    item.base.updated_at = Utc::now();
                    mutation.replace(item.clone())?;
                    Ok(Some(item))
                })
                .await;
        }

        use surrealdb::sql::{Array, Value};

        let mut response = self
            .client
            .query_bind(
                "UPDATE $id SET
                    claim_ids = array::union(claim_ids, $claim_ids),
                    updated_at = time::now()
                 WHERE space_id = $space_id AND resolved = false AND deleted_at IS NONE RETURN AFTER;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "claim_ids" => Value::Array(Array::from(claim_ids)),
                },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_group: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(ConflictGroup::try_from)
            .transpose()
    }
}

#[async_trait]
impl Repository<ConflictGroup> for ConflictGroupRepository {
    async fn create(&self, record: ConflictGroup) -> StorageResult<ConflictGroup> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::conflict_group_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {CONFLICT_GROUP_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created conflict_group: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but conflict_group not found".to_string())
        })?;

        ConflictGroup::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<ConflictGroup>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_group: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(ConflictGroup::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: ConflictGroup,
    ) -> StorageResult<ConflictGroup> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {CONFLICT_GROUP_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::conflict_group_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated conflict_group: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("ConflictGroup not found in space: {id}"))
        })?;

        ConflictGroup::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<ConflictGroup>(space_id, id).await;
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
    ) -> StorageResult<Vec<ConflictGroup>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<ConflictGroup>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {CONFLICT_GROUP_PROJECTION} FROM conflict_group WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<ConflictGroupDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse conflict_groups: {e}"))
        })?;

        items.into_iter().map(ConflictGroup::try_from).collect()
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81100);

    const CONFLICT_GROUP_SCHEMA: &str = r"
        DEFINE TABLE conflict_group SCHEMALESS;
    ";

    async fn setup_test_repo() -> ConflictGroupRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("conflict_group_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(CONFLICT_GROUP_SCHEMA).await.unwrap();

        ConflictGroupRepository::new(client)
    }

    fn create_test_conflict_group() -> ConflictGroup {
        let id = RecordId::new_v4(TableName::ConflictGroup);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        ConflictGroup {
            base,
            slot_key: "entity:123:works_at:hash".to_string(),
            slot_type: "rel".to_string(),
            claim_ids: vec!["rel:claim1".to_string(), "rel:claim2".to_string()],
            resolved: false,
            resolution_rationale: None,
            detected_at: Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: None,
            adjudicated_at: None,
        }
    }

    #[tokio::test]
    async fn test_create_conflict_group() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.slot_key, "entity:123:works_at:hash");
        assert_eq!(created.slot_type, "rel");
        assert!(!created.resolved);
    }

    #[tokio::test]
    async fn test_get_conflict_group() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_conflict_group_missing_claim_ids_error() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        repo.client
            .query_bind(
                "UPDATE $id SET claim_ids = NONE",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&id) },
            )
            .await
            .unwrap();

        let err = repo
            .get("test_space", &id)
            .await
            .expect_err("missing claim_ids should error");
        let message = err.to_string();
        assert!(
            message.contains("claim_ids"),
            "expected claim_ids error, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_conflict_group_missing_resolved_error() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        repo.client
            .query_bind(
                "UPDATE $id SET resolved = NONE",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&id) },
            )
            .await
            .unwrap();

        let err = repo
            .get("test_space", &id)
            .await
            .expect_err("missing resolved should error");
        let message = err.to_string();
        assert!(
            message.contains("resolved"),
            "expected resolved error, got: {message}"
        );
    }

    #[tokio::test]
    async fn test_conflict_group_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_conflict_group();
        item.claim_ids = vec![
            "claim1".to_string(),
            "claim2".to_string(),
            "claim3".to_string(),
        ];

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.claim_ids.len(), 3);
        assert_eq!(created.slot_key, item.slot_key);
        assert_eq!(created.slot_type, item.slot_type);
    }

    #[tokio::test]
    async fn test_conflict_group_adjudication_fields_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_conflict_group();
        let adjudicated_at = Utc::now();
        item.adjudicated_claim_set_hash = Some("sha256:abc123".to_string());
        item.adjudicated_at = Some(adjudicated_at);
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();
        assert_eq!(
            created.adjudicated_claim_set_hash,
            Some("sha256:abc123".to_string())
        );
        assert_eq!(created.adjudicated_at, Some(adjudicated_at));

        let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(
            retrieved.adjudicated_claim_set_hash,
            Some("sha256:abc123".to_string())
        );
        assert!(retrieved.adjudicated_at.is_some());
    }

    #[tokio::test]
    async fn test_conflict_group_adjudication_fields_upsert_roundtrip() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        let created = repo.upsert(item).await.unwrap();
        assert!(created.adjudicated_claim_set_hash.is_none());
        assert!(created.adjudicated_at.is_none());

        let mut updated = created;
        updated.adjudicated_claim_set_hash = Some("sha256:def456".to_string());
        updated.adjudicated_at = Some(Utc::now());
        repo.upsert(updated).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(
            retrieved.adjudicated_claim_set_hash,
            Some("sha256:def456".to_string())
        );
        assert!(retrieved.adjudicated_at.is_some());
    }

    #[tokio::test]
    async fn test_find_unresolved_by_slot_key_and_type() {
        let repo = setup_test_repo().await;

        let mut rel_set_group = create_test_conflict_group();
        rel_set_group.slot_key = "entity:123:__rel_set__:works_at:hash".to_string();
        rel_set_group.slot_type = "rel_set".to_string();
        repo.create(rel_set_group.clone()).await.unwrap();

        let mut attr_group = create_test_conflict_group();
        attr_group.base.id = RecordId::new_v4(TableName::ConflictGroup);
        attr_group.slot_key = rel_set_group.slot_key.clone();
        attr_group.slot_type = "attribute".to_string();
        repo.create(attr_group).await.unwrap();

        // Same slot_key, distinguished by slot_type.
        let found = repo
            .find_unresolved_by_slot_key_and_type("test_space", &rel_set_group.slot_key, "rel_set")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.slot_type, "rel_set");
        assert_eq!(found.base.id, rel_set_group.base.id);

        let found_attr = repo
            .find_unresolved_by_slot_key_and_type(
                "test_space",
                &rel_set_group.slot_key,
                "attribute",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found_attr.slot_type, "attribute");

        // No group with this slot_type.
        let missing = repo
            .find_unresolved_by_slot_key_and_type("test_space", &rel_set_group.slot_key, "rel")
            .await
            .unwrap();
        assert!(missing.is_none());

        // Other space is isolated.
        let other_space = repo
            .find_unresolved_by_slot_key_and_type("other_space", &rel_set_group.slot_key, "rel_set")
            .await
            .unwrap();
        assert!(other_space.is_none());
    }

    #[tokio::test]
    async fn test_find_unresolved_by_slot_key_and_type_skips_resolved() {
        let repo = setup_test_repo().await;

        let mut group = create_test_conflict_group();
        group.resolved = true;
        group.resolved_at = Some(Utc::now());
        repo.create(group.clone()).await.unwrap();

        let found = repo
            .find_unresolved_by_slot_key_and_type("test_space", &group.slot_key, "rel")
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_update_conflict_group() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.resolved = true;
        updated.resolution_rationale = Some("Higher confidence claim wins".to_string());
        updated.resolved_at = Some(Utc::now());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!(result.resolved);
        assert_eq!(
            result.resolution_rationale,
            Some("Higher confidence claim wins".to_string())
        );
        assert!(result.resolved_at.is_some());
    }

    #[tokio::test]
    async fn test_delete_conflict_group() {
        let repo = setup_test_repo().await;
        let item = create_test_conflict_group();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
