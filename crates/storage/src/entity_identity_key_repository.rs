//! Entity identity key repository implementation for `SurrealDB`.
//!
//! `entity_identity_key` is the source-of-truth table for stable identity keys
//! (see `docs/spec.md`).

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use elephant_types::records::EntityIdentityKey;

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

pub(crate) const ENTITY_IDENTITY_KEY_PROJECTION: &str = r"
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
    key,
    entity,
    confidence,
    first_seen,
    last_seen,
    IF evidence IS NOT NONE THEN evidence ELSE [] END AS evidence,
    status
";

/// Database-only proxy for `EntityIdentityKey` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct EntityIdentityKeyDb {
    id: surrealdb::sql::Thing,
    space_id: String,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
    created_by: String,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: elephant_types::records::base::Provenance,
    security: elephant_types::records::base::Security,
    subjects: Vec<surrealdb::sql::Thing>,
    subjects_state: elephant_types::records::base::SubjectsState,
    key: String,
    entity: surrealdb::sql::Thing,
    confidence: f64,
    first_seen: Option<chrono::DateTime<Utc>>,
    last_seen: Option<chrono::DateTime<Utc>>,
    evidence: Vec<surrealdb::sql::Thing>,
    status: elephant_types::records::IdentityKeyStatus,
}

impl TryFrom<EntityIdentityKeyDb> for EntityIdentityKey {
    type Error = StorageError;

    fn try_from(db: EntityIdentityKeyDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: db.space_id,
                created_at: db.created_at,
                updated_at: db.updated_at,
                created_by: db.created_by,
                deleted_at: db.deleted_at,
                provenance: db.provenance,
                security: db.security,
                subjects: db
                    .subjects
                    .into_iter()
                    .map(|t| crate::surreal::record_id_from_thing(&t))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StorageError::QueryError(e.to_string()))?
                    .into_iter()
                    .map(Link::new)
                    .collect(),
                subjects_state: db.subjects_state,
            },
            key: db.key,
            entity: Link::new(
                crate::surreal::record_id_from_thing(&db.entity)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            confidence: db.confidence,
            first_seen: db.first_seen,
            last_seen: db.last_seen,
            evidence: db
                .evidence
                .into_iter()
                .map(|t| crate::surreal::record_id_from_thing(&t))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::QueryError(e.to_string()))?
                .into_iter()
                .map(Link::new)
                .collect(),
            status: db.status,
        })
    }
}

/// Entity identity key repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct EntityIdentityKeyRepository {
    client: StorageClient,
}

impl EntityIdentityKeyRepository {
    /// Creates a new identity key repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds an identity key by `(space_id, key)` (excluding soft-deleted).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> StorageResult<Option<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            let mut records = store.scan::<EntityIdentityKey>().await?;
            records.retain(|record| {
                record.base.space_id == space_id
                    && record.base.deleted_at.is_none()
                    && record.key == key
            });
            records.sort_by_key(|record| record.base.id.to_string());
            return Ok(records.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE space_id = $space_id AND key = $key AND deleted_at IS NONE"),
                crate::bindings! {
                    "space_id" => space_id,
                    "key" => key,
                },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_key: {e}"))
        })?;

        records
            .into_iter()
            .next()
            .map(EntityIdentityKey::try_from)
            .transpose()
    }

    /// Lists identity keys for a given entity (excluding soft-deleted).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn list_for_entity(
        &self,
        entity_id: &RecordId,
        include_revoked: bool,
    ) -> StorageResult<Vec<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            let mut records = store.scan::<EntityIdentityKey>().await?;
            records.retain(|record| {
                record.entity.id() == entity_id
                    && record.base.deleted_at.is_none()
                    && (include_revoked
                        || record.status == elephant_types::records::IdentityKeyStatus::Active)
            });
            records.sort_by_key(|record| record.base.id.to_string());
            return Ok(records);
        }

        let mut query = format!("SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE entity = $entity_id AND deleted_at IS NONE");
        if !include_revoked {
            query.push_str(" AND status = 'active'");
        }

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! { "entity_id" => crate::surreal::thing_from_record_id(entity_id) },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_keys: {e}"))
        })?;

        records
            .into_iter()
            .map(EntityIdentityKey::try_from)
            .collect()
    }

    /// Repoints identity keys from one entity to another.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn repoint_entity(
        &self,
        old_rid: &RecordId,
        new_rid: &RecordId,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let records = mutation.scan::<EntityIdentityKey>()?;
                    let mut total = 0_usize;
                    let now = Utc::now();
                    for mut record in records {
                        if record.base.deleted_at.is_some() || record.entity.id() != old_rid {
                            continue;
                        }
                        record.entity = Link::new(new_rid.clone());
                        record.base.updated_at = now;
                        mutation.replace(record)?;
                        total += 1;
                    }
                    Ok(total)
                })
                .await;
        }

        // Repoint and return the affected ids so the count reflects the rows
        // actually updated (no separate, racy pre-count) and a statement-level
        // failure surfaces instead of reporting the pre-count as success.
        let mut response = self
            .client
            .query_bind_checked(
                r"
                UPDATE entity_identity_key
                SET entity = $new_id,
                    updated_at = time::now()
                WHERE entity = $old_id
                  AND deleted_at IS NONE
                RETURN VALUE id;
                ",
                crate::bindings! {
                    "old_id" => crate::surreal::thing_from_record_id(old_rid),
                    "new_id" => crate::surreal::thing_from_record_id(new_rid),
                },
            )
            .await?;

        let repointed_ids: Vec<surrealdb::sql::Thing> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!(
                "Failed to parse repointed entity_identity_key ids: {e}"
            ))
        })?;

        Ok(repointed_ids.len())
    }

    /// Revokes identity keys for an entity by key string.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn revoke_for_entity(
        &self,
        entity_id: &RecordId,
        keys: &[String],
        revoked_at: DateTime<Utc>,
    ) -> StorageResult<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        if let Some(store) = self.client.embedded_store() {
            let keys: std::collections::HashSet<_> = keys.iter().cloned().collect();
            return store
                .mutate(|mutation| {
                    let records = mutation.scan::<EntityIdentityKey>()?;
                    let mut updated = 0_usize;
                    for mut record in records {
                        if record.base.deleted_at.is_some()
                            || record.entity.id() != entity_id
                            || !keys.contains(&record.key)
                        {
                            continue;
                        }
                        record.status = elephant_types::records::IdentityKeyStatus::Revoked;
                        record.base.updated_at = revoked_at;
                        mutation.replace(record)?;
                        updated += 1;
                    }
                    Ok(updated)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind(
                r"
                UPDATE entity_identity_key
                SET status = 'revoked',
                    updated_at = $revoked_at
                WHERE entity = $entity_id
                  AND key INSIDE $keys
                  AND deleted_at IS NONE;
                ",
                crate::bindings! {
                    "entity_id" => crate::surreal::thing_from_record_id(entity_id),
                    "keys" => keys.to_vec(),
                    "revoked_at" => revoked_at,
                },
            )
            .await?;

        // UPDATE returns the modified records; count them.
        let updated: Vec<serde_json::Value> = crate::response::take_first(
            &mut response,
            "Failed to parse entity_identity_key revoke response",
        )?;
        Ok(updated.len())
    }
}

#[async_trait]
impl Repository<EntityIdentityKey> for EntityIdentityKeyRepository {
    async fn create(&self, record: EntityIdentityKey) -> StorageResult<EntityIdentityKey> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate =
                        mutation
                            .scan::<EntityIdentityKey>()?
                            .into_iter()
                            .any(|existing| {
                                existing.base.space_id == record.base.space_id
                                    && existing.key == record.key
                            });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate entity_identity_key for space {}: {}",
                            record.base.space_id, record.key
                        )));
                    }
                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {ENTITY_IDENTITY_KEY_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::entity_identity_key_to_value(&record)?,
                },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created entity_identity_key: {e}"))
        })?;

        let record_db = records.into_iter().next().ok_or_else(|| {
            StorageError::QueryError(
                "Create succeeded but entity_identity_key not found".to_string(),
            )
        })?;

        EntityIdentityKey::try_from(record_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<EntityIdentityKey>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_key: {e}"))
        })?;

        records
            .into_iter()
            .next()
            .map(EntityIdentityKey::try_from)
            .transpose()
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: EntityIdentityKey,
    ) -> StorageResult<EntityIdentityKey> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<EntityIdentityKey>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate =
                        mutation
                            .scan::<EntityIdentityKey>()?
                            .into_iter()
                            .any(|existing| {
                                existing.base.id != target_id
                                    && existing.base.space_id == target_space_id
                                    && existing.key == normalized.key
                            });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate entity_identity_key for space {}: {}",
                            target_space_id, normalized.key
                        )));
                    }

                    Ok(mutation.replace(normalized.clone())?)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ENTITY_IDENTITY_KEY_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::entity_identity_key_to_value(&normalized)?,
                },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated entity_identity_key: {e}"))
        })?;

        let record_db = records.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("EntityIdentityKey not found in space: {id}"))
        })?;

        EntityIdentityKey::try_from(record_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<EntityIdentityKey>(space_id, id).await;
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
    ) -> StorageResult<Vec<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<EntityIdentityKey>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_keys: {e}"))
        })?;

        Ok(records
            .into_iter()
            .map(EntityIdentityKey::try_from)
            .collect::<Result<Vec<_>, _>>()?)
    }
}

#[async_trait]
impl SearchRepository<EntityIdentityKey> for EntityIdentityKeyRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search::<EntityIdentityKey, _>(criteria, |record, criteria| {
                    criteria
                        .query
                        .as_ref()
                        .is_none_or(|query| record.key.contains(query))
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
            conditions.push("key CONTAINS $query".to_string());
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::EntityIdentityKey)
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
            "SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let records_db: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_keys: {e}"))
        })?;

        let mut records: Vec<EntityIdentityKey> = records_db
            .into_iter()
            .map(EntityIdentityKey::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = records.len() > criteria.limit;
        if has_more {
            records.pop();
        }

        let next_cursor = if has_more {
            records.last().map(|r| {
                r.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&r.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM entity_identity_key {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> = crate::response::take_first(
            &mut count_response,
            "Failed to parse entity_identity_key list count",
        )?;
        let total = counts.first().map(|c| c.total).ok_or_else(|| {
            StorageError::QueryError("Missing entity_identity_key list count row".to_string())
        })?;

        Ok(PaginatedResult::new(records, next_cursor, has_more, total))
    }
}

impl EntityIdentityKeyRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityIdentityKey>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<EntityIdentityKey>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ENTITY_IDENTITY_KEY_PROJECTION} FROM entity_identity_key WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let records: Vec<EntityIdentityKeyDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity_identity_key: {e}"))
        })?;

        records
            .into_iter()
            .next()
            .map(EntityIdentityKey::try_from)
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

    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::identity::IdentityKeyStatus;
    use elephant_types::{Link, RecordId, TableName};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(42000);

    const SCHEMA: &str = r"
        DEFINE TABLE entity_identity_key SCHEMALESS;
    ";

    async fn setup_test_repo() -> Result<EntityIdentityKeyRepository, Box<dyn std::error::Error>> {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("entity_identity_key_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await?;
        client.query(SCHEMA).await?;
        Ok(EntityIdentityKeyRepository::new(client))
    }

    async fn setup_embedded_test_repo() -> EntityIdentityKeyRepository {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        EntityIdentityKeyRepository::new(client)
    }

    fn make_key(entity_id: RecordId, key: &str) -> EntityIdentityKey {
        let id = RecordId::new_v4(TableName::EntityIdentityKey);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        EntityIdentityKey {
            base,
            key: key.to_string(),
            entity: Link::new(entity_id),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: Vec::new(),
            status: IdentityKeyStatus::Active,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_and_find_by_key() -> Result<(), Box<dyn std::error::Error>> {
        let repo = setup_test_repo().await?;
        let entity_id = RecordId::new_v4(TableName::Entity);

        let record = make_key(entity_id, "email:test@example.com");
        let created = repo.create(record).await?;

        let fetched = repo
            .find_by_key("test_space", "email:test@example.com")
            .await?;
        assert!(fetched.is_some());
        let fetched =
            fetched.ok_or_else(|| StorageError::NotFound("Missing fetched key".to_string()))?;
        assert_eq!(fetched.base.id, created.base.id);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_duplicate_key() {
        let repo = setup_embedded_test_repo().await;
        let first = make_key(
            RecordId::new_v4(TableName::Entity),
            "email:test@example.com",
        );
        let second = make_key(
            RecordId::new_v4(TableName::Entity),
            "email:test@example.com",
        );

        repo.create(first).await.expect("create first key");
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate key should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate entity_identity_key"))
        );
    }
}
