//! Predicate alias repository implementation for `SurrealDB`.
//!
//! Per spec §10, predicate_alias stores alias mappings for predicates.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use surrealdb::sql::Value;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::predicate::{AliasScope, AliasStatus, AliasType, PredicateAlias};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

const PREDICATE_ALIAS_PROJECTION: &str = r"
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
    alias,
    alias_type,
    source_plugin,
    confidence,
    status,
    scope,
    predicate_id,
    confidence_version,
    usage_count
";

/// Database-only proxy for PredicateAlias.
#[derive(Deserialize)]
struct PredicateAliasDb {
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
    alias: Option<String>,
    alias_type: Option<AliasType>,
    source_plugin: Option<String>,
    confidence: Option<f32>,
    status: Option<AliasStatus>,
    scope: Option<AliasScope>,
    predicate_id: Option<String>,
    confidence_version: Option<u32>,
    usage_count: Option<u64>,
}

impl TryFrom<PredicateAliasDb> for PredicateAlias {
    type Error = StorageError;

    fn try_from(db: PredicateAliasDb) -> Result<Self, Self::Error> {
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
            alias: db
                .alias
                .ok_or_else(|| StorageError::QueryError("Missing alias".to_string()))?,
            alias_type: required(db.alias_type, "alias_type")?,
            source_plugin: db.source_plugin,
            confidence: required(db.confidence, "confidence")?,
            status: required(db.status, "status")?,
            scope: required(db.scope, "scope")?,
            predicate_id: db
                .predicate_id
                .ok_or_else(|| StorageError::QueryError("Missing predicate_id".to_string()))?,
            confidence_version: required(db.confidence_version, "confidence_version")?,
            usage_count: required(db.usage_count, "usage_count")?,
        })
    }
}

/// Predicate alias repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct PredicateAliasRepository {
    client: StorageClient,
}

impl PredicateAliasRepository {
    /// Creates a new predicate alias repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds an active predicate alias by raw alias string (case-insensitive).
    ///
    /// Per spec §10.3: Canonicalization queries predicate_alias table for active
    /// aliases matching the raw predicate string.
    ///
    /// # Arguments
    /// * `space_id` - The space to search in
    /// * `raw` - The raw predicate string to look up (case-insensitive)
    ///
    /// # Returns
    /// The first active alias matching the raw string, or None if not found.
    ///
    /// # Errors
    /// Returns `StorageError::QueryError` if the database query fails.
    /// Returns `StorageError::DataIntegrityError` if the result cannot be converted.
    pub async fn find_active_by_raw(
        &self,
        space_id: &str,
        raw: &str,
    ) -> StorageResult<Option<PredicateAlias>> {
        if let Some(store) = self.client.embedded_store() {
            let raw_lower = raw.to_ascii_lowercase();
            let mut items = store.scan::<PredicateAlias>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.status == AliasStatus::Active
                    && item.alias.to_ascii_lowercase() == raw_lower
            });
            items.sort_by(|lhs, rhs| {
                rhs.confidence
                    .partial_cmp(&lhs.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            return Ok(items.into_iter().next());
        }

        let raw_lower = raw.to_lowercase();
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias \
                     WHERE space_id = $space_id \
                     AND string::lowercase(alias) = $raw_lower \
                     AND status = 'active' \
                     AND deleted_at IS NONE \
                     ORDER BY confidence DESC \
                     LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "raw_lower" => raw_lower
                },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to find active predicate alias: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(PredicateAlias::try_from)
            .transpose()
    }

    /// Finds all predicate aliases for a canonical predicate_id.
    ///
    /// # Arguments
    /// * `space_id` - The space to search in
    /// * `predicate_id` - The canonical predicate_id to match
    ///
    /// # Errors
    /// Returns `StorageError::QueryError` if the database query fails.
    /// Returns `StorageError::DataIntegrityError` if the result cannot be converted.
    pub async fn find_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> StorageResult<Vec<PredicateAlias>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<PredicateAlias>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.predicate_id == predicate_id
            });
            items.sort_by_key(|item| item.base.id.to_string());
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias \
                     WHERE space_id = $space_id \
                     AND predicate_id = $predicate_id \
                     AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "predicate_id" => predicate_id
                },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to find predicate aliases: {e}"))
        })?;

        items.into_iter().map(PredicateAlias::try_from).collect()
    }

    /// Converts a PredicateAlias to a SurrealDB value for insertion.
    pub(crate) fn predicate_alias_to_value(item: &PredicateAlias) -> StorageResult<Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("alias".to_string(), item.alias.clone().into());
        obj.insert(
            "alias_type".to_string(),
            crate::surreal::enum_to_surreal(&item.alias_type)?,
        );
        obj.insert(
            "source_plugin".to_string(),
            item.source_plugin.clone().map_or(Value::None, Value::from),
        );
        obj.insert("confidence".to_string(), Value::from(item.confidence));
        obj.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );
        obj.insert(
            "scope".to_string(),
            crate::surreal::enum_to_surreal(&item.scope)?,
        );
        obj.insert("predicate_id".to_string(), item.predicate_id.clone().into());
        obj.insert(
            "confidence_version".to_string(),
            Value::from(item.confidence_version),
        );
        obj.insert("usage_count".to_string(), Value::from(item.usage_count));
        Ok(Value::Object(obj))
    }
}

#[async_trait]
impl Repository<PredicateAlias> for PredicateAliasRepository {
    async fn create(&self, record: PredicateAlias) -> StorageResult<PredicateAlias> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::predicate_alias_to_value(&record)?;
        let response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;
        response.check().map_err(|e| {
            StorageError::QueryError(format!("Failed to create predicate_alias: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created predicate_alias: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but predicate_alias not found".to_string())
        })?;

        PredicateAlias::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateAlias>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<PredicateAlias>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_alias: {e}"))
        })?;

        Ok(items
            .into_iter()
            .next()
            .map(PredicateAlias::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: PredicateAlias,
    ) -> StorageResult<PredicateAlias> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {PREDICATE_ALIAS_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::predicate_alias_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated predicate_alias: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("PredicateAlias not found in space: {id}"))
        })?;

        PredicateAlias::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<PredicateAlias>(space_id, id).await;
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
    ) -> StorageResult<Vec<PredicateAlias>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<PredicateAlias>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_aliases: {e}"))
        })?;

        items.into_iter().map(PredicateAlias::try_from).collect()
    }
}

impl PredicateAliasRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<PredicateAlias>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<PredicateAlias>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_ALIAS_PROJECTION} FROM predicate_alias WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<PredicateAliasDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_alias: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(PredicateAlias::try_from)
            .transpose()
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81000);

    const PREDICATE_ALIAS_SCHEMA: &str = r"
        DEFINE TABLE predicate_alias SCHEMALESS;
    ";

    async fn setup_test_repo() -> PredicateAliasRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("predicate_alias_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(PREDICATE_ALIAS_SCHEMA).await.unwrap();

        PredicateAliasRepository::new(client)
    }

    fn create_test_predicate_alias() -> PredicateAlias {
        let id = RecordId::new_v4(TableName::PredicateAlias);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        PredicateAlias {
            base,
            alias: "employed_at".to_string(),
            alias_type: AliasType::RawString,
            source_plugin: None,
            confidence: 0.9,
            status: AliasStatus::Proposed,
            scope: AliasScope::Global,
            predicate_id: "works_at".to_string(),
            confidence_version: 1,
            usage_count: 0,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_predicate_alias() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_alias();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.alias, "employed_at");
        assert_eq!(created.predicate_id, "works_at");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_predicate_alias() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_alias();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_predicate_alias_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_predicate_alias();
        item.confidence = 0.85;
        item.usage_count = 50;
        item.source_plugin = Some("extractor_v1".to_string());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert!((created.confidence - 0.85).abs() < f32::EPSILON);
        assert_eq!(created.usage_count, 50);
        assert_eq!(created.source_plugin, Some("extractor_v1".to_string()));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_predicate_alias() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_alias();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = AliasStatus::Active;
        updated.confidence = 0.95;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, AliasStatus::Active);
        assert!((result.confidence - 0.95).abs() < f32::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_predicate_alias() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_alias();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_active_by_raw() {
        let repo = setup_test_repo().await;
        let mut item = create_test_predicate_alias();
        item.alias = "Works At".to_string(); // Mixed case
        item.status = AliasStatus::Active;
        item.confidence = 0.95;

        repo.create(item).await.unwrap();

        // Should find with case-insensitive match
        let found = repo
            .find_active_by_raw("test_space", "works at")
            .await
            .unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.predicate_id, "works_at");
        assert!((found.confidence - 0.95).abs() < f32::EPSILON);

        // Should find with different case
        let found = repo
            .find_active_by_raw("test_space", "WORKS AT")
            .await
            .unwrap();
        assert!(found.is_some());

        // Should not find non-existent alias
        let not_found = repo
            .find_active_by_raw("test_space", "nonexistent")
            .await
            .unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_active_by_raw_ignores_proposed() {
        let repo = setup_test_repo().await;
        let mut item = create_test_predicate_alias();
        item.alias = "test_alias".to_string();
        item.status = AliasStatus::Proposed; // Not active

        repo.create(item).await.unwrap();

        // Should not find proposed alias
        let not_found = repo
            .find_active_by_raw("test_space", "test_alias")
            .await
            .unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_active_by_raw_returns_highest_confidence() {
        let repo = setup_test_repo().await;

        // Create two active aliases with same raw string but different confidence
        let mut item1 = create_test_predicate_alias();
        item1.alias = "same_alias".to_string();
        item1.status = AliasStatus::Active;
        item1.confidence = 0.7;
        item1.predicate_id = "low_confidence".to_string();
        repo.create(item1).await.unwrap();

        let mut item2 = create_test_predicate_alias();
        item2.base.id = RecordId::new_v4(TableName::PredicateAlias);
        item2.alias = "same_alias".to_string();
        item2.status = AliasStatus::Active;
        item2.confidence = 0.95;
        item2.predicate_id = "high_confidence".to_string();
        repo.create(item2).await.unwrap();

        // Should return the highest confidence one
        let found = repo
            .find_active_by_raw("test_space", "same_alias")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().predicate_id, "high_confidence");
    }
}
