//! Predicate registry repository implementation for `SurrealDB`.
//!
//! Per spec §10, predicate_registry stores canonical predicate definitions.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use surrealdb::sql::Value;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::predicate::{PredicateRegistry, PredicateSemantics, SemanticsStatus};
use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

pub(crate) const PREDICATE_REGISTRY_PROJECTION: &str = r"
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
    predicate_id,
    description,
    status,
    semantics,
    semantics_status,
    first_seen_at,
    usage_count,
    last_used_at
";

/// Database-only proxy for PredicateRegistry.
#[derive(Deserialize)]
pub(crate) struct PredicateRegistryDb {
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
    predicate_id: Option<String>,
    description: Option<String>,
    status: Option<String>,
    semantics: Option<PredicateSemantics>,
    semantics_status: Option<SemanticsStatus>,
    first_seen_at: Option<chrono::DateTime<Utc>>,
    usage_count: Option<u64>,
    last_used_at: Option<chrono::DateTime<Utc>>,
}

impl TryFrom<PredicateRegistryDb> for PredicateRegistry {
    type Error = StorageError;

    fn try_from(db: PredicateRegistryDb) -> Result<Self, Self::Error> {
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
            predicate_id: required_field(db.predicate_id, "predicate_id")?,
            description: db.description,
            status: required_field(db.status, "status")?,
            semantics: db.semantics,
            semantics_status: required_field(db.semantics_status, "semantics_status")?,
            first_seen_at: db.first_seen_at,
            usage_count: required_field(db.usage_count, "usage_count")?,
            last_used_at: db.last_used_at,
        })
    }
}

/// Predicate registry repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct PredicateRegistryRepository {
    client: StorageClient,
}

impl PredicateRegistryRepository {
    /// Creates a new predicate registry repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds a predicate by its canonical predicate_id string.
    ///
    /// Per spec §10.2: Looks up a predicate by its unique identifier.
    ///
    /// # Arguments
    /// * `space_id` - The space to search in
    /// * `predicate_id` - The canonical predicate identifier string
    ///
    /// # Returns
    /// The predicate registry entry if found, None otherwise.
    ///
    /// # Errors
    /// Returns `StorageError::QueryError` if the database query fails.
    pub async fn find_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> StorageResult<Option<PredicateRegistry>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<PredicateRegistry>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.predicate_id == predicate_id
            });
            items.sort_by_key(|item| item.base.id.to_string());
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry \
                     WHERE space_id = $space_id \
                     AND predicate_id = $predicate_id \
                     AND deleted_at IS NONE \
                     LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "predicate_id" => predicate_id
                },
            )
            .await?;

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to find predicate by predicate_id: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(PredicateRegistry::try_from)
            .transpose()
    }

    /// Upserts semantics for a predicate without overwriting operator configuration.
    ///
    /// If the registry row exists with `Configured` semantics it is returned
    /// unchanged; any other status gets the new semantics and status. Missing
    /// rows are created. The provenance note becomes the description when no
    /// description exists yet.
    ///
    /// # Errors
    /// Returns `StorageError::QueryError` if the database query fails.
    pub async fn upsert_semantics(
        &self,
        space_id: &str,
        predicate_id: &str,
        semantics: PredicateSemantics,
        status: SemanticsStatus,
        provenance_note: &str,
    ) -> StorageResult<PredicateRegistry> {
        if let Some(existing) = self.find_by_predicate_id(space_id, predicate_id).await? {
            if existing.semantics_status == SemanticsStatus::Configured {
                return Ok(existing);
            }

            let id = existing.base.id.clone();
            let mut updated = existing;
            updated.semantics = Some(semantics);
            updated.semantics_status = status;
            if updated.description.is_none() && !provenance_note.is_empty() {
                updated.description = Some(provenance_note.to_string());
            }
            return self.update(space_id, &id, updated).await;
        }

        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, space_id.to_string(), "truth_maintenance".to_string());
        let record = PredicateRegistry {
            base,
            predicate_id: predicate_id.to_string(),
            description: (!provenance_note.is_empty()).then(|| provenance_note.to_string()),
            status: "active".to_string(),
            semantics: Some(semantics),
            semantics_status: status,
            first_seen_at: Some(Utc::now()),
            usage_count: 0,
            last_used_at: None,
        };
        self.create(record).await
    }

    /// Converts a PredicateRegistry to a SurrealDB value for insertion.
    pub(crate) fn predicate_registry_to_value(item: &PredicateRegistry) -> StorageResult<Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("predicate_id".to_string(), item.predicate_id.clone().into());
        obj.insert(
            "description".to_string(),
            item.description.clone().map_or(Value::None, Value::from),
        );
        obj.insert("status".to_string(), item.status.clone().into());
        obj.insert(
            "semantics".to_string(),
            match &item.semantics {
                Some(semantics) => crate::surreal::json_to_surreal(
                    serde_json::to_value(semantics)
                        .map_err(|e| StorageError::SerializationError(e.to_string()))?,
                )?,
                None => Value::None,
            },
        );
        obj.insert(
            "semantics_status".to_string(),
            crate::surreal::enum_to_surreal(&item.semantics_status)?,
        );
        obj.insert(
            "first_seen_at".to_string(),
            item.first_seen_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        obj.insert("usage_count".to_string(), Value::from(item.usage_count));
        obj.insert(
            "last_used_at".to_string(),
            item.last_used_at.map_or(Value::None, |dt| {
                Value::from(crate::surreal::datetime_from_chrono(dt))
            }),
        );
        Ok(Value::Object(obj))
    }
}

#[async_trait]
impl Repository<PredicateRegistry> for PredicateRegistryRepository {
    async fn create(&self, record: PredicateRegistry) -> StorageResult<PredicateRegistry> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate =
                        mutation
                            .scan::<PredicateRegistry>()?
                            .into_iter()
                            .any(|existing| {
                                existing.base.space_id == record.base.space_id
                                    && existing.predicate_id == record.predicate_id
                            });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate predicate_registry predicate_id in space {}",
                            record.base.space_id
                        )));
                    }

                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        let record_value = Self::predicate_registry_to_value(&record)?;
        let response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await
            .map_err(|e| {
                StorageError::classify_query_error(format!(
                    "Failed to create predicate_registry: {e}"
                ))
            })?;
        response.check().map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create predicate_registry: {e}"))
        })?;

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry WHERE id = $id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created predicate_registry: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError(
                "Create succeeded but predicate_registry not found".to_string(),
            )
        })?;

        PredicateRegistry::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateRegistry>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<PredicateRegistry>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_registry: {e}"))
        })?;

        Ok(items
            .into_iter()
            .next()
            .map(PredicateRegistry::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: PredicateRegistry,
    ) -> StorageResult<PredicateRegistry> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut normalized = record;
        normalized.base.space_id.clone_from(&target_space_id);
        normalized.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<PredicateRegistry>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate = mutation
                        .scan::<PredicateRegistry>()?
                        .into_iter()
                        .any(|item| {
                            item.base.space_id == target_space_id
                                && item.predicate_id == normalized.predicate_id
                                && item.base.id != target_id
                        });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate predicate_registry predicate_id in space {target_space_id}"
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {PREDICATE_REGISTRY_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::predicate_registry_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated predicate_registry: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("PredicateRegistry not found in space: {id}"))
        })?;

        PredicateRegistry::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<PredicateRegistry>(space_id, id).await;
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
    ) -> StorageResult<Vec<PredicateRegistry>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<PredicateRegistry>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_registries: {e}"))
        })?;

        items.into_iter().map(PredicateRegistry::try_from).collect()
    }
}

impl PredicateRegistryRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<PredicateRegistry>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<PredicateRegistry>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {PREDICATE_REGISTRY_PROJECTION} FROM predicate_registry WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<PredicateRegistryDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse predicate_registry: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(PredicateRegistry::try_from)
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80900);

    const PREDICATE_REGISTRY_SCHEMA: &str = r"
        DEFINE TABLE predicate_registry SCHEMALESS;
    ";

    async fn setup_test_repo() -> PredicateRegistryRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("predicate_registry_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(PREDICATE_REGISTRY_SCHEMA).await.unwrap();

        PredicateRegistryRepository::new(client)
    }

    async fn setup_embedded_repo() -> PredicateRegistryRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        PredicateRegistryRepository::new(client)
    }

    fn create_test_predicate_registry() -> PredicateRegistry {
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        PredicateRegistry {
            base,
            predicate_id: "works_at".to_string(),
            description: Some("Employment relationship".to_string()),
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::Default,
            first_seen_at: Some(Utc::now()),
            usage_count: 0,
            last_used_at: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_predicate_registry() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.predicate_id, "works_at");
        assert_eq!(created.status, "active");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_predicate_registry() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();
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
    async fn test_predicate_registry_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_predicate_registry();
        item.usage_count = 100;
        item.semantics = Some(PredicateSemantics::default());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.usage_count, 100);
        assert!(created.semantics.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_predicate_registry() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = "deprecated".to_string();
        updated.usage_count = 50;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, "deprecated");
        assert_eq!(result.usage_count, 50);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_create_rejects_duplicate_predicate_id() {
        let repo = setup_embedded_repo().await;
        let first = create_test_predicate_registry();
        let mut second = create_test_predicate_registry();
        second.base.id = RecordId::new_v4(TableName::PredicateRegistry);

        repo.create(first).await.unwrap();
        let err = repo
            .create(second)
            .await
            .expect_err("duplicate predicate_id should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate predicate_registry predicate_id"))
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_update_rejects_duplicate_predicate_id() {
        let repo = setup_embedded_repo().await;
        let first = repo.create(create_test_predicate_registry()).await.unwrap();
        let mut second = create_test_predicate_registry();
        second.base.id = RecordId::new_v4(TableName::PredicateRegistry);
        second.predicate_id = "manages".to_string();
        let second = repo.create(second).await.unwrap();

        let mut updated = second.clone();
        updated.predicate_id = first.predicate_id.clone();

        let err = repo
            .update("test_space", &second.base.id, updated)
            .await
            .expect_err("duplicate predicate_id should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate predicate_registry predicate_id"))
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_predicate_registry() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();
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
    async fn test_find_by_predicate_id() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();

        repo.create(item).await.unwrap();

        // Should find the predicate by its predicate_id string
        let found = repo
            .find_by_predicate_id("test_space", "works_at")
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().predicate_id, "works_at");

        // Should not find non-existent predicate
        let not_found = repo
            .find_by_predicate_id("test_space", "nonexistent")
            .await
            .unwrap();
        assert!(not_found.is_none());
    }

    async fn assert_upsert_semantics_behavior(repo: &PredicateRegistryRepository) {
        use elephant_types::records::predicate::Cardinality;

        // Missing row: creates one with the proposed semantics.
        let mut proposed = PredicateSemantics {
            cardinality: Cardinality::One,
            ..PredicateSemantics::default()
        };
        let created = repo
            .upsert_semantics(
                "test_space",
                "works_at",
                proposed.clone(),
                SemanticsStatus::LlmProposed,
                "proposed by adjudicator",
            )
            .await
            .unwrap();
        assert_eq!(created.predicate_id, "works_at");
        assert_eq!(created.semantics_status, SemanticsStatus::LlmProposed);
        assert_eq!(
            created.semantics.as_ref().map(|s| s.cardinality),
            Some(Cardinality::One)
        );
        assert_eq!(
            created.description,
            Some("proposed by adjudicator".to_string())
        );

        // Existing non-Configured row: semantics are revised.
        proposed.cardinality = Cardinality::Many;
        let revised = repo
            .upsert_semantics(
                "test_space",
                "works_at",
                proposed.clone(),
                SemanticsStatus::LlmProposed,
                "revised by adjudicator",
            )
            .await
            .unwrap();
        assert_eq!(revised.base.id, created.base.id);
        assert_eq!(
            revised.semantics.as_ref().map(|s| s.cardinality),
            Some(Cardinality::Many)
        );
        // Existing description is preserved.
        assert_eq!(
            revised.description,
            Some("proposed by adjudicator".to_string())
        );

        // Configured rows are never overwritten.
        let mut configured = create_test_predicate_registry();
        configured.base.id = RecordId::new_v4(TableName::PredicateRegistry);
        configured.predicate_id = "born_in".to_string();
        configured.semantics = Some(PredicateSemantics {
            cardinality: Cardinality::One,
            ..PredicateSemantics::default()
        });
        configured.semantics_status = SemanticsStatus::Configured;
        repo.create(configured).await.unwrap();

        let untouched = repo
            .upsert_semantics(
                "test_space",
                "born_in",
                PredicateSemantics::default(),
                SemanticsStatus::LlmProposed,
                "should not apply",
            )
            .await
            .unwrap();
        assert_eq!(untouched.semantics_status, SemanticsStatus::Configured);
        assert_eq!(
            untouched.semantics.as_ref().map(|s| s.cardinality),
            Some(Cardinality::One)
        );

        let reloaded = repo
            .find_by_predicate_id("test_space", "born_in")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.semantics_status, SemanticsStatus::Configured);
        assert_eq!(
            reloaded.semantics.as_ref().map(|s| s.cardinality),
            Some(Cardinality::One)
        );
    }

    #[tokio::test]
    async fn test_upsert_semantics() {
        let repo = setup_test_repo().await;
        assert_upsert_semantics_behavior(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_upsert_semantics() {
        let repo = setup_embedded_repo().await;
        assert_upsert_semantics_behavior(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_by_predicate_id_ignores_deleted() {
        let repo = setup_test_repo().await;
        let item = create_test_predicate_registry();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        // Should not find deleted predicate
        let not_found = repo
            .find_by_predicate_id("test_space", "works_at")
            .await
            .unwrap();
        assert!(not_found.is_none());
    }
}
