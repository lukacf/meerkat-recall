//! `SpaceConfig` repository implementation for `SurrealDB`.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::SpaceConfig;
use elephant_types::RecordId;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

const SPACE_CONFIG_PROJECTION: &str = r"
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
    embedding_provider,
    embedding_model,
    embedding_dims,
    purpose,
    skill
";

/// Database-only proxy for `SpaceConfig` to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct SpaceConfigDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<elephant_types::Link<elephant_types::records::entity::Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    embedding_provider: Option<String>,
    embedding_model: Option<String>,
    embedding_dims: Option<f64>,
    purpose: Option<String>,
    skill: Option<String>,
}

impl TryFrom<SpaceConfigDb> for SpaceConfig {
    type Error = StorageError;

    fn try_from(db: SpaceConfigDb) -> Result<Self, Self::Error> {
        let embedding_dims = db
            .embedding_dims
            .ok_or_else(|| StorageError::QueryError("Missing embedding_dims".to_string()))?;
        let rounded = embedding_dims.round();
        if (embedding_dims - rounded).abs() > f64::EPSILON
            || rounded < f64::from(i32::MIN)
            || rounded > f64::from(i32::MAX)
        {
            return Err(StorageError::QueryError(format!(
                "Invalid embedding_dims value: {embedding_dims}"
            )));
        }
        #[allow(clippy::cast_possible_truncation)]
        let embedding_dims = rounded as i32;

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
            embedding_provider: db.embedding_provider.ok_or_else(|| {
                StorageError::QueryError("Missing embedding_provider".to_string())
            })?,
            embedding_model: db
                .embedding_model
                .ok_or_else(|| StorageError::QueryError("Missing embedding_model".to_string()))?,
            embedding_dims,
            purpose: db.purpose,
            skill: db.skill,
        })
    }
}

/// `SpaceConfig` repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct SpaceConfigRepository {
    client: StorageClient,
}

impl SpaceConfigRepository {
    /// Creates a new space config repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Gets the space config for a given space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_by_space_id(&self, space_id: &str) -> StorageResult<Option<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            let mut configs = store.scan::<SpaceConfig>().await?;
            configs.retain(|config| {
                config.base.space_id == space_id && config.base.deleted_at.is_none()
            });
            configs.sort_by_key(|config| config.base.id.to_string());
            return Ok(configs.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE space_id = $space_id AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse space_config: {e}")))?;

        configs
            .into_iter()
            .next()
            .map(SpaceConfig::try_from)
            .transpose()
    }

    /// Lists all space configs across all spaces (admin use).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn list_all(
        &self,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            let mut configs = store.scan::<SpaceConfig>().await?;
            configs.retain(|config| config.base.deleted_at.is_none());
            configs.sort_by_key(|config| config.base.id.to_string());
            if let Some(cursor) = cursor {
                let cursor_id = cursor.to_string();
                configs.retain(|config| config.base.id.to_string() > cursor_id);
            }
            if configs.len() > limit {
                configs.truncate(limit);
            }
            return Ok(configs);
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE deleted_at IS NONE ORDER BY id LIMIT {limit}"
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
                },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse space_config list: {e}"))
        })?;

        configs.into_iter().map(SpaceConfig::try_from).collect()
    }
}

#[async_trait]
impl Repository<SpaceConfig> for SpaceConfigRepository {
    async fn create(&self, record: SpaceConfig) -> StorageResult<SpaceConfig> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let duplicate = mutation
                        .scan::<SpaceConfig>()?
                        .into_iter()
                        .any(|existing| existing.base.space_id == record.base.space_id);
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate space_config for space {}",
                            record.base.space_id
                        )));
                    }

                    Ok(mutation.create(record.clone())?)
                })
                .await;
        }

        // Checked single round-trip: statement errors (including duplicate ids)
        // are classified via classify_query_error inside query_bind_checked, so
        // a duplicate CREATE surfaces as StorageError::Conflict instead of
        // silently returning the pre-existing row.
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {SPACE_CONFIG_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::space_config_to_value(&record)?,
                },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created space_config: {e}"))
        })?;

        let config_db = configs.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but space_config not found".to_string())
        })?;

        SpaceConfig::try_from(config_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<SpaceConfig>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<SpaceConfig>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {SPACE_CONFIG_PROJECTION} FROM space_config WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse space_config: {e}")))?;

        Ok(configs
            .into_iter()
            .next()
            .map(SpaceConfig::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: SpaceConfig,
    ) -> StorageResult<SpaceConfig> {
        let mut updated = record;
        updated.base.updated_at = Utc::now();
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        updated.base.space_id.clone_from(&target_space_id);
        updated.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(_existing) =
                        mutation.get::<SpaceConfig>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };

                    let duplicate = mutation.scan::<SpaceConfig>()?.into_iter().any(|item| {
                        item.base.space_id == target_space_id && item.base.id != target_id
                    });
                    if duplicate {
                        return Err(StorageError::Conflict(format!(
                            "Duplicate space_config for space {target_space_id}"
                        )));
                    }

                    Ok(mutation.replace(updated.clone())?)
                })
                .await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id CONTENT $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {SPACE_CONFIG_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::space_config_to_value(&updated)?,
                },
            )
            .await?;

        let configs: Vec<SpaceConfigDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated space_config: {e}"))
        })?;

        let config_db = configs
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("SpaceConfig not found: {id}")))?;

        SpaceConfig::try_from(config_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<SpaceConfig>(space_id, id).await;
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
        _space_id: &str,
        _limit: usize,
        _cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SpaceConfig>> {
        Err(StorageError::QueryError(
            "List not supported for space_config".to_string(),
        ))
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
    use crate::{Repository, StorageClient};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{Provenance, Security, SubjectsState};

    async fn setup_embedded_repo() -> SpaceConfigRepository {
        let client = StorageClient::new_embedded()
            .await
            .expect("embedded client");
        SpaceConfigRepository::new(client)
    }

    fn create_test_space_config(space_id: &str) -> SpaceConfig {
        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            space_id.to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        SpaceConfig {
            base,
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        }
    }

    #[tokio::test]
    async fn test_space_config_roundtrip() {
        let client = StorageClient::new_memory().await.expect("mem client");
        let repo = SpaceConfigRepository::new(client);

        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_test".to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let cfg = SpaceConfig {
            base: base.clone(),
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        };

        let created = repo.create(cfg).await.expect("create config");
        assert_eq!(created.embedding_model, "test-model");

        let fetched = repo
            .get_by_space_id("space_test")
            .await
            .expect("get config")
            .expect("missing config");
        assert_eq!(fetched.embedding_dims, 3);
    }

    #[tokio::test]
    async fn test_space_config_purpose_roundtrip() {
        let client = StorageClient::new_memory().await.expect("mem client");
        let repo = SpaceConfigRepository::new(client);

        let mut base = elephant_types::records::base::BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_purpose_test".to_string(),
            "system".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let cfg = SpaceConfig {
            base,
            embedding_provider: "fake".to_string(),
            embedding_model: "test-model".to_string(),
            embedding_dims: 3,
            purpose: Some("Track project relationships".to_string()),
            skill: None,
        };

        let created = repo.create(cfg).await.expect("create config");
        assert_eq!(
            created.purpose,
            Some("Track project relationships".to_string())
        );

        let fetched = repo
            .get_by_space_id("space_purpose_test")
            .await
            .expect("get config")
            .expect("missing config");
        assert_eq!(
            fetched.purpose,
            Some("Track project relationships".to_string())
        );
    }

    #[tokio::test]
    async fn test_embedded_create_rejects_duplicate_space_id() {
        let repo = setup_embedded_repo().await;

        repo.create(create_test_space_config("space_test"))
            .await
            .expect("create first config");
        let err = repo
            .create(create_test_space_config("space_test"))
            .await
            .expect_err("duplicate space_id should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate space_config"))
        );
    }

    #[tokio::test]
    async fn test_embedded_store_rejects_duplicate_space_id_direct_insert() {
        let repo = setup_embedded_repo().await;
        repo.create(create_test_space_config("space_a"))
            .await
            .expect("create first config");

        let second = create_test_space_config("space_a");
        let err = repo
            .client
            .embedded_store()
            .expect("embedded store")
            .create(second.clone())
            .await
            .expect_err("direct duplicate insert should conflict");
        assert!(
            matches!(err, StorageError::Conflict(msg) if msg.contains("Duplicate space_config"))
        );
    }
}
