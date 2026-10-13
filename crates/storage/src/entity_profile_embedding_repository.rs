//! Derived entity vectors partitioned by complete approved profile identity.
//!
//! These rows are separate from canonical entities and never replace their
//! metadata. Staging a profile leaves the active profile's vectors intact.

use elephant_types::records::{Entity, SpaceConfig};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

use crate::backend::{StorageBackend, SurrealStorageBackend};
use crate::{StorageClient, StorageError, StorageResult};

#[derive(Deserialize)]
struct HashDb {
    entity_id: String,
    text_hash: String,
}

#[derive(Deserialize)]
struct VectorDb {
    entity_id: String,
    vector: Vec<f32>,
    text_hash: String,
    current_display_name: String,
    current_aliases: Vec<String>,
    current_summary: Option<String>,
}

/// Canonical text preprocessing shared by embedding/reindex and stale-input checks.
#[must_use]
pub fn entity_input_text(entity: &Entity) -> String {
    entity_input_text_fields(
        &entity.display_name,
        &entity.aliases,
        entity.summary.as_deref(),
    )
}

fn entity_input_text_fields(
    display_name: &str,
    aliases: &[String],
    summary: Option<&str>,
) -> String {
    let mut text = display_name.trim().to_string();
    if !aliases.is_empty() {
        text.push('\n');
        text.push_str(&aliases.join(", "));
    }
    if let Some(summary) = summary.map(str::trim).filter(|text| !text.is_empty()) {
        text.push('\n');
        text.push_str(summary);
    }
    text
}

/// SHA256 of the exact canonical input used for entity embedding staleness checks.
#[must_use]
pub fn entity_input_hash(entity: &Entity) -> String {
    entity_input_hash_fields(
        &entity.display_name,
        &entity.aliases,
        entity.summary.as_deref(),
    )
}

fn entity_input_hash_fields(
    display_name: &str,
    aliases: &[String],
    summary: Option<&str>,
) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::digest(entity_input_text_fields(display_name, aliases, summary).as_bytes())
    )
}

/// Derived vector table; rows are partitioned by the complete approved profile.
pub const ENTITY_PROFILE_EMBEDDING_TABLE: &str = "entity_profile_embedding";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EntityProfileVector {
    id: String,
    space_id: String,
    entity_id: String,
    model: String,
    text_hash: String,
    vector: Vec<f32>,
    source_refs: Vec<String>,
    entity_version: chrono::DateTime<chrono::Utc>,
}

/// Repository for derived entity vectors with current input and profile guards.
#[derive(Clone)]
pub struct EntityProfileEmbeddingRepository<B: StorageBackend = SurrealStorageBackend> {
    backend: B,
}

impl EntityProfileEmbeddingRepository {
    /// Use the managed client so vector writes retain storage admission and durability.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self::new_with_backend(SurrealStorageBackend::new(client))
    }
}

impl<B: StorageBackend> EntityProfileEmbeddingRepository<B> {
    /// Use an explicit backend for the same guarded vector contract.
    #[must_use]
    pub const fn new_with_backend(backend: B) -> Self {
        Self { backend }
    }

    /// Resolve current configuration through the same backend as vector storage.
    pub async fn current_config(&self, space: &str) -> StorageResult<SpaceConfig> {
        if let Some(store) = self.backend.embedded_store() {
            return store
                .scan::<SpaceConfig>()
                .await?
                .into_iter()
                .find(|config| config.base.space_id == space && config.base.deleted_at.is_none())
                .ok_or_else(|| StorageError::NotFound("entity vector space configuration".into()));
        }
        let mut response = self.backend.query_bind(
            &format!("SELECT {} FROM space_config WHERE space_id = $space AND deleted_at IS NONE LIMIT 1", crate::space_config_repository::SPACE_CONFIG_PROJECTION),
            crate::bindings! { "space" => space },
        ).await?;
        let rows: Vec<crate::space_config_repository::SpaceConfigDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound("entity vector space configuration".into()))?
            .try_into()
    }

    /// Persist a vector only while the entity and approval/content snapshot are
    /// current. The config-row write serializes with reindex activation/erasure.
    pub async fn store(
        &self,
        config: &SpaceConfig,
        entity: &Entity,
        model: &str,
        text_hash: &str,
        vector: Vec<f32>,
    ) -> StorageResult<()> {
        self.store_inner(config, entity, model, text_hash, vector, None)
            .await
    }

    /// Store a staged vector and check its original queue claim atomically.
    ///
    /// # Errors
    /// Rejects stale claims, changed entity/configuration snapshots and vectors
    /// outside the approved staged profile.
    pub async fn store_with_attempt(
        &self,
        config: &SpaceConfig,
        entity: &Entity,
        model: &str,
        text_hash: &str,
        vector: Vec<f32>,
        attempt: &elephant_types::publication::AttemptFence,
    ) -> StorageResult<()> {
        self.store_inner(config, entity, model, text_hash, vector, Some(attempt))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::too_many_lines,
        reason = "Keep profile approval, exact entity preimage and optional reindex attempt checks next to their atomic vector write on both backends."
    )]
    async fn store_inner(
        &self,
        config: &SpaceConfig,
        entity: &Entity,
        model: &str,
        text_hash: &str,
        vector: Vec<f32>,
        attempt: Option<&elephant_types::publication::AttemptFence>,
    ) -> StorageResult<()> {
        if config.base.space_id != entity.base.space_id
            || entity.base.deleted_at.is_some()
            || text_hash != entity_input_hash(entity)
        {
            return Err(StorageError::Conflict(
                "entity vector belongs to another or deleted space".into(),
            ));
        }
        let identity: elephant_types::provider_binding::VectorPartition =
            serde_json::from_str(model.strip_prefix("recall:").ok_or_else(|| {
                StorageError::Conflict("approved entity embedding identity required".into())
            })?)
            .map_err(|_| StorageError::Conflict("invalid entity embedding identity".into()))?;
        let processing = config.processing.as_ref().ok_or_else(|| {
            StorageError::Conflict("provider_authorization: entity approval missing".into())
        })?;
        if attempt.is_some()
            && processing
                .pending_reindex
                .as_ref()
                .map(|pending| pending.to_profile_id.as_str())
                != Some(identity.profile_id.as_str())
        {
            return Err(StorageError::Conflict(
                "reindex vector does not target the staged profile".into(),
            ));
        }
        let operation = if attempt.is_none()
            && processing.active_embedding_profile_id.as_deref() == Some(&identity.profile_id)
        {
            elephant_types::provider_binding::ModelOperation::DocumentEmbedding
        } else {
            elephant_types::provider_binding::ModelOperation::ReindexEmbedding
        };
        let resolved = elephant_types::provider_binding::ResolvedSpaceConfig {
            token: elephant_types::provider_binding::SpaceConfigToken {
                space_id: config.base.space_id.clone(),
                config_id: config.base.id.to_string(),
                generation: processing.generation,
                policy_id: processing.policy_id.clone(),
            },
            processing: processing.clone(),
            purpose: config.purpose.clone(),
            skill: config.skill.clone(),
        };
        let approved = resolved
            .authorize(&entity.base.space_id, operation, Some(&identity.profile_id))
            .map_err(|error| StorageError::Conflict(format!("provider_authorization: {error}")))?;
        if approved
            .vector_partition()
            .map_err(|error| StorageError::Conflict(error.to_string()))?
            != identity
            || vector.len() != identity.dimensions
            || vector.iter().any(|value| !value.is_finite())
        {
            return Err(StorageError::Conflict(
                "entity vector does not match approved profile".into(),
            ));
        }
        let uuid = uuid::Uuid::new_v5(&entity.base.id.id(), model.as_bytes());
        let row = EntityProfileVector {
            id: format!("{ENTITY_PROFILE_EMBEDDING_TABLE}:{uuid}"),
            space_id: entity.base.space_id.clone(),
            entity_id: entity.base.id.to_string(),
            model: model.into(),
            text_hash: text_hash.into(),
            vector,
            entity_version: entity.base.updated_at,
            source_refs: entity
                .base
                .provenance
                .sources
                .iter()
                .map(ToString::to_string)
                .collect(),
        };
        if let Some(store) = self.backend.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(attempt) = attempt {
                        crate::reindex_guard::check(tx, config, attempt)?;
                    }
                    let current_config = tx
                        .get::<SpaceConfig>(&config.base.space_id, &config.base.id, false)?
                        .ok_or_else(|| {
                            StorageError::Conflict("entity vector space removed".into())
                        })?;
                    let current_entity = tx
                        .get::<Entity>(&entity.base.space_id, &entity.base.id, false)?
                        .ok_or_else(|| {
                            StorageError::Conflict("entity vector source removed".into())
                        })?;
                    if current_config.processing != config.processing || current_entity != *entity {
                        return Err(StorageError::Conflict(
                            "entity embedding inputs changed".into(),
                        ));
                    }
                    tx.upsert_extension_row(
                        ENTITY_PROFILE_EMBEDDING_TABLE,
                        serde_json::to_value(&row)
                            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
                    )?;
                    Ok(())
                })
                .await;
        }
        let mut row_value = serde_json::to_value(&row)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        if let Some(object) = row_value.as_object_mut() {
            object.remove("id");
        }
        let mut bindings = crate::bindings! {
            "config_id" => crate::surreal::thing_from_record_id(&config.base.id),
            "processing" => crate::surreal::stored_native_value(crate::surreal::json_to_surreal(serde_json::to_value(processing).map_err(|error| StorageError::SerializationError(error.to_string()))?)?),
            "entity_id" => crate::surreal::thing_from_record_id(&entity.base.id),
            "space" => entity.base.space_id.clone(),
            "entity_version" => crate::surreal::datetime_from_chrono(entity.base.updated_at),
            "display_name" => entity.display_name.clone(),
            "aliases" => entity.aliases.clone(),
            "summary" => entity.summary.clone().map_or(surrealdb::sql::Value::None, surrealdb::sql::Value::from),
            "security" => crate::surreal::security_to_value(&entity.base.security),
            "id" => surrealdb::sql::Thing::from((ENTITY_PROFILE_EMBEDDING_TABLE, surrealdb::sql::Id::Uuid(uuid.into()))),
            "row" => crate::surreal::json_to_surreal(row_value)?,
        };
        let mut sql = "BEGIN TRANSACTION;\n".to_owned();
        if let Some(attempt) = attempt {
            crate::reindex_guard::append(&mut sql, &mut bindings, config, attempt)?;
        }
        sql.push_str("             LET $config_guard = (UPDATE ONLY $config_id SET processing = processing WHERE processing = $processing AND deleted_at IS NONE RETURN AFTER);
             IF $config_guard = NONE { THROW 'entity vector approval/content changed'; };
             LET $current = (SELECT * FROM ONLY $entity_id WHERE space_id = $space AND deleted_at IS NONE);
             IF $current = NONE OR $current.updated_at != $entity_version OR $current.display_name != $display_name OR $current.aliases != $aliases OR $current.summary != $summary OR $current.security != $security { THROW 'entity embedding inputs changed'; };
             UPSERT $id CONTENT $row;
             UPDATE $id SET entity = $entity_id, entity_version = $entity_version;
             COMMIT TRANSACTION;");
        self.backend.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }

    /// Profile metadata used by the sweep to walk past already-covered prefixes.
    pub async fn text_hashes(
        &self,
        space: &str,
        model: &str,
        dims: usize,
    ) -> StorageResult<std::collections::BTreeMap<String, String>> {
        if let Some(store) = self.backend.embedded_store() {
            return store
                .mutate(|tx| {
                    let mut hashes = std::collections::BTreeMap::new();
                    for (_, value) in tx.scan_extension_rows(ENTITY_PROFILE_EMBEDDING_TABLE)? {
                        let row: EntityProfileVector = serde_json::from_value(value)
                            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
                        if row.space_id == space && row.model == model && row.vector.len() == dims {
                            hashes.insert(row.entity_id, row.text_hash);
                        }
                    }
                    Ok(hashes)
                })
                .await;
        }
        let mut response = self.backend.query_bind(
            "SELECT entity_id, text_hash FROM entity_profile_embedding WHERE space_id = $space AND model = $model AND array::len(vector) = $dims AND entity.deleted_at IS NONE AND entity.updated_at = entity_version",
            crate::bindings! { "space" => space, "model" => model, "dims" => i64::try_from(dims).map_err(|_| StorageError::DataIntegrityError("dimensions overflow".into()))? },
        ).await?;
        let rows: Vec<HashDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.entity_id, row.text_hash))
            .collect())
    }

    /// Load only exact-profile vectors with still-live entity records. No vector
    /// snapshot survives the current storage admission or restore incarnation.
    pub async fn load(
        &self,
        space: &str,
        model: &str,
        dims: usize,
    ) -> StorageResult<Vec<(RecordId, Vec<f32>)>> {
        let config = self.current_config(space).await?;
        let processing = config.processing.as_ref().ok_or_else(|| {
            StorageError::Conflict("provider_authorization: entity query approval missing".into())
        })?;
        let resolved = elephant_types::provider_binding::ResolvedSpaceConfig {
            token: elephant_types::provider_binding::SpaceConfigToken {
                space_id: space.into(),
                config_id: config.base.id.to_string(),
                generation: processing.generation,
                policy_id: processing.policy_id.clone(),
            },
            processing: processing.clone(),
            purpose: config.purpose.clone(),
            skill: config.skill.clone(),
        };
        let current = resolved
            .authorize(
                space,
                elephant_types::provider_binding::ModelOperation::QueryEmbedding,
                None,
            )
            .map_err(|error| StorageError::Conflict(format!("provider_authorization: {error}")))?;
        let policy = elephant_embeddings::governed::policy_for_binding(&current)
            .map_err(|error| StorageError::Conflict(error.to_string()))?;
        if policy.model != model || policy.dims != dims {
            return Err(StorageError::Conflict(
                "provider_authorization: stale query profile".into(),
            ));
        }
        if let Some(store) = self.backend.embedded_store() {
            return store
                .mutate(|tx| {
                    let current = tx
                        .get::<SpaceConfig>(space, &config.base.id, false)?
                        .ok_or_else(|| StorageError::Conflict("query space removed".into()))?;
                    if current.processing != config.processing {
                        return Err(StorageError::Conflict(
                            "provider_authorization: query approval changed".into(),
                        ));
                    }
                    let mut rows = Vec::new();
                    for (_, value) in tx.scan_extension_rows(ENTITY_PROFILE_EMBEDDING_TABLE)? {
                        let row: EntityProfileVector = serde_json::from_value(value)
                            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
                        if row.space_id != space || row.model != model || row.vector.len() != dims {
                            continue;
                        }
                        let entity_id: RecordId = row.entity_id.parse().map_err(|error| {
                            StorageError::DataIntegrityError(format!(
                                "invalid entity vector ID: {error}"
                            ))
                        })?;
                        if tx
                            .get::<Entity>(space, &entity_id, false)?
                            .is_some_and(|entity| {
                                entity.status
                                    != elephant_types::records::entity::EntityStatus::Merged
                                    && !crate::entity_repository::is_source_inventory_entity(
                                        &entity,
                                    )
                                    && entity_input_hash(&entity) == row.text_hash
                            })
                        {
                            rows.push((entity_id, row.vector));
                        }
                    }
                    Ok(rows)
                })
                .await;
        }
        // A trusted content update can preserve updated_at. Read the current
        // preimage with each vector, then use the same canonical hash as store
        // and the embedded backend; timestamps alone cannot prove freshness.
        let mut response = self.backend.query_bind(
            "SELECT entity_id, vector, text_hash, entity.display_name AS current_display_name, IF entity.aliases IS NOT NONE THEN entity.aliases ELSE [] END AS current_aliases, entity.summary AS current_summary FROM entity_profile_embedding WHERE space_id = $space AND model = $model AND array::len(vector) = $dims AND entity.deleted_at IS NONE AND entity.space_id = $space AND entity.updated_at = entity_version AND entity.status != 'merged'",
            crate::bindings! { "space" => space, "model" => model, "dims" => i64::try_from(dims).map_err(|_| StorageError::DataIntegrityError("dimensions overflow".into()))? },
        ).await?;
        let rows: Vec<VectorDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .filter(|row| {
                entity_input_hash_fields(
                    &row.current_display_name,
                    &row.current_aliases,
                    row.current_summary.as_deref(),
                ) == row.text_hash
            })
            .map(|row| {
                Ok((
                    row.entity_id.parse().map_err(|error| {
                        StorageError::DataIntegrityError(format!(
                            "invalid entity vector ID: {error}"
                        ))
                    })?,
                    row.vector,
                ))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::float_cmp)]
    use super::*;
    use crate::entity_repository::EntityVectorCache;
    use crate::{EntityRepository, Repository, SpaceConfigRepository};
    use elephant_embeddings::governed::policy_for_binding;
    use elephant_types::provider_binding::*;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::entity::EntityType;
    use elephant_types::TableName;
    use std::collections::BTreeMap;

    fn config(space: &str) -> SpaceConfig {
        let connection = ProviderConnection {
            id: "account".into(),
            provider: "openai".into(),
            account_id: "tenant-a".into(),
            endpoint: "https://model.example.test/v1".into(),
            credential_ref: Some("UNUSED_TEST_REFERENCE".into()),
            credential_generation: 1,
            revoked: false,
        };
        let profile = ModelProfile {
            id: "v1".into(),
            connection_id: "account".into(),
            model: "embed".into(),
            model_version: "1".into(),
            dimensions: Some(2),
            preprocessing: "text-v1".into(),
        };
        let mut target = profile.clone();
        target.id = "v2".into();
        target.model_version = "2".into();
        SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                "test".into(),
            ),
            embedding_provider: "openai".into(),
            embedding_model: "embed".into(),
            embedding_dims: 2,
            purpose: None,
            skill: None,
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id: "approved".into(),
                connections: BTreeMap::from([("account".into(), connection)]),
                profiles: BTreeMap::from([("v1".into(), profile), ("v2".into(), target)]),
                bindings: BTreeMap::from([
                    (
                        ModelOperation::QueryEmbedding,
                        OperationBinding {
                            id: "query".into(),
                            profile_id: "v1".into(),
                            revoked: false,
                        },
                    ),
                    (
                        ModelOperation::DocumentEmbedding,
                        OperationBinding {
                            id: "document".into(),
                            profile_id: "v1".into(),
                            revoked: false,
                        },
                    ),
                    (
                        ModelOperation::ReindexEmbedding,
                        OperationBinding {
                            id: "reindex".into(),
                            profile_id: "v2".into(),
                            revoked: false,
                        },
                    ),
                ]),
                active_embedding_profile_id: Some("v1".into()),
                pending_reindex: None,
            }),
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep configs, entities, vectors in the original fixture scope through assertions and cleanup."
    )]
    async fn exact_profile_queries_recheck_revocation_and_live_entities_on_both_backends() {
        for client in [
            StorageClient::new_memory().await.unwrap(),
            StorageClient::new_embedded().await.unwrap(),
        ] {
            if client.embedded_store().is_none() {
                client
                    .query_bind_checked(
                        include_str!("../../migrations/sql/033_space_processing.surql"),
                        crate::bindings! {},
                    )
                    .await
                    .unwrap();
            }
            let configs = SpaceConfigRepository::new(client.clone());
            configs.create(config("profiles-a")).await.unwrap();
            configs.create(config("profiles-b")).await.unwrap();
            let entities = EntityRepository::new(client.clone());
            let entity = entities
                .create(Entity::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Entity),
                        "profiles-a".into(),
                        "test".into(),
                    ),
                    EntityType::new("person"),
                    "Ada Example".into(),
                ))
                .await
                .unwrap();
            let current = configs
                .get_by_space_id("profiles-a")
                .await
                .unwrap()
                .unwrap();
            let vectors = EntityProfileEmbeddingRepository::new(client.clone());
            let active = policy_for_binding(
                &configs
                    .authorize_model_operation(
                        "profiles-a",
                        ModelOperation::DocumentEmbedding,
                        None,
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
            vectors
                .store(
                    &current,
                    &entity,
                    &active.model,
                    &entity_input_hash(&entity),
                    vec![1.0, 0.0],
                )
                .await
                .unwrap();
            assert!(entities
                .list_embedding_candidates_for_policy("profiles-a", &active.model, 2, 10)
                .await
                .unwrap()
                .is_empty());
            let cache = EntityVectorCache::new();
            let hits = entities
                .vector_search_entities_for_policy(&cache, "profiles-a", &active, &[1.0, 0.0], 10)
                .await
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert!(entities
                .vector_search_entities_for_policy(&cache, "profiles-b", &active, &[1.0, 0.0], 10)
                .await
                .is_err());
            let staged = configs
                .start_embedding_reindex(&current, "v2")
                .await
                .unwrap();
            let target = policy_for_binding(
                &configs
                    .authorize_model_operation("profiles-a", ModelOperation::ReindexEmbedding, None)
                    .await
                    .unwrap(),
            )
            .unwrap();
            vectors
                .store(
                    &staged,
                    &entity,
                    &target.model,
                    &entity_input_hash(&entity),
                    vec![0.0, 1.0],
                )
                .await
                .unwrap();
            assert_eq!(
                vectors.load("profiles-a", &active.model, 2).await.unwrap()[0].1,
                vec![1.0, 0.0]
            );
            assert!(
                vectors.load("profiles-a", &target.model, 2).await.is_err(),
                "staged profile is not query authority"
            );
            let entity_phase = configs
                .record_embedding_reindex_page(&staged, None, 0, true)
                .await
                .unwrap();
            let completed = configs
                .record_embedding_reindex_page(&entity_phase, None, 1, true)
                .await
                .unwrap();
            let reindex_id = completed
                .processing
                .as_ref()
                .unwrap()
                .pending_reindex
                .as_ref()
                .unwrap()
                .id
                .clone();
            let activated = configs
                .activate_embedding_reindex(&completed, &reindex_id)
                .await
                .unwrap();
            assert_eq!(
                vectors.load("profiles-a", &target.model, 2).await.unwrap()[0].1,
                vec![0.0, 1.0]
            );
            assert!(vectors.load("profiles-a", &active.model, 2).await.is_err());
            let mut revoked = activated.clone();
            let processing = revoked.processing.as_mut().unwrap();
            processing.generation += 1;
            processing
                .bindings
                .get_mut(&ModelOperation::QueryEmbedding)
                .unwrap()
                .revoked = true;
            let revoked = configs
                .replace_authorized(&activated, revoked)
                .await
                .unwrap();
            assert!(entities
                .vector_search_entities_for_policy(&cache, "profiles-a", &target, &[0.0, 1.0], 10)
                .await
                .is_err());
            let mut restored = revoked.clone();
            let processing = restored.processing.as_mut().unwrap();
            processing.generation += 1;
            processing
                .bindings
                .get_mut(&ModelOperation::QueryEmbedding)
                .unwrap()
                .revoked = false;
            configs
                .replace_authorized(&revoked, restored)
                .await
                .unwrap();
            entities
                .hard_delete("profiles-a", &entity.base.id)
                .await
                .unwrap();
            assert!(entities
                .vector_search_entities_for_policy(&cache, "profiles-a", &target, &[0.0, 1.0], 10)
                .await
                .unwrap()
                .is_empty());
            assert!(vectors
                .store(
                    &staged,
                    &entity,
                    &target.model,
                    &entity_input_hash(&entity),
                    vec![0.0, 1.0]
                )
                .await
                .is_err());
        }
    }
}
