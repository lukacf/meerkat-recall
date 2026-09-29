//! Entity repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use num_traits::ToPrimitive;
use serde::Deserialize;
use std::str::FromStr;

use elephant_types::records::entity::EntityStatus;
use elephant_types::records::{Entity, Rel};
use elephant_types::{Link, RecordId};

use crate::backend::{StorageBackend, SurrealStorageBackend};
use crate::client::StorageClient;
use crate::embedded::{cosine_similarity, lexical_score};
use crate::error::{StorageError, StorageResult};
use crate::rel_repository::REL_PROJECTION;
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

/// Metadata key carrying Angel protectee attribution on entities.
const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

const ENTITY_PROJECTION: &str = r"
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
    type,
    display_name,
    IF aliases IS NOT NONE THEN aliases ELSE [] END AS aliases,
    summary,
    IF handles IS NOT NONE THEN handles ELSE [] END AS handles,
    IF identity_keys IS NOT NONE THEN identity_keys ELSE [] END AS identity_keys,
    risk_level,
    trust_level,
    known_safe,
    first_seen,
    last_seen,
    status,
    redirect_to,
    metadata
";

/// Search hit for entity lookup with a score.
#[derive(Debug, Clone)]
pub struct EntitySearchHit {
    /// Entity ID.
    pub entity_id: RecordId,
    /// Relevance score.
    pub score: f32,
}

/// Lightweight view of an entity for the embedding sweep.
///
/// Carries the text that gets embedded plus the stored model/hash used to
/// skip unchanged entities. No vectors, so listing candidates stays cheap.
#[derive(Debug, Clone)]
pub struct EntityEmbeddingCandidate {
    /// Entity ID.
    pub entity_id: RecordId,
    /// Display name.
    pub display_name: String,
    /// Aliases.
    pub aliases: Vec<String>,
    /// Summary.
    pub summary: Option<String>,
    /// Model that produced the stored embedding, if any.
    pub embedding_model: Option<String>,
    /// Hash of the text the stored embedding was computed from, if any.
    pub embedding_text_hash: Option<String>,
    /// Dimensions of the stored embedding vector, if any. Participates in
    /// the re-embed gate so a dims change re-embeds even under an unchanged
    /// model string.
    pub embedding_dims: Option<usize>,
}

#[derive(Deserialize)]
struct EntityEmbeddingCandidateDb {
    id: surrealdb::sql::Thing,
    display_name: Option<String>,
    aliases: Option<Vec<String>>,
    summary: Option<String>,
    name_embedding_model: Option<String>,
    name_embedding_text_hash: Option<String>,
    embedding_dims: Option<usize>,
}

impl TryFrom<EntityEmbeddingCandidateDb> for EntityEmbeddingCandidate {
    type Error = StorageError;

    fn try_from(db: EntityEmbeddingCandidateDb) -> Result<Self, Self::Error> {
        Ok(Self {
            entity_id: crate::surreal::record_id_from_thing(&db.id)
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            display_name: db.display_name.unwrap_or_default(),
            aliases: db.aliases.unwrap_or_default(),
            summary: db.summary,
            embedding_model: db.name_embedding_model,
            embedding_text_hash: db.name_embedding_text_hash,
            embedding_dims: db.embedding_dims,
        })
    }
}

#[derive(Deserialize)]
struct EntitySearchHitDb {
    id: surrealdb::sql::Thing,
    score: f64,
}

impl TryFrom<EntitySearchHitDb> for EntitySearchHit {
    type Error = StorageError;

    fn try_from(db: EntitySearchHitDb) -> Result<Self, Self::Error> {
        Ok(Self {
            entity_id: crate::surreal::record_id_from_thing(&db.id)
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            score: db.score.to_f32().ok_or_else(|| {
                StorageError::QueryError(format!("Invalid entity search score: {}", db.score))
            })?,
        })
    }
}

/// Database-only proxy for Entity to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct EntityDb {
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
    #[serde(rename = "type")]
    entity_type: elephant_types::records::entity::EntityType,
    display_name: String,
    aliases: Vec<String>,
    summary: Option<String>,
    handles: Vec<elephant_types::records::entity::Handle>,
    identity_keys: Vec<String>,
    risk_level: Option<f64>,
    trust_level: Option<f64>,
    known_safe: bool,
    first_seen: Option<chrono::DateTime<Utc>>,
    last_seen: Option<chrono::DateTime<Utc>>,
    status: elephant_types::records::entity::EntityStatus,
    redirect_to: Option<surrealdb::sql::Thing>,
    #[serde(default = "default_metadata")]
    metadata: serde_json::Value,
}

impl TryFrom<EntityDb> for Entity {
    type Error = StorageError;

    fn try_from(db: EntityDb) -> Result<Self, Self::Error> {
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
            entity_type: db.entity_type,
            display_name: db.display_name,
            aliases: db.aliases,
            summary: db.summary,
            handles: db.handles,
            identity_keys: db.identity_keys,
            risk_level: db.risk_level,
            trust_level: db.trust_level,
            known_safe: db.known_safe,
            first_seen: db.first_seen,
            last_seen: db.last_seen,
            status: db.status,
            redirect_to: db
                .redirect_to
                .map(|t| crate::surreal::record_id_from_thing(&t).map(Link::new))
                .transpose()
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            metadata: db.metadata,
        })
    }
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Entity repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct EntityRepository<B = SurrealStorageBackend> {
    backend: B,
}

impl EntityRepository {
    /// Creates a new entity repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self {
            backend: SurrealStorageBackend::new(client),
        }
    }
}

impl<B> EntityRepository<B> {
    /// Creates a repository from an arbitrary backend.
    #[must_use]
    pub const fn new_with_backend(backend: B) -> Self {
        Self { backend }
    }
}

impl<B> EntityRepository<B>
where
    B: StorageBackend,
{
    /// Gets multiple entities by ID while preserving requested order.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval or deserialization fails.
    pub async fn get_many(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> StorageResult<Vec<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            return store
                .get_many::<Entity>(space_id, ids, include_deleted)
                .await;
        }

        let mut entities = Vec::new();
        for id in ids {
            let entity = if include_deleted {
                let mut response = self
                    .backend
                    .query_bind(
                        &format!("SELECT {ENTITY_PROJECTION} FROM entity WHERE id = $id AND space_id = $space_id"),
                        crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
                    )
                    .await?;
                let rows: Vec<EntityDb> = response.take(0).map_err(|e| {
                    StorageError::QueryError(format!("Failed to parse entity: {e}"))
                })?;
                rows.into_iter().next().map(Entity::try_from).transpose()?
            } else {
                Repository::<Entity>::get(self, space_id, id).await?
            };
            if let Some(entity) = entity {
                entities.push(entity);
            }
        }
        Ok(entities)
    }
}

#[async_trait]
impl<B> Repository<Entity> for EntityRepository<B>
where
    B: StorageBackend,
{
    async fn create(&self, record: Entity) -> StorageResult<Entity> {
        if let Some(store) = self.backend.embedded_store() {
            return store.create(record).await;
        }

        // Checked single round-trip: a duplicate id now surfaces as
        // StorageError::Conflict instead of silently returning the
        // pre-existing row.
        let mut response = self
            .backend
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {ENTITY_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::entity_to_value(&record)?,
                },
            )
            .await?;

        let entities: Vec<EntityDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created entity: {e}"))
        })?;

        let entity_db = entities.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but entity not found".to_string())
        })?;

        Entity::try_from(entity_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            return store.get::<Entity>(space_id, id, false).await;
        }

        let mut response = self
            .backend
            .query_bind(
                &format!(
                    "SELECT {ENTITY_PROJECTION} FROM entity WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let entities: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity: {e}")))?;

        entities
            .into_iter()
            .map(Entity::try_from)
            .find_map(|result| match result {
                Ok(entity) if !is_source_inventory_entity(&entity) => Some(Ok(entity)),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .transpose()
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Entity) -> StorageResult<Entity> {
        if let Some(store) = self.backend.embedded_store() {
            let mut updated = record;
            updated.base.updated_at = Utc::now();
            return store.update_existing(space_id, id, updated).await;
        }

        // Normalize the caller-scoped identity fields so a stale id/space_id
        // inside the payload cannot re-target or re-scope the record.
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        let mut response = self
            .backend
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ENTITY_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::entity_to_value(&normalized)?,
                },
            )
            .await?;

        let entities: Vec<EntityDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated entity: {e}"))
        })?;

        let entity_db = entities
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Entity not found in space: {id}")))?;

        Entity::try_from(entity_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.backend.embedded_store() {
            return store.soft_delete::<Entity>(space_id, id).await;
        }

        self.backend
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
    ) -> StorageResult<Vec<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut entities = store
                .scan::<Entity>()
                .await?
                .into_iter()
                .filter(|entity| entity.base.space_id == space_id)
                .filter(|entity| entity.base.deleted_at.is_none())
                .filter(|entity| entity.status != EntityStatus::Merged)
                .filter(|entity| !is_source_inventory_entity(entity))
                .collect::<Vec<_>>();
            entities.sort_by(|left, right| left.base.id.cmp(&right.base.id));
            if let Some(cursor) = cursor {
                entities.retain(|entity| &entity.base.id > cursor);
            }
            entities.truncate(limit);
            return Ok(entities);
        }

        if limit == 0 {
            return Ok(Vec::new());
        }
        let scan_limit = limit.max(500);
        let mut scan_cursor = cursor.cloned();
        let mut visible = Vec::new();
        loop {
            let query = if scan_cursor.is_some() {
                format!(
                    "SELECT {ENTITY_PROJECTION} FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND status != 'merged' AND id > $cursor ORDER BY id LIMIT {scan_limit}"
                )
            } else {
                format!(
                    "SELECT {ENTITY_PROJECTION} FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND status != 'merged' ORDER BY id LIMIT {scan_limit}"
                )
            };
            let mut response = self
                .backend
                .query_bind(&query, crate::bindings! {
                    "cursor" => scan_cursor.as_ref().map_or(surrealdb::sql::Value::None, |record_id| surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(record_id))),
                    "space_id" => space_id
                })
                .await?;
            let entities: Vec<EntityDb> = response.take(0).map_err(|error| {
                StorageError::QueryError(format!("Failed to parse entities: {error}"))
            })?;
            let raw_count = entities.len();
            let entities = entities
                .into_iter()
                .map(Entity::try_from)
                .collect::<StorageResult<Vec<_>>>()?;
            scan_cursor = entities.last().map(|entity| entity.base.id.clone());
            visible.extend(
                entities
                    .into_iter()
                    .filter(|entity| !is_source_inventory_entity(entity)),
            );
            if visible.len() >= limit || raw_count < scan_limit {
                break;
            }
        }
        visible.truncate(limit);
        Ok(visible)
    }
}

impl<B> EntityRepository<B>
where
    B: StorageBackend,
{
    /// Sets or clears `metadata.angel_protectees` on an entity.
    ///
    /// `Some(ids)` replaces the stored list exactly; `None` removes the key
    /// entirely, returning the entity to family-wide semantics (entities
    /// without the key match every `attributed_to` filter, while an explicit
    /// empty array would match none).
    ///
    /// # Errors
    ///
    /// Returns an error if the entity does not exist or the update fails.
    pub async fn set_angel_protectees(
        &self,
        space_id: &str,
        id: &RecordId,
        protectees: Option<&[String]>,
    ) -> StorageResult<()> {
        if let Some(store) = self.backend.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_id = id.clone();
            let values = protectees.map(<[String]>::to_vec);
            return store
                .mutate(|mutation| {
                    let Some(mut existing) =
                        mutation.get::<Entity>(&target_space_id, &target_id, false)?
                    else {
                        return Err(StorageError::NotFound(target_id.to_string()));
                    };
                    match &values {
                        Some(ids) => {
                            if !existing.metadata.is_object() {
                                existing.metadata =
                                    serde_json::Value::Object(serde_json::Map::new());
                            }
                            if let Some(map) = existing.metadata.as_object_mut() {
                                map.insert(
                                    ANGEL_PROTECTEES_METADATA_KEY.to_string(),
                                    serde_json::Value::Array(
                                        ids.iter()
                                            .cloned()
                                            .map(serde_json::Value::String)
                                            .collect(),
                                    ),
                                );
                            }
                        }
                        None => {
                            if let Some(map) = existing.metadata.as_object_mut() {
                                map.remove(ANGEL_PROTECTEES_METADATA_KEY);
                            }
                        }
                    }
                    existing.base.updated_at = Utc::now();
                    Ok(mutation.replace(existing)?)
                })
                .await
                .map(|_: Entity| ());
        }

        // Setting a SurrealDB field to NONE removes it, which is exactly the
        // family-wide semantics we need for the `None` case (the plain
        // Repository::update path uses MERGE and cannot drop a nested key).
        let values = protectees.map_or(surrealdb::sql::Value::None, |ids| {
            surrealdb::sql::Value::from(ids.to_vec())
        });
        self.backend
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET metadata.{ANGEL_PROTECTEES_METADATA_KEY} = $values, updated_at = time::now() \
                     WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "values" => values,
                },
            )
            .await?;

        Ok(())
    }

    /// Find the first active entity in a space that has a matching identity key.
    pub async fn find_by_identity_key(
        &self,
        space_id: &str,
        identity_key: &str,
    ) -> StorageResult<Option<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            let entities = store.scan::<Entity>().await?;
            return Ok(entities.into_iter().find(|entity| {
                entity.base.space_id == space_id
                    && entity.base.deleted_at.is_none()
                    && entity.status != EntityStatus::Merged
                    && !is_source_inventory_entity(entity)
                    && entity.identity_keys.iter().any(|key| key == identity_key)
            }));
        }

        let query = format!(
            "SELECT {ENTITY_PROJECTION} FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND status != 'merged' AND identity_keys CONTAINS $key LIMIT 1"
        );

        let mut response = self
            .backend
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "key" => identity_key
                },
            )
            .await?;

        let entities: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entities: {e}")))?;

        entities
            .into_iter()
            .next()
            .map(Entity::try_from)
            .transpose()
    }

    /// Find all active entities in a space that have a matching identity key.
    pub async fn find_by_identity_key_all(
        &self,
        space_id: &str,
        identity_key: &str,
    ) -> StorageResult<Vec<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut entities: Vec<_> = store
                .scan::<Entity>()
                .await?
                .into_iter()
                .filter(|entity| {
                    entity.base.space_id == space_id
                        && entity.base.deleted_at.is_none()
                        && entity.status != EntityStatus::Merged
                        && !is_source_inventory_entity(entity)
                        && entity.identity_keys.iter().any(|key| key == identity_key)
                })
                .collect();
            entities.sort_by_key(|entity| entity.base.id.to_string());
            return Ok(entities);
        }

        let query = format!(
            "SELECT {ENTITY_PROJECTION} FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND status != 'merged' AND identity_keys CONTAINS $key ORDER BY id"
        );

        let mut response = self
            .backend
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "key" => identity_key
                },
            )
            .await?;

        let entities: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entities: {e}")))?;

        entities
            .into_iter()
            .map(Entity::try_from)
            .filter_map(|result| match result {
                Ok(entity) if !is_source_inventory_entity(&entity) => Some(Ok(entity)),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .collect()
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl<B> SearchRepository<Entity> for EntityRepository<B>
where
    B: StorageBackend,
{
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut rows = store.scan::<Entity>().await?;
            if let Some(space_id) = &criteria.space_id {
                rows.retain(|entity| &entity.base.space_id == space_id);
            }
            if !criteria.include_deleted {
                rows.retain(|entity| entity.base.deleted_at.is_none());
                rows.retain(|entity| entity.status != EntityStatus::Merged);
                rows.retain(|entity| !is_source_inventory_entity(entity));
            }
            if let Some(entity_type) = &criteria.entity_type {
                rows.retain(|entity| &entity.entity_type == entity_type);
            }
            if let Some(attributed_to) = &criteria.attributed_to {
                rows.retain(|entity| entity_is_attributed_to(entity, attributed_to));
            }
            if let Some(query) = &criteria.query {
                rows.retain(|entity| entity_matches_search_query(entity, query));
                rows.sort_by(|left, right| {
                    entity_search_rank(right, query)
                        .cmp(&entity_search_rank(left, query))
                        .then_with(|| left.display_name.cmp(&right.display_name))
                        .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
                });
            } else {
                rows.sort_by_key(|entity| entity.base.id.to_string());
            }

            let total = rows.len();
            if let Some(cursor) = &criteria.cursor {
                let cursor_id = if cursor.contains(':') {
                    cursor.clone()
                } else {
                    format!("entity:{cursor}")
                };
                if let Some(index) = rows
                    .iter()
                    .position(|entity| entity.base.id.to_string() == cursor_id)
                {
                    rows.drain(..=index);
                } else {
                    rows.retain(|entity| entity.base.id.to_string() > cursor_id);
                }
            }

            if criteria.limit == 0 {
                return Ok(PaginatedResult::new(Vec::new(), None, false, total));
            }
            let has_more = rows.len() > criteria.limit;
            if has_more {
                rows.truncate(criteria.limit + 1);
            }
            let next_cursor = if rows.len() > criteria.limit {
                let _ = rows.pop();
                rows.last().map(|entity| {
                    let id_str = entity.base.id.to_string();
                    id_str
                        .split(':')
                        .nth(1)
                        .map_or_else(|| id_str.clone(), str::to_string)
                })
            } else {
                None
            };
            return Ok(PaginatedResult::new(rows, next_cursor, has_more, total));
        }

        #[derive(Deserialize)]
        struct CountResult {
            total: usize,
        }

        let mut conditions = Vec::new();
        let mut bindings = std::collections::BTreeMap::new();

        if !criteria.include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
            conditions.push("status != 'merged'".to_string());
        }

        if let Some(ref space_id) = criteria.space_id {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        if let Some(ref entity_type) = criteria.entity_type {
            conditions.push("type = $entity_type".to_string());
            bindings.insert(
                "entity_type".to_string(),
                surrealdb::sql::Value::from(entity_type.as_str().to_string()),
            );
        }

        if let Some(ref attributed_to) = criteria.attributed_to {
            conditions.push(
                "(metadata.angel_protectees IS NONE OR $attributed_to IN metadata.angel_protectees)"
                    .to_string(),
            );
            bindings.insert(
                "attributed_to".to_string(),
                surrealdb::sql::Value::from(attributed_to.clone()),
            );
        }

        if let Some(ref query) = criteria.query {
            conditions.push("(string::lowercase(display_name) CONTAINS $query_lower OR aliases CONTAINS $query OR identity_keys CONTAINS $query_lower)".to_string());
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
            bindings.insert(
                "query_lower".to_string(),
                surrealdb::sql::Value::from(query.to_ascii_lowercase()),
            );
        }

        if let Some(ref cursor) = criteria.cursor {
            let rid = if cursor.contains(':') {
                RecordId::from_str(cursor).map_err(|e| StorageError::QueryError(e.to_string()))?
            } else {
                RecordId::from_str(&format!("entity:{cursor}"))
                    .map_err(|e| StorageError::QueryError(e.to_string()))?
            };
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
            "SELECT {ENTITY_PROJECTION} FROM entity {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.backend.query_bind(&query, bindings.clone()).await?;

        let entities_db: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entities: {e}")))?;

        let mut entities: Vec<Entity> = entities_db
            .into_iter()
            .map(Entity::try_from)
            .filter_map(|result| match result {
                Ok(entity) if criteria.include_deleted || !is_source_inventory_entity(&entity) => {
                    Some(Ok(entity))
                }
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = criteria.limit > 0 && entities.len() > criteria.limit;
        if has_more {
            entities.pop();
        } else if criteria.limit == 0 {
            entities.clear();
        }

        let next_cursor = if has_more {
            entities.last().map(|e| {
                let id_str = e.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        let count_query = format!("SELECT count() as total FROM entity {where_clause} GROUP ALL");

        let mut count_response = self.backend.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> = count_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity count: {e}")))?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(entities, next_cursor, has_more, total))
    }
}

/// Additional entity-specific operations.
impl EntityRepository {
    /// Performs BM25 search over display_name, aliases, and summary.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn bm25_search_entities(
        &self,
        space_id: &str,
        query: &str,
        limit: usize,
    ) -> StorageResult<Vec<EntitySearchHit>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut hits: Vec<_> = store
                .scan::<Entity>()
                .await?
                .into_iter()
                .filter(|entity| {
                    entity.base.space_id == space_id
                        && entity.base.deleted_at.is_none()
                        && entity.status != EntityStatus::Merged
                        && !is_source_inventory_entity(entity)
                })
                .filter_map(|entity| {
                    let alias_text = entity.aliases.join(" ");
                    let summary = entity.summary.clone().unwrap_or_default();
                    let score = lexical_score(
                        query,
                        &[
                            (entity.display_name.as_str(), 1.0),
                            (alias_text.as_str(), 0.9),
                            (summary.as_str(), 0.6),
                        ],
                    );
                    (score > 0.0).then_some(EntitySearchHit {
                        entity_id: entity.base.id,
                        score,
                    })
                })
                .collect();

            hits.sort_by(|left, right| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if hits.len() > limit {
                hits.truncate(limit);
            }
            return Ok(hits);
        }

        let name_hits = self
            .bm25_search_field(space_id, "display_name", query, limit)
            .await?;
        let alias_hits = self
            .bm25_search_field(space_id, "aliases", query, limit)
            .await?;
        let summary_hits = self
            .bm25_search_field(space_id, "summary", query, limit)
            .await?;

        let mut merged: std::collections::HashMap<RecordId, f32> = std::collections::HashMap::new();
        for hit in name_hits {
            let score = normalize_bm25_score(hit.score) * 1.0;
            merged
                .entry(hit.entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }
        for hit in alias_hits {
            let score = normalize_bm25_score(hit.score) * 0.9;
            merged
                .entry(hit.entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }
        for hit in summary_hits {
            let score = normalize_bm25_score(hit.score) * 0.6;
            merged
                .entry(hit.entity_id)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        }

        let mut hits: Vec<EntitySearchHit> = merged
            .into_iter()
            .map(|(entity_id, score)| EntitySearchHit { entity_id, score })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if hits.len() > limit {
            hits.truncate(limit);
        }

        Ok(hits)
    }

    /// Finds an entity by exact display name.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_display_name(
        &self,
        space_id: &str,
        display_name: &str,
    ) -> StorageResult<Option<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            return Ok(store.scan::<Entity>().await?.into_iter().find(|entity| {
                entity.base.space_id == space_id
                    && entity.base.deleted_at.is_none()
                    && !is_source_inventory_entity(entity)
                    && entity.display_name == display_name
            }));
        }

        let mut response = self
            .backend
            .query_bind(
                &format!(
                    "SELECT {ENTITY_PROJECTION} FROM entity WHERE space_id = $space_id AND display_name = $display_name AND deleted_at IS NONE LIMIT 1"
                ),
                crate::bindings! { "space_id" => space_id, "display_name" => display_name },
            )
            .await?;

        let entities: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity: {e}")))?;

        entities
            .into_iter()
            .map(Entity::try_from)
            .find_map(|result| match result {
                Ok(entity) if !is_source_inventory_entity(&entity) => Some(Ok(entity)),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .transpose()
    }

    /// Performs vector similarity search over entity name embeddings.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn vector_search_entities(
        &self,
        space_id: &str,
        embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<EntitySearchHit>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut hits: Vec<_> = store
                .scan::<Entity>()
                .await?
                .into_iter()
                .filter(|entity| {
                    entity.base.space_id == space_id
                        && entity.base.deleted_at.is_none()
                        && !is_source_inventory_entity(entity)
                })
                .filter_map(|entity| {
                    let candidate = entity_name_embedding(&entity, embedding.len())?;
                    let score = cosine_similarity(&candidate, embedding);
                    (score > 0.0).then_some(EntitySearchHit {
                        entity_id: entity.base.id,
                        score,
                    })
                })
                .collect();

            hits.sort_by(|left, right| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if hits.len() > limit {
                hits.truncate(limit);
            }
            return Ok(hits);
        }

        // The array::len guard keeps mixed-dimension rows (e.g. right after
        // an embedding-model change, before the sweep re-embeds) from
        // erroring the whole cosine query.
        let query = format!(
            "SELECT id, vector::similarity::cosine(name_embedding, $vector) AS score \
             FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND name_embedding IS NOT NONE \
             AND array::len(name_embedding) = {} \
             ORDER BY score DESC LIMIT {limit}",
            embedding.len()
        );

        let mut response = self
            .backend
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "vector" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(embedding.to_vec()))
                },
            )
            .await?;

        let hits: Vec<EntitySearchHitDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity vector hits: {e}"))
        })?;

        hits.into_iter().map(EntitySearchHit::try_from).collect()
    }

    /// Stores the semantic-search embedding for an entity, together with the
    /// producing model and a hash of the embedded text (used by the
    /// maintenance sweep to skip unchanged entities).
    ///
    /// On the `SurrealDB` backend the vector lives in the dedicated
    /// `name_embedding` field (outside `ENTITY_PROJECTION`, so ordinary reads
    /// never carry it); on the embedded backend it lives under
    /// `metadata.name_embedding`, which is where that backend's
    /// `vector_search_entities` path reads it from.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn set_name_embedding(
        &self,
        space_id: &str,
        id: &RecordId,
        embedding: &[f32],
        model: &str,
        text_hash: &str,
    ) -> StorageResult<()> {
        if let Some(store) = self.backend.embedded_store() {
            let target_space_id = space_id.to_string();
            let target_id = id.clone();
            let embedding = embedding.to_vec();
            let model = model.to_string();
            let text_hash = text_hash.to_string();
            return store
                .mutate(move |mutation| {
                    let Some(mut entity) =
                        mutation.get::<Entity>(&target_space_id, &target_id, false)?
                    else {
                        return Ok(());
                    };
                    if !entity.metadata.is_object() {
                        entity.metadata = serde_json::Value::Object(serde_json::Map::new());
                    }
                    if let Some(map) = entity.metadata.as_object_mut() {
                        map.insert("name_embedding".to_string(), serde_json::json!(embedding));
                        map.insert(
                            "name_embedding_model".to_string(),
                            serde_json::Value::String(model.clone()),
                        );
                        map.insert(
                            "name_embedding_text_hash".to_string(),
                            serde_json::Value::String(text_hash),
                        );
                    }
                    mutation.replace(entity)?;
                    Ok(())
                })
                .await;
        }

        self.backend
            .query_bind_checked(
                "UPDATE $id SET name_embedding = $vector, name_embedding_model = $model, \
                 name_embedding_text_hash = $text_hash \
                 WHERE space_id = $space_id AND deleted_at IS NONE",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "vector" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(embedding.to_vec())),
                    "model" => model,
                    "text_hash" => text_hash,
                },
            )
            .await?;

        Ok(())
    }

    /// Lists active entities as embedding-sweep candidates: the text to
    /// embed plus stored model/hash, no vectors. Merged entities and source
    /// inventory rows are excluded — they are never search results.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn list_embedding_candidates(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<EntityEmbeddingCandidate>> {
        if let Some(store) = self.backend.embedded_store() {
            let mut candidates: Vec<_> = store
                .scan::<Entity>()
                .await?
                .into_iter()
                .filter(|entity| {
                    entity.base.space_id == space_id
                        && entity.base.deleted_at.is_none()
                        && entity.status != EntityStatus::Merged
                        && !is_source_inventory_entity(entity)
                })
                .map(|entity| EntityEmbeddingCandidate {
                    entity_id: entity.base.id.clone(),
                    display_name: entity.display_name.clone(),
                    aliases: entity.aliases.clone(),
                    summary: entity.summary.clone(),
                    embedding_model: entity
                        .metadata
                        .get("name_embedding_model")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string),
                    embedding_text_hash: entity
                        .metadata
                        .get("name_embedding_text_hash")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string),
                    embedding_dims: entity
                        .metadata
                        .get("name_embedding")
                        .and_then(serde_json::Value::as_array)
                        .map(Vec::len),
                })
                .collect();
            candidates.sort_by(|left, right| {
                left.entity_id.to_string().cmp(&right.entity_id.to_string())
            });
            candidates.truncate(limit);
            return Ok(candidates);
        }

        let query = format!(
            "SELECT id, display_name, \
             IF aliases IS NOT NONE THEN aliases ELSE [] END AS aliases, \
             summary, name_embedding_model, name_embedding_text_hash, \
             IF name_embedding IS NOT NONE THEN array::len(name_embedding) ELSE NONE END AS embedding_dims \
             FROM entity WHERE space_id = $space_id AND deleted_at IS NONE AND status != 'merged' \
             ORDER BY id LIMIT {limit}"
        );

        let mut response = self
            .backend
            .query_bind(&query, crate::bindings! { "space_id" => space_id })
            .await?;

        let rows: Vec<EntityEmbeddingCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse embedding candidates: {e}"))
        })?;

        Ok(rows
            .into_iter()
            .map(EntityEmbeddingCandidate::try_from)
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .filter(|candidate| !is_source_inventory_label(&candidate.display_name))
            .collect())
    }

    async fn bm25_search_field(
        &self,
        space_id: &str,
        field: &str,
        query: &str,
        limit: usize,
    ) -> StorageResult<Vec<EntitySearchHit>> {
        let query_value = surrealdb::sql::Value::from(query);
        let statement = format!(
            "SELECT id, search::score(0) AS score FROM entity \
             WHERE space_id = $space_id AND deleted_at IS NONE AND {field} @0@ {query_value} \
             ORDER BY score DESC LIMIT {limit}"
        );

        let mut response = self
            .backend
            .query_bind(
                &statement,
                crate::bindings! {
                    "space_id" => space_id,
                },
            )
            .await?;

        let hits: Vec<EntitySearchHitDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse entity BM25 hits: {e}"))
        })?;

        hits.into_iter().map(EntitySearchHit::try_from).collect()
    }
    /// Gets an entity including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Entity>> {
        if let Some(store) = self.backend.embedded_store() {
            return store.get::<Entity>(space_id, id, true).await;
        }

        let mut response = self
            .backend
            .query_bind(
                &format!("SELECT {ENTITY_PROJECTION} FROM entity WHERE id = $id AND space_id = $space_id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let entities: Vec<EntityDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse entity: {e}")))?;

        entities
            .into_iter()
            .next()
            .map(Entity::try_from)
            .transpose()
    }

    /// Restores a soft-deleted entity.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or the entity is not found.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Entity> {
        if let Some(store) = self.backend.embedded_store() {
            return store.restore::<Entity>(space_id, id).await;
        }

        // Checked single round-trip: the restore only returns rows it actually
        // updated, so a foreign-space id (or a swallowed statement error) can
        // no longer surface as a "successful" restore of a still-deleted or
        // cross-space entity.
        let mut response = self
            .backend
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {ENTITY_PROJECTION}"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let entities: Vec<EntityDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored entity: {e}"))
        })?;

        let entity_db = entities
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Entity not found: {id}")))?;

        Entity::try_from(entity_db)
    }

    /// Hard deletes an entity (permanent).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete query fails.
    pub async fn hard_delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.backend.embedded_store() {
            return store.hard_delete::<Entity>(space_id, id).await;
        }

        self.backend
            .query_bind_checked(
                "DELETE FROM $id WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    /// Upserts an entity based on its ID.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval, restore, or update fails.
    pub async fn upsert(&self, record: Entity) -> StorageResult<Entity> {
        let space_id = record.base.space_id.clone();
        let id = record.base.id.clone();

        match self.get_with_deleted(&space_id, &id).await? {
            Some(existing) => {
                if existing.base.deleted_at.is_some() {
                    let _ = self.restore(&space_id, &id).await?;
                }
                self.update(&space_id, &id, record).await
            }
            None => self.create(record).await,
        }
    }

    /// Gets entity with all relationships.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_relations(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityWithRelations>> {
        if let Some(store) = self.backend.embedded_store() {
            let Some(entity) = store.get::<Entity>(space_id, id, false).await? else {
                return Ok(None);
            };

            let relations = store.entity_relations(space_id, id).await?;
            let outgoing_target_ids = relations
                .outgoing
                .iter()
                .map(|rel| rel.object.id().clone())
                .collect::<Vec<_>>();
            let incoming_source_ids = relations
                .incoming
                .iter()
                .map(|rel| rel.subject.id().clone())
                .collect::<Vec<_>>();
            let outgoing_targets = store
                .get_many::<Entity>(space_id, &outgoing_target_ids, false)
                .await?
                .into_iter()
                .map(|entity| entity.base.id)
                .collect::<std::collections::HashSet<_>>();
            let incoming_sources = store
                .get_many::<Entity>(space_id, &incoming_source_ids, false)
                .await?
                .into_iter()
                .map(|entity| entity.base.id)
                .collect::<std::collections::HashSet<_>>();
            let outgoing_relations = relations
                .outgoing
                .into_iter()
                .filter(|rel| outgoing_targets.contains(rel.object.id()))
                .collect();
            let incoming_relations = relations
                .incoming
                .into_iter()
                .filter(|rel| incoming_sources.contains(rel.subject.id()))
                .collect();

            return Ok(Some(EntityWithRelations {
                entity,
                outgoing_relations,
                incoming_relations,
            }));
        }

        let entity = self.get(space_id, id).await?;

        let Some(entity) = entity else {
            return Ok(None);
        };

        // Get outgoing relationships
        let mut out_response = self
            .backend
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE `in` = $id AND space_id = $space_id AND deleted_at IS NONE AND `out`.deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let outgoing_db: Vec<crate::rel_repository::RelDb> = out_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse outgoing rels: {e}")))?;

        let outgoing: Vec<Rel> = outgoing_db
            .into_iter()
            .map(Rel::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        // Get incoming relationships
        let mut in_response = self
            .backend
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE `out` = $id AND space_id = $space_id AND deleted_at IS NONE AND `in`.deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let incoming_db: Vec<crate::rel_repository::RelDb> = in_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse incoming rels: {e}")))?;

        let incoming: Vec<Rel> = incoming_db
            .into_iter()
            .map(Rel::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(EntityWithRelations {
            entity,
            outgoing_relations: outgoing,
            incoming_relations: incoming,
        }))
    }
}

fn normalize_inventory_entity_label(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// Whether an entity is a source-inventory artifact (per-message/per-file
/// bookkeeping rows) that must be hidden from entity reads and search.
#[must_use]
pub fn is_source_inventory_entity(entity: &Entity) -> bool {
    is_source_inventory_label(&entity.display_name)
}

fn is_source_inventory_label(display_name: &str) -> bool {
    let normalized = normalize_inventory_entity_label(display_name);
    [
        "email message ",
        "mail message ",
        "calendar event ",
        "source document ",
        "source excerpt ",
        "source file ",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
}

fn normalize_bm25_score(score: f32) -> f32 {
    let score = score.abs();
    if score == 0.0 {
        0.0
    } else {
        score / (score + 1.0)
    }
}

fn entity_is_attributed_to(entity: &Entity, attributed_to: &str) -> bool {
    entity
        .metadata
        .get("angel_protectees")
        .and_then(serde_json::Value::as_array)
        .is_none_or(|protectees| {
            protectees
                .iter()
                .any(|value| value.as_str() == Some(attributed_to))
        })
}

fn entity_matches_search_query(entity: &Entity, query: &str) -> bool {
    let tokens: Vec<_> = query
        .split_whitespace()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    if tokens.is_empty() {
        return true;
    }

    let mut haystack = String::new();
    haystack.push_str(&entity.display_name);
    haystack.push(' ');
    haystack.push_str(&entity.aliases.join(" "));
    haystack.push(' ');
    haystack.push_str(&entity.identity_keys.join(" "));
    if let Some(summary) = &entity.summary {
        haystack.push(' ');
        haystack.push_str(summary);
    }
    let haystack = haystack.to_ascii_lowercase();

    tokens.iter().all(|token| haystack.contains(token))
}

fn entity_search_rank(entity: &Entity, query: &str) -> i32 {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return 0;
    }
    let display = entity.display_name.to_ascii_lowercase();
    let aliases: Vec<String> = entity
        .aliases
        .iter()
        .map(|alias| alias.to_ascii_lowercase())
        .collect();
    let identity_keys: Vec<String> = entity
        .identity_keys
        .iter()
        .map(|key| key.to_ascii_lowercase())
        .collect();

    let mut score = 0;
    if identity_keys.iter().any(|key| key == &query) {
        score += 130;
    }
    if aliases.iter().any(|alias| alias == &query) {
        score += 120;
    }
    if display == query {
        score += 100;
    }
    if identity_keys.iter().any(|key| key.contains(&query)) {
        score += 70;
    }
    if aliases.iter().any(|alias| alias.contains(&query)) {
        score += 60;
    }
    if display.contains(&query) {
        score += 50;
    }
    if entity
        .summary
        .as_ref()
        .is_some_and(|summary| summary.to_ascii_lowercase().contains(&query))
    {
        score += 10;
    }
    score += i32::try_from(entity.aliases.len().min(20)).unwrap_or(0);
    score += i32::try_from(entity.identity_keys.len().min(20)).unwrap_or(0);
    score
}

fn entity_name_embedding(entity: &Entity, dims: usize) -> Option<Vec<f32>> {
    entity
        .metadata
        .get("name_embedding")
        .and_then(serde_json::Value::as_array)
        .and_then(|values| {
            let parsed = values
                .iter()
                .map(|value| value.as_f64().and_then(|value| value.to_f32()))
                .collect::<Option<Vec<_>>>()?;
            (parsed.len() == dims).then_some(parsed)
        })
}

/// Entity with its relationships.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EntityWithRelations {
    /// The entity.
    pub entity: Entity,
    /// Outgoing relationships (where this entity is the source).
    pub outgoing_relations: Vec<Rel>,
    /// Incoming relationships (where this entity is the target).
    pub incoming_relations: Vec<Rel>,
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
    use crate::{StorageBackendKind, StorageConfig};

    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(10000);

    const ENTITY_SCHEMA: &str = r"
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
    ";

    async fn setup_test_repo() -> EntityRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("entity_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(ENTITY_SCHEMA).await.unwrap();

        EntityRepository::new(client)
    }

    async fn setup_embedded_test_repo() -> EntityRepository {
        let client = StorageClient::new(StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        })
        .await
        .unwrap();

        EntityRepository::new(client)
    }

    fn create_test_entity(display_name: &str) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Entity::new(base, EntityType::new("person"), display_name.to_string())
    }

    #[tokio::test]
    async fn test_create_entity() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("John Doe");

        let created = repo.create(entity.clone()).await.unwrap();

        assert_eq!(created.display_name, "John Doe");
        assert_eq!(created.entity_type, EntityType::new("person"));
    }

    #[tokio::test]
    async fn test_get_entity() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("Jane Doe");
        let id = entity.base.id.clone();

        repo.create(entity).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().display_name, "Jane Doe");
    }

    #[tokio::test]
    async fn test_get_nonexistent_entity() {
        let repo = setup_test_repo().await;
        let id = RecordId::new_v4(TableName::Entity);

        let result = repo.get("test_space", &id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_update_entity() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("Original Name");
        let id = entity.base.id.clone();

        let created = repo.create(entity).await.unwrap();

        let mut updated_entity = created.clone();
        updated_entity.display_name = "Updated Name".to_string();

        let updated = repo
            .update("test_space", &id, updated_entity)
            .await
            .unwrap();
        assert_eq!(updated.display_name, "Updated Name");

        let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(retrieved.display_name, "Updated Name");
    }

    #[tokio::test]
    async fn test_soft_delete_entity() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("To Delete");
        let id = entity.base.id.clone();

        repo.create(entity).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let result = repo.get("test_space", &id).await.unwrap();
        assert!(result.is_none());

        let deleted = repo.get_with_deleted("test_space", &id).await.unwrap();
        assert!(deleted.is_some());
        assert!(deleted.unwrap().base.deleted_at.is_some());
    }

    #[tokio::test]
    async fn test_restore_entity() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("To Restore");
        let id = entity.base.id.clone();

        repo.create(entity).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        assert!(repo.get("test_space", &id).await.unwrap().is_none());

        let restored = repo.restore("test_space", &id).await.unwrap();
        assert!(restored.base.deleted_at.is_none());

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_list_entities() {
        let repo = setup_test_repo().await;

        for i in 0..5 {
            let entity = create_test_entity(&format!("Entity {i}"));
            repo.create(entity).await.unwrap();
        }

        let entities = repo.list("test_space", 10, None).await.unwrap();
        assert_eq!(entities.len(), 5);
    }

    #[tokio::test]
    async fn test_search_by_space_id() {
        let repo = setup_test_repo().await;

        let mut entity1 = create_test_entity("Entity 1");
        entity1.base.space_id = "space_a".to_string();
        repo.create(entity1).await.unwrap();

        let mut entity2 = create_test_entity("Entity 2");
        entity2.base.space_id = "space_b".to_string();
        repo.create(entity2).await.unwrap();

        let criteria = SearchCriteria::new().with_space_id("space_a");
        let result = repo.search(&criteria).await.unwrap();

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].base.space_id, "space_a");
    }

    #[tokio::test]
    async fn test_search_by_query() {
        let repo = setup_test_repo().await;

        repo.create(create_test_entity("Alice Smith"))
            .await
            .unwrap();
        repo.create(create_test_entity("Bob Jones")).await.unwrap();
        repo.create(create_test_entity("Alice Johnson"))
            .await
            .unwrap();

        let criteria = SearchCriteria::new()
            .with_space_id("test_space")
            .with_query("Alice");
        let result = repo.search(&criteria).await.unwrap();

        assert_eq!(result.results.len(), 2);
        assert!(result
            .results
            .iter()
            .all(|e| e.display_name.contains("Alice")));
    }

    #[tokio::test]
    async fn test_source_inventory_wrapper_entities_are_hidden_by_default() {
        let repo = setup_test_repo().await;

        repo.create(create_test_entity("Source document Ada Example"))
            .await
            .unwrap();
        repo.create(create_test_entity("Ada Example"))
            .await
            .unwrap();

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Ada Example"),
            )
            .await
            .unwrap();

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].display_name, "Ada Example");
    }

    #[tokio::test]
    async fn test_list_paginates_past_source_inventory_rows() {
        let repo = setup_test_repo().await;
        for index in 0..501_u128 {
            let mut entity = create_test_entity(&format!("Source document {index:03}"));
            entity.base.id = RecordId::new(TableName::Entity, uuid::Uuid::from_u128(index + 1));
            repo.create(entity).await.expect("source inventory entity");
        }
        for index in 0..2_u128 {
            let mut entity = create_test_entity(&format!("Household topic {index}"));
            entity.base.id =
                RecordId::new(TableName::Entity, uuid::Uuid::from_u128(u128::MAX - index));
            repo.create(entity).await.expect("semantic entity");
        }

        let listed = repo.list("test_space", 2, None).await.expect("list");
        assert_eq!(listed.len(), 2);
        assert!(listed
            .iter()
            .all(|entity| entity.display_name.starts_with("Household topic")));
    }

    #[tokio::test]
    async fn test_search_by_alias_and_identity_keys() {
        let repo = setup_test_repo().await;

        let mut entity = create_test_entity("alex@example.test");
        entity.aliases = vec!["Alex Example".to_string()];
        entity.identity_keys = vec![
            "alex".to_string(),
            "alex example".to_string(),
            "alex@example.test".to_string(),
        ];
        repo.create(entity).await.unwrap();
        repo.create(create_test_entity("Riley Example"))
            .await
            .unwrap();

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Alex Example"),
            )
            .await
            .unwrap();

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].display_name, "alex@example.test");

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("alex@example.test"),
            )
            .await
            .unwrap();

        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].display_name, "alex@example.test");
    }

    async fn seed_attribution_entities(repo: &EntityRepository) {
        let mut entity_a = create_test_entity("Protectee A Contact");
        entity_a.metadata = serde_json::json!({ "angel_protectees": ["a"] });
        repo.create(entity_a).await.unwrap();

        let mut entity_b = create_test_entity("Protectee B Contact");
        entity_b.metadata = serde_json::json!({ "angel_protectees": ["b"] });
        repo.create(entity_b).await.unwrap();

        repo.create(create_test_entity("Family Contact"))
            .await
            .unwrap();
    }

    async fn assert_attributed_search(repo: &EntityRepository) {
        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_attributed_to("a"),
            )
            .await
            .unwrap();

        assert_eq!(result.results.len(), 2);
        assert_eq!(result.total, 2);
        let names: Vec<&str> = result
            .results
            .iter()
            .map(|entity| entity.display_name.as_str())
            .collect();
        assert!(names.contains(&"Protectee A Contact"));
        assert!(names.contains(&"Family Contact"));

        // Count/total stay consistent when paginating through the filter.
        let first_page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_attributed_to("a")
                    .with_limit(1),
            )
            .await
            .unwrap();
        assert_eq!(first_page.results.len(), 1);
        assert_eq!(first_page.total, 2);
        assert!(first_page.has_more);

        let second_page = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_attributed_to("a")
                    .with_limit(1)
                    .with_cursor(first_page.cursor.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second_page.results.len(), 1);
        assert!(!second_page.has_more);

        let mut paged_names = vec![
            first_page.results[0].display_name.clone(),
            second_page.results[0].display_name.clone(),
        ];
        paged_names.sort();
        assert_eq!(
            paged_names,
            vec![
                "Family Contact".to_string(),
                "Protectee A Contact".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_search_attributed_to_includes_unattributed() {
        let repo = setup_test_repo().await;
        seed_attribution_entities(&repo).await;
        assert_attributed_search(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_search_attributed_to_includes_unattributed() {
        let repo = setup_embedded_test_repo().await;
        seed_attribution_entities(&repo).await;
        assert_attributed_search(&repo).await;
    }

    async fn assert_set_angel_protectees_replaces_and_clears(repo: &EntityRepository) {
        let mut entity = create_test_entity("Attributed Contact");
        entity.metadata =
            serde_json::json!({ "angel_protectees": ["child-a", "child-b"], "note": "keep" });
        let entity_id = entity.base.id.clone();
        repo.create(entity).await.unwrap();

        // Replace, not union: child-b must be gone afterwards.
        repo.set_angel_protectees("test_space", &entity_id, Some(&["child-a".to_string()]))
            .await
            .unwrap();
        let updated = repo
            .get("test_space", &entity_id)
            .await
            .unwrap()
            .expect("entity exists");
        assert_eq!(
            updated.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
        assert_eq!(updated.metadata["note"], serde_json::json!("keep"));

        // `None` removes the key entirely (family-wide semantics): the entity
        // must be visible again through any attributed_to filter.
        repo.set_angel_protectees("test_space", &entity_id, None)
            .await
            .unwrap();
        let cleared = repo
            .get("test_space", &entity_id)
            .await
            .unwrap()
            .expect("entity exists");
        assert!(
            cleared.metadata.get("angel_protectees").is_none(),
            "angel_protectees key should be removed, got: {}",
            cleared.metadata
        );
        assert_eq!(cleared.metadata["note"], serde_json::json!("keep"));

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_attributed_to("child-b"),
            )
            .await
            .unwrap();
        assert!(result
            .results
            .iter()
            .any(|entity| entity.display_name == "Attributed Contact"));
    }

    #[tokio::test]
    async fn test_set_angel_protectees_replaces_and_clears() {
        let repo = setup_test_repo().await;
        assert_set_angel_protectees_replaces_and_clears(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_set_angel_protectees_replaces_and_clears() {
        let repo = setup_embedded_test_repo().await;
        assert_set_angel_protectees_replaces_and_clears(&repo).await;
    }

    #[tokio::test]
    async fn test_find_by_identity_key_all_returns_all_matches() {
        let repo = setup_test_repo().await;

        let mut first = create_test_entity("Acme");
        first.identity_keys = vec!["acme".to_string()];
        let first_id = first.base.id.clone();
        repo.create(first).await.unwrap();

        let mut second = create_test_entity("Acme Corp");
        second.identity_keys = vec!["acme".to_string(), "acme corp".to_string()];
        let second_id = second.base.id.clone();
        repo.create(second).await.unwrap();

        let mut unrelated = create_test_entity("Globex");
        unrelated.identity_keys = vec!["globex".to_string()];
        repo.create(unrelated).await.unwrap();

        let mut deleted = create_test_entity("Acme Old");
        deleted.identity_keys = vec!["acme".to_string()];
        let deleted_id = deleted.base.id.clone();
        repo.create(deleted).await.unwrap();
        repo.delete("test_space", &deleted_id).await.unwrap();

        let mut other_space = create_test_entity("Acme Elsewhere");
        other_space.base.space_id = "other_space".to_string();
        other_space.identity_keys = vec!["acme".to_string()];
        repo.create(other_space).await.unwrap();

        let matches = repo
            .find_by_identity_key_all("test_space", "acme")
            .await
            .unwrap();

        assert_eq!(matches.len(), 2);
        let mut ids: Vec<_> = matches
            .iter()
            .map(|entity| entity.base.id.to_string())
            .collect();
        ids.sort();
        let mut expected = vec![first_id.to_string(), second_id.to_string()];
        expected.sort();
        assert_eq!(ids, expected);

        let single = repo
            .find_by_identity_key("test_space", "acme")
            .await
            .unwrap();
        assert!(single.is_some());
    }

    #[tokio::test]
    async fn test_embedded_find_by_identity_key_all_returns_all_matches() {
        let repo = setup_embedded_test_repo().await;

        let mut first = create_test_entity("Acme");
        first.identity_keys = vec!["acme".to_string()];
        repo.create(first).await.unwrap();

        let mut second = create_test_entity("Acme Corp");
        second.identity_keys = vec!["acme".to_string()];
        repo.create(second).await.unwrap();

        let mut merged = create_test_entity("Acme Merged");
        merged.identity_keys = vec!["acme".to_string()];
        merged.status = EntityStatus::Merged;
        repo.create(merged).await.unwrap();

        let matches = repo
            .find_by_identity_key_all("test_space", "acme")
            .await
            .unwrap();
        assert_eq!(matches.len(), 2);
        assert!(matches
            .iter()
            .all(|entity| entity.status != EntityStatus::Merged));
    }

    #[tokio::test]
    async fn test_embedded_search_ranks_alias_and_identity_keys_before_shell_names() {
        let repo = setup_embedded_test_repo().await;

        let mut canonical = create_test_entity("alex@example.test");
        canonical.aliases = vec![
            "Alex".to_string(),
            "Alex Example".to_string(),
            "alexex".to_string(),
        ];
        canonical.identity_keys = vec![
            "email:alex@example.test".to_string(),
            "person:alex-example".to_string(),
        ];
        repo.create(canonical).await.unwrap();

        repo.create(create_test_entity("Alex")).await.unwrap();

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Alex"),
            )
            .await
            .unwrap();

        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0].display_name, "alex@example.test");
        assert_eq!(result.results[1].display_name, "Alex");

        let full_name_result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Alex Example"),
            )
            .await
            .unwrap();

        assert_eq!(full_name_result.results.len(), 1);
        assert_eq!(
            full_name_result.results[0].display_name,
            "alex@example.test"
        );
    }

    #[tokio::test]
    async fn test_search_zero_limit_has_no_cursor() {
        let repo = setup_test_repo().await;

        repo.create(create_test_entity("Alice Smith"))
            .await
            .unwrap();
        repo.create(create_test_entity("Alice Johnson"))
            .await
            .unwrap();

        let result = repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Alice")
                    .with_limit(0),
            )
            .await
            .unwrap();

        assert!(result.results.is_empty());
        assert!(!result.has_more);
        assert!(result.cursor.is_none());
        assert_eq!(result.total, 2);
    }

    #[tokio::test]
    async fn test_hard_delete() {
        let repo = setup_test_repo().await;
        let entity = create_test_entity("To Hard Delete");
        let id = entity.base.id.clone();

        repo.create(entity).await.unwrap();
        repo.hard_delete("test_space", &id).await.unwrap();

        let result = repo.get_with_deleted("test_space", &id).await.unwrap();
        assert!(result.is_none());
    }

    async fn assert_name_embedding_roundtrip(repo: &EntityRepository) {
        let entity = create_test_entity("Riley Kim");
        let id = entity.base.id.clone();
        repo.create(entity).await.unwrap();

        repo.set_name_embedding("test_space", &id, &[1.0, 0.0, 0.0], "test-model", "hash-1")
            .await
            .unwrap();

        // A same-direction query vector ranks the entity first.
        let hits = repo
            .vector_search_entities("test_space", &[1.0, 0.0, 0.0], 5)
            .await
            .unwrap();
        assert_eq!(
            hits.first().map(|hit| hit.entity_id.clone()),
            Some(id.clone())
        );
        assert!(
            hits[0].score > 0.99,
            "cosine of identical vectors: {hits:?}"
        );

        // Candidates surface stored model + hash for staleness checks.
        let candidates = repo
            .list_embedding_candidates("test_space", 10)
            .await
            .unwrap();
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.entity_id == id)
            .expect("embedding candidate");
        assert_eq!(candidate.embedding_model.as_deref(), Some("test-model"));
        assert_eq!(candidate.embedding_text_hash.as_deref(), Some("hash-1"));
        assert_eq!(candidate.embedding_dims, Some(3));

        // Mismatched query dimensions must not error the search — rows are
        // guarded out instead (mixed dims occur right after a model change).
        let hits = repo
            .vector_search_entities("test_space", &[1.0, 0.0, 0.0, 0.0], 5)
            .await
            .unwrap();
        assert!(hits.is_empty(), "mismatched dims are filtered: {hits:?}");
    }

    #[tokio::test]
    async fn test_name_embedding_roundtrip() {
        let repo = setup_test_repo().await;
        assert_name_embedding_roundtrip(&repo).await;
    }

    #[tokio::test]
    async fn test_embedded_name_embedding_roundtrip() {
        let repo = setup_embedded_test_repo().await;
        assert_name_embedding_roundtrip(&repo).await;
    }
}
