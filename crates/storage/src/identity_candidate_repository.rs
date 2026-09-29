//! Identity candidate repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::entity::Entity;
use elephant_types::records::{IdentityCandidate, IdentityCandidateStatus};

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

const IDENTITY_CANDIDATE_PROJECTION: &str = r"
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
    entity_a,
    entity_b,
    signals,
    confidence,
    status,
    proposed_by,
    decided_by,
    decided_at
";

/// Database-only proxy for `IdentityCandidate` to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct IdentityCandidateDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    entity_a: Option<surrealdb::sql::Thing>,
    entity_b: Option<surrealdb::sql::Thing>,
    signals: serde_json::Value,
    confidence: Option<f64>,
    status: Option<IdentityCandidateStatus>,
    proposed_by: Option<String>,
    decided_by: Option<String>,
    decided_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Deserialize)]
struct IdentityCandidatePairDb {
    entity_a: Option<surrealdb::sql::Thing>,
    entity_b: Option<surrealdb::sql::Thing>,
}

impl TryFrom<IdentityCandidateDb> for IdentityCandidate {
    type Error = StorageError;

    fn try_from(db: IdentityCandidateDb) -> Result<Self, Self::Error> {
        fn required<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
            value.ok_or_else(|| StorageError::QueryError(format!("Missing {field}")))
        }

        Ok(Self {
            base: elephant_types::records::base::BaseFields {
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
            entity_a: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.entity_a
                        .ok_or_else(|| StorageError::QueryError("Missing entity_a".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            entity_b: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.entity_b
                        .ok_or_else(|| StorageError::QueryError("Missing entity_b".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            signals: db.signals,
            confidence: required(db.confidence, "confidence")?,
            status: required(db.status, "status")?,
            proposed_by: required(db.proposed_by, "proposed_by")?,
            decided_by: db.decided_by,
            decided_at: db.decided_at,
        })
    }
}

/// Identity candidate repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct IdentityCandidateRepository {
    client: StorageClient,
}

impl IdentityCandidateRepository {
    /// Creates a new identity candidate repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Gets pending candidates for a space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_pending(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            let mut candidates = store.scan::<IdentityCandidate>().await?;
            candidates.retain(|candidate| {
                candidate.base.space_id == space_id
                    && candidate.base.deleted_at.is_none()
                    && candidate.status == IdentityCandidateStatus::Pending
            });
            candidates.sort_by_key(|candidate| candidate.base.id.to_string());
            if candidates.len() > limit {
                candidates.truncate(limit);
            }
            return Ok(candidates);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE space_id = $space_id AND status = 'pending' AND deleted_at IS NONE LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidates: {e}"))
        })?;

        candidates
            .into_iter()
            .map(IdentityCandidate::try_from)
            .collect()
    }

    /// Checks if an identity candidate already exists for a pair of entities.
    /// Checks both orderings (a,b) and (b,a) to avoid duplicates.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    #[allow(clippy::similar_names)]
    pub async fn find_by_entity_pair(
        &self,
        space_id: &str,
        entity_a_id: &RecordId,
        entity_b_id: &RecordId,
    ) -> StorageResult<Option<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            let mut candidates = store.scan::<IdentityCandidate>().await?;
            candidates.retain(|candidate| {
                candidate.base.space_id == space_id
                    && candidate.base.deleted_at.is_none()
                    && ((candidate.entity_a.id() == entity_a_id
                        && candidate.entity_b.id() == entity_b_id)
                        || (candidate.entity_a.id() == entity_b_id
                            && candidate.entity_b.id() == entity_a_id))
            });
            candidates.sort_by_key(|candidate| candidate.base.id.to_string());
            return Ok(candidates.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE space_id = $space_id AND deleted_at IS NONE AND ((entity_a = $entity_a AND entity_b = $entity_b) OR (entity_a = $entity_b AND entity_b = $entity_a)) LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "entity_a" => crate::surreal::thing_from_record_id(entity_a_id),
                    "entity_b" => crate::surreal::thing_from_record_id(entity_b_id)
                },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidates: {e}"))
        })?;

        candidates
            .into_iter()
            .next()
            .map(IdentityCandidate::try_from)
            .transpose()
    }

    /// Returns all non-deleted candidate entity-pair keys for a space.
    ///
    /// This is intentionally a compact projection used by maintenance sweeps.
    /// Calling `find_by_entity_pair` for every discovered pair forces the
    /// embedded store to rescan/deserialise the full candidate table repeatedly.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or pair IDs cannot be parsed.
    pub async fn existing_pair_keys(
        &self,
        space_id: &str,
    ) -> StorageResult<HashSet<(String, String)>> {
        if let Some(store) = self.client.embedded_store() {
            let candidates = store.scan::<IdentityCandidate>().await?;
            return Ok(candidates
                .into_iter()
                .filter(|candidate| {
                    candidate.base.space_id == space_id && candidate.base.deleted_at.is_none()
                })
                .map(|candidate| {
                    identity_pair_key(candidate.entity_a.id(), candidate.entity_b.id())
                })
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                "SELECT entity_a, entity_b FROM identity_candidate WHERE space_id = $space_id AND deleted_at IS NONE",
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let candidates: Vec<IdentityCandidatePairDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidate pairs: {e}"))
        })?;

        candidates
            .into_iter()
            .map(|candidate| {
                let entity_a = candidate
                    .entity_a
                    .ok_or_else(|| StorageError::QueryError("Missing entity_a".to_string()))?;
                let entity_b = candidate
                    .entity_b
                    .ok_or_else(|| StorageError::QueryError("Missing entity_b".to_string()))?;
                let entity_a = crate::surreal::record_id_from_thing(&entity_a)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?;
                let entity_b = crate::surreal::record_id_from_thing(&entity_b)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?;
                Ok(identity_pair_key(&entity_a, &entity_b))
            })
            .collect()
    }

    /// Gets high-confidence candidates for a space.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_high_confidence(
        &self,
        space_id: &str,
        threshold: f64,
        limit: usize,
    ) -> StorageResult<Vec<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            let mut candidates = store.scan::<IdentityCandidate>().await?;
            candidates.retain(|candidate| {
                candidate.base.space_id == space_id
                    && candidate.base.deleted_at.is_none()
                    && candidate.status == IdentityCandidateStatus::Pending
                    && candidate.confidence >= threshold
            });
            candidates.sort_by_key(|candidate| candidate.base.id.to_string());
            if candidates.len() > limit {
                candidates.truncate(limit);
            }
            return Ok(candidates);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE space_id = $space_id AND status = 'pending' AND confidence >= $threshold AND deleted_at IS NONE LIMIT {limit}"
                ),
                crate::bindings! { "space_id" => space_id, "threshold" => threshold },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidates: {e}"))
        })?;

        candidates
            .into_iter()
            .map(IdentityCandidate::try_from)
            .collect()
    }

    /// Restores a soft-deleted identity candidate.
    ///
    /// # Errors
    ///
    /// Returns an error if the candidate is not found, not deleted, or the query fails.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<IdentityCandidate> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<IdentityCandidate>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {IDENTITY_CANDIDATE_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id
                },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse restored identity_candidate: {e}"))
        })?;

        let candidate_db = candidates.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Identity candidate not found in space: {id}"))
        })?;

        IdentityCandidate::try_from(candidate_db)
    }
}

fn identity_pair_key(left_entity_id: &RecordId, right_entity_id: &RecordId) -> (String, String) {
    let a = left_entity_id.to_string();
    let b = right_entity_id.to_string();
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

#[async_trait]
impl Repository<IdentityCandidate> for IdentityCandidateRepository {
    async fn create(&self, record: IdentityCandidate) -> StorageResult<IdentityCandidate> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {IDENTITY_CANDIDATE_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::identity_candidate_to_value(&record)?,
                },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created identity_candidate: {e}"))
        })?;

        let candidate_db = candidates.into_iter().next().ok_or_else(|| {
            StorageError::QueryError(
                "Create succeeded but identity_candidate not found".to_string(),
            )
        })?;

        IdentityCandidate::try_from(candidate_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<IdentityCandidate>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidate: {e}"))
        })?;

        Ok(candidates
            .into_iter()
            .next()
            .map(IdentityCandidate::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: IdentityCandidate,
    ) -> StorageResult<IdentityCandidate> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {IDENTITY_CANDIDATE_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::identity_candidate_to_value(&normalized)?,
                },
            )
            .await?;

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated identity_candidate: {e}"))
        })?;

        let candidate_db = candidates.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("Identity candidate not found in space: {id}"))
        })?;

        IdentityCandidate::try_from(candidate_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<IdentityCandidate>(space_id, id).await;
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
    ) -> StorageResult<Vec<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<IdentityCandidate>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let candidates: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidates: {e}"))
        })?;

        candidates
            .into_iter()
            .map(IdentityCandidate::try_from)
            .collect()
    }
}

#[async_trait]
impl SearchRepository<IdentityCandidate> for IdentityCandidateRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<IdentityCandidate>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search::<IdentityCandidate, _>(criteria, |_candidate, _criteria| true)
                .await;
        }

        #[derive(serde::Deserialize)]
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

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::IdentityCandidate)
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
            "SELECT {IDENTITY_CANDIDATE_PROJECTION} FROM identity_candidate {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let candidates_db: Vec<IdentityCandidateDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse identity_candidates: {e}"))
        })?;

        let mut candidates: Vec<IdentityCandidate> = candidates_db
            .into_iter()
            .map(IdentityCandidate::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = candidates.len() > criteria.limit;
        if has_more {
            candidates.pop();
        }

        let next_cursor = if has_more {
            candidates.last().map(|c| {
                c.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&c.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM identity_candidate {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> = crate::response::take_first(
            &mut count_response,
            "Failed to parse identity candidate count",
        )?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(
            candidates,
            next_cursor,
            has_more,
            total,
        ))
    }
}
