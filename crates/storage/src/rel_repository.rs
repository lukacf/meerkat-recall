//! Relationship (edge) repository implementation for `SurrealDB`.
//!
//! Per spec §11.2, rel records represent resolved entity-to-entity claims.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use std::str::FromStr;

use elephant_types::records::entity::Entity;
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, ValidityWindow};
use elephant_types::records::temporal::TemporalAudit;

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};

/// SQL projection for relationship records.
pub const REL_PROJECTION: &str = r"
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
    string::concat(meta::tb(`in`), ':', meta::id(`in`)) AS subject,
    string::concat(meta::tb(`out`), ':', meta::id(`out`)) AS object,
    rel_type,
    status,
    confidence,
    validity_window,
    evidence,
    temporal_audit
";

/// Graph neighbor hit with hop distance and rel type.
#[derive(Debug, Clone)]
pub struct GraphNeighbor {
    /// Neighbor entity ID.
    pub entity_id: RecordId,
    /// Hop distance from the start entity.
    pub distance: usize,
    /// Relationship type used to reach this neighbor.
    pub rel_type: String,
}

/// Database-only proxy for Rel to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub struct RelDb {
    /// Record ID.
    pub id: surrealdb::sql::Thing,
    /// Space ID.
    pub space_id: Option<String>,
    /// Creation timestamp.
    pub created_at: Option<chrono::DateTime<Utc>>,
    /// Last update timestamp.
    pub updated_at: Option<chrono::DateTime<Utc>>,
    /// Agent that created the record.
    pub created_by: Option<String>,
    /// Soft-deletion timestamp.
    pub deleted_at: Option<chrono::DateTime<Utc>>,
    /// Provenance metadata.
    pub provenance: Option<elephant_types::records::base::Provenance>,
    /// Security labels.
    pub security: Option<elephant_types::records::base::Security>,
    /// Subject references.
    pub subjects: Option<Vec<surrealdb::sql::Thing>>,
    /// Completeness of subjects.
    pub subjects_state: Option<elephant_types::records::base::SubjectsState>,
    /// Subject entity (incoming edge).
    pub subject: Option<String>,
    /// Object entity (outgoing edge).
    pub object: Option<String>,
    /// Type of relationship (predicate).
    pub rel_type: Option<String>,
    /// Status of the rel.
    pub status: Option<ClaimStatus>,
    /// Confidence score.
    pub confidence: Option<f32>,
    /// Validity window.
    pub validity_window: Option<ValidityWindow>,
    /// Evidence references.
    pub evidence: Option<Vec<EvidenceRef>>,
    /// Temporal audit info.
    pub temporal_audit: Option<TemporalAudit>,
}

impl TryFrom<RelDb> for Rel {
    type Error = StorageError;

    fn try_from(db: RelDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
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
                    .ok_or_else(|| StorageError::QueryError("Missing subjects".to_string()))?
                    .into_iter()
                    .map(|thing| {
                        crate::surreal::record_id_from_thing(&thing)
                            .map(Link::new)
                            .map_err(|e| StorageError::QueryError(e.to_string()))
                    })
                    .collect::<Result<Vec<Link<Entity>>, StorageError>>()?,
                subjects_state: db.subjects_state.ok_or_else(|| {
                    StorageError::QueryError("Missing subjects_state".to_string())
                })?,
            },
            subject: Link::new(
                RecordId::from_str(
                    db.subject.as_deref().ok_or_else(|| {
                        StorageError::QueryError("Missing subject link".to_string())
                    })?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            object: Link::new(
                RecordId::from_str(
                    db.object.as_deref().ok_or_else(|| {
                        StorageError::QueryError("Missing object link".to_string())
                    })?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            rel_type: db
                .rel_type
                .ok_or_else(|| StorageError::QueryError("Missing rel_type".to_string()))?,
            status: db
                .status
                .ok_or_else(|| StorageError::QueryError("Missing status".to_string()))?,
            confidence: db
                .confidence
                .ok_or_else(|| StorageError::QueryError("Missing confidence".to_string()))?,
            validity_window: db.validity_window,
            evidence: db
                .evidence
                .ok_or_else(|| StorageError::QueryError("Missing evidence".to_string()))?,
            temporal_audit: db.temporal_audit,
        })
    }
}

/// Relationship repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct RelRepository {
    client: StorageClient,
}

impl RelRepository {
    /// Restores a relationship only if it is unchanged since `authorized`, the row
    /// the caller's authorization was decided on.
    ///
    /// See [`crate::lifecycle_guard`]: the stored security, subjects, subjects
    /// state and exact lifecycle must still equal `authorized` in the same
    /// mutation that restores, and so must its subject and object endpoints (a
    /// hard delete and re-create can reuse the ID with other endpoints).
    ///
    /// # Errors
    ///
    /// Returns a conflict if the row is missing or changed, or an error if
    /// storage access fails.
    pub async fn restore_guarded(
        &self,
        space_id: &str,
        id: &RecordId,
        authorized: &Rel,
    ) -> StorageResult<Rel> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(mut current) = mutation.get::<Rel>(space_id, id, true)? else {
                        return Err(crate::lifecycle_guard::guard_conflict("Rel", id));
                    };
                    if !crate::lifecycle_guard::base_envelope_unchanged(
                        &current.base,
                        &authorized.base,
                    ) || current.subject != authorized.subject
                        || current.object != authorized.object
                    {
                        return Err(crate::lifecycle_guard::guard_conflict("Rel", id));
                    }
                    current.base.deleted_at = None;
                    current.base.updated_at = chrono::Utc::now();
                    Ok(mutation.replace(current)?)
                })
                .await;
        }

        let (condition, mut bindings) =
            crate::lifecycle_guard::base_envelope_condition(&authorized.base)?;
        bindings.insert(
            "id".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(id)),
        );
        bindings.insert(
            "space_id".to_string(),
            surrealdb::sql::Value::from(space_id),
        );
        bindings.insert(
            "guard_subject".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(
                authorized.subject.id(),
            )),
        );
        bindings.insert(
            "guard_object".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(
                authorized.object.id(),
            )),
        );
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() \
                     WHERE space_id = $space_id AND {condition} \
                     AND `in` = $guard_subject AND `out` = $guard_object RETURN {REL_PROJECTION}"
                ),
                bindings,
            )
            .await?;
        let rows: Vec<RelDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse guarded relationship restore: {e}"))
        })?;
        Rel::try_from(
            rows.into_iter()
                .next()
                .ok_or_else(|| crate::lifecycle_guard::guard_conflict("Rel", id))?,
        )
    }

    /// Creates a new relationship repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Returns entities within N hops of the start entity.
    ///
    /// Traverses both incoming and outgoing relationships.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_entities_within_n_hops(
        &self,
        space_id: &str,
        start: &RecordId,
        hops: usize,
    ) -> StorageResult<Vec<GraphNeighbor>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .graph_neighbors(space_id, start, hops)
                .await
                .map(|neighbors| {
                    neighbors
                        .into_iter()
                        .map(|neighbor| GraphNeighbor {
                            entity_id: neighbor.entity_id,
                            distance: neighbor.distance,
                            rel_type: neighbor.rel_type,
                        })
                        .collect()
                });
        }

        if hops == 0 {
            return Ok(Vec::new());
        }

        let mut visited: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
        visited.insert(start.clone());

        let mut frontier: Vec<RecordId> = vec![start.clone()];
        let mut results = Vec::new();

        for distance in 1..=hops {
            if frontier.is_empty() {
                break;
            }

            let neighbors = self.fetch_neighbors(space_id, &frontier).await?;
            let mut next_frontier = Vec::new();

            for (neighbor, rel_type) in neighbors {
                if visited.insert(neighbor.clone()) {
                    results.push(GraphNeighbor {
                        entity_id: neighbor.clone(),
                        distance,
                        rel_type,
                    });
                    next_frontier.push(neighbor);
                }
            }

            frontier = next_frontier;
        }

        Ok(results)
    }

    async fn fetch_neighbors(
        &self,
        space_id: &str,
        frontier: &[RecordId],
    ) -> StorageResult<Vec<(RecordId, String)>> {
        #[derive(Deserialize)]
        struct NeighborRow {
            neighbor: surrealdb::sql::Thing,
            rel_type: String,
        }

        let frontier_values = surrealdb::sql::Value::Array(crate::surreal::things_from_record_ids(
            frontier.iter().cloned(),
        ));

        let mut outgoing_response = self
            .client
            .query_bind(
                "SELECT `out` AS neighbor, rel_type FROM rel \
                 WHERE space_id = $space_id AND deleted_at IS NONE AND `in` IN $frontier",
                crate::bindings! {
                    "space_id" => space_id,
                    "frontier" => frontier_values.clone(),
                },
            )
            .await?;

        let mut incoming_response = self
            .client
            .query_bind(
                "SELECT `in` AS neighbor, rel_type FROM rel \
                 WHERE space_id = $space_id AND deleted_at IS NONE AND `out` IN $frontier",
                crate::bindings! {
                    "space_id" => space_id,
                    "frontier" => frontier_values,
                },
            )
            .await?;

        let mut neighbors: Vec<(RecordId, String)> = Vec::new();

        let outgoing_rows: Vec<NeighborRow> = outgoing_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rel neighbors: {e}")))?;
        for row in outgoing_rows {
            let id = crate::surreal::record_id_from_thing(&row.neighbor)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            neighbors.push((id, row.rel_type));
        }

        let incoming_rows: Vec<NeighborRow> = incoming_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rel neighbors: {e}")))?;
        for row in incoming_rows {
            let id = crate::surreal::record_id_from_thing(&row.neighbor)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            neighbors.push((id, row.rel_type));
        }

        Ok(neighbors)
    }
}

async fn active_entity_ids(
    store: &std::sync::Arc<crate::embedded::EmbeddedStore>,
    space_id: &str,
) -> StorageResult<std::collections::HashSet<RecordId>> {
    Ok(store
        .scan::<elephant_types::records::Entity>()
        .await?
        .into_iter()
        .filter(|entity| {
            entity.base.deleted_at.is_none()
                && (space_id.is_empty() || entity.base.space_id == space_id)
        })
        .map(|entity| entity.base.id)
        .collect())
}

#[async_trait]
impl Repository<Rel> for RelRepository {
    async fn create(&self, record: Rel) -> StorageResult<Rel> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let query = r"
            RELATE $subject->$rel_id->$object
            CONTENT $record;
        ";

        let record_value = crate::surreal::rel_to_value(&record)?;
        let mut create_response = self
            .client
            .query_bind(
                query,
                crate::bindings! {
                    "record" => record_value,
                    "subject" => crate::surreal::thing_from_record_id(record.subject.id()),
                    "object" => crate::surreal::thing_from_record_id(record.object.id()),
                    "rel_id" => crate::surreal::thing_from_record_id(&record.base.id),
                },
            )
            .await?;

        let _: surrealdb::Value = create_response.take(0).map_err(|e| {
            StorageError::classify_query_error(format!("Failed to create rel: {e}"))
        })?;

        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Rel>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rel: {e}")))?;

        Ok(rels.into_iter().next().map(Rel::try_from).transpose()?)
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Rel) -> StorageResult<Rel> {
        let target_space_id = space_id.to_string();
        let target_id = id.clone();
        let mut record = record;
        record.base.space_id.clone_from(&target_space_id);
        record.base.id.clone_from(&target_id);

        if let Some(store) = self.client.embedded_store() {
            let mut updated = record;
            updated.base.updated_at = Utc::now();
            return store.update_existing(space_id, id, updated).await;
        }

        record.base.updated_at = Utc::now();

        // One atomic request: MERGE the record fields, then repoint the graph
        // endpoints and return the projected row. The explicit BEGIN..COMMIT
        // makes the two statements one transaction, which keeps
        // query_bind_checked's conflict retry safe (a retryable conflict means
        // nothing was committed) and rolls back the MERGE if the endpoint
        // repoint fails, so no half-applied write can persist.
        //
        // BEGIN/COMMIT produce no entries in the response (verified against
        // surrealdb-core's executor), so statement indexes are unchanged:
        // take(1) below still addresses the second UPDATE.
        let query = format!(
            "BEGIN TRANSACTION; \
             UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE; \
             UPDATE $id SET `in` = $subject, `out` = $object WHERE space_id = $space_id AND deleted_at IS NONE RETURN {REL_PROJECTION}; \
             COMMIT TRANSACTION;"
        );

        let record_value = crate::surreal::rel_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &query,
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => record_value,
                    "subject" => crate::surreal::thing_from_record_id(record.subject.id()),
                    "object" => crate::surreal::thing_from_record_id(record.object.id()),
                },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(1)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse updated rel: {e}")))?;

        let rel_db = rels
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Rel not found in space: {id}")))?;

        Rel::try_from(rel_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Rel>(space_id, id).await;
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
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<Rel>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<Rel> for RelRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search(criteria, |rel: &Rel, criteria| {
                    criteria.query.as_ref().is_none_or(|query| {
                        rel.rel_type
                            .to_ascii_lowercase()
                            .contains(&query.to_ascii_lowercase())
                    })
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
            conditions
                .push("string::lowercase(rel_type) CONTAINS string::lowercase($query)".to_string());
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::Rel)
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
            "SELECT {REL_PROJECTION} FROM rel {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let rels_db: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        let mut rels: Vec<Rel> = rels_db
            .into_iter()
            .map(Rel::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = criteria.limit > 0 && rels.len() > criteria.limit;
        if has_more {
            rels.pop();
        } else if criteria.limit == 0 {
            rels.clear();
        }

        let next_cursor = if has_more {
            rels.last().map(|r| {
                let id_str = r.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        let count_query = format!("SELECT count() as total FROM rel {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> = count_response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rel count: {e}")))?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(rels, next_cursor, has_more, total))
    }
}

/// Additional rel-specific operations.
impl RelRepository {
    /// Gets a relationship including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Rel>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {REL_PROJECTION} FROM rel WHERE id = $id AND space_id = $space_id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rel: {e}")))?;

        rels.into_iter().next().map(Rel::try_from).transpose()
    }

    /// Soft-restores a relationship.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails or the relationship is not found.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Rel> {
        if let Some(store) = self.client.embedded_store() {
            return store.restore::<Rel>(space_id, id).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id SET deleted_at = NONE, updated_at = time::now() WHERE space_id = $space_id RETURN {REL_PROJECTION}"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse restored rel: {e}")))?;

        let rel_db = rels
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;

        Rel::try_from(rel_db)
    }

    /// Hard deletes a relationship (permanent).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete query fails.
    pub async fn hard_delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.hard_delete::<Rel>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "DELETE FROM $id WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    /// Upserts a relationship (create if not exists, update if exists).
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn upsert(&self, rel: Rel) -> StorageResult<Rel> {
        let space_id = rel.base.space_id.clone();
        let rid = rel.base.id.clone();

        // Check if the relationship already exists
        match self.get_with_deleted(&space_id, &rid).await? {
            Some(existing) => {
                // If soft-deleted, restore first
                if existing.base.deleted_at.is_some() {
                    let _ = self.restore(&space_id, &rid).await?;
                }
                // Update
                self.update(&space_id, &rid, rel).await
            }
            None => {
                // Create new
                Repository::create(self, rel).await
            }
        }
    }

    /// Finds relationships between two entities for a specific space.
    /// Standardizes "Ghost Edge" prevention by checking targets' `deleted_at` (§7.1).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_between(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            let active_entities = active_entity_ids(&store, space_id).await?;
            return Ok(store
                .scan::<Rel>()
                .await?
                .into_iter()
                .filter(|rel| {
                    rel.base.space_id == space_id
                        && rel.base.deleted_at.is_none()
                        && rel.subject.id() == subject_id
                        && rel.object.id() == object_id
                        && active_entities.contains(rel.subject.id())
                        && active_entities.contains(rel.object.id())
                })
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND `in` = $subject_id AND `out` = $object_id AND deleted_at IS NONE AND `in`.deleted_at IS NONE AND `out`.deleted_at IS NONE"
                ),
                crate::bindings! {
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                    "object_id" => crate::surreal::thing_from_record_id(object_id),
                    "space_id" => space_id,
                },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Finds outgoing relationships for an entity.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_outgoing(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<Rel>()
                .await?
                .into_iter()
                .filter(|rel| {
                    rel.base.space_id == space_id
                        && rel.base.deleted_at.is_none()
                        && rel.subject.id() == entity_id
                })
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE `in` = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(entity_id), "space_id" => space_id },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Finds incoming relationships for an entity.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_incoming(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store
                .scan::<Rel>()
                .await?
                .into_iter()
                .filter(|rel| {
                    rel.base.space_id == space_id
                        && rel.base.deleted_at.is_none()
                        && rel.object.id() == entity_id
                })
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE `out` = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(entity_id), "space_id" => space_id },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Finds relationships by type.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_type(&self, rel_type: &str) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            let active_entities = active_entity_ids(&store, "").await?;
            return Ok(store
                .scan::<Rel>()
                .await?
                .into_iter()
                .filter(|rel| {
                    rel.base.deleted_at.is_none()
                        && rel.rel_type == rel_type
                        && active_entities.contains(rel.subject.id())
                        && active_entities.contains(rel.object.id())
                })
                .collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE rel_type = $rel_type AND deleted_at IS NONE AND `in`.deleted_at IS NONE AND `out`.deleted_at IS NONE"
                ),
                crate::bindings! { "rel_type" => rel_type },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Finds a relationship by subject, object, and rel_type (including deleted).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_endpoints_with_deleted(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
    ) -> StorageResult<Option<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            return Ok(store.scan::<Rel>().await?.into_iter().find(|rel| {
                rel.base.space_id == space_id
                    && rel.subject.id() == subject_id
                    && rel.object.id() == object_id
                    && rel.rel_type == rel_type
            }));
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND `in` = $subject_id AND `out` = $object_id AND rel_type = $rel_type LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                    "object_id" => crate::surreal::thing_from_record_id(object_id),
                    "rel_type" => rel_type
                },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        match rels.into_iter().next() {
            Some(rel_db) => Ok(Some(Rel::try_from(rel_db)?)),
            None => Ok(None),
        }
    }

    /// Finds relationships for a truth slot (subject, object, rel_type combination).
    ///
    /// Per spec §12.3: truth slots are keyed by subject, object, and predicate.
    /// This returns all active claims that could contribute to a slot's truth.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rels: Vec<_> = store
                .entity_relations(space_id, subject_id)
                .await?
                .outgoing
                .into_iter()
                .filter(|rel| {
                    rel.base.space_id == space_id
                        && rel.base.deleted_at.is_none()
                        && rel.object.id() == object_id
                        && rel.rel_type == rel_type
                })
                .collect();
            rels.sort_by_key(|rel| rel.base.created_at);
            return Ok(rels);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND `in` = $subject_id AND `out` = $object_id AND rel_type = $rel_type AND deleted_at IS NONE ORDER BY created_at ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                    "object_id" => crate::surreal::thing_from_record_id(object_id),
                    "rel_type" => rel_type
                },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Finds all active relationships for a subject and predicate across all objects.
    ///
    /// Per spec §12: cross-object conflict detection needs every claim for a
    /// (subject, predicate) pair, regardless of object.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_subject_predicate(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        rel_type: &str,
    ) -> StorageResult<Vec<Rel>> {
        if let Some(store) = self.client.embedded_store() {
            let mut rels: Vec<_> = store
                .entity_relations(space_id, subject_id)
                .await?
                .outgoing
                .into_iter()
                .filter(|rel| {
                    rel.base.space_id == space_id
                        && rel.base.deleted_at.is_none()
                        && rel.rel_type == rel_type
                })
                .collect();
            rels.sort_by_key(|rel| rel.base.created_at);
            return Ok(rels);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REL_PROJECTION} FROM rel WHERE space_id = $space_id AND `in` = $subject_id AND rel_type = $rel_type AND deleted_at IS NONE ORDER BY created_at ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                    "rel_type" => rel_type
                },
            )
            .await?;

        let rels: Vec<RelDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse rels: {e}")))?;

        rels.into_iter().map(Rel::try_from).collect()
    }

    /// Repoints relationships from one entity to another.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or update fails.
    pub async fn repoint_entity(
        &self,
        old_rid: &RecordId,
        new_rid: &RecordId,
    ) -> StorageResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let old_rid = old_rid.clone();
            let new_rid = new_rid.clone();
            return store
                .mutate(move |mutation| {
                    let mut updated_endpoints = 0usize;
                    for mut rel in mutation.scan::<Rel>()? {
                        if rel.base.deleted_at.is_some()
                            || (rel.subject.id() != &old_rid && rel.object.id() != &old_rid)
                        {
                            continue;
                        }

                        let mut touched = 0usize;
                        if rel.subject.id() == &old_rid {
                            rel.subject = Link::new(new_rid.clone());
                            touched += 1;
                        }
                        if rel.object.id() == &old_rid {
                            rel.object = Link::new(new_rid.clone());
                            touched += 1;
                        }

                        if touched > 0 {
                            updated_endpoints += touched;
                            rel.base.updated_at = Utc::now();
                            let _ = mutation.replace(rel)?;
                        }
                    }
                    Ok(updated_endpoints)
                })
                .await;
        }

        #[derive(Deserialize)]
        struct IdResult {
            id: surrealdb::sql::Thing,
        }

        let mut response = self
            .client
            .query_bind(
                "SELECT id FROM rel WHERE deleted_at IS NONE AND (`in` = $old_id OR `out` = $old_id)",
                crate::bindings! { "old_id" => crate::surreal::thing_from_record_id(old_rid) },
            )
            .await?;

        let rel_ids: Vec<IdResult> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to list rels for repoint: {e}"))
        })?;
        let mut updated_endpoints = 0usize;

        for id_result in rel_ids {
            let mut response = self
                .client
                .query_bind(
                    &format!("SELECT {REL_PROJECTION} FROM rel WHERE id = $id"),
                    crate::bindings! { "id" => id_result.id.clone() },
                )
                .await?;
            let rels: Vec<RelDb> = response.take(0).map_err(|e| {
                StorageError::QueryError(format!("Failed to parse rel during repoint: {e}"))
            })?;
            let Some(rel_db) = rels.into_iter().next() else {
                continue;
            };
            let mut rel = Rel::try_from(rel_db)?;

            let mut updated_endpoints_count = 0;
            let changed = if rel.subject.id() == old_rid {
                rel.subject = Link::new(new_rid.clone());
                updated_endpoints_count += 1;
                true
            } else {
                false
            } | if rel.object.id() == old_rid {
                rel.object = Link::new(new_rid.clone());
                updated_endpoints_count += 1;
                true
            } else {
                false
            };

            updated_endpoints += updated_endpoints_count;

            if !changed {
                continue;
            }

            rel.base.updated_at = Utc::now();

            // Checked: a swallowed DELETE error followed by create() would
            // raise a spurious "already exists" Conflict or duplicate the rel.
            self.client
                .query_bind_checked(
                    "DELETE FROM $id",
                    crate::bindings! { "id" => id_result.id.clone() },
                )
                .await?;

            let _ = Repository::create(self, rel).await?;
        }

        Ok(updated_endpoints)
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

    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::rel::{EvidenceRef, TimeBasis};
    use elephant_types::{Link, RecordId, TableName};

    use std::sync::atomic::{AtomicU64, Ordering};
    use surrealdb::Uuid;

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(20000);

    const REL_SCHEMA: &str = r"
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
    ";

    async fn setup_test_repo() -> RelRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("rel_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(REL_SCHEMA).await.unwrap();

        RelRepository::new(client)
    }

    fn create_test_rel(rel_type: &str) -> Rel {
        let id = RecordId::new_v4(TableName::Rel);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        Rel {
            base,
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: rel_type.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    fn make_rel_db() -> RelDb {
        RelDb {
            id: crate::surreal::thing_from_record_id(&RecordId::new_v4(TableName::Rel)),
            space_id: Some("space_1".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("tester".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            subject: Some(RecordId::new_v4(TableName::Entity).to_string()),
            object: Some(RecordId::new_v4(TableName::Entity).to_string()),
            rel_type: Some("works_at".to_string()),
            status: Some(ClaimStatus::Candidate),
            confidence: Some(0.9),
            validity_window: None,
            evidence: Some(vec![EvidenceRef {
                assertion: Link::new(RecordId::new_v4(TableName::Assertion)),
                span: EvidenceSpan {
                    revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                    node: None,
                    start: 0,
                    end: 10,
                    quote_hash: "sha256:test_hash".to_string(),
                },
                time_basis: TimeBasis::ObservedAt,
                time_value: Utc::now(),
            }]),
            temporal_audit: None,
        }
    }

    #[test]
    fn test_rel_missing_space_id_errors() {
        let mut db = make_rel_db();
        db.space_id = None;
        let err = Rel::try_from(db).expect_err("missing space_id should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("space_id")));
    }

    #[test]
    fn test_rel_missing_rel_type_errors() {
        let mut db = make_rel_db();
        db.rel_type = None;
        let err = Rel::try_from(db).expect_err("missing rel_type should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("rel_type")));
    }

    #[test]
    fn test_rel_missing_status_errors() {
        let mut db = make_rel_db();
        db.status = None;
        let err = Rel::try_from(db).expect_err("missing status should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("status")));
    }

    #[test]
    fn test_rel_missing_confidence_errors() {
        let mut db = make_rel_db();
        db.confidence = None;
        let err = Rel::try_from(db).expect_err("missing confidence should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("confidence")));
    }

    #[test]
    fn test_rel_missing_evidence_errors() {
        let mut db = make_rel_db();
        db.evidence = None;
        let err = Rel::try_from(db).expect_err("missing evidence should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("evidence")));
    }

    #[tokio::test]
    async fn test_create_rel() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");

        let created = repo.create(rel.clone()).await.unwrap();

        assert_eq!(created.rel_type, "works_at");
        assert_eq!(created.status, ClaimStatus::Candidate);
    }

    #[tokio::test]
    async fn test_get_rel() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");
        let id = rel.base.id.clone();

        repo.create(rel).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().rel_type, "works_at");
    }

    #[tokio::test]
    async fn test_get_nonexistent_rel() {
        let repo = setup_test_repo().await;
        let id = RecordId::new_v4(TableName::Rel);

        let result = repo.get("test_space", &id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_update_rel() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");
        let id = rel.base.id.clone();

        let created = repo.create(rel).await.unwrap();

        let mut updated_rel = created.clone();
        updated_rel.status = ClaimStatus::Approved;
        updated_rel.confidence = 0.95;

        let updated = repo.update("test_space", &id, updated_rel).await.unwrap();
        assert_eq!(updated.status, ClaimStatus::Approved);
        assert!((updated.confidence - 0.95).abs() < f32::EPSILON);

        let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, ClaimStatus::Approved);
    }

    #[tokio::test]
    async fn test_soft_delete_rel() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");
        let id = rel.base.id.clone();

        repo.create(rel).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let result = repo.get("test_space", &id).await.unwrap();
        assert!(result.is_none());

        let deleted = repo.get_with_deleted("test_space", &id).await.unwrap();
        assert!(deleted.is_some());
        assert!(deleted.unwrap().base.deleted_at.is_some());
    }

    #[tokio::test]
    async fn test_restore_rel() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");
        let id = rel.base.id.clone();

        repo.create(rel).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        assert!(repo.get("test_space", &id).await.unwrap().is_none());

        let restored = repo.restore("test_space", &id).await.unwrap();
        assert!(restored.base.deleted_at.is_none());

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_list_rels() {
        let repo = setup_test_repo().await;

        for _i in 0..5 {
            let rel = create_test_rel("works_at");
            repo.create(rel).await.unwrap();
        }

        let rels = repo.list("test_space", 10, None).await.unwrap();
        assert_eq!(rels.len(), 5);
    }

    #[tokio::test]
    async fn test_hard_delete() {
        let repo = setup_test_repo().await;
        let rel = create_test_rel("works_at");
        let id = rel.base.id.clone();

        repo.create(rel).await.unwrap();
        repo.hard_delete("test_space", &id).await.unwrap();

        let result = repo.get_with_deleted("test_space", &id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_find_by_type() {
        let repo = setup_test_repo().await;

        repo.create(create_test_rel("works_at")).await.unwrap();
        repo.create(create_test_rel("lives_in")).await.unwrap();
        repo.create(create_test_rel("works_at")).await.unwrap();

        let rels = repo.find_by_type("works_at").await.unwrap();
        // Note: find_by_type doesn't filter by target entity existence in these tests as entities are fake
        assert_eq!(rels.len(), 2);
    }

    fn create_test_rel_with_entities(
        rel_type: &str,
        subject_id: RecordId,
        object_id: RecordId,
    ) -> Rel {
        let id = RecordId::new_v4(TableName::Rel);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Rel {
            base,
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: rel_type.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    #[tokio::test]
    async fn test_find_by_subject_predicate() {
        let repo = setup_test_repo().await;

        let subject = RecordId::new_v4(TableName::Entity);
        let object_a = RecordId::new_v4(TableName::Entity);
        let object_b = RecordId::new_v4(TableName::Entity);
        let other_subject = RecordId::new_v4(TableName::Entity);

        // Two rels for the same subject+predicate with different objects.
        repo.create(create_test_rel_with_entities(
            "works_at",
            subject.clone(),
            object_a.clone(),
        ))
        .await
        .unwrap();
        repo.create(create_test_rel_with_entities(
            "works_at",
            subject.clone(),
            object_b.clone(),
        ))
        .await
        .unwrap();

        // Different predicate for the same subject.
        repo.create(create_test_rel_with_entities(
            "lives_in",
            subject.clone(),
            object_a.clone(),
        ))
        .await
        .unwrap();

        // Same predicate, different subject.
        repo.create(create_test_rel_with_entities(
            "works_at",
            other_subject.clone(),
            object_a.clone(),
        ))
        .await
        .unwrap();

        // Soft-deleted rel for the same subject+predicate.
        let deleted = create_test_rel_with_entities("works_at", subject.clone(), object_b.clone());
        let deleted_id = deleted.base.id.clone();
        repo.create(deleted).await.unwrap();
        repo.delete("test_space", &deleted_id).await.unwrap();

        // Same subject+predicate in another space.
        let mut other_space =
            create_test_rel_with_entities("works_at", subject.clone(), object_a.clone());
        other_space.base.space_id = "other_space".to_string();
        repo.create(other_space).await.unwrap();

        let rels = repo
            .find_by_subject_predicate("test_space", &subject, "works_at")
            .await
            .unwrap();

        assert_eq!(rels.len(), 2);
        let mut objects: Vec<_> = rels.iter().map(|rel| rel.object.id().to_string()).collect();
        objects.sort();
        let mut expected = vec![object_a.to_string(), object_b.to_string()];
        expected.sort();
        assert_eq!(objects, expected);
        assert!(rels.iter().all(|rel| rel.subject.id() == &subject));
        assert!(rels.iter().all(|rel| rel.rel_type == "works_at"));
        assert!(rels.iter().all(|rel| rel.base.deleted_at.is_none()));
    }

    #[tokio::test]
    async fn test_repoint_entity() {
        let repo = setup_test_repo().await;

        // Create entity IDs
        let old_entity = RecordId::new_v4(TableName::Entity);
        let new_entity = RecordId::new_v4(TableName::Entity);
        let other_entity_1 = RecordId::new_v4(TableName::Entity);
        let other_entity_2 = RecordId::new_v4(TableName::Entity);

        // Create relationships:
        // rel1: old_entity -> other_entity_1 (should repoint subject)
        // rel2: other_entity_2 -> old_entity (should repoint object)
        // rel3: other_entity_1 -> other_entity_2 (should NOT change)
        let rel1 =
            create_test_rel_with_entities("works_at", old_entity.clone(), other_entity_1.clone());
        let rel2 =
            create_test_rel_with_entities("knows", other_entity_2.clone(), old_entity.clone());
        let rel3 = create_test_rel_with_entities(
            "lives_in",
            other_entity_1.clone(),
            other_entity_2.clone(),
        );

        let created1 = repo.create(rel1).await.unwrap();
        let created2 = repo.create(rel2).await.unwrap();
        let created3 = repo.create(rel3).await.unwrap();

        // Repoint old_entity to new_entity
        let count = repo.repoint_entity(&old_entity, &new_entity).await.unwrap();

        // Should have updated 2 relationships (rel1's subject and rel2's object)
        assert_eq!(count, 2, "Should have repointed 2 relationship endpoints");

        // Verify rel1: subject should now point to new_entity
        let updated1 = repo
            .get("test_space", &created1.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated1.subject.id().to_string(),
            new_entity.to_string(),
            "rel1's subject should point to new_entity"
        );
        assert_eq!(
            updated1.object.id().to_string(),
            other_entity_1.to_string(),
            "rel1's object should be unchanged"
        );

        // Verify rel2: object should now point to new_entity
        let updated2 = repo
            .get("test_space", &created2.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated2.subject.id().to_string(),
            other_entity_2.to_string(),
            "rel2's subject should be unchanged"
        );
        assert_eq!(
            updated2.object.id().to_string(),
            new_entity.to_string(),
            "rel2's object should point to new_entity"
        );

        // Verify rel3: should be completely unchanged
        let updated3 = repo
            .get("test_space", &created3.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated3.subject.id().to_string(),
            other_entity_1.to_string(),
            "rel3's subject should be unchanged"
        );
        assert_eq!(
            updated3.object.id().to_string(),
            other_entity_2.to_string(),
            "rel3's object should be unchanged"
        );
    }

    /// RCT round-trip test: verifies all Rel fields survive database serialization.
    #[tokio::test]
    async fn test_rel_roundtrip() {
        use elephant_types::records::assertion::EvidenceSpan;
        use elephant_types::records::rel::TimeBasis;
        use elephant_types::records::temporal::{RecencyBasis, TemporalAudit};

        let repo = setup_test_repo().await;

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let doc_revision_id = RecordId::new_v4(TableName::DocRevision);
        let assertion_id = RecordId::new_v4(TableName::Assertion);

        let now = chrono::Utc::now();
        let validity_window = Some(ValidityWindow::closed(
            now - chrono::Duration::days(30),
            now + chrono::Duration::days(365),
        ));

        let evidence = vec![EvidenceRef {
            assertion: Link::new(assertion_id.clone()),
            span: EvidenceSpan {
                revision: Link::new(doc_revision_id),
                node: None,
                start: 100,
                end: 200,
                quote_hash: "sha256:test_hash".to_string(),
            },
            time_basis: TimeBasis::ObservedAt,
            time_value: now,
        }];

        let temporal_audit = TemporalAudit {
            time_basis: RecencyBasis::SourceEventTime,
            time_refinement: None,
        };

        let id = RecordId::new_v4(TableName::Rel);
        let mut base = BaseFields::new(
            id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 2,
            labels: vec!["confidential".to_string()],
            handling: vec!["noforn".to_string()],
        };

        let original = Rel {
            base,
            rel_type: "employed_by".to_string(),
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            status: ClaimStatus::Approved,
            confidence: 0.87,
            validity_window: validity_window.clone(),
            evidence: evidence.clone(),
            temporal_audit: Some(temporal_audit.clone()),
        };

        // Create and retrieve
        let _created = repo.create(original.clone()).await.expect("create rel");
        let retrieved = repo
            .get("test_space", &id)
            .await
            .expect("get rel")
            .expect("rel exists");

        // Verify all fields survive round-trip
        assert_eq!(retrieved.rel_type, "employed_by", "rel_type round-trip");
        assert_eq!(
            *retrieved.subject.id(),
            *original.subject.id(),
            "subject round-trip"
        );
        assert_eq!(
            *retrieved.object.id(),
            *original.object.id(),
            "object round-trip"
        );
        assert_eq!(retrieved.status, ClaimStatus::Approved, "status round-trip");
        assert!(
            (retrieved.confidence - 0.87).abs() < 0.001,
            "confidence round-trip"
        );

        // Validity window
        assert!(
            retrieved.validity_window.is_some(),
            "validity_window round-trip"
        );
        let vw = retrieved.validity_window.unwrap();
        assert!(vw.start.is_some(), "validity_window.start round-trip");
        assert!(vw.end.is_some(), "validity_window.end round-trip");

        // Evidence
        assert_eq!(retrieved.evidence.len(), 1, "evidence count round-trip");
        assert_eq!(
            retrieved.evidence[0].span.start, 100,
            "evidence span.start round-trip"
        );
        assert_eq!(
            retrieved.evidence[0].span.end, 200,
            "evidence span.end round-trip"
        );
        assert_eq!(
            retrieved.evidence[0].span.quote_hash, "sha256:test_hash",
            "evidence quote_hash round-trip"
        );

        // Temporal audit
        assert!(
            retrieved.temporal_audit.is_some(),
            "temporal_audit round-trip"
        );
        let ta = retrieved.temporal_audit.unwrap();
        assert_eq!(
            ta.time_basis,
            RecencyBasis::SourceEventTime,
            "time_basis round-trip"
        );

        // Security envelope (ABAC inheritance)
        assert_eq!(
            retrieved.base.security.level, 2,
            "security level round-trip"
        );
        assert!(
            retrieved
                .base
                .security
                .labels
                .contains(&"confidential".to_string()),
            "security labels round-trip"
        );
    }

    #[tokio::test]
    async fn test_repoint_entity_skips_deleted() {
        let repo = setup_test_repo().await;

        let old_entity = RecordId::new_v4(TableName::Entity);
        let new_entity = RecordId::new_v4(TableName::Entity);
        let other_entity = RecordId::new_v4(TableName::Entity);

        // Create a relationship and soft-delete it
        let rel =
            create_test_rel_with_entities("works_at", old_entity.clone(), other_entity.clone());
        let created = repo.create(rel).await.unwrap();
        repo.delete("test_space", &created.base.id).await.unwrap();

        // Repoint should skip the deleted relationship
        let count = repo.repoint_entity(&old_entity, &new_entity).await.unwrap();

        assert_eq!(count, 0, "Should not repoint deleted relationships");
    }

    /// Guarded restore acts only on the snapshot the caller was authorized on.
    async fn assert_restore_guarded_lifecycle(repo: &RelRepository, space: &str, record: Rel) {
        let id = record.base.id.clone();
        repo.create(record).await.unwrap();

        // A fresh tombstone snapshot restores.
        repo.delete(space, &id).await.unwrap();
        let tombstone = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let restored = repo.restore_guarded(space, &id, &tombstone).await.unwrap();
        assert!(restored.base.deleted_at.is_none());

        // Tombstone ABA: snapshot D1, restored and deleted again as D2; the
        // stale restore from D1 conflicts and the row stays deleted.
        repo.delete(space, &id).await.unwrap();
        let tombstone_d1 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        repo.restore(space, &id).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        repo.delete(space, &id).await.unwrap();
        let result = repo.restore_guarded(space, &id, &tombstone_d1).await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
        assert!(repo.get(space, &id).await.unwrap().is_none());

        // Another space never matches.
        let tombstone_d2 = repo.get_with_deleted(space, &id).await.unwrap().unwrap();
        let result = repo
            .restore_guarded("other_space", &id, &tombstone_d2)
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "{result:?}"
        );
    }

    /// A row re-created under the same ID with other endpoints is not the
    /// authorized row, even when its envelope and lifecycle are identical.
    async fn assert_restore_guarded_endpoints(repo: &RelRepository, space: &str) {
        let subject = RecordId::new_v4(TableName::Entity);
        let object = RecordId::new_v4(TableName::Entity);
        let record = create_test_rel_with_entities("works_at", subject.clone(), object.clone());
        let id = record.base.id.clone();
        let authorized = repo.create(record.clone()).await.unwrap();

        for (new_subject, new_object) in [
            (subject.clone(), RecordId::new_v4(TableName::Entity)),
            (RecordId::new_v4(TableName::Entity), object.clone()),
        ] {
            repo.hard_delete(space, &id).await.unwrap();
            let mut replacement =
                create_test_rel_with_entities("works_at", new_subject.clone(), new_object.clone());
            replacement.base = record.base.clone();
            repo.create(replacement).await.unwrap();

            let result = repo.restore_guarded(space, &id, &authorized).await;
            assert!(
                matches!(result, Err(StorageError::Conflict(_))),
                "{result:?}"
            );
            let stored = repo.get(space, &id).await.unwrap().unwrap();
            assert_eq!(stored.subject.id(), &new_subject);
            assert_eq!(stored.object.id(), &new_object);
        }
    }

    #[tokio::test]
    async fn test_restore_guarded_endpoints_surreal() {
        let repo = setup_test_repo().await;
        assert_restore_guarded_endpoints(&repo, "test_space").await;
    }

    #[tokio::test]
    async fn test_restore_guarded_endpoints_embedded() {
        let repo = RelRepository::new(StorageClient::new_embedded().await.unwrap());
        assert_restore_guarded_endpoints(&repo, "test_space").await;
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_surreal() {
        let repo = setup_test_repo().await;
        assert_restore_guarded_lifecycle(&repo, "test_space", create_test_rel("works_at")).await;
    }

    #[tokio::test]
    async fn test_restore_guarded_lifecycle_embedded() {
        let repo = RelRepository::new(StorageClient::new_embedded().await.unwrap());
        assert_restore_guarded_lifecycle(&repo, "test_space", create_test_rel("works_at")).await;
    }
}
