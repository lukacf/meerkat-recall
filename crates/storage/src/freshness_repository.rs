//! Freshness repository implementation for `SurrealDB`.
//!
//! Per spec §13, freshness scores are derived metadata for ranking.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use surrealdb::sql::Value;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::freshness::{DecayModel, Freshness, FreshnessFactors};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

const FRESHNESS_PROJECTION: &str = r"
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
    target,
    target_type,
    decay_model,
    score,
    confidence,
    rationale,
    factors,
    computed_at,
    next_review_at,
    demote,
    demotion_threshold
";

/// Database-only proxy for Freshness.
#[derive(Deserialize)]
struct FreshnessDb {
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
    target: Option<String>,
    target_type: Option<String>,
    decay_model: Option<DecayModel>,
    score: Option<f32>,
    confidence: Option<f32>,
    rationale: Option<String>,
    factors: Option<FreshnessFactors>,
    computed_at: Option<chrono::DateTime<Utc>>,
    next_review_at: Option<chrono::DateTime<Utc>>,
    demote: Option<bool>,
    demotion_threshold: Option<f32>,
}

impl TryFrom<FreshnessDb> for Freshness {
    type Error = StorageError;

    fn try_from(db: FreshnessDb) -> Result<Self, Self::Error> {
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
            target: db
                .target
                .ok_or_else(|| StorageError::QueryError("Missing target".to_string()))?,
            target_type: db
                .target_type
                .ok_or_else(|| StorageError::QueryError("Missing target_type".to_string()))?,
            decay_model: required_field(db.decay_model, "decay_model")?,
            score: required_field(db.score, "score")?,
            confidence: required_field(db.confidence, "confidence")?,
            rationale: db.rationale,
            factors: required_field(db.factors, "factors")?,
            computed_at: db
                .computed_at
                .ok_or_else(|| StorageError::QueryError("Missing computed_at".to_string()))?,
            next_review_at: db.next_review_at,
            demote: required_field(db.demote, "demote")?,
            demotion_threshold: db.demotion_threshold,
        })
    }
}

/// Freshness repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct FreshnessRepository {
    client: StorageClient,
}

impl FreshnessRepository {
    /// Creates a new freshness repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a Freshness to a SurrealDB value for insertion.
    fn freshness_to_value(item: &Freshness) -> StorageResult<surrealdb::sql::Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("target".to_string(), item.target.clone().into());
        obj.insert("target_type".to_string(), item.target_type.clone().into());
        obj.insert(
            "decay_model".to_string(),
            crate::surreal::enum_to_surreal(&item.decay_model)?,
        );
        obj.insert("score".to_string(), item.score.into());
        obj.insert("confidence".to_string(), item.confidence.into());
        obj.insert(
            "rationale".to_string(),
            item.rationale
                .as_ref()
                .map_or(Value::None, |s| s.clone().into()),
        );

        let factors_json = serde_json::to_value(&item.factors).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize freshness.factors: {e}"))
        })?;
        obj.insert(
            "factors".to_string(),
            crate::surreal::json_to_surreal(factors_json)?,
        );

        obj.insert(
            "computed_at".to_string(),
            crate::surreal::datetime_from_chrono(item.computed_at).into(),
        );
        obj.insert(
            "next_review_at".to_string(),
            item.next_review_at
                .map(crate::surreal::datetime_from_chrono)
                .map_or(Value::None, Value::from),
        );
        obj.insert("demote".to_string(), item.demote.into());
        obj.insert(
            "demotion_threshold".to_string(),
            item.demotion_threshold.map_or(Value::None, Value::from),
        );

        Ok(Value::Object(obj))
    }
}

#[async_trait]
impl Repository<Freshness> for FreshnessRepository {
    async fn create(&self, record: Freshness) -> StorageResult<Freshness> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::freshness_to_value(&record)?;
        let write_response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;
        write_response.check()?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {FRESHNESS_PROJECTION} FROM freshness WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let items: Vec<FreshnessDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created freshness: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but freshness not found".to_string())
        })?;

        Freshness::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Freshness>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Freshness>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {FRESHNESS_PROJECTION} FROM freshness WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<FreshnessDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse freshness: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(Freshness::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Freshness,
    ) -> StorageResult<Freshness> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {FRESHNESS_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::freshness_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<FreshnessDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated freshness: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Freshness not found in space: {id}")))?;

        Freshness::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Freshness>(space_id, id).await;
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
    ) -> StorageResult<Vec<Freshness>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<Freshness>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {FRESHNESS_PROJECTION} FROM freshness WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {FRESHNESS_PROJECTION} FROM freshness WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<FreshnessDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse freshnesses: {e}")))?;

        items.into_iter().map(Freshness::try_from).collect()
    }
}

impl FreshnessRepository {
    /// Query freshness records that are due for review.
    ///
    /// Returns records where `next_review_at <= now` or `next_review_at IS NONE`,
    /// ordered by `next_review_at` ascending (nulls first).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_due_for_review(
        &self,
        space_id: &str,
        now: chrono::DateTime<Utc>,
        limit: usize,
    ) -> StorageResult<Vec<Freshness>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<Freshness>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.next_review_at.is_none_or(|next| next <= now)
            });
            items.sort_by_key(|item| {
                (
                    item.next_review_at.is_some(),
                    item.next_review_at,
                    item.base.id.to_string(),
                )
            });
            if items.len() > limit {
                items.truncate(limit);
            }
            return Ok(items);
        }

        let query = format!(
            "SELECT {FRESHNESS_PROJECTION} FROM freshness \
             WHERE space_id = $space_id \
             AND deleted_at IS NONE \
             AND (next_review_at IS NONE OR next_review_at <= $now) \
             ORDER BY next_review_at ASC \
             LIMIT {limit}"
        );

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "now" => now
                },
            )
            .await?;

        let items: Vec<FreshnessDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse freshnesses: {e}")))?;

        items.into_iter().map(Freshness::try_from).collect()
    }

    /// Finds the freshness record for a specific target.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_target(
        &self,
        space_id: &str,
        target: &str,
    ) -> StorageResult<Option<Freshness>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<Freshness>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.target == target
            });
            items.sort_by_key(|item| {
                (
                    std::cmp::Reverse(item.computed_at),
                    item.base.id.to_string(),
                )
            });
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {FRESHNESS_PROJECTION} FROM freshness \
                     WHERE space_id = $space_id \
                     AND target = $target \
                     AND deleted_at IS NONE \
                     ORDER BY computed_at DESC \
                     LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "target" => target
                },
            )
            .await?;

        let items: Vec<FreshnessDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse freshness: {e}")))?;

        items
            .into_iter()
            .next()
            .map(Freshness::try_from)
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81600);

    const FRESHNESS_SCHEMA: &str = r"
        DEFINE TABLE freshness SCHEMALESS;
    ";

    async fn setup_test_repo() -> FreshnessRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("freshness_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(FRESHNESS_SCHEMA).await.unwrap();

        FreshnessRepository::new(client)
    }

    fn create_test_freshness() -> Freshness {
        let id = RecordId::new_v4(TableName::Freshness);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        Freshness {
            base,
            target: "rel:abc123".to_string(),
            target_type: "rel".to_string(),
            decay_model: DecayModel::Exponential,
            score: 0.85,
            confidence: 0.9,
            rationale: Some("Recent evidence with high source reliability".to_string()),
            factors: FreshnessFactors {
                evidence_age_days: 30.0,
                conflict_severity: 0.1,
                evidence_strength: 0.9,
                source_reliability: 0.95,
                corroboration_count: 3,
                custom: std::collections::HashMap::new(),
            },
            computed_at: Utc::now(),
            next_review_at: None,
            demote: false,
            demotion_threshold: None,
        }
    }

    #[tokio::test]
    async fn test_create_freshness() {
        let repo = setup_test_repo().await;
        let item = create_test_freshness();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.target, "rel:abc123");
        assert_eq!(created.decay_model, DecayModel::Exponential);
        assert!((created.score - 0.85).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn test_get_freshness() {
        let repo = setup_test_repo().await;
        let item = create_test_freshness();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_freshness_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_freshness();
        item.factors.evidence_age_days = 45.5;
        item.factors.corroboration_count = 5;
        item.demote = true;
        item.demotion_threshold = Some(0.3);

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert!((created.factors.evidence_age_days - 45.5).abs() < f64::EPSILON);
        assert_eq!(created.factors.corroboration_count, 5);
        assert!(created.demote);
        assert_eq!(created.demotion_threshold, Some(0.3));
    }

    #[tokio::test]
    async fn test_update_freshness() {
        let repo = setup_test_repo().await;
        let item = create_test_freshness();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.score = 0.5;
        updated.demote = true;
        updated.demotion_threshold = Some(0.6);
        updated.rationale = Some("Score dropped below threshold".to_string());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!((result.score - 0.5).abs() < f32::EPSILON);
        assert!(result.demote);
        assert_eq!(result.demotion_threshold, Some(0.6));
    }

    #[tokio::test]
    async fn test_delete_freshness() {
        let repo = setup_test_repo().await;
        let item = create_test_freshness();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
