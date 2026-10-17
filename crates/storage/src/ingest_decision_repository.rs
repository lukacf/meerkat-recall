//! Ingest decision repository implementation for `SurrealDB`.
//!
//! Per spec §6, ingest decisions store triage results for sources.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::ingest::{BudgetSnapshot, IngestDecision, IngestMode};
use elephant_types::records::source::Source;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

pub(crate) const INGEST_DECISION_PROJECTION: &str = r"
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
    source,
    decision,
    reason_codes,
    confidence,
    estimated_cost,
    cost_ledger_id,
    budget_snapshot,
    policy_version,
    downgrade_reason,
    original_mode
";

/// Database-only proxy for IngestDecision.
#[derive(Deserialize)]
pub(crate) struct IngestDecisionDb {
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
    source: Option<Link<Source>>,
    decision: Option<IngestMode>,
    reason_codes: Option<Vec<String>>,
    confidence: Option<f32>,
    estimated_cost: Option<f64>,
    cost_ledger_id: Option<RecordId>,
    budget_snapshot: Option<BudgetSnapshot>,
    policy_version: Option<String>,
    downgrade_reason: Option<String>,
    original_mode: Option<IngestMode>,
}

impl TryFrom<IngestDecisionDb> for IngestDecision {
    type Error = StorageError;

    fn try_from(db: IngestDecisionDb) -> Result<Self, Self::Error> {
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
            source: db
                .source
                .ok_or_else(|| StorageError::QueryError("Missing source".to_string()))?,
            decision: required_field(db.decision, "decision")?,
            reason_codes: required_field(db.reason_codes, "reason_codes")?,
            confidence: required_field(db.confidence, "confidence")?,
            estimated_cost: required_field(db.estimated_cost, "estimated_cost")?,
            cost_ledger_id: db.cost_ledger_id,
            budget_snapshot: db.budget_snapshot,
            policy_version: db.policy_version,
            downgrade_reason: db.downgrade_reason,
            original_mode: db.original_mode,
        })
    }
}

/// Ingest decision repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct IngestDecisionRepository {
    client: StorageClient,
}

impl IngestDecisionRepository {
    /// Creates a new ingest decision repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts an IngestDecision to a SurrealDB value for insertion.
    pub(crate) fn ingest_decision_to_value(
        item: &IngestDecision,
    ) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;
        let mut object = crate::surreal::base_fields_to_object(&item.base)?;
        object.insert(
            "source".into(),
            crate::surreal::thing_from_record_id(item.source.id()).into(),
        );
        object.insert(
            "decision".into(),
            crate::surreal::enum_to_surreal(&item.decision)?,
        );
        object.insert(
            "reason_codes".into(),
            Value::Array(item.reason_codes.clone().into()),
        );
        object.insert("confidence".into(), Value::from(f64::from(item.confidence)));
        object.insert("estimated_cost".into(), Value::from(item.estimated_cost));
        object.insert(
            "cost_ledger_id".into(),
            item.cost_ledger_id.as_ref().map_or(Value::None, |id| {
                crate::surreal::thing_from_record_id(id).into()
            }),
        );
        object.insert(
            "budget_snapshot".into(),
            match &item.budget_snapshot {
                Some(snapshot) => crate::surreal::json_to_surreal(
                    serde_json::to_value(snapshot)
                        .map_err(|error| StorageError::SerializationError(error.to_string()))?,
                )?,
                None => Value::None,
            },
        );
        object.insert(
            "policy_version".into(),
            item.policy_version
                .as_ref()
                .map_or(Value::None, |value| Value::from(value.clone())),
        );
        object.insert(
            "downgrade_reason".into(),
            item.downgrade_reason
                .as_ref()
                .map_or(Value::None, |value| Value::from(value.clone())),
        );
        object.insert(
            "original_mode".into(),
            item.original_mode
                .as_ref()
                .map(crate::surreal::enum_to_surreal)
                .transpose()?
                .unwrap_or(Value::None),
        );
        Ok(Value::Object(object))
    }
}

impl IngestDecisionRepository {
    /// Finds the most recent ingest decision for a source.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_latest_by_source(
        &self,
        space_id: &str,
        source_id: &RecordId,
    ) -> StorageResult<Option<IngestDecision>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<IngestDecision>().await?;
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.source.id() == source_id
            });
            items.sort_by_key(|item| {
                (
                    std::cmp::Reverse(item.base.created_at),
                    item.base.id.to_string(),
                )
            });
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {INGEST_DECISION_PROJECTION} FROM ingest_decision WHERE space_id = $space_id AND deleted_at IS NONE AND (source = $source_id OR source = $source_id_str) ORDER BY created_at DESC LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "source_id" => crate::surreal::thing_from_record_id(source_id),
                    "source_id_str" => source_id.to_string(),
                },
            )
            .await?;

        let items: Vec<IngestDecisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse ingest_decision by source: {e}"))
        })?;

        items
            .into_iter()
            .next()
            .map(IngestDecision::try_from)
            .transpose()
    }
}

#[async_trait]
impl Repository<IngestDecision> for IngestDecisionRepository {
    async fn create(&self, record: IngestDecision) -> StorageResult<IngestDecision> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::ingest_decision_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {INGEST_DECISION_PROJECTION}"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<IngestDecisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created ingest_decision: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but ingest_decision not found".to_string())
        })?;

        IngestDecision::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<IngestDecision>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<IngestDecision>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {INGEST_DECISION_PROJECTION} FROM ingest_decision WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<IngestDecisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse ingest_decision: {e}"))
        })?;

        Ok(items
            .into_iter()
            .next()
            .map(IngestDecision::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: IngestDecision,
    ) -> StorageResult<IngestDecision> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {INGEST_DECISION_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::ingest_decision_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<IngestDecisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated ingest_decision: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("IngestDecision not found in space: {id}"))
        })?;

        IngestDecision::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<IngestDecision>(space_id, id).await;
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
    ) -> StorageResult<Vec<IngestDecision>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<IngestDecision>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {INGEST_DECISION_PROJECTION} FROM ingest_decision WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {INGEST_DECISION_PROJECTION} FROM ingest_decision WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<IngestDecisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse ingest_decisions: {e}"))
        })?;

        items.into_iter().map(IngestDecision::try_from).collect()
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
    use chrono::Duration;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81300);

    const INGEST_DECISION_SCHEMA: &str = r"
        DEFINE TABLE ingest_decision SCHEMALESS;
    ";

    async fn setup_test_repo() -> IngestDecisionRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("ingest_decision_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(INGEST_DECISION_SCHEMA).await.unwrap();

        IngestDecisionRepository::new(client)
    }

    fn create_test_ingest_decision() -> IngestDecision {
        let id = RecordId::new_v4(TableName::IngestDecision);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let source_id = RecordId::new_v4(TableName::Source);

        IngestDecision {
            base,
            source: Link::new(source_id),
            decision: IngestMode::Retrieval,
            reason_codes: vec!["high_value".to_string(), "budget_ok".to_string()],
            confidence: 0.85,
            estimated_cost: 0.05,
            cost_ledger_id: Some(RecordId::new_v4(TableName::CostLedgerDaily)),
            budget_snapshot: None,
            policy_version: Some("v1.0".to_string()),
            downgrade_reason: None,
            original_mode: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_ingest_decision() {
        let repo = setup_test_repo().await;
        let item = create_test_ingest_decision();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.decision, IngestMode::Retrieval);
        assert!((created.confidence - 0.85).abs() < f32::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_ingest_decision() {
        let repo = setup_test_repo().await;
        let item = create_test_ingest_decision();
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
    async fn test_find_latest_by_source() {
        let repo = setup_test_repo().await;
        let first = create_test_ingest_decision();
        let source_id = first.source.id().clone();

        let mut second = create_test_ingest_decision();
        second.source = Link::new(source_id.clone());
        second.base.created_at = first.base.created_at + Duration::seconds(10);
        second.base.updated_at = second.base.created_at;

        let first_created = repo.create(first).await.unwrap();
        let second_created = repo.create(second).await.unwrap();

        let latest = repo
            .find_latest_by_source("test_space", &source_id)
            .await
            .unwrap()
            .expect("latest");

        assert_eq!(latest.base.id, second_created.base.id);
        assert_ne!(latest.base.id, first_created.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_ingest_decision_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_ingest_decision();
        item.budget_snapshot = Some(BudgetSnapshot {
            remaining_euros_retrieval: 9.5,
            remaining_euros_graph: 45.0,
            remaining_items_retrieval: Some(990),
            remaining_items_graph: Some(95),
            snapshot_at: Utc::now(),
        });
        item.original_mode = Some(IngestMode::Graph);
        item.downgrade_reason = Some("Budget exceeded".to_string());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert!(created.budget_snapshot.is_some());
        let snapshot = created.budget_snapshot.unwrap();
        assert!((snapshot.remaining_euros_retrieval - 9.5).abs() < f64::EPSILON);
        assert_eq!(created.original_mode, Some(IngestMode::Graph));
        assert_eq!(
            created.downgrade_reason,
            Some("Budget exceeded".to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_ingest_decision() {
        let repo = setup_test_repo().await;
        let item = create_test_ingest_decision();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.decision = IngestMode::Metadata;
        updated.downgrade_reason = Some("Budget constraint".to_string());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.decision, IngestMode::Metadata);
        assert_eq!(
            result.downgrade_reason,
            Some("Budget constraint".to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_ingest_decision() {
        let repo = setup_test_repo().await;
        let item = create_test_ingest_decision();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }
}
