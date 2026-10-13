//! Reprocess job repository implementation for `SurrealDB`.
//!
//! Per spec §16, reprocess jobs manage backfill operations.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use surrealdb::sql::Value;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

#[path = "reprocess_creation.rs"]
mod reprocess_creation;
#[path = "reprocess_queue.rs"]
mod reprocess_queue;
pub use reprocess_creation::PreparedReprocessJobCreation;
pub(crate) use reprocess_queue::intended_work;
pub use reprocess_queue::verify_reprocess_handoff;

const REPROCESS_JOB_PROJECTION: &str = r"
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
    name,
    selection_criteria,
    target_stages,
    status,
    progress,
    priority,
    budget_limit_euros,
    max_sources_per_day,
    started_at,
    completed_at,
    error,
    reason
";

/// Database-only proxy for ReprocessJob.
#[derive(Deserialize)]
struct ReprocessJobDb {
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
    name: Option<String>,
    selection_criteria: Option<SelectionCriteria>,
    #[serde(default)]
    target_stages: Option<Vec<ReprocessTargetStage>>,
    status: Option<ReprocessStatus>,
    progress: Option<ReprocessProgress>,
    priority: Option<i32>,
    budget_limit_euros: Option<f64>,
    max_sources_per_day: Option<u32>,
    started_at: Option<chrono::DateTime<Utc>>,
    completed_at: Option<chrono::DateTime<Utc>>,
    error: Option<String>,
    reason: Option<String>,
}

impl TryFrom<ReprocessJobDb> for ReprocessJob {
    type Error = StorageError;

    fn try_from(db: ReprocessJobDb) -> Result<Self, Self::Error> {
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
            name: db
                .name
                .ok_or_else(|| StorageError::QueryError("Missing name".to_string()))?,
            selection_criteria: required_field(db.selection_criteria, "selection_criteria")?,
            target_stages: required_field(db.target_stages, "target_stages")?,
            status: required_field(db.status, "status")?,
            progress: required_field(db.progress, "progress")?,
            priority: required_field(db.priority, "priority")?,
            budget_limit_euros: db.budget_limit_euros,
            max_sources_per_day: db.max_sources_per_day,
            started_at: db.started_at,
            completed_at: db.completed_at,
            error: db.error,
            reason: db.reason,
        })
    }
}

/// Reprocess job repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct ReprocessJobRepository {
    client: StorageClient,
}

impl ReprocessJobRepository {
    /// Creates a new reprocess job repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a ReprocessJob to a SurrealDB value for insertion.
    pub(crate) fn reprocess_job_to_value(
        item: &ReprocessJob,
    ) -> StorageResult<surrealdb::sql::Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert("name".to_string(), item.name.clone().into());

        let selection_criteria_json =
            serde_json::to_value(&item.selection_criteria).map_err(|e| {
                StorageError::SerializationError(format!(
                    "Failed to serialize reprocess_job.selection_criteria: {e}"
                ))
            })?;
        obj.insert(
            "selection_criteria".to_string(),
            crate::surreal::json_to_surreal(selection_criteria_json)?,
        );

        let target_stages_json = serde_json::to_value(&item.target_stages).map_err(|e| {
            StorageError::SerializationError(format!(
                "Failed to serialize reprocess_job.target_stages: {e}"
            ))
        })?;
        obj.insert(
            "target_stages".to_string(),
            crate::surreal::json_to_surreal(target_stages_json)?,
        );

        obj.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );

        let progress_json = serde_json::to_value(&item.progress).map_err(|e| {
            StorageError::SerializationError(format!(
                "Failed to serialize reprocess_job.progress: {e}"
            ))
        })?;
        obj.insert(
            "progress".to_string(),
            crate::surreal::json_to_surreal(progress_json)?,
        );

        obj.insert("priority".to_string(), item.priority.into());
        obj.insert(
            "budget_limit_euros".to_string(),
            item.budget_limit_euros.map_or(Value::None, Value::from),
        );
        obj.insert(
            "max_sources_per_day".to_string(),
            item.max_sources_per_day.map_or(Value::None, Value::from),
        );
        obj.insert(
            "started_at".to_string(),
            item.started_at
                .map(crate::surreal::datetime_from_chrono)
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "completed_at".to_string(),
            item.completed_at
                .map(crate::surreal::datetime_from_chrono)
                .map_or(Value::None, Value::from),
        );
        obj.insert(
            "error".to_string(),
            item.error
                .as_ref()
                .map_or(Value::None, |s| s.clone().into()),
        );
        obj.insert(
            "reason".to_string(),
            item.reason
                .as_ref()
                .map_or(Value::None, |s| s.clone().into()),
        );

        Ok(Value::Object(obj))
    }

    /// Updates a reprocess job only if it matches one of the expected statuses.
    ///
    /// Returns `Ok(None)` when the status does not match, indicating another
    /// worker has already transitioned the job.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn update_if_status_in(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: ReprocessJob,
        expected_statuses: &[ReprocessStatus],
    ) -> StorageResult<Option<ReprocessJob>> {
        record.base.updated_at = Utc::now();

        if let Some(store) = self.client.embedded_store() {
            let expected_statuses = expected_statuses.to_vec();
            return store
                .mutate(|mutation| {
                    let Some(current) = mutation.get::<ReprocessJob>(space_id, id, false)? else {
                        return Ok(None);
                    };
                    if !expected_statuses.contains(&current.status) {
                        return Ok(None);
                    }
                    record.base.id = id.clone();
                    record.base.space_id = space_id.to_string();
                    mutation.replace(record.clone())?;
                    Ok(Some(record.clone()))
                })
                .await;
        }

        record.base.space_id = space_id.to_string();
        record.base.id.clone_from(id);

        let statuses: Vec<String> = expected_statuses.iter().map(ToString::to_string).collect();
        let mut response = self
            .client
            .query_bind(
                "UPDATE $id MERGE $record WHERE space_id = $space_id AND status IN $statuses AND deleted_at IS NONE RETURN AFTER",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "statuses" => statuses,
                    "record" => Self::reprocess_job_to_value(&record)?,
                },
            )
            .await?;

        let items: Vec<ReprocessJobDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated reprocess_job: {e}"))
        })?;
        if items.is_empty() {
            return Ok(None);
        }

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Empty result after successful update".to_string())
        })?;

        Ok(Some(ReprocessJob::try_from(item_db)?))
    }
}

#[async_trait]
impl Repository<ReprocessJob> for ReprocessJobRepository {
    async fn create(&self, record: ReprocessJob) -> StorageResult<ReprocessJob> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    tx.create(record.clone())?;
                    crate::work_producer_repository::initialize_reprocess_intent_tx(tx, &record)?;
                    Ok(record)
                })
                .await;
        }
        let mut sql = "BEGIN TRANSACTION; CREATE $id CONTENT $record;\n".to_string();
        let mut bindings = crate::bindings! {
            "id" => crate::surreal::thing_from_record_id(&record.base.id),
            "record" => Self::reprocess_job_to_value(&record)?,
        };
        crate::work_producer_repository::append_reprocess_intent_sql(
            &mut sql,
            &mut bindings,
            "new_job",
            &record,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;

        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE id = $id"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id) },
            )
            .await?;

        let items: Vec<ReprocessJobDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created reprocess_job: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but reprocess_job not found".to_string())
        })?;

        ReprocessJob::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<ReprocessJob>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<ReprocessJob>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<ReprocessJobDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse reprocess_job: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(ReprocessJob::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: ReprocessJob,
    ) -> StorageResult<ReprocessJob> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {REPROCESS_JOB_PROJECTION}"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::reprocess_job_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<ReprocessJobDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated reprocess_job: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("ReprocessJob not found in space: {id}"))
        })?;

        ReprocessJob::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<ReprocessJob>(space_id, id).await;
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
    ) -> StorageResult<Vec<ReprocessJob>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<ReprocessJob>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<ReprocessJobDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse reprocess_jobs: {e}"))
        })?;

        items.into_iter().map(ReprocessJob::try_from).collect()
    }
}

impl ReprocessJobRepository {
    /// Complete physical before-image for canonical progress/intent transactions.
    pub(crate) async fn get_including_deleted(
        &self,
        space: &str,
        id: &RecordId,
    ) -> StorageResult<Option<ReprocessJob>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<ReprocessJob>(space, id, true).await;
        }
        let mut response = self.client.query_bind_checked(&format!("SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE id=$id AND space_id=$space"),crate::bindings!{"id"=>crate::surreal::thing_from_record_id(id),"space"=>space}).await?;
        let rows: Vec<ReprocessJobDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(error.to_string()))?;
        rows.into_iter()
            .next()
            .map(ReprocessJob::try_from)
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
    use crate::error::StorageError;
    use crate::StorageConfig;
    use chrono::NaiveDate;
    use elephant_types::records::reprocess::default_reprocess_target_stages;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(81400);

    const REPROCESS_JOB_SCHEMA: &str = r"
        DEFINE TABLE reprocess_job SCHEMALESS;
    ";

    async fn setup_test_repo() -> ReprocessJobRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("reprocess_job_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(REPROCESS_JOB_SCHEMA).await.unwrap();

        ReprocessJobRepository::new(client)
    }

    fn create_test_reprocess_job() -> ReprocessJob {
        let id = RecordId::new_v4(TableName::ReprocessJob);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        ReprocessJob {
            base,
            name: "Backfill extractor v2".to_string(),
            selection_criteria: SelectionCriteria {
                source_types: vec!["pdf".to_string(), "docx".to_string()],
                date_from: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                date_to: Some(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()),
                extractor_version_before: Some("2.0.0".to_string()),
                predicate_ids: vec![],
                space_ids: vec![],
                source_status: None,
                custom_filter: None,
            },
            target_stages: default_reprocess_target_stages(),
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 5,
            budget_limit_euros: Some(100.0),
            max_sources_per_day: Some(1000),
            started_at: None,
            completed_at: None,
            error: None,
            reason: Some("Extractor v2 released with better accuracy".to_string()),
        }
    }

    fn create_test_reprocess_job_db() -> ReprocessJobDb {
        let id = RecordId::new_v4(TableName::ReprocessJob);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

        ReprocessJobDb {
            id: crate::surreal::thing_from_record_id(&base.id),
            space_id: Some(base.space_id),
            created_at: Some(base.created_at),
            updated_at: Some(base.updated_at),
            created_by: Some(base.created_by),
            deleted_at: base.deleted_at,
            provenance: Some(base.provenance),
            security: Some(base.security),
            subjects: Some(base.subjects),
            subjects_state: Some(base.subjects_state),
            name: Some("Backfill extractor v2".to_string()),
            selection_criteria: Some(SelectionCriteria::default()),
            target_stages: Some(default_reprocess_target_stages()),
            status: Some(ReprocessStatus::Pending),
            progress: Some(ReprocessProgress::default()),
            priority: Some(5),
            budget_limit_euros: Some(100.0),
            max_sources_per_day: Some(1000),
            started_at: None,
            completed_at: None,
            error: None,
            reason: Some("Extractor v2 released with better accuracy".to_string()),
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_reprocess_job() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.name, "Backfill extractor v2");
        assert_eq!(created.status, ReprocessStatus::Pending);
        assert_eq!(created.priority, 5);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_reprocess_job() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();
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
    async fn test_reprocess_job_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_reprocess_job();
        item.progress = ReprocessProgress {
            total_sources: 1000,
            processed_sources: 500,
            succeeded_sources: 490,
            failed_sources: 10,
            skipped_sources: 0,
            progress_percent: 50.0,
            eta_seconds: Some(3600),
            cost_spent_euros: 25.5,
        };

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.progress.total_sources, 1000);
        assert_eq!(created.progress.processed_sources, 500);
        assert!((created.progress.progress_percent - 50.0).abs() < f32::EPSILON);
        assert!((created.progress.cost_spent_euros - 25.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_reprocess_job() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ReprocessStatus::Running;
        updated.started_at = Some(Utc::now());
        updated.progress.processed_sources = 100;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, ReprocessStatus::Running);
        assert!(result.started_at.is_some());
        assert_eq!(result.progress.processed_sources, 100);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_in_matches() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ReprocessStatus::Running;
        updated.started_at = Some(Utc::now());

        let result = repo
            .update_if_status_in(
                "test_space",
                &id,
                updated,
                &[ReprocessStatus::Pending, ReprocessStatus::Paused],
            )
            .await
            .unwrap();

        let updated_job = result.expect("expected status transition to succeed");
        assert_eq!(updated_job.status, ReprocessStatus::Running);
        assert!(updated_job.started_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_in_mismatch_returns_none() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ReprocessStatus::Running;
        updated.started_at = Some(Utc::now());

        let result = repo
            .update_if_status_in(
                "test_space",
                &id,
                updated,
                &[ReprocessStatus::Pending, ReprocessStatus::Paused],
            )
            .await
            .unwrap();
        assert!(result.is_some(), "first claim should succeed");

        let mut second_update = created.clone();
        second_update.status = ReprocessStatus::Running;

        let result = repo
            .update_if_status_in(
                "test_space",
                &id,
                second_update,
                &[ReprocessStatus::Pending, ReprocessStatus::Paused],
            )
            .await
            .unwrap();
        assert!(result.is_none(), "second claim should be rejected");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_in_uses_target_id_not_payload_id() {
        let repo = setup_test_repo().await;
        let target = repo.create(create_test_reprocess_job()).await.unwrap();
        let other = repo.create(create_test_reprocess_job()).await.unwrap();

        let mut payload = other.clone();
        payload.status = ReprocessStatus::Running;
        payload.started_at = Some(Utc::now());

        let updated = repo
            .update_if_status_in(
                "test_space",
                &target.base.id,
                payload,
                &[ReprocessStatus::Pending, ReprocessStatus::Paused],
            )
            .await
            .unwrap()
            .expect("status match");

        assert_eq!(updated.base.id, target.base.id);

        let fetched_target = repo
            .get("test_space", &target.base.id)
            .await
            .unwrap()
            .expect("target job");
        assert_eq!(fetched_target.status, ReprocessStatus::Running);

        let fetched_other = repo
            .get("test_space", &other.base.id)
            .await
            .unwrap()
            .expect("other job");
        assert_eq!(fetched_other.status, ReprocessStatus::Pending);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_reprocess_job() {
        let repo = setup_test_repo().await;
        let item = create_test_reprocess_job();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_reprocess_job_requires_selection_status_progress() {
        let mut db = create_test_reprocess_job_db();
        db.selection_criteria = None;
        let err = ReprocessJob::try_from(db).unwrap_err();
        assert!(
            matches!(err, StorageError::QueryError(ref msg) if msg.contains("selection_criteria")),
            "expected missing selection_criteria error, got {err:?}"
        );

        let mut db = create_test_reprocess_job_db();
        db.target_stages = None;
        let err = ReprocessJob::try_from(db).unwrap_err();
        assert!(
            matches!(err, StorageError::QueryError(ref msg) if msg.contains("target_stages")),
            "expected missing target_stages error, got {err:?}"
        );

        let mut db = create_test_reprocess_job_db();
        db.status = None;
        let err = ReprocessJob::try_from(db).unwrap_err();
        assert!(
            matches!(err, StorageError::QueryError(ref msg) if msg.contains("status")),
            "expected missing status error, got {err:?}"
        );

        let mut db = create_test_reprocess_job_db();
        db.progress = None;
        let err = ReprocessJob::try_from(db).unwrap_err();
        assert!(
            matches!(err, StorageError::QueryError(ref msg) if msg.contains("progress")),
            "expected missing progress error, got {err:?}"
        );
    }
}
