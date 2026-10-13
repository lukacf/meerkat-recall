//! Work run repository implementation for `SurrealDB`.
//!
//! Per spec §5, work runs represent execution attempts of work items.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::work::{WorkItem, WorkRun, WorkRunMetrics};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

#[path = "trusted_proposal_context.rs"]
mod trusted_proposal_context;
pub use trusted_proposal_context::create_trusted_proposal_context;

pub(crate) const WORK_RUN_PROJECTION: &str = r"
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
    work_item,
    attempt,
    started_at,
    completed_at,
    success,
    error,
    metrics,
    worker_id,
    worker_version,
    model_version,
    prompt_id,
    config_hash
";

/// Database-only proxy for WorkRun.
#[derive(Deserialize)]
pub(crate) struct WorkRunDb {
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
    work_item: Option<Link<WorkItem>>,
    attempt: Option<u32>,
    started_at: Option<chrono::DateTime<Utc>>,
    completed_at: Option<chrono::DateTime<Utc>>,
    success: Option<bool>,
    error: Option<String>,
    metrics: Option<WorkRunMetrics>,
    worker_id: Option<String>,
    worker_version: Option<String>,
    model_version: Option<String>,
    prompt_id: Option<String>,
    config_hash: Option<String>,
}

impl TryFrom<WorkRunDb> for WorkRun {
    type Error = StorageError;

    fn try_from(db: WorkRunDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
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
                    .ok_or_else(|| StorageError::QueryError("Missing subjects".to_string()))?,
                subjects_state: db.subjects_state.ok_or_else(|| {
                    StorageError::QueryError("Missing subjects_state".to_string())
                })?,
            },
            work_item: db
                .work_item
                .ok_or_else(|| StorageError::QueryError("Missing work_item".to_string()))?,
            attempt: db
                .attempt
                .ok_or_else(|| StorageError::QueryError("Missing attempt".to_string()))?,
            started_at: db
                .started_at
                .ok_or_else(|| StorageError::QueryError("Missing started_at".to_string()))?,
            completed_at: db.completed_at,
            success: db
                .success
                .ok_or_else(|| StorageError::QueryError("Missing success".to_string()))?,
            error: db.error,
            metrics: db
                .metrics
                .ok_or_else(|| StorageError::QueryError("Missing metrics".to_string()))?,
            worker_id: db
                .worker_id
                .ok_or_else(|| StorageError::QueryError("Missing worker_id".to_string()))?,
            worker_version: db
                .worker_version
                .ok_or_else(|| StorageError::QueryError("Missing worker_version".to_string()))?,
            model_version: db.model_version,
            prompt_id: db.prompt_id,
            config_hash: db.config_hash,
        })
    }
}

/// Work run repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct WorkRunRepository {
    client: StorageClient,
}

impl WorkRunRepository {
    /// Creates a new work run repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Converts a WorkRun to a SurrealDB value for insertion.
    pub(crate) fn work_run_to_value(item: &WorkRun) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::{Object, Value};
        let mut fields = crate::surreal::base_fields_to_object(&item.base)?.0;
        fields.insert(
            "work_item".to_string(),
            crate::surreal::thing_from_record_id(item.work_item.id()).into(),
        );
        fields.insert("attempt".to_string(), item.attempt.into());
        fields.insert(
            "started_at".to_string(),
            crate::surreal::datetime_from_chrono(item.started_at).into(),
        );
        fields.insert(
            "completed_at".to_string(),
            item.completed_at.map_or(Value::None, |at| {
                crate::surreal::datetime_from_chrono(at).into()
            }),
        );
        fields.insert("success".to_string(), item.success.into());
        fields.insert(
            "error".to_string(),
            item.error.clone().map_or(Value::None, Into::into),
        );
        fields.insert(
            "metrics".to_string(),
            crate::surreal::json_to_surreal(
                serde_json::to_value(&item.metrics)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?,
            )?,
        );
        fields.insert("worker_id".to_string(), item.worker_id.clone().into());
        fields.insert(
            "worker_version".to_string(),
            item.worker_version.clone().into(),
        );
        fields.insert(
            "model_version".to_string(),
            item.model_version.clone().map_or(Value::None, Into::into),
        );
        fields.insert(
            "prompt_id".to_string(),
            item.prompt_id.clone().map_or(Value::None, Into::into),
        );
        fields.insert(
            "config_hash".to_string(),
            item.config_hash.clone().map_or(Value::None, Into::into),
        );
        Ok(Value::Object(Object::from(fields)))
    }
}

#[async_trait]
impl Repository<WorkRun> for WorkRunRepository {
    async fn create(&self, record: WorkRun) -> StorageResult<WorkRun> {
        crate::work_retention::create_run(&self.client, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkRun>> {
        crate::work_retention::get_run(&self.client, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkRun,
    ) -> StorageResult<WorkRun> {
        crate::work_retention::update_run(&self.client, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(mut run) = self.get(space_id, id).await? {
            run.base.deleted_at = Some(Utc::now());
            self.update(space_id, id, run).await?;
        }
        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<WorkRun>> {
        crate::work_retention::list_runs(&self.client, space_id, limit, cursor).await
    }
}

/// Additional work-run-specific operations.
impl WorkRunRepository {
    /// Close an interrupted run only when its original lease is no longer live.
    ///
    /// The recorded attempt identifies the historical receipt, never authority
    /// to execute or publish. Outcome and lease inspection share a transaction.
    ///
    /// # Errors
    /// Returns malformed-receipt or storage errors. Legacy runs with no captured
    /// fence, already finished runs, and live attempts are left unchanged.
    #[allow(
        clippy::too_many_lines,
        reason = "Keep original lease/generation checks, renewal serialization and interrupted-run completion in one transaction on both backends."
    )]
    pub async fn finish_abandoned(&self, observed: &WorkRun) -> StorageResult<bool> {
        use elephant_types::publication::AttemptFence;
        use elephant_types::records::work::WorkItemStatus;
        let Some(value) = observed.metrics.custom.get("attempt_fence") else {
            return Ok(false);
        };
        let fence: AttemptFence = serde_json::from_value(value.clone())
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
        if fence.space_id != observed.base.space_id
            || &fence.work_item_id != observed.work_item.id()
        {
            return Err(StorageError::DataIntegrityError(
                "work run attempt identity mismatch".into(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let Some(mut run) =
                        tx.get::<WorkRun>(&observed.base.space_id, &observed.base.id, false)?
                    else {
                        return Ok(false);
                    };
                    if run.completed_at.is_some()
                        || run.work_item != observed.work_item
                        || run.metrics.custom.get("attempt_fence") != Some(value)
                    {
                        return Ok(false);
                    }
                    let now = Utc::now();
                    let item = tx.get::<WorkItem>(&fence.space_id, &fence.work_item_id, false)?;
                    let authority =
                        crate::work_attempt_repository::read_attempt(tx, &fence.work_item_id)?;
                    let live = item.as_ref().is_some_and(|item| {
                        item.status == WorkItemStatus::Running
                            && item.leased_by.as_deref() == Some(fence.lease_token.as_str())
                            && item.lease_expires_at.is_some_and(|expiry| expiry > now)
                    }) && authority.as_ref().is_some_and(|row| {
                        row.space_id == fence.space_id
                            && row.work_item_id == fence.work_item_id
                            && row.generation == fence.generation
                            && row.lease_token == fence.lease_token
                            && row.status == "running"
                            && row.lease_expires_at.is_some_and(|expiry| expiry > now)
                    });
                    if live {
                        return Ok(false);
                    }
                    // Serialize expiry observation with an overlapping renewal
                    // of this exact generation; never touch a successor attempt.
                    if let Some(mut row) = authority.filter(|row| {
                        row.space_id == fence.space_id
                            && row.work_item_id == fence.work_item_id
                            && row.generation == fence.generation
                            && row.lease_token == fence.lease_token
                    }) {
                        row.commit_nonce = uuid::Uuid::new_v4().to_string();
                        tx.upsert_extension_row(
                            "work_attempt",
                            serde_json::to_value(row).map_err(|error| {
                                StorageError::SerializationError(error.to_string())
                            })?,
                        )?;
                    }
                    run.completed_at = Some(now);
                    run.success = false;
                    run.error =
                        Some("attempt_interrupted: original lease is no longer live".into());
                    run.base.updated_at = now;
                    run.metrics.custom.insert("interrupted".into(), true.into());
                    tx.replace(run)?;
                    Ok(true)
                })
                .await;
        }
        let mut response = self.client.query_bind_checked(
            "BEGIN TRANSACTION;
             LET $run = SELECT * FROM ONLY $run_id;
             LET $item = SELECT * FROM ONLY $item_id;
             LET $authority = SELECT * FROM ONLY $attempt_id;
             LET $live = $item IS NOT NONE AND $item.space_id = $space AND $item.deleted_at IS NONE AND $item.status = 'running' AND $item.leased_by = $work_lease_token AND $item.lease_expires_at > time::now()
                AND $authority IS NOT NONE AND $authority.space_id = $space AND $authority.work_item_id = $item_id AND $authority.generation = $generation AND $authority.lease_token = $work_lease_token AND $authority.status = 'running' AND $authority.lease_expires_at > time::now();
             LET $changed = IF $run IS NOT NONE AND $run.space_id = $space AND $run.deleted_at IS NONE AND $run.completed_at IS NONE AND $run.work_item = $item_id AND $run.metrics.custom.attempt_fence = $fence AND $live = false {
                UPDATE $attempt_id SET commit_nonce = $nonce WHERE space_id = $space AND work_item_id = $item_id AND generation = $generation AND lease_token = $work_lease_token;
                UPDATE $run_id SET completed_at = time::now(), success = false, error = 'attempt_interrupted: original lease is no longer live', updated_at = time::now(), metrics.custom.interrupted = true;
                RETURN true;
             } ELSE { RETURN false; };
             RETURN $changed; COMMIT TRANSACTION;",
            crate::bindings! {
                "run_id" => crate::surreal::thing_from_record_id(&observed.base.id),
                "item_id" => crate::surreal::thing_from_record_id(&fence.work_item_id),
                "attempt_id" => crate::work_attempt_repository::attempt_id(&fence.work_item_id),
                "space" => fence.space_id,
                "work_lease_token" => fence.lease_token,
                "generation" => fence.generation,
                "nonce" => uuid::Uuid::new_v4().to_string(),
                "fence" => crate::surreal::json_to_surreal(value.clone())?,
            },
        ).await?;
        let changed: Option<bool> =
            response
                .take(response.num_statements() - 1)
                .map_err(|error| {
                    StorageError::DataIntegrityError(format!(
                        "invalid interrupted-run result: {error}"
                    ))
                })?;
        changed.ok_or_else(|| {
            StorageError::DataIntegrityError("missing interrupted-run result".into())
        })
    }

    /// Finds all runs for a specific work item.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_work_item(&self, work_item_id: &RecordId) -> StorageResult<Vec<WorkRun>> {
        crate::work_retention::runs_for_item(&self.client, work_item_id).await
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80600);

    const WORK_RUN_SCHEMA: &str = r"
        DEFINE TABLE work_run SCHEMALESS;
    ";

    async fn setup_test_repo() -> WorkRunRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("work_run_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(WORK_RUN_SCHEMA).await.unwrap();

        WorkRunRepository::new(client)
    }

    fn create_test_work_run() -> WorkRun {
        let id = RecordId::new_v4(TableName::WorkRun);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let work_item_id = RecordId::new_v4(TableName::WorkItem);

        WorkRun {
            base,
            work_item: Link::new(work_item_id),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "worker-1".to_string(),
            worker_version: "1.0.0".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        }
    }

    fn create_test_work_run_db() -> WorkRunDb {
        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let record_id = RecordId::new_v4(TableName::WorkRun);
        WorkRunDb {
            id: crate::surreal::thing_from_record_id(&record_id),
            space_id: Some("test_space".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("test_agent".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            work_item: Some(Link::new(work_item_id)),
            attempt: Some(1),
            started_at: Some(Utc::now()),
            completed_at: None,
            success: Some(false),
            error: None,
            metrics: Some(WorkRunMetrics::default()),
            worker_id: Some("worker-1".to_string()),
            worker_version: Some("1.0.0".to_string()),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_work_run() {
        let repo = setup_test_repo().await;
        let item = create_test_work_run();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.worker_id, "worker-1");
        assert_eq!(created.attempt, 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_work_run() {
        let repo = setup_test_repo().await;
        let item = create_test_work_run();
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
    async fn test_work_run_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_run();
        item.metrics.tokens_used = 1000;
        item.metrics.wall_time_ms = 5000;
        item.model_version = Some("gpt-4".to_string());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.metrics.tokens_used, 1000);
        assert_eq!(created.metrics.wall_time_ms, 5000);
        assert_eq!(created.model_version, Some("gpt-4".to_string()));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_work_run() {
        let repo = setup_test_repo().await;
        let item = create_test_work_run();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.success = true;
        updated.completed_at = Some(Utc::now());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert!(result.success);
        assert!(result.completed_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_work_run() {
        let repo = setup_test_repo().await;
        let item = create_test_work_run();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_work_run_missing_base_fields_errors() {
        let mut db = create_test_work_run_db();
        db.space_id = None;

        let err = WorkRun::try_from(db).expect_err("missing space_id should error");
        match err {
            StorageError::QueryError(message) => {
                assert!(message.contains("space_id"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
