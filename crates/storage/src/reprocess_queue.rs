//! A committed Pending reprocess job is durable queue intent. Recovery does not
//! infer authority from a request, fabricate a claim, or retry terminal work.
use super::{
    ReprocessJob, ReprocessJobDb, ReprocessJobRepository, ReprocessStatus, REPROCESS_JOB_PROJECTION,
};
use crate::{StorageError, StorageResult, WorkItemRepository};
use elephant_types::records::work::{WorkItem, WorkItemArchive, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use std::fmt::Write as _;

const KIND: &str = "execute_reprocess_job";

fn work_for(job: &ReprocessJob) -> StorageResult<WorkItem> {
    if job.base.id.table() != TableName::ReprocessJob
        || job.base.space_id.is_empty()
        || job.base.deleted_at.is_some()
        || !matches!(
            job.status,
            ReprocessStatus::Pending | ReprocessStatus::Running
        )
    {
        return Err(StorageError::Conflict(
            "reprocess_handoff_requires_active_job".into(),
        ));
    }
    intended_work(job)
}

pub fn intended_work(job: &ReprocessJob) -> StorageResult<WorkItem> {
    let identity = serde_json::to_vec(&(KIND, &job.base.space_id, &job.base.id))
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    let mut base = job.base.clone();
    base.id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity),
    );
    Ok(WorkItem {
        base,
        kind: KIND.into(),
        payload: serde_json::json!({"job_id":job.base.id.to_string()}),
        status: WorkItemStatus::Runnable,
        priority: job.priority,
        idempotency_key: format!("{KIND}:{}", job.base.id),
        attempts: 0,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: None,
    })
}

fn same_work(old: &WorkItem, wanted: &WorkItem) -> bool {
    let mut payload = old.payload.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY);
    }
    old.base.id == wanted.base.id
        && old.base.space_id == wanted.base.space_id
        && old.base.deleted_at.is_none()
        && old.base.created_by == wanted.base.created_by
        && old.base.security == wanted.base.security
        && old.base.subjects == wanted.base.subjects
        && old.base.subjects_state == wanted.base.subjects_state
        && old.base.provenance == wanted.base.provenance
        && old.kind == wanted.kind
        && old.idempotency_key == wanted.idempotency_key
        && payload == wanted.payload
}

/// Verify the complete structural envelope of an already read job/work pair.
///
/// The caller must read both actual durable records and retain its own attempt
/// authority. This is neither an authorization grant nor a job-status check.
/// Historical retained handoffs may have a random work ID; their exact payload,
/// kind, idempotency identity and authorization/provenance must still agree.
///
/// # Errors
/// Rejects malformed identities, deleted records and altered handoff evidence.
pub fn verify_reprocess_handoff(job: &ReprocessJob, work: &WorkItem) -> StorageResult<()> {
    if job.base.id.table() != TableName::ReprocessJob
        || work.base.id.table() != TableName::WorkItem
        || job.base.space_id.is_empty()
        || job.base.deleted_at.is_some()
    {
        return Err(StorageError::DataIntegrityError(
            "Invalid reprocess handoff owner".into(),
        ));
    }
    let mut expected = intended_work(job)?;
    expected.base.id.clone_from(&work.base.id);
    if !same_work(work, &expected) {
        return Err(StorageError::DataIntegrityError(
            "Reprocess handoff differs from durable job authority".into(),
        ));
    }
    Ok(())
}

impl ReprocessJobRepository {
    /// Create a new Pending job and its initial queue handoff atomically.
    /// The caller must authorize the complete job envelope before this call.
    ///
    /// # Errors
    /// Rejects non-Pending jobs, existing identities and storage failures.
    pub async fn create_with_work(&self, job: ReprocessJob) -> StorageResult<ReprocessJob> {
        if job.status != ReprocessStatus::Pending {
            return Err(StorageError::Conflict(
                "reprocess_create_requires_pending".into(),
            ));
        }
        self.persist_work(&job, true, None).await?;
        Ok(job)
    }

    /// Create the exact job authorized after its configured-space preparation.
    ///
    /// # Errors
    /// Refuses a changed original space configuration, invalid job state,
    /// duplicate identities or any atomic job/origin/queue persistence failure.
    pub async fn create_prepared_with_work(
        &self,
        prepared: super::PreparedReprocessJobCreation,
    ) -> StorageResult<ReprocessJob> {
        let super::PreparedReprocessJobCreation { job, configuration } = prepared;
        if job.status != ReprocessStatus::Pending {
            return Err(StorageError::Conflict(
                "reprocess_create_requires_pending".into(),
            ));
        }
        self.persist_work(&job, true, Some(&configuration)).await?;
        Ok(job)
    }

    /// Ensure the initial handoff for the exact observed durable job.
    ///
    /// A canonical
    /// mutation may commit the Pending job first; all-space recovery closes any
    /// subsequent crash gap. Existing hot/archive work keeps its original state,
    /// attempts and lease, including Failed/Cancelled/Done outcomes. Validation
    /// of an existing handoff is observational: only repairing missing work may
    /// write recovery fences. Dispatch still requires its own attempt authority.
    ///
    /// # Errors
    /// Rejects changed job snapshots, changed/deleted work envelopes and storage errors.
    pub async fn ensure_work(&self, observed: &ReprocessJob) -> StorageResult<RecordId> {
        self.persist_work(observed, false, None).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep producer, configuration, retained archive and exact handoff guards adjacent to the atomic job/work transaction on both backends."
    )]
    async fn persist_work(
        &self,
        job: &ReprocessJob,
        create: bool,
        configuration: Option<&elephant_types::records::SpaceConfig>,
    ) -> StorageResult<RecordId> {
        let mut work = work_for(job)?;
        if !create {
            // Retained callers historically allocated a random ID with the same
            // idempotency key. Preserve that actual handoff instead of creating
            // a second concurrent executor. The transaction rechecks its body.
            if let Some(existing) = WorkItemRepository::new(self.client.clone())
                .find_by_idempotency_key(&job.base.space_id, &work.idempotency_key)
                .await?
            {
                work.base.id = existing.base.id;
            }
        }
        let archive_id = crate::work_retention::archive_id(&work.base.id);
        if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    if let Some(configuration) = configuration {
                        super::reprocess_creation::check_configuration(tx, configuration)?;
                    }
                    if create {
                        tx.create(job.clone())?;
                        crate::work_producer_repository::initialize_reprocess_tx(tx, &work, job)?;
                    } else if tx
                        .get::<ReprocessJob>(&job.base.space_id, &job.base.id, false)?
                        .as_ref()
                        != Some(job)
                    {
                        return Err(StorageError::Conflict(
                            "reprocess_handoff_job_changed".into(),
                        ));
                    }
                    crate::work_producer_repository::require_reprocess_tx(tx, &work, job)?;
                    let old = match tx.get_by_id::<WorkItem>(&work.base.id, true)? {
                        Some(old) => Some(old),
                        None => match tx.get_by_id::<WorkItemArchive>(&archive_id, true)? {
                            Some(archive)
                                if archive.base.deleted_at.is_none()
                                    && archive.base.space_id == job.base.space_id
                                    && archive.original_id == work.base.id =>
                            {
                                Some(archive.record)
                            }
                            Some(_) => {
                                return Err(StorageError::Conflict(
                                    "reprocess_handoff_work_changed".into(),
                                ))
                            }
                            None => None,
                        },
                    };
                    match old {
                        None => {
                            tx.create(work.clone())?;
                        }
                        Some(old) if !create && same_work(&old, &work) => {}
                        Some(_) => {
                            return Err(StorageError::Conflict(
                                "reprocess_handoff_work_changed".into(),
                            ))
                        }
                    }
                    Ok(())
                })
                .await?;
        } else {
            let mut sql = "BEGIN TRANSACTION;\n".to_string();
            let mut recovery_fences = String::new();
            let mut bindings = crate::bindings! {
                "handoff_job_id" => crate::surreal::thing_from_record_id(&job.base.id),
                "handoff_work_id" => crate::surreal::thing_from_record_id(&work.base.id),
                "handoff_archive_id" => crate::surreal::thing_from_record_id(&archive_id),
                "handoff_job" => crate::surreal::stored_native_value(Self::reprocess_job_to_value(job)?),
                "handoff_work" => crate::surreal::stored_native_value(WorkItemRepository::work_item_to_value(&work)?),
                "handoff_create" => create,
            };
            if let Some(configuration) = configuration {
                super::reprocess_creation::append_configuration_guard(
                    &mut sql,
                    &mut bindings,
                    configuration,
                )?;
            }
            if create {
                crate::work_producer_repository::append_reprocess_sql(
                    &mut sql,
                    &mut bindings,
                    "new_job",
                    &work,
                    job,
                )?;
            } else {
                recovery_fences =
                    crate::work_producer_repository::prepare_reprocess_recovery_guard(
                        &self.client,
                        &mut sql,
                        &mut bindings,
                        "original_job",
                        &work,
                        job,
                    )
                    .await?;
            }
            if create {
                sql.push_str("CREATE $handoff_job_id CONTENT $handoff_job;\n");
            } else {
                sql.push_str("LET $handoff_current = SELECT * FROM ONLY $handoff_job_id; IF $handoff_current = NONE OR $handoff_current.id != $handoff_job_id { THROW 'reprocess_handoff_job_changed'; };\n");
                // Emit comparisons directly. Native anonymous closures cannot capture bindings.
                for field in [
                    "space_id",
                    "created_at",
                    "updated_at",
                    "created_by",
                    "deleted_at",
                    "security",
                    "provenance",
                    "subjects",
                    "subjects_state",
                    "name",
                    "selection_criteria",
                    "target_stages",
                    "status",
                    "progress",
                    "priority",
                    "budget_limit_euros",
                    "max_sources_per_day",
                    "started_at",
                    "completed_at",
                    "error",
                    "reason",
                ] {
                    let _ = writeln!(sql, "IF $handoff_current.{field} != $handoff_job.{field} {{ THROW 'reprocess_handoff_job_changed'; }};");
                }
                // The epoch must exist even for an observational replay. A
                // missing-handoff repair additionally fences the job snapshot:
                // migration031 makes every job mutation write this epoch row.
                bindings.insert(
                    "handoff_epoch_id".into(),
                    surrealdb::sql::Thing::from((
                        "canonical_epoch",
                        surrealdb::sql::Id::String(job.base.space_id.clone()),
                    ))
                    .into(),
                );
                sql.push_str("LET $handoff_epoch = SELECT * FROM ONLY $handoff_epoch_id;
                    IF $handoff_epoch = NONE OR $handoff_epoch.space_id != $handoff_job.space_id { THROW 'canonical_schema_missing:reprocess_handoff_epoch'; };\n");
                recovery_fences.push_str(
                    "UPDATE $handoff_epoch_id SET queue_read_nonce=<string>rand::uuid();\n",
                );
            }
            sql.push_str("LET $handoff_hot = SELECT * FROM ONLY $handoff_work_id;
                LET $handoff_cold = SELECT * FROM ONLY $handoff_archive_id;
                IF $handoff_hot = NONE AND $handoff_cold != NONE AND ($handoff_cold.deleted_at != NONE OR $handoff_cold.original_id != $handoff_work_id OR $handoff_cold.space_id != $handoff_job.space_id) { THROW 'reprocess_handoff_work_changed'; };
                LET $handoff_old = IF $handoff_hot != NONE THEN $handoff_hot ELSE $handoff_cold.record END;
                IF $handoff_old = NONE {\n");
            // Repeated recovery of hot/archived work must not write either
            // shared fence. Only the branch that actually enqueues missing work
            // needs serialization against changed job/private origin evidence.
            sql.push_str(&recovery_fences);
            sql.push_str("CREATE $handoff_work_id CONTENT $handoff_work;
                } ELSE {
                  IF $handoff_create OR $handoff_old.id != $handoff_work_id { THROW 'reprocess_handoff_work_changed'; };
                  LET $handoff_payload = object::from_entries(array::filter(object::entries($handoff_old.payload), |$entry| $entry[0] != '_attempt_fence'));
                  IF $handoff_payload != $handoff_work.payload { THROW 'reprocess_handoff_work_changed'; };\n");
            for field in [
                "space_id",
                "created_by",
                "deleted_at",
                "security",
                "provenance",
                "subjects",
                "subjects_state",
                "kind",
                "idempotency_key",
            ] {
                let _ = writeln!(sql, "IF $handoff_old.{field} != $handoff_work.{field} {{ THROW 'reprocess_handoff_work_changed'; }};");
            }
            sql.push_str("}; COMMIT TRANSACTION;");
            self.client.query_bind_checked(&sql, bindings).await?;
        }
        Ok(work.base.id)
    }

    /// Page durable Pending/Running intents across every space, including spaces
    /// without configuration or other work. The cursor is the durable job ID.
    ///
    /// # Errors
    /// Rejects invalid page limits/cursors and malformed persisted records.
    pub async fn recoverable_after(
        &self,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<ReprocessJob>> {
        if limit == 0
            || limit > 256
            || cursor.is_some_and(|id| id.table() != TableName::ReprocessJob)
        {
            return Err(StorageError::DataIntegrityError(
                "Invalid reprocess recovery page".into(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            let mut jobs: Vec<_> = store
                .scan::<ReprocessJob>()
                .await?
                .into_iter()
                .filter(|job| {
                    job.base.deleted_at.is_none()
                        && matches!(
                            job.status,
                            ReprocessStatus::Pending | ReprocessStatus::Running
                        )
                        && cursor.is_none_or(|cursor| job.base.id > *cursor)
                })
                .collect();
            jobs.sort_by(|a, b| a.base.id.cmp(&b.base.id));
            jobs.truncate(limit);
            return Ok(jobs);
        }
        let after = if cursor.is_some() {
            " AND id > $handoff_after"
        } else {
            ""
        };
        let mut response = self.client.query_bind_checked(&format!("SELECT {REPROCESS_JOB_PROJECTION} FROM reprocess_job WHERE deleted_at IS NONE AND status IN ['pending', 'running']{after} ORDER BY id LIMIT {limit}"),
            crate::bindings! { "handoff_after" => cursor.map_or(surrealdb::sql::Value::None, |id| crate::surreal::thing_from_record_id(id).into()) }).await?;
        let rows: Vec<ReprocessJobDb> = response
            .take(0)
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
        rows.into_iter().map(ReprocessJob::try_from).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::{Repository, StorageClient};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::reprocess::{
        ReprocessProgress, ReprocessTargetStage, SelectionCriteria,
    };

    pub(super) fn job() -> ReprocessJob {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "quiet-no-config".into(),
            "authorized-caller".into(),
        );
        base.security.level = 3;
        ReprocessJob {
            base,
            name: "authorized repair".into(),
            selection_criteria: SelectionCriteria::default(),
            target_stages: vec![ReprocessTargetStage::ReResolve],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 7,
            budget_limit_euros: None,
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: None,
        }
    }

    pub(super) async fn prepare_storage(storage: &StorageClient) {
        if !storage.is_embedded() {
            for schema in [
                include_str!("../../migrations/sql/031_source_publication.surql"),
                include_str!("../../migrations/sql/032_work_recovery.surql"),
                include_str!("../../migrations/sql/041_work_producer_origin.surql"),
            ] {
                storage
                    .query_checked(schema)
                    .await
                    .expect("actual original queue and canonical epoch schema before records");
            }
            crate::reference_fence::install_native_reference_guards(storage, &[])
                .await
                .expect("actual private and physical guard schema before records");
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep jobs, items in the original fixture scope through assertions and cleanup."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep this ordered conformance scenario and its failure assertions in one function."
    )]
    async fn contract(storage: StorageClient) {
        prepare_storage(&storage).await;
        let jobs = ReprocessJobRepository::new(storage.clone());
        let items = WorkItemRepository::new(storage.clone());
        let first = jobs.create_with_work(job()).await.expect("atomic creation");
        let id = jobs.ensure_work(&first).await.expect("exact replay");
        let queued = items
            .get(&first.base.space_id, &id)
            .await
            .expect("read")
            .expect("work");
        verify_reprocess_handoff(&first, &queued).expect("actual job handoff");
        let mut altered = queued.clone();
        altered.base.security.level = 0;
        assert!(verify_reprocess_handoff(&first, &altered).is_err());
        altered = queued.clone();
        altered.payload["job_id"] =
            serde_json::json!(RecordId::new_v4(TableName::ReprocessJob).to_string());
        assert!(verify_reprocess_handoff(&first, &altered).is_err());
        let mut paused = first.clone();
        paused.status = ReprocessStatus::Paused;
        paused.budget_limit_euros = Some(1.0);
        verify_reprocess_handoff(&paused, &queued)
            .expect("paused owner remains traceable, not authorized to dispatch");
        assert_eq!(queued.base.security, first.base.security);
        assert_eq!(queued.base.provenance, first.base.provenance);
        assert_eq!(
            queued.payload,
            serde_json::json!({"job_id":first.base.id.to_string()})
        );
        let claimed = items
            .claim_with_lease(
                &first.base.space_id,
                &id,
                "original",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("owned");
        jobs.ensure_work(&first).await.expect("running replay");
        assert_eq!(
            items.get(&first.base.space_id, &id).await.expect("read"),
            Some(claimed.clone())
        );
        let fence =
            crate::work_attempt_repository::fence_from_claim(&claimed).expect("real authority");
        let failed = items
            .update_running_for_worker(
                &first.base.space_id,
                &id,
                &fence.lease_token,
                WorkItemStatus::Failed,
                Some("fixture failure".into()),
            )
            .await
            .expect("fail")
            .expect("original owner");
        jobs.ensure_work(&first).await.expect("terminal replay");
        assert_eq!(
            items.get(&first.base.space_id, &id).await.expect("read"),
            Some(failed)
        );

        crate::work_retention::archive_terminal_work(
            &storage,
            chrono::Utc::now() + chrono::Duration::seconds(1),
            100,
        )
        .await
        .expect("archive terminal handoff");
        assert_eq!(jobs.ensure_work(&first).await.expect("archive replay"), id);

        let retained = jobs.create(job()).await.expect("retained intent");
        let mut old_work = work_for(&retained).expect("retained payload");
        old_work.base.id = RecordId::new_v4(TableName::WorkItem);
        let old_id = old_work.base.id.clone();
        items
            .create(old_work)
            .await
            .expect("retained random identity");
        assert!(
            jobs.ensure_work(&retained).await.is_err(),
            "pre-link handoffs cannot reconstruct their original budget from today's job"
        );
        assert_eq!(
            crate::work_producer_repository::WorkProducerRepository::new(storage.clone())
                .origin_for_work(
                    &items
                        .get(&retained.base.space_id, &old_id)
                        .await
                        .expect("read")
                        .expect("retained work")
                )
                .await
                .expect("private origin"),
            crate::work_producer_repository::WorkProducerOrigin::Unknown
        );
        // It remains visible to recovery, which reports its blocked evidence and
        // still processes independent fresh intents in the same page walk.

        // A canonical approval can commit the intent before the process dies.
        let orphan = jobs
            .create(job())
            .await
            .expect("committed intent without queue");
        let mut cursor = None;
        let mut seen = Vec::new();
        loop {
            let page = jobs
                .recoverable_after(1, cursor.as_ref())
                .await
                .expect("all-space page");
            if page.is_empty() {
                break;
            }
            for observed in &page {
                if observed.base.id == retained.base.id {
                    assert!(jobs.ensure_work(observed).await.is_err());
                } else {
                    jobs.ensure_work(observed).await.expect("repair handoff");
                }
                seen.push(observed.base.id.clone());
            }
            cursor = page.last().map(|job| job.base.id.clone());
        }
        assert!(seen.contains(&orphan.base.id));
        let repaired_id = jobs
            .ensure_work(&orphan)
            .await
            .expect("idempotent recovery");
        assert!(items
            .get(&orphan.base.space_id, &repaired_id)
            .await
            .expect("read")
            .is_some());
        let mut changed = orphan.clone();
        changed.base.security.level = 4;
        jobs.update(&orphan.base.space_id, &orphan.base.id, changed)
            .await
            .expect("change authorization snapshot");
        assert!(jobs.ensure_work(&orphan).await.is_err());

        // Failure to create the work half rolls back the job half.
        let blocked = job();
        let mut conflicting = work_for(&blocked).expect("identity");
        conflicting.kind = "different".into();
        items
            .create(conflicting)
            .await
            .expect("occupy work identity");
        assert!(jobs.create_with_work(blocked.clone()).await.is_err());
        assert!(jobs
            .get(&blocked.base.space_id, &blocked.base.id)
            .await
            .expect("read job")
            .is_none());
        let mut paused = job();
        paused.status = ReprocessStatus::Paused;
        assert!(jobs.create_with_work(paused).await.is_err());
    }

    #[tokio::test]
    async fn embedded_reprocess_handoff_is_atomic_and_quiet_space_recoverable() {
        contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_reprocess_handoff_is_atomic_and_quiet_space_recoverable() {
        contract(StorageClient::new_memory().await.expect("memory")).await;
    }
}

#[cfg(test)]
#[path = "reprocess_queue_observation_tests.rs"]
mod observation_tests;
