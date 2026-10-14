//! Explicit job-root admission versus original inherited execution authority.
//!
//! Selected content lineage never supplies this edge. Only the real admitting
//! queue attempt and its persisted WorkRun can construct an inherited root.
use super::{
    append_insert, append_origin_guard, changed, insert_tx, invalid, read_tx, reprocess,
    work_intent_fingerprint, AttemptFence, BTreeMap, CanonicalRecord, Deserialize,
    EmbeddedMutation, OriginEnvelope, RecordId, Repository, ReprocessJob, Serialize, StorageClient,
    StorageResult, Value, WorkItem, WorkProducerOrigin, WorkProducerRepository, WorkRun,
};
use crate::work_attempt_repository::{append_attempt_fence, check_attempt_fence, fence_from_claim};

/// Immutable execution origin recorded with the initial job intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReprocessExecutionOrigin {
    /// Explicit independently authorized job/root creation or trusted direct API.
    Independent,
    /// Job spawned by this actual original execution. Own job limits are
    /// additional; the parent's restrictions and standing ceilings also apply.
    Inherited {
        /// Original admitting Work, retained through archival.
        parent_work_item_id: RecordId,
        /// Original actual execution receipt, never a replacement attempt.
        original_parent_work_run_id: RecordId,
        /// Original claim which authorized creation of this job intent.
        original_parent_attempt: AttemptFence,
        /// Immutable original parent intent, including standing/publication refs.
        parent_intent_fingerprint: String,
    },
}

/// Original live parent admission. Private fields and no Serde prevent a caller
/// from submitting a fabricated parent descriptor as a capability.
#[derive(Clone)]
pub struct PreparedReprocessParent {
    work: WorkItem,
    run: WorkRun,
    attempt: AttemptFence,
    origin: OriginEnvelope,
}
impl std::fmt::Debug for PreparedReprocessParent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedReprocessParent")
            .field("original_authority", &"redacted")
            .finish_non_exhaustive()
    }
}

impl PreparedReprocessParent {
    /// Capture exactly the canonical request's original attempt, after its
    /// read-only replay fast path.
    ///
    /// No current-worker/actor lookup substitutes it.
    /// # Errors
    /// Refuses obsolete/missing claim, unknown parent origin or ambiguous run.
    pub async fn capture(client: &StorageClient, attempt: &AttemptFence) -> StorageResult<Self> {
        let work = crate::WorkItemRepository::new(client.clone())
            .get(&attempt.space_id, &attempt.work_item_id)
            .await?
            .ok_or_else(changed)?;
        if work.base.deleted_at.is_some()
            || work.status != elephant_types::records::WorkItemStatus::Running
            || work
                .lease_expires_at
                .is_none_or(|expires| expires <= chrono::Utc::now())
            || fence_from_claim(&work)? != *attempt
        {
            return Err(changed());
        }
        let runs = crate::WorkRunRepository::new(client.clone())
            .find_by_work_item(&work.base.id)
            .await?;
        let mut original = runs.into_iter().filter(|run| {
            run.base.space_id == attempt.space_id
                && run.base.deleted_at.is_none()
                && run.completed_at.is_none()
                && run.work_item.id() == &work.base.id
                && run
                    .metrics
                    .custom
                    .get("attempt_fence")
                    .and_then(|value| serde_json::from_value::<AttemptFence>(value.clone()).ok())
                    .as_ref()
                    == Some(attempt)
        });
        let run = original.next().ok_or_else(changed)?;
        if original.next().is_some() {
            return Err(changed());
        }
        let origin = WorkProducerRepository::new(client.clone())
            .read_envelope(&work)
            .await?
            .ok_or_else(changed)?;
        if matches!(origin.origin, WorkProducerOrigin::Unknown) {
            return Err(changed());
        }
        Ok(Self {
            work,
            run,
            attempt: attempt.clone(),
            origin,
        })
    }

    fn descriptor(&self) -> StorageResult<ReprocessExecutionOrigin> {
        Ok(ReprocessExecutionOrigin::Inherited {
            parent_work_item_id: self.work.base.id.clone(),
            original_parent_work_run_id: self.run.base.id.clone(),
            original_parent_attempt: self.attempt.clone(),
            parent_intent_fingerprint: work_intent_fingerprint(&self.work)?,
        })
    }

    fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        check_attempt_fence(tx, &self.attempt, chrono::Utc::now())?;
        if tx
            .get::<WorkItem>(&self.attempt.space_id, &self.work.base.id, false)?
            .as_ref()
            != Some(&self.work)
            || tx
                .get::<WorkRun>(&self.attempt.space_id, &self.run.base.id, false)?
                .as_ref()
                != Some(&self.run)
            || read_tx(tx, &self.work)?.as_ref() != Some(&self.origin)
        {
            return Err(changed());
        }
        Ok(())
    }

    fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
        prefix: &str,
    ) -> StorageResult<()> {
        if bindings
            .get("space")
            .is_some_and(|value| value != &Value::from(self.attempt.space_id.as_str()))
        {
            return Err(changed());
        }
        bindings.insert("space".into(), self.attempt.space_id.clone().into());
        append_attempt_fence(sql, bindings, &self.attempt)?;
        for (suffix, row) in [
            (
                "work",
                CanonicalRecord::WorkItem(Box::new(self.work.clone())),
            ),
            ("run", CanonicalRecord::WorkRun(Box::new(self.run.clone()))),
        ] {
            crate::canonical_commit::append_row_guard(
                sql,
                bindings,
                &format!("{prefix}_parent_{suffix}"),
                &row,
                Some(&row),
            )?;
        }
        append_origin_guard(sql, bindings, &format!("{prefix}_parent"), &self.origin)
    }
}

/// Verify a historical inherited job parent; its original lease may have ended.
///
/// Live writes separately check the consuming attempt and every current ceiling.
/// # Errors
/// Rejects substituted parent intent/run/attempt, foreign or deleted evidence.
pub fn verify_reprocess_execution_parent(
    origin: &ReprocessExecutionOrigin,
    parent: &WorkItem,
    run: &WorkRun,
) -> StorageResult<()> {
    let ReprocessExecutionOrigin::Inherited {
        parent_work_item_id,
        original_parent_work_run_id,
        original_parent_attempt,
        parent_intent_fingerprint,
    } = origin
    else {
        return Err(changed());
    };
    let captured: AttemptFence = serde_json::from_value(
        run.metrics
            .custom
            .get("attempt_fence")
            .cloned()
            .ok_or_else(changed)?,
    )
    .map_err(invalid)?;
    if parent.base.deleted_at.is_some()
        || run.base.deleted_at.is_some()
        || &parent.base.id != parent_work_item_id
        || &run.base.id != original_parent_work_run_id
        || run.base.space_id != parent.base.space_id
        || original_parent_attempt.space_id != parent.base.space_id
        || &original_parent_attempt.work_item_id != parent_work_item_id
        || run.work_item.id() != &parent.base.id
        || captured != *original_parent_attempt
        || work_intent_fingerprint(parent)? != *parent_intent_fingerprint
    {
        return Err(changed());
    }
    Ok(())
}

fn inherited_envelope(
    job: &ReprocessJob,
    parent: Option<&PreparedReprocessParent>,
) -> StorageResult<OriginEnvelope> {
    let work = crate::reprocess_job_repository::intended_work(job)?;
    let mut envelope = reprocess(&work, job)?;
    if let Some(parent) = parent {
        if parent.work.base.space_id != job.base.space_id {
            return Err(changed());
        }
        let WorkProducerOrigin::ReprocessJob {
            execution_origin, ..
        } = &mut envelope.origin
        else {
            return Err(changed());
        };
        *execution_origin = parent.descriptor()?;
        envelope
            .source_ids
            .extend(parent.origin.source_ids.iter().cloned());
        if let Some(value) = parent.work.payload.get("_publication") {
            let fence: elephant_types::publication::PublicationFence =
                serde_json::from_value(value.clone()).map_err(invalid)?;
            envelope.source_ids.push(fence.source_id);
        }
        envelope.source_ids.sort();
        envelope.source_ids.dedup();
    }
    Ok(envelope)
}

pub fn initialize_reprocess_intent_with_parent_tx(
    tx: &mut EmbeddedMutation<'_>,
    job: &ReprocessJob,
    parent: Option<&PreparedReprocessParent>,
) -> StorageResult<()> {
    if let Some(parent) = parent {
        parent.check_tx(tx)?;
    }
    insert_tx(tx, &inherited_envelope(job, parent)?)
}

pub fn append_reprocess_intent_with_parent_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    job: &ReprocessJob,
    parent: Option<&PreparedReprocessParent>,
) -> StorageResult<()> {
    if let Some(parent) = parent {
        parent.append_guard_sql(sql, bindings, prefix)?;
    }
    append_insert(sql, bindings, prefix, &inherited_envelope(job, parent)?)
}

#[cfg(test)]
#[path = "work_producer_parent_tests.rs"]
mod tests;
