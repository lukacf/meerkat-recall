//! Private immutable queue producer evidence. It restricts physical dispatch;
//! it never grants record authorization, a lease, or semantic publication.
use crate::canonical_commit::{source_support_fingerprint, CanonicalRecord};
use crate::{EmbeddedMutation, Repository, StorageClient, StorageError, StorageResult};
use elephant_types::publication::AttemptFence;
use elephant_types::records::reprocess::ReprocessJob;
use elephant_types::records::{ConflictGroup, WorkItem, WorkRun};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use surrealdb::sql::{Thing, Value};

const TABLE: &str = "work_producer_origin";

/// Original private producer restriction; absent evidence is never direct work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkProducerOrigin {
    /// Historical/missing evidence. Physical dispatch must refuse to infer origin.
    Unknown,
    /// Explicitly no queue parent. A source operation still has its own origin.
    Direct,
    /// Original durable reprocess intent, captured independently of later progress.
    ReprocessJob {
        /// Actual job whose original intent produced the handoff.
        job_id: RecordId,
        /// Immutable intent identity, excluding mutable progress/budget policy.
        original_job_intent_fingerprint: String,
        /// A captured finite currency restriction cannot be silently removed.
        original_currency_restricted: bool,
        /// Explicit original job admission. Missing history never means an
        /// independent root; this required field has no deserialization default.
        execution_origin: ReprocessExecutionOrigin,
    },
    /// Actual original parent attempt that atomically created this child.
    Child {
        /// Private original parent work identity.
        parent_work_item_id: RecordId,
        /// Private original execution receipt identity.
        original_parent_work_run_id: RecordId,
        /// Original authority used at enqueue, never refreshed during dispatch.
        original_parent_attempt: AttemptFence,
        /// Parent semantic intent captured at enqueue.
        parent_intent_fingerprint: String,
        /// Exact child semantic intent captured at enqueue.
        child_intent_fingerprint: String,
        /// Whether scheduling waits for the real parent's acknowledged outcome.
        wait_for_parent_completion: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OriginEnvelope {
    version: u32,
    space_id: String,
    work_item_id: RecordId,
    intent_fingerprint: String,
    source_ids: Vec<RecordId>,
    origin: WorkProducerOrigin,
}

#[derive(Deserialize)]
struct OriginDb {
    payload: String,
}

fn invalid(message: impl std::fmt::Display) -> StorageError {
    StorageError::DataIntegrityError(message.to_string())
}
fn changed() -> StorageError {
    StorageError::Conflict("work_producer_origin_changed_or_unknown".into())
}
fn origin_id(work: &RecordId) -> Thing {
    Thing::from((TABLE, surrealdb::sql::Id::String(work.id().to_string())))
}
fn origin_key(work: &RecordId) -> String {
    format!("{TABLE}:{}", work.id())
}

/// Fingerprint immutable queue intent while excluding per-claim/runtime state.
///
/// # Errors
/// Returns serialization errors or malformed queue identity.
pub fn work_intent_fingerprint(work: &WorkItem) -> StorageResult<String> {
    if work.base.id.table() != TableName::WorkItem || work.base.space_id.is_empty() {
        return Err(invalid("Invalid producer work identity"));
    }
    let mut payload = work.payload.clone();
    let object = payload
        .as_object_mut()
        .ok_or_else(|| invalid("Work payload must be an object"))?;
    object.remove(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY);
    source_support_fingerprint(&serde_json::json!({
        "format": "work-producer-intent/v1",
        "id": work.base.id,
        "space_id": work.base.space_id,
        "created_by": work.base.created_by,
        "security": work.base.security,
        "subjects": work.base.subjects,
        "subjects_state": work.base.subjects_state,
        "provenance": work.base.provenance,
        "kind": work.kind,
        "idempotency_key": work.idempotency_key,
        "payload": payload,
    }))
}

/// Stable original job intent; current currency policy is checked separately.
///
/// # Errors
/// Returns serialization errors or an invalid job identity.
pub fn job_intent_fingerprint(job: &ReprocessJob) -> StorageResult<String> {
    if job.base.id.table() != TableName::ReprocessJob || job.base.space_id.is_empty() {
        return Err(invalid("Invalid producer job identity"));
    }
    source_support_fingerprint(&serde_json::json!({
        "format": "reprocess-producer-intent/v1",
        "id": job.base.id,
        "space_id": job.base.space_id,
        "created_by": job.base.created_by,
        "security": job.base.security,
        "subjects": job.base.subjects,
        "subjects_state": job.base.subjects_state,
        "provenance": job.base.provenance,
        "selection_criteria": job.selection_criteria,
        "target_stages": job.target_stages,
    }))
}

impl OriginEnvelope {
    fn new(
        work: &WorkItem,
        origin: WorkProducerOrigin,
        additional_sources: &[RecordId],
    ) -> StorageResult<Self> {
        let mut source_ids = work.base.provenance.sources.clone();
        source_ids.extend_from_slice(additional_sources);
        source_ids.sort();
        source_ids.dedup();
        if source_ids.iter().any(|id| id.table() != TableName::Source) {
            return Err(invalid("Producer source reference has the wrong table"));
        }
        Ok(Self {
            version: 1,
            space_id: work.base.space_id.clone(),
            work_item_id: work.base.id.clone(),
            intent_fingerprint: work_intent_fingerprint(work)?,
            source_ids,
            origin,
        })
    }
    fn verify(&self, work: &WorkItem) -> StorageResult<()> {
        if self.version != 1
            || self.space_id != work.base.space_id
            || self.work_item_id != work.base.id
            || self.intent_fingerprint != work_intent_fingerprint(work)?
            || work.base.deleted_at.is_some()
        {
            return Err(changed());
        }
        if let WorkProducerOrigin::Child {
            original_parent_attempt,
            child_intent_fingerprint,
            parent_work_item_id,
            original_parent_work_run_id,
            ..
        } = &self.origin
        {
            if original_parent_attempt.space_id != self.space_id
                || &original_parent_attempt.work_item_id != parent_work_item_id
                || original_parent_work_run_id.table() != TableName::WorkRun
                || child_intent_fingerprint != &self.intent_fingerprint
            {
                return Err(invalid("Producer parent identity mismatch"));
            }
        }
        Ok(())
    }
    fn payload(&self) -> StorageResult<String> {
        serde_json::to_string(self).map_err(invalid)
    }
    const fn refs(&self) -> (Option<&RecordId>, Option<&RecordId>, Option<&RecordId>) {
        match &self.origin {
            WorkProducerOrigin::ReprocessJob {
                job_id,
                execution_origin,
                ..
            } => {
                if let ReprocessExecutionOrigin::Inherited {
                    parent_work_item_id,
                    original_parent_work_run_id,
                    ..
                } = execution_origin
                {
                    (
                        Some(job_id),
                        Some(parent_work_item_id),
                        Some(original_parent_work_run_id),
                    )
                } else {
                    (Some(job_id), None, None)
                }
            }
            WorkProducerOrigin::Child {
                parent_work_item_id,
                original_parent_work_run_id,
                ..
            } => (
                None,
                Some(parent_work_item_id),
                Some(original_parent_work_run_id),
            ),
            _ => (None, None, None),
        }
    }
    fn embedded(&self) -> StorageResult<serde_json::Value> {
        let (job, parent, run) = self.refs();
        Ok(serde_json::json!({
            "id":origin_key(&self.work_item_id), "space_id":self.space_id,
            "work_item_id":self.work_item_id, "source_ids":self.source_ids,
            "job_id":job, "parent_work_item_id":parent, "parent_work_run_id":run,
            "payload":self.payload()?, "created_at":chrono::Utc::now(),
        }))
    }
    fn native(&self) -> StorageResult<Value> {
        let (job, parent, run) = self.refs();
        let record_ref = |value: Option<&RecordId>| {
            value.map_or(Value::None, |id| {
                crate::surreal::thing_from_record_id(id).into()
            })
        };
        Ok(Value::Object(surrealdb::sql::Object::from(
            crate::bindings! {
                "space_id" => self.space_id.clone(), "work_item_id" => crate::surreal::thing_from_record_id(&self.work_item_id),
                "source_ids" => crate::surreal::things_from_record_ids(self.source_ids.iter().cloned()),
                "job_id" => record_ref(job), "parent_work_item_id" => record_ref(parent), "parent_work_run_id" => record_ref(run),
                "payload" => self.payload()?, "created_at" => crate::surreal::datetime_from_chrono(chrono::Utc::now()),
            },
        )))
    }
}

fn read_tx(tx: &EmbeddedMutation<'_>, work: &WorkItem) -> StorageResult<Option<OriginEnvelope>> {
    tx.scan_extension_rows(TABLE)?
        .into_iter()
        .find(|(key, _)| key == &origin_key(&work.base.id))
        .map(|(_, row)| {
            if row["space_id"] != work.base.space_id
                || row["work_item_id"] != work.base.id.to_string()
            {
                return Err(changed());
            }
            let envelope: OriginEnvelope = serde_json::from_str(
                row["payload"]
                    .as_str()
                    .ok_or_else(|| invalid("Missing producer payload"))?,
            )
            .map_err(invalid)?;
            envelope.verify(work)?;
            Ok(envelope)
        })
        .transpose()
}

fn insert_tx(tx: &mut EmbeddedMutation<'_>, envelope: &OriginEnvelope) -> StorageResult<()> {
    let id = origin_key(&envelope.work_item_id);
    let old = tx
        .scan_extension_rows(TABLE)?
        .into_iter()
        .find(|(key, _)| key == &id);
    if let Some((_, old)) = old {
        let prior: OriginEnvelope = serde_json::from_str(
            old["payload"]
                .as_str()
                .ok_or_else(|| invalid("Missing producer payload"))?,
        )
        .map_err(invalid)?;
        if &prior != envelope {
            return Err(changed());
        }
        return Ok(());
    }
    tx.upsert_extension_row(TABLE, envelope.embedded()?)?;
    Ok(())
}

fn append_insert(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    envelope: &OriginEnvelope,
) -> StorageResult<()> {
    bindings.insert(
        format!("{prefix}_origin_id"),
        origin_id(&envelope.work_item_id).into(),
    );
    bindings.insert(format!("{prefix}_origin"), envelope.native()?);
    let _ = writeln!(sql,"LET ${prefix}_old_origin = SELECT * FROM ONLY ${prefix}_origin_id;\nIF ${prefix}_old_origin = NONE {{ CREATE ${prefix}_origin_id CONTENT ${prefix}_origin; }} ELSE {{ IF ${prefix}_old_origin.space_id != ${prefix}_origin.space_id OR ${prefix}_old_origin.work_item_id != ${prefix}_origin.work_item_id OR ${prefix}_old_origin.payload != ${prefix}_origin.payload {{ THROW 'work_producer_origin_changed_or_unknown'; }}; }};");
    Ok(())
}

/// Validate the exact payload retained by an original-control read set. Callers
/// must guard these same bytes; a later lookup cannot validate an earlier row.
pub(crate) fn verify_original_control_payload(
    work: &WorkItem,
    payload: &str,
) -> StorageResult<WorkProducerOrigin> {
    let envelope: OriginEnvelope = serde_json::from_str(payload).map_err(invalid)?;
    envelope.verify(work)?;
    Ok(envelope.origin)
}

/// Private origin lookup. This repository has no public HTTP/MCP projection.
#[derive(Clone, Debug)]
pub struct WorkProducerRepository {
    client: StorageClient,
}
impl WorkProducerRepository {
    /// Bind original producer evidence to an existing storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Read the exact actual hot/archive work's immutable origin. Missing is Unknown.
    ///
    /// # Errors
    /// Rejects missing/changed work, corrupt origin evidence and storage errors.
    pub async fn origin_for_work(&self, observed: &WorkItem) -> StorageResult<WorkProducerOrigin> {
        let actual = crate::WorkItemRepository::new(self.client.clone())
            .get(&observed.base.space_id, &observed.base.id)
            .await?
            .ok_or_else(changed)?;
        if work_intent_fingerprint(&actual)? != work_intent_fingerprint(observed)? {
            return Err(changed());
        }
        Ok(self
            .read_envelope(observed)
            .await?
            .map_or(WorkProducerOrigin::Unknown, |row| row.origin))
    }

    /// Read the original job restriction even before its queue handoff materializes.
    ///
    /// # Errors
    /// Rejects changed actual job intent, corrupt control or unavailable storage.
    pub async fn origin_for_job(
        &self,
        observed: &ReprocessJob,
    ) -> StorageResult<WorkProducerOrigin> {
        let actual =
            crate::reprocess_job_repository::ReprocessJobRepository::new(self.client.clone())
                .get(&observed.base.space_id, &observed.base.id)
                .await?
                .ok_or_else(changed)?;
        if job_intent_fingerprint(&actual)? != job_intent_fingerprint(observed)? {
            return Err(changed());
        }
        let intended = crate::reprocess_job_repository::intended_work(&actual)?;
        let Some(envelope) = self.read_envelope(&intended).await? else {
            return Ok(WorkProducerOrigin::Unknown);
        };
        verify_reprocess_origin(&envelope.origin, &actual, &intended)?;
        Ok(envelope.origin)
    }

    async fn read_envelope(&self, observed: &WorkItem) -> StorageResult<Option<OriginEnvelope>> {
        if let Some(store) = self.client.embedded_store() {
            return crate::private_control_read::payload(
                &store,
                TABLE,
                &observed.base.space_id,
                &observed.base.id.id().to_string(),
            )
            .await?
            .map(|payload| {
                let envelope: OriginEnvelope = serde_json::from_str(&payload).map_err(invalid)?;
                envelope.verify(observed)?;
                Ok(envelope)
            })
            .transpose();
        }
        let mut response = self.client.query_bind_checked("SELECT payload FROM $origin WHERE space_id=$space AND work_item_id=$work", crate::bindings! {
            "origin" => origin_id(&observed.base.id), "space" => observed.base.space_id.as_str(), "work" => crate::surreal::thing_from_record_id(&observed.base.id),
        }).await?;
        let rows: Vec<OriginDb> = response.take(0).map_err(invalid)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let envelope: OriginEnvelope = serde_json::from_str(&row.payload).map_err(invalid)?;
        envelope.verify(observed)?;
        Ok(Some(envelope))
    }
}

/// Verify original parent/run identity after its live attempt has ended.
///
/// # Errors
/// Rejects changed semantic intent, run linkage, or original attempt evidence.
pub fn verify_parent_origin(
    origin: &WorkProducerOrigin,
    parent: &WorkItem,
    run: &WorkRun,
) -> StorageResult<()> {
    let WorkProducerOrigin::Child {
        parent_work_item_id,
        original_parent_work_run_id,
        original_parent_attempt,
        parent_intent_fingerprint,
        ..
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
        || run.work_item.id() != &parent.base.id
        || captured != *original_parent_attempt
        || work_intent_fingerprint(parent)? != *parent_intent_fingerprint
    {
        return Err(changed());
    }
    Ok(())
}

/// Verify the original actual reprocess root, without treating current None as unlimited.
///
/// # Errors
/// Rejects absent/mismatched job intent or a different real queue handoff.
pub fn verify_reprocess_origin(
    origin: &WorkProducerOrigin,
    job: &ReprocessJob,
    work: &WorkItem,
) -> StorageResult<()> {
    let WorkProducerOrigin::ReprocessJob {
        job_id,
        original_job_intent_fingerprint,
        ..
    } = origin
    else {
        return Err(changed());
    };
    crate::reprocess_job_repository::verify_reprocess_handoff(job, work)?;
    if &job.base.id != job_id || job_intent_fingerprint(job)? != *original_job_intent_fingerprint {
        return Err(changed());
    }
    Ok(())
}

pub(crate) fn require_reprocess_tx(
    tx: &EmbeddedMutation<'_>,
    work: &WorkItem,
    job: &ReprocessJob,
) -> StorageResult<()> {
    let envelope = read_tx(tx, work)?.ok_or_else(changed)?;
    verify_reprocess_origin(&envelope.origin, job, work)
}

/// Compare private evidence in the caller's transaction; absence never inserts.
fn append_origin_check(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    envelope: &OriginEnvelope,
) -> StorageResult<()> {
    bindings.insert(
        format!("{prefix}_origin_id"),
        origin_id(&envelope.work_item_id).into(),
    );
    bindings.insert(
        format!("{prefix}_origin_payload"),
        envelope.payload()?.into(),
    );
    bindings.insert(
        format!("{prefix}_origin_space"),
        envelope.space_id.clone().into(),
    );
    let _ = writeln!(sql,"LET ${prefix}_origin_read = SELECT * FROM ONLY ${prefix}_origin_id; IF !recall::original_row_matches(${prefix}_origin_read, ${prefix}_origin_space, {{payload:${prefix}_origin_payload}}) {{ THROW 'work_producer_origin_changed_or_unknown'; }};");
    Ok(())
}

/// Compare and fence private evidence for a mutating caller.
fn append_origin_guard(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    envelope: &OriginEnvelope,
) -> StorageResult<()> {
    append_origin_check(sql, bindings, prefix, envelope)?;
    let _ = writeln!(
        sql,
        "UPDATE ${prefix}_origin_id SET read_nonce=<string>rand::uuid();"
    );
    Ok(())
}

/// Append the exact producer check and return its mutation fence separately.
///
/// Recovery must append the returned fence in the same transaction before it
/// creates missing work. Observing an existing handoff performs no mutation and
/// grants no execution authority, so it must not contend with its executor.
pub(crate) async fn prepare_reprocess_recovery_guard(
    client: &StorageClient,
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    work: &WorkItem,
    job: &ReprocessJob,
) -> StorageResult<String> {
    let envelope = WorkProducerRepository::new(client.clone())
        .read_envelope(work)
        .await?
        .ok_or_else(changed)?;
    verify_reprocess_origin(&envelope.origin, job, work)?;
    append_origin_check(sql, bindings, prefix, &envelope)?;
    Ok(format!(
        "UPDATE ${prefix}_origin_id SET read_nonce=<string>rand::uuid();\n"
    ))
}

fn direct(work: &WorkItem) -> StorageResult<OriginEnvelope> {
    // A reserved reprocess handoff needs the real original job transaction.
    let origin = if work.kind == "execute_reprocess_job" {
        WorkProducerOrigin::Unknown
    } else {
        WorkProducerOrigin::Direct
    };
    OriginEnvelope::new(work, origin, &[])
}

pub(crate) fn initialize_direct_tx(
    tx: &mut EmbeddedMutation<'_>,
    work: &WorkItem,
) -> StorageResult<()> {
    insert_tx(tx, &direct(work)?)
}
pub(crate) fn append_direct_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    work: &WorkItem,
) -> StorageResult<()> {
    append_insert(sql, bindings, prefix, &direct(work)?)
}

fn reprocess(work: &WorkItem, job: &ReprocessJob) -> StorageResult<OriginEnvelope> {
    crate::reprocess_job_repository::verify_reprocess_handoff(job, work)?;
    if job
        .budget_limit_euros
        .is_some_and(|limit| !limit.is_finite() || limit < 0.0)
    {
        return Err(invalid("Invalid original currency restriction"));
    }
    OriginEnvelope::new(
        work,
        WorkProducerOrigin::ReprocessJob {
            job_id: job.base.id.clone(),
            original_job_intent_fingerprint: job_intent_fingerprint(job)?,
            original_currency_restricted: job.budget_limit_euros.is_some(),
            execution_origin: ReprocessExecutionOrigin::Independent,
        },
        &job.base.provenance.sources,
    )
}

pub(crate) fn initialize_reprocess_tx(
    tx: &mut EmbeddedMutation<'_>,
    work: &WorkItem,
    job: &ReprocessJob,
) -> StorageResult<()> {
    insert_tx(tx, &reprocess(work, job)?)
}
pub(crate) fn append_reprocess_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    work: &WorkItem,
    job: &ReprocessJob,
) -> StorageResult<()> {
    append_insert(sql, bindings, prefix, &reprocess(work, job)?)
}

pub(crate) fn initialize_reprocess_intent_tx(
    tx: &mut EmbeddedMutation<'_>,
    job: &ReprocessJob,
) -> StorageResult<()> {
    let work = crate::reprocess_job_repository::intended_work(job)?;
    initialize_reprocess_tx(tx, &work, job)
}
pub(crate) fn append_reprocess_intent_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    job: &ReprocessJob,
) -> StorageResult<()> {
    let work = crate::reprocess_job_repository::intended_work(job)?;
    append_reprocess_sql(sql, bindings, prefix, &work, job)
}

#[path = "work_producer_enqueue.rs"]
mod enqueue;

#[cfg(test)]
#[path = "work_producer_tests.rs"]
mod tests;

/// Retained queue reactivation must retain actual private origin evidence.
pub(crate) fn check_existing_work_origin_tx(
    tx: &EmbeddedMutation<'_>,
    work: &WorkItem,
) -> StorageResult<()> {
    let envelope = read_tx(tx, work)?.ok_or_else(changed)?;
    if matches!(envelope.origin, WorkProducerOrigin::Unknown) {
        return Err(changed());
    }
    Ok(())
}

pub(crate) async fn append_existing_work_origin_guard(
    client: &StorageClient,
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    work: &WorkItem,
) -> StorageResult<()> {
    let envelope = WorkProducerRepository::new(client.clone())
        .read_envelope(work)
        .await?
        .ok_or_else(changed)?;
    if matches!(envelope.origin, WorkProducerOrigin::Unknown) {
        return Err(changed());
    }
    append_origin_guard(sql, bindings, prefix, &envelope)
}

#[path = "work_producer_parent.rs"]
mod parent;
pub(crate) use parent::{
    append_reprocess_intent_with_parent_sql, initialize_reprocess_intent_with_parent_tx,
};
pub use parent::{
    verify_reprocess_execution_parent, PreparedReprocessParent, ReprocessExecutionOrigin,
};
