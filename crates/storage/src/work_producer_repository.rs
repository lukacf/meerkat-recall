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

// Bound native record targets avoid scanning every historical producer origin.
const QUEUE_ORIGINS_QUERY: &str = "SELECT work_item_id, space_id, payload FROM $origins";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    historical_transfer: Option<HistoricalTransferProof>,
}

/// New transfer evidence, never a claim about the original producer execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalTransferProof {
    version: u32,
    snapshot_sha256: String,
    original_row_sha256: String,
    hash_domain: String,
    source_space_id: String,
    target_space_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_hold_sha256: Option<String>,
}

fn valid_transfer_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
            historical_transfer: None,
        })
    }
    fn verify_identity(&self, work: &WorkItem) -> StorageResult<()> {
        if self.version != 1
            || self.space_id != work.base.space_id
            || self.work_item_id != work.base.id
        {
            return Err(changed());
        }
        if let Some(proof) = &self.historical_transfer {
            let original = proof.version == 1
                && proof.hash_domain == "homecore-python-json-v1"
                && proof.previous_hold_sha256.is_none();
            let catchup = proof.version == 2
                && proof.hash_domain == "recall-historical-runtime-row/v1"
                && proof
                    .previous_hold_sha256
                    .as_ref()
                    .is_none_or(|hash| valid_transfer_digest(hash));
            if !(original || catchup)
                || proof.source_space_id.trim().is_empty()
                || proof.target_space_id != self.space_id
                || !valid_transfer_digest(&proof.snapshot_sha256)
                || !valid_transfer_digest(&proof.original_row_sha256)
                || !matches!(self.origin, WorkProducerOrigin::Unknown)
            {
                return Err(invalid("Invalid historical transfer hold"));
            }
        }
        Ok(())
    }
    fn verify(&self, work: &WorkItem) -> StorageResult<()> {
        self.verify_identity(work)?;
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

fn read_tx_origin(
    tx: &EmbeddedMutation<'_>,
    work: &WorkItem,
) -> StorageResult<Option<OriginEnvelope>> {
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
            envelope.verify_identity(work)?;
            Ok(envelope)
        })
        .transpose()
}

fn read_tx(tx: &EmbeddedMutation<'_>, work: &WorkItem) -> StorageResult<Option<OriginEnvelope>> {
    let row = read_tx_origin(tx, work)?;
    if let Some(envelope) = &row {
        envelope.verify(work)?;
    }
    Ok(row)
}

/// Unknown historical work remains inert even before dispatch admission.
pub(crate) fn may_claim_or_recover_tx(
    tx: &EmbeddedMutation<'_>,
    work: &WorkItem,
) -> StorageResult<bool> {
    // This gate only prevents invented historical execution. Current authority
    // and producer-intent validation remain the dispatcher's separate contract.
    Ok(read_tx_origin(tx, work)?
        .is_some_and(|row| !matches!(row.origin, WorkProducerOrigin::Unknown)))
}

#[derive(Deserialize)]
struct QueueOrigin {
    work_item_id: Thing,
    space_id: String,
    payload: String,
}

/// Read one bounded queue page's immutable origins without one native query per row.
/// Claims repeat the check with a mutation fence; this read grants no authority.
pub(crate) async fn schedulable_ids(
    client: &StorageClient,
    work: &[WorkItem],
) -> StorageResult<std::collections::BTreeSet<RecordId>> {
    let mut allowed = std::collections::BTreeSet::new();
    if work.is_empty() {
        return Ok(allowed);
    }
    if client.embedded_store().is_some() {
        let repository = WorkProducerRepository::new(client.clone());
        for item in work {
            if repository
                .read_origin_envelope(item)
                .await?
                .is_some_and(|row| !matches!(row.origin, WorkProducerOrigin::Unknown))
            {
                allowed.insert(item.base.id.clone());
            }
        }
        return Ok(allowed);
    }
    let ids: Vec<Value> = work
        .iter()
        .map(|item| Value::Thing(origin_id(&item.base.id)))
        .collect();
    let mut response = client
        .query_bind_checked(
            QUEUE_ORIGINS_QUERY,
            crate::bindings! { "origins" => Value::Array(ids.into()) },
        )
        .await?;
    let rows: Vec<QueueOrigin> = response.take(0).map_err(invalid)?;
    let by_id: BTreeMap<_, _> = work.iter().map(|item| (&item.base.id, item)).collect();
    for row in rows {
        let id = crate::surreal::record_id_from_thing(&row.work_item_id).map_err(invalid)?;
        let item = by_id.get(&id).ok_or_else(changed)?;
        if row.space_id != item.base.space_id {
            return Err(changed());
        }
        let envelope: OriginEnvelope = serde_json::from_str(&row.payload).map_err(invalid)?;
        envelope.verify_identity(item)?;
        if !matches!(envelope.origin, WorkProducerOrigin::Unknown) {
            allowed.insert(id);
        }
    }
    Ok(allowed)
}

/// Prepare the exact immutable producer fence for a claim or lease recovery.
/// Missing history never creates a new origin or changes the original work.
pub(crate) async fn append_claim_or_recovery_guard(
    client: &StorageClient,
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    work: &WorkItem,
) -> StorageResult<bool> {
    let Some(envelope) = WorkProducerRepository::new(client.clone())
        .read_origin_envelope(work)
        .await?
    else {
        return Ok(false);
    };
    if matches!(envelope.origin, WorkProducerOrigin::Unknown) {
        return Ok(false);
    }
    append_origin_guard(sql, bindings, prefix, &envelope)?;
    Ok(true)
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
    let _ = writeln!(
        sql,
        "LET ${prefix}_old_origin = SELECT * FROM ONLY ${prefix}_origin_id;\nIF ${prefix}_old_origin = NONE {{ CREATE ${prefix}_origin_id CONTENT ${prefix}_origin; }} ELSE {{ IF ${prefix}_old_origin.space_id != ${prefix}_origin.space_id OR ${prefix}_old_origin.work_item_id != ${prefix}_origin.work_item_id OR ${prefix}_old_origin.payload != ${prefix}_origin.payload {{ THROW 'work_producer_origin_changed_or_unknown'; }}; }};"
    );
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

/// Original typed hot/archive row plus its independently verified snapshot hash.
#[derive(Debug, Clone)]
pub struct HistoricalWorkHoldInput {
    /// Complete original hot or archived work row.
    pub original: crate::transfer_batch::TransferRecord,
    /// SHA-256 of the original lexical snapshot row.
    pub original_row_sha256: String,
    /// Original snapshot namespace.
    pub source_space_id: String,
    /// Declared destination namespace after relocation.
    pub target_space_id: String,
}

/// Exact committed private control inventory in native `type::string` SHA256 domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalHoldEffect {
    /// Physical table changed while registering the hold.
    pub table: String,
    /// Exact native control record identity.
    pub id: String,
    /// Original native row digest, absent when the control did not exist.
    pub before_sha256: Option<String>,
    /// Resulting native row digest, including the complete control wrapper.
    pub after_sha256: String,
}

/// Ordered payload identities plus full native control row changes.
#[derive(Debug, Clone)]
pub struct HistoricalWorkHoldRegistration {
    /// Registered origin payload digests in the input row order.
    pub payload_sha256: Vec<String>,
    /// Exact native control and guard changes for final inventory reconciliation.
    pub native_effects: Vec<HistoricalHoldEffect>,
}

/// Private origin lookup. This repository has no public HTTP/MCP projection.
#[derive(Clone, Debug)]
pub struct WorkProducerRepository {
    client: StorageClient,
}
type PreparedHistoricalHold<'a> = (&'a WorkItem, bool, OriginEnvelope, Value);

fn prepare_historical_holds<'a>(
    snapshot_sha256: &str,
    originals: &[HistoricalWorkHoldInput],
    projected: &'a [crate::transfer_batch::TransferRecord],
) -> StorageResult<Vec<PreparedHistoricalHold<'a>>> {
    use crate::transfer_batch::TransferRecord;
    let mut seen = std::collections::BTreeSet::new();
    let mut prepared = Vec::with_capacity(originals.len());
    let mut byte_count = 0usize;
    for (input, projected) in originals.iter().zip(projected) {
        let (work, archived) = match projected {
            TransferRecord::WorkItem(work) => (work, false),
            TransferRecord::WorkItemArchive(archive) => (&archive.record, true),
            _ => {
                return Err(invalid(
                    "Historical work hold requires work or work archive",
                ))
            }
        };
        if !seen.insert(&work.base.id) || !valid_transfer_digest(&input.original_row_sha256) {
            return Err(invalid("Historical hold duplicate or original hash"));
        }
        let mut envelope = OriginEnvelope::new(work, WorkProducerOrigin::Unknown, &[])?;
        envelope.historical_transfer = Some(HistoricalTransferProof {
            version: 1,
            snapshot_sha256: snapshot_sha256.into(),
            original_row_sha256: input.original_row_sha256.clone(),
            hash_domain: "homecore-python-json-v1".into(),
            source_space_id: input.source_space_id.clone(),
            target_space_id: input.target_space_id.clone(),
            previous_hold_sha256: None,
        });
        envelope.verify_identity(work)?;
        let Value::Object(mut native) = projected.native()? else {
            return Err(invalid("Historical work is not an object"));
        };
        native.insert(
            "id".into(),
            crate::surreal::thing_from_record_id(input.original.id()).into(),
        );
        let native = crate::surreal::stored_native_value(Value::Object(native));
        let payload_len = envelope.payload()?.len();
        byte_count = byte_count
            .checked_add(native.to_string().len())
            .and_then(|n| n.checked_add(payload_len))
            .ok_or_else(|| invalid("Historical hold bytes overflow"))?;
        if byte_count > 256 * 1024 * 1024 {
            return Err(invalid("Historical hold encoded bytes bound"));
        }
        prepared.push((work, archived, envelope, native));
    }
    Ok(prepared)
}

fn append_historical_hold_rows(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    originals: &[HistoricalWorkHoldInput],
    prepared: &[PreparedHistoricalHold<'_>],
) -> StorageResult<()> {
    for (index, (input, (work, archived, envelope, native))) in
        originals.iter().zip(prepared).enumerate()
    {
        bindings.insert(
            format!("work_{index}"),
            crate::surreal::thing_from_record_id(input.original.id()).into(),
        );
        let other = if *archived {
            work.base.id.clone()
        } else {
            crate::work_retention::archive_id(&work.base.id)
        };
        bindings.insert(
            format!("other_{index}"),
            crate::surreal::thing_from_record_id(&other).into(),
        );
        bindings.insert(format!("expected_{index}"), native.clone());
        writeln!(sql, "IF (SELECT * FROM ONLY $work_{index}) != $expected_{index} OR (SELECT VALUE id FROM ONLY $other_{index}) != NONE {{ THROW 'historical_work_changed'; }};").map_err(invalid)?;
        bindings.insert(
            format!("origin_inventory_{index}"),
            origin_id(&work.base.id).into(),
        );
        writeln!(sql, "LET $origin_before_{index} = (SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $origin_inventory_{index});").map_err(invalid)?;
        append_insert(sql, bindings, &format!("historical_{index}"), envelope)?;
    }
    Ok(())
}

fn validate_historical_hold_effects(
    effects: &[HistoricalHoldEffect],
    prepared: &[PreparedHistoricalHold<'_>],
    spaces: &[String],
) -> StorageResult<()> {
    if effects.len() != prepared.len() + spaces.len()
        || effects
            .iter()
            .zip(prepared)
            .any(|(effect, (work, _, _, _))| {
                effect.table != TABLE
                    || effect.id != origin_id(&work.base.id).to_string()
                    || !valid_transfer_digest(&effect.after_sha256)
                    || effect
                        .before_sha256
                        .as_ref()
                        .is_some_and(|hash| !valid_transfer_digest(hash))
            })
    {
        return Err(invalid("Historical hold native inventory differs"));
    }
    if effects[prepared.len()..]
        .iter()
        .zip(spaces)
        .any(|(effect, space)| {
            effect.table != "source_erasure_guard"
                || effect.id
                    != crate::source_publication::control_thing("source_erasure_guard", space)
                        .to_string()
                || !valid_transfer_digest(&effect.after_sha256)
                || effect
                    .before_sha256
                    .as_ref()
                    .is_some_and(|hash| !valid_transfer_digest(hash))
        })
    {
        return Err(invalid("Historical hold guard inventory differs"));
    }
    Ok(())
}

impl WorkProducerRepository {
    /// Bind original producer evidence to an existing storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Bind an imported row to an inert historical hold after complete input
    /// reconciliation, while exclusive transfer custody is still active.
    /// Returns the exact private payload SHA for final control reconciliation.
    ///
    /// # Errors
    /// Refuses wrong custody/input, changed original work or an existing producer.
    pub async fn register_historical_hold(
        &self,
        work: &WorkItem,
        snapshot_sha256: &str,
        original_row_sha256: &str,
    ) -> StorageResult<String> {
        self.register_historical_holds(
            snapshot_sha256,
            &[HistoricalWorkHoldInput {
                original: crate::transfer_batch::TransferRecord::WorkItem(work.clone()),
                original_row_sha256: original_row_sha256.into(),
                source_space_id: work.base.space_id.clone(),
                target_space_id: work.base.space_id.clone(),
            }],
        )
        .await?
        .payload_sha256
        .into_iter()
        .next()
        .ok_or_else(changed)
    }

    /// Register at most 256 reconciled original hot/archive work rows atomically.
    /// No origin is inserted until every original payload in the batch matches.
    /// Inputs retain the original source namespace; this method applies only the
    /// declared typed namespace relocation. Payload hashes retain supplied order.
    /// Native effects capture complete actual origin rows and their exact per-space
    /// erasure-guard revision effects in the same transaction.
    ///
    /// # Errors
    /// Refuses custody, duplicate identities, bounds, typed row mismatch or changed producers.
    pub async fn register_historical_holds(
        &self,
        snapshot_sha256: &str,
        originals: &[HistoricalWorkHoldInput],
    ) -> StorageResult<HistoricalWorkHoldRegistration> {
        use crate::transfer_batch::TransferRecord;
        use elephant_types::records::WorkItemArchive;
        use sha2::Digest as _;
        self.client.require_transfer_custody()?;
        let marker = self
            .client
            .erasure_journal()?
            .transfer_marker()?
            .ok_or_else(changed)?;
        if marker.source_sha256 != snapshot_sha256
            || !valid_transfer_digest(snapshot_sha256)
            || originals.is_empty()
            || originals.len() > 256
        {
            return Err(invalid("Historical transfer binding or row bound"));
        }
        let projected = originals
            .iter()
            .map(|input| {
                if input.source_space_id.trim().is_empty()
                    || input.target_space_id.trim().is_empty()
                    || input.original.base().space_id != input.source_space_id
                {
                    return Err(invalid("Historical hold source mapping differs"));
                }
                let mut row = input.original.clone();
                row.relocate_space(&input.source_space_id, &input.target_space_id)?;
                Ok(row)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let prepared = prepare_historical_holds(snapshot_sha256, originals, &projected)?;
        let native_effects = if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    self.client.require_transfer_custody()?;
                    for (projected, (work, archived, envelope, _)) in
                        projected.iter().zip(&prepared)
                    {
                        let hot = tx.get::<WorkItem>(&work.base.space_id, &work.base.id, true)?;
                        let cold = tx.get::<WorkItemArchive>(
                            &work.base.space_id,
                            &crate::work_retention::archive_id(&work.base.id),
                            true,
                        )?;
                        match projected {
                            TransferRecord::WorkItem(original)
                                if !*archived
                                    && cold.is_none()
                                    && hot.as_ref() == Some(original) => {}
                            TransferRecord::WorkItemArchive(original)
                                if *archived
                                    && hot.is_none()
                                    && cold.as_ref() == Some(original) => {}
                            _ => return Err(changed()),
                        }
                        insert_tx(tx, envelope)?;
                    }
                    Ok(())
                })
                .await?;
            Vec::new()
        } else {
            self.register_native_historical_holds(originals, &prepared)
                .await?
        };
        let payload_sha256 = prepared
            .iter()
            .map(|(_, _, envelope, _)| {
                Ok(hex::encode(sha2::Sha256::digest(
                    envelope.payload()?.as_bytes(),
                )))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(HistoricalWorkHoldRegistration {
            payload_sha256,
            native_effects,
        })
    }

    async fn register_native_historical_holds(
        &self,
        originals: &[HistoricalWorkHoldInput],
        prepared: &[PreparedHistoricalHold<'_>],
    ) -> StorageResult<Vec<HistoricalHoldEffect>> {
        let mut sql = "BEGIN TRANSACTION;".to_string();
        let mut bindings = BTreeMap::new();
        let spaces = prepared
            .iter()
            .map(|(work, _, _, _)| work.base.space_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        sql.push_str("LET $hold_key_tokens = recall::reference_tokens($__recall_journal_identity, '__recall_custody__', $__recall_reference_key, [], type::thing('recall_reference_key', $__recall_journal_identity)); LET $hold_key_fingerprint = $hold_key_tokens[0];");
        for (index, space) in spaces.iter().enumerate() {
            bindings.insert(
                format!("hold_guard_{index}_id"),
                crate::source_publication::control_thing("source_erasure_guard", space).into(),
            );
            bindings.insert(format!("hold_space_{index}"), space.clone().into());
            writeln!(sql, "LET $hold_guard_before_{index} = SELECT * FROM ONLY $hold_guard_{index}_id; IF $hold_guard_before_{index} != NONE AND ($hold_guard_before_{index}.space_id != $hold_space_{index} OR $hold_guard_before_{index}.journal_id != $__recall_journal_identity OR $hold_guard_before_{index}.key_fingerprint != $hold_key_fingerprint OR $hold_guard_before_{index}.version != 1 OR $hold_guard_before_{index}.membership_count != 0 OR $hold_guard_before_{index}.erasure_reservation != NONE OR !type::is::int($hold_guard_before_{index}.revision) OR $hold_guard_before_{index}.revision < 0) {{ THROW 'historical_hold_guard_changed'; }}; IF array::len(SELECT VALUE id FROM source_erasure_membership WHERE space_id = $hold_space_{index}) != 0 {{ THROW 'historical_hold_memberships_present'; }};").map_err(invalid)?;
        }
        append_historical_hold_rows(&mut sql, &mut bindings, originals, prepared)?;
        for (index, space) in spaces.iter().enumerate() {
            let increments = prepared
                .iter()
                .enumerate()
                .filter(|(_, (work, _, _, _))| &work.base.space_id == space)
                .map(|(i, _)| format!("(IF $origin_before_{i} = NONE THEN 1 ELSE 0 END)"))
                .collect::<Vec<_>>()
                .join(", ");
            // Each array element has fixed expression depth even at the 256-row bound.
            let increments = format!("math::sum([{increments}])");
            writeln!(sql, "LET $hold_guard_base_{index} = IF $hold_guard_before_{index} = NONE THEN {{id:$hold_guard_{index}_id,space_id:$hold_space_{index},journal_id:$__recall_journal_identity,key_fingerprint:$hold_key_fingerprint,version:1,membership_count:0,revision:0}} ELSE $hold_guard_before_{index} END; LET $hold_guard_expected_{index} = object::from_entries(array::concat(array::filter(object::entries($hold_guard_base_{index}), |$entry| $entry[0] != 'revision'), [['revision', $hold_guard_base_{index}.revision + {increments}]])); IF (SELECT * FROM ONLY $hold_guard_{index}_id) != $hold_guard_expected_{index} {{ THROW 'historical_hold_guard_effect_differs'; }};").map_err(invalid)?;
        }
        sql.push_str("RETURN [");
        for index in 0..prepared.len() {
            if index > 0 {
                sql.push(',');
            }
            write!(sql, "{{table:'work_producer_origin',id:type::string($origin_inventory_{index}),before_sha256:$origin_before_{index},after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $origin_inventory_{index})}}").map_err(invalid)?;
        }
        for index in 0..spaces.len() {
            write!(sql, ",{{table:'source_erasure_guard',id:type::string($hold_guard_{index}_id),before_sha256:IF $hold_guard_before_{index} = NONE THEN NONE ELSE crypto::sha256(type::string($hold_guard_before_{index})) END,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $hold_guard_{index}_id)}}").map_err(invalid)?;
        }
        sql.push_str("]; COMMIT TRANSACTION;");
        self.client.require_transfer_custody()?;
        let mut response = self.client.query_bind_checked(&sql, bindings).await?;
        let result_index = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(changed)?;
        let effects: Vec<HistoricalHoldEffect> = response.take(result_index).map_err(invalid)?;
        validate_historical_hold_effects(&effects, prepared, &spaces)?;
        Ok(effects)
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
        let row = self.read_origin_envelope(observed).await?;
        if let Some(envelope) = &row {
            envelope.verify(observed)?;
        }
        Ok(row)
    }

    async fn read_origin_envelope(
        &self,
        observed: &WorkItem,
    ) -> StorageResult<Option<OriginEnvelope>> {
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
                envelope.verify_identity(observed)?;
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
        envelope.verify_identity(observed)?;
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
    let _ = writeln!(
        sql,
        "LET ${prefix}_origin_read = SELECT * FROM ONLY ${prefix}_origin_id; IF !recall::original_row_matches(${prefix}_origin_read, ${prefix}_origin_space, {{payload:${prefix}_origin_payload}}) {{ THROW 'work_producer_origin_changed_or_unknown'; }};"
    );
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

#[cfg(test)]
#[path = "work_producer_queue_tests.rs"]
mod queue_tests;

#[cfg(test)]
#[path = "historical_work_hold_tests.rs"]
mod historical_work_hold_tests;

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

/// Prepare an S1 Unknown envelope while retaining a cryptographic link to the
/// original held payload. The caller must CAS the full origin and work images.
#[cfg(test)]
pub(crate) fn historical_catchup_envelope(
    before: Option<&WorkItem>,
    after: Option<&WorkItem>,
    original_payload: Option<&str>,
    spaces: (&str, &str),
    snapshots: (&str, &str),
    after_original_hash: Option<&str>,
) -> StorageResult<Option<(String, Vec<RecordId>)>> {
    historical_catchup_envelope_with_lineage(
        before,
        after,
        original_payload,
        spaces,
        snapshots,
        after_original_hash,
        None,
    )
}

/// Preserve a truthfully older hold only with storage-verified exact predecessor
/// evidence. The proof never changes the Unknown producer or dispatch admission.
pub(crate) fn historical_catchup_envelope_with_lineage(
    before: Option<&WorkItem>,
    after: Option<&WorkItem>,
    original_payload: Option<&str>,
    spaces: (&str, &str),
    snapshots: (&str, &str),
    after_original_hash: Option<&str>,
    inherited: Option<&crate::client::historical_catchup::completion::VerifiedHistoricalControl>,
) -> StorageResult<Option<(String, Vec<RecordId>)>> {
    use sha2::Digest as _;
    let (source_space, target_space) = spaces;
    let (before_snapshot, after_snapshot) = snapshots;
    let previous = match (before, original_payload) {
        (Some(work), Some(payload)) => {
            let old: OriginEnvelope = serde_json::from_str(payload).map_err(invalid)?;
            old.verify_identity(work)?;
            let proof = old.historical_transfer.as_ref().ok_or_else(changed)?;
            if old.origin != WorkProducerOrigin::Unknown
                || old.intent_fingerprint != work_intent_fingerprint(work)?
                || proof.source_space_id != source_space
                || proof.target_space_id != target_space
                || (proof.snapshot_sha256 != before_snapshot
                    && !inherited
                        .is_some_and(|proof| proof.matches_work_payload(&work.base.id, payload)))
            {
                return Err(changed());
            }
            Some(hex::encode(sha2::Sha256::digest(payload.as_bytes())))
        }
        (None, None) => None,
        _ => return Err(changed()),
    };
    let Some(work) = after else {
        return Ok(None);
    };
    let hash = after_original_hash.ok_or_else(changed)?;
    if !valid_transfer_digest(hash)
        || !valid_transfer_digest(after_snapshot)
        || work.base.space_id != target_space
    {
        return Err(changed());
    }
    let mut envelope = OriginEnvelope::new(work, WorkProducerOrigin::Unknown, &[])?;
    envelope.historical_transfer = Some(HistoricalTransferProof {
        version: 2,
        snapshot_sha256: after_snapshot.into(),
        original_row_sha256: hash.into(),
        hash_domain: "recall-historical-runtime-row/v1".into(),
        source_space_id: source_space.into(),
        target_space_id: target_space.into(),
        previous_hold_sha256: previous,
    });
    envelope.verify_identity(work)?;
    Ok(Some((envelope.payload()?, envelope.source_ids)))
}
