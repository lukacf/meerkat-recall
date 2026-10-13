//! Original source-operation accounting provenance.
//!
//! Selection semantics belong to the reprocessing coordinator; this boundary seals its
//! actual read set and immutable producer identity in the same transaction as source
//! acceptance.

use std::collections::BTreeMap;

use elephant_types::publication::{AttemptFence, PublicationFence, SourceProducerOrigin};
use elephant_types::records::{ArtifactStatus, ReprocessJob, Source, WorkArtifact};
use elephant_types::{RecordId, TableName};
use surrealdb::sql::Value;

use crate::canonical_commit::{source_support_fingerprint, CanonicalRecord};
use crate::source_publication::{
    conflict, integrity, AcceptSourceRevision, SourcePublicationRepository, StoredOperation,
    OPERATION_TABLE,
};
use crate::{EmbeddedMutation, StorageResult};

/// A coordinator-verified saved selection. Private fields prevent a transport
/// from supplying arbitrary producer IDs or a caller-chosen budget flag.
#[derive(Debug, Clone)]
pub struct PreparedSourceProducer {
    observed_epoch: String,
    job: ReprocessJob,
    selection: WorkArtifact,
    intent: WorkArtifact,
    verified_intents: Vec<WorkArtifact>,
    selection_digest: String,
    intent_fingerprint: String,
    configuration: elephant_types::provider_binding::SpaceConfigToken,
}

impl PreparedSourceProducer {
    /// Captures the real rows after the coordinator verifies its closed selection
    /// count, digest, source intent and recipe under this original epoch.
    ///
    /// The
    /// acceptance transaction rechecks that epoch and every full row snapshot.
    /// The coordinator and physical dispatcher must additionally verify the
    /// job's immutable original queue origin; Direct never means unlimited.
    ///
    /// # Errors
    /// Rejects absent identity, foreign/deleted rows, wrong artifact kinds,
    /// invalid finite budgets, or mismatched saved job/source identities.
    pub fn from_verified_reprocess_selection(
        observed_epoch: &str,
        job: &ReprocessJob,
        selection: &WorkArtifact,
        intent: &WorkArtifact,
        verified_intents: &[WorkArtifact],
    ) -> StorageResult<Self> {
        if observed_epoch.is_empty()
            || job.base.id.table() != TableName::ReprocessJob
            || job.base.deleted_at.is_some()
            || job.base.space_id.trim().is_empty()
            || job
                .budget_limit_euros
                .is_some_and(|budget| !budget.is_finite() || budget < 0.0)
        {
            return Err(integrity("invalid original source producer job"));
        }
        for (row, kind) in [
            (selection, "reprocess_selection/v1"),
            (intent, "reprocess_source_intent/v1"),
        ] {
            if row.base.id.table() != TableName::WorkArtifact
                || row.base.space_id != job.base.space_id
                || row.base.deleted_at.is_some()
                || row.artifact_type != kind
                || row.status != ArtifactStatus::Validated
                || row.validated_at.is_none()
                || record_field(&row.data, "job_id")? != job.base.id
            {
                return Err(integrity("invalid saved source producer evidence"));
            }
        }
        if selection.base.id == intent.base.id || selection.work_run != intent.work_run {
            return Err(integrity("source producer selection and intent differ"));
        }
        let source: Source =
            serde_json::from_value(required_field(&intent.data, "source")?.clone())
                .map_err(|error| integrity(&error.to_string()))?;
        if source.base.id.table() != TableName::Source
            || source.base.space_id != job.base.space_id
            || source.base.deleted_at.is_some()
        {
            return Err(integrity("invalid saved producer source"));
        }
        let selection_digest = string_field(&selection.data, "source_artifact_digest")?.to_owned();
        if selection_digest.is_empty()
            || required_field(&selection.data, "source_count")?
                .as_u64()
                .is_none_or(|count| count == 0)
            || required_field(&selection.data, "configuration")?
                != required_field(&intent.data, "configuration")?
        {
            return Err(integrity("incomplete original source selection"));
        }
        let mut ids = std::collections::BTreeSet::new();
        for row in verified_intents {
            if row.base.id.table() != TableName::WorkArtifact
                || row.base.space_id != job.base.space_id
                || row.base.deleted_at.is_some()
                || row.artifact_type != "reprocess_source_intent/v1"
                || row.status != ArtifactStatus::Validated
                || row.validated_at.is_none()
                || row.work_run != selection.work_run
                || record_field(&row.data, "job_id")? != job.base.id
                || !ids.insert(row.base.id.clone())
            {
                return Err(integrity("invalid closed source selection"));
            }
        }
        if required_field(&selection.data, "source_count")?.as_u64()
            != u64::try_from(verified_intents.len()).ok()
            || !verified_intents.iter().any(|row| row == intent)
        {
            return Err(integrity("incomplete closed source selection"));
        }
        Ok(Self {
            observed_epoch: observed_epoch.into(),
            job: job.clone(),
            selection: selection.clone(),
            intent: intent.clone(),
            verified_intents: verified_intents.to_vec(),
            selection_digest,
            intent_fingerprint: source_support_fingerprint(intent)?,
            configuration: serde_json::from_value(
                required_field(&intent.data, "configuration")?.clone(),
            )
            .map_err(|error| integrity(&error.to_string()))?,
        })
    }

    fn origin(&self, attempt: Option<&AttemptFence>) -> SourceProducerOrigin {
        SourceProducerOrigin::Reprocess {
            job_id: self.job.base.id.clone(),
            selection_artifact_id: self.selection.base.id.clone(),
            source_intent_id: self.intent.base.id.clone(),
            selection_digest: self.selection_digest.clone(),
            intent_fingerprint: self.intent_fingerprint.clone(),
            currency_restricted: self.job.budget_limit_euros.is_some(),
            original_attempt: attempt.cloned(),
        }
    }

    pub(crate) fn validate_request(&self, request: &AcceptSourceRevision) -> StorageResult<()> {
        let saved: Source =
            serde_json::from_value(required_field(&self.intent.data, "source")?.clone())
                .map_err(|error| integrity(&error.to_string()))?;
        if self.job.base.space_id != request.source.base.space_id
            || saved.base.id != request.source.base.id
            || crate::canonical_commit::canonical_json(
                serde_json::to_value(&saved).map_err(|error| integrity(&error.to_string()))?,
            ) != crate::canonical_commit::canonical_json(
                serde_json::to_value(&request.source)
                    .map_err(|error| integrity(&error.to_string()))?,
            )
            || string_field(&self.intent.data, "revision_id")? != request.revision_id
            || string_field(&self.intent.data, "idempotency_key")? != request.idempotency_key
            || string_field(&self.intent.data, "request_fingerprint")?
                != request.request_fingerprint
            || required_field(&self.intent.data, "expected")?
                != &serde_json::to_value(&request.expected)
                    .map_err(|error| integrity(&error.to_string()))?
            || request
                .authorized
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| integrity(&error.to_string()))?
                .as_ref()
                != Some(required_field(&self.intent.data, "authorized")?)
            || request.reactivate
            || request.recreate_after_erasure
        {
            return Err(conflict("source_producer_intent_changed"));
        }
        Ok(())
    }

    fn dependencies(&self) -> Vec<CanonicalRecord> {
        let mut rows = vec![
            CanonicalRecord::ReprocessJob(Box::new(self.job.clone())),
            CanonicalRecord::WorkArtifact(Box::new(self.selection.clone())),
        ];
        rows.extend(
            self.verified_intents
                .iter()
                .cloned()
                .map(|row| CanonicalRecord::WorkArtifact(Box::new(row))),
        );
        rows
    }

    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "This guard is called within the exclusive mutation protocol; preserve its existing mutable transaction-borrow interface and lifetime through validation."
    )]
    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        if crate::canonical_commit::epoch_tx(tx, &self.job.base.space_id)?.as_deref()
            != Some(self.observed_epoch.as_str())
        {
            return Err(conflict("source_producer_inputs_changed"));
        }
        for before in self.dependencies() {
            let current = CanonicalRecord::load_tx(tx, &self.job.base.space_id, &before.base().id)?;
            if current
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| integrity(&error.to_string()))?
                != Some(
                    serde_json::to_value(before).map_err(|error| integrity(&error.to_string()))?,
                )
            {
                return Err(conflict("source_producer_evidence_changed"));
            }
        }
        let actual: std::collections::BTreeSet<_> = tx
            .scan::<WorkArtifact>()?
            .into_iter()
            .filter(|row| {
                row.base.space_id == self.job.base.space_id
                    && row.base.deleted_at.is_none()
                    && row.artifact_type == "reprocess_source_intent/v1"
                    && row.data.get("job_id") == Some(&serde_json::json!(self.job.base.id))
            })
            .map(|row| row.base.id)
            .collect();
        if actual
            != self
                .verified_intents
                .iter()
                .map(|row| row.base.id.clone())
                .collect()
        {
            return Err(conflict("source_producer_selection_changed"));
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.extend(crate::bindings! {"producer_epoch_id"=>crate::source_publication::control_thing("canonical_epoch",&self.job.base.space_id),"producer_observed_epoch"=>self.observed_epoch.clone()});
        sql.push_str("IF (SELECT VALUE version FROM ONLY publication_schema:work_producer) != 1 { THROW 'publication:source_producer_schema_missing'; };\nIF (SELECT VALUE epoch FROM ONLY $producer_epoch_id) != $producer_observed_epoch { THROW 'publication:source_producer_inputs_changed'; };\n");
        for (index, before) in self.dependencies().iter().enumerate() {
            crate::canonical_commit::append_row_guard(
                sql,
                bindings,
                &format!("source_producer_{index}"),
                before,
                Some(before),
            )?;
        }
        bindings.insert(
            "producer_selection_job".into(),
            crate::surreal::json_to_surreal(serde_json::json!(self.job.base.id))?,
        );
        bindings.insert(
            "producer_selection_ids".into(),
            Value::Array(surrealdb::sql::Array::from(
                self.verified_intents
                    .iter()
                    .map(|row| Value::from(crate::surreal::thing_from_record_id(&row.base.id)))
                    .collect::<Vec<_>>(),
            )),
        );
        sql.push_str("IF array::sort((SELECT VALUE id FROM work_artifact WHERE space_id=$space AND deleted_at=NONE AND artifact_type='reprocess_source_intent/v1' AND data.job_id=$producer_selection_job)) != array::sort($producer_selection_ids) { THROW 'publication:source_producer_selection_changed'; };\n");
        Ok(())
    }
}

fn required_field<'a>(
    value: &'a serde_json::Value,
    name: &str,
) -> StorageResult<&'a serde_json::Value> {
    value
        .get(name)
        .ok_or_else(|| integrity("incomplete source producer evidence"))
}
fn string_field<'a>(value: &'a serde_json::Value, name: &str) -> StorageResult<&'a str> {
    required_field(value, name)?
        .as_str()
        .ok_or_else(|| integrity("invalid source producer field"))
}
fn record_field(value: &serde_json::Value, name: &str) -> StorageResult<RecordId> {
    serde_json::from_value(required_field(value, name)?.clone())
        .map_err(|error| integrity(&error.to_string()))
}

pub(crate) fn prepared_origin(
    producer: Option<&PreparedSourceProducer>,
    attempt: Option<&AttemptFence>,
) -> SourceProducerOrigin {
    producer.map_or(SourceProducerOrigin::Direct, |producer| {
        producer.origin(attempt)
    })
}

/// A retry cannot change its producer. Mutable current budget or retry claim
/// does not replace the original restriction/attempt already in the receipt.
pub(crate) fn same_producer_intent(
    left: &SourceProducerOrigin,
    right: &SourceProducerOrigin,
) -> bool {
    match (left, right) {
        (SourceProducerOrigin::UnknownOriginal, SourceProducerOrigin::UnknownOriginal)
        | (SourceProducerOrigin::Direct, SourceProducerOrigin::Direct) => true,
        (
            SourceProducerOrigin::Reprocess {
                job_id: left_job_id,
                selection_artifact_id: left_selection_artifact_id,
                source_intent_id: left_source_intent_id,
                selection_digest: left_selection_digest,
                intent_fingerprint: left_intent_fingerprint,
                ..
            },
            SourceProducerOrigin::Reprocess {
                job_id: right_job_id,
                selection_artifact_id: right_selection_artifact_id,
                source_intent_id: right_source_intent_id,
                selection_digest: right_selection_digest,
                intent_fingerprint: right_intent_fingerprint,
                ..
            },
        ) => {
            left_job_id == right_job_id
                && left_selection_artifact_id == right_selection_artifact_id
                && left_source_intent_id == right_source_intent_id
                && left_selection_digest == right_selection_digest
                && left_intent_fingerprint == right_intent_fingerprint
        }
        (
            SourceProducerOrigin::Watch {
                watch_id: left_watch_id,
                watch_fingerprint: left_watch_fingerprint,
                work_item_id: left_work_item_id,
                work_run_id: left_work_run_id,
                original_attempt: left_original_attempt,
                work_intent_fingerprint: left_work_intent_fingerprint,
            },
            SourceProducerOrigin::Watch {
                watch_id: right_watch_id,
                watch_fingerprint: right_watch_fingerprint,
                work_item_id: right_work_item_id,
                work_run_id: right_work_run_id,
                original_attempt: right_original_attempt,
                work_intent_fingerprint: right_work_intent_fingerprint,
            },
        ) => {
            left_watch_id == right_watch_id
                && left_watch_fingerprint == right_watch_fingerprint
                && left_work_item_id == right_work_item_id
                && left_work_run_id == right_work_run_id
                && left_original_attempt == right_original_attempt
                && left_work_intent_fingerprint == right_work_intent_fingerprint
        }
        _ => false,
    }
}

impl SourcePublicationRepository {
    /// Accepts saved reprocessing intent with original producer, contact, claim,
    /// configuration and source guards in one storage transaction.
    pub async fn accept_with_source_producer(
        &self,
        request: AcceptSourceRevision,
        producer: &PreparedSourceProducer,
        attempt: Option<&AttemptFence>,
        configuration: Option<&elephant_types::provider_binding::SpaceConfigToken>,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
    ) -> StorageResult<elephant_types::publication::OperationReceipt> {
        if configuration.is_some_and(|configuration| configuration != &producer.configuration) {
            return Err(conflict("source_producer_configuration_changed"));
        }
        self.accept_inner(
            request,
            attempt,
            Some(&producer.configuration),
            contact,
            Some(producer),
            None,
            None,
        )
        .await
    }

    /// Reads only the original private accounting restriction for this exact
    /// accepted operation.
    ///
    /// Physical dispatch still requires the current source
    /// fence, actual referenced job/selection, and immutable queue producer chain.
    pub async fn source_operation_producer(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<SourceProducerOrigin> {
        let Some(operation) = self
            .read_control::<StoredOperation>(OPERATION_TABLE, &publication.operation_id, space)
            .await?
        else {
            return Ok(SourceProducerOrigin::UnknownOriginal);
        };
        let receipt = &operation.receipt;
        if receipt.space_id != space
            || receipt.source_id != publication.source_id
            || receipt.source_generation != publication.generation
            || receipt.operation_id != publication.operation_id
            || receipt.desired_revision.as_deref() != Some(publication.revision_id.as_str())
        {
            return Err(conflict("source_producer_publication_changed"));
        }
        Ok(operation.producer_origin)
    }
}
