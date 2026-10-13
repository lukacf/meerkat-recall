//! Transactional source acceptance and the desired/published head authority.
//!
//! Control rows use an explicit payload string at the database boundary. This
//! avoids flatten/native-value deserialization and preserves one typed public
//! authority on both backends. Indexed identity fields are stored separately.

use std::fmt::Write as _;

use chrono::Utc;
use elephant_types::publication::{
    OperationReceipt, OperationState, SourceAuthorizationEnvelope, SourceAuthorizationSnapshot,
    SourceHead, SourcePrecondition, SourceRevision, SourceVisibility, MAX_SOURCE_TEXT_BYTES,
};
use elephant_types::records::{Source, SourceType, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use surrealdb::sql::{Thing, Value};
use uuid::Uuid;

use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};

// Shared projection for the two payload-only control reads. No flattened
// fields, conversion defaults or query/transaction changes are introduced.
#[derive(Deserialize)]
struct ControlPayloadDb {
    payload: String,
}

pub(crate) const HEAD_TABLE: &str = "source_head";
pub(crate) const REVISION_TABLE: &str = "source_revision";
pub(crate) const OPERATION_TABLE: &str = "source_operation";

/// Input accepted after adapter authorization of both the old and new envelope.
#[derive(Debug, Clone)]
pub struct AcceptSourceRevision {
    /// Complete source-first intake, including staged manual input if present.
    pub source: Source,
    /// Immutable consumer-selected source revision.
    pub revision_id: String,
    /// Explicit accepted-head comparison.
    pub expected: SourcePrecondition,
    /// Stable request identity within the space.
    pub idempotency_key: String,
    /// Canonical adapter request fingerprint, excluding the expected generation.
    pub request_fingerprint: String,
    /// Explicit revival of a retracted incarnation; never enabled by retries.
    pub reactivate: bool,
    /// Explicit allocation of a fresh incarnation after physical erasure.
    pub recreate_after_erasure: bool,
    /// Pre-state already authorized by the adapter; absent only for a new source.
    pub authorized: Option<SourceAuthorizationSnapshot>,
    /// Exact authorized document pre-state for retained document correction/revert.
    pub authorized_document: Option<elephant_types::records::Doc>,
}

/// Read-only admission proof. A plain accepted operation is distinct from a
/// new intent; editable source metadata cannot create a contact capability.
#[derive(Debug, Clone)]
pub enum SourceAcceptanceRetry {
    /// No operation exists for the scoped idempotency key.
    NewIntent,
    /// Matching original admission, including a legitimately absent capability.
    Existing {
        /// Original receipt; this does not grant new mutation authority.
        operation: OperationReceipt,
        /// Original server-prepared contact authority, when actually admitted.
        contact: Option<crate::source_contact_intake::PreparedSourceContact>,
    },
    /// Admission survives, but the historical prepared plan is unavailable.
    /// Return the receipt read-only; fresh processing needs a new authorized intent.
    ExistingContactPlanUnavailable {
        /// Exact original operation, never relabeled as a new admission.
        operation: OperationReceipt,
        /// Original capability classification, not a reconstructible effect plan.
        capability: AcceptedContactCapability,
    },
}

/// Private Rust capability descriptor rooted in atomic original source admission.
/// It is deliberately not serializable and contains no historical effect payload.
#[derive(Debug, Clone)]
pub struct AcceptedContactCapability {
    pub(crate) space_id: String,
    pub(crate) publication: elephant_types::publication::PublicationFence,
    pub(crate) digest: String,
}
impl AcceptedContactCapability {
    /// Named space of the original admission.
    #[must_use]
    pub fn space_id(&self) -> &str {
        &self.space_id
    }
    /// Exact acceptance instance; a revision alone is insufficient.
    #[must_use]
    pub const fn publication(&self) -> &elephant_types::publication::PublicationFence {
        &self.publication
    }
    /// Original server-sealed digest stored with the accepted operation.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Authorized source lifecycle request. No erased payload is required for replay.
#[derive(Debug, Clone)]
pub struct SourceMutation {
    /// Explicit owning space.
    pub space_id: String,
    /// Source incarnation to change.
    pub source_id: RecordId,
    /// Accepted-head comparison.
    pub expected: SourcePrecondition,
    /// Stable operation identity.
    pub idempotency_key: String,
    /// Canonical request fingerprint.
    pub request_fingerprint: String,
    /// Authorized actor for audit/outbox attribution.
    pub actor: String,
    /// Exact policy and lifecycle pre-state already authorized by the adapter.
    pub authorized: SourceAuthorizationSnapshot,
    /// Exact authorized document pre-state when lifecycle intent originates from a document.
    pub authorized_document: Option<elephant_types::records::Doc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceIdentity {
    source_id: RecordId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredOperation {
    pub receipt: OperationReceipt,
    pub request_fingerprint: String,
    pub input_fingerprint: String,
    pub authorization: SourceAuthorizationEnvelope,
    /// Presence distinguishes generic acceptance from a sealed contact intent.
    #[serde(default)]
    pub contact_digest: Option<String>,
    /// Original private producer, never inferred unrestricted after evidence loss.
    #[serde(default)]
    pub producer_origin: elephant_types::publication::SourceProducerOrigin,
    /// Exact acceptance before retraction, including later A -> B -> A instances.
    /// Lifecycle commits capture this reference atomically; missing means unknown.
    #[serde(default)]
    pub predecessor_publication: Option<elephant_types::publication::PublicationFence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRevision {
    revision: SourceRevision,
    operation_id: String,
    input_fingerprint: String,
}

/// Ephemeral result of the actual acceptance transaction, not operation state.
/// Adapters may enqueue fresh work only for a newly inserted acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceAcceptanceOutcome {
    /// This invocation atomically inserted the accepted operation and source head.
    Accepted(OperationReceipt),
    /// The same original semantic operation already existed; no write occurred.
    ReceiptOnly(OperationReceipt),
}

impl SourceAcceptanceOutcome {
    /// Extracts the original receipt without changing its state or identity.
    #[must_use]
    pub fn into_receipt(self) -> OperationReceipt {
        match self {
            Self::Accepted(receipt) | Self::ReceiptOnly(receipt) => receipt,
        }
    }
}

/// Source revision and operation repository; all mutations use a real transaction.
#[derive(Debug, Clone)]
pub struct SourcePublicationRepository {
    pub(crate) client: StorageClient,
}

impl SourcePublicationRepository {
    /// Creates a repository bound to a storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Reads the accepted head, including retracted/erased incarnations.
    pub async fn head(
        &self,
        space_id: &str,
        source_id: &RecordId,
    ) -> StorageResult<Option<SourceHead>> {
        self.read_control(HEAD_TABLE, &head_key(space_id, source_id), space_id)
            .await
    }

    /// Resolves an external identity to its current physical incarnation.
    /// The pointer stores no external identifier or source payload.
    pub async fn external_head(
        &self,
        space: &str,
        source_type: SourceType,
        external_id: &str,
    ) -> StorageResult<Option<SourceHead>> {
        let key = external_identity_key(space, source_type, external_id);
        let identity: Option<SourceIdentity> =
            self.read_control("source_identity", &key, space).await?;
        match identity {
            Some(identity) => self.head(space, &identity.source_id).await,
            None => Ok(None),
        }
    }

    /// Reads a payload-free public operation receipt in its explicit space.
    pub async fn operation(
        &self,
        space_id: &str,
        operation_id: &str,
    ) -> StorageResult<Option<OperationReceipt>> {
        Ok(self
            .read_control::<StoredOperation>(OPERATION_TABLE, operation_id, space_id)
            .await?
            .map(|row| row.receipt))
    }

    /// Looks up an accepted intent without exposing its key-derivation algorithm.
    /// Used by bounded reprocessing admission accounting and crash recovery.
    pub async fn operation_by_idempotency_key(
        &self,
        space: &str,
        key: &str,
    ) -> StorageResult<Option<OperationReceipt>> {
        if space.trim().is_empty() || key.trim().is_empty() {
            return Err(integrity("operation space and idempotency key required"));
        }
        self.operation(space, &operation_key(space, key)).await
    }

    /// Reads only the retained policy envelope for this physical incarnation.
    pub async fn authorization(
        &self,
        space_id: &str,
        source_id: &RecordId,
    ) -> StorageResult<Option<SourceAuthorizationEnvelope>> {
        self.read_control(
            "source_authorization",
            &head_key(space_id, source_id),
            space_id,
        )
        .await
    }

    /// Captures policy and lifecycle version for caller authorization before mutation.
    pub async fn authorization_snapshot(
        &self,
        space: &str,
        source_id: &RecordId,
    ) -> StorageResult<Option<SourceAuthorizationSnapshot>> {
        let source = crate::SourceRepository::new(self.client.clone())
            .get_with_deleted(space, source_id)
            .await?;
        if let Some(source) = source {
            return Ok(Some(SourceAuthorizationSnapshot::from(&source)));
        }
        Ok(self.authorization(space, source_id).await?.map(|envelope| {
            SourceAuthorizationSnapshot {
                envelope,
                source_updated_at: None,
                source_deleted_at: None,
            }
        }))
    }

    /// Reads the immutable operation policy envelope, including after erasure.
    pub async fn operation_authorization(
        &self,
        space_id: &str,
        operation_id: &str,
    ) -> StorageResult<Option<SourceAuthorizationEnvelope>> {
        Ok(self
            .read_control::<StoredOperation>(OPERATION_TABLE, operation_id, space_id)
            .await?
            .map(|row| row.authorization))
    }

    /// Reads an immutable accepted input. Callers must authorize its envelope.
    pub async fn revision(
        &self,
        space_id: &str,
        source_id: &RecordId,
        revision_id: &str,
    ) -> StorageResult<Option<SourceRevision>> {
        Ok(self
            .read_control::<StoredRevision>(
                REVISION_TABLE,
                &revision_key(space_id, source_id, revision_id),
                space_id,
            )
            .await?
            .map(|row| row.revision))
    }

    /// Atomically accepts immutable input, a desired head, and an operation.
    /// A pending newer desired head fences all work accepted under an older head.
    pub async fn accept(&self, request: AcceptSourceRevision) -> StorageResult<OperationReceipt> {
        self.accept_inner(request, None, None, None, None, None, None)
            .await
    }

    /// Accepts background reprocessing intent under its original queue attempt.
    ///
    /// A completed identical receipt may be read again; a new acceptance requires
    /// the same still-live claim inside the source transaction.
    pub async fn accept_with_attempt(
        &self,
        request: AcceptSourceRevision,
        attempt: &elephant_types::publication::AttemptFence,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(request, Some(attempt), None, None, None, None, None)
            .await
    }

    /// Trusted direct reprocessing uses the same captured configuration guard
    /// without manufacturing a queued attempt which does not exist.
    pub async fn accept_with_configuration(
        &self,
        request: AcceptSourceRevision,
        configuration: &elephant_types::provider_binding::SpaceConfigToken,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(request, None, Some(configuration), None, None, None, None)
            .await
    }

    /// Accepts deterministic reprocessing intent only while both its original
    /// attempt and captured processing configuration remain current.
    pub async fn accept_with_attempt_and_configuration(
        &self,
        request: AcceptSourceRevision,
        attempt: &elephant_types::publication::AttemptFence,
        configuration: &elephant_types::provider_binding::SpaceConfigToken,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(
            request,
            Some(attempt),
            Some(configuration),
            None,
            None,
            None,
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Preserve the existing acceptance interface separating source change, contact proposal, predecessor, producer, and standing authority."
    )]
    pub(crate) async fn accept_inner(
        &self,
        request: AcceptSourceRevision,
        attempt: Option<&elephant_types::publication::AttemptFence>,
        configuration: Option<&elephant_types::provider_binding::SpaceConfigToken>,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
        producer: Option<&crate::source_producer::PreparedSourceProducer>,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
        watch: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner_outcome(
            request,
            attempt,
            configuration,
            contact,
            producer,
            documents,
            watch,
        )
        .await
        .map(SourceAcceptanceOutcome::into_receipt)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Both backend branches atomically compare publication and producer guards before staging head, authorization, revisions, and outbox; preserve the single acceptance boundary."
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "Preserve the existing acceptance interface separating source change, contact proposal, predecessor, producer, and standing authority."
    )]
    pub(crate) async fn accept_inner_outcome(
        &self,
        mut request: AcceptSourceRevision,
        attempt: Option<&elephant_types::publication::AttemptFence>,
        configuration: Option<&elephant_types::provider_binding::SpaceConfigToken>,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
        producer: Option<&crate::source_producer::PreparedSourceProducer>,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
        watch: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
    ) -> StorageResult<SourceAcceptanceOutcome> {
        validate_request(&request)?;
        if watch.is_some() && (producer.is_some() || attempt.is_some()) {
            return Err(integrity(
                "source acceptance has multiple producer authorities",
            ));
        }
        let producer_origin = watch.map_or_else(
            || crate::source_producer::prepared_origin(producer, attempt),
            crate::source_watch_producer::PreparedSourceWatchProducer::origin,
        );
        if attempt.is_some_and(|attempt| attempt.space_id != request.source.base.space_id) {
            return Err(integrity("foreign-space source acceptance attempt"));
        }
        if configuration.is_some_and(|config| config.space_id != request.source.base.space_id) {
            return Err(integrity("foreign-space source processing configuration"));
        }
        let space_owned = request.source.base.space_id.clone();
        let space = &space_owned;
        let identity_key = request.source.external_id.as_ref().map_or_else(
            || stable_key(&[space, "internal", &request.source.base.id.to_string()]),
            |external_id| external_identity_key(space, request.source.type_, external_id),
        );
        let old_identity: Option<SourceIdentity> = self
            .read_control("source_identity", &identity_key, space)
            .await?;
        if let Some(identity) = &old_identity {
            request.source.base.id.clone_from(&identity.source_id);
        }
        let expected_source_id = request.source.base.id.clone();
        if let Some(documents) = documents {
            documents.validate_scope(space, &expected_source_id)?;
        }
        if let Some(document) = &request.authorized_document {
            if document.base.space_id != *space
                || document
                    .source
                    .as_ref()
                    .is_none_or(|source| source.id() != &expected_source_id)
            {
                return Err(integrity(
                    "authorized document does not belong to source incarnation",
                ));
            }
        }
        let operation_id = operation_key(space, &request.idempotency_key);
        let input_fingerprint = input_fingerprint(&request, &identity_key)?;
        let old_head = self.head(space, &expected_source_id).await?;
        let source_id = &expected_source_id;
        if let Some(old) = self
            .read_control::<StoredOperation>(OPERATION_TABLE, &operation_id, space)
            .await?
        {
            return replay_receipt(
                &old,
                &request.request_fingerprint,
                &input_fingerprint,
                contact.is_some(),
                &producer_origin,
            )
            .map(SourceAcceptanceOutcome::ReceiptOnly);
        }
        // A fresh idempotency key is a new acceptance intent, even when its
        // immutable content appeared in an older generation (A -> B -> A).
        // Reuse payload only; the current source CAS still governs every write.
        let revision_id = revision_key(space, source_id, &request.revision_id);
        let observed_revision = self
            .read_control::<StoredRevision>(REVISION_TABLE, &revision_id, space)
            .await?;
        if observed_revision
            .as_ref()
            .is_some_and(|old| old.input_fingerprint != input_fingerprint)
        {
            return Err(conflict("immutable_revision_conflict"));
        }
        check_precondition(old_head.as_ref(), &request.expected)?;
        let observed_authorization = self
            .authorization_snapshot(space, &expected_source_id)
            .await?;
        check_authorized_snapshot(observed_authorization.as_ref(), request.authorized.as_ref())?;
        if let Some(head) = &old_head {
            match head.lifecycle {
                SourceVisibility::Erased if request.recreate_after_erasure => {
                    request.source.base.id =
                        recreated_source_id(space, &expected_source_id, &operation_id)?;
                }
                SourceVisibility::Erased => return Err(conflict("source_erased")),
                SourceVisibility::Erasing => return Err(conflict("source_erasing")),
                SourceVisibility::Retracted if !request.reactivate => {
                    return Err(conflict("source_retracted"))
                }
                _ => {}
            }
        }
        if let Some(contact) = contact {
            contact.validate_source(&request.source)?;
            // A reused immutable revision remains the authority for later
            // finalization, restore and inverse projection. A caller cannot
            // seal a different bookkeeping snapshot and still publish this one.
            if let Some(retained) = observed_revision
                .as_ref()
                .filter(|retained| retained.revision.source_id == request.source.base.id)
            {
                contact.validate_source(&retained.revision.source)?;
            }
        }
        if let Some(producer) = producer {
            producer.validate_request(&request)?;
        }
        if let Some(watch) = watch {
            watch.validate_request(&request)?;
        }
        let source_id = &request.source.base.id;
        let observed_revision =
            observed_revision.filter(|old| old.revision.source_id == *source_id);
        let identity = SourceIdentity {
            source_id: source_id.clone(),
        };
        let revision_id = revision_key(space, source_id, &request.revision_id);
        let now = Utc::now();
        let head = SourceHead {
            space_id: space.clone(),
            source_id: source_id.clone(),
            generation: Uuid::new_v4().to_string(),
            desired_revision: Some(request.revision_id.clone()),
            published_revision: old_head
                .as_ref()
                .filter(|head| head.source_id == *source_id)
                .and_then(|head| head.published_revision.clone()),
            operation_id: operation_id.clone(),
            lifecycle: SourceVisibility::Active,
            updated_at: now,
        };
        let receipt = OperationReceipt {
            space_id: space.clone(),
            operation_id: operation_id.clone(),
            source_id: source_id.clone(),
            source_generation: head.generation.clone(),
            desired_revision: head.desired_revision.clone(),
            published_revision: head.published_revision.clone(),
            state: OperationState::Accepted,
            accepted_at: now,
            completed_at: None,
            error_code: None,
        };
        let operation = StoredOperation {
            receipt: receipt.clone(),
            request_fingerprint: request.request_fingerprint.clone(),
            input_fingerprint: input_fingerprint.clone(),
            authorization: SourceAuthorizationEnvelope::from(&request.source),
            contact_digest: contact.map(|contact| contact.digest().to_owned()),
            producer_origin: producer_origin.clone(),
            predecessor_publication: None,
        };
        let revision = StoredRevision {
            revision: SourceRevision {
                space_id: space.clone(),
                source_id: source_id.clone(),
                revision_id: request.revision_id.clone(),
                content_hash: request.source.hash.clone(),
                accepted_at: now,
                source: request.source.clone(),
            },
            operation_id: operation_id.clone(),
            input_fingerprint,
        };
        let accepted_contact = contact
            .map(|contact| {
                crate::source_contact_publication::AcceptedSourceContact::new(
                    &head,
                    contact.clone(),
                )
            })
            .transpose()?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    require_space(tx, space)?;
                    if let Some(old) = read_control_tx::<StoredOperation>(
                        tx,
                        OPERATION_TABLE,
                        &operation_id,
                        space,
                    )? {
                        return replay_receipt(
                            &old,
                            &operation.request_fingerprint,
                            &operation.input_fingerprint,
                            contact.is_some(),
                            &producer_origin,
                        )
                        .map(SourceAcceptanceOutcome::ReceiptOnly);
                    }
                    if let Some(documents) = documents {
                        documents.check_tx(tx)?;
                    }
                    if let Some(producer) = producer {
                        producer.check_tx(tx)?;
                    }
                    if let Some(contact) = contact {
                        crate::source_contact_publication::check_admission_tx(tx, contact)?;
                    }
                    if let Some(watch) = watch {
                        watch.check_tx(tx)?;
                    }
                    if let Some(attempt) = attempt {
                        crate::work_attempt_repository::check_attempt_fence(
                            tx,
                            attempt,
                            Utc::now(),
                        )?;
                    }
                    if let Some(configuration) = configuration {
                        check_accept_configuration_tx(tx, configuration)?;
                    }
                    let current: Option<SourceHead> = read_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, &expected_source_id),
                        space,
                    )?;
                    let current_identity: Option<SourceIdentity> =
                        read_control_tx(tx, "source_identity", &identity_key, space)?;
                    if current_identity.as_ref().map(|row| &row.source_id)
                        != old_identity.as_ref().map(|row| &row.source_id)
                    {
                        return Err(conflict("source_incarnation_changed"));
                    }
                    check_precondition(current.as_ref(), &request.expected)?;
                    let observed = authorization_snapshot_tx(tx, space, &expected_source_id)?;
                    check_authorized_snapshot(observed.as_ref(), request.authorized.as_ref())?;
                    if let Some(document) = &request.authorized_document {
                        if tx
                            .get::<elephant_types::records::Doc>(space, &document.base.id, true)?
                            .as_ref()
                            != Some(document)
                        {
                            return Err(conflict("authorized_document_changed"));
                        }
                    }
                    if current
                        .as_ref()
                        .is_some_and(|head| head.lifecycle == SourceVisibility::Erased)
                        && !request.recreate_after_erasure
                    {
                        return Err(conflict("source_erased"));
                    }
                    let current_revision =
                        read_control_tx::<StoredRevision>(tx, REVISION_TABLE, &revision_id, space)?;
                    if optional_payload(current_revision.as_ref())?
                        != optional_payload(observed_revision.as_ref())?
                    {
                        return Err(conflict("immutable_revision_conflict"));
                    }
                    let existing = tx.get_by_id::<Source>(source_id, true)?;
                    let source = normalize_accepted_source(existing.as_ref(), &request.source)?;
                    if existing.is_some() {
                        tx.replace(source)?;
                    } else {
                        tx.create(source)?;
                    }
                    if let Some(previous) = current.as_ref() {
                        supersede_tx(tx, previous)?;
                    }
                    write_control_tx(
                        tx,
                        "source_identity",
                        &identity_key,
                        space,
                        source_id,
                        &identity,
                    )?;
                    write_control_tx(
                        tx,
                        "source_authorization",
                        &head_key(space, source_id),
                        space,
                        source_id,
                        &operation.authorization,
                    )?;
                    write_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, source_id),
                        space,
                        source_id,
                        &head,
                    )?;
                    if observed_revision.is_none() {
                        write_control_tx(
                            tx,
                            REVISION_TABLE,
                            &revision_id,
                            space,
                            source_id,
                            &revision,
                        )?;
                    }
                    write_control_tx(
                        tx,
                        OPERATION_TABLE,
                        &operation_id,
                        space,
                        source_id,
                        &operation,
                    )?;
                    if let Some(contact) = &accepted_contact {
                        contact.write_tx(tx)?;
                    }
                    Ok(SourceAcceptanceOutcome::Accepted(receipt.clone()))
                })
                .await;
        }
        let existing = crate::SourceRepository::new(self.client.clone())
            .get_with_deleted(space, source_id)
            .await?;
        let source = normalize_accepted_source(existing.as_ref(), &request.source)?;
        let mut bindings = crate::bindings! {
            "space" => space.clone(), "source" => crate::surreal::thing_from_record_id(source_id),
            "source_record" => crate::surreal::source_to_value(&source)?,
            "head" => control_thing(HEAD_TABLE, &head_key(space, &expected_source_id)),
            "next_head" => control_thing(HEAD_TABLE, &head_key(space, source_id)),
            "identity" => control_thing("source_identity", &identity_key),
            "identity_record" => control_value(space, source_id, &identity)?,
            "old_identity_payload" => optional_payload(old_identity.as_ref())?,
            "revision" => control_thing(REVISION_TABLE, &revision_id),
            "operation" => control_thing(OPERATION_TABLE, &operation_id),
            "head_record" => control_value(space, source_id, &head)?,
            "authorization" => control_thing("source_authorization", &head_key(space, source_id)),
            "authorization_record" => control_value(space, source_id, &operation.authorization)?,
            "revision_record" => control_value(space, source_id, &revision)?,
            "observed_revision_payload" => optional_payload(observed_revision.as_ref())?,
            "operation_record" => control_value(space, source_id, &operation)?,
            "old_head_payload" => optional_payload(old_head.as_ref())?,
            "authorized_document" => request.authorized_document.as_ref().map_or(Value::None,|document|crate::surreal::thing_from_record_id(&document.base.id).into()),
            "authorized_document_record" => crate::canonical_commit::native_expected_fields(request.authorized_document.as_ref().map_or(Ok(Value::None),crate::surreal::doc_to_value)?),
            "authorized_source" => crate::surreal::thing_from_record_id(&expected_source_id),
            "authorized_security" => observed_authorization.as_ref().map_or(Value::None, |row| crate::surreal::stored_native_value(crate::surreal::security_to_value(&row.envelope.security))),
            "authorized_subjects" => observed_authorization.as_ref().map_or(Value::None, |row| Value::Array(crate::surreal::things_from_links(row.envelope.subjects.clone()))),
            "authorized_subjects_state" => observed_authorization.as_ref().map_or(Ok(Value::None), |row| crate::surreal::enum_to_surreal(&row.envelope.subjects_state))?,
            "authorized_source_updated" => observed_authorization.as_ref().and_then(|row| row.source_updated_at).map_or(Value::None, |value| crate::surreal::datetime_from_chrono(value).into()),
            "authorized_source_deleted" => observed_authorization.as_ref().and_then(|row| row.source_deleted_at).map_or(Value::None, |value| crate::surreal::datetime_from_chrono(value).into()),
            "authorized_envelope_id" => control_thing("source_authorization", &head_key(space, &expected_source_id)),
            "authorized_envelope_payload" => optional_payload(observed_authorization.as_ref().map(|row| &row.envelope))?,
            "old_source_updated" => existing.as_ref().map_or(Value::None, |source| crate::surreal::datetime_from_chrono(source.base.updated_at).into()),
        };
        let mut supersede = String::new();
        if let Some(previous) = old_head.as_ref() {
            let mut old: StoredOperation = self
                .read_control(OPERATION_TABLE, &previous.operation_id, space)
                .await?
                .ok_or_else(|| integrity("head has no operation"))?;
            let expected_previous_payload = encode_payload(&old)?;
            supersede_operation(&mut old);
            bindings.extend(crate::bindings! {
                "previous_operation" => control_thing(OPERATION_TABLE, &previous.operation_id),
                "previous_record" => control_value(space, &previous.source_id, &old)?,
                "previous_expected_payload" => expected_previous_payload,
            });
            supersede.push_str("IF (SELECT VALUE payload FROM ONLY $previous_operation) != $previous_expected_payload { THROW 'publication:source_operation_changed'; };\nUPDATE $previous_operation CONTENT $previous_record;\n");
        }
        let mut attempt_guard = String::new();
        if let Some(attempt) = attempt {
            crate::work_attempt_repository::append_attempt_fence(
                &mut attempt_guard,
                &mut bindings,
                attempt,
            )?;
        }
        if let Some(configuration) = configuration {
            append_accept_configuration_sql(&mut attempt_guard, &mut bindings, configuration)?;
        }
        let mut contact_guard = String::new();
        if let Some(documents) = documents {
            documents.append_guard_sql(&mut contact_guard, &mut bindings)?;
        }
        if let Some(producer) = producer {
            producer.append_guard_sql(&mut contact_guard, &mut bindings)?;
        }
        if let Some(contact) = contact {
            crate::source_contact_publication::append_admission_sql(
                &mut contact_guard,
                &mut bindings,
                contact,
            )?;
        }
        if let Some(watch) = watch {
            watch.append_guard_sql(&mut contact_guard, &mut bindings)?;
        }
        let mut contact_write = String::new();
        if let Some(contact) = &accepted_contact {
            contact.append_write_sql(&mut contact_write, &mut bindings)?;
        }
        let sql = format!(
            r"
BEGIN TRANSACTION;
{contact_guard}
{attempt_guard}
LET $spaces = SELECT id FROM space_config WHERE space_id = $space AND deleted_at IS NONE;
IF array::len($spaces) != 1 {{ THROW 'publication:unknown_space'; }};
UPDATE space_config SET updated_at = time::now() WHERE id = $spaces[0].id;
IF (SELECT VALUE payload FROM ONLY $head) != $old_head_payload {{ THROW 'publication:conflict_generation'; }};
IF (SELECT VALUE payload FROM ONLY $identity) != $old_identity_payload {{ THROW 'publication:source_incarnation_changed'; }};
IF (SELECT * FROM ONLY $operation) != NONE {{ THROW 'publication:idempotency_conflict'; }};
IF (SELECT VALUE payload FROM ONLY $revision) != $observed_revision_payload {{ THROW 'publication:immutable_revision_conflict'; }};
IF $authorized_document != NONE {{
    LET $document_current = SELECT * FROM ONLY $authorized_document;
    IF $document_current = NONE {{ THROW 'publication:authorized_document_changed'; }};
    FOR $entry IN object::entries($authorized_document_record) {{ IF $document_current[$entry[0]] != $entry[1] {{ THROW 'publication:authorized_document_changed'; }}; }};
}};
LET $authorized_current = SELECT * FROM ONLY $authorized_source;
IF $authorized_current != NONE AND ($authorized_current.space_id != $space OR $authorized_current.updated_at != $authorized_source_updated OR $authorized_current.deleted_at != $authorized_source_deleted OR $authorized_current.security != $authorized_security OR $authorized_current.subjects != $authorized_subjects OR $authorized_current.subjects_state != $authorized_subjects_state) {{ THROW 'publication:authorized_source_changed'; }};
IF $authorized_current = NONE AND $authorized_source_updated != NONE {{ THROW 'publication:authorized_source_changed'; }};
IF $authorized_current = NONE AND (SELECT VALUE payload FROM ONLY $authorized_envelope_id) != $authorized_envelope_payload {{ THROW 'publication:authorized_envelope_changed'; }};
LET $old_source = SELECT * FROM ONLY $source;
IF $old_source != NONE AND ($old_source.space_id != $space OR $old_source.updated_at != $old_source_updated) {{ THROW 'publication:source_changed'; }};
IF $old_source = NONE AND $old_source_updated != NONE {{ THROW 'publication:source_changed'; }};
UPSERT $source CONTENT $source_record;
{supersede}
UPSERT $identity CONTENT $identity_record;
UPSERT $next_head CONTENT $head_record;
UPSERT $authorization CONTENT $authorization_record;
IF $observed_revision_payload = NONE {{ CREATE $revision CONTENT $revision_record; }};
CREATE $operation CONTENT $operation_record;
{contact_write}
COMMIT TRANSACTION;"
        );
        if let Err(error) = self
            .client
            .query_bind_source_acceptance(&sql, bindings)
            .await
        {
            // A competing identical request may have committed first. Return
            // its receipt only after comparing immutable request identities.
            if let Some(old) = self
                .read_control::<StoredOperation>(OPERATION_TABLE, &operation_id, space)
                .await?
            {
                return replay_receipt(
                    &old,
                    &operation.request_fingerprint,
                    &operation.input_fingerprint,
                    contact.is_some(),
                    &producer_origin,
                )
                .map(SourceAcceptanceOutcome::ReceiptOnly);
            }
            return Err(publication_error(error));
        }
        Ok(SourceAcceptanceOutcome::Accepted(receipt))
    }

    pub(crate) async fn read_control<T: DeserializeOwned>(
        &self,
        table: &str,
        key: &str,
        space: &str,
    ) -> StorageResult<Option<T>> {
        self.read_control_payload(table, key, space)
            .await?
            .as_deref()
            .map(decode_payload)
            .transpose()
    }

    pub(crate) async fn read_control_payload(
        &self,
        table: &str,
        key: &str,
        space: &str,
    ) -> StorageResult<Option<String>> {
        if let Some(store) = self.client.embedded_store() {
            // Original authority inspection must not enter the managed writer
            // path or trigger a checkpoint merely to read one private payload.
            return crate::private_control_read::payload(&store, table, space, key).await;
        }
        let mut response = self
            .client
            .query_bind_checked(
                "SELECT payload FROM $row WHERE space_id = $space",
                crate::bindings! { "row" => control_thing(table, key), "space" => space },
            )
            .await?;
        let rows: Vec<ControlPayloadDb> = response
            .take(0)
            .map_err(|error| integrity(&error.to_string()))?;
        Ok(rows.into_iter().next().map(|row| row.payload))
    }
}

fn validate_request(request: &AcceptSourceRevision) -> StorageResult<()> {
    if request.source.base.id.table() != TableName::Source
        || request.source.base.space_id.trim().is_empty()
        || request.revision_id.trim().is_empty()
        || request.idempotency_key.trim().is_empty()
        || request.request_fingerprint.trim().is_empty()
        || request.source.base.deleted_at.is_some()
    {
        return Err(integrity("invalid source acceptance identity or lifecycle"));
    }
    if let Some(text) = request
        .source
        .metadata
        .get(elephant_types::records::source::SOURCE_MANUAL_DOC_TEMPLATE_KEY)
        .and_then(|doc| doc.get("text"))
        .and_then(serde_json::Value::as_str)
    {
        if text.len() > MAX_SOURCE_TEXT_BYTES {
            return Err(integrity("payload_too_large"));
        }
    }
    Ok(())
}

pub(crate) fn input_fingerprint(
    request: &AcceptSourceRevision,
    identity_key: &str,
) -> StorageResult<String> {
    let mut input =
        serde_json::to_value(&request.source).map_err(|error| integrity(&error.to_string()))?;
    if let Some(object) = input.as_object_mut() {
        for key in [
            "id",
            "created_at",
            "updated_at",
            "created_by",
            "lifecycle_status",
            "current_ingest_mode",
            "latest_ingest_decision",
        ] {
            object.remove(key);
        }
    }
    if let Some(metadata) = input
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.remove("observed_at");
        if let Some(doc_metadata) = metadata
            .get_mut("_manual_doc")
            .and_then(|doc| doc.get_mut("metadata"))
            .and_then(serde_json::Value::as_object_mut)
        {
            doc_metadata.remove("observed_at");
        }
    }
    Ok(stable_key(&[
        identity_key,
        &request.revision_id,
        &serde_json::to_string(&crate::canonical_commit::canonical_json(input))
            .map_err(|error| integrity(&error.to_string()))?,
    ]))
}

impl SourcePublicationRepository {
    /// Preview the exact physical source identity before contact authorization.
    ///
    /// A new erasure recreation uses a deterministic fresh incarnation for its
    /// scoped intent; the actual acceptance still checks the old head atomically.
    pub async fn acceptance_source(&self, request: &AcceptSourceRevision) -> StorageResult<Source> {
        validate_request(request)?;
        let space = &request.source.base.space_id;
        let identity_key = acceptance_identity_key(request);
        let operation_id = operation_key(space, &request.idempotency_key);
        let mut source = request.source.clone();
        if let Some(old) = self
            .read_control::<StoredOperation>(OPERATION_TABLE, &operation_id, space)
            .await?
        {
            replay_receipt(
                &old,
                &request.request_fingerprint,
                &input_fingerprint(request, &identity_key)?,
                old.contact_digest.is_some(),
                &old.producer_origin,
            )?;
            source.base.id = old.receipt.source_id;
            return self
                .retained_acceptance_source(request, source, &identity_key)
                .await;
        }
        if let Some(identity) = self
            .read_control::<SourceIdentity>("source_identity", &identity_key, space)
            .await?
        {
            source.base.id = identity.source_id;
        }
        if request.recreate_after_erasure
            && self
                .head(space, &source.base.id)
                .await?
                .is_some_and(|head| head.lifecycle == SourceVisibility::Erased)
        {
            source.base.id = recreated_source_id(space, &source.base.id, &operation_id)?;
        }
        self.retained_acceptance_source(request, source, &identity_key)
            .await
    }

    async fn retained_acceptance_source(
        &self,
        request: &AcceptSourceRevision,
        proposed: Source,
        identity_key: &str,
    ) -> StorageResult<Source> {
        let space = &proposed.base.space_id;
        let Some(retained) = self
            .read_control::<StoredRevision>(
                REVISION_TABLE,
                &revision_key(space, &proposed.base.id, &request.revision_id),
                space,
            )
            .await?
        else {
            return Ok(proposed);
        };
        if retained.input_fingerprint != input_fingerprint(request, identity_key)? {
            return Err(conflict("immutable_revision_conflict"));
        }
        if retained.revision.space_id != *space
            || retained.revision.source_id != proposed.base.id
            || retained.revision.revision_id != request.revision_id
            || retained.revision.source.base.id != proposed.base.id
            || retained.revision.source.base.space_id != *space
        {
            return Err(integrity("immutable source revision identity changed"));
        }
        Ok(retained.revision.source)
    }
}

pub(crate) fn acceptance_identity_key(request: &AcceptSourceRevision) -> String {
    let space = &request.source.base.space_id;
    request.source.external_id.as_ref().map_or_else(
        || stable_key(&[space, "internal", &request.source.base.id.to_string()]),
        |external| external_identity_key(space, request.source.type_, external),
    )
}

pub(crate) fn recreated_source_id(
    space: &str,
    previous: &RecordId,
    operation: &str,
) -> StorageResult<RecordId> {
    let key = stable_key(&[space, &previous.to_string(), operation, "source-recreation"]);
    let id = Uuid::parse_str(&key).map_err(|error| integrity(&error.to_string()))?;
    Ok(RecordId::new(TableName::Source, id))
}

fn normalize_accepted_source(
    existing: Option<&Source>,
    proposed: &Source,
) -> StorageResult<Source> {
    let mut source = proposed.clone();
    if let Some(existing) = existing {
        if existing.base.space_id != proposed.base.space_id
            || existing.external_id != proposed.external_id
            || existing.type_ != proposed.type_
        {
            return Err(conflict("source_envelope_changed"));
        }
        source.base.created_at = existing.base.created_at;
        source.base.created_by.clone_from(&existing.base.created_by);
    }
    source.base.updated_at = Utc::now();
    Ok(source)
}

pub(crate) fn stable_key(parts: &[&str]) -> String {
    // Length-delimited components prevent ambiguity between tuples.
    let mut encoded = String::new();
    for part in parts {
        let _ = write!(encoded, "{}:{part}", part.len());
    }
    Uuid::new_v5(&Uuid::NAMESPACE_OID, encoded.as_bytes()).to_string()
}

pub(crate) fn head_key(space: &str, source: &RecordId) -> String {
    stable_key(&[space, &source.to_string()])
}
pub(crate) fn external_identity_key(
    space: &str,
    source_type: SourceType,
    external_id: &str,
) -> String {
    stable_key(&[space, &format!("{source_type:?}"), external_id])
}

pub(crate) fn revision_key(space: &str, source: &RecordId, revision: &str) -> String {
    stable_key(&[space, &source.to_string(), revision])
}
pub(crate) fn operation_key(space: &str, key: &str) -> String {
    stable_key(&[space, key])
}
pub(crate) fn control_thing(table: &str, key: &str) -> Thing {
    Thing::from((table.to_owned(), key.to_owned()))
}

pub(crate) fn check_precondition(
    head: Option<&SourceHead>,
    expected: &SourcePrecondition,
) -> StorageResult<()> {
    match (head, expected) {
        (None, SourcePrecondition::Absent) => Ok(()),
        (Some(head), SourcePrecondition::Generation(generation))
            if head.generation == *generation =>
        {
            Ok(())
        }
        _ => Err(conflict("conflict_generation")),
    }
}

pub(crate) fn require_space(tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<()> {
    let count = tx
        .scan::<SpaceConfig>()?
        .iter()
        .filter(|config| config.base.space_id == space && config.base.deleted_at.is_none())
        .count();
    if count == 1 {
        Ok(())
    } else {
        Err(conflict("unknown_space"))
    }
}

pub(crate) fn read_control_tx<T: DeserializeOwned>(
    tx: &EmbeddedMutation<'_>,
    table: &str,
    key: &str,
    space: &str,
) -> StorageResult<Option<T>> {
    read_control_payload_tx(tx, table, key, space)?
        .as_deref()
        .map(decode_payload)
        .transpose()
}

pub(crate) fn read_control_payload_tx(
    tx: &EmbeddedMutation<'_>,
    table: &str,
    key: &str,
    space: &str,
) -> StorageResult<Option<String>> {
    let full_key = format!("{table}:{key}");
    tx.scan_extension_rows(table)?
        .into_iter()
        .find(|(row_key, row)| {
            row_key == &full_key
                && row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
        })
        .map(|(_, row)| {
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| integrity("missing control payload"))
                .map(str::to_owned)
        })
        .transpose()
}

pub(crate) fn write_control_tx<T: Serialize>(
    tx: &mut EmbeddedMutation<'_>,
    table: &str,
    key: &str,
    space: &str,
    source: &RecordId,
    value: &T,
) -> StorageResult<()> {
    let row = serde_json::json!({ "id": format!("{table}:{key}"), "space_id": space, "source_id": source.to_string(), "payload": encode_payload(value)?, "state": serde_json::to_value(value).map_err(|error| integrity(&error.to_string()))? });
    tx.upsert_extension_row(table, row)?;
    Ok(())
}

pub(crate) fn control_value<T: Serialize>(
    space: &str,
    source: &RecordId,
    value: &T,
) -> StorageResult<Value> {
    crate::surreal::json_to_surreal(
        serde_json::json!({ "space_id": space, "source_id": source.to_string(), "payload": encode_payload(value)?, "state": serde_json::to_value(value).map_err(|error| integrity(&error.to_string()))? }),
    )
}

pub(crate) fn encode_payload<T: Serialize>(value: &T) -> StorageResult<String> {
    serde_json::to_string(value).map_err(|error| integrity(&error.to_string()))
}
pub(crate) fn decode_payload<T: DeserializeOwned>(payload: &str) -> StorageResult<T> {
    serde_json::from_str(payload).map_err(|error| integrity(&error.to_string()))
}
fn optional_payload<T: Serialize>(value: Option<&T>) -> StorageResult<Value> {
    value.map_or(Ok(Value::None), |value| {
        encode_payload(value).map(Value::from)
    })
}

pub(crate) fn replay_receipt(
    old: &StoredOperation,
    fingerprint: &str,
    input: &str,
    has_contact: bool,
    producer: &elephant_types::publication::SourceProducerOrigin,
) -> StorageResult<OperationReceipt> {
    if old.request_fingerprint != fingerprint
        || old.input_fingerprint != input
        || old.contact_digest.is_some() != has_contact
        || !crate::source_producer::same_producer_intent(&old.producer_origin, producer)
    {
        return Err(conflict("idempotency_conflict"));
    }
    Ok(old.receipt.clone())
}

fn supersede_operation(old: &mut StoredOperation) {
    if matches!(
        old.receipt.state,
        OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
    ) {
        old.receipt.state = OperationState::Superseded;
        old.receipt.completed_at = Some(Utc::now());
    }
}

fn supersede_tx(tx: &mut EmbeddedMutation<'_>, head: &SourceHead) -> StorageResult<()> {
    let mut old: StoredOperation =
        read_control_tx(tx, OPERATION_TABLE, &head.operation_id, &head.space_id)?
            .ok_or_else(|| integrity("head has no operation"))?;
    supersede_operation(&mut old);
    write_control_tx(
        tx,
        OPERATION_TABLE,
        &head.operation_id,
        &head.space_id,
        &head.source_id,
        &old,
    )
}

pub(crate) fn conflict(code: &str) -> StorageError {
    StorageError::Conflict(code.to_owned())
}
pub(crate) fn integrity(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.to_owned())
}
pub(crate) fn publication_error(error: StorageError) -> StorageError {
    // The shared attempt fence predates the publication namespace. Match its
    // exact native refusal, never incidental text in another kind of error.
    let stale_attempt = match &error {
        StorageError::QueryError(message) => message == "An error occurred: stale_attempt",
        StorageError::Surreal(error) => match error.as_ref() {
            surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)) => {
                message == "stale_attempt"
            }
            surrealdb::Error::Api(surrealdb::error::Api::Query(message)) => {
                message == "An error occurred: stale_attempt"
            }
            _ => false,
        },
        _ => false,
    };
    if stale_attempt {
        return conflict("stale_attempt");
    }
    let message = error.to_string();
    if let Some((_, code)) = message.split_once("publication:") {
        conflict(code)
    } else {
        error
    }
}

impl SourcePublicationRepository {
    /// Reads provenance support without creating a second visibility authority.
    pub async fn support_for_record(
        &self,
        space: &str,
        record_id: &RecordId,
    ) -> StorageResult<Vec<crate::canonical_commit::SourceEffectSupport>> {
        let rows: Vec<crate::canonical_commit::SourceEffectSupport> = self
            .list_controls("source_effect_support", Some(space))
            .await?;
        Ok(rows
            .into_iter()
            .filter(|row| row.record_id == *record_id)
            .collect())
    }

    /// Trusted all-space recovery discovery, including accepted-but-never-enqueued work.
    pub async fn recoverable_operations(
        &self,
        limit: usize,
    ) -> StorageResult<Vec<OperationReceipt>> {
        self.recoverable_operations_after(limit, None).await
    }

    /// Cursor-based all-space recovery; each operation is visited once per sweep.
    pub async fn recoverable_operations_after(
        &self,
        limit: usize,
        after: Option<&str>,
    ) -> StorageResult<Vec<OperationReceipt>> {
        let operations: Vec<StoredOperation> = self.list_controls(OPERATION_TABLE, None).await?;
        let mut receipts = operations
            .into_iter()
            .map(|operation| operation.receipt)
            .filter(|receipt| {
                after.is_none_or(|after| receipt.operation_id.as_str() > after)
                    && matches!(
                        receipt.state,
                        OperationState::Accepted
                            | OperationState::Processing
                            | OperationState::FailedRetryable
                            | OperationState::Erasing
                    )
            })
            .collect::<Vec<_>>();
        receipts.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        receipts.truncate(limit);
        Ok(receipts)
    }

    /// Rotates preparation fences after restore before ordinary readiness resumes.
    pub async fn rotate_canonical_epochs(&self) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let mut spaces = tx
                        .scan::<SpaceConfig>()?
                        .into_iter()
                        .map(|config| config.base.space_id)
                        .collect::<std::collections::BTreeSet<_>>();
                    for (_, row) in tx.scan_extension_rows("canonical_epoch")? {
                        if let Some(space) = row.get("space_id").and_then(serde_json::Value::as_str)
                        {
                            spaces.insert(space.to_owned());
                        }
                    }
                    for space in spaces {
                        tx.advance_canonical_epoch(&space)?;
                    }
                    Ok(())
                })
                .await;
        }
        self.client.query_checked("BEGIN TRANSACTION; UPDATE canonical_epoch SET epoch = <string>rand::uuid(); FOR $config IN (SELECT space_id FROM space_config) { UPSERT type::thing('canonical_epoch', $config.space_id) SET space_id = $config.space_id, epoch = <string>rand::uuid(); }; COMMIT TRANSACTION;").await?;
        Ok(())
    }

    pub(crate) async fn list_controls<T: DeserializeOwned>(
        &self,
        table: &str,
        space: Option<&str>,
    ) -> StorageResult<Vec<T>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    tx.scan_extension_rows(table)?
                        .into_iter()
                        .filter(|(_, row)| {
                            space.is_none_or(|space| {
                                row.get("space_id").and_then(serde_json::Value::as_str)
                                    == Some(space)
                            })
                        })
                        .map(|(_, row)| {
                            row.get("payload")
                                .and_then(serde_json::Value::as_str)
                                .ok_or_else(|| integrity("missing control payload"))
                                .and_then(decode_payload)
                        })
                        .collect()
                })
                .await;
        }
        let mut response = self.client.query_bind_checked(
            "SELECT payload FROM type::table($table) WHERE $all_spaces OR space_id = $space",
            crate::bindings! { "table" => table, "all_spaces" => space.is_none(), "space" => space.map_or(Value::None, Value::from) },
        ).await?;
        let rows: Vec<ControlPayloadDb> = response
            .take(0)
            .map_err(|error| integrity(&error.to_string()))?;
        rows.into_iter()
            .map(|row| decode_payload(&row.payload))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::{Repository, SpaceConfigRepository, StorageConfig};
    use elephant_types::records::base::BaseFields;

    #[test]
    fn exact_native_attempt_refusal_maps_to_publication_conflict() {
        let native = || surrealdb::Error::Db(surrealdb::error::Db::Thrown("stale_attempt".into()));
        // The checked-query adapter preserves this exact native display text.
        assert_eq!(native().to_string(), "An error occurred: stale_attempt");
        for error in [
            StorageError::classify_query_error(native().to_string()),
            StorageError::Surreal(Box::new(native())),
            StorageError::Surreal(Box::new(surrealdb::Error::Api(
                surrealdb::error::Api::Query("An error occurred: stale_attempt".into()),
            ))),
        ] {
            assert!(
                matches!(publication_error(error), StorageError::Conflict(code) if code == "stale_attempt")
            );
        }
        let embedded = conflict("stale_attempt: publication ownership expired or changed");
        let original = format!("{embedded:?}");
        assert_eq!(format!("{:?}", publication_error(embedded)), original);
    }

    #[test]
    fn attempt_words_in_other_errors_do_not_gain_conflict_semantics() {
        for message in [
            "stale_attempt",
            "query text mentions stale_attempt",
            "An error occurred: stale_attempt trailing data",
            "An error occurred: stale_attempt\n",
            "An error occurred: stale_attempt ",
        ] {
            let error = StorageError::QueryError(message.into());
            let original = format!("{error:?}");
            assert_eq!(format!("{:?}", publication_error(error)), original);
        }
        for error in [
            StorageError::TransactionError("An error occurred: stale_attempt".into()),
            StorageError::NotFound("An error occurred: stale_attempt".into()),
            StorageError::Surreal(Box::new(surrealdb::Error::Db(
                surrealdb::error::Db::Thrown("stale_attempt trailing data".into()),
            ))),
            StorageError::Surreal(Box::new(surrealdb::Error::Api(
                surrealdb::error::Api::Query("stale_attempt".into()),
            ))),
        ] {
            let original = format!("{error:?}");
            assert_eq!(format!("{:?}", publication_error(error)), original);
        }
    }

    pub(super) async fn client(embedded: bool) -> StorageClient {
        let client = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new(StorageConfig::default()).await
        }
        .expect("test backend");
        if !embedded {
            client
                .query_checked(include_str!(
                    "../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .expect("publication schema");
        }
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id": RecordId::new_v4(TableName::SpaceConfig), "space_id":"business-a",
            "created_at": Utc::now(), "updated_at": Utc::now(), "created_by":"test", "deleted_at": null,
            "security": {"policy_id":"default", "level":0, "labels":[], "handling":[]},
            "provenance": {}, "subjects":[], "subjects_state":"complete",
            "embedding_provider":"fake", "embedding_model":"test", "embedding_dims":8
        })).expect("config fixture");
        SpaceConfigRepository::new(client.clone())
            .create(config)
            .await
            .expect("create named space");
        client
    }

    pub(super) fn request(
        id: &RecordId,
        revision: &str,
        expected: SourcePrecondition,
    ) -> AcceptSourceRevision {
        let mut source = Source::new(
            BaseFields::new(id.clone(), "business-a".into(), "test".into()),
            SourceType::Manual,
            format!("hash-{revision}"),
        );
        source.external_id = Some("upstream-record".into());
        source.metadata = serde_json::json!({"_manual_doc":{"text":format!("complete source {revision}")}, "observed_at":"first observation"});
        AcceptSourceRevision {
            source,
            revision_id: revision.into(),
            expected,
            idempotency_key: format!("key-{revision}"),
            request_fingerprint: format!("fingerprint-{revision}"),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn acceptance_contract(embedded: bool) {
        let client = client(embedded).await;
        let repo = SourcePublicationRepository::new(client);
        let id = RecordId::new_v4(TableName::Source);
        let initial = request(&id, "r1", SourcePrecondition::Absent);
        let receipt = repo
            .accept(initial.clone())
            .await
            .expect("initial acceptance");
        assert_eq!(receipt.state, OperationState::Accepted);
        assert_eq!(receipt.desired_revision.as_deref(), Some("r1"));
        assert_eq!(receipt.published_revision, None);
        let mut observed_again = initial.clone();
        observed_again.source.metadata["observed_at"] = serde_json::json!("later observation");
        assert_eq!(
            repo.accept(observed_again).await.expect("poll retry"),
            receipt
        );
        assert_eq!(
            repo.revision("business-a", &id, "r1")
                .await
                .expect("revision")
                .expect("present")
                .source
                .metadata["observed_at"],
            "first observation"
        );
        let mut changed = initial;
        changed.source.metadata["_manual_doc"]["text"] =
            serde_json::json!("different confidential content");
        assert!(
            repo.accept(changed).await.is_err(),
            "caller fingerprint cannot conceal changed bytes"
        );
        let mut next = request(
            &id,
            "r2",
            SourcePrecondition::Generation(receipt.source_generation.clone()),
        );
        next.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("authorized pre-state");
        let next_receipt = repo.accept(next).await.expect("replacement accepted");
        assert_ne!(next_receipt.source_generation, receipt.source_generation);
        assert_eq!(
            repo.operation("business-a", &receipt.operation_id)
                .await
                .expect("old operation")
                .expect("present")
                .state,
            OperationState::Superseded
        );
        assert!(repo
            .accept(request(
                &id,
                "delayed-r3",
                SourcePrecondition::Generation(receipt.source_generation)
            ))
            .await
            .is_err());
        let head = repo
            .external_head("business-a", SourceType::Manual, "upstream-record")
            .await
            .expect("external identity")
            .expect("head");
        assert_eq!(head.operation_id, next_receipt.operation_id);
        assert_eq!(
            head.published_revision, None,
            "acceptance is never publication"
        );
        assert!(repo
            .head("wrong-space", &id)
            .await
            .expect("foreign lookup")
            .is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_acceptance_has_immutable_input_and_desired_head_cas() {
        acceptance_contract(true).await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_acceptance_has_immutable_input_and_desired_head_cas() {
        acceptance_contract(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn competing_replacements(embedded: bool) {
        let repo = SourcePublicationRepository::new(client(embedded).await);
        let id = RecordId::new_v4(TableName::Source);
        let initial = repo
            .accept(request(&id, "r1", SourcePrecondition::Absent))
            .await
            .expect("accept r1");
        let mut left = request(
            &id,
            "r2-left",
            SourcePrecondition::Generation(initial.source_generation.clone()),
        );
        let mut right = request(
            &id,
            "r2-right",
            SourcePrecondition::Generation(initial.source_generation),
        );
        left.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("pre-state");
        right.authorized = left.authorized.clone();
        let (left, right) = tokio::join!(repo.accept(left), repo.accept(right));
        assert_ne!(
            left.is_ok(),
            right.is_ok(),
            "exactly one desired-head CAS wins"
        );
        let receipt = left.or(right).expect("winner");
        assert_eq!(
            repo.head("business-a", &id)
                .await
                .expect("head")
                .expect("present")
                .operation_id,
            receipt.operation_id
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_concurrent_replacements_have_one_winner() {
        competing_replacements(true).await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_concurrent_replacements_have_one_winner() {
        competing_replacements(false).await;
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn authorized_reclassification(embedded: bool) {
        let client = client(embedded).await;
        let repo = SourcePublicationRepository::new(client.clone());
        let id = RecordId::new_v4(TableName::Source);
        let accepted = repo
            .accept(request(&id, "r1", SourcePrecondition::Absent))
            .await
            .expect("initial");
        let mut replacement = request(
            &id,
            "r2",
            SourcePrecondition::Generation(accepted.source_generation),
        );
        replacement.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("observed authorized source");
        replacement.source.base.security.level = 3;
        let accepted = repo
            .accept(replacement)
            .await
            .expect("explicit new revision may reclassify");
        assert_eq!(
            repo.authorization("business-a", &id)
                .await
                .expect("policy")
                .expect("present")
                .security
                .level,
            3
        );
        let mut stale = request(
            &id,
            "r3",
            SourcePrecondition::Generation(accepted.source_generation),
        );
        stale.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("pre-state");
        let repository = crate::SourceRepository::new(client);
        let mut changed = repository
            .get("business-a", &id)
            .await
            .expect("source")
            .expect("present");
        changed.base.security.level = 4;
        repository
            .update("business-a", &id, changed)
            .await
            .expect("competing policy change");
        assert!(
            repo.accept(stale).await.is_err(),
            "head CAS alone must not bypass a later policy change"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_new_revision_reclassification_checks_authorized_prestate() {
        authorized_reclassification(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_new_revision_reclassification_checks_authorized_prestate() {
        authorized_reclassification(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete fault and recovery sequence together with its original assertions."
    )]
    async fn execution_state_requires_original_authority(embedded: bool) {
        use elephant_types::publication::PublicationFence;
        use elephant_types::records::{WorkItem, WorkItemStatus};
        let client = client(embedded).await;
        if !embedded {
            client
                .query_checked(include_str!("../../migrations/sql/032_work_recovery.surql"))
                .await
                .expect("attempt schema");
        }
        let config_repo = SpaceConfigRepository::new(client.clone());
        let observed = config_repo
            .get_by_space_id("business-a")
            .await
            .expect("config")
            .expect("present");
        let mut configured = observed.clone();
        configured.processing = Some(elephant_types::provider_binding::SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "reprocess-policy".into(),
            connections: std::collections::BTreeMap::default(),
            profiles: std::collections::BTreeMap::default(),
            bindings: std::collections::BTreeMap::default(),
            active_embedding_profile_id: None,
            pending_reindex: None,
        });
        let configured = config_repo
            .replace_authorized(&observed, configured)
            .await
            .expect("processing config");
        let mut configuration = elephant_types::provider_binding::SpaceConfigToken {
            space_id: "business-a".into(),
            config_id: configured.base.id.to_string(),
            generation: 1,
            policy_id: "reprocess-policy".into(),
        };
        let repo = SourcePublicationRepository::new(client.clone());
        let id = RecordId::new_v4(TableName::Source);
        let accepted = repo
            .accept(request(&id, "r1", SourcePrecondition::Absent))
            .await
            .expect("accept");
        let publication = PublicationFence {
            source_id: id.clone(),
            generation: accepted.source_generation.clone(),
            revision_id: "r1".into(),
            operation_id: accepted.operation_id.clone(),
        };
        let work = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "business-a".into(),
                "test".into(),
            ),
            kind: "render_doc".into(),
            payload: serde_json::json!({"_publication":publication}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "execution-test".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let work_repo = crate::WorkItemRepository::new(client);
        let work = work_repo.create(work).await.expect("queue");
        let claimed = work_repo
            .claim_with_lease(
                "business-a",
                &work.base.id,
                "worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("claimed");
        let attempt =
            crate::work_attempt_repository::fence_from_claim(&claimed).expect("captured attempt");
        repo.record_execution_state(
            "business-a",
            &publication,
            &attempt,
            OperationState::Processing,
            None,
        )
        .await
        .expect("processing");
        repo.record_execution_state(
            "business-a",
            &publication,
            &attempt,
            OperationState::FailedRetryable,
            Some("provider_unavailable"),
        )
        .await
        .expect("bounded failure");
        assert!(
            repo.record_execution_state(
                "business-a",
                &publication,
                &attempt,
                OperationState::Completed,
                None
            )
            .await
            .is_err(),
            "stage cannot claim source completion"
        );
        assert_eq!(
            repo.operation("business-a", &accepted.operation_id)
                .await
                .expect("operation")
                .expect("present")
                .state,
            OperationState::FailedRetryable
        );
        let mut replacement = request(
            &id,
            "r2",
            SourcePrecondition::Generation(accepted.source_generation),
        );
        replacement.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("authorize old");
        let mut stale_attempt = attempt.clone();
        stale_attempt.lease_token = "abandoned-attempt-token".into();
        assert!(
            repo.accept_with_attempt(replacement.clone(), &stale_attempt)
                .await
                .is_err(),
            "acceptance must compare original queue claim inside transaction"
        );
        assert_eq!(
            repo.head("business-a", &id)
                .await
                .expect("head")
                .expect("present")
                .desired_revision
                .as_deref(),
            Some("r1")
        );
        let observed = config_repo
            .get_by_space_id("business-a")
            .await
            .expect("current config")
            .expect("present");
        let mut changed = observed.clone();
        changed.processing.as_mut().expect("processing").generation = 2;
        config_repo
            .replace_authorized(&observed, changed)
            .await
            .expect("legitimate config edit");
        assert!(
            repo.accept_with_attempt_and_configuration(
                replacement.clone(),
                &attempt,
                &configuration
            )
            .await
            .is_err(),
            "accepted revision must not adopt changed processing configuration"
        );
        configuration.generation = 2;
        let replacement_receipt = repo
            .accept_with_attempt_and_configuration(replacement, &attempt, &configuration)
            .await
            .expect("new source head under original claim and current captured configuration");
        let mut revert = request(
            &id,
            "r1",
            SourcePrecondition::Generation(replacement_receipt.source_generation),
        );
        revert.idempotency_key = "fresh-aba-r1".into();
        revert.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("authorize reacceptance");
        let reaccepted = repo
            .accept_with_attempt_and_configuration(revert, &attempt, &configuration)
            .await
            .expect("same revision under new acceptance instance");
        assert_eq!(
            reaccepted.desired_revision.as_deref(),
            Some(publication.revision_id.as_str())
        );
        assert_ne!(reaccepted.source_generation, publication.generation);
        assert!(
            repo.record_execution_state(
                "business-a",
                &publication,
                &attempt,
                OperationState::FailedTerminal,
                Some("late_failure")
            )
            .await
            .is_err(),
            "old work cannot update a new operation even after the same revision is reaccepted"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_execution_status_is_source_and_attempt_fenced() {
        execution_state_requires_original_authority(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_execution_status_is_source_and_attempt_fenced() {
        execution_state_requires_original_authority(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete fault and recovery sequence together with its original assertions."
    )]
    async fn immutable_revision_can_be_reaccepted_only_as_a_fresh_cas_intent(embedded: bool) {
        let client = client(embedded).await;
        let repo = SourcePublicationRepository::new(client);
        let id = RecordId::new_v4(TableName::Source);
        let original = request(&id, "content-a", SourcePrecondition::Absent);
        let first = repo.accept(original.clone()).await.expect("first A");
        let immutable = encode_payload(
            &repo
                .revision("business-a", &id, "content-a")
                .await
                .expect("revision")
                .expect("original immutable A"),
        )
        .expect("encode");
        let mut b = request(
            &id,
            "content-b",
            SourcePrecondition::Generation(first.source_generation.clone()),
        );
        b.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("authorized A");
        let second = repo.accept(b).await.expect("B");
        let historical = repo
            .accept(original)
            .await
            .expect("original key is only a replay");
        assert_eq!(historical.operation_id, first.operation_id);
        assert_eq!(historical.state, OperationState::Superseded);
        assert_eq!(
            repo.head("business-a", &id)
                .await
                .expect("head")
                .expect("B still current")
                .generation,
            second.source_generation
        );
        let mut revert = request(
            &id,
            "content-a",
            SourcePrecondition::Generation(second.source_generation.clone()),
        );
        revert.idempotency_key = "fresh-explicit-revert-a".into();
        revert.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("authorized B");
        let (left, right) = tokio::join!(repo.accept(revert.clone()), repo.accept(revert.clone()));
        let restored = left.expect("one revert intent");
        assert_eq!(restored, right.expect("concurrent same-key retry"));
        assert_ne!(restored.operation_id, first.operation_id);
        assert_ne!(restored.source_generation, first.source_generation);
        assert_ne!(restored.source_generation, second.source_generation);
        assert_eq!(restored.desired_revision.as_deref(), Some("content-a"));
        assert_eq!(
            encode_payload(
                &repo
                    .revision("business-a", &id, "content-a")
                    .await
                    .expect("revision")
                    .expect("immutable reused")
            )
            .expect("encode"),
            immutable,
            "first immutable snapshot must not be rewritten by acceptance instances"
        );
        let mut stale = revert.clone();
        stale.idempotency_key = "another-intent-stale-cas".into();
        stale.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("current authorization");
        assert!(repo
            .accept(stale)
            .await
            .expect_err("stale source CAS")
            .to_string()
            .contains("conflict_generation"));
        let mut altered = revert.clone();
        altered.idempotency_key = "altered-immutable-a".into();
        altered.expected = SourcePrecondition::Generation(restored.source_generation.clone());
        altered.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("current authorization");
        altered.source.metadata["semantic_label"] = serde_json::json!("changed");
        assert!(
            repo.accept(altered).await.is_err(),
            "fresh intent cannot rebind immutable content/security/metadata"
        );
        let mut competing = revert;
        competing.expected = SourcePrecondition::Generation(restored.source_generation.clone());
        competing.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .expect("current authorization");
        competing.idempotency_key = "explicit-reaccept-one".into();
        let mut other = competing.clone();
        other.idempotency_key = "explicit-reaccept-two".into();
        let (left, right) = tokio::join!(repo.accept(competing), repo.accept(other));
        assert_ne!(
            left.is_ok(),
            right.is_ok(),
            "distinct intents cannot both accept the same current generation"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_aba_revision_reuse_is_a_fresh_explicit_cas_acceptance() {
        immutable_revision_can_be_reaccepted_only_as_a_fresh_cas_intent(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_aba_revision_reuse_is_a_fresh_explicit_cas_acceptance() {
        immutable_revision_can_be_reaccepted_only_as_a_fresh_cas_intent(false).await;
    }

    #[test]
    fn idempotency_fingerprint_binds_internal_identity_without_observation_time() {
        let id = RecordId::new_v4(TableName::Source);
        let mut request = request(&id, "r1", SourcePrecondition::Absent);
        request.source.external_id = None;
        let first = input_fingerprint(&request, "logical-first").expect("fingerprint");
        request.source.metadata["observed_at"] = serde_json::json!("later");
        assert_eq!(
            first,
            input_fingerprint(&request, "logical-first").expect("same payload")
        );
        assert_ne!(
            first,
            input_fingerprint(&request, "logical-second").expect("distinct identity")
        );
        request.source.base.security.level = 3;
        assert_ne!(
            first,
            input_fingerprint(&request, "logical-first").expect("changed security")
        );
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn original_operation_identity(embedded: bool) {
        use elephant_types::publication::{PublicationFence, SourceProducerOrigin};

        let client = client(embedded).await;
        let repo = SourcePublicationRepository::new(client);
        let id = RecordId::new_v4(TableName::Source);
        let original = repo
            .accept(request(
                &id,
                "original-generation",
                SourcePrecondition::Absent,
            ))
            .await
            .unwrap();
        let publication = PublicationFence {
            source_id: id.clone(),
            revision_id: "original-generation".into(),
            generation: original.source_generation.clone(),
            operation_id: original.operation_id.clone(),
        };
        let mut next = request(
            &id,
            "next-generation",
            SourcePrecondition::Generation(original.source_generation.clone()),
        );
        next.authorized = repo
            .authorization_snapshot("business-a", &id)
            .await
            .unwrap();
        let current = repo.accept(next).await.unwrap();
        let historical = repo
            .operation("business-a", &original.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(historical.operation_id, original.operation_id);
        assert_eq!(historical.source_generation, original.source_generation);
        assert_eq!(historical.state, OperationState::Superseded);
        assert_ne!(historical.operation_id, current.operation_id);
        assert_eq!(
            repo.source_operation_producer("business-a", &publication)
                .await
                .unwrap(),
            SourceProducerOrigin::Direct
        );
        assert!(repo
            .operation("business-b", &original.operation_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            repo.source_operation_producer("business-b", &publication)
                .await
                .unwrap(),
            SourceProducerOrigin::UnknownOriginal
        );
        for changed in [
            PublicationFence {
                source_id: RecordId::new_v4(TableName::Source),
                ..publication.clone()
            },
            PublicationFence {
                generation: current.source_generation.clone(),
                ..publication.clone()
            },
            PublicationFence {
                revision_id: "next-generation".into(),
                ..publication.clone()
            },
        ] {
            assert!(repo
                .source_operation_producer("business-a", &changed)
                .await
                .is_err());
        }
        assert_eq!(
            repo.source_operation_producer(
                "business-a",
                &PublicationFence {
                    operation_id: "unknown-original-operation".into(),
                    ..publication
                },
            )
            .await
            .unwrap(),
            SourceProducerOrigin::UnknownOriginal
        );
        assert_eq!(
            repo.head("business-a", &id)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            current.operation_id
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_original_operation_identity_never_adopts_current_head() {
        original_operation_identity(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_original_operation_identity_never_adopts_current_head() {
        original_operation_identity(false).await;
    }

    #[test]
    fn retained_authorization_contains_no_source_payload() {
        let id = RecordId::new_v4(TableName::Source);
        let request = request(&id, "r1", SourcePrecondition::Absent);
        let envelope = SourceAuthorizationEnvelope::from(&request.source);
        let encoded = serde_json::to_value(&envelope).expect("envelope");
        assert_eq!(envelope.source_id, id);
        for field in [
            "metadata",
            "external_id",
            "hash",
            "text",
            "title",
            "observed_at",
        ] {
            assert!(
                encoded.get(field).is_none(),
                "payload field {field} was retained"
            );
        }
        assert_eq!(envelope.security, request.source.base.security);
    }
}

pub(crate) fn check_authorized_snapshot(
    observed: Option<&SourceAuthorizationSnapshot>,
    authorized: Option<&SourceAuthorizationSnapshot>,
) -> StorageResult<()> {
    if observed != authorized {
        return Err(conflict("authorized_source_changed"));
    }
    Ok(())
}

pub(crate) fn authorization_snapshot_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    source_id: &RecordId,
) -> StorageResult<Option<SourceAuthorizationSnapshot>> {
    if let Some(source) = tx.get::<Source>(space, source_id, true)? {
        return Ok(Some(SourceAuthorizationSnapshot::from(&source)));
    }
    let envelope: Option<SourceAuthorizationEnvelope> = read_control_tx(
        tx,
        "source_authorization",
        &head_key(space, source_id),
        space,
    )?;
    Ok(envelope.map(|envelope| SourceAuthorizationSnapshot {
        envelope,
        source_updated_at: None,
        source_deleted_at: None,
    }))
}

impl SourcePublicationRepository {
    /// Records worker progress/failure under its original source and attempt authority.
    /// This method cannot certify completion or change a terminal/lifecycle outcome.
    pub async fn record_execution_state(
        &self,
        space: &str,
        publication: &elephant_types::publication::PublicationFence,
        attempt: &elephant_types::publication::AttemptFence,
        state: OperationState,
        error_code: Option<&str>,
    ) -> StorageResult<()> {
        self.record_execution_state_with(space, publication, attempt, state, error_code, |_| {
            std::future::ready(Ok(()))
        })
        .await
    }

    // The private pre-commit callback makes the read/commit race deterministic in
    // storage tests. Production supplies an immediately ready no-op; it cannot
    // introduce a provider call or change the captured publication/attempt.
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the finite transaction loop beside its original-payload and attempt guards"
    )]
    async fn record_execution_state_with<F, Fut>(
        &self,
        space: &str,
        publication: &elephant_types::publication::PublicationFence,
        attempt: &elephant_types::publication::AttemptFence,
        state: OperationState,
        error_code: Option<&str>,
        mut before_commit: F,
    ) -> StorageResult<()>
    where
        F: FnMut(usize) -> Fut,
        Fut: std::future::Future<Output = StorageResult<()>>,
    {
        if attempt.space_id != space
            || !matches!(
                state,
                OperationState::Processing
                    | OperationState::FailedRetryable
                    | OperationState::FailedTerminal
            )
        {
            return Err(integrity("invalid source execution transition"));
        }
        if error_code.is_some_and(|code| {
            code.len() > 96
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        }) {
            return Err(integrity(
                "execution error must be a bounded payload-free code",
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            before_commit(0).await?;
            return store
                .mutate(|tx| {
                    require_space(tx, space)?;
                    crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
                    let work = tx
                        .get::<elephant_types::records::WorkItem>(
                            space,
                            &attempt.work_item_id,
                            false,
                        )?
                        .ok_or_else(|| conflict("stale_attempt"))?;
                    if work.payload.get("_publication")
                        != Some(
                            &serde_json::to_value(publication)
                                .map_err(|error| integrity(&error.to_string()))?,
                        )
                    {
                        return Err(conflict("attempt_source_mismatch"));
                    }
                    let head: SourceHead = read_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, &publication.source_id),
                        space,
                    )?
                    .ok_or_else(|| conflict("source_head_missing"))?;
                    let payload = read_control_payload_tx(
                        tx,
                        OPERATION_TABLE,
                        &publication.operation_id,
                        space,
                    )?
                    .ok_or_else(|| integrity("source operation missing"))?;
                    let operation = execution_progress_payload(
                        &head,
                        &payload,
                        publication,
                        state,
                        error_code,
                    )?;
                    write_control_tx(
                        tx,
                        OPERATION_TABLE,
                        &publication.operation_id,
                        space,
                        &publication.source_id,
                        &operation,
                    )
                })
                .await;
        }
        let head_payload = self
            .read_control_payload(HEAD_TABLE, &head_key(space, &publication.source_id), space)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        let head: SourceHead = decode_payload(&head_payload)?;
        let mut payload = self
            .read_control_payload(OPERATION_TABLE, &publication.operation_id, space)
            .await?
            .ok_or_else(|| integrity("source operation missing"))?;
        let original_authority = live_execution_authority(&payload)?;
        for preparation in 0..EXECUTION_PROGRESS_PREPARATIONS {
            let updated =
                execution_progress_payload(&head, &payload, publication, state, error_code)?;
            before_commit(preparation).await?;
            let result = self
                .commit_execution_progress(
                    space,
                    publication,
                    attempt,
                    &head_payload,
                    &payload,
                    &updated,
                )
                .await;
            let Err(error) = result else {
                return Ok(());
            };
            let receipt_cas = native_execution_progress_conflict(&error);
            let error = publication_error(error);
            if preparation + 1 == EXECUTION_PROGRESS_PREPARATIONS || !receipt_cas {
                return Err(error);
            }
            // A failed CAS is not permission to adopt new source or producer
            // authority. Only actual live state/error drift can be reconciled.
            let current_head = self
                .read_control_payload(HEAD_TABLE, &head_key(space, &publication.source_id), space)
                .await?;
            let current = self
                .read_control_payload(OPERATION_TABLE, &publication.operation_id, space)
                .await?;
            let Some(current) = current else {
                return Err(error);
            };
            if current_head.as_deref() != Some(head_payload.as_str())
                || current == payload
                || live_execution_authority(&current)? != original_authority
            {
                return Err(error);
            }
            payload = current;
        }
        Err(conflict("stale_source_execution"))
    }

    async fn commit_execution_progress(
        &self,
        space: &str,
        publication: &elephant_types::publication::PublicationFence,
        attempt: &elephant_types::publication::AttemptFence,
        head_payload: &str,
        operation_payload: &str,
        updated: &serde_json::Value,
    ) -> StorageResult<()> {
        let mut bindings = crate::bindings! {
            "space"=>space,
            "execution_head"=>control_thing(HEAD_TABLE,&head_key(space,&publication.source_id)),
            "execution_head_payload"=>head_payload,
            "execution_operation"=>control_thing(OPERATION_TABLE,&publication.operation_id),
            "execution_old_payload"=>operation_payload,
            "execution_record"=>control_value(space,&publication.source_id,updated)?,
            "execution_publication"=>crate::surreal::json_to_surreal(serde_json::to_value(publication).map_err(|error|integrity(&error.to_string()))?)?,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        crate::work_attempt_repository::append_attempt_fence(&mut sql, &mut bindings, attempt)?;
        sql.push_str("IF $fenced_work.payload._publication != $execution_publication { THROW 'publication:attempt_source_mismatch'; };\nIF (SELECT VALUE payload FROM ONLY $execution_head) != $execution_head_payload OR (SELECT VALUE payload FROM ONLY $execution_operation) != $execution_old_payload { THROW 'publication:stale_source_execution'; };\nUPDATE $execution_head SET fence_nonce=<string>rand::uuid();\nUPDATE $execution_operation CONTENT $execution_record;\nCOMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}

const EXECUTION_PROGRESS_PREPARATIONS: usize = 4;

// This local retry path recognizes only the native transaction's exact throw.
// The shared publication error mapper also accepts diagnostic text from other
// paths; that broader compatibility mapping cannot authorize reconciliation.
fn native_execution_progress_conflict(error: &StorageError) -> bool {
    const CODE: &str = "publication:stale_source_execution";
    const DISPLAY: &str = "An error occurred: publication:stale_source_execution";
    match error {
        StorageError::QueryError(message) => message == DISPLAY,
        StorageError::Surreal(error) => match error.as_ref() {
            surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)) => message == CODE,
            surrealdb::Error::Api(surrealdb::error::Api::Query(message)) => message == DISPLAY,
            _ => false,
        },
        _ => false,
    }
}

// Compare the entire original payload, including fields unknown to this binary.
// State/error bookkeeping is the only live receipt drift that may be adopted.
fn live_execution_authority(payload: &str) -> StorageResult<serde_json::Value> {
    let operation: StoredOperation = decode_payload(payload)?;
    if !matches!(
        operation.receipt.state,
        OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
    ) || operation.receipt.completed_at.is_some()
    {
        return Err(conflict("stale_source_execution"));
    }
    let mut value: serde_json::Value = decode_payload(payload)?;
    let receipt = value
        .get_mut("receipt")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| integrity("source operation receipt missing"))?;
    receipt.remove("state");
    receipt.remove("error_code");
    Ok(value)
}

fn execution_progress_payload(
    head: &SourceHead,
    payload: &str,
    publication: &elephant_types::publication::PublicationFence,
    state: OperationState,
    error_code: Option<&str>,
) -> StorageResult<serde_json::Value> {
    let operation = updated_execution_operation(
        head,
        decode_payload(payload)?,
        publication,
        state,
        error_code,
    )?;
    let mut value: serde_json::Value = decode_payload(payload)?;
    let receipt = value
        .get_mut("receipt")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| integrity("source operation receipt missing"))?;
    for (key, updated) in [
        ("state", serde_json::to_value(operation.receipt.state)),
        (
            "error_code",
            serde_json::to_value(operation.receipt.error_code),
        ),
        (
            "completed_at",
            serde_json::to_value(operation.receipt.completed_at),
        ),
    ] {
        receipt.insert(
            key.into(),
            updated.map_err(|error| integrity(&error.to_string()))?,
        );
    }
    Ok(value)
}

fn updated_execution_operation(
    head: &SourceHead,
    mut operation: StoredOperation,
    publication: &elephant_types::publication::PublicationFence,
    state: OperationState,
    error_code: Option<&str>,
) -> StorageResult<StoredOperation> {
    if head.lifecycle != SourceVisibility::Active
        || head.source_id != publication.source_id
        || head.generation != publication.generation
        || head.desired_revision.as_deref() != Some(publication.revision_id.as_str())
        || head.operation_id != publication.operation_id
        || operation.receipt.source_generation != publication.generation
        || operation.receipt.source_id != publication.source_id
        || operation.receipt.desired_revision.as_deref() != Some(publication.revision_id.as_str())
        || operation.receipt.completed_at.is_some()
        || !matches!(
            operation.receipt.state,
            OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
        )
    {
        return Err(conflict("stale_source_execution"));
    }
    operation.receipt.state = state;
    operation.receipt.error_code = error_code.map(str::to_owned);
    operation.receipt.completed_at = if state == OperationState::FailedTerminal {
        Some(Utc::now())
    } else {
        None
    };
    Ok(operation)
}

#[cfg(test)]
#[path = "source_execution_progress_tests.rs"]
mod execution_progress_tests;

/// Rechecks a document-originated lifecycle intent under the source transaction.
pub(crate) fn check_authorized_document_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    source: &RecordId,
    document: Option<&elephant_types::records::Doc>,
) -> StorageResult<()> {
    if let Some(document) = document {
        if document.base.space_id != space
            || document
                .source
                .as_ref()
                .is_none_or(|link| link.id() != source)
            || tx
                .get::<elephant_types::records::Doc>(space, &document.base.id, true)?
                .as_ref()
                != Some(document)
        {
            return Err(conflict("authorized_document_changed"));
        }
    }
    Ok(())
}

pub(crate) fn append_authorized_document_guard(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    space: &str,
    source: &RecordId,
    document: Option<&elephant_types::records::Doc>,
) -> StorageResult<()> {
    let Some(document) = document else {
        return Ok(());
    };
    if document.base.space_id != space
        || document
            .source
            .as_ref()
            .is_none_or(|link| link.id() != source)
    {
        return Err(conflict("authorized_document_changed"));
    }
    bindings.insert(
        "lifecycle_authorized_document".into(),
        crate::surreal::thing_from_record_id(&document.base.id).into(),
    );
    bindings.insert(
        "lifecycle_authorized_document_record".into(),
        crate::canonical_commit::native_expected_fields(crate::surreal::doc_to_value(document)?),
    );
    sql.push_str("LET $lifecycle_doc_current = SELECT * FROM ONLY $lifecycle_authorized_document;\nIF $lifecycle_doc_current = NONE { THROW 'publication:authorized_document_changed'; };\nFOR $entry IN object::entries($lifecycle_authorized_document_record) { IF $lifecycle_doc_current[$entry[0]] != $entry[1] { THROW 'publication:authorized_document_changed'; }; };\n");
    Ok(())
}

fn check_accept_configuration_tx(
    tx: &EmbeddedMutation<'_>,
    token: &elephant_types::provider_binding::SpaceConfigToken,
) -> StorageResult<()> {
    let id: RecordId = token
        .config_id
        .parse()
        .map_err(|error: elephant_types::error::TypeError| integrity(&error.to_string()))?;
    let current = tx
        .get::<elephant_types::records::SpaceConfig>(&token.space_id, &id, false)?
        .ok_or_else(|| conflict("source_configuration_changed"))?;
    if current.processing.as_ref().is_none_or(|processing| {
        processing.generation != token.generation || processing.policy_id != token.policy_id
    }) {
        return Err(conflict("source_configuration_changed"));
    }
    Ok(())
}

fn append_accept_configuration_sql(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    token: &elephant_types::provider_binding::SpaceConfigToken,
) -> StorageResult<()> {
    let id: RecordId = token
        .config_id
        .parse()
        .map_err(|error: elephant_types::error::TypeError| integrity(&error.to_string()))?;
    bindings.extend(crate::bindings!{"accept_config_id"=>crate::surreal::thing_from_record_id(&id),"accept_config_generation"=>token.generation,"accept_config_policy"=>token.policy_id.clone()});
    sql.push_str("LET $accept_config = SELECT * FROM ONLY $accept_config_id;\nIF $accept_config = NONE OR $accept_config.space_id != $space OR $accept_config.deleted_at != NONE OR $accept_config.processing = NONE OR $accept_config.processing.generation != $accept_config_generation OR $accept_config.processing.policy_id != $accept_config_policy { THROW 'publication:source_configuration_changed'; };\nUPDATE $accept_config_id SET updated_at=time::now();\n");
    Ok(())
}

#[cfg(test)]
#[path = "source_producer_tests.rs"]
mod producer_tests;

#[cfg(test)]
#[path = "source_document_guards_tests.rs"]
mod document_guard_tests;
