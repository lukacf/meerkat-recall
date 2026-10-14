//! Shared preparation for retained source intake adapters.
//!
//! This module owns no durable authority. Acceptance and lifecycle remain in
//! `SourcePublicationRepository`; a queue receives the exact returned fence.

use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceAuthorizationSnapshot, SourceHead,
    SourcePrecondition, SourceVisibility, MAX_SOURCE_TEXT_BYTES,
};
use elephant_types::records::{Doc, DocMediaLocation, MediaAsset, Source};
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Write as _;
use uuid::Uuid;

use crate::source_publication::{
    AcceptSourceRevision, SourceAcceptanceRetry, SourcePublicationRepository,
};
use crate::{Repository, SourceRepository, StorageClient, StorageError, StorageResult};

/// Remove the private replay template from an outbound source projection.
///
/// The stored source and immutable revision retain this recovery payload. Raw
/// document text and attachment bytes are served through document authorization,
/// never through this internal source metadata field. URI/path scope redaction
/// remains the transport's responsibility.
#[must_use]
pub fn public_source_projection(mut source: Source) -> Source {
    if let Some(metadata) = source.metadata.as_object_mut() {
        metadata.remove("_manual_doc");
    }
    source
}

/// Adapter controls, distinct from source metadata and durable operation state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntakeControl {
    /// Caller-selected immutable input revision; omitted creates derive one.
    #[serde(default)]
    pub revision_id: Option<String>,
    /// Stable retry key; omitted creates derive one from the canonical input.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Explicit desired-head CAS token. Omission means create only.
    #[serde(default)]
    pub expected_generation: Option<String>,
}

impl IntakeControl {
    /// Decode the optional top-level adapter control before mutating anything.
    pub fn from_params(params: &Value) -> StorageResult<Self> {
        let control = params.get("intake").map_or_else(
            || Ok(Self::default()),
            |value| serde_json::from_value(value.clone()).map_err(serialization),
        )?;
        control.validate()?;
        Ok(control)
    }

    /// Validate controls for accepting source content. Lifecycle-only retract
    /// and erase controls do not need an immutable replacement revision.
    ///
    /// # Errors
    /// Rejects incomplete explicit replacement controls or invalid strings.
    pub fn validate_acceptance(&self) -> StorageResult<()> {
        self.validate()?;
        if self.expected_generation.is_some()
            && (self.revision_id.is_none() || self.idempotency_key.is_none())
        {
            return Err(StorageError::DataIntegrityError(
                "replacement requires revision_id, idempotency_key and expected_generation".into(),
            ));
        }
        Ok(())
    }

    fn validate(&self) -> StorageResult<()> {
        for value in [
            &self.revision_id,
            &self.idempotency_key,
            &self.expected_generation,
        ]
        .into_iter()
        .flatten()
        {
            if value.trim().is_empty() || value.len() > 1024 {
                return Err(StorageError::DataIntegrityError(
                    "intake controls must contain 1..=1024 UTF-8 bytes".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Join source/document restrictions without dropping either input envelope.
///
/// Empty generated base envelopes are placeholders; explicit policy domains must agree.
/// Guidance remains on the document and source metadata is retained.
pub fn attach_document(source: &mut Source, doc: &mut Doc) -> StorageResult<()> {
    use elephant_types::records::base::{Security, SubjectsState};
    let placeholder_security = |security: &Security| {
        security.policy_id == "default"
            && security.level == 0
            && security.labels.is_empty()
            && security.handling.is_empty()
    };
    if placeholder_security(&source.base.security) {
        source.base.security = doc.base.security.clone();
    }
    if placeholder_security(&doc.base.security) {
        doc.base.security = source.base.security.clone();
    }
    if source.base.subjects.is_empty() && source.base.subjects_state == SubjectsState::Unknown {
        source.base.subjects.clone_from(&doc.base.subjects);
        source.base.subjects_state = doc.base.subjects_state;
    }
    if doc.base.subjects.is_empty() && doc.base.subjects_state == SubjectsState::Unknown {
        doc.base.subjects = source.base.subjects.clone();
        doc.base.subjects_state = source.base.subjects_state;
    }
    if source.base.provenance == elephant_types::records::base::Provenance::default() {
        source.base.provenance = doc.base.provenance.clone();
    }
    crate::public_mutation::inherit_public_envelope(&mut source.base, &doc.base)?;
    doc.base.security = source.base.security.clone();
    doc.base.subjects = source.base.subjects.clone();
    doc.base.subjects_state = source.base.subjects_state;
    if source.author.is_none() {
        source.author = Some(doc.base.created_by.clone());
    }
    source.set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(doc))?;
    Ok(())
}

/// Bind a retained storage-shaped source/document request to stable identity.
///
/// A supplied external identity or already accepted physical source wins. A new unbound
/// source otherwise uses the caller's stable Doc ID, matching ingest_doc. This only reads;
/// ordinary authorization and acceptance still follow.
///
/// # Errors
/// Rejects mismatched document space/identity or unavailable accepted source input.
pub async fn bind_legacy_document_source_identity(
    storage: &StorageClient,
    source: &mut Source,
    document: &Doc,
) -> StorageResult<()> {
    if source.base.space_id != document.base.space_id || document.base.id.table() != TableName::Doc
    {
        return Err(StorageError::DataIntegrityError(
            "document identity does not belong to intake space".into(),
        ));
    }
    let mut accepted_identity = None;
    if source.external_id.is_none() {
        let repository = SourcePublicationRepository::new(storage.clone());
        if repository
            .head(&source.base.space_id, &source.base.id)
            .await?
            .is_some()
        {
            let original = SourceRepository::new(storage.clone())
                .get_with_deleted(&source.base.space_id, &source.base.id)
                .await?
                .ok_or_else(|| {
                    StorageError::Conflict("accepted source payload is unavailable".into())
                })?;
            accepted_identity = Some(source.base.id.clone());
            source.external_id = original.external_id;
        } else {
            source.external_id = Some(document.base.id.to_string());
        }
    }
    let resolved = resolve_source_identity(storage, source).await?;
    if accepted_identity
        .as_ref()
        .is_some_and(|original| original != &resolved)
    {
        return Err(StorageError::Conflict(
            "source incarnation changed during document preflight".into(),
        ));
    }
    source.base.id = resolved;
    Ok(())
}

/// Find the source explicitly addressed by a manual document submission, even before that
/// document has ever been stored.
///
/// Missing links never mint a new source incarnation. A caller without a link may reuse a
/// stored document link. This read grants no authorization; adapters still preflight
/// old/new envelopes.
///
/// # Errors
/// Refuses unknown/foreign linked sources and a reserved erasure.
pub async fn existing_source_for_manual_document(
    storage: &StorageClient,
    doc: &Doc,
) -> StorageResult<Option<Source>> {
    let Some(link) = &doc.source else {
        return crate::find_existing_source_for_doc(storage, doc).await;
    };
    if link.id().table() != TableName::Source {
        return Err(StorageError::DataIntegrityError(
            "manual document source link is invalid".into(),
        ));
    }
    if SourcePublicationRepository::new(storage.clone())
        .head(&doc.base.space_id, link.id())
        .await?
        .is_some_and(|head| head.lifecycle == SourceVisibility::Erasing)
    {
        return Err(StorageError::ErasureInProgress);
    }
    SourceRepository::new(storage.clone())
        .get_with_deleted(&doc.base.space_id, link.id())
        .await?
        .map(Some)
        .ok_or_else(|| StorageError::NotFound("linked manual document source".into()))
}

/// Resolve an intake locator through the publication repository's current external pointer,
/// or derive its first physical ID without writing a head.
///
/// This is a preflight lookup, never worker publication authority.
///
/// # Errors
/// Rejects blank external identity or failed storage reads.
pub async fn resolve_source_identity(
    storage: &StorageClient,
    source: &Source,
) -> StorageResult<RecordId> {
    let Some(external_id) = source.external_id.as_deref() else {
        return Ok(source.base.id.clone());
    };
    if external_id.trim().is_empty() {
        return Err(StorageError::DataIntegrityError(
            "external_id cannot be empty".into(),
        ));
    }
    Ok(
        match SourcePublicationRepository::new(storage.clone())
            .external_head(&source.base.space_id, source.type_, external_id)
            .await?
        {
            Some(head) => head.source_id,
            None => RecordId::new(
                TableName::Source,
                stable_uuid(&[
                    "source-intake",
                    &source.base.space_id,
                    &format!("{:?}", source.type_),
                    external_id,
                ]),
            ),
        },
    )
}

/// Fully prepared input. Adapters authorize both this proposal and `previous`
/// before calling `accept`; preparation itself performs only reads.
#[derive(Debug, Clone)]
pub struct PreparedSourceIntake {
    /// Exact proposed source envelope and immutable staged payload.
    pub source: Source,
    /// Current source to authorize, including a retracted source.
    pub previous: Option<Source>,
    /// Immutable per-source-revision document projection, when present.
    pub document: Option<Doc>,
    /// Complete current desired document snapshots for old-envelope preflight.
    pub previous_documents: Vec<Doc>,
    observed_head: Option<SourceHead>,
    absent_documents: Vec<RecordId>,
    authorized_documents: Vec<Doc>,
    manifest_observation: Option<DocumentManifestObservation>,
    request: AcceptSourceRevision,
}

#[derive(Debug, Clone)]
struct DocumentManifestObservation {
    publication: PublicationFence,
    manifest: Option<crate::source_publication_manifest::SourcePublicationManifest>,
}

#[derive(Debug, Clone, Default)]
struct CurrentDocumentObservations {
    documents: Vec<Doc>,
    absent: Vec<RecordId>,
    manifest: Option<DocumentManifestObservation>,
}

// Capture physical document identities from the ORIGINAL desired-head input
// and any already closed render manifest. Never look up a newer head midway.
async fn current_document_observations(
    storage: &StorageClient,
    head: Option<&SourceHead>,
) -> StorageResult<CurrentDocumentObservations> {
    let Some(head) = head else {
        return Ok(CurrentDocumentObservations::default());
    };
    if matches!(
        head.lifecycle,
        SourceVisibility::Erasing | SourceVisibility::Erased
    ) {
        // The actual acceptance/lifecycle authority returns the typed barrier.
        return Ok(CurrentDocumentObservations::default());
    }
    let repository = SourcePublicationRepository::new(storage.clone());
    let original = if head.lifecycle == SourceVisibility::Retracted {
        repository
            .publication_before_retraction(&head.space_id, head)
            .await?
    } else {
        let revision_id = head.desired_revision.clone().ok_or_else(|| {
            StorageError::DataIntegrityError("desired source revision is unavailable".into())
        })?;
        PublicationFence {
            source_id: head.source_id.clone(),
            generation: head.generation.clone(),
            revision_id,
            operation_id: head.operation_id.clone(),
        }
    };
    let revision = repository
        .revision(&head.space_id, &head.source_id, &original.revision_id)
        .await?
        .ok_or_else(|| {
            StorageError::DataIntegrityError("desired source revision is unavailable".into())
        })?;
    drop(repository);
    let mut identities = std::collections::BTreeSet::new();
    if let Some(document) = accepted_document_projection(&revision.source, &original)? {
        identities.insert(document.base.id);
    }
    let manifest = crate::source_publication_manifest::SourcePublicationManifestRepository::new(
        storage.clone(),
    )
    .get(&head.space_id, &original)
    .await?;
    if let Some(manifest) = &manifest {
        manifest.validate()?;
        if manifest.space_id != head.space_id || manifest.publication != original {
            return Err(StorageError::DataIntegrityError(
                "desired source document manifest changed identity".into(),
            ));
        }
        identities.extend(
            manifest
                .documents
                .iter()
                .map(|mapping| mapping.doc_id.clone()),
        );
    }
    let documents = crate::DocRepository::new(storage.clone());
    let mut present = Vec::new();
    let mut absent = Vec::new();
    for id in identities {
        match documents.get_with_deleted(&head.space_id, &id).await? {
            Some(document) => {
                if document.source.as_ref().map(Link::id) != Some(&head.source_id) {
                    return Err(StorageError::DataIntegrityError(
                        "desired document belongs to another source".into(),
                    ));
                }
                present.push(document);
            }
            None => absent.push(id),
        }
    }
    Ok(CurrentDocumentObservations {
        documents: present,
        absent,
        manifest: Some(DocumentManifestObservation {
            publication: original,
            manifest,
        }),
    })
}

/// Capture the same original desired Doc/absence/manifest set for standing
/// watch registration and dispatch preflight. This grants no source authority.
pub(crate) async fn current_watch_document_guards(
    storage: &StorageClient,
    head: &SourceHead,
) -> StorageResult<(Vec<Doc>, crate::source_document_guards::PreparedDocGuards)> {
    let observed = current_document_observations(storage, Some(head)).await?;
    let mut guards = crate::source_document_guards::PreparedDocGuards::from_authorized_documents(
        &head.space_id,
        &head.source_id,
        Some(head),
        &observed.documents,
    )?
    .with_absent_documents(&observed.absent)?;
    if let Some(manifest) = observed.manifest {
        guards =
            guards.with_manifest_observation(&manifest.publication, manifest.manifest.as_ref())?;
    }
    Ok((observed.documents, guards))
}

/// Accepted input plus the original authority to pass to the queue. This value
/// is deliberately not serializable: wire responses expose `operation` only.
#[derive(Debug, Clone)]
pub struct AcceptedSourceIntake {
    /// Immutable accepted source snapshot, never the latest mutable source.
    pub source: Source,
    /// Document projection from that same accepted snapshot.
    pub document: Option<Doc>,
    /// Payload-free operation receipt, including the original retry identity.
    pub operation: OperationReceipt,
    /// Original source authority; never reconstruct this from a current head.
    pub publication: PublicationFence,
    // Ephemeral classification from A's original semantic retry proof, never
    // durable state or a caller-controlled instruction to change receipt status.
    receipt_only: bool,
}

impl AcceptedSourceIntake {
    /// A retry of an already terminal operation returns its receipt without
    /// scheduling stale work or restaging historical evidence.
    #[must_use]
    pub const fn needs_processing(&self) -> bool {
        !self.receipt_only
            && matches!(
                self.operation.state,
                OperationState::Accepted
                    | OperationState::Processing
                    | OperationState::FailedRetryable
            )
    }
}

impl PreparedSourceIntake {
    /// Resolve stable identity, validate size, and allocate an immutable document.
    pub async fn prepare(
        storage: &StorageClient,
        source: Source,
        control: IntakeControl,
    ) -> StorageResult<Self> {
        Box::pin(Self::prepare_with_assets(storage, source, control, &[])).await
    }

    /// Preserve recoverable media input in the accepted source snapshot. This
    /// method only reads; assets are written under the accepted head fence.
    pub async fn prepare_with_assets(
        storage: &StorageClient,
        mut source: Source,
        control: IntakeControl,
        assets: &[MediaAsset],
    ) -> StorageResult<Self> {
        control.validate_acceptance()?;
        if source.base.space_id.trim().is_empty() || source.base.deleted_at.is_some() {
            return Err(StorageError::DataIntegrityError(
                "invalid intake space or lifecycle".into(),
            ));
        }
        let repo = SourcePublicationRepository::new(storage.clone());
        source.base.id = resolve_source_identity(storage, &source).await?;
        let observed_head = repo.head(&source.base.space_id, &source.base.id).await?;
        let observations = current_document_observations(storage, observed_head.as_ref()).await?;
        let previous = SourceRepository::new(storage.clone())
            .get_with_deleted(&source.base.space_id, &source.base.id)
            .await?;
        let mut assets = if assets.is_empty() {
            accepted_assets(&source)?
        } else {
            assets.to_vec()
        };
        let mut template = source.manual_doc_template()?;
        capture_managed_assets(storage, &source, template.as_ref(), &mut assets).await?;
        normalize_assets(&mut source, &mut template, &mut assets, "input")?;
        if let Some(doc) = &template {
            if doc.text.len() > MAX_SOURCE_TEXT_BYTES
                || doc
                    .sidecar_text
                    .as_ref()
                    .is_some_and(|text| text.len() > MAX_SOURCE_TEXT_BYTES)
            {
                return Err(StorageError::DataIntegrityError("payload_too_large".into()));
            }
        }
        // Generated record IDs, render keys and audit times are not caller intent.
        // Security, subjects, source timestamp and extraction guidance ARE intent.
        let fingerprint = fingerprint(&source)?;
        let revision_id = control
            .revision_id
            .unwrap_or_else(|| format!("input-{fingerprint}"));
        if let Some(doc) = &mut template {
            doc.doc_id =
                immutable_document_id(&source.base.space_id, &source.base.id, &revision_id)
                    .to_string();
            doc.rendition_key = format!("source-revision:{revision_id}");
            source.set_manual_doc_template(doc)?;
        }
        normalize_assets(&mut source, &mut template, &mut assets, &revision_id)?;
        let document = document_projection(&source)?;
        if document
            .as_ref()
            .is_some_and(|doc| !doc.metadata.is_object())
        {
            return Err(StorageError::DataIntegrityError(
                "document metadata must be an object".into(),
            ));
        }
        let request = AcceptSourceRevision {
            source: source.clone(),
            revision_id: revision_id.clone(),
            expected: control
                .expected_generation
                .map_or(SourcePrecondition::Absent, SourcePrecondition::Generation),
            idempotency_key: control
                .idempotency_key
                .unwrap_or_else(|| format!("intake:{}:{revision_id}", source.base.id)),
            request_fingerprint: fingerprint,
            reactivate: false,
            recreate_after_erasure: false,
            authorized: previous.as_ref().map(SourceAuthorizationSnapshot::from),
            authorized_document: None,
        };
        // A previews the exact physical acceptance identity. A retained intake
        // never enables erasure recreation, so a changed pointer is a conflict
        // requiring fresh preflight rather than silently retargeted authority.
        let proposed = repo.acceptance_source(&request).await?;
        drop(repo);
        if proposed.base.id != source.base.id {
            return Err(StorageError::Conflict(
                "source incarnation changed during intake preparation".into(),
            ));
        }
        let request = AcceptSourceRevision {
            source: proposed.clone(),
            ..request
        };
        let document = document_projection(&proposed)?;
        Ok(Self {
            source: proposed,
            previous,
            document,
            previous_documents: observations.documents,
            observed_head,
            absent_documents: observations.absent,
            authorized_documents: Vec::new(),
            manifest_observation: observations.manifest,
            request,
        })
    }

    /// Preserve an authorization observation captured before an external read.
    /// The authority transaction compares this exact snapshot at mutation time.
    #[must_use]
    pub fn with_authorized_snapshot(mut self, snapshot: SourceAuthorizationSnapshot) -> Self {
        self.request.authorized = Some(snapshot);
        self
    }

    /// Bind the exact existing document which passed transport authorization.
    /// A checks this complete row inside the source acceptance transaction.
    #[must_use]
    pub fn with_authorized_document(mut self, document: Doc) -> Self {
        // Source-less manual documents still need exact old-row guards when
        // their caller identity is admitted as a new source-backed revision.
        self.authorized_documents.push(document.clone());
        if document.source.is_some() {
            self.request.authorized_document = Some(document);
        }
        self
    }

    /// Check background intake against its originally saved source authority.
    /// This grants no contact capability and does not replace source/attempt CAS.
    ///
    /// # Errors
    /// Refuses changed source restrictions or a document outside that envelope.
    pub fn validate_service_authority(
        &self,
        authorized: &SourceAuthorizationSnapshot,
    ) -> StorageResult<()> {
        let envelope = &authorized.envelope;
        if self.source.base.id != envelope.source_id
            || self.source.base.space_id != envelope.space_id
            || self.source.base.security != envelope.security
            || self.source.base.subjects != envelope.subjects
            || self.source.base.subjects_state != envelope.subjects_state
        {
            return Err(StorageError::Conflict(
                "source service authority changed".into(),
            ));
        }
        for document in self
            .previous_documents
            .iter()
            .chain(self.authorized_documents.iter())
            .chain(self.document.iter())
        {
            let mut allowed = document.base.clone();
            allowed.space_id.clone_from(&envelope.space_id);
            allowed.security.clone_from(&envelope.security);
            allowed.subjects.clone_from(&envelope.subjects);
            allowed.subjects_state = envelope.subjects_state;
            let mut joined = allowed.clone();
            crate::public_mutation::inherit_public_envelope(&mut joined, &document.base)?;
            if joined.security != allowed.security
                || joined.subjects != allowed.subjects
                || joined.subjects_state != allowed.subjects_state
            {
                return Err(StorageError::Conflict(
                    "document preflight exceeds original service authority".into(),
                ));
            }
        }
        Ok(())
    }

    /// Explicit restoration endpoint only. Ordinary intake never calls this.
    /// The caller must authorize the retained source and supply a new revision
    /// plus the retracted generation before invoking this method.
    #[must_use]
    pub const fn reactivate(mut self) -> Self {
        self.request.reactivate = true;
        self
    }

    /// Accept through A's transaction after adapter authorization. No rollback
    /// may undo this operation if later queue submission fails.
    pub async fn accept(self, storage: &StorageClient) -> StorageResult<AcceptedSourceIntake> {
        Box::pin(self.accept_mode(storage, None, None)).await
    }

    /// Resolve an exact original receipt whose admitted contact plan is no
    /// longer available.
    ///
    /// This performs no acceptance, document staging or work
    /// submission. The returned value never needs processing, even when its
    /// truthful original status remains pending or retryable.
    ///
    /// Call only after authorizing every current/proposed source and document
    /// snapshot. A verifies the full semantic input, including its envelope;
    /// possession of an idempotency key alone cannot retrieve a receipt.
    ///
    /// # Errors
    /// Refuses conflicting intent, unavailable retained input or envelope drift.
    pub async fn read_only_contact_retry(
        &self,
        storage: &StorageClient,
    ) -> StorageResult<Option<AcceptedSourceIntake>> {
        let SourceAcceptanceRetry::ExistingContactPlanUnavailable {
            operation,
            capability,
        } = SourcePublicationRepository::new(storage.clone())
            .source_acceptance_retry(&self.request)
            .await?
        else {
            return Ok(None);
        };
        let accepted = Self::load_accepted(storage, operation, true).await?;
        if accepted.publication != *capability.publication()
            || accepted.source.base.id != self.source.base.id
            || accepted.source.base.space_id != self.source.base.space_id
            || accepted.source.base.security != self.source.base.security
            || accepted.source.base.subjects != self.source.base.subjects
            || accepted.source.base.subjects_state != self.source.base.subjects_state
        {
            return Err(StorageError::DataIntegrityError(
                "original receipt envelope does not match authorized intake".into(),
            ));
        }
        Ok(Some(accepted))
    }

    /// Reuse the original sealed contact intent on a semantic retry; otherwise
    /// prepare from the source identity resolved before adapter authorization.
    ///
    /// # Errors
    /// Rejects mismatched intent reuse or invalid current contact dependencies.
    pub async fn prepare_contact(
        &self,
        storage: &StorageClient,
    ) -> StorageResult<Option<crate::source_contact_intake::PreparedSourceContact>> {
        match SourcePublicationRepository::new(storage.clone())
            .source_acceptance_retry(&self.request)
            .await?
        {
            crate::source_publication::SourceAcceptanceRetry::Existing { contact, .. } => {
                Ok(contact)
            }
            SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => Err(
                StorageError::Conflict("accepted_contact_plan_unavailable".into()),
            ),
            crate::source_publication::SourceAcceptanceRetry::NewIntent => {
                Box::pin(
                    crate::source_contact_intake::PreparedSourceContact::prepare(
                        storage,
                        &self.source,
                    ),
                )
                .await
            }
        }
    }

    /// Preserve an admitted capability on an explicit caller replacement.
    ///
    /// The original head is the one captured before transport authorization;
    /// background producers must instead supply their saved publication fence.
    /// New plain sources cannot gain authority from contact metadata alone.
    ///
    /// # Errors
    /// Refuses conflicting retries, stale acceptance or invalid contact effects.
    pub async fn prepare_replacement_contact(
        &self,
        storage: &StorageClient,
    ) -> StorageResult<Option<crate::source_contact_intake::PreparedSourceContact>> {
        let repository = SourcePublicationRepository::new(storage.clone());
        match repository.source_acceptance_retry(&self.request).await? {
            SourceAcceptanceRetry::Existing { contact, .. } => return Ok(contact),
            SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => {
                return Err(StorageError::Conflict(
                    "accepted_contact_plan_unavailable".into(),
                ));
            }
            SourceAcceptanceRetry::NewIntent => {}
        }
        let Some(head) = self
            .observed_head
            .as_ref()
            .filter(|head| head.lifecycle == SourceVisibility::Active)
        else {
            return Ok(None);
        };
        let original = PublicationFence {
            source_id: head.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: head.desired_revision.clone().ok_or_else(|| {
                StorageError::DataIntegrityError("accepted source has no desired revision".into())
            })?,
            operation_id: head.operation_id.clone(),
        };
        Box::pin(repository.prepared_reprocess_contact(&self.request, &original)).await
    }

    /// Prepare fresh contact authority only when the original immutable input
    /// had an admitted capability; identical retries keep original Some/None.
    ///
    /// Callers authorize every returned old/new contact envelope before accept.
    ///
    /// # Errors
    /// Rejects stale original publication, mismatched intent, or invalid contacts.
    pub async fn prepare_reprocess_contact(
        &self,
        storage: &StorageClient,
        original: &PublicationFence,
    ) -> StorageResult<Option<crate::source_contact_intake::PreparedSourceContact>> {
        Box::pin(
            SourcePublicationRepository::new(storage.clone())
                .prepared_reprocess_contact(&self.request, original),
        )
        .await
    }

    /// Restore only the capability admitted by the exact pre-retraction
    /// acceptance.
    ///
    /// Existing retries retain their original admitted Some/None.
    /// The adapter must authorize all returned contact envelopes before accept.
    ///
    /// # Errors
    /// Refuses missing predecessor proof, stale retraction, or implicit revival.
    pub async fn prepare_restore_contact(
        &self,
        storage: &StorageClient,
    ) -> StorageResult<Option<crate::source_contact_intake::PreparedSourceContact>> {
        let original_head = self
            .observed_head
            .as_ref()
            .ok_or_else(|| StorageError::Conflict("restore requires an accepted source".into()))?;
        Box::pin(
            SourcePublicationRepository::new(storage.clone())
                .prepared_restore_contact(&self.request, original_head),
        )
        .await
    }

    /// Accept the server-prepared contact attachment in the same source transaction.
    /// The caller authorizes every contact dependency and proposed envelope first.
    ///
    /// # Errors
    /// Returns the source admission or exact contact read-set conflict unchanged.
    pub async fn accept_with_prepared_contact(
        self,
        storage: &StorageClient,
        contact: &crate::source_contact_intake::PreparedSourceContact,
    ) -> StorageResult<AcceptedSourceIntake> {
        Box::pin(self.accept_mode(storage, Some(contact), None)).await
    }

    /// Accept a watched revision with its original live execution guard.
    ///
    /// Receipt-only classification comes from A's actual transaction result;
    /// only a fresh acceptance may stage its document or enqueue initial work.
    pub async fn accept_with_watch_producer(
        self,
        storage: &StorageClient,
        producer: &crate::source_watch_producer::PreparedSourceWatchProducer,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
    ) -> StorageResult<AcceptedSourceIntake> {
        Box::pin(self.accept_mode(storage, contact, Some(producer))).await
    }

    async fn accept_mode(
        self,
        storage: &StorageClient,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
        producer: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
    ) -> StorageResult<AcceptedSourceIntake> {
        if let Some(receipt) = self.read_only_contact_retry(storage).await? {
            if producer.is_some() {
                return Err(StorageError::Conflict(
                    "accepted_contact_plan_unavailable".into(),
                ));
            }
            return Ok(receipt);
        }
        let repo = SourcePublicationRepository::new(storage.clone());
        let mut documents = self.previous_documents;
        documents.extend(self.authorized_documents);
        let mut guards =
            crate::source_document_guards::PreparedDocGuards::from_authorized_documents(
                &self.source.base.space_id,
                &self.source.base.id,
                self.observed_head.as_ref(),
                &documents,
            )?
            .with_absent_documents(&self.absent_documents)?;
        if let Some(observation) = &self.manifest_observation {
            guards = guards.with_manifest_observation(
                &observation.publication,
                observation.manifest.as_ref(),
            )?;
        }
        let (operation, receipt_only) = if let Some(producer) = producer {
            use crate::source_watch_producer::WatchSourceAcceptance;
            match Box::pin(repo.accept_with_watch_producer_outcome(
                self.request,
                producer,
                contact,
                Some(&guards),
            ))
            .await?
            {
                WatchSourceAcceptance::Accepted(operation) => (operation, false),
                WatchSourceAcceptance::ReceiptOnly(operation) => (operation, true),
            }
        } else {
            (
                Box::pin(repo.accept_with_document_guards(self.request, &guards, contact)).await?,
                false,
            )
        };
        let accepted = Self::load_accepted(storage, operation, receipt_only).await?;
        if accepted.needs_processing() {
            Box::pin(stage_source_revision_mode(
                storage,
                &accepted.source,
                &accepted.publication,
                producer,
            ))
            .await?;
        }
        Ok(accepted)
    }

    async fn load_accepted(
        storage: &StorageClient,
        operation: OperationReceipt,
        receipt_only: bool,
    ) -> StorageResult<AcceptedSourceIntake> {
        let repo = SourcePublicationRepository::new(storage.clone());
        let revision_id = operation.desired_revision.clone().ok_or_else(|| {
            StorageError::DataIntegrityError("accepted operation has no immutable revision".into())
        })?;
        let revision = repo
            .revision(&operation.space_id, &operation.source_id, &revision_id)
            .await?
            .ok_or_else(|| {
                StorageError::Conflict("accepted source payload is no longer available".into())
            })?;
        drop(repo);
        let publication = PublicationFence {
            source_id: operation.source_id.clone(),
            generation: operation.source_generation.clone(),
            revision_id,
            operation_id: operation.operation_id.clone(),
        };
        let document = accepted_document_projection(&revision.source, &publication)?;
        let accepted = AcceptedSourceIntake {
            source: revision.source,
            document,
            operation,
            publication,
            receipt_only,
        };
        Ok(accepted)
    }
}

/// Read-only preparation for the source/doc branch of public audit revert.
/// Public adapters must authorize the audit, current record, current source,
/// and proposed source/document before calling `accept`.
#[derive(Debug, Clone)]
pub struct PreparedSourceAuditRevert {
    /// Current source snapshot bound to the mutation's authorization CAS.
    pub current_source: Source,
    /// Current source-backed document when the audit targets a document.
    pub current_document: Option<Doc>,
    /// Additional current physical documents requiring full old-row authorization.
    pub current_documents: Vec<Doc>,
    /// Original admitted contact capability freshly prepared for explicit restore.
    /// Authorize every old/new/dependency row before committing this inverse.
    pub contact: Option<crate::source_contact_intake::PreparedSourceContact>,
    /// Proposed accepted source, absent for reversal by retraction.
    pub proposed_source: Option<Source>,
    /// Proposed immutable document, absent for reversal by retraction.
    pub proposed_document: Option<Doc>,
    prepared: Option<PreparedSourceIntake>,
    retraction: Option<PreparedSourceRetraction>,
}

/// Outcome of an authorized inverse. An accepted input must receive the same
/// original-fence queue handoff as ordinary intake; a retraction needs no queue.
#[derive(Debug, Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "Preserve the public Accepted(AcceptedSourceIntake) variant shape; boxing the payload would change the exposed audit-revert API."
)]
pub enum SourceAuditRevertOutcome {
    /// New accepted revision restoring the historical input.
    Accepted(AcceptedSourceIntake),
    /// Source support retracted by A's lifecycle authority.
    Retracted(OperationReceipt),
}

impl PreparedSourceAuditRevert {
    /// Commit only after transport policy preflight of all public snapshots.
    pub async fn accept(
        self,
        storage: &StorageClient,
        actor: &str,
    ) -> StorageResult<SourceAuditRevertOutcome> {
        if let Some(prepared) = self.prepared {
            let accepted = match &self.contact {
                Some(contact) => {
                    prepared
                        .accept_with_prepared_contact(storage, contact)
                        .await?
                }
                None => prepared.accept(storage).await?,
            };
            Ok(SourceAuditRevertOutcome::Accepted(accepted))
        } else {
            let mut retraction = self.retraction.ok_or_else(|| {
                StorageError::DataIntegrityError("audit inverse has no prepared mutation".into())
            })?;
            retraction.request.actor = actor.into();
            Ok(SourceAuditRevertOutcome::Retracted(
                Box::pin(retraction.accept(storage)).await?,
            ))
        }
    }
}

/// Prepare an explicit audit inverse without changing durable state. Unsupported
/// source-less documents belong to the canonical manual lifecycle adapter.
#[allow(
    clippy::too_many_lines,
    reason = "Audit reversion binds current receipts, original document observations, contacts, and authorization into one prepared reversal; preserve this capture order and public preparation boundary."
)]
pub async fn prepare_source_audit_revert(
    storage: &StorageClient,
    space: &str,
    audit: &elephant_types::records::AuditLog,
    control: IntakeControl,
) -> StorageResult<PreparedSourceAuditRevert> {
    use elephant_types::records::AuditAction;
    if audit.base.space_id != space
        || control.expected_generation.is_none()
        || control.idempotency_key.is_none()
    {
        return Err(StorageError::DataIntegrityError(
            "source audit revert requires selected space, expected_generation and idempotency_key"
                .into(),
        ));
    }
    let current_document = if audit.record.table() == TableName::Doc {
        Some(
            crate::DocRepository::new(storage.clone())
                .get_with_deleted(space, &audit.record)
                .await?
                .ok_or_else(|| StorageError::NotFound("audit target document".into()))?,
        )
    } else {
        None
    };
    let source_id = match audit.record.table() {
        TableName::Source => audit.record.clone(),
        TableName::Doc => current_document
            .as_ref()
            .and_then(|doc| doc.source.as_ref())
            .map(|source| source.id().clone())
            .ok_or_else(|| {
                StorageError::DataIntegrityError(
                    "source-less document requires canonical manual audit inverse".into(),
                )
            })?,
        _ => {
            return Err(StorageError::DataIntegrityError(
                "source audit inverse requires source or document".into(),
            ))
        }
    };
    let current_source = SourceRepository::new(storage.clone())
        .get_with_deleted(space, &source_id)
        .await?
        .ok_or_else(|| {
            StorageError::Conflict("erased source cannot be restored from an audit payload".into())
        })?;
    let current_value = match &current_document {
        Some(doc) => serde_json::to_value(doc).map_err(serialization)?,
        None => serde_json::to_value(&current_source).map_err(serialization)?,
    };
    let stale = if let Some(after) = &audit.after {
        current_value != *after
    } else if audit.action == AuditAction::Delete {
        let mut current = current_value.clone();
        let mut before = audit.before.clone().ok_or_else(|| {
            StorageError::DataIntegrityError("delete audit has no before snapshot".into())
        })?;
        let is_deleted = current
            .get("deleted_at")
            .is_some_and(|value| !value.is_null());
        for value in [&mut current, &mut before] {
            if let Some(object) = value.as_object_mut() {
                object.remove("updated_at");
                object.remove("deleted_at");
            }
        }
        !is_deleted || current != before
    } else {
        true
    };
    if stale {
        return Err(StorageError::Conflict(
            "audit target changed after the audited action".into(),
        ));
    }
    if matches!(audit.action, AuditAction::Create | AuditAction::Restore) {
        let retraction = PreparedSourceRetraction::prepare(
            storage,
            &current_source,
            control,
            &audit.actor,
            current_document.as_ref(),
        )
        .await?;
        return Ok(PreparedSourceAuditRevert {
            current_source,
            current_document,
            current_documents: retraction.documents.clone(),
            contact: None,
            proposed_source: None,
            proposed_document: None,
            prepared: None,
            retraction: Some(retraction),
        });
    }
    if !matches!(audit.action, AuditAction::Update | AuditAction::Delete) {
        return Err(StorageError::DataIntegrityError(
            "unsupported source audit inverse action".into(),
        ));
    }
    let before = audit.before.clone().ok_or_else(|| {
        StorageError::DataIntegrityError("audit inverse has no before snapshot".into())
    })?;
    let mut proposed = if audit.record.table() == TableName::Source {
        let source: Source = serde_json::from_value(before).map_err(serialization)?;
        if source.base.id != source_id || source.base.space_id != space {
            return Err(StorageError::DataIntegrityError(
                "audit source identity mismatch".into(),
            ));
        }
        source
    } else {
        let doc: Doc = serde_json::from_value(before).map_err(serialization)?;
        if doc.base.id != audit.record
            || doc.base.space_id != space
            || doc.source.as_ref().map(Link::id) != Some(&source_id)
        {
            return Err(StorageError::DataIntegrityError(
                "audit document identity mismatch".into(),
            ));
        }
        let mut source = current_source.clone();
        source.base.security = doc.base.security.clone();
        source.base.subjects.clone_from(&doc.base.subjects);
        source.base.subjects_state = doc.base.subjects_state;
        source.set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(
            &doc,
        ))?;
        source
    };
    proposed.base.deleted_at = None;
    let mut prepared = PreparedSourceIntake::prepare(storage, proposed, control.clone())
        .await?
        .with_authorized_snapshot(SourceAuthorizationSnapshot::from(&current_source));
    if let Some(document) = &current_document {
        prepared = prepared.with_authorized_document(document.clone());
    }
    if audit.action == AuditAction::Delete {
        prepared = prepared.reactivate();
    }
    let contact = if prepared.read_only_contact_retry(storage).await?.is_some() {
        // Public audit adapters still authorize the exposed source/doc rows;
        // accept_mode returns only this same semantic receipt, with no staging.
        None
    } else if audit.action == AuditAction::Delete {
        prepared.prepare_restore_contact(storage).await?
    } else {
        None
    };
    Ok(PreparedSourceAuditRevert {
        current_source,
        current_document,
        current_documents: prepared.previous_documents.clone(),
        contact,
        proposed_source: Some(prepared.source.clone()),
        proposed_document: prepared.document.clone(),
        prepared: Some(prepared),
        retraction: None,
    })
}

/// Read-only retraction preparation. Transport callers authorize every
/// document in `documents` before invoking `accept`; A/E own the mutation.
#[derive(Debug, Clone)]
pub struct PreparedSourceRetraction {
    /// Exact caller and current desired physical document snapshots.
    pub documents: Vec<Doc>,
    request: crate::source_publication::SourceMutation,
    guards: crate::source_document_guards::PreparedDocGuards,
}

impl PreparedSourceRetraction {
    /// Capture the original head and complete current document observations.
    /// `source` and any caller document must also pass transport authorization.
    ///
    /// # Errors
    /// Refuses missing lifecycle controls, inconsistent scope, or missing proof.
    pub async fn prepare(
        storage: &StorageClient,
        source: &Source,
        control: IntakeControl,
        actor: &str,
        authorized_document: Option<&Doc>,
    ) -> StorageResult<Self> {
        control.validate()?;
        let generation = control.expected_generation.ok_or_else(|| {
            StorageError::DataIntegrityError("retract requires intake.expected_generation".into())
        })?;
        let key = control.idempotency_key.ok_or_else(|| {
            StorageError::DataIntegrityError("retract requires intake.idempotency_key".into())
        })?;
        let repository = SourcePublicationRepository::new(storage.clone());
        let head = repository
            .head(&source.base.space_id, &source.base.id)
            .await?;
        drop(repository);
        let observations = current_document_observations(storage, head.as_ref()).await?;
        let mut documents = observations.documents;
        if let Some(document) = authorized_document {
            if documents
                .iter()
                .any(|current| current.base.id == document.base.id && current != document)
            {
                return Err(StorageError::Conflict("authorized_document_changed".into()));
            }
            if !documents.contains(document) {
                documents.push(document.clone());
            }
        }
        let mut guards =
            crate::source_document_guards::PreparedDocGuards::from_authorized_documents(
                &source.base.space_id,
                &source.base.id,
                head.as_ref(),
                &documents,
            )?
            .with_absent_documents(&observations.absent)?;
        if let Some(observation) = &observations.manifest {
            guards = guards.with_manifest_observation(
                &observation.publication,
                observation.manifest.as_ref(),
            )?;
        }
        let request = crate::source_publication::SourceMutation {
            space_id: source.base.space_id.clone(),
            source_id: source.base.id.clone(),
            expected: SourcePrecondition::Generation(generation.clone()),
            request_fingerprint: stable_uuid(&[
                "retract",
                &source.base.space_id,
                &source.base.id.to_string(),
                &generation,
                &key,
            ])
            .to_string(),
            idempotency_key: key,
            actor: actor.into(),
            authorized: SourceAuthorizationSnapshot::from(source),
            // The new multi-row guard also supports source-less caller docs.
            authorized_document: authorized_document
                .filter(|doc| doc.source.is_some())
                .cloned(),
        };
        Ok(Self {
            documents,
            request,
            guards,
        })
    }

    /// Retract after complete source/document authorization. The original
    /// snapshots and absence observations are checked in the lifecycle transaction.
    ///
    /// # Errors
    /// Returns lifecycle, erasure-barrier and exact document conflicts unchanged.
    pub async fn accept(self, storage: &StorageClient) -> StorageResult<OperationReceipt> {
        Box::pin(
            SourcePublicationRepository::new(storage.clone())
                .retract_with_document_guards(self.request, &self.guards),
        )
        .await
    }
}

/// Trusted storage convenience for an already authorized source. Transport
/// adapters use `PreparedSourceRetraction` to authorize its current documents.
pub async fn retract_authorized_source(
    storage: &StorageClient,
    source: &Source,
    control: IntakeControl,
    actor: &str,
) -> StorageResult<OperationReceipt> {
    Box::pin(retract_authorized_source_document(
        storage, source, control, actor, None,
    ))
    .await
}

/// Trusted storage convenience for already authorized source/document state.
/// Public handlers must preflight `PreparedSourceRetraction::documents` as well.
pub async fn retract_authorized_source_document(
    storage: &StorageClient,
    source: &Source,
    control: IntakeControl,
    actor: &str,
    authorized_document: Option<&Doc>,
) -> StorageResult<OperationReceipt> {
    Box::pin(
        PreparedSourceRetraction::prepare(storage, source, control, actor, authorized_document)
            .await?
            .accept(storage),
    )
    .await
}

/// Resolve immutable accepted input for an existing document. Reprocessing an
/// older rendition must never substitute the mutable source's latest payload.
pub async fn source_for_document(storage: &StorageClient, doc: &Doc) -> StorageResult<Source> {
    source_and_publication_for_document(storage, doc)
        .await
        .map(|(source, _)| source)
}

/// Resolve immutable input together with its exact original document authority.
///
/// # Errors
/// Rejects missing lineage, mismatched source identity, or unavailable input.
pub async fn source_and_publication_for_document(
    storage: &StorageClient,
    doc: &Doc,
) -> StorageResult<(Source, PublicationFence)> {
    let mapping = crate::source_publication_manifest::SourcePublicationManifestRepository::new(
        storage.clone(),
    )
    .source_revision_for_document(&doc.base.space_id, &doc.base.id)
    .await?
    .ok_or_else(|| {
        StorageError::Conflict("document has no completed source render mapping".into())
    })?;
    if mapping.space_id != doc.base.space_id
        || mapping.doc_id != doc.base.id
        || doc.source.as_ref().map(Link::id) != Some(&mapping.publication.source_id)
    {
        return Err(StorageError::Conflict(
            "document source mapping mismatch".into(),
        ));
    }
    SourcePublicationRepository::new(storage.clone())
        .revision(
            &doc.base.space_id,
            &mapping.publication.source_id,
            &mapping.publication.revision_id,
        )
        .await?
        .map(|revision| (revision.source, mapping.publication))
        .ok_or_else(|| StorageError::Conflict("accepted source payload is unavailable".into()))
}

/// Materialize the accepted template without overwriting published evidence.
///
/// The head is checked and serialized with concurrent lifecycle mutations in the same
/// transaction. Failed staging never rolls back durable acceptance.
pub async fn stage_accepted_document(
    storage: &StorageClient,
    accepted: &AcceptedSourceIntake,
) -> StorageResult<()> {
    stage_source_revision(storage, &accepted.source, &accepted.publication).await
}

/// Recovery materialization using only the immutable revision and its original
/// acceptance fence. Never reconstruct this authority from the current head.
pub async fn stage_source_revision(
    storage: &StorageClient,
    source: &Source,
    publication: &PublicationFence,
) -> StorageResult<()> {
    Box::pin(stage_source_revision_mode(
        storage,
        source,
        publication,
        None,
    ))
    .await
}

#[allow(
    clippy::too_many_lines,
    reason = "The embedded and native branches stage the same head, revision, authorization, and assets atomically; keep their complete transaction composition together."
)]
async fn stage_source_revision_mode(
    storage: &StorageClient,
    source: &Source,
    publication: &PublicationFence,
    producer: Option<&crate::source_watch_producer::PreparedSourceWatchProducer>,
) -> StorageResult<()> {
    use crate::source_publication::{control_thing, head_key, read_control_tx, HEAD_TABLE};
    if source.base.id != publication.source_id {
        return Err(StorageError::Conflict("source_fence_mismatch".into()));
    }
    let document = accepted_document_projection(source, publication)?;
    let assets = accepted_assets(source)?;
    let Some(doc) = &document else {
        return Ok(());
    };
    let space = &source.base.space_id;
    let standing = if producer.is_none() {
        crate::standing_watch_repository::PreparedStandingWatchAuthority::capture_for_source_operation(
            storage, space, publication,
        ).await?
    } else {
        None
    };
    let current_source = SourceRepository::new(storage.clone())
        .get(space, &publication.source_id)
        .await?
        .ok_or_else(|| StorageError::Conflict("source_staging_authorization_changed".into()))?;
    if current_source.base.security != source.base.security
        || current_source.base.subjects != source.base.subjects
        || current_source.base.subjects_state != source.base.subjects_state
    {
        return Err(StorageError::Conflict(
            "source_staging_authorization_changed".into(),
        ));
    }
    let current_document = crate::DocRepository::new(storage.clone())
        .get_with_deleted(space, &doc.base.id)
        .await?;
    let original_head = SourcePublicationRepository::new(storage.clone())
        .head(space, &publication.source_id)
        .await?
        .filter(|head| {
            head.lifecycle == SourceVisibility::Active
                && head.generation == publication.generation
                && head.desired_revision.as_deref() == Some(publication.revision_id.as_str())
                && head.operation_id == publication.operation_id
        })
        .ok_or_else(|| StorageError::Conflict("stale_source_publication".into()))?;
    let mut document_guards =
        crate::source_document_guards::PreparedDocGuards::from_authorized_documents(
            space,
            &source.base.id,
            Some(&original_head),
            current_document.as_slice(),
        )?;
    if current_document.is_none() {
        document_guards =
            document_guards.with_absent_documents(std::slice::from_ref(&doc.base.id))?;
    }
    let principals =
        producer
            .iter()
            .map(|producer| producer.principal())
            .chain(standing.iter().flat_map(
                crate::standing_watch_repository::PreparedStandingSourceAuthority::principals,
            ));
    for principal in principals {
        use crate::canonical_commit::CanonicalRecord;
        use crate::source_watch_registration::authorize_standing_source_input;
        for observed in [&current_source, source] {
            authorize_standing_source_input(
                principal,
                &CanonicalRecord::Source(Box::new(observed.clone())),
            )?;
        }
        for observed in current_document.iter().chain(std::iter::once(doc)) {
            authorize_standing_source_input(
                principal,
                &CanonicalRecord::Doc(Box::new(observed.clone())),
            )?;
        }
        for asset in &assets {
            crate::source_watch_registration::authorize_standing_media_asset(principal, asset)?;
        }
    }
    let matches = |head: &SourceHead| {
        head.lifecycle == SourceVisibility::Active
            && head.source_id == publication.source_id
            && head.generation == publication.generation
            && head.desired_revision.as_deref() == Some(publication.revision_id.as_str())
            && head.operation_id == publication.operation_id
    };
    if let Some(store) = storage.embedded_store() {
        return store
            .mutate(|tx| {
                if tx
                    .get::<Source>(space, &current_source.base.id, false)?
                    .as_ref()
                    != Some(&current_source)
                {
                    return Err(StorageError::Conflict(
                        "source_staging_authorization_changed".into(),
                    ));
                }
                document_guards.check_tx(tx)?;
                if let Some(producer) = producer {
                    producer.check_tx(tx)?;
                } else if let Some(standing) = &standing {
                    standing.check_tx(tx)?;
                }
                let head: SourceHead = read_control_tx(
                    tx,
                    HEAD_TABLE,
                    &head_key(space, &publication.source_id),
                    space,
                )?
                .filter(&matches)
                .ok_or_else(|| StorageError::Conflict("stale_source_publication".into()))?;
                let _ = head;
                for asset in &assets {
                    if let Some(existing) = tx.get_by_id::<MediaAsset>(&asset.base.id, true)? {
                        if existing != *asset {
                            return Err(StorageError::Conflict(
                                "immutable_attachment_conflict".into(),
                            ));
                        }
                    } else {
                        tx.create(asset.clone())?;
                    }
                }
                if let Some(existing) = tx.get_by_id::<Doc>(&doc.base.id, true)? {
                    if existing.base.space_id != *space
                        || existing.source != doc.source
                        || existing.base.security != doc.base.security
                        || existing.base.subjects != doc.base.subjects
                        || existing.base.subjects_state != doc.base.subjects_state
                        || existing.base.deleted_at.is_some()
                        || existing.rendition_key != doc.rendition_key
                    {
                        return Err(StorageError::Conflict("immutable_document_conflict".into()));
                    }
                    return Ok(());
                }
                tx.create(doc.clone())?;
                Ok(())
            })
            .await;
    }
    let head = original_head;
    let mut bindings = crate::bindings! {
        "space" => space.clone(),
        "head" => control_thing(HEAD_TABLE, &head_key(space, &publication.source_id)),
        "head_payload" => serde_json::to_string(&head).map_err(serialization)?,
        "doc" => crate::surreal::thing_from_record_id(&doc.base.id),
        "record" => crate::surreal::doc_to_value(doc)?,
        "assets" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(
            assets.iter().map(native_asset_for_staging).collect::<StorageResult<Vec<_>>>()?
        )),
    };
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    if let Some(producer) = producer {
        producer.append_guard_sql(&mut sql, &mut bindings)?;
    } else if let Some(standing) = &standing {
        standing.append_guard_sql(&mut sql, &mut bindings)?;
    }
    let source_record = crate::canonical_commit::CanonicalRecord::Source(Box::new(current_source));
    crate::canonical_commit::append_row_guard(
        &mut sql,
        &mut bindings,
        "staging_source",
        &source_record,
        Some(&source_record),
    )?;
    sql.push_str("fn::recall_reference_guard({ id:$staging_source_id, space_id:$space },false);\n");
    document_guards.append_guard_sql(&mut sql, &mut bindings)?;
    // The captured present head must participate in native write conflict
    // detection. Rewriting its identical payload is optimized away; this
    // private SCHEMALESS nonce leaves every semantic head field unchanged.
    sql.push_str(
        "LET $head_now = SELECT * FROM ONLY $head;
         IF $head_now.payload != $head_payload { THROW 'stale_source_publication'; };
         UPDATE $head SET canonical_read_nonce = <string>rand::uuid();
         FOR $asset IN $assets {
           LET $existing_asset = SELECT * FROM ONLY $asset.id;
           IF $existing_asset != NONE {
             FOR $field IN object::entries($asset) {
               IF $existing_asset[$field[0]] != $field[1] { THROW 'immutable_attachment_conflict'; };
             };
           } ELSE { CREATE $asset.id CONTENT $asset; };
         };
         LET $old = SELECT * FROM ONLY $doc;
         IF $old != NONE {
           IF $old.space_id != $record.space_id OR $old.source != $record.source
             OR $old.security != $record.security OR $old.subjects != $record.subjects
             OR $old.subjects_state != $record.subjects_state OR $old.deleted_at != NONE
             OR $old.rendition_key != $record.rendition_key { THROW 'immutable_document_conflict'; };
         } ELSE { CREATE $doc CONTENT $record; };
         COMMIT TRANSACTION;",
    );
    storage
        .query_bind_checked(&sql, bindings)
        .await
        .map(|_| ())
        .map_err(crate::source_publication::publication_error)
}

// Repository CONTENT conversion deliberately omits the separately bound ID.
// The staging loop carries whole records, so it must add the typed physical ID
// itself. Keep absent top-level fields for exact retry guards; normalize nested
// NONE fields with the same owner helper used by canonical row guards.
fn native_asset_for_staging(asset: &MediaAsset) -> StorageResult<surrealdb::sql::Value> {
    let surrealdb::sql::Value::Object(mut row) =
        crate::MediaAssetRepository::media_asset_to_value(asset)?
    else {
        return Err(StorageError::DataIntegrityError(
            "invalid media asset storage shape".into(),
        ));
    };
    row.insert(
        "id".into(),
        crate::surreal::thing_from_record_id(&asset.base.id).into(),
    );
    Ok(crate::canonical_commit::native_expected_fields(
        surrealdb::sql::Value::Object(row),
    ))
}

/// Stable create identity for convenience forms without an explicit document ID.
///
/// A caller key separates equal-content creates; source replacement still requires the
/// existing identity and an explicit generation.
pub fn create_document_id(
    space: &str,
    request: &Value,
    control: &IntakeControl,
) -> StorageResult<RecordId> {
    let mut request = request.clone();
    if let Some(object) = request.as_object_mut() {
        object.remove("intake");
    }
    let intent = match &control.idempotency_key {
        Some(key) => key.clone(),
        None => serde_json::to_string(&request).map_err(serialization)?,
    };
    Ok(RecordId::new(
        TableName::Doc,
        stable_uuid(&["retained-document-create", space, &intent]),
    ))
}

/// Shared physical document identity used by intake and the worker renderer.
///
/// The source template ID is input; the original acceptance fence scopes the projected
/// evidence identity to exactly one immutable source operation.
pub fn accepted_document_id(
    publication: &PublicationFence,
    original_doc_id: &str,
) -> StorageResult<RecordId> {
    let bytes = serde_json::to_vec(&(publication, original_doc_id)).map_err(serialization)?;
    Ok(RecordId::new(
        TableName::Doc,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, &bytes),
    ))
}

fn accepted_document_projection(
    source: &Source,
    publication: &PublicationFence,
) -> StorageResult<Option<Doc>> {
    document_projection(source)?
        .map(|mut doc| {
            doc.base.id = accepted_document_id(publication, &doc.base.id.to_string())?;
            doc.rendition_key = format!("accepted:{}", doc.base.id);
            let metadata = doc.metadata.as_object_mut().ok_or_else(|| {
                StorageError::DataIntegrityError("document metadata must be an object".into())
            })?;
            metadata.insert(
                "_publication".into(),
                serde_json::to_value(publication).map_err(serialization)?,
            );
            Ok(doc)
        })
        .transpose()
}

/// Allocate distinct evidence identities even when two sources have equal text.
#[must_use]
pub fn immutable_document_id(space: &str, source: &RecordId, revision: &str) -> RecordId {
    RecordId::new(
        TableName::Doc,
        stable_uuid(&["source-document", space, &source.to_string(), revision]),
    )
}

const INTAKE_ASSETS_KEY: &str = "_intake_assets";

fn accepted_assets(source: &Source) -> StorageResult<Vec<MediaAsset>> {
    source
        .metadata
        .get("_manual_doc")
        .and_then(|doc| doc.get(INTAKE_ASSETS_KEY))
        .map_or_else(
            || Ok(Vec::new()),
            |value| serde_json::from_value(value.clone()).map_err(serialization),
        )
}

async fn capture_managed_assets(
    storage: &StorageClient,
    source: &Source,
    template: Option<&elephant_types::records::ManualDocTemplate>,
    assets: &mut Vec<MediaAsset>,
) -> StorageResult<()> {
    if let Some(doc) = template {
        // Copy an explicitly referenced managed asset into this source's
        // own immutable inventory, so erasure never depends on shared IDs.
        for attachment in &doc.media_attachments {
            if let DocMediaLocation::ManagedAsset { asset_id } = &attachment.location {
                if !assets
                    .iter()
                    .any(|asset| asset.base.id.to_string() == *asset_id)
                {
                    let id = crate::parse_id(asset_id, TableName::MediaAsset)?;
                    let asset = crate::MediaAssetRepository::new(storage.clone())
                        .get_with_payload(&source.base.space_id, &id)
                        .await?
                        .ok_or_else(|| {
                            StorageError::NotFound("managed source attachment".into())
                        })?;
                    if asset.base.security != source.base.security
                        || asset.base.subjects != source.base.subjects
                        || asset.base.subjects_state != source.base.subjects_state
                    {
                        return Err(StorageError::Conflict(
                            "attachment_envelope_mismatch".into(),
                        ));
                    }
                    assets.push(asset);
                }
            }
        }
    }
    Ok(())
}

fn normalize_assets(
    source: &mut Source,
    template: &mut Option<elephant_types::records::ManualDocTemplate>,
    assets: &mut Vec<MediaAsset>,
    revision: &str,
) -> StorageResult<()> {
    let Some(doc) = template else {
        if !assets.is_empty() {
            return Err(StorageError::DataIntegrityError(
                "attachments require a document template".into(),
            ));
        }
        return Ok(());
    };
    let mut ordered = Vec::new();
    for (index, attachment) in doc.media_attachments.iter_mut().enumerate() {
        if let DocMediaLocation::ManagedAsset { asset_id } = &mut attachment.location {
            let mut asset = assets
                .iter()
                .find(|asset| asset.base.id.to_string() == *asset_id)
                .cloned()
                .ok_or_else(|| {
                    StorageError::DataIntegrityError("missing recoverable attachment".into())
                })?;
            if asset.payload_base64.is_none() {
                return Err(StorageError::DataIntegrityError(
                    "managed source attachment requires recoverable bytes".into(),
                ));
            }
            if asset.kind != attachment.kind || asset.media_type != attachment.media_type {
                return Err(StorageError::DataIntegrityError(
                    "attachment media mismatch".into(),
                ));
            }
            let id = RecordId::new(
                TableName::MediaAsset,
                stable_uuid(&[
                    "source-asset",
                    &source.base.space_id,
                    &source.base.id.to_string(),
                    revision,
                    &index.to_string(),
                ]),
            );
            asset.base = source.base.clone();
            asset.base.id = id.clone();
            // Stable nested audit fields are part of the immutable input. The
            // enclosing source operation supplies the actual acceptance time.
            asset.base.created_at = chrono::DateTime::UNIX_EPOCH;
            asset.base.updated_at = chrono::DateTime::UNIX_EPOCH;
            asset.base.created_by = "source_intake".into();
            asset.base.deleted_at = None;
            asset.storage_path =
                elephant_types::records::media_asset::managed_asset_relative_path(&id);
            *asset_id = id.to_string();
            attachment.attachment_id = format!("source-attachment-{index}");
            attachment.sha256 = Some(asset.sha256.clone());
            attachment.size_bytes = Some(asset.size_bytes);
            ordered.push(asset);
        }
    }
    if ordered.len() != assets.len() {
        return Err(StorageError::DataIntegrityError(
            "unreferenced or duplicate source attachment".into(),
        ));
    }
    *assets = ordered;
    source.set_manual_doc_template(doc)?;
    if !assets.is_empty() {
        source.metadata["_manual_doc"][INTAKE_ASSETS_KEY] =
            serde_json::to_value(&assets).map_err(serialization)?;
        // Legacy manual builders included random attachment IDs in this hash.
        // Canonicalize before acceptance so equivalent requests retry cleanly.
        if source.type_ == elephant_types::records::SourceType::Manual {
            let mut content = serde_json::to_value(doc).map_err(serialization)?;
            if let Some(object) = content.as_object_mut() {
                object.remove("doc_id");
                object.remove("rendition_key");
            }
            content.sort_all_objects();
            source.hash = stable_uuid(&[&serde_json::to_string(&content).map_err(serialization)?])
                .to_string();
        }
    }
    Ok(())
}

fn document_projection(source: &Source) -> StorageResult<Option<Doc>> {
    source
        .manual_doc_template()?
        .map(|template| {
            let mut base = source.base.clone();
            base.id = crate::parse_id(&template.doc_id, TableName::Doc)?;
            Ok(Doc {
                base,
                title: template.title,
                text: template.text,
                sidecar_text: template.sidecar_text,
                source: Some(Link::new(source.base.id.clone())),
                media_attachments: template.media_attachments,
                rendition_key: template.rendition_key,
                metadata: template.metadata,
            })
        })
        .transpose()
}

fn fingerprint(source: &Source) -> StorageResult<String> {
    let mut value = serde_json::to_value(source).map_err(serialization)?;
    if let Some(object) = value.as_object_mut() {
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
    if let Some(metadata) = value.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.remove("observed_at");
        if let Some(doc) = metadata
            .get_mut("_manual_doc")
            .and_then(Value::as_object_mut)
        {
            doc.remove("doc_id");
            doc.remove("rendition_key");
            if let Some(metadata) = doc.get_mut("metadata").and_then(Value::as_object_mut) {
                metadata.remove("observed_at");
            }
        }
    }
    value.sort_all_objects();
    Ok(stable_uuid(&[&serde_json::to_string(&value).map_err(serialization)?]).to_string())
}

fn stable_uuid(parts: &[&str]) -> Uuid {
    let mut tuple = String::new();
    for part in parts {
        let _ = write!(tuple, "{}:{part}", part.len());
    }
    Uuid::new_v5(&Uuid::NAMESPACE_OID, tuple.as_bytes())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "This error adapter is passed directly to Result::map_err, whose callback receives the owned serialization error."
)]
fn serialization(error: serde_json::Error) -> StorageError {
    StorageError::SerializationError(error.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::{Repository, SpaceConfigRepository, StorageConfig};
    use elephant_types::records::base::{BaseFields, SubjectsState};
    use elephant_types::records::{ManualDocTemplate, SourceType, SpaceConfig};

    pub(super) async fn backend(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new(StorageConfig::default()).await
        }
        .expect("backend");
        if !embedded {
            storage
                .query_checked(include_str!(
                    "../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .expect("publication schema");
            crate::reference_fence::install_native_reference_guards(&storage, &[])
                .await
                .expect("actual native reference and physical-key guards");
        }
        for space in ["household", "research"] {
            SpaceConfigRepository::new(storage.clone())
                .create(SpaceConfig {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::SpaceConfig),
                        space.into(),
                        "fixture".into(),
                    ),
                    embedding_provider: "fake".into(),
                    embedding_model: "fixture".into(),
                    embedding_dims: 8,
                    processing: None,
                    purpose: None,
                    skill: None,
                })
                .await
                .expect("named space");
        }
        storage
    }

    #[test]
    fn public_source_does_not_expose_private_recovery_input() {
        let mut original = source("alpha", "private document text");
        // This fixture helper uses text inside a fake hash for identity tests.
        // The public projection intentionally retains the real content hash;
        // use an opaque fixture digest to isolate the private recovery payload.
        original.hash = "opaque-fixture-digest".into();
        original.metadata["_manual_doc"]["_intake_assets"] =
            serde_json::json!([{"data": "private media bytes"}]);
        let projected = public_source_projection(original.clone());
        assert!(projected.metadata.get("_manual_doc").is_none());
        assert_eq!(projected.metadata["observed_at"], "first");
        assert_eq!(projected.base, original.base);
        assert!(original.metadata.get("_manual_doc").is_some());
        let serialized = serde_json::to_string(&projected).expect("projection");
        assert!(!serialized.contains("private document text"));
        assert!(!serialized.contains("private media bytes"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete fault and recovery sequence together with its original assertions."
    )]
    async fn native_real_document_staging_changes_only_private_head_fence_and_refuses_stale_input()
    {
        use crate::source_publication::{control_thing, head_key, HEAD_TABLE};
        #[derive(Debug, Deserialize, PartialEq)]
        struct NativeHead {
            payload: String,
            canonical_read_nonce: Option<String>,
        }
        async fn native_head(storage: &StorageClient, source: &RecordId) -> NativeHead {
            let mut response = storage.query_bind_checked(
                "SELECT payload,canonical_read_nonce FROM ONLY $head;",
                crate::bindings! {"head" => control_thing(HEAD_TABLE, &head_key("household", source))},
            ).await.unwrap();
            response.take::<Option<NativeHead>>(0).unwrap().unwrap()
        }
        let storage = Box::pin(backend(false)).await;
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            source("household", "First immutable input"),
            IntakeControl::default(),
        ))
        .await
        .unwrap();
        let accepted = Box::pin(prepared.accept(&storage)).await.unwrap();
        let publications = SourcePublicationRepository::new(storage.clone());
        let sources = SourceRepository::new(storage.clone());
        let documents = crate::DocRepository::new(storage.clone());
        let doc_id = &accepted.document.as_ref().unwrap().base.id;
        let original_source = sources
            .get("household", &accepted.source.base.id)
            .await
            .unwrap();
        let original_doc = documents.get("household", doc_id).await.unwrap();
        let original_head = publications
            .head("household", &accepted.source.base.id)
            .await
            .unwrap();
        let before = Box::pin(native_head(&storage, &accepted.source.base.id)).await;
        Box::pin(stage_source_revision(
            &storage,
            &accepted.source,
            &accepted.publication,
        ))
        .await
        .unwrap();
        let after = Box::pin(native_head(&storage, &accepted.source.base.id)).await;
        assert!(after.canonical_read_nonce.is_some());
        assert_ne!(after.canonical_read_nonce, before.canonical_read_nonce,
            "the actual production staging transaction must make a real native write even when the Doc exists");
        assert_eq!(after.payload, before.payload);
        assert_eq!(
            publications
                .head("household", &accepted.source.base.id)
                .await
                .unwrap(),
            original_head
        );
        assert_eq!(
            sources
                .get("household", &accepted.source.base.id)
                .await
                .unwrap(),
            original_source
        );
        assert_eq!(
            documents.get("household", doc_id).await.unwrap(),
            original_doc
        );
        assert_eq!(
            publications
                .operation("household", &accepted.operation.operation_id)
                .await
                .unwrap(),
            Some(accepted.operation.clone())
        );

        let mut replacement = accepted.source.clone();
        let mut template = replacement.manual_doc_template().unwrap().unwrap();
        template.text = "New immutable input".into();
        replacement.hash = "new-immutable-input-hash".into();
        replacement.set_manual_doc_template(&template).unwrap();
        let next = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            replacement,
            IntakeControl {
                revision_id: Some("next-real-staging-head".into()),
                idempotency_key: Some("next-real-staging-head".into()),
                expected_generation: Some(accepted.publication.generation.clone()),
            },
        ))
        .await
        .unwrap();
        let next = Box::pin(next.accept(&storage)).await.unwrap();
        // A missing historical projection is a recovery fault, not an erasure
        // or publication oracle. Its stale original input must not recreate it.
        documents.hard_delete("household", doc_id).await.unwrap();
        let newer_head = Box::pin(native_head(&storage, &accepted.source.base.id)).await;
        let newer_source = sources
            .get("household", &accepted.source.base.id)
            .await
            .unwrap();
        assert!(matches!(
            Box::pin(stage_source_revision(
                &storage,
                &accepted.source,
                &accepted.publication
            ))
            .await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            Box::pin(native_head(&storage, &accepted.source.base.id)).await,
            newer_head
        );
        assert_eq!(
            sources
                .get("household", &accepted.source.base.id)
                .await
                .unwrap(),
            newer_source
        );
        assert!(documents.get("household", doc_id).await.unwrap().is_none());
        assert_eq!(
            documents
                .get("household", &next.document.as_ref().unwrap().base.id)
                .await
                .unwrap(),
            next.document
        );
        assert_eq!(
            publications
                .operation("household", &next.operation.operation_id)
                .await
                .unwrap(),
            Some(next.operation)
        );
    }

    pub(super) fn source(space: &str, text: &str) -> Source {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "producer".into(),
            ),
            SourceType::Manual,
            format!("hash-{text}"),
        );
        source.external_id = Some("upstream:stable-document".into());
        source.base.security.level = 3;
        source.base.security.labels = vec!["family".into()];
        source.base.security.handling = vec!["restricted".into()];
        source.base.subjects_state = SubjectsState::Partial;
        source.metadata = serde_json::json!({"observed_at":"first"});
        source.set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(), title: "Source note".into(), text: text.into(),
            sidecar_text: None, media_attachments: vec![], rendition_key: Uuid::new_v4().to_string(),
            metadata: serde_json::json!({"extraction_skill":"extraction/home","extraction_purpose":"source facts","observed_at":"first"}),
        }).expect("template");
        source
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete fault and recovery sequence together with its original assertions."
    )]
    async fn unavailable_contact_retry_contract(embedded: bool) {
        let storage = backend(embedded).await;
        if !embedded {
            for schema in [
                include_str!("../../migrations/sql/039_private_reference_fence.surql"),
                include_str!("../../migrations/sql/040_source_contact_intent.surql"),
            ] {
                storage.query_checked(schema).await.unwrap();
            }
        }
        let mut input = source("household", "Surviving accepted input");
        input.metadata["contact"] = serde_json::json!({
            "name":"Surviving Person", "email":"survivor@example.invalid"
        });
        let control = IntakeControl {
            revision_id: Some("original-sealed".into()),
            idempotency_key: Some("original-sealed".into()),
            expected_generation: None,
        };
        let prepared = PreparedSourceIntake::prepare(&storage, input.clone(), control.clone())
            .await
            .unwrap();
        let contact = prepared.prepare_contact(&storage).await.unwrap().unwrap();
        let first = prepared
            .accept_with_prepared_contact(&storage, &contact)
            .await
            .unwrap();
        assert_eq!(first.operation.state, OperationState::Accepted);
        let doc_id = first.document.as_ref().unwrap().base.id.clone();
        let repo = SourcePublicationRepository::new(storage.clone());
        let head = repo.head("household", &first.source.base.id).await.unwrap();
        // A storage-loss fault, not a substitute for F/E's real cross-source
        // erasure oracle. Remove exactly the original private plan and staged
        // Doc to prove that a pending semantic receipt cannot recreate either.
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    let rows = tx.scan_extension_rows("source_contact_intent")?;
                    assert_eq!(rows.len(), 1);
                    tx.hard_delete_extension_row("source_contact_intent", &rows[0].0, "household")
                        .map_err(Into::into)
                })
                .await
                .unwrap();
        } else {
            storage
                .query_bind_checked(
                    "DELETE source_contact_intent WHERE space_id = $space;",
                    crate::bindings! {"space" => "household"},
                )
                .await
                .unwrap();
        }
        crate::DocRepository::new(storage.clone())
            .hard_delete("household", &doc_id)
            .await
            .unwrap();
        let retry = PreparedSourceIntake::prepare(&storage, input.clone(), control.clone())
            .await
            .unwrap();
        assert!(matches!(retry.prepare_contact(&storage).await,
            Err(StorageError::Conflict(message)) if message == "accepted_contact_plan_unavailable"));
        let read_only = retry
            .read_only_contact_retry(&storage)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_only.operation, first.operation);
        assert_eq!(read_only.source, first.source);
        assert_eq!(read_only.document, first.document);
        assert_eq!(read_only.publication, first.publication);
        assert!(!read_only.needs_processing());
        let direct = retry.accept(&storage).await.unwrap();
        assert_eq!(direct.operation, first.operation);
        assert!(!direct.needs_processing());
        assert!(crate::DocRepository::new(storage.clone())
            .get_with_deleted("household", &doc_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            repo.head("household", &first.source.base.id).await.unwrap(),
            head
        );
        assert_eq!(
            repo.operation("household", &first.operation.operation_id)
                .await
                .unwrap(),
            Some(first.operation.clone())
        );
        assert!(repo
            .accepted_contact_for_publication("household", &first.publication)
            .await
            .is_err());
        let capability = repo
            .accepted_contact_capability_for_publication("household", &first.publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capability.digest(), contact.digest());
        let mut changed = input.clone();
        changed.hash = "changed-semantic-input".into();
        if let Ok(changed) = PreparedSourceIntake::prepare(&storage, changed, control).await {
            assert!(changed.read_only_contact_retry(&storage).await.is_err());
        }
        // A new explicit CAS/key may prepare a fresh authorized plan from the
        // surviving input. Admission capability survives original plan loss.
        let fresh = PreparedSourceIntake::prepare(
            &storage,
            input,
            IntakeControl {
                revision_id: Some("fresh-after-loss".into()),
                idempotency_key: Some("fresh-after-loss".into()),
                expected_generation: Some(first.publication.generation.clone()),
            },
        )
        .await
        .unwrap();
        let fresh_contact = fresh
            .prepare_replacement_contact(&storage)
            .await
            .unwrap()
            .unwrap();
        let replacement = fresh
            .accept_with_prepared_contact(&storage, &fresh_contact)
            .await
            .unwrap();
        assert_ne!(
            replacement.operation.operation_id,
            first.operation.operation_id
        );
        assert_ne!(
            replacement.publication.generation,
            first.publication.generation
        );
        assert!(replacement.needs_processing());
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_unavailable_contact_retry_is_read_only() {
        unavailable_contact_retry_contract(true).await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_unavailable_contact_retry_is_read_only() {
        unavailable_contact_retry_contract(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn legacy_document_identity_contract(embedded: bool) {
        let storage = backend(embedded).await;
        let mut first = source("household", "first input");
        first.external_id = None;
        let document = document_projection(&first).unwrap().unwrap();
        bind_legacy_document_source_identity(&storage, &mut first, &document)
            .await
            .unwrap();
        assert_eq!(first.external_id, Some(document.base.id.to_string()));
        let accepted = PreparedSourceIntake::prepare(&storage, first, IntakeControl::default())
            .await
            .unwrap()
            .accept(&storage)
            .await
            .unwrap();
        let mut changed = source("household", "different input and generated source ID");
        changed.external_id = None;
        bind_legacy_document_source_identity(&storage, &mut changed, &document)
            .await
            .unwrap();
        assert_eq!(changed.base.id, accepted.source.base.id);
        let prepared = PreparedSourceIntake::prepare(
            &storage,
            changed,
            IntakeControl {
                revision_id: Some("legacy-doc-r2".into()),
                idempotency_key: Some("legacy-doc-r2".into()),
                expected_generation: Some(accepted.publication.generation.clone()),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            prepared.previous.as_ref().unwrap().base.security,
            accepted.source.base.security
        );
        let replacement = prepared.accept(&storage).await.unwrap();
        assert_eq!(replacement.source.base.id, accepted.source.base.id);
        assert_ne!(
            replacement.document.as_ref().unwrap().base.id,
            accepted.document.as_ref().unwrap().base.id
        );

        // An explicitly addressed existing source retains its original identity
        // even when the caller omits its external alias in the legacy body.
        let mut explicit = replacement.source.clone();
        explicit.external_id = None;
        let unrelated_document = document_projection(&source("household", "new caller doc ID"))
            .unwrap()
            .unwrap();
        bind_legacy_document_source_identity(&storage, &mut explicit, &unrelated_document)
            .await
            .unwrap();
        assert_eq!(explicit.base.id, replacement.source.base.id);
        assert_eq!(explicit.external_id, replacement.source.external_id);
        let mut foreign = unrelated_document;
        foreign.base.space_id = "research".into();
        assert!(
            bind_legacy_document_source_identity(&storage, &mut explicit, &foreign)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_legacy_doc_identity_survives_changed_content() {
        legacy_document_identity_contract(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_legacy_doc_identity_survives_changed_content() {
        legacy_document_identity_contract(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn retry_and_replacement_contract(embedded: bool) {
        let storage = backend(embedded).await;
        let first = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Original facts"),
            IntakeControl::default(),
        )
        .await
        .expect("prepare")
        .accept(&storage)
        .await
        .expect("accept");
        let mut replay = source("household", "Original facts");
        replay.metadata["observed_at"] = Value::String("new observation".into());
        let mut template = replay
            .manual_doc_template()
            .expect("template")
            .expect("doc");
        template.metadata["observed_at"] = Value::String("new observation".into());
        replay.set_manual_doc_template(&template).expect("template");
        let replay = PreparedSourceIntake::prepare(&storage, replay, IntakeControl::default())
            .await
            .expect("prepare retry")
            .accept(&storage)
            .await
            .expect("retry");
        assert_eq!(first.operation, replay.operation);
        assert_eq!(first.publication, replay.publication);
        assert_eq!(
            first.document.as_ref().unwrap().base.id,
            replay.document.as_ref().unwrap().base.id
        );
        assert_eq!(replay.source.metadata["observed_at"], "first");
        let blind = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Replacement facts"),
            IntakeControl::default(),
        )
        .await
        .expect("prepare blind replacement")
        .accept(&storage)
        .await;
        assert!(matches!(blind, Err(StorageError::Conflict(_))));
        let control = IntakeControl {
            revision_id: Some("r2".into()),
            idempotency_key: Some("replace-2".into()),
            expected_generation: Some(first.publication.generation.clone()),
        };
        let next = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Replacement facts"),
            control.clone(),
        )
        .await
        .expect("prepare replacement")
        .accept(&storage)
        .await
        .expect("replace");
        assert_eq!(next.source.base.id, first.source.base.id);
        assert_ne!(
            next.document.as_ref().unwrap().base.id,
            first.document.as_ref().unwrap().base.id
        );
        assert_ne!(next.publication.generation, first.publication.generation);
        let original = SourcePublicationRepository::new(storage.clone())
            .revision(
                "household",
                &first.source.base.id,
                &first.publication.revision_id,
            )
            .await
            .expect("history")
            .expect("immutable original");
        assert_eq!(
            original.source.manual_doc_template().unwrap().unwrap().text,
            "Original facts"
        );
        let other_space = PreparedSourceIntake::prepare(
            &storage,
            source("research", "Original facts"),
            IntakeControl::default(),
        )
        .await
        .expect("prepare other space")
        .accept(&storage)
        .await
        .expect("accept other space");
        assert_ne!(other_space.source.base.id, first.source.base.id);
        let doc = next.document.unwrap();
        assert_eq!(doc.base.security, next.source.base.security);
        assert_eq!(doc.base.subjects_state, SubjectsState::Partial);
        assert_eq!(doc.metadata["extraction_skill"], "extraction/home");
        let serialized = serde_json::to_string(&next.operation).expect("public receipt");
        assert!(!serialized.contains("Replacement facts"));
        assert!(!serialized.contains("upstream:stable-document"));
    }

    #[tokio::test]
    async fn embedded_intake_retries_cas_and_immutable_evidence() {
        retry_and_replacement_contract(true).await;
    }

    #[tokio::test]
    async fn surreal_intake_retries_cas_and_immutable_evidence() {
        retry_and_replacement_contract(false).await;
    }

    #[test]
    fn intake_identity_ignores_object_order_but_preserves_array_order_and_values() {
        let mut first = source("household", "Original facts");
        first.metadata["guidance"] =
            serde_json::from_str(r#"{"z":{"b":null,"a":[1,2]},"a":"keep"}"#).unwrap();
        let mut reordered = first.clone();
        reordered.metadata["guidance"] =
            serde_json::from_str(r#"{"a":"keep","z":{"a":[1,2],"b":null}}"#).unwrap();
        assert_eq!(
            fingerprint(&first).unwrap(),
            fingerprint(&reordered).unwrap()
        );
        reordered.metadata["guidance"]["z"]["a"] = serde_json::json!([2, 1]);
        assert_ne!(
            fingerprint(&first).unwrap(),
            fingerprint(&reordered).unwrap()
        );
        reordered.metadata["guidance"] = first.metadata["guidance"].clone();
        reordered.metadata["guidance"]["z"]["b"] = "null".into();
        assert_ne!(
            fingerprint(&first).unwrap(),
            fingerprint(&reordered).unwrap()
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn utf8_byte_limit_is_checked_before_acceptance() {
        let storage = backend(true).await;
        let text = "é".repeat(MAX_SOURCE_TEXT_BYTES / 2 + 1);
        let result = PreparedSourceIntake::prepare(
            &storage,
            source("household", &text),
            IntakeControl::default(),
        )
        .await;
        assert!(matches!(result, Err(StorageError::DataIntegrityError(_))));
        let head = SourcePublicationRepository::new(storage)
            .external_head("household", SourceType::Manual, "upstream:stable-document")
            .await
            .expect("head");
        assert!(head.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn authorized_reclassification_requires_unchanged_preflight_snapshot() {
        let storage = backend(true).await;
        let accepted = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Protected"),
            IntakeControl::default(),
        )
        .await
        .unwrap()
        .accept(&storage)
        .await
        .unwrap();
        let mut downgraded = source("household", "Changed");
        downgraded.base.security.handling.clear();
        let prepared = PreparedSourceIntake::prepare(
            &storage,
            downgraded,
            IntakeControl {
                revision_id: Some("r2".into()),
                idempotency_key: Some("r2".into()),
                expected_generation: Some(accepted.publication.generation),
            },
        )
        .await
        .unwrap();
        let mut changed_current = accepted.source.clone();
        changed_current
            .base
            .security
            .labels
            .push("new-policy".into());
        SourceRepository::new(storage.clone())
            .update(
                "household",
                &changed_current.base.id.clone(),
                changed_current,
            )
            .await
            .expect("concurrent reclassification");
        assert!(matches!(
            prepared.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn document_preflight_race(embedded: bool) {
        let storage = backend(embedded).await;
        let accepted = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Original"),
            IntakeControl::default(),
        )
        .await
        .unwrap()
        .accept(&storage)
        .await
        .unwrap();
        stage_accepted_document(&storage, &accepted).await.unwrap();
        let observed = accepted.document.clone().unwrap();
        let prepared = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Replacement"),
            IntakeControl {
                revision_id: Some("guarded-doc-r2".into()),
                idempotency_key: Some("guarded-doc-r2".into()),
                expected_generation: Some(accepted.publication.generation.clone()),
            },
        )
        .await
        .unwrap()
        .with_authorized_document(observed.clone());
        let mut changed = observed.clone();
        changed
            .base
            .security
            .handling
            .push("new-document-restriction".into());
        crate::DocRepository::new(storage.clone())
            .update("household", &observed.base.id, changed)
            .await
            .unwrap();
        assert!(matches!(
            prepared.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
        assert!(matches!(
            retract_authorized_source_document(
                &storage,
                &accepted.source,
                IntakeControl {
                    revision_id: None,
                    idempotency_key: Some("stale-document-retract".into()),
                    expected_generation: Some(accepted.publication.generation.clone()),
                },
                "test",
                Some(&observed),
            )
            .await,
            Err(StorageError::Conflict(_))
        ));
        let authority = SourcePublicationRepository::new(storage);
        assert_eq!(
            authority
                .head("household", &accepted.source.base.id)
                .await
                .unwrap()
                .unwrap()
                .generation,
            accepted.publication.generation
        );
        assert!(authority
            .revision("household", &accepted.source.base.id, "guarded-doc-r2")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn embedded_document_preflight_is_checked_at_acceptance() {
        document_preflight_race(true).await;
    }

    #[tokio::test]
    async fn surreal_document_preflight_is_checked_at_acceptance() {
        document_preflight_race(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn automatic_current_document_guard(embedded: bool) {
        let storage = backend(embedded).await;
        let first = PreparedSourceIntake::prepare(
            &storage,
            source("household", "First"),
            IntakeControl::default(),
        )
        .await
        .unwrap()
        .accept(&storage)
        .await
        .unwrap();
        let old = first.document.clone().unwrap();
        let prepared = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Replacement"),
            IntakeControl {
                revision_id: Some("auto-doc-r2".into()),
                idempotency_key: Some("auto-doc-r2".into()),
                expected_generation: Some(first.publication.generation.clone()),
            },
        )
        .await
        .unwrap();
        assert_eq!(prepared.previous_documents, vec![old.clone()]);
        let mut changed = old.clone();
        changed.base.security.level += 1;
        crate::DocRepository::new(storage.clone())
            .update("household", &old.base.id, changed.clone())
            .await
            .unwrap();
        assert!(matches!(
            prepared.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
        let repository = SourcePublicationRepository::new(storage.clone());
        assert_eq!(
            repository
                .head("household", &first.source.base.id)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            first.operation.operation_id
        );
        assert!(repository
            .revision("household", &first.source.base.id, "auto-doc-r2")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            crate::DocRepository::new(storage)
                .get("household", &old.base.id)
                .await
                .unwrap()
                .unwrap(),
            changed
        );
    }

    #[tokio::test]
    async fn embedded_intake_guards_current_physical_doc_without_caller_id_match() {
        automatic_current_document_guard(true).await;
    }
    #[tokio::test]
    async fn surreal_intake_guards_current_physical_doc_without_caller_id_match() {
        automatic_current_document_guard(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn current_document_absence_guard(embedded: bool) {
        let storage = backend(embedded).await;
        let prepared = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Original"),
            IntakeControl::default(),
        )
        .await
        .unwrap();
        let repository = SourcePublicationRepository::new(storage.clone());
        // Exercise an actual accepted operation whose physical staging failed
        // before any document write; do not fabricate a head or operation.
        let operation = repository.accept(prepared.request.clone()).await.unwrap();
        let original = PublicationFence {
            source_id: operation.source_id.clone(),
            generation: operation.source_generation.clone(),
            revision_id: operation.desired_revision.clone().unwrap(),
            operation_id: operation.operation_id.clone(),
        };
        let expected = accepted_document_projection(&prepared.source, &original)
            .unwrap()
            .unwrap();
        let control = IntakeControl {
            revision_id: Some("after-unstaged".into()),
            idempotency_key: Some("after-unstaged".into()),
            expected_generation: Some(original.generation.clone()),
        };
        let next = PreparedSourceIntake::prepare(
            &storage,
            source("household", "Replacement"),
            control.clone(),
        )
        .await
        .unwrap();
        assert!(next.previous_documents.is_empty());
        assert_eq!(next.absent_documents, vec![expected.base.id.clone()]);
        let original_doc = crate::DocRepository::new(storage.clone())
            .create(expected)
            .await
            .unwrap();
        assert!(matches!(
            next.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            repository
                .head("household", &operation.source_id)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            operation.operation_id
        );
        assert!(repository
            .operation_by_idempotency_key("household", "after-unstaged")
            .await
            .unwrap()
            .is_none());
        let fresh =
            PreparedSourceIntake::prepare(&storage, source("household", "Replacement"), control)
                .await
                .unwrap();
        assert_eq!(fresh.previous_documents, vec![original_doc.clone()]);
        fresh.accept(&storage).await.unwrap();
        assert_eq!(
            crate::DocRepository::new(storage)
                .get("household", &original_doc.base.id)
                .await
                .unwrap()
                .unwrap(),
            original_doc
        );
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_intake_guards_unstaged_current_document_absence() {
        current_document_absence_guard(true).await;
    }
    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_intake_guards_unstaged_current_document_absence() {
        current_document_absence_guard(false).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn sourceless_caller_document_guard(embedded: bool) {
        let storage = backend(embedded).await;
        let mut input = source("household", "Manual graph document");
        let mut document = document_projection(&input).unwrap().unwrap();
        document.source = None;
        let documents = crate::DocRepository::new(storage.clone());
        let observed = documents.create(document).await.unwrap();
        input.external_id = None;
        bind_legacy_document_source_identity(&storage, &mut input, &observed)
            .await
            .unwrap();
        let prepared =
            PreparedSourceIntake::prepare(&storage, input.clone(), IntakeControl::default())
                .await
                .unwrap()
                .with_authorized_document(observed.clone());
        let mut changed = observed.clone();
        changed.base.security.level += 1;
        let changed = documents
            .update("household", &observed.base.id, changed)
            .await
            .unwrap();
        assert!(matches!(
            prepared.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
        assert!(SourcePublicationRepository::new(storage.clone())
            .head("household", &input.base.id)
            .await
            .unwrap()
            .is_none());
        // A newly authorized observation is admitted without a fake source on
        // the independent manual graph document or an in-place overwrite.
        input.base.security.clone_from(&changed.base.security);
        let accepted = PreparedSourceIntake::prepare(&storage, input, IntakeControl::default())
            .await
            .unwrap()
            .with_authorized_document(changed.clone())
            .accept(&storage)
            .await
            .unwrap();
        assert_ne!(accepted.document.unwrap().base.id, observed.base.id);
        assert_eq!(
            documents
                .get("household", &observed.base.id)
                .await
                .unwrap()
                .unwrap(),
            changed
        );
    }
    #[tokio::test]
    async fn embedded_intake_guards_sourceless_caller_document() {
        sourceless_caller_document_guard(true).await;
    }
    #[tokio::test]
    async fn surreal_intake_guards_sourceless_caller_document() {
        sourceless_caller_document_guard(false).await;
    }

    #[test]
    fn controls_require_complete_explicit_replacement_intent() {
        assert!(
            IntakeControl::from_params(&serde_json::json!({"intake":{"unexpected":true}})).is_err()
        );
        assert!(IntakeControl {
            expected_generation: Some("generation".into()),
            ..IntakeControl::default()
        }
        .validate()
        .is_ok()); // Lifecycle controls do not require a replacement revision.
        let lifecycle = IntakeControl::from_params(&serde_json::json!({"intake":{
            "expected_generation":"generation", "idempotency_key":"retract-1"
        }}))
        .unwrap();
        assert!(lifecycle.validate_acceptance().is_err());
        assert!(IntakeControl::from_params(&serde_json::json!({"intake":{
            "expected_generation":"generation", "idempotency_key":"replace-1", "revision_id":"r2"
        }}))
        .unwrap()
        .validate_acceptance()
        .is_ok());
        assert!(IntakeControl {
            revision_id: Some(" ".into()),
            ..IntakeControl::default()
        }
        .validate()
        .is_err());
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete fault and recovery sequence together with its original assertions."
    )]
    async fn media_staging_contract(embedded: bool) {
        use elephant_types::records::{DocMediaAttachment, DocMediaKind};
        let storage = backend(embedded).await;
        let make = || {
            let mut input = source("household", "image context");
            let id = RecordId::new_v4(TableName::MediaAsset);
            let mut base = input.base.clone();
            base.id = id.clone();
            let asset = MediaAsset {
                base,
                kind: DocMediaKind::Image,
                media_type: "image/png".into(),
                storage_path: "uncommitted-input".into(),
                size_bytes: 3,
                sha256: "fixture-digest".into(),
                payload_base64: Some("AQID".into()),
                duration_ms: None,
                label: None,
            };
            let mut template = input.manual_doc_template().unwrap().unwrap();
            template.media_attachments.push(DocMediaAttachment {
                attachment_id: Uuid::new_v4().to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/png".into(),
                location: DocMediaLocation::ManagedAsset {
                    asset_id: id.to_string(),
                },
                label: None,
                size_bytes: Some(3),
                duration_ms: None,
                sha256: Some("fixture-digest".into()),
            });
            input.set_manual_doc_template(&template).unwrap();
            (input, asset)
        };
        let (source, asset) = make();
        let native = native_asset_for_staging(&asset).unwrap();
        let surrealdb::sql::Value::Object(native) = native else {
            unreachable!()
        };
        assert_eq!(
            native.get("id"),
            Some(&surrealdb::sql::Value::Thing(
                crate::surreal::thing_from_record_id(&asset.base.id)
            ))
        );
        assert_eq!(
            native.get("payload_base64"),
            Some(&surrealdb::sql::Value::from("AQID"))
        );
        assert_eq!(native.get("deleted_at"), Some(&surrealdb::sql::Value::None));
        let prepared = PreparedSourceIntake::prepare_with_assets(
            &storage,
            source,
            IntakeControl::default(),
            &[asset],
        )
        .await
        .unwrap();
        let staged = accepted_assets(&prepared.source).unwrap();
        assert!(crate::MediaAssetRepository::new(storage.clone())
            .get("household", &staged[0].base.id)
            .await
            .unwrap()
            .is_none());
        let accepted = prepared.accept(&storage).await.unwrap();
        let assets = accepted_assets(&accepted.source).unwrap();
        let loaded = crate::MediaAssetRepository::new(storage.clone())
            .get_with_payload("household", &assets[0].base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.payload_base64.as_deref(), Some("AQID"));
        let (source, asset) = make();
        let replay = PreparedSourceIntake::prepare_with_assets(
            &storage,
            source,
            IntakeControl::default(),
            &[asset],
        )
        .await
        .unwrap()
        .accept(&storage)
        .await
        .unwrap();
        assert_eq!(accepted.operation, replay.operation);
        assert_eq!(accepted.document, replay.document);
        assert!(!serde_json::to_string(&accepted.operation)
            .unwrap()
            .contains("AQID"));
        let (mut source, asset) = make();
        source.uri = Some("changed-intent".into());
        let stale = PreparedSourceIntake::prepare_with_assets(
            &storage,
            source,
            IntakeControl {
                revision_id: Some("stale-media".into()),
                idempotency_key: Some("stale-media".into()),
                expected_generation: Some("wrong-generation".into()),
            },
            &[asset],
        )
        .await
        .unwrap();
        let rejected_asset = accepted_assets(&stale.source).unwrap().remove(0);
        assert!(matches!(
            stale.accept(&storage).await,
            Err(StorageError::Conflict(_))
        ));
        assert!(crate::MediaAssetRepository::new(storage)
            .get("household", &rejected_asset.base.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn embedded_attachments_are_durable_scoped_and_never_written_before_acceptance() {
        media_staging_contract(true).await;
    }

    #[tokio::test]
    #[expect(
        clippy::large_futures,
        reason = "Keep the conformance helper's original await and allocation shape."
    )]
    async fn surreal_attachments_are_durable_scoped_and_never_written_before_acceptance() {
        media_staging_contract(false).await;
    }

    #[tokio::test]
    async fn incomplete_replacement_controls_fail_before_head_mutation() {
        let storage = backend(true).await;
        assert!(PreparedSourceIntake::prepare(
            &storage,
            source("household", "facts"),
            IntakeControl {
                expected_generation: Some("known".into()),
                ..IntakeControl::default()
            }
        )
        .await
        .is_err());
    }
    #[test]
    fn source_document_attachment_preserves_both_restrictions_and_guidance() {
        let mut source = source("household", "facts");
        source.metadata["upstream"] = serde_json::json!("source metadata");
        let mut doc = document_projection(&source).unwrap().unwrap();
        doc.base.security.level = 6;
        doc.base.security.handling.push("no-export".into());
        doc.base
            .subjects
            .push(Link::new(RecordId::new_v4(TableName::Entity)));
        attach_document(&mut source, &mut doc).unwrap();
        assert_eq!(source.base.security, doc.base.security);
        assert_eq!(source.base.security.level, 6);
        assert!(source
            .base
            .security
            .handling
            .iter()
            .any(|label| label == "restricted"));
        assert!(source
            .base
            .security
            .handling
            .iter()
            .any(|label| label == "no-export"));
        assert_eq!(source.base.subjects, doc.base.subjects);
        assert_eq!(source.metadata["upstream"], "source metadata");
        assert_eq!(
            source.manual_doc_template().unwrap().unwrap().metadata["extraction_skill"],
            "extraction/home"
        );
    }

    #[test]
    fn accepted_projection_scopes_physical_rendition_without_changing_input() {
        let source = source("household", "Original");
        let original_template = source.manual_doc_template().unwrap().unwrap();
        let original = source.clone();
        let publication = PublicationFence {
            source_id: source.base.id.clone(),
            generation: "original-generation".into(),
            revision_id: "original-revision".into(),
            operation_id: "original-operation".into(),
        };
        let document = accepted_document_projection(&source, &publication)
            .unwrap()
            .unwrap();
        assert_eq!(
            document.rendition_key,
            format!("accepted:{}", document.base.id)
        );
        assert_eq!(
            document.base.id,
            accepted_document_id(&publication, &original_template.doc_id).unwrap()
        );
        assert_eq!(source, original);
        assert_eq!(
            source.manual_doc_template().unwrap().unwrap(),
            original_template
        );
    }
}

#[cfg(test)]
#[path = "source_watch_intake_tests.rs"]
mod watch_tests;
