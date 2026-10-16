//! Receipt-based grouped no-op. It cannot enqueue work or accept new source input.

use super::{
    integrity, source_support_fingerprint, BTreeMap, Deserialize, HistoricalAdoptionReceipt,
    HistoricalAdoptionRepository, HistoricalCommitProvenance, RecordId, Serialize, StorageResult,
    MAX_HISTORICAL_GROUP_DOCUMENTS,
};
use crate::canonical_commit::CanonicalRepository;
use crate::source_publication::SourcePublicationRepository;
use crate::source_publication_manifest::SourcePublicationManifestRepository;
use crate::{DocRepository, DocRevisionRepository, Repository, SourceRepository};
use elephant_types::publication::{
    OperationState, PublicationFence, SourceAuthorizationSnapshot, SourceVisibility,
};

/// Supply exact original adoption receipt identity and the complete current
/// document content map. No replay headers or representative document are used.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalNoopRequest {
    /// Destination namespace containing the adopted population.
    pub space_id: String,
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Exact published source generation required for unchanged recognition.
    pub expected_generation: String,
    /// Historical adapter revision identity accepted for this source.
    pub accepted_revision: String,
    /// Historical adapter request fingerprint; not a manual source PUT fingerprint.
    pub accepted_request_fingerprint: String,
    /// Exact original adoption commit identifier retained for validation.
    pub original_commit_id: String,
    /// Complete document identity to original title-and-text digest map.
    pub document_content_sha256: BTreeMap<RecordId, String>,
    /// Epoch observed before authorizing the source and every document.
    pub authorization_epoch: Option<String>,
    /// Exact full typed rows actually authorized by the adapter.
    pub authorized_document_fingerprints: BTreeMap<RecordId, String>,
    /// The adapter must authorize this exact current source envelope first.
    pub authorized: SourceAuthorizationSnapshot,
}

/// Read-only recognition of the exact original adopted group and current operation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HistoricalNoopReceipt {
    /// True only after exact group content, receipt and current authorization checks.
    pub unchanged: bool,
    /// Operation receipt for the bound historical publication.
    pub operation: elephant_types::publication::OperationReceipt,
    /// Original adoption receipt retained by unchanged recognition.
    pub original_adoption: HistoricalAdoptionReceipt,
}

async fn verify_documents(
    storage: &crate::StorageClient,
    request: &HistoricalNoopRequest,
    manifests: &SourcePublicationManifestRepository,
    manifest: &crate::source_publication_manifest::SourcePublicationManifest,
    accepted: &elephant_types::publication::SourceRevision,
) -> StorageResult<()> {
    for mapping in &manifest.documents {
        let doc = DocRepository::new(storage.clone())
            .get(&request.space_id, &mapping.doc_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged document missing"))?;
        let revision = DocRevisionRepository::new(storage.clone())
            .get(&request.space_id, &mapping.doc_revision_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged revision missing"))?;
        if request.authorized_document_fingerprints.get(&doc.base.id)
            != Some(&source_support_fingerprint(&doc)?)
        {
            return Err(integrity(
                "historical unchanged authorized document changed",
            ));
        }
        if !manifests
            .completed_document_revision_is_current(mapping, &doc, &revision, accepted)
            .await?
        {
            return Err(integrity("historical unchanged document proof differs"));
        }
    }
    Ok(())
}

fn verify_unchanged_manifest(
    request: &HistoricalNoopRequest,
    manifest: &crate::source_publication_manifest::SourcePublicationManifest,
) -> StorageResult<()> {
    manifest.validate()?;
    let proof = manifest
        .historical_adoption
        .as_ref()
        .ok_or_else(|| integrity("historical unchanged proof missing"))?;
    if proof.external_source_id != request.external_source_id
        || proof.commit_id(manifest) != request.original_commit_id
        || proof
            .documents
            .iter()
            .map(|(id, doc)| (id.clone(), doc.doc_content_sha256.clone()))
            .collect::<BTreeMap<_, _>>()
            != request.document_content_sha256
    {
        return Err(integrity(
            "historical unchanged complete document input differs",
        ));
    }
    Ok(())
}

impl HistoricalAdoptionRepository {
    /// Verify a grouped unchanged resend read-only. Changed groups require the
    /// separately guarded delta adapter; this API never starts extraction.
    pub async fn verify_unchanged(
        &self,
        request: &HistoricalNoopRequest,
    ) -> StorageResult<HistoricalNoopReceipt> {
        let canonical = CanonicalRepository::new(self.client().clone());
        let epoch = canonical.observe_epoch(&request.space_id).await?;
        if epoch != request.authorization_epoch
            || request.document_content_sha256.is_empty()
            || request.document_content_sha256.len() > MAX_HISTORICAL_GROUP_DOCUMENTS
            || request
                .authorized_document_fingerprints
                .keys()
                .ne(request.document_content_sha256.keys())
        {
            return Err(integrity(
                "historical unchanged authorization changed or group invalid",
            ));
        }

        let source = SourceRepository::new(self.client().clone())
            .get(&request.space_id, &request.source_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged source missing"))?;
        if SourceAuthorizationSnapshot::from(&source) != request.authorized
            || source.external_id.as_deref() != Some(request.external_source_id.as_str())
        {
            return Err(integrity(
                "historical unchanged authorization or identity differs",
            ));
        }
        let publications = SourcePublicationRepository::new(self.client().clone());
        let head = publications
            .head(&request.space_id, &request.source_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged head missing"))?;
        if head.lifecycle != SourceVisibility::Active
            || head.generation != request.expected_generation
            || head.desired_revision.as_deref() != Some(request.accepted_revision.as_str())
            || head.published_revision != head.desired_revision
        {
            return Err(integrity("historical unchanged generation differs"));
        }
        let publication = PublicationFence {
            source_id: request.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: request.accepted_revision.clone(),
            operation_id: head.operation_id.clone(),
        };
        let manifests = SourcePublicationManifestRepository::new(self.client().clone());
        let manifest = manifests
            .get(&request.space_id, &publication)
            .await?
            .ok_or_else(|| integrity("historical unchanged manifest missing"))?;
        verify_unchanged_manifest(request, &manifest)?;
        let accepted = publications
            .revision(
                &request.space_id,
                &request.source_id,
                &request.accepted_revision,
            )
            .await?
            .ok_or_else(|| integrity("historical unchanged accepted source missing"))?;
        verify_documents(self.client(), request, &manifests, &manifest, &accepted).await?;
        drop(manifests);
        let commit = canonical
            .receipt_read_only(&request.space_id, &request.original_commit_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged actual commit missing"))?;
        if commit.input_fingerprint != request.accepted_request_fingerprint
            || commit.historical_import.as_ref()
                != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
        {
            return Err(integrity(
                "historical unchanged request fingerprint differs",
            ));
        }
        let original: HistoricalAdoptionReceipt = serde_json::from_value(
            commit
                .replay_output
                .ok_or_else(|| integrity("historical unchanged original receipt missing"))?,
        )
        .map_err(|_| integrity("historical unchanged receipt invalid"))?;
        let operation = publications
            .operation(&request.space_id, &head.operation_id)
            .await?
            .ok_or_else(|| integrity("historical unchanged operation missing"))?;
        drop(publications);
        if !matches!(
            operation.state,
            OperationState::Processing | OperationState::Completed
        ) || canonical.observe_epoch(&request.space_id).await? != epoch
        {
            return Err(integrity("historical unchanged state changed during read"));
        }
        drop(canonical);
        Ok(HistoricalNoopReceipt {
            unchanged: true,
            operation,
            original_adoption: original,
        })
    }
}
