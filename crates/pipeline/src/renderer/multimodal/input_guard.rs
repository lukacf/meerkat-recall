//! Original media input custody at the physical provider boundary.
//! Debug output deliberately excludes source text, payloads and policy material.

use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::standing_watch_repository::{
    PreparedStandingSourceAuthority, PreparedStandingWatchAuthority,
};
use elephant_storage::{DocRepository, Repository, SourceRepository, StorageClient};
use elephant_types::provider_binding::{ModelBindingProvenance, ModelOperation};
use elephant_types::publication::{AttemptFence, PublicationFence};
use elephant_types::records::doc::{Doc, DocMediaAttachment, DocMediaLocation};
use elephant_types::records::{MediaAsset, Source, WorkItem};

use crate::media::{read_media_payload, LoadedMediaInput};
use crate::renderer::RenderContext;
use crate::scheduler::evidence_work_kinds::captured_render_input;
use crate::scheduler::grounding::same_authorization_partition;
use crate::PipelineError;

struct OriginalAttachment {
    attachment: DocMediaAttachment,
    asset: Option<MediaAsset>,
    payload_sha256: String,
    size_bytes: usize,
}

pub(super) struct PreparedMediaInputs {
    storage: StorageClient,
    input_doc: Doc,
    raw_doc: Option<Doc>,
    raw_source: Option<Source>,
    accepted_source: Option<Source>,
    publication: Option<PublicationFence>,
    authority: Option<PreparedStandingSourceAuthority>,
    attempt: Option<AttemptFence>,
    inputs: Vec<OriginalAttachment>,
}

impl std::fmt::Debug for PreparedMediaInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedMediaInputs")
            .field("space", &self.input_doc.base.space_id)
            .field("doc", &self.input_doc.base.id)
            .field("attachment_count", &self.inputs.len())
            .finish_non_exhaustive()
    }
}

fn changed() -> PipelineError {
    PipelineError::ValidationError("Original media input or execution authority changed".into())
}

fn payload_hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

// Absence is resolved by the original execution-ancestry owner below; a
// present descriptor must always select exactly the captured source generation.
fn validate_original_publication(
    work: &WorkItem,
    captured: Option<&PublicationFence>,
) -> Result<(), PipelineError> {
    if let Some(original) = crate::source_queue::publication_from_work(work)? {
        if Some(&original) != captured {
            return Err(changed());
        }
    }
    Ok(())
}

// A scoped miss is not global absence: a foreign or deleted physical row
// must not turn a managed document into the retained unmanaged primitive.
async fn read_current_doc(
    storage: &StorageClient,
    doc: &Doc,
) -> Result<Option<Doc>, PipelineError> {
    let repository = DocRepository::new(storage.clone());
    let current = repository
        .get_with_deleted(&doc.base.space_id, &doc.base.id)
        .await?;
    if current.is_none() && repository.exists_globally(&doc.base.id).await? {
        return Err(changed());
    }
    Ok(current)
}

impl PreparedMediaInputs {
    pub(super) async fn capture(
        ctx: &RenderContext,
        original_work: Option<&WorkItem>,
    ) -> Result<Self, PipelineError> {
        let storage = ctx.storage.clone().ok_or_else(changed)?;
        if ctx.doc.base.space_id != ctx.space_id || ctx.doc.base.is_deleted() {
            return Err(changed());
        }
        let raw_doc = read_current_doc(&storage, &ctx.doc).await?;
        let (raw_source, accepted_source, publication) = if ctx.doc.source.is_some() {
            let raw = raw_doc.as_ref().ok_or_else(changed)?;
            let (accepted, render_doc, publication) = captured_render_input(&storage, raw).await?;
            if render_doc != ctx.doc {
                return Err(changed());
            }
            let source = SourceRepository::new(storage.clone())
                .get(&ctx.space_id, &accepted.base.id)
                .await?
                .ok_or_else(changed)?;
            (Some(source), Some(accepted), publication)
        } else {
            // Existing direct renderer primitives can carry an unmanaged Doc.
            // This branch is never evidence of Independent execution: it has
            // no original Work, accepted marker, or selected source authority.
            if original_work.is_some()
                || ctx.doc.metadata.get("_publication").is_some()
                || raw_doc.as_ref().is_some_and(|raw| raw != &ctx.doc)
            {
                return Err(changed());
            }
            (None, None, None)
        };
        let mut authority = if let Some(work) = original_work {
            if work.base.space_id != ctx.space_id
                || work.kind != "render_doc"
                || work
                    .payload
                    .get("doc_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(ctx.doc.base.id.to_string().as_str())
            {
                return Err(changed());
            }
            if let Some(source_id) = work.payload.get("source_id") {
                let expected = ctx
                    .doc
                    .source
                    .as_ref()
                    .ok_or_else(changed)?
                    .id()
                    .to_string();
                if !source_id.is_null() && source_id.as_str() != Some(expected.as_str()) {
                    return Err(changed());
                }
            }
            validate_original_publication(work, publication.as_ref())?;
            Some(PreparedStandingSourceAuthority::capture_for_work(&storage, work).await?)
        } else if let Some(fence) = &publication {
            Some(
                PreparedStandingWatchAuthority::capture_for_source_operation(
                    &storage,
                    &ctx.space_id,
                    fence,
                )
                .await?
                .ok_or_else(changed)?,
            )
        } else {
            None
        };
        if let Some(proof) = &mut authority {
            proof.authorize_record(CanonicalRecord::Doc(Box::new(
                raw_doc.clone().ok_or_else(changed)?,
            )))?;
            proof.authorize_record(CanonicalRecord::Source(Box::new(
                raw_source.clone().ok_or_else(changed)?,
            )))?;
            for principal in proof.principals() {
                for record in [
                    CanonicalRecord::Doc(Box::new(ctx.doc.clone())),
                    CanonicalRecord::Source(Box::new(accepted_source.clone().ok_or_else(changed)?)),
                ] {
                    elephant_storage::source_watch_registration::authorize_standing_source_input(
                        principal, &record,
                    )?;
                }
            }
        }
        let attempt = original_work
            .map(elephant_storage::work_attempt_repository::fence_from_claim)
            .transpose()?;
        let captured = Self {
            storage,
            input_doc: ctx.doc.clone(),
            raw_doc,
            raw_source,
            accepted_source,
            publication,
            authority,
            attempt,
            inputs: Vec::new(),
        };
        captured.verify_authority().await?;
        Ok(captured)
    }

    pub(super) fn retain(
        &mut self,
        attachment: &DocMediaAttachment,
        loaded: &LoadedMediaInput,
    ) -> Result<(), PipelineError> {
        // Exactly the original ordered document attachments, without duplicate
        // IDs or a replacement attachment selected after loading began.
        if self.input_doc.media_attachments.get(self.inputs.len()) != Some(attachment)
            || self
                .inputs
                .iter()
                .any(|input| input.attachment.attachment_id == attachment.attachment_id)
        {
            return Err(changed());
        }
        let hash = payload_hash(&loaded.bytes);
        let size = u64::try_from(loaded.bytes.len()).map_err(|_| changed())?;
        if attachment
            .sha256
            .as_ref()
            .is_some_and(|expected| expected != &hash)
            || attachment
                .size_bytes
                .is_some_and(|expected| expected != size)
        {
            return Err(changed());
        }
        match (&attachment.location, &loaded.asset) {
            (DocMediaLocation::ManagedAsset { asset_id }, Some(asset)) => {
                if asset.base.id.to_string() != *asset_id
                    || asset.base.is_deleted()
                    || !same_authorization_partition(&asset.base, &self.input_doc.base)
                    || asset.kind != attachment.kind
                    || asset.media_type != attachment.media_type
                    || asset.sha256 != hash
                    || asset.size_bytes != size
                    || asset.duration_ms.is_some_and(|duration| {
                        attachment
                            .duration_ms
                            .is_some_and(|expected| duration != expected)
                    })
                {
                    return Err(changed());
                }
                if let Some(proof) = &self.authority {
                    for principal in proof.principals() {
                        elephant_storage::source_watch_registration::authorize_standing_media_asset(principal, asset)?;
                    }
                }
                // Accepted source media has a complete immutable asset row.
                // A live replacement with matching public metadata is not it.
                if let Some(source) = &self.accepted_source {
                    let value = source
                        .metadata
                        .get("_manual_doc")
                        .and_then(|doc| doc.get("_intake_assets"));
                    if self.publication.is_some() || value.is_some() {
                        let accepted: Vec<MediaAsset> =
                            serde_json::from_value(value.cloned().ok_or_else(changed)?)
                                .map_err(|_| changed())?;
                        let matching: Vec<_> = accepted
                            .iter()
                            .filter(|original| original.base.id == asset.base.id)
                            .collect();
                        if matching.len() != 1 || matching[0] != asset {
                            return Err(changed());
                        }
                    }
                }
            }
            (DocMediaLocation::Path { .. }, None) => {}
            _ => return Err(changed()),
        }
        self.inputs.push(OriginalAttachment {
            attachment: attachment.clone(),
            asset: loaded.asset.clone(),
            payload_sha256: hash,
            size_bytes: loaded.bytes.len(),
        });
        Ok(())
    }

    async fn verify_authority(&self) -> Result<(), PipelineError> {
        let space = &self.input_doc.base.space_id;
        let current = read_current_doc(&self.storage, &self.input_doc).await?;
        if current != self.raw_doc {
            return Err(changed());
        }
        if let Some(original) = &self.raw_source {
            let raw = SourceRepository::new(self.storage.clone())
                .get(space, &original.base.id)
                .await?;
            if raw.as_ref() != Some(original) {
                return Err(changed());
            }
            let (accepted, input, publication) =
                captured_render_input(&self.storage, self.raw_doc.as_ref().ok_or_else(changed)?)
                    .await?;
            if self.accepted_source.as_ref() != Some(&accepted)
                || input != self.input_doc
                || publication != self.publication
            {
                return Err(changed());
            }
        }
        if let Some(proof) = &self.authority {
            proof.verify_current(&self.storage).await?;
        }
        if let Some(attempt) = &self.attempt {
            let work = elephant_storage::WorkItemRepository::new(self.storage.clone())
                .get(space, &attempt.work_item_id)
                .await?
                .ok_or_else(changed)?;
            if elephant_storage::work_attempt_repository::fence_from_claim(&work)? != *attempt
                || work
                    .lease_expires_at
                    .is_none_or(|expires| expires <= chrono::Utc::now())
            {
                return Err(changed());
            }
        }
        Ok(())
    }

    pub(super) async fn verify_current(&self) -> Result<(), PipelineError> {
        if self.inputs.len() != self.input_doc.media_attachments.len() {
            return Err(changed());
        }
        self.verify_authority().await?;
        for input in &self.inputs {
            let (bytes, asset) = read_media_payload(
                Some(&self.storage),
                &self.input_doc.base.space_id,
                &input.attachment,
            )
            .await?;
            if asset != input.asset
                || bytes.len() != input.size_bytes
                || payload_hash(&bytes) != input.payload_sha256
            {
                return Err(changed());
            }
        }
        // Payload reads may wait on storage/filesystem. Keep the same proof
        // and revalidate again after those waits, without capturing a new one.
        self.verify_authority().await
    }
}

#[async_trait::async_trait]
impl ProviderDispatchObserver for PreparedMediaInputs {
    async fn before_dispatch(
        &self,
        _call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        if provenance.config.space_id != self.input_doc.base.space_id
            || provenance.operation != ModelOperation::MediaRender
        {
            return Err(EmbeddingError::ProviderError(
                "media input guard selected for another operation".into(),
            ));
        }
        self.verify_current()
            .await
            .map_err(|error| EmbeddingError::ProviderError(error.to_string()))
    }
    async fn observe_usage(
        &self,
        _call_id: &str,
        _provenance: &ModelBindingProvenance,
        _input_tokens: Option<u64>,
        _output_tokens: Option<u64>,
    ) -> EmbeddingResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod original_publication_tests {
    use super::*;
    use elephant_types::records::{BaseFields, WorkItemStatus};
    use elephant_types::{RecordId, TableName};

    #[test]
    fn present_original_work_fence_cannot_select_another_media_input() {
        let original = PublicationFence {
            source_id: RecordId::new_v4(TableName::Source),
            generation: "original-generation".into(),
            revision_id: "original-revision".into(),
            operation_id: "original-operation".into(),
        };
        let mut work = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "media-proof".into(),
                "test".into(),
            ),
            kind: "render_doc".into(),
            payload: serde_json::json!({"_publication":original}),
            status: WorkItemStatus::Pending,
            priority: 0,
            idempotency_key: "proof-selector".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        assert!(validate_original_publication(&work, Some(&original)).is_ok());
        assert!(validate_original_publication(&work, None).is_err());
        for field in ["source_id", "generation", "revision_id", "operation_id"] {
            let mut replacement = original.clone();
            match field {
                "source_id" => replacement.source_id = RecordId::new_v4(TableName::Source),
                "generation" => replacement.generation = "new-generation".into(),
                "revision_id" => replacement.revision_id = "new-revision".into(),
                _ => replacement.operation_id = "new-operation".into(),
            }
            assert!(validate_original_publication(&work, Some(&replacement)).is_err());
        }
        work.payload["_publication"] = serde_json::Value::Null;
        assert!(validate_original_publication(&work, Some(&original)).is_err());
        work.payload["_publication"] = serde_json::json!({"generation":"incomplete"});
        assert!(validate_original_publication(&work, Some(&original)).is_err());
    }
}
