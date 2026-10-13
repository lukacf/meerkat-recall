//! Exact render/section-summary inputs captured before the first own mutation.
//! This is a read/dispatch guard; publication remains the storage owner's job.

use super::*;
use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::standing_watch_repository::{
    PreparedStandingSourceAuthority, PreparedStandingWatchAuthority,
};
use elephant_types::provider_binding::{ModelBindingProvenance, ModelOperation};
use elephant_types::publication::AttemptFence;
use elephant_types::records::WorkItem;

pub(super) struct PreparedSummaryInputs {
    storage: StorageClient,
    raw_source: Source,
    accepted_source: Source,
    doc: Doc,
    revision: Option<DocRevision>,
    publication: Option<PublicationFence>,
    authority: Option<PreparedStandingSourceAuthority>,
    attempt: Option<AttemptFence>,
}

impl std::fmt::Debug for PreparedSummaryInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedSummaryInputs")
            .field("space", &self.doc.base.space_id)
            .field(
                "revision",
                &self.revision.as_ref().map(|revision| &revision.base.id),
            )
            .finish_non_exhaustive()
    }
}

fn changed() -> PipelineError {
    PipelineError::ValidationError("Original render/summary input or authority changed".into())
}

// Validate the original descriptor before following its private authority.
// An absent optional source descriptor is not evidence of Independent origin;
// capture_for_work below still establishes the original admitted authority.
pub(super) fn validate_original_work_input(
    work: &WorkItem,
    source: &Source,
    doc: &Doc,
    revision: Option<&DocRevision>,
    publication: Option<&PublicationFence>,
) -> Result<(), PipelineError> {
    let exact_input = match work.kind.as_str() {
        RENDER_DOC_KIND => {
            work.payload
                .get("doc_id")
                .and_then(serde_json::Value::as_str)
                == Some(doc.base.id.to_string().as_str())
        }
        BUILD_DOC_TREE_KIND => revision.is_some_and(|revision| {
            work.payload
                .get("revision_id")
                .and_then(serde_json::Value::as_str)
                == Some(revision.base.id.to_string().as_str())
        }),
        _ => false,
    };
    if work.base.space_id != doc.base.space_id
        || source.base.space_id != doc.base.space_id
        || doc.source.as_ref().map(elephant_types::Link::id) != Some(&source.base.id)
        || !exact_input
    {
        return Err(changed());
    }
    if let Some(source_id) = work.payload.get("source_id") {
        if !source_id.is_null() && source_id.as_str() != Some(source.base.id.to_string().as_str()) {
            return Err(changed());
        }
    }
    if let Some(original) = crate::source_queue::publication_from_work(work)? {
        if Some(&original) != publication || original.source_id != source.base.id {
            return Err(changed());
        }
    }
    Ok(())
}

impl PreparedSummaryInputs {
    pub(super) async fn capture(
        storage: &StorageClient,
        accepted_source: &Source,
        doc: &Doc,
        revision: &DocRevision,
        publication: Option<&PublicationFence>,
        original_work: Option<&WorkItem>,
    ) -> Result<Self, PipelineError> {
        Self::capture_inner(
            storage,
            accepted_source,
            doc,
            Some(revision),
            publication,
            original_work,
        )
        .await
    }

    pub(super) async fn capture_render(
        storage: &StorageClient,
        accepted_source: &Source,
        doc: &Doc,
        publication: Option<&PublicationFence>,
        original_work: Option<&WorkItem>,
    ) -> Result<Self, PipelineError> {
        Self::capture_inner(
            storage,
            accepted_source,
            doc,
            None,
            publication,
            original_work,
        )
        .await
    }

    async fn capture_inner(
        storage: &StorageClient,
        accepted_source: &Source,
        doc: &Doc,
        revision: Option<&DocRevision>,
        publication: Option<&PublicationFence>,
        original_work: Option<&WorkItem>,
    ) -> Result<Self, PipelineError> {
        let raw_source = elephant_storage::SourceRepository::new(storage.clone())
            .get(&doc.base.space_id, &accepted_source.base.id)
            .await?
            .ok_or_else(changed)?;
        let mut authority = if let Some(work) = original_work {
            validate_original_work_input(work, accepted_source, doc, revision, publication)?;
            // Independently admitted execution is not subject to a selected
            // source's historical Watch veto. Follow the actual original Work.
            Some(PreparedStandingSourceAuthority::capture_for_work(storage, work).await?)
        } else if let Some(publication) = publication {
            Some(
                PreparedStandingWatchAuthority::capture_for_source_operation(
                    storage,
                    &doc.base.space_id,
                    publication,
                )
                .await?
                .ok_or_else(changed)?,
            )
        } else {
            // captured_render_input permits this retained direct-library path
            // only when there is no accepted SourceHead at all.
            None
        };
        if let Some(authority) = &mut authority {
            for record in [
                CanonicalRecord::Source(Box::new(raw_source.clone())),
                CanonicalRecord::Doc(Box::new(doc.clone())),
            ] {
                authority.authorize_record(record)?;
            }
            if let Some(revision) = revision {
                authority
                    .authorize_record(CanonicalRecord::DocRevision(Box::new(revision.clone())))?;
            }
            // The immutable accepted input is a distinct envelope from the
            // current raw row; authorize it without replacing that raw CAS row.
            for principal in authority.principals() {
                elephant_storage::source_watch_registration::authorize_standing_source_input(
                    principal,
                    &CanonicalRecord::Source(Box::new(accepted_source.clone())),
                )?;
            }
        }
        let attempt = original_work
            .map(elephant_storage::work_attempt_repository::fence_from_claim)
            .transpose()?;
        let captured = Self {
            storage: storage.clone(),
            raw_source,
            accepted_source: accepted_source.clone(),
            doc: doc.clone(),
            revision: revision.cloned(),
            publication: publication.cloned(),
            authority,
            attempt,
        };
        captured.verify_current().await?;
        Ok(captured)
    }

    pub(super) async fn verify_current(&self) -> Result<(), PipelineError> {
        let space = &self.doc.base.space_id;
        let raw = elephant_storage::SourceRepository::new(self.storage.clone())
            .get(space, &self.raw_source.base.id)
            .await?;
        let doc = DocRepository::new(self.storage.clone())
            .get(space, &self.doc.base.id)
            .await?;
        if raw.as_ref() != Some(&self.raw_source) || doc.as_ref() != Some(&self.doc) {
            return Err(changed());
        }
        if let Some(original) = &self.revision {
            let revision = DocRevisionRepository::new(self.storage.clone())
                .get(space, &original.base.id)
                .await?;
            if revision.as_ref() != Some(original) {
                return Err(changed());
            }
        }
        // Recheck the original generation/visibility and accepted bytes. No
        // freshly observed head, current grant, or replacement input is adopted.
        let (accepted, _, publication) = captured_render_input(&self.storage, &self.doc).await?;
        if accepted != self.accepted_source || publication != self.publication {
            return Err(changed());
        }
        if let Some(authority) = &self.authority {
            authority.verify_current(&self.storage).await?;
        }
        if let Some(attempt) = &self.attempt {
            let work = elephant_storage::WorkItemRepository::new(self.storage.clone())
                .get(space, &attempt.work_item_id)
                .await?
                .ok_or_else(changed)?;
            if elephant_storage::work_attempt_repository::fence_from_claim(&work)? != *attempt
                || work
                    .lease_expires_at
                    .is_none_or(|expires| expires <= Utc::now())
            {
                return Err(changed());
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProviderDispatchObserver for PreparedSummaryInputs {
    async fn before_dispatch(
        &self,
        _call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        if provenance.config.space_id != self.doc.base.space_id
            || provenance.operation != ModelOperation::Summary
            || self.revision.is_none()
        {
            return Err(EmbeddingError::ProviderError(
                "provider_authorization: summary input scope differs".into(),
            ));
        }
        self.verify_current().await.map_err(|error| {
            EmbeddingError::ProviderError(format!("provider_authorization: {error}"))
        })
    }

    async fn observe_usage(
        &self,
        _call_id: &str,
        _provenance: &ModelBindingProvenance,
        _input_tokens: Option<u64>,
        _output_tokens: Option<u64>,
    ) -> EmbeddingResult<()> {
        // The composed original observer remains the sole accounting owner.
        Ok(())
    }
}
