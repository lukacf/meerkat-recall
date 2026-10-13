//! Exact pre-retraction acceptance proof for retained document and contact
//! restoration. A lifecycle head is never reinterpreted as an acceptance fence.

use elephant_types::publication::{
    OperationState, PublicationFence, SourceHead, SourcePrecondition, SourceVisibility,
};

use crate::source_contact_intake::PreparedSourceContact;
use crate::source_contact_publication::validate_accepted_operation;
use crate::source_publication::{
    conflict, integrity, AcceptSourceRevision, SourceAcceptanceRetry, SourcePublicationRepository,
    StoredOperation, OPERATION_TABLE,
};
use crate::StorageResult;

impl SourcePublicationRepository {
    /// Returns the exact last acceptance captured by the retraction transaction.
    ///
    /// Missing historical proof is unknown; immutable revision first-acceptance
    /// metadata and operation timestamps cannot reconstruct a later ABA instance.
    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "The retraction receipt source_generation must equal the retracted head generation, distinct from the earlier acceptance generation."
    )]
    pub async fn publication_before_retraction(
        &self,
        space: &str,
        retracted: &SourceHead,
    ) -> StorageResult<PublicationFence> {
        if retracted.space_id != space || retracted.lifecycle != SourceVisibility::Retracted {
            return Err(conflict("source_not_retracted"));
        }
        if self.head(space, &retracted.source_id).await?.as_ref() != Some(retracted) {
            return Err(conflict("retracted_source_changed"));
        }
        let operation: StoredOperation = self
            .read_control(OPERATION_TABLE, &retracted.operation_id, space)
            .await?
            .ok_or_else(|| integrity("retraction operation missing"))?;
        let receipt = &operation.receipt;
        if receipt.space_id != space
            || receipt.state != OperationState::Retracted
            || receipt.published_revision.is_some()
            || receipt.source_id != retracted.source_id
            || receipt.source_generation != retracted.generation
            || receipt.operation_id != retracted.operation_id
            || receipt.desired_revision != retracted.desired_revision
        {
            return Err(conflict("retraction_operation_changed"));
        }
        let original = operation
            .predecessor_publication
            .ok_or_else(|| conflict("retraction_predecessor_unknown"))?;
        if original.source_id != retracted.source_id
            || original.generation.is_empty()
            || original.revision_id.is_empty()
            || original.operation_id.is_empty()
            || original.generation == retracted.generation
            || Some(original.revision_id.as_str()) != retracted.desired_revision.as_deref()
            || original.operation_id == retracted.operation_id
        {
            return Err(conflict("retraction_predecessor_changed"));
        }
        // The actual original operation and seal, not the first acceptance
        // stored with a reusable immutable revision, bind this instance.
        let original_operation: StoredOperation = self
            .read_control(OPERATION_TABLE, &original.operation_id, space)
            .await?
            .ok_or_else(|| integrity("original acceptance operation missing"))?;
        validate_accepted_operation(space, &original, &original_operation)?;
        if self
            .revision(space, &original.source_id, &original.revision_id)
            .await?
            .is_none()
        {
            return Err(conflict("retracted_revision_missing"));
        }
        Ok(original)
    }

    /// Preserves an original restore retry or prepares new contact effects from
    /// explicitly authorized restored input and the real pre-retraction seal.
    pub async fn prepared_restore_contact(
        &self,
        request: &AcceptSourceRevision,
        retracted: &SourceHead,
    ) -> StorageResult<Option<PreparedSourceContact>> {
        if !request.reactivate
            || request.source.base.space_id != retracted.space_id
            || request.source.base.id != retracted.source_id
        {
            return Err(conflict("explicit_source_reactivation_required"));
        }
        match self.source_acceptance_retry(request).await? {
            SourceAcceptanceRetry::Existing { contact, .. } => return Ok(contact),
            SourceAcceptanceRetry::ExistingContactPlanUnavailable { .. } => {
                return Err(conflict("accepted_contact_plan_unavailable"))
            }
            SourceAcceptanceRetry::NewIntent => {}
        }
        if request.expected != SourcePrecondition::Generation(retracted.generation.clone()) {
            return Err(conflict("conflict_generation"));
        }
        let original = self
            .publication_before_retraction(&retracted.space_id, retracted)
            .await?;
        if self
            .contact_capability_for_acceptance(&retracted.space_id, &original)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        PreparedSourceContact::prepare(&self.client, &request.source).await
    }
}
