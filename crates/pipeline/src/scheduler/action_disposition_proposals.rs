//! Exact, bounded model links staged for later authenticated review. Nothing in
//! this module mutates the action, grants owner authority, or changes truth.
use super::grounding::{same_authorization_partition, GroundingLineage};
use crate::extraction::{ActionDispositionProposal, AssertionValidator};
use crate::PipelineError;
use elephant_storage::action_disposition::ActionDispositionRepository;
use elephant_storage::canonical_commit::{source_support_fingerprint, CanonicalRepository};
use elephant_storage::read_eligibility::ReadEligibility;
use elephant_storage::{AssertionRepository, EntityRepository, Repository, StorageClient};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Untrusted exact-action candidate retained separately from factual assertions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedActionDispositionProposal {
    /// Version of this untrusted proposal format.
    pub version: u32,
    /// Literal untrusted_model_proposal; never a permission grant.
    pub authority: String,
    /// Exact existing requires_action assertion.
    pub target_action_id: RecordId,
    /// Semantic before-image to send as expected_action_fingerprint on explicit close.
    pub target_fingerprint: String,
    /// Exact current subject; this does not close other actions for that subject.
    pub subject_entity_id: RecordId,
    /// Exact original nonempty revision quote, never model-created evidence.
    pub owner_statement: EvidenceSpan,
    /// Proposed reason requiring separate authenticated approval.
    pub reason: String,
}
fn invalid() -> PipelineError {
    PipelineError::ValidationError(
        "Action proposal identity, evidence or current authorization invalid".into(),
    )
}
fn evidence(
    proposal: &ActionDispositionProposal,
    lineage: &GroundingLineage,
    window: [usize; 2],
) -> Result<EvidenceSpan, PipelineError> {
    if proposal.target_action_id.table() != TableName::Assertion
        || proposal.subject_entity_id.table() != TableName::Entity
        || proposal.reason.trim().is_empty()
        || proposal.reason.len() > 4096
        || proposal.evidence_quote.len() > 32768
        || proposal.evidence_start >= proposal.evidence_end
        || window[0] < lineage.node.span.start as usize
        || window[1] > lineage.node.span.end as usize
        || window[0] >= window[1]
        || lineage.revision.text.get(window[0]..window[1]).is_none()
    {
        return Err(invalid());
    }
    let start = window[0]
        .checked_add(proposal.evidence_start as usize)
        .ok_or_else(invalid)?;
    let end = window[0]
        .checked_add(proposal.evidence_end as usize)
        .ok_or_else(invalid)?;
    if end > window[1]
        || lineage.revision.text.get(start..end) != Some(proposal.evidence_quote.as_str())
    {
        return Err(invalid());
    }
    Ok(EvidenceSpan {
        revision: Link::new(lineage.revision.base.id.clone()),
        node: Some(Link::new(lineage.node.base.id.clone())),
        start: u32::try_from(start).map_err(|_| invalid())?,
        end: u32::try_from(end).map_err(|_| invalid())?,
        quote_hash: AssertionValidator::hash_quote(&proposal.evidence_quote),
    })
}

pub(crate) async fn prepare(
    storage: &StorageClient,
    lineage: &GroundingLineage,
    window: [usize; 2],
    proposals: &[ActionDispositionProposal],
) -> Result<Vec<StagedActionDispositionProposal>, PipelineError> {
    if proposals.is_empty() {
        return Ok(vec![]);
    }
    if proposals.len() > 16 {
        return Err(invalid());
    }
    lineage.validate(&lineage.source.base.space_id)?;
    let space = lineage.source.base.space_id.as_str();
    let canonical = CanonicalRepository::new(storage.clone());
    let epoch = canonical.observe_epoch(space).await?.ok_or_else(invalid)?;
    let reader = ReadEligibility::new(storage.clone());
    let authorize = |base: &elephant_types::records::BaseFields| {
        same_authorization_partition(base, &lineage.source.base)
    };
    let mut targets = BTreeSet::new();
    let mut prepared = Vec::with_capacity(proposals.len());
    for proposal in proposals {
        if !targets.insert(proposal.target_action_id.clone()) {
            return Err(invalid());
        }
        let span = evidence(proposal, lineage, window)?;
        let action = AssertionRepository::new(storage.clone())
            .get(space, &proposal.target_action_id)
            .await?
            .ok_or_else(invalid)?;
        if action
            .predicate_id
            .as_deref()
            .unwrap_or(&action.predicate_raw)
            != "requires_action"
            || action.subject_entity.as_ref().map(|id| id.id()) != Some(&proposal.subject_entity_id)
            || !authorize(&action.base)
            || reader.project(action.clone(), &authorize).await?.is_none()
        {
            return Err(invalid());
        }
        let subject = EntityRepository::new(storage.clone())
            .get(space, &proposal.subject_entity_id)
            .await?
            .ok_or_else(invalid)?;
        if !authorize(&subject.base)
            || reader.project(subject, &authorize).await?.is_none()
            || ActionDispositionRepository::new(storage.clone())
                .current(space, &proposal.target_action_id, &authorize)
                .await?
                .is_some()
        {
            return Err(invalid());
        }
        prepared.push(StagedActionDispositionProposal {
            version: 1,
            authority: "untrusted_model_proposal".into(),
            target_action_id: proposal.target_action_id.clone(),
            target_fingerprint: source_support_fingerprint(&action)?,
            subject_entity_id: proposal.subject_entity_id.clone(),
            owner_statement: span,
            reason: proposal.reason.clone(),
        });
    }
    reader.verify_current().await?;
    if canonical.observe_epoch(space).await?.as_deref() != Some(epoch.as_str()) {
        return Err(invalid());
    }
    Ok(prepared)
}

#[cfg(test)]
#[path = "action_disposition_proposal_tests.rs"]
mod tests;
