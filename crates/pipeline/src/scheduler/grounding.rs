//! Fail-closed grounding at the active artifact and promotion boundaries.
//! These reads produce validation preconditions, not a publication transaction.
//! The canonical owner must recheck the captured lineage at atomic publication.

use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository, SourceRepository,
    StorageClient,
};
use elephant_types::records::base::{BaseFields, ProvenanceStep};
use elephant_types::records::doc_revision::{DocRevision, RevisionStatus};
use elephant_types::records::source::{Source, SourceLifecycleStatus};
use elephant_types::records::{Doc, DocNode};
use elephant_types::RecordId;

use crate::error::PipelineError;
use crate::extraction::{AssertionValidator, RawAssertion};

/// Immutable revision provenance entry binding the accepted source content.
pub const SOURCE_CONTENT_STEP: &str = "source_content";

/// Exact immutable evidence and authorized source lineage read for promotion.
#[derive(Debug, Clone)]
pub struct GroundingLineage {
    /// Active source; its generation is fenced separately by canonical commit.
    pub source: Source,
    /// Current parent document projection.
    pub doc: Doc,
    /// Immutable rendered evidence.
    pub revision: DocRevision,
    /// Exact structural node in that revision.
    pub node: DocNode,
}

fn invalid(message: &str) -> PipelineError {
    PipelineError::ValidationError(message.to_string())
}

/// Load and validate source, document, revision, and node without fallback.
pub async fn load_grounding_lineage(
    storage: &StorageClient,
    space_id: &str,
    revision_id: &RecordId,
    node_id: &RecordId,
) -> Result<GroundingLineage, PipelineError> {
    let revision = DocRevisionRepository::new(storage.clone())
        .get_by_id(revision_id)
        .await?
        .ok_or_else(|| invalid("Grounding revision is missing or deleted"))?;
    let node = DocNodeRepository::new(storage.clone())
        .get(space_id, node_id)
        .await?
        .ok_or_else(|| invalid("Grounding node is missing or deleted"))?;
    let doc = DocRepository::new(storage.clone())
        .get(space_id, revision.doc.id())
        .await?
        .ok_or_else(|| invalid("Grounding document is missing or deleted"))?;
    let source_id = doc
        .source
        .as_ref()
        .ok_or_else(|| invalid("Grounding document has no source"))?
        .id();
    let source = SourceRepository::new(storage.clone())
        .get(space_id, source_id)
        .await?
        .ok_or_else(|| invalid("Grounding source is missing or deleted"))?;
    let lineage = GroundingLineage {
        source,
        doc,
        revision,
        node,
    };
    lineage.validate(space_id)?;
    Ok(lineage)
}

impl GroundingLineage {
    /// Validate a captured lineage again before constructing any canonical effect.
    pub fn validate(&self, space_id: &str) -> Result<(), PipelineError> {
        validate_revision_lineage(&self.source, &self.doc, &self.revision, space_id)?;
        if self.node.base.space_id != space_id
            || self.node.base.is_deleted()
            || !same_authorization_partition(&self.node.base, &self.source.base)
            || self.node.revision.id() != &self.revision.base.id
        {
            return Err(invalid(
                "Grounding node is deleted or its revision/authorization lineage changed",
            ));
        }
        if self.node.span.start >= self.node.span.end
            || self
                .revision
                .text
                .get(self.node.span.start as usize..self.node.span.end as usize)
                .is_none()
        {
            return Err(invalid(
                "Grounding node has an invalid UTF-8 evidence range",
            ));
        }
        Ok(())
    }

    /// Validate every interpretation before any entity or graph effect is prepared.
    pub fn validate_assertions(&self, assertions: &[RawAssertion]) -> Result<(), PipelineError> {
        self.validate_assertions_with_support(assertions, true)
    }

    pub(crate) fn validate_assertions_with_support(
        &self,
        assertions: &[RawAssertion],
        require_support: bool,
    ) -> Result<(), PipelineError> {
        let len = u32::try_from(self.revision.text.len())
            .map_err(|_| invalid("Revision exceeds supported evidence size"))?;
        for assertion in assertions {
            if assertion.evidence_span.revision.id() != &self.revision.base.id
                || assertion.evidence_span.node.as_ref().map(|node| node.id())
                    != Some(&self.node.base.id)
            {
                return Err(invalid(
                    "Assertion evidence refers to another node or revision",
                ));
            }
            AssertionValidator::new(true)
                .validate(assertion, len)
                .map_err(|errors| {
                    PipelineError::ValidationError(
                        errors
                            .iter()
                            .map(|error| error.message.as_str())
                            .collect::<Vec<_>>()
                            .join("; "),
                    )
                })?;
            AssertionValidator::validate_evidence(assertion, &self.revision.text)
                .map_err(PipelineError::ValidationError)?;
            if require_support || assertion.support_verification.is_some() {
                assertion.validate_support()?;
            }
        }
        Ok(())
    }

    /// Inherit the whole authorization lineage while retaining derived subjects.
    /// Incomplete source subject metadata must never become complete downstream.
    pub fn inherit(&self, base: &mut BaseFields) {
        base.security = self.source.base.security.clone();
        for subject in &self.source.base.subjects {
            if !base
                .subjects
                .iter()
                .any(|existing| existing.id() == subject.id())
            {
                base.subjects.push(subject.clone());
            }
        }
        base.subjects
            .sort_by_key(|subject| subject.id().to_string());
        base.subjects_state = self.source.base.subjects_state;
        base.provenance.sources = vec![self.source.base.id.clone()];
        base.provenance.chain = self.revision.base.provenance.chain.clone();
        base.provenance.chain.push(ProvenanceStep {
            step: "grounded_promotion".into(),
            input: self.revision.base.id.to_string(),
            output: base.id.to_string(),
        });
    }
}

/// Validate immutable rendered input before tree building or model dispatch.
pub fn validate_revision_lineage(
    source: &Source,
    doc: &Doc,
    revision: &DocRevision,
    space_id: &str,
) -> Result<(), PipelineError> {
    for base in [&source.base, &doc.base, &revision.base] {
        if base.space_id != space_id
            || base.is_deleted()
            || !same_authorization_partition(base, &source.base)
        {
            return Err(invalid(
                "Grounding source authorization lineage changed or was deleted",
            ));
        }
    }
    if revision.status != RevisionStatus::Active
        || matches!(
            source.lifecycle_status,
            SourceLifecycleStatus::Rejected | SourceLifecycleStatus::Skipped
        )
        || revision.doc.id() != &doc.base.id
        || doc.source.as_ref().map(|source| source.id()) != Some(&source.base.id)
    {
        return Err(invalid(
            "Grounding lineage does not identify one active source revision",
        ));
    }
    if revision.text != doc.text
        || revision.text_hash != AssertionValidator::hash_quote(&revision.text)
    {
        return Err(invalid(
            "Grounding revision content changed or its hash is invalid",
        ));
    }
    let bindings: Vec<_> = revision
        .base
        .provenance
        .chain
        .iter()
        .filter(|step| step.step == SOURCE_CONTENT_STEP)
        .collect();
    if bindings.len() != 1
        || bindings[0].input != source.base.id.to_string()
        || bindings[0].output != source.hash
        || source.hash.is_empty()
    {
        return Err(invalid(
            "Grounding revision lacks an exact current source-content binding",
        ));
    }
    Ok(())
}

/// Exact policy and subject partition (set order is immaterial).
pub fn same_authorization_partition(left: &BaseFields, right: &BaseFields) -> bool {
    fn sorted<T: Ord>(mut values: Vec<T>) -> Vec<T> {
        values.sort();
        values.dedup();
        values
    }
    left.space_id == right.space_id
        && left.security.policy_id == right.security.policy_id
        && left.security.level == right.security.level
        && sorted(left.security.labels.clone()) == sorted(right.security.labels.clone())
        && sorted(left.security.handling.clone()) == sorted(right.security.handling.clone())
        && left.subjects_state == right.subjects_state
        && sorted(
            left.subjects
                .iter()
                .map(|subject| subject.id().to_string())
                .collect(),
        ) == sorted(
            right
                .subjects
                .iter()
                .map(|subject| subject.id().to_string())
                .collect(),
        )
}

/// Exact independent support partition for enrichment and evidence coalescing.
/// A source-exclusive label/alias must not survive by borrowing another source's
/// support. Explicit identity-resolution operations may relate such partitions.
pub fn same_support_partition(left: &BaseFields, right: &BaseFields) -> bool {
    let mut left_sources = left.provenance.sources.clone();
    left_sources.sort_by_key(ToString::to_string);
    left_sources.dedup();
    let mut right_sources = right.provenance.sources.clone();
    right_sources.sort_by_key(ToString::to_string);
    right_sources.dedup();
    let revisions = |base: &BaseFields| {
        let mut revisions: Vec<_> = base
            .provenance
            .chain
            .iter()
            .filter(|step| step.step == "grounded_promotion")
            .map(|step| step.input.clone())
            .collect();
        revisions.sort();
        revisions.dedup();
        revisions
    };
    let left_revisions = revisions(left);
    let right_revisions = revisions(right);
    same_authorization_partition(left, right)
        && left_sources == right_sources
        && !left_sources.is_empty()
        && !left_revisions.is_empty()
        && left_revisions == right_revisions
}
