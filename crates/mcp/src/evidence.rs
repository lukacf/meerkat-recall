//! Shared immutable evidence validation for HTTP and MCP reads.
//!
//! Callers authorize every returned record before exposing any part of the
//! chain. Missing or inconsistent lineage never produces a partial citation.

use elephant_storage::{AssertionRepository, Repository, StorageClient};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::{Assertion, Doc, DocNode, DocRevision, Source};
use elephant_types::RecordId;

use crate::{McpError, McpResult};

/// Resolved evidence whose offsets, immutable text and assertion agree.
pub struct EvidenceChain {
    /// Assertion supporting the citation, when the caller supplied one.
    pub assertion: Option<Assertion>,
    /// Immutable rendered text.
    pub revision: DocRevision,
    /// Document that owns the revision.
    pub doc: Doc,
    /// Current source envelope, if this is a source-backed document.
    pub source: Option<Source>,
    /// Exact node, if recorded by the citation.
    pub node: Option<DocNode>,
    /// Verified nonempty quote at the supplied UTF-8 byte offsets.
    pub quote: String,
}

fn unavailable() -> McpError {
    McpError::ExecutionError("Evidence unavailable or inconsistent".into())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Consume the storage error through the existing map_err function interface."
)]
fn storage_error(error: elephant_storage::StorageError) -> McpError {
    McpError::ExecutionError(error.to_string())
}

/// Resolve immutable evidence, checking every parent, offset and digest.
///
/// # Errors
/// Returns an error for absent/deleted/foreign lineage or inconsistent evidence.
pub async fn load_evidence(
    storage: &StorageClient,
    space: &str,
    span: &EvidenceSpan,
    assertion_id: Option<&RecordId>,
) -> McpResult<EvidenceChain> {
    let assertion = if let Some(id) = assertion_id {
        let assertion = AssertionRepository::new(storage.clone())
            .get(space, id)
            .await
            .map_err(storage_error)?
            .ok_or_else(unavailable)?;
        // Comparing only revision IDs permits a claim to cite another passage.
        if assertion.evidence != *span {
            return Err(unavailable());
        }
        let current = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone())
            .project(assertion, &|_| true)
            .await
            .map_err(storage_error)?
            .ok_or_else(unavailable)?;
        Some(current)
    } else {
        None
    };
    let resolved = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone())
        .evidence(space, span, &|_| true)
        .await
        .map_err(storage_error)?
        .ok_or_else(unavailable)?;
    let elephant_storage::read_eligibility::CurrentEvidence {
        revision,
        doc,
        source,
        node,
        quote,
    } = resolved;
    Ok(EvidenceChain {
        assertion,
        revision,
        doc,
        source,
        node,
        quote,
    })
}
