//! Stable consumer projections over the storage-owned source state.
use crate::publication::{OperationReceipt, OperationState, SourceHead, SourceVisibility};
use serde::Serialize;

/// Acceptance metadata shared by HTTP and MCP lifecycle responses.
#[derive(Debug, Serialize)]
pub struct AcceptedSource {
    /// Opaque durable operation identity.
    pub operation_id: String,
    /// Opaque physical incarnation; distinct from `evidence.source_id` (external).
    pub internal_source_id: crate::RecordId,
    /// Accepted source generation; never a content hash.
    pub generation: String,
    /// Accepted immutable revision, when this operation has one.
    pub desired_revision: Option<String>,
    /// Revision with complete required search coverage observed by this receipt.
    pub published_revision: Option<String>,
    /// Consumer lifecycle: active, retracted, or erased.
    pub lifecycle: &'static str,
    /// Physical cleanup state, separate from publication.
    pub cleanup_state: &'static str,
}
/// Payload-free durable operation status, with consumer-stable state names.
///
/// Completed receipts describe historical outcomes; consult the current head and
/// revision-constrained search coverage for present visibility.
#[derive(Debug, Serialize)]
pub struct SourceOperation {
    /// Opaque durable operation identity.
    pub operation_id: String,
    /// One of the agreed five public operation states.
    pub state: &'static str,
    /// Opaque physical incarnation; distinct from `evidence.source_id` (external).
    pub internal_source_id: crate::RecordId,
    /// Accepted source generation; never a content hash.
    pub generation: String,
    /// Accepted immutable revision, when this operation has one.
    pub desired_revision: Option<String>,
    /// Revision with complete required search coverage observed by this receipt.
    pub published_revision: Option<String>,
    /// Consumer lifecycle: active, retracted, or erased.
    pub lifecycle: &'static str,
    /// Physical cleanup state, separate from publication.
    pub cleanup_state: &'static str,
    /// Safe terminal or retryable reason, including superseded.
    pub error_code: Option<String>,
}

/// Project durable state without exposing internal scheduler terminology.
#[must_use]
pub const fn public_state(state: OperationState) -> &'static str {
    match state {
        OperationState::Accepted => "pending",
        OperationState::Processing | OperationState::Erasing => "running",
        OperationState::Completed | OperationState::Retracted | OperationState::Erased => {
            "complete"
        }
        OperationState::FailedRetryable => "failed_retryable",
        OperationState::Superseded | OperationState::FailedTerminal => "failed_final",
    }
}
/// Read the public processing state of a durable receipt.
#[must_use]
pub const fn operation_state(receipt: &OperationReceipt) -> &'static str {
    public_state(receipt.state)
}
/// Project the lifecycle outcome represented by this receipt.
#[must_use]
pub const fn lifecycle(receipt: &OperationReceipt) -> &'static str {
    match receipt.state {
        OperationState::Retracted => "retracted",
        OperationState::Erasing | OperationState::Erased => "erased",
        _ => "active",
    }
}
/// Project visibility; erasure in progress already withdraws public visibility.
#[must_use]
pub const fn head_lifecycle(visibility: SourceVisibility) -> &'static str {
    match visibility {
        SourceVisibility::Active => "active",
        SourceVisibility::Retracted => "retracted",
        SourceVisibility::Erasing | SourceVisibility::Erased => "erased",
    }
}
/// Keep pending physical erasure distinct from completed cleanup.
#[must_use]
pub const fn cleanup(receipt: &OperationReceipt) -> &'static str {
    match receipt.state {
        OperationState::Erasing => "pending",
        OperationState::Erased | OperationState::Retracted => "complete",
        _ => "not_required",
    }
}
/// Build acceptance metadata without payload or policy metadata.
#[must_use]
pub fn accepted_source(receipt: &OperationReceipt) -> AcceptedSource {
    AcceptedSource {
        operation_id: receipt.operation_id.clone(),
        internal_source_id: receipt.source_id.clone(),
        generation: receipt.source_generation.clone(),
        desired_revision: receipt.desired_revision.clone(),
        published_revision: receipt.published_revision.clone(),
        lifecycle: lifecycle(receipt),
        cleanup_state: cleanup(receipt),
    }
}
/// Build a status response and preserve an explicit superseded reason.
#[must_use]
pub fn operation(receipt: &OperationReceipt) -> SourceOperation {
    SourceOperation {
        operation_id: receipt.operation_id.clone(),
        internal_source_id: receipt.source_id.clone(),
        state: operation_state(receipt),
        generation: receipt.source_generation.clone(),
        desired_revision: receipt.desired_revision.clone(),
        published_revision: receipt.published_revision.clone(),
        lifecycle: lifecycle(receipt),
        cleanup_state: cleanup(receipt),
        error_code: if receipt.state == OperationState::Superseded {
            Some("superseded".into())
        } else {
            receipt.error_code.clone()
        },
    }
}
/// Build the canonical head shape from an authorized consistent observation.
#[must_use]
pub fn head(head: &SourceHead, receipt: &OperationReceipt) -> serde_json::Value {
    serde_json::json!({"internal_source_id":head.source_id,"operation_id":head.operation_id,"generation":head.generation,"desired_revision":head.desired_revision,"published_revision":head.published_revision,"lifecycle":head_lifecycle(head.lifecycle),"cleanup_state":cleanup(receipt),"state":operation_state(receipt)})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_the_agreed_five_operation_states_are_exposed() {
        for (state, expected) in [
            (OperationState::Accepted, "pending"),
            (OperationState::Processing, "running"),
            (OperationState::Completed, "complete"),
            (OperationState::FailedRetryable, "failed_retryable"),
            (OperationState::FailedTerminal, "failed_final"),
            (OperationState::Superseded, "failed_final"),
            (OperationState::Retracted, "complete"),
            (OperationState::Erasing, "running"),
            (OperationState::Erased, "complete"),
        ] {
            assert_eq!(public_state(state), expected);
        }
        assert_eq!(head_lifecycle(SourceVisibility::Erasing), "erased");
    }
}
