//! Source acceptance, publication, and attempt contracts shared by all adapters.
//!
//! A generation orders accepted intent, including a pending replacement. A
//! revision identifies immutable source content; it is never an ordering token.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::records::{Entity, Security, Source, SubjectsState};
use crate::{Link, RecordId};

/// Maximum decoded source text size, measured in UTF-8 bytes.
pub const MAX_SOURCE_TEXT_BYTES: usize = 2_000_000;

/// Minimal policy metadata retained to authorize status after payload erasure.
/// This contains no source text, metadata, external identifier, or title.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceAuthorizationEnvelope {
    /// Owning space; never inferred from the caller's default.
    pub space_id: String,
    /// Physical source incarnation this envelope authorizes.
    pub source_id: RecordId,
    /// Original classification and policy labels.
    pub security: Security,
    /// Subject-scoping references required by the policy engine.
    pub subjects: Vec<Link<Entity>>,
    /// Completeness of subject scoping.
    pub subjects_state: SubjectsState,
}

/// Authorized pre-state carried into a mutation to fence late policy changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceAuthorizationSnapshot {
    /// Minimal policy metadata actually evaluated by the caller.
    pub envelope: SourceAuthorizationEnvelope,
    /// Observed source version; absent only after physical deletion.
    pub source_updated_at: Option<DateTime<Utc>>,
    /// Observed lifecycle deletion marker, including guarded restoration.
    pub source_deleted_at: Option<DateTime<Utc>>,
}

impl From<&Source> for SourceAuthorizationSnapshot {
    fn from(source: &Source) -> Self {
        Self {
            envelope: SourceAuthorizationEnvelope::from(source),
            source_updated_at: Some(source.base.updated_at),
            source_deleted_at: source.base.deleted_at,
        }
    }
}

impl From<&Source> for SourceAuthorizationEnvelope {
    fn from(source: &Source) -> Self {
        Self {
            space_id: source.base.space_id.clone(),
            source_id: source.base.id.clone(),
            security: source.base.security.clone(),
            subjects: source.base.subjects.clone(),
            subjects_state: source.base.subjects_state,
        }
    }
}

/// Visibility of the current source incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceVisibility {
    /// Accepted content can be processed and published.
    Active,
    /// All support from this source has been withdrawn.
    Retracted,
    /// Erasure is reserved and visibility fenced while durable cleanup runs.
    Erasing,
    /// Payload has been physically removed; this incarnation cannot be retried.
    Erased,
}

/// An explicit acceptance precondition; omission must never mean blind replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "generation", rename_all = "snake_case")]
pub enum SourcePrecondition {
    /// Create only, provided no head has ever existed for this source ID.
    Absent,
    /// Change exactly this accepted head, including when publication is pending.
    Generation(String),
}

/// Sole authority for accepted and published source revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHead {
    /// Explicit owning space.
    pub space_id: String,
    /// Opaque source identity; external identities are resolved within a space.
    pub source_id: RecordId,
    /// Opaque compare-and-swap token for the accepted intent.
    pub generation: String,
    /// Accepted revision, which may still be processing.
    pub desired_revision: Option<String>,
    /// Last completely published revision, distinct from desired revision.
    pub published_revision: Option<String>,
    /// Operation that accepted this generation.
    pub operation_id: String,
    /// Immediate source visibility and publication eligibility.
    pub lifecycle: SourceVisibility,
    /// Time of the last head transition.
    pub updated_at: DateTime<Utc>,
}

/// Immutable accepted source snapshot. Erasure deletes the entire record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRevision {
    /// Explicit owning space.
    pub space_id: String,
    /// Source this revision belongs to.
    pub source_id: RecordId,
    /// Consumer-assigned immutable revision identity.
    pub revision_id: String,
    /// Content fingerprint; does not convey ordering or completeness.
    pub content_hash: String,
    /// Time the revision was accepted.
    pub accepted_at: DateTime<Utc>,
    /// Complete source envelope and staged input accepted for this revision.
    pub source: Source,
}

/// Durable operation state, independent of a particular worker attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    /// Desired head and immutable input are durably accepted.
    Accepted,
    /// Work is in progress; publication has not completed.
    Processing,
    /// All required canonical effects, outbox, receipt, and search coverage are ready.
    Completed,
    /// A later accepted head fenced this operation.
    Superseded,
    /// Work failed and can be retried against the same current generation.
    FailedRetryable,
    /// Work cannot complete without a new request or operator action.
    FailedTerminal,
    /// Source support has been withdrawn.
    Retracted,
    /// Source visibility is fenced while physical cleanup is outstanding.
    Erasing,
    /// Live payload and exclusive support were physically removed.
    Erased,
}

/// Public durable receipt. It contains no source payload or external identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationReceipt {
    /// Explicit owning space.
    pub space_id: String,
    /// Stable identity returned on identical idempotent retries.
    pub operation_id: String,
    /// Source incarnation affected by the operation.
    pub source_id: RecordId,
    /// Accepted generation, retained even after supersession.
    pub source_generation: String,
    /// Requested revision.
    pub desired_revision: Option<String>,
    /// Revision completely published at the receipt's last transition.
    pub published_revision: Option<String>,
    /// Durable semantic state; queue success alone cannot set completion.
    pub state: OperationState,
    /// Acceptance timestamp.
    pub accepted_at: DateTime<Utc>,
    /// Completion timestamp for terminal semantic outcomes.
    pub completed_at: Option<DateTime<Utc>>,
    /// Stable safe error code, never a provider response or source excerpt.
    pub error_code: Option<String>,
}

/// Authority issued by the queue's dedicated durable attempt row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFence {
    /// Explicit owning space.
    pub space_id: String,
    /// Work item whose current attempt may publish.
    pub work_item_id: RecordId,
    /// Monotonic, never-reset claim generation; distinct from retry count.
    pub generation: u64,
    /// Unpredictable per-claim ownership token.
    pub lease_token: String,
}

/// Source fence copied from acceptance through every successor work payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFence {
    /// Source incarnation that supplied the input.
    pub source_id: RecordId,
    /// Accepted head generation; never reconstructed from a later head.
    pub generation: String,
    /// Exact immutable source revision being processed.
    pub revision_id: String,
    /// Original operation identity.
    pub operation_id: String,
}

/// Original producer of an accepted source operation. This is private accounting
/// provenance, never an authorization grant or a field in public receipts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceProducerOrigin {
    /// Pre-link or missing evidence cannot establish an unrestricted producer.
    #[default]
    UnknownOriginal,
    /// Explicit direct intake, with no reprocessing producer.
    Direct,
    /// Actual original watch execution; later accounting must follow this
    /// work item's private producer ancestry and retain its restrictions.
    Watch {
        /// Original watch identity.
        watch_id: RecordId,
        /// Full original watch observation, independent of later watch progress.
        watch_fingerprint: String,
        /// Actual admitting queue work.
        work_item_id: RecordId,
        /// Actual original execution receipt, retained through archive.
        work_run_id: RecordId,
        /// Original admitting claim; historical verification never refreshes it.
        original_attempt: AttemptFence,
        /// B's immutable work intent, excluding mutable runtime bookkeeping.
        work_intent_fingerprint: String,
    },
    /// Exact original saved reprocessing intent and its immutable restriction.
    Reprocess {
        /// Original job; missing/deleted referenced evidence refuses dispatch.
        job_id: RecordId,
        /// Saved selection manifest in the coordinator's private partition.
        selection_artifact_id: RecordId,
        /// Saved source intent, retaining its source authorization partition.
        source_intent_id: RecordId,
        /// Original closed selection digest.
        selection_digest: String,
        /// Exact saved source-intent content fingerprint.
        intent_fingerprint: String,
        /// Finite currency restriction observed at admission, never relaxed by retry.
        currency_restricted: bool,
        /// Original admitting queue claim; absent only for trusted direct work.
        original_attempt: Option<AttemptFence>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_roundtrip_keeps_pending_and_published_revisions_distinct(
    ) -> Result<(), serde_json::Error> {
        let head = SourceHead {
            space_id: "business-a".into(),
            source_id: RecordId::new_v4(crate::TableName::Source),
            generation: "opaque-next-head".into(),
            desired_revision: Some("rev-2".into()),
            published_revision: Some("rev-1".into()),
            operation_id: "operation-2".into(),
            lifecycle: SourceVisibility::Active,
            updated_at: Utc::now(),
        };
        let decoded: SourceHead = serde_json::from_str(&serde_json::to_string(&head)?)?;
        assert_eq!(decoded, head);
        assert_ne!(decoded.desired_revision, decoded.published_revision);
        Ok(())
    }

    #[test]
    fn missing_generation_is_not_a_blind_replace() {
        assert!(serde_json::from_str::<SourcePrecondition>(r#"{"kind":"generation"}"#).is_err());
        assert!(serde_json::from_str::<SourcePrecondition>("null").is_err());
    }
}
