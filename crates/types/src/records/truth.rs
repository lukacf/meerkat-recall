//! Truth maintenance types for conflict resolution and current truth views.
//!
//! Per spec §12, these types manage conflict groups and truth slots.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::Link;

use super::rel::{ClaimStatus, EvidenceRef};

/// Policy for handling conflicts between competing claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    /// Both coexist; `truth_slot` shows all with confidence ranking.
    #[default]
    AllowParallel,
    /// Newer timestamp wins; older gets `validity_window.end` set.
    SupersedeByRecency,
    /// Role-dependent truths don't conflict.
    SplitByContext,
    /// Don't auto-resolve; flag for review.
    RequireMoreEvidence,
}

impl std::fmt::Display for ConflictPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllowParallel => write!(f, "allow_parallel"),
            Self::SupersedeByRecency => write!(f, "supersede_by_recency"),
            Self::SplitByContext => write!(f, "split_by_context"),
            Self::RequireMoreEvidence => write!(f, "require_more_evidence"),
        }
    }
}

impl FromStr for ConflictPolicy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "allow_parallel" => Ok(Self::AllowParallel),
            "supersede_by_recency" => Ok(Self::SupersedeByRecency),
            "split_by_context" => Ok(Self::SplitByContext),
            "require_more_evidence" => Ok(Self::RequireMoreEvidence),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "conflict_policy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ConflictPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ConflictPolicyVisitor;

        impl Visitor<'_> for ConflictPolicyVisitor {
            type Value = ConflictPolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ConflictPolicy")
            }

            fn visit_str<E>(self, value: &str) -> Result<ConflictPolicy, E>
            where
                E: de::Error,
            {
                ConflictPolicy::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ConflictPolicyVisitor)
    }
}

/// A group of competing claims for the same slot.
///
/// Per spec §12.3: groups rel/attribute records that compete for the same truth slot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConflictGroup {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Deterministic slot key.
    /// For attributes: `(subject_entity, attr_type, context_bucket?, security_hash)`
    /// For rels: `(in, out, rel_type, security_hash)`
    pub slot_key: String,

    /// Type of slot ("rel" or "attribute").
    pub slot_type: String,

    /// IDs of competing claims.
    #[serde(default)]
    pub claim_ids: Vec<String>,

    /// Current resolution status.
    #[serde(default)]
    pub resolved: bool,

    /// Resolution rationale (if resolved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_rationale: Option<String>,

    /// When the conflict was detected.
    pub detected_at: DateTime<Utc>,

    /// When the conflict was resolved (if resolved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,

    /// Hash of the claim set that was last LLM-adjudicated (memoization).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adjudicated_claim_set_hash: Option<String>,

    /// When the claim set was last LLM-adjudicated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adjudicated_at: Option<DateTime<Utc>>,
}

/// A current value in a truth slot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurrentValue {
    /// The record ID of the winning claim.
    pub claim_id: String,

    /// Confidence score (0.0 to 1.0).
    pub confidence: f32,

    /// Status of the claim.
    pub status: ClaimStatus,

    /// Supporting evidence refs.
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

/// The current best answer for a truth slot.
///
/// Per spec §12.4: stores the computed "current truth" for fast queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TruthSlot {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Deterministic slot key (same as `ConflictGroup.slot_key`).
    pub slot_key: String,

    /// Type of slot ("rel" or "attribute").
    pub slot_type: String,

    /// Subject entity ID.
    pub subject_entity: String,

    /// Predicate ID.
    pub predicate_id: String,

    /// One or more current values (for `AllowParallel` policy).
    #[serde(default)]
    pub current_values: Vec<CurrentValue>,

    /// Link to conflict group (if unresolved conflicts exist).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_group: Option<Link<ConflictGroup>>,

    /// Overall confidence for this slot.
    #[serde(default)]
    pub confidence: f32,

    /// Rationale for the current truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,

    /// Version of predicate semantics used.
    #[serde(default)]
    pub semantics_version: u32,

    /// Version of alias mappings used.
    #[serde(default)]
    pub alias_version: u32,

    /// Whether this slot is stale and needs recomputation.
    #[serde(default)]
    pub stale: bool,

    /// Reason for staleness (if stale).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,

    /// When this slot was last computed.
    pub computed_at: DateTime<Utc>,

    /// Security hash for ABAC envelope.
    pub security_hash: String,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;

    #[test]
    fn test_conflict_policy_roundtrip() {
        for policy in [
            ConflictPolicy::AllowParallel,
            ConflictPolicy::SupersedeByRecency,
            ConflictPolicy::SplitByContext,
            ConflictPolicy::RequireMoreEvidence,
        ] {
            let s = policy.to_string();
            let parsed = ConflictPolicy::from_str(&s).unwrap();
            assert_eq!(policy, parsed);
        }
    }

    #[test]
    fn test_conflict_policy_serde() {
        let policy = ConflictPolicy::SupersedeByRecency;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, "\"supersede_by_recency\"");

        let parsed: ConflictPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, policy);
    }
}
