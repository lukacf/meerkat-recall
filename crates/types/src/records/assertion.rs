//! Assertion types for raw extracted claims.
//!
//! Per spec §2.3 and §9.5, assertions are unresolved claims extracted from documents.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::records::entity::Entity;
use crate::Link;

use super::doc_node::DocNode;
use super::doc_revision::DocRevision;

/// Resolution status of an assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    /// Not yet attempted resolution.
    #[default]
    Pending,
    /// Successfully resolved to entities.
    Resolved,
    /// Partially resolved (subject or object resolved, not both).
    Partial,
    /// Resolution abstained due to ambiguity/low confidence.
    Abstained,
    /// Resolution failed after max attempts.
    Failed,
    /// Replaced by a newer assertion from reprocessing.
    Superseded,
}

impl std::fmt::Display for ResolutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Resolved => write!(f, "resolved"),
            Self::Partial => write!(f, "partial"),
            Self::Abstained => write!(f, "abstained"),
            Self::Failed => write!(f, "failed"),
            Self::Superseded => write!(f, "superseded"),
        }
    }
}

impl FromStr for ResolutionStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "resolved" => Ok(Self::Resolved),
            "partial" => Ok(Self::Partial),
            "abstained" => Ok(Self::Abstained),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "resolution_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ResolutionStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ResolutionStatusVisitor;

        impl Visitor<'_> for ResolutionStatusVisitor {
            type Value = ResolutionStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ResolutionStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<ResolutionStatus, E>
            where
                E: de::Error,
            {
                ResolutionStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ResolutionStatusVisitor)
    }
}

/// Retry state for assertion resolution.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AssertionRetryState {
    /// Number of resolution attempts.
    #[serde(default)]
    pub resolution_attempts: u32,

    /// When resolution was last attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_resolution_at: Option<DateTime<Utc>>,

    /// Reason for abstention (if abstained).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abstention_reason: Option<String>,
}

/// Evidence reference pointing to a span in a document revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceSpan {
    /// The document revision containing the evidence.
    pub revision: Link<DocRevision>,

    /// The specific node within the revision (if applicable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<Link<DocNode>>,

    /// Start offset in the revision text (byte index).
    pub start: u32,

    /// End offset in the revision text (byte index).
    pub end: u32,

    /// Hash of the quoted text (for integrity, avoids storing raw quote).
    pub quote_hash: String,
}

/// A raw assertion extracted from a document.
///
/// Per spec §2.3: assertions are unresolved claims with raw mention strings.
/// They are resolved to entities in a later stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assertion {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Raw subject mention string (not yet resolved to entity).
    pub subject_text: String,

    /// Uncanonicalized predicate from extraction.
    pub predicate_raw: String,

    /// Canonical predicate ID (from predicate registry), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_id: Option<String>,

    /// Raw object mention string (for entity-to-entity claims).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_text: Option<String>,

    /// Literal value (for entity-to-literal claims).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_literal: Option<serde_json::Value>,

    /// Evidence span pointing to source document.
    pub evidence: EvidenceSpan,

    /// When the asserted claim occurred in the real world (if known).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_time: Option<DateTime<Utc>>,

    /// When the document was observed/ingested.
    pub observed_at: DateTime<Utc>,

    /// When this assertion was extracted.
    pub asserted_at: DateTime<Utc>,

    /// Idempotency key for deduplication.
    pub assertion_key: String,

    /// Resolution status.
    #[serde(default)]
    pub resolution_status: ResolutionStatus,

    /// Method used to resolve (or abstain).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_method: Option<String>,

    /// Confidence of the resolution decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_confidence: Option<f32>,

    /// Retry state for resolution.
    #[serde(default)]
    pub retry_state: AssertionRetryState,

    /// Resolved subject entity (if resolution succeeded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_entity: Option<Link<Entity>>,

    /// Resolved object entity (if resolution succeeded for entity-to-entity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_entity: Option<Link<Entity>>,

    /// Confidence score from extraction (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Entity type classification for the subject (e.g. "person", "org", "topic").
    /// Populated during extraction for use in inline promotion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_entity_type: Option<String>,

    /// Entity type classification for the object (e.g. "person", "org", "topic").
    /// Populated during extraction for use in inline promotion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_entity_type: Option<String>,

    /// Significance assessment given the space purpose: "skip", "low", "medium", "high".
    /// Populated during extraction for use in inline promotion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub significance: Option<String>,

    /// Lifecycle tier for assertion archival: "active" or "archived".
    /// Assertions older than a configurable threshold with `resolution_status` = resolved
    /// get moved to "archived". Archived assertions are excluded from default queries
    /// but remain for audit and provenance tracing.
    #[serde(default = "default_lifecycle_tier")]
    pub lifecycle_tier: String,
}

fn default_lifecycle_tier() -> String {
    "active".to_string()
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
    fn test_resolution_status_roundtrip() {
        for status in [
            ResolutionStatus::Pending,
            ResolutionStatus::Resolved,
            ResolutionStatus::Partial,
            ResolutionStatus::Abstained,
            ResolutionStatus::Failed,
        ] {
            let s = status.to_string();
            let parsed = ResolutionStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_resolution_status_serde() {
        let status = ResolutionStatus::Abstained;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"abstained\"");

        let parsed: ResolutionStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_retry_state_default() {
        let state = AssertionRetryState::default();
        assert_eq!(state.resolution_attempts, 0);
        assert!(state.last_resolution_at.is_none());
        assert!(state.abstention_reason.is_none());
    }

    #[test]
    fn test_assertion_lifecycle_defaults_to_active() {
        // Verify the default function
        assert_eq!(default_lifecycle_tier(), "active");

        // Verify deserialization with explicit value
        let json = r#"{"lifecycle_tier": "active"}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["lifecycle_tier"], "active");

        // Verify deserialization with "archived" value
        let json = r#"{"lifecycle_tier": "archived"}"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["lifecycle_tier"], "archived");
    }
}
