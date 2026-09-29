//! v0.8 Rel type for entity-to-entity relationships.
//!
//! Per spec §11.2, rel records represent resolved entity-to-entity claims.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::records::entity::Entity;
use crate::Link;

use super::assertion::EvidenceSpan;
use super::temporal::TemporalAudit;

/// Status of a rel/attribute in the knowledge graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    /// Resolved but not yet validated.
    #[default]
    Candidate,
    /// Validated and promoted (= "fact").
    Approved,
    /// Replaced by newer evidence.
    Superseded,
    /// Conflicting evidence, requires resolution.
    Disputed,
}

impl std::fmt::Display for ClaimStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Candidate => write!(f, "candidate"),
            Self::Approved => write!(f, "approved"),
            Self::Superseded => write!(f, "superseded"),
            Self::Disputed => write!(f, "disputed"),
        }
    }
}

impl FromStr for ClaimStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "candidate" => Ok(Self::Candidate),
            "approved" => Ok(Self::Approved),
            "superseded" => Ok(Self::Superseded),
            "disputed" => Ok(Self::Disputed),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "claim_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ClaimStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ClaimStatusVisitor;

        impl Visitor<'_> for ClaimStatusVisitor {
            type Value = ClaimStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ClaimStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<ClaimStatus, E>
            where
                E: de::Error,
            {
                ClaimStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ClaimStatusVisitor)
    }
}

/// Time range for validity windows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidityWindow {
    /// Start of validity (None = unknown/unbounded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<DateTime<Utc>>,

    /// End of validity (None = ongoing/current).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<DateTime<Utc>>,
}

impl ValidityWindow {
    /// Creates an unbounded (always valid) window.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// Creates a window that starts at a given time and is ongoing.
    #[must_use]
    pub const fn starting_at(start: DateTime<Utc>) -> Self {
        Self {
            start: Some(start),
            end: None,
        }
    }

    /// Creates a closed window with both start and end.
    #[must_use]
    pub const fn closed(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start: Some(start),
            end: Some(end),
        }
    }

    /// Returns true if this is an ongoing (open-ended) window.
    #[must_use]
    pub const fn is_ongoing(&self) -> bool {
        self.end.is_none()
    }

    /// Returns true if the window contains the given timestamp.
    #[must_use]
    pub fn contains(&self, ts: DateTime<Utc>) -> bool {
        let after_start = self.start.is_none_or(|s| ts >= s);
        let before_end = self.end.is_none_or(|e| ts < e);
        after_start && before_end
    }
}

/// Evidence reference for a rel/attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRef {
    /// The raw assertion this evidence came from.
    pub assertion: Link<super::assertion::Assertion>,

    /// The evidence span in the source document.
    pub span: EvidenceSpan,

    /// Which timestamp was used for recency determination.
    pub time_basis: TimeBasis,

    /// The timestamp value used.
    pub time_value: DateTime<Utc>,
}

/// Which timestamp was used for recency determination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    /// Used `source_event_time` (when the thing happened).
    SourceEventTime,
    /// Used `observed_at` (when we ingested the document).
    #[default]
    ObservedAt,
    /// Used `asserted_at` (when extraction ran).
    AssertedAt,
}

impl std::fmt::Display for TimeBasis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceEventTime => write!(f, "source_event_time"),
            Self::ObservedAt => write!(f, "observed_at"),
            Self::AssertedAt => write!(f, "asserted_at"),
        }
    }
}

impl FromStr for TimeBasis {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "source_event_time" => Ok(Self::SourceEventTime),
            "observed_at" => Ok(Self::ObservedAt),
            "asserted_at" => Ok(Self::AssertedAt),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "time_basis".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for TimeBasis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct TimeBasisVisitor;

        impl Visitor<'_> for TimeBasisVisitor {
            type Value = TimeBasis;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for TimeBasis")
            }

            fn visit_str<E>(self, value: &str) -> Result<TimeBasis, E>
            where
                E: de::Error,
            {
                TimeBasis::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(TimeBasisVisitor)
    }
}

/// An entity-to-entity relationship in the knowledge graph.
///
/// Per spec §11.2: rels represent resolved claims linking two entities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rel {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Subject entity (the "from" side).
    #[serde(rename = "in")]
    pub subject: Link<Entity>,

    /// Object entity (the "to" side).
    #[serde(rename = "out")]
    pub object: Link<Entity>,

    /// Canonical predicate from the registry.
    pub rel_type: String,

    /// Current status of this rel.
    #[serde(default)]
    pub status: ClaimStatus,

    /// Confidence score (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Validity window for temporal claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity_window: Option<ValidityWindow>,

    /// Evidence supporting this rel.
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,

    /// Temporal audit information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_audit: Option<TemporalAudit>,
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
    fn test_claim_status_roundtrip() {
        for status in [
            ClaimStatus::Candidate,
            ClaimStatus::Approved,
            ClaimStatus::Superseded,
            ClaimStatus::Disputed,
        ] {
            let s = status.to_string();
            let parsed = ClaimStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_validity_window_contains() {
        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);
        let later = now + chrono::Duration::hours(1);

        let unbounded = ValidityWindow::unbounded();
        assert!(unbounded.contains(now));

        let ongoing = ValidityWindow::starting_at(earlier);
        assert!(ongoing.contains(now));
        assert!(!ongoing.contains(earlier - chrono::Duration::seconds(1)));

        let closed = ValidityWindow::closed(earlier, later);
        assert!(closed.contains(now));
        assert!(!closed.contains(later + chrono::Duration::seconds(1)));
    }

    #[test]
    fn test_time_basis_serde() {
        let basis = TimeBasis::SourceEventTime;
        let json = serde_json::to_string(&basis).unwrap();
        assert_eq!(json, "\"source_event_time\"");

        let parsed: TimeBasis = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, basis);
    }
}
