//! Temporal model types for time-varying knowledge.
//!
//! Per spec §10.5, these types handle how truth maintenance manages competing claims over time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::Link;

use super::assertion::Assertion;

/// Temporal personality of a predicate.
///
/// Determines how truth maintenance handles competing claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalPersonality {
    /// Once true, always true. New evidence corroborates or conflicts.
    /// Examples: birthplace, `biological_parent`, `date_of_birth`
    Persistent,
    /// True at a point in time, but can change. Creates validity windows.
    /// Examples: employer, `job_title`, residence, `relationship_status`
    #[default]
    StateOverTime,
    /// Happened at a specific time. Multiple event-like rels accumulate.
    /// Examples: `attended_meeting`, `sent_email`, `made_purchase`
    EventLike,
}

impl std::fmt::Display for TemporalPersonality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Persistent => write!(f, "persistent"),
            Self::StateOverTime => write!(f, "state_over_time"),
            Self::EventLike => write!(f, "event_like"),
        }
    }
}

impl FromStr for TemporalPersonality {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "persistent" => Ok(Self::Persistent),
            "state_over_time" => Ok(Self::StateOverTime),
            "event_like" => Ok(Self::EventLike),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "temporal_personality".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for TemporalPersonality {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct TemporalPersonalityVisitor;

        impl Visitor<'_> for TemporalPersonalityVisitor {
            type Value = TemporalPersonality;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for TemporalPersonality")
            }

            fn visit_str<E>(self, value: &str) -> Result<TemporalPersonality, E>
            where
                E: de::Error,
            {
                TemporalPersonality::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(TemporalPersonalityVisitor)
    }
}

/// Which timestamp basis is being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecencyBasis {
    /// Use `source_event_time` (when the thing happened).
    SourceEventTime,
    /// Use `observed_at` (when we ingested the document).
    #[default]
    ObservedAt,
    /// Use `asserted_at` (when extraction ran).
    AssertedAt,
}

impl std::fmt::Display for RecencyBasis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceEventTime => write!(f, "source_event_time"),
            Self::ObservedAt => write!(f, "observed_at"),
            Self::AssertedAt => write!(f, "asserted_at"),
        }
    }
}

impl FromStr for RecencyBasis {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "source_event_time" => Ok(Self::SourceEventTime),
            "observed_at" => Ok(Self::ObservedAt),
            "asserted_at" => Ok(Self::AssertedAt),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "recency_basis".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for RecencyBasis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct RecencyBasisVisitor;

        impl Visitor<'_> for RecencyBasisVisitor {
            type Value = RecencyBasis;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for RecencyBasis")
            }

            fn visit_str<E>(self, value: &str) -> Result<RecencyBasis, E>
            where
                E: de::Error,
            {
                RecencyBasis::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(RecencyBasisVisitor)
    }
}

/// Refinement information when "implicit now" is updated with explicit timestamps.
///
/// Per spec §10.5.3a: tracks when temporal data is refined by better evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeRefinement {
    /// The original time basis before refinement.
    pub original_basis: RecencyBasis,

    /// The original timestamp value.
    pub original_value: DateTime<Utc>,

    /// The assertion that provided the refined timestamp.
    pub refined_by: Link<Assertion>,

    /// When the refinement occurred.
    pub refined_at: DateTime<Utc>,
}

/// Temporal audit information for a rel/attribute.
///
/// Per spec §10.5.3a: tracks which timestamp was used and any refinements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalAudit {
    /// Which timestamp basis is currently being used.
    pub time_basis: RecencyBasis,

    /// Refinement history (if the timestamp was updated by better evidence).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_refinement: Option<TimeRefinement>,
}

/// Policy for handling implicit "now" in claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImplicitNowPolicy {
    /// Assume "now" means when we ingested the document (`observed_at`).
    #[default]
    TreatAsObservedAt,
    /// Reject claims without explicit temporal grounding.
    Reject,
    /// Store with null `source_event_time`, use fallback.
    AllowUnknown,
}

impl std::fmt::Display for ImplicitNowPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TreatAsObservedAt => write!(f, "treat_as_observed_at"),
            Self::Reject => write!(f, "reject"),
            Self::AllowUnknown => write!(f, "allow_unknown"),
        }
    }
}

impl FromStr for ImplicitNowPolicy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "treat_as_observed_at" => Ok(Self::TreatAsObservedAt),
            "reject" => Ok(Self::Reject),
            "allow_unknown" => Ok(Self::AllowUnknown),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "implicit_now_policy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ImplicitNowPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ImplicitNowPolicyVisitor;

        impl Visitor<'_> for ImplicitNowPolicyVisitor {
            type Value = ImplicitNowPolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ImplicitNowPolicy")
            }

            fn visit_str<E>(self, value: &str) -> Result<ImplicitNowPolicy, E>
            where
                E: de::Error,
            {
                ImplicitNowPolicy::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ImplicitNowPolicyVisitor)
    }
}

/// Policy for refining implicit timestamps when better evidence arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefinementPolicy {
    /// Update earlier claim's timestamps based on new evidence.
    #[default]
    RefineImplicit,
    /// Treat explicit timestamp as more authoritative; supersede implicit.
    SupersedeImplicit,
    /// Retain both claims; let confidence scoring sort it out.
    KeepBoth,
}

impl std::fmt::Display for RefinementPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RefineImplicit => write!(f, "refine_implicit"),
            Self::SupersedeImplicit => write!(f, "supersede_implicit"),
            Self::KeepBoth => write!(f, "keep_both"),
        }
    }
}

impl FromStr for RefinementPolicy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "refine_implicit" => Ok(Self::RefineImplicit),
            "supersede_implicit" => Ok(Self::SupersedeImplicit),
            "keep_both" => Ok(Self::KeepBoth),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "refinement_policy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for RefinementPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct RefinementPolicyVisitor;

        impl Visitor<'_> for RefinementPolicyVisitor {
            type Value = RefinementPolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for RefinementPolicy")
            }

            fn visit_str<E>(self, value: &str) -> Result<RefinementPolicy, E>
            where
                E: de::Error,
            {
                RefinementPolicy::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(RefinementPolicyVisitor)
    }
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
    fn test_temporal_personality_roundtrip() {
        for personality in [
            TemporalPersonality::Persistent,
            TemporalPersonality::StateOverTime,
            TemporalPersonality::EventLike,
        ] {
            let s = personality.to_string();
            let parsed = TemporalPersonality::from_str(&s).unwrap();
            assert_eq!(personality, parsed);
        }
    }

    #[test]
    fn test_recency_basis_roundtrip() {
        for basis in [
            RecencyBasis::SourceEventTime,
            RecencyBasis::ObservedAt,
            RecencyBasis::AssertedAt,
        ] {
            let s = basis.to_string();
            let parsed = RecencyBasis::from_str(&s).unwrap();
            assert_eq!(basis, parsed);
        }
    }

    #[test]
    fn test_implicit_now_policy_serde() {
        let policy = ImplicitNowPolicy::TreatAsObservedAt;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, "\"treat_as_observed_at\"");

        let parsed: ImplicitNowPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, policy);
    }
}
