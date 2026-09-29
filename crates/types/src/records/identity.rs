//! Identity-related record types.
//!
//! Includes entity identity keys, identity candidates, and entity merges.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::Link;

use super::base::BaseFields;
use super::entity::Entity;
use super::source::Source;

/// Status of an identity key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityKeyStatus {
    /// Key is active.
    #[default]
    Active,
    /// Key has been revoked.
    Revoked,
}

impl<'de> Deserialize<'de> for IdentityKeyStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct IdentityKeyStatusVisitor;

        impl<'de> Visitor<'de> for IdentityKeyStatusVisitor {
            type Value = IdentityKeyStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for IdentityKeyStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<IdentityKeyStatus, E>
            where
                E: de::Error,
            {
                IdentityKeyStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return IdentityKeyStatus::from_str(&s).map_err(de::Error::custom);
                }

                IdentityKeyStatus::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(IdentityKeyStatusVisitor)
    }
}

impl FromStr for IdentityKeyStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "identity_key_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// An entity identity key record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityIdentityKey {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The canonical identity key string.
    pub key: String,

    /// The entity this key belongs to.
    pub entity: Link<Entity>,

    /// Confidence score.
    pub confidence: f64,

    /// First seen datetime.
    #[serde(default)]
    pub first_seen: Option<DateTime<Utc>>,

    /// Last seen datetime.
    #[serde(default)]
    pub last_seen: Option<DateTime<Utc>>,

    /// Evidence sources.
    #[serde(default)]
    pub evidence: Vec<Link<Source>>,

    /// Key status.
    #[serde(default)]
    pub status: IdentityKeyStatus,
}

/// Status of an identity candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityCandidateStatus {
    /// Pending review.
    #[default]
    Pending,
    /// Approved for merge.
    Approved,
    /// Rejected.
    Rejected,
}

impl<'de> Deserialize<'de> for IdentityCandidateStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct IdentityCandidateStatusVisitor;

        impl<'de> Visitor<'de> for IdentityCandidateStatusVisitor {
            type Value = IdentityCandidateStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for IdentityCandidateStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<IdentityCandidateStatus, E>
            where
                E: de::Error,
            {
                IdentityCandidateStatus::from_str(value)
                    .map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return IdentityCandidateStatus::from_str(&s).map_err(de::Error::custom);
                }

                IdentityCandidateStatus::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(IdentityCandidateStatusVisitor)
    }
}

impl FromStr for IdentityCandidateStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "identity_candidate_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// An identity candidate for potential merge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentityCandidate {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// First entity.
    pub entity_a: Link<Entity>,

    /// Second entity.
    pub entity_b: Link<Entity>,

    /// Signals supporting the match.
    pub signals: serde_json::Value,

    /// Confidence score.
    pub confidence: f64,

    /// Candidate status.
    #[serde(default)]
    pub status: IdentityCandidateStatus,

    /// Who proposed this candidate.
    pub proposed_by: String,

    /// Who decided on this candidate.
    #[serde(default)]
    pub decided_by: Option<String>,

    /// When decision was made.
    #[serde(default)]
    pub decided_at: Option<DateTime<Utc>>,
}

/// Strategies for merging entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Keep the primary entity's data, combine handles/keys.
    #[default]
    KeepA,
    /// Keep the secondary entity's data, combine handles/keys.
    KeepB,
    /// Combine all data from both entities.
    Combine,
}

impl<'de> Deserialize<'de> for MergeStrategy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct MergeStrategyVisitor;

        impl<'de> Visitor<'de> for MergeStrategyVisitor {
            type Value = MergeStrategy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for MergeStrategy")
            }

            fn visit_str<E>(self, value: &str) -> Result<MergeStrategy, E>
            where
                E: de::Error,
            {
                MergeStrategy::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return MergeStrategy::from_str(&s).map_err(de::Error::custom);
                }

                MergeStrategy::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(MergeStrategyVisitor)
    }
}

impl FromStr for MergeStrategy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "keep_a" => Ok(Self::KeepA),
            "keep_b" => Ok(Self::KeepB),
            "combine" => Ok(Self::Combine),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "merge_strategy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// Entity merge lineage record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityMerge {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Entity that was merged from.
    pub from: Link<Entity>,

    /// Entity that was merged into.
    pub into: Link<Entity>,

    /// Merge strategy used.
    pub strategy: MergeStrategy,

    /// Who performed the merge.
    pub decided_by: String,

    /// When the merge was performed.
    pub decided_at: DateTime<Utc>,

    /// Run ID if part of a batch.
    #[serde(default)]
    pub run_id: Option<String>,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use crate::ids::{RecordId, TableName};

    #[test]
    fn test_identity_key_status() {
        assert!(matches!(
            IdentityKeyStatus::from_str("active"),
            Ok(IdentityKeyStatus::Active)
        ));
        assert!(IdentityKeyStatus::from_str("invalid").is_err());
    }

    #[test]
    fn test_identity_candidate_status() {
        assert!(matches!(
            IdentityCandidateStatus::from_str("pending"),
            Ok(IdentityCandidateStatus::Pending)
        ));
    }

    #[test]
    fn test_merge_strategy() {
        assert!(matches!(
            MergeStrategy::from_str("keep_a"),
            Ok(MergeStrategy::KeepA)
        ));
        assert!(matches!(
            MergeStrategy::from_str("combine"),
            Ok(MergeStrategy::Combine)
        ));
    }

    #[test]
    fn test_entity_identity_key() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::EntityIdentityKey),
            "space_1".to_string(),
            "test".to_string(),
        );

        let key = EntityIdentityKey {
            base,
            key: "email:test@example.com".to_string(),
            entity: Link::new(RecordId::new_v4(TableName::Entity)),
            confidence: 0.95,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        };

        assert_eq!(key.key, "email:test@example.com");
    }
}
