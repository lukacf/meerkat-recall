//! Predicate governance types for vocabulary management.
//!
//! Per spec §10, these types manage the predicate registry and alias mappings.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;

use super::temporal::{ImplicitNowPolicy, RecencyBasis, RefinementPolicy, TemporalPersonality};
use super::truth::ConflictPolicy;

/// Status of a predicate alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasStatus {
    /// Proposed but not yet active.
    #[default]
    Proposed,
    /// Active and used for canonicalization.
    Active,
    /// Deprecated (retained for history).
    Deprecated,
}

impl std::fmt::Display for AliasStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Proposed => write!(f, "proposed"),
            Self::Active => write!(f, "active"),
            Self::Deprecated => write!(f, "deprecated"),
        }
    }
}

impl FromStr for AliasStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "proposed" => Ok(Self::Proposed),
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "alias_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for AliasStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct AliasStatusVisitor;

        impl Visitor<'_> for AliasStatusVisitor {
            type Value = AliasStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for AliasStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<AliasStatus, E>
            where
                E: de::Error,
            {
                AliasStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(AliasStatusVisitor)
    }
}

/// Type of an alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasType {
    /// Raw string from extraction.
    #[default]
    RawString,
    /// Plugin-generated code.
    PluginCode,
    /// Schema field name.
    SchemaField,
}

impl std::fmt::Display for AliasType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RawString => write!(f, "raw_string"),
            Self::PluginCode => write!(f, "plugin_code"),
            Self::SchemaField => write!(f, "schema_field"),
        }
    }
}

impl FromStr for AliasType {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "raw_string" => Ok(Self::RawString),
            "plugin_code" => Ok(Self::PluginCode),
            "schema_field" => Ok(Self::SchemaField),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "alias_type".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for AliasType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct AliasTypeVisitor;

        impl Visitor<'_> for AliasTypeVisitor {
            type Value = AliasType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for AliasType")
            }

            fn visit_str<E>(self, value: &str) -> Result<AliasType, E>
            where
                E: de::Error,
            {
                AliasType::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(AliasTypeVisitor)
    }
}

/// Scope of an alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasScope {
    /// Global scope (applies everywhere).
    #[default]
    Global,
    /// Plugin-specific scope.
    PluginOnly,
    /// Source type scope.
    SourceType,
}

impl std::fmt::Display for AliasScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Global => write!(f, "global"),
            Self::PluginOnly => write!(f, "plugin_only"),
            Self::SourceType => write!(f, "source_type"),
        }
    }
}

impl FromStr for AliasScope {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "global" => Ok(Self::Global),
            "plugin_only" => Ok(Self::PluginOnly),
            "source_type" => Ok(Self::SourceType),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "alias_scope".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for AliasScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct AliasScopeVisitor;

        impl Visitor<'_> for AliasScopeVisitor {
            type Value = AliasScope;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for AliasScope")
            }

            fn visit_str<E>(self, value: &str) -> Result<AliasScope, E>
            where
                E: de::Error,
            {
                AliasScope::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(AliasScopeVisitor)
    }
}

/// Status of predicate semantics configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticsStatus {
    /// Explicitly configured by operator.
    Configured,
    /// Using system defaults.
    #[default]
    Default,
    /// Needs operator review.
    NeedsReview,
    /// Proposed by LLM adjudication (revisable, never overwrites Configured).
    LlmProposed,
}

impl std::fmt::Display for SemanticsStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configured => write!(f, "configured"),
            Self::Default => write!(f, "default"),
            Self::NeedsReview => write!(f, "needs_review"),
            Self::LlmProposed => write!(f, "llm_proposed"),
        }
    }
}

impl FromStr for SemanticsStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "configured" => Ok(Self::Configured),
            "default" => Ok(Self::Default),
            "needs_review" => Ok(Self::NeedsReview),
            "llm_proposed" => Ok(Self::LlmProposed),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "semantics_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for SemanticsStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct SemanticsStatusVisitor;

        impl Visitor<'_> for SemanticsStatusVisitor {
            type Value = SemanticsStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for SemanticsStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<SemanticsStatus, E>
            where
                E: de::Error,
            {
                SemanticsStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(SemanticsStatusVisitor)
    }
}

/// Cardinality constraint for a predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// Single value allowed.
    One,
    /// Multiple values allowed.
    #[default]
    Many,
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::One => write!(f, "one"),
            Self::Many => write!(f, "many"),
        }
    }
}

impl FromStr for Cardinality {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "one" => Ok(Self::One),
            "many" => Ok(Self::Many),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "cardinality".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for Cardinality {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct CardinalityVisitor;

        impl Visitor<'_> for CardinalityVisitor {
            type Value = Cardinality;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for Cardinality")
            }

            fn visit_str<E>(self, value: &str) -> Result<Cardinality, E>
            where
                E: de::Error,
            {
                Cardinality::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(CardinalityVisitor)
    }
}

/// Policy for handling partial resolution during synthesis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialResolutionPolicy {
    /// Defer synthesis until full resolution.
    #[default]
    Defer,
    /// Synthesize attribute from unresolved object text.
    AttributeFromObjectText,
}

impl std::fmt::Display for PartialResolutionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Defer => write!(f, "defer"),
            Self::AttributeFromObjectText => write!(f, "attribute_from_object_text"),
        }
    }
}

impl FromStr for PartialResolutionPolicy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "defer" => Ok(Self::Defer),
            "attribute_from_object_text" => Ok(Self::AttributeFromObjectText),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "partial_resolution_policy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for PartialResolutionPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct PartialResolutionPolicyVisitor;

        impl Visitor<'_> for PartialResolutionPolicyVisitor {
            type Value = PartialResolutionPolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for PartialResolutionPolicy")
            }

            fn visit_str<E>(self, value: &str) -> Result<PartialResolutionPolicy, E>
            where
                E: de::Error,
            {
                PartialResolutionPolicy::from_str(value)
                    .map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(PartialResolutionPolicyVisitor)
    }
}

/// Confidence thresholds for alias usage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AliasThresholds {
    /// Minimum confidence for using alias in truth maintenance.
    #[serde(default = "default_truth_min_confidence")]
    pub truth_min_confidence: f32,

    /// Minimum confidence for query expansion.
    #[serde(default = "default_query_expand_min_confidence")]
    pub query_expand_min_confidence: f32,
}

const fn default_truth_min_confidence() -> f32 {
    0.9
}

const fn default_query_expand_min_confidence() -> f32 {
    0.6
}

impl Default for AliasThresholds {
    fn default() -> Self {
        Self {
            truth_min_confidence: 0.9,
            query_expand_min_confidence: 0.6,
        }
    }
}

/// Semantic configuration for a predicate.
///
/// Per spec §12.1: determines how truth maintenance handles this predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PredicateSemantics {
    /// Temporal model for this predicate.
    #[serde(default)]
    pub temporal_model: TemporalPersonality,

    /// Cardinality constraint.
    #[serde(default)]
    pub cardinality: Cardinality,

    /// Conflict resolution policy.
    #[serde(default)]
    pub conflict_policy: ConflictPolicy,

    /// Which timestamp to use for recency.
    #[serde(default)]
    pub recency_basis: RecencyBasis,

    /// Fallback order for missing timestamps.
    #[serde(default)]
    pub fallback_order: Vec<RecencyBasis>,

    /// Policy for handling implicit "now".
    #[serde(default)]
    pub implicit_now_policy: ImplicitNowPolicy,

    /// Policy for refining implicit timestamps.
    #[serde(default)]
    pub refinement_policy: RefinementPolicy,

    /// Minimum evidence count for promotion.
    #[serde(default = "default_min_evidence")]
    pub min_evidence: u32,

    /// Minimum confidence for promotion (truth slot winner selection).
    ///
    /// This is distinct from `alias_thresholds.truth_min_confidence`, which gates
    /// predicate alias canonicalization for truth maintenance.
    #[serde(default = "default_promotion_threshold")]
    pub promotion_threshold: f32,

    /// Source priority ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_priority: Option<Vec<String>>,

    /// Alias confidence thresholds.
    #[serde(default)]
    pub alias_thresholds: AliasThresholds,

    /// Policy for handling partial resolution during synthesis.
    #[serde(default)]
    pub partial_resolution_policy: PartialResolutionPolicy,
}

const fn default_min_evidence() -> u32 {
    1
}

const fn default_promotion_threshold() -> f32 {
    0.7
}

impl Default for PredicateSemantics {
    fn default() -> Self {
        Self {
            temporal_model: TemporalPersonality::default(),
            cardinality: Cardinality::default(),
            conflict_policy: ConflictPolicy::default(),
            recency_basis: RecencyBasis::default(),
            fallback_order: vec![
                RecencyBasis::SourceEventTime,
                RecencyBasis::ObservedAt,
                RecencyBasis::AssertedAt,
            ],
            implicit_now_policy: ImplicitNowPolicy::default(),
            refinement_policy: RefinementPolicy::default(),
            min_evidence: 1,
            promotion_threshold: default_promotion_threshold(),
            source_priority: None,
            alias_thresholds: AliasThresholds::default(),
            partial_resolution_policy: PartialResolutionPolicy::default(),
        }
    }
}

/// A canonical predicate in the registry.
///
/// Per spec §10.2: stores canonical predicate IDs and semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PredicateRegistry {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Unique predicate identifier.
    pub predicate_id: String,

    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Current status (active/deprecated).
    #[serde(default = "default_predicate_status")]
    pub status: String,

    /// Semantic configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantics: Option<PredicateSemantics>,

    /// Status of semantics configuration.
    #[serde(default)]
    pub semantics_status: SemanticsStatus,

    /// When this predicate was first used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Total usage count.
    #[serde(default)]
    pub usage_count: u64,

    /// When last used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn default_predicate_status() -> String {
    "active".to_string()
}

/// An alias mapping for a predicate.
///
/// Per spec §10.2a: stores alias mappings with confidence and scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PredicateAlias {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The alias string.
    pub alias: String,

    /// Type of alias.
    #[serde(default)]
    pub alias_type: AliasType,

    /// Source plugin (if plugin-generated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_plugin: Option<String>,

    /// Confidence score (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Current status.
    #[serde(default)]
    pub status: AliasStatus,

    /// Scope of this alias.
    #[serde(default)]
    pub scope: AliasScope,

    /// Target canonical predicate.
    pub predicate_id: String,

    /// Version of the confidence score.
    #[serde(default)]
    pub confidence_version: u32,

    /// Usage count for this alias.
    #[serde(default)]
    pub usage_count: u64,
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
    fn test_alias_status_roundtrip() {
        for status in [
            AliasStatus::Proposed,
            AliasStatus::Active,
            AliasStatus::Deprecated,
        ] {
            let s = status.to_string();
            let parsed = AliasStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_alias_type_roundtrip() {
        for alias_type in [
            AliasType::RawString,
            AliasType::PluginCode,
            AliasType::SchemaField,
        ] {
            let s = alias_type.to_string();
            let parsed = AliasType::from_str(&s).unwrap();
            assert_eq!(alias_type, parsed);
        }
    }

    #[test]
    fn test_semantics_status_roundtrip() {
        for status in [
            SemanticsStatus::Configured,
            SemanticsStatus::Default,
            SemanticsStatus::NeedsReview,
            SemanticsStatus::LlmProposed,
        ] {
            let s = status.to_string();
            let parsed = SemanticsStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_semantics_status_serde() {
        let status = SemanticsStatus::LlmProposed;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"llm_proposed\"");

        let parsed: SemanticsStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_alias_thresholds_default() {
        let thresholds = AliasThresholds::default();
        assert!((thresholds.truth_min_confidence - 0.9).abs() < f32::EPSILON);
        assert!((thresholds.query_expand_min_confidence - 0.6).abs() < f32::EPSILON);
    }

    #[test]
    fn test_predicate_semantics_default() {
        let semantics = PredicateSemantics::default();
        assert_eq!(semantics.temporal_model, TemporalPersonality::StateOverTime);
        assert_eq!(semantics.cardinality, Cardinality::Many);
        assert_eq!(semantics.conflict_policy, ConflictPolicy::AllowParallel);
        assert!((semantics.promotion_threshold - 0.7).abs() < f32::EPSILON);
        assert_eq!(
            semantics.partial_resolution_policy,
            PartialResolutionPolicy::Defer
        );
    }

    #[test]
    fn test_partial_resolution_policy_roundtrip() {
        for policy in [
            PartialResolutionPolicy::Defer,
            PartialResolutionPolicy::AttributeFromObjectText,
        ] {
            let s = policy.to_string();
            let parsed = PartialResolutionPolicy::from_str(&s).unwrap();
            assert_eq!(policy, parsed);
        }
    }
}
