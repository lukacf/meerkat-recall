//! Freshness types for ranking and demotion.
//!
//! Per spec §13, freshness is derived metadata for ranking, never "truth."

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;

/// Decay model for freshness computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecayModel {
    /// Linear decay over time.
    #[default]
    Linear,
    /// Exponential decay (half-life based).
    Exponential,
    /// Step function with thresholds.
    StepFunction,
}

impl std::fmt::Display for DecayModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Linear => write!(f, "linear"),
            Self::Exponential => write!(f, "exponential"),
            Self::StepFunction => write!(f, "step_function"),
        }
    }
}

impl FromStr for DecayModel {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "linear" => Ok(Self::Linear),
            "exponential" => Ok(Self::Exponential),
            "step_function" => Ok(Self::StepFunction),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "decay_model".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for DecayModel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct DecayModelVisitor;

        impl Visitor<'_> for DecayModelVisitor {
            type Value = DecayModel;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for DecayModel")
            }

            fn visit_str<E>(self, value: &str) -> Result<DecayModel, E>
            where
                E: de::Error,
            {
                DecayModel::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(DecayModelVisitor)
    }
}

/// Factors contributing to freshness computation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FreshnessFactors {
    /// Age of the evidence in days.
    #[serde(default)]
    pub evidence_age_days: f64,

    /// Conflict severity (0.0 = no conflicts, 1.0 = severe).
    #[serde(default)]
    pub conflict_severity: f32,

    /// Evidence strength (0.0 to 1.0).
    #[serde(default)]
    pub evidence_strength: f32,

    /// Source reliability factor (0.0 to 1.0).
    #[serde(default)]
    pub source_reliability: f32,

    /// Number of corroborating sources.
    #[serde(default)]
    pub corroboration_count: u32,

    /// Custom factors from plugins.
    #[serde(default)]
    pub custom: std::collections::HashMap<String, f64>,
}

impl Default for FreshnessFactors {
    fn default() -> Self {
        Self {
            evidence_age_days: 0.0,
            conflict_severity: 0.0,
            evidence_strength: 1.0,
            source_reliability: 1.0,
            corroboration_count: 1,
            custom: std::collections::HashMap::new(),
        }
    }
}

/// Freshness score for a target (rel, attribute, entity, etc.).
///
/// Per spec §13: freshness is derived metadata for ranking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Freshness {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Target record ID (rel, attribute, entity, etc.).
    pub target: String,

    /// Target type ("rel", "attribute", "entity").
    pub target_type: String,

    /// Decay model used for computation.
    #[serde(default)]
    pub decay_model: DecayModel,

    /// Computed freshness score (0.0 to 1.0).
    #[serde(default)]
    pub score: f32,

    /// Confidence in the freshness score (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Rationale for the score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,

    /// Factors used in computation.
    #[serde(default)]
    pub factors: FreshnessFactors,

    /// When freshness was last computed.
    pub computed_at: DateTime<Utc>,

    /// When to next review this record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_review_at: Option<DateTime<Utc>>,

    /// Whether this record should be demoted.
    #[serde(default)]
    pub demote: bool,

    /// Demotion threshold that triggered demotion (if demoted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demotion_threshold: Option<f32>,
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
    fn test_decay_model_roundtrip() {
        for model in [
            DecayModel::Linear,
            DecayModel::Exponential,
            DecayModel::StepFunction,
        ] {
            let s = model.to_string();
            let parsed = DecayModel::from_str(&s).unwrap();
            assert_eq!(model, parsed);
        }
    }

    #[test]
    fn test_decay_model_serde() {
        let model = DecayModel::Exponential;
        let json = serde_json::to_string(&model).unwrap();
        assert_eq!(json, "\"exponential\"");

        let parsed: DecayModel = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, model);
    }

    #[test]
    fn test_freshness_factors_default() {
        let factors = FreshnessFactors::default();
        assert!((factors.evidence_age_days - 0.0).abs() < f64::EPSILON);
        assert!((factors.evidence_strength - 1.0).abs() < f32::EPSILON);
        assert_eq!(factors.corroboration_count, 1);
    }
}
