//! Base fields shared by all Elephant records.
//!
//! Per the spec, all core tables include these common fields:
//! - id, `space_id`, `created_at`, `updated_at`, `created_by`, `deleted_at`
//! - provenance, security, subjects, `subjects_state`

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::{Link, RecordId};

use super::entity::Entity;

/// State of subject population on a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectsState {
    /// All relevant subjects are known.
    Complete,
    /// Some subjects are unknown.
    Partial,
    /// Subject inference is not possible.
    #[default]
    Unknown,
}

impl std::fmt::Display for SubjectsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Complete => write!(f, "complete"),
            Self::Partial => write!(f, "partial"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

impl<'de> Deserialize<'de> for SubjectsState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct SubjectsStateVisitor;

        impl<'de> Visitor<'de> for SubjectsStateVisitor {
            type Value = SubjectsState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for SubjectsState")
            }

            fn visit_str<E>(self, value: &str) -> Result<SubjectsState, E>
            where
                E: de::Error,
            {
                SubjectsState::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                // Try newtype first
                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return SubjectsState::from_str(&s).map_err(de::Error::custom);
                }

                SubjectsState::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(SubjectsStateVisitor)
    }
}

impl FromStr for SubjectsState {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "complete" => Ok(Self::Complete),
            "partial" => Ok(Self::Partial),
            "unknown" => Ok(Self::Unknown),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "subjects_state".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// Security labels for access control.
///
/// All records carry security labels evaluated under a policy schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Security {
    /// Identifies the policy schema for the record.
    pub policy_id: String,
    /// Ordered integer sensitivity level (0..n).
    pub level: i32,
    /// Compartments/tags (set membership).
    #[serde(default)]
    pub labels: Vec<String>,
    /// Caveats (e.g., NOFORN, RELTO).
    #[serde(default)]
    pub handling: Vec<String>,
}

/// Extractor metadata within provenance.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Extractor {
    /// Name of the extractor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Version of the extractor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Model used by the extractor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// A step in the provenance chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceStep {
    /// The step type (e.g., "extraction", "promotion").
    pub step: String,
    /// Input record ID.
    pub input: String,
    /// Output record ID.
    pub output: String,
}

/// Provenance information for a record.
///
/// Every derived fact must point to its sources and extraction chain.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Provenance {
    /// Array of source record links.
    #[serde(default)]
    pub sources: Vec<RecordId>,
    /// Extractor information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor: Option<Extractor>,
    /// Work run identifier that produced this record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_run_id: Option<RecordId>,
    /// Run/job identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Prompt identifier (template/version) used for this record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_id: Option<String>,
    /// Config identifier or hash used for this record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_id: Option<String>,
    /// Chain of processing steps.
    #[serde(default)]
    pub chain: Vec<ProvenanceStep>,
}

/// Common base fields for all Elephant records.
///
/// These fields are present on every core table and must be included
/// in all API requests and responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseFields {
    /// Unique record identifier.
    pub id: RecordId,
    /// Tenant/workspace scope.
    pub space_id: String,
    /// When the record was created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
    /// Agent/service that created the record.
    pub created_by: String,
    /// Soft delete timestamp (None if not deleted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,
    /// Provenance information.
    pub provenance: Provenance,
    /// Security labels for access control.
    pub security: Security,
    /// Entity links for subject scoping.
    #[serde(default)]
    pub subjects: Vec<Link<Entity>>,
    /// State of subject population.
    #[serde(default)]
    pub subjects_state: SubjectsState,
}

impl BaseFields {
    /// Creates new base fields with minimal required data.
    #[must_use]
    pub fn new(id: RecordId, space_id: String, created_by: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            space_id,
            created_at: now,
            updated_at: now,
            created_by,
            deleted_at: None,
            provenance: Provenance::default(),
            security: Security {
                policy_id: "default".to_string(),
                ..Security::default()
            },
            subjects: Vec::new(),
            subjects_state: SubjectsState::Unknown,
        }
    }

    /// Returns true if the record has been soft-deleted.
    #[must_use]
    pub const fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Marks the record as deleted.
    pub fn soft_delete(&mut self) {
        self.deleted_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Restores a soft-deleted record.
    pub fn restore(&mut self) {
        self.deleted_at = None;
        self.updated_at = Utc::now();
    }
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
    use crate::ids::TableName;

    #[test]
    fn test_subjects_state_parse() {
        assert!(matches!(
            SubjectsState::from_str("complete"),
            Ok(SubjectsState::Complete)
        ));
        assert!(matches!(
            SubjectsState::from_str("partial"),
            Ok(SubjectsState::Partial)
        ));
        assert!(matches!(
            SubjectsState::from_str("unknown"),
            Ok(SubjectsState::Unknown)
        ));
        assert!(SubjectsState::from_str("invalid").is_err());
    }

    #[test]
    fn test_base_fields_soft_delete() {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "space_1".to_string(), "test_agent".to_string());

        assert!(!base.is_deleted());
        base.soft_delete();
        assert!(base.is_deleted());
        base.restore();
        assert!(!base.is_deleted());
    }

    #[test]
    fn test_security_default() {
        let security = Security::default();
        assert!(security.policy_id.is_empty());
        assert_eq!(security.level, 0);
        assert!(security.labels.is_empty());
        assert!(security.handling.is_empty());
    }
}
