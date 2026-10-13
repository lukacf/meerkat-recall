//! Attribute type for entity-to-literal claims.
//!
//! Per spec §11.2, attributes represent resolved entity-to-literal claims.

use serde::{Deserialize, Serialize};

use crate::records::base::BaseFields;
use crate::records::entity::Entity;
use crate::Link;

use super::rel::{ClaimStatus, EvidenceRef, ValidityWindow};
use super::temporal::TemporalAudit;

/// Literal value types for attributes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Literal {
    /// String value.
    String(String),
    /// Integer value.
    Integer(i64),
    /// Floating point value.
    Float(f64),
    /// Boolean value.
    Boolean(bool),
    /// Date value (ISO 8601 date string).
    Date(String),
    /// `DateTime` value (ISO 8601 datetime string).
    DateTime(String),
    /// Duration value (ISO 8601 duration string).
    Duration(String),
    /// JSON object/array for complex values.
    Json(serde_json::Value),
}

impl Literal {
    /// Returns the type name of this literal.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::Integer(_) => "integer",
            Self::Float(_) => "float",
            Self::Boolean(_) => "boolean",
            Self::Date(_) => "date",
            Self::DateTime(_) => "datetime",
            Self::Duration(_) => "duration",
            Self::Json(_) => "json",
        }
    }
}

/// An entity-to-literal attribute in the knowledge graph.
///
/// Per spec §11.2: attributes represent resolved claims linking an entity to a literal value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Subject entity.
    pub subject: Link<Entity>,

    /// Canonical attribute type from the predicate registry.
    pub attr_type: String,

    /// The literal value.
    pub value: Literal,

    /// Current status of this attribute.
    #[serde(default)]
    pub status: ClaimStatus,

    /// Confidence score (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Validity window for temporal claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity_window: Option<ValidityWindow>,

    /// Evidence supporting this attribute.
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
        clippy::approx_constant,
        unused_imports,
        dead_code
    )]
    use super::*;

    #[test]
    fn test_literal_type_names() {
        assert_eq!(Literal::String("test".to_string()).type_name(), "string");
        assert_eq!(Literal::Integer(42).type_name(), "integer");
        assert_eq!(Literal::Float(3.14).type_name(), "float");
        assert_eq!(Literal::Boolean(true).type_name(), "boolean");
        assert_eq!(Literal::Date("2024-01-01".to_string()).type_name(), "date");
    }

    #[test]
    fn test_literal_serde_roundtrip() {
        let literals = vec![
            Literal::String("hello".to_string()),
            Literal::Integer(42),
            Literal::Float(3.14),
            Literal::Boolean(true),
            Literal::Date("2024-01-01".to_string()),
            Literal::DateTime("2024-01-01T12:00:00Z".to_string()),
            Literal::Duration("P1Y2M3D".to_string()),
            Literal::Json(serde_json::json!({"key": "value"})),
        ];

        for literal in literals {
            let json = serde_json::to_string(&literal).unwrap();
            let parsed: Literal = serde_json::from_str(&json).unwrap();
            assert_eq!(literal, parsed);
        }
    }
}
