//! System record types.
//!
//! Includes system events (outbox), audit log, ID map, and rel evidence.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::{Link, RecordId};

use super::base::BaseFields;
use super::rel::Rel;
use super::source::Source;

/// A system event in the outbox.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemEvent {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Monotonic sequence number.
    pub seq: i64,

    /// Event type.
    pub event_type: String,

    /// Event payload (references only).
    pub payload: serde_json::Value,
}

/// Actions tracked in audit logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    /// Record created.
    Create,
    /// Record updated.
    Update,
    /// Record deleted.
    Delete,
    /// Record restored.
    Restore,
    /// Record purged (permanent delete).
    Purge,
    /// Fact promoted.
    Promote,
    /// Entities merged.
    Merge,
}

impl<'de> Deserialize<'de> for AuditAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct AuditActionVisitor;

        impl<'de> Visitor<'de> for AuditActionVisitor {
            type Value = AuditAction;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for AuditAction")
            }

            fn visit_str<E>(self, value: &str) -> Result<AuditAction, E>
            where
                E: de::Error,
            {
                AuditAction::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return AuditAction::from_str(&s).map_err(de::Error::custom);
                }

                AuditAction::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(AuditActionVisitor)
    }
}

impl FromStr for AuditAction {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "restore" => Ok(Self::Restore),
            "purge" => Ok(Self::Purge),
            "promote" => Ok(Self::Promote),
            "merge" => Ok(Self::Merge),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "audit_action".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// An audit log entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditLog {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Action performed.
    pub action: AuditAction,

    /// Actor who performed the action.
    pub actor: String,

    /// Record that was affected.
    pub record: RecordId,

    /// State before the action.
    #[serde(default)]
    pub before: Option<serde_json::Value>,

    /// State after the action.
    #[serde(default)]
    pub after: Option<serde_json::Value>,

    /// When the action occurred.
    pub timestamp: DateTime<Utc>,

    /// Reason for the action (e.g., run ID).
    #[serde(default)]
    pub reason: Option<String>,
}

/// Legacy ID mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdMap {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Legacy ID string.
    pub legacy_id: String,

    /// Current record.
    pub record: RecordId,

    /// Source system.
    pub source_system: String,
}

/// Evidence linking a relationship to its source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelEvidence {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The relationship.
    pub rel: Link<Rel>,

    /// The supporting fact (stored as a bare record ID reference).
    pub fact: Link<()>,

    /// The source.
    pub source: Link<Source>,

    /// Confidence score.
    pub confidence: f64,

    /// When observed.
    pub observed_at: DateTime<Utc>,

    /// Run ID.
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
    use crate::ids::TableName;

    #[test]
    fn test_audit_action() {
        assert!(matches!(
            AuditAction::from_str("create"),
            Ok(AuditAction::Create)
        ));
        assert!(matches!(
            AuditAction::from_str("merge"),
            Ok(AuditAction::Merge)
        ));
        assert!(AuditAction::from_str("invalid").is_err());
    }

    #[test]
    fn test_system_event() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "space_1".to_string(),
            "system".to_string(),
        );

        let event = SystemEvent {
            base,
            seq: 42,
            event_type: "entity.created".to_string(),
            payload: serde_json::json!({"records": []}),
        };

        assert_eq!(event.seq, 42);
    }

    #[test]
    fn test_audit_log() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::AuditLog),
            "space_1".to_string(),
            "system".to_string(),
        );

        let log = AuditLog {
            base,
            action: AuditAction::Create,
            actor: "test_user".to_string(),
            record: RecordId::new_v4(TableName::Entity),
            before: None,
            after: Some(serde_json::json!({"name": "Test"})),
            timestamp: Utc::now(),
            reason: None,
        };

        assert_eq!(log.action, AuditAction::Create);
    }
}
