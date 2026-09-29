//! Identity key management.

use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

/// Status of an identity key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    /// Key is active.
    #[default]
    Active,
    /// Key has been revoked.
    Revoked,
}

/// An identity key record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityKey {
    /// The canonical identity key string.
    pub key: String,
    /// The entity this key belongs to.
    pub entity: RecordId,
    /// Confidence score.
    pub confidence: f64,
    /// Key status.
    #[serde(default)]
    pub status: KeyStatus,
}

impl IdentityKey {
    /// Creates a new identity key.
    #[must_use]
    pub const fn new(key: String, entity: RecordId, confidence: f64) -> Self {
        Self {
            key,
            entity,
            confidence,
            status: KeyStatus::Active,
        }
    }

    /// Revokes the key.
    pub const fn revoke(&mut self) {
        self.status = KeyStatus::Revoked;
    }

    /// Returns true if the key is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == KeyStatus::Active
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
    use elephant_types::ids::TableName;

    #[test]
    fn test_identity_key() {
        let entity_id = RecordId::new_v4(TableName::Entity);
        let mut key = IdentityKey::new("email:test@example.com".to_string(), entity_id, 0.95);

        assert!(key.is_active());
        key.revoke();
        assert!(!key.is_active());
    }
}
