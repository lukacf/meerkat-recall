//! Entity record type.
//!
//! Entities are the core nodes in the Elephant knowledge graph.
//! Entity types are freeform strings — any domain-appropriate type is valid.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::{Link, RecordId};

use super::base::BaseFields;

/// Freeform entity type. Any lowercase string is valid (e.g. "person", "character").
/// Extraction skills define which types are appropriate for each domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityType(String);

impl EntityType {
    /// Creates a new entity type, normalizing to lowercase.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into().to_lowercase())
    }

    /// Returns the type as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for EntityType {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for EntityType {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Status of an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityStatus {
    /// Entity is active.
    #[default]
    Active,
    /// Entity has been merged into another.
    Merged,
}

impl std::fmt::Display for EntityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Merged => write!(f, "merged"),
        }
    }
}

impl<'de> Deserialize<'de> for EntityStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct EntityStatusVisitor;

        impl<'de> Visitor<'de> for EntityStatusVisitor {
            type Value = EntityStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for EntityStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<EntityStatus, E>
            where
                E: de::Error,
            {
                EntityStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                // Try newtype first
                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return EntityStatus::from_str(&s).map_err(de::Error::custom);
                }

                EntityStatus::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(EntityStatusVisitor)
    }
}

impl FromStr for EntityStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "merged" => Ok(Self::Merged),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "entity_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// A handle (username/identifier) on a platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Handle {
    /// Platform name (e.g., "twitter", "instagram").
    pub platform: String,
    /// Username on the platform.
    pub username: String,
    /// Confidence score (0.0 to 1.0).
    #[serde(default)]
    pub confidence: Option<f64>,
    /// Whether the handle has been verified.
    #[serde(default)]
    pub verified: bool,
    /// When the handle was first seen.
    #[serde(default)]
    pub first_seen: Option<DateTime<Utc>>,
    /// When the handle was last seen.
    #[serde(default)]
    pub last_seen: Option<DateTime<Utc>>,
    /// Evidence for the handle association.
    #[serde(default)]
    pub evidence: Vec<RecordId>,
}

/// An entity in the Elephant knowledge graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Type of entity.
    #[serde(rename = "type")]
    pub entity_type: EntityType,

    /// Display name.
    pub display_name: String,

    /// Alternative names/aliases.
    #[serde(default)]
    pub aliases: Vec<String>,

    /// Brief summary of the entity.
    #[serde(default)]
    pub summary: Option<String>,

    /// Handles on various platforms.
    #[serde(default)]
    pub handles: Vec<Handle>,

    /// Stable identity keys (convenience cache).
    #[serde(default)]
    pub identity_keys: Vec<String>,

    /// Risk level score.
    #[serde(default)]
    pub risk_level: Option<f64>,

    /// Trust level score.
    #[serde(default)]
    pub trust_level: Option<f64>,

    /// Whether marked as known safe by user.
    #[serde(default)]
    pub known_safe: bool,

    /// When the entity was first observed.
    #[serde(default)]
    pub first_seen: Option<DateTime<Utc>>,

    /// When the entity was last observed.
    #[serde(default)]
    pub last_seen: Option<DateTime<Utc>>,

    /// Entity status.
    #[serde(default)]
    pub status: EntityStatus,

    /// For merged entities, link to the survivor.
    #[serde(default)]
    pub redirect_to: Option<Link<Self>>,

    /// Flexible metadata.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

impl Entity {
    /// Creates a new entity with minimal required fields.
    #[must_use]
    pub fn new(base: BaseFields, entity_type: EntityType, display_name: String) -> Self {
        Self {
            base,
            entity_type,
            display_name,
            aliases: Vec::new(),
            summary: None,
            handles: Vec::new(),
            identity_keys: Vec::new(),
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Returns true if the entity has been merged.
    #[must_use]
    pub fn is_merged(&self) -> bool {
        self.status == EntityStatus::Merged
    }

    /// Adds an alias if not already present.
    pub fn add_alias(&mut self, alias: String) {
        if !self.aliases.contains(&alias) && alias != self.display_name {
            self.aliases.push(alias);
        }
    }

    /// Adds a handle.
    pub fn add_handle(&mut self, handle: Handle) {
        // Check for duplicate platform/username
        let exists = self
            .handles
            .iter()
            .any(|h| h.platform == handle.platform && h.username == handle.username);
        if !exists {
            self.handles.push(handle);
        }
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
    fn test_entity_type_freeform() {
        let t = EntityType::new("person");
        assert_eq!(t.as_str(), "person");

        // Normalizes to lowercase
        let t2 = EntityType::new("Person");
        assert_eq!(t2.as_str(), "person");
        assert_eq!(t, t2);

        // Any string is valid
        let custom = EntityType::new("magical_creature");
        assert_eq!(custom.as_str(), "magical_creature");
    }

    #[test]
    fn test_entity_type_serialization() {
        let t = EntityType::new("character");
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, "\"character\"");

        let parsed: EntityType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, t);
    }

    #[test]
    fn test_entity_creation() {
        let id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let entity = Entity::new(base, EntityType::new("person"), "John Doe".to_string());

        assert_eq!(entity.display_name, "John Doe");
        assert_eq!(entity.entity_type, EntityType::new("person"));
        assert!(!entity.is_merged());
    }

    #[test]
    fn test_entity_add_alias() {
        let id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let mut entity = Entity::new(base, EntityType::new("person"), "John Doe".to_string());

        entity.add_alias("Johnny".to_string());
        assert_eq!(entity.aliases.len(), 1);

        entity.add_alias("Johnny".to_string());
        assert_eq!(entity.aliases.len(), 1);

        entity.add_alias("John Doe".to_string());
        assert_eq!(entity.aliases.len(), 1);
    }

    #[test]
    fn test_entity_serialization() {
        let id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let entity = Entity::new(base, EntityType::new("person"), "John Doe".to_string());

        let json = serde_json::to_string(&entity).ok();
        assert!(json.is_some());
        let parsed = json
            .as_ref()
            .and_then(|payload| serde_json::from_str::<Entity>(payload).ok());
        assert!(parsed.is_some());

        if let Some(parsed) = parsed {
            assert_eq!(entity.display_name, parsed.display_name);
            assert_eq!(entity.entity_type, parsed.entity_type);
            assert_eq!(entity, parsed);
        }
    }
}
