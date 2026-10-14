//! Space-level configuration records.

use serde::{Deserialize, Serialize};

use super::base::BaseFields;

/// Per-space configuration for embedding policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpaceConfig {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Embedding provider identifier (e.g. "fastembed", "openai", "fake").
    pub embedding_provider: String,

    /// Embedding model name.
    pub embedding_model: String,

    /// Embedding vector dimensions.
    pub embedding_dims: i32,

    /// Explicit processor approvals. Absence never authorizes a model call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing: Option<crate::provider_binding::SpaceProcessingConfig>,

    /// Optional purpose describing what kind of knowledge this space stores.
    /// Injected into extraction prompts to guide the LLM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,

    /// Optional skill ID for extraction (e.g. "extraction/fiction").
    /// References a Meerkat skill loaded via `FilesystemSkillSource`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill: Option<String>,
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
    use crate::RecordId;

    #[test]
    fn test_space_config() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_1".to_string(),
            "system".to_string(),
        );

        let cfg = SpaceConfig {
            processing: None,
            base,
            embedding_provider: "fastembed".to_string(),
            embedding_model: "bge-small-en-v1.5".to_string(),
            embedding_dims: 384,
            purpose: None,
            skill: None,
        };

        assert_eq!(cfg.embedding_provider, "fastembed");
        assert_eq!(cfg.embedding_dims, 384);
        assert_eq!(cfg.purpose, None);
        assert_eq!(cfg.skill, None);
    }

    #[test]
    fn test_space_config_with_purpose_roundtrip() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "project_space".to_string(),
            "system".to_string(),
        );

        let cfg = SpaceConfig {
            processing: None,
            base,
            embedding_provider: "fastembed".to_string(),
            embedding_model: "bge-small-en-v1.5".to_string(),
            embedding_dims: 384,
            purpose: Some("Track project relationships".to_string()),
            skill: None,
        };

        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: SpaceConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.purpose,
            Some("Track project relationships".to_string())
        );
    }

    #[test]
    fn test_space_config_without_purpose_deserializes() {
        // Ensure backwards compatibility: JSON without purpose field still deserializes
        let base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space_1".to_string(),
            "system".to_string(),
        );

        let cfg = SpaceConfig {
            processing: None,
            base,
            embedding_provider: "fastembed".to_string(),
            embedding_model: "bge-small-en-v1.5".to_string(),
            embedding_dims: 384,
            purpose: None,
            skill: None,
        };

        // Serialize (purpose=None should be skipped)
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(!json.contains("purpose"));

        // Deserialize back
        let deserialized: SpaceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.purpose, None);
    }
}
