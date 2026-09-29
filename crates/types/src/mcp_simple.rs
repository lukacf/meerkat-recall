//! Simplified MCP tool input shapes.

use serde::{Deserialize, Serialize};

use crate::records::DocMediaInput;

/// Optional extraction guidance for simple ingest tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExtractionGuidanceInput {
    /// Optional extraction skill identifier (for example `extraction/example-observations`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_skill: Option<String>,
    /// Optional extraction purpose hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_purpose: Option<String>,
    /// Optional extraction domain hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_domain: Option<String>,
}

impl ExtractionGuidanceInput {
    /// Writes extraction guidance into a metadata object.
    pub fn apply_to_metadata(&self, metadata: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(skill) = self
            .extraction_skill
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                "extraction_skill".to_string(),
                serde_json::Value::String(skill.to_string()),
            );
        }
        if let Some(purpose) = self
            .extraction_purpose
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                "extraction_purpose".to_string(),
                serde_json::Value::String(purpose.to_string()),
            );
        }
        if let Some(domain) = self
            .extraction_domain
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            metadata.insert(
                "extraction_domain".to_string(),
                serde_json::Value::String(domain.to_string()),
            );
        }
    }
}

/// Convenience input for the `ingest_doc_simple` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestDocSimpleInput {
    /// Document title.
    pub title: String,
    /// Optional document text content or sidecar metadata for media docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Optional media attachments to ingest with the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<DocMediaInput>,
    /// Optional collection name for grouping documents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Optional metadata key-value pairs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional priority for the ingestion work item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

impl IngestDocSimpleInput {
    /// Returns true when the input includes meaningful text.
    #[must_use]
    pub fn has_text(&self) -> bool {
        self.text
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    }

    /// Returns true when the input includes at least one media attachment.
    #[must_use]
    pub const fn has_media(&self) -> bool {
        !self.media.is_empty()
    }
}

/// Convenience input for the `create_entity_simple` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateEntitySimpleInput {
    /// Entity type (e.g., "person", "organization", "topic").
    pub entity_type: String,
    /// Human-readable display name for the entity.
    pub display_name: String,
    /// Optional summary description of the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Convenience input for the `create_source_simple` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSourceSimpleInput {
    /// Human-readable name for the source.
    pub name: String,
    /// Optional URI for the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Optional source type (e.g., "email", "web", "manual").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

#[cfg(test)]
mod rct_contracts {
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

    #[test]
    fn test_ingest_doc_simple_input_roundtrip() {
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "source".to_string(),
            serde_json::Value::String("unit-test".to_string()),
        );

        let input = IngestDocSimpleInput {
            title: "Test Doc".to_string(),
            text: Some("Hello world".to_string()),
            media: Vec::new(),
            collection: Some("default".to_string()),
            metadata: Some(metadata),
            priority: None,
            extraction_guidance: ExtractionGuidanceInput::default(),
        };

        let encoded = serde_json::to_string(&input).unwrap();
        let decoded: IngestDocSimpleInput = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, input);
    }

    #[test]
    fn test_ingest_doc_simple_input_metadata_must_be_object() {
        let input = serde_json::json!({
            "title": "Doc",
            "text": "Hi",
            "metadata": "not-an-object"
        });

        let decoded: Result<IngestDocSimpleInput, _> = serde_json::from_value(input);
        assert!(decoded.is_err());
    }

    #[test]
    fn test_create_entity_simple_input_roundtrip() {
        let input = CreateEntitySimpleInput {
            entity_type: "person".to_string(),
            display_name: "Alex Chen".to_string(),
            summary: Some("Founder".to_string()),
        };

        let encoded = serde_json::to_string(&input).unwrap();
        let decoded: CreateEntitySimpleInput = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, input);
    }

    #[test]
    fn test_create_source_simple_input_roundtrip() {
        let input = CreateSourceSimpleInput {
            name: "My Feed".to_string(),
            uri: Some("https://example.com/feed".to_string()),
            source_type: Some("rss".to_string()),
            extraction_guidance: ExtractionGuidanceInput::default(),
        };

        let encoded = serde_json::to_string(&input).unwrap();
        let decoded: CreateSourceSimpleInput = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, input);
    }

    #[test]
    fn test_extraction_guidance_input_applies_to_metadata() {
        let guidance = ExtractionGuidanceInput {
            extraction_skill: Some("extraction/example-observations".to_string()),
            extraction_purpose: Some("extract observational evidence".to_string()),
            extraction_domain: Some("example_observation".to_string()),
        };

        let mut metadata = serde_json::Map::new();
        guidance.apply_to_metadata(&mut metadata);

        assert_eq!(
            metadata.get("extraction_skill"),
            Some(&serde_json::Value::String(
                "extraction/example-observations".to_string()
            ))
        );
        assert_eq!(
            metadata.get("extraction_purpose"),
            Some(&serde_json::Value::String(
                "extract observational evidence".to_string()
            ))
        );
        assert_eq!(
            metadata.get("extraction_domain"),
            Some(&serde_json::Value::String(
                "example_observation".to_string()
            ))
        );
    }

    #[test]
    fn test_ingest_doc_simple_input_supports_media_without_text() {
        let input = serde_json::json!({
            "title": "Frame",
            "media": [{
                "kind": "image",
                "media_type": "image/jpeg",
                "path": "/tmp/frame.jpg"
            }]
        });

        let decoded: IngestDocSimpleInput = serde_json::from_value(input).unwrap();
        assert_eq!(decoded.text, None);
        assert!(decoded.has_media());
        assert!(!decoded.has_text());
    }
}
