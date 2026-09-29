//! Document record types.
//!
//! Documents store canonical text content in Elephant for embedding and
//! retrieval. Media-backed documents keep their searchable projection in
//! `text` while preserving typed attachment metadata alongside the document.

use serde::{Deserialize, Serialize};

use crate::Link;

use super::base::BaseFields;
use super::doc_node::DocNode;
use super::source::Source;

/// Typed kind of media attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocMediaKind {
    /// Raster image content.
    Image,
    /// Video content.
    Video,
}

/// External or managed location of a media attachment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum DocMediaLocation {
    /// Media stored in an Elephant-managed asset record.
    ManagedAsset {
        /// Managed `media_asset:*` identifier.
        asset_id: String,
    },
    /// Media referenced by a local filesystem path.
    Path {
        /// Absolute or repo-local filesystem path.
        path: String,
    },
    /// Media referenced by a URI.
    Uri {
        /// Absolute URI for externally hosted media.
        uri: String,
    },
}

/// Public ingest input describing an image or video payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocMediaInput {
    /// Media kind.
    pub kind: DocMediaKind,
    /// MIME type.
    pub media_type: String,
    /// Inline base64 payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_base64: Option<String>,
    /// External filesystem path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// External URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Duration in milliseconds for inline video payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

impl DocMediaInput {
    /// Returns the number of populated transport fields.
    #[must_use]
    pub fn transport_count(&self) -> usize {
        usize::from(self.inline_base64.is_some())
            + usize::from(self.path.is_some())
            + usize::from(self.uri.is_some())
    }
}

/// Persisted media attachment metadata on a document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocMediaAttachment {
    /// Stable attachment identifier within the document.
    pub attachment_id: String,
    /// Media kind.
    pub kind: DocMediaKind,
    /// MIME type.
    pub media_type: String,
    /// Where the attachment lives.
    pub location: DocMediaLocation,
    /// Optional original filename or label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Optional byte length when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Optional duration for video attachments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Optional SHA-256 fingerprint when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// A document in Elephant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Doc {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Document title.
    pub title: String,

    /// Canonical searchable text content.
    pub text: String,

    /// Caller-supplied sidecar text preserved separately from rendered text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_text: Option<String>,

    /// Source of the document.
    #[serde(default)]
    pub source: Option<Link<Source>>,

    /// Media attachments associated with this document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_attachments: Vec<DocMediaAttachment>,

    /// Rendition key for idempotency.
    pub rendition_key: String,

    /// Flexible metadata.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// An embedding vector for a document node (chunk, paragraph, or summary).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeEmbedding {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Reference to the doc node.
    pub node: Link<DocNode>,

    /// Embedding model.
    pub model: String,

    /// Vector dimensions.
    pub dims: i32,

    /// The embedding vector.
    pub vector: Vec<f32>,
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
    fn test_doc_creation() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "space_1".to_string(),
            "test".to_string(),
        );

        let doc = Doc {
            base,
            title: "Test Document".to_string(),
            text: "This is test content.".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "doc_123".to_string(),
            metadata: serde_json::Value::Null,
        };

        assert_eq!(doc.title, "Test Document");
    }

    #[test]
    fn test_media_input_counts_one_transport() {
        let input = DocMediaInput {
            kind: DocMediaKind::Image,
            media_type: "image/jpeg".to_string(),
            inline_base64: Some("abc".to_string()),
            path: None,
            uri: None,
            duration_ms: None,
        };

        assert_eq!(input.transport_count(), 1);
    }

    #[test]
    fn test_doc_roundtrip_with_media_attachment() {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "space_1".to_string(),
            "test".to_string(),
        );

        let doc = Doc {
            base,
            title: "Media Document".to_string(),
            text: "Rendered projection".to_string(),
            sidecar_text: Some("User supplied note".to_string()),
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Video,
                media_type: "video/mp4".to_string(),
                location: DocMediaLocation::ManagedAsset {
                    asset_id: RecordId::new_v4(TableName::MediaAsset).to_string(),
                },
                label: Some("segment.mp4".to_string()),
                size_bytes: Some(1024),
                duration_ms: Some(12_345),
                sha256: Some("deadbeef".to_string()),
            }],
            rendition_key: "doc_media_123".to_string(),
            metadata: serde_json::json!({"collection":"media"}),
        };

        let encoded = serde_json::to_string(&doc).expect("encode doc");
        let decoded: Doc = serde_json::from_str(&encoded).expect("decode doc");

        assert_eq!(decoded, doc);
    }
}
