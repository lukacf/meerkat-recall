//! Document revision type for immutable text snapshots.
//!
//! Per spec §7.1, `doc_revision` represents normalized text produced by a renderer plugin.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::records::doc::{Doc, DocMediaKind, DocMediaLocation};
use crate::Link;

/// Status of a document revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionStatus {
    /// Active revision (current).
    #[default]
    Active,
    /// Superseded by a newer revision.
    Superseded,
    /// Rendering failed.
    Failed,
}

impl std::fmt::Display for RevisionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Superseded => write!(f, "superseded"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl FromStr for RevisionStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "superseded" => Ok(Self::Superseded),
            "failed" => Ok(Self::Failed),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "revision_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for RevisionStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct RevisionStatusVisitor;

        impl Visitor<'_> for RevisionStatusVisitor {
            type Value = RevisionStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for RevisionStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<RevisionStatus, E>
            where
                E: de::Error,
            {
                RevisionStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(RevisionStatusVisitor)
    }
}

/// Renderer metadata for a document revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendererMetadata {
    /// Name of the renderer plugin.
    pub name: String,
    /// Version of the renderer plugin.
    pub version: String,
    /// Hash of the renderer configuration used.
    #[serde(default)]
    pub config_hash: Option<String>,
}

/// Attachment metadata captured during rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderManifestAttachment {
    /// Attachment identifier from the parent document.
    pub attachment_id: String,
    /// Media kind.
    pub kind: DocMediaKind,
    /// MIME type.
    pub media_type: String,
    /// Immutable attachment location snapshot used for downstream extraction.
    pub location: DocMediaLocation,
    /// Optional label.
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

/// Media time range aligned to rendered text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderManifestTimeRange {
    /// Start offset in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ms: Option<u64>,
    /// End offset in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<u64>,
}

/// Mapping from a text span back to a media attachment and optional time range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderManifestSpanBinding {
    /// Attachment identifier referenced by this span.
    pub attachment_id: String,
    /// Text start offset in bytes.
    pub text_start: u32,
    /// Text end offset in bytes.
    pub text_end: u32,
    /// Optional media time range for video-backed spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<RenderManifestTimeRange>,
}

/// Media-aware render manifest persisted with a revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenderManifest {
    /// Attachments that informed the render.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<RenderManifestAttachment>,
    /// Span bindings from rendered text back to media.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub span_bindings: Vec<RenderManifestSpanBinding>,
}

/// An immutable document revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocRevision {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Parent document this revision belongs to.
    pub doc: Link<Doc>,

    /// The normalized text content (ABAC-gated).
    pub text: String,

    /// Hash of the text content for integrity verification.
    pub text_hash: String,

    /// Renderer that produced this revision.
    pub renderer: RendererMetadata,

    /// Media-aware render manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<RenderManifest>,

    /// Link to the revision this one supersedes (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<Link<Self>>,

    /// Current status of this revision.
    #[serde(default)]
    pub status: RevisionStatus,

    /// When the revision was rendered.
    pub rendered_at: DateTime<Utc>,
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
    use crate::ids::{RecordId, TableName};

    #[test]
    fn test_revision_status_roundtrip() {
        for status in [
            RevisionStatus::Active,
            RevisionStatus::Superseded,
            RevisionStatus::Failed,
        ] {
            let s = status.to_string();
            let parsed = RevisionStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_revision_status_serde() {
        let status = RevisionStatus::Active;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"active\"");

        let parsed: RevisionStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_doc_revision_roundtrip_with_manifest() {
        let revision = DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "space_1".to_string(),
                "test".to_string(),
            ),
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: "00:00 Intro\n00:15 Demo".to_string(),
            text_hash: "abc123".to_string(),
            renderer: RendererMetadata {
                name: "multimodal".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: Some(RenderManifest {
                attachments: vec![RenderManifestAttachment {
                    attachment_id: "att-1".to_string(),
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    location: crate::records::doc::DocMediaLocation::ManagedAsset {
                        asset_id: "media_asset:test".to_string(),
                    },
                    label: Some("demo.mp4".to_string()),
                    size_bytes: Some(1024),
                    duration_ms: Some(60_000),
                    sha256: Some("abc123".to_string()),
                }],
                span_bindings: vec![RenderManifestSpanBinding {
                    attachment_id: "att-1".to_string(),
                    text_start: 0,
                    text_end: 9,
                    time_range: Some(RenderManifestTimeRange {
                        start_ms: Some(0),
                        end_ms: Some(15_000),
                    }),
                }],
            }),
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };

        let encoded = serde_json::to_string(&revision).expect("encode");
        let decoded: DocRevision = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, revision);
    }
}
