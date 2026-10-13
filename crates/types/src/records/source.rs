//! Source record type.
//!
//! Sources track the provenance of data in Elephant, representing
//! emails, calendar entries, documents, device frames, transcripts, etc.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use crate::error::{TypeError, TypeResult};
use crate::records::ingest::{IngestDecision, IngestMode};
use crate::validation::validate_display_name;
use crate::{Link, RecordId, TableName};

use super::base::{BaseFields, Provenance, SubjectsState};
use super::doc::{Doc, DocMediaAttachment};

/// Type of source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    /// Email message.
    Email,
    /// Calendar event.
    Calendar,
    /// Web page or URL.
    Web,
    /// Drive/cloud storage document.
    Drive,
    /// Chat/messaging.
    Chat,
    /// Device frame capture.
    DeviceFrame,
    /// Device video capture.
    DeviceVideo,
    /// Transcript of audio/video.
    Transcript,
    /// Manually entered.
    Manual,
}

impl<'de> Deserialize<'de> for SourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct SourceTypeVisitor;

        impl<'de> Visitor<'de> for SourceTypeVisitor {
            type Value = SourceType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for SourceType")
            }

            fn visit_str<E>(self, value: &str) -> Result<SourceType, E>
            where
                E: de::Error,
            {
                SourceType::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return SourceType::from_str(&s).map_err(de::Error::custom);
                }

                SourceType::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(SourceTypeVisitor)
    }
}

impl FromStr for SourceType {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "email" => Ok(Self::Email),
            "calendar" => Ok(Self::Calendar),
            "web" => Ok(Self::Web),
            "drive" => Ok(Self::Drive),
            "chat" => Ok(Self::Chat),
            "device_frame" => Ok(Self::DeviceFrame),
            "device_video" => Ok(Self::DeviceVideo),
            "transcript" => Ok(Self::Transcript),
            "manual" => Ok(Self::Manual),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "source_type".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

/// Stable lifecycle state for a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceLifecycleStatus {
    /// Source exists but has not yet been triaged.
    #[default]
    Discovered,
    /// Triage has produced an ingest decision.
    Triaged,
    /// Metadata-only processing has completed.
    MetadataReady,
    /// Retrieval/evidence materialization has completed.
    RetrievalReady,
    /// Graph/canonical knowledge materialization has completed.
    GraphReady,
    /// Source was intentionally skipped.
    Skipped,
    /// Source was rejected as invalid/unsupported.
    Rejected,
}

impl std::fmt::Display for SourceLifecycleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discovered => write!(f, "discovered"),
            Self::Triaged => write!(f, "triaged"),
            Self::MetadataReady => write!(f, "metadata_ready"),
            Self::RetrievalReady => write!(f, "retrieval_ready"),
            Self::GraphReady => write!(f, "graph_ready"),
            Self::Skipped => write!(f, "skipped"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

impl FromStr for SourceLifecycleStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "discovered" => Ok(Self::Discovered),
            "triaged" => Ok(Self::Triaged),
            "metadata_ready" => Ok(Self::MetadataReady),
            "retrieval_ready" => Ok(Self::RetrievalReady),
            "graph_ready" => Ok(Self::GraphReady),
            "skipped" => Ok(Self::Skipped),
            "rejected" => Ok(Self::Rejected),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "source_lifecycle_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for SourceLifecycleStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct SourceLifecycleStatusVisitor;

        impl<'de> Visitor<'de> for SourceLifecycleStatusVisitor {
            type Value = SourceLifecycleStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for SourceLifecycleStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<SourceLifecycleStatus, E>
            where
                E: de::Error,
            {
                SourceLifecycleStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return SourceLifecycleStatus::from_str(&s).map_err(de::Error::custom);
                }

                SourceLifecycleStatus::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(SourceLifecycleStatusVisitor)
    }
}

/// Range within a parent source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRange {
    /// Start time in milliseconds.
    #[serde(default)]
    pub start_ms: Option<i64>,
    /// End time in milliseconds.
    #[serde(default)]
    pub end_ms: Option<i64>,
    /// Position index.
    #[serde(default)]
    pub index: Option<i64>,
}

/// Staged manual document payload attached to a source-first ingest request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualDocTemplate {
    /// Preallocated document identifier.
    pub doc_id: String,
    /// Document title.
    pub title: String,
    /// Document body text.
    pub text: String,
    /// Caller-supplied sidecar text preserved before render.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_text: Option<String>,
    /// Typed media attachments staged for the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_attachments: Vec<DocMediaAttachment>,
    /// Rendition key used for render idempotency.
    pub rendition_key: String,
    /// Flexible document metadata.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

/// Metadata key storing the requested ingest mode for a source workflow.
pub const SOURCE_REQUESTED_INGEST_MODE_KEY: &str = "_requested_ingest_mode";
/// Metadata key storing the staged manual document template for source-first ingest.
pub const SOURCE_MANUAL_DOC_TEMPLATE_KEY: &str = "_manual_doc";

/// A source of knowledge in Elephant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Type of source.
    #[serde(rename = "type")]
    pub type_: SourceType,

    /// Stable lifecycle status for the source.
    #[serde(default)]
    pub lifecycle_status: SourceLifecycleStatus,

    /// Highest ingest mode that has been successfully achieved.
    #[serde(default)]
    pub current_ingest_mode: Option<IngestMode>,

    /// Most recent ingest decision for this source.
    #[serde(default)]
    pub latest_ingest_decision: Option<Link<IngestDecision>>,

    /// External identifier for the source.
    #[serde(default)]
    pub external_id: Option<String>,

    /// URI or path to the source.
    #[serde(default)]
    pub uri: Option<String>,

    /// File path (if applicable).
    #[serde(default)]
    pub path: Option<String>,

    /// Parent source (e.g., frame -> video).
    #[serde(default)]
    pub parent: Option<Link<Self>>,

    /// Range within parent.
    #[serde(default)]
    pub range: Option<SourceRange>,

    /// Timestamp of the source content.
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,

    /// Author of the content.
    #[serde(default)]
    pub author: Option<String>,

    /// Channel/platform.
    #[serde(default)]
    pub channel: Option<String>,

    /// Content hash for deduplication.
    pub hash: String,

    /// Flexible metadata.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

impl Source {
    /// Creates a new source with minimal required fields.
    #[must_use]
    pub fn new(base: BaseFields, source_type: SourceType, hash: String) -> Self {
        Self {
            base,
            type_: source_type,
            lifecycle_status: SourceLifecycleStatus::Discovered,
            current_ingest_mode: None,
            latest_ingest_decision: None,
            external_id: None,
            uri: None,
            path: None,
            parent: None,
            range: None,
            timestamp: None,
            author: None,
            channel: None,
            hash,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Returns the deduplication key for this source.
    #[must_use]
    pub fn dedup_key(&self) -> String {
        self.external_id.as_ref().map_or_else(
            || format!("{:?}:{}", self.type_, self.hash).to_lowercase(),
            |ext_id| format!("{:?}:{}:{}", self.type_, ext_id, self.hash).to_lowercase(),
        )
    }

    /// Returns the requested ingest mode stored in metadata, if any.
    #[must_use]
    pub fn requested_ingest_mode(&self) -> Option<IngestMode> {
        self.metadata
            .get(SOURCE_REQUESTED_INGEST_MODE_KEY)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| IngestMode::from_str(value).ok())
    }

    /// Stores the requested ingest mode in metadata.
    pub fn set_requested_ingest_mode(&mut self, mode: IngestMode) {
        let map = ensure_metadata_object(&mut self.metadata);
        map.insert(
            SOURCE_REQUESTED_INGEST_MODE_KEY.to_string(),
            serde_json::Value::String(mode.to_string()),
        );
    }

    /// Returns the staged manual document payload, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored metadata payload cannot be deserialized as
    /// a `ManualDocTemplate`.
    pub fn manual_doc_template(&self) -> TypeResult<Option<ManualDocTemplate>> {
        let Some(value) = self.metadata.get(SOURCE_MANUAL_DOC_TEMPLATE_KEY).cloned() else {
            return Ok(None);
        };
        serde_json::from_value(value).map(Some).map_err(|e| {
            TypeError::ValidationError(format!("invalid source manual_doc template: {e}"))
        })
    }

    /// Stores the staged manual document payload in metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the template cannot be serialized into metadata.
    pub fn set_manual_doc_template(&mut self, doc: &ManualDocTemplate) -> TypeResult<()> {
        let value = serde_json::to_value(doc).map_err(|e| {
            TypeError::ValidationError(format!("failed to serialize manual_doc template: {e}"))
        })?;
        let map = ensure_metadata_object(&mut self.metadata);
        map.insert(SOURCE_MANUAL_DOC_TEMPLATE_KEY.to_string(), value);
        Ok(())
    }
}

/// Returns the deterministic source id used for source-first ingest retries
/// without a staged manual document.
#[must_use]
pub fn deterministic_source_ingest_id(source: &Source) -> RecordId {
    RecordId::new(
        TableName::Source,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}|{}", source.base.space_id, source.dedup_key()).as_bytes(),
        ),
    )
}

const MAX_SOURCE_SIMPLE_URI_LEN: usize = 4096;

/// Validates simple source-ingest fields shared by API/MCP convenience surfaces.
///
/// # Errors
///
/// Returns a validation error if the human-readable name is invalid or if the
/// optional URI is blank/oversized.
pub fn validate_simple_source_fields(name: &str, uri: Option<&str>) -> TypeResult<()> {
    validate_display_name(name)?;

    if let Some(uri) = uri {
        let trimmed = uri.trim();
        if trimmed.is_empty() {
            return Err(TypeError::ValidationError(
                "uri cannot be empty when provided".to_string(),
            ));
        }
        if trimmed.len() > MAX_SOURCE_SIMPLE_URI_LEN {
            return Err(TypeError::ValidationError(format!(
                "uri cannot exceed {MAX_SOURCE_SIMPLE_URI_LEN} characters"
            )));
        }
    }

    Ok(())
}

/// Validates explicitly provided source/doc space IDs before caller overrides.
///
/// # Errors
///
/// Returns a validation error when both records specify non-empty space IDs that
/// disagree.
pub fn validate_explicit_source_doc_space_match(source: &Source, doc: &Doc) -> TypeResult<()> {
    let source_space = source.base.space_id.trim();
    let doc_space = doc.base.space_id.trim();
    if !source_space.is_empty() && !doc_space.is_empty() && source_space != doc_space {
        return Err(TypeError::ValidationError(format!(
            "source/doc space mismatch: source is in `{source_space}` but doc is in `{doc_space}`"
        )));
    }
    Ok(())
}

/// Builds a staged manual document template from a document record.
#[must_use]
pub fn manual_doc_template_from_doc(doc: &Doc) -> ManualDocTemplate {
    ManualDocTemplate {
        doc_id: doc.base.id.to_string(),
        title: doc.title.clone(),
        text: doc.text.clone(),
        sidecar_text: doc.sidecar_text.clone(),
        media_attachments: doc.media_attachments.clone(),
        rendition_key: doc.rendition_key.clone(),
        metadata: doc.metadata.clone(),
    }
}

/// Applies a staged manual document to a source, synchronizing source ACL fields.
///
/// # Errors
///
/// Returns an error if the manual document template cannot be serialized.
pub fn apply_manual_doc_to_source(source: &mut Source, doc: &Doc) -> TypeResult<String> {
    if source.base.space_id != doc.base.space_id {
        return Err(TypeError::ValidationError(format!(
            "source/doc space mismatch: source is in `{}` but doc is in `{}`",
            source.base.space_id, doc.base.space_id
        )));
    }
    source.base.security = doc.base.security.clone();
    source.base.provenance = doc.base.provenance.clone();
    source.base.subjects.clone_from(&doc.base.subjects);
    source.base.subjects_state = doc.base.subjects_state;
    source.author = Some(doc.base.created_by.clone());
    source.metadata = doc.metadata.clone();
    let template = manual_doc_template_from_doc(doc);
    let doc_id = template.doc_id.clone();
    source.set_manual_doc_template(&template)?;
    Ok(doc_id)
}

/// Prepares an existing source for a new source-first ingest pass while
/// preserving stable identity/audit fields and resetting lifecycle progress.
///
/// Generated file and URL sources retain the stored access envelope; a staged
/// manual document supplies the replacement envelope.
#[must_use]
pub fn prepare_source_for_reingest(existing: &Source, mut incoming: Source) -> Source {
    let has_staged_manual_doc = incoming.manual_doc_template().ok().flatten().is_some();
    if !has_staged_manual_doc {
        incoming.base.security = existing.base.security.clone();
        incoming.base.provenance = existing.base.provenance.clone();
        incoming.base.subjects.clone_from(&existing.base.subjects);
        incoming.base.subjects_state = existing.base.subjects_state;
    }
    reset_source_for_reingest(existing, incoming)
}

fn reset_source_for_reingest(existing: &Source, mut incoming: Source) -> Source {
    incoming.base.id = existing.base.id.clone();
    incoming.base.space_id.clone_from(&existing.base.space_id);
    incoming.base.created_at = existing.base.created_at;
    incoming
        .base
        .created_by
        .clone_from(&existing.base.created_by);
    incoming.base.deleted_at = None;
    incoming.lifecycle_status = SourceLifecycleStatus::Discovered;
    incoming.current_ingest_mode = None;
    incoming.latest_ingest_decision = None;
    incoming
}

/// Prepares a source-only re-ingest while retaining stored access and lineage.
///
/// Retains provenance for fields carrying source constructor defaults. Access fields and
/// provenance are considered independently, so adding lineage cannot clear ACLs.
/// Explicit access fields remain subject to the caller's write authorization.
/// Use the record-update API to intentionally clear an envelope to defaults.
#[must_use]
pub fn prepare_source_for_reingest_preserving_acl_if_placeholder(
    existing: &Source,
    incoming: Source,
) -> Source {
    let preserve_security = incoming.base.security.policy_id == "default"
        && incoming.base.security.level == 0
        && incoming.base.security.labels.is_empty()
        && incoming.base.security.handling.is_empty();
    let preserve_provenance = incoming.base.provenance == Provenance::default();
    let preserve_subjects =
        incoming.base.subjects.is_empty() && incoming.base.subjects_state == SubjectsState::Unknown;
    let mut prepared = reset_source_for_reingest(existing, incoming);
    if preserve_security {
        prepared.base.security = existing.base.security.clone();
    }
    if preserve_provenance {
        prepared.base.provenance = existing.base.provenance.clone();
    }
    if preserve_subjects {
        prepared.base.subjects.clone_from(&existing.base.subjects);
        prepared.base.subjects_state = existing.base.subjects_state;
    }
    prepared
}

/// Validates that an ingest mode is compatible with the presence of a staged doc.
///
/// # Errors
///
/// Returns a validation error when retrieval or graph mode is requested without
/// a staged manual document.
pub fn validate_manual_ingest_mode(ingest_mode: IngestMode, has_doc: bool) -> TypeResult<()> {
    if has_doc || matches!(ingest_mode, IngestMode::Metadata | IngestMode::Skip) {
        return Ok(());
    }

    Err(TypeError::ValidationError(
        "retrieval/graph source ingest requires a staged doc".to_string(),
    ))
}

fn default_metadata() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn ensure_metadata_object(
    metadata: &mut serde_json::Value,
) -> &mut serde_json::Map<String, serde_json::Value> {
    if let serde_json::Value::Object(map) = metadata {
        map
    } else {
        *metadata = default_metadata();
        ensure_metadata_object(metadata)
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
    use crate::ids::{RecordId, TableName};

    #[test]
    fn test_source_type_parse() {
        assert!(matches!(
            SourceType::from_str("email"),
            Ok(SourceType::Email)
        ));
        assert!(matches!(
            SourceType::from_str("device_frame"),
            Ok(SourceType::DeviceFrame)
        ));
        assert!(SourceType::from_str("invalid").is_err());
    }

    #[test]
    fn test_source_creation() {
        let id = RecordId::new_v4(TableName::Source);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let source = Source::new(base, SourceType::Email, "abc123".to_string());

        assert_eq!(source.type_, SourceType::Email);
        assert_eq!(source.hash, "abc123");
        assert_eq!(source.lifecycle_status, SourceLifecycleStatus::Discovered);
        assert_eq!(source.current_ingest_mode, None);
    }

    #[test]
    fn test_source_dedup_key() {
        let id = RecordId::new_v4(TableName::Source);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let mut source = Source::new(base, SourceType::Email, "abc123".to_string());

        // Without external_id
        assert_eq!(source.dedup_key(), "email:abc123");

        // With external_id
        source.external_id = Some("msg_456".to_string());
        assert_eq!(source.dedup_key(), "email:msg_456:abc123");
    }

    #[test]
    fn test_source_requested_ingest_mode_roundtrip() {
        let id = RecordId::new_v4(TableName::Source);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let mut source = Source::new(base, SourceType::Manual, "abc123".to_string());

        assert_eq!(source.requested_ingest_mode(), None);
        source.set_requested_ingest_mode(IngestMode::Graph);
        assert_eq!(source.requested_ingest_mode(), Some(IngestMode::Graph));
    }

    #[test]
    fn test_deterministic_source_ingest_id_uses_dedup_key() {
        let mut first = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "space_1".to_string(),
                "test".to_string(),
            ),
            SourceType::Manual,
            "abc123".to_string(),
        );
        first.external_id = Some("same".to_string());

        let mut second = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "space_1".to_string(),
                "other".to_string(),
            ),
            SourceType::Manual,
            "abc123".to_string(),
        );
        second.external_id = Some("same".to_string());

        let mut third = second.clone();
        third.external_id = Some("different".to_string());

        assert_eq!(
            deterministic_source_ingest_id(&first),
            deterministic_source_ingest_id(&second)
        );
        assert_ne!(
            deterministic_source_ingest_id(&first),
            deterministic_source_ingest_id(&third)
        );
    }

    #[test]
    fn test_source_manual_doc_template_roundtrip() {
        let id = RecordId::new_v4(TableName::Source);
        let base = BaseFields::new(id, "space_1".to_string(), "test".to_string());
        let mut source = Source::new(base, SourceType::Manual, "abc123".to_string());
        let template = ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Hello".to_string(),
            text: "World".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"collection": "default"}),
        };

        source
            .set_manual_doc_template(&template)
            .expect("set manual doc template");

        assert_eq!(
            source
                .manual_doc_template()
                .expect("read manual doc template"),
            Some(template)
        );
    }

    #[test]
    fn test_apply_manual_doc_to_source_inherits_security_and_template() {
        let source_id = RecordId::new_v4(TableName::Source);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let base = BaseFields::new(source_id, "space_1".to_string(), "test".to_string());
        let mut source = Source::new(base, SourceType::Manual, "abc123".to_string());

        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), "space_1".to_string(), "actor".to_string()),
            title: "Hello".to_string(),
            text: "World".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"collection": "default"}),
        };
        doc.base.security.level = 5;

        let staged_doc_id =
            apply_manual_doc_to_source(&mut source, &doc).expect("apply manual doc");

        assert_eq!(staged_doc_id, doc_id.to_string());
        assert_eq!(source.base.security.level, 5);
        assert_eq!(
            source
                .manual_doc_template()
                .expect("manual doc template")
                .expect("staged template")
                .doc_id,
            doc_id.to_string()
        );
        assert!(source
            .manual_doc_template()
            .expect("manual doc template")
            .expect("staged template")
            .media_attachments
            .is_empty());
    }

    #[test]
    fn test_apply_manual_doc_to_source_rejects_cross_space_doc() {
        let source_id = RecordId::new_v4(TableName::Source);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let base = BaseFields::new(source_id, "source-space".to_string(), "test".to_string());
        let mut source = Source::new(base, SourceType::Manual, "abc123".to_string());

        let doc = Doc {
            base: BaseFields::new(doc_id, "doc-space".to_string(), "actor".to_string()),
            title: "Hello".to_string(),
            text: "World".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"collection": "default"}),
        };

        let err = apply_manual_doc_to_source(&mut source, &doc)
            .expect_err("cross-space staged doc should fail");
        assert!(err.to_string().contains("space mismatch"));
    }

    #[test]
    fn test_validate_manual_ingest_mode_requires_doc_for_graph() {
        let err = validate_manual_ingest_mode(IngestMode::Graph, false)
            .expect_err("graph without doc should fail");
        assert!(err.to_string().contains("requires a staged doc"));
    }

    #[test]
    fn test_validate_simple_source_fields() {
        assert!(validate_simple_source_fields("Source name", Some("https://example.com")).is_ok());
        assert!(validate_simple_source_fields("", Some("https://example.com")).is_err());
        assert!(validate_simple_source_fields("Source name", Some("   ")).is_err());
        assert!(validate_simple_source_fields("Source name", Some(&"x".repeat(4097))).is_err());
    }

    #[test]
    fn test_validate_explicit_source_doc_space_match() {
        let source_id = RecordId::new_v4(TableName::Source);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let source = Source::new(
            BaseFields::new(source_id, "space_a".to_string(), "actor".to_string()),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(doc_id, "space_b".to_string(), "actor".to_string()),
            title: "Doc".to_string(),
            text: "Body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        let err = validate_explicit_source_doc_space_match(&source, &doc)
            .expect_err("mismatched explicit spaces should fail");
        assert!(err.to_string().contains("space mismatch"));
    }

    #[test]
    fn test_prepare_source_for_reingest_preserves_base_fields_and_resets_lifecycle() {
        let source_id = RecordId::new_v4(TableName::Source);
        let mut existing = Source::new(
            BaseFields::new(
                source_id.clone(),
                "space_1".to_string(),
                "creator".to_string(),
            ),
            SourceType::Manual,
            "existing-hash".to_string(),
        );
        existing.base.security.level = 5;
        let provenance_source = RecordId::new_v4(TableName::Source);
        existing.base.provenance.sources = vec![provenance_source.clone()];
        let subject_id = RecordId::new_v4(TableName::Entity);
        existing.base.subjects = vec![Link::new(subject_id.clone())];
        existing.lifecycle_status = SourceLifecycleStatus::GraphReady;
        existing.current_ingest_mode = Some(IngestMode::Graph);
        existing.latest_ingest_decision =
            Some(Link::new(RecordId::new_v4(TableName::IngestDecision)));

        let mut incoming = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "space_2".to_string(),
                "other".to_string(),
            ),
            SourceType::Manual,
            "incoming-hash".to_string(),
        );
        let incoming_provenance_source = RecordId::new_v4(TableName::Source);
        let incoming_subject_id = RecordId::new_v4(TableName::Entity);
        incoming.base.security.level = 9;
        incoming.base.provenance.sources = vec![incoming_provenance_source];
        incoming.base.subjects = vec![Link::new(incoming_subject_id)];
        incoming.metadata = serde_json::json!({"fresh": true});
        incoming.lifecycle_status = SourceLifecycleStatus::Rejected;
        incoming.current_ingest_mode = Some(IngestMode::Metadata);

        let prepared = prepare_source_for_reingest(&existing, incoming);

        assert_eq!(prepared.base.id, source_id);
        assert_eq!(prepared.base.space_id, "space_1");
        assert_eq!(prepared.base.created_by, "creator");
        assert_eq!(prepared.base.security.level, 5);
        assert_eq!(prepared.base.provenance.sources, vec![provenance_source]);
        assert_eq!(prepared.base.subjects, vec![Link::new(subject_id)]);
        assert_eq!(prepared.lifecycle_status, SourceLifecycleStatus::Discovered);
        assert_eq!(prepared.current_ingest_mode, None);
        assert!(prepared.latest_ingest_decision.is_none());
        assert_eq!(prepared.metadata, serde_json::json!({"fresh": true}));
    }

    #[test]
    fn test_source_only_reingest_preserves_placeholder_acl_even_with_manual_template() {
        let mut existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".into(),
                "creator".into(),
            ),
            SourceType::Manual,
            "original".into(),
        );
        existing.base.security.level = 5;
        existing.base.security.labels = vec!["private".into()];
        existing.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        existing.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        existing.base.subjects_state = SubjectsState::Complete;
        let mut incoming = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".into(),
                "other".into(),
            ),
            SourceType::Manual,
            "updated".into(),
        );
        let doc = Doc {
            base: incoming.base.clone(),
            title: "Stale template".into(),
            text: "Template metadata does not authorize an ACL replacement".into(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "template".into(),
            metadata: serde_json::json!({}),
        };
        incoming
            .set_manual_doc_template(&manual_doc_template_from_doc(&doc))
            .expect("template");

        let prepared =
            prepare_source_for_reingest_preserving_acl_if_placeholder(&existing, incoming);
        assert_eq!(prepared.base.security, existing.base.security);
        assert_eq!(prepared.base.provenance, existing.base.provenance);
        assert_eq!(prepared.base.subjects, existing.base.subjects);
        assert_eq!(prepared.base.subjects_state, existing.base.subjects_state);
        assert_eq!(prepared.hash, "updated");
    }

    #[test]
    fn test_source_only_reingest_honors_explicit_acl_and_provenance() {
        let existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".into(),
                "creator".into(),
            ),
            SourceType::Manual,
            "original".into(),
        );
        let mut incoming = existing.clone();
        incoming.base.security.level = 7;
        incoming.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        incoming.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        incoming.base.subjects_state = SubjectsState::Complete;
        let prepared =
            prepare_source_for_reingest_preserving_acl_if_placeholder(&existing, incoming.clone());
        assert_eq!(prepared.base.security, incoming.base.security);
        assert_eq!(prepared.base.provenance, incoming.base.provenance);
        assert_eq!(prepared.base.subjects, incoming.base.subjects);
        assert_eq!(prepared.base.subjects_state, incoming.base.subjects_state);
    }

    #[test]
    fn test_source_only_reingest_lineage_update_does_not_clear_acl_placeholders() {
        let mut existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".into(),
                "creator".into(),
            ),
            SourceType::Manual,
            "original".into(),
        );
        existing.base.security.level = 5;
        existing.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        existing.base.subjects_state = SubjectsState::Complete;
        let mut incoming = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".into(),
                "other".into(),
            ),
            SourceType::Manual,
            "updated".into(),
        );
        incoming.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        let prepared =
            prepare_source_for_reingest_preserving_acl_if_placeholder(&existing, incoming.clone());
        assert_eq!(prepared.base.security, existing.base.security);
        assert_eq!(prepared.base.subjects, existing.base.subjects);
        assert_eq!(prepared.base.subjects_state, existing.base.subjects_state);
        assert_eq!(prepared.base.provenance, incoming.base.provenance);
    }
}
