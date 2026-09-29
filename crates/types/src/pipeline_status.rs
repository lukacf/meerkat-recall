//! Pipeline status views for MCP/HTTP consumers.

use serde::{Deserialize, Serialize};

use crate::RecordId;

/// Status of document chunking and embedding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkStatus {
    /// Total number of chunks in the document.
    pub total: usize,
    /// Number of chunks that have been embedded.
    pub embedded: usize,
}

/// State of the extraction pipeline for a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionState {
    /// Extraction is queued but not yet started.
    Pending,
    /// Extraction is currently in progress.
    Running,
    /// Extraction completed successfully.
    Completed,
    /// Extraction failed with an error.
    Failed,
    /// Extraction has not been triggered for this document.
    NotTriggered,
}

/// Debug view of how extraction guidance (purpose/skill) was resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractionGuidanceStatus {
    /// Optional resolved extraction purpose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    /// Source that provided the resolved purpose.
    pub purpose_source: String,
    /// Optional resolved skill ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_id: Option<String>,
    /// Source that provided the resolved skill.
    pub skill_source: String,
    /// Ordered trace of fallback checks and hits/misses for debugging.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution_trace: Vec<String>,
}

/// Pipeline status for a document, including chunking, embedding, and extraction progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocPipelineStatus {
    /// The document ID.
    pub doc_id: RecordId,
    /// Chunking and embedding status.
    pub chunks: ChunkStatus,
    /// Current revision being processed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<RecordId>,
    /// Number of document nodes created.
    pub nodes_count: usize,
    /// Current state of extraction.
    pub extraction_state: ExtractionState,
    /// Number of assertions extracted.
    pub assertions_count: usize,
    /// Number of entities created during extraction.
    pub entities_created: usize,
    /// Optional extraction guidance resolution details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_guidance: Option<ExtractionGuidanceStatus>,
}

impl DocPipelineStatus {
    /// Creates an empty status for a document that hasn't been processed.
    #[must_use]
    pub const fn empty(doc_id: RecordId) -> Self {
        Self {
            doc_id,
            chunks: ChunkStatus {
                total: 0,
                embedded: 0,
            },
            revision: None,
            nodes_count: 0,
            extraction_state: ExtractionState::NotTriggered,
            assertions_count: 0,
            entities_created: 0,
            extraction_guidance: None,
        }
    }
}

use crate::records::work::WorkItemStatus;

/// Derives the extraction state from the assertion count and relevant work item statuses.
///
/// Shared by both the HTTP API and MCP pipeline-status handlers.
#[must_use]
pub fn derive_extraction_state(
    assertions_count: usize,
    relevant_work_items: &[WorkItemStatus],
) -> ExtractionState {
    if relevant_work_items.is_empty() {
        if assertions_count > 0 {
            return ExtractionState::Completed;
        }
        return ExtractionState::NotTriggered;
    }

    if relevant_work_items.contains(&WorkItemStatus::Failed) {
        return ExtractionState::Failed;
    }

    if relevant_work_items.contains(&WorkItemStatus::Running) {
        return ExtractionState::Running;
    }

    if relevant_work_items
        .iter()
        .any(|status| matches!(status, WorkItemStatus::Pending | WorkItemStatus::Runnable))
    {
        return ExtractionState::Pending;
    }

    ExtractionState::Completed
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
    use crate::ids::TableName;

    #[test]
    fn test_doc_pipeline_status_roundtrip() {
        let doc_id = RecordId::new_v4(TableName::Doc);
        let revision = RecordId::new_v4(TableName::DocRevision);

        let status = DocPipelineStatus {
            doc_id,
            chunks: ChunkStatus {
                total: 12,
                embedded: 9,
            },
            revision: Some(revision),
            nodes_count: 4,
            extraction_state: ExtractionState::Running,
            assertions_count: 2,
            entities_created: 1,
            extraction_guidance: Some(ExtractionGuidanceStatus {
                purpose: Some("test purpose".to_string()),
                purpose_source: "space_config".to_string(),
                skill_id: Some("extraction/technical-docs".to_string()),
                skill_source: "doc.metadata".to_string(),
                resolution_trace: vec![
                    "purpose:space_config=hit".to_string(),
                    "skill:doc.metadata=hit".to_string(),
                ],
            }),
        };

        let encoded = serde_json::to_string(&status).unwrap();
        let decoded: DocPipelineStatus = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, status);
    }

    #[test]
    fn test_extraction_state_encoding() {
        let encoded = serde_json::to_value(ExtractionState::Pending).unwrap();
        assert_eq!(encoded, serde_json::Value::String("pending".to_string()));

        let encoded = serde_json::to_value(ExtractionState::NotTriggered).unwrap();
        assert_eq!(
            encoded,
            serde_json::Value::String("not_triggered".to_string())
        );
    }
}
