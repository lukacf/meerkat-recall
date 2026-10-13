//! Document node type for structural representation.
//!
//! Per spec §7.2, `doc_node` represents structural nodes over a revision.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::Link;

use super::doc_revision::DocRevision;

/// Role of a document node in the structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Root document node.
    #[default]
    Document,
    /// Section heading/container.
    Section,
    /// Paragraph block.
    Paragraph,
    /// Inline span.
    Span,
    /// Generated summary.
    Summary,
    /// Retrieval-optimized chunk.
    Chunk,
    /// List container (ul/ol).
    List,
    /// Table container.
    Table,
    /// Table cell (child of Table).
    TableCell,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Document => write!(f, "document"),
            Self::Section => write!(f, "section"),
            Self::Paragraph => write!(f, "paragraph"),
            Self::Span => write!(f, "span"),
            Self::Summary => write!(f, "summary"),
            Self::Chunk => write!(f, "chunk"),
            Self::List => write!(f, "list"),
            Self::Table => write!(f, "table"),
            Self::TableCell => write!(f, "table_cell"),
        }
    }
}

impl FromStr for NodeRole {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "document" => Ok(Self::Document),
            "section" => Ok(Self::Section),
            "paragraph" => Ok(Self::Paragraph),
            "span" => Ok(Self::Span),
            "summary" => Ok(Self::Summary),
            "chunk" => Ok(Self::Chunk),
            "list" => Ok(Self::List),
            "table" => Ok(Self::Table),
            "table_cell" => Ok(Self::TableCell),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "node_role".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for NodeRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct NodeRoleVisitor;

        impl Visitor<'_> for NodeRoleVisitor {
            type Value = NodeRole;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for NodeRole")
            }

            fn visit_str<E>(self, value: &str) -> Result<NodeRole, E>
            where
                E: de::Error,
            {
                NodeRole::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(NodeRoleVisitor)
    }
}

/// Span offsets into revision text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextSpan {
    /// Start offset (inclusive, byte index).
    pub start: u32,
    /// End offset (exclusive, byte index).
    pub end: u32,
}

impl TextSpan {
    /// Creates a new text span.
    #[must_use]
    pub const fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }

    /// Returns the length of the span in bytes.
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.end.saturating_sub(self.start)
    }

    /// Returns true if the span is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Validates that the span is within bounds of the given text length.
    #[must_use]
    pub const fn is_valid(&self, text_len: u32) -> bool {
        self.start <= self.end && self.end <= text_len
    }
}

/// A structural node in a document revision.
///
/// Per spec §7.2: nodes are revision-scoped with roles and span offsets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocNode {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Parent revision this node belongs to.
    pub revision: Link<DocRevision>,

    /// Role of this node in the document structure.
    pub role: NodeRole,

    /// Span offsets into the revision text.
    pub span: TextSpan,

    /// Stable node key for idempotency.
    pub node_key: String,

    /// Optional text content (for summary nodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,

    /// Parent node (if not root).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Link<Self>>,

    /// Position within parent (for ordering siblings).
    #[serde(default)]
    pub position: i32,

    /// Depth in the node tree (0 for document root).
    #[serde(default)]
    pub depth: i32,

    /// Embedding model used for this node (set after embedding).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_model: Option<String>,

    /// Collection name for retrieval grouping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
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

    #[test]
    fn test_node_role_roundtrip() {
        for role in [
            NodeRole::Document,
            NodeRole::Section,
            NodeRole::Paragraph,
            NodeRole::Span,
            NodeRole::Summary,
            NodeRole::Chunk,
            NodeRole::List,
            NodeRole::Table,
            NodeRole::TableCell,
        ] {
            let s = role.to_string();
            let parsed = NodeRole::from_str(&s).unwrap();
            assert_eq!(role, parsed);
        }
    }

    #[test]
    fn test_text_span_validation() {
        let span = TextSpan::new(10, 20);
        assert!(span.is_valid(100));
        assert!(span.is_valid(20));
        assert!(!span.is_valid(15));
        assert_eq!(span.len(), 10);
        assert!(!span.is_empty());

        let empty_span = TextSpan::new(10, 10);
        assert!(empty_span.is_empty());
    }

    #[test]
    fn test_node_role_serde() {
        let role = NodeRole::Chunk;
        let json = serde_json::to_string(&role).unwrap();
        assert_eq!(json, "\"chunk\"");

        let parsed: NodeRole = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, role);
    }

    #[test]
    fn test_doc_node_embedding_fields_serde() {
        use crate::ids::TableName;
        use crate::records::base::BaseFields;
        use crate::RecordId;

        let id = RecordId::new_v4(TableName::DocNode);
        let base = BaseFields::new(id, "space1".to_string(), "agent".to_string());
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let node = DocNode {
            base,
            revision: Link::new(revision_id),
            role: NodeRole::Chunk,
            span: TextSpan::new(0, 50),
            node_key: "k1".to_string(),
            text: None,
            parent: None,
            position: 0,
            depth: 1,
            embedding_model: Some("text-embedding-3-small".to_string()),
            collection: Some("notes".to_string()),
        };

        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["embedding_model"], "text-embedding-3-small");
        assert_eq!(json["collection"], "notes");

        // Round-trip
        let parsed: DocNode = serde_json::from_value(json).unwrap();
        assert_eq!(
            parsed.embedding_model.as_deref(),
            Some("text-embedding-3-small")
        );
        assert_eq!(parsed.collection.as_deref(), Some("notes"));
    }

    #[test]
    fn test_doc_node_embedding_fields_default_to_none() {
        use crate::ids::TableName;
        use crate::records::base::BaseFields;
        use crate::RecordId;

        let id = RecordId::new_v4(TableName::DocNode);
        let base = BaseFields::new(id, "space1".to_string(), "agent".to_string());
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let node = DocNode {
            base,
            revision: Link::new(revision_id),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, 50),
            node_key: "k2".to_string(),
            text: None,
            parent: None,
            position: 0,
            depth: 1,
            embedding_model: None,
            collection: None,
        };

        let json = serde_json::to_value(&node).unwrap();
        // Fields should be absent when None (skip_serializing_if)
        assert!(json.get("embedding_model").is_none());
        assert!(json.get("collection").is_none());

        // Deserializing without these fields should default to None
        let parsed: DocNode = serde_json::from_value(json).unwrap();
        assert!(parsed.embedding_model.is_none());
        assert!(parsed.collection.is_none());
    }
}
