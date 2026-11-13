//! Field-level redaction helpers for MCP tool responses.
//!
//! This implements the redaction contract described in `docs/spec.md`.

use elephant_policy::{Principal as AbacPrincipal, RedactionEnvelope};
use elephant_types::records::Doc;

fn has_scope(principal: &AbacPrincipal, scope: &str) -> bool {
    principal.scopes.contains(scope) || principal.scopes.contains("*")
}

/// Redacts a doc for the given principal.
///
/// Without `read:doc:raw`, remove text, sidecar text and attachment locations.
#[must_use]
pub fn redact_doc(principal: &AbacPrincipal, mut doc: Doc) -> RedactionEnvelope<Doc> {
    let mut redactions: Vec<String> = Vec::new();

    if !has_scope(principal, "read:doc:raw") {
        if !doc.text.is_empty() {
            doc.text.clear();
            redactions.push("doc.text".to_string());
        }
        if doc.sidecar_text.take().is_some() {
            redactions.push("doc.sidecar_text".to_string());
        }
        if !doc.media_attachments.is_empty() {
            doc.media_attachments.clear();
            redactions.push("doc.media_attachments".to_string());
        }
    }

    RedactionEnvelope::with_redactions(doc, redactions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_types::records::{BaseFields, DocMediaAttachment, DocMediaKind, DocMediaLocation};
    use elephant_types::{RecordId, TableName};
    use std::collections::HashSet;

    fn fixture() -> Doc {
        Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "projection".into(),
                "fixture".into(),
            ),
            title: "Metadata title".into(),
            text: String::new(),
            sidecar_text: Some("private sidecar canary".into()),
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "attachment".into(),
                kind: DocMediaKind::Image,
                media_type: "image/png".into(),
                location: DocMediaLocation::Path {
                    path: "/private/media-canary.png".into(),
                },
                label: None,
                size_bytes: None,
                duration_ms: None,
                sha256: None,
            }],
            rendition_key: "fixture".into(),
            metadata: serde_json::json!({"collection":"public-metadata"}),
        }
    }

    fn principal(scope: &str) -> AbacPrincipal {
        AbacPrincipal {
            space_id: "projection".into(),
            scopes: HashSet::from([scope.into()]),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    #[test]
    fn metadata_read_hides_sidecar_and_attachments_even_without_main_text() {
        let original = fixture();
        let output = redact_doc(&principal("read:doc"), original.clone());
        assert!(output.data.text.is_empty());
        assert!(output.data.sidecar_text.is_none());
        assert!(output.data.media_attachments.is_empty());
        assert_eq!(output.data.title, original.title);
        assert_eq!(output.data.metadata, original.metadata);
        assert_eq!(
            output.redactions,
            ["doc.sidecar_text", "doc.media_attachments"]
        );
        assert!(original.sidecar_text.is_some());
    }

    #[test]
    fn raw_and_wildcard_reads_preserve_all_document_fields() {
        let mut original = fixture();
        original.text = "private primary text".into();
        for scope in ["read:doc:raw", "*"] {
            let output = redact_doc(&principal(scope), original.clone());
            assert_eq!(output.data, original);
            assert!(output.redactions.is_empty());
        }
    }
}
