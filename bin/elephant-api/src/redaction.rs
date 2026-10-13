//! Field-level redaction helpers for API responses.
//!
//! This implements the redaction contract described in `docs/spec.md`.

use elephant_policy::RedactionEnvelope;
use elephant_types::records::{Doc, Source};

use crate::auth::Principal as AuthPrincipal;

#[allow(dead_code)]
fn has_scope(principal: &AuthPrincipal, scope: &str) -> bool {
    principal.bypass_abac || principal.scopes.iter().any(|s| s == scope || s == "*")
}

/// Redacts a doc for the given principal.
///
/// Without `read:doc:raw`, remove text, sidecar text and attachment locations.
#[must_use]
#[allow(dead_code)]
pub fn redact_doc(principal: &AuthPrincipal, mut doc: Doc) -> RedactionEnvelope<Doc> {
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

/// Redacts a source for the given principal.
///
/// If `read:source:raw` is missing, `source.uri` and `source.path` are redacted.
#[must_use]
#[allow(dead_code)]
pub fn redact_source(principal: &AuthPrincipal, source: Source) -> RedactionEnvelope<Source> {
    let private_template = source.metadata.get("_manual_doc").is_some();
    let mut source = elephant_storage::source_intake::public_source_projection(source);
    let mut redactions: Vec<String> = Vec::new();
    if private_template {
        redactions.push("source.metadata._manual_doc".to_string());
    }

    if !has_scope(principal, "read:source:raw") {
        if source.uri.is_some() {
            source.uri = None;
            redactions.push("source.uri".to_string());
        }
        if source.path.is_some() {
            source.path = None;
            redactions.push("source.path".to_string());
        }
    }

    RedactionEnvelope::with_redactions(source, redactions)
}
