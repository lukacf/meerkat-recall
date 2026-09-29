//! Field-level redaction helpers for API responses.
//!
//! This implements the redaction contract described in `docs/spec.md`.

use elephant_policy::RedactionEnvelope;
use elephant_types::records::{Doc, Source};

use crate::auth::Principal as AuthPrincipal;

#[allow(dead_code)]
fn has_scope(principal: &AuthPrincipal, scope: &str) -> bool {
    principal.scopes.iter().any(|s| s == scope || s == "*")
}

/// Redacts a doc for the given principal.
///
/// If `read:doc:raw` is missing, `doc.text` is redacted.
#[must_use]
#[allow(dead_code)]
pub fn redact_doc(principal: &AuthPrincipal, mut doc: Doc) -> RedactionEnvelope<Doc> {
    let mut redactions: Vec<String> = Vec::new();

    if !has_scope(principal, "read:doc:raw") && !doc.text.is_empty() {
        doc.text.clear();
        redactions.push("doc.text".to_string());
    }

    RedactionEnvelope::with_redactions(doc, redactions)
}

/// Redacts a source for the given principal.
///
/// If `read:source:raw` is missing, `source.uri` and `source.path` are redacted.
#[must_use]
#[allow(dead_code)]
pub fn redact_source(principal: &AuthPrincipal, mut source: Source) -> RedactionEnvelope<Source> {
    let mut redactions: Vec<String> = Vec::new();

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
