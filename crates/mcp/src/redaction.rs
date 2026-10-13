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
/// If `read:doc:raw` is missing, `doc.text` is redacted.
#[must_use]
pub fn redact_doc(principal: &AbacPrincipal, mut doc: Doc) -> RedactionEnvelope<Doc> {
    let mut redactions: Vec<String> = Vec::new();

    if !has_scope(principal, "read:doc:raw") && !doc.text.is_empty() {
        doc.text.clear();
        redactions.push("doc.text".to_string());
    }

    RedactionEnvelope::with_redactions(doc, redactions)
}
