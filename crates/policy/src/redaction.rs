//! Field-level redaction for responses.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Response envelope with redaction information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionEnvelope<T> {
    /// The (possibly redacted) data.
    pub data: T,
    /// List of redacted field paths.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redactions: Vec<String>,
}

impl<T> RedactionEnvelope<T> {
    /// Creates an envelope with no redactions.
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self {
            data,
            redactions: Vec::new(),
        }
    }

    /// Creates an envelope with redactions.
    #[must_use]
    pub const fn with_redactions(data: T, redactions: Vec<String>) -> Self {
        Self { data, redactions }
    }

    /// Returns true if any fields were redacted.
    #[must_use]
    pub const fn has_redactions(&self) -> bool {
        !self.redactions.is_empty()
    }
}

/// Redaction rules for different scopes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RedactionRules {
    /// Fields to redact when scope is missing.
    #[serde(default)]
    rules: Vec<RedactionRule>,
}

/// A single redaction rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionRule {
    /// The scope required to see this field.
    pub required_scope: String,
    /// Field paths to redact if scope is missing.
    #[serde(default)]
    pub fields: Vec<String>,
}

impl RedactionRules {
    /// Creates a new empty ruleset.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a redaction rule.
    pub fn add_rule(&mut self, required_scope: impl Into<String>, fields: Vec<String>) {
        self.rules.push(RedactionRule {
            required_scope: required_scope.into(),
            fields,
        });
    }

    /// Returns the fields to redact given the principal's scopes.
    ///
    /// Returns a list of field paths that should be redacted because
    /// the principal lacks the required scope.
    #[must_use]
    pub fn fields_to_redact(&self, scopes: &HashSet<String>) -> Vec<String> {
        let mut redactions = Vec::new();
        for rule in &self.rules {
            if !scopes.contains(&rule.required_scope) {
                redactions.extend(rule.fields.clone());
            }
        }
        redactions
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

    #[test]
    fn test_redaction_envelope() {
        let envelope = RedactionEnvelope::with_redactions("data", vec!["doc.text".to_string()]);

        assert!(envelope.has_redactions());
        assert_eq!(envelope.redactions[0], "doc.text");
    }

    #[test]
    fn test_redaction_rules() {
        let mut rules = RedactionRules::new();
        rules.add_rule("read:doc:raw", vec!["doc.text".to_string()]);
        rules.add_rule("read:source:raw", vec!["source.path".to_string()]);

        let scopes: HashSet<String> = std::iter::once("read:entity".to_string()).collect();
        let redactions = rules.fields_to_redact(&scopes);

        assert!(redactions.contains(&"doc.text".to_string()));
        assert!(redactions.contains(&"source.path".to_string()));

        // With the required scope, no redaction
        let scopes: HashSet<String> = std::iter::once("read:doc:raw".to_string()).collect();
        let redactions = rules.fields_to_redact(&scopes);
        assert!(!redactions.contains(&"doc.text".to_string()));
    }
}
