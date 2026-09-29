//! Assertion validation per spec §8.5

use crate::extraction::RawAssertion;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A validation error for an assertion field.
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// The field that failed validation.
    pub field: String,
    /// Human-readable error message.
    pub message: String,
    /// Error code for programmatic handling.
    pub code: ValidationErrorCode,
}

/// Error codes for validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationErrorCode {
    /// Evidence span end exceeds revision text length.
    SpanOutOfBounds,
    /// Evidence span start > end.
    SpanInvalid,
    /// Evidence span is missing a node reference.
    MissingEvidenceNode,
    /// Subject text is empty or missing.
    MissingSubject,
    /// Predicate text is empty or missing.
    MissingPredicate,
    /// Raw quote provided but not allowed by policy.
    QuoteNotAllowed,
}

/// Validator for raw assertions extracted from documents.
///
/// Per spec §8.5: validates assertion commit invariants including
/// span bounds, required fields, and quote policy.
pub struct AssertionValidator {
    /// Whether raw quotes are allowed in assertions.
    pub allow_raw_quotes: bool,
    /// Maximum span length in bytes.
    pub max_span_length: u32,
}

impl Default for AssertionValidator {
    fn default() -> Self {
        Self {
            allow_raw_quotes: false,
            max_span_length: 10_000,
        }
    }
}

impl AssertionValidator {
    /// Creates a new validator with the specified quote policy.
    #[must_use]
    pub fn new(allow_raw_quotes: bool) -> Self {
        Self {
            allow_raw_quotes,
            ..Default::default()
        }
    }

    /// Validates an assertion per §8.5 commit invariants.
    ///
    /// # Errors
    ///
    /// Returns a list of validation errors if any of the following conditions are violated:
    /// - Evidence span end exceeds revision text length
    /// - Evidence span start > end
    /// - Raw quote present when quotes are disallowed
    /// - Missing subject or predicate text
    pub fn validate(
        &self,
        assertion: &RawAssertion,
        revision_text_len: u32,
    ) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Check span bounds
        if assertion.evidence_span.end > revision_text_len {
            errors.push(ValidationError {
                field: "evidence_span".into(),
                message: format!(
                    "end {} > text len {}",
                    assertion.evidence_span.end, revision_text_len
                ),
                code: ValidationErrorCode::SpanOutOfBounds,
            });
        }
        if assertion.evidence_span.start > assertion.evidence_span.end {
            errors.push(ValidationError {
                field: "evidence_span".into(),
                message: "start > end".into(),
                code: ValidationErrorCode::SpanInvalid,
            });
        }
        if assertion.evidence_span.node.is_none() {
            errors.push(ValidationError {
                field: "evidence_span.node".into(),
                message: "Required".into(),
                code: ValidationErrorCode::MissingEvidenceNode,
            });
        }

        // Check quote policy
        if !self.allow_raw_quotes && assertion.quote.is_some() {
            errors.push(ValidationError {
                field: "quote".into(),
                message: "Raw quotes not allowed".into(),
                code: ValidationErrorCode::QuoteNotAllowed,
            });
        }

        // Check required fields
        if assertion.subject_text.is_empty() {
            errors.push(ValidationError {
                field: "subject_text".into(),
                message: "Required".into(),
                code: ValidationErrorCode::MissingSubject,
            });
        }
        if assertion.predicate_text.is_empty() {
            errors.push(ValidationError {
                field: "predicate_text".into(),
                message: "Required".into(),
                code: ValidationErrorCode::MissingPredicate,
            });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Generates an idempotent assertion key for deduplication.
    ///
    /// The key is derived from the revision ID, subject text, predicate text,
    /// and span start position to ensure uniqueness within a revision.
    #[must_use]
    pub fn generate_assertion_key(assertion: &RawAssertion, revision_id: &str) -> String {
        let mut h = DefaultHasher::new();
        revision_id.hash(&mut h);
        assertion.subject_text.hash(&mut h);
        assertion.predicate_text.hash(&mut h);
        assertion.evidence_span.start.hash(&mut h);
        assertion.evidence_span.end.hash(&mut h);
        if let Some(object_text) = &assertion.object_text {
            object_text.hash(&mut h);
        }
        if let Some(object_literal) = &assertion.object_literal {
            if let Ok(serialized) = serde_json::to_string(object_literal) {
                serialized.hash(&mut h);
            }
        }
        format!("assertion_{:x}", h.finish())
    }

    /// Hashes a quote string for storage (avoids storing raw quotes).
    #[must_use]
    pub fn hash_quote(quote: &str) -> String {
        let mut h = DefaultHasher::new();
        quote.hash(&mut h);
        format!("{:x}", h.finish())
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use crate::extraction::RawAssertion;
    use elephant_types::ids::TableName;
    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::{Link, RecordId};

    fn make_assertion(subject: &str, predicate: &str, start: u32, end: u32) -> RawAssertion {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let node_id = RecordId::new_v4(TableName::DocNode);
        RawAssertion {
            subject_text: subject.into(),
            predicate_text: predicate.into(),
            object_text: None,
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(revision_id),
                node: Some(Link::new(node_id)),
                start,
                end,
                quote_hash: "test_hash".into(),
            },
            confidence: 0.9,
            quote: None,
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: None,
            object_entity_id: None,
            object_entity_type: None,
            significance: None,
        }
    }

    #[test]
    fn test_validate_valid() {
        let v = AssertionValidator::default();
        let a = make_assertion("Alice", "knows", 0, 10);
        assert!(v.validate(&a, 100).is_ok());
    }

    #[test]
    fn test_validate_span_out_of_bounds() {
        let v = AssertionValidator::default();
        let a = make_assertion("Alice", "knows", 0, 200);
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::SpanOutOfBounds));
    }

    #[test]
    fn test_validate_span_invalid() {
        let v = AssertionValidator::default();
        let a = make_assertion("Alice", "knows", 50, 10);
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::SpanInvalid));
    }

    #[test]
    fn test_validate_missing_evidence_node() {
        let v = AssertionValidator::default();
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let mut a = make_assertion("Alice", "knows", 0, 10);
        a.evidence_span.revision = Link::new(revision_id);
        a.evidence_span.node = None;
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::MissingEvidenceNode));
    }

    #[test]
    fn test_validate_quote_not_allowed() {
        let v = AssertionValidator::default();
        let mut a = make_assertion("Alice", "knows", 0, 10);
        a.quote = Some("raw quote".into());
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::QuoteNotAllowed));
    }

    #[test]
    fn test_validate_quote_allowed() {
        let v = AssertionValidator::new(true);
        let mut a = make_assertion("Alice", "knows", 0, 10);
        a.quote = Some("raw quote".into());
        assert!(v.validate(&a, 100).is_ok());
    }

    #[test]
    fn test_validate_missing_subject() {
        let v = AssertionValidator::default();
        let a = make_assertion("", "knows", 0, 10);
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::MissingSubject));
    }

    #[test]
    fn test_validate_missing_predicate() {
        let v = AssertionValidator::default();
        let a = make_assertion("Alice", "", 0, 10);
        let errs = v.validate(&a, 100).unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == ValidationErrorCode::MissingPredicate));
    }

    #[test]
    fn test_assertion_key_deterministic() {
        let a = make_assertion("Alice", "knows", 0, 10);
        let k1 = AssertionValidator::generate_assertion_key(&a, "rev1");
        let k2 = AssertionValidator::generate_assertion_key(&a, "rev1");
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_assertion_key_different_for_different_revisions() {
        let a = make_assertion("Alice", "knows", 0, 10);
        let k1 = AssertionValidator::generate_assertion_key(&a, "rev1");
        let k2 = AssertionValidator::generate_assertion_key(&a, "rev2");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_hash_quote() {
        let h1 = AssertionValidator::hash_quote("test quote");
        let h2 = AssertionValidator::hash_quote("test quote");
        assert_eq!(h1, h2);

        let h3 = AssertionValidator::hash_quote("different quote");
        assert_ne!(h1, h3);
    }
}
