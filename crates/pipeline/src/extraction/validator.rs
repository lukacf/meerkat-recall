//! Assertion validation per spec §8.5

use crate::extraction::RawAssertion;
use sha2::{Digest, Sha256};
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
    /// Evidence span is empty, reversed, or exceeds the configured maximum.
    SpanInvalid,
    /// Evidence span is missing a node reference.
    MissingEvidenceNode,
    /// Confidence is not finite or is outside 0..=1.
    InvalidConfidence,
    /// Source quote hash is absent or not a SHA-256 digest.
    InvalidQuoteHash,
    /// Object has inconsistent entity/literal forms or a nonfinite value.
    InvalidObject,
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
        if assertion.evidence_span.start >= assertion.evidence_span.end
            || assertion
                .evidence_span
                .end
                .saturating_sub(assertion.evidence_span.start)
                > self.max_span_length
        {
            errors.push(ValidationError {
                field: "evidence_span".into(),
                message: "Evidence must be nonempty and within the span length limit".into(),
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

        if !assertion.confidence.is_finite() || !(0.0..=1.0).contains(&assertion.confidence) {
            errors.push(ValidationError {
                field: "confidence".into(),
                message: "Confidence must be finite and between zero and one".into(),
                code: ValidationErrorCode::InvalidConfidence,
            });
        }
        if !Self::is_quote_hash(&assertion.evidence_span.quote_hash) {
            errors.push(ValidationError {
                field: "evidence_span.quote_hash".into(),
                message: "An exact SHA-256 source quote hash is required".into(),
                code: ValidationErrorCode::InvalidQuoteHash,
            });
        }

        if (assertion.object_text.is_some() && assertion.object_literal.is_some())
            || assertion
                .object_text
                .as_deref()
                .is_some_and(|object| object.trim().is_empty())
            || matches!(assertion.object_literal, Some(elephant_types::records::attribute::Literal::Float(value)) if !value.is_finite())
        {
            errors.push(ValidationError {
                field: "object".into(),
                message: "Object must have one consistent, nonempty entity or finite literal value"
                    .into(),
                code: ValidationErrorCode::InvalidObject,
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
        if assertion.subject_text.trim().is_empty() {
            errors.push(ValidationError {
                field: "subject_text".into(),
                message: "Required".into(),
                code: ValidationErrorCode::MissingSubject,
            });
        }
        if assertion.predicate_text.trim().is_empty() {
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

    /// Whether a value is a canonical lowercase SHA-256 quote digest.
    #[must_use]
    pub fn is_quote_hash(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    /// Validates the exact byte slice, UTF-8 boundaries, hash and optional quote.
    /// This validates evidence identity; semantic support is checked separately.
    pub fn validate_evidence(assertion: &RawAssertion, text: &str) -> Result<(), String> {
        let span = &assertion.evidence_span;
        if span.start >= span.end {
            return Err("Evidence span must be nonempty".into());
        }
        let quote = text
            .get(span.start as usize..span.end as usize)
            .ok_or_else(|| "Evidence span is outside source text or splits UTF-8".to_string())?;
        if quote.trim().is_empty() || span.quote_hash != Self::hash_quote(quote) {
            return Err("Evidence quote hash does not match immutable source bytes".into());
        }
        if assertion
            .quote
            .as_deref()
            .is_some_and(|provided| provided != quote)
        {
            return Err("Evidence quote does not match immutable source bytes".into());
        }
        Ok(())
    }

    /// Hashes exact UTF-8 quote bytes using SHA-256.
    #[must_use]
    pub fn hash_quote(quote: &str) -> String {
        hex::encode(Sha256::digest(quote.as_bytes()))
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
            support_verification: None,
            subject_text: subject.into(),
            predicate_text: predicate.into(),
            object_text: None,
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(revision_id),
                node: Some(Link::new(node_id)),
                start,
                end,
                quote_hash: AssertionValidator::hash_quote("test quote"),
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
