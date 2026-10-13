//! Evidence identity and independently verified support are different contracts.
//! This receipt is minted by trusted verification code, never deserialized from
//! an extractor's model response. It binds every promoted semantic field to an
//! exact immutable revision and quote. It is not a proof of real-world truth.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::RawAssertion;
use crate::error::PipelineError;

/// Version of the independent extraction support-verification contract.
pub const SUPPORT_VERIFIER_VERSION: &str = "recall-assertion-support-v2";

/// Trusted verification receipt persisted with a staged interpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportVerification {
    fingerprint: String,
    verifier_version: String,
    model: String,
}

impl SupportVerification {
    /// Bind a successful independent support verdict to its complete input.
    /// Only trusted verifier implementations may call this; model output must
    /// first pass the complete, one-verdict-per-claim response validation.
    pub(crate) fn verified(assertion: &RawAssertion, model: &str) -> Self {
        Self {
            fingerprint: support_fingerprint(assertion, model),
            verifier_version: SUPPORT_VERIFIER_VERSION.to_string(),
            model: model.to_string(),
        }
    }

    /// Refuse a missing, obsolete, or altered support decision.
    pub fn validate(&self, assertion: &RawAssertion) -> Result<(), PipelineError> {
        if self.verifier_version != SUPPORT_VERIFIER_VERSION
            || self.model.trim().is_empty()
            || self.fingerprint != support_fingerprint(assertion, &self.model)
        {
            return Err(PipelineError::ValidationError(
                "Assertion support verification is obsolete or does not match the claim".into(),
            ));
        }
        Ok(())
    }
}

/// Complete interpretation passed to an independent verifier, including exact
/// byte coordinates. Trusted extraction code explicitly rebinds only a checked
/// node-local to revision-global offset translation; arbitrary moves invalidate
/// the receipt even if the same quote occurs elsewhere in the revision.
pub(crate) fn support_input(assertion: &RawAssertion) -> Value {
    json!({
        "revision": assertion.evidence_span.revision.id().to_string(),
        "node": assertion.evidence_span.node.as_ref().map(|node| node.id().to_string()),
        "quote_hash": assertion.evidence_span.quote_hash,
        "start": assertion.evidence_span.start,
        "end": assertion.evidence_span.end,
        "subject": assertion.subject_text,
        "predicate": assertion.predicate_text,
        "object": assertion.object_text,
        "literal": assertion.object_literal,
        "subject_type": assertion.subject_entity_type,
        "object_type": assertion.object_entity_type,
        "subject_entity": assertion.subject_entity_id,
        "object_entity": assertion.object_entity_id,
        "event_time": assertion.source_event_time,
        "event_precision": assertion.source_event_time_precision,
        "event_title": assertion.event_title,
        "event_lineage": assertion.event_lineage_key,
        "confidence": assertion.confidence,
        "significance": assertion.significance,
    })
}

fn support_fingerprint(assertion: &RawAssertion, verifier: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(SUPPORT_VERIFIER_VERSION.as_bytes());
    digest.update(
        json!({ "verifier": verifier, "claim": support_input(assertion) })
            .to_string()
            .as_bytes(),
    );
    hex::encode(digest.finalize())
}

impl RawAssertion {
    /// Translate trusted extraction coordinates, preserving only a previously
    /// valid receipt. The caller supplies the actual selected revision offset.
    pub(crate) fn translate_evidence_offset(
        &mut self,
        offset: u32,
        revision_len: usize,
    ) -> Result<(), PipelineError> {
        let receipt = self.support_verification.clone();
        if let Some(receipt) = &receipt {
            receipt.validate(self)?;
        }
        let start = self
            .evidence_span
            .start
            .checked_add(offset)
            .ok_or_else(|| PipelineError::InvalidInput("Evidence start overflow".into()))?;
        let end = self
            .evidence_span
            .end
            .checked_add(offset)
            .ok_or_else(|| PipelineError::InvalidInput("Evidence end overflow".into()))?;
        if end as usize > revision_len {
            return Err(PipelineError::InvalidInput(
                "Evidence extends beyond immutable revision".into(),
            ));
        }
        self.evidence_span.start = start;
        self.evidence_span.end = end;
        if let Some(receipt) = receipt {
            self.support_verification = Some(SupportVerification::verified(self, &receipt.model));
        }
        Ok(())
    }

    /// Require an independent support decision before canonical promotion.
    pub fn validate_support(&self) -> Result<(), PipelineError> {
        self.support_verification
            .as_ref()
            .ok_or_else(|| {
                PipelineError::ValidationError(
                    "Unsupported interpretation cannot be promoted".into(),
                )
            })?
            .validate(self)
    }
}

/// One claim and its exact quote, submitted to a trusted independent verifier.
#[derive(Debug, Clone, Serialize)]
pub struct SupportCandidate {
    /// Original position, retained when invalid evidence is excluded.
    pub index: usize,
    /// Full semantic interpretation, excluding extractor instructions.
    pub claim: Value,
    /// Exact immutable text slice; untrusted data, never instructions.
    pub quote: String,
    /// Truth value claimed for the predicate, only for a typed boolean literal.
    /// This describes the claim, not whether its evidence supports it. Preserve
    /// the predicate verbatim, including any negation already in its wording.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate_truth: Option<bool>,
}

/// Independent verifier decision for a single input candidate.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportVerdict {
    /// Original candidate position.
    pub index: usize,
    /// Whether all claimed semantics follow from the quoted evidence alone.
    pub supported: bool,
}

/// Pluggable, trusted verifier for source-supported extraction. Test verifiers
/// should encode independently authored expectations, not mirror the extractor.
#[async_trait::async_trait]
pub trait AssertionSupportVerifier: Send + Sync {
    /// Stable model or deterministic verifier identity for the support receipt.
    fn identity(&self) -> &str;
    /// Evaluate every supplied candidate, without external corpus knowledge.
    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError>;
}

/// Validate evidence, independently verify support, and bind successful decisions.
/// Unsupported candidates remain available in the staged artifact with no receipt.
/// Verifier failure is an error, never implicit approval.
pub async fn verify_assertion_support(
    assertions: &mut [RawAssertion],
    text: &str,
    verifier: &dyn AssertionSupportVerifier,
) -> Result<(), PipelineError> {
    let candidates = support_candidates(assertions, text);
    if candidates.is_empty() {
        return Ok(());
    }
    let verdicts = verifier.verify(&candidates).await?;
    apply_support_verdicts(assertions, &candidates, &verdicts, verifier.identity())
}

pub(crate) fn support_candidates(
    assertions: &mut [RawAssertion],
    text: &str,
) -> Vec<SupportCandidate> {
    assertions
        .iter_mut()
        .enumerate()
        .filter_map(|(index, assertion)| {
            // Reverification never retains a previous receipt on failure.
            assertion.support_verification = None;
            super::AssertionValidator::new(true)
                .validate(assertion, u32::try_from(text.len()).ok()?)
                .ok()?;
            super::AssertionValidator::validate_evidence(assertion, text).ok()?;
            let quote = text.get(
                assertion.evidence_span.start as usize..assertion.evidence_span.end as usize,
            )?;
            Some(SupportCandidate {
                index,
                claim: support_input(assertion),
                quote: quote.to_string(),
                predicate_truth: match &assertion.object_literal {
                    Some(elephant_types::records::attribute::Literal::Boolean(value)) => {
                        Some(*value)
                    }
                    _ => None,
                },
            })
        })
        .collect()
}

pub(crate) fn apply_support_verdicts(
    assertions: &mut [RawAssertion],
    candidates: &[SupportCandidate],
    verdicts: &[SupportVerdict],
    model: &str,
) -> Result<(), PipelineError> {
    let expected: std::collections::BTreeSet<_> =
        candidates.iter().map(|candidate| candidate.index).collect();
    let returned: std::collections::BTreeSet<_> =
        verdicts.iter().map(|verdict| verdict.index).collect();
    if model.trim().is_empty() || verdicts.len() != expected.len() || returned != expected {
        return Err(PipelineError::ValidationError(
            "Support verifier must return exactly one decision for every candidate".into(),
        ));
    }
    for verdict in verdicts {
        let assertion = assertions.get_mut(verdict.index).ok_or_else(|| {
            PipelineError::ValidationError("Support verifier returned an invalid index".into())
        })?;
        if verdict.supported {
            assertion.support_verification = Some(SupportVerification::verified(assertion, model));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::extraction::AssertionValidator;
    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::records::attribute::Literal;
    use elephant_types::{Link, RecordId, TableName};

    const QUOTE: &str = "The pump is not running.";

    fn boolean_claim() -> RawAssertion {
        RawAssertion {
            support_verification: None,
            subject_text: "The pump".into(),
            predicate_text: "is_running".into(),
            object_text: None,
            object_literal: Some(Literal::Boolean(false)),
            evidence_span: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: Some(Link::new(RecordId::new_v4(TableName::DocNode))),
                start: 0,
                end: u32::try_from(QUOTE.len()).unwrap(),
                quote_hash: AssertionValidator::hash_quote(QUOTE),
            },
            confidence: 0.9,
            quote: Some(QUOTE.into()),
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
    fn boolean_support_descriptor_distinguishes_typed_false_from_missing_and_nonboolean() {
        // Candidate construction does not infer support from any of these
        // values; only the independently returned verdict may issue a receipt.
        let cases = [
            (Some(Literal::Boolean(false)), Some(false)),
            (Some(Literal::Boolean(true)), Some(true)),
            (None, None),
            (Some(Literal::String("false".into())), None),
            (Some(Literal::Integer(0)), None),
            (Some(Literal::Float(0.0)), None),
            (Some(Literal::Json(Value::Null)), None),
            (Some(Literal::Json(Value::Bool(false))), None),
        ];
        for (literal, expected) in cases {
            let mut raw = boolean_claim();
            raw.object_literal = literal;
            let original = support_input(&raw);
            let candidates = support_candidates(std::slice::from_mut(&mut raw), QUOTE);
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].predicate_truth, expected);
            assert_eq!(candidates[0].claim, original);
            assert_eq!(candidates[0].quote, QUOTE);
            let wire = serde_json::to_value(&candidates[0]).unwrap();
            assert_eq!(
                wire.get("predicate_truth"),
                expected.map(Value::Bool).as_ref()
            );
            assert!(raw.validate_support().is_err());
        }
    }

    #[test]
    fn boolean_support_descriptor_preserves_a_negated_predicate_verbatim() {
        let mut raw = boolean_claim();
        raw.predicate_text = "is_not_running".into();
        let candidates = support_candidates(std::slice::from_mut(&mut raw), QUOTE);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].predicate_truth, Some(false));
        assert_eq!(candidates[0].claim["predicate"], "is_not_running");
        assert_eq!(
            candidates[0].claim["literal"],
            json!({"type":"boolean","value":false})
        );
        // The descriptor never strips predicate negation or infers that this
        // double-negative proposition is supported by a negative quotation.
        assert!(raw.validate_support().is_err());
    }

    #[test]
    fn boolean_support_negative_verdict_remains_authoritative() {
        let denied = boolean_claim();
        let mut affirmed = denied.clone();
        affirmed.object_literal = Some(Literal::Boolean(true));
        let mut assertions = [affirmed, denied];
        for assertion in &mut assertions {
            assertion.support_verification =
                Some(SupportVerification::verified(assertion, "old-fixture"));
        }
        let candidates = support_candidates(&mut assertions, QUOTE);
        assert_eq!(candidates.len(), 2);
        assert!(assertions
            .iter()
            .all(|raw| raw.support_verification.is_none()));
        // Fixed, independently authored fixture decisions for an explicitly
        // negative quotation. Opposite literals and reversed response order
        // cannot confuse claim truth with support truth or candidate identity.
        apply_support_verdicts(
            &mut assertions,
            &candidates,
            &[
                SupportVerdict {
                    index: 1,
                    supported: true,
                },
                SupportVerdict {
                    index: 0,
                    supported: false,
                },
            ],
            "pump-fixture-v1",
        )
        .unwrap();
        assert!(assertions[0].validate_support().is_err());
        assert!(assertions[1].validate_support().is_ok());

        // Even an explicit denial and an earlier successful receipt cannot
        // override a later negative support decision for the false claim.
        let raw = &mut assertions[1];
        let candidates = support_candidates(std::slice::from_mut(raw), QUOTE);
        apply_support_verdicts(
            std::slice::from_mut(raw),
            &candidates,
            &[SupportVerdict {
                index: 0,
                supported: false,
            }],
            "pump-fixture-v2",
        )
        .unwrap();
        assert!(raw.support_verification.is_none());
        assert!(raw.validate_support().is_err());
    }

    #[test]
    fn boolean_support_receipt_rejects_changed_truth_type_predicate_and_revision() {
        let raw = boolean_claim();
        let receipt = SupportVerification::verified(&raw, "pump-fixture-v1");
        assert!(receipt.validate(&raw).is_ok());
        for literal in [Literal::Boolean(true), Literal::String("false".into())] {
            let mut changed = raw.clone();
            changed.object_literal = Some(literal);
            assert!(receipt.validate(&changed).is_err());
        }
        let mut changed = raw.clone();
        changed.predicate_text = "is_not_running".into();
        assert!(receipt.validate(&changed).is_err());
        let mut changed = raw.clone();
        changed.evidence_span.revision = Link::new(RecordId::new_v4(TableName::DocRevision));
        assert!(receipt.validate(&changed).is_err());
        assert_ne!(
            receipt,
            SupportVerification::verified(&raw, "pump-fixture-v2")
        );
    }

    #[test]
    fn boolean_support_descriptor_never_bypasses_exact_quote_validation() {
        let mut raw = boolean_claim();
        raw.support_verification = Some(SupportVerification::verified(&raw, "pump-fixture-v1"));
        raw.evidence_span.quote_hash = AssertionValidator::hash_quote("The pump is running.");
        assert!(support_candidates(std::slice::from_mut(&mut raw), QUOTE).is_empty());
        assert!(raw.support_verification.is_none());
        assert!(raw.validate_support().is_err());
    }
}
