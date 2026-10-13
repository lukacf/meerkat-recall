//! Decision-input identity, separate from claim membership / delivery identity.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Semantic input fingerprint. Record/evidence ordering is immaterial; literal
/// array order remains meaningful. Callers supply only the selected slot's
/// inputs, so unrelated corpus changes cannot invalidate this decision.
#[must_use]
pub fn decision_input_fingerprint(
    claims: &[Value],
    evidence: &[Value],
    predicate_semantics: &Value,
    adjudicator_version: &str,
) -> String {
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
            value => value.clone(),
        }
    }
    fn sorted(values: &[Value]) -> Vec<String> {
        let mut items: Vec<_> = values
            .iter()
            .map(|value| canonical(value).to_string())
            .collect();
        items.sort();
        items.dedup();
        items
    }
    let payload = serde_json::json!({
        "version": "recall-tm-decision-v1",
        "adjudicator": adjudicator_version,
        "claims": sorted(claims), "evidence": sorted(evidence),
        "predicate_semantics": canonical(predicate_semantics),
    });
    format!(
        "decision-v1:{}",
        hex::encode(Sha256::digest(payload.to_string().as_bytes()))
    )
}

/// Exclude bookkeeping and adjudication outputs from the next decision input.
/// The actual evidence, values, security, confidence and predicate remain.
pub(super) fn semantic_record(mut value: Value) -> Value {
    if let Some(record) = value.as_object_mut() {
        for key in [
            "updated_at",
            "status",
            "validity_window",
            "temporal_audit",
            "resolution_status",
            "resolution_method",
            "resolution_confidence",
            "retry_state",
            "lifecycle_tier",
        ] {
            record.remove(key);
        }
        for field in ["evidence", "assertion_refs", "source_refs", "subjects"] {
            if let Some(Value::Array(values)) = record.get_mut(field) {
                values.sort_by_key(Value::to_string);
                values.dedup();
            }
        }
        if let Some(Value::Object(security)) = record.get_mut("security") {
            for field in ["labels", "handling"] {
                if let Some(Value::Array(values)) = security.get_mut(field) {
                    values.sort_by_key(Value::to_string);
                    values.dedup();
                }
            }
        }
        if let Some(Value::Object(provenance)) = record.get_mut("provenance") {
            if let Some(Value::Array(values)) = provenance.get_mut("sources") {
                values.sort_by_key(Value::to_string);
                values.dedup();
            }
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn order_independent_and_delimited() {
        let a = json!({"id":"claim:a", "value":[1, 2]});
        let b = json!({"value":"ab\\nc", "id":"claim:b"});
        let evidence = [json!({"revision":"r1","quote_hash":"hash"})];
        assert_eq!(
            decision_input_fingerprint(&[a.clone(), b.clone()], &evidence, &Value::Null, "v1"),
            decision_input_fingerprint(&[b, a], &evidence, &Value::Null, "v1")
        );
    }

    #[test]
    fn semantic_mutations_invalidate_unchanged_ids() {
        let claims = [json!({"id":"claim:a","value":"engineer","confidence":0.9})];
        let evidence =
            [json!({"id":"assertion:a","revision":"r1","start":0,"end":24,"quote_hash":"h1"})];
        let baseline = decision_input_fingerprint(
            &claims,
            &evidence,
            &json!({"cardinality":"one"}),
            "model-v1",
        );
        let changes = [
            decision_input_fingerprint(
                &[json!({"id":"claim:a","value":"manager","confidence":0.9})],
                &evidence,
                &json!({"cardinality":"one"}),
                "model-v1",
            ),
            decision_input_fingerprint(
                &claims,
                &[json!({"id":"assertion:a","revision":"r2","start":0,"end":24,"quote_hash":"h2"})],
                &json!({"cardinality":"one"}),
                "model-v1",
            ),
            decision_input_fingerprint(
                &claims,
                &evidence,
                &json!({"cardinality":"many"}),
                "model-v1",
            ),
            decision_input_fingerprint(
                &claims,
                &evidence,
                &json!({"cardinality":"one"}),
                "model-v2",
            ),
        ];
        assert!(changes.iter().all(|changed| changed != &baseline));
        // Literal array order is semantic, unlike evidence-set ordering.
        assert_ne!(
            decision_input_fingerprint(&[json!({"value":[1,2]})], &[], &Value::Null, "v1"),
            decision_input_fingerprint(&[json!({"value":[2,1]})], &[], &Value::Null, "v1")
        );
    }

    #[test]
    fn unrelated_corpus_and_bookkeeping_are_not_inputs() {
        let first = semantic_record(
            json!({"id":"claim:a","value":"blue","updated_at":"old","status":"candidate"}),
        );
        let after = semantic_record(
            json!({"id":"claim:a","value":"blue","updated_at":"new","status":"approved"}),
        );
        let unrelated_corpus = json!({"id":"claim:unrelated","value":"red"});
        assert_ne!(first, unrelated_corpus);
        assert_eq!(
            decision_input_fingerprint(&[first], &[], &Value::Null, "v1"),
            decision_input_fingerprint(&[after], &[], &Value::Null, "v1")
        );
    }
}
