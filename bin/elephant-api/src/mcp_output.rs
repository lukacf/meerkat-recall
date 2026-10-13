//! Bounded public MCP responses. Immutable evidence is never silently truncated.

use elephant_types::public_error::{ErrorCode, PublicError};
use serde_json::Value;

#[expect(
    clippy::redundant_pub_crate,
    reason = "Keep the explicit internal visibility boundary."
)]
pub(crate) const MAX_TOOL_RESPONSE_BYTES: usize = 262_144;

fn strip_vectors(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.retain(|name, value| {
                !(matches!(
                    name.as_str(),
                    "embedding" | "embedding_vector" | "vector" | "vectors"
                ) && value.is_array())
            });
            for child in fields.values_mut() {
                strip_vectors(child);
            }
        }
        Value::Array(items) => {
            for child in items {
                strip_vectors(child);
            }
        }
        _ => {}
    }
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "Keep the explicit internal visibility boundary."
)]
pub(crate) fn bounded_response(mut value: Value) -> Result<Value, PublicError> {
    strip_vectors(&mut value);
    let bytes = serde_json::to_vec(&value)
        .map_err(|_| PublicError::new(ErrorCode::Unavailable, "Cannot encode tool response"))?;
    if bytes.len() > MAX_TOOL_RESPONSE_BYTES {
        return Err(PublicError::new(
            ErrorCode::PayloadTooLarge,
            "Tool response exceeds 262144 bytes; reduce the requested limit or use a narrower query.",
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_vectors_are_removed_while_provenance_and_model_identity_remain() {
        let result = bounded_response(json!({"results":[{
            "embedding":[0.1, 0.2], "embedding_model":"approved-model",
            "evidence":{"quote":"A precise quote.","span":{"start":0,"end":16}}
        }]}))
        .ok();
        assert_eq!(
            result,
            Some(json!({"results":[{
                "embedding_model":"approved-model",
                "evidence":{"quote":"A precise quote.","span":{"start":0,"end":16}}
            }]}))
        );
    }

    #[test]
    fn oversized_evidence_returns_error_instead_of_partial_success() {
        let result = bounded_response(json!({"quote":"x".repeat(MAX_TOOL_RESPONSE_BYTES)}));
        assert!(matches!(
            result,
            Err(PublicError {
                error_code: ErrorCode::PayloadTooLarge,
                ..
            })
        ));
    }
}
