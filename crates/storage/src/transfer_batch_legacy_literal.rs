//! Typed historical view of the existing native assertion literal encoding.

use crate::{StorageError, StorageResult};
use serde_json::Value;

/// Project historical storage JSON into the current typed record representation.
///
/// Only an assertion's exact native `{type,value}` literal wrapper is decoded.
/// The borrowed original remains the authority for lexical and snapshot hashes;
/// callers must retain it separately. Ordinary public writes do not use this view.
///
/// # Errors
///
/// Returns an integrity error when a recognized native wrapper's value does not
/// match its tag. Other objects and all fields outside this wrapper are unchanged.
pub fn historical_record_projection(table: &str, original: &Value) -> StorageResult<Value> {
    let mut projected = original.clone();
    if table != "assertion" {
        return Ok(projected);
    }
    let Some(literal) = projected.get_mut("object_literal") else {
        return Ok(projected);
    };
    let Some(wrapper) = literal.as_object() else {
        return Ok(projected);
    };
    if wrapper.len() != 2 {
        return Ok(projected);
    }
    let (Some(tag), Some(value)) = (
        wrapper.get("type").and_then(Value::as_str),
        wrapper.get("value"),
    ) else {
        return Ok(projected);
    };
    let matches = match tag {
        "integer" => value.is_i64() || value.is_u64(),
        "float" => value.is_f64(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "json" => value.is_array(),
        _ => return Ok(projected),
    };
    if !matches {
        return Err(StorageError::DataIntegrityError(
            "historical assertion literal wrapper type/value mismatch".into(),
        ));
    }
    let decoded = value.clone();
    *literal = decoded;
    Ok(projected)
}
