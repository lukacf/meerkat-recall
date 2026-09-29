//! Parameter coercion utilities for MCP tool execution.

use serde_json::Value;

/// Coerce MCP tool parameters to match their expected schema types.
///
/// Handles common type mismatches from LLM-generated tool calls, such as
/// string-encoded integers and booleans.
#[must_use]
pub fn coerce_params(params: Value, schema: &Value) -> Value {
    coerce_value(params, schema)
}

fn coerce_value(value: Value, schema: &Value) -> Value {
    match value {
        Value::Object(map) => coerce_object(map, schema),
        Value::Array(values) => coerce_array(values, schema),
        Value::String(s) => coerce_string(s, schema),
        other => other,
    }
}

fn coerce_object(map: serde_json::Map<String, Value>, schema: &Value) -> Value {
    let properties = schema.get("properties").and_then(Value::as_object);
    Value::Object(
        map.into_iter()
            .map(|(key, value)| {
                let coerced = match properties.and_then(|props| props.get(&key)) {
                    Some(prop_schema) => coerce_value(value, prop_schema),
                    None => value,
                };
                (key, coerced)
            })
            .collect(),
    )
}

fn coerce_array(values: Vec<Value>, schema: &Value) -> Value {
    let items_schema = schema.get("items");
    Value::Array(
        values
            .into_iter()
            .map(|value| match items_schema {
                Some(item_schema) => coerce_value(value, item_schema),
                None => value,
            })
            .collect(),
    )
}

fn coerce_string(s: String, schema: &Value) -> Value {
    if schema_expects_datetime(schema) {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&s) {
            return Value::String(parsed.with_timezone(&chrono::Utc).to_rfc3339());
        }
    }

    if schema_expects_bool(schema) {
        match s.as_str() {
            "true" => return Value::Bool(true),
            "false" => return Value::Bool(false),
            _ => {}
        }
    }

    if schema_expects_integer(schema) {
        if let Ok(parsed) = s.parse::<u64>() {
            return Value::Number(serde_json::Number::from(parsed));
        }
    }

    Value::String(s)
}

fn schema_expects_datetime(schema: &Value) -> bool {
    schema.get("format").and_then(Value::as_str) == Some("date-time")
}

fn schema_expects_bool(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(t)) => t == "boolean",
        Some(Value::Array(types)) => types.iter().any(|t| t.as_str() == Some("boolean")),
        _ => false,
    }
}

fn schema_expects_integer(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(t)) => t == "integer",
        Some(Value::Array(types)) => types.iter().any(|t| t.as_str() == Some("integer")),
        _ => false,
    }
}

#[cfg(test)]
mod rct_contracts {
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
    fn test_coerce_string_to_usize() {
        let params = serde_json::json!({"limit": "10"});
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "limit": { "type": "integer" }
            }
        });

        let coerced = coerce_params(params, &schema);
        assert_eq!(coerced["limit"], serde_json::json!(10));
    }

    #[test]
    fn test_coerce_string_to_bool() {
        let params = serde_json::json!({"enabled": "true"});
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "enabled": { "type": "boolean" }
            }
        });

        let coerced = coerce_params(params, &schema);
        assert_eq!(coerced["enabled"], serde_json::json!(true));
    }

    #[test]
    fn test_coerce_idempotent() {
        let params = serde_json::json!({"limit": 10, "enabled": false});
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "limit": { "type": "integer" },
                "enabled": { "type": "boolean" }
            }
        });

        let coerced = coerce_params(params.clone(), &schema);
        assert_eq!(coerced, params);
    }

    #[test]
    fn rct_contracts_coerce_iso_timestamp_to_datetime_utc() {
        let input = "2026-02-02T00:00:00-05:00";
        let params = serde_json::json!({"timestamp": input});
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "timestamp": { "type": "string", "format": "date-time" }
            }
        });

        let expected = chrono::DateTime::parse_from_rfc3339(input)
            .unwrap()
            .with_timezone(&chrono::Utc);

        let coerced = coerce_params(params, &schema);
        assert_eq!(
            coerced["timestamp"],
            serde_json::json!(expected.to_rfc3339())
        );

        let decoded: chrono::DateTime<chrono::Utc> =
            serde_json::from_value(coerced["timestamp"].clone()).unwrap();
        assert_eq!(decoded, expected);
    }
}
