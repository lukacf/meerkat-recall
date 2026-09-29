
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestEnum {
    Alpha,
    Beta,
}

impl FromStr for TestEnum {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "alpha" => Ok(Self::Alpha),
            "beta" => Ok(Self::Beta),
            _ => Err("invalid".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Base {
    pub name: String,
    pub status: TestEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Derived {
    #[serde(flatten)]
    pub base: Base,
    pub extra: i32,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::mixed_attributes_style, clippy::similar_names, clippy::type_complexity, clippy::float_cmp, clippy::const_is_empty, clippy::expect_used, clippy::panic, unused_imports, dead_code, clippy::too_many_lines, clippy::cognitive_complexity)]
    use super::*;

    #[test]
    fn test_repro_flatten_enum() {
        // This simulates a deserializer that provides "enum input"
        // In a real SurrealDB scenario, this would happen when deserializing from a Value::Enumeration

        // Let's see if standard JSON (which is string-based) works.
        // If it does, then the issue is indeed with non-string-based deserializers (like SurrealDB Value).
        let json = r#"{"name": "test", "status": "alpha", "extra": 123}"#;
        let derived = serde_json::from_str::<Derived>(json).ok();
        assert!(matches!(
            derived,
            Some(derived) if derived.base.status == TestEnum::Alpha
        ));
    }
}
