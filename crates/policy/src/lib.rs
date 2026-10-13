//! ABAC policy engine for Elephant.
//!
//! This crate implements Attribute-Based Access Control (ABAC) as specified
//! in the Elephant spec. The engine evaluates access based on:
//! - Principal claims (scopes, clearances, subject allowlists)
//! - Record security labels (`policy_id`, level, labels, handling)
//! - Subject state

pub mod engine;
pub mod error;
pub mod redaction;
pub mod registry;
pub mod update;

pub use engine::{AbacEngine, AccessDecision, Clearance, Principal, RecordAttributes};
pub use error::{PolicyError, PolicyResult};
pub use redaction::{RedactionEnvelope, RedactionRule, RedactionRules};
pub use registry::{PolicyDefinition, PolicyRegistry};
pub use update::authorize_ordinary_update;

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
    fn test_abac_engine_builder_pattern() {
        use std::collections::HashSet;

        // Test that AbacEngine can be created with builder pattern
        let mut purposes = HashSet::new();
        purposes.insert("analytics".to_string());

        let engine = AbacEngine::new().with_required_purposes(purposes);

        // Verify engine was created (more substantive tests in engine module)
        // The fact that with_required_purposes returns Self proves the builder works
        let _ = engine;
    }

    #[test]
    fn test_access_decision_is_allowed() {
        assert!(AccessDecision::Allow.is_allowed());
        assert!(!AccessDecision::Deny("test".to_string()).is_allowed());
    }
}
