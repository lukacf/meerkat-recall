//! Identity resolution and entity merging for Elephant.
//!
//! This crate provides identity resolution features:
//! - Identity key management
//! - Identity candidate management
//! - Merge strategy execution
//! - Entity merge lineage tracking
//! - Reference rewriting during merges

pub mod candidate;
pub mod error;
pub mod keys;
pub mod merge;
pub mod resolver;

pub use candidate::{CandidateStatus, IdentityCandidate, IdentitySignals};
pub use error::{IdentityError, IdentityResult};
pub use keys::{IdentityKey, KeyStatus};
pub use merge::{MergeLineage, MergeResult, MergeStrategy};
pub use resolver::{
    apply_entity_merge_rewrites, generate_id_map_entry, generate_merge_tombstone,
    generate_rewrite_query, prepare_entity_merge, prepare_entity_merge_on_plan,
    rewrite_declared_entity_refs_in_json, rewrite_extension_rows_in_mutation, validate_merge,
    MergeRequest, OnMergeBehavior, ReferenceCardinality, ReferenceDeclaration, ReferenceRegistry,
    CORE_ENTITY_REF_TABLES,
};

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
    fn test_identity_error_display() {
        let err = IdentityError::EntityNotFound("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
