//! Ingestion and processing pipeline for Elephant.
//!
//! This crate provides pipeline workers:
//! - Source normalization and dedup
//! - Document ingestion and chunking
//! - Embedding generation
//! - Fact promotion

// Allow doc_markdown for snake_case identifiers in doc comments
// Allow const_fn and must_use for new code that will be refined later
// Allow struct field postfix patterns for repository structs
// Allow various stylistic lints in new v0.8 code
#![allow(
    clippy::doc_markdown,
    clippy::pedantic,
    clippy::nursery,
    clippy::missing_const_for_fn,
    clippy::return_self_not_must_use,
    clippy::must_use_candidate,
    clippy::struct_field_names,
    clippy::unused_async,
    clippy::unused_self,
    clippy::assigning_clones,
    clippy::option_if_let_else,
    clippy::single_match_else,
    clippy::redundant_closure_for_method_calls,
    clippy::map_unwrap_or,
    clippy::needless_pass_by_value,
    clippy::unnecessary_map_or,
    clippy::redundant_closure,
    clippy::redundant_clone
)]

pub mod embed;
pub mod embedding_reindex;
pub mod entity_type_consolidation;
pub mod error;
pub mod extraction;
pub mod freshness;
#[cfg(all(test, feature = "meerkat-extraction"))]
mod governed_request_test_fixture;
#[cfg(feature = "meerkat-extraction")]
mod llm;
pub mod maintenance;
/// Atomic preparation of retained truth-maintenance and resolution effects.
pub mod maintenance_commit;
pub mod media;
pub mod outbox_delivery;
pub mod predicate;
pub mod predicate_hygiene;
pub(crate) mod provenance;
pub mod provider_dispatch;
pub mod provider_usage;
pub mod public_predicate_proposal;
pub mod remote_source;
pub mod renderer;
pub mod reprocess;
pub mod resolution;
pub mod retention;
pub mod scheduler;
pub mod source_finalization;
pub mod source_queue;
pub mod source_watch;
pub mod summarizer;
pub mod truth_maintenance;
pub mod wiki_editorial;
pub mod worker;

pub use embed::{EmbedResult, EmbedWorker};
pub use error::{PipelineError, PipelineResult};
pub use retention::{RetentionResult, RetentionWorker};
pub use worker::{WorkerConfig, WorkerStatus};

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
    fn test_pipeline_error_display() {
        let err = PipelineError::IngestionError("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
