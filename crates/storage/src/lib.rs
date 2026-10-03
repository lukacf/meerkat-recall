//! `SurrealDB` storage layer for Elephant.
//!
//! This crate provides the database abstraction for Elephant, including:
//! - `SurrealDB` client wrapper with Arc for sharing across tasks
//! - Connection pooling with per-server pools and LRU session caching
//! - Transaction helpers for atomic operations
//! - Repository traits for CRUD operations
//! - Concrete repository implementations for all core types
//! - Query builders

// Allow doc_markdown for snake_case identifiers in doc comments (e.g., space_id, created_at)
#![allow(clippy::doc_markdown)]
// Workspace lints run `cargo clippy ... -D warnings`; keep this crate's API docs
// lightweight by not requiring `# Errors` / `# Panics` sections everywhere.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

// Core infrastructure
pub mod backend;
pub mod client;
pub mod embedded;
pub mod error;
pub mod pool;
pub mod repository;
/// Required-field decoding helpers for storage repositories.
pub mod require;
mod response;
/// `SurrealQL` query building and native driver bridge.
pub mod surreal;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod transaction;

// Core entity repositories
pub mod audit_log_repository;
pub mod chunk_embedding_repository;
pub mod doc_repository;
pub mod entity_identity_key_repository;
pub mod entity_merge_repository;
pub mod entity_repository;
pub mod event_repository;
pub mod fake_embedding_repair;
pub mod identity_candidate_repository;
pub mod media_asset_repository;
pub mod rel_repository;
pub mod source_ingest;
pub mod source_repository;
pub mod source_watch_repository;
pub mod space_config_repository;
pub mod system_event_repository;

// Ingestion-to-Graph repositories (formerly v08)
pub mod assertion_repository;
pub mod attribute_repository;
pub mod conflict_group_repository;
pub mod cost_ledger_daily_repository;
pub mod doc_node_repository;
pub mod doc_revision_repository;
pub mod freshness_repository;
pub mod ingest_decision_repository;
pub mod knowledge_view_definition_repository;
pub mod knowledge_view_repository;
pub mod predicate_alias_repository;
pub mod predicate_registry_repository;
pub mod reprocess_job_repository;
pub mod truth_slot_repository;
pub mod work_artifact_repository;
pub mod work_deadletter_repository;
pub mod work_edge_repository;
pub mod work_item_repository;
pub mod work_retention;
pub mod work_run_repository;

// RCT invariant tests
#[cfg(test)]
mod rct_contracts;
#[cfg(test)]
mod rct_invariant_tests;

// Core re-exports
pub use audit_log_repository::AuditLogRepository;
pub use backend::{MockStorageBackend, StorageBackend, SurrealStorageBackend};
pub use chunk_embedding_repository::NodeEmbeddingRepository;
pub use chunk_embedding_repository::NodeEmbeddingVectorHit;
pub use chunk_embedding_repository::{semantic_window_settled, PartitionWindow};
pub use client::{StorageBackendKind, StorageClient, StorageConfig};
pub use doc_repository::DocRepository;
pub use embedded::{
    cosine_similarity, hashed_text_embedding, lexical_score, EmbeddedMutation, EmbeddedRecord,
    EmbeddedStore,
};
pub use entity_identity_key_repository::EntityIdentityKeyRepository;
pub use entity_merge_repository::EntityMergeRepository;
pub use entity_repository::{
    is_source_inventory_entity, EntityEmbeddingCandidate, EntityRepository, EntitySearchHit,
    EntityWithRelations,
};
pub use error::{StorageError, StorageResult};
pub use event_repository::{EventRepository, EventWindowQuery};
pub use identity_candidate_repository::IdentityCandidateRepository;
pub use media_asset_repository::MediaAssetRepository;
pub use pool::{ConnectionPool, PoolConfig, PoolStats, ServerStats, SessionKey};
pub use rel_repository::{GraphNeighbor, RelRepository};
pub use repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
pub use source_ingest::{
    deterministic_source_id_for_ingest, find_existing_doc_for_doc, find_existing_source_for_doc,
    find_existing_source_for_source, persist_manual_doc_for_source,
    persist_manual_doc_for_source_detailed, preview_persisted_manual_doc_for_source,
    rollback_persisted_manual_doc, rollback_persisted_source, update_existing_source_for_reingest,
    PersistedManualDoc,
};
pub use source_repository::SourceRepository;
pub use source_watch_repository::SourceWatchRepository;
pub use space_config_repository::SpaceConfigRepository;
pub use system_event_repository::SystemEventRepository;
pub use transaction::{execute_atomic, TransactionBuilder, TransactionExt};

#[cfg(any(test, feature = "test-utils"))]
pub use test_utils::{
    MockAssertionRepository, MockAuditLogRepository, MockDocRepository, MockEntityRepository,
    MockEventRepository, MockIdentityCandidateRepository, MockPredicateAliasRepository,
    MockPredicateRegistryRepository, MockRelRepository, MockSourceRepository,
    MockSystemEventRepository, MockWorkItemRepository,
};

// Ingestion-to-Graph re-exports (formerly v08)
pub use assertion_repository::AssertionRepository;
pub use attribute_repository::AttributeRepository;
pub use conflict_group_repository::ConflictGroupRepository;
pub use cost_ledger_daily_repository::CostLedgerDailyRepository;
pub use doc_node_repository::DocNodeRepository;
pub use doc_revision_repository::DocRevisionRepository;
pub use freshness_repository::FreshnessRepository;
pub use ingest_decision_repository::IngestDecisionRepository;
pub use knowledge_view_definition_repository::KnowledgeViewDefinitionRepository;
pub use knowledge_view_repository::KnowledgeViewRepository;
pub use predicate_alias_repository::PredicateAliasRepository;
pub use predicate_registry_repository::PredicateRegistryRepository;
pub use reprocess_job_repository::ReprocessJobRepository;
pub use truth_slot_repository::TruthSlotRepository;
pub use work_artifact_repository::WorkArtifactRepository;
pub use work_deadletter_repository::WorkDeadletterRepository;
pub use work_edge_repository::WorkEdgeRepository;
pub use work_item_repository::WorkItemRepository;
pub use work_run_repository::WorkRunRepository;

/// Helper to parse a record ID from a string, with a default table if the prefix is missing.
///
/// # Errors
///
/// Returns an error if the ID is not a valid `RecordId` or UUID string.
pub fn parse_id(
    id: &str,
    default_table: elephant_types::ids::TableName,
) -> StorageResult<elephant_types::RecordId> {
    use std::str::FromStr;
    if id.contains(':') {
        elephant_types::RecordId::from_str(id).map_err(|e| StorageError::QueryError(e.to_string()))
    } else {
        let uuid = uuid::Uuid::parse_str(id)
            .map_err(|e| StorageError::QueryError(format!("Invalid UUID: {e}")))?;
        Ok(elephant_types::RecordId::new(default_table, uuid))
    }
}

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
    fn test_storage_error_display() {
        let err = StorageError::ConnectionError("test".to_string());
        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert_eq!(config.url, "mem://");
        assert_eq!(config.namespace, "test");
        assert_eq!(config.database, "test");
    }
}
