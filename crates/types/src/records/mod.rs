//! Record types for all Elephant tables.
//!
//! This module provides both DTO (Data Transfer Object) types for the API layer
//! and Record types for the database layer. The separation allows for:
//! - Different validation rules at each layer
//! - Schema evolution without breaking the API
//! - Clear ownership of serialization concerns

// Base types
pub mod base;
pub mod entity;
pub mod space;

// Core document/source types
pub mod doc;
pub mod media_asset;
pub mod source;
pub mod source_watch;

// Identity types
pub mod identity;

// System types
pub mod system;

// v0.8 types - document processing
pub mod doc_node;
pub mod doc_revision;

// v0.8 types - assertion extraction
pub mod assertion;
pub mod temporal;

// v0.8 types - predicate governance
pub mod predicate;
pub mod security_view;

// v0.8 types - resolved claims
pub mod attribute;
pub mod event;
pub mod rel;

// v0.8 types - truth maintenance
pub mod truth;

// v0.8 types - work scheduler
pub mod work;

// v0.8 types - ingestion and cost
pub mod ingest;

// v0.8 types - freshness and reprocessing
pub mod freshness;
pub mod knowledge_view;
pub mod reprocess;

// Re-exports: base types
pub use base::{BaseFields, Provenance, Security, SubjectsState};
pub use entity::{Entity, EntityStatus, EntityType, Handle};
pub use space::SpaceConfig;

// Re-exports: document/source types
pub use doc::{
    Doc, DocMediaAttachment, DocMediaInput, DocMediaKind, DocMediaLocation, NodeEmbedding,
};
pub use media_asset::MediaAsset;
pub use source::{
    apply_manual_doc_to_source, deterministic_source_ingest_id, manual_doc_template_from_doc,
    prepare_source_for_reingest, prepare_source_for_reingest_preserving_acl_if_placeholder,
    validate_explicit_source_doc_space_match, validate_manual_ingest_mode,
    validate_simple_source_fields, ManualDocTemplate, Source, SourceLifecycleStatus, SourceType,
    SOURCE_MANUAL_DOC_TEMPLATE_KEY, SOURCE_REQUESTED_INGEST_MODE_KEY,
};
pub use source_watch::{SourceWatch, SourceWatchCheck, SourceWatchOutcome, SourceWatchStatus};

// Re-exports: identity types
pub use identity::{
    EntityIdentityKey, EntityMerge, IdentityCandidate, IdentityCandidateStatus, IdentityKeyStatus,
    MergeStrategy,
};

// Re-exports: system types
pub use system::{AuditAction, AuditLog, IdMap, RelEvidence, SystemEvent};

// Re-exports: v0.8 document processing
pub use doc_node::{DocNode, NodeRole};
pub use doc_revision::{
    DocRevision, RenderManifest, RenderManifestAttachment, RenderManifestSpanBinding,
    RenderManifestTimeRange, RevisionStatus,
};

// Re-exports: v0.8 assertion extraction
pub use assertion::{Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus};
pub use temporal::{
    ImplicitNowPolicy, RecencyBasis, RefinementPolicy, TemporalAudit, TemporalPersonality,
    TimeRefinement,
};

// Re-exports: v0.8 predicate governance
pub use predicate::{
    AliasScope, AliasStatus, AliasThresholds, AliasType, PartialResolutionPolicy, PredicateAlias,
    PredicateRegistry, PredicateSemantics, SemanticsStatus,
};
pub use security_view::HasSecurity;

// Re-exports: v0.8 resolved claims
pub use attribute::Attribute;
pub use event::{Event, EventStatus, TimePrecision};
pub use rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis, ValidityWindow};

// Re-exports: v0.8 truth maintenance
pub use truth::{ConflictGroup, ConflictPolicy, CurrentValue, TruthSlot};

// Re-exports: v0.8 work scheduler
pub use work::{
    ArtifactStatus, Determinism, PluginManifest, RetryPolicy, WorkArtifact, WorkDeadletter,
    WorkEdge, WorkItem, WorkItemArchive, WorkItemStatus, WorkRun, WorkRunArchive, WorkRunMetrics,
};

// Re-exports: v0.8 ingestion and cost
pub use attribute::Literal;
pub use ingest::{Budget, CostLedgerDaily, DowngradePolicy, IngestDecision, IngestMode};

// Re-exports: v0.8 freshness and reprocessing
pub use freshness::{DecayModel, Freshness, FreshnessFactors};
pub use knowledge_view::{
    CitationKind, CompilerReceipt, KnowledgeCitation, KnowledgeDependency,
    KnowledgeQueryAttributeFilter, KnowledgeStatement, KnowledgeView, KnowledgeViewBlock,
    KnowledgeViewDefinition, KnowledgeViewDependencyIndex, KnowledgeViewKind,
    KnowledgeViewNavigation, KnowledgeViewPublicationStatus, KnowledgeViewRevision,
    KnowledgeViewSelector, KnowledgeViewSurface, ProjectionFreshness, ProjectionState,
    StatementDerivation, StatementVerification, ValidationReceipt, ViewBlockKind,
};
pub use reprocess::{ReprocessJob, SelectionCriteria};
