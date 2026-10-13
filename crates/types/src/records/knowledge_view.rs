//! First-class derived knowledge projections.
//!
//! Knowledge views are compiled from Elephant's canonical evidence graph. They
//! are never evidence and must never be re-ingested as sources. A stable
//! [`KnowledgeView`] points at an immutable [`KnowledgeViewRevision`] that
//! records its complete dependency and citation receipt.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::assertion::EvidenceSpan;
use super::attribute::Literal;
use super::base::BaseFields;
use super::entity::Entity;

/// Supported knowledge-view families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeViewKind {
    /// LLM-authored, evidence-verified semantic article.
    Article,
    /// A view rooted at a canonical entity.
    Entity,
    /// A view rooted at an immutable source snapshot.
    Source,
    /// A graph-derived topic view.
    Topic,
    /// A graph-community synthesis view.
    Community,
    /// A saved query projection.
    Query,
}

/// Human surface on which a derived projection may appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeViewSurface {
    /// Curated semantic article intended for normal readers.
    Wiki,
    /// Source material and provenance intended for evidence review.
    Evidence,
    /// Internal projection intended for engineering inspection.
    Inspector,
}

/// Editorial publication state independent of compilation freshness.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeViewPublicationStatus {
    /// Compilable article candidate that is not visible in the public wiki.
    #[default]
    Draft,
    /// Explicitly eligible for its assigned human surface.
    Published,
    /// Retained definition intentionally hidden from normal browsing.
    Hidden,
}

/// Freshness and publication state of a derived view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionState {
    /// All known dependencies match the published revision.
    Current,
    /// One or more dependencies changed after publication.
    Dirty,
    /// A replacement revision is being compiled.
    Rebuilding,
    /// The revision is usable but has missing evidence, stale claims, or conflicts.
    Degraded,
    /// Compilation or validation failed and no current revision can be trusted.
    Failed,
}

/// Stable definition and current pointer for one knowledge view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeView {
    /// Shared record fields, including the materialization security envelope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Deterministic key within a space and security envelope.
    pub key: String,
    /// View family.
    pub kind: KnowledgeViewKind,
    /// Human surface assigned by the compiler's authority contract.
    pub surface: KnowledgeViewSurface,
    /// Editorial state, independent of whether the projection is fresh.
    pub publication_status: KnowledgeViewPublicationStatus,
    /// Canonical record at the root of this view.
    pub root_record: String,
    /// Human-readable title.
    pub title: String,
    /// Stable URL-safe identifier.
    pub slug: String,
    /// Hash of the exact security envelope used during compilation.
    pub security_fingerprint: String,
    /// Current immutable revision ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_revision: Option<String>,
    /// Monotonic revision number.
    #[serde(default)]
    pub revision_count: u64,
    /// Current projection state.
    pub state: ProjectionState,
    /// Why the view is dirty or degraded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    /// When the current revision was published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
}

/// Semantic type of a structured wiki block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewBlockKind {
    /// Identity and overview material.
    Overview,
    /// Current entity-to-literal claims.
    CurrentFacts,
    /// Current entity-to-entity graph claims.
    Relationships,
    /// First-class temporal events.
    Timeline,
    /// Source-to-document and immutable-revision lineage.
    SourceLineage,
    /// Deterministically selected graph members and their inclusion receipts.
    Members,
    /// Typed selection definition and execution bounds.
    SelectionReceipt,
    /// Conflicting, disputed, or stale knowledge.
    Conflicts,
    /// Coverage and validation information.
    EvidenceStatus,
}

/// Exact approved-attribute predicate in a saved knowledge query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeQueryAttributeFilter {
    /// Canonical attribute predicate.
    pub attr_type: String,
    /// Exact typed value required for a match.
    pub value: Literal,
}

/// Governed selector for a graph-derived knowledge view.
///
/// This deliberately is not raw `SurrealQL`. The bounded typed form gives the
/// compiler stable semantics, permits embedded/Surreal parity, and prevents a
/// saved view from becoming an unrestricted database execution surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KnowledgeViewSelector {
    /// Traverse approved graph relationships around one canonical topic node.
    Topic {
        /// Canonical entity representing the topic.
        topic: crate::Link<Entity>,
        /// Allowed predicates. Empty means all approved relationship types.
        #[serde(default)]
        relation_types: Vec<String>,
        /// Maximum traversal distance, in the inclusive range 1-4.
        max_hops: u8,
        /// Maximum selected entities, in the inclusive range 1-250.
        max_members: u16,
    },
    /// Traverse an approved relationship subgraph from explicit seed nodes.
    Community {
        /// Canonical seed entities. Seeds are always included.
        seeds: Vec<crate::Link<Entity>>,
        /// Allowed predicates. Empty means all approved relationship types.
        #[serde(default)]
        relation_types: Vec<String>,
        /// Maximum traversal distance, in the inclusive range 1-4.
        max_hops: u8,
        /// Maximum selected entities, in the inclusive range 1-250.
        max_members: u16,
    },
    /// Evaluate a safe, bounded entity query.
    Query {
        /// Allowed entity types. Empty means all types.
        #[serde(default)]
        entity_types: Vec<String>,
        /// Case-insensitive substring required in the display name or alias.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name_contains: Option<String>,
        /// Exact approved attributes that must all match.
        #[serde(default)]
        attributes: Vec<KnowledgeQueryAttributeFilter>,
        /// Maximum entities inspected. Reaching this bound is disclosed.
        scan_limit: u16,
        /// Maximum selected entities, in the inclusive range 1-250.
        max_members: u16,
    },
}

impl KnowledgeViewSelector {
    /// Returns the view family implied by this selector.
    #[must_use]
    pub const fn kind(&self) -> KnowledgeViewKind {
        match self {
            Self::Topic { .. } => KnowledgeViewKind::Topic,
            Self::Community { .. } => KnowledgeViewKind::Community,
            Self::Query { .. } => KnowledgeViewKind::Query,
        }
    }
}

/// Reader navigation assigned by the LLM wiki organizer.
///
/// Navigation is disposable derived metadata. It organizes published article
/// definitions without becoming a graph assertion or factual article prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeViewNavigation {
    /// Reader-facing title for this managed wiki.
    pub wiki_title: String,
    /// LLM-inferred purpose of this knowledge space.
    #[serde(default)]
    pub wiki_purpose: String,
    /// Stable organizer-local section identifier.
    pub section_id: String,
    /// Reader-facing section title.
    pub section_title: String,
    /// Section position in the wiki index.
    pub section_order: u16,
    /// Article position within its section.
    pub article_order: u16,
    /// Organizer-authored explanation of what belongs in this reader lens.
    #[serde(default)]
    pub section_description: String,
    /// Editorial importance within the whole wiki, from 1 (deep reference)
    /// to 5 (one of the defining subjects of the space).
    #[serde(default)]
    pub article_importance: u8,
    /// Editorial role used to keep overview and core pages ahead of details.
    #[serde(default)]
    pub page_role: String,
    /// Whether the page should be visible on the compact wiki front page.
    #[serde(default)]
    pub featured: bool,
}

/// Durable, security-scoped definition for a graph-derived knowledge view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeViewDefinition {
    /// Shared record fields and exact compilation security envelope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Human-readable title.
    pub title: String,
    /// Context-plan state. Definitions are drafts unless explicitly approved.
    #[serde(default)]
    pub publication_status: KnowledgeViewPublicationStatus,
    /// Canonical entities selected by the wiki planner as related articles.
    ///
    /// These are navigation targets, not security subjects and not claims.
    /// The reader resolves them only when a published article exists for the
    /// target entity in the same space and the caller may read it.
    #[serde(default)]
    pub related_articles: Vec<crate::Link<Entity>>,
    /// LLM-selected reader navigation for managed wiki articles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigation: Option<KnowledgeViewNavigation>,
    /// Typed, bounded selector evaluated by the deterministic compiler.
    pub selector: KnowledgeViewSelector,
}

/// How a rendered statement was derived from its cited evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementDerivation {
    /// A faithful rendering of one or more canonical Elephant records.
    Direct,
    /// A synthesis across multiple canonical records or evidence spans.
    Synthesized,
    /// A verbatim quotation backed by an immutable evidence span.
    VerbatimQuote,
}

/// How support for a statement was verified before publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementVerification {
    /// The compiler rendered the statement directly from typed graph fields.
    DeterministicRecordRendering,
    /// A verifier checked that cited source material entails the statement.
    EvidenceEntailment,
}

/// Smallest publishable unit of claim-bearing prose.
///
/// Wiki prose is rendered from these statements. This prevents a generator
/// from surrounding a few citations with untracked filler text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeStatement {
    /// Stable identifier within the block.
    pub id: String,
    /// Markdown-safe claim-bearing text.
    pub text: String,
    /// Whether the text is direct, synthesized, or a verbatim quotation.
    pub derivation: StatementDerivation,
    /// Verification gate that authorized publication of this statement.
    pub verification: StatementVerification,
    /// Citation IDs that support this exact statement.
    pub citations: Vec<String>,
}

/// One stable, independently citable section of a knowledge view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeViewBlock {
    /// Stable identifier within the revision.
    pub id: String,
    /// Semantic block type.
    pub kind: ViewBlockKind,
    /// Display heading.
    pub heading: String,
    /// Markdown rendering for this block.
    pub markdown: String,
    /// Claim-bearing statements from which factual Markdown is rendered.
    #[serde(default)]
    pub statements: Vec<KnowledgeStatement>,
    /// Citation IDs used by this block.
    #[serde(default)]
    pub citations: Vec<String>,
}

/// What a citation directly addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CitationKind {
    /// Exact source-document evidence span.
    EvidenceSpan,
    /// Canonical claim record without an available evidence span.
    ClaimRecord,
    /// Canonical non-claim graph record.
    GraphRecord,
}

/// Provenance pointer for a factual view statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeCitation {
    /// Stable citation ID within the revision.
    pub id: String,
    /// Citation strength/type.
    pub kind: CitationKind,
    /// Canonical record directly supporting the statement.
    pub record_id: String,
    /// Assertion that introduced the supporting evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assertion_id: Option<String>,
    /// Original source record when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// Exact immutable evidence span when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_span: Option<EvidenceSpan>,
}

/// A record read while compiling a projection revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeDependency {
    /// Canonical record ID.
    pub record_id: String,
    /// Logical table/type of the dependency.
    pub record_type: String,
    /// Role played by the record in compilation.
    pub role: String,
    /// Last observed update timestamp, if the record was materialized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_updated_at: Option<DateTime<Utc>>,
}

/// Projection freshness is independent from source and claim freshness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionFreshness {
    /// State at publication time.
    pub state: ProjectionState,
    /// Oldest claim-level freshness score observed in the view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_claim_score: Option<f32>,
    /// Number of truth slots already marked stale.
    #[serde(default)]
    pub stale_truth_slots: u32,
    /// Number of unresolved conflicts exposed by the view.
    #[serde(default)]
    pub unresolved_conflicts: u32,
    /// Number of factual claim records without source evidence.
    #[serde(default)]
    pub unsupported_claims: u32,
    /// Number of claim freshness receipts whose review deadline has passed.
    #[serde(default)]
    pub expired_claims: u32,
    /// Whether a selector hit an explicit scan or membership bound.
    #[serde(default)]
    pub selection_truncated: bool,
    /// Earliest time by which the projection should be checked again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<DateTime<Utc>>,
}

/// Deterministic validation results attached to every published revision.
#[allow(clippy::struct_excessive_bools)] // Named publication gates are clearer as independent receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReceipt {
    /// Whether every citation points at a declared dependency.
    pub citations_resolve: bool,
    /// Whether every factual block carries at least one citation.
    pub factual_blocks_cited: bool,
    /// Whether every claim-bearing statement has resolvable support.
    #[serde(default)]
    pub statements_supported: bool,
    /// Whether factual block Markdown is exactly rendered from its statements.
    #[serde(default)]
    pub prose_is_structured: bool,
    /// Number of factual blocks checked.
    pub factual_block_count: u32,
    /// Number of citations checked.
    pub citation_count: u32,
    /// Number of claim-bearing statements checked.
    #[serde(default)]
    pub statement_count: u32,
    /// Human-readable validation warnings.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Compiler identity and reproducibility metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompilerReceipt {
    /// Stable compiler implementation name.
    pub compiler: String,
    /// Compiler contract version.
    pub version: u32,
    /// Hash of the ordered projection dependencies.
    pub dependency_hash: String,
    /// Hash of the security envelope.
    pub policy_fingerprint: String,
}

/// Immutable materialization of a knowledge view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeViewRevision {
    /// Shared record fields. Security must exactly match the parent view.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Parent knowledge-view ID.
    pub view_id: String,
    /// Monotonic revision number within the view.
    pub revision: u64,
    /// View family copied into the immutable receipt.
    pub kind: KnowledgeViewKind,
    /// Root record copied into the immutable receipt.
    pub root_record: String,
    /// Structured content blocks.
    pub blocks: Vec<KnowledgeViewBlock>,
    /// Flat Markdown rendering of the structured blocks.
    pub rendered_markdown: String,
    /// Complete citation inventory.
    pub citations: Vec<KnowledgeCitation>,
    /// Complete dependency inventory.
    pub dependencies: Vec<KnowledgeDependency>,
    /// Projection freshness at publication time.
    pub freshness: ProjectionFreshness,
    /// Deterministic validation results.
    pub validation: ValidationReceipt,
    /// Reproducible compiler metadata.
    pub compiler: CompilerReceipt,
    /// Publication time.
    pub compiled_at: DateTime<Utc>,
    /// Previous immutable revision, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
}

/// Current reverse dependency edge used for indexed view invalidation.
///
/// These records are derived indexes, not evidence. Publication atomically
/// replaces the complete edge set for a view's current revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeViewDependencyIndex {
    /// Shared fields copied from the view's exact security envelope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Stable view owning this current dependency edge.
    pub view: crate::Link<KnowledgeView>,
    /// Immutable revision from which the edge was materialized.
    pub revision: crate::Link<KnowledgeViewRevision>,
    /// Canonical record ID on which the view depends.
    pub dependency_record: String,
    /// Canonical record table/type.
    pub dependency_type: String,
    /// Compiler role for this dependency.
    pub role: String,
    /// Dependency update timestamp observed during compilation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_updated_at: Option<DateTime<Utc>>,
}
