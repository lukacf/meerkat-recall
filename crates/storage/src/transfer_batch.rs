//! Bounded typed fresh-target transfer. No queue admission or generic JSON import.
//!
//! Uses the same native converters and typed projections as normal repositories.
//! A full native after-image is checked inside the transaction, then typed
//! readback is compared without lossy native-to-JSON conversion. Errors are
//! never retried: the enclosing transfer marker keeps the destination unready.

use crate::assertion_repository::AssertionDb;
use crate::attribute_repository::AttributeDb;
use crate::audit_log_repository::AuditLogDb;
use crate::conflict_group_repository::ConflictGroupDb;
use crate::cost_ledger_daily_repository::CostLedgerDailyDb;
use crate::doc_node_repository::DocNodeDb;
use crate::doc_repository::DocDb;
use crate::doc_revision_repository::DocRevisionDb;
use crate::entity_identity_key_repository::EntityIdentityKeyDb;
use crate::entity_merge_repository::EntityMergeDb;
use crate::entity_repository::EntityDb;
use crate::event_repository::EventDb;
use crate::freshness_repository::FreshnessDb;
use crate::identity_candidate_repository::IdentityCandidateDb;
use crate::ingest_decision_repository::IngestDecisionDb;
use crate::media_asset_repository::MediaAssetDb;
use crate::predicate_alias_repository::PredicateAliasDb;
use crate::predicate_registry_repository::PredicateRegistryDb;
use crate::rel_repository::RelDb;
use crate::reprocess_job_repository::ReprocessJobDb;
use crate::source_repository::SourceDb;
use crate::system_event_repository::SystemEventDb;
use crate::truth_slot_repository::TruthSlotDb;
use crate::work_artifact_repository::WorkArtifactDb;
use crate::work_deadletter_repository::WorkDeadletterDb;
use crate::work_edge_repository::WorkEdgeDb;
use crate::work_item_repository::WorkItemDb;
use crate::work_run_repository::WorkRunDb;
use crate::{StorageClient, StorageError, StorageResult};
use elephant_types::records::{
    Assertion, Attribute, AuditLog, BaseFields, ConflictGroup, CostLedgerDaily, Doc, DocNode,
    DocRevision, Entity, EntityIdentityKey, EntityMerge, Event, Freshness, IdentityCandidate,
    IngestDecision, MediaAsset, NodeEmbedding, PredicateAlias, PredicateRegistry, Rel,
    ReprocessJob, Source, SystemEvent, TruthSlot, WorkArtifact, WorkDeadletter, WorkEdge, WorkItem,
    WorkItemArchive, WorkRun, WorkRunArchive,
};
use elephant_types::RecordId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use surrealdb::sql::{Object, Value};

#[path = "transfer_batch_legacy_literal.rs"]
mod legacy_literal;
pub use legacy_literal::historical_record_projection;

/// Maximum rows in one transaction, independent of caller configuration.
pub const MAX_BATCH_ROWS: usize = 256;
/// Maximum encoded payload per transaction (both original and native forms).
pub const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
/// Maximum single original lexical row accepted by the shared planner.
pub const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;

fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

/// Exactly one supported typed payload; control and opaque rows are excluded.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "table", content = "record", rename_all = "snake_case")]
pub enum TransferRecord {
    /// Original audit history.
    AuditLog(AuditLog),
    /// Original daily cost accounting.
    CostLedgerDaily(CostLedgerDaily),
    /// Original document content and metadata.
    Doc(Doc),
    /// Immutable original rendered revision.
    DocRevision(DocRevision),
    /// Original structural node and evidence offsets.
    DocNode(DocNode),
    /// Original managed media record and payload.
    MediaAsset(MediaAsset),
    /// Original entity record, including retained metadata.
    Entity(Entity),
    /// Original source record.
    Source(Source),
    /// Original entity identity-key association.
    EntityIdentityKey(EntityIdentityKey),
    /// Original identity-resolution candidate.
    IdentityCandidate(IdentityCandidate),
    /// Original entity-merge history.
    EntityMerge(EntityMerge),
    /// Original predicate definition.
    PredicateRegistry(PredicateRegistry),
    /// Original predicate alias mapping.
    PredicateAlias(PredicateAlias),
    /// Original claim and evidence.
    Assertion(Assertion),
    /// Original resolved attribute.
    Attribute(Attribute),
    /// Original graph relationship and endpoints.
    Rel(Rel),
    /// Original conflict grouping.
    ConflictGroup(ConflictGroup),
    /// Original truth-maintenance state.
    TruthSlot(TruthSlot),
    /// Original timeline event.
    Event(Event),
    /// Original freshness tracking.
    Freshness(Freshness),
    /// Original reprocessing history.
    ReprocessJob(ReprocessJob),
    /// Original hot work row, requiring a verified historical hold.
    WorkItem(WorkItem),
    /// Original work dependency edge.
    WorkEdge(WorkEdge),
    /// Original execution history, without synthesized dispatch authority.
    WorkRun(WorkRun),
    /// Original staged work result.
    WorkArtifact(WorkArtifact),
    /// Original terminal failed-work history.
    WorkDeadletter(WorkDeadletter),
    /// Original outbox row, retained without replaying historical effects.
    SystemEvent(SystemEvent),
    /// Original ingestion decision.
    IngestDecision(IngestDecision),
    /// Complete cold work row and its original public identity.
    WorkItemArchive(WorkItemArchive),
    /// Complete cold execution history and its original public identity.
    WorkRunArchive(WorkRunArchive),
}

impl TransferRecord {
    /// Parse historical input and prove its exact typed compatibility projection.
    /// Original lexical bytes remain external evidence; no transaction is opened.
    pub fn from_json(table: &str, raw: &str) -> StorageResult<Self> {
        if raw.len() > MAX_ROW_BYTES {
            return Err(invalid("transfer row exceeds byte bound"));
        }
        // Keep lexical source authority outside this typed compatibility view.
        let original = historical_record_projection(table, &strict_json(raw)?)?;
        let record = match table {
            "audit_log" => Self::AuditLog(decode_original(&original)?),
            "cost_ledger_daily" => Self::CostLedgerDaily(decode_original(&original)?),
            "doc" => Self::Doc(decode_original(&original)?),
            "doc_revision" => Self::DocRevision(decode_original(&original)?),
            "doc_node" => Self::DocNode(decode_original(&original)?),
            "media_asset" => Self::MediaAsset(decode_original(&original)?),
            "entity" => Self::Entity(decode_original(&original)?),
            "source" => Self::Source(decode_original(&original)?),
            "entity_identity_key" => Self::EntityIdentityKey(decode_original(&original)?),
            "identity_candidate" => Self::IdentityCandidate(decode_original(&original)?),
            "entity_merge" => Self::EntityMerge(decode_original(&original)?),
            "predicate_registry" => Self::PredicateRegistry(decode_original(&original)?),
            "predicate_alias" => Self::PredicateAlias(decode_original(&original)?),
            "assertion" => Self::Assertion(decode_original(&original)?),
            "attribute" => Self::Attribute(decode_original(&original)?),
            "rel" => Self::Rel(decode_original(&original)?),
            "conflict_group" => Self::ConflictGroup(decode_original(&original)?),
            "truth_slot" => Self::TruthSlot(decode_original(&original)?),
            "event" => Self::Event(decode_original(&original)?),
            "freshness" => Self::Freshness(decode_original(&original)?),
            "reprocess_job" => Self::ReprocessJob(decode_original(&original)?),
            "work_item" => Self::WorkItem(decode_original(&original)?),
            "work_edge" => Self::WorkEdge(decode_original(&original)?),
            "work_run" => Self::WorkRun(decode_original(&original)?),
            "work_artifact" => Self::WorkArtifact(decode_original(&original)?),
            "work_deadletter" => Self::WorkDeadletter(decode_original(&original)?),
            "system_event" => Self::SystemEvent(decode_original(&original)?),
            "ingest_decision" => Self::IngestDecision(decode_original(&original)?),
            "work_item_archive" => Self::WorkItemArchive(decode_original(&original)?),
            "work_run_archive" => Self::WorkRunArchive(decode_original(&original)?),
            _ => return Err(invalid("table requires a distinct explicit transfer path")),
        };
        validate_original_representation(table, &original, &record.json()?)?;
        if record.base().id.table().as_str() != table || record.base().space_id.is_empty() {
            return Err(invalid("transfer record identity/space mismatch"));
        }
        // Reuse strict native conversion before target admission as well.
        record.native()?;
        Ok(record)
    }

    /// Exact storage table for the typed variant.
    #[must_use]
    pub const fn table(&self) -> &'static str {
        match self {
            Self::AuditLog(_) => "audit_log",
            Self::CostLedgerDaily(_) => "cost_ledger_daily",
            Self::Doc(_) => "doc",
            Self::DocRevision(_) => "doc_revision",
            Self::DocNode(_) => "doc_node",
            Self::MediaAsset(_) => "media_asset",
            Self::Entity(_) => "entity",
            Self::Source(_) => "source",
            Self::EntityIdentityKey(_) => "entity_identity_key",
            Self::IdentityCandidate(_) => "identity_candidate",
            Self::EntityMerge(_) => "entity_merge",
            Self::PredicateRegistry(_) => "predicate_registry",
            Self::PredicateAlias(_) => "predicate_alias",
            Self::Assertion(_) => "assertion",
            Self::Attribute(_) => "attribute",
            Self::Rel(_) => "rel",
            Self::ConflictGroup(_) => "conflict_group",
            Self::TruthSlot(_) => "truth_slot",
            Self::Event(_) => "event",
            Self::Freshness(_) => "freshness",
            Self::ReprocessJob(_) => "reprocess_job",
            Self::WorkItem(_) => "work_item",
            Self::WorkEdge(_) => "work_edge",
            Self::WorkRun(_) => "work_run",
            Self::WorkArtifact(_) => "work_artifact",
            Self::WorkDeadletter(_) => "work_deadletter",
            Self::SystemEvent(_) => "system_event",
            Self::IngestDecision(_) => "ingest_decision",
            Self::WorkItemArchive(_) => "work_item_archive",
            Self::WorkRunArchive(_) => "work_run_archive",
        }
    }

    /// Original typed identity (archive identities remain in their cold tables).
    #[must_use]
    pub const fn id(&self) -> &RecordId {
        &self.base().id
    }

    /// Borrow the unchanged common fields of the original typed record.
    #[must_use]
    pub const fn base(&self) -> &BaseFields {
        match self {
            Self::AuditLog(r) => &r.base,
            Self::CostLedgerDaily(r) => &r.base,
            Self::Doc(r) => &r.base,
            Self::DocRevision(r) => &r.base,
            Self::DocNode(r) => &r.base,
            Self::MediaAsset(r) => &r.base,
            Self::Entity(r) => &r.base,
            Self::Source(r) => &r.base,
            Self::EntityIdentityKey(r) => &r.base,
            Self::IdentityCandidate(r) => &r.base,
            Self::EntityMerge(r) => &r.base,
            Self::PredicateRegistry(r) => &r.base,
            Self::PredicateAlias(r) => &r.base,
            Self::Assertion(r) => &r.base,
            Self::Attribute(r) => &r.base,
            Self::Rel(r) => &r.base,
            Self::ConflictGroup(r) => &r.base,
            Self::TruthSlot(r) => &r.base,
            Self::Event(r) => &r.base,
            Self::Freshness(r) => &r.base,
            Self::ReprocessJob(r) => &r.base,
            Self::WorkItem(r) => &r.base,
            Self::WorkEdge(r) => &r.base,
            Self::WorkRun(r) => &r.base,
            Self::WorkArtifact(r) => &r.base,
            Self::WorkDeadletter(r) => &r.base,
            Self::SystemEvent(r) => &r.base,
            Self::IngestDecision(r) => &r.base,
            Self::WorkItemArchive(r) => &r.base,
            Self::WorkRunArchive(r) => &r.base,
        }
    }

    /// Explicit tenant relocation. The caller retains and hashes the original
    /// lexical row separately. Only typed base scopes move; record links, policy
    /// identifiers and arbitrary payload strings retain their original values.
    pub fn relocate_space(&mut self, source: &str, target: &str) -> StorageResult<()> {
        if source.trim().is_empty() || target.trim().is_empty() {
            return Err(invalid("empty transfer space mapping"));
        }
        if self.base().space_id != source {
            return Ok(());
        }
        match self {
            Self::WorkItemArchive(r) => {
                if r.record.base.space_id != source {
                    return Err(invalid("archive inner/outer space differs"));
                }
                r.record.base.space_id = target.into();
            }
            Self::WorkRunArchive(r) => {
                if r.record.base.space_id != source {
                    return Err(invalid("archive inner/outer space differs"));
                }
                r.record.base.space_id = target.into();
            }
            _ => {}
        }
        let base = match self {
            Self::AuditLog(r) => &mut r.base,
            Self::CostLedgerDaily(r) => &mut r.base,
            Self::Doc(r) => &mut r.base,
            Self::DocRevision(r) => &mut r.base,
            Self::DocNode(r) => &mut r.base,
            Self::MediaAsset(r) => &mut r.base,
            Self::Entity(r) => &mut r.base,
            Self::Source(r) => &mut r.base,
            Self::EntityIdentityKey(r) => &mut r.base,
            Self::IdentityCandidate(r) => &mut r.base,
            Self::EntityMerge(r) => &mut r.base,
            Self::PredicateRegistry(r) => &mut r.base,
            Self::PredicateAlias(r) => &mut r.base,
            Self::Assertion(r) => &mut r.base,
            Self::Attribute(r) => &mut r.base,
            Self::Rel(r) => &mut r.base,
            Self::ConflictGroup(r) => &mut r.base,
            Self::TruthSlot(r) => &mut r.base,
            Self::Event(r) => &mut r.base,
            Self::Freshness(r) => &mut r.base,
            Self::ReprocessJob(r) => &mut r.base,
            Self::WorkItem(r) => &mut r.base,
            Self::WorkEdge(r) => &mut r.base,
            Self::WorkRun(r) => &mut r.base,
            Self::WorkArtifact(r) => &mut r.base,
            Self::WorkDeadletter(r) => &mut r.base,
            Self::SystemEvent(r) => &mut r.base,
            Self::IngestDecision(r) => &mut r.base,
            Self::WorkItemArchive(r) => &mut r.base,
            Self::WorkRunArchive(r) => &mut r.base,
        };
        base.space_id = target.into();
        Ok(())
    }

    /// Serialize the complete original row without the transfer enum table wrapper.
    pub fn json(&self) -> StorageResult<serde_json::Value> {
        let value = match self {
            Self::AuditLog(r) => serde_json::to_value(r),
            Self::CostLedgerDaily(r) => serde_json::to_value(r),
            Self::Doc(r) => serde_json::to_value(r),
            Self::DocRevision(r) => serde_json::to_value(r),
            Self::DocNode(r) => serde_json::to_value(r),
            Self::MediaAsset(r) => serde_json::to_value(r),
            Self::Entity(r) => serde_json::to_value(r),
            Self::Source(r) => serde_json::to_value(r),
            Self::EntityIdentityKey(r) => serde_json::to_value(r),
            Self::IdentityCandidate(r) => serde_json::to_value(r),
            Self::EntityMerge(r) => serde_json::to_value(r),
            Self::PredicateRegistry(r) => serde_json::to_value(r),
            Self::PredicateAlias(r) => serde_json::to_value(r),
            Self::Assertion(r) => serde_json::to_value(r),
            Self::Attribute(r) => serde_json::to_value(r),
            Self::Rel(r) => serde_json::to_value(r),
            Self::ConflictGroup(r) => serde_json::to_value(r),
            Self::TruthSlot(r) => serde_json::to_value(r),
            Self::Event(r) => serde_json::to_value(r),
            Self::Freshness(r) => serde_json::to_value(r),
            Self::ReprocessJob(r) => serde_json::to_value(r),
            Self::WorkItem(r) => serde_json::to_value(r),
            Self::WorkEdge(r) => serde_json::to_value(r),
            Self::WorkRun(r) => serde_json::to_value(r),
            Self::WorkArtifact(r) => serde_json::to_value(r),
            Self::WorkDeadletter(r) => serde_json::to_value(r),
            Self::SystemEvent(r) => serde_json::to_value(r),
            Self::IngestDecision(r) => serde_json::to_value(r),
            Self::WorkItemArchive(r) => serde_json::to_value(r),
            Self::WorkRunArchive(r) => serde_json::to_value(r),
        };
        value.map_err(|e| invalid(e.to_string()))
    }

    pub(crate) fn native(&self) -> StorageResult<Value> {
        if self.id().table().as_str() != self.table() || self.base().space_id.is_empty() {
            return Err(invalid("typed transfer variant identity/space mismatch"));
        }
        let value = match self {
            Self::AuditLog(r) => crate::surreal::audit_log_to_value(r)?,
            Self::CostLedgerDaily(r) => crate::cost_ledger_daily_repository::CostLedgerDailyRepository::cost_ledger_daily_to_value(r)?,
            Self::Doc(r) => crate::surreal::doc_to_value(r)?,
            Self::DocRevision(r) => crate::doc_revision_repository::DocRevisionRepository::doc_revision_to_value(r)?,
            Self::DocNode(r) => crate::doc_node_repository::DocNodeRepository::doc_node_to_value(r)?,
            Self::MediaAsset(r) => crate::media_asset_repository::MediaAssetRepository::media_asset_to_value(r)?,
            Self::Entity(r) => crate::surreal::entity_to_value(r)?,
            Self::Source(r) => crate::surreal::source_to_value(r)?,
            Self::EntityIdentityKey(r) => crate::surreal::entity_identity_key_to_value(r)?,
            Self::IdentityCandidate(r) => crate::surreal::identity_candidate_to_value(r)?,
            Self::EntityMerge(r) => crate::surreal::entity_merge_to_value(r)?,
            Self::PredicateRegistry(r) => crate::predicate_registry_repository::PredicateRegistryRepository::predicate_registry_to_value(r)?,
            Self::PredicateAlias(r) => crate::predicate_alias_repository::PredicateAliasRepository::predicate_alias_to_value(r)?,
            Self::Assertion(r) => crate::assertion_repository::AssertionRepository::assertion_to_value(r)?,
            Self::Attribute(r) => crate::attribute_repository::AttributeRepository::attribute_to_value(r)?,
            Self::Rel(r) => crate::surreal::rel_to_value(r)?,
            Self::ConflictGroup(r) => crate::conflict_group_repository::ConflictGroupRepository::conflict_group_to_value(r)?,
            Self::TruthSlot(r) => crate::truth_slot_repository::TruthSlotRepository::truth_slot_to_value(r)?,
            Self::Event(r) => crate::event_repository::EventRepository::event_to_value(r)?,
            Self::Freshness(r) => crate::freshness_repository::FreshnessRepository::freshness_to_value(r)?,
            Self::ReprocessJob(r) => crate::reprocess_job_repository::ReprocessJobRepository::reprocess_job_to_value(r)?,
            Self::WorkItem(r) => crate::work_item_repository::WorkItemRepository::work_item_to_value(r)?,
            Self::WorkEdge(r) => crate::work_edge_repository::WorkEdgeRepository::work_edge_to_value(r)?,
            Self::WorkRun(r) => crate::work_run_repository::WorkRunRepository::work_run_to_value(r)?,
            Self::WorkArtifact(r) => crate::work_artifact_repository::WorkArtifactRepository::work_artifact_to_value(r)?,
            Self::WorkDeadletter(r) => crate::work_deadletter_repository::WorkDeadletterRepository::work_deadletter_to_value(r)?,
            Self::SystemEvent(r) => crate::system_event_repository::SystemEventRepository::system_event_to_value(r)?,
            Self::IngestDecision(r) => crate::ingest_decision_repository::IngestDecisionRepository::ingest_decision_to_value(r)?,
            Self::WorkItemArchive(r) => archive_native(&r.base, &r.record.base, &r.original_id, r.archived_at, crate::WorkItemRepository::work_item_to_value(&r.record)?)?,
            Self::WorkRunArchive(r) => archive_native(&r.base, &r.record.base, &r.original_id, r.archived_at, crate::WorkRunRepository::work_run_to_value(&r.record)?)?,
        };
        let Value::Object(mut fields) = value else {
            return Err(invalid("typed converter did not return an object"));
        };
        fields.insert(
            "id".into(),
            crate::surreal::thing_from_record_id(self.id()).into(),
        );
        if let Self::Rel(r) = self {
            fields.insert(
                "in".into(),
                crate::surreal::thing_from_record_id(r.subject.id()).into(),
            );
            fields.insert(
                "out".into(),
                crate::surreal::thing_from_record_id(r.object.id()).into(),
            );
        }
        Ok(Value::Object(fields))
    }

    fn projection(&self) -> String {
        match self {
            Self::AuditLog(_) => crate::audit_log_repository::AUDIT_LOG_PROJECTION.into(),
            Self::CostLedgerDaily(_) => {
                crate::cost_ledger_daily_repository::COST_LEDGER_DAILY_PROJECTION.into()
            }
            Self::Doc(_) => crate::doc_repository::DOC_PROJECTION.into(),
            Self::DocRevision(_) => crate::doc_revision_repository::DOC_REVISION_PROJECTION.into(),
            Self::DocNode(_) => crate::doc_node_repository::DOC_NODE_PROJECTION.into(),
            Self::MediaAsset(_) => format!(
                "{}, payload_base64",
                crate::media_asset_repository::MEDIA_ASSET_METADATA_PROJECTION
            ),
            Self::Entity(_) => crate::entity_repository::ENTITY_PROJECTION.into(),
            Self::Source(_) => crate::source_repository::SOURCE_PROJECTION.into(),
            Self::EntityIdentityKey(_) => {
                crate::entity_identity_key_repository::ENTITY_IDENTITY_KEY_PROJECTION.into()
            }
            Self::IdentityCandidate(_) => {
                crate::identity_candidate_repository::IDENTITY_CANDIDATE_PROJECTION.into()
            }
            Self::EntityMerge(_) => crate::entity_merge_repository::ENTITY_MERGE_PROJECTION.into(),
            Self::PredicateRegistry(_) => {
                crate::predicate_registry_repository::PREDICATE_REGISTRY_PROJECTION.into()
            }
            Self::PredicateAlias(_) => {
                crate::predicate_alias_repository::PREDICATE_ALIAS_PROJECTION.into()
            }
            Self::Assertion(_) => crate::assertion_repository::ASSERTION_PROJECTION.into(),
            Self::Attribute(_) => crate::attribute_repository::ATTRIBUTE_PROJECTION.into(),
            Self::Rel(_) => crate::rel_repository::REL_PROJECTION.into(),
            Self::ConflictGroup(_) => {
                crate::conflict_group_repository::CONFLICT_GROUP_PROJECTION.into()
            }
            Self::TruthSlot(_) => crate::truth_slot_repository::TRUTH_SLOT_PROJECTION.into(),
            Self::Event(_) => crate::event_repository::EVENT_PROJECTION.into(),
            Self::Freshness(_) => crate::freshness_repository::FRESHNESS_PROJECTION.into(),
            Self::ReprocessJob(_) => {
                crate::reprocess_job_repository::REPROCESS_JOB_PROJECTION.into()
            }
            Self::WorkItem(_) => crate::work_item_repository::WORK_ITEM_PROJECTION.into(),
            Self::WorkEdge(_) => crate::work_edge_repository::WORK_EDGE_PROJECTION.into(),
            Self::WorkRun(_) => crate::work_run_repository::WORK_RUN_PROJECTION.into(),
            Self::WorkArtifact(_) => {
                crate::work_artifact_repository::WORK_ARTIFACT_PROJECTION.into()
            }
            Self::WorkDeadletter(_) => {
                crate::work_deadletter_repository::WORK_DEADLETTER_PROJECTION.into()
            }
            Self::SystemEvent(_) => crate::system_event_repository::SYSTEM_EVENT_PROJECTION.into(),
            Self::IngestDecision(_) => {
                crate::ingest_decision_repository::INGEST_DECISION_PROJECTION.into()
            }
            Self::WorkItemArchive(_) | Self::WorkRunArchive(_) => {
                "id, original_id, archived_at".into()
            }
        }
    }

    fn decode(&self, response: &mut surrealdb::Response, index: usize) -> StorageResult<()> {
        let actual = match self {
            Self::AuditLog(_) => decode_native_row::<AuditLogDb, AuditLog>(response, index)?,
            Self::CostLedgerDaily(_) => {
                decode_native_row::<CostLedgerDailyDb, CostLedgerDaily>(response, index)?
            }
            Self::Doc(_) => decode_native_row::<DocDb, Doc>(response, index)?,
            Self::DocRevision(_) => {
                decode_native_row::<DocRevisionDb, DocRevision>(response, index)?
            }
            Self::DocNode(_) => decode_native_row::<DocNodeDb, DocNode>(response, index)?,
            Self::MediaAsset(_) => decode_native_row::<MediaAssetDb, MediaAsset>(response, index)?,
            Self::Entity(_) => decode_native_row::<EntityDb, Entity>(response, index)?,
            Self::Source(_) => decode_native_row::<SourceDb, Source>(response, index)?,
            Self::EntityIdentityKey(_) => {
                decode_native_row::<EntityIdentityKeyDb, EntityIdentityKey>(response, index)?
            }
            Self::IdentityCandidate(_) => {
                decode_native_row::<IdentityCandidateDb, IdentityCandidate>(response, index)?
            }
            Self::EntityMerge(_) => {
                decode_native_row::<EntityMergeDb, EntityMerge>(response, index)?
            }
            Self::PredicateRegistry(_) => {
                decode_native_row::<PredicateRegistryDb, PredicateRegistry>(response, index)?
            }
            Self::PredicateAlias(_) => {
                decode_native_row::<PredicateAliasDb, PredicateAlias>(response, index)?
            }
            Self::Assertion(_) => decode_native_row::<AssertionDb, Assertion>(response, index)?,
            Self::Attribute(_) => decode_native_row::<AttributeDb, Attribute>(response, index)?,
            Self::Rel(_) => decode_native_row::<RelDb, Rel>(response, index)?,
            Self::ConflictGroup(_) => {
                decode_native_row::<ConflictGroupDb, ConflictGroup>(response, index)?
            }
            Self::TruthSlot(_) => decode_native_row::<TruthSlotDb, TruthSlot>(response, index)?,
            Self::Event(_) => decode_native_row::<EventDb, Event>(response, index)?,
            Self::Freshness(_) => decode_native_row::<FreshnessDb, Freshness>(response, index)?,
            Self::ReprocessJob(_) => {
                decode_native_row::<ReprocessJobDb, ReprocessJob>(response, index)?
            }
            Self::WorkItem(_) => decode_native_row::<WorkItemDb, WorkItem>(response, index)?,
            Self::WorkEdge(_) => decode_native_row::<WorkEdgeDb, WorkEdge>(response, index)?,
            Self::WorkRun(_) => decode_native_row::<WorkRunDb, WorkRun>(response, index)?,
            Self::WorkArtifact(_) => {
                decode_native_row::<WorkArtifactDb, WorkArtifact>(response, index)?
            }
            Self::WorkDeadletter(_) => {
                decode_native_row::<WorkDeadletterDb, WorkDeadletter>(response, index)?
            }
            Self::SystemEvent(_) => {
                decode_native_row::<SystemEventDb, SystemEvent>(response, index)?
            }
            Self::IngestDecision(_) => {
                decode_native_row::<IngestDecisionDb, IngestDecision>(response, index)?
            }
            Self::WorkItemArchive(_) | Self::WorkRunArchive(_) => return Ok(()), // Full native envelope+record checked atomically.
        };
        if actual != self.json()? {
            return Err(invalid("typed native readback changed source payload"));
        }
        Ok(())
    }
}

fn decode_original<T: serde::de::DeserializeOwned>(
    original: &serde_json::Value,
) -> StorageResult<T> {
    serde_json::from_value(original.clone()).map_err(|e| invalid(e.to_string()))
}

fn decode_native_row<D, R>(
    response: &mut surrealdb::Response,
    index: usize,
) -> StorageResult<serde_json::Value>
where
    D: serde::de::DeserializeOwned,
    R: TryFrom<D> + serde::Serialize,
    R::Error: Into<StorageError>,
{
    let rows: Vec<D> = response.take(index).map_err(|e| invalid(e.to_string()))?;
    if rows.len() != 1 {
        return Err(invalid("typed transfer readback count differs"));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| invalid("missing readback"))?;
    let record = R::try_from(row).map_err(Into::into)?;
    serde_json::to_value(record).map_err(|e| invalid(e.to_string()))
}

/// Compare original lexical JSON with the typed serializer, allowing only the
/// explicitly named serde defaults/omitted Option fields below.
///
/// Original lexical bytes and their hash remain separate evidence; unknown fields, timestamp
/// normalization and arbitrary metadata changes are never allowed here.
pub fn validate_original_representation(
    table: &str,
    original: &serde_json::Value,
    typed: &serde_json::Value,
) -> StorageResult<()> {
    fn defaults(value: &mut serde_json::Value, fields: &[(&str, serde_json::Value)]) {
        if let Some(object) = value.as_object_mut() {
            for (name, default) in fields {
                object
                    .entry((*name).to_owned())
                    .or_insert_with(|| default.clone());
            }
        }
    }
    fn absent_nulls(value: &mut serde_json::Value, fields: &[&str]) {
        if let Some(object) = value.as_object_mut() {
            for name in fields {
                if object.get(*name).is_some_and(serde_json::Value::is_null) {
                    object.remove(*name);
                }
            }
        }
    }
    fn base(value: &mut serde_json::Value) {
        defaults(
            value,
            &[
                ("subjects", serde_json::json!([])),
                ("subjects_state", serde_json::json!("unknown")),
            ],
        );
        absent_nulls(value, &["deleted_at"]);
        if let Some(security) = value.get_mut("security") {
            defaults(
                security,
                &[
                    ("labels", serde_json::json!([])),
                    ("handling", serde_json::json!([])),
                ],
            );
        }
        if let Some(provenance) = value.get_mut("provenance") {
            defaults(
                provenance,
                &[
                    ("sources", serde_json::json!([])),
                    ("chain", serde_json::json!([])),
                ],
            );
            absent_nulls(
                provenance,
                &[
                    "extractor",
                    "work_run_id",
                    "run_id",
                    "prompt_id",
                    "config_id",
                ],
            );
            if let Some(extractor) = provenance.get_mut("extractor") {
                absent_nulls(extractor, &["name", "version", "model"]);
            }
        }
    }
    let mut expected = original.clone();
    base(&mut expected);
    if matches!(table, "work_item_archive" | "work_run_archive") {
        if let Some(record) = expected.get_mut("record") {
            base(record);
        }
    }
    if table == "doc" {
        defaults(
            &mut expected,
            &[
                ("source", serde_json::Value::Null),
                ("metadata", serde_json::json!({})),
            ],
        );
        absent_nulls(&mut expected, &["sidecar_text"]);
        if let Some(object) = expected.as_object_mut() {
            if object.get("media_attachments") == Some(&serde_json::json!([])) {
                object.remove("media_attachments");
            }
        }
    }
    if &expected != typed {
        return Err(invalid("typed transfer would change or omit source fields"));
    }
    Ok(())
}

fn archive_native(
    base: &BaseFields,
    record_base: &BaseFields,
    original: &RecordId,
    at: chrono::DateTime<chrono::Utc>,
    record: Value,
) -> StorageResult<Value> {
    let mut expected_base = record_base.clone();
    expected_base.id = base.id.clone();
    if &expected_base != base
        || &record_base.id != original
        || base.id.id() != original.id()
        || crate::work_retention::archive_id(original) != base.id
    {
        return Err(invalid("noncanonical archive envelope"));
    }
    let Value::Object(mut record) = record else {
        return Err(invalid("archive record is not an object"));
    };
    record.insert(
        "id".into(),
        crate::surreal::thing_from_record_id(original).into(),
    );
    Ok(Value::Object(Object::from(
        BTreeMap::<String, Value>::from([
            ("space_id".into(), base.space_id.clone().into()),
            (
                "original_id".into(),
                crate::surreal::thing_from_record_id(original).into(),
            ),
            (
                "archived_at".into(),
                crate::surreal::datetime_from_chrono(at).into(),
            ),
            ("record".into(), Value::Object(record)),
        ]),
    )))
}

fn prepare_transfer_rows(rows: &[TransferRecord]) -> StorageResult<(Vec<Value>, usize)> {
    let mut ids = BTreeSet::new();
    let mut bytes = 0usize;
    let mut native = Vec::with_capacity(rows.len());
    for row in rows {
        if !ids.insert(row.id().clone()) {
            return Err(invalid("duplicate transfer identity in batch"));
        }
        let value = row.native()?;
        bytes = bytes
            .checked_add(
                serde_json::to_vec(&row.json()?)
                    .map_err(|e| invalid(e.to_string()))?
                    .len(),
            )
            .and_then(|n| n.checked_add(value.to_string().len()))
            .ok_or_else(|| invalid("batch byte overflow"))?;
        if bytes > MAX_BATCH_BYTES {
            return Err(invalid("transfer batch byte bound"));
        }
        native.push(value);
    }
    Ok((native, bytes))
}

fn decode_transfer_rows(
    rows: &[TransferRecord],
    response: &mut surrealdb::Response,
) -> StorageResult<()> {
    let start = response
        .num_statements()
        .checked_sub(rows.len())
        .ok_or_else(|| invalid("batch readback statements missing"))?;
    for (index, row) in rows.iter().enumerate() {
        row.decode(response, start + index)?;
    }
    Ok(())
}

fn transfer_statement(
    rows: &[TransferRecord],
    native: Vec<Value>,
) -> StorageResult<(String, BTreeMap<String, Value>)> {
    let mut sql = String::from(
        "BEGIN TRANSACTION;
",
    );
    let mut bindings = BTreeMap::<String, Value>::new();
    for (i, (row, value)) in rows.iter().zip(native).enumerate() {
        bindings.insert(
            format!("id_{i}"),
            crate::surreal::thing_from_record_id(row.id()).into(),
        );
        bindings.insert(format!("record_{i}"), value.clone());
        bindings.insert(
            format!("expected_{i}"),
            crate::surreal::stored_native_value(value),
        );
        if matches!(
            row,
            TransferRecord::WorkItem(_)
                | TransferRecord::WorkRun(_)
                | TransferRecord::WorkItemArchive(_)
                | TransferRecord::WorkRunArchive(_)
        ) {
            let other = match row {
                TransferRecord::WorkItemArchive(r) => r.original_id.clone(),
                TransferRecord::WorkRunArchive(r) => r.original_id.clone(),
                _ => crate::work_retention::archive_id(row.id()),
            };
            bindings.insert(
                format!("other_{i}"),
                crate::surreal::thing_from_record_id(&other).into(),
            );
            writeln!(sql, "IF (SELECT VALUE id FROM ONLY $other_{i}) != NONE {{ THROW 'transfer_hot_cold_collision'; }};").map_err(|e| invalid(e.to_string()))?;
        }
        if matches!(row, TransferRecord::WorkItem(_)) {
            sql.push_str("IF array::len((SELECT VALUE id FROM work_producer_origin LIMIT 1)) != 0 { THROW 'transfer_existing_producer'; };
");
        }
        if let TransferRecord::Event(event) = row {
            event.validate()?;
            for (j, subject) in event.base.subjects.iter().enumerate() {
                bindings.insert(
                    format!("subject_{i}_{j}"),
                    crate::surreal::thing_from_record_id(subject.id()).into(),
                );
                bindings.insert(format!("space_{i}"), event.base.space_id.clone().into());
                writeln!(sql, "IF (SELECT VALUE id FROM ONLY $subject_{i}_{j} WHERE space_id = $space_{i} AND deleted_at IS NONE) = NONE {{ THROW 'transfer_event_subject'; }};").map_err(|e| invalid(e.to_string()))?;
            }
        }
        if let TransferRecord::Rel(rel) = row {
            bindings.insert(
                format!("subject_{i}"),
                crate::surreal::thing_from_record_id(rel.subject.id()).into(),
            );
            bindings.insert(
                format!("object_{i}"),
                crate::surreal::thing_from_record_id(rel.object.id()).into(),
            );
            writeln!(sql, "IF (SELECT VALUE id FROM ONLY $id_{i}) != NONE {{ THROW 'transfer_identity_exists'; }}; RELATE $subject_{i}->$id_{i}->$object_{i} CONTENT $record_{i} RETURN NONE;").map_err(|e| invalid(e.to_string()))?;
        } else {
            writeln!(sql, "CREATE $id_{i} CONTENT $record_{i} RETURN NONE;")
                .map_err(|e| invalid(e.to_string()))?;
        }
        writeln!(sql, "IF (SELECT * FROM ONLY $id_{i}) != $expected_{i} {{ THROW 'transfer_native_payload_changed'; }};").map_err(|e| invalid(e.to_string()))?;
    }
    // Select-backs are last, so indexing is independent of extra per-row guards.
    for (i, row) in rows.iter().enumerate() {
        writeln!(sql, "SELECT {} FROM $id_{i};", row.projection())
            .map_err(|e| invalid(e.to_string()))?;
    }
    sql.push_str("COMMIT TRANSACTION;");
    Ok((sql, bindings))
}

/// Import one bounded transaction, with no retry of ambiguous results.
/// Destination custody and original snapshot identity are checked every call.
pub async fn import_batch(
    client: &StorageClient,
    source_sha256: &str,
    rows: &[TransferRecord],
) -> StorageResult<()> {
    let batch_started = std::time::Instant::now();
    client.require_transfer_custody()?;
    let marker = client
        .erasure_journal()?
        .transfer_marker()?
        .ok_or_else(|| invalid("missing transfer marker"))?;
    if marker.source_sha256 != source_sha256 {
        return Err(invalid("batch snapshot identity differs"));
    }
    if rows.is_empty() || rows.len() > MAX_BATCH_ROWS {
        return Err(invalid("transfer batch row bound"));
    }
    if client.embedded_store().is_some() {
        return Err(invalid("typed transfer batch requires native destination"));
    }
    let (native, bytes) = prepare_transfer_rows(rows)?;
    let (sql, bindings) = transfer_statement(rows, native)?;
    let prepare_seconds = batch_started.elapsed().as_secs_f64();
    let query_started = std::time::Instant::now();
    let mut response = client.query_bind_checked(&sql, bindings).await?;
    let query_seconds = query_started.elapsed().as_secs_f64();
    let decode_started = std::time::Instant::now();
    decode_transfer_rows(rows, &mut response)?;
    tracing::debug!(
        target: "recall_transfer_timing",
        kind = "ordinary",
        table = rows[0].table(),
        rows = rows.len(),
        prepared_bytes = bytes,
        prepare_seconds,
        query_seconds,
        decode_seconds = decode_started.elapsed().as_secs_f64(),
        total_seconds = batch_started.elapsed().as_secs_f64(),
        "typed transfer batch completed"
    );
    Ok(())
}
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
struct Strict(serde_json::Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| Strict(n.into()))
                    .ok_or_else(|| E::custom("nonfinite JSON"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Strict, E> {
                self.visit_string(v.into())
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Strict, E> {
                if v.contains('\0') {
                    return Err(E::custom("NUL in immutable JSON"));
                }
                Ok(Strict(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(serde_json::Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(Strict(v)) = a.next_element()? {
                    values.push(v);
                }
                Ok(Strict(values.into()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if k.contains('\0') || values.contains_key(&k) {
                        return Err(serde::de::Error::custom("duplicate/NUL JSON key"));
                    }
                    let Strict(v) = a.next_value()?;
                    values.insert(k, v);
                }
                Ok(Strict(values.into()))
            }
        }
        de.deserialize_any(V)
    }
}

/// Parse original JSON without silently discarding duplicate members or integers
/// outside the typed JSON boundary.
///
/// The original lexical payload remains in the planner spool; this value is only the typed conversion input.
pub fn strict_json(raw: &str) -> StorageResult<serde_json::Value> {
    // serde_json otherwise rounds integers larger than u64 into f64. Refuse
    // them explicitly; parsing a quoted string never treats its digits as data.
    let bytes = raw.as_bytes();
    let (mut i, mut quoted, mut escaped) = (0, false, false);
    while i < bytes.len() {
        let c = bytes[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
            i += 1;
            continue;
        }
        if c == b'"' {
            quoted = true;
            i += 1;
            continue;
        }
        if c == b'-' || c.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit()
                    || matches!(bytes[i], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                i += 1;
            }
            let token = &raw[start..i];
            if !token.contains(['.', 'e', 'E'])
                && token.parse::<i64>().is_err()
                && token.parse::<u64>().is_err()
            {
                return Err(invalid("JSON integer exceeds lossless typed range"));
            }
        } else {
            i += 1;
        }
    }
    serde_json::from_str::<Strict>(raw)
        .map(|v| v.0)
        .map_err(|e| invalid(format!("strict transfer JSON: {e}")))
}

type PreparedEmbeddingRow = (
    elephant_embeddings::PartitionKey,
    surrealdb::sql::Thing,
    Value,
);

fn prepare_embedding_rows(
    rows: &[NodeEmbedding],
) -> StorageResult<(Vec<PreparedEmbeddingRow>, usize)> {
    use elephant_embeddings::{level_bucket, PartitionKey};
    let mut prepared = Vec::new();
    let mut bytes = 0usize;
    let mut unique = Vec::new();
    for row in rows {
        if row.dims <= 0
            || usize::try_from(row.dims).ok() != Some(row.vector.len())
            || row.model.trim().is_empty()
            || !row.vector.iter().all(|v| v.is_finite())
        {
            return Err(invalid("invalid original embedding"));
        }
        let norm = row.vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if !norm.is_finite() {
            return Err(invalid("embedding norm overflow"));
        }
        let key = PartitionKey::new(
            row.base.security.policy_id.clone(),
            level_bucket(row.base.security.level),
            row.model.clone(),
            row.vector.len(),
        )
        .map_err(|e| invalid(e.to_string()))?;
        let table = key.table_name();
        let mut id = crate::surreal::thing_from_record_id(row.node.id());
        id.tb = table;
        // The batch has at most 256 entries. Preserve native Ord equivalence
        // without using an interior-mutable native value as a tree key.
        match unique.binary_search(&id) {
            Ok(_) => return Err(invalid("duplicate embedding destination")),
            Err(position) => unique.insert(position, id.clone()),
        }
        let mut obj = crate::surreal::base_fields_to_object(&row.base)?;
        obj.insert("id".into(), id.clone().into());
        obj.insert(
            "node".into(),
            crate::surreal::thing_from_record_id(row.node.id()).into(),
        );
        obj.insert("model".into(), row.model.clone().into());
        obj.insert("dims".into(), row.dims.into());
        obj.insert("vector_norm".into(), f64::from(norm).into());
        obj.insert(
            "vector".into(),
            Value::Array(surrealdb::sql::Array::from(row.vector.clone())),
        );
        let native = Value::Object(obj);
        bytes = bytes
            .checked_add(native.to_string().len())
            .ok_or_else(|| invalid("embedding bytes overflow"))?;
        if bytes > MAX_BATCH_BYTES {
            return Err(invalid("embedding batch byte bound"));
        }
        prepared.push((key, id, native));
    }
    Ok((prepared, bytes))
}

/// Preserve partitioned vectors in one create-only transaction, including
/// original timestamps. This does not claim model/profile compatibility.
pub async fn import_embedding_batch(
    client: &StorageClient,
    source_sha256: &str,
    manager: &elephant_embeddings::PartitionManager,
    rows: &[NodeEmbedding],
) -> StorageResult<Vec<String>> {
    let batch_started = std::time::Instant::now();
    client.require_transfer_custody()?;
    let marker = client
        .erasure_journal()?
        .transfer_marker()?
        .ok_or_else(|| invalid("missing transfer marker"))?;
    if marker.source_sha256 != source_sha256 || rows.is_empty() || rows.len() > MAX_BATCH_ROWS {
        return Err(invalid("embedding transfer binding/bound"));
    }
    let (prepared, bytes) = prepare_embedding_rows(rows)?;
    // Validate entire payload before DDL; failed DDL leaves transfer unready.
    for (key, _, _) in &prepared {
        manager
            .ensure_partition(key, {
                let client = client.clone();
                move |ddl| async move {
                    client
                        .query_checked(&ddl)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
            })
            .await
            .map_err(|e| invalid(e.to_string()))?;
    }
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let mut bindings = BTreeMap::new();
    for (i, (_, id, value)) in prepared.iter().enumerate() {
        bindings.insert(format!("id_{i}"), Value::from(id.clone()));
        bindings.insert(format!("value_{i}"), value.clone());
        bindings.insert(
            format!("expected_{i}"),
            crate::surreal::stored_native_value(value.clone()),
        );
        writeln!(sql,"CREATE $id_{i} CONTENT $value_{i} RETURN NONE; IF (SELECT * FROM ONLY $id_{i}) != $expected_{i} {{ THROW 'embedding_transfer_payload_changed'; }};").map_err(|e|invalid(e.to_string()))?;
    }
    for i in 0..rows.len() {
        writeln!(sql,"SELECT id,space_id,created_at,updated_at,created_by,deleted_at,security,provenance,subjects,subjects_state,node,model,dims,vector FROM $id_{i};").map_err(|e|invalid(e.to_string()))?;
    }
    sql.push_str("COMMIT TRANSACTION;");
    let prepare_seconds = batch_started.elapsed().as_secs_f64();
    let query_started = std::time::Instant::now();
    let mut response = client.query_bind_checked(&sql, bindings).await?;
    let query_seconds = query_started.elapsed().as_secs_f64();
    let decode_started = std::time::Instant::now();
    let start = response
        .num_statements()
        .checked_sub(rows.len())
        .ok_or_else(|| invalid("embedding readback statements missing"))?;
    for (i, original) in rows.iter().enumerate() {
        let mut readback: Vec<crate::chunk_embedding_repository::NodeEmbeddingDb> = response
            .take(start + i)
            .map_err(|e| invalid(e.to_string()))?;
        if readback.len() != 1 {
            return Err(invalid("embedding readback missing"));
        }
        let mut actual =
            NodeEmbedding::try_from(readback.pop().ok_or_else(|| invalid("missing embedding"))?)?;
        // Physical partition IDs are node IDs; preserve original identity in
        // the source spool and report this explicit representation mapping.
        actual.base.id = original.base.id.clone();
        if &actual != original {
            return Err(invalid("embedding typed readback differs"));
        }
    }
    tracing::debug!(
        target: "recall_transfer_timing",
        kind = "embedding",
        rows = rows.len(),
        prepared_bytes = bytes,
        prepare_seconds,
        query_seconds,
        decode_seconds = decode_started.elapsed().as_secs_f64(),
        total_seconds = batch_started.elapsed().as_secs_f64(),
        "typed transfer batch completed"
    );
    Ok(prepared.into_iter().map(|(_, id, _)| id.tb).collect())
}

#[cfg(test)]
#[path = "transfer_batch_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "transfer_batch_legacy_literal_tests.rs"]
mod legacy_literal_tests;
