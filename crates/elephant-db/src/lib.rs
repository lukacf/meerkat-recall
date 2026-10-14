//! Sync in-memory storage kernel for Elephant.
//!
//! This crate intentionally implements only the storage-kernel features that
//! Elephant actively needs for embedded execution:
//! - typed record storage over logical Elephant tables
//! - atomic mutation closures
//! - snapshot import/export over bytes/readers/writers
//! - graph adjacency indexes for `rel`
//! - doc-tree indexes for `doc_node` / `doc_revision`
//! - partitioned vector indexes for `chunk_embedding`
//!
//! It does not implement a query language, filesystem persistence, networking,
//! or SurrealDB-specific runtime behavior.

#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod durable_commit;
mod erasure_guard;
#[cfg(test)]
mod erasure_guard_tests;
pub use durable_commit::KernelMutation;
pub use erasure_guard::ReferenceTokenNormalizer;
mod index_generation;
#[cfg(test)]
mod publication_tests;

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};

use chrono::{DateTime, Utc};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{
    Assertion, Attribute, AuditLog, ConflictGroup, CostLedgerDaily, Doc, DocNode, DocRevision,
    Entity, EntityIdentityKey, EntityMerge, Event, Freshness, IdMap, IdentityCandidate,
    IngestDecision, KnowledgeView, KnowledgeViewDefinition, KnowledgeViewDependencyIndex,
    KnowledgeViewRevision, MediaAsset, NodeEmbedding, PredicateAlias, PredicateRegistry, Rel,
    ReprocessJob, Source, SourceWatch, SourceWatchCheck, SpaceConfig, SystemEvent, TruthSlot,
    WorkArtifact, WorkDeadletter, WorkEdge, WorkItem, WorkItemArchive, WorkRun, WorkRunArchive,
};
use elephant_types::{RecordId, TableName};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Kernel result type.
pub type Result<T> = std::result::Result<T, ElephantDbError>;

/// Storage-kernel error.
#[derive(Debug, Clone, Error)]
pub enum ElephantDbError {
    /// Serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Missing record.
    #[error("record not found: {0}")]
    NotFound(String),
    /// Duplicate record or unique conflict.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The original erasure intent temporarily owns admission in this space.
    #[error("Source erasure is in progress; retry after cleanup")]
    ErasureInProgress,
    /// Invalid or unsupported snapshot payload.
    #[error("snapshot error: {0}")]
    Snapshot(String),
    /// Data integrity issue inside the embedded kernel.
    #[error("data integrity error: {0}")]
    DataIntegrity(String),
    /// Lock poisoning in the in-memory store.
    #[error("lock poisoned")]
    LockPoisoned,
}

/// Record types that can be stored in the kernel.
pub trait ElephantDbRecord: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Logical Elephant table for the record.
    const TABLE: TableName;

    /// Shared base fields.
    fn base(&self) -> &BaseFields;

    /// Mutable access to shared base fields.
    fn base_mut(&mut self) -> &mut BaseFields;
}

macro_rules! impl_elephant_db_record {
    ($($ty:ty => $table:expr),+ $(,)?) => {
        $(
            impl ElephantDbRecord for $ty {
                const TABLE: TableName = $table;

                fn base(&self) -> &BaseFields {
                    &self.base
                }

                fn base_mut(&mut self) -> &mut BaseFields {
                    &mut self.base
                }
            }
        )+
    };
}

impl_elephant_db_record!(
    Entity => TableName::Entity,
    Rel => TableName::Rel,
    Doc => TableName::Doc,
    NodeEmbedding => TableName::ChunkEmbedding,
    EntityIdentityKey => TableName::EntityIdentityKey,
    IdMap => TableName::IdMap,
    SystemEvent => TableName::SystemEvent,
    Event => TableName::Event,
    Source => TableName::Source,
    AuditLog => TableName::AuditLog,
    IdentityCandidate => TableName::IdentityCandidate,
    EntityMerge => TableName::EntityMerge,
    SpaceConfig => TableName::SpaceConfig,
    DocRevision => TableName::DocRevision,
    DocNode => TableName::DocNode,
    Assertion => TableName::Assertion,
    Attribute => TableName::Attribute,
    WorkItem => TableName::WorkItem,
    WorkItemArchive => TableName::WorkItemArchive,
    WorkEdge => TableName::WorkEdge,
    WorkRun => TableName::WorkRun,
    WorkRunArchive => TableName::WorkRunArchive,
    WorkArtifact => TableName::WorkArtifact,
    WorkDeadletter => TableName::WorkDeadletter,
    PredicateRegistry => TableName::PredicateRegistry,
    PredicateAlias => TableName::PredicateAlias,
    ConflictGroup => TableName::ConflictGroup,
    TruthSlot => TableName::TruthSlot,
    IngestDecision => TableName::IngestDecision,
    CostLedgerDaily => TableName::CostLedgerDaily,
    Freshness => TableName::Freshness,
    ReprocessJob => TableName::ReprocessJob,
    MediaAsset => TableName::MediaAsset,
    KnowledgeView => TableName::KnowledgeView,
    KnowledgeViewRevision => TableName::KnowledgeViewRevision,
    KnowledgeViewDefinition => TableName::KnowledgeViewDefinition,
    KnowledgeViewDependencyIndex => TableName::KnowledgeViewDependency,
    SourceWatch => TableName::SourceWatch,
    SourceWatchCheck => TableName::SourceWatchCheck,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotPayload {
    backend: String,
    version: u32,
    exported_at: DateTime<Utc>,
    tables: HashMap<String, Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Default)]
struct ElephantDbState {
    tables: HashMap<TableName, HashMap<RecordId, serde_json::Value>>,
    extension_tables: HashMap<String, HashMap<String, serde_json::Value>>,
    indexes: IndexState,
    reference_guard: Option<erasure_guard::RuntimeReferenceGuard>,
}

#[derive(Debug, Clone, Default)]
struct IndexState {
    graph_by_space: HashMap<String, GraphSpaceIndex>,
    docs_by_space: HashMap<String, DocSpaceIndex>,
    vector_partitions: HashMap<String, VectorPartitionIndex>,
    unique: UniqueIndexState,
}

#[derive(Debug, Clone, Default)]
struct UniqueIndexState {
    doc_rendition: HashMap<(String, String), RecordId>,
    entity_identity_key: HashMap<(String, String), RecordId>,
    assertion_key: HashMap<(String, String), RecordId>,
    predicate_registry: HashMap<(String, String), RecordId>,
    truth_slot: HashMap<(String, String), RecordId>,
    system_event_seq: HashMap<(String, i64), RecordId>,
    id_map_lookup: HashMap<(String, String, String), RecordId>,
    space_config: HashMap<String, RecordId>,
}

#[derive(Debug, Clone, Default)]
struct GraphSpaceIndex {
    graph: StableDiGraph<RecordId, GraphEdge>,
    node_by_entity_id: HashMap<RecordId, NodeIndex>,
    edge_by_rel_id: HashMap<RecordId, petgraph::stable_graph::EdgeIndex>,
}

#[derive(Debug, Clone)]
struct GraphEdge {
    rel_id: RecordId,
    rel_type: String,
}

impl GraphSpaceIndex {
    fn ensure_node(&mut self, entity_id: &RecordId) -> NodeIndex {
        if let Some(index) = self.node_by_entity_id.get(entity_id).copied() {
            return index;
        }
        let index = self.graph.add_node(entity_id.clone());
        self.node_by_entity_id.insert(entity_id.clone(), index);
        index
    }
}

#[derive(Debug, Clone, Default)]
struct DocSpaceIndex {
    nodes_by_revision: HashMap<RecordId, Vec<DocNodeOrder>>,
    chunks_by_revision: HashMap<RecordId, Vec<DocNodeOrder>>,
    parent_by_node: HashMap<RecordId, RecordId>,
    node_revision: HashMap<RecordId, RecordId>,
    node_collection: HashMap<RecordId, Option<String>>,
    doc_by_revision: HashMap<RecordId, RecordId>,
    doc_search: HashMap<RecordId, DocSearchEntry>,
    token_postings: HashMap<String, HashSet<RecordId>>,
    total_token_count: usize,
}

#[derive(Debug, Clone)]
struct DocNodeOrder {
    node_id: RecordId,
    depth: i32,
    position: i32,
}

#[derive(Debug, Clone)]
struct DocSearchEntry {
    normalized_search_text: String,
    token_counts: HashMap<String, usize>,
    token_count: usize,
    deleted: bool,
}

#[derive(Debug, Clone, Default)]
struct VectorPartitionIndex {
    dims: usize,
    spaces: HashMap<String, SpaceVectorPartition>,
}

#[derive(Debug, Clone, Default)]
struct SpaceVectorPartition {
    entries: Vec<VectorEntry>,
    entry_index_by_embedding_id: HashMap<RecordId, usize>,
}

#[derive(Debug, Clone)]
struct VectorEntry {
    node_id: RecordId,
    doc_id: Option<RecordId>,
    collection: Option<String>,
    deleted: bool,
    normalized_vector: Box<[f32]>,
}

/// Shared sync storage kernel.
#[derive(Debug, Default)]
pub struct ElephantDb {
    state: RwLock<ElephantDbState>,
}

/// Tables whose mutations can change canonical preparation reads or selectors.
/// Queue leases and outbox delivery do not alter semantic planning inputs.
#[must_use]
pub const fn fences_canonical_reads(table: TableName) -> bool {
    !matches!(
        table,
        TableName::WorkItem
            | TableName::WorkItemArchive
            | TableName::WorkEdge
            | TableName::WorkRun
            | TableName::WorkRunArchive
            | TableName::WorkArtifact
            | TableName::WorkDeadletter
            | TableName::SystemEvent
            | TableName::CostLedgerDaily
            | TableName::AuditLog
    )
}

/// Saved reprocessing selections are canonical admission inputs even though
/// ordinary extraction staging artifacts intentionally do not invalidate plans.
fn fences_canonical_row(table: TableName, row: &serde_json::Value) -> bool {
    fences_canonical_reads(table)
        || (table == TableName::WorkArtifact
            && matches!(
                row.get("artifact_type").and_then(serde_json::Value::as_str),
                Some("reprocess_selection/v1" | "reprocess_source_intent/v1")
            ))
}

fn fences_canonical_extension_reads(table: &str) -> bool {
    // Unknown extension tables may participate in declared identity selectors.
    // Only bookkeeping with separate exact fences is excluded. Never recurse.
    !matches!(
        table,
        "canonical_epoch"
            | "work_attempt"
            | "outbox_checkpoint"
            | "provider_call_receipt"
            | "source_operation"
            | "source_commit_receipt"
            | "source_decision_sequence"
            | "source_erasure_files"
            | "source_erasure_guard"
            | "source_erasure_membership"
    )
}

fn fences_canonical_extension_change(
    table: &str,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
) -> bool {
    if table != "work_producer_origin" {
        return fences_canonical_extension_reads(table);
    }
    let Some(before) = before else {
        return false;
    };
    let Some(after) = after else {
        return true;
    };
    let semantic = |value: &serde_json::Value| {
        let mut value = value.clone();
        if let Some(fields) = value.as_object_mut() {
            fields.remove("read_nonce");
        }
        value
    };
    semantic(before) != semantic(after)
}

fn checks_extension_erasure_refs(table: &str) -> bool {
    !matches!(
        table,
        "canonical_epoch"
            | "source_head"
            | "source_identity"
            | "source_authorization"
            | "source_operation"
            | "source_tombstone_ref"
            | "source_erasure_files"
            | "source_erasure_guard"
            | "source_erasure_membership"
            | "source_decision_sequence"
    )
}

fn canonical_epoch_row(space_id: &str) -> (String, serde_json::Value) {
    let key = format!("canonical_epoch:{space_id}");
    let epoch = RecordId::new_v4(TableName::Source).id().to_string();
    (
        key.clone(),
        serde_json::json!({"id": key, "space_id": space_id, "epoch": epoch}),
    )
}

/// Mutable transaction view over the kernel.
pub struct ElephantDbTx<'a> {
    state: &'a mut ElephantDbState,
    dirty_tables: HashSet<TableName>,
    undo_log: Vec<UndoEntry>,
    reference_permit: Option<String>,
    erasure_failure: Option<ElephantDbError>,
}

#[derive(Debug, Clone)]
enum UndoEntry {
    TableRow {
        table: TableName,
        id: RecordId,
        previous: Option<serde_json::Value>,
    },
    ExtensionRow {
        table_name: String,
        row_key: String,
        previous: Option<serde_json::Value>,
    },
}

/// Graph neighbor hit with hop distance and rel type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNeighbor {
    /// Neighbor entity ID.
    pub entity_id: RecordId,
    /// Hop distance from the start entity.
    pub distance: usize,
    /// Relationship type used to reach this neighbor.
    pub rel_type: String,
}

/// Relationship records grouped by direction for one entity.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntityRelations {
    /// Outgoing active relationship rows.
    pub outgoing: Vec<Rel>,
    /// Incoming active relationship rows.
    pub incoming: Vec<Rel>,
}

/// Vector search hit from the node embedding index.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEmbeddingSearchHit {
    /// Chunk/node record ID.
    pub node_id: RecordId,
    /// Parent doc ID.
    pub doc_id: RecordId,
    /// Similarity score.
    pub score: f64,
}

/// Options controlling snapshot import/index rebuild behavior.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotImportOptions {
    /// Rebuild node-embedding vector indexes during import.
    pub rebuild_vector_indexes: bool,
}

impl Default for SnapshotImportOptions {
    fn default() -> Self {
        Self {
            rebuild_vector_indexes: true,
        }
    }
}

impl ElephantDb {
    /// Creates a new empty kernel.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a kernel from snapshot bytes.
    pub fn from_snapshot_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_snapshot_bytes_with_options(bytes, SnapshotImportOptions::default())
    }

    /// Creates a kernel from snapshot bytes using explicit import options.
    pub fn from_snapshot_bytes_with_options(
        bytes: &[u8],
        options: SnapshotImportOptions,
    ) -> Result<Self> {
        let db = Self::new();
        db.import_snapshot_bytes_with_options(bytes, options)?;
        Ok(db)
    }

    /// Loads a private, identity-verified live checkpoint during exclusive recovery.
    /// Running work is preserved for lease recovery; ordinary backup imports remain guarded.
    pub fn from_recovery_snapshot_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_recovery_snapshot_bytes_with_options(bytes, SnapshotImportOptions::default())
    }

    /// Loads a trusted live checkpoint using explicit index rebuild options.
    pub fn from_recovery_snapshot_bytes_with_options(
        bytes: &[u8],
        options: SnapshotImportOptions,
    ) -> Result<Self> {
        let db = Self::new();
        db.import_snapshot_bytes_internal(bytes, options, true)?;
        Ok(db)
    }

    /// Installs runtime-only reference normalization from the storage adapter.
    /// The private key remains inside the adapter and never enters snapshots.
    pub fn install_reference_token_normalizer(
        &self,
        normalizer: std::sync::Arc<dyn ReferenceTokenNormalizer>,
    ) -> Result<()> {
        let guard = erasure_guard::RuntimeReferenceGuard(normalizer);
        let mut state = self.write_state();
        guard.validate_anchors(&state)?;
        state.reference_guard = Some(guard);
        drop(state);
        Ok(())
    }

    /// Returns nonempty persisted tables and derived vector partition names.
    pub fn table_names(&self) -> Result<Vec<String>> {
        let state = self.read_state();
        Ok(state.table_names())
    }

    /// Returns nonempty typed and extension tables, excluding derived indexes.
    pub fn persisted_table_names(&self) -> Result<Vec<String>> {
        Ok(self.read_state().persisted_table_names())
    }

    /// Returns true when the kernel contains no records.
    pub fn is_empty(&self) -> Result<bool> {
        let state = self.read_state();
        Ok(state.tables.values().all(HashMap::is_empty)
            && state.extension_tables.values().all(HashMap::is_empty))
    }

    /// Exports a JSON snapshot to bytes.
    pub fn export_snapshot_bytes(&self) -> Result<Vec<u8>> {
        let payload = self.read_state().snapshot_payload();
        serde_json::to_vec_pretty(&payload)
            .map_err(|err| ElephantDbError::Serialization(err.to_string()))
    }

    /// Exports an ordinary backup without private erasure membership tokens.
    /// The anchor/count remain so importing without independent journal replay
    /// cannot admit writes. Private live checkpoints use `export_snapshot_bytes`.
    pub fn export_backup_snapshot_bytes(&self) -> Result<Vec<u8>> {
        let mut payload = self.read_state().snapshot_payload();
        payload.tables.remove("source_erasure_membership");
        serde_json::to_vec_pretty(&payload)
            .map_err(|error| ElephantDbError::Serialization(error.to_string()))
    }

    /// Writes a snapshot to an arbitrary writer.
    pub fn export_snapshot_to_writer<W>(&self, mut writer: W) -> Result<()>
    where
        W: Write,
    {
        let bytes = self.export_snapshot_bytes()?;
        writer
            .write_all(&bytes)
            .map_err(|err| ElephantDbError::Snapshot(err.to_string()))
    }

    /// Imports a snapshot from bytes, replacing the live state.
    pub fn import_snapshot_bytes(&self, bytes: &[u8]) -> Result<()> {
        self.import_snapshot_bytes_with_options(bytes, SnapshotImportOptions::default())
    }

    /// Imports a snapshot from bytes with explicit index rebuild options.
    pub fn import_snapshot_bytes_with_options(
        &self,
        bytes: &[u8],
        options: SnapshotImportOptions,
    ) -> Result<()> {
        self.import_snapshot_bytes_internal(bytes, options, false)
    }

    fn import_snapshot_bytes_internal(
        &self,
        bytes: &[u8],
        options: SnapshotImportOptions,
        allow_running: bool,
    ) -> Result<()> {
        let payload: SnapshotPayload = serde_json::from_slice(bytes)
            .map_err(|err| ElephantDbError::Snapshot(err.to_string()))?;

        if payload.backend != "elephant-db" {
            return Err(ElephantDbError::Snapshot(format!(
                "unsupported backend `{}`",
                payload.backend
            )));
        }
        if payload.version != 1 {
            return Err(ElephantDbError::Snapshot(format!(
                "unsupported snapshot version `{}`",
                payload.version
            )));
        }

        let mut tables = HashMap::new();
        let mut extension_tables = HashMap::new();
        for (table_name, rows) in payload.tables {
            if let Ok(table) = table_name.parse::<TableName>() {
                let mut table_rows = HashMap::new();
                for row in rows {
                    let id = record_id_from_value(&row)?;
                    if table_rows.insert(id.clone(), row).is_some() {
                        return Err(ElephantDbError::Snapshot(format!(
                            "duplicate snapshot row id `{id}` in table `{table_name}`"
                        )));
                    }
                }
                tables.insert(table, table_rows);
            } else {
                let mut table_rows = HashMap::new();
                for row in rows {
                    let row_key = extension_row_key_from_value(&table_name, &row)?;
                    if table_rows.insert(row_key.clone(), row).is_some() {
                        return Err(ElephantDbError::Snapshot(format!(
                            "duplicate snapshot extension row key `{row_key}` in table `{table_name}`"
                        )));
                    }
                }
                extension_tables.insert(table_name, table_rows);
            }
        }

        let mut state = ElephantDbState {
            tables,
            extension_tables,
            ..ElephantDbState::default()
        };
        if !allow_running {
            state.ensure_no_running_work_items()?;
        }
        state.validate_unique_constraints()?;
        state.rebuild_all_indexes_with_options(options.rebuild_vector_indexes)?;

        let mut guard = self.write_state();
        state.reference_guard.clone_from(&guard.reference_guard);
        *guard = state;
        drop(guard);
        Ok(())
    }

    /// Imports a pre-validated snapshot kernel, replacing the live state.
    pub fn import_snapshot_db(&self, snapshot: Self) -> Result<()> {
        let mut state = snapshot.state.into_inner();
        state.ensure_no_running_work_items()?;
        let mut guard = self.write_state();
        state.reference_guard.clone_from(&guard.reference_guard);
        *guard = state;
        drop(guard);
        Ok(())
    }

    /// Imports a snapshot from an arbitrary reader.
    pub fn import_snapshot_from_reader<R>(&self, mut reader: R) -> Result<()>
    where
        R: Read,
    {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|err| ElephantDbError::Snapshot(err.to_string()))?;
        self.import_snapshot_bytes_with_options(&bytes, SnapshotImportOptions::default())
    }

    /// Runs a mutation closure atomically.
    pub fn tx<R, E, F>(&self, f: F) -> std::result::Result<R, E>
    where
        F: FnOnce(&mut ElephantDbTx<'_>) -> std::result::Result<R, E>,
        E: From<ElephantDbError>,
    {
        self.tx_with_commit(f, |_| Ok(()))
    }

    /// Runs a transaction with a storage-owned durability acknowledgement.
    /// The callback runs under the write lock after index validation. Returning
    /// an error rolls back all rows and indexes before any reader sees the write.
    pub fn tx_with_commit<R, E, F, C>(&self, f: F, commit: C) -> std::result::Result<R, E>
    where
        F: FnOnce(&mut ElephantDbTx<'_>) -> std::result::Result<R, E>,
        E: From<ElephantDbError>,
        C: FnOnce(&[KernelMutation]) -> std::result::Result<(), E>,
    {
        let mut guard = self.write_state();
        // Seal the original journal-qualified maintenance scope once, before
        // executing any user mutation closure. It cannot change mid-transaction.
        let reference_permit = guard
            .reference_guard
            .as_ref()
            .and_then(|runtime| runtime.0.erasure_reservation());
        let mut tx = ElephantDbTx {
            state: &mut guard,
            dirty_tables: HashSet::new(),
            undo_log: Vec::new(),
            reference_permit,
            erasure_failure: None,
        };

        let result = match f(&mut tx) {
            Ok(result) => result,
            Err(err) => {
                tx.rollback();
                return Err(err);
            }
        };
        if let Some(error) = tx.erasure_failure.take() {
            tx.rollback();
            return Err(E::from(error));
        }
        let dirty_tables = tx.dirty_tables.clone();
        let revert_tables = dirty_tables.clone();
        let undo_log = tx.undo_log.clone();
        drop(tx);
        if let Err(err) = guard.rebuild_for_tables(&dirty_tables) {
            ElephantDbTx::rollback_state(&mut guard, &undo_log);
            guard.rebuild_for_tables(&revert_tables).map_err(E::from)?;
            drop(guard);
            return Err(E::from(err));
        }
        let delta = durable_commit::committed_delta(&guard, &undo_log);
        if !delta.is_empty() {
            if let Err(error) = commit(&delta) {
                ElephantDbTx::rollback_state(&mut guard, &undo_log);
                guard.rebuild_for_tables(&revert_tables).map_err(E::from)?;
                return Err(error);
            }
        }
        drop(guard);
        Ok(result)
    }

    /// Creates a record.
    pub fn create<T>(&self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let private_fencing = {
            let state = self.read_state();
            state.reference_guard.is_some()
                || state.extension_tables.contains_key("source_erasure_guard")
        };
        if private_fencing
            || matches!(
                T::TABLE,
                TableName::Source
                    | TableName::Doc
                    | TableName::DocRevision
                    | TableName::DocNode
                    | TableName::Entity
            )
        {
            return self.tx(|tx| tx.create(record));
        }
        let mut state = self.write_state();
        if state.reference_guard.is_some()
            || state.extension_tables.contains_key("source_erasure_guard")
        {
            drop(state);
            return self.tx(|tx| tx.create(record));
        }
        state.create_record(record)
    }

    /// Replaces an existing record.
    pub fn replace<T>(&self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let private_fencing = {
            let state = self.read_state();
            state.reference_guard.is_some()
                || state.extension_tables.contains_key("source_erasure_guard")
        };
        if private_fencing
            || matches!(
                T::TABLE,
                TableName::Source
                    | TableName::Doc
                    | TableName::DocRevision
                    | TableName::DocNode
                    | TableName::Entity
            )
        {
            return self.tx(|tx| tx.replace(record));
        }
        let mut state = self.write_state();
        if state.reference_guard.is_some()
            || state.extension_tables.contains_key("source_erasure_guard")
        {
            drop(state);
            return self.tx(|tx| tx.replace(record));
        }
        state.replace_record(record)
    }

    /// Gets a record by ID, optionally including soft-deleted rows.
    pub fn get<T>(&self, space_id: &str, id: &RecordId, include_deleted: bool) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        let state = self.read_state();
        state.get_visible_record::<T>(space_id, id, include_deleted)
    }

    /// Gets a record by globally unique ID, optionally including soft-deleted rows.
    pub fn get_by_id<T>(&self, id: &RecordId, include_deleted: bool) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        let state = self.read_state();
        state.get_visible_record_by_id::<T>(id, include_deleted)
    }

    /// Finds an assertion by its unique assertion key inside a space.
    pub fn assertion_by_key(
        &self,
        space_id: &str,
        assertion_key: &str,
    ) -> Result<Option<Assertion>> {
        let state = self.read_state();
        let Some(id) = state
            .indexes
            .unique
            .assertion_key
            .get(&(space_id.to_string(), assertion_key.to_string()))
        else {
            return Ok(None);
        };
        state.get_visible_record::<Assertion>(space_id, id, false)
    }

    /// Scans a logical table.
    pub fn scan<T>(&self) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        let state = self.read_state();
        state.scan_table::<T>()
    }

    /// Returns a limited work-item status view without deserializing unrelated rows.
    pub fn work_items_by_status(
        &self,
        space_id: &str,
        status: &str,
        kind: Option<&str>,
        limit: usize,
        runnable_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<WorkItem>> {
        let state = self.read_state();
        state.work_items_by_status(space_id, status, kind, limit, runnable_at)
    }

    /// Gets multiple records by ID while preserving the provided order.
    pub fn get_many<T>(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        let state = self.read_state();
        state.collect_visible_records_by_ids::<T>(space_id, ids, include_deleted)
    }

    /// Returns matching document IDs in stable ID order using the cached doc search index.
    pub fn search_doc_ids(
        &self,
        space_id: &str,
        query: Option<&str>,
        include_deleted: bool,
    ) -> Result<Vec<RecordId>> {
        let (ids, _) = self.read_state().search_doc_ids_page(
            space_id,
            query,
            include_deleted,
            None,
            usize::MAX,
        );
        Ok(ids)
    }

    /// Returns a limited page of matching document IDs plus the total match count.
    pub fn search_doc_ids_page(
        &self,
        space_id: &str,
        query: Option<&str>,
        include_deleted: bool,
        cursor: Option<&RecordId>,
        limit: usize,
    ) -> Result<(Vec<RecordId>, usize)> {
        let state = self.read_state();
        Ok(state.search_doc_ids_page(space_id, query, include_deleted, cursor, limit))
    }

    /// Hard deletes a record.
    pub fn hard_delete<T>(&self, space_id: &str, id: &RecordId) -> Result<()>
    where
        T: ElephantDbRecord,
    {
        let private_fencing = {
            let state = self.read_state();
            state.reference_guard.is_some()
                || state.extension_tables.contains_key("source_erasure_guard")
        };
        if private_fencing
            || matches!(
                T::TABLE,
                TableName::Source
                    | TableName::Doc
                    | TableName::DocRevision
                    | TableName::DocNode
                    | TableName::Entity
            )
        {
            return self.tx(|tx| tx.hard_delete::<T>(space_id, id));
        }
        let mut state = self.write_state();
        if state.reference_guard.is_some()
            || state.extension_tables.contains_key("source_erasure_guard")
        {
            drop(state);
            return self.tx(|tx| tx.hard_delete::<T>(space_id, id));
        }
        state.hard_delete_record::<T>(space_id, id)
    }

    /// Reads one physical extension key under the state read lock. The key is
    /// fully qualified (`table:id`); no space filter or row scan is applied.
    /// This internal storage observation does not authorize access to its payload.
    pub fn extension_row_by_key(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        let state = self.read_state();
        Ok(state
            .extension_tables
            .get(table_name)
            .and_then(|rows| rows.get(key))
            .cloned())
    }

    /// Scans an opaque extension table, returning stable row-key/value pairs.
    pub fn scan_extension_rows(
        &self,
        table_name: &str,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let state = self.read_state();
        Ok(state.scan_extension_table(table_name))
    }

    /// Physical typed IDs, including deletion markers, in stable ID order.
    /// Only bounded IDs are cloned; no unbounded payload snapshot is allocated.
    pub fn physical_record_ids_page(
        &self,
        table: TableName,
        space: &str,
        after: Option<&RecordId>,
        limit: usize,
    ) -> Result<Vec<RecordId>> {
        let limit = limit.min(128);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.read_state();
        let mut page = std::collections::BTreeSet::new();
        if let Some(rows) = state.tables.get(&table) {
            for (id, row) in rows {
                if after.is_some_and(|cursor| id <= cursor)
                    || row.get("space_id").and_then(serde_json::Value::as_str) != Some(space)
                {
                    continue;
                }
                page.insert(id);
                if page.len() > limit {
                    page.pop_last();
                }
            }
        }
        let page = page.into_iter().cloned().collect();
        drop(state);
        Ok(page)
    }

    /// Reads a bounded page of opaque extension rows in stable key order.
    ///
    /// Selects at most `min(limit, 101)` rows in one exact space after the given
    /// full row key. The hash table requires a key scan, but only the bounded
    /// selected payloads are cloned; no full-table payload snapshot is made.
    pub fn extension_rows_page(
        &self,
        table_name: &str,
        space_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let limit = limit.min(101);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let state = self.read_state();
        let Some(table) = state.extension_tables.get(table_name) else {
            return Ok(Vec::new());
        };
        let mut page = std::collections::BTreeMap::new();
        for (key, row) in table {
            if after.is_some_and(|after| key.as_str() <= after)
                || row.get("space_id").and_then(serde_json::Value::as_str) != Some(space_id)
            {
                continue;
            }
            if page.len() == limit && page.last_key_value().is_some_and(|(last, _)| key >= *last) {
                continue;
            }
            let _ = page.insert(key, row);
            if page.len() > limit {
                let _ = page.pop_last();
            }
        }
        let rows = page
            .into_iter()
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        drop(state);
        Ok(rows)
    }

    /// Returns graph neighbors within `hops` traversing both directions.
    pub fn graph_neighbors(
        &self,
        space_id: &str,
        start: &RecordId,
        hops: usize,
    ) -> Result<Vec<GraphNeighbor>> {
        if hops == 0 {
            return Ok(Vec::new());
        }

        let state = self.read_state();
        let mut results = {
            let Some(space) = state.indexes.graph_by_space.get(space_id) else {
                return Ok(Vec::new());
            };
            let Some(start_index) = space.node_by_entity_id.get(start).copied() else {
                return Ok(Vec::new());
            };

            let mut visited = HashSet::from([start.clone()]);
            let mut frontier = vec![start_index];
            let mut results = Vec::new();

            for distance in 1..=hops {
                if frontier.is_empty() {
                    break;
                }

                let mut next_frontier = Vec::new();
                for node in frontier {
                    for edge in space.graph.edges(node) {
                        let neighbor = edge.target();
                        let entity_id = space.graph[neighbor].clone();
                        if visited.insert(entity_id.clone()) {
                            results.push(GraphNeighbor {
                                entity_id: entity_id.clone(),
                                distance,
                                rel_type: edge.weight().rel_type.clone(),
                            });
                            next_frontier.push(neighbor);
                        }
                    }
                    for edge in space
                        .graph
                        .edges_directed(node, petgraph::Direction::Incoming)
                    {
                        let neighbor = edge.source();
                        let entity_id = space.graph[neighbor].clone();
                        if visited.insert(entity_id.clone()) {
                            results.push(GraphNeighbor {
                                entity_id: entity_id.clone(),
                                distance,
                                rel_type: edge.weight().rel_type.clone(),
                            });
                            next_frontier.push(neighbor);
                        }
                    }
                }
                frontier = next_frontier;
            }

            results
        };
        drop(state);

        results.sort_by(|left, right| {
            left.distance
                .cmp(&right.distance)
                .then_with(|| compare_record_ids(&left.entity_id, &right.entity_id))
                .then_with(|| left.rel_type.cmp(&right.rel_type))
        });
        Ok(results)
    }

    /// Returns outgoing and incoming active relations for the entity.
    pub fn entity_relations(
        &self,
        space_id: &str,
        entity_id: &RecordId,
    ) -> Result<EntityRelations> {
        let (outgoing, incoming) = {
            let state = self.read_state();
            let Some(space) = state.indexes.graph_by_space.get(space_id) else {
                return Ok(EntityRelations::default());
            };
            let Some(node) = space.node_by_entity_id.get(entity_id).copied() else {
                return Ok(EntityRelations::default());
            };

            let mut outgoing_ids: Vec<_> = space
                .graph
                .edges(node)
                .map(|edge| edge.weight().rel_id.clone())
                .collect();
            let mut incoming_ids: Vec<_> = space
                .graph
                .edges_directed(node, petgraph::Direction::Incoming)
                .map(|edge| edge.weight().rel_id.clone())
                .collect();
            outgoing_ids.sort_by(compare_record_ids);
            incoming_ids.sort_by(compare_record_ids);

            (
                state.collect_records_by_ids::<Rel>(&outgoing_ids)?,
                state.collect_records_by_ids::<Rel>(&incoming_ids)?,
            )
        };
        Ok(EntityRelations { outgoing, incoming })
    }

    /// Returns all active nodes for the revision ordered by depth and position.
    pub fn doc_nodes_for_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
    ) -> Result<Vec<DocNode>> {
        let state = self.read_state();
        state.doc_nodes_for_revision(space_id, revision_id, false)
    }

    /// Returns active chunk nodes for the revision ordered by position.
    pub fn doc_chunks_for_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
    ) -> Result<Vec<DocNode>> {
        let state = self.read_state();
        state.doc_nodes_for_revision(space_id, revision_id, true)
    }

    /// Walks active parent pointers from `node_id` up to the root.
    pub fn doc_parent_chain(&self, space_id: &str, node_id: &RecordId) -> Result<Vec<DocNode>> {
        let state = self.read_state();
        state.doc_parent_chain(space_id, node_id)
    }

    /// Searches a vector partition using exact cosine similarity over the
    /// partitioned vector index.
    pub fn search_node_embeddings(
        &self,
        partition_name: &str,
        space_id: &str,
        vector: &[f32],
        limit: usize,
        collection: Option<&str>,
        include_deleted: bool,
    ) -> Result<Vec<NodeEmbeddingSearchHit>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let normalized_query = normalize_vector(vector);
        if normalized_query.is_empty() {
            return Ok(Vec::new());
        }

        let state = self.read_state();
        let mut hits = {
            let Some(partition) = state.indexes.vector_partitions.get(partition_name) else {
                return Ok(Vec::new());
            };
            if partition.dims != normalized_query.len() {
                return Err(ElephantDbError::DataIntegrity(format!(
                    "vector dimension mismatch for partition {partition_name}: query dims {} != partition dims {}",
                    normalized_query.len(),
                    partition.dims
                )));
            }
            let Some(space_partition) = partition.spaces.get(space_id) else {
                return Ok(Vec::new());
            };

            let mut hits = Vec::with_capacity(space_partition.entries.len());
            for entry in &space_partition.entries {
                if entry.deleted && !include_deleted {
                    continue;
                }
                if let Some(expected_collection) = collection {
                    if entry.collection.as_deref() != Some(expected_collection) {
                        continue;
                    }
                }

                let Some(doc_id) = &entry.doc_id else {
                    continue;
                };

                hits.push(NodeEmbeddingSearchHit {
                    node_id: entry.node_id.clone(),
                    doc_id: doc_id.clone(),
                    score: f64::from(dot_similarity(&entry.normalized_vector, &normalized_query)),
                });
            }

            hits
        };
        drop(state);

        if hits.len() > limit {
            let split_index = limit - 1;
            let _ = hits.select_nth_unstable_by(split_index, compare_embedding_hits_desc);
            hits.truncate(limit);
        }
        hits.sort_by(compare_embedding_hits_desc);
        Ok(hits)
    }

    fn read_state(&self) -> RwLockReadGuard<'_, ElephantDbState> {
        self.state.read()
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, ElephantDbState> {
        self.state.write()
    }
}

impl ElephantDbTx<'_> {
    /// Creates a record inside the transaction.
    pub fn create<T>(&mut self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let value = serialize_record("create", &record)?;
        let guarded = erasure_guard::check_erasure_guard(
            self.state,
            T::TABLE,
            &value,
            self.reference_permit.as_deref(),
        );
        self.record_erasure_result(guarded)?;
        let id = record.base().id.clone();
        self.state
            .ensure_unique_constraints_scan(T::TABLE, &id, &value)?;
        let table = self.state.tables.entry(T::TABLE).or_default();
        if table.contains_key(&id) {
            return Err(ElephantDbError::Conflict(id.to_string()));
        }
        let canonical_changed = fences_canonical_row(T::TABLE, &value);
        table.insert(id.clone(), value);
        let _ = self.dirty_tables.insert(T::TABLE);
        self.undo_log.push(UndoEntry::TableRow {
            table: T::TABLE,
            id,
            previous: None,
        });
        if canonical_changed {
            self.advance_canonical_epoch(&record.base().space_id)?;
        }
        self.bump_index_generation(T::TABLE, &record.base().space_id)?;
        self.advance_reference_guard_revision(&record.base().space_id)?;
        Ok(record)
    }

    /// Replaces an existing record inside the transaction.
    pub fn replace<T>(&mut self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let value = serialize_record("replace", &record)?;
        let guarded = erasure_guard::check_erasure_guard(
            self.state,
            T::TABLE,
            &value,
            self.reference_permit.as_deref(),
        );
        self.record_erasure_result(guarded)?;
        let id = record.base().id.clone();
        self.state
            .ensure_unique_constraints_scan(T::TABLE, &id, &value)?;
        let table = self.state.tables.entry(T::TABLE).or_default();
        let Some(previous) = table.get(&id).cloned() else {
            return Err(ElephantDbError::NotFound(id.to_string()));
        };
        let previous_space = json_string_field(&previous, "space_id").map(str::to_owned);
        let canonical_changed =
            fences_canonical_row(T::TABLE, &previous) || fences_canonical_row(T::TABLE, &value);
        let _ = table.insert(id.clone(), value);
        let _ = self.dirty_tables.insert(T::TABLE);
        self.undo_log.push(UndoEntry::TableRow {
            table: T::TABLE,
            id,
            previous: Some(previous),
        });
        if let Some(old_space) = previous_space
            .as_ref()
            .filter(|space| *space != &record.base().space_id)
        {
            self.advance_reference_guard_revision(old_space)?;
        }
        if canonical_changed {
            if let Some(previous_space) =
                previous_space.filter(|space| space != &record.base().space_id)
            {
                self.advance_canonical_epoch(&previous_space)?;
                self.bump_index_generation(T::TABLE, &previous_space)?;
            }
            self.advance_canonical_epoch(&record.base().space_id)?;
        }
        self.bump_index_generation(T::TABLE, &record.base().space_id)?;
        self.advance_reference_guard_revision(&record.base().space_id)?;
        Ok(record)
    }

    /// Gets a record by ID inside the transaction.
    pub fn get<T>(&self, space_id: &str, id: &RecordId, include_deleted: bool) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        self.state
            .get_visible_record::<T>(space_id, id, include_deleted)
    }

    /// Gets a globally unique record inside a transaction, including its owning space.
    pub fn get_by_id<T>(&self, id: &RecordId, include_deleted: bool) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        self.state
            .get_visible_record_by_id::<T>(id, include_deleted)
    }

    /// Scans an entire logical table inside the transaction.
    pub fn scan<T>(&self) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        self.state.scan_table::<T>()
    }

    /// Scans physical typed rows for coordinated erasure, including deleted rows.
    pub fn scan_raw_rows(&self, table: TableName) -> Result<Vec<(RecordId, serde_json::Value)>> {
        let mut rows = self.state.tables.get(&table).map_or_else(Vec::new, |rows| {
            rows.iter()
                .map(|(id, row)| (id.clone(), row.clone()))
                .collect::<Vec<_>>()
        });
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }

    /// Deletes a physical typed row with the same rollback/index fences as typed deletion.
    /// The table is explicit because an embedding can reuse its node's record ID.
    pub fn hard_delete_raw_row(
        &mut self,
        table: TableName,
        space: &str,
        id: &RecordId,
    ) -> Result<()> {
        let rows = self.state.tables.entry(table).or_default();
        let previous = rows
            .get(id)
            .ok_or_else(|| ElephantDbError::NotFound(id.to_string()))?;
        if previous.get("space_id").and_then(serde_json::Value::as_str) != Some(space) {
            return Err(ElephantDbError::NotFound(id.to_string()));
        }
        let previous = rows
            .remove(id)
            .ok_or_else(|| ElephantDbError::NotFound(id.to_string()))?;
        let canonical_changed = fences_canonical_row(table, &previous);
        self.dirty_tables.insert(table);
        self.undo_log.push(UndoEntry::TableRow {
            table,
            id: id.clone(),
            previous: Some(previous),
        });
        if canonical_changed {
            self.advance_canonical_epoch(space)?;
        }
        self.bump_index_generation(table, space)?;
        self.advance_reference_guard_revision(space)?;
        Ok(())
    }

    /// Hard deletes a record inside the transaction.
    pub fn hard_delete<T>(&mut self, space_id: &str, id: &RecordId) -> Result<()>
    where
        T: ElephantDbRecord,
    {
        let table = self.state.tables.entry(T::TABLE).or_default();
        let Some(previous) = table.remove(id) else {
            return Err(ElephantDbError::NotFound(id.to_string()));
        };
        let record = deserialize_record::<T>("hard_delete", previous.clone())?;
        if record.base().space_id != space_id {
            let _ = table.insert(id.clone(), previous);
            return Err(ElephantDbError::NotFound(id.to_string()));
        }
        let canonical_changed = fences_canonical_row(T::TABLE, &previous);
        let _ = self.dirty_tables.insert(T::TABLE);
        self.undo_log.push(UndoEntry::TableRow {
            table: T::TABLE,
            id: id.clone(),
            previous: Some(previous),
        });
        if canonical_changed {
            self.advance_canonical_epoch(space_id)?;
        }
        self.bump_index_generation(T::TABLE, space_id)?;
        self.advance_reference_guard_revision(space_id)?;
        Ok(())
    }

    /// Scans an opaque extension table inside the transaction.
    pub fn scan_extension_rows(
        &self,
        table_name: &str,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        Ok(self.state.scan_extension_table(table_name))
    }

    /// Lists opaque extension tables that currently contain rows.
    #[must_use]
    pub fn extension_table_names(&self) -> Vec<String> {
        self.state
            .extension_tables
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(table, _)| table.clone())
            .collect()
    }

    /// Inserts or replaces an opaque extension row inside the transaction.
    pub fn upsert_extension_row(
        &mut self,
        table_name: impl Into<String>,
        row: serde_json::Value,
    ) -> Result<String> {
        let table_name = table_name.into();
        let row_key = extension_row_key_from_value(&table_name, &row)?;
        let space = row
            .get("space_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ElephantDbError::DataIntegrity("extension row missing space".into()))?
            .to_owned();
        let fenced = fences_canonical_extension_change(
            &table_name,
            self.state
                .extension_tables
                .get(&table_name)
                .and_then(|rows| rows.get(&row_key)),
            Some(&row),
        );
        let reference_fenced = checks_extension_erasure_refs(table_name.as_ref());
        if reference_fenced {
            let guarded = erasure_guard::check_serialized_references(
                self.state,
                &space,
                table_name.as_ref(),
                &row,
                self.reference_permit.as_deref(),
            );
            self.record_erasure_result(guarded)?;
        }
        let visibility_changed = index_generation::control_visibility_changed(
            &table_name,
            self.state
                .extension_tables
                .get(&table_name)
                .and_then(|rows| rows.get(&row_key)),
            Some(&row),
        );
        let previous = self
            .state
            .extension_tables
            .entry(table_name.clone())
            .or_default()
            .insert(row_key.clone(), row);
        let old_space = previous
            .as_ref()
            .and_then(|row| row.get("space_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        self.undo_log.push(UndoEntry::ExtensionRow {
            table_name,
            row_key: row_key.clone(),
            previous,
        });
        if fenced {
            self.advance_canonical_epoch(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old_space| *old_space != &space) {
                self.advance_canonical_epoch(old_space)?;
            }
        }
        if visibility_changed {
            self.bump_space_index_generation(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old_space| *old_space != &space) {
                self.bump_space_index_generation(old_space)?;
            }
        }
        if reference_fenced {
            self.advance_reference_guard_revision(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old| *old != &space) {
                self.advance_reference_guard_revision(old_space)?;
            }
        }
        Ok(row_key)
    }

    /// Replaces an existing opaque extension row inside the transaction.
    pub fn replace_extension_row(
        &mut self,
        table_name: &str,
        row_key: &str,
        row: serde_json::Value,
    ) -> Result<()> {
        let space = row
            .get("space_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ElephantDbError::DataIntegrity("extension row missing space".into()))?
            .to_owned();
        if checks_extension_erasure_refs(table_name) {
            let guarded = erasure_guard::check_serialized_references(
                self.state,
                &space,
                table_name,
                &row,
                self.reference_permit.as_deref(),
            );
            self.record_erasure_result(guarded)?;
        }
        let table = self
            .state
            .extension_tables
            .get_mut(table_name)
            .ok_or_else(|| ElephantDbError::NotFound(format!("{table_name}:{row_key}")))?;
        if !table.contains_key(row_key) {
            return Err(ElephantDbError::NotFound(format!("{table_name}:{row_key}")));
        }
        let payload_key = extension_row_key_from_value(table_name, &row)?;
        if payload_key != row_key {
            return Err(ElephantDbError::DataIntegrity(format!(
                "extension row key mismatch: expected {row_key}, payload encodes {payload_key}"
            )));
        }
        let visibility_changed = index_generation::control_visibility_changed(
            table_name,
            table.get(row_key),
            Some(&row),
        );
        let fenced = fences_canonical_extension_change(table_name, table.get(row_key), Some(&row));
        let previous = table.insert(row_key.to_string(), row);
        let old_space = previous
            .as_ref()
            .and_then(|row| row.get("space_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        self.undo_log.push(UndoEntry::ExtensionRow {
            table_name: table_name.to_string(),
            row_key: row_key.to_string(),
            previous,
        });
        if fenced {
            self.advance_canonical_epoch(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old_space| *old_space != &space) {
                self.advance_canonical_epoch(old_space)?;
            }
        }
        if visibility_changed {
            self.bump_space_index_generation(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old_space| *old_space != &space) {
                self.bump_space_index_generation(old_space)?;
            }
        }
        if checks_extension_erasure_refs(table_name) {
            self.advance_reference_guard_revision(&space)?;
            if let Some(old_space) = old_space.as_ref().filter(|old| *old != &space) {
                self.advance_reference_guard_revision(old_space)?;
            }
        }
        Ok(())
    }

    /// Advances the private inventory revision with the same rollback as payload writes.
    pub fn advance_reference_guard_revision(&mut self, space: &str) -> Result<()> {
        let next =
            erasure_guard::next_revision_row(self.state, space, self.reference_permit.as_deref());
        if let Some(row) = self.record_erasure_result(next)? {
            let written = self.upsert_extension_row("source_erasure_guard", row);
            self.record_erasure_result(written)?;
        }
        Ok(())
    }

    fn record_erasure_result<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            if self.erasure_failure.is_none() {
                self.erasure_failure = Some(error.clone());
            }
        }
        result
    }

    /// Advances the predicate fence within this transaction, including rollback.
    pub fn advance_canonical_epoch(&mut self, space_id: &str) -> Result<String> {
        let (_, row) = canonical_epoch_row(space_id);
        let epoch = row["epoch"]
            .as_str()
            .ok_or_else(|| ElephantDbError::DataIntegrity("missing canonical epoch".to_owned()))?
            .to_owned();
        self.upsert_extension_row("canonical_epoch", row)?;
        Ok(epoch)
    }

    /// Physically removes an extension row without leaving payload in a tombstone.
    pub fn hard_delete_extension_row(
        &mut self,
        table_name: &str,
        row_key: &str,
        space_id: &str,
    ) -> Result<()> {
        let table = self
            .state
            .extension_tables
            .get_mut(table_name)
            .ok_or_else(|| ElephantDbError::NotFound(row_key.to_owned()))?;
        let previous = table
            .get(row_key)
            .ok_or_else(|| ElephantDbError::NotFound(row_key.to_owned()))?;
        if previous.get("space_id").and_then(serde_json::Value::as_str) != Some(space_id) {
            return Err(ElephantDbError::NotFound(row_key.to_owned()));
        }
        let visibility_changed =
            index_generation::control_visibility_changed(table_name, Some(previous), None);
        let previous = table.remove(row_key);
        self.undo_log.push(UndoEntry::ExtensionRow {
            table_name: table_name.to_owned(),
            row_key: row_key.to_owned(),
            previous,
        });
        if fences_canonical_extension_reads(table_name) {
            self.advance_canonical_epoch(space_id)?;
        }
        if visibility_changed {
            self.bump_space_index_generation(space_id)?;
        }
        if checks_extension_erasure_refs(table_name) {
            self.advance_reference_guard_revision(space_id)?;
        }
        Ok(())
    }

    fn rollback(&mut self) {
        Self::rollback_state(self.state, &self.undo_log);
    }

    fn rollback_state(state: &mut ElephantDbState, undo_log: &[UndoEntry]) {
        for entry in undo_log.iter().rev() {
            match entry {
                UndoEntry::TableRow {
                    table,
                    id,
                    previous,
                } => {
                    let rows = state.tables.entry(*table).or_default();
                    match previous {
                        Some(previous) => {
                            let _ = rows.insert(id.clone(), previous.clone());
                        }
                        None => {
                            let _ = rows.remove(id);
                        }
                    }
                }
                UndoEntry::ExtensionRow {
                    table_name,
                    row_key,
                    previous,
                } => {
                    let rows = state
                        .extension_tables
                        .entry(table_name.clone())
                        .or_default();
                    match previous {
                        Some(previous) => {
                            let _ = rows.insert(row_key.clone(), previous.clone());
                        }
                        None => {
                            let _ = rows.remove(row_key);
                        }
                    }
                }
            }
        }
    }
}

impl ElephantDbState {
    fn advance_canonical_epoch(&mut self, space_id: &str) {
        let (key, row) = canonical_epoch_row(space_id);
        self.extension_tables
            .entry("canonical_epoch".to_owned())
            .or_default()
            .insert(key, row);
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self.persisted_table_names();
        names.extend(self.indexes.vector_partitions.keys().cloned());
        names.sort();
        names.dedup();
        names
    }

    fn persisted_table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .tables
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(table, _)| table.as_str().to_string())
            .collect();
        names.extend(
            self.extension_tables
                .iter()
                .filter(|(_, rows)| !rows.is_empty())
                .map(|(table, _)| table.clone()),
        );
        names.sort();
        names.dedup();
        names
    }

    /// Whether snapshot export compacts embedding vector floats (default OFF).
    ///
    /// The floats in `chunk_embedding.vector` are f32 values that were widened to
    /// f64 on their way into `serde_json::Value`, so they serialize as up to 17
    /// significant digits ("0.5440000295639038" IS f32 0.544). Measured on the
    /// live household store, that widening is 95% of a 2.38GB snapshot — the
    /// direct cause of ~14-minute shutdown exports and 4GB boot loads.
    ///
    /// Compaction quantizes each component back through f32 and re-parses its
    /// shortest decimal form, so the exported text prints "0.544" and re-imports
    /// to the IDENTICAL f32 bits (shortest-form round-tripping is exact). ~1.8x
    /// smaller snapshots with no format change: still elephant-snapshot-v1 JSON.
    ///
    /// OFF by default because deployment certification compares the candidate's
    /// re-export against the quiesced export byte-content-wise: a binary that
    /// starts compacting mid-deploy fails its own certification against the old
    /// binary's fat export. Turn it on only when the running binary already has
    /// it, so both sides of the next deploy compact alike.
    fn compact_vector_export_enabled() -> bool {
        std::env::var("ELEPHANT_DB_COMPACT_VECTOR_EXPORT")
            .map(|value| value == "1" || value == "true")
            .unwrap_or(false)
    }

    /// Quantizes a row's `vector` components to their shortest f32 decimal form.
    fn compact_embedding_vector_row(row: &mut serde_json::Value) {
        let Some(vector) = row
            .get_mut("vector")
            .and_then(serde_json::Value::as_array_mut)
        else {
            return;
        };
        for component in vector.iter_mut() {
            if let Some(wide) = component.as_f64() {
                #[allow(clippy::cast_possible_truncation)]
                let quantized = wide as f32;
                if let Ok(narrow) = format!("{quantized}").parse::<f64>() {
                    if let Some(number) = serde_json::Number::from_f64(narrow) {
                        *component = serde_json::Value::Number(number);
                    }
                }
            }
        }
    }

    fn snapshot_payload(&self) -> SnapshotPayload {
        let mut table_names: Vec<_> = self.tables.keys().copied().collect();
        table_names.sort_by_key(|table| table.as_str().to_string());

        let compact_vectors = Self::compact_vector_export_enabled();
        let mut tables = HashMap::new();
        for table in table_names {
            let Some(rows) = self.tables.get(&table) else {
                continue;
            };
            let mut values: Vec<_> = rows.values().cloned().collect();
            if compact_vectors && table == TableName::ChunkEmbedding {
                for value in &mut values {
                    Self::compact_embedding_vector_row(value);
                }
            }
            values.sort_by_key(snapshot_sort_key);
            let _ = tables.insert(table.as_str().to_string(), values);
        }

        let mut extension_table_names: Vec<_> = self.extension_tables.keys().cloned().collect();
        extension_table_names.sort();
        for table_name in extension_table_names {
            let Some(rows) = self.extension_tables.get(&table_name) else {
                continue;
            };
            let mut values: Vec<_> = rows.values().cloned().collect();
            values.sort_by_key(|value| {
                extension_row_key_from_value(&table_name, value).unwrap_or_default()
            });
            let _ = tables.insert(table_name, values);
        }

        SnapshotPayload {
            backend: "elephant-db".to_string(),
            version: 1,
            exported_at: Utc::now(),
            tables,
        }
    }

    fn scan_extension_table(&self, table_name: &str) -> Vec<(String, serde_json::Value)> {
        let Some(table) = self.extension_tables.get(table_name) else {
            return Vec::new();
        };
        let mut rows: Vec<_> = table
            .iter()
            .map(|(row_key, row)| (row_key.clone(), row.clone()))
            .collect();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows
    }

    fn create_record<T>(&mut self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let value = serialize_record("create", &record)?;
        erasure_guard::check_erasure_guard(self, T::TABLE, &value, None)?;
        let id = record.base().id.clone();
        self.ensure_unique_constraints(T::TABLE, &id, &value)?;
        let table = self.tables.entry(T::TABLE).or_default();
        if table.contains_key(&id) {
            return Err(ElephantDbError::Conflict(id.to_string()));
        }
        let canonical_changed = fences_canonical_row(T::TABLE, &value);
        table.insert(id.clone(), value.clone());
        if let Err(err) = self.index_created_record(T::TABLE, &id, &value) {
            let _ = self.tables.entry(T::TABLE).or_default().remove(&id);
            let dirty_tables = HashSet::from([T::TABLE]);
            self.rebuild_for_tables(&dirty_tables)?;
            return Err(err);
        }
        if canonical_changed {
            self.advance_canonical_epoch(&record.base().space_id);
        }
        Ok(record)
    }

    fn replace_record<T>(&mut self, record: T) -> Result<T>
    where
        T: ElephantDbRecord,
    {
        let value = serialize_record("replace", &record)?;
        erasure_guard::check_erasure_guard(self, T::TABLE, &value, None)?;
        let id = record.base().id.clone();
        self.ensure_unique_constraints(T::TABLE, &id, &value)?;
        let table = self.tables.entry(T::TABLE).or_default();
        let Some(previous) = table.get(&id).cloned() else {
            return Err(ElephantDbError::NotFound(id.to_string()));
        };
        let previous_space = json_string_field(&previous, "space_id").map(str::to_owned);
        let canonical_changed =
            fences_canonical_row(T::TABLE, &previous) || fences_canonical_row(T::TABLE, &value);
        table.insert(id.clone(), value);
        let dirty_tables = HashSet::from([T::TABLE]);
        if let Err(err) = self.rebuild_for_tables(&dirty_tables) {
            let _ = self
                .tables
                .entry(T::TABLE)
                .or_default()
                .insert(id, previous);
            self.rebuild_for_tables(&dirty_tables)?;
            return Err(err);
        }
        if canonical_changed {
            if let Some(previous_space) =
                previous_space.filter(|space| space != &record.base().space_id)
            {
                self.advance_canonical_epoch(&previous_space);
            }
            self.advance_canonical_epoch(&record.base().space_id);
        }
        Ok(record)
    }

    fn index_created_record(
        &mut self,
        table: TableName,
        id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        match table {
            TableName::Doc => {
                self.insert_unique_string_index_entry(
                    table,
                    id,
                    value,
                    "rendition_key",
                    |unique| &mut unique.doc_rendition,
                )?;
                self.insert_doc_index(value)
            }
            TableName::EntityIdentityKey => {
                self.insert_unique_string_index_entry(table, id, value, "key", |unique| {
                    &mut unique.entity_identity_key
                })
            }
            TableName::Assertion => {
                self.insert_unique_string_index_entry(table, id, value, "assertion_key", |unique| {
                    &mut unique.assertion_key
                })
            }
            TableName::PredicateRegistry => {
                self.insert_unique_string_index_entry(table, id, value, "predicate_id", |unique| {
                    &mut unique.predicate_registry
                })
            }
            TableName::TruthSlot => {
                self.insert_unique_string_index_entry(table, id, value, "slot_key", |unique| {
                    &mut unique.truth_slot
                })
            }
            TableName::SystemEvent => {
                self.insert_unique_i64_index_entry(table, id, value, "seq", |unique| {
                    &mut unique.system_event_seq
                })
            }
            TableName::IdMap => self.insert_id_map_unique_index_entry(id, value),
            TableName::SpaceConfig => self.insert_space_config_unique_index_entry(id, value),
            TableName::Rel => self.insert_rel_index(value),
            TableName::DocRevision => self.insert_doc_revision_index(value),
            TableName::DocNode => self.insert_doc_node_index(value),
            TableName::ChunkEmbedding => self.insert_node_embedding_index(value),
            _ => Ok(()),
        }
    }

    fn ensure_unique_constraints(
        &self,
        table: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        match table {
            TableName::Doc => Self::ensure_unique_map_string_field(
                &self.indexes.unique.doc_rendition,
                table,
                target_id,
                value,
                "rendition_key",
                "doc rendition_key",
            ),
            TableName::EntityIdentityKey => Self::ensure_unique_map_string_field(
                &self.indexes.unique.entity_identity_key,
                table,
                target_id,
                value,
                "key",
                "entity_identity_key",
            ),
            TableName::Assertion => Self::ensure_unique_map_string_field(
                &self.indexes.unique.assertion_key,
                table,
                target_id,
                value,
                "assertion_key",
                "assertion assertion_key",
            ),
            TableName::PredicateRegistry => Self::ensure_unique_map_string_field(
                &self.indexes.unique.predicate_registry,
                table,
                target_id,
                value,
                "predicate_id",
                "predicate_registry",
            ),
            TableName::TruthSlot => Self::ensure_unique_map_string_field(
                &self.indexes.unique.truth_slot,
                table,
                target_id,
                value,
                "slot_key",
                "truth_slot slot_key",
            ),
            TableName::SystemEvent => Self::ensure_unique_map_i64_field(
                &self.indexes.unique.system_event_seq,
                table,
                target_id,
                value,
                "seq",
                "system_event sequence",
            ),
            TableName::IdMap => self.ensure_unique_id_map_lookup_fast(target_id, value),
            TableName::SpaceConfig => self.ensure_unique_space_config_fast(target_id, value),
            _ => Ok(()),
        }
    }

    fn ensure_unique_constraints_scan(
        &self,
        table: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        match table {
            TableName::Doc => self.ensure_unique_string_field_scan(
                table,
                target_id,
                value,
                "rendition_key",
                "doc rendition_key",
                true,
            ),
            TableName::EntityIdentityKey => self.ensure_unique_string_field_scan(
                table,
                target_id,
                value,
                "key",
                "entity_identity_key",
                true,
            ),
            TableName::Assertion => self.ensure_unique_string_field_scan(
                table,
                target_id,
                value,
                "assertion_key",
                "assertion assertion_key",
                true,
            ),
            TableName::PredicateRegistry => self.ensure_unique_string_field_scan(
                table,
                target_id,
                value,
                "predicate_id",
                "predicate_registry",
                true,
            ),
            TableName::TruthSlot => self.ensure_unique_string_field_scan(
                table,
                target_id,
                value,
                "slot_key",
                "truth_slot slot_key",
                true,
            ),
            TableName::SystemEvent => self.ensure_unique_i64_field_scan(
                table,
                target_id,
                value,
                "seq",
                "system_event sequence",
                true,
            ),
            TableName::IdMap => self.ensure_unique_id_map_lookup_scan(target_id, value),
            TableName::SpaceConfig => self.ensure_unique_space_config_scan(target_id, value),
            _ => Ok(()),
        }
    }

    fn ensure_unique_id_map_lookup_fast(
        &self,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id")
            .ok_or_else(|| ElephantDbError::DataIntegrity("id_map missing space_id".to_string()))?;
        let source_system = json_string_field(value, "source_system").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing source_system".to_string())
        })?;
        let legacy_id = json_string_field(value, "legacy_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing legacy_id".to_string())
        })?;
        let key = (
            space_id.to_string(),
            source_system.to_string(),
            legacy_id.to_string(),
        );
        if self
            .indexes
            .unique
            .id_map_lookup
            .get(&key)
            .is_some_and(|existing_id| existing_id != target_id)
        {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate id_map lookup in space {space_id}: {source_system}/{legacy_id}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_id_map_lookup_scan(
        &self,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id")
            .ok_or_else(|| ElephantDbError::DataIntegrity("id_map missing space_id".to_string()))?;
        let source_system = json_string_field(value, "source_system").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing source_system".to_string())
        })?;
        let legacy_id = json_string_field(value, "legacy_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing legacy_id".to_string())
        })?;
        let Some(table) = self.tables.get(&TableName::IdMap) else {
            return Ok(());
        };
        let duplicate = table
            .iter()
            .filter(|(id, _)| *id != target_id)
            .any(|(_, existing)| {
                if json_deleted(existing) {
                    return false;
                }
                json_string_field(existing, "space_id") == Some(space_id)
                    && json_string_field(existing, "source_system") == Some(source_system)
                    && json_string_field(existing, "legacy_id") == Some(legacy_id)
            });
        if duplicate {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate id_map lookup in space {space_id}: {source_system}/{legacy_id}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_space_config_fast(
        &self,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("space_config missing space_id".to_string())
        })?;
        if self
            .indexes
            .unique
            .space_config
            .get(space_id)
            .is_some_and(|existing_id| existing_id != target_id)
        {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate space_config for space {space_id}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_space_config_scan(
        &self,
        target_id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("space_config missing space_id".to_string())
        })?;
        let Some(table) = self.tables.get(&TableName::SpaceConfig) else {
            return Ok(());
        };
        let duplicate = table
            .iter()
            .filter(|(id, _)| *id != target_id)
            .any(|(_, existing)| {
                !json_deleted(existing) && json_string_field(existing, "space_id") == Some(space_id)
            });
        if duplicate {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate space_config for space {space_id}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_map_string_field(
        index: &HashMap<(String, String), RecordId>,
        table_name: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        label: &str,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_string_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        if index
            .get(&(space_id.to_string(), field_value.to_string()))
            .is_some_and(|existing_id| existing_id != target_id)
        {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {label} in space {space_id}: {field_value}"
            )));
        }
        Ok(())
    }

    fn insert_unique_string_index_entry<F>(
        &mut self,
        table_name: TableName,
        id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        index_map: F,
    ) -> Result<()>
    where
        F: Fn(&mut UniqueIndexState) -> &mut HashMap<(String, String), RecordId>,
    {
        if json_deleted(value) {
            return Ok(());
        }
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_string_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        let key = (space_id.to_string(), field_value.to_string());
        let index = index_map(&mut self.indexes.unique);
        if let Some(existing) = index.insert(key.clone(), id.clone()) {
            let _ = index.insert(key.clone(), existing.clone());
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {table_name} unique key {key:?}: {existing} vs {id}"
            )));
        }
        Ok(())
    }

    fn insert_unique_i64_index_entry<F>(
        &mut self,
        table_name: TableName,
        id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        index_map: F,
    ) -> Result<()>
    where
        F: Fn(&mut UniqueIndexState) -> &mut HashMap<(String, i64), RecordId>,
    {
        if json_deleted(value) {
            return Ok(());
        }
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_i64_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        let key = (space_id.to_string(), field_value);
        let index = index_map(&mut self.indexes.unique);
        if let Some(existing) = index.insert(key.clone(), id.clone()) {
            let _ = index.insert(key, existing.clone());
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {table_name} unique key in space {space_id}: {existing} vs {id}"
            )));
        }
        Ok(())
    }

    fn insert_id_map_unique_index_entry(
        &mut self,
        id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        if json_deleted(value) {
            return Ok(());
        }
        let space_id = json_string_field(value, "space_id")
            .ok_or_else(|| ElephantDbError::DataIntegrity("id_map missing space_id".to_string()))?;
        let source_system = json_string_field(value, "source_system").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing source_system".to_string())
        })?;
        let legacy_id = json_string_field(value, "legacy_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("id_map missing legacy_id".to_string())
        })?;
        let key = (
            space_id.to_string(),
            source_system.to_string(),
            legacy_id.to_string(),
        );
        if let Some(existing) = self
            .indexes
            .unique
            .id_map_lookup
            .insert(key.clone(), id.clone())
        {
            let _ = self
                .indexes
                .unique
                .id_map_lookup
                .insert(key, existing.clone());
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate id_map lookup in space {space_id}: {existing} vs {id}"
            )));
        }
        Ok(())
    }

    fn insert_space_config_unique_index_entry(
        &mut self,
        id: &RecordId,
        value: &serde_json::Value,
    ) -> Result<()> {
        if json_deleted(value) {
            return Ok(());
        }
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity("space_config missing space_id".to_string())
        })?;
        if let Some(existing) = self
            .indexes
            .unique
            .space_config
            .insert(space_id.to_string(), id.clone())
        {
            let _ = self
                .indexes
                .unique
                .space_config
                .insert(space_id.to_string(), existing.clone());
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate space_config for space {space_id}: {existing} vs {id}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_string_field_scan(
        &self,
        table_name: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        label: &str,
        ignore_soft_deleted: bool,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_string_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        let Some(table) = self.tables.get(&table_name) else {
            return Ok(());
        };
        let duplicate = table
            .iter()
            .filter(|(id, _)| *id != target_id)
            .any(|(_, existing)| {
                if ignore_soft_deleted && json_deleted(existing) {
                    return false;
                }
                json_string_field(existing, "space_id") == Some(space_id)
                    && json_string_field(existing, field_name) == Some(field_value)
            });
        if duplicate {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {label} in space {space_id}: {field_value}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_map_i64_field(
        index: &HashMap<(String, i64), RecordId>,
        table_name: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        label: &str,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_i64_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        if index
            .get(&(space_id.to_string(), field_value))
            .is_some_and(|existing_id| existing_id != target_id)
        {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {label} in space {space_id}: {field_value}"
            )));
        }
        Ok(())
    }

    fn ensure_unique_i64_field_scan(
        &self,
        table_name: TableName,
        target_id: &RecordId,
        value: &serde_json::Value,
        field_name: &str,
        label: &str,
        ignore_soft_deleted: bool,
    ) -> Result<()> {
        let space_id = json_string_field(value, "space_id").ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing space_id"))
        })?;
        let field_value = json_i64_field(value, field_name).ok_or_else(|| {
            ElephantDbError::DataIntegrity(format!("{table_name} missing {field_name}"))
        })?;
        let Some(table) = self.tables.get(&table_name) else {
            return Ok(());
        };
        let duplicate = table
            .iter()
            .filter(|(id, _)| *id != target_id)
            .any(|(_, existing)| {
                if ignore_soft_deleted && json_deleted(existing) {
                    return false;
                }
                json_string_field(existing, "space_id") == Some(space_id)
                    && json_i64_field(existing, field_name) == Some(field_value)
            });
        if duplicate {
            return Err(ElephantDbError::Conflict(format!(
                "Duplicate {label} in space {space_id}: {field_value}"
            )));
        }
        Ok(())
    }

    fn ensure_no_running_work_items(&self) -> Result<()> {
        let Some(table) = self.tables.get(&TableName::WorkItem) else {
            return Ok(());
        };
        let running = table
            .values()
            .filter(|value| {
                !json_deleted(value) && json_string_field(value, "status") == Some("running")
            })
            .count();
        if running > 0 {
            return Err(ElephantDbError::Snapshot(format!(
                "Cannot import snapshot containing {running} running work item(s)"
            )));
        }
        Ok(())
    }

    fn hard_delete_record<T>(&mut self, space_id: &str, id: &RecordId) -> Result<()>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get_mut(&T::TABLE) else {
            return Err(ElephantDbError::NotFound(id.to_string()));
        };
        let Some(value) = table.get(id) else {
            return Err(ElephantDbError::NotFound(id.to_string()));
        };
        let record = deserialize_record::<T>("hard_delete", value.clone())?;
        if record.base().space_id != space_id {
            return Err(ElephantDbError::NotFound(id.to_string()));
        }
        let removed = table
            .remove(id)
            .ok_or_else(|| ElephantDbError::NotFound(id.to_string()))?;
        let dirty_tables = HashSet::from([T::TABLE]);
        if let Err(err) = self.rebuild_for_tables(&dirty_tables) {
            let _ = self
                .tables
                .entry(T::TABLE)
                .or_default()
                .insert(id.clone(), removed);
            self.rebuild_for_tables(&dirty_tables)?;
            return Err(err);
        }
        if fences_canonical_row(T::TABLE, &removed) {
            self.advance_canonical_epoch(space_id);
        }
        Ok(())
    }

    fn get_visible_record<T>(
        &self,
        space_id: &str,
        id: &RecordId,
        include_deleted: bool,
    ) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get(&T::TABLE) else {
            return Ok(None);
        };
        let Some(value) = table.get(id) else {
            return Ok(None);
        };
        let record = deserialize_record::<T>("get", value.clone())?;
        if record.base().space_id != space_id {
            return Ok(None);
        }
        if !include_deleted && record.base().deleted_at.is_some() {
            return Ok(None);
        }
        Ok(Some(record))
    }

    fn get_visible_record_by_id<T>(&self, id: &RecordId, include_deleted: bool) -> Result<Option<T>>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get(&T::TABLE) else {
            return Ok(None);
        };
        let Some(value) = table.get(id) else {
            return Ok(None);
        };
        let record = deserialize_record::<T>("get_by_id", value.clone())?;
        if !include_deleted && record.base().deleted_at.is_some() {
            return Ok(None);
        }
        Ok(Some(record))
    }

    fn scan_table<T>(&self) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get(&T::TABLE) else {
            return Ok(Vec::new());
        };
        let mut rows = table
            .values()
            .cloned()
            .map(|value| deserialize_record::<T>("scan", value))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|left, right| compare_record_ids(&left.base().id, &right.base().id));
        Ok(rows)
    }

    fn work_items_by_status(
        &self,
        space_id: &str,
        status: &str,
        kind: Option<&str>,
        limit: usize,
        runnable_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<WorkItem>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(table) = self.tables.get(&TableName::WorkItem) else {
            return Ok(Vec::new());
        };
        let mut rows = table
            .values()
            .filter(|value| work_item_json_matches(value, space_id, status, kind, runnable_at))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| compare_work_item_json_order(left, right));
        rows.into_iter()
            .take(limit)
            .cloned()
            .map(|value| deserialize_record::<WorkItem>("work_items_by_status", value))
            .collect()
    }

    fn collect_records_by_ids<T>(&self, ids: &[RecordId]) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get(&T::TABLE) else {
            return Ok(Vec::new());
        };
        ids.iter()
            .filter_map(|id| table.get(id).cloned())
            .map(|value| deserialize_record::<T>("collect_records_by_ids", value))
            .collect()
    }

    fn collect_visible_records_by_ids<T>(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> Result<Vec<T>>
    where
        T: ElephantDbRecord,
    {
        let Some(table) = self.tables.get(&T::TABLE) else {
            return Ok(Vec::new());
        };

        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(value) = table.get(id).cloned() else {
                continue;
            };
            let row = deserialize_record::<T>("collect_visible_records_by_ids", value)?;
            if row.base().space_id != space_id {
                continue;
            }
            if !include_deleted && row.base().deleted_at.is_some() {
                continue;
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn search_doc_ids_page(
        &self,
        space_id: &str,
        query: Option<&str>,
        include_deleted: bool,
        cursor: Option<&RecordId>,
        limit: usize,
    ) -> (Vec<RecordId>, usize) {
        let Some(docs) = self.indexes.docs_by_space.get(space_id) else {
            return (Vec::new(), 0);
        };

        let Some(normalized_query) = query.map(str::to_lowercase) else {
            return collect_matching_doc_ids(docs, include_deleted, cursor, limit, None);
        };

        let query_tokens = tokenize(&normalized_query);
        if query_tokens.is_empty() {
            return collect_matching_doc_ids(
                docs,
                include_deleted,
                cursor,
                limit,
                Some(&normalized_query),
            );
        }

        let mut scored_ids =
            score_matching_doc_ids(docs, &normalized_query, query_tokens, include_deleted);
        let total = scored_ids.len();
        if limit == 0 {
            return (Vec::new(), total);
        }

        if let Some(cursor_id) = cursor {
            scored_ids.retain(|(id, _)| compare_record_ids(id, cursor_id).is_gt());
        }

        if scored_ids.len() > limit {
            let split_index = limit - 1;
            let _ = scored_ids.select_nth_unstable_by(split_index, compare_scored_doc_ids_desc);
            scored_ids.truncate(limit);
        }
        scored_ids.sort_by(compare_scored_doc_ids_desc);
        (scored_ids.into_iter().map(|(id, _)| id).collect(), total)
    }

    fn rebuild_all_indexes_with_options(&mut self, rebuild_vector_indexes: bool) -> Result<()> {
        self.rebuild_unique_indexes()?;
        self.rebuild_graph_indexes()?;
        self.rebuild_doc_indexes()?;
        if rebuild_vector_indexes {
            self.rebuild_vector_indexes()?;
        }
        Ok(())
    }

    fn validate_unique_constraints(&self) -> Result<()> {
        let mut state = Self {
            tables: self.tables.clone(),
            extension_tables: self.extension_tables.clone(),
            indexes: IndexState::default(),
            reference_guard: self.reference_guard.clone(),
        };
        state.rebuild_unique_indexes()?;
        Ok(())
    }

    fn rebuild_for_tables(&mut self, dirty_tables: &HashSet<TableName>) -> Result<()> {
        if dirty_tables.is_empty() {
            return Ok(());
        }

        self.rebuild_unique_indexes_for_tables(dirty_tables)?;

        if dirty_tables.contains(&TableName::Rel) {
            self.rebuild_graph_indexes()?;
        }

        if dirty_tables.contains(&TableName::Doc)
            || dirty_tables.contains(&TableName::DocNode)
            || dirty_tables.contains(&TableName::DocRevision)
        {
            self.rebuild_doc_indexes()?;
        }

        if dirty_tables.contains(&TableName::ChunkEmbedding)
            || dirty_tables.contains(&TableName::DocNode)
            || dirty_tables.contains(&TableName::DocRevision)
        {
            self.rebuild_vector_indexes()?;
        }

        Ok(())
    }

    fn rebuild_unique_indexes(&mut self) -> Result<()> {
        self.indexes.unique = UniqueIndexState::default();

        self.rebuild_unique_string_index(TableName::Doc, "rendition_key", |unique| {
            &mut unique.doc_rendition
        })?;
        self.rebuild_unique_string_index(TableName::EntityIdentityKey, "key", |unique| {
            &mut unique.entity_identity_key
        })?;
        self.rebuild_unique_string_index(TableName::Assertion, "assertion_key", |unique| {
            &mut unique.assertion_key
        })?;
        self.rebuild_unique_string_index(TableName::PredicateRegistry, "predicate_id", |unique| {
            &mut unique.predicate_registry
        })?;
        self.rebuild_unique_string_index(TableName::TruthSlot, "slot_key", |unique| {
            &mut unique.truth_slot
        })?;
        self.rebuild_unique_i64_index(TableName::SystemEvent, "seq", |unique| {
            &mut unique.system_event_seq
        })?;
        self.rebuild_unique_id_map_index()?;
        self.rebuild_unique_space_config_index()?;
        Ok(())
    }

    fn rebuild_unique_indexes_for_tables(
        &mut self,
        dirty_tables: &HashSet<TableName>,
    ) -> Result<()> {
        if dirty_tables.contains(&TableName::Doc) {
            self.rebuild_unique_string_index(TableName::Doc, "rendition_key", |unique| {
                &mut unique.doc_rendition
            })?;
        }
        if dirty_tables.contains(&TableName::EntityIdentityKey) {
            self.rebuild_unique_string_index(TableName::EntityIdentityKey, "key", |unique| {
                &mut unique.entity_identity_key
            })?;
        }
        if dirty_tables.contains(&TableName::Assertion) {
            self.rebuild_unique_string_index(TableName::Assertion, "assertion_key", |unique| {
                &mut unique.assertion_key
            })?;
        }
        if dirty_tables.contains(&TableName::PredicateRegistry) {
            self.rebuild_unique_string_index(
                TableName::PredicateRegistry,
                "predicate_id",
                |unique| &mut unique.predicate_registry,
            )?;
        }
        if dirty_tables.contains(&TableName::TruthSlot) {
            self.rebuild_unique_string_index(TableName::TruthSlot, "slot_key", |unique| {
                &mut unique.truth_slot
            })?;
        }
        if dirty_tables.contains(&TableName::SystemEvent) {
            self.rebuild_unique_i64_index(TableName::SystemEvent, "seq", |unique| {
                &mut unique.system_event_seq
            })?;
        }
        if dirty_tables.contains(&TableName::IdMap) {
            self.rebuild_unique_id_map_index()?;
        }
        if dirty_tables.contains(&TableName::SpaceConfig) {
            self.rebuild_unique_space_config_index()?;
        }
        Ok(())
    }

    fn rebuild_unique_string_index<F>(
        &mut self,
        table: TableName,
        field_name: &str,
        index_map: F,
    ) -> Result<()>
    where
        F: Fn(&mut UniqueIndexState) -> &mut HashMap<(String, String), RecordId>,
    {
        let rows = self.tables.get(&table).cloned().unwrap_or_default();
        let index = index_map(&mut self.indexes.unique);
        index.clear();
        for (id, value) in rows {
            if json_deleted(&value) {
                continue;
            }
            let space_id = json_string_field(&value, "space_id").ok_or_else(|| {
                ElephantDbError::DataIntegrity(format!("{table} missing space_id"))
            })?;
            let field_value = json_string_field(&value, field_name).ok_or_else(|| {
                ElephantDbError::DataIntegrity(format!("{table} missing {field_name}"))
            })?;
            let key = (space_id.to_string(), field_value.to_string());
            if let Some(existing) = index.insert(key.clone(), id.clone()) {
                return Err(ElephantDbError::Conflict(format!(
                    "Duplicate {table} unique key {key:?}: {existing} vs {id}"
                )));
            }
        }
        Ok(())
    }

    fn rebuild_unique_i64_index<F>(
        &mut self,
        table: TableName,
        field_name: &str,
        index_map: F,
    ) -> Result<()>
    where
        F: Fn(&mut UniqueIndexState) -> &mut HashMap<(String, i64), RecordId>,
    {
        let rows = self.tables.get(&table).cloned().unwrap_or_default();
        let index = index_map(&mut self.indexes.unique);
        index.clear();
        for (id, value) in rows {
            if json_deleted(&value) {
                continue;
            }
            let space_id = json_string_field(&value, "space_id").ok_or_else(|| {
                ElephantDbError::DataIntegrity(format!("{table} missing space_id"))
            })?;
            let field_value = json_i64_field(&value, field_name).ok_or_else(|| {
                ElephantDbError::DataIntegrity(format!("{table} missing {field_name}"))
            })?;
            let key = (space_id.to_string(), field_value);
            if let Some(existing) = index.insert(key, id.clone()) {
                return Err(ElephantDbError::Conflict(format!(
                    "Duplicate {table} unique key in space {space_id}: {existing} vs {id}"
                )));
            }
        }
        Ok(())
    }

    fn rebuild_unique_id_map_index(&mut self) -> Result<()> {
        let rows = self
            .tables
            .get(&TableName::IdMap)
            .cloned()
            .unwrap_or_default();
        self.indexes.unique.id_map_lookup.clear();
        for (id, value) in rows {
            if json_deleted(&value) {
                continue;
            }
            let space_id = json_string_field(&value, "space_id").ok_or_else(|| {
                ElephantDbError::DataIntegrity("id_map missing space_id".to_string())
            })?;
            let source_system = json_string_field(&value, "source_system").ok_or_else(|| {
                ElephantDbError::DataIntegrity("id_map missing source_system".to_string())
            })?;
            let legacy_id = json_string_field(&value, "legacy_id").ok_or_else(|| {
                ElephantDbError::DataIntegrity("id_map missing legacy_id".to_string())
            })?;
            let key = (
                space_id.to_string(),
                source_system.to_string(),
                legacy_id.to_string(),
            );
            if let Some(existing) = self.indexes.unique.id_map_lookup.insert(key, id.clone()) {
                return Err(ElephantDbError::Conflict(format!(
                    "Duplicate id_map lookup in space {space_id}: {existing} vs {id}"
                )));
            }
        }
        Ok(())
    }

    fn rebuild_unique_space_config_index(&mut self) -> Result<()> {
        let rows = self
            .tables
            .get(&TableName::SpaceConfig)
            .cloned()
            .unwrap_or_default();
        self.indexes.unique.space_config.clear();
        for (id, value) in rows {
            if json_deleted(&value) {
                continue;
            }
            let space_id = json_string_field(&value, "space_id").ok_or_else(|| {
                ElephantDbError::DataIntegrity("space_config missing space_id".to_string())
            })?;
            if let Some(existing) = self
                .indexes
                .unique
                .space_config
                .insert(space_id.to_string(), id.clone())
            {
                return Err(ElephantDbError::Conflict(format!(
                    "Duplicate space_config for space {space_id}: {existing} vs {id}"
                )));
            }
        }
        Ok(())
    }

    fn rebuild_graph_indexes(&mut self) -> Result<()> {
        self.indexes.graph_by_space.clear();
        for rel in self.scan_table::<Rel>()? {
            let value = serialize_record("rebuild_graph_indexes", &rel)?;
            self.insert_rel_index(&value)?;
        }
        Ok(())
    }

    fn rebuild_doc_indexes(&mut self) -> Result<()> {
        self.indexes.docs_by_space.clear();

        for doc in self.scan_table::<Doc>()? {
            let value = serialize_record("rebuild_doc_indexes_doc", &doc)?;
            self.insert_doc_index(&value)?;
        }

        for revision in self.scan_table::<DocRevision>()? {
            let value = serialize_record("rebuild_doc_indexes_revision", &revision)?;
            self.insert_doc_revision_index(&value)?;
        }

        for node in self.scan_table::<DocNode>()? {
            let value = serialize_record("rebuild_doc_indexes_node", &node)?;
            self.insert_doc_node_index(&value)?;
        }

        Ok(())
    }

    fn rebuild_vector_indexes(&mut self) -> Result<()> {
        self.indexes.vector_partitions.clear();

        for embedding in self.scan_table::<NodeEmbedding>()? {
            let value = serialize_record("rebuild_vector_indexes", &embedding)?;
            self.insert_node_embedding_index(&value)?;
        }

        Ok(())
    }

    fn doc_nodes_for_revision(
        &self,
        space_id: &str,
        revision_id: &RecordId,
        chunks_only: bool,
    ) -> Result<Vec<DocNode>> {
        let Some(docs) = self.indexes.docs_by_space.get(space_id) else {
            return Ok(Vec::new());
        };
        let ids: Vec<_> = if chunks_only {
            docs.chunks_by_revision
                .get(revision_id)
                .into_iter()
                .flatten()
                .map(|entry| entry.node_id.clone())
                .collect()
        } else {
            docs.nodes_by_revision
                .get(revision_id)
                .into_iter()
                .flatten()
                .map(|entry| entry.node_id.clone())
                .collect()
        };
        self.collect_records_by_ids::<DocNode>(&ids)
    }

    fn doc_parent_chain(&self, space_id: &str, node_id: &RecordId) -> Result<Vec<DocNode>> {
        let Some(current) = self.get_visible_record::<DocNode>(space_id, node_id, false)? else {
            return Ok(Vec::new());
        };
        let Some(docs) = self.indexes.docs_by_space.get(space_id) else {
            return Ok(Vec::new());
        };

        let mut chain_ids = Vec::new();
        let mut parent_id = current.parent.as_ref().map(|parent| parent.id().clone());

        while let Some(id) = parent_id {
            let parent = self
                .get_visible_record::<DocNode>(space_id, &id, false)?
                .ok_or_else(|| {
                    ElephantDbError::DataIntegrity(format!(
                        "parent node {id} not found for chain walk"
                    ))
                })?;
            chain_ids.push(parent.base.id.clone());
            parent_id = docs.parent_by_node.get(&id).cloned();
        }

        chain_ids.reverse();
        self.collect_records_by_ids::<DocNode>(&chain_ids)
    }

    fn insert_rel_index(&mut self, value: &serde_json::Value) -> Result<()> {
        let rel = deserialize_record::<Rel>("insert_rel_index", value.clone())?;
        if rel.base.deleted_at.is_some() {
            return Ok(());
        }

        let space = self
            .indexes
            .graph_by_space
            .entry(rel.base.space_id.clone())
            .or_default();
        let subject = space.ensure_node(rel.subject.id());
        let object = space.ensure_node(rel.object.id());
        let edge_index = space.graph.add_edge(
            subject,
            object,
            GraphEdge {
                rel_id: rel.base.id.clone(),
                rel_type: rel.rel_type.clone(),
            },
        );
        let _ = space.edge_by_rel_id.insert(rel.base.id.clone(), edge_index);
        Ok(())
    }

    fn insert_doc_index(&mut self, value: &serde_json::Value) -> Result<()> {
        let doc = deserialize_record::<Doc>("insert_doc_index", value.clone())?;
        let docs = self
            .indexes
            .docs_by_space
            .entry(doc.base.space_id.clone())
            .or_default();
        if let Some(existing) = docs.doc_search.remove(&doc.base.id) {
            remove_doc_search_entry(docs, &doc.base.id, &existing);
        }
        let search_entry = build_doc_search_entry(&doc);
        add_doc_search_entry(docs, doc.base.id.clone(), search_entry);
        Ok(())
    }

    fn insert_doc_revision_index(&mut self, value: &serde_json::Value) -> Result<()> {
        let revision =
            deserialize_record::<DocRevision>("insert_doc_revision_index", value.clone())?;
        if revision.base.deleted_at.is_some() {
            return Ok(());
        }

        let docs = self
            .indexes
            .docs_by_space
            .entry(revision.base.space_id.clone())
            .or_default();
        let _ = docs
            .doc_by_revision
            .insert(revision.base.id.clone(), revision.doc.id().clone());
        Ok(())
    }

    fn insert_doc_node_index(&mut self, value: &serde_json::Value) -> Result<()> {
        let node = deserialize_record::<DocNode>("insert_doc_node_index", value.clone())?;
        if node.base.deleted_at.is_some() {
            return Ok(());
        }

        let docs = self
            .indexes
            .docs_by_space
            .entry(node.base.space_id.clone())
            .or_default();
        let order = DocNodeOrder {
            node_id: node.base.id.clone(),
            depth: node.depth,
            position: node.position,
        };
        insert_ordered_node(
            docs.nodes_by_revision
                .entry(node.revision.id().clone())
                .or_default(),
            order.clone(),
            false,
        );
        if node.role == elephant_types::records::doc_node::NodeRole::Chunk {
            insert_ordered_node(
                docs.chunks_by_revision
                    .entry(node.revision.id().clone())
                    .or_default(),
                order,
                true,
            );
        }
        if let Some(parent) = &node.parent {
            let _ = docs
                .parent_by_node
                .insert(node.base.id.clone(), parent.id().clone());
        }
        let _ = docs
            .node_revision
            .insert(node.base.id.clone(), node.revision.id().clone());
        let _ = docs
            .node_collection
            .insert(node.base.id.clone(), node.collection);
        Ok(())
    }

    fn insert_node_embedding_index(&mut self, value: &serde_json::Value) -> Result<()> {
        let embedding =
            deserialize_record::<NodeEmbedding>("insert_node_embedding_index", value.clone())?;
        let node_id = embedding.node.id().clone();
        let (doc_id, collection) = self
            .indexes
            .docs_by_space
            .get(&embedding.base.space_id)
            .map_or((None, None), |docs| {
                let doc_id = docs
                    .node_revision
                    .get(&node_id)
                    .and_then(|revision_id| docs.doc_by_revision.get(revision_id))
                    .cloned();
                let collection = docs.node_collection.get(&node_id).cloned().flatten();
                (doc_id, collection)
            });
        let partition_name = vector_partition_name(
            &embedding.base.security.policy_id,
            embedding.base.security.level,
            &embedding.model,
            embedding.vector.len(),
        );
        let partition = self
            .indexes
            .vector_partitions
            .entry(partition_name)
            .or_insert_with(|| VectorPartitionIndex {
                dims: embedding.vector.len(),
                ..VectorPartitionIndex::default()
            });
        let space_partition = partition
            .spaces
            .entry(embedding.base.space_id.clone())
            .or_default();
        let vector_entry = VectorEntry {
            node_id,
            doc_id,
            collection,
            deleted: embedding.base.deleted_at.is_some(),
            normalized_vector: normalize_vector(&embedding.vector).into_boxed_slice(),
        };

        if let Some(index) = space_partition
            .entry_index_by_embedding_id
            .get(&embedding.base.id)
            .copied()
        {
            space_partition.entries[index] = vector_entry;
        } else {
            let index = space_partition.entries.len();
            space_partition.entries.push(vector_entry);
            let _ = space_partition
                .entry_index_by_embedding_id
                .insert(embedding.base.id.clone(), index);
        }
        Ok(())
    }
}

fn insert_ordered_node(entries: &mut Vec<DocNodeOrder>, entry: DocNodeOrder, position_only: bool) {
    let compare = |left: &DocNodeOrder, right: &DocNodeOrder| {
        if position_only {
            left.position
                .cmp(&right.position)
                .then_with(|| compare_record_ids(&left.node_id, &right.node_id))
        } else {
            left.depth
                .cmp(&right.depth)
                .then_with(|| left.position.cmp(&right.position))
                .then_with(|| compare_record_ids(&left.node_id, &right.node_id))
        }
    };

    if entries
        .last()
        .is_none_or(|last| compare(last, &entry) != std::cmp::Ordering::Greater)
    {
        entries.push(entry);
        return;
    }

    let index = entries
        .binary_search_by(|candidate| compare(candidate, &entry))
        .unwrap_or_else(|index| index);
    entries.insert(index, entry);
}

fn serialize_record<T>(context: &str, value: &T) -> Result<serde_json::Value>
where
    T: Serialize,
{
    serde_json::to_value(value).map_err(|err| {
        ElephantDbError::Serialization(format!(
            "failed to serialize embedded record for {context}: {err}"
        ))
    })
}

fn deserialize_record<T>(context: &str, value: serde_json::Value) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(value).map_err(|err| {
        ElephantDbError::Serialization(format!(
            "failed to deserialize embedded record for {context}: {err}"
        ))
    })
}

fn record_id_from_value(value: &serde_json::Value) -> Result<RecordId> {
    let Some(id_value) = value.get("id").cloned() else {
        return Err(ElephantDbError::Snapshot(
            "snapshot row missing `id`".to_string(),
        ));
    };
    serde_json::from_value(id_value)
        .map_err(|err| ElephantDbError::Snapshot(format!("failed to decode record id: {err}")))
}

fn extension_row_key_from_value(table_name: &str, value: &serde_json::Value) -> Result<String> {
    if value
        .get("space_id")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(ElephantDbError::Snapshot(format!(
            "extension row for `{table_name}` is missing string `space_id`"
        )));
    }

    let Some(id_value) = value.get("id") else {
        return Err(ElephantDbError::Snapshot(format!(
            "snapshot row missing `id` for extension table `{table_name}`"
        )));
    };

    match id_value {
        serde_json::Value::String(id) => {
            if id.contains(':') {
                Ok(id.clone())
            } else {
                Ok(format!("{table_name}:{}", extension_string_key(id)))
            }
        }
        serde_json::Value::Object(map) => {
            let table = map
                .get("tb")
                .or_else(|| map.get("table"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(table_name);
            let raw_id = map.get("id").ok_or_else(|| {
                ElephantDbError::Snapshot(format!(
                    "snapshot row missing nested `id` for extension table `{table_name}`"
                ))
            })?;
            let id = match raw_id {
                serde_json::Value::String(id) => extension_string_key(id),
                serde_json::Value::Number(id) => id.to_string(),
                other => {
                    return Err(ElephantDbError::Snapshot(format!(
                        "unsupported extension row id shape for `{table_name}`: {other}"
                    )))
                }
            };
            Ok(format!("{table}:{id}"))
        }
        serde_json::Value::Number(id) => Ok(format!("{table_name}:{id}")),
        other => Err(ElephantDbError::Snapshot(format!(
            "unsupported extension row id shape for `{table_name}`: {other}"
        ))),
    }
}

// Numeric JSON strings are native string keys, not native integer keys. Keep
// older nonnumeric opaque key spelling; quoting numeric strings removes the
// otherwise destructive collision across supported extension identity shapes.
fn extension_string_key(id: &str) -> String {
    if id.is_empty()
        || id.parse::<i64>().is_ok()
        || id.bytes().all(|byte| byte.is_ascii_digit() || byte == b'_')
    {
        format!("`{id}`")
    } else {
        id.to_owned()
    }
}

fn snapshot_sort_key(value: &serde_json::Value) -> String {
    value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map_or_else(String::new, str::to_string)
}

fn compare_record_ids(left: &RecordId, right: &RecordId) -> std::cmp::Ordering {
    left.table()
        .as_str()
        .cmp(right.table().as_str())
        .then_with(|| left.id().cmp(&right.id()))
}

fn compare_embedding_hits_desc(
    left: &NodeEmbeddingSearchHit,
    right: &NodeEmbeddingSearchHit,
) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| compare_record_ids(&left.node_id, &right.node_id))
}

fn collect_matching_doc_ids(
    docs: &DocSpaceIndex,
    include_deleted: bool,
    cursor: Option<&RecordId>,
    limit: usize,
    substring: Option<&str>,
) -> (Vec<RecordId>, usize) {
    let mut ids: Vec<_> = docs
        .doc_search
        .iter()
        .filter(|(_, entry)| include_deleted || !entry.deleted)
        .filter(|(_, entry)| {
            substring.is_none_or(|needle| entry.normalized_search_text.contains(needle))
        })
        .map(|(id, _)| id.clone())
        .collect();
    ids.sort_by(compare_record_ids);
    let total = ids.len();
    if let Some(cursor_id) = cursor {
        ids.retain(|id| compare_record_ids(id, cursor_id).is_gt());
    }
    if ids.len() > limit {
        ids.truncate(limit);
    }
    (ids, total)
}

fn score_matching_doc_ids(
    docs: &DocSpaceIndex,
    normalized_query: &str,
    query_tokens: Vec<String>,
    include_deleted: bool,
) -> Vec<(RecordId, f32)> {
    let unique_query_tokens: HashSet<_> = query_tokens.into_iter().collect();
    let visible_doc_count = docs
        .doc_search
        .values()
        .filter(|entry| include_deleted || !entry.deleted)
        .count()
        .max(1);
    #[allow(clippy::cast_precision_loss)]
    let doc_count = visible_doc_count as f32;
    let visible_total_token_count = if include_deleted {
        docs.total_token_count
    } else {
        docs.doc_search
            .values()
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.token_count)
            .sum()
    };
    let query_terms: Vec<_> = unique_query_tokens
        .iter()
        .filter_map(|token| {
            docs.token_postings.get(token).and_then(|postings| {
                let df = postings
                    .iter()
                    .filter(|candidate_id| {
                        docs.doc_search
                            .get(*candidate_id)
                            .is_some_and(|entry| include_deleted || !entry.deleted)
                    })
                    .count();
                if df == 0 {
                    return None;
                }
                #[allow(clippy::cast_precision_loss)]
                let df = df as f32;
                let idf = ((doc_count - df + 0.5) / (df + 0.5)).ln_1p();
                Some((token.clone(), idf))
            })
        })
        .collect();

    let avg_doc_len = avg_doc_len(visible_total_token_count, visible_doc_count);
    let candidate_capacity = query_terms
        .iter()
        .filter_map(|(token, _)| {
            docs.token_postings.get(token).map(|postings| {
                postings
                    .iter()
                    .filter(|candidate_id| {
                        docs.doc_search
                            .get(*candidate_id)
                            .is_some_and(|entry| include_deleted || !entry.deleted)
                    })
                    .count()
            })
        })
        .sum::<usize>()
        .min(docs.doc_search.len());
    let mut scored_ids: HashMap<&RecordId, f32> = HashMap::with_capacity(candidate_capacity);

    for (token, idf) in &query_terms {
        let Some(postings) = docs.token_postings.get(token) else {
            continue;
        };

        for candidate_id in postings {
            let Some(entry) = docs.doc_search.get(candidate_id) else {
                continue;
            };
            if !include_deleted && entry.deleted {
                continue;
            }

            let Some(tf) = entry.token_counts.get(token).copied() else {
                continue;
            };

            let score = bm25_term_score(tf, entry.token_count, avg_doc_len, *idf);
            scored_ids
                .entry(candidate_id)
                .and_modify(|current| *current += score)
                .or_insert(score);
        }
    }

    if !scored_ids.is_empty() && !normalized_query.is_empty() {
        #[allow(clippy::cast_precision_loss)]
        let phrase_bonus = query_terms.len() as f32 * 0.5;
        for (candidate_id, score) in &mut scored_ids {
            if docs
                .doc_search
                .get(*candidate_id)
                .is_some_and(|entry| entry.normalized_search_text.contains(normalized_query))
            {
                *score += phrase_bonus;
            }
        }
    }

    scored_ids
        .into_iter()
        .filter(|(_, score)| *score > 0.0)
        .map(|(id, score)| (id.clone(), score))
        .collect()
}

fn compare_scored_doc_ids_desc(
    left: &(RecordId, f32),
    right: &(RecordId, f32),
) -> std::cmp::Ordering {
    right
        .1
        .total_cmp(&left.1)
        .then_with(|| compare_record_ids(&left.0, &right.0))
}

fn avg_doc_len(total_token_count: usize, doc_count: usize) -> f32 {
    #[allow(clippy::cast_precision_loss)]
    {
        (total_token_count.max(1) as f32) / (doc_count.max(1) as f32)
    }
}

fn bm25_term_score(tf: usize, doc_token_count: usize, avg_doc_len: f32, idf: f32) -> f32 {
    #[allow(clippy::cast_precision_loss)]
    let tf = tf as f32;
    #[allow(clippy::cast_precision_loss)]
    let entry_len = doc_token_count.max(1) as f32;
    let doc_len_ratio = entry_len / avg_doc_len.max(1.0);
    let length_penalty = 0.75f32.mul_add(doc_len_ratio, 0.25);
    let norm = 1.2f32.mul_add(length_penalty, tf);
    idf * ((tf * 2.2) / norm)
}

fn build_normalized_doc_search_text(doc: &Doc) -> String {
    let mut text = String::with_capacity(doc.title.len() + doc.text.len() + 1);
    text.push_str(&doc.title);
    text.push('\n');
    text.push_str(&doc.text);
    text.to_lowercase()
}

fn hex_encode(input: &str) -> String {
    let mut output = String::with_capacity(input.len() * 2);
    for byte in input.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn vector_partition_name(policy_id: &str, level: i32, model: &str, dims: usize) -> String {
    format!(
        "emb__{}__{}__{}__{}",
        hex_encode(policy_id),
        hex_encode(&format!("L{level}")),
        hex_encode(model),
        dims
    )
}

fn build_doc_search_entry(doc: &Doc) -> DocSearchEntry {
    let normalized_search_text = build_normalized_doc_search_text(doc);
    let mut token_counts = HashMap::new();
    let mut token_count = 0_usize;

    for_each_token(&normalized_search_text, |token| {
        token_count += 1;
        *token_counts.entry(token.to_string()).or_insert(0) += 1;
    });

    DocSearchEntry {
        normalized_search_text,
        token_counts,
        token_count,
        deleted: doc.base.deleted_at.is_some(),
    }
}

fn add_doc_search_entry(docs: &mut DocSpaceIndex, doc_id: RecordId, entry: DocSearchEntry) {
    docs.total_token_count += entry.token_count;
    for token in entry.token_counts.keys() {
        let _ = docs
            .token_postings
            .entry(token.clone())
            .or_default()
            .insert(doc_id.clone());
    }
    let _ = docs.doc_search.insert(doc_id, entry);
}

fn remove_doc_search_entry(docs: &mut DocSpaceIndex, doc_id: &RecordId, entry: &DocSearchEntry) {
    docs.total_token_count = docs.total_token_count.saturating_sub(entry.token_count);
    let mut empty_tokens = Vec::new();
    for token in entry.token_counts.keys() {
        let Some(postings) = docs.token_postings.get_mut(token) else {
            continue;
        };
        let _ = postings.remove(doc_id);
        if postings.is_empty() {
            empty_tokens.push(token.clone());
        }
    }
    for token in empty_tokens {
        let _ = docs.token_postings.remove(&token);
    }
}

/// Tokenizes text for deterministic lexical scoring or synthetic embeddings.
#[must_use]
pub fn tokenize(text: &str) -> Vec<String> {
    let normalized = text.to_lowercase();
    let mut tokens = Vec::new();
    for_each_token(&normalized, |token| tokens.push(token.to_string()));
    tokens
}

/// Scores a haystack against query tokens using a simple deterministic lexical heuristic.
#[must_use]
pub fn lexical_score(query: &str, haystacks: &[(&str, f32)]) -> f32 {
    let query_tokens = tokenize(query);
    if query_tokens.is_empty() {
        return 0.0;
    }

    let mut score = 0.0_f32;
    for (haystack, weight) in haystacks {
        if haystack.is_empty() {
            continue;
        }
        let normalized = haystack.to_lowercase();
        let mut token_set = HashSet::new();
        for_each_token(&normalized, |token| {
            let _ = token_set.insert(token.to_string());
        });

        for token in &query_tokens {
            if normalized == *token {
                score += 3.0 * *weight;
            } else if normalized.contains(token) {
                score += 1.5 * *weight;
            }

            if token_set.contains(token.as_str()) {
                score += *weight;
            }
        }
    }

    score
}

fn json_string_field<'a>(value: &'a serde_json::Value, field_name: &str) -> Option<&'a str> {
    value.get(field_name)?.as_str()
}

fn json_i64_field(value: &serde_json::Value, field_name: &str) -> Option<i64> {
    value.get(field_name)?.as_i64()
}

fn json_deleted(value: &serde_json::Value) -> bool {
    value
        .get("deleted_at")
        .is_some_and(|deleted_at| !deleted_at.is_null())
}

fn work_item_json_matches(
    value: &serde_json::Value,
    space_id: &str,
    status: &str,
    kind: Option<&str>,
    runnable_at: Option<DateTime<Utc>>,
) -> bool {
    json_string_field(value, "space_id") == Some(space_id)
        && !json_deleted(value)
        && json_string_field(value, "status") == Some(status)
        && kind.is_none_or(|expected| json_string_field(value, "kind") == Some(expected))
        && runnable_at.is_none_or(|now| work_item_run_after_ready(value, now))
}

fn work_item_run_after_ready(value: &serde_json::Value, now: DateTime<Utc>) -> bool {
    let Some(run_after) = value.get("run_after") else {
        return true;
    };
    if run_after.is_null() {
        return true;
    }
    let Some(raw) = run_after.as_str() else {
        return true;
    };
    DateTime::parse_from_rfc3339(raw).map_or(true, |parsed| parsed.with_timezone(&Utc) <= now)
}

fn compare_work_item_json_order(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> std::cmp::Ordering {
    let left_priority = json_i64_field(left, "priority").unwrap_or_default();
    let right_priority = json_i64_field(right, "priority").unwrap_or_default();
    right_priority
        .cmp(&left_priority)
        .then_with(|| {
            json_string_field(left, "created_at")
                .unwrap_or_default()
                .cmp(json_string_field(right, "created_at").unwrap_or_default())
        })
        .then_with(|| {
            json_string_field(left, "id")
                .unwrap_or_default()
                .cmp(json_string_field(right, "id").unwrap_or_default())
        })
}

/// Exact cosine similarity over two vectors.
#[must_use]
pub fn cosine_similarity(lhs: &[f32], rhs: &[f32]) -> f32 {
    if lhs.is_empty() || rhs.is_empty() || lhs.len() != rhs.len() {
        return 0.0;
    }

    let (dot, lhs_norm, rhs_norm) = lhs.iter().zip(rhs.iter()).fold(
        (0.0_f32, 0.0_f32, 0.0_f32),
        |(dot, lhs_norm, rhs_norm), (left, right)| {
            (
                dot + (left * right),
                lhs_norm + (left * left),
                rhs_norm + (right * right),
            )
        },
    );

    if lhs_norm == 0.0 || rhs_norm == 0.0 {
        return 0.0;
    }

    dot / (lhs_norm.sqrt() * rhs_norm.sqrt())
}

/// Deterministically synthesizes a text embedding for approximate semantic search.
#[must_use]
pub fn hashed_text_embedding(text: &str, dims: usize) -> Vec<f32> {
    if dims == 0 {
        return Vec::new();
    }

    let mut vector = vec![0.0_f32; dims];
    for_each_token(text, |token| {
        let hash = hash_token(token);
        #[allow(clippy::cast_possible_truncation)]
        let index = (hash as usize) % dims;
        let sign = if (hash >> 63) == 0 { 1.0_f32 } else { -1.0_f32 };
        vector[index] += sign;
    });

    normalize_vector_in_place(&mut vector);
    vector
}

fn dot_similarity(lhs: &[f32], rhs: &[f32]) -> f32 {
    if lhs.len() != rhs.len() {
        return 0.0;
    }

    let mut sum0 = 0.0_f32;
    let mut sum1 = 0.0_f32;
    let mut sum2 = 0.0_f32;
    let mut sum3 = 0.0_f32;
    let mut index = 0_usize;
    while index + 4 <= lhs.len() {
        sum0 += lhs[index] * rhs[index];
        sum1 += lhs[index + 1] * rhs[index + 1];
        sum2 += lhs[index + 2] * rhs[index + 2];
        sum3 += lhs[index + 3] * rhs[index + 3];
        index += 4;
    }

    let mut sum = sum0 + sum1 + sum2 + sum3;
    while index < lhs.len() {
        sum += lhs[index] * rhs[index];
        index += 1;
    }

    sum
}

fn normalize_vector(values: &[f32]) -> Vec<f32> {
    if values.is_empty() {
        return Vec::new();
    }

    let norm_sq = squared_norm(values);
    if norm_sq == 0.0 {
        return vec![0.0_f32; values.len()];
    }

    let inv_norm = norm_sq.sqrt().recip();
    let mut normalized = Vec::with_capacity(values.len());
    normalized.extend(values.iter().map(|value| value * inv_norm));
    normalized
}

fn normalize_vector_in_place(values: &mut [f32]) {
    let norm_sq = squared_norm(values);
    if norm_sq == 0.0 {
        return;
    }

    let inv_norm = norm_sq.sqrt().recip();
    for value in values {
        *value *= inv_norm;
    }
}

fn squared_norm(values: &[f32]) -> f32 {
    let mut sum0 = 0.0_f32;
    let mut sum1 = 0.0_f32;
    let mut sum2 = 0.0_f32;
    let mut sum3 = 0.0_f32;
    let mut index = 0_usize;
    while index + 4 <= values.len() {
        sum0 += values[index] * values[index];
        sum1 += values[index + 1] * values[index + 1];
        sum2 += values[index + 2] * values[index + 2];
        sum3 += values[index + 3] * values[index + 3];
        index += 4;
    }

    let mut sum = sum0 + sum1 + sum2 + sum3;
    while index < values.len() {
        sum += values[index] * values[index];
        index += 1;
    }

    sum
}

fn for_each_token(text: &str, mut f: impl FnMut(&str)) {
    for token in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        f(token);
    }
}

fn hash_token(token: &str) -> u64 {
    // FNV-1a keeps synthetic embeddings stable across processes and runs.
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;

    token.bytes().fold(OFFSET_BASIS, |hash, byte| {
        let normalized = byte.to_ascii_lowercase();
        (hash ^ u64::from(normalized)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::too_many_lines
    )]

    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::records::entity::EntityType;
    use elephant_types::Link;

    fn base_for(table: TableName, space_id: &str) -> BaseFields {
        let mut base = BaseFields::new(
            RecordId::new_v4(table),
            space_id.to_string(),
            "test".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        base
    }

    fn entity(space_id: &str, name: &str) -> Entity {
        Entity::new(
            base_for(TableName::Entity, space_id),
            EntityType::new("person"),
            name.to_string(),
        )
    }

    fn rel(space_id: &str, subject: &RecordId, object: &RecordId, rel_type: &str) -> Rel {
        Rel {
            base: base_for(TableName::Rel, space_id),
            subject: Link::new(subject.clone()),
            object: Link::new(object.clone()),
            rel_type: rel_type.to_string(),
            status: elephant_types::records::rel::ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: Vec::new(),
            temporal_audit: None,
        }
    }

    fn revision(space_id: &str, doc_id: &RecordId) -> DocRevision {
        DocRevision {
            base: base_for(TableName::DocRevision, space_id),
            doc: Link::new(doc_id.clone()),
            text: "Alice saw the White Rabbit near the rabbit hole.".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn doc_node(
        space_id: &str,
        revision_id: &RecordId,
        parent: Option<&RecordId>,
        role: NodeRole,
        position: i32,
        depth: i32,
        text: Option<&str>,
        collection: Option<&str>,
    ) -> DocNode {
        DocNode {
            base: base_for(TableName::DocNode, space_id),
            revision: Link::new(revision_id.clone()),
            role,
            span: TextSpan::new(0, 10),
            node_key: format!("node-{position}-{depth}"),
            text: text.map(str::to_string),
            parent: parent.cloned().map(Link::new),
            position,
            depth,
            embedding_model: None,
            collection: collection.map(str::to_string),
        }
    }

    fn embedding(space_id: &str, node_id: &RecordId, text: &str, dims: usize) -> NodeEmbedding {
        NodeEmbedding {
            base: base_for(TableName::ChunkEmbedding, space_id),
            node: Link::new(node_id.clone()),
            model: "hashed".to_string(),
            dims: i32::try_from(dims).unwrap(),
            vector: hashed_text_embedding(text, dims),
        }
    }

    /// The env flag must stay honest under concurrency: these three cases run
    /// in ONE test because Rust tests share a process and env mutation races.
    #[test]
    fn compact_vector_export_quantizes_exactly_and_only_when_enabled() {
        let db = ElephantDb::new();
        let node_id = RecordId::new_v4(TableName::DocNode);
        let mut record = embedding("space", &node_id, "compact me", 4);
        // The classic inflation case: f32 0.544 widens to 0.5440000295639038.
        record.vector = vec![0.544_f32, 1.0, -0.25, 3.402_823_5e38];
        let original: Vec<f32> = record.vector.clone();
        db.create(record).unwrap();

        // OFF (default): the export carries the widened f64 text.
        std::env::remove_var("ELEPHANT_DB_COMPACT_VECTOR_EXPORT");
        let fat = String::from_utf8(db.export_snapshot_bytes().unwrap()).unwrap();
        assert!(
            fat.contains("0.5440000295639038"),
            "without the flag the widened form must be preserved byte-for-byte, \
             because deploy certification compares export content across binaries"
        );

        // ON: shortest f32 decimals, ~1.8x smaller text for real vectors.
        std::env::set_var("ELEPHANT_DB_COMPACT_VECTOR_EXPORT", "1");
        let slim = String::from_utf8(db.export_snapshot_bytes().unwrap()).unwrap();
        std::env::remove_var("ELEPHANT_DB_COMPACT_VECTOR_EXPORT");
        assert!(!slim.contains("0.5440000295639038"));
        assert!(slim.contains("0.544"), "shortest form must appear");

        // EXACTNESS is the whole claim: re-import the compact snapshot and the
        // vector must reproduce the original f32 BITS, not merely close values.
        let restored = ElephantDb::new();
        restored.import_snapshot_bytes(slim.as_bytes()).unwrap();
        let rows = restored.scan::<NodeEmbedding>().unwrap();
        assert_eq!(rows.len(), 1);
        for (index, (a, b)) in original.iter().zip(rows[0].vector.iter()).enumerate() {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "component {index} must round-trip bit-exactly ({a} vs {b})"
            );
        }
    }

    #[test]
    fn graph_index_supports_n_hops() {
        let db = ElephantDb::new();
        let alice = entity("space", "Alice");
        let rabbit = entity("space", "White Rabbit");
        let queen = entity("space", "Queen");

        db.create(alice.clone()).unwrap();
        db.create(rabbit.clone()).unwrap();
        db.create(queen.clone()).unwrap();
        db.create(rel("space", &alice.base.id, &rabbit.base.id, "knows"))
            .unwrap();
        db.create(rel("space", &rabbit.base.id, &queen.base.id, "serves"))
            .unwrap();

        let neighbors = db.graph_neighbors("space", &alice.base.id, 2).unwrap();
        assert_eq!(neighbors.len(), 2);
        assert_eq!(neighbors[0].entity_id, rabbit.base.id);
        assert_eq!(neighbors[0].distance, 1);
        assert_eq!(neighbors[1].entity_id, queen.base.id);
        assert_eq!(neighbors[1].distance, 2);
    }

    #[test]
    fn doc_tree_indexes_support_revision_queries_and_parent_chains() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Alice".to_string(),
            text: "Alice saw the White Rabbit.".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "doc-1".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("storybook"),
        );
        let chunk = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            1,
            1,
            Some("Alice saw the White Rabbit."),
            Some("storybook"),
        );
        let revision_id = revision.base.id.clone();

        db.create(doc).unwrap();
        db.create(revision).unwrap();
        db.create(root.clone()).unwrap();
        db.create(chunk.clone()).unwrap();

        let nodes = db.doc_nodes_for_revision("space", &revision_id).unwrap();
        assert_eq!(nodes.len(), 2);
        let chunks = db.doc_chunks_for_revision("space", &revision_id).unwrap();
        assert_eq!(chunks, vec![chunk.clone()]);
        let chain = db.doc_parent_chain("space", &chunk.base.id).unwrap();
        assert_eq!(chain, vec![root]);
    }

    #[test]
    fn snapshot_roundtrip_preserves_extension_tables() {
        let db = ElephantDb::new();
        db.tx(|tx| {
            tx.upsert_extension_row(
                "sampleapp_todo",
                serde_json::json!({
                    "id": "sampleapp_todo:todo-1",
                    "space_id": "space",
                    "owner": "entity:550e8400-e29b-41d4-a716-446655440000",
                    "secondary": ["entity:550e8400-e29b-41d4-a716-446655440001"]
                }),
            )?;
            Ok::<_, ElephantDbError>(())
        })
        .unwrap();

        let snapshot = db.export_snapshot_bytes().unwrap();
        let restored = ElephantDb::from_snapshot_bytes(&snapshot).unwrap();
        let rows = restored.scan_extension_rows("sampleapp_todo").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "sampleapp_todo:todo-1");
        assert_eq!(
            rows[0].1["owner"],
            serde_json::json!("entity:550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn snapshot_import_rejects_duplicate_row_ids() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "doc": [
                    {"id": "doc:00000000-0000-0000-0000-000000000001"},
                    {"id": "doc:00000000-0000-0000-0000-000000000001"}
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("duplicate ids should be rejected");
        assert!(matches!(error, ElephantDbError::Snapshot(_)));
        assert!(error.to_string().contains("duplicate snapshot row id"));
        assert!(error
            .to_string()
            .contains("doc:00000000-0000-0000-0000-000000000001"));
    }

    #[test]
    fn snapshot_import_rejects_duplicate_extension_row_keys() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "sampleapp_todo": [
                    {"id": "todo-1", "space_id": "space"},
                    {"id": "sampleapp_todo:todo-1", "space_id": "space"}
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("duplicate extension row keys should be rejected");
        assert!(matches!(error, ElephantDbError::Snapshot(_)));
        assert!(error
            .to_string()
            .contains("duplicate snapshot extension row key"));
        assert!(error.to_string().contains("sampleapp_todo:todo-1"));
    }

    #[test]
    fn snapshot_import_rejects_extension_rows_without_space_id() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "sampleapp_todo": [
                    {"id": "todo-1"}
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("missing space_id should be rejected");
        assert!(matches!(error, ElephantDbError::Snapshot(_)));
        assert!(error.to_string().contains("space_id"));
    }

    #[test]
    fn snapshot_import_rejects_duplicate_doc_unique_keys() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "doc": [
                    {
                        "id": "doc:00000000-0000-0000-0000-000000000010",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "provenance": {"sources": [], "chain": []},
                        "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                        "subjects": [],
                        "subjects_state": "unknown",
                        "title": "One",
                        "text": "Doc one",
                        "source": serde_json::Value::Null,
                        "rendition_key": "duplicate-key",
                        "metadata": {}
                    },
                    {
                        "id": "doc:00000000-0000-0000-0000-000000000011",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "provenance": {"sources": [], "chain": []},
                        "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                        "subjects": [],
                        "subjects_state": "unknown",
                        "title": "Two",
                        "text": "Doc two",
                        "source": serde_json::Value::Null,
                        "rendition_key": "duplicate-key",
                        "metadata": {}
                    }
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("duplicate doc unique key should be rejected");
        assert!(matches!(error, ElephantDbError::Conflict(_)));
        assert!(error.to_string().contains("Duplicate doc"));
    }

    #[test]
    fn snapshot_import_allows_reusing_doc_unique_keys_after_soft_delete() {
        let db = ElephantDb::new();
        let deleted_at = Utc::now();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "doc": [
                    {
                        "id": "doc:00000000-0000-0000-0000-000000000012",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": deleted_at,
                        "provenance": {"sources": [], "chain": []},
                        "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                        "subjects": [],
                        "subjects_state": "unknown",
                        "title": "Deleted",
                        "text": "Deleted doc",
                        "source": serde_json::Value::Null,
                        "rendition_key": "duplicate-key",
                        "metadata": {}
                    },
                    {
                        "id": "doc:00000000-0000-0000-0000-000000000013",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "provenance": {"sources": [], "chain": []},
                        "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                        "subjects": [],
                        "subjects_state": "unknown",
                        "title": "Live",
                        "text": "Live doc",
                        "source": serde_json::Value::Null,
                        "rendition_key": "duplicate-key",
                        "metadata": {}
                    }
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        db.import_snapshot_bytes(&bytes)
            .expect("soft-deleted docs should not block unique key reuse");
        let state = db.read_state();
        assert_eq!(state.indexes.unique.doc_rendition.len(), 1);
        drop(state);
        let ids = db.search_doc_ids("space", Some("doc"), true).unwrap();
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn snapshot_import_rejects_duplicate_id_map_lookup_keys() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "id_map": [
                    {
                        "id": "id_map:00000000-0000-0000-0000-000000000021",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "legacy_id": "legacy-1",
                        "record": "entity:00000000-0000-0000-0000-000000000099",
                        "source_system": "crm"
                    },
                    {
                        "id": "id_map:00000000-0000-0000-0000-000000000022",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "legacy_id": "legacy-1",
                        "record": "entity:00000000-0000-0000-0000-000000000100",
                        "source_system": "crm"
                    }
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("duplicate id_map lookup should be rejected");
        assert!(matches!(error, ElephantDbError::Conflict(_)));
        assert!(error.to_string().contains("Duplicate id_map lookup"));
    }

    #[test]
    fn snapshot_import_rejects_running_work_items() {
        let db = ElephantDb::new();
        let payload = serde_json::json!({
            "backend": "elephant-db",
            "version": 1,
            "exported_at": Utc::now(),
            "tables": {
                "work_item": [
                    {
                        "id": "work_item:00000000-0000-0000-0000-000000000031",
                        "space_id": "space",
                        "created_at": Utc::now(),
                        "updated_at": Utc::now(),
                        "created_by": "tester",
                        "deleted_at": serde_json::Value::Null,
                        "kind": "embed",
                        "payload": {},
                        "status": "running",
                        "priority": 0,
                        "idempotency_key": "running-import",
                        "attempts": 1,
                        "lease_expires_at": serde_json::Value::Null,
                        "leased_by": "worker-1",
                        "run_after": serde_json::Value::Null,
                        "last_error": serde_json::Value::Null
                    }
                ]
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let error = db
            .import_snapshot_bytes(&bytes)
            .expect_err("running work items should be rejected");
        assert!(matches!(error, ElephantDbError::Snapshot(_)));
        assert!(error.to_string().contains("running work item"));
    }

    #[test]
    fn tx_rolls_back_rows_on_error() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Rollback".to_string(),
            text: "rollback me".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "rollback-doc".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let doc_id = doc.base.id.clone();

        let result = db.tx(|tx| {
            tx.create(doc)?;
            Err::<(), ElephantDbError>(ElephantDbError::Conflict("boom".to_string()))
        });
        assert!(matches!(result, Err(ElephantDbError::Conflict(_))));
        assert!(db.get::<Doc>("space", &doc_id, true).unwrap().is_none());
    }

    #[test]
    fn soft_deleted_unique_keys_do_not_block_recreate() {
        let db = ElephantDb::new();
        let mut deleted = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Deleted".to_string(),
            text: "deleted".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "reusable-rendition-key".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let deleted_id = deleted.base.id.clone();

        db.create(deleted.clone()).unwrap();
        deleted.base.deleted_at = Some(Utc::now());
        db.replace(deleted).unwrap();

        let recreated = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Recreated".to_string(),
            text: "recreated".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "reusable-rendition-key".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let recreated_id = recreated.base.id.clone();
        db.create(recreated).unwrap();

        assert!(db.get::<Doc>("space", &deleted_id, true).unwrap().is_some());
        assert!(db
            .get::<Doc>("space", &recreated_id, false)
            .unwrap()
            .is_some());
    }

    #[test]
    fn created_doc_is_immediately_lexically_searchable() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Sample debug context_fact category project debug-probe-exact"
                .to_string(),
            text: "context_fact category project marker debug-probe-exact immediate lexical search check"
                .to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "debug-probe-exact".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let doc_id = doc.base.id.clone();

        db.create(doc).unwrap();

        let (ids, total) = db
            .search_doc_ids_page(
                "space",
                Some("context_fact category project debug-probe-exact"),
                false,
                None,
                10,
            )
            .unwrap();
        assert!(total > 0);
        assert!(ids.contains(&doc_id));
    }

    #[test]
    fn vector_partition_search_returns_best_matching_doc() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Alice".to_string(),
            text: "Alice saw the White Rabbit near the rabbit hole.".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "doc-1".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("storybook"),
        );
        let chunk_a = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            1,
            1,
            Some("Alice followed the White Rabbit into the rabbit hole."),
            Some("storybook"),
        );
        let chunk_b = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            2,
            1,
            Some("The Queen played croquet in the garden."),
            Some("storybook"),
        );

        db.create(doc.clone()).unwrap();
        db.create(revision).unwrap();
        db.create(root).unwrap();
        db.create(chunk_a.clone()).unwrap();
        db.create(chunk_b.clone()).unwrap();

        let dims = 64;
        let emb_a = embedding("space", &chunk_a.base.id, "white rabbit rabbit hole", dims);
        let emb_b = embedding("space", &chunk_b.base.id, "queen croquet garden", dims);
        let partition_name = vector_partition_name("default", 0, "hashed", dims);

        db.create(emb_a).unwrap();
        db.create(emb_b).unwrap();

        let hits = db
            .search_node_embeddings(
                &partition_name,
                "space",
                &hashed_text_embedding("rabbit hole", dims),
                5,
                Some("storybook"),
                false,
            )
            .unwrap();

        assert!(!hits.is_empty());
        assert_eq!(hits[0].node_id, chunk_a.base.id);
        assert_eq!(hits[0].doc_id, doc.base.id);
    }

    #[test]
    fn snapshot_roundtrip_preserves_indexes() {
        let db = ElephantDb::new();
        let alice = entity("space", "Alice");
        let rabbit = entity("space", "White Rabbit");
        db.create(alice.clone()).unwrap();
        db.create(rabbit.clone()).unwrap();
        db.create(rel("space", &alice.base.id, &rabbit.base.id, "knows"))
            .unwrap();

        let bytes = db.export_snapshot_bytes().unwrap();
        let restored = ElephantDb::from_snapshot_bytes(&bytes).unwrap();
        let neighbors = restored
            .graph_neighbors("space", &alice.base.id, 1)
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].entity_id, rabbit.base.id);
    }

    #[test]
    fn ranked_doc_search_total_is_stable_across_cursor_pages() {
        let db = ElephantDb::new();
        for index in 0..3 {
            let doc = Doc {
                base: base_for(TableName::Doc, "space"),
                title: format!("Rabbit note {index}"),
                text: "white rabbit rabbit hole".to_string(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: format!("doc-{index}"),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            };
            db.create(doc.clone()).unwrap();
        }

        let (first_page, total) = db
            .search_doc_ids_page("space", Some("rabbit"), false, None, 3)
            .unwrap();
        assert_eq!(total, 3);
        let cursor = first_page.first().cloned().expect("at least one hit");

        let (_later_page, later_total) = db
            .search_doc_ids_page("space", Some("rabbit"), false, Some(&cursor), 3)
            .unwrap();
        assert_eq!(later_total, 3);
    }

    #[test]
    fn deleted_docs_do_not_affect_visible_doc_search_ranking() {
        let query = "white rabbit";

        let baseline = ElephantDb::new();
        baseline
            .create(Doc {
                base: base_for(TableName::Doc, "space"),
                title: "Exact phrase".to_string(),
                text: "white rabbit".to_string(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: "baseline-exact".to_string(),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            })
            .unwrap();
        baseline
            .create(Doc {
                base: base_for(TableName::Doc, "space"),
                title: "Loose mention".to_string(),
                text: "rabbit in the garden".to_string(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: "baseline-loose".to_string(),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            })
            .unwrap();
        let (baseline_ids, baseline_total) = baseline
            .search_doc_ids_page("space", Some(query), false, None, 10)
            .unwrap();

        let with_deleted = ElephantDb::new();
        let exact = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Exact phrase".to_string(),
            text: "white rabbit".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "with-deleted-exact".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let exact_id = exact.base.id.clone();
        with_deleted.create(exact).unwrap();
        with_deleted
            .create(Doc {
                base: base_for(TableName::Doc, "space"),
                title: "Loose mention".to_string(),
                text: "rabbit in the garden".to_string(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: "with-deleted-loose".to_string(),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            })
            .unwrap();
        let deleted = with_deleted
            .create(Doc {
                base: base_for(TableName::Doc, "space"),
                title: "Deleted spam".to_string(),
                text: "rabbit ".repeat(512),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: "with-deleted-spam".to_string(),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            })
            .unwrap();
        let mut deleted = deleted;
        deleted.base.deleted_at = Some(Utc::now());
        with_deleted.replace(deleted).unwrap();

        let (visible_ids, visible_total) = with_deleted
            .search_doc_ids_page("space", Some(query), false, None, 10)
            .unwrap();

        assert_eq!(baseline_total, visible_total);
        assert_eq!(visible_ids.len(), baseline_ids.len());
        assert_eq!(visible_ids.first(), Some(&exact_id));
    }

    #[test]
    fn vector_search_dimension_mismatch_errors() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Alice".to_string(),
            text: "Alice saw the White Rabbit.".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "doc-dims".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("storybook"),
        );
        let chunk = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            1,
            1,
            Some("Alice saw the White Rabbit."),
            Some("storybook"),
        );
        let dims = 64;
        let partition_name = vector_partition_name("default", 0, "hashed", dims);

        db.create(doc).unwrap();
        db.create(revision).unwrap();
        db.create(root).unwrap();
        db.create(chunk.clone()).unwrap();
        db.create(embedding(
            "space",
            &chunk.base.id,
            "white rabbit rabbit hole",
            dims,
        ))
        .unwrap();

        let error = db
            .search_node_embeddings(&partition_name, "space", &[0.0; 16], 5, None, false)
            .expect_err("dimension mismatch should error");
        assert!(error.to_string().contains("vector dimension mismatch"));
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn wasm_smoke_snapshot_and_graph() {
        let db = ElephantDb::new();
        let alice = entity("space", "Alice");
        let rabbit = entity("space", "White Rabbit");
        db.create(alice.clone()).unwrap();
        db.create(rabbit.clone()).unwrap();
        db.create(rel("space", &alice.base.id, &rabbit.base.id, "knows"))
            .unwrap();

        let bytes = db.export_snapshot_bytes().unwrap();
        let restored = ElephantDb::from_snapshot_bytes(&bytes).unwrap();
        let hits = restored
            .graph_neighbors("space", &alice.base.id, 1)
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn wasm_doc_tree_indexes_support_revision_queries_and_parent_chains() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Alice".to_string(),
            text: "Alice saw the White Rabbit.".to_string(),
            source: None,
            rendition_key: "doc-1".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("storybook"),
        );
        let chunk = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            1,
            1,
            Some("Alice saw the White Rabbit."),
            Some("storybook"),
        );

        db.create(doc).unwrap();
        db.create(revision.clone()).unwrap();
        db.create(root.clone()).unwrap();
        db.create(chunk.clone()).unwrap();

        let nodes = db
            .doc_nodes_for_revision("space", &revision.base.id)
            .unwrap();
        assert_eq!(nodes.len(), 2);
        let chunks = db
            .doc_chunks_for_revision("space", &revision.base.id)
            .unwrap();
        assert_eq!(chunks, vec![chunk.clone()]);
        let chain = db.doc_parent_chain("space", &chunk.base.id).unwrap();
        assert_eq!(chain, vec![root]);
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn wasm_vector_partition_search_returns_best_matching_doc() {
        let db = ElephantDb::new();
        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Alice".to_string(),
            text: "Alice saw the White Rabbit near the rabbit hole.".to_string(),
            source: None,
            rendition_key: "doc-1".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("storybook"),
        );
        let chunk_a = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            1,
            1,
            Some("Alice followed the White Rabbit into the rabbit hole."),
            Some("storybook"),
        );
        let chunk_b = doc_node(
            "space",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            2,
            1,
            Some("The Queen played croquet in the garden."),
            Some("storybook"),
        );

        db.create(doc.clone()).unwrap();
        db.create(revision.clone()).unwrap();
        db.create(root).unwrap();
        db.create(chunk_a.clone()).unwrap();
        db.create(chunk_b.clone()).unwrap();

        let dims = 64;
        let emb_a = embedding("space", &chunk_a.base.id, "white rabbit rabbit hole", dims);
        let emb_b = embedding("space", &chunk_b.base.id, "queen croquet garden", dims);
        let partition_name = vector_partition_name("default", 0, "hashed", dims);

        db.create(emb_a).unwrap();
        db.create(emb_b).unwrap();

        let hits = db
            .search_node_embeddings(
                &partition_name,
                "space",
                &hashed_text_embedding("rabbit hole", dims),
                5,
                Some("storybook"),
                false,
            )
            .unwrap();

        assert!(!hits.is_empty());
        assert_eq!(hits[0].node_id, chunk_a.base.id);
        assert_eq!(hits[0].doc_id, doc.base.id);
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn wasm_benchmark_report_graph_vector_snapshot() {
        let db = ElephantDb::new();
        let initial_pages = core::arch::wasm32::memory_size(0);
        let now_ms = || js_sys::Date::now();

        let mut entity_ids = Vec::with_capacity(1_500);
        let started = now_ms();
        for index in 0..1_500 {
            let item = entity("space", &format!("entity-{index}"));
            entity_ids.push(item.base.id.clone());
            db.create(item).unwrap();
        }
        for index in 0..1_500 {
            for offset in 1..=4 {
                let target = &entity_ids[(index + offset) % entity_ids.len()];
                db.create(rel("space", &entity_ids[index], target, "linked"))
                    .unwrap();
            }
        }
        let graph_build_ms = now_ms() - started;

        let started = now_ms();
        let graph_hits = db
            .graph_neighbors("space", &entity_ids[entity_ids.len() / 2], 3)
            .unwrap();
        let graph_query_ms = now_ms() - started;
        let after_graph_pages = core::arch::wasm32::memory_size(0);

        let doc = Doc {
            base: base_for(TableName::Doc, "space"),
            title: "Benchmark Story".to_string(),
            text: "Alice followed the White Rabbit.".to_string(),
            source: None,
            rendition_key: "bench-doc".to_string(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let revision = revision("space", &doc.base.id);
        let root = doc_node(
            "space",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("benchmark"),
        );
        db.create(doc).unwrap();
        db.create(revision.clone()).unwrap();
        db.create(root.clone()).unwrap();

        let dims = 128;
        let started = now_ms();
        for index in 0..1_500 {
            let text = if index % 12 == 0 {
                format!("white rabbit rabbit hole wonderland chunk {index}")
            } else {
                format!("garden croquet benchmark chunk {index}")
            };
            let chunk = doc_node(
                "space",
                &revision.base.id,
                Some(&root.base.id),
                NodeRole::Chunk,
                index,
                1,
                Some(&text),
                Some("benchmark"),
            );
            let node_id = chunk.base.id.clone();
            db.create(chunk).unwrap();
            db.create(embedding("space", &node_id, &text, dims))
                .unwrap();
        }
        let vector_build_ms = now_ms() - started;

        let partition_name = vector_partition_name("default", 0, "hashed", dims);
        let query = hashed_text_embedding("white rabbit rabbit hole", dims);
        let started = now_ms();
        let vector_hits = db
            .search_node_embeddings(
                &partition_name,
                "space",
                &query,
                10,
                Some("benchmark"),
                false,
            )
            .unwrap();
        let vector_search_ms = now_ms() - started;
        let after_vector_pages = core::arch::wasm32::memory_size(0);

        let started = now_ms();
        let snapshot = db.export_snapshot_bytes().unwrap();
        let snapshot_export_ms = now_ms() - started;
        let snapshot_bytes = snapshot.len();

        let started = now_ms();
        let restored = ElephantDb::from_snapshot_bytes(&snapshot).unwrap();
        let snapshot_import_ms = now_ms() - started;
        let restored_hits = restored
            .graph_neighbors("space", &entity_ids[entity_ids.len() / 2], 3)
            .unwrap();
        let final_pages = core::arch::wasm32::memory_size(0);

        assert!(!graph_hits.is_empty());
        assert!(!vector_hits.is_empty());
        assert_eq!(graph_hits.len(), restored_hits.len());

        let report = serde_json::json!({
            "initial_pages": initial_pages,
            "after_graph_pages": after_graph_pages,
            "after_vector_pages": after_vector_pages,
            "final_pages": final_pages,
            "linear_memory_bytes": {
                "initial": u64::try_from(initial_pages).unwrap_or(u64::MAX) * 65_536,
                "after_graph": u64::try_from(after_graph_pages).unwrap_or(u64::MAX) * 65_536,
                "after_vector": u64::try_from(after_vector_pages).unwrap_or(u64::MAX) * 65_536,
                "final": u64::try_from(final_pages).unwrap_or(u64::MAX) * 65_536,
            },
            "graph": {
                "entity_count": entity_ids.len(),
                "edge_count": entity_ids.len() * 4,
                "hit_count": graph_hits.len(),
                "build_ms": graph_build_ms,
                "query_ms": graph_query_ms,
            },
            "vector": {
                "chunk_count": 1_500,
                "dims": dims,
                "hit_count": vector_hits.len(),
                "build_ms": vector_build_ms,
                "search_ms": vector_search_ms,
            },
            "snapshot": {
                "bytes": snapshot_bytes,
                "export_ms": snapshot_export_ms,
                "import_ms": snapshot_import_ms,
            }
        });
        web_sys::console::log_1(&wasm_bindgen::JsValue::from_str(&format!(
            "wasm-benchmark:{report}"
        )));
    }
}

#[cfg(test)]
mod extension_page_tests {
    use super::*;

    #[test]
    fn pages_are_tenant_scoped_stable_and_bounded() -> Result<()> {
        let db = ElephantDb::default();
        db.tx(|tx| {
            for index in (0..105).rev() {
                tx.upsert_extension_row(
                    "inventory_fixture",
                    serde_json::json!({
                        "id":format!("inventory_fixture:{index:03}"),
                        "space_id":"alpha", "payload":"opaque"
                    }),
                )?;
            }
            tx.upsert_extension_row(
                "inventory_fixture",
                serde_json::json!({
                    "id":"inventory_fixture:000-foreign", "space_id":"beta", "payload":"private"
                }),
            )?;
            Ok::<_, ElephantDbError>(())
        })?;
        let first = db.extension_rows_page("inventory_fixture", "alpha", None, 2)?;
        assert_eq!(
            first
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            ["inventory_fixture:000", "inventory_fixture:001"]
        );
        let second = db.extension_rows_page("inventory_fixture", "alpha", Some(&first[1].0), 2)?;
        assert_eq!(second[0].0, "inventory_fixture:002");
        assert_eq!(
            db.extension_rows_page("inventory_fixture", "alpha", None, usize::MAX)?
                .len(),
            101
        );
        assert!(db
            .extension_rows_page("inventory_fixture", "missing", None, 2)?
            .is_empty());
        assert!(db
            .extension_rows_page("inventory_fixture", "alpha", None, 0)?
            .is_empty());
        Ok(())
    }
}
