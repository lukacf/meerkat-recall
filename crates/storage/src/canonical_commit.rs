//! Complete canonical effects, outbox, and receipts in one backend transaction.
//!
//! Preparation captures the space epoch before reading. Every competing
//! canonical mutation advances that epoch, including records absent from the
//! preparation result. Publication compares and writes the epoch, fencing
//! predicate phantoms as well as changed rows on both backends.

use chrono::{DateTime, Utc};
use elephant_types::publication::{AttemptFence, PublicationFence, SourceHead, SourceVisibility};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{
    Assertion, Attribute, AuditLog, ConflictGroup, Doc, DocNode, DocRevision, Entity,
    EntityIdentityKey, EntityMerge, Event, Freshness, IdMap, IdentityCandidate,
    KnowledgeViewDefinition, PredicateAlias, PredicateRegistry, Rel, ReprocessJob,
    ResolutionStatus, Source, SystemEvent, TruthSlot, WorkArtifact, WorkItem, WorkItemArchive,
    WorkRun, WorkRunArchive,
};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use surrealdb::sql::Value;

use crate::source_publication::{
    conflict, control_thing, control_value, encode_payload, head_key, integrity, publication_error,
    read_control_tx, require_space, stable_key, write_control_tx, SourcePublicationRepository,
    HEAD_TABLE,
};
use crate::{EmbeddedMutation, StorageClient, StorageResult};

#[path = "canonical_audit_authority.rs"]
mod audit_authority;

pub use audit_authority::inherit_maintenance_audit_authority;

/// A typed canonical row; converters preserve native database representations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "table", content = "record", rename_all = "snake_case")]
pub enum CanonicalRecord {
    /// An audit row committed with the effect it certifies.
    AuditLog(Box<AuditLog>),
    /// Legacy mappings rewritten atomically with explicit identity merges.
    IdMap(Box<IdMap>),
    /// Durable selection intent and progress for retained reprocessing.
    ReprocessJob(Box<ReprocessJob>),
    /// A retained lifecycle EntityMerge row.
    EntityMerge(Box<EntityMerge>),
    /// A retained lifecycle IdentityCandidate row.
    IdentityCandidate(Box<IdentityCandidate>),
    /// A retained lifecycle SystemEvent row.
    SystemEvent(Box<SystemEvent>),
    /// A complete Entity row.
    Entity(Box<Entity>),
    /// A complete EntityIdentityKey row.
    EntityIdentityKey(Box<EntityIdentityKey>),
    /// A complete Assertion row.
    Assertion(Box<Assertion>),
    /// A complete Rel row.
    Rel(Box<Rel>),
    /// A complete Attribute row.
    Attribute(Box<Attribute>),
    /// A complete Event row.
    Event(Box<Event>),
    /// A complete Freshness row.
    Freshness(Box<Freshness>),
    /// A complete Source row.
    Source(Box<Source>),
    /// A complete Doc row.
    Doc(Box<Doc>),
    /// A complete DocRevision row.
    DocRevision(Box<DocRevision>),
    /// A complete DocNode row.
    DocNode(Box<DocNode>),
    /// An original queue record accepted only as a read dependency.
    WorkItem(Box<WorkItem>),
    /// An original execution record accepted only as a read dependency.
    WorkRun(Box<WorkRun>),
    /// A complete WorkArtifact row.
    WorkArtifact(Box<WorkArtifact>),
    /// A complete TruthSlot row.
    TruthSlot(Box<TruthSlot>),
    /// A complete ConflictGroup row.
    ConflictGroup(Box<ConflictGroup>),
    /// A complete PredicateRegistry row.
    PredicateRegistry(Box<PredicateRegistry>),
    /// A complete governed graph-view definition.
    KnowledgeViewDefinition(Box<KnowledgeViewDefinition>),
    /// A complete PredicateAlias row.
    PredicateAlias(Box<PredicateAlias>),
}

impl CanonicalRecord {
    /// Common immutable identity and authorization fields.
    #[must_use]
    pub fn base(&self) -> &BaseFields {
        match self {
            Self::AuditLog(record) => &record.base,
            Self::IdMap(record) => &record.base,
            Self::ReprocessJob(record) => &record.base,
            Self::EntityMerge(record) => &record.base,
            Self::IdentityCandidate(record) => &record.base,
            Self::SystemEvent(record) => &record.base,
            Self::Entity(record) => &record.base,
            Self::EntityIdentityKey(record) => &record.base,
            Self::Assertion(record) => &record.base,
            Self::Rel(record) => &record.base,
            Self::Attribute(record) => &record.base,
            Self::Event(record) => &record.base,
            Self::Freshness(record) => &record.base,
            Self::Source(record) => &record.base,
            Self::Doc(record) => &record.base,
            Self::DocRevision(record) => &record.base,
            Self::DocNode(record) => &record.base,
            Self::WorkItem(record) => &record.base,
            Self::WorkRun(record) => &record.base,
            Self::WorkArtifact(record) => &record.base,
            Self::TruthSlot(record) => &record.base,
            Self::ConflictGroup(record) => &record.base,
            Self::PredicateRegistry(record) => &record.base,
            Self::KnowledgeViewDefinition(record) => &record.base,
            Self::PredicateAlias(record) => &record.base,
        }
    }

    pub(crate) fn native(&self) -> StorageResult<Value> {
        match self {
            Self::AuditLog(record) => crate::surreal::audit_log_to_value(record),
            Self::SystemEvent(record) => crate::SystemEventRepository::system_event_to_value(record),
            Self::IdentityCandidate(record) => crate::surreal::identity_candidate_to_value(record),
            Self::IdMap(record) => crate::canonical_id_map::native(record),
            Self::ReprocessJob(record) => crate::reprocess_job_repository::ReprocessJobRepository::reprocess_job_to_value(record),
            Self::EntityMerge(record) => crate::surreal::entity_merge_to_value(record),
            Self::Entity(record) => crate::surreal::entity_to_value(record),
            Self::EntityIdentityKey(record) => crate::surreal::entity_identity_key_to_value(record),
            Self::Assertion(record) => crate::assertion_repository::AssertionRepository::assertion_to_value(record),
            Self::Rel(record) => rel_value(record),
            Self::Attribute(record) => crate::attribute_repository::AttributeRepository::attribute_to_value(record),
            Self::Event(record) => crate::event_repository::EventRepository::event_to_value(record),
            Self::Freshness(record) => crate::freshness_repository::FreshnessRepository::freshness_to_value(record),
            Self::Source(record) => crate::surreal::source_to_value(record),
            Self::Doc(record) => crate::surreal::doc_to_value(record),
            Self::DocRevision(record) => crate::doc_revision_repository::DocRevisionRepository::doc_revision_to_value(record),
            Self::DocNode(record) => crate::doc_node_repository::DocNodeRepository::doc_node_to_value(record),
            Self::WorkItem(record) => crate::work_item_repository::WorkItemRepository::work_item_to_value(record),
            Self::WorkRun(record) => crate::work_run_repository::WorkRunRepository::work_run_to_value(record),
            Self::WorkArtifact(record) => crate::work_artifact_repository::WorkArtifactRepository::work_artifact_to_value(record),
            Self::TruthSlot(record) => crate::truth_slot_repository::TruthSlotRepository::truth_slot_to_value(record),
            Self::ConflictGroup(record) => crate::conflict_group_repository::ConflictGroupRepository::conflict_group_to_value(record),
            Self::PredicateRegistry(record) => crate::predicate_registry_repository::PredicateRegistryRepository::predicate_registry_to_value(record),
            Self::KnowledgeViewDefinition(record) => crate::knowledge_view_definition_repository::KnowledgeViewDefinitionRepository::to_value(record),
            Self::PredicateAlias(record) => crate::predicate_alias_repository::PredicateAliasRepository::predicate_alias_to_value(record),
        }
    }

    pub(crate) fn current(&self, tx: &EmbeddedMutation<'_>) -> StorageResult<Option<Self>> {
        Self::load_tx(tx, &self.base().space_id, &self.base().id)
    }

    pub(crate) fn put(&self, tx: &mut EmbeddedMutation<'_>, create: bool) -> StorageResult<()> {
        macro_rules! put_record {
            ($($variant:ident),+ $(,)?) => {
                match self {
                    Self::WorkItem(_) | Self::WorkRun(_) => {
                        return Err(integrity("queue records are read dependencies only"));
                    }
                    $(Self::$variant(row) => {
                        if create {
                            tx.create((**row).clone())?;
                        } else {
                            tx.replace((**row).clone())?;
                        }
                    })+
                }
            };
        }
        put_record!(
            SystemEvent,
            IdentityCandidate,
            IdMap,
            ReprocessJob,
            EntityMerge,
            AuditLog,
            Entity,
            EntityIdentityKey,
            Assertion,
            Rel,
            Attribute,
            Event,
            Freshness,
            Source,
            Doc,
            DocRevision,
            DocNode,
            WorkArtifact,
            TruthSlot,
            ConflictGroup,
            PredicateRegistry,
            KnowledgeViewDefinition,
            PredicateAlias,
        );
        Ok(())
    }

    pub(crate) fn delete(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        let base = self.base();
        match self {
            Self::WorkItem(_) | Self::WorkRun(_) => {
                return Err(integrity("queue records are read dependencies only"))
            }
            Self::AuditLog(_) => tx.hard_delete::<AuditLog>(&base.space_id, &base.id)?,
            Self::IdMap(_) => tx.hard_delete::<IdMap>(&base.space_id, &base.id)?,
            Self::ReprocessJob(_) => tx.hard_delete::<ReprocessJob>(&base.space_id, &base.id)?,
            Self::EntityMerge(_) => tx.hard_delete::<EntityMerge>(&base.space_id, &base.id)?,
            Self::IdentityCandidate(_) => {
                tx.hard_delete::<IdentityCandidate>(&base.space_id, &base.id)?;
            }
            Self::SystemEvent(_) => tx.hard_delete::<SystemEvent>(&base.space_id, &base.id)?,
            Self::Entity(_) => tx.hard_delete::<Entity>(&base.space_id, &base.id)?,
            Self::EntityIdentityKey(_) => {
                tx.hard_delete::<EntityIdentityKey>(&base.space_id, &base.id)?;
            }
            Self::Assertion(_) => tx.hard_delete::<Assertion>(&base.space_id, &base.id)?,
            Self::Rel(_) => tx.hard_delete::<Rel>(&base.space_id, &base.id)?,
            Self::Attribute(_) => tx.hard_delete::<Attribute>(&base.space_id, &base.id)?,
            Self::Event(_) => tx.hard_delete::<Event>(&base.space_id, &base.id)?,
            Self::Freshness(_) => tx.hard_delete::<Freshness>(&base.space_id, &base.id)?,
            Self::Source(_) => tx.hard_delete::<Source>(&base.space_id, &base.id)?,
            Self::Doc(_) => tx.hard_delete::<Doc>(&base.space_id, &base.id)?,
            Self::DocRevision(_) => tx.hard_delete::<DocRevision>(&base.space_id, &base.id)?,
            Self::DocNode(_) => tx.hard_delete::<DocNode>(&base.space_id, &base.id)?,
            Self::WorkArtifact(_) => tx.hard_delete::<WorkArtifact>(&base.space_id, &base.id)?,
            Self::TruthSlot(_) => tx.hard_delete::<TruthSlot>(&base.space_id, &base.id)?,
            Self::ConflictGroup(_) => tx.hard_delete::<ConflictGroup>(&base.space_id, &base.id)?,
            Self::PredicateRegistry(_) => {
                tx.hard_delete::<PredicateRegistry>(&base.space_id, &base.id)?;
            }
            Self::KnowledgeViewDefinition(_) => {
                tx.hard_delete::<KnowledgeViewDefinition>(&base.space_id, &base.id)?;
            }
            Self::PredicateAlias(_) => {
                tx.hard_delete::<PredicateAlias>(&base.space_id, &base.id)?;
            }
        }
        Ok(())
    }
}

fn rel_value(rel: &Rel) -> StorageResult<Value> {
    let Value::Object(mut record) = crate::surreal::rel_to_value(rel)? else {
        return Err(integrity("relationship conversion is not an object"));
    };
    record.insert(
        "in".into(),
        crate::surreal::thing_from_record_id(rel.subject.id()).into(),
    );
    record.insert(
        "out".into(),
        crate::surreal::thing_from_record_id(rel.object.id()).into(),
    );
    Ok(Value::Object(record))
}

/// One complete create, replace, or physical delete, with its observed before row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEffect {
    /// Exact observed row; absent means create-only.
    pub before: Option<CanonicalRecord>,
    /// Complete result row; absent means physical deletion.
    pub after: Option<CanonicalRecord>,
}

impl CanonicalEffect {
    fn row(&self) -> StorageResult<&CanonicalRecord> {
        self.after
            .as_ref()
            .or(self.before.as_ref())
            .ok_or_else(|| integrity("empty canonical effect"))
    }
}

/// An immutable semantic plan prepared without writing live canonical state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CanonicalPlan {
    /// Capture before the first preparation read; empty values are rejected.
    pub observed_epoch: String,
    /// Exact lineage/security rows influencing the preparation.
    pub dependencies: Vec<CanonicalRecord>,
    /// Complete canonical writes; an assertion is only one member of this set.
    pub effects: Vec<CanonicalEffect>,
    /// Declared extension records, under the same predicate and before-image fence.
    #[serde(default)]
    pub extension_effects: Vec<crate::canonical_extension::CanonicalExtensionEffect>,
    /// Durable events written in the same transaction; sequence is allocated here.
    pub outbox: Vec<SystemEvent>,
}

// Retained event effects may restore a sequence above the current live maximum.
// Outbox allocation must also stay above those explicitly guarded after-images.
fn effect_event_sequence_floor(plan: &CanonicalPlan) -> i64 {
    plan.effects
        .iter()
        .filter_map(|effect| match effect.after.as_ref() {
            Some(CanonicalRecord::SystemEvent(event)) => Some(event.seq),
            _ => None,
        })
        .fold(0, i64::max)
}

impl CanonicalPlan {
    /// Adds an effect or coalesces a later mutation of the same prepared row.
    pub fn push(
        &mut self,
        before: Option<CanonicalRecord>,
        after: Option<CanonicalRecord>,
    ) -> StorageResult<()> {
        let incoming = CanonicalEffect { before, after };
        let id = &incoming.row()?.base().id;
        if let Some(existing) = self
            .effects
            .iter_mut()
            .find(|effect| effect.row().is_ok_and(|row| &row.base().id == id))
        {
            if !same_optional(existing.after.as_ref(), incoming.before.as_ref())? {
                return Err(integrity("nonsequential canonical effects for one row"));
            }
            existing.after = incoming.after;
        } else {
            self.effects.push(incoming);
        }
        Ok(())
    }
}

/// Storage-owned prepared retirement; callers cannot supply control deletions.
#[derive(Debug, Clone)]
pub(crate) struct SourceReplacementPlan {
    pub plan: CanonicalPlan,
    pub contact_survival: Option<crate::source_contact_survival_proof::PreparedContactSurvival>,
    /// Full normalized control keys paired with their exact table.
    pub control_deletes: Vec<(String, String)>,
}

/// Captured acceptance and queue authority carried through original work payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalPublicationContext {
    /// Source generation accepted for this exact immutable input.
    pub source: PublicationFence,
    /// Original claim's durable attempt fence.
    pub attempt: AttemptFence,
}

/// Captured current support, distinct from a pending desired source revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PublishedSourceFence {
    /// Exact current input authority for a restricted job-control mutation.
    /// This does not certify graph publication and is invalid in other modes.
    PendingControl {
        /// Original accepted head, including its desired/published distinction.
        head: Box<SourceHead>,
        /// Exact live input envelope and payload observed with the head.
        source: Box<Source>,
    },
    /// A current published revision under explicit source-head authority.
    Head {
        /// Physical source incarnation.
        source_id: RecordId,
        /// Exact accepted generation.
        generation: String,
        /// Already-published revision.
        published_revision: String,
    },
    /// Retained source-first lineage before a source head exists. Commit checks
    /// both exact source snapshot and absence of a subsequently accepted head.
    Legacy {
        /// Exact same-space, live source observed during maintenance planning.
        source: Box<Source>,
    },
}

impl PublishedSourceFence {
    /// Returns the physical source identity in either explicit authority mode.
    #[must_use]
    pub fn source_id(&self) -> &RecordId {
        match self {
            Self::PendingControl { head, .. } => &head.source_id,
            Self::Head { source_id, .. } => source_id,
            Self::Legacy { source } => &source.base.id,
        }
    }
}

/// Whether an authorized change derives content or supplies independent input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalMaintenanceOrigin {
    /// Existing reprocess job status/progress only; grants no graph support.
    ControlOnly,
    /// Computed decisions, including trusted direct truth maintenance and merges.
    Derived,
    /// Explicit independently supplied public edits; does not certify new evidence.
    IndependentMutation,
}

/// Queue authority and already-published support for retained maintenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalMaintenanceContext {
    /// Explicit derivation origin; never inferred from an absent attempt fence.
    pub origin: CanonicalMaintenanceOrigin,
    /// The original attempt running this maintenance work.
    pub attempt: Option<AttemptFence>,
    /// Every source whose support influences the maintenance plan.
    pub sources: Vec<PublishedSourceFence>,
}

/// An atomic publication request; callers must authorize before preparing it.
#[derive(Debug, Clone)]
pub struct CanonicalCommit {
    /// Explicit owning space for every effect and event.
    pub space_id: String,
    /// Stable complete-bundle identity, normally the commit artifact ID.
    pub commit_id: String,
    /// Fingerprint of immutable semantic inputs, excluding attempt/plan randomness.
    pub input_fingerprint: String,
    /// Durable successor output needed after commit-before-enqueue interruption.
    pub replay_output: Option<serde_json::Value>,
    /// Captured source/attempt authority; absent only for trusted source-less work.
    pub publication: Option<CanonicalPublicationContext>,
    /// Authority for maintenance of published or independently authored records.
    pub maintenance: Option<CanonicalMaintenanceContext>,
    /// Sealed complete-source expectations rechecked inside the transaction.
    pub finalization: Option<crate::source_publication_manifest::SourcePublicationRequirements>,
    /// Fully prepared effects and read dependencies.
    pub plan: CanonicalPlan,
}

/// Complete artifact receipt, distinct from a fully searchable source operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalCommitReceipt {
    /// Present only for an actual custody-owned historical import transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_import: Option<crate::historical_adoption::HistoricalCommitProvenance>,
    /// Owning space.
    pub space_id: String,
    /// Stable complete-bundle identity.
    pub commit_id: String,
    /// Fingerprint of immutable semantic inputs, excluding attempt/plan randomness.
    pub input_fingerprint: String,
    /// Fingerprint of exact prepared effect/dependency/outbox contents and bindings.
    pub plan_fingerprint: String,
    /// Exact committed successor output; physically erased with any input source.
    pub replay_output: Option<serde_json::Value>,
    /// Union of causal source inputs, including multi-source maintenance.
    pub source_ids: Vec<RecordId>,
    /// Original source publication authority, rechecked before payload replay.
    pub publication_authority: Option<PublicationFence>,
    /// Published inputs still required before maintenance payload replay.
    pub maintenance_authority: Vec<PublishedSourceFence>,
    /// Source incarnation, when this is a source-derived publication.
    pub source_id: Option<RecordId>,
    /// Exact source revision that supplied the effects.
    pub source_revision: Option<String>,
    /// Every canonical row affected by this transaction.
    pub effect_ids: Vec<RecordId>,
    /// Native extension row identities affected by this transaction.
    #[serde(default)]
    pub extension_ids: Vec<surrealdb::sql::Thing>,
    /// Every outbox row written by the transaction.
    pub outbox_ids: Vec<RecordId>,
    /// Timestamp of complete canonical publication.
    pub committed_at: DateTime<Utc>,
}

/// The sole storage transaction boundary for prepared canonical effects.
#[derive(Debug, Clone)]
pub struct CanonicalRepository {
    pub(crate) client: StorageClient,
}

impl CanonicalRepository {
    /// Binds the canonical repository to a storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Private read adapter.
    ///
    /// The caller holds activity admission and compares
    /// the canonical epoch before/after the complete export. This shares the
    /// exact replay source predicate; it never authorizes historical payloads.
    pub(crate) async fn receipt_read_only(
        &self,
        space: &str,
        commit_id: &str,
    ) -> StorageResult<Option<CanonicalCommitReceipt>> {
        use crate::Repository;
        let publication = SourcePublicationRepository::new(self.client.clone());
        let receipt: Option<CanonicalCommitReceipt> = publication
            .read_control(
                "source_commit_receipt",
                &stable_key(&[space, commit_id]),
                space,
            )
            .await?;
        if let Some(receipt) = &receipt {
            if receipt.space_id != space || receipt.commit_id != commit_id {
                return Err(integrity("receipt_identity_mismatch"));
            }
            for fence in receipt_authority(receipt) {
                let source = crate::SourceRepository::new(self.client.clone())
                    .get(space, fence.source_id())
                    .await?
                    .ok_or_else(|| conflict("receipt_source_not_current"))?;
                let head = publication.head(space, fence.source_id()).await?;
                check_replay_source(&source, head.as_ref(), &fence)?;
            }
        }
        drop(publication);
        Ok(receipt)
    }

    /// Observes original canonical membership, including physical absence.
    ///
    /// This read neither initializes state nor grants named-space authority.
    /// A caller must retain and compare this exact Option after its input reads.
    ///
    /// # Errors
    /// Refuses foreign or malformed existing control state.
    pub async fn observe_epoch(&self, space: &str) -> StorageResult<Option<String>> {
        #[derive(Deserialize)]
        struct EpochDb {
            space_id: String,
            epoch: String,
        }
        let original: Option<EpochDb> = if let Some(store) = self.client.embedded_store() {
            store
                .extension_row_by_key("canonical_epoch", &format!("canonical_epoch:{space}"))
                .await?
                .map(|row| {
                    serde_json::from_value(row).map_err(|error| integrity(&error.to_string()))
                })
                .transpose()?
        } else {
            let mut response = self
                .client
                .query_bind_checked(
                    "SELECT space_id,epoch FROM $epoch_id;",
                    crate::bindings! { "epoch_id" => control_thing("canonical_epoch", space) },
                )
                .await?;
            let mut rows: Vec<EpochDb> = response
                .take(0)
                .map_err(|error| integrity(&error.to_string()))?;
            if rows.len() > 1 {
                return Err(integrity("duplicate canonical epoch"));
            }
            rows.pop()
        };
        original
            .map(|row| {
                if row.space_id != space || row.epoch.is_empty() {
                    Err(integrity("canonical epoch scope or value invalid"))
                } else {
                    Ok(row.epoch)
                }
            })
            .transpose()
    }

    /// Reads an existing canonical membership epoch without initializing state.
    ///
    /// Source input capture requires an epoch; source-less original execution
    /// observers may instead retain an explicit `observe_epoch` absence token.
    ///
    /// # Errors
    /// Refuses missing, foreign or malformed existing control state.
    pub async fn current_epoch(&self, space: &str) -> StorageResult<String> {
        self.observe_epoch(space)
            .await?
            .ok_or_else(|| conflict("canonical_epoch_missing"))
    }

    /// Captures a coarse predicate fence before any planning reads.
    pub async fn epoch(&self, space: &str) -> StorageResult<String> {
        #[derive(Deserialize)]
        struct EpochDb {
            epoch: String,
        }

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    require_space(tx, space)?;
                    epoch_tx(tx, space)?
                        .map_or_else(|| tx.advance_canonical_epoch(space).map_err(Into::into), Ok)
                })
                .await;
        }
        self.client.query_bind_checked(
            "BEGIN TRANSACTION; IF (SELECT VALUE version FROM ONLY publication_schema:canonical) != 1 { THROW 'publication:canonical_schema_missing'; }; LET $current = SELECT * FROM ONLY $epoch_id; IF $current = NONE { CREATE $epoch_id SET space_id = $space, epoch = <string>rand::uuid(); }; COMMIT TRANSACTION;",
            crate::bindings! { "space" => space, "epoch_id" => control_thing("canonical_epoch", space) },
        ).await.map_err(publication_error)?;
        let mut response = self
            .client
            .query_bind_checked(
                "SELECT epoch FROM $epoch_id",
                crate::bindings! { "epoch_id" => control_thing("canonical_epoch", space) },
            )
            .await?;
        let rows: Vec<EpochDb> = response
            .take(0)
            .map_err(|error| integrity(&error.to_string()))?;
        rows.into_iter()
            .next()
            .map(|row| row.epoch)
            .ok_or_else(|| integrity("canonical epoch absent"))
    }

    /// Reads an exact receipt only while its payload's source authority is current.
    pub async fn receipt(
        &self,
        space: &str,
        commit_id: &str,
    ) -> StorageResult<Option<CanonicalCommitReceipt>> {
        let publication = SourcePublicationRepository::new(self.client.clone());
        let receipt: Option<CanonicalCommitReceipt> = publication
            .read_control(
                "source_commit_receipt",
                &stable_key(&[space, commit_id]),
                space,
            )
            .await?;
        if let Some(receipt) = &receipt {
            if let Some(store) = self.client.embedded_store() {
                store
                    .mutate(|tx| check_receipt_authority_tx(tx, receipt))
                    .await?;
            } else {
                use crate::Repository;
                for fence in receipt_authority(receipt) {
                    let source = crate::SourceRepository::new(self.client.clone())
                        .get(space, fence.source_id())
                        .await?
                        .ok_or_else(|| conflict("receipt_source_not_current"))?;
                    let head = publication.head(space, fence.source_id()).await?;
                    check_replay_source(&source, head.as_ref(), &fence)?;
                }
            }
        }
        drop(publication);
        Ok(receipt)
    }
}

fn same_optional(
    left: Option<&CanonicalRecord>,
    right: Option<&CanonicalRecord>,
) -> StorageResult<bool> {
    Ok(match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            serde_json::to_value(left).map_err(|error| integrity(&error.to_string()))?
                == serde_json::to_value(right).map_err(|error| integrity(&error.to_string()))?
        }
        _ => false,
    })
}

pub(crate) fn epoch_tx(tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<Option<String>> {
    tx.scan_extension_rows("canonical_epoch")?
        .into_iter()
        .find(|(_, row)| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space))
        .map(|(_, row)| {
            row.get("epoch")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| integrity("missing canonical epoch"))
        })
        .transpose()
}

/// Payload-free support identity used by erasure and revision-aware readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEffectSupport {
    /// Explicit owning space.
    pub space_id: String,
    /// Source incarnation supplying support.
    pub source_id: RecordId,
    /// Exact immutable source revision.
    pub source_revision: String,
    /// Canonical row supported by this revision.
    pub record_id: RecordId,
    /// Imported payload has unknown independent/source ownership. Its exact
    /// bytes may be read through the historical receipt, but destructive
    /// replacement/erasure requires a separately proved baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_unknown: Option<crate::historical_adoption::HistoricalUnknownOwnership>,
    /// The row already had independently authoritative content before this source.
    pub independent: bool,
    /// Verified canonical content; a direct unverified mutation invalidates this proof.
    pub record_fingerprint: String,
    /// Actual canonical transaction which last certified this support row.
    /// Missing historical pointers remain unknown; no revision/time inference.
    #[serde(default)]
    pub last_commit_id: Option<String>,
}

/// Fault injection points exercised inside the real publication transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalFault {
    /// No injected failure.
    None,
    /// Abort after the first canonical effect has been written.
    AfterFirstEffect,
    /// Abort after effects, before any outbox event.
    BeforeOutbox,
    /// Abort after effects and outbox, before the complete-bundle receipt.
    BeforeReceipt,
}

impl CanonicalRepository {
    /// Commits a complete effect set or leaves every affected row unchanged.
    pub async fn commit(&self, request: CanonicalCommit) -> StorageResult<CanonicalCommitReceipt> {
        Box::pin(self.commit_inner(request, CanonicalFault::None)).await
    }

    /// Injects failure in the actual transaction for independent rollback oracles.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn commit_with_fault(
        &self,
        request: CanonicalCommit,
        fault: CanonicalFault,
    ) -> StorageResult<CanonicalCommitReceipt> {
        Box::pin(self.commit_inner(request, fault)).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep the original fence checks, retirement, effects and receipt in one ordered atomic commit path."
    )]
    async fn commit_inner(
        &self,
        mut request: CanonicalCommit,
        fault: CanonicalFault,
    ) -> StorageResult<CanonicalCommitReceipt> {
        validate_commit(&request)?;
        let plan_fingerprint = canonical_plan_fingerprint(&request)?;
        // Exact durable replay precedes re-reading effects that this very
        // transaction already changed. It never authorizes a new mutation.
        if let Some(old) = self.receipt(&request.space_id, &request.commit_id).await? {
            if old.input_fingerprint != request.input_fingerprint
                || old.plan_fingerprint != plan_fingerprint
            {
                return Err(conflict("canonical_idempotency_conflict"));
            }
            return Ok(old);
        }
        // A job spawned by queued work must retain that exact execution's
        // private ancestry. The selected source revision is content provenance,
        // not a substitute parent or a veto from an unrelated historical Watch.
        let reprocess_parent = if request.plan.effects.iter().any(|effect| {
            effect.before.is_none()
                && matches!(effect.after, Some(CanonicalRecord::ReprocessJob(_)))
        }) {
            let original_attempt = request
                .publication
                .as_ref()
                .map(|context| &context.attempt)
                .or_else(|| {
                    request
                        .maintenance
                        .as_ref()
                        .and_then(|context| context.attempt.as_ref())
                });
            match original_attempt {
                Some(attempt) => Some(
                    crate::work_producer_repository::PreparedReprocessParent::capture(
                        &self.client,
                        attempt,
                    )
                    .await?,
                ),
                // This is the existing explicitly trusted, unqueued Rust commit
                // path. Missing private history on a queued parent never reaches
                // this branch: capture above refuses it before any write.
                None => None,
            }
        } else {
            None
        };
        // Source-head authority and original recurring-service permission are
        // both required for a new write. Exact receipt replay above remains a
        // read and never adopts a current grant as fresh mutation authority.
        let mut standing_authority = if let Some(publication) = &request.publication {
            crate::standing_watch_repository::PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client, &request.space_id, &publication.source,
            ).await?
        } else if let Some(attempt) = request
            .maintenance
            .as_ref()
            .and_then(|context| context.attempt.as_ref())
        {
            use crate::Repository;
            let original = crate::WorkItemRepository::new(self.client.clone())
                .get(&request.space_id, &attempt.work_item_id)
                .await?
                .ok_or_else(|| conflict("standing_original_work_missing"))?;
            Some(crate::standing_watch_repository::PreparedStandingSourceAuthority::capture_for_work(
                &self.client, &original,
            ).await?)
        } else {
            None
        };
        let accepted_contact = if let Some(publication) = &request.publication {
            Some(
                crate::source_contact_publication::AcceptedContactState::load(
                    &self.client,
                    &request.space_id,
                    &publication.source,
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(contact) = &accepted_contact {
            contact.validate_plan(&request.plan)?;
        }
        let (retired_controls, contact_survival) = self
            .prepare_retirement(&mut request, accepted_contact.as_ref())
            .await?;
        let replacement_continuations =
            crate::source_contact_lineage::prepare_replacement_continuations(
                &self.client,
                &mut request,
                &retired_controls,
            )
            .await?;
        // Retirement can append effects, so bind the final prepared read/write
        // population to every original ceiling only after that expansion.
        let standing_documents = match standing_authority.as_mut() {
            Some(authority) => {
                prepare_standing_record_inputs(&self.client, &request, authority).await?
            }
            None => Vec::new(),
        };
        let support_updates = self.prepare_support_updates(&request).await?;
        let mut contact_lineage =
            crate::source_contact_lineage::ContactLineage::prepare(&self.client, &request).await?;
        if let Some(continuations) = &replacement_continuations {
            continuations.attach_canonical_roots(&mut contact_lineage)?;
        }
        let definition_lineage =
            crate::canonical_definition::PreparedDefinitionLineage::prepare(&self.client, &request)
                .await?;
        let key = stable_key(&[&request.space_id, &request.commit_id]);
        let source_id = request
            .publication
            .as_ref()
            .map(|publication| publication.source.source_id.clone());
        let receipt = CanonicalCommitReceipt {
            historical_import: None,
            space_id: request.space_id.clone(),
            commit_id: request.commit_id.clone(),
            input_fingerprint: request.input_fingerprint.clone(),
            plan_fingerprint,
            replay_output: request.replay_output.clone(),
            source_ids: causal_source_ids(&request),
            publication_authority: request
                .publication
                .as_ref()
                .map(|publication| publication.source.clone()),
            maintenance_authority: request
                .maintenance
                .as_ref()
                .map_or_else(Vec::new, |maintenance| maintenance.sources.clone()),
            source_id: source_id.clone(),
            source_revision: request
                .publication
                .as_ref()
                .map(|publication| publication.source.revision_id.clone()),
            effect_ids: request
                .plan
                .effects
                .iter()
                .map(|effect| Ok(effect.row()?.base().id.clone()))
                .collect::<StorageResult<Vec<_>>>()?,
            extension_ids: request
                .plan
                .extension_effects
                .iter()
                .map(|effect| effect.id.clone())
                .collect(),
            outbox_ids: request
                .plan
                .outbox
                .iter()
                .map(|event| event.base.id.clone())
                .collect(),
            committed_at: Utc::now(),
        };
        // Source-less receipts use a sentinel only in their indexing field; the
        // typed receipt retains None and cannot invent a source lineage.
        let receipt_source = source_id.unwrap_or_else(|| {
            elephant_types::RecordId::new(elephant_types::TableName::Source, uuid::Uuid::nil())
        });
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    require_space(tx, &request.space_id)?;
                    if let Some(old) = read_control_tx::<CanonicalCommitReceipt>(
                        tx,
                        "source_commit_receipt",
                        &key,
                        &request.space_id,
                    )? {
                        check_receipt_authority_tx(tx, &old)?;
                        return check_replayed_commit(old, &receipt);
                    }
                    if epoch_tx(tx, &request.space_id)?.as_deref()
                        != Some(request.plan.observed_epoch.as_str())
                    {
                        return Err(conflict("canonical_inputs_changed"));
                    }
                    definition_lineage.check_tx(tx, &request.space_id)?;
                    if let Some(authority) = &standing_authority { authority.check_tx(tx)?; }
                    for documents in &standing_documents { documents.check_tx(tx)?; }
                    if let Some(contact) = &accepted_contact {
                        contact.check_tx(tx)?;
                    }
                    if let Some(survival) = &contact_survival { survival.check_tx(tx)?; }
                    if let Some(lineage) = &contact_lineage { lineage.check_tx(tx, &request.space_id)?; }
                    if let Some(continuations) = &replacement_continuations { continuations.check_tx(tx)?; }
                    tx.advance_canonical_epoch(&request.space_id)?;
                    if let Some(publication) = &request.publication {
                        check_publication_tx(tx, &request.space_id, publication)?;
                        let manifest = request.finalization.as_ref().ok_or_else(|| {
                            integrity("source publication requires complete manifest")
                        })?;
                        crate::source_publication_manifest::check_ready_tx(
                            tx,
                            &request.space_id,
                            &publication.source,
                            manifest,
                        )?;
                    }
                    if let Some(maintenance) = &request.maintenance {
                        check_maintenance_tx(tx, &request.space_id, maintenance)?;
                    }
                    for dependency in &request.plan.dependencies {
                        let current = dependency.current(tx)?;
                        if !same_optional(current.as_ref(), Some(dependency))? {
                            return Err(conflict("canonical_dependency_changed"));
                        }
                    }
                    // Validate the complete original write set before any effect
                    // can change another effect's observed row.
                    for effect in &request.plan.effects {
                        let row = effect.row()?;
                        let current = row.current(tx)?;
                        if !same_optional(current.as_ref(), effect.before.as_ref())? {
                            return Err(conflict("canonical_effect_changed"));
                        }
                    }
                    // Preserve the original tail even when effects delete every
                    // event and this commit emits none. Reservation and effects
                    // share this transaction, including its rollback boundary.
                    let mut sequence = crate::event_sequence::reserve_tx(
                        tx,
                        &request.space_id,
                        effect_event_sequence_floor(&request.plan),
                        request.plan.outbox.len(),
                    )?;
                    for (index, effect) in request.plan.effects.iter().enumerate() {
                        if let Some(after) = &effect.after {
                            after.put(tx, effect.before.is_none())?;
                            if effect.before.is_none() {
                                if let CanonicalRecord::ReprocessJob(job) = after {
                                    crate::work_producer_repository::initialize_reprocess_intent_with_parent_tx(tx, job, reprocess_parent.as_ref())?;
                                }
                            }
                        } else if let Some(before) = &effect.before {
                            before.delete(tx)?;
                        }
                        if let Some(publication) = &request.publication {
                            if effect
                                .after
                                .as_ref()
                                .is_some_and(|row| row.base().deleted_at.is_none())
                                && effect_attributes_source(effect, &publication.source.source_id)
                            {
                                write_support_tx(
                                    tx,
                                    &request.space_id,
                                    &request.commit_id,
                                    &publication.source.source_id,
                                    &publication.source.revision_id,
                                    effect,
                                )?;
                            }
                        }
                        if let Some(maintenance) = &request.maintenance {
                            for (source_id, revision_id) in
                                maintenance_supports(maintenance, effect)
                            {
                                write_support_tx(
                                    tx,
                                    &request.space_id,
                                    &request.commit_id,
                                    &source_id,
                                    &revision_id,
                                    effect,
                                )?;
                            }
                        }
                        if index == 0 && fault == CanonicalFault::AfterFirstEffect {
                            return Err(integrity("injected canonical fault after first effect"));
                        }
                    }
                    crate::canonical_extension::apply_extension_effects_tx(
                        tx,
                        &request.space_id,
                        &request.plan.extension_effects,
                    )?;
                    if request.plan.effects.is_empty()
                        && !request.plan.extension_effects.is_empty()
                        && fault == CanonicalFault::AfterFirstEffect
                    {
                        return Err(integrity(
                            "injected canonical fault after extension effects",
                        ));
                    }
                    for support in &support_updates {
                        write_existing_support_tx(tx, support)?;
                    }
                    if fault == CanonicalFault::BeforeOutbox {
                        return Err(integrity("injected canonical fault before outbox"));
                    }
                    for event in &request.plan.outbox {
                        sequence = sequence
                            .checked_add(1)
                            .ok_or_else(|| integrity("outbox sequence exhausted"))?;
                        let mut event = event.clone();
                        event.seq = sequence;
                        tx.create(event)?;
                    }
                    if fault == CanonicalFault::BeforeReceipt {
                        return Err(integrity("injected canonical fault before receipt"));
                    }
                    for (table, key) in &retired_controls {
                        tx.hard_delete_extension_row(table, key, &request.space_id)?;
                    }
                    if let Some(requirements) = &request.finalization {
                        crate::source_publication_manifest::mark_completed_tx(
                            tx,
                            requirements,
                            &request.commit_id,
                        )?;
                        complete_source_tx(
                            tx,
                            &request.space_id,
                            &requirements.manifest().publication,
                        )?;
                    }
                    crate::source_decision::write_decision_tx(
                        tx,
                        &request,
                        accepted_contact.as_ref(),
                    )?;
                    write_control_tx(
                        tx,
                        "source_commit_receipt",
                        &key,
                        &request.space_id,
                        &receipt_source,
                        &definition_lineage.seal_receipt(crate::source_contact_lineage::stored_receipt(&receipt, contact_lineage.as_ref())?)?,
                    )?;
                    Ok(receipt.clone())
                })
                .await;
        }
        self.commit_surreal(
            &request,
            &receipt,
            &receipt_source,
            &key,
            &retired_controls,
            &support_updates,
            accepted_contact.as_ref(),
            contact_lineage.as_ref(),
            replacement_continuations.as_ref(),
            contact_survival.as_ref(),
            standing_authority.as_ref(),
            &standing_documents,
            &definition_lineage,
            reprocess_parent.as_ref(),
            fault,
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep native SQL guards, ordered effects and receipt publication in one transaction builder."
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "All captured source, attempt, maintenance and retirement inputs stay explicit at the single transaction boundary."
    )]
    async fn commit_surreal(
        &self,
        request: &CanonicalCommit,
        receipt: &CanonicalCommitReceipt,
        receipt_source: &RecordId,
        key: &str,
        retired_controls: &[(String, String)],
        support_updates: &[SourceEffectSupport],
        accepted_contact: Option<&crate::source_contact_publication::AcceptedContactState>,
        contact_lineage: Option<&crate::source_contact_lineage::ContactLineage>,
        replacement_continuations: Option<
            &crate::source_contact_lineage::PreparedSupportContinuations,
        >,
        contact_survival: Option<&crate::source_contact_survival_proof::PreparedContactSurvival>,
        standing_authority: Option<
            &crate::standing_watch_repository::PreparedStandingSourceAuthority,
        >,
        standing_documents: &[crate::source_document_guards::PreparedDocGuards],
        definition_lineage: &crate::canonical_definition::PreparedDefinitionLineage,
        reprocess_parent: Option<&crate::work_producer_repository::PreparedReprocessParent>,
        fault: CanonicalFault,
    ) -> StorageResult<CanonicalCommitReceipt> {
        // Receipt lookup never certifies a different revision or source. The
        // identity denotes the immutable validated bundle, not an assertion.
        if let Some(old) = self.receipt(&request.space_id, &request.commit_id).await? {
            return check_replayed_commit(old, receipt);
        }
        let mut bindings = crate::bindings! {
            "space" => request.space_id.clone(), "epoch_id" => control_thing("canonical_epoch", &request.space_id),
            "observed_epoch" => request.plan.observed_epoch.clone(),
            "receipt_id" => control_thing("source_commit_receipt", key),
            "receipt_record" => control_value(&request.space_id, receipt_source, &definition_lineage.seal_receipt(crate::source_contact_lineage::stored_receipt(receipt, contact_lineage)?)?)?,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\nIF (SELECT VALUE version FROM ONLY publication_schema:canonical) != 1 { THROW 'publication:canonical_schema_missing'; };\nIF (SELECT VALUE epoch FROM ONLY $epoch_id) != $observed_epoch { THROW 'publication:canonical_inputs_changed'; };\nUPDATE $epoch_id SET epoch = <string>rand::uuid();\nLET $spaces = SELECT id FROM space_config WHERE space_id = $space AND deleted_at IS NONE;\nIF array::len($spaces) != 1 { THROW 'publication:unknown_space'; };\nIF (SELECT * FROM ONLY $receipt_id) != NONE { THROW 'publication:duplicate_commit'; };\n");
        definition_lineage.append_guard_sql(&mut sql, &mut bindings, &request.space_id);
        if let Some(authority) = standing_authority {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        for (index, documents) in standing_documents.iter().enumerate() {
            documents.append_guard_sql_scoped(
                &mut sql,
                &mut bindings,
                &format!("standing_inputs_{index}"),
            )?;
        }
        if let Some(survival) = contact_survival {
            survival.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(contact) = accepted_contact {
            contact.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(lineage) = contact_lineage {
            lineage.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(continuations) = replacement_continuations {
            continuations.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(publication) = &request.publication {
            append_publication_guard(&mut sql, &mut bindings, &request.space_id, publication);
            let manifest = request
                .finalization
                .as_ref()
                .ok_or_else(|| integrity("source publication requires complete manifest"))?;
            crate::source_publication_manifest::append_ready_sql(
                &mut sql,
                &mut bindings,
                &request.space_id,
                &publication.source,
                manifest,
            )?;
        }
        if let Some(maintenance) = &request.maintenance {
            append_maintenance_guard(&mut sql, &mut bindings, &request.space_id, maintenance)?;
        }
        for (index, dependency) in request.plan.dependencies.iter().enumerate() {
            append_row_guard(
                &mut sql,
                &mut bindings,
                &format!("dependency_{index}"),
                dependency,
                Some(dependency),
            )?;
        }
        // Native endpoint deletion also removes incident edges. Every original
        // before-image must therefore be checked before the first effect write,
        // rather than against the preceding effects of this same transaction.
        for (index, effect) in request.plan.effects.iter().enumerate() {
            append_row_guard(
                &mut sql,
                &mut bindings,
                &format!("effect_{index}"),
                effect.row()?,
                effect.before.as_ref(),
            )?;
        }
        // Reserve the complete range before any history-decreasing effect. The
        // same private floor write fences concurrent retirement/allocation;
        // a failed commit rolls it back with the effects and receipt.
        let sequence = crate::event_sequence::append_reserve_sql(
            &mut sql,
            &mut bindings,
            &request.space_id,
            "canonical_events",
            effect_event_sequence_floor(&request.plan),
            request.plan.outbox.len(),
        )?;
        for (index, effect) in request.plan.effects.iter().enumerate() {
            let prefix = format!("effect_{index}");
            if let Some(after) = &effect.after {
                bindings.insert(format!("{prefix}_after"), after.native()?);
                let moved_endpoints = matches!(
                    (effect.before.as_ref(), after),
                    (Some(CanonicalRecord::Rel(before)), CanonicalRecord::Rel(after))
                        if before.subject != after.subject || before.object != after.object
                );
                if effect.before.is_none() || moved_endpoints {
                    if let CanonicalRecord::Rel(rel) = after {
                        // Native edges retain their endpoints on UPDATE CONTENT.
                        // Recreate the same physical ID inside this transaction
                        // so native graph indices move with the canonical row.
                        if moved_endpoints {
                            writeln!(sql, "DELETE ${prefix}_id;").map_err(|error| {
                                crate::StorageError::DataIntegrityError(error.to_string())
                            })?;
                        }
                        bindings.insert(
                            format!("{prefix}_subject"),
                            crate::surreal::thing_from_record_id(rel.subject.id()).into(),
                        );
                        bindings.insert(
                            format!("{prefix}_object"),
                            crate::surreal::thing_from_record_id(rel.object.id()).into(),
                        );
                        writeln!(sql, "RELATE ${prefix}_subject->${prefix}_id->${prefix}_object CONTENT ${prefix}_after;").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
                    } else {
                        writeln!(sql, "CREATE ${prefix}_id CONTENT ${prefix}_after;").map_err(
                            |error| crate::StorageError::DataIntegrityError(error.to_string()),
                        )?;
                    }
                } else {
                    writeln!(sql, "UPDATE ${prefix}_id CONTENT ${prefix}_after;").map_err(
                        |error| crate::StorageError::DataIntegrityError(error.to_string()),
                    )?;
                }
                if effect.before.is_none() {
                    if let CanonicalRecord::ReprocessJob(job) = after {
                        crate::work_producer_repository::append_reprocess_intent_with_parent_sql(
                            &mut sql,
                            &mut bindings,
                            &prefix,
                            job,
                            reprocess_parent,
                        )?;
                    }
                }
                if let Some(publication) = request.publication.as_ref().filter(|publication| {
                    after.base().deleted_at.is_none()
                        && effect_attributes_source(effect, &publication.source.source_id)
                }) {
                    append_support(
                        &mut sql,
                        &mut bindings,
                        &prefix,
                        &request.space_id,
                        &request.commit_id,
                        &publication.source.source_id,
                        &publication.source.revision_id,
                        effect,
                    )?;
                }
            } else {
                writeln!(sql, "DELETE ${prefix}_id;")
                    .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
            }
            if let Some(maintenance) = &request.maintenance {
                for (support_index, (source_id, revision_id)) in
                    maintenance_supports(maintenance, effect).iter().enumerate()
                {
                    append_support(
                        &mut sql,
                        &mut bindings,
                        &format!("{prefix}_maintenance_{support_index}"),
                        &request.space_id,
                        &request.commit_id,
                        source_id,
                        revision_id,
                        effect,
                    )?;
                }
            }
            if index == 0 && fault == CanonicalFault::AfterFirstEffect {
                sql.push_str("THROW 'injected canonical fault after first effect';\n");
            }
        }
        crate::canonical_extension::append_extension_effects_sql(
            &mut sql,
            &mut bindings,
            &request.space_id,
            &request.plan.extension_effects,
        )?;
        if request.plan.effects.is_empty()
            && !request.plan.extension_effects.is_empty()
            && fault == CanonicalFault::AfterFirstEffect
        {
            sql.push_str("THROW 'injected canonical fault after extension effects';\n");
        }
        for (index, support) in support_updates.iter().enumerate() {
            append_existing_support_sql(
                &mut sql,
                &mut bindings,
                &format!("support_refresh_{index}"),
                support,
            )?;
        }
        if fault == CanonicalFault::BeforeOutbox {
            sql.push_str("THROW 'injected canonical fault before outbox';\n");
        }
        // Keep the unique (space,seq) index as an independent invariant; a
        // collision still aborts the complete canonical batch.
        for (index, event) in request.plan.outbox.iter().enumerate() {
            bindings.insert(
                format!("outbox_{index}_id"),
                crate::surreal::thing_from_record_id(&event.base.id).into(),
            );
            bindings.insert(
                format!("outbox_{index}_record"),
                crate::SystemEventRepository::system_event_to_value(event)?,
            );
            writeln!(sql, "CREATE $outbox_{index}_id CONTENT object::from_entries(array::concat(object::entries($outbox_{index}_record), [['seq', {sequence} + {}]]));", index + 1).map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        }
        if fault == CanonicalFault::BeforeReceipt {
            sql.push_str("THROW 'injected canonical fault before receipt';\n");
        }
        for (index, (table, key)) in retired_controls.iter().enumerate() {
            let bare_key = key
                .strip_prefix(&format!("{table}:"))
                .ok_or_else(|| integrity("replacement control key mismatch"))?;
            bindings.insert(
                format!("retired_control_{index}"),
                control_thing(table, bare_key).into(),
            );
            writeln!(sql, "DELETE $retired_control_{index};")
                .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        }
        if let Some(requirements) = &request.finalization {
            crate::source_publication_manifest::append_completed_sql(
                &mut sql,
                &mut bindings,
                requirements,
                &request.commit_id,
            )?;
            self.append_source_completion(
                &mut sql,
                &mut bindings,
                &request.space_id,
                &requirements.manifest().publication,
            )
            .await?;
        }
        crate::source_decision::append_decision_sql(
            &mut sql,
            &mut bindings,
            request,
            accepted_contact,
        )?;
        sql.push_str("CREATE $receipt_id CONTENT $receipt_record;\nCOMMIT TRANSACTION;");
        if let Err(error) = self.client.query_bind_checked(&sql, bindings).await {
            if let Some(old) = self.receipt(&request.space_id, &request.commit_id).await? {
                return check_replayed_commit(old, receipt);
            }
            return Err(publication_error(error));
        }
        Ok(receipt.clone())
    }
}

/// This is an ephemeral consumer of the single original-ancestry resolver and
/// intake's complete desired-input selector. It establishes no new authority.
async fn prepare_standing_record_inputs(
    client: &StorageClient,
    request: &CanonicalCommit,
    authority: &mut crate::standing_watch_repository::PreparedStandingSourceAuthority,
) -> StorageResult<Vec<crate::source_document_guards::PreparedDocGuards>> {
    const fn source_input(record: &CanonicalRecord) -> bool {
        matches!(
            record,
            CanonicalRecord::Source(_)
                | CanonicalRecord::Doc(_)
                | CanonicalRecord::DocRevision(_)
                | CanonicalRecord::DocNode(_)
                | CanonicalRecord::WorkArtifact(_)
        )
    }

    if authority.principals().next().is_none() {
        // Proven Direct/Independent executions still retain their private
        // ancestry guards, without inheriting unrelated content producers.
        return Ok(Vec::new());
    }
    for original in request
        .plan
        .dependencies
        .iter()
        .chain(
            request
                .plan
                .effects
                .iter()
                .filter_map(|effect| effect.before.as_ref()),
        )
        .filter(|record| source_input(record))
    {
        authority.authorize_record(original.clone())?;
    }
    // Proposed rows are authorization subjects, not persisted original reads.
    // Retaining them in the readset would make every legitimate update fail.
    for proposed in request
        .plan
        .effects
        .iter()
        .filter_map(|effect| effect.after.as_ref())
        .filter(|record| source_input(record))
    {
        for principal in authority.principals() {
            crate::source_watch_registration::authorize_standing_source_input(principal, proposed)?;
        }
    }
    let original_sources: Vec<_> = authority.source_ids().cloned().collect();
    let mut documents = Vec::with_capacity(original_sources.len());
    for source_id in original_sources {
        let inputs = crate::standing_dispatch_inputs::PreparedStandingDispatchInputs::capture(
            client,
            &request.space_id,
            &source_id,
        )
        .await?;
        for original in inputs.records() {
            authority.authorize_record(original.clone())?;
        }
        documents.push(inputs.document_guards().clone());
    }
    Ok(documents)
}

fn validate_commit(request: &CanonicalCommit) -> StorageResult<()> {
    crate::canonical_definition::validate_commit(request)?;
    if request.space_id.trim().is_empty()
        || request.commit_id.trim().is_empty()
        || request.input_fingerprint.trim().is_empty()
        || request.plan.observed_epoch.is_empty()
    {
        return Err(integrity("canonical identity and observed epoch required"));
    }
    validate_commit_effects(request)?;
    if request.publication.is_some() && request.maintenance.is_some() {
        return Err(integrity("canonical commit modes are mutually exclusive"));
    }
    if let Some(publication) = &request.publication {
        if publication.attempt.space_id != request.space_id {
            return Err(integrity("foreign-space attempt fence"));
        }
        let manifest = request
            .finalization
            .as_ref()
            .ok_or_else(|| integrity("source publication requires complete manifest"))?;
        if !manifest.matches_attempt(&publication.attempt)
            || manifest.manifest().space_id != request.space_id
            || manifest.manifest().publication != publication.source
            || manifest.commit_id() != request.commit_id
        {
            return Err(integrity(
                "source completion manifest does not match captured publication",
            ));
        }
    } else if let Some(maintenance) = &request.maintenance {
        if maintenance.origin == CanonicalMaintenanceOrigin::ControlOnly {
            crate::canonical_reprocess_control::validate_control_commit(request)?;
        } else if maintenance
            .sources
            .iter()
            .any(|source| matches!(source, PublishedSourceFence::PendingControl { .. }))
        {
            return Err(integrity(
                "pending source control cannot certify canonical publication",
            ));
        }
        if maintenance
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.space_id != request.space_id)
            || request.finalization.is_some()
        {
            return Err(integrity("invalid maintenance publication authority"));
        }
        if maintenance.origin == CanonicalMaintenanceOrigin::Derived {
            audit_authority::validate(&request.plan)?;
        }
        for effect in &request.plan.effects {
            if let Some(after) = &effect.after {
                if after.base().provenance.sources.iter().any(|source| {
                    !maintenance
                        .sources
                        .iter()
                        .any(|fence| fence.source_id() == source)
                }) {
                    return Err(integrity(
                        "maintenance output claims uncaptured source support",
                    ));
                }
            }
        }
        for dependency in &request.plan.dependencies {
            if let CanonicalRecord::Source(source) = dependency {
                if !maintenance
                    .sources
                    .iter()
                    .any(|fence| fence.source_id() == &source.base.id)
                {
                    return Err(integrity(
                        "maintenance source lacks captured publication fence",
                    ));
                }
            }
        }
    } else if request
        .plan
        .dependencies
        .iter()
        .any(|row| matches!(row, CanonicalRecord::Source(_)))
        || request
            .plan
            .effects
            .iter()
            .filter_map(|effect| effect.after.as_ref())
            .any(|row| !row.base().provenance.sources.is_empty())
    {
        return Err(integrity(
            "source-backed publication requires captured source and attempt fences",
        ));
    }
    Ok(())
}

fn validate_commit_effects(request: &CanonicalCommit) -> StorageResult<()> {
    let mut ids = std::collections::HashSet::new();
    for effect in &request.plan.effects {
        let row = effect.row()?;
        if matches!(
            row,
            CanonicalRecord::WorkItem(_) | CanonicalRecord::WorkRun(_)
        ) {
            return Err(integrity("queue records are read dependencies only"));
        }
        if row.base().space_id != request.space_id || !ids.insert(row.base().id.clone()) {
            return Err(integrity("foreign-space or duplicate canonical effect"));
        }
        if let (Some(before), Some(after)) = (&effect.before, &effect.after) {
            if before.base().id != after.base().id
                || before.base().space_id != after.base().space_id
            {
                return Err(integrity("canonical replacement changed identity"));
            }
        }
    }
    #[allow(
        clippy::mutable_key_type,
        reason = "Validated native identities retain typed equality and are never mutated while stored in this temporary set."
    )]
    let mut extension_ids = std::collections::BTreeSet::new();
    for effect in &request.plan.extension_effects {
        crate::canonical_extension::validate_extension_effect(&request.space_id, effect)?;
        if !extension_ids.insert(effect.id.clone()) {
            return Err(integrity("duplicate canonical extension effect"));
        }
    }
    if request
        .plan
        .dependencies
        .iter()
        .any(|row| row.base().space_id != request.space_id)
        || request
            .plan
            .outbox
            .iter()
            .any(|row| row.base.space_id != request.space_id)
    {
        return Err(integrity("foreign-space canonical dependency or outbox"));
    }
    Ok(())
}

fn check_replayed_commit(
    old: CanonicalCommitReceipt,
    requested: &CanonicalCommitReceipt,
) -> StorageResult<CanonicalCommitReceipt> {
    if old.space_id != requested.space_id
        || old.commit_id != requested.commit_id
        || old.input_fingerprint != requested.input_fingerprint
        || old.plan_fingerprint != requested.plan_fingerprint
        || old.source_id != requested.source_id
        || old.source_revision != requested.source_revision
    {
        return Err(conflict("canonical_idempotency_conflict"));
    }
    Ok(old)
}

fn check_publication_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    context: &CanonicalPublicationContext,
) -> StorageResult<()> {
    let head: SourceHead = read_control_tx(
        tx,
        HEAD_TABLE,
        &head_key(space, &context.source.source_id),
        space,
    )?
    .ok_or_else(|| conflict("source_head_missing"))?;
    check_source_fence(&head, &context.source)?;
    let source = tx
        .get::<Source>(space, &context.source.source_id, false)?
        .ok_or_else(|| conflict("source_missing"))?;
    if source.base.deleted_at.is_some() {
        return Err(conflict("source_retracted"));
    }
    crate::work_attempt_repository::check_attempt_fence(tx, &context.attempt, Utc::now())?;
    Ok(())
}

pub(crate) fn check_source_fence(head: &SourceHead, fence: &PublicationFence) -> StorageResult<()> {
    if head.source_id != fence.source_id
        || head.lifecycle != SourceVisibility::Active
        || head.generation != fence.generation
        || head.operation_id != fence.operation_id
        || head.desired_revision.as_deref() != Some(fence.revision_id.as_str())
    {
        return Err(conflict("stale_source_generation"));
    }
    Ok(())
}

fn append_publication_guard(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    space: &str,
    context: &CanonicalPublicationContext,
) {
    bindings.extend(crate::bindings! {
        "source_head_id" => control_thing(HEAD_TABLE, &head_key(space, &context.source.source_id)),
        "source_record_id" => crate::surreal::thing_from_record_id(&context.source.source_id),
        "source_generation" => context.source.generation.clone(),
        "source_revision" => context.source.revision_id.clone(),
        "source_operation" => context.source.operation_id.clone(),
    });
    // The state projection and payload are encoded together by one helper.
    // Native predicates use state; Rust decodes only the explicit payload proxy.
    sql.push_str("LET $source_head_row = SELECT * FROM ONLY $source_head_id;\nIF $source_head_row = NONE OR $source_head_row.space_id != $space { THROW 'publication:source_head_missing'; };\nLET $source_head = $source_head_row.state;\nIF $source_head.generation != $source_generation OR $source_head.desired_revision != $source_revision OR $source_head.operation_id != $source_operation OR $source_head.lifecycle != 'active' { THROW 'publication:stale_source_generation'; };\nLET $source_record = SELECT * FROM ONLY $source_record_id;\nIF $source_record = NONE OR $source_record.space_id != $space OR $source_record.deleted_at != NONE { THROW 'publication:source_missing'; };\nUPDATE $source_head_id SET fence_nonce = <string>rand::uuid();\n");
    append_attempt(sql, bindings, &context.attempt);
}

/// Normalize nested persisted objects while retaining every top-level field,
/// including `NONE`: a field that was absent must still be absent at commit.
pub(crate) fn native_expected_fields(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(surrealdb::sql::Object::from(
            object
                .into_iter()
                .map(|(key, value)| (key, crate::surreal::stored_native_value(value)))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )),
        other => crate::surreal::stored_native_value(other),
    }
}

pub(crate) fn append_row_guard(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    prefix: &str,
    row: &CanonicalRecord,
    before: Option<&CanonicalRecord>,
) -> StorageResult<()> {
    bindings.insert(
        format!("{prefix}_id"),
        crate::surreal::thing_from_record_id(&row.base().id).into(),
    );
    let queue_dependency = matches!(
        row,
        CanonicalRecord::WorkItem(_) | CanonicalRecord::WorkRun(_)
    );
    if queue_dependency {
        bindings.insert(
            format!("{prefix}_archive"),
            crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(
                &row.base().id,
            ))
            .into(),
        );
        // A present hot row owns the guard, including refusal if it changed.
        // Do not fetch unused cold history for every live queue ancestor.
        // Use a value expression: a bare SELECT inside a statement block has
        // its result discarded by the native evaluator and yields NONE.
        writeln!(sql, "LET ${prefix}_hot = SELECT * FROM ONLY ${prefix}_id;\nLET ${prefix}_cold = IF ${prefix}_hot = NONE THEN (SELECT * FROM ONLY ${prefix}_archive) ELSE NONE END;\nLET ${prefix}_current = IF ${prefix}_hot != NONE THEN ${prefix}_hot ELSE ${prefix}_cold.record END;\nIF ${prefix}_hot = NONE AND (${prefix}_cold.space_id != $space OR ${prefix}_cold.deleted_at != NONE) {{ THROW 'publication:canonical_row_changed'; }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    } else {
        writeln!(
            sql,
            "LET ${prefix}_current = SELECT * FROM ONLY ${prefix}_id;"
        )
        .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    }
    if let Some(before) = before {
        bindings.insert(
            format!("{prefix}_before"),
            native_expected_fields(before.native()?),
        );
        writeln!(sql, "IF !recall::original_row_matches(${prefix}_current, $space, ${prefix}_before) {{ THROW 'publication:canonical_row_changed'; }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    } else {
        writeln!(
            sql,
            "IF ${prefix}_current != NONE {{ THROW 'publication:canonical_row_exists'; }};"
        )
        .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    }
    if queue_dependency {
        writeln!(sql, "IF ${prefix}_hot != NONE {{ UPDATE ${prefix}_id SET canonical_read_nonce=<string>rand::uuid(); }} ELSE {{ UPDATE ${prefix}_archive SET canonical_read_nonce=<string>rand::uuid(); }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    } else if before.is_some()
        && matches!(
            row,
            CanonicalRecord::AuditLog(_)
                | CanonicalRecord::SystemEvent(_)
                | CanonicalRecord::WorkArtifact(_)
        )
    {
        // These operational inputs lack an unconditional canonical epoch event.
        // The exact present-row comparison above plus the real shared revision
        // conflicts every 039 writer, including deletion or a space move.
        writeln!(
            sql,
            "fn::recall_reference_guard({{id:${prefix}_id,space_id:$space}},false);"
        )
        .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    }
    Ok(())
}

pub(crate) fn support_key(
    space: &str,
    source_id: &RecordId,
    revision_id: &str,
    record: &RecordId,
) -> String {
    stable_key(&[
        space,
        &source_id.to_string(),
        revision_id,
        &record.to_string(),
    ])
}

fn write_support_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    commit_id: &str,
    source_id: &RecordId,
    revision_id: &str,
    effect: &CanonicalEffect,
) -> StorageResult<()> {
    let row = effect.row()?;
    let existing_supports = tx.scan_extension_rows("source_effect_support")?;
    let prior: Vec<SourceEffectSupport> = existing_supports
        .iter()
        .filter(|(_, row)| {
            row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
                && row.get("record_id").and_then(serde_json::Value::as_str)
                    == Some(row_id(effect).as_str())
        })
        .map(|(_, row)| {
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| integrity("missing support payload"))
                .and_then(crate::source_publication::decode_payload)
        })
        .collect::<StorageResult<_>>()?;
    if prior
        .iter()
        .any(|support| support.historical_unknown.is_some())
    {
        return Err(conflict("source_write_historical_baseline_unresolved"));
    }
    let created_by_decision = tx
        .scan_extension_rows(crate::source_decision::DECISION_TABLE)?
        .iter()
        .filter(|(_, value)| {
            value.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
        })
        .map(|(_, value)| {
            value
                .get("payload")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| integrity("decision receipt missing payload"))
                .and_then(
                    crate::source_publication::decode_payload::<
                        crate::source_decision::SourceDecisionReceipt,
                    >,
                )
        })
        .collect::<StorageResult<Vec<_>>>()?
        .iter()
        .any(|decision| {
            decision.effects.iter().any(|effect| {
                effect.before.is_none()
                    && effect
                        .after
                        .as_ref()
                        .is_some_and(|after| after.base().id == row.base().id)
            })
        });
    let independent = prior.iter().any(|support| support.independent)
        || (prior.is_empty()
            && !created_by_decision
            && effect
                .before
                .as_ref()
                .is_some_and(|row| row.base().deleted_at.is_none()));
    let support = SourceEffectSupport {
        space_id: space.to_owned(),
        source_id: source_id.clone(),
        source_revision: revision_id.to_owned(),
        record_id: row.base().id.clone(),
        historical_unknown: None,
        independent,
        record_fingerprint: row.fingerprint()?,
        last_commit_id: Some(commit_id.to_owned()),
    };
    let key = support_key(space, source_id, revision_id, &support.record_id);
    tx.upsert_extension_row("source_effect_support", serde_json::json!({ "id": format!("source_effect_support:{key}"), "space_id": space, "source_id": source_id.to_string(), "record_id": support.record_id.to_string(), "source_revision": support.source_revision, "independent": independent, "payload": encode_payload(&support)? }))?;
    Ok(())
}

fn row_id(effect: &CanonicalEffect) -> String {
    effect
        .after
        .as_ref()
        .or(effect.before.as_ref())
        .map(|row| row.base().id.to_string())
        .unwrap_or_default()
}

#[allow(
    clippy::too_many_arguments,
    reason = "The support row binds its original space, source, revision and effect without a second state authority."
)]
fn append_support(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    prefix: &str,
    space: &str,
    commit_id: &str,
    source_id: &RecordId,
    revision_id: &str,
    effect: &CanonicalEffect,
) -> StorageResult<()> {
    let row = effect.row()?;
    let support = SourceEffectSupport {
        space_id: space.to_owned(),
        source_id: source_id.clone(),
        source_revision: revision_id.to_owned(),
        record_id: row.base().id.clone(),
        historical_unknown: None,
        independent: false,
        record_fingerprint: row.fingerprint()?,
        last_commit_id: Some(commit_id.to_owned()),
    };
    bindings.insert(
        format!("{prefix}_support_id"),
        control_thing(
            "source_effect_support",
            &support_key(space, source_id, revision_id, &support.record_id),
        )
        .into(),
    );
    bindings.insert(
        format!("{prefix}_support_source"),
        source_id.to_string().into(),
    );
    bindings.insert(
        format!("{prefix}_support_record"),
        row.base().id.to_string().into(),
    );
    bindings.insert(
        format!("{prefix}_support_revision"),
        revision_id.to_owned().into(),
    );
    let mut independent_support = support.clone();
    independent_support.independent = true;
    bindings.insert(
        format!("{prefix}_support_dependent_payload"),
        encode_payload(&support)?.into(),
    );
    bindings.insert(
        format!("{prefix}_support_independent_payload"),
        encode_payload(&independent_support)?.into(),
    );
    let was_existing = effect
        .before
        .as_ref()
        .is_some_and(|row| row.base().deleted_at.is_none());
    writeln!(sql, "LET ${prefix}_supports = SELECT * FROM source_effect_support WHERE space_id = $space AND record_id = ${prefix}_support_record;\nIF array::any(array::map(${prefix}_supports, |$support| $support.historical_unknown != NONE AND $support.historical_unknown != NULL)) {{ THROW 'source_write_historical_baseline_unresolved'; }};\nLET ${prefix}_decision_owners = SELECT id FROM source_decision_receipt WHERE space_id = $space AND ${prefix}_support_record IN array::map(state.effects, |$effect| IF $effect.before = NULL THEN $effect.after.record.id ELSE NONE END);\nLET ${prefix}_independent = (array::len(${prefix}_supports) = 0 AND array::len(${prefix}_decision_owners) = 0 AND {was_existing}) OR array::any(array::map(${prefix}_supports, |$support| $support.independent));\nUPSERT ${prefix}_support_id SET space_id = $space, source_id = ${prefix}_support_source, record_id = ${prefix}_support_record, source_revision = ${prefix}_support_revision, independent = ${prefix}_independent, payload = IF ${prefix}_independent THEN ${prefix}_support_independent_payload ELSE ${prefix}_support_dependent_payload END;").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    Ok(())
}

fn append_attempt(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    attempt: &AttemptFence,
) {
    bindings.extend(crate::bindings! {
        "attempt_id" => crate::work_attempt_repository::attempt_id(&attempt.work_item_id),
        "attempt_work_item" => crate::surreal::thing_from_record_id(&attempt.work_item_id),
        "attempt_space" => attempt.space_id.clone(),
        "attempt_generation" => attempt.generation,
        "attempt_token" => attempt.lease_token.clone(),
    });
    sql.push_str(crate::work_attempt_repository::ATTEMPT_FENCE_SQL);
}

fn check_maintenance_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    context: &CanonicalMaintenanceContext,
) -> StorageResult<()> {
    if let Some(attempt) = &context.attempt {
        crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
    }
    for fence in &context.sources {
        let head: Option<SourceHead> =
            read_control_tx(tx, HEAD_TABLE, &head_key(space, fence.source_id()), space)?;
        let current = tx
            .get::<Source>(space, fence.source_id(), false)?
            .ok_or_else(|| conflict("maintenance_source_missing"))?;
        match fence {
            PublishedSourceFence::PendingControl {
                head: expected,
                source,
            } => {
                if head.as_ref() != Some(expected.as_ref()) || current != **source {
                    return Err(conflict("maintenance_control_source_changed"));
                }
            }
            PublishedSourceFence::Head {
                generation,
                published_revision,
                ..
            } => {
                let head = head.ok_or_else(|| conflict("maintenance_source_missing"))?;
                if head.lifecycle != SourceVisibility::Active
                    || head.generation != *generation
                    || head.published_revision.as_deref() != Some(published_revision.as_str())
                {
                    return Err(conflict("maintenance_source_changed"));
                }
            }
            PublishedSourceFence::Legacy { source } => {
                if head.is_some() || current != **source {
                    return Err(conflict("maintenance_source_changed"));
                }
            }
        }
    }
    Ok(())
}

fn append_maintenance_guard(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    space: &str,
    context: &CanonicalMaintenanceContext,
) -> StorageResult<()> {
    if let Some(attempt) = &context.attempt {
        append_attempt(sql, bindings, attempt);
    }
    for (index, fence) in context.sources.iter().enumerate() {
        bindings.extend(crate::bindings! {
            format!("maintenance_{index}_head") => control_thing(HEAD_TABLE, &head_key(space, fence.source_id())),
            format!("maintenance_{index}_source") => crate::surreal::thing_from_record_id(fence.source_id()),
        });
        writeln!(sql, "LET $maintenance_{index}_row = SELECT * FROM ONLY $maintenance_{index}_head;\nLET $maintenance_{index}_source_row = SELECT * FROM ONLY $maintenance_{index}_source;\nIF $maintenance_{index}_source_row = NONE OR $maintenance_{index}_source_row.space_id != $space OR $maintenance_{index}_source_row.deleted_at != NONE {{ THROW 'publication:maintenance_source_missing'; }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        match fence {
            PublishedSourceFence::PendingControl { head, source } => {
                bindings.insert(
                    format!("maintenance_{index}_head_payload"),
                    encode_payload(head.as_ref())?.into(),
                );
                bindings.insert(
                    format!("maintenance_{index}_expected"),
                    native_expected_fields(crate::surreal::source_to_value(source)?),
                );
                writeln!(sql, "IF $maintenance_{index}_row.payload != $maintenance_{index}_head_payload {{ THROW 'publication:maintenance_control_source_changed'; }};\nFOR $entry IN object::entries($maintenance_{index}_expected) {{ IF $maintenance_{index}_source_row[$entry[0]] != $entry[1] {{ THROW 'publication:maintenance_control_source_changed'; }}; }};\nUPDATE $maintenance_{index}_head SET fence_nonce=<string>rand::uuid();").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
            }
            PublishedSourceFence::Head {
                generation,
                published_revision,
                ..
            } => {
                bindings.insert(
                    format!("maintenance_{index}_generation"),
                    generation.clone().into(),
                );
                bindings.insert(
                    format!("maintenance_{index}_revision"),
                    published_revision.clone().into(),
                );
                writeln!(sql, "IF $maintenance_{index}_row = NONE OR $maintenance_{index}_row.space_id != $space {{ THROW 'publication:maintenance_source_missing'; }};\nLET $maintenance_{index}_state = $maintenance_{index}_row.state;\nIF $maintenance_{index}_state.lifecycle != 'active' OR $maintenance_{index}_state.generation != $maintenance_{index}_generation OR $maintenance_{index}_state.published_revision != $maintenance_{index}_revision {{ THROW 'publication:maintenance_source_changed'; }};\nUPDATE $maintenance_{index}_head SET fence_nonce = <string>rand::uuid();").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
            }
            PublishedSourceFence::Legacy { source } => {
                bindings.insert(
                    format!("maintenance_{index}_expected"),
                    native_expected_fields(crate::surreal::source_to_value(source)?),
                );
                writeln!(sql, "IF $maintenance_{index}_row != NONE {{ THROW 'publication:maintenance_source_changed'; }};\nFOR $entry IN object::entries($maintenance_{index}_expected) {{ IF $maintenance_{index}_source_row[$entry[0]] != $entry[1] {{ THROW 'publication:maintenance_source_changed'; }}; }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
            }
        }
    }
    Ok(())
}

fn completed_source_rows(
    mut head: SourceHead,
    mut operation: crate::source_publication::StoredOperation,
    fence: &PublicationFence,
) -> StorageResult<(SourceHead, crate::source_publication::StoredOperation)> {
    check_source_fence(&head, fence)?;
    if operation.receipt.source_generation != fence.generation
        || operation.receipt.source_id != fence.source_id
        || operation.receipt.operation_id != fence.operation_id
    {
        return Err(conflict("source_operation_changed"));
    }
    let now = Utc::now();
    head.published_revision = Some(fence.revision_id.clone());
    head.updated_at = now;
    operation.receipt.published_revision = Some(fence.revision_id.clone());
    operation.receipt.state = elephant_types::publication::OperationState::Completed;
    operation.receipt.completed_at = Some(now);
    operation.receipt.error_code = None;
    Ok((head, operation))
}

fn complete_source_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    fence: &PublicationFence,
) -> StorageResult<()> {
    let head: SourceHead =
        read_control_tx(tx, HEAD_TABLE, &head_key(space, &fence.source_id), space)?
            .ok_or_else(|| conflict("source_head_missing"))?;
    let operation = read_control_tx(
        tx,
        crate::source_publication::OPERATION_TABLE,
        &fence.operation_id,
        space,
    )?
    .ok_or_else(|| integrity("source operation missing"))?;
    let (head, operation) = completed_source_rows(head, operation, fence)?;
    write_control_tx(
        tx,
        HEAD_TABLE,
        &head_key(space, &fence.source_id),
        space,
        &fence.source_id,
        &head,
    )?;
    write_control_tx(
        tx,
        crate::source_publication::OPERATION_TABLE,
        &fence.operation_id,
        space,
        &fence.source_id,
        &operation,
    )
}

impl CanonicalRepository {
    async fn append_source_completion(
        &self,
        sql: &mut String,
        bindings: &mut std::collections::BTreeMap<String, Value>,
        space: &str,
        fence: &PublicationFence,
    ) -> StorageResult<()> {
        let repository = SourcePublicationRepository::new(self.client.clone());
        let head = repository
            .head(space, &fence.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        let operation = repository
            .read_control(
                crate::source_publication::OPERATION_TABLE,
                &fence.operation_id,
                space,
            )
            .await?
            .ok_or_else(|| integrity("source operation missing"))?;
        let (head, operation) = completed_source_rows(head, operation, fence)?;
        bindings.extend(crate::bindings! {
            "completed_source_head" => control_value(space, &fence.source_id, &head)?,
            "completed_source_operation_id" => control_thing(crate::source_publication::OPERATION_TABLE, &fence.operation_id),
            "completed_source_operation" => control_value(space, &fence.source_id, &operation)?,
        });
        sql.push_str("UPDATE $source_head_id CONTENT $completed_source_head;\nUPDATE $completed_source_operation_id CONTENT $completed_source_operation;\n");
        drop(repository);
        Ok(())
    }
}

pub(crate) fn canonical_plan_fingerprint(request: &CanonicalCommit) -> StorageResult<String> {
    let semantic = serde_json::json!({
        "source": request.publication.as_ref().map(|context| &context.source),
        "published_support": request.maintenance.as_ref().map(|context| &context.sources),
        "maintenance_origin":request.maintenance.as_ref().map(|context|context.origin),
        "manifest": request.finalization.as_ref().map(crate::source_publication_manifest::SourcePublicationRequirements::manifest),
        "dependencies": &request.plan.dependencies,
        "effects": &request.plan.effects,
        "extension_effects": &request.plan.extension_effects,
        "outbox": &request.plan.outbox,
        "replay_output": &request.replay_output,
    });
    Ok(stable_key(&[&encode_payload(&semantic)?]))
}

impl CanonicalRecord {
    pub(crate) fn base_mut(&mut self) -> &mut BaseFields {
        match self {
            Self::AuditLog(row) => &mut row.base,
            Self::IdMap(row) => &mut row.base,
            Self::ReprocessJob(row) => &mut row.base,
            Self::EntityMerge(row) => &mut row.base,
            Self::IdentityCandidate(row) => &mut row.base,
            Self::SystemEvent(row) => &mut row.base,
            Self::Entity(row) => &mut row.base,
            Self::EntityIdentityKey(row) => &mut row.base,
            Self::Assertion(row) => &mut row.base,
            Self::Rel(row) => &mut row.base,
            Self::Attribute(row) => &mut row.base,
            Self::Event(row) => &mut row.base,
            Self::Freshness(row) => &mut row.base,
            Self::Source(row) => &mut row.base,
            Self::Doc(row) => &mut row.base,
            Self::DocRevision(row) => &mut row.base,
            Self::DocNode(row) => &mut row.base,
            Self::WorkItem(row) => &mut row.base,
            Self::WorkRun(row) => &mut row.base,
            Self::WorkArtifact(row) => &mut row.base,
            Self::TruthSlot(row) => &mut row.base,
            Self::ConflictGroup(row) => &mut row.base,
            Self::PredicateRegistry(row) => &mut row.base,
            Self::KnowledgeViewDefinition(row) => &mut row.base,
            Self::PredicateAlias(row) => &mut row.base,
        }
    }

    pub(crate) fn load_tx(
        tx: &EmbeddedMutation<'_>,
        space: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Self>> {
        Ok(match id.table() {
            elephant_types::TableName::SystemEvent => tx
                .get::<SystemEvent>(space, id, true)?
                .map(|row| Self::SystemEvent(Box::new(row))),
            elephant_types::TableName::IdentityCandidate => tx
                .get::<IdentityCandidate>(space, id, true)?
                .map(|row| Self::IdentityCandidate(Box::new(row))),
            elephant_types::TableName::IdMap => tx
                .get::<IdMap>(space, id, true)?
                .map(|row| Self::IdMap(Box::new(row))),
            elephant_types::TableName::ReprocessJob => tx
                .get::<ReprocessJob>(space, id, true)?
                .map(|row| Self::ReprocessJob(Box::new(row))),
            elephant_types::TableName::EntityMerge => tx
                .get::<EntityMerge>(space, id, true)?
                .map(|row| Self::EntityMerge(Box::new(row))),
            elephant_types::TableName::AuditLog => tx
                .get::<AuditLog>(space, id, true)?
                .map(|row| Self::AuditLog(Box::new(row))),
            elephant_types::TableName::Entity => tx
                .get::<Entity>(space, id, true)?
                .map(|row| Self::Entity(Box::new(row))),
            elephant_types::TableName::EntityIdentityKey => tx
                .get::<EntityIdentityKey>(space, id, true)?
                .map(|row| Self::EntityIdentityKey(Box::new(row))),
            elephant_types::TableName::Assertion => tx
                .get::<Assertion>(space, id, true)?
                .map(|row| Self::Assertion(Box::new(row))),
            elephant_types::TableName::Rel => tx
                .get::<Rel>(space, id, true)?
                .map(|row| Self::Rel(Box::new(row))),
            elephant_types::TableName::Attribute => tx
                .get::<Attribute>(space, id, true)?
                .map(|row| Self::Attribute(Box::new(row))),
            elephant_types::TableName::Event => tx
                .get::<Event>(space, id, true)?
                .map(|row| Self::Event(Box::new(row))),
            elephant_types::TableName::Freshness => tx
                .get::<Freshness>(space, id, true)?
                .map(|row| Self::Freshness(Box::new(row))),
            elephant_types::TableName::Source => tx
                .get::<Source>(space, id, true)?
                .map(|row| Self::Source(Box::new(row))),
            elephant_types::TableName::Doc => tx
                .get::<Doc>(space, id, true)?
                .map(|row| Self::Doc(Box::new(row))),
            elephant_types::TableName::DocRevision => tx
                .get::<DocRevision>(space, id, true)?
                .map(|row| Self::DocRevision(Box::new(row))),
            elephant_types::TableName::DocNode => tx
                .get::<DocNode>(space, id, true)?
                .map(|row| Self::DocNode(Box::new(row))),
            elephant_types::TableName::WorkItem => tx
                .get::<WorkItem>(space, id, true)?
                .or(tx
                    .get::<WorkItemArchive>(space, &crate::work_retention::archive_id(id), false)?
                    .map(|archive| archive.record))
                .map(|row| Self::WorkItem(Box::new(row))),
            elephant_types::TableName::WorkRun => tx
                .get::<WorkRun>(space, id, true)?
                .or(tx
                    .get::<WorkRunArchive>(space, &crate::work_retention::archive_id(id), false)?
                    .map(|archive| archive.record))
                .map(|row| Self::WorkRun(Box::new(row))),
            elephant_types::TableName::WorkArtifact => tx
                .get::<WorkArtifact>(space, id, true)?
                .map(|row| Self::WorkArtifact(Box::new(row))),
            elephant_types::TableName::TruthSlot => tx
                .get::<TruthSlot>(space, id, true)?
                .map(|row| Self::TruthSlot(Box::new(row))),
            elephant_types::TableName::ConflictGroup => tx
                .get::<ConflictGroup>(space, id, true)?
                .map(|row| Self::ConflictGroup(Box::new(row))),
            elephant_types::TableName::PredicateRegistry => tx
                .get::<PredicateRegistry>(space, id, true)?
                .map(|row| Self::PredicateRegistry(Box::new(row))),
            elephant_types::TableName::KnowledgeViewDefinition => tx
                .get::<KnowledgeViewDefinition>(space, id, true)?
                .map(|row| Self::KnowledgeViewDefinition(Box::new(row))),
            elephant_types::TableName::PredicateAlias => tx
                .get::<PredicateAlias>(space, id, true)?
                .map(|row| Self::PredicateAlias(Box::new(row))),
            _ => return Err(integrity("unsupported canonical support record table")),
        })
    }

    pub(crate) async fn load(
        client: &StorageClient,
        space: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Self>> {
        macro_rules! load_record {
            ($repository:path, $method:ident, $variant:ident) => {
                <$repository>::new(client.clone())
                    .$method(space, id)
                    .await?
                    .map(|row| Self::$variant(Box::new(row)))
            };
        }
        Ok(match id.table() {
            elephant_types::TableName::Entity => load_record!(
                crate::entity_repository::EntityRepository,
                get_with_deleted,
                Entity
            ),
            elephant_types::TableName::EntityIdentityKey => load_record!(
                crate::entity_identity_key_repository::EntityIdentityKeyRepository,
                get_including_deleted,
                EntityIdentityKey
            ),
            elephant_types::TableName::Assertion => load_record!(
                crate::assertion_repository::AssertionRepository,
                get_with_deleted,
                Assertion
            ),
            elephant_types::TableName::Rel => {
                load_record!(crate::rel_repository::RelRepository, get_with_deleted, Rel)
            }
            elephant_types::TableName::Attribute => load_record!(
                crate::attribute_repository::AttributeRepository,
                get_with_deleted,
                Attribute
            ),
            elephant_types::TableName::Event => load_record!(
                crate::event_repository::EventRepository,
                get_with_deleted,
                Event
            ),
            elephant_types::TableName::Freshness => load_record!(
                crate::freshness_repository::FreshnessRepository,
                get_including_deleted,
                Freshness
            ),
            elephant_types::TableName::Source => load_record!(
                crate::source_repository::SourceRepository,
                get_with_deleted,
                Source
            ),
            elephant_types::TableName::Doc => {
                load_record!(crate::doc_repository::DocRepository, get_with_deleted, Doc)
            }
            elephant_types::TableName::DocRevision => load_record!(
                crate::doc_revision_repository::DocRevisionRepository,
                get_including_deleted,
                DocRevision
            ),
            elephant_types::TableName::DocNode => load_record!(
                crate::doc_node_repository::DocNodeRepository,
                get_including_deleted,
                DocNode
            ),
            elephant_types::TableName::TruthSlot => load_record!(
                crate::truth_slot_repository::TruthSlotRepository,
                get_including_deleted,
                TruthSlot
            ),
            elephant_types::TableName::ConflictGroup => load_record!(
                crate::conflict_group_repository::ConflictGroupRepository,
                get_including_deleted,
                ConflictGroup
            ),
            elephant_types::TableName::PredicateRegistry => load_record!(
                crate::predicate_registry_repository::PredicateRegistryRepository,
                get_including_deleted,
                PredicateRegistry
            ),
            elephant_types::TableName::PredicateAlias => load_record!(
                crate::predicate_alias_repository::PredicateAliasRepository,
                get_including_deleted,
                PredicateAlias
            ),
            _ => return Box::pin(Self::load_control(client, space, id)).await,
        })
    }

    async fn load_control(
        client: &StorageClient,
        space: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Self>> {
        macro_rules! load_record {
            ($repository:path, $method:ident, $variant:ident) => {
                <$repository>::new(client.clone())
                    .$method(space, id)
                    .await?
                    .map(|row| Self::$variant(Box::new(row)))
            };
        }
        Ok(match id.table() {
            elephant_types::TableName::SystemEvent => load_record!(
                crate::system_event_repository::SystemEventRepository,
                get_including_deleted,
                SystemEvent
            ),
            elephant_types::TableName::IdentityCandidate => load_record!(
                crate::identity_candidate_repository::IdentityCandidateRepository,
                get_including_deleted,
                IdentityCandidate
            ),
            elephant_types::TableName::IdMap => crate::canonical_id_map::load(client, space, id)
                .await?
                .map(|row| Self::IdMap(Box::new(row))),
            elephant_types::TableName::ReprocessJob => load_record!(
                crate::ReprocessJobRepository,
                get_including_deleted,
                ReprocessJob
            ),
            elephant_types::TableName::EntityMerge => load_record!(
                crate::entity_merge_repository::EntityMergeRepository,
                get_including_deleted,
                EntityMerge
            ),
            elephant_types::TableName::AuditLog => load_record!(
                crate::audit_log_repository::AuditLogRepository,
                get_including_deleted,
                AuditLog
            ),
            elephant_types::TableName::WorkItem => {
                crate::work_retention::get_item(client, space, id)
                    .await?
                    .map(|row| Self::WorkItem(Box::new(row)))
            }
            elephant_types::TableName::WorkRun => crate::work_retention::get_run(client, space, id)
                .await?
                .map(|row| Self::WorkRun(Box::new(row))),
            elephant_types::TableName::WorkArtifact => load_record!(
                crate::work_artifact_repository::WorkArtifactRepository,
                get_including_deleted,
                WorkArtifact
            ),
            elephant_types::TableName::KnowledgeViewDefinition => load_record!(
                crate::knowledge_view_definition_repository::KnowledgeViewDefinitionRepository,
                get_including_deleted,
                KnowledgeViewDefinition
            ),
            _ => return Err(integrity("unsupported canonical support record table")),
        })
    }
}

impl CanonicalRepository {
    /// Seeds source promotion with surviving state after retiring its old support.
    /// Capture this before any promotion reads; it performs no live mutations.
    pub async fn prepare_source_replacement(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<CanonicalPlan> {
        let mut prepared = SourcePublicationRepository::new(self.client.clone())
            .prepare_source_replacement(space, publication)
            .await?;
        self.append_replacement_event(
            space,
            publication,
            &mut prepared.plan,
            &prepared.control_deletes,
        )
        .await?;
        Ok(prepared.plan)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Source replacement must reconcile original supports and the complete retirement plan in their existing order."
    )]
    async fn prepare_retirement(
        &self,
        request: &mut CanonicalCommit,
        accepted_contact: Option<&crate::source_contact_publication::AcceptedContactState>,
    ) -> StorageResult<(
        Vec<(String, String)>,
        Option<crate::source_contact_survival_proof::PreparedContactSurvival>,
    )> {
        let Some(publication) = request
            .publication
            .as_ref()
            .map(|context| context.source.clone())
        else {
            return Ok((Vec::new(), None));
        };
        let repository = SourcePublicationRepository::new(self.client.clone());
        let mut required = repository
            .prepare_source_replacement(&request.space_id, &publication)
            .await?;
        if required.plan.observed_epoch != request.plan.observed_epoch {
            return Err(conflict("canonical_inputs_changed"));
        }
        self.append_replacement_event(
            &request.space_id,
            &publication,
            &mut required.plan,
            &required.control_deletes,
        )
        .await?;
        let required_ids = required
            .plan
            .effects
            .iter()
            .filter_map(|effect| {
                effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .map(|row| row.base().id.clone())
            })
            .collect::<std::collections::HashSet<_>>();
        let supports: Vec<SourceEffectSupport> = repository
            .list_controls("source_effect_support", Some(&request.space_id))
            .await?;
        for effect in &request.plan.effects {
            if !required_ids.contains(&effect.row()?.base().id)
                && !accepted_contact
                    .map_or(Ok(false), |contact| contact.certifies_effect(effect, None))?
            {
                validate_supported_enrichment(effect, &supports, &publication)?;
            }
        }
        for expected in required.plan.effects {
            let id = expected.row()?.base().id.clone();
            if let Some(actual) = request
                .plan
                .effects
                .iter()
                .find(|effect| effect.row().is_ok_and(|row| row.base().id == id))
            {
                if !same_optional(actual.before.as_ref(), expected.before.as_ref())?
                    || (!accepted_contact.map_or(Ok(false), |contact| {
                        contact.certifies_effect(actual, Some(&expected))
                    })? && retirement_semantics(actual.after.as_ref())?
                        != retirement_semantics(expected.after.as_ref())?)
                {
                    return Err(conflict("source_replacement_requires_surviving_baseline"));
                }
            } else {
                request.plan.effects.push(expected);
            }
        }
        for expected in required.plan.extension_effects {
            if let Some(actual) = request
                .plan
                .extension_effects
                .iter()
                .find(|effect| effect.id == expected.id)
            {
                if actual.before != expected.before || actual.after != expected.after {
                    return Err(conflict("source_replacement_requires_extension_baseline"));
                }
            } else {
                request.plan.extension_effects.push(expected);
            }
        }
        for event in required.plan.outbox {
            if !request
                .plan
                .outbox
                .iter()
                .any(|existing| existing.base.id == event.base.id)
            {
                request.plan.outbox.push(event);
            }
        }
        // A fresh operation may publish the same immutable revision again.
        // Its support keys are then identical to the keys being retired. The
        // new complete row already replaces those old values atomically; never
        // delete the replacement proof after writing it.
        let republished_supports = request
            .plan
            .effects
            .iter()
            .filter(|effect| {
                effect.after.is_some() && effect_attributes_source(effect, &publication.source_id)
            })
            .map(|effect| {
                Ok(format!(
                    "source_effect_support:{}",
                    support_key(
                        &request.space_id,
                        &publication.source_id,
                        &publication.revision_id,
                        &effect.row()?.base().id
                    )
                ))
            })
            .collect::<StorageResult<std::collections::HashSet<_>>>()?;
        required.control_deletes.retain(|(table, key)| {
            table != "source_effect_support" || !republished_supports.contains(key)
        });
        drop(repository);
        Ok((required.control_deletes, required.contact_survival))
    }

    async fn append_replacement_event(
        &self,
        space: &str,
        publication: &PublicationFence,
        plan: &mut CanonicalPlan,
        controls: &[(String, String)],
    ) -> StorageResult<()> {
        if plan.effects.is_empty() && plan.extension_effects.is_empty() && controls.is_empty() {
            return Ok(());
        }
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(space, &publication.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        let id = RecordId::new(
            elephant_types::TableName::SystemEvent,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{}:replacement", publication.operation_id).as_bytes(),
            ),
        );
        if plan.outbox.iter().any(|event| event.base.id == id) {
            return Ok(());
        }
        let mut base = BaseFields::new(id, space.to_owned(), "source-publication".into());
        base.created_at = head.updated_at;
        base.updated_at = head.updated_at;
        let records=plan.effects.iter().map(|effect|Ok(serde_json::json!({"id":effect.row()?.base().id,"table":effect.row()?.base().id.table().as_str(),"op":if effect.after.as_ref().is_none_or(|after|after.base().deleted_at.is_some()) {"delete"} else {"update"}}))).collect::<StorageResult<Vec<_>>>()?;
        plan.outbox.push(SystemEvent{base,seq:0,event_type:"source.replaced".into(),payload:serde_json::json!({"space_id":space,"event_type":"source.replaced","actor":"source-publication","run_id":null,"records":records})});
        Ok(())
    }
}

fn retirement_semantics(record: Option<&CanonicalRecord>) -> StorageResult<serde_json::Value> {
    let Some(record) = record else {
        return Ok(serde_json::Value::Null);
    };
    let mut value = serde_json::to_value(record).map_err(|error| integrity(&error.to_string()))?;
    if let Some(fields) = value
        .get_mut("record")
        .and_then(serde_json::Value::as_object_mut)
    {
        for field in [
            "updated_at",
            "provenance",
            "evidence",
            "assertion_refs",
            "source_refs",
        ] {
            fields.remove(field);
        }
    }
    Ok(value)
}

fn effect_attributes_source(effect: &CanonicalEffect, source: &RecordId) -> bool {
    effect.after.as_ref().is_some_and(|row| {
        // The Source row is mutable accepted input, governed by its head and
        // immutable revision. It is not its own derived canonical contribution.
        // A self-support fingerprint would certify the prior lifecycle/input,
        // then falsely reject a legitimate pending replacement of that Source.
        if matches!(row, CanonicalRecord::Source(value) if value.base.id == *source) {
            return false;
        }
        row.base().provenance.sources.contains(source)
            || matches!(row, CanonicalRecord::Doc(value) if value.source.as_ref().is_some_and(|link| link.id() == source))
            || matches!(row, CanonicalRecord::Event(value) if value.source_refs.iter().any(|link| link.id() == source))
    })
}

pub(crate) const fn is_derived_graph_table(table: elephant_types::TableName) -> bool {
    matches!(
        table,
        elephant_types::TableName::Entity
            | elephant_types::TableName::EntityIdentityKey
            | elephant_types::TableName::Assertion
            | elephant_types::TableName::Rel
            | elephant_types::TableName::Attribute
            | elephant_types::TableName::Event
            | elephant_types::TableName::Freshness
            | elephant_types::TableName::TruthSlot
            | elephant_types::TableName::ConflictGroup
            | elephant_types::TableName::KnowledgeViewDefinition
    )
}

fn validate_supported_enrichment(
    effect: &CanonicalEffect,
    supports: &[SourceEffectSupport],
    source: &PublicationFence,
) -> StorageResult<()> {
    let (Some(before), Some(after)) = (&effect.before, &effect.after) else {
        return Ok(());
    };
    if !matches!(
        after,
        CanonicalRecord::Entity(_)
            | CanonicalRecord::EntityIdentityKey(_)
            | CanonicalRecord::Rel(_)
            | CanonicalRecord::Attribute(_)
            | CanonicalRecord::Event(_)
    ) {
        return Ok(());
    }
    let prior: Vec<_> = supports
        .iter()
        .filter(|support| support.record_id == before.base().id)
        .collect();
    let exclusively_this_revision = !prior.is_empty()
        && prior.iter().all(|support| {
            !support.independent
                && support.source_id == source.source_id
                && support.source_revision == source.revision_id
        });
    if exclusively_this_revision {
        return Ok(());
    }
    let clean = |record: &CanonicalRecord| -> StorageResult<serde_json::Value> {
        let mut value =
            serde_json::to_value(record).map_err(|error| integrity(&error.to_string()))?;
        if let Some(fields) = value
            .get_mut("record")
            .and_then(serde_json::Value::as_object_mut)
        {
            for field in [
                "updated_at",
                "provenance",
                "evidence",
                "assertion_refs",
                "source_refs",
            ] {
                fields.remove(field);
            }
        }
        Ok(value)
    };
    if clean(before)? != clean(after)? {
        return Err(conflict("shared_record_enrichment_requires_field_support"));
    }
    Ok(())
}

impl CanonicalRepository {
    /// Captures all source authority influencing retained maintenance inputs.
    ///
    /// Call after `epoch`; pass observed dependencies and effect-before rows only.
    /// Proposed outputs derive authority from their separately validated inputs.
    pub async fn capture_maintenance_support(
        &self,
        space: &str,
        records: &[CanonicalRecord],
    ) -> StorageResult<Vec<PublishedSourceFence>> {
        self.capture_maintenance_support_with_extensions(space, records, &[])
            .await
    }

    /// Also captures causal authority from native extension before-images.
    /// Include unchanged material extension reads as before=after effects.
    #[allow(
        clippy::too_many_lines,
        reason = "The exact source and extension read-set is captured together before canonical planning; retain its ordered refusal checks."
    )]
    pub async fn capture_maintenance_support_with_extensions(
        &self,
        space: &str,
        records: &[CanonicalRecord],
        extensions: &[crate::canonical_extension::CanonicalExtensionEffect],
    ) -> StorageResult<Vec<PublishedSourceFence>> {
        use crate::Repository;
        let mut observed = std::collections::BTreeMap::new();
        for record in records {
            let encoded = encode_payload(record)?;
            if observed
                .insert(record.base().id.clone(), encoded.clone())
                .is_some_and(|prior| prior != encoded)
            {
                return Err(integrity(
                    "differing observed snapshots for one maintenance input",
                ));
            }
        }
        let mut sources = std::collections::BTreeSet::new();
        let mut revisions = std::collections::BTreeSet::new();
        let mut docs = std::collections::BTreeSet::new();
        let mut assertions = std::collections::BTreeSet::new();
        let ids = records
            .iter()
            .map(|record| record.base().id.clone())
            .collect::<std::collections::HashSet<_>>();
        for record in records {
            if record.base().space_id != space {
                return Err(integrity("foreign-space maintenance input"));
            }
            for source in &record.base().provenance.sources {
                sources.insert(source.clone());
            }
            match record {
                CanonicalRecord::Source(source) => {
                    sources.insert(source.base.id.clone());
                }
                CanonicalRecord::Doc(doc) => {
                    if let Some(source) = &doc.source {
                        sources.insert(source.id().clone());
                    }
                }
                CanonicalRecord::DocRevision(revision) => {
                    docs.insert(revision.doc.id().clone());
                }
                CanonicalRecord::DocNode(node) => {
                    revisions.insert(node.revision.id().clone());
                }
                CanonicalRecord::Assertion(assertion) => {
                    revisions.insert(assertion.evidence.revision.id().clone());
                }
                CanonicalRecord::Rel(rel) => {
                    for evidence in &rel.evidence {
                        revisions.insert(evidence.span.revision.id().clone());
                    }
                }
                CanonicalRecord::Attribute(attribute) => {
                    for evidence in &attribute.evidence {
                        revisions.insert(evidence.span.revision.id().clone());
                    }
                }
                CanonicalRecord::Event(event) => {
                    for assertion in &event.assertion_refs {
                        assertions.insert(assertion.id().clone());
                    }
                    for source in &event.source_refs {
                        sources.insert(source.id().clone());
                    }
                }
                _ => {}
            }
        }
        let publication = SourcePublicationRepository::new(self.client.clone());
        let decisions: Vec<crate::source_decision::SourceDecisionReceipt> = publication
            .list_controls(crate::source_decision::DECISION_TABLE, Some(space))
            .await?;
        for decision in decisions.iter().filter(|decision| {
            decision.extension_effects.iter().any(|effect| {
                extensions
                    .iter()
                    .any(|input| input.before.is_some() && input.id == effect.id)
            }) || decision.effects.iter().any(|effect| {
                effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .is_some_and(|row| ids.contains(&row.base().id))
            })
        }) {
            sources.extend(decision.source_ids.iter().cloned());
        }
        let support: Vec<SourceEffectSupport> = publication
            .list_controls("source_effect_support", Some(space))
            .await?;
        let input_supports = support
            .into_iter()
            .filter(|support| ids.contains(&support.record_id))
            .collect::<Vec<_>>();
        for support in &input_supports {
            if let Some(record) = records
                .iter()
                .find(|record| record.base().id == support.record_id)
            {
                if is_derived_graph_table(record.base().id.table())
                    && record.fingerprint()? != support.record_fingerprint
                {
                    return Err(conflict("canonical_support_content_changed"));
                }
            }
            sources.insert(support.source_id.clone());
        }
        let optional_revisions = records
            .iter()
            .filter_map(|record| {
                if let CanonicalRecord::Assertion(assertion) = record {
                    if assertion.base.provenance.sources.is_empty()
                        && !input_supports
                            .iter()
                            .any(|support| support.record_id == assertion.base.id)
                    {
                        return Some(assertion.evidence.revision.id().clone());
                    }
                }
                None
            })
            .collect::<std::collections::HashSet<_>>();
        for id in assertions {
            let assertion =
                crate::assertion_repository::AssertionRepository::new(self.client.clone())
                    .get(space, &id)
                    .await?
                    .ok_or_else(|| conflict("maintenance_assertion_missing"))?;
            revisions.insert(assertion.evidence.revision.id().clone());
        }
        for id in revisions {
            let revision =
                crate::doc_revision_repository::DocRevisionRepository::new(self.client.clone())
                    .get(space, &id)
                    .await?;
            let Some(revision) = revision else {
                if optional_revisions.contains(&id) {
                    continue;
                }
                return Err(conflict("maintenance_revision_missing"));
            };
            docs.insert(revision.doc.id().clone());
        }
        for id in docs {
            let doc = crate::DocRepository::new(self.client.clone())
                .get(space, &id)
                .await?
                .ok_or_else(|| conflict("maintenance_document_missing"))?;
            if let Some(source) = doc.source {
                sources.insert(source.id().clone());
            }
        }
        for effect in extensions {
            crate::canonical_extension::validate_extension_effect(space, effect)?;
            if let Some(before) = &effect.before {
                extension_source_refs(before, &mut sources);
            }
        }
        let mut fences = Vec::new();
        for source_id in sources {
            let source = crate::SourceRepository::new(self.client.clone())
                .get(space, &source_id)
                .await?
                .ok_or_else(|| conflict("maintenance_source_missing"))?;
            if let Some(head) = publication.head(space, &source_id).await? {
                if head.lifecycle != SourceVisibility::Active {
                    return Err(conflict("maintenance_source_not_active"));
                }
                let published_revision = head
                    .published_revision
                    .ok_or_else(|| conflict("maintenance_source_not_published"))?;
                if input_supports.iter().any(|support| {
                    support.source_id == source_id && support.source_revision != published_revision
                }) {
                    return Err(conflict("maintenance_support_revision_changed"));
                }
                fences.push(PublishedSourceFence::Head {
                    source_id,
                    generation: head.generation,
                    published_revision,
                });
            } else {
                if input_supports
                    .iter()
                    .any(|support| support.source_id == source_id)
                {
                    return Err(conflict("maintenance_support_head_missing"));
                }
                fences.push(PublishedSourceFence::Legacy {
                    source: Box::new(source),
                });
            }
        }
        Ok(fences)
    }
}

// Support rows inherit captured published revisions, never a newer desired head.
fn maintenance_supports(
    context: &CanonicalMaintenanceContext,
    effect: &CanonicalEffect,
) -> Vec<(RecordId, String)> {
    if context.origin == CanonicalMaintenanceOrigin::ControlOnly {
        return Vec::new();
    }
    let Some(after) = &effect.after else {
        return Vec::new();
    };
    if after.base().deleted_at.is_some() {
        return Vec::new();
    }
    context
        .sources
        .iter()
        .filter_map(|fence| {
            let PublishedSourceFence::Head {
                source_id,
                published_revision,
                ..
            } = fence
            else {
                return None;
            };
            // Audit snapshots inherit their affected record's source envelope.
            // The transaction's causal union still fences and retracts the
            // complete decision; it is not independent support for every audit.
            if !after.base().provenance.sources.contains(source_id) {
                return None;
            }
            Some((source_id.clone(), published_revision.clone()))
        })
        .collect()
}

#[cfg(test)]
#[path = "canonical_commit_tests.rs"]
mod tests;

pub(crate) fn causal_source_ids(request: &CanonicalCommit) -> Vec<RecordId> {
    let mut ids = std::collections::BTreeSet::new();
    if let Some(publication) = &request.publication {
        ids.insert(publication.source.source_id.clone());
    }
    if let Some(maintenance) = &request.maintenance {
        ids.extend(
            maintenance
                .sources
                .iter()
                .map(|source| source.source_id().clone()),
        );
    }
    ids.into_iter().collect()
}

/// JSON objects have no semantic field order, including objects inside arrays.
/// Keep array order and every scalar representation intact.
pub(crate) fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        scalar => scalar,
    }
}

/// Fingerprints the persisted domain row used as grounded support proof.
///
/// Only bookkeeping update time is excluded; evidence, policy, provenance and
/// semantic fields are all bound. Pass a domain row, not a tagged CanonicalRecord.
pub fn source_support_fingerprint<T: Serialize>(record: &T) -> StorageResult<String> {
    let mut value = serde_json::to_value(record).map_err(|error| integrity(&error.to_string()))?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| integrity("support fingerprint requires record object"))?;
    fields.remove("updated_at");
    Ok(stable_key(&[&encode_payload(&canonical_json(value))?]))
}

impl CanonicalRecord {
    /// Returns the same semantic proof as fingerprinting this variant's domain row.
    pub fn fingerprint(&self) -> StorageResult<String> {
        let value = serde_json::to_value(self).map_err(|error| integrity(&error.to_string()))?;
        source_support_fingerprint(
            value
                .get("record")
                .ok_or_else(|| integrity("canonical row shape"))?,
        )
    }
}

pub(crate) fn write_existing_support_tx(
    tx: &mut EmbeddedMutation<'_>,
    support: &SourceEffectSupport,
) -> StorageResult<()> {
    let key = support_key(
        &support.space_id,
        &support.source_id,
        &support.source_revision,
        &support.record_id,
    );
    tx.upsert_extension_row("source_effect_support",serde_json::json!({"id":format!("source_effect_support:{key}"),"space_id":support.space_id,"source_id":support.source_id.to_string(),"source_revision":support.source_revision,"record_id":support.record_id.to_string(),"independent":support.independent,"record_fingerprint":support.record_fingerprint,"historical_unknown":support.historical_unknown,"payload":encode_payload(support)?}))?;
    Ok(())
}

pub(crate) fn append_existing_support_sql(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    prefix: &str,
    support: &SourceEffectSupport,
) -> StorageResult<()> {
    let key = support_key(
        &support.space_id,
        &support.source_id,
        &support.source_revision,
        &support.record_id,
    );
    bindings.insert(
        format!("{prefix}_id"),
        control_thing("source_effect_support", &key).into(),
    );
    bindings.insert(format!("{prefix}_value"),crate::surreal::json_to_surreal(serde_json::json!({"space_id":support.space_id,"source_id":support.source_id.to_string(),"source_revision":support.source_revision,"record_id":support.record_id.to_string(),"independent":support.independent,"record_fingerprint":support.record_fingerprint,"historical_unknown":support.historical_unknown,"payload":encode_payload(support)?}))?);
    writeln!(sql, "UPSERT ${prefix}_id CONTENT ${prefix}_value;")
        .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    Ok(())
}

impl CanonicalRepository {
    async fn prepare_support_updates(
        &self,
        request: &CanonicalCommit,
    ) -> StorageResult<Vec<SourceEffectSupport>> {
        if request
            .maintenance
            .as_ref()
            .is_some_and(|context| context.origin == CanonicalMaintenanceOrigin::ControlOnly)
        {
            return Ok(Vec::new());
        }
        let supports: Vec<SourceEffectSupport> =
            SourcePublicationRepository::new(self.client.clone())
                .list_controls("source_effect_support", Some(&request.space_id))
                .await?;
        if request.plan.effects.iter().any(|effect| {
            effect.row().is_ok_and(|row| {
                supports.iter().any(|support| {
                    support.record_id == row.base().id && support.historical_unknown.is_some()
                })
            })
        }) {
            return Err(conflict("source_write_historical_baseline_unresolved"));
        }
        for observed in request.plan.dependencies.iter().chain(
            request
                .plan
                .effects
                .iter()
                .filter_map(|effect| effect.before.as_ref()),
        ) {
            if !is_derived_graph_table(observed.base().id.table()) {
                continue;
            }
            let fingerprint = observed.fingerprint()?;
            if supports.iter().any(|support| {
                support.record_id == observed.base().id && support.record_fingerprint != fingerprint
            }) {
                return Err(conflict("canonical_support_content_changed"));
            }
        }
        if request.maintenance.as_ref().is_some_and(|context| {
            context.origin == CanonicalMaintenanceOrigin::IndependentMutation
        }) {
            for effect in &request.plan.effects {
                validate_independent_factual_proof(effect, &supports, request)?;
            }
        }
        let mut updates = Vec::new();
        for effect in &request.plan.effects {
            let Some(after) = effect.after.as_ref() else {
                continue;
            };
            let fingerprint = after.fingerprint()?;
            for support in supports
                .iter()
                .filter(|support| support.record_id == after.base().id)
            {
                let mut support = support.clone();
                support.record_fingerprint.clone_from(&fingerprint);
                support.last_commit_id = Some(request.commit_id.clone());
                updates.push(support);
            }
        }
        Ok(updates)
    }
}

fn receipt_authority(receipt: &CanonicalCommitReceipt) -> Vec<PublishedSourceFence> {
    let mut authority = receipt.maintenance_authority.clone();
    if let Some(publication) = &receipt.publication_authority {
        authority.push(PublishedSourceFence::Head {
            source_id: publication.source_id.clone(),
            generation: publication.generation.clone(),
            published_revision: publication.revision_id.clone(),
        });
    }
    authority
}

fn check_replay_source(
    source: &Source,
    head: Option<&SourceHead>,
    fence: &PublishedSourceFence,
) -> StorageResult<()> {
    let valid = match fence {
        PublishedSourceFence::PendingControl {
            head: expected,
            source: original,
        } => head == Some(expected.as_ref()) && source == original.as_ref(),
        PublishedSourceFence::Head {
            source_id,
            generation,
            published_revision,
        } => head.is_some_and(|head| {
            head.source_id == *source_id
                && head.generation == *generation
                && head.lifecycle == SourceVisibility::Active
                && head.published_revision.as_deref() == Some(published_revision.as_str())
        }),
        PublishedSourceFence::Legacy { source: expected } => {
            head.is_none() && source == expected.as_ref()
        }
    };
    if !valid || source.base.deleted_at.is_some() {
        return Err(conflict("receipt_source_not_current"));
    }
    Ok(())
}

fn check_receipt_authority_tx(
    tx: &EmbeddedMutation<'_>,
    receipt: &CanonicalCommitReceipt,
) -> StorageResult<()> {
    for fence in receipt_authority(receipt) {
        let source = tx
            .get::<Source>(&receipt.space_id, fence.source_id(), false)?
            .ok_or_else(|| conflict("receipt_source_not_current"))?;
        let head: Option<SourceHead> = read_control_tx(
            tx,
            HEAD_TABLE,
            &head_key(&receipt.space_id, fence.source_id()),
            &receipt.space_id,
        )?;
        check_replay_source(&source, head.as_ref(), &fence)?;
    }
    Ok(())
}

/// Installs intrinsic native mutation fences for declared extension-owned tables.
///
/// Call before capturing an identity-merge planning epoch, including empty tables.
/// Embedded extension mutations already carry this intrinsic fence.
pub async fn ensure_extension_epoch_guards(
    client: &StorageClient,
    tables: &[String],
) -> StorageResult<()> {
    let mut names = std::collections::BTreeSet::new();
    for table in tables {
        crate::canonical_extension::validate_extension_table(table)?;
        names.insert(table);
    }
    if client.embedded_store().is_some() {
        return Ok(());
    }
    crate::reference_fence::install_native_reference_guards(client, tables).await?;
    let mut sql = String::new();
    for table in names {
        writeln!(sql, r"
DEFINE EVENT OVERWRITE recall_extension_epoch ON TABLE {table}
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {{
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE {{ UPSERT type::thing('canonical_epoch',$space) SET space_id=$space,epoch=<string>rand::uuid(); }};
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {{ UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid(); }};
}};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        sql.push_str(&crate::reference_fence::native_reference_event(table));
    }
    if !sql.is_empty() {
        client.query_checked(&sql).await?;
    }
    Ok(())
}

fn extension_source_refs(value: &Value, sources: &mut std::collections::BTreeSet<RecordId>) {
    match value {
        Value::Thing(id) if id.tb == "source" => {
            if let Ok(id) = crate::surreal::record_id_from_thing(id) {
                sources.insert(id);
            }
        }
        Value::Strand(text) => {
            if let Ok(id) = text.as_str().parse::<RecordId>() {
                if id.table() == elephant_types::TableName::Source {
                    sources.insert(id);
                }
            }
        }
        Value::Object(object) => {
            for value in object.values() {
                extension_source_refs(value, sources);
            }
        }
        Value::Array(array) => {
            for value in &array.0 {
                extension_source_refs(value, sources);
            }
        }
        _ => {}
    }
}

// A content fingerprint certifies a validated row, not entailment of a new fact.
// Public typed validators check genuinely new evidence; this guard prevents an
// independent edit from recycling an unchanged proof for a changed signature.
fn validate_independent_factual_proof(
    effect: &CanonicalEffect,
    supports: &[SourceEffectSupport],
    request: &CanonicalCommit,
) -> StorageResult<()> {
    let (Some(before), Some(after)) = (&effect.before, &effect.after) else {
        return Ok(());
    };
    if factual_signature(before) == factual_signature(after) {
        return Ok(());
    }
    let Some(_) = factual_signature(before) else {
        return Ok(());
    };
    let previous_proofs = documentary_proofs(before)?;
    let source_supported = !before.base().provenance.sources.is_empty()
        || supports
            .iter()
            .any(|support| support.record_id == before.base().id)
        || matches!(before,CanonicalRecord::Rel(row) if !row.evidence.is_empty())
        || matches!(before,CanonicalRecord::Attribute(row) if !row.evidence.is_empty());
    if !source_supported {
        return Ok(());
    }
    let next_proofs = documentary_proofs(after)?;
    if next_proofs.is_empty()
        || (next_proofs
            .iter()
            .any(|proof| previous_proofs.contains(proof))
            && !existing_matching_claim_proof(before, after, supports, request)?)
    {
        return Err(conflict("factual_change_requires_coherent_new_evidence"));
    }
    Ok(())
}

fn factual_signature(record: &CanonicalRecord) -> Option<serde_json::Value> {
    match record {
        CanonicalRecord::Rel(row) => Some(
            serde_json::json!({"subject":row.subject,"predicate":row.rel_type,"object":row.object,"validity_window":row.validity_window}),
        ),
        CanonicalRecord::Attribute(row) => Some(
            serde_json::json!({"subject":row.subject,"predicate":row.attr_type,"literal":row.value,"validity_window":row.validity_window}),
        ),
        CanonicalRecord::Assertion(row) => Some(
            serde_json::json!({"subject_text":row.subject_text,"predicate_raw":row.predicate_raw,"predicate_id":row.predicate_id,"object_text":row.object_text,"literal":row.object_literal,"subject_entity":row.subject_entity,"object_entity":row.object_entity,"source_event_time":row.source_event_time}),
        ),
        _ => None,
    }
}

fn documentary_proofs(
    record: &CanonicalRecord,
) -> StorageResult<std::collections::BTreeSet<String>> {
    let mut proofs = std::collections::BTreeSet::new();
    let citations = match record {
        CanonicalRecord::Rel(row) => Some(&row.evidence),
        CanonicalRecord::Attribute(row) => Some(&row.evidence),
        CanonicalRecord::Assertion(row) => {
            proofs.insert(encode_payload(&row.evidence)?);
            None
        }
        _ => None,
    };
    if let Some(citations) = citations {
        for citation in citations {
            // Recency interpretation is not new documentary evidence.
            proofs.insert(encode_payload(&citation.span)?);
        }
    }
    Ok(proofs)
}

// Existing, currently published assertions can support a correction within the
// same passage. A freshly fabricated assertion ID has no observed certificate.
fn existing_matching_claim_proof(
    before: &CanonicalRecord,
    after: &CanonicalRecord,
    supports: &[SourceEffectSupport],
    request: &CanonicalCommit,
) -> StorageResult<bool> {
    let citations = match (before, after) {
        (CanonicalRecord::Rel(old), CanonicalRecord::Rel(new))
            if old.validity_window == new.validity_window =>
        {
            &new.evidence
        }
        (CanonicalRecord::Attribute(old), CanonicalRecord::Attribute(new))
            if old.validity_window == new.validity_window =>
        {
            &new.evidence
        }
        _ => return Ok(false),
    };
    if citations.is_empty() {
        return Ok(false);
    }
    for citation in citations {
        let Some(CanonicalRecord::Assertion(assertion)) = request
            .plan
            .dependencies
            .iter()
            .find(|record| record.base().id == *citation.assertion.id())
        else {
            return Ok(false);
        };
        if assertion.base.deleted_at.is_some()
            || assertion.evidence != citation.span
            || assertion.resolution_status != ResolutionStatus::Resolved
        {
            return Ok(false);
        }
        let proof = source_support_fingerprint(assertion.as_ref())?;
        if !supports.iter().any(|support|support.record_id==assertion.base.id && support.record_fingerprint==proof && request.maintenance.as_ref().is_some_and(|context|context.sources.iter().any(|source|matches!(source,PublishedSourceFence::Head{source_id,published_revision,..} if *source_id==support.source_id && *published_revision==support.source_revision)))) { return Ok(false); }
        let predicate = observed_predicate(
            request,
            assertion
                .predicate_id
                .as_deref()
                .unwrap_or(&assertion.predicate_raw),
        );
        let coherent = match after {
            CanonicalRecord::Rel(row) => {
                assertion.subject_entity.as_ref() == Some(&row.subject)
                    && assertion.object_entity.as_ref() == Some(&row.object)
                    && assertion.object_literal.is_none()
                    && predicate == observed_predicate(request, &row.rel_type)
            }
            CanonicalRecord::Attribute(row) => {
                assertion.subject_entity.as_ref() == Some(&row.subject)
                    && assertion.object_entity.is_none()
                    && predicate == observed_predicate(request, &row.attr_type)
                    && assertion.object_literal.as_ref() == Some(&attribute_literal(&row.value)?)
            }
            _ => false,
        };
        if !coherent {
            return Ok(false);
        }
    }
    Ok(true)
}

fn observed_predicate<'a>(request: &'a CanonicalCommit, predicate: &'a str) -> &'a str {
    request
        .plan
        .dependencies
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::PredicateAlias(alias)
                if alias.alias == predicate.trim()
                    && alias.base.deleted_at.is_none()
                    && alias.status == elephant_types::records::predicate::AliasStatus::Active =>
            {
                Some(alias.predicate_id.as_str())
            }
            _ => None,
        })
        .unwrap_or_else(|| predicate.trim())
}

fn attribute_literal(
    literal: &elephant_types::records::attribute::Literal,
) -> StorageResult<serde_json::Value> {
    use elephant_types::records::attribute::Literal;
    Ok(match literal {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => serde_json::Value::String(value.clone()),
        Literal::Integer(value) => (*value).into(),
        Literal::Float(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(*value)
                .ok_or_else(|| integrity("nonfinite claim literal"))?,
        ),
        Literal::Boolean(value) => (*value).into(),
        Literal::Json(value) => value.clone(),
    })
}
