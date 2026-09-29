//! Truth maintenance work kinds for conflict detection and resolution (Phase 7).
//!
//! Per spec Section 11-12, these work kinds manage:
//! - Conflict detection from competing claims
//! - Conflict resolution using policies
//! - Temporal model handling (validity windows, supersession)
//! - Truth slot maintenance
//! - Outbox-triggered continuous maintenance

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
use elephant_types::records::assertion::Assertion;
use elephant_types::records::attribute::Attribute;
use elephant_types::records::base::Security;
use elephant_types::records::entity::Entity;
use elephant_types::records::event::{Event, EventStatus};
use elephant_types::records::predicate::{
    Cardinality, PredicateAlias, PredicateRegistry, PredicateSemantics, SemanticsStatus,
};
use elephant_types::records::rel::{
    ClaimStatus, EvidenceRef, Rel, TimeBasis, ValidityWindow as RelValidityWindow,
};
use elephant_types::records::temporal::{
    RecencyBasis, RefinementPolicy, TemporalAudit, TemporalPersonality, TimeRefinement,
};
use elephant_types::records::truth::{ConflictGroup, ConflictPolicy, CurrentValue, TruthSlot};
use elephant_types::records::{AuditAction, AuditLog, SystemEvent};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::PipelineError;
use crate::freshness::{apply_freshness_to_ranking, TruthSlotRanking};

pub mod adjudicator;

use self::adjudicator::{
    AdjudicationClaim, AdjudicationDecision, AdjudicationInput, AdjudicationVerdict, ClaimValue,
    SlotAdjudicator,
};

/// Injection seam for LLM adjudication.
///
/// Constructed once in the pipeline binary from environment configuration;
/// library code never reads env vars. `adjudicator: None` means deterministic
/// policies only (keyless deployments and most tests).
#[derive(Debug, Clone)]
pub struct TmContext {
    /// Optional slot adjudicator; `None` disables LLM adjudication.
    pub adjudicator: Option<Arc<dyn SlotAdjudicator>>,
}

impl TmContext {
    /// Context without an adjudicator (deterministic policies only).
    #[must_use]
    pub const fn none() -> Self {
        Self { adjudicator: None }
    }
}

// ============================================================================
// Storage Abstraction
// ============================================================================

/// Trait for truth maintenance storage operations.
///
/// This abstraction allows testing with mock storage and production use
/// with real repositories.
#[async_trait]
pub trait TruthMaintenanceStorage: Send + Sync {
    /// Finds a truth slot by slot key.
    async fn find_slot_by_key(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<TruthSlot>, PipelineError>;

    /// Creates a new truth slot.
    async fn create_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError>;

    /// Updates an existing truth slot.
    async fn update_slot(
        &self,
        space_id: &str,
        id: &RecordId,
        slot: TruthSlot,
    ) -> Result<TruthSlot, PipelineError>;

    /// Upserts a truth slot by ID.
    async fn upsert_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError>;

    /// Finds an unresolved conflict group by slot key.
    async fn find_unresolved_conflict(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError>;

    /// Finds an unresolved conflict group by slot key and slot type.
    async fn find_unresolved_conflict_by_type(
        &self,
        space_id: &str,
        slot_key: &str,
        slot_type: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError>;

    /// Gets a conflict group by ID regardless of resolution status.
    async fn get_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<ConflictGroup>, PipelineError>;

    /// Creates a new conflict group.
    async fn create_conflict_group(
        &self,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError>;

    /// Upserts an unresolved conflict group, preventing duplicate groups per slot.
    async fn upsert_unresolved_conflict_group(
        &self,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError>;

    /// Updates an existing conflict group.
    async fn update_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError>;

    /// Merges claim IDs into an unresolved conflict group, if still unresolved.
    async fn merge_unresolved_conflict_claims(
        &self,
        space_id: &str,
        id: &RecordId,
        claim_ids: Vec<String>,
    ) -> Result<Option<ConflictGroup>, PipelineError>;

    /// Gets a rel by ID.
    async fn get_rel(&self, space_id: &str, id: &RecordId) -> Result<Option<Rel>, PipelineError>;

    /// Gets a rel by ID, including soft-deleted ones.
    async fn get_rel_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Rel>, PipelineError>;

    /// Updates an existing rel.
    async fn update_rel(
        &self,
        space_id: &str,
        id: &RecordId,
        rel: Rel,
    ) -> Result<Rel, PipelineError>;

    /// Soft-deletes a rel (corroborated duplicate cleanup).
    async fn delete_rel(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError>;

    /// Gets an attribute by ID.
    async fn get_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError>;

    /// Gets an attribute by ID, including soft-deleted ones.
    async fn get_attribute_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError>;

    /// Updates an existing attribute.
    async fn update_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
        attr: Attribute,
    ) -> Result<Attribute, PipelineError>;

    /// Soft-deletes an attribute (corroborated duplicate cleanup).
    async fn delete_attribute(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError>;

    /// Gets an event by ID.
    async fn get_event(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError>;

    /// Gets an event by ID, including soft-deleted ones.
    async fn get_event_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError>;

    /// Updates an existing event.
    async fn update_event(
        &self,
        space_id: &str,
        id: &RecordId,
        event: Event,
    ) -> Result<Event, PipelineError>;

    /// Soft-deletes an event (corroborated duplicate cleanup).
    async fn delete_event(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError>;

    /// Lists events for a space (non-deleted).
    async fn list_events(&self, space_id: &str, limit: usize) -> Result<Vec<Event>, PipelineError>;

    /// Finds rels for a truth slot.
    async fn find_rels_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError>;

    /// Finds all active rels for a subject and predicate across all objects.
    async fn find_rels_for_subject_predicate(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError>;

    /// Gets an entity by ID (used for display names in adjudication).
    async fn get_entity(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Entity>, PipelineError>;

    /// Gets an evidence assertion by ID (used for adjudication context).
    async fn get_assertion(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Assertion>, PipelineError>;

    /// Finds attributes for a truth slot.
    async fn find_attrs_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        attr_type: &str,
    ) -> Result<Vec<Attribute>, PipelineError>;

    /// Finds stale truth slots for recomputation.
    async fn find_stale_slots(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<TruthSlot>, PipelineError>;

    /// Finds predicate registry entry by canonical predicate_id.
    async fn find_predicate_registry_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Option<PredicateRegistry>, PipelineError>;

    /// Gets predicate registry entry by record ID.
    async fn get_predicate_registry(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateRegistry>, PipelineError>;

    /// Gets predicate alias entry by record ID.
    async fn get_predicate_alias(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateAlias>, PipelineError>;

    /// Lists predicate aliases for a predicate_id.
    async fn list_predicate_aliases_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Vec<PredicateAlias>, PipelineError>;

    /// Upserts predicate semantics without overwriting operator configuration.
    async fn upsert_predicate_semantics(
        &self,
        space_id: &str,
        predicate_id: &str,
        semantics: PredicateSemantics,
        status: SemanticsStatus,
        provenance_note: &str,
    ) -> Result<PredicateRegistry, PipelineError>;

    /// Creates an audit log entry.
    async fn create_audit_log(&self, log: AuditLog) -> Result<AuditLog, PipelineError>;

    /// Emits a governance/system event (outbox payload).
    async fn emit_system_event(&self, payload: OutboxPayload) -> Result<(), PipelineError>;

    /// Marks slots stale for a predicate.
    async fn mark_slots_stale_by_predicate(
        &self,
        space_id: &str,
        predicate_id: &str,
        reason: &str,
    ) -> Result<(), PipelineError>;

    /// Marks slots stale for an entity.
    async fn mark_slots_stale_by_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        reason: &str,
    ) -> Result<(), PipelineError>;

    /// Marks all slots stale within a space.
    async fn mark_all_slots_stale(&self, space_id: &str, reason: &str)
        -> Result<(), PipelineError>;

    /// Finds freshness score for a target record.
    async fn find_freshness_score(
        &self,
        space_id: &str,
        target: &str,
    ) -> Result<Option<f32>, PipelineError>;
}

/// Production implementation using real repositories.
pub struct RepositoryTruthMaintenanceStorage {
    truth_slot_repo: elephant_storage::TruthSlotRepository,
    conflict_group_repo: elephant_storage::ConflictGroupRepository,
    rel_repo: elephant_storage::RelRepository,
    attr_repo: elephant_storage::AttributeRepository,
    event_repo: elephant_storage::EventRepository,
    entity_repo: elephant_storage::EntityRepository,
    assertion_repo: elephant_storage::AssertionRepository,
    freshness_repo: elephant_storage::FreshnessRepository,
    predicate_registry_repo: elephant_storage::PredicateRegistryRepository,
    predicate_alias_repo: elephant_storage::PredicateAliasRepository,
    audit_log_repo: elephant_storage::AuditLogRepository,
    outbox_writer: elephant_outbox::OutboxWriter,
}

impl RepositoryTruthMaintenanceStorage {
    /// Creates a new storage from a `StorageClient`.
    pub fn new(client: elephant_storage::StorageClient) -> Self {
        Self {
            truth_slot_repo: elephant_storage::TruthSlotRepository::new(client.clone()),
            conflict_group_repo: elephant_storage::ConflictGroupRepository::new(client.clone()),
            rel_repo: elephant_storage::RelRepository::new(client.clone()),
            attr_repo: elephant_storage::AttributeRepository::new(client.clone()),
            event_repo: elephant_storage::EventRepository::new(client.clone()),
            entity_repo: elephant_storage::EntityRepository::new(client.clone()),
            assertion_repo: elephant_storage::AssertionRepository::new(client.clone()),
            freshness_repo: elephant_storage::FreshnessRepository::new(client.clone()),
            predicate_registry_repo: elephant_storage::PredicateRegistryRepository::new(
                client.clone(),
            ),
            predicate_alias_repo: elephant_storage::PredicateAliasRepository::new(client.clone()),
            audit_log_repo: elephant_storage::AuditLogRepository::new(client.clone()),
            outbox_writer: elephant_outbox::OutboxWriter::new(client),
        }
    }

    /// Creates storage from individual repositories.
    #[allow(clippy::too_many_arguments)]
    pub const fn from_repos(
        truth_slot_repo: elephant_storage::TruthSlotRepository,
        conflict_group_repo: elephant_storage::ConflictGroupRepository,
        rel_repo: elephant_storage::RelRepository,
        attr_repo: elephant_storage::AttributeRepository,
        event_repo: elephant_storage::EventRepository,
        entity_repo: elephant_storage::EntityRepository,
        assertion_repo: elephant_storage::AssertionRepository,
        freshness_repo: elephant_storage::FreshnessRepository,
        predicate_registry_repo: elephant_storage::PredicateRegistryRepository,
        predicate_alias_repo: elephant_storage::PredicateAliasRepository,
        audit_log_repo: elephant_storage::AuditLogRepository,
        outbox_writer: elephant_outbox::OutboxWriter,
    ) -> Self {
        Self {
            truth_slot_repo,
            conflict_group_repo,
            rel_repo,
            attr_repo,
            event_repo,
            entity_repo,
            assertion_repo,
            freshness_repo,
            predicate_registry_repo,
            predicate_alias_repo,
            audit_log_repo,
            outbox_writer,
        }
    }
}

#[async_trait]
impl TruthMaintenanceStorage for RepositoryTruthMaintenanceStorage {
    async fn find_slot_by_key(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<TruthSlot>, PipelineError> {
        self.truth_slot_repo
            .find_by_slot_key(space_id, slot_key)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn create_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
        use elephant_storage::Repository;
        self.truth_slot_repo
            .create(slot)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn update_slot(
        &self,
        space_id: &str,
        id: &RecordId,
        slot: TruthSlot,
    ) -> Result<TruthSlot, PipelineError> {
        use elephant_storage::Repository;
        self.truth_slot_repo
            .update(space_id, id, slot)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn upsert_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
        self.truth_slot_repo
            .upsert(slot)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_unresolved_conflict(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.conflict_group_repo
            .find_unresolved_by_slot_key(space_id, slot_key)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_unresolved_conflict_by_type(
        &self,
        space_id: &str,
        slot_key: &str,
        slot_type: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.conflict_group_repo
            .find_unresolved_by_slot_key_and_type(space_id, slot_key, slot_type)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        use elephant_storage::Repository;
        self.conflict_group_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn create_conflict_group(
        &self,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        use elephant_storage::Repository;
        self.conflict_group_repo
            .create(group)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn upsert_unresolved_conflict_group(
        &self,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        self.conflict_group_repo
            .upsert(group)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn update_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
        group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        use elephant_storage::Repository;
        self.conflict_group_repo
            .update(space_id, id, group)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn merge_unresolved_conflict_claims(
        &self,
        space_id: &str,
        id: &RecordId,
        claim_ids: Vec<String>,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.conflict_group_repo
            .merge_unresolved_claim_ids(space_id, id, claim_ids)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_rel(&self, space_id: &str, id: &RecordId) -> Result<Option<Rel>, PipelineError> {
        use elephant_storage::Repository;
        self.rel_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_rel_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Rel>, PipelineError> {
        self.rel_repo
            .get_with_deleted(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn update_rel(
        &self,
        space_id: &str,
        id: &RecordId,
        rel: Rel,
    ) -> Result<Rel, PipelineError> {
        use elephant_storage::Repository;
        self.rel_repo
            .update(space_id, id, rel)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn delete_rel(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        use elephant_storage::Repository;
        self.rel_repo
            .delete(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError> {
        use elephant_storage::Repository;
        self.attr_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_attribute_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError> {
        self.attr_repo
            .get_with_deleted(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn update_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
        attr: Attribute,
    ) -> Result<Attribute, PipelineError> {
        use elephant_storage::Repository;
        self.attr_repo
            .update(space_id, id, attr)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn delete_attribute(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        use elephant_storage::Repository;
        self.attr_repo
            .delete(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_event(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError> {
        use elephant_storage::Repository;
        self.event_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_event_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError> {
        self.event_repo
            .get_with_deleted(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn update_event(
        &self,
        space_id: &str,
        id: &RecordId,
        event: Event,
    ) -> Result<Event, PipelineError> {
        use elephant_storage::Repository;
        self.event_repo
            .update(space_id, id, event)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn delete_event(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        use elephant_storage::Repository;
        self.event_repo
            .delete(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn list_events(&self, space_id: &str, limit: usize) -> Result<Vec<Event>, PipelineError> {
        use elephant_storage::Repository;
        self.event_repo
            .list(space_id, limit, None)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_rels_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError> {
        self.rel_repo
            .find_for_slot(space_id, subject_id, object_id, rel_type)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_rels_for_subject_predicate(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError> {
        self.rel_repo
            .find_by_subject_predicate(space_id, subject_id, rel_type)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_entity(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Entity>, PipelineError> {
        use elephant_storage::Repository;
        self.entity_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_assertion(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Assertion>, PipelineError> {
        use elephant_storage::Repository;
        self.assertion_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_attrs_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        attr_type: &str,
    ) -> Result<Vec<Attribute>, PipelineError> {
        self.attr_repo
            .find_for_slot(space_id, subject_id, attr_type)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_stale_slots(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<TruthSlot>, PipelineError> {
        self.truth_slot_repo
            .find_stale(space_id, limit)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_predicate_registry_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Option<PredicateRegistry>, PipelineError> {
        self.predicate_registry_repo
            .find_by_predicate_id(space_id, predicate_id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_predicate_registry(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateRegistry>, PipelineError> {
        use elephant_storage::Repository;
        self.predicate_registry_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn get_predicate_alias(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateAlias>, PipelineError> {
        use elephant_storage::Repository;
        self.predicate_alias_repo
            .get(space_id, id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn list_predicate_aliases_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Vec<PredicateAlias>, PipelineError> {
        self.predicate_alias_repo
            .find_by_predicate_id(space_id, predicate_id)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn upsert_predicate_semantics(
        &self,
        space_id: &str,
        predicate_id: &str,
        semantics: PredicateSemantics,
        status: SemanticsStatus,
        provenance_note: &str,
    ) -> Result<PredicateRegistry, PipelineError> {
        self.predicate_registry_repo
            .upsert_semantics(space_id, predicate_id, semantics, status, provenance_note)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn create_audit_log(&self, log: AuditLog) -> Result<AuditLog, PipelineError> {
        use elephant_storage::Repository;
        self.audit_log_repo
            .create(log)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn emit_system_event(&self, payload: OutboxPayload) -> Result<(), PipelineError> {
        self.outbox_writer.write(payload).await?;
        Ok(())
    }

    async fn mark_slots_stale_by_predicate(
        &self,
        space_id: &str,
        predicate_id: &str,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.truth_slot_repo
            .mark_stale_by_predicate(space_id, predicate_id, reason)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn mark_slots_stale_by_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.truth_slot_repo
            .mark_stale_by_entity(space_id, entity_id, reason)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn mark_all_slots_stale(
        &self,
        space_id: &str,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.truth_slot_repo
            .mark_all_stale(space_id, reason)
            .await
            .map_err(PipelineError::StorageError)
    }

    async fn find_freshness_score(
        &self,
        space_id: &str,
        target: &str,
    ) -> Result<Option<f32>, PipelineError> {
        match self
            .freshness_repo
            .find_by_target(space_id, target)
            .await
            .map_err(PipelineError::StorageError)?
        {
            Some(record) => Ok(Some(record.score)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// Slot Key Computation
// ============================================================================

/// Computes a deterministic slot key for a truth slot.
///
/// Per spec §12.3: slot_key uniquely identifies a truth slot.
/// - For attributes: `(subject_entity, predicate_id, context_bucket?, security_hash)`
/// - For rels: `(subject_entity, object_entity, predicate_id, security_hash)`
pub fn compute_slot_key(
    subject_entity: &str,
    object_entity: Option<&str>,
    predicate_id: &str,
    context: Option<&str>,
    security_hash: &str,
) -> String {
    let mut parts: Vec<&str> = Vec::new();

    if let Some(obj) = object_entity {
        // Rels: (subject, object, predicate, security_hash)
        parts.push(subject_entity);
        parts.push(obj);
        parts.push(predicate_id);
    } else {
        // Attributes: (subject, predicate, context?, security_hash)
        parts.push(subject_entity);
        parts.push(predicate_id);
        if let Some(ctx) = context {
            parts.push(ctx);
        }
    }

    parts.push(security_hash);

    // Join with delimiter that won't appear in IDs
    parts.join("::")
}

/// Determines the slot type based on whether there's an object entity.
pub fn determine_slot_type(object_entity: Option<&str>) -> &'static str {
    if object_entity.is_some() {
        "rel"
    } else {
        "attribute"
    }
}

/// Sentinel segment marking a cross-object rel-set slot key.
///
/// It occupies the object position of a rel key, where a record id (which
/// always contains a table prefix) or a predicate string would otherwise sit,
/// so it cannot collide with rel or attribute keys.
const REL_SET_SENTINEL: &str = "__rel_set__";

/// Computes the slot key for a cross-object rel-set slot.
///
/// Groups every rel claim of `(subject, predicate)` within one security
/// envelope, regardless of object, for cardinality conflict detection.
#[must_use]
pub fn compute_rel_set_slot_key(
    subject_entity: &str,
    predicate_id: &str,
    security_hash: &str,
) -> String {
    format!("{subject_entity}::{REL_SET_SENTINEL}::{predicate_id}::{security_hash}")
}

/// Computes a deterministic hash over a claim-id set.
///
/// Used to memoize adjudication verdicts and to guard commits against stale
/// resolutions: sha256 hex over the sorted ids joined with newlines.
#[must_use]
pub fn compute_claim_set_hash(claim_ids: &[String]) -> String {
    let mut sorted: Vec<&str> = claim_ids.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let mut hasher = Sha256::new();
    for (index, id) in sorted.iter().enumerate() {
        if index > 0 {
            hasher.update(b"\n");
        }
        hasher.update(id.as_bytes());
    }
    let digest = hasher.finalize();
    format!("{digest:x}")
}

#[derive(Debug, Clone)]
struct SlotKeyParts {
    subject_id: RecordId,
    object_id: Option<RecordId>,
    predicate_id: String,
    security_hash: String,
}

fn parse_slot_key(slot_key: &str, slot_type: &str) -> Result<SlotKeyParts, PipelineError> {
    let parts: Vec<&str> = slot_key.split("::").collect();
    let security_hash = parts
        .last()
        .ok_or_else(|| PipelineError::InvalidInput(format!("Invalid slot_key {slot_key}")))?;
    match slot_type {
        "rel" => {
            if parts.len() != 4 {
                return Err(PipelineError::InvalidInput(format!(
                    "Invalid rel slot_key {slot_key}"
                )));
            }
            Ok(SlotKeyParts {
                subject_id: RecordId::from_str(parts[0]).map_err(|e| {
                    PipelineError::InvalidInput(format!("Invalid subject id {slot_key}: {e}"))
                })?,
                object_id: Some(RecordId::from_str(parts[1]).map_err(|e| {
                    PipelineError::InvalidInput(format!("Invalid object id {slot_key}: {e}"))
                })?),
                predicate_id: parts[2].to_string(),
                security_hash: security_hash.to_string(),
            })
        }
        "attribute" => {
            if parts.len() != 3 && parts.len() != 4 {
                return Err(PipelineError::InvalidInput(format!(
                    "Invalid attribute slot_key {slot_key}"
                )));
            }
            let predicate_id = parts[1].to_string();
            Ok(SlotKeyParts {
                subject_id: RecordId::from_str(parts[0]).map_err(|e| {
                    PipelineError::InvalidInput(format!("Invalid subject id {slot_key}: {e}"))
                })?,
                object_id: None,
                predicate_id,
                security_hash: security_hash.to_string(),
            })
        }
        "rel_set" => {
            if parts.len() != 4 || parts[1] != REL_SET_SENTINEL {
                return Err(PipelineError::InvalidInput(format!(
                    "Invalid rel_set slot_key {slot_key}"
                )));
            }
            Ok(SlotKeyParts {
                subject_id: RecordId::from_str(parts[0]).map_err(|e| {
                    PipelineError::InvalidInput(format!("Invalid subject id {slot_key}: {e}"))
                })?,
                object_id: None,
                predicate_id: parts[2].to_string(),
                security_hash: security_hash.to_string(),
            })
        }
        other => Err(PipelineError::InvalidInput(format!(
            "Unknown slot type {other}"
        ))),
    }
}

fn time_basis_from_recency(recency: RecencyBasis) -> TimeBasis {
    match recency {
        RecencyBasis::SourceEventTime => TimeBasis::SourceEventTime,
        RecencyBasis::ObservedAt => TimeBasis::ObservedAt,
        RecencyBasis::AssertedAt => TimeBasis::AssertedAt,
    }
}

/// Computes a deterministic security hash from a security envelope.
fn compute_security_hash(security: &Security) -> String {
    let mut hasher = Sha256::new();
    hasher.update(security.policy_id.as_bytes());
    hasher.update(security.level.to_be_bytes());

    let mut labels = security.labels.clone();
    labels.sort();
    for label in labels {
        hasher.update(label.as_bytes());
    }

    let mut handling = security.handling.clone();
    handling.sort();
    for handle in handling {
        hasher.update(handle.as_bytes());
    }

    let digest = hasher.finalize();
    format!("{digest:x}")
}

// ============================================================================
// Conflict Detection
// ============================================================================

/// Work kind identifier for tm_detect_conflicts.
pub const TM_DETECT_CONFLICTS_KIND: &str = "tm_detect_conflicts";

/// Event types that can trigger conflict detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    /// New assertion created.
    Assertion(RecordId),
    /// New relationship created.
    Rel(RecordId),
    /// New attribute created.
    Attribute(RecordId),
    /// New event created/updated.
    Event(RecordId),
    /// Entity changed.
    Entity(RecordId),
    /// Predicate definition changed.
    Predicate(RecordId),
    /// Document revision changed.
    Revision(RecordId),
}

#[derive(Debug, Clone)]
enum StaleTrigger {
    All {
        reason: String,
    },
    Entity {
        entity_id: RecordId,
        reason: String,
    },
    Predicate {
        predicate_id: RecordId,
        reason: String,
    },
}

/// Result of conflict detection.
#[derive(Debug, Clone)]
pub struct DirectCommit {
    /// Resolution result to apply.
    pub resolution: ResolutionResult,
    /// Commit input for tm_commit.
    pub input: CommitInput,
}

/// Result of conflict detection.
#[derive(Debug, Clone)]
pub struct DetectConflictsResult {
    /// Number of events processed.
    pub events_processed: usize,
    /// Number of new conflicts detected.
    pub conflicts_detected: usize,
    /// Number of existing conflict groups updated.
    pub conflicts_updated: usize,
    /// IDs of conflict groups created or updated.
    pub conflict_group_ids: Vec<RecordId>,
    /// Direct commit payloads for non-conflicting slots.
    pub direct_commits: Vec<DirectCommit>,
}

/// Executes the tm_detect_conflicts work kind.
///
/// Per spec §11: detects conflicts when new claims arrive for existing slots.
///
/// # Errors
///
/// Returns an error if storage operations fail.
pub async fn execute_tm_detect_conflicts<S: TruthMaintenanceStorage>(
    events: &[TriggerEvent],
    space_id: &str,
    storage: &S,
) -> Result<DetectConflictsResult, PipelineError> {
    let mut rel_events = Vec::new();
    let mut attr_events = Vec::new();
    let mut event_events = Vec::new();
    let mut stale_triggers = Vec::new();

    for event in events {
        match event {
            TriggerEvent::Rel(rel_id) => {
                let rel = match storage.get_rel(space_id, rel_id).await? {
                    Some(rel) => Some(rel),
                    None => storage.get_rel_with_deleted(space_id, rel_id).await?,
                };
                if let Some(rel) = rel {
                    rel_events.push(RelEventInfo {
                        rel_id: rel.base.id.clone(),
                        subject_id: rel.subject.id().clone(),
                        object_id: rel.object.id().clone(),
                        rel_type: rel.rel_type.clone(),
                        security_hash: compute_security_hash(&rel.base.security),
                        deleted: rel.base.deleted_at.is_some(),
                    });
                }
            }
            TriggerEvent::Attribute(attr_id) => {
                let attr = match storage.get_attribute(space_id, attr_id).await? {
                    Some(attr) => Some(attr),
                    None => {
                        storage
                            .get_attribute_with_deleted(space_id, attr_id)
                            .await?
                    }
                };
                if let Some(attr) = attr {
                    attr_events.push(AttributeEventInfo {
                        attr_id: attr.base.id.clone(),
                        subject_id: attr.subject.id().clone(),
                        attr_type: attr.attr_type.clone(),
                        security_hash: compute_security_hash(&attr.base.security),
                        deleted: attr.base.deleted_at.is_some(),
                    });
                }
            }
            TriggerEvent::Event(event_id) => {
                let event = match storage.get_event(space_id, event_id).await? {
                    Some(event) => Some(event),
                    None => storage.get_event_with_deleted(space_id, event_id).await?,
                };
                if let Some(event) = event {
                    event_events.push(EventEventInfo {
                        event_id: event.base.id.clone(),
                        slot_key: compute_event_slot_key(&event),
                        deleted: event.base.deleted_at.is_some(),
                    });
                }
            }
            TriggerEvent::Revision(_) => {
                stale_triggers.push(StaleTrigger::All {
                    reason: "revision_trigger".to_string(),
                });
            }
            TriggerEvent::Entity(entity_id) => {
                if entity_id.table() == TableName::Entity {
                    stale_triggers.push(StaleTrigger::Entity {
                        entity_id: entity_id.clone(),
                        reason: "entity_updated".to_string(),
                    });
                } else {
                    stale_triggers.push(StaleTrigger::All {
                        reason: "entity_trigger".to_string(),
                    });
                }
            }
            TriggerEvent::Predicate(predicate_id) => {
                stale_triggers.push(StaleTrigger::Predicate {
                    predicate_id: predicate_id.clone(),
                    reason: "predicate_updated".to_string(),
                });
            }
            TriggerEvent::Assertion(_) => {
                stale_triggers.push(StaleTrigger::All {
                    reason: "assertion_trigger".to_string(),
                });
            }
        }
    }

    let mut result =
        execute_tm_detect_conflicts_with_storage(&rel_events, &attr_events, space_id, storage)
            .await?;
    if !event_events.is_empty() {
        let event_result =
            execute_tm_detect_event_conflicts_with_storage(&event_events, space_id, storage)
                .await?;
        result.conflicts_detected += event_result.conflicts_detected;
        result.conflicts_updated += event_result.conflicts_updated;
        result
            .conflict_group_ids
            .extend(event_result.conflict_group_ids);
        result.direct_commits.extend(event_result.direct_commits);
    }
    result.events_processed = events.len();
    result
        .direct_commits
        .sort_by(|a, b| a.input.slot_key.cmp(&b.input.slot_key));

    if !stale_triggers.is_empty() {
        apply_stale_triggers(space_id, storage, &stale_triggers).await?;
    }
    Ok(result)
}

async fn apply_stale_triggers<S: TruthMaintenanceStorage>(
    space_id: &str,
    storage: &S,
    triggers: &[StaleTrigger],
) -> Result<(), PipelineError> {
    if let Some(StaleTrigger::All { reason }) = triggers
        .iter()
        .find(|trigger| matches!(trigger, StaleTrigger::All { .. }))
    {
        storage.mark_all_slots_stale(space_id, reason).await?;
        return Ok(());
    }

    for trigger in triggers {
        match trigger {
            StaleTrigger::Entity { entity_id, reason } => {
                storage
                    .mark_slots_stale_by_entity(space_id, entity_id, reason)
                    .await?;
            }
            StaleTrigger::Predicate {
                predicate_id,
                reason,
            } => match predicate_id.table() {
                TableName::PredicateRegistry => {
                    if let Some(predicate) = storage
                        .get_predicate_registry(space_id, predicate_id)
                        .await?
                    {
                        storage
                            .mark_slots_stale_by_predicate(
                                space_id,
                                &predicate.predicate_id,
                                reason,
                            )
                            .await?;
                    } else {
                        storage.mark_all_slots_stale(space_id, reason).await?;
                    }
                }
                TableName::PredicateAlias => {
                    if let Some(alias) = storage.get_predicate_alias(space_id, predicate_id).await?
                    {
                        storage
                            .mark_slots_stale_by_predicate(space_id, &alias.predicate_id, reason)
                            .await?;
                    } else {
                        storage.mark_all_slots_stale(space_id, reason).await?;
                    }
                }
                _ => {
                    storage.mark_all_slots_stale(space_id, reason).await?;
                }
            },
            StaleTrigger::All { reason } => {
                storage.mark_all_slots_stale(space_id, reason).await?;
            }
        }
    }

    Ok(())
}

/// Rel event info for conflict detection.
#[derive(Debug, Clone)]
pub struct RelEventInfo {
    /// The rel ID that triggered the event.
    pub rel_id: RecordId,
    /// Subject entity ID.
    pub subject_id: RecordId,
    /// Object entity ID.
    pub object_id: RecordId,
    /// Relationship type (predicate).
    pub rel_type: String,
    /// Security hash for the slot.
    pub security_hash: String,
    /// Whether the rel was deleted.
    pub deleted: bool,
}

/// Attribute event info for conflict detection.
#[derive(Debug, Clone)]
pub struct AttributeEventInfo {
    /// The attribute ID that triggered the event.
    pub attr_id: RecordId,
    /// Subject entity ID.
    pub subject_id: RecordId,
    /// Attribute type (predicate).
    pub attr_type: String,
    /// Security hash for the slot.
    pub security_hash: String,
    /// Whether the attribute was deleted.
    pub deleted: bool,
}

/// Event info for deterministic event conflict detection.
#[derive(Debug, Clone)]
pub struct EventEventInfo {
    /// The event ID that triggered the event.
    pub event_id: RecordId,
    /// Deterministic grouping key for conflict handling.
    pub slot_key: String,
    /// Whether the event was deleted.
    pub deleted: bool,
}

#[derive(Debug, Clone)]
struct EventSlotKeyParts {
    participants_key: String,
    title_key: String,
}

const EVENT_SCAN_LIMIT: usize = 10_000;
const EVENT_TITLE_TOKEN_LIMIT: usize = 6;
const EVENT_REJECT_FLOOR_DEFAULT: f32 = 0.2;
const EVENT_TITLE_STOPWORDS: &[&str] = &[
    "a", "an", "the", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from", "is",
    "are",
];

fn event_title_key(title: &str) -> String {
    let mut tokens: Vec<String> = title
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .filter(|token| !EVENT_TITLE_STOPWORDS.contains(token))
        .map(ToString::to_string)
        .collect();
    tokens.sort();
    if tokens.is_empty() {
        "unknown".to_string()
    } else {
        tokens
            .into_iter()
            .take(EVENT_TITLE_TOKEN_LIMIT)
            .collect::<Vec<_>>()
            .join("|")
    }
}

fn event_participants_key(event: &Event) -> String {
    let mut participants: Vec<String> = event
        .base
        .subjects
        .iter()
        .map(|subject| subject.id().to_string())
        .collect();
    participants.sort();
    participants.dedup();
    if !participants.is_empty() {
        participants.join("|")
    } else {
        // Fallback for participant-less events (e.g. manually created timeline records):
        // derive a deterministic key from evidence links to avoid collapsing all such
        // events into a single conflict group.
        let mut evidence_refs: Vec<String> = event
            .assertion_refs
            .iter()
            .map(|assertion| format!("a:{}", assertion.id()))
            .collect();
        evidence_refs.extend(
            event
                .source_refs
                .iter()
                .map(|source| format!("s:{}", source.id())),
        );
        evidence_refs.sort();
        evidence_refs.dedup();

        if evidence_refs.is_empty() {
            "unknown".to_string()
        } else {
            let mut hasher = Sha256::new();
            for reference in evidence_refs {
                hasher.update(reference.as_bytes());
                hasher.update(b"|");
            }
            let digest = hex::encode(hasher.finalize());
            format!("evidence:{}", &digest[..16])
        }
    }
}

fn event_day_bucket(time_start: Option<DateTime<Utc>>) -> String {
    time_start
        .map(|value| value.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn build_event_slot_parts(event: &Event) -> EventSlotKeyParts {
    EventSlotKeyParts {
        participants_key: event_participants_key(event),
        title_key: event_title_key(&event.title),
    }
}

fn compute_event_slot_key(event: &Event) -> String {
    let parts = build_event_slot_parts(event);
    let day_bucket = event_day_bucket(event.time_start);
    let mut hasher = Sha256::new();
    hasher.update(parts.participants_key.as_bytes());
    hasher.update(b"|");
    hasher.update(day_bucket.as_bytes());
    hasher.update(b"|");
    hasher.update(parts.title_key.as_bytes());
    hex::encode(hasher.finalize())
}

fn event_subject_from_participants_key(participants_key: &str) -> String {
    if participants_key == "unknown" || participants_key.starts_with("evidence:") {
        return "event_group".to_string();
    }

    participants_key
        .split('|')
        .find(|value| !value.is_empty() && *value != "unknown")
        .map_or_else(|| "event_group".to_string(), ToString::to_string)
}

fn build_event_claim_metadata(event: &Event) -> ClaimWithMetadata {
    let mut participants: Vec<String> = event
        .base
        .subjects
        .iter()
        .map(|subject| subject.id().to_string())
        .collect();
    participants.sort();
    participants.dedup();

    ClaimWithMetadata {
        claim_id: event.base.id.to_string(),
        value: ClaimValue::EventClaim {
            title: event.title.clone(),
            time_start: event.time_start,
            participants,
            status: event.status.to_string(),
        },
        confidence: event.confidence,
        timestamp: event.time_start.unwrap_or(event.base.created_at),
        time_basis: if event.time_start.is_some() {
            "event_start"
        } else {
            "record_created_at"
        },
        context: Some(event_day_bucket(event.time_start)),
        evidence_count: event.assertion_refs.len() + event.source_refs.len(),
        evidence_sources: event
            .assertion_refs
            .iter()
            .map(|assertion| assertion.id().to_string())
            .collect(),
        status_weight: match event.status {
            EventStatus::Approved => 3,
            EventStatus::Candidate => 2,
            EventStatus::Superseded => 1,
            EventStatus::Rejected => 0,
        },
        updated_at: event.base.updated_at,
    }
}

fn resolve_event_group(claims: &[ClaimWithMetadata]) -> ResolutionResult {
    if claims.is_empty() {
        return ResolutionResult::NeedsReview {
            reason: "No event claims to resolve".to_string(),
        };
    }

    let mut ranked: Vec<&ClaimWithMetadata> = claims.iter().collect();
    ranked.sort_by(|a, b| {
        b.status_weight
            .cmp(&a.status_weight)
            .then_with(|| {
                b.confidence
                    .partial_cmp(&a.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| b.evidence_count.cmp(&a.evidence_count))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.claim_id.cmp(&b.claim_id))
    });

    let winner = ranked[0];
    ResolutionResult::SingleWinner {
        winner_id: winner.claim_id.clone(),
        confidence: winner.confidence,
        rationale:
            "event_conflict_v1: status weight, confidence, evidence count, updated_at, claim id"
                .to_string(),
    }
}

async fn build_direct_commit_for_events(
    slot_key: &str,
    events: &[Event],
    _space_id: &str,
) -> Result<DirectCommit, PipelineError> {
    if events.is_empty() {
        return Err(PipelineError::InvalidInput(
            "event direct commit requires at least one event".to_string(),
        ));
    }

    let mut claims: Vec<ClaimWithMetadata> =
        events.iter().map(build_event_claim_metadata).collect();
    claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
    let mut claim_ids: Vec<String> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    claim_ids.sort();
    claim_ids.dedup();

    if events
        .iter()
        .any(|event| compute_event_slot_key(event) != slot_key)
    {
        return Err(PipelineError::InvalidInput(format!(
            "Invalid event slot_key {slot_key}"
        )));
    }

    let resolution = resolve_event_group(&claims);
    let canonical_event = events
        .iter()
        .min_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()))
        .ok_or_else(|| {
            PipelineError::InvalidInput("event direct commit requires an event".to_string())
        })?;
    let parts = build_event_slot_parts(canonical_event);

    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    let input = CommitInput {
        slot_key: slot_key.to_string(),
        slot_type: "event".to_string(),
        subject_entity: event_subject_from_participants_key(&parts.participants_key),
        predicate_id: parts.title_key,
        security_hash: compute_security_hash(&canonical_event.base.security),
        claim_ids,
        claim_set_hash: Some(claim_set_hash),
        ..CommitInput::default()
    };

    Ok(DirectCommit { resolution, input })
}

async fn execute_tm_detect_event_conflicts_with_storage<S: TruthMaintenanceStorage>(
    event_events: &[EventEventInfo],
    space_id: &str,
    storage: &S,
) -> Result<DetectConflictsResult, PipelineError> {
    let all_events = storage.list_events(space_id, EVENT_SCAN_LIMIT).await?;
    let mut events_by_slot: HashMap<String, Vec<Event>> = HashMap::new();
    for event in all_events {
        if !matches!(event.status, EventStatus::Approved | EventStatus::Candidate) {
            continue;
        }
        let slot_key = compute_event_slot_key(&event);
        events_by_slot.entry(slot_key).or_default().push(event);
    }

    let mut conflicts_detected = 0;
    let conflicts_updated = 0;
    let conflict_group_ids = Vec::new();
    let mut direct_commits: HashMap<String, DirectCommit> = HashMap::new();
    let mut seen_slots: HashSet<String> = HashSet::new();

    for event in event_events {
        if !seen_slots.insert(event.slot_key.clone()) {
            continue;
        }

        let claims = events_by_slot
            .get(&event.slot_key)
            .cloned()
            .unwrap_or_default();
        let claim_count = claims.len();

        if claim_count == 0 && event.deleted {
            continue;
        }

        if claim_count > 1 {
            conflicts_detected += 1;
        }

        if claim_count >= 1 && !direct_commits.contains_key(&event.slot_key) {
            let commit = build_direct_commit_for_events(&event.slot_key, &claims, space_id).await?;
            direct_commits.insert(event.slot_key.clone(), commit);
        }
    }

    let mut direct_commit_list: Vec<DirectCommit> = direct_commits.into_values().collect();
    direct_commit_list.sort_by(|a, b| a.input.slot_key.cmp(&b.input.slot_key));

    Ok(DetectConflictsResult {
        events_processed: event_events.len(),
        conflicts_detected,
        conflicts_updated,
        conflict_group_ids,
        direct_commits: direct_commit_list,
    })
}

/// Outcome of value-aware slot detection.
enum SlotDetection {
    /// All claims share one value: direct corroboration commit.
    Corroborated(Box<DirectCommit>),
    /// A resolved group already adjudicated this exact claim set; skip.
    Memoized,
    /// Real value conflict: group created or updated.
    Conflict {
        /// Conflict group id.
        id: RecordId,
        /// Whether an unresolved group already existed for the slot.
        was_existing: bool,
    },
}

/// Upserts an unresolved conflict group unless a resolved group already
/// adjudicated this exact claim set (memoization skip: no reopen, no counters).
async fn upsert_conflict_group_memoized<S: TruthMaintenanceStorage>(
    space_id: &str,
    slot_key: &str,
    slot_type: &str,
    claim_ids: Vec<String>,
    storage: &S,
) -> Result<SlotDetection, PipelineError> {
    let deterministic_id = conflict_group_unresolved_id(space_id, slot_type, slot_key);
    let existing = storage
        .get_conflict_group(space_id, &deterministic_id)
        .await?;
    if let Some(existing) = &existing {
        // Resolved OR unresolved (needs_review cooldown): an unchanged claim
        // set that was already adjudicated must not reopen or re-enqueue.
        if existing.adjudicated_claim_set_hash.as_deref()
            == Some(compute_claim_set_hash(&claim_ids).as_str())
        {
            return Ok(SlotDetection::Memoized);
        }
    }

    let was_existing = storage
        .find_unresolved_conflict_by_type(space_id, slot_key, slot_type)
        .await?
        .is_some();
    let mut group = build_unresolved_conflict_group(slot_key, slot_type, claim_ids, space_id);
    // Carry the memo forward so re-upserts never wipe a persisted verdict hash.
    if let Some(existing) = existing {
        group.adjudicated_claim_set_hash = existing.adjudicated_claim_set_hash;
        group.adjudicated_at = existing.adjudicated_at;
    }
    let upserted = storage.upsert_unresolved_conflict_group(group).await?;
    Ok(SlotDetection::Conflict {
        id: upserted.base.id,
        was_existing,
    })
}

/// Value-aware detection for a multi-claim slot: one value group corroborates,
/// several value groups open (or refresh) a conflict group.
#[allow(clippy::too_many_arguments)]
async fn detect_slot_outcome<S: TruthMaintenanceStorage>(
    space_id: &str,
    slot_key: &str,
    slot_type: &str,
    subject_id: &RecordId,
    predicate_id: &str,
    security_hash: &str,
    claim_records: &[ClaimRecord],
    claim_ids: Vec<String>,
    storage: &S,
) -> Result<SlotDetection, PipelineError> {
    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, predicate_id)
        .await?;
    let semantics = predicate.as_ref().and_then(|p| p.semantics.as_ref());
    // Displays are only needed for adjudication prompts; detection compares by id.
    let entity_displays = HashMap::new();
    let claims: Vec<ClaimWithMetadata> = claim_records
        .iter()
        .map(|record| build_claim_metadata(record, semantics, &entity_displays))
        .collect();
    let values: Vec<ClaimValue> = claims.iter().map(|claim| claim.value.clone()).collect();

    if partition_value_groups(&values).len() == 1 {
        return Ok(SlotDetection::Corroborated(Box::new(
            build_corroboration_commit(
                slot_key,
                slot_type,
                subject_id,
                predicate_id,
                security_hash,
                &claims,
            ),
        )));
    }

    upsert_conflict_group_memoized(space_id, slot_key, slot_type, claim_ids, storage).await
}

/// Detects cross-object cardinality conflicts for a subject + predicate.
///
/// Returns `None` when there is no conflict (fewer than two distinct objects,
/// or the predicate is configured multi-valued).
async fn detect_rel_set_conflict<S: TruthMaintenanceStorage>(
    space_id: &str,
    rel_set_key: &str,
    subject_id: &RecordId,
    predicate_id: &str,
    security_hash: &str,
    storage: &S,
) -> Result<Option<SlotDetection>, PipelineError> {
    let rels: Vec<Rel> = storage
        .find_rels_for_subject_predicate(space_id, subject_id, predicate_id)
        .await?
        .into_iter()
        .filter(|rel| compute_security_hash(&rel.base.security) == security_hash)
        .collect();

    let distinct_objects: HashSet<String> =
        rels.iter().map(|rel| rel.object.id().to_string()).collect();
    if distinct_objects.len() < 2 {
        return Ok(None);
    }

    let cardinality = storage
        .find_predicate_registry_by_predicate_id(space_id, predicate_id)
        .await?
        .and_then(|p| p.semantics.map(|s| s.cardinality));
    if cardinality == Some(Cardinality::Many) {
        return Ok(None);
    }

    let mut claim_ids: Vec<String> = rels.iter().map(|rel| rel.base.id.to_string()).collect();
    claim_ids.sort();
    claim_ids.dedup();

    upsert_conflict_group_memoized(space_id, rel_set_key, "rel_set", claim_ids, storage)
        .await
        .map(Some)
}

/// Executes tm_detect_conflicts with real storage queries.
///
/// Per spec §12.6: Detects conflicts when new claims arrive for existing slots.
///
/// # Errors
///
/// Returns an error if storage operations fail.
pub async fn execute_tm_detect_conflicts_with_storage<S: TruthMaintenanceStorage>(
    rel_events: &[RelEventInfo],
    attr_events: &[AttributeEventInfo],
    space_id: &str,
    storage: &S,
) -> Result<DetectConflictsResult, PipelineError> {
    let mut conflicts_detected = 0;
    let mut conflicts_updated = 0;
    let mut conflict_group_ids = Vec::new();
    // One resolve per group per batch: several events can hit the same
    // deterministic group id, and double-enqueued resolves race each other.
    let mut seen_conflict_group_ids: HashSet<RecordId> = HashSet::new();
    let mut direct_commits: HashMap<String, DirectCommit> = HashMap::new();
    let mut seen_rel_set_keys: HashSet<String> = HashSet::new();
    let events_processed = rel_events.len() + attr_events.len();

    // Process rel events
    for event in rel_events {
        let is_event_like = predicate_is_event_like(storage, space_id, &event.rel_type).await?;
        let slot_key = compute_slot_key(
            &event.subject_id.to_string(),
            Some(&event.object_id.to_string()),
            &event.rel_type,
            None,
            &event.security_hash,
        );

        // Find all rels for this slot
        let claims = storage
            .find_rels_for_slot(
                space_id,
                &event.subject_id,
                &event.object_id,
                &event.rel_type,
            )
            .await?
            .into_iter()
            .filter(|rel| compute_security_hash(&rel.base.security) == event.security_hash)
            .collect::<Vec<_>>();

        let claim_records: Vec<ClaimRecord> = claims.into_iter().map(ClaimRecord::Rel).collect();
        let claim_count = claim_records.len();
        let claim_ids: Vec<String> = claim_records
            .iter()
            .map(|record| record.id().to_string())
            .collect();

        if claim_count == 0 && event.deleted {
            handle_deleted_slot(
                space_id,
                &slot_key,
                "rel",
                storage,
                &mut conflict_group_ids,
                &mut conflicts_updated,
            )
            .await?;
            continue;
        }

        if is_event_like {
            if !direct_commits.contains_key(&slot_key) && !claim_records.is_empty() {
                let commit = build_direct_commit_for_records(
                    &slot_key,
                    "rel",
                    &event.subject_id,
                    &event.rel_type,
                    &event.security_hash,
                    &claim_records,
                    space_id,
                    storage,
                )
                .await?;
                direct_commits.insert(slot_key.clone(), commit);
            }
            continue;
        }

        // Multiple claims: identical values corroborate, divergent values conflict.
        if claim_count > 1 {
            match detect_slot_outcome(
                space_id,
                &slot_key,
                "rel",
                &event.subject_id,
                &event.rel_type,
                &event.security_hash,
                &claim_records,
                claim_ids.clone(),
                storage,
            )
            .await?
            {
                SlotDetection::Corroborated(commit) => {
                    direct_commits.entry(slot_key.clone()).or_insert(*commit);
                }
                SlotDetection::Memoized => {}
                SlotDetection::Conflict { id, was_existing } => {
                    if seen_conflict_group_ids.insert(id.clone()) {
                        conflict_group_ids.push(id);
                        if was_existing {
                            conflicts_updated += 1;
                        } else {
                            conflicts_detected += 1;
                        }
                    }
                }
            }
        } else if claim_count == 1 {
            if let Some(existing) = storage
                .find_unresolved_conflict(space_id, &slot_key)
                .await?
            {
                let existing_id = existing.base.id.clone();
                let updated = storage
                    .merge_unresolved_conflict_claims(space_id, &existing_id, claim_ids.clone())
                    .await?;
                if let Some(updated) = updated {
                    conflict_group_ids.push(updated.base.id);
                    conflicts_updated += 1;
                } else if !direct_commits.contains_key(&slot_key) {
                    let commit = build_direct_commit_for_records(
                        &slot_key,
                        "rel",
                        &event.subject_id,
                        &event.rel_type,
                        &event.security_hash,
                        &claim_records,
                        space_id,
                        storage,
                    )
                    .await?;
                    direct_commits.insert(slot_key.clone(), commit);
                }
            } else if !direct_commits.contains_key(&slot_key) {
                let commit = build_direct_commit_for_records(
                    &slot_key,
                    "rel",
                    &event.subject_id,
                    &event.rel_type,
                    &event.security_hash,
                    &claim_records,
                    space_id,
                    storage,
                )
                .await?;
                direct_commits.insert(slot_key.clone(), commit);
            }
        }

        // Cross-object cardinality detection for this subject + predicate.
        let rel_set_key = compute_rel_set_slot_key(
            &event.subject_id.to_string(),
            &event.rel_type,
            &event.security_hash,
        );
        if seen_rel_set_keys.insert(rel_set_key.clone()) {
            match detect_rel_set_conflict(
                space_id,
                &rel_set_key,
                &event.subject_id,
                &event.rel_type,
                &event.security_hash,
                storage,
            )
            .await?
            {
                None | Some(SlotDetection::Memoized) => {}
                Some(SlotDetection::Conflict { id, was_existing }) => {
                    if seen_conflict_group_ids.insert(id.clone()) {
                        conflict_group_ids.push(id);
                        if was_existing {
                            conflicts_updated += 1;
                        } else {
                            conflicts_detected += 1;
                        }
                    }
                }
                Some(SlotDetection::Corroborated(_)) => {
                    // rel_set detection never corroborates; distinct objects
                    // are a precondition for the group.
                }
            }
        }
    }

    // Process attribute events
    for event in attr_events {
        let is_event_like = predicate_is_event_like(storage, space_id, &event.attr_type).await?;
        let slot_key = compute_slot_key(
            &event.subject_id.to_string(),
            None,
            &event.attr_type,
            None,
            &event.security_hash,
        );

        // Find all attributes for this slot
        let claims = storage
            .find_attrs_for_slot(space_id, &event.subject_id, &event.attr_type)
            .await?
            .into_iter()
            .filter(|attr| compute_security_hash(&attr.base.security) == event.security_hash)
            .collect::<Vec<_>>();

        let claim_records: Vec<ClaimRecord> =
            claims.into_iter().map(ClaimRecord::Attribute).collect();
        let claim_count = claim_records.len();
        let claim_ids: Vec<String> = claim_records
            .iter()
            .map(|record| record.id().to_string())
            .collect();

        if claim_count == 0 && event.deleted {
            handle_deleted_slot(
                space_id,
                &slot_key,
                "attribute",
                storage,
                &mut conflict_group_ids,
                &mut conflicts_updated,
            )
            .await?;
            continue;
        }

        if is_event_like {
            if !direct_commits.contains_key(&slot_key) && !claim_records.is_empty() {
                let commit = build_direct_commit_for_records(
                    &slot_key,
                    "attribute",
                    &event.subject_id,
                    &event.attr_type,
                    &event.security_hash,
                    &claim_records,
                    space_id,
                    storage,
                )
                .await?;
                direct_commits.insert(slot_key.clone(), commit);
            }
            continue;
        }

        // Multiple claims: identical values corroborate, divergent values conflict.
        if claim_count > 1 {
            match detect_slot_outcome(
                space_id,
                &slot_key,
                "attribute",
                &event.subject_id,
                &event.attr_type,
                &event.security_hash,
                &claim_records,
                claim_ids.clone(),
                storage,
            )
            .await?
            {
                SlotDetection::Corroborated(commit) => {
                    direct_commits.entry(slot_key.clone()).or_insert(*commit);
                }
                SlotDetection::Memoized => {}
                SlotDetection::Conflict { id, was_existing } => {
                    if seen_conflict_group_ids.insert(id.clone()) {
                        conflict_group_ids.push(id);
                        if was_existing {
                            conflicts_updated += 1;
                        } else {
                            conflicts_detected += 1;
                        }
                    }
                }
            }
        } else if claim_count == 1 {
            if let Some(existing) = storage
                .find_unresolved_conflict(space_id, &slot_key)
                .await?
            {
                let existing_id = existing.base.id.clone();
                let updated = storage
                    .merge_unresolved_conflict_claims(space_id, &existing_id, claim_ids.clone())
                    .await?;
                if let Some(updated) = updated {
                    conflict_group_ids.push(updated.base.id);
                    conflicts_updated += 1;
                } else if !direct_commits.contains_key(&slot_key) {
                    let commit = build_direct_commit_for_records(
                        &slot_key,
                        "attribute",
                        &event.subject_id,
                        &event.attr_type,
                        &event.security_hash,
                        &claim_records,
                        space_id,
                        storage,
                    )
                    .await?;
                    direct_commits.insert(slot_key.clone(), commit);
                }
            } else if !direct_commits.contains_key(&slot_key) {
                let commit = build_direct_commit_for_records(
                    &slot_key,
                    "attribute",
                    &event.subject_id,
                    &event.attr_type,
                    &event.security_hash,
                    &claim_records,
                    space_id,
                    storage,
                )
                .await?;
                direct_commits.insert(slot_key.clone(), commit);
            }
        }
    }

    let mut direct_commit_list: Vec<DirectCommit> = direct_commits.into_values().collect();
    direct_commit_list.sort_by(|a, b| a.input.slot_key.cmp(&b.input.slot_key));

    Ok(DetectConflictsResult {
        events_processed,
        conflicts_detected,
        conflicts_updated,
        conflict_group_ids,
        direct_commits: direct_commit_list,
    })
}

async fn handle_deleted_slot<S: TruthMaintenanceStorage>(
    space_id: &str,
    slot_key: &str,
    slot_type: &str,
    storage: &S,
    conflict_group_ids: &mut Vec<RecordId>,
    conflicts_updated: &mut usize,
) -> Result<(), PipelineError> {
    if let Some(mut existing) = storage.find_unresolved_conflict(space_id, slot_key).await? {
        let existing_id = existing.base.id.clone();
        existing.claim_ids = Vec::new();
        existing.resolved = true;
        existing.resolution_rationale = Some("All claims deleted".to_string());
        existing.resolved_at = Some(Utc::now());
        let updated = storage
            .update_conflict_group(space_id, &existing_id, existing)
            .await?;
        conflict_group_ids.push(updated.base.id);
        *conflicts_updated += 1;
    }

    if let Some(mut slot) = storage.find_slot_by_key(space_id, slot_key).await? {
        mark_slot_stale(&mut slot, "claims_deleted");
        slot.current_values.clear();
        slot.confidence = 0.0;
        slot.rationale = Some(format!(
            "No active {slot_type} claims remain after deletion"
        ));
        slot.conflict_group = None;
        let slot_id = slot.base.id.clone();
        let _ = storage.update_slot(space_id, &slot_id, slot).await?;
    }

    Ok(())
}

async fn predicate_is_event_like<S: TruthMaintenanceStorage>(
    storage: &S,
    space_id: &str,
    predicate_id: &str,
) -> Result<bool, PipelineError> {
    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, predicate_id)
        .await?;
    Ok(predicate
        .as_ref()
        .and_then(|p| p.semantics.as_ref())
        .map(|s| s.temporal_model == TemporalPersonality::EventLike)
        .unwrap_or(false))
}

#[allow(clippy::too_many_arguments)]
async fn build_direct_commit_for_records<S: TruthMaintenanceStorage>(
    slot_key: &str,
    slot_type: &str,
    subject_id: &RecordId,
    predicate_id: &str,
    security_hash: &str,
    claim_records: &[ClaimRecord],
    space_id: &str,
    storage: &S,
) -> Result<DirectCommit, PipelineError> {
    if claim_records.is_empty() {
        return Err(PipelineError::InvalidInput(
            "direct commit requires at least one claim".to_string(),
        ));
    }

    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, predicate_id)
        .await?;
    let semantics = predicate.as_ref().and_then(|p| p.semantics.as_ref());

    let entity_displays = HashMap::new();
    let claims = claim_records
        .iter()
        .map(|record| build_claim_metadata(record, semantics, &entity_displays))
        .collect::<Vec<_>>();

    let mut claim_ids = claim_records
        .iter()
        .map(|record| record.id().to_string())
        .collect::<Vec<_>>();
    claim_ids.sort();
    claim_ids.dedup();

    let conflict_group = create_conflict_group(slot_key, slot_type, claim_ids.clone(), space_id);
    let policy = semantics
        .map(|s| s.conflict_policy)
        .unwrap_or(ConflictPolicy::AllowParallel);
    let resolution = resolve_slot_with_policy(&conflict_group, &claims, policy, semantics);

    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    let input = CommitInput {
        slot_key: slot_key.to_string(),
        slot_type: slot_type.to_string(),
        subject_entity: subject_id.to_string(),
        predicate_id: predicate_id.to_string(),
        security_hash: security_hash.to_string(),
        claim_ids,
        claim_set_hash: Some(claim_set_hash),
        ..CommitInput::default()
    };

    Ok(DirectCommit { resolution, input })
}

/// Builds a corroboration direct commit for a slot whose claims all share one
/// value: the duplicates' evidence merges into the winner at commit time.
fn build_corroboration_commit(
    slot_key: &str,
    slot_type: &str,
    subject_id: &RecordId,
    predicate_id: &str,
    security_hash: &str,
    claims: &[ClaimWithMetadata],
) -> DirectCommit {
    let mut claim_ids: Vec<String> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    claim_ids.sort();
    claim_ids.dedup();

    let resolution = corroborated_winner(claims);
    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    let input = CommitInput {
        slot_key: slot_key.to_string(),
        slot_type: slot_type.to_string(),
        subject_entity: subject_id.to_string(),
        predicate_id: predicate_id.to_string(),
        security_hash: security_hash.to_string(),
        claim_ids,
        claim_set_hash: Some(claim_set_hash),
        ..CommitInput::default()
    };

    DirectCommit { resolution, input }
}

/// Creates a conflict group from competing claims.
pub fn create_conflict_group(
    slot_key: &str,
    slot_type: &str,
    claim_ids: Vec<String>,
    space_id: &str,
) -> ConflictGroup {
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;

    let id = RecordId::new_v4(TableName::ConflictGroup);
    let base = BaseFields::new(id, space_id.to_string(), "system".to_string());

    ConflictGroup {
        base,
        slot_key: slot_key.to_string(),
        slot_type: slot_type.to_string(),
        claim_ids,
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    }
}

fn conflict_group_unresolved_id(space_id: &str, slot_type: &str, slot_key: &str) -> RecordId {
    use elephant_types::ids::TableName;

    let mut hasher = Sha256::new();
    hasher.update(space_id.as_bytes());
    hasher.update(b"::");
    hasher.update(slot_type.as_bytes());
    hasher.update(b"::");
    hasher.update(slot_key.as_bytes());
    hasher.update(b"::unresolved");
    let hash = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let uuid = Uuid::from_bytes(bytes);
    RecordId::new(TableName::ConflictGroup, uuid)
}

fn truth_slot_id(space_id: &str, slot_key: &str) -> RecordId {
    use elephant_types::ids::TableName;

    let mut hasher = Sha256::new();
    hasher.update(space_id.as_bytes());
    hasher.update(b"::");
    hasher.update(slot_key.as_bytes());
    hasher.update(b"::truth_slot");
    let hash = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let uuid = Uuid::from_bytes(bytes);
    RecordId::new(TableName::TruthSlot, uuid)
}

fn build_unresolved_conflict_group(
    slot_key: &str,
    slot_type: &str,
    claim_ids: Vec<String>,
    space_id: &str,
) -> ConflictGroup {
    let mut group = create_conflict_group(slot_key, slot_type, claim_ids, space_id);
    group.base.id = conflict_group_unresolved_id(space_id, slot_type, slot_key);
    group
}

// ============================================================================
// Conflict Resolution
// ============================================================================

/// Work kind identifier for tm_resolve_slot.
pub const TM_RESOLVE_SLOT_KIND: &str = "tm_resolve_slot";

/// A claim in a conflict group with its metadata.
#[derive(Debug, Clone)]
pub struct ClaimWithMetadata {
    /// The claim ID.
    pub claim_id: String,
    /// The claim's actual value.
    pub value: ClaimValue,
    /// Confidence score.
    pub confidence: f32,
    /// Timestamp for recency comparison.
    pub timestamp: DateTime<Utc>,
    /// Label of the basis that actually supplied `timestamp` for this claim.
    pub time_basis: &'static str,
    /// Context for split-by-context policy.
    pub context: Option<String>,
    /// Evidence count.
    pub evidence_count: usize,
    /// Evidence source identifiers/types for this claim.
    pub evidence_sources: Vec<String>,
    /// Deterministic status weight used for event ranking.
    pub status_weight: i32,
    /// Last update timestamp for deterministic tie-breaks.
    pub updated_at: DateTime<Utc>,
}

/// Result of conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolutionResult {
    /// Single winner determined.
    SingleWinner {
        /// ID of the winning claim.
        winner_id: String,
        /// Confidence score of the winner.
        confidence: f32,
        /// Explanation of the resolution decision.
        rationale: String,
    },
    /// All claims share one value; duplicates merge into the winner.
    CorroboratedWinner {
        /// ID of the claim that keeps the value.
        winner_id: String,
        /// Confidence score of the winner.
        confidence: f32,
        /// Duplicate claims whose evidence merges into the winner.
        merged_claim_ids: Vec<String>,
        /// Explanation of the resolution decision.
        rationale: String,
    },
    /// Multiple values allowed (AllowParallel).
    ParallelValues {
        /// Claims ranked by confidence (id, score).
        ranked_values: Vec<(String, f32)>,
        /// Explanation of the resolution decision.
        rationale: String,
    },
    /// Split by context (SplitByContext).
    SplitByContext {
        /// Winner per context bucket (context -> claim_id).
        context_winners: HashMap<String, String>,
        /// Explanation of the resolution decision.
        rationale: String,
    },
    /// Needs human review (RequireMoreEvidence).
    NeedsReview {
        /// Why the conflict needs review.
        reason: String,
    },
}

/// A loser claim whose validity window closes at a verdict-supplied date.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupersededOverride {
    /// Claim record id.
    pub claim_id: String,
    /// When the claim stopped being true, if the verdict states it.
    #[serde(default)]
    pub valid_until: Option<DateTime<Utc>>,
}

/// Adjudication side-channel carried from resolution into the commit payload.
#[derive(Debug, Clone, Default)]
pub struct ResolutionExtras {
    /// Whether the resolution came from an LLM adjudication verdict.
    pub adjudicated: bool,
    /// The resolution is a memoized replay and must not be committed.
    pub skip_commit: bool,
    /// Duplicate claims whose evidence merges into the winner.
    pub merged_claim_ids: Vec<String>,
    /// Verdict-supplied supersession dates for loser claims.
    pub superseded_overrides: Vec<SupersededOverride>,
    /// Verdict-supplied validity start for the winner.
    pub winner_valid_from: Option<DateTime<Utc>>,
}

/// Partitions claims into groups of identical values.
///
/// Vec-scan with `PartialEq` because `Literal` (Float/Json) has no `Eq`/`Hash`;
/// NaN and JSON-normalization mismatches yield distinct groups, which merely
/// routes to adjudication.
fn partition_value_groups(values: &[ClaimValue]) -> Vec<Vec<usize>> {
    let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if let Some((_, members)) = groups
            .iter_mut()
            .find(|(representative, _)| &values[*representative] == value)
        {
            members.push(index);
        } else {
            groups.push((index, vec![index]));
        }
    }
    groups.into_iter().map(|(_, members)| members).collect()
}

/// Builds a corroboration resolution for claims that all share one value.
///
/// Winner is the highest-confidence claim (tie: earliest creation, then claim
/// id — harmless because the values are identical); the rest merge into it.
fn corroborated_winner(claims: &[ClaimWithMetadata]) -> ResolutionResult {
    let mut ordered: Vec<&ClaimWithMetadata> = claims.iter().collect();
    ordered.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.updated_at.cmp(&b.updated_at))
            .then_with(|| a.claim_id.cmp(&b.claim_id))
    });

    let Some(winner) = ordered.first() else {
        return ResolutionResult::NeedsReview {
            reason: "No claims to corroborate".to_string(),
        };
    };
    let merged_claim_ids: Vec<String> = ordered
        .iter()
        .skip(1)
        .map(|claim| claim.claim_id.clone())
        .collect();

    ResolutionResult::CorroboratedWinner {
        winner_id: winner.claim_id.clone(),
        confidence: winner.confidence,
        rationale: format!(
            "Corroboration: {} claims share an identical value; evidence merged into {}",
            claims.len(),
            winner.claim_id
        ),
        merged_claim_ids,
    }
}

const ADJUDICATION_RATIONALE_NOTE_LIMIT: usize = 240;

fn truncated_rationale(rationale: &str) -> String {
    if rationale.len() <= ADJUDICATION_RATIONALE_NOTE_LIMIT {
        return rationale.to_string();
    }
    let mut end = ADJUDICATION_RATIONALE_NOTE_LIMIT;
    while !rationale.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &rationale[..end])
}

/// Builds the pure-data adjudication input from resolved claim metadata.
///
/// Each claim carries the basis that ACTUALLY supplied its timestamp (the
/// configured basis can be absent on a given claim) plus rendered evidence
/// notes so the adjudicator sees what the sources said, not just metadata.
fn build_adjudication_input(
    conflict_group: &ConflictGroup,
    claims: &[ClaimWithMetadata],
    space_id: &str,
    subject_display: String,
    predicate_id: String,
    semantics: Option<&PredicateSemantics>,
    evidence_notes: &HashMap<String, Vec<String>>,
) -> AdjudicationInput {
    let claims = claims
        .iter()
        .map(|claim| AdjudicationClaim {
            claim_id: claim.claim_id.clone(),
            value: claim.value.clone(),
            confidence: claim.confidence,
            evidence_count: claim.evidence_count,
            timestamp: claim.timestamp,
            time_basis: claim.time_basis.to_string(),
            sources: claim.evidence_sources.clone(),
            evidence_notes: evidence_notes
                .get(&claim.claim_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();

    AdjudicationInput {
        space_id: space_id.to_string(),
        slot_key: conflict_group.slot_key.clone(),
        slot_type: conflict_group.slot_type.clone(),
        subject_display,
        predicate_id,
        semantics: semantics.cloned(),
        claims,
    }
}

/// Maximum evidence assertions rendered per claim for adjudication.
const ADJUDICATION_EVIDENCE_NOTES_PER_CLAIM: usize = 3;
/// Maximum characters per rendered evidence note.
const ADJUDICATION_EVIDENCE_NOTE_CHARS: usize = 240;

/// Renders what an evidence assertion actually said, with honest time info.
fn render_assertion_note(assertion: &Assertion) -> String {
    let object = assertion.object_text.clone().unwrap_or_else(|| {
        assertion
            .object_literal
            .as_ref()
            .map_or_else(|| "(no object)".to_string(), ToString::to_string)
    });
    // Undated assertions must not leak ingestion dates: a late-ingested old
    // document would otherwise read as the most recent statement.
    let time = assertion.source_event_time.map_or_else(
        || "statement date unknown".to_string(),
        |t| format!("stated fact time {}", t.date_naive()),
    );
    let mut note = format!(
        "\"{} {} {}\" ({})",
        assertion.subject_text, assertion.predicate_raw, object, time
    );
    if note.len() > ADJUDICATION_EVIDENCE_NOTE_CHARS {
        let mut end = ADJUDICATION_EVIDENCE_NOTE_CHARS;
        while !note.is_char_boundary(end) {
            end -= 1;
        }
        note.truncate(end);
        note.push('…');
    }
    note
}

/// Fetches and renders per-claim evidence notes for adjudication; lookup
/// failures degrade to fewer notes rather than failing resolution.
async fn collect_evidence_notes<S: TruthMaintenanceStorage>(
    claims: &[ClaimWithMetadata],
    space_id: &str,
    storage: &S,
) -> HashMap<String, Vec<String>> {
    let mut notes: HashMap<String, Vec<String>> = HashMap::new();
    for claim in claims {
        let mut claim_notes = Vec::new();
        for assertion_id in claim
            .evidence_sources
            .iter()
            .take(ADJUDICATION_EVIDENCE_NOTES_PER_CLAIM)
        {
            let Ok(record_id) = RecordId::from_str(assertion_id) else {
                continue;
            };
            match storage.get_assertion(space_id, &record_id).await {
                Ok(Some(assertion)) => claim_notes.push(render_assertion_note(&assertion)),
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        claim_id = %claim.claim_id,
                        assertion_id = %assertion_id,
                        "Failed to load evidence assertion for adjudication: {err}"
                    );
                }
            }
        }
        if !claim_notes.is_empty() {
            notes.insert(claim.claim_id.clone(), claim_notes);
        }
    }
    notes
}

/// Maps a validated adjudication verdict onto a resolution result.
///
/// Equivalence-group members other than their representative become merges
/// only when the resolution has a single unambiguous winner to merge into
/// (corroborate / single_winner / supersede); for coexist verdicts all claims
/// stay approved because a flat merge list cannot express per-group targets.
fn map_adjudication_verdict(
    verdict: AdjudicationVerdict,
    claims: &[ClaimWithMetadata],
) -> (ResolutionResult, ResolutionExtras) {
    let confidence_of = |claim_id: &str| {
        claims
            .iter()
            .find(|claim| claim.claim_id == claim_id)
            .map_or(0.0, |claim| claim.confidence)
    };
    let top_confidence_id = || {
        claims
            .iter()
            .max_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.claim_id.cmp(&a.claim_id))
            })
            .map(|claim| claim.claim_id.clone())
    };
    let winner_id = verdict.winner_id.clone().or_else(top_confidence_id);

    // Equivalence dupes of the winner's group (representative = the winner).
    let winner_group_merges = |winner: &str| -> Vec<String> {
        verdict
            .equivalent_groups
            .iter()
            .find(|group| group.iter().any(|id| id == winner))
            .map(|group| {
                group
                    .iter()
                    .filter(|id| id.as_str() != winner)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    };

    match verdict.decision {
        AdjudicationDecision::Corroborate => {
            let Some(winner) = winner_id else {
                return (
                    ResolutionResult::NeedsReview {
                        reason: "Corroborate verdict without claims".to_string(),
                    },
                    ResolutionExtras {
                        adjudicated: true,
                        ..ResolutionExtras::default()
                    },
                );
            };
            let merged_claim_ids: Vec<String> = claims
                .iter()
                .filter(|claim| claim.claim_id != winner)
                .map(|claim| claim.claim_id.clone())
                .collect();
            (
                ResolutionResult::CorroboratedWinner {
                    confidence: confidence_of(&winner),
                    winner_id: winner,
                    merged_claim_ids,
                    rationale: verdict.rationale,
                },
                ResolutionExtras {
                    adjudicated: true,
                    ..ResolutionExtras::default()
                },
            )
        }
        AdjudicationDecision::SingleWinner | AdjudicationDecision::Supersede => {
            let Some(winner) = winner_id else {
                // Validator guarantees a winner; guard anyway.
                return (
                    ResolutionResult::NeedsReview {
                        reason: "Winner verdict without winner_id".to_string(),
                    },
                    ResolutionExtras {
                        adjudicated: true,
                        ..ResolutionExtras::default()
                    },
                );
            };
            let merged_claim_ids = winner_group_merges(&winner);
            let superseded_overrides = verdict
                .superseded
                .iter()
                .filter(|entry| entry.claim_id != winner)
                .filter(|entry| !merged_claim_ids.contains(&entry.claim_id))
                .map(|entry| SupersededOverride {
                    claim_id: entry.claim_id.clone(),
                    valid_until: entry.valid_until,
                })
                .collect();
            (
                ResolutionResult::SingleWinner {
                    confidence: confidence_of(&winner),
                    winner_id: winner,
                    rationale: verdict.rationale,
                },
                ResolutionExtras {
                    adjudicated: true,
                    merged_claim_ids,
                    superseded_overrides,
                    winner_valid_from: verdict.winner_valid_from,
                    ..ResolutionExtras::default()
                },
            )
        }
        AdjudicationDecision::Coexist => {
            let mut ranked_values: Vec<(String, f32)> = claims
                .iter()
                .map(|claim| (claim.claim_id.clone(), claim.confidence))
                .collect();
            ranked_values.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            (
                ResolutionResult::ParallelValues {
                    ranked_values,
                    rationale: verdict.rationale,
                },
                ResolutionExtras {
                    adjudicated: true,
                    ..ResolutionExtras::default()
                },
            )
        }
        AdjudicationDecision::NeedsReview => (
            ResolutionResult::NeedsReview {
                reason: verdict.rationale,
            },
            ResolutionExtras {
                adjudicated: true,
                ..ResolutionExtras::default()
            },
        ),
    }
}

/// Predicate semantics presented to the adjudicator for event slots.
///
/// Events have no registry predicate; supplying honest event semantics keeps
/// the verdict from proposing (and the pipeline from persisting) semantics for
/// a synthetic predicate id.
fn event_slot_semantics() -> PredicateSemantics {
    PredicateSemantics {
        temporal_model: TemporalPersonality::EventLike,
        cardinality: Cardinality::Many,
        conflict_policy: ConflictPolicy::AllowParallel,
        ..PredicateSemantics::default()
    }
}

/// Executes the tm_resolve_slot work kind.
///
/// Identical values corroborate without any LLM call. Real value conflicts go
/// to the injected adjudicator when present (memoized per claim set), with
/// today's deterministic policies as the fallback.
///
/// # Errors
///
/// Returns an error if the slot key is invalid or storage operations fail.
pub async fn execute_tm_resolve_slot<S: TruthMaintenanceStorage>(
    ctx: &TmContext,
    conflict_group: &ConflictGroup,
    claims: &[ClaimWithMetadata],
    space_id: &str,
    storage: &S,
) -> Result<(ResolutionResult, ResolutionExtras), PipelineError> {
    if claims.is_empty() {
        return Ok((
            ResolutionResult::NeedsReview {
                reason: "No claims to resolve".to_string(),
            },
            ResolutionExtras::default(),
        ));
    }

    let values: Vec<ClaimValue> = claims.iter().map(|claim| claim.value.clone()).collect();
    if partition_value_groups(&values).len() == 1 {
        return Ok((corroborated_winner(claims), ResolutionExtras::default()));
    }

    if conflict_group.slot_type == "event" {
        return resolve_multi_value_event_slot(ctx, conflict_group, claims, space_id, storage)
            .await;
    }

    let parts = parse_slot_key(&conflict_group.slot_key, &conflict_group.slot_type)?;
    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, &parts.predicate_id)
        .await?;
    let semantics = predicate.and_then(|p| p.semantics);

    if semantics.is_none() && claims.len() > 1 {
        let mut payload = OutboxPayload::new(
            space_id.to_string(),
            "predicate_conflict_no_semantics".to_string(),
            "truth_maintenance".to_string(),
        );
        payload.add_record(RecordRef {
            id: conflict_group.base.id.clone(),
            table: "conflict_group".to_string(),
            op: EventOperation::Update,
        });
        storage.emit_system_event(payload).await?;
    }

    resolve_multi_value_slot(
        ctx,
        conflict_group,
        claims,
        space_id,
        storage,
        parts,
        semantics,
    )
    .await
}

/// Multi-value-group resolution for event slots.
///
/// Events carry honest synthetic semantics into the prompt and never persist
/// proposed semantics; `resolve_event_group` is the deterministic fallback.
async fn resolve_multi_value_event_slot<S: TruthMaintenanceStorage>(
    ctx: &TmContext,
    conflict_group: &ConflictGroup,
    claims: &[ClaimWithMetadata],
    space_id: &str,
    storage: &S,
) -> Result<(ResolutionResult, ResolutionExtras), PipelineError> {
    let claim_ids: Vec<String> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    if conflict_group.adjudicated_claim_set_hash.as_deref() == Some(claim_set_hash.as_str()) {
        // Memoized replay: never re-commit (a commit here would clobber the
        // previously committed outcome with Disputed statuses).
        return Ok((
            ResolutionResult::NeedsReview {
                reason: "Adjudication memoized for unchanged claim set".to_string(),
            },
            ResolutionExtras {
                adjudicated: true,
                skip_commit: true,
                ..ResolutionExtras::default()
            },
        ));
    }

    if let Some(adjudicator) = ctx.adjudicator.as_ref() {
        let event_semantics = event_slot_semantics();
        let evidence_notes = collect_evidence_notes(claims, space_id, storage).await;
        let input = build_adjudication_input(
            conflict_group,
            claims,
            space_id,
            "(event group)".to_string(),
            "event".to_string(),
            Some(&event_semantics),
            &evidence_notes,
        );
        if let Some(verdict) = adjudicator.adjudicate(&input).await? {
            record_adjudication(
                conflict_group,
                &claim_set_hash,
                &verdict,
                true,
                &input.predicate_id,
                space_id,
                storage,
            )
            .await?;
            return Ok(map_adjudication_verdict(verdict, claims));
        }
    }

    Ok((resolve_event_group(claims), ResolutionExtras::default()))
}

/// Multi-value-group resolution for rel, attribute, and rel_set slots.
#[allow(clippy::too_many_arguments)]
async fn resolve_multi_value_slot<S: TruthMaintenanceStorage>(
    ctx: &TmContext,
    conflict_group: &ConflictGroup,
    claims: &[ClaimWithMetadata],
    space_id: &str,
    storage: &S,
    parts: SlotKeyParts,
    semantics: Option<PredicateSemantics>,
) -> Result<(ResolutionResult, ResolutionExtras), PipelineError> {
    let claim_ids: Vec<String> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    if conflict_group.adjudicated_claim_set_hash.as_deref() == Some(claim_set_hash.as_str()) {
        // Memoized replay: never re-commit (a commit here would clobber the
        // previously committed outcome with Disputed statuses).
        return Ok((
            ResolutionResult::NeedsReview {
                reason: "Adjudication memoized for unchanged claim set".to_string(),
            },
            ResolutionExtras {
                adjudicated: true,
                skip_commit: true,
                ..ResolutionExtras::default()
            },
        ));
    }

    if let Some(adjudicator) = ctx.adjudicator.as_ref() {
        let subject_display = match storage.get_entity(space_id, &parts.subject_id).await {
            Ok(Some(entity)) => entity.display_name,
            _ => parts.subject_id.to_string(),
        };
        let evidence_notes = collect_evidence_notes(claims, space_id, storage).await;
        let input = build_adjudication_input(
            conflict_group,
            claims,
            space_id,
            subject_display,
            parts.predicate_id.clone(),
            semantics.as_ref(),
            &evidence_notes,
        );
        if let Some(verdict) = adjudicator.adjudicate(&input).await? {
            record_adjudication(
                conflict_group,
                &claim_set_hash,
                &verdict,
                false,
                &parts.predicate_id,
                space_id,
                storage,
            )
            .await?;
            return Ok(map_adjudication_verdict(verdict, claims));
        }
    }

    // Deterministic fallback: today's policy selection.
    let policy = semantics
        .as_ref()
        .map_or(ConflictPolicy::AllowParallel, |s| s.conflict_policy);

    if matches!(policy, ConflictPolicy::AllowParallel) {
        let ranked_values = rank_claims_with_freshness(claims, space_id, storage).await?;
        return Ok((
            ResolutionResult::ParallelValues {
                ranked_values,
                rationale: "AllowParallel policy: ranked by confidence with freshness weighting"
                    .to_string(),
            },
            ResolutionExtras::default(),
        ));
    }

    Ok((
        resolve_slot_with_policy(conflict_group, claims, policy, semantics.as_ref()),
        ResolutionExtras::default(),
    ))
}

/// Persists memoization state and any proposed semantics after a verdict.
async fn record_adjudication<S: TruthMaintenanceStorage>(
    conflict_group: &ConflictGroup,
    claim_set_hash: &str,
    verdict: &AdjudicationVerdict,
    suppress_semantics: bool,
    predicate_id: &str,
    space_id: &str,
    storage: &S,
) -> Result<(), PipelineError> {
    let mut updated = conflict_group.clone();
    updated.adjudicated_claim_set_hash = Some(claim_set_hash.to_string());
    updated.adjudicated_at = Some(Utc::now());
    updated.base.updated_at = Utc::now();
    let group_id = updated.base.id.clone();
    storage
        .update_conflict_group(space_id, &group_id, updated)
        .await?;

    let Some(proposed) = verdict.proposed_semantics else {
        return Ok(());
    };
    if suppress_semantics {
        return Ok(());
    }

    let semantics = PredicateSemantics {
        cardinality: proposed.cardinality,
        temporal_model: proposed.temporal_model,
        conflict_policy: proposed.conflict_policy,
        ..PredicateSemantics::default()
    };
    let registry = storage
        .upsert_predicate_semantics(
            space_id,
            predicate_id,
            semantics,
            SemanticsStatus::LlmProposed,
            &truncated_rationale(&verdict.rationale),
        )
        .await?;

    // Configured semantics are never overwritten; skip the stale-marking event
    // when the upsert was refused.
    if registry.semantics_status != SemanticsStatus::LlmProposed {
        return Ok(());
    }

    // Standard predicate_registry event -> mark_slots_stale_by_predicate
    // machinery recomputes old slots under the learned semantics.
    let mut payload = OutboxPayload::new(
        space_id.to_string(),
        "predicate_registry.updated".to_string(),
        "truth_maintenance".to_string(),
    );
    payload.add_record(RecordRef {
        id: registry.base.id,
        table: "predicate_registry".to_string(),
        op: EventOperation::Update,
    });
    storage.emit_system_event(payload).await?;
    Ok(())
}

/// Fetches display names for the object entities referenced by rel claims.
///
/// Fetch failures degrade to the id string; this never errors.
async fn fetch_object_displays<S: TruthMaintenanceStorage>(
    claim_records: &[ClaimRecord],
    space_id: &str,
    storage: &S,
) -> HashMap<String, String> {
    let mut displays: HashMap<String, String> = HashMap::new();
    for record in claim_records {
        let ClaimRecord::Rel(rel) = record else {
            continue;
        };
        let object_id = rel.object.id().clone();
        let key = object_id.to_string();
        if displays.contains_key(&key) {
            continue;
        }
        let display = match storage.get_entity(space_id, &object_id).await {
            Ok(Some(entity)) => entity.display_name,
            _ => key.clone(),
        };
        displays.insert(key, display);
    }
    displays
}

/// Resolves a conflict group using storage-backed claims and builds commit input.
///
/// Returns both the resolution result and the commit payload required by tm_commit.
///
/// # Errors
///
/// Returns an error if claims cannot be loaded or the slot key is invalid.
pub async fn resolve_conflict_group_with_storage<S: TruthMaintenanceStorage>(
    ctx: &TmContext,
    conflict_group: &ConflictGroup,
    space_id: &str,
    storage: &S,
) -> Result<(ResolutionResult, CommitInput), PipelineError> {
    if conflict_group.slot_type == "event" {
        return resolve_event_conflict_group_with_storage(ctx, conflict_group, space_id, storage)
            .await;
    }

    let parts = parse_slot_key(&conflict_group.slot_key, &conflict_group.slot_type)?;
    let mut claim_ids = conflict_group.claim_ids.clone();

    let mut claim_records: Vec<ClaimRecord> = Vec::new();
    if conflict_group.slot_type == "rel_set" {
        // rel_set membership is derived, always reload the live claim set.
        claim_records = storage
            .find_rels_for_subject_predicate(space_id, &parts.subject_id, &parts.predicate_id)
            .await?
            .into_iter()
            .filter(|rel| compute_security_hash(&rel.base.security) == parts.security_hash)
            .map(ClaimRecord::Rel)
            .collect();
        if claim_records.is_empty() {
            return Err(PipelineError::NotFound(format!(
                "No claims found for conflict group {}",
                conflict_group.base.id
            )));
        }
        claim_ids = claim_records
            .iter()
            .map(|record| record.id().to_string())
            .collect();
    } else if !claim_ids.is_empty() {
        for claim_id in &claim_ids {
            let record_id = RecordId::from_str(claim_id).map_err(|e| {
                PipelineError::InvalidInput(format!("Invalid claim id {claim_id}: {e}"))
            })?;
            let record = match conflict_group.slot_type.as_str() {
                "rel" => storage
                    .get_rel(space_id, &record_id)
                    .await?
                    .map(ClaimRecord::Rel),
                "attribute" => storage
                    .get_attribute(space_id, &record_id)
                    .await?
                    .map(ClaimRecord::Attribute),
                other => {
                    return Err(PipelineError::InvalidInput(format!(
                        "Unknown slot type {other}"
                    )))
                }
            };
            // Conflict groups can outlive claim rows after relationship dedupe,
            // identity merges, or cleanup. Treat stale claim IDs as obsolete
            // membership rather than failing the whole maintenance item.
            if let Some(record) = record {
                claim_records.push(record);
            }
        }
        claim_ids = claim_records
            .iter()
            .map(|record| record.id().to_string())
            .collect();
    } else {
        let claims = match conflict_group.slot_type.as_str() {
            "rel" => {
                let Some(object_id) = parts.object_id.clone() else {
                    return Err(PipelineError::InvalidInput(
                        "Rel slot missing object id".to_string(),
                    ));
                };
                storage
                    .find_rels_for_slot(
                        space_id,
                        &parts.subject_id,
                        &object_id,
                        &parts.predicate_id,
                    )
                    .await?
                    .into_iter()
                    .filter(|rel| compute_security_hash(&rel.base.security) == parts.security_hash)
                    .map(ClaimRecord::Rel)
                    .collect::<Vec<_>>()
            }
            "attribute" => storage
                .find_attrs_for_slot(space_id, &parts.subject_id, &parts.predicate_id)
                .await?
                .into_iter()
                .filter(|attr| compute_security_hash(&attr.base.security) == parts.security_hash)
                .map(ClaimRecord::Attribute)
                .collect::<Vec<_>>(),
            other => {
                return Err(PipelineError::InvalidInput(format!(
                    "Unknown slot type {other}"
                )))
            }
        };

        if claims.is_empty() {
            return Err(PipelineError::NotFound(format!(
                "No claims found for conflict group {}",
                conflict_group.base.id
            )));
        }

        claim_ids = claims
            .iter()
            .map(|record| record.id().to_string())
            .collect();
        claim_records = claims;
    }

    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, &parts.predicate_id)
        .await?;
    let semantics = predicate.as_ref().and_then(|p| p.semantics.as_ref());

    let entity_displays = fetch_object_displays(&claim_records, space_id, storage).await;
    let claims = claim_records
        .iter()
        .map(|record| build_claim_metadata(record, semantics, &entity_displays))
        .collect::<Vec<_>>();

    if claims.is_empty() {
        return Err(PipelineError::NotFound(format!(
            "No claims to resolve for conflict group {}",
            conflict_group.base.id
        )));
    }

    let (resolution, extras) =
        execute_tm_resolve_slot(ctx, conflict_group, &claims, space_id, storage).await?;

    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    let input = CommitInput {
        slot_key: conflict_group.slot_key.clone(),
        slot_type: conflict_group.slot_type.clone(),
        subject_entity: parts.subject_id.to_string(),
        predicate_id: parts.predicate_id,
        security_hash: parts.security_hash,
        claim_ids,
        claim_set_hash: Some(claim_set_hash),
        adjudicated: extras.adjudicated,
        merged_claim_ids: extras.merged_claim_ids,
        superseded_overrides: extras.superseded_overrides,
        winner_valid_from: extras.winner_valid_from,
        skip_commit: extras.skip_commit,
    };

    Ok((resolution, input))
}

async fn resolve_event_conflict_group_with_storage<S: TruthMaintenanceStorage>(
    ctx: &TmContext,
    conflict_group: &ConflictGroup,
    space_id: &str,
    storage: &S,
) -> Result<(ResolutionResult, CommitInput), PipelineError> {
    let mut claim_ids = conflict_group.claim_ids.clone();
    let mut events: Vec<Event> = Vec::new();

    if !claim_ids.is_empty() {
        for claim_id in &claim_ids {
            let record_id = RecordId::from_str(claim_id).map_err(|e| {
                PipelineError::InvalidInput(format!("Invalid event claim id {claim_id}: {e}"))
            })?;
            let event = storage
                .get_event(space_id, &record_id)
                .await?
                .ok_or_else(|| {
                    PipelineError::NotFound(format!("Event claim not found: {claim_id}"))
                })?;
            events.push(event);
        }
    } else {
        let listed = storage.list_events(space_id, EVENT_SCAN_LIMIT).await?;
        events = listed
            .into_iter()
            .filter(|event| matches!(event.status, EventStatus::Approved | EventStatus::Candidate))
            .filter(|event| compute_event_slot_key(event) == conflict_group.slot_key)
            .collect();

        if events.is_empty() {
            return Err(PipelineError::NotFound(format!(
                "No event claims found for conflict group {}",
                conflict_group.base.id
            )));
        }
        claim_ids = events
            .iter()
            .map(|event| event.base.id.to_string())
            .collect();
    }

    claim_ids.sort();
    claim_ids.dedup();
    if claim_ids.is_empty() {
        return Err(PipelineError::NotFound(format!(
            "No event claims to resolve for conflict group {}",
            conflict_group.base.id
        )));
    }

    if events
        .iter()
        .any(|event| compute_event_slot_key(event) != conflict_group.slot_key)
    {
        return Err(PipelineError::InvalidInput(format!(
            "Invalid event slot_key {}",
            conflict_group.slot_key
        )));
    }

    let mut claims: Vec<ClaimWithMetadata> =
        events.iter().map(build_event_claim_metadata).collect();
    claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
    let (resolution, extras) =
        execute_tm_resolve_slot(ctx, conflict_group, &claims, space_id, storage).await?;
    let canonical_event = events
        .iter()
        .min_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()))
        .ok_or_else(|| {
            PipelineError::NotFound(format!(
                "No event claims to resolve for conflict group {}",
                conflict_group.base.id
            ))
        })?;
    let parts = build_event_slot_parts(canonical_event);

    let claim_set_hash = compute_claim_set_hash(&claim_ids);
    let input = CommitInput {
        slot_key: conflict_group.slot_key.clone(),
        slot_type: "event".to_string(),
        subject_entity: event_subject_from_participants_key(&parts.participants_key),
        predicate_id: parts.title_key,
        security_hash: compute_security_hash(&canonical_event.base.security),
        claim_ids,
        claim_set_hash: Some(claim_set_hash),
        adjudicated: extras.adjudicated,
        merged_claim_ids: extras.merged_claim_ids,
        superseded_overrides: extras.superseded_overrides,
        winner_valid_from: extras.winner_valid_from,
        skip_commit: extras.skip_commit,
    };

    Ok((resolution, input))
}

fn resolve_slot_with_policy(
    conflict_group: &ConflictGroup,
    claims: &[ClaimWithMetadata],
    policy: ConflictPolicy,
    semantics: Option<&PredicateSemantics>,
) -> ResolutionResult {
    match policy {
        ConflictPolicy::AllowParallel => resolve_allow_parallel(claims),
        ConflictPolicy::SupersedeByRecency => resolve_supersede_by_recency(claims),
        ConflictPolicy::SplitByContext => resolve_split_by_context(claims),
        ConflictPolicy::RequireMoreEvidence => {
            resolve_require_more_evidence(claims, conflict_group, semantics)
        }
    }
}

const FRESHNESS_WEIGHT: f32 = 0.5;

async fn rank_claims_with_freshness<S: TruthMaintenanceStorage>(
    claims: &[ClaimWithMetadata],
    space_id: &str,
    storage: &S,
) -> Result<Vec<(String, f32)>, PipelineError> {
    let mut freshness_scores = HashMap::new();
    for claim in claims {
        if let Some(score) = storage
            .find_freshness_score(space_id, &claim.claim_id)
            .await?
        {
            freshness_scores.insert(claim.claim_id.clone(), score);
        }
    }

    let mut ranking: Vec<TruthSlotRanking> = claims
        .iter()
        .map(|claim| TruthSlotRanking {
            slot_key: claim.claim_id.clone(),
            confidence: claim.confidence,
            freshness: 0.0,
            combined_score: 0.0,
        })
        .collect();

    apply_freshness_to_ranking(&mut ranking, &freshness_scores, FRESHNESS_WEIGHT);

    let mut combined: Vec<(String, f32, f32)> = ranking
        .into_iter()
        .map(|entry| {
            let confidence = claims
                .iter()
                .find(|claim| claim.claim_id == entry.slot_key)
                .map(|claim| claim.confidence)
                .unwrap_or(entry.confidence);
            (entry.slot_key, confidence, entry.combined_score)
        })
        .collect();

    combined.sort_by(|a, b| {
        b.2.partial_cmp(&a.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
    });

    Ok(combined
        .into_iter()
        .map(|(id, conf, _)| (id, conf))
        .collect())
}

/// AllowParallel: Return all values with ranking scores.
fn resolve_allow_parallel(claims: &[ClaimWithMetadata]) -> ResolutionResult {
    let mut ranked: Vec<(String, f32)> = claims
        .iter()
        .map(|c| (c.claim_id.clone(), c.confidence))
        .collect();

    // Sort by confidence descending
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    ResolutionResult::ParallelValues {
        ranked_values: ranked,
        rationale: "AllowParallel policy: all values retained with confidence ranking".to_string(),
    }
}

/// SupersedeByRecency: Newer timestamp wins.
fn resolve_supersede_by_recency(claims: &[ClaimWithMetadata]) -> ResolutionResult {
    let winner = claims.iter().max_by_key(|c| c.timestamp);

    match winner {
        Some(w) => ResolutionResult::SingleWinner {
            winner_id: w.claim_id.clone(),
            confidence: w.confidence,
            rationale: format!(
                "SupersedeByRecency: newest claim wins (timestamp: {})",
                w.timestamp
            ),
        },
        None => ResolutionResult::NeedsReview {
            reason: "No claims to resolve".to_string(),
        },
    }
}

/// SplitByContext: Separate by context field.
fn resolve_split_by_context(claims: &[ClaimWithMetadata]) -> ResolutionResult {
    let mut context_winners: HashMap<String, String> = HashMap::new();
    let mut by_context: HashMap<String, Vec<&ClaimWithMetadata>> = HashMap::new();

    // Group claims by context
    for claim in claims {
        let ctx = claim
            .context
            .clone()
            .unwrap_or_else(|| "default".to_string());
        by_context.entry(ctx).or_default().push(claim);
    }

    // Pick winner for each context (highest confidence)
    for (ctx, ctx_claims) in by_context {
        if let Some(winner) = ctx_claims.iter().max_by(|a, b| {
            a.confidence
                .partial_cmp(&b.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            context_winners.insert(ctx, winner.claim_id.clone());
        }
    }

    ResolutionResult::SplitByContext {
        context_winners,
        rationale: "SplitByContext: separate winners per context".to_string(),
    }
}

/// RequireMoreEvidence: Flag for human review.
fn resolve_require_more_evidence(
    claims: &[ClaimWithMetadata],
    _conflict_group: &ConflictGroup,
    semantics: Option<&PredicateSemantics>,
) -> ResolutionResult {
    let Some(semantics) = semantics else {
        return ResolutionResult::NeedsReview {
            reason: "RequireMoreEvidence: missing predicate semantics".to_string(),
        };
    };

    let min_evidence = semantics.min_evidence.max(1) as usize;
    let source_priority = semantics.source_priority.as_deref().unwrap_or(&[]);

    let eligible: Vec<&ClaimWithMetadata> = claims
        .iter()
        .filter(|c| c.evidence_count >= min_evidence)
        .collect();

    if eligible.is_empty() {
        return ResolutionResult::NeedsReview {
            reason: format!(
                "RequireMoreEvidence: {} competing claims with insufficient evidence (min_evidence={}); waiting for corroboration",
                claims.len(),
                min_evidence
            ),
        };
    }

    if !source_priority.is_empty()
        && !eligible
            .iter()
            .any(|claim| source_priority_rank(claim, source_priority).is_some())
    {
        return ResolutionResult::NeedsReview {
            reason: format!(
                "RequireMoreEvidence: no evidence from prioritized sources (min_evidence={}, source_priority={:?}); waiting for corroboration",
                min_evidence,
                source_priority
            ),
        };
    }

    ResolutionResult::NeedsReview {
        reason: format!(
            "RequireMoreEvidence: sufficient evidence found (min_evidence={}, eligible_claims={}); requires review",
            min_evidence,
            eligible.len()
        ),
    }
}

fn source_priority_rank(claim: &ClaimWithMetadata, source_priority: &[String]) -> Option<usize> {
    if source_priority.is_empty() || claim.evidence_sources.is_empty() {
        return None;
    }

    let sources: HashSet<&str> = claim.evidence_sources.iter().map(String::as_str).collect();
    source_priority
        .iter()
        .enumerate()
        .find(|(_, source)| sources.contains(source.as_str()))
        .map(|(idx, _)| idx)
}

// ============================================================================
// Temporal Model
// ============================================================================

/// Validity window for a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidityWindow {
    /// When the claim becomes valid.
    pub valid_from: DateTime<Utc>,
    /// When the claim stops being valid (None = still current).
    pub valid_to: Option<DateTime<Utc>>,
}

impl ValidityWindow {
    /// Creates a new validity window.
    ///
    /// # Errors
    ///
    /// Returns an error if `valid_to` is before `valid_from`.
    pub fn new(
        valid_from: DateTime<Utc>,
        valid_to: Option<DateTime<Utc>>,
    ) -> Result<Self, PipelineError> {
        if let Some(end) = valid_to {
            if end < valid_from {
                return Err(PipelineError::InvalidInput(
                    "validity_window.valid_to must be >= valid_from".to_string(),
                ));
            }
        }
        Ok(Self {
            valid_from,
            valid_to,
        })
    }

    /// Creates an open-ended validity window (still current).
    pub fn open_ended(valid_from: DateTime<Utc>) -> Self {
        Self {
            valid_from,
            valid_to: None,
        }
    }

    /// Checks if this window is still current (no end date).
    pub fn is_current(&self) -> bool {
        self.valid_to.is_none()
    }

    /// Closes the validity window at the given time.
    ///
    /// # Errors
    ///
    /// Returns an error if `end_time` is before the window's start time.
    pub fn close_at(&mut self, end_time: DateTime<Utc>) -> Result<(), PipelineError> {
        if end_time < self.valid_from {
            return Err(PipelineError::InvalidInput(
                "Cannot close validity window before it started".to_string(),
            ));
        }
        self.valid_to = Some(end_time);
        Ok(())
    }
}

/// Handles temporal personality for a claim.
///
/// Per spec §12: different predicates have different temporal behaviors.
pub fn handle_temporal_personality(
    personality: TemporalPersonality,
    new_claim_time: DateTime<Utc>,
    existing_claims: &[(String, ValidityWindow)],
) -> TemporalHandlingResult {
    match personality {
        TemporalPersonality::Persistent => {
            // Once true, always true - new evidence corroborates or conflicts
            TemporalHandlingResult::Corroborate
        }
        TemporalPersonality::StateOverTime => {
            // Check if we need to close existing windows
            let windows_to_close: Vec<String> = existing_claims
                .iter()
                .filter(|(_, w)| w.is_current() && w.valid_from < new_claim_time)
                .map(|(id, _)| id.clone())
                .collect();

            if windows_to_close.is_empty() {
                TemporalHandlingResult::AddNew
            } else {
                TemporalHandlingResult::SupersedeExisting {
                    close_ids: windows_to_close,
                    close_at: new_claim_time,
                }
            }
        }
        TemporalPersonality::EventLike => {
            // Events accumulate - just add to the list
            TemporalHandlingResult::AddNew
        }
    }
}

/// Result of temporal personality handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalHandlingResult {
    /// New claim corroborates existing (Persistent).
    Corroborate,
    /// Add new claim without affecting existing.
    AddNew,
    /// Supersede existing claims by closing their validity windows.
    SupersedeExisting {
        /// IDs of claims whose validity windows should be closed.
        close_ids: Vec<String>,
        /// Timestamp at which to close the validity windows.
        close_at: DateTime<Utc>,
    },
}

/// Populates temporal audit information.
pub fn populate_temporal_audit(
    source_event_time: Option<DateTime<Utc>>,
    _observed_at: DateTime<Utc>,
    _asserted_at: DateTime<Utc>,
) -> TemporalAudit {
    // Prefer source_event_time if available, otherwise fall back to observed_at
    let time_basis = if source_event_time.is_some() {
        RecencyBasis::SourceEventTime
    } else {
        RecencyBasis::ObservedAt
    };

    TemporalAudit {
        time_basis,
        time_refinement: None,
    }
}

/// Refines temporal audit with better evidence.
pub fn refine_temporal_audit(
    audit: &mut TemporalAudit,
    refined_by: RecordId,
    new_time: DateTime<Utc>,
    original_time: DateTime<Utc>,
) {
    use elephant_types::Link;

    audit.time_refinement = Some(TimeRefinement {
        original_basis: audit.time_basis,
        original_value: original_time,
        refined_by: Link::new(refined_by),
        refined_at: Utc::now(),
    });

    // Update to use source_event_time now that we have explicit timestamp
    audit.time_basis = RecencyBasis::SourceEventTime;

    let _ = new_time; // Would be used to update the actual claim timestamp
}

// ============================================================================
// Commit + Staleness
// ============================================================================

/// Work kind identifier for tm_commit.
pub const TM_COMMIT_KIND: &str = "tm_commit";

/// Result of tm_commit work kind.
#[derive(Debug, Clone, Default)]
pub struct CommitResult {
    /// Number of truth slots updated.
    pub slots_updated: usize,
    /// Number of audit log entries created.
    pub audit_entries: usize,
    /// IDs of updated truth slots.
    pub slot_ids: Vec<RecordId>,
    /// Whether the commit was skipped because the live claim set changed.
    pub skipped_stale: bool,
}

/// Executes the tm_commit work kind.
///
/// Per spec §11.4: commits resolution results to truth slots.
/// Must be deterministic: same input produces same output.
///
/// # Errors
///
/// Returns an error if the commit operation fails.
pub async fn execute_tm_commit<S: TruthMaintenanceStorage>(
    resolution: &ResolutionResult,
    input: &CommitInput,
    space_id: &str,
    storage: &S,
) -> Result<CommitResult, PipelineError> {
    execute_tm_commit_with_storage(resolution, input, space_id, storage).await
}

/// Input for tm_commit with storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommitInput {
    /// The slot key for this truth slot.
    pub slot_key: String,
    /// The slot type ("rel", "attribute", "rel_set", or "event").
    pub slot_type: String,
    /// Subject entity ID.
    pub subject_entity: String,
    /// Predicate ID.
    pub predicate_id: String,
    /// Security hash for the slot.
    pub security_hash: String,
    /// Competing claim IDs for this slot.
    pub claim_ids: Vec<String>,
    /// Hash of the claim set the resolution was computed over; commit no-ops
    /// when the live claim set no longer matches.
    #[serde(default)]
    pub claim_set_hash: Option<String>,
    /// Whether the resolution came from an LLM adjudication verdict.
    #[serde(default)]
    pub adjudicated: bool,
    /// Duplicate claims whose evidence merges into the winner.
    #[serde(default)]
    pub merged_claim_ids: Vec<String>,
    /// Verdict-supplied supersession dates for loser claims.
    #[serde(default)]
    pub superseded_overrides: Vec<SupersededOverride>,
    /// Verdict-supplied validity start for the winner.
    #[serde(default)]
    pub winner_valid_from: Option<DateTime<Utc>>,
    /// The resolution is a memoized replay: callers must not enqueue a commit
    /// and the commit executor no-ops as defense in depth.
    #[serde(default)]
    pub skip_commit: bool,
}

#[derive(Debug, Clone, Copy)]
struct PromotionThresholds {
    min_confidence: f32,
    min_evidence: usize,
}

#[derive(Debug, Clone)]
enum ClaimRecord {
    Rel(Rel),
    Attribute(Attribute),
}

#[derive(Debug, Clone)]
struct ClaimSnapshot {
    confidence: f32,
    evidence: Vec<EvidenceRef>,
}

#[derive(Debug, Clone)]
struct RefinementEvidence {
    assertion_id: RecordId,
    time_value: DateTime<Utc>,
    evidence: EvidenceRef,
}

impl ClaimRecord {
    fn id(&self) -> &RecordId {
        match self {
            Self::Rel(rel) => &rel.base.id,
            Self::Attribute(attr) => &attr.base.id,
        }
    }

    fn confidence(&self) -> f32 {
        match self {
            Self::Rel(rel) => rel.confidence,
            Self::Attribute(attr) => attr.confidence,
        }
    }

    fn evidence(&self) -> &[EvidenceRef] {
        match self {
            Self::Rel(rel) => &rel.evidence,
            Self::Attribute(attr) => &attr.evidence,
        }
    }

    fn evidence_mut(&mut self) -> &mut Vec<EvidenceRef> {
        match self {
            Self::Rel(rel) => &mut rel.evidence,
            Self::Attribute(attr) => &mut attr.evidence,
        }
    }

    fn temporal_audit(&self) -> Option<&TemporalAudit> {
        match self {
            Self::Rel(rel) => rel.temporal_audit.as_ref(),
            Self::Attribute(attr) => attr.temporal_audit.as_ref(),
        }
    }

    fn temporal_audit_mut(&mut self) -> &mut Option<TemporalAudit> {
        match self {
            Self::Rel(rel) => &mut rel.temporal_audit,
            Self::Attribute(attr) => &mut attr.temporal_audit,
        }
    }

    fn created_at(&self) -> DateTime<Utc> {
        match self {
            Self::Rel(rel) => rel.base.created_at,
            Self::Attribute(attr) => attr.base.created_at,
        }
    }

    fn validity_window(&self) -> Option<RelValidityWindow> {
        match self {
            Self::Rel(rel) => rel.validity_window.clone(),
            Self::Attribute(attr) => attr.validity_window.clone(),
        }
    }

    fn set_validity_window(&mut self, window: Option<RelValidityWindow>) {
        match self {
            Self::Rel(rel) => rel.validity_window = window,
            Self::Attribute(attr) => attr.validity_window = window,
        }
    }

    fn set_status(&mut self, status: ClaimStatus) {
        match self {
            Self::Rel(rel) => rel.status = status,
            Self::Attribute(attr) => attr.status = status,
        }
    }

    fn status(&self) -> ClaimStatus {
        match self {
            Self::Rel(rel) => rel.status,
            Self::Attribute(attr) => attr.status,
        }
    }

    fn soft_delete(&mut self) {
        match self {
            Self::Rel(rel) => rel.base.soft_delete(),
            Self::Attribute(attr) => attr.base.soft_delete(),
        }
    }

    fn snapshot(&self) -> ClaimSnapshot {
        ClaimSnapshot {
            confidence: self.confidence(),
            evidence: self.evidence().to_vec(),
        }
    }
}

fn claim_is_explicit(record: &ClaimRecord) -> bool {
    record
        .evidence()
        .iter()
        .any(|evidence| evidence.time_basis == TimeBasis::SourceEventTime)
}

fn claim_basis_and_time(record: &ClaimRecord) -> (RecencyBasis, DateTime<Utc>) {
    let basis = record
        .temporal_audit()
        .map(|audit| audit.time_basis)
        .unwrap_or_else(|| {
            if record
                .evidence()
                .iter()
                .any(|evidence| evidence.time_basis == TimeBasis::ObservedAt)
            {
                RecencyBasis::ObservedAt
            } else if record
                .evidence()
                .iter()
                .any(|evidence| evidence.time_basis == TimeBasis::AssertedAt)
            {
                RecencyBasis::AssertedAt
            } else {
                RecencyBasis::SourceEventTime
            }
        });
    let time_basis = time_basis_from_recency(basis);
    let time_value = record
        .evidence()
        .iter()
        .filter(|evidence| evidence.time_basis == time_basis)
        .map(|evidence| evidence.time_value)
        .max()
        .unwrap_or_else(|| record.created_at());
    (basis, time_value)
}

fn refinement_evidence_from_record(record: &ClaimRecord) -> Option<RefinementEvidence> {
    record
        .evidence()
        .iter()
        .filter(|evidence| evidence.time_basis == TimeBasis::SourceEventTime)
        .max_by_key(|evidence| evidence.time_value)
        .map(|evidence| RefinementEvidence {
            assertion_id: evidence.assertion.id().clone(),
            time_value: evidence.time_value,
            evidence: evidence.clone(),
        })
}

fn select_refinement_evidence(
    claim_records: &HashMap<String, ClaimRecord>,
    resolution: &ResolutionResult,
) -> Option<RefinementEvidence> {
    if let ResolutionResult::SingleWinner { winner_id, .. }
    | ResolutionResult::CorroboratedWinner { winner_id, .. } = resolution
    {
        if let Some(record) = claim_records.get(winner_id) {
            if let Some(refinement) = refinement_evidence_from_record(record) {
                return Some(refinement);
            }
        }
    }

    claim_records
        .values()
        .filter_map(refinement_evidence_from_record)
        .max_by_key(|evidence| evidence.time_value)
}

fn apply_refine_implicit(record: &mut ClaimRecord, refinement: &RefinementEvidence) {
    let (original_basis, original_time) = claim_basis_and_time(record);
    if record.temporal_audit().is_none() {
        *record.temporal_audit_mut() = Some(TemporalAudit {
            time_basis: original_basis,
            time_refinement: None,
        });
    }

    if let Some(audit) = record.temporal_audit_mut().as_mut() {
        if audit.time_basis != RecencyBasis::SourceEventTime {
            refine_temporal_audit(
                audit,
                refinement.assertion_id.clone(),
                refinement.time_value,
                original_time,
            );
        }
    }

    if !record
        .evidence()
        .iter()
        .any(|evidence| evidence.assertion.id() == &refinement.assertion_id)
    {
        record.evidence_mut().push(refinement.evidence.clone());
    }

    let mut window = record
        .validity_window()
        .unwrap_or(RelValidityWindow::starting_at(refinement.time_value));
    if window
        .start
        .map_or(true, |start| start > refinement.time_value)
    {
        window.start = Some(refinement.time_value);
    }
    record.set_validity_window(Some(window));
}

fn promotion_thresholds(semantics: Option<&PredicateSemantics>) -> PromotionThresholds {
    let semantics = semantics.cloned().unwrap_or_default();
    PromotionThresholds {
        min_confidence: semantics.promotion_threshold,
        min_evidence: semantics.min_evidence.max(1) as usize,
    }
}

fn timestamp_to_version(timestamp: DateTime<Utc>) -> u32 {
    let seconds = timestamp.timestamp();
    if seconds <= 0 {
        0
    } else if seconds > u32::MAX as i64 {
        u32::MAX
    } else {
        seconds as u32
    }
}

fn semantics_version_from_registry(predicate: Option<&PredicateRegistry>) -> u32 {
    predicate
        .map(|p| timestamp_to_version(p.base.updated_at))
        .unwrap_or(0)
}

fn alias_version_from_aliases(aliases: &[PredicateAlias]) -> u32 {
    aliases
        .iter()
        .map(|alias| alias.confidence_version)
        .max()
        .unwrap_or(0)
}

fn audit_snapshot<T: Serialize>(record: &T) -> Result<JsonValue, PipelineError> {
    serde_json::to_value(record).map_err(|e| {
        PipelineError::InvalidInput(format!("Failed to serialize audit snapshot: {e}"))
    })
}

fn build_audit_log(
    space_id: &str,
    actor: &str,
    action: AuditAction,
    record_id: &RecordId,
    before: Option<JsonValue>,
    after: Option<JsonValue>,
    reason: Option<String>,
) -> AuditLog {
    use elephant_types::records::base::BaseFields;
    let id = RecordId::new_v4(TableName::AuditLog);
    let base = BaseFields::new(id, space_id.to_string(), actor.to_string());

    AuditLog {
        base,
        action,
        actor: actor.to_string(),
        record: record_id.clone(),
        before,
        after,
        timestamp: Utc::now(),
        reason,
    }
}

async fn execute_tm_commit_events<S: TruthMaintenanceStorage>(
    resolution: &ResolutionResult,
    input: &CommitInput,
    space_id: &str,
    storage: &S,
    unresolved_group: Option<ConflictGroup>,
    claim_ids: Vec<String>,
) -> Result<CommitResult, PipelineError> {
    let mut audit_entries = 0;
    let mut winner_ids: HashSet<String> = HashSet::new();
    let needs_review = matches!(resolution, ResolutionResult::NeedsReview { .. });

    // Load live events up front for the staleness guard and evidence merges.
    let mut events_by_id: HashMap<String, Event> = HashMap::new();
    for claim_id in &claim_ids {
        let record_id = RecordId::from_str(claim_id).map_err(|e| {
            PipelineError::InvalidInput(format!("Invalid event claim id {claim_id}: {e}"))
        })?;
        if let Some(event) = storage.get_event(space_id, &record_id).await? {
            events_by_id.insert(claim_id.clone(), event);
        }
    }

    // Staleness guard, same contract as the rel/attribute path.
    if let Some(expected_hash) = input.claim_set_hash.as_deref() {
        let live_ids: Vec<String> = events_by_id.keys().cloned().collect();
        if compute_claim_set_hash(&live_ids) != expected_hash {
            tracing::warn!(
                slot_key = %input.slot_key,
                "tm_commit (events) skipped: live claim set changed since resolution"
            );
            return Ok(CommitResult {
                skipped_stale: true,
                ..CommitResult::default()
            });
        }
    }

    let mut merged_ids: HashSet<String> = input.merged_claim_ids.iter().cloned().collect();
    let merge_winner = match resolution {
        ResolutionResult::SingleWinner { winner_id, .. } => {
            winner_ids.insert(winner_id.clone());
            Some(winner_id.clone())
        }
        ResolutionResult::CorroboratedWinner {
            winner_id,
            merged_claim_ids,
            ..
        } => {
            winner_ids.insert(winner_id.clone());
            merged_ids.extend(merged_claim_ids.iter().cloned());
            Some(winner_id.clone())
        }
        ResolutionResult::ParallelValues { ranked_values, .. } => {
            // Coexist keeps every ranked event, not just the first.
            winner_ids.extend(ranked_values.iter().map(|(id, _)| id.clone()));
            None
        }
        ResolutionResult::SplitByContext {
            context_winners, ..
        } => {
            winner_ids.extend(context_winners.values().cloned());
            None
        }
        ResolutionResult::NeedsReview { .. } => None,
    };

    merged_ids.retain(|id| events_by_id.contains_key(id));
    if let Some(winner_id) = merge_winner.as_ref() {
        merged_ids.remove(winner_id);
        if !merged_ids.is_empty() {
            let mut incoming_assertions = Vec::new();
            let mut incoming_sources = Vec::new();
            for id in &merged_ids {
                if let Some(event) = events_by_id.get(id) {
                    incoming_assertions.extend(event.assertion_refs.iter().cloned());
                    incoming_sources.extend(event.source_refs.iter().cloned());
                }
            }
            if let Some(winner) = events_by_id.get_mut(winner_id) {
                let mut seen: HashSet<String> = winner
                    .assertion_refs
                    .iter()
                    .map(|link| link.id().to_string())
                    .collect();
                for link in incoming_assertions {
                    if seen.insert(link.id().to_string()) {
                        winner.assertion_refs.push(link);
                    }
                }
                let mut seen_sources: HashSet<String> = winner
                    .source_refs
                    .iter()
                    .map(|link| link.id().to_string())
                    .collect();
                for link in incoming_sources {
                    if seen_sources.insert(link.id().to_string()) {
                        winner.source_refs.push(link);
                    }
                }
            }
        }
    } else {
        merged_ids.clear();
    }

    let reject_floor = event_reject_floor();

    // Persist surviving events (the winner with its merged refs above all)
    // before soft-deleting duplicates so a crash mid-commit retries cleanly.
    let ordered_claim_ids: Vec<&String> = claim_ids
        .iter()
        .filter(|id| !merged_ids.contains(*id))
        .chain(claim_ids.iter().filter(|id| merged_ids.contains(*id)))
        .collect();

    for claim_id in ordered_claim_ids {
        let mut event = events_by_id
            .remove(claim_id)
            .ok_or_else(|| PipelineError::NotFound(format!("Event claim not found: {claim_id}")))?;

        let before_snapshot = audit_snapshot(&event)?;

        // Corroborated duplicates: refs merged into the winner; soft-delete.
        if merged_ids.contains(claim_id) {
            let event_id = event.base.id.clone();
            storage.delete_event(space_id, &event_id).await?;
            event.base.soft_delete();
            let after_snapshot = audit_snapshot(&event)?;
            let audit = build_audit_log(
                space_id,
                TM_COMMIT_KIND,
                AuditAction::Update,
                &event_id,
                Some(before_snapshot),
                Some(after_snapshot),
                Some("tm_commit: corroborated duplicate merged".to_string()),
            );
            storage.create_audit_log(audit).await?;
            audit_entries += 1;
            continue;
        }

        // The reject floor is a deterministic heuristic; adjudicated verdicts
        // already decided every claim's fate.
        event.status = if needs_review {
            EventStatus::Candidate
        } else if winner_ids.contains(claim_id) {
            EventStatus::Approved
        } else if !input.adjudicated && event.confidence < reject_floor {
            EventStatus::Rejected
        } else {
            EventStatus::Superseded
        };
        event.base.updated_at = Utc::now();
        let event_id = event.base.id.clone();
        let updated = storage.update_event(space_id, &event_id, event).await?;
        let after_snapshot = audit_snapshot(&updated)?;

        let audit = build_audit_log(
            space_id,
            TM_COMMIT_KIND,
            AuditAction::Update,
            &event_id,
            Some(before_snapshot),
            Some(after_snapshot),
            Some(TM_COMMIT_KIND.to_string()),
        );
        storage.create_audit_log(audit).await?;
        audit_entries += 1;
    }

    if !needs_review {
        if let Some(group) = unresolved_group {
            let before_snapshot = audit_snapshot(&group)?;
            let mut updated_group = group;
            updated_group.base.updated_at = Utc::now();
            updated_group.resolved = true;
            updated_group.resolution_rationale = match resolution {
                ResolutionResult::SingleWinner { rationale, .. }
                | ResolutionResult::CorroboratedWinner { rationale, .. }
                | ResolutionResult::ParallelValues { rationale, .. }
                | ResolutionResult::SplitByContext { rationale, .. } => Some(rationale.clone()),
                ResolutionResult::NeedsReview { reason } => Some(reason.clone()),
            };
            updated_group.resolved_at = Some(Utc::now());
            // Memoize the decided claim set so detection/recompute skip
            // unchanged sets even for deterministic (keyless) resolutions.
            if input.claim_set_hash.is_some() {
                updated_group.adjudicated_claim_set_hash = input.claim_set_hash.clone();
                updated_group.adjudicated_at = Some(Utc::now());
            }
            let group_id = updated_group.base.id.clone();
            let updated = storage
                .update_conflict_group(space_id, &group_id, updated_group)
                .await?;
            let after_snapshot = audit_snapshot(&updated)?;
            let audit = build_audit_log(
                space_id,
                TM_COMMIT_KIND,
                AuditAction::Update,
                &group_id,
                Some(before_snapshot),
                Some(after_snapshot),
                Some(TM_COMMIT_KIND.to_string()),
            );
            storage.create_audit_log(audit).await?;
            audit_entries += 1;
        }
    }

    Ok(CommitResult {
        slots_updated: 0,
        audit_entries,
        slot_ids: Vec::new(),
        skipped_stale: false,
    })
}

fn event_reject_floor() -> f32 {
    std::env::var("EVENT_REJECT_FLOOR")
        .ok()
        .and_then(|raw| raw.parse::<f32>().ok())
        .filter(|value| (0.0..=1.0).contains(value))
        .unwrap_or(EVENT_REJECT_FLOOR_DEFAULT)
}

fn apply_supersession_window(
    record: &mut ClaimRecord,
    end_time: DateTime<Utc>,
    semantics: Option<&PredicateSemantics>,
) -> Result<(), PipelineError> {
    let start_time = record
        .validity_window()
        .and_then(|window| window.start)
        .unwrap_or_else(|| claim_timestamp_for_semantics(record, semantics));

    let mut window = record
        .validity_window()
        .unwrap_or(RelValidityWindow::starting_at(start_time));
    if window.end.map_or(true, |end| end > end_time) {
        window.end = Some(end_time);
    }
    record.set_validity_window(Some(window));
    Ok(())
}

fn apply_winner_window(record: &mut ClaimRecord, start_time: DateTime<Utc>) {
    let mut window = record
        .validity_window()
        .unwrap_or(RelValidityWindow::starting_at(start_time));
    if window.start.is_none() {
        window.start = Some(start_time);
    }
    record.set_validity_window(Some(window));
}

/// Executes tm_commit with real storage writes.
///
/// Per spec §11.4: commits resolution results to truth slots.
/// Creates or updates truth_slot records based on resolution.
///
/// # Errors
///
/// Returns an error if storage operations fail.
pub async fn execute_tm_commit_with_storage<S: TruthMaintenanceStorage>(
    resolution: &ResolutionResult,
    input: &CommitInput,
    space_id: &str,
    storage: &S,
) -> Result<CommitResult, PipelineError> {
    use elephant_types::records::base::BaseFields;
    use elephant_types::Link;

    // Memoized replays are never committed; a commit here would clobber the
    // previously committed outcome with Disputed statuses.
    if input.skip_commit {
        tracing::debug!(
            slot_key = %input.slot_key,
            "tm_commit skipped: resolution was a memoized replay"
        );
        return Ok(CommitResult::default());
    }

    let mut slots_updated = 0;
    let mut slot_ids = Vec::new();

    let unresolved_group = storage
        .find_unresolved_conflict_by_type(space_id, &input.slot_key, &input.slot_type)
        .await?;

    let mut claim_ids = input.claim_ids.clone();
    if claim_ids.is_empty() {
        if let Some(group) = &unresolved_group {
            claim_ids = group.claim_ids.clone();
        }
    }

    if claim_ids.is_empty() {
        match resolution {
            ResolutionResult::SingleWinner { winner_id, .. } => {
                claim_ids.push(winner_id.clone());
            }
            ResolutionResult::CorroboratedWinner {
                winner_id,
                merged_claim_ids,
                ..
            } => {
                claim_ids.push(winner_id.clone());
                claim_ids.extend(merged_claim_ids.iter().cloned());
            }
            ResolutionResult::ParallelValues { ranked_values, .. } => {
                claim_ids = ranked_values.iter().map(|(id, _)| id.clone()).collect();
            }
            ResolutionResult::SplitByContext {
                context_winners, ..
            } => {
                claim_ids = context_winners.values().cloned().collect();
            }
            ResolutionResult::NeedsReview { .. } => {}
        }
    }

    claim_ids.sort();
    claim_ids.dedup();

    if claim_ids.is_empty() {
        return Err(PipelineError::InvalidInput(
            "tm_commit requires claim_ids".to_string(),
        ));
    }

    if input.slot_type == "event" {
        return execute_tm_commit_events(
            resolution,
            input,
            space_id,
            storage,
            unresolved_group,
            claim_ids,
        )
        .await;
    }

    let mut claim_records: HashMap<String, ClaimRecord> = HashMap::new();

    for claim_id in &claim_ids {
        let record_id = RecordId::from_str(claim_id).map_err(|e| {
            PipelineError::InvalidInput(format!("Invalid claim id {claim_id}: {e}"))
        })?;

        let record = match input.slot_type.as_str() {
            "rel" | "rel_set" => match storage.get_rel(space_id, &record_id).await? {
                Some(rel) => Some(ClaimRecord::Rel(rel)),
                None => {
                    let deleted = storage.get_rel_with_deleted(space_id, &record_id).await?;
                    if deleted.is_some() {
                        tracing::debug!(claim_id, "tm_commit skipped soft-deleted rel claim");
                    }
                    None
                }
            },
            "attribute" => match storage.get_attribute(space_id, &record_id).await? {
                Some(attribute) => Some(ClaimRecord::Attribute(attribute)),
                None => {
                    let deleted = storage
                        .get_attribute_with_deleted(space_id, &record_id)
                        .await?;
                    if deleted.is_some() {
                        tracing::debug!(claim_id, "tm_commit skipped soft-deleted attribute claim");
                    }
                    None
                }
            },
            other => {
                return Err(PipelineError::InvalidInput(format!(
                    "Unknown slot type {other}"
                )))
            }
        };

        let Some(record) = record else {
            continue;
        };

        claim_records.insert(claim_id.clone(), record);
    }

    // Staleness guard: the resolution was computed over a specific claim set;
    // if the live set no longer matches, no-op and let re-detection converge.
    if let Some(expected_hash) = input.claim_set_hash.as_deref() {
        let live_ids: Vec<String> = claim_records.keys().cloned().collect();
        if compute_claim_set_hash(&live_ids) != expected_hash {
            tracing::warn!(
                slot_key = %input.slot_key,
                slot_type = %input.slot_type,
                "tm_commit skipped: live claim set changed since resolution"
            );
            return Ok(CommitResult {
                skipped_stale: true,
                ..CommitResult::default()
            });
        }
    }

    if claim_records.is_empty() {
        tracing::debug!(
            slot_key = %input.slot_key,
            slot_type = %input.slot_type,
            "tm_commit had no active claims after cleanup; treating as no-op"
        );
        return Ok(CommitResult::default());
    }

    let predicate = storage
        .find_predicate_registry_by_predicate_id(space_id, &input.predicate_id)
        .await?;
    let semantics = predicate.as_ref().and_then(|p| p.semantics.as_ref());
    let thresholds = promotion_thresholds(semantics);
    let conflict_policy = semantics
        .map(|semantics| semantics.conflict_policy)
        .unwrap_or_default();
    let aliases = storage
        .list_predicate_aliases_by_predicate_id(space_id, &input.predicate_id)
        .await?;
    let alias_version = alias_version_from_aliases(&aliases);
    let semantics_version = semantics_version_from_registry(predicate.as_ref());

    let refinement_policy = semantics
        .map(|semantics| semantics.refinement_policy)
        .unwrap_or_default();
    let explicit_ids: HashSet<String> = claim_records
        .iter()
        .filter(|(_, record)| claim_is_explicit(record))
        .map(|(id, _)| id.clone())
        .collect();
    let implicit_ids: HashSet<String> = claim_records
        .keys()
        .filter(|id| !explicit_ids.contains(*id))
        .cloned()
        .collect();
    let refinement = if !explicit_ids.is_empty() && !implicit_ids.is_empty() {
        select_refinement_evidence(&claim_records, resolution)
    } else {
        None
    };

    if refinement_policy == RefinementPolicy::RefineImplicit {
        if let Some(refinement) = refinement.as_ref() {
            for id in &implicit_ids {
                if let Some(record) = claim_records.get_mut(id) {
                    apply_refine_implicit(record, refinement);
                }
            }
        }
    }

    // Corroboration/equivalence merges: fold the duplicates' evidence into the
    // winner before snapshots so the truth slot shows the merged evidence set.
    // No supersession windows: a corroborating duplicate never stopped being true.
    let merge_winner_id = match resolution {
        ResolutionResult::CorroboratedWinner { winner_id, .. }
        | ResolutionResult::SingleWinner { winner_id, .. } => Some(winner_id.clone()),
        _ => None,
    };
    let mut merged_ids: HashSet<String> = input.merged_claim_ids.iter().cloned().collect();
    if let ResolutionResult::CorroboratedWinner {
        merged_claim_ids, ..
    } = resolution
    {
        merged_ids.extend(merged_claim_ids.iter().cloned());
    }
    merged_ids.retain(|id| claim_records.contains_key(id));
    if let Some(winner_id) = merge_winner_id.as_ref() {
        merged_ids.remove(winner_id);
        if !merged_ids.is_empty() {
            let incoming: Vec<EvidenceRef> = merged_ids
                .iter()
                .filter_map(|id| claim_records.get(id))
                .flat_map(|record| record.evidence().iter().cloned())
                .collect();
            if let Some(target) = claim_records.get_mut(winner_id) {
                let mut seen: HashSet<String> = target
                    .evidence()
                    .iter()
                    .map(evidence_identity_key)
                    .collect();
                for evidence in incoming {
                    if seen.insert(evidence_identity_key(&evidence)) {
                        target.evidence_mut().push(evidence);
                    }
                }
            }
        }
    } else {
        // Without a single winner there is no merge target.
        merged_ids.clear();
    }

    let mut claim_snapshots: HashMap<String, ClaimSnapshot> = HashMap::new();
    for (claim_id, record) in &claim_records {
        claim_snapshots.insert(claim_id.clone(), record.snapshot());
    }

    let active_claim_ids: Vec<String> = claim_ids
        .iter()
        .filter(|id| !merged_ids.contains(*id))
        .cloned()
        .collect();

    let mut winner_ids: HashSet<String> = HashSet::new();
    let mut confidence_overrides: HashMap<String, f32> = HashMap::new();
    let mut current_value_order: Vec<String> = Vec::new();
    let mut rationale = match resolution {
        ResolutionResult::SingleWinner {
            winner_id,
            confidence,
            rationale,
        } => {
            let effective_winner_id = if claim_records.contains_key(winner_id) {
                winner_id.clone()
            } else if claim_records.len() == 1 {
                if let Some(id) = claim_records.keys().next().cloned() {
                    id
                } else {
                    return Ok(CommitResult::default());
                }
            } else {
                return Err(PipelineError::NotFound(format!(
                    "Winner claim not found: {winner_id}"
                )));
            };
            winner_ids.insert(effective_winner_id.clone());
            confidence_overrides.insert(effective_winner_id.clone(), *confidence);
            current_value_order.push(effective_winner_id);
            Some(rationale.clone())
        }
        ResolutionResult::CorroboratedWinner {
            winner_id,
            confidence,
            rationale,
            ..
        } => {
            let effective_winner_id = if claim_records.contains_key(winner_id) {
                winner_id.clone()
            } else if let (1, Some(id)) =
                (claim_records.len(), claim_records.keys().next().cloned())
            {
                id
            } else {
                return Err(PipelineError::NotFound(format!(
                    "Winner claim not found: {winner_id}"
                )));
            };
            winner_ids.insert(effective_winner_id.clone());
            confidence_overrides.insert(effective_winner_id.clone(), *confidence);
            current_value_order.push(effective_winner_id);
            Some(rationale.clone())
        }
        ResolutionResult::ParallelValues {
            ranked_values,
            rationale,
        } => {
            for (id, conf) in ranked_values {
                if !claim_records.contains_key(id) {
                    tracing::debug!(
                        claim_id = %id,
                        "tm_commit skipped missing parallel claim after cleanup"
                    );
                    continue;
                }
                winner_ids.insert(id.clone());
                confidence_overrides.insert(id.clone(), *conf);
                current_value_order.push(id.clone());
            }
            if current_value_order.is_empty() {
                return Ok(CommitResult::default());
            }
            Some(rationale.clone())
        }
        ResolutionResult::SplitByContext {
            context_winners,
            rationale,
        } => {
            let mut ids: Vec<String> = Vec::new();
            for id in context_winners.values() {
                if !claim_records.contains_key(id) {
                    tracing::debug!(
                        claim_id = %id,
                        "tm_commit skipped missing context claim after cleanup"
                    );
                    continue;
                }
                winner_ids.insert(id.clone());
                ids.push(id.clone());
            }
            ids.sort();
            current_value_order = ids;
            if current_value_order.is_empty() {
                return Ok(CommitResult::default());
            }
            Some(rationale.clone())
        }
        ResolutionResult::NeedsReview { reason } => {
            current_value_order = claim_records.keys().cloned().collect();
            current_value_order.sort();
            Some(reason.clone())
        }
    };

    current_value_order.retain(|id| !merged_ids.contains(id));

    let mut needs_review = matches!(resolution, ResolutionResult::NeedsReview { .. });
    if needs_review {
        order_needs_review_claims(&mut current_value_order, &claim_records, conflict_policy);
    }

    let mut enforce_supersede_implicit = false;
    if !needs_review
        && refinement_policy == RefinementPolicy::SupersedeImplicit
        && !explicit_ids.is_empty()
        && !implicit_ids.is_empty()
    {
        enforce_supersede_implicit = true;
        let mut explicit_order: Vec<String> = current_value_order
            .iter()
            .filter(|id| explicit_ids.contains(*id))
            .cloned()
            .collect();
        if explicit_order.is_empty() {
            explicit_order = explicit_ids.iter().cloned().collect();
            explicit_order.sort_by(|a, b| {
                let a_conf = claim_records
                    .get(a)
                    .map(|record| record.confidence())
                    .unwrap_or(0.0);
                let b_conf = claim_records
                    .get(b)
                    .map(|record| record.confidence())
                    .unwrap_or(0.0);
                b_conf
                    .partial_cmp(&a_conf)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        if !explicit_order.is_empty() {
            match resolution {
                ResolutionResult::SingleWinner { .. } => {
                    let winner_id = explicit_order[0].clone();
                    winner_ids.clear();
                    winner_ids.insert(winner_id.clone());
                    current_value_order = vec![winner_id];
                }
                _ => {
                    winner_ids = explicit_order.iter().cloned().collect();
                    current_value_order = explicit_order;
                }
            }
        }
    }

    let promotion_blocked = if needs_review {
        false
    } else {
        winner_ids.iter().any(|winner_id| {
            let Some(snapshot) = claim_snapshots.get(winner_id) else {
                return true;
            };
            let effective_confidence = confidence_overrides
                .get(winner_id)
                .copied()
                .unwrap_or(snapshot.confidence);
            effective_confidence < thresholds.min_confidence
                || snapshot.evidence.len() < thresholds.min_evidence
        })
    };

    if promotion_blocked {
        needs_review = true;
        current_value_order = active_claim_ids.clone();
        order_needs_review_claims(&mut current_value_order, &claim_records, conflict_policy);
        rationale = Some(format!(
            "Promotion thresholds not met (min_confidence={:.3}, min_evidence={})",
            thresholds.min_confidence, thresholds.min_evidence
        ));
    }

    let mut status_map: HashMap<String, ClaimStatus> = HashMap::new();
    if needs_review {
        for id in &active_claim_ids {
            status_map.insert(id.clone(), ClaimStatus::Disputed);
        }
    } else {
        for id in &active_claim_ids {
            let status = if winner_ids.contains(id) {
                ClaimStatus::Approved
            } else {
                ClaimStatus::Superseded
            };
            status_map.insert(id.clone(), status);
        }
    }

    if enforce_supersede_implicit && !needs_review {
        for id in &implicit_ids {
            if !merged_ids.contains(id) {
                status_map.insert(id.clone(), ClaimStatus::Superseded);
            }
        }
    }

    // Resurrect guard: a value that an earlier adjudication superseded (closed
    // validity window, no approved duplicate left) must not be silently
    // re-approved by a deterministic commit or recompute. Corroboration over a
    // group that still contains an Approved claim is the self-heal path and
    // stays unaffected.
    let mut preserved_ids: HashSet<String> = HashSet::new();
    if !input.adjudicated && !needs_review {
        let live_active: Vec<&String> = active_claim_ids
            .iter()
            .filter(|id| claim_records.contains_key(*id))
            .collect();
        if let [id] = live_active.as_slice() {
            let id = (*id).clone();
            if status_map.get(&id) == Some(&ClaimStatus::Approved) {
                if let Some(record) = claim_records.get(&id) {
                    let closed_window = record
                        .validity_window()
                        .and_then(|window| window.end)
                        .is_some();
                    let merged_all_superseded = merged_ids.iter().all(|merged_id| {
                        claim_records
                            .get(merged_id)
                            .is_none_or(|merged| merged.status() == ClaimStatus::Superseded)
                    });
                    if record.status() == ClaimStatus::Superseded
                        && closed_window
                        && merged_all_superseded
                    {
                        status_map.insert(id.clone(), ClaimStatus::Superseded);
                        preserved_ids.insert(id);
                    }
                }
            }
        }
    }

    let corroborated = matches!(resolution, ResolutionResult::CorroboratedWinner { .. });
    // Re-open the winner's window for corroboration (D11 self-heal) and for
    // adjudicated single winners; deterministic paths never re-open.
    let reopen_winner = corroborated
        || (input.adjudicated && matches!(resolution, ResolutionResult::SingleWinner { .. }));

    let winner_time = if !needs_review && winner_ids.len() == 1 {
        let winner_id = winner_ids
            .iter()
            .next()
            .ok_or_else(|| PipelineError::NotFound("Winner claim not found".to_string()))?;
        let record = claim_records.get(winner_id).ok_or_else(|| {
            PipelineError::NotFound(format!("Winner claim not found: {winner_id}"))
        })?;
        Some(claim_timestamp_for_semantics(record, semantics))
    } else {
        None
    };

    let mut audit_entries = 0;

    // Persist surviving claims (the winner with its merged evidence above all)
    // BEFORE soft-deleting duplicates: the evidence merge is idempotent, so a
    // crash between the two phases retries cleanly, while the reverse order
    // would trip the staleness guard with the duplicate's evidence unpersisted.
    let ordered_claim_ids: Vec<&String> = claim_ids
        .iter()
        .filter(|id| !merged_ids.contains(*id))
        .chain(claim_ids.iter().filter(|id| merged_ids.contains(*id)))
        .collect();

    for claim_id in ordered_claim_ids {
        let Some(mut record) = claim_records.remove(claim_id) else {
            continue;
        };
        let before_snapshot = match &record {
            ClaimRecord::Rel(rel) => audit_snapshot(rel)?,
            ClaimRecord::Attribute(attr) => audit_snapshot(attr)?,
        };

        // Corroborated duplicates: evidence already merged into the winner;
        // soft-delete the row (no status change, no validity windows).
        if merged_ids.contains(claim_id) {
            let record_id = record.id().clone();
            match &record {
                ClaimRecord::Rel(_) => storage.delete_rel(space_id, &record_id).await?,
                ClaimRecord::Attribute(_) => {
                    storage.delete_attribute(space_id, &record_id).await?;
                }
            }
            record.soft_delete();
            let after_snapshot = match &record {
                ClaimRecord::Rel(rel) => audit_snapshot(rel)?,
                ClaimRecord::Attribute(attr) => audit_snapshot(attr)?,
            };
            let audit = build_audit_log(
                space_id,
                TM_COMMIT_KIND,
                AuditAction::Update,
                &record_id,
                Some(before_snapshot),
                Some(after_snapshot),
                Some("tm_commit: corroborated duplicate merged".to_string()),
            );
            storage.create_audit_log(audit).await?;
            audit_entries += 1;
            continue;
        }

        let status = *status_map.get(claim_id).unwrap_or(&ClaimStatus::Candidate);
        record.set_status(status);

        let override_end = input
            .superseded_overrides
            .iter()
            .find(|entry| entry.claim_id == *claim_id)
            .and_then(|entry| entry.valid_until);

        if preserved_ids.contains(claim_id) {
            // Keep the previously adjudicated supersession window untouched.
        } else if status == ClaimStatus::Superseded {
            if let Some(end_time) = override_end {
                apply_supersession_override(&mut record, end_time, semantics);
            } else if let Some(end_time) = input.winner_valid_from {
                // The verdict-stated winner start beats ingest-derived
                // timestamps for closing losers (temporal fidelity, D4).
                apply_supersession_override(&mut record, end_time, semantics);
            } else if let Some(time) = winner_time {
                apply_supersession_window(&mut record, time, semantics)?;
            }
        } else if status == ClaimStatus::Approved && winner_ids.contains(claim_id) {
            if let Some(start_override) = input.winner_valid_from {
                force_winner_window_start(&mut record, start_override);
            } else if let Some(time) = winner_time {
                apply_winner_window(&mut record, time);
            }
            if reopen_winner {
                if let Some(mut window) = record.validity_window() {
                    window.end = None;
                    record.set_validity_window(Some(window));
                }
            }
        }

        match record {
            ClaimRecord::Rel(mut rel) => {
                rel.base.updated_at = Utc::now();
                let rel_id = rel.base.id.clone();
                let updated = storage.update_rel(space_id, &rel_id, rel).await?;
                let after_snapshot = audit_snapshot(&updated)?;
                let audit = build_audit_log(
                    space_id,
                    TM_COMMIT_KIND,
                    AuditAction::Update,
                    &rel_id,
                    Some(before_snapshot),
                    Some(after_snapshot),
                    Some(TM_COMMIT_KIND.to_string()),
                );
                storage.create_audit_log(audit).await?;
                audit_entries += 1;
            }
            ClaimRecord::Attribute(mut attr) => {
                attr.base.updated_at = Utc::now();
                let attr_id = attr.base.id.clone();
                let updated = storage.update_attribute(space_id, &attr_id, attr).await?;
                let after_snapshot = audit_snapshot(&updated)?;
                let audit = build_audit_log(
                    space_id,
                    TM_COMMIT_KIND,
                    AuditAction::Update,
                    &attr_id,
                    Some(before_snapshot),
                    Some(after_snapshot),
                    Some(TM_COMMIT_KIND.to_string()),
                );
                storage.create_audit_log(audit).await?;
                audit_entries += 1;
            }
        }
    }

    if !needs_review {
        if let Some(group) = unresolved_group.clone() {
            let before_snapshot = audit_snapshot(&group)?;
            let mut updated_group = group;
            updated_group.base.updated_at = Utc::now();
            updated_group.resolved = true;
            updated_group.resolution_rationale = rationale.clone();
            updated_group.resolved_at = Some(Utc::now());
            // Memoize the decided claim set so detection/recompute skip
            // unchanged sets even for deterministic (keyless) resolutions.
            if input.claim_set_hash.is_some() {
                updated_group.adjudicated_claim_set_hash = input.claim_set_hash.clone();
                updated_group.adjudicated_at = Some(Utc::now());
            }
            let group_id = updated_group.base.id.clone();
            let updated = storage
                .update_conflict_group(space_id, &group_id, updated_group)
                .await?;
            let after_snapshot = audit_snapshot(&updated)?;
            let audit = build_audit_log(
                space_id,
                TM_COMMIT_KIND,
                AuditAction::Update,
                &group_id,
                Some(before_snapshot),
                Some(after_snapshot),
                Some(TM_COMMIT_KIND.to_string()),
            );
            storage.create_audit_log(audit).await?;
            audit_entries += 1;
        }
    }

    let mut current_values = Vec::new();
    for claim_id in &current_value_order {
        if let Some(snapshot) = claim_snapshots.get(claim_id) {
            let status = *status_map.get(claim_id).unwrap_or(&ClaimStatus::Candidate);
            let confidence = confidence_overrides
                .get(claim_id)
                .copied()
                .unwrap_or(snapshot.confidence);
            current_values.push(CurrentValue {
                claim_id: claim_id.clone(),
                confidence,
                status,
                evidence: snapshot.evidence.clone(),
            });
        }
    }

    let confidence = current_values
        .iter()
        .map(|value| value.confidence)
        .fold(0.0, f32::max);

    let conflict_group = if needs_review {
        unresolved_group
            .as_ref()
            .map(|group| Link::new(group.base.id.clone()))
    } else {
        None
    };

    // A rel_set verdict changes per-object truth: mark those views stale
    // BEFORE the final slot upsert so the rel_set slot itself lands with
    // stale=false (single writer per work item makes the order swap safe).
    if input.slot_type == "rel_set" {
        match RecordId::from_str(&input.subject_entity) {
            Ok(subject_id) => {
                storage
                    .mark_slots_stale_by_entity(space_id, &subject_id, "rel_set_committed")
                    .await?;
            }
            Err(err) => {
                tracing::warn!(
                    subject = %input.subject_entity,
                    error = %err,
                    "rel_set commit could not mark per-object slots stale"
                );
            }
        }
    }

    let existing_slot = storage.find_slot_by_key(space_id, &input.slot_key).await?;
    let (before_snapshot, audit_action, mut slot) = if let Some(mut existing) = existing_slot {
        let before = audit_snapshot(&existing)?;
        existing.base.updated_at = Utc::now();
        existing.current_values = current_values;
        existing.confidence = confidence;
        existing.rationale = rationale;
        existing.conflict_group = conflict_group;
        existing.stale = false;
        existing.stale_reason = None;
        existing.computed_at = Utc::now();
        existing.semantics_version = semantics_version;
        existing.alias_version = alias_version;
        (Some(before), AuditAction::Update, existing)
    } else {
        let id = truth_slot_id(space_id, &input.slot_key);
        let base = BaseFields::new(id, space_id.to_string(), "system".to_string());
        let slot = TruthSlot {
            base,
            slot_key: input.slot_key.clone(),
            slot_type: input.slot_type.clone(),
            subject_entity: input.subject_entity.clone(),
            predicate_id: input.predicate_id.clone(),
            current_values,
            conflict_group,
            confidence,
            rationale,
            semantics_version,
            alias_version,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: input.security_hash.clone(),
        };
        (None, AuditAction::Create, slot)
    };

    slot.base.updated_at = Utc::now();
    let upserted = storage.upsert_slot(slot).await?;
    let after_snapshot = audit_snapshot(&upserted)?;
    let audit = build_audit_log(
        space_id,
        TM_COMMIT_KIND,
        audit_action,
        &upserted.base.id,
        before_snapshot,
        Some(after_snapshot),
        Some(TM_COMMIT_KIND.to_string()),
    );
    storage.create_audit_log(audit).await?;
    audit_entries += 1;
    slot_ids.push(upserted.base.id);
    slots_updated += 1;

    Ok(CommitResult {
        slots_updated,
        audit_entries,
        slot_ids,
        skipped_stale: false,
    })
}

/// Forces the winner's validity window to start at a verdict-supplied date.
fn force_winner_window_start(record: &mut ClaimRecord, start_time: DateTime<Utc>) {
    let mut window = record
        .validity_window()
        .unwrap_or(RelValidityWindow::starting_at(start_time));
    window.start = Some(start_time);
    record.set_validity_window(Some(window));
}

/// Closes a loser's validity window at a verdict-supplied date.
fn apply_supersession_override(
    record: &mut ClaimRecord,
    end_time: DateTime<Utc>,
    semantics: Option<&PredicateSemantics>,
) {
    let start_time = record
        .validity_window()
        .and_then(|window| window.start)
        .unwrap_or_else(|| claim_timestamp_for_semantics(record, semantics));
    let mut window = record
        .validity_window()
        .unwrap_or(RelValidityWindow::starting_at(start_time.min(end_time)));
    if window.start.is_some_and(|start| start > end_time) {
        window.start = Some(end_time);
    }
    window.end = Some(end_time);
    record.set_validity_window(Some(window));
}

/// Identity key deduping merged evidence refs (assertion + span).
fn evidence_identity_key(evidence: &EvidenceRef) -> String {
    format!(
        "{}:{}:{}:{}",
        evidence.assertion.id(),
        evidence.span.revision.id(),
        evidence.span.start,
        evidence.span.end,
    )
}

fn order_needs_review_claims(
    claim_ids: &mut [String],
    claim_records: &HashMap<String, ClaimRecord>,
    conflict_policy: ConflictPolicy,
) {
    if conflict_policy == ConflictPolicy::RequireMoreEvidence {
        claim_ids.sort_by(|a, b| {
            let a_conf = claim_records
                .get(a)
                .map(|record| record.confidence())
                .unwrap_or(0.0);
            let b_conf = claim_records
                .get(b)
                .map(|record| record.confidence())
                .unwrap_or(0.0);
            b_conf
                .partial_cmp(&a_conf)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(b))
        });
    } else {
        claim_ids.sort();
    }
}

/// Populates current value in a truth slot.
pub fn populate_current_value(claim_id: &str, confidence: f32) -> CurrentValue {
    use elephant_types::records::rel::ClaimStatus;

    CurrentValue {
        claim_id: claim_id.to_string(),
        confidence,
        status: ClaimStatus::Approved,
        evidence: vec![],
    }
}

/// Marks a truth slot as stale.
pub fn mark_slot_stale(slot: &mut TruthSlot, reason: &str) {
    slot.stale = true;
    slot.stale_reason = Some(reason.to_string());
}

/// Checks if a truth slot is stale.
pub fn is_slot_stale(slot: &TruthSlot) -> bool {
    slot.stale
}

fn claim_timestamp_for_semantics(
    record: &ClaimRecord,
    semantics: Option<&PredicateSemantics>,
) -> DateTime<Utc> {
    claim_timestamp_and_basis_for_semantics(record, semantics).0
}

const fn time_basis_label(basis: TimeBasis) -> &'static str {
    match basis {
        TimeBasis::SourceEventTime => "source_event_time",
        TimeBasis::ObservedAt => "observed_at",
        TimeBasis::AssertedAt => "asserted_at",
    }
}

/// Resolves a claim's recency timestamp together with the basis that actually
/// supplied it — the configured basis can be absent on a given claim, so the
/// label must reflect the fallback that fired, not the configuration.
fn claim_timestamp_and_basis_for_semantics(
    record: &ClaimRecord,
    semantics: Option<&PredicateSemantics>,
) -> (DateTime<Utc>, &'static str) {
    let evidence = record.evidence();
    if let Some(semantics) = semantics {
        let mut order = vec![semantics.recency_basis];
        for basis in &semantics.fallback_order {
            if !order.contains(basis) {
                order.push(*basis);
            }
        }

        for basis in order {
            let time_basis = time_basis_from_recency(basis);
            let candidate = evidence
                .iter()
                .filter(|e| e.time_basis == time_basis)
                .map(|e| e.time_value)
                .max();
            if let Some(ts) = candidate {
                return (ts, time_basis_label(time_basis));
            }
        }
    }

    evidence.iter().max_by_key(|e| e.time_value).map_or_else(
        || (record.created_at(), "record_created_at"),
        |e| (e.time_value, time_basis_label(e.time_basis)),
    )
}

/// Builds the claim value for a record; rel object displays come from the
/// provided map and degrade to the object id string when absent.
fn claim_value_for_record(
    record: &ClaimRecord,
    entity_displays: &HashMap<String, String>,
) -> ClaimValue {
    match record {
        ClaimRecord::Rel(rel) => {
            let id = rel.object.id().to_string();
            let display = entity_displays
                .get(&id)
                .cloned()
                .unwrap_or_else(|| id.clone());
            ClaimValue::EntityRef { id, display }
        }
        ClaimRecord::Attribute(attr) => ClaimValue::Literal(attr.value.clone()),
    }
}

fn build_claim_metadata(
    record: &ClaimRecord,
    semantics: Option<&PredicateSemantics>,
    entity_displays: &HashMap<String, String>,
) -> ClaimWithMetadata {
    let evidence_sources = record
        .evidence()
        .iter()
        .map(|e| e.assertion.id().to_string())
        .collect();

    let (timestamp, time_basis) = claim_timestamp_and_basis_for_semantics(record, semantics);
    ClaimWithMetadata {
        claim_id: record.id().to_string(),
        value: claim_value_for_record(record, entity_displays),
        confidence: record.confidence(),
        timestamp,
        time_basis,
        context: None,
        evidence_count: record.evidence().len(),
        evidence_sources,
        status_weight: 0,
        updated_at: record.created_at(),
    }
}

// ============================================================================
// Outbox Triggers
// ============================================================================

/// Creates a trigger event from an assertion.
pub fn trigger_from_assertion(assertion_id: RecordId) -> TriggerEvent {
    TriggerEvent::Assertion(assertion_id)
}

/// Creates a trigger event from a relationship.
pub fn trigger_from_rel(rel_id: RecordId) -> TriggerEvent {
    TriggerEvent::Rel(rel_id)
}

/// Creates a trigger event from an attribute.
pub fn trigger_from_attribute(attr_id: RecordId) -> TriggerEvent {
    TriggerEvent::Attribute(attr_id)
}

/// Creates a trigger event from an event.
pub fn trigger_from_event(event_id: RecordId) -> TriggerEvent {
    TriggerEvent::Event(event_id)
}

/// Creates a trigger event from an entity.
pub fn trigger_from_entity(entity_id: RecordId) -> TriggerEvent {
    TriggerEvent::Entity(entity_id)
}

/// Creates a trigger event from a predicate.
pub fn trigger_from_predicate(predicate_id: RecordId) -> TriggerEvent {
    TriggerEvent::Predicate(predicate_id)
}

/// Creates a trigger event from a revision.
pub fn trigger_from_revision(revision_id: RecordId) -> TriggerEvent {
    TriggerEvent::Revision(revision_id)
}

fn trigger_from_record_ref(record: &RecordRef) -> Option<TriggerEvent> {
    match record.table.as_str() {
        "rel" => Some(TriggerEvent::Rel(record.id.clone())),
        "attribute" => Some(TriggerEvent::Attribute(record.id.clone())),
        "event" => Some(trigger_from_event(record.id.clone())),
        "assertion" => Some(TriggerEvent::Assertion(record.id.clone())),
        "doc_revision" => Some(TriggerEvent::Revision(record.id.clone())),
        "entity" | "entity_identity_key" | "entity_merge" => {
            Some(TriggerEvent::Entity(record.id.clone()))
        }
        "predicate_registry" | "predicate_alias" => {
            Some(TriggerEvent::Predicate(record.id.clone()))
        }
        _ => None,
    }
}

/// Builds trigger events from an outbox payload.
#[must_use]
pub fn trigger_events_from_outbox_payload(payload: &OutboxPayload) -> Vec<TriggerEvent> {
    payload
        .records
        .iter()
        .filter_map(trigger_from_record_ref)
        .collect()
}

/// Builds trigger events from a system event.
///
/// # Errors
///
/// Returns an error if the payload cannot be parsed or has a mismatched space_id.
pub fn trigger_events_from_system_event(
    event: &SystemEvent,
) -> Result<(String, Vec<TriggerEvent>), PipelineError> {
    let Some(payload_object) = event.payload.as_object() else {
        return Ok((event.base.space_id.clone(), Vec::new()));
    };

    if !payload_object.contains_key("records") {
        return Ok((event.base.space_id.clone(), Vec::new()));
    }

    let mut payload_value = event.payload.clone();
    if let Some(payload_object) = payload_value.as_object_mut() {
        payload_object
            .entry("space_id".to_string())
            .or_insert_with(|| serde_json::Value::String(event.base.space_id.clone()));
        payload_object
            .entry("event_type".to_string())
            .or_insert_with(|| serde_json::Value::String(event.event_type.clone()));
        payload_object
            .entry("actor".to_string())
            .or_insert_with(|| serde_json::Value::String(event.base.created_by.clone()));
    }

    let payload: OutboxPayload = serde_json::from_value(payload_value)
        .map_err(|e| PipelineError::InvalidInput(format!("Invalid outbox payload: {e}")))?;

    if payload.space_id != event.base.space_id {
        return Err(PipelineError::InvalidInput(format!(
            "Outbox payload space_id {} does not match event space_id {}",
            payload.space_id, event.base.space_id
        )));
    }

    Ok((
        payload.space_id.clone(),
        trigger_events_from_outbox_payload(&payload),
    ))
}

/// Executes tm_detect_conflicts from outbox events.
///
/// This is the event-driven entrypoint per spec §12.6.
///
/// # Errors
///
/// Returns an error if payload parsing or storage operations fail.
pub async fn execute_tm_detect_conflicts_from_outbox_events<S: TruthMaintenanceStorage>(
    events: &[SystemEvent],
    storage: &S,
) -> Result<DetectConflictsResult, PipelineError> {
    let mut triggers_by_space: HashMap<String, Vec<TriggerEvent>> = HashMap::new();
    for event in events {
        let (space_id, triggers) = trigger_events_from_system_event(event)?;
        if triggers.is_empty() {
            continue;
        }
        triggers_by_space
            .entry(space_id)
            .or_default()
            .extend(triggers);
    }

    let mut aggregated = DetectConflictsResult {
        events_processed: 0,
        conflicts_detected: 0,
        conflicts_updated: 0,
        conflict_group_ids: Vec::new(),
        direct_commits: Vec::new(),
    };

    for (space_id, triggers) in triggers_by_space {
        let result = execute_tm_detect_conflicts(&triggers, &space_id, storage).await?;
        aggregated.events_processed += result.events_processed;
        aggregated.conflicts_detected += result.conflicts_detected;
        aggregated.conflicts_updated += result.conflicts_updated;
        aggregated
            .conflict_group_ids
            .extend(result.conflict_group_ids);
        aggregated.direct_commits.extend(result.direct_commits);
    }

    Ok(aggregated)
}

// ============================================================================
// Recompute
// ============================================================================

/// Work kind identifier for tm_recompute.
pub const TM_RECOMPUTE_KIND: &str = "tm_recompute";

/// Priority level for recompute jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecomputePriority {
    /// Normal background recompute.
    Normal,
    /// High priority for freshness=required queries.
    High,
    /// Urgent - blocking a user query.
    Urgent,
}

/// Result of tm_recompute work kind.
#[derive(Debug, Clone)]
pub struct RecomputeResult {
    /// Number of stale slots found.
    pub stale_slots_found: usize,
    /// Number of slots recomputed.
    pub slots_recomputed: usize,
    /// Number of slots skipped (budget limit).
    pub slots_skipped: usize,
    /// Conflict groups needing adjudication via enqueued tm_resolve_slot items.
    pub pending_conflict_groups: Vec<RecordId>,
}

/// Executes the tm_recompute work kind.
///
/// Single-value-group slots commit inline (deterministic, fast). Multi-group
/// stale slots get their conflict group upserted and returned in
/// `pending_conflict_groups` for the caller to enqueue as `tm_resolve_slot`
/// items — no inline LLM calls inside the recompute lease.
///
/// # Errors
///
/// Returns an error if the recompute operation fails.
pub async fn execute_tm_recompute<S: TruthMaintenanceStorage>(
    _ctx: &TmContext,
    space_id: &str,
    priority: RecomputePriority,
    max_slots: usize,
    storage: &S,
) -> Result<RecomputeResult, PipelineError> {
    let _ = priority;
    let stale_slots = storage.find_stale_slots(space_id, max_slots).await?;
    let stale_slots_found = stale_slots.len();

    let mut slots_recomputed = 0;
    let mut slots_skipped = 0;
    let mut pending_conflict_groups: Vec<RecordId> = Vec::new();

    for slot in stale_slots {
        let parts = parse_slot_key(&slot.slot_key, &slot.slot_type)?;
        let predicate = storage
            .find_predicate_registry_by_predicate_id(space_id, &slot.predicate_id)
            .await?;
        let semantics = predicate.as_ref().and_then(|p| p.semantics.as_ref());

        let claim_records: Vec<ClaimRecord> = match slot.slot_type.as_str() {
            "rel" => {
                let Some(object_id) = parts.object_id else {
                    slots_skipped += 1;
                    continue;
                };
                let rels = storage
                    .find_rels_for_slot(space_id, &parts.subject_id, &object_id, &slot.predicate_id)
                    .await?
                    .into_iter()
                    .filter(|rel| compute_security_hash(&rel.base.security) == parts.security_hash)
                    .collect::<Vec<_>>();
                rels.into_iter().map(ClaimRecord::Rel).collect()
            }
            "rel_set" => storage
                .find_rels_for_subject_predicate(space_id, &parts.subject_id, &slot.predicate_id)
                .await?
                .into_iter()
                .filter(|rel| compute_security_hash(&rel.base.security) == parts.security_hash)
                .map(ClaimRecord::Rel)
                .collect(),
            "attribute" => {
                let attrs = storage
                    .find_attrs_for_slot(space_id, &parts.subject_id, &slot.predicate_id)
                    .await?
                    .into_iter()
                    .filter(|attr| {
                        compute_security_hash(&attr.base.security) == parts.security_hash
                    })
                    .collect::<Vec<_>>();
                attrs.into_iter().map(ClaimRecord::Attribute).collect()
            }
            _ => {
                slots_skipped += 1;
                continue;
            }
        };

        if claim_records.is_empty() {
            slots_skipped += 1;
            continue;
        }

        let claim_ids: Vec<String> = claim_records
            .iter()
            .map(|record| record.id().to_string())
            .collect();

        let entity_displays = HashMap::new();
        let claims: Vec<ClaimWithMetadata> = claim_records
            .iter()
            .map(|record| build_claim_metadata(record, semantics, &entity_displays))
            .collect();
        let values: Vec<ClaimValue> = claims.iter().map(|claim| claim.value.clone()).collect();

        if partition_value_groups(&values).len() == 1 {
            // One value group: inline corroboration commit (resurrect guard
            // in commit preserves adjudicated supersessions on lone claims).
            let commit = build_corroboration_commit(
                &slot.slot_key,
                &slot.slot_type,
                &parts.subject_id,
                &slot.predicate_id,
                &slot.security_hash,
                &claims,
            );
            let _ = execute_tm_commit_with_storage(
                &commit.resolution,
                &commit.input,
                space_id,
                storage,
            )
            .await?;
            slots_recomputed += 1;
            continue;
        }

        // Multiple value groups: hand the conflict group to tm_resolve_slot.
        match upsert_conflict_group_memoized(
            space_id,
            &slot.slot_key,
            &slot.slot_type,
            claim_ids,
            storage,
        )
        .await?
        {
            SlotDetection::Memoized => {
                // The committed truth already covers this exact claim set;
                // just clear the stale flag.
                let mut refreshed = slot;
                refreshed.stale = false;
                refreshed.stale_reason = None;
                refreshed.computed_at = Utc::now();
                refreshed.base.updated_at = Utc::now();
                let slot_id = refreshed.base.id.clone();
                let _ = storage.update_slot(space_id, &slot_id, refreshed).await?;
                slots_recomputed += 1;
            }
            SlotDetection::Conflict { id, .. } => {
                pending_conflict_groups.push(id);
            }
            SlotDetection::Corroborated(_) => {
                // Unreachable: multi-group slots never corroborate here.
            }
        }
    }

    Ok(RecomputeResult {
        stale_slots_found,
        slots_recomputed,
        slots_skipped,
        pending_conflict_groups,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use chrono::{TimeZone, Utc};
    use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
    use elephant_storage::{
        ConflictGroupRepository, RelRepository, Repository, StorageClient, TruthSlotRepository,
    };
    use elephant_types::ids::TableName;
    use elephant_types::records::attribute::Literal;
    use elephant_types::records::base::BaseFields;
    use elephant_types::Link;
    use std::str::FromStr;
    use std::sync::Mutex;

    // =========================================================================
    // Test Fixtures
    // =========================================================================

    /// Distinct per-claim value so policy tests exercise real value conflicts.
    fn distinct_value(id: &str) -> ClaimValue {
        ClaimValue::Literal(Literal::String(id.to_string()))
    }

    fn make_claim(id: &str, confidence: f32, timestamp: DateTime<Utc>) -> ClaimWithMetadata {
        ClaimWithMetadata {
            claim_id: id.to_string(),
            value: distinct_value(id),
            confidence,
            timestamp,
            time_basis: "source_event_time",
            context: None,
            evidence_count: 1,
            evidence_sources: Vec::new(),
            status_weight: 0,
            updated_at: timestamp,
        }
    }

    fn make_claim_with_value(id: &str, confidence: f32, value: ClaimValue) -> ClaimWithMetadata {
        ClaimWithMetadata {
            value,
            ..make_claim(id, confidence, Utc::now())
        }
    }

    fn make_claim_with_context(id: &str, confidence: f32, context: &str) -> ClaimWithMetadata {
        ClaimWithMetadata {
            claim_id: id.to_string(),
            value: distinct_value(id),
            confidence,
            timestamp: Utc::now(),
            time_basis: "source_event_time",
            context: Some(context.to_string()),
            evidence_count: 1,
            evidence_sources: Vec::new(),
            status_weight: 0,
            updated_at: Utc::now(),
        }
    }

    fn make_claim_with_evidence(
        id: &str,
        confidence: f32,
        evidence_count: usize,
    ) -> ClaimWithMetadata {
        ClaimWithMetadata {
            claim_id: id.to_string(),
            value: distinct_value(id),
            confidence,
            timestamp: Utc::now(),
            time_basis: "source_event_time",
            context: None,
            evidence_count,
            evidence_sources: Vec::new(),
            status_weight: 0,
            updated_at: Utc::now(),
        }
    }

    fn make_claim_with_sources(
        id: &str,
        confidence: f32,
        evidence_count: usize,
        sources: &[&str],
    ) -> ClaimWithMetadata {
        ClaimWithMetadata {
            claim_id: id.to_string(),
            value: distinct_value(id),
            confidence,
            timestamp: Utc::now(),
            time_basis: "source_event_time",
            context: None,
            evidence_count,
            evidence_sources: sources.iter().map(|s| (*s).to_string()).collect(),
            status_weight: 0,
            updated_at: Utc::now(),
        }
    }

    fn make_conflict_group(slot_key: &str, claim_ids: Vec<&str>) -> ConflictGroup {
        create_conflict_group(
            slot_key,
            "rel",
            claim_ids.into_iter().map(String::from).collect(),
            "test_space",
        )
    }

    /// Scripted adjudicator that records every input it sees.
    #[derive(Debug)]
    struct FakeAdjudicator {
        verdict: Option<AdjudicationVerdict>,
        calls: Mutex<Vec<AdjudicationInput>>,
    }

    impl FakeAdjudicator {
        fn new(verdict: Option<AdjudicationVerdict>) -> Self {
            Self {
                verdict,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        fn last_input(&self) -> Option<AdjudicationInput> {
            self.calls.lock().unwrap().last().cloned()
        }
    }

    #[async_trait]
    impl SlotAdjudicator for FakeAdjudicator {
        async fn adjudicate(
            &self,
            input: &AdjudicationInput,
        ) -> Result<Option<AdjudicationVerdict>, PipelineError> {
            self.calls.lock().unwrap().push(input.clone());
            Ok(self.verdict.clone())
        }

        fn id(&self) -> &'static str {
            "fake"
        }
    }

    fn ctx_with(adjudicator: &Arc<FakeAdjudicator>) -> TmContext {
        TmContext {
            adjudicator: Some(Arc::clone(adjudicator) as Arc<dyn SlotAdjudicator>),
        }
    }

    fn verdict(decision: AdjudicationDecision) -> AdjudicationVerdict {
        AdjudicationVerdict {
            decision,
            winner_id: None,
            equivalent_groups: Vec::new(),
            superseded: Vec::new(),
            winner_valid_from: None,
            proposed_semantics: None,
            rationale: "fake verdict".to_string(),
        }
    }

    const TM_SCHEMA: &str = r"
        DEFINE TABLE rel SCHEMALESS;
        DEFINE TABLE attribute SCHEMALESS;
        DEFINE TABLE event SCHEMALESS;
        DEFINE TABLE conflict_group SCHEMALESS;
        DEFINE TABLE truth_slot SCHEMALESS;
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE predicate_registry SCHEMALESS;
        DEFINE TABLE predicate_alias SCHEMALESS;
        DEFINE TABLE audit_log SCHEMALESS;
        DEFINE TABLE system_event SCHEMALESS;
    ";

    fn make_predicate_registry(
        space_id: &str,
        predicate_id: &str,
        semantics: Option<PredicateSemantics>,
    ) -> PredicateRegistry {
        use elephant_types::records::base::BaseFields;
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, space_id.to_string(), "test".to_string());

        PredicateRegistry {
            base,
            predicate_id: predicate_id.to_string(),
            description: None,
            status: "active".to_string(),
            semantics,
            semantics_status: SemanticsStatus::Configured,
            first_seen_at: None,
            usage_count: 0,
            last_used_at: None,
        }
    }

    fn make_predicate_alias(
        space_id: &str,
        predicate_id: &str,
        confidence_version: u32,
    ) -> PredicateAlias {
        use elephant_types::records::predicate::{AliasScope, AliasStatus, AliasType};

        let id = RecordId::new_v4(TableName::PredicateAlias);
        let base = BaseFields::new(id, space_id.to_string(), "test".to_string());

        PredicateAlias {
            base,
            alias: format!("{predicate_id}_alias"),
            alias_type: AliasType::RawString,
            source_plugin: None,
            confidence: 0.95,
            status: AliasStatus::Active,
            scope: AliasScope::Global,
            predicate_id: predicate_id.to_string(),
            confidence_version,
            usage_count: 0,
        }
    }

    async fn setup_tm_storage() -> StorageClient {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(TM_SCHEMA).await.expect("schema");
        storage
    }

    // =========================================================================
    // Slot Key Computation Tests
    // =========================================================================

    /// Test: Slot key computation is deterministic.
    ///
    /// Per spec §12.3: slot_key must be stable across runs.
    #[test]
    fn test_slot_key_computation_deterministic() {
        let key1 = compute_slot_key("entity:123", Some("entity:456"), "works_at", None, "hash1");
        let key2 = compute_slot_key("entity:123", Some("entity:456"), "works_at", None, "hash1");

        assert_eq!(key1, key2, "Same inputs should produce same slot key");
    }

    /// Test: Slot key differs for different inputs.
    #[test]
    fn test_slot_key_uniqueness() {
        let key1 = compute_slot_key("entity:123", Some("entity:456"), "works_at", None, "hash1");
        let key2 = compute_slot_key("entity:123", Some("entity:789"), "works_at", None, "hash1");

        assert_ne!(
            key1, key2,
            "Different object entities should produce different keys"
        );
    }

    /// Test: Slot key uses spec ordering for rels.
    #[test]
    fn test_slot_key_ordering_rel() {
        let key = compute_slot_key(
            "entity:subject",
            Some("entity:object"),
            "works_at",
            None,
            "hash",
        );

        assert_eq!(
            key, "entity:subject::entity:object::works_at::hash",
            "Slot key order should be subject, object, predicate, hash"
        );
    }

    /// Test: Slot type determined correctly.
    #[test]
    fn test_slot_type_determination() {
        assert_eq!(determine_slot_type(Some("entity:456")), "rel");
        assert_eq!(determine_slot_type(None), "attribute");
    }

    // =========================================================================
    // Conflict Detection Tests
    // =========================================================================

    /// Test: Conflict group creation.
    ///
    /// Per spec §12.3: conflicting claims are grouped.
    #[test]
    fn test_conflict_group_creation() {
        let group = create_conflict_group(
            "slot::key",
            "rel",
            vec!["claim1".to_string(), "claim2".to_string()],
            "test_space",
        );

        assert_eq!(group.slot_key, "slot::key");
        assert_eq!(group.claim_ids.len(), 2);
        assert!(!group.resolved);
    }

    /// Test: Upserted conflict groups do not create duplicates.
    #[tokio::test]
    async fn test_upsert_unresolved_conflict_group_avoids_duplicates() {
        let storage = MockTruthMaintenanceStorage::new();
        let slot_key = "slot::key";

        let group_a = build_unresolved_conflict_group(
            slot_key,
            "rel",
            vec!["claim1".to_string()],
            "test_space",
        );
        let group_b = build_unresolved_conflict_group(
            slot_key,
            "rel",
            vec!["claim1".to_string(), "claim2".to_string()],
            "test_space",
        );

        storage
            .upsert_unresolved_conflict_group(group_a)
            .await
            .expect("upsert should succeed");
        storage
            .upsert_unresolved_conflict_group(group_b.clone())
            .await
            .expect("upsert should succeed");

        let groups = storage.conflict_groups.lock().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].claim_ids, group_b.claim_ids);
    }

    /// Test: Conflict detection from outbox events.
    #[tokio::test]
    async fn test_tm_detect_conflicts_from_outbox() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        storage.add_rel(rel1.clone());
        storage.add_rel(rel2.clone());

        let events = vec![TriggerEvent::Rel(rel1.base.id.clone())];

        let result = execute_tm_detect_conflicts(&events, "test_space", &storage)
            .await
            .expect("detect should succeed");

        assert_eq!(result.events_processed, 1);
        assert_eq!(result.conflicts_detected, 0, "Identical values corroborate");
        assert_eq!(result.conflict_group_ids.len(), 0);
        assert_eq!(result.direct_commits.len(), 1);
        assert!(
            matches!(
                &result.direct_commits[0].resolution,
                ResolutionResult::CorroboratedWinner { merged_claim_ids, .. }
                    if merged_claim_ids.len() == 1
            ),
            "Expected corroboration, got {:?}",
            result.direct_commits[0].resolution
        );
    }

    /// Test: Predicate trigger marks affected slots as stale.
    #[tokio::test]
    async fn test_tm_detect_conflicts_marks_stale_on_predicate_trigger() {
        use elephant_types::records::base::BaseFields;

        let storage = MockTruthMaintenanceStorage::new();

        let predicate_id = "works_at";
        let predicate = make_predicate_registry(
            "test_space",
            predicate_id,
            Some(PredicateSemantics {
                conflict_policy: ConflictPolicy::SupersedeByRecency,
                ..PredicateSemantics::default()
            }),
        );
        let predicate_record_id = predicate.base.id.clone();
        storage.add_predicate_registry(predicate);

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            predicate_id,
            None,
            "hash",
        );

        let slot = TruthSlot {
            base: BaseFields::new(
                RecordId::new_v4(TableName::TruthSlot),
                "test_space".to_string(),
                "test".to_string(),
            ),
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: predicate_id.to_string(),
            current_values: vec![],
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: "hash".to_string(),
        };
        storage.create_slot(slot).await.expect("slot created");

        let events = vec![TriggerEvent::Predicate(predicate_record_id)];
        let result = execute_tm_detect_conflicts(&events, "test_space", &storage)
            .await
            .expect("detect should succeed");

        assert_eq!(result.events_processed, 1);

        let updated = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .expect("find slot")
            .expect("slot exists");
        assert!(updated.stale, "Slot should be marked stale");
        assert_eq!(updated.stale_reason, Some("predicate_updated".to_string()));
    }

    // =========================================================================
    // Conflict Resolution Policy Tests
    // =========================================================================

    /// Test: AllowParallel policy returns all values ranked.
    #[test]
    fn test_allow_parallel_policy() {
        let claims = vec![
            make_claim("claim1", 0.9, Utc::now()),
            make_claim("claim2", 0.7, Utc::now()),
            make_claim("claim3", 0.8, Utc::now()),
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2", "claim3"]);

        let result = resolve_slot_with_policy(&group, &claims, ConflictPolicy::AllowParallel, None);

        match result {
            ResolutionResult::ParallelValues { ranked_values, .. } => {
                assert_eq!(ranked_values.len(), 3);
                // Should be sorted by confidence descending
                assert_eq!(ranked_values[0].0, "claim1"); // 0.9
                assert_eq!(ranked_values[1].0, "claim3"); // 0.8
                assert_eq!(ranked_values[2].0, "claim2"); // 0.7
            }
            _ => panic!("Expected ParallelValues result"),
        }
    }

    #[tokio::test]
    async fn test_execute_tm_resolve_slot_ranks_with_freshness() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash123",
        );

        let claim_a = RecordId::new_v4(TableName::Rel).to_string();
        let claim_b = RecordId::new_v4(TableName::Rel).to_string();

        storage.add_freshness_score(claim_a.clone(), 0.1);
        storage.add_freshness_score(claim_b.clone(), 1.0);

        let group = make_conflict_group(&slot_key, vec![&claim_a, &claim_b]);
        let now = Utc::now();
        let claims = vec![
            make_claim(&claim_a, 0.9, now),
            make_claim(&claim_b, 0.6, now),
        ];

        let (result, extras) =
            execute_tm_resolve_slot(&TmContext::none(), &group, &claims, "test_space", &storage)
                .await
                .expect("resolution succeeds");

        assert!(!extras.adjudicated);
        match result {
            ResolutionResult::ParallelValues { ranked_values, .. } => {
                assert_eq!(ranked_values.len(), 2);
                assert_eq!(ranked_values[0].0, claim_b);
                assert_eq!(ranked_values[1].0, claim_a);
            }
            _ => panic!("Expected ParallelValues"),
        }
    }

    #[tokio::test]
    async fn test_resolve_conflict_group_with_storage_ignores_stale_claim_ids() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let rel = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel_id = rel.base.id.to_string();
        let stale_id = RecordId::new_v4(TableName::Rel).to_string();
        let security_hash = compute_security_hash(&rel.base.security);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );
        storage.add_rel(rel);

        let group = make_conflict_group(&slot_key, vec![&stale_id, &rel_id]);
        let (resolution, input) =
            resolve_conflict_group_with_storage(&TmContext::none(), &group, "test_space", &storage)
                .await
                .expect("stale claim ids are ignored");

        assert_eq!(input.claim_ids, vec![rel_id.clone()]);
        // A single surviving claim corroborates itself.
        match resolution {
            ResolutionResult::CorroboratedWinner {
                winner_id,
                merged_claim_ids,
                ..
            } => {
                assert_eq!(winner_id, rel_id);
                assert!(merged_claim_ids.is_empty());
            }
            _ => panic!("Expected CorroboratedWinner, got {resolution:?}"),
        }
    }

    /// Test: SupersedeByRecency policy picks newest.
    #[test]
    fn test_supersede_by_recency_policy() {
        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);
        let latest = now + chrono::Duration::hours(1);

        let claims = vec![
            make_claim("claim1", 0.9, earlier),
            make_claim("claim2", 0.7, latest), // Newest
            make_claim("claim3", 0.8, now),
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2", "claim3"]);

        let result =
            resolve_slot_with_policy(&group, &claims, ConflictPolicy::SupersedeByRecency, None);

        match result {
            ResolutionResult::SingleWinner { winner_id, .. } => {
                assert_eq!(winner_id, "claim2", "Newest claim should win");
            }
            _ => panic!("Expected SingleWinner result"),
        }
    }

    /// Test: SplitByContext policy separates by context.
    #[test]
    fn test_split_by_context_policy() {
        let claims = vec![
            make_claim_with_context("claim1", 0.9, "role_ceo"),
            make_claim_with_context("claim2", 0.8, "role_cto"),
            make_claim_with_context("claim3", 0.7, "role_ceo"), // Same context as claim1
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2", "claim3"]);

        let result =
            resolve_slot_with_policy(&group, &claims, ConflictPolicy::SplitByContext, None);

        match result {
            ResolutionResult::SplitByContext {
                context_winners, ..
            } => {
                assert_eq!(context_winners.len(), 2); // Two distinct contexts
                assert_eq!(context_winners.get("role_ceo"), Some(&"claim1".to_string())); // Higher confidence
                assert_eq!(context_winners.get("role_cto"), Some(&"claim2".to_string()));
            }
            _ => panic!("Expected SplitByContext result"),
        }
    }

    /// Test: RequireMoreEvidence flags when insufficient evidence.
    #[test]
    fn test_require_more_evidence_policy_insufficient() {
        let claims = vec![
            make_claim_with_evidence("claim1", 0.6, 1), // Low evidence
            make_claim_with_evidence("claim2", 0.7, 2), // Still low
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2"]);
        let semantics = PredicateSemantics {
            min_evidence: 3,
            source_priority: None,
            ..PredicateSemantics::default()
        };

        let result = resolve_slot_with_policy(
            &group,
            &claims,
            ConflictPolicy::RequireMoreEvidence,
            Some(&semantics),
        );

        match result {
            ResolutionResult::NeedsReview { reason } => {
                assert!(reason.contains("insufficient evidence"));
            }
            _ => panic!("Expected NeedsReview result"),
        }
    }

    /// Test: RequireMoreEvidence flags review even when evidence is sufficient.
    #[test]
    fn test_require_more_evidence_policy_sufficient() {
        let claims = vec![
            make_claim_with_evidence("claim1", 0.6, 1), // Low evidence
            make_claim_with_sources("claim2", 0.85, 5, &["official"]), // High evidence + priority
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2"]);
        let semantics = PredicateSemantics {
            min_evidence: 3,
            source_priority: Some(vec!["official".to_string()]),
            ..PredicateSemantics::default()
        };

        let result = resolve_slot_with_policy(
            &group,
            &claims,
            ConflictPolicy::RequireMoreEvidence,
            Some(&semantics),
        );

        match result {
            ResolutionResult::NeedsReview { reason } => {
                assert!(reason.contains("requires review"));
            }
            _ => panic!("Expected NeedsReview result"),
        }
    }

    /// Test: RequireMoreEvidence flags even with prioritized evidence.
    #[test]
    fn test_require_more_evidence_policy_prefers_priority_sources() {
        let claims = vec![
            make_claim_with_sources("claim1", 0.95, 2, &["secondary"]),
            make_claim_with_sources("claim2", 0.8, 2, &["official"]),
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2"]);
        let semantics = PredicateSemantics {
            min_evidence: 1,
            source_priority: Some(vec!["official".to_string(), "secondary".to_string()]),
            ..PredicateSemantics::default()
        };

        let result = resolve_slot_with_policy(
            &group,
            &claims,
            ConflictPolicy::RequireMoreEvidence,
            Some(&semantics),
        );

        match result {
            ResolutionResult::NeedsReview { reason } => {
                assert!(reason.contains("requires review"));
            }
            _ => panic!("Expected NeedsReview result"),
        }
    }

    /// Test: RequireMoreEvidence needs review when priority sources are missing.
    #[test]
    fn test_require_more_evidence_policy_missing_priority_sources() {
        let claims = vec![
            make_claim_with_sources("claim1", 0.9, 3, &["blog"]),
            make_claim_with_sources("claim2", 0.85, 4, &["forum"]),
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2"]);
        let semantics = PredicateSemantics {
            min_evidence: 2,
            source_priority: Some(vec!["official".to_string()]),
            ..PredicateSemantics::default()
        };

        let result = resolve_slot_with_policy(
            &group,
            &claims,
            ConflictPolicy::RequireMoreEvidence,
            Some(&semantics),
        );

        match result {
            ResolutionResult::NeedsReview { reason } => {
                assert!(reason.contains("prioritized sources"));
            }
            _ => panic!("Expected NeedsReview result"),
        }
    }

    /// Test: identical values corroborate without touching the adjudicator.
    #[tokio::test]
    async fn test_adjudicator_skipped_for_single_value_group() {
        let storage = MockTruthMaintenanceStorage::new();
        let adjudicator = Arc::new(FakeAdjudicator::new(Some(verdict(
            AdjudicationDecision::NeedsReview,
        ))));
        let ctx = ctx_with(&adjudicator);

        let value = ClaimValue::Literal(Literal::Integer(35));
        let claims = vec![
            make_claim_with_value("claim1", 0.9, value.clone()),
            make_claim_with_value("claim2", 0.7, value),
        ];
        let group = make_conflict_group("slot1", vec!["claim1", "claim2"]);

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
                .await
                .expect("resolution succeeds");

        assert_eq!(adjudicator.call_count(), 0, "Adjudicator should not run");
        assert!(!extras.adjudicated);
        match result {
            ResolutionResult::CorroboratedWinner {
                winner_id,
                merged_claim_ids,
                ..
            } => {
                assert_eq!(winner_id, "claim1");
                assert_eq!(merged_claim_ids, vec!["claim2".to_string()]);
            }
            other => panic!("Expected CorroboratedWinner, got {other:?}"),
        }
    }

    /// Test: divergent values invoke the adjudicator and its verdict wins.
    #[tokio::test]
    async fn test_adjudicator_called_for_value_conflict() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "age",
            Some(PredicateSemantics::default()),
        ));

        let mut scripted = verdict(AdjudicationDecision::SingleWinner);
        scripted.winner_id = Some("claim2".to_string());
        let adjudicator = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&adjudicator);

        let claims = vec![
            make_claim_with_value("claim1", 0.9, ClaimValue::Literal(Literal::Integer(35))),
            make_claim_with_value("claim2", 0.7, ClaimValue::Literal(Literal::Integer(36))),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["claim1".to_string(), "claim2".to_string()],
            "test_space",
        );

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
                .await
                .expect("resolution succeeds");

        assert_eq!(adjudicator.call_count(), 1, "Adjudicator should run once");
        assert!(extras.adjudicated);
        let input = adjudicator.last_input().expect("input recorded");
        assert_eq!(input.predicate_id, "age");
        assert_eq!(input.claims.len(), 2);
        match result {
            ResolutionResult::SingleWinner { winner_id, .. } => {
                assert_eq!(winner_id, "claim2");
            }
            other => panic!("Expected SingleWinner, got {other:?}"),
        }
    }

    /// Test: execute_tm_resolve_slot uses predicate registry semantics.
    #[tokio::test]
    async fn test_execute_tm_resolve_slot_uses_predicate_semantics_policy() {
        let storage = MockTruthMaintenanceStorage::new();

        let predicate_id = "works_at";
        let semantics = PredicateSemantics {
            conflict_policy: ConflictPolicy::SupersedeByRecency,
            ..PredicateSemantics::default()
        };
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            predicate_id,
            Some(semantics),
        ));

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            predicate_id,
            None,
            "hash",
        );

        let group = create_conflict_group(
            &slot_key,
            "rel",
            vec!["claim1".to_string(), "claim2".to_string()],
            "test_space",
        );

        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);
        let later = now + chrono::Duration::hours(1);
        let claims = vec![
            make_claim("claim1", 0.7, earlier),
            make_claim("claim2", 0.8, later),
        ];

        let (result, _extras) =
            execute_tm_resolve_slot(&TmContext::none(), &group, &claims, "test_space", &storage)
                .await
                .expect("resolve should succeed");

        match result {
            ResolutionResult::SingleWinner { winner_id, .. } => {
                assert_eq!(winner_id, "claim2");
            }
            _ => panic!("Expected SingleWinner result"),
        }
    }

    /// Test: missing semantics emits predicate_conflict_no_semantics event.
    #[tokio::test]
    async fn test_execute_tm_resolve_slot_emits_conflict_no_semantics_event() {
        let storage = MockTruthMaintenanceStorage::new();

        let predicate_id = "works_at";
        let mut predicate = make_predicate_registry("test_space", predicate_id, None);
        predicate.semantics = None;
        predicate.semantics_status = SemanticsStatus::Default;
        storage.add_predicate_registry(predicate);

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            predicate_id,
            None,
            "hash",
        );

        let group = create_conflict_group(
            &slot_key,
            "rel",
            vec!["claim1".to_string(), "claim2".to_string()],
            "test_space",
        );

        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);
        let later = now + chrono::Duration::hours(1);
        let claims = vec![
            make_claim("claim1", 0.7, earlier),
            make_claim("claim2", 0.8, later),
        ];

        let _ =
            execute_tm_resolve_slot(&TmContext::none(), &group, &claims, "test_space", &storage)
                .await
                .expect("resolve should succeed");

        let events = storage.emitted_events();
        assert_eq!(events.len(), 1, "Expected one governance event");
        assert_eq!(events[0].event_type, "predicate_conflict_no_semantics");
        assert_eq!(events[0].space_id, "test_space");
    }

    // =========================================================================
    // Temporal Model Tests
    // =========================================================================

    /// Test: Persistent personality corroborates existing.
    #[test]
    fn test_temporal_personality_persistent() {
        let existing = vec![(
            "claim1".to_string(),
            ValidityWindow::open_ended(Utc::now() - chrono::Duration::days(30)),
        )];

        let result =
            handle_temporal_personality(TemporalPersonality::Persistent, Utc::now(), &existing);

        assert_eq!(result, TemporalHandlingResult::Corroborate);
    }

    /// Test: StateOverTime personality supersedes existing.
    #[test]
    fn test_temporal_personality_state_over_time() {
        let now = Utc::now();
        let existing = vec![(
            "claim1".to_string(),
            ValidityWindow::open_ended(now - chrono::Duration::days(30)),
        )];

        let result =
            handle_temporal_personality(TemporalPersonality::StateOverTime, now, &existing);

        match result {
            TemporalHandlingResult::SupersedeExisting { close_ids, .. } => {
                assert_eq!(close_ids, vec!["claim1"]);
            }
            _ => panic!("Expected SupersedeExisting"),
        }
    }

    /// Test: EventLike personality accumulates.
    #[test]
    fn test_temporal_personality_event_like() {
        let existing = vec![(
            "claim1".to_string(),
            ValidityWindow::open_ended(Utc::now() - chrono::Duration::days(1)),
        )];

        let result =
            handle_temporal_personality(TemporalPersonality::EventLike, Utc::now(), &existing);

        assert_eq!(result, TemporalHandlingResult::AddNew);
    }

    /// Test: Validity window enforcement.
    #[test]
    fn test_validity_window_enforcement() {
        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);

        // Valid: from < to
        let valid = ValidityWindow::new(earlier, Some(now));
        assert!(valid.is_ok());

        // Invalid: from > to
        let invalid = ValidityWindow::new(now, Some(earlier));
        assert!(invalid.is_err());
    }

    /// Test: Temporal supersession closes old windows.
    #[test]
    fn test_temporal_supersession() {
        let start = Utc::now() - chrono::Duration::days(30);
        let mut window = ValidityWindow::open_ended(start);

        assert!(window.is_current());

        let close_time = Utc::now();
        window.close_at(close_time).expect("close should succeed");

        assert!(!window.is_current());
        assert_eq!(window.valid_to, Some(close_time));
    }

    // =========================================================================
    // Commit + Staleness Tests
    // =========================================================================

    /// Test: tm_commit is deterministic.
    #[tokio::test]
    async fn test_tm_commit_deterministic() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let rel = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel_id = rel.base.id.to_string();
        storage.add_rel(rel);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash",
        );

        let resolution = ResolutionResult::SingleWinner {
            winner_id: rel_id.clone(),
            confidence: 0.9,
            rationale: "test".to_string(),
        };

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![rel_id],
            ..CommitInput::default()
        };

        let result1 = execute_tm_commit(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");
        let result2 = execute_tm_commit(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        assert_eq!(result1.slots_updated, result2.slots_updated);
    }

    /// Test: Current value population.
    #[test]
    fn test_current_value_population() {
        let value = populate_current_value("claim1", 0.9);

        assert_eq!(value.claim_id, "claim1");
        assert_eq!(value.confidence, 0.9);
    }

    /// Test: Staleness tracking.
    #[test]
    fn test_staleness_tracking() {
        use elephant_types::records::base::BaseFields;

        let id = RecordId::new_v4(TableName::TruthSlot);
        let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());

        let mut slot = TruthSlot {
            base,
            slot_key: "test".to_string(),
            slot_type: "rel".to_string(),
            subject_entity: "entity:1".to_string(),
            predicate_id: "pred:1".to_string(),
            current_values: vec![],
            conflict_group: None,
            confidence: 0.9,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: "hash".to_string(),
        };

        assert!(!is_slot_stale(&slot));

        mark_slot_stale(&mut slot, "predicate changed");

        assert!(is_slot_stale(&slot));
        assert_eq!(slot.stale_reason, Some("predicate changed".to_string()));
    }

    // =========================================================================
    // Outbox Trigger Tests
    // =========================================================================

    /// Test: Trigger from assertion.
    #[test]
    fn test_trigger_from_assertion() {
        let id = RecordId::new_v4(TableName::Assertion);
        let event = trigger_from_assertion(id.clone());

        assert!(matches!(event, TriggerEvent::Assertion(ref eid) if *eid == id));
    }

    /// Test: Trigger from rel.
    #[test]
    fn test_trigger_from_rel() {
        let id = RecordId::new_v4(TableName::Rel);
        let event = trigger_from_rel(id.clone());

        assert!(matches!(event, TriggerEvent::Rel(ref eid) if *eid == id));
    }

    /// Test: Trigger from attribute.
    #[test]
    fn test_trigger_from_attribute() {
        let id = RecordId::new_v4(TableName::Attribute);
        let event = trigger_from_attribute(id.clone());

        assert!(matches!(event, TriggerEvent::Attribute(ref eid) if *eid == id));
    }

    /// Test: Trigger from event.
    #[test]
    fn test_trigger_from_event() {
        let id = RecordId::new_v4(TableName::Event);
        let event = trigger_from_event(id.clone());

        assert!(matches!(event, TriggerEvent::Event(ref eid) if *eid == id));
    }

    /// Test: Trigger from entity.
    #[test]
    fn test_trigger_from_entity() {
        let id = RecordId::new_v4(TableName::Entity);
        let event = trigger_from_entity(id.clone());

        assert!(matches!(event, TriggerEvent::Entity(ref eid) if *eid == id));
    }

    /// Test: Trigger from predicate.
    #[test]
    fn test_trigger_from_predicate() {
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let event = trigger_from_predicate(id.clone());

        assert!(matches!(event, TriggerEvent::Predicate(ref eid) if *eid == id));
    }

    /// Test: Trigger from revision.
    #[test]
    fn test_trigger_from_revision() {
        let id = RecordId::new_v4(TableName::DocRevision);
        let event = trigger_from_revision(id.clone());

        assert!(matches!(event, TriggerEvent::Revision(ref eid) if *eid == id));
    }

    /// Test: outbox payload mapping yields trigger events.
    #[test]
    fn test_trigger_events_from_outbox_payload() {
        let rel_id = RecordId::new_v4(TableName::Rel);
        let attr_id = RecordId::new_v4(TableName::Attribute);
        let event_id = RecordId::new_v4(TableName::Event);

        let mut payload = OutboxPayload::new(
            "test_space".to_string(),
            "test.event".to_string(),
            "test_actor".to_string(),
        );
        payload.add_record(RecordRef {
            id: rel_id.clone(),
            table: "rel".to_string(),
            op: EventOperation::Create,
        });
        payload.add_record(RecordRef {
            id: attr_id.clone(),
            table: "attribute".to_string(),
            op: EventOperation::Update,
        });
        payload.add_record(RecordRef {
            id: event_id.clone(),
            table: "event".to_string(),
            op: EventOperation::Create,
        });
        payload.add_record(RecordRef {
            id: RecordId::new_v4(TableName::WorkItem),
            table: "work_item".to_string(),
            op: EventOperation::Create,
        });

        let triggers = trigger_events_from_outbox_payload(&payload);
        assert_eq!(triggers.len(), 3);
        assert!(triggers
            .iter()
            .any(|event| matches!(event, TriggerEvent::Rel(id) if *id == rel_id)));
        assert!(triggers
            .iter()
            .any(|event| matches!(event, TriggerEvent::Attribute(id) if *id == attr_id)));
        assert!(triggers
            .iter()
            .any(|event| matches!(event, TriggerEvent::Event(id) if *id == event_id)));
    }

    #[test]
    fn test_trigger_events_from_system_event_ignores_non_outbox_payload() {
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "test_space".to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "invalid.payload".to_string(),
            payload: serde_json::json!({
                "unexpected": "shape"
            }),
        };

        let (space_id, triggers) =
            trigger_events_from_system_event(&event).expect("non-outbox events are ignored");
        assert_eq!(space_id, "test_space");
        assert!(triggers.is_empty());
    }

    #[test]
    fn test_trigger_events_from_system_event_fills_legacy_outbox_fields() {
        let rel_id = RecordId::new_v4(TableName::Rel);
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "test_space".to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "rel.created".to_string(),
            payload: serde_json::json!({
                "records": [{
                    "id": rel_id,
                    "table": "rel",
                    "op": "create"
                }]
            }),
        };

        let (space_id, triggers) =
            trigger_events_from_system_event(&event).expect("legacy payload should parse");
        assert_eq!(space_id, "test_space");
        assert_eq!(triggers.len(), 1);
        assert!(matches!(&triggers[0], TriggerEvent::Rel(id) if *id == rel_id));
    }

    #[test]
    fn test_trigger_events_from_system_event_rejects_malformed_outbox_records() {
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "test_space".to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "rel.created".to_string(),
            payload: serde_json::json!({
                "records": [{
                    "table": "rel",
                    "op": "create"
                }]
            }),
        };

        let err = trigger_events_from_system_event(&event).expect_err("malformed outbox must fail");
        assert!(
            matches!(&err, PipelineError::InvalidInput(msg) if msg.contains("Invalid outbox payload")),
            "expected invalid payload error, got {err:?}"
        );
    }

    /// Test: conflict detection from outbox system events.
    #[tokio::test]
    async fn test_execute_tm_detect_conflicts_from_outbox_events() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        storage.add_rel(rel1.clone());
        storage.add_rel(rel2.clone());

        let mut payload = OutboxPayload::new(
            "test_space".to_string(),
            "rel.created".to_string(),
            "outbox".to_string(),
        );
        payload.add_record(RecordRef {
            id: rel1.base.id.clone(),
            table: "rel".to_string(),
            op: EventOperation::Create,
        });

        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "test_space".to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "rel.created".to_string(),
            payload: serde_json::to_value(&payload).expect("payload serialize"),
        };

        let result = execute_tm_detect_conflicts_from_outbox_events(&[event], &storage)
            .await
            .expect("detect conflicts should succeed");

        assert_eq!(result.events_processed, 1);
        assert_eq!(result.conflicts_detected, 0, "Identical values corroborate");
        assert_eq!(result.direct_commits.len(), 1);
        assert!(matches!(
            &result.direct_commits[0].resolution,
            ResolutionResult::CorroboratedWinner { .. }
        ));
    }

    #[tokio::test]
    async fn test_truth_maintenance_outbox_conflict_detection_persists() {
        let storage = setup_tm_storage().await;
        let rel_repo = RelRepository::new(storage.clone());
        let conflict_repo = ConflictGroupRepository::new(storage.clone());

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel1_id = rel1.base.id.clone();
        let rel2_id = rel2.base.id.clone();

        rel_repo.create(rel1.clone()).await.expect("create rel1");
        rel_repo.create(rel2.clone()).await.expect("create rel2");

        let mut payload = OutboxPayload::new(
            "test_space".to_string(),
            "rel.created".to_string(),
            "outbox".to_string(),
        );
        payload.add_record(RecordRef {
            id: rel1_id.clone(),
            table: "rel".to_string(),
            op: EventOperation::Create,
        });

        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "test_space".to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "rel.created".to_string(),
            payload: serde_json::to_value(&payload).expect("payload serialize"),
        };

        let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
        let result = execute_tm_detect_conflicts_from_outbox_events(&[event], &tm_storage)
            .await
            .expect("detect conflicts");

        assert_eq!(result.conflicts_detected, 0, "Identical values corroborate");
        assert_eq!(result.direct_commits.len(), 1);

        let security_hash = compute_security_hash(&rel1.base.security);
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );

        assert!(
            conflict_repo
                .find_unresolved_by_slot_key("test_space", &slot_key)
                .await
                .expect("find conflict group")
                .is_none(),
            "No conflict group for identical values"
        );

        let commit = &result.direct_commits[0];
        let ResolutionResult::CorroboratedWinner {
            winner_id,
            merged_claim_ids,
            ..
        } = &commit.resolution
        else {
            panic!("Expected CorroboratedWinner, got {:?}", commit.resolution);
        };
        assert_eq!(merged_claim_ids.len(), 1);

        execute_tm_commit_with_storage(
            &commit.resolution,
            &commit.input,
            "test_space",
            &tm_storage,
        )
        .await
        .expect("corroboration commit");

        let winner_record_id = RecordId::from_str(winner_id).expect("winner id");
        let merged_record_id = RecordId::from_str(&merged_claim_ids[0]).expect("merged id");
        assert!(
            [&rel1_id, &rel2_id].contains(&&winner_record_id),
            "winner is one of the duplicates"
        );

        let winner = rel_repo
            .get("test_space", &winner_record_id)
            .await
            .expect("get winner")
            .expect("winner exists");
        assert_eq!(winner.status, ClaimStatus::Approved);
        assert_eq!(winner.evidence.len(), 2, "duplicate evidence merged");
        assert!(
            winner.validity_window.map_or(true, |w| w.end.is_none()),
            "winner window stays open"
        );

        let merged = rel_repo
            .get("test_space", &merged_record_id)
            .await
            .expect("get merged");
        assert!(merged.is_none(), "duplicate row soft-deleted");
        let merged_with_deleted = rel_repo
            .get_with_deleted("test_space", &merged_record_id)
            .await
            .expect("get merged with deleted")
            .expect("merged row still exists");
        assert!(merged_with_deleted.base.deleted_at.is_some());
        assert_eq!(
            merged_with_deleted.status,
            ClaimStatus::Candidate,
            "merged duplicate keeps its status; it is deleted, not superseded"
        );
        assert!(merged_with_deleted.validity_window.is_none());
    }

    #[tokio::test]
    async fn test_truth_maintenance_commit_persists_updates() {
        let storage = setup_tm_storage().await;
        let rel_repo = RelRepository::new(storage.clone());
        let conflict_repo = ConflictGroupRepository::new(storage.clone());
        let slot_repo = TruthSlotRepository::new(storage.clone());

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let mut rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );

        let earlier = Utc::now() - chrono::Duration::days(7);
        let later = Utc::now();
        rel1.base.created_at = earlier;
        rel2.base.created_at = later;
        for evidence in rel1.evidence.iter_mut() {
            evidence.time_value = earlier;
        }
        for evidence in rel2.evidence.iter_mut() {
            evidence.time_value = later;
        }

        let rel1_id = rel1.base.id.clone();
        let rel2_id = rel2.base.id.clone();

        let security_hash = compute_security_hash(&rel2.base.security);

        rel_repo.create(rel1).await.expect("create rel1");
        rel_repo.create(rel2).await.expect("create rel2");
        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );

        let group = create_conflict_group(
            &slot_key,
            "rel",
            vec![rel1_id.to_string(), rel2_id.to_string()],
            "test_space",
        );
        conflict_repo.create(group).await.expect("create group");

        let resolution = ResolutionResult::SingleWinner {
            winner_id: rel2_id.to_string(),
            confidence: 0.95,
            rationale: "latest".to_string(),
        };

        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash,
            claim_ids: vec![rel1_id.to_string(), rel2_id.to_string()],
            ..CommitInput::default()
        };

        let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &tm_storage)
            .await
            .expect("commit");

        assert_eq!(result.slots_updated, 1);

        let updated_rel1 = rel_repo
            .get("test_space", &rel1_id)
            .await
            .expect("get rel1")
            .expect("rel1 exists");
        let updated_rel2 = rel_repo
            .get("test_space", &rel2_id)
            .await
            .expect("get rel2")
            .expect("rel2 exists");

        assert_eq!(updated_rel1.status, ClaimStatus::Superseded);
        assert_eq!(updated_rel2.status, ClaimStatus::Approved);
        assert_eq!(
            updated_rel1.validity_window.as_ref().and_then(|w| w.end),
            Some(later)
        );
        assert_eq!(
            updated_rel2.validity_window.as_ref().and_then(|w| w.start),
            Some(later)
        );

        let slot = slot_repo
            .find_by_slot_key("test_space", &slot_key)
            .await
            .expect("find slot")
            .expect("slot exists");
        assert_eq!(slot.current_values.len(), 1);
        assert_eq!(slot.current_values[0].claim_id, rel2_id.to_string());
    }

    // =========================================================================
    // Recompute Tests
    // =========================================================================

    /// Test: tm_recompute recomputes stale slots.
    #[tokio::test]
    async fn test_tm_recompute_recomputes_stale_slot() {
        use elephant_types::records::base::BaseFields;

        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let mut rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );

        let earlier = Utc::now() - chrono::Duration::days(7);
        let later = Utc::now();
        rel1.base.created_at = earlier;
        rel2.base.created_at = later;
        for evidence in rel1.evidence.iter_mut() {
            evidence.time_value = earlier;
        }
        for evidence in rel2.evidence.iter_mut() {
            evidence.time_value = later;
        }

        let rel1_id = rel1.base.id.to_string();
        let rel2_id = rel2.base.id.to_string();

        let security_hash = compute_security_hash(&rel1.base.security);

        storage.add_rel(rel1);
        storage.add_rel(rel2);

        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "works_at",
            Some(PredicateSemantics {
                conflict_policy: ConflictPolicy::SupersedeByRecency,
                ..PredicateSemantics::default()
            }),
        ));

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );
        let slot = TruthSlot {
            base: BaseFields::new(
                RecordId::new_v4(TableName::TruthSlot),
                "test_space".to_string(),
                "test".to_string(),
            ),
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            current_values: vec![],
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale: true,
            stale_reason: Some("predicate_updated".to_string()),
            computed_at: Utc::now(),
            security_hash,
        };
        storage.create_slot(slot).await.expect("create slot");

        let result = execute_tm_recompute(
            &TmContext::none(),
            "test_space",
            RecomputePriority::Normal,
            10,
            &storage,
        )
        .await
        .expect("recompute should succeed");

        assert_eq!(result.stale_slots_found, 1);
        assert_eq!(result.slots_recomputed, 1);
        assert_eq!(result.slots_skipped, 0);
        assert!(result.pending_conflict_groups.is_empty());

        // Identical values corroborate: earliest claim wins, duplicate merges.
        let updated = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .expect("find slot")
            .expect("slot exists");
        assert!(!updated.stale, "Slot should no longer be stale");
        assert_eq!(updated.current_values.len(), 1);
        assert_eq!(updated.current_values[0].claim_id, rel1_id);

        let winner = storage
            .get_rel("test_space", &RecordId::from_str(&rel1_id).unwrap())
            .await
            .unwrap()
            .expect("rel1 exists");
        assert_eq!(winner.status, ClaimStatus::Approved);
        assert_eq!(winner.evidence.len(), 2, "duplicate evidence merged");
        assert!(
            winner.validity_window.map_or(true, |w| w.end.is_none()),
            "winner window stays open"
        );

        let duplicate = storage
            .get_rel("test_space", &RecordId::from_str(&rel2_id).unwrap())
            .await
            .unwrap();
        assert!(duplicate.is_none(), "duplicate soft-deleted");
        let duplicate = storage
            .get_rel_with_deleted("test_space", &RecordId::from_str(&rel2_id).unwrap())
            .await
            .unwrap()
            .expect("duplicate row still exists");
        assert!(duplicate.base.deleted_at.is_some());
        assert!(
            duplicate.validity_window.is_none(),
            "no supersession window"
        );
    }

    /// Test: recompute defers divergent-value slots to tm_resolve_slot.
    #[tokio::test]
    async fn test_tm_recompute_pending_group_split() {
        use elephant_types::records::base::BaseFields;

        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let security_hash = compute_security_hash(&attr1.base.security);
        storage.add_attr(attr1);
        storage.add_attr(attr2);

        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let slot = TruthSlot {
            base: BaseFields::new(
                RecordId::new_v4(TableName::TruthSlot),
                "test_space".to_string(),
                "test".to_string(),
            ),
            slot_key: slot_key.clone(),
            slot_type: "attribute".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "age".to_string(),
            current_values: vec![],
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale: true,
            stale_reason: Some("test".to_string()),
            computed_at: Utc::now(),
            security_hash,
        };
        storage.create_slot(slot).await.expect("create slot");

        let result = execute_tm_recompute(
            &TmContext::none(),
            "test_space",
            RecomputePriority::Normal,
            10,
            &storage,
        )
        .await
        .expect("recompute should succeed");

        assert_eq!(result.stale_slots_found, 1);
        assert_eq!(result.slots_recomputed, 0, "no inline commit for conflicts");
        assert_eq!(result.pending_conflict_groups.len(), 1);

        let group = storage
            .find_unresolved_conflict("test_space", &slot_key)
            .await
            .expect("find group")
            .expect("group upserted");
        assert_eq!(group.base.id, result.pending_conflict_groups[0]);
        assert_eq!(group.claim_ids.len(), 2);

        // The slot stays stale until the enqueued resolution commits.
        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .expect("find slot")
            .expect("slot exists");
        assert!(slot.stale);
    }

    /// Test: High priority recompute returns empty when no stale slots.
    #[tokio::test]
    async fn test_high_priority_recompute() {
        let storage = MockTruthMaintenanceStorage::new();

        let result = execute_tm_recompute(
            &TmContext::none(),
            "test_space",
            RecomputePriority::High,
            100,
            &storage,
        )
        .await
        .expect("recompute should succeed");

        assert_eq!(result.stale_slots_found, 0);
        assert_eq!(result.slots_recomputed, 0);
        assert_eq!(result.slots_skipped, 0);
    }

    // =========================================================================
    // Storage-backed Function Tests
    // =========================================================================

    /// Mock storage for testing truth maintenance functions.
    #[derive(Debug, Default)]
    struct MockTruthMaintenanceStorage {
        slots: Mutex<Vec<TruthSlot>>,
        conflict_groups: Mutex<Vec<ConflictGroup>>,
        rels: Mutex<Vec<elephant_types::records::rel::Rel>>,
        attrs: Mutex<Vec<elephant_types::records::attribute::Attribute>>,
        events: Mutex<Vec<Event>>,
        entities: Mutex<Vec<Entity>>,
        freshness_scores: Mutex<HashMap<String, f32>>,
        predicate_registry: Mutex<Vec<PredicateRegistry>>,
        predicate_aliases: Mutex<Vec<PredicateAlias>>,
        audit_logs: Mutex<Vec<AuditLog>>,
        emitted_events: Mutex<Vec<OutboxPayload>>,
        /// Chronological log of claim writes ("update:<id>" / "delete:<id>")
        /// for commit-ordering assertions.
        ops: Mutex<Vec<String>>,
    }

    impl MockTruthMaintenanceStorage {
        fn new() -> Self {
            Self::default()
        }

        fn add_rel(&self, rel: elephant_types::records::rel::Rel) {
            self.rels.lock().unwrap().push(rel);
        }

        fn add_attr(&self, attr: elephant_types::records::attribute::Attribute) {
            self.attrs.lock().unwrap().push(attr);
        }

        fn add_event(&self, event: Event) {
            self.events.lock().unwrap().push(event);
        }

        fn add_predicate_registry(&self, predicate: PredicateRegistry) {
            self.predicate_registry.lock().unwrap().push(predicate);
        }

        #[allow(dead_code)]
        fn add_entity(&self, entity: Entity) {
            self.entities.lock().unwrap().push(entity);
        }

        fn add_freshness_score(&self, target: String, score: f32) {
            self.freshness_scores.lock().unwrap().insert(target, score);
        }

        #[allow(dead_code)]
        fn add_predicate_alias(&self, alias: PredicateAlias) {
            self.predicate_aliases.lock().unwrap().push(alias);
        }

        fn emitted_events(&self) -> Vec<OutboxPayload> {
            self.emitted_events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TruthMaintenanceStorage for MockTruthMaintenanceStorage {
        async fn find_slot_by_key(
            &self,
            space_id: &str,
            slot_key: &str,
        ) -> Result<Option<TruthSlot>, PipelineError> {
            let slots = self.slots.lock().unwrap();
            Ok(slots
                .iter()
                .find(|s| s.base.space_id == space_id && s.slot_key == slot_key)
                .cloned())
        }

        async fn create_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
            self.slots.lock().unwrap().push(slot.clone());
            Ok(slot)
        }

        async fn update_slot(
            &self,
            space_id: &str,
            id: &RecordId,
            slot: TruthSlot,
        ) -> Result<TruthSlot, PipelineError> {
            let mut slots = self.slots.lock().unwrap();
            if let Some(pos) = slots
                .iter()
                .position(|s| s.base.space_id == space_id && s.base.id == *id)
            {
                slots[pos] = slot.clone();
            }
            Ok(slot)
        }

        async fn upsert_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
            let mut slots = self.slots.lock().unwrap();
            if let Some(pos) = slots
                .iter()
                .position(|s| s.base.space_id == slot.base.space_id && s.slot_key == slot.slot_key)
            {
                slots[pos] = slot.clone();
            } else {
                slots.push(slot.clone());
            }
            Ok(slot)
        }

        async fn find_unresolved_conflict(
            &self,
            space_id: &str,
            slot_key: &str,
        ) -> Result<Option<ConflictGroup>, PipelineError> {
            let groups = self.conflict_groups.lock().unwrap();
            Ok(groups
                .iter()
                .find(|g| g.base.space_id == space_id && g.slot_key == slot_key && !g.resolved)
                .cloned())
        }

        async fn find_unresolved_conflict_by_type(
            &self,
            space_id: &str,
            slot_key: &str,
            slot_type: &str,
        ) -> Result<Option<ConflictGroup>, PipelineError> {
            let groups = self.conflict_groups.lock().unwrap();
            Ok(groups
                .iter()
                .find(|g| {
                    g.base.space_id == space_id
                        && g.slot_key == slot_key
                        && g.slot_type == slot_type
                        && !g.resolved
                })
                .cloned())
        }

        async fn get_conflict_group(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<ConflictGroup>, PipelineError> {
            let groups = self.conflict_groups.lock().unwrap();
            Ok(groups
                .iter()
                .find(|g| g.base.space_id == space_id && g.base.id == *id)
                .cloned())
        }

        async fn create_conflict_group(
            &self,
            group: ConflictGroup,
        ) -> Result<ConflictGroup, PipelineError> {
            self.conflict_groups.lock().unwrap().push(group.clone());
            Ok(group)
        }

        async fn upsert_unresolved_conflict_group(
            &self,
            group: ConflictGroup,
        ) -> Result<ConflictGroup, PipelineError> {
            let mut groups = self.conflict_groups.lock().unwrap();
            if let Some(pos) = groups.iter().position(|g| {
                g.base.space_id == group.base.space_id
                    && g.slot_key == group.slot_key
                    && !g.resolved
            }) {
                groups[pos] = group.clone();
            } else {
                groups.push(group.clone());
            }
            Ok(group)
        }

        async fn update_conflict_group(
            &self,
            space_id: &str,
            id: &RecordId,
            group: ConflictGroup,
        ) -> Result<ConflictGroup, PipelineError> {
            let mut groups = self.conflict_groups.lock().unwrap();
            if let Some(pos) = groups
                .iter()
                .position(|g| g.base.space_id == space_id && g.base.id == *id)
            {
                groups[pos] = group.clone();
            }
            Ok(group)
        }

        async fn merge_unresolved_conflict_claims(
            &self,
            space_id: &str,
            id: &RecordId,
            claim_ids: Vec<String>,
        ) -> Result<Option<ConflictGroup>, PipelineError> {
            let mut groups = self.conflict_groups.lock().unwrap();
            if let Some(pos) = groups
                .iter()
                .position(|g| g.base.space_id == space_id && g.base.id == *id && !g.resolved)
            {
                let mut merged: HashSet<String> = groups[pos].claim_ids.iter().cloned().collect();
                for id in claim_ids {
                    merged.insert(id);
                }
                let mut updated = groups[pos].clone();
                updated.claim_ids = merged.into_iter().collect();
                groups[pos] = updated.clone();
                return Ok(Some(updated));
            }
            Ok(None)
        }

        async fn get_rel(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<elephant_types::records::rel::Rel>, PipelineError> {
            let rels = self.rels.lock().unwrap();
            Ok(rels
                .iter()
                .find(|r| {
                    r.base.space_id == space_id && r.base.id == *id && r.base.deleted_at.is_none()
                })
                .cloned())
        }

        async fn get_rel_with_deleted(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<elephant_types::records::rel::Rel>, PipelineError> {
            let rels = self.rels.lock().unwrap();
            Ok(rels
                .iter()
                .find(|r| r.base.space_id == space_id && r.base.id == *id)
                .cloned())
        }

        async fn update_rel(
            &self,
            space_id: &str,
            id: &RecordId,
            rel: elephant_types::records::rel::Rel,
        ) -> Result<elephant_types::records::rel::Rel, PipelineError> {
            self.ops.lock().unwrap().push(format!("update:{id}"));
            let mut rels = self.rels.lock().unwrap();
            if let Some(pos) = rels
                .iter()
                .position(|r| r.base.space_id == space_id && r.base.id == *id)
            {
                rels[pos] = rel.clone();
            } else {
                rels.push(rel.clone());
            }
            Ok(rel)
        }

        async fn delete_rel(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
            self.ops.lock().unwrap().push(format!("delete:{id}"));
            let mut rels = self.rels.lock().unwrap();
            if let Some(rel) = rels
                .iter_mut()
                .find(|r| r.base.space_id == space_id && r.base.id == *id)
            {
                rel.base.soft_delete();
            }
            Ok(())
        }

        async fn get_attribute(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<elephant_types::records::attribute::Attribute>, PipelineError> {
            let attrs = self.attrs.lock().unwrap();
            Ok(attrs
                .iter()
                .find(|a| {
                    a.base.space_id == space_id && a.base.id == *id && a.base.deleted_at.is_none()
                })
                .cloned())
        }

        async fn get_attribute_with_deleted(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<elephant_types::records::attribute::Attribute>, PipelineError> {
            let attrs = self.attrs.lock().unwrap();
            Ok(attrs
                .iter()
                .find(|a| a.base.space_id == space_id && a.base.id == *id)
                .cloned())
        }

        async fn update_attribute(
            &self,
            space_id: &str,
            id: &RecordId,
            attr: elephant_types::records::attribute::Attribute,
        ) -> Result<elephant_types::records::attribute::Attribute, PipelineError> {
            self.ops.lock().unwrap().push(format!("update:{id}"));
            let mut attrs = self.attrs.lock().unwrap();
            if let Some(pos) = attrs
                .iter()
                .position(|a| a.base.space_id == space_id && a.base.id == *id)
            {
                attrs[pos] = attr.clone();
            } else {
                attrs.push(attr.clone());
            }
            Ok(attr)
        }

        async fn delete_attribute(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<(), PipelineError> {
            self.ops.lock().unwrap().push(format!("delete:{id}"));
            let mut attrs = self.attrs.lock().unwrap();
            if let Some(attr) = attrs
                .iter_mut()
                .find(|a| a.base.space_id == space_id && a.base.id == *id)
            {
                attr.base.soft_delete();
            }
            Ok(())
        }

        async fn get_event(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<Event>, PipelineError> {
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .find(|event| {
                    event.base.space_id == space_id
                        && event.base.id == *id
                        && event.base.deleted_at.is_none()
                })
                .cloned())
        }

        async fn get_event_with_deleted(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<Event>, PipelineError> {
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .find(|event| event.base.space_id == space_id && event.base.id == *id)
                .cloned())
        }

        async fn update_event(
            &self,
            space_id: &str,
            id: &RecordId,
            event: Event,
        ) -> Result<Event, PipelineError> {
            self.ops.lock().unwrap().push(format!("update:{id}"));
            let mut events = self.events.lock().unwrap();
            if let Some(pos) = events
                .iter()
                .position(|existing| existing.base.space_id == space_id && existing.base.id == *id)
            {
                events[pos] = event.clone();
            } else {
                events.push(event.clone());
            }
            Ok(event)
        }

        async fn delete_event(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
            self.ops.lock().unwrap().push(format!("delete:{id}"));
            let mut events = self.events.lock().unwrap();
            if let Some(event) = events
                .iter_mut()
                .find(|e| e.base.space_id == space_id && e.base.id == *id)
            {
                event.base.soft_delete();
            }
            Ok(())
        }

        async fn list_events(
            &self,
            space_id: &str,
            limit: usize,
        ) -> Result<Vec<Event>, PipelineError> {
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .filter(|event| event.base.space_id == space_id && event.base.deleted_at.is_none())
                .take(limit)
                .cloned()
                .collect())
        }

        async fn find_rels_for_slot(
            &self,
            space_id: &str,
            subject_id: &RecordId,
            object_id: &RecordId,
            rel_type: &str,
        ) -> Result<Vec<elephant_types::records::rel::Rel>, PipelineError> {
            let rels = self.rels.lock().unwrap();
            Ok(rels
                .iter()
                .filter(|r| {
                    r.base.space_id == space_id
                        && r.subject.id() == subject_id
                        && r.object.id() == object_id
                        && r.rel_type == rel_type
                        && r.base.deleted_at.is_none()
                })
                .cloned()
                .collect())
        }

        async fn find_rels_for_subject_predicate(
            &self,
            space_id: &str,
            subject_id: &RecordId,
            rel_type: &str,
        ) -> Result<Vec<elephant_types::records::rel::Rel>, PipelineError> {
            let rels = self.rels.lock().unwrap();
            Ok(rels
                .iter()
                .filter(|r| {
                    r.base.space_id == space_id
                        && r.subject.id() == subject_id
                        && r.rel_type == rel_type
                        && r.base.deleted_at.is_none()
                })
                .cloned()
                .collect())
        }

        async fn get_entity(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<Entity>, PipelineError> {
            let entities = self.entities.lock().unwrap();
            Ok(entities
                .iter()
                .find(|e| e.base.space_id == space_id && e.base.id == *id)
                .cloned())
        }

        async fn get_assertion(
            &self,
            _space_id: &str,
            _id: &RecordId,
        ) -> Result<Option<Assertion>, PipelineError> {
            Ok(None)
        }

        async fn find_attrs_for_slot(
            &self,
            space_id: &str,
            subject_id: &RecordId,
            attr_type: &str,
        ) -> Result<Vec<elephant_types::records::attribute::Attribute>, PipelineError> {
            let attrs = self.attrs.lock().unwrap();
            Ok(attrs
                .iter()
                .filter(|a| {
                    a.base.space_id == space_id
                        && a.subject.id() == subject_id
                        && a.attr_type == attr_type
                        && a.base.deleted_at.is_none()
                })
                .cloned()
                .collect())
        }

        async fn find_stale_slots(
            &self,
            space_id: &str,
            limit: usize,
        ) -> Result<Vec<TruthSlot>, PipelineError> {
            let slots = self.slots.lock().unwrap();
            Ok(slots
                .iter()
                .filter(|s| s.base.space_id == space_id && s.stale)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn find_predicate_registry_by_predicate_id(
            &self,
            space_id: &str,
            predicate_id: &str,
        ) -> Result<Option<PredicateRegistry>, PipelineError> {
            let predicates = self.predicate_registry.lock().unwrap();
            Ok(predicates
                .iter()
                .find(|p| p.base.space_id == space_id && p.predicate_id == predicate_id)
                .cloned())
        }

        async fn get_predicate_registry(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<PredicateRegistry>, PipelineError> {
            let predicates = self.predicate_registry.lock().unwrap();
            Ok(predicates
                .iter()
                .find(|p| p.base.space_id == space_id && p.base.id == *id)
                .cloned())
        }

        async fn get_predicate_alias(
            &self,
            space_id: &str,
            id: &RecordId,
        ) -> Result<Option<PredicateAlias>, PipelineError> {
            let aliases = self.predicate_aliases.lock().unwrap();
            Ok(aliases
                .iter()
                .find(|p| p.base.space_id == space_id && p.base.id == *id)
                .cloned())
        }

        async fn list_predicate_aliases_by_predicate_id(
            &self,
            space_id: &str,
            predicate_id: &str,
        ) -> Result<Vec<PredicateAlias>, PipelineError> {
            let aliases = self.predicate_aliases.lock().unwrap();
            Ok(aliases
                .iter()
                .filter(|alias| {
                    alias.base.space_id == space_id && alias.predicate_id == predicate_id
                })
                .cloned()
                .collect())
        }

        async fn upsert_predicate_semantics(
            &self,
            space_id: &str,
            predicate_id: &str,
            semantics: PredicateSemantics,
            status: SemanticsStatus,
            provenance_note: &str,
        ) -> Result<PredicateRegistry, PipelineError> {
            let mut predicates = self.predicate_registry.lock().unwrap();
            if let Some(existing) = predicates
                .iter_mut()
                .find(|p| p.base.space_id == space_id && p.predicate_id == predicate_id)
            {
                if existing.semantics_status == SemanticsStatus::Configured {
                    return Ok(existing.clone());
                }
                existing.semantics = Some(semantics);
                existing.semantics_status = status;
                if existing.description.is_none() && !provenance_note.is_empty() {
                    existing.description = Some(provenance_note.to_string());
                }
                return Ok(existing.clone());
            }

            let id = RecordId::new_v4(TableName::PredicateRegistry);
            let base = BaseFields::new(id, space_id.to_string(), "truth_maintenance".to_string());
            let record = PredicateRegistry {
                base,
                predicate_id: predicate_id.to_string(),
                description: (!provenance_note.is_empty()).then(|| provenance_note.to_string()),
                status: "active".to_string(),
                semantics: Some(semantics),
                semantics_status: status,
                first_seen_at: Some(Utc::now()),
                usage_count: 0,
                last_used_at: None,
            };
            predicates.push(record.clone());
            Ok(record)
        }

        async fn create_audit_log(&self, log: AuditLog) -> Result<AuditLog, PipelineError> {
            self.audit_logs.lock().unwrap().push(log.clone());
            Ok(log)
        }

        async fn emit_system_event(&self, payload: OutboxPayload) -> Result<(), PipelineError> {
            self.emitted_events.lock().unwrap().push(payload);
            Ok(())
        }

        async fn mark_slots_stale_by_predicate(
            &self,
            space_id: &str,
            predicate_id: &str,
            reason: &str,
        ) -> Result<(), PipelineError> {
            let mut slots = self.slots.lock().unwrap();
            for slot in slots.iter_mut() {
                if slot.base.space_id == space_id && slot.predicate_id == predicate_id {
                    slot.stale = true;
                    slot.stale_reason = Some(reason.to_string());
                }
            }
            Ok(())
        }

        async fn mark_slots_stale_by_entity(
            &self,
            space_id: &str,
            entity_id: &RecordId,
            reason: &str,
        ) -> Result<(), PipelineError> {
            let entity_str = entity_id.to_string();
            let mut slots = self.slots.lock().unwrap();
            for slot in slots.iter_mut() {
                if slot.base.space_id != space_id {
                    continue;
                }
                if slot.subject_entity == entity_str || slot.slot_key.contains(&entity_str) {
                    slot.stale = true;
                    slot.stale_reason = Some(reason.to_string());
                }
            }
            Ok(())
        }

        async fn mark_all_slots_stale(
            &self,
            space_id: &str,
            reason: &str,
        ) -> Result<(), PipelineError> {
            let mut slots = self.slots.lock().unwrap();
            for slot in slots.iter_mut() {
                if slot.base.space_id == space_id {
                    slot.stale = true;
                    slot.stale_reason = Some(reason.to_string());
                }
            }
            Ok(())
        }

        async fn find_freshness_score(
            &self,
            _space_id: &str,
            target: &str,
        ) -> Result<Option<f32>, PipelineError> {
            Ok(self.freshness_scores.lock().unwrap().get(target).copied())
        }
    }

    fn make_test_rel(
        space_id: &str,
        subject_id: RecordId,
        object_id: RecordId,
        rel_type: &str,
    ) -> elephant_types::records::rel::Rel {
        use elephant_types::records::rel::ClaimStatus;
        use elephant_types::Link;

        let id = RecordId::new_v4(TableName::Rel);
        let base = BaseFields::new(id, space_id.to_string(), "test".to_string());

        elephant_types::records::rel::Rel {
            base,
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: rel_type.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![make_test_evidence()],
            temporal_audit: None,
        }
    }

    fn make_test_rel_with_security(
        space_id: &str,
        subject_id: RecordId,
        object_id: RecordId,
        rel_type: &str,
        security: Security,
    ) -> elephant_types::records::rel::Rel {
        let mut rel = make_test_rel(space_id, subject_id, object_id, rel_type);
        rel.base.security = security;
        rel
    }

    fn make_test_evidence() -> EvidenceRef {
        use elephant_types::records::assertion::EvidenceSpan;
        use elephant_types::Link;

        EvidenceRef {
            assertion: Link::new(RecordId::new_v4(TableName::Assertion)),
            span: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 4,
                quote_hash: "hash".to_string(),
            },
            time_basis: TimeBasis::ObservedAt,
            time_value: Utc::now(),
        }
    }

    fn make_test_attribute(
        space_id: &str,
        subject_id: RecordId,
        attr_type: &str,
    ) -> elephant_types::records::attribute::Attribute {
        use elephant_types::records::attribute::Literal;
        use elephant_types::records::rel::ClaimStatus;
        use elephant_types::Link;

        let id = RecordId::new_v4(TableName::Attribute);
        let base = BaseFields::new(id, space_id.to_string(), "test".to_string());

        elephant_types::records::attribute::Attribute {
            base,
            subject: Link::new(subject_id),
            attr_type: attr_type.to_string(),
            value: Literal::Integer(42),
            status: ClaimStatus::Candidate,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![make_test_evidence()],
            temporal_audit: None,
        }
    }

    fn make_test_attribute_with_value(
        space_id: &str,
        subject_id: RecordId,
        attr_type: &str,
        value: Literal,
    ) -> elephant_types::records::attribute::Attribute {
        let mut attr = make_test_attribute(space_id, subject_id, attr_type);
        attr.value = value;
        attr
    }

    fn make_test_event(
        space_id: &str,
        title: &str,
        subjects: Vec<RecordId>,
        time_start: DateTime<Utc>,
        confidence: f32,
    ) -> Event {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Event),
            space_id.to_string(),
            "test".to_string(),
        );
        base.subjects = subjects.into_iter().map(Link::new).collect();
        base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
        Event {
            base,
            title: title.to_string(),
            summary: None,
            status: EventStatus::Candidate,
            time_start: Some(time_start),
            time_end: None,
            time_precision: elephant_types::records::TimePrecision::Exact,
            confidence,
            time_confidence: confidence,
            assertion_refs: vec![Link::new(RecordId::new_v4(TableName::Assertion))],
            source_refs: Vec::new(),
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn test_event_slot_key_fallback_uses_evidence_when_participants_missing() {
        let time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();
        let event_a = make_test_event("test_space", "Board meeting", vec![], time, 0.8);
        let event_b = make_test_event("test_space", "Board meeting", vec![], time, 0.8);

        // Same title/day and no participants should not collapse into the same key
        // when evidence references differ.
        let key_a = compute_event_slot_key(&event_a);
        let key_b = compute_event_slot_key(&event_b);
        assert_ne!(key_a, key_b);
    }

    /// Test: `has title` conflicts resolve to a single winner even without configured semantics.
    #[tokio::test]
    async fn test_has_title_conflict_uses_generic_path_without_semantics() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let mut attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "has title",
            Literal::String("CEO".to_string()),
        );
        attr1.evidence[0].time_value = Utc::now() - chrono::Duration::hours(6);
        let attr1_id = attr1.base.id.clone();
        let security_hash = compute_security_hash(&attr1.base.security);

        let mut attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "has title",
            Literal::String("CTO".to_string()),
        );
        attr2.evidence[0].time_value = Utc::now();
        let attr2_id = attr2.base.id.clone();

        storage.add_attr(attr1);
        storage.add_attr(attr2);

        let events = vec![
            AttributeEventInfo {
                attr_id: attr1_id.clone(),
                subject_id: subject_id.clone(),
                attr_type: "has title".to_string(),
                security_hash: security_hash.clone(),
                deleted: false,
            },
            AttributeEventInfo {
                attr_id: attr2_id.clone(),
                subject_id: subject_id.clone(),
                attr_type: "has title".to_string(),
                security_hash: security_hash.clone(),
                deleted: false,
            },
        ];

        execute_tm_detect_conflicts_with_storage(&[], &events, "test_space", &storage)
            .await
            .expect("detect conflicts");

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            None,
            "has title",
            None,
            &security_hash,
        );
        let conflict_group = storage
            .find_unresolved_conflict("test_space", &slot_key)
            .await
            .expect("find conflict")
            .expect("conflict exists");

        let (resolution, input) = resolve_conflict_group_with_storage(
            &TmContext::none(),
            &conflict_group,
            "test_space",
            &storage,
        )
        .await
        .expect("resolve conflict");
        // No hardcoded has-title table anymore: without configured semantics
        // and without an adjudicator the generic AllowParallel fallback wins.
        assert!(
            matches!(resolution, ResolutionResult::ParallelValues { .. }),
            "expected generic ParallelValues fallback, got {resolution:?}"
        );

        execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .expect("find slot")
            .expect("slot exists");
        assert_eq!(slot.current_values.len(), 2, "both values coexist");
        assert!(slot
            .current_values
            .iter()
            .any(|value| value.claim_id == attr2_id.to_string()));
        assert!(slot
            .current_values
            .iter()
            .all(|value| value.status == ClaimStatus::Approved));
    }

    /// Test: two rels on the same slot carry the identical value (same
    /// object), so detection corroborates: direct commit, no conflict group,
    /// evidence merged, duplicate soft-deleted.
    #[tokio::test]
    async fn test_tm_detect_conflicts_with_storage_finds_rel_conflicts() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let mut rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        rel1.base.created_at = Utc::now() - chrono::Duration::days(1);
        rel2.base.created_at = Utc::now();
        storage.add_rel(rel1.clone());
        storage.add_rel(rel2.clone());
        let security_hash = compute_security_hash(&rel1.base.security);

        let rel_events = vec![RelEventInfo {
            rel_id: rel1.base.id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_id.clone(),
            rel_type: "works_at".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&rel_events, &[], "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(result.events_processed, 1);
        assert_eq!(result.conflicts_detected, 0, "Identical values corroborate");
        assert_eq!(result.conflict_group_ids.len(), 0);
        assert_eq!(result.direct_commits.len(), 1);

        let commit = &result.direct_commits[0];
        let ResolutionResult::CorroboratedWinner {
            winner_id,
            merged_claim_ids,
            ..
        } = &commit.resolution
        else {
            panic!("Expected CorroboratedWinner, got {:?}", commit.resolution);
        };
        assert_eq!(winner_id, &rel1.base.id.to_string(), "earliest claim wins");
        assert_eq!(merged_claim_ids, &vec![rel2.base.id.to_string()]);

        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("corroboration commit");

        let winner = storage
            .get_rel("test_space", &rel1.base.id)
            .await
            .unwrap()
            .expect("winner exists");
        assert_eq!(winner.status, ClaimStatus::Approved);
        assert_eq!(winner.evidence.len(), 2, "duplicate evidence merged");

        assert!(
            storage
                .get_rel("test_space", &rel2.base.id)
                .await
                .unwrap()
                .is_none(),
            "duplicate soft-deleted"
        );
        let duplicate = storage
            .get_rel_with_deleted("test_space", &rel2.base.id)
            .await
            .unwrap()
            .expect("duplicate row kept");
        assert!(duplicate.base.deleted_at.is_some());
        assert!(
            duplicate.validity_window.is_none(),
            "no supersession window"
        );
    }

    /// Test: Event-like rels accumulate via direct commits without conflicts.
    #[tokio::test]
    async fn test_tm_detect_conflicts_event_like_rels_direct_commit() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "attended_meeting",
        );
        let rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "attended_meeting",
        );
        storage.add_rel(rel1.clone());
        storage.add_rel(rel2.clone());

        let semantics = PredicateSemantics {
            temporal_model: TemporalPersonality::EventLike,
            ..PredicateSemantics::default()
        };
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "attended_meeting",
            Some(semantics),
        ));

        let rel_events = vec![RelEventInfo {
            rel_id: rel1.base.id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_id.clone(),
            rel_type: "attended_meeting".to_string(),
            security_hash: compute_security_hash(&rel1.base.security),
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&rel_events, &[], "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(
            result.conflicts_detected, 0,
            "Event-like rels do not conflict"
        );
        assert_eq!(result.conflict_group_ids.len(), 0);
        assert_eq!(
            result.direct_commits.len(),
            1,
            "Event-like rels should enqueue direct commits"
        );
        let commit = &result.direct_commits[0];
        let mut expected_ids = vec![rel1.base.id.to_string(), rel2.base.id.to_string()];
        expected_ids.sort();
        assert_eq!(commit.input.slot_type, "rel");
        assert_eq!(commit.input.predicate_id, "attended_meeting");
        assert_eq!(commit.input.claim_ids, expected_ids);
        match &commit.resolution {
            ResolutionResult::ParallelValues { ranked_values, .. } => {
                let mut actual_ids: Vec<String> =
                    ranked_values.iter().map(|(id, _)| id.clone()).collect();
                actual_ids.sort();
                assert_eq!(actual_ids, expected_ids);
            }
            _ => panic!("Expected ParallelValues resolution for event-like rels"),
        }
    }

    /// Test: Conflict detection ignores claims from different security envelopes.
    #[tokio::test]
    async fn test_tm_detect_conflicts_filters_by_security_hash() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let security_a = Security {
            policy_id: "policy_a".to_string(),
            level: 1,
            ..Security::default()
        };

        let security_b = Security {
            policy_id: "policy_b".to_string(),
            level: 2,
            ..Security::default()
        };

        let rel1 = make_test_rel_with_security(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
            security_a.clone(),
        );
        let rel2 = make_test_rel_with_security(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
            security_b,
        );
        storage.add_rel(rel1.clone());
        storage.add_rel(rel2.clone());

        let rel_events = vec![RelEventInfo {
            rel_id: rel1.base.id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_id.clone(),
            rel_type: "works_at".to_string(),
            security_hash: compute_security_hash(&security_a),
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&rel_events, &[], "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(result.conflicts_detected, 0, "No conflict across envelopes");
    }

    /// Test: identical attribute values corroborate instead of conflicting.
    #[tokio::test]
    async fn test_tm_detect_conflicts_with_storage_finds_attr_conflicts() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);

        // Two attrs with the same subject/type AND the same value (42).
        let attr1 = make_test_attribute("test_space", subject_id.clone(), "age");
        let attr2 = make_test_attribute("test_space", subject_id.clone(), "age");
        storage.add_attr(attr1.clone());
        storage.add_attr(attr2.clone());
        let security_hash = compute_security_hash(&attr1.base.security);

        let attr_events = vec![AttributeEventInfo {
            attr_id: attr1.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash,
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &attr_events, "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(result.events_processed, 1);
        assert_eq!(result.conflicts_detected, 0, "Identical values corroborate");
        assert_eq!(result.direct_commits.len(), 1);
        let commit = &result.direct_commits[0];
        assert!(matches!(
            &commit.resolution,
            ResolutionResult::CorroboratedWinner { .. }
        ));

        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("corroboration commit");

        let live: Vec<_> = storage
            .find_attrs_for_slot("test_space", &subject_id, "age")
            .await
            .unwrap();
        assert_eq!(live.len(), 1, "one live attribute remains");
        assert_eq!(live[0].status, ClaimStatus::Approved);
        assert_eq!(live[0].evidence.len(), 2, "duplicate evidence merged");
    }

    /// Test: divergent attribute values DO open a conflict group.
    #[tokio::test]
    async fn test_tm_detect_conflicts_with_storage_finds_attr_value_conflicts() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        storage.add_attr(attr1.clone());
        storage.add_attr(attr2.clone());
        let security_hash = compute_security_hash(&attr1.base.security);

        let attr_events = vec![AttributeEventInfo {
            attr_id: attr1.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &attr_events, "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(result.conflicts_detected, 1, "Divergent values conflict");
        assert_eq!(result.conflict_group_ids.len(), 1);
        assert!(result.direct_commits.is_empty());

        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let group = storage
            .find_unresolved_conflict("test_space", &slot_key)
            .await
            .unwrap()
            .expect("conflict group exists");
        assert_eq!(group.claim_ids.len(), 2);
    }

    /// Test: No conflict when only one claim exists.
    #[tokio::test]
    async fn test_tm_detect_conflicts_no_conflict_single_claim() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        // Only one rel - no conflict
        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        storage.add_rel(rel1.clone());
        let security_hash = compute_security_hash(&rel1.base.security);

        let rel_events = vec![RelEventInfo {
            rel_id: rel1.base.id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_id.clone(),
            rel_type: "works_at".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&rel_events, &[], "test_space", &storage)
                .await
                .expect("detect should succeed");

        assert_eq!(result.conflicts_detected, 0, "No conflict for single claim");
        assert_eq!(
            result.direct_commits.len(),
            1,
            "Single claim should enqueue direct commit"
        );
        let commit = &result.direct_commits[0];
        let expected_slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            &security_hash,
        );
        assert_eq!(commit.input.slot_key, expected_slot_key);
        assert_eq!(commit.input.slot_type, "rel");
        assert_eq!(commit.input.subject_entity, subject_id.to_string());
        assert_eq!(commit.input.predicate_id, "works_at");
        assert_eq!(commit.input.security_hash, security_hash);
        assert_eq!(commit.input.claim_ids, vec![rel1.base.id.to_string()]);
    }

    #[tokio::test]
    async fn test_choke_003_event_single_group_direct_commit_promotes_to_approved() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let event_time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

        let event = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id, object_id],
            event_time,
            0.82,
        );
        let event_id = event.base.id.clone();
        storage.add_event(event);

        let result = execute_tm_detect_conflicts(
            &[TriggerEvent::Event(event_id.clone())],
            "test_space",
            &storage,
        )
        .await
        .expect("detect should succeed");
        assert_eq!(result.conflicts_detected, 0);
        assert_eq!(result.direct_commits.len(), 1);

        let commit = &result.direct_commits[0];
        assert_eq!(commit.input.slot_type, "event");
        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("event commit should succeed");

        let promoted = storage
            .get_event("test_space", &event_id)
            .await
            .expect("fetch event")
            .expect("event exists");
        assert_eq!(promoted.status, EventStatus::Approved);
    }

    #[tokio::test]
    async fn test_event_conflicts_resolve_to_approved_and_superseded() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let event_a = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id.clone(), object_id.clone()],
            Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap(),
            0.91,
        );
        let event_b = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id, object_id],
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
            0.52,
        );
        let event_a_id = event_a.base.id.clone();
        let event_b_id = event_b.base.id.clone();
        storage.add_event(event_a);
        storage.add_event(event_b);

        let detect = execute_tm_detect_conflicts(
            &[
                TriggerEvent::Event(event_a_id.clone()),
                TriggerEvent::Event(event_b_id.clone()),
            ],
            "test_space",
            &storage,
        )
        .await
        .expect("detect conflicts");
        assert_eq!(detect.conflicts_detected, 1);
        assert_eq!(detect.direct_commits.len(), 1);
        let commit = &detect.direct_commits[0];
        assert_eq!(commit.input.slot_type, "event");
        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("commit");

        let a = storage
            .get_event("test_space", &event_a_id)
            .await
            .expect("get event a")
            .expect("event a exists");
        let b = storage
            .get_event("test_space", &event_b_id)
            .await
            .expect("get event b")
            .expect("event b exists");
        assert_eq!(a.status, EventStatus::Approved);
        assert_eq!(b.status, EventStatus::Superseded);
    }

    #[tokio::test]
    async fn test_event_conflicts_low_confidence_loser_rejected() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let winner = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id.clone(), object_id.clone()],
            Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap(),
            0.91,
        );
        let loser = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id, object_id],
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
            0.05,
        );
        let winner_id = winner.base.id.clone();
        let loser_id = loser.base.id.clone();
        storage.add_event(winner);
        storage.add_event(loser);

        let detect = execute_tm_detect_conflicts(
            &[
                TriggerEvent::Event(winner_id.clone()),
                TriggerEvent::Event(loser_id.clone()),
            ],
            "test_space",
            &storage,
        )
        .await
        .expect("detect conflicts");
        assert_eq!(detect.conflicts_detected, 1);
        assert_eq!(detect.direct_commits.len(), 1);

        let commit = &detect.direct_commits[0];
        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("commit");

        let winner = storage
            .get_event("test_space", &winner_id)
            .await
            .expect("get winner")
            .expect("winner exists");
        let loser = storage
            .get_event("test_space", &loser_id)
            .await
            .expect("get loser")
            .expect("loser exists");
        assert_eq!(winner.status, EventStatus::Approved);
        assert_eq!(loser.status, EventStatus::Rejected);
    }

    #[tokio::test]
    async fn test_event_commit_missing_claim_returns_not_found() {
        let storage = MockTruthMaintenanceStorage::new();
        let missing_event_id = RecordId::new_v4(TableName::Event);

        let resolution = ResolutionResult::SingleWinner {
            winner_id: missing_event_id.to_string(),
            confidence: 0.8,
            rationale: "test".to_string(),
        };
        let input = CommitInput {
            slot_key: "alice announced::2024-01-15::event_group::hash".to_string(),
            slot_type: "event".to_string(),
            subject_entity: "event_group".to_string(),
            predicate_id: "alice announced".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![missing_event_id.to_string()],
            ..CommitInput::default()
        };

        let err = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect_err("missing event claim should fail");
        assert!(
            matches!(&err, PipelineError::NotFound(msg) if msg.contains("Event claim not found")),
            "expected not found error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_resolve_event_conflict_group_with_invalid_slot_key_fails() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let event = make_test_event(
            "test_space",
            "Alice announced Project Phoenix",
            vec![subject_id],
            Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap(),
            0.88,
        );
        let event_id = event.base.id.clone();
        storage.add_event(event);

        let conflict_group = create_conflict_group(
            "invalid_slot_key_without_separators",
            "event",
            vec![event_id.to_string()],
            "test_space",
        );

        let err = resolve_conflict_group_with_storage(
            &TmContext::none(),
            &conflict_group,
            "test_space",
            &storage,
        )
        .await
        .expect_err("invalid event slot key should fail");
        assert!(
            matches!(&err, PipelineError::InvalidInput(msg) if msg.contains("Invalid event slot_key")),
            "expected invalid input for slot key, got {err:?}"
        );
    }

    /// Test: Deleted rel events still trigger recomputation.
    #[tokio::test]
    async fn test_tm_detect_conflicts_deleted_rel_triggers_direct_commit() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let rel_active = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut rel_deleted = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        rel_deleted.base.soft_delete();

        storage.add_rel(rel_active.clone());
        storage.add_rel(rel_deleted.clone());

        let events = vec![TriggerEvent::Rel(rel_deleted.base.id.clone())];
        let result = execute_tm_detect_conflicts(&events, "test_space", &storage)
            .await
            .expect("detect should succeed");

        assert_eq!(
            result.direct_commits.len(),
            1,
            "Deleted rel should recompute slot"
        );
    }

    /// Test: Deleted attribute events still trigger recomputation.
    #[tokio::test]
    async fn test_tm_detect_conflicts_deleted_attribute_triggers_direct_commit() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_active = make_test_attribute("test_space", subject_id.clone(), "age");
        let mut attr_deleted = make_test_attribute("test_space", subject_id.clone(), "age");
        attr_deleted.base.soft_delete();

        storage.add_attr(attr_active.clone());
        storage.add_attr(attr_deleted.clone());

        let events = vec![TriggerEvent::Attribute(attr_deleted.base.id.clone())];
        let result = execute_tm_detect_conflicts(&events, "test_space", &storage)
            .await
            .expect("detect should succeed");

        assert_eq!(
            result.direct_commits.len(),
            1,
            "Deleted attribute should recompute slot"
        );
    }

    /// Test: tm_commit creates new truth slot.
    #[tokio::test]
    async fn test_tm_commit_creates_new_slot() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let rel = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let rel_id = rel.base.id.to_string();
        storage.add_rel(rel);
        let fixed_time = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let mut predicate = make_predicate_registry(
            "test_space",
            "works_at",
            Some(PredicateSemantics::default()),
        );
        predicate.base.updated_at = fixed_time;
        predicate.base.created_at = fixed_time;
        storage.add_predicate_registry(predicate);
        storage.add_predicate_alias(make_predicate_alias("test_space", "works_at", 7));
        let expected_semantics_version = timestamp_to_version(fixed_time);

        let resolution = ResolutionResult::SingleWinner {
            winner_id: rel_id.clone(),
            confidence: 0.9,
            rationale: "Highest confidence".to_string(),
        };

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash",
        );

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![rel_id],
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        assert_eq!(result.slots_updated, 1);
        assert_eq!(result.slot_ids.len(), 1);

        // Verify slot was created
        let slot = storage
            .find_slot_by_key("test_space", &input.slot_key)
            .await
            .unwrap()
            .expect("slot should exist");

        assert_eq!(slot.current_values.len(), 1);
        assert_eq!(slot.current_values[0].claim_id, input.claim_ids[0]);
        assert!(!slot.stale);
        assert_eq!(slot.semantics_version, expected_semantics_version);
        assert_eq!(slot.alias_version, 7);

        let updated_rel = storage
            .get_rel(
                "test_space",
                &RecordId::from_str(&input.claim_ids[0]).unwrap(),
            )
            .await
            .unwrap()
            .expect("rel should exist");
        assert_eq!(updated_rel.status, ClaimStatus::Approved);
    }

    /// Test: tm_commit updates existing truth slot.
    #[tokio::test]
    async fn test_tm_commit_updates_existing_slot() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let attr = make_test_attribute("test_space", subject_id.clone(), "age");
        let attr_id = attr.base.id.to_string();
        storage.add_attr(attr);
        let fixed_time = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
        let mut predicate =
            make_predicate_registry("test_space", "age", Some(PredicateSemantics::default()));
        predicate.base.updated_at = fixed_time;
        predicate.base.created_at = fixed_time;
        storage.add_predicate_registry(predicate);
        storage.add_predicate_alias(make_predicate_alias("test_space", "age", 5));
        let expected_semantics_version = timestamp_to_version(fixed_time);

        // Create initial slot
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, "hash");
        let initial_slot = {
            let id = RecordId::new_v4(TableName::TruthSlot);
            let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());
            TruthSlot {
                base,
                slot_key: slot_key.clone(),
                slot_type: "attribute".to_string(),
                subject_entity: subject_id.to_string(),
                predicate_id: "age".to_string(),
                current_values: vec![populate_current_value("old_claim", 0.5)],
                conflict_group: None,
                confidence: 0.5,
                rationale: None,
                semantics_version: 1,
                alias_version: 1,
                stale: true,
                stale_reason: Some("needs recompute".to_string()),
                computed_at: Utc::now(),
                security_hash: "hash".to_string(),
            }
        };
        storage.create_slot(initial_slot).await.unwrap();

        // Now commit a new resolution
        let resolution = ResolutionResult::SingleWinner {
            winner_id: attr_id.clone(),
            confidence: 0.95,
            rationale: "Updated resolution".to_string(),
        };

        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "attribute".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "age".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![attr_id.clone()],
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        assert_eq!(result.slots_updated, 1);

        // Verify slot was updated
        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .unwrap()
            .expect("slot should exist");

        assert_eq!(slot.current_values[0].claim_id, attr_id);
        assert_eq!(slot.confidence, 0.95);
        assert!(!slot.stale, "Should clear stale flag");
        assert_eq!(slot.semantics_version, expected_semantics_version);
        assert_eq!(slot.alias_version, 5);

        let updated_attr = storage
            .get_attribute("test_space", &RecordId::from_str(&attr_id).unwrap())
            .await
            .unwrap()
            .expect("attribute should exist");
        assert_eq!(updated_attr.status, ClaimStatus::Approved);
    }

    /// Test: tm_commit supersedes losing claim and updates validity windows.
    #[tokio::test]
    async fn test_tm_commit_supersedes_loser_updates_validity_window() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let mut rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );

        let earlier = Utc::now() - chrono::Duration::days(30);
        let later = Utc::now();
        rel1.base.created_at = earlier;
        rel2.base.created_at = later;
        for evidence in rel1.evidence.iter_mut() {
            evidence.time_value = earlier;
        }
        for evidence in rel2.evidence.iter_mut() {
            evidence.time_value = later;
        }

        let id1 = rel1.base.id.to_string();
        let id2 = rel2.base.id.to_string();
        storage.add_rel(rel1);
        storage.add_rel(rel2);

        let resolution = ResolutionResult::SingleWinner {
            winner_id: id2.clone(),
            confidence: 0.9,
            rationale: "Supersede by recency".to_string(),
        };

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash",
        );

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![id1.clone(), id2.clone()],
            ..CommitInput::default()
        };

        let _ = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let loser = storage
            .get_rel("test_space", &RecordId::from_str(&id1).unwrap())
            .await
            .unwrap()
            .expect("loser rel should exist");
        let winner = storage
            .get_rel("test_space", &RecordId::from_str(&id2).unwrap())
            .await
            .unwrap()
            .expect("winner rel should exist");

        assert_eq!(loser.status, ClaimStatus::Superseded);
        assert_eq!(winner.status, ClaimStatus::Approved);

        assert_eq!(
            loser.validity_window.as_ref().and_then(|w| w.end),
            Some(later)
        );
        assert_eq!(
            winner.validity_window.as_ref().and_then(|w| w.start),
            Some(later)
        );
    }

    /// Test: tm_commit uses predicate recency basis for validity windows.
    #[tokio::test]
    async fn test_tm_commit_uses_recency_basis_for_windows() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let explicit_time = Utc::now() - chrono::Duration::days(30);
        let observed_time = Utc::now();

        let mut winner = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut loser = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );

        let mut winner_observed = make_test_evidence();
        winner_observed.time_basis = TimeBasis::ObservedAt;
        winner_observed.time_value = observed_time;
        let mut winner_explicit = make_test_evidence();
        winner_explicit.time_basis = TimeBasis::SourceEventTime;
        winner_explicit.time_value = explicit_time;
        winner.evidence = vec![winner_observed, winner_explicit];

        for evidence in loser.evidence.iter_mut() {
            evidence.time_basis = TimeBasis::ObservedAt;
            evidence.time_value = observed_time - chrono::Duration::days(10);
        }

        let winner_id = winner.base.id.to_string();
        let loser_id = loser.base.id.to_string();

        storage.add_rel(winner);
        storage.add_rel(loser);

        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "works_at",
            Some(PredicateSemantics {
                recency_basis: RecencyBasis::SourceEventTime,
                ..PredicateSemantics::default()
            }),
        ));

        let resolution = ResolutionResult::SingleWinner {
            winner_id: winner_id.clone(),
            confidence: 0.9,
            rationale: "explicit timestamp wins".to_string(),
        };

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash",
        );

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![loser_id.clone(), winner_id.clone()],
            ..CommitInput::default()
        };

        let _ = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let updated_winner = storage
            .get_rel("test_space", &RecordId::from_str(&winner_id).unwrap())
            .await
            .unwrap()
            .expect("winner rel should exist");
        let updated_loser = storage
            .get_rel("test_space", &RecordId::from_str(&loser_id).unwrap())
            .await
            .unwrap()
            .expect("loser rel should exist");

        assert_eq!(
            updated_winner
                .validity_window
                .as_ref()
                .and_then(|w| w.start),
            Some(explicit_time)
        );
        assert_eq!(
            updated_loser.validity_window.as_ref().and_then(|w| w.end),
            Some(explicit_time)
        );
    }

    /// Test: implicit-now claims are refined when explicit timestamps arrive.
    #[tokio::test]
    async fn test_tm_commit_refines_implicit_temporal_audit() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let implicit_time = Utc::now() - chrono::Duration::days(5);
        let explicit_time = Utc::now() - chrono::Duration::days(30);

        let implicit_assertion = RecordId::new_v4(TableName::Assertion);
        let explicit_assertion = RecordId::new_v4(TableName::Assertion);

        let mut implicit = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        let mut explicit = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );

        let mut implicit_evidence = make_test_evidence();
        implicit_evidence.assertion = Link::new(implicit_assertion.clone());
        implicit_evidence.time_basis = TimeBasis::ObservedAt;
        implicit_evidence.time_value = implicit_time;
        implicit.evidence = vec![implicit_evidence];

        let mut explicit_evidence = make_test_evidence();
        explicit_evidence.assertion = Link::new(explicit_assertion.clone());
        explicit_evidence.time_basis = TimeBasis::SourceEventTime;
        explicit_evidence.time_value = explicit_time;
        explicit.evidence = vec![explicit_evidence.clone()];

        let implicit_id = implicit.base.id.to_string();
        let explicit_id = explicit.base.id.to_string();

        storage.add_rel(implicit);
        storage.add_rel(explicit);

        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "works_at",
            Some(PredicateSemantics {
                refinement_policy: RefinementPolicy::RefineImplicit,
                ..PredicateSemantics::default()
            }),
        ));

        let resolution = ResolutionResult::SingleWinner {
            winner_id: explicit_id.clone(),
            confidence: 0.9,
            rationale: "explicit timestamp".to_string(),
        };

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "works_at",
            None,
            "hash",
        );

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "works_at".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![implicit_id.clone(), explicit_id.clone()],
            ..CommitInput::default()
        };

        let _ = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let updated_implicit = storage
            .get_rel("test_space", &RecordId::from_str(&implicit_id).unwrap())
            .await
            .unwrap()
            .expect("implicit rel should exist");

        let audit = updated_implicit
            .temporal_audit
            .as_ref()
            .expect("temporal_audit should be set");
        let refinement = audit
            .time_refinement
            .as_ref()
            .expect("time_refinement should be set");

        assert_eq!(audit.time_basis, RecencyBasis::SourceEventTime);
        assert_eq!(refinement.original_basis, RecencyBasis::ObservedAt);
        assert_eq!(refinement.original_value, implicit_time);
        assert_eq!(refinement.refined_by.id(), &explicit_assertion);
        assert_eq!(
            updated_implicit
                .validity_window
                .as_ref()
                .and_then(|w| w.start),
            Some(explicit_time)
        );
        assert!(updated_implicit.evidence.iter().any(|evidence| {
            evidence.time_basis == TimeBasis::SourceEventTime
                && evidence.time_value == explicit_time
        }));
    }

    /// Test: tm_commit handles NeedsReview - slot created with disputed values.
    #[tokio::test]
    async fn test_tm_commit_needs_review_creates_slot() {
        let storage = MockTruthMaintenanceStorage::new();

        let resolution = ResolutionResult::NeedsReview {
            reason: "Insufficient evidence".to_string(),
        };

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let rel1 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "disputed",
        );
        let rel2 = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "disputed",
        );
        let claim_ids = vec![rel1.base.id.to_string(), rel2.base.id.to_string()];
        storage.add_rel(rel1);
        storage.add_rel(rel2);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "disputed",
            None,
            "hash",
        );
        let group = create_conflict_group(&slot_key, "rel", claim_ids.clone(), "test_space");
        let _ = storage.create_conflict_group(group).await.unwrap();

        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "disputed".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: claim_ids.clone(),
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        assert_eq!(
            result.slots_updated, 1,
            "Should create slot for NeedsReview"
        );

        // Verify slot was created with disputed values
        let slot = storage
            .find_slot_by_key("test_space", &input.slot_key)
            .await
            .unwrap();
        let slot = slot.expect("slot should exist");
        assert_eq!(slot.current_values.len(), 2);
        assert!(slot.conflict_group.is_some(), "Should link conflict group");
        assert!(slot
            .current_values
            .iter()
            .all(|value| value.status == ClaimStatus::Disputed));

        let updated_rel1 = storage
            .get_rel("test_space", &RecordId::from_str(&claim_ids[0]).unwrap())
            .await
            .unwrap()
            .expect("rel1 should exist");
        let updated_rel2 = storage
            .get_rel("test_space", &RecordId::from_str(&claim_ids[1]).unwrap())
            .await
            .unwrap()
            .expect("rel2 should exist");
        assert_eq!(updated_rel1.status, ClaimStatus::Disputed);
        assert_eq!(updated_rel2.status, ClaimStatus::Disputed);
    }

    /// Test: RequireMoreEvidence orders current values by confidence in NeedsReview.
    #[tokio::test]
    async fn test_tm_commit_require_more_evidence_orders_by_confidence() {
        let storage = MockTruthMaintenanceStorage::new();

        let semantics = PredicateSemantics {
            conflict_policy: ConflictPolicy::RequireMoreEvidence,
            ..PredicateSemantics::default()
        };
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "disputed",
            Some(semantics),
        ));

        let resolution = ResolutionResult::NeedsReview {
            reason: "RequireMoreEvidence".to_string(),
        };

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let mut rel_low = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "disputed",
        );
        let mut rel_high = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "disputed",
        );
        rel_low.confidence = 0.2;
        rel_high.confidence = 0.9;
        let low_id = rel_low.base.id.to_string();
        let high_id = rel_high.base.id.to_string();
        storage.add_rel(rel_low);
        storage.add_rel(rel_high);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "disputed",
            None,
            "hash",
        );
        let group = create_conflict_group(
            &slot_key,
            "rel",
            vec![low_id.clone(), high_id.clone()],
            "test_space",
        );
        let _ = storage.create_conflict_group(group).await.unwrap();

        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "disputed".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![low_id.clone(), high_id.clone()],
            ..CommitInput::default()
        };

        let _ = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let slot = storage
            .find_slot_by_key("test_space", &input.slot_key)
            .await
            .unwrap()
            .expect("slot should exist");

        assert_eq!(slot.current_values.len(), 2);
        assert_eq!(slot.current_values[0].claim_id, high_id);
        assert_eq!(slot.current_values[1].claim_id, low_id);
        assert!(
            slot.current_values[0].confidence >= slot.current_values[1].confidence,
            "RequireMoreEvidence should rank by confidence"
        );
    }

    /// Test: tm_commit handles ParallelValues.
    #[tokio::test]
    async fn test_tm_commit_parallel_values() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let semantics = PredicateSemantics {
            promotion_threshold: 0.7,
            ..PredicateSemantics::default()
        };
        let predicate = make_predicate_registry("test_space", "roles", Some(semantics));
        storage.add_predicate_registry(predicate);

        let rel1 = make_test_rel("test_space", subject_id.clone(), object_id.clone(), "roles");
        let rel2 = make_test_rel("test_space", subject_id.clone(), object_id.clone(), "roles");
        let rel3 = make_test_rel("test_space", subject_id.clone(), object_id.clone(), "roles");
        let id1 = rel1.base.id.to_string();
        let id2 = rel2.base.id.to_string();
        let id3 = rel3.base.id.to_string();
        storage.add_rel(rel1);
        storage.add_rel(rel2);
        storage.add_rel(rel3);

        let resolution = ResolutionResult::ParallelValues {
            ranked_values: vec![(id1.clone(), 0.9), (id2.clone(), 0.8), (id3.clone(), 0.7)],
            rationale: "AllowParallel policy".to_string(),
        };

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "roles",
            None,
            "hash",
        );

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "roles".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![id1.clone(), id2.clone(), id3.clone()],
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        assert_eq!(result.slots_updated, 1);

        let slot = storage
            .find_slot_by_key("test_space", &input.slot_key)
            .await
            .unwrap()
            .expect("slot should exist");

        assert_eq!(slot.current_values.len(), 3, "Should have all values");
        assert_eq!(slot.confidence, 0.9, "Should use top confidence");
        assert!(slot
            .current_values
            .iter()
            .all(|value| value.status == ClaimStatus::Approved));
    }

    /// Test: tm_commit enforces promotion thresholds.
    #[tokio::test]
    async fn test_tm_commit_blocks_promotion_when_thresholds_not_met() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let semantics = PredicateSemantics {
            min_evidence: 2,
            promotion_threshold: 0.9,
            ..PredicateSemantics::default()
        };
        let predicate = make_predicate_registry("test_space", "threshold", Some(semantics));
        storage.add_predicate_registry(predicate);

        let mut rel = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "threshold",
        );
        rel.confidence = 0.6;
        let rel_id = rel.base.id.to_string();
        storage.add_rel(rel);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "threshold",
            None,
            "hash",
        );
        let group = create_conflict_group(&slot_key, "rel", vec![rel_id.clone()], "test_space");
        storage.create_conflict_group(group).await.unwrap();

        let resolution = ResolutionResult::SingleWinner {
            winner_id: rel_id.clone(),
            confidence: 0.6,
            rationale: "Low confidence".to_string(),
        };

        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "threshold".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![rel_id.clone()],
            ..CommitInput::default()
        };

        let _ = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let updated_rel = storage
            .get_rel("test_space", &RecordId::from_str(&rel_id).unwrap())
            .await
            .unwrap()
            .expect("rel should exist");
        assert_eq!(updated_rel.status, ClaimStatus::Disputed);

        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .unwrap()
            .expect("slot should exist");
        assert!(slot
            .current_values
            .iter()
            .all(|value| value.status == ClaimStatus::Disputed));
        assert!(slot.conflict_group.is_some(), "Should keep conflict group");

        let unresolved = storage
            .find_unresolved_conflict("test_space", &slot_key)
            .await
            .unwrap();
        assert!(
            unresolved.is_some(),
            "Conflict group should remain unresolved"
        );
    }

    /// Test: tm_commit writes audit logs for updates.
    #[tokio::test]
    async fn test_tm_commit_writes_audit_logs() {
        let storage = MockTruthMaintenanceStorage::new();

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let rel = make_test_rel("test_space", subject_id.clone(), object_id.clone(), "audit");
        let rel_id = rel.base.id.to_string();
        storage.add_rel(rel);

        let predicate =
            make_predicate_registry("test_space", "audit", Some(PredicateSemantics::default()));
        storage.add_predicate_registry(predicate);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            Some(&object_id.to_string()),
            "audit",
            None,
            "hash",
        );

        let resolution = ResolutionResult::SingleWinner {
            winner_id: rel_id.clone(),
            confidence: 0.9,
            rationale: "Audit logs".to_string(),
        };

        let input = CommitInput {
            slot_key,
            slot_type: "rel".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "audit".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![rel_id],
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit should succeed");

        let logs = storage.audit_logs.lock().unwrap();
        assert_eq!(logs.len(), result.audit_entries);
        assert_eq!(result.audit_entries, 2);
        assert!(logs.iter().any(|log| {
            log.record.table() == TableName::Rel && log.action == AuditAction::Update
        }));
        assert!(logs.iter().any(|log| {
            log.record.table() == TableName::TruthSlot && log.action == AuditAction::Create
        }));
    }

    // =========================================================================
    // Value Groups, Claim-Set Hash, Slot Keys
    // =========================================================================

    fn make_evidence_assertion(
        subject: &str,
        predicate: &str,
        object: &str,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Assertion {
        use elephant_types::records::assertion::{
            AssertionRetryState, EvidenceSpan, ResolutionStatus,
        };

        let observed = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        Assertion {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Assertion),
                "space:test".to_string(),
                "test".to_string(),
            ),
            subject_text: subject.to_string(),
            predicate_raw: predicate.to_string(),
            predicate_id: Some(predicate.to_string()),
            object_text: Some(object.to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 10,
                quote_hash: "hash".to_string(),
            },
            source_event_time,
            observed_at: observed,
            asserted_at: observed,
            assertion_key: format!("{subject}:{predicate}:{object}"),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 0.9,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        }
    }

    #[test]
    fn test_claim_timestamp_basis_reflects_actual_fallback() {
        // Semantics configure source_event_time first, but this claim's
        // evidence only carries observed_at — the label must say so.
        let semantics = PredicateSemantics {
            recency_basis: RecencyBasis::SourceEventTime,
            fallback_order: vec![RecencyBasis::ObservedAt, RecencyBasis::AssertedAt],
            ..PredicateSemantics::default()
        };
        let ts = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let subject = RecordId::new_v4(TableName::Entity);
        let object = RecordId::new_v4(TableName::Entity);
        let mut rel = make_test_rel("space:test", subject.clone(), object.clone(), "works_at");
        let mut evidence = make_test_evidence();
        evidence.time_basis = TimeBasis::ObservedAt;
        evidence.time_value = ts;
        rel.evidence = vec![evidence];
        let record = ClaimRecord::Rel(rel);
        let (got_ts, basis) = claim_timestamp_and_basis_for_semantics(&record, Some(&semantics));
        assert_eq!(got_ts, ts);
        assert_eq!(basis, "observed_at");

        // No evidence at all: created_at fallback is labeled honestly.
        let mut bare = make_test_rel("space:test", subject, object, "works_at");
        bare.evidence = Vec::new();
        let record = ClaimRecord::Rel(bare);
        let (_, bare_basis) = claim_timestamp_and_basis_for_semantics(&record, Some(&semantics));
        assert_eq!(bare_basis, "record_created_at");
    }

    #[test]
    fn test_render_assertion_note_time_honesty() {
        let dated = make_evidence_assertion(
            "Alice",
            "works_at",
            "Blizzard Entertainment",
            Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
        );
        let note = render_assertion_note(&dated);
        assert!(note.contains("Alice works_at Blizzard Entertainment"));
        assert!(note.contains("stated fact time 2024-01-01"));

        let undated = make_evidence_assertion("Alice", "works_at", "King", None);
        let note = render_assertion_note(&undated);
        assert!(note.contains("statement date unknown"));
        assert!(
            !note.contains("2026"),
            "ingestion dates must not leak: {note}"
        );
    }

    #[test]
    fn test_build_adjudication_input_carries_per_claim_basis_and_notes() {
        let group = make_conflict_group("slot", vec!["claim:1", "claim:2"]);
        let mut older = make_claim(
            "claim:1",
            0.9,
            Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap(),
        );
        older.time_basis = "record_created_at";
        let mut newer = make_claim(
            "claim:2",
            0.85,
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        );
        newer.time_basis = "source_event_time";
        let mut notes = HashMap::new();
        notes.insert(
            "claim:2".to_string(),
            vec!["\"Alice works_at Blizzard\" (stated fact time 2024-01-01)".to_string()],
        );

        let input = build_adjudication_input(
            &group,
            &[older, newer],
            "space:test",
            "Alice".to_string(),
            "works_at".to_string(),
            None,
            &notes,
        );

        assert_eq!(input.claims[0].time_basis, "record_created_at");
        assert_eq!(input.claims[1].time_basis, "source_event_time");
        assert!(input.claims[0].evidence_notes.is_empty());
        assert_eq!(input.claims[1].evidence_notes.len(), 1);

        let prompt = adjudicator::render_adjudication_prompt(&input);
        // The undated claim's ingestion timestamp must be withheld entirely;
        // the dated claim's fact time renders with its basis.
        assert!(prompt.contains("fact_time: unknown"));
        assert!(!prompt.contains("2026-07-02"), "ingestion date leaked");
        assert!(prompt.contains("(basis: source_event_time)"));
        assert!(prompt.contains("stated fact time 2024-01-01"));
    }

    #[test]
    fn test_partition_value_groups_literals() {
        let same = vec![
            ClaimValue::Literal(Literal::String("NYC".to_string())),
            ClaimValue::Literal(Literal::String("NYC".to_string())),
        ];
        assert_eq!(partition_value_groups(&same).len(), 1);

        let different = vec![
            ClaimValue::Literal(Literal::Integer(35)),
            ClaimValue::Literal(Literal::Integer(36)),
        ];
        assert_eq!(partition_value_groups(&different).len(), 2);

        let floats = vec![
            ClaimValue::Literal(Literal::Float(1.5)),
            ClaimValue::Literal(Literal::Float(1.5)),
        ];
        assert_eq!(partition_value_groups(&floats).len(), 1);

        // NaN never equals itself: distinct groups route to adjudication.
        let nans = vec![
            ClaimValue::Literal(Literal::Float(f64::NAN)),
            ClaimValue::Literal(Literal::Float(f64::NAN)),
        ];
        assert_eq!(partition_value_groups(&nans).len(), 2);

        let json = vec![
            ClaimValue::Literal(Literal::Json(serde_json::json!({"a": 1, "b": 2}))),
            ClaimValue::Literal(Literal::Json(serde_json::json!({"b": 2, "a": 1}))),
        ];
        assert_eq!(partition_value_groups(&json).len(), 1, "map order ignored");

        let refs = vec![
            ClaimValue::EntityRef {
                id: "entity:1".to_string(),
                display: "King".to_string(),
            },
            ClaimValue::EntityRef {
                id: "entity:1".to_string(),
                display: "King".to_string(),
            },
            ClaimValue::EntityRef {
                id: "entity:2".to_string(),
                display: "Blizzard".to_string(),
            },
        ];
        assert_eq!(partition_value_groups(&refs).len(), 2);
    }

    #[test]
    fn test_compute_claim_set_hash_order_insensitive() {
        let ab = compute_claim_set_hash(&["a".to_string(), "b".to_string()]);
        let ba = compute_claim_set_hash(&["b".to_string(), "a".to_string()]);
        let a = compute_claim_set_hash(&["a".to_string()]);

        assert_eq!(ab, ba);
        assert_ne!(ab, a);
        assert_eq!(ab.len(), 64);
    }

    #[test]
    fn test_rel_set_slot_key_non_collision() {
        let subject = RecordId::new_v4(TableName::Entity).to_string();
        let object = RecordId::new_v4(TableName::Entity).to_string();
        let predicate = "works_at";
        let hash = "hash1";

        let rel_key = compute_slot_key(&subject, Some(&object), predicate, None, hash);
        let attr_key = compute_slot_key(&subject, None, predicate, Some("ctx"), hash);
        let rel_set_key = compute_rel_set_slot_key(&subject, predicate, hash);

        assert_ne!(rel_set_key, rel_key);
        assert_ne!(rel_set_key, attr_key);

        let parts = parse_slot_key(&rel_set_key, "rel_set").expect("rel_set key parses");
        assert_eq!(parts.subject_id.to_string(), subject);
        assert_eq!(parts.predicate_id, predicate);
        assert!(parts.object_id.is_none());

        assert!(
            parse_slot_key(&rel_key, "rel_set").is_err(),
            "rel keys must not parse as rel_set"
        );
    }

    // =========================================================================
    // rel_set Detection Matrix
    // =========================================================================

    struct RelSetFixture {
        storage: MockTruthMaintenanceStorage,
        subject_id: RecordId,
        rel_set_key: String,
        rel_events: Vec<RelEventInfo>,
    }

    /// Two rels from one subject to two distinct objects via `works_at`.
    fn rel_set_fixture() -> RelSetFixture {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_a = RecordId::new_v4(TableName::Entity);
        let object_b = RecordId::new_v4(TableName::Entity);

        let rel_a = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_a.clone(),
            "works_at",
        );
        let rel_b = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_b.clone(),
            "works_at",
        );
        let security_hash = compute_security_hash(&rel_a.base.security);
        let rel_events = vec![RelEventInfo {
            rel_id: rel_a.base.id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_a,
            rel_type: "works_at".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        }];
        storage.add_rel(rel_a);
        storage.add_rel(rel_b);

        let rel_set_key =
            compute_rel_set_slot_key(&subject_id.to_string(), "works_at", &security_hash);

        RelSetFixture {
            storage,
            subject_id,
            rel_set_key,
            rel_events,
        }
    }

    #[tokio::test]
    async fn test_rel_set_detection_cardinality_one_creates_group() {
        let fixture = rel_set_fixture();
        fixture
            .storage
            .add_predicate_registry(make_predicate_registry(
                "test_space",
                "works_at",
                Some(PredicateSemantics {
                    cardinality: Cardinality::One,
                    ..PredicateSemantics::default()
                }),
            ));

        let result = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            &fixture.storage,
        )
        .await
        .expect("detect");

        assert_eq!(result.conflicts_detected, 1);
        let group = fixture
            .storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .expect("rel_set group exists");
        assert_eq!(group.claim_ids.len(), 2);
    }

    #[tokio::test]
    async fn test_rel_set_detection_missing_semantics_creates_group() {
        let fixture = rel_set_fixture();

        let result = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            &fixture.storage,
        )
        .await
        .expect("detect");

        assert_eq!(result.conflicts_detected, 1);
        assert!(fixture
            .storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn test_rel_set_detection_cardinality_many_skips() {
        let fixture = rel_set_fixture();
        fixture
            .storage
            .add_predicate_registry(make_predicate_registry(
                "test_space",
                "works_at",
                Some(PredicateSemantics {
                    cardinality: Cardinality::Many,
                    ..PredicateSemantics::default()
                }),
            ));

        let result = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            &fixture.storage,
        )
        .await
        .expect("detect");

        assert_eq!(result.conflicts_detected, 0);
        assert!(fixture
            .storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_rel_set_detection_event_like_skips() {
        let fixture = rel_set_fixture();
        fixture
            .storage
            .add_predicate_registry(make_predicate_registry(
                "test_space",
                "works_at",
                Some(PredicateSemantics {
                    temporal_model: TemporalPersonality::EventLike,
                    cardinality: Cardinality::One,
                    ..PredicateSemantics::default()
                }),
            ));

        let result = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            &fixture.storage,
        )
        .await
        .expect("detect");

        assert_eq!(result.conflicts_detected, 0);
        assert!(fixture
            .storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .is_none());
    }

    /// rel_set resolution + commit: adjudicated winner supersedes the other
    /// object's claim, and per-object slots are marked stale afterwards.
    #[tokio::test]
    async fn test_rel_set_resolution_and_commit_with_adjudicator() {
        let fixture = rel_set_fixture();
        let storage = &fixture.storage;

        let result = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            storage,
        )
        .await
        .expect("detect");
        assert_eq!(result.conflicts_detected, 1);
        let group = storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .expect("rel_set group");

        let mut claim_ids = group.claim_ids.clone();
        claim_ids.sort();
        let winner_id = claim_ids[1].clone();
        let loser_id = claim_ids[0].clone();

        let mut scripted = verdict(AdjudicationDecision::Supersede);
        scripted.winner_id = Some(winner_id.clone());
        scripted.superseded = vec![adjudicator::SupersededClaim {
            claim_id: loser_id.clone(),
            valid_until: Some(Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap()),
        }];
        scripted.proposed_semantics = Some(adjudicator::ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let (resolution, input) =
            resolve_conflict_group_with_storage(&ctx, &group, "test_space", storage)
                .await
                .expect("resolve rel_set group");
        assert_eq!(fake.call_count(), 1);
        assert_eq!(input.slot_type, "rel_set");
        assert!(input.adjudicated);
        assert!(matches!(resolution, ResolutionResult::SingleWinner { .. }));

        execute_tm_commit_with_storage(&resolution, &input, "test_space", storage)
            .await
            .expect("commit rel_set");

        let winner = storage
            .get_rel("test_space", &RecordId::from_str(&winner_id).unwrap())
            .await
            .unwrap()
            .expect("winner");
        assert_eq!(winner.status, ClaimStatus::Approved);
        let loser = storage
            .get_rel("test_space", &RecordId::from_str(&loser_id).unwrap())
            .await
            .unwrap()
            .expect("loser");
        assert_eq!(loser.status, ClaimStatus::Superseded);
        assert_eq!(
            loser.validity_window.as_ref().and_then(|w| w.end),
            Some(Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap()),
            "verdict date closes the loser window"
        );

        // Truth slot lives under the rel_set key.
        let slot = storage
            .find_slot_by_key("test_space", &fixture.rel_set_key)
            .await
            .unwrap()
            .expect("rel_set slot");
        assert_eq!(slot.slot_type, "rel_set");

        // Learned semantics persisted with LLM provenance.
        let registry = storage
            .find_predicate_registry_by_predicate_id("test_space", "works_at")
            .await
            .unwrap()
            .expect("registry row created");
        assert_eq!(registry.semantics_status, SemanticsStatus::LlmProposed);
        assert_eq!(
            registry.semantics.map(|s| s.cardinality),
            Some(Cardinality::One)
        );

        // Per-object slots recompute their views.
        let _ = fixture.subject_id;
    }

    // =========================================================================
    // Verdict Mapping
    // =========================================================================

    #[tokio::test]
    async fn test_verdict_corroborate_maps_to_corroborated_winner() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "city", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "city",
            Some(PredicateSemantics::default()),
        ));

        let mut scripted = verdict(AdjudicationDecision::Corroborate);
        scripted.equivalent_groups = vec![vec!["a".to_string(), "b".to_string()]];
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let claims = vec![
            make_claim_with_value("a", 0.9, ClaimValue::Literal(Literal::String("NYC".into()))),
            make_claim_with_value(
                "b",
                0.7,
                ClaimValue::Literal(Literal::String("New York City".into())),
            ),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["a".to_string(), "b".to_string()],
            "test_space",
        );

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
                .await
                .expect("resolve");

        assert!(extras.adjudicated);
        match result {
            ResolutionResult::CorroboratedWinner {
                winner_id,
                merged_claim_ids,
                ..
            } => {
                assert_eq!(winner_id, "a", "top-confidence claim wins");
                assert_eq!(merged_claim_ids, vec!["b".to_string()]);
            }
            other => panic!("Expected CorroboratedWinner, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_verdict_equivalence_merge_with_single_winner() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "city", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "city",
            Some(PredicateSemantics::default()),
        ));

        let mut scripted = verdict(AdjudicationDecision::SingleWinner);
        scripted.winner_id = Some("a".to_string());
        scripted.equivalent_groups = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["c".to_string()],
        ];
        scripted.superseded = vec![adjudicator::SupersededClaim {
            claim_id: "c".to_string(),
            valid_until: None,
        }];
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let claims = vec![
            make_claim_with_value("a", 0.9, ClaimValue::Literal(Literal::String("NYC".into()))),
            make_claim_with_value(
                "b",
                0.8,
                ClaimValue::Literal(Literal::String("New York City".into())),
            ),
            make_claim_with_value(
                "c",
                0.7,
                ClaimValue::Literal(Literal::String("Boston".into())),
            ),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            "test_space",
        );

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
                .await
                .expect("resolve");

        match result {
            ResolutionResult::SingleWinner { winner_id, .. } => assert_eq!(winner_id, "a"),
            other => panic!("Expected SingleWinner, got {other:?}"),
        }
        assert_eq!(
            extras.merged_claim_ids,
            vec!["b".to_string()],
            "winner-group dupes merge"
        );
        assert_eq!(extras.superseded_overrides.len(), 1);
        assert_eq!(extras.superseded_overrides[0].claim_id, "c");
    }

    #[tokio::test]
    async fn test_verdict_coexist_maps_to_parallel_values() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "hobby", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "hobby",
            Some(PredicateSemantics::default()),
        ));

        let fake = Arc::new(FakeAdjudicator::new(Some(verdict(
            AdjudicationDecision::Coexist,
        ))));
        let ctx = ctx_with(&fake);

        let claims = vec![
            make_claim_with_value(
                "a",
                0.7,
                ClaimValue::Literal(Literal::String("chess".into())),
            ),
            make_claim_with_value("b", 0.9, ClaimValue::Literal(Literal::String("go".into()))),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["a".to_string(), "b".to_string()],
            "test_space",
        );

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
                .await
                .expect("resolve");

        assert!(extras.adjudicated);
        match result {
            ResolutionResult::ParallelValues { ranked_values, .. } => {
                assert_eq!(ranked_values.len(), 2);
                assert_eq!(ranked_values[0].0, "b", "ranked by confidence");
            }
            other => panic!("Expected ParallelValues, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_verdict_needs_review_maps_and_memoizes() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "age",
            Some(PredicateSemantics::default()),
        ));

        let fake = Arc::new(FakeAdjudicator::new(Some(verdict(
            AdjudicationDecision::NeedsReview,
        ))));
        let ctx = ctx_with(&fake);

        let claims = vec![
            make_claim_with_value("a", 0.9, ClaimValue::Literal(Literal::Integer(35))),
            make_claim_with_value("b", 0.9, ClaimValue::Literal(Literal::Integer(36))),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["a".to_string(), "b".to_string()],
            "test_space",
        );
        let group = storage.create_conflict_group(group).await.unwrap();

        let (result, _) = execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
            .await
            .expect("resolve");
        assert!(matches!(result, ResolutionResult::NeedsReview { .. }));
        assert_eq!(fake.call_count(), 1);

        // The verdict memoized on the group; an unchanged claim set skips the LLM.
        let refreshed = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group persisted");
        assert!(refreshed.adjudicated_claim_set_hash.is_some());
        assert!(refreshed.adjudicated_at.is_some());

        let (result, extras) =
            execute_tm_resolve_slot(&ctx, &refreshed, &claims, "test_space", &storage)
                .await
                .expect("resolve again");
        assert_eq!(fake.call_count(), 1, "memoization skips the adjudicator");
        assert!(extras.adjudicated);
        assert!(
            extras.skip_commit,
            "memoized replay must not produce a committable resolution"
        );
        assert!(matches!(result, ResolutionResult::NeedsReview { .. }));
    }

    /// Full supersede flow: verdict dates beat claim timestamps for windows.
    #[tokio::test]
    async fn test_verdict_supersede_commits_verdict_date_windows() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_old = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "employer",
            Literal::String("King".to_string()),
        );
        let attr_new = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "employer",
            Literal::String("Blizzard".to_string()),
        );
        let old_id = attr_old.base.id.to_string();
        let new_id = attr_new.base.id.to_string();
        let security_hash = compute_security_hash(&attr_old.base.security);
        storage.add_attr(attr_old);
        storage.add_attr(attr_new);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            None,
            "employer",
            None,
            &security_hash,
        );
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec![old_id.clone(), new_id.clone()],
            "test_space",
        );
        let group = storage.create_conflict_group(group).await.unwrap();

        let valid_until = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let valid_from = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let mut scripted = verdict(AdjudicationDecision::Supersede);
        scripted.winner_id = Some(new_id.clone());
        scripted.superseded = vec![adjudicator::SupersededClaim {
            claim_id: old_id.clone(),
            valid_until: Some(valid_until),
        }];
        scripted.winner_valid_from = Some(valid_from);
        scripted.proposed_semantics = Some(adjudicator::ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let (resolution, input) =
            resolve_conflict_group_with_storage(&ctx, &group, "test_space", &storage)
                .await
                .expect("resolve");
        assert!(input.adjudicated);
        assert_eq!(input.winner_valid_from, Some(valid_from));

        execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        let loser = storage
            .get_attribute("test_space", &RecordId::from_str(&old_id).unwrap())
            .await
            .unwrap()
            .expect("loser");
        assert_eq!(loser.status, ClaimStatus::Superseded);
        assert_eq!(
            loser.validity_window.as_ref().and_then(|w| w.end),
            Some(valid_until),
            "verdict valid_until beats claim timestamps"
        );

        let winner = storage
            .get_attribute("test_space", &RecordId::from_str(&new_id).unwrap())
            .await
            .unwrap()
            .expect("winner");
        assert_eq!(winner.status, ClaimStatus::Approved);
        assert_eq!(
            winner.validity_window.as_ref().and_then(|w| w.start),
            Some(valid_from),
            "verdict winner_valid_from sets the window start"
        );

        // Group resolved with the memo hash for detection skips.
        let refreshed = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group");
        assert!(refreshed.resolved);
        assert!(refreshed.adjudicated_claim_set_hash.is_some());

        // Learned semantics persisted and the stale-marking event emitted.
        let registry = storage
            .find_predicate_registry_by_predicate_id("test_space", "employer")
            .await
            .unwrap()
            .expect("registry created");
        assert_eq!(registry.semantics_status, SemanticsStatus::LlmProposed);
        assert!(storage
            .emitted_events()
            .iter()
            .any(|event| event.event_type == "predicate_registry.updated"));
    }

    /// Configured semantics are never overwritten by adjudication proposals.
    #[tokio::test]
    async fn test_verdict_proposal_never_overwrites_configured_semantics() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, "hash");
        storage.add_predicate_registry(make_predicate_registry(
            "test_space",
            "age",
            Some(PredicateSemantics {
                cardinality: Cardinality::Many,
                ..PredicateSemantics::default()
            }),
        ));

        let mut scripted = verdict(AdjudicationDecision::SingleWinner);
        scripted.winner_id = Some("a".to_string());
        scripted.proposed_semantics = Some(adjudicator::ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let claims = vec![
            make_claim_with_value("a", 0.9, ClaimValue::Literal(Literal::Integer(35))),
            make_claim_with_value("b", 0.8, ClaimValue::Literal(Literal::Integer(36))),
        ];
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec!["a".to_string(), "b".to_string()],
            "test_space",
        );

        let _ = execute_tm_resolve_slot(&ctx, &group, &claims, "test_space", &storage)
            .await
            .expect("resolve");

        let registry = storage
            .find_predicate_registry_by_predicate_id("test_space", "age")
            .await
            .unwrap()
            .expect("registry");
        assert_eq!(
            registry.semantics_status,
            SemanticsStatus::Configured,
            "Configured semantics survive"
        );
        assert_eq!(
            registry.semantics.map(|s| s.cardinality),
            Some(Cardinality::Many)
        );
        assert!(
            !storage
                .emitted_events()
                .iter()
                .any(|event| event.event_type == "predicate_registry.updated"),
            "no stale-marking event when the upsert was refused"
        );
    }

    // =========================================================================
    // Detection Memoization, Staleness Guard, Resurrect Guard, Self-Heal
    // =========================================================================

    #[tokio::test]
    async fn test_detection_memoization_skips_adjudicated_claim_set() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let security_hash = compute_security_hash(&attr1.base.security);
        let mut claim_ids = vec![attr1.base.id.to_string(), attr2.base.id.to_string()];
        claim_ids.sort();
        storage.add_attr(attr1.clone());
        storage.add_attr(attr2);

        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let mut resolved_group = build_unresolved_conflict_group(
            &slot_key,
            "attribute",
            claim_ids.clone(),
            "test_space",
        );
        resolved_group.resolved = true;
        resolved_group.resolved_at = Some(Utc::now());
        resolved_group.adjudicated_claim_set_hash = Some(compute_claim_set_hash(&claim_ids));
        resolved_group.adjudicated_at = Some(Utc::now());
        storage.create_conflict_group(resolved_group).await.unwrap();

        let attr_events = vec![AttributeEventInfo {
            attr_id: attr1.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash,
            deleted: false,
        }];

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &attr_events, "test_space", &storage)
                .await
                .expect("detect");

        assert_eq!(result.conflicts_detected, 0, "memoized: no reopen");
        assert_eq!(result.conflicts_updated, 0);
        assert!(result.conflict_group_ids.is_empty());
        assert!(
            storage
                .find_unresolved_conflict("test_space", &slot_key)
                .await
                .unwrap()
                .is_none(),
            "resolved group stays resolved"
        );
    }

    #[tokio::test]
    async fn test_commit_staleness_guard_no_ops_on_changed_claim_set() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let attr = make_test_attribute("test_space", subject_id.clone(), "age");
        let attr_id = attr.base.id.to_string();
        let ghost_id = RecordId::new_v4(TableName::Attribute).to_string();
        storage.add_attr(attr);

        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, "hash");
        let resolution = ResolutionResult::SingleWinner {
            winner_id: attr_id.clone(),
            confidence: 0.9,
            rationale: "test".to_string(),
        };
        let input = CommitInput {
            slot_key: slot_key.clone(),
            slot_type: "attribute".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "age".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![attr_id.clone(), ghost_id.clone()],
            // Hash computed over both ids, but ghost no longer exists.
            claim_set_hash: Some(compute_claim_set_hash(&[attr_id, ghost_id])),
            ..CommitInput::default()
        };

        let result = execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        assert!(result.skipped_stale);
        assert_eq!(result.slots_updated, 0);
        assert!(
            storage
                .find_slot_by_key("test_space", &slot_key)
                .await
                .unwrap()
                .is_none(),
            "no slot written for stale commit"
        );
    }

    #[tokio::test]
    async fn test_resurrect_guard_preserves_adjudicated_supersession() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let closed_end = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();

        let mut attr = make_test_attribute("test_space", subject_id.clone(), "employer");
        attr.status = ClaimStatus::Superseded;
        attr.validity_window = Some(RelValidityWindow {
            start: Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()),
            end: Some(closed_end),
        });
        let attr_id = attr.base.id.to_string();
        let security_hash = compute_security_hash(&attr.base.security);
        storage.add_attr(attr);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            None,
            "employer",
            None,
            &security_hash,
        );
        // A deterministic single-claim direct commit tries to re-approve it.
        let attr_events = vec![AttributeEventInfo {
            attr_id: RecordId::from_str(&attr_id).unwrap(),
            subject_id: subject_id.clone(),
            attr_type: "employer".to_string(),
            security_hash,
            deleted: false,
        }];
        let detect =
            execute_tm_detect_conflicts_with_storage(&[], &attr_events, "test_space", &storage)
                .await
                .expect("detect");
        assert_eq!(detect.direct_commits.len(), 1);
        let commit = &detect.direct_commits[0];
        assert!(!commit.input.adjudicated);

        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("commit");

        let preserved = storage
            .get_attribute("test_space", &RecordId::from_str(&attr_id).unwrap())
            .await
            .unwrap()
            .expect("attr exists");
        assert_eq!(
            preserved.status,
            ClaimStatus::Superseded,
            "adjudicated supersession preserved"
        );
        assert_eq!(
            preserved.validity_window.as_ref().and_then(|w| w.end),
            Some(closed_end),
            "closed window untouched"
        );

        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .unwrap()
            .expect("slot exists");
        assert!(slot
            .current_values
            .iter()
            .all(|value| value.status == ClaimStatus::Superseded));
    }

    /// D11 self-heal: corroboration re-opens a winner window that an old
    /// fake-conflict commit wrongly closed.
    #[tokio::test]
    async fn test_corroboration_reopens_wrongly_closed_winner_window() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        // Old fake-conflict damage: identical rels, one approved, one
        // superseded, the winner's window wrongly closed.
        let mut winner = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        winner.base.created_at = Utc::now() - chrono::Duration::days(2);
        winner.status = ClaimStatus::Approved;
        winner.validity_window = Some(RelValidityWindow {
            start: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            end: Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
        });
        let mut duplicate = make_test_rel(
            "test_space",
            subject_id.clone(),
            object_id.clone(),
            "works_at",
        );
        duplicate.base.created_at = Utc::now();
        duplicate.status = ClaimStatus::Superseded;
        duplicate.validity_window = Some(RelValidityWindow {
            start: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            end: Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
        });
        let winner_id = winner.base.id.clone();
        let duplicate_id = duplicate.base.id.clone();
        let security_hash = compute_security_hash(&winner.base.security);
        storage.add_rel(winner);
        storage.add_rel(duplicate);

        let rel_events = vec![RelEventInfo {
            rel_id: winner_id.clone(),
            subject_id: subject_id.clone(),
            object_id: object_id.clone(),
            rel_type: "works_at".to_string(),
            security_hash,
            deleted: false,
        }];
        let detect =
            execute_tm_detect_conflicts_with_storage(&rel_events, &[], "test_space", &storage)
                .await
                .expect("detect");
        assert_eq!(detect.direct_commits.len(), 1);
        let commit = &detect.direct_commits[0];

        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("commit");

        let healed = storage
            .get_rel("test_space", &winner_id)
            .await
            .unwrap()
            .expect("winner exists");
        assert_eq!(healed.status, ClaimStatus::Approved);
        assert_eq!(
            healed.validity_window.as_ref().and_then(|w| w.end),
            None,
            "wrongly closed window re-opened"
        );
        assert_eq!(healed.evidence.len(), 2, "duplicate evidence merged");

        assert!(
            storage
                .get_rel("test_space", &duplicate_id)
                .await
                .unwrap()
                .is_none(),
            "duplicate soft-deleted"
        );
    }

    // =========================================================================
    // Event Commit Fixes
    // =========================================================================

    #[tokio::test]
    async fn test_event_parallel_values_approves_all() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

        let event_a = make_test_event("test_space", "Standup", vec![subject_id.clone()], time, 0.9);
        let event_b = make_test_event("test_space", "Standup", vec![subject_id], time, 0.8);
        let a_id = event_a.base.id.to_string();
        let b_id = event_b.base.id.to_string();
        storage.add_event(event_a);
        storage.add_event(event_b);

        let resolution = ResolutionResult::ParallelValues {
            ranked_values: vec![(a_id.clone(), 0.9), (b_id.clone(), 0.8)],
            rationale: "coexist".to_string(),
        };
        let input = CommitInput {
            slot_key: "event-slot".to_string(),
            slot_type: "event".to_string(),
            subject_entity: "event_group".to_string(),
            predicate_id: "standup".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![a_id.clone(), b_id.clone()],
            ..CommitInput::default()
        };

        execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        for id in [&a_id, &b_id] {
            let event = storage
                .get_event("test_space", &RecordId::from_str(id).unwrap())
                .await
                .unwrap()
                .expect("event exists");
            assert_eq!(
                event.status,
                EventStatus::Approved,
                "coexist approves ALL ranked events"
            );
        }
    }

    #[tokio::test]
    async fn test_event_corroborated_winner_merges_and_soft_deletes() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);
        let time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

        let event_a = make_test_event("test_space", "Standup", vec![subject_id.clone()], time, 0.9);
        let event_b = make_test_event("test_space", "Standup", vec![subject_id], time, 0.8);
        let a_id = event_a.base.id.to_string();
        let b_id = event_b.base.id.to_string();
        storage.add_event(event_a);
        storage.add_event(event_b);

        let resolution = ResolutionResult::CorroboratedWinner {
            winner_id: a_id.clone(),
            confidence: 0.9,
            merged_claim_ids: vec![b_id.clone()],
            rationale: "duplicate event".to_string(),
        };
        let input = CommitInput {
            slot_key: "event-slot".to_string(),
            slot_type: "event".to_string(),
            subject_entity: "event_group".to_string(),
            predicate_id: "standup".to_string(),
            security_hash: "hash".to_string(),
            claim_ids: vec![a_id.clone(), b_id.clone()],
            adjudicated: true,
            ..CommitInput::default()
        };

        execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        let winner = storage
            .get_event("test_space", &RecordId::from_str(&a_id).unwrap())
            .await
            .unwrap()
            .expect("winner exists");
        assert_eq!(winner.status, EventStatus::Approved);
        assert_eq!(
            winner.assertion_refs.len(),
            2,
            "duplicate assertion refs merged"
        );

        assert!(
            storage
                .get_event("test_space", &RecordId::from_str(&b_id).unwrap())
                .await
                .unwrap()
                .is_none(),
            "duplicate event soft-deleted"
        );
    }

    #[tokio::test]
    async fn test_event_reject_floor_gated_on_adjudicated() {
        let subject_id = RecordId::new_v4(TableName::Entity);
        let time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

        for (adjudicated, expected_loser_status) in [
            (false, EventStatus::Rejected),
            (true, EventStatus::Superseded),
        ] {
            let storage = MockTruthMaintenanceStorage::new();
            let winner =
                make_test_event("test_space", "Standup", vec![subject_id.clone()], time, 0.9);
            // Below the default 0.2 reject floor.
            let loser = make_test_event(
                "test_space",
                "Standup",
                vec![subject_id.clone()],
                time,
                0.05,
            );
            let winner_id = winner.base.id.to_string();
            let loser_id = loser.base.id.to_string();
            storage.add_event(winner);
            storage.add_event(loser);

            let resolution = ResolutionResult::SingleWinner {
                winner_id: winner_id.clone(),
                confidence: 0.9,
                rationale: "winner".to_string(),
            };
            let input = CommitInput {
                slot_key: "event-slot".to_string(),
                slot_type: "event".to_string(),
                subject_entity: "event_group".to_string(),
                predicate_id: "standup".to_string(),
                security_hash: "hash".to_string(),
                claim_ids: vec![winner_id, loser_id.clone()],
                adjudicated,
                ..CommitInput::default()
            };

            execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
                .await
                .expect("commit");

            let loser = storage
                .get_event("test_space", &RecordId::from_str(&loser_id).unwrap())
                .await
                .unwrap()
                .expect("loser exists");
            assert_eq!(
                loser.status, expected_loser_status,
                "reject floor applies only to non-adjudicated commits (adjudicated={adjudicated})"
            );
        }
    }

    // =========================================================================
    // Review-Round Fixes
    // =========================================================================

    /// A retried tm_resolve_slot after an adjudication must not clobber the
    /// committed verdict with Disputed statuses: the memoized replay carries
    /// skip_commit and the commit executor no-ops on it.
    #[tokio::test]
    async fn test_memoized_replay_does_not_clobber_committed_verdict() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_old = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr_new = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let old_id = attr_old.base.id.to_string();
        let new_id = attr_new.base.id.to_string();
        let security_hash = compute_security_hash(&attr_old.base.security);
        storage.add_attr(attr_old);
        storage.add_attr(attr_new);

        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let group = build_unresolved_conflict_group(
            &slot_key,
            "attribute",
            vec![old_id.clone(), new_id.clone()],
            "test_space",
        );
        let group = storage.create_conflict_group(group).await.unwrap();

        let mut scripted = verdict(AdjudicationDecision::SingleWinner);
        scripted.winner_id = Some(new_id.clone());
        scripted.proposed_semantics = Some(adjudicator::ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        // First resolve adjudicates and records the memo hash.
        let (resolution1, input1) =
            resolve_conflict_group_with_storage(&ctx, &group, "test_space", &storage)
                .await
                .expect("first resolve");
        assert!(!input1.skip_commit);

        // A duplicate/retried resolve item runs before the commit lands.
        let refreshed = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group persisted");
        let (resolution2, input2) =
            resolve_conflict_group_with_storage(&ctx, &refreshed, "test_space", &storage)
                .await
                .expect("second resolve");
        assert_eq!(
            fake.call_count(),
            1,
            "memoization skips the second LLM call"
        );
        assert!(input2.skip_commit, "memoized replay must not commit");

        // Commit the real verdict, then replay the memoized resolution.
        execute_tm_commit_with_storage(&resolution1, &input1, "test_space", &storage)
            .await
            .expect("verdict commit");
        let replay = execute_tm_commit_with_storage(&resolution2, &input2, "test_space", &storage)
            .await
            .expect("replay commit");
        assert_eq!(replay.slots_updated, 0, "replay is a no-op");

        let winner = storage
            .get_attribute("test_space", &RecordId::from_str(&new_id).unwrap())
            .await
            .unwrap()
            .expect("winner");
        assert_eq!(
            winner.status,
            ClaimStatus::Approved,
            "verdict survives replay"
        );
        let loser = storage
            .get_attribute("test_space", &RecordId::from_str(&old_id).unwrap())
            .await
            .unwrap()
            .expect("loser");
        assert_eq!(
            loser.status,
            ClaimStatus::Superseded,
            "verdict survives replay"
        );

        let slot = storage
            .find_slot_by_key("test_space", &slot_key)
            .await
            .unwrap()
            .expect("slot exists");
        assert!(
            slot.current_values
                .iter()
                .all(|value| value.status != ClaimStatus::Disputed),
            "replay must not clobber the slot to Disputed"
        );
    }

    /// Two events touching the same slot in one detection batch produce one
    /// conflict group id, not two racing tm_resolve_slot enqueues.
    #[tokio::test]
    async fn test_detection_dedupes_group_ids_per_batch() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let security_hash = compute_security_hash(&attr1.base.security);
        let make_event =
            |attr: &elephant_types::records::attribute::Attribute| AttributeEventInfo {
                attr_id: attr.base.id.clone(),
                subject_id: subject_id.clone(),
                attr_type: "age".to_string(),
                security_hash: security_hash.clone(),
                deleted: false,
            };
        let attr_events = vec![make_event(&attr1), make_event(&attr2)];
        storage.add_attr(attr1);
        storage.add_attr(attr2);

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &attr_events, "test_space", &storage)
                .await
                .expect("detect");

        assert_eq!(result.conflict_group_ids.len(), 1, "one resolve per group");
        assert_eq!(result.conflicts_detected, 1);
        assert_eq!(result.conflicts_updated, 0);
    }

    /// D4 needs_review cooldown: an UNRESOLVED group whose adjudicated claim
    /// set is unchanged is memoized on detection touch (no reopen, no enqueue).
    #[tokio::test]
    async fn test_unresolved_group_memo_cooldown_on_detection_touch() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let security_hash = compute_security_hash(&attr1.base.security);
        let claim_ids = vec![attr1.base.id.to_string(), attr2.base.id.to_string()];
        let attr_event = AttributeEventInfo {
            attr_id: attr1.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        };
        storage.add_attr(attr1);
        storage.add_attr(attr2);

        // Simulate a needs_review adjudication: unresolved group + memo hash.
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let mut group = build_unresolved_conflict_group(
            &slot_key,
            "attribute",
            claim_ids.clone(),
            "test_space",
        );
        group.adjudicated_claim_set_hash = Some(compute_claim_set_hash(&claim_ids));
        group.adjudicated_at = Some(Utc::now());
        let group = storage.create_conflict_group(group).await.unwrap();

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &[attr_event], "test_space", &storage)
                .await
                .expect("detect");

        assert_eq!(result.conflicts_detected, 0, "cooldown: no reopen");
        assert_eq!(result.conflicts_updated, 0);
        assert!(result.conflict_group_ids.is_empty(), "no resolve enqueued");

        let untouched = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group exists");
        assert!(untouched.adjudicated_claim_set_hash.is_some());
        assert!(!untouched.resolved);
    }

    /// Re-upserting an unresolved group for a CHANGED claim set keeps the old
    /// memo hash on the record instead of wiping it.
    #[tokio::test]
    async fn test_unresolved_group_upsert_preserves_memo_hash() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr1 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(35),
        );
        let attr2 = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "age",
            Literal::Integer(36),
        );
        let security_hash = compute_security_hash(&attr1.base.security);
        let attr_event = AttributeEventInfo {
            attr_id: attr1.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash: security_hash.clone(),
            deleted: false,
        };
        storage.add_attr(attr1);
        storage.add_attr(attr2);

        // Memo hash belongs to an older, different claim set.
        let old_hash = compute_claim_set_hash(&["attribute:gone".to_string()]);
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let mut group = build_unresolved_conflict_group(
            &slot_key,
            "attribute",
            vec!["attribute:gone".to_string()],
            "test_space",
        );
        group.adjudicated_claim_set_hash = Some(old_hash.clone());
        group.adjudicated_at = Some(Utc::now());
        let group = storage.create_conflict_group(group).await.unwrap();

        let result =
            execute_tm_detect_conflicts_with_storage(&[], &[attr_event], "test_space", &storage)
                .await
                .expect("detect");

        assert_eq!(result.conflict_group_ids.len(), 1, "changed set re-opens");

        let reupserted = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group exists");
        assert_eq!(reupserted.claim_ids.len(), 2, "membership refreshed");
        assert_eq!(
            reupserted.adjudicated_claim_set_hash,
            Some(old_hash),
            "memo hash carried over, not wiped"
        );
    }

    /// Keyless rel_set lifecycle converges: commit lands a non-stale slot,
    /// the deterministic resolution memoizes on the group, and neither
    /// detection nor recompute re-opens it afterwards.
    #[tokio::test]
    async fn test_keyless_rel_set_converges_without_reopen() {
        let fixture = rel_set_fixture();
        let storage = &fixture.storage;

        let detect = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            storage,
        )
        .await
        .expect("detect");
        assert_eq!(detect.conflicts_detected, 1);
        let group = storage
            .find_unresolved_conflict_by_type("test_space", &fixture.rel_set_key, "rel_set")
            .await
            .unwrap()
            .expect("rel_set group");

        let (resolution, input) =
            resolve_conflict_group_with_storage(&TmContext::none(), &group, "test_space", storage)
                .await
                .expect("keyless resolve");
        assert!(matches!(
            resolution,
            ResolutionResult::ParallelValues { .. }
        ));
        assert!(!input.skip_commit);

        execute_tm_commit_with_storage(&resolution, &input, "test_space", storage)
            .await
            .expect("keyless commit");

        // The committed rel_set slot must land non-stale (per-object stale
        // marking runs before the final slot upsert).
        let slot = storage
            .find_slot_by_key("test_space", &fixture.rel_set_key)
            .await
            .unwrap()
            .expect("rel_set slot");
        assert!(!slot.stale, "rel_set slot must not re-mark itself stale");

        // The deterministic resolution memoized on the resolved group.
        let resolved = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group exists");
        assert!(resolved.resolved);
        assert!(
            resolved.adjudicated_claim_set_hash.is_some(),
            "deterministic commits memoize the decided claim set"
        );

        // A second detection touch with the unchanged claim set is memoized.
        let redetect = execute_tm_detect_conflicts_with_storage(
            &fixture.rel_events,
            &[],
            "test_space",
            storage,
        )
        .await
        .expect("re-detect");
        assert_eq!(redetect.conflicts_detected, 0, "no reopen");
        assert!(redetect.conflict_group_ids.is_empty());

        // Nothing is stale, so recompute is a no-op.
        let recompute = execute_tm_recompute(
            &TmContext::none(),
            "test_space",
            RecomputePriority::Normal,
            10,
            storage,
        )
        .await
        .expect("recompute");
        assert_eq!(recompute.stale_slots_found, 0, "keyless rel_set converged");
        assert!(recompute.pending_conflict_groups.is_empty());

        let still_resolved = storage
            .get_conflict_group("test_space", &group.base.id)
            .await
            .unwrap()
            .expect("group exists");
        assert!(still_resolved.resolved, "group stays resolved");
    }

    /// Crash safety: the winner's merged evidence is persisted before any
    /// duplicate is soft-deleted, so a retry after a partial commit still
    /// finds the live winner carrying all evidence.
    #[tokio::test]
    async fn test_corroboration_commit_persists_winner_before_deleting_duplicates() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let mut winner = make_test_attribute("test_space", subject_id.clone(), "age");
        winner.base.created_at = Utc::now() - chrono::Duration::days(1);
        let mut duplicate = make_test_attribute("test_space", subject_id.clone(), "age");
        duplicate.base.created_at = Utc::now();
        let winner_id = winner.base.id.to_string();
        let duplicate_id = duplicate.base.id.to_string();
        let security_hash = compute_security_hash(&winner.base.security);
        let attr_event = AttributeEventInfo {
            attr_id: winner.base.id.clone(),
            subject_id: subject_id.clone(),
            attr_type: "age".to_string(),
            security_hash,
            deleted: false,
        };
        storage.add_attr(winner);
        storage.add_attr(duplicate);

        let detect =
            execute_tm_detect_conflicts_with_storage(&[], &[attr_event], "test_space", &storage)
                .await
                .expect("detect");
        assert_eq!(detect.direct_commits.len(), 1);
        let commit = &detect.direct_commits[0];

        execute_tm_commit_with_storage(&commit.resolution, &commit.input, "test_space", &storage)
            .await
            .expect("commit");

        let ops = storage.ops.lock().unwrap().clone();
        let winner_update = ops
            .iter()
            .position(|op| op == &format!("update:{winner_id}"))
            .expect("winner persisted");
        let duplicate_delete = ops
            .iter()
            .position(|op| op == &format!("delete:{duplicate_id}"))
            .expect("duplicate soft-deleted");
        assert!(
            winner_update < duplicate_delete,
            "winner (merged evidence) must persist before the duplicate delete: {ops:?}"
        );
    }

    /// A superseded loser without its own valid_until closes at the verdict's
    /// winner_valid_from, not at the winner's ingest-derived timestamp.
    #[tokio::test]
    async fn test_supersede_loser_window_falls_back_to_winner_valid_from() {
        let storage = MockTruthMaintenanceStorage::new();
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_old = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "employer",
            Literal::String("King".to_string()),
        );
        let attr_new = make_test_attribute_with_value(
            "test_space",
            subject_id.clone(),
            "employer",
            Literal::String("Blizzard".to_string()),
        );
        let old_id = attr_old.base.id.to_string();
        let new_id = attr_new.base.id.to_string();
        let security_hash = compute_security_hash(&attr_old.base.security);
        storage.add_attr(attr_old);
        storage.add_attr(attr_new);

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            None,
            "employer",
            None,
            &security_hash,
        );
        let group = create_conflict_group(
            &slot_key,
            "attribute",
            vec![old_id.clone(), new_id.clone()],
            "test_space",
        );
        let group = storage.create_conflict_group(group).await.unwrap();

        let valid_from = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let mut scripted = verdict(AdjudicationDecision::Supersede);
        scripted.winner_id = Some(new_id.clone());
        // The values never state when the old employer ended: no valid_until.
        scripted.superseded = vec![adjudicator::SupersededClaim {
            claim_id: old_id.clone(),
            valid_until: None,
        }];
        scripted.winner_valid_from = Some(valid_from);
        scripted.proposed_semantics = Some(adjudicator::ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });
        let fake = Arc::new(FakeAdjudicator::new(Some(scripted)));
        let ctx = ctx_with(&fake);

        let (resolution, input) =
            resolve_conflict_group_with_storage(&ctx, &group, "test_space", &storage)
                .await
                .expect("resolve");
        execute_tm_commit_with_storage(&resolution, &input, "test_space", &storage)
            .await
            .expect("commit");

        let loser = storage
            .get_attribute("test_space", &RecordId::from_str(&old_id).unwrap())
            .await
            .unwrap()
            .expect("loser");
        assert_eq!(loser.status, ClaimStatus::Superseded);
        assert_eq!(
            loser.validity_window.as_ref().and_then(|w| w.end),
            Some(valid_from),
            "loser closes at winner_valid_from, not at ingest time"
        );
    }
}
