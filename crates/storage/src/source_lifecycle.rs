//! Source-scoped retraction and physical erasure on both storage backends.
//!
//! A lifecycle reservation uses the same accepted head and authorization snapshot
//! as intake. An independently durable, payload-free journal is written only
//! after that reservation succeeds. Erasure then drains old producers, removes
//! the complete dependency closure, persists the result, and completes the receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::str::FromStr;

use chrono::Utc;
use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceAuthorizationEnvelope, SourceHead,
    SourceVisibility,
};
use elephant_types::records::{BaseFields, Source, SystemEvent};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use surrealdb::sql::{Array, Thing, Value};
use uuid::Uuid;

use crate::canonical_commit::{
    check_source_fence, epoch_tx, CanonicalEffect, CanonicalPlan, CanonicalRecord,
    CanonicalRepository, SourceEffectSupport, SourceReplacementPlan,
};
use crate::erasure_journal::ErasureIntent;
use crate::source_publication::{
    authorization_snapshot_tx, check_authorized_snapshot, check_precondition, conflict,
    control_thing, control_value, decode_payload, encode_payload, head_key, integrity,
    operation_key, publication_error, read_control_tx, require_space, stable_key, write_control_tx,
    SourceMutation, SourcePublicationRepository, StoredOperation, HEAD_TABLE, OPERATION_TABLE,
};
use crate::{EmbeddedMutation, StorageResult};

#[path = "source_lifecycle_archive.rs"]
mod operational_archive;

/// Deliberate failures at durable erasure boundaries. They exercise the active path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErasureFault {
    /// Complete the operation normally.
    #[default]
    None,
    /// Reservation committed; no journal intent has yet been appended.
    AfterReservation,
    /// Independently durable journal intent exists; no payload was removed.
    AfterJournal,
    /// Shared Space admission is reserved; no reference membership was fsynced.
    AfterAdmissionReservation,
    /// Private reference membership is durable; payload is still present.
    AfterReferenceMembership,
    /// Abort inside the atomic payload transaction after its first deletion.
    DuringPurge,
    /// Payload transaction committed; persistent local state has not been replaced.
    AfterPurge,
    /// Purged local state persisted; the complete receipt has not been committed.
    AfterPersistence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationKind {
    Retract,
    Erase,
}

impl MutationKind {
    const fn lifecycle(self) -> SourceVisibility {
        match self {
            Self::Retract => SourceVisibility::Retracted,
            Self::Erase => SourceVisibility::Erasing,
        }
    }
    const fn state(self) -> OperationState {
        match self {
            Self::Retract => OperationState::Retracted,
            Self::Erase => OperationState::Erasing,
        }
    }
    const fn name(self) -> &'static str {
        match self {
            Self::Retract => "retract",
            Self::Erase => "erase",
        }
    }
}

#[derive(Clone)]
struct LifecycleReservation {
    before: SourceHead,
    after: SourceHead,
    operation: StoredOperation,
    prior_operation: StoredOperation,
    observed_operations: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct InventoryRow {
    table: String,
    key: String,
    native_id: Option<Thing>,
    record_id: Option<RecordId>,
    value: serde_json::Value,
    // Keep native composite-ID component types for original-journal comparisons.
    // The conservative JSON search projection is not an authoritative postimage.
    native_value: Option<Value>,
}

impl InventoryRow {
    fn contains(&self, ids: &BTreeSet<Uuid>) -> bool {
        contains_reference(&self.value, ids)
    }

    fn physical_key(&self) -> (String, String) {
        (self.table.clone(), self.key.clone())
    }

    fn reference_key(&self) -> String {
        self.native_id
            .as_ref()
            .map_or_else(|| self.key.clone(), normalized_native_key)
    }

    fn physical_identity(&self) -> StorageResult<Thing> {
        if let Some(identity) = &self.native_id {
            return Ok(identity.clone());
        }
        if let Some(identity) = &self.record_id {
            return Ok(Thing::from((
                self.table.as_str(),
                surrealdb::sql::Id::Uuid(identity.id().into()),
            )));
        }
        // Internal control tables use native string keys, while declared
        // extensions preserve their explicit typed object/number identity.
        crate::canonical_extension::embedded_physical_identity(&self.table, &self.value)
    }

    fn contains_key_reference(
        &self,
        journal: &crate::erasure_journal::ErasureJournal,
        space: &str,
        keys: &BTreeSet<String>,
        identity_tokens: &BTreeSet<String>,
    ) -> StorageResult<bool> {
        if contains_key_reference(&self.value, keys) {
            return Ok(true);
        }
        if self.native_value.is_none() {
            return Ok(false);
        }
        // Core/control display keys intentionally normalize UUID spelling. They
        // are not a reversible native identity encoding (nor always valid SQL).
        // Tokens come directly from the original physical Things instead.
        self.intersects_membership(journal, space, identity_tokens)
    }

    fn references_erased_identity(
        &self,
        journal: &crate::erasure_journal::ErasureJournal,
        space: &str,
        plan: &LifecyclePlan,
        historical_membership: &BTreeSet<String>,
    ) -> StorageResult<bool> {
        Ok(self.contains(&plan.removed)
            || self.contains_key_reference(
                journal,
                space,
                &plan.removed_keys,
                &plan.removed_identity_tokens,
            )?
            || self.intersects_membership(journal, space, historical_membership)?)
    }

    fn intersects_membership(
        &self,
        journal: &crate::erasure_journal::ErasureJournal,
        space: &str,
        memberships: &BTreeSet<String>,
    ) -> StorageResult<bool> {
        if memberships.is_empty() {
            return Ok(false);
        }
        let value = Value::Array(
            vec![
                Value::Thing(self.physical_identity()?),
                match &self.native_value {
                    Some(value) => value.clone(),
                    None => crate::surreal::json_to_surreal(self.value.clone())?,
                },
            ]
            .into(),
        );
        let references = crate::reference_fence::reference_tokens(
            journal.identity(),
            space,
            journal.reference_key_bytes(),
            &value,
        )?;
        Ok(!references.is_disjoint(memberships))
    }
}

#[derive(Default)]
struct LifecyclePlan {
    observed_epoch: String,
    archive_deletes: operational_archive::ArchiveSelection,
    deletes: Vec<InventoryRow>,
    replacements: Vec<CanonicalRecord>,
    removed: BTreeSet<Uuid>,
    removed_keys: BTreeSet<String>,
    removed_identity_tokens: BTreeSet<String>,
    media_assets: BTreeSet<RecordId>,
    support_deletes: Vec<InventoryRow>,
    inverse_effects: Vec<CanonicalEffect>,
    extension_effects: Vec<crate::canonical_extension::CanonicalExtensionEffect>,
    invalidation_deletes: Vec<InventoryRow>,
    support_updates: Vec<SourceEffectSupport>,
    original_supports: Vec<SourceEffectSupport>,
    support_continuations: Option<crate::source_contact_lineage::PreparedSupportContinuations>,
    contact_survival: Option<crate::source_contact_survival_proof::PreparedContactSurvival>,
    event_records: Vec<RecordId>,
}

/// Control authority survives payload erasure and contains no source document.
const RETAINED_CONTROL_TABLES: &[&str] = &[
    "source_head",
    "source_authorization",
    "source_identity",
    "source_operation",
    "source_tombstone_ref",
    "source_erasure_files",
    "source_erasure_journal_anchor",
    "source_erasure_guard",
    "source_erasure_membership",
    "canonical_epoch",
    "source_decision_sequence",
    "space_grant",
    "outbox_checkpoint",
];

/// Every typed table is enumerated, including cold archives and retained lifecycle tables.
const RECORD_TABLES: &[TableName] = &[
    TableName::Entity,
    TableName::Rel,
    TableName::Doc,
    TableName::MediaAsset,
    TableName::DocChunk,
    TableName::ChunkEmbedding,
    TableName::EntityIdentityKey,
    TableName::SystemEvent,
    TableName::Event,
    TableName::Source,
    TableName::AuditLog,
    TableName::IdMap,
    TableName::IdentityCandidate,
    TableName::RelEvidence,
    TableName::EntityMerge,
    TableName::SpaceConfig,
    TableName::DocRevision,
    TableName::DocNode,
    TableName::Assertion,
    TableName::Attribute,
    TableName::WorkItem,
    TableName::WorkItemArchive,
    TableName::WorkEdge,
    TableName::WorkRun,
    TableName::WorkRunArchive,
    TableName::WorkArtifact,
    TableName::WorkDeadletter,
    TableName::PredicateRegistry,
    TableName::PredicateAlias,
    TableName::ConflictGroup,
    TableName::TruthSlot,
    TableName::IngestDecision,
    TableName::CostLedgerDaily,
    TableName::Freshness,
    TableName::ReprocessJob,
    TableName::KnowledgeView,
    TableName::KnowledgeViewRevision,
    TableName::KnowledgeViewDefinition,
    TableName::KnowledgeViewDependency,
    TableName::SourceWatch,
    TableName::SourceWatchCheck,
];

impl SourcePublicationRepository {
    /// Prepares the retirement prefix for replacing support without withdrawing
    /// the source head. The canonical authority validates and applies this prefix
    /// together with the new revision's complete effects.
    pub(crate) async fn prepare_source_replacement(
        &self,
        space: &str,
        source: &PublicationFence,
    ) -> StorageResult<SourceReplacementPlan> {
        let lifecycle = self
            .prepare_lifecycle_plan(space, &source.source_id, false)
            .await?;
        let head = self
            .head(space, &source.source_id)
            .await?
            .ok_or_else(|| conflict("source_head_missing"))?;
        check_source_fence(&head, source)?;
        let source_record = CanonicalRecord::load(&self.client, space, &source.source_id)
            .await?
            .filter(|record| record.base().deleted_at.is_none())
            .ok_or_else(|| conflict("source_missing"))?;
        let mut plan = CanonicalPlan {
            observed_epoch: lifecycle.observed_epoch,
            dependencies: vec![source_record],
            extension_effects: lifecycle.extension_effects,
            ..CanonicalPlan::default()
        };
        for effect in lifecycle.inverse_effects {
            let row = effect
                .after
                .as_ref()
                .or(effect.before.as_ref())
                .ok_or_else(|| integrity("empty replacement inverse"))?;
            // Retained merges may have rewritten document subjects or other
            // payload references. Preserve those causal inverses when the row
            // survives, while acceptance remains authoritative for this source.
            // Exclusive raw payload deletion belongs to physical erasure.
            if row.base().id != source.source_id
                && (effect.after.is_some() || replacement_effect_table(row.base().id.table()))
            {
                plan.push(effect.before, effect.after)?;
            }
        }
        for mut after in lifecycle.replacements {
            // Recomputing the same retirement prefix must be deterministic.
            // The accepted head timestamp belongs to this original generation.
            if after.base().deleted_at.is_some() {
                after.base_mut().deleted_at = Some(head.updated_at);
            }
            after.base_mut().updated_at = head.updated_at;
            let id = &after.base().id;
            if !replacement_effect_table(id.table()) {
                continue;
            }
            let before = if let Some(effect) = plan.effects.iter().find(|effect| {
                effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .is_some_and(|record| record.base().id == *id)
            }) {
                effect.after.clone()
            } else {
                CanonicalRecord::load(&self.client, space, id).await?
            };
            plan.push(before, Some(after))?;
        }
        let mut control_deletes = BTreeSet::new();
        for row in lifecycle
            .support_deletes
            .into_iter()
            .chain(lifecycle.invalidation_deletes)
        {
            if let Some(id) = row.record_id {
                if !replacement_effect_table(id.table()) {
                    continue;
                }
                let before = if let Some(effect) = plan.effects.iter().find(|effect| {
                    effect
                        .after
                        .as_ref()
                        .or(effect.before.as_ref())
                        .is_some_and(|record| record.base().id == id)
                }) {
                    effect.after.clone()
                } else {
                    CanonicalRecord::load(&self.client, space, &id).await?
                };
                if let Some(before) = before {
                    plan.push(Some(before), None)?;
                }
            } else {
                control_deletes.insert((row.table, row.key));
            }
        }
        // Retain the first epoch: this is a consistency check, never a refresh
        // after preparation. The actual publication checks it again in its tx.
        if CanonicalRepository::new(self.client.clone())
            .epoch(space)
            .await?
            != plan.observed_epoch
        {
            return Err(conflict("canonical_epoch_changed"));
        }
        Ok(SourceReplacementPlan {
            plan,
            contact_survival: lifecycle.contact_survival,
            control_deletes: control_deletes.into_iter().collect(),
        })
    }

    /// Withdraws current support with source-head CAS and the authorized old envelope.
    pub async fn retract(&self, request: SourceMutation) -> StorageResult<OperationReceipt> {
        Box::pin(self.retract_inner(request, None)).await
    }

    /// Retracts only while the original head and all authorized documents match.
    /// Matching original receipts are read-only replays before document guards.
    pub async fn retract_with_document_guards(
        &self,
        request: SourceMutation,
        documents: &crate::source_document_guards::PreparedDocGuards,
    ) -> StorageResult<OperationReceipt> {
        Box::pin(self.retract_inner(request, Some(documents))).await
    }

    async fn retract_inner(
        &self,
        request: SourceMutation,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
    ) -> StorageResult<OperationReceipt> {
        if let Some(receipt) = self
            .lifecycle_replay(&request, MutationKind::Retract)
            .await?
        {
            return Ok(receipt);
        }
        let result: StorageResult<OperationReceipt> = async {
            if let Some(documents) = documents {
                documents.validate_scope(&request.space_id, &request.source_id)?;
            }
            let reservation = self
                .prepare_lifecycle(&request, MutationKind::Retract)
                .await?;
            let mut plan = self
                .prepare_lifecycle_plan(&request.space_id, &request.source_id, false)
                .await?;
            self.prepare_lifecycle_support_continuations(
                &request.space_id,
                &request.source_id,
                &reservation.after.operation_id,
                &mut plan,
                false,
            )
            .await?;
            self.commit_lifecycle_reservation(&request, &reservation, Some(&plan), documents)
                .await?;
            Ok(reservation.operation.receipt)
        }
        .await;
        match result {
            Ok(receipt) => Ok(receipt),
            Err(error) => self
                .lifecycle_replay(&request, MutationKind::Retract)
                .await?
                .ok_or(error),
        }
    }

    /// Physically removes this source incarnation and every exclusively derived representation.
    pub async fn erase_live(&self, request: SourceMutation) -> StorageResult<OperationReceipt> {
        self.erase_live_with_fault(request, ErasureFault::None)
            .await
    }

    /// The same erasure path with explicit durable-boundary fault injection.
    pub async fn erase_live_with_fault(
        &self,
        request: SourceMutation,
        fault: ErasureFault,
    ) -> StorageResult<OperationReceipt> {
        // Once the authorized mutation begins, a disconnected HTTP/MCP caller
        // must not cancel between a successful durable reservation and journal
        // append. The owned task retains the storage client through cleanup.
        let repository = self.clone();
        tokio::spawn(async move { Box::pin(repository.erase_live_owned(request, fault)).await })
            .await
            .map_err(|_| conflict("source_erasure_task_failed"))?
    }

    async fn erase_live_owned(
        &self,
        request: SourceMutation,
        fault: ErasureFault,
    ) -> StorageResult<OperationReceipt> {
        let (receipt, already_reserved) =
            if let Some(receipt) = self.lifecycle_replay(&request, MutationKind::Erase).await? {
                (receipt, true)
            } else {
                let reserve: StorageResult<OperationReceipt> = async {
                    let reservation = self
                        .prepare_lifecycle(&request, MutationKind::Erase)
                        .await?;
                    self.commit_lifecycle_reservation(&request, &reservation, None, None)
                        .await?;
                    Ok(reservation.operation.receipt)
                }
                .await;
                match reserve {
                    Ok(receipt) => (receipt, false),
                    Err(error) => (
                        self.lifecycle_replay(&request, MutationKind::Erase)
                            .await?
                            .ok_or(error)?,
                        true,
                    ),
                }
            };
        if receipt.state == OperationState::Erased || already_reserved {
            // The first owned task or startup recovery owns outstanding cleanup.
            // An identical concurrent request returns its durable pending receipt
            // instead of starting a second exclusive purge and poisoning readiness.
            return Ok(receipt);
        }
        if fault == ErasureFault::AfterReservation {
            self.client.block_on_incomplete_erasure();
            return Err(injected_fault());
        }
        let intent = match self
            .client
            .erasure_journal()?
            .record_intent(&request.space_id, &request.source_id)
        {
            Ok(intent) => intent,
            Err(error) => {
                self.client.block_on_incomplete_erasure();
                return Err(error);
            }
        };
        if fault == ErasureFault::AfterJournal {
            self.client.block_on_incomplete_erasure();
            return Err(injected_fault());
        }
        let space = request.space_id;
        let operation_id = receipt.operation_id;
        Box::pin(
            self.client
                .with_erasure_barrier(move |exclusive| async move {
                    let repository = Self::new(exclusive.clone());
                    Box::pin(repository.purge_all_reserved_sources(Some((&intent, fault)))).await?;
                    repository
                        .operation(&space, &operation_id)
                        .await?
                        .ok_or_else(|| integrity("erasure completion receipt missing"))
                }),
        )
        .await
    }

    /// Recovers the durable reservation-to-journal gap before storage becomes ready.
    pub async fn recover_pending_erasures(&self) -> StorageResult<()> {
        Box::pin(self.client.with_erasure_barrier(|exclusive| async move {
            let repository = Self::new(exclusive.clone());
            Box::pin(repository.purge_all_reserved_sources(None)).await
        }))
        .await
    }

    /// Replays an immutable journal identity against its old physical source ID.
    /// It never resolves the external-identity pointer to a later incarnation.
    pub async fn replay_erasure(
        &self,
        space: &str,
        source: &RecordId,
        intent_id: &str,
    ) -> StorageResult<()> {
        let intent = self
            .client
            .erasure_journal()?
            .intents()?
            .into_iter()
            .find(|intent| {
                intent.intent_id == intent_id
                    && intent.space_id == space
                    && intent.source_id == *source
            })
            .ok_or_else(|| integrity("erasure replay has no matching durable intent"))?;
        Box::pin(
            self.client
                .with_erasure_barrier(move |exclusive| async move {
                    Box::pin(
                        Self::new(exclusive)
                            .purge_all_reserved_sources(Some((&intent, ErasureFault::None))),
                    )
                    .await
                }),
        )
        .await
    }

    async fn lifecycle_replay(
        &self,
        request: &SourceMutation,
        kind: MutationKind,
    ) -> StorageResult<Option<OperationReceipt>> {
        let operation_id = operation_key(&request.space_id, &request.idempotency_key);
        let Some(operation) = self
            .read_control::<StoredOperation>(OPERATION_TABLE, &operation_id, &request.space_id)
            .await?
        else {
            return Ok(None);
        };
        if operation.receipt.source_id != request.source_id
            || operation.request_fingerprint != request.request_fingerprint
            || operation.input_fingerprint != mutation_fingerprint(request, kind)?
            || operation.authorization != request.authorized.envelope
        {
            return Err(conflict("idempotency_conflict"));
        }
        Ok(Some(operation.receipt))
    }

    async fn lifecycle_operation_snapshot(
        &self,
        space: &str,
        key: &str,
    ) -> StorageResult<(StoredOperation, String)> {
        let payload = if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| lifecycle_operation_payload_tx(tx, space, key))
                .await?
        } else {
            #[derive(Deserialize)]
            struct Payload {
                payload: String,
            }
            let mut response = self.client.query_bind_checked(
                "SELECT payload FROM $original_operation WHERE space_id = $space;",
                crate::bindings! { "original_operation" => control_thing(OPERATION_TABLE, key), "space" => space },
            ).await?;
            let rows: Vec<Payload> = response.take(0).map_err(|error| integrity(&error.to_string()))?;
            rows.into_iter().next().map(|row| row.payload)
        }
        .ok_or_else(|| integrity("source head operation missing"))?;
        Ok((decode_payload(&payload)?, payload))
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "OperationReceipt.source_generation must equal the observed SourceHead.generation before this lifecycle transition."
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "Lifecycle preparation captures authorization, references, inverse effects, and contact guards before accepting a durable reservation; preserve this phase ordering."
    )]
    async fn prepare_lifecycle(
        &self,
        request: &SourceMutation,
        kind: MutationKind,
    ) -> StorageResult<LifecycleReservation> {
        if request.space_id.trim().is_empty()
            || request.source_id.table() != TableName::Source
            || request.idempotency_key.trim().is_empty()
            || request.request_fingerprint.trim().is_empty()
            || request.actor.trim().is_empty()
        {
            return Err(integrity("invalid source mutation"));
        }
        let before = self
            .head(&request.space_id, &request.source_id)
            .await?
            .ok_or_else(|| conflict("source_not_found"))?;
        check_precondition(Some(&before), &request.expected)?;
        let observed = self
            .authorization_snapshot(&request.space_id, &request.source_id)
            .await?;
        check_authorized_snapshot(observed.as_ref(), Some(&request.authorized))?;
        if matches!(
            before.lifecycle,
            SourceVisibility::Erasing | SourceVisibility::Erased
        ) {
            return Err(conflict("source_erased"));
        }
        let now = Utc::now();
        let mut after = before.clone();
        after.generation = Uuid::new_v4().to_string();
        after.operation_id = operation_key(&request.space_id, &request.idempotency_key);
        after.lifecycle = kind.lifecycle();
        after.published_revision = None;
        after.updated_at = now;
        let receipt = OperationReceipt {
            space_id: request.space_id.clone(),
            operation_id: after.operation_id.clone(),
            source_id: request.source_id.clone(),
            source_generation: after.generation.clone(),
            desired_revision: after.desired_revision.clone(),
            published_revision: None,
            state: kind.state(),
            accepted_at: now,
            completed_at: (kind == MutationKind::Retract).then_some(now),
            error_code: None,
        };
        // Preserve original serialization, including absent defaulted fields,
        // for the transaction comparison; do not manufacture a new preimage.
        let (mut prior_operation, prior_snapshot) = self
            .lifecycle_operation_snapshot(&request.space_id, &before.operation_id)
            .await?;
        let prior_receipt = &prior_operation.receipt;
        if prior_receipt.space_id != request.space_id
            || prior_receipt.source_id != request.source_id
            || prior_receipt.operation_id != before.operation_id
            || prior_receipt.source_generation != before.generation
            || prior_receipt.desired_revision != before.desired_revision
        {
            return Err(conflict("source_head_operation_changed"));
        }
        let mut observed_operations = vec![(before.operation_id.clone(), prior_snapshot)];
        let predecessor_publication = if kind == MutationKind::Retract {
            let predecessor = match before.lifecycle {
                SourceVisibility::Active => {
                    if matches!(
                        prior_receipt.state,
                        OperationState::Retracted
                            | OperationState::Erasing
                            | OperationState::Erased
                    ) || prior_operation.predecessor_publication.is_some()
                    {
                        return Err(conflict("source_acceptance_operation_changed"));
                    }
                    PublicationFence {
                        source_id: before.source_id.clone(),
                        generation: before.generation.clone(),
                        revision_id: before
                            .desired_revision
                            .clone()
                            .ok_or_else(|| integrity("source acceptance revision missing"))?,
                        operation_id: before.operation_id.clone(),
                    }
                }
                SourceVisibility::Retracted => {
                    if prior_receipt.state != OperationState::Retracted {
                        return Err(conflict("retraction_operation_changed"));
                    }
                    let original = prior_operation
                        .predecessor_publication
                        .clone()
                        .ok_or_else(|| conflict("retraction_predecessor_unknown"))?;
                    if original.source_id != request.source_id
                        || Some(original.revision_id.as_str()) != before.desired_revision.as_deref()
                        || original.operation_id == before.operation_id
                        || original.generation == before.generation
                    {
                        return Err(conflict("retraction_predecessor_changed"));
                    }
                    let (accepted, snapshot) = self
                        .lifecycle_operation_snapshot(&request.space_id, &original.operation_id)
                        .await?;
                    if accepted.receipt.space_id != request.space_id
                        || accepted.receipt.source_id != original.source_id
                        || accepted.receipt.operation_id != original.operation_id
                        || accepted.receipt.source_generation != original.generation
                        || accepted.receipt.desired_revision.as_deref()
                            != Some(original.revision_id.as_str())
                        || matches!(
                            accepted.receipt.state,
                            OperationState::Retracted
                                | OperationState::Erasing
                                | OperationState::Erased
                        )
                        || accepted.predecessor_publication.is_some()
                    {
                        return Err(conflict("retraction_predecessor_changed"));
                    }
                    observed_operations.push((original.operation_id.clone(), snapshot));
                    original
                }
                SourceVisibility::Erasing | SourceVisibility::Erased => {
                    return Err(conflict("source_erased"));
                }
            };
            if predecessor.generation.trim().is_empty()
                || predecessor.operation_id.trim().is_empty()
                || predecessor.revision_id.trim().is_empty()
            {
                return Err(integrity("empty original acceptance identity"));
            }
            Some(predecessor)
        } else {
            None
        };
        let operation = StoredOperation {
            receipt,
            predecessor_publication,
            contact_digest: None,
            producer_origin: elephant_types::publication::SourceProducerOrigin::UnknownOriginal,
            request_fingerprint: request.request_fingerprint.clone(),
            input_fingerprint: mutation_fingerprint(request, kind)?,
            authorization: request.authorized.envelope.clone(),
        };
        if matches!(
            prior_operation.receipt.state,
            OperationState::Accepted | OperationState::Processing | OperationState::FailedRetryable
        ) {
            prior_operation.receipt.state = OperationState::Superseded;
            prior_operation.receipt.completed_at = Some(now);
        }
        Ok(LifecycleReservation {
            before,
            after,
            operation,
            prior_operation,
            observed_operations,
        })
    }
}

fn lifecycle_operation_payload_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    key: &str,
) -> StorageResult<Option<String>> {
    let physical_key = format!("{OPERATION_TABLE}:{key}");
    tx.scan_extension_rows(OPERATION_TABLE)?
        .into_iter()
        .find(|(key, row)| {
            key == &physical_key
                && row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
        })
        .map(|(_, row)| {
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| integrity("missing source operation payload"))
        })
        .transpose()
}

fn mutation_fingerprint(request: &SourceMutation, kind: MutationKind) -> StorageResult<String> {
    Ok(stable_key(&[
        &request.space_id,
        &request.source_id.to_string(),
        kind.name(),
        &encode_payload(&request.authorized.envelope)?,
    ]))
}

fn injected_fault() -> crate::StorageError {
    conflict("injected_source_erasure_fault")
}

fn contains_key_reference(value: &serde_json::Value, keys: &BTreeSet<String>) -> bool {
    if keys.is_empty() {
        return false;
    }
    match value {
        serde_json::Value::String(value) => {
            keys.contains(value)
                || (value.contains(':')
                    && surrealdb::sql::thing(value).is_ok_and(|id| {
                        id.to_string() == *value && keys.contains(&normalized_native_key(&id))
                    }))
                || ((value.trim_start().starts_with('{')
                    || value.trim_start().starts_with('[')
                    || value.trim_start().starts_with('"'))
                    && serde_json::from_str::<serde_json::Value>(value)
                        .is_ok_and(|value| contains_key_reference(&value, keys)))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| contains_key_reference(value, keys)),
        serde_json::Value::Object(values) => {
            let native = values
                .get("tb")
                .or_else(|| values.get("table"))
                .and_then(serde_json::Value::as_str)
                .filter(|_| values.contains_key("id"))
                .and_then(|table| {
                    crate::canonical_extension::embedded_extension_identity(table, value).ok()
                });
            native.is_some_and(|id| keys.contains(&normalized_native_key(&id)))
                || values.iter().any(|(key, value)| {
                    keys.contains(key)
                        || (key.contains(':')
                            && surrealdb::sql::thing(key).is_ok_and(|id| {
                                id.to_string() == *key && keys.contains(&normalized_native_key(&id))
                            }))
                        || contains_key_reference(value, keys)
                })
        }
        _ => false,
    }
}

fn contains_reference(value: &serde_json::Value, removed: &BTreeSet<Uuid>) -> bool {
    fn string_reference(value: &str, removed: &BTreeSet<Uuid>) -> bool {
        // Legacy control payloads also carry whole bare UUIDs. Preserve that
        // documented form without searching within prose, paths, or prefixes.
        if Uuid::parse_str(value).is_ok_and(|id| removed.contains(&id)) {
            return true;
        }
        if let Some(uuid) = value
            .strip_prefix("u'")
            .and_then(|value| value.strip_suffix("'"))
        {
            if Uuid::parse_str(uuid).is_ok_and(|id| removed.contains(&id)) {
                return true;
            }
        }
        if let Ok(id) = RecordId::from_str(value) {
            if removed.contains(&id.id()) {
                return true;
            }
        }
        if let Ok(id) = surrealdb::sql::thing(value) {
            if id.to_string() == value {
                // Opaque extension UUID-looking keys retain table/type identity.
                // Composite keys may contain a reference to a core record.
                match &id.id {
                    surrealdb::sql::Id::Array(values) => {
                        return contains_reference(
                            &inventory_json(Value::Array(values.clone())),
                            removed,
                        );
                    }
                    surrealdb::sql::Id::Object(values) => {
                        return contains_reference(
                            &inventory_json(Value::Object(values.clone())),
                            removed,
                        );
                    }
                    _ if crate::canonical_extension::validate_extension_table(&id.tb).is_err() => {
                        if uuid_from_key(value).is_some_and(|uuid| removed.contains(&uuid)) {
                            return true;
                        }
                    }
                    _ => {}
                }
            }
        }
        let first = value.trim_start().as_bytes().first().copied();
        matches!(first, Some(b'{' | b'[' | b'"'))
            && serde_json::from_str::<serde_json::Value>(value)
                .is_ok_and(|value| contains_reference(&value, removed))
    }
    match value {
        serde_json::Value::String(value) => string_reference(value, removed),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| contains_reference(value, removed)),
        serde_json::Value::Object(values) => {
            let declared = values
                .get("tb")
                .or_else(|| values.get("table"))
                .and_then(serde_json::Value::as_str);
            let opaque_identity = declared.is_some_and(|table| {
                crate::canonical_extension::validate_extension_table(table).is_ok()
            });
            values.iter().any(|(key, value)| {
                string_reference(key, removed)
                    || (!(opaque_identity && key == "id" && value.is_string())
                        && contains_reference(value, removed))
            })
        }
        _ => false,
    }
}

impl SourcePublicationRepository {
    /// Test-only physical row projection for independent lifecycle oracles.
    /// This bypasses eligibility views so absence assertions inspect actual payloads.
    #[cfg(test)]
    pub(crate) async fn erasure_inventory_rows(
        &self,
        space: &str,
    ) -> StorageResult<Vec<serde_json::Value>> {
        Ok(self
            .inventory(space)
            .await?
            .into_iter()
            .map(|row| row.value)
            .collect())
    }

    async fn inventory(&self, space: &str) -> StorageResult<Vec<InventoryRow>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let mut rows = Vec::new();
                    for table in RECORD_TABLES {
                        for (id, value) in tx.scan_raw_rows(*table)? {
                            if row_space(&value) != Some(space) {
                                continue;
                            }
                            rows.push(InventoryRow {
                                table: table.as_str().into(),
                                key: format!("{}:{}", table.as_str(), id.id()),
                                native_id: None,
                                record_id: Some(id),
                                value,
                                native_value: None,
                            });
                        }
                    }
                    for table in tx.extension_table_names() {
                        for (key, value) in tx.scan_extension_rows(&table)? {
                            if row_space(&value) != Some(space) {
                                continue;
                            }
                            // Keep the original physical key for deletion. Opaque
                            // identities can use object/numeric/unqualified JSON
                            // forms whose canonical native reference differs.
                            let native_id =
                                if crate::canonical_extension::validate_extension_table(&table)
                                    .is_ok()
                                {
                                    Some(crate::canonical_extension::embedded_extension_identity(
                                        &table, &value,
                                    )?)
                                } else {
                                    None
                                };
                            rows.push(InventoryRow {
                                table: table.clone(),
                                key,
                                native_id,
                                record_id: None,
                                value,
                                native_value: None,
                            });
                        }
                    }
                    Ok(rows)
                })
                .await;
        }
        let mut rows = Vec::new();
        for table in self.client.list_tables().await? {
            // Table identifiers are discovered from the database and still validated
            // before interpolation; tenant values are always bound parameters.
            if !table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(integrity("unsupported erasure inventory table identifier"));
            }
            let mut response = self
                .client
                .query_bind_checked(
                    &format!("SELECT * FROM {table} WHERE space_id = $space"),
                    crate::bindings! { "space" => space },
                )
                .await?;
            let value: surrealdb::Value = response
                .take(0)
                .map_err(|error| integrity(&error.to_string()))?;
            let Value::Array(values) = value.into_inner() else {
                return Err(integrity("source erasure inventory is not an array"));
            };
            for value in values.0 {
                let Value::Object(object) = &value else {
                    return Err(integrity("source erasure row is not an object"));
                };
                let Some(Value::Thing(id)) = object.get("id") else {
                    return Err(integrity("source erasure row has no native identity"));
                };
                let id = id.clone();
                let record_id = crate::surreal::record_id_from_thing(&id).ok();
                rows.push(InventoryRow {
                    table: table.clone(),
                    key: normalized_native_key(&id),
                    native_id: Some(id),
                    record_id,
                    value: inventory_json(value.clone()),
                    native_value: Some(value),
                });
            }
        }
        Ok(rows)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "The erasure plan derives canonical inverses, references, and dependent work from a single ordered observation; splitting its shared overlays risks changing deletion and survival precedence."
    )]
    async fn prepare_lifecycle_plan(
        &self,
        space: &str,
        source: &RecordId,
        physical: bool,
    ) -> StorageResult<LifecyclePlan> {
        let historical_membership = if physical {
            let journal = self.client.erasure_journal()?;
            let intent = journal
                .intents()?
                .into_iter()
                .find(|intent| intent.space_id == space && intent.source_id == *source)
                .ok_or_else(|| integrity("physical closure has no durable intent"))?;
            journal.reference_membership(&intent)?
        } else {
            BTreeSet::new()
        };
        let observed_epoch = CanonicalRepository::new(self.client.clone())
            .epoch(space)
            .await?;
        let invalidation =
            crate::source_decision::SourceDecisionRepository::new(self.client.clone())
                .invalidation(space, source)
                .await?;
        if invalidation.observed_epoch != observed_epoch {
            return Err(conflict("canonical_epoch_changed"));
        }
        let mut rows = self.inventory(space).await?;
        let original_rows = rows.clone();
        let retained_completions = retained_erasure_completions(space, &rows)?;
        // Build the physical dependency closure over the surviving causal state,
        // not the invalidated decision output. Otherwise erasing source B could
        // delete source A's baseline merely because B temporarily superseded it.
        let mut invalidation_deletes = Vec::new();
        for effect in &invalidation.effects {
            let Some(record) = effect.after.as_ref().or(effect.before.as_ref()) else {
                return Err(integrity("empty lifecycle decision inverse"));
            };
            let id = &record.base().id;
            if let Some(position) = rows.iter().position(|row| {
                row.table == id.table().as_str() && row.record_id.as_ref() == Some(id)
            }) {
                match &effect.after {
                    Some(after) => {
                        rows[position].value = canonical_row_json(after)?;
                        rows[position].native_value = None;
                    }
                    None => {
                        rows.remove(position);
                    }
                }
            } else if let Some(after) = &effect.after {
                rows.push(InventoryRow {
                    table: id.table().as_str().into(),
                    key: id.to_string(),
                    native_id: Some(crate::surreal::thing_from_record_id(id)),
                    record_id: Some(id.clone()),
                    value: canonical_row_json(after)?,
                    native_value: None,
                });
            }
        }
        // A merge can rewrite declared native extension fields. Reconstruct
        // their surviving baseline before dependency closure, preserving Things
        // and native datetimes in the eventual inverse write.
        for effect in &invalidation.extension_effects {
            let key = normalized_native_key(&effect.id);
            let position = rows.iter().position(|row| {
                row.native_id.as_ref() == Some(&effect.id) || row.reference_key() == key
            });
            match (position, &effect.after) {
                (Some(position), Some(after)) => {
                    rows[position].value = inventory_json(after.clone());
                    rows[position].native_value = Some(after.clone());
                }
                (Some(position), None) => {
                    rows.remove(position);
                }
                (None, Some(after)) => rows.push(InventoryRow {
                    table: effect.id.tb.clone(),
                    key,
                    native_id: Some(effect.id.clone()),
                    record_id: None,
                    value: inventory_json(after.clone()),
                    native_value: Some(after.clone()),
                }),
                (None, None) => {}
            }
        }
        let decision_keys = invalidation
            .decision_keys
            .iter()
            .map(|key| format!("source_decision_receipt:{key}"))
            .collect::<BTreeSet<_>>();
        let receipt_keys = invalidation
            .receipt_keys
            .iter()
            .map(|key| format!("source_commit_receipt:{key}"))
            .collect::<BTreeSet<_>>();
        rows.retain(|row| {
            let invalid = decision_keys.contains(&row.key)
                || receipt_keys.contains(&row.key)
                || row
                    .record_id
                    .as_ref()
                    .is_some_and(|id| invalidation.outbox_ids.contains(id));
            if invalid {
                invalidation_deletes.push(row.clone());
            }
            !invalid
        });
        let supports: Vec<SourceEffectSupport> = rows
            .iter()
            .filter(|row| row.table == "source_effect_support")
            .map(|row| {
                row.value
                    .get("payload")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| integrity("source support missing payload"))
                    .and_then(decode_payload)
            })
            .collect::<StorageResult<_>>()?;
        for effect in &invalidation.effects {
            if let Some(before) = &effect.before {
                let fingerprint = before.fingerprint()?;
                if supports.iter().any(|support| {
                    support.record_id == before.base().id
                        && support.record_fingerprint != fingerprint
                }) {
                    return Err(conflict("source_support_fingerprint_changed"));
                }
            }
        }
        let mut plan = LifecyclePlan {
            observed_epoch,
            inverse_effects: invalidation.effects,
            extension_effects: invalidation.extension_effects,
            contact_survival: invalidation.contact_survival,
            invalidation_deletes,
            original_supports: supports.clone(),
            ..LifecyclePlan::default()
        };
        plan.removed.insert(source.id());
        let journal = self.client.erasure_journal()?;
        let identity_token = |identity: &Thing| {
            crate::reference_fence::reference_identity_token(
                journal.identity(),
                space,
                journal.reference_key_bytes(),
                identity,
            )
        };
        // Rows removed from the overlay still seed reverse dependencies and
        // permanent late-writer fences. Otherwise an artifact that names only
        // an invalidated output/event could outlive the erased source.
        for effect in &plan.inverse_effects {
            if effect.after.is_none() {
                if let Some(before) = &effect.before {
                    plan.removed.insert(before.base().id.id());
                    plan.removed_identity_tokens.insert(identity_token(
                        &crate::surreal::thing_from_record_id(&before.base().id),
                    )?);
                }
            }
        }
        for effect in &plan.extension_effects {
            if effect.after.is_none() {
                plan.removed_keys.insert(normalized_native_key(&effect.id));
                plan.removed_identity_tokens
                    .insert(identity_token(&effect.id)?);
            }
        }
        for row in &plan.invalidation_deletes {
            plan.removed_keys.insert(row.reference_key());
            plan.removed_identity_tokens
                .insert(identity_token(&row.physical_identity()?)?);
            if let Some(id) = &row.record_id {
                plan.removed.insert(id.id());
            }
            if let Some(id) = uuid_from_key(&row.key) {
                plan.removed.insert(id);
            }
        }
        let removed_inverse_ids = plan
            .inverse_effects
            .iter()
            .filter(|effect| effect.after.is_none())
            .filter_map(|effect| {
                effect
                    .before
                    .as_ref()
                    .map(|record| record.base().id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut inverse_survivors = BTreeSet::new();
        for record in plan
            .inverse_effects
            .iter()
            .filter_map(|effect| effect.after.as_ref())
        {
            if !contains_reference(&canonical_row_json(record)?, &BTreeSet::from([source.id()])) {
                inverse_survivors.insert(record.base().id.clone());
            }
        }
        if supports
            .iter()
            .any(|support| support.source_id == *source && support.historical_unknown.is_some())
        {
            return Err(conflict("source_erasure_historical_baseline_unresolved"));
        }
        let archives = if physical {
            operational_archive::open_registered(&self.client).await?
        } else {
            Vec::new()
        };
        let mut selected = BTreeSet::new();
        let mut shared = BTreeMap::new();
        for support in supports
            .iter()
            .filter(|support| support.source_id == *source)
        {
            // A derived output requires every input even when its support ledger
            // contains several sources. Its causal inverse already deleted it
            // from the overlay; surviving support cannot resurrect that output.
            if removed_inverse_ids.contains(&support.record_id) {
                continue;
            }
            // Pre-existing operational rows also receive support on publication.
            // Their independent flag cannot make immutable source-owned payload
            // (for example an artifact's publication fence) a shared graph fact.
            if is_shared_graph_table(support.record_id.table().as_str())
                && (inverse_survivors.contains(&support.record_id)
                    || support.independent
                    || supports.iter().any(|other| {
                        other.record_id == support.record_id && other.source_id != *source
                    }))
            {
                shared.insert(support.record_id.id(), support.record_id.clone());
            } else {
                plan.removed.insert(support.record_id.id());
            }
        }
        // IDs belonging to a source are discovered to a fixed point. This includes
        // reverse refs from hot/cold work, revisions, views, and dynamic vectors.
        loop {
            let mut changed = false;
            for row in &rows {
                if RETAINED_CONTROL_TABLES.contains(&row.table.as_str())
                    || retained_completions.contains(&row.physical_key())
                    || row.table == "source_effect_support"
                {
                    continue;
                }
                if selected.contains(&row.physical_key()) {
                    continue;
                }
                let own_id = row.record_id.as_ref().map(RecordId::id);
                if own_id.is_some_and(|id| shared.contains_key(&id)) {
                    continue;
                }
                if !row.contains(&plan.removed)
                    && !row.contains_key_reference(
                        self.client.erasure_journal()?,
                        space,
                        &plan.removed_keys,
                        &plan.removed_identity_tokens,
                    )?
                    && (!physical
                        || !row.intersects_membership(
                            self.client.erasure_journal()?,
                            space,
                            &historical_membership,
                        )?)
                {
                    continue;
                }
                if row.table == "space_config" || row.table == "cost_ledger_daily" {
                    return Err(conflict(
                        "source_erasure_requires_shared_aggregate_reconciliation",
                    ));
                }
                // A second source owns its own lifecycle. A provenance-bearing
                // source cannot be destroyed through an unrelated source request.
                if row.table == "source" && own_id != Some(source.id()) {
                    return Err(conflict("source_erasure_cross_source_dependency"));
                }
                if is_shared_graph_table(&row.table) {
                    let Some(id) = row.record_id.as_ref() else {
                        return Err(integrity("canonical graph row missing typed identity"));
                    };
                    let row_support: Vec<_> = supports
                        .iter()
                        .filter(|support| support.record_id == *id)
                        .collect();
                    if row_support
                        .iter()
                        .any(|support| support.independent || support.source_id != *source)
                    {
                        shared.insert(id.id(), id.clone());
                        continue;
                    }
                    // Historical mixed provenance without a ledger cannot prove
                    // which aliases/text belong to an independent baseline.
                    if row_support.is_empty() && has_other_source(&row.value, source) {
                        return Err(conflict("source_erasure_unresolved_shared_support"));
                    }
                }
                selected.insert(row.physical_key());
                changed |= plan.removed_keys.insert(row.reference_key());
                changed |= plan
                    .removed_identity_tokens
                    .insert(identity_token(&row.physical_identity()?)?);
                if let Some(id) = own_id {
                    changed |= plan.removed.insert(id);
                }
                // Internal control UUIDs share the core incarnation fence. An
                // arbitrary extension key that merely looks like a UUID must
                // retain its native type and table in the exact-reference path.
                if crate::canonical_extension::validate_extension_table(&row.table).is_err() {
                    if let Some(id) = uuid_from_key(&row.key) {
                        changed |= plan.removed.insert(id);
                    }
                }
                // Managed assets are forward links from source documents. Only
                // assets without an unaffected owner can be physically removed.
                for asset in managed_asset_references(&row.value)? {
                    let asset_tokens = BTreeSet::from([asset.id()]);
                    let mut shared_owner = false;
                    for owner in &rows {
                        if owner.physical_key() != row.physical_key()
                            && !selected.contains(&owner.physical_key())
                            && owner.record_id.as_ref() != Some(&asset)
                            && !RETAINED_CONTROL_TABLES.contains(&owner.table.as_str())
                            && owner.contains(&asset_tokens)
                            && !owner.contains(&plan.removed)
                            && !owner.contains_key_reference(
                                self.client.erasure_journal()?,
                                space,
                                &plan.removed_keys,
                                &plan.removed_identity_tokens,
                            )?
                        {
                            shared_owner = true;
                            break;
                        }
                    }
                    if !shared_owner {
                        plan.media_assets.insert(asset.clone());
                        changed |= plan.removed.insert(asset.id());
                    }
                }
            }
            // Both inventories participate in the same fixed point. An archived
            // run selected from a source can select a native document on the next
            // iteration; a native inverse can likewise select archived artifacts.
            if physical {
                changed |= operational_archive::extend_selection(
                    &archives,
                    journal,
                    space,
                    &historical_membership,
                    &mut plan,
                )?;
            }
            if !changed {
                break;
            }
        }
        for id in shared.values() {
            let original = CanonicalRecord::load(&self.client, space, id)
                .await?
                .ok_or_else(|| integrity("shared support points to missing canonical row"))?;
            let fingerprint = original.fingerprint()?;
            if supports.iter().any(|support| {
                support.record_id == *id && support.record_fingerprint != fingerprint
            }) {
                return Err(conflict("source_support_fingerprint_changed"));
            }
            let mut record = if let Some(effect) = plan.inverse_effects.iter().find(|effect| {
                effect
                    .after
                    .as_ref()
                    .is_some_and(|record| record.base().id == *id)
            }) {
                effect
                    .after
                    .clone()
                    .ok_or_else(|| integrity("missing decision inverse row"))?
            } else {
                CanonicalRecord::load(&self.client, space, id)
                    .await?
                    .ok_or_else(|| integrity("shared support points to missing canonical row"))?
            };
            if scrub_source_lineage(&mut record, &plan.removed)? {
                plan.replacements.push(record);
            }
        }
        for support in &supports {
            if support.source_id == *source || removed_inverse_ids.contains(&support.record_id) {
                continue;
            }
            let replacement = plan
                .replacements
                .iter()
                .find(|row| row.base().id == support.record_id)
                .or_else(|| {
                    plan.inverse_effects
                        .iter()
                        .filter_map(|effect| effect.after.as_ref())
                        .find(|row| row.base().id == support.record_id)
                });
            if let Some(replacement) = replacement {
                let mut support = support.clone();
                support.record_fingerprint = replacement.fingerprint()?;
                plan.support_updates.push(support);
            }
        }
        for row in rows {
            if row.table == "source_effect_support"
                && row
                    .value
                    .get("source_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(source.to_string().as_str())
            {
                plan.support_deletes.push(row);
            } else if selected.contains(&row.physical_key()) {
                if physical {
                    plan.deletes.push(row);
                } else if is_shared_graph_table(&row.table) {
                    if let Some(id) = &row.record_id {
                        if let Some(mut record) =
                            CanonicalRecord::load(&self.client, space, id).await?
                        {
                            record.base_mut().deleted_at = Some(Utc::now());
                            record.base_mut().updated_at = Utc::now();
                            plan.replacements.push(record);
                        }
                    }
                }
            }
        }
        // A physically removed canonical row cannot retain support from any
        // source. Independent mutations also create source-bearing audit rows:
        // dependency closure removes those directly, without a decision inverse.
        // The independent fact can survive while its payload-bearing audit and
        // every support identity naming that audit must physically disappear.
        // Retraction keeps ordinary payload rows, so only actual inverse deletes
        // contribute in that case.
        let mut deleted_support_records = removed_inverse_ids
            .into_iter()
            .chain(
                plan.deletes
                    .iter()
                    .filter(|_| physical)
                    .filter_map(|row| row.record_id.clone()),
            )
            .map(|id| id.to_string())
            .collect::<BTreeSet<_>>();
        if physical && !historical_membership.is_empty() {
            // A prior purge may have committed the payload delete and then failed
            // native closure because another source's support was left behind.
            // Replay must use the original durable *record identity*, even when
            // that record is now absent. A stale receipt pointer alone does not
            // justify deleting support for an independently surviving fact.
            for support in &supports {
                let token =
                    identity_token(&crate::surreal::thing_from_record_id(&support.record_id))?;
                if historical_membership.contains(&token) {
                    let record = support.record_id.to_string();
                    if !deleted_support_records.contains(&record)
                        && original_rows
                            .iter()
                            .any(|row| row.record_id.as_ref() == Some(&support.record_id))
                    {
                        // Deleted-identity membership is distinct from historical
                        // version commitments. Never discard a retained fact's
                        // support if those authorities disagree with inventory.
                        return Err(conflict("source_erasure_membership_names_surviving_record"));
                    }
                    deleted_support_records.insert(record);
                }
            }
        }
        // Inverse preparation removed rows from the overlay. Read support from
        // the original inventory so inverse, closure and replay are covered.
        for row in original_rows {
            if row.table == "source_effect_support"
                && row
                    .value
                    .get("record_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| deleted_support_records.contains(id))
                && !plan
                    .support_deletes
                    .iter()
                    .any(|other| other.physical_key() == row.physical_key())
            {
                plan.support_deletes.push(row);
            }
        }
        if physical && !historical_membership.is_empty() {
            // Historical active membership was admitted before the original
            // purge. A restored independent survivor must not retain one of
            // those exact references; never ignore this disagreement at Ready.
            for record in plan
                .inverse_effects
                .iter()
                .filter_map(|effect| effect.after.as_ref())
                .chain(plan.replacements.iter())
            {
                let row = InventoryRow {
                    table: record.base().id.table().as_str().into(),
                    key: record.base().id.to_string(),
                    native_id: Some(crate::surreal::thing_from_record_id(&record.base().id)),
                    record_id: Some(record.base().id.clone()),
                    value: canonical_row_json(record)?,
                    native_value: None,
                };
                if row.intersects_membership(
                    self.client.erasure_journal()?,
                    space,
                    &historical_membership,
                )? {
                    return Err(conflict(
                        "source_erasure_survivor_references_erased_identity",
                    ));
                }
            }
        }
        plan.event_records = plan
            .replacements
            .iter()
            .map(|record| record.base().id.clone())
            .chain(plan.inverse_effects.iter().filter_map(|effect| {
                effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .map(|record| record.base().id.clone())
            }))
            .chain(plan.deletes.iter().filter_map(|row| row.record_id.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(plan)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "This proof phase joins surviving source supports with the lifecycle inverse under its captured epoch; keep the dependent reads and final checks together."
    )]
    async fn prepare_lifecycle_support_continuations(
        &self,
        space: &str,
        source: &RecordId,
        lifecycle_commit_id: &str,
        plan: &mut LifecyclePlan,
        physical: bool,
    ) -> StorageResult<()> {
        let deleted_records = plan
            .deletes
            .iter()
            .filter(|_| physical)
            .filter_map(|row| row.record_id.clone())
            .chain(plan.inverse_effects.iter().filter_map(|effect| {
                effect
                    .after
                    .is_none()
                    .then(|| effect.before.as_ref().map(|row| row.base().id.clone()))
                    .flatten()
            }))
            .collect::<BTreeSet<_>>();
        let deleted_receipts = plan
            .invalidation_deletes
            .iter()
            .chain(plan.deletes.iter().filter(|_| physical))
            .filter(|row| row.table == "source_commit_receipt")
            .map(|row| {
                row.reference_key()
                    .strip_prefix("source_commit_receipt:")
                    .map(str::to_owned)
                    .ok_or_else(|| integrity("invalid lifecycle receipt identity"))
            })
            .collect::<StorageResult<BTreeSet<_>>>()?;
        // Coalesce the actual final rows in transaction write order. Replacing
        // a row after an inverse must not certify the superseded intermediate.
        let mut surviving_records = plan
            .inverse_effects
            .iter()
            .filter_map(|effect| effect.after.clone())
            .chain(plan.replacements.iter().cloned())
            .map(|row| (row.base().id.clone(), row))
            .collect::<BTreeMap<_, _>>();
        let tombstones = surviving_records
            .values()
            .filter(|row| row.base().is_deleted())
            .map(|row| row.base().id.clone())
            .collect::<BTreeSet<_>>();
        surviving_records.retain(|id, _| !deleted_records.contains(id) && !tombstones.contains(id));
        // An invalidated transaction can leave identical domain bytes. Its
        // support pointer still needs repair before that receipt disappears.
        let mut unknown_updates = Vec::new();
        for support in &plan.original_supports {
            if support.source_id == *source
                || deleted_records.contains(&support.record_id)
                || tombstones.contains(&support.record_id)
                || surviving_records.contains_key(&support.record_id)
                || !support
                    .last_commit_id
                    .as_ref()
                    .is_some_and(|commit| deleted_receipts.contains(&stable_key(&[space, commit])))
            {
                continue;
            }
            let record = CanonicalRecord::load(&self.client, space, &support.record_id)
                .await?
                .ok_or_else(|| integrity("surviving support points to missing canonical row"))?;
            if record.fingerprint()? != support.record_fingerprint {
                return Err(conflict("source_support_fingerprint_changed"));
            }
            if record.base().is_deleted() {
                // An ineligible tombstone grants no fact authority. Its pointer
                // cannot continue naming a receipt physically removed below.
                let mut unknown = support.clone();
                unknown.last_commit_id = None;
                unknown_updates.push(unknown);
                continue;
            }
            surviving_records.insert(support.record_id.clone(), record);
        }
        let prepared = crate::source_contact_lineage::prepare_support_continuations(
            &self.client,
            space,
            source,
            &plan.observed_epoch,
            lifecycle_commit_id,
            &surviving_records.into_values().collect::<Vec<_>>(),
            &plan.original_supports,
            &deleted_receipts.iter().cloned().collect::<Vec<_>>(),
        )
        .await?;
        // These receipts did not exist during inventory. Validate their exact
        // future database rows before allowing the current purge to create any.
        // The native permanent fence is installed after inverse writes, so its
        // ordering alone cannot certify a newly prepared continuation payload.
        let journal = self.client.erasure_journal()?;
        let historical_membership = if physical {
            let intent = journal
                .intents()?
                .into_iter()
                .find(|intent| intent.space_id == space && intent.source_id == *source)
                .ok_or_else(|| integrity("physical continuation has no durable intent"))?;
            journal.reference_membership(&intent)?
        } else {
            BTreeSet::new()
        };
        for (id, value) in prepared.native_rows()? {
            let row = InventoryRow {
                table: id.tb.clone(),
                key: normalized_native_key(&id),
                native_id: Some(id),
                record_id: None,
                value: inventory_json(value.clone()),
                native_value: Some(value),
            };
            if row.references_erased_identity(journal, space, plan, &historical_membership)? {
                return Err(conflict("source_continuation_references_erased_identity"));
            }
        }
        // Keep fingerprint refreshes for ineligible generic tombstones without
        // retaining deleted receipt pointers. Only the owner helper can supply
        // a proven surviving contact pointer or a new private continuation.
        for support in &mut plan.support_updates {
            if support
                .last_commit_id
                .as_ref()
                .is_some_and(|commit| deleted_receipts.contains(&stable_key(&[space, commit])))
            {
                support.last_commit_id = None;
            }
        }
        for support in unknown_updates
            .into_iter()
            .chain(prepared.support_updates.iter().cloned())
        {
            if let Some(existing) = plan.support_updates.iter_mut().find(|existing| {
                existing.space_id == support.space_id
                    && existing.source_id == support.source_id
                    && existing.source_revision == support.source_revision
                    && existing.record_id == support.record_id
            }) {
                *existing = support;
            } else {
                plan.support_updates.push(support);
            }
        }
        for support in &plan.support_updates {
            let value =
                serde_json::to_value(support).map_err(|error| integrity(&error.to_string()))?;
            if support.space_id != space
                || support.source_id == *source
                || deleted_records.contains(&support.record_id)
                || contains_reference(&value, &plan.removed)
                || contains_key_reference(&value, &plan.removed_keys)
                || support
                    .last_commit_id
                    .as_ref()
                    .is_some_and(|commit| deleted_receipts.contains(&stable_key(&[space, commit])))
            {
                return Err(conflict(
                    "source_support_continuation_references_erased_identity",
                ));
            }
        }
        plan.support_continuations = Some(prepared);
        Ok(())
    }
}

/// A conservative search projection only.
///
/// Never deserialize it as an
/// authoritative record or use it to write native rows. Unknown native field
/// types must not make unrelated tenant rows block source cleanup.
fn inventory_json(value: Value) -> serde_json::Value {
    match value {
        Value::None | Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(value),
        // Declared native reference objects use integer-valued `id` fields.
        // Stringifying them would silently turn Number(42) into String("42")
        // and omit reverse dependencies from the physical erasure closure.
        Value::Number(surrealdb::sql::Number::Int(value)) => {
            serde_json::Value::Number(value.into())
        }
        Value::Strand(value) => serde_json::Value::String(value.0),
        Value::Array(values) => {
            serde_json::Value::Array(values.0.into_iter().map(inventory_json).collect())
        }
        Value::Object(values) => serde_json::Value::Object(
            values
                .0
                .into_iter()
                .map(|(key, value)| (key, inventory_json(value)))
                .collect(),
        ),
        value => serde_json::Value::String(value.to_string()),
    }
}

// Completion events retain only opaque IDs and the already-retained authorization
// envelope. A producer-supplied event label must not exempt arbitrary content.
#[allow(
    clippy::too_many_lines,
    reason = "A retained completion is accepted only after all journal, receipt, outbox, and physical-history checks succeed; retain the complete fail-closed proof in one phase."
)]
fn retained_erasure_completions(
    space: &str,
    rows: &[InventoryRow],
) -> StorageResult<BTreeSet<(String, String)>> {
    let operations = rows
        .iter()
        .filter(|row| row.table == OPERATION_TABLE && row_space(&row.value) == Some(space))
        .map(|row| {
            let operation: StoredOperation = decode_payload(
                row.value
                    .get("payload")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| integrity("operation missing payload"))?,
            )?;
            Ok((operation.receipt.operation_id.clone(), operation))
        })
        .collect::<StorageResult<BTreeMap<_, _>>>()?;
    let mut retained = BTreeSet::new();
    for row in rows.iter().filter(|row| row.table == "system_event") {
        let value = &row.value;
        let Some(payload) = value.get("payload").and_then(serde_json::Value::as_object) else {
            continue;
        };
        let Some(operation_id) = payload
            .get("operation_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(operation) = operations.get(operation_id) else {
            continue;
        };
        let receipt = &operation.receipt;
        let expected_id = RecordId::new(
            TableName::SystemEvent,
            Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("{operation_id}:source.erased").as_bytes(),
            ),
        );
        if receipt.space_id != space
            || receipt.state != OperationState::Erased
            || row.record_id.as_ref() != Some(&expected_id)
            || row_space(value) != Some(space)
            || value.get("event_type").and_then(serde_json::Value::as_str) != Some("source.erased")
            || value.get("created_by").and_then(serde_json::Value::as_str)
                != Some("source-lifecycle")
            || value
                .get("deleted_at")
                .is_some_and(|value| !value.is_null())
            || value
                .get("seq")
                .and_then(serde_json::Value::as_i64)
                .is_none_or(|seq| seq < 0)
            || payload.len() != 4
            || payload.get("space_id").and_then(serde_json::Value::as_str) != Some(space)
            || completion_record_id(payload.get("source_id")) != Some(receipt.source_id.clone())
        {
            continue;
        }
        let allowed = [
            "id",
            "space_id",
            "created_at",
            "updated_at",
            "created_by",
            "deleted_at",
            "security",
            "provenance",
            "subjects",
            "subjects_state",
            "seq",
            "event_type",
            "payload",
        ];
        if value
            .as_object()
            .is_none_or(|fields| fields.keys().any(|key| !allowed.contains(&key.as_str())))
            || ["created_at", "updated_at"].iter().any(|field| {
                value
                    .get(*field)
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|stamp| {
                        let stamp = stamp
                            .strip_prefix("d'")
                            .and_then(|stamp| stamp.strip_suffix('\''))
                            .unwrap_or(stamp);
                        chrono::DateTime::parse_from_rfc3339(stamp).is_err()
                    })
            })
        {
            continue;
        }
        let provenance_safe = value
            .get("provenance")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|fields| {
                fields.iter().all(|(key, value)| match key.as_str() {
                    "sources" | "chain" => value.as_array().is_some_and(Vec::is_empty),
                    "extractor" | "run_id" | "work_run_id" | "prompt_id" | "config_id" => {
                        value.is_null()
                    }
                    _ => false,
                })
            });
        let expected_security = serde_json::to_value(&operation.authorization.security)
            .map_err(|error| integrity(&error.to_string()))?;
        let expected_subjects_state = serde_json::to_value(operation.authorization.subjects_state)
            .map_err(|error| integrity(&error.to_string()))?;
        let expected_subjects = operation
            .authorization
            .subjects
            .iter()
            .map(|subject| subject.id().clone())
            .collect::<Vec<_>>();
        let actual_subjects = value
            .get("subjects")
            .and_then(serde_json::Value::as_array)
            .and_then(|subjects| {
                subjects
                    .iter()
                    .map(|subject| completion_record_id(Some(subject)))
                    .collect::<Option<Vec<_>>>()
            });
        if !provenance_safe
            || value.get("security") != Some(&expected_security)
            || value.get("subjects_state") != Some(&expected_subjects_state)
            || actual_subjects.as_ref() != Some(&expected_subjects)
        {
            continue;
        }
        let Some(records) = payload.get("records").and_then(serde_json::Value::as_array) else {
            continue;
        };
        let mut source_deletes = 0;
        let safe_records = records.iter().all(|record| {
            let Some(fields) = record.as_object() else {
                return false;
            };
            let Some(id) = completion_record_id(fields.get("id")) else {
                return false;
            };
            if fields.len() != 3
                || fields.get("table").and_then(serde_json::Value::as_str)
                    != Some(id.table().as_str())
            {
                return false;
            }
            let expected_op = if id == receipt.source_id {
                source_deletes += 1;
                "delete"
            } else {
                "update"
            };
            fields.get("op").and_then(serde_json::Value::as_str) == Some(expected_op)
        });
        if safe_records && source_deletes >= 1 {
            retained.insert(row.physical_key());
        }
    }
    Ok(retained)
}

fn completion_record_id(value: Option<&serde_json::Value>) -> Option<RecordId> {
    let value = value?.as_str()?;
    RecordId::from_str(value).ok().or_else(|| {
        let native = surrealdb::sql::thing(value).ok()?;
        (native.to_string() == value)
            .then(|| crate::surreal::record_id_from_thing(&native).ok())
            .flatten()
    })
}

fn row_space(value: &serde_json::Value) -> Option<&str> {
    value.get("space_id").and_then(serde_json::Value::as_str)
}

fn is_shared_graph_table(table: &str) -> bool {
    matches!(
        table,
        "entity"
            | "entity_identity_key"
            | "assertion"
            | "rel"
            | "attribute"
            | "event"
            | "freshness"
            | "truth_slot"
            | "conflict_group"
            | "identity_candidate"
            | "entity_merge"
            | "predicate_registry"
            | "predicate_alias"
    )
}

fn replacement_effect_table(table: TableName) -> bool {
    is_shared_graph_table(table.as_str())
        || matches!(
            table,
            TableName::AuditLog
                | TableName::SystemEvent
                | TableName::IdMap
                | TableName::ReprocessJob
        )
}

fn has_other_source(value: &serde_json::Value, source: &RecordId) -> bool {
    value
        .get("provenance")
        .and_then(|value| value.get("sources"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|sources| {
            sources
                .iter()
                .any(|value| !contains_reference(value, &BTreeSet::from([source.id()])))
        })
}

fn uuid_from_key(key: &str) -> Option<Uuid> {
    let value = key.rsplit(':').next()?.trim_matches(['⟨', '⟩', '\'', 'u']);
    Uuid::parse_str(value).ok()
}

fn managed_asset_references(value: &serde_json::Value) -> StorageResult<BTreeSet<RecordId>> {
    fn asset_identity(value: &str) -> StorageResult<RecordId> {
        let asset = RecordId::from_str(value).or_else(|_| {
            surrealdb::sql::thing(value)
                .map_err(|error| integrity(&error.to_string()))
                .and_then(|id| {
                    crate::surreal::record_id_from_thing(&id)
                        .map_err(|error| integrity(&error.to_string()))
                })
        })?;
        if asset.table() != TableName::MediaAsset {
            return Err(integrity("managed media reference has wrong table"));
        }
        Ok(asset)
    }
    fn visit(value: &serde_json::Value, output: &mut BTreeSet<RecordId>) -> StorageResult<()> {
        match value {
            serde_json::Value::Object(object) => {
                // Accepted intake can persist complete staged MediaAsset rows
                // before the media repository is reached. Their IDs must join
                // the same cleanup closure as ordinary attachment links.
                if let Some(serde_json::Value::String(id)) = object.get("id") {
                    if id.starts_with("media_asset:") {
                        output.insert(asset_identity(id)?);
                    }
                }
                if let Some(serde_json::Value::String(asset)) = object.get("asset_id") {
                    output.insert(asset_identity(asset)?);
                }
                for value in object.values() {
                    visit(value, output)?;
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, output)?;
                }
            }
            // Accepted inputs and artifacts also contain JSON payload strings.
            serde_json::Value::String(value)
                if value.starts_with('{') || value.starts_with('[') =>
            {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(value) {
                    visit(&value, output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut output = BTreeSet::new();
    visit(value, &mut output)?;
    Ok(output)
}

/// Remove only source lineage from a shared semantic row. Unsupported residual
/// references fail the transaction instead of discarding independent fields.
pub(crate) fn scrub_source_lineage(
    record: &mut CanonicalRecord,
    removed: &BTreeSet<Uuid>,
) -> StorageResult<bool> {
    let original = serde_json::to_value(&*record).map_err(|error| integrity(&error.to_string()))?;
    let mut value = original.clone();
    let fields = value
        .get_mut("record")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| integrity("canonical source scrub missing record"))?;
    if let Some(provenance) = fields
        .get_mut("provenance")
        .and_then(serde_json::Value::as_object_mut)
    {
        for field in ["sources", "chain"] {
            if let Some(serde_json::Value::Array(values)) = provenance.get_mut(field) {
                values.retain(|value| !contains_reference(value, removed));
            }
        }
        for field in ["work_run_id", "run_id"] {
            if provenance
                .get(field)
                .is_some_and(|value| contains_reference(value, removed))
            {
                provenance.remove(field);
            }
        }
    }
    for field in ["evidence", "assertion_refs", "source_refs"] {
        if let Some(serde_json::Value::Array(values)) = fields.get_mut(field) {
            values.retain(|value| !contains_reference(value, removed));
        }
    }
    if contains_reference(&value, removed) {
        return Err(conflict("source_erasure_shared_semantic_reference"));
    }
    if value == original {
        return Ok(false);
    }
    *record = serde_json::from_value(value).map_err(|error| integrity(&error.to_string()))?;
    record.base_mut().updated_at = Utc::now();
    Ok(true)
}

impl SourcePublicationRepository {
    #[allow(
        clippy::too_many_lines,
        reason = "Reservation guards, source tombstone, journal membership, head, and outbox changes form one transaction on each backend; retain both complete transaction branches."
    )]
    async fn commit_lifecycle_reservation(
        &self,
        request: &SourceMutation,
        reservation: &LifecycleReservation,
        plan: Option<&LifecyclePlan>,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
    ) -> StorageResult<()> {
        let space = &request.space_id;
        let source = &request.source_id;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    require_space(tx, space)?;
                    let current: Option<SourceHead> =
                        read_control_tx(tx, HEAD_TABLE, &head_key(space, source), space)?;
                    if current.as_ref() != Some(&reservation.before) {
                        return Err(conflict("conflict_generation"));
                    }
                    check_precondition(current.as_ref(), &request.expected)?;
                    for (key, expected) in &reservation.observed_operations {
                        if lifecycle_operation_payload_tx(tx, space, key)?.as_ref()
                            != Some(expected)
                        {
                            return Err(conflict("lifecycle_original_operation_changed"));
                        }
                    }
                    let observed = authorization_snapshot_tx(tx, space, source)?;
                    check_authorized_snapshot(observed.as_ref(), Some(&request.authorized))?;
                    crate::source_publication::check_authorized_document_tx(
                        tx,
                        space,
                        source,
                        request.authorized_document.as_ref(),
                    )?;
                    if let Some(documents) = documents {
                        documents.check_tx(tx)?;
                    }
                    if read_control_tx::<StoredOperation>(
                        tx,
                        OPERATION_TABLE,
                        &reservation.after.operation_id,
                        space,
                    )?
                    .is_some()
                    {
                        return Err(conflict("idempotency_conflict"));
                    }
                    if let Some(plan) = plan {
                        check_epoch(tx, space, &plan.observed_epoch)?;
                        crate::event_sequence::preserve_tx(tx, space)?;
                        apply_plan_tx(tx, space, plan, false, ErasureFault::None)?;
                        if let Some(mut row) = tx.get::<Source>(space, source, true)? {
                            row.base.deleted_at = Some(Utc::now());
                            row.base.updated_at = Utc::now();
                            tx.replace(row)?;
                        }
                        append_event_tx(
                            tx,
                            lifecycle_event(
                                space,
                                source,
                                &reservation.after,
                                &request.authorized.envelope,
                                "source.retracted",
                                &plan.event_records,
                            ),
                        )?;
                    }
                    write_control_tx(
                        tx,
                        OPERATION_TABLE,
                        &reservation.before.operation_id,
                        space,
                        source,
                        &reservation.prior_operation,
                    )?;
                    write_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, source),
                        space,
                        source,
                        &reservation.after,
                    )?;
                    write_control_tx(
                        tx,
                        "source_authorization",
                        &head_key(space, source),
                        space,
                        source,
                        &request.authorized.envelope,
                    )?;
                    write_control_tx(
                        tx,
                        OPERATION_TABLE,
                        &reservation.after.operation_id,
                        space,
                        source,
                        &reservation.operation,
                    )?;
                    Ok(())
                })
                .await;
        }
        let mut bindings = crate::bindings! {
            "space" => space.clone(), "source" => crate::surreal::thing_from_record_id(source),
            "head" => control_thing(HEAD_TABLE, &head_key(space, source)),
            "old_head_payload" => encode_payload(&reservation.before)?,
            "head_record" => control_value(space, source, &reservation.after)?,
            "authorization" => control_thing("source_authorization", &head_key(space, source)),
            "authorization_record" => control_value(space, source, &request.authorized.envelope)?,
            "operation" => control_thing(OPERATION_TABLE, &reservation.after.operation_id),
            "operation_record" => control_value(space, source, &reservation.operation)?,
            "prior_operation" => control_thing(OPERATION_TABLE, &reservation.before.operation_id),
            "prior_record" => control_value(space, source, &reservation.prior_operation)?,
            "authorized_updated" => request.authorized.source_updated_at.map_or(Value::None, |value| crate::surreal::datetime_from_chrono(value).into()),
            "authorized_deleted" => request.authorized.source_deleted_at.map_or(Value::None, |value| crate::surreal::datetime_from_chrono(value).into()),
            "authorized_security" => crate::surreal::json_to_surreal(serde_json::to_value(&request.authorized.envelope.security).map_err(|error| integrity(&error.to_string()))?)?,
            "authorized_subjects" => Array::from(request.authorized.envelope.subjects.iter().map(|subject| Value::from(crate::surreal::thing_from_record_id(subject.id()))).collect::<Vec<_>>()),
            "authorized_subjects_state" => crate::surreal::enum_to_surreal(&request.authorized.envelope.subjects_state)?,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\nLET $spaces = SELECT id FROM space_config WHERE space_id = $space AND deleted_at IS NONE;\nIF array::len($spaces) != 1 { THROW 'publication:unknown_space'; };\nIF (SELECT VALUE payload FROM ONLY $head) != $old_head_payload { THROW 'publication:conflict_generation'; };\nIF (SELECT * FROM ONLY $operation) != NONE { THROW 'publication:idempotency_conflict'; };\nLET $authorized_current = SELECT * FROM ONLY $source;\nIF $authorized_current = NONE OR $authorized_current.space_id != $space OR $authorized_current.updated_at != $authorized_updated OR $authorized_current.deleted_at != $authorized_deleted OR $authorized_current.security != $authorized_security OR $authorized_current.subjects != $authorized_subjects OR $authorized_current.subjects_state != $authorized_subjects_state { THROW 'publication:authorized_source_changed'; };\n");
        for (index, (key, expected)) in reservation.observed_operations.iter().enumerate() {
            bindings.insert(
                format!("lifecycle_original_operation_{index}"),
                control_thing(OPERATION_TABLE, key).into(),
            );
            bindings.insert(
                format!("lifecycle_original_payload_{index}"),
                expected.clone().into(),
            );
            let _ = writeln!(sql,
                "LET $lifecycle_original_row_{index} = SELECT * FROM ONLY $lifecycle_original_operation_{index};\nIF $lifecycle_original_row_{index} = NONE OR $lifecycle_original_row_{index}.space_id != $space OR $lifecycle_original_row_{index}.payload != $lifecycle_original_payload_{index} {{ THROW 'publication:lifecycle_original_operation_changed'; }};"
            );
        }
        crate::source_publication::append_authorized_document_guard(
            &mut sql,
            &mut bindings,
            space,
            source,
            request.authorized_document.as_ref(),
        )?;
        if let Some(documents) = documents {
            documents.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(plan) = plan {
            append_epoch_guard(&mut sql, &mut bindings, space, &plan.observed_epoch);
            crate::event_sequence::append_preserve_sql(
                &mut sql,
                &mut bindings,
                space,
                "lifecycle_retired_events",
            )?;
            append_plan_sql(
                &mut sql,
                &mut bindings,
                space,
                plan,
                false,
                ErasureFault::None,
            )?;
            sql.push_str(
                "UPDATE $source SET deleted_at = time::now(), updated_at = time::now();\n",
            );
            append_event_sql(
                &mut sql,
                &mut bindings,
                &lifecycle_event(
                    space,
                    source,
                    &reservation.after,
                    &request.authorized.envelope,
                    "source.retracted",
                    &plan.event_records,
                ),
            )?;
        }
        sql.push_str("UPDATE $prior_operation CONTENT $prior_record;\nUPDATE $head CONTENT $head_record;\nUPSERT $authorization CONTENT $authorization_record;\nCREATE $operation CONTENT $operation_record;\nCOMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    /// The backend checkpoint consumes the complete original journal.
    ///
    /// Finish
    /// every logical purge before asking it to reclaim any native generation.
    /// This method runs only under the caller's existing exclusive barrier.
    async fn purge_all_reserved_sources(
        &self,
        target: Option<(&ErasureIntent, ErasureFault)>,
    ) -> StorageResult<()> {
        let journal = self.client.erasure_journal()?;
        let reservations = crate::reference_fence::reference_reservations(&self.client).await?;
        let prior_intents = journal.intents()?;
        // An interrupted admitted owner must already have its original intent.
        // Never mint a replacement intent while closing another head's gap.
        for (space, owner) in &reservations {
            if !prior_intents
                .iter()
                .any(|intent| intent.space_id == *space && intent.intent_id == *owner)
            {
                return Err(integrity("reserved erasure has no original durable intent"));
            }
        }
        // First close every reservation-to-journal gap. An owned live erasure
        // may already have reserved a different head while awaiting this gate.
        let heads: Vec<SourceHead> = self.list_controls(HEAD_TABLE, None).await?;
        for head in heads {
            if head.lifecycle == SourceVisibility::Erasing {
                journal.record_intent(&head.space_id, &head.source_id)?;
            }
        }
        let intents = journal.intents()?;
        if let Some((target, _)) = target {
            if !intents.iter().any(|intent| intent == target) {
                return Err(integrity("physical purge has no matching durable intent"));
            }
        }
        // A Space can have one interrupted admitted owner. Resume it before
        // the other original intents, without manufacturing or clearing custody.
        let mut ordered = Vec::with_capacity(intents.len());
        let mut selected = BTreeSet::new();
        for (space, intent_id) in reservations {
            let intent = intents
                .iter()
                .find(|intent| intent.space_id == space && intent.intent_id == intent_id)
                .ok_or_else(|| integrity("reserved erasure has no original durable intent"))?;
            if !selected.insert(intent.intent_id.clone()) {
                return Err(integrity("duplicate original erasure reservation"));
            }
            ordered.push(intent);
        }
        // Native purge may have released its reference reservation before an
        // archive rewrite was interrupted. Its durable archive denial remains
        // an original owner and must finish before unrelated replay intents.
        for entry in journal.operational_archive_entries()? {
            if let crate::erasure_journal::ArchiveGenerationState::Redacting { intent_id, .. } =
                &entry.state
            {
                let intent = intents
                    .iter()
                    .find(|i| &i.intent_id == intent_id)
                    .ok_or_else(|| integrity("archive reservation has no original intent"))?;
                if selected.insert(intent.intent_id.clone()) {
                    ordered.push(intent);
                }
            }
        }
        for intent in &intents {
            if selected.insert(intent.intent_id.clone()) {
                ordered.push(intent);
            }
        }
        let mut completions = Vec::new();
        for intent in ordered {
            let fault = target
                .filter(|(original, _)| *original == intent)
                .map_or(ErasureFault::None, |(_, fault)| fault);
            let affected = Box::pin(self.client.with_erasure_reservation(
                intent,
                self.purge_reserved_source_scoped(
                    &intent.space_id,
                    &intent.source_id,
                    intent,
                    fault,
                ),
            ))
            .await?;
            if let Some(affected) = affected {
                // Retain only original identities, never a payload-bearing plan.
                completions.push((intent, affected));
            }
        }
        if intents.is_empty() {
            return Ok(());
        }
        self.client.persist_restored_state().await?;
        if target.is_some_and(|(_, fault)| fault == ErasureFault::AfterPersistence) {
            return Err(injected_fault());
        }
        for (intent, affected) in completions {
            self.client
                .with_erasure_reservation(
                    intent,
                    self.complete_erasure(&intent.space_id, &intent.source_id, &affected),
                )
                .await?;
        }
        // The existing outer barrier persists these completion receipts before
        // reopening ordinary admission; no new maintenance clock is started.
        Ok(())
    }

    async fn purge_reserved_source_scoped(
        &self,
        space: &str,
        source: &RecordId,
        intent: &ErasureIntent,
        fault: ErasureFault,
    ) -> StorageResult<Option<Vec<RecordId>>> {
        let journal = self.client.erasure_journal()?;
        let intent_id = &intent.intent_id;
        // Shared admission closes ownership changes during inventory and the
        // external journal fsync. An interrupted owner remains resumable.
        crate::reference_fence::reserve_reference_guard(&self.client, space, intent).await?;
        if fault == ErasureFault::AfterAdmissionReservation {
            return Err(injected_fault());
        }
        // Ordinary backups omit the private projection. Rebuild the complete
        // original-journal union before inverse writes, including prior erasures
        // whose retained anchor count predates this reserved intent.
        self.replay_space_reference_memberships(space).await?;
        // Every payload writer advances this independent Space generation,
        // including staging rows excluded from the canonical graph epoch.
        let observed_reference_generation =
            crate::reference_fence::reference_guard_revision(&self.client, space).await?;
        let head = self.head(space, source).await?;
        let inventory = self.inventory(space).await?;
        let source_token = BTreeSet::from([source.id()]);
        let historical_membership = journal.reference_membership(intent)?;
        let mut has_payload = false;
        for row in &inventory {
            if !RETAINED_CONTROL_TABLES.contains(&row.table.as_str())
                && (row.contains(&source_token)
                    || row.intersects_membership(journal, space, &historical_membership)?)
            {
                has_payload = true;
                break;
            }
        }
        if head.is_none() && !has_payload && journal.operational_archive_entries()?.is_empty() {
            // A backup can predate this source entirely. Keep the independently
            // durable physical-incarnation fence even without a head or payload,
            // otherwise a delayed producer could reintroduce data after Ready.
            // No source, space, head, or schema is synthesized here.
            self.install_absent_source_fence(space, source, intent, &observed_reference_generation)
                .await?;
            return Ok(None);
        }
        let mut plan = self.prepare_lifecycle_plan(space, source, true).await?;
        // The durable original intent also identifies replay from backups that
        // predate the lifecycle reservation or the source itself.
        self.prepare_lifecycle_support_continuations(
            space,
            source,
            &intent.intent_id,
            &mut plan,
            true,
        )
        .await?;
        // Persist the exact identities before any destructive write. A backup
        // predating every deleted row still has an independent late-write fence.
        let memberships = self.persist_plan_reference_membership(intent, &plan)?;
        if fault == ErasureFault::AfterReferenceMembership {
            return Err(injected_fault());
        }
        if head
            .as_ref()
            .is_some_and(|head| head.lifecycle == SourceVisibility::Erased)
            && plan.deletes.is_empty()
            && plan.support_deletes.is_empty()
            && plan.invalidation_deletes.is_empty()
            && plan.support_updates.is_empty()
            && plan.replacements.is_empty()
            && plan.inverse_effects.is_empty()
            && plan.extension_effects.is_empty()
        {
            self.install_reference_membership(
                space,
                intent_id,
                &memberships,
                &observed_reference_generation,
            )
            .await?;
            operational_archive::redact_selected(&self.client, intent, &plan.archive_deletes)
                .await?;
            self.remove_managed_erasure_files(space, source).await?;
            return Ok(None);
        }
        self.commit_physical_purge(
            space,
            source,
            intent_id,
            &plan,
            head.as_ref(),
            &memberships,
            &observed_reference_generation,
            fault,
        )
        .await?;
        if fault == ErasureFault::AfterPurge {
            return Err(injected_fault());
        }
        operational_archive::redact_selected(&self.client, intent, &plan.archive_deletes).await?;
        self.remove_managed_erasure_files(space, source).await?;
        Ok(Some(plan.event_records))
    }

    async fn replay_space_reference_memberships(&self, space: &str) -> StorageResult<()> {
        let journal = self.client.erasure_journal()?;
        let mut memberships = BTreeSet::new();
        for intent in journal
            .intents()?
            .into_iter()
            .filter(|intent| intent.space_id == space)
        {
            memberships.extend(journal.reference_membership(&intent)?);
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    crate::reference_fence::project_reference_membership_tx(
                        tx,
                        space,
                        journal.identity(),
                        journal.reference_key_fingerprint(),
                        &memberships,
                    )
                })
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        crate::reference_fence::append_reference_membership_sql(
            &mut sql,
            &mut bindings,
            space,
            journal.identity(),
            journal.reference_key_fingerprint(),
            &memberships,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    fn persist_plan_reference_membership(
        &self,
        intent: &ErasureIntent,
        plan: &LifecyclePlan,
    ) -> StorageResult<BTreeSet<String>> {
        let journal = self.client.erasure_journal()?;
        let mut identities = vec![crate::surreal::thing_from_record_id(&intent.source_id)];
        for row in plan
            .deletes
            .iter()
            .chain(&plan.support_deletes)
            .chain(&plan.invalidation_deletes)
        {
            identities.push(row.physical_identity()?);
        }
        for effect in &plan.inverse_effects {
            if effect.after.is_none() {
                if let Some(before) = &effect.before {
                    identities.push(crate::surreal::thing_from_record_id(&before.base().id));
                }
            }
        }
        for effect in &plan.extension_effects {
            if effect.after.is_none() {
                identities.push(effect.id.clone());
            }
        }
        identities.extend(plan.archive_deletes.identities());
        // Tokenize identities, never whole payloads: an erased row may cite a
        // surviving independently supported record that must remain writable.
        let memberships = identities
            .iter()
            .map(|identity| {
                crate::reference_fence::reference_identity_token(
                    journal.identity(),
                    &intent.space_id,
                    journal.reference_key_bytes(),
                    identity,
                )
            })
            .collect::<StorageResult<BTreeSet<_>>>()?;
        journal.record_reference_membership(intent, &memberships)?;
        journal.reference_membership(intent)
    }

    async fn install_reference_membership(
        &self,
        space: &str,
        intent_id: &str,
        memberships: &BTreeSet<String>,
        observed_generation: &crate::reference_fence::ReferenceGuardRevision,
    ) -> StorageResult<()> {
        let journal = self.client.erasure_journal()?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    crate::reference_fence::check_reference_guard_revision_tx(
                        tx,
                        observed_generation,
                    )?;
                    crate::reference_fence::project_reference_membership_tx(
                        tx,
                        space,
                        journal.identity(),
                        journal.reference_key_fingerprint(),
                        memberships,
                    )?;
                    crate::reference_fence::release_reference_guard_tx(tx, space, intent_id)
                })
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        crate::reference_fence::append_reference_guard_revision_sql(
            &mut sql,
            &mut bindings,
            observed_generation,
        )?;
        crate::reference_fence::append_reference_membership_sql(
            &mut sql,
            &mut bindings,
            space,
            journal.identity(),
            journal.reference_key_fingerprint(),
            memberships,
        )?;
        crate::reference_fence::append_release_reference_guard_sql(
            &mut sql,
            &mut bindings,
            space,
            intent_id,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    async fn install_absent_source_fence(
        &self,
        space: &str,
        source: &RecordId,
        intent: &ErasureIntent,
        observed_generation: &crate::reference_fence::ReferenceGuardRevision,
    ) -> StorageResult<()> {
        let journal = self.client.erasure_journal()?;
        let memberships =
            self.persist_plan_reference_membership(intent, &LifecyclePlan::default())?;
        let intent_id = &intent.intent_id;
        let key = stable_key(&[space, &source.to_string(), &source.id().to_string()]);
        let row = serde_json::json!({
            "id": format!("source_tombstone_ref:{key}"), "space_id": space,
            "source_id": source.to_string(), "token": source.id().to_string(), "intent_id": intent_id,
        });
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    crate::reference_fence::check_reference_guard_revision_tx(
                        tx,
                        observed_generation,
                    )?;
                    tx.upsert_extension_row("source_tombstone_ref", row.clone())?;
                    crate::reference_fence::project_reference_membership_tx(
                        tx,
                        space,
                        journal.identity(),
                        journal.reference_key_fingerprint(),
                        &memberships,
                    )?;
                    crate::reference_fence::release_reference_guard_tx(tx, space, intent_id)
                })
                .await;
        }
        let mut native = crate::surreal::json_to_surreal(row)?;
        if let Value::Object(object) = &mut native {
            object.remove("id");
        }
        let mut sql = String::from("BEGIN TRANSACTION; IF (SELECT VALUE version FROM ONLY publication_schema:canonical) != 1 { THROW 'publication:canonical_schema_missing'; };\n");
        let mut bindings = crate::bindings! { "id" => control_thing("source_tombstone_ref", &key), "row" => native };
        crate::reference_fence::append_reference_guard_revision_sql(
            &mut sql,
            &mut bindings,
            observed_generation,
        )?;
        sql.push_str("UPSERT $id CONTENT $row;\n");
        crate::reference_fence::append_reference_membership_sql(
            &mut sql,
            &mut bindings,
            space,
            journal.identity(),
            journal.reference_key_fingerprint(),
            &memberships,
        )?;
        crate::reference_fence::append_release_reference_guard_sql(
            &mut sql,
            &mut bindings,
            space,
            intent_id,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Physical deletion and durable completion must remain inside the caller-held erasure reservation and each backend transaction; retain their complete ordered write phase."
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "The purge boundary separately binds source, journal intent, reservation, membership, and cancellation authority; preserve these typed inputs and their lifetimes."
    )]
    async fn commit_physical_purge(
        &self,
        space: &str,
        source: &RecordId,
        intent_id: &str,
        plan: &LifecyclePlan,
        old_head: Option<&SourceHead>,
        memberships: &BTreeSet<String>,
        observed_reference_generation: &crate::reference_fence::ReferenceGuardRevision,
        fault: ErasureFault,
    ) -> StorageResult<()> {
        let journal = self.client.erasure_journal()?;
        let mut head = old_head.cloned();
        if let Some(head) = &mut head {
            head.lifecycle = SourceVisibility::Erasing;
            head.published_revision = None;
            head.desired_revision = None;
            head.updated_at = Utc::now();
            // Journal replay can load an older Active generation; rotate it once.
            if old_head.is_some_and(|old| {
                old.lifecycle != SourceVisibility::Erasing
                    && old.lifecycle != SourceVisibility::Erased
            }) {
                head.generation = Uuid::new_v4().to_string();
            }
        }
        if let Some(store) = self.client.embedded_store() {
            return store.mutate(|tx| {
                crate::reference_fence::check_reference_guard_revision_tx(tx, observed_reference_generation)?;
                check_epoch(tx, space, &plan.observed_epoch)?;
                let current: Option<SourceHead> = read_control_tx(tx, HEAD_TABLE, &head_key(space, source), space)?;
                if current.as_ref() != old_head { return Err(conflict("conflict_generation")); }
                crate::event_sequence::preserve_tx(tx, space)?;
                apply_plan_tx(tx, space, plan, true, fault)?;
                for asset in &plan.media_assets {
                    write_control_tx(tx, "source_erasure_files", &stable_key(&[space, &source.to_string(), &asset.to_string()]), space, source, &ErasureFile { asset_id: asset.clone() })?;
                }
                if let Some(head) = &head {
                    write_control_tx(tx, HEAD_TABLE, &head_key(space, source), space, source, head)?;
                }
                sanitize_operations_tx(tx, space, source, false)?;
                // Permanent fences are inserted after reconciliation, so cleanup
                // writes cannot trip the exact guard they are installing.
                for token in &plan.removed {
                    tx.upsert_extension_row("source_tombstone_ref", serde_json::json!({
                        "id": format!("source_tombstone_ref:{}", stable_key(&[space, &source.to_string(), &token.to_string()])),
                        "space_id": space, "source_id": source.to_string(), "token": token.to_string(), "intent_id": intent_id,
                    }))?;
                }
                crate::reference_fence::project_reference_membership_tx(tx, space, journal.identity(), journal.reference_key_fingerprint(), memberships)?;
                crate::reference_fence::release_reference_guard_tx(tx, space, intent_id)
            }).await;
        }
        let mut bindings = crate::bindings! { "space" => space, "head" => control_thing(HEAD_TABLE, &head_key(space, source)), "old_head" => old_head.map_or(Ok(Value::None), |head| encode_payload(head).map(Value::from))? };
        let mut sql = String::from("BEGIN TRANSACTION;\nIF (SELECT VALUE payload FROM ONLY $head) != $old_head { THROW 'publication:conflict_generation'; };\n");
        crate::reference_fence::append_reference_guard_revision_sql(
            &mut sql,
            &mut bindings,
            observed_reference_generation,
        )?;
        append_epoch_guard(&mut sql, &mut bindings, space, &plan.observed_epoch);
        crate::event_sequence::append_preserve_sql(
            &mut sql,
            &mut bindings,
            space,
            "purge_retired_events",
        )?;
        append_plan_sql(&mut sql, &mut bindings, space, plan, true, fault)?;
        for (index, asset) in plan.media_assets.iter().enumerate() {
            bindings.insert(
                format!("asset_id_{index}"),
                control_thing(
                    "source_erasure_files",
                    &stable_key(&[space, &source.to_string(), &asset.to_string()]),
                )
                .into(),
            );
            bindings.insert(
                format!("asset_row_{index}"),
                control_value(
                    space,
                    source,
                    &ErasureFile {
                        asset_id: asset.clone(),
                    },
                )?,
            );
            let _ = writeln!(sql, "UPSERT $asset_id_{index} CONTENT $asset_row_{index};");
        }
        if let Some(head) = &head {
            bindings.insert("head_record".into(), control_value(space, source, head)?);
            sql.push_str("UPDATE $head CONTENT $head_record;\n");
        }
        self.append_sanitized_operations(&mut sql, &mut bindings, space, source, false)
            .await?;
        for (index, token) in plan.removed.iter().enumerate() {
            bindings.insert(
                format!("tombstone_{index}"),
                control_thing(
                    "source_tombstone_ref",
                    &stable_key(&[space, &source.to_string(), &token.to_string()]),
                )
                .into(),
            );
            bindings.insert(format!("tombstone_record_{index}"), crate::surreal::json_to_surreal(serde_json::json!({"space_id":space,"source_id":source.to_string(),"token":token.to_string(),"intent_id":intent_id}))?);
            let _ = writeln!(
                sql,
                "UPSERT $tombstone_{index} CONTENT $tombstone_record_{index};"
            );
        }
        crate::reference_fence::append_reference_membership_sql(
            &mut sql,
            &mut bindings,
            space,
            journal.identity(),
            journal.reference_key_fingerprint(),
            memberships,
        )?;
        crate::reference_fence::append_release_reference_guard_sql(
            &mut sql,
            &mut bindings,
            space,
            intent_id,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    async fn append_sanitized_operations(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
        space: &str,
        source: &RecordId,
        complete: bool,
    ) -> StorageResult<()> {
        let operations: Vec<StoredOperation> =
            self.list_controls(OPERATION_TABLE, Some(space)).await?;
        for (index, mut operation) in operations
            .into_iter()
            .filter(|operation| operation.receipt.source_id == *source)
            .enumerate()
        {
            sanitize_operation(&mut operation, complete);
            bindings.insert(
                format!("sanitize_operation_id_{index}"),
                control_thing(OPERATION_TABLE, &operation.receipt.operation_id).into(),
            );
            bindings.insert(
                format!("sanitize_operation_row_{index}"),
                control_value(space, source, &operation)?,
            );
            let _ =
                writeln!(sql,
                "UPDATE $sanitize_operation_id_{index} CONTENT $sanitize_operation_row_{index};"
            );
        }
        Ok(())
    }

    async fn complete_erasure(
        &self,
        space: &str,
        source: &RecordId,
        affected: &[RecordId],
    ) -> StorageResult<()> {
        let Some(mut head) = self.head(space, source).await? else {
            return Ok(());
        };
        head.lifecycle = SourceVisibility::Erased;
        head.desired_revision = None;
        head.published_revision = None;
        head.updated_at = Utc::now();
        let envelope = self
            .authorization(space, source)
            .await?
            .filter(|envelope| envelope.space_id == space && envelope.source_id == *source)
            .ok_or_else(|| integrity("erased source missing retained authorization"))?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    sanitize_operations_tx(tx, space, source, true)?;
                    write_control_tx(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, source),
                        space,
                        source,
                        &head,
                    )?;
                    append_event_tx(
                        tx,
                        lifecycle_event(space, source, &head, &envelope, "source.erased", affected),
                    )?;
                    Ok(())
                })
                .await;
        }
        let mut bindings = crate::bindings! { "space" => space, "head" => control_thing(HEAD_TABLE, &head_key(space, source)), "head_record" => control_value(space, source, &head)? };
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        self.append_sanitized_operations(&mut sql, &mut bindings, space, source, true)
            .await?;
        sql.push_str("UPDATE $head CONTENT $head_record;\n");
        append_event_sql(
            &mut sql,
            &mut bindings,
            &lifecycle_event(space, source, &head, &envelope, "source.erased", affected),
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }
}

fn sanitize_operation(operation: &mut StoredOperation, complete: bool) {
    operation.receipt.desired_revision = None;
    operation.receipt.published_revision = None;
    operation.receipt.error_code = None;
    if matches!(
        operation.receipt.state,
        OperationState::Erasing | OperationState::Erased
    ) {
        operation.receipt.state = if complete {
            OperationState::Erased
        } else {
            OperationState::Erasing
        };
        if complete {
            operation.receipt.completed_at.get_or_insert_with(Utc::now);
        } else if operation.receipt.state == OperationState::Erasing {
            operation.receipt.completed_at = None;
        }
    } else {
        operation.receipt.state = OperationState::Superseded;
        operation.receipt.completed_at.get_or_insert_with(Utc::now);
    }
}

fn sanitize_operations_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    source: &RecordId,
    complete: bool,
) -> StorageResult<()> {
    for (key, row) in tx.scan_extension_rows(OPERATION_TABLE)? {
        if row_space(&row) != Some(space)
            || row.get("source_id").and_then(serde_json::Value::as_str)
                != Some(source.to_string().as_str())
        {
            continue;
        }
        let mut operation: StoredOperation = decode_payload(
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| integrity("operation missing payload"))?,
        )?;
        sanitize_operation(&mut operation, complete);
        let key = key
            .strip_prefix("source_operation:")
            .ok_or_else(|| integrity("invalid source operation key"))?;
        write_control_tx(tx, OPERATION_TABLE, key, space, source, &operation)?;
    }
    Ok(())
}

fn check_epoch(tx: &EmbeddedMutation<'_>, space: &str, observed: &str) -> StorageResult<()> {
    if epoch_tx(tx, space)?.as_deref() != Some(observed) {
        return Err(conflict("canonical_epoch_changed"));
    }
    Ok(())
}

fn append_epoch_guard(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    epoch: &str,
) {
    bindings.insert(
        "epoch_id".into(),
        control_thing("canonical_epoch", space).into(),
    );
    bindings.insert("observed_epoch".into(), epoch.into());
    sql.push_str("IF (SELECT VALUE epoch FROM ONLY $epoch_id) != $observed_epoch { THROW 'publication:canonical_epoch_changed'; };\nUPDATE $epoch_id SET epoch = <string>rand::uuid();\n");
}

fn apply_plan_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    plan: &LifecyclePlan,
    physical: bool,
    fault: ErasureFault,
) -> StorageResult<()> {
    if let Some(survival) = &plan.contact_survival {
        survival.check_tx(tx)?;
    }
    if let Some(continuations) = &plan.support_continuations {
        continuations.check_tx(tx)?;
    }
    crate::canonical_extension::apply_extension_effects_tx(tx, space, &plan.extension_effects)?;
    for effect in &plan.inverse_effects {
        match (&effect.before, &effect.after) {
            (_, Some(after)) => after.put(tx, effect.before.is_none())?,
            (Some(before), None) => before.delete(tx)?,
            (None, None) => return Err(integrity("empty source decision inverse")),
        }
    }
    for record in &plan.replacements {
        record.put(tx, false)?;
    }
    for support in &plan.support_updates {
        crate::canonical_commit::write_existing_support_tx(tx, support)?;
    }
    if let Some(continuations) = &plan.support_continuations {
        continuations.write_tx(tx)?;
    }
    let mut deleted = false;
    for row in plan
        .deletes
        .iter()
        .chain(&plan.support_deletes)
        .chain(&plan.invalidation_deletes)
    {
        if physical
            || row.table == "source_effect_support"
            || plan
                .invalidation_deletes
                .iter()
                .any(|invalid| invalid.physical_key() == row.physical_key())
        {
            if let Some(id) = &row.record_id {
                tx.hard_delete_raw_row(
                    TableName::from_str(&row.table)
                        .map_err(|error| integrity(&error.to_string()))?,
                    space,
                    id,
                )?;
            } else {
                tx.hard_delete_extension_row(&row.table, &row.key, space)?;
            }
            if !deleted && fault == ErasureFault::DuringPurge {
                return Err(injected_fault());
            }
            deleted = true;
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "This transaction fragment emits lifecycle guards, inverse writes, and dependent cleanup in a fixed order using shared bindings; keep the complete fragment auditable."
)]
fn append_plan_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    plan: &LifecyclePlan,
    physical: bool,
    fault: ErasureFault,
) -> StorageResult<()> {
    if let Some(survival) = &plan.contact_survival {
        survival.append_guard_sql(sql, bindings)?;
    }
    if let Some(continuations) = &plan.support_continuations {
        continuations.append_guard_sql(sql, bindings)?;
    }
    crate::canonical_extension::append_extension_effects_sql(
        sql,
        bindings,
        space,
        &plan.extension_effects,
    )?;
    for (index, effect) in plan.inverse_effects.iter().enumerate() {
        let row = effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .ok_or_else(|| integrity("empty source decision inverse"))?;
        bindings.insert(
            format!("inverse_id_{index}"),
            crate::surreal::thing_from_record_id(&row.base().id).into(),
        );
        if let Some(after) = &effect.after {
            bindings.insert(format!("inverse_record_{index}"), after.native()?);
            if let CanonicalRecord::Rel(rel) = after {
                // SurrealDB 2.6 rejects UPSERT on TYPE RELATION tables. An
                // unchanged edge can be updated; absent or moved edges need
                // RELATE so the native edge keys match the canonical endpoints.
                let moved_endpoints = matches!(
                    effect.before.as_ref(),
                    Some(CanonicalRecord::Rel(before))
                        if before.subject != rel.subject || before.object != rel.object
                );
                if effect.before.is_none() || moved_endpoints {
                    if moved_endpoints {
                        let _ = writeln!(sql, "DELETE $inverse_id_{index};");
                    }
                    bindings.insert(
                        format!("inverse_subject_{index}"),
                        crate::surreal::thing_from_record_id(rel.subject.id()).into(),
                    );
                    bindings.insert(
                        format!("inverse_object_{index}"),
                        crate::surreal::thing_from_record_id(rel.object.id()).into(),
                    );
                    let _ = writeln!(sql,
                        "RELATE $inverse_subject_{index}->$inverse_id_{index}->$inverse_object_{index} CONTENT $inverse_record_{index};"
                    );
                } else {
                    let _ = writeln!(
                        sql,
                        "UPDATE $inverse_id_{index} CONTENT $inverse_record_{index};"
                    );
                }
            } else {
                let _ = writeln!(
                    sql,
                    "UPSERT $inverse_id_{index} CONTENT $inverse_record_{index};"
                );
            }
        } else {
            let _ = writeln!(sql, "DELETE $inverse_id_{index} WHERE space_id = $space;");
        }
    }
    for (index, record) in plan.replacements.iter().enumerate() {
        bindings.insert(
            format!("replacement_id_{index}"),
            crate::surreal::thing_from_record_id(&record.base().id).into(),
        );
        bindings.insert(format!("replacement_row_{index}"), record.native()?);
        let _ = writeln!(
            sql,
            "UPDATE $replacement_id_{index} CONTENT $replacement_row_{index};"
        );
    }
    for (index, support) in plan.support_updates.iter().enumerate() {
        crate::canonical_commit::append_existing_support_sql(
            sql,
            bindings,
            &format!("surviving_support_{index}"),
            support,
        )?;
    }
    if let Some(continuations) = &plan.support_continuations {
        continuations.append_write_sql(sql, bindings)?;
    }
    let mut deleted = false;
    for (index, row) in plan
        .deletes
        .iter()
        .chain(&plan.support_deletes)
        .chain(&plan.invalidation_deletes)
        .enumerate()
    {
        if !physical
            && row.table != "source_effect_support"
            && !plan
                .invalidation_deletes
                .iter()
                .any(|invalid| invalid.physical_key() == row.physical_key())
        {
            continue;
        }
        let id = row
            .native_id
            .clone()
            .ok_or_else(|| integrity("Surreal erasure row missing native id"))?;
        bindings.insert(format!("delete_id_{index}"), id.into());
        let _ = writeln!(sql, "DELETE $delete_id_{index} WHERE space_id = $space;");
        if !deleted && fault == ErasureFault::DuringPurge {
            sql.push_str("THROW 'publication:injected_source_erasure_fault';\n");
        }
        deleted = true;
    }
    let _ = space;
    Ok(())
}

fn lifecycle_event(
    space: &str,
    source: &RecordId,
    head: &SourceHead,
    envelope: &SourceAuthorizationEnvelope,
    event_type: &str,
    affected: &[RecordId],
) -> SystemEvent {
    let id = RecordId::new(
        TableName::SystemEvent,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{event_type}", head.operation_id).as_bytes(),
        ),
    );
    let records = std::iter::once(source).chain(affected).map(|id| serde_json::json!({"id":id,"table":id.table().as_str(),"op":if id == source {"delete"} else {"update"}})).collect::<Vec<_>>();
    let mut base = BaseFields::new(id, space.into(), "source-lifecycle".into());
    base.security = envelope.security.clone();
    base.subjects.clone_from(&envelope.subjects);
    base.subjects_state = envelope.subjects_state;
    SystemEvent {
        base,
        seq: 0,
        event_type: event_type.into(),
        payload: serde_json::json!({"space_id":space,"source_id":source,"operation_id":head.operation_id,"records":records}),
    }
}

fn append_event_tx(tx: &mut EmbeddedMutation<'_>, mut event: SystemEvent) -> StorageResult<()> {
    if tx
        .get::<SystemEvent>(&event.base.space_id, &event.base.id, true)?
        .is_some()
    {
        return Ok(());
    }
    event.seq = crate::event_sequence::reserve_tx(tx, &event.base.space_id, 0, 1)?
        .checked_add(1)
        .ok_or_else(|| integrity("outbox sequence exhausted"))?;
    tx.create(event)?;
    Ok(())
}

fn append_event_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    event: &SystemEvent,
) -> StorageResult<()> {
    bindings.insert(
        "event_id".into(),
        crate::surreal::thing_from_record_id(&event.base.id).into(),
    );
    bindings.insert(
        "event_record".into(),
        crate::SystemEventRepository::system_event_to_value(event)?,
    );
    sql.push_str("IF (SELECT * FROM ONLY $event_id) = NONE {\n");
    let sequence = crate::event_sequence::append_reserve_sql(
        sql,
        bindings,
        &event.base.space_id,
        "lifecycle_event",
        0,
        1,
    )?;
    let _ = writeln!(sql,"CREATE $event_id CONTENT object::from_entries(array::concat(object::entries($event_record), [['seq', {sequence} + 1]])); }};");
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErasureFile {
    asset_id: RecordId,
}

impl SourcePublicationRepository {
    async fn remove_managed_erasure_files(
        &self,
        space: &str,
        source: &RecordId,
    ) -> StorageResult<()> {
        let rows = self.inventory(space).await?;
        for row in rows.iter().filter(|row| {
            row.table == "source_erasure_files"
                && row
                    .value
                    .get("source_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(source.to_string().as_str())
        }) {
            let file: ErasureFile = decode_payload(
                row.value
                    .get("payload")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| integrity("erasure file receipt missing payload"))?,
            )?;
            if file.asset_id.table() != TableName::MediaAsset {
                return Err(integrity("invalid erasure asset table"));
            }
            let path = elephant_types::records::media_asset::managed_asset_path(&file.asset_id);
            let parent = path
                .parent()
                .ok_or_else(|| integrity("managed asset parent missing"))?;
            if let Ok(metadata) = std::fs::symlink_metadata(parent) {
                if metadata.file_type().is_symlink() {
                    return Err(integrity("managed asset parent is a symlink"));
                }
            }
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                    return Err(integrity("managed asset is not a regular file"))
                }
                Ok(_) => {
                    std::fs::remove_file(&path).map_err(crate::durability_io::io_error)?;
                    crate::durability_io::sync_directory(parent)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(crate::durability_io::io_error(error)),
            }
            if let Some(store) = self.client.embedded_store() {
                store
                    .mutate(|tx| {
                        tx.hard_delete_extension_row(&row.table, &row.key, space)?;
                        Ok(())
                    })
                    .await?;
            } else {
                self.client.query_bind_checked("DELETE $row WHERE space_id = $space", crate::bindings! { "row" => row.native_id.clone().ok_or_else(|| integrity("erasure file missing native id"))?, "space" => space }).await?;
            }
        }
        Ok(())
    }
}

fn canonical_row_json(record: &CanonicalRecord) -> StorageResult<serde_json::Value> {
    serde_json::to_value(record)
        .map_err(|error| integrity(&error.to_string()))?
        .get("record")
        .cloned()
        .ok_or_else(|| integrity("canonical inverse has no record"))
}

fn normalized_native_key(id: &Thing) -> String {
    let displayed = id.to_string();
    if crate::canonical_extension::validate_extension_table(&id.tb).is_ok() {
        // Arbitrary extension String/Number/Uuid keys retain their native type.
        // Core/control UUID identities keep their established RecordId spelling.
        return displayed;
    }
    // Private String UUID keys are displayed with SurrealQL quoting. Keeping
    // that quoting as key data would make a later control_thing reconstruction
    // delete a different physical record. Read the original typed identity;
    // extension and non-UUID keys retain their existing exact representation.
    let uuid = match &id.id {
        surrealdb::sql::Id::String(value) => Uuid::parse_str(value).ok(),
        surrealdb::sql::Id::Uuid(value) => Some(Uuid::from_bytes(*value.as_bytes())),
        _ => None,
    };
    uuid.map_or(displayed, |uuid| format!("{}:{uuid}", id.tb))
}

#[cfg(test)]
#[path = "source_lifecycle_tests.rs"]
mod tests;

// Shared exact row gate for administrative managed archive readers. Ordinary
// source/record ABAC remains the caller's responsibility.
pub(crate) fn verify_archive_erasure_access(
    journal: &crate::erasure_journal::ErasureJournal,
    archive: &crate::historical_operational_archive::VerifiedArchive,
) -> StorageResult<()> {
    operational_archive::verify_erasure_access(journal, archive)
}
