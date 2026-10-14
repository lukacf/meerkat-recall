//! Prepare retained maintenance effects without mutating the live store.
//!
//! The overlay preserves the existing decision algorithms while collecting a
//! typed read set and complete effect set. Only `CanonicalRepository` writes
//! live records, outbox events and the durable receipt. An error at any point
//! in preparation discards the overlay.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::Utc;
use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
use elephant_storage::canonical_commit::{CanonicalPlan, CanonicalRecord, CanonicalRepository};
use elephant_storage::{Repository, StorageClient};
use elephant_types::publication::AttemptFence;
use elephant_types::records::base::BaseFields;
use elephant_types::records::*;
use elephant_types::{RecordId, TableName};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::PipelineError;
use crate::truth_maintenance::{RepositoryTruthMaintenanceStorage, TruthMaintenanceStorage};

fn invalid(message: impl Into<String>) -> PipelineError {
    PipelineError::InvalidState(message.into())
}

// Keep nested evidence branches out of their caller's async state machine.
// Future size alone does not bound the stack used while polling descendants;
// these static phases also distinguish preparation from transaction entry.
fn boxed_maintenance_phase<F: std::future::Future>(
    phase: &'static str,
    future: F,
) -> std::pin::Pin<Box<F>> {
    tracing::debug!(phase, future_bytes = std::mem::size_of::<F>());
    Box::pin(future)
}

fn baseline_security(security: &Security) -> bool {
    matches!(security.policy_id.as_str(), "" | "default")
        && security.level == 0
        && security.labels.is_empty()
        && security.handling.is_empty()
}

fn same<T: Serialize>(left: &T, right: &T) -> Result<bool, PipelineError> {
    Ok(
        serde_json::to_value(left).map_err(|e| invalid(e.to_string()))?
            == serde_json::to_value(right).map_err(|e| invalid(e.to_string()))?,
    )
}

/// A closed historical claim still describes its captured evidence. Only the
/// exact original payload may use that evidence after assertion re-resolution;
/// a new or otherwise changed claim must satisfy the prepared assertion scope.
fn retirement_uses_original_evidence(
    before: Option<&CanonicalRecord>,
    after: &CanonicalRecord,
) -> Result<bool, PipelineError> {
    let Some(before) = before else {
        return Ok(false);
    };
    if before.base().deleted_at.is_some() || after.base().deleted_at.is_none() {
        return Ok(false);
    }
    let mut original_payload = after.clone();
    macro_rules! normalize_retirement {
        ($old:expr, $new:expr, $superseded:path) => {{
            if $old.status == $superseded || $new.status != $superseded {
                return Ok(false);
            }
            $new.status = $old.status;
            $new.base.deleted_at = $old.base.deleted_at;
            $new.base.updated_at = $old.base.updated_at;
        }};
    }
    match (before, &mut original_payload) {
        (CanonicalRecord::Rel(old), CanonicalRecord::Rel(new)) => {
            normalize_retirement!(
                old,
                new,
                elephant_types::records::rel::ClaimStatus::Superseded
            );
        }
        (CanonicalRecord::Attribute(old), CanonicalRecord::Attribute(new)) => {
            normalize_retirement!(
                old,
                new,
                elephant_types::records::rel::ClaimStatus::Superseded
            );
        }
        (CanonicalRecord::Event(old), CanonicalRecord::Event(new)) => {
            normalize_retirement!(
                old,
                new,
                elephant_types::records::event::EventStatus::Superseded
            );
        }
        _ => return Ok(false),
    }
    same(before, &original_payload)
}

/// Stable semantic request identity, independent of output timestamps and IDs.
pub fn request_fingerprint<T: Serialize>(request: &T) -> Result<String, PipelineError> {
    // Canonical JSON object ordering makes HashMap insertion order irrelevant.
    let mut value = serde_json::to_value(request).map_err(|e| invalid(e.to_string()))?;
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(crate) trait PlanRecord: Clone + Serialize + Send + Sync {
    fn record(self) -> CanonicalRecord;
    fn from_record(record: &CanonicalRecord) -> Option<Self>;
    fn base(&self) -> &BaseFields;
}

macro_rules! plan_record {
    ($($ty:ident),+ $(,)?) => {$(
        impl PlanRecord for $ty {
            fn record(self) -> CanonicalRecord { CanonicalRecord::$ty(Box::new(self)) }
            fn from_record(record: &CanonicalRecord) -> Option<Self> {
                match record { CanonicalRecord::$ty(row) => Some((**row).clone()), _ => None }
            }
            fn base(&self) -> &BaseFields { &self.base }
        }
    )+};
}

plan_record!(
    TruthSlot,
    ConflictGroup,
    Rel,
    Attribute,
    Event,
    Entity,
    Assertion,
    Freshness,
    PredicateRegistry,
    PredicateAlias,
    AuditLog,
    Source,
    Doc,
    DocRevision,
    DocNode,
    ReprocessJob,
    IdentityCandidate,
    EntityMerge,
    IdMap,
    WorkItem,
    WorkRun,
    WorkArtifact,
    KnowledgeViewDefinition
);

impl PlanRecord for CanonicalRecord {
    fn record(self) -> CanonicalRecord {
        self
    }
    fn from_record(record: &CanonicalRecord) -> Option<Self> {
        Some(record.clone())
    }
    fn base(&self) -> &BaseFields {
        CanonicalRecord::base(self)
    }
}

/// Physical source rows are contributors in their own right, even when their
/// own provenance is empty. This is the exact source union for a wiki inventory.
pub(crate) fn wiki_inventory_sources(contributors: &[CanonicalRecord]) -> Vec<RecordId> {
    let mut sources = Vec::new();
    for contributor in contributors {
        sources.extend(contributor.base().provenance.sources.iter().cloned());
        if matches!(contributor, CanonicalRecord::Source(_)) {
            sources.push(contributor.base().id.clone());
        }
    }
    sources.sort();
    sources.dedup();
    sources
}

/// A read-your-writes overlay over one space and one captured canonical epoch.
/// The live client is never exposed to decision code through this adapter.
pub struct PlanningTruthMaintenanceStorage {
    client: StorageClient,
    reader: RepositoryTruthMaintenanceStorage,
    space_id: String,
    attempt: Option<AttemptFence>,
    plan: Mutex<CanonicalPlan>,
    classification_repairs: Mutex<BTreeMap<RecordId, ClassificationRepair>>,
}

#[derive(Clone)]
struct ClassificationRepair {
    envelope: BaseFields,
    documents: Vec<RecordId>,
}

impl PlanningTruthMaintenanceStorage {
    /// Capture the epoch before the first decision or lineage read.
    pub async fn new(
        client: StorageClient,
        space_id: &str,
        attempt: Option<AttemptFence>,
    ) -> Result<Self, PipelineError> {
        if attempt
            .as_ref()
            .is_some_and(|fence| fence.space_id != space_id)
        {
            return Err(invalid("maintenance attempt belongs to another space"));
        }
        let observed_epoch = CanonicalRepository::new(client.clone())
            .epoch(space_id)
            .await?;
        Ok(Self {
            reader: RepositoryTruthMaintenanceStorage::new(client.clone()),
            client,
            space_id: space_id.to_owned(),
            attempt,
            plan: Mutex::new(CanonicalPlan {
                observed_epoch,
                ..CanonicalPlan::default()
            }),
            classification_repairs: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn has_attempt(&self) -> bool {
        self.attempt.is_some()
    }

    /// Keep an independent preparation branch at the same original epoch and
    /// attempt. Dropping a speculative branch cannot retain any of its effects.
    pub(crate) fn fork(&self) -> Result<Self, PipelineError> {
        Ok(Self {
            client: self.client.clone(),
            reader: RepositoryTruthMaintenanceStorage::new(self.client.clone()),
            space_id: self.space_id.clone(),
            attempt: self.attempt.clone(),
            plan: Mutex::new(self.prepared_plan()?),
            classification_repairs: Mutex::new(
                self.classification_repairs
                    .lock()
                    .map_err(|_| invalid("classification plan lock poisoned"))?
                    .clone(),
            ),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, CanonicalPlan>, PipelineError> {
        self.plan
            .lock()
            .map_err(|_| invalid("maintenance plan lock poisoned"))
    }

    fn require_space(&self, space: &str) -> Result<(), PipelineError> {
        if space != self.space_id {
            return Err(invalid("maintenance crossed space boundary"));
        }
        Ok(())
    }

    pub(crate) fn observe<T: PlanRecord>(&self, row: &T) -> Result<(), PipelineError> {
        self.require_space(&row.base().space_id)?;
        let record = row.clone().record();
        let mut plan = self.lock()?;
        if let Some(effect) = plan.effects.iter().find(|effect| {
            effect
                .after
                .as_ref()
                .or(effect.before.as_ref())
                .is_some_and(|value| value.base().id == row.base().id)
        }) {
            if let Some(after) = &effect.after {
                if same(after, &record)? {
                    return Ok(());
                }
            }
            if !effect
                .before
                .as_ref()
                .is_some_and(|before| same(before, &record).unwrap_or(false))
            {
                return Err(invalid(
                    "observed row is neither original nor currently prepared",
                ));
            }
        }
        if let Some(previous) = plan
            .dependencies
            .iter()
            .find(|r| r.base().id == row.base().id)
        {
            if !same(previous, &record)? {
                return Err(invalid(
                    "maintenance input changed while preparing; reprepare",
                ));
            }
        } else {
            plan.dependencies.push(record);
        }
        Ok(())
    }

    fn overlay<T: PlanRecord>(&self, id: &RecordId) -> Result<Option<Option<T>>, PipelineError> {
        let plan = self.lock()?;
        Ok(plan.effects.iter().find_map(|effect| {
            let row = effect.after.as_ref().or(effect.before.as_ref())?;
            (&row.base().id == id).then(|| effect.after.as_ref().and_then(T::from_record))
        }))
    }

    fn observed<T: PlanRecord>(
        &self,
        live: Option<T>,
        id: &RecordId,
        deleted: bool,
    ) -> Result<Option<T>, PipelineError> {
        if let Some(row) = &live {
            self.observe(row)?;
        }
        self.preview_row(live, id, deleted)
    }

    /// Inspect the overlay during discovery without assigning unused rows as
    /// causal inputs. Selected records must be observed before preparing effects.
    pub(crate) fn preview_row<T: PlanRecord>(
        &self,
        live: Option<T>,
        id: &RecordId,
        deleted: bool,
    ) -> Result<Option<T>, PipelineError> {
        if let Some(row) = &live {
            self.require_space(&row.base().space_id)?;
        }
        let result = self.overlay(id)?.unwrap_or(live);
        Ok(result.filter(|row| deleted || row.base().deleted_at.is_none()))
    }

    /// Apply prepared effects to a real repository row while capturing its
    /// original snapshot. Bounded sweep helpers use this for read-your-writes.
    pub(crate) fn current_row<T: PlanRecord>(
        &self,
        live: Option<T>,
        id: &RecordId,
        with_deleted: bool,
    ) -> Result<Option<T>, PipelineError> {
        self.observed(live, id, with_deleted)
    }

    /// Apply the complete overlay to a materialized query, including newly
    /// created rows, then re-evaluate its predicate against current values.
    pub(crate) fn current_rows<T: PlanRecord>(
        &self,
        live: Vec<T>,
        predicate: impl Fn(&T) -> bool,
    ) -> Result<Vec<T>, PipelineError> {
        self.selected(live, predicate)
    }

    /// Predicate discovery is fenced by the captured epoch. Call observe (or
    /// stage a complete before-image) for rows actually used by a decision.
    pub(crate) fn preview_rows<T: PlanRecord>(
        &self,
        live: Vec<T>,
        predicate: impl Fn(&T) -> bool,
    ) -> Result<Vec<T>, PipelineError> {
        self.project_rows(live, predicate, false)
    }

    fn selected<T: PlanRecord>(
        &self,
        live: Vec<T>,
        predicate: impl Fn(&T) -> bool,
    ) -> Result<Vec<T>, PipelineError> {
        self.project_rows(live, predicate, true)
    }

    fn project_rows<T: PlanRecord>(
        &self,
        live: Vec<T>,
        predicate: impl Fn(&T) -> bool,
        observe: bool,
    ) -> Result<Vec<T>, PipelineError> {
        let mut rows = BTreeMap::new();
        for row in live {
            self.require_space(&row.base().space_id)?;
            if observe {
                self.observe(&row)?;
            }
            rows.insert(row.base().id.to_string(), row);
        }
        for effect in &self.lock()?.effects {
            if let Some(row) = effect.before.as_ref().and_then(T::from_record) {
                rows.remove(&row.base().id.to_string());
            }
            if let Some(row) = effect.after.as_ref().and_then(T::from_record) {
                rows.insert(row.base().id.to_string(), row);
            }
        }
        Ok(rows
            .into_values()
            .filter(|row| row.base().deleted_at.is_none() && predicate(row))
            .collect())
    }

    pub(crate) fn stage<T: PlanRecord>(
        &self,
        before: Option<T>,
        after: T,
    ) -> Result<T, PipelineError> {
        self.stage_checked(before, after, false)
    }

    /// Explicitly tighten subject requirements without changing security policy,
    /// completeness or original source ownership. Ordinary stage stays exact.
    pub(crate) fn stage_restricted<T: PlanRecord>(
        &self,
        before: Option<T>,
        after: T,
    ) -> Result<T, PipelineError> {
        self.stage_checked(before, after, true)
    }

    /// A derived decision may retain stricter restrictions from actual observed
    /// context. It cannot change policy domains or drop any original authority.
    pub(crate) fn stage_inherited<T: PlanRecord>(
        &self,
        before: Option<T>,
        after: T,
        contributors: &[BaseFields],
    ) -> Result<T, PipelineError> {
        self.require_space(&after.base().space_id)?;
        if let Some(old) = &before {
            if old.base().id != after.base().id
                || !contains_authorization(after.base(), old.base())?
                || old
                    .base()
                    .provenance
                    .sources
                    .iter()
                    .any(|source| !after.base().provenance.sources.contains(source))
            {
                return Err(invalid("derived decision removed original authority"));
            }
        }
        for input in contributors {
            let captured = {
                let plan = self.lock()?;
                let mut found = false;
                for row in plan.dependencies.iter().chain(
                    plan.effects
                        .iter()
                        .flat_map(|effect| effect.before.iter().chain(effect.after.iter())),
                ) {
                    if row.base().id == input.id && same(input, row.base())? {
                        found = true;
                        break;
                    }
                }
                found
            };
            if !captured
                || !contains_authorization(after.base(), input)?
                || input
                    .provenance
                    .sources
                    .iter()
                    .any(|source| !after.base().provenance.sources.contains(source))
            {
                return Err(invalid(
                    "derived decision omitted actual observed context restrictions",
                ));
            }
        }
        self.lock()?
            .push(before.map(PlanRecord::record), Some(after.clone().record()))?;
        Ok(after)
    }

    /// Replace an entire generated definition from a fresh, captured inventory.
    /// Unlike claim mutation, regeneration may drop obsolete contribution scope;
    /// no old semantic payload is inherited by this type-specific operation.
    pub(crate) fn stage_wiki_definition(
        &self,
        before: Option<KnowledgeViewDefinition>,
        after: KnowledgeViewDefinition,
        contributors: &[CanonicalRecord],
    ) -> Result<KnowledgeViewDefinition, PipelineError> {
        self.require_space(&after.base.space_id)?;
        if contributors.is_empty() || after.base.deleted_at.is_some() {
            return Err(invalid(
                "wiki definition requires a live complete inventory",
            ));
        }
        if let Some(old) = &before {
            if old.base.id != after.base.id
                || old.base.space_id != after.base.space_id
                || old.base.deleted_at.is_some()
                || old.base.created_at != after.base.created_at
                || old.base.created_by != after.base.created_by
            {
                return Err(invalid(
                    "wiki regeneration changed identity or revived a definition",
                ));
            }
        }
        for contributor in contributors {
            self.observe(contributor)?;
            let envelope = elephant_storage::canonical_definition::input_envelope(contributor)?;
            if contributor.base().deleted_at.is_some()
                || !contains_authorization(&after.base, &envelope)?
            {
                return Err(invalid("wiki definition omitted contributing authority"));
            }
        }
        if wiki_inventory_sources(contributors) != after.base.provenance.sources {
            return Err(invalid(
                "wiki definition source union differs from captured inventory",
            ));
        }
        self.lock()?
            .push(before.map(PlanRecord::record), Some(after.clone().record()))?;
        Ok(after)
    }

    /// Opt-in repair of an unclassified historical derived row. The complete
    /// semantic payload and source ownership remain unchanged; actual document
    /// dependencies determine the only permitted replacement envelope.
    pub(crate) fn stage_source_classification<T: PlanRecord>(
        &self,
        before: T,
        after: T,
        docs: &[Doc],
    ) -> Result<T, PipelineError> {
        if !matches!(
            before.base().id.table(),
            TableName::Assertion | TableName::Rel | TableName::Attribute | TableName::Event
        ) || !baseline_security(&before.base().security)
            || docs.is_empty()
        {
            return Err(invalid(
                "classification repair requires baseline derived content and complete documents",
            ));
        }
        self.require_space(&before.base().space_id)?;
        self.require_space(&after.base().space_id)?;
        let mut inherited = before.base().clone();
        inherited.security.clone_from(&docs[0].base.security);
        for doc in docs {
            if doc.base.deleted_at.is_some()
                || doc.base.security.policy_id != inherited.security.policy_id
            {
                return Err(invalid(
                    "classification documents have incompatible policies",
                ));
            }
            match self.overlay::<Doc>(&doc.base.id)? {
                None => self.observe(doc)?,
                Some(Some(current)) if same(&current, doc)? => {}
                _ => {
                    return Err(invalid(
                        "classification document differs from prepared state",
                    ))
                }
            }
            elephant_storage::public_mutation::inherit_public_envelope(&mut inherited, &doc.base)?;
        }
        let mut required = inherited.security;
        required.labels.sort();
        required.labels.dedup();
        required.handling.sort();
        required.handling.dedup();
        let mut actual = after.base().security.clone();
        actual.labels.sort();
        actual.labels.dedup();
        actual.handling.sort();
        actual.handling.dedup();
        if baseline_security(&required)
            || actual != required
            || after.base().subjects_state != inherited.subjects_state
            || canonical_strings(
                &inherited
                    .subjects
                    .iter()
                    .map(|link| link.id())
                    .collect::<Vec<_>>(),
            ) != canonical_strings(
                &after
                    .base()
                    .subjects
                    .iter()
                    .map(|link| link.id())
                    .collect::<Vec<_>>(),
            )
        {
            return Err(invalid(
                "classification repair does not inherit the exact document requirements",
            ));
        }
        let comparable = |row: &T| -> Result<serde_json::Value, PipelineError> {
            let mut value =
                serde_json::to_value(row).map_err(|error| invalid(error.to_string()))?;
            let object = value
                .as_object_mut()
                .ok_or_else(|| invalid("invalid classification record"))?;
            object.remove("security");
            object.remove("subjects");
            object.remove("subjects_state");
            object.remove("updated_at");
            Ok(value)
        };
        if comparable(&before)? != comparable(&after)? {
            return Err(invalid(
                "classification repair changed semantic content, completeness or ownership",
            ));
        }
        self.lock()?
            .push(Some(before.record()), Some(after.clone().record()))?;
        self.classification_repairs
            .lock()
            .map_err(|_| invalid("classification plan lock poisoned"))?
            .insert(
                after.base().id.clone(),
                ClassificationRepair {
                    envelope: after.base().clone(),
                    documents: docs.iter().map(|doc| doc.base.id.clone()).collect(),
                },
            );
        Ok(after)
    }

    fn evidence_classification(&self, base: &mut BaseFields) -> Result<(), PipelineError> {
        if !baseline_security(&base.security) {
            return Ok(());
        }
        if let Some(verified) = self
            .classification_repairs
            .lock()
            .map_err(|_| invalid("classification plan lock poisoned"))?
            .get(&base.id)
        {
            // Validation only: the actual original row stays in the exact
            // canonical before-image, source fingerprint and transaction fence.
            base.security.clone_from(&verified.envelope.security);
            base.subjects.clone_from(&verified.envelope.subjects);
            base.subjects_state = verified.envelope.subjects_state;
        }
        Ok(())
    }

    fn stage_checked<T: PlanRecord>(
        &self,
        before: Option<T>,
        after: T,
        restrict_subjects: bool,
    ) -> Result<T, PipelineError> {
        self.require_space(&after.base().space_id)?;
        if let Some(previous) = &before {
            if previous.base().id != after.base().id {
                return Err(invalid("maintenance changed record identity"));
            }
            let permitted = if restrict_subjects {
                contains_subject_authorization(after.base(), previous.base())?
            } else {
                same_authorization(previous.base(), after.base())?
            };
            if !permitted {
                return Err(invalid("maintenance changed security partition"));
            }
            // Raw claims keep their original independent contribution. Derived
            // aggregates may replace their support set after membership shrinks;
            // the canonical causal inverse guards that entire decision.
            if !matches!(
                after.base().id.table(),
                TableName::TruthSlot | TableName::ConflictGroup | TableName::PredicateRegistry
            ) && previous
                .base()
                .provenance
                .sources
                .iter()
                .any(|id| !after.base().provenance.sources.contains(id))
            {
                return Err(invalid("maintenance removed original source ownership"));
            }
        }
        self.lock()?
            .push(before.map(PlanRecord::record), Some(after.clone().record()))?;
        Ok(after)
    }

    /// Return a coherent typed plan for storage-owner validation or fault tests.
    pub fn prepared_plan(&self) -> Result<CanonicalPlan, PipelineError> {
        Ok(self.lock()?.clone())
    }

    /// Adopt an owner-prepared extension of the exact current overlay. The
    /// owner planner must preserve the original before-images and effects.
    pub(crate) fn adopt_plan(
        &self,
        expected: &CanonicalPlan,
        replacement: CanonicalPlan,
    ) -> Result<(), PipelineError> {
        let mut guard = self.lock()?;
        if replacement.observed_epoch != guard.observed_epoch || !same(&*guard, expected)? {
            return Err(invalid(
                "maintenance overlay changed during owner preparation",
            ));
        }
        for dependency in &guard.dependencies {
            let retained = replacement
                .dependencies
                .iter()
                .find(|row| row.base().id == dependency.base().id)
                .ok_or_else(|| invalid("owner preparation dropped a captured dependency"))?;
            if !same(dependency, retained)? {
                return Err(invalid("owner preparation replaced a captured dependency"));
            }
        }
        for effect in &guard.effects {
            let row = effect
                .after
                .as_ref()
                .or(effect.before.as_ref())
                .ok_or_else(|| invalid("empty prior effect"))?;
            let retained = replacement
                .effects
                .iter()
                .find(|next| {
                    next.after
                        .as_ref()
                        .or(next.before.as_ref())
                        .is_some_and(|next| next.base().id == row.base().id)
                })
                .ok_or_else(|| invalid("owner preparation dropped an earlier effect"))?;
            if !same(&effect.before, &retained.before)? {
                return Err(invalid(
                    "owner preparation changed an original before-image",
                ));
            }
        }
        for effect in &guard.extension_effects {
            let retained = replacement
                .extension_effects
                .iter()
                .find(|next| next.id == effect.id)
                .ok_or_else(|| invalid("owner preparation dropped an extension effect"))?;
            if effect.before != retained.before {
                return Err(invalid("owner preparation changed extension before-image"));
            }
        }
        for event in &guard.outbox {
            let retained = replacement
                .outbox
                .iter()
                .find(|next| next.base.id == event.base.id)
                .ok_or_else(|| invalid("owner preparation dropped a prepared notification"))?;
            if !same(event, retained)? {
                return Err(invalid(
                    "owner preparation replaced a prepared notification",
                ));
            }
        }
        for row in replacement.dependencies.iter().chain(
            replacement
                .effects
                .iter()
                .flat_map(|effect| effect.before.iter().chain(effect.after.iter())),
        ) {
            self.require_space(&row.base().space_id)?;
        }
        *guard = replacement;
        Ok(())
    }

    async fn all_slots(&self) -> Result<Vec<TruthSlot>, PipelineError> {
        // Page through every affected row; never silently truncate invalidation.
        let repo = elephant_storage::TruthSlotRepository::new(self.client.clone());
        let mut rows = Vec::new();
        let mut cursor = None;
        loop {
            let page = repo.list(&self.space_id, 256, cursor.as_ref()).await?;
            let count = page.len();
            let next = page.last().map(|row| row.base.id.clone());
            rows.extend(page);
            if count < 256 {
                break;
            }
            if next == cursor {
                return Err(invalid("truth slot pagination did not advance"));
            }
            cursor = next;
        }
        self.selected(rows, |_| true)
    }

    async fn stale_slots(
        &self,
        reason: &str,
        predicate: impl Fn(&TruthSlot) -> bool,
    ) -> Result<(), PipelineError> {
        for before in self.all_slots().await?.into_iter().filter(predicate) {
            let mut after = before.clone();
            after.stale = true;
            after.stale_reason = Some(reason.to_owned());
            after.base.updated_at = Utc::now();
            self.stage(Some(before), after)?;
        }
        Ok(())
    }
}

#[async_trait]
impl TruthMaintenanceStorage for PlanningTruthMaintenanceStorage {
    async fn has_source_support(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<bool, PipelineError> {
        self.require_space(space_id)?;
        Ok(
            elephant_storage::source_publication::SourcePublicationRepository::new(
                self.client.clone(),
            )
            .has_causal_source_support(space_id, id)
            .await?,
        )
    }
    fn is_preparing(&self) -> bool {
        true
    }
    fn prepared_epoch(&self) -> Result<Option<String>, PipelineError> {
        Ok(Some(self.lock()?.observed_epoch.clone()))
    }
    async fn get_rel(&self, space_id: &str, id: &RecordId) -> Result<Option<Rel>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_rel(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_rel_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Rel>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_rel_with_deleted(space_id, id).await?;
        self.observed(live, id, true)
    }
    async fn get_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_attribute(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_attribute_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Attribute>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_attribute_with_deleted(space_id, id).await?;
        self.observed(live, id, true)
    }
    async fn get_event(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_event(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_event_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Event>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_event_with_deleted(space_id, id).await?;
        self.observed(live, id, true)
    }
    async fn get_entity(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Entity>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_entity(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_assertion(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<Assertion>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_assertion(space_id, id).await?;
        let row = self.observed(live, id, false)?;
        if let Some(assertion) = &row {
            self.validate_assertion_evidence(assertion).await?;
        }
        Ok(row)
    }
    async fn get_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_conflict_group(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_predicate_registry(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateRegistry>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_predicate_registry(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn get_predicate_alias(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> Result<Option<PredicateAlias>, PipelineError> {
        self.require_space(space_id)?;
        let live = self.reader.get_predicate_alias(space_id, id).await?;
        self.observed(live, id, false)
    }
    async fn find_slot_by_key(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<TruthSlot>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_slot_by_key(space_id, slot_key)
            .await?
            .into_iter()
            .collect();
        self.selected(live, |row| row.slot_key == slot_key)
            .map(|rows| rows.into_iter().next())
    }
    async fn find_unresolved_conflict(
        &self,
        space_id: &str,
        slot_key: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_unresolved_conflict(space_id, slot_key)
            .await?
            .into_iter()
            .collect();
        self.selected(live, |row| row.slot_key == slot_key && !row.resolved)
            .map(|rows| rows.into_iter().next())
    }
    async fn find_unresolved_conflict_by_type(
        &self,
        space_id: &str,
        slot_key: &str,
        slot_type: &str,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_unresolved_conflict_by_type(space_id, slot_key, slot_type)
            .await?
            .into_iter()
            .collect();
        self.selected(live, |row| {
            row.slot_key == slot_key && row.slot_type == slot_type && !row.resolved
        })
        .map(|rows| rows.into_iter().next())
    }
    async fn find_predicate_registry_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Option<PredicateRegistry>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_predicate_registry_by_predicate_id(space_id, predicate_id)
            .await?
            .into_iter()
            .collect();
        self.selected(live, |row| row.predicate_id == predicate_id)
            .map(|rows| rows.into_iter().next())
    }
    async fn find_rels_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_rels_for_slot(space_id, subject_id, object_id, rel_type)
            .await?;
        self.selected(live, |row| {
            row.subject.id() == subject_id
                && row.object.id() == object_id
                && row.rel_type == rel_type
        })
    }
    async fn find_rels_for_subject_predicate(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        rel_type: &str,
    ) -> Result<Vec<Rel>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_rels_for_subject_predicate(space_id, subject_id, rel_type)
            .await?;
        self.selected(live, |row| {
            row.subject.id() == subject_id && row.rel_type == rel_type
        })
    }
    async fn find_attrs_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        attr_type: &str,
    ) -> Result<Vec<Attribute>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .find_attrs_for_slot(space_id, subject_id, attr_type)
            .await?;
        self.selected(live, |row| {
            row.subject.id() == subject_id && row.attr_type == attr_type
        })
    }
    async fn list_predicate_aliases_by_predicate_id(
        &self,
        space_id: &str,
        predicate_id: &str,
    ) -> Result<Vec<PredicateAlias>, PipelineError> {
        self.require_space(space_id)?;
        let live = self
            .reader
            .list_predicate_aliases_by_predicate_id(space_id, predicate_id)
            .await?;
        self.selected(live, |row| row.predicate_id == predicate_id)
    }
    async fn update_rel(
        &self,
        space_id: &str,
        id: &RecordId,
        mut row: Rel,
    ) -> Result<Rel, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .get_rel_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        if !same_partition(&before.base, &row.base)? {
            return Err(invalid("claim update supplied unearned support"));
        }
        self.validate_rel_evidence_at(&before, false).await?;
        for evidence in &row.evidence {
            let assertion = self
                .get_assertion(space_id, evidence.assertion.id())
                .await?
                .ok_or_else(|| invalid("claim evidence assertion is missing"))?;
            merge_evidence_authority(&mut row.base, &assertion.base)?;
        }
        self.validate_rel_evidence_at(&row, true).await?;
        self.stage(Some(before), row)
    }
    async fn delete_rel(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        let before = self
            .get_rel_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        let mut after = before.clone();
        after.base.deleted_at = Some(Utc::now());
        after.base.updated_at = Utc::now();
        self.stage(Some(before), after)?;
        Ok(())
    }
    async fn update_attribute(
        &self,
        space_id: &str,
        id: &RecordId,
        mut row: Attribute,
    ) -> Result<Attribute, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .get_attribute_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        if !same_partition(&before.base, &row.base)? {
            return Err(invalid("claim update supplied unearned support"));
        }
        self.validate_claim_evidence(&before.base, &before.evidence)
            .await?;
        for evidence in &row.evidence {
            let assertion = self
                .get_assertion(space_id, evidence.assertion.id())
                .await?
                .ok_or_else(|| invalid("claim evidence assertion is missing"))?;
            merge_evidence_authority(&mut row.base, &assertion.base)?;
        }
        self.validate_claim_evidence(&row.base, &row.evidence)
            .await?;
        self.stage(Some(before), row)
    }
    async fn delete_attribute(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        let before = self
            .get_attribute_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        let mut after = before.clone();
        after.base.deleted_at = Some(Utc::now());
        after.base.updated_at = Utc::now();
        self.stage(Some(before), after)?;
        Ok(())
    }
    async fn update_event(
        &self,
        space_id: &str,
        id: &RecordId,
        mut row: Event,
    ) -> Result<Event, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .get_event_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        if !same_partition(&before.base, &row.base)? {
            return Err(invalid("event update supplied unearned support"));
        }
        self.validate_event_evidence(&before).await?;
        for reference in &row.assertion_refs {
            let assertion = self
                .get_assertion(space_id, reference.id())
                .await?
                .ok_or_else(|| invalid("event evidence assertion is missing"))?;
            merge_evidence_authority(&mut row.base, &assertion.base)?;
        }
        self.validate_event_evidence(&row).await?;
        self.stage(Some(before), row)
    }
    async fn delete_event(&self, space_id: &str, id: &RecordId) -> Result<(), PipelineError> {
        let before = self
            .get_event_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        let mut after = before.clone();
        after.base.deleted_at = Some(Utc::now());
        after.base.updated_at = Utc::now();
        self.stage(Some(before), after)?;
        Ok(())
    }
    async fn update_conflict_group(
        &self,
        space_id: &str,
        id: &RecordId,
        row: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .get_conflict_group(space_id, id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        self.stage(Some(before), row)
    }

    async fn create_slot(&self, mut slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
        if self
            .find_slot_by_key(&slot.base.space_id, &slot.slot_key)
            .await?
            .is_some()
        {
            return Err(invalid("truth slot already exists"));
        }
        let ids = slot
            .current_values
            .iter()
            .map(|value| value.claim_id.clone())
            .collect::<Vec<_>>();
        self.inherit_claims(&mut slot.base, &ids).await?;
        self.stage(None, slot)
    }

    async fn update_slot(
        &self,
        space_id: &str,
        id: &RecordId,
        slot: TruthSlot,
    ) -> Result<TruthSlot, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .find_slot_by_key(space_id, &slot.slot_key)
            .await?
            .filter(|row| &row.base.id == id)
            .ok_or_else(|| PipelineError::NotFound(id.to_string()))?;
        self.stage(Some(before), slot)
    }

    async fn upsert_slot(&self, slot: TruthSlot) -> Result<TruthSlot, PipelineError> {
        match self
            .find_slot_by_key(&slot.base.space_id, &slot.slot_key)
            .await?
        {
            Some(before) => self.stage(Some(before), slot),
            None => self.create_slot(slot).await,
        }
    }

    async fn create_conflict_group(
        &self,
        mut group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        if self
            .get_conflict_group(&group.base.space_id, &group.base.id)
            .await?
            .is_some()
        {
            return Err(invalid("conflict group already exists"));
        }
        group.base.provenance.sources.clear();
        self.inherit_claims(&mut group.base, &group.claim_ids)
            .await?;
        self.stage(None, group)
    }

    async fn upsert_unresolved_conflict_group(
        &self,
        mut group: ConflictGroup,
    ) -> Result<ConflictGroup, PipelineError> {
        let before = self
            .get_conflict_group(&group.base.space_id, &group.base.id)
            .await?;
        group.base.provenance.sources.clear();
        self.inherit_claims(&mut group.base, &group.claim_ids)
            .await?;
        group.base.updated_at = Utc::now();
        self.stage(before, group)
    }

    async fn merge_unresolved_conflict_claims(
        &self,
        space_id: &str,
        id: &RecordId,
        claim_ids: Vec<String>,
    ) -> Result<Option<ConflictGroup>, PipelineError> {
        let Some(before) = self
            .get_conflict_group(space_id, id)
            .await?
            .filter(|row| !row.resolved)
        else {
            return Ok(None);
        };
        let mut after = before.clone();
        after.claim_ids.extend(claim_ids);
        after.claim_ids.sort();
        after.claim_ids.dedup();
        after.base.provenance.sources.clear();
        self.inherit_claims(&mut after.base, &after.claim_ids)
            .await?;
        after.base.updated_at = Utc::now();
        self.stage(Some(before), after).map(Some)
    }

    async fn list_events(
        &self,
        space_id: &str,
        _limit: usize,
    ) -> Result<Vec<Event>, PipelineError> {
        self.require_space(space_id)?;
        let repo = elephant_storage::EventRepository::new(self.client.clone());
        let mut rows = Vec::new();
        let mut cursor = None;
        loop {
            let page = repo.list(space_id, 256, cursor.as_ref()).await?;
            let count = page.len();
            let next = page.last().map(|row| row.base.id.clone());
            rows.extend(page);
            if count < 256 {
                break;
            }
            if next == cursor {
                return Err(invalid("event pagination did not advance"));
            }
            cursor = next;
        }
        self.selected(rows, |_| true)
    }

    async fn find_stale_slots(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<TruthSlot>, PipelineError> {
        self.require_space(space_id)?;
        let mut slots = self
            .all_slots()
            .await?
            .into_iter()
            .filter(|slot| slot.stale)
            .collect::<Vec<_>>();
        slots.sort_by_key(|slot| (slot.computed_at, slot.base.id.to_string()));
        slots.truncate(limit);
        Ok(slots)
    }

    async fn upsert_predicate_semantics(
        &self,
        space_id: &str,
        predicate_id: &str,
        semantics: PredicateSemantics,
        status: SemanticsStatus,
        provenance_note: &str,
        authority: &BaseFields,
    ) -> Result<Option<PredicateRegistry>, PipelineError> {
        self.require_space(space_id)?;
        let before = self
            .find_predicate_registry_by_predicate_id(space_id, predicate_id)
            .await?;
        if let Some(row) = &before {
            if row.semantics_status == SemanticsStatus::Configured {
                return crate::truth_maintenance::scoped_predicate(
                    Some(row.clone()),
                    Some(authority),
                    self,
                )
                .await;
            }
            if !same_partition(&row.base, authority)? {
                return Ok(None);
            }
        }
        let mut after = before.clone().unwrap_or_else(|| PredicateRegistry {
            base: BaseFields::new(
                RecordId::new_v4(TableName::PredicateRegistry),
                space_id.to_owned(),
                "truth_maintenance".to_owned(),
            ),
            predicate_id: predicate_id.to_owned(),
            description: None,
            status: "active".to_owned(),
            semantics: None,
            semantics_status: status,
            first_seen_at: Some(Utc::now()),
            usage_count: 0,
            last_used_at: None,
        });
        if before.is_none() {
            inherit(&mut after.base, authority);
        }
        after.semantics = Some(semantics);
        after.semantics_status = status;
        after.base.updated_at = Utc::now();
        if after.description.is_none() && !provenance_note.is_empty() {
            after.description = Some(provenance_note.to_owned());
        }
        self.stage(before, after).map(Some)
    }

    async fn create_audit_log(&self, mut log: AuditLog) -> Result<AuditLog, PipelineError> {
        // Audit payloads can retain a source removed from the current aggregate.
        // Derive their envelope from both trusted target images, not just after.
        elephant_storage::canonical_commit::inherit_maintenance_audit_authority(
            &self.prepared_plan()?,
            &mut log,
        )?;
        self.stage(None, log)
    }

    async fn emit_system_event(&self, payload: OutboxPayload) -> Result<(), PipelineError> {
        self.require_space(&payload.space_id)?;
        // Partition each event by its affected record. IDs themselves disclose
        // protected lineage and must inherit that record's complete envelope.
        if payload.records.is_empty() {
            return Err(invalid("maintenance event has no affected records"));
        }
        for record in &payload.records {
            let source = self.record_base(&record.id)?;
            let mut base = BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                self.space_id.clone(),
                payload.actor.clone(),
            );
            inherit(&mut base, &source);
            let mut single = payload.clone();
            single.records = vec![record.clone()];
            self.lock()?.outbox.push(SystemEvent {
                base,
                seq: 0,
                event_type: single.event_type.clone(),
                payload: serde_json::to_value(single).map_err(|e| invalid(e.to_string()))?,
            });
        }
        Ok(())
    }

    async fn mark_slots_stale_by_predicate(
        &self,
        space_id: &str,
        predicate_id: &str,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.require_space(space_id)?;
        self.stale_slots(reason, |slot| slot.predicate_id == predicate_id)
            .await
    }

    async fn mark_slots_stale_by_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.require_space(space_id)?;
        let key = entity_id.to_string();
        self.stale_slots(reason, |slot| {
            slot.subject_entity == key || slot.slot_key.contains(&key)
        })
        .await
    }

    async fn mark_all_slots_stale(
        &self,
        space_id: &str,
        reason: &str,
    ) -> Result<(), PipelineError> {
        self.require_space(space_id)?;
        self.stale_slots(reason, |_| true).await
    }

    async fn find_freshness_score(
        &self,
        space_id: &str,
        target: &str,
    ) -> Result<Option<f32>, PipelineError> {
        self.require_space(space_id)?;
        let row = elephant_storage::FreshnessRepository::new(self.client.clone())
            .find_by_target(space_id, target)
            .await?;
        if let Some(row) = &row {
            self.observe(row)?;
        }
        Ok(row.map(|row| row.score))
    }
}

/// Conjunction authority for a decision, distinct from independent raw support.
/// Canonical storage records causal inverses before these effects can publish.
pub(crate) fn merge_authority(
    target: &mut BaseFields,
    source: &BaseFields,
) -> Result<(), PipelineError> {
    if !same_authorization(target, source)? {
        return Err(invalid("maintenance mixed security/subject partitions"));
    }
    target
        .provenance
        .sources
        .extend(source.provenance.sources.iter().cloned());
    target.provenance.sources.sort();
    target.provenance.sources.dedup();
    Ok(())
}

pub(crate) fn inherit(target: &mut BaseFields, source: &BaseFields) {
    target.security = source.security.clone();
    target.subjects = source.subjects.clone();
    target.subjects_state = source.subjects_state;
    target.provenance.sources = source.provenance.sources.clone();
}

impl PlanningTruthMaintenanceStorage {
    fn record_base(&self, id: &RecordId) -> Result<BaseFields, PipelineError> {
        let plan = self.lock()?;
        plan.effects
            .iter()
            .filter_map(|effect| effect.after.as_ref().or(effect.before.as_ref()))
            .chain(plan.dependencies.iter())
            .find(|row| &row.base().id == id)
            .map(|row| row.base().clone())
            .ok_or_else(|| invalid(format!("missing observed authority for {id}")))
    }

    async fn inherit_claims(
        &self,
        target: &mut BaseFields,
        claim_ids: &[String],
    ) -> Result<(), PipelineError> {
        let mut authority: Option<BaseFields> = None;
        for claim_id in claim_ids {
            let id: RecordId = claim_id
                .parse()
                .map_err(|e| invalid(format!("invalid claim ID: {e}")))?;
            let base = match id.table() {
                TableName::Rel => self
                    .get_rel_with_deleted(&self.space_id, &id)
                    .await?
                    .map(|row| row.base),
                TableName::Attribute => self
                    .get_attribute_with_deleted(&self.space_id, &id)
                    .await?
                    .map(|row| row.base),
                TableName::Event => self
                    .get_event_with_deleted(&self.space_id, &id)
                    .await?
                    .map(|row| row.base),
                _ => return Err(invalid("unsupported claim authority")),
            }
            .ok_or_else(|| PipelineError::NotFound(claim_id.clone()))?;
            if let Some(first) = &mut authority {
                merge_authority(first, &base)?;
            } else {
                authority = Some(base);
            }
        }
        if let Some(mut authority) = authority {
            if !target.provenance.sources.is_empty() {
                merge_authority(&mut authority, target)?;
            }
            inherit(target, &authority);
        } else if target.provenance.sources.is_empty() {
            return Err(invalid(
                "new derived maintenance record has no claim authority",
            ));
        }
        Ok(())
    }
}

impl PlanningTruthMaintenanceStorage {
    /// Start an outer preparation; nested algorithms share the existing overlay.
    pub(crate) async fn for_storage<S: TruthMaintenanceStorage>(
        storage: &S,
        space_id: &str,
    ) -> Result<Option<Self>, PipelineError> {
        if storage.is_preparing() {
            return Ok(None);
        }
        let client = storage.canonical_client().ok_or_else(|| {
            invalid("truth maintenance requires the canonical transaction adapter")
        })?;
        Self::new(client, space_id, storage.maintenance_attempt())
            .await
            .map(Some)
    }
}

impl PlanningTruthMaintenanceStorage {
    async fn validate_rel_evidence_at(
        &self,
        row: &Rel,
        prepared: bool,
    ) -> Result<(), PipelineError> {
        if !row.evidence.is_empty() || row.base.provenance.sources.is_empty() {
            return boxed_maintenance_phase(
                "maintenance.rel.claims",
                self.validate_claim_evidence_at(&row.base, &row.evidence, prepared),
            )
            .await;
        }
        if !row.confidence.is_finite() || !(0.0..=1.0).contains(&row.confidence) {
            return Err(invalid("structured contact confidence is invalid"));
        }
        let plan = self.prepared_plan()?;
        let originals = plan.dependencies.iter().chain(
            plan.effects
                .iter()
                .filter_map(|effect| effect.before.as_ref()),
        );
        let mut rels = BTreeMap::new();
        let mut entities = BTreeMap::new();
        for original in originals {
            match original {
                CanonicalRecord::Rel(rel) => {
                    rels.insert(rel.base.id.clone(), (**rel).clone());
                }
                CanonicalRecord::Entity(entity) => {
                    entities.insert(entity.base.id.clone(), (**entity).clone());
                }
                _ => {}
            }
        }
        if !rels.contains_key(&row.base.id) {
            if let Some(original) = elephant_storage::RelRepository::new(self.client.clone())
                .get(&self.space_id, &row.base.id)
                .await?
            {
                self.observe(&original)?;
                rels.insert(original.base.id.clone(), original);
            }
        }
        let entities = entities.into_values().collect::<Vec<_>>();
        let target_sources = row
            .base
            .provenance
            .sources
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut contributors = Vec::new();
        let mut covered = std::collections::BTreeSet::new();
        let original_id_exists = rels.contains_key(&row.base.id);
        let mut original_id_preserved = false;
        for original in rels.into_values() {
            if !prepared && original.base.id != row.base.id {
                continue;
            }
            if (!prepared && !retains_contact_fact(&original, row)?)
                || original.base.deleted_at.is_some()
                || !original.evidence.is_empty()
                || original
                    .base
                    .provenance
                    .sources
                    .iter()
                    .any(|id| !target_sources.contains(id))
                || !contains_authorization(&row.base, &original.base)?
                || !CanonicalRepository::contact_rel_fact_preserved(&original, row, &entities)?
            {
                continue;
            }
            if original.base.id == row.base.id {
                original_id_preserved = true;
            }
            covered.extend(original.base.provenance.sources.iter().cloned());
            contributors.push(original);
        }
        if contributors.is_empty()
            || covered != target_sources
            || (original_id_exists && !original_id_preserved)
            || (!prepared && !original_id_preserved)
        {
            return Err(invalid(
                "structured contact transition lacks its exact original fact/support",
            ));
        }
        let canonical = CanonicalRepository::new(self.client.clone());
        for original in contributors {
            self.observe(&original)?;
            // An independently authored original is a real baseline; it does
            // not manufacture source support or replace another contributor.
            for source_id in &original.base.provenance.sources {
                let source = elephant_storage::SourceRepository::new(self.client.clone())
                    .get(&self.space_id, source_id)
                    .await?
                    .ok_or_else(|| invalid("structured contact source is unavailable"))?;
                self.observe(&source)?;
                if !contains_authorization(&row.base, &source.base)?
                    || !canonical
                        .accepted_contact_rel_support(&self.space_id, &original, source_id)
                        .await?
                {
                    return Err(invalid(
                        "structured contact claim lacks current accepted support",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Merge actual duplicate facts without dropping an evidence-less contact's
    /// physical source ownership. This does not relax ordinary claim updates.
    pub(crate) async fn merge_relationships(
        &self,
        before: Rel,
        mut after: Rel,
        contributors: &[Rel],
    ) -> Result<Rel, PipelineError> {
        let mut sources = before
            .base
            .provenance
            .sources
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut bases = vec![before.base.clone()];
        self.observe(&before)?;
        for contributor in contributors {
            if contributor.subject != before.subject
                || contributor.object != before.object
                || contributor.rel_type != before.rel_type
                || contributor.validity_window != before.validity_window
                || !same_authorization(&before.base, &contributor.base)?
            {
                return Err(invalid(
                    "relationship dedupe crossed factual or authorization partition",
                ));
            }
            self.observe(contributor)?;
            sources.extend(contributor.base.provenance.sources.iter().cloned());
            bases.push(contributor.base.clone());
        }
        after.base.provenance.sources = sources.into_iter().collect();
        self.validate_rel_evidence_at(&after, true).await?;
        self.stage_inherited(Some(before), after, &bases)
    }

    async fn validate_claim_evidence(
        &self,
        base: &BaseFields,
        evidence: &[elephant_types::records::rel::EvidenceRef],
    ) -> Result<(), PipelineError> {
        self.validate_claim_evidence_at(base, evidence, true).await
    }

    async fn evidence_assertion(
        &self,
        id: &RecordId,
        prepared: bool,
    ) -> Result<Option<Assertion>, PipelineError> {
        if prepared {
            return self.get_assertion(&self.space_id, id).await;
        }
        let captured = {
            let plan = self.lock()?;
            plan.dependencies
                .iter()
                .chain(
                    plan.effects
                        .iter()
                        .filter_map(|effect| effect.before.as_ref()),
                )
                .find_map(|record| match record {
                    CanonicalRecord::Assertion(row) if &row.base.id == id => Some((**row).clone()),
                    _ => None,
                })
        };
        if captured.is_some() {
            return Ok(captured);
        }
        let live = self.reader.get_assertion(&self.space_id, id).await?;
        if let Some(row) = &live {
            self.observe(row)?;
        }
        Ok(live)
    }

    async fn validate_claim_evidence_at(
        &self,
        base: &BaseFields,
        evidence: &[elephant_types::records::rel::EvidenceRef],
        prepared: bool,
    ) -> Result<(), PipelineError> {
        // Independently authored source-less claims remain authoritative without
        // manufactured evidence. Source-backed claims require accepted support.
        if evidence.is_empty() && !base.provenance.sources.is_empty() {
            return Err(invalid(
                "source-backed claim has no immutable assertion evidence",
            ));
        }
        for evidence in evidence {
            let mut assertion = self
                .evidence_assertion(evidence.assertion.id(), prepared)
                .await?
                .ok_or_else(|| invalid("claim evidence assertion is missing"))?;
            self.evidence_classification(&mut assertion.base)?;
            if !same(&evidence.span, &assertion.evidence)? {
                return Err(invalid(
                    "claim evidence differs from its immutable assertion span",
                ));
            }
            if !contains_authorization(base, &assertion.base)?
                || assertion
                    .base
                    .provenance
                    .sources
                    .iter()
                    .any(|source| !base.provenance.sources.contains(source))
            {
                return Err(invalid(
                    "claim evidence crosses its captured support/security authority",
                ));
            }
            boxed_maintenance_phase(
                "maintenance.claim.assertion",
                self.validate_assertion_evidence(&assertion),
            )
            .await?;
        }
        Ok(())
    }

    /// Verify an existing source-backed assertion against accepted immutable input.
    /// Current source intake may contain a pending newer revision; it does not
    /// replace the published revision as evidence authority.
    pub(crate) async fn validate_assertion_evidence(
        &self,
        assertion: &Assertion,
    ) -> Result<(), PipelineError> {
        let revision = elephant_storage::DocRevisionRepository::new(self.client.clone())
            .get_by_id(assertion.evidence.revision.id())
            .await?;
        let Some(revision) = revision else {
            if assertion.base.provenance.sources.is_empty()
                && assertion.base.provenance.extractor.is_none()
                && !assertion
                    .base
                    .provenance
                    .chain
                    .iter()
                    .any(|step| step.step == "grounded_promotion")
            {
                // Independently authored assertions can have a domain-required
                // placeholder span; it never grants a source evidence receipt.
                return Ok(());
            }
            return Err(invalid("maintenance evidence revision is missing"));
        };
        self.observe(&revision)?;
        let doc = elephant_storage::DocRepository::new(self.client.clone())
            .get(&self.space_id, revision.doc.id())
            .await?
            .ok_or_else(|| invalid("maintenance evidence document is missing"))?;
        self.observe(&doc)?;
        for base in [&revision.base, &doc.base] {
            if !contains_authorization(&assertion.base, base)? || base.deleted_at.is_some() {
                return Err(invalid("maintenance evidence authorization changed"));
            }
        }
        if let Some(reference) = &doc.source {
            let source_id = reference.id();
            if canonical_strings(&assertion.base.provenance.sources) != vec![source_id.to_string()]
            {
                return Err(invalid(
                    "assertion source support is not the exact immutable evidence source",
                ));
            }
            let source = elephant_storage::SourceRepository::new(self.client.clone())
                .get(&self.space_id, source_id)
                .await?
                .ok_or_else(|| invalid("maintenance source was removed"))?;
            self.observe(&source)?;
            if !contains_authorization(&assertion.base, &source.base)? {
                return Err(invalid("maintenance source authorization changed"));
            }
            let repo = elephant_storage::source_publication::SourcePublicationRepository::new(
                self.client.clone(),
            );
            let head = repo.head(&self.space_id, source_id).await?;
            let bindings = revision
                .base
                .provenance
                .chain
                .iter()
                .filter(|step| step.step == "source_content")
                .collect::<Vec<_>>();
            if let Some(head) = head {
                if head.lifecycle != elephant_types::publication::SourceVisibility::Active {
                    return Err(invalid("maintenance evidence source is not active"));
                }
                let published = head
                    .published_revision
                    .as_deref()
                    .ok_or_else(|| invalid("maintenance source has no published support"))?;
                let accepted = repo
                    .revision(&self.space_id, source_id, published)
                    .await?
                    .ok_or_else(|| invalid("accepted immutable maintenance source is missing"))?;
                let mappings = elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(self.client.clone());
                let mapping = mappings
                    .source_revision_for_document_revision(&self.space_id, &revision.base.id)
                    .await?
                    .ok_or_else(|| invalid("maintenance immutable evidence mapping is missing"))?;
                let doc_mapping = mappings
                    .source_revision_for_document(&self.space_id, &doc.base.id)
                    .await?
                    .ok_or_else(|| invalid("maintenance immutable document mapping is missing"))?;
                if mapping != doc_mapping
                    || mapping.doc_id != doc.base.id
                    || mapping.doc_revision_id != revision.base.id
                    || mapping.publication.source_id != *source_id
                    || mapping.publication.revision_id != published
                {
                    return Err(invalid("maintenance evidence mapping does not identify the published source revision"));
                }
                // Rendered bytes may differ from the raw accepted template.
                // The shared reader verifies the ORIGINAL completed manifest,
                // accepted source and exact rendered Doc/Revision fingerprints;
                // a renderer marker or a matching hash alone grants nothing.
                let reader =
                    elephant_storage::read_eligibility::ReadEligibility::new(self.client.clone());
                let authorize = |base: &BaseFields| {
                    contains_authorization(&assertion.base, base).is_ok_and(|allowed| allowed)
                };
                let rendered = boxed_maintenance_phase(
                    "maintenance.assertion.render_proof",
                    reader.revision(&revision, &authorize),
                )
                .await?;
                if !rendered.is_some_and(|(current_doc, current_source)| {
                    current_doc == doc && current_source.as_ref() == Some(&source)
                }) {
                    return Err(invalid(
                        "maintenance evidence lacks its original completed render proof",
                    ));
                }
                boxed_maintenance_phase("maintenance.assertion.current", reader.verify_current())
                    .await?;
                if !contains_authorization(&assertion.base, &accepted.source.base)?
                    || bindings.len() != 1
                    || bindings[0].input != source_id.to_string()
                    || bindings[0].output != accepted.source.hash
                {
                    return Err(invalid(
                        "maintenance evidence does not bind the accepted published source",
                    ));
                }
            } else {
                // Explicit no-head lineage. The storage owner captures the full
                // live source snapshot and head absence in its Legacy fence.
                if doc.text != revision.text
                    || revision.status != RevisionStatus::Active
                    || matches!(
                        source.lifecycle_status,
                        SourceLifecycleStatus::Rejected | SourceLifecycleStatus::Skipped
                    )
                    || (!bindings.is_empty()
                        && (bindings.len() != 1
                            || bindings[0].input != source_id.to_string()
                            || bindings[0].output != source.hash))
                {
                    return Err(invalid("maintenance legacy source lineage changed"));
                }
            }
        } else if !assertion.base.provenance.sources.is_empty() {
            return Err(invalid("maintenance evidence document lost its source"));
        }
        let span = &assertion.evidence;
        let text = revision
            .text
            .get(span.start as usize..span.end as usize)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| invalid("invalid maintenance UTF-8 evidence span"))?;
        if crate::extraction::AssertionValidator::hash_quote(text) != span.quote_hash
            || crate::extraction::AssertionValidator::hash_quote(&revision.text)
                != revision.text_hash
        {
            return Err(invalid("maintenance evidence hash mismatch"));
        }
        if let Some(node_id) = &span.node {
            let node = elephant_storage::DocNodeRepository::new(self.client.clone())
                .get(&self.space_id, node_id.id())
                .await?
                .ok_or_else(|| invalid("maintenance evidence node is missing"))?;
            self.observe(&node)?;
            if node.revision.id() != &revision.base.id
                || !contains_authorization(&assertion.base, &node.base)?
                || !elephant_storage::read_eligibility::evidence_within_accepted_window(
                    &self.client,
                    &self.space_id,
                    &revision,
                    &node,
                    span,
                )
                .await?
            {
                return Err(invalid("maintenance evidence node lineage differs"));
            }
        }
        Ok(())
    }

    pub(crate) fn stage_assertion(
        &self,
        before: Assertion,
        after: Assertion,
    ) -> Result<Assertion, PipelineError> {
        if !contains_subject_authorization(&after.base, &before.base)?
            || canonical_strings(&before.base.provenance.sources)
                != canonical_strings(&after.base.provenance.sources)
        {
            return Err(invalid("resolution changed assertion support"));
        }
        self.stage_restricted(Some(before), after)
    }

    pub(crate) fn stage_artifact(
        &self,
        before: WorkArtifact,
        after: WorkArtifact,
    ) -> Result<WorkArtifact, PipelineError> {
        self.observe(&before)?;
        self.stage(Some(before), after)
    }
}

fn retains_contact_fact(before: &Rel, after: &Rel) -> Result<bool, PipelineError> {
    Ok(before.base.id == after.base.id
        && before.subject == after.subject
        && before.object == after.object
        && before.rel_type == after.rel_type
        && before.validity_window == after.validity_window
        && canonical_strings(&before.base.provenance.sources)
            == canonical_strings(&after.base.provenance.sources)
        && !before.base.provenance.sources.is_empty()
        && contains_authorization(&after.base, &before.base)?
        && after.confidence.is_finite()
        && (0.0..=1.0).contains(&after.confidence))
}

fn canonical_strings<T: ToString>(values: &[T]) -> Vec<String> {
    let mut values = values.iter().map(ToString::to_string).collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

impl PlanningTruthMaintenanceStorage {
    pub(crate) fn commit_identity(&self, kind: &str, scope: &str) -> Result<String, PipelineError> {
        // Work identity survives lease renewal/reclaim. Trusted direct TM calls
        // observe a fresh epoch; explicit resolution operation keys stay stable.
        let owner = self
            .attempt
            .as_ref()
            .map(|fence| fence.work_item_id.to_string())
            .unwrap_or_else(|| "direct".to_owned());
        let generation = if self.attempt.is_none()
            && !kind.starts_with("resolution")
            && !kind.starts_with("supersession")
        {
            self.lock()?.observed_epoch.clone()
        } else {
            String::new()
        };
        Ok(format!(
            "maintenance:{}",
            request_fingerprint(&(kind, scope, owner, generation))?
        ))
    }

    /// Replay the exact committed successor output before re-reading changed rows.
    pub(crate) async fn replay<R: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
    ) -> Result<Option<R>, PipelineError> {
        let key = self.commit_identity(kind, scope)?;
        let repo = CanonicalRepository::new(self.client.clone());
        let Some(receipt) = repo.receipt(&self.space_id, &key).await? else {
            return Ok(None);
        };
        if receipt.input_fingerprint != request_fingerprint(input)? {
            return Err(PipelineError::StorageError(
                elephant_storage::StorageError::Conflict(
                    "maintenance idempotency key has different semantic input".to_owned(),
                ),
            ));
        }
        let output = receipt
            .replay_output
            .ok_or_else(|| invalid("maintenance receipt has no durable successor output"))?;
        let value = output
            .get("value")
            .cloned()
            .ok_or_else(|| invalid("maintenance receipt output envelope is missing"))?;
        serde_json::from_value(value)
            .map(Some)
            .map_err(|e| invalid(format!("invalid maintenance receipt output: {e}")))
    }

    async fn emit_effect_events(&self, kind: &str) -> Result<(), PipelineError> {
        let actor = if kind.starts_with("execute_tm_") || kind == "tm_memo_acknowledgement" {
            "truth_maintenance"
        } else {
            "maintenance_worker"
        };
        let effects = self.prepared_plan()?.effects;
        for effect in effects {
            let Some(row) = effect.after.as_ref().or(effect.before.as_ref()) else {
                return Err(invalid("empty maintenance effect"));
            };
            if matches!(
                row,
                CanonicalRecord::AuditLog(_) | CanonicalRecord::WorkArtifact(_)
            ) {
                continue;
            }
            let op = if effect
                .after
                .as_ref()
                .is_none_or(|after| after.base().deleted_at.is_some())
            {
                EventOperation::Delete
            } else if effect.before.is_none() {
                EventOperation::Create
            } else {
                EventOperation::Update
            };
            let table = row.base().id.table().as_str();
            let mut payload = OutboxPayload::new(
                self.space_id.clone(),
                format!("{table}.maintenance"),
                actor.to_owned(),
            );
            payload.add_record(RecordRef {
                id: row.base().id.clone(),
                table: table.to_owned(),
                op,
            });
            self.emit_system_event(payload).await?;
        }
        Ok(())
    }

    /// Assemble the storage-owned derived-graph request.
    pub(crate) async fn prepare_request<R: Serialize>(
        &self,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
        output: &R,
    ) -> Result<elephant_storage::canonical_commit::CanonicalCommit, PipelineError> {
        let sources = boxed_maintenance_phase(
            "maintenance.prepare.support",
            self.capture_published_support(),
        )
        .await?;
        boxed_maintenance_phase(
            "maintenance.prepare.assemble",
            self.assemble_request(
                kind,
                scope,
                input,
                output,
                elephant_storage::canonical_commit::CanonicalMaintenanceOrigin::Derived,
                sources,
            ),
        )
        .await
    }

    /// Prepare only an existing reprocess job's control transition. The owner
    /// validates the complete allowed field/effect set, original source heads,
    /// audit snapshots, epoch and attempt in the same canonical transaction.
    /// Pending source intake never becomes graph publication authority.
    pub(crate) async fn prepare_reprocess_control_request<R: Serialize>(
        &self,
        original_job: &ReprocessJob,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
        output: &R,
    ) -> Result<elephant_storage::canonical_commit::CanonicalCommit, PipelineError> {
        self.observe(original_job)?;
        let canonical = CanonicalRepository::new(self.client.clone());
        let sources =
            Box::pin(canonical.capture_reprocess_control_support(&self.space_id, original_job))
                .await?;
        self.assemble_request(
            kind,
            scope,
            input,
            output,
            elephant_storage::canonical_commit::CanonicalMaintenanceOrigin::ControlOnly,
            sources,
        )
        .await
    }

    async fn assemble_request<R: Serialize>(
        &self,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
        output: &R,
        origin: elephant_storage::canonical_commit::CanonicalMaintenanceOrigin,
        sources: Vec<elephant_storage::canonical_commit::PublishedSourceFence>,
    ) -> Result<elephant_storage::canonical_commit::CanonicalCommit, PipelineError> {
        use elephant_storage::canonical_commit::{CanonicalCommit, CanonicalMaintenanceContext};
        self.emit_effect_events(kind).await?;
        let mut plan = self.prepared_plan()?;
        // A later coalesced target effect can acquire stricter authority after
        // an earlier audit was staged. Refresh only its envelope from the final
        // trusted target union; preserve all original audit payloads.
        for index in 0..plan.effects.len() {
            let Some(CanonicalRecord::AuditLog(audit)) = &plan.effects[index].after else {
                continue;
            };
            let mut audit = audit.clone();
            elephant_storage::canonical_commit::inherit_maintenance_audit_authority(
                &plan, &mut audit,
            )?;
            plan.effects[index].after = Some(CanonicalRecord::AuditLog(audit));
        }
        Ok(CanonicalCommit {
            space_id: self.space_id.clone(),
            commit_id: self.commit_identity(kind, scope)?,
            input_fingerprint: request_fingerprint(input)?,
            replay_output: Some(
                serde_json::json!({"value": serde_json::to_value(output).map_err(|e| invalid(e.to_string()))?}),
            ),
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin,
                attempt: self.attempt.clone(),
                sources,
            }),
            plan,
        })
    }

    pub(crate) async fn commit_reprocess_control<R: Serialize>(
        &self,
        original_job: &ReprocessJob,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
        output: &R,
    ) -> Result<(), PipelineError> {
        let request = Box::pin(self.prepare_reprocess_control_request(
            original_job,
            kind,
            scope,
            input,
            output,
        ))
        .await?;
        let canonical = CanonicalRepository::new(self.client.clone());
        Box::pin(canonical.commit(request)).await?;
        Ok(())
    }

    /// Add audit rows for prepared domain effects that their owner has not
    /// already audited. The rows inherit the affected record's full envelope.
    pub(crate) async fn audit_pending_effects(&self, reason: &str) -> Result<(), PipelineError> {
        let plan = self.prepared_plan()?;
        let audited: std::collections::BTreeSet<_> = plan
            .effects
            .iter()
            .filter_map(|effect| match &effect.after {
                Some(CanonicalRecord::AuditLog(row)) => Some(row.record.clone()),
                _ => None,
            })
            .collect();
        for effect in plan.effects {
            let record = effect
                .after
                .as_ref()
                .or(effect.before.as_ref())
                .ok_or_else(|| invalid("empty maintenance audit effect"))?;
            if matches!(record, CanonicalRecord::AuditLog(_)) || audited.contains(&record.base().id)
            {
                continue;
            }
            let domain = |row: &CanonicalRecord| -> Result<serde_json::Value, PipelineError> {
                serde_json::to_value(row)
                    .map_err(|error| invalid(error.to_string()))?
                    .get("record")
                    .cloned()
                    .ok_or_else(|| invalid("canonical audit domain payload is missing"))
            };
            let action = if effect.before.is_none() {
                AuditAction::Create
            } else if effect
                .after
                .as_ref()
                .is_none_or(|row| row.base().deleted_at.is_some())
            {
                AuditAction::Delete
            } else {
                AuditAction::Update
            };
            let mut audit_base = BaseFields::new(
                RecordId::new_v4(TableName::AuditLog),
                self.space_id.clone(),
                "maintenance_worker".into(),
            );
            audit_base.provenance = record.base().provenance.clone();
            self.create_audit_log(AuditLog {
                base: audit_base,
                action,
                actor: "maintenance_worker".into(),
                record: record.base().id.clone(),
                before: effect.before.as_ref().map(domain).transpose()?,
                after: effect.after.as_ref().map(domain).transpose()?,
                timestamp: Utc::now(),
                reason: Some(reason.into()),
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn commit<R: Serialize>(
        &self,
        kind: &str,
        scope: &str,
        input: &serde_json::Value,
        output: &R,
    ) -> Result<(), PipelineError> {
        // Keep the storage transaction future out of every enclosing TM and
        // dispatcher state machine. Its full validation/write branches remain
        // unchanged and are polled through one owned heap allocation.
        let request = boxed_maintenance_phase(
            "maintenance.commit.prepare",
            self.prepare_request(kind, scope, input, output),
        )
        .await?;
        let canonical = CanonicalRepository::new(self.client.clone());
        boxed_maintenance_phase("maintenance.commit.canonical", canonical.commit(request)).await?;
        tracing::debug!(phase = "maintenance.commit.done");
        Ok(())
    }
}

/// Evidence may add subject restrictions to the source's requirements, never
/// remove one or change opaque security policy/completeness.
pub(crate) fn contains_authorization(
    derived: &BaseFields,
    contributor: &BaseFields,
) -> Result<bool, PipelineError> {
    if derived.space_id != contributor.space_id
        || derived.security.policy_id != contributor.security.policy_id
    {
        return Ok(false);
    }
    // Reuse the storage owner's ordering for levels, required sets and
    // completeness. Opaque policy identifiers have no implicit ordering.
    let mut joined = derived.clone();
    elephant_storage::public_mutation::inherit_public_envelope(&mut joined, contributor)?;
    same_authorization(derived, &joined)
}

fn contains_subject_authorization(
    derived: &BaseFields,
    contributor: &BaseFields,
) -> Result<bool, PipelineError> {
    let mut aligned = contributor.clone();
    aligned.subjects.clone_from(&derived.subjects);
    Ok(same_authorization(derived, &aligned)?
        && contributor
            .subjects
            .iter()
            .all(|subject| derived.subjects.contains(subject)))
}

fn merge_evidence_authority(
    target: &mut BaseFields,
    evidence: &BaseFields,
) -> Result<(), PipelineError> {
    if !contains_authorization(target, evidence)? {
        return Err(invalid("maintenance evidence removes subject requirements"));
    }
    target
        .provenance
        .sources
        .extend(evidence.provenance.sources.iter().cloned());
    target.provenance.sources.sort();
    target.provenance.sources.dedup();
    Ok(())
}

pub(crate) fn same_authorization(
    left: &BaseFields,
    right: &BaseFields,
) -> Result<bool, PipelineError> {
    Ok(left.space_id == right.space_id
        && left.security.policy_id == right.security.policy_id
        && left.security.level == right.security.level
        && canonical_strings(&left.security.labels) == canonical_strings(&right.security.labels)
        && canonical_strings(&left.security.handling)
            == canonical_strings(&right.security.handling)
        && canonical_strings(
            &left
                .subjects
                .iter()
                .map(|link| link.id())
                .collect::<Vec<_>>(),
        ) == canonical_strings(
            &right
                .subjects
                .iter()
                .map(|link| link.id())
                .collect::<Vec<_>>(),
        )
        && same(&left.subjects_state, &right.subjects_state)?)
}

pub(crate) fn same_partition(left: &BaseFields, right: &BaseFields) -> Result<bool, PipelineError> {
    Ok(same_authorization(left, right)?
        && canonical_strings(&left.provenance.sources)
            == canonical_strings(&right.provenance.sources))
}

impl PlanningTruthMaintenanceStorage {
    async fn capture_published_support(
        &self,
    ) -> Result<Vec<elephant_storage::canonical_commit::PublishedSourceFence>, PipelineError> {
        boxed_maintenance_phase(
            "maintenance.support.evidence",
            self.validate_decision_evidence(),
        )
        .await?;
        let plan = self.prepared_plan()?;
        let mut records = plan.dependencies;
        for effect in plan.effects {
            records.extend(effect.before);
        }
        let canonical = CanonicalRepository::new(self.client.clone());
        Ok(boxed_maintenance_phase(
            "maintenance.support.capture",
            canonical.capture_maintenance_support_with_extensions(
                &self.space_id,
                &records,
                &plan.extension_effects,
            ),
        )
        .await?)
    }
}

impl PlanningTruthMaintenanceStorage {
    async fn validate_event_evidence(&self, row: &Event) -> Result<(), PipelineError> {
        self.validate_event_evidence_at(row, true).await
    }

    async fn validate_event_evidence_at(
        &self,
        row: &Event,
        prepared: bool,
    ) -> Result<(), PipelineError> {
        if !row.base.provenance.sources.is_empty() && row.assertion_refs.is_empty() {
            return Err(invalid(
                "source-backed event has no immutable assertion evidence",
            ));
        }
        for reference in &row.assertion_refs {
            let mut assertion = self
                .evidence_assertion(reference.id(), prepared)
                .await?
                .ok_or_else(|| invalid("event evidence assertion is missing"))?;
            self.evidence_classification(&mut assertion.base)?;
            if !contains_authorization(&row.base, &assertion.base)?
                || assertion
                    .base
                    .provenance
                    .sources
                    .iter()
                    .any(|source| !row.base.provenance.sources.contains(source))
            {
                return Err(invalid(
                    "event evidence crosses its captured support/security authority",
                ));
            }
            boxed_maintenance_phase(
                "maintenance.event.assertion",
                self.validate_assertion_evidence(&assertion),
            )
            .await?;
        }
        if canonical_strings(
            &row.source_refs
                .iter()
                .map(|link| link.id())
                .collect::<Vec<_>>(),
        ) != canonical_strings(&row.base.provenance.sources)
        {
            return Err(invalid(
                "event source references disagree with support authority",
            ));
        }
        Ok(())
    }

    async fn validate_decision_evidence(&self) -> Result<(), PipelineError> {
        let plan = self.prepared_plan()?;
        let mut records = plan
            .dependencies
            .into_iter()
            .map(|record| (record, false))
            .collect::<Vec<_>>();
        for effect in plan.effects {
            let prepared = match &effect.after {
                Some(after) => !retirement_uses_original_evidence(effect.before.as_ref(), after)?,
                None => true,
            };
            records.extend(effect.before.into_iter().map(|record| (record, false)));
            records.extend(effect.after.into_iter().map(|record| (record, prepared)));
        }
        for (record, prepared) in records {
            boxed_maintenance_phase(
                "maintenance.evidence.classification",
                self.validate_classification_documents(&record),
            )
            .await?;
            match record {
                CanonicalRecord::Assertion(mut row) => {
                    self.evidence_classification(&mut row.base)?;
                    boxed_maintenance_phase(
                        "maintenance.evidence.assertion",
                        self.validate_assertion_evidence(&row),
                    )
                    .await?
                }
                CanonicalRecord::Rel(mut row) => {
                    self.evidence_classification(&mut row.base)?;
                    boxed_maintenance_phase(
                        "maintenance.evidence.rel",
                        self.validate_rel_evidence_at(&row, prepared),
                    )
                    .await?
                }
                CanonicalRecord::Attribute(mut row) => {
                    self.evidence_classification(&mut row.base)?;
                    boxed_maintenance_phase(
                        "maintenance.evidence.attribute",
                        self.validate_claim_evidence_at(&row.base, &row.evidence, prepared),
                    )
                    .await?
                }
                CanonicalRecord::Event(mut row) => {
                    self.evidence_classification(&mut row.base)?;
                    boxed_maintenance_phase(
                        "maintenance.evidence.event",
                        self.validate_event_evidence_at(&row, prepared),
                    )
                    .await?
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn validate_classification_documents(
        &self,
        record: &CanonicalRecord,
    ) -> Result<(), PipelineError> {
        let repair = self
            .classification_repairs
            .lock()
            .map_err(|_| invalid("classification plan lock poisoned"))?
            .get(&record.base().id)
            .cloned();
        let Some(repair) = repair else {
            return Ok(());
        };
        let mut revisions = Vec::new();
        match record {
            CanonicalRecord::Assertion(row) => revisions.push(row.evidence.revision.id().clone()),
            CanonicalRecord::Rel(row) => {
                revisions.extend(row.evidence.iter().map(|e| e.span.revision.id().clone()))
            }
            CanonicalRecord::Attribute(row) => {
                revisions.extend(row.evidence.iter().map(|e| e.span.revision.id().clone()))
            }
            CanonicalRecord::Event(row) => {
                for reference in &row.assertion_refs {
                    let assertion = self
                        .get_assertion(&self.space_id, reference.id())
                        .await?
                        .ok_or_else(|| invalid("classification event assertion missing"))?;
                    revisions.push(assertion.evidence.revision.id().clone());
                }
            }
            _ => {
                return Err(invalid(
                    "classification proof belongs to an unsupported record",
                ))
            }
        }
        let mut documents = Vec::new();
        for id in revisions {
            let revision = elephant_storage::DocRevisionRepository::new(self.client.clone())
                .get(&self.space_id, &id)
                .await?
                .ok_or_else(|| invalid("classification evidence revision missing"))?;
            self.observe(&revision)?;
            documents.push(revision.doc.id().clone());
        }
        if canonical_strings(&documents) != canonical_strings(&repair.documents) {
            return Err(invalid(
                "classification documents do not match all actual evidence",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "maintenance_commit_tests.rs"]
mod tests;
