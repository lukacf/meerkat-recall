//! Causal inverse metadata for decisions over more than one published source.
//!
//! Raw source support is disjunctive: an independent contribution can survive.
//! A derived decision depends on every input. Withdrawing any input invalidates
//! that decision and its dependent decisions, restoring surviving baseline fields.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use surrealdb::sql::Value;

use crate::canonical_commit::{
    causal_source_ids, CanonicalCommit, CanonicalEffect, CanonicalRecord, CanonicalRepository,
};
use crate::source_publication::{
    control_thing, control_value, integrity, read_control_tx, stable_key, write_control_tx,
    SourcePublicationRepository,
};
use crate::{EmbeddedMutation, StorageClient, StorageResult};

/// Payload-bearing causal metadata; physical erasure removes the complete row.
pub const DECISION_TABLE: &str = "source_decision_receipt";

impl SourcePublicationRepository {
    /// Whether a row has retained causal source authority, even after an edit
    /// cleared its visible provenance.
    ///
    /// This is a conservative policy guard,
    /// not proof that the row is currently published or eligible for retrieval.
    /// Callers capture the canonical epoch before this read and guard the exact
    /// row in their eventual mutation transaction.
    pub async fn has_causal_source_support(
        &self,
        space: &str,
        record_id: &RecordId,
    ) -> StorageResult<bool> {
        if !self.support_for_record(space, record_id).await?.is_empty() {
            return Ok(true);
        }
        let decisions: Vec<SourceDecisionReceipt> =
            self.list_controls(DECISION_TABLE, Some(space)).await?;
        Ok(decisions.iter().any(|decision| {
            decision.space_id == space
                && !decision.source_ids.is_empty()
                && decision.effects.iter().any(|effect| {
                    effect
                        .before
                        .iter()
                        .chain(effect.after.iter())
                        .any(|row| row.base().space_id == space && row.base().id == *record_id)
                })
        }))
    }
}

/// Exact inverse information recorded with the decision's canonical effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDecisionReceipt {
    /// Named space of every effect and input.
    pub space_id: String,
    /// Stable canonical receipt identity.
    pub commit_id: String,
    /// Monotonic per-space commit order, allocated inside the same transaction.
    pub sequence: u64,
    /// Conjunctive input support: all are required for this decision to remain valid.
    pub source_ids: Vec<RecordId>,
    /// Material read identities, including range results through the captured epoch.
    pub dependency_ids: Vec<RecordId>,
    /// Exact observed baseline and resulting rows.
    pub effects: Vec<CanonicalEffect>,
    /// Exact native extension inverse snapshots under the same decision.
    #[serde(default)]
    pub extension_effects: Vec<crate::canonical_extension::CanonicalExtensionEffect>,
    /// Derived outbox payloads to erase when their decision is invalidated.
    pub outbox_ids: Vec<RecordId>,
    /// Time this inverse metadata committed with its effects.
    pub committed_at: DateTime<Utc>,
}

impl SourceDecisionReceipt {
    fn from_request(
        request: &CanonicalCommit,
        sequence: u64,
        contact: Option<&crate::source_contact_publication::AcceptedContactState>,
    ) -> StorageResult<Option<Self>> {
        if let Some(contact) = contact.filter(|_| request.publication.is_some()) {
            let effects = contact.decision_effects(&request.plan)?;
            if effects.is_empty() {
                return Ok(None);
            }
            // Publication contact deltas need their independently surviving
            // baseline, not the old generation's tainted actual before-image.
            let source_ids = causal_source_ids(request)
                .into_iter()
                .chain(
                    effects
                        .iter()
                        .flat_map(|effect| effect.before.iter().chain(effect.after.iter()))
                        .flat_map(|row| row.base().provenance.sources.iter().cloned()),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let dependency_ids =
                request
                    .plan
                    .dependencies
                    .iter()
                    .map(|row| row.base().id.clone())
                    .chain(request.plan.effects.iter().filter_map(|effect| {
                        effect.before.as_ref().map(|row| row.base().id.clone())
                    }))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
            return Ok(Some(Self {
                space_id: request.space_id.clone(),
                commit_id: request.commit_id.clone(),
                sequence,
                source_ids,
                dependency_ids,
                effects,
                extension_effects: Vec::new(),
                outbox_ids: contact.decision_outbox_ids(),
                committed_at: Utc::now(),
            }));
        }
        let source_ids = causal_source_ids(request);
        if request.maintenance.as_ref().is_none_or(|context| {
            context.origin != crate::canonical_commit::CanonicalMaintenanceOrigin::Derived
        }) || (source_ids.is_empty()
            && !request.plan.effects.iter().any(|effect| {
                matches!(
                    &effect.after,
                    Some(CanonicalRecord::KnowledgeViewDefinition(_))
                )
            }))
        {
            return Ok(None);
        }
        let dependency_ids =
            request
                .plan
                .dependencies
                .iter()
                .map(|row| row.base().id.clone())
                .chain(
                    request.plan.effects.iter().filter_map(|effect| {
                        effect.before.as_ref().map(|row| row.base().id.clone())
                    }),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        Ok(Some(Self {
            space_id: request.space_id.clone(),
            commit_id: request.commit_id.clone(),
            sequence,
            source_ids,
            dependency_ids,
            effects: request.plan.effects.clone(),
            extension_effects: request.plan.extension_effects.clone(),
            outbox_ids: request
                .plan
                .outbox
                .iter()
                .map(|event| event.base.id.clone())
                .collect(),
            committed_at: Utc::now(),
        }))
    }
}

pub(crate) fn write_decision_tx(
    tx: &mut EmbeddedMutation<'_>,
    request: &CanonicalCommit,
    contact: Option<&crate::source_contact_publication::AcceptedContactState>,
) -> StorageResult<()> {
    let Some(mut decision) = SourceDecisionReceipt::from_request(request, 0, contact)? else {
        return Ok(());
    };
    let sequence: Option<u64> = read_control_tx(
        tx,
        "source_decision_sequence",
        &request.space_id,
        &request.space_id,
    )?;
    decision.sequence = sequence
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| integrity("source decision sequence exhausted"))?;
    // The sentinel is only a control-row index; the typed source_ids stays
    // empty and cannot invent source support for an intrinsic decision.
    let index_source = RecordId::new(elephant_types::TableName::Source, uuid::Uuid::nil());
    let source = decision.source_ids.first().unwrap_or(&index_source);
    write_control_tx(
        tx,
        "source_decision_sequence",
        &request.space_id,
        &request.space_id,
        source,
        &decision.sequence,
    )?;
    write_control_tx(
        tx,
        DECISION_TABLE,
        &stable_key(&[&request.space_id, &request.commit_id]),
        &request.space_id,
        source,
        &decision,
    )
}

pub(crate) fn append_decision_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    request: &CanonicalCommit,
    contact: Option<&crate::source_contact_publication::AcceptedContactState>,
) -> StorageResult<()> {
    let Some(decision) = SourceDecisionReceipt::from_request(request, 0, contact)? else {
        return Ok(());
    };
    // The sentinel is only a control-row index; the typed source_ids stays
    // empty and cannot invent source support for an intrinsic decision.
    let index_source = RecordId::new(elephant_types::TableName::Source, uuid::Uuid::nil());
    let source = decision.source_ids.first().unwrap_or(&index_source);
    bindings.insert(
        "decision_sequence_id".into(),
        control_thing("source_decision_sequence", &request.space_id).into(),
    );
    bindings.insert(
        "decision_id".into(),
        control_thing(
            DECISION_TABLE,
            &stable_key(&[&request.space_id, &request.commit_id]),
        )
        .into(),
    );
    bindings.insert(
        "decision_record".into(),
        control_value(&request.space_id, source, &decision)?,
    );
    // Native state stores the allocated order. The payload stays immutable and
    // the explicit database proxy below merges only this one native scalar.
    sql.push_str("LET $decision_counter = SELECT VALUE sequence FROM ONLY $decision_sequence_id;\nLET $decision_sequence = IF $decision_counter = NONE THEN 1 ELSE $decision_counter + 1 END;\nIF $decision_sequence <= 0 { THROW 'publication:decision_sequence_exhausted'; };\nUPSERT $decision_sequence_id SET space_id = $space, sequence = $decision_sequence;\nCREATE $decision_id CONTENT $decision_record;\nUPDATE $decision_id SET sequence = $decision_sequence;\n");
    Ok(())
}

/// The original ordered causal closure, shared by live invalidation and exact
/// historical proof projection. The caller validates receipt scope and origin.
#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
pub(crate) fn invalidated_decisions(
    mut decisions: Vec<SourceDecisionReceipt>,
    source_id: &RecordId,
) -> Vec<SourceDecisionReceipt> {
    decisions.sort_by_key(|decision| decision.sequence);
    let mut affected = BTreeSet::new();
    let mut affected_extensions = BTreeSet::new();
    decisions
        .into_iter()
        .filter(|decision| {
            if decision.source_ids.contains(source_id)
                || decision.extension_effects.iter().any(|effect| {
                    effect.before.is_some() && affected_extensions.contains(&effect.id)
                })
                || decision
                    .dependency_ids
                    .iter()
                    .any(|id| affected.contains(id))
            {
                affected_extensions.extend(
                    decision
                        .extension_effects
                        .iter()
                        .map(|effect| effect.id.clone()),
                );
                affected.extend(decision.effects.iter().filter_map(|effect| {
                    effect
                        .after
                        .as_ref()
                        .or(effect.before.as_ref())
                        .map(|row| row.base().id.clone())
                }));
                true
            } else {
                false
            }
        })
        .collect::<Vec<_>>()
}

/// A deterministic rollback plan consumed by the source lifecycle transaction.
#[derive(Debug, Clone)]
pub struct SourceDecisionInvalidation {
    /// Original independently admitted contributions and same-transaction guards.
    pub(crate) contact_survival:
        Option<crate::source_contact_survival_proof::PreparedContactSurvival>,
    /// Preparation epoch that must still match when applying this plan.
    pub observed_epoch: String,
    /// Complete current-before/surviving-after rows, combined across inverses.
    pub effects: Vec<CanonicalEffect>,
    /// Native extension inverses, committed with the typed effects.
    pub extension_effects: Vec<crate::canonical_extension::CanonicalExtensionEffect>,
    /// Opaque decision control keys to physically remove with the inverses.
    pub decision_keys: Vec<String>,
    /// Canonical receipt keys containing now-invalid successor payloads.
    pub receipt_keys: Vec<String>,
    /// Outbox events whose decision payloads no longer have valid support.
    pub outbox_ids: Vec<RecordId>,
}

/// Read-only preparation over the canonical authority; this does not apply writes.
#[derive(Debug, Clone)]
pub struct SourceDecisionRepository {
    client: StorageClient,
}

impl SourceDecisionRepository {
    /// Binds a decision reader to the same database as canonical effects.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    async fn ordered_decision_receipts(
        &self,
        space: &str,
    ) -> StorageResult<Vec<SourceDecisionReceipt>> {
        let decisions: Vec<SourceDecisionReceipt> =
            SourcePublicationRepository::new(self.client.clone())
                .list_controls(DECISION_TABLE, Some(space))
                .await?;
        if self.client.embedded_store().is_none() {
            #[derive(Deserialize)]
            struct SequenceDb {
                payload: String,
                sequence: u64,
            }
            let mut response = self
                .client
                .query_bind_checked(
                    "SELECT payload,sequence FROM source_decision_receipt WHERE space_id=$space",
                    crate::bindings! {"space"=>space},
                )
                .await?;
            let rows: Vec<SequenceDb> = response
                .take(0)
                .map_err(|error| integrity(&error.to_string()))?;
            rows.into_iter()
                .map(|row| {
                    let mut decision: SourceDecisionReceipt =
                        crate::source_publication::decode_payload(&row.payload)?;
                    decision.sequence = row.sequence;
                    Ok(decision)
                })
                .collect::<StorageResult<_>>()
        } else {
            Ok(decisions)
        }
    }

    /// Prepares reverse-ordered invalidation after an input source is withdrawn.
    /// The caller must compare `observed_epoch` inside its atomic lifecycle write.
    #[allow(
        clippy::too_many_lines,
        reason = "Canonical and extension inverse overlays must advance in the same reverse receipt order with contact-survival and independent-support checks; preserve that ordered mutation phase."
    )]
    #[allow(
        clippy::mutable_key_type,
        reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
    )]
    pub async fn invalidation(
        &self,
        space: &str,
        source_id: &RecordId,
    ) -> StorageResult<SourceDecisionInvalidation> {
        let observed_epoch = CanonicalRepository::new(self.client.clone())
            .epoch(space)
            .await?;
        let decisions = self.ordered_decision_receipts(space).await?;
        let invalidated = invalidated_decisions(decisions, source_id);
        let contact_survival = crate::source_contact_survival_proof::prepare_contact_survival(
            &self.client,
            space,
            source_id,
            &invalidated,
        )
        .await?;
        let supports: Vec<crate::canonical_commit::SourceEffectSupport> =
            SourcePublicationRepository::new(self.client.clone())
                .list_controls("source_effect_support", Some(space))
                .await?;
        let mut current = BTreeMap::<RecordId, Option<CanonicalRecord>>::new();
        let mut original = BTreeMap::<RecordId, Option<CanonicalRecord>>::new();
        let mut extension_current = BTreeMap::new();
        let mut extension_original = BTreeMap::new();
        let mut decision_keys = Vec::new();
        let mut receipt_keys = Vec::new();
        let mut outbox_ids = BTreeSet::new();
        for decision in invalidated.into_iter().rev() {
            let key = stable_key(&[space, &decision.commit_id]);
            decision_keys.push(key.clone());
            receipt_keys.push(key);
            outbox_ids.extend(decision.outbox_ids.iter().cloned());
            for effect in decision.extension_effects.iter().rev() {
                if !extension_current.contains_key(&effect.id) {
                    let loaded = crate::canonical_extension::load_extension_record(
                        &self.client,
                        space,
                        &effect.id,
                    )
                    .await?;
                    extension_original.insert(effect.id.clone(), loaded.clone());
                    extension_current.insert(effect.id.clone(), loaded);
                }
                let current = extension_current
                    .remove(&effect.id)
                    .ok_or_else(|| integrity("missing extension inverse overlay"))?;
                let restored =
                    crate::canonical_extension::inverse_extension_effect(current, effect)?;
                extension_current.insert(effect.id.clone(), restored);
            }
            for effect in decision.effects.iter().rev() {
                let row = effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .ok_or_else(|| integrity("empty decision inverse"))?;
                let id = row.base().id.clone();
                if !current.contains_key(&id) {
                    let loaded = CanonicalRecord::load(&self.client, space, &id).await?;
                    original.insert(id.clone(), loaded.clone());
                    current.insert(id.clone(), loaded);
                }
                let existing = current
                    .remove(&id)
                    .ok_or_else(|| integrity("missing inverse overlay"))?;
                if let Some(survival) = contact_survival
                    .as_ref()
                    .filter(|survival| survival.applies_to(&decision, effect))
                {
                    let restored = survival.inverse_effect(
                        &decision,
                        existing,
                        original.get(&id).and_then(Option::as_ref),
                        effect,
                    )?;
                    current.insert(id, restored);
                    continue;
                }
                let preserve_independent = effect.before.is_none()
                    && supports
                        .iter()
                        .any(|support| support.record_id == id && support.independent);
                if preserve_independent {
                    let observed = original
                        .get(&id)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| integrity("independent decision baseline missing"))?;
                    let fingerprint = observed.fingerprint()?;
                    if supports
                        .iter()
                        .filter(|support| support.record_id == id)
                        .any(|support| fingerprint != support.record_fingerprint)
                    {
                        return Err(crate::source_publication::conflict(
                            "decision_independent_support_changed",
                        ));
                    }
                    current.insert(id, existing);
                } else {
                    current.insert(id, inverse_effect(existing, effect)?);
                }
            }
        }
        let effects = changed_canonical_rows(current, original)?;
        let mut extension_effects = Vec::new();
        for (id, after) in extension_current {
            let before = extension_original
                .remove(&id)
                .ok_or_else(|| integrity("missing extension inverse baseline"))?;
            if before != after {
                extension_effects.push(crate::canonical_extension::CanonicalExtensionEffect {
                    id,
                    before,
                    after,
                });
            }
        }
        Ok(SourceDecisionInvalidation {
            contact_survival,
            observed_epoch,
            effects,
            extension_effects,
            decision_keys,
            receipt_keys,
            outbox_ids: outbox_ids.into_iter().collect(),
        })
    }
}

fn changed_canonical_rows(
    current: BTreeMap<RecordId, Option<CanonicalRecord>>,
    mut original: BTreeMap<RecordId, Option<CanonicalRecord>>,
) -> StorageResult<Vec<CanonicalEffect>> {
    let mut effects = Vec::new();
    for (id, after) in current {
        let before = original
            .remove(&id)
            .ok_or_else(|| integrity("missing inverse baseline"))?;
        if serde_json::to_value(&before).map_err(|e| integrity(&e.to_string()))?
            != serde_json::to_value(&after).map_err(|e| integrity(&e.to_string()))?
        {
            effects.push(CanonicalEffect { before, after });
        }
    }
    Ok(effects)
}

/// Reverses only fields still equal to the invalidated decision's output.
/// Independent later edits to other fields survive. Call newest decision first.
pub(crate) fn inverse_effect(
    current: Option<CanonicalRecord>,
    effect: &CanonicalEffect,
) -> StorageResult<Option<CanonicalRecord>> {
    match (&effect.before, &effect.after, current) {
        (None, Some(after), Some(current)) => {
            if current.fingerprint()? != after.fingerprint()? {
                return Err(crate::source_publication::conflict(
                    "decision_inverse_requires_independent_baseline",
                ));
            }
            Ok(None)
        }
        (_, Some(_), None) => Ok(None),
        (Some(before), None, None) => Ok(Some(before.clone())),
        (Some(_), None, current) => Ok(current),
        (Some(before), Some(after), Some(current)) => {
            if before.base().id != after.base().id || current.base().id != after.base().id {
                return Err(integrity("decision inverse identity changed"));
            }
            let before = serde_json::to_value(before).map_err(|e| integrity(&e.to_string()))?;
            let after = serde_json::to_value(after).map_err(|e| integrity(&e.to_string()))?;
            let mut result =
                serde_json::to_value(current).map_err(|e| integrity(&e.to_string()))?;
            let baseline = before
                .get("record")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| integrity("decision baseline shape"))?;
            let written = after
                .get("record")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| integrity("decision result shape"))?;
            let surviving = result
                .get_mut("record")
                .and_then(serde_json::Value::as_object_mut)
                .ok_or_else(|| integrity("decision current shape"))?;
            let keys = baseline
                .keys()
                .chain(written.keys())
                .collect::<BTreeSet<_>>();
            for key in keys {
                if matches!(
                    key.as_str(),
                    "id" | "space_id" | "created_at" | "created_by" | "updated_at"
                ) {
                    continue;
                }
                let restored = inverse_json_field(
                    baseline.get(key),
                    written.get(key),
                    surviving.get(key),
                    key,
                )?;
                if let Some(value) = restored {
                    surviving.insert(key.clone(), value);
                } else {
                    surviving.remove(key);
                }
            }
            let mut row: CanonicalRecord =
                serde_json::from_value(result).map_err(|e| integrity(&e.to_string()))?;
            row.base_mut().updated_at = Utc::now();
            Ok(Some(row))
        }
        (None, None, _) => Err(integrity("empty decision inverse")),
    }
}

/// Restores changed object leaves without discarding independently edited siblings.
///
/// Arrays are treated as sets only for domain fields with explicit set semantics;
/// ambiguous mixed edits to ordered/unknown arrays keep cleanup pending.
#[allow(
    clippy::needless_collect,
    reason = "The union must deduplicate overlapping keys before mutating the inverse object; iterating both key streams directly could reverse the same field twice."
)]
pub(crate) fn inverse_json_field(
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
    current: Option<&serde_json::Value>,
    path: &str,
) -> StorageResult<Option<serde_json::Value>> {
    use serde_json::Value;
    if before == after {
        return Ok(current.cloned());
    }
    if current == after {
        return Ok(before.cloned());
    }
    if let Some(Value::Object(surviving)) = current {
        if before.is_some_and(Value::is_object) || after.is_some_and(Value::is_object) {
            let object_or_absent = |value: Option<&Value>| {
                value.is_none_or(|value| value.is_object() || value.is_null())
            };
            if !object_or_absent(before) || !object_or_absent(after) {
                return Err(crate::source_publication::conflict(
                    "decision_inverse_ambiguous_type",
                ));
            }
            let empty = serde_json::Map::new();
            let baseline = before.and_then(Value::as_object).unwrap_or(&empty);
            let written = after.and_then(Value::as_object).unwrap_or(&empty);
            let mut result = surviving.clone();
            for key in baseline
                .keys()
                .chain(written.keys())
                .collect::<BTreeSet<_>>()
            {
                let restored = inverse_json_field(
                    baseline.get(key),
                    written.get(key),
                    result.get(key),
                    &format!("{path}.{key}"),
                )?;
                if let Some(value) = restored {
                    result.insert(key.clone(), value);
                } else {
                    result.remove(key);
                }
            }
            return Ok(if result.is_empty() && before.is_none_or(Value::is_null) {
                before.cloned()
            } else {
                Some(Value::Object(result))
            });
        }
    }
    if let Some(Value::Array(surviving)) = current {
        if before.is_some_and(Value::is_array) || after.is_some_and(Value::is_array) {
            if !matches!(
                path,
                "aliases"
                    | "security.labels"
                    | "security.handling"
                    | "subjects"
                    | "provenance.sources"
                    | "identity_keys"
                    | "evidence"
                    | "assertion_refs"
                    | "source_refs"
            ) {
                return Err(crate::source_publication::conflict(
                    "decision_inverse_ambiguous_array",
                ));
            }
            let array_or_absent = |value: Option<&Value>| {
                value.is_none_or(|value| value.is_array() || value.is_null())
            };
            if !array_or_absent(before) || !array_or_absent(after) {
                return Err(crate::source_publication::conflict(
                    "decision_inverse_ambiguous_type",
                ));
            }
            let empty = Vec::new();
            let baseline = before.and_then(Value::as_array).unwrap_or(&empty);
            let written = after.and_then(Value::as_array).unwrap_or(&empty);
            let mut result = surviving.clone();
            let mut baseline_unmatched = baseline.clone();
            for added in written {
                if let Some(index) = baseline_unmatched.iter().position(|value| value == added) {
                    baseline_unmatched.remove(index);
                } else if let Some(index) = result.iter().position(|value| value == added) {
                    result.remove(index);
                }
            }
            for removed in baseline_unmatched {
                if !result.contains(&removed) {
                    result.push(removed);
                }
            }
            return Ok(if result.is_empty() && before.is_none_or(Value::is_null) {
                before.cloned()
            } else {
                Some(Value::Array(result))
            });
        }
    }
    // A later different primitive is independently authored, or absent now.
    Ok(current.cloned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use elephant_types::records::{
        base::BaseFields,
        entity::{Entity, EntityType},
    };
    use elephant_types::TableName;

    #[test]
    fn inverse_preserves_later_independent_fields_and_reopens_baseline() {
        let baseline = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "a".into(),
                "independent".into(),
            ),
            EntityType::new("project"),
            "baseline name".into(),
        );
        let mut decided = baseline.clone();
        decided.base.deleted_at = Some(Utc::now());
        let mut independently_edited = decided.clone();
        independently_edited.display_name = "new independently authored name".into();
        let inverse = inverse_effect(
            Some(CanonicalRecord::Entity(Box::new(independently_edited))),
            &CanonicalEffect {
                before: Some(CanonicalRecord::Entity(Box::new(baseline))),
                after: Some(CanonicalRecord::Entity(Box::new(decided))),
            },
        )
        .expect("inverse")
        .expect("baseline survives");
        let CanonicalRecord::Entity(inverse) = inverse else {
            unreachable!()
        };
        assert_eq!(inverse.display_name, "new independently authored name");
        assert_eq!(inverse.base.deleted_at, None);
    }
    #[test]
    fn inverse_refuses_to_delete_changed_new_output_without_independent_baseline() {
        let created = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "a".into(),
                "decision".into(),
            ),
            EntityType::new("project"),
            "derived name".into(),
        );
        let mut changed = created.clone();
        changed.display_name = "later independently edited name".into();
        assert!(inverse_effect(
            Some(CanonicalRecord::Entity(Box::new(changed))),
            &CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::Entity(Box::new(created)))
            }
        )
        .is_err());
    }
    #[test]
    fn inverse_restores_nested_source_leaf_and_preserves_independent_sibling() {
        let mut baseline = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "a".into(),
                "baseline".into(),
            ),
            EntityType::new("project"),
            "name".into(),
        );
        baseline.metadata = serde_json::json!({"nested":{"a":"baseline"}});
        let mut decided = baseline.clone();
        decided.metadata =
            serde_json::json!({"nested":{"a":"B-secret","introduced":{"secret":"B-only"}}});
        let mut current = decided.clone();
        current.metadata["nested"]["independent"] = serde_json::json!("keep");
        current.metadata["nested"]["introduced"]["independent"] = serde_json::json!("keep too");
        let result = inverse_effect(
            Some(CanonicalRecord::Entity(Box::new(current))),
            &CanonicalEffect {
                before: Some(CanonicalRecord::Entity(Box::new(baseline))),
                after: Some(CanonicalRecord::Entity(Box::new(decided))),
            },
        )
        .expect("recursive inverse")
        .expect("baseline");
        let CanonicalRecord::Entity(row) = result else {
            unreachable!()
        };
        assert_eq!(
            row.metadata,
            serde_json::json!({"nested":{"a":"baseline","independent":"keep","introduced":{"independent":"keep too"}}})
        );
    }

    #[test]
    fn inverse_preserves_independent_set_members_but_rejects_mixed_ordered_arrays() {
        let before = serde_json::json!(["A"]);
        let after = serde_json::json!(["A", "B"]);
        let current = serde_json::json!(["A", "B", "C"]);
        assert_eq!(
            inverse_json_field(Some(&before), Some(&after), Some(&current), "aliases")
                .expect("set inverse"),
            Some(serde_json::json!(["A", "C"]))
        );
        assert!(inverse_json_field(
            Some(&before),
            Some(&after),
            Some(&current),
            "metadata.steps"
        )
        .is_err());
    }
    #[test]
    fn inverse_removes_introduced_nested_map_from_null_baseline() {
        let before = serde_json::Value::Null;
        let after = serde_json::json!({"private":"source B"});
        let current = serde_json::json!({"private":"source B","independent":"source C"});
        assert_eq!(
            inverse_json_field(Some(&before), Some(&after), Some(&current), "metadata")
                .expect("mixed map"),
            Some(serde_json::json!({"independent":"source C"}))
        );
    }
}
