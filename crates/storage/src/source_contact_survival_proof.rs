//! Storage-owned original admission proof for inverse contact composition.
//! This produces no writes, new admission capabilities, or selector decisions.

use std::collections::BTreeMap;

use elephant_types::publication::{OperationState, SourceHead, SourceRevision, SourceVisibility};
use elephant_types::RecordId;
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::canonical_commit::{
    CanonicalCommitReceipt, CanonicalEffect, CanonicalRecord, SourceEffectSupport,
};
use crate::source_contact_lineage::OriginalControls;
use crate::source_decision::SourceDecisionReceipt;
use crate::source_publication::{
    conflict, encode_payload, head_key, integrity, revision_key, stable_key, StoredOperation,
};
use crate::{EmbeddedMutation, StorageClient, StorageResult};

#[derive(Debug, Clone)]
pub struct PreparedContactSurvival {
    space: String,
    controls: OriginalControls,
    // Exact earliest invalidated decision/effect, not a latest-history guess.
    earliest: BTreeMap<RecordId, (String, u64, String)>,
    contributions: BTreeMap<RecordId, Vec<CanonicalRecord>>,
}

impl PreparedContactSurvival {
    pub(crate) fn check_tx(&self, tx: &EmbeddedMutation<'_>) -> StorageResult<()> {
        self.controls.check_tx(tx, &self.space)
    }
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Preserve the existing fallible guard-composition interface used by atomic SQL transaction callers."
    )]
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        self.controls
            .append_guard_sql(sql, bindings, &self.space, "survival_guard");
        Ok(())
    }
    pub(crate) fn applies_to(
        &self,
        decision: &SourceDecisionReceipt,
        effect: &CanonicalEffect,
    ) -> bool {
        let Some(row) = effect.before.as_ref().or(effect.after.as_ref()) else {
            return false;
        };
        let id = &row.base().id;
        decision.space_id == self.space
            && self.contributions.contains_key(id)
            && self
                .earliest
                .get(id)
                .is_some_and(|(commit, sequence, encoded)| {
                    commit == &decision.commit_id
                        && *sequence == decision.sequence
                        && encode_payload(effect).is_ok_and(|value| value == *encoded)
                })
    }

    pub(crate) fn inverse_effect(
        &self,
        decision: &SourceDecisionReceipt,
        current: Option<CanonicalRecord>,
        original_current: Option<&CanonicalRecord>,
        effect: &CanonicalEffect,
    ) -> StorageResult<Option<CanonicalRecord>> {
        let id = &effect
            .before
            .as_ref()
            .or(effect.after.as_ref())
            .ok_or_else(|| integrity("empty contact survival inverse"))?
            .base()
            .id;
        let exact = self.applies_to(decision, effect);
        let Some(rows) = self.contributions.get(id).filter(|_| exact) else {
            return crate::source_decision::inverse_effect(current, effect);
        };
        let Some(original) = original_current else {
            return Ok(None);
        };
        if original.base().deleted_at.is_some() {
            return Ok(Some(original.clone()));
        }
        if current
            .as_ref()
            .is_none_or(|row| row.base().deleted_at.is_some())
        {
            return Ok(current);
        }
        crate::source_contact_survival::preserve_contact_contributions(
            current, original, decision, effect, rows,
        )
    }
}

#[derive(Deserialize)]
struct RevisionPayload {
    revision: SourceRevision,
}

/// Candidate authority comes from each actual invalidated canonical publication,
/// not from source metadata, an arbitrary matching revision, or a current selector.
#[allow(
    clippy::too_many_lines,
    reason = "Surviving contact evidence and inverse projections are selected together from the original invalidated receipts; keep all fail-closed lineage checks in this proof phase."
)]
pub async fn prepare_contact_survival(
    client: &StorageClient,
    space: &str,
    withdrawn: &RecordId,
    invalidated: &[SourceDecisionReceipt],
) -> StorageResult<Option<PreparedContactSurvival>> {
    let mut proof = PreparedContactSurvival {
        space: space.to_owned(),
        controls: OriginalControls::default(),
        earliest: BTreeMap::new(),
        contributions: BTreeMap::new(),
    };
    for decision in invalidated {
        if decision.space_id != space {
            return Err(conflict("contact_survival_decision_scope_changed"));
        }
        let decision_key = stable_key(&[space, &decision.commit_id]);
        let stored: Option<SourceDecisionReceipt> = proof
            .controls
            .read(
                client,
                space,
                crate::source_decision::DECISION_TABLE,
                &decision_key,
            )
            .await?;
        let mut stored = stored.ok_or_else(|| conflict("contact_survival_decision_changed"))?;
        stored.sequence = decision.sequence;
        if crate::canonical_commit::canonical_json(
            serde_json::to_value(stored).map_err(|error| integrity(&error.to_string()))?,
        ) != crate::canonical_commit::canonical_json(
            serde_json::to_value(decision).map_err(|error| integrity(&error.to_string()))?,
        ) {
            return Err(conflict("contact_survival_decision_changed"));
        }
        for effect in &decision.effects {
            let row = effect
                .before
                .as_ref()
                .or(effect.after.as_ref())
                .ok_or_else(|| integrity("empty contact survival inverse"))?;
            proof.earliest.entry(row.base().id.clone()).or_insert((
                decision.commit_id.clone(),
                decision.sequence,
                encode_payload(effect)?,
            ));
        }
    }
    for decision in invalidated {
        let key = stable_key(&[space, &decision.commit_id]);
        let receipt: Option<CanonicalCommitReceipt> = proof
            .controls
            .read(client, space, "source_commit_receipt", &key)
            .await?;
        let Some(receipt) = receipt else { continue };
        if receipt.space_id != space || receipt.commit_id != decision.commit_id {
            return Err(conflict("contact_survival_receipt_changed"));
        }
        let Some(publication) = receipt
            .publication_authority
            .as_ref()
            .filter(|fence| &fence.source_id != withdrawn)
        else {
            continue;
        };
        let head: Option<SourceHead> = proof
            .controls
            .read(
                client,
                space,
                "source_head",
                &head_key(space, &publication.source_id),
            )
            .await?;
        let Some(head) = head.filter(|head| {
            head.space_id == space
                && head.source_id == publication.source_id
                && head.lifecycle == SourceVisibility::Active
                && head.published_revision.as_deref() == Some(publication.revision_id.as_str())
        }) else {
            continue;
        };
        let Some(source) = CanonicalRecord::load(client, space, &publication.source_id)
            .await?
            .filter(|row| row.base().deleted_at.is_none())
        else {
            continue;
        };
        if !matches!(&source, CanonicalRecord::Source(_)) || source.base().id != head.source_id {
            return Err(conflict("contact_survival_source_changed"));
        }
        let operation: Option<StoredOperation> = proof
            .controls
            .read(client, space, "source_operation", &publication.operation_id)
            .await?;
        let Some(operation) = operation else { continue };
        crate::source_contact_publication::validate_accepted_operation(
            space,
            publication,
            &operation,
        )?;
        if operation.receipt.state != OperationState::Completed
            || operation.contact_digest.is_none()
        {
            continue;
        }
        let completion_key = crate::source_publication_manifest::manifest_key(space, publication);
        let completion: Option<crate::source_publication_manifest::SourcePublicationCompletion> =
            proof
                .controls
                .read(
                    client,
                    space,
                    crate::source_publication_manifest::COMPLETION_TABLE,
                    &completion_key,
                )
                .await?;
        if completion.as_ref().is_none_or(|row| {
            row.space_id != space
                || row.publication != *publication
                || row.commit_id != receipt.commit_id
        }) {
            return Err(conflict("contact_survival_completion_changed"));
        }
        let revision: Option<RevisionPayload> = proof
            .controls
            .read(
                client,
                space,
                "source_revision",
                &revision_key(space, &publication.source_id, &publication.revision_id),
            )
            .await?;
        let revision = revision
            .ok_or_else(|| conflict("contact_survival_original_source_missing"))?
            .revision;
        if revision.space_id != space
            || revision.source_id != publication.source_id
            || revision.revision_id != publication.revision_id
            || revision.source.base.id != publication.source_id
            || revision.source.base.space_id != space
        {
            return Err(conflict("contact_survival_original_source_changed"));
        }
        // Explicit source-declared dependence is conjunctive. An accepted
        // contact plan does not erase that dependency to manufacture independence.
        if revision.source.base.provenance.sources.contains(withdrawn) {
            continue;
        }
        let contact_key = crate::source_contact_publication::contact_key(space, publication);
        let plan: Option<serde_json::Value> = proof
            .controls
            .read(
                client,
                space,
                crate::source_contact_publication::CONTACT_TABLE,
                &contact_key,
            )
            .await?;
        if plan.is_none() {
            return Err(conflict("accepted_contact_plan_unavailable"));
        }
        let state = crate::source_contact_publication::AcceptedContactState::load(
            client,
            space,
            publication,
        )
        .await?;
        let rows = state.independent_contribution_rows(&revision.source)?;
        // Guard both the actual immutable decision payload and every private
        // original plan read used by the projector. Native sequence is allocated
        // separately; all other fields must equal this selected decision.
        let stored: Option<SourceDecisionReceipt> = proof
            .controls
            .read(client, space, crate::source_decision::DECISION_TABLE, &key)
            .await?;
        let mut stored = stored.ok_or_else(|| conflict("contact_survival_decision_changed"))?;
        stored.sequence = decision.sequence;
        if crate::canonical_commit::canonical_json(
            serde_json::to_value(stored).map_err(|error| integrity(&error.to_string()))?,
        ) != crate::canonical_commit::canonical_json(
            serde_json::to_value(decision).map_err(|error| integrity(&error.to_string()))?,
        ) {
            return Err(conflict("contact_survival_decision_changed"));
        }
        let _: Option<serde_json::Value> = proof
            .controls
            .read(
                client,
                space,
                crate::source_contact_publication::CONTACT_TABLE,
                &contact_key,
            )
            .await?;
        let _: Option<StoredOperation> = proof
            .controls
            .read(client, space, "source_operation", &publication.operation_id)
            .await?;
        for row in rows {
            let id = &row.base().id;
            if row.base().space_id != space
                || !receipt.effect_ids.contains(id)
                || !decision.effects.iter().any(|effect| {
                    effect
                        .after
                        .as_ref()
                        .is_some_and(|after| after.base().id == *id)
                })
            {
                return Err(conflict("contact_survival_contribution_not_committed"));
            }
            let current = CanonicalRecord::load(client, space, id).await?;
            let Some(current) = current.filter(|row| row.base().deleted_at.is_none()) else {
                continue;
            };
            let support_key = stable_key(&[
                space,
                &publication.source_id.to_string(),
                &publication.revision_id,
                &id.to_string(),
            ]);
            let support: Option<SourceEffectSupport> = proof
                .controls
                .read(client, space, "source_effect_support", &support_key)
                .await?;
            let support = support.ok_or_else(|| conflict("contact_survival_support_missing"))?;
            if support.space_id != space
                || support.source_id != publication.source_id
                || support.source_revision != publication.revision_id
                || support.record_id != *id
                || support.last_commit_id.is_none()
                || support.record_fingerprint != current.fingerprint()?
            {
                return Err(conflict("contact_survival_support_changed"));
            }
            let writer = support
                .last_commit_id
                .as_ref()
                .ok_or_else(|| conflict("contact_survival_support_unknown"))?;
            let writer_receipt: Option<CanonicalCommitReceipt> = proof
                .controls
                .read(
                    client,
                    space,
                    "source_commit_receipt",
                    &stable_key(&[space, writer]),
                )
                .await?;
            if writer_receipt.as_ref().is_none_or(|receipt| {
                receipt.space_id != space
                    || receipt.commit_id != *writer
                    || !receipt.effect_ids.contains(id)
            }) {
                return Err(conflict("contact_survival_support_writer_changed"));
            }
            if !crate::source_contact_lineage::proves_original_contact_publication(
                client,
                space,
                publication,
                &current,
                &support,
                &mut proof.controls,
            )
            .await?
            {
                return Err(conflict("contact_survival_original_publication_changed"));
            }
            proof.contributions.entry(id.clone()).or_default().push(row);
        }
    }
    Ok((!proof.contributions.is_empty()).then_some(proof))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use elephant_types::records::{entity::EntityType, BaseFields, Entity};
    use elephant_types::TableName;

    /// A routing oracle only: no manufactured completed operation, sealed plan,
    /// or persisted positive fact/support is used to test the inverse boundary.
    #[test]
    fn surviving_contribution_is_routed_only_to_its_exact_earliest_decision_effect() {
        let row = CanonicalRecord::Entity(Box::new(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".into(),
                "fixture".into(),
            ),
            EntityType::new("person"),
            "Original".into(),
        )));
        let effect = CanonicalEffect {
            before: None,
            after: Some(row.clone()),
        };
        let decision = SourceDecisionReceipt {
            space_id: "space".into(),
            commit_id: "earliest-actual-identity".into(),
            sequence: 7,
            source_ids: Vec::new(),
            dependency_ids: Vec::new(),
            effects: vec![effect.clone()],
            extension_effects: Vec::new(),
            outbox_ids: Vec::new(),
            committed_at: chrono::Utc::now(),
        };
        let proof = PreparedContactSurvival {
            space: "space".into(),
            controls: OriginalControls::default(),
            earliest: BTreeMap::from([(
                row.base().id.clone(),
                (
                    decision.commit_id.clone(),
                    decision.sequence,
                    encode_payload(&effect).unwrap(),
                ),
            )]),
            contributions: BTreeMap::from([(row.base().id.clone(), vec![row.clone()])]),
        };
        assert!(proof.applies_to(&decision, &effect));
        let mut later = decision.clone();
        later.sequence += 1;
        assert!(!proof.applies_to(&later, &effect));
        later.sequence = decision.sequence;
        later.commit_id = "other-decision".into();
        assert!(!proof.applies_to(&later, &effect));
        later.commit_id = decision.commit_id.clone();
        later.space_id = "foreign".into();
        assert!(!proof.applies_to(&later, &effect));
        let mut changed = effect;
        if let Some(CanonicalRecord::Entity(row)) = &mut changed.after {
            row.display_name = "other fact".into();
        }
        assert!(!proof.applies_to(&decision, &changed));
        let wrong = CanonicalEffect {
            before: Some(row),
            after: None,
        };
        assert!(!proof.applies_to(&decision, &wrong));
    }
}
