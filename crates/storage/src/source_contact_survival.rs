//! Pure composition of independently admitted contact contributions during inverse.
//!
//! The caller owns original admission proof, exact decision ordering and atomic
//! read guards. These functions neither discover authority nor write records.

use crate::canonical_commit::{CanonicalEffect, CanonicalRecord};
use crate::public_mutation::inherit_public_envelope;
use crate::source_decision::{inverse_effect, SourceDecisionReceipt};
use crate::source_publication::{conflict, integrity};
use crate::StorageResult;

/// Supply a surviving factual baseline at the first invalidated effect for one
/// physical record.
///
/// Later inverses have already been applied to `current`.
/// Positive projections contain only fields asserted by their original sources.
pub fn preserve_contact_contributions(
    current: Option<CanonicalRecord>,
    original_current: &CanonicalRecord,
    decision: &SourceDecisionReceipt,
    earliest_invalidated: &CanonicalEffect,
    independently_admitted: &[CanonicalRecord],
) -> StorageResult<Option<CanonicalRecord>> {
    if independently_admitted.is_empty() {
        return inverse_effect(current, earliest_invalidated);
    }
    let Some(current) = current else {
        return Ok(None);
    };
    // A factual contribution cannot undo a later explicit deletion.
    if original_current.base().deleted_at.is_some() {
        return Ok(Some(original_current.clone()));
    }
    if current.base().deleted_at.is_some() {
        return Ok(Some(current));
    }
    let written = earliest_invalidated
        .after
        .as_ref()
        .ok_or_else(|| conflict("contact_survival_has_no_live_output"))?;
    if written.base().deleted_at.is_some()
        || current.base().id != written.base().id
        || original_current.base().id != written.base().id
        || current.base().space_id != decision.space_id
        || original_current.base().space_id != decision.space_id
        || written.base().space_id != decision.space_id
        || !decision
            .effects
            .iter()
            .any(|effect| same_effect(effect, earliest_invalidated))
    {
        return Err(conflict("contact_survival_decision_identity_changed"));
    }
    let mut baseline = earliest_invalidated.before.clone();
    if baseline
        .as_ref()
        .is_some_and(|row| row.base().deleted_at.is_some())
    {
        return Err(conflict("contact_survival_deleted_baseline"));
    }
    for contribution in independently_admitted {
        if contribution.base().id != written.base().id
            || contribution.base().space_id != decision.space_id
            || contribution.base().deleted_at.is_some()
        {
            return Err(conflict("contact_survival_contribution_identity_changed"));
        }
        let mut contribution = contribution.clone();
        retain_original_positive_sets(&mut contribution, original_current)?;
        match &mut baseline {
            Some(baseline) => merge_positive(baseline, &contribution)?,
            None => baseline = Some(contribution),
        }
    }
    let baseline = baseline.ok_or_else(|| integrity("contact survival baseline missing"))?;
    // The ordinary three-way inverse removes unsupported A fields and retains
    // genuinely later edits. This is a baseline substitution before any write,
    // never restoration of a record which the lifecycle has already deleted.
    let mut surviving = inverse_effect(
        Some(current),
        &CanonicalEffect {
            before: Some(baseline),
            after: earliest_invalidated.after.clone(),
        },
    )?;
    // Required contributor restrictions remain conjunctive with independently
    // added restrictions even when a scalar changed again after publication.
    if let Some(surviving) = &mut surviving {
        for contribution in independently_admitted {
            inherit_public_envelope(surviving.base_mut(), contribution.base())?;
        }
    }
    // A later independent display-name edit may coexist with an admitted name.
    // Keep that name as an alias only if the actual pre-withdrawal row kept it.
    if let (Some(CanonicalRecord::Entity(surviving)), CanonicalRecord::Entity(original)) =
        (&mut surviving, original_current)
    {
        for contribution in independently_admitted {
            let CanonicalRecord::Entity(contribution) = contribution else {
                continue;
            };
            let name = &contribution.display_name;
            if (original.display_name == *name || original.aliases.contains(name))
                && surviving.display_name != *name
                && !surviving.aliases.contains(name)
            {
                surviving.aliases.push(name.clone());
            }
        }
    }
    Ok(surviving)
}

fn same_effect(left: &CanonicalEffect, right: &CanonicalEffect) -> bool {
    serde_json::to_value(left)
        .ok()
        .zip(serde_json::to_value(right).ok())
        .is_some_and(|(left, right)| left == right)
}

/// Positive sets manually removed from the actual live record must not be
/// restored merely because an older admission asserted them.
///
/// Inverse overlays
/// may have removed a still-live B contribution, hence the original snapshot.
fn retain_original_positive_sets(
    contribution: &mut CanonicalRecord,
    original: &CanonicalRecord,
) -> StorageResult<()> {
    match (contribution, original) {
        (CanonicalRecord::Entity(row), CanonicalRecord::Entity(original)) => {
            if row.entity_type != original.entity_type {
                return Err(conflict("contact_survival_entity_type_changed"));
            }
            row.aliases.retain(|name| original.aliases.contains(name));
            row.identity_keys
                .retain(|key| original.identity_keys.contains(key));
        }
        (CanonicalRecord::EntityIdentityKey(row), CanonicalRecord::EntityIdentityKey(original)) => {
            if row.key != original.key || row.entity != original.entity {
                return Err(conflict("contact_survival_identity_fact_changed"));
            }
            row.evidence
                .retain(|evidence| original.evidence.contains(evidence));
        }
        (CanonicalRecord::Rel(row), CanonicalRecord::Rel(original)) => {
            if row.subject != original.subject
                || row.object != original.object
                || row.rel_type != original.rel_type
                || row.validity_window != original.validity_window
            {
                return Err(conflict("contact_survival_relationship_fact_changed"));
            }
            row.evidence
                .retain(|evidence| original.evidence.contains(evidence));
        }
        _ => return Err(conflict("contact_survival_record_type_changed")),
    }
    Ok(())
}

fn extend_unique<T: Clone + PartialEq>(target: &mut Vec<T>, additions: &[T]) {
    for value in additions {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

/// Union positive facts into an independently surviving older baseline. Default
/// values in a standalone projection are not deletion or overwrite requests.
fn merge_positive(target: &mut CanonicalRecord, addition: &CanonicalRecord) -> StorageResult<()> {
    if target.base().id != addition.base().id
        || target.base().space_id != addition.base().space_id
        || target.base().deleted_at.is_some()
    {
        return Err(conflict("contact_survival_baseline_identity_changed"));
    }
    inherit_public_envelope(target.base_mut(), addition.base())?;
    merge_provenance(target.base_mut(), addition.base())?;
    match (target, addition) {
        (CanonicalRecord::Entity(target), CanonicalRecord::Entity(addition)) => {
            if target.entity_type != addition.entity_type {
                return Err(conflict("contact_survival_entity_type_changed"));
            }
            if target.display_name != addition.display_name
                && !target.aliases.contains(&addition.display_name)
            {
                target.aliases.push(addition.display_name.clone());
            }
            extend_unique(&mut target.aliases, &addition.aliases);
            extend_unique(&mut target.identity_keys, &addition.identity_keys);
        }
        (
            CanonicalRecord::EntityIdentityKey(target),
            CanonicalRecord::EntityIdentityKey(addition),
        ) => {
            if target.key != addition.key || target.entity != addition.entity {
                return Err(conflict("contact_survival_identity_fact_changed"));
            }
            extend_unique(&mut target.evidence, &addition.evidence);
        }
        (CanonicalRecord::Rel(target), CanonicalRecord::Rel(addition)) => {
            if target.subject != addition.subject
                || target.object != addition.object
                || target.rel_type != addition.rel_type
                || target.validity_window != addition.validity_window
            {
                return Err(conflict("contact_survival_relationship_fact_changed"));
            }
            extend_unique(&mut target.evidence, &addition.evidence);
        }
        _ => return Err(conflict("contact_survival_record_type_changed")),
    }
    Ok(())
}

fn merge_provenance(
    target: &mut elephant_types::records::BaseFields,
    addition: &elephant_types::records::BaseFields,
) -> StorageResult<()> {
    let mut value =
        serde_json::to_value(&target.provenance).map_err(|error| integrity(&error.to_string()))?;
    let incoming = serde_json::to_value(&addition.provenance)
        .map_err(|error| integrity(&error.to_string()))?;
    let values = value
        .as_object_mut()
        .ok_or_else(|| integrity("provenance shape"))?;
    for (field, incoming) in incoming
        .as_object()
        .ok_or_else(|| integrity("provenance shape"))?
    {
        if field == "sources"
            || incoming.is_null()
            || incoming.as_array().is_some_and(Vec::is_empty)
        {
            continue;
        }
        match values.get(field) {
            None | Some(serde_json::Value::Null) => {
                values.insert(field.clone(), incoming.clone());
            }
            Some(previous) if previous == incoming => {}
            Some(serde_json::Value::Array(previous)) if previous.is_empty() => {
                values.insert(field.clone(), incoming.clone());
            }
            _ => return Err(conflict("contact_survival_ambiguous_provenance")),
        }
    }
    target.provenance =
        serde_json::from_value(value).map_err(|error| integrity(&error.to_string()))?;
    Ok(())
}

#[cfg(test)]
#[path = "source_contact_survival_tests.rs"]
mod tests;
