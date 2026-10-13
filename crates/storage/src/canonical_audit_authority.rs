//! Per-audit authority comes from its observed target, not the whole decision.

use super::{integrity, AuditLog, BaseFields, CanonicalPlan, CanonicalRecord};
use crate::{public_mutation::inherit_public_envelope, StorageResult};

/// Derive audit authority from the target's original and prepared images.
///
/// Missing or mismatched target/snapshot authority fails.
/// The payload, identity, timestamps and provenance chain remain unchanged.
///
/// # Errors
/// Returns an integrity error for absent or mismatched authority, or a conflict
/// when the target images cross space or policy domains.
pub fn inherit_maintenance_audit_authority(
    plan: &CanonicalPlan,
    audit: &mut AuditLog,
) -> StorageResult<()> {
    let authority = affected_authority(plan, audit)?;
    audit.base.security = authority.security;
    audit.base.subjects = authority.subjects;
    audit.base.subjects_state = authority.subjects_state;
    audit.base.provenance.sources = authority.provenance.sources;
    Ok(())
}

pub(super) fn validate(plan: &CanonicalPlan) -> StorageResult<()> {
    for effect in &plan.effects {
        let Some(CanonicalRecord::AuditLog(audit)) = &effect.after else {
            continue;
        };
        let expected = affected_authority(plan, audit)?;
        // Caller-supplied provenance cannot omit a before-only source or add
        // unrelated context. Stronger authorization restrictions are allowed.
        if source_ids(&audit.base) != source_ids(&expected) {
            return Err(integrity("maintenance audit source authority mismatch"));
        }
        let mut joined = audit.base.clone();
        inherit_public_envelope(&mut joined, &expected)?;
        if !same_authority(&audit.base, &joined) {
            return Err(integrity("maintenance audit weakens target authority"));
        }
    }
    Ok(())
}

fn affected_authority(plan: &CanonicalPlan, audit: &AuditLog) -> StorageResult<BaseFields> {
    if audit.record == audit.base.id {
        return Err(integrity("maintenance audit cannot certify itself"));
    }
    let mut matching = plan.effects.iter().filter(|effect| {
        effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .is_some_and(|row| row.base().id == audit.record)
    });
    let effect = matching.next();
    if matching.next().is_some() {
        return Err(integrity("maintenance audit target is ambiguous"));
    }
    let (before, after) = if let Some(effect) = effect {
        (effect.before.as_ref(), effect.after.as_ref())
    } else {
        let mut matching = plan
            .dependencies
            .iter()
            .filter(|row| row.base().id == audit.record);
        let observed = matching
            .next()
            .ok_or_else(|| integrity("maintenance audit target was not observed"))?;
        if matching.next().is_some() {
            return Err(integrity("maintenance audit target is ambiguous"));
        }
        (Some(observed), Some(observed))
    };
    let selected = after
        .or(before)
        .ok_or_else(|| integrity("maintenance audit target has no image"))?;
    let mut authority = selected.base().clone();
    for row in before.into_iter().chain(after) {
        if matches!(row, CanonicalRecord::AuditLog(_))
            || row.base().id != audit.record
            || row.base().space_id != audit.base.space_id
        {
            return Err(integrity("maintenance audit target identity mismatch"));
        }
        inherit_public_envelope(&mut authority, row.base())?;
    }
    // Coalesced effects may also retain an original observed target image.
    for row in plan
        .dependencies
        .iter()
        .filter(|row| row.base().id == audit.record)
    {
        inherit_public_envelope(&mut authority, row.base())?;
    }
    // An audit can describe an intermediate mutation within a coalesced plan.
    // Its supplied envelope cannot add authority beyond the trusted target
    // union; the audit itself always retains that complete union. Domain
    // payloads are deliberately not reserialized or float-normalized.
    validate_snapshot(audit.before.as_ref(), &authority)?;
    validate_snapshot(audit.after.as_ref(), &authority)?;
    authority.provenance.sources.sort();
    authority.provenance.sources.dedup();
    Ok(authority)
}

fn validate_snapshot(
    snapshot: Option<&serde_json::Value>,
    authority: &BaseFields,
) -> StorageResult<()> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let object = snapshot
        .as_object()
        .ok_or_else(|| integrity("maintenance audit snapshot has no envelope"))?;
    if !["id", "space_id", "security", "subjects", "subjects_state"]
        .iter()
        .all(|field| object.contains_key(*field))
        || snapshot
            .get("provenance")
            .and_then(|value| value.get("sources"))
            .is_none()
    {
        return Err(integrity("maintenance audit snapshot authority is missing"));
    }
    // This is an in-memory typed audit payload, not a database proxy or a
    // custom deserializer. Mandatory authority fields are checked above.
    let base: BaseFields = serde_json::from_value(snapshot.clone())
        .map_err(|_| integrity("maintenance audit snapshot authority is malformed"))?;
    let mut joined = authority.clone();
    inherit_public_envelope(&mut joined, &base)?;
    if base.id != authority.id || !same_authority(&joined, authority) {
        return Err(integrity("maintenance audit snapshot authority mismatch"));
    }
    Ok(())
}

fn source_ids(base: &BaseFields) -> Vec<String> {
    sorted(base.provenance.sources.iter().map(ToString::to_string))
}

fn sorted(values: impl Iterator<Item = String>) -> Vec<String> {
    let mut values: Vec<_> = values.collect();
    values.sort();
    values.dedup();
    values
}

fn same_authority(left: &BaseFields, right: &BaseFields) -> bool {
    left.space_id == right.space_id
        && left.security.policy_id == right.security.policy_id
        && left.security.level == right.security.level
        && sorted(left.security.labels.iter().cloned())
            == sorted(right.security.labels.iter().cloned())
        && sorted(left.security.handling.iter().cloned())
            == sorted(right.security.handling.iter().cloned())
        && sorted(left.subjects.iter().map(|subject| subject.id().to_string()))
            == sorted(
                right
                    .subjects
                    .iter()
                    .map(|subject| subject.id().to_string()),
            )
        && left.subjects_state == right.subjects_state
        && source_ids(left) == source_ids(right)
}

#[cfg(test)]
#[path = "canonical_audit_authority_tests.rs"]
mod tests;
