//! Bounds of ordinary record replacement.
//!
//! Read/write clearance does not confer authority to relabel existing data.
//! A separately authorized release operation is not implemented yet, so ordinary
//! replacements preserve the complete security and subject envelope.

use std::collections::HashSet;

use elephant_types::records::base::BaseFields;

/// Refuses an ordinary update that changes resource identity or its envelope.
///
/// Label, handling and subject order and duplicate entries are not policy facts.
/// Subject completeness is a fact and cannot be replaced by ordinary write access.
///
/// # Errors
///
/// Returns an error when a separate identity or relabel operation is required.
pub fn authorize_ordinary_update(
    existing: &BaseFields,
    proposed: &BaseFields,
) -> Result<(), &'static str> {
    if existing.id != proposed.id || existing.space_id != proposed.space_id {
        return Err("ordinary updates cannot change record identity or space");
    }
    let old = &existing.security;
    let new = &proposed.security;
    let subjects = |base: &BaseFields| {
        base.subjects
            .iter()
            .map(|subject| subject.id().clone())
            .collect::<HashSet<_>>()
    };
    if old.policy_id != new.policy_id
        || old.level != new.level
        || old.labels.iter().collect::<HashSet<_>>() != new.labels.iter().collect::<HashSet<_>>()
        || old.handling.iter().collect::<HashSet<_>>()
            != new.handling.iter().collect::<HashSet<_>>()
        || subjects(existing) != subjects(proposed)
        || existing.subjects_state != proposed.subjects_state
    {
        return Err(
            "security and subject changes require a separately authorized relabel operation",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::SubjectsState;
    use elephant_types::{Link, RecordId};

    #[test]
    fn ordinary_update_preserves_all_envelope_facts() {
        let mut original = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "space".into(),
            "actor".into(),
        );
        original.security.policy_id = "policy".into();
        original.security.level = 2;
        original.security.labels = vec!["private".into(), "medical".into()];
        original.security.handling = vec!["no-export".into()];
        original.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        original.subjects_state = SubjectsState::Partial;
        let mutations: [fn(&mut BaseFields); 7] = [
            |base| base.security.policy_id = "other".into(),
            |base| base.security.level = 0,
            |base| base.security.labels.clear(),
            |base| base.security.handling = vec!["handling:subjectless_ok".into()],
            |base| base.subjects.clear(),
            |base| base.subjects_state = SubjectsState::Complete,
            |base| base.space_id = "other".into(),
        ];
        for mutate in mutations {
            let mut proposed = original.clone();
            mutate(&mut proposed);
            assert!(authorize_ordinary_update(&original, &proposed).is_err());
        }
        let mut proposed = original.clone();
        proposed.security.labels.reverse();
        proposed.subjects.extend(original.subjects.clone());
        assert_eq!(authorize_ordinary_update(&original, &proposed), Ok(()));
    }
}
