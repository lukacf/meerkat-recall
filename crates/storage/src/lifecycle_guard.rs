//! Guarded lifecycle mutations.
//!
//! An API restore authorizes a snapshot of the row and then mutates it. The
//! guard makes the mutation itself require that the stored row still has the
//! snapshot's authorization envelope and exact lifecycle, so a concurrent
//! reclassification, subject change, or delete/restore cycle in between is a
//! conflict instead of being acted on (and returned) under the earlier decision.

use std::collections::BTreeMap;

use elephant_types::records::base::BaseFields;
use elephant_types::RecordId;

use crate::error::{StorageError, StorageResult};

/// Whether `current` still has the base authorization envelope and exact
/// lifecycle of `authorized`.
///
/// Compares security, subjects, subjects state and the deletion timestamp (or
/// its absence): a row restored and deleted again since the snapshot carries a
/// different tombstone and does not match.
#[must_use]
pub fn base_envelope_unchanged(current: &BaseFields, authorized: &BaseFields) -> bool {
    current.security == authorized.security
        && current.subjects == authorized.subjects
        && current.subjects_state == authorized.subjects_state
        && current.deleted_at == authorized.deleted_at
}

/// The `SurrealDB` form of [`base_envelope_unchanged`].
///
/// Returns conditions to `AND` into an `UPDATE` predicate, with their
/// bindings, encoded exactly as rows are written.
pub(crate) fn base_envelope_condition(
    authorized: &BaseFields,
) -> StorageResult<(String, BTreeMap<String, surrealdb::sql::Value>)> {
    let lifecycle = if authorized.deleted_at.is_some() {
        "deleted_at = $guard_deleted_at"
    } else {
        "deleted_at IS NONE"
    };
    let condition = format!(
        "security = $guard_security AND subjects = $guard_subjects \
         AND subjects_state = $guard_subjects_state AND {lifecycle}"
    );
    let mut bindings = crate::bindings! {
        "guard_security" => crate::surreal::security_to_value(&authorized.security),
        "guard_subjects" => surrealdb::sql::Value::Array(crate::surreal::things_from_links(
            authorized.subjects.clone(),
        )),
        "guard_subjects_state" => crate::surreal::enum_to_surreal(&authorized.subjects_state)?,
    };
    if let Some(deleted_at) = authorized.deleted_at {
        bindings.insert(
            "guard_deleted_at".to_string(),
            surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(deleted_at)),
        );
    }
    Ok((condition, bindings))
}

/// The content-free conflict a failed guard returns.
pub(crate) fn guard_conflict(kind: &str, id: &RecordId) -> StorageError {
    StorageError::Conflict(format!(
        "{kind} changed since it was authorized (envelope or lifecycle): {id}"
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::records::base::SubjectsState;
    use elephant_types::{Link, TableName};

    #[test]
    fn test_each_envelope_field_breaks_the_match() {
        let authorized = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "space".to_string(),
            "tester".to_string(),
        );
        assert!(base_envelope_unchanged(&authorized, &authorized));

        let mut level = authorized.clone();
        level.security.level += 1;
        let mut labels = authorized.clone();
        labels.security.labels.push("private".to_string());
        let mut subjects = authorized.clone();
        subjects
            .subjects
            .push(Link::new(RecordId::new_v4(TableName::Entity)));
        let mut state = authorized.clone();
        state.subjects_state = if authorized.subjects_state == SubjectsState::Complete {
            SubjectsState::Partial
        } else {
            SubjectsState::Complete
        };
        let mut deleted = authorized.clone();
        deleted.deleted_at = Some(chrono::Utc::now());
        for changed in [level, labels, subjects, state, deleted.clone()] {
            assert!(!base_envelope_unchanged(&changed, &authorized));
        }

        // A different tombstone is a different lifecycle.
        let mut later = deleted.clone();
        later.deleted_at = deleted
            .deleted_at
            .map(|at| at + chrono::Duration::seconds(1));
        assert!(!base_envelope_unchanged(&later, &deleted));
    }
}
