//! A new job's space envelope is prepared before caller authorization, then
//! guarded inside the existing atomic job, private origin and queue handoff.
use super::{ReprocessJob, ReprocessJobRepository, ReprocessStatus};
use crate::{EmbeddedMutation, SpaceConfigRepository, StorageError, StorageResult};
use elephant_types::records::SpaceConfig;
use elephant_types::TableName;
use std::collections::BTreeMap;
use surrealdb::sql::Value;

/// Immutable new-job proposal and its original configured-space observation.
/// The caller authorizes `job()` before committing this exact preparation.
#[derive(Debug)]
pub struct PreparedReprocessJobCreation {
    pub(super) job: ReprocessJob,
    pub(super) configuration: SpaceConfig,
}

impl PreparedReprocessJobCreation {
    /// The complete classified job envelope the caller must authorize.
    #[must_use]
    pub const fn job(&self) -> &ReprocessJob {
        &self.job
    }
}

fn changed() -> StorageError {
    StorageError::Conflict("reprocess_creation_space_changed".into())
}

impl ReprocessJobRepository {
    /// Prepare a new Pending job under the named space's complete envelope.
    ///
    /// Existing trusted creation APIs remain explicit; this preparation never
    /// assumes default clearance when the named configuration is unavailable.
    ///
    /// # Errors
    /// Refuses malformed/new-job state, an unknown space or a failed read.
    pub async fn prepare_creation(
        &self,
        mut job: ReprocessJob,
    ) -> StorageResult<PreparedReprocessJobCreation> {
        if job.base.id.table() != TableName::ReprocessJob
            || job.base.deleted_at.is_some()
            || job.base.space_id.trim().is_empty()
            || job.status != ReprocessStatus::Pending
        {
            return Err(StorageError::Conflict(
                "reprocess_create_requires_pending".into(),
            ));
        }
        let configuration = SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(&job.base.space_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("space_config:{}", job.base.space_id)))?;
        job.base.security.clone_from(&configuration.base.security);
        job.base.subjects.clone_from(&configuration.base.subjects);
        job.base.subjects_state = configuration.base.subjects_state;
        job.base
            .provenance
            .clone_from(&configuration.base.provenance);
        Ok(PreparedReprocessJobCreation { job, configuration })
    }
}

pub(super) fn check_configuration(
    tx: &EmbeddedMutation<'_>,
    original: &SpaceConfig,
) -> StorageResult<()> {
    let configurations: Vec<_> = tx
        .scan::<SpaceConfig>()?
        .into_iter()
        .filter(|current| {
            current.base.space_id == original.base.space_id && current.base.deleted_at.is_none()
        })
        .collect();
    if configurations.as_slice() != std::slice::from_ref(original) {
        return Err(changed());
    }
    Ok(())
}

pub(super) fn append_configuration_guard(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    original: &SpaceConfig,
) -> StorageResult<()> {
    bindings.insert(
        "prepared_job_space".into(),
        original.base.space_id.clone().into(),
    );
    bindings.insert(
        "prepared_job_config_id".into(),
        crate::surreal::thing_from_record_id(&original.base.id).into(),
    );
    bindings.insert(
        "prepared_job_config".into(),
        crate::canonical_commit::native_expected_fields(crate::surreal::space_config_to_value(
            original,
        )?),
    );
    sql.push_str("LET $prepared_job_spaces = SELECT * FROM space_config WHERE space_id = $prepared_job_space AND deleted_at IS NONE;
        IF array::len($prepared_job_spaces) != 1 OR $prepared_job_spaces[0].id != $prepared_job_config_id { THROW 'reprocess_creation_space_changed'; };
        LET $prepared_job_current = $prepared_job_spaces[0];
        FOR $field IN object::entries($prepared_job_config) {
            IF $prepared_job_current[$field[0]] != $field[1] { THROW 'reprocess_creation_space_changed'; };
        };
        ");
    // The only caller creates the actual ReprocessJob in this transaction.
    // Its migration031 event and every SpaceConfig writer both write the same
    // canonical_epoch(space) row, supplying the native snapshot conflict. Do not
    // replace this obligation with a same-value UPDATE (optimized away).
    Ok(())
}

#[cfg(test)]
#[path = "reprocess_creation_tests.rs"]
mod tests;
