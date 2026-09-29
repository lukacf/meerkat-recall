//! Helpers for building versioned provenance from work runs.

use crate::error::PipelineError;
use elephant_storage::{Repository, StorageClient, WorkRunRepository};
use elephant_types::records::base::{Extractor, Provenance};
use elephant_types::RecordId;

pub(crate) async fn apply_work_run_provenance(
    storage: &StorageClient,
    work_run_id: &RecordId,
    space_id: &str,
    provenance: &mut Provenance,
) -> Result<(), PipelineError> {
    let work_run_repo = WorkRunRepository::new(storage.clone());
    let work_run = work_run_repo
        .get(space_id, work_run_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(work_run_id.to_string()))?;

    provenance.work_run_id = Some(work_run.base.id.clone());
    provenance.run_id = Some(work_run.base.id.to_string());
    provenance.prompt_id = work_run.prompt_id.clone();
    provenance.config_id = work_run.config_hash.clone();
    provenance.extractor = Some(Extractor {
        name: Some(work_run.worker_id.clone()),
        version: Some(work_run.worker_version.clone()),
        model: work_run.model_version.clone(),
    });

    Ok(())
}
