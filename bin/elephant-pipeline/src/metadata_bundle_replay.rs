//! Reuse deterministic metadata without replacing its original candidate.
//!
//! Metadata is derived from accepted source input. Unlike model candidates,
//! two completed attempts of the same producer can prove identical output.
//! The selected bundle, candidate and run stay unchanged; final publication
//! still performs its original atomic evidence, source and erasure checks.

use elephant_pipeline::scheduler::execution_contract::{FailureClass, WorkFailure};
use elephant_pipeline::scheduler::ingest_work_kinds::{
    SourceMetadataArtifactData, INGEST_METADATA_KIND, SOURCE_METADATA_ARTIFACT_TYPE,
};
use elephant_pipeline::scheduler::staged_commit::COMMIT_BUNDLE_ARTIFACT_TYPE;
use elephant_pipeline::source_queue::{configuration_from_work, publication_from_work};
use elephant_storage::{
    Repository, SpaceConfigRepository, StorageClient, StorageError, WorkArtifactRepository,
    WorkItemRepository, WorkRunRepository,
};
use elephant_types::publication::AttemptFence;
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun,
};
use elephant_types::{RecordId, TableName};
use std::str::FromStr;

const CONTEXT_KEYS: [&str; 3] = ["_publication", "_source_configuration", "_source_plan"];

/// Keep the existing commit child's actual validator predecessor. Authenticate
/// that parent against the selected producer run before comparing the complete
/// child intent; only its per-claim fence is excluded, as in queue idempotence.
pub(super) async fn original_commit_child(
    storage: &StorageClient,
    expected: &WorkItem,
    existing: &WorkItem,
) -> anyhow::Result<bool> {
    if existing.kind != super::COMMIT_ARTIFACTS_KIND
        || existing.kind != expected.kind
        || existing.idempotency_key != expected.idempotency_key
        || !same_lineage(&existing.base, &expected.base)
        || existing.base.created_by != expected.base.created_by
    {
        return Ok(false);
    }
    let Some(parent_id) = existing.payload[super::source_runtime::PREDECESSOR].as_str() else {
        return Ok(false);
    };
    let Ok(parent_id) = RecordId::from_str(parent_id) else {
        return Ok(false);
    };
    let Some(parent) = WorkItemRepository::new(storage.clone())
        .get(&expected.base.space_id, &parent_id)
        .await?
    else {
        return Ok(false);
    };
    if parent.kind != super::VALIDATE_ARTIFACTS_KIND
        || !same_lineage(&parent.base, &expected.base)
        || parent.base.created_by != expected.base.created_by
        || CONTEXT_KEYS
            .iter()
            .any(|key| parent.payload[*key] != expected.payload[*key])
    {
        return Ok(false);
    }
    if parent.payload["work_run_id"] != expected.payload["work_run_id"] {
        // A replaying validator can be the real creator of a missing commit
        // child for an earlier selected run. Authenticate that same metadata
        // equivalence again; never strip or replace its recorded predecessor.
        let Some(publication) = publication_from_work(expected)? else {
            return Ok(false);
        };
        let Some(configuration) = configuration_from_work(expected)? else {
            return Ok(false);
        };
        let Some(parent_run_id) = parent.payload["work_run_id"].as_str() else {
            return Ok(false);
        };
        let Ok(parent_run_id) = RecordId::from_str(parent_run_id) else {
            return Ok(false);
        };
        let Some(parent_run) = WorkRunRepository::new(storage.clone())
            .get(&expected.base.space_id, &parent_run_id)
            .await?
        else {
            return Ok(false);
        };
        let Some(producer) = WorkItemRepository::new(storage.clone())
            .get(&expected.base.space_id, parent_run.work_item.id())
            .await?
        else {
            return Ok(false);
        };
        let artifacts = WorkArtifactRepository::new(storage.clone());
        let Some(candidate) = artifacts
            .get(
                &expected.base.space_id,
                &super::source_runtime::metadata_stage_id(&producer, &parent_run_id)?,
            )
            .await?
        else {
            return Ok(false);
        };
        let Some(selected) = artifacts
            .get(
                &expected.base.space_id,
                &super::source_runtime::metadata_bundle_id(&publication, &configuration),
            )
            .await?
        else {
            return Ok(false);
        };
        let mut proposed = selected.clone();
        proposed.work_run = candidate.work_run.clone();
        proposed.data["source_artifact_id"] = serde_json::json!(candidate.base.id.to_string());
        proposed.status = ArtifactStatus::Validated;
        proposed.committed_at = None;
        proposed.validated_at = candidate.validated_at;
        if Box::pin(original_run(
            storage, &parent, &candidate, &proposed, &selected,
        ))
        .await?
        .map(|id| id.to_string())
        .as_deref()
            != expected.payload["work_run_id"].as_str()
        {
            return Ok(false);
        }
    }
    let mut retained = existing.payload.clone();
    let Some(object) = retained.as_object_mut() else {
        return Ok(false);
    };
    object.remove(elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY);
    let mut intended = expected.payload.clone();
    intended[super::source_runtime::PREDECESSOR] = serde_json::json!(parent_id.to_string());
    Ok(retained == intended)
}

/// Only the exact duplicate record can justify checking a concurrent winner.
/// Storage performs erasure/authority checks before uniqueness checks, so even
/// an existing equal bundle cannot turn another CREATE refusal into success.
pub(super) fn duplicate_bundle(error: &StorageError, id: &RecordId) -> bool {
    let StorageError::Conflict(message) = error else {
        return false;
    };
    let native = format!(
        "Database record `{}` already exists",
        elephant_storage::surreal::thing_from_record_id(id)
    );
    message == &id.to_string()
        || message == &native
        || message == &format!("An error occurred: {native}")
}

/// Complete an absent-bundle observation with create-only persistence. A
/// concurrent metadata winner must pass the same original-result proof; a
/// duplicate-key error alone never authorizes reuse or overwrite.
pub(super) async fn create_bundle(
    storage: &StorageClient,
    validator: &WorkItem,
    candidate: &WorkArtifact,
    proposed: WorkArtifact,
    single_candidate: bool,
) -> anyhow::Result<RecordId> {
    let repository = WorkArtifactRepository::new(storage.clone());
    match repository.create(proposed.clone()).await {
        Ok(_) => Ok(proposed.work_run.id().clone()),
        Err(error) => {
            if single_candidate
                && candidate.artifact_type == SOURCE_METADATA_ARTIFACT_TYPE
                && duplicate_bundle(&error, &proposed.base.id)
            {
                if let Some(selected) = repository
                    .get(&validator.base.space_id, &proposed.base.id)
                    .await?
                {
                    if let Some(run) = Box::pin(original_run(
                        storage, validator, candidate, &proposed, &selected,
                    ))
                    .await?
                    {
                        return Ok(run);
                    }
                }
            }
            Err(error.into())
        }
    }
}

fn same_lineage(left: &BaseFields, right: &BaseFields) -> bool {
    !left.is_deleted()
        && !right.is_deleted()
        && left.space_id == right.space_id
        && left.security == right.security
        && left.subjects == right.subjects
        && left.subjects_state == right.subjects_state
        && left.provenance == right.provenance
}

fn validated(artifact: &WorkArtifact) -> bool {
    !artifact.base.is_deleted()
        && artifact.validated_at.is_some()
        && artifact
            .validation_errors
            .as_ref()
            .is_none_or(Vec::is_empty)
        && match artifact.status {
            ArtifactStatus::Validated | ArtifactStatus::Committing => {
                artifact.committed_at.is_none()
            }
            ArtifactStatus::Committed => artifact.committed_at.is_some(),
            _ => false,
        }
}

fn run_fence(run: &WorkRun, producer: &WorkItem) -> Option<AttemptFence> {
    let fence: AttemptFence =
        serde_json::from_value(run.metrics.custom.get("attempt_fence")?.clone()).ok()?;
    (run.base.space_id == producer.base.space_id
        && !run.base.is_deleted()
        && run.base.id.table() == TableName::WorkRun
        && run.work_item.id() == &producer.base.id
        && run.success
        && run.error.is_none()
        && run.completed_at.is_some_and(|end| end >= run.started_at)
        && run.attempt > 0
        && fence.space_id == producer.base.space_id
        && fence.work_item_id == producer.base.id
        && fence.generation > 0
        && !fence.lease_token.is_empty())
    .then_some(fence)
}

/// Prove that a conflicting run-local candidate is an exact metadata replay.
/// Returns the selected bundle's original run, never a rewritten candidate or
/// refreshed publication authority. All other conflicts retain normal refusal.
pub(super) async fn original_run(
    storage: &StorageClient,
    validator: &WorkItem,
    candidate: &WorkArtifact,
    proposed: &WorkArtifact,
    selected: &WorkArtifact,
) -> anyhow::Result<Option<RecordId>> {
    let Some(publication) = publication_from_work(validator)? else {
        return Ok(None);
    };
    let Some(configuration) = configuration_from_work(validator)? else {
        return Ok(None);
    };
    if candidate.artifact_type != SOURCE_METADATA_ARTIFACT_TYPE
        || proposed.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE
        || selected.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE
        || proposed.data["bundle_kind"] != "source_metadata"
        || proposed.data["payload"]["kind"] != "source_metadata"
        || proposed.data["source_artifact_type"] != SOURCE_METADATA_ARTIFACT_TYPE
        || proposed.work_run != candidate.work_run
        || candidate.status != ArtifactStatus::Validated
        || proposed.status != ArtifactStatus::Validated
        || proposed.work_run == selected.work_run
        || proposed.base.id != selected.base.id
        || proposed.base.id
            != super::source_runtime::metadata_bundle_id(&publication, &configuration)
        || !validated(candidate)
        || !validated(proposed)
        || !validated(selected)
        || !same_lineage(&candidate.base, &proposed.base)
        || !same_lineage(&proposed.base, &selected.base)
        || proposed.base.created_by != selected.base.created_by
    {
        return Ok(None);
    }
    let space = validator.base.space_id.as_str();
    let artifacts = WorkArtifactRepository::new(storage.clone());
    let Some(selected_candidate_id) = selected.data["source_artifact_id"].as_str() else {
        return Ok(None);
    };
    let Ok(selected_candidate_id) = RecordId::from_str(selected_candidate_id) else {
        return Ok(None);
    };
    if selected_candidate_id.table() != TableName::WorkArtifact
        || selected_candidate_id == candidate.base.id
        || proposed.data["source_artifact_id"] != candidate.base.id.to_string()
    {
        return Ok(None);
    }
    let Some(original_candidate) = artifacts.get(space, &selected_candidate_id).await? else {
        return Ok(None);
    };
    if !validated(&original_candidate)
        || !matches!(
            original_candidate.status,
            ArtifactStatus::Validated | ArtifactStatus::Committed
        )
        || original_candidate.artifact_type != SOURCE_METADATA_ARTIFACT_TYPE
        || original_candidate.work_run != selected.work_run
        || !same_lineage(&original_candidate.base, &selected.base)
        || original_candidate.base.created_by != candidate.base.created_by
        || original_candidate.data != candidate.data
    {
        return Ok(None);
    }
    // Only the authenticated candidate reference may differ. In particular,
    // payload, publication, configuration, plan, kind and unknown fields must
    // all match before retaining the selected candidate's original provenance.
    let mut expected_bundle = proposed.data.clone();
    expected_bundle["source_artifact_id"] = selected.data["source_artifact_id"].clone();
    if selected.data != expected_bundle {
        return Ok(None);
    }
    let runs = WorkRunRepository::new(storage.clone());
    let Some(current_run) = runs.get(space, candidate.work_run.id()).await? else {
        return Ok(None);
    };
    let Some(original_run) = runs.get(space, selected.work_run.id()).await? else {
        return Ok(None);
    };
    if current_run.work_item != original_run.work_item {
        return Ok(None);
    }
    let Some(producer) = WorkItemRepository::new(storage.clone())
        .get(space, current_run.work_item.id())
        .await?
    else {
        return Ok(None);
    };
    if producer.kind != INGEST_METADATA_KIND
        || producer.status != WorkItemStatus::Done
        || producer.base.is_deleted()
        || publication_from_work(&producer)?.as_ref() != Some(&publication)
        || configuration_from_work(&producer)?.as_ref() != Some(&configuration)
        || !same_lineage(&producer.base, &validator.base)
        || producer.base.created_by != validator.base.created_by
    {
        return Ok(None);
    }
    let Some(current_fence) = run_fence(&current_run, &producer) else {
        return Ok(None);
    };
    let Some(original_fence) = run_fence(&original_run, &producer) else {
        return Ok(None);
    };
    let latest_fence: AttemptFence = serde_json::from_value(
        producer.payload[elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY].clone(),
    )?;
    if current_fence.generation == original_fence.generation
        || latest_fence.space_id != space
        || latest_fence.work_item_id != producer.base.id
        || latest_fence.generation < current_fence.generation.max(original_fence.generation)
        || latest_fence.lease_token.is_empty()
        || super::source_runtime::metadata_stage_id(&producer, &current_run.base.id)?
            != candidate.base.id
        || super::source_runtime::metadata_stage_id(&producer, &original_run.base.id)?
            != original_candidate.base.id
    {
        return Ok(None);
    }
    let Some(source) = super::source_runtime::accepted_source(storage, validator).await? else {
        return Ok(None);
    };
    if !same_lineage(&source.base, &candidate.base) {
        return Ok(None);
    }
    let mut expected_metadata = serde_json::to_value(SourceMetadataArtifactData::derive(&source))?;
    for key in CONTEXT_KEYS {
        if producer.payload[key] != validator.payload[key] {
            return Ok(None);
        }
        expected_metadata[key] = validator.payload[key].clone();
    }
    if candidate.data != expected_metadata
        || SpaceConfigRepository::new(storage.clone())
            .resolve_space(space)
            .await?
            .token
            != configuration
    {
        return Ok(None);
    }
    if selected.status == ArtifactStatus::Committing {
        // Source finalization normally goes Validated -> Committed atomically.
        // A retained in-progress row grants no completion: leave its original
        // owner/legacy recovery in control and recheck without another producer.
        return Err(WorkFailure {
            class: FailureClass::PendingDependencies,
            message: "Selected metadata bundle commit has not completed".into(),
        }
        .into());
    }
    Ok(Some(original_run.base.id))
}
