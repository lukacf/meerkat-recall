//! Staged commit workflow for work artifacts.
//!
//! Per spec §5.4: Artifacts are staged, validated, and then committed
//! to canonical tables in a deterministic workflow.
//!
//! Per spec §6.2: Budget is enforced at both scheduling AND commit time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use elephant_storage::work_artifact_repository::WorkArtifactRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::RecordId;

use crate::error::PipelineError;
use crate::resolution::{
    validate_resolution_patch_payload, ResolutionPatch, RESOLUTION_PATCH_ARTIFACT_TYPE,
};
use crate::scheduler::budget::{BudgetCheckResult, BudgetEnforcer};
use crate::scheduler::extraction_work_kinds::{
    ExtractionArtifactData, KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
};
use crate::scheduler::ingest_work_kinds::{
    SourceMetadataArtifactData, SOURCE_METADATA_ARTIFACT_TYPE,
};
use std::future::Future;

/// Artifact type emitted by `validate_artifacts` and consumed by `commit_artifacts`.
pub const COMMIT_BUNDLE_ARTIFACT_TYPE: &str = "commit_bundle";

/// Typed commit bundle dispatch kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitBundleKind {
    /// Source intelligence / metadata normalization bundle.
    SourceMetadata,
    /// Extracted knowledge candidates bundle.
    KnowledgeCandidates,
    /// Resolution patch set bundle.
    ResolutionPatchSet,
}

/// Typed payload committed by `commit_artifacts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CommitBundlePayload {
    /// Normalized source metadata.
    SourceMetadata(SourceMetadataArtifactData),
    /// Extracted knowledge candidates ready for canonicalization.
    KnowledgeCandidates(ExtractionArtifactData),
    /// Resolution patch set ready for commit.
    ResolutionPatchSet(ResolutionPatch),
}

/// Validated commit payload derived from an earlier candidate artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitBundleArtifactData {
    /// The staged artifact that was validated into this bundle.
    pub source_artifact_id: String,
    /// The type of the staged artifact that produced this bundle.
    pub source_artifact_type: String,
    /// The semantic bundle kind to dispatch.
    pub bundle_kind: CommitBundleKind,
    /// The validated artifact payload to commit.
    pub payload: CommitBundlePayload,
}

/// Staged commit handler.
#[derive(Debug, Clone)]
pub struct StagedCommit {
    artifact_repo: WorkArtifactRepository,
    #[allow(dead_code)]
    storage: StorageClient,
}

impl StagedCommit {
    /// Creates a new staged commit handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            artifact_repo: WorkArtifactRepository::new(storage.clone()),
            storage,
        }
    }

    /// Stages an artifact for later validation and commit.
    ///
    /// # Errors
    ///
    /// Returns an error if staging fails.
    pub async fn stage(&self, artifact: WorkArtifact) -> Result<WorkArtifact, PipelineError> {
        let mut staged = artifact;
        staged.status = ArtifactStatus::Draft;
        staged.validated_at = None;
        staged.committed_at = None;

        self.artifact_repo
            .create(staged)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Validates a staged artifact.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub async fn validate<F>(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
        validator: F,
    ) -> Result<WorkArtifact, PipelineError>
    where
        F: FnOnce(&WorkArtifact) -> Result<(), Vec<String>>,
    {
        let artifact = self
            .artifact_repo
            .get(space_id, artifact_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

        if artifact.status != ArtifactStatus::Draft {
            return Err(PipelineError::InvalidStateTransition {
                from: artifact.status.to_string(),
                to: "validation".to_string(),
            });
        }

        let mut updated = artifact;
        match validator(&updated) {
            Ok(()) => {
                updated.status = ArtifactStatus::Validated;
                updated.validated_at = Some(Utc::now());
                updated.validation_errors = None;
            }
            Err(errors) => {
                updated.status = ArtifactStatus::Rejected;
                updated.validation_errors = Some(errors);
            }
        }

        let updated = self
            .artifact_repo
            .update_if_status(space_id, artifact_id, updated, ArtifactStatus::Draft)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::InvalidState("Artifact already validated".to_string()))?;

        Ok(updated)
    }

    /// Commits a validated artifact to canonical tables.
    ///
    /// # Errors
    ///
    /// Returns an error if commit fails.
    pub async fn commit<F, Fut>(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
        committer: F,
    ) -> Result<WorkArtifact, PipelineError>
    where
        F: FnOnce(&WorkArtifact) -> Fut,
        Fut: Future<Output = Result<(), PipelineError>>,
    {
        let artifact = self
            .artifact_repo
            .get(space_id, artifact_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

        if artifact.status == ArtifactStatus::Committed {
            return Ok(artifact);
        }

        if artifact.status != ArtifactStatus::Validated {
            return Err(PipelineError::InvalidStateTransition {
                from: artifact.status.to_string(),
                to: "commit".to_string(),
            });
        }

        // Claim the commit transition BEFORE executing the committer to prevent
        // concurrent workers or retries from double-applying side effects.
        let mut committing = artifact;
        committing.status = ArtifactStatus::Committing;

        let committing = self
            .artifact_repo
            .update_if_status(space_id, artifact_id, committing, ArtifactStatus::Validated)
            .await
            .map_err(PipelineError::StorageError)?;

        let committing = match committing {
            Some(artifact) => artifact,
            None => {
                let artifact = self
                    .artifact_repo
                    .get(space_id, artifact_id)
                    .await
                    .map_err(PipelineError::StorageError)?
                    .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

                return match artifact.status {
                    ArtifactStatus::Committed => Ok(artifact),
                    ArtifactStatus::Committing => Err(PipelineError::InvalidState(
                        "Artifact commit already in progress".to_string(),
                    )),
                    _ => Err(PipelineError::InvalidState(format!(
                        "Artifact no longer validated: {}",
                        artifact.status
                    ))),
                };
            }
        };

        let commit_result = committer(&committing).await;
        if let Err(err) = commit_result {
            let mut restored = committing.clone();
            // Restore the bundle to `Validated` so the generic worker-level
            // transient retry path can reattempt it. Retries remain bounded by
            // `PipelineConfig::transient_retry_max_attempts`; once that cap is
            // exhausted the shared worker loop stops retrying and the item
            // falls through the normal failed/deadletter handling path.
            restored.status = ArtifactStatus::Validated;
            let _ = self
                .artifact_repo
                .update_if_status(space_id, artifact_id, restored, ArtifactStatus::Committing)
                .await;
            return Err(err);
        }

        // Mark as committed
        let mut updated = committing;
        updated.status = ArtifactStatus::Committed;
        updated.committed_at = Some(Utc::now());

        let updated = self
            .artifact_repo
            .update_if_status(space_id, artifact_id, updated, ArtifactStatus::Committing)
            .await
            .map_err(PipelineError::StorageError)?;

        match updated {
            Some(artifact) => Ok(artifact),
            None => {
                let artifact = self
                    .artifact_repo
                    .get(space_id, artifact_id)
                    .await
                    .map_err(PipelineError::StorageError)?
                    .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

                if artifact.status == ArtifactStatus::Committed {
                    Ok(artifact)
                } else {
                    Err(PipelineError::InvalidState(
                        "Artifact commit finalization failed".to_string(),
                    ))
                }
            }
        }
    }

    /// Gets all artifacts for a work run.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_artifacts_for_run(
        &self,
        work_run_id: &RecordId,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        self.artifact_repo
            .find_by_work_run(work_run_id)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Restores stale in-progress commit bundles so a timed-out commit work item
    /// can safely retry the staged commit path.
    ///
    /// Worker-level timeouts can abort the future after the artifact has moved
    /// from `Validated` to `Committing`, bypassing the normal error restoration
    /// branch in `commit`. The committers are expected to be idempotent, so the
    /// durable recovery action is to make abandoned bundles visible to retry.
    ///
    /// # Errors
    ///
    /// Returns an error if the artifact scan or conditional update fails.
    pub async fn recover_abandoned_commits_for_run(
        &self,
        space_id: &str,
        work_run_id: &RecordId,
        abandoned_before: DateTime<Utc>,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        let artifacts = self.get_artifacts_for_run(work_run_id).await?;
        let mut recovered = Vec::new();

        for artifact in artifacts {
            if artifact.base.space_id != space_id
                || artifact.status != ArtifactStatus::Committing
                || artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE
                || artifact.base.updated_at >= abandoned_before
            {
                continue;
            }

            let mut restored = artifact.clone();
            restored.status = ArtifactStatus::Validated;
            restored.committed_at = None;

            if let Some(restored) = self
                .artifact_repo
                .update_if_status(
                    space_id,
                    &artifact.base.id,
                    restored,
                    ArtifactStatus::Committing,
                )
                .await
                .map_err(PipelineError::StorageError)?
            {
                recovered.push(restored);
            }
        }

        Ok(recovered)
    }

    /// Gets draft artifacts pending validation.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_pending_validation(
        &self,
        space_id: &str,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        self.artifact_repo
            .find_by_status(space_id, ArtifactStatus::Draft)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Gets validated artifacts pending commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_pending_commit(
        &self,
        space_id: &str,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        self.artifact_repo
            .find_by_status(space_id, ArtifactStatus::Validated)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Commits a validated artifact with budget check at commit time.
    ///
    /// Per spec §6.2: Budget is enforced at both scheduling AND commit time.
    /// This method re-checks budget using actual cost before committing.
    ///
    /// # Errors
    ///
    /// Returns an error if budget check fails or commit fails.
    pub async fn commit_with_budget_check<F, Fut>(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
        budget_enforcer: &BudgetEnforcer,
        mode: IngestMode,
        actual_cost: f64,
        committer: F,
    ) -> Result<WorkArtifact, PipelineError>
    where
        F: FnOnce(&WorkArtifact) -> Fut,
        Fut: Future<Output = Result<(), PipelineError>>,
    {
        // Check budget at commit time per §6.2
        let budget_result = budget_enforcer
            .check_budget_at_commit(mode, actual_cost)
            .await?;

        match budget_result {
            BudgetCheckResult::Allowed => {
                // Budget allows commit, proceed
                self.commit(space_id, artifact_id, committer).await
            }
            BudgetCheckResult::Rejected { reason } => Err(PipelineError::BudgetExceeded(format!(
                "Commit-time budget check failed: {reason}"
            ))),
            BudgetCheckResult::Downgrade { reason, .. } => {
                // At commit time, downgrade is treated as rejection since work is already done
                Err(PipelineError::BudgetExceeded(format!(
                    "Budget exhausted during work execution: {reason}"
                )))
            }
            BudgetCheckResult::Queued { worker_kind } => {
                // At commit time, queuing doesn't make sense - treat as rejection
                Err(PipelineError::BudgetExceeded(format!(
                    "Commit blocked due to concurrency limit: {worker_kind}"
                )))
            }
        }
    }
}

/// Work kind for validating artifacts.
pub mod validate_artifacts {
    use super::{
        validate_resolution_patch_payload, ArtifactStatus, ExtractionArtifactData, PipelineError,
        RecordId, ResolutionPatch, SourceMetadataArtifactData, StagedCommit, WorkArtifact,
        KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE, RESOLUTION_PATCH_ARTIFACT_TYPE,
        SOURCE_METADATA_ARTIFACT_TYPE,
    };
    use crate::scheduler::extraction_work_kinds::LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE;
    use std::str::FromStr;

    /// Work kind identifier.
    pub const KIND: &str = "validate_artifacts";

    fn validate_source_metadata_payload(artifact: &WorkArtifact) -> Vec<String> {
        let mut errors = Vec::new();
        let payload =
            match serde_json::from_value::<SourceMetadataArtifactData>(artifact.data.clone()) {
                Ok(payload) => payload,
                Err(error) => {
                    errors.push(format!("Invalid source_metadata payload: {error}"));
                    return errors;
                }
            };

        if RecordId::from_str(&payload.source_id).is_err() {
            errors.push(format!("Invalid source_id: {}", payload.source_id));
        }
        if !payload.raw_metadata_patch.is_object() {
            errors.push("source_metadata.raw_metadata_patch must be a JSON object".to_string());
        }
        if payload
            .normalized_uri
            .as_deref()
            .is_some_and(|uri| uri.trim().is_empty())
        {
            errors.push("normalized_uri cannot be blank".to_string());
        }

        errors
    }

    fn validate_knowledge_candidates_payload(artifact: &WorkArtifact) -> Vec<String> {
        let mut errors = Vec::new();
        let payload = match serde_json::from_value::<ExtractionArtifactData>(artifact.data.clone())
        {
            Ok(payload) => payload,
            Err(error) => {
                errors.push(format!("Invalid knowledge_candidates payload: {error}"));
                return errors;
            }
        };

        if RecordId::from_str(&payload.node_id).is_err() {
            errors.push(format!("Invalid node_id: {}", payload.node_id));
        }
        if RecordId::from_str(&payload.revision_id).is_err() {
            errors.push(format!("Invalid revision_id: {}", payload.revision_id));
        }
        if payload
            .source_id
            .as_deref()
            .is_some_and(|source_id| RecordId::from_str(source_id).is_err())
        {
            errors.push(format!(
                "Invalid source_id: {}",
                payload.source_id.unwrap_or_default()
            ));
        }

        for (idx, assertion) in payload.assertions.iter().enumerate() {
            if assertion.subject_text.trim().is_empty() {
                errors.push(format!("Assertion {idx} subject_text cannot be empty"));
            }
            if assertion.predicate_text.trim().is_empty() {
                errors.push(format!("Assertion {idx} predicate_text cannot be empty"));
            }
            if assertion.evidence_end < assertion.evidence_start {
                errors.push(format!(
                    "Assertion {idx} has invalid evidence span {}..{}",
                    assertion.evidence_start, assertion.evidence_end
                ));
            }
            if assertion
                .event_lineage_key
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            {
                errors.push(format!("Assertion {idx} event_lineage_key cannot be blank"));
            }
        }

        errors
    }

    async fn validation_errors(
        staged_commit: &StagedCommit,
        space_id: &str,
        artifact: &WorkArtifact,
    ) -> Result<Vec<String>, PipelineError> {
        let errors = match artifact.artifact_type.as_str() {
            SOURCE_METADATA_ARTIFACT_TYPE => validate_source_metadata_payload(artifact),
            KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE | LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE => {
                validate_knowledge_candidates_payload(artifact)
            }
            RESOLUTION_PATCH_ARTIFACT_TYPE => {
                let patch = serde_json::from_value::<ResolutionPatch>(artifact.data.clone())
                    .map_err(|error| {
                        PipelineError::InvalidInput(format!(
                            "Invalid resolution patch data: {error}"
                        ))
                    })?;
                validate_resolution_patch_payload(&staged_commit.storage, space_id, &patch).await?
            }
            _ => {
                if artifact.data.is_null() {
                    vec!["Artifact data cannot be null".to_string()]
                } else {
                    Vec::new()
                }
            }
        };

        Ok(errors)
    }

    /// Validates all draft artifacts for a work run.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub async fn execute(
        staged_commit: &StagedCommit,
        space_id: &str,
        work_run_id: &RecordId,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        let artifacts = staged_commit.get_artifacts_for_run(work_run_id).await?;

        let mut results = Vec::new();
        for artifact in artifacts {
            if artifact.status == ArtifactStatus::Draft {
                let errors = validation_errors(staged_commit, space_id, &artifact).await?;
                let validated = staged_commit
                    .validate(space_id, &artifact.base.id, move |_artifact| {
                        if errors.is_empty() {
                            Ok(())
                        } else {
                            Err(errors)
                        }
                    })
                    .await?;
                results.push(validated);
            }
        }

        Ok(results)
    }
}

/// Work kind for committing artifacts.
pub mod commit_artifacts {
    use super::{
        ArtifactStatus, BudgetEnforcer, IngestMode, PipelineError, RecordId, StagedCommit,
        WorkArtifact,
    };
    use elephant_storage::work_run_repository::WorkRunRepository;
    use elephant_storage::Repository;

    /// Work kind identifier.
    pub const KIND: &str = "commit_artifacts";

    /// Commits all validated artifacts for a work run.
    ///
    /// # Errors
    ///
    /// Returns an error if commit fails.
    pub async fn execute(
        staged_commit: &StagedCommit,
        space_id: &str,
        work_run_id: &RecordId,
        _budget_enforcer: &BudgetEnforcer,
        _mode: IngestMode,
    ) -> Result<Vec<WorkArtifact>, PipelineError> {
        let work_run_repo = WorkRunRepository::new(staged_commit.storage.clone());
        let work_run = work_run_repo
            .get(space_id, work_run_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(work_run_id.to_string()))?;
        let _actual_cost = work_run.metrics.estimated_cost_euros;

        let artifacts = staged_commit.get_artifacts_for_run(work_run_id).await?;

        let mut results = Vec::new();
        let mut saw_validated = false;
        for artifact in artifacts {
            match artifact.status {
                ArtifactStatus::Committed => results.push(artifact),
                ArtifactStatus::Validated => {
                    saw_validated = true;
                }
                _ => {}
            }
        }

        if saw_validated {
            return Err(PipelineError::ConfigError(
                "commit_artifacts::execute requires a runtime-specific typed committer".to_string(),
            ));
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::work::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
    use elephant_types::Link;

    const TEST_SPACE: &str = "default";

    fn make_artifact(work_run_id: &RecordId) -> WorkArtifact {
        WorkArtifact {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkArtifact),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            work_run: Link::new(work_run_id.clone()),
            artifact_type: "test_artifact".to_string(),
            data: serde_json::json!({"key": "value"}),
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        }
    }

    async fn create_work_run(
        storage: &StorageClient,
        work_run_id: &RecordId,
        work_item_id: &RecordId,
    ) {
        let work_run = WorkRun {
            base: BaseFields::new(
                work_run_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            work_item: Link::new(work_item_id.clone()),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "test-worker".to_string(),
            worker_version: "test".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };

        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                TEST_SPACE.to_string(),
                "test".to_string(),
            ),
            kind: "test".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("test:{work_item_id}"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        elephant_storage::WorkItemRepository::new(storage.clone())
            .create(work_item)
            .await
            .expect("create work item");
        elephant_storage::WorkRunRepository::new(storage.clone())
            .create(work_run)
            .await
            .expect("create work run");
    }

    #[tokio::test]
    async fn test_stage_artifact() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        let staged = staged_commit.stage(artifact).await.expect("stage");
        assert_eq!(staged.status, ArtifactStatus::Draft);
        assert!(staged.validated_at.is_none());
        assert!(staged.committed_at.is_none());
    }

    #[tokio::test]
    async fn test_validate_artifact_success() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        let staged = staged_commit.stage(artifact).await.expect("stage");

        let validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        assert_eq!(validated.status, ArtifactStatus::Validated);
        assert!(validated.validated_at.is_some());
        assert!(validated.validation_errors.is_none());
    }

    #[tokio::test]
    async fn test_validate_artifact_failure() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        let staged = staged_commit.stage(artifact).await.expect("stage");

        let rejected = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| {
                Err(vec!["Invalid data".to_string()])
            })
            .await
            .expect("validate");

        assert_eq!(rejected.status, ArtifactStatus::Rejected);
        assert!(rejected.validation_errors.is_some());
        assert_eq!(rejected.validation_errors.unwrap(), vec!["Invalid data"]);
    }

    #[tokio::test]
    async fn test_commit_artifact() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        let staged = staged_commit.stage(artifact).await.expect("stage");
        let validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        let committed = staged_commit
            .commit(TEST_SPACE, &validated.base.id, |_| async { Ok(()) })
            .await
            .expect("commit");

        assert_eq!(committed.status, ArtifactStatus::Committed);
        assert!(committed.committed_at.is_some());
    }

    #[tokio::test]
    async fn test_recover_abandoned_commit_restores_stale_bundle() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let mut artifact = make_artifact(&work_run_id);
        artifact.artifact_type = COMMIT_BUNDLE_ARTIFACT_TYPE.to_string();

        let staged = staged_commit.stage(artifact).await.expect("stage");
        let validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        let mut committing = validated.clone();
        committing.status = ArtifactStatus::Committing;
        staged_commit
            .artifact_repo
            .update_if_status(
                TEST_SPACE,
                &validated.base.id,
                committing,
                ArtifactStatus::Validated,
            )
            .await
            .expect("claim commit")
            .expect("status match");

        let recovered = staged_commit
            .recover_abandoned_commits_for_run(
                TEST_SPACE,
                &work_run_id,
                Utc::now() + chrono::Duration::seconds(1),
            )
            .await
            .expect("recover abandoned commit");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, ArtifactStatus::Validated);
    }

    #[tokio::test]
    async fn test_recover_abandoned_commit_leaves_fresh_bundle_claimed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let mut artifact = make_artifact(&work_run_id);
        artifact.artifact_type = COMMIT_BUNDLE_ARTIFACT_TYPE.to_string();

        let staged = staged_commit.stage(artifact).await.expect("stage");
        let validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        let mut committing = validated.clone();
        committing.status = ArtifactStatus::Committing;
        staged_commit
            .artifact_repo
            .update_if_status(
                TEST_SPACE,
                &validated.base.id,
                committing,
                ArtifactStatus::Validated,
            )
            .await
            .expect("claim commit")
            .expect("status match");

        let recovered = staged_commit
            .recover_abandoned_commits_for_run(
                TEST_SPACE,
                &work_run_id,
                Utc::now() - chrono::Duration::seconds(1),
            )
            .await
            .expect("recover abandoned commit");

        assert!(recovered.is_empty());

        let artifact = staged_commit
            .artifact_repo
            .get(TEST_SPACE, &validated.base.id)
            .await
            .expect("load artifact")
            .expect("artifact");
        assert_eq!(artifact.status, ArtifactStatus::Committing);
    }

    #[tokio::test]
    async fn test_cannot_commit_unvalidated() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        let staged = staged_commit.stage(artifact).await.expect("stage");

        let result = staged_commit
            .commit(TEST_SPACE, &staged.base.id, |_| async { Ok(()) })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_artifacts_work_kind() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage);

        let work_run_id = RecordId::new_v4(TableName::WorkRun);

        // Stage two artifacts
        let artifact1 = make_artifact(&work_run_id);
        let artifact2 = make_artifact(&work_run_id);

        staged_commit.stage(artifact1).await.expect("stage1");
        staged_commit.stage(artifact2).await.expect("stage2");

        // Validate all
        let validated = validate_artifacts::execute(&staged_commit, TEST_SPACE, &work_run_id)
            .await
            .expect("validate all");

        assert_eq!(validated.len(), 2);
        for artifact in validated {
            assert_eq!(artifact.status, ArtifactStatus::Validated);
        }
    }

    #[tokio::test]
    async fn test_commit_with_budget_check_allowed() {
        use elephant_types::records::ingest::{Budget, DowngradePolicy, IngestMode};
        use std::collections::HashMap;

        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage.clone());

        let budget = Budget {
            daily_euros_retrieval: 10.0,
            daily_euros_graph: 50.0,
            daily_items_retrieval: Some(100),
            daily_items_graph: Some(10),
            per_item_tokens: Some(100_000),
            per_item_wall_time_secs: Some(300),
            concurrent_workers: HashMap::new(),
            downgrade_policy: DowngradePolicy::DegradeMode,
        };
        let budget_enforcer = BudgetEnforcer::new(storage, budget, TEST_SPACE.to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let artifact = make_artifact(&work_run_id);

        // Stage and validate
        let staged = staged_commit.stage(artifact).await.expect("stage");
        let validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        // Commit with budget check - small cost should pass
        let committed = staged_commit
            .commit_with_budget_check(
                TEST_SPACE,
                &validated.base.id,
                &budget_enforcer,
                IngestMode::Retrieval,
                1.0, // actual cost
                |_| async { Ok(()) },
            )
            .await
            .expect("commit");

        assert_eq!(committed.status, ArtifactStatus::Committed);
        assert!(committed.committed_at.is_some());
    }

    #[tokio::test]
    async fn test_commit_artifacts_execute_rejects_silent_validated_noop() {
        use elephant_types::records::ingest::{Budget, DowngradePolicy, IngestMode};
        use std::collections::HashMap;

        let storage = StorageClient::new_memory().await.expect("storage");
        let staged_commit = StagedCommit::new(storage.clone());

        let budget = Budget {
            daily_euros_retrieval: 10.0,
            daily_euros_graph: 50.0,
            daily_items_retrieval: Some(100),
            daily_items_graph: Some(10),
            per_item_tokens: Some(100_000),
            per_item_wall_time_secs: Some(300),
            concurrent_workers: HashMap::new(),
            downgrade_policy: DowngradePolicy::DegradeMode,
        };
        let budget_enforcer = BudgetEnforcer::new(storage.clone(), budget, TEST_SPACE.to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        create_work_run(&storage, &work_run_id, &work_item_id).await;
        let artifact = make_artifact(&work_run_id);
        let staged = staged_commit.stage(artifact).await.expect("stage");
        let _validated = staged_commit
            .validate(TEST_SPACE, &staged.base.id, |_| Ok(()))
            .await
            .expect("validate");

        let result = commit_artifacts::execute(
            &staged_commit,
            TEST_SPACE,
            &work_run_id,
            &budget_enforcer,
            IngestMode::Graph,
        )
        .await;

        assert!(matches!(result, Err(PipelineError::ConfigError(_))));
    }
}
