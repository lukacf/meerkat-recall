//! Whole-source finalization: stage every node, then publish one revision.
//!
//! The runner records the closed manifest before fanout and supplies the
//! finalizer's own originally captured attempt. Preparation never changes live
//! canonical records. Missing bundles/indexes remain incomplete across restart.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::Utc;
use elephant_storage::canonical_commit::{
    CanonicalCommit, CanonicalCommitReceipt, CanonicalPlan, CanonicalPublicationContext,
    CanonicalRecord, CanonicalRepository,
};
use elephant_storage::source_publication_manifest::{
    check_expected_bundle, check_staging_bundle, SourcePublicationManifestRepository,
};
use elephant_storage::{Repository, StorageClient, WorkArtifactRepository};
use elephant_types::publication::{AttemptFence, PublicationFence};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::RecordId;
use sha2::{Digest, Sha256};

use crate::error::{PipelineError, PipelineResult};
use crate::scheduler::extraction_work_kinds::ExtractionArtifactData;
use crate::scheduler::inline_promotion::{
    prepare_assertion_groups_with_plan, PromotionGroup, PromotionOptions,
};
use crate::scheduler::promotion_disambiguation::{
    DisambiguationRequest, PromotionDisambiguationDecision, PromotionDisambiguator,
};
use crate::scheduler::staged_commit::{
    CommitBundleArtifactData, CommitBundleKind, CommitBundlePayload,
};

/// Durable runner work kind, recovered from manifests in every named space.
pub const FINALIZE_SOURCE_REVISION_KIND: &str = "finalize_source_revision";

/// Execute a finalizer under the worker's existing retry contract. An epoch
/// race in preparation or the final commit, before any disambiguation, is local
/// preparation contention, not a failed provider attempt. Reclaiming rebuilds
/// the plan with fresh reads while keeping
/// the accepted publication identity and checking the new original attempt.
///
/// # Errors
/// Returns the original failure, except that an exact epoch conflict from a
/// provider-free preparation requests a bounded prerequisite poll.
pub async fn execute_finalizer(
    storage: &StorageClient,
    space: &str,
    publication: &PublicationFence,
    attempt: &AttemptFence,
    actor: &str,
    options: &PromotionOptions,
) -> anyhow::Result<CanonicalCommitReceipt> {
    let consulted = Arc::new(AtomicBool::new(false));
    let mut tracked = options.clone();
    if let Some(inner) = tracked.disambiguator.take() {
        tracked.disambiguator = Some(Arc::new(ObservedDisambiguator {
            inner,
            consulted: Arc::clone(&consulted),
        }));
    }
    Box::pin(finalize_source_revision(
        storage,
        space,
        publication,
        attempt,
        actor,
        &tracked,
    ))
    .await
    .map_err(|error| execution_error(error, consulted.load(Ordering::Acquire)))
}

#[derive(Debug)]
struct ObservedDisambiguator {
    inner: Arc<dyn PromotionDisambiguator>,
    consulted: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl PromotionDisambiguator for ObservedDisambiguator {
    async fn disambiguate(
        &self,
        input: &DisambiguationRequest,
    ) -> PipelineResult<Option<PromotionDisambiguationDecision>> {
        // Mark before awaiting, including provider errors/cancellation. Never
        // refund a potentially paid call merely because no verdict returned.
        self.consulted.store(true, Ordering::Release);
        self.inner.disambiguate(input).await
    }
}

fn execution_error(error: PipelineError, disambiguation_consulted: bool) -> anyhow::Error {
    // Both codes report a rejected original planning epoch: promotion/retirement
    // use the first, while retirement recheck and the final atomic commit use
    // the second. Never refresh that epoch or replay this rejected plan. A new
    // claim must redo all reads and authority checks, even after a late conflict.
    if !disambiguation_consulted
        && matches!(&error, PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(code)) if matches!(code.as_str(), "canonical_epoch_changed" | "canonical_inputs_changed"))
    {
        return crate::scheduler::execution_contract::WorkFailure {
            class: crate::scheduler::execution_contract::FailureClass::PendingDependencies,
            message: "Source publication epoch changed; reprepare from current canonical state"
                .into(),
        }
        .into();
    }
    error.into()
}

/// A node commit only acknowledges an already durable validated bundle. It
/// performs no canonical writes and leaves the artifact Validated for the final
/// source transaction. Duplicate notifications are harmless.
pub async fn register_bundle(
    storage: &StorageClient,
    space: &str,
    publication: &PublicationFence,
    artifact_id: &RecordId,
    attempt: &AttemptFence,
) -> PipelineResult<()> {
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let manifest = Box::pin(manifests.staging_manifest_for_attempt(space, publication, attempt))
        .await
        .map_err(finalization_error)?;
    let repo = WorkArtifactRepository::new(storage.clone());
    let bundle = required_artifact(&repo, space, artifact_id).await?;
    check_staging_bundle(manifest.as_ref(), space, publication, &bundle)
        .map_err(finalization_error)?;
    let _ = Box::pin(checked_candidate(&repo, space, publication, &bundle)).await?;
    Box::pin(manifests.register_bundle(space, publication, artifact_id, attempt))
        .await
        .map_err(finalization_error)
}

/// Gather exactly the declared work, prepare all groups with one shared overlay,
/// and publish the complete source revision in one fenced canonical transaction.
///
/// The attempt argument belongs to this finalizer claim. Never pass an extraction
/// worker's attempt or fetch a new fence during completion. Replay reads the
/// canonical receipt before preparing, so existing assertions never masquerade
/// as proof that effects or outbox delivery completed.
pub async fn finalize_source_revision(
    storage: &StorageClient,
    space: &str,
    publication: &PublicationFence,
    attempt: &AttemptFence,
    actor: &str,
    options: &PromotionOptions,
) -> PipelineResult<CanonicalCommitReceipt> {
    if attempt.space_id != space || attempt.generation == 0 || attempt.lease_token.is_empty() {
        return Err(PipelineError::LeaseConflict(
            "source finalizer requires its original captured attempt".into(),
        ));
    }
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    // Each phase keeps its large storage/planning future off the dispatcher's
    // stack. The original authority and sequential transaction order are unchanged.
    let requirements = Box::pin(manifests.requirements_for_attempt(space, publication, attempt))
        .await
        .map_err(finalization_error)?;
    let commit_id = requirements.commit_id();
    let input_fingerprint = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(requirements.manifest()).map_err(invalid_payload)?)
    );
    let canonical = CanonicalRepository::new(storage.clone());
    if let Some(receipt) = canonical.receipt(space, &commit_id).await? {
        if receipt.input_fingerprint != input_fingerprint
            || receipt.space_id != space
            || receipt.source_id.as_ref() != Some(&publication.source_id)
            || receipt.source_revision.as_deref() != Some(publication.revision_id.as_str())
        {
            return Err(PipelineError::ValidationError(
                "source completion receipt identity mismatch".into(),
            ));
        }
        let completion = manifests
            .completion(space, publication)
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState(
                    "canonical source receipt lacks atomic completion manifest".into(),
                )
            })?;
        if completion.commit_id != receipt.commit_id || completion.publication != *publication {
            return Err(PipelineError::ValidationError(
                "source completion certificate identity mismatch".into(),
            ));
        }
        return Ok(receipt);
    }

    // This is an early readiness check, repeated inside the final transaction.
    // No provider or disambiguator call should run for an incomplete revision.
    Box::pin(manifests.check_ready(&requirements))
        .await
        .map_err(finalization_error)?;
    let manifest = requirements.manifest();
    let repo = WorkArtifactRepository::new(storage.clone());
    let mut groups = Vec::with_capacity(manifest.bundles.len());
    let mut artifacts = Vec::new();
    let mut metadata = Vec::new();
    let mut artifact_ids = BTreeSet::new();
    for id in manifest
        .metadata_artifact_ids
        .iter()
        .chain(manifest.bundles.iter().map(|bundle| &bundle.artifact_id))
    {
        let artifact = required_artifact(&repo, space, id).await?;
        check_expected_bundle(manifest, &artifact).map_err(finalization_error)?;
        let candidate = checked_candidate(&repo, space, publication, &artifact).await?;
        let bundle: CommitBundleArtifactData = serde_json::from_value(artifact.data.clone())
            .map_err(|error| {
                PipelineError::ValidationError(format!("invalid source bundle: {error}"))
            })?;
        match bundle.payload {
            CommitBundlePayload::KnowledgeCandidates(data) => {
                let node_id = RecordId::from_str(&data.node_id).map_err(invalid_id)?;
                let revision_id = RecordId::from_str(&data.revision_id).map_err(invalid_id)?;
                let assertions = data
                    .assertions
                    .iter()
                    .map(|raw| raw.to_raw(&revision_id, &node_id))
                    .collect::<PipelineResult<Vec<_>>>()?;
                groups.push(PromotionGroup {
                    assertions,
                    work_run_id: artifact.work_run.id().clone(),
                    doc_node_id: node_id,
                    revision_id,
                });
            }
            CommitBundlePayload::SourceMetadata(data) => metadata.push(data),
            CommitBundlePayload::ResolutionPatchSet(_) => {
                return Err(PipelineError::ValidationError(
                    "resolution patches are not source extraction bundles".into(),
                ));
            }
        }
        for record in [artifact, candidate] {
            if !artifact_ids.insert(record.base.id.to_string()) {
                return Err(PipelineError::ValidationError(
                    "source bundles alias a staged artifact".into(),
                ));
            }
            artifacts.push(record);
        }
    }
    if groups.len() != manifest.bundles.len() {
        return Err(PipelineError::InvalidState(
            "source_publication_incomplete:missing_extraction_group".into(),
        ));
    }
    for id in &manifest.planning_artifact_ids {
        let artifact = required_artifact(&repo, space, id).await?;
        if artifact.status != ArtifactStatus::Validated
            || artifact.validated_at.is_none()
            || artifact.artifact_type != "extraction_plan"
            || !artifact_ids.insert(artifact.base.id.to_string())
        {
            return Err(PipelineError::InvalidState(
                "source_publication_incomplete:planning_receipt_not_validated".into(),
            ));
        }
        artifacts.push(artifact);
    }
    let context = CanonicalPublicationContext {
        source: publication.clone(),
        attempt: attempt.clone(),
    };
    let mut final_options = options.clone();
    final_options.publication = Some(context.clone());
    final_options.commit_id = Some(commit_id.clone());
    let mut plan = Box::pin(canonical.prepare_source_replacement(space, publication)).await?;
    // Contact effects were authorized and captured with this exact acceptance.
    // Add them before promotion reads the shared overlay, preserving the original
    // before-images and predicate guards. Never regenerate caller authority from
    // public metadata, restart retirement, or overwrite promoted after-images.
    Box::pin(canonical.append_accepted_source_effects(&mut plan, space, publication)).await?;
    if !groups.is_empty() {
        plan = Box::pin(prepare_assertion_groups_with_plan(
            storage,
            space,
            &groups,
            actor,
            &final_options,
            plan,
        ))
        .await?
        .plan;
    }
    // Metadata/retrieval and explicitly planned zero-node graph outcomes retain
    // the same authorized plan. The manifest still checks their required effects.
    for artifact in artifacts {
        append_artifact_completion(&mut plan, artifact)?;
    }
    // The intake lifecycle is a projection of the completed source transaction,
    // never a node worker's interpretation of queue quiescence.
    let source = elephant_storage::SourceRepository::new(storage.clone())
        .get(space, &publication.source_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound("source publication input disappeared".into()))?;
    let mut completed_source = source.clone();
    for item in metadata {
        apply_source_metadata(&mut completed_source, &item)?;
    }
    completed_source.current_ingest_mode = Some(manifest.mode);
    completed_source.latest_ingest_decision = requirements
        .published_ingest_decision()
        .map_err(finalization_error)?;
    completed_source.lifecycle_status = match manifest.mode {
        elephant_types::records::ingest::IngestMode::Skip => {
            elephant_types::records::source::SourceLifecycleStatus::Skipped
        }
        elephant_types::records::ingest::IngestMode::Metadata => {
            elephant_types::records::source::SourceLifecycleStatus::MetadataReady
        }
        elephant_types::records::ingest::IngestMode::Retrieval => {
            elephant_types::records::source::SourceLifecycleStatus::RetrievalReady
        }
        elephant_types::records::ingest::IngestMode::Graph => {
            elephant_types::records::source::SourceLifecycleStatus::GraphReady
        }
    };
    completed_source.base.updated_at = Utc::now();
    plan.push(
        Some(CanonicalRecord::Source(Box::new(source))),
        Some(CanonicalRecord::Source(Box::new(completed_source))),
    )?;
    Box::pin(canonical.commit(CanonicalCommit {
        space_id: space.to_owned(),
        commit_id,
        input_fingerprint,
        replay_output: None,
        publication: Some(context),
        plan,
        finalization: Some(requirements),
        maintenance: None,
    }))
    .await
    .map_err(finalization_error)
}

async fn required_artifact(
    repo: &WorkArtifactRepository,
    space: &str,
    id: &RecordId,
) -> PipelineResult<WorkArtifact> {
    repo.get(space, id).await?.ok_or_else(|| {
        PipelineError::InvalidState(
            "source_publication_incomplete:expected_artifact_missing".into(),
        )
    })
}

async fn checked_candidate(
    repo: &WorkArtifactRepository,
    space: &str,
    publication: &PublicationFence,
    artifact: &WorkArtifact,
) -> PipelineResult<WorkArtifact> {
    let bundle: CommitBundleArtifactData =
        serde_json::from_value(artifact.data.clone()).map_err(|error| {
            PipelineError::ValidationError(format!("invalid source bundle: {error}"))
        })?;
    let candidate_id = RecordId::from_str(&bundle.source_artifact_id).map_err(invalid_id)?;
    let candidate = required_artifact(repo, space, &candidate_id).await?;
    if candidate.base.id == artifact.base.id
        || candidate.work_run != artifact.work_run
        || candidate.artifact_type != bundle.source_artifact_type
        || candidate.base.security != artifact.base.security
        || candidate.base.subjects != artifact.base.subjects
        || candidate.base.subjects_state != artifact.base.subjects_state
        || !matches!(
            candidate.status,
            ArtifactStatus::Validated | ArtifactStatus::Committed
        )
        || candidate.validated_at.is_none()
        || candidate
            .validation_errors
            .as_ref()
            .is_some_and(|errors| !errors.is_empty())
    {
        return Err(PipelineError::ValidationError(
            "source bundle differs from validated candidate lineage".into(),
        ));
    }
    match (&bundle.bundle_kind, &bundle.payload) {
        (CommitBundleKind::KnowledgeCandidates, CommitBundlePayload::KnowledgeCandidates(data)) => {
            let mut original: ExtractionArtifactData =
                serde_json::from_value(candidate.data.clone()).map_err(|error| {
                    PipelineError::ValidationError(format!("invalid candidate: {error}"))
                })?;
            // The runner may attach the original acceptance to a candidate
            // whose extraction stage emitted no source ID. No semantics change.
            if original
                .source_id
                .as_ref()
                .is_some_and(|source| source != &publication.source_id.to_string())
            {
                return Err(PipelineError::ValidationError(
                    "candidate source differs from accepted source".into(),
                ));
            }
            original.source_id = Some(publication.source_id.to_string());
            if serde_json::to_value(&original).map_err(invalid_payload)?
                != serde_json::to_value(data).map_err(invalid_payload)?
            {
                return Err(PipelineError::ValidationError(
                    "source bundle changed validated interpretation".into(),
                ));
            }
        }
        (CommitBundleKind::SourceMetadata, CommitBundlePayload::SourceMetadata(data)) => {
            let mut original = candidate.data.clone();
            let object = original.as_object_mut().ok_or_else(|| {
                PipelineError::ValidationError("metadata candidate must be an object".into())
            })?;
            let publication_value = serde_json::to_value(publication).map_err(invalid_payload)?;
            if object
                .get("_publication")
                .is_some_and(|value| value != &publication_value)
            {
                return Err(PipelineError::ValidationError(
                    "metadata candidate publication differs from accepted source".into(),
                ));
            }
            // Producer context is checked against the original claim and sealed
            // configuration by the manifest transaction; these tags are not metadata.
            for key in ["_publication", "_source_configuration", "_source_plan"] {
                object.remove(key);
            }
            if original != serde_json::to_value(data).map_err(invalid_payload)? {
                return Err(PipelineError::ValidationError(
                    "source bundle changed validated metadata".into(),
                ));
            }
        }
        _ => {
            return Err(PipelineError::ValidationError(
                "source bundle kind/payload mismatch".into(),
            ))
        }
    }
    Ok(candidate)
}

fn apply_source_metadata(
    source: &mut elephant_types::records::Source,
    metadata: &crate::scheduler::ingest_work_kinds::SourceMetadataArtifactData,
) -> PipelineResult<()> {
    source.author.clone_from(&metadata.normalized_author);
    source.channel.clone_from(&metadata.normalized_channel);
    source.timestamp = metadata.normalized_timestamp;
    source.uri.clone_from(&metadata.normalized_uri);
    // Consumer identity belongs to acceptance. Derived aliases remain in the
    // normalized metadata receipt and cannot rebind that stable identity.
    if !source.metadata.is_object() {
        source.metadata = serde_json::json!({});
    }
    let object = source.metadata.as_object_mut().ok_or_else(|| {
        PipelineError::ValidationError("source metadata must be an object".into())
    })?;
    object.insert(
        crate::scheduler::ingest_work_kinds::SOURCE_NORMALIZED_METADATA_KEY.into(),
        serde_json::to_value(metadata).map_err(invalid_payload)?,
    );
    if let Some(title) = &metadata.normalized_title {
        object
            .entry("title".to_string())
            .or_insert_with(|| serde_json::Value::String(title.clone()));
    }
    Ok(())
}

fn append_artifact_completion(
    plan: &mut CanonicalPlan,
    artifact: WorkArtifact,
) -> PipelineResult<()> {
    if artifact.status == ArtifactStatus::Committed {
        plan.dependencies
            .push(CanonicalRecord::WorkArtifact(Box::new(artifact)));
        return Ok(());
    }
    if artifact.status != ArtifactStatus::Validated {
        return Err(PipelineError::InvalidState(
            "source_publication_incomplete:artifact_not_validated".into(),
        ));
    }
    let mut committed = artifact.clone();
    committed.status = ArtifactStatus::Committed;
    committed.committed_at = Some(Utc::now());
    committed.base.updated_at = Utc::now();
    plan.push(
        Some(CanonicalRecord::WorkArtifact(Box::new(artifact))),
        Some(CanonicalRecord::WorkArtifact(Box::new(committed))),
    )?;
    Ok(())
}

fn invalid_id(error: elephant_types::TypeError) -> PipelineError {
    PipelineError::ValidationError(format!("invalid source artifact record identity: {error}"))
}

fn invalid_payload(error: serde_json::Error) -> PipelineError {
    PipelineError::ValidationError(format!("invalid source artifact payload: {error}"))
}

fn finalization_error(error: elephant_storage::StorageError) -> PipelineError {
    if error.to_string().contains("source_publication_incomplete:") {
        PipelineError::InvalidState(error.to_string())
    } else {
        PipelineError::StorageError(error)
    }
}

#[cfg(test)]
#[path = "source_finalization_contention_tests.rs"]
mod contention_tests;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_storage::source_publication_manifest::SourcePublicationManifest;
    use elephant_types::provider_binding::SpaceConfigToken;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::ingest::IngestMode;
    use elephant_types::{Link, TableName};

    #[test]
    fn finalizer_epoch_contention_does_not_exhaust_provider_attempt_budget() {
        use crate::scheduler::execution_contract::{classify_failure, retry_action, RetryAction};
        // The observed failure was a third-attempt finalizer after other sources
        // changed the space epoch. Every retry still has to rebuild and commit;
        // the adapter grants neither a receipt nor permission to skip the CAS.
        for code in ["canonical_epoch_changed", "canonical_inputs_changed"] {
            for attempt in [1, 2, 3, 14] {
                let error = execution_error(
                    elephant_storage::StorageError::Conflict(code.into()).into(),
                    false,
                );
                assert_eq!(
                    retry_action(classify_failure(&error), attempt, 3, 10, 120),
                    RetryAction::Blocked { delay_secs: 5 }
                );
            }
        }
    }

    #[test]
    fn finalizer_contention_adapter_preserves_other_fences_and_paid_limits() {
        use crate::scheduler::execution_contract::{
            classify_failure, retry_action, FailureClass, RetryAction,
        };
        use elephant_storage::StorageError;
        for (error, expected) in [
            (
                PipelineError::LeaseConflict("original attempt lost".into()),
                FailureClass::LostLease,
            ),
            (
                StorageError::Conflict("stale_attempt".into()).into(),
                FailureClass::LostLease,
            ),
            (
                StorageError::Conflict("stale_source_publication".into()).into(),
                FailureClass::StaleInput,
            ),
            (
                StorageError::Conflict("source_configuration_changed".into()).into(),
                FailureClass::ConfigurationBlocked,
            ),
            (
                StorageError::Conflict("canonical_row_changed".into()).into(),
                FailureClass::Transient,
            ),
            (
                StorageError::Conflict("prefix canonical_epoch_changed suffix".into()).into(),
                FailureClass::Transient,
            ),
            (
                StorageError::Conflict("prefix canonical_inputs_changed suffix".into()).into(),
                FailureClass::Transient,
            ),
            (
                StorageError::QueryError("canonical_inputs_changed".into()).into(),
                FailureClass::Terminal,
            ),
            (
                StorageError::DataIntegrityError("canonical_inputs_changed".into()).into(),
                FailureClass::Terminal,
            ),
            (
                PipelineError::SecurityViolation("canonical_inputs_changed".into()),
                FailureClass::Terminal,
            ),
            (
                StorageError::Conflict("canonical_effect_changed".into()).into(),
                FailureClass::Transient,
            ),
            (
                StorageError::Conflict("canonical_dependency_changed".into()).into(),
                FailureClass::Transient,
            ),
            (
                PipelineError::ValidationError("canonical_epoch_changed".into()),
                FailureClass::Terminal,
            ),
            (
                PipelineError::SecurityViolation("canonical_epoch_changed".into()),
                FailureClass::Terminal,
            ),
        ] {
            assert_eq!(classify_failure(&execution_error(error, false)), expected);
        }
        for code in ["canonical_epoch_changed", "canonical_inputs_changed"] {
            let paid = execution_error(StorageError::Conflict(code.into()).into(), true);
            assert_eq!(classify_failure(&paid), FailureClass::Transient);
            assert_eq!(
                retry_action(classify_failure(&paid), 3, 3, 10, 120),
                RetryAction::Fail
            );
            assert_eq!(
                classify_failure(&anyhow::Error::from(StorageError::Conflict(code.into()))),
                FailureClass::Transient
            );
        }
        // The generic storage policy is deliberately unchanged outside this
        // finalizer boundary, including unrelated workers/provider operations.
        assert_eq!(
            classify_failure(&anyhow::Error::from(StorageError::Conflict(
                "canonical_epoch_changed".into()
            ))),
            FailureClass::Transient
        );
    }

    #[derive(Debug)]
    struct RefusedDisambiguation;

    #[async_trait::async_trait]
    impl PromotionDisambiguator for RefusedDisambiguation {
        async fn disambiguate(
            &self,
            _: &DisambiguationRequest,
        ) -> PipelineResult<Option<PromotionDisambiguationDecision>> {
            Err(PipelineError::ExtractionError(
                "provider did not return a verdict".into(),
            ))
        }
    }

    #[tokio::test]
    async fn even_unsuccessful_disambiguation_prevents_contention_refund() {
        let consulted = Arc::new(AtomicBool::new(false));
        let observer = ObservedDisambiguator {
            inner: Arc::new(RefusedDisambiguation),
            consulted: Arc::clone(&consulted),
        };
        assert!(!consulted.load(Ordering::Acquire));
        observer
            .disambiguate(&DisambiguationRequest {
                mention: "Jordan".into(),
                normalized_type: "person".into(),
                context_snippet: "Jordan attended".into(),
                candidates: Vec::new(),
            })
            .await
            .unwrap_err();
        assert!(consulted.load(Ordering::Acquire));
        for code in ["canonical_epoch_changed", "canonical_inputs_changed"] {
            let error = execution_error(
                elephant_storage::StorageError::Conflict(code.into()).into(),
                consulted.load(Ordering::Acquire),
            );
            assert_eq!(
                crate::scheduler::execution_contract::classify_failure(&error),
                crate::scheduler::execution_contract::FailureClass::Transient
            );
        }
    }

    async fn candidate_fixture() -> (
        WorkArtifactRepository,
        SourcePublicationManifest,
        WorkArtifact,
    ) {
        let storage = StorageClient::new_embedded().await.unwrap();
        let repo = WorkArtifactRepository::new(storage);
        let source_id = RecordId::new_v4(TableName::Source);
        let data = serde_json::json!({"source_id": source_id.to_string(), "raw_metadata_patch": {"title": "Original accepted metadata"}});
        let candidate = WorkArtifact {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkArtifact),
                "candidate-space".into(),
                "fixture".into(),
            ),
            work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
            artifact_type: "source_metadata".into(),
            data: data.clone(),
            status: ArtifactStatus::Validated,
            validation_errors: None,
            validated_at: Some(Utc::now()),
            committed_at: None,
        };
        repo.create(candidate.clone()).await.unwrap();
        let publication = PublicationFence {
            source_id,
            generation: "accepted-generation".into(),
            revision_id: "revision".into(),
            operation_id: "operation".into(),
        };
        let mut bundle = candidate.clone();
        bundle.base.id = RecordId::new_v4(TableName::WorkArtifact);
        bundle.artifact_type = "commit_bundle".into();
        bundle.data = serde_json::json!({
            "publication": publication, "bundle_kind": "source_metadata", "source_artifact_id": candidate.base.id.to_string(),
            "source_artifact_type": "source_metadata", "payload": { "kind": "source_metadata", "data": data },
        });
        let manifest = SourcePublicationManifest {
            space_id: "candidate-space".into(),
            publication,
            mode: IngestMode::Metadata,
            configuration: SpaceConfigToken {
                space_id: "candidate-space".into(),
                config_id: RecordId::new_v4(TableName::SpaceConfig).to_string(),
                generation: 1,
                policy_id: "fixture".into(),
            },
            embedding_profile: None,
            documents: Vec::new(),
            metadata_artifact_ids: vec![bundle.base.id.clone()],
            planning_artifact_ids: Vec::new(),
            bundles: Vec::new(),
            embeddings: Vec::new(),
            embedding_work_item_ids: Vec::new(),
        };
        (repo, manifest, bundle)
    }

    #[tokio::test]
    async fn validated_candidate_cannot_be_replaced_by_changed_bundle_semantics() {
        let (repo, manifest, mut bundle) = candidate_fixture().await;
        checked_candidate(&repo, &manifest.space_id, &manifest.publication, &bundle)
            .await
            .unwrap();
        bundle.data["payload"]["data"]["raw_metadata_patch"]["title"] =
            serde_json::json!("Changed after validation");
        let error = checked_candidate(&repo, &manifest.space_id, &manifest.publication, &bundle)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("changed validated metadata"));
    }

    #[tokio::test]
    async fn validated_candidate_requires_exact_original_envelope() {
        let (repo, manifest, mut bundle) = candidate_fixture().await;
        bundle.base.security.level += 1;
        assert!(
            checked_candidate(&repo, &manifest.space_id, &manifest.publication, &bundle)
                .await
                .unwrap_err()
                .to_string()
                .contains("lineage")
        );
    }

    #[tokio::test]
    async fn source_publication_futures_fit_dispatcher_stack_budget() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let publication = PublicationFence {
            source_id: RecordId::new_v4(TableName::Source),
            generation: "size-only".into(),
            revision_id: "size-only".into(),
            operation_id: "size-only".into(),
        };
        let attempt = AttemptFence {
            space_id: "size-only".into(),
            work_item_id: RecordId::new_v4(TableName::WorkItem),
            generation: 1,
            lease_token: "size-only".into(),
        };
        let options = PromotionOptions::default();
        fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
            std::mem::size_of::<F>()
        }
        let artifact_id = RecordId::new_v4(TableName::WorkArtifact);
        // Inspect returned future TYPES without constructing or polling them.
        // This still reports a giant future instead of overflowing before its
        // caller could move it into Box::pin. No authority is exercised.
        for (phase, bytes) in [
            (
                "worker finalizer",
                future_bytes(|| {
                    execute_finalizer(
                        &storage,
                        "size-only",
                        &publication,
                        &attempt,
                        "size-oracle",
                        &options,
                    )
                }),
            ),
            (
                "finalizer",
                future_bytes(|| {
                    finalize_source_revision(
                        &storage,
                        "size-only",
                        &publication,
                        &attempt,
                        "size-oracle",
                        &options,
                    )
                }),
            ),
            (
                "registration",
                future_bytes(|| {
                    register_bundle(&storage, "size-only", &publication, &artifact_id, &attempt)
                }),
            ),
        ] {
            assert!(
                bytes <= 64 * 1024,
                "{phase} future uses {bytes} stack bytes"
            );
        }
    }

    #[tokio::test]
    async fn finalizer_cannot_use_another_spaces_attempt() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let publication = PublicationFence {
            source_id: RecordId::new_v4(TableName::Source),
            generation: "generation".into(),
            revision_id: "revision".into(),
            operation_id: "operation".into(),
        };
        let attempt = AttemptFence {
            space_id: "wrong-space".into(),
            work_item_id: RecordId::new_v4(TableName::WorkItem),
            generation: 1,
            lease_token: "captured-token".into(),
        };
        assert!(matches!(
            finalize_source_revision(
                &storage,
                "right-space",
                &publication,
                &attempt,
                "fixture",
                &PromotionOptions::default()
            )
            .await,
            Err(PipelineError::LeaseConflict(_))
        ));
    }
    #[test]
    fn normalized_metadata_aliases_cannot_replace_accepted_source_identity() {
        use elephant_types::records::source::{Source, SourceType};
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "named".into(),
                "consumer".into(),
            ),
            SourceType::Manual,
            "accepted-content-hash".into(),
        );
        source.external_id = Some("consumer-stable-id".into());
        let security = source.base.security.clone();
        for aliases in [vec!["derived-alias"], Vec::new()] {
            let metadata: crate::scheduler::ingest_work_kinds::SourceMetadataArtifactData =
                serde_json::from_value(serde_json::json!({
                    "source_id": source.base.id.to_string(), "normalized_external_ids": aliases,
                    "normalized_title": "Normalized title", "raw_metadata_patch": {},
                }))
                .unwrap();
            apply_source_metadata(&mut source, &metadata).unwrap();
            assert_eq!(source.external_id.as_deref(), Some("consumer-stable-id"));
            assert_eq!(source.hash, "accepted-content-hash");
            assert_eq!(source.type_, SourceType::Manual);
            assert_eq!(source.base.security, security);
            assert_eq!(
                source.metadata
                    [crate::scheduler::ingest_work_kinds::SOURCE_NORMALIZED_METADATA_KEY],
                serde_json::to_value(&metadata).unwrap()
            );
        }
    }
}
