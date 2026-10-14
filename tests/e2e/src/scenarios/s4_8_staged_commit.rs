#![allow(clippy::expect_used, clippy::panic)]
//! S4.8: actual accepted-source staging, rejection, retry and publication.

use elephant_pipeline::scheduler::extraction_work_kinds::{
    EXTRACT_ASSERTIONS_KIND, KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
};
use elephant_pipeline::scheduler::staged_commit::{
    validate_artifacts, StagedCommit, COMMIT_BUNDLE_ARTIFACT_TYPE,
};
use elephant_storage::{
    AssertionRepository, Repository, SystemEventRepository, WorkArtifactRepository,
    WorkItemRepository,
};
use elephant_types::publication::OperationState;
use elephant_types::records::work::{ArtifactStatus, WorkItem, WorkItemStatus};
use elephant_types::RecordId;

use crate::grounded_source_fixture::{EmploymentExtractor, GroundedSourceFixture};
use crate::Harness;

async fn runnable_for_run(
    items: &WorkItemRepository,
    space: &str,
    kind: &str,
    run: &RecordId,
) -> anyhow::Result<WorkItem> {
    let matches = items
        .find_by_status(space, WorkItemStatus::Runnable)
        .await?
        .into_iter()
        .filter(|item| item.kind == kind && item.payload["work_run_id"] == run.to_string())
        .collect::<Vec<_>>();
    anyhow::ensure!(matches.len() == 1, "expected one {kind} for original run");
    Ok(matches[0].clone())
}

#[tokio::test]
async fn scenario_s4_8_staged_commit() -> anyhow::Result<()> {
    let harness = Box::pin(Harness::new()).await?;
    let storage = &harness.client;
    let space = "Space_StagedCommit";
    let fixture = GroundedSourceFixture::new(storage, space).await?;
    let extractor = EmploymentExtractor::new();
    let accepted =
        Box::pin(fixture.accept_text(storage, "Supported employment", "Alice works at Acme Corp."))
            .await?;
    let items = WorkItemRepository::new(storage.clone());
    let artifacts = WorkArtifactRepository::new(storage.clone());
    let assertions = AssertionRepository::new(storage.clone());
    let events = SystemEventRepository::new(storage.clone());

    // Run real accepted metadata, rendering and planning, then stop immediately
    // after the one tiny document's extraction. Every stage claims its own lease.
    let mut extracted_run = None;
    for _ in 0..20 {
        let runnable = items
            .find_by_status(space, WorkItemStatus::Runnable)
            .await?;
        anyhow::ensure!(!runnable.is_empty(), "extraction was never scheduled");
        if let Some(extract) = runnable
            .iter()
            .find(|item| item.kind == EXTRACT_ASSERTIONS_KIND)
        {
            extracted_run = Some(Box::pin(fixture.run_item(storage, &extractor, extract)).await?);
            break;
        }
        for item in runnable {
            Box::pin(fixture.run_item(storage, &extractor, &item)).await?;
        }
    }
    let run = extracted_run.expect("bounded source stages must reach extraction");
    let staged = artifacts.find_by_work_run(&run).await?;
    assert_eq!(staged.len(), 1);
    let candidate = staged[0].clone();
    assert_eq!(candidate.artifact_type, KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE);
    // Source producers validate their complete batch before reporting success.
    // The queued validation stage must still recheck it before bundling.
    assert_eq!(candidate.status, ArtifactStatus::Validated);
    assert!(candidate.validated_at.is_some());
    assert_eq!(
        candidate.data["assertions"]
            .as_array()
            .expect("claims")
            .len(),
        1
    );
    assert!(assertions.list(space, 10, None).await?.is_empty());
    let outbox_before = serde_json::to_value(events.get_after_seq(space, 0, 100).await?)?;

    // A prior validation stamp cannot authorize a subsequently damaged payload.
    let mut unsupported = candidate.clone();
    unsupported.data["assertions"][0]["support_verification"] = serde_json::Value::Null;
    artifacts
        .update(space, &candidate.base.id, unsupported.clone())
        .await?;
    let replay_error = Box::pin(validate_artifacts::execute(
        &StagedCommit::new(storage.clone()),
        space,
        &run,
    ))
    .await
    .expect_err("changed evidence cannot reuse prior validation");
    assert!(replay_error
        .to_string()
        .contains("Previously validated artifact no longer passes validation"));

    // Inject the same corruption before validation by removing its old stamp.
    // The real evidence validator must reject it and retain the damaged payload.
    // No test marks a candidate validated, rejected or committed.
    unsupported.status = ArtifactStatus::Draft;
    unsupported.validated_at = None;
    let rejected_data = unsupported.data.clone();
    artifacts
        .update(space, &candidate.base.id, unsupported)
        .await?;
    let results = Box::pin(validate_artifacts::execute(
        &StagedCommit::new(storage.clone()),
        space,
        &run,
    ))
    .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ArtifactStatus::Rejected);
    assert_eq!(results[0].data, rejected_data);
    assert!(results[0]
        .validation_errors
        .as_ref()
        .expect("rejection reason")
        .iter()
        .any(|error| error.contains("unsupported interpretation")));
    assert!(assertions.list(space, 10, None).await?.is_empty());
    assert_eq!(
        serde_json::to_value(events.get_after_seq(space, 0, 100).await?)?,
        outbox_before
    );
    assert!(artifacts
        .find_by_work_run(&run)
        .await?
        .iter()
        .all(|artifact| artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE));

    // Repair only the test's injected staged corruption using the unchanged
    // original supported extraction. Normal queued validation, bundle creation,
    // registration and finalization still execute; no canonical row is seeded.
    let mut retry = candidate.clone();
    retry.status = ArtifactStatus::Draft;
    retry.validated_at = None;
    artifacts
        .update_if_status(space, &candidate.base.id, retry, ArtifactStatus::Rejected)
        .await?
        .expect("rejected candidate retained for retry");
    let validate = runnable_for_run(&items, space, "validate_artifacts", &run).await?;
    Box::pin(fixture.run_item(storage, &extractor, &validate)).await?;
    let validated = artifacts.find_by_work_run(&run).await?;
    let revalidated = validated
        .iter()
        .find(|artifact| artifact.base.id == candidate.base.id)
        .expect("original candidate revalidated by queued work");
    assert_eq!(revalidated.status, ArtifactStatus::Validated);
    assert!(revalidated.validated_at.is_some());
    assert_eq!(revalidated.data, candidate.data);
    let bundle = validated
        .iter()
        .find(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
        .expect("actual validated bundle");
    assert_eq!(bundle.status, ArtifactStatus::Validated);
    assert!(assertions.list(space, 10, None).await?.is_empty());
    let commit = runnable_for_run(&items, space, "commit_artifacts", &run).await?;
    Box::pin(fixture.run_item(storage, &extractor, &commit)).await?;
    assert!(
        assertions.list(space, 10, None).await?.is_empty(),
        "registration alone cannot expose an incomplete source"
    );

    Box::pin(fixture.drive(storage, &extractor)).await?;
    let operation =
        elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
            .operation(space, &accepted.operation.operation_id)
            .await?
            .expect("original operation");
    assert_eq!(operation.state, OperationState::Completed);
    assert_eq!(
        operation.published_revision.as_deref(),
        Some(accepted.publication.revision_id.as_str())
    );
    let published = assertions.list(space, 10, None).await?;
    assert_eq!(
        published.len(),
        1,
        "retry publishes one supported claim exactly once"
    );
    let evidence = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone())
        .evidence(space, &published[0].evidence, &|_| true)
        .await?
        .expect("current exact source evidence");
    assert_eq!(evidence.quote, "Alice works at Acme Corp.");
    let terminal = artifacts.find_by_work_run(&run).await?;
    assert_eq!(terminal.len(), 2);
    assert!(
        terminal
            .iter()
            .all(|artifact| artifact.status == ArtifactStatus::Committed
                && artifact.committed_at.is_some()),
        "successful publication retires staged work durably"
    );
    assert_ne!(
        serde_json::to_value(events.get_after_seq(space, 0, 100).await?)?,
        outbox_before
    );
    Ok(())
}
