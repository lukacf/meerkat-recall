//! Real accepted metadata sources sharing one canonical epoch. The staged
//! work and finalizers use the active dispatcher and original attempt receipts;
//! the fixture has no provider bindings and invokes no model or embedding.

use super::*;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_types::publication::{OperationState, PublicationFence, SourcePrecondition};

pub(super) const SPACE: &str = "concurrent-finalizer-space";
pub(super) const ACTOR: &str = "concurrent-finalizer-oracle";
const COUNT: usize = 14;

pub(super) async fn accept_sources(
    storage: &StorageClient,
    count: usize,
    with_document: bool,
) -> Vec<PublicationFence> {
    ensure_fixture_space(storage, SPACE, ACTOR).await;
    let configuration = SpaceConfigRepository::new(storage.clone())
        .resolve_space(SPACE)
        .await
        .unwrap()
        .token;
    let sources = SourcePublicationRepository::new(storage.clone());
    let mut publications = Vec::new();
    for index in 0..count {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                SPACE.into(),
                ACTOR.into(),
            ),
            SourceType::Manual,
            format!("metadata-content-{index}"),
        );
        source.set_requested_ingest_mode(IngestMode::Metadata);
        if with_document {
            source
                .set_manual_doc_template(&ManualDocTemplate {
                    doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                    title: format!("Original document {index}"),
                    text: format!("Original accepted text {index}"),
                    sidecar_text: None,
                    media_attachments: Vec::new(),
                    rendition_key: format!("readiness-document-{index}"),
                    metadata: serde_json::json!({}),
                })
                .unwrap();
        }
        let accepted = sources
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "r1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: format!("accept-{index}"),
                request_fingerprint: format!("request-{index}"),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .unwrap();
        let publication = PublicationFence {
            source_id: accepted.source_id,
            generation: accepted.source_generation,
            revision_id: accepted.desired_revision.unwrap(),
            operation_id: accepted.operation_id,
        };
        if with_document {
            Box::pin(elephant_storage::source_intake::stage_source_revision(
                storage,
                &source,
                &publication,
            ))
            .await
            .unwrap();
        }
        elephant_pipeline::source_queue::enqueue_source_plan(
            storage,
            &source,
            &publication,
            &configuration,
            ACTOR,
        )
        .await
        .unwrap();
        publications.push(publication);
    }
    drop(sources);
    publications
}

async fn dispatch(storage: &StorageClient, item: &WorkItem) -> anyhow::Result<WorkRunMetrics> {
    let run = RecordId::new_v4(TableName::WorkRun);
    Box::pin(dispatch_fixture_with_extractor(
        item,
        &run,
        storage.clone(),
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        None,
    ))
    .await
}

#[derive(Debug)]
pub(super) enum FinalizerOutcome {
    Completed,
    ExecutionFailed {
        class: FailureClass,
        action: RetryAction,
        error: anyhow::Error,
    },
}

/// Use the worker's complete failure path for an already claimed attempt. The shared
/// stage fixture intentionally terminalizes errors other than dependencies;
/// that would bypass the finite transient retries needed for a commit-time CAS
/// conflict, which the finalizer adapter deliberately does not reclassify.
async fn dispatch_finalizer(
    storage: &StorageClient,
    claimed: &WorkItem,
) -> anyhow::Result<FinalizerOutcome> {
    Box::pin(dispatch_claimed(storage, claimed, None)).await
}

pub(super) async fn dispatch_claimed(
    storage: &StorageClient,
    claimed: &WorkItem,
    extractor: Option<&dyn Extractor>,
) -> anyhow::Result<FinalizerOutcome> {
    let space = claimed.base.space_id.as_str();
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(claimed)?;
    let run_id = create_work_run(storage, space, ACTOR, &claimed.base.id).await;
    let mut run = WorkRunRepository::new(storage.clone())
        .get(space, &run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("original run missing"))?;
    run.attempt = claimed.attempts;
    run.metrics
        .custom
        .insert("attempt_fence".into(), serde_json::to_value(&attempt)?);
    WorkRunRepository::new(storage.clone())
        .update(space, &run_id, run)
        .await?;
    let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
        storage.clone(),
        &run_id,
        attempt,
    );
    let config = WorkQueueWorkerConfig::default();
    let started = std::time::Instant::now();
    let result = storage
        .with_activity(
            elephant_pipeline::provider_dispatch::with_dispatch_observer(
                observer.clone(),
                Box::pin(super::super::execute_work_item_with_extractor(
                    claimed,
                    &run_id,
                    storage.clone(),
                    &config,
                    &TmContext::none(),
                    extractor,
                )),
            ),
        )
        .await?;
    let completed_at = Utc::now();
    let wall_time_ms = u64::try_from(started.elapsed().as_millis()).expect("bounded fixture time");
    let usage = observer.summarize(AttemptUsage::reserved(0, 0.0)).await?;
    drop(observer);
    match result {
        Ok(mut metrics) => {
            let mut run = WorkRunRepository::new(storage.clone())
                .get(space, &run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("original run disappeared"))?;
            for (key, value) in run.metrics.custom {
                metrics.custom.entry(key).or_insert(value);
            }
            usage.write_metrics(&mut metrics)?;
            metrics.wall_time_ms = wall_time_ms;
            run.metrics = metrics;
            run.success = true;
            run.completed_at = Some(completed_at);
            WorkRunRepository::new(storage.clone())
                .update(space, &run_id, run)
                .await?;
            elephant_pipeline::provider_usage::reconcile_finished_usage(storage, space, &run_id)
                .await?;
            let queue =
                WorkQueue::new(storage.clone(), WorkQueueConfig::default()).for_attempt(claimed)?;
            complete_work_item_and_promote(
                &queue,
                &DagExecutor::new(storage.clone()),
                space,
                &claimed.base.id,
            )
            .await?;
            Ok(FinalizerOutcome::Completed)
        }
        Err(error) => {
            let (class, action) = Box::pin(source_runtime::record_failed_execution(
                storage,
                claimed,
                &run_id,
                &error,
                &usage,
                completed_at,
                wall_time_ms,
                &config,
            ))
            .await?;
            Box::pin(source_runtime::apply_failed_disposition(
                storage, claimed, &error, class, action,
            ))
            .await?;
            Ok(FinalizerOutcome::ExecutionFailed {
                class,
                action,
                error,
            })
        }
    }
}

/// Close actual metadata producers while retaining their original finalizers.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
pub(super) async fn stage_finalizers(
    storage: &StorageClient,
    count: usize,
    with_document: bool,
) -> (Vec<PublicationFence>, Vec<WorkItem>) {
    let publications = Box::pin(accept_sources(storage, count, with_document)).await;
    let items = WorkItemRepository::new(storage.clone());
    // Close every original metadata bundle first. Only final publication is
    // concurrent, so the control cannot spend retries on missing predecessors.
    for _ in 0..count * 8 {
        let ready = items
            .find_by_status(SPACE, WorkItemStatus::Runnable)
            .await
            .unwrap();
        let stages: Vec<_> = ready
            .into_iter()
            .filter(|item| {
                item.kind != elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
            })
            .collect();
        if stages.is_empty() {
            break;
        }
        for item in stages {
            Box::pin(dispatch(storage, &item)).await.unwrap();
        }
    }
    let finalizers = items
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .unwrap();
    assert_eq!(finalizers.len(), count);
    assert!(finalizers
        .iter()
        .all(|item| item.kind
            == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND));
    (publications, finalizers)
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep accepted-source staging, concurrent execution and final receipt assertions together"
)]
async fn concurrent_publications(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        Box::pin(initialize_fixture_schema(&storage)).await;
        storage
    };
    let (publications, _) = Box::pin(stage_finalizers(&storage, COUNT, false)).await;
    let items = WorkItemRepository::new(storage.clone());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
    let mut completed = std::collections::BTreeSet::new();
    // Every round begins with all due finalizers together; completed source
    // publications provide the competing real epoch writes for their peers.
    // Scheduling is intentionally not asserted: the expected outcome is that
    // all accepted sources complete regardless of which publication wins.
    while completed.len() < COUNT {
        assert!(
            tokio::time::Instant::now() < deadline,
            "finalizers did not become quiescent"
        );
        let ready = items
            .find_by_status(SPACE, WorkItemStatus::Runnable)
            .await
            .unwrap();
        if ready.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        // The production queue acquires a batch serially before dispatching it.
        // Claim failures precede execution and cannot be counted as a consumed
        // execution attempt. Keep them outside the execution-error oracle.
        let mut claimed = Vec::with_capacity(ready.len());
        for item in ready {
            assert_eq!(
                item.kind,
                elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
            );
            let original = items
                .claim_with_lease(
                    SPACE,
                    &item.base.id,
                    ACTOR,
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("pre-execution claim failed for {}: {error:#}", item.base.id)
                })
                .expect("original finalizer must be runnable before execution");
            assert_eq!(original.attempts, item.attempts + 1);
            assert_eq!(original.status, WorkItemStatus::Running);
            elephant_storage::work_attempt_repository::fence_from_claim(&original)
                .expect("original claimed authority before dispatch");
            claimed.push(original);
        }
        let results = tokio::time::timeout_at(
            deadline,
            futures::future::join_all(
                claimed
                    .iter()
                    .map(|item| dispatch_finalizer(&storage, item)),
            ),
        )
        .await
        .expect("bounded concurrent publication");
        for (item, result) in claimed.iter().zip(results) {
            let outcome = result.unwrap_or_else(|error| {
                panic!(
                    "attempt bookkeeping failed for {} at claimed attempt {}: {error:#}",
                    item.base.id, item.attempts
                )
            });
            match outcome {
                FinalizerOutcome::Completed => {
                    assert!(completed.insert(item.base.id.clone()));
                }
                FinalizerOutcome::ExecutionFailed {
                    class,
                    action,
                    error,
                } => {
                    assert_eq!(class, classify_failure(&error));
                    assert!(
                        matches!(
                            class,
                            FailureClass::PendingDependencies | FailureClass::Transient
                        ),
                        "unexpected finalizer failure: {error:#}"
                    );
                    assert!(
                        matches!(
                            (class, action),
                            (FailureClass::PendingDependencies, RetryAction::Blocked { .. })
                                | (FailureClass::Transient, RetryAction::Retry { .. })
                        ),
                        "unexpected disposition {class:?}/{action:?} at claimed attempt {}: {error:#}",
                        item.attempts
                    );
                    let queued = items.get(SPACE, &item.base.id).await.unwrap().unwrap();
                    assert_eq!(
                        queued.status,
                        WorkItemStatus::Runnable,
                        "finalizer exhausted its preserved retry budget: {error:#}"
                    );
                    assert_eq!(
                        queued.attempts,
                        item.attempts - u32::from(class == FailureClass::PendingDependencies),
                        "execution budget mismatch for {} at claimed attempt {}, {class:?}/{action:?}: {error:#}",
                        item.base.id,
                        item.attempts
                    );
                    assert!(queued.run_after.is_some());
                    assert!(queued.leased_by.is_none() && queued.lease_expires_at.is_none());
                }
            }
        }
    }
    let sources = SourcePublicationRepository::new(storage.clone());
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone());
    drop(storage);
    for publication in publications {
        let operation = sources
            .operation(SPACE, &publication.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.state, OperationState::Completed);
        assert_eq!(operation.published_revision.as_deref(), Some("r1"));
        assert_eq!(operation.source_generation, publication.generation);
        let completion = manifests
            .completion(SPACE, &publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completion.publication, publication);
        let receipt = canonical
            .receipt(SPACE, &completion.commit_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.publication_authority.as_ref(), Some(&publication));
    }
    drop(sources);
    drop(manifests);
    drop(canonical);
    assert!(items
        .find_by_status(SPACE, WorkItemStatus::Failed)
        .await
        .unwrap()
        .is_empty());
    assert!(items
        .find_by_status(SPACE, WorkItemStatus::Running)
        .await
        .unwrap()
        .is_empty());
    // Model-free fixture: no extraction work is regenerated by publication
    // retries, and every original work kind remains part of the metadata chain.
    let all = items.list(SPACE, COUNT * 16, None).await.unwrap();
    drop(items);
    assert!(all.len() < COUNT * 16);
    assert!(!all.iter().any(|item| item.kind == EXTRACT_ASSERTIONS_KIND));
}

#[tokio::test]
async fn embedded_fourteen_same_space_source_finalizers_complete() {
    Box::pin(concurrent_publications(true)).await;
}

#[tokio::test]
async fn surreal_fourteen_same_space_source_finalizers_complete() {
    Box::pin(concurrent_publications(false)).await;
}
