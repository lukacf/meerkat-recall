//! Overlapping Graph pipelines through actual claims, dispatch and retry writes.
//! Fixed supported extraction and a loopback embedding spy; no paid model calls.
//! Raw lineage retains derived participants; typed security additionally includes
//! entity self-subjects and relation/assertion endpoints. Check both explicitly.
use super::source_contact_finalization_tests::{known_graph_extractor, provider_spy};
use super::source_finalizer_contention_tests::{dispatch_claimed, FinalizerOutcome};
use super::*;
use elephant_pipeline::extraction::{ExtractionContext, ExtractionResult};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_types::publication::{OperationState, PublicationFence};
use elephant_types::records::{HasSecurity, SubjectsState};
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const SPACE: &str = "overlapping-graph-progress";
const QUOTE: &str = "Aria works at Cedar.";
const FINALIZE: &str = elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND;

/// Diagnostic observations never schedule work or change the original outcome.
/// Keep fixed-size histories and safe typed codes, never payloads or lease tokens.
struct ProgressTrace {
    started: tokio::time::Instant,
    events: Mutex<VecDeque<serde_json::Value>>,
    active: Mutex<BTreeMap<String, serde_json::Value>>,
    driver_phase: Mutex<PhaseTiming>,
    last_completion: Mutex<serde_json::Value>,
    last_publication: Mutex<serde_json::Value>,
    total_events: AtomicUsize,
}

struct PhaseTiming {
    current: &'static str,
    changed_at: std::time::Duration,
    longest: &'static str,
    longest_duration: std::time::Duration,
}

impl ProgressTrace {
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            events: Mutex::new(VecDeque::new()),
            active: Mutex::new(BTreeMap::new()),
            driver_phase: Mutex::new(PhaseTiming {
                current: "starting",
                changed_at: std::time::Duration::ZERO,
                longest: "starting",
                longest_duration: std::time::Duration::ZERO,
            }),
            last_completion: Mutex::new(serde_json::Value::Null),
            last_publication: Mutex::new(serde_json::Value::Null),
            total_events: AtomicUsize::new(0),
        }
    }

    fn phase(&self, phase: &'static str) {
        let elapsed = self.started.elapsed();
        let mut timing = self.driver_phase.lock().unwrap();
        let duration = elapsed.saturating_sub(timing.changed_at);
        if duration > timing.longest_duration {
            timing.longest = timing.current;
            timing.longest_duration = duration;
        }
        timing.current = phase;
        timing.changed_at = elapsed;
    }

    fn work(&self, item: &WorkItem, phase: &'static str, details: serde_json::Value) {
        let mut event = serde_json::json!({
            "at_ms": self.started.elapsed().as_millis(),
            "id": item.base.id, "kind": item.kind,
            "attempt": item.attempts, "phase": phase,
        });
        event["details"] = details;
        if phase == "completed" {
            *self.last_completion.lock().unwrap() = event.clone();
            if item.kind == FINALIZE {
                *self.last_publication.lock().unwrap() = event.clone();
            }
        }
        let mut active = self.active.lock().unwrap();
        if matches!(
            phase,
            "completed" | "failed" | "requeued" | "bookkeeping_error"
        ) {
            active.remove(&item.base.id.to_string());
        } else {
            active.insert(item.base.id.to_string(), event.clone());
        }
        drop(active);
        let mut events = self.events.lock().unwrap();
        if events.len() == 64 {
            events.pop_front();
        }
        events.push_back(event);
        drop(events);
        self.total_events.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> serde_json::Value {
        let elapsed = self.started.elapsed();
        let driver = {
            let timing = self.driver_phase.lock().unwrap();
            serde_json::json!({
                "phase": timing.current, "since_ms": timing.changed_at.as_millis(),
                "duration_ms": elapsed.saturating_sub(timing.changed_at).as_millis(),
                "longest_finished_phase": timing.longest,
                "longest_finished_phase_ms": timing.longest_duration.as_millis(),
            })
        };
        serde_json::json!({
            "elapsed_ms": elapsed.as_millis(),
            "driver": driver,
            "last_work_completion": self.last_completion.lock().unwrap().clone(),
            "last_publication_completion": self.last_publication.lock().unwrap().clone(),
            "active": self.active.lock().unwrap().clone(),
            "recent_events": self.events.lock().unwrap().clone(),
            "total_events": self.total_events.load(Ordering::Relaxed),
            "recent_event_limit": 64,
        })
    }
}

fn safe_failure(error: &anyhow::Error) -> serde_json::Value {
    use elephant_pipeline::scheduler::execution_contract::WorkFailure;
    // The finalizer maps this exact local conflict to PendingDependencies.
    // Retain its identity without copying arbitrary error messages.
    let epoch_reprepare = error.chain().take(16).any(|cause| {
        cause.downcast_ref::<WorkFailure>().is_some_and(|failure| {
            failure.class == FailureClass::PendingDependencies
                && failure.message
                    == "Source publication epoch changed; reprepare from current canonical state"
        })
    });
    serde_json::json!({
        "class": classify_failure(error),
        "diagnostic": failure_diagnostic::FailureDiagnostic::from_error(error).as_json(),
        "epoch_reprepare": epoch_reprepare,
    })
}

/// A separate three-second read-only diagnostic budget starts only after the
/// existing progress bound fails. It cannot turn a timeout into a passing run.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn fail_progress(
    storage: &StorageClient,
    publications: &[PublicationFence],
    trace: &ProgressTrace,
    reason: &'static str,
    progress: serde_json::Value,
) -> ! {
    let mut stored = serde_json::json!({});
    let observed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let items = WorkItemRepository::new(storage.clone())
            .list(SPACE, 96, None)
            .await;
        stored["items"] = items.map_or_else(
            |_| serde_json::json!({"query_failed": true}),
            |items| serde_json::json!({
                "at_limit": items.len() == 96,
                "rows": items.iter().map(|item| serde_json::json!({
                    "id": item.base.id, "kind": item.kind, "status": item.status,
                    "attempts": item.attempts, "run_after": item.run_after,
                    "due": item.run_after.is_none_or(|after| after <= Utc::now()),
                    "leased": item.leased_by.is_some(), "lease_expires_at": item.lease_expires_at,
                    "publication": elephant_pipeline::source_queue::publication_from_work(item).ok().flatten(),
                })).collect::<Vec<_>>(),
            }),
        );
        let edges = elephant_storage::WorkEdgeRepository::new(storage.clone())
            .list(SPACE, 96, None)
            .await;
        stored["edges"] = edges.map_or_else(
            |_| serde_json::json!({"query_failed": true}),
            |edges| serde_json::json!({
                "at_limit": edges.len() == 96,
                "rows": edges.iter().map(|edge| serde_json::json!({
                    "from": edge.from.id(), "to": edge.to.id(), "type": edge.edge_type,
                })).collect::<Vec<_>>(),
            }),
        );
        let sources = SourcePublicationRepository::new(storage.clone());
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        let mut source_rows = Vec::new();
        for publication in publications.iter().take(3) {
            let head = sources.head(SPACE, &publication.source_id).await;
            let operation = sources.operation(SPACE, &publication.operation_id).await;
            let manifest = manifests.get(SPACE, publication).await;
            let completion = manifests.completion(SPACE, publication).await;
            source_rows.push(serde_json::json!({
                "publication": publication,
                "head": head.as_ref().ok(), "head_query_failed": head.is_err(),
                "operation": operation.as_ref().ok(), "operation_query_failed": operation.is_err(),
                "manifest": manifest.as_ref().ok().and_then(Option::as_ref).map(|value| {
                    serde_json::json!({"mode": value.mode, "bundles": value.bundles.len(),
                        "embeddings": value.embeddings.len(), "documents": value.documents.len(),
                        "metadata_artifacts": value.metadata_artifact_ids,
                        "planning_artifacts": value.planning_artifact_ids,
                        "embedding_work_items": value.embedding_work_item_ids})
                }),
                "manifest_query_failed": manifest.is_err(),
                "completion_present": completion.as_ref().is_ok_and(Option::is_some),
                "completion_query_failed": completion.is_err(),
            }));
            stored["sources"] = serde_json::json!(source_rows);
        }
        let runs = WorkRunRepository::new(storage.clone())
            .list(SPACE, 96, None)
            .await;
        stored["runs"] = runs.map_or_else(
            |_| serde_json::json!({"query_failed": true}),
            |runs| serde_json::json!({
                "at_limit": runs.len() == 96,
                "rows": runs.iter().map(|run| serde_json::json!({
                    "id": run.base.id, "item": run.work_item.id(), "attempt": run.attempt,
                    "started_at": run.started_at, "completed_at": run.completed_at,
                    "success": run.success, "wall_time_ms": run.metrics.wall_time_ms,
                    "failure_class": run.metrics.custom.get("failure_class").cloned()
                        .and_then(|value| serde_json::from_value::<FailureClass>(value).ok()),
                })).collect::<Vec<_>>(),
            }),
        );
    })
    .await;
    panic!(
        "{reason}; graph_progress_diagnostics={}",
        serde_json::json!({"progress": progress, "trace": trace.snapshot(),
            "stored": stored, "snapshot_timed_out": observed.is_err(), "row_limit_per_query": 96})
    );
}

struct CountedExtractor<T> {
    inner: T,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl<T: Extractor> Extractor for CountedExtractor<T> {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        self.inner.manifest()
    }

    async fn extract(&self, context: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.extract(context).await
    }
}

async fn accept_graph(storage: &StorageClient, index: usize) -> PublicationFence {
    // An agent-authored manual document uses the same admitted source pipeline;
    // this fixture exercises the typed intake seam, not HTTP authentication.
    let actor = if index == 2 {
        "later-agent-writer"
    } else {
        "seed-writer"
    };
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            actor.into(),
        ),
        SourceType::Manual,
        elephant_pipeline::extraction::AssertionValidator::hash_quote(QUOTE),
    );
    source.external_id = Some(format!("overlap-source-{index}"));
    // Separate authorized envelopes make unrelated corpus inputs deterministic
    // and avoid asking the spy to decide entity identity across sources.
    source.base.security.labels = vec![format!("source-{index}")];
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: format!("Overlapping Graph source {index}"),
            text: QUOTE.into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: format!("overlap-source-{index}"),
            metadata: serde_json::json!({}),
        })
        .unwrap();
    source.set_requested_ingest_mode(IngestMode::Graph);
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        storage,
        source,
        IntakeControl {
            revision_id: Some(format!("revision-{index}")),
            idempotency_key: Some(format!("original-acceptance-{index}")),
            expected_generation: None,
        },
    ))
    .await
    .unwrap();
    let accepted = Box::pin(prepared.accept(storage)).await.unwrap();
    assert_eq!(accepted.operation.state, OperationState::Accepted);
    Box::pin(elephant_pipeline::source_queue::enqueue_accepted_source(
        storage,
        &accepted.source,
        &accepted.publication,
        actor,
    ))
    .await
    .unwrap();
    accepted.publication
}

fn check_failure(item: &WorkItem, class: FailureClass, action: RetryAction, error: &anyhow::Error) {
    assert_eq!(class, classify_failure(error));
    assert!(
        matches!(
            (class, action),
            (
                FailureClass::PendingDependencies,
                RetryAction::Blocked { .. }
            ) | (FailureClass::Transient, RetryAction::Retry { .. })
        ),
        "unexpected {:?} attempt {}: {class:?}/{action:?}: {error:#}",
        item.kind,
        item.attempts
    );
}

async fn ready_to_prepare(storage: &StorageClient, item: &WorkItem) -> bool {
    let publication = elephant_pipeline::source_queue::publication_from_work(item)
        .unwrap()
        .unwrap();
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item).unwrap();
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let Ok(requirements) =
        Box::pin(manifests.requirements_for_attempt(SPACE, &publication, &attempt)).await
    else {
        // Fixture scheduling does not classify or forgive the failure.
        // The actual dispatcher below records its original disposition.
        return false;
    };
    Box::pin(manifests.check_ready(&requirements)).await.is_ok()
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep concurrent original claims, finite progress and receipt assertions in one oracle"
)]
#[allow(
    clippy::significant_drop_tightening,
    reason = "Keep fixture owners and the controlled provider alive through the final accounting assertions"
)]
#[expect(
    clippy::future_not_send,
    reason = "The fixture deliberately polls boxed_local dispatch futures on one task; it never spawns or transfers this driver"
)]
async fn overlapping_graph_progress(backend: provider_spy::Backend) {
    let storage = Box::pin(provider_spy::storage(backend)).await.unwrap();
    let spy = provider_spy::Spy::start().await.unwrap();
    Box::pin(provider_spy::configure(
        &SpaceConfigRepository::new(storage.clone()),
        SPACE,
        &spy.endpoint,
        8,
    ))
    .await
    .unwrap();
    let mut publications = vec![
        Box::pin(accept_graph(&storage, 0)).await,
        Box::pin(accept_graph(&storage, 1)).await,
    ];
    let extractor = Arc::new(CountedExtractor {
        inner: known_graph_extractor(),
        calls: AtomicUsize::new(0),
    });
    let items = WorkItemRepository::new(storage.clone());
    let queue = WorkQueue::with_defaults(storage.clone());
    let mut running = futures::stream::FuturesUnordered::new();
    let mut gated = BTreeSet::new();
    let arrived = Arc::new(AtomicUsize::new(0));
    let (release, gate) = tokio::sync::watch::channel(false);
    let mut completed = BTreeSet::new();
    let trace = Arc::new(ProgressTrace::new());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(150);
    let first_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
    let mut ticks = tokio::time::interval(std::time::Duration::from_millis(50));
    let mut claims = 0;

    macro_rules! progress_state {
        () => {
            serde_json::json!({
                "claims": claims, "completed": completed, "gated": gated,
                "arrived": arrived.load(Ordering::SeqCst), "running": running.len(),
                "accepted": publications.len(), "extractor_calls": extractor.calls.load(Ordering::SeqCst),
            })
        };
    }
    while completed.len() < 3 {
        if tokio::time::Instant::now() >= deadline {
            fail_progress(
                &storage,
                &publications,
                &trace,
                "all original Graph publications must finish",
                progress_state!(),
            )
            .await;
        }
        if completed.is_empty() && tokio::time::Instant::now() >= first_deadline {
            fail_progress(
                &storage,
                &publications,
                &trace,
                "first Graph publication must make bounded progress",
                progress_state!(),
            )
            .await;
        }
        // Reproduce the later agent write while BOTH original seed sources are
        // still pending. Both ready finalizers have reached the execution gate,
        // while all prior claims/stages ran normally and interleaved.
        if arrived.load(Ordering::SeqCst) == 2 && publications.len() == 2 {
            trace.phase("later_source_acceptance");
            assert!(completed.is_empty());
            for publication in &publications {
                assert!(SourcePublicationRepository::new(storage.clone())
                    .head(SPACE, &publication.source_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .published_revision
                    .is_none());
            }
            publications.push(Box::pin(accept_graph(&storage, 2)).await);
            release.send(true).unwrap();
        }
        // Real worker batch selection enforces run_after and original leases;
        // never force delayed work due or overwrite its attempt budget.
        trace.phase("claim_batch");
        let batch = tokio::time::timeout_at(
            deadline,
            queue.claim_batch_prefer_kind(EXTRACT_ASSERTIONS_KIND, 3, 3 - running.len(), |_| true),
        )
        .await;
        let batch = match batch {
            Ok(Ok(batch)) => batch,
            Ok(Err(error)) => {
                let mut state = progress_state!();
                state["claim_failure"] = safe_failure(&error.into());
                fail_progress(&storage, &publications, &trace, "Graph claim failed", state).await;
            }
            Err(_) => {
                fail_progress(
                    &storage,
                    &publications,
                    &trace,
                    "Graph claim deadline",
                    progress_state!(),
                )
                .await;
            }
        };
        for claimed in batch {
            claims += 1;
            assert!(claims <= 120, "bounded execution count");
            trace.work(&claimed, "claimed", serde_json::json!({
                "publication": elephant_pipeline::source_queue::publication_from_work(&claimed).ok().flatten(),
                "lease_expires_at": claimed.lease_expires_at,
            }));
            // Enroll both original finalizers before polling dispatch. A claim
            // can become ready after this loop examines it, so an early false
            // readiness result must not let it bypass the overlap barrier.
            let wait_for_overlap = if claimed.kind == FINALIZE
                && gated.len() < 2
                && gated.insert(claimed.base.id.clone())
            {
                Some(gate.clone())
            } else {
                None
            };
            let storage = storage.clone();
            let extractor = Arc::clone(&extractor);
            let arrived = Arc::clone(&arrived);
            let trace = Arc::clone(&trace);
            running.push(
                async move {
                    if let Some(mut gate) = wait_for_overlap {
                        trace.work(&claimed, "readiness_wait", serde_json::Value::Null);
                        // Keep the real claim and lease while prerequisite
                        // dispatches progress in the remaining driver slot.
                        // The unchanged outer deadline bounds this wait.
                        while !Box::pin(ready_to_prepare(&storage, &claimed)).await {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        trace.work(&claimed, "overlap_wait", serde_json::Value::Null);
                        arrived.fetch_add(1, Ordering::SeqCst);
                        if !*gate.borrow() {
                            gate.changed().await.unwrap();
                        }
                    }
                    trace.work(&claimed, "dispatch", serde_json::Value::Null);
                    let result = elephant_pipeline::provider_dispatch::with_dispatch_credentials(
                        Arc::new(provider_spy::SpyCredentials),
                        Box::pin(dispatch_claimed(
                            &storage,
                            &claimed,
                            Some(extractor.as_ref()),
                        )),
                    )
                    .await;
                    (claimed, result)
                }
                .boxed_local(),
            );
        }
        trace.phase("poll_running");
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => {
                fail_progress(&storage, &publications, &trace,
                    "Graph worker progress deadline", progress_state!()).await;
            },
            _ = ticks.tick() => {},
            Some((item, result)) = running.next(), if !running.is_empty() => {
                match result.unwrap_or_else(|error| {
                    trace.work(&item, "bookkeeping_error", safe_failure(&error));
                    panic!("original bookkeeping failed {}; diagnostics={}", item.base.id, trace.snapshot());
                }) {
                    FinalizerOutcome::Completed => {
                        trace.work(&item, "completed", serde_json::Value::Null);
                        if item.kind == FINALIZE {
                            assert!(completed.insert(item.base.id.clone()));
                        }
                    }
                    FinalizerOutcome::ExecutionFailed { class, action, error } => {
                        trace.work(&item, "failed", serde_json::json!({
                            "failure": safe_failure(&error), "action": format!("{action:?}"),
                        }));
                        check_failure(&item, class, action, &error);
                        trace.phase("read_requeued_item");
                        let queued = items.get(SPACE, &item.base.id).await.unwrap().unwrap();
                        trace.work(&item, "requeued", serde_json::json!({
                            "status": queued.status, "attempts": queued.attempts,
                            "run_after": queued.run_after, "leased": queued.leased_by.is_some(),
                            "lease_expires_at": queued.lease_expires_at,
                        }));
                        assert_eq!(queued.status, WorkItemStatus::Runnable);
                        assert_eq!(queued.attempts, item.attempts - u32::from(class == FailureClass::PendingDependencies));
                        assert!(queued.run_after.is_some());
                        assert!(queued.leased_by.is_none() && queued.lease_expires_at.is_none());
                    }
                }
            }
        }
    }
    assert_eq!(
        gated.len(),
        2,
        "the first two original finalizers actually overlap"
    );
    while let Some((_item, result)) = tokio::time::timeout_at(deadline, running.next())
        .await
        .unwrap()
    {
        assert!(
            matches!(result.unwrap(), FinalizerOutcome::Completed),
            "no unfinished execution may hide behind completion"
        );
    }
    assert_eq!(
        extractor.calls.load(Ordering::SeqCst),
        3,
        "publication retries cannot replay successful extraction"
    );
    verify_graph_receipts(&storage, &publications).await;
    let requests = spy.requests().unwrap();
    assert_eq!(
        requests.len(),
        3,
        "exactly one controlled embedding dispatch per short document"
    );
    assert!(requests
        .iter()
        .all(|request| request.path.ends_with("/embeddings")));
    assert!(
        requests.iter().all(|request| request.body["input"]
            .as_array()
            .is_some_and(|texts| texts.len() == 2)),
        "one paragraph and one chunk per immutable document"
    );
    for status in [
        WorkItemStatus::Failed,
        WorkItemStatus::Running,
        WorkItemStatus::Pending,
        WorkItemStatus::Runnable,
    ] {
        assert!(
            items
                .find_by_status(SPACE, status)
                .await
                .unwrap()
                .is_empty(),
            "unfinished work: {status}"
        );
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Check the original publication, graph effects and exact immutable evidence together"
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "Each support row must match both the source ID and that source publication's distinct revision ID"
)]
async fn verify_graph_receipts(storage: &StorageClient, publications: &[PublicationFence]) {
    let sources = SourcePublicationRepository::new(storage.clone());
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    let assertions = AssertionRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert_eq!(assertions.len(), 3);
    let entities = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let relationships = RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let events = EventRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert_eq!(
        entities.len(),
        6,
        "two entities per exact security partition"
    );
    assert_eq!(relationships.len(), 3);
    assert_eq!(events.len(), 3);
    for publication in publications {
        let operation = sources
            .operation(SPACE, &publication.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.state, OperationState::Completed);
        assert_eq!(operation.source_generation, publication.generation);
        assert_eq!(
            operation.published_revision.as_ref(),
            Some(&publication.revision_id)
        );
        let head = sources
            .head(SPACE, &publication.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.generation, publication.generation);
        assert_eq!(head.operation_id, publication.operation_id);
        assert_eq!(head.published_revision, head.desired_revision);
        let manifest = manifests.get(SPACE, publication).await.unwrap().unwrap();
        assert_eq!(manifest.mode, IngestMode::Graph);
        assert_eq!(manifest.bundles.len(), 1);
        assert_eq!(manifest.embeddings.len(), 2);
        let completion = manifests
            .completion(SPACE, publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&completion.publication, publication);
        let receipt = canonical
            .receipt(SPACE, &completion.commit_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.publication_authority.as_ref(), Some(publication));
        let source = SourceRepository::new(storage.clone())
            .get(SPACE, &publication.source_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.lifecycle_status,
            elephant_types::records::SourceLifecycleStatus::GraphReady
        );
        assert!(source.base.subjects.is_empty());
        assert_eq!(source.base.subjects_state, SubjectsState::Unknown);
        let source_entities = entities
            .iter()
            .filter(|row| row.base.provenance.sources.contains(&publication.source_id))
            .collect::<Vec<_>>();
        assert_eq!(source_entities.len(), 2);
        let aria = source_entities
            .iter()
            .find(|row| row.display_name == "Aria")
            .unwrap();
        let cedar = source_entities
            .iter()
            .find(|row| row.display_name == "Cedar")
            .unwrap();
        assert_ne!(aria.base.id, cedar.base.id);
        let source_assertions = assertions
            .iter()
            .filter(|row| row.base.provenance.sources.contains(&publication.source_id))
            .collect::<Vec<_>>();
        assert_eq!(source_assertions.len(), 1);
        let assertion = source_assertions[0];
        let source_relationships = relationships
            .iter()
            .filter(|row| row.base.provenance.sources.contains(&publication.source_id))
            .collect::<Vec<_>>();
        assert_eq!(source_relationships.len(), 1);
        let relationship = source_relationships[0];
        let source_events = events
            .iter()
            .filter(|row| row.base.provenance.sources.contains(&publication.source_id))
            .collect::<Vec<_>>();
        assert_eq!(source_events.len(), 1);
        let event = source_events[0];

        // Expected identities come from the fixed extraction's names within this
        // exact source partition, not from the endpoints being checked.
        assert_eq!(assertion.subject_text, "Aria");
        assert_eq!(assertion.object_text.as_deref(), Some("Cedar"));
        assert_eq!(assertion.predicate_id.as_deref(), Some("works_at"));
        assert_eq!(
            assertion.subject_entity.as_ref().map(Link::id),
            Some(&aria.base.id)
        );
        assert_eq!(
            assertion.object_entity.as_ref().map(Link::id),
            Some(&cedar.base.id)
        );
        assert_eq!(relationship.subject.id(), &aria.base.id);
        assert_eq!(relationship.object.id(), &cedar.base.id);
        assert_eq!(relationship.rel_type, "works_at");
        assert_eq!(relationship.evidence.len(), 1);
        assert_eq!(relationship.evidence[0].assertion.id(), &assertion.base.id);
        assert_eq!(relationship.evidence[0].span, assertion.evidence);
        assert_eq!(
            event.assertion_refs,
            vec![Link::new(assertion.base.id.clone())]
        );
        assert_eq!(
            event.source_refs,
            vec![Link::new(publication.source_id.clone())]
        );

        let aria_subjects = vec![Link::new(aria.base.id.clone())];
        let cedar_subjects = vec![Link::new(cedar.base.id.clone())];
        let mut participants = vec![aria_subjects[0].clone(), cedar_subjects[0].clone()];
        participants.sort_by_key(|subject| subject.id().to_string());
        for (base, expected_subjects) in [
            (&aria.base, &source.base.subjects),
            (&cedar.base, &source.base.subjects),
            (&assertion.base, &aria_subjects),
            (&relationship.base, &aria_subjects),
            (&event.base, &participants),
        ] {
            assert_eq!(base.security, source.base.security);
            assert_eq!(
                &base.subjects, expected_subjects,
                "raw subjects: {}",
                base.id
            );
            assert_eq!(base.subjects_state, source.base.subjects_state);
            assert_eq!(base.provenance.sources, vec![publication.source_id.clone()]);
            assert!(receipt.effect_ids.contains(&base.id));
            let support = sources.support_for_record(SPACE, &base.id).await.unwrap();
            assert!(support
                .iter()
                .any(|row| row.source_id == publication.source_id
                    && row.source_revision == publication.revision_id
                    && !row.independent));
        }
        // The stored source lineage and effective typed authorization are
        // different views. Preserve mandatory entity-self/endpoint subjects.
        assert_eq!(aria.subjects(), aria_subjects);
        assert_eq!(cedar.subjects(), cedar_subjects);
        for mut effective in [
            assertion.subjects(),
            relationship.subjects(),
            event.subjects(),
        ] {
            effective.sort_by_key(|subject| subject.id().to_string());
            assert_eq!(effective, participants);
        }
        assert_eq!(aria.subjects_state(), SubjectsState::Complete);
        assert_eq!(cedar.subjects_state(), SubjectsState::Complete);
        assert_eq!(relationship.subjects_state(), SubjectsState::Complete);
        assert_eq!(assertion.subjects_state(), SubjectsState::Unknown);
        assert_eq!(event.subjects_state(), SubjectsState::Unknown);
        assert_eq!(
            assertion.evidence.revision.id(),
            &manifest.bundles[0].revision_id
        );
        assert!(receipt.effect_ids.contains(&assertion.base.id));
        let evidence = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone())
            .evidence(SPACE, &assertion.evidence, &|_| true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(evidence.quote, QUOTE);
        let support = sources
            .support_for_record(SPACE, &assertion.base.id)
            .await
            .unwrap();
        assert!(support
            .iter()
            .any(|row| row.source_id == publication.source_id
                && row.source_revision == publication.revision_id
                && !row.independent));
    }
}

#[tokio::test]
async fn surreal_overlapping_graph_sources_and_later_agent_write_complete() {
    Box::pin(overlapping_graph_progress(
        provider_spy::Backend::SurrealMemory,
    ))
    .await;
}

#[tokio::test]
async fn embedded_overlapping_graph_sources_and_later_agent_write_complete() {
    Box::pin(overlapping_graph_progress(
        provider_spy::Backend::ElephantDb,
    ))
    .await;
}
