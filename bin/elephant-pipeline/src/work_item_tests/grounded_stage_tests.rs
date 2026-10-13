//! Retained stage oracles use real acceptance, render/tree and queue authority.
//! The only semantic substitute is an exact known-answer support verifier.
use super::*;
use elephant_pipeline::extraction::{
    verify_assertion_support, AssertionSupportVerifier, AssertionValidator, ExtractionContext,
    ExtractionResult, SupportCandidate, SupportVerdict,
};
use elephant_storage::source_intake::{AcceptedSourceIntake, IntakeControl, PreparedSourceIntake};

#[allow(dead_code)]
#[path = "../../../../tests/recall_preview_semantics/provider_spy.rs"]
mod provider_spy;

const TEXT: &str = "Alice works at Acme Corp.";

/// Preserve the shared dispatcher's actual failed attempt and production
/// dependency retry. None is a deferred original stage, never a successful run.
pub(super) async fn dispatch_grounded_step(
    storage: &StorageClient,
    item: &WorkItem,
    extractor: &dyn Extractor,
) -> Option<RecordId> {
    let config = WorkQueueWorkerConfig {
        transient_retry_max_delay_secs: 30,
        ..WorkQueueWorkerConfig::default()
    };
    Box::pin(dispatch_grounded_step_with_config(
        storage, item, extractor, &config,
    ))
    .await
}

async fn dispatch_grounded_step_with_config(
    storage: &StorageClient,
    item: &WorkItem,
    extractor: &dyn Extractor,
    config: &WorkQueueWorkerConfig,
) -> Option<RecordId> {
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let result = Box::pin(dispatch_fixture_with_extractor(
        item,
        &run_id,
        storage.clone(),
        config,
        &TmContext::none(),
        Some(extractor),
    ))
    .await;
    let dispatch_finished = Utc::now();
    let Err(error) = result else {
        return Some(run_id);
    };
    assert_eq!(
        classify_failure(&error),
        FailureClass::PendingDependencies,
        "actual grounded stage failed: {error:#}"
    );
    let queued = WorkItemRepository::new(storage.clone())
        .get(&item.base.space_id, &item.base.id)
        .await
        .expect("original deferred work lookup")
        .expect("original deferred work retained");
    assert_eq!(queued.status, WorkItemStatus::Runnable);
    assert_eq!(
        queued.attempts, item.attempts,
        "dependency retry must refund attempt"
    );
    assert!(queued.leased_by.is_none() && queued.lease_expires_at.is_none());
    let run = WorkRunRepository::new(storage.clone())
        .get(&item.base.space_id, &run_id)
        .await
        .expect("original failed run lookup")
        .expect("original failed run retained");
    assert!(!run.success && run.error.is_some());
    assert_eq!(run.work_item.id(), &item.base.id);
    assert_eq!(run.attempt, item.attempts + 1);
    let completed_at = run.completed_at.expect("actual failed attempt completed");
    // Local dependency polling has a five-second interval; configuration
    // recovery retains its separate thirty-second floor. Check the independent
    // policy value against persisted timestamps without rewriting the due time.
    let dependency_interval = chrono::Duration::seconds(5);
    assert!(
        queued
            .run_after
            .is_some_and(|due| due >= completed_at + dependency_interval),
        "original dependency retry must retain production minimum delay"
    );
    assert!(
        queued
            .run_after
            .is_some_and(|due| due <= dispatch_finished + dependency_interval),
        "original dependency retry must use the local prerequisite interval"
    );
    let attempt: elephant_types::publication::AttemptFence =
        serde_json::from_value(run.metrics.custom["attempt_fence"].clone())
            .expect("original persisted attempt fence");
    assert_eq!(attempt.space_id, item.base.space_id);
    assert_eq!(attempt.work_item_id, item.base.id);
    assert!(!attempt.lease_token.is_empty() && attempt.generation > 0);
    // Claiming appends the real lease fence to the original input. Retain and
    // check that exact persisted attempt; do not erase runtime authority just
    // to compare against the pre-claim payload.
    let mut expected_payload = item.payload.clone();
    expected_payload
        .as_object_mut()
        .expect("original work payload object")
        .insert(
            "_attempt_fence".into(),
            serde_json::to_value(&attempt).expect("actual original attempt fence"),
        );
    assert_eq!(
        queued.payload, expected_payload,
        "dependency retry must retain original input and the actual claimed fence"
    );
    let publication = elephant_pipeline::source_queue::publication_from_work(item)
        .expect("original source authority")
        .expect("grounded work requires accepted source");
    let operation =
        elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
            .operation(&item.base.space_id, &publication.operation_id)
            .await
            .expect("original source operation lookup")
            .expect("original source operation retained");
    assert_eq!(operation.source_generation, publication.generation);
    assert_eq!(
        operation.desired_revision.as_deref(),
        Some(publication.revision_id.as_str())
    );
    assert_eq!(
        operation.state,
        elephant_types::publication::OperationState::Processing
    );
    assert!(
        operation.published_revision.is_none(),
        "deferred original source is not published"
    );
    None
}

/// Runnable queries omit delayed work. Inspect this fixture's complete bounded
/// queue and wait for its real due time without modifying stored scheduling.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn ready_grounded_items(
    storage: &StorageClient,
    space: &str,
    deadline: std::time::Instant,
) -> Vec<WorkItem> {
    let items = WorkItemRepository::new(storage.clone());
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "grounded publication deadline exceeded"
        );
        let ready = items
            .find_by_status(space, WorkItemStatus::Runnable)
            .await
            .expect("due original grounded work");
        if !ready.is_empty() {
            return ready;
        }
        let all = items
            .list(space, 2048, None)
            .await
            .expect("complete bounded fixture queue");
        assert!(all.len() < 2048, "fixture queue inspection bound exceeded");
        let due = all
            .iter()
            .filter(|item| item.status == WorkItemStatus::Runnable)
            .filter_map(|item| item.run_after)
            .min();
        let Some(due) = due else { return Vec::new() };
        let wait = (due - Utc::now()).to_std().unwrap_or_default();
        assert!(
            wait <= deadline.saturating_duration_since(std::time::Instant::now()),
            "original dependency retry exceeds fixture deadline"
        );
        tokio::time::sleep(wait).await;
    }
}

struct ExactSupport;
#[async_trait::async_trait]
impl AssertionSupportVerifier for ExactSupport {
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "Retain the original controlled extractor identity lifetime signature"
    )]
    fn identity(&self) -> &str {
        "worker-stage-known-answer-v1"
    }

    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == TEXT
                    && candidate.claim["subject"] == "Alice"
                    && candidate.claim["predicate"] == "works at"
                    && candidate.claim["object"] == "Acme Corp"
                    && candidate.claim["literal"].is_null()
                    && candidate.claim["subject_type"] == "person"
                    && candidate.claim["object_type"] == "org"
                    && candidate.claim["subject_entity"].is_null()
                    && candidate.claim["object_entity"].is_null()
                    && candidate.claim["event_time"].is_null()
                    && candidate.claim["event_precision"].is_null()
                    && candidate.claim["event_title"].is_null()
                    && candidate.claim["event_lineage"].is_null(),
            })
            .collect())
    }
}

struct GroundedExtractor(FixtureExtractor);
#[async_trait::async_trait]
impl Extractor for GroundedExtractor {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        self.0.manifest()
    }

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        let text = ctx.node_text.clone();
        let mut result = self.0.extract(ctx).await?;
        verify_assertion_support(&mut result.raw_assertions, &text, &ExactSupport).await?;
        Ok(result)
    }
}

pub(super) struct Fixture {
    pub(super) storage: StorageClient,
    pub(super) accepted: AcceptedSourceIntake,
    spy: provider_spy::Spy,
    pub(super) config: WorkQueueWorkerConfig,
}

impl Fixture {
    async fn new(space: &str) -> Self {
        Box::pin(Self::new_with_backend(space, false)).await
    }

    pub(super) async fn new_with_backend(space: &str, embedded: bool) -> Self {
        Box::pin(Self::new_with_text(space, embedded, TEXT)).await
    }

    pub(super) async fn new_with_text(space: &str, embedded: bool, text: &str) -> Self {
        Box::pin(Self::new_with_text_and_generation_endpoint(
            space, embedded, text, None,
        ))
        .await
    }

    pub(super) async fn new_with_text_and_generation_endpoint(
        space: &str,
        embedded: bool,
        text: &str,
        generation_endpoint: Option<&str>,
    ) -> Self {
        let storage = if embedded {
            StorageClient::new_embedded()
                .await
                .expect("embedded storage")
        } else {
            StorageClient::new_memory().await.expect("native storage")
        };
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("complete runtime schema");
        let spy = provider_spy::Spy::start()
            .await
            .expect("local dispatch guard");
        let configured = provider_spy::configure(
            &SpaceConfigRepository::new(storage.clone()),
            space,
            &spy.endpoint,
            8,
        )
        .await
        .expect("actual governed named space");
        if let Some(endpoint) = generation_endpoint {
            let approved = provider_spy::edit(
                &SpaceConfigRepository::new(storage.clone()),
                space,
                |processing| {
                    // Endpoint and model identities are immutable after approval.
                    // Approve new identities through the real replacement path.
                    let mut connection = processing
                        .connections
                        .get("generate")
                        .ok_or_else(|| anyhow::anyhow!("generation connection missing"))?
                        .clone();
                    connection.id = format!("{space}-repair-generate");
                    connection.endpoint = if connection.provider == "anthropic" {
                        endpoint
                            .strip_suffix("/v1")
                            .ok_or_else(|| anyhow::anyhow!("generation endpoint needs /v1"))?
                            .to_string()
                    } else {
                        endpoint.to_string()
                    };
                    let mut generation = processing
                        .profiles
                        .get(&format!("{space}-generation"))
                        .ok_or_else(|| anyhow::anyhow!("generation profile missing"))?
                        .clone();
                    let original_profile_id = generation.id.clone();
                    generation.id = format!("{space}-repair-generation");
                    generation.connection_id = connection.id.clone();
                    for binding in processing.bindings.values_mut() {
                        if binding.profile_id == original_profile_id {
                            binding.profile_id = generation.id.clone();
                        }
                    }
                    let mut support = generation.clone();
                    support.id = format!("{space}-support");
                    support.model = format!("{}-independent-support", support.model);
                    processing
                        .bindings
                        .get_mut(
                            &elephant_types::provider_binding::ModelOperation::SupportVerification,
                        )
                        .ok_or_else(|| anyhow::anyhow!("support binding missing"))?
                        .profile_id = support.id.clone();
                    anyhow::ensure!(
                        processing
                            .connections
                            .insert(connection.id.clone(), connection)
                            .is_none(),
                        "repair connection must have a fresh identity"
                    );
                    anyhow::ensure!(
                        processing
                            .profiles
                            .insert(generation.id.clone(), generation)
                            .is_none(),
                        "repair generation profile must have a fresh identity"
                    );
                    anyhow::ensure!(
                        processing
                            .profiles
                            .insert(support.id.clone(), support)
                            .is_none(),
                        "support profile must have a fresh identity"
                    );
                    Ok(())
                },
            )
            .await
            .expect("original distinct support approval before acceptance");
            let before = configured.processing.as_ref().expect("original approval");
            let after = approved.processing.as_ref().expect("replacement approval");
            assert_eq!(after.generation, before.generation + 1);
            assert_eq!(after.index_generation, before.index_generation);
            assert_eq!(after.pending_reindex, before.pending_reindex);
            assert_eq!(after.policy_id, before.policy_id);
            assert_eq!(
                after.active_embedding_profile_id,
                before.active_embedding_profile_id
            );
            for (id, connection) in &before.connections {
                assert_eq!(after.connections.get(id), Some(connection));
            }
            for (id, profile) in &before.profiles {
                assert_eq!(after.profiles.get(id), Some(profile));
            }
            for operation in provider_spy::EMBEDDING {
                assert_eq!(
                    after.bindings.get(&operation),
                    before.bindings.get(&operation)
                );
            }
            let generation = &after.profiles[&format!("{space}-repair-generation")];
            let support = &after.profiles[&format!("{space}-support")];
            let connection = &after.connections[&generation.connection_id];
            let expected_endpoint = if connection.provider == "anthropic" {
                endpoint.strip_suffix("/v1").expect("Anthropic endpoint")
            } else {
                endpoint
            };
            assert_eq!(connection.endpoint, expected_endpoint);
            assert_ne!(connection.id, "generate");
            assert_ne!(generation.id, support.id);
            assert_ne!(generation.model, support.model);
            assert_eq!(
                support.model,
                format!("{}-independent-support", generation.model)
            );
            assert_eq!(generation.connection_id, support.connection_id);
            for operation in provider_spy::GENERATION {
                let expected = if operation
                    == elephant_types::provider_binding::ModelOperation::SupportVerification
                {
                    &support.id
                } else {
                    &generation.id
                };
                assert_eq!(&after.bindings[&operation].profile_id, expected);
            }
        }
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "stage-oracle".into(),
            ),
            SourceType::Manual,
            AssertionValidator::hash_quote(text),
        );
        source.external_id = Some(format!("stage-oracle:{}", uuid::Uuid::new_v4()));
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Employment note".into(),
                text: text.into(),
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "stage-oracle".into(),
                metadata: serde_json::json!({}),
            })
            .expect("immutable source template");
        source.set_requested_ingest_mode(IngestMode::Graph);
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            source,
            IntakeControl {
                revision_id: Some("employment-v1".into()),
                idempotency_key: Some(format!("stage-oracle:{}", uuid::Uuid::new_v4())),
                expected_generation: None,
            },
        ))
        .await
        .expect("prepare source");
        let accepted = Box::pin(prepared.accept(&storage))
            .await
            .expect("accept source");
        elephant_pipeline::source_queue::enqueue_accepted_source(
            &storage,
            &accepted.source,
            &accepted.publication,
            "stage-oracle",
        )
        .await
        .expect("queue accepted source");
        Self {
            storage,
            accepted,
            spy,
            config: WorkQueueWorkerConfig {
                transient_retry_max_delay_secs: 30,
                ..WorkQueueWorkerConfig::default()
            },
        }
    }

    pub(super) fn space(&self) -> &str {
        &self.accepted.source.base.space_id
    }

    async fn dispatch(&self, item: &WorkItem) -> Option<RecordId> {
        let extractor = GroundedExtractor(FixtureExtractor::new());
        // No ambient deployment credentials or real provider calls are permitted.
        elephant_pipeline::provider_dispatch::with_dispatch_credentials(
            std::sync::Arc::new(provider_spy::SpyCredentials),
            Box::pin(dispatch_grounded_step_with_config(
                &self.storage,
                item,
                &extractor,
                &self.config,
            )),
        )
        .await
    }

    async fn run(&self, item: &WorkItem) -> RecordId {
        let run = Box::pin(self.dispatch(item))
            .await
            .expect("requested stage must complete");
        assert!(self.spy.requests().expect("captured calls").is_empty());
        run
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep original repository and fixture ownership through the complete control assertions"
    )]
    pub(super) async fn finish_publication(&self) {
        let items = WorkItemRepository::new(self.storage.clone());
        let mut complete = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        for _ in 0..100 {
            let runnable =
                Box::pin(ready_grounded_items(&self.storage, self.space(), deadline)).await;
            if runnable.is_empty() {
                for status in [
                    WorkItemStatus::Pending,
                    WorkItemStatus::Running,
                    WorkItemStatus::Failed,
                ] {
                    assert!(
                        items
                            .find_by_status(self.space(), status)
                            .await
                            .expect("remaining work")
                            .is_empty(),
                        "source publication left {status} work"
                    );
                }
                complete = true;
                break;
            }
            for item in runnable {
                let _completed_run = Box::pin(self.dispatch(&item)).await;
            }
        }
        assert!(complete, "actual publication exceeded finite fixture bound");
        let operation = elephant_storage::source_publication::SourcePublicationRepository::new(
            self.storage.clone(),
        )
        .operation(self.space(), &self.accepted.operation.operation_id)
        .await
        .expect("operation")
        .expect("original accepted operation");
        assert_eq!(
            operation.state,
            elephant_types::publication::OperationState::Completed
        );
        assert_eq!(
            operation.published_revision.as_deref(),
            Some(self.accepted.publication.revision_id.as_str())
        );
        let calls = self.spy.requests().expect("physical calls");
        assert!(
            !calls.is_empty(),
            "real publication requires embedding transport"
        );
        assert!(calls.iter().all(|call| call.path.ends_with("/embeddings")));
    }

    pub(super) async fn items(&self, kind: &str) -> Vec<WorkItem> {
        WorkItemRepository::new(self.storage.clone())
            .list(self.space(), 100, None)
            .await
            .expect("work items")
            .into_iter()
            .filter(|item| item.kind == kind && item.status == WorkItemStatus::Runnable)
            .collect()
    }

    pub(super) async fn advance_to(&self, kind: &str) -> WorkItem {
        for _ in 0..12 {
            let runnable = WorkItemRepository::new(self.storage.clone())
                .find_by_status(self.space(), WorkItemStatus::Runnable)
                .await
                .expect("runnable items");
            let matching: Vec<_> = runnable
                .iter()
                .filter(|item| item.kind == kind && item.status == WorkItemStatus::Runnable)
                .collect();
            if !matching.is_empty() {
                assert_eq!(matching.len(), 1, "one small-document stage expected");
                return matching[0].clone();
            }
            assert!(!runnable.is_empty(), "requested stage was never scheduled");
            for item in runnable {
                assert!(
                    matches!(
                        item.kind.as_str(),
                        TRIAGE_SOURCE_KIND
                            | INGEST_METADATA_KIND
                            | VALIDATE_ARTIFACTS_KIND
                            | COMMIT_ARTIFACTS_KIND
                            | UPGRADE_INGEST_MODE_KIND
                            | RENDER_DOC_KIND
                            | PLAN_EXTRACTION_KIND
                    ),
                    "unexpected predecessor {}",
                    item.kind
                );
                if matches!(
                    item.kind.as_str(),
                    VALIDATE_ARTIFACTS_KIND | COMMIT_ARTIFACTS_KIND
                ) {
                    let run = RecordId::from_str(
                        item.payload["work_run_id"].as_str().expect("metadata run"),
                    )
                    .expect("run ID");
                    let artifacts = WorkArtifactRepository::new(self.storage.clone())
                        .find_by_work_run(&run)
                        .await
                        .expect("metadata artifacts");
                    assert!(artifacts.iter().any(|artifact| artifact.artifact_type == SOURCE_METADATA_ARTIFACT_TYPE),
                        "only accepted-source metadata may precede the requested graph stage");
                    assert!(artifacts.iter().all(
                        |artifact| artifact.artifact_type != KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
                    ));
                }
                Box::pin(self.run(&item)).await;
            }
        }
        panic!("bounded accepted-source stage traversal exhausted");
    }

    async fn omit_source_hint(&self, item: WorkItem) -> WorkItem {
        assert_eq!(item.status, WorkItemStatus::Runnable);
        assert_eq!(
            item.attempts, 0,
            "source hint must be omitted before the first claim"
        );
        assert!(item.leased_by.is_none());
        let original_id = item.base.id.clone();
        let mut replacement = item.clone();
        replacement
            .payload
            .as_object_mut()
            .expect("object payload")
            .remove("source_id");
        replacement.idempotency_key =
            format!("{}:original-without-source-hint", item.idempotency_key);
        // This is a new original fixture intent. Preserve the real source,
        // configuration, predecessor and metadata authority before first create;
        // never rewrite a stored producer attestation or publication manifest.
        let replacement =
            elephant_pipeline::source_queue::enqueue_idempotent(&self.storage, replacement)
                .await
                .expect("enqueue original source-hint-free intent");
        assert_ne!(replacement.base.id, original_id);
        let mut retired = item;
        retired.status = WorkItemStatus::Cancelled;
        WorkItemRepository::new(self.storage.clone())
            .update_if_status(
                self.space(),
                &original_id,
                retired,
                WorkItemStatus::Runnable,
            )
            .await
            .expect("retire unclaimed alternative fixture intent")
            .expect("original fixture intent remained unclaimed");
        replacement
    }

    fn assert_source(&self, item: &WorkItem) {
        assert_eq!(
            item.payload["source_id"].as_str(),
            Some(self.accepted.source.base.id.to_string().as_str())
        );
        assert_eq!(
            elephant_pipeline::source_queue::publication_from_work(item)
                .expect("source fence")
                .as_ref(),
            Some(&self.accepted.publication)
        );
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn render_enqueues_plan() {
    let mut fixture = Box::pin(Fixture::new("render-stage-space")).await;
    fixture.config.doc_tree.create_paragraphs = false;
    let captured_recipe =
        serde_json::to_value(&fixture.config.doc_tree).expect("original constructed tree recipe");
    let render = fixture.advance_to(RENDER_DOC_KIND).await;
    fixture.run(&render).await;
    let plans = fixture.items(PLAN_EXTRACTION_KIND).await;
    assert_eq!(plans.len(), 1);
    let revision_id =
        RecordId::from_str(plans[0].payload["revision_id"].as_str().expect("revision"))
            .expect("revision ID");
    let revision = DocRevisionRepository::new(fixture.storage.clone())
        .get(fixture.space(), &revision_id)
        .await
        .expect("revision lookup")
        .expect("immutable renderer output");
    let rendered = format!("{TEXT}\n");
    assert_eq!(revision.text, rendered);
    assert_eq!(
        revision.text_hash,
        AssertionValidator::hash_quote(&rendered)
    );
    fixture.assert_source(&plans[0]);
    let nodes = DocNodeRepository::new(fixture.storage.clone())
        .find_by_revision(&revision_id)
        .await
        .expect("actual persisted tree");
    assert!(nodes
        .iter()
        .any(|node| node.role == elephant_types::records::NodeRole::Chunk));
    assert!(
        nodes
            .iter()
            .all(|node| node.role != elephant_types::records::NodeRole::Paragraph),
        "actual render dispatch must use the retained non-default tree recipe"
    );
    assert_eq!(
        serde_json::to_value(&fixture.config.doc_tree).expect("same original worker recipe"),
        captured_recipe
    );
}

pub(super) async fn plan_enqueues_extract(omit_source: bool) {
    let fixture = Box::pin(Fixture::new("plan-stage-space")).await;
    let plan = fixture.advance_to(PLAN_EXTRACTION_KIND).await;
    let plan = if omit_source {
        fixture.omit_source_hint(plan).await
    } else {
        plan
    };
    fixture.run(&plan).await;
    let extracts = fixture.items(EXTRACT_ASSERTIONS_KIND).await;
    assert_eq!(extracts.len(), 1);
    assert_eq!(
        extracts[0].payload["revision_id"],
        plan.payload["revision_id"]
    );
    let node_id = RecordId::from_str(extracts[0].payload["node_id"].as_str().expect("node"))
        .expect("node ID");
    let node = DocNodeRepository::new(fixture.storage.clone())
        .get(fixture.space(), &node_id)
        .await
        .expect("node lookup")
        .expect("real planned evidence node");
    assert_eq!(
        node.revision.id().to_string(),
        plan.payload["revision_id"].as_str().expect("revision")
    );
    assert_eq!(
        node.span,
        TextSpan::new(
            0,
            u32::try_from(format!("{TEXT}\n").len()).expect("bounded rendered text")
        )
    );
    fixture.assert_source(&extracts[0]);
}

async fn extracted(omit_source: bool) -> (Fixture, RecordId) {
    Box::pin(extracted_with_backend(omit_source, false)).await
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn extracted_with_backend(omit_source: bool, embedded: bool) -> (Fixture, RecordId) {
    let fixture = Box::pin(Fixture::new_with_backend("extract-stage-space", embedded)).await;
    let extract = fixture.advance_to(EXTRACT_ASSERTIONS_KIND).await;
    let extract = if omit_source {
        fixture.omit_source_hint(extract).await
    } else {
        extract
    };
    let events = SystemEventRepository::new(fixture.storage.clone());
    let before = events
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("outbox before");
    let run = fixture.run(&extract).await;
    let after = events
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("outbox after");
    assert_eq!(
        serde_json::to_value(after).expect("events"),
        serde_json::to_value(before).expect("events"),
        "extraction stages candidates without publishing graph events"
    );
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone())
        .find_by_work_run(&run)
        .await
        .expect("run artifacts");
    assert_eq!(artifacts.len(), 1);
    assert_eq!(
        artifacts[0].artifact_type,
        KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
    );
    // Producer validation precedes its success and the downstream bundle stage.
    assert_eq!(artifacts[0].status, ArtifactStatus::Validated);
    assert!(artifacts[0].validated_at.is_some());
    assert!(artifacts[0].validation_errors.is_none());
    let data: ExtractionArtifactData =
        serde_json::from_value(artifacts[0].data.clone()).expect("candidate data");
    assert_eq!(
        data.assertions.len(),
        1,
        "known supported claim remains staged"
    );
    assert_eq!(
        data.source_id,
        Some(fixture.accepted.source.base.id.to_string())
    );
    assert_eq!(
        data.revision_id,
        extract.payload["revision_id"].as_str().expect("revision")
    );
    let validates = fixture.items(VALIDATE_ARTIFACTS_KIND).await;
    assert_eq!(validates.len(), 1);
    fixture.assert_source(&validates[0]);
    assert_eq!(validates[0].payload["work_run_id"], run.to_string());
    (fixture, run)
}

pub(super) async fn extraction_stages_candidate(omit_source: bool) {
    extracted(omit_source).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn legacy_alias_bundles() {
    let (fixture, run) = extracted(false).await;
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone());
    let mut candidate = artifacts
        .find_by_work_run(&run)
        .await
        .expect("candidates")
        .remove(0);
    // Exercise the retained wire alias with identical valid evidence and support.
    candidate.artifact_type = LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE.into();
    artifacts
        .update(fixture.space(), &candidate.base.id.clone(), candidate)
        .await
        .expect("legacy alias");
    let validate = fixture.items(VALIDATE_ARTIFACTS_KIND).await.remove(0);
    fixture.run(&validate).await;
    let result = artifacts
        .find_by_work_run(&run)
        .await
        .expect("validated artifacts");
    let original = result
        .iter()
        .find(|artifact| artifact.artifact_type == LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE)
        .expect("retained alias");
    assert_eq!(original.status, ArtifactStatus::Validated);
    assert!(
        result
            .iter()
            .any(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE),
        "legacy assertion_batch artifacts should still produce commit bundles"
    );
}

pub(super) async fn validation_stages_before_canonical_writes() {
    let (fixture, run) = Box::pin(extracted(false)).await;
    let before = SystemEventRepository::new(fixture.storage.clone())
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("outbox before validation");
    let validate = fixture.items(VALIDATE_ARTIFACTS_KIND).await.remove(0);
    Box::pin(fixture.run(&validate)).await;
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone())
        .find_by_work_run(&run)
        .await
        .expect("validated artifacts");
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.artifact_type == KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE));
    let bundle = artifacts
        .iter()
        .find(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
        .expect("validated commit bundle");
    assert_eq!(bundle.status, ArtifactStatus::Validated);
    assert_eq!(fixture.items(COMMIT_ARTIFACTS_KIND).await.len(), 1);
    assert!(
        AssertionRepository::new(fixture.storage.clone())
            .list(fixture.space(), 10, None)
            .await
            .expect("canonical assertions")
            .is_empty(),
        "validation cannot publish canonical assertions"
    );
    assert!(
        RelRepository::new(fixture.storage.clone())
            .list(fixture.space(), 10, None)
            .await
            .expect("canonical relations")
            .is_empty(),
        "validation cannot publish relations"
    );
    let after = SystemEventRepository::new(fixture.storage.clone())
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("outbox after validation");
    assert_eq!(
        serde_json::to_value(after).expect("events"),
        serde_json::to_value(before).expect("events"),
        "validation cannot emit canonical publication events"
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn extract_to_truth_maintenance_chain() {
    use elephant_pipeline::truth_maintenance::{
        execute_tm_commit, execute_tm_detect_conflicts_from_outbox_events,
        RepositoryTruthMaintenanceStorage,
    };
    let (fixture, run) = Box::pin(extracted(false)).await;
    let validate = fixture.items(VALIDATE_ARTIFACTS_KIND).await.remove(0);
    Box::pin(fixture.run(&validate)).await;
    let commit = fixture.items(COMMIT_ARTIFACTS_KIND).await.remove(0);
    fixture.assert_source(&commit);
    Box::pin(fixture.run(&commit)).await;
    // Bundle registration is intentionally not source publication. Complete the
    // real embedding/finalizer tail before checking consumer-visible TM effects.
    Box::pin(fixture.finish_publication()).await;
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone())
        .find_by_work_run(&run)
        .await
        .expect("committed artifacts");
    assert!(artifacts.iter().any(
        |artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE
            && artifact.status == ArtifactStatus::Committed
    ));
    let assertions = AssertionRepository::new(fixture.storage.clone())
        .list(fixture.space(), 10, None)
        .await
        .expect("published assertions");
    assert_eq!(assertions.len(), 1);
    let events = SystemEventRepository::new(fixture.storage.clone())
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("canonical events");
    assert!(!events.is_empty(), "publication must notify consumers");
    let tm_storage = RepositoryTruthMaintenanceStorage::new(fixture.storage.clone());
    let detected = Box::pin(execute_tm_detect_conflicts_from_outbox_events(
        &events,
        &tm_storage,
    ))
    .await
    .expect("truth maintenance detection");
    assert!(detected.events_processed > 0);
    assert!(
        detected.conflict_group_ids.is_empty(),
        "one supported employment relation is unambiguous"
    );
    for commit in &detected.direct_commits {
        Box::pin(execute_tm_commit(
            &commit.resolution,
            &commit.input,
            fixture.space(),
            &tm_storage,
        ))
        .await
        .expect("actual truth maintenance commit");
    }
    if !detected.direct_commits.is_empty() {
        let slots = elephant_storage::TruthSlotRepository::new(fixture.storage.clone())
            .list(fixture.space(), 10, None)
            .await
            .expect("truth slots");
        assert!(
            !slots.is_empty(),
            "direct commits must persist their truth slots"
        );
    }
}

/// Exercise the actual validation dispatcher after an accepted, rendered and
/// successfully extracted source. The fixed verifier and loopback spy preserve
/// the existing stage oracle; this control does not measure extraction quality.
#[tokio::test]
async fn source_validation_rejection_and_replay_preserve_complete_grounding() {
    for embedded in [true, false] {
        for case in [
            "rejected",
            "mixed",
            "missing",
            "committed_candidate",
            "resume",
            "empty",
            "changed_claim",
            "changed_source",
            "conflicting_bundle",
        ] {
            Box::pin(source_validation_case(embedded, case)).await;
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn source_validation_case(embedded: bool, case: &str) {
    use elephant_pipeline::scheduler::staged_commit::validate_artifacts;
    use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;

    let (fixture, producer_run) = Box::pin(extracted_with_backend(false, embedded)).await;
    let artifacts = WorkArtifactRepository::new(fixture.storage.clone());
    let staged = StagedCommit::new(fixture.storage.clone());
    let candidate = artifacts
        .find_by_work_run(&producer_run)
        .await
        .expect("original producer artifacts")
        .remove(0);
    let candidate_id = candidate.base.id.clone();
    let validator = fixture.items(VALIDATE_ARTIFACTS_KIND).await.remove(0);
    let events = SystemEventRepository::new(fixture.storage.clone());
    let before_events = events
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("events before source validation");

    // Another producer's rejected artifact must neither poison this batch nor
    // disappear when its original candidate is validated or resumed.
    let mut unrelated = candidate.clone();
    unrelated.base.id = RecordId::new_v4(TableName::WorkArtifact);
    unrelated.work_run = Link::new(RecordId::new_v4(TableName::WorkRun));
    unrelated.status = ArtifactStatus::Rejected;
    unrelated.validation_errors = Some(vec!["unrelated rejection".into()]);
    let unrelated_id = unrelated.base.id.clone();
    let unrelated = artifacts
        .create(unrelated)
        .await
        .expect("unrelated artifact");

    match case {
        "rejected" | "mixed" => {
            if case == "mixed" {
                let mut supported = candidate.clone();
                supported.base.id = RecordId::new_v4(TableName::WorkArtifact);
                artifacts
                    .create(supported)
                    .await
                    .expect("supported sibling");
            }
            let mut unsupported = candidate.clone();
            // Inject a new unvalidated proposal at the retained validator seam.
            // The active producer now validates its own complete batch earlier.
            unsupported.status = ArtifactStatus::Draft;
            unsupported.validated_at = None;
            unsupported.validation_errors = None;
            unsupported.data["assertions"][0]
                .as_object_mut()
                .expect("assertion object")
                .remove("support_verification");
            artifacts
                .update(fixture.space(), &candidate_id, unsupported)
                .await
                .expect("stage unsupported interpretation");
        }
        "missing" => {
            artifacts
                .delete(fixture.space(), &candidate_id)
                .await
                .expect("delete candidate");
        }
        "committed_candidate" => {
            let mut committed = candidate.clone();
            committed.status = ArtifactStatus::Committed;
            committed.validated_at = Some(Utc::now());
            committed.committed_at = Some(Utc::now());
            artifacts
                .update(fixture.space(), &candidate_id, committed)
                .await
                .expect("unexpected candidate state");
        }
        "empty" => {
            let mut empty = candidate.clone();
            empty.data["assertions"] = serde_json::json!([]);
            artifacts
                .update(fixture.space(), &candidate_id, empty)
                .await
                .expect("empty extraction");
        }
        "resume" | "changed_claim" | "changed_source" => {
            let validated = Box::pin(validate_artifacts::execute(
                &staged,
                fixture.space(),
                &producer_run,
            ))
            .await
            .expect("durable validation before interrupted bundle staging");
            assert_eq!(validated.len(), 1);
            assert_eq!(validated[0].status, ArtifactStatus::Validated);
            assert_eq!(
                artifacts
                    .find_by_work_run(&producer_run)
                    .await
                    .expect("no bundle yet")
                    .len(),
                1
            );
            if case == "changed_claim" {
                let mut changed = validated[0].clone();
                changed.data["assertions"][0]["subject_text"] = serde_json::json!("Mallory");
                artifacts
                    .update(fixture.space(), &candidate_id, changed)
                    .await
                    .expect("changed stored semantics");
            } else if case == "changed_source" {
                let mut changed = SourceRepository::new(fixture.storage.clone())
                    .get(fixture.space(), &fixture.accepted.source.base.id)
                    .await
                    .expect("current source lookup")
                    .expect("current source");
                changed.base.security.level += 1;
                SourceRepository::new(fixture.storage.clone())
                    .update(fixture.space(), &changed.base.id.clone(), changed)
                    .await
                    .expect("changed source authorization");
            }
        }
        "conflicting_bundle" => {
            let manifest = SourcePublicationManifestRepository::new(fixture.storage.clone())
                .get(fixture.space(), &fixture.accepted.publication)
                .await
                .expect("original manifest")
                .expect("planned source");
            assert_eq!(manifest.bundles.len(), 1);
            let data: ExtractionArtifactData =
                serde_json::from_value(candidate.data.clone()).expect("candidate payload");
            let mut conflicting = candidate.clone();
            conflicting.base.id = manifest.bundles[0].artifact_id.clone();
            conflicting.base.security.level += 1;
            conflicting.artifact_type = COMMIT_BUNDLE_ARTIFACT_TYPE.into();
            conflicting.status = ArtifactStatus::Validated;
            conflicting.validated_at = Some(Utc::now());
            conflicting.data = serde_json::to_value(CommitBundleArtifactData {
                source_artifact_id: candidate_id.to_string(),
                source_artifact_type: KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE.into(),
                bundle_kind: CommitBundleKind::KnowledgeCandidates,
                payload: CommitBundlePayload::KnowledgeCandidates(data),
            })
            .expect("valid bundle shape with conflicting authorization");
            for key in [
                "publication",
                elephant_pipeline::source_queue::SOURCE_CONFIGURATION_KEY,
                elephant_pipeline::source_queue::SOURCE_PLAN_KEY,
            ] {
                conflicting.data[key] = candidate.data[key].clone();
            }
            artifacts
                .create(conflicting)
                .await
                .expect("conflicting prior bundle");
        }
        _ => unreachable!("finite source validation oracle"),
    }

    let validator_run = RecordId::new_v4(TableName::WorkRun);
    let extractor = GroundedExtractor(FixtureExtractor::new());
    let result = Box::pin(dispatch_fixture_with_extractor(
        &validator,
        &validator_run,
        fixture.storage.clone(),
        &fixture.config,
        &TmContext::none(),
        Some(&extractor),
    ))
    .await;
    let success = matches!(case, "resume" | "empty");
    assert_eq!(
        result.is_ok(),
        success,
        "backend embedded={embedded}, case={case}: {result:?}"
    );
    if let Err(error) = result {
        let class = classify_failure(&error);
        if case == "changed_source" {
            assert!(
                matches!(
                    class,
                    FailureClass::Terminal
                        | FailureClass::StaleInput
                        | FailureClass::ConfigurationBlocked
                ),
                "changed source must fail its current authority: {error:#}"
            );
        } else {
            assert_eq!(
                class,
                FailureClass::Terminal,
                "source validation must not become an endless dependency wait: {error:#}"
            );
        }
    }
    let completed = WorkRunRepository::new(fixture.storage.clone())
        .get(fixture.space(), &validator_run)
        .await
        .expect("validator run")
        .expect("durable validator outcome");
    assert_eq!(completed.success, success);
    assert!(completed.completed_at.is_some());
    let work = WorkItemRepository::new(fixture.storage.clone())
        .get(fixture.space(), &validator.base.id)
        .await
        .expect("validator work")
        .expect("retained work");
    assert_eq!(
        work.status,
        if success {
            WorkItemStatus::Done
        } else {
            WorkItemStatus::Failed
        }
    );
    assert_eq!(
        fixture.items(COMMIT_ARTIFACTS_KIND).await.len(),
        usize::from(success)
    );
    let current = artifacts
        .find_by_work_run(&producer_run)
        .await
        .expect("durable candidate outcomes");
    let bundles: Vec<_> = current
        .iter()
        .filter(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
        .collect();
    assert_eq!(
        bundles.len(),
        usize::from(success || case == "conflicting_bundle")
    );
    if case == "rejected" || case == "mixed" {
        let rejected = current
            .iter()
            .find(|artifact| artifact.base.id == candidate_id)
            .expect("rejected candidate retained");
        assert_eq!(rejected.status, ArtifactStatus::Rejected);
        assert_eq!(
            rejected.validation_errors.as_deref(),
            Some(["Assertion 0 is an unsupported interpretation".to_string()].as_slice())
        );
        assert!(
            Box::pin(validate_artifacts::execute_source(
                &staged,
                fixture.space(),
                &producer_run
            ))
            .await
            .is_err(),
            "a persisted rejection must remain a failure on replay"
        );
    }
    if success {
        assert_eq!(
            bundles[0].data["source_artifact_id"],
            candidate_id.to_string()
        );
        assert_eq!(bundles[0].base.security, candidate.base.security);
        assert_eq!(
            bundles[0].base.subjects_state,
            candidate.base.subjects_state
        );
        let replayed = Box::pin(validate_artifacts::execute_source(
            &staged,
            fixture.space(),
            &producer_run,
        ))
        .await
        .expect("repeat validation uses original candidate, excluding its generated bundle");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].base.id, candidate_id);
    }
    let retained = artifacts
        .get(fixture.space(), &unrelated_id)
        .await
        .expect("unrelated lookup")
        .expect("unrelated retained");
    assert_eq!(
        serde_json::to_value(retained).expect("retained artifact"),
        serde_json::to_value(unrelated).expect("original artifact")
    );
    assert!(AssertionRepository::new(fixture.storage.clone())
        .list(fixture.space(), 10, None)
        .await
        .expect("assertions")
        .is_empty());
    assert!(RelRepository::new(fixture.storage.clone())
        .list(fixture.space(), 10, None)
        .await
        .expect("relations")
        .is_empty());
    let after_events = events
        .get_after_seq(fixture.space(), 0, 100)
        .await
        .expect("events after validation");
    assert_eq!(
        serde_json::to_value(before_events).expect("before events"),
        serde_json::to_value(after_events).expect("after events")
    );
    assert!(fixture
        .spy
        .requests()
        .expect("provider call capture")
        .is_empty());
}
