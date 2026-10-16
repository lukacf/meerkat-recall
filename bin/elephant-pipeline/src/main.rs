#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Elephant Pipeline Worker
//!
//! Background worker for document embedding generation, truth maintenance, and the work queue scheduler.
//!
//! This binary runs worker loops:
//! - **`EmbedWorker`**: Polls for nodes without embeddings and generates them
//! - **Work queue**: Processes DAG-scheduled work items (render, plan, extract, truth maintenance)
//! - **Truth maintenance**: Detects conflicts from outbox events and resolves truth slots

mod attempt_settlement;
mod metadata_bundle_replay;
#[path = "source_runtime.rs"]
mod source_runtime;

use std::env;
use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

mod pipeline_shutdown;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use elephant_embeddings::EmbeddingProviderCache;
use elephant_pipeline::provider_dispatch::ProviderDispatchContext;
use elephant_pipeline::renderer::default::{DefaultTextRenderer, PassthroughRenderer};
use elephant_pipeline::renderer::multimodal::MultimodalRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::execution_contract::{
    classify_failure, retry_action, AttemptUsage, FailureClass, RetryAction, WorkFailure,
};
use elephant_pipeline::scheduler::extraction_work_kinds::{
    ExtractionArtifactData, KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
    LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE,
};
use elephant_pipeline::scheduler::ingest_work_kinds::{
    SourceMetadataArtifactData, SOURCE_METADATA_ARTIFACT_TYPE, SOURCE_NORMALIZED_METADATA_KEY,
};
use elephant_pipeline::scheduler::staged_commit::{
    CommitBundleArtifactData, CommitBundleKind, CommitBundlePayload, COMMIT_BUNDLE_ARTIFACT_TYPE,
};
use elephant_pipeline::scheduler::work_queue::WorkQueueConfig;
use elephant_pipeline::source_queue::{enqueue_idempotent, inherit_source_context};
use elephant_pipeline::{
    error::PipelineError,
    extraction::Extractor,
    freshness::{execute_freshness_sweep, WORK_KIND_FRESHNESS_SWEEP},
    reprocess::{
        execute_reprocess_job_with_attempt, ReprocessConfig, ReprocessExecutionContext,
        WORK_KIND_EXECUTE_REPROCESS_JOB,
    },
    resolution::{
        commit_resolution_patches, validate_resolution_patches, BackoffConfig,
        ResolutionPatchWorkflow,
    },
    scheduler::DagExecutor,
    scheduler::{
        execute_ingest_metadata, execute_ingest_source_stub, execute_plan_extraction,
        execute_triage_source, execute_upgrade_ingest_mode, promote_assertions_inline_with_options,
        BudgetEnforcer, DailyCapCheck, DefaultTriagePlugin, ExtractionPlanConfig, StagedCommit,
        WorkQueue, COMMIT_ARTIFACTS_KIND, EXTRACT_ASSERTIONS_KIND, INGEST_METADATA_KIND,
        INGEST_SOURCE_STUB_KIND, PLAN_EXTRACTION_KIND, RENDER_DOC_KIND, TRIAGE_SOURCE_KIND,
        UPGRADE_INGEST_MODE_KIND, VALIDATE_ARTIFACTS_KIND,
    },
    source_watch::{enqueue_source_watch_refresh, SourceWatchService, REFRESH_SOURCE_WATCH_KIND},
    truth_maintenance::{
        execute_tm_commit, execute_tm_detect_conflicts_from_outbox_events, execute_tm_recompute,
        resolve_conflict_group_with_storage, CommitInput, DetectConflictsResult, RecomputePriority,
        RepositoryTruthMaintenanceStorage, ResolutionResult, TmContext, TM_COMMIT_KIND,
        TM_DETECT_CONFLICTS_KIND, TM_RECOMPUTE_KIND, TM_RESOLVE_SLOT_KIND,
    },
    wiki_editorial::{
        enqueue_managed_wiki_graph_change, enqueue_managed_wiki_successor, ManagedWikiPayload,
        RECONCILE_LIVING_WIKI_KIND,
    },
    EmbedWorker,
};
use elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository;
use elephant_storage::Repository;
use elephant_storage::{
    AssertionRepository, ConflictGroupRepository, DocNodeRepository, DocRepository,
    DocRevisionRepository, FreshnessRepository, IngestDecisionRepository, KnowledgeViewRepository,
    ReprocessJobRepository, SourceRepository, SourceWatchRepository, SpaceConfigRepository,
    StorageBackendKind, StorageClient, StorageConfig, StorageError, SystemEventRepository,
    WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{Budget, DowngradePolicy, IngestMode};
use elephant_types::records::source::{Source, SourceLifecycleStatus};
use elephant_types::records::work::{
    ArtifactStatus, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::{Doc, SystemEvent};
use elephant_types::{Link, RecordId};

pub mod config;

// These are cumulative process totals, not usage attributed to this work item.
fn record_summary_process_totals(metrics: &mut WorkRunMetrics) {
    metrics.custom.insert(
        "summary_process_totals".to_owned(),
        serde_json::json!({
            "scope": "process",
            "totals": elephant_pipeline::summarizer::summary_metrics_snapshot(),
        }),
    );
}

fn validate_backend_env() -> anyhow::Result<()> {
    if let Ok(value) = env::var("ELEPHANT_DB") {
        value
            .parse::<StorageBackendKind>()
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("Invalid ELEPHANT_DB value `{value}`: {err}"))?;
    }
    Ok(())
}

use config::{
    EmbedWorkerConfig, PipelineConfig, TruthMaintenanceWorkerConfig, WorkQueueWorkerConfig,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[cfg(feature = "meerkat-extraction")]
use elephant_pipeline::extraction::{AgenticExtractor, AgenticExtractorConfig};
#[cfg(feature = "meerkat-extraction")]
use elephant_pipeline::wiki_editorial::execute_managed_wiki_reconciliation_with_attempt;

const AGENTIC_EXTRACTOR_ID: &str = "elephant.extractor.agentic";
const MANAGED_WIKI_TIMEOUT_SECS: u64 = 4 * 60 * 60;
const MANAGED_WIKI_LEASE_GRACE_SECS: u64 = 60;

fn build_semantic_extractor(extractor_id: Option<&str>) -> anyhow::Result<Box<dyn Extractor>> {
    let extractor_id = extractor_id.unwrap_or(AGENTIC_EXTRACTOR_ID);

    #[cfg(feature = "meerkat-extraction")]
    {
        if extractor_id == AGENTIC_EXTRACTOR_ID {
            let mcp_url = std::env::var("PIPELINE_AGENTIC_MCP_URL")
                .unwrap_or_else(|_| "http://localhost:3000/mcp/agent".to_string());
            let model = std::env::var("PIPELINE_AGENTIC_MODEL")
                .unwrap_or_else(|_| "gpt-6.1-sol".to_string());
            return Ok(Box::new(AgenticExtractor::new(
                AgenticExtractorConfig::new(mcp_url, model),
            )));
        }
        Err(anyhow::anyhow!(
            "Unknown extractor_id for extract_assertions: {extractor_id}"
        ))
    }

    #[cfg(not(feature = "meerkat-extraction"))]
    {
        let _ = extractor_id;
        Err(anyhow::anyhow!(
            "semantic_extractor_unavailable: elephant-pipeline was built without the `meerkat-extraction` feature"
        ))
    }
}

#[cfg(test)]
fn is_transient_work_error(error: &anyhow::Error) -> bool {
    matches!(
        classify_failure(error),
        FailureClass::Transient | FailureClass::Timeout
    )
}

#[cfg(test)]
fn compute_transient_retry_delay_secs(
    attempt: u32,
    base_delay_secs: i64,
    max_delay_secs: i64,
) -> i64 {
    let base = base_delay_secs.max(1);
    let max_delay = max_delay_secs.max(base);
    let exponent = attempt.saturating_sub(1).min(10);
    let factor = 1_i64 << exponent;
    base.saturating_mul(factor).min(max_delay)
}

#[cfg(test)]
mod work_item_tests {

    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        clippy::large_futures
    )]

    mod dispatch_wake_tests;
    mod governed_extraction_repair_tests;
    mod grounded_event_tests;
    mod grounded_stage_tests;
    mod renewal_render_tests;
    mod resolution_bundle_tests;
    // Compile the production policy/controller verbatim in this test binary.
    // No public testing API or copied retry implementation is introduced.
    mod metadata_bundle_replay_tests;
    #[path = "../../../../crates/storage/src/client_source_acceptance_retry.rs"]
    mod source_acceptance_retry_controls;
    mod source_admission_contention_tests;
    mod source_contact_finalization_tests;
    mod source_finalizer_contention_tests;
    mod source_finalizer_readiness_epoch_tests;
    mod source_graph_progress_tests;
    mod source_triage_decision_tests;
    mod source_watch_attempt_tests;
    mod source_watch_recovery_tests;
    mod source_watch_test_authority;

    use super::*;
    use chrono::Utc;
    use elephant_pipeline::resolution::apply_resolution_patch;
    use elephant_pipeline::truth_maintenance::{compute_slot_key, create_conflict_group};
    use elephant_storage::{
        AttributeRepository, ConflictGroupRepository, DocNodeRepository, DocRevisionRepository,
        EntityRepository, EventRepository, RelRepository, Repository, WorkArtifactRepository,
        WorkItemRepository, WorkRunRepository,
    };
    use elephant_types::records::assertion::{
        Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
    };
    use elephant_types::records::attribute::{Attribute, Literal};
    use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::records::entity::{Entity, EntityType};
    use elephant_types::records::event::TimePrecision;
    use elephant_types::records::rel::{ClaimStatus, EvidenceRef, TimeBasis};
    use elephant_types::records::source::{ManualDocTemplate, SourceType};
    use elephant_types::records::work::{WorkItemStatus, WorkRun, WorkRunMetrics};

    #[tokio::test]
    async fn dispatcher_future_storage_stays_bounded_on_default_stack() {
        // Inspect the actual future type without constructing or polling it.
        // This catches growth before constructing an oversized dispatcher could
        // itself overflow the default test/worker stack.
        fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
            std::mem::size_of::<F>()
        }
        let storage = StorageClient::new_embedded().await.expect("storage");
        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "size-only".into(),
                "fixture".into(),
            ),
            kind: TM_RECOMPUTE_KIND.into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "size-only".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let run = RecordId::new_v4(TableName::WorkRun);
        let config = WorkQueueWorkerConfig::default();
        let tm = TmContext::none();
        let bytes = future_bytes(|| {
            execute_work_item_with_extractor(&item, &run, storage, &config, &tm, None)
        });
        assert!(
            bytes <= 64 * 1024,
            "dispatcher future is {bytes} bytes; box the growing phase boundary"
        );
    }

    /// A fixed extractor response exercises orchestration without network calls.
    struct FixtureExtractor {
        manifest: elephant_types::records::work::PluginManifest,
    }

    #[async_trait::async_trait]
    impl elephant_pipeline::extraction::Extractor for FixtureExtractor {
        fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
            &self.manifest
        }

        async fn extract(
            &self,
            ctx: elephant_pipeline::extraction::ExtractionContext,
        ) -> Result<elephant_pipeline::extraction::ExtractionResult, elephant_pipeline::PipelineError>
        {
            use elephant_pipeline::extraction::{ExtractionResult, RawAssertion};
            use elephant_types::records::assertion::EvidenceSpan;
            use sha2::{Digest, Sha256};
            let quote = "Alice works at Acme Corp.";
            let start = ctx.node_text.find(quote).ok_or_else(|| {
                elephant_pipeline::PipelineError::ExtractionError(
                    "fixture text missing".to_string(),
                )
            })?;
            Ok(ExtractionResult::new(
                vec![RawAssertion {
                    support_verification: None,
                    subject_text: "Alice".to_string(),
                    predicate_text: "works at".to_string(),
                    object_text: Some("Acme Corp".to_string()),
                    object_literal: None,
                    evidence_span: EvidenceSpan {
                        revision: Link::new(ctx.revision_id),
                        node: Some(Link::new(ctx.node_id)),
                        start: u32::try_from(start).expect("small fixture"),
                        end: u32::try_from(start + quote.len()).expect("small fixture"),
                        quote_hash: format!("{:x}", Sha256::digest(quote.as_bytes())),
                    },
                    confidence: 0.99,
                    quote: Some(quote.to_string()),
                    source_event_time: None,
                    source_event_time_precision: None,
                    event_title: None,
                    event_lineage_key: None,
                    subject_entity_id: None,
                    subject_entity_type: Some("person".to_string()),
                    object_entity_id: None,
                    object_entity_type: Some("org".to_string()),
                    significance: Some("high".to_string()),
                }],
                0,
                "deterministic-fixture".to_string(),
            ))
        }
    }

    impl FixtureExtractor {
        fn new() -> Self {
            Self {
                manifest: elephant_pipeline::extraction::DefaultExtractor::new()
                    .manifest()
                    .clone(),
            }
        }
    }

    /// Native dispatcher tests use the same explicitly initialized schema as
    /// the service. Table declarations alone omit canonical fences and receipts.
    async fn initialize_fixture_schema(storage: &StorageClient) {
        Box::pin(elephant_migrations::apply_schema(storage))
            .await
            .expect("explicit complete fixture schema");
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn create_revision_with_node(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        text: &str,
    ) -> (RecordId, RecordId) {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let doc_id = RecordId::new_v4(TableName::Doc);
        DocRepository::new(storage.clone())
            .create(Doc {
                base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
                title: "Fixture evidence".into(),
                text: text.into(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: doc_id.to_string(),
                metadata: serde_json::json!({}),
            })
            .await
            .expect("physical fixture document");
        let revision = DocRevision {
            base: BaseFields::new(revision_id.clone(), space_id.to_string(), actor.to_string()),
            doc: Link::new(doc_id),
            text: text.to_string(),
            text_hash: elephant_pipeline::extraction::AssertionValidator::hash_quote(text),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
            manifest: None,
        };

        let revision_repo = DocRevisionRepository::new(storage.clone());
        revision_repo
            .create(revision)
            .await
            .expect("create revision");

        let node_id = RecordId::new_v4(TableName::DocNode);
        let span_end = u32::try_from(text.len()).unwrap_or(u32::MAX);
        let node = DocNode {
            base: BaseFields::new(node_id.clone(), space_id.to_string(), actor.to_string()),
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, span_end),
            node_key: format!("node:{node_id}"),
            text: None,
            parent: None,
            position: 0,
            depth: 1,

            embedding_model: None,
            collection: None,
        };

        let node_repo = DocNodeRepository::new(storage.clone());
        node_repo.create(node).await.expect("create node");

        (revision_id, node_id)
    }

    async fn create_source_backed_revision_with_node(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        text: &str,
    ) -> (RecordId, RecordId, RecordId) {
        let source = SourceRepository::new(storage.clone())
            .create(Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                SourceType::Drive,
                "source-backed-revision".to_string(),
            ))
            .await
            .expect("create source");

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                space_id.to_string(),
                actor.to_string(),
            ),
            title: "Source-backed doc".to_string(),
            text: text.to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source.base.id.clone())),
            rendition_key: "source-backed-rendition".to_string(),
            metadata: serde_json::json!({}),
        };
        let doc_id = doc.base.id.clone();
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("create source-backed doc");

        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let revision = DocRevision {
            base: BaseFields::new(revision_id.clone(), space_id.to_string(), actor.to_string()),
            doc: Link::new(doc_id),
            text: text.to_string(),
            text_hash: elephant_pipeline::extraction::AssertionValidator::hash_quote(text),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
            manifest: None,
        };
        DocRevisionRepository::new(storage.clone())
            .create(revision)
            .await
            .expect("create source-backed revision");

        let node_id = RecordId::new_v4(TableName::DocNode);
        let span_end = u32::try_from(text.len()).unwrap_or(u32::MAX);
        let node = DocNode {
            base: BaseFields::new(node_id.clone(), space_id.to_string(), actor.to_string()),
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, span_end),
            node_key: format!("node:{node_id}"),
            text: None,
            parent: None,
            position: 0,
            depth: 1,
            embedding_model: None,
            collection: None,
        };
        DocNodeRepository::new(storage.clone())
            .create(node)
            .await
            .expect("create source-backed node");

        (revision_id, node_id, source.base.id)
    }

    #[expect(
        clippy::default_trait_access,
        clippy::significant_drop_tightening,
        reason = "Preserve the fixture collection defaults and repository owner lifetime."
    )]
    async fn ensure_fixture_space(storage: &StorageClient, space_id: &str, actor: &str) {
        let spaces = SpaceConfigRepository::new(storage.clone());
        if spaces
            .get_by_space_id(space_id)
            .await
            .expect("space lookup")
            .is_none()
        {
            spaces
                .create(elephant_types::records::SpaceConfig {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::SpaceConfig),
                        space_id.to_string(),
                        actor.to_string(),
                    ),
                    embedding_provider: "openai".to_string(),
                    embedding_model: "text-embedding-3-small".to_string(),
                    embedding_dims: 1536,
                    purpose: None,
                    skill: None,
                    processing: Some(elephant_types::provider_binding::SpaceProcessingConfig {
                        generation: 1,
                        index_generation: 0,
                        policy_id: "fixture-no-model-approval".to_string(),
                        connections: Default::default(),
                        profiles: Default::default(),
                        bindings: Default::default(),
                        active_embedding_profile_id: None,
                        pending_reindex: None,
                        completed_embedding_reindex: None,
                    }),
                })
                .await
                .expect("explicit fixture space");
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn create_work_run(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        work_item_id: &RecordId,
    ) -> RecordId {
        ensure_fixture_space(storage, space_id, actor).await;
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let work_run = WorkRun {
            base: BaseFields::new(work_run_id.clone(), space_id.to_string(), actor.to_string()),
            work_item: Link::new(work_item_id.clone()),
            attempt: 1,
            started_at: chrono::Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "test-worker".to_string(),
            worker_version: "test-v1".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };

        let work_run_repo = WorkRunRepository::new(storage.clone());
        work_run_repo
            .create(work_run)
            .await
            .expect("create work_run");

        work_run_id
    }

    /// Exercise the real dispatcher with durable queue authority and run receipts.
    /// The unit fixture supplies its explicit TM context (including deterministic
    /// `None`) instead of constructing a live model client from ambient settings.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_fixture(
        item: &WorkItem,
        run_id: &RecordId,
        storage: StorageClient,
        config: &WorkQueueWorkerConfig,
        _spaces: &SpaceConfigRepository,
        _providers: &EmbeddingProviderCache,
        _embedding: &EmbedWorkerConfig,
        tm: &TmContext,
    ) -> anyhow::Result<WorkRunMetrics> {
        dispatch_fixture_with_extractor(item, run_id, storage, config, tm, None).await
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn dispatch_fixture_with_extractor(
        item: &WorkItem,
        run_id: &RecordId,
        storage: StorageClient,
        config: &WorkQueueWorkerConfig,
        tm: &TmContext,
        extractor: Option<&dyn Extractor>,
    ) -> anyhow::Result<WorkRunMetrics> {
        ensure_fixture_space(&storage, &item.base.space_id, &item.base.created_by).await;
        let items = WorkItemRepository::new(storage.clone());
        if items
            .get(&item.base.space_id, &item.base.id)
            .await?
            .is_none()
        {
            items.create(item.clone()).await?;
        }
        let claimed = items
            .claim_with_lease(
                &item.base.space_id,
                &item.base.id,
                "fixture-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("fixture item must be runnable"))?;
        let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)?;
        let runs = WorkRunRepository::new(storage.clone());
        let existing = runs.get(&item.base.space_id, run_id).await?;
        let is_new = existing.is_none();
        let mut run = existing.unwrap_or_else(|| {
            let mut base = claimed.base.clone();
            base.id = run_id.clone();
            WorkRun {
                base,
                work_item: Link::new(claimed.base.id.clone()),
                attempt: claimed.attempts,
                started_at: Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics: WorkRunMetrics::default(),
                worker_id: "fixture-worker".into(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            }
        });
        anyhow::ensure!(
            run.work_item.id() == &claimed.base.id,
            "fixture run identity"
        );
        run.attempt = claimed.attempts;
        run.metrics
            .custom
            .insert("attempt_fence".into(), serde_json::to_value(&attempt)?);
        if is_new {
            runs.create(run.clone()).await?;
        } else {
            runs.update(&item.base.space_id, run_id, run.clone())
                .await?;
        }
        let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
            storage.clone(),
            run_id,
            attempt.clone(),
        );
        eprintln!(
            "fixture dispatch begin kind={} work={} generation={}",
            item.kind, item.base.id, attempt.generation
        );
        let result = storage
            .with_activity(
                elephant_pipeline::provider_dispatch::with_dispatch_observer(
                    observer.clone(),
                    Box::pin(super::execute_work_item_with_extractor(
                        &claimed,
                        run_id,
                        storage.clone(),
                        config,
                        tm,
                        extractor,
                    )),
                ),
            )
            .await?;
        eprintln!(
            "fixture dispatch end kind={} work={} success={}",
            item.kind,
            item.base.id,
            result.is_ok()
        );
        run = runs
            .get(&item.base.space_id, run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("fixture original run disappeared"))?;
        run.completed_at = Some(Utc::now());
        run.success = result.is_ok();
        run.error = result.as_ref().err().map(ToString::to_string);
        if let Ok(metrics) = &result {
            let preserved = run.metrics.custom.clone();
            run.metrics = metrics.clone();
            for (key, value) in preserved {
                run.metrics.custom.entry(key).or_insert(value);
            }
        }
        run.metrics
            .custom
            .insert("attempt_fence".into(), serde_json::to_value(&attempt)?);
        observer
            .summarize(AttemptUsage::reserved(0, 0.0))
            .await?
            .write_metrics(&mut run.metrics)?;
        super::record_summary_process_totals(&mut run.metrics);
        runs.update(&item.base.space_id, run_id, run).await?;
        let queue =
            WorkQueue::new(storage.clone(), WorkQueueConfig::default()).for_attempt(&claimed)?;
        if result.is_ok() {
            complete_work_item_and_promote(
                &queue,
                &DagExecutor::new(storage),
                &item.base.space_id,
                &item.base.id,
            )
            .await?;
        } else if result
            .as_ref()
            .err()
            .is_some_and(|error| classify_failure(error) == FailureClass::PendingDependencies)
        {
            let pending_error = result.as_ref().err().ok_or_else(|| {
                anyhow::anyhow!("dependency retry requires the actual failed result")
            })?;
            let RetryAction::Blocked { delay_secs } = retry_action(
                FailureClass::PendingDependencies,
                claimed.attempts,
                config.transient_retry_max_attempts,
                config.transient_retry_base_delay_secs,
                config.transient_retry_max_delay_secs,
            ) else {
                anyhow::bail!("pending dependency did not receive production blocked policy");
            };
            source_runtime::record_operation_failure(
                &storage,
                &claimed,
                FailureClass::PendingDependencies,
                RetryAction::Blocked { delay_secs },
            )
            .await?;
            items
                .requeue_pending_dependencies(
                    &item.base.space_id,
                    &item.base.id,
                    &attempt.lease_token,
                    delay_secs,
                    pending_error.to_string(),
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("fixture lost original blocked attempt"))?;
            // The caller sees the original typed error and independently checks
            // the failed run, refund and real due time before scheduling again.
        } else {
            items
                .update_running_for_worker(
                    &item.base.space_id,
                    &item.base.id,
                    &attempt.lease_token,
                    WorkItemStatus::Failed,
                    result.as_ref().err().map(ToString::to_string),
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("fixture lost original attempt"))?;
        }
        result
    }

    async fn create_entity(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        display_name: &str,
        entity_type: &str,
    ) -> Entity {
        EntityRepository::new(storage.clone())
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                EntityType::new(entity_type),
                display_name.to_string(),
            ))
            .await
            .expect("create entity")
    }

    async fn create_pending_assertion(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        evidence_ids: (&RecordId, &RecordId),
        subject_text: &str,
        predicate_raw: &str,
        object_text: Option<&str>,
    ) -> Assertion {
        ensure_fixture_space(storage, space_id, actor).await;
        let (revision_id, node_id) = evidence_ids;
        let revision = DocRevisionRepository::new(storage.clone())
            .get(space_id, revision_id)
            .await
            .expect("fixture revision lookup")
            .expect("fixture revision exists");
        let now = Utc::now();
        AssertionRepository::new(storage.clone())
            .create(Assertion {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Assertion),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                subject_text: subject_text.to_string(),
                predicate_raw: predicate_raw.to_string(),
                predicate_id: None,
                object_text: object_text.map(str::to_string),
                object_literal: None,
                evidence: EvidenceSpan {
                    revision: Link::new(revision_id.clone()),
                    node: Some(Link::new(node_id.clone())),
                    start: 0,
                    end: u32::try_from(revision.text.len()).expect("fixture text length"),
                    quote_hash: elephant_pipeline::extraction::AssertionValidator::hash_quote(
                        &revision.text,
                    ),
                },
                source_event_time: None,
                observed_at: now,
                asserted_at: now,
                assertion_key: format!("assertion:{}", RecordId::new_v4(TableName::Assertion)),
                resolution_status: ResolutionStatus::Pending,
                resolution_method: None,
                resolution_confidence: None,
                retry_state: AssertionRetryState::default(),
                subject_entity: None,
                object_entity: None,
                confidence: 0.8,
                subject_entity_type: Some("person".to_string()),
                object_entity_type: Some("organization".to_string()),
                significance: Some("medium".to_string()),
                lifecycle_tier: "active".to_string(),
            })
            .await
            .expect("create assertion")
    }

    async fn create_manual_source(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        title: &str,
        text: &str,
        requested_mode: IngestMode,
    ) -> Source {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space_id.to_string(),
                actor.to_string(),
            ),
            elephant_types::records::source::SourceType::Manual,
            format!("manual:{title}"),
        );
        source.set_requested_ingest_mode(requested_mode);
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: title.to_string(),
                text: text.to_string(),
                sidecar_text: None,
                media_attachments: Vec::new(),
                rendition_key: format!("rk:{title}"),
                metadata: serde_json::json!({ "kind": "manual" }),
            })
            .expect("set manual doc template");

        let repo = SourceRepository::new(storage.clone());
        repo.create(source).await.expect("create source")
    }

    #[test]
    fn test_is_transient_work_error_detects_write_conflict() {
        let err = anyhow::anyhow!(
            "Storage error: Failed to commit transaction due to a read or write conflict"
        );
        assert!(super::is_transient_work_error(&err));
    }

    #[test]
    fn test_is_transient_work_error_detects_storage_conflict_type() {
        let err = anyhow::Error::new(StorageError::Conflict("unique index conflict".to_string()));
        assert!(super::is_transient_work_error(&err));
    }

    #[test]
    fn test_is_transient_work_error_detects_pipeline_storage_conflict_type() {
        let err = anyhow::Error::new(PipelineError::StorageError(StorageError::Conflict(
            "write conflict".to_string(),
        )));
        assert!(super::is_transient_work_error(&err));
    }

    #[test]
    fn test_compute_transient_retry_delay_caps_backoff() {
        let delay_1 = super::compute_transient_retry_delay_secs(1, 10, 120);
        let delay_2 = super::compute_transient_retry_delay_secs(2, 10, 120);
        let delay_10 = super::compute_transient_retry_delay_secs(10, 10, 120);

        assert_eq!(delay_1, 10);
        assert_eq!(delay_2, 20);
        assert_eq!(delay_10, 120);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_unconfigured_space_cannot_fall_back_to_ambient_embeddings() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        ensure_fixture_space(&storage, "denied-embedding", "fixture").await;
        let context =
            ProviderDispatchContext::new(SpaceConfigRepository::new(storage), "denied-embedding");
        let result = context.embedding(ModelOperation::DocumentEmbedding).await;
        assert!(
            matches!(result, Err(PipelineError::ConfigError(_))),
            "explicit model approval is required"
        );
    }

    #[tokio::test]
    async fn test_execute_work_item_plan_extraction_enqueues_extract() {
        grounded_stage_tests::plan_enqueues_extract(false).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_execute_work_item_plan_extraction_reports_missing_revision_as_stale_input() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "obsolete_plan_space";
        let actor = "tester";
        let missing_revision_id = RecordId::new_v4(TableName::DocRevision);
        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: PLAN_EXTRACTION_KIND.to_string(),
            payload: serde_json::json!({
                "revision_id": missing_revision_id.to_string(),
                "ingest_mode": "graph",
                "policy_version": "v1.0"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "obsolete-plan-extraction-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let error = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect_err("missing immutable input cannot complete");

        assert_eq!(classify_failure(&error), FailureClass::StaleInput);
        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");
        assert!(items
            .iter()
            .all(|item| item.kind != EXTRACT_ASSERTIONS_KIND));
    }

    #[tokio::test]
    async fn test_execute_work_item_plan_extraction_derives_source_from_revision_doc() {
        grounded_stage_tests::plan_enqueues_extract(true).await;
    }

    #[tokio::test]
    async fn test_execute_work_item_render_doc_enqueues_plan_extraction() {
        grounded_stage_tests::render_enqueues_plan().await;
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_render_doc_does_not_regress_graph_ready_source_mode() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "default";
        let actor = "tester";

        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Graph Source",
            "Already graph-ready source",
            IngestMode::Graph,
        )
        .await;
        update_source_record(&storage, space_id, &source.base.id, |record| {
            record.lifecycle_status = SourceLifecycleStatus::GraphReady;
            record.current_ingest_mode = Some(IngestMode::Graph);
        })
        .await
        .expect("seed graph-ready source");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = elephant_types::records::Doc {
            base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
            title: "Test Doc".to_string(),
            text: "Alice works at Acme.".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source.base.id.clone())),
            rendition_key: "test-rendition".to_string(),
            metadata: serde_json::json!({}),
        };
        let doc_repo = DocRepository::new(storage.clone());
        let _ = doc_repo.create(doc).await.expect("create doc");

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: RENDER_DOC_KIND.to_string(),
            payload: serde_json::json!({
                "doc_id": doc_id.to_string(),
                "source_id": source.base.id.to_string(),
                "ingest_mode": IngestMode::Retrieval.to_string(),
            }),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: "render-doc-no-regress".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let _ = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute render_doc");

        let refreshed = SourceRepository::new(storage)
            .get(space_id, &source.base.id)
            .await
            .expect("get source")
            .expect("source exists");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::GraphReady
        );
        assert_eq!(refreshed.current_ingest_mode, Some(IngestMode::Graph));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_execute_work_item_ingest_source_stub_accepts_discovered_source() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "source-stub-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Stub Source",
            "Only a source stub should run.",
            IngestMode::Metadata,
        )
        .await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: INGEST_SOURCE_STUB_KIND.to_string(),
            payload: serde_json::json!({ "source_id": source.base.id.to_string() }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: format!("ingest_source_stub:{}", source.base.id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute ingest_source_stub");

        let refreshed = SourceRepository::new(storage)
            .get(space_id, &source.base.id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
    }

    #[tokio::test]
    async fn test_execute_work_item_extract_assertions_creates_artifact() {
        grounded_stage_tests::extraction_stages_candidate(false).await;
    }

    #[tokio::test]
    async fn test_execute_work_item_extract_assertions_derives_source_for_artifact_workflow() {
        grounded_stage_tests::extraction_stages_candidate(true).await;
    }

    #[tokio::test]
    async fn test_extract_to_truth_maintenance_chain() {
        Box::pin(grounded_stage_tests::extract_to_truth_maintenance_chain()).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_source_metadata_mode_records_metadata_artifact_without_doc_materialization() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "source-metadata-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Metadata source",
            "Only metadata should be extracted",
            IngestMode::Metadata,
        )
        .await;

        let triage_item_id = RecordId::new_v4(TableName::WorkItem);
        let triage_item = WorkItem {
            base: BaseFields::new(
                triage_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: TRIAGE_SOURCE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "policy_version": "default",
                "target_mode": IngestMode::Metadata.to_string(),
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: format!("triage_source:{}", source.base.id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let triage_run_id = create_work_run(&storage, space_id, actor, &triage_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let _ = dispatch_fixture(
            &triage_item,
            &triage_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute triage_source");

        let item_repo = WorkItemRepository::new(storage.clone());
        let metadata_item = item_repo
            .find_by_idempotency_key(
                space_id,
                &format!("{INGEST_METADATA_KIND}:{}", source.base.id),
            )
            .await
            .expect("find metadata item")
            .expect("metadata item");
        assert_eq!(
            metadata_item.priority,
            successor_priority(triage_item.priority),
            "successor stages must outrank same-priority new arrivals"
        );
        let metadata_run_id =
            create_work_run(&storage, space_id, actor, &metadata_item.base.id).await;
        let _ = dispatch_fixture(
            &metadata_item,
            &metadata_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute ingest_metadata");

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        let staged_artifacts = artifact_repo
            .find_by_work_run(&metadata_run_id)
            .await
            .expect("find metadata artifacts");
        assert_eq!(staged_artifacts.len(), 1);
        assert_eq!(
            staged_artifacts[0].artifact_type,
            SOURCE_METADATA_ARTIFACT_TYPE
        );
        assert_eq!(staged_artifacts[0].status, ArtifactStatus::Draft);

        let queued_after_metadata = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list queued after metadata");
        let validate_item = queued_after_metadata
            .iter()
            .find(|item| item.kind == VALIDATE_ARTIFACTS_KIND)
            .expect("validate item")
            .clone();
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        let _ = dispatch_fixture(
            &validate_item,
            &validate_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute validate_artifacts");

        let validated_artifacts = artifact_repo
            .find_by_work_run(&metadata_run_id)
            .await
            .expect("find validated artifacts");
        let metadata_artifact = validated_artifacts
            .iter()
            .find(|artifact| artifact.artifact_type == SOURCE_METADATA_ARTIFACT_TYPE)
            .expect("metadata artifact");
        assert_eq!(metadata_artifact.status, ArtifactStatus::Validated);
        let commit_bundle = validated_artifacts
            .iter()
            .find(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
            .expect("commit bundle");
        assert_eq!(commit_bundle.status, ArtifactStatus::Validated);

        let queued_after_validate = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list queued after validate");
        let commit_item = queued_after_validate
            .iter()
            .find(|item| item.kind == COMMIT_ARTIFACTS_KIND)
            .expect("commit item")
            .clone();
        let commit_run_id = create_work_run(&storage, space_id, actor, &commit_item.base.id).await;
        let _ = dispatch_fixture(
            &commit_item,
            &commit_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute commit_artifacts");

        let source_repo = SourceRepository::new(storage.clone());
        let refreshed = source_repo
            .get(space_id, &source.base.id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::MetadataReady
        );
        assert_eq!(refreshed.current_ingest_mode, Some(IngestMode::Metadata));

        let doc_repo = DocRepository::new(storage.clone());
        let docs = doc_repo.list(space_id, 10, None).await.expect("list docs");
        assert!(
            docs.is_empty(),
            "metadata-only ingest should not materialize docs"
        );

        let final_artifacts = artifact_repo
            .find_by_work_run(&metadata_run_id)
            .await
            .expect("find final artifacts");
        let final_commit_bundle = final_artifacts
            .iter()
            .find(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
            .expect("final commit bundle");
        assert_eq!(final_commit_bundle.status, ArtifactStatus::Committed);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_graph_mode_metadata_commit_enqueues_upgrade_without_marking_graph_ready() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "source-graph-metadata-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Graph source",
            "Metadata commit should not short-circuit graph ingestion",
            IngestMode::Graph,
        )
        .await;

        let triage_item_id = RecordId::new_v4(TableName::WorkItem);
        let triage_item = WorkItem {
            base: BaseFields::new(
                triage_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: TRIAGE_SOURCE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "policy_version": "default",
                "target_mode": IngestMode::Graph.to_string(),
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: format!("triage_source:{}", source.base.id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let triage_run_id = create_work_run(&storage, space_id, actor, &triage_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &triage_item,
            &triage_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute triage_source");

        let item_repo = WorkItemRepository::new(storage.clone());
        let metadata_item = item_repo
            .find_by_idempotency_key(
                space_id,
                &format!("{INGEST_METADATA_KIND}:{}", source.base.id),
            )
            .await
            .expect("find metadata item")
            .expect("metadata item");
        assert_eq!(
            metadata_item.priority,
            successor_priority(triage_item.priority),
            "successor stages must outrank same-priority new arrivals"
        );
        let metadata_run_id =
            create_work_run(&storage, space_id, actor, &metadata_item.base.id).await;
        dispatch_fixture(
            &metadata_item,
            &metadata_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute ingest_metadata");

        let validate_item = item_repo
            .find_by_idempotency_key(
                space_id,
                &format!("{VALIDATE_ARTIFACTS_KIND}:{metadata_run_id}"),
            )
            .await
            .expect("find validate item")
            .expect("validate item");
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        dispatch_fixture(
            &validate_item,
            &validate_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute validate_artifacts");

        let commit_item = item_repo
            .find_by_idempotency_key(
                space_id,
                &format!("{COMMIT_ARTIFACTS_KIND}:{metadata_run_id}"),
            )
            .await
            .expect("find commit item")
            .expect("commit item");
        let commit_run_id = create_work_run(&storage, space_id, actor, &commit_item.base.id).await;
        dispatch_fixture(
            &commit_item,
            &commit_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute commit_artifacts");

        let refreshed = SourceRepository::new(storage.clone())
            .get(space_id, &source.base.id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::MetadataReady
        );
        assert_eq!(refreshed.current_ingest_mode, Some(IngestMode::Metadata));

        let upgrade_item = item_repo
            .find_by_idempotency_key(
                space_id,
                &format!(
                    "{UPGRADE_INGEST_MODE_KIND}:{}:{}",
                    source.base.id,
                    IngestMode::Graph
                ),
            )
            .await
            .expect("find upgrade item")
            .expect("upgrade item");
        assert_eq!(upgrade_item.status, WorkItemStatus::Runnable);

        let upgrade_run_id =
            create_work_run(&storage, space_id, actor, &upgrade_item.base.id).await;
        dispatch_fixture(
            &upgrade_item,
            &upgrade_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute upgrade_ingest_mode");

        let render_item = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list work items")
            .into_iter()
            .find(|item| item.kind == RENDER_DOC_KIND)
            .expect("render_doc item");
        assert_eq!(render_item.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_execute_work_item_ingest_metadata_skips_when_source_not_triaged() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "metadata-skip-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Skip metadata",
            "Source has not been triaged yet.",
            IngestMode::Metadata,
        )
        .await;

        let metadata_item_id = RecordId::new_v4(TableName::WorkItem);
        let metadata_item = WorkItem {
            base: BaseFields::new(
                metadata_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: INGEST_METADATA_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "target_mode": IngestMode::Metadata.to_string(),
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: format!("{INGEST_METADATA_KIND}:{}", source.base.id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let metadata_run_id = create_work_run(&storage, space_id, actor, &metadata_item_id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &metadata_item,
            &metadata_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute ingest_metadata");

        let artifacts = WorkArtifactRepository::new(storage.clone())
            .find_by_work_run(&metadata_run_id)
            .await
            .expect("artifacts");
        assert!(
            artifacts.is_empty(),
            "metadata ingest should skip without triage"
        );

        let refreshed = SourceRepository::new(storage)
            .get(space_id, &source.base.id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
    }

    #[tokio::test]
    async fn test_validate_artifacts_creates_commit_bundle_before_canonical_writes() {
        Box::pin(grounded_stage_tests::validation_stages_before_canonical_writes()).await;
    }

    #[tokio::test]
    async fn test_validate_artifacts_legacy_assertion_batch_alias_still_bundles() {
        grounded_stage_tests::legacy_alias_bundles().await;
    }

    #[tokio::test]
    async fn test_commit_knowledge_candidates_refines_event_across_nodes_for_same_source() {
        grounded_event_tests::refines_event_across_nodes().await;
    }

    #[tokio::test]
    async fn test_commit_knowledge_candidates_distinguishes_fallback_lineage_by_event_time() {
        grounded_event_tests::distinguishes_exact_event_times().await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_validate_artifacts_rejects_resolution_patch_without_target_assertion() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "resolution-validate-space";
        let actor = "tester";
        let resolution_item_id = RecordId::new_v4(TableName::WorkItem);
        let resolution_run_id =
            create_work_run(&storage, space_id, actor, &resolution_item_id).await;
        let workflow = ResolutionPatchWorkflow::new(storage.clone());
        let (revision, node) = Box::pin(create_revision_with_node(
            &storage,
            space_id,
            actor,
            "Alice works at Acme Corp.",
        ))
        .await;
        let target = Box::pin(create_pending_assertion(
            &storage,
            space_id,
            actor,
            (&revision, &node),
            "Alice",
            "works_at",
            Some("Acme Corp"),
        ))
        .await;

        workflow
            .stage_resolution_patch(
                &resolution_run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    prepared_epoch: None,
                    assertion_id: target.base.id.clone(),
                    resolved_subject: None,
                    resolved_object: None,
                    confidence: 0.4,
                    resolution_method: "candidate_context".to_string(),
                    status: ResolutionStatus::Abstained,
                    abstention_reason: Some("ambiguous".to_string()),
                },
                space_id,
            )
            .await
            .expect("stage resolution patch");
        // Staging requires the actual authorized target. Remove it only after
        // staging so this remains an oracle for the later validation boundary.
        AssertionRepository::new(storage.clone())
            .delete(space_id, &target.base.id)
            .await
            .expect("remove staged target");

        let staged_commit = StagedCommit::new(storage.clone());
        let validated = elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute(
            &staged_commit,
            space_id,
            &resolution_run_id,
        )
        .await
        .expect("validate artifacts");

        assert_eq!(validated.len(), 1);
        assert_eq!(validated[0].status, ArtifactStatus::Rejected);
        assert!(
            validated[0]
                .validation_errors
                .clone()
                .unwrap_or_default()
                .iter()
                .any(|error| error.contains("Assertion not found")),
            "expected repository-backed validation error, got {:?}",
            validated[0].validation_errors
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_validate_artifacts_rejected_artifacts_do_not_bundle_or_enqueue_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "resolution-reject-space";
        let actor = "tester";
        let resolution_item_id = RecordId::new_v4(TableName::WorkItem);
        let resolution_run_id =
            create_work_run(&storage, space_id, actor, &resolution_item_id).await;
        let workflow = ResolutionPatchWorkflow::new(storage.clone());
        let (revision, node) = Box::pin(create_revision_with_node(
            &storage,
            space_id,
            actor,
            "Alice works at Acme Corp.",
        ))
        .await;
        let target = Box::pin(create_pending_assertion(
            &storage,
            space_id,
            actor,
            (&revision, &node),
            "Alice",
            "works_at",
            Some("Acme Corp"),
        ))
        .await;

        workflow
            .stage_resolution_patch(
                &resolution_run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    prepared_epoch: None,
                    assertion_id: target.base.id.clone(),
                    resolved_subject: None,
                    resolved_object: None,
                    confidence: 0.4,
                    resolution_method: "candidate_context".to_string(),
                    status: ResolutionStatus::Abstained,
                    abstention_reason: Some("ambiguous".to_string()),
                },
                space_id,
            )
            .await
            .expect("stage resolution patch");
        // Staging requires the actual authorized target. Remove it only after
        // staging so this remains an oracle for the later validation boundary.
        AssertionRepository::new(storage.clone())
            .delete(space_id, &target.base.id)
            .await
            .expect("remove staged target");

        let validate_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: VALIDATE_ARTIFACTS_KIND.to_string(),
            payload: serde_json::json!({
                "work_run_id": resolution_run_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "validate-rejected-patch".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &validate_item,
            &validate_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute validate_artifacts");

        let artifacts = WorkArtifactRepository::new(storage.clone())
            .find_by_work_run(&resolution_run_id)
            .await
            .expect("find artifacts");
        assert_eq!(
            artifacts
                .iter()
                .filter(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
                .count(),
            0,
            "rejected artifacts must not produce commit bundles"
        );

        let queued = WorkItemRepository::new(storage)
            .list(space_id, 50, None)
            .await
            .expect("list work items");
        assert!(
            queued.iter().all(|item| item.kind != COMMIT_ARTIFACTS_KIND),
            "rejected artifacts must not enqueue commit_artifacts"
        );
    }

    #[tokio::test]
    async fn test_commit_artifacts_applies_resolution_patch_bundle() {
        Box::pin(resolution_bundle_tests::contract(
            false,
            resolution_bundle_tests::Scenario::Commit,
        ))
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_apply_resolution_patch_rejects_second_patch_after_first_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "resolution-cas-space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme Corp.").await;
        let assertion = create_pending_assertion(
            &storage,
            space_id,
            actor,
            (&revision_id, &node_id),
            "Alice",
            "works_at",
            Some("Acme Corp"),
        )
        .await;
        let first_entity = create_entity(&storage, space_id, actor, "Alice", "person").await;
        let second_entity = create_entity(&storage, space_id, actor, "Alicia", "person").await;
        let backoff_config = BackoffConfig::default();

        apply_resolution_patch(
            &storage,
            space_id,
            &elephant_pipeline::resolution::ResolutionPatch {
                prepared_epoch: None,
                assertion_id: assertion.base.id.clone(),
                resolved_subject: Some(first_entity.base.id.clone()),
                resolved_object: None,
                confidence: 0.91,
                resolution_method: "graph_context".to_string(),
                status: ResolutionStatus::Resolved,
                abstention_reason: None,
            },
            &backoff_config,
            None,
        )
        .await
        .expect("first patch");

        let second = apply_resolution_patch(
            &storage,
            space_id,
            &elephant_pipeline::resolution::ResolutionPatch {
                prepared_epoch: None,
                assertion_id: assertion.base.id.clone(),
                resolved_subject: Some(second_entity.base.id.clone()),
                resolved_object: None,
                confidence: 0.95,
                resolution_method: "graph_context".to_string(),
                status: ResolutionStatus::Resolved,
                abstention_reason: None,
            },
            &backoff_config,
            None,
        )
        .await;
        assert!(second.is_err(), "second patch should fail CAS");

        let refreshed = AssertionRepository::new(storage)
            .get(space_id, &assertion.base.id)
            .await
            .expect("load assertion")
            .expect("assertion");
        assert_eq!(
            refreshed
                .subject_entity
                .as_ref()
                .map(|link| link.id().clone()),
            Some(first_entity.base.id)
        );
        assert_eq!(refreshed.resolution_status, ResolutionStatus::Resolved);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_upgrade_ingest_mode_skips_when_source_already_at_target_mode() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "upgrade-skip-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Already graph",
            "This source is already graph-ready.",
            IngestMode::Graph,
        )
        .await;
        update_source_record(&storage, space_id, &source.base.id, |record| {
            record.lifecycle_status = SourceLifecycleStatus::GraphReady;
            record.current_ingest_mode = Some(IngestMode::Graph);
        })
        .await
        .expect("seed graph-ready source");

        let upgrade_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: UPGRADE_INGEST_MODE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "target_mode": IngestMode::Graph.to_string(),
                "reason": "already_graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "upgrade-skip".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let upgrade_run_id =
            create_work_run(&storage, space_id, actor, &upgrade_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &upgrade_item,
            &upgrade_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute upgrade_ingest_mode");

        let docs = DocRepository::new(storage.clone())
            .list(space_id, 10, None)
            .await
            .expect("list docs");
        assert!(docs.is_empty(), "skip path should not materialize docs");
        let queued = WorkItemRepository::new(storage)
            .list(space_id, 20, None)
            .await
            .expect("list work items");
        assert!(
            queued.iter().all(|item| item.kind != RENDER_DOC_KIND),
            "no render job should be enqueued when source is already at target mode"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_enqueue_upgrade_after_metadata_commit_reopens_terminal_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "upgrade-reopen-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Metadata source",
            "This source should reopen upgrade work.",
            IngestMode::Graph,
        )
        .await;

        let metadata_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: VALIDATE_ARTIFACTS_KIND.to_string(),
            payload: serde_json::json!({ "source_id": source.base.id.to_string() }),
            status: WorkItemStatus::Done,
            priority: 7,
            idempotency_key: "validate-metadata".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let idempotency_key = format!(
            "{UPGRADE_INGEST_MODE_KIND}:{}:{}",
            source.base.id,
            IngestMode::Graph
        );
        let existing_upgrade = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: UPGRADE_INGEST_MODE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "target_mode": IngestMode::Graph.to_string(),
                "reason": "old_run"
            }),
            status: WorkItemStatus::Failed,
            priority: 1,
            idempotency_key: idempotency_key.clone(),
            attempts: 2,
            lease_expires_at: None,
            leased_by: Some("worker".to_string()),
            run_after: Some(chrono::Utc::now() + chrono::Duration::minutes(30)),
            last_error: Some("old failure".to_string()),
        };

        let repo = WorkItemRepository::new(storage.clone());
        repo.create(existing_upgrade.clone())
            .await
            .expect("create existing upgrade item");

        enqueue_upgrade_after_metadata_commit(
            &storage,
            &metadata_item,
            &source.base.id,
            IngestMode::Graph,
        )
        .await
        .expect("reopen existing upgrade item");

        let reopened = repo
            .find_by_idempotency_key(space_id, &idempotency_key)
            .await
            .expect("load reopened item")
            .expect("existing item");
        assert_eq!(reopened.base.id, existing_upgrade.base.id);
        assert_eq!(reopened.status, WorkItemStatus::Runnable);
        assert_eq!(reopened.attempts, 0);
        assert!(reopened.leased_by.is_none());
        assert!(reopened.run_after.is_none());
        assert!(reopened.last_error.is_none());
        assert_eq!(
            reopened.priority,
            successor_priority(metadata_item.priority)
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_upgrade_ingest_mode_reopens_terminal_render_doc_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "upgrade-render-reopen-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Render source",
            "This source should reopen render work.",
            IngestMode::Graph,
        )
        .await;
        update_source_record(&storage, space_id, &source.base.id, |record| {
            record.lifecycle_status = SourceLifecycleStatus::MetadataReady;
            record.current_ingest_mode = Some(IngestMode::Metadata);
        })
        .await
        .expect("seed metadata-ready source");

        let doc = materialize_doc_from_source(&storage, &source, actor)
            .await
            .expect("materialize doc");
        let render_idempotency_key =
            format!("{RENDER_DOC_KIND}:{}:{}", doc.base.id, doc.rendition_key);
        let existing_render = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: RENDER_DOC_KIND.to_string(),
            payload: serde_json::json!({
                "doc_id": doc.base.id.to_string(),
                "source_id": source.base.id.to_string(),
                "ingest_mode": IngestMode::Graph.to_string(),
            }),
            status: WorkItemStatus::Failed,
            priority: 1,
            idempotency_key: render_idempotency_key.clone(),
            attempts: 2,
            lease_expires_at: None,
            leased_by: Some("worker".to_string()),
            run_after: Some(chrono::Utc::now() + chrono::Duration::minutes(30)),
            last_error: Some("old failure".to_string()),
        };
        let work_item_repo = WorkItemRepository::new(storage.clone());
        work_item_repo
            .create(existing_render.clone())
            .await
            .expect("create existing render item");

        let upgrade_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: UPGRADE_INGEST_MODE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "target_mode": IngestMode::Graph.to_string(),
                "reason": "retry_render"
            }),
            status: WorkItemStatus::Runnable,
            priority: 7,
            idempotency_key: "upgrade-render-reopen".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let upgrade_run_id =
            create_work_run(&storage, space_id, actor, &upgrade_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        dispatch_fixture(
            &upgrade_item,
            &upgrade_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute upgrade_ingest_mode");

        let reopened = work_item_repo
            .find_by_idempotency_key(space_id, &render_idempotency_key)
            .await
            .expect("load render item")
            .expect("render item");
        assert_eq!(reopened.base.id, existing_render.base.id);
        assert_eq!(reopened.status, WorkItemStatus::Runnable);
        assert_eq!(reopened.attempts, 0);
        assert!(reopened.leased_by.is_none());
        assert!(reopened.run_after.is_none());
        assert!(reopened.last_error.is_none());
        assert_eq!(reopened.priority, successor_priority(upgrade_item.priority));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_mark_source_graph_ready_waits_for_render_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "graph-ready-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Graph source",
            "Pending render work should block graph ready.",
            IngestMode::Graph,
        )
        .await;
        update_source_record(&storage, space_id, &source.base.id, |record| {
            record.lifecycle_status = SourceLifecycleStatus::RetrievalReady;
            record.current_ingest_mode = Some(IngestMode::Graph);
        })
        .await
        .expect("seed retrieval-ready source");

        let render_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: RENDER_DOC_KIND.to_string(),
            payload: serde_json::json!({
                "doc_id": RecordId::new_v4(TableName::Doc).to_string(),
                "source_id": source.base.id.to_string(),
                "ingest_mode": IngestMode::Graph.to_string(),
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "render-pending".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        WorkItemRepository::new(storage.clone())
            .create(render_item)
            .await
            .expect("create render work item");

        mark_source_graph_ready_if_quiescent(
            &storage,
            space_id,
            &source.base.id,
            &RecordId::new_v4(TableName::WorkItem),
        )
        .await
        .expect("mark graph ready");

        let refreshed = SourceRepository::new(storage)
            .get(space_id, &source.base.id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(
            refreshed.lifecycle_status,
            SourceLifecycleStatus::RetrievalReady
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_materialize_doc_from_source_refreshes_doc_acl_and_provenance() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "materialize-source-space";
        let actor = "tester";
        let mut source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Source title",
            "Source body",
            IngestMode::Graph,
        )
        .await;
        source.base.security.level = 6;
        source.base.provenance.sources = vec![source.base.id.clone()];
        source.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        source.base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
        let source_id = source.base.id.clone();
        source = SourceRepository::new(storage.clone())
            .update(space_id, &source_id, source)
            .await
            .expect("update source");

        let template = source
            .manual_doc_template()
            .expect("template result")
            .expect("template");
        let doc_id = RecordId::from_str(&template.doc_id).expect("doc id");
        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
            title: "Old title".to_string(),
            text: "Old body".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "old-rk".to_string(),
            metadata: serde_json::json!({"old": true}),
        };
        doc.base.security.level = 0;
        doc.base.provenance = elephant_types::records::base::Provenance::default();
        doc.base.subjects = Vec::new();
        doc.base.subjects_state = elephant_types::records::base::SubjectsState::Unknown;
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("create stale doc");

        let updated = materialize_doc_from_source(&storage, &source, actor)
            .await
            .expect("materialize");

        assert_eq!(updated.base.security.level, 6);
        assert_eq!(updated.base.provenance, source.base.provenance);
        assert_eq!(updated.base.subjects, source.base.subjects);
        assert_eq!(
            updated.base.subjects_state,
            elephant_types::records::base::SubjectsState::Complete
        );
        assert_eq!(updated.title, "Source title");
        assert_eq!(updated.text, "Source body");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_materialize_doc_from_source_uses_existing_linked_doc_without_manual_template() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "materialize-linked-doc-space";
        let actor = "tester";
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space_id.to_string(),
                actor.to_string(),
            ),
            SourceType::Drive,
            "linked-doc-source".to_string(),
        );
        source.set_requested_ingest_mode(IngestMode::Graph);
        source.base.security.level = 7;
        source.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        source.base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
        let source = SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("create source");

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                space_id.to_string(),
                actor.to_string(),
            ),
            title: "Linked file".to_string(),
            text: "File-backed source body".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source.base.id.clone())),
            rendition_key: "linked-rk".to_string(),
            metadata: serde_json::json!({"content_type": "text/markdown"}),
        };
        let doc_id = doc.base.id.clone();
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("create linked doc");

        let updated = materialize_doc_from_source(&storage, &source, actor)
            .await
            .expect("materialize linked doc");

        assert_eq!(updated.base.id, doc_id);
        assert_eq!(updated.title, "Linked file");
        assert_eq!(updated.text, "File-backed source body");
        assert_eq!(updated.base.security.level, 7);
        assert_eq!(updated.base.subjects, source.base.subjects);
        assert_eq!(
            updated.base.subjects_state,
            elephant_types::records::base::SubjectsState::Complete
        );
        assert_eq!(updated.source, Some(Link::new(source.base.id.clone())));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_materialize_doc_from_source_restores_soft_deleted_staged_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "materialize-restores-doc-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Source title",
            "Source body",
            IngestMode::Graph,
        )
        .await;

        let template = source
            .manual_doc_template()
            .expect("template result")
            .expect("template");
        let doc_id = RecordId::from_str(&template.doc_id).expect("doc id");
        let doc = Doc {
            base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
            title: "Deleted title".to_string(),
            text: "Deleted body".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source.base.id.clone())),
            rendition_key: "old-rk".to_string(),
            metadata: serde_json::json!({"old": true}),
        };
        let doc_repo = DocRepository::new(storage.clone());
        doc_repo.create(doc).await.expect("create doc");
        doc_repo
            .delete(space_id, &doc_id)
            .await
            .expect("soft delete doc");

        let updated = materialize_doc_from_source(&storage, &source, actor)
            .await
            .expect("materialize");

        assert!(updated.base.deleted_at.is_none());
        assert_eq!(updated.title, "Source title");
        assert_eq!(updated.text, "Source body");
        assert_eq!(updated.source, Some(Link::new(source.base.id.clone())));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_upgrade_ingest_mode_rejects_graph_without_staged_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "upgrade-no-doc-space";
        let actor = "tester";
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space_id.to_string(),
                actor.to_string(),
            ),
            SourceType::Web,
            "no-doc-source".to_string(),
        );
        source.lifecycle_status = SourceLifecycleStatus::MetadataReady;
        source.current_ingest_mode = Some(IngestMode::Metadata);
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .expect("create source");

        let upgrade_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: UPGRADE_INGEST_MODE_KIND.to_string(),
            payload: serde_json::json!({
                "source_id": source.base.id.to_string(),
                "target_mode": IngestMode::Graph.to_string(),
                "reason": "missing_doc"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "upgrade-no-doc".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let upgrade_run_id =
            create_work_run(&storage, space_id, actor, &upgrade_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let err = dispatch_fixture(
            &upgrade_item,
            &upgrade_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect_err("upgrade should reject sources without staged docs");

        assert!(
            err.to_string()
                .contains("no staged manual doc or linked doc"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_resolution_dependency_promotes_after_completion() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "test_space";
        let actor = "tester";
        let item_repo = WorkItemRepository::new(storage.clone());
        let dag = DagExecutor::new(storage.clone());

        let extract_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: "test_kind".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: format!("idem-{}", RecordId::new_v4(TableName::WorkItem)),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let resolve_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: "test_kind_dependent".to_string(),
            payload: serde_json::json!({"assertion_ids": []}),
            status: WorkItemStatus::Pending,
            priority: 1,
            idempotency_key: format!("idem-{}", RecordId::new_v4(TableName::WorkItem)),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            last_error: None,
        };

        let extract_item = item_repo
            .create(extract_item)
            .await
            .expect("create extract item");
        let resolve_item = item_repo
            .create(resolve_item)
            .await
            .expect("create resolve item");

        dag.add_dependency(&extract_item.base.id, &resolve_item.base.id, space_id)
            .await
            .expect("create dependency");

        let queue_config = WorkQueueConfig {
            worker_id: "test-worker".to_string(),
            ..WorkQueueConfig::default()
        };
        let work_queue = WorkQueue::new(storage.clone(), queue_config);

        let running_item = work_queue.claim().await.expect("claim").expect("item");
        assert_eq!(running_item.base.id, extract_item.base.id);
        let work_queue = work_queue
            .for_attempt(&running_item)
            .expect("captured attempt");

        complete_work_item_and_promote(&work_queue, &dag, space_id, &extract_item.base.id)
            .await
            .expect("complete and promote");

        let updated = item_repo
            .get(space_id, &resolve_item.base.id)
            .await
            .expect("get resolve item")
            .expect("resolve item");
        assert_eq!(updated.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_truth_maintenance_execute_work_item_tm_resolve_slot_enqueues_tm_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_space";
        let actor = "tester";

        let subject_id = RecordId::new_v4(TableName::Entity);
        let attribute = make_chain_attribute(
            &storage,
            space_id,
            actor,
            &subject_id,
            "title",
            Literal::String("CEO".into()),
        )
        .await;
        let attr_id = attribute.base.id.clone();

        let attr_repo = AttributeRepository::new(storage.clone());
        attr_repo.create(attribute).await.expect("create attribute");

        let slot_key = compute_slot_key(
            &subject_id.to_string(),
            None,
            "title",
            None,
            "security_hash",
        );
        let conflict_group =
            create_conflict_group(&slot_key, "attribute", vec![attr_id.to_string()], space_id);
        let conflict_group_id = conflict_group.base.id.clone();
        let conflict_repo = ConflictGroupRepository::new(storage.clone());
        conflict_repo
            .create(conflict_group)
            .await
            .expect("create conflict group");

        let work_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: TM_RESOLVE_SLOT_KIND.to_string(),
            payload: serde_json::json!({ "conflict_group_id": conflict_group_id.to_string() }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "tm-resolve-slot-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let _ = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute tm_resolve_slot");

        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");
        assert!(items.iter().any(|item| item.kind == TM_COMMIT_KIND));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_truth_maintenance_execute_work_item_tm_detect_conflicts_commits_single_claim() {
        use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
        use elephant_types::records::rel::{ClaimStatus, Rel};
        use elephant_types::records::SystemEvent;

        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_space";
        let actor = "tester";

        let subject_id = Box::pin(create_entity(&storage, space_id, actor, "Alice", "person"))
            .await
            .base
            .id;
        let object_id = Box::pin(create_entity(&storage, space_id, actor, "Acme Corp", "org"))
            .await
            .base
            .id;
        let rel_id = RecordId::new_v4(TableName::Rel);
        let rel = Rel {
            base: BaseFields::new(rel_id.clone(), space_id.to_string(), actor.to_string()),
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: "works_at".to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.92,
            validity_window: None,
            evidence: Vec::new(),
            temporal_audit: None,
        };

        let rel_repo = RelRepository::new(storage.clone());
        rel_repo.create(rel).await.expect("create rel");

        let mut payload = OutboxPayload::new(
            space_id.to_string(),
            "rel.created".to_string(),
            "outbox".to_string(),
        );
        payload.add_record(RecordRef {
            id: rel_id.clone(),
            table: "rel".to_string(),
            op: EventOperation::Create,
        });

        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                space_id.to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "rel.created".to_string(),
            payload: serde_json::to_value(&payload).expect("serialize payload"),
        };

        let event_repo = SystemEventRepository::new(storage.clone());
        let created_event = event_repo.create(event).await.expect("create event");

        let work_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: TM_DETECT_CONFLICTS_KIND.to_string(),
            payload: serde_json::json!({
                "event_ids": [created_event.base.id.to_string()]
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "tm-detect-conflicts-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let _ = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute tm_detect_conflicts");

        let item_repo = WorkItemRepository::new(storage.clone());
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");
        assert!(!items.iter().any(|item| item.kind == TM_COMMIT_KIND));
        let slots = elephant_storage::TruthSlotRepository::new(storage)
            .list(space_id, 20, None)
            .await
            .expect("committed slots");
        assert_eq!(
            slots.len(),
            1,
            "single claim is applied in detection transaction"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_truth_maintenance_execute_work_item_tm_recompute_runs() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_space";
        let actor = "tester";

        let work_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: TM_RECOMPUTE_KIND.to_string(),
            payload: serde_json::json!({ "max_slots": 5 }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "tm-recompute-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();

        let _ = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage,
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect("execute tm_recompute");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn missing_space_cannot_acquire_adjudication_from_ambient_configuration() {
        let storage = StorageClient::new_embedded().await.expect("store");
        let context = ProviderDispatchContext::new(SpaceConfigRepository::new(storage), "missing");
        let error = build_tm_context_for_space(context)
            .await
            .expect_err("explicit space is required");
        assert_eq!(classify_failure(&error), FailureClass::ConfigurationBlocked);
    }

    /// Scripted adjudicator for chain tests.
    #[derive(Debug)]
    struct FakeChainAdjudicator {
        verdict: elephant_pipeline::truth_maintenance::adjudicator::AdjudicationVerdict,
    }

    #[async_trait::async_trait]
    impl elephant_pipeline::truth_maintenance::adjudicator::SlotAdjudicator for FakeChainAdjudicator {
        async fn adjudicate(
            &self,
            _input: &elephant_pipeline::truth_maintenance::adjudicator::AdjudicationInput,
        ) -> Result<
            Option<elephant_pipeline::truth_maintenance::adjudicator::AdjudicationVerdict>,
            PipelineError,
        > {
            Ok(Some(self.verdict.clone()))
        }

        fn id(&self) -> &'static str {
            "fake_chain"
        }
    }

    async fn make_chain_attribute(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        subject_id: &RecordId,
        attr_type: &str,
        value: Literal,
    ) -> Attribute {
        let attr_id = RecordId::new_v4(TableName::Attribute);
        let text = format!("Fixture {subject_id} {attr_type}: {value:?}");
        let (revision, node) = create_revision_with_node(storage, space_id, actor, &text).await;
        let assertion = create_pending_assertion(
            storage,
            space_id,
            actor,
            (&revision, &node),
            "Fixture subject",
            attr_type,
            None,
        )
        .await;
        let evidence = EvidenceRef {
            assertion: Link::new(assertion.base.id),
            span: assertion.evidence,
            time_basis: TimeBasis::ObservedAt,
            time_value: Utc::now(),
        };
        Attribute {
            base: BaseFields::new(attr_id, space_id.to_string(), actor.to_string()),
            subject: Link::new(subject_id.clone()),
            attr_type: attr_type.to_string(),
            value,
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![evidence],
            temporal_audit: None,
        }
    }

    async fn enqueue_attr_outbox_event(
        storage: &StorageClient,
        space_id: &str,
        attr_id: &RecordId,
    ) -> RecordId {
        use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
        use elephant_types::records::SystemEvent;

        let mut payload = OutboxPayload::new(
            space_id.to_string(),
            "attribute.created".to_string(),
            "outbox".to_string(),
        );
        payload.add_record(RecordRef {
            id: attr_id.clone(),
            table: "attribute".to_string(),
            op: EventOperation::Create,
        });
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                space_id.to_string(),
                "outbox".to_string(),
            ),
            seq: 1,
            event_type: "attribute.created".to_string(),
            payload: serde_json::to_value(&payload).expect("serialize payload"),
        };
        let event_repo = SystemEventRepository::new(storage.clone());
        event_repo
            .create(event)
            .await
            .expect("create event")
            .base
            .id
    }

    fn make_tm_work_item(
        space_id: &str,
        actor: &str,
        kind: &str,
        payload: serde_json::Value,
        idempotency_key: &str,
    ) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: kind.to_string(),
            payload,
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: idempotency_key.to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    async fn run_tm_item(storage: &StorageClient, item: &WorkItem, tm_ctx: &TmContext) {
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();
        let _ = dispatch_fixture(
            item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            tm_ctx,
        )
        .await
        .expect("execute work item");
    }

    /// Chain: two identical attribute claims corroborate end-to-end through
    /// the TM work kinds (detect -> direct commit, no conflict group).
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_tm_chain_identical_attrs_corroborate_end_to_end() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_chain_space";
        let actor = "tester";
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_repo = AttributeRepository::new(storage.clone());
        let attr1 = make_chain_attribute(
            &storage,
            space_id,
            actor,
            &subject_id,
            "age",
            Literal::Integer(35),
        )
        .await;
        let attr2 = make_chain_attribute(
            &storage,
            space_id,
            actor,
            &subject_id,
            "age",
            Literal::Integer(35),
        )
        .await;
        let attr1_id = attr1.base.id.clone();
        let attr2_id = attr2.base.id.clone();
        attr_repo.create(attr1).await.expect("create attr1");
        attr_repo.create(attr2).await.expect("create attr2");

        let event_id = enqueue_attr_outbox_event(&storage, space_id, &attr1_id).await;
        let detect_item = make_tm_work_item(
            space_id,
            actor,
            TM_DETECT_CONFLICTS_KIND,
            serde_json::json!({ "event_ids": [event_id.to_string()] }),
            "tm-chain-detect",
        );
        run_tm_item(&storage, &detect_item, &TmContext::none()).await;

        let item_repo = WorkItemRepository::new(storage.clone());
        let queued = item_repo.list(space_id, 20, None).await.expect("list");
        assert!(
            !queued.iter().any(|item| item.kind == TM_RESOLVE_SLOT_KIND),
            "identical values must not enqueue resolution"
        );
        assert!(
            !queued.iter().any(|item| item.kind == TM_COMMIT_KIND),
            "deterministic corroboration commits with detection"
        );

        let live1 = attr_repo.get(space_id, &attr1_id).await.expect("get attr1");
        let live2 = attr_repo.get(space_id, &attr2_id).await.expect("get attr2");
        let survivors: Vec<&Attribute> = [live1.as_ref(), live2.as_ref()]
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(survivors.len(), 1, "exactly one duplicate survives");
        assert_eq!(survivors[0].status, ClaimStatus::Approved);
        assert_eq!(survivors[0].evidence.len(), 2, "evidence merged");

        let conflict_repo = ConflictGroupRepository::new(storage.clone());
        let unresolved = conflict_repo
            .find_all_unresolved(space_id, 10)
            .await
            .expect("list groups");
        assert!(unresolved.is_empty(), "no conflict group for corroboration");
    }

    /// Chain: divergent attribute values go through `tm_resolve_slot` where
    /// the injected adjudicator's verdict wins, then `tm_commit` applies it.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_tm_chain_value_conflict_adjudicated_end_to_end() {
        use elephant_pipeline::truth_maintenance::adjudicator::{
            AdjudicationDecision, AdjudicationVerdict,
        };

        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_chain_space";
        let actor = "tester";
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_repo = AttributeRepository::new(storage.clone());
        let attr_old = make_chain_attribute(
            &storage,
            space_id,
            actor,
            &subject_id,
            "employer",
            Literal::String("King".to_string()),
        )
        .await;
        let attr_new = make_chain_attribute(
            &storage,
            space_id,
            actor,
            &subject_id,
            "employer",
            Literal::String("Blizzard".to_string()),
        )
        .await;
        let old_id = attr_old.base.id.clone();
        let new_id = attr_new.base.id.clone();
        attr_repo.create(attr_old).await.expect("create old");
        attr_repo.create(attr_new).await.expect("create new");

        let event_id = enqueue_attr_outbox_event(&storage, space_id, &old_id).await;
        let detect_item = make_tm_work_item(
            space_id,
            actor,
            TM_DETECT_CONFLICTS_KIND,
            serde_json::json!({ "event_ids": [event_id.to_string()] }),
            "tm-chain-detect-conflict",
        );
        run_tm_item(&storage, &detect_item, &TmContext::none()).await;

        let item_repo = WorkItemRepository::new(storage.clone());
        let queued = item_repo.list(space_id, 20, None).await.expect("list");
        let resolve_item = queued
            .iter()
            .find(|item| item.kind == TM_RESOLVE_SLOT_KIND)
            .expect("value conflict enqueues resolution")
            .clone();

        let verdict = AdjudicationVerdict {
            decision: AdjudicationDecision::SingleWinner,
            winner_id: Some(new_id.to_string()),
            equivalent_groups: Vec::new(),
            superseded: Vec::new(),
            winner_valid_from: None,
            proposed_semantics: Some(
                elephant_pipeline::truth_maintenance::adjudicator::ProposedSemantics {
                    cardinality: elephant_types::records::predicate::Cardinality::One,
                    temporal_model:
                        elephant_types::records::temporal::TemporalPersonality::StateOverTime,
                    conflict_policy:
                        elephant_types::records::truth::ConflictPolicy::SupersedeByRecency,
                },
            ),
            rationale: "Blizzard is current".to_string(),
        };
        let tm_ctx = TmContext {
            adjudicator: Some(Arc::new(FakeChainAdjudicator { verdict })),
        };
        run_tm_item(&storage, &resolve_item, &tm_ctx).await;

        let queued = item_repo.list(space_id, 20, None).await.expect("list");
        let commit_item = queued
            .iter()
            .find(|item| item.kind == TM_COMMIT_KIND)
            .expect("adjudicated commit enqueued")
            .clone();
        run_tm_item(&storage, &commit_item, &TmContext::none()).await;

        let winner = attr_repo
            .get(space_id, &new_id)
            .await
            .expect("get winner")
            .expect("winner exists");
        assert_eq!(winner.status, ClaimStatus::Approved);
        let loser = attr_repo
            .get(space_id, &old_id)
            .await
            .expect("get loser")
            .expect("loser exists");
        assert_eq!(loser.status, ClaimStatus::Superseded);

        let conflict_repo = ConflictGroupRepository::new(storage.clone());
        let unresolved = conflict_repo
            .find_all_unresolved(space_id, 10)
            .await
            .expect("list groups");
        assert!(unresolved.is_empty(), "group resolved after adjudication");
    }

    /// Read the complete original queue inventory, including retained history.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn tm_items(storage: &StorageClient, space: &str, kind: &str) -> Vec<WorkItem> {
        let items = WorkItemRepository::new(storage.clone());
        let mut selected = Vec::new();
        let mut cursor = None;
        loop {
            let page = items
                .list(space, 256, cursor.as_ref())
                .await
                .expect("actual queue page");
            let count = page.len();
            if let Some(last) = page.last() {
                assert_ne!(
                    cursor.as_ref(),
                    Some(&last.base.id),
                    "queue cursor advances"
                );
                cursor = Some(last.base.id.clone());
            }
            selected.extend(page.into_iter().filter(|item| item.kind == kind));
            if count < 256 {
                return selected;
            }
        }
    }

    /// Seed the slot through actual detection, resolution and atomic commit.
    /// Its original partition and decision state come from the active runtime.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn seed_divergent_attr_slot(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        stale: bool,
    ) -> (String, RecordId, Vec<String>) {
        ensure_fixture_space(storage, space_id, actor).await;
        // The default AllowParallel policy resolves both values without a
        // provider. This fixture needs a real unresolved review requirement
        // so the subsequent queue arms exercise pending conflict work.
        elephant_storage::PredicateRegistryRepository::new(storage.clone())
            .create(elephant_types::records::PredicateRegistry {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::PredicateRegistry),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                predicate_id: "age".into(),
                description: None,
                status: "active".into(),
                semantics: Some(elephant_types::records::predicate::PredicateSemantics {
                    conflict_policy:
                        elephant_types::records::truth::ConflictPolicy::RequireMoreEvidence,
                    ..Default::default()
                }),
                semantics_status: elephant_types::records::predicate::SemanticsStatus::Configured,
                first_seen_at: None,
                usage_count: 0,
                last_used_at: None,
            })
            .await
            .expect("original explicit review-required predicate");
        let subject_id = RecordId::new_v4(TableName::Entity);
        let attr_repo = AttributeRepository::new(storage.clone());
        let mut claim_ids = Vec::new();
        for value in [35_i64, 36] {
            let attr = make_chain_attribute(
                storage,
                space_id,
                actor,
                &subject_id,
                "age",
                Literal::Integer(value),
            )
            .await;
            claim_ids.push(attr.base.id.to_string());
            attr_repo
                .create(attr)
                .await
                .expect("create actual grounded attribute");
        }
        claim_ids.sort();
        let changed: RecordId = claim_ids[0].parse().expect("original attribute identity");
        let event = enqueue_attr_outbox_event(storage, space_id, &changed).await;
        let detection = make_tm_work_item(
            space_id,
            actor,
            TM_DETECT_CONFLICTS_KIND,
            serde_json::json!({"event_ids":[event.to_string()]}),
            "seed-actual-partition-detect",
        );
        run_tm_item(storage, &detection, &TmContext::none()).await;
        let resolution = tm_items(storage, space_id, TM_RESOLVE_SLOT_KIND)
            .await
            .into_iter()
            .find(|item| item.status == WorkItemStatus::Runnable)
            .expect("actual detected conflict work");
        let group_id: RecordId = resolution.payload["conflict_group_id"]
            .as_str()
            .expect("actual original group")
            .parse()
            .expect("typed group");
        run_tm_item(storage, &resolution, &TmContext::none()).await;
        let commit = tm_items(storage, space_id, TM_COMMIT_KIND)
            .await
            .into_iter()
            .find(|item| item.status == WorkItemStatus::Runnable)
            .expect("actual unresolved decision commit");
        run_tm_item(storage, &commit, &TmContext::none()).await;
        let group = ConflictGroupRepository::new(storage.clone())
            .get(space_id, &group_id)
            .await
            .expect("actual group read")
            .expect("retained group");
        assert!(
            !group.resolved,
            "no provider verdict was invented during setup"
        );
        assert!(
            group.adjudicated_claim_set_hash.is_none(),
            "setup did not fabricate memo authority"
        );
        let slots = elephant_storage::TruthSlotRepository::new(storage.clone());
        let slot = slots
            .find_by_slot_key(space_id, &group.slot_key)
            .await
            .expect("actual committed partition lookup")
            .expect("slot committed by real runtime");
        assert_eq!(slot.subject_entity, subject_id.to_string());
        if stale {
            slots
                .mark_stale_by_entity(space_id, &subject_id, "explicit recompute fixture")
                .await
                .expect("sanctioned actual stale transition");
        }
        (slot.slot_key, subject_id, claim_ids)
    }

    /// A memoized replay must not enqueue a `tm_commit` (it would clobber the
    /// previously committed verdict with Disputed statuses).
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_tm_resolve_slot_arm_skips_commit_for_memoized_replay() {
        use elephant_pipeline::truth_maintenance::adjudicator::{
            AdjudicationDecision, AdjudicationVerdict, ProposedSemantics,
        };
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;
        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _, claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, false).await;
        let groups = ConflictGroupRepository::new(storage.clone());
        let group = groups
            .find_unresolved_by_slot_key_and_type(space_id, &slot_key, "attribute")
            .await
            .expect("original group")
            .expect("unresolved group");
        let context = TmContext {
            adjudicator: Some(Arc::new(FakeChainAdjudicator {
                verdict: AdjudicationVerdict {
                    decision: AdjudicationDecision::SingleWinner,
                    winner_id: Some(claim_ids[1].clone()),
                    equivalent_groups: vec![],
                    superseded: vec![],
                    winner_valid_from: None,
                    proposed_semantics: Some(ProposedSemantics {
                        cardinality: elephant_types::records::predicate::Cardinality::One,
                        temporal_model:
                            elephant_types::records::temporal::TemporalPersonality::StateOverTime,
                        conflict_policy:
                            elephant_types::records::truth::ConflictPolicy::SupersedeByRecency,
                    }),
                    rationale: "actual original scripted fixture verdict".into(),
                },
            })),
        };
        let original = make_tm_work_item(
            space_id,
            actor,
            TM_RESOLVE_SLOT_KIND,
            serde_json::json!({"conflict_group_id":group.base.id.to_string()}),
            "tm-original-adjudication",
        );
        run_tm_item(&storage, &original, &context).await;
        let commit = tm_items(&storage, space_id, TM_COMMIT_KIND)
            .await
            .into_iter()
            .find(|item| item.status == WorkItemStatus::Runnable)
            .expect("real original adjudicated commit");
        run_tm_item(&storage, &commit, &TmContext::none()).await;
        let completed = groups
            .get(space_id, &group.base.id)
            .await
            .expect("actual memo read")
            .expect("group");
        assert!(completed.resolved && completed.adjudicated_claim_set_hash.is_some());
        let attributes = AttributeRepository::new(storage.clone());
        let winner_id: RecordId = claim_ids[1].parse().expect("winner identity");
        let loser_id: RecordId = claim_ids[0].parse().expect("loser identity");
        let winner = attributes
            .get(space_id, &winner_id)
            .await
            .expect("winner")
            .expect("present");
        let loser = attributes
            .get(space_id, &loser_id)
            .await
            .expect("loser")
            .expect("present");
        assert_eq!(winner.status, ClaimStatus::Approved);
        assert_eq!(loser.status, ClaimStatus::Superseded);
        let before: std::collections::BTreeSet<_> = tm_items(&storage, space_id, TM_COMMIT_KIND)
            .await
            .into_iter()
            .map(|item| item.base.id)
            .collect();
        let replay = make_tm_work_item(
            space_id,
            actor,
            TM_RESOLVE_SLOT_KIND,
            serde_json::json!({"conflict_group_id":group.base.id.to_string()}),
            "tm-resolve-memoized",
        );
        run_tm_item(&storage, &replay, &context).await;
        let after: std::collections::BTreeSet<_> = tm_items(&storage, space_id, TM_COMMIT_KIND)
            .await
            .into_iter()
            .map(|item| item.base.id)
            .collect();
        assert_eq!(
            after, before,
            "memoized replay must not enqueue another tm_commit"
        );
        assert_eq!(
            attributes
                .get(space_id, &winner_id)
                .await
                .expect("winner unchanged"),
            Some(winner)
        );
        assert_eq!(
            attributes
                .get(space_id, &loser_id)
                .await
                .expect("loser unchanged"),
            Some(loser)
        );
        assert_eq!(
            groups
                .get(space_id, &group.base.id)
                .await
                .expect("memo unchanged"),
            Some(completed)
        );
    }

    /// The recompute arm enqueues one `tm_resolve_slot` per pending group.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_tm_recompute_arm_enqueues_pending_resolves() {
        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _subject_id, _claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, true).await;
        let prior_resolves: std::collections::BTreeSet<_> =
            tm_items(&storage, space_id, TM_RESOLVE_SLOT_KIND)
                .await
                .into_iter()
                .map(|item| item.base.id)
                .collect();

        let work_item = make_tm_work_item(
            space_id,
            actor,
            TM_RECOMPUTE_KIND,
            serde_json::json!({ "max_slots": 5 }),
            "tm-recompute-pending",
        );
        run_tm_item(&storage, &work_item, &TmContext::none()).await;

        let items = tm_items(&storage, space_id, TM_RESOLVE_SLOT_KIND).await;
        let resolve_item = items
            .iter()
            .find(|item| !prior_resolves.contains(&item.base.id))
            .expect("pending group enqueued as tm_resolve_slot");

        let conflict_repo = ConflictGroupRepository::new(storage);
        let group = conflict_repo
            .find_unresolved_by_slot_key_and_type(space_id, &slot_key, "attribute")
            .await
            .expect("find group")
            .expect("group upserted");
        assert_eq!(
            resolve_item
                .payload
                .get("conflict_group_id")
                .and_then(|value| value.as_str()),
            Some(group.base.id.to_string().as_str())
        );
    }

    /// The reprocess arm enqueues `tm_resolve_slot` items for the conflict
    /// groups its reconvergence recompute deferred.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repository owners alive through the original regression assertion scope."
    )]
    async fn test_reprocess_arm_enqueues_pending_conflict_resolves() {
        use elephant_types::records::reprocess::{
            default_reprocess_target_stages, SelectionCriteria,
        };

        let storage = StorageClient::new_memory().await.expect("storage");
        initialize_fixture_schema(&storage).await;

        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _subject_id, _claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, false).await;
        let prior_resolves: std::collections::BTreeSet<_> =
            tm_items(&storage, space_id, TM_RESOLVE_SLOT_KIND)
                .await
                .into_iter()
                .map(|item| item.base.id)
                .collect();

        // No sources match the criteria: the job runs straight into the
        // remaintain recompute, which defers the divergent slot.
        let job_repo = ReprocessJobRepository::new(storage.clone());
        let creation = elephant_pipeline::reprocess::create_reprocess_job(
            &job_repo,
            "Reconverge",
            SelectionCriteria {
                source_types: vec!["email".to_string()],
                ..Default::default()
            },
            default_reprocess_target_stages(),
            space_id,
            actor,
            None,
        )
        .await
        .expect("create reprocess job");

        let work_id = job_repo
            .ensure_work(&creation.job)
            .await
            .expect("original durable handoff");
        let work_item = WorkItemRepository::new(storage.clone())
            .get(space_id, &work_id)
            .await
            .expect("read handoff")
            .expect("actual job work");
        // Reprocess loads its work run for provenance; seed a real one.
        let work_run_id = create_work_run(&storage, space_id, actor, &work_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();
        let requeue_started = Utc::now();
        let error = dispatch_fixture(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &space_config_repo,
            &provider_cache,
            &embed_config,
            &TmContext::none(),
        )
        .await
        .expect_err("real conflict children leave the original reprocess attempt pending");
        let requeue_finished = Utc::now();
        assert_eq!(
            error
                .downcast_ref::<WorkFailure>()
                .map(|failure| failure.class),
            Some(FailureClass::PendingDependencies),
            "the original dispatcher must return typed pending work: {error}"
        );
        let original_work = WorkItemRepository::new(storage.clone())
            .get(space_id, &work_id)
            .await
            .expect("read original requeued work")
            .expect("original work retained");
        assert_eq!(original_work.base.id, work_item.base.id);
        assert_eq!(original_work.status, WorkItemStatus::Runnable);
        assert_eq!(original_work.attempts, work_item.attempts);
        assert!(original_work.leased_by.is_none());
        assert!(original_work.lease_expires_at.is_none());
        assert_eq!(
            original_work.last_error.as_deref(),
            Some(format!("pending_dependencies: {error}").as_str())
        );
        let run_after = original_work.run_after.expect("original dependency delay");
        assert!(run_after >= requeue_started + Duration::seconds(5));
        assert!(run_after <= requeue_finished + Duration::seconds(5));
        let original_run = WorkRunRepository::new(storage.clone())
            .get(space_id, &work_run_id)
            .await
            .expect("read actual original run")
            .expect("original failed run retained");
        assert_eq!(original_run.work_item.id(), &work_id);
        assert_eq!(original_run.attempt, work_item.attempts + 1);
        assert!(!original_run.success);
        assert_eq!(
            original_run.error.as_deref(),
            Some(error.to_string().as_str())
        );
        let completed = original_run.completed_at.expect("original run finished");
        assert!(
            original_work.run_after.is_some_and(|due| due > completed),
            "actual blocked work must retain a future retry time"
        );
        let fence: elephant_types::publication::AttemptFence = serde_json::from_value(
            original_run
                .metrics
                .custom
                .get("attempt_fence")
                .expect("original claimed authority recorded")
                .clone(),
        )
        .expect("typed original attempt fence");
        assert_eq!(fence.work_item_id, work_id);
        assert_eq!(fence.space_id, space_id);
        assert!(fence.generation > 0 && !fence.lease_token.is_empty());
        let pending_job = job_repo
            .get(space_id, &creation.job.base.id)
            .await
            .expect("read original paused job")
            .expect("original job retained");
        assert_eq!(pending_job.target_stages, default_reprocess_target_stages());
        assert_eq!(
            pending_job.status,
            elephant_types::records::reprocess::ReprocessStatus::Paused
        );
        assert!(pending_job.completed_at.is_none());

        let items = tm_items(&storage, space_id, TM_RESOLVE_SLOT_KIND).await;
        let resolve_item = items
            .iter()
            .find(|item| !prior_resolves.contains(&item.base.id))
            .expect("reprocess must enqueue tm_resolve_slot for pending groups");

        let conflict_repo = ConflictGroupRepository::new(storage);
        let group = conflict_repo
            .find_unresolved_by_slot_key_and_type(space_id, &slot_key, "attribute")
            .await
            .expect("find group")
            .expect("group upserted");
        assert_eq!(
            resolve_item
                .payload
                .get("conflict_group_id")
                .and_then(|value| value.as_str()),
            Some(group.base.id.to_string().as_str())
        );
    }
}

async fn list_configured_space_ids(repo: &SpaceConfigRepository) -> anyhow::Result<Vec<String>> {
    const PAGE: usize = 256;
    let mut spaces = std::collections::BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = repo.list_all(PAGE, cursor.as_ref()).await?;
        let count = page.len();
        if let Some(last) = page.last() {
            if cursor.as_ref() == Some(&last.base.id) {
                anyhow::bail!("space registry cursor did not advance");
            }
            cursor = Some(last.base.id.clone());
        }
        spaces.extend(page.into_iter().map(|space| space.base.space_id));
        if count < PAGE {
            break;
        }
    }
    Ok(spaces.into_iter().collect())
}

/// Optional deterministic TM remains available without a model binding. A
/// configured but revoked adjudication binding fails; ambient keys never grant it.
async fn build_tm_context_for_space(context: ProviderDispatchContext) -> anyhow::Result<TmContext> {
    let resolved = context
        .repository
        .resolve_space(&context.space_id)
        .await
        .map_err(|error| PipelineError::ConfigError(format!("provider_authorization: {error}")))?;
    if !resolved
        .processing
        .bindings
        .contains_key(&ModelOperation::TruthMaintenance)
    {
        return Ok(TmContext::none());
    }
    #[cfg(feature = "meerkat-extraction")]
    {
        use elephant_pipeline::truth_maintenance::adjudicator::{
            AdjudicationConfig, AgenticSlotAdjudicator,
        };
        let adjudicator = AgenticSlotAdjudicator::new(AdjudicationConfig::default())
            .with_provider_context(context)
            .await?;
        Ok(TmContext {
            adjudicator: Some(Arc::new(adjudicator)),
        })
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    Err(anyhow::Error::new(PipelineError::ConfigError(
        "semantic_extractor_unavailable: truth maintenance binding requires meerkat-extraction"
            .to_string(),
    )))
}

fn is_obsolete_tm_commit_error(err: &PipelineError) -> bool {
    match err {
        PipelineError::NotFound(message) => {
            message.contains("Claim not found")
                || message.contains("Winner claim not found")
                || message.contains("Event claim not found")
        }
        PipelineError::StorageError(StorageError::NotFound(message)) => {
            message.contains("rel:") || message.contains("attribute:")
        }
        _ => false,
    }
}

fn is_obsolete_tm_resolve_error(err: &PipelineError) -> bool {
    match err {
        PipelineError::NotFound(message) => {
            message.contains("No claims found") || message.contains("No claims to resolve")
        }
        PipelineError::StorageError(StorageError::NotFound(message)) => {
            message.contains("rel:") || message.contains("attribute:")
        }
        _ => false,
    }
}

fn is_obsolete_extraction_reference_error(err: &PipelineError) -> bool {
    match err {
        PipelineError::NotFound(message)
        | PipelineError::StorageError(StorageError::NotFound(message)) => {
            message.starts_with("doc_revision:") || message.starts_with("doc_node:")
        }
        _ => false,
    }
}

#[derive(Debug, Deserialize)]
struct RenderDocPayload {
    doc_id: String,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    ingest_mode: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceLifecyclePayload {
    source_id: String,
    #[serde(default)]
    policy_version: Option<String>,
    #[serde(default)]
    target_mode: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourceWatchPayload {
    watch_id: String,
}

#[derive(Debug, Deserialize)]
struct PlanExtractionPayload {
    #[serde(
        alias = "revision_id",
        alias = "doc_revision",
        alias = "doc_revision_id",
        alias = "revision"
    )]
    revision_id: String,
    #[serde(default)]
    policy_version: Option<String>,
    #[serde(default)]
    extractor_id: Option<String>,
    #[serde(default)]
    allow_raw_quotes: bool,
    #[serde(default)]
    ingest_mode: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExtractAssertionsPayload {
    #[serde(alias = "node_id", alias = "doc_node", alias = "doc_node_id")]
    node_id: String,
    #[serde(
        alias = "revision_id",
        alias = "doc_revision",
        alias = "doc_revision_id",
        alias = "revision"
    )]
    revision_id: String,
    #[serde(default)]
    extractor_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    allow_raw_quotes: bool,
    #[serde(default)]
    #[allow(dead_code)]
    ingest_mode: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    mode: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    /// Byte span override for batched extraction (start, end).
    #[serde(default)]
    span_start: Option<u32>,
    #[serde(default)]
    span_end: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ReprocessJobPayload {
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct WorkRunPayload {
    work_run_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ArtifactWorkflowPayload {
    work_run_id: String,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    ingest_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DetectConflictsPayload {
    event_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitTruthPayload {
    resolution: ResolutionResult,
    input: CommitInput,
}

#[derive(Debug, Deserialize)]
struct ResolveSlotPayload {
    conflict_group_id: String,
}

#[derive(Debug, Deserialize)]
struct RecomputePayload {
    #[serde(default)]
    priority: Option<String>,
    #[serde(default = "default_recompute_max_slots")]
    max_slots: usize,
}

const fn default_recompute_max_slots() -> usize {
    100
}

fn parse_recompute_priority(value: Option<&str>) -> RecomputePriority {
    match value.unwrap_or("normal") {
        "high" => RecomputePriority::High,
        "urgent" => RecomputePriority::Urgent,
        _ => RecomputePriority::Normal,
    }
}

#[derive(Debug, Default)]
struct BudgetEstimates {
    ingest_mode: IngestMode,
    estimated_cost_euros: f64,
    estimated_tokens: u32,
    estimated_wall_time_secs: u32,
}

fn budget_estimates_from_payload(payload: &serde_json::Value) -> BudgetEstimates {
    let mut estimates = BudgetEstimates {
        ingest_mode: IngestMode::default(),
        ..BudgetEstimates::default()
    };

    let obj = payload.as_object();
    if let Some(obj) = obj {
        if let Some(mode_val) = obj
            .get("ingest_mode")
            .or_else(|| obj.get("mode"))
            .and_then(|v| v.as_str())
        {
            if let Ok(mode) = IngestMode::from_str(mode_val) {
                estimates.ingest_mode = mode;
            }
        }

        if let Some(cost_val) = obj
            .get("estimated_cost_euros")
            .or_else(|| obj.get("estimated_cost"))
            .and_then(serde_json::Value::as_f64)
        {
            estimates.estimated_cost_euros = cost_val;
        }

        if let Some(tokens_val) = obj
            .get("estimated_tokens")
            .and_then(serde_json::Value::as_u64)
        {
            estimates.estimated_tokens = u32::try_from(tokens_val).unwrap_or(u32::MAX);
        }

        if let Some(wall_val) = obj
            .get("estimated_wall_time_secs")
            .and_then(serde_json::Value::as_u64)
        {
            estimates.estimated_wall_time_secs = u32::try_from(wall_val).unwrap_or(u32::MAX);
        }
    }

    estimates
}

fn estimate_cost_from_tokens(tokens: u32) -> f64 {
    f64::from(tokens) * 0.000_001
}

fn parse_optional_ingest_mode(value: Option<&str>, default: IngestMode) -> IngestMode {
    value
        .and_then(|raw| IngestMode::from_str(raw).ok())
        .unwrap_or(default)
}

const fn source_has_reached_ingest_mode(source: &Source, mode: IngestMode) -> bool {
    matches!(
        (mode, source.lifecycle_status),
        (IngestMode::Skip, SourceLifecycleStatus::Skipped)
            | (
                IngestMode::Metadata,
                SourceLifecycleStatus::MetadataReady
                    | SourceLifecycleStatus::RetrievalReady
                    | SourceLifecycleStatus::GraphReady
            )
            | (
                IngestMode::Retrieval,
                SourceLifecycleStatus::RetrievalReady | SourceLifecycleStatus::GraphReady
            )
            | (IngestMode::Graph, SourceLifecycleStatus::GraphReady)
    )
}

async fn load_source_record(
    storage: &StorageClient,
    space_id: &str,
    source_id: &RecordId,
) -> anyhow::Result<Source> {
    SourceRepository::new(storage.clone())
        .get(space_id, source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Source not found: {source_id}"))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn update_source_record<F>(
    storage: &StorageClient,
    space_id: &str,
    source_id: &RecordId,
    mutator: F,
) -> anyhow::Result<Source>
where
    F: FnOnce(&mut Source),
{
    let repo = SourceRepository::new(storage.clone());
    let mut source = repo
        .get(space_id, source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Source not found: {source_id}"))?;
    mutator(&mut source);
    let updated_source = repo.update(space_id, source_id, source).await?;
    Ok(updated_source)
}

async fn materialize_doc_from_source(
    storage: &StorageClient,
    source: &Source,
    actor: &str,
) -> anyhow::Result<Doc> {
    let doc_repo = DocRepository::new(storage.clone());
    let Some(template) = source
        .manual_doc_template()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?
    else {
        let mut linked_docs = doc_repo
            .find_by_source(&source.base.space_id, &source.base.id, 1)
            .await?;
        let Some(mut linked_doc) = linked_docs.pop() else {
            return Err(anyhow::anyhow!(
                "Source {} has no staged manual doc or linked doc",
                source.base.id
            ));
        };

        linked_doc.base.updated_at = chrono::Utc::now();
        linked_doc.base.security = source.base.security.clone();
        linked_doc.base.provenance = source.base.provenance.clone();
        linked_doc.base.subjects.clone_from(&source.base.subjects);
        linked_doc.base.subjects_state = source.base.subjects_state;
        linked_doc.source = Some(Link::new(source.base.id.clone()));
        let linked_doc_id = linked_doc.base.id.clone();
        return doc_repo
            .update(&source.base.space_id, &linked_doc_id, linked_doc)
            .await
            .map_err(Into::into);
    };

    let doc_id = RecordId::from_str(&template.doc_id)
        .map_err(|e| anyhow::anyhow!("Invalid staged doc_id {}: {e}", template.doc_id))?;

    if let Some(existing) = doc_repo
        .get_with_deleted(&source.base.space_id, &doc_id)
        .await?
    {
        let mut updated = existing;
        updated.base.updated_at = chrono::Utc::now();
        updated.base.security = source.base.security.clone();
        updated.base.provenance = source.base.provenance.clone();
        updated.base.subjects.clone_from(&source.base.subjects);
        updated.base.subjects_state = source.base.subjects_state;
        updated.title = template.title;
        updated.text = template.text;
        updated.sidecar_text = template.sidecar_text;
        updated.media_attachments = template.media_attachments;
        updated.source = Some(Link::new(source.base.id.clone()));
        updated.rendition_key = template.rendition_key;
        updated.metadata = template.metadata;
        return if updated.base.deleted_at.is_some() {
            doc_repo
                .restore_and_replace_existing(&source.base.space_id, &doc_id, updated)
                .await
                .map_err(Into::into)
        } else {
            doc_repo
                .update(&source.base.space_id, &doc_id, updated)
                .await
                .map_err(Into::into)
        };
    }

    let mut base = BaseFields::new(doc_id, source.base.space_id.clone(), actor.to_string());
    base.security = source.base.security.clone();
    base.provenance = source.base.provenance.clone();
    base.subjects.clone_from(&source.base.subjects);
    base.subjects_state = source.base.subjects_state;

    let doc = Doc {
        base,
        title: template.title,
        text: template.text,
        sidecar_text: template.sidecar_text,
        media_attachments: template.media_attachments,
        source: Some(Link::new(source.base.id.clone())),
        rendition_key: template.rendition_key,
        metadata: template.metadata,
    };

    doc_repo.create(doc).await.map_err(Into::into)
}

async fn derive_source_id_for_revision(
    storage: &StorageClient,
    space_id: &str,
    revision_id: &RecordId,
) -> anyhow::Result<Option<String>> {
    let revision = DocRevisionRepository::new(storage.clone())
        .get(space_id, revision_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Doc revision not found: {revision_id}"))?;
    let doc = DocRepository::new(storage.clone())
        .get(space_id, revision.doc.id())
        .await?;

    Ok(doc.and_then(|doc| doc.source.map(|source| source.id().to_string())))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn mark_source_graph_ready_if_quiescent(
    storage: &StorageClient,
    space_id: &str,
    source_id: &RecordId,
    current_work_item_id: &RecordId,
) -> anyhow::Result<()> {
    let source_repo = SourceRepository::new(storage.clone());
    let _updated = source_repo
        .mark_graph_ready_if_quiescent(
            space_id,
            source_id,
            current_work_item_id,
            &[
                RENDER_DOC_KIND,
                PLAN_EXTRACTION_KIND,
                EXTRACT_ASSERTIONS_KIND,
                VALIDATE_ARTIFACTS_KIND,
                COMMIT_ARTIFACTS_KIND,
            ],
        )
        .await?;
    Ok(())
}

fn extraction_artifact_data(
    artifact_data: &serde_json::Value,
) -> Result<ExtractionArtifactData, PipelineError> {
    serde_json::from_value(artifact_data.clone()).map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to deserialize knowledge_candidates artifact: {e}"
        ))
    })
}

fn extraction_artifact_raw_assertions(
    data: &ExtractionArtifactData,
) -> Result<
    (
        RecordId,
        RecordId,
        Vec<elephant_pipeline::extraction::RawAssertion>,
    ),
    PipelineError,
> {
    let revision_id = RecordId::from_str(&data.revision_id).map_err(|e| {
        PipelineError::InvalidInput(format!("Invalid revision_id {}: {e}", data.revision_id))
    })?;
    let node_id = RecordId::from_str(&data.node_id).map_err(|e| {
        PipelineError::InvalidInput(format!("Invalid node_id {}: {e}", data.node_id))
    })?;
    let assertions = data
        .assertions
        .iter()
        .map(|assertion| assertion.to_raw(&revision_id, &node_id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((revision_id, node_id, assertions))
}

fn commit_bundle_payload(
    artifact: &elephant_types::records::work::WorkArtifact,
) -> Result<CommitBundleArtifactData, PipelineError> {
    serde_json::from_value(artifact.data.clone()).map_err(|e| {
        PipelineError::ConfigError(format!("Failed to deserialize commit_bundle artifact: {e}"))
    })
}

fn semantic_bundle_kind(artifact_type: &str) -> Option<CommitBundleKind> {
    match artifact_type {
        SOURCE_METADATA_ARTIFACT_TYPE => Some(CommitBundleKind::SourceMetadata),
        KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE | LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE => {
            Some(CommitBundleKind::KnowledgeCandidates)
        }
        elephant_pipeline::resolution::RESOLUTION_PATCH_ARTIFACT_TYPE => {
            Some(CommitBundleKind::ResolutionPatchSet)
        }
        _ => None,
    }
}

fn build_event_lineage_key(
    source_scope: &str,
    raw: &elephant_pipeline::extraction::RawAssertion,
) -> String {
    use sha2::{Digest, Sha256};

    let normalized_subject = raw.subject_text.trim().to_ascii_lowercase();
    let normalized_predicate = raw.predicate_text.trim().to_ascii_lowercase();
    let normalized_object = raw
        .object_text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .or_else(|| {
            raw.object_literal
                .as_ref()
                .map(normalized_literal_for_event_lineage)
        })
        .unwrap_or_default();
    let normalized_title = raw
        .event_title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(source_scope.as_bytes());
    hasher.update(b"|");
    hasher.update(normalized_subject.as_bytes());
    hasher.update(b"|");
    hasher.update(normalized_predicate.as_bytes());
    hasher.update(b"|");
    hasher.update(normalized_object.as_bytes());
    hasher.update(b"|");
    hasher.update(normalized_title.as_bytes());
    format!("source:{}:{}", source_scope, hex::encode(hasher.finalize()))
}

fn normalized_literal_for_event_lineage(
    literal: &elephant_types::records::attribute::Literal,
) -> String {
    use elephant_types::records::attribute::Literal;

    match literal {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => value.trim().to_ascii_lowercase(),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => format!("{value}"),
        Literal::Boolean(value) => value.to_string(),
        Literal::Json(value) => value.to_string(),
    }
}

async fn enqueue_work_successor(
    storage: &StorageClient,
    parent: &WorkItem,
    mut child: WorkItem,
) -> anyhow::Result<WorkItem> {
    if parent.kind.starts_with("tm_") || parent.kind == WORK_KIND_EXECUTE_REPROCESS_JOB {
        let run = original_execution_run(storage, parent).await?;
        child.base.security.clone_from(&parent.base.security);
        child.base.subjects.clone_from(&parent.base.subjects);
        child.base.subjects_state = parent.base.subjects_state;
        child.base.provenance.clone_from(&parent.base.provenance);
        return Ok(
            elephant_storage::work_producer_repository::WorkProducerRepository::new(
                storage.clone(),
            )
            .enqueue_successor(parent, &run, child, true)
            .await?,
        );
    }
    inherit_source_context(parent, &mut child)?;
    child.payload[source_runtime::PREDECESSOR] = serde_json::json!(parent.base.id.to_string());
    if let Some(ids) = parent.payload.get(source_runtime::METADATA_IDS) {
        if child.payload.get(source_runtime::METADATA_IDS).is_none() {
            child.payload[source_runtime::METADATA_IDS] = ids.clone();
        }
    }
    if parent.kind == TRIAGE_SOURCE_KIND
        && child.kind == INGEST_METADATA_KIND
        && elephant_pipeline::source_queue::publication_from_work(parent)?.is_some()
    {
        let run = original_execution_run(storage, parent).await?;
        return Ok(
            elephant_storage::work_producer_repository::WorkProducerRepository::new(
                storage.clone(),
            )
            .enqueue_source_metadata(parent, &run, child)
            .await?,
        );
    }
    Ok(enqueue_idempotent(storage, child).await?)
}

/// Locate the actual receipt for the captured original claim, never a latest run.
async fn original_execution_run(
    storage: &StorageClient,
    parent: &WorkItem,
) -> anyhow::Result<elephant_types::records::WorkRun> {
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(parent)?;
    let runs = WorkRunRepository::new(storage.clone())
        .find_by_work_item(&parent.base.id)
        .await?;
    let mut matching = runs.into_iter().filter(|run| {
        run.base.space_id == parent.base.space_id
            && run
                .metrics
                .custom
                .get("attempt_fence")
                .and_then(|value| {
                    serde_json::from_value::<elephant_types::publication::AttemptFence>(
                        value.clone(),
                    )
                    .ok()
                })
                .as_ref()
                == Some(&fence)
    });
    let run = matching
        .next()
        .ok_or_else(|| anyhow::anyhow!("Original attempt execution receipt missing"))?;
    if matching.next().is_some() {
        anyhow::bail!("Original attempt has ambiguous execution receipts");
    }
    Ok(run)
}

/// Group-derived children keep the canonical group's complete public authority.
/// The original coordinator/run restriction is stored only in private evidence.
async fn enqueue_group_resolution(
    storage: &StorageClient,
    parent: &WorkItem,
    work_run: &RecordId,
    group_id: &RecordId,
    wait: bool,
) -> anyhow::Result<WorkItem> {
    let group = ConflictGroupRepository::new(storage.clone())
        .get(&parent.base.space_id, group_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Original conflict group missing"))?;
    let run = original_execution_run(storage, parent).await?;
    if &run.base.id != work_run {
        anyhow::bail!("Conflict child uses a different original run");
    }
    let mut base = group.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    let child = WorkItem {
        base,
        kind: TM_RESOLVE_SLOT_KIND.into(),
        payload: serde_json::json!({"conflict_group_id": group_id.to_string()}),
        status: WorkItemStatus::Runnable,
        priority: successor_priority(parent.priority),
        idempotency_key: format!("tm_resolve_slot:{group_id}:{work_run}"),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    Ok(
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone())
            .enqueue_group_child(parent, &run, &group, child, wait)
            .await?,
    )
}

/// Coordinator continuations can run while the original job awaits their outcome.
async fn enqueue_reprocess_resolution(
    storage: &StorageClient,
    parent: &WorkItem,
    work_run: &RecordId,
    group_id: &RecordId,
) -> anyhow::Result<WorkItem> {
    enqueue_group_resolution(storage, parent, work_run, group_id, false).await
}

async fn commit_source_metadata_bundle(
    storage: &StorageClient,
    space_id: &str,
    metadata: &SourceMetadataArtifactData,
) -> Result<(), PipelineError> {
    let metadata_value = serde_json::to_value(metadata).map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to serialize normalized source metadata for {}: {e}",
            metadata.source_id
        ))
    })?;
    let source_id = RecordId::from_str(&metadata.source_id).map_err(|e| {
        PipelineError::InvalidInput(format!("Invalid source_id {}: {e}", metadata.source_id))
    })?;
    update_source_record(storage, space_id, &source_id, |source| {
        source.lifecycle_status = SourceLifecycleStatus::MetadataReady;
        source.current_ingest_mode = Some(IngestMode::Metadata);
        source.author.clone_from(&metadata.normalized_author);
        source.channel.clone_from(&metadata.normalized_channel);
        source.timestamp = metadata.normalized_timestamp;
        source.uri.clone_from(&metadata.normalized_uri);
        source.external_id = metadata.normalized_external_ids.first().cloned();
        if !source.metadata.is_object() {
            source.metadata = serde_json::json!({});
        }
        let Some(metadata_object) = source.metadata.as_object_mut() else {
            return;
        };
        metadata_object.insert(
            SOURCE_NORMALIZED_METADATA_KEY.to_string(),
            metadata_value.clone(),
        );
        if let Some(title) = metadata.normalized_title.as_ref() {
            metadata_object
                .entry("title".to_string())
                .or_insert_with(|| serde_json::Value::String(title.clone()));
        }
    })
    .await
    .map_err(|e| {
        PipelineError::ConfigError(format!(
            "Failed to commit source metadata for {source_id}: {e}"
        ))
    })?;
    Ok(())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn enqueue_upgrade_after_metadata_commit(
    storage: &StorageClient,
    item: &WorkItem,
    source_id: &RecordId,
    target_mode: IngestMode,
) -> anyhow::Result<()> {
    if target_mode == IngestMode::Metadata {
        return Ok(());
    }

    let work_item_repo = WorkItemRepository::new(storage.clone());
    let upgrade_kind = UPGRADE_INGEST_MODE_KIND;
    let idempotency_key = format!("{upgrade_kind}:{source_id}:{target_mode}");
    if let Some(mut existing) = work_item_repo
        .find_by_idempotency_key(&item.base.space_id, &idempotency_key)
        .await?
    {
        if matches!(
            existing.status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            let existing_id = existing.base.id.clone();
            existing.base.security = item.base.security.clone();
            existing.base.provenance = item.base.provenance.clone();
            existing.base.subjects.clone_from(&item.base.subjects);
            existing.base.subjects_state = item.base.subjects_state;
            existing.status = WorkItemStatus::Runnable;
            existing.priority = successor_priority(item.priority);
            existing.attempts = 0;
            existing.lease_expires_at = None;
            existing.leased_by = None;
            existing.run_after = None;
            existing.last_error = None;
            existing.base.updated_at = Utc::now();
            let _ = work_item_repo
                .update(&item.base.space_id, &existing_id, existing)
                .await?;
        }
        return Ok(());
    }

    let upgrade_item = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            item.base.space_id.clone(),
            item.base.created_by.clone(),
        ),
        kind: upgrade_kind.to_string(),
        payload: serde_json::json!({
            "source_id": source_id.to_string(),
            "target_mode": target_mode.to_string(),
            "reason": "metadata_committed"
        }),
        status: WorkItemStatus::Runnable,
        priority: successor_priority(item.priority),
        idempotency_key,
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    enqueue_work_successor(storage, item, upgrade_item).await?;
    Ok(())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn enqueue_render_doc_work_item(
    storage: &StorageClient,
    item: &WorkItem,
    source_id: &RecordId,
    doc: &Doc,
    ingest_mode: IngestMode,
) -> anyhow::Result<()> {
    let work_item_repo = WorkItemRepository::new(storage.clone());
    let idempotency_key = format!("{RENDER_DOC_KIND}:{}:{}", doc.base.id, doc.rendition_key);
    if let Some(mut existing) = work_item_repo
        .find_by_idempotency_key(&item.base.space_id, &idempotency_key)
        .await?
    {
        if matches!(
            existing.status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            let existing_id = existing.base.id.clone();
            existing.base.security = item.base.security.clone();
            existing.base.provenance = item.base.provenance.clone();
            existing.base.subjects.clone_from(&item.base.subjects);
            existing.base.subjects_state = item.base.subjects_state;
            existing.status = WorkItemStatus::Runnable;
            existing.priority = successor_priority(item.priority);
            existing.attempts = 0;
            existing.lease_expires_at = None;
            existing.leased_by = None;
            existing.run_after = None;
            existing.last_error = None;
            existing.base.updated_at = Utc::now();
            existing.payload = serde_json::json!({
                "doc_id": doc.base.id.to_string(),
                "source_id": source_id.to_string(),
                "ingest_mode": ingest_mode.to_string(),
            });
            let _ = work_item_repo
                .update(&item.base.space_id, &existing_id, existing)
                .await?;
        }
        return Ok(());
    }

    let render_item = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            item.base.space_id.clone(),
            item.base.created_by.clone(),
        ),
        kind: RENDER_DOC_KIND.to_string(),
        payload: serde_json::json!({
            "doc_id": doc.base.id.to_string(),
            "source_id": source_id.to_string(),
            "ingest_mode": ingest_mode.to_string(),
        }),
        status: WorkItemStatus::Runnable,
        priority: successor_priority(item.priority),
        idempotency_key,
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    enqueue_work_successor(storage, item, render_item).await?;
    Ok(())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn source_promotion_options(
    storage: &StorageClient,
    space_id: &str,
) -> Result<elephant_pipeline::scheduler::PromotionOptions, PipelineError> {
    let context =
        ProviderDispatchContext::new(SpaceConfigRepository::new(storage.clone()), space_id);
    let resolved = context
        .repository
        .resolve_space(space_id)
        .await
        .map_err(|error| PipelineError::ConfigError(format!("provider_authorization: {error}")))?;
    let mut promotion_options = elephant_pipeline::scheduler::PromotionOptions::default();
    if resolved
        .processing
        .bindings
        .contains_key(&ModelOperation::PromotionDisambiguation)
    {
        #[cfg(feature = "meerkat-extraction")]
        {
            use elephant_pipeline::scheduler::promotion_disambiguation::{
                AgenticPromotionDisambiguator, PromotionDisambiguationConfig,
            };
            promotion_options.disambiguator = Some(Arc::new(
                AgenticPromotionDisambiguator::new(PromotionDisambiguationConfig::default())
                    .with_provider_context(context)
                    .await?,
            ));
        }
        #[cfg(not(feature = "meerkat-extraction"))]
        return Err(PipelineError::ConfigError(
            "semantic_extractor_unavailable: promotion binding requires meerkat-extraction"
                .to_string(),
        ));
    }
    Ok(promotion_options)
}

async fn commit_knowledge_candidates_bundle(
    storage: &StorageClient,
    space_id: &str,
    run_id: &RecordId,
    actor: &str,
    data: &ExtractionArtifactData,
) -> Result<(), PipelineError> {
    let (revision_id, node_id, mut assertions) = extraction_artifact_raw_assertions(data)?;
    if let Some(source_scope) = data.source_id.as_deref() {
        for assertion in &mut assertions {
            if assertion.event_lineage_key.is_none() {
                assertion.event_lineage_key =
                    Some(build_event_lineage_key(source_scope, assertion));
            }
        }
    }
    let promotion_options = source_promotion_options(storage, space_id).await?;
    promote_assertions_inline_with_options(
        storage,
        space_id,
        &assertions,
        run_id,
        &node_id,
        &revision_id,
        actor,
        &promotion_options,
    )
    .await?;
    Ok(())
}

fn load_budget_from_env() -> Budget {
    let mut budget = Budget::default();

    if let Ok(val) = std::env::var("PIPELINE_BUDGET_DAILY_ITEMS_GRAPH") {
        if let Ok(parsed) = val.parse::<u32>() {
            budget.daily_items_graph = Some(parsed);
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_DAILY_ITEMS_RETRIEVAL") {
        if let Ok(parsed) = val.parse::<u32>() {
            budget.daily_items_retrieval = Some(parsed);
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_DAILY_EUROS_GRAPH") {
        if let Ok(parsed) = val.parse::<f64>() {
            budget.daily_euros_graph = parsed;
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_DAILY_EUROS_RETRIEVAL") {
        if let Ok(parsed) = val.parse::<f64>() {
            budget.daily_euros_retrieval = parsed;
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_PER_ITEM_TOKENS") {
        if let Ok(parsed) = val.parse::<u32>() {
            budget.per_item_tokens = Some(parsed);
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_PER_ITEM_WALL_TIME_SECS") {
        if let Ok(parsed) = val.parse::<u32>() {
            budget.per_item_wall_time_secs = Some(parsed);
        }
    }
    if let Ok(val) = std::env::var("PIPELINE_BUDGET_DOWNGRADE_POLICY") {
        if let Ok(parsed) = val.parse::<DowngradePolicy>() {
            budget.downgrade_policy = parsed;
        }
    }

    budget
}

/// Priority for work items enqueued as the next pipeline stage of a parent.
///
/// Successor items are created at parent completion, which places them at the
/// back of the `created_at` FIFO within their priority band; one extra
/// priority point per stage lets in-flight documents finish ahead of
/// same-priority new arrivals instead of re-waiting the whole backlog at
/// every stage.
const fn successor_priority(parent_priority: i32) -> i32 {
    parent_priority.saturating_add(1)
}

#[allow(clippy::large_stack_frames)]
#[allow(clippy::too_many_arguments)]
async fn run_on_dedicated_stack<F, T>(
    thread_name: &str,
    stack_bytes: usize,
    future: F,
) -> anyhow::Result<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    let dispatch_scope = elephant_pipeline::provider_dispatch::capture_dispatch_scope();
    let (mut sender, receiver) = tokio::sync::oneshot::channel();
    let thread_name = thread_name.to_string();
    std::thread::Builder::new()
        .name(thread_name)
        .stack_size(stack_bytes)
        .spawn(move || {
            let output = runtime.block_on(dispatch_scope.run(async {
                tokio::select! {
                    biased;
                    () = sender.closed() => None,
                    output = future => Some(output),
                }
            }));
            if let Some(output) = output {
                let _ = sender.send(output);
            }
        })
        .map_err(|error| anyhow::anyhow!("Could not start dedicated worker thread: {error}"))?;
    receiver
        .await
        .map_err(|error| anyhow::anyhow!("Dedicated worker thread exited early: {error}"))
}

#[cfg(feature = "meerkat-extraction")]
#[expect(
    clippy::large_futures,
    clippy::large_stack_frames,
    reason = "Keep the dedicated-stack wiki execution and its original future layout"
)]
async fn execute_managed_wiki_work_item(
    item: &WorkItem,
    work_run_id: &RecordId,
    storage: StorageClient,
    payload: ManagedWikiPayload,
) -> anyhow::Result<WorkRunMetrics> {
    const MANAGED_WIKI_STACK_BYTES: usize = 32 * 1024 * 1024;
    let reconciliation_storage = storage.clone();
    let reconciliation_space = item.base.space_id.clone();
    let reconciliation_payload = payload.clone();
    let original_attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
    let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
        storage.clone(),
        work_run_id,
        original_attempt.clone(),
    );
    let result = run_on_dedicated_stack(
        "elephant-living-wiki",
        MANAGED_WIKI_STACK_BYTES,
        async move {
            let activity_client = reconciliation_storage.clone();
            activity_client
                .with_activity(
                    elephant_pipeline::provider_dispatch::with_dispatch_observer(
                        observer,
                        execute_managed_wiki_reconciliation_with_attempt(
                            reconciliation_storage,
                            &reconciliation_space,
                            &reconciliation_payload,
                            original_attempt,
                        ),
                    ),
                )
                .await
                .map_err(PipelineError::from)?
        },
    )
    .await?
    .map_err(|error| anyhow::Error::new(error).context("Managed wiki reconciliation failed"))?;
    let mut next_payload = payload;
    next_payload.last_graph_fingerprint = (result.failed_articles == 0
        && result.partition_failures.is_empty()
        && result.reconciliation_errors.is_empty())
    .then(|| result.graph_fingerprint.clone());
    let next_run = Utc::now() + Duration::minutes(i64::from(next_payload.refresh_interval_minutes));
    if let Err(error) = enqueue_managed_wiki_successor(
        &storage,
        &item.base.space_id,
        &item.base.created_by,
        &item.base.id,
        &next_payload,
        next_run,
    )
    .await
    {
        if next_payload
            .authority
            .is_active(&storage, &item.base.space_id)
            .await?
        {
            return Err(anyhow::anyhow!(
                "Failed to schedule next managed wiki reconciliation: {error}"
            ));
        }
    }
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "wiki_graph_changed".to_string(),
        serde_json::json!(result.changed),
    );
    metrics.custom.insert(
        "wiki_selected_articles".to_string(),
        serde_json::json!(result.selected_articles),
    );
    metrics.custom.insert(
        "wiki_compiled_articles".to_string(),
        serde_json::json!(result.compiled_articles),
    );
    metrics.custom.insert(
        "wiki_failed_articles".to_string(),
        serde_json::json!(result.failed_articles),
    );
    metrics.custom.insert(
        "wiki_hidden_articles".to_string(),
        serde_json::json!(result.hidden_articles),
    );
    metrics.custom.insert(
        "wiki_partition_failures".to_string(),
        serde_json::to_value(&result.partition_failures)?,
    );
    metrics.custom.insert(
        "wiki_reconciliation_errors".to_string(),
        serde_json::to_value(&result.reconciliation_errors)?,
    );
    Ok(metrics)
}

// Large branch futures are boxed at their await boundaries below. A semaphore
// limits concurrent work, but cannot bound the stack storage of one dispatcher.
// The graph-scale Living Wiki arm also uses its dedicated execution stack.
#[allow(clippy::too_many_arguments, clippy::large_stack_frames)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn execute_work_item(
    item: &WorkItem,
    work_run_id: &RecordId,
    storage: StorageClient,
    config: &WorkQueueWorkerConfig,
    _space_config_repo: &SpaceConfigRepository,
    _provider_cache: &EmbeddingProviderCache,
    _embed_config: &EmbedWorkerConfig,
    tm_ctx: &TmContext,
) -> anyhow::Result<WorkRunMetrics> {
    let context = ProviderDispatchContext::new(
        SpaceConfigRepository::new(storage.clone()),
        item.base.space_id.clone(),
    );
    context
        .repository
        .resolve_space(&item.base.space_id)
        .await?;
    let resolved_tm;
    let tm_ctx = if matches!(item.kind.as_str(), TM_RESOLVE_SLOT_KIND | TM_RECOMPUTE_KIND)
        && tm_ctx.adjudicator.is_none()
    {
        resolved_tm = build_tm_context_for_space(context).await?;
        &resolved_tm
    } else {
        tm_ctx
    };
    Box::pin(execute_work_item_with_extractor(
        item,
        work_run_id,
        storage,
        config,
        tm_ctx,
        None,
    ))
    .await
}

/// Executes the real work-kind dispatcher with a caller-supplied extractor.
///
/// Only exposed for integration tests; production workers construct their
/// semantic extractor through the configured runtime path.
///
/// # Errors
///
/// Returns the dispatch error if work execution or persistence fails.
#[cfg(feature = "test-utils")]
#[allow(clippy::large_stack_frames)]
pub async fn execute_work_item_for_test(
    item: &WorkItem,
    work_run_id: &RecordId,
    storage: StorageClient,
    extractor: &dyn Extractor,
) -> anyhow::Result<WorkRunMetrics> {
    eprintln!(
        "test dispatch begin kind={} work={}",
        item.kind, item.base.id
    );
    let result = Box::pin(execute_work_item_with_extractor(
        item,
        work_run_id,
        storage,
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        Some(extractor),
    ))
    .await;
    eprintln!(
        "test dispatch end kind={} work={} success={}",
        item.kind,
        item.base.id,
        result.is_ok()
    );
    result
}

// Tests scope only the existing network policy so the real dispatcher can
// exercise a paused loopback fetch. There is no runtime configuration bypass.
#[cfg(test)]
tokio::task_local! {
    static WATCH_FETCH_POLICY: elephant_pipeline::remote_source::RemoteFetchPolicy;
}

fn watch_service(storage: StorageClient) -> SourceWatchService {
    #[cfg(test)]
    if let Ok(policy) = WATCH_FETCH_POLICY.try_with(|policy| *policy) {
        return SourceWatchService::with_fetch_policy(storage, policy);
    }
    SourceWatchService::new(storage)
}

// Construct only the selected branch outside the dispatcher poll frame.
// Its body remains lazy and is polled in the caller's original task scopes.
#[inline(never)]
fn boxed_work_item_phase<F: std::future::Future>(
    make: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

#[allow(clippy::too_many_lines, clippy::large_stack_frames)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn execute_work_item_with_extractor(
    item: &WorkItem,
    work_run_id: &RecordId,
    storage: StorageClient,
    config: &WorkQueueWorkerConfig,
    tm_ctx: &TmContext,
    extractor_override: Option<&dyn Extractor>,
) -> anyhow::Result<WorkRunMetrics> {
    let space_id = &item.base.space_id;
    let accepted_source = source_runtime::accepted_source(&storage, item).await?;
    let publication = elephant_pipeline::source_queue::publication_from_work(item)?;
    let mut metrics = WorkRunMetrics::default();
    if source_runtime::begin_operation(&storage, item).await? {
        metrics.custom.insert(
            "source_dispatch_short_circuit".into(),
            serde_json::json!(true),
        );
        return Ok(metrics);
    }
    // Check original configuration before predecessor readiness, so an obsolete
    // chain can recover even when its old predecessor has been cancelled.
    source_runtime::require_predecessor(&storage, item).await?;
    // Each work kind has its own poll function. A single async match can have
    // a small stored future but a very large poll stack from temporary values
    // in all of its branches, including branches this invocation never uses.
    match item.kind.as_str() {
        elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND => boxed_work_item_phase(|| async move {
            let publication = publication
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Finalizer lacks original source fence"))?;
            let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
            let manifest = elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(storage.clone())
                .get(space_id, publication).await?.ok_or_else(|| WorkFailure {
                    class: FailureClass::PendingDependencies, message: "Source finalizer manifest missing".into(),
                })?;
            let options = if manifest.mode == IngestMode::Graph {
                source_promotion_options(&storage, space_id).await?
            } else {
                elephant_pipeline::scheduler::PromotionOptions::default()
            };
            let receipt = Box::pin(
                elephant_pipeline::source_finalization::execute_finalizer(
                    &storage,
                    space_id,
                    publication,
                    &attempt,
                    &item.base.created_by,
                    &options,
                ),
            )
            .await?;
            metrics.custom.insert(
                "canonical_commit_id".to_string(),
                serde_json::json!(receipt.commit_id),
            );
            Ok(metrics)
        }).await,
        "embed_source_revision" => boxed_work_item_phase(|| async move {
            Box::pin(source_runtime::execute_embeddings(&storage, item)).await
        }).await,
        "reindex_space_embeddings" => boxed_work_item_phase(|| async move {
            let configs = SpaceConfigRepository::new(storage.clone());
            let current = configs
                .get_by_space_id(space_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Reindex space missing"))?;
            let expected = item
                .payload
                .get("reindex_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("Reindex work lacks original accepted ID"))?;
            if current
                .processing
                .as_ref()
                .and_then(|processing| processing.pending_reindex.as_ref())
                .map(|pending| pending.id.as_str())
                != Some(expected)
            {
                anyhow::bail!("Reindex was cancelled or superseded");
            }
            let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
            let progress = Box::pin(
                elephant_pipeline::embedding_reindex::process_embedding_reindex_with_attempt(
                    &storage, space_id, 128, expected, &attempt,
                ),
            )
            .await?;
            metrics
                .custom
                .insert("reindex".to_string(), serde_json::to_value(&progress)?);
            if !progress.ready_to_activate {
                return Err(WorkFailure {
                    class: FailureClass::PendingDependencies,
                    message: "Reindex page persisted; more coverage remains".to_string(),
                }
                .into());
            }
            Ok(metrics)
        }).await,
        REFRESH_SOURCE_WATCH_KIND => boxed_work_item_phase(|| async move {
            let payload: SourceWatchPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid source-watch payload: {e}"))?;
            let watch_id = RecordId::from_str(&payload.watch_id)
                .map_err(|e| anyhow::anyhow!("Invalid source-watch id: {e}"))?;
            let watch = SourceWatchRepository::new(storage.clone())
                .get(space_id, &watch_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Source watch not found: {watch_id}"))?;
            if !matches!(
                watch.status,
                elephant_types::records::SourceWatchStatus::Enabled
                    | elephant_types::records::SourceWatchStatus::Error
            ) {
                info!(
                    watch_id = %watch_id,
                    status = %watch.status,
                    "Skipping disabled source-watch refresh"
                );
                return Ok(metrics);
            }
            let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
            let outcome = Box::pin(watch_service(storage.clone()).check_with_attempt(
                watch,
                &item.base.created_by,
                &attempt,
                work_run_id,
            ))
            .await
            .map_err(|e| anyhow::Error::new(e).context("source-watch refresh failed"))?;
            match outcome {
                elephant_pipeline::source_watch::SourceWatchAttemptOutcome::Observed(run) => {
                    info!(watch_id = %run.watch.base.id, outcome = %run.check.outcome,
                        triage_work_item = ?run.triage_work_item, "Source-watch observation recorded");
                }
                elephant_pipeline::source_watch::SourceWatchAttemptOutcome::ReceiptOnly(
                    receipt,
                ) => {
                    info!(operation_id = %receipt.operation_id, state = ?receipt.state,
                        "Original source-watch receipt replayed without new observation or queue work");
                }
            }
            Ok(metrics)
        }).await,
        INGEST_SOURCE_STUB_KIND => boxed_work_item_phase(|| async move {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid ingest_source_stub payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = match &accepted_source {
                Some(source) if source.base.id == source_id => source.clone(),
                Some(_) => anyhow::bail!("Work payload source differs from accepted revision"),
                None => load_source_record(&storage, space_id, &source_id).await?,
            };
            if publication.is_none() && source.lifecycle_status != SourceLifecycleStatus::Discovered
            {
                info!(
                    kind = INGEST_SOURCE_STUB_KIND,
                    source_id = %source_id,
                    current_status = ?source.lifecycle_status,
                    "Skipping source work item because source has already advanced"
                );
                return Ok(metrics);
            }
            let _ = execute_ingest_source_stub(source, &storage)
                .await
                .map_err(|e| anyhow::Error::new(e).context("ingest_source_stub failed"))?;
            Ok(metrics)
        }).await,
        TRIAGE_SOURCE_KIND => boxed_work_item_phase(|| async move {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid triage_source payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = match &accepted_source {
                Some(source) if source.base.id == source_id => source.clone(),
                Some(_) => anyhow::bail!("Work payload source differs from accepted revision"),
                None => load_source_record(&storage, space_id, &source_id).await?,
            };
            if publication.is_none() && source.lifecycle_status != SourceLifecycleStatus::Discovered
            {
                info!(
                    kind = TRIAGE_SOURCE_KIND,
                    source_id = %source_id,
                    current_status = ?source.lifecycle_status,
                    "Skipping source work item because source has already advanced"
                );
                return Ok(metrics);
            }
            let budget_enforcer =
                BudgetEnforcer::new(storage.clone(), load_budget_from_env(), space_id.clone());
            let triage_plugin = DefaultTriagePlugin::new();
            let decision_repo = IngestDecisionRepository::new(storage.clone());
            let policy_version = payload.policy_version.as_deref().unwrap_or("default");
            let triage_result = if publication.is_some() {
                elephant_pipeline::scheduler::ingest_work_kinds::execute_triage_source_for_work_attempt(
                &source,
                &triage_plugin,
                &budget_enforcer,
                &decision_repo,
                space_id,
                policy_version,
                &item.base.created_by,
            )
            .await
                .map_err(|e| anyhow::Error::new(e).context("triage_source failed"))?
            } else {
                execute_triage_source(
                    &source,
                    &triage_plugin,
                    &budget_enforcer,
                    &decision_repo,
                    space_id,
                    policy_version,
                    &item.base.created_by,
                )
                .await
                .map_err(|e| anyhow::Error::new(e).context("triage_source failed"))?
            };
            if publication.is_some() && triage_result.was_downgraded {
                return Err(PipelineError::BudgetExceeded(
                    triage_result.downgrade_reason.unwrap_or_else(|| "budget unavailable".into()),
                ).into());
            }

            if publication.is_some() {
                metrics.custom.insert(
                    "source_ingest_decision_id".into(),
                    serde_json::Value::String(triage_result.decision_id.to_string()),
                );
            }

            if publication.is_none() {
                let latest_decision = Link::new(triage_result.decision_id.clone());
                update_source_record(&storage, space_id, &source_id, |source| {
                    source.latest_ingest_decision = Some(latest_decision);
                    source.lifecycle_status = match triage_result.decided_mode {
                        IngestMode::Skip => SourceLifecycleStatus::Skipped,
                        _ => SourceLifecycleStatus::Triaged,
                    };
                    source.current_ingest_mode = match triage_result.decided_mode {
                        IngestMode::Skip => Some(IngestMode::Skip),
                        _ => None,
                    };
                })
                .await?;
            }
            if publication.is_some() || triage_result.decided_mode != IngestMode::Skip {
                let next_kind = INGEST_METADATA_KIND;
                let next_payload = serde_json::json!({
                    "source_id": source_id.to_string(),
                    "policy_version": policy_version,
                    "target_mode": triage_result.decided_mode.to_string(),
                    "reason": triage_result.downgrade_reason,
                });
                let next_item = WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
                    kind: next_kind.to_string(),
                    payload: next_payload,
                    status: WorkItemStatus::Runnable,
                    priority: successor_priority(item.priority),
                    idempotency_key: format!("{next_kind}:{source_id}"),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                };
                enqueue_work_successor(&storage, item, next_item).await?;
            }
            Ok(metrics)
        }).await,
        INGEST_METADATA_KIND => boxed_work_item_phase(|| async move {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid ingest_metadata payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = match &accepted_source {
                Some(source) if source.base.id == source_id => source.clone(),
                Some(_) => anyhow::bail!("Work payload source differs from accepted revision"),
                None => load_source_record(&storage, space_id, &source_id).await?,
            };
            if publication.is_none() && source.lifecycle_status != SourceLifecycleStatus::Triaged {
                info!(
                    kind = INGEST_METADATA_KIND,
                    source_id = %source_id,
                    current_status = ?source.lifecycle_status,
                    "Skipping source work item because source is not triaged"
                );
                return Ok(metrics);
            }
            let result = execute_ingest_metadata(&source, &storage)
                .await
                .map_err(|e| anyhow::Error::new(e).context("ingest_metadata failed"))?;
            let mut metadata_base = source.base.clone();
            metadata_base.id = RecordId::new_v4(TableName::WorkArtifact);
            let metadata_artifact = elephant_types::records::work::WorkArtifact {
                base: metadata_base,
                work_run: Link::new(work_run_id.clone()),
                artifact_type: SOURCE_METADATA_ARTIFACT_TYPE.to_string(),
                data: serde_json::to_value(&result.metadata_artifact).map_err(|e| {
                    anyhow::anyhow!("Failed to serialize source metadata artifact: {e}")
                })?,
                status: ArtifactStatus::Draft,
                validation_errors: None,
                validated_at: None,
                committed_at: None,
            };
            source_runtime::persist_metadata_stage(&storage, item, work_run_id, metadata_artifact)
                .await?;

            let validate_payload = ArtifactWorkflowPayload {
                work_run_id: work_run_id.to_string(),
                source_id: Some(source_id.to_string()),
                ingest_mode: payload.target_mode.clone(),
            };
            let work_item_repo = WorkItemRepository::new(storage.clone());
            let validate_idempotency_key = format!("{VALIDATE_ARTIFACTS_KIND}:{work_run_id}");
            if work_item_repo
                .find_by_idempotency_key(space_id, &validate_idempotency_key)
                .await?
                .is_none()
            {
                let validate_item = WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
                    kind: VALIDATE_ARTIFACTS_KIND.to_string(),
                    payload: serde_json::to_value(&validate_payload).map_err(|e| {
                        anyhow::anyhow!("Failed to serialize validate_artifacts payload: {e}")
                    })?,
                    status: WorkItemStatus::Runnable,
                    priority: successor_priority(item.priority),
                    idempotency_key: validate_idempotency_key,
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                };
                enqueue_work_successor(&storage, item, validate_item).await?;
            }
            Ok(metrics)
        }).await,
        UPGRADE_INGEST_MODE_KIND => boxed_work_item_phase(|| async move {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid upgrade_ingest_mode payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = match &accepted_source {
                Some(source) if source.base.id == source_id => source.clone(),
                Some(_) => anyhow::bail!("Work payload source differs from accepted revision"),
                None => load_source_record(&storage, space_id, &source_id).await?,
            };
            let target_mode =
                parse_optional_ingest_mode(payload.target_mode.as_deref(), IngestMode::Retrieval);
            if publication.is_none()
                && (!matches!(
                    source.lifecycle_status,
                    SourceLifecycleStatus::MetadataReady | SourceLifecycleStatus::RetrievalReady
                ) || source_has_reached_ingest_mode(&source, target_mode))
            {
                info!(
                    kind = UPGRADE_INGEST_MODE_KIND,
                    source_id = %source_id,
                    current_status = ?source.lifecycle_status,
                    current_mode = ?source.current_ingest_mode,
                    target_mode = ?target_mode,
                    "Skipping source upgrade because lifecycle already advanced or source is not ready"
                );
                return Ok(metrics);
            }
            let current_mode = source.current_ingest_mode.unwrap_or(IngestMode::Metadata);
            let budget_enforcer =
                BudgetEnforcer::new(storage.clone(), load_budget_from_env(), space_id.clone());
            let upgrade = if publication.is_some() {
                elephant_pipeline::scheduler::ingest_work_kinds::execute_upgrade_ingest_mode_for_work_attempt(
                &source,
                current_mode,
                target_mode,
                &budget_enforcer,
                0.0,
                payload.reason.as_deref().unwrap_or("source_upgrade"),
            )
            .await
            .map_err(|e| anyhow::Error::new(e).context("upgrade_ingest_mode failed"))?
            } else {
                execute_upgrade_ingest_mode(
                    &source,
                    current_mode,
                    target_mode,
                    &budget_enforcer,
                    0.0,
                    payload.reason.as_deref().unwrap_or("source_upgrade"),
                )
                .await
                .map_err(|e| anyhow::Error::new(e).context("upgrade_ingest_mode failed"))?
            };

            if !upgrade.allowed {
                if publication.is_some() {
                    return Err(PipelineError::BudgetExceeded(
                        upgrade.denied_reason.unwrap_or_else(|| "budget unavailable".into()),
                    ).into());
                }
                return Ok(metrics);
            }

            let doc = Box::pin(source_runtime::materialize(&storage, &source, item)).await?;
            enqueue_render_doc_work_item(&storage, item, &source_id, &doc, upgrade.new_mode)
                .await?;
            Ok(metrics)
        }).await,
        RENDER_DOC_KIND => boxed_work_item_phase(|| async move {
            let payload: RenderDocPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid render_doc payload: {e}"))?;
            if payload.doc_id.trim().is_empty() {
                return Err(anyhow::anyhow!("render_doc requires doc_id payload"));
            }

            let doc_id = RecordId::from_str(&payload.doc_id)
                .map_err(|e| anyhow::anyhow!("Invalid doc id {}: {e}", payload.doc_id))?;
            let doc_repo = DocRepository::new(storage.clone());
            let doc = doc_repo
                .get(space_id, &doc_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Doc not found: {doc_id}"))?;

            let mut renderer_registry = RendererRegistry::new();
            renderer_registry.register(Box::new(MultimodalRenderer::new()));
            renderer_registry.register(Box::new(DefaultTextRenderer::new()));
            renderer_registry.register(Box::new(PassthroughRenderer::new()));

            let source_type = doc
                .media_attachments
                .first()
                .map_or("text/plain", |attachment| attachment.media_type.as_str());

            let render_result = Box::pin(elephant_pipeline::scheduler::evidence_work_kinds::execute_render_doc_for_work(
                &storage,
                &renderer_registry,
                &doc,
                Some(source_type),
                item,
            ))
            .await
            .map_err(|e| anyhow::Error::new(e).context("render_doc failed"))?;

            Box::pin(elephant_pipeline::scheduler::evidence_work_kinds::execute_build_doc_tree_for_work(
                &storage,
                &render_result.revision_id,
                &config.doc_tree,
                item,
            ))
            .await
            .map_err(|e| anyhow::Error::new(e).context("build_doc_tree failed"))?;

            let ingest_mode =
                parse_optional_ingest_mode(payload.ingest_mode.as_deref(), IngestMode::Graph);
            if item.payload.get("repair_for").is_some() && publication.is_some() {
                // The closed manifest retains the original planned identities.
                // Its ordinary pending sweep recreates exact lost successors.
                return Ok(metrics);
            }
            if publication.is_none() {
                if let Some(source_id) = payload.source_id.as_deref() {
                    let source_id = RecordId::from_str(source_id).map_err(|e| {
                        anyhow::anyhow!("Invalid render_doc source id {source_id}: {e}")
                    })?;
                    update_source_record(&storage, space_id, &source_id, |source| {
                        if source.lifecycle_status != SourceLifecycleStatus::GraphReady {
                            source.lifecycle_status = SourceLifecycleStatus::RetrievalReady;
                            source.current_ingest_mode = Some(IngestMode::Retrieval);
                        }
                    })
                    .await?;
                }
            }

            if ingest_mode == IngestMode::Retrieval {
                if publication.is_some() {
                    Box::pin(source_runtime::seal_manifest(
                        &storage,
                        item,
                        ingest_mode,
                        Some(&render_result.revision_id),
                        None,
                    ))
                    .await?;
                    source_runtime::enqueue_finalizer(&storage, item).await?;
                }
                return Ok(metrics);
            }

            let work_item_repo = WorkItemRepository::new(storage.clone());
            let idempotency_key = format!("{PLAN_EXTRACTION_KIND}:{}", render_result.revision_id);
            let existing = work_item_repo
                .find_by_idempotency_key(space_id, &idempotency_key)
                .await?;
            if existing.is_some() {
                return Ok(metrics);
            }
            #[cfg(feature = "meerkat-extraction")]
            let extractor_id = AGENTIC_EXTRACTOR_ID;
            #[cfg(not(feature = "meerkat-extraction"))]
            {
                return Err(anyhow::anyhow!(
                    "semantic_extractor_unavailable: elephant-pipeline was built without the `meerkat-extraction` feature"
                ));
            }

            let mut base = BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.clone(),
                item.base.created_by.clone(),
            );
            base.security = doc.base.security.clone();
            base.provenance = doc.base.provenance.clone();

            let plan_item = WorkItem {
                base,
                kind: PLAN_EXTRACTION_KIND.to_string(),
                payload: serde_json::json!({
                    "revision_id": render_result.revision_id.to_string(),
                    "policy_version": "default",
                    "extractor_id": extractor_id,
                    "allow_raw_quotes": false,
                    "ingest_mode": ingest_mode.to_string(),
                    "source_id": payload.source_id,
                }),
                status: WorkItemStatus::Runnable,
                priority: successor_priority(item.priority),
                idempotency_key,
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };

            enqueue_work_successor(&storage, item, plan_item)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to enqueue plan_extraction: {e}"))?;
            Ok(metrics)
        }).await,
        PLAN_EXTRACTION_KIND => boxed_work_item_phase(|| async move {
            let payload: PlanExtractionPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid plan_extraction payload: {e}"))?;
            if payload.revision_id.is_empty() {
                return Err(anyhow::anyhow!(
                    "plan_extraction requires revision_id payload"
                ));
            }

            let revision_id = RecordId::from_str(&payload.revision_id)
                .map_err(|e| anyhow::anyhow!("Invalid revision id {}: {e}", payload.revision_id))?;
            let policy_version = payload
                .policy_version
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("plan_extraction requires policy_version payload")
                })?;
            if let Some(publication) = &publication {
                let captured_configuration =
                    elephant_pipeline::source_queue::configuration_from_work(item)?;
                if let Some(manifest) = elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(storage.clone()).get(space_id, publication).await?
                    .filter(|manifest| captured_configuration.as_ref() == Some(&manifest.configuration)) {
                    elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(storage.clone()).resume_plan_attempt(
                        space_id, publication, &elephant_storage::work_attempt_repository::fence_from_claim(item)?, work_run_id).await?;
                    source_runtime::enqueue_manifest_children(&storage, item, &manifest).await?;
                    source_runtime::enqueue_finalizer(&storage, item).await?;
                    return Ok(metrics);
                }
            }
            let config = ExtractionPlanConfig::default();
            let result = match Box::pin(execute_plan_extraction(
                &storage,
                &revision_id,
                policy_version,
                &config,
                work_run_id,
                space_id,
                &item.base.created_by,
            ))
            .await
            {
                Ok(result) => result,
                Err(err) if is_obsolete_extraction_reference_error(&err) => {
                    warn!(
                        work_item = %item.base.id,
                        revision_id = %revision_id,
                        error = %err,
                        "Stale plan_extraction reference requires revision-aware recovery"
                    );
                    return Err(anyhow::Error::new(WorkFailure {
                        class: FailureClass::StaleInput,
                        message: err.to_string(),
                    }));
                }
                Err(err) => return Err(anyhow::Error::new(err).context("plan_extraction failed")),
            };

            metrics.custom.insert(
                "planned_tokens".to_string(),
                serde_json::json!(result.estimated_tokens),
            );
            metrics.custom.insert(
                "planned_budget_units".to_string(),
                serde_json::json!(result.plan.estimated_cost_euros),
            );

            let ingest_mode = payload
                .ingest_mode
                .as_deref()
                .or(payload.mode.as_deref())
                .and_then(|value| IngestMode::from_str(value).ok())
                .unwrap_or(IngestMode::Graph);
            let source_id_for_extraction = match payload
                .source_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                Some(source_id) => Some(source_id.to_string()),
                None => derive_source_id_for_revision(&storage, space_id, &revision_id).await?,
            };

            if publication.is_some() {
                let manifest = Box::pin(source_runtime::seal_manifest(
                    &storage,
                    item,
                    ingest_mode,
                    Some(&revision_id),
                    Some(&result),
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("Source manifest not persisted"))?;
                source_runtime::enqueue_manifest_children(&storage, item, &manifest).await?;
                source_runtime::enqueue_finalizer(&storage, item).await?;
                return Ok(metrics);
            }
            let node_selections = result.plan.node_selections;
            let no_node_selections = node_selections.is_empty();
            for selection in node_selections {
                let estimated_cost_euros = estimate_cost_from_tokens(selection.estimated_tokens);
                let extract_payload = serde_json::json!({
                    "node_id": selection.node_id.to_string(),
                    "node_ids": selection.node_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    "revision_id": revision_id.to_string(),
                    "extractor_id": payload.extractor_id.clone(),
                    "allow_raw_quotes": payload.allow_raw_quotes,
                    "estimated_tokens": selection.estimated_tokens,
                    "estimated_cost_euros": estimated_cost_euros,
                    "ingest_mode": ingest_mode.to_string(),
                    "source_id": source_id_for_extraction.clone(),
                    "span_start": selection.span_start,
                    "span_end": selection.span_end,
                });

                let work_item = WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
                    kind: EXTRACT_ASSERTIONS_KIND.to_string(),
                    payload: extract_payload,
                    status: WorkItemStatus::Runnable,
                    priority: successor_priority(item.priority),
                    idempotency_key: format!(
                        "{EXTRACT_ASSERTIONS_KIND}:{}:{}",
                        revision_id, selection.node_id
                    ),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                };

                enqueue_work_successor(&storage, item, work_item)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to enqueue extract_assertions: {e}"))?;
            }

            if no_node_selections {
                if let Some(source_id) = source_id_for_extraction.as_deref() {
                    let source_id = RecordId::from_str(source_id)
                        .map_err(|e| anyhow::anyhow!("Invalid source id {source_id}: {e}"))?;
                    mark_source_graph_ready_if_quiescent(
                        &storage,
                        space_id,
                        &source_id,
                        &item.base.id,
                    )
                    .await?;
                }
            }
            Ok(metrics)
        }).await,
        EXTRACT_ASSERTIONS_KIND => boxed_work_item_phase(|| async move {
            let payload: ExtractAssertionsPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid extract_assertions payload: {e}"))?;
            if payload.node_id.is_empty() || payload.revision_id.is_empty() {
                return Err(anyhow::anyhow!(
                    "extract_assertions requires node_id and revision_id payload"
                ));
            }

            let node_id = RecordId::from_str(&payload.node_id)
                .map_err(|e| anyhow::anyhow!("Invalid node id {}: {e}", payload.node_id))?;
            let revision_id = RecordId::from_str(&payload.revision_id)
                .map_err(|e| anyhow::anyhow!("Invalid revision id {}: {e}", payload.revision_id))?;
            let source_id_for_extraction = match payload
                .source_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                Some(source_id) => Some(source_id.to_string()),
                None => derive_source_id_for_revision(&storage, space_id, &revision_id).await?,
            };

            let configured_extractor;
            let extractor = if let Some(extractor) = extractor_override {
                extractor
            } else {
                configured_extractor = build_semantic_extractor(payload.extractor_id.as_deref())?;
                configured_extractor.as_ref()
            };

            let span_override = payload.span_start.zip(payload.span_end);

            let result = match elephant_pipeline::scheduler::extraction_work_kinds::execute_source_extraction(
                &storage,
                extractor,
                &node_id,
                &revision_id,
                work_run_id,
                space_id,
                &item.base.created_by,
                span_override,
                config.transient_retry_max_attempts,
            )
            .await
            {
                Ok(result) => result,
                Err(err) if is_obsolete_extraction_reference_error(&err) => {
                    warn!(
                        work_item = %item.base.id,
                        node_id = %node_id,
                        revision_id = %revision_id,
                        error = %err,
                        "Stale extraction reference requires revision-aware recovery"
                    );
                    return Err(anyhow::Error::new(WorkFailure {
                        class: FailureClass::StaleInput,
                        message: err.to_string(),
                    }));
                }
                Err(err) => {
                    return Err(anyhow::Error::new(err).context("extract_assertions failed"))
                }
            };

            if let Some(publication) = &publication {
                let repo = WorkArtifactRepository::new(storage.clone());
                let artifact = repo
                    .get(space_id, &result.artifact_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Extraction candidate disappeared"))?;
                if artifact.data["source_id"] != serde_json::json!(publication.source_id.to_string())
                    || !elephant_pipeline::scheduler::grounding::same_authorization_partition(&artifact.base, &item.base)
                {
                    return Err(PipelineError::ValidationError("Extraction producer source or authorization changed".into()).into());
                }
            }
            metrics.tokens_used = result.tokens_used;
            metrics.custom.insert(
                "measured_tokens".to_string(),
                serde_json::json!(result.tokens_used),
            );
            let workflow_payload = ArtifactWorkflowPayload {
                work_run_id: work_run_id.to_string(),
                source_id: source_id_for_extraction,
                ingest_mode: payload.ingest_mode.clone().or_else(|| payload.mode.clone()),
            };
            let validate_idempotency_key = format!("{VALIDATE_ARTIFACTS_KIND}:{work_run_id}");
            let work_item_repo = WorkItemRepository::new(storage.clone());
            if work_item_repo
                .find_by_idempotency_key(space_id, &validate_idempotency_key)
                .await?
                .is_none()
            {
                let validate_item = WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
                    kind: VALIDATE_ARTIFACTS_KIND.to_string(),
                    payload: serde_json::to_value(&workflow_payload).map_err(|e| {
                        anyhow::anyhow!("Failed to serialize validate_artifacts payload: {e}")
                    })?,
                    status: WorkItemStatus::Runnable,
                    priority: successor_priority(item.priority),
                    idempotency_key: validate_idempotency_key,
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                };
                enqueue_work_successor(&storage, item, validate_item).await?;
            }
            Ok(metrics)
        }).await,
        VALIDATE_ARTIFACTS_KIND => boxed_work_item_phase(|| async move {
            let payload: ArtifactWorkflowPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid validate_artifacts payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id {}: {e}", payload.work_run_id))?;
            if publication.is_some() {
                source_runtime::require_successful_run(&storage, space_id, &run_id).await?;
            }
            let staged_commit = StagedCommit::new(storage.clone());
            let artifact_repo = WorkArtifactRepository::new(storage.clone());
            let validated = if publication.is_some() {
                Box::pin(
                    elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute_source(
                        &staged_commit,
                        space_id,
                        &run_id,
                    ),
                )
                .await
            } else {
                Box::pin(
                    elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute(
                        &staged_commit,
                        space_id,
                        &run_id,
                    ),
                )
                .await
            }
            .map_err(|e| anyhow::Error::new(e).context("validate_artifacts failed"))?;
            let existing_artifacts: Vec<_> = artifact_repo.find_by_work_run(&run_id).await?;
            let any_validated = validated
                .iter()
                .any(|artifact| artifact.status == ArtifactStatus::Validated);
            // A deterministic metadata replay keeps the selected bundle's
            // original producer. Its commit handoff must follow that same run.
            let mut commit_run_id = run_id.clone();
            for artifact in &validated {
                if artifact.status != ArtifactStatus::Validated {
                    continue;
                }
                let Some(bundle_kind) = semantic_bundle_kind(&artifact.artifact_type) else {
                    continue;
                };
                let existing_commit_bundle = existing_artifacts.iter().find(|candidate| {
                    candidate.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE
                        && candidate
                            .data
                            .get("source_artifact_id")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|id| id == artifact.base.id.to_string())
                });
                // A prior bundle is a replay candidate, not proof that its
                // semantics or authorization still match the validated artifact.
                let commit_payload = match bundle_kind {
                    CommitBundleKind::SourceMetadata => CommitBundlePayload::SourceMetadata(
                        serde_json::from_value(artifact.data.clone()).map_err(|e| {
                            anyhow::anyhow!("Failed to deserialize source metadata artifact: {e}")
                        })?,
                    ),
                    CommitBundleKind::KnowledgeCandidates => {
                        let mut data = extraction_artifact_data(&artifact.data).map_err(|e| {
                            anyhow::anyhow!("Failed to deserialize knowledge candidates: {e}")
                        })?;
                        if data.source_id.is_none() {
                            data.source_id.clone_from(&payload.source_id);
                        }
                        CommitBundlePayload::KnowledgeCandidates(data)
                    }
                    CommitBundleKind::ResolutionPatchSet => {
                        CommitBundlePayload::ResolutionPatchSet(
                            serde_json::from_value(artifact.data.clone()).map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to deserialize resolution patch artifact: {e}"
                                )
                            })?,
                        )
                    }
                };

                let mut bundle_base = artifact.base.clone();
                bundle_base.id = match (&publication, &commit_payload) {
                    (Some(fence), CommitBundlePayload::SourceMetadata(_)) => {
                        source_runtime::metadata_bundle_id(
                            fence,
                            &elephant_pipeline::source_queue::configuration_from_work(item)?
                                .ok_or_else(|| {
                                    anyhow::anyhow!("Metadata producer configuration missing")
                                })?,
                        )
                    }
                    (Some(fence), CommitBundlePayload::KnowledgeCandidates(data)) => {
                        elephant_storage::source_publication_manifest::source_bundle_id_for_plan(
                            fence,
                            &elephant_pipeline::source_queue::configuration_from_work(item)?
                                .ok_or_else(|| {
                                    anyhow::anyhow!("Bundle producer configuration missing")
                                })?,
                            &RecordId::from_str(&data.revision_id)?,
                            &RecordId::from_str(&data.node_id)?,
                        )
                    }
                    _ => existing_commit_bundle.map_or_else(
                        || RecordId::new_v4(TableName::WorkArtifact),
                        |bundle| bundle.base.id.clone(),
                    ),
                };
                bundle_base.created_at = Utc::now();
                bundle_base.updated_at = bundle_base.created_at;
                bundle_base.created_by.clone_from(&item.base.created_by);
                let mut bundle = elephant_types::records::work::WorkArtifact {
                    base: bundle_base,
                    work_run: Link::new(run_id.clone()),
                    artifact_type: COMMIT_BUNDLE_ARTIFACT_TYPE.to_string(),
                    data: serde_json::to_value(CommitBundleArtifactData {
                        source_artifact_id: artifact.base.id.to_string(),
                        source_artifact_type: artifact.artifact_type.clone(),
                        bundle_kind,
                        payload: commit_payload,
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to serialize commit bundle: {e}"))?,
                    status: ArtifactStatus::Validated,
                    validation_errors: None,
                    validated_at: Some(chrono::Utc::now()),
                    committed_at: None,
                };
                if let Some(publication) = &publication {
                    bundle.data["publication"] = serde_json::to_value(publication)?;
                    bundle.data[elephant_pipeline::source_queue::SOURCE_CONFIGURATION_KEY] = item
                        .payload[elephant_pipeline::source_queue::SOURCE_CONFIGURATION_KEY]
                        .clone();
                    bundle.data[elephant_pipeline::source_queue::SOURCE_PLAN_KEY] =
                        item.payload[elephant_pipeline::source_queue::SOURCE_PLAN_KEY].clone();
                }
                if let Some(existing) = artifact_repo.get(space_id, &bundle.base.id).await? {
                    if existing.data != bundle.data
                        || existing.base.space_id != bundle.base.space_id
                        || existing.base.deleted_at.is_some()
                        || existing.work_run != bundle.work_run
                        || existing.artifact_type != bundle.artifact_type
                        || existing.validated_at.is_none()
                        || existing
                            .validation_errors
                            .as_ref()
                            .is_some_and(|errors| !errors.is_empty())
                        || !matches!(
                            existing.status,
                            ArtifactStatus::Validated
                                | ArtifactStatus::Committing
                                | ArtifactStatus::Committed
                        )
                        || existing.base.security != bundle.base.security
                        || existing.base.subjects != bundle.base.subjects
                        || existing.base.subjects_state != bundle.base.subjects_state
                    {
                        let replay = if validated.len() == 1 {
                            Box::pin(metadata_bundle_replay::original_run(
                                &storage, item, artifact, &bundle, &existing,
                            ))
                            .await?
                        } else {
                            None
                        };
                        if let Some(original_run) = replay {
                            commit_run_id = original_run;
                        } else {
                            return Err(PipelineError::ValidationError(
                                "Commit bundle conflicts with prior validated candidate".into(),
                            )
                            .into());
                        }
                    }
                } else {
                    commit_run_id = Box::pin(metadata_bundle_replay::create_bundle(
                        &storage, item, artifact, bundle, validated.len() == 1,
                    ))
                    .await?;
                }
            }
            if any_validated {
                let work_item_repo = WorkItemRepository::new(storage.clone());
                let commit_payload = ArtifactWorkflowPayload {
                    work_run_id: commit_run_id.to_string(),
                    source_id: payload.source_id.clone(),
                    ingest_mode: payload.ingest_mode.clone(),
                };
                let mut commit_item = WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
                    kind: COMMIT_ARTIFACTS_KIND.to_string(),
                    payload: serde_json::to_value(&commit_payload).map_err(|e| {
                        anyhow::anyhow!("Failed to serialize commit_artifacts payload: {e}")
                    })?,
                    status: WorkItemStatus::Runnable,
                    priority: successor_priority(item.priority),
                    idempotency_key: format!("{COMMIT_ARTIFACTS_KIND}:{commit_run_id}"),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                };
                // Lookup uses the actual durable key, including source/plan
                // suffix. Keep an existing child's original validator parent;
                // the replay must not enqueue it under another predecessor.
                inherit_source_context(item, &mut commit_item)?;
                if let Some(ids) = item.payload.get(source_runtime::METADATA_IDS) {
                    commit_item.payload[source_runtime::METADATA_IDS] = ids.clone();
                }
                if let Some(existing) = work_item_repo
                    .find_by_idempotency_key(space_id, &commit_item.idempotency_key)
                    .await?
                {
                    if publication.is_some()
                        && !Box::pin(metadata_bundle_replay::original_commit_child(
                            &storage, &commit_item, &existing,
                        )).await?
                    {
                        return Err(PipelineError::ValidationError(
                            "Existing source commit handoff differs from selected producer".into(),
                        ).into());
                    }
                } else {
                    enqueue_work_successor(&storage, item, commit_item).await?;
                }
            }
            Ok(metrics)
        }).await,
        COMMIT_ARTIFACTS_KIND => boxed_work_item_phase(|| async move {
            let payload: ArtifactWorkflowPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid commit_artifacts payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id {}: {e}", payload.work_run_id))?;
            let ingest_mode =
                parse_optional_ingest_mode(payload.ingest_mode.as_deref(), IngestMode::Graph);
            if let Some(publication) = &publication {
                let attempt = elephant_storage::work_attempt_repository::fence_from_claim(item)?;
                let artifacts = WorkArtifactRepository::new(storage.clone())
                    .find_by_work_run(&run_id)
                    .await?;
                for artifact in artifacts.into_iter().filter(|artifact| {
                    artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE
                        && artifact.status == ArtifactStatus::Validated
                }) {
                    let bundle = commit_bundle_payload(&artifact)?;
                    let mut successor_parent = item.clone();
                    if matches!(bundle.payload, CommitBundlePayload::SourceMetadata(_)) {
                        successor_parent.payload[source_runtime::METADATA_IDS] =
                            serde_json::json!([artifact.base.id.to_string()]);
                        if matches!(ingest_mode, IngestMode::Metadata | IngestMode::Skip) {
                            Box::pin(source_runtime::seal_manifest(
                                &storage,
                                &successor_parent,
                                ingest_mode,
                                None,
                                None,
                            ))
                            .await?;
                        }
                    }
                    elephant_pipeline::source_finalization::register_bundle(
                        &storage,
                        space_id,
                        publication,
                        &artifact.base.id,
                        &attempt,
                    )
                    .await?;
                    if matches!(bundle.payload, CommitBundlePayload::SourceMetadata(_))
                        && matches!(ingest_mode, IngestMode::Graph | IngestMode::Retrieval)
                    {
                        enqueue_upgrade_after_metadata_commit(
                            &storage,
                            &successor_parent,
                            &publication.source_id,
                            ingest_mode,
                        )
                        .await?;
                    } else {
                        source_runtime::enqueue_finalizer(&storage, &successor_parent).await?;
                    }
                }
                return Ok(metrics);
            }
            let staged_commit = StagedCommit::new(storage.clone());
            let work_run = WorkRunRepository::new(storage.clone())
                .get(space_id, &run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("WorkRun not found: {run_id}"))?;
            let budget_enforcer =
                BudgetEnforcer::new(storage.clone(), load_budget_from_env(), space_id.clone());
            let should_recover_abandoned_commits = item.attempts > 1
                || item
                    .last_error
                    .as_deref()
                    .is_some_and(|err| err.contains("timed out"));
            if should_recover_abandoned_commits {
                let timeout_secs = i64::try_from(config.work_item_timeout_secs)
                    .unwrap_or(i64::MAX)
                    .max(1);
                let abandoned_before = Utc::now() - Duration::seconds(timeout_secs);
                let recovered = staged_commit
                    .recover_abandoned_commits_for_run(space_id, &run_id, abandoned_before)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to recover abandoned commit artifacts: {e}")
                    })?;
                if !recovered.is_empty() {
                    info!(
                        work_item = %item.base.id,
                        work_run = %run_id,
                        recovered_artifacts = recovered.len(),
                        "Recovered abandoned commit artifacts before retry"
                    );
                }
            }
            let artifacts = staged_commit
                .get_artifacts_for_run(&run_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load staged artifacts: {e}"))?;
            // Resolution bundles are one canonical batch. The maintenance
            // owner verifies the original patch/run/epoch and commits both
            // artifact states with all assertion effects and the receipt.
            // Include committed rows so retries must verify that receipt.
            let mut resolution_bundles = Vec::new();
            for artifact in &artifacts {
                if artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE
                    && matches!(
                        commit_bundle_payload(artifact)?.payload,
                        CommitBundlePayload::ResolutionPatchSet(_)
                    )
                {
                    resolution_bundles.push(artifact.base.id.clone());
                }
            }
            if !resolution_bundles.is_empty() {
                let workflow = ResolutionPatchWorkflow::new(storage.clone()).with_attempt(
                    elephant_storage::work_attempt_repository::fence_from_claim(item)?,
                );
                Box::pin(workflow.commit_resolution_bundle_batch(
                    space_id,
                    &resolution_bundles,
                    &BackoffConfig::default(),
                    None,
                ))
                .await
                .map_err(|error| anyhow::Error::new(error).context("commit resolution bundles"))?;
            }
            let mut committed_graph_artifacts = !resolution_bundles.is_empty();

            for artifact in artifacts {
                if resolution_bundles.contains(&artifact.base.id) {
                    continue;
                }
                if artifact.status != ArtifactStatus::Validated
                    || artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE
                {
                    continue;
                }

                let bundle_preview = commit_bundle_payload(&artifact)
                    .map_err(|e| anyhow::Error::new(e).context("commit_artifacts failed"))?;
                committed_graph_artifacts |= !matches!(
                    bundle_preview.payload,
                    CommitBundlePayload::SourceMetadata(_)
                );

                let storage_for_commit = storage.clone();
                let actor = item.base.created_by.clone();
                let run_id_for_commit = run_id.clone();
                Box::pin(staged_commit.commit_with_budget_check(
                    space_id,
                    &artifact.base.id,
                    &budget_enforcer,
                    ingest_mode,
                    work_run.metrics.estimated_cost_euros,
                    move |artifact| {
                        let storage = storage_for_commit.clone();
                        let actor = actor.clone();
                        let run_id = run_id_for_commit.clone();
                        let bundle = commit_bundle_payload(artifact);
                        async move {
                            let bundle = bundle?;
                            match bundle.payload {
                                CommitBundlePayload::SourceMetadata(metadata) => {
                                    commit_source_metadata_bundle(&storage, space_id, &metadata)
                                        .await?;
                                    let source_id = RecordId::from_str(&metadata.source_id)
                                        .map_err(|e| {
                                            PipelineError::InvalidInput(format!(
                                                "Invalid source id {source_id}: {e}",
                                                source_id = metadata.source_id
                                            ))
                                        })?;
                                    enqueue_upgrade_after_metadata_commit(
                                        &storage,
                                        item,
                                        &source_id,
                                        ingest_mode,
                                    )
                                    .await
                                    .map_err(|e| {
                                        PipelineError::ConfigError(format!(
                                            "Failed to enqueue upgrade after metadata commit: {e}"
                                        ))
                                    })?;
                                    Ok(())
                                }
                                CommitBundlePayload::KnowledgeCandidates(data) => {
                                    Box::pin(commit_knowledge_candidates_bundle(
                                        &storage, space_id, &run_id, &actor, &data,
                                    ))
                                    .await
                                }
                                CommitBundlePayload::ResolutionPatchSet(_) => {
                                    Err(PipelineError::InvalidState(
                                        "Resolution bundles require the complete atomic maintenance batch".into(),
                                    ))
                                }
                            }
                        }
                    },
                ))
                .await
                .map_err(|e| anyhow::Error::new(e).context("commit_artifacts failed"))?;
            }

            if ingest_mode == IngestMode::Graph && committed_graph_artifacts {
                if let Some(source_id) = payload.source_id.as_deref() {
                    let source_id = RecordId::from_str(source_id)
                        .map_err(|e| anyhow::anyhow!("Invalid source id {source_id}: {e}"))?;
                    mark_source_graph_ready_if_quiescent(
                        &storage,
                        space_id,
                        &source_id,
                        &item.base.id,
                    )
                    .await?;
                }
            }
            Ok(metrics)
        }).await,
        validate_resolution_patches::KIND => boxed_work_item_phase(|| async move {
            let payload: WorkRunPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid validate payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id: {e}"))?;
            let workflow = ResolutionPatchWorkflow::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            Box::pin(validate_resolution_patches::execute(
                &workflow, space_id, &run_id,
            ))
            .await?;
            Ok(metrics)
        }).await,
        commit_resolution_patches::KIND => boxed_work_item_phase(|| async move {
            let payload: WorkRunPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid commit payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id: {e}"))?;
            let workflow = ResolutionPatchWorkflow::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            Box::pin(commit_resolution_patches::execute(
                &workflow,
                space_id,
                &run_id,
                &BackoffConfig::default(),
                None,
            ))
            .await?;
            Ok(metrics)
        }).await,
        WORK_KIND_EXECUTE_REPROCESS_JOB => boxed_work_item_phase(|| async move {
            let payload: ReprocessJobPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid reprocess payload: {e}"))?;
            if payload.job_id.is_empty() {
                return Err(anyhow::anyhow!(
                    "execute_reprocess_job requires job_id payload"
                ));
            }

            let job_id = RecordId::from_str(&payload.job_id)
                .map_err(|e| anyhow::anyhow!("Invalid reprocess job id {}: {e}", payload.job_id))?;
            let ctx = ReprocessExecutionContext {
                storage: storage.clone(),
                job_repo: ReprocessJobRepository::new(storage.clone()),
                source_repo: SourceRepository::new(storage.clone()),
                doc_repo: DocRepository::new(storage.clone()),
                doc_revision_repo: DocRevisionRepository::new(storage.clone()),
                assertion_repo: AssertionRepository::new(storage.clone()),
                work_run_repo: WorkRunRepository::new(storage.clone()),
            };
            let reprocess_config = ReprocessConfig::default();
            let reprocess_result = Box::pin(execute_reprocess_job_with_attempt(
                &ctx,
                &job_id,
                work_run_id,
                space_id,
                &reprocess_config,
                Some(elephant_storage::work_attempt_repository::fence_from_claim(
                    item,
                )?),
            ))
            .await?;

            let pending = reprocess_result.continuation_required
                || !reprocess_result.pending_operations.is_empty();
            for group_id in &reprocess_result.pending_conflict_groups {
                enqueue_reprocess_resolution(&storage, item, work_run_id, group_id).await?;
            }
            if pending {
                return Err(WorkFailure {
                    class: FailureClass::PendingDependencies,
                    message: "Reprocess progress persisted; accepted source operations or maintenance continuations remain".into(),
                }.into());
            }
            Ok(metrics)
        }).await,
        TM_DETECT_CONFLICTS_KIND => boxed_work_item_phase(|| async move {
            let payload: DetectConflictsPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_detect_conflicts payload: {e}"))?;
            if payload.event_ids.is_empty() {
                return Err(anyhow::anyhow!(
                    "tm_detect_conflicts requires event_ids payload"
                ));
            }

            let event_repo = SystemEventRepository::new(storage.clone());
            let mut events = Vec::with_capacity(payload.event_ids.len());
            for id in payload.event_ids {
                let record_id = RecordId::from_str(&id)
                    .map_err(|e| anyhow::anyhow!("Invalid system_event id {id}: {e}"))?;
                let event = event_repo
                    .get_by_id(&record_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Required outbox event missing: {record_id}"))?;
                if event.base.space_id != *space_id {
                    anyhow::bail!("Required outbox event crosses spaces: {record_id}");
                }
                events.push(event);
            }

            if events.is_empty() {
                return Ok(metrics);
            }

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            let result = Box::pin(execute_tm_detect_conflicts_from_outbox_events(
                &events,
                &tm_storage,
            ))
            .await?;
            let DetectConflictsResult {
                conflict_group_ids,
                direct_commits,
                ..
            } = result;

            if !conflict_group_ids.is_empty() || !direct_commits.is_empty() {
                for group_id in conflict_group_ids {
                    enqueue_group_resolution(&storage, item, work_run_id, &group_id, true).await?;
                }

                for commit in direct_commits {
                    let work_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            item.base.created_by.clone(),
                        ),
                        kind: TM_COMMIT_KIND.to_string(),
                        payload: serde_json::to_value(CommitTruthPayload {
                            resolution: commit.resolution,
                            input: commit.input.clone(),
                        })
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to serialize tm_commit payload: {e}")
                        })?,
                        status: WorkItemStatus::Runnable,
                        priority: successor_priority(item.priority),
                        idempotency_key: format!(
                            "tm_commit:slot:{}:{work_run_id}",
                            commit.input.slot_key
                        ),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };

                    enqueue_work_successor(&storage, item, work_item)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_commit: {e}"))?;
                }
            }
            Ok(metrics)
        }).await,
        TM_COMMIT_KIND => boxed_work_item_phase(|| async move {
            let payload: CommitTruthPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_commit payload: {e}"))?;

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            let result = Box::pin(execute_tm_commit(
                &payload.resolution,
                &payload.input,
                space_id,
                &tm_storage,
            ))
            .await
            .map_err(|error| {
                if is_obsolete_tm_commit_error(&error) {
                    anyhow::Error::new(WorkFailure {
                        class: FailureClass::StaleInput,
                        message: error.to_string(),
                    })
                } else {
                    anyhow::Error::new(error)
                }
            })?;
            if result.skipped_stale {
                return Err(WorkFailure {
                    class: FailureClass::StaleInput,
                    message: "Truth-maintenance decision needs fresh preparation".into(),
                }
                .into());
            }
            Ok(metrics)
        }).await,
        TM_RESOLVE_SLOT_KIND => boxed_work_item_phase(|| async move {
            let payload: ResolveSlotPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_resolve_slot payload: {e}"))?;
            if payload.conflict_group_id.is_empty() {
                return Err(anyhow::anyhow!(
                    "tm_resolve_slot requires conflict_group_id payload"
                ));
            }

            let group_id = RecordId::from_str(&payload.conflict_group_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid conflict_group_id {}: {e}",
                    payload.conflict_group_id
                )
            })?;
            let conflict_repo = ConflictGroupRepository::new(storage.clone());
            let Some(conflict_group) = conflict_repo
                .get(space_id, &group_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load conflict_group: {e}"))?
            else {
                return Err(anyhow::anyhow!(
                    "Conflict group not found: {}",
                    payload.conflict_group_id
                ));
            };

            if conflict_group.resolved {
                return Ok(metrics);
            }

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            let (resolution, input) = match Box::pin(resolve_conflict_group_with_storage(
                tm_ctx,
                &conflict_group,
                space_id,
                &tm_storage,
            ))
            .await
            {
                Ok(result) => result,
                Err(err) if is_obsolete_tm_resolve_error(&err) => {
                    return Err(WorkFailure {
                        class: FailureClass::StaleInput,
                        message: err.to_string(),
                    }
                    .into());
                }
                Err(err) => return Err(err.into()),
            };

            // Memoized replay: the outcome was already committed (or the
            // needs_review cooldown holds); committing again would clobber it.
            if input.skip_commit {
                info!(
                    "tm_resolve_slot {} skipped commit: adjudication memoized for unchanged claim set",
                    item.base.id
                );
                return Ok(metrics);
            }

            let commit_item = WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    space_id.clone(),
                    item.base.created_by.clone(),
                ),
                kind: TM_COMMIT_KIND.to_string(),
                payload: serde_json::to_value(CommitTruthPayload { resolution, input })
                    .map_err(|e| anyhow::anyhow!("Failed to serialize tm_commit payload: {e}"))?,
                status: WorkItemStatus::Runnable,
                priority: successor_priority(item.priority),
                idempotency_key: format!("tm_commit:{group_id}:{work_run_id}"),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };

            enqueue_work_successor(&storage, item, commit_item)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_commit: {e}"))?;
            Ok(metrics)
        }).await,
        TM_RECOMPUTE_KIND => boxed_work_item_phase(|| async move {
            let payload: RecomputePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_recompute payload: {e}"))?;
            let priority = parse_recompute_priority(payload.priority.as_deref());
            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone()).with_attempt(
                elephant_storage::work_attempt_repository::fence_from_claim(item)?,
            );
            let result = Box::pin(execute_tm_recompute(
                tm_ctx,
                space_id,
                priority,
                payload.max_slots,
                &tm_storage,
            ))
            .await?;

            // Multi-value-group slots defer adjudication out of the recompute
            // lease: enqueue one tm_resolve_slot item per pending group.
            if !result.pending_conflict_groups.is_empty() {
                for group_id in result.pending_conflict_groups {
                    enqueue_group_resolution(&storage, item, work_run_id, &group_id, true).await?;
                }
            }
            if result.slots_skipped > 0 {
                let mut base = item.base.clone();
                base.id = RecordId::new_v4(TableName::WorkItem);
                enqueue_work_successor(
                    &storage,
                    item,
                    WorkItem {
                        base,
                        kind: TM_RECOMPUTE_KIND.into(),
                        payload: item.payload.clone(),
                        status: WorkItemStatus::Runnable,
                        priority: item.priority,
                        idempotency_key: format!("tm_recompute_continuation:{}", item.base.id),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    },
                )
                .await?;
            }
            Ok(metrics)
        }).await,
        WORK_KIND_FRESHNESS_SWEEP => boxed_work_item_phase(|| async move {
            let freshness_repo = FreshnessRepository::new(storage.clone());
            let result = execute_freshness_sweep(freshness_repo, space_id, None).await?;
            metrics.custom.insert(
                "items_processed".to_string(),
                serde_json::json!(result.processed),
            );
            Ok(metrics)
        }).await,
        RECONCILE_LIVING_WIKI_KIND => boxed_work_item_phase(|| async move {
            let payload: ManagedWikiPayload = serde_json::from_value(item.payload.clone())
                .map_err(|error| anyhow::anyhow!("Invalid managed wiki payload: {error}"))?;
            #[cfg(feature = "meerkat-extraction")]
            {
                return Box::pin(execute_managed_wiki_work_item(
                    item,
                    work_run_id,
                    storage,
                    payload,
                ))
                .await;
            }

            #[cfg(not(feature = "meerkat-extraction"))]
            return Err(anyhow::anyhow!(
                "reconcile_living_wiki requires the meerkat-extraction feature"
            ));
        }).await,
        other => {
            Err(anyhow::anyhow!("Unsupported work kind: {other}"))
        }
    }
}

mod failure_diagnostic;

/// Expose only reviewed static context codes, never arbitrary nested errors.
fn failure_context_code(error: &anyhow::Error) -> Option<&'static str> {
    error.chain().find_map(|cause| {
        let storage = cause.downcast_ref::<StorageError>().or_else(|| {
            match cause.downcast_ref::<PipelineError>() {
                Some(PipelineError::StorageError(storage)) => Some(storage),
                _ => None,
            }
        });
        match storage {
            Some(StorageError::Conflict(message))
                if message == "standing_watch_authority_changed_or_missing" =>
            {
                Some("original_work_authority_changed")
            }
            _ => None,
        }
    })
}

/// Renew the original attempt and carry its exact updated snapshot to dispatch.
/// The queue changes only lease bookkeeping; never adopt a changed work input.
async fn renew_execution_work(
    queue: &WorkQueue,
    original: &WorkItem,
    execution_timeout_secs: u64,
) -> Result<WorkItem, PipelineError> {
    let renewed = queue
        .renew_lease(
            &original.base.space_id,
            &original.base.id,
            execution_timeout_secs.saturating_add(MANAGED_WIKI_LEASE_GRACE_SECS),
        )
        .await?;
    let mut expected = original.clone();
    expected.base.updated_at = renewed.base.updated_at;
    expected.lease_expires_at = renewed.lease_expires_at;
    // These fields are cleared by the existing renewal transition on both backends.
    expected.run_after = None;
    expected.last_error = None;
    if renewed != expected {
        return Err(PipelineError::LeaseConflict(original.base.id.to_string()));
    }
    Ok(renewed)
}

/// A completed owned task may have published runnable children or freed capacity.
/// Keep the fallback timer alive across completions: other producers, delayed
/// retries and recovery sweeps must not starve behind a stream of short tasks.
#[derive(Debug, PartialEq, Eq)]
enum WorkQueueWake {
    Poll,
    Completion,
}

async fn wait_for_work_queue_wake(
    tasks: &mut JoinSet<()>,
    poll: std::pin::Pin<&mut tokio::time::Sleep>,
) -> WorkQueueWake {
    tokio::select! {
        biased;
        () = poll => WorkQueueWake::Poll,
        result = tasks.join_next(), if !tasks.is_empty() => {
            if let Some(Err(error)) = result {
                warn!(%error, "Work queue task failed");
            }
            WorkQueueWake::Completion
        }
    }
}

#[expect(
    clippy::large_futures,
    clippy::option_if_let_else,
    clippy::significant_drop_tightening,
    reason = "Keep the explicit timeout disposition, admission lifetimes and original future layout"
)]
async fn run_work_queue_loop(
    storage: StorageClient,
    config: WorkQueueWorkerConfig,
    space_config_repo: SpaceConfigRepository,
    provider_cache: EmbeddingProviderCache,
    embed_config: EmbedWorkerConfig,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let default_queue_config = WorkQueueConfig::default();
    let queue_config = WorkQueueConfig {
        batch_size: config.batch_size,
        lease_duration_secs: config.lease_duration_secs,
        worker_id: config
            .worker_id
            .clone()
            .unwrap_or(default_queue_config.worker_id),
        scan_limit: config.scan_limit,
    };

    let work_queue = WorkQueue::new(storage.clone(), queue_config.clone());
    let work_run_repo = WorkRunRepository::new(storage.clone());
    let dag_executor = DagExecutor::new(storage.clone());
    let budget = load_budget_from_env();
    let daily_cap_items = budget
        .daily_items_graph
        .unwrap_or(u32::MAX)
        .saturating_add(budget.daily_items_retrieval.unwrap_or(u32::MAX));
    let daily_cap = DailyCapCheck::new(
        CostLedgerDailyRepository::new(storage.clone()),
        daily_cap_items,
    );

    // Wrap shared state in Arcs for spawned tasks
    let storage = Arc::new(storage);
    let config = Arc::new(config);
    let space_config_repo = Arc::new(space_config_repo);
    let provider_cache = Arc::new(provider_cache);
    let embed_config = Arc::new(embed_config);
    let work_queue = Arc::new(work_queue);
    let work_run_repo = Arc::new(work_run_repo);
    let dag_executor = Arc::new(dag_executor);
    let queue_config = Arc::new(queue_config);
    let budget = Arc::new(budget);
    let daily_cap = Arc::new(daily_cap);
    // Built once near worker startup; the only TM env read in the binary.
    let tm_ctx = Arc::new(TmContext::none());

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
    let extract_semaphore = Arc::new(Semaphore::new(config.max_extract_concurrent));
    let mut tasks = JoinSet::new();
    let poll = tokio::time::sleep(config.poll_interval());
    tokio::pin!(poll);

    info!(
        "Work queue loop starting with max_concurrent={} max_extract_concurrent={} allowed_kinds={:?} disabled_kinds={:?}",
        config.max_concurrent,
        config.max_extract_concurrent,
        config.allowed_kinds,
        config.disabled_kinds,
    );

    loop {
        tokio::select! {
            _ = rx.changed() => {
                if *rx.borrow() {
                    info!("Work queue worker received shutdown signal, draining {} in-flight tasks", tasks.len());
                    while let Some(result) = tasks.join_next().await {
                        if let Err(e) = result {
                            warn!("Task failed during drain: {e}");
                        }
                    }
                    break;
                }
            }
            wake = wait_for_work_queue_wake(&mut tasks, poll.as_mut()) => {
                if wake == WorkQueueWake::Poll {
                    poll.as_mut().reset(tokio::time::Instant::now() + config.poll_interval());
                    let recovery: anyhow::Result<()> = async { storage.with_activity(source_runtime::recover_sources(&storage)).await??; Ok(()) }.await;
                    if let Err(error) = recovery { warn!(%error, "Source recovery sweep incomplete; durable intent retained"); }
                }
                // Coalesce completions into one bounded claim pass. The wake
                // grants no authority: the existing claim still checks due time,
                // original attempt fences, kind selection and available permits.
                while let Some(result) = tasks.try_join_next() {
                    if let Err(e) = result {
                        warn!("Task panicked: {e}");
                    }
                }

                loop {
                    // Drain as much runnable work as we have capacity for on each
                    // wake. One batched claim fills every free permit from a
                    // single candidate scan instead of paying the scan per item —
                    // at deep queues the scan dominates, which serialized claims.
                    let free_permits = semaphore.available_permits();
                    if free_permits == 0 {
                        break;
                    }
                    let extract_slots = extract_semaphore.available_permits();
                    let claimed_items = match work_queue
                        .claim_batch_prefer_kind(
                            EXTRACT_ASSERTIONS_KIND,
                            extract_slots,
                            free_permits,
                            |item| config.allows_kind(&item.kind),
                        )
                        .await
                    {
                        Ok(items) => items,
                        Err(err) => {
                            warn!("Work queue claim failed: {}", err);
                            break;
                        }
                    };
                    if claimed_items.is_empty() {
                        break;
                    }
                    let batch_len = claimed_items.len();
                    for item in claimed_items {
                        let work_queue = match work_queue.for_attempt(&item) {
                            Ok(queue) => Arc::new(queue),
                            Err(error) => {
                                error!(work_item = %item.base.id, %error, "Invalid work claim");
                                continue;
                            }
                        };
                        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                            // Batch is sized by free permits and nothing else takes
                            // from this semaphore, so this is defensive: put the
                            // item back rather than leaking its lease.
                            let _ = work_queue
                                .release(&item.base.space_id, &item.base.id)
                                .await;
                            continue;
                        };
                        {
                            info!(
                                work_item = %item.base.id,
                                kind = %item.kind,
                                attempts = item.attempts,
                                "Claimed work item"
                            );
                            if item.kind == RECONCILE_LIVING_WIKI_KIND {
                                let work_item_repo =
                                    WorkItemRepository::new((*storage).clone());
                                let serialize = match work_item_repo
                                    .count_running_by_kind(
                                        &item.base.space_id,
                                        RECONCILE_LIVING_WIKI_KIND,
                                    )
                                    .await
                                {
                                    Ok(count) => count > 1,
                                    Err(err) => {
                                        warn!(
                                            work_item = %item.base.id,
                                            error = %err,
                                            "Could not verify Living Wiki work serialization; requeueing"
                                        );
                                        true
                                    }
                                };
                                if serialize {
                                    let _ = work_item_repo
                                        .requeue_with_delay(
                                            &item.base.space_id,
                                            &item.base.id,
                                            work_queue.lease_owner(),
                                            60,
                                            Some("living_wiki_serialization".to_string()),
                                        )
                                        .await;
                                    drop(permit);
                                    continue;
                                }
                            }
                            if item.kind == EXTRACT_ASSERTIONS_KIND {
                                let admission = elephant_pipeline::scheduler::extraction_repair::semantic_retry_limit(
                                    &storage, &item, config.transient_retry_max_attempts,
                                ).await;
                                let refusal = match admission {
                                    Ok(limit) if item.attempts > limit => Some(PipelineError::GroundingRepairExhausted),
                                    Ok(_) => None,
                                    Err(error) => Some(error),
                                };
                                if let Some(failure) = refusal {
                                    if let Err(error) = source_runtime::record_preflight_failure(&storage, &item, &failure.into(), &config).await {
                                        warn!(%error, "Extraction admission receipt remains pending original lease recovery");
                                    }
                                    drop(permit); continue;
                                }
                            }
                            // Check daily cap
                        match daily_cap.try_reserve(&item.base.space_id).await {
                            Ok(false) => {
                                // Cap reached — put item back as runnable
                                let work_item_repo = WorkItemRepository::new((*storage).clone());
                                let delay = i64::try_from(config.poll_interval_secs).unwrap_or(i64::MAX);
                                let _ = work_item_repo
                                    .requeue_configuration_blocked(
                                        &item.base.space_id, &item.base.id, work_queue.lease_owner(),
                                        delay.max(30), "daily_cap_reached".to_string(),
                                    )
                                    .await;
                                drop(permit);
                                continue;
                            }
                            Err(err) => {
                                warn!("Daily cap check failed for {}: {}", item.base.id, err);
                                if let Err(error) = source_runtime::record_preflight_failure(&storage, &item, &err.into(), &config).await {
                                    warn!(%error, "Daily cap failure transition remains pending lease recovery");
                                }
                                drop(permit); continue;
                            }
                            Ok(true) => {} // Under cap, proceed
                        }

                        let work_item_timeout_secs = if item.kind == RECONCILE_LIVING_WIKI_KIND {
                            config.work_item_timeout_secs.max(MANAGED_WIKI_TIMEOUT_SECS)
                        } else {
                            config.work_item_timeout_secs
                        };
                        let item = match renew_execution_work(
                            &work_queue, &item, work_item_timeout_secs,
                        ).await {
                            Ok(renewed) => renewed,
                            Err(err) => {
                                warn!(
                                    work_item = %item.base.id,
                                    error = %err,
                                    "Could not extend attempt lease to execution deadline"
                                );
                                if let Err(error) = source_runtime::record_preflight_failure(&storage, &item, &err.into(), &config).await {
                                    warn!(%error, "Lease deadline failure transition remains pending recovery");
                                }
                                drop(permit);
                                continue;
                            }
                        };

                        // Clone Arcs for the spawned task
                        let storage = Arc::clone(&storage);
                        let config = Arc::clone(&config);
                        let space_config_repo = Arc::clone(&space_config_repo);
                        let provider_cache = Arc::clone(&provider_cache);
                        let embed_config = Arc::clone(&embed_config);
                        let work_queue = Arc::clone(&work_queue);
                        let work_run_repo = Arc::clone(&work_run_repo);
                        let dag_executor = Arc::clone(&dag_executor);
                        let queue_config = Arc::clone(&queue_config);
                        let budget = Arc::clone(&budget);
                        let tm_ctx = Arc::clone(&tm_ctx);
                        let extract_permit = if item.kind == EXTRACT_ASSERTIONS_KIND {
                            let acquired = extract_semaphore.clone().try_acquire_owned();
                            if let Ok(permit) = acquired {
                                Some(permit)
                            } else {
                                warn!(
                                    work_item = %item.base.id,
                                    "Extract slot unavailable after claim; releasing work item back to runnable"
                                );
                                let _ = work_queue.release(&item.base.space_id, &item.base.id).await;
                                drop(permit);
                                continue;
                            }
                        } else {
                            None
                        };

                            tasks.spawn(async move {
                            let activity_client = (*storage).clone();
                            if let Err(error) = activity_client.with_activity(async {
                            let _permit = permit; // Hold permit until task completes
                            let _extract_permit = extract_permit;
                            let timeout = std::time::Duration::from_secs(work_item_timeout_secs);
                                let estimates = {
                                    let mut est = budget_estimates_from_payload(&item.payload);
                                    if item.kind == PLAN_EXTRACTION_KIND {
                                        est.ingest_mode = IngestMode::Metadata;
                                    }
                                    est
                                };

                                let budget_enforcer = BudgetEnforcer::new(
                                    (*storage).clone(),
                                    (*budget).clone(),
                                    item.base.space_id.clone(),
                                );

                                let ledger_id = match budget_enforcer
                                    .reserve_cost(
                                        estimates.ingest_mode,
                                        &item.kind,
                                        estimates.estimated_cost_euros,
                                    )
                                    .await
                                {
                                    Ok(id) => id,
                                    Err(err) => {
                                        warn!(
                                            "Failed to reserve cost for work item {}: {}",
                                            item.base.id, err
                                        );
                                        if let Err(error) = source_runtime::record_preflight_failure(&storage, &item, &err.into(), &config).await {
                                            warn!(%error, "Cost reservation failure transition remains pending lease recovery");
                                        }
                                        return;
                                    }
                                };

                                let run_id = RecordId::new_v4(TableName::WorkRun);
                                let mut base = item.base.clone();
                                base.id = run_id.clone();
                                base.created_by = queue_config.worker_id.clone();
                                base.created_at = Utc::now(); base.updated_at = base.created_at;
                                let started_at = chrono::Utc::now();
                                let mut usage = AttemptUsage::reserved(estimates.estimated_tokens, estimates.estimated_cost_euros);
                                let mut initial_metrics = WorkRunMetrics::default();
                                initial_metrics.custom.insert("cost_ledger_id".to_string(), serde_json::json!(ledger_id.to_string()));
                                initial_metrics.custom.insert("attempt_fence".to_string(), item.payload["_attempt_fence"].clone());
                                if let Err(error) = usage.write_metrics(&mut initial_metrics) {
                                    error!(%error, "Cannot serialize attempt usage");
                                    return;
                                }

                                let work_run = WorkRun {
                                    base,
                                    work_item: Link::new(item.base.id.clone()),
                                    attempt: item.attempts,
                                    started_at,
                                    completed_at: None,
                                    success: false,
                                    error: None,
                                    metrics: initial_metrics,
                                    worker_id: queue_config.worker_id.clone(),
                                    worker_version: env!("CARGO_PKG_VERSION").to_string(),
                                    model_version: None,
                                    prompt_id: None,
                                    config_hash: None,
                                };

                                let created_run = match work_run_repo.create(work_run).await {
                                    Ok(run) => run,
                                    Err(err) => {
                                        error!("Failed to create work_run for {}: {}", item.base.id, err);
                                        if let Err(error) = budget_enforcer.reconcile_cost(&ledger_id, 0.0, 0, false).await {
                                            warn!(%error, "Failed to release unused reservation");
                                        }
                                        let _ = work_queue
                                            .fail(
                                                &item.base.space_id,
                                                &item.base.id,
                                                format!("work_run creation failed: {err}"),
                                            )
                                            .await;
                                        return;
                                    }
                                };

                                let attempt = match elephant_storage::work_attempt_repository::fence_from_claim(&item) {
                                    Ok(attempt) => attempt,
                                    Err(error) => { error!(%error, "Invalid original attempt fence"); return; }
                                };
                                let usage_observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_attempt(
                                    (*storage).clone(), &created_run.base.id, attempt);
                                let execution = elephant_pipeline::provider_dispatch::with_dispatch_observer(
                                    usage_observer.clone(), Box::pin(execute_work_item(
                                    &item,
                                    &created_run.base.id,
                                    (*storage).clone(),
                                    &config,
                                    &space_config_repo,
                                    &provider_cache,
                                    &embed_config,
                                    &tm_ctx,
                                )));
                                let result = match tokio::time::timeout(timeout, execution).await {
                                    Ok(result) => result,
                                    Err(_) => Err(anyhow::Error::new(WorkFailure::timeout(work_item_timeout_secs))),
                                };

                                let completed_at = chrono::Utc::now();
                                let wall_time_ms_i64 = completed_at
                                    .signed_duration_since(started_at)
                                    .num_milliseconds()
                                    .max(0);
                                let wall_time_ms = u64::try_from(wall_time_ms_i64).unwrap_or(0);

                                let settlement = match attempt_settlement::AttemptSettlement::new(&storage, &item) {
                                    Ok(settlement) => settlement,
                                    Err(error) => { warn!(work_item = %item.base.id, %error, "Original outcome cannot settle after lease expiry"); return; }
                                };

                                let mut updated_run = match settlement.persist("read_original_run", || async {
                                    Ok(work_run_repo.get(&item.base.space_id, &created_run.base.id).await?)
                                }).await
                                {
                                    Ok(Some(run)) => run,
                                    Ok(None) => {
                                        error!(work_run = %created_run.base.id, "Attempt receipt disappeared; refusing completion");
                                        return;
                                    },
                                    Err(err) => {
                                        warn!(
                                            "Failed to refresh work_run {}: {}",
                                            created_run.base.id, err
                                        );
                                        return;
                                    }
                                };
                                updated_run.completed_at = Some(completed_at);
                                usage = match settlement.persist("summarize_original_usage", || async {
                                    Ok(usage_observer.summarize(usage.clone()).await?)
                                }).await {
                                    Ok(usage) => usage,
                                    Err(error) => { error!(%error, "Usage receipts unavailable; refusing completion"); return; }
                                };

                                match result {
                                    Ok(mut run_metrics) => {
                                        run_metrics.wall_time_ms = wall_time_ms;
                                        if let Err(error) = usage.write_metrics(&mut run_metrics) {
                                            error!(%error, "Cannot persist usage receipt");
                                            return;
                                        }
                                        record_summary_process_totals(&mut run_metrics);
                                        run_metrics.custom.insert("attempt_fence".to_string(), item.payload["_attempt_fence"].clone());
                                        run_metrics.custom.insert("cost_ledger_id".to_string(), serde_json::json!(ledger_id.to_string()));
                                        let frozen_key = elephant_pipeline::scheduler::extraction_repair::EXTRACTION_BUDGET_KEY;
                                        if updated_run.metrics.custom.get(frozen_key).is_some_and(|original| {
                                            run_metrics.custom.get(frozen_key).is_some_and(|returned| returned != original)
                                        }) {
                                            error!("Returned metrics changed frozen extraction input; refusing completion");
                                            return;
                                        }
                                        for (key, value) in &updated_run.metrics.custom {
                                            run_metrics.custom.entry(key.clone()).or_insert_with(|| value.clone());
                                        }
                                        updated_run.metrics = run_metrics.clone();
                                        updated_run.success = true;
                                        if let Err(err) = settlement.persist("record_successful_outcome", || async {
                                            Ok(work_run_repo.update(&item.base.space_id, &created_run.base.id, updated_run.clone()).await?)
                                        }).await
                                        {
                                            warn!("Failed to update work_run {}: {}", created_run.base.id, err);
                                            return;
                                        }
                                        if let Err(error) = settlement.persist("settle_successful_usage", || async {
                                            Ok(elephant_pipeline::provider_usage::reconcile_finished_usage(&storage, &item.base.space_id, &created_run.base.id).await?)
                                        }).await {
                                            warn!(%error, "Usage settlement incomplete; recovery will retry"); return;
                                        }
                                        if let Err(err) = complete_work_item_and_promote(
                                            &work_queue,
                                            &dag_executor,
                                            &item.base.space_id,
                                            &item.base.id,
                                        )
                                        .await
                                        {
                                            error!(
                                                "Failed to complete work item {}: {}",
                                                item.base.id, err
                                            );
                                        } else {
                                            info!(
                                                work_item = %item.base.id,
                                                kind = %item.kind,
                                                "Completed work item"
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        let (class, action) = match settlement.persist("record_failed_outcome_and_usage", || source_runtime::record_failed_execution(
                                            &storage, &item, &created_run.base.id, &err, &usage,
                                            completed_at, wall_time_ms, &config,
                                        )).await {
                                            Ok(disposition) => disposition,
                                            Err(error) => { warn!(%error, "Failed outcome or usage remains pending original lease recovery"); return; }
                                        };
                                        if let Err(error) = source_runtime::apply_failed_disposition(
                                            &storage, &item, &err, class, action,
                                        ).await {
                                            warn!(%error, "Original failed disposition remains pending lease recovery");
                                        } else {
                                            let diagnostic = failure_diagnostic::FailureDiagnostic::from_error(&err);
                                            warn!(work_item = %item.base.id, kind = %item.kind, error = %err,
                                                failure_kind = diagnostic.kind, failure_stage = diagnostic.stage,
                                                failure_code = diagnostic.code, failure_action = diagnostic.action(),
                                                ?class, ?action, "Recorded failed work attempt");
                                        }
                                    }
                                }
                            }).await { warn!(%error, "Work activity admission closed; lease remains recoverable"); }
                            });
                        }
                    }
                    if batch_len < free_permits {
                        // The queue had less claimable work than capacity —
                        // wait for a completion or the retained poll deadline.
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn knowledge_view_event_records(events: &[SystemEvent]) -> anyhow::Result<(bool, Vec<String>)> {
    let mut records = Vec::new();
    for event in events {
        if let Some(value) = event.payload.get("records") {
            records.extend(serde_json::from_value::<Vec<elephant_outbox::RecordRef>>(
                value.clone(),
            )?);
        } else if event.event_type == "records.changed" {
            anyhow::bail!("records.changed event {} has no records", event.base.id);
        }
    }
    let living_wiki_graph_changed = records.iter().any(|record| {
        matches!(
            record.id.table(),
            TableName::Entity
                | TableName::Rel
                | TableName::Assertion
                | TableName::Attribute
                | TableName::Event
                | TableName::Source
                | TableName::Doc
                | TableName::DocRevision
                | TableName::DocNode
                | TableName::PredicateRegistry
                | TableName::PredicateAlias
                | TableName::TruthSlot
                | TableName::Freshness
                | TableName::EntityMerge
        )
    });
    let affected_projection_records = records
        .into_iter()
        .flat_map(|record| {
            let table_trigger = format!("trigger:table:{}", record.id.table().as_str());
            [record.id.to_string(), table_trigger]
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok((living_wiki_graph_changed, affected_projection_records))
}

async fn process_knowledge_view_events(
    storage: &StorageClient,
    space_id: &str,
    after_seq: i64,
    events: &[SystemEvent],
) -> anyhow::Result<()> {
    let (living_wiki_graph_changed, affected_projection_records) =
        knowledge_view_event_records(events)?;
    if !affected_projection_records.is_empty() {
        let knowledge_views = KnowledgeViewRepository::new(storage.clone());
        let reason = format!(
            "outbox dependency update through seq {}",
            events.last().map_or(after_seq, |event| event.seq)
        );
        match knowledge_views
            .mark_dirty_for_dependencies(space_id, &affected_projection_records, &reason)
            .await
        {
            Ok(count) if count > 0 => {
                info!("Marked {} knowledge view(s) dirty for {}", count, space_id);
            }
            Ok(_) => {}
            Err(err) => {
                return Err(err.into());
            }
        }
    }
    if living_wiki_graph_changed {
        let debounce_seconds = std::env::var("ELEPHANT_WIKI_CHANGE_DEBOUNCE_SECS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(300)
            .clamp(30, 3_600);
        let run_after = Utc::now() + Duration::seconds(debounce_seconds);
        let change_observed_at = events
            .iter()
            .map(|event| event.base.created_at)
            .max()
            .unwrap_or_else(Utc::now);
        match enqueue_managed_wiki_graph_change(
            storage,
            space_id,
            "outbox.living_wiki",
            change_observed_at,
            run_after,
        )
        .await
        {
            Ok(Some(work_item)) => info!(
                work_item = %work_item.base.id,
                run_after = %run_after,
                "Debounced graph change into managed Living Wiki reconciliation"
            ),
            Ok(None) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn run_knowledge_view_invalidation_loop(
    storage: StorageClient,
    poll_interval: std::time::Duration,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    const BATCH_SIZE: usize = 500;
    let event_repo = SystemEventRepository::new(storage.clone());
    loop {
        tokio::select! {
            _ = rx.changed() => {
                if *rx.borrow() {
                    info!("Knowledge-view invalidation worker received shutdown signal");
                    break;
                }
            }
            () = tokio::time::sleep(poll_interval) => {
                let spaces = match event_repo.get_all_spaces().await {
                    Ok(spaces) => spaces,
                    Err(err) => {
                        error!("Knowledge-view invalidation worker failed to list spaces: {err}");
                        continue;
                    }
                };
                for space_id in spaces {
                    let client = &storage;
                    let space = &space_id;
                    let delivery: anyhow::Result<usize> = elephant_pipeline::outbox_delivery::deliver_batch(
                        client, space, "knowledge_views", BATCH_SIZE,
                        |after, events| async move {
                            process_knowledge_view_events(client, space, after, &events).await
                        },
                    ).await;
                    if let Err(error) = delivery {
                        warn!(%space_id, %error, "Outbox batch failed; checkpoint retained for replay");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Acknowledgment transfers a complete event batch into durable work. All
/// canonical mutations run later under the worker's original attempt fence.
async fn process_truth_maintenance_events(
    storage: &StorageClient,
    space_id: &str,
    events: &[SystemEvent],
) -> anyhow::Result<()> {
    let Some(last) = events.last() else {
        return Ok(());
    };
    if events.iter().any(|event| event.base.space_id != space_id) {
        anyhow::bail!("Truth-maintenance delivery crosses spaces");
    }
    let first = events
        .first()
        .ok_or_else(|| anyhow::anyhow!("Missing first event"))?;
    let work_item = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            space_id.to_string(),
            "truth_maintenance".to_string(),
        ),
        kind: TM_DETECT_CONFLICTS_KIND.to_string(),
        payload: serde_json::json!({"event_ids":events.iter().map(|event| event.base.id.to_string()).collect::<Vec<_>>()}),
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!("tm_detect_conflicts:{}:{}", first.seq, last.seq),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    elephant_pipeline::source_queue::enqueue_idempotent(storage, work_item).await?;
    Ok(())
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn run_truth_maintenance_loop(
    storage: StorageClient,
    config: TruthMaintenanceWorkerConfig,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let event_repo = SystemEventRepository::new(storage.clone());
    loop {
        tokio::select! {
            _ = rx.changed() => {
                if *rx.borrow() { break; }
            }
            () = tokio::time::sleep(config.poll_interval()) => {
                let spaces = match event_repo.get_all_spaces().await {
                    Ok(spaces) => spaces,
                    Err(error) => {
                        error!(%error, "Cannot discover truth-maintenance outbox spaces");
                        continue;
                    }
                };
                for space_id in spaces {
                    let client = &storage;
                    let space = &space_id;
                    let delivery: anyhow::Result<usize> = elephant_pipeline::outbox_delivery::deliver_batch(
                        client, space, "truth_maintenance", config.batch_size,
                        |_, events| async move {
                            process_truth_maintenance_events(client, space, &events).await
                        },
                    ).await;
                    if let Err(error) = delivery {
                        warn!(%space_id, %error, "Truth-maintenance delivery failed; checkpoint retained for replay");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn complete_work_item_and_promote(
    work_queue: &WorkQueue,
    dag_executor: &DagExecutor,
    space_id: &str,
    item_id: &RecordId,
) -> Result<(), PipelineError> {
    work_queue.complete(space_id, item_id).await?;
    dag_executor.on_item_completed(space_id, item_id).await?;
    Ok(())
}

/// How often the embed loop logs each space's embedding progress.
const EMBED_PROGRESS_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(900);

/// Logs one space's embedding progress for the operator.
///
/// Counts are derived from space content, so they go to the server log under
/// the `elephant::embedding_progress` target at debug, never to an API. Opt in
/// with `RUST_LOG=...,elephant::embedding_progress=debug`; the default `info`
/// filter leaves it off, and while it is off the loop never runs the count.
async fn report_embedding_progress(node_repo: &DocNodeRepository, space_id: &str, model: &str) {
    match node_repo.count_embedding_progress(space_id).await {
        Ok(counts) => tracing::debug!(
            target: "elephant::embedding_progress",
            space_id,
            pending = counts.pending,
            embedded = counts.embedded(),
            unembeddable = counts.unembeddable,
            embedded_by_model = ?counts.embedded_by_model,
            active_model = model,
            "embedding progress"
        ),
        Err(err) => warn!("Embedding progress count failed for space {space_id}: {err}"),
    }
}

/// Helper function to run the embed worker loop.
#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
async fn run_embed_loop(
    storage: StorageClient,
    doc_repo: DocRepository,
    node_repo: DocNodeRepository,
    revision_repo: DocRevisionRepository,
    embedding_repo: elephant_storage::NodeEmbeddingRepository,
    space_config_repo: SpaceConfigRepository,
    _provider_cache: EmbeddingProviderCache,
    embed_config: EmbedWorkerConfig,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut last_progress_report: Option<std::time::Instant> = None;
    loop {
        tokio::select! {
            _ = rx.changed() => {
                if *rx.borrow() {
                    info!("Embed worker received shutdown signal");
                    break;
                }
            }
            () = tokio::time::sleep(embed_config.poll_interval()) => {
                let spaces = match doc_repo.get_all_spaces().await {
                    Ok(spaces) => spaces,
                    Err(error) => { error!(%error, "Cannot discover embedding spaces"); continue; }
                };
                // The count walks doc_node: only pay for it when the operator
                // opted in to the debug progress target (off under the default
                // `info` filter).
                let report_progress = tracing::enabled!(target: "elephant::embedding_progress", Level::DEBUG)
                    && last_progress_report
                        .is_none_or(|at| at.elapsed() >= EMBED_PROGRESS_REPORT_INTERVAL);
                if report_progress {
                    last_progress_report = Some(std::time::Instant::now());
                }

                for space_id in spaces {
                    let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_activity(storage.clone(), &space_id, "embed_batch");
                    let context = ProviderDispatchContext::new(space_config_repo.clone(), space_id.clone()).with_observer(observer);
                    let (policy, provider, _binding) = match context.embedding(ModelOperation::DocumentEmbedding).await {
                        Ok(binding) => binding,
                        Err(error) => { warn!(%space_id, %error, "Embedding blocked by space configuration"); continue; }
                    };
                    let worker = EmbedWorker::new(
                        node_repo.clone(),
                        revision_repo.clone(),
                        embedding_repo.clone(),
                        provider,
                    );

                    let outcome: anyhow::Result<_> = async {
                        Ok(storage.with_activity(worker.process_pending(&space_id, embed_config.batch_size)).await??)
                    }.await;
                    match outcome {
                        Ok(result) => {
                            if result.chunks_processed > 0 {
                                info!(
                                    "Embed worker processed {} nodes for space {} with model {}",
                                    result.chunks_processed, space_id, result.model
                                );
                            }
                        }
                        Err(e) => {
                            error!("Embed worker error for space {}: {}", space_id, e);
                        }
                    }
                    if report_progress {
                        report_embedding_progress(&node_repo, &space_id, &policy.model).await;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Application entry point.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Elephant Pipeline worker");
    validate_backend_env()?;

    // Load configuration
    let config = PipelineConfig::from_env()?;
    info!(
        "Configuration loaded: backend={}, db_url={}, namespace={}, database={}",
        config.db_backend.as_str(),
        config.db_url,
        config.db_namespace,
        config.db_database
    );

    if config.db_backend == StorageBackendKind::ElephantDb {
        anyhow::bail!(
            "Standalone elephant-pipeline does not support ELEPHANT_DB=elephant-db. Run elephant-supervisor for embedded mode."
        );
    }

    // Initialize storage client
    let storage_config = StorageConfig {
        backend: config.db_backend,
        url: config.db_url.clone(),
        username: config.db_user.clone(),
        password: config.db_pass.clone(),
        namespace: config.db_namespace.clone(),
        database: config.db_database.clone(),
        snapshot_load: config.db_snapshot_load.clone(),
        ..StorageConfig::default()
    }
    .with_env_timeouts()?;
    let storage_client = StorageClient::new(storage_config).await?;
    info!("Connected to database");

    run_pipeline_with_client(config, storage_client).await
}

/// Runs the full pipeline runtime with its own Ctrl-C shutdown watcher.
///
/// # Errors
///
/// Returns an error if any worker or runtime dependency fails.
pub async fn run_pipeline_with_client(
    config: PipelineConfig,
    storage_client: StorageClient,
) -> anyhow::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    let result = run_pipeline_with_shutdown(config, storage_client, shutdown_rx).await;
    signal_task.abort();
    result
}

/// Runs the full pipeline runtime using a caller-provided shutdown channel.
///
/// # Errors
///
/// Returns an error if any worker or runtime dependency fails.
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Preserve the original repository lifetimes and future ownership"
)]
pub async fn run_pipeline_with_shutdown(
    config: PipelineConfig,
    storage_client: StorageClient,
    shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut shutdown_rx = shutdown_rx;
    apply_multimodal_runtime_env(&config.multimodal);
    // Create worker tasks
    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // Spawn embed worker
    if config.embed.enabled {
        let client = storage_client.clone();
        let embed_config = config.embed.clone();
        let rx = shutdown_rx.clone();

        tasks.spawn(async move {
            info!("Embed worker started");
            let doc_repo = DocRepository::new(client.clone());
            let node_repo = DocNodeRepository::new(client.clone());
            let revision_repo = DocRevisionRepository::new(client.clone());
            let partition_manager = elephant_embeddings::PartitionManager::new();
            let embedding_repo =
                elephant_storage::NodeEmbeddingRepository::new(client.clone(), partition_manager);
            let space_config_repo = SpaceConfigRepository::new(client.clone());
            let provider_cache = EmbeddingProviderCache::new();

            run_embed_loop(
                client.clone(),
                doc_repo,
                node_repo,
                revision_repo,
                embedding_repo,
                space_config_repo,
                provider_cache,
                embed_config,
                rx,
            )
            .await
        });
    }

    // Archive scheduler history independently: an unrelated legacy DELETE
    // timing out must never prevent the next bounded archival pass.
    if config.retention.enabled {
        let client = storage_client.clone();
        let retention_config = config.retention.clone();
        let mut rx = shutdown_rx.clone();
        tasks.spawn(async move {
            let worker = elephant_pipeline::retention::RetentionWorker::new(
                client.clone(),
                retention_config.retention_period(),
            );
            let batch_size = retention_config.work_history_batch_size();
            let mut interval = tokio::time::interval(retention_config.work_history_poll_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            info!(batch_size, "Work history archival worker started");
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let outcome: anyhow::Result<_> = async { Ok(client.with_activity(worker.process_work_history(batch_size)).await??) }.await;
                        match outcome {
                            Ok(result) => {
                                if result.items_archived > 0 || result.runs_archived > 0 {
                                    info!(
                                        items = result.items_archived,
                                        attempts = result.runs_archived,
                                        "Archived terminal work history"
                                    );
                                }
                            }
                            Err(error) => error!(%error, "Work history archival failed"),
                        }
                    }
                }
            }
            Ok(())
        });
    }

    // Spawn retention worker
    if config.retention.enabled {
        let client = storage_client.clone();
        let retention_config = config.retention.clone();
        let mut rx = shutdown_rx.clone();

        tasks.spawn(async move {
            info!(
                "Retention worker started with retention period: {} days",
                retention_config.retention_days
            );
            let worker = elephant_pipeline::retention::RetentionWorker::new(
                client.clone(),
                retention_config.retention_period(),
            );

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!("Retention worker received shutdown signal");
                            break;
                        }
                    }
                    () = tokio::time::sleep(retention_config.poll_interval()) => {
                        let outcome: anyhow::Result<_> = async { Ok(client.with_activity(worker.process_cleanup()).await??) }.await;
                        match outcome {
                            Ok(result) => {
                                if result.total_deleted > 0 {
                                    info!(
                                        "Retention worker cleaned up {} records",
                                        result.total_deleted
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Retention worker error: {}", e);
                            }
                        }
                    }
                }
            }
            Ok(())
        });
    }

    // Knowledge-view invalidation and Living Wiki scheduling are independent
    // of truth maintenance. Keeping this watcher first-class means disabling
    // conflict resolution cannot silently disable wiki freshness.
    {
        let client = storage_client.clone();
        let rx = shutdown_rx.clone();
        let poll_seconds = env::var("ELEPHANT_KNOWLEDGE_VIEW_EVENT_POLL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5)
            .clamp(1, 300);
        tasks.spawn(async move {
            info!("Knowledge-view invalidation worker started");
            run_knowledge_view_invalidation_loop(
                client,
                std::time::Duration::from_secs(poll_seconds),
                rx,
            )
            .await
        });
    }

    // Spawn truth maintenance worker
    if config.truth_maintenance.enabled {
        let client = storage_client.clone();
        let truth_config = config.truth_maintenance.clone();
        let rx = shutdown_rx.clone();

        tasks.spawn(async move {
            info!("Truth maintenance worker started");
            Box::pin(run_truth_maintenance_loop(client, truth_config, rx)).await
        });
    }

    // Spawn work queue worker
    if config.work_queue.enabled {
        let client = storage_client.clone();
        let queue_config = config.work_queue.clone();
        let embed_config = config.embed.clone();
        let rx = shutdown_rx.clone();

        tasks.spawn(async move {
            info!("Work queue worker started");
            let space_config_repo = SpaceConfigRepository::new(client.clone());
            let provider_cache = EmbeddingProviderCache::new();
            Box::pin(run_work_queue_loop(
                client,
                queue_config,
                space_config_repo,
                provider_cache,
                embed_config,
                rx,
            ))
            .await
        });
    }

    // Spawn maintenance worker (identity resolution, entity type consolidation, predicate hygiene)
    if config.maintenance.enabled {
        let client = storage_client.clone();
        let maintenance_config = config.maintenance.clone();
        let mut rx = shutdown_rx.clone();

        tasks.spawn(async move {
            info!(
                "Maintenance worker started (poll interval: {}s)",
                maintenance_config.poll_interval_secs
            );

            // Convert pipeline config to maintenance config
            let maint_cfg = elephant_pipeline::maintenance::MaintenanceConfig {
                mcp_url: maintenance_config.mcp_url.clone(),
                model: maintenance_config.model.clone(),
                auto_merge_threshold: maintenance_config.auto_merge_threshold,
                max_candidates_per_sweep: maintenance_config.max_candidates_per_sweep,
            };

            // Get list of spaces to sweep
            let space_config_repo = SpaceConfigRepository::new(client.clone());


            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!("Maintenance worker received shutdown signal");
                            break;
                        }
                    }
                    () = tokio::time::sleep(maintenance_config.poll_interval()) => {
                        let space_ids = match list_configured_space_ids(&space_config_repo).await {
                            Ok(spaces) => spaces,
                            Err(error) => { error!(%error, "Cannot enumerate maintenance spaces"); continue; }
                        };
                        for space_id in &space_ids {
                            let observer = elephant_pipeline::provider_usage::DurableUsageObserver::for_activity(client.clone(), space_id, "maintenance_sweep");
                            let context = ProviderDispatchContext::new(space_config_repo.clone(), space_id.clone()).with_observer(observer.clone());
                            let (_policy, embedding_provider, _binding) = match context.embedding(ModelOperation::DocumentEmbedding).await {
                                Ok(binding) => binding,
                                Err(error) => { warn!(%space_id, %error, "Maintenance embedding blocked by configuration"); continue; }
                            };
                            let outcome: anyhow::Result<_> = async { Ok(client.with_activity(
                                elephant_pipeline::provider_dispatch::with_dispatch_observer(observer, Box::pin(elephant_pipeline::maintenance::run_maintenance_sweep_with_attempt(
                                &client,
                                space_id,
                                &maint_cfg,
                                embedding_provider.as_ref(),
                                None,
                                None,
                            )))).await??) }.await;
                            match outcome {
                                Ok(result) => {
                                    if result.identity.candidates_evaluated > 0
                                        || result.entity_hygiene.retired > 0
                                        || result.entity_hygiene.relationships_deleted > 0
                                        || result.entity_hygiene.assertions_superseded > 0
                                        || result.assertions.superseded > 0
                                        || result.relationships.merged > 0
                                        || result.predicate.proposals_created > 0
                                        || result.predicate.aliases_created > 0
                                        || result.predicate.registered > 0
                                        || result.entity_type.reclassified > 0
                                        || result.entity_embeddings.embedded > 0
                                        || result.entity_embeddings.failed > 0
                                        || result.failed_work_recovery.examined > 0
                                    {
                                        // warn, not info: production runs RUST_LOG=warn and
                                        // this line is the only evidence a sweep did work.
                                        warn!(
                                            "Maintenance sweep for '{space_id}': {result}",
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Maintenance sweep failed for '{space_id}': {e}",
                                    );
                                }
                            }

                            let source_watch_repo = SourceWatchRepository::new(client.clone());
                            match source_watch_repo.list_due(space_id, Utc::now(), 100).await {
                                Ok(watches) => {
                                    for watch in watches {
                                        if let Err(error) = enqueue_source_watch_refresh(
                                            &client,
                                            &watch,
                                            "pipeline.source_watch_scheduler",
                                            5,
                                        ).await {
                                            error!(
                                                watch_id = %watch.base.id,
                                                "Failed to enqueue due source watch: {error}"
                                            );
                                        }
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Maintenance worker failed to list due source watches for '{space_id}': {error}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        });
    }

    info!(
        "Pipeline workers started: embed={}, truth_maintenance={}, work_queue={}, maintenance={}",
        config.embed.enabled,
        config.truth_maintenance.enabled,
        config.work_queue.enabled,
        config.maintenance.enabled
    );

    pipeline_shutdown::wait(&mut tasks, &mut shutdown_rx).await?;

    info!("All workers stopped. Goodbye!");
    Ok(())
}

fn apply_multimodal_runtime_env(config: &config::MultimodalRenderConfig) {
    std::env::set_var("PIPELINE_MULTIMODAL_RENDER_MODEL", &config.model);
    std::env::set_var(
        "PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS",
        config.timeout_secs.to_string(),
    );
    std::env::set_var(
        "PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT",
        config.max_render_concurrent.to_string(),
    );
    std::env::set_var("PIPELINE_MULTIMODAL_ASSET_ROOT", &config.asset_root);
    std::env::set_var(
        "PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES",
        config.inline_image_max_bytes.to_string(),
    );
    std::env::set_var(
        "PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES",
        config.inline_video_max_bytes.to_string(),
    );
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_config_loads() {
        let config = PipelineConfig::default();
        assert!(config.embed.enabled);
        assert!(config.promote.enabled);
    }

    #[test]
    fn knowledge_view_event_records_detects_semantic_graph_changes() {
        let entity_id = RecordId::new_v4(TableName::Entity);
        let view_id = RecordId::new_v4(TableName::KnowledgeView);
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                "space".to_string(),
                "test".to_string(),
            ),
            seq: 1,
            event_type: "records.changed".to_string(),
            payload: serde_json::json!({
                "records": [
                    {"id": entity_id, "table": "entity", "op": "update"},
                    {"id": view_id, "table": "knowledge_view", "op": "update"}
                ]
            }),
        };

        let (graph_changed, affected) =
            knowledge_view_event_records(&[event]).expect("valid event");

        assert!(graph_changed);
        assert!(affected.contains(&entity_id.to_string()));
        assert!(affected.contains(&"trigger:table:entity".to_string()));
        assert!(affected.contains(&view_id.to_string()));
        assert!(affected.contains(&"trigger:table:knowledge_view".to_string()));
    }

    #[tokio::test]
    async fn dedicated_stack_polls_the_future_on_the_named_thread() {
        let thread_name =
            run_on_dedicated_stack("living-wiki-stack-test", 8 * 1024 * 1024, async {
                std::thread::current().name().map(ToString::to_string)
            })
            .await
            .expect("dedicated stack");

        assert_eq!(thread_name.as_deref(), Some("living-wiki-stack-test"));
    }
}
