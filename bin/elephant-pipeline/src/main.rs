#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Elephant Pipeline Worker
//!
//! Background worker for document embedding generation, truth maintenance, and the work queue scheduler.
//!
//! This binary runs worker loops:
//! - **`EmbedWorker`**: Polls for nodes without embeddings and generates them
//! - **Work queue**: Processes DAG-scheduled work items (render, plan, extract, truth maintenance)
//! - **Truth maintenance**: Detects conflicts from outbox events and resolves truth slots

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use elephant_embeddings::{EmbeddingPolicy, EmbeddingProviderCache};
use elephant_pipeline::renderer::default::{DefaultTextRenderer, PassthroughRenderer};
use elephant_pipeline::renderer::multimodal::MultimodalRenderer;
use elephant_pipeline::renderer::RendererRegistry;
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
use elephant_pipeline::{
    error::PipelineError,
    extraction::Extractor,
    freshness::{execute_freshness_sweep, WORK_KIND_FRESHNESS_SWEEP},
    reprocess::{
        execute_reprocess_job, ReprocessConfig, ReprocessExecutionContext,
        WORK_KIND_EXECUTE_REPROCESS_JOB,
    },
    resolution::{
        apply_resolution_patch, commit_resolution_patches, validate_resolution_patches,
        BackoffConfig, ResolutionPatchWorkflow,
    },
    scheduler::DagExecutor,
    scheduler::{
        execute_build_doc_tree, execute_extract_assertions, execute_ingest_metadata,
        execute_ingest_source_stub, execute_plan_extraction, execute_render_doc,
        execute_triage_source, execute_upgrade_ingest_mode, promote_assertions_inline_with_options,
        BudgetEnforcer, DailyCapCheck, DefaultTriagePlugin, DocTreeConfig, ExtractionPlanConfig,
        StagedCommit, WorkQueue, COMMIT_ARTIFACTS_KIND, EXTRACT_ASSERTIONS_KIND,
        INGEST_METADATA_KIND, INGEST_SOURCE_STUB_KIND, PLAN_EXTRACTION_KIND, RENDER_DOC_KIND,
        TRIAGE_SOURCE_KIND, UPGRADE_INGEST_MODE_KIND, VALIDATE_ARTIFACTS_KIND,
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
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{Budget, DowngradePolicy, IngestMode};
use elephant_types::records::source::{Source, SourceLifecycleStatus};
use elephant_types::records::work::{
    ArtifactStatus, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::{Doc, SystemEvent};
use elephant_types::{Link, RecordId};

pub mod config;

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
use elephant_embeddings::EmbeddingProvider;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[cfg(feature = "meerkat-extraction")]
use elephant_pipeline::extraction::{AgenticExtractor, AgenticExtractorConfig};
#[cfg(feature = "meerkat-extraction")]
use elephant_pipeline::wiki_editorial::execute_managed_wiki_reconciliation;

const AGENTIC_EXTRACTOR_ID: &str = "elephant.extractor.agentic";
const MANAGED_WIKI_TIMEOUT_SECS: u64 = 4 * 60 * 60;
const MANAGED_WIKI_LEASE_GRACE_SECS: u64 = 60;

fn policy_from_embed_config(config: &EmbedWorkerConfig) -> EmbeddingPolicy {
    EmbeddingPolicy::new(config.provider.clone(), config.model.clone(), config.dims)
}

fn build_semantic_extractor(extractor_id: Option<&str>) -> anyhow::Result<Box<dyn Extractor>> {
    let extractor_id = extractor_id.unwrap_or(AGENTIC_EXTRACTOR_ID);

    #[cfg(feature = "meerkat-extraction")]
    {
        if extractor_id == AGENTIC_EXTRACTOR_ID {
            let mcp_url = std::env::var("PIPELINE_AGENTIC_MCP_URL")
                .unwrap_or_else(|_| "http://localhost:3000/mcp/agent".to_string());
            let model = std::env::var("PIPELINE_AGENTIC_MODEL")
                .unwrap_or_else(|_| "gpt-5.6-sol".to_string());
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

fn is_transient_work_error(error: &anyhow::Error) -> bool {
    fn is_transient_message(message: &str) -> bool {
        let normalized = message.to_ascii_lowercase();
        [
            "read or write conflict",
            "transaction can be retried",
            "temporarily unavailable",
            "timeout",
            "timed out",
            "rate limit",
            "too many requests",
            "connection reset",
            "broken pipe",
        ]
        .iter()
        .any(|pattern| normalized.contains(pattern))
    }

    fn is_transient_storage_error(error: &StorageError) -> bool {
        match error {
            StorageError::Conflict(_)
            | StorageError::ConnectionError(_)
            | StorageError::TransactionError(_) => true,
            StorageError::QueryError(message) => is_transient_message(message),
            StorageError::Surreal(inner) => is_transient_message(&inner.to_string()),
            StorageError::NotFound(_)
            | StorageError::SerializationError(_)
            | StorageError::DataIntegrityError(_)
            | StorageError::MigrationError(_)
            | StorageError::TypeError(_) => false,
        }
    }

    fn is_transient_pipeline_error(error: &PipelineError) -> bool {
        match error {
            PipelineError::LeaseConflict(_) => true,
            PipelineError::StorageError(storage_error) => is_transient_storage_error(storage_error),
            PipelineError::IngestionError(_)
            | PipelineError::ExtractionError(_)
            | PipelineError::PromotionError(_)
            | PipelineError::OutboxError(_)
            | PipelineError::EmbeddingError(_)
            | PipelineError::Duplicate(_)
            | PipelineError::NotFound(_)
            | PipelineError::InvalidStateTransition { .. }
            | PipelineError::CycleDetected
            | PipelineError::SecurityViolation(_)
            | PipelineError::ConfigError(_)
            | PipelineError::BudgetExceeded(_)
            | PipelineError::BudgetBreach { .. }
            | PipelineError::PerItemCapExceeded { .. }
            | PipelineError::InvalidInput(_)
            | PipelineError::ValidationError(_)
            | PipelineError::InvalidState(_) => false,
        }
    }

    // error.chain() includes the root error as its first element,
    // so there's no need to check downcast_ref separately before the loop.
    for cause in error.chain() {
        if let Some(pipeline_error) = cause.downcast_ref::<PipelineError>() {
            if is_transient_pipeline_error(pipeline_error) {
                return true;
            }
        }
        if let Some(storage_error) = cause.downcast_ref::<StorageError>() {
            if is_transient_storage_error(storage_error) {
                return true;
            }
        }
    }

    is_transient_message(&error.to_string())
}

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

    use super::*;
    use chrono::Utc;
    use elephant_pipeline::scheduler::extraction_work_kinds::SerializedRawAssertion;
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

    const SCHEMA: &str = r"
        DEFINE TABLE doc SCHEMALESS;
        DEFINE TABLE doc_chunk SCHEMALESS;
        DEFINE TABLE doc_revision SCHEMALESS;
        DEFINE TABLE doc_node SCHEMALESS;
        DEFINE TABLE source SCHEMALESS;
        DEFINE TABLE ingest_decision SCHEMALESS;
        DEFINE TABLE work_artifact SCHEMALESS;
        DEFINE TABLE work_item SCHEMALESS;
        DEFINE TABLE work_edge SCHEMALESS;
        DEFINE TABLE work_run SCHEMALESS;
        DEFINE TABLE assertion SCHEMALESS;
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
        DEFINE TABLE event SCHEMALESS;
        DEFINE TABLE conflict_group SCHEMALESS;
        DEFINE TABLE attribute SCHEMALESS;
        DEFINE TABLE truth_slot SCHEMALESS;
        DEFINE TABLE audit_log SCHEMALESS;
        DEFINE TABLE predicate_registry SCHEMALESS;
        DEFINE TABLE predicate_alias SCHEMALESS;
        DEFINE TABLE system_event SCHEMALESS;
        DEFINE TABLE reprocess_job SCHEMALESS;
        DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id, seq UNIQUE;
    ";

    async fn create_revision_with_node(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        text: &str,
    ) -> (RecordId, RecordId) {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let revision = DocRevision {
            base: BaseFields::new(revision_id.clone(), space_id.to_string(), actor.to_string()),
            doc: Link::new(doc_id),
            text: text.to_string(),
            text_hash: "hash".to_string(),
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
            text_hash: "source-backed-hash".to_string(),
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

    async fn create_work_run(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        work_item_id: &RecordId,
    ) -> RecordId {
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
        let (revision_id, node_id) = evidence_ids;
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
                    end: 10,
                    quote_hash: "pending-assertion".to_string(),
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
    async fn test_build_embedding_provider_supports_hashed() {
        let policy = EmbeddingPolicy::new("hashed", "hashed-bow-v1", 32);
        let provider = super::build_embedding_provider(&policy);
        let vectors = provider
            .embed(&["Alice visits Wonderland".to_string()])
            .await
            .unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].len(), 32);
        assert!(vectors[0].iter().any(|value| *value != 0.0));
    }

    #[tokio::test]
    async fn test_execute_work_item_plan_extraction_enqueues_extract() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "plan_space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme.").await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: PLAN_EXTRACTION_KIND.to_string(),
            payload: serde_json::json!({
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph",
                "policy_version": "v1.0"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "plan-extraction-test".to_string(),
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

        let _ = execute_work_item(
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
        .expect("execute plan_extraction");

        let node_id_str = node_id.to_string();
        let revision_id_str = revision_id.to_string();
        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");

        let extract_items: Vec<_> = items
            .iter()
            .filter(|item| item.kind == EXTRACT_ASSERTIONS_KIND)
            .collect();
        assert_eq!(extract_items.len(), 1);
        assert_eq!(
            extract_items[0]
                .payload
                .get("node_id")
                .and_then(|v| v.as_str()),
            Some(node_id_str.as_str())
        );
        assert_eq!(
            extract_items[0]
                .payload
                .get("revision_id")
                .and_then(|v| v.as_str()),
            Some(revision_id_str.as_str())
        );
    }

    #[tokio::test]
    async fn test_execute_work_item_plan_extraction_completes_missing_revision_as_obsolete() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let metrics = execute_work_item(
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
        .expect("obsolete plan_extraction should complete");

        assert_eq!(metrics.tokens_used, 0);
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
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "plan_source_space";
        let actor = "tester";
        let (revision_id, _node_id, source_id) = create_source_backed_revision_with_node(
            &storage,
            space_id,
            actor,
            "Alice works at Acme.",
        )
        .await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: PLAN_EXTRACTION_KIND.to_string(),
            payload: serde_json::json!({
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph",
                "policy_version": "v1.0"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "plan-extraction-source-test".to_string(),
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

        execute_work_item(
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
        .expect("execute source-backed plan_extraction");

        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");
        let extract_item = items
            .iter()
            .find(|item| item.kind == EXTRACT_ASSERTIONS_KIND)
            .expect("extract item");
        let source_id_str = source_id.to_string();

        assert_eq!(
            extract_item
                .payload
                .get("source_id")
                .and_then(|value| value.as_str()),
            Some(source_id_str.as_str())
        );
    }

    #[tokio::test]
    async fn test_execute_work_item_render_doc_enqueues_plan_extraction() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "default";
        let actor = "tester";

        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = elephant_types::records::Doc {
            base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
            title: "Test Doc".to_string(),
            text: "Alice works at Acme.".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
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
            payload: serde_json::json!({ "doc_id": doc_id.to_string() }),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: "render-doc-test".to_string(),
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

        let _ = execute_work_item(
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

        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 50, None)
            .await
            .expect("list work items");

        let plan_items: Vec<_> = items
            .iter()
            .filter(|item| item.kind == PLAN_EXTRACTION_KIND)
            .collect();
        assert_eq!(plan_items.len(), 1);
        assert!(plan_items[0]
            .payload
            .get("revision_id")
            .and_then(|v| v.as_str())
            .is_some());
    }

    #[tokio::test]
    async fn test_render_doc_does_not_regress_graph_ready_source_mode() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let _ = execute_work_item(
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
    async fn test_execute_work_item_ingest_source_stub_accepts_discovered_source() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        execute_work_item(
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
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "extract_space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme Corp.").await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: EXTRACT_ASSERTIONS_KIND.to_string(),
            payload: serde_json::json!({
                "node_id": node_id.to_string(),
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "extract-assertions-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;
        let config = WorkQueueWorkerConfig::default();

        let _ = execute_work_item_with_extractor(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &TmContext::none(),
            Some(&FixtureExtractor::new()),
        )
        .await
        .expect("execute extract_assertions");

        // Verify the knowledge_candidates artifact was created by extract_assertions.
        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        let artifacts = artifact_repo
            .list(space_id, 10, None)
            .await
            .expect("list artifacts");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_type, "knowledge_candidates");

        // No outbox events should exist until commit_artifacts materializes canonical knowledge.
        let event_repo = SystemEventRepository::new(storage.clone());
        let events = event_repo
            .get_after_seq(space_id, 0, 10)
            .await
            .expect("get outbox events");
        assert!(
            events.is_empty(),
            "extract_assertions should only stage candidate artifacts"
        );

        // The staged path should enqueue validation for the new artifact.
        let item_repo = WorkItemRepository::new(storage);
        let queued = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list queued work items");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].kind, VALIDATE_ARTIFACTS_KIND);
    }

    #[tokio::test]
    async fn test_execute_work_item_extract_assertions_derives_source_for_artifact_workflow() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "extract_source_space";
        let actor = "tester";
        let (revision_id, node_id, source_id) = create_source_backed_revision_with_node(
            &storage,
            space_id,
            actor,
            "Alice works at Acme Corp.",
        )
        .await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: EXTRACT_ASSERTIONS_KIND.to_string(),
            payload: serde_json::json!({
                "node_id": node_id.to_string(),
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "extract-source-test".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };

        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;
        let config = WorkQueueWorkerConfig::default();

        execute_work_item_with_extractor(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &TmContext::none(),
            Some(&FixtureExtractor::new()),
        )
        .await
        .expect("execute source-backed extract_assertions");

        let item_repo = WorkItemRepository::new(storage);
        let queued = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list queued work items");
        let validate_item = queued
            .iter()
            .find(|item| item.kind == VALIDATE_ARTIFACTS_KIND)
            .expect("validate item");
        let source_id_str = source_id.to_string();

        assert_eq!(
            validate_item
                .payload
                .get("source_id")
                .and_then(|value| value.as_str()),
            Some(source_id_str.as_str())
        );
    }

    #[tokio::test]
    async fn test_extract_to_truth_maintenance_chain() {
        use elephant_pipeline::truth_maintenance::{
            execute_tm_detect_conflicts_from_outbox_events, RepositoryTruthMaintenanceStorage,
        };

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        // Also define entity and rel tables for staged artifact commit promotion.
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE rel SCHEMALESS; DEFINE TABLE attribute SCHEMALESS;")
            .await
            .expect("entity/rel/attr schema");

        let space_id = "chain_space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme Corp.").await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: EXTRACT_ASSERTIONS_KIND.to_string(),
            payload: serde_json::json!({
                "node_id": node_id.to_string(),
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "chain-test".to_string(),
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

        // Step 1: Execute extraction (stages knowledge candidates and validation work)
        let _ = execute_work_item_with_extractor(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &TmContext::none(),
            Some(&FixtureExtractor::new()),
        )
        .await
        .expect("execute extract_assertions");

        let item_repo = WorkItemRepository::new(storage.clone());
        let queued_after_extract = item_repo
            .list(space_id, 20, None)
            .await
            .expect("queued after extract");
        assert_eq!(queued_after_extract.len(), 1);
        assert_eq!(queued_after_extract[0].kind, VALIDATE_ARTIFACTS_KIND);

        let validate_item = queued_after_extract[0].clone();
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        let _ = execute_work_item(
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

        let queued_after_validate = item_repo
            .list(space_id, 20, None)
            .await
            .expect("queued after validate");
        assert!(
            queued_after_validate
                .iter()
                .any(|item| item.kind == COMMIT_ARTIFACTS_KIND),
            "validate_artifacts should enqueue commit_artifacts"
        );

        let commit_item = queued_after_validate
            .into_iter()
            .find(|item| item.kind == COMMIT_ARTIFACTS_KIND)
            .expect("commit item");
        let commit_run_id = create_work_run(&storage, space_id, actor, &commit_item.base.id).await;
        let _ = execute_work_item(
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

        // Step 2: Verify outbox events exist after commit
        let event_repo = SystemEventRepository::new(storage.clone());
        let events = event_repo
            .get_after_seq(space_id, 0, 100)
            .await
            .expect("get outbox events");
        assert!(
            !events.is_empty(),
            "Should have outbox events after extraction"
        );

        // Step 3: Run truth maintenance on outbox events
        let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
        let tm_result = execute_tm_detect_conflicts_from_outbox_events(&events, &tm_storage)
            .await
            .expect("truth maintenance detect");

        // For a single document's assertions (no conflicts), we expect direct commits
        // or no-ops (if no rels/attrs were created by the DefaultExtractor).
        assert!(
            tm_result.events_processed > 0,
            "Truth maintenance should have processed events"
        );

        // Step 4: Execute direct commits to populate truth_slots
        for commit in &tm_result.direct_commits {
            elephant_pipeline::truth_maintenance::execute_tm_commit(
                &commit.resolution,
                &commit.input,
                space_id,
                &tm_storage,
            )
            .await
            .expect("tm_commit");
        }

        // Step 5: Verify truth_slots exist if there were commits
        if !tm_result.direct_commits.is_empty() {
            let slot_result: Vec<serde_json::Value> = storage
                .query_bind(
                    "SELECT count() FROM truth_slot WHERE space_id = $space_id GROUP ALL",
                    serde_json::json!({ "space_id": space_id }),
                )
                .await
                .expect("count truth_slots")
                .take(0)
                .expect("take result");

            let slot_count = slot_result
                .first()
                .and_then(|v| v.get("count"))
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);

            assert!(
                slot_count > 0,
                "Truth slots should be created after direct commits"
            );
        }
    }

    #[tokio::test]
    async fn test_source_metadata_mode_records_metadata_artifact_without_doc_materialization() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let _ = execute_work_item(
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
        let _ = execute_work_item(
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
        let _ = execute_work_item(
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
        let _ = execute_work_item(
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
    async fn test_graph_mode_metadata_commit_enqueues_upgrade_without_marking_graph_ready() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        execute_work_item(
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
        execute_work_item(
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
        execute_work_item(
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
        execute_work_item(
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
        execute_work_item(
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
    async fn test_execute_work_item_ingest_metadata_skips_when_source_not_triaged() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        execute_work_item(
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
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "artifact-boundary-space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme Corp.").await;

        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: EXTRACT_ASSERTIONS_KIND.to_string(),
            payload: serde_json::json!({
                "node_id": node_id.to_string(),
                "revision_id": revision_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "artifact-boundary-test".to_string(),
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

        let _ = execute_work_item_with_extractor(
            &work_item,
            &work_run_id,
            storage.clone(),
            &config,
            &TmContext::none(),
            Some(&FixtureExtractor::new()),
        )
        .await
        .expect("execute extract_assertions");

        let item_repo = WorkItemRepository::new(storage.clone());
        let validate_item = item_repo
            .list(space_id, 20, None)
            .await
            .expect("list queued work items")
            .into_iter()
            .find(|item| item.kind == VALIDATE_ARTIFACTS_KIND)
            .expect("validate item");
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        let _ = execute_work_item(
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

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        let artifacts = artifact_repo
            .find_by_work_run(&work_run_id)
            .await
            .expect("find artifacts");
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.artifact_type == KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE),
            "knowledge candidates should still be present"
        );
        let commit_bundle = artifacts
            .iter()
            .find(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE)
            .expect("commit bundle");
        assert_eq!(commit_bundle.status, ArtifactStatus::Validated);

        let assertion_repo = AssertionRepository::new(storage.clone());
        let assertions = assertion_repo
            .list(space_id, 10, None)
            .await
            .expect("list assertions");
        assert!(
            assertions.is_empty(),
            "canonical assertions must not exist before commit_artifacts"
        );

        let event_repo = SystemEventRepository::new(storage);
        let events = event_repo
            .get_after_seq(space_id, 0, 10)
            .await
            .expect("list outbox events");
        assert!(
            events.is_empty(),
            "outbox should remain empty before commit_artifacts"
        );
    }

    #[tokio::test]
    async fn test_validate_artifacts_legacy_assertion_batch_alias_still_bundles() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "legacy-artifact-space";
        let actor = "tester";
        let (revision_id, node_id) =
            create_revision_with_node(&storage, space_id, actor, "Alice works at Acme Corp.").await;
        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_run_id = create_work_run(&storage, space_id, actor, &work_item_id).await;

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        artifact_repo
            .create(elephant_types::records::work::WorkArtifact {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkArtifact),
                    space_id.to_string(),
                    actor.to_string(),
                ),
                work_run: Link::new(work_run_id.clone()),
                artifact_type: LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE.to_string(),
                data: serde_json::to_value(ExtractionArtifactData {
                    source_id: None,
                    node_id: node_id.to_string(),
                    revision_id: revision_id.to_string(),
                    assertions: Vec::new(),
                    tokens_used: 0,
                    model: "legacy".to_string(),
                    revision_text_len: 0,
                })
                .expect("serialize legacy artifact"),
                status: ArtifactStatus::Draft,
                validation_errors: None,
                validated_at: None,
                committed_at: None,
            })
            .await
            .expect("create legacy artifact");

        let validate_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: VALIDATE_ARTIFACTS_KIND.to_string(),
            payload: serde_json::json!({
                "work_run_id": work_run_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "validate-legacy-assertion-batch".to_string(),
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

        execute_work_item(
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

        let artifacts = artifact_repo
            .find_by_work_run(&work_run_id)
            .await
            .expect("find artifacts");
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE),
            "legacy assertion_batch artifacts should still produce commit bundles"
        );
    }

    #[tokio::test]
    async fn test_commit_knowledge_candidates_refines_event_across_nodes_for_same_source() {
        use elephant_storage::EventRepository;
        use elephant_types::records::attribute::Literal;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "event-lineage-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Launch source",
            "Alice launched Project Phoenix.",
            IngestMode::Graph,
        )
        .await;
        let (revision_a, node_a) =
            create_revision_with_node(&storage, space_id, actor, "Alice launched Project Phoenix.")
                .await;
        let (revision_b, node_b) = create_revision_with_node(
            &storage,
            space_id,
            actor,
            "Project Phoenix launch date was confirmed later.",
        )
        .await;

        let mut first = elephant_pipeline::extraction::RawAssertion {
            subject_text: "Alice".to_string(),
            predicate_text: "launched".to_string(),
            object_text: Some("Project Phoenix".to_string()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(revision_a.clone()),
                node: Some(Link::new(node_a.clone())),
                start: 0,
                end: 30,
                quote_hash: "event-a".to_string(),
            },
            confidence: 0.95,
            quote: Some("Alice launched Project Phoenix.".to_string()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: Some("Project Phoenix launch".to_string()),
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("project".to_string()),
            significance: Some("high".to_string()),
        };
        let lineage_key = format!("source:{}:project-phoenix-launch", source.base.id);
        first.event_lineage_key = Some(lineage_key.clone());
        let mut second = first.clone();
        second.evidence_span = EvidenceSpan {
            revision: Link::new(revision_b.clone()),
            node: Some(Link::new(node_b.clone())),
            start: 0,
            end: 44,
            quote_hash: "event-b".to_string(),
        };
        second.object_text = None;
        second.object_literal = Some(Literal::String("Project Phoenix".to_string()));
        second.source_event_time = Some(
            chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
                .expect("parse time")
                .with_timezone(&Utc),
        );
        second.source_event_time_precision = Some(TimePrecision::Day);

        let data_first = ExtractionArtifactData {
            source_id: Some(source.base.id.to_string()),
            node_id: node_a.to_string(),
            revision_id: revision_a.to_string(),
            assertions: vec![SerializedRawAssertion::from_raw(&first).expect("serialize first")],
            tokens_used: 10,
            model: "test-model".to_string(),
            revision_text_len: 30,
        };
        let data_second = ExtractionArtifactData {
            source_id: Some(source.base.id.to_string()),
            node_id: node_b.to_string(),
            revision_id: revision_b.to_string(),
            assertions: vec![SerializedRawAssertion::from_raw(&second).expect("serialize second")],
            tokens_used: 12,
            model: "test-model".to_string(),
            revision_text_len: 44,
        };

        commit_knowledge_candidates_bundle(
            &storage,
            space_id,
            &RecordId::new_v4(TableName::WorkRun),
            actor,
            &data_first,
        )
        .await
        .expect("commit first bundle");
        commit_knowledge_candidates_bundle(
            &storage,
            space_id,
            &RecordId::new_v4(TableName::WorkRun),
            actor,
            &data_second,
        )
        .await
        .expect("commit second bundle");

        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list_recent(space_id, 10)
            .await
            .expect("list events");
        assert_eq!(
            events.len(),
            1,
            "same source lineage should refine one event"
        );
        let event = &events[0];
        assert_eq!(
            event.time_start,
            Some(
                chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
                    .expect("parse time")
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(event.time_precision, TimePrecision::Day);
        let lineage = event
            .metadata
            .get("lineage_key")
            .and_then(serde_json::Value::as_str)
            .expect("lineage key");
        assert!(
            lineage.starts_with(&format!("source:{}:", source.base.id)),
            "lineage should be source-scoped, got {lineage}"
        );
        assert_eq!(lineage, lineage_key);
    }

    #[tokio::test]
    async fn test_commit_knowledge_candidates_distinguishes_fallback_lineage_by_event_time() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "event-lineage-time-space";
        let actor = "tester";
        let source = create_manual_source(
            &storage,
            space_id,
            actor,
            "Launch source",
            "Alice launched Project Phoenix twice.",
            IngestMode::Graph,
        )
        .await;
        let (revision_a, node_a) =
            create_revision_with_node(&storage, space_id, actor, "Alice launched Project Phoenix.")
                .await;
        let (revision_b, node_b) = create_revision_with_node(
            &storage,
            space_id,
            actor,
            "Alice launched Project Phoenix again.",
        )
        .await;

        let first = elephant_pipeline::extraction::RawAssertion {
            subject_text: "Alice".to_string(),
            predicate_text: "launched".to_string(),
            object_text: Some("Project Phoenix".to_string()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(revision_a.clone()),
                node: Some(Link::new(node_a.clone())),
                start: 0,
                end: 30,
                quote_hash: "event-a".to_string(),
            },
            confidence: 0.95,
            quote: Some("Alice launched Project Phoenix.".to_string()),
            source_event_time: Some(
                chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
                    .expect("parse time")
                    .with_timezone(&Utc),
            ),
            source_event_time_precision: Some(TimePrecision::Exact),
            event_title: Some("Project Phoenix launch".to_string()),
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("project".to_string()),
            significance: Some("high".to_string()),
        };
        let mut second = first.clone();
        second.evidence_span = EvidenceSpan {
            revision: Link::new(revision_b.clone()),
            node: Some(Link::new(node_b.clone())),
            start: 0,
            end: 36,
            quote_hash: "event-b".to_string(),
        };
        second.source_event_time = Some(
            chrono::DateTime::parse_from_rfc3339("2024-03-10T13:00:00Z")
                .expect("parse time")
                .with_timezone(&Utc),
        );

        let data_first = ExtractionArtifactData {
            source_id: Some(source.base.id.to_string()),
            node_id: node_a.to_string(),
            revision_id: revision_a.to_string(),
            assertions: vec![SerializedRawAssertion::from_raw(&first).expect("serialize first")],
            tokens_used: 10,
            model: "test-model".to_string(),
            revision_text_len: 30,
        };
        let data_second = ExtractionArtifactData {
            source_id: Some(source.base.id.to_string()),
            node_id: node_b.to_string(),
            revision_id: revision_b.to_string(),
            assertions: vec![SerializedRawAssertion::from_raw(&second).expect("serialize second")],
            tokens_used: 12,
            model: "test-model".to_string(),
            revision_text_len: 36,
        };

        commit_knowledge_candidates_bundle(
            &storage,
            space_id,
            &RecordId::new_v4(TableName::WorkRun),
            actor,
            &data_first,
        )
        .await
        .expect("commit first bundle");
        commit_knowledge_candidates_bundle(
            &storage,
            space_id,
            &RecordId::new_v4(TableName::WorkRun),
            actor,
            &data_second,
        )
        .await
        .expect("commit second bundle");

        let events = EventRepository::new(storage)
            .list_recent(space_id, 10)
            .await
            .expect("list events");
        assert_eq!(
            events.len(),
            2,
            "distinct exact event times should produce separate events even with the same fallback lineage key"
        );
    }

    #[tokio::test]
    async fn test_validate_artifacts_rejects_resolution_patch_without_target_assertion() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "resolution-validate-space";
        let actor = "tester";
        let resolution_item_id = RecordId::new_v4(TableName::WorkItem);
        let resolution_run_id =
            create_work_run(&storage, space_id, actor, &resolution_item_id).await;
        let workflow = ResolutionPatchWorkflow::new(storage.clone());

        workflow
            .stage_resolution_patch(
                &resolution_run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    assertion_id: RecordId::new_v4(TableName::Assertion),
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
    async fn test_validate_artifacts_rejected_artifacts_do_not_bundle_or_enqueue_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "resolution-reject-space";
        let actor = "tester";
        let resolution_item_id = RecordId::new_v4(TableName::WorkItem);
        let resolution_run_id =
            create_work_run(&storage, space_id, actor, &resolution_item_id).await;
        let workflow = ResolutionPatchWorkflow::new(storage.clone());

        workflow
            .stage_resolution_patch(
                &resolution_run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    assertion_id: RecordId::new_v4(TableName::Assertion),
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

        execute_work_item(
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
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "resolution-commit-space";
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
        let resolved_subject = create_entity(&storage, space_id, actor, "Alice", "person").await;

        let resolution_item_id = RecordId::new_v4(TableName::WorkItem);
        let resolution_run_id =
            create_work_run(&storage, space_id, actor, &resolution_item_id).await;
        let workflow = ResolutionPatchWorkflow::new(storage.clone());
        workflow
            .stage_resolution_patch(
                &resolution_run_id,
                elephant_pipeline::resolution::ResolutionPatch {
                    assertion_id: assertion.base.id.clone(),
                    resolved_subject: Some(resolved_subject.base.id.clone()),
                    resolved_object: None,
                    confidence: 0.91,
                    resolution_method: "graph_context".to_string(),
                    status: ResolutionStatus::Resolved,
                    abstention_reason: None,
                },
                space_id,
            )
            .await
            .expect("stage resolution patch");

        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();
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
            idempotency_key: "validate-resolution-bundle".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let validate_run_id =
            create_work_run(&storage, space_id, actor, &validate_item.base.id).await;
        execute_work_item(
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

        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        let bundled = artifact_repo
            .find_by_work_run(&resolution_run_id)
            .await
            .expect("find bundle artifacts");
        assert!(
            bundled
                .iter()
                .any(|artifact| artifact.artifact_type == COMMIT_BUNDLE_ARTIFACT_TYPE),
            "resolution patch should produce a commit bundle"
        );

        let commit_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                actor.to_string(),
            ),
            kind: COMMIT_ARTIFACTS_KIND.to_string(),
            payload: serde_json::json!({
                "work_run_id": resolution_run_id.to_string(),
                "ingest_mode": "graph"
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: "commit-resolution-bundle".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let commit_run_id = create_work_run(&storage, space_id, actor, &commit_item.base.id).await;
        execute_work_item(
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

        let refreshed = AssertionRepository::new(storage.clone())
            .get(space_id, &assertion.base.id)
            .await
            .expect("load assertion")
            .expect("assertion should exist");
        assert_eq!(refreshed.resolution_status, ResolutionStatus::Resolved);
        assert_eq!(
            refreshed
                .subject_entity
                .as_ref()
                .map(|link| link.id().clone()),
            Some(resolved_subject.base.id.clone())
        );
        assert_eq!(
            refreshed.resolution_method.as_deref(),
            Some("graph_context")
        );
        assert_eq!(refreshed.resolution_confidence, Some(0.91));
    }

    #[tokio::test]
    async fn test_apply_resolution_patch_rejects_second_patch_after_first_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_upgrade_ingest_mode_skips_when_source_already_at_target_mode() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        execute_work_item(
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
    async fn test_enqueue_upgrade_after_metadata_commit_reopens_terminal_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_upgrade_ingest_mode_reopens_terminal_render_doc_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        execute_work_item(
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
    async fn test_mark_source_graph_ready_waits_for_render_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_materialize_doc_from_source_refreshes_doc_acl_and_provenance() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_materialize_doc_from_source_uses_existing_linked_doc_without_manual_template() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_materialize_doc_from_source_restores_soft_deleted_staged_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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
    async fn test_upgrade_ingest_mode_rejects_graph_without_staged_doc() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let err = execute_work_item(
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
    async fn test_resolution_dependency_promotes_after_completion() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let mut running_item = extract_item.clone();
        running_item.status = WorkItemStatus::Running;
        running_item.leased_by = Some("test-worker".to_string());
        running_item.lease_expires_at = Some(chrono::Utc::now() + chrono::Duration::minutes(5));
        item_repo
            .update(space_id, &extract_item.base.id, running_item)
            .await
            .expect("mark running");

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
    async fn test_truth_maintenance_execute_work_item_tm_resolve_slot_enqueues_tm_commit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_space";
        let actor = "tester";

        let subject_id = RecordId::new_v4(TableName::Entity);
        let assertion_id = RecordId::new_v4(TableName::Assertion);
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let attr_id = RecordId::new_v4(TableName::Attribute);

        let base = BaseFields::new(attr_id.clone(), space_id.to_string(), actor.to_string());
        let evidence = EvidenceRef {
            assertion: Link::new(assertion_id),
            span: EvidenceSpan {
                revision: Link::new(revision_id),
                node: None,
                start: 0,
                end: 5,
                quote_hash: "hash".to_string(),
            },
            time_basis: TimeBasis::ObservedAt,
            time_value: chrono::Utc::now(),
        };

        let attribute = Attribute {
            base,
            subject: Link::new(subject_id.clone()),
            attr_type: "title".to_string(),
            value: Literal::String("CEO".to_string()),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![evidence],
            temporal_audit: None,
        };

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

        let _ = execute_work_item(
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
    async fn test_truth_maintenance_execute_work_item_tm_detect_conflicts_enqueues_tm_commit_for_single_claim(
    ) {
        use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
        use elephant_types::records::rel::{ClaimStatus, Rel};
        use elephant_types::records::SystemEvent;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_space";
        let actor = "tester";

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
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

        let _ = execute_work_item(
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

        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo
            .list(space_id, 10, None)
            .await
            .expect("list work items");
        assert!(items.iter().any(|item| item.kind == TM_COMMIT_KIND));
    }

    #[tokio::test]
    async fn test_truth_maintenance_execute_work_item_tm_recompute_runs() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

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

        let _ = execute_work_item(
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

    #[test]
    fn test_tm_context_from_requires_api_key() {
        assert!(tm_context_from(false, None).adjudicator.is_none());
        assert!(tm_context_from(false, Some("claude-custom".to_string()))
            .adjudicator
            .is_none());
    }

    #[test]
    fn test_tm_context_from_key_present_gated_on_feature() {
        for model in [
            None,
            Some("claude-custom".to_string()),
            Some(" ".to_string()),
        ] {
            let ctx = tm_context_from(true, model);
            #[cfg(feature = "meerkat-extraction")]
            assert!(
                ctx.adjudicator.is_some(),
                "key present enables adjudication"
            );
            #[cfg(not(feature = "meerkat-extraction"))]
            assert!(ctx.adjudicator.is_none(), "no adjudicator without meerkat");
        }
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

    fn make_chain_attribute(
        space_id: &str,
        actor: &str,
        subject_id: &RecordId,
        attr_type: &str,
        value: Literal,
    ) -> Attribute {
        let attr_id = RecordId::new_v4(TableName::Attribute);
        let evidence = EvidenceRef {
            assertion: Link::new(RecordId::new_v4(TableName::Assertion)),
            span: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 5,
                quote_hash: "hash".to_string(),
            },
            time_basis: TimeBasis::ObservedAt,
            time_value: chrono::Utc::now(),
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
        let _ = execute_work_item(
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
    async fn test_tm_chain_identical_attrs_corroborate_end_to_end() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_chain_space";
        let actor = "tester";
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_repo = AttributeRepository::new(storage.clone());
        let attr1 = make_chain_attribute(space_id, actor, &subject_id, "age", Literal::Integer(35));
        let attr2 = make_chain_attribute(space_id, actor, &subject_id, "age", Literal::Integer(35));
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
        let commit_item = queued
            .iter()
            .find(|item| item.kind == TM_COMMIT_KIND)
            .expect("corroboration commit enqueued")
            .clone();
        run_tm_item(&storage, &commit_item, &TmContext::none()).await;

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
    async fn test_tm_chain_value_conflict_adjudicated_end_to_end() {
        use elephant_pipeline::truth_maintenance::adjudicator::{
            AdjudicationDecision, AdjudicationVerdict,
        };

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_chain_space";
        let actor = "tester";
        let subject_id = RecordId::new_v4(TableName::Entity);

        let attr_repo = AttributeRepository::new(storage.clone());
        let attr_old = make_chain_attribute(
            space_id,
            actor,
            &subject_id,
            "employer",
            Literal::String("King".to_string()),
        );
        let attr_new = make_chain_attribute(
            space_id,
            actor,
            &subject_id,
            "employer",
            Literal::String("Blizzard".to_string()),
        );
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

    /// Mirrors the private truth-maintenance security hash for slot keys.
    fn security_hash_of(security: &elephant_types::records::base::Security) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(security.policy_id.as_bytes());
        hasher.update(security.level.to_be_bytes());
        let mut labels = security.labels.clone();
        labels.sort();
        for label in labels {
            hasher.update(label.as_bytes());
        }
        let mut handling = security.handling.clone();
        handling.sort();
        for handle in handling {
            hasher.update(handle.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Seeds two divergent `age` attributes plus their truth slot; returns the
    /// slot key.
    async fn seed_divergent_attr_slot(
        storage: &StorageClient,
        space_id: &str,
        actor: &str,
        stale: bool,
    ) -> (String, RecordId, Vec<String>) {
        use elephant_types::records::truth::TruthSlot;

        let subject_id = RecordId::new_v4(TableName::Entity);
        let attr_repo = AttributeRepository::new(storage.clone());
        let mut claim_ids = Vec::new();
        let mut security_hash = String::new();
        for value in [35_i64, 36] {
            let attr =
                make_chain_attribute(space_id, actor, &subject_id, "age", Literal::Integer(value));
            security_hash = security_hash_of(&attr.base.security);
            claim_ids.push(attr.base.id.to_string());
            attr_repo.create(attr).await.expect("create attr");
        }
        claim_ids.sort();
        let slot_key = compute_slot_key(&subject_id.to_string(), None, "age", None, &security_hash);
        let slot = TruthSlot {
            base: BaseFields::new(
                RecordId::new_v4(TableName::TruthSlot),
                space_id.to_string(),
                actor.to_string(),
            ),
            slot_key: slot_key.clone(),
            slot_type: "attribute".to_string(),
            subject_entity: subject_id.to_string(),
            predicate_id: "age".to_string(),
            current_values: vec![],
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 1,
            alias_version: 1,
            stale,
            stale_reason: stale.then(|| "test".to_string()),
            computed_at: chrono::Utc::now() - chrono::Duration::days(10),
            security_hash,
        };
        let slot_repo = elephant_storage::TruthSlotRepository::new(storage.clone());
        slot_repo.create(slot).await.expect("create slot");

        (slot_key, subject_id, claim_ids)
    }

    /// A memoized replay must not enqueue a `tm_commit` (it would clobber the
    /// previously committed verdict with Disputed statuses).
    #[tokio::test]
    async fn test_tm_resolve_slot_arm_skips_commit_for_memoized_replay() {
        use elephant_pipeline::truth_maintenance::compute_claim_set_hash;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _subject_id, claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, false).await;

        // Unresolved group already adjudicated for this exact claim set.
        let mut conflict_group =
            create_conflict_group(&slot_key, "attribute", claim_ids.clone(), space_id);
        conflict_group.adjudicated_claim_set_hash = Some(compute_claim_set_hash(&claim_ids));
        conflict_group.adjudicated_at = Some(chrono::Utc::now());
        let conflict_group_id = conflict_group.base.id.clone();
        let conflict_repo = ConflictGroupRepository::new(storage.clone());
        conflict_repo
            .create(conflict_group)
            .await
            .expect("create conflict group");

        let work_item = make_tm_work_item(
            space_id,
            actor,
            TM_RESOLVE_SLOT_KIND,
            serde_json::json!({ "conflict_group_id": conflict_group_id.to_string() }),
            "tm-resolve-memoized",
        );
        run_tm_item(&storage, &work_item, &TmContext::none()).await;

        let item_repo = WorkItemRepository::new(storage);
        let items = item_repo.list(space_id, 10, None).await.expect("list");
        assert!(
            !items.iter().any(|item| item.kind == TM_COMMIT_KIND),
            "memoized replay must not enqueue tm_commit"
        );
    }

    /// The recompute arm enqueues one `tm_resolve_slot` per pending group.
    #[tokio::test]
    async fn test_tm_recompute_arm_enqueues_pending_resolves() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _subject_id, _claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, true).await;

        let work_item = make_tm_work_item(
            space_id,
            actor,
            TM_RECOMPUTE_KIND,
            serde_json::json!({ "max_slots": 5 }),
            "tm-recompute-pending",
        );
        run_tm_item(&storage, &work_item, &TmContext::none()).await;

        let item_repo = WorkItemRepository::new(storage.clone());
        let items = item_repo.list(space_id, 10, None).await.expect("list");
        let resolve_item = items
            .iter()
            .find(|item| item.kind == TM_RESOLVE_SLOT_KIND)
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
    async fn test_reprocess_arm_enqueues_pending_conflict_resolves() {
        use elephant_types::records::reprocess::{
            default_reprocess_target_stages, SelectionCriteria,
        };

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let space_id = "tm_space";
        let actor = "tester";
        let (slot_key, _subject_id, _claim_ids) =
            seed_divergent_attr_slot(&storage, space_id, actor, false).await;

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

        let work_item = make_tm_work_item(
            space_id,
            actor,
            WORK_KIND_EXECUTE_REPROCESS_JOB,
            serde_json::json!({ "job_id": creation.job.base.id.to_string() }),
            "reprocess-pending",
        );
        // Reprocess loads its work run for provenance; seed a real one.
        let work_run_id = create_work_run(&storage, space_id, actor, &work_item.base.id).await;
        let config = WorkQueueWorkerConfig::default();
        let space_config_repo = SpaceConfigRepository::new(storage.clone());
        let provider_cache = EmbeddingProviderCache::new();
        let embed_config = EmbedWorkerConfig::default();
        let _ = execute_work_item(
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
        .expect("execute reprocess job");

        let item_repo = WorkItemRepository::new(storage.clone());
        let items = item_repo.list(space_id, 10, None).await.expect("list");
        let resolve_item = items
            .iter()
            .find(|item| item.kind == TM_RESOLVE_SLOT_KIND)
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

async fn resolve_embedding_policy(
    repo: &SpaceConfigRepository,
    space_id: &str,
    fallback: &EmbeddingPolicy,
) -> anyhow::Result<EmbeddingPolicy> {
    let policy = match repo.get_by_space_id(space_id).await {
        Ok(Some(config)) => EmbeddingPolicy::from_space_config(&config),
        Ok(None) => fallback.clone(),
        Err(err) => {
            warn!("Failed to load space_config, falling back to config: {err}");
            fallback.clone()
        }
    };

    if policy.dims == 0 {
        return Err(anyhow::anyhow!("Embedding policy has invalid dimensions"));
    }

    Ok(policy)
}

/// Builds the truth-maintenance adjudication context.
///
/// LLM adjudication activates only when the binary is built with
/// `meerkat-extraction` AND `ANTHROPIC_API_KEY` is present; the model comes
/// from `PIPELINE_TM_MODEL`. This is the single place TM reads env vars.
fn build_tm_context() -> TmContext {
    let key_present = std::env::var("ANTHROPIC_API_KEY")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let model = std::env::var("PIPELINE_TM_MODEL").ok();
    tm_context_from(key_present, model)
}

/// Pure decision half of [`build_tm_context`], unit-testable without env mutation.
#[cfg(feature = "meerkat-extraction")]
fn tm_context_from(key_present: bool, model: Option<String>) -> TmContext {
    use elephant_pipeline::truth_maintenance::adjudicator::{
        AdjudicationConfig, AgenticSlotAdjudicator,
    };

    if !key_present {
        return TmContext::none();
    }
    let mut config = AdjudicationConfig::default();
    if let Some(model) = model {
        let trimmed = model.trim();
        if !trimmed.is_empty() {
            config.model = trimmed.to_string();
        }
    }
    TmContext {
        adjudicator: Some(Arc::new(AgenticSlotAdjudicator::new(config))),
    }
}

/// Without the meerkat feature there is no adjudicator implementation.
#[cfg(not(feature = "meerkat-extraction"))]
fn tm_context_from(_key_present: bool, _model: Option<String>) -> TmContext {
    TmContext::none()
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

fn build_embedding_provider(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    elephant_embeddings::provider_for_policy(policy)
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
    work_item_repo.create(upgrade_item).await?;
    Ok(())
}

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
    work_item_repo.create(render_item).await?;
    Ok(())
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
    let promotion_options = elephant_pipeline::scheduler::PromotionOptions {
        disambiguator: elephant_pipeline::scheduler::promotion_disambiguator_from(
            std::env::var("ANTHROPIC_API_KEY").is_ok_and(|k| !k.trim().is_empty()),
            std::env::var("PIPELINE_PROMOTION_DISAMBIGUATION_MODEL").ok(),
        ),
    };
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
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let thread_name = thread_name.to_string();
    std::thread::Builder::new()
        .name(thread_name)
        .stack_size(stack_bytes)
        .spawn(move || {
            let output = runtime.block_on(future);
            let _ = sender.send(output);
        })
        .map_err(|error| anyhow::anyhow!("Could not start dedicated worker thread: {error}"))?;
    receiver
        .await
        .map_err(|error| anyhow::anyhow!("Dedicated worker thread exited early: {error}"))
}

#[cfg(feature = "meerkat-extraction")]
async fn execute_managed_wiki_work_item(
    item: &WorkItem,
    storage: StorageClient,
    payload: ManagedWikiPayload,
) -> anyhow::Result<WorkRunMetrics> {
    const MANAGED_WIKI_STACK_BYTES: usize = 32 * 1024 * 1024;
    let reconciliation_storage = storage.clone();
    let reconciliation_space = item.base.space_id.clone();
    let reconciliation_payload = payload.clone();
    let result = run_on_dedicated_stack(
        "elephant-living-wiki",
        MANAGED_WIKI_STACK_BYTES,
        async move {
            execute_managed_wiki_reconciliation(
                reconciliation_storage,
                &reconciliation_space,
                &reconciliation_payload,
            )
            .await
        },
    )
    .await?
    .map_err(|error| anyhow::anyhow!("Managed wiki reconciliation failed: {error}"))?;
    let mut next_payload = payload;
    next_payload.last_graph_fingerprint =
        (result.failed_articles == 0).then(|| result.graph_fingerprint.clone());
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
    Ok(metrics)
}

// This is the central typed work-kind dispatcher. Its broad dependency set and
// match-state size are bounded by the worker semaphore; the graph-scale Living
// Wiki arm is additionally moved onto `run_on_dedicated_stack` above.
#[allow(clippy::too_many_arguments, clippy::large_stack_frames)]
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
    Box::pin(execute_work_item_with_extractor(
        item,
        work_run_id,
        storage,
        &WorkQueueWorkerConfig::default(),
        &TmContext::none(),
        Some(extractor),
    ))
    .await
}

#[allow(clippy::too_many_lines, clippy::large_stack_frames)]
async fn execute_work_item_with_extractor(
    item: &WorkItem,
    work_run_id: &RecordId,
    storage: StorageClient,
    config: &WorkQueueWorkerConfig,
    tm_ctx: &TmContext,
    extractor_override: Option<&dyn Extractor>,
) -> anyhow::Result<WorkRunMetrics> {
    let space_id = &item.base.space_id;
    let mut metrics = WorkRunMetrics::default();
    match item.kind.as_str() {
        REFRESH_SOURCE_WATCH_KIND => {
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
            let run = Box::pin(
                SourceWatchService::new(storage.clone()).check(watch, &item.base.created_by),
            )
            .await
            .map_err(|e| anyhow::anyhow!("source-watch refresh failed: {e}"))?;
            info!(
                watch_id = %run.watch.base.id,
                outcome = %run.check.outcome,
                triage_work_item = ?run.triage_work_item,
                "Source-watch refresh completed"
            );
        }
        INGEST_SOURCE_STUB_KIND => {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid ingest_source_stub payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = load_source_record(&storage, space_id, &source_id).await?;
            if source.lifecycle_status != SourceLifecycleStatus::Discovered {
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
                .map_err(|e| anyhow::anyhow!("ingest_source_stub failed: {e}"))?;
        }
        TRIAGE_SOURCE_KIND => {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid triage_source payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = load_source_record(&storage, space_id, &source_id).await?;
            if source.lifecycle_status != SourceLifecycleStatus::Discovered {
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
            let triage_result = execute_triage_source(
                &source,
                &triage_plugin,
                &budget_enforcer,
                &decision_repo,
                space_id,
                policy_version,
                &item.base.created_by,
            )
            .await
            .map_err(|e| anyhow::anyhow!("triage_source failed: {e}"))?;

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

            if triage_result.decided_mode != IngestMode::Skip {
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
                WorkItemRepository::new(storage.clone())
                    .create(next_item)
                    .await?;
            }
        }
        INGEST_METADATA_KIND => {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid ingest_metadata payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = load_source_record(&storage, space_id, &source_id).await?;
            if source.lifecycle_status != SourceLifecycleStatus::Triaged {
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
                .map_err(|e| anyhow::anyhow!("ingest_metadata failed: {e}"))?;
            let artifact_repo = WorkArtifactRepository::new(storage.clone());
            let metadata_artifact = elephant_types::records::work::WorkArtifact {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkArtifact),
                    space_id.clone(),
                    item.base.created_by.clone(),
                ),
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
            artifact_repo.create(metadata_artifact).await?;

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
                work_item_repo.create(validate_item).await?;
            }
        }
        UPGRADE_INGEST_MODE_KIND => {
            let payload: SourceLifecyclePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid upgrade_ingest_mode payload: {e}"))?;
            let source_id = RecordId::from_str(&payload.source_id).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid source id {payload_source_id}: {e}",
                    payload_source_id = payload.source_id
                )
            })?;
            let source = load_source_record(&storage, space_id, &source_id).await?;
            let target_mode =
                parse_optional_ingest_mode(payload.target_mode.as_deref(), IngestMode::Retrieval);
            if !matches!(
                source.lifecycle_status,
                SourceLifecycleStatus::MetadataReady | SourceLifecycleStatus::RetrievalReady
            ) || source_has_reached_ingest_mode(&source, target_mode)
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
            let upgrade = execute_upgrade_ingest_mode(
                &source,
                current_mode,
                target_mode,
                &budget_enforcer,
                0.0,
                payload.reason.as_deref().unwrap_or("source_upgrade"),
            )
            .await
            .map_err(|e| anyhow::anyhow!("upgrade_ingest_mode failed: {e}"))?;

            if !upgrade.allowed {
                return Ok(metrics);
            }

            let doc = materialize_doc_from_source(&storage, &source, &item.base.created_by).await?;
            enqueue_render_doc_work_item(&storage, item, &source_id, &doc, upgrade.new_mode)
                .await?;
        }
        RENDER_DOC_KIND => {
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

            let render_result =
                execute_render_doc(&storage, &renderer_registry, &doc, Some(source_type))
                    .await
                    .map_err(|e| anyhow::anyhow!("render_doc failed: {e}"))?;

            let mut tree_config = DocTreeConfig::default();
            if let Ok(model) = std::env::var("PIPELINE_SUMMARIZER_MODEL") {
                if !model.trim().is_empty() {
                    tree_config.summarizer_model = model;
                }
            }
            execute_build_doc_tree(&storage, &render_result.revision_id, &tree_config)
                .await
                .map_err(|e| anyhow::anyhow!("build_doc_tree failed: {e}"))?;

            let ingest_mode =
                parse_optional_ingest_mode(payload.ingest_mode.as_deref(), IngestMode::Graph);
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

            if ingest_mode == IngestMode::Retrieval {
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

            work_item_repo
                .create(plan_item)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to enqueue plan_extraction: {e}"))?;
        }
        PLAN_EXTRACTION_KIND => {
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
            let config = ExtractionPlanConfig::default();
            let result = match execute_plan_extraction(
                &storage,
                &revision_id,
                policy_version,
                &config,
                work_run_id,
                space_id,
                &item.base.created_by,
            )
            .await
            {
                Ok(result) => result,
                Err(err) if is_obsolete_extraction_reference_error(&err) => {
                    warn!(
                        work_item = %item.base.id,
                        revision_id = %revision_id,
                        error = %err,
                        "Completing obsolete plan_extraction work item for missing extraction reference"
                    );
                    return Ok(metrics);
                }
                Err(err) => return Err(anyhow::anyhow!("plan_extraction failed: {err}")),
            };

            metrics.tokens_used = result.estimated_tokens;
            metrics.estimated_cost_euros = result.plan.estimated_cost_euros;

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

            let work_item_repo = WorkItemRepository::new(storage.clone());
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

                work_item_repo
                    .create(work_item)
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
        }
        EXTRACT_ASSERTIONS_KIND => {
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

            let result = match execute_extract_assertions(
                &storage,
                extractor,
                &node_id,
                &revision_id,
                work_run_id,
                space_id,
                &item.base.created_by,
                span_override,
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
                        "Completing obsolete extract_assertions work item for missing extraction reference"
                    );
                    return Ok(metrics);
                }
                Err(err) => return Err(anyhow::anyhow!("extract_assertions failed: {err}")),
            };

            metrics.tokens_used = result.tokens_used;
            metrics.estimated_cost_euros = estimate_cost_from_tokens(result.tokens_used);
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
                work_item_repo.create(validate_item).await?;
            }
        }
        VALIDATE_ARTIFACTS_KIND => {
            let payload: ArtifactWorkflowPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid validate_artifacts payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id {}: {e}", payload.work_run_id))?;
            let staged_commit = StagedCommit::new(storage.clone());
            let artifact_repo = WorkArtifactRepository::new(storage.clone());
            let validated =
                elephant_pipeline::scheduler::staged_commit::validate_artifacts::execute(
                    &staged_commit,
                    space_id,
                    &run_id,
                )
                .await
                .map_err(|e| anyhow::anyhow!("validate_artifacts failed: {e}"))?;
            let existing_artifacts: Vec<_> = artifact_repo.find_by_work_run(&run_id).await?;
            let any_validated = validated
                .iter()
                .any(|artifact| artifact.status == ArtifactStatus::Validated);
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
                if existing_commit_bundle.is_some() {
                    continue;
                }

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

                let bundle = elephant_types::records::work::WorkArtifact {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkArtifact),
                        space_id.clone(),
                        item.base.created_by.clone(),
                    ),
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
                artifact_repo.create(bundle).await?;
            }
            if any_validated {
                let work_item_repo = WorkItemRepository::new(storage.clone());
                let commit_idempotency_key = format!("{COMMIT_ARTIFACTS_KIND}:{run_id}");
                if work_item_repo
                    .find_by_idempotency_key(space_id, &commit_idempotency_key)
                    .await?
                    .is_none()
                {
                    let commit_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            item.base.created_by.clone(),
                        ),
                        kind: COMMIT_ARTIFACTS_KIND.to_string(),
                        payload: serde_json::to_value(&payload).map_err(|e| {
                            anyhow::anyhow!("Failed to serialize commit_artifacts payload: {e}")
                        })?,
                        status: WorkItemStatus::Runnable,
                        priority: successor_priority(item.priority),
                        idempotency_key: commit_idempotency_key,
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };
                    work_item_repo.create(commit_item).await?;
                }
            }
        }
        COMMIT_ARTIFACTS_KIND => {
            let payload: ArtifactWorkflowPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid commit_artifacts payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id {}: {e}", payload.work_run_id))?;
            let ingest_mode =
                parse_optional_ingest_mode(payload.ingest_mode.as_deref(), IngestMode::Graph);
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
            let mut committed_graph_artifacts = false;

            for artifact in artifacts {
                if artifact.status != ArtifactStatus::Validated
                    || artifact.artifact_type != COMMIT_BUNDLE_ARTIFACT_TYPE
                {
                    continue;
                }

                let bundle_preview = commit_bundle_payload(&artifact)
                    .map_err(|e| anyhow::anyhow!("commit_artifacts failed: {e}"))?;
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
                                    commit_knowledge_candidates_bundle(
                                        &storage, space_id, &run_id, &actor, &data,
                                    )
                                    .await
                                }
                                CommitBundlePayload::ResolutionPatchSet(patch) => {
                                    apply_resolution_patch(
                                        &storage,
                                        space_id,
                                        &patch,
                                        &BackoffConfig::default(),
                                        None,
                                    )
                                    .await
                                }
                            }
                        }
                    },
                ))
                .await
                .map_err(|e| anyhow::anyhow!("commit_artifacts failed: {e}"))?;
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
        }
        validate_resolution_patches::KIND => {
            let payload: WorkRunPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid validate payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id: {e}"))?;
            let workflow = ResolutionPatchWorkflow::new(storage.clone());
            validate_resolution_patches::execute(&workflow, space_id, &run_id).await?;
        }
        commit_resolution_patches::KIND => {
            let payload: WorkRunPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid commit payload: {e}"))?;
            let run_id = RecordId::from_str(&payload.work_run_id)
                .map_err(|e| anyhow::anyhow!("Invalid work_run_id: {e}"))?;
            let workflow = ResolutionPatchWorkflow::new(storage.clone());
            commit_resolution_patches::execute(
                &workflow,
                space_id,
                &run_id,
                &BackoffConfig::default(),
                None,
            )
            .await?;
        }
        WORK_KIND_EXECUTE_REPROCESS_JOB => {
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
            let reprocess_result = Box::pin(execute_reprocess_job(
                &ctx,
                &job_id,
                work_run_id,
                space_id,
                &reprocess_config,
            ))
            .await?;

            // The reconvergence recompute defers multi-value-group slots;
            // enqueue one tm_resolve_slot per pending conflict group.
            if !reprocess_result.pending_conflict_groups.is_empty() {
                let work_item_repo = WorkItemRepository::new(storage.clone());
                for group_id in reprocess_result.pending_conflict_groups {
                    let work_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            item.base.created_by.clone(),
                        ),
                        kind: TM_RESOLVE_SLOT_KIND.to_string(),
                        payload: serde_json::json!({
                            "conflict_group_id": group_id.to_string()
                        }),
                        status: WorkItemStatus::Runnable,
                        priority: successor_priority(item.priority),
                        idempotency_key: format!("tm_resolve_slot:{group_id}:{work_run_id}"),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };

                    work_item_repo
                        .create(work_item)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_resolve_slot: {e}"))?;
                }
            }
        }
        TM_DETECT_CONFLICTS_KIND => {
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
                if let Some(event) = event_repo.get_by_id(&record_id).await? {
                    events.push(event);
                }
            }

            if events.is_empty() {
                return Ok(metrics);
            }

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
            let result =
                execute_tm_detect_conflicts_from_outbox_events(&events, &tm_storage).await?;
            let DetectConflictsResult {
                conflict_group_ids,
                direct_commits,
                ..
            } = result;

            if !conflict_group_ids.is_empty() || !direct_commits.is_empty() {
                let work_item_repo = WorkItemRepository::new(storage.clone());

                for group_id in conflict_group_ids {
                    let work_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            item.base.created_by.clone(),
                        ),
                        kind: TM_RESOLVE_SLOT_KIND.to_string(),
                        payload: serde_json::json!({
                            "conflict_group_id": group_id.to_string()
                        }),
                        status: WorkItemStatus::Runnable,
                        priority: successor_priority(item.priority),
                        idempotency_key: format!("tm_resolve_slot:{group_id}:{work_run_id}"),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };

                    work_item_repo
                        .create(work_item)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_resolve_slot: {e}"))?;
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

                    work_item_repo
                        .create(work_item)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_commit: {e}"))?;
                }
            }
        }
        TM_COMMIT_KIND => {
            let payload: CommitTruthPayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_commit payload: {e}"))?;

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
            if let Err(err) =
                execute_tm_commit(&payload.resolution, &payload.input, space_id, &tm_storage).await
            {
                if is_obsolete_tm_commit_error(&err) {
                    warn!(
                        "Skipping obsolete tm_commit work item {} after cleanup: {}",
                        item.base.id, err
                    );
                } else {
                    return Err(err.into());
                }
            }
        }
        TM_RESOLVE_SLOT_KIND => {
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

            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
            let (resolution, input) = match resolve_conflict_group_with_storage(
                tm_ctx,
                &conflict_group,
                space_id,
                &tm_storage,
            )
            .await
            {
                Ok(result) => result,
                Err(err) if is_obsolete_tm_resolve_error(&err) => {
                    warn!(
                        "Resolving obsolete tm_resolve_slot work item {} after cleanup: {}",
                        item.base.id, err
                    );
                    let mut resolved_group = conflict_group.clone();
                    resolved_group.resolved = true;
                    resolved_group.resolved_at = Some(Utc::now());
                    resolved_group.resolution_rationale =
                        Some("No active claims remain after cleanup".to_string());
                    conflict_repo
                        .update(space_id, &group_id, resolved_group)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to mark obsolete conflict_group resolved: {e}")
                        })?;
                    return Ok(metrics);
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

            let work_item_repo = WorkItemRepository::new(storage.clone());
            work_item_repo
                .create(commit_item)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_commit: {e}"))?;
        }
        TM_RECOMPUTE_KIND => {
            let payload: RecomputePayload = serde_json::from_value(item.payload.clone())
                .map_err(|e| anyhow::anyhow!("Invalid tm_recompute payload: {e}"))?;
            let priority = parse_recompute_priority(payload.priority.as_deref());
            let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
            let result =
                execute_tm_recompute(tm_ctx, space_id, priority, payload.max_slots, &tm_storage)
                    .await?;

            // Multi-value-group slots defer adjudication out of the recompute
            // lease: enqueue one tm_resolve_slot item per pending group.
            if !result.pending_conflict_groups.is_empty() {
                let work_item_repo = WorkItemRepository::new(storage.clone());
                for group_id in result.pending_conflict_groups {
                    let work_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.clone(),
                            item.base.created_by.clone(),
                        ),
                        kind: TM_RESOLVE_SLOT_KIND.to_string(),
                        payload: serde_json::json!({
                            "conflict_group_id": group_id.to_string()
                        }),
                        status: WorkItemStatus::Runnable,
                        priority: successor_priority(item.priority),
                        idempotency_key: format!("tm_resolve_slot:{group_id}:{work_run_id}"),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };

                    work_item_repo
                        .create(work_item)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to enqueue tm_resolve_slot: {e}"))?;
                }
            }
        }
        WORK_KIND_FRESHNESS_SWEEP => {
            let freshness_repo = FreshnessRepository::new(storage.clone());
            let result = execute_freshness_sweep(freshness_repo, space_id, None).await?;
            metrics.custom.insert(
                "items_processed".to_string(),
                serde_json::json!(result.processed),
            );
        }
        RECONCILE_LIVING_WIKI_KIND => {
            let payload: ManagedWikiPayload = serde_json::from_value(item.payload.clone())
                .map_err(|error| anyhow::anyhow!("Invalid managed wiki payload: {error}"))?;
            #[cfg(feature = "meerkat-extraction")]
            {
                metrics = Box::pin(execute_managed_wiki_work_item(item, storage, payload)).await?;
            }

            #[cfg(not(feature = "meerkat-extraction"))]
            return Err(anyhow::anyhow!(
                "reconcile_living_wiki requires the meerkat-extraction feature"
            ));
        }
        other => {
            return Err(anyhow::anyhow!("Unsupported work kind: {other}"));
        }
    }

    Ok(metrics)
}

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
    let tm_ctx = Arc::new(build_tm_context());

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
    let extract_semaphore = Arc::new(Semaphore::new(config.max_extract_concurrent));
    let mut tasks = JoinSet::new();

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
            () = tokio::time::sleep(config.poll_interval()) => {
                // Reap completed tasks
                while let Some(result) = tasks.try_join_next() {
                    if let Err(e) = result {
                        warn!("Task panicked: {e}");
                    }
                }

                loop {
                    // Drain as much runnable work as we have capacity for on each
                    // poll tick. One batched claim fills every free permit from a
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
                                            &queue_config.worker_id,
                                            60,
                                            Some("living_wiki_serialization".to_string()),
                                        )
                                        .await;
                                    drop(permit);
                                    continue;
                                }
                            }
                            // Check daily cap
                        match daily_cap.try_reserve(&item.base.space_id).await {
                            Ok(false) => {
                                // Cap reached — put item back as runnable
                                let work_item_repo = WorkItemRepository::new((*storage).clone());
                                let delay = i64::try_from(config.poll_interval_secs).unwrap_or(i64::MAX);
                                let _ = work_item_repo
                                    .requeue_with_delay(
                                        &item.base.space_id,
                                        &item.base.id,
                                        &queue_config.worker_id,
                                        delay.max(1),
                                        Some("daily_cap_reached".to_string()),
                                    )
                                    .await;
                                drop(permit);
                                continue;
                            }
                            Err(err) => {
                                warn!("Daily cap check failed for {}: {}", item.base.id, err);
                                // On error, allow work to proceed rather than blocking
                            }
                            Ok(true) => {} // Under cap, proceed
                        }

                        let work_item_timeout_secs = if item.kind == RECONCILE_LIVING_WIKI_KIND {
                            config.work_item_timeout_secs.max(MANAGED_WIKI_TIMEOUT_SECS)
                        } else {
                            config.work_item_timeout_secs
                        };
                        if item.kind == RECONCILE_LIVING_WIKI_KIND {
                            let lease_duration = work_item_timeout_secs
                                .saturating_add(MANAGED_WIKI_LEASE_GRACE_SECS);
                            if let Err(err) = work_queue
                                .renew_lease(&item.base.space_id, &item.base.id, lease_duration)
                                .await
                            {
                                warn!(
                                    work_item = %item.base.id,
                                    error = %err,
                                    "Could not extend managed wiki lease; refusing duplicate-prone execution"
                                );
                                drop(permit);
                                continue;
                            }
                        }

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
                            let _permit = permit; // Hold permit until task completes
                            let _extract_permit = extract_permit;
                            let timeout = std::time::Duration::from_secs(work_item_timeout_secs);
                            let timed_out_item_id = item.base.id.clone();
                            let timed_out_space_id = item.base.space_id.clone();
                            let timed_out_kind = item.kind.clone();
                            let timed_out_attempt = item.attempts;

                            let task_result = tokio::time::timeout(timeout, async {
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
                                        let _ = work_queue
                                            .fail(
                                                &item.base.space_id,
                                                &item.base.id,
                                                format!("cost reserve failed: {err}"),
                                            )
                                            .await;
                                        return;
                                    }
                                };

                                let run_id = RecordId::new_v4(TableName::WorkRun);
                                let base = BaseFields::new(
                                    run_id.clone(),
                                    item.base.space_id.clone(),
                                    queue_config.worker_id.clone(),
                                );
                                let started_at = chrono::Utc::now();
                                let initial_metrics = WorkRunMetrics {
                                    estimated_cost_euros: estimates.estimated_cost_euros,
                                    tokens_used: estimates.estimated_tokens,
                                    ..WorkRunMetrics::default()
                                };

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

                                let result = Box::pin(execute_work_item(
                                    &item,
                                    &created_run.base.id,
                                    (*storage).clone(),
                                    &config,
                                    &space_config_repo,
                                    &provider_cache,
                                    &embed_config,
                                    &tm_ctx,
                                ))
                                .await;

                                let completed_at = chrono::Utc::now();
                                let wall_time_ms_i64 = completed_at
                                    .signed_duration_since(started_at)
                                    .num_milliseconds()
                                    .max(0);
                                let wall_time_ms = u64::try_from(wall_time_ms_i64).unwrap_or(0);

                                let mut updated_run = match work_run_repo
                                    .get(&item.base.space_id, &created_run.base.id)
                                    .await
                                {
                                    Ok(Some(run)) => run,
                                    Ok(None) => created_run.clone(),
                                    Err(err) => {
                                        warn!(
                                            "Failed to refresh work_run {}: {}",
                                            created_run.base.id, err
                                        );
                                        created_run.clone()
                                    }
                                };
                                updated_run.completed_at = Some(completed_at);

                                match result {
                                    Ok(mut run_metrics) => {
                                        run_metrics.wall_time_ms = wall_time_ms;
                                        if run_metrics.tokens_used == 0 {
                                            run_metrics.tokens_used = estimates.estimated_tokens;
                                        }
                                        if run_metrics.estimated_cost_euros == 0.0 {
                                            run_metrics.estimated_cost_euros = estimates.estimated_cost_euros;
                                        }
                                        updated_run.metrics = run_metrics.clone();
                                        updated_run.success = true;

                                        let actual_cost = if run_metrics.estimated_cost_euros > 0.0 {
                                            run_metrics.estimated_cost_euros
                                        } else {
                                            estimate_cost_from_tokens(run_metrics.tokens_used)
                                        };
                                        if let Err(err) = budget_enforcer
                                            .reconcile_cost(
                                                &ledger_id,
                                                actual_cost,
                                                u64::from(run_metrics.tokens_used),
                                                true,
                                            )
                                            .await
                                        {
                                            warn!(
                                                "Failed to reconcile cost for work item {}: {}",
                                                item.base.id, err
                                            );
                                        }

                                        if let Err(err) = work_run_repo
                                            .update(&item.base.space_id, &created_run.base.id, updated_run)
                                            .await
                                        {
                                            warn!("Failed to update work_run {}: {}", created_run.base.id, err);
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
                                        let actual_cost =
                                            estimate_cost_from_tokens(estimates.estimated_tokens);
                                        let _ = budget_enforcer
                                            .reconcile_cost(
                                                &ledger_id,
                                                actual_cost,
                                                u64::from(estimates.estimated_tokens),
                                                false,
                                            )
                                            .await;

                                        updated_run.success = false;
                                        updated_run.error = Some(err.to_string());
                                        updated_run.metrics.wall_time_ms = wall_time_ms;
                                        updated_run.metrics.tokens_used = estimates.estimated_tokens;
                                        if let Err(err) = work_run_repo.update(&item.base.space_id, &created_run.base.id, updated_run).await {
                                            warn!("Failed to update failed work_run {}: {}", created_run.base.id, err);
                                        }

                                        let should_retry = item.attempts < config.transient_retry_max_attempts
                                            && is_transient_work_error(&err);

                                        if should_retry {
                                            let delay_secs = compute_transient_retry_delay_secs(
                                                item.attempts,
                                                config.transient_retry_base_delay_secs,
                                                config.transient_retry_max_delay_secs,
                                            );
                                            let work_item_repo = WorkItemRepository::new((*storage).clone());
                                            match work_item_repo
                                                .requeue_with_delay(
                                                    &item.base.space_id,
                                                    &item.base.id,
                                                    &queue_config.worker_id,
                                                    delay_secs,
                                                    Some(format!(
                                                        "transient_retry attempt {}: {err}",
                                                        item.attempts
                                                    )),
                                                )
                                                .await
                                            {
                                                Ok(Some(_)) => {
                                                    warn!(
                                                        "Requeued transiently failing work item {} (attempt {} / {}, delay {}s): {}",
                                                        item.base.id,
                                                        item.attempts,
                                                        config.transient_retry_max_attempts,
                                                        delay_secs,
                                                        err
                                                    );
                                                    return;
                                                }
                                                Ok(None) => {
                                                    warn!(
                                                        "Transient retry skipped for work item {} due to lease/state conflict; marking failed",
                                                        item.base.id
                                                    );
                                                }
                                                Err(requeue_err) => {
                                                    warn!(
                                                        "Failed to requeue transiently failing work item {}: {}",
                                                        item.base.id,
                                                        requeue_err
                                                    );
                                                }
                                            }
                                        }

                                        if let Err(err) = work_queue.fail(&item.base.space_id, &item.base.id, err.to_string()).await {
                                            error!("Failed to mark work item {} failed: {}", item.base.id, err);
                                        } else {
                                            warn!(
                                                work_item = %item.base.id,
                                                kind = %item.kind,
                                                error = %err,
                                                "Failed work item"
                                            );
                                        }
                                    }
                                }
                            }).await;

                            if task_result.is_err() {
                                let timeout_message =
                                    format!("work item timed out after {work_item_timeout_secs}s");
                                error!(
                                    work_item = %timed_out_item_id,
                                    kind = %timed_out_kind,
                                    attempt = timed_out_attempt,
                                    timeout_secs = work_item_timeout_secs,
                                    "Work item timed out"
                                );
                                if let Err(err) = work_queue
                                    .fail(&timed_out_space_id, &timed_out_item_id, timeout_message)
                                    .await
                                {
                                    warn!(
                                        work_item = %timed_out_item_id,
                                        kind = %timed_out_kind,
                                        error = %err,
                                        "Failed to mark timed out work item as failed"
                                    );
                                }
                            }
                            });
                        }
                    }
                    if batch_len < free_permits {
                        // The queue had less claimable work than capacity —
                        // nothing more to drain this tick.
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn knowledge_view_event_records(events: &[SystemEvent]) -> (bool, Vec<String>) {
    let records = events
        .iter()
        .filter_map(|event| event.payload.get("records"))
        .filter_map(|records| {
            serde_json::from_value::<Vec<elephant_outbox::RecordRef>>(records.clone()).ok()
        })
        .flatten()
        .collect::<Vec<_>>();
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
    (living_wiki_graph_changed, affected_projection_records)
}

async fn process_knowledge_view_events(
    storage: &StorageClient,
    space_id: &str,
    after_seq: i64,
    events: &[SystemEvent],
) {
    let (living_wiki_graph_changed, affected_projection_records) =
        knowledge_view_event_records(events);
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
                warn!(
                    "Knowledge-view invalidation failed for {}: {}",
                    space_id, err
                );
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
            Err(err) => warn!(
                "Living Wiki graph-change scheduling failed for {}: {}",
                space_id, err
            ),
        }
    }
}

async fn run_knowledge_view_invalidation_loop(
    storage: StorageClient,
    poll_interval: std::time::Duration,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    const BATCH_SIZE: usize = 500;
    let event_repo = SystemEventRepository::new(storage.clone());
    let mut last_seq_by_space: HashMap<String, i64> = HashMap::new();
    for space_id in event_repo.get_all_spaces().await? {
        let latest = event_repo.get_latest_seq(&space_id).await?;
        last_seq_by_space.insert(space_id, latest);
    }
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
                    let after_seq = last_seq_by_space.get(&space_id).copied().unwrap_or(0);
                    let events = match event_repo
                        .get_after_seq(&space_id, after_seq, BATCH_SIZE)
                        .await
                    {
                        Ok(events) => events,
                        Err(err) => {
                            warn!(
                                "Knowledge-view invalidation worker failed to fetch events for {}: {}",
                                space_id, err
                            );
                            continue;
                        }
                    };
                    if events.is_empty() {
                        continue;
                    }
                    process_knowledge_view_events(&storage, &space_id, after_seq, &events).await;
                    if let Some(last) = events.last() {
                        last_seq_by_space.insert(space_id, last.seq);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_truth_maintenance_loop(
    storage: StorageClient,
    config: TruthMaintenanceWorkerConfig,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let event_repo = SystemEventRepository::new(storage.clone());
    let work_item_repo = WorkItemRepository::new(storage.clone());
    let mut last_seq_by_space: HashMap<String, i64> = HashMap::new();
    let mut last_event_id_by_space: HashMap<String, RecordId> = HashMap::new();

    loop {
        tokio::select! {
            _ = rx.changed() => {
                if *rx.borrow() {
                    info!("Truth maintenance worker received shutdown signal");
                    break;
                }
            }
            () = tokio::time::sleep(config.poll_interval()) => {
                let spaces = match event_repo.get_all_spaces().await {
                    Ok(spaces) => spaces,
                    Err(err) => {
                        error!("Truth maintenance worker failed to list spaces: {}", err);
                        continue;
                    }
                };

                for space_id in spaces {
                    let after_seq = last_seq_by_space.get(&space_id).copied().unwrap_or(0);
                    let events = match event_repo
                        .get_after_seq(&space_id, after_seq, config.batch_size)
                        .await
                    {
                        Ok(events) => events,
                        Err(err) => {
                            error!(
                                "Truth maintenance worker failed to fetch events for {}: {}",
                                space_id, err
                            );
                            continue;
                        }
                    };

                    if events.is_empty() {
                        if after_seq > 0 {
                            match event_repo.get_latest_seq(&space_id).await {
                                Ok(latest) if latest < after_seq => {
                                    let reset_seq = 0;
                                    info!(
                                        "Truth maintenance seq reset detected for {}, resetting cursor from {} to {}",
                                        space_id, after_seq, reset_seq
                                    );
                                    last_seq_by_space.insert(space_id.clone(), reset_seq);
                                    last_event_id_by_space.remove(&space_id);
                                }
                                Ok(latest) if latest == after_seq => {
                                    if let Some(last_id) = last_event_id_by_space.get(&space_id) {
                                        let before_seq = latest.saturating_add(1);
                                        match event_repo
                                            .get_before_seq(&space_id, before_seq, 1)
                                            .await
                                        {
                                            Ok(latest_events) => {
                                                if let Some(latest_event) = latest_events.first() {
                                                    if &latest_event.base.id != last_id {
                                                        let reset_seq = 0;
                                                        info!(
                                                            "Truth maintenance seq reset detected for {}, resetting cursor from {} to {} (seq reuse)",
                                                            space_id, after_seq, reset_seq
                                                        );
                                                        last_seq_by_space.insert(space_id.clone(), reset_seq);
                                                        last_event_id_by_space.remove(&space_id);
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                warn!(
                                                    "Truth maintenance worker failed to fetch latest event for {}: {}",
                                                    space_id, err
                                                );
                                            }
                                        }
                                    }
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    warn!(
                                        "Truth maintenance worker failed to fetch latest seq for {}: {}",
                                        space_id, err
                                    );
                                }
                            }
                        }
                        continue;
                    }

                    info!(
                        "Truth maintenance processing {} events for {}",
                        events.len(),
                        space_id
                    );
                    let tm_storage = RepositoryTruthMaintenanceStorage::new(storage.clone());
                    match execute_tm_detect_conflicts_from_outbox_events(&events, &tm_storage).await
                    {
                        Ok(result) => {
                            info!(
                                "Truth maintenance detected {} direct commits and {} conflict groups for {}",
                                result.direct_commits.len(),
                                result.conflict_group_ids.len(),
                                space_id
                            );
                            for commit in result.direct_commits {
                                if let Err(err) = execute_tm_commit(
                                    &commit.resolution,
                                    &commit.input,
                                    &space_id,
                                    &tm_storage,
                                )
                                .await
                                {
                                    warn!(
                                        "Truth maintenance direct commit failed for {}: {}",
                                        space_id, err
                                    );
                                }
                            }

                            if !result.conflict_group_ids.is_empty() {
                                for group_id in result.conflict_group_ids {
                                    let work_item = WorkItem {
                                        base: BaseFields::new(
                                            RecordId::new_v4(TableName::WorkItem),
                                            space_id.clone(),
                                            "truth_maintenance".to_string(),
                                        ),
                                        kind: TM_RESOLVE_SLOT_KIND.to_string(),
                                        payload: serde_json::json!({
                                            "conflict_group_id": group_id.to_string()
                                        }),
                                        status: WorkItemStatus::Runnable,
                                        priority: 50,
                                        idempotency_key: format!(
                                            "tm_resolve_slot:{}:{}",
                                            group_id,
                                            events.last().map_or(after_seq, |event| event.seq)
                                        ),
                                        attempts: 0,
                                        lease_expires_at: None,
                                        leased_by: None,
                                        run_after: None,
                                        last_error: None,
                                    };

                                    if let Err(err) = work_item_repo.create(work_item).await {
                                        error!(
                                            "Truth maintenance worker failed to enqueue tm_resolve_slot for {}: {}",
                                            space_id, err
                                        );
                                    }
                                }
                            }

                            if let Some(last) = events.last() {
                                last_seq_by_space.insert(space_id.clone(), last.seq);
                                last_event_id_by_space.insert(space_id.clone(), last.base.id.clone());
                            }
                        }
                        Err(err) => {
                            warn!(
                                "Truth maintenance worker failed to process events for {}: {}",
                                space_id, err
                            );
                        }
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
/// the `elephant::embedding_progress` target, never to an API. Enable them
/// with `RUST_LOG=...,elephant::embedding_progress=info`.
async fn report_embedding_progress(node_repo: &DocNodeRepository, space_id: &str, model: &str) {
    match node_repo.count_embedding_progress(space_id).await {
        Ok(counts) => info!(
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
async fn run_embed_loop(
    doc_repo: DocRepository,
    node_repo: DocNodeRepository,
    revision_repo: DocRevisionRepository,
    embedding_repo: elephant_storage::NodeEmbeddingRepository,
    space_config_repo: SpaceConfigRepository,
    provider_cache: EmbeddingProviderCache,
    embed_config: EmbedWorkerConfig,
    mut rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let fallback_policy = policy_from_embed_config(&embed_config);
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
                let spaces = doc_repo.get_all_spaces().await.unwrap_or_else(|_| vec!["default".to_string()]);
                let spaces = if spaces.is_empty() { vec!["default".to_string()] } else { spaces };
                let report_progress = last_progress_report
                    .is_none_or(|at| at.elapsed() >= EMBED_PROGRESS_REPORT_INTERVAL);
                if report_progress {
                    last_progress_report = Some(std::time::Instant::now());
                }

                for space_id in spaces {
                    let policy = match resolve_embedding_policy(
                        &space_config_repo,
                        &space_id,
                        &fallback_policy,
                    )
                    .await
                    {
                        Ok(policy) => policy,
                        Err(err) => {
                            error!("Failed to resolve embedding policy for space {}: {}", space_id, err);
                            continue;
                        }
                    };

                    let provider = provider_cache
                        .get_or_create(policy.clone(), build_embedding_provider)
                        .await;
                    let worker = EmbedWorker::new(
                        node_repo.clone(),
                        revision_repo.clone(),
                        embedding_repo.clone(),
                        provider,
                    );

                    match worker.process_pending(&space_id, embed_config.batch_size).await {
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
                client,
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
                        match worker.process_work_history(batch_size).await {
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
                client,
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
                        match worker.process_cleanup().await {
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
        let embed_config = config.embed.clone();
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
            let provider_cache = EmbeddingProviderCache::new();
            let fallback_policy = policy_from_embed_config(&embed_config);

            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!("Maintenance worker received shutdown signal");
                            break;
                        }
                    }
                    () = tokio::time::sleep(maintenance_config.poll_interval()) => {
                        // Get space IDs to sweep. If no space_config records exist,
                        // fall back to sweeping "default" (the implicit space).
                        let space_ids: Vec<String> = match space_config_repo.list_all(100, None).await {
                            Ok(s) if !s.is_empty() => s.into_iter().map(|sc| sc.base.space_id).collect(),
                            Ok(_) => vec!["default".to_string()],
                            Err(e) => {
                                error!("Maintenance worker failed to list spaces: {e}");
                                continue;
                            }
                        };

                        for space_id in &space_ids {
                            let embedding_policy = match resolve_embedding_policy(
                                &space_config_repo,
                                space_id,
                                &fallback_policy,
                            ).await {
                                Ok(policy) => policy,
                                Err(e) => {
                                    error!(
                                        "Maintenance worker failed to resolve embedding policy for '{space_id}': {e}",
                                    );
                                    continue;
                                }
                            };
                            let embedding_provider = provider_cache
                                .get_or_create(embedding_policy, build_embedding_provider)
                                .await;
                            match Box::pin(elephant_pipeline::maintenance::run_maintenance_sweep(
                                &client,
                                space_id,
                                &maint_cfg,
                                embedding_provider.as_ref(),
                            )).await {
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

    tokio::select! {
        changed = shutdown_rx.changed() => {
            changed?;
            info!("Received shutdown signal, stopping workers...");
        }
        result = tasks.join_next(), if !tasks.is_empty() => {
            match result {
                Some(Ok(Ok(()))) => {
                    anyhow::bail!("pipeline worker exited unexpectedly");
                }
                Some(Ok(Err(err))) => {
                    return Err(err.context("pipeline worker stopped with error"));
                }
                Some(Err(err)) => {
                    anyhow::bail!("pipeline worker task panicked: {err}");
                }
                None => {
                    anyhow::bail!("all pipeline workers exited unexpectedly");
                }
            }
        }
    }

    // Wait for all workers to complete
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => info!("Worker stopped successfully"),
            Ok(Err(e)) => warn!("Worker stopped with error: {}", e),
            Err(e) => error!("Worker task panicked: {}", e),
        }
    }

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

        let (graph_changed, affected) = knowledge_view_event_records(&[event]);

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
