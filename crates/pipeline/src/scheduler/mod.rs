//! Work scheduler for durable job execution.
//!
//! Per spec §5, the scheduler provides:
//! - Priority queue with lease semantics
//! - DAG-based dependency tracking
//! - Staged commit workflow
//! - Dead letter handling
//! - Plugin security enforcement (§2.4a)
//! - Plugin configuration and discovery (§2.4a)
//! - Budget enforcement (§6.2, §17)
//! - Progressive ingestion triage (§6.3)

pub mod budget;
pub mod dag_executor;
pub mod deadletter;
pub mod evidence_work_kinds;
pub mod execution_contract;
pub mod extraction_repair;
pub mod extraction_work_kinds;
pub mod grounding;
pub mod ingest_work_kinds;
pub mod inline_promotion;
pub mod plugin_config;
pub mod plugin_security;
pub mod promotion_disambiguation;
mod promotion_plan;
pub mod source_stage;
pub mod staged_commit;
pub mod triage;
pub mod work_queue;
pub mod worker_registry;

pub use crate::freshness::{execute_freshness_sweep, SweepResult, WORK_KIND_FRESHNESS_SWEEP};
pub use crate::reprocess::{
    create_reprocess_job, execute_reprocess_job, ReprocessResult, WORK_KIND_CREATE_REPROCESS_JOB,
    WORK_KIND_EXECUTE_REPROCESS_JOB,
};
pub use crate::resolution::stage_resolution_patches;
pub use crate::summarizer::{NoOpSummarizer, SummarizationBudget, SummarizationContext};
pub use crate::summarizer::{SummarizationOptions, Summarizer, SummaryResult};
pub use crate::truth_maintenance::{
    execute_tm_commit, execute_tm_detect_conflicts, execute_tm_detect_conflicts_from_outbox_events,
    execute_tm_recompute, execute_tm_resolve_slot, CommitResult, DetectConflictsResult,
    RecomputeResult, TM_COMMIT_KIND, TM_DETECT_CONFLICTS_KIND, TM_RECOMPUTE_KIND,
    TM_RESOLVE_SLOT_KIND,
};
pub use budget::{
    AllBudgetsResult, BudgetCheckResult, BudgetEnforcer, CapBreachResult, CostReconciliation,
    DailyCapCheck,
};
pub use dag_executor::DagExecutor;
pub use deadletter::DeadletterHandler;
pub use evidence_work_kinds::{
    execute_build_doc_tree, execute_render_doc, validate_node_spans, validate_span,
    BuildDocTreeResult, DocTreeConfig, RenderDocResult, BUILD_DOC_TREE_KIND, EMBED_CHUNKS_KIND,
    RENDER_DOC_KIND,
};
pub use extraction_work_kinds::{
    execute_extract_assertions, execute_plan_extraction, resolve_extraction_guidance_status,
    validate_extraction_guidance_assets, ExtractAssertionsResult, ExtractionPlanConfig,
    PlanExtractionResult, EXTRACT_ASSERTIONS_KIND, PLAN_EXTRACTION_KIND,
};
pub use ingest_work_kinds::{
    execute_ingest_metadata, execute_ingest_source_stub, execute_triage_source,
    execute_upgrade_ingest_mode, IngestMetadataResult, IngestSourceStubResult, TriageSourceResult,
    UpgradeIngestModeResult, INGEST_METADATA_KIND, INGEST_SOURCE_STUB_KIND, TRIAGE_SOURCE_KIND,
    UPGRADE_INGEST_MODE_KIND,
};
pub use inline_promotion::{
    promote_assertions_inline, promote_assertions_inline_with_options, PromotionOptions,
    PromotionResult,
};
pub use plugin_config::PluginConfigRegistry;
pub use plugin_security::{
    AbacFilter, OutputSecurityValidator, PluginSecurityContext, PluginSecurityEnforcer,
    ToolAccessEnforcer,
};
pub use promotion_disambiguation::{
    promotion_disambiguator_from, CandidateEntity, DisambiguationRequest,
    PromotionDisambiguationConfig, PromotionDisambiguationDecision, PromotionDisambiguator,
};
pub use staged_commit::StagedCommit;
pub use staged_commit::{
    commit_artifacts::KIND as COMMIT_ARTIFACTS_KIND,
    validate_artifacts::KIND as VALIDATE_ARTIFACTS_KIND,
};
pub use triage::{DefaultTriagePlugin, TriageContext, TriagePlugin, TriageResult};
pub use work_queue::WorkQueue;
pub use worker_registry::{Worker, WorkerRegistry};

#[cfg(feature = "meerkat-extraction")]
pub use crate::summarizer::AgenticSummarizer;
#[cfg(feature = "meerkat-extraction")]
pub use promotion_disambiguation::AgenticPromotionDisambiguator;

#[cfg(test)]
mod multimodal_e2e_tests;

/// Re-export work kinds for convenience.
pub mod work_kinds {
    pub use super::evidence_work_kinds::{
        execute_build_doc_tree, execute_render_doc, validate_node_spans, validate_span,
        BUILD_DOC_TREE_KIND, EMBED_CHUNKS_KIND, RENDER_DOC_KIND,
    };
    pub use super::extraction_work_kinds::{
        execute_extract_assertions, execute_plan_extraction, EXTRACT_ASSERTIONS_KIND,
        PLAN_EXTRACTION_KIND,
    };
    pub use super::ingest_work_kinds::{
        execute_ingest_metadata, execute_ingest_source_stub, execute_triage_source,
        execute_upgrade_ingest_mode, INGEST_METADATA_KIND, INGEST_SOURCE_STUB_KIND,
        TRIAGE_SOURCE_KIND, UPGRADE_INGEST_MODE_KIND,
    };
    pub use crate::freshness::{execute_freshness_sweep, WORK_KIND_FRESHNESS_SWEEP};
    pub use crate::reprocess::{
        create_reprocess_job, execute_reprocess_job, WORK_KIND_CREATE_REPROCESS_JOB,
        WORK_KIND_EXECUTE_REPROCESS_JOB,
    };
    pub use crate::resolution::{commit_resolution_patches, validate_resolution_patches};
    pub use crate::truth_maintenance::{
        execute_tm_commit, execute_tm_detect_conflicts,
        execute_tm_detect_conflicts_from_outbox_events, execute_tm_recompute,
        execute_tm_resolve_slot, TM_COMMIT_KIND, TM_DETECT_CONFLICTS_KIND, TM_RECOMPUTE_KIND,
        TM_RESOLVE_SLOT_KIND,
    };

    /// Planned non-active work kinds that are intentionally scaffolded.
    pub mod experimental {
        pub use super::super::ops::{rebuild_indexes, reembed, snapshot_export};
        pub use super::super::staged_commit::{commit_artifacts, validate_artifacts};
    }
}

mod ops;
