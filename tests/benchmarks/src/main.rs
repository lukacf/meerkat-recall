//! Benchmark runner for elephant-db kernel operations, backend comparisons,
//! and embedded Elephant flows.

#![recursion_limit = "256"]

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use elephant_db::ElephantDb;
use elephant_e2e::scenarios::s5_13_alice_in_wonderland::extract_alice_graph_snapshot;
use elephant_e2e::scenarios::s5_15_wizard_of_oz::extract_oz_graph_snapshot;
use elephant_e2e::scenarios::s5_16_elephant_session_log::extract_elephant_session_graph_snapshot;
use elephant_embeddings::{level_bucket, HashedEmbeddingProvider, PartitionKey, PartitionManager};
use elephant_mcp::handlers::GetEntityWithRelationsHandler;
use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_agent_dispatcher_with_context,
    create_dispatcher, ToolContext, ToolDispatcher, ToolResponse,
};
use elephant_migrations::apply_schema;
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{execute_build_doc_tree, execute_render_doc, DocTreeConfig};
use elephant_pipeline::EmbedWorker;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, DocNodeRepository, DocRepository, DocRevisionRepository, EntityRepository,
    NodeEmbeddingRepository, RelRepository, Repository, SearchRepository, SpaceConfigRepository,
    StorageBackendKind, StorageClient, StorageConfig,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::space::SpaceConfig;
use elephant_types::records::{Assertion, Doc, NodeEmbedding, Rel};
use elephant_types::{Link, RecordId};
use meerkat::{
    AgentBuildConfig, AgentEvent, AgentFactory, AgentToolDispatcher, BudgetLimits, CommsCommand,
    Config, InputSource, ToolDef,
};
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ContentInput, HandlingMode, SessionId, ToolCallView, ToolResult};
use meerkat_core::InputStreamMode;
use meerkat_core::ToolCategoryOverride;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, watch, Barrier};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone, Parser)]
#[command(name = "elephant-benchmarks")]
#[command(about = "Benchmark harness for elephant-db kernel and embedded Elephant flows")]
struct Cli {
    /// Which benchmark suite to run.
    #[arg(long, value_enum, default_value_t = BenchmarkSuite::All)]
    suite: BenchmarkSuite,
    /// Which storage backend to target for backend-aware suites.
    #[arg(long, value_enum, default_value_t = BenchmarkBackend::ElephantDb)]
    backend: BenchmarkBackend,
    /// Dataset and iteration scale.
    #[arg(long, value_enum, default_value_t = BenchmarkScale::Solid)]
    scale: BenchmarkScale,
    /// Print JSON instead of the human-readable table.
    #[arg(long)]
    json: bool,
    /// Optional path to write the JSON report to.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkSuite {
    Kernel,
    Concurrency,
    PipelineConcurrency,
    Compare,
    Scenario,
    Quality,
    Transcripts,
    AliceQa,
    SessionQa,
    OzQa,
    All,
}

#[derive(Debug, Clone, Copy, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkScale {
    Quick,
    Solid,
}

#[derive(Debug, Clone, Copy, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum BenchmarkBackend {
    ElephantDb,
    SurrealDb,
    Both,
}

#[derive(Debug, Clone, Copy)]
struct ScaleConfig {
    graph_small_nodes: usize,
    graph_large_nodes: usize,
    edges_per_node: usize,
    graph_small_iters: usize,
    graph_large_iters: usize,
    doc_chunks: usize,
    doc_iters: usize,
    vector_medium_count: usize,
    vector_large_count: usize,
    vector_dims: usize,
    vector_medium_iters: usize,
    vector_large_iters: usize,
    snapshot_entities: usize,
    snapshot_doc_count: usize,
    snapshot_chunks_per_doc: usize,
    snapshot_iters: usize,
    compare_graph_nodes: usize,
    compare_graph_iters: usize,
    compare_doc_chunks: usize,
    compare_doc_iters: usize,
    alice_total_iters: usize,
    alice_search_iters: usize,
    transcript_docs_per_corpus: usize,
    transcript_mixed_docs_per_source: usize,
    transcript_query_terms: usize,
    transcript_max_doc_chars: usize,
    transcript_max_block_chars: usize,
    transcript_max_blocks: usize,
}

impl BenchmarkScale {
    const fn config(self) -> ScaleConfig {
        match self {
            Self::Quick => ScaleConfig {
                graph_small_nodes: 2_000,
                graph_large_nodes: 8_000,
                edges_per_node: 4,
                graph_small_iters: 150,
                graph_large_iters: 50,
                doc_chunks: 1_000,
                doc_iters: 200,
                vector_medium_count: 2_000,
                vector_large_count: 8_000,
                vector_dims: 256,
                vector_medium_iters: 100,
                vector_large_iters: 40,
                snapshot_entities: 500,
                snapshot_doc_count: 40,
                snapshot_chunks_per_doc: 15,
                snapshot_iters: 6,
                compare_graph_nodes: 1_000,
                compare_graph_iters: 120,
                compare_doc_chunks: 800,
                compare_doc_iters: 120,
                alice_total_iters: 1,
                alice_search_iters: 10,
                transcript_docs_per_corpus: 8,
                transcript_mixed_docs_per_source: 4,
                transcript_query_terms: 6,
                transcript_max_doc_chars: 24_000,
                transcript_max_block_chars: 1_500,
                transcript_max_blocks: 120,
            },
            Self::Solid => ScaleConfig {
                graph_small_nodes: 5_000,
                graph_large_nodes: 20_000,
                edges_per_node: 4,
                graph_small_iters: 300,
                graph_large_iters: 120,
                doc_chunks: 4_000,
                doc_iters: 300,
                vector_medium_count: 5_000,
                vector_large_count: 20_000,
                vector_dims: 256,
                vector_medium_iters: 150,
                vector_large_iters: 60,
                snapshot_entities: 2_000,
                snapshot_doc_count: 100,
                snapshot_chunks_per_doc: 25,
                snapshot_iters: 10,
                compare_graph_nodes: 2_500,
                compare_graph_iters: 200,
                compare_doc_chunks: 2_000,
                compare_doc_iters: 180,
                alice_total_iters: 3,
                alice_search_iters: 40,
                transcript_docs_per_corpus: 16,
                transcript_mixed_docs_per_source: 8,
                transcript_query_terms: 6,
                transcript_max_doc_chars: 36_000,
                transcript_max_block_chars: 2_000,
                transcript_max_blocks: 180,
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    generated_at: String,
    suite: BenchmarkSuite,
    backend: BenchmarkBackend,
    scale: BenchmarkScale,
    cases: Vec<BenchmarkCase>,
}

#[derive(Debug, Serialize)]
struct BenchmarkCase {
    section: String,
    name: String,
    iterations: usize,
    metrics: BenchmarkMetrics,
    details: Value,
}

#[derive(Debug, Serialize)]
struct BenchmarkMetrics {
    total_ms: f64,
    mean_ms: f64,
    stddev_ms: f64,
    median_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    max_ms: f64,
    ops_per_sec: f64,
}

#[derive(Debug)]
struct GraphFixture {
    db: ElephantDb,
    start_id: RecordId,
    node_count: usize,
    edge_count: usize,
}

#[derive(Debug)]
struct DocFixture {
    db: ElephantDb,
    revision_id: RecordId,
    deep_chunk_id: RecordId,
    node_count: usize,
    chunk_count: usize,
    depth: i32,
}

#[derive(Debug)]
struct VectorFixture {
    db: ElephantDb,
    partition_name: String,
    query_vector: Vec<f32>,
    chunk_count: usize,
    dims: usize,
}

#[derive(Debug)]
struct ConcurrencyFixture {
    db: Arc<ElephantDb>,
    space_id: String,
    entity_ids: Arc<Vec<RecordId>>,
    start_id: RecordId,
    partition_name: String,
    query_vector: Arc<Vec<f32>>,
}

#[derive(Debug)]
struct SnapshotFixture {
    db: ElephantDb,
    snapshot_bytes: Vec<u8>,
    entity_count: usize,
    rel_count: usize,
    doc_count: usize,
    chunk_count: usize,
    embedding_count: usize,
}

#[derive(Debug)]
struct ConcurrencyRun {
    elapsed: Duration,
    logical_ops: usize,
    ops_per_sec: f64,
    mean_thread_ms: f64,
    max_thread_ms: f64,
}

struct AliceFixture {
    dispatcher: Arc<ToolDispatcher>,
    chunk_count: usize,
    doc_count: usize,
    search_query: String,
    expected_top_title: String,
    observed_top_title: Option<String>,
    search_result_count: usize,
}

struct SearchFixture {
    dispatcher: Arc<ToolDispatcher>,
    client: StorageClient,
    doc_count: usize,
    chunk_count: usize,
}

struct AliceQaFixture {
    client: StorageClient,
    doc_count: usize,
    entity_count: usize,
    assertion_count: usize,
    rel_count: usize,
}

#[derive(Debug, Clone)]
struct AliceGraphArtifact {
    space_config: SpaceConfig,
    docs: Vec<Doc>,
    entities: Vec<Entity>,
    assertions: Vec<Assertion>,
    rels: Vec<Rel>,
}

#[derive(Debug)]
struct AliceRun {
    total: Duration,
    ingest_two_docs: Duration,
    render_docs: Duration,
    build_doc_trees: Duration,
    embed_chunks: Duration,
    search_once: Duration,
    chunk_count: usize,
    doc_count: usize,
    observed_top_title: Option<String>,
    search_result_count: usize,
}

#[derive(Debug, Serialize)]
struct PipelineDocRun {
    title: String,
    ingest_ms: f64,
    render_ms: f64,
    build_doc_tree_ms: f64,
    embed_ms: f64,
    total_ms: f64,
    chunk_count: usize,
}

#[derive(Debug)]
struct PipelineOverlapRun {
    total: Duration,
    docs: Vec<PipelineDocRun>,
    search_metrics: BenchmarkMetrics,
    search_count: usize,
    non_empty_search_count: usize,
}

#[derive(Debug)]
struct BackendGraphFixture {
    client: StorageClient,
    start_id: RecordId,
    node_count: usize,
    edge_count: usize,
}

#[derive(Debug)]
struct BackendDocFixture {
    client: StorageClient,
    revision_id: RecordId,
    deep_chunk_id: RecordId,
    node_count: usize,
    chunk_count: usize,
    depth: i32,
}

#[derive(Debug, Clone, Serialize)]
struct SearchExpectation {
    query: String,
    expected_top_title: String,
    query_kind: String,
}

#[derive(Debug, Clone, Serialize)]
struct QueryQualityResult {
    query: String,
    query_kind: String,
    search_mode: String,
    expected_top_title: String,
    top_title: String,
    titles: Vec<String>,
    reciprocal_rank: f64,
    top1_match: bool,
    search_ms: f64,
}

#[derive(Debug, Clone, Copy)]
enum SearchMode {
    Semantic,
    RawText,
}

impl SearchMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::RawText => "raw-text",
        }
    }
}

#[derive(Debug, Clone)]
struct TranscriptDocument {
    title: String,
    text: String,
    collection: String,
    source: &'static str,
    source_path: PathBuf,
    project_hint: Option<String>,
    first_user_prompt: Option<String>,
    last_user_prompt: Option<String>,
    tool_event_count: usize,
}

#[derive(Debug, Clone)]
struct TranscriptScenario {
    name: &'static str,
    docs: Vec<TranscriptDocument>,
}

#[derive(Debug, Clone, Copy)]
struct TranscriptExtractionLimits {
    doc_chars: usize,
    block_chars: usize,
    blocks: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum TranscriptQueryStrategy {
    FirstUser,
    LastUser,
    SessionSalience,
}

impl TranscriptQueryStrategy {
    const fn label(self) -> &'static str {
        match self {
            Self::FirstUser => "first-user",
            Self::LastUser => "last-user",
            Self::SessionSalience => "session-salience",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TranscriptOrdering {
    ModifiedDesc,
    FileSizeDesc,
}

#[derive(Debug, Clone)]
struct AliceQuestion {
    category: &'static str,
    question: &'static str,
    expected_answers: &'static [&'static str],
}

#[derive(Debug, Clone, Serialize)]
struct AliceQaResult {
    question: String,
    category: String,
    expected_answers: Vec<String>,
    found_answers: Vec<String>,
    correct: bool,
    top_entities: Vec<String>,
    top_assertions: Vec<String>,
    search_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct AliceAgentQaResult {
    question: String,
    category: String,
    expected_answers: Vec<String>,
    answer: String,
    supporting_facts: Vec<String>,
    supporting_entity_ids: Vec<String>,
    supporting_assertion_ids: Vec<String>,
    supporting_doc_ids: Vec<String>,
    supporting_quotes: Vec<String>,
    correct: bool,
    answer_ms: f64,
    estimated_model_ms: f64,
    tool_ms: f64,
    turns: u32,
    tool_calls: u32,
    tool_events: u32,
    turn_events: u32,
    input_tokens: usize,
    output_tokens: usize,
    total_tokens: usize,
    cumulative_total_tokens: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct AliceAgentQaAnswer {
    answer: String,
    #[serde(default)]
    supporting_facts: Vec<String>,
    #[serde(default)]
    supporting_entity_ids: Vec<String>,
    #[serde(default)]
    supporting_assertion_ids: Vec<String>,
    #[serde(default)]
    supporting_doc_ids: Vec<String>,
    #[serde(default)]
    supporting_quotes: Vec<String>,
}

struct AliceGraphQaAgentHarness {
    comms: Arc<dyn meerkat_core::CommsRuntime>,
    session_id: SessionId,
    host_task: JoinHandle<Result<(), String>>,
    event_rx: mpsc::Receiver<AgentEvent>,
    previous_input_tokens: u64,
    previous_output_tokens: u64,
}

#[derive(Debug, Default)]
struct AgentRunTrace {
    tool_ms: u64,
    tool_events: u32,
    turn_events: u32,
}

impl Drop for AliceGraphQaAgentHarness {
    fn drop(&mut self) {
        self.host_task.abort();
    }
}

#[derive(Debug)]
struct HostModeRunOutcome {
    result_text: String,
    usage_input_tokens: u64,
    usage_output_tokens: u64,
    turns: u32,
    tool_calls: u32,
    trace: AgentRunTrace,
}

#[derive(Debug, Clone)]
struct EntitySearchCandidate {
    id: RecordId,
    display_name: String,
}

/// Adapter bridging `elephant_mcp`'s `ToolDispatcher` to meerkat's `AgentToolDispatcher`.
struct ElephantToolBridge {
    dispatcher: ToolDispatcher,
    tool_defs: Arc<[Arc<ToolDef>]>,
}

impl ElephantToolBridge {
    fn new(dispatcher: ToolDispatcher) -> Self {
        let tool_defs: Arc<[Arc<ToolDef>]> = dispatcher
            .all_tool_meta()
            .into_iter()
            .map(|meta| Arc::new(ToolDef::new(meta.name, meta.description, meta.input_schema)))
            .collect();
        Self {
            dispatcher,
            tool_defs,
        }
    }
}

#[async_trait]
impl AgentToolDispatcher for ElephantToolBridge {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        self.tool_defs.clone()
    }

    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        let args =
            serde_json::from_str(call.args.get()).unwrap_or_else(|_| Value::Object(Map::default()));
        match self.dispatcher.execute(call.name, args).await {
            Ok(result) => {
                let content = serde_json::to_string(&result).unwrap_or_default();
                Ok(ToolDispatchOutcome::from(ToolResult::new(
                    call.id.to_string(),
                    content,
                    false,
                )))
            }
            Err(error) => Ok(ToolDispatchOutcome::from(ToolResult::new(
                call.id.to_string(),
                format!("Tool error: {error}"),
                true,
            ))),
        }
    }
}

fn quality_queries() -> Vec<SearchExpectation> {
    vec![
        SearchExpectation {
            query: "white rabbit rabbit hole wonderland".to_string(),
            expected_top_title: "Alice's Adventures in Wonderland".to_string(),
            query_kind: "synthetic".to_string(),
        },
        SearchExpectation {
            query: "butter sugar cocoa vanilla bake cake".to_string(),
            expected_top_title: "Chocolate Cake Recipe".to_string(),
            query_kind: "synthetic".to_string(),
        },
        SearchExpectation {
            query: "lagomorph burrow long ears herbivore".to_string(),
            expected_top_title: "Rabbit Biology Primer".to_string(),
            query_kind: "synthetic".to_string(),
        },
        SearchExpectation {
            query: "database rows query table sql".to_string(),
            expected_top_title: "SQL Query Basics".to_string(),
            query_kind: "synthetic".to_string(),
        },
        SearchExpectation {
            query: "queen hearts croquet wonderland".to_string(),
            expected_top_title: "Alice's Adventures in Wonderland".to_string(),
            query_kind: "synthetic".to_string(),
        },
    ]
}

fn selected_storage_backends(selection: BenchmarkBackend) -> Vec<StorageBackendKind> {
    match selection {
        BenchmarkBackend::ElephantDb => vec![StorageBackendKind::ElephantDb],
        BenchmarkBackend::SurrealDb => vec![StorageBackendKind::SurrealDb],
        BenchmarkBackend::Both => vec![
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ],
    }
}

const fn backend_label(kind: StorageBackendKind) -> &'static str {
    match kind {
        StorageBackendKind::ElephantDb => "elephant-db",
        StorageBackendKind::SurrealDb => "surreal-db",
    }
}

fn include_local_session_qa_in_all() -> bool {
    std::env::var("ELEPHANT_ENABLE_LOCAL_SESSION_QA")
        .ok()
        .as_deref()
        .is_some_and(|value| matches!(value, "1" | "true" | "yes"))
}

fn include_local_transcripts_in_all() -> bool {
    std::env::var("ELEPHANT_ENABLE_LOCAL_TRANSCRIPTS")
        .ok()
        .as_deref()
        .is_some_and(|value| matches!(value, "1" | "true" | "yes"))
}

fn include_llm_qa_in_all() -> bool {
    std::env::var("ELEPHANT_ENABLE_LLM_QA")
        .ok()
        .as_deref()
        .is_some_and(|value| matches!(value, "1" | "true" | "yes"))
}

fn transcript_inputs_available_under(home: &Path) -> bool {
    collect_direct_paths(&home.join(".codex").join("sessions"), "json")
        .map(|paths| !paths.is_empty())
        .unwrap_or(false)
        || collect_direct_paths(&home.join(".codex").join("archived_sessions"), "jsonl")
            .map(|paths| !paths.is_empty())
            .unwrap_or(false)
        || collect_claude_project_paths(&home.join(".claude").join("projects"))
            .map(|paths| !paths.is_empty())
            .unwrap_or(false)
}

fn local_transcript_inputs_available() -> bool {
    let Ok(home) = home_dir() else {
        return false;
    };

    transcript_inputs_available_under(&home)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BenchmarkLlmProvider {
    Anthropic,
    OpenAi,
    Gemini,
}

impl BenchmarkLlmProvider {
    const fn env_names(self) -> &'static [&'static str] {
        match self {
            Self::Anthropic => &["ANTHROPIC_API_KEY"],
            Self::OpenAi => &["OPENAI_API_KEY"],
            Self::Gemini => &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        }
    }

    const fn display_name(self) -> &'static str {
        match self {
            Self::Anthropic => "Anthropic",
            Self::OpenAi => "OpenAI",
            Self::Gemini => "Gemini",
        }
    }
}

fn infer_llm_provider(model: &str) -> Option<BenchmarkLlmProvider> {
    let normalized = model.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    if normalized.starts_with("claude") || normalized.starts_with("anthropic/") {
        return Some(BenchmarkLlmProvider::Anthropic);
    }
    if normalized.starts_with("gpt-")
        || normalized.starts_with("openai/")
        || ["o1", "o3", "o4", "o5"]
            .iter()
            .any(|prefix| normalized.starts_with(prefix))
    {
        return Some(BenchmarkLlmProvider::OpenAi);
    }
    if normalized.starts_with("gemini") || normalized.starts_with("google/") {
        return Some(BenchmarkLlmProvider::Gemini);
    }

    None
}

fn extraction_model() -> String {
    std::env::var("EXTRACTION_MODEL").unwrap_or_else(|_| "gpt-5.2".to_string())
}

fn has_nonempty_env(env_getter: &impl Fn(&str) -> Option<String>, name: &str) -> bool {
    env_getter(name)
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn missing_llm_benchmark_credentials_with(
    extraction_model: &str,
    qa_model: &str,
    env_getter: impl Fn(&str) -> Option<String>,
) -> Result<Vec<String>> {
    let models = [("extraction", extraction_model), ("qa", qa_model)];
    let mut missing: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();

    for (role, model) in models {
        let provider = infer_llm_provider(model).ok_or_else(|| {
            anyhow!("could not infer provider credentials for {role} model `{model}`")
        })?;
        if provider
            .env_names()
            .iter()
            .any(|name| has_nonempty_env(&env_getter, name))
        {
            continue;
        }

        let env_label = provider.env_names().join(" or ");
        missing.entry(env_label).or_default().push(format!(
            "{role} model `{model}` ({})",
            provider.display_name()
        ));
    }

    Ok(missing
        .into_iter()
        .map(|(env_label, usages)| format!("{env_label} for {}", usages.join(", ")))
        .collect())
}

fn missing_llm_benchmark_credentials() -> Result<Vec<String>> {
    let extraction_model = extraction_model();
    let qa_model = qa_model();
    missing_llm_benchmark_credentials_with(&extraction_model, &qa_model, |name| {
        std::env::var(name).ok()
    })
}

fn ensure_llm_benchmark_configured() -> Result<()> {
    let missing = missing_llm_benchmark_credentials()?;
    if missing.is_empty() {
        return Ok(());
    }

    bail!("llm-backed QA benchmarks require {}", missing.join("; "))
}

fn session_qa_input_configured() -> bool {
    std::env::var("ELEPHANT_SESSION_QA_TEXT_PATH").is_ok()
        || (std::env::var("ELEPHANT_SESSION_QA_SESSION_ID").is_ok()
            && std::env::var("ELEPHANT_SESSION_QA_HISTORY_PATH").is_ok())
}

fn ensure_session_qa_input_configured() -> Result<()> {
    if session_qa_input_configured() {
        return Ok(());
    }

    bail!(
        "session-qa requires ELEPHANT_SESSION_QA_TEXT_PATH or ELEPHANT_SESSION_QA_SESSION_ID together with ELEPHANT_SESSION_QA_HISTORY_PATH"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = cli.scale.config();

    let mut cases = Vec::new();

    match cli.suite {
        BenchmarkSuite::Kernel => {
            cases.extend(run_kernel_suite(config)?);
        }
        BenchmarkSuite::Concurrency => {
            cases.extend(run_concurrency_suite(config)?);
        }
        BenchmarkSuite::PipelineConcurrency => {
            cases.extend(run_pipeline_concurrency_suite(cli.backend).await?);
        }
        BenchmarkSuite::Compare => {
            cases.extend(run_compare_suite(config, cli.backend).await?);
        }
        BenchmarkSuite::Scenario => {
            cases.extend(run_scenario_suite(config, cli.backend).await?);
        }
        BenchmarkSuite::Quality => {
            cases.extend(run_quality_suite(cli.backend).await?);
        }
        BenchmarkSuite::Transcripts => {
            let transcript_cases = run_transcript_suite(config, cli.backend).await?;
            if transcript_cases.is_empty() {
                bail!(
                    "no transcript scenarios were found; configure local ~/.codex or ~/.claude corpora before running --suite transcripts"
                );
            }
            cases.extend(transcript_cases);
        }
        BenchmarkSuite::AliceQa => {
            ensure_llm_benchmark_configured()?;
            cases.extend(Box::pin(run_alice_qa_suite(cli.backend)).await?);
        }
        BenchmarkSuite::SessionQa => {
            ensure_llm_benchmark_configured()?;
            ensure_session_qa_input_configured()?;
            cases.extend(Box::pin(run_elephant_session_qa_suite(cli.backend)).await?);
        }
        BenchmarkSuite::OzQa => {
            ensure_llm_benchmark_configured()?;
            cases.extend(Box::pin(run_oz_qa_suite(cli.backend)).await?);
        }
        BenchmarkSuite::All => {
            cases.extend(run_kernel_suite(config)?);
            cases.extend(run_concurrency_suite(config)?);
            cases.extend(run_pipeline_concurrency_suite(cli.backend).await?);
            cases.extend(run_compare_suite(config, cli.backend).await?);
            cases.extend(run_scenario_suite(config, cli.backend).await?);
            cases.extend(run_quality_suite(cli.backend).await?);
            if include_local_transcripts_in_all() {
                if !local_transcript_inputs_available() {
                    bail!(
                        "ELEPHANT_ENABLE_LOCAL_TRANSCRIPTS=1 requires local ~/.codex or ~/.claude transcript corpora"
                    );
                }
                cases.extend(run_transcript_suite(config, cli.backend).await?);
            }
            if include_llm_qa_in_all() {
                ensure_llm_benchmark_configured()?;
                cases.extend(Box::pin(run_alice_qa_suite(cli.backend)).await?);
                if include_local_session_qa_in_all() {
                    ensure_session_qa_input_configured()?;
                    cases.extend(Box::pin(run_elephant_session_qa_suite(cli.backend)).await?);
                }
                cases.extend(Box::pin(run_oz_qa_suite(cli.backend)).await?);
            }
        }
    }

    let report = BenchmarkReport {
        generated_at: Utc::now().to_rfc3339(),
        suite: cli.suite,
        backend: cli.backend,
        scale: cli.scale,
        cases,
    };

    if let Some(path) = cli.json_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create benchmark report directory `{}`",
                    parent.display()
                )
            })?;
        }
        fs::write(&path, serde_json::to_vec_pretty(&report)?)
            .with_context(|| format!("failed to write benchmark report to `{}`", path.display()))?;
    }

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_human_report(&report);
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_kernel_suite(config: ScaleConfig) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();

    let graph_build_iters = 3;
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "graph_build_{}_nodes_{}_edges",
            config.graph_small_nodes,
            config.graph_small_nodes * config.edges_per_node
        ),
        graph_build_iters,
        0,
        json!({
            "node_count": config.graph_small_nodes,
            "edge_count": config.graph_small_nodes * config.edges_per_node,
            "edges_per_node": config.edges_per_node,
        }),
        || build_graph_fixture(config.graph_small_nodes, config.edges_per_node),
    )?);

    let graph_small = build_graph_fixture(config.graph_small_nodes, config.edges_per_node)?;
    let small_graph_hits = graph_small
        .db
        .graph_neighbors("bench", &graph_small.start_id, 2)?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "graph_query_h2_{}_nodes_{}_edges",
            graph_small.node_count, graph_small.edge_count
        ),
        config.graph_small_iters,
        5,
        json!({
            "node_count": graph_small.node_count,
            "edge_count": graph_small.edge_count,
            "hops": 2,
            "result_count": small_graph_hits,
        }),
        || {
            Ok(graph_small
                .db
                .graph_neighbors("bench", &graph_small.start_id, 2)?)
        },
    )?);

    let graph_large = build_graph_fixture(config.graph_large_nodes, config.edges_per_node)?;
    let large_graph_hits = graph_large
        .db
        .graph_neighbors("bench", &graph_large.start_id, 3)?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "graph_query_h3_{}_nodes_{}_edges",
            graph_large.node_count, graph_large.edge_count
        ),
        config.graph_large_iters,
        3,
        json!({
            "node_count": graph_large.node_count,
            "edge_count": graph_large.edge_count,
            "hops": 3,
            "result_count": large_graph_hits,
        }),
        || {
            Ok(graph_large
                .db
                .graph_neighbors("bench", &graph_large.start_id, 3)?)
        },
    )?);

    let doc_build_iters = 3;
    cases.push(benchmark_sync_case(
        "kernel",
        format!("doc_tree_build_{}_chunks", config.doc_chunks),
        doc_build_iters,
        0,
        json!({
            "chunk_count": config.doc_chunks,
        }),
        || build_doc_fixture(config.doc_chunks),
    )?);

    let doc_fixture = build_doc_fixture(config.doc_chunks)?;
    let doc_chunks = doc_fixture
        .db
        .doc_chunks_for_revision("bench", &doc_fixture.revision_id)?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!("doc_chunks_lookup_{}_chunks", doc_fixture.chunk_count),
        config.doc_iters,
        10,
        json!({
            "node_count": doc_fixture.node_count,
            "chunk_count": doc_fixture.chunk_count,
            "revision_id": doc_fixture.revision_id.to_string(),
            "result_count": doc_chunks,
        }),
        || {
            Ok(doc_fixture
                .db
                .doc_chunks_for_revision("bench", &doc_fixture.revision_id)?)
        },
    )?);

    let parent_chain_depth = doc_fixture
        .db
        .doc_parent_chain("bench", &doc_fixture.deep_chunk_id)?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!("doc_parent_chain_depth_{}", doc_fixture.depth),
        config.doc_iters,
        10,
        json!({
            "node_count": doc_fixture.node_count,
            "chunk_count": doc_fixture.chunk_count,
            "depth": doc_fixture.depth,
            "chain_length": parent_chain_depth,
        }),
        || {
            Ok(doc_fixture
                .db
                .doc_parent_chain("bench", &doc_fixture.deep_chunk_id)?)
        },
    )?);

    let vector_build_iters = 3;
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "vector_build_{}_embeddings_dim{}",
            config.vector_medium_count, config.vector_dims
        ),
        vector_build_iters,
        0,
        json!({
            "embedding_count": config.vector_medium_count,
            "dims": config.vector_dims,
        }),
        || build_vector_fixture(config.vector_medium_count, config.vector_dims),
    )?);

    let vector_medium = build_vector_fixture(config.vector_medium_count, config.vector_dims)?;
    let medium_hits = vector_medium
        .db
        .search_node_embeddings(
            &vector_medium.partition_name,
            "bench",
            &vector_medium.query_vector,
            10,
            Some("benchmark"),
            false,
        )?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "vector_search_{}_embeddings_dim{}",
            vector_medium.chunk_count, vector_medium.dims
        ),
        config.vector_medium_iters,
        10,
        json!({
            "embedding_count": vector_medium.chunk_count,
            "dims": vector_medium.dims,
            "limit": 10,
            "result_count": medium_hits,
        }),
        || {
            Ok(vector_medium.db.search_node_embeddings(
                &vector_medium.partition_name,
                "bench",
                &vector_medium.query_vector,
                10,
                Some("benchmark"),
                false,
            )?)
        },
    )?);

    let vector_large = build_vector_fixture(config.vector_large_count, config.vector_dims)?;
    let large_hits = vector_large
        .db
        .search_node_embeddings(
            &vector_large.partition_name,
            "bench",
            &vector_large.query_vector,
            10,
            Some("benchmark"),
            false,
        )?
        .len();
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "vector_search_{}_embeddings_dim{}",
            vector_large.chunk_count, vector_large.dims
        ),
        config.vector_large_iters,
        5,
        json!({
            "embedding_count": vector_large.chunk_count,
            "dims": vector_large.dims,
            "limit": 10,
            "result_count": large_hits,
        }),
        || {
            Ok(vector_large.db.search_node_embeddings(
                &vector_large.partition_name,
                "bench",
                &vector_large.query_vector,
                10,
                Some("benchmark"),
                false,
            )?)
        },
    )?);

    let snapshot_fixture = build_snapshot_fixture(
        config.snapshot_entities,
        config.edges_per_node / 2,
        config.snapshot_doc_count,
        config.snapshot_chunks_per_doc,
        config.vector_dims,
    )?;
    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "snapshot_export_{}_entities_{}_docs_{}_chunks",
            snapshot_fixture.entity_count, snapshot_fixture.doc_count, snapshot_fixture.chunk_count
        ),
        config.snapshot_iters,
        1,
        json!({
            "entity_count": snapshot_fixture.entity_count,
            "rel_count": snapshot_fixture.rel_count,
            "doc_count": snapshot_fixture.doc_count,
            "chunk_count": snapshot_fixture.chunk_count,
            "embedding_count": snapshot_fixture.embedding_count,
            "snapshot_bytes": snapshot_fixture.snapshot_bytes.len(),
        }),
        || Ok(snapshot_fixture.db.export_snapshot_bytes()?),
    )?);

    cases.push(benchmark_sync_case(
        "kernel",
        format!(
            "snapshot_import_{}_entities_{}_docs_{}_chunks",
            snapshot_fixture.entity_count, snapshot_fixture.doc_count, snapshot_fixture.chunk_count
        ),
        config.snapshot_iters,
        1,
        json!({
            "entity_count": snapshot_fixture.entity_count,
            "rel_count": snapshot_fixture.rel_count,
            "doc_count": snapshot_fixture.doc_count,
            "chunk_count": snapshot_fixture.chunk_count,
            "embedding_count": snapshot_fixture.embedding_count,
            "snapshot_bytes": snapshot_fixture.snapshot_bytes.len(),
        }),
        || {
            Ok(ElephantDb::from_snapshot_bytes(
                &snapshot_fixture.snapshot_bytes,
            )?)
        },
    )?);

    Ok(cases)
}

fn run_concurrency_suite(config: ScaleConfig) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let fixture = Arc::new(build_concurrency_fixture(config)?);

    let (read_iters_single, read_iters_contended, write_iters_single, write_iters_contended) =
        match config.graph_small_nodes {
            2_000 => (400, 200, 120, 60),
            _ => (1_000, 400, 240, 120),
        };

    for &(workers, iterations) in &[(1_usize, read_iters_single), (8, read_iters_contended)] {
        let run = run_read_concurrency(fixture.clone(), workers, iterations)?;
        cases.push(one_shot_case(
            "concurrency",
            format!("elephant_db_read_workers_{workers}_iters_{iterations}"),
            run.elapsed,
            json!({
                "workers": workers,
                "iterations_per_worker": iterations,
                "logical_ops": run.logical_ops,
                "internal_calls_per_op": 4,
                "ops_per_sec": run.ops_per_sec,
                "mean_thread_ms": run.mean_thread_ms,
                "max_thread_ms": run.max_thread_ms,
            }),
        ));
    }

    for &(workers, iterations) in &[(1_usize, write_iters_single), (4, write_iters_contended)] {
        let run = run_simple_write_concurrency(fixture.clone(), workers, iterations)?;
        cases.push(one_shot_case(
            "concurrency",
            format!("elephant_db_write_simple_workers_{workers}_iters_{iterations}"),
            run.elapsed,
            json!({
                "workers": workers,
                "iterations_per_worker": iterations,
                "logical_ops": run.logical_ops,
                "internal_calls_per_op": 2,
                "ops_per_sec": run.ops_per_sec,
                "mean_thread_ms": run.mean_thread_ms,
                "max_thread_ms": run.max_thread_ms,
            }),
        ));
    }

    for &(workers, iterations) in &[
        (1_usize, write_iters_single / 2),
        (4, write_iters_contended / 2),
    ] {
        let run = run_tx_write_concurrency(fixture.clone(), workers, iterations)?;
        cases.push(one_shot_case(
            "concurrency",
            format!("elephant_db_write_tx_workers_{workers}_iters_{iterations}"),
            run.elapsed,
            json!({
                "workers": workers,
                "iterations_per_worker": iterations,
                "logical_ops": run.logical_ops,
                "ops_per_sec": run.ops_per_sec,
                "mean_thread_ms": run.mean_thread_ms,
                "max_thread_ms": run.max_thread_ms,
            }),
        ));
    }

    let mixed = run_mixed_concurrency(fixture, 8, 4, read_iters_contended, write_iters_contended)?;
    cases.push(one_shot_case(
        "concurrency",
        format!(
            "elephant_db_mixed_readers_{}_writers_{}_read_iters_{}_write_iters_{}",
            8, 4, read_iters_contended, write_iters_contended
        ),
        mixed.elapsed,
        json!({
            "reader_workers": 8,
            "writer_workers": 4,
            "read_iterations_per_worker": read_iters_contended,
            "write_iterations_per_worker": write_iters_contended,
            "logical_ops": mixed.logical_ops,
            "ops_per_sec": mixed.ops_per_sec,
            "mean_thread_ms": mixed.mean_thread_ms,
            "max_thread_ms": mixed.max_thread_ms,
        }),
    ));

    Ok(cases)
}

async fn run_pipeline_concurrency_suite(selection: BenchmarkBackend) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let run = run_pipeline_overlap_once(backend).await?;
        cases.push(one_shot_case(
            "pipeline-concurrency",
            format!("{label}_two_ingests_two_searchers_overlap"),
            run.total,
            json!({
                "backend": label,
                "doc_runs": run.docs,
                "search_count": run.search_count,
                "non_empty_search_count": run.non_empty_search_count,
                "non_empty_rate": if run.search_count == 0 {
                    0.0
                } else {
                    usize_to_f64(run.non_empty_search_count) / usize_to_f64(run.search_count)
                },
                "search_latency_ms": {
                    "total": run.search_metrics.total_ms,
                    "mean": run.search_metrics.mean_ms,
                    "median": run.search_metrics.median_ms,
                    "p95": run.search_metrics.p95_ms,
                    "max": run.search_metrics.max_ms,
                    "ops_per_sec": run.search_metrics.ops_per_sec,
                },
            }),
        ));
    }
    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark fixture ownership and drop order through measurements and reporting"
)]
async fn run_compare_suite(
    config: ScaleConfig,
    selection: BenchmarkBackend,
) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);

        let started = Instant::now();
        let graph_fixture =
            build_backend_graph_fixture(backend, config.compare_graph_nodes, config.edges_per_node)
                .await?;
        let graph_seed = started.elapsed();
        let graph_repo = RelRepository::new(graph_fixture.client.clone());
        let graph_h2_hits = graph_repo
            .get_entities_within_n_hops("bench", &graph_fixture.start_id, 2)
            .await?
            .len();
        cases.push(one_shot_case(
            "compare",
            format!(
                "{label}_graph_seed_{}_nodes_{}_edges",
                graph_fixture.node_count, graph_fixture.edge_count
            ),
            graph_seed,
            json!({
                "backend": label,
                "node_count": graph_fixture.node_count,
                "edge_count": graph_fixture.edge_count,
            }),
        ));

        let graph_h2_repo = graph_repo.clone();
        let graph_h2_start = graph_fixture.start_id.clone();
        cases.push(
            benchmark_async_case(
                "compare",
                format!(
                    "{label}_graph_query_h2_{}_nodes_{}_edges",
                    graph_fixture.node_count, graph_fixture.edge_count
                ),
                config.compare_graph_iters,
                5,
                json!({
                    "backend": label,
                    "node_count": graph_fixture.node_count,
                    "edge_count": graph_fixture.edge_count,
                    "hops": 2,
                    "result_count": graph_h2_hits,
                }),
                move || {
                    let repo = graph_h2_repo.clone();
                    let start = graph_h2_start.clone();
                    async move { Ok(repo.get_entities_within_n_hops("bench", &start, 2).await?) }
                },
            )
            .await?,
        );

        let graph_h3_repo = graph_repo.clone();
        let graph_h3_start = graph_fixture.start_id.clone();
        let graph_h3_hits = graph_repo
            .get_entities_within_n_hops("bench", &graph_fixture.start_id, 3)
            .await?
            .len();
        cases.push(
            benchmark_async_case(
                "compare",
                format!(
                    "{label}_graph_query_h3_{}_nodes_{}_edges",
                    graph_fixture.node_count, graph_fixture.edge_count
                ),
                usize::max(1, config.compare_graph_iters / 2),
                3,
                json!({
                    "backend": label,
                    "node_count": graph_fixture.node_count,
                    "edge_count": graph_fixture.edge_count,
                    "hops": 3,
                    "result_count": graph_h3_hits,
                }),
                move || {
                    let repo = graph_h3_repo.clone();
                    let start = graph_h3_start.clone();
                    async move { Ok(repo.get_entities_within_n_hops("bench", &start, 3).await?) }
                },
            )
            .await?,
        );

        let started = Instant::now();
        let doc_fixture = build_backend_doc_fixture(backend, config.compare_doc_chunks).await?;
        let doc_seed = started.elapsed();
        let node_repo = DocNodeRepository::new(doc_fixture.client.clone());
        let chunk_results = node_repo
            .find_chunks_by_revision(&doc_fixture.revision_id)
            .await?
            .len();
        let chain_results = node_repo
            .get_parent_chain(&doc_fixture.deep_chunk_id)
            .await?
            .len();
        cases.push(one_shot_case(
            "compare",
            format!("{label}_doc_tree_seed_{}_chunks", doc_fixture.chunk_count),
            doc_seed,
            json!({
                "backend": label,
                "node_count": doc_fixture.node_count,
                "chunk_count": doc_fixture.chunk_count,
            }),
        ));

        let doc_chunks_repo = node_repo.clone();
        let doc_chunks_revision = doc_fixture.revision_id.clone();
        cases.push(
            benchmark_async_case(
                "compare",
                format!(
                    "{label}_doc_chunks_lookup_{}_chunks",
                    doc_fixture.chunk_count
                ),
                config.compare_doc_iters,
                5,
                json!({
                    "backend": label,
                    "node_count": doc_fixture.node_count,
                    "chunk_count": doc_fixture.chunk_count,
                    "result_count": chunk_results,
                }),
                move || {
                    let repo = doc_chunks_repo.clone();
                    let revision_id = doc_chunks_revision.clone();
                    async move { Ok(repo.find_chunks_by_revision(&revision_id).await?) }
                },
            )
            .await?,
        );

        let parent_chain_repo = node_repo.clone();
        let deep_chunk_id = doc_fixture.deep_chunk_id.clone();
        cases.push(
            benchmark_async_case(
                "compare",
                format!("{label}_doc_parent_chain_depth_{}", doc_fixture.depth),
                config.compare_doc_iters,
                5,
                json!({
                    "backend": label,
                    "node_count": doc_fixture.node_count,
                    "chunk_count": doc_fixture.chunk_count,
                    "depth": doc_fixture.depth,
                    "chain_length": chain_results,
                }),
                move || {
                    let repo = parent_chain_repo.clone();
                    let node_id = deep_chunk_id.clone();
                    async move { Ok(repo.get_parent_chain(&node_id).await?) }
                },
            )
            .await?,
        );
    }

    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark fixture ownership and drop order through measurements and reporting"
)]
async fn run_scenario_suite(
    config: ScaleConfig,
    selection: BenchmarkBackend,
) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let alice_total = benchmark_async_case(
            "scenario",
            format!("{label}_alice_pipeline_total"),
            config.alice_total_iters,
            0,
            json!({
                "backend": label,
                "iterations_rebuild_fixture": true,
            }),
            move || async move {
                let run = run_alice_pipeline_once(backend).await?;
                Ok(run.total)
            },
        )
        .await?;
        cases.push(alice_total);

        let single_run = run_alice_pipeline_once(backend).await?;
        cases.push(one_shot_case(
            "scenario",
            format!("{label}_alice_ingest_two_docs"),
            single_run.ingest_two_docs,
            json!({
                "backend": label,
                "doc_count": single_run.doc_count,
            }),
        ));
        cases.push(one_shot_case(
            "scenario",
            format!("{label}_alice_render_docs"),
            single_run.render_docs,
            json!({
                "backend": label,
                "doc_count": single_run.doc_count,
            }),
        ));
        cases.push(one_shot_case(
            "scenario",
            format!("{label}_alice_build_doc_trees"),
            single_run.build_doc_trees,
            json!({
                "backend": label,
                "doc_count": single_run.doc_count,
                "chunk_count": single_run.chunk_count,
            }),
        ));
        cases.push(one_shot_case(
            "scenario",
            format!("{label}_alice_embed_chunks"),
            single_run.embed_chunks,
            json!({
                "backend": label,
                "chunk_count": single_run.chunk_count,
            }),
        ));
        cases.push(one_shot_case(
            "scenario",
            format!("{label}_alice_search_single"),
            single_run.search_once,
            json!({
                "backend": label,
                "chunk_count": single_run.chunk_count,
                "search_query": "white rabbit rabbit hole wonderland",
                "search_result_count": single_run.search_result_count,
                "observed_top_title": single_run.observed_top_title,
                "expected_top_title": "Alice's Adventures in Wonderland",
            }),
        ));

        let prepared = prepare_alice_fixture(backend).await?;
        let search_results = prepared.search_result_count;
        let dispatcher = prepared.dispatcher;
        let query = prepared.search_query.clone();
        let expected_top_title = prepared.expected_top_title.clone();
        let observed_top_title = prepared.observed_top_title.clone();
        cases.push(
            benchmark_async_case(
                "scenario",
                format!("{label}_alice_search_repeated"),
                config.alice_search_iters,
                3,
                json!({
                    "backend": label,
                    "doc_count": prepared.doc_count,
                    "chunk_count": prepared.chunk_count,
                    "search_query": query.clone(),
                    "expected_top_title": expected_top_title.clone(),
                    "observed_top_title": observed_top_title.clone(),
                    "result_count": search_results,
                }),
                move || {
                    let dispatcher = dispatcher.clone();
                    let query = query.clone();
                    let expected_top_title = expected_top_title.clone();
                    async move {
                        let titles = search_titles(&dispatcher, &query, 5).await?;
                        if let Some(title) = titles.first() {
                            if title != &expected_top_title {
                                eprintln!(
                                    "unexpected top title during repeated search benchmark: expected `{expected_top_title}`, got `{title}`"
                                );
                            }
                        } else {
                            eprintln!(
                                "search_docs returned no titles during repeated search benchmark"
                            );
                        }
                        Ok(titles)
                    }
                },
            )
            .await?,
        );
    }

    Ok(cases)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark fixture ownership and drop order through measurements and reporting"
)]
async fn run_quality_suite(selection: BenchmarkBackend) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let mut backend_results: HashMap<(String, String), Vec<QueryQualityResult>> = HashMap::new();
    let expectations = quality_queries();

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let fixture = prepare_quality_fixture(backend).await?;
        for mode in [SearchMode::Semantic, SearchMode::RawText] {
            let started = Instant::now();
            let results = evaluate_quality_fixture(&fixture, &expectations, mode).await?;
            let evaluation_duration = started.elapsed();
            let top1_accuracy = quality_top1_accuracy(&results);
            let mean_reciprocal_rank = quality_mean_reciprocal_rank(&results);
            let non_empty_result_rate = quality_non_empty_result_rate(&results);
            let search_latency = quality_latency_metrics(&results);

            cases.push(one_shot_case(
                "quality",
                format!("{label}_{}_expected_top1_quality", mode.label()),
                evaluation_duration,
                json!({
                    "backend": label,
                    "search_mode": mode.label(),
                    "query_count": results.len(),
                    "top1_accuracy": top1_accuracy,
                    "mean_reciprocal_rank": mean_reciprocal_rank,
                    "non_empty_result_rate": non_empty_result_rate,
                    "search_latency": search_latency,
                    "per_query": &results,
                }),
            ));
            let _ = backend_results.insert((label.to_string(), mode.label().to_string()), results);
        }
    }

    for mode in [SearchMode::Semantic, SearchMode::RawText] {
        let elephant_key = (
            backend_label(StorageBackendKind::ElephantDb).to_string(),
            mode.label().to_string(),
        );
        let surreal_key = (
            backend_label(StorageBackendKind::SurrealDb).to_string(),
            mode.label().to_string(),
        );
        if let (Some(elephant), Some(surreal)) = (
            backend_results.get(&elephant_key),
            backend_results.get(&surreal_key),
        ) {
            cases.push(one_shot_case(
                "quality",
                format!("{}_backend_quality_agreement", mode.label()),
                Duration::ZERO,
                quality_agreement_details(elephant, surreal),
            ));
        }
    }

    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark fixture ownership and drop order through measurements and reporting"
)]
async fn run_transcript_suite(
    config: ScaleConfig,
    selection: BenchmarkBackend,
) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let scenarios = build_transcript_scenarios(config)?;
    let mut backend_results: HashMap<(String, String, String, String), Vec<QueryQualityResult>> =
        HashMap::new();
    let mut overall_backend_results: HashMap<(String, String), Vec<QueryQualityResult>> =
        HashMap::new();

    for scenario in &scenarios {
        let corpus: Vec<(String, String, String)> = scenario
            .docs
            .iter()
            .map(|doc| (doc.title.clone(), doc.text.clone(), doc.collection.clone()))
            .collect();
        let source_counts = transcript_source_counts(&scenario.docs);
        let sample_files = transcript_sample_files(&scenario.docs, 6);
        let total_text_bytes: usize = scenario.docs.iter().map(|doc| doc.text.len()).sum();
        let project_counts = transcript_project_counts(&scenario.docs);
        let tool_event_total: usize = scenario.docs.iter().map(|doc| doc.tool_event_count).sum();
        let tool_event_mean = if scenario.docs.is_empty() {
            0.0
        } else {
            usize_to_f64(tool_event_total) / usize_to_f64(scenario.docs.len())
        };

        for backend in selected_storage_backends(selection) {
            let label = backend_label(backend);
            let started = Instant::now();
            let fixture = prepare_search_fixture(backend, &corpus).await?;
            let build_duration = started.elapsed();

            cases.push(one_shot_case(
                "transcripts",
                format!("{label}_{}_build_search_fixture", scenario.name),
                build_duration,
                json!({
                    "backend": label,
                    "scenario": scenario.name,
                    "doc_count": fixture.doc_count,
                    "chunk_count": fixture.chunk_count,
                    "total_text_bytes": total_text_bytes,
                    "source_counts": &source_counts,
                    "project_counts": &project_counts,
                    "tool_event_total": tool_event_total,
                    "tool_event_mean_per_doc": tool_event_mean,
                    "sample_files": &sample_files,
                }),
            ));

            for strategy in [
                TranscriptQueryStrategy::FirstUser,
                TranscriptQueryStrategy::LastUser,
                TranscriptQueryStrategy::SessionSalience,
            ] {
                let expectations = build_transcript_expectations(
                    &scenario.docs,
                    strategy,
                    config.transcript_query_terms,
                );
                if expectations.is_empty() {
                    continue;
                }

                for mode in [SearchMode::Semantic, SearchMode::RawText] {
                    let started = Instant::now();
                    let results = evaluate_quality_fixture(&fixture, &expectations, mode).await?;
                    let evaluation_duration = started.elapsed();
                    let top1_accuracy = quality_top1_accuracy(&results);
                    let top3_hit_rate = quality_topk_hit_rate(&results, 3);
                    let mean_reciprocal_rank = quality_mean_reciprocal_rank(&results);
                    let non_empty_result_rate = quality_non_empty_result_rate(&results);
                    let search_latency = quality_latency_metrics(&results);

                    cases.push(one_shot_case(
                        "transcripts",
                        format!(
                            "{label}_{}_{}_{}_quality",
                            scenario.name,
                            strategy.label(),
                            mode.label()
                        ),
                        evaluation_duration,
                        json!({
                            "backend": label,
                            "scenario": scenario.name,
                            "query_kind": strategy.label(),
                            "search_mode": mode.label(),
                            "doc_count": fixture.doc_count,
                            "chunk_count": fixture.chunk_count,
                            "query_count": results.len(),
                            "total_text_bytes": total_text_bytes,
                            "source_counts": &source_counts,
                            "project_counts": &project_counts,
                            "tool_event_total": tool_event_total,
                            "tool_event_mean_per_doc": tool_event_mean,
                            "top1_accuracy": top1_accuracy,
                            "top3_hit_rate": top3_hit_rate,
                            "mean_reciprocal_rank": mean_reciprocal_rank,
                            "non_empty_result_rate": non_empty_result_rate,
                            "search_latency": search_latency,
                            "sample_files": &sample_files,
                            "per_query": &results,
                        }),
                    ));

                    if selection == BenchmarkBackend::Both {
                        let _ = backend_results.insert(
                            (
                                scenario.name.to_string(),
                                strategy.label().to_string(),
                                mode.label().to_string(),
                                label.to_string(),
                            ),
                            results,
                        );
                    } else {
                        overall_backend_results
                            .entry((label.to_string(), mode.label().to_string()))
                            .or_default()
                            .extend(results);
                    }
                }
            }
        }

        if selection == BenchmarkBackend::Both {
            for strategy in [
                TranscriptQueryStrategy::FirstUser,
                TranscriptQueryStrategy::LastUser,
                TranscriptQueryStrategy::SessionSalience,
            ] {
                for mode in [SearchMode::Semantic, SearchMode::RawText] {
                    let scenario_key = scenario.name.to_string();
                    let strategy_key = strategy.label().to_string();
                    let elephant_key = (
                        scenario_key.clone(),
                        strategy_key.clone(),
                        mode.label().to_string(),
                        backend_label(StorageBackendKind::ElephantDb).to_string(),
                    );
                    let surreal_key = (
                        scenario_key.clone(),
                        strategy_key.clone(),
                        mode.label().to_string(),
                        backend_label(StorageBackendKind::SurrealDb).to_string(),
                    );
                    if let (Some(elephant), Some(surreal)) = (
                        backend_results.get(&elephant_key),
                        backend_results.get(&surreal_key),
                    ) {
                        cases.push(one_shot_case(
                            "transcripts",
                            format!(
                                "{}_{}_{}_backend_agreement",
                                scenario.name,
                                strategy.label(),
                                mode.label()
                            ),
                            Duration::ZERO,
                            quality_agreement_details(elephant, surreal),
                        ));
                    }
                }
            }
        }
    }

    if selection == BenchmarkBackend::Both {
        for backend in [
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ] {
            let label = backend_label(backend).to_string();
            for mode in [SearchMode::Semantic, SearchMode::RawText] {
                let mut combined_results = Vec::new();
                for scenario in &scenarios {
                    for strategy in [
                        TranscriptQueryStrategy::FirstUser,
                        TranscriptQueryStrategy::LastUser,
                        TranscriptQueryStrategy::SessionSalience,
                    ] {
                        let key = (
                            scenario.name.to_string(),
                            strategy.label().to_string(),
                            mode.label().to_string(),
                            label.clone(),
                        );
                        if let Some(results) = backend_results.get(&key) {
                            combined_results.extend(results.iter().cloned());
                        }
                    }
                }

                if !combined_results.is_empty() {
                    cases.push(one_shot_case(
                        "transcripts",
                        format!("{label}_{}_overall_quality", mode.label()),
                        Duration::ZERO,
                        json!({
                            "backend": label,
                            "search_mode": mode.label(),
                            "query_count": combined_results.len(),
                            "top1_accuracy": quality_top1_accuracy(&combined_results),
                            "top3_hit_rate": quality_topk_hit_rate(&combined_results, 3),
                            "mean_reciprocal_rank": quality_mean_reciprocal_rank(&combined_results),
                            "non_empty_result_rate": quality_non_empty_result_rate(&combined_results),
                            "search_latency": quality_latency_metrics(&combined_results),
                        }),
                    ));
                }
            }
        }
    } else {
        for ((label, mode), combined_results) in overall_backend_results {
            if !combined_results.is_empty() {
                cases.push(one_shot_case(
                    "transcripts",
                    format!("{label}_{mode}_overall_quality"),
                    Duration::ZERO,
                    json!({
                        "backend": label,
                        "search_mode": mode,
                        "query_count": combined_results.len(),
                        "top1_accuracy": quality_top1_accuracy(&combined_results),
                        "top3_hit_rate": quality_topk_hit_rate(&combined_results, 3),
                        "mean_reciprocal_rank": quality_mean_reciprocal_rank(&combined_results),
                        "non_empty_result_rate": quality_non_empty_result_rate(&combined_results),
                        "search_latency": quality_latency_metrics(&combined_results),
                    }),
                ));
            }
        }
    }

    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_alice_qa_suite(selection: BenchmarkBackend) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let started = Instant::now();
    let artifact = extract_alice_graph_artifact().await?;
    let extraction_duration = started.elapsed();
    cases.push(one_shot_case(
        "alice-qa",
        "alice_extract_once".to_string(),
        extraction_duration,
        json!({
            "backend": "shared",
            "doc_count": artifact.docs.len(),
            "entity_count": artifact.entities.len(),
            "assertion_count": artifact.assertions.len(),
            "rel_count": artifact.rels.len(),
        }),
    ));

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let started = Instant::now();
        let fixture = replay_alice_graph_artifact(&artifact, backend).await?;
        let setup_duration = started.elapsed();
        cases.push(one_shot_case(
            "alice-qa",
            format!("{label}_alice_graph_replay"),
            setup_duration,
            json!({
                "backend": label,
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
            }),
        ));

        let started = Instant::now();
        let mut results = Vec::new();
        for question in alice_questions() {
            results.push(evaluate_alice_question(&fixture, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        cases.push(one_shot_case(
            "alice-qa",
            format!("{label}_alice_fiction_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": results.len(),
                "answer_accuracy": alice_answer_accuracy(&results),
                "search_latency": alice_latency_metrics(&results),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": results,
            }),
        ));

        let started = Instant::now();
        let mut agentic_results = Vec::new();
        let mut agent = Box::pin(build_alice_graph_qa_agent(&fixture.client)).await?;
        for question in alice_questions() {
            agentic_results.push(evaluate_alice_question_with_agent(&mut agent, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        let turns: u32 = agentic_results.iter().map(|result| result.turns).sum();
        let tool_calls: u32 = agentic_results.iter().map(|result| result.tool_calls).sum();
        let tool_events: u32 = agentic_results
            .iter()
            .map(|result| result.tool_events)
            .sum();
        let turn_events: u32 = agentic_results
            .iter()
            .map(|result| result.turn_events)
            .sum();
        let tool_ms: f64 = agentic_results.iter().map(|result| result.tool_ms).sum();
        let estimated_model_ms: f64 = agentic_results
            .iter()
            .map(|result| result.estimated_model_ms)
            .sum();
        let input_tokens: usize = agentic_results
            .iter()
            .map(|result| result.input_tokens)
            .sum();
        let output_tokens: usize = agentic_results
            .iter()
            .map(|result| result.output_tokens)
            .sum();
        let total_tokens: usize = agentic_results
            .iter()
            .map(|result| result.total_tokens)
            .sum();
        cases.push(one_shot_case(
            "alice-qa",
            format!("{label}_alice_fiction_agent_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": agentic_results.len(),
                "answer_accuracy": alice_agent_answer_accuracy(&agentic_results),
                "answer_latency": alice_agent_latency_metrics(&agentic_results),
                "turns": turns,
                "tool_calls": tool_calls,
                "tool_events": tool_events,
                "turn_events": turn_events,
                "tool_ms": tool_ms,
                "estimated_model_ms": estimated_model_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "per_question_avg_ms": evaluation_duration.as_secs_f64() * 1000.0 / usize_to_f64(agentic_results.len()),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": agentic_results,
            }),
        ));
    }

    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_oz_qa_suite(selection: BenchmarkBackend) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let started = Instant::now();
    let artifact = extract_oz_graph_artifact().await?;
    let extraction_duration = started.elapsed();
    cases.push(one_shot_case(
        "oz-qa",
        "oz_extract_once".to_string(),
        extraction_duration,
        json!({
            "backend": "shared",
            "doc_count": artifact.docs.len(),
            "entity_count": artifact.entities.len(),
            "assertion_count": artifact.assertions.len(),
            "rel_count": artifact.rels.len(),
        }),
    ));

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let started = Instant::now();
        let fixture = replay_alice_graph_artifact(&artifact, backend).await?;
        let setup_duration = started.elapsed();
        cases.push(one_shot_case(
            "oz-qa",
            format!("{label}_oz_graph_replay"),
            setup_duration,
            json!({
                "backend": label,
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
            }),
        ));

        let started = Instant::now();
        let mut results = Vec::new();
        for question in oz_questions() {
            results.push(evaluate_alice_question(&fixture, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        cases.push(one_shot_case(
            "oz-qa",
            format!("{label}_oz_fiction_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": results.len(),
                "answer_accuracy": alice_answer_accuracy(&results),
                "search_latency": alice_latency_metrics(&results),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": results,
            }),
        ));

        let started = Instant::now();
        let mut agentic_results = Vec::new();
        let mut agent = Box::pin(build_alice_graph_qa_agent(&fixture.client)).await?;
        for question in oz_questions() {
            agentic_results.push(evaluate_alice_question_with_agent(&mut agent, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        let turns: u32 = agentic_results.iter().map(|result| result.turns).sum();
        let tool_calls: u32 = agentic_results.iter().map(|result| result.tool_calls).sum();
        let tool_events: u32 = agentic_results
            .iter()
            .map(|result| result.tool_events)
            .sum();
        let turn_events: u32 = agentic_results
            .iter()
            .map(|result| result.turn_events)
            .sum();
        let tool_ms: f64 = agentic_results.iter().map(|result| result.tool_ms).sum();
        let estimated_model_ms: f64 = agentic_results
            .iter()
            .map(|result| result.estimated_model_ms)
            .sum();
        let input_tokens: usize = agentic_results
            .iter()
            .map(|result| result.input_tokens)
            .sum();
        let output_tokens: usize = agentic_results
            .iter()
            .map(|result| result.output_tokens)
            .sum();
        let total_tokens: usize = agentic_results
            .iter()
            .map(|result| result.total_tokens)
            .sum();
        cases.push(one_shot_case(
            "oz-qa",
            format!("{label}_oz_fiction_agent_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": agentic_results.len(),
                "answer_accuracy": alice_agent_answer_accuracy(&agentic_results),
                "answer_latency": alice_agent_latency_metrics(&agentic_results),
                "turns": turns,
                "tool_calls": tool_calls,
                "tool_events": tool_events,
                "turn_events": turn_events,
                "tool_ms": tool_ms,
                "estimated_model_ms": estimated_model_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "per_question_avg_ms": evaluation_duration.as_secs_f64() * 1000.0 / usize_to_f64(agentic_results.len()),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": agentic_results,
            }),
        ));
    }

    Ok(cases)
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_elephant_session_qa_suite(selection: BenchmarkBackend) -> Result<Vec<BenchmarkCase>> {
    let mut cases = Vec::new();
    let started = Instant::now();
    let artifact = extract_elephant_session_graph_artifact().await?;
    let extraction_duration = started.elapsed();
    cases.push(one_shot_case(
        "session-qa",
        "elephant_session_extract_once".to_string(),
        extraction_duration,
        json!({
            "backend": "shared",
            "doc_count": artifact.docs.len(),
            "entity_count": artifact.entities.len(),
            "assertion_count": artifact.assertions.len(),
            "rel_count": artifact.rels.len(),
        }),
    ));

    for backend in selected_storage_backends(selection) {
        let label = backend_label(backend);
        let started = Instant::now();
        let fixture = replay_alice_graph_artifact(&artifact, backend).await?;
        let setup_duration = started.elapsed();
        cases.push(one_shot_case(
            "session-qa",
            format!("{label}_elephant_session_graph_replay"),
            setup_duration,
            json!({
                "backend": label,
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
            }),
        ));

        let started = Instant::now();
        let mut results = Vec::new();
        for question in elephant_session_questions() {
            results.push(evaluate_alice_question(&fixture, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        cases.push(one_shot_case(
            "session-qa",
            format!("{label}_elephant_session_graph_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": results.len(),
                "answer_accuracy": alice_answer_accuracy(&results),
                "search_latency": alice_latency_metrics(&results),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": results,
            }),
        ));

        let started = Instant::now();
        let mut agentic_results = Vec::new();
        let mut agent = Box::pin(build_alice_graph_qa_agent(&fixture.client)).await?;
        for question in elephant_session_questions() {
            agentic_results.push(evaluate_alice_question_with_agent(&mut agent, &question).await?);
        }
        let evaluation_duration = started.elapsed();
        let turns: u32 = agentic_results.iter().map(|result| result.turns).sum();
        let tool_calls: u32 = agentic_results.iter().map(|result| result.tool_calls).sum();
        let tool_events: u32 = agentic_results
            .iter()
            .map(|result| result.tool_events)
            .sum();
        let turn_events: u32 = agentic_results
            .iter()
            .map(|result| result.turn_events)
            .sum();
        let tool_ms: f64 = agentic_results.iter().map(|result| result.tool_ms).sum();
        let estimated_model_ms: f64 = agentic_results
            .iter()
            .map(|result| result.estimated_model_ms)
            .sum();
        let input_tokens: usize = agentic_results
            .iter()
            .map(|result| result.input_tokens)
            .sum();
        let output_tokens: usize = agentic_results
            .iter()
            .map(|result| result.output_tokens)
            .sum();
        let total_tokens: usize = agentic_results
            .iter()
            .map(|result| result.total_tokens)
            .sum();
        cases.push(one_shot_case(
            "session-qa",
            format!("{label}_elephant_session_agent_question_quality"),
            evaluation_duration,
            json!({
                "backend": label,
                "question_count": agentic_results.len(),
                "answer_accuracy": alice_agent_answer_accuracy(&agentic_results),
                "answer_latency": alice_agent_latency_metrics(&agentic_results),
                "turns": turns,
                "tool_calls": tool_calls,
                "tool_events": tool_events,
                "turn_events": turn_events,
                "tool_ms": tool_ms,
                "estimated_model_ms": estimated_model_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "per_question_avg_ms": evaluation_duration.as_secs_f64() * 1000.0 / usize_to_f64(agentic_results.len()),
                "doc_count": fixture.doc_count,
                "entity_count": fixture.entity_count,
                "assertion_count": fixture.assertion_count,
                "rel_count": fixture.rel_count,
                "per_question": agentic_results,
            }),
        ));
    }

    Ok(cases)
}

fn alice_answer_accuracy(results: &[AliceQaResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }

    let correct = results.iter().filter(|result| result.correct).count();
    usize_to_f64(correct) / usize_to_f64(results.len())
}

fn alice_latency_metrics(results: &[AliceQaResult]) -> BenchmarkMetrics {
    let samples: Vec<f64> = results.iter().map(|result| result.search_ms).collect();
    summarize_ms_samples(&samples)
}

fn normalized_answer_text(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn alice_agent_answer_accuracy(results: &[AliceAgentQaResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }

    let correct = results.iter().filter(|result| result.correct).count();
    usize_to_f64(correct) / usize_to_f64(results.len())
}

fn alice_agent_latency_metrics(results: &[AliceAgentQaResult]) -> BenchmarkMetrics {
    let samples: Vec<f64> = results.iter().map(|result| result.answer_ms).collect();
    summarize_ms_samples(&samples)
}

fn alice_questions() -> Vec<AliceQuestion> {
    vec![
        AliceQuestion {
            category: "general",
            question: "What strange world does Alice enter after following the rabbit?",
            expected_answers: &["Wonderland"],
        },
        AliceQuestion {
            category: "general",
            question: "Which animal does Alice follow at the beginning of the story?",
            expected_answers: &["White Rabbit"],
        },
        AliceQuestion {
            category: "specific",
            question: "What is the name of Alice's cat?",
            expected_answers: &["Dinah"],
        },
        AliceQuestion {
            category: "specific",
            question: "What does Alice fall down to get into Wonderland?",
            expected_answers: &["rabbit hole"],
        },
        AliceQuestion {
            category: "specific",
            question: "Which queen keeps ordering executions?",
            expected_answers: &["Queen of Hearts"],
        },
        AliceQuestion {
            category: "specific",
            question: "What game does Alice play with the Queen of Hearts?",
            expected_answers: &["croquet"],
        },
        AliceQuestion {
            category: "specific",
            question: "Which character can disappear and leave only its grin behind?",
            expected_answers: &["Cheshire Cat"],
        },
        AliceQuestion {
            category: "specific",
            question: "Who asks Alice 'Who are you?' while sitting on a mushroom?",
            expected_answers: &["Caterpillar"],
        },
        AliceQuestion {
            category: "specific",
            question: "At whose place is the tea party held?",
            expected_answers: &["March Hare"],
        },
        AliceQuestion {
            category: "specific",
            question: "Who is accused of stealing the tarts at the trial?",
            expected_answers: &["Knave of Hearts"],
        },
    ]
}

fn oz_questions() -> Vec<AliceQuestion> {
    vec![
        AliceQuestion {
            category: "general",
            question: "What magical land does Dorothy travel to after the cyclone?",
            expected_answers: &["Oz"],
        },
        AliceQuestion {
            category: "general",
            question: "What is the name of Dorothy's little dog?",
            expected_answers: &["Toto"],
        },
        AliceQuestion {
            category: "specific",
            question: "Which road does Dorothy follow toward the Emerald City?",
            expected_answers: &["Yellow Brick Road"],
        },
        AliceQuestion {
            category: "specific",
            question: "What does the Scarecrow want from the Wizard?",
            expected_answers: &["brain", "brains"],
        },
        AliceQuestion {
            category: "specific",
            question: "What does the Tin Woodman ask the Wizard to give him?",
            expected_answers: &["heart"],
        },
        AliceQuestion {
            category: "specific",
            question: "What does the Cowardly Lion want most?",
            expected_answers: &["courage"],
        },
        AliceQuestion {
            category: "specific",
            question: "Which witch rules the West and becomes Dorothy's enemy?",
            expected_answers: &["Wicked Witch of the West"],
        },
        AliceQuestion {
            category: "specific",
            question: "What city are Dorothy and her friends trying to reach?",
            expected_answers: &["Emerald City"],
        },
        AliceQuestion {
            category: "specific",
            question: "What shoes does Dorothy receive after the first witch dies?",
            expected_answers: &["silver shoes"],
        },
        AliceQuestion {
            category: "specific",
            question: "Who helps Dorothy understand how she can return home?",
            expected_answers: &["Glinda", "Good Witch of the South"],
        },
    ]
}

fn elephant_session_questions() -> Vec<AliceQuestion> {
    vec![
        AliceQuestion {
            category: "architecture",
            question: "What kind of storage backend did the user want instead of relying only on SurrealDB?",
            expected_answers: &["embedded lightweight DIY alternative", "graph storage", "vectordb hybrid"],
        },
        AliceQuestion {
            category: "architecture",
            question: "What did the user insist was still needed after hearing the first simplified elephant-db design?",
            expected_answers: &["proper graph", "proper vectordb", "proper vector db"],
        },
        AliceQuestion {
            category: "wasm",
            question: "What future deployment target did the user mention for Elephant?",
            expected_answers: &["WASM", "wasm"],
        },
        AliceQuestion {
            category: "tooling",
            question: "What Rust build-isolation mechanism did the user explicitly include as the escape hatch for multiple agents sharing one checkout?",
            expected_answers: &["RUST_LANE_ID"],
        },
        AliceQuestion {
            category: "benchmarking",
            question: "What benchmark comparison did the user ask for regarding SurrealDB after asking about the WASM package memory number?",
            expected_answers: &["performance", "footprint", "quality of results"],
        },
        AliceQuestion {
            category: "benchmarking",
            question: "Which two directories did the user want used for session-transcription benchmark scenarios?",
            expected_answers: &["~/.claude", "~/.codex"],
        },
        AliceQuestion {
            category: "qa",
            question: "What kind of QA did the user say was actually interesting instead of chunk-retrieval quality?",
            expected_answers: &["extracted-knowledge-graph QA", "graph QA", "knowledge-graph QA"],
        },
        AliceQuestion {
            category: "meerkat",
            question: "What did the user say was wrong with the early Surreal benchmark setup before demanding it be fixed?",
            expected_answers: &["no surreal db running", "no elephant api docker container", "no elephant-local mcp", "invalid benchmark"],
        },
        AliceQuestion {
            category: "meerkat",
            question: "What workaround did the user suggest so the reusable Meerkat Q&A agent would stay alive between questions?",
            expected_answers: &["host mode", "host-mode"],
        },
        AliceQuestion {
            category: "logs",
            question: "What did the user ultimately demand be processed instead of a written fixture summary?",
            expected_answers: &["full massive raw log", "raw log", "this conversation"],
        },
    ]
}

fn quality_top1_accuracy(results: &[QueryQualityResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }

    let correct = results.iter().filter(|result| result.top1_match).count();
    usize_to_f64(correct) / usize_to_f64(results.len())
}

fn quality_topk_hit_rate(results: &[QueryQualityResult], k: usize) -> f64 {
    if results.is_empty() || k == 0 {
        return 0.0;
    }

    let hits = results
        .iter()
        .filter(|result| {
            result
                .titles
                .iter()
                .take(k)
                .any(|title| title == &result.expected_top_title)
        })
        .count();
    usize_to_f64(hits) / usize_to_f64(results.len())
}

fn quality_mean_reciprocal_rank(results: &[QueryQualityResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }

    let reciprocal_rank_sum: f64 = results.iter().map(|result| result.reciprocal_rank).sum();
    reciprocal_rank_sum / usize_to_f64(results.len())
}

fn quality_non_empty_result_rate(results: &[QueryQualityResult]) -> f64 {
    if results.is_empty() {
        return 0.0;
    }

    let non_empty = results
        .iter()
        .filter(|result| !result.titles.is_empty())
        .count();
    usize_to_f64(non_empty) / usize_to_f64(results.len())
}

fn quality_latency_metrics(results: &[QueryQualityResult]) -> BenchmarkMetrics {
    let samples: Vec<f64> = results.iter().map(|result| result.search_ms).collect();
    summarize_ms_samples(&samples)
}

fn quality_agreement_details(
    elephant: &[QueryQualityResult],
    surreal: &[QueryQualityResult],
) -> Value {
    let mut exact_top1_matches = 0_usize;
    let mut exact_top3_matches = 0_usize;
    let mut mean_top3_overlap = 0.0_f64;
    let mut compared_queries = 0_usize;
    let mut query_details = Vec::new();

    for (left, right) in elephant.iter().zip(surreal) {
        compared_queries += 1;
        if left.top_title == right.top_title {
            exact_top1_matches += 1;
        }

        let left_top3: BTreeSet<_> = left.titles.iter().take(3).cloned().collect();
        let right_top3: BTreeSet<_> = right.titles.iter().take(3).cloned().collect();
        if left.titles.iter().take(3).eq(right.titles.iter().take(3)) {
            exact_top3_matches += 1;
        }

        let overlap = left_top3.intersection(&right_top3).count();
        mean_top3_overlap += usize_to_f64(overlap) / 3.0;
        query_details.push(json!({
            "query": left.query,
            "query_kind": left.query_kind,
            "elephant_top_titles": left.titles,
            "surreal_top_titles": right.titles,
            "top1_match": left.top_title == right.top_title,
            "top3_overlap_count": overlap,
        }));
    }

    let compared = usize::max(compared_queries, 1);
    json!({
        "query_count": compared_queries,
        "top1_agreement": usize_to_f64(exact_top1_matches) / usize_to_f64(compared),
        "exact_top3_order_agreement": usize_to_f64(exact_top3_matches) / usize_to_f64(compared),
        "mean_top3_overlap": mean_top3_overlap / usize_to_f64(compared),
        "per_query": query_details,
    })
}

fn summarize_ms_samples(samples_ms: &[f64]) -> BenchmarkMetrics {
    let durations: Vec<Duration> = samples_ms
        .iter()
        .copied()
        .map(|sample| Duration::from_secs_f64(sample / 1000.0))
        .collect();
    summarize_durations(&durations)
}

#[allow(clippy::too_many_lines)]
fn build_transcript_scenarios(config: ScaleConfig) -> Result<Vec<TranscriptScenario>> {
    let limits = TranscriptExtractionLimits {
        doc_chars: config.transcript_max_doc_chars,
        block_chars: config.transcript_max_block_chars,
        blocks: config.transcript_max_blocks,
    };
    let candidate_limit = usize::max(config.transcript_docs_per_corpus * 4, 32);
    let broad_project_candidate_limit = usize::max(config.transcript_docs_per_corpus * 16, 256);
    let cross_project_limit =
        config.transcript_docs_per_corpus + config.transcript_mixed_docs_per_source;
    let codex_sessions_recent_pool =
        load_codex_session_documents(candidate_limit, limits, TranscriptOrdering::ModifiedDesc)?;
    let codex_sessions_longform_pool =
        load_codex_session_documents(candidate_limit, limits, TranscriptOrdering::FileSizeDesc)?;
    let codex_archived_recent_pool =
        load_codex_archived_documents(candidate_limit, limits, TranscriptOrdering::ModifiedDesc)?;
    let codex_archived_longform_pool =
        load_codex_archived_documents(candidate_limit, limits, TranscriptOrdering::FileSizeDesc)?;
    let claude_projects_recent_pool =
        load_claude_project_documents(candidate_limit, limits, TranscriptOrdering::ModifiedDesc)?;
    let claude_projects_longform_pool =
        load_claude_project_documents(candidate_limit, limits, TranscriptOrdering::FileSizeDesc)?;
    let claude_projects_broad_pool = load_claude_project_documents(
        broad_project_candidate_limit,
        limits,
        TranscriptOrdering::ModifiedDesc,
    )?;
    let codex_sessions = take_transcript_documents(
        &codex_sessions_recent_pool,
        config.transcript_docs_per_corpus,
    );
    let codex_sessions_longform = take_transcript_documents(
        &codex_sessions_longform_pool,
        config.transcript_docs_per_corpus,
    );
    let codex_archived = take_transcript_documents(
        &codex_archived_recent_pool,
        config.transcript_docs_per_corpus,
    );
    let codex_archived_longform = take_transcript_documents(
        &codex_archived_longform_pool,
        config.transcript_docs_per_corpus,
    );
    let claude_projects = take_transcript_documents(
        &claude_projects_recent_pool,
        config.transcript_docs_per_corpus,
    );
    let claude_projects_longform = take_transcript_documents(
        &claude_projects_longform_pool,
        config.transcript_docs_per_corpus,
    );

    let mut mixed_recent = Vec::new();
    mixed_recent.extend(
        codex_sessions
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );
    mixed_recent.extend(
        codex_archived
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );
    mixed_recent.extend(
        claude_projects
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );

    let mut mixed_longform = Vec::new();
    mixed_longform.extend(
        codex_sessions_longform
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );
    mixed_longform.extend(
        codex_archived_longform
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );
    mixed_longform.extend(
        claude_projects_longform
            .iter()
            .take(config.transcript_mixed_docs_per_source)
            .cloned(),
    );

    let claude_meerkat = filter_documents_by_project(
        &claude_projects_recent_pool,
        "meerkat",
        config.transcript_docs_per_corpus,
    );
    let claude_raik = filter_documents_by_project(
        &claude_projects_broad_pool,
        "raik",
        config.transcript_docs_per_corpus,
    );
    let claude_elephant = filter_documents_by_project(
        &claude_projects_broad_pool,
        "elephant",
        config.transcript_docs_per_corpus,
    );
    let claude_unknown = filter_documents_by_unknown_project(
        &claude_projects_broad_pool,
        config.transcript_docs_per_corpus,
    );
    let claude_gclean = filter_documents_by_project(
        &claude_projects_broad_pool,
        "gclean",
        config.transcript_docs_per_corpus,
    );
    let claude_ob3_validator = filter_documents_by_project(
        &claude_projects_broad_pool,
        "ob3_validator",
        config.transcript_docs_per_corpus,
    );
    let claude_goldfish = filter_documents_by_project(
        &claude_projects_broad_pool,
        "goldfish",
        config.transcript_docs_per_corpus,
    );
    let claude_meerkat_mobkit = filter_documents_by_project(
        &claude_projects_broad_pool,
        "meerkat-mobkit",
        config.transcript_docs_per_corpus,
    );
    let archived_raik = filter_documents_by_project(
        &codex_archived_recent_pool,
        "raik",
        config.transcript_docs_per_corpus,
    );
    let archived_meerkat = filter_documents_by_project(
        &codex_archived_recent_pool,
        "meerkat",
        config.transcript_docs_per_corpus,
    );
    let archived_elephant = filter_documents_by_project(
        &codex_archived_recent_pool,
        "elephant",
        config.transcript_docs_per_corpus,
    );
    let codex_sessions_tool_heavy = select_tool_heavy_documents(
        &codex_sessions_recent_pool,
        config.transcript_docs_per_corpus,
    );
    let claude_projects_tool_heavy = select_tool_heavy_documents(
        &claude_projects_broad_pool,
        config.transcript_docs_per_corpus,
    );
    let claude_projects_tool_light = select_tool_light_documents(
        &claude_projects_broad_pool,
        config.transcript_docs_per_corpus,
    );
    let codex_sessions_tool_light = select_tool_light_documents(
        &codex_sessions_recent_pool,
        config.transcript_docs_per_corpus,
    );
    let mut mixed_tool_heavy_pool = Vec::new();
    mixed_tool_heavy_pool.extend(select_tool_heavy_documents(
        &codex_sessions_recent_pool,
        config.transcript_mixed_docs_per_source,
    ));
    mixed_tool_heavy_pool.extend(select_tool_heavy_documents(
        &codex_archived_recent_pool,
        config.transcript_mixed_docs_per_source,
    ));
    mixed_tool_heavy_pool.extend(select_tool_heavy_documents(
        &claude_projects_broad_pool,
        config.transcript_mixed_docs_per_source,
    ));
    let mixed_tool_heavy = sort_tool_heavy_documents(mixed_tool_heavy_pool);
    let mut mixed_tool_light_pool = Vec::new();
    mixed_tool_light_pool.extend(select_tool_light_documents(
        &codex_sessions_recent_pool,
        config.transcript_mixed_docs_per_source,
    ));
    mixed_tool_light_pool.extend(select_tool_light_documents(
        &codex_archived_recent_pool,
        config.transcript_mixed_docs_per_source,
    ));
    mixed_tool_light_pool.extend(select_tool_light_documents(
        &claude_projects_broad_pool,
        config.transcript_mixed_docs_per_source,
    ));
    let mixed_tool_light = sort_tool_light_documents(mixed_tool_light_pool);
    let cross_raik_broad =
        combine_transcript_documents(&[&claude_raik, &archived_raik], cross_project_limit);
    let cross_meerkat_broad =
        combine_transcript_documents(&[&claude_meerkat, &archived_meerkat], cross_project_limit);
    let cross_elephant_broad =
        combine_transcript_documents(&[&claude_elephant, &archived_elephant], cross_project_limit);

    let mut scenarios = vec![
        TranscriptScenario {
            name: "codex_sessions_recent",
            docs: codex_sessions,
        },
        TranscriptScenario {
            name: "codex_sessions_longform",
            docs: codex_sessions_longform,
        },
        TranscriptScenario {
            name: "codex_archived_recent",
            docs: codex_archived,
        },
        TranscriptScenario {
            name: "codex_archived_longform",
            docs: codex_archived_longform,
        },
        TranscriptScenario {
            name: "claude_projects_recent",
            docs: claude_projects,
        },
        TranscriptScenario {
            name: "claude_projects_longform",
            docs: claude_projects_longform,
        },
        TranscriptScenario {
            name: "mixed_recent",
            docs: mixed_recent,
        },
        TranscriptScenario {
            name: "mixed_longform",
            docs: mixed_longform,
        },
    ];

    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_meerkat_recent", claude_meerkat);
    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_raik_broad", claude_raik);
    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_elephant_broad", claude_elephant);
    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_unknown_broad", claude_unknown);
    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_gclean_broad", claude_gclean);
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "claude_ob3_validator_broad",
        claude_ob3_validator,
    );
    push_transcript_scenario_if_non_empty(&mut scenarios, "claude_goldfish_broad", claude_goldfish);
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "claude_meerkat_mobkit_broad",
        claude_meerkat_mobkit,
    );
    push_transcript_scenario_if_non_empty(&mut scenarios, "archived_raik_recent", archived_raik);
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "archived_meerkat_recent",
        archived_meerkat,
    );
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "codex_sessions_tool_heavy",
        codex_sessions_tool_heavy,
    );
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "claude_projects_tool_heavy",
        claude_projects_tool_heavy,
    );
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "claude_projects_tool_light",
        claude_projects_tool_light,
    );
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "codex_sessions_tool_light",
        codex_sessions_tool_light,
    );
    push_transcript_scenario_if_non_empty(&mut scenarios, "mixed_tool_heavy", mixed_tool_heavy);
    push_transcript_scenario_if_non_empty(&mut scenarios, "mixed_tool_light", mixed_tool_light);
    push_transcript_scenario_if_non_empty(&mut scenarios, "cross_raik_broad", cross_raik_broad);
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "cross_meerkat_broad",
        cross_meerkat_broad,
    );
    push_transcript_scenario_if_non_empty(
        &mut scenarios,
        "cross_elephant_broad",
        cross_elephant_broad,
    );

    scenarios.retain(|scenario| !scenario.docs.is_empty());
    Ok(scenarios)
}

fn take_transcript_documents(docs: &[TranscriptDocument], limit: usize) -> Vec<TranscriptDocument> {
    docs.iter().take(limit).cloned().collect()
}

fn filter_documents_by_project(
    docs: &[TranscriptDocument],
    project: &str,
    limit: usize,
) -> Vec<TranscriptDocument> {
    let project = project.to_ascii_lowercase();
    docs.iter()
        .filter(|doc| {
            doc.project_hint
                .as_deref()
                .is_some_and(|hint| hint.to_ascii_lowercase().contains(&project))
        })
        .take(limit)
        .cloned()
        .collect()
}

fn filter_documents_by_unknown_project(
    docs: &[TranscriptDocument],
    limit: usize,
) -> Vec<TranscriptDocument> {
    docs.iter()
        .filter(|doc| doc.project_hint.is_none())
        .take(limit)
        .cloned()
        .collect()
}

fn select_tool_heavy_documents(
    docs: &[TranscriptDocument],
    limit: usize,
) -> Vec<TranscriptDocument> {
    sort_tool_heavy_documents(docs.to_vec())
        .into_iter()
        .take(limit)
        .collect()
}

fn select_tool_light_documents(
    docs: &[TranscriptDocument],
    limit: usize,
) -> Vec<TranscriptDocument> {
    sort_tool_light_documents(docs.to_vec())
        .into_iter()
        .take(limit)
        .collect()
}

fn sort_tool_heavy_documents(mut docs: Vec<TranscriptDocument>) -> Vec<TranscriptDocument> {
    docs.sort_by(|left, right| {
        right
            .tool_event_count
            .cmp(&left.tool_event_count)
            .then_with(|| modified_key(&right.source_path).cmp(&modified_key(&left.source_path)))
            .then_with(|| right.title.cmp(&left.title))
    });
    docs
}

fn sort_tool_light_documents(mut docs: Vec<TranscriptDocument>) -> Vec<TranscriptDocument> {
    docs.sort_by(|left, right| {
        left.tool_event_count
            .cmp(&right.tool_event_count)
            .then_with(|| modified_key(&right.source_path).cmp(&modified_key(&left.source_path)))
            .then_with(|| left.title.cmp(&right.title))
    });
    docs
}

fn combine_transcript_documents(
    slices: &[&[TranscriptDocument]],
    limit: usize,
) -> Vec<TranscriptDocument> {
    let mut combined = Vec::new();
    for slice in slices {
        combined.extend_from_slice(slice);
    }

    sort_recent_documents(dedupe_transcript_documents(combined))
        .into_iter()
        .take(limit)
        .collect()
}

fn dedupe_transcript_documents(docs: Vec<TranscriptDocument>) -> Vec<TranscriptDocument> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for doc in docs {
        let key = doc.source_path.clone();
        if seen.insert(key) {
            deduped.push(doc);
        }
    }
    deduped
}

fn sort_recent_documents(mut docs: Vec<TranscriptDocument>) -> Vec<TranscriptDocument> {
    docs.sort_by(|left, right| {
        modified_key(&right.source_path)
            .cmp(&modified_key(&left.source_path))
            .then_with(|| right.title.cmp(&left.title))
    });
    docs
}

fn push_transcript_scenario_if_non_empty(
    scenarios: &mut Vec<TranscriptScenario>,
    name: &'static str,
    docs: Vec<TranscriptDocument>,
) {
    if docs.is_empty() {
        return;
    }

    scenarios.push(TranscriptScenario { name, docs });
}

fn load_codex_session_documents(
    limit: usize,
    limits: TranscriptExtractionLimits,
    ordering: TranscriptOrdering,
) -> Result<Vec<TranscriptDocument>> {
    let paths = collect_direct_paths(&home_dir()?.join(".codex").join("sessions"), "json")?;
    Ok(load_transcript_documents(paths, limit, ordering, |path| {
        parse_codex_session_document(path, limits)
    }))
}

fn load_codex_archived_documents(
    limit: usize,
    limits: TranscriptExtractionLimits,
    ordering: TranscriptOrdering,
) -> Result<Vec<TranscriptDocument>> {
    let paths = collect_direct_paths(
        &home_dir()?.join(".codex").join("archived_sessions"),
        "jsonl",
    )?;
    Ok(load_transcript_documents(paths, limit, ordering, |path| {
        parse_codex_archived_document(path, limits)
    }))
}

fn load_claude_project_documents(
    limit: usize,
    limits: TranscriptExtractionLimits,
    ordering: TranscriptOrdering,
) -> Result<Vec<TranscriptDocument>> {
    let paths = collect_claude_project_paths(&home_dir()?.join(".claude").join("projects"))?;
    Ok(load_transcript_documents(paths, limit, ordering, |path| {
        parse_claude_project_document(path, limits)
    }))
}

fn load_transcript_documents<F>(
    mut paths: Vec<PathBuf>,
    limit: usize,
    ordering: TranscriptOrdering,
    mut parser: F,
) -> Vec<TranscriptDocument>
where
    F: FnMut(&Path) -> Result<Option<TranscriptDocument>>,
{
    sort_transcript_paths(&mut paths, ordering);
    let mut documents = Vec::with_capacity(limit);

    for path in paths {
        if documents.len() >= limit {
            break;
        }

        match parser(&path) {
            Ok(Some(document)) => documents.push(document),
            Ok(None) => {}
            Err(error) => {
                eprintln!(
                    "skipping transcript `{}` due to parse error: {error:#}",
                    path.display()
                );
            }
        }
    }

    documents
}

fn sort_transcript_paths(paths: &mut [PathBuf], ordering: TranscriptOrdering) {
    match ordering {
        TranscriptOrdering::ModifiedDesc => {
            paths.sort_by_key(|path| Reverse(modified_key(path)));
        }
        TranscriptOrdering::FileSizeDesc => {
            paths.sort_by_key(|path| Reverse(file_size_key(path)));
        }
    }
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME environment variable is not set"))
}

fn collect_direct_paths(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read transcript directory `{}`", dir.display()))?;
    let mut paths = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == extension)
        {
            paths.push(path);
        }
    }

    Ok(paths)
}

fn collect_claude_project_paths(projects_dir: &Path) -> Result<Vec<PathBuf>> {
    if !projects_dir.exists() {
        return Ok(Vec::new());
    }
    let project_dirs = fs::read_dir(projects_dir).with_context(|| {
        format!(
            "failed to read Claude projects directory `{}`",
            projects_dir.display()
        )
    })?;
    let mut paths = Vec::new();

    for entry in project_dirs {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let transcripts = fs::read_dir(&path)
            .with_context(|| format!("failed to read Claude project `{}`", path.display()))?;
        for transcript in transcripts {
            let transcript = transcript?;
            let transcript_path = transcript.path();
            if transcript_path.is_file()
                && transcript_path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == "jsonl")
            {
                paths.push(transcript_path);
            }
        }
    }

    Ok(paths)
}

fn modified_key(path: &Path) -> Duration {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|timestamp| timestamp.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
}

fn file_size_key(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn parse_codex_session_document(
    path: &Path,
    limits: TranscriptExtractionLimits,
) -> Result<Option<TranscriptDocument>> {
    let raw = fs::read(path)
        .with_context(|| format!("failed to read Codex session `{}`", path.display()))?;
    let value: Value = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse Codex session `{}`", path.display()))?;
    let items = value
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing `items` array"))?;

    let mut blocks = Vec::new();
    let mut total_chars = 0_usize;
    let mut first_user_prompt = None;
    let mut last_user_prompt = None;
    let mut tool_event_count = 0_usize;

    for item in items {
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        match item_type {
            "message" => {
                let role = item
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("message");
                if role == "system" {
                    continue;
                }
                for text in extract_text_fragments(item.get("content").unwrap_or(&Value::Null)) {
                    if role == "user" && is_meaningful_prompt(&text) {
                        let normalized = normalize_text(&text);
                        if first_user_prompt.is_none() {
                            first_user_prompt = Some(normalized.clone());
                        }
                        last_user_prompt = Some(normalized);
                    }
                    push_transcript_block(&mut blocks, &mut total_chars, role, &text, limits);
                }
            }
            "function_call" => {
                tool_event_count += 1;
                let name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_transcript_block(
                    &mut blocks,
                    &mut total_chars,
                    "tool_call",
                    &format!("{name} {arguments}"),
                    limits,
                );
            }
            "function_call_output" => {
                tool_event_count += 1;
                let output = item
                    .get("output")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_transcript_block(&mut blocks, &mut total_chars, "tool_result", output, limits);
            }
            _ => {}
        }
    }

    Ok(build_transcript_document(
        "codex-sessions",
        path,
        None,
        first_user_prompt,
        last_user_prompt,
        tool_event_count,
        &blocks,
    ))
}

#[allow(clippy::too_many_lines)]
fn parse_codex_archived_document(
    path: &Path,
    limits: TranscriptExtractionLimits,
) -> Result<Option<TranscriptDocument>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read archived Codex session `{}`", path.display()))?;
    let mut blocks = Vec::new();
    let mut total_chars = 0_usize;
    let mut first_user_prompt = None;
    let mut last_user_prompt = None;
    let mut project_hint = None;
    let mut tool_event_count = 0_usize;

    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if project_hint.is_none() {
            project_hint = event
                .get("payload")
                .and_then(|payload| payload.get("cwd"))
                .and_then(Value::as_str)
                .map(project_hint_from_cwd)
                .or_else(|| {
                    event
                        .get("cwd")
                        .and_then(Value::as_str)
                        .map(project_hint_from_cwd)
                });
        }

        let Some(payload) = event.get("payload") else {
            continue;
        };
        let Some(payload_type) = payload.get("type").and_then(Value::as_str) else {
            continue;
        };

        match payload_type {
            "user_message" => {
                for text in extract_text_fragments(
                    payload
                        .get("message")
                        .or_else(|| payload.get("content"))
                        .unwrap_or(&Value::Null),
                ) {
                    if is_meaningful_prompt(&text) {
                        let normalized = normalize_text(&text);
                        if first_user_prompt.is_none() {
                            first_user_prompt = Some(normalized.clone());
                        }
                        last_user_prompt = Some(normalized);
                    }
                    push_transcript_block(&mut blocks, &mut total_chars, "user", &text, limits);
                }
            }
            "agent_message" | "message" => {
                for text in extract_text_fragments(
                    payload
                        .get("message")
                        .or_else(|| payload.get("content"))
                        .unwrap_or(&Value::Null),
                ) {
                    push_transcript_block(
                        &mut blocks,
                        &mut total_chars,
                        "assistant",
                        &text,
                        limits,
                    );
                }
            }
            "function_call" => {
                tool_event_count += 1;
                let name = payload
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                let arguments = payload
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_transcript_block(
                    &mut blocks,
                    &mut total_chars,
                    "tool_call",
                    &format!("{name} {arguments}"),
                    limits,
                );
            }
            "function_call_output" => {
                tool_event_count += 1;
                let output = payload
                    .get("output")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_transcript_block(&mut blocks, &mut total_chars, "tool_result", output, limits);
            }
            _ => {}
        }
    }

    Ok(build_transcript_document(
        "codex-archived",
        path,
        project_hint.as_deref(),
        first_user_prompt,
        last_user_prompt,
        tool_event_count,
        &blocks,
    ))
}

fn parse_claude_project_document(
    path: &Path,
    limits: TranscriptExtractionLimits,
) -> Result<Option<TranscriptDocument>> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read Claude project transcript `{}`",
            path.display()
        )
    })?;
    let mut blocks = Vec::new();
    let mut total_chars = 0_usize;
    let mut first_user_prompt = None;
    let mut last_user_prompt = None;
    let mut project_hint = None;
    let mut tool_event_count = 0_usize;

    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        if project_hint.is_none() {
            project_hint = event
                .get("cwd")
                .and_then(Value::as_str)
                .map(project_hint_from_cwd);
        }

        match event.get("type").and_then(Value::as_str) {
            Some("user") => {
                for text in extract_claude_message_fragments(event.get("message")) {
                    if is_meaningful_prompt(&text) {
                        let normalized = normalize_text(&text);
                        if first_user_prompt.is_none() {
                            first_user_prompt = Some(normalized.clone());
                        }
                        last_user_prompt = Some(normalized);
                    }
                    push_transcript_block(&mut blocks, &mut total_chars, "user", &text, limits);
                }
            }
            Some("assistant") => {
                for text in extract_claude_message_fragments(event.get("message")) {
                    push_transcript_block(
                        &mut blocks,
                        &mut total_chars,
                        "assistant",
                        &text,
                        limits,
                    );
                }
            }
            _ => {}
        }

        if let Some(message) = event
            .get("message")
            .and_then(|message| message.get("content"))
        {
            tool_event_count += count_claude_tool_events(message);
        }
    }

    Ok(build_transcript_document(
        "claude-projects",
        path,
        project_hint.as_deref(),
        first_user_prompt,
        last_user_prompt,
        tool_event_count,
        &blocks,
    ))
}

fn build_transcript_document(
    source: &'static str,
    path: &Path,
    project_hint: Option<&str>,
    first_user_prompt: Option<String>,
    last_user_prompt: Option<String>,
    tool_event_count: usize,
    blocks: &[String],
) -> Option<TranscriptDocument> {
    if blocks.is_empty() {
        return None;
    }

    let text = blocks.join("\n");
    let title = transcript_title(
        source,
        path,
        project_hint,
        first_user_prompt.as_deref(),
        &text,
    );

    Some(TranscriptDocument {
        title,
        text,
        collection: source.to_string(),
        source,
        source_path: path.to_path_buf(),
        project_hint: project_hint.map(ToString::to_string),
        first_user_prompt,
        last_user_prompt,
        tool_event_count,
    })
}

fn extract_text_fragments(value: &Value) -> Vec<String> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Vec::new(),
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items.iter().flat_map(extract_text_fragments).collect(),
        Value::Object(map) => {
            let mut fragments = Vec::new();
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                fragments.push(text.to_string());
            }
            if let Some(message) = map.get("message") {
                fragments.extend(extract_text_fragments(message));
            }
            if let Some(content) = map.get("content") {
                fragments.extend(extract_text_fragments(content));
            }
            if let Some(output) = map.get("output").and_then(Value::as_str) {
                fragments.push(output.to_string());
            }
            fragments
        }
    }
}

fn extract_claude_message_fragments(message: Option<&Value>) -> Vec<String> {
    let Some(message) = message else {
        return Vec::new();
    };
    let Some(content) = message.get("content") else {
        return Vec::new();
    };

    match content {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => {
            let mut fragments = Vec::new();
            for item in items {
                match item.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if let Some(text) = item.get("text").and_then(Value::as_str) {
                            fragments.push(text.to_string());
                        }
                    }
                    Some("tool_use") => {
                        let name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
                        let input = item.get("input").map_or_else(String::new, json_preview);
                        fragments.push(format!("{name} {input}"));
                    }
                    Some("tool_result") => {
                        fragments.extend(extract_text_fragments(
                            item.get("content").unwrap_or(&Value::Null),
                        ));
                    }
                    _ => {}
                }
            }
            fragments
        }
        _ => Vec::new(),
    }
}

fn count_claude_tool_events(content: &Value) -> usize {
    match content {
        Value::Array(items) => items
            .iter()
            .filter(|item| {
                item.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|item_type| matches!(item_type, "tool_use" | "tool_result"))
            })
            .count(),
        _ => 0,
    }
}

fn push_transcript_block(
    blocks: &mut Vec<String>,
    total_chars: &mut usize,
    label: &str,
    text: &str,
    limits: TranscriptExtractionLimits,
) {
    if blocks.len() >= limits.blocks || *total_chars >= limits.doc_chars {
        return;
    }

    let normalized = normalize_text(text);
    if normalized.is_empty() || normalized == "[Request interrupted by user for tool use]" {
        return;
    }

    let mut block = format!("[{label}] {normalized}");
    block = truncate_chars(&block, limits.block_chars);
    let remaining = limits.doc_chars.saturating_sub(*total_chars);
    if remaining < 32 {
        return;
    }

    let block = truncate_chars(&block, remaining);
    *total_chars += block.chars().count() + 1;
    blocks.push(block);
}

fn normalize_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut last_was_space = false;

    for ch in text.chars() {
        let normalized_char = if ch.is_whitespace() || ch.is_control() {
            ' '
        } else {
            ch
        };
        if normalized_char == ' ' {
            if !last_was_space {
                normalized.push(' ');
                last_was_space = true;
            }
        } else {
            normalized.push(normalized_char);
            last_was_space = false;
        }
    }

    normalized.trim().to_string()
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text.to_string();
    }

    if max_chars <= 3 {
        return text.chars().take(max_chars).collect();
    }

    let mut truncated = text.chars().take(max_chars - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn is_meaningful_prompt(text: &str) -> bool {
    let normalized = normalize_text(text);
    normalized.len() >= 24
        && normalized != "[Request interrupted by user for tool use]"
        && normalized.chars().any(|ch| ch.is_ascii_alphabetic())
}

fn transcript_title(
    source: &str,
    path: &Path,
    project_hint: Option<&str>,
    first_user_prompt: Option<&str>,
    text: &str,
) -> String {
    let source_hint = project_hint.unwrap_or(source);
    let file_hint = short_file_hint(path);
    let snippet_source = first_user_prompt.unwrap_or(text);
    let snippet = truncate_chars(&normalize_text(snippet_source), 72);
    format!("{source_hint} {file_hint} | {snippet}")
}

fn short_file_hint(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("session");
    truncate_chars(stem, 12)
}

fn project_hint_from_cwd(cwd: &str) -> String {
    Path::new(cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("project")
        .to_string()
}

fn json_preview(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn build_transcript_expectations(
    docs: &[TranscriptDocument],
    strategy: TranscriptQueryStrategy,
    query_terms: usize,
) -> Vec<SearchExpectation> {
    let primary_tokens: Vec<Vec<String>> = docs
        .iter()
        .map(|doc| {
            let source = match strategy {
                TranscriptQueryStrategy::FirstUser => doc
                    .first_user_prompt
                    .as_deref()
                    .unwrap_or(doc.text.as_str()),
                TranscriptQueryStrategy::LastUser => doc
                    .last_user_prompt
                    .as_deref()
                    .or(doc.first_user_prompt.as_deref())
                    .unwrap_or(doc.text.as_str()),
                TranscriptQueryStrategy::SessionSalience => doc.text.as_str(),
            };
            transcript_query_tokens(source)
        })
        .collect();
    let full_tokens: Vec<Vec<String>> = docs
        .iter()
        .map(|doc| transcript_query_tokens(&doc.text))
        .collect();
    let primary_doc_frequency = token_document_frequency(&primary_tokens);
    let full_doc_frequency = token_document_frequency(&full_tokens);

    docs.iter()
        .zip(primary_tokens)
        .zip(full_tokens)
        .filter_map(|((doc, primary), fallback)| {
            let mut tokens = select_query_terms(&primary, &primary_doc_frequency, query_terms);
            if tokens.len() < 3 {
                tokens = select_query_terms(&fallback, &full_doc_frequency, query_terms);
            }
            if tokens.is_empty() {
                return None;
            }

            Some(SearchExpectation {
                query: tokens.join(" "),
                expected_top_title: doc.title.clone(),
                query_kind: strategy.label().to_string(),
            })
        })
        .collect()
}

fn transcript_query_tokens(text: &str) -> Vec<String> {
    elephant_db::tokenize(text)
        .into_iter()
        .filter(|token| is_query_token(token))
        .collect()
}

fn token_document_frequency(token_lists: &[Vec<String>]) -> HashMap<String, usize> {
    let mut document_frequency = HashMap::new();
    for tokens in token_lists {
        let unique: HashSet<_> = tokens.iter().cloned().collect();
        for token in unique {
            *document_frequency.entry(token).or_insert(0) += 1;
        }
    }
    document_frequency
}

fn select_query_terms(
    tokens: &[String],
    document_frequency: &HashMap<String, usize>,
    query_terms: usize,
) -> Vec<String> {
    let mut term_frequency = HashMap::new();
    for token in tokens {
        *term_frequency.entry(token.clone()).or_insert(0_usize) += 1;
    }

    let mut ranked: Vec<(String, usize, usize)> = term_frequency
        .into_iter()
        .map(|(token, count)| {
            let doc_freq = *document_frequency.get(&token).unwrap_or(&usize::MAX);
            (token, doc_freq, count)
        })
        .collect();
    ranked.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| right.0.len().cmp(&left.0.len()))
            .then_with(|| left.0.cmp(&right.0))
    });

    ranked
        .into_iter()
        .take(query_terms)
        .map(|(token, _, _)| token)
        .collect()
}

fn is_query_token(token: &str) -> bool {
    if token.len() < 3 || token.len() > 32 {
        return false;
    }
    if !token.chars().any(|ch| ch.is_ascii_alphabetic()) {
        return false;
    }
    if token.chars().all(|ch| ch.is_ascii_hexdigit()) && token.len() > 8 {
        return false;
    }
    let digit_count = token.chars().filter(char::is_ascii_digit).count();
    if digit_count > token.len() / 2 {
        return false;
    }

    !matches!(
        token,
        "the"
            | "and"
            | "for"
            | "with"
            | "that"
            | "this"
            | "from"
            | "into"
            | "while"
            | "when"
            | "where"
            | "what"
            | "which"
            | "your"
            | "have"
            | "will"
            | "would"
            | "could"
            | "should"
            | "there"
            | "their"
            | "they"
            | "them"
            | "then"
            | "just"
            | "like"
            | "need"
            | "needs"
            | "also"
            | "only"
            | "after"
            | "before"
            | "because"
            | "using"
            | "used"
            | "user"
            | "assistant"
            | "system"
            | "tool"
            | "tools"
            | "message"
            | "messages"
            | "implement"
            | "following"
            | "plan"
            | "file"
            | "files"
            | "line"
            | "lines"
            | "code"
            | "json"
            | "jsonl"
            | "session"
            | "sessions"
            | "claude"
            | "codex"
            | "project"
            | "projects"
            | "request"
            | "response"
            | "prompt"
            | "please"
            | "read"
            | "write"
            | "grep"
            | "found"
            | "output"
            | "input"
            | "content"
            | "text"
            | "callback"
            | "chunk"
            | "wall"
            | "time"
            | "process"
            | "exited"
            | "tokens"
            | "token"
            | "null"
    )
}

fn transcript_source_counts(docs: &[TranscriptDocument]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for doc in docs {
        *counts.entry(doc.source.to_string()).or_insert(0_usize) += 1;
    }
    counts
}

fn transcript_project_counts(docs: &[TranscriptDocument]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for doc in docs {
        let key = doc
            .project_hint
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        *counts.entry(key).or_insert(0_usize) += 1;
    }
    counts
}

fn transcript_sample_files(docs: &[TranscriptDocument], limit: usize) -> Vec<String> {
    docs.iter()
        .take(limit)
        .map(|doc| {
            doc.source_path
                .file_name()
                .and_then(|name| name.to_str())
                .map_or_else(
                    || doc.source_path.display().to_string(),
                    ToString::to_string,
                )
        })
        .collect()
}

fn benchmark_sync_case<R, F>(
    section: &str,
    name: String,
    iterations: usize,
    warmup: usize,
    details: Value,
    mut f: F,
) -> Result<BenchmarkCase>
where
    F: FnMut() -> Result<R>,
{
    let metrics = measure_sync(iterations, warmup, &mut f)?;
    Ok(BenchmarkCase {
        section: section.to_string(),
        name,
        iterations,
        metrics,
        details,
    })
}

async fn benchmark_async_case<R, F, Fut>(
    section: &str,
    name: String,
    iterations: usize,
    warmup: usize,
    details: Value,
    mut f: F,
) -> Result<BenchmarkCase>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<R>>,
{
    let metrics = measure_async(iterations, warmup, &mut f).await?;
    Ok(BenchmarkCase {
        section: section.to_string(),
        name,
        iterations,
        metrics,
        details,
    })
}

fn one_shot_case(section: &str, name: String, duration: Duration, details: Value) -> BenchmarkCase {
    BenchmarkCase {
        section: section.to_string(),
        name,
        iterations: 1,
        metrics: summarize_durations(&[duration]),
        details,
    }
}

fn measure_sync<R, F>(iterations: usize, warmup: usize, f: &mut F) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Result<R>,
{
    for _ in 0..warmup {
        black_box(f()?);
    }

    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let result = f()?;
        black_box(result);
        durations.push(started.elapsed());
    }
    Ok(summarize_durations(&durations))
}

async fn measure_async<R, F, Fut>(
    iterations: usize,
    warmup: usize,
    f: &mut F,
) -> Result<BenchmarkMetrics>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<R>>,
{
    for _ in 0..warmup {
        black_box(f().await?);
    }

    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let result = f().await?;
        black_box(result);
        durations.push(started.elapsed());
    }
    Ok(summarize_durations(&durations))
}

fn summarize_durations(durations: &[Duration]) -> BenchmarkMetrics {
    let mut samples: Vec<f64> = durations.iter().map(Duration::as_secs_f64).collect();
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));

    let total = samples.iter().sum::<f64>();
    let count = f64::from(u32::try_from(samples.len()).unwrap_or(u32::MAX).max(1));
    let median_index = samples.len() / 2;
    let p95_index = samples
        .len()
        .saturating_mul(95)
        .div_ceil(100)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    let min = *samples.first().unwrap_or(&0.0);
    let max = *samples.last().unwrap_or(&0.0);
    let mean = if samples.is_empty() {
        0.0
    } else {
        total / count
    };
    let variance = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|sample| {
                let delta = *sample - mean;
                delta * delta
            })
            .sum::<f64>()
            / count
    };
    let stddev = variance.sqrt();
    let ops_per_sec = if total > 0.0 { count / total } else { 0.0 };

    BenchmarkMetrics {
        total_ms: total * 1000.0,
        mean_ms: mean * 1000.0,
        stddev_ms: stddev * 1000.0,
        median_ms: samples.get(median_index).copied().unwrap_or(0.0) * 1000.0,
        p95_ms: samples.get(p95_index).copied().unwrap_or(0.0) * 1000.0,
        min_ms: min * 1000.0,
        max_ms: max * 1000.0,
        ops_per_sec,
    }
}

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn u64_to_f64(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn print_human_report(report: &BenchmarkReport) {
    println!(
        "# Elephant Benchmark Report\nGenerated: {}\nSuite: {:?}\nBackend: {:?}\nScale: {:?}\n",
        report.generated_at, report.suite, report.backend, report.scale
    );
    println!(
        "{:<12} {:<42} {:>8} {:>11} {:>11} {:>11} {:>11}",
        "Section", "Case", "Iters", "Median ms", "P95 ms", "Mean ms", "Ops/s"
    );
    println!("{}", "-".repeat(114));
    for case in &report.cases {
        println!(
            "{:<12} {:<42} {:>8} {:>11.3} {:>11.3} {:>11.3} {:>11.1}",
            case.section,
            truncate_name(&case.name, 42),
            case.iterations,
            case.metrics.median_ms,
            case.metrics.p95_ms,
            case.metrics.mean_ms,
            case.metrics.ops_per_sec,
        );
    }
}

fn truncate_name(name: &str, width: usize) -> String {
    if name.chars().count() <= width {
        return name.to_string();
    }

    let keep = width.saturating_sub(1);
    let mut out = name.chars().take(keep).collect::<String>();
    out.push('…');
    out
}

fn build_graph_fixture(node_count: usize, edges_per_node: usize) -> Result<GraphFixture> {
    let db = ElephantDb::new();
    let mut ids = Vec::with_capacity(node_count);

    for index in 0..node_count {
        let entity = entity("bench", &format!("entity-{index}"));
        ids.push(entity.base.id.clone());
        db.create(entity)?;
    }

    for index in 0..node_count {
        for offset in 1..=edges_per_node {
            let target = &ids[(index + offset) % node_count];
            db.create(rel("bench", &ids[index], target, "linked"))?;
        }
    }

    Ok(GraphFixture {
        db,
        start_id: ids[node_count / 2].clone(),
        node_count,
        edge_count: node_count * edges_per_node,
    })
}

fn build_doc_fixture(chunk_count: usize) -> Result<DocFixture> {
    let db = ElephantDb::new();
    let doc = doc("bench", "Doc Benchmark", "Document benchmark text.");
    let revision = revision("bench", &doc.base.id, "Document benchmark text.");
    db.create(doc)?;
    db.create(revision.clone())?;

    let root = doc_node(
        "bench",
        &revision.base.id,
        None,
        NodeRole::Document,
        0,
        0,
        None,
        Some("benchmark"),
    );
    db.create(root.clone())?;

    let section_count = usize::max(4, chunk_count / 200);
    let paragraphs_per_section = 4_usize;
    let mut paragraph_ids = Vec::new();
    let mut node_count = 1;

    for section_index in 0..section_count {
        let section = doc_node(
            "bench",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Section,
            i32::try_from(section_index)?,
            1,
            None,
            Some("benchmark"),
        );
        let section_id = section.base.id.clone();
        db.create(section)?;
        node_count += 1;

        for paragraph_index in 0..paragraphs_per_section {
            let position = section_index * paragraphs_per_section + paragraph_index;
            let paragraph = doc_node(
                "bench",
                &revision.base.id,
                Some(&section_id),
                NodeRole::Paragraph,
                i32::try_from(position)?,
                2,
                None,
                Some("benchmark"),
            );
            paragraph_ids.push(paragraph.base.id.clone());
            db.create(paragraph)?;
            node_count += 1;
        }
    }

    let mut deep_chunk_id = None;
    for chunk_index in 0..chunk_count {
        let parent_id = &paragraph_ids[chunk_index % paragraph_ids.len()];
        let chunk = doc_node(
            "bench",
            &revision.base.id,
            Some(parent_id),
            NodeRole::Chunk,
            i32::try_from(chunk_index)?,
            3,
            Some(&format!(
                "Chunk {chunk_index} mentions Alice and the White Rabbit."
            )),
            Some("benchmark"),
        );
        deep_chunk_id = Some(chunk.base.id.clone());
        db.create(chunk)?;
        node_count += 1;
    }

    Ok(DocFixture {
        db,
        revision_id: revision.base.id,
        deep_chunk_id: deep_chunk_id.ok_or_else(|| anyhow!("missing deep chunk id"))?,
        node_count,
        chunk_count,
        depth: 3,
    })
}

fn build_vector_fixture(chunk_count: usize, dims: usize) -> Result<VectorFixture> {
    let db = ElephantDb::new();
    let doc = doc("bench", "Vector Benchmark", "Vector benchmark text.");
    let revision = revision("bench", &doc.base.id, "Vector benchmark text.");
    db.create(doc)?;
    db.create(revision.clone())?;

    let root = doc_node(
        "bench",
        &revision.base.id,
        None,
        NodeRole::Document,
        0,
        0,
        None,
        Some("benchmark"),
    );
    db.create(root.clone())?;

    let partition_name = PartitionKey::new(
        "default".to_string(),
        level_bucket(0),
        "hashed".to_string(),
        dims,
    )?
    .table_name();

    for index in 0..chunk_count {
        let text = if index % 16 == 0 {
            format!("white rabbit rabbit hole wonderland chunk {index}")
        } else {
            format!("queen croquet garden benchmark chunk {index}")
        };

        let chunk = doc_node(
            "bench",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            i32::try_from(index)?,
            1,
            Some(&text),
            Some("benchmark"),
        );
        let node_id = chunk.base.id.clone();
        db.create(chunk)?;

        let embedding = NodeEmbedding {
            base: base_for(TableName::ChunkEmbedding, "bench"),
            node: Link::new(node_id),
            model: "hashed".to_string(),
            dims: i32::try_from(dims)?,
            vector: elephant_db::hashed_text_embedding(&text, dims),
        };
        db.create(embedding)?;
    }

    Ok(VectorFixture {
        db,
        partition_name,
        query_vector: elephant_db::hashed_text_embedding("white rabbit rabbit hole", dims),
        chunk_count,
        dims,
    })
}

fn build_snapshot_fixture(
    entity_count: usize,
    edges_per_node: usize,
    doc_count: usize,
    chunks_per_doc: usize,
    dims: usize,
) -> Result<SnapshotFixture> {
    let db = ElephantDb::new();
    let mut entity_ids = Vec::with_capacity(entity_count);

    for index in 0..entity_count {
        let entity = entity("bench", &format!("entity-{index}"));
        entity_ids.push(entity.base.id.clone());
        db.create(entity)?;
    }

    for index in 0..entity_count {
        for offset in 1..=edges_per_node {
            let target = &entity_ids[(index + offset) % entity_count];
            db.create(rel("bench", &entity_ids[index], target, "linked"))?;
        }
    }
    let rel_count = entity_count * edges_per_node;

    let mut chunk_count = 0_usize;
    for doc_index in 0..doc_count {
        let doc = doc(
            "bench",
            &format!("Snapshot Doc {doc_index}"),
            "Snapshot benchmark document text.",
        );
        let revision = revision("bench", &doc.base.id, "Snapshot benchmark document text.");
        db.create(doc)?;
        db.create(revision.clone())?;
        let root = doc_node(
            "bench",
            &revision.base.id,
            None,
            NodeRole::Document,
            0,
            0,
            None,
            Some("snapshot"),
        );
        db.create(root.clone())?;

        for chunk_index in 0..chunks_per_doc {
            let text = if chunk_index % 8 == 0 {
                format!("white rabbit snapshot chunk {doc_index}-{chunk_index}")
            } else {
                format!("generic snapshot chunk {doc_index}-{chunk_index}")
            };
            let chunk = doc_node(
                "bench",
                &revision.base.id,
                Some(&root.base.id),
                NodeRole::Chunk,
                i32::try_from(chunk_index)?,
                1,
                Some(&text),
                Some("snapshot"),
            );
            let node_id = chunk.base.id.clone();
            db.create(chunk)?;

            let embedding = NodeEmbedding {
                base: base_for(TableName::ChunkEmbedding, "bench"),
                node: Link::new(node_id),
                model: "hashed".to_string(),
                dims: i32::try_from(dims)?,
                vector: elephant_db::hashed_text_embedding(&text, dims),
            };
            db.create(embedding)?;
            chunk_count += 1;
        }
    }

    let snapshot_bytes = db.export_snapshot_bytes()?;

    Ok(SnapshotFixture {
        db,
        snapshot_bytes,
        entity_count,
        rel_count,
        doc_count,
        chunk_count,
        embedding_count: chunk_count,
    })
}

fn build_concurrency_fixture(config: ScaleConfig) -> Result<ConcurrencyFixture> {
    let entity_count = if config.graph_small_nodes <= 2_000 {
        1_024
    } else {
        2_048
    };
    let chunk_count = if config.doc_chunks <= 1_000 { 256 } else { 512 };
    let dims = config.vector_dims;
    let space_id = "bench".to_string();
    let db = Arc::new(ElephantDb::new());
    let mut entity_ids = Vec::with_capacity(entity_count);

    for index in 0..entity_count {
        let entity = entity(&space_id, &format!("concurrency-entity-{index}"));
        entity_ids.push(entity.base.id.clone());
        db.create(entity)?;
    }

    for index in 0..entity_count {
        for offset in 1..=2 {
            let target = &entity_ids[(index + offset) % entity_count];
            db.create(rel(&space_id, &entity_ids[index], target, "linked"))?;
        }
    }

    let doc = doc(
        &space_id,
        "Concurrency Benchmark Doc",
        "white rabbit wonderland concurrency benchmark",
    );
    let revision = revision(
        &space_id,
        &doc.base.id,
        "white rabbit wonderland concurrency benchmark",
    );
    db.create(doc)?;
    db.create(revision.clone())?;
    let root = doc_node(
        &space_id,
        &revision.base.id,
        None,
        NodeRole::Document,
        0,
        0,
        None,
        Some("benchmark"),
    );
    db.create(root.clone())?;

    let partition_name = PartitionKey::new(
        "default".to_string(),
        level_bucket(0),
        "hashed".to_string(),
        dims,
    )?
    .table_name();

    for index in 0..chunk_count {
        let text = if index % 8 == 0 {
            format!("white rabbit wonderland concurrency chunk {index}")
        } else {
            format!("queue leasing graph merge vector search chunk {index}")
        };

        let chunk = doc_node(
            &space_id,
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Chunk,
            index,
            1,
            Some(&text),
            Some("benchmark"),
        );
        let node_id = chunk.base.id.clone();
        db.create(chunk)?;

        let embedding = NodeEmbedding {
            base: base_for(TableName::ChunkEmbedding, &space_id),
            node: Link::new(node_id),
            model: "hashed".to_string(),
            dims: i32::try_from(dims)?,
            vector: elephant_db::hashed_text_embedding(&text, dims),
        };
        db.create(embedding)?;
    }

    Ok(ConcurrencyFixture {
        db,
        space_id,
        entity_ids: Arc::new(entity_ids.clone()),
        start_id: entity_ids[entity_count / 2].clone(),
        partition_name,
        query_vector: Arc::new(elephant_db::hashed_text_embedding(
            "white rabbit wonderland",
            dims,
        )),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_concurrency(
    fixture: Arc<ConcurrencyFixture>,
    workers: usize,
    iterations: usize,
) -> Result<ConcurrencyRun> {
    let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let fixture = fixture.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || -> Result<Duration> {
            barrier.wait();
            let started = Instant::now();
            for iteration in 0..iterations {
                let entity_id = &fixture.entity_ids
                    [(worker_id * iterations + iteration) % fixture.entity_ids.len()];
                let entity = fixture
                    .db
                    .get::<Entity>(&fixture.space_id, entity_id, false)?
                    .ok_or_else(|| anyhow!("missing benchmark entity"))?;
                black_box(entity);
                black_box(
                    fixture
                        .db
                        .graph_neighbors(&fixture.space_id, &fixture.start_id, 2)?,
                );
                black_box(fixture.db.search_doc_ids(
                    &fixture.space_id,
                    Some("white rabbit wonderland"),
                    false,
                )?);
                black_box(fixture.db.search_node_embeddings(
                    &fixture.partition_name,
                    &fixture.space_id,
                    fixture.query_vector.as_slice(),
                    5,
                    Some("benchmark"),
                    false,
                )?);
            }
            Ok(started.elapsed())
        }));
    }

    let started = Instant::now();
    barrier.wait();
    let mut thread_durations = Vec::with_capacity(workers);
    for handle in handles {
        thread_durations.push(
            handle
                .join()
                .map_err(|_| anyhow!("read concurrency worker panicked"))??,
        );
    }
    Ok(summarize_concurrency_run(
        started.elapsed(),
        workers * iterations,
        &thread_durations,
    ))
}

#[allow(clippy::needless_pass_by_value)]
fn run_simple_write_concurrency(
    fixture: Arc<ConcurrencyFixture>,
    workers: usize,
    iterations: usize,
) -> Result<ConcurrencyRun> {
    let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let fixture = fixture.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || -> Result<Duration> {
            barrier.wait();
            let started = Instant::now();
            for iteration in 0..iterations {
                let mut created = entity(
                    &fixture.space_id,
                    &format!("write-worker-{worker_id}-entity-{iteration}"),
                );
                created = fixture.db.create(created)?;
                created.display_name = format!("write-worker-{worker_id}-updated-{iteration}");
                let _ = fixture.db.replace(created)?;
            }
            Ok(started.elapsed())
        }));
    }

    let started = Instant::now();
    barrier.wait();
    let mut thread_durations = Vec::with_capacity(workers);
    for handle in handles {
        thread_durations.push(
            handle
                .join()
                .map_err(|_| anyhow!("simple write worker panicked"))??,
        );
    }
    Ok(summarize_concurrency_run(
        started.elapsed(),
        workers * iterations,
        &thread_durations,
    ))
}

#[allow(clippy::needless_pass_by_value)]
fn run_tx_write_concurrency(
    fixture: Arc<ConcurrencyFixture>,
    workers: usize,
    iterations: usize,
) -> Result<ConcurrencyRun> {
    let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let fixture = fixture.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || -> Result<Duration> {
            barrier.wait();
            let started = Instant::now();
            for iteration in 0..iterations {
                fixture.db.tx(|tx| {
                    let mut created = entity(
                        &fixture.space_id,
                        &format!("tx-worker-{worker_id}-entity-{iteration}"),
                    );
                    created = tx.create(created)?;
                    created.display_name = format!("tx-worker-{worker_id}-updated-{iteration}");
                    let _ = tx.replace(created)?;
                    Ok::<(), elephant_db::ElephantDbError>(())
                })?;
            }
            Ok(started.elapsed())
        }));
    }

    let started = Instant::now();
    barrier.wait();
    let mut thread_durations = Vec::with_capacity(workers);
    for handle in handles {
        thread_durations.push(
            handle
                .join()
                .map_err(|_| anyhow!("tx write worker panicked"))??,
        );
    }
    Ok(summarize_concurrency_run(
        started.elapsed(),
        workers * iterations,
        &thread_durations,
    ))
}

#[allow(clippy::needless_pass_by_value)]
fn run_mixed_concurrency(
    fixture: Arc<ConcurrencyFixture>,
    readers: usize,
    writers: usize,
    read_iterations: usize,
    write_iterations: usize,
) -> Result<ConcurrencyRun> {
    let barrier = Arc::new(std::sync::Barrier::new(readers + writers + 1));
    let mut handles = Vec::with_capacity(readers + writers);

    for worker_id in 0..readers {
        let fixture = fixture.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || -> Result<Duration> {
            barrier.wait();
            let started = Instant::now();
            for iteration in 0..read_iterations {
                let entity_id = &fixture.entity_ids
                    [(worker_id * read_iterations + iteration) % fixture.entity_ids.len()];
                black_box(
                    fixture
                        .db
                        .get::<Entity>(&fixture.space_id, entity_id, false)?,
                );
                black_box(
                    fixture
                        .db
                        .graph_neighbors(&fixture.space_id, &fixture.start_id, 2)?,
                );
                black_box(fixture.db.search_node_embeddings(
                    &fixture.partition_name,
                    &fixture.space_id,
                    fixture.query_vector.as_slice(),
                    5,
                    Some("benchmark"),
                    false,
                )?);
            }
            Ok(started.elapsed())
        }));
    }

    for worker_id in 0..writers {
        let fixture = fixture.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || -> Result<Duration> {
            barrier.wait();
            let started = Instant::now();
            for iteration in 0..write_iterations {
                let mut created = entity(
                    &fixture.space_id,
                    &format!("mixed-worker-{worker_id}-entity-{iteration}"),
                );
                created = fixture.db.create(created)?;
                created.display_name = format!("mixed-worker-{worker_id}-updated-{iteration}");
                let _ = fixture.db.replace(created)?;
            }
            Ok(started.elapsed())
        }));
    }

    let started = Instant::now();
    barrier.wait();
    let mut thread_durations = Vec::with_capacity(readers + writers);
    for handle in handles {
        thread_durations.push(
            handle
                .join()
                .map_err(|_| anyhow!("mixed concurrency worker panicked"))??,
        );
    }
    Ok(summarize_concurrency_run(
        started.elapsed(),
        readers * read_iterations + writers * write_iterations,
        &thread_durations,
    ))
}

fn summarize_concurrency_run(
    elapsed: Duration,
    logical_ops: usize,
    thread_durations: &[Duration],
) -> ConcurrencyRun {
    let total_ms = elapsed.as_secs_f64() * 1_000.0;
    let mean_thread_ms = if thread_durations.is_empty() {
        0.0
    } else {
        thread_durations
            .iter()
            .map(Duration::as_secs_f64)
            .sum::<f64>()
            * 1_000.0
            / f64::from(u32::try_from(thread_durations.len()).unwrap_or(u32::MAX))
    };
    let max_thread_ms = thread_durations
        .iter()
        .map(Duration::as_secs_f64)
        .fold(0.0_f64, f64::max)
        * 1_000.0;

    ConcurrencyRun {
        elapsed,
        logical_ops,
        ops_per_sec: if total_ms == 0.0 {
            0.0
        } else {
            usize_to_f64(logical_ops) / elapsed.as_secs_f64()
        },
        mean_thread_ms,
        max_thread_ms,
    }
}

async fn build_backend_graph_fixture(
    backend: StorageBackendKind,
    node_count: usize,
    edges_per_node: usize,
) -> Result<BackendGraphFixture> {
    let client = storage_client_for_backend(backend).await?;
    let entity_repo = EntityRepository::new(client.clone());
    let rel_repo = RelRepository::new(client.clone());
    let mut ids = Vec::with_capacity(node_count);

    for index in 0..node_count {
        let entity = entity("bench", &format!("entity-{index}"));
        ids.push(entity.base.id.clone());
        entity_repo.create(entity).await?;
    }

    for index in 0..node_count {
        for offset in 1..=edges_per_node {
            let target = &ids[(index + offset) % node_count];
            rel_repo
                .create(rel("bench", &ids[index], target, "linked"))
                .await?;
        }
    }

    Ok(BackendGraphFixture {
        client,
        start_id: ids[node_count / 2].clone(),
        node_count,
        edge_count: node_count * edges_per_node,
    })
}

async fn build_backend_doc_fixture(
    backend: StorageBackendKind,
    chunk_count: usize,
) -> Result<BackendDocFixture> {
    let client = storage_client_for_backend(backend).await?;
    let doc_repo = DocRepository::new(client.clone());
    let revision_repo = DocRevisionRepository::new(client.clone());
    let node_repo = DocNodeRepository::new(client.clone());

    let doc = doc("bench", "Doc Benchmark", "Document benchmark text.");
    let revision = revision("bench", &doc.base.id, "Document benchmark text.");
    doc_repo.create(doc).await?;
    revision_repo.create(revision.clone()).await?;

    let root = doc_node(
        "bench",
        &revision.base.id,
        None,
        NodeRole::Document,
        0,
        0,
        None,
        Some("benchmark"),
    );
    node_repo.create(root.clone()).await?;

    let section_count = usize::max(4, chunk_count / 200);
    let paragraphs_per_section = 4_usize;
    let mut paragraph_ids = Vec::new();
    let mut node_count_total = 1;

    for section_index in 0..section_count {
        let section = doc_node(
            "bench",
            &revision.base.id,
            Some(&root.base.id),
            NodeRole::Section,
            i32::try_from(section_index)?,
            1,
            None,
            Some("benchmark"),
        );
        let section_id = section.base.id.clone();
        node_repo.create(section).await?;
        node_count_total += 1;

        for paragraph_index in 0..paragraphs_per_section {
            let position = section_index * paragraphs_per_section + paragraph_index;
            let paragraph = doc_node(
                "bench",
                &revision.base.id,
                Some(&section_id),
                NodeRole::Paragraph,
                i32::try_from(position)?,
                2,
                None,
                Some("benchmark"),
            );
            paragraph_ids.push(paragraph.base.id.clone());
            node_repo.create(paragraph).await?;
            node_count_total += 1;
        }
    }

    let mut deep_chunk_id = None;
    for chunk_index in 0..chunk_count {
        let parent_id = &paragraph_ids[chunk_index % paragraph_ids.len()];
        let chunk = doc_node(
            "bench",
            &revision.base.id,
            Some(parent_id),
            NodeRole::Chunk,
            i32::try_from(chunk_index)?,
            3,
            Some(&format!(
                "Chunk {chunk_index} mentions Alice and the White Rabbit."
            )),
            Some("benchmark"),
        );
        deep_chunk_id = Some(chunk.base.id.clone());
        node_repo.create(chunk).await?;
        node_count_total += 1;
    }

    Ok(BackendDocFixture {
        client,
        revision_id: revision.base.id,
        deep_chunk_id: deep_chunk_id.ok_or_else(|| anyhow!("missing deep chunk id"))?,
        node_count: node_count_total,
        chunk_count,
        depth: 3,
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_alice_pipeline_once(backend: StorageBackendKind) -> Result<AliceRun> {
    let started_total = Instant::now();
    let client = storage_client_for_backend(backend).await?;
    let principal = test_principal("default");
    let dispatcher = create_dispatcher(client.clone(), principal);
    let dims = 256;

    let config_repo = SpaceConfigRepository::new(client.clone());
    config_repo.create(space_config("default", dims)).await?;

    let alice_text = read_alice_fixture()?;

    let started = Instant::now();
    ingest_doc_simple(
        &dispatcher,
        "Alice's Adventures in Wonderland",
        &alice_text,
        "fiction",
    )
    .await?;
    ingest_doc_simple(
        &dispatcher,
        "Chocolate Cake Recipe",
        "Mix butter, sugar, flour, cocoa powder, eggs, and vanilla. Bake until the cake is firm and fragrant.",
        "recipes",
    )
    .await?;
    let ingest_two_docs = started.elapsed();

    let doc_repo = DocRepository::new(client.clone());
    let docs = doc_repo.list("default", 10, None).await?;
    let doc_count = docs.len();

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));

    let started = Instant::now();
    let mut revision_ids = Vec::new();
    for doc in &docs {
        let render = execute_render_doc(&client, &registry, doc, Some("text/plain")).await?;
        revision_ids.push(render.revision_id);
    }
    let render_docs = started.elapsed();

    let node_repo = DocNodeRepository::new(client.clone());
    let started = Instant::now();
    let mut chunk_ids = Vec::new();
    for revision_id in &revision_ids {
        execute_build_doc_tree(&client, revision_id, &DocTreeConfig::default()).await?;
        let chunks = node_repo.find_chunks_by_revision(revision_id).await?;
        chunk_ids.extend(chunks.into_iter().map(|chunk| chunk.base.id));
    }
    let build_doc_trees = started.elapsed();

    let embed_worker = EmbedWorker::new(
        node_repo,
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );
    let started = Instant::now();
    let embed_result = embed_worker.embed_chunks("default", &chunk_ids).await?;
    let embed_chunks = started.elapsed();
    if embed_result.chunks_processed == 0 {
        bail!("expected embedded chunks for Alice scenario");
    }

    let started = Instant::now();
    let search_titles =
        search_titles(&dispatcher, "white rabbit rabbit hole wonderland", 5).await?;
    let search_once = started.elapsed();

    Ok(AliceRun {
        total: started_total.elapsed(),
        ingest_two_docs,
        render_docs,
        build_doc_trees,
        embed_chunks,
        search_once,
        chunk_count: chunk_ids.len(),
        doc_count,
        observed_top_title: search_titles.first().cloned(),
        search_result_count: search_titles.len(),
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_pipeline_overlap_once(backend: StorageBackendKind) -> Result<PipelineOverlapRun> {
    let started_total = Instant::now();
    let client = storage_client_for_backend(backend).await?;
    let dims = 256;
    let config_repo = SpaceConfigRepository::new(client.clone());
    config_repo.create(space_config("default", dims)).await?;

    let barrier = Arc::new(Barrier::new(5));
    let (stop_tx, stop_rx) = watch::channel(false);

    let writer_specs = vec![
        (
            "Alice's Adventures in Wonderland".to_string(),
            read_alice_fixture()?,
            "fiction".to_string(),
        ),
        (
            "The Wonderful Wizard of Oz".to_string(),
            read_oz_fixture()?,
            "fiction".to_string(),
        ),
    ];

    let mut writer_handles = Vec::with_capacity(writer_specs.len());
    for (title, text, collection) in writer_specs {
        let client = client.clone();
        let barrier = barrier.clone();
        writer_handles.push(tokio::spawn(async move {
            run_pipeline_doc(client, barrier, title, text, collection, dims).await
        }));
    }

    let search_queries = Arc::new([
        "white rabbit wonderland".to_string(),
        "emerald city yellow brick".to_string(),
    ]);
    let mut search_handles = Vec::with_capacity(2);
    for worker_id in 0..2_usize {
        let client = client.clone();
        let barrier = barrier.clone();
        let stop_rx = stop_rx.clone();
        let search_queries = search_queries.clone();
        search_handles.push(tokio::spawn(async move {
            run_overlap_searcher(client, barrier, stop_rx, search_queries, worker_id).await
        }));
    }

    barrier.wait().await;

    let mut doc_runs = Vec::new();
    for handle in writer_handles {
        doc_runs.push(handle.await??);
    }
    let _ = stop_tx.send(true);

    let mut search_durations = Vec::new();
    let mut non_empty_search_count = 0_usize;
    for handle in search_handles {
        let (durations, non_empty) = handle.await??;
        non_empty_search_count += non_empty;
        search_durations.extend(durations);
    }

    Ok(PipelineOverlapRun {
        total: started_total.elapsed(),
        docs: doc_runs,
        search_metrics: summarize_durations(&search_durations),
        search_count: search_durations.len(),
        non_empty_search_count,
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_pipeline_doc(
    client: StorageClient,
    barrier: Arc<Barrier>,
    title: String,
    text: String,
    collection: String,
    dims: usize,
) -> Result<PipelineDocRun> {
    let principal = test_principal("default");
    let dispatcher = Arc::new(create_dispatcher(client.clone(), principal));
    let node_repo = DocNodeRepository::new(client.clone());
    let revision_repo = DocRevisionRepository::new(client.clone());
    let embedding_repo = NodeEmbeddingRepository::new(client.clone(), PartitionManager::new());
    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let embed_worker = EmbedWorker::new(
        node_repo.clone(),
        revision_repo,
        embedding_repo,
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );

    barrier.wait().await;
    let started_total = Instant::now();

    let started = Instant::now();
    let doc = ingest_doc_simple_doc(&dispatcher, &title, &text, &collection).await?;
    let ingest_elapsed = started.elapsed();

    let started = Instant::now();
    let render = execute_render_doc(&client, &registry, &doc, Some("text/plain")).await?;
    let render_elapsed = started.elapsed();

    let started = Instant::now();
    execute_build_doc_tree(&client, &render.revision_id, &DocTreeConfig::default()).await?;
    let chunks = node_repo
        .find_chunks_by_revision(&render.revision_id)
        .await?;
    let chunk_ids = chunks
        .iter()
        .map(|chunk| chunk.base.id.clone())
        .collect::<Vec<_>>();
    let build_elapsed = started.elapsed();

    let started = Instant::now();
    let embed_result = embed_worker.embed_chunks("default", &chunk_ids).await?;
    let embed_elapsed = started.elapsed();
    if embed_result.chunks_processed == 0 {
        bail!("expected embedded chunks for overlap benchmark");
    }

    Ok(PipelineDocRun {
        title,
        ingest_ms: duration_ms(ingest_elapsed),
        render_ms: duration_ms(render_elapsed),
        build_doc_tree_ms: duration_ms(build_elapsed),
        embed_ms: duration_ms(embed_elapsed),
        total_ms: duration_ms(started_total.elapsed()),
        chunk_count: chunk_ids.len(),
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn run_overlap_searcher(
    client: StorageClient,
    barrier: Arc<Barrier>,
    mut stop_rx: watch::Receiver<bool>,
    queries: Arc<[String; 2]>,
    worker_id: usize,
) -> Result<(Vec<Duration>, usize)> {
    let principal = test_principal("default");
    let dispatcher = create_dispatcher(client, principal);
    let mut durations = Vec::new();
    let mut non_empty = 0_usize;
    let mut query_index = worker_id % queries.len();

    barrier.wait().await;

    loop {
        if *stop_rx.borrow() {
            break;
        }

        let started = Instant::now();
        let titles = search_titles(&dispatcher, &queries[query_index], 5).await?;
        durations.push(started.elapsed());
        if !titles.is_empty() {
            non_empty += 1;
        }
        query_index = (query_index + 1) % queries.len();

        if *stop_rx.borrow_and_update() {
            break;
        }
    }

    Ok((durations, non_empty))
}

async fn prepare_alice_fixture(backend: StorageBackendKind) -> Result<AliceFixture> {
    let client = storage_client_for_backend(backend).await?;
    let principal = test_principal("default");
    let dispatcher = Arc::new(create_dispatcher(client.clone(), principal));
    let dims = 256;

    let config_repo = SpaceConfigRepository::new(client.clone());
    config_repo.create(space_config("default", dims)).await?;

    let alice_text = read_alice_fixture()?;
    ingest_doc_simple(
        &dispatcher,
        "Alice's Adventures in Wonderland",
        &alice_text,
        "fiction",
    )
    .await?;
    ingest_doc_simple(
        &dispatcher,
        "Chocolate Cake Recipe",
        "Mix butter, sugar, flour, cocoa powder, eggs, and vanilla. Bake until the cake is firm and fragrant.",
        "recipes",
    )
    .await?;

    let doc_repo = DocRepository::new(client.clone());
    let docs = doc_repo.list("default", 10, None).await?;
    let doc_count = docs.len();

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let node_repo = DocNodeRepository::new(client.clone());
    let mut chunk_ids = Vec::new();

    for doc in &docs {
        let render = execute_render_doc(&client, &registry, doc, Some("text/plain")).await?;
        execute_build_doc_tree(&client, &render.revision_id, &DocTreeConfig::default()).await?;
        let chunks = node_repo
            .find_chunks_by_revision(&render.revision_id)
            .await?;
        chunk_ids.extend(chunks.into_iter().map(|chunk| chunk.base.id));
    }

    let embed_worker = EmbedWorker::new(
        node_repo,
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );
    let embed_result = embed_worker.embed_chunks("default", &chunk_ids).await?;
    if embed_result.chunks_processed == 0 {
        bail!("expected embedded chunks for Alice fixture");
    }

    let search_query = "white rabbit rabbit hole wonderland".to_string();
    let titles = search_titles(&dispatcher, &search_query, 5).await?;

    Ok(AliceFixture {
        dispatcher,
        chunk_count: chunk_ids.len(),
        doc_count,
        search_query,
        expected_top_title: "Alice's Adventures in Wonderland".to_string(),
        observed_top_title: titles.first().cloned(),
        search_result_count: titles.len(),
    })
}

async fn prepare_quality_fixture(backend: StorageBackendKind) -> Result<SearchFixture> {
    let corpus = vec![
        (
            "Alice's Adventures in Wonderland".to_string(),
            "Alice followed the White Rabbit down the rabbit hole into Wonderland. \
             There she met the Queen of Hearts, heard cries of 'Off with their heads!', \
             and watched a strange croquet game in the garden."
                .to_string(),
            "fiction".to_string(),
        ),
        (
            "Chocolate Cake Recipe".to_string(),
            "Mix butter, sugar, flour, cocoa powder, eggs, and vanilla. Bake until the cake is firm and fragrant."
                .to_string(),
            "recipes".to_string(),
        ),
        (
            "Rabbit Biology Primer".to_string(),
            "Rabbits are lagomorph herbivores with long ears. They often live in burrows and eat grasses and leafy plants."
                .to_string(),
            "biology".to_string(),
        ),
        (
            "SQL Query Basics".to_string(),
            "SQL queries select rows from a table, filter with WHERE clauses, and sort results with ORDER BY."
                .to_string(),
            "tech".to_string(),
        ),
    ];
    prepare_search_fixture(backend, &corpus).await
}

async fn evaluate_quality_fixture(
    fixture: &SearchFixture,
    expectations: &[SearchExpectation],
    mode: SearchMode,
) -> Result<Vec<QueryQualityResult>> {
    let mut results = Vec::with_capacity(expectations.len());

    for expectation in expectations {
        let started = Instant::now();
        let titles = match mode {
            SearchMode::Semantic => {
                search_titles(&fixture.dispatcher, &expectation.query, 5).await?
            }
            SearchMode::RawText => {
                search_titles_raw(&fixture.client, &expectation.query, 5).await?
            }
        };
        let search_ms = started.elapsed().as_secs_f64() * 1000.0;
        let top_title = titles
            .first()
            .cloned()
            .unwrap_or_else(|| "<none>".to_string());
        let reciprocal_rank = titles
            .iter()
            .position(|title| title == &expectation.expected_top_title)
            .map_or(0.0, |rank| {
                1.0 / f64::from(u32::try_from(rank + 1).unwrap_or(u32::MAX))
            });

        results.push(QueryQualityResult {
            query: expectation.query.clone(),
            query_kind: expectation.query_kind.clone(),
            search_mode: mode.label().to_string(),
            expected_top_title: expectation.expected_top_title.clone(),
            top_title: top_title.clone(),
            titles,
            reciprocal_rank,
            top1_match: top_title == expectation.expected_top_title,
            search_ms,
        });
    }

    Ok(results)
}

async fn prepare_search_fixture(
    backend: StorageBackendKind,
    corpus: &[(String, String, String)],
) -> Result<SearchFixture> {
    let client = storage_client_for_backend(backend).await?;
    let principal = test_principal("default");
    let dispatcher = Arc::new(create_dispatcher(client.clone(), principal));
    let dims = 256;

    let config_repo = SpaceConfigRepository::new(client.clone());
    config_repo.create(space_config("default", dims)).await?;

    for (title, text, collection) in corpus {
        ingest_doc_simple(&dispatcher, title, text, collection).await?;
    }

    let doc_repo = DocRepository::new(client.clone());
    let docs = doc_repo.list("default", 100, None).await?;
    let doc_count = docs.len();

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let node_repo = DocNodeRepository::new(client.clone());
    let mut chunk_ids = Vec::new();

    for doc in &docs {
        let render = execute_render_doc(&client, &registry, doc, Some("text/plain")).await?;
        execute_build_doc_tree(&client, &render.revision_id, &DocTreeConfig::default()).await?;
        let chunks = node_repo
            .find_chunks_by_revision(&render.revision_id)
            .await?;
        chunk_ids.extend(chunks.into_iter().map(|chunk| chunk.base.id));
    }

    let embed_worker = EmbedWorker::new(
        node_repo,
        DocRevisionRepository::new(client.clone()),
        NodeEmbeddingRepository::new(client.clone(), PartitionManager::new()),
        HashedEmbeddingProvider::new("hashed-bow-v1", dims),
    );
    let embed_result = embed_worker.embed_chunks("default", &chunk_ids).await?;
    if embed_result.chunks_processed == 0 {
        bail!("expected embedded chunks for prepared search dispatcher");
    }

    Ok(SearchFixture {
        dispatcher,
        client,
        doc_count,
        chunk_count: chunk_ids.len(),
    })
}

async fn extract_alice_graph_artifact() -> Result<AliceGraphArtifact> {
    let snapshot = extract_alice_graph_snapshot().await?;
    Ok(AliceGraphArtifact {
        space_config: fiction_space_config("default", 256),
        docs: snapshot.docs,
        entities: snapshot.entities,
        assertions: snapshot.assertions,
        rels: snapshot.rels,
    })
}

async fn extract_oz_graph_artifact() -> Result<AliceGraphArtifact> {
    let snapshot = extract_oz_graph_snapshot().await?;
    Ok(AliceGraphArtifact {
        space_config: fiction_space_config("default", 256),
        docs: snapshot.docs,
        entities: snapshot.entities,
        assertions: snapshot.assertions,
        rels: snapshot.rels,
    })
}

async fn extract_elephant_session_graph_artifact() -> Result<AliceGraphArtifact> {
    let snapshot = extract_elephant_session_graph_snapshot().await?;
    Ok(AliceGraphArtifact {
        space_config: technical_transcript_space_config("default", 256),
        docs: snapshot.docs,
        entities: snapshot.entities,
        assertions: snapshot.assertions,
        rels: snapshot.rels,
    })
}

async fn replay_alice_graph_artifact(
    artifact: &AliceGraphArtifact,
    backend: StorageBackendKind,
) -> Result<AliceQaFixture> {
    let client = storage_client_for_backend(backend).await?;
    SpaceConfigRepository::new(client.clone())
        .create(artifact.space_config.clone())
        .await?;

    let doc_repo = DocRepository::new(client.clone());
    for doc in &artifact.docs {
        let _ = doc_repo.create(doc.clone()).await?;
    }

    let entity_repo = EntityRepository::new(client.clone());
    for entity in &artifact.entities {
        let _ = entity_repo.create(entity.clone()).await?;
    }

    let assertion_repo = AssertionRepository::new(client.clone());
    for assertion in &artifact.assertions {
        let _ = assertion_repo.create(assertion.clone()).await?;
    }

    let rel_repo = RelRepository::new(client.clone());
    for rel in &artifact.rels {
        let _ = rel_repo.create(rel.clone()).await?;
    }

    Ok(AliceQaFixture {
        client,
        doc_count: artifact.docs.len(),
        entity_count: artifact.entities.len(),
        assertion_count: artifact.assertions.len(),
        rel_count: artifact.rels.len(),
    })
}

async fn evaluate_alice_question(
    fixture: &AliceQaFixture,
    question: &AliceQuestion,
) -> Result<AliceQaResult> {
    let started = Instant::now();
    let top_entities = search_entity_candidates(&fixture.client, question.question, 5).await?;
    let top_assertions =
        search_assertion_texts(&fixture.client, question.question, &top_entities, 8).await?;
    let search_ms = started.elapsed().as_secs_f64() * 1000.0;

    let mut searchable_text = String::new();
    for entity in &top_entities {
        searchable_text.push_str(&entity.display_name);
        searchable_text.push('\n');
    }
    for assertion in &top_assertions {
        searchable_text.push_str(assertion);
        searchable_text.push('\n');
    }
    let searchable_text = normalized_answer_text(&searchable_text);

    let found_answers: Vec<String> = question
        .expected_answers
        .iter()
        .filter(|answer| searchable_text.contains(&normalized_answer_text(answer)))
        .map(|answer| (*answer).to_string())
        .collect();

    Ok(AliceQaResult {
        question: question.question.to_string(),
        category: question.category.to_string(),
        expected_answers: question
            .expected_answers
            .iter()
            .map(|answer| (*answer).to_string())
            .collect(),
        found_answers: found_answers.clone(),
        correct: !found_answers.is_empty(),
        top_entities: top_entities
            .into_iter()
            .map(|entity| entity.display_name)
            .collect(),
        top_assertions: top_assertions
            .into_iter()
            .map(|text| summarize_snippet(&text, 220))
            .collect(),
        search_ms,
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn build_alice_graph_qa_agent(client: &StorageClient) -> Result<AliceGraphQaAgentHarness> {
    let tool_bridge = Arc::new(ElephantToolBridge::new(create_graph_qa_dispatcher(
        client.clone(),
    )));
    let model = qa_model();
    let system_prompt = concat!(
        "You answer questions strictly from an Elephant knowledge graph.\n",
        "Use only the provided tools; do not rely on prior knowledge.\n",
        "Use graph-centric tools first: search_entities, get_entity, get_entity_with_relations, ",
        "get_entity_context_bundle, search_assertions, and search_all.\n",
        "Reuse context from earlier questions in this same session when helpful.\n",
        "Prefer assertions and relations over document-chunk retrieval. Use search_docs only as a last resort.\n",
        "If the graph does not support a reliable answer, answer 'unknown'.\n",
        "Do not emit progress updates or commentary.\n",
        "Every final answer must be exactly one JSON object with keys: answer, supporting_facts, supporting_entity_ids, supporting_assertion_ids, supporting_doc_ids, supporting_quotes.\n",
        "Do not wrap the JSON in markdown."
    )
    .to_string();

    let factory = AgentFactory::new(
        std::env::temp_dir().join(format!("elephant-alice-qa-{}", Uuid::new_v4())),
    );
    let mut config = Config::default();
    config.compaction.auto_compact_threshold = 120_000;
    config.compaction.recent_turn_budget = 2;
    config.compaction.max_summary_tokens = 2_048;
    config.compaction.min_turns_between_compactions = 1;
    let (event_tx, event_rx) = mpsc::channel(4_096);
    let mut build = AgentBuildConfig::new(model.clone());
    build.max_tokens = Some(8_000);
    build.structured_output_retries = Some(0);
    build.keep_alive = true;
    build.comms_name = Some(format!("alice-graph-qa-{}", Uuid::new_v4()));
    build.budget_limits = Some(
        BudgetLimits::default()
            .with_max_tokens(10_000_000)
            .with_max_duration(Duration::from_secs(1_800))
            .with_max_tool_calls(200),
    );
    build.external_tools = Some(tool_bridge);
    build.override_shell = ToolCategoryOverride::from_override(Some(false));
    build.system_prompt = meerkat_core::SystemPromptOverride::Set(system_prompt);
    build.event_tx = Some(event_tx);

    let mut agent = Box::pin(factory.build_agent(build, &config))
        .await
        .with_context(|| format!("failed to build host-mode graph QA agent for model `{model}`"))?;
    let comms = agent
        .comms_arc()
        .context("host-mode graph QA agent is missing a comms runtime")?;
    let session_id = agent.session().id().clone();
    let host_task = tokio::spawn(async move {
        // Box the agent run future: on meerkat 0.7.5 it exceeds clippy's
        // large_futures threshold (~16 KB), which `-D warnings` rejects.
        Box::pin(agent.run(ContentInput::Text(String::new())))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    });

    Ok(AliceGraphQaAgentHarness {
        comms,
        session_id,
        host_task,
        event_rx,
        previous_input_tokens: 0,
        previous_output_tokens: 0,
    })
}

async fn evaluate_alice_question_with_agent(
    harness: &mut AliceGraphQaAgentHarness,
    question: &AliceQuestion,
) -> Result<AliceAgentQaResult> {
    let prompt = format!(
        concat!(
            "You are answering a question from an Elephant knowledge graph built from the current benchmark dataset.\n",
            "Use the tools to inspect entities, relations, and assertions before answering.\n",
            "Re-use anything useful from earlier questions in this conversation, but re-check facts when needed.\n",
            "If the graph does not support a reliable answer, answer 'unknown'.\n",
            "Do not stop after partial exploration; your final response must be a single JSON object with keys ",
            "answer, supporting_facts, supporting_entity_ids, supporting_assertion_ids, supporting_doc_ids, supporting_quotes.\n",
            "Do not wrap the JSON in markdown.\n\n",
            "Question: {}"
        ),
        question.question
    );

    let started = Instant::now();
    let _receipt = harness
        .comms
        .send(CommsCommand::Input {
            session_id: harness.session_id.clone(),
            body: prompt,
            blocks: None,
            handling_mode: HandlingMode::Queue,
            source: InputSource::Rpc,
            stream: InputStreamMode::None,
            allow_self_session: true,
        })
        .await
        .context("failed to inject host-mode graph QA input")?;
    let run = wait_for_host_mode_turn(&mut harness.event_rx)
        .await
        .context("host-mode graph QA turn failed")?;
    let answer_ms = started.elapsed().as_secs_f64() * 1000.0;
    let payload = parse_alice_agent_answer(&run.result_text)?;

    let answer_lower = normalized_answer_text(&payload.answer);
    let correct = question
        .expected_answers
        .iter()
        .any(|expected| answer_lower.contains(&normalized_answer_text(expected)));

    let input_tokens_u64 = run
        .usage_input_tokens
        .saturating_sub(harness.previous_input_tokens);
    let output_tokens_u64 = run
        .usage_output_tokens
        .saturating_sub(harness.previous_output_tokens);
    harness.previous_input_tokens = run.usage_input_tokens;
    harness.previous_output_tokens = run.usage_output_tokens;
    let total_tokens_u64 = input_tokens_u64.saturating_add(output_tokens_u64);
    let cumulative_total_tokens_u64 = run
        .usage_input_tokens
        .saturating_add(run.usage_output_tokens);
    let tool_ms = u64_to_f64(run.trace.tool_ms);
    let estimated_model_ms = (answer_ms - tool_ms).max(0.0);

    Ok(AliceAgentQaResult {
        question: question.question.to_string(),
        category: question.category.to_string(),
        expected_answers: question
            .expected_answers
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        answer: payload.answer,
        supporting_facts: payload.supporting_facts,
        supporting_entity_ids: payload.supporting_entity_ids,
        supporting_assertion_ids: payload.supporting_assertion_ids,
        supporting_doc_ids: payload.supporting_doc_ids,
        supporting_quotes: payload.supporting_quotes,
        correct,
        answer_ms,
        estimated_model_ms,
        tool_ms,
        turns: run.turns,
        tool_calls: run.tool_calls,
        tool_events: run.trace.tool_events,
        turn_events: run.trace.turn_events,
        input_tokens: usize::try_from(input_tokens_u64).unwrap_or(usize::MAX),
        output_tokens: usize::try_from(output_tokens_u64).unwrap_or(usize::MAX),
        total_tokens: usize::try_from(total_tokens_u64).unwrap_or(usize::MAX),
        cumulative_total_tokens: usize::try_from(cumulative_total_tokens_u64).unwrap_or(usize::MAX),
    })
}

async fn wait_for_host_mode_turn(
    event_rx: &mut mpsc::Receiver<AgentEvent>,
) -> Result<HostModeRunOutcome> {
    let mut trace = AgentRunTrace::default();
    let timeout = Duration::from_secs(180);
    let started = Instant::now();

    loop {
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            bail!("timed out waiting for host-mode turn completion");
        }

        let event = tokio::time::timeout(remaining, event_rx.recv())
            .await
            .context("timed out waiting for host-mode graph QA events")?
            .context("host-mode graph QA event stream closed unexpectedly")?;

        match event {
            AgentEvent::ToolExecutionCompleted { duration_ms, .. } => {
                trace.tool_ms = trace.tool_ms.saturating_add(duration_ms);
                trace.tool_events = trace.tool_events.saturating_add(1);
            }
            AgentEvent::TurnCompleted { .. } => {
                trace.turn_events = trace.turn_events.saturating_add(1);
            }
            AgentEvent::RunCompleted { result, usage, .. } => {
                return Ok(HostModeRunOutcome {
                    result_text: result,
                    usage_input_tokens: usage.input_tokens,
                    usage_output_tokens: usage.output_tokens,
                    turns: trace.turn_events,
                    tool_calls: trace.tool_events,
                    trace,
                });
            }
            AgentEvent::RunFailed { error_report, .. } => {
                bail!("host-mode run failed: {}", error_report.message);
            }
            _ => {}
        }
    }
}

fn parse_alice_agent_answer(text: &str) -> Result<AliceAgentQaAnswer> {
    if let Ok(parsed) = serde_json::from_str::<AliceAgentQaAnswer>(text) {
        return Ok(parsed);
    }

    let Some(start) = text.find('{') else {
        bail!("graph QA host-mode response did not contain JSON");
    };
    let Some(end) = text.rfind('}') else {
        bail!("graph QA host-mode response contained unterminated JSON");
    };
    serde_json::from_str::<AliceAgentQaAnswer>(&text[start..=end])
        .context("failed to decode graph QA JSON response")
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn search_entity_candidates(
    client: &StorageClient,
    query: &str,
    limit: usize,
) -> Result<Vec<EntitySearchCandidate>> {
    let repo = EntityRepository::new(client.clone());
    let hits = repo.bm25_search_entities("default", query, limit).await?;
    let mut entities = Vec::with_capacity(hits.len());
    for hit in hits {
        let Some(entity) = repo.get("default", &hit.entity_id).await? else {
            continue;
        };
        entities.push(EntitySearchCandidate {
            id: entity.base.id,
            display_name: entity.display_name,
        });
    }
    Ok(entities)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn search_assertion_texts(
    client: &StorageClient,
    query: &str,
    entities: &[EntitySearchCandidate],
    limit: usize,
) -> Result<Vec<String>> {
    let repo = AssertionRepository::new(client.clone());
    let mut seen = HashSet::new();
    let mut texts = Vec::new();

    let direct = repo
        .search(
            &elephant_storage::SearchCriteria::new()
                .with_space_id("default")
                .with_query(query)
                .with_limit(limit.saturating_mul(2)),
        )
        .await?;

    for assertion in direct.results {
        let text = format_assertion(&assertion);
        if seen.insert(text.clone()) {
            texts.push(text);
        }
        if texts.len() >= limit {
            return Ok(texts);
        }
    }

    for entity in entities {
        let related = repo
            .find_by_subject_entity("default", &entity.id, limit)
            .await?;
        for assertion in related {
            let text = format_assertion(&assertion);
            if seen.insert(text.clone()) {
                texts.push(text);
            }
            if texts.len() >= limit {
                return Ok(texts);
            }
        }
    }

    Ok(texts)
}

fn format_assertion(assertion: &Assertion) -> String {
    match assertion.object_text.as_deref() {
        Some(object_text) if !object_text.trim().is_empty() => {
            format!(
                "{} {} {}",
                assertion.subject_text, assertion.predicate_raw, object_text
            )
        }
        _ => format!("{} {}", assertion.subject_text, assertion.predicate_raw),
    }
}

fn summarize_snippet(text: &str, limit: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= limit {
        return normalized;
    }

    normalized.chars().take(limit).collect::<String>() + "..."
}

fn fiction_space_config(space_id: &str, dims: usize) -> SpaceConfig {
    let mut config = space_config(space_id, dims);
    config.purpose = Some(
        "Fiction novel knowledge graph — characters, relationships, locations, and story events"
            .to_string(),
    );
    config.skill = Some("fiction".to_string());
    config
}

fn technical_transcript_space_config(space_id: &str, dims: usize) -> SpaceConfig {
    let mut config = space_config(space_id, dims);
    config.purpose = Some(
        "Engineering session transcript about Elephant embedded-db design, SurrealDB benchmarking, WASM planning, and Meerkat integration"
            .to_string(),
    );
    config.skill = Some("technical-docs".to_string());
    config
}

async fn ingest_doc_simple(
    dispatcher: &ToolDispatcher,
    title: &str,
    text: &str,
    collection: &str,
) -> Result<()> {
    let response = dispatcher
        .execute(
            "ingest_doc_simple",
            json!({
                "title": title,
                "text": text,
                "collection": collection,
            }),
        )
        .await?;
    let decoded: ToolResponse<Value> = serde_json::from_value(response)?;
    if !decoded.ok {
        bail!("ingest_doc_simple failed for `{title}`");
    }
    Ok(())
}

async fn ingest_doc_simple_doc(
    dispatcher: &ToolDispatcher,
    title: &str,
    text: &str,
    collection: &str,
) -> Result<Doc> {
    let response = dispatcher
        .execute(
            "ingest_doc_simple",
            json!({
                "title": title,
                "text": text,
                "collection": collection,
            }),
        )
        .await?;
    let decoded: ToolResponse<Value> = serde_json::from_value(response)?;
    if !decoded.ok {
        bail!("ingest_doc_simple failed for `{title}`");
    }
    let doc_value = decoded
        .data
        .and_then(|value| value.get("data").cloned())
        .ok_or_else(|| anyhow!("ingest_doc_simple returned no document for `{title}`"))?;
    Ok(serde_json::from_value(doc_value)?)
}

async fn search_titles(dispatcher: &ToolDispatcher, query: &str, k: usize) -> Result<Vec<String>> {
    let response = dispatcher
        .execute(
            "search_docs",
            json!({
                "query": query,
                "k": k,
            }),
        )
        .await?;
    let decoded: ToolResponse<Value> = serde_json::from_value(response)?;
    extract_titles_from_response(&decoded)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain benchmark handle ownership and drop order through each original operation"
)]
async fn search_titles_raw(client: &StorageClient, query: &str, k: usize) -> Result<Vec<String>> {
    let repo = DocRepository::new(client.clone());
    let result = repo
        .search(
            &elephant_storage::SearchCriteria::new()
                .with_space_id("default")
                .with_query(query)
                .with_limit(k),
        )
        .await?;
    Ok(result.results.into_iter().map(|doc| doc.title).collect())
}

fn extract_titles_from_response(decoded: &ToolResponse<Value>) -> Result<Vec<String>> {
    let items = decoded
        .data
        .as_ref()
        .and_then(|value| value.get("items"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("search_docs did not return an items array"))?;

    items
        .iter()
        .map(|value| {
            value
                .get("doc")
                .and_then(|value| value.get("data"))
                .and_then(|value| value.get("title"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| anyhow!("search_docs item did not include a document title"))
        })
        .collect()
}

fn read_alice_fixture() -> Result<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../scenarios/fixtures/alice_in_wonderland.txt");
    fs::read_to_string(&path)
        .with_context(|| format!("failed to read Alice fixture at `{}`", path.display()))
}

fn read_oz_fixture() -> Result<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../e2e/fixtures/wizard_of_oz.txt");
    let text = fs::read_to_string(&path)
        .with_context(|| format!("failed to read Oz fixture at `{}`", path.display()))?;
    Ok(normalize_fixture_text(&text))
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn normalize_fixture_text(input: &str) -> String {
    input
        .trim_start_matches('\u{feff}')
        .replace(['“', '”'], "\"")
        .replace(['‘', '’'], "'")
        .replace(['—', '–'], "-")
}

async fn storage_client_for_backend(backend: StorageBackendKind) -> Result<StorageClient> {
    match backend {
        StorageBackendKind::ElephantDb => StorageClient::new(StorageConfig {
            backend,
            ..StorageConfig::default()
        })
        .await
        .map_err(Into::into),
        StorageBackendKind::SurrealDb => {
            let id = Uuid::new_v4().simple().to_string();
            let fixed_db = std::env::var("SURREALDB_BENCH_FIXED").is_ok_and(|value| value == "1");
            let url = std::env::var("SURREALDB_URL").unwrap_or_else(|_| "mem://".to_string());
            let username = std::env::var("SURREALDB_USER").unwrap_or_else(|_| "root".to_string());
            let password = std::env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".to_string());
            let namespace = std::env::var("SURREALDB_NAMESPACE").map_or_else(
                |_| format!("bench_{id}"),
                |value| {
                    if fixed_db {
                        value
                    } else {
                        format!("{value}_bench_{id}")
                    }
                },
            );
            let database = std::env::var("SURREALDB_DATABASE").map_or_else(
                |_| format!("bench_{id}"),
                |value| {
                    if fixed_db {
                        value
                    } else {
                        format!("{value}_bench_{id}")
                    }
                },
            );
            let client = StorageClient::new(StorageConfig {
                backend,
                url,
                username,
                password,
                namespace,
                database,
                ..StorageConfig::default()
            })
            .await?;
            apply_schema(&client)
                .await
                .map_err(|err| anyhow!("failed to apply schema for surreal-db benchmark: {err}"))?;
            Ok(client)
        }
    }
}

fn test_principal(space_id: &str) -> Principal {
    let mut scopes = HashSet::new();
    let _ = scopes.insert("*".to_string());

    Principal {
        space_id: space_id.to_string(),
        scopes,
        clearances: vec![Clearance {
            policy_id: "default".to_string(),
            max_level: i32::MAX,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

fn create_graph_qa_dispatcher(storage: StorageClient) -> ToolDispatcher {
    let principal = test_principal("default");
    let authz = Arc::new(McpAuthzService::new_with_skip_auth(true));
    let context = ToolContext::with_authz(storage.clone(), authz, principal);
    let mut dispatcher = create_agent_dispatcher_with_context(context);
    dispatcher.register(GetEntityWithRelationsHandler::new(storage));
    dispatcher
}

fn qa_model() -> String {
    std::env::var("QA_MODEL")
        .or_else(|_| std::env::var("EXTRACTION_MODEL"))
        .unwrap_or_else(|_| "claude-haiku-4-5".to_string())
}

fn space_config(space_id: &str, dims: usize) -> SpaceConfig {
    let mut base = base_for(TableName::SpaceConfig, space_id);
    base.created_by = "benchmark".to_string();
    SpaceConfig {
        processing: None,
        base,
        embedding_provider: "hashed".to_string(),
        embedding_model: "hashed-bow-v1".to_string(),
        embedding_dims: i32::try_from(dims).unwrap_or(256),
        purpose: Some("Benchmark space".to_string()),
        skill: None,
    }
}

fn base_for(table: TableName, space_id: &str) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        space_id.to_string(),
        "benchmark".to_string(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: Vec::new(),
        handling: Vec::new(),
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;
    base
}

fn entity(space_id: &str, name: &str) -> Entity {
    Entity::new(
        base_for(TableName::Entity, space_id),
        EntityType::new("benchmark"),
        name.to_string(),
    )
}

fn rel(space_id: &str, subject: &RecordId, object: &RecordId, rel_type: &str) -> Rel {
    Rel {
        base: base_for(TableName::Rel, space_id),
        subject: Link::new(subject.clone()),
        object: Link::new(object.clone()),
        rel_type: rel_type.to_string(),
        status: elephant_types::records::rel::ClaimStatus::Approved,
        confidence: 0.9,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    }
}

fn doc(space_id: &str, title: &str, text: &str) -> Doc {
    Doc {
        base: base_for(TableName::Doc, space_id),
        title: title.to_string(),
        text: text.to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: format!("rendition-{title}"),
        metadata: Value::Object(serde_json::Map::new()),
    }
}

fn revision(space_id: &str, doc_id: &RecordId, text: &str) -> DocRevision {
    DocRevision {
        base: base_for(TableName::DocRevision, space_id),
        doc: Link::new(doc_id.clone()),
        text: text.to_string(),
        text_hash: "hash".to_string(),
        renderer: RendererMetadata {
            name: "benchmark".to_string(),
            version: "1".to_string(),
            config_hash: None,
        },
        supersedes: None,
        manifest: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    }
}

#[allow(clippy::too_many_arguments)]
fn doc_node(
    space_id: &str,
    revision_id: &RecordId,
    parent: Option<&RecordId>,
    role: NodeRole,
    position: i32,
    depth: i32,
    text: Option<&str>,
    collection: Option<&str>,
) -> DocNode {
    DocNode {
        base: base_for(TableName::DocNode, space_id),
        revision: Link::new(revision_id.clone()),
        role,
        span: TextSpan::new(0, 10),
        node_key: format!("{role}-{position}-{depth}"),
        text: text.map(str::to_string),
        parent: parent.cloned().map(Link::new),
        position,
        depth,
        embedding_model: None,
        collection: collection.map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn infer_llm_provider_supports_current_defaults() {
        assert_eq!(
            infer_llm_provider("gpt-5.2"),
            Some(BenchmarkLlmProvider::OpenAi)
        );
        assert_eq!(
            infer_llm_provider("claude-haiku-4-5"),
            Some(BenchmarkLlmProvider::Anthropic)
        );
        assert_eq!(
            infer_llm_provider("gemini-2.5-pro"),
            Some(BenchmarkLlmProvider::Gemini)
        );
    }

    #[test]
    fn missing_llm_benchmark_credentials_reports_both_default_requirements() {
        let missing =
            missing_llm_benchmark_credentials_with("gpt-5.2", "claude-haiku-4-5", |_| None);
        assert!(
            missing.is_ok(),
            "provider inference should succeed: {missing:?}"
        );
        let Ok(missing) = missing else {
            return;
        };
        assert_eq!(missing.len(), 2);
        assert!(missing.iter().any(|entry| entry.contains("OPENAI_API_KEY")));
        assert!(missing
            .iter()
            .any(|entry| entry.contains("ANTHROPIC_API_KEY")));
    }

    #[test]
    fn missing_llm_benchmark_credentials_dedupes_shared_provider_requirements() {
        let missing = missing_llm_benchmark_credentials_with("gpt-5.2", "gpt-5.2", |_| None);
        assert!(
            missing.is_ok(),
            "provider inference should succeed: {missing:?}"
        );
        let Ok(missing) = missing else {
            return;
        };
        assert_eq!(missing.len(), 1);
        assert!(missing[0].contains("OPENAI_API_KEY"));
    }

    #[test]
    fn missing_llm_benchmark_credentials_accepts_either_gemini_key() {
        let missing =
            missing_llm_benchmark_credentials_with("gemini-2.5-pro", "claude-haiku-4-5", |name| {
                match name {
                    "GOOGLE_API_KEY" | "ANTHROPIC_API_KEY" => Some("configured".to_string()),
                    _ => None,
                }
            });
        assert!(
            missing.is_ok(),
            "provider inference should succeed: {missing:?}"
        );
        let Ok(missing) = missing else {
            return;
        };
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_llm_benchmark_credentials_errors_for_unknown_model_provider() {
        let error = missing_llm_benchmark_credentials_with("custom-model", "gpt-5.2", |_| None);
        assert!(
            error.is_err(),
            "unknown provider should fail fast: {error:?}"
        );
        let Err(error) = error else {
            return;
        };
        assert!(error
            .to_string()
            .contains("could not infer provider credentials"));
    }

    #[test]
    fn transcript_inputs_available_under_detects_expected_roots() {
        let temp = tempdir();
        assert!(temp.is_ok(), "temp dir should be created: {temp:?}");
        let Ok(temp) = temp else {
            return;
        };
        assert!(!transcript_inputs_available_under(temp.path()));

        let sessions = temp.path().join(".codex").join("sessions");
        let created = std::fs::create_dir_all(&sessions);
        assert!(
            created.is_ok(),
            "sessions dir should be created: {created:?}"
        );
        let Ok(()) = created else {
            return;
        };
        let written = std::fs::write(sessions.join("sample.json"), "{}");
        assert!(
            written.is_ok(),
            "sample session should be written: {written:?}"
        );
        assert!(transcript_inputs_available_under(temp.path()));
    }

    #[test]
    fn local_transcripts_in_all_is_opt_in() {
        std::env::remove_var("ELEPHANT_ENABLE_LOCAL_TRANSCRIPTS");
        assert!(!include_local_transcripts_in_all());

        std::env::set_var("ELEPHANT_ENABLE_LOCAL_TRANSCRIPTS", "true");
        assert!(include_local_transcripts_in_all());
        std::env::remove_var("ELEPHANT_ENABLE_LOCAL_TRANSCRIPTS");
    }

    #[test]
    fn llm_qa_in_all_is_opt_in() {
        std::env::remove_var("ELEPHANT_ENABLE_LLM_QA");
        assert!(!include_llm_qa_in_all());

        std::env::set_var("ELEPHANT_ENABLE_LLM_QA", "true");
        assert!(include_llm_qa_in_all());
        std::env::remove_var("ELEPHANT_ENABLE_LLM_QA");
    }
}
