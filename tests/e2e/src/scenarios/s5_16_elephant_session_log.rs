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

use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use elephant_embeddings::FakeEmbeddingProvider;
use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_agent_dispatcher_with_context,
    create_dispatcher, ToolContext, ToolResponse,
};
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, AnthropicLlmAdapter, ExtractionContext,
    ExtractionResult, Extractor,
};
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_extract_assertions, execute_plan_extraction, work_kinds,
    DocTreeConfig, ExtractionPlanConfig,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, DocRepository, EntityRepository, RelRepository, Repository,
    SearchRepository, SpaceConfigRepository, StorageClient, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::space::SpaceConfig;
use elephant_types::records::work::{
    Determinism, PluginManifest, RetryPolicy, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::{Assertion, Doc, Entity, Rel};
use elephant_types::{Link, RecordId};

use meerkat::{AgentLlmClient, AgentToolDispatcher, ToolDef};
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ToolCallView, ToolResult};

use crate::Harness;

#[derive(Debug, Clone)]
pub struct ElephantSessionGraphSnapshot {
    pub docs: Vec<Doc>,
    pub entities: Vec<Entity>,
    pub assertions: Vec<Assertion>,
    pub rels: Vec<Rel>,
}

fn test_principal(space_id: &str) -> Principal {
    let mut scopes = HashSet::new();
    scopes.insert("*".to_string());

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

struct ElephantToolBridge {
    dispatcher: elephant_mcp::ToolDispatcher,
    tool_defs: Arc<[Arc<ToolDef>]>,
}

impl ElephantToolBridge {
    fn new(dispatcher: elephant_mcp::ToolDispatcher) -> Self {
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
        let args: Value =
            serde_json::from_str(call.args.get()).unwrap_or(Value::Object(Default::default()));

        match self.dispatcher.execute(call.name, args).await {
            Ok(result) => {
                let content = serde_json::to_string(&result).unwrap_or_default();
                Ok(ToolDispatchOutcome::from(ToolResult::new(
                    call.id.to_string(),
                    content,
                    false,
                )))
            }
            Err(e) => Ok(ToolDispatchOutcome::from(ToolResult::new(
                call.id.to_string(),
                format!("Tool error: {e}"),
                true,
            ))),
        }
    }
}

struct InProcessAgenticExtractor {
    inner: AgenticExtractor,
    tool_bridge: Arc<ElephantToolBridge>,
    llm: Arc<dyn AgentLlmClient>,
}

#[async_trait]
impl Extractor for InProcessAgenticExtractor {
    fn manifest(&self) -> &PluginManifest {
        self.inner.manifest()
    }

    async fn extract(
        &self,
        ctx: ExtractionContext,
    ) -> Result<ExtractionResult, elephant_pipeline::error::PipelineError> {
        self.inner
            .run_agent(ctx, self.tool_bridge.clone(), self.llm.clone())
            .await
    }
}

async fn create_work_run(
    storage: StorageClient,
    space_id: &str,
    item_id: &RecordId,
) -> anyhow::Result<RecordId> {
    let repo = WorkRunRepository::new(storage);
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let work_run = WorkRun {
        base: BaseFields::new(
            work_run_id.clone(),
            space_id.to_string(),
            "scenario".to_string(),
        ),
        work_item: Link::new(item_id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "scenario-worker".to_string(),
        worker_version: "test".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    repo.create(work_run).await?;
    Ok(work_run_id)
}

fn create_in_process_extractor(storage: &StorageClient) -> InProcessAgenticExtractor {
    let model = std::env::var("EXTRACTION_MODEL").unwrap_or_else(|_| "gpt-5.2".to_string());
    eprintln!("[session] Using model: {model}");

    let principal = test_principal("default");
    let authz = Arc::new(McpAuthzService::new_with_skip_auth(true));
    let context = ToolContext::with_authz(storage.clone(), authz, principal);
    let dispatcher = create_agent_dispatcher_with_context(context);
    let tool_bridge = Arc::new(ElephantToolBridge::new(dispatcher));

    let dummy_api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_else(|_| "unused".to_string());
    let llm: Arc<dyn AgentLlmClient> = Arc::new(
        AnthropicLlmAdapter::new(dummy_api_key, model.clone())
            .expect("Failed to create LLM adapter"),
    );

    let config =
        AgenticExtractorConfig::new("unused://in-process", model).with_connect_on_init(false);

    InProcessAgenticExtractor {
        inner: AgenticExtractor::new(config),
        tool_bridge,
        llm,
    }
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

fn set_env_var_scoped(key: &'static str, value: &std::path::Path) -> EnvVarGuard {
    let previous = std::env::var_os(key);
    std::env::set_var(key, value);
    EnvVarGuard { key, previous }
}

fn configure_skills_dir() -> EnvVarGuard {
    let skills_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/../../../skills/extraction")
        .canonicalize()
        .expect("skills/extraction dir must exist");
    set_env_var_scoped("PIPELINE_SKILLS_DIR", &skills_dir)
}

fn transcript_space_config() -> SpaceConfig {
    let base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "default".to_string(),
        "scenario".to_string(),
    );
    SpaceConfig {
        base,
        embedding_provider: "fake".to_string(),
        embedding_model: "test-model".to_string(),
        embedding_dims: 8,
        purpose: Some(
            "Engineering conversation transcript about Elephant, embedded databases, SurrealDB, WASM architecture, benchmarks, and Meerkat integration"
                .to_string(),
        ),
        skill: Some("technical-docs".to_string()),
    }
}

fn normalize_fixture_text(text: &str) -> String {
    text.trim_start_matches('\u{feff}')
        .replace(['“', '”'], "\"")
        .replace(['‘', '’'], "'")
        .replace(['—', '–'], "-")
}

fn render_codex_history_message(value: &serde_json::Value) -> Option<String> {
    let payload = value.get("payload")?;
    if payload.get("type")?.as_str()? != "message" {
        return None;
    }

    let role = payload
        .get("role")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let content = payload
        .get("content")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    let text = content
        .iter()
        .filter_map(|item| {
            item.get("text")
                .and_then(serde_json::Value::as_str)
                .or_else(|| item.get("input_text").and_then(serde_json::Value::as_str))
                .or_else(|| item.get("output_text").and_then(serde_json::Value::as_str))
        })
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    if text.is_empty() {
        None
    } else {
        Some(format!("{}: {text}", role.to_ascii_uppercase()))
    }
}

fn read_raw_codex_session_log() -> anyhow::Result<String> {
    if let Ok(path) = env::var("ELEPHANT_SESSION_QA_TEXT_PATH") {
        return std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!("failed to read ELEPHANT_SESSION_QA_TEXT_PATH `{path}`: {e}")
        });
    }

    let target_session_id = env::var("ELEPHANT_SESSION_QA_SESSION_ID").map_err(|_| {
        anyhow::anyhow!(
            "ELEPHANT_SESSION_QA_SESSION_ID is required for session QA when ELEPHANT_SESSION_QA_TEXT_PATH is not set"
        )
    })?;
    let path = env::var("ELEPHANT_SESSION_QA_HISTORY_PATH")
        .map(PathBuf::from)
        .map_err(|_| {
            anyhow::anyhow!(
                "ELEPHANT_SESSION_QA_HISTORY_PATH is required when using ELEPHANT_SESSION_QA_SESSION_ID"
            )
        })?;
    let raw = std::fs::read_to_string(&path)?;
    let mut lines = Vec::new();
    let mut archived_session_matches = false;
    let archived_path_matches = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains(target_session_id.as_str()));

    for line in raw.lines() {
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(session_id) = value.get("session_id").and_then(serde_json::Value::as_str) {
            if session_id != target_session_id.as_str() {
                continue;
            }
            let text = value
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .unwrap_or("");
            if text.is_empty() {
                continue;
            }
            let ts = value
                .get("ts")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default();
            lines.push(format!("[ts={ts}] User: {text}"));
            continue;
        }

        if value.get("type").and_then(serde_json::Value::as_str) == Some("session_meta") {
            archived_session_matches = value
                .get("payload")
                .and_then(|payload| payload.get("id"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|session_id| session_id == target_session_id.as_str());
            continue;
        }

        if !archived_session_matches && !archived_path_matches {
            continue;
        }

        let Some(message) = render_codex_history_message(&value) else {
            continue;
        };
        let ts = value
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        lines.push(format!("[ts={ts}] {message}"));
    }

    if lines.is_empty() {
        anyhow::bail!(
            "no raw Codex history entries found for session {} in {}",
            target_session_id,
            path.display()
        );
    }

    Ok(lines.join("\n\n"))
}

pub async fn run_elephant_session_extraction_flow(harness: &Harness) -> anyhow::Result<()> {
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let _skills_dir_guard = configure_skills_dir();

    let config_repo = SpaceConfigRepository::new(harness.client.clone());
    config_repo.create(transcript_space_config()).await?;
    eprintln!("[session] Space configured with technical-docs extraction skill");

    let raw_text = read_raw_codex_session_log()?;
    let session_text = normalize_fixture_text(&raw_text);

    let ingest_params = serde_json::json!({
        "title": "Elephant embedded-db design and benchmark session transcript",
        "text": session_text,
        "collection": "transcript",
    });
    let response = dispatcher
        .execute("ingest_doc_simple", ingest_params)
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "ingest_doc_simple should succeed");
    eprintln!("[session] Transcript ingested successfully");

    let extractor = create_in_process_extractor(&harness.client);
    eprintln!("\n--- Driving transcript extraction pipeline (AgenticExtractor, in-process) ---\n");
    drive_pipeline(harness.client.clone(), &extractor, 200).await?;

    Ok(())
}

pub async fn collect_elephant_session_graph_snapshot(
    client: &StorageClient,
) -> anyhow::Result<ElephantSessionGraphSnapshot> {
    Ok(ElephantSessionGraphSnapshot {
        docs: DocRepository::new(client.clone())
            .list("default", 1000, None)
            .await?,
        entities: EntityRepository::new(client.clone())
            .list("default", 5000, None)
            .await?,
        assertions: AssertionRepository::new(client.clone())
            .list("default", 20_000, None)
            .await?,
        rels: RelRepository::new(client.clone())
            .list("default", 20_000, None)
            .await?,
    })
}

pub async fn extract_elephant_session_graph_snapshot(
) -> anyhow::Result<ElephantSessionGraphSnapshot> {
    let harness = Harness::new().await?;
    run_elephant_session_extraction_flow(&harness).await?;
    collect_elephant_session_graph_snapshot(&harness.client).await
}

async fn drive_pipeline(
    storage: StorageClient,
    extractor: &InProcessAgenticExtractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    let space_id = "default";
    let item_repo = WorkItemRepository::new(storage.clone());
    let doc_repo = DocRepository::new(storage.clone());
    let dag = elephant_pipeline::scheduler::DagExecutor::new(storage.clone());

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));

    for step in 0..max_steps {
        let runnable = item_repo
            .find_by_status(space_id, WorkItemStatus::Runnable)
            .await?;
        if runnable.is_empty() {
            eprintln!("[session] Pipeline quiesced after {step} steps");
            break;
        }

        for item in runnable {
            let item_id = item.base.id.clone();

            let _ = item_repo
                .update_status(space_id, &item_id, WorkItemStatus::Running)
                .await;

            let work_run_id = create_work_run(storage.clone(), space_id, &item_id).await?;

            eprintln!("[session] step={step} kind={} id={}", item.kind, item_id);

            match item.kind.as_str() {
                work_kinds::RENDER_DOC_KIND => {
                    let doc_id = item
                        .payload
                        .get("doc_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("render_doc missing doc_id payload"))?;
                    let doc_id = RecordId::from_str(doc_id).map_err(|e| anyhow::anyhow!("{e}"))?;
                    let doc = doc_repo
                        .get(space_id, &doc_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("doc not found: {doc_id}"))?;

                    let render_result = work_kinds::execute_render_doc(
                        &storage,
                        &registry,
                        &doc,
                        Some("text/plain"),
                    )
                    .await?;
                    execute_build_doc_tree(
                        &storage,
                        &render_result.revision_id,
                        &DocTreeConfig::default(),
                    )
                    .await?;

                    let plan_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space_id.to_string(),
                            "scenario".to_string(),
                        ),
                        kind: work_kinds::PLAN_EXTRACTION_KIND.to_string(),
                        payload: serde_json::json!({
                            "revision_id": render_result.revision_id.to_string(),
                            "policy_version": "default",
                            "extractor_id": "elephant.extractor.agentic",
                            "allow_raw_quotes": false,
                            "ingest_mode": "graph",
                        }),
                        status: WorkItemStatus::Runnable,
                        priority: item.priority,
                        idempotency_key: format!("plan_extraction:{}", render_result.revision_id),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };
                    let _ = item_repo.create(plan_item).await?;
                }
                work_kinds::PLAN_EXTRACTION_KIND => {
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("plan_extraction missing revision_id"))?;
                    let revision_id = RecordId::from_str(revision_id)?;
                    let policy_version = item
                        .payload
                        .get("policy_version")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default");

                    let config = ExtractionPlanConfig::default();
                    let result = execute_plan_extraction(
                        &storage,
                        &revision_id,
                        policy_version,
                        &config,
                        &work_run_id,
                        space_id,
                        "scenario",
                    )
                    .await?;

                    eprintln!(
                        "[session] Extraction plan: {} nodes selected",
                        result.plan.node_selections.len()
                    );
                    for selection in result.plan.node_selections {
                        let extract_item = WorkItem {
                            base: BaseFields::new(
                                RecordId::new_v4(TableName::WorkItem),
                                space_id.to_string(),
                                "scenario".to_string(),
                            ),
                            kind: work_kinds::EXTRACT_ASSERTIONS_KIND.to_string(),
                            payload: serde_json::json!({
                                "node_id": selection.node_id.to_string(),
                                "revision_id": revision_id.to_string(),
                                "extractor_id": "elephant.extractor.agentic",
                                "allow_raw_quotes": false,
                                "estimated_tokens": selection.estimated_tokens,
                                "estimated_cost_euros": 0.0,
                                "ingest_mode": "graph",
                            }),
                            status: WorkItemStatus::Runnable,
                            priority: item.priority,
                            idempotency_key: format!(
                                "extract_assertions:{}:{}",
                                revision_id, selection.node_id
                            ),
                            attempts: 0,
                            lease_expires_at: None,
                            leased_by: None,
                            run_after: None,
                            last_error: None,
                        };
                        let _ = item_repo.create(extract_item).await?;
                    }
                }
                work_kinds::EXTRACT_ASSERTIONS_KIND => {
                    let node_id = item
                        .payload
                        .get("node_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing node_id"))?;
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing revision_id"))?;
                    let node_id = RecordId::from_str(node_id)?;
                    let revision_id = RecordId::from_str(revision_id)?;

                    let result = execute_extract_assertions(
                        &storage,
                        extractor,
                        &node_id,
                        &revision_id,
                        &work_run_id,
                        space_id,
                        "scenario",
                        None,
                    )
                    .await?;

                    eprintln!(
                        "[session] Extracted {} assertions (artifact={})",
                        result.assertions_extracted, result.artifact_id
                    );
                }
                work_kinds::TM_DETECT_CONFLICTS_KIND
                | work_kinds::TM_RESOLVE_SLOT_KIND
                | work_kinds::TM_COMMIT_KIND
                | work_kinds::TM_RECOMPUTE_KIND
                | work_kinds::EMBED_CHUNKS_KIND
                | work_kinds::BUILD_DOC_TREE_KIND
                | work_kinds::WORK_KIND_FRESHNESS_SWEEP
                | work_kinds::experimental::validate_artifacts::KIND
                | work_kinds::experimental::commit_artifacts::KIND => {}
                other => {
                    eprintln!("[session] WARNING: unhandled work kind: {other}");
                }
            }

            item_repo
                .update_status(space_id, &item_id, WorkItemStatus::Done)
                .await?;
            let _ = dag.on_item_completed(space_id, &item_id).await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::read_raw_codex_session_log;
    use std::ffi::OsString;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{name}-{}.txt", Uuid::new_v4()))
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn set_env_var_scoped(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> EnvVarGuard {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        EnvVarGuard { key, previous }
    }

    fn clear_env_var_scoped(key: &'static str) -> EnvVarGuard {
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
        EnvVarGuard { key, previous }
    }

    #[test]
    fn test_read_raw_codex_session_log_prefers_text_path() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("elephant-session-qa");
        fs::write(&path, "raw transcript fixture").expect("write fixture");

        let _session_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_SESSION_ID");
        let _history_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_HISTORY_PATH");
        let _text_guard = set_env_var_scoped("ELEPHANT_SESSION_QA_TEXT_PATH", &path);

        let result = read_raw_codex_session_log().expect("text path should be used");
        assert_eq!(result, "raw transcript fixture");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_read_raw_codex_session_log_requires_session_id_without_text_path() {
        let _guard = env_lock().lock().expect("env lock");
        let _text_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_TEXT_PATH");
        let _session_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_SESSION_ID");
        let _history_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_HISTORY_PATH");

        let error = read_raw_codex_session_log().expect_err("missing session id should fail");
        assert!(error
            .to_string()
            .contains("ELEPHANT_SESSION_QA_SESSION_ID is required"));
    }

    #[test]
    fn test_read_raw_codex_session_log_requires_history_path_in_session_mode() {
        let _guard = env_lock().lock().expect("env lock");
        let _text_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_TEXT_PATH");
        let _session_guard = set_env_var_scoped("ELEPHANT_SESSION_QA_SESSION_ID", "session-123");
        let _history_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_HISTORY_PATH");

        let error = read_raw_codex_session_log().expect_err("missing history path should fail");
        assert!(error
            .to_string()
            .contains("ELEPHANT_SESSION_QA_HISTORY_PATH is required"));
    }

    #[test]
    fn test_read_raw_codex_session_log_parses_archived_codex_session_format() {
        let _guard = env_lock().lock().expect("env lock");
        let path = unique_temp_path("elephant-session-qa-archived");
        fs::write(
            &path,
            concat!(
                "{\"timestamp\":\"2026-01-26T14:36:05.095Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-123\"}}\n",
                "{\"timestamp\":\"2026-01-26T14:36:07.441Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"Hello Elephant\"}]}}\n",
                "{\"timestamp\":\"2026-01-26T14:36:08.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hi there\"}]}}\n"
            ),
        )
        .expect("write archived fixture");

        let _text_guard = clear_env_var_scoped("ELEPHANT_SESSION_QA_TEXT_PATH");
        let _session_guard = set_env_var_scoped("ELEPHANT_SESSION_QA_SESSION_ID", "session-123");
        let _history_guard = set_env_var_scoped("ELEPHANT_SESSION_QA_HISTORY_PATH", &path);

        let result = read_raw_codex_session_log().expect("archived session should parse");
        assert!(result.contains("USER: Hello Elephant"));
        assert!(result.contains("ASSISTANT: Hi there"));

        let _ = fs::remove_file(path);
    }
}
