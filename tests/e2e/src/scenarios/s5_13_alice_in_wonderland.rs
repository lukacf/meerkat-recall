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
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_agent_dispatcher_with_context,
    create_dispatcher, ToolContext, ToolResponse,
};
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, AnthropicLlmAdapter, ExtractionContext,
    ExtractionResult, Extractor,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, DocRepository, EntityRepository, RelRepository, Repository,
    SpaceConfigRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::space::SpaceConfig;
use elephant_types::records::work::PluginManifest;
use elephant_types::records::{Assertion, Doc, Entity, Rel};
use elephant_types::RecordId;

use meerkat::{AgentLlmClient, AgentToolDispatcher, ToolDef};
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ToolCallView, ToolResult};

use crate::Harness;

#[derive(Debug, Clone)]
pub struct AliceGraphSnapshot {
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

// ─────────────────────────────────────────────────────────────────────────────
// Bridge: elephant_mcp::ToolDispatcher → meerkat::AgentToolDispatcher
// ─────────────────────────────────────────────────────────────────────────────

/// Adapter bridging elephant_mcp's ToolDispatcher to meerkat's AgentToolDispatcher trait.
///
/// This allows the AgenticExtractor's agent loop to call Elephant's MCP tools
/// directly in-process, without an HTTP server or MCP protocol.
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

// ─────────────────────────────────────────────────────────────────────────────
// InProcessAgenticExtractor: wraps AgenticExtractor.run_agent with in-process tools
// ─────────────────────────────────────────────────────────────────────────────

/// An Extractor that uses the AgenticExtractor's agent loop with in-process
/// tool dispatch, bypassing the HTTP/MCP layer entirely.
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

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline driver
// ─────────────────────────────────────────────────────────────────────────────

/// Creates an in-process AgenticExtractor with Elephant MCP tools wired directly.
///
/// Model is read from `EXTRACTION_MODEL` env var, defaulting to `gpt-5.2`.
fn create_in_process_extractor(storage: &StorageClient) -> InProcessAgenticExtractor {
    let model = std::env::var("EXTRACTION_MODEL").unwrap_or_else(|_| "gpt-5.2".to_string());
    eprintln!("[alice] Using model: {model}");

    // Create agent-profile tool dispatcher (read-only tools for extraction)
    let principal = test_principal("default");
    let authz = Arc::new(McpAuthzService::new_with_skip_auth(true));
    let context = ToolContext::with_authz(storage.clone(), authz, principal);
    let dispatcher = create_agent_dispatcher_with_context(context);
    let tool_bridge = Arc::new(ElephantToolBridge::new(dispatcher));

    // LLM is resolved by AgentFactory from model name + env API keys.
    // We still need a dummy AgentLlmClient for the InProcessAgenticExtractor
    // signature, but run_agent ignores it.
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

fn configure_fiction_skills_dir() -> EnvVarGuard {
    let skills_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../skills/extraction")
        .canonicalize()
        .expect("skills/extraction dir must exist");
    set_env_var_scoped("PIPELINE_SKILLS_DIR", &skills_dir)
}

fn fiction_space_config() -> SpaceConfig {
    let base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "default".to_string(),
        "scenario".to_string(),
    );
    SpaceConfig {
        processing: None,
        base,
        embedding_provider: "fake".to_string(),
        embedding_model: "test-model".to_string(),
        embedding_dims: 8,
        purpose: Some(
            "Fiction novel knowledge graph — characters, relationships, locations, and story events"
                .to_string(),
        ),
        skill: Some("fiction".to_string()),
    }
}

pub async fn run_alice_extraction_flow(harness: &Harness) -> anyhow::Result<()> {
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));

    let _skills_dir_guard = configure_fiction_skills_dir();

    let config_repo = SpaceConfigRepository::new(harness.client.clone());
    config_repo.create(fiction_space_config()).await?;
    eprintln!("[alice] Space configured with fiction extraction skill");

    let book_text = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("alice_in_wonderland.txt"),
    )?;

    eprintln!("\n============================================================");
    eprintln!("  INGESTING: Alice's Adventures in Wonderland (full book)");
    eprintln!(
        "  Text length: {} chars / ~{} tokens (est.)",
        book_text.len(),
        book_text.len() / 4
    );
    eprintln!("============================================================\n");

    let ingest_params = serde_json::json!({
        "title": "Alice's Adventures in Wonderland by Lewis Carroll",
        "text": book_text,
    });
    let response = dispatcher
        .execute("ingest_doc_simple", ingest_params)
        .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    assert!(decoded.ok, "ingest_doc_simple should succeed");
    eprintln!("[alice] Document ingested successfully");

    let extractor = create_in_process_extractor(&harness.client);
    eprintln!("\n--- Driving extraction pipeline (AgenticExtractor, in-process) ---\n");
    drive_pipeline(harness.client.clone(), &extractor, 200).await?;

    Ok(())
}

pub async fn collect_alice_graph_snapshot(
    client: &StorageClient,
) -> anyhow::Result<AliceGraphSnapshot> {
    Ok(AliceGraphSnapshot {
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

pub async fn extract_alice_graph_snapshot() -> anyhow::Result<AliceGraphSnapshot> {
    let harness = Harness::new().await?;
    run_alice_extraction_flow(&harness).await?;
    collect_alice_graph_snapshot(&harness.client).await
}

/// Drives the pipeline to completion, executing every work kind the system produces.
async fn drive_pipeline(
    storage: StorageClient,
    extractor: &InProcessAgenticExtractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    crate::pipeline_driver::drive_pipeline(&storage, "default", extractor, max_steps).await
}

#[tokio::test]
#[ignore = "requires live LLM credentials; run cargo test-e2e-live"]
async fn scenario_s5_13_alice_in_wonderland_live_llm() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));
    run_alice_extraction_flow(&harness).await?;

    // --- Report: Entities ---
    let entity_repo = EntityRepository::new(harness.client.clone());
    let entities = entity_repo.list("default", 1000, None).await?;
    eprintln!("\n============================================================");
    eprintln!("  ENTITIES: {} extracted", entities.len());
    eprintln!("============================================================");
    for e in &entities {
        eprintln!(
            "  [{}] {} (type={})",
            e.base.id, e.display_name, e.entity_type
        );
    }

    // --- Report: Assertions ---
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let assertions = assertion_repo.list("default", 1000, None).await?;
    eprintln!("\n============================================================");
    eprintln!("  ASSERTIONS: {} extracted", assertions.len());
    eprintln!("============================================================");
    for a in &assertions {
        let subj = &a.subject_text;
        let obj = a.object_text.as_deref().unwrap_or_default();
        let obj_part = if obj.is_empty() {
            String::new()
        } else {
            format!(" -> {obj}")
        };
        eprintln!(
            "  {} | {}{} (status={:?}, conf={:.2})",
            subj, a.predicate_raw, obj_part, a.resolution_status, a.confidence
        );
    }

    // --- Report: Relationships ---
    let rel_repo = RelRepository::new(harness.client.clone());
    let rels = rel_repo.list("default", 1000, None).await?;
    eprintln!("\n============================================================");
    eprintln!("  RELATIONSHIPS: {}", rels.len());
    eprintln!("============================================================");
    for r in &rels {
        eprintln!(
            "  {:?} --[{}]--> {:?} (conf={:.2})",
            r.subject, r.rel_type, r.object, r.confidence
        );
    }

    // --- MCP search spot checks ---
    for query in &[
        "Alice",
        "White Rabbit",
        "Queen",
        "Cheshire Cat",
        "Mad Hatter",
        "Dinah",
    ] {
        let search_params = serde_json::json!({ "query": query, "limit": 10 });
        let response = dispatcher.execute("search_entities", search_params).await?;
        let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
        let names: Vec<&str> = decoded
            .data
            .as_ref()
            .and_then(|v| v.get("items"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|i| i.get("display_name").and_then(|v| v.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        eprintln!("  search('{query}'): {} results {:?}", names.len(), names);
    }

    // --- Verify outbox events (Phase 2 wiring) ---
    let event_repo = elephant_storage::SystemEventRepository::new(harness.client.clone());
    let outbox_events = event_repo.get_after_seq("default", 0, 100).await?;
    eprintln!("\n============================================================");
    eprintln!(
        "  OUTBOX EVENTS: {} written by artifact commit",
        outbox_events.len()
    );
    eprintln!("============================================================");
    for evt in &outbox_events {
        let payload: serde_json::Value =
            serde_json::from_value(evt.payload.clone()).unwrap_or_default();
        let record_count = payload
            .get("records")
            .and_then(|v| v.as_array())
            .map_or(0, |a| a.len());
        eprintln!(
            "  seq={} type={} records={}",
            evt.seq, evt.event_type, record_count
        );
    }

    assert!(
        !outbox_events.is_empty(),
        "artifact commit should have written outbox events for truth maintenance"
    );

    // --- Run truth maintenance on outbox events ---
    let tm_storage = elephant_pipeline::truth_maintenance::RepositoryTruthMaintenanceStorage::new(
        harness.client.clone(),
    );
    let tm_result =
        elephant_pipeline::truth_maintenance::execute_tm_detect_conflicts_from_outbox_events(
            &outbox_events,
            &tm_storage,
        )
        .await?;
    eprintln!("\n============================================================");
    eprintln!("  TRUTH MAINTENANCE:");
    eprintln!("    Events processed: {}", tm_result.events_processed);
    eprintln!("    Conflicts detected: {}", tm_result.conflicts_detected);
    eprintln!("    Direct commits: {}", tm_result.direct_commits.len());
    eprintln!(
        "    Conflict groups: {}",
        tm_result.conflict_group_ids.len()
    );
    eprintln!("============================================================");

    // Execute direct commits (first-claim, no conflict)
    for commit in &tm_result.direct_commits {
        elephant_pipeline::truth_maintenance::execute_tm_commit(
            &commit.resolution,
            &commit.input,
            "default",
            &tm_storage,
        )
        .await?;
    }

    // Check truth_slots were created
    let truth_slot_count: Vec<serde_json::Value> = harness
        .client
        .query_bind(
            "SELECT count() FROM truth_slot WHERE space_id = $space_id GROUP ALL",
            serde_json::json!({ "space_id": "default" }),
        )
        .await?
        .take(0)?;
    let slot_count = truth_slot_count
        .first()
        .and_then(|v| v.get("count"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    eprintln!("\n  Truth slots created: {slot_count}");

    // --- Summary ---
    eprintln!("\n============================================================");
    eprintln!(
        "  FINAL: {} entities, {} assertions, {} relationships, {} truth_slots, {} outbox_events",
        entities.len(),
        assertions.len(),
        rels.len(),
        slot_count,
        outbox_events.len()
    );
    eprintln!("============================================================\n");

    assert!(
        entities.len() >= 3,
        "expected at least a few characters, got {} entities",
        entities.len()
    );

    assert!(
        slot_count > 0 || tm_result.direct_commits.is_empty(),
        "if there are direct commits, truth_slots should be created"
    );

    Ok(())
}
