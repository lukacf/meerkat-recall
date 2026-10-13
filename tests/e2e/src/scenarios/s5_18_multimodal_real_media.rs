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
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Value};

use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_agent_dispatcher_with_context, ToolContext,
};
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, ExtractionContext, ExtractionResult, Extractor,
};
use elephant_pipeline::renderer::multimodal::MultimodalRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_extract_assertions, execute_plan_extraction,
    execute_render_doc, DocTreeConfig, ExtractionPlanConfig,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    DocRepository, DocRevisionRepository, Repository, SourceRepository, StorageClient,
    WorkArtifactRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::{Doc, DocMediaAttachment, DocMediaKind, DocMediaLocation};
use elephant_types::records::source::{Source, SourceLifecycleStatus, SourceType};
use elephant_types::records::work::{PluginManifest, WorkRun, WorkRunMetrics};
use elephant_types::{Link, RecordId};

use meerkat::{AgentError, AgentLlmClient, AgentToolDispatcher, ToolDef};
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ToolCallView, ToolResult};

use crate::Harness;

const EXAMPLE_MEDIA_ROOT: &str = "/tmp/elephant-example-media";
const EXAMPLE_SKILLS_ROOT: &str = "/tmp/elephant-example-skills";

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

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl Into<OsString>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value.into());
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            std::env::set_var(self.key, value);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn gemini_credentials_present() -> bool {
    [
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "GOOGLE_GENERATIVE_AI_API_KEY",
    ]
    .iter()
    .any(|key| {
        std::env::var(key)
            .ok()
            .is_some_and(|value| !value.trim().is_empty())
    })
}

fn first_file_with_extension(root: &Path, extension: &str) -> Option<PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = first_file_with_extension(&path, extension) {
                return Some(found);
            }
            continue;
        }
        if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case(extension))
        {
            return Some(path);
        }
    }
    None
}

fn first_probeable_video(root: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = first_probeable_video(&path) {
                return Some(found);
            }
            continue;
        }
        if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case("mp4"))
            && video_duration_ms_probeable(&path)
        {
            return Some(path);
        }
    }
    None
}

fn video_duration_ms_probeable(path: &Path) -> bool {
    let path_str = path.display().to_string();

    let ffprobe_ok = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            &path_str,
        ])
        .output()
        .ok()
        .is_some_and(|output| {
            output.status.success() && !String::from_utf8_lossy(&output.stdout).trim().is_empty()
        });
    if ffprobe_ok {
        return true;
    }

    Command::new("mdls")
        .args(["-raw", "-name", "kMDItemDurationSeconds", &path_str])
        .output()
        .ok()
        .is_some_and(|output| {
            output.status.success() && {
                let value = String::from_utf8_lossy(&output.stdout);
                let trimmed = value.trim();
                !trimmed.is_empty() && trimmed != "(null)"
            }
        })
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

struct NoopLlmClient;

#[async_trait]
impl AgentLlmClient for NoopLlmClient {
    async fn stream_response(
        &self,
        _messages: &[meerkat::Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&meerkat_core::lifecycle::run_primitive::ProviderParamsOverride>,
    ) -> Result<meerkat::LlmStreamResult, AgentError> {
        Err(AgentError::ConfigError(
            "NoopLlmClient should not be called; AgentFactory resolves the provider-specific client"
                .to_string(),
        ))
    }

    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::from_name("noop")
    }

    fn model(&self) -> &str {
        "noop"
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

async fn create_work_run(storage: StorageClient, space_id: &str) -> anyhow::Result<RecordId> {
    let repo = WorkRunRepository::new(storage);
    let work_run = WorkRun {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkRun),
            space_id.to_string(),
            "scenario".to_string(),
        ),
        work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
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
    let work_run = repo.create(work_run).await?;
    Ok(work_run.base.id)
}

fn create_in_process_extractor(storage: &StorageClient, model: &str) -> InProcessAgenticExtractor {
    let principal = test_principal("default");
    let authz = Arc::new(McpAuthzService::new_with_skip_auth(true));
    let context = ToolContext::with_authz(storage.clone(), authz, principal);
    let dispatcher = create_agent_dispatcher_with_context(context);
    let tool_bridge = Arc::new(ElephantToolBridge::new(dispatcher));

    let config = AgenticExtractorConfig::new("unused://in-process", model.to_string())
        .with_connect_on_init(false);

    InProcessAgenticExtractor {
        inner: AgenticExtractor::new(config),
        tool_bridge,
        llm: Arc::new(NoopLlmClient),
    }
}

fn make_real_media_doc(path: &Path, kind: DocMediaKind, media_type: &str) -> Doc {
    Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "default".to_string(),
            "scenario".to_string(),
        ),
        title: format!(
            "Example {}",
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("media")
        ),
        text: String::new(),
        sidecar_text: Some("Example evidence sample".to_string()),
        source: None,
        media_attachments: vec![DocMediaAttachment {
            attachment_id: "att-live-1".to_string(),
            kind,
            media_type: media_type.to_string(),
            location: DocMediaLocation::Path {
                path: path.display().to_string(),
            },
            label: path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string),
            size_bytes: std::fs::metadata(path).ok().map(|meta| meta.len()),
            duration_ms: None,
            sha256: None,
        }],
        rendition_key: format!("rendition-{}", uuid::Uuid::new_v4()),
        metadata: json!({
            "extraction_skill": "extraction/example-observations",
            "extraction_purpose": "extract observational evidence",
            "extraction_domain": "example_observation"
        }),
    }
}

fn source_timestamp_for_path(path: &Path) -> Option<chrono::DateTime<Utc>> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    Some(chrono::DateTime::<Utc>::from(modified))
}

fn make_real_media_source(path: &Path, kind: DocMediaKind) -> Source {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "default".to_string(),
            "scenario".to_string(),
        ),
        match kind {
            DocMediaKind::Image => SourceType::DeviceFrame,
            DocMediaKind::Video => SourceType::DeviceVideo,
        },
        format!("hash-{}", uuid::Uuid::new_v4()),
    );
    source.lifecycle_status = SourceLifecycleStatus::GraphReady;
    source.path = Some(path.display().to_string());
    source.timestamp = source_timestamp_for_path(path);
    source
}

fn non_scaffolding_assertion_count(assertions: &[Value]) -> usize {
    let banned_subjects = [
        "Observed participants",
        "Angel hints",
        "Concern level",
        "Observation type",
        "screen analysis",
    ];

    assertions
        .iter()
        .filter(|item| {
            let subject = item
                .get("subject_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let predicate = item
                .get("predicate_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let object_text = item
                .get("object_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            !subject.is_empty()
                && !predicate.is_empty()
                && (!object_text.is_empty() || item.get("object_literal").is_some())
                && !banned_subjects
                    .iter()
                    .any(|candidate| subject.eq_ignore_ascii_case(candidate))
        })
        .count()
}

fn grounded_non_scaffolding_assertion_count(assertions: &[Value]) -> usize {
    let banned_subjects = [
        "Observed participants",
        "Angel hints",
        "Concern level",
        "Observation type",
        "screen analysis",
    ];

    assertions
        .iter()
        .filter(|item| {
            let subject = item
                .get("subject_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let predicate = item
                .get("predicate_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let object_text = item
                .get("object_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let evidence_start = item
                .get("evidence_start")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let evidence_end = item
                .get("evidence_end")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let quote_hash = item
                .get("quote_hash")
                .and_then(Value::as_str)
                .unwrap_or_default();

            !subject.is_empty()
                && !predicate.is_empty()
                && (!object_text.is_empty() || item.get("object_literal").is_some())
                && !banned_subjects
                    .iter()
                    .any(|candidate| subject.eq_ignore_ascii_case(candidate))
                && (evidence_end > evidence_start || !quote_hash.is_empty())
        })
        .count()
}

fn event_timed_assertion_count(assertions: &[Value]) -> usize {
    assertions
        .iter()
        .filter(|item| {
            item.get("event_title")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
                && item
                    .get("source_event_time")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
        })
        .count()
}

fn multimodal_benchmark_report_path(kind: DocMediaKind) -> PathBuf {
    std::env::temp_dir().join(format!(
        "elephant-multimodal-{}-benchmark-summary.json",
        match kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        }
    ))
}

async fn run_real_media_case(
    model: &str,
    path: &Path,
    kind: DocMediaKind,
    media_type: &str,
) -> anyhow::Result<Value> {
    let _skills_guard = EnvVarGuard::set("PIPELINE_SKILLS_DIR", EXAMPLE_SKILLS_ROOT);
    let harness = Harness::new().await?;
    let storage = harness.client.clone();
    let doc_repo = DocRepository::new(storage.clone());
    let source_repo = SourceRepository::new(storage.clone());
    let artifact_repo = WorkArtifactRepository::new(storage.clone());

    let source = source_repo
        .create(make_real_media_source(path, kind))
        .await?;
    let mut doc = make_real_media_doc(path, kind, media_type);
    doc.source = Some(Link::new(source.base.id.clone()));
    let doc = doc_repo.create(doc).await?;
    assert!(
        !doc.media_attachments.is_empty(),
        "persisted real-media doc should retain media attachments"
    );

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(MultimodalRenderer::for_live_model(model)));

    let render_started = std::time::Instant::now();
    let render_result = execute_render_doc(&storage, &registry, &doc, Some(media_type)).await?;
    let render_ms = render_started.elapsed().as_millis();
    if !render_result.success {
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let revision = revision_repo
            .get("default", &render_result.revision_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("rendered revision {} missing", render_result.revision_id)
            })?;
        anyhow::bail!(
            "render_doc did not produce an active revision: status={:?} manifest_present={} text_len={} doc_media_attachments={}",
            revision.status,
            revision.manifest.is_some(),
            revision.text.len(),
            doc.media_attachments.len()
        );
    }

    let tree_started = std::time::Instant::now();
    let tree_result = execute_build_doc_tree(
        &storage,
        &render_result.revision_id,
        &DocTreeConfig::default(),
    )
    .await?;
    let tree_ms = tree_started.elapsed().as_millis();

    let work_run_id = create_work_run(storage.clone(), "default").await?;
    let plan_started = std::time::Instant::now();
    let plan_result = execute_plan_extraction(
        &storage,
        &render_result.revision_id,
        "default",
        &ExtractionPlanConfig::default(),
        &work_run_id,
        "default",
        "scenario",
    )
    .await?;
    let plan_ms = plan_started.elapsed().as_millis();
    assert!(
        !plan_result.plan.node_selections.is_empty(),
        "expected at least one extraction node for real media case"
    );

    let extractor = create_in_process_extractor(&storage, model);
    let selection = plan_result
        .plan
        .node_selections
        .first()
        .cloned()
        .expect("real media selection");

    let extract_started = std::time::Instant::now();
    let extract_result = execute_extract_assertions(
        &storage,
        &extractor,
        &selection.node_id,
        &render_result.revision_id,
        &work_run_id,
        "default",
        "scenario",
        None,
    )
    .await?;
    let extract_ms = extract_started.elapsed().as_millis();
    assert!(
        extract_result.assertions_extracted > 0,
        "expected assertions from real multimodal extraction"
    );

    let artifact = artifact_repo
        .get("default", &extract_result.artifact_id)
        .await?
        .expect("knowledge artifact");
    let assertions = artifact
        .data
        .get("assertions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let sensible = non_scaffolding_assertion_count(&assertions);
    assert!(
        sensible > 0,
        "expected at least one non-scaffolding assertion, got: {assertions:#?}"
    );
    let grounded = grounded_non_scaffolding_assertion_count(&assertions);
    assert!(
        grounded > 0,
        "expected at least one grounded non-scaffolding assertion, got: {assertions:#?}"
    );
    let timed_events = event_timed_assertion_count(&assertions);

    let report = json!({
        "model": model,
        "kind": match kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        },
        "file": path.display().to_string(),
        "render_ms": render_ms,
        "tree_ms": tree_ms,
        "plan_ms": plan_ms,
        "extract_ms": extract_ms,
        "chunk_nodes": tree_result.chunk_nodes,
        "planned_nodes": plan_result.plan.node_selections.len(),
        "assertions_extracted": extract_result.assertions_extracted,
        "grounded_assertions": grounded,
        "event_timed_assertions": timed_events,
        "tokens_used": extract_result.tokens_used,
        "sample_assertions": assertions.iter().take(5).cloned().collect::<Vec<_>>()
    });

    let report_path = std::env::temp_dir().join(format!(
        "elephant-multimodal-{}-{}.json",
        match kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        },
        model.replace('/', "-")
    ));
    std::fs::write(&report_path, serde_json::to_vec_pretty(&report)?)
        .expect("write benchmark report");
    eprintln!(
        "wrote multimodal benchmark report to {}",
        report_path.display()
    );

    Ok(report)
}

async fn run_real_media_matrix(
    path: &Path,
    kind: DocMediaKind,
    media_type: &str,
) -> anyhow::Result<()> {
    let mut reports = Vec::new();
    let mut failures = Vec::new();

    for model in [
        "gemini-3-flash-preview",
        "gemini-3.1-flash-lite-preview",
        "gemini-3.1-pro-preview",
    ] {
        match run_real_media_case(model, path, kind, media_type).await {
            Ok(report) => {
                eprintln!(
                    "{} benchmark {model}: {}",
                    match kind {
                        DocMediaKind::Image => "frame",
                        DocMediaKind::Video => "video",
                    },
                    serde_json::to_string_pretty(&report)?
                );
                reports.push(json!({
                    "model": model,
                    "success": true,
                    "report": report,
                }));
            }
            Err(error) => {
                let error_message = format!("{error:#}");
                eprintln!(
                    "{} benchmark {model} failed: {}",
                    match kind {
                        DocMediaKind::Image => "frame",
                        DocMediaKind::Video => "video",
                    },
                    error_message
                );
                failures.push(json!({
                    "model": model,
                    "error": error_message,
                }));
            }
        }
    }

    let summary = json!({
        "kind": match kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        },
        "file": path.display().to_string(),
        "successes": reports,
        "failures": failures,
    });
    let report_path = multimodal_benchmark_report_path(kind);
    std::fs::write(&report_path, serde_json::to_vec_pretty(&summary)?)
        .expect("write multimodal benchmark summary");
    eprintln!(
        "wrote multimodal benchmark summary to {}",
        report_path.display()
    );

    assert!(
        summary["successes"]
            .as_array()
            .is_some_and(|results| !results.is_empty()),
        "expected at least one successful multimodal benchmark result: {}",
        serde_json::to_string_pretty(&summary)?
    );
    if matches!(kind, DocMediaKind::Video) {
        assert!(
            summary["successes"].as_array().is_some_and(|results| results.iter().any(|result| {
                result["report"]["event_timed_assertions"]
                    .as_u64()
                    .is_some_and(|count| count > 0)
            })),
            "expected at least one successful video benchmark result with a time-grounded event assertion: {}",
            serde_json::to_string_pretty(&summary)?
        );
    }
    if summary["failures"]
        .as_array()
        .is_some_and(|results| !results.is_empty())
    {
        eprintln!(
            "multimodal benchmark had provider/model failures: {}",
            serde_json::to_string_pretty(&summary)?
        );
    }

    Ok(())
}

#[tokio::test]
#[ignore = "local real-media multimodal benchmark"]
async fn scenario_s5_18_real_example_frame_multimodal_e2e_across_models() -> anyhow::Result<()> {
    if !gemini_credentials_present() {
        eprintln!("Skipping real-media frame benchmark: no Gemini credentials in env");
        return Ok(());
    }
    if !Path::new(EXAMPLE_SKILLS_ROOT)
        .join("extraction/example-observations/SKILL.md")
        .exists()
    {
        eprintln!("Skipping real-media frame benchmark: example extraction skill missing");
        return Ok(());
    }

    let image = first_file_with_extension(Path::new(EXAMPLE_MEDIA_ROOT), "jpg")
        .expect("expected real example frame under media root");

    run_real_media_matrix(&image, DocMediaKind::Image, "image/jpeg").await
}

#[tokio::test]
#[ignore = "local real-media multimodal benchmark"]
async fn scenario_s5_18_real_example_video_multimodal_e2e_across_models() -> anyhow::Result<()> {
    if !gemini_credentials_present() {
        eprintln!("Skipping real-media video benchmark: no Gemini credentials in env");
        return Ok(());
    }
    if !Path::new(EXAMPLE_SKILLS_ROOT)
        .join("extraction/example-observations/SKILL.md")
        .exists()
    {
        eprintln!("Skipping real-media video benchmark: example extraction skill missing");
        return Ok(());
    }

    let video = first_probeable_video(Path::new(EXAMPLE_MEDIA_ROOT))
        .expect("expected probeable real example video under media root");

    run_real_media_matrix(&video, DocMediaKind::Video, "video/mp4").await
}
