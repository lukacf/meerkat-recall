#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines
)]

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use elephant_storage::{
    DocRepository, Repository, SourceRepository, StorageClient, WorkArtifactRepository,
    WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::{Doc, DocMediaAttachment, DocMediaKind, DocMediaLocation};
use elephant_types::records::source::{Source, SourceLifecycleStatus, SourceType};
use elephant_types::records::work::{
    Determinism, PluginManifest, RetryPolicy, WorkRun, WorkRunMetrics,
};
use elephant_types::{Link, RecordId};

use crate::extraction::{ExtractionContext, ExtractionResult, Extractor, RawAssertion};
use crate::renderer::multimodal::{FakeMediaRender, MultimodalRenderer};
use crate::renderer::RendererRegistry;
use crate::scheduler::{
    execute_build_doc_tree, execute_extract_assertions, execute_plan_extraction,
    execute_render_doc, DocTreeConfig, ExtractionPlanConfig,
};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
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

fn write_skill_tree(skill_id: &str, body: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("elephant-skill-{}", uuid::Uuid::new_v4()));
    let skill_path = root.join(skill_id).join("SKILL.md");
    std::fs::create_dir_all(
        skill_path
            .parent()
            .expect("skill path should have parent directory"),
    )
    .expect("create skill tree");
    std::fs::write(&skill_path, body).expect("write skill body");
    root
}

fn make_work_run(space_id: &str) -> WorkRun {
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let item_id = RecordId::new_v4(TableName::WorkItem);
    WorkRun {
        base: BaseFields::new(work_run_id, space_id.to_string(), "test".to_string()),
        work_item: Link::new(item_id),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "test-worker".to_string(),
        worker_version: "test".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    }
}

fn make_media_doc(kind: DocMediaKind) -> Doc {
    let (media_type, file_name, duration_ms) = match kind {
        DocMediaKind::Image => ("image/jpeg", "frame.jpg", None),
        DocMediaKind::Video => ("video/mp4", "segment.mp4", Some(123_000)),
    };

    let asset_path = std::env::temp_dir().join(format!(
        "elephant-media-{}-{}",
        match kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        },
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&asset_path, b"fixture-bytes").expect("write test media");

    Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "default".to_string(),
            "test".to_string(),
        ),
        title: format!(
            "Angel {} sample",
            match kind {
                DocMediaKind::Image => "frame",
                DocMediaKind::Video => "video",
            }
        ),
        text: "Caller note: prioritize concrete apps and people.".to_string(),
        sidecar_text: Some("Caller note: prioritize concrete apps and people.".to_string()),
        source: None,
        media_attachments: vec![DocMediaAttachment {
            attachment_id: "att-1".to_string(),
            kind,
            media_type: media_type.to_string(),
            location: DocMediaLocation::Path {
                path: asset_path.display().to_string(),
            },
            label: Some(file_name.to_string()),
            size_bytes: Some(13),
            duration_ms,
            sha256: None,
        }],
        rendition_key: format!("rendition-{}", uuid::Uuid::new_v4()),
        metadata: serde_json::json!({
            "extraction_skill": "extraction/test-multimodal",
            "extraction_purpose": "extract concrete media observations",
            "extraction_domain": "test_multimodal"
        }),
    }
}

fn make_media_source(kind: DocMediaKind) -> Source {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "default".to_string(),
            "test".to_string(),
        ),
        match kind {
            DocMediaKind::Image => SourceType::DeviceFrame,
            DocMediaKind::Video => SourceType::DeviceVideo,
        },
        format!("hash-{}", uuid::Uuid::new_v4()),
    );
    source.lifecycle_status = SourceLifecycleStatus::GraphReady;
    source.timestamp = Some(
        Utc.with_ymd_and_hms(2026, 4, 10, 14, 0, 0)
            .single()
            .expect("valid source timestamp"),
    );
    source
}

#[derive(Clone, Default)]
struct CapturingExtractor {
    seen: Arc<Mutex<Option<ExtractionContext>>>,
}

#[async_trait]
impl Extractor for CapturingExtractor {
    fn manifest(&self) -> &PluginManifest {
        static MANIFEST: OnceLock<PluginManifest> = OnceLock::new();
        MANIFEST.get_or_init(|| PluginManifest {
            plugin_id: "elephant.extractor.capturing".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "extractor".to_string(),
            required_scopes: vec![],
            tool_access: vec![],
            work_kinds: vec!["extract_assertions".to_string()],
            determinism: Determinism::Deterministic,
            max_tokens: None,
            max_wall_time_secs: None,
            input_types: vec!["doc_node".to_string()],
            output_types: vec!["knowledge_candidates".to_string()],
            validator_profile: None,
            config_schema: None,
            health_check: None,
            retry_policy: RetryPolicy::default(),
        })
    }

    async fn extract(
        &self,
        ctx: ExtractionContext,
    ) -> Result<ExtractionResult, crate::error::PipelineError> {
        let quote = ctx.node_text.clone();
        *self.seen.lock().expect("captured context lock") = Some(ctx.clone());
        Ok(ExtractionResult::new(
            vec![RawAssertion {
                subject_text: "alex".to_string(),
                predicate_text: "uses".to_string(),
                object_text: Some("Claude-Code / Terminal IDE".to_string()),
                object_literal: None,
                evidence_span: EvidenceSpan {
                    revision: Link::new(ctx.revision_id.clone()),
                    node: Some(Link::new(ctx.node_id.clone())),
                    start: 0,
                    end: quote.len() as u32,
                    quote_hash: "fixture".to_string(),
                },
                confidence: 0.91,
                quote: Some(quote),
                source_event_time: None,
                source_event_time_precision: None,
                event_title: Some("Observed IDE activity".to_string()),
                event_lineage_key: None,
                subject_entity_id: None,
                subject_entity_type: Some("person".to_string()),
                object_entity_id: None,
                object_entity_type: Some("platform".to_string()),
                significance: Some("high".to_string()),
            }],
            321,
            "fixture-model".to_string(),
        ))
    }
}

async fn run_multimodal_pipeline(
    doc: Doc,
    derived_text: &str,
) -> anyhow::Result<(Doc, ExtractionContext)> {
    let storage = StorageClient::new_memory().await?;
    let doc_repo = DocRepository::new(storage.clone());
    let source_repo = SourceRepository::new(storage.clone());
    let work_run_repo = WorkRunRepository::new(storage.clone());
    let artifact_repo = WorkArtifactRepository::new(storage.clone());

    let mut doc = doc;
    let source = make_media_source(doc.media_attachments[0].kind);
    let stored_source = source_repo.create(source).await?;
    doc.source = Some(Link::new(stored_source.base.id.clone()));

    let stored_doc = doc_repo.create(doc).await?;
    let media_type = stored_doc
        .media_attachments
        .first()
        .map(|attachment| attachment.media_type.as_str())
        .unwrap_or("application/octet-stream");

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(MultimodalRenderer::for_tests(vec![
        FakeMediaRender {
            attachment_id: "att-1".to_string(),
            derived_text: derived_text.to_string(),
        },
    ])));

    let render_result =
        execute_render_doc(&storage, &registry, &stored_doc, Some(media_type)).await?;
    let tree_result = execute_build_doc_tree(
        &storage,
        &render_result.revision_id,
        &DocTreeConfig::default(),
    )
    .await?;
    assert!(
        tree_result.chunk_nodes >= 1,
        "expected chunk nodes for rendered media doc"
    );

    let work_run = work_run_repo.create(make_work_run("default")).await?;
    let plan = execute_plan_extraction(
        &storage,
        &render_result.revision_id,
        "default",
        &ExtractionPlanConfig::default(),
        &work_run.base.id,
        "default",
        "test",
    )
    .await?;
    assert!(
        !plan.plan.node_selections.is_empty(),
        "expected extraction plan to select at least one node"
    );

    let extractor = CapturingExtractor::default();
    let selection = plan
        .plan
        .node_selections
        .first()
        .cloned()
        .expect("plan selection");
    let extract_result = execute_extract_assertions(
        &storage,
        &extractor,
        &selection.node_id,
        &render_result.revision_id,
        &work_run.base.id,
        "default",
        "test",
        None,
    )
    .await?;
    assert_eq!(extract_result.assertions_extracted, 1);

    let artifact = artifact_repo
        .get("default", &extract_result.artifact_id)
        .await?
        .expect("knowledge artifact should exist");
    let assertions = artifact
        .data
        .get("assertions")
        .and_then(serde_json::Value::as_array)
        .expect("artifact assertions array");
    assert_eq!(assertions.len(), 1);

    let captured = extractor
        .seen
        .lock()
        .expect("captured context lock")
        .clone()
        .expect("captured extraction context");

    Ok((stored_doc, captured))
}

#[tokio::test]
async fn test_multimodal_image_pipeline_loads_skill_and_media_context() -> anyhow::Result<()> {
    let _lock = env_lock().lock().await;
    let skill_root = write_skill_tree(
        "extraction/test-multimodal",
        r#"---
name: Test Multimodal Skill
description: Extract concrete apps and people from media-backed docs
---

Prefer concrete people, handles, apps, websites, and groups over scaffolding.
"#,
    );
    let _guard = EnvVarGuard::set("PIPELINE_SKILLS_DIR", skill_root);

    let doc = make_media_doc(DocMediaKind::Image);
    let (_stored_doc, captured) = run_multimodal_pipeline(
        doc,
        "Visible handle: example-user. Visible app: Claude-Code / Terminal IDE. Visible site: github.com.",
    )
    .await?;

    assert_eq!(captured.doc_title.as_deref(), Some("Angel frame sample"));
    assert_eq!(
        captured.sidecar_text.as_deref(),
        Some("Caller note: prioritize concrete apps and people.")
    );
    assert_eq!(captured.media_attachments.len(), 1);
    assert_eq!(captured.media_attachments[0].attachment_id, "att-1");
    assert!(
        captured.media_attachments[0].time_range.is_none(),
        "image attachments should not carry time ranges"
    );
    assert!(
        captured
            .skill_body
            .as_deref()
            .is_some_and(|body: &str| body.contains("Prefer concrete people, handles, apps")),
        "expected skill body to be loaded from PIPELINE_SKILLS_DIR"
    );

    Ok(())
}

#[tokio::test]
async fn test_multimodal_video_pipeline_preserves_time_range_context() -> anyhow::Result<()> {
    let _lock = env_lock().lock().await;
    let skill_root = write_skill_tree(
        "extraction/test-multimodal",
        r#"---
name: Test Multimodal Skill
description: Extract concrete apps and people from media-backed docs
---

Prefer concrete people, handles, apps, websites, and groups over scaffolding.
"#,
    );
    let _guard = EnvVarGuard::set("PIPELINE_SKILLS_DIR", skill_root);

    let doc = make_media_doc(DocMediaKind::Video);
    let (_stored_doc, captured) = run_multimodal_pipeline(
        doc,
        "[00:00-02:03] Visible speaker: alex. Visible app: Claude-Code / Terminal IDE. Visible topic: Rust agent runtime.",
    )
    .await?;

    assert_eq!(captured.media_attachments.len(), 1);
    assert_eq!(captured.media_attachments[0].attachment_id, "att-1");
    assert!(
        captured.media_attachments[0].time_range.is_some(),
        "video attachments should carry render-manifest time range provenance"
    );
    let time_range = captured.media_attachments[0]
        .time_range
        .clone()
        .expect("video time range");
    assert_eq!(time_range.start_ms, Some(0));
    assert_eq!(time_range.end_ms, Some(123_000));
    assert_eq!(
        captured.source_timestamp,
        Some(
            Utc.with_ymd_and_hms(2026, 4, 10, 14, 0, 0)
                .single()
                .expect("valid timestamp")
        )
    );

    Ok(())
}
