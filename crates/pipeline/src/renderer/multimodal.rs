//! Multimodal renderer for image and video-backed documents.

#[cfg(test)]
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(feature = "meerkat-extraction")]
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "meerkat-extraction")]
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
#[cfg(feature = "meerkat-extraction")]
use base64::Engine;
#[cfg(any(test, feature = "meerkat-extraction"))]
use elephant_types::records::doc::DocMediaKind;
use elephant_types::records::doc::{Doc, DocMediaAttachment};
use elephant_types::records::doc_revision::{
    RenderManifest, RenderManifestAttachment, RenderManifestSpanBinding, RenderManifestTimeRange,
    RendererMetadata,
};
use elephant_types::records::work::{Determinism, PluginManifest, RetryPolicy};

use super::{RenderContext, RenderResult, Renderer};
use crate::error::PipelineError;
#[cfg(feature = "meerkat-extraction")]
use crate::media::load_media_input;

#[cfg(feature = "meerkat-extraction")]
mod input_guard;

#[cfg(feature = "meerkat-extraction")]
const DEFAULT_MULTIMODAL_RENDER_MODEL: &str = "gpt-6.1-sol";

/// Result of analyzing a single media attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRenderAnalysis {
    /// Attachment identifier from the source document.
    pub attachment_id: String,
    /// Faithful derived text for the attachment.
    pub derived_text: String,
    /// Optional attachment-scoped time range.
    pub time_range: Option<RenderManifestTimeRange>,
}

#[async_trait]
trait MediaRenderAnalyzer: Send + Sync {
    /// Analyzes every attachment of a document in a single pass, returning one
    /// analysis per attachment in the same order.
    async fn analyze(
        &self,
        ctx: &RenderContext,
        attachments: &[DocMediaAttachment],
        original_work: Option<&elephant_types::records::WorkItem>,
    ) -> Result<Vec<AttachmentRenderAnalysis>, PipelineError>;
}

/// Multimodal renderer that derives canonical searchable text from image/video attachments.
pub struct MultimodalRenderer {
    manifest: PluginManifest,
    source_types: Vec<String>,
    analyzer: Arc<dyn MediaRenderAnalyzer>,
}

impl Default for MultimodalRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl MultimodalRenderer {
    /// Creates a renderer backed by the default analyzer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_analyzer(Arc::new(DefaultMediaRenderAnalyzer::new()))
    }

    fn with_analyzer(analyzer: Arc<dyn MediaRenderAnalyzer>) -> Self {
        let manifest = PluginManifest {
            plugin_id: "elephant.renderer.multimodal".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "renderer".to_string(),
            required_scopes: vec!["read:docs".to_string()],
            tool_access: vec![],
            work_kinds: vec!["render_doc".to_string()],
            determinism: Determinism::LlmSingleShot,
            max_tokens: None,
            max_wall_time_secs: None,
            input_types: vec!["doc".to_string()],
            output_types: vec!["doc_revision".to_string()],
            validator_profile: None,
            config_schema: None,
            health_check: None,
            retry_policy: RetryPolicy::default(),
        };

        Self {
            manifest,
            source_types: vec!["image/*".to_string(), "video/*".to_string()],
            analyzer,
        }
    }

    /// Creates a deterministic test renderer with canned attachment analyses.
    #[cfg(test)]
    #[must_use]
    pub fn for_tests(renders: Vec<FakeMediaRender>) -> Self {
        Self::with_analyzer(Arc::new(QueueMediaRenderAnalyzer::new(renders)))
    }

    /// Creates a renderer backed by the live multimodal analyzer for a specific model.
    #[cfg(feature = "meerkat-extraction")]
    #[must_use]
    pub fn for_live_model(model: impl Into<String>) -> Self {
        Self::with_analyzer(Arc::new(DefaultMediaRenderAnalyzer::with_model(
            model.into(),
        )))
    }

    fn metadata(&self) -> RendererMetadata {
        RendererMetadata {
            name: self.manifest.plugin_id.clone(),
            version: self.manifest.version.clone(),
            config_hash: None,
        }
    }
}

#[async_trait]
impl Renderer for MultimodalRenderer {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError> {
        self.render_input(ctx, None).await
    }

    async fn render_for_work(
        &self,
        ctx: RenderContext,
        original_work: &elephant_types::records::WorkItem,
    ) -> Result<RenderResult, PipelineError> {
        self.render_input(ctx, Some(original_work)).await
    }
}

impl MultimodalRenderer {
    async fn render_input(
        &self,
        ctx: RenderContext,
        original_work: Option<&elephant_types::records::WorkItem>,
    ) -> Result<RenderResult, PipelineError> {
        if ctx.doc.base.space_id != ctx.space_id {
            return Err(PipelineError::ConfigError(
                "provider_authorization: document space differs from media render context".into(),
            ));
        }
        if ctx.doc.media_attachments.is_empty() {
            return Ok(RenderResult::failed(
                "Document has no media attachments to render".to_string(),
                self.metadata(),
            ));
        }

        let analyses = self
            .analyzer
            .analyze(&ctx, &ctx.doc.media_attachments, original_work)
            .await?;

        let (text, manifest) = build_render_projection(&ctx.doc, &analyses);
        Ok(RenderResult::success_with_manifest(
            text,
            self.metadata(),
            Some(manifest),
        ))
    }
}

fn build_render_projection(
    doc: &Doc,
    analyses: &[AttachmentRenderAnalysis],
) -> (String, RenderManifest) {
    let mut text = doc
        .sidecar_text
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    let mut span_bindings = Vec::new();

    let attachments = doc
        .media_attachments
        .iter()
        .map(|attachment| RenderManifestAttachment {
            attachment_id: attachment.attachment_id.clone(),
            kind: attachment.kind,
            media_type: attachment.media_type.clone(),
            location: attachment.location.clone(),
            label: attachment.label.clone(),
            size_bytes: attachment.size_bytes,
            duration_ms: attachment.duration_ms,
            sha256: attachment.sha256.clone(),
        })
        .collect::<Vec<_>>();

    for analysis in analyses {
        if let Some(attachment) = doc
            .media_attachments
            .iter()
            .find(|attachment| attachment.attachment_id == analysis.attachment_id)
        {
            let analysis_text = analysis.derived_text.trim();
            if analysis_text.is_empty() {
                continue;
            }
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            let section_start = u32::try_from(text.len()).unwrap_or(u32::MAX);
            text.push_str(analysis_text);
            let section_end = u32::try_from(text.len()).unwrap_or(u32::MAX);
            span_bindings.push(RenderManifestSpanBinding {
                attachment_id: attachment.attachment_id.clone(),
                text_start: section_start,
                text_end: section_end,
                time_range: analysis.time_range.clone().or_else(|| {
                    attachment
                        .duration_ms
                        .map(|duration_ms| RenderManifestTimeRange {
                            start_ms: Some(0),
                            end_ms: Some(duration_ms),
                        })
                }),
            });
        }
    }

    (
        text.trim_end().to_string() + "\n",
        RenderManifest {
            attachments,
            span_bindings,
        },
    )
}

#[cfg(feature = "meerkat-extraction")]
struct DefaultMediaRenderAnalyzer {
    concurrency: tokio::sync::Semaphore,
    model: String,
    timeout_secs: u64,
}

#[cfg(feature = "meerkat-extraction")]
impl DefaultMediaRenderAnalyzer {
    fn new() -> Self {
        let model = std::env::var("PIPELINE_MULTIMODAL_RENDER_MODEL")
            .or_else(|_| std::env::var("PIPELINE_AGENTIC_MODEL"))
            .unwrap_or_else(|_| DEFAULT_MULTIMODAL_RENDER_MODEL.to_string());
        let timeout_secs = std::env::var("PIPELINE_MULTIMODAL_RENDER_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(300);
        let max_render_concurrent = std::env::var("PIPELINE_MULTIMODAL_RENDER_MAX_CONCURRENT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4);
        Self::with_settings(model, timeout_secs, max_render_concurrent)
    }

    fn with_model(model: String) -> Self {
        Self::with_settings(model, 300, 4)
    }

    fn with_settings(model: String, timeout_secs: u64, max_render_concurrent: usize) -> Self {
        Self {
            concurrency: tokio::sync::Semaphore::new(max_render_concurrent.max(1)),
            model,
            timeout_secs,
        }
    }
}

/// A media attachment with its payload loaded and base64-encoded for the model.
#[cfg(feature = "meerkat-extraction")]
struct LoadedAttachment {
    attachment: DocMediaAttachment,
    encoded: String,
    duration_ms: u64,
}

#[cfg(feature = "meerkat-extraction")]
#[async_trait]
impl MediaRenderAnalyzer for DefaultMediaRenderAnalyzer {
    async fn analyze(
        &self,
        ctx: &RenderContext,
        attachments: &[DocMediaAttachment],
        original_work: Option<&elephant_types::records::WorkItem>,
    ) -> Result<Vec<AttachmentRenderAnalysis>, PipelineError> {
        use meerkat_core::schema::MeerkatSchema;
        use meerkat_core::{ContentInput, OutputSchema};
        let _permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| PipelineError::ConfigError("media renderer stopped".into()))?;

        let mut inputs = input_guard::PreparedMediaInputs::capture(ctx, original_work).await?;
        let mut parts = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let loaded = load_media_input(ctx.storage.as_ref(), &ctx.space_id, attachment).await?;
            inputs.retain(attachment, &loaded)?;
            parts.push(LoadedAttachment {
                attachment: attachment.clone(),
                encoded: BASE64_STANDARD.encode(loaded.bytes),
                duration_ms: loaded.duration_ms,
            });
        }
        inputs.verify_current().await?;
        let inputs = Arc::new(inputs);

        let blocks = build_segment_blocks(&ctx.doc, &parts);
        let total = parts.len();

        let schema = OutputSchema {
            schema: MeerkatSchema::new(serde_json::json!({
                "type": "object",
                "properties": {
                    "analyses": {
                        "type": "array",
                        "minItems": total,
                        "maxItems": total,
                        "items": {
                            "type": "string",
                            "description": "Faithful text projection of the corresponding frame, in frame order. For video include time anchors when they matter."
                        }
                    }
                },
                "required": ["analyses"]
            }))
            .map_err(|e| PipelineError::ConfigError(format!("Invalid multimodal render schema: {e}")))?,
            name: Some("MultimodalRenderResult".to_string()),
            strict: false,
            compat: Default::default(),
            format: Default::default(),
        };

        let mut build = meerkat::AgentBuildConfig::new(self.model.clone());
        build.output_schema = Some(schema);
        build.structured_output_retries = Some(2);
        build.max_tokens = Some(render_output_max_tokens(total));
        build.system_prompt = meerkat_core::SystemPromptOverride::Set(
            "You are Elephant's multimodal renderer. Produce a faithful text projection of visible content. Prefer concrete people, apps, sites, groups, visible text, filenames, URLs, and time-anchored observations. Do not emit generic scaffolding.".into());
        build.override_shell = meerkat::ToolCategoryOverride::Disable;
        build.override_mob = meerkat::ToolCategoryOverride::Disable;
        let storage = ctx.storage.clone().ok_or_else(|| {
            PipelineError::ConfigError(
                "provider_authorization: media rendering requires storage context".into(),
            )
        })?;
        crate::provider_dispatch::ProviderDispatchContext::new(
            elephant_storage::SpaceConfigRepository::new(storage),
            &ctx.space_id,
        )
        .with_additional_observer(inputs.clone())
        .govern_agent_build(
            elephant_types::provider_binding::ModelOperation::MediaRender,
            &mut build,
        )
        .await?;
        let factory =
            meerkat::AgentFactory::new(std::env::temp_dir().join("elephant-multimodal-renderer"));
        let result = tokio::time::timeout(Duration::from_secs(self.timeout_secs), async {
            let mut agent = Box::pin(factory.build_agent(build, &meerkat::Config::default()))
                .await
                .map_err(|error| {
                    PipelineError::ExtractionError(format!("Multimodal build failed: {error}"))
                })?;
            Box::pin(agent.run(ContentInput::Blocks(blocks)))
                .await
                .map_err(|error| {
                    PipelineError::ExtractionError(format!("Multimodal render failed: {error}"))
                })
        })
        .await
        .map_err(|_| {
            PipelineError::ExtractionError(format!(
                "Multimodal render timed out after {}s",
                self.timeout_secs
            ))
        })??;

        // A response cannot publish a projection after the original input or
        // execution authority changed while the provider was running.
        inputs.verify_current().await?;
        let structured = result.structured_output.ok_or_else(|| {
            PipelineError::ExtractionError(
                "Multimodal render returned no structured output".to_string(),
            )
        })?;

        let values = structured
            .get("analyses")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                PipelineError::ExtractionError(
                    "Multimodal render returned no analyses array".to_string(),
                )
            })?;
        if values.len() != total {
            return Err(PipelineError::ExtractionError(format!(
                "Multimodal render returned {} analyses for {} attachments",
                values.len(),
                total
            )));
        }

        let mut analyses = Vec::with_capacity(total);
        let mut any_nonempty = false;
        for (part, value) in parts.iter().zip(values) {
            let derived_text = value.as_str().unwrap_or_default().trim().to_string();
            if !derived_text.is_empty() {
                any_nonempty = true;
            }
            analyses.push(AttachmentRenderAnalysis {
                attachment_id: part.attachment.attachment_id.clone(),
                derived_text,
                time_range: match part.attachment.kind {
                    DocMediaKind::Image => None,
                    DocMediaKind::Video => Some(RenderManifestTimeRange {
                        start_ms: Some(0),
                        end_ms: Some(part.duration_ms),
                    }),
                },
            });
        }
        if !any_nonempty {
            return Err(PipelineError::ExtractionError(
                "Multimodal render returned empty analysis".to_string(),
            ));
        }

        Ok(analyses)
    }
}

#[cfg(not(feature = "meerkat-extraction"))]
struct DefaultMediaRenderAnalyzer;

#[cfg(not(feature = "meerkat-extraction"))]
impl DefaultMediaRenderAnalyzer {
    fn new() -> Self {
        Self
    }
}

#[cfg(not(feature = "meerkat-extraction"))]
#[async_trait]
impl MediaRenderAnalyzer for DefaultMediaRenderAnalyzer {
    async fn analyze(
        &self,
        _ctx: &RenderContext,
        _attachments: &[DocMediaAttachment],
        _original_work: Option<&elephant_types::records::WorkItem>,
    ) -> Result<Vec<AttachmentRenderAnalysis>, PipelineError> {
        Err(PipelineError::ConfigError(
            "Multimodal rendering requires `meerkat-extraction`".to_string(),
        ))
    }
}

/// Output-token budget for a batched render call: the historical single-frame
/// budget plus headroom per additional frame, capped for provider limits.
#[cfg(feature = "meerkat-extraction")]
fn render_output_max_tokens(attachment_count: usize) -> u32 {
    let extra = u32::try_from(attachment_count.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .saturating_mul(2_048);
    32_768_u32.saturating_add(extra).min(65_536)
}

/// Builds the ordered content blocks for a single batched render call:
/// `Frame i of N` marker + media payload per attachment, then the prompt.
#[cfg(feature = "meerkat-extraction")]
fn build_segment_blocks(doc: &Doc, parts: &[LoadedAttachment]) -> Vec<meerkat_core::ContentBlock> {
    use meerkat_core::{ContentBlock, ImageData, VideoData};

    let total = parts.len();
    let mut blocks = Vec::with_capacity(parts.len() * 2 + 1);
    for (index, part) in parts.iter().enumerate() {
        let kind = match part.attachment.kind {
            DocMediaKind::Image => "image",
            DocMediaKind::Video => "video",
        };
        blocks.push(ContentBlock::Text {
            text: format!(
                "Frame {} of {} — attachment id {} ({}, {})",
                index + 1,
                total,
                part.attachment.attachment_id,
                kind,
                part.attachment.media_type
            ),
        });
        match part.attachment.kind {
            DocMediaKind::Image => blocks.push(ContentBlock::Image {
                media_type: part.attachment.media_type.clone(),
                data: ImageData::Inline {
                    data: part.encoded.clone(),
                },
            }),
            DocMediaKind::Video => blocks.push(ContentBlock::Video {
                media_type: part.attachment.media_type.clone(),
                duration_ms: part.duration_ms,
                data: VideoData::Inline {
                    data: part.encoded.clone(),
                },
            }),
        }
    }
    blocks.push(ContentBlock::Text {
        text: build_segment_prompt(doc, total),
    });
    blocks
}

#[cfg(feature = "meerkat-extraction")]
fn build_segment_prompt(doc: &Doc, total: usize) -> String {
    let sidecar = doc
        .sidecar_text
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .map(|text| format!("\nCaller sidecar text:\n{}\n", text.trim()))
        .unwrap_or_default();

    format!(
        "Document title: {}\nThe document has {total} media attachment(s), shown above in order as Frame 1 through Frame {total}.\n{sidecar}\nProduce a faithful text projection of what is visible in each frame.\n- Return an `analyses` array with exactly {total} entries; entry i describes Frame i only.\n- Prefer exact visible names, handles, apps, sites, groups, filenames, URLs, branch names, repo names, and quoted text.\n- Preserve verbatim visible strings whenever possible instead of paraphrasing them.\n- If visible text appears on screen, include the exact text in the projection on its own line or in a clearly quoted form.\n- Frames are ordered chronologically; when consecutive frames are near-duplicates, focus each later entry on what changed, without dropping identifying context.\n- For videos, include time anchors only when supported by the content.",
        doc.title
    )
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
/// Deterministic fake multimodal render output used in unit tests.
pub struct FakeMediaRender {
    /// Attachment identifier expected by the fake analyzer.
    pub attachment_id: String,
    /// Derived text returned for the attachment.
    pub derived_text: String,
}

#[cfg(test)]
struct QueueMediaRenderAnalyzer {
    renders: Mutex<VecDeque<FakeMediaRender>>,
}

#[cfg(test)]
impl QueueMediaRenderAnalyzer {
    fn new(renders: Vec<FakeMediaRender>) -> Self {
        Self {
            renders: Mutex::new(VecDeque::from(renders)),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl MediaRenderAnalyzer for QueueMediaRenderAnalyzer {
    async fn analyze(
        &self,
        _ctx: &RenderContext,
        attachments: &[DocMediaAttachment],
        _original_work: Option<&elephant_types::records::WorkItem>,
    ) -> Result<Vec<AttachmentRenderAnalysis>, PipelineError> {
        let mut renders = self
            .renders
            .lock()
            .map_err(|_| PipelineError::InvalidState("Render queue lock poisoned".to_string()))?;
        let mut analyses = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let render = renders.pop_front().ok_or_else(|| {
                PipelineError::InvalidState("No fake media render result available".to_string())
            })?;
            assert_eq!(render.attachment_id, attachment.attachment_id);
            analyses.push(AttachmentRenderAnalysis {
                attachment_id: render.attachment_id,
                derived_text: render.derived_text,
                time_range: attachment
                    .duration_ms
                    .map(|duration_ms| RenderManifestTimeRange {
                        start_ms: Some(0),
                        end_ms: Some(duration_ms),
                    }),
            });
        }
        Ok(analyses)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]

    use super::*;
    #[cfg(feature = "meerkat-extraction")]
    use crate::media::probe_video_duration_ms;
    use crate::renderer::Renderer;
    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc::DocMediaLocation;
    use std::path::{Path, PathBuf};

    fn make_media_doc(sidecar_text: Option<&str>) -> Doc {
        Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "test".to_string(),
            ),
            title: "GPU article".to_string(),
            text: String::new(),
            sidecar_text: sidecar_text.map(str::to_string),
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Path {
                    path: "/tmp/frame.jpg".to_string(),
                },
                label: Some("frame.jpg".to_string()),
                size_bytes: Some(1234),
                duration_ms: None,
                sha256: None,
            }],
            rendition_key: format!("rendition-{}", uuid::Uuid::new_v4()),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn test_multimodal_renderer_includes_sidecar_and_manifest() {
        let renderer = MultimodalRenderer::for_tests(vec![FakeMediaRender {
            attachment_id: "att-1".to_string(),
            derived_text: "Visible article title: Triton kernels".to_string(),
        }]);
        let ctx = crate::renderer::RenderContext {
            doc: make_media_doc(Some("User note: benchmark this later")),
            storage: None,
            space_id: "default".to_string(),
            config: None,
        };

        let result = renderer.render(ctx).await.expect("render");

        assert!(result.text.contains("User note: benchmark this later"));
        assert!(result
            .text
            .contains("Visible article title: Triton kernels"));
        assert!(!result.text.contains("Sidecar text:"));
        assert!(!result.text.contains("Attachment id:"));
        assert!(!result.text.contains("Derived analysis:"));
        let manifest = result.manifest.expect("manifest");
        assert_eq!(manifest.attachments.len(), 1);
        assert_eq!(manifest.span_bindings.len(), 1);
        assert_eq!(manifest.attachments[0].attachment_id, "att-1");
        let binding = &manifest.span_bindings[0];
        let bound_text = &result.text[binding.text_start as usize..binding.text_end as usize];
        assert_eq!(bound_text, "Visible article title: Triton kernels");
    }

    fn make_multi_frame_doc(count: usize) -> Doc {
        let mut doc = make_media_doc(None);
        doc.title = "Chat session segment".to_string();
        doc.media_attachments = (1..=count)
            .map(|index| DocMediaAttachment {
                attachment_id: format!("att-{index}"),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Path {
                    path: format!("/tmp/frame-{index}.jpg"),
                },
                label: Some(format!("frame-{index}.jpg")),
                size_bytes: Some(1234),
                duration_ms: None,
                sha256: None,
            })
            .collect();
        doc
    }

    #[tokio::test]
    async fn test_multimodal_renderer_orders_multi_frame_sections() {
        let renderer = MultimodalRenderer::for_tests(vec![
            FakeMediaRender {
                attachment_id: "att-1".to_string(),
                derived_text: "Frame one: DM from BingoBoss99".to_string(),
            },
            FakeMediaRender {
                attachment_id: "att-2".to_string(),
                derived_text: "Frame two: invite to private group".to_string(),
            },
            FakeMediaRender {
                attachment_id: "att-3".to_string(),
                derived_text: "Frame three: link shared".to_string(),
            },
        ]);
        let ctx = crate::renderer::RenderContext {
            doc: make_multi_frame_doc(3),
            storage: None,
            space_id: "default".to_string(),
            config: None,
        };

        let result = renderer.render(ctx).await.expect("render");

        let first = result.text.find("Frame one").expect("first section");
        let second = result.text.find("Frame two").expect("second section");
        let third = result.text.find("Frame three").expect("third section");
        assert!(
            first < second && second < third,
            "sections must keep frame order"
        );

        let manifest = result.manifest.expect("manifest");
        assert_eq!(manifest.attachments.len(), 3);
        assert_eq!(manifest.span_bindings.len(), 3);
        for (index, (attachment, binding)) in manifest
            .attachments
            .iter()
            .zip(&manifest.span_bindings)
            .enumerate()
        {
            let expected_id = format!("att-{}", index + 1);
            assert_eq!(attachment.attachment_id, expected_id);
            assert_eq!(binding.attachment_id, expected_id);
            let bound_text = &result.text[binding.text_start as usize..binding.text_end as usize];
            assert!(bound_text.starts_with(&format!("Frame {}", ["one", "two", "three"][index])));
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    #[test]
    fn test_render_output_max_tokens_scales_with_frames() {
        assert_eq!(render_output_max_tokens(1), 32_768);
        assert_eq!(render_output_max_tokens(4), 32_768 + 3 * 2_048);
        assert_eq!(render_output_max_tokens(32), 65_536);
    }

    #[cfg(feature = "meerkat-extraction")]
    #[test]
    fn test_build_segment_blocks_orders_markers_and_prompt() {
        use meerkat_core::ContentBlock;

        let doc = make_multi_frame_doc(3);
        let parts = doc
            .media_attachments
            .iter()
            .map(|attachment| LoadedAttachment {
                attachment: attachment.clone(),
                encoded: "AAEC".to_string(),
                duration_ms: 0,
            })
            .collect::<Vec<_>>();

        let blocks = build_segment_blocks(&doc, &parts);

        assert_eq!(blocks.len(), 7, "marker + payload per frame, then prompt");
        for index in 0..3 {
            match &blocks[index * 2] {
                ContentBlock::Text { text } => {
                    assert!(text.starts_with(&format!("Frame {} of 3", index + 1)));
                    assert!(text.contains(&format!("att-{}", index + 1)));
                }
                other => panic!("expected marker text block, got {other:?}"),
            }
            assert!(matches!(blocks[index * 2 + 1], ContentBlock::Image { .. }));
        }
        match blocks.last().expect("prompt block") {
            ContentBlock::Text { text } => {
                assert!(text.contains("exactly 3 entries"));
                assert!(text.contains("Chat session segment"));
            }
            other => panic!("expected prompt text block, got {other:?}"),
        }
    }

    #[cfg(feature = "meerkat-extraction")]
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

    #[cfg(feature = "meerkat-extraction")]
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
                && probe_video_duration_ms(&path.display().to_string()).is_ok()
            {
                return Some(path);
            }
        }
        None
    }

    #[cfg(feature = "meerkat-extraction")]
    fn make_live_doc(path: &Path, kind: DocMediaKind, media_type: &str) -> Doc {
        Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "test".to_string(),
            ),
            title: format!(
                "Example {}",
                path.file_name().and_then(|v| v.to_str()).unwrap_or("media")
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
            metadata: serde_json::json!({}),
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    fn assert_live_render_is_sensible(text: &str) {
        let lowered = text.to_ascii_lowercase();
        assert!(
            text.len() > 80,
            "expected substantial render text, got: {text}"
        );
        assert!(
            !lowered.contains("document has no media attachments"),
            "render should not fail: {text}"
        );
        assert!(
            !lowered.contains("returned empty analysis"),
            "render should not be empty: {text}"
        );
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    #[ignore = "local real-media multimodal benchmark"]
    async fn test_multimodal_renderer_real_example_frame_across_models() {
        let root = Path::new("/tmp/elephant-example-media");
        let image = first_file_with_extension(root, "jpg").expect("expected real example frame");

        for model in [
            "gemini-3-flash-preview",
            "gemini-3.1-flash-lite-preview",
            "gemini-3.1-pro-preview",
        ] {
            let renderer = MultimodalRenderer::for_live_model(model);
            let doc = make_live_doc(&image, DocMediaKind::Image, "image/jpeg");
            let start = std::time::Instant::now();
            let result = renderer
                .render(crate::renderer::RenderContext {
                    doc,
                    storage: None,
                    space_id: "default".to_string(),
                    config: None,
                })
                .await
                .expect("live image render");
            eprintln!(
                "live-image model={model} elapsed_ms={} chars={}",
                start.elapsed().as_millis(),
                result.text.len()
            );
            assert_live_render_is_sensible(&result.text);
            let manifest = result.manifest.expect("manifest");
            assert_eq!(manifest.attachments.len(), 1);
            assert_eq!(manifest.span_bindings.len(), 1);
        }
    }

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    #[ignore = "local real-media multimodal benchmark"]
    async fn test_multimodal_renderer_real_example_video_across_models() {
        let root = Path::new("/tmp/elephant-example-media");
        let video = first_probeable_video(root).expect("expected valid example video");

        for model in [
            "gemini-3-flash-preview",
            "gemini-3.1-flash-lite-preview",
            "gemini-3.1-pro-preview",
        ] {
            let renderer = MultimodalRenderer::for_live_model(model);
            let doc = make_live_doc(&video, DocMediaKind::Video, "video/mp4");
            let start = std::time::Instant::now();
            let result = renderer
                .render(crate::renderer::RenderContext {
                    doc,
                    storage: None,
                    space_id: "default".to_string(),
                    config: None,
                })
                .await
                .expect("live video render");
            eprintln!(
                "live-video model={model} elapsed_ms={} chars={}",
                start.elapsed().as_millis(),
                result.text.len()
            );
            assert_live_render_is_sensible(&result.text);
            let manifest = result.manifest.expect("manifest");
            assert_eq!(manifest.attachments.len(), 1);
            assert_eq!(manifest.span_bindings.len(), 1);
            assert!(
                manifest.span_bindings[0].time_range.is_some(),
                "video render should carry time range provenance"
            );
        }
    }
}
