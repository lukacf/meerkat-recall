//! Default text renderer for plain text documents.
//!
//! Per spec §7.1: The default renderer handles plain text sources by normalizing
//! whitespace and line endings.

use async_trait::async_trait;

use elephant_types::records::doc_revision::RendererMetadata;
use elephant_types::records::work::{Determinism, PluginManifest, RetryPolicy};

use super::{RenderContext, RenderResult, Renderer};
use crate::error::PipelineError;

/// Default renderer for plain text documents.
///
/// Handles source types:
/// - `text/plain`
/// - `text/*` (as fallback)
#[derive(Debug, Clone)]
pub struct DefaultTextRenderer {
    manifest: PluginManifest,
    source_types: Vec<String>,
}

impl Default for DefaultTextRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultTextRenderer {
    /// Creates a new default text renderer.
    #[must_use]
    pub fn new() -> Self {
        let manifest = PluginManifest {
            plugin_id: "elephant.renderer.text".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "renderer".to_string(),
            required_scopes: vec!["read:docs".to_string()],
            tool_access: vec![],
            work_kinds: vec!["render_doc".to_string()],
            determinism: Determinism::Deterministic,
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
            source_types: vec!["text/plain".to_string(), "text/*".to_string()],
        }
    }

    /// Normalizes text content.
    ///
    /// - Normalizes line endings to LF
    /// - Trims trailing whitespace from lines
    /// - Ensures single trailing newline
    fn normalize_text(text: &str) -> String {
        text.replace("\r\n", "\n")
            .replace('\r', "\n")
            .lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    }

    /// Returns the renderer metadata.
    fn metadata(&self) -> RendererMetadata {
        RendererMetadata {
            name: self.manifest.plugin_id.clone(),
            version: self.manifest.version.clone(),
            config_hash: None,
        }
    }
}

#[async_trait]
impl Renderer for DefaultTextRenderer {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError> {
        // For plain text, the doc.text is the source text
        // In a real implementation, we might fetch from source URL or process
        let raw_text = &ctx.doc.text;

        if raw_text.is_empty() {
            return Ok(RenderResult::failed(
                "Document has no content to render".to_string(),
                self.metadata(),
            ));
        }

        let normalized = Self::normalize_text(raw_text);
        Ok(RenderResult::success(normalized, self.metadata()))
    }
}

/// A pass-through renderer that doesn't modify content.
///
/// Useful for pre-normalized documents or testing.
#[derive(Debug, Clone)]
pub struct PassthroughRenderer {
    manifest: PluginManifest,
    source_types: Vec<String>,
}

impl Default for PassthroughRenderer {
    fn default() -> Self {
        Self::new()
    }
}

impl PassthroughRenderer {
    /// Creates a new passthrough renderer.
    #[must_use]
    pub fn new() -> Self {
        let manifest = PluginManifest {
            plugin_id: "elephant.renderer.passthrough".to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "renderer".to_string(),
            required_scopes: vec!["read:docs".to_string()],
            tool_access: vec![],
            work_kinds: vec!["render_doc".to_string()],
            determinism: Determinism::Deterministic,
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
            source_types: vec!["*".to_string()],
        }
    }

    /// Returns the renderer metadata.
    fn metadata(&self) -> RendererMetadata {
        RendererMetadata {
            name: self.manifest.plugin_id.clone(),
            version: self.manifest.version.clone(),
            config_hash: None,
        }
    }
}

#[async_trait]
impl Renderer for PassthroughRenderer {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn source_types(&self) -> &[String] {
        &self.source_types
    }

    async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError> {
        let text = &ctx.doc.text;

        if text.is_empty() {
            return Ok(RenderResult::failed(
                "Document has no content to render".to_string(),
                self.metadata(),
            ));
        }

        Ok(RenderResult::success(text.clone(), self.metadata()))
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
    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc::Doc;
    use elephant_types::records::doc_revision::RevisionStatus;
    use elephant_types::Link;

    fn make_doc(text: &str) -> Doc {
        Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "test".to_string(),
            ),
            title: "Test Document".to_string(),
            text: text.to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("test_{}", uuid::Uuid::new_v4()),
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn test_normalize_text_line_endings() {
        let text = "Hello\r\nWorld\rFoo\nBar";
        let normalized = DefaultTextRenderer::normalize_text(text);
        assert_eq!(normalized, "Hello\nWorld\nFoo\nBar\n");
    }

    #[test]
    fn test_normalize_text_trailing_whitespace() {
        let text = "Hello   \nWorld\t\t\nFoo";
        let normalized = DefaultTextRenderer::normalize_text(text);
        assert_eq!(normalized, "Hello\nWorld\nFoo\n");
    }

    #[test]
    fn test_normalize_text_ensures_trailing_newline() {
        let text = "Hello\nWorld";
        let normalized = DefaultTextRenderer::normalize_text(text);
        assert!(normalized.ends_with('\n'));
    }

    #[tokio::test]
    async fn test_default_renderer_success() {
        let renderer = DefaultTextRenderer::new();
        let doc = make_doc("Hello\r\nWorld");
        let ctx = RenderContext {
            doc,
            storage: None,
            space_id: "default".to_string(),
            config: None,
        };

        let result = renderer.render(ctx).await.expect("render");
        assert_eq!(result.status, RevisionStatus::Active);
        assert_eq!(result.text, "Hello\nWorld\n");
        assert!(!result.text_hash.is_empty());
    }

    #[tokio::test]
    async fn test_default_renderer_empty_content() {
        let renderer = DefaultTextRenderer::new();
        let doc = make_doc("");
        let ctx = RenderContext {
            doc,
            storage: None,
            space_id: "default".to_string(),
            config: None,
        };

        let result = renderer.render(ctx).await.expect("render");
        assert_eq!(result.status, RevisionStatus::Failed);
    }

    #[tokio::test]
    async fn test_passthrough_renderer() {
        let renderer = PassthroughRenderer::new();
        let doc = make_doc("Hello\r\nWorld");
        let ctx = RenderContext {
            doc,
            storage: None,
            space_id: "default".to_string(),
            config: None,
        };

        let result = renderer.render(ctx).await.expect("render");
        assert_eq!(result.status, RevisionStatus::Active);
        // Passthrough doesn't normalize
        assert_eq!(result.text, "Hello\r\nWorld");
    }

    #[test]
    fn test_default_renderer_source_types() {
        let renderer = DefaultTextRenderer::new();
        let types = renderer.source_types();
        assert!(types.contains(&"text/plain".to_string()));
        assert!(types.contains(&"text/*".to_string()));
    }

    #[test]
    fn test_passthrough_renderer_source_types() {
        let renderer = PassthroughRenderer::new();
        let types = renderer.source_types();
        assert!(types.contains(&"*".to_string()));
    }
}
