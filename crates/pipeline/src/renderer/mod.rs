//! Renderer framework for document revision creation.
//!
//! Per spec §7.1: Renderers transform source documents into immutable normalized text
//! stored as `doc_revision` records.
//!
//! The renderer architecture follows the plugin model defined in §2.4a:
//! - Renderers declare manifests specifying which source types they handle
//! - Core provides the pipeline stage and storage
//! - Rendering logic is implemented by renderer plugins

use async_trait::async_trait;

use elephant_storage::StorageClient;
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{RenderManifest, RendererMetadata, RevisionStatus};
use elephant_types::records::work::PluginManifest;

use crate::error::PipelineError;

pub mod default;
pub mod multimodal;

/// Result of a render operation.
#[derive(Debug, Clone)]
pub struct RenderResult {
    /// The normalized text content.
    pub text: String,
    /// Hash of the text for integrity verification.
    pub text_hash: String,
    /// Renderer metadata.
    pub renderer: RendererMetadata,
    /// Optional media-aware render manifest.
    pub manifest: Option<RenderManifest>,
    /// Initial status of the revision.
    pub status: RevisionStatus,
}

impl RenderResult {
    /// Creates a successful render result.
    #[must_use]
    pub fn success(text: String, renderer: RendererMetadata) -> Self {
        Self::success_with_manifest(text, renderer, None)
    }

    /// Creates a successful render result with an optional manifest.
    #[must_use]
    pub fn success_with_manifest(
        text: String,
        renderer: RendererMetadata,
        manifest: Option<RenderManifest>,
    ) -> Self {
        let text_hash = compute_text_hash(&text);
        Self {
            text,
            text_hash,
            renderer,
            manifest,
            status: RevisionStatus::Active,
        }
    }

    /// Creates a failed render result.
    #[must_use]
    pub fn failed(error: String, renderer: RendererMetadata) -> Self {
        Self {
            text: error,
            text_hash: String::new(),
            renderer,
            manifest: None,
            status: RevisionStatus::Failed,
        }
    }
}

/// Computes SHA-256 hash of text content.
fn compute_text_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Context provided to renderers during execution.
#[derive(Debug, Clone)]
pub struct RenderContext {
    /// The document to render.
    pub doc: Doc,
    /// Storage client for managed asset and revision lookups.
    pub storage: Option<StorageClient>,
    /// Space ID for security context.
    pub space_id: String,
    /// Optional configuration for the renderer.
    pub config: Option<serde_json::Value>,
}

/// Trait for document renderers.
///
/// Per spec §2.4a: Renderers declare a manifest and transform documents into
/// normalized text for storage as `doc_revision`.
#[async_trait]
pub trait Renderer: Send + Sync {
    /// Returns the renderer's manifest.
    fn manifest(&self) -> &PluginManifest;

    /// Returns the source types this renderer handles.
    ///
    /// Patterns can include wildcards (e.g., "text/*", "application/pdf").
    fn source_types(&self) -> &[String] {
        &[]
    }

    /// Renders a document to normalized text.
    ///
    /// # Errors
    ///
    /// Returns an error if rendering fails.
    async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError>;

    /// Optional health check.
    async fn health_check(&self) -> Result<bool, PipelineError> {
        Ok(true)
    }
}

/// Registry of renderers.
#[derive(Default)]
pub struct RendererRegistry {
    renderers: Vec<Box<dyn Renderer>>,
}

impl RendererRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a renderer.
    pub fn register(&mut self, renderer: Box<dyn Renderer>) {
        self.renderers.push(renderer);
    }

    /// Finds a renderer for the given source type.
    ///
    /// Returns the first matching renderer based on source type patterns.
    #[must_use]
    pub fn find_for_source_type(&self, source_type: &str) -> Option<&dyn Renderer> {
        for renderer in &self.renderers {
            for pattern in renderer.source_types() {
                if matches_pattern(pattern, source_type) {
                    return Some(renderer.as_ref());
                }
            }
        }
        None
    }

    /// Returns all registered renderers.
    #[must_use]
    pub fn all(&self) -> &[Box<dyn Renderer>] {
        &self.renderers
    }

    /// Returns the number of registered renderers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.renderers.len()
    }

    /// Returns true if no renderers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.renderers.is_empty()
    }
}

/// Matches a source type against a pattern.
///
/// Supports:
/// - Exact match: "text/plain" matches "text/plain"
/// - Wildcard suffix: "text/*" matches "text/plain", "text/html"
/// - Universal wildcard: "*" matches everything
fn matches_pattern(pattern: &str, source_type: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if pattern.ends_with("/*") {
        let prefix = &pattern[..pattern.len() - 1]; // Remove the "*"
        return source_type.starts_with(prefix);
    }

    pattern == source_type
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

    #[test]
    fn test_matches_pattern_exact() {
        assert!(matches_pattern("text/plain", "text/plain"));
        assert!(!matches_pattern("text/plain", "text/html"));
    }

    #[test]
    fn test_matches_pattern_wildcard() {
        assert!(matches_pattern("text/*", "text/plain"));
        assert!(matches_pattern("text/*", "text/html"));
        assert!(!matches_pattern("text/*", "application/json"));
    }

    #[test]
    fn test_matches_pattern_universal() {
        assert!(matches_pattern("*", "text/plain"));
        assert!(matches_pattern("*", "application/pdf"));
    }

    #[test]
    fn test_compute_text_hash() {
        let hash = compute_text_hash("Hello, World!");
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // SHA-256 hex is 64 chars

        // Same input should produce same hash
        let hash2 = compute_text_hash("Hello, World!");
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = compute_text_hash("Hello, World");
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_render_result_success() {
        let renderer = RendererMetadata {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        };
        let result = RenderResult::success("Hello".to_string(), renderer);
        assert_eq!(result.status, RevisionStatus::Active);
        assert!(!result.text_hash.is_empty());
        assert!(result.manifest.is_none());
    }

    #[test]
    fn test_render_result_failed() {
        let renderer = RendererMetadata {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        };
        let result = RenderResult::failed("Error".to_string(), renderer);
        assert_eq!(result.status, RevisionStatus::Failed);
        assert!(result.text_hash.is_empty());
    }
}
