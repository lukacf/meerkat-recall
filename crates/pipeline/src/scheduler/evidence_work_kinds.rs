//! Work kinds for the evidence layer (Phase 3).
//!
//! Per spec §7: These work kinds transform sources into immutable document
//! representations with structural nodes.
//!
//! Work kinds:
//! - `render_doc`: source → doc_revision
//! - `build_doc_tree`: doc_revision → doc_node tree
//! - `embed_chunks`: chunk nodes → embeddings

// Allow index casts - node positions and text lengths are small enough to fit in i32/u32
#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

use chrono::Utc;
use std::collections::BTreeSet;
use std::sync::Mutex;

use elephant_types::publication::{PublicationFence, SourceVisibility};
use elephant_types::records::source::Source;

use elephant_embeddings::{EmbeddingProvider, PartitionManager};
use elephant_storage::doc_node_repository::DocNodeRepository;
use elephant_storage::doc_revision_repository::DocRevisionRepository;
use elephant_storage::error::StorageError;
use elephant_storage::{DocRepository, NodeEmbeddingRepository, Repository, StorageClient};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RevisionStatus};
use elephant_types::records::NodeEmbedding;
use elephant_types::Link;
use elephant_types::RecordId;

use crate::error::PipelineError;
use crate::renderer::{RenderContext, RendererRegistry};
use crate::summarizer::{
    AgenticSummarizer, SummarizationBudget, SummarizationContext, SummarizationOptions, Summarizer,
};

mod summary_inputs;

/// Detected content type for structural analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    /// Markdown format
    Markdown,
    /// HTML format
    Html,
    /// Plain text
    PlainText,
    /// Email message (RFC 2822)
    Email,
    /// Structured data (JSON, XML)
    Structured,
}

/// Work kind identifier for render_doc.
pub const RENDER_DOC_KIND: &str = "render_doc";

/// Work kind identifier for build_doc_tree.
pub const BUILD_DOC_TREE_KIND: &str = "build_doc_tree";

/// Work kind identifier for embed_chunks.
pub const EMBED_CHUNKS_KIND: &str = "embed_chunks";

fn default_summarizer(
    storage: &StorageClient,
    space_id: &str,
    inputs: Option<std::sync::Arc<summary_inputs::PreparedSummaryInputs>>,
) -> Box<dyn Summarizer> {
    let mut context = crate::provider_dispatch::ProviderDispatchContext::new(
        elephant_storage::SpaceConfigRepository::new(storage.clone()),
        space_id,
    );
    if let Some(inputs) = inputs {
        context = context.with_additional_observer(inputs);
    }
    Box::new(AgenticSummarizer::new().with_provider_context(context))
}

/// Result of render_doc work kind.
#[derive(Debug, Clone)]
pub struct RenderDocResult {
    /// The created revision.
    pub revision_id: RecordId,
    /// Superseded revision (if any).
    pub superseded_id: Option<RecordId>,
    /// Whether rendering was successful.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Result of build_doc_tree work kind.
#[derive(Debug, Clone)]
pub struct BuildDocTreeResult {
    /// Number of nodes created.
    pub nodes_created: usize,
    /// Number of chunk nodes created.
    pub chunk_nodes: usize,
    /// Root node ID.
    pub root_node_id: RecordId,
    /// Detected content type.
    pub content_type: ContentType,
}

/// Result of embed_chunks work kind.
#[derive(Debug, Clone)]
pub struct EmbedChunksResult {
    /// Number of chunks that were embedded.
    pub chunks_embedded: usize,
    /// IDs of the doc_node chunks that were embedded.
    pub embedded_node_ids: Vec<RecordId>,
    /// Embedding model used.
    pub model: String,
}

/// Configuration for doc tree building.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::struct_excessive_bools)] // Config struct with feature flags
pub struct DocTreeConfig {
    /// Maximum chunk size in bytes.
    pub max_chunk_size: usize,
    /// Chunk overlap in bytes.
    pub chunk_overlap: usize,
    /// Whether to create paragraph nodes.
    pub create_paragraphs: bool,
    /// Whether to create section nodes from headings.
    pub detect_sections: bool,
    /// Whether to use sentence-aware chunking (avoids splitting mid-sentence).
    pub semantic_chunking: bool,
    /// Whether to create summary nodes for large sections.
    pub create_summaries: bool,
    /// Minimum section size in bytes to trigger summary generation.
    pub summary_threshold: usize,
    /// Model used for LLM section summaries.
    pub summarizer_model: String,
}

impl Default for DocTreeConfig {
    fn default() -> Self {
        Self {
            max_chunk_size: 512,
            chunk_overlap: 50,
            create_paragraphs: true,
            detect_sections: false,
            semantic_chunking: true,
            create_summaries: false,
            summary_threshold: 1000,
            summarizer_model: "claude-sonnet-5-5".to_string(),
        }
    }
}

/// Executes the render_doc work kind.
///
/// Transforms a document into an immutable doc_revision using the appropriate renderer.
///
/// # Errors
///
/// Returns an error if rendering or storage fails.
pub async fn execute_render_doc(
    storage: &StorageClient,
    registry: &RendererRegistry,
    doc: &Doc,
    source_type: Option<&str>,
) -> Result<RenderDocResult, PipelineError> {
    Box::pin(execute_render_doc_with_original_work(
        storage,
        registry,
        doc,
        source_type,
        None,
    ))
    .await
}

/// Render using the original claimed execution and its exact intended document.
/// # Errors
/// Refuses a stale, foreign or mismatched original Work or changed render input.
pub async fn execute_render_doc_for_work(
    storage: &StorageClient,
    registry: &RendererRegistry,
    doc: &Doc,
    source_type: Option<&str>,
    original_work: &elephant_types::records::WorkItem,
) -> Result<RenderDocResult, PipelineError> {
    Box::pin(execute_render_doc_with_original_work(
        storage,
        registry,
        doc,
        source_type,
        Some(original_work),
    ))
    .await
}

async fn execute_render_doc_with_original_work(
    storage: &StorageClient,
    registry: &RendererRegistry,
    doc: &Doc,
    source_type: Option<&str>,
    original_work: Option<&elephant_types::records::WorkItem>,
) -> Result<RenderDocResult, PipelineError> {
    let doc_repo = DocRepository::new(storage.clone());
    let revision_repo = DocRevisionRepository::new(storage.clone());

    // Find appropriate renderer
    let source_type = source_type.unwrap_or("text/plain");
    let renderer = registry.find_for_source_type(source_type).ok_or_else(|| {
        PipelineError::ConfigError(format!("No renderer for source type: {source_type}"))
    })?;

    let (source, render_doc, publication) = captured_render_input(storage, doc).await?;
    let raw_doc = doc_repo
        .get(&doc.base.space_id, &doc.base.id)
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Original render document is absent".into())
        })?;
    let inputs = Box::pin(summary_inputs::PreparedSummaryInputs::capture_render(
        storage,
        &source,
        &raw_doc,
        publication.as_ref(),
        original_work,
    ))
    .await?;
    let recipe = crate::extraction::AssertionValidator::hash_quote(
        &serde_json::to_string(&serde_json::json!({
            "version": "immutable-render-v1",
            "source_type": source_type,
            "renderer": renderer.manifest().plugin_id,
            "renderer_version": renderer.manifest().version,
            "source_hash": source.hash,
            "input": {
                "title": render_doc.title,
                "text": render_doc.text,
                "sidecar_text": render_doc.sidecar_text,
                "media_attachments": render_doc.media_attachments,
                "rendition_key": render_doc.rendition_key,
                "metadata": render_doc.metadata,
            },
        }))
        .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
    );
    let captured_id = publication.as_ref().map(|_| {
        RecordId::new(
            TableName::DocRevision,
            uuid::Uuid::new_v5(&doc.base.id.id(), b"immutable-render-v1"),
        )
    });
    if let Some(id) = &captured_id {
        if let Some(existing) = revision_repo.get(&doc.base.space_id, id).await? {
            validate_render_replay(&existing, &render_doc, &source, &recipe)?;
            inputs.verify_current().await?;
            doc_repo
                .update_render_projection(&doc.base.space_id, &doc.base.id, &existing.text)
                .await?;
            return Ok(RenderDocResult {
                revision_id: existing.base.id,
                superseded_id: None,
                success: true,
                error: None,
            });
        }
    }

    let ctx = RenderContext {
        doc: render_doc,
        storage: Some(storage.clone()),
        space_id: doc.base.space_id.clone(),
        config: None,
    };
    inputs.verify_current().await?;
    let render_result = if let Some(work) = original_work {
        Box::pin(renderer.render_for_work(ctx, work)).await?
    } else {
        Box::pin(renderer.render(ctx)).await?
    };
    inputs.verify_current().await?;
    if render_result.status == RevisionStatus::Active
        && render_result.text_hash
            != crate::extraction::AssertionValidator::hash_quote(&render_result.text)
    {
        return Err(PipelineError::ValidationError(
            "Renderer text hash disagrees with rendered bytes".into(),
        ));
    }
    if render_result.status != RevisionStatus::Active {
        if let Some(id) = &captured_id {
            // A failed provider response cannot occupy the immutable successful
            // render identity and prevent a later retry of the same acceptance.
            return Ok(RenderDocResult {
                revision_id: id.clone(),
                superseded_id: None,
                success: false,
                error: Some("Rendering failed".into()),
            });
        }
    }

    // Check for existing active revision to supersede
    let existing_revisions = revision_repo
        .find_by_doc(&doc.base.id)
        .await
        .map_err(PipelineError::StorageError)?;

    let active_revision_id = existing_revisions
        .iter()
        .find(|r| r.status == RevisionStatus::Active)
        .map(|r| r.base.id.clone());
    if publication.is_some()
        && active_revision_id.is_some()
        && active_revision_id.as_ref() != captured_id.as_ref()
    {
        return Err(PipelineError::InvalidState(
            "Accepted document already has a different immutable render identity".into(),
        ));
    }
    let superseded_id = (publication.is_none() && render_result.status == RevisionStatus::Active)
        .then(|| active_revision_id.clone())
        .flatten();

    // Create the revision
    let mut base = BaseFields::new(
        captured_id
            .clone()
            .unwrap_or_else(|| RecordId::new_v4(TableName::DocRevision)),
        doc.base.space_id.clone(),
        doc.base.created_by.clone(),
    );
    base.security = doc.base.security.clone();
    base.subjects = doc.base.subjects.clone();
    base.subjects_state = doc.base.subjects_state;
    base.provenance = doc.base.provenance.clone();
    base.provenance.sources = vec![source.base.id.clone()];
    base.provenance
        .chain
        .retain(|step| step.step != super::grounding::SOURCE_CONTENT_STEP);
    base.provenance
        .chain
        .push(elephant_types::records::base::ProvenanceStep {
            step: super::grounding::SOURCE_CONTENT_STEP.into(),
            input: source.base.id.to_string(),
            output: source.hash.clone(),
        });

    if publication.is_some() {
        base.provenance
            .chain
            .push(elephant_types::records::base::ProvenanceStep {
                step: "immutable_render_recipe_v1".into(),
                input: doc.base.id.to_string(),
                output: recipe.clone(),
            });
    }

    let revision = DocRevision {
        base,
        doc: Link::new(doc.base.id.clone()),
        text: render_result.text,
        text_hash: render_result.text_hash,
        renderer: render_result.renderer,
        manifest: render_result.manifest,
        supersedes: superseded_id.clone().map(Link::new),
        status: render_result.status,
        rendered_at: Utc::now(),
    };

    inputs.verify_current().await?;
    let created = match revision_repo.create(revision).await {
        Ok(created) => created,
        Err(error) => {
            let Some(id) = &captured_id else {
                return Err(error.into());
            };
            let Some(existing) = revision_repo.get(&doc.base.space_id, id).await? else {
                return Err(error.into());
            };
            // An overlapping retry may have persisted the first complete output.
            // Reuse that exact result, never overwrite it with a fresh generation.
            validate_render_replay(&existing, doc, &source, &recipe)?;
            existing
        }
    };

    if created.status == RevisionStatus::Active {
        doc_repo
            .update_render_projection(&doc.base.space_id, &doc.base.id, &created.text)
            .await?;
    }

    // Retained unaccepted render callers still supersede explicitly; storage
    // failures must not be hidden as successful publication.
    if let Some(old_id) = &superseded_id {
        let mut old_rev = revision_repo
            .get(&doc.base.space_id, old_id)
            .await?
            .ok_or_else(|| PipelineError::NotFound(old_id.to_string()))?;
        old_rev.status = RevisionStatus::Superseded;
        revision_repo
            .update(&doc.base.space_id, old_id, old_rev)
            .await?;
    }

    let success = created.status == RevisionStatus::Active;
    let error = if success {
        None
    } else {
        Some("Rendering failed".to_string())
    };

    Ok(RenderDocResult {
        revision_id: created.base.id,
        superseded_id,
        success,
        error,
    })
}

/// Resolve the original accepted snapshot before any renderer or summary call.
/// A latest source row can validate current visibility; it cannot supply input.
pub(crate) async fn captured_render_input(
    storage: &StorageClient,
    doc: &Doc,
) -> Result<(Source, Doc, Option<PublicationFence>), PipelineError> {
    use elephant_storage::source_publication::SourcePublicationRepository;
    let current_doc = DocRepository::new(storage.clone())
        .get(&doc.base.space_id, &doc.base.id)
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Rendering document is missing or deleted".into())
        })?;
    if current_doc.source != doc.source
        || !super::grounding::same_authorization_partition(&current_doc.base, &doc.base)
        || current_doc.metadata.get("_publication") != doc.metadata.get("_publication")
    {
        return Err(PipelineError::ValidationError(
            "Rendering document lineage changed".into(),
        ));
    }
    let source_id = doc
        .source
        .as_ref()
        .ok_or_else(|| {
            PipelineError::ValidationError("Rendering evidence requires source lineage".into())
        })?
        .id();
    let current = elephant_storage::SourceRepository::new(storage.clone())
        .get(&doc.base.space_id, source_id)
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Rendering source is missing or deleted".into())
        })?;
    let repository = SourcePublicationRepository::new(storage.clone());
    let head = repository.head(&doc.base.space_id, source_id).await?;
    let Some(value) = doc.metadata.get("_publication") else {
        if head.is_some()
            || repository
                .head_exists_globally(&doc.base.space_id, source_id)
                .await?
        {
            return Err(PipelineError::ValidationError(
                "Accepted document lacks its original publication fence".into(),
            ));
        }
        if !super::grounding::same_authorization_partition(&current.base, &doc.base) {
            return Err(PipelineError::ValidationError(
                "Rendering source authorization lineage changed".into(),
            ));
        }
        return Ok((current, doc.clone(), None));
    };
    let publication: PublicationFence = serde_json::from_value(value.clone())
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
    let head =
        head.ok_or_else(|| PipelineError::ValidationError("Accepted source head missing".into()))?;
    if publication.source_id != *source_id
        || head.generation != publication.generation
        || head.operation_id != publication.operation_id
        || head.desired_revision.as_deref() != Some(publication.revision_id.as_str())
        || head.lifecycle != SourceVisibility::Active
    {
        return Err(PipelineError::ValidationError(
            "Accepted render input is superseded or withdrawn".into(),
        ));
    }
    let snapshot = repository
        .revision(&doc.base.space_id, source_id, &publication.revision_id)
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Accepted source revision is missing".into())
        })?;
    let source = snapshot.source;
    if source.base.id != *source_id
        || source.base.space_id != doc.base.space_id
        || source.hash != current.hash
        || !super::grounding::same_authorization_partition(&source.base, &current.base)
        || !super::grounding::same_authorization_partition(&source.base, &doc.base)
    {
        return Err(PipelineError::ValidationError(
            "Accepted render source envelope changed".into(),
        ));
    }
    let template = source
        .manual_doc_template()
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?
        .ok_or_else(|| {
            PipelineError::ValidationError("Accepted source has no immutable document input".into())
        })?;
    let expected_id = crate::source_queue::accepted_document_id(&publication, &template.doc_id)?;
    if expected_id != doc.base.id {
        return Err(PipelineError::ValidationError(
            "Document is not the accepted immutable rendition".into(),
        ));
    }
    let mut input = doc.clone();
    input.title = template.title;
    input.text = template.text;
    input.sidecar_text = template.sidecar_text;
    input.media_attachments = template.media_attachments;
    input.rendition_key = template.rendition_key;
    input.metadata = template.metadata;
    if !input.metadata.is_object() {
        return Err(PipelineError::ValidationError(
            "Accepted render metadata is not an object".into(),
        ));
    }
    input.metadata["_publication"] = serde_json::to_value(&publication)
        .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
    Ok((source, input, Some(publication)))
}

fn validate_render_replay(
    revision: &DocRevision,
    doc: &Doc,
    source: &Source,
    recipe: &str,
) -> Result<(), PipelineError> {
    let recipes: Vec<_> = revision
        .base
        .provenance
        .chain
        .iter()
        .filter(|step| step.step == "immutable_render_recipe_v1")
        .collect();
    if revision.base.is_deleted()
        || revision.status != RevisionStatus::Active
        || revision.doc.id() != &doc.base.id
        || revision.base.space_id != doc.base.space_id
        || revision.text_hash != crate::extraction::AssertionValidator::hash_quote(&revision.text)
        || !super::grounding::same_authorization_partition(&revision.base, &source.base)
        || recipes.len() != 1
        || recipes[0].input != doc.base.id.to_string()
        || recipes[0].output != recipe
    {
        return Err(PipelineError::ValidationError(
            "Immutable render replay differs from its captured input or recipe".into(),
        ));
    }
    Ok(())
}

/// A detected section in the document.
#[derive(Debug, Clone)]
struct DetectedSection {
    /// Title of the section (heading text).
    title: String,
    /// Start byte offset of the section (inclusive of heading).
    start: u32,
    /// End byte offset of the section (exclusive).
    end: u32,
}

/// A detected paragraph in the document.
#[derive(Debug, Clone)]
struct DetectedParagraph {
    /// Start byte offset of the paragraph.
    start: u32,
    /// End byte offset of the paragraph.
    end: u32,
}

/// Detects the content type from text content.
fn detect_content_type(text: &str) -> ContentType {
    let trimmed = text.trim();

    // Check for HTML
    if trimmed.starts_with("<!DOCTYPE")
        || trimmed.starts_with("<html")
        || trimmed.contains("<body")
        || trimmed.contains("<div")
    {
        return ContentType::Html;
    }

    // Check for email headers
    if trimmed.starts_with("From:")
        || trimmed.starts_with("Subject:")
        || trimmed.starts_with("Date:")
        || (trimmed.contains("\nFrom:") && trimmed.contains("\nSubject:"))
    {
        return ContentType::Email;
    }

    // Check for structured data
    if trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.starts_with('<') {
        return ContentType::Structured;
    }

    // Check for Markdown indicators
    if trimmed.contains("\n# ")
        || trimmed.contains("\n## ")
        || trimmed.starts_with("# ")
        || trimmed.contains("\n```")
    {
        return ContentType::Markdown;
    }

    ContentType::PlainText
}

/// Yields line contents with byte offsets into the original text.
///
/// Like `str::lines`, removes LF or CRLF endings from the contents, but counts
/// every delimiter byte when advancing the offset.
fn lines_with_byte_offsets(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.split_inclusive('\n').scan(0, |offset, raw_line| {
        let start = *offset;
        *offset += raw_line.len();
        let line = raw_line
            .strip_suffix("\r\n")
            .or_else(|| raw_line.strip_suffix('\n'))
            .unwrap_or(raw_line);
        Some((start, line))
    })
}

/// Detects section boundaries in text.
///
/// Sections are detected by:
/// - Lines starting with `#` (Markdown headings)
/// - Lines followed by `===` or `---` underlines (Setext-style headings)
#[allow(clippy::cast_possible_truncation)] // Text positions fit in u32
fn detect_sections(text: &str) -> Vec<DetectedSection> {
    let mut sections = Vec::new();
    let lines: Vec<_> = lines_with_byte_offsets(text).collect();

    if lines.is_empty() {
        return sections;
    }

    let mut current_section_start: Option<(usize, String)> = None;
    for (i, &(line_start, line)) in lines.iter().enumerate() {
        // Check for Markdown-style heading (# Heading)
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            // Find the heading text after #s
            let hash_count = trimmed.chars().take_while(|c| *c == '#').count();
            if hash_count <= 6 {
                let title = trimmed[hash_count..].trim().to_string();

                // Close previous section if exists
                if let Some((prev_start, prev_title)) = current_section_start.take() {
                    sections.push(DetectedSection {
                        title: prev_title,
                        start: prev_start as u32,
                        end: line_start as u32,
                    });
                }

                current_section_start = Some((line_start, title));
            }
        }
        // Check for Setext-style heading (underline on next line)
        else if i + 1 < lines.len() {
            let next_line = lines[i + 1].1.trim();
            let is_setext = !line.trim().is_empty()
                && (next_line.chars().all(|c| c == '=') && next_line.len() >= 3
                    || next_line.chars().all(|c| c == '-') && next_line.len() >= 3);

            if is_setext {
                let title = line.trim().to_string();

                // Close previous section if exists
                if let Some((prev_start, prev_title)) = current_section_start.take() {
                    sections.push(DetectedSection {
                        title: prev_title,
                        start: prev_start as u32,
                        end: line_start as u32,
                    });
                }

                current_section_start = Some((line_start, title));
            }
        }
    }

    // Close final section
    if let Some((prev_start, prev_title)) = current_section_start {
        sections.push(DetectedSection {
            title: prev_title,
            start: prev_start as u32,
            end: text.len() as u32,
        });
    }

    sections
}

/// Detects paragraph boundaries in text within a given range.
///
/// Paragraphs are separated by double newlines (blank lines).
#[allow(clippy::cast_possible_truncation)] // Text positions fit in u32
fn detect_paragraphs(text: &str, range_start: u32, range_end: u32) -> Vec<DetectedParagraph> {
    let mut paragraphs = Vec::new();
    let range_text = &text[range_start as usize..range_end as usize];

    if range_text.trim().is_empty() {
        return paragraphs;
    }

    let mut para_start: Option<usize> = None;
    let mut last_non_empty_end = 0usize;
    let mut prev_was_empty = true;

    for (line_start, line) in lines_with_byte_offsets(range_text) {
        let line_len = line.len();
        let is_empty = line.trim().is_empty();

        if !is_empty {
            if para_start.is_none() {
                // Start new paragraph
                para_start = Some(line_start);
            }
            last_non_empty_end = line_start + line_len;
        } else if !prev_was_empty && para_start.is_some() {
            // End of paragraph (first blank line after content)
            paragraphs.push(DetectedParagraph {
                start: range_start + para_start.take().unwrap_or(0) as u32,
                end: range_start + last_non_empty_end as u32,
            });
        }

        prev_was_empty = is_empty;
    }

    // Handle final paragraph (no trailing newline)
    if let Some(start) = para_start {
        paragraphs.push(DetectedParagraph {
            start: range_start + start as u32,
            end: range_start + last_non_empty_end as u32,
        });
    }

    paragraphs
}

/// A detected list structure in HTML.
#[derive(Debug, Clone)]
struct DetectedList {
    list_type: ListType,
    start: u32,
    end: u32,
}

/// Type of HTML list (ordered or unordered).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListType {
    Ordered,   // <ol>
    Unordered, // <ul>
}

/// A detected table in HTML.
#[derive(Debug, Clone)]
struct DetectedTable {
    start: u32,
    end: u32,
}

/// A detected table cell in HTML.
#[derive(Debug, Clone)]
struct DetectedTableCell {
    /// Start byte offset of the cell.
    start: u32,
    /// End byte offset of the cell.
    end: u32,
    /// Whether this is a header cell (th) or data cell (td).
    is_header: bool,
}

/// Detects section boundaries in HTML content.
/// Finds h1-h6 tags and creates sections.
#[allow(clippy::cast_possible_truncation)]
fn detect_html_sections(text: &str) -> Vec<DetectedSection> {
    let mut sections = Vec::new();

    // Simple regex-free HTML heading detection
    // Looking for patterns like <h1>, <h2>, etc.
    let heading_patterns = ["<h1", "<h2", "<h3", "<h4", "<h5", "<h6"];

    let mut current_section: Option<(usize, String)> = None;
    let text_lower = text.to_lowercase();

    for pattern in &heading_patterns {
        let mut search_start = 0;
        while let Some(tag_start) = text_lower[search_start..].find(pattern) {
            let absolute_start = search_start + tag_start;

            // Find the closing >
            if let Some(tag_end) = text[absolute_start..].find('>') {
                let content_start = absolute_start + tag_end + 1;

                // Find the closing tag (e.g., </h1>)
                let close_tag = format!("</{}>", &pattern[1..3]);
                if let Some(content_end) = text_lower[content_start..].find(&close_tag) {
                    let title = text[content_start..content_start + content_end]
                        .trim()
                        .to_string();

                    // Close previous section
                    if let Some((prev_start, prev_title)) = current_section.take() {
                        sections.push(DetectedSection {
                            title: prev_title,
                            start: prev_start as u32,
                            end: absolute_start as u32,
                        });
                    }

                    current_section = Some((absolute_start, title));
                }
            }

            search_start = absolute_start + pattern.len();
        }
    }

    // Close final section
    if let Some((prev_start, prev_title)) = current_section {
        sections.push(DetectedSection {
            title: prev_title,
            start: prev_start as u32,
            end: text.len() as u32,
        });
    }

    // Sort by start position (since we searched by pattern, not position)
    sections.sort_by_key(|s| s.start);

    sections
}

/// Detects list elements in HTML content.
fn detect_html_lists(text: &str) -> Vec<DetectedList> {
    let mut lists = Vec::new();
    let text_lower = text.to_lowercase();

    // Find ordered lists
    let mut search_start = 0;
    while let Some(start) = text_lower[search_start..].find("<ol") {
        let absolute_start = search_start + start;
        if let Some(end) = text_lower[absolute_start..].find("</ol>") {
            lists.push(DetectedList {
                list_type: ListType::Ordered,
                start: absolute_start as u32,
                end: (absolute_start + end + 5) as u32,
            });
            search_start = absolute_start + end + 5;
        } else {
            search_start = absolute_start + 3;
        }
    }

    // Find unordered lists
    search_start = 0;
    while let Some(start) = text_lower[search_start..].find("<ul") {
        let absolute_start = search_start + start;
        if let Some(end) = text_lower[absolute_start..].find("</ul>") {
            lists.push(DetectedList {
                list_type: ListType::Unordered,
                start: absolute_start as u32,
                end: (absolute_start + end + 5) as u32,
            });
            search_start = absolute_start + end + 5;
        } else {
            search_start = absolute_start + 3;
        }
    }

    lists.sort_by_key(|l| l.start);
    lists
}

/// Detects table elements in HTML content.
fn detect_html_tables(text: &str) -> Vec<DetectedTable> {
    let mut tables = Vec::new();
    let text_lower = text.to_lowercase();

    let mut search_start = 0;
    while let Some(start) = text_lower[search_start..].find("<table") {
        let absolute_start = search_start + start;
        if let Some(end) = text_lower[absolute_start..].find("</table>") {
            tables.push(DetectedTable {
                start: absolute_start as u32,
                end: (absolute_start + end + 8) as u32,
            });
            search_start = absolute_start + end + 8;
        } else {
            search_start = absolute_start + 6;
        }
    }

    tables
}

/// Detects table cells (td and th elements) within a table range.
#[allow(clippy::cast_possible_truncation)]
fn detect_html_table_cells(text: &str, table_start: u32, table_end: u32) -> Vec<DetectedTableCell> {
    let mut cells = Vec::new();
    let table_text = &text[table_start as usize..table_end as usize];
    let table_lower = table_text.to_lowercase();

    // Find header cells (th)
    let mut search_start = 0;
    while let Some(start) = table_lower[search_start..].find("<th") {
        let absolute_start = search_start + start;
        // Find the closing > of the opening tag
        if let Some(tag_end) = table_lower[absolute_start..].find('>') {
            let content_start = absolute_start + tag_end + 1;
            // Find the closing </th> tag
            if let Some(end) = table_lower[content_start..].find("</th>") {
                cells.push(DetectedTableCell {
                    start: table_start + absolute_start as u32,
                    end: table_start + (content_start + end + 5) as u32,
                    is_header: true,
                });
                search_start = content_start + end + 5;
            } else {
                search_start = absolute_start + 3;
            }
        } else {
            search_start = absolute_start + 3;
        }
    }

    // Find data cells (td)
    search_start = 0;
    while let Some(start) = table_lower[search_start..].find("<td") {
        let absolute_start = search_start + start;
        // Find the closing > of the opening tag
        if let Some(tag_end) = table_lower[absolute_start..].find('>') {
            let content_start = absolute_start + tag_end + 1;
            // Find the closing </td> tag
            if let Some(end) = table_lower[content_start..].find("</td>") {
                cells.push(DetectedTableCell {
                    start: table_start + absolute_start as u32,
                    end: table_start + (content_start + end + 5) as u32,
                    is_header: false,
                });
                search_start = content_start + end + 5;
            } else {
                search_start = absolute_start + 3;
            }
        } else {
            search_start = absolute_start + 3;
        }
    }

    // Sort by start position
    cells.sort_by_key(|c| c.start);
    cells
}

/// A retry checks every expected node. Finding a root or one chunk is never a
/// completion marker. Deterministic IDs permit repairing a missing structural
/// node without changing the sealed source manifest's evidence identities.
struct ReplayTreeWriter {
    repository: DocNodeRepository,
    revision_id: RecordId,
    space_id: String,
    expected_ids: Mutex<BTreeSet<String>>,
    sealed: bool,
}

impl ReplayTreeWriter {
    fn node_id(&self, key: &str) -> RecordId {
        RecordId::new(
            TableName::DocNode,
            uuid::Uuid::new_v5(&self.revision_id.id(), key.as_bytes()),
        )
    }

    async fn existing(&self, key: &str) -> Result<Option<DocNode>, StorageError> {
        self.repository
            .get(&self.space_id, &self.node_id(key))
            .await
    }

    async fn create(&self, mut proposed: DocNode) -> Result<DocNode, StorageError> {
        proposed.base.id = self.node_id(&proposed.node_key);
        let result = if let Some(existing) = self.existing(&proposed.node_key).await? {
            validate_node_replay(&existing, &proposed)?;
            existing
        } else {
            if self.sealed && matches!(proposed.role, NodeRole::Document | NodeRole::Summary) {
                return Err(StorageError::DataIntegrityError(
                    "A sealed tree lost its recipe root or captured summary; exact restoration is required".into(),
                ));
            }
            match self.repository.create(proposed.clone()).await {
                Ok(created) => created,
                Err(error) => {
                    let Some(existing) = self.existing(&proposed.node_key).await? else {
                        return Err(error);
                    };
                    validate_node_replay(&existing, &proposed)?;
                    existing
                }
            }
        };
        self.expected_ids
            .lock()
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?
            .insert(result.base.id.to_string());
        Ok(result)
    }

    async fn finish(&self) -> Result<(), PipelineError> {
        let actual: BTreeSet<_> = self
            .repository
            .find_by_revision(&self.revision_id)
            .await?
            .into_iter()
            .map(|node| node.base.id.to_string())
            .collect();
        let expected = self
            .expected_ids
            .lock()
            .map_err(|error| PipelineError::InvalidState(error.to_string()))?;
        if actual != *expected {
            return Err(PipelineError::ValidationError(
                "Rendered tree contains missing or unexpected nodes; subset replay cannot succeed"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn validate_node_replay(existing: &DocNode, proposed: &DocNode) -> Result<(), StorageError> {
    if existing.base.is_deleted()
        || existing.base.id != proposed.base.id
        || existing.base.space_id != proposed.base.space_id
        || !super::grounding::same_authorization_partition(&existing.base, &proposed.base)
        || existing.base.provenance != proposed.base.provenance
        || existing.revision != proposed.revision
        || existing.role != proposed.role
        || existing.span != proposed.span
        || existing.node_key != proposed.node_key
        || existing.text != proposed.text
        || existing.parent != proposed.parent
        || existing.position != proposed.position
        || existing.depth != proposed.depth
        || existing.collection != proposed.collection
    {
        return Err(StorageError::DataIntegrityError(
            "Immutable document node replay changes its evidence, authorization or tree recipe"
                .into(),
        ));
    }
    // Embedding progress and timestamps are mutable bookkeeping. Return the
    // stored row unchanged, including any real embedding marker.
    Ok(())
}

/// Context for building doc tree nodes.
struct DocTreeContext<'a> {
    node_repo: &'a ReplayTreeWriter,
    revision_id: &'a RecordId,
    space_id: &'a str,
    created_by: &'a str,
    text: &'a str,
    security: &'a elephant_types::records::base::Security,
    provenance: &'a elephant_types::records::base::Provenance,
    subjects: &'a [Link<elephant_types::records::entity::Entity>],
    subjects_state: elephant_types::records::base::SubjectsState,
    /// Collection from the parent doc's metadata (propagated to chunk nodes).
    collection: Option<String>,
}

/// Executes the build_doc_tree work kind.
///
/// Builds a structural node tree from a doc_revision.
///
/// When `config.detect_sections` is true, section boundaries are detected
/// from headings and `NodeRole::Section` nodes are created.
///
/// When `config.create_paragraphs` is true, paragraph boundaries are detected
/// from double newlines and `NodeRole::Paragraph` nodes are created.
///
/// The hierarchy is: Document > Section (optional) > Paragraph (optional) > Chunk
///
/// # Errors
///
/// Returns an error if tree building or storage fails.
#[allow(clippy::too_many_lines, clippy::cast_possible_truncation)] // Complex doc tree building
pub async fn execute_build_doc_tree(
    storage: &StorageClient,
    revision_id: &RecordId,
    config: &DocTreeConfig,
) -> Result<BuildDocTreeResult, PipelineError> {
    Box::pin(build_doc_tree_with_original_work(
        storage,
        revision_id,
        config,
        None,
        None,
    ))
    .await
}

/// Build the tree under the actual original claimed execution, preserving its
/// independently admitted or inherited authority through optional model calls.
/// # Errors
/// Refuses changed inputs, stale attempts, or any revoked original authority.
pub async fn execute_build_doc_tree_for_work(
    storage: &StorageClient,
    revision_id: &RecordId,
    config: &DocTreeConfig,
    original_work: &elephant_types::records::WorkItem,
) -> Result<BuildDocTreeResult, PipelineError> {
    Box::pin(build_doc_tree_with_original_work(
        storage,
        revision_id,
        config,
        None,
        Some(original_work),
    ))
    .await
}

#[cfg(test)]
async fn build_doc_tree_with_summarizer(
    storage: &StorageClient,
    revision_id: &RecordId,
    config: &DocTreeConfig,
    summarizer: Option<&dyn Summarizer>,
) -> Result<BuildDocTreeResult, PipelineError> {
    Box::pin(build_doc_tree_with_original_work(
        storage,
        revision_id,
        config,
        summarizer,
        None,
    ))
    .await
}

#[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
async fn build_doc_tree_with_original_work(
    storage: &StorageClient,
    revision_id: &RecordId,
    config: &DocTreeConfig,
    summarizer: Option<&dyn Summarizer>,
    original_work: Option<&elephant_types::records::WorkItem>,
) -> Result<BuildDocTreeResult, PipelineError> {
    let revision_repo = DocRevisionRepository::new(storage.clone());

    // Fetch the revision
    let revision = revision_repo
        .get_by_id(revision_id)
        .await
        .map_err(PipelineError::StorageError)?
        .filter(|revision| revision.status == RevisionStatus::Active)
        .ok_or_else(|| PipelineError::NotFound(revision_id.to_string()))?;

    let space_id = &revision.base.space_id;
    let text = &revision.text;
    let text_len = text.len() as u32;

    // Validate source content and authorization before any summary dispatch.
    let doc = DocRepository::new(storage.clone())
        .get(space_id, revision.doc.id())
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Tree revision has no live document".into())
        })?;
    let (source, _, publication) = captured_render_input(storage, &doc).await?;
    super::grounding::validate_revision_lineage(&source, &doc, &revision, space_id)?;
    let summary_inputs = if config.create_summaries || summarizer.is_some() {
        Some(std::sync::Arc::new(
            Box::pin(summary_inputs::PreparedSummaryInputs::capture(
                storage,
                &source,
                &doc,
                &revision,
                publication.as_ref(),
                original_work,
            ))
            .await?,
        ))
    } else {
        None
    };
    let default_summary = (config.create_summaries && summarizer.is_none())
        .then(|| default_summarizer(storage, space_id, summary_inputs.clone()));
    let summarizer = summarizer.or(default_summary.as_deref());
    let doc_collection = doc
        .metadata
        .get("collection")
        .and_then(serde_json::Value::as_str)
        .map(String::from);

    if config.max_chunk_size == 0 || config.chunk_overlap >= config.max_chunk_size {
        return Err(PipelineError::InvalidInput(
            "Tree chunk size must be positive and exceed overlap".into(),
        ));
    }
    let sealed = if let Some(publication) = &publication {
        elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(
            storage.clone(),
        )
        .get(space_id, publication)
        .await?
        .is_some()
    } else {
        false
    };
    let node_repo = ReplayTreeWriter {
        repository: DocNodeRepository::new(storage.clone()),
        revision_id: revision_id.clone(),
        space_id: space_id.clone(),
        expected_ids: Mutex::new(BTreeSet::new()),
        sealed,
    };
    let mut tree_provenance = revision.base.provenance.clone();
    tree_provenance
        .chain
        .push(elephant_types::records::base::ProvenanceStep {
            step: "immutable_tree_recipe_v1".into(),
            input: revision_id.to_string(),
            output: crate::extraction::AssertionValidator::hash_quote(
                &serde_json::to_string(&serde_json::json!({
                    "algorithm": "tree-v1", "configuration": config,
                    "text_hash": revision.text_hash, "collection": doc_collection,
                }))
                .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
            ),
        });

    // Detect content type for structural analysis
    let content_type = detect_content_type(text);

    // Create root document node
    let mut root_base = BaseFields::new(
        RecordId::new_v4(TableName::DocNode),
        space_id.clone(),
        revision.base.created_by.clone(),
    );
    root_base.security = revision.base.security.clone();
    root_base.subjects = revision.base.subjects.clone();
    root_base.subjects_state = revision.base.subjects_state;
    root_base.provenance = tree_provenance.clone();

    let root_node = DocNode {
        base: root_base,
        revision: Link::new(revision_id.clone()),
        role: NodeRole::Document,
        span: TextSpan::new(0, text_len),
        node_key: format!("doc_{revision_id}"),
        text: None,
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: doc_collection.clone(),
    };

    let created_root = node_repo
        .create(root_node)
        .await
        .map_err(PipelineError::StorageError)?;

    let mut nodes_created = 1;
    let mut chunk_nodes = 0;
    let mut chunk_counter = 0;
    let mut table_counter = 0;
    let mut list_counter = 0;

    let ctx = DocTreeContext {
        node_repo: &node_repo,
        revision_id,
        space_id,
        created_by: &revision.base.created_by,
        text,
        security: &revision.base.security,
        provenance: &tree_provenance,
        subjects: &revision.base.subjects,
        subjects_state: revision.base.subjects_state,
        collection: doc_collection.clone(),
    };

    // Determine structural hierarchy based on config and content type
    let sections = if config.detect_sections {
        match content_type {
            ContentType::Html => detect_html_sections(text),
            ContentType::Markdown
            | ContentType::PlainText
            | ContentType::Email
            | ContentType::Structured => detect_sections(text),
        }
    } else {
        Vec::new()
    };

    if !sections.is_empty() {
        // Handle content before first section (if any)
        if sections[0].start > 0 {
            let pre_section_chunks = build_section_content(
                &ctx,
                0,
                sections[0].start,
                &created_root.base.id,
                1, // depth
                config,
                &mut chunk_counter,
            )
            .await?;

            nodes_created += pre_section_chunks.0;
            chunk_nodes += pre_section_chunks.1;

            // Create table and list nodes for HTML content before first section
            if content_type == ContentType::Html {
                let table_nodes = create_html_tables_under_parent(
                    &ctx,
                    0,
                    sections[0].start,
                    &created_root.base.id,
                    1, // depth
                    &mut table_counter,
                )
                .await?;
                nodes_created += table_nodes;

                let list_nodes = create_html_lists_under_parent(
                    &ctx,
                    0,
                    sections[0].start,
                    &created_root.base.id,
                    1, // depth
                    &mut list_counter,
                )
                .await?;
                nodes_created += list_nodes;
            }
        }

        // Create section nodes and build tree under them
        for (section_idx, section) in sections.iter().enumerate() {
            let mut section_base = BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                space_id.clone(),
                revision.base.created_by.clone(),
            );
            section_base.security = revision.base.security.clone();
            section_base.subjects = revision.base.subjects.clone();
            section_base.subjects_state = revision.base.subjects_state;
            section_base.provenance = tree_provenance.clone();

            let section_node = DocNode {
                base: section_base,
                revision: Link::new(revision_id.clone()),
                role: NodeRole::Section,
                span: TextSpan::new(section.start, section.end),
                node_key: format!("section_{revision_id}_{section_idx}"),
                text: Some(section.title.clone()),
                parent: Some(Link::new(created_root.base.id.clone())),
                #[allow(clippy::cast_possible_wrap)] // Section index fits in i32
                position: section_idx as i32,
                depth: 1,
                embedding_model: None,
                collection: ctx.collection.clone(),
            };

            let created_section = node_repo
                .create(section_node)
                .await
                .map_err(PipelineError::StorageError)?;
            nodes_created += 1;

            // Create summary node for large sections if enabled
            let section_text_len = (section.end - section.start) as usize;
            if section_text_len >= config.summary_threshold {
                if let Some(summarizer) = summarizer {
                    let section_text = &text[section.start as usize..section.end as usize];
                    let ctx = SummarizationContext::new(
                        section_text.to_string(),
                        SummarizationBudget::default(),
                        SummarizationOptions {
                            min_text_length: config.summary_threshold,
                            model: config.summarizer_model.clone(),
                        },
                    );

                    let summary_key = format!("summary_{revision_id}_{section_idx}");
                    let existing_summary = node_repo.existing(&summary_key).await?;
                    if existing_summary.is_some() || summarizer.should_summarize(&ctx) {
                        let summary_text = if let Some(existing) = existing_summary {
                            existing.text.ok_or_else(|| {
                                PipelineError::ValidationError(
                                    "Captured summary has no text".into(),
                                )
                            })?
                        } else {
                            if node_repo.sealed {
                                return Err(PipelineError::ValidationError("Sealed summary is missing; generation cannot replace its captured output".into()));
                            }
                            let inputs = summary_inputs.as_ref().ok_or_else(|| {
                                PipelineError::ValidationError(
                                    "Section summary lacks original input guard".into(),
                                )
                            })?;
                            inputs.verify_current().await?;
                            let summary = Box::pin(summarizer.summarize(&ctx)).await?;
                            inputs.verify_current().await?;
                            summary.text
                        };
                        {
                            let mut summary_base = BaseFields::new(
                                RecordId::new_v4(TableName::DocNode),
                                space_id.clone(),
                                revision.base.created_by.clone(),
                            );
                            summary_base.security = revision.base.security.clone();
                            summary_base.subjects = revision.base.subjects.clone();
                            summary_base.subjects_state = revision.base.subjects_state;
                            summary_base.provenance = tree_provenance.clone();

                            let summary_node = DocNode {
                                base: summary_base,
                                revision: Link::new(revision_id.clone()),
                                role: NodeRole::Summary,
                                span: TextSpan::new(section.start, section.end),
                                node_key: format!("summary_{revision_id}_{section_idx}"),
                                text: Some(summary_text),
                                parent: Some(Link::new(created_section.base.id.clone())),
                                position: 0, // Summary comes first under section
                                depth: 2,
                                embedding_model: None,
                                collection: doc_collection.clone(),
                            };

                            node_repo
                                .create(summary_node)
                                .await
                                .map_err(PipelineError::StorageError)?;
                            nodes_created += 1;
                        }
                    }
                }
            }

            // Build paragraphs or chunks under this section
            let section_chunks = build_section_content(
                &ctx,
                section.start,
                section.end,
                &created_section.base.id,
                2, // depth
                config,
                &mut chunk_counter,
            )
            .await?;

            nodes_created += section_chunks.0;
            chunk_nodes += section_chunks.1;

            // Create table and list nodes for HTML content within this section
            if content_type == ContentType::Html {
                let table_nodes = create_html_tables_under_parent(
                    &ctx,
                    section.start,
                    section.end,
                    &created_section.base.id,
                    2, // depth
                    &mut table_counter,
                )
                .await?;
                nodes_created += table_nodes;

                let list_nodes = create_html_lists_under_parent(
                    &ctx,
                    section.start,
                    section.end,
                    &created_section.base.id,
                    2, // depth
                    &mut list_counter,
                )
                .await?;
                nodes_created += list_nodes;
            }
        }
    } else if config.create_paragraphs {
        // No sections, but create paragraphs directly under root
        let paragraphs = detect_paragraphs(text, 0, text_len);

        if paragraphs.is_empty() {
            // No paragraphs detected, fall back to chunking directly
            let direct_chunks = create_chunks_under_parent(
                &ctx,
                0,
                text_len,
                &created_root.base.id,
                1,
                config,
                &mut chunk_counter,
            )
            .await?;

            nodes_created += direct_chunks.0;
            chunk_nodes += direct_chunks.1;
        } else {
            for (para_idx, para) in paragraphs.iter().enumerate() {
                let mut para_base = BaseFields::new(
                    RecordId::new_v4(TableName::DocNode),
                    space_id.clone(),
                    revision.base.created_by.clone(),
                );
                para_base.security = revision.base.security.clone();
                para_base.subjects = revision.base.subjects.clone();
                para_base.subjects_state = revision.base.subjects_state;
                para_base.provenance = tree_provenance.clone();

                let para_node = DocNode {
                    base: para_base,
                    revision: Link::new(revision_id.clone()),
                    role: NodeRole::Paragraph,
                    span: TextSpan::new(para.start, para.end),
                    node_key: format!("para_{revision_id}_{para_idx}"),
                    text: None,
                    parent: Some(Link::new(created_root.base.id.clone())),
                    #[allow(clippy::cast_possible_wrap)] // Paragraph index fits in i32
                    position: para_idx as i32,
                    depth: 1,
                        embedding_model: None,
                    collection: ctx.collection.clone(),
                };

                let created_para = node_repo
                    .create(para_node)
                    .await
                    .map_err(PipelineError::StorageError)?;
                nodes_created += 1;

                // Create chunks under paragraph
                let para_chunks = create_chunks_under_parent(
                    &ctx,
                    para.start,
                    para.end,
                    &created_para.base.id,
                    2,
                    config,
                    &mut chunk_counter,
                )
                .await?;

                nodes_created += para_chunks.0;
                chunk_nodes += para_chunks.1;
            }
        }

        // Create table and list nodes for HTML content (no sections case)
        if content_type == ContentType::Html {
            let table_nodes = create_html_tables_under_parent(
                &ctx,
                0,
                text_len,
                &created_root.base.id,
                1, // depth
                &mut table_counter,
            )
            .await?;
            nodes_created += table_nodes;

            let list_nodes = create_html_lists_under_parent(
                &ctx,
                0,
                text_len,
                &created_root.base.id,
                1, // depth
                &mut list_counter,
            )
            .await?;
            nodes_created += list_nodes;
        }
    } else {
        // No sections, no paragraphs - original behavior: chunks directly under root
        let chunks = if config.semantic_chunking {
            semantic_split_text(text, config.max_chunk_size, config.chunk_overlap)
        } else {
            split_text_into_chunks(text, config.max_chunk_size, config.chunk_overlap)
        };

        for (i, (start, end)) in chunks.iter().enumerate() {
            let chunk_key = format!("chunk_{revision_id}_{i}");

            let mut chunk_node_base = BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                space_id.clone(),
                revision.base.created_by.clone(),
            );
            chunk_node_base.security = revision.base.security.clone();
            chunk_node_base.subjects = revision.base.subjects.clone();
            chunk_node_base.subjects_state = revision.base.subjects_state;
            chunk_node_base.provenance = tree_provenance.clone();

            let chunk_node = DocNode {
                base: chunk_node_base,
                revision: Link::new(revision_id.clone()),
                role: NodeRole::Chunk,
                span: TextSpan::new(*start, *end),
                node_key: chunk_key,
                text: None,
                parent: Some(Link::new(created_root.base.id.clone())),
                position: i as i32,
                depth: 1,
                embedding_model: None,
                collection: ctx.collection.clone(),
            };

            node_repo
                .create(chunk_node)
                .await
                .map_err(PipelineError::StorageError)?;

            nodes_created += 1;
            chunk_nodes += 1;
        }

        // Create table and list nodes for HTML content (no sections, no paragraphs case)
        if content_type == ContentType::Html {
            let table_nodes = create_html_tables_under_parent(
                &ctx,
                0,
                text_len,
                &created_root.base.id,
                1, // depth
                &mut table_counter,
            )
            .await?;
            nodes_created += table_nodes;

            let list_nodes = create_html_lists_under_parent(
                &ctx,
                0,
                text_len,
                &created_root.base.id,
                1, // depth
                &mut list_counter,
            )
            .await?;
            nodes_created += list_nodes;
        }
    }

    node_repo.finish().await?;

    Ok(BuildDocTreeResult {
        nodes_created,
        chunk_nodes,
        root_node_id: created_root.base.id,
        content_type,
    })
}

/// Builds content (paragraphs and/or chunks) within a section or root.
async fn build_section_content(
    ctx: &DocTreeContext<'_>,
    range_start: u32,
    range_end: u32,
    parent_id: &RecordId,
    base_depth: i32,
    config: &DocTreeConfig,
    chunk_counter: &mut usize,
) -> Result<(usize, usize), PipelineError> {
    let mut nodes_created = 0;
    let mut chunk_nodes = 0;

    if config.create_paragraphs {
        let paragraphs = detect_paragraphs(ctx.text, range_start, range_end);

        if paragraphs.is_empty() {
            // No paragraphs in this range, create chunks directly
            let direct_chunks = create_chunks_under_parent(
                ctx,
                range_start,
                range_end,
                parent_id,
                base_depth,
                config,
                chunk_counter,
            )
            .await?;
            return Ok(direct_chunks);
        }

        for (para_idx, para) in paragraphs.iter().enumerate() {
            let mut para_base = BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                ctx.space_id.to_string(),
                ctx.created_by.to_string(),
            );
            para_base.security = ctx.security.clone();
            para_base.subjects = ctx.subjects.to_vec();
            para_base.subjects_state = ctx.subjects_state;
            para_base.provenance = ctx.provenance.clone();

            let para_node = DocNode {
                base: para_base,
                revision: Link::new(ctx.revision_id.clone()),
                role: NodeRole::Paragraph,
                span: TextSpan::new(para.start, para.end),
                node_key: format!("para_{}_{}_{}", ctx.revision_id, parent_id, para_idx),
                text: None,
                parent: Some(Link::new(parent_id.clone())),
                position: para_idx as i32,
                depth: base_depth,
                embedding_model: None,
                collection: ctx.collection.clone(),
            };

            let created_para = ctx
                .node_repo
                .create(para_node)
                .await
                .map_err(PipelineError::StorageError)?;
            nodes_created += 1;

            // Create chunks under paragraph
            let para_chunks = create_chunks_under_parent(
                ctx,
                para.start,
                para.end,
                &created_para.base.id,
                base_depth + 1,
                config,
                chunk_counter,
            )
            .await?;

            nodes_created += para_chunks.0;
            chunk_nodes += para_chunks.1;
        }
    } else {
        // No paragraphs, create chunks directly under parent
        let direct_chunks = create_chunks_under_parent(
            ctx,
            range_start,
            range_end,
            parent_id,
            base_depth,
            config,
            chunk_counter,
        )
        .await?;

        nodes_created += direct_chunks.0;
        chunk_nodes += direct_chunks.1;
    }

    Ok((nodes_created, chunk_nodes))
}

/// Creates chunk nodes under a given parent within a text range.
async fn create_chunks_under_parent(
    ctx: &DocTreeContext<'_>,
    range_start: u32,
    range_end: u32,
    parent_id: &RecordId,
    depth: i32,
    config: &DocTreeConfig,
    chunk_counter: &mut usize,
) -> Result<(usize, usize), PipelineError> {
    let range_text = &ctx.text[range_start as usize..range_end as usize];

    if range_text.trim().is_empty() {
        return Ok((0, 0));
    }

    let chunks = if config.semantic_chunking {
        semantic_split_text(range_text, config.max_chunk_size, config.chunk_overlap)
    } else {
        split_text_into_chunks(range_text, config.max_chunk_size, config.chunk_overlap)
    };
    let mut nodes_created = 0;

    for (local_idx, (local_start, local_end)) in chunks.iter().enumerate() {
        // Adjust offsets to be relative to the full document
        let abs_start = range_start + local_start;
        let abs_end = range_start + local_end;

        let chunk_key = format!("chunk_{}_{}", ctx.revision_id, *chunk_counter);

        let mut chunk_node_base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            ctx.space_id.to_string(),
            ctx.created_by.to_string(),
        );
        chunk_node_base.security = ctx.security.clone();
        chunk_node_base.subjects = ctx.subjects.to_vec();
        chunk_node_base.subjects_state = ctx.subjects_state;
        chunk_node_base.provenance = ctx.provenance.clone();

        let chunk_node = DocNode {
            base: chunk_node_base,
            revision: Link::new(ctx.revision_id.clone()),
            role: NodeRole::Chunk,
            span: TextSpan::new(abs_start, abs_end),
            node_key: chunk_key,
            text: None,
            parent: Some(Link::new(parent_id.clone())),
            position: local_idx as i32,
            depth,
            embedding_model: None,
            collection: ctx.collection.clone(),
        };

        ctx.node_repo
            .create(chunk_node)
            .await
            .map_err(PipelineError::StorageError)?;

        nodes_created += 1;
        *chunk_counter += 1;
    }

    Ok((nodes_created, nodes_created))
}

/// Creates list nodes within a given range for HTML content.
///
/// Detects `<ul>` and `<ol>` elements and creates List nodes.
#[allow(clippy::cast_possible_wrap)]
async fn create_html_lists_under_parent(
    ctx: &DocTreeContext<'_>,
    range_start: u32,
    range_end: u32,
    parent_id: &RecordId,
    base_depth: i32,
    list_counter: &mut usize,
) -> Result<usize, PipelineError> {
    let mut nodes_created = 0;

    // Detect lists within the range
    let all_lists = detect_html_lists(ctx.text);
    let lists_in_range: Vec<_> = all_lists
        .into_iter()
        .filter(|l| l.start >= range_start && l.end <= range_end)
        .collect();

    for list in lists_in_range {
        // Create list node
        let mut list_base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            ctx.space_id.to_string(),
            ctx.created_by.to_string(),
        );
        list_base.security = ctx.security.clone();
        list_base.subjects = ctx.subjects.to_vec();
        list_base.subjects_state = ctx.subjects_state;
        list_base.provenance = ctx.provenance.clone();

        let list_type_str = match list.list_type {
            ListType::Ordered => "ol",
            ListType::Unordered => "ul",
        };

        let list_node = DocNode {
            base: list_base,
            revision: Link::new(ctx.revision_id.clone()),
            role: NodeRole::List,
            span: TextSpan::new(list.start, list.end),
            node_key: format!(
                "list_{}_{}_{}",
                ctx.revision_id, *list_counter, list_type_str
            ),
            text: None,
            parent: Some(Link::new(parent_id.clone())),
            position: *list_counter as i32,
            depth: base_depth,
            embedding_model: None,
            collection: ctx.collection.clone(),
        };

        ctx.node_repo
            .create(list_node)
            .await
            .map_err(PipelineError::StorageError)?;
        nodes_created += 1;
        *list_counter += 1;
    }

    Ok(nodes_created)
}

/// Creates table nodes and their cell children within a given range for HTML content.
///
/// Detects `<table>` elements and creates Table nodes with TableCell children.
#[allow(clippy::cast_possible_wrap)]
async fn create_html_tables_under_parent(
    ctx: &DocTreeContext<'_>,
    range_start: u32,
    range_end: u32,
    parent_id: &RecordId,
    base_depth: i32,
    table_counter: &mut usize,
) -> Result<usize, PipelineError> {
    let mut nodes_created = 0;

    // Detect tables within the range
    let all_tables = detect_html_tables(ctx.text);
    let tables_in_range: Vec<_> = all_tables
        .into_iter()
        .filter(|t| t.start >= range_start && t.end <= range_end)
        .collect();

    for table in tables_in_range {
        // Create table node
        let mut table_base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            ctx.space_id.to_string(),
            ctx.created_by.to_string(),
        );
        table_base.security = ctx.security.clone();
        table_base.subjects = ctx.subjects.to_vec();
        table_base.subjects_state = ctx.subjects_state;
        table_base.provenance = ctx.provenance.clone();

        let table_node = DocNode {
            base: table_base,
            revision: Link::new(ctx.revision_id.clone()),
            role: NodeRole::Table,
            span: TextSpan::new(table.start, table.end),
            node_key: format!("table_{}_{}", ctx.revision_id, *table_counter),
            text: None,
            parent: Some(Link::new(parent_id.clone())),
            position: *table_counter as i32,
            depth: base_depth,
            embedding_model: None,
            collection: ctx.collection.clone(),
        };

        let created_table = ctx
            .node_repo
            .create(table_node)
            .await
            .map_err(PipelineError::StorageError)?;
        nodes_created += 1;
        *table_counter += 1;

        // Detect and create cell nodes
        let cells = detect_html_table_cells(ctx.text, table.start, table.end);
        for (cell_idx, cell) in cells.iter().enumerate() {
            let mut cell_base = BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                ctx.space_id.to_string(),
                ctx.created_by.to_string(),
            );
            cell_base.security = ctx.security.clone();
            cell_base.subjects = ctx.subjects.to_vec();
            cell_base.subjects_state = ctx.subjects_state;
            cell_base.provenance = ctx.provenance.clone();

            let cell_node = DocNode {
                base: cell_base,
                revision: Link::new(ctx.revision_id.clone()),
                role: NodeRole::TableCell,
                span: TextSpan::new(cell.start, cell.end),
                node_key: format!(
                    "cell_{}_{}_{}_{}",
                    ctx.revision_id,
                    *table_counter - 1,
                    cell_idx,
                    if cell.is_header { "h" } else { "d" }
                ),
                text: None,
                parent: Some(Link::new(created_table.base.id.clone())),
                position: cell_idx as i32,
                depth: base_depth + 1,
                embedding_model: None,
                collection: ctx.collection.clone(),
            };

            ctx.node_repo
                .create(cell_node)
                .await
                .map_err(PipelineError::StorageError)?;
            nodes_created += 1;
        }
    }

    Ok(nodes_created)
}

/// Executes the embed_chunks work kind.
///
/// Generates embeddings for chunk nodes belonging to a revision.
///
/// # Errors
///
/// Returns an error if node retrieval, embedding generation, or storage fails.
pub async fn execute_embed_chunks<P: EmbeddingProvider>(
    storage: &StorageClient,
    revision_id: &RecordId,
    embed_provider: &P,
) -> Result<EmbedChunksResult, PipelineError> {
    let node_repo = DocNodeRepository::new(storage.clone());
    let revision_repo = DocRevisionRepository::new(storage.clone());
    let partition_manager = PartitionManager::new();
    let embedding_repo = NodeEmbeddingRepository::new(storage.clone(), partition_manager);

    // Fetch the revision to get the text. Point get by id instead of loading
    // every active revision just to .find() this one.
    let revision = revision_repo
        .get_by_id(revision_id)
        .await
        .map_err(PipelineError::StorageError)?
        .filter(|revision| revision.status == RevisionStatus::Active)
        .ok_or_else(|| PipelineError::NotFound(revision_id.to_string()))?;

    let revision_text = &revision.text;

    // Find all chunk nodes for this revision
    let chunk_nodes = node_repo
        .find_chunks_by_revision(revision_id)
        .await
        .map_err(PipelineError::StorageError)?;

    if chunk_nodes.is_empty() {
        return Ok(EmbedChunksResult {
            chunks_embedded: 0,
            embedded_node_ids: vec![],
            model: embed_provider.model().to_string(),
        });
    }

    // Extract text for each chunk using its span. Same discipline as the
    // embed worker: skip a span that lies outside the text, clamp one that
    // merely runs past the end, and never let one bad node fail the batch.
    let mut kept_nodes = Vec::with_capacity(chunk_nodes.len());
    let mut texts: Vec<String> = Vec::with_capacity(chunk_nodes.len());
    for node in chunk_nodes {
        let start = node.span.start as usize;
        let mut end = node.span.end as usize;

        if start >= revision_text.len() || start > end {
            tracing::warn!(
                node = %node.base.id,
                revision = %revision_id,
                span_start = start,
                span_end = end,
                text_len = revision_text.len(),
                "Chunk span lies outside the revision text; skipping node"
            );
            continue;
        }
        if end > revision_text.len() {
            tracing::warn!(
                node = %node.base.id,
                revision = %revision_id,
                span_start = start,
                span_end = end,
                text_len = revision_text.len(),
                "Chunk span clamped to the revision text"
            );
            end = revision_text.len();
        }
        // Adjust to char boundaries to avoid panicking on multi-byte UTF-8
        let mut safe_start = start;
        while safe_start > 0 && !revision_text.is_char_boundary(safe_start) {
            safe_start -= 1;
        }
        let mut safe_end = end;
        while safe_end < revision_text.len() && !revision_text.is_char_boundary(safe_end) {
            safe_end += 1;
        }

        texts.push(revision_text[safe_start..safe_end].to_string());
        kept_nodes.push(node);
    }
    let chunk_nodes = kept_nodes;

    if chunk_nodes.is_empty() {
        return Ok(EmbedChunksResult {
            chunks_embedded: 0,
            embedded_node_ids: vec![],
            model: embed_provider.model().to_string(),
        });
    }

    // Generate embeddings for all chunks
    let embeddings = embed_provider.embed(&texts).await?;

    // Store embeddings in NodeEmbeddingRepository
    let mut embedded_node_ids = Vec::with_capacity(chunk_nodes.len());
    for (node, embedding) in chunk_nodes.iter().zip(embeddings.into_iter()) {
        let mut base = BaseFields::new(
            node.base.id.clone(),
            node.base.space_id.clone(),
            node.base.created_by.clone(),
        );
        base.security = node.base.security.clone();
        base.subjects = node.base.subjects.clone();
        base.subjects_state = node.base.subjects_state;
        base.provenance = node.base.provenance.clone();

        let chunk_embedding = NodeEmbedding {
            base,
            node: Link::new(node.base.id.clone()),
            model: embed_provider.model().to_string(),
            dims: i32::try_from(embedding.len()).unwrap_or(0),
            vector: embedding,
        };

        embedding_repo
            .store_embedding(chunk_embedding)
            .await
            .map_err(PipelineError::StorageError)?;

        embedded_node_ids.push(node.base.id.clone());
    }

    Ok(EmbedChunksResult {
        chunks_embedded: embedded_node_ids.len(),
        embedded_node_ids,
        model: embed_provider.model().to_string(),
    })
}

/// Splits text into chunks with overlap.
///
/// Returns a vector of (start, end) byte offsets on UTF-8 character boundaries.
/// A single character may exceed the byte budget rather than being split.
fn split_text_into_chunks(text: &str, max_size: usize, overlap: usize) -> Vec<(u32, u32)> {
    if text.is_empty() || max_size == 0 {
        return vec![];
    }

    let text_len = text.len();
    if text_len <= max_size {
        return vec![(0, text_len as u32)];
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;

    while start < text_len {
        let end = text.floor_char_boundary(start.saturating_add(max_size).min(text_len));
        let end = if end == start {
            text.ceil_char_boundary(start + 1)
        } else {
            end
        };

        // Try to break at a word boundary
        let actual_end = if end < text_len {
            let word_end = find_word_boundary(text, end);
            if word_end > start {
                word_end
            } else {
                end
            }
        } else {
            end
        };

        chunks.push((start as u32, actual_end as u32));

        if actual_end >= text_len {
            break;
        }

        // Round overlap forward so it cannot begin inside a UTF-8 character.
        let next_start = text.ceil_char_boundary(actual_end.saturating_sub(overlap));

        // Even an overlap larger than the chunk must advance by one character.
        start = if next_start <= start {
            text.ceil_char_boundary(start + 1)
        } else {
            next_start
        };
    }

    chunks
}

/// Finds a word boundary near the target position.
fn find_word_boundary(text: &str, target: usize) -> usize {
    // Look backwards for whitespace
    let mut pos = target;
    let bytes = text.as_bytes();

    while pos > 0 && pos > target.saturating_sub(50) {
        if bytes[pos - 1].is_ascii_whitespace() {
            return pos;
        }
        pos -= 1;
    }

    // No good boundary found, use target
    target
}

/// Finds sentence boundary positions in text.
/// Returns positions after sentence-ending punctuation (. ! ?) followed by space or newline.
fn find_sentence_boundaries(text: &str) -> Vec<usize> {
    let mut boundaries = Vec::new();
    let bytes = text.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        if b == b'.' || b == b'!' || b == b'?' {
            // Check if followed by whitespace (indicating sentence end)
            if i + 1 < bytes.len() {
                let next = bytes[i + 1];
                if next == b' ' || next == b'\n' || next == b'\r' {
                    boundaries.push(i + 1);
                }
            } else {
                // End of text
                boundaries.push(i + 1);
            }
        }
    }

    boundaries
}

/// Splits text into chunks respecting sentence boundaries.
///
/// Chunks will not split mid-sentence. If a sentence is longer than max_size,
/// it will be placed in its own chunk (may exceed max_size).
#[allow(clippy::cast_possible_truncation)]
fn semantic_split_text(text: &str, max_size: usize, overlap: usize) -> Vec<(u32, u32)> {
    if text.is_empty() || max_size == 0 {
        return Vec::new();
    }

    let boundaries = find_sentence_boundaries(text);
    let text_len = text.len();

    // If no sentence boundaries found, fall back to fixed-size chunking
    if boundaries.is_empty() {
        return split_text_into_chunks(text, max_size, overlap);
    }

    let mut chunks = Vec::new();
    let mut chunk_start = 0usize;
    let mut last_boundary = 0usize;

    for &boundary in &boundaries {
        // Check if adding this sentence would exceed max_size
        let potential_end = boundary;
        let chunk_len = potential_end - chunk_start;

        if chunk_len > max_size && last_boundary > chunk_start {
            // Current chunk is full, close it at last boundary
            chunks.push((chunk_start as u32, last_boundary as u32));

            // Start new chunk with overlap
            chunk_start = if last_boundary > overlap {
                // Find a sentence boundary for overlap start
                let overlap_target = last_boundary - overlap;
                boundaries
                    .iter()
                    .filter(|&&b| b <= last_boundary && b >= overlap_target)
                    .copied()
                    .next_back()
                    .unwrap_or(last_boundary)
            } else {
                0
            };
        }

        last_boundary = boundary;
    }

    // Add final chunk if there's remaining content
    if chunk_start < text_len {
        chunks.push((chunk_start as u32, text_len as u32));
    }

    chunks
}

/// Validates that a span is within the revision text bounds.
///
/// Per spec §8.5: evidence spans must be within revision text bounds.
///
/// # Errors
///
/// Returns an error if the span is out of bounds.
pub fn validate_span(span: &TextSpan, text_len: u32) -> Result<(), PipelineError> {
    if !span.is_valid(text_len) {
        return Err(PipelineError::InvalidInput(format!(
            "Span [{}, {}) is out of bounds for text length {}",
            span.start, span.end, text_len
        )));
    }
    Ok(())
}

/// Validates all nodes in a tree have valid spans.
///
/// # Errors
///
/// Returns an error if any node span is out of bounds.
pub fn validate_node_spans(nodes: &[DocNode], text_len: u32) -> Result<(), PipelineError> {
    for node in nodes {
        validate_span(&node.span, text_len)?;
    }
    Ok(())
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
    use crate::renderer::default::PassthroughRenderer;
    use crate::renderer::multimodal::{FakeMediaRender, MultimodalRenderer};
    use crate::renderer::{RenderContext, RenderResult, Renderer};
    use elephant_types::records::base::Security;
    use elephant_types::records::doc::{DocMediaAttachment, DocMediaKind, DocMediaLocation};
    use elephant_types::records::doc_revision::RendererMetadata;
    use elephant_types::records::work::PluginManifest;

    #[derive(Debug)]
    struct AlwaysFailRenderer {
        manifest: PluginManifest,
        source_types: Vec<String>,
    }

    impl AlwaysFailRenderer {
        fn new() -> Self {
            Self {
                manifest: PluginManifest {
                    plugin_id: "always-fail".to_string(),
                    version: "test".to_string(),
                    plugin_type: "renderer".to_string(),
                    required_scopes: Vec::new(),
                    tool_access: Vec::new(),
                    work_kinds: Vec::new(),
                    determinism: Default::default(),
                    max_tokens: None,
                    max_wall_time_secs: None,
                    input_types: Vec::new(),
                    output_types: Vec::new(),
                    validator_profile: None,
                    config_schema: None,
                    health_check: None,
                    retry_policy: Default::default(),
                },
                source_types: vec!["text/plain".to_string()],
            }
        }

        fn metadata(&self) -> RendererMetadata {
            RendererMetadata {
                name: "always-fail".to_string(),
                version: "test".to_string(),
                config_hash: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl Renderer for AlwaysFailRenderer {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }

        fn source_types(&self) -> &[String] {
            &self.source_types
        }

        async fn render(&self, _ctx: RenderContext) -> Result<RenderResult, PipelineError> {
            Ok(RenderResult::failed(
                "synthetic render failure".to_string(),
                self.metadata(),
            ))
        }
    }

    async fn render_fixture(
        storage: &StorageClient,
        registry: &RendererRegistry,
        doc: &Doc,
        source_type: Option<&str>,
    ) -> Result<RenderDocResult, PipelineError> {
        use elephant_storage::{Repository, SourceRepository};
        use elephant_types::records::source::{Source, SourceType};
        let repository = DocRepository::new(storage.clone());
        let existing = repository.get(&doc.base.space_id, &doc.base.id).await?;
        let mut doc = doc.clone();
        let source_id = doc
            .source
            .as_ref()
            .or_else(|| existing.as_ref().and_then(|doc| doc.source.as_ref()))
            .map_or_else(
                || RecordId::new_v4(TableName::Source),
                |source| source.id().clone(),
            );
        let sources = SourceRepository::new(storage.clone());
        if let Some(mut source) = sources.get(&doc.base.space_id, &source_id).await? {
            source.hash = crate::extraction::AssertionValidator::hash_quote(&doc.text);
            sources
                .update(&doc.base.space_id, &source_id, source)
                .await?;
        } else {
            let mut base = doc.base.clone();
            base.id = source_id.clone();
            sources
                .create(Source::new(
                    base,
                    SourceType::Manual,
                    crate::extraction::AssertionValidator::hash_quote(&doc.text),
                ))
                .await?;
        }
        doc.source = Some(Link::new(source_id));
        if existing.is_some() {
            repository
                .update(&doc.base.space_id, &doc.base.id, doc.clone())
                .await?;
        } else {
            repository.create(doc.clone()).await?;
        }
        super::execute_render_doc(storage, registry, &doc, source_type).await
    }

    fn make_doc(text: &str) -> Doc {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "default".to_string(),
            "test".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        Doc {
            base,
            title: "Test Document".to_string(),
            text: text.to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("test_{}", uuid::Uuid::new_v4()),
            metadata: serde_json::json!({}),
        }
    }

    fn make_media_doc(sidecar_text: &str) -> Doc {
        let mut doc = make_doc("");
        doc.sidecar_text = Some(sidecar_text.to_string());
        doc.media_attachments = vec![DocMediaAttachment {
            attachment_id: "att-1".to_string(),
            kind: DocMediaKind::Image,
            media_type: "image/jpeg".to_string(),
            location: DocMediaLocation::Path {
                path: "/tmp/frame.jpg".to_string(),
            },
            label: Some("frame.jpg".to_string()),
            size_bytes: None,
            duration_ms: None,
            sha256: None,
        }];
        doc
    }

    async fn accepted_render_fixture(
        storage: &StorageClient,
        text: &str,
    ) -> (Source, Doc, PublicationFence) {
        use elephant_storage::source_publication::{
            AcceptSourceRevision, SourcePublicationRepository,
        };
        use elephant_types::publication::SourcePrecondition;
        use elephant_types::records::source::{ManualDocTemplate, SourceType};
        let mut doc = make_doc(text);
        doc.base.space_id = "render-replay".into();
        doc.metadata = serde_json::json!({});
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(serde_json::json!({
            "id": RecordId::new_v4(TableName::SpaceConfig), "space_id":doc.base.space_id,
            "created_at": Utc::now(), "updated_at": Utc::now(), "created_by":"test", "deleted_at": null,
            "security": {"policy_id":"default", "level":0, "labels":[], "handling":[]},
            "provenance": {}, "subjects":[], "subjects_state":"complete",
            "embedding_provider":"fake", "embedding_model":"unused", "embedding_dims":8
        })).expect("named source fixture configuration");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("named space");
        let mut base = doc.base.clone();
        base.id = RecordId::new_v4(TableName::Source);
        let mut source = Source::new(
            base,
            SourceType::Manual,
            crate::extraction::AssertionValidator::hash_quote(text),
        );
        source
            .set_manual_doc_template(&ManualDocTemplate {
                doc_id: doc.base.id.to_string(),
                title: doc.title.clone(),
                text: doc.text.clone(),
                sidecar_text: None,
                media_attachments: Vec::new(),
                rendition_key: doc.rendition_key.clone(),
                metadata: serde_json::json!({}),
            })
            .expect("immutable source input");
        let receipt = SourcePublicationRepository::new(storage.clone())
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "revision-1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "render-replay-1".into(),
                request_fingerprint: source.hash.clone(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .expect("real source acceptance");
        let publication = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: "revision-1".into(),
            operation_id: receipt.operation_id,
        };
        doc.base.id =
            crate::source_queue::accepted_document_id(&publication, &doc.base.id.to_string())
                .expect("accepted doc identity");
        doc.source = Some(Link::new(source.base.id.clone()));
        doc.metadata["_publication"] = serde_json::to_value(&publication).expect("fence metadata");
        let doc = DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("accepted document");
        (source, doc, publication)
    }

    struct CountingRenderer {
        delegate: PassthroughRenderer,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Renderer for CountingRenderer {
        fn manifest(&self) -> &PluginManifest {
            self.delegate.manifest()
        }
        fn source_types(&self) -> &[String] {
            self.delegate.source_types()
        }
        async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut rendered = self.delegate.render(ctx).await?;
            rendered.text = format!("Rendered: {}", rendered.text);
            rendered.text_hash = crate::extraction::AssertionValidator::hash_quote(&rendered.text);
            Ok(rendered)
        }
    }

    #[tokio::test]
    async fn accepted_render_reuses_exact_revision_without_redispatch() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let (_, mut doc, _) = accepted_render_fixture(&storage, "immutable source text").await;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(CountingRenderer {
            delegate: PassthroughRenderer::new(),
            calls: calls.clone(),
        }));
        // A mutable document projection is never the accepted source input.
        doc.text = "untrusted changed projection".into();
        let first = execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        DocRepository::new(storage.clone())
            .update_render_projection(&doc.base.space_id, &doc.base.id, "immutable source text")
            .await
            .expect("simulate materialization retry");
        let second = execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("replay");
        assert_eq!(first.revision_id, second.revision_id);
        assert_eq!(second.superseded_id, None);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let revisions = DocRevisionRepository::new(storage.clone())
            .find_by_doc(&doc.base.id)
            .await
            .expect("revisions");
        assert_eq!(revisions.len(), 1);
        assert_eq!(revisions[0].text, "Rendered: immutable source text");
        assert_eq!(revisions[0].status, RevisionStatus::Active);
        let stored = DocRepository::new(storage)
            .get(&doc.base.space_id, &doc.base.id)
            .await
            .expect("document")
            .expect("present");
        assert_eq!(stored.text, revisions[0].text);
    }

    #[tokio::test]
    async fn failed_accepted_render_can_retry_without_replacing_a_revision() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let (_, doc, _) = accepted_render_fixture(&storage, "immutable source text").await;
        let mut failed_registry = RendererRegistry::new();
        failed_registry.register(Box::new(AlwaysFailRenderer::new()));
        let failed = execute_render_doc(&storage, &failed_registry, &doc, Some("text/plain"))
            .await
            .expect("failed render outcome");
        assert!(!failed.success);
        let revisions = DocRevisionRepository::new(storage.clone());
        assert!(revisions
            .find_by_doc(&doc.base.id)
            .await
            .expect("revisions")
            .is_empty());
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        let successful = execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("successful retry");
        assert!(successful.success);
        assert_eq!(failed.revision_id, successful.revision_id);
        assert_eq!(
            revisions
                .find_by_doc(&doc.base.id)
                .await
                .expect("revisions")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn accepted_render_rejects_changed_recipe_before_new_provider_call() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let (_, doc, _) = accepted_render_fixture(&storage, "immutable source text").await;
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        let first = execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        let mut changed_registry = RendererRegistry::new();
        changed_registry.register(Box::new(AlwaysFailRenderer::new()));
        assert!(
            execute_render_doc(&storage, &changed_registry, &doc, Some("text/plain"))
                .await
                .is_err()
        );
        let revision = DocRevisionRepository::new(storage)
            .get(&doc.base.space_id, &first.revision_id)
            .await
            .expect("revision")
            .expect("present");
        assert_eq!(revision.status, RevisionStatus::Active);
        assert_eq!(revision.text, "immutable source text");
    }

    #[tokio::test]
    async fn accepted_render_refuses_stale_fence_even_when_old_revision_exists() {
        use elephant_storage::source_publication::{
            AcceptSourceRevision, SourcePublicationRepository,
        };
        use elephant_types::publication::SourcePrecondition;
        let storage = StorageClient::new_embedded().await.expect("storage");
        let (mut source, doc, fence) =
            accepted_render_fixture(&storage, "immutable source text").await;
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        let repository = SourcePublicationRepository::new(storage.clone());
        let authorized = repository
            .authorization_snapshot(&doc.base.space_id, &source.base.id)
            .await
            .expect("authorization")
            .expect("present");
        source.hash = crate::extraction::AssertionValidator::hash_quote("replacement");
        let mut template = source
            .manual_doc_template()
            .expect("template")
            .expect("present");
        template.text = "replacement".into();
        source
            .set_manual_doc_template(&template)
            .expect("replacement input");
        repository
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "revision-2".into(),
                expected: SourcePrecondition::Generation(fence.generation),
                idempotency_key: "render-replay-2".into(),
                request_fingerprint: source.hash.clone(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: Some(authorized),
                authorized_document: None,
            })
            .await
            .expect("replacement accepted");
        assert!(
            execute_render_doc(&storage, &registry, &doc, Some("text/plain"))
                .await
                .is_err()
        );
        assert!(execute_build_doc_tree(
            &storage,
            &DocRevisionRepository::new(storage.clone())
                .find_by_doc(&doc.base.id)
                .await
                .expect("revision")[0]
                .base
                .id,
            &DocTreeConfig::default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn tree_replay_repairs_missing_chunk_with_original_identity() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc(
            "The first paragraph has evidence.\n\nThe second paragraph has more evidence.",
        );
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        let rendered = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        let config = DocTreeConfig::default();
        let first = execute_build_doc_tree(&storage, &rendered.revision_id, &config)
            .await
            .expect("tree");
        let repository = DocNodeRepository::new(storage.clone());
        let before = repository
            .find_by_revision(&rendered.revision_id)
            .await
            .expect("nodes");
        let missing = before
            .iter()
            .find(|node| node.role == NodeRole::Chunk)
            .expect("chunk");
        storage
            .query_bind_checked(
                "DELETE $node;",
                (
                    "node",
                    elephant_storage::surreal::thing_from_record_id(&missing.base.id),
                ),
            )
            .await
            .expect("simulate interrupted storage");
        let second = execute_build_doc_tree(&storage, &rendered.revision_id, &config)
            .await
            .expect("complete replay");
        assert_eq!(first.root_node_id, second.root_node_id);
        let after = repository
            .find_by_revision(&rendered.revision_id)
            .await
            .expect("nodes");
        assert_eq!(before.len(), after.len());
        for expected in &before {
            let actual = after
                .iter()
                .find(|node| node.base.id == expected.base.id)
                .expect("original node identity");
            validate_node_replay(actual, expected).expect("original evidence and shape");
            if expected.base.id != missing.base.id {
                assert_eq!(actual, expected);
            }
        }
        let changed_config = DocTreeConfig {
            max_chunk_size: 20,
            chunk_overlap: 5,
            ..config
        };
        assert!(
            execute_build_doc_tree(&storage, &rendered.revision_id, &changed_config)
                .await
                .is_err()
        );
        let unchanged = repository
            .find_by_revision(&rendered.revision_id)
            .await
            .expect("nodes");
        assert_eq!(unchanged.len(), after.len());
        assert!(after.iter().all(|node| unchanged.contains(node)));
    }

    #[tokio::test]
    async fn tree_replay_does_not_accept_an_unexpected_node_as_complete() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc("An exact immutable evidence paragraph.");
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        let rendered = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        execute_build_doc_tree(&storage, &rendered.revision_id, &DocTreeConfig::default())
            .await
            .expect("tree");
        let repository = DocNodeRepository::new(storage.clone());
        let mut unexpected = repository
            .find_by_revision(&rendered.revision_id)
            .await
            .expect("nodes")[0]
            .clone();
        unexpected.base.id = RecordId::new_v4(TableName::DocNode);
        unexpected.node_key = "unplanned-node".into();
        repository
            .create(unexpected)
            .await
            .expect("unexpected node");
        assert!(
            execute_build_doc_tree(&storage, &rendered.revision_id, &DocTreeConfig::default())
                .await
                .is_err()
        );
    }

    #[test]
    fn test_split_text_into_chunks_empty() {
        let chunks = split_text_into_chunks("", 100, 20);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_text_into_chunks_small() {
        let chunks = split_text_into_chunks("Hello", 100, 20);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (0, 5));
    }

    #[test]
    fn test_split_text_into_chunks_unicode_targets_and_overlap() {
        assert_eq!(
            split_text_into_chunks("a\u{e9}b", 2, 0),
            vec![(0, 1), (1, 3), (3, 4)]
        );
        assert_eq!(
            split_text_into_chunks("ab\u{e9} cd", 5, 2),
            vec![(0, 5), (4, 7)]
        );
        assert!(split_text_into_chunks("\u{732b}", 0, 0).is_empty());
    }

    #[test]
    fn test_fixed_and_semantic_fallback_chunks_preserve_unicode_coverage() {
        let ascii = format!("a {}", "b".repeat(80));
        for text in ["a\u{e9}\u{732b}\u{1f642} \t\r\nb\u{732b}", ascii.as_str()] {
            for max_size in [1, 2, 3, 4, 5, 8, usize::MAX] {
                for overlap in [0, 1, 2, 7, usize::MAX] {
                    for chunks in [
                        split_text_into_chunks(text, max_size, overlap),
                        semantic_split_text(text, max_size, overlap),
                    ] {
                        assert_eq!(chunks.first().unwrap().0, 0);
                        assert_eq!(chunks.last().unwrap().1 as usize, text.len());
                        assert!(chunks.len() <= text.chars().count());
                        for &(start, end) in &chunks {
                            assert!(start < end);
                            assert!(text.get(start as usize..end as usize).is_some());
                        }
                        for pair in chunks.windows(2) {
                            assert!(pair[1].0 > pair[0].0, "starts must advance");
                            assert!(pair[1].0 <= pair[0].1, "coverage must not have gaps");
                            assert!(pair[1].1 >= pair[0].1, "ends must not move backward");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_split_text_into_chunks_multiple() {
        let text =
            "This is a longer text that should be split into multiple chunks for testing purposes.";
        let chunks = split_text_into_chunks(text, 20, 5);

        assert!(chunks.len() > 1);

        // Verify chunks cover all text
        let first_start = chunks.first().unwrap().0;
        let last_end = chunks.last().unwrap().1;
        assert_eq!(first_start, 0);
        assert_eq!(last_end as usize, text.len());

        // Verify no gaps (with overlap allowed)
        for window in chunks.windows(2) {
            let end_prev = window[0].1;
            let start_next = window[1].0;
            assert!(
                start_next <= end_prev,
                "Chunks should overlap or be adjacent: {end_prev} vs {start_next}"
            );
        }
    }

    #[test]
    fn test_find_sentence_boundaries() {
        let text = "Hello world. This is a test! How are you? Fine.";
        let boundaries = find_sentence_boundaries(text);
        assert!(boundaries.contains(&12)); // After "Hello world."
        assert!(boundaries.contains(&28)); // After "This is a test!"
        assert!(boundaries.contains(&41)); // After "How are you?"
        assert!(boundaries.contains(&47)); // After "Fine." (end of text)
    }

    #[test]
    fn test_find_sentence_boundaries_no_sentences() {
        let text = "No punctuation here";
        let boundaries = find_sentence_boundaries(text);
        assert!(boundaries.is_empty());
    }

    #[test]
    fn test_semantic_split_respects_sentences() {
        let text = "First sentence. Second sentence. Third sentence.";
        let chunks = semantic_split_text(text, 20, 0);

        // Verify no chunk splits mid-sentence
        for (start, end) in &chunks {
            let chunk_text = &text[*start as usize..*end as usize];
            // Each chunk should end with sentence-ending punctuation or be at text end
            let trimmed = chunk_text.trim();
            assert!(
                trimmed.ends_with('.')
                    || trimmed.ends_with('!')
                    || trimmed.ends_with('?')
                    || *end as usize == text.len(),
                "Chunk '{}' doesn't end at sentence boundary",
                chunk_text
            );
        }
    }

    #[test]
    fn test_semantic_split_handles_long_sentence() {
        let text = "This is a very long sentence that exceeds the maximum chunk size limit.";
        let chunks = semantic_split_text(text, 20, 0);
        // Should still produce chunks even if sentence is long
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_semantic_split_empty() {
        let chunks = semantic_split_text("", 100, 20);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_semantic_split_falls_back_no_boundaries() {
        let text = "No punctuation in this text at all";
        let chunks = semantic_split_text(text, 10, 2);
        // Should fall back to fixed-size chunking
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_validate_span_valid() {
        let span = TextSpan::new(10, 20);
        assert!(validate_span(&span, 100).is_ok());
        assert!(validate_span(&span, 20).is_ok());
    }

    #[test]
    fn test_validate_span_invalid() {
        let span = TextSpan::new(10, 20);
        assert!(validate_span(&span, 15).is_err()); // end > text_len
        assert!(validate_span(&span, 5).is_err()); // start > text_len
    }

    #[test]
    fn test_validate_span_start_greater_than_end() {
        let span = TextSpan::new(20, 10); // Invalid: start > end
        assert!(validate_span(&span, 100).is_err());
    }

    #[tokio::test]
    async fn test_render_doc_success() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc("Hello World");

        // Create a simple registry with passthrough renderer
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        assert!(result.success);
        assert!(result.superseded_id.is_none());
    }

    #[tokio::test]
    async fn test_render_doc_persists_manifest_and_updates_doc_text_for_media() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let doc = doc_repo
            .create(make_media_doc("User note: keep this"))
            .await
            .expect("create media doc");

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(MultimodalRenderer::for_tests(vec![
            FakeMediaRender {
                attachment_id: "att-1".to_string(),
                derived_text: "Visible page title: Triton kernels".to_string(),
            },
        ])));

        let result = render_fixture(&storage, &registry, &doc, Some("image/jpeg"))
            .await
            .expect("render media doc");

        let stored_doc = doc_repo
            .get(&doc.base.space_id, &doc.base.id)
            .await
            .expect("get stored doc")
            .expect("stored doc exists");
        assert!(stored_doc
            .text
            .contains("Visible page title: Triton kernels"));
        assert_eq!(
            stored_doc.sidecar_text.as_deref(),
            Some("User note: keep this")
        );

        let revision = revision_repo
            .get(&doc.base.space_id, &result.revision_id)
            .await
            .expect("get revision")
            .expect("revision exists");
        assert!(revision.manifest.is_some());
        assert_eq!(
            revision
                .manifest
                .as_ref()
                .expect("manifest")
                .attachments
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_render_doc_failure_does_not_overwrite_existing_doc_text() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let doc = doc_repo
            .create(make_doc("Last known good projection"))
            .await
            .expect("create doc");

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(AlwaysFailRenderer::new()));

        let result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render should persist failed revision");

        let stored_doc = doc_repo
            .get(&doc.base.space_id, &doc.base.id)
            .await
            .expect("get stored doc")
            .expect("stored doc exists");
        assert_eq!(stored_doc.text, "Last known good projection");

        let revision = revision_repo
            .get(&doc.base.space_id, &result.revision_id)
            .await
            .expect("get revision")
            .expect("revision exists");
        assert_eq!(revision.status, RevisionStatus::Failed);
        assert_eq!(revision.text, "synthetic render failure");
    }

    #[tokio::test]
    async fn test_render_doc_failure_does_not_supersede_last_active_revision() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let doc = make_doc("Initial text");

        let mut success_registry = RendererRegistry::new();
        success_registry.register(Box::new(PassthroughRenderer::new()));
        let success = render_fixture(&storage, &success_registry, &doc, Some("text/plain"))
            .await
            .expect("initial render");

        let mut failure_registry = RendererRegistry::new();
        failure_registry.register(Box::new(AlwaysFailRenderer::new()));
        let failed = render_fixture(&storage, &failure_registry, &doc, Some("text/plain"))
            .await
            .expect("failed rerender");

        let active_revision = revision_repo
            .get(&doc.base.space_id, &success.revision_id)
            .await
            .expect("get active revision")
            .expect("active revision exists");
        assert_eq!(active_revision.status, RevisionStatus::Active);

        let failed_revision = revision_repo
            .get(&doc.base.space_id, &failed.revision_id)
            .await
            .expect("get failed revision")
            .expect("failed revision exists");
        assert_eq!(failed_revision.status, RevisionStatus::Failed);
        assert!(failed_revision.supersedes.is_none());
    }

    #[tokio::test]
    async fn test_build_doc_tree_creates_nodes() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc("This is a test document with enough content to create chunks.");

        // First render the doc
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build the doc tree
        let config = DocTreeConfig {
            max_chunk_size: 20,
            chunk_overlap: 5,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        assert!(tree_result.nodes_created >= 2); // root + at least 1 chunk
        assert!(tree_result.chunk_nodes >= 1);
    }

    #[tokio::test]
    async fn test_build_doc_tree_default_unicode_fallback_spans() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let text = "\u{732b}".repeat(400);
        let doc = make_doc(&text);
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));
        let rendered = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");
        let config = DocTreeConfig::default();

        let result = execute_build_doc_tree(&storage, &rendered.revision_id, &config)
            .await
            .expect("build tree");
        let mut chunks = DocNodeRepository::new(storage)
            .find_chunks_by_revision(&rendered.revision_id)
            .await
            .expect("stored chunks");
        chunks.sort_by_key(|node| node.span.start);

        assert!(chunks.len() > 1);
        assert_eq!(chunks.len(), result.chunk_nodes);
        assert_eq!(chunks.first().unwrap().span.start, 0);
        assert_eq!(chunks.last().unwrap().span.end as usize, text.len());
        for chunk in &chunks {
            let excerpt = text
                .get(chunk.span.start as usize..chunk.span.end as usize)
                .expect("stored span must align with original UTF-8 text");
            assert!(!excerpt.is_empty());
            assert!(excerpt.len() <= config.max_chunk_size);
        }
        for pair in chunks.windows(2) {
            assert!(pair[1].span.start > pair[0].span.start);
            assert!(pair[1].span.start <= pair[0].span.end);
        }
    }

    #[tokio::test]
    async fn test_supersession_handling() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc("Initial content");

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        // First render
        let result1 = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render 1");
        assert!(result1.superseded_id.is_none());

        // Second render (should supersede first)
        let result2 = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render 2");
        assert_eq!(result2.superseded_id, Some(result1.revision_id.clone()));

        // Verify first revision is now superseded
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let old_rev = revision_repo
            .get(&doc.base.space_id, &result1.revision_id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(old_rev.status, RevisionStatus::Superseded);
    }

    #[tokio::test]
    async fn test_embed_chunks_success() {
        use elephant_embeddings::FakeEmbeddingProvider;

        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc(
            "This is a test document with enough content to create multiple chunks for embedding.",
        );

        // Render the doc
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build the doc tree to create chunk nodes
        let config = DocTreeConfig {
            max_chunk_size: 30,
            chunk_overlap: 5,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        assert!(
            tree_result.chunk_nodes >= 1,
            "Should have at least one chunk node"
        );

        // Embed the chunks
        let provider = FakeEmbeddingProvider::new("test-model", 384);
        let embed_result = execute_embed_chunks(&storage, &render_result.revision_id, &provider)
            .await
            .expect("embed chunks");

        assert_eq!(embed_result.chunks_embedded, tree_result.chunk_nodes);
        assert_eq!(
            embed_result.embedded_node_ids.len(),
            tree_result.chunk_nodes
        );
        assert_eq!(embed_result.model, "test-model");
    }

    #[tokio::test]
    async fn test_embed_chunks_empty_revision() {
        use elephant_embeddings::FakeEmbeddingProvider;

        let storage = StorageClient::new_memory().await.expect("storage");
        let doc = make_doc("Short");

        // Render the doc
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build doc tree with large chunk size so everything fits in one chunk (the root)
        let config = DocTreeConfig {
            max_chunk_size: 1000,
            chunk_overlap: 0,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // With small text and large chunk size, we should have at least 1 chunk
        assert!(tree_result.chunk_nodes >= 1);

        // Embed the chunks
        let provider = FakeEmbeddingProvider::new("test-model", 384);
        let embed_result = execute_embed_chunks(&storage, &render_result.revision_id, &provider)
            .await
            .expect("embed chunks");

        // Should have embedded the chunk(s)
        assert_eq!(embed_result.chunks_embedded, tree_result.chunk_nodes);
        assert_eq!(embed_result.model, "test-model");
    }

    #[tokio::test]
    async fn test_build_doc_tree_creates_chunk_nodes_without_doc_chunks() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc =
            make_doc("This is a test document with enough content to create multiple chunks.");

        // Render the doc
        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build the doc tree with small chunks to get multiple
        // Disable semantic chunking to force fixed-size splits for this test
        let config = DocTreeConfig {
            max_chunk_size: 20,
            chunk_overlap: 5,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        assert!(
            tree_result.chunk_nodes >= 2,
            "Should have at least 2 chunk nodes"
        );

        // Verify chunk nodes exist
        let node_repo = DocNodeRepository::new(storage.clone());
        let chunk_nodes = node_repo
            .find_chunks_by_revision(&render_result.revision_id)
            .await
            .expect("find chunks");

        assert_eq!(chunk_nodes.len(), tree_result.chunk_nodes);

        for node in &chunk_nodes {
            assert!(
                node.embedding_model.is_none(),
                "Chunk nodes should not have embedding_model yet"
            );
        }
    }

    #[test]
    fn test_detect_sections_markdown_style() {
        let text = "# Introduction\n\nSome intro text.\n\n## Background\n\nBackground info.\n\n## Methods\n\nMethods section.";

        let sections = detect_sections(text);

        assert_eq!(sections.len(), 3);
        assert_eq!(sections[0].title, "Introduction");
        assert_eq!(sections[1].title, "Background");
        assert_eq!(sections[2].title, "Methods");

        // Verify spans don't overlap
        for i in 1..sections.len() {
            assert_eq!(
                sections[i - 1].end,
                sections[i].start,
                "Section spans should be adjacent"
            );
        }

        // Verify last section ends at text end
        assert_eq!(sections.last().unwrap().end as usize, text.len());
    }

    #[test]
    fn test_detect_sections_setext_style() {
        let text = "Introduction\n===\n\nSome intro.\n\nBackground\n---\n\nBackground info.";

        let sections = detect_sections(text);

        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].title, "Introduction");
        assert_eq!(sections[1].title, "Background");
    }

    #[test]
    fn test_detect_sections_mixed_line_endings_exact_spans() {
        let prefix = "preamble\r\n\r\n";
        let first = "# Caf\u{e9}\r\n\r\nintro \u{1f642}\n\n";
        for ending in ["", "\n", "\r\n"] {
            let second = format!("\u{732b}\r\n===\r\n\r\nlast{ending}");
            let text = format!("{prefix}{first}{second}");
            let sections = detect_sections(&text);

            assert_eq!(sections.len(), 2);
            assert_eq!(sections[0].title, "Caf\u{e9}");
            assert_eq!(sections[1].title, "\u{732b}");
            assert_eq!(sections[0].start as usize, prefix.len());
            assert_eq!(sections[0].end as usize, prefix.len() + first.len());
            assert_eq!(sections[1].start, sections[0].end);
            assert_eq!(sections[1].end as usize, text.len());
            for (section, expected) in sections.iter().zip([first, second.as_str()]) {
                assert_eq!(
                    text.get(section.start as usize..section.end as usize),
                    Some(expected)
                );
            }
        }
    }

    #[test]
    fn test_detect_sections_no_sections() {
        let text = "Just plain text without any headings.";

        let sections = detect_sections(text);

        assert!(sections.is_empty());
    }

    #[test]
    fn test_detect_paragraphs_basic() {
        let text = "First paragraph line 1.\nFirst paragraph line 2.\n\nSecond paragraph.\n\nThird paragraph.";

        let paragraphs = detect_paragraphs(text, 0, text.len() as u32);

        assert_eq!(paragraphs.len(), 3);

        // Verify extracted text for each paragraph
        let para1_text = &text[paragraphs[0].start as usize..paragraphs[0].end as usize];
        let para2_text = &text[paragraphs[1].start as usize..paragraphs[1].end as usize];
        let para3_text = &text[paragraphs[2].start as usize..paragraphs[2].end as usize];

        assert!(para1_text.contains("First paragraph"));
        assert!(para2_text.contains("Second paragraph"));
        assert!(para3_text.contains("Third paragraph"));
    }

    #[test]
    fn test_detect_paragraphs_in_range() {
        let text = "# Section\n\nFirst paragraph.\n\nSecond paragraph.\n\n# Another";

        // Detect paragraphs only in a portion of the text (within the first section)
        let range_start = 10; // After "# Section\n"
        let range_end = text.find("# Another").unwrap() as u32;

        let paragraphs = detect_paragraphs(text, range_start, range_end);

        assert_eq!(paragraphs.len(), 2);
    }

    #[test]
    fn test_detect_paragraphs_mixed_line_endings_exact_spans() {
        let prefix = "excluded \u{1f642}\r\n";
        let expected = ["Caf\u{e9}\r\ncontinued.", "\u{732b}\u{1f642}", "last"];
        for ending in ["", "\n", "\r\n"] {
            let body = format!(
                "{}\r\n \t\r\n\n{}\n\n{}{ending}",
                expected[0], expected[1], expected[2]
            );
            let text = format!("{prefix}{body}\nexcluded suffix");
            let paragraphs = detect_paragraphs(
                &text,
                prefix.len() as u32,
                (prefix.len() + body.len()) as u32,
            );

            assert_eq!(paragraphs.len(), expected.len());
            for (paragraph, expected_text) in paragraphs.iter().zip(expected) {
                let expected_start = text.find(expected_text).unwrap();
                assert_eq!(paragraph.start as usize, expected_start);
                assert_eq!(paragraph.end as usize, expected_start + expected_text.len());
                assert_eq!(
                    text.get(paragraph.start as usize..paragraph.end as usize),
                    Some(expected_text)
                );
            }
        }
    }

    #[test]
    fn test_detect_paragraphs_preserves_final_bare_carriage_return() {
        let text = "Caf\u{e9}\r";
        let paragraphs = detect_paragraphs(text, 0, text.len() as u32);

        assert_eq!(paragraphs.len(), 1);
        assert_eq!(paragraphs[0].start, 0);
        assert_eq!(paragraphs[0].end as usize, text.len());
    }

    #[test]
    fn test_detect_paragraphs_empty_text() {
        let paragraphs = detect_paragraphs("", 0, 0);
        assert!(paragraphs.is_empty());

        let whitespace_only = "   \n\n   \n";
        let paragraphs = detect_paragraphs(whitespace_only, 0, whitespace_only.len() as u32);
        assert!(paragraphs.is_empty());
    }

    #[tokio::test]
    async fn test_build_doc_tree_with_section_detection() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let text =
            "# Introduction\n\nThis is the intro.\n\n# Methods\n\nThis is the methods section.";
        let doc = make_doc(text);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with section detection enabled
        let config = DocTreeConfig {
            max_chunk_size: 100,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Should have: 1 root + 2 sections + at least 2 chunks
        assert!(tree_result.nodes_created >= 5);

        // Verify sections were created
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let section_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Section)
            .collect();

        assert_eq!(section_nodes.len(), 2, "Should have 2 section nodes");

        // Verify section titles
        let titles: Vec<_> = section_nodes
            .iter()
            .filter_map(|n| n.text.as_ref())
            .collect();
        assert!(titles.contains(&&"Introduction".to_string()));
        assert!(titles.contains(&&"Methods".to_string()));
    }

    #[tokio::test]
    async fn test_build_doc_tree_with_paragraph_detection() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let text =
            "First paragraph content.\n\nSecond paragraph content.\n\nThird paragraph content.";
        let doc = make_doc(text);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with paragraph detection enabled (no sections)
        let config = DocTreeConfig {
            max_chunk_size: 100,
            chunk_overlap: 0,
            create_paragraphs: true,
            detect_sections: false,
            semantic_chunking: false,
            ..Default::default()
        };

        let _tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Verify paragraphs were created
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let paragraph_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Paragraph)
            .collect();

        assert_eq!(paragraph_nodes.len(), 3, "Should have 3 paragraph nodes");

        // Verify paragraph depth
        for para in &paragraph_nodes {
            assert_eq!(para.depth, 1, "Paragraphs should be at depth 1");
        }
    }

    #[tokio::test]
    async fn test_build_doc_tree_with_sections_and_paragraphs() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let text = "# First Section\n\nPara 1 in section 1.\n\nPara 2 in section 1.\n\n# Second Section\n\nPara 1 in section 2.";
        let doc = make_doc(text);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with both section and paragraph detection
        let config = DocTreeConfig {
            max_chunk_size: 100,
            chunk_overlap: 0,
            create_paragraphs: true,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let _tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        // Count by role
        let doc_count = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Document)
            .count();
        let section_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Section)
            .collect();
        let paragraph_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Paragraph)
            .collect();
        let chunk_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Chunk)
            .collect();

        assert_eq!(doc_count, 1, "Should have 1 document node");
        assert_eq!(section_nodes.len(), 2, "Should have 2 section nodes");
        assert!(
            paragraph_nodes.len() >= 3,
            "Should have at least 3 paragraph nodes"
        );
        assert!(chunk_nodes.len() >= 3, "Should have at least 3 chunk nodes");

        // Verify hierarchy: sections at depth 1, paragraphs at depth 2, chunks at depth 3
        for section in &section_nodes {
            assert_eq!(section.depth, 1, "Sections should be at depth 1");
        }
        for para in &paragraph_nodes {
            assert_eq!(
                para.depth, 2,
                "Paragraphs should be at depth 2 under sections"
            );
        }
        for chunk in &chunk_nodes {
            assert_eq!(
                chunk.depth, 3,
                "Chunks should be at depth 3 under paragraphs"
            );
        }
    }

    #[tokio::test]
    async fn test_build_doc_tree_backward_compatible() {
        // Verify that when both flags are false, we get the original behavior
        let storage = StorageClient::new_memory().await.expect("storage");
        let text = "# Heading\n\nPara 1.\n\nPara 2.";
        let doc = make_doc(text);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with both flags disabled
        let config = DocTreeConfig {
            max_chunk_size: 50,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: false,
            semantic_chunking: false,
            ..Default::default()
        };

        let _tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        // Should only have Document and Chunk roles (no Section or Paragraph)
        let section_count = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Section)
            .count();
        let paragraph_count = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Paragraph)
            .count();

        assert_eq!(
            section_count, 0,
            "Should have no section nodes when detect_sections=false"
        );
        assert_eq!(
            paragraph_count, 0,
            "Should have no paragraph nodes when create_paragraphs=false"
        );

        // All chunks should be direct children of root (depth 1)
        let chunk_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Chunk)
            .collect();
        for chunk in &chunk_nodes {
            assert_eq!(
                chunk.depth, 1,
                "Chunks should be at depth 1 (direct children of root)"
            );
        }
    }

    #[tokio::test]
    async fn render_response_cannot_persist_after_original_source_changes() {
        use elephant_storage::SourceRepository;
        #[derive(Debug)]
        struct ChangingRenderer {
            manifest: PluginManifest,
            source_types: Vec<String>,
            calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl Renderer for ChangingRenderer {
            fn manifest(&self) -> &PluginManifest {
                &self.manifest
            }
            fn source_types(&self) -> &[String] {
                &self.source_types
            }
            async fn render(&self, ctx: RenderContext) -> Result<RenderResult, PipelineError> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let storage = ctx.storage.as_ref().unwrap();
                let source_id = ctx.doc.source.as_ref().unwrap().id();
                let sources = SourceRepository::new(storage.clone());
                let mut source = sources.get(&ctx.space_id, source_id).await?.unwrap();
                source.metadata = serde_json::json!({"changed_during_render": true});
                sources.update(&ctx.space_id, source_id, source).await?;
                Ok(RenderResult::success(
                    ctx.doc.text,
                    RendererMetadata {
                        name: "changing-render-input".into(),
                        version: "1".into(),
                        config_hash: None,
                    },
                ))
            }
        }
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                StorageClient::new_memory().await.unwrap()
            };
            elephant_migrations::apply_schema(&storage).await.unwrap();
            let doc = make_doc("Original raw source evidence.");
            let mut registry = RendererRegistry::new();
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            registry.register(Box::new(ChangingRenderer {
                manifest: PassthroughRenderer::new().manifest().clone(),
                source_types: vec!["text/plain".into()],
                calls: calls.clone(),
            }));
            let error = Box::pin(render_fixture(
                &storage,
                &registry,
                &doc,
                Some("text/plain"),
            ))
            .await
            .expect_err("changed original source must refuse the renderer response");
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(
                error
                    .to_string()
                    .contains("Original render/summary input or authority changed"),
                "the post-response original-input guard must refuse: {error}"
            );
            assert!(
                DocRevisionRepository::new(storage.clone())
                    .find_by_doc(&doc.base.id)
                    .await
                    .unwrap()
                    .is_empty(),
                "render response for changed original input cannot persist a revision"
            );
            assert_eq!(
                DocRepository::new(storage)
                    .get(&doc.base.space_id, &doc.base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .text,
                doc.text
            );
        }
    }

    #[tokio::test]
    async fn cached_render_still_requires_original_claimed_work() {
        use elephant_types::records::{WorkItem, WorkItemStatus};
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                StorageClient::new_memory().await.unwrap()
            };
            elephant_migrations::apply_schema(&storage).await.unwrap();
            let (_, doc, _) = Box::pin(accepted_render_fixture(
                &storage,
                "Original accepted evidence.",
            ))
            .await;
            let mut registry = RendererRegistry::new();
            registry.register(Box::new(PassthroughRenderer::new()));
            let first = Box::pin(execute_render_doc(
                &storage,
                &registry,
                &doc,
                Some("text/plain"),
            ))
            .await
            .unwrap();
            let revisions = DocRevisionRepository::new(storage.clone());
            let before = revisions
                .get(&doc.base.space_id, &first.revision_id)
                .await
                .unwrap()
                .unwrap();
            let work = WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    doc.base.space_id.clone(),
                    "negative-unclaimed".into(),
                ),
                kind: RENDER_DOC_KIND.into(),
                payload: serde_json::json!({"doc_id": doc.base.id.to_string()}),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: "unclaimed-render-negative".into(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };
            // No fake Running lease or original provenance: absence must refuse.
            assert!(Box::pin(execute_render_doc_for_work(
                &storage,
                &registry,
                &doc,
                Some("text/plain"),
                &work
            ))
            .await
            .is_err());
            assert_eq!(
                revisions
                    .get(&doc.base.space_id, &first.revision_id)
                    .await
                    .unwrap()
                    .unwrap(),
                before
            );
            assert_eq!(revisions.find_by_doc(&doc.base.id).await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn original_work_descriptor_cannot_select_another_source_or_fence() {
        use elephant_types::records::{WorkItem, WorkItemStatus};
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                StorageClient::new_memory().await.unwrap()
            };
            elephant_migrations::apply_schema(&storage).await.unwrap();
            let (source, doc, fence) = Box::pin(accepted_render_fixture(
                &storage,
                "Original accepted descriptor evidence.",
            ))
            .await;
            let original = WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    doc.base.space_id.clone(),
                    "descriptor-only-control".into(),
                ),
                kind: RENDER_DOC_KIND.into(),
                payload: serde_json::json!({
                    "doc_id": doc.base.id.to_string(),
                    "source_id": source.base.id.to_string(),
                    "_publication": fence,
                }),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: "original-descriptor-control".into(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };
            // This validates descriptor selection only, not execution authority
            // or a claimed lease. capture_for_work must still prove both.
            let check = |work: &WorkItem, selected: Option<&PublicationFence>| {
                summary_inputs::validate_original_work_input(work, &source, &doc, None, selected)
            };
            check(&original, Some(&fence)).unwrap();
            assert!(check(&original, None).is_err());
            for field in ["source_id", "generation", "revision_id", "operation_id"] {
                let mut changed = original.clone();
                changed.payload["_publication"][field] = if field == "source_id" {
                    serde_json::json!(RecordId::new_v4(TableName::Source))
                } else {
                    serde_json::json!("different-original-value")
                };
                assert!(check(&changed, Some(&fence)).is_err(), "{field}");
            }
            for value in [
                serde_json::json!(RecordId::new_v4(TableName::Source).to_string()),
                serde_json::json!({"id": source.base.id}),
            ] {
                let mut changed = original.clone();
                changed.payload["source_id"] = value;
                assert!(check(&changed, Some(&fence)).is_err());
            }
            let mut malformed = original.clone();
            malformed.payload["_publication"] = serde_json::Value::Null;
            assert!(check(&malformed, Some(&fence)).is_err());
            // A changed selected Doc cannot use the same original descriptor.
            let mut foreign_doc = doc.clone();
            foreign_doc.source = Some(Link::new(RecordId::new_v4(TableName::Source)));
            assert!(summary_inputs::validate_original_work_input(
                &original,
                &source,
                &foreign_doc,
                None,
                Some(&fence),
            )
            .is_err());
            let mut descriptor_absent = original.clone();
            descriptor_absent
                .payload
                .as_object_mut()
                .unwrap()
                .remove("_publication");
            descriptor_absent
                .payload
                .as_object_mut()
                .unwrap()
                .remove("source_id");
            check(&descriptor_absent, Some(&fence)).unwrap();
            check(&original, Some(&fence)).unwrap();
        }
    }

    #[tokio::test]
    async fn summary_input_guard_rejects_changed_original_rows_on_both_backends() {
        use elephant_embeddings::governed::ProviderDispatchObserver;
        use elephant_storage::SourceRepository;
        use elephant_types::provider_binding::{
            ModelBindingProvenance, ModelOperation, SpaceConfigToken,
        };
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                StorageClient::new_memory().await.unwrap()
            };
            elephant_migrations::apply_schema(&storage).await.unwrap();
            let original = make_doc("# Original section\n\nOriginal source evidence.");
            let mut registry = RendererRegistry::new();
            registry.register(Box::new(PassthroughRenderer::new()));
            let rendered = Box::pin(render_fixture(
                &storage,
                &registry,
                &original,
                Some("text/plain"),
            ))
            .await
            .unwrap();
            let docs = DocRepository::new(storage.clone());
            let revisions = DocRevisionRepository::new(storage.clone());
            let sources = SourceRepository::new(storage.clone());
            let doc = docs
                .get(&original.base.space_id, &original.base.id)
                .await
                .unwrap()
                .unwrap();
            let revision = revisions
                .get(&doc.base.space_id, &rendered.revision_id)
                .await
                .unwrap()
                .unwrap();
            let (source, _, publication) = captured_render_input(&storage, &doc).await.unwrap();
            let inputs = Box::pin(summary_inputs::PreparedSummaryInputs::capture(
                &storage,
                &source,
                &doc,
                &revision,
                publication.as_ref(),
                None,
            ))
            .await
            .unwrap();
            let binding = ModelBindingProvenance {
                config: SpaceConfigToken {
                    space_id: doc.base.space_id.clone(),
                    config_id: "summary-test-config".into(),
                    generation: 1,
                    policy_id: "default".into(),
                },
                operation: ModelOperation::Summary,
                binding_id: "summary-test-binding".into(),
                profile_id: "summary-test-profile".into(),
            };
            // Exercise the actual callback used by the composed SDK observer;
            // these source tests do not claim to dispatch HTTP or a model.
            inputs.before_dispatch("original", &binding).await.unwrap();
            let mut changed = revision.clone();
            changed.text.push_str(" Changed evidence.");
            changed.text_hash = crate::extraction::AssertionValidator::hash_quote(&changed.text);
            revisions
                .update(&doc.base.space_id, &revision.base.id, changed)
                .await
                .unwrap();
            assert!(inputs
                .before_dispatch("changed-revision", &binding)
                .await
                .is_err());
            revisions
                .update(&doc.base.space_id, &revision.base.id, revision.clone())
                .await
                .unwrap();
            inputs.verify_current().await.unwrap();

            let mut changed = doc.clone();
            changed.title.push_str(" changed");
            docs.update(&doc.base.space_id, &doc.base.id, changed)
                .await
                .unwrap();
            assert!(inputs
                .before_dispatch("changed-doc", &binding)
                .await
                .is_err());
            docs.update(&doc.base.space_id, &doc.base.id, doc.clone())
                .await
                .unwrap();
            inputs.verify_current().await.unwrap();

            let mut changed = source.clone();
            changed.base.security.level += 1;
            sources
                .update(&doc.base.space_id, &source.base.id, changed)
                .await
                .unwrap();
            assert!(inputs
                .before_dispatch("changed-source", &binding)
                .await
                .is_err());
            sources
                .update(&doc.base.space_id, &source.base.id, source.clone())
                .await
                .unwrap();
            inputs.verify_current().await.unwrap();

            let mut foreign = binding.clone();
            foreign.config.space_id = "foreign".into();
            assert!(inputs.before_dispatch("foreign", &foreign).await.is_err());
            foreign = binding.clone();
            foreign.operation = ModelOperation::Extraction;
            assert!(inputs
                .before_dispatch("wrong-operation", &foreign)
                .await
                .is_err());
            // Actual unrelated corpus writes do not revoke these fixed inputs.
            let unrelated = make_doc("Unrelated corpus text.");
            Box::pin(render_fixture(
                &storage,
                &registry,
                &unrelated,
                Some("text/plain"),
            ))
            .await
            .unwrap();
            inputs
                .before_dispatch("unrelated-corpus", &binding)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn summary_response_cannot_publish_after_original_input_changes() {
        use elephant_storage::SourceRepository;
        #[derive(Debug)]
        struct ChangeOriginalSource {
            storage: StorageClient,
            source: Source,
            calls: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl Summarizer for ChangeOriginalSource {
            fn id(&self) -> &'static str {
                "change-original-summary-input"
            }
            async fn summarize(
                &self,
                ctx: &SummarizationContext,
            ) -> Result<crate::summarizer::SummaryResult, PipelineError> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert!(ctx.text.contains("Original source evidence"));
                let mut changed = self.source.clone();
                changed.metadata = serde_json::json!({"changed_after_capture": true});
                SourceRepository::new(self.storage.clone())
                    .update(
                        &changed.base.space_id.clone(),
                        &changed.base.id.clone(),
                        changed,
                    )
                    .await?;
                Ok(crate::summarizer::SummaryResult {
                    text: "Original evidence summary.".into(),
                    token_count: 4,
                })
            }
        }
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                StorageClient::new_memory().await.unwrap()
            };
            elephant_migrations::apply_schema(&storage).await.unwrap();
            let original = make_doc("# Original section\n\nOriginal source evidence.");
            let mut registry = RendererRegistry::new();
            registry.register(Box::new(PassthroughRenderer::new()));
            let rendered = Box::pin(render_fixture(
                &storage,
                &registry,
                &original,
                Some("text/plain"),
            ))
            .await
            .unwrap();
            let doc = DocRepository::new(storage.clone())
                .get(&original.base.space_id, &original.base.id)
                .await
                .unwrap()
                .unwrap();
            let (source, _, _) = captured_render_input(&storage, &doc).await.unwrap();
            let summarizer = ChangeOriginalSource {
                storage: storage.clone(),
                source,
                calls: std::sync::atomic::AtomicUsize::new(0),
            };
            let config = DocTreeConfig {
                create_summaries: true,
                detect_sections: true,
                summary_threshold: 1,
                ..DocTreeConfig::default()
            };
            assert!(Box::pin(build_doc_tree_with_summarizer(
                &storage,
                &rendered.revision_id,
                &config,
                Some(&summarizer)
            ))
            .await
            .is_err());
            assert_eq!(
                summarizer.calls.load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            let nodes = DocNodeRepository::new(storage)
                .find_by_revision(&rendered.revision_id)
                .await
                .unwrap();
            assert!(nodes.iter().any(|node| node.role == NodeRole::Document));
            assert!(
                nodes.iter().all(|node| node.role != NodeRole::Summary),
                "a response for changed original inputs must not create summary output"
            );
        }
    }

    #[tokio::test]
    async fn test_build_doc_tree_creates_summary_nodes() {
        #[derive(Debug)]
        struct FixtureSummary(std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait]
        impl Summarizer for FixtureSummary {
            fn id(&self) -> &'static str {
                "fixture-summary"
            }
            async fn summarize(
                &self,
                ctx: &SummarizationContext,
            ) -> Result<crate::summarizer::SummaryResult, PipelineError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert!(ctx.text.contains("This is a very long section"));
                Ok(crate::summarizer::SummaryResult {
                    text: "A long section with repeated content.".into(),
                    token_count: 9,
                })
            }
        }
        let storage = StorageClient::new_memory().await.expect("storage");

        // Create a document with multiple sections, one of which is large enough for a summary
        let large_section_content = "This is a very long section with lots of content. ".repeat(50);
        let text = format!(
            "# Small Section\n\nShort content.\n\n# Large Section\n\n{}\n\n# Another Small\n\nAlso short.",
            large_section_content
        );
        let doc = make_doc(&text);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with summaries enabled and threshold of 500 bytes
        let config = DocTreeConfig {
            max_chunk_size: 200,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            create_summaries: true,
            summary_threshold: 500, // The large section should exceed this
            ..Default::default()
        };

        let summary = FixtureSummary(std::sync::atomic::AtomicUsize::new(0));
        let _tree_result = build_doc_tree_with_summarizer(
            &storage,
            &render_result.revision_id,
            &config,
            Some(&summary),
        )
        .await
        .expect("build tree");

        let initial_calls = summary.0.load(std::sync::atomic::Ordering::SeqCst);
        assert!(initial_calls > 0);
        build_doc_tree_with_summarizer(
            &storage,
            &render_result.revision_id,
            &config,
            Some(&summary),
        )
        .await
        .expect("summary replay");
        assert_eq!(
            summary.0.load(std::sync::atomic::Ordering::SeqCst),
            initial_calls
        );

        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        // Should have Summary nodes for large sections
        let summary_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Summary)
            .collect();

        assert!(
            !summary_nodes.is_empty(),
            "Should have at least one summary node for the large section"
        );

        // Verify summary nodes have text content
        for summary in &summary_nodes {
            assert!(
                summary.text.is_some(),
                "Summary nodes should have text content"
            );
        }
    }

    #[tokio::test]
    async fn test_default_summarizer_uses_agentic() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded fixture");
        let summarizer = super::default_summarizer(&storage, "test", None);
        assert_eq!(summarizer.id(), "agentic");
    }

    #[test]
    fn test_detect_content_type_html() {
        assert_eq!(
            detect_content_type("<!DOCTYPE html><html>"),
            ContentType::Html
        );
        assert_eq!(detect_content_type("<html><body>"), ContentType::Html);
        assert_eq!(
            detect_content_type("Some text with <div>content</div>"),
            ContentType::Html
        );
    }

    #[test]
    fn test_detect_content_type_email() {
        assert_eq!(
            detect_content_type("From: alice@example.com\nSubject: Hello"),
            ContentType::Email
        );
        assert_eq!(
            detect_content_type("Subject: Test Email\nBody here"),
            ContentType::Email
        );
        assert_eq!(
            detect_content_type("Date: Mon, 1 Jan 2024\nFrom: test@test.com"),
            ContentType::Email
        );
    }

    #[test]
    fn test_detect_content_type_markdown() {
        assert_eq!(
            detect_content_type("# Heading\n\nParagraph"),
            ContentType::Markdown
        );
        assert_eq!(
            detect_content_type("Some text\n## Subheading"),
            ContentType::Markdown
        );
        assert_eq!(
            detect_content_type("Text\n```rust\ncode\n```"),
            ContentType::Markdown
        );
    }

    #[test]
    fn test_detect_content_type_structured() {
        assert_eq!(
            detect_content_type("{\"key\": \"value\"}"),
            ContentType::Structured
        );
        assert_eq!(detect_content_type("[1, 2, 3]"), ContentType::Structured);
        assert_eq!(
            detect_content_type("<xml><tag/></xml>"),
            ContentType::Structured
        );
    }

    #[test]
    fn test_detect_content_type_plain() {
        assert_eq!(
            detect_content_type("Just some plain text."),
            ContentType::PlainText
        );
        assert_eq!(
            detect_content_type("Multiple lines\nof plain\ntext"),
            ContentType::PlainText
        );
    }

    #[test]
    fn test_detect_html_sections() {
        let html = "<h1>Title</h1><p>Content</p><h2>Subtitle</h2><p>More</p>";
        let sections = detect_html_sections(html);
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].title, "Title");
        assert_eq!(sections[1].title, "Subtitle");
    }

    #[test]
    fn test_detect_html_sections_with_attributes() {
        let html = r#"<h1 class="main">Title</h1><p>Content</p><h2 id="sub">Subtitle</h2>"#;
        let sections = detect_html_sections(html);
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].title, "Title");
        assert_eq!(sections[1].title, "Subtitle");
    }

    #[test]
    fn test_detect_html_sections_case_insensitive() {
        let html = "<H1>Title</H1><H2>Subtitle</H2>";
        let sections = detect_html_sections(html);
        assert_eq!(sections.len(), 2);
    }

    #[test]
    fn test_detect_html_sections_empty() {
        let html = "<p>No headings here</p>";
        let sections = detect_html_sections(html);
        assert!(sections.is_empty());
    }

    #[test]
    fn test_detect_html_lists() {
        let html = "<ul><li>Item 1</li></ul><ol><li>Item A</li></ol>";
        let lists = detect_html_lists(html);
        assert_eq!(lists.len(), 2);
        assert_eq!(lists[0].list_type, ListType::Unordered);
        assert_eq!(lists[1].list_type, ListType::Ordered);
    }

    #[test]
    fn test_detect_html_lists_with_attributes() {
        let html = r#"<ul class="menu"><li>Item</li></ul><ol start="5"><li>Item</li></ol>"#;
        let lists = detect_html_lists(html);
        assert_eq!(lists.len(), 2);
    }

    #[test]
    fn test_detect_html_lists_empty() {
        let html = "<p>No lists here</p>";
        let lists = detect_html_lists(html);
        assert!(lists.is_empty());
    }

    #[test]
    fn test_detect_html_tables() {
        let html = "<table><tr><td>Cell</td></tr></table>";
        let tables = detect_html_tables(html);
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn test_detect_html_tables_multiple() {
        let html =
            "<table><tr><td>A</td></tr></table><p>text</p><table><tr><td>B</td></tr></table>";
        let tables = detect_html_tables(html);
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_detect_html_tables_with_attributes() {
        let html = r#"<table class="data" border="1"><tr><td>Cell</td></tr></table>"#;
        let tables = detect_html_tables(html);
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn test_detect_html_tables_empty() {
        let html = "<p>No tables here</p>";
        let tables = detect_html_tables(html);
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn test_build_doc_tree_html_content_type_dispatch() {
        // Verify that HTML content uses detect_html_sections instead of detect_sections
        let storage = StorageClient::new_memory().await.expect("storage");

        // HTML content with h1/h2 headings (not markdown # style)
        let html = "<html><body><h1>Introduction</h1><p>Intro text</p><h2>Methods</h2><p>Methods text</p></body></html>";
        let doc = make_doc(html);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/html"))
            .await
            .expect("render");

        // Build tree with section detection enabled
        let config = DocTreeConfig {
            max_chunk_size: 200,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Content type should be detected as HTML
        assert_eq!(tree_result.content_type, ContentType::Html);

        // Should have detected the HTML h1/h2 sections
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let section_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Section)
            .collect();

        assert_eq!(
            section_nodes.len(),
            2,
            "Should have 2 section nodes from HTML h1/h2 tags"
        );

        // Verify section titles
        let titles: Vec<_> = section_nodes
            .iter()
            .filter_map(|n| n.text.as_ref())
            .collect();
        assert!(
            titles.contains(&&"Introduction".to_string()),
            "Should have Introduction section"
        );
        assert!(
            titles.contains(&&"Methods".to_string()),
            "Should have Methods section"
        );
    }

    #[tokio::test]
    async fn test_build_doc_tree_markdown_not_detected_as_html() {
        // Verify that markdown with # headings doesn't accidentally trigger HTML detection
        let storage = StorageClient::new_memory().await.expect("storage");

        // Markdown content with # style headings
        let markdown = "# Introduction\n\nIntro text\n\n## Methods\n\nMethods text";
        let doc = make_doc(markdown);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/markdown"))
            .await
            .expect("render");

        let config = DocTreeConfig {
            max_chunk_size: 200,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Content type should be detected as Markdown (not HTML)
        assert_eq!(tree_result.content_type, ContentType::Markdown);

        // Should have detected the Markdown # sections
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let section_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Section)
            .collect();

        assert_eq!(
            section_nodes.len(),
            2,
            "Should have 2 section nodes from Markdown # headings"
        );
    }

    #[tokio::test]
    async fn test_build_doc_tree_creates_list_nodes() {
        // Verify that HTML with lists creates List nodes
        let storage = StorageClient::new_memory().await.expect("storage");

        // HTML content with both ordered and unordered lists
        let html = r#"<html><body>
            <h1>Shopping</h1>
            <ul><li>Apples</li><li>Bananas</li></ul>
            <h1>Steps</h1>
            <ol><li>First step</li><li>Second step</li></ol>
        </body></html>"#;
        let doc = make_doc(html);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/html"))
            .await
            .expect("render");

        // Build tree with section detection enabled (lists are detected for HTML content)
        let config = DocTreeConfig {
            max_chunk_size: 200,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Content type should be detected as HTML
        assert_eq!(tree_result.content_type, ContentType::Html);

        // Verify list nodes were created
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let list_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::List)
            .collect();

        assert_eq!(
            list_nodes.len(),
            2,
            "Should have 2 list nodes (one ul, one ol)"
        );

        // Verify list nodes have correct hierarchy (should be children of sections or root)
        for list in &list_nodes {
            assert!(list.parent.is_some(), "List nodes should have a parent");
            // Lists should be at depth 2 (under sections) or depth 1 (under root)
            assert!(
                list.depth == 1 || list.depth == 2,
                "List nodes should be at depth 1 or 2, got {}",
                list.depth
            );
        }
    }

    #[tokio::test]
    async fn test_build_doc_tree_creates_table_nodes_with_cells() {
        let storage = StorageClient::new_memory().await.expect("storage");

        // HTML content with a table containing cells
        let html = r#"<html><body>
<h1>Data Report</h1>
<table>
<tr><th>Name</th><th>Value</th></tr>
<tr><td>Alpha</td><td>100</td></tr>
<tr><td>Beta</td><td>200</td></tr>
</table>
<p>Some text after the table.</p>
</body></html>"#;
        let doc = make_doc(html);

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/html"))
            .await
            .expect("render");

        // Build tree with section detection enabled (tables are detected for HTML)
        let config = DocTreeConfig {
            max_chunk_size: 200,
            chunk_overlap: 0,
            create_paragraphs: false,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        // Content type should be detected as HTML
        assert_eq!(tree_result.content_type, ContentType::Html);

        // Verify table nodes were created
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        let table_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::Table)
            .collect();

        assert_eq!(table_nodes.len(), 1, "Should have 1 table node");

        // Verify table cell nodes were created
        let cell_nodes: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.role == NodeRole::TableCell)
            .collect();

        // Should have 6 cells: 2 header cells (th) + 4 data cells (td)
        assert_eq!(
            cell_nodes.len(),
            6,
            "Should have 6 table cell nodes (2 th + 4 td)"
        );

        // Verify cells are children of the table
        let table_id = &table_nodes[0].base.id;
        for cell in &cell_nodes {
            assert!(cell.parent.is_some(), "Table cells should have a parent");
            assert_eq!(
                cell.parent.as_ref().unwrap().id(),
                table_id,
                "Table cells should be children of the table node"
            );
        }
    }

    #[test]
    fn test_detect_html_table_cells() {
        let html = "<table><tr><th>Header1</th><th>Header2</th></tr><tr><td>Data1</td><td>Data2</td></tr></table>";
        let tables = detect_html_tables(html);
        assert_eq!(tables.len(), 1);

        let table = &tables[0];
        let cells = detect_html_table_cells(html, table.start, table.end);

        // Should have 4 cells: 2 th + 2 td
        assert_eq!(cells.len(), 4);

        // Verify cell types
        let header_cells: Vec<_> = cells.iter().filter(|c| c.is_header).collect();
        let data_cells: Vec<_> = cells.iter().filter(|c| !c.is_header).collect();
        assert_eq!(header_cells.len(), 2, "Should have 2 header cells");
        assert_eq!(data_cells.len(), 2, "Should have 2 data cells");
    }

    #[test]
    fn test_detect_html_table_cells_empty_table() {
        let html = "<table></table>";
        let tables = detect_html_tables(html);
        assert_eq!(tables.len(), 1);

        let table = &tables[0];
        let cells = detect_html_table_cells(html, table.start, table.end);
        assert!(cells.is_empty(), "Empty table should have no cells");
    }

    #[tokio::test]
    async fn test_build_doc_tree_propagates_collection_to_all_nodes() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let text =
            "# Introduction\n\nThis is the intro paragraph.\n\n# Methods\n\nThis is the methods section with enough text to create chunks.";

        // Create doc with collection metadata and persist it
        let mut doc = make_doc(text);
        doc.metadata = serde_json::json!({ "collection": "research-papers" });
        let doc_repo = DocRepository::new(storage.clone());
        doc_repo.create(doc.clone()).await.expect("persist doc");

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(PassthroughRenderer::new()));

        let render_result = render_fixture(&storage, &registry, &doc, Some("text/plain"))
            .await
            .expect("render");

        // Build tree with sections enabled
        let config = DocTreeConfig {
            max_chunk_size: 50,
            chunk_overlap: 0,
            create_paragraphs: true,
            detect_sections: true,
            semantic_chunking: false,
            ..Default::default()
        };

        let tree_result = execute_build_doc_tree(&storage, &render_result.revision_id, &config)
            .await
            .expect("build tree");

        assert!(
            tree_result.nodes_created >= 3,
            "Should create multiple nodes"
        );

        // Verify ALL nodes have collection set
        let node_repo = DocNodeRepository::new(storage.clone());
        let all_nodes = node_repo
            .find_by_revision(&render_result.revision_id)
            .await
            .expect("find nodes");

        for node in &all_nodes {
            assert_eq!(
                node.collection.as_deref(),
                Some("research-papers"),
                "Node {} (role={:?}, depth={}) should have collection 'research-papers' but got {:?}",
                node.node_key,
                node.role,
                node.depth,
                node.collection,
            );
        }

        // Verify we have nodes of different roles (section, paragraph/chunk)
        let has_role = |role: NodeRole| all_nodes.iter().any(|n| n.role == role);
        assert!(has_role(NodeRole::Document), "Should have document node");
        assert!(has_role(NodeRole::Section), "Should have section nodes");
        assert!(
            all_nodes.iter().any(|n| matches!(
                n.role,
                NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
            )),
            "Should have embeddable nodes (chunk/paragraph/summary)"
        );
    }
}
