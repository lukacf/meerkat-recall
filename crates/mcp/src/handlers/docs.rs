//! Document MCP tool handlers.
//!
//! Implements:
//! - `search_docs`
//! - `ingest_doc`
//! - `list_doc_collections`
//! - `get_doc`
//! - `list_doc_chunks` (returns `doc_node` records)
//! - `get_doc_chunk` (returns a `doc_node` record)
//! - `delete_doc`
//! - `restore_doc`
//! - `batch_ingest_docs`
//! - `restamp_attribution`

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

use elephant_embeddings::{
    discover_accessible_partition_keys, EmbeddingPolicy, EmbeddingProvider, EmbeddingProviderCache,
    PartitionKey,
};
use elephant_pipeline::media::{probe_video_duration_ms, probe_video_duration_ms_from_bytes};
use elephant_pipeline::renderer::default::{DefaultTextRenderer, PassthroughRenderer};
use elephant_pipeline::renderer::multimodal::MultimodalRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_render_doc,
    ingest_work_kinds::{
        enqueue_triage_source_work_item as enqueue_triage_source_work_item_for_source,
        INGEST_METADATA_KIND, TRIAGE_SOURCE_KIND, UPGRADE_INGEST_MODE_KIND,
    },
    resolve_extraction_guidance_status, DocTreeConfig, COMMIT_ARTIFACTS_KIND,
    EXTRACT_ASSERTIONS_KIND, PLAN_EXTRACTION_KIND, RENDER_DOC_KIND, VALIDATE_ARTIFACTS_KIND,
};
use elephant_policy::RedactionEnvelope;
use elephant_storage::{
    deterministic_source_id_for_ingest, find_existing_doc_for_doc, find_existing_source_for_doc,
    find_existing_source_for_source, parse_id, persist_manual_doc_for_source_detailed,
    preview_persisted_manual_doc_for_source, rollback_persisted_manual_doc,
    rollback_persisted_source, update_existing_source_for_reingest, AssertionRepository,
    DocNodeRepository, DocRepository, DocRevisionRepository, EntityRepository,
    MediaAssetRepository, NodeEmbeddingRepository, PersistedManualDoc, Repository, SearchCriteria,
    SearchRepository, SourceRepository, SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::ids::TableName;
use elephant_types::mcp_simple::{ExtractionGuidanceInput, IngestDocSimpleInput};
use elephant_types::pipeline_status::{
    derive_extraction_state, ChunkStatus, DocPipelineStatus, ExtractionState,
};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::media_asset::{
    inline_media_max_bytes, managed_asset_relative_path, MAX_DOC_MEDIA_ATTACHMENTS,
};
use elephant_types::records::source::{
    apply_manual_doc_to_source, manual_doc_template_from_doc, prepare_source_for_reingest,
    SourceLifecycleStatus,
};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::{Doc, Source, SourceType};
use elephant_types::records::{
    DocMediaAttachment, DocMediaInput, DocMediaKind, DocMediaLocation, MediaAsset,
};
use elephant_types::RecordId;
use elephant_types::{BaseFieldsBuilder, Link};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::redaction::redact_doc;
use crate::tools::ToolResponse;

const AGENTIC_EXTRACTOR_ID: &str = "elephant.extractor.agentic";

use super::{merge_base_fields, BatchError, ToolContext, ToolHandler};

fn normalize_doc_value(doc_value: &mut Value, ctx: &ToolContext) -> McpResult<()> {
    let obj = doc_value
        .as_object_mut()
        .ok_or_else(|| McpError::InvalidParameters("doc must be an object".to_string()))?;
    merge_base_fields(obj, TableName::Doc, ctx)?;
    if !obj.contains_key("rendition_key") {
        obj.insert(
            "rendition_key".to_string(),
            Value::String(format!("mcp-{}", Uuid::new_v4())),
        );
    }
    if !obj.contains_key("text") {
        let inferred_text = obj
            .get("sidecar_text")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(std::borrow::ToOwned::to_owned)
            .or_else(|| {
                obj.get("media_attachments")
                    .and_then(Value::as_array)
                    .filter(|items| !items.is_empty())
                    .map(|_| String::new())
            });
        if let Some(text) = inferred_text {
            obj.insert("text".to_string(), Value::String(text));
        }
    }
    if let Some(media_attachments) = obj.get("media_attachments").and_then(Value::as_array) {
        if media_attachments.len() > MAX_DOC_MEDIA_ATTACHMENTS {
            return Err(McpError::InvalidParameters(format!(
                "media_attachments accepts at most {MAX_DOC_MEDIA_ATTACHMENTS} attachments per document (got {})",
                media_attachments.len()
            )));
        }
        for (index, attachment) in media_attachments.iter().enumerate() {
            let Some(location) = attachment.get("location").and_then(Value::as_object) else {
                continue;
            };
            let transport = location
                .get("transport")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if matches!(transport, "path" | "uri") {
                return Err(McpError::InvalidParameters(format!(
                    "media_attachments[{index}] must not use path or uri transports on ingest_doc"
                )));
            }
        }
    }
    Ok(())
}

fn doc_media_location_schema() -> Value {
    json!({
        "type": "object",
        "required": ["transport", "asset_id"],
        "properties": {
            "transport": { "const": "managed_asset" },
            "asset_id": { "type": "string", "description": "Managed media_asset:* identifier" }
        },
        "additionalProperties": false
    })
}

fn doc_media_attachment_schema() -> Value {
    json!({
        "type": "object",
        "required": ["attachment_id", "kind", "media_type", "location"],
        "properties": {
            "attachment_id": { "type": "string", "description": "Stable attachment identifier within the document" },
            "kind": {
                "type": "string",
                "enum": ["image", "video"],
                "description": "Media kind"
            },
            "media_type": { "type": "string", "description": "MIME type for the attachment" },
            "location": doc_media_location_schema(),
            "label": { "type": "string", "description": "Optional attachment label or filename" },
            "size_bytes": { "type": "integer", "minimum": 0, "description": "Optional attachment size in bytes" },
            "duration_ms": { "type": "integer", "minimum": 0, "description": "Optional attachment duration in milliseconds" },
            "sha256": { "type": "string", "description": "Optional SHA-256 fingerprint" }
        },
        "additionalProperties": false
    })
}

fn doc_payload_schema() -> Value {
    json!({
        "type": "object",
        "description": "Document fields. Base fields (id, space_id, created_at, security, etc.) and `rendition_key` are optional and will be auto-generated if omitted. Canonical `text` is required unless the document is media-backed via `media_attachments`.",
        "required": ["title"],
        "anyOf": [
            { "required": ["text"] },
            { "required": ["media_attachments"] }
        ],
        "properties": {
            "title": { "type": "string", "description": "Document title" },
            "text": { "type": "string", "description": "Canonical searchable text content or initial sidecar text for media-backed docs" },
            "sidecar_text": { "type": "string", "description": "Optional caller-supplied sidecar text preserved separately from rendered text" },
            "media_attachments": {
                "type": "array",
                "items": doc_media_attachment_schema(),
                "maxItems": MAX_DOC_MEDIA_ATTACHMENTS,
                "description": "Typed media attachments already normalized for the document"
            },
            "metadata": {
                "type": "object",
                "required": [],
                "description": "Arbitrary metadata (e.g., collection, tags).",
                "additionalProperties": true
            },
            "source": { "type": "string", "description": "Optional source record ID (e.g., source:550e8400-e29b-41d4-a716-446655440000)" },
            "rendition_key": { "type": "string", "description": "Optional rendition key for idempotency (auto-generated if omitted)" }
        },
        "additionalProperties": true
    })
}

fn doc_media_input_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "media_type"],
        "properties": {
            "kind": {
                "type": "string",
                "enum": ["image", "video"],
                "description": "Media kind"
            },
            "media_type": {
                "type": "string",
                "description": "MIME type for the attachment"
            },
            "inline_base64": {
                "type": "string",
                "description": "Optional inline base64-encoded payload"
            },
            "duration_ms": {
                "type": "integer",
                "minimum": 0,
                "description": "Optional duration for video media, required for inline video"
            }
        }
    })
}

fn normalize_optional_text(text: Option<String>) -> Option<String> {
    text.filter(|value| !value.trim().is_empty())
}

fn inferred_content_type(media: &[DocMediaInput]) -> String {
    media
        .first()
        .map_or_else(|| "text/plain".to_string(), |item| item.media_type.clone())
}

fn decode_inline_media(index: usize, payload: &str) -> McpResult<Vec<u8>> {
    let encoded = payload
        .split_once(',')
        .filter(|(prefix, _)| prefix.contains(";base64"))
        .map_or(payload, |(_, data)| data)
        .trim();

    BASE64_STANDARD.decode(encoded).map_err(|e| {
        McpError::InvalidParameters(format!(
            "media[{index}].inline_base64 is not valid base64: {e}"
        ))
    })
}

fn persist_inline_media_asset(
    storage: &StorageClient,
    space_id: &str,
    media: &DocMediaInput,
    index: usize,
) -> McpResult<(DocMediaAttachment, MediaAsset)> {
    let payload = media.inline_base64.as_deref().ok_or_else(|| {
        McpError::InvalidParameters(format!("media[{index}] missing inline_base64 payload"))
    })?;
    let bytes = decode_inline_media(index, payload)?;
    persist_managed_media_asset_bytes(storage, space_id, media, index, bytes)
}

fn persist_managed_media_asset_bytes(
    storage: &StorageClient,
    space_id: &str,
    media: &DocMediaInput,
    index: usize,
    bytes: Vec<u8>,
) -> McpResult<(DocMediaAttachment, MediaAsset)> {
    persist_managed_media_snapshot(
        storage,
        space_id,
        index,
        ManagedMediaSnapshot {
            kind: media.kind,
            media_type: media.media_type.clone(),
            duration_ms: media.duration_ms,
            label: None,
            bytes,
        },
    )
}

struct ManagedMediaSnapshot {
    kind: DocMediaKind,
    media_type: String,
    duration_ms: Option<u64>,
    label: Option<String>,
    bytes: Vec<u8>,
}

fn persist_managed_media_snapshot(
    _storage: &StorageClient,
    space_id: &str,
    index: usize,
    snapshot: ManagedMediaSnapshot,
) -> McpResult<(DocMediaAttachment, MediaAsset)> {
    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let storage_path = managed_asset_relative_path(&asset_id);
    let ManagedMediaSnapshot {
        kind,
        media_type,
        duration_ms,
        label,
        bytes,
    } = snapshot;
    let size_bytes = u64::try_from(bytes.len()).map_err(|_| {
        McpError::ExecutionError(format!(
            "media[{index}] payload is too large to persist as a managed asset"
        ))
    })?;
    let max_inline_bytes = inline_media_max_bytes(kind);
    if size_bytes > max_inline_bytes {
        return Err(McpError::InvalidParameters(format!(
            "media[{index}] inline payload exceeds configured limit of {max_inline_bytes} bytes"
        )));
    }
    let sha256 = hex::encode(Sha256::digest(&bytes));

    let mut asset = MediaAsset {
        base: BaseFieldsBuilder::new(space_id)
            .for_table(TableName::MediaAsset)
            .build(),
        kind,
        media_type: media_type.clone(),
        storage_path,
        size_bytes,
        sha256: sha256.clone(),
        payload_base64: Some(BASE64_STANDARD.encode(&bytes)),
        duration_ms,
        label: label.clone(),
    };
    asset.base.id = asset_id;
    let asset_id_string = asset.base.id.to_string();

    let attachment = DocMediaAttachment {
        attachment_id: format!("attachment-{}", Uuid::new_v4()),
        kind,
        media_type,
        location: DocMediaLocation::ManagedAsset {
            asset_id: asset_id_string,
        },
        label,
        size_bytes: Some(size_bytes),
        duration_ms,
        sha256: Some(sha256),
    };

    Ok((attachment, asset))
}

#[derive(Debug, Clone)]
struct PreparedMediaAttachments {
    attachments: Vec<DocMediaAttachment>,
    assets: Vec<MediaAsset>,
}

fn build_media_attachments(
    storage: &StorageClient,
    space_id: &str,
    inputs: &[DocMediaInput],
) -> McpResult<PreparedMediaAttachments> {
    if inputs.len() > MAX_DOC_MEDIA_ATTACHMENTS {
        return Err(McpError::InvalidParameters(format!(
            "media accepts at most {MAX_DOC_MEDIA_ATTACHMENTS} attachments per document (got {})",
            inputs.len()
        )));
    }
    let mut attachments = Vec::with_capacity(inputs.len());
    let mut assets = Vec::with_capacity(inputs.len());
    for (index, media) in inputs.iter().enumerate() {
        if media.media_type.trim().is_empty() {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}].media_type cannot be empty"
            )));
        }
        if media.transport_count() != 1 {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}] must specify exactly one media transport"
            )));
        }
        if media.path.is_some() || media.uri.is_some() {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}] must use inline_base64; public ingest does not accept path or uri media transports"
            )));
        }
        if matches!(media.kind, DocMediaKind::Video) && media.inline_base64.is_some() {
            match media.duration_ms {
                Some(duration_ms) if duration_ms > 0 => {}
                _ => {
                    return Err(McpError::InvalidParameters(format!(
                        "media[{index}].duration_ms must be a positive integer for inline video payloads"
                    )));
                }
            }
        }
        let (attachment, asset) = persist_inline_media_asset(storage, space_id, media, index)?;
        attachments.push(attachment);
        assets.push(asset);
    }

    Ok(PreparedMediaAttachments {
        attachments,
        assets,
    })
}

async fn persist_prepared_media_assets(
    storage: &StorageClient,
    assets: &[MediaAsset],
) -> McpResult<()> {
    let repo = MediaAssetRepository::new(storage.clone());
    let mut persisted_ids = Vec::with_capacity(assets.len());
    for asset in assets {
        match repo.create(asset.clone()).await {
            Ok(_) => persisted_ids.push(asset.base.id.clone()),
            Err(error) => {
                rollback_media_assets_by_ids(storage, &asset.base.space_id, &persisted_ids).await;
                return Err(McpError::ExecutionError(format!(
                    "failed to persist media asset metadata: {error}"
                )));
            }
        }
    }

    Ok(())
}

async fn rollback_media_assets_by_ids(
    storage: &StorageClient,
    space_id: &str,
    asset_ids: &[RecordId],
) {
    let repo = MediaAssetRepository::new(storage.clone());
    for asset_id in asset_ids {
        if let Err(error) = repo.delete(space_id, asset_id).await {
            warn!(asset_id = %asset_id, error = %error, "failed to roll back prepared media asset");
        }
    }
}

async fn rollback_managed_media_attachments(
    storage: &StorageClient,
    space_id: &str,
    attachments: &[DocMediaAttachment],
) {
    let mut asset_ids = Vec::new();
    for attachment in attachments {
        let DocMediaLocation::ManagedAsset { asset_id } = &attachment.location else {
            continue;
        };
        let Ok(record_id) = RecordId::from_str(asset_id) else {
            continue;
        };
        asset_ids.push(record_id);
    }
    rollback_media_assets_by_ids(storage, space_id, &asset_ids).await;
}

async fn rollback_prepared_media_assets(storage: &StorageClient, assets: &[MediaAsset]) {
    let Some(first_asset) = assets.first() else {
        return;
    };
    let asset_ids = assets
        .iter()
        .map(|asset| asset.base.id.clone())
        .collect::<Vec<_>>();
    rollback_media_assets_by_ids(storage, &first_asset.base.space_id, &asset_ids).await;
}

fn manual_source_hash(doc: &Doc) -> String {
    let mut hasher = Sha256::new();
    hasher.update(doc.title.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(doc.text.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(doc.sidecar_text.as_deref().unwrap_or_default().as_bytes());
    for attachment in &doc.media_attachments {
        hasher.update(b"\x1e");
        hasher.update(attachment.attachment_id.as_bytes());
        hasher.update(b"\x1f");
        hasher.update(match attachment.kind {
            DocMediaKind::Image => b"image".as_slice(),
            DocMediaKind::Video => b"video".as_slice(),
        });
        hasher.update(b"\x1f");
        hasher.update(attachment.media_type.as_bytes());
        hasher.update(b"\x1f");
        hasher.update(attachment.label.as_deref().unwrap_or_default().as_bytes());
        hasher.update(b"\x1f");
        hasher.update(
            attachment
                .size_bytes
                .unwrap_or_default()
                .to_string()
                .as_bytes(),
        );
        hasher.update(b"\x1f");
        hasher.update(
            attachment
                .duration_ms
                .unwrap_or_default()
                .to_string()
                .as_bytes(),
        );
        hasher.update(b"\x1f");
        hasher.update(attachment.sha256.as_deref().unwrap_or_default().as_bytes());
        hasher.update(b"\x1f");
        match &attachment.location {
            DocMediaLocation::ManagedAsset { asset_id } => {
                hasher.update(b"managed_asset");
                hasher.update(b"\x1f");
                hasher.update(asset_id.as_bytes());
            }
            DocMediaLocation::Path { path } => {
                hasher.update(b"path");
                hasher.update(b"\x1f");
                hasher.update(path.as_bytes());
            }
            DocMediaLocation::Uri { uri } => {
                hasher.update(b"uri");
                hasher.update(b"\x1f");
                hasher.update(uri.as_bytes());
            }
        }
    }
    hex::encode(hasher.finalize())
}

fn stage_manual_source(doc: &Doc) -> Result<Source, McpError> {
    let mut base = BaseFieldsBuilder::new(doc.base.space_id.clone())
        .for_table(TableName::Source)
        .created_by(doc.base.created_by.clone())
        .build();
    base.security = doc.base.security.clone();
    base.provenance = doc.base.provenance.clone();
    base.subjects.clone_from(&doc.base.subjects);
    base.subjects_state = doc.base.subjects_state;

    let template = manual_doc_template_from_doc(doc);

    let mut source = Source::new(base, SourceType::Manual, manual_source_hash(doc));
    source.external_id = Some(doc.base.id.to_string());
    source.author = Some(doc.base.created_by.clone());
    source.metadata = doc.metadata.clone();
    source
        .set_manual_doc_template(&template)
        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
    source.set_requested_ingest_mode(IngestMode::Graph);
    Ok(source)
}

async fn stage_source_for_manual_doc(
    ctx: &ToolContext,
    doc: &Doc,
) -> Result<(Source, Option<Source>), McpError> {
    if let Some(existing_doc) = find_existing_doc_for_doc(&ctx.storage, doc).await? {
        ctx.authz
            .authorize_record_write(&ctx.principal, &existing_doc, "write:doc")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    }

    if let Some(existing_source) = find_existing_source_for_doc(&ctx.storage, doc).await? {
        ctx.authz
            .authorize_record_write(&ctx.principal, &existing_source, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        let mut updated_source = existing_source.clone();
        apply_manual_doc_to_source(&mut updated_source, doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        updated_source.external_id = Some(doc.base.id.to_string());
        updated_source.set_requested_ingest_mode(IngestMode::Graph);
        let updated_source = prepare_source_for_reingest(&existing_source, updated_source);
        ctx.authz
            .authorize_record_write(&ctx.principal, &updated_source, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        return Ok((updated_source, Some(existing_source)));
    }

    let source = stage_manual_source(doc)?;
    ctx.authz
        .authorize_record_write(&ctx.principal, &source, "write:source")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    Ok((source, None))
}

async fn stage_source_for_source_backed_doc(
    ctx: &ToolContext,
    source: Source,
    doc: &Doc,
) -> Result<(Source, Option<Source>), McpError> {
    if let Some(existing_doc) = find_existing_doc_for_doc(&ctx.storage, doc).await? {
        ctx.authz
            .authorize_record_write(&ctx.principal, &existing_doc, "write:doc")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    }

    let existing_source = match find_existing_source_for_doc(&ctx.storage, doc).await? {
        Some(existing_source) => Some(existing_source),
        None => find_existing_source_for_source(&ctx.storage, &source).await?,
    };

    if let Some(existing_source) = existing_source {
        ctx.authz
            .authorize_record_write(&ctx.principal, &existing_source, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        let updated_source = prepare_source_for_reingest(&existing_source, source);
        ctx.authz
            .authorize_record_write(&ctx.principal, &updated_source, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        return Ok((updated_source, Some(existing_source)));
    }

    ctx.authz
        .authorize_record_write(&ctx.principal, &source, "write:source")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    Ok((source, None))
}

async fn authorize_persisted_manual_doc_access(
    ctx: &ToolContext,
    source: &Source,
    doc: &Doc,
) -> McpResult<()> {
    let preview = preview_persisted_manual_doc_for_source(&ctx.storage, source, doc)
        .await
        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
    ctx.authz
        .authorize_record_write(&ctx.principal, &preview, "write:doc")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    ctx.authz
        .authorize_record_read(&ctx.principal, &preview, "read:doc:metadata")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))
}

async fn persist_staged_manual_source_and_doc(
    ctx: &ToolContext,
    source_repo: &SourceRepository,
    source_candidate: Source,
    previous_source: Option<Source>,
    doc: Doc,
) -> Result<(Source, PersistedManualDoc), McpError> {
    if let Some(previous_source) = previous_source {
        let updated_source =
            update_existing_source_for_reingest(source_repo, &previous_source, source_candidate)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        match persist_manual_doc_for_source_detailed(&ctx.storage, &updated_source, doc).await {
            Ok(staged_doc) => Ok((updated_source, staged_doc)),
            Err(err) => {
                if let Err(rollback_err) =
                    rollback_persisted_source(&ctx.storage, &updated_source, Some(&previous_source))
                        .await
                {
                    warn!(
                        source_id = %previous_source.base.id,
                        error = %err,
                        rollback_error = %rollback_err,
                        "failed to roll back staged source after manual doc persistence error"
                    );
                }
                Err(McpError::ExecutionError(err.to_string()))
            }
        }
    } else {
        let created_source = source_repo
            .create(source_candidate)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        match persist_manual_doc_for_source_detailed(&ctx.storage, &created_source, doc).await {
            Ok(staged_doc) => Ok((created_source, staged_doc)),
            Err(err) => {
                if let Err(cleanup_err) =
                    rollback_persisted_source(&ctx.storage, &created_source, None).await
                {
                    warn!(
                        source_id = %created_source.base.id,
                        error = %err,
                        cleanup_error = %cleanup_err,
                        "failed to roll back staged source after manual doc persistence error"
                    );
                }
                Err(McpError::ExecutionError(err.to_string()))
            }
        }
    }
}

async fn rollback_staged_source_and_doc(
    ctx: &ToolContext,
    source: &Source,
    previous_source: Option<&Source>,
    staged_doc: &PersistedManualDoc,
) {
    if let Err(err) = rollback_persisted_manual_doc(&ctx.storage, staged_doc).await {
        warn!(
            doc_id = %staged_doc.doc.base.id,
            source_id = %source.base.id,
            rollback_error = %err,
            "failed to roll back staged manual doc after ingest failure"
        );
    }

    match previous_source {
        Some(previous_source) => {
            if let Err(err) =
                rollback_persisted_source(&ctx.storage, source, Some(previous_source)).await
            {
                warn!(
                    source_id = %previous_source.base.id,
                    rollback_error = %err,
                    "failed to roll back staged source after ingest failure"
                );
            }
        }
        None => {
            if let Err(err) = rollback_persisted_source(&ctx.storage, source, None).await {
                warn!(
                    source_id = %source.base.id,
                    rollback_error = %err,
                    "failed to delete staged source after ingest failure"
                );
            }
        }
    }
}

async fn enqueue_triage_source_work_item(
    storage: StorageClient,
    source: &Source,
    priority: Option<i32>,
) -> Result<(), McpError> {
    enqueue_triage_source_work_item_for_source(
        &storage,
        source,
        "mcp.ingest_doc",
        priority.unwrap_or(5),
    )
    .await
    .map(|_| ())
    .map_err(|e| McpError::ExecutionError(e.to_string()))
}

fn redact_staged_doc(
    principal: &elephant_policy::Principal,
    mut doc: Doc,
    source_id: &RecordId,
) -> elephant_policy::RedactionEnvelope<Doc> {
    doc.source = Some(Link::new(source_id.clone()));
    redact_doc(principal, doc)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn infer_source_type_hint(path: &std::path::Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    match ext.as_str() {
        "md" | "markdown" => "text/markdown",
        "pdf" => "application/pdf",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "tif" | "tiff" => "image/tiff",
        "mp4" | "m4v" => "video/mp4",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "avi" => "video/x-msvideo",
        _ => "text/plain",
    }
}

fn media_kind_for_source_type(source_type: &str) -> Option<DocMediaKind> {
    if source_type.starts_with("image/") {
        Some(DocMediaKind::Image)
    } else if source_type.starts_with("video/") {
        Some(DocMediaKind::Video)
    } else {
        None
    }
}

fn default_url_title(url: &str) -> String {
    url.rsplit('/')
        .find(|segment| !segment.is_empty())
        .map_or_else(
            || "URL ingestion".to_string(),
            std::borrow::ToOwned::to_owned,
        )
}

fn strip_xml_tags_to_text(xml: &str) -> String {
    let mut out = String::with_capacity(xml.len() / 4);
    let mut in_tag = false;
    let mut entity = String::new();
    let mut in_entity = false;

    for ch in xml.chars() {
        if in_entity {
            if ch == ';' {
                let decoded = match entity.as_str() {
                    "lt" => "<",
                    "gt" => ">",
                    "amp" => "&",
                    "quot" => "\"",
                    "apos" => "'",
                    _ => "",
                };
                out.push_str(decoded);
                entity.clear();
                in_entity = false;
            } else {
                entity.push(ch);
            }
            continue;
        }

        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            '&' if !in_tag => in_entity = true,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_text_from_docx(bytes: &[u8]) -> Result<String, McpError> {
    use std::io::{Cursor, Read};

    let reader = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|e| McpError::ExecutionError(format!("Failed to read docx zip: {e}")))?;
    let mut document = archive
        .by_name("word/document.xml")
        .map_err(|e| McpError::ExecutionError(format!("Missing word/document.xml: {e}")))?;
    let mut xml = String::new();
    document
        .read_to_string(&mut xml)
        .map_err(|e| McpError::ExecutionError(format!("Failed to read docx XML: {e}")))?;
    Ok(strip_xml_tags_to_text(&xml))
}

fn extract_text_by_source_type(bytes: &[u8], source_type: &str) -> Result<String, McpError> {
    match source_type {
        "application/pdf" => pdf_extract::extract_text_from_mem(bytes)
            .map_err(|e| McpError::ExecutionError(format!("Failed to extract PDF text: {e}"))),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            extract_text_from_docx(bytes)
        }
        _ => String::from_utf8(bytes.to_vec())
            .map_err(|e| McpError::ExecutionError(format!("File is not valid UTF-8: {e}"))),
    }
}

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `search_docs` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocsInput {
    /// Search query.
    pub query: String,
    /// Number of results (k-nearest).
    #[serde(default = "default_k")]
    pub k: usize,
    /// Filters.
    #[serde(default)]
    pub filters: Option<DocFilters>,
}

const fn default_k() -> usize {
    10
}

const SEMANTIC_CANDIDATE_MULTIPLIER: usize = 8;
const SEMANTIC_MIN_CANDIDATES: usize = 24;
const MAX_VISIBLE_TEXT_SEARCH_PAGES: usize = 1024;

/// Document filters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DocFilters {
    /// Filter by collection.
    #[serde(default)]
    pub collection: Option<String>,
    /// Include deleted.
    #[serde(default)]
    pub include_deleted: bool,
}

/// Output for `search_docs` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocsOutput {
    /// Search results with scores.
    pub items: Vec<DocSearchResult>,
    /// Visible results counted. Lexical counting stops once `k` documents are
    /// collected, so this is a lower bound when more matches exist.
    pub total: usize,
    /// Search mode actually used to produce the results.
    pub mode: String,
}

/// Document search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSearchResult {
    /// Document.
    pub doc: RedactionEnvelope<Doc>,
    /// Relevance score.
    pub score: f64,
    /// Matching chunks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_chunks: Option<Vec<String>>,
}

/// Input for `ingest_doc` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDocInput {
    /// Document to ingest.
    pub doc: Doc,
}

/// Input for `list_doc_chunks` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDocChunksInput {
    /// Document ID.
    pub id: String,
}

/// Input for `get_doc/delete/restore` tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocIdInput {
    /// Document ID.
    pub id: String,
}

/// Input for `extract_doc` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractDocInput {
    /// Document ID.
    pub id: String,
}

/// Input for `get_doc_pipeline_status` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDocPipelineStatusInput {
    /// Document ID.
    pub id: String,
}

/// Input for `batch_ingest_docs` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIngestDocsInput {
    /// Documents to ingest.
    pub docs: Vec<Doc>,
}

/// Output for batch operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIngestDocsOutput {
    /// Job/work item ID for progress tracking.
    pub job_id: String,
    /// Successfully processed items.
    pub succeeded: Vec<RedactionEnvelope<Doc>>,
    /// Failed items.
    pub failed: Vec<BatchError>,
}

/// Output for `list_doc_collections`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocCollectionsOutput {
    /// Collection names.
    pub collections: Vec<String>,
    /// Document counts per collection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counts: Option<std::collections::HashMap<String, usize>>,
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `search_docs` tool.
pub struct SearchDocsHandler {
    storage: StorageClient,
    doc_repo: DocRepository,
    embedding_repo: NodeEmbeddingRepository,
    space_config_repo: SpaceConfigRepository,
    provider_cache: EmbeddingProviderCache,
}

impl SearchDocsHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        let partition_manager = elephant_embeddings::PartitionManager::new();
        Self {
            storage: storage.clone(),
            doc_repo: DocRepository::new(storage.clone()),
            embedding_repo: NodeEmbeddingRepository::new(storage.clone(), partition_manager),
            space_config_repo: SpaceConfigRepository::new(storage),
            provider_cache: EmbeddingProviderCache::new(),
        }
    }
}

#[async_trait]
impl ToolHandler for SearchDocsHandler {
    fn name(&self) -> &'static str {
        "search_docs"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search documents using vector similarity when embeddings are available, ",
            "with lexical fallback when semantic search cannot be used.\n\n",
            "Example:\n",
            r#"{ "query": "Alex Chen", "k": 10, "filters": { "collection": "notes" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query text"
                },
                "k": {
                    "type": "integer",
                    "default": 10,
                    "maximum": 100,
                    "description": "Number of results to return"
                },
                "filters": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "collection": { "type": "string" },
                        "include_deleted": { "type": "boolean", "default": false }
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SearchDocsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);
        if let Err(reason) = ctx
            .authz
            .evaluate(&ctx.principal, &record, "read:doc:metadata")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let include_deleted = input
            .filters
            .as_ref()
            .is_some_and(|filters| filters.include_deleted);

        let policy = resolve_embedding_policy(&self.space_config_repo, &space_id).await?;
        let provider = self
            .provider_cache
            .get_or_create(policy.clone(), build_embedding_provider)
            .await;

        let embeddings = match provider.embed(std::slice::from_ref(&input.query)).await {
            Ok(embeddings) => embeddings,
            Err(err) => {
                warn!("Embedding provider failed, falling back to text search: {err}");
                let (docs, total_results) = collect_visible_text_search_results(
                    &self.doc_repo,
                    ctx,
                    &space_id,
                    &input.query,
                    input.k,
                    include_deleted,
                    input
                        .filters
                        .as_ref()
                        .and_then(|filters| filters.collection.as_deref()),
                )
                .await?;

                let items = ranked_doc_results(&ctx.principal, docs);

                let output = SearchDocsOutput {
                    total: total_results,
                    items,
                    mode: "lexical-fallback".to_string(),
                };

                let response = ToolResponse::success(output);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        };

        let Some(vector) = embeddings.into_iter().next() else {
            warn!("Embedding provider returned no vectors, falling back to text search");
            let (docs, total_results) = collect_visible_text_search_results(
                &self.doc_repo,
                ctx,
                &space_id,
                &input.query,
                input.k,
                include_deleted,
                input
                    .filters
                    .as_ref()
                    .and_then(|filters| filters.collection.as_deref()),
            )
            .await?;

            let items = ranked_doc_results(&ctx.principal, docs);

            let output = SearchDocsOutput {
                total: total_results,
                items,
                mode: "lexical-fallback".to_string(),
            };

            let response = ToolResponse::success(output);
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        let collection = input
            .filters
            .as_ref()
            .and_then(|filters| filters.collection.as_deref());
        let mut candidate_limit = semantic_candidate_limit(input.k.max(1));

        let keys = build_partition_keys(&self.storage, &policy, &ctx.principal).await;
        loop {
            let mut best_scores: HashMap<RecordId, f64> = HashMap::new();
            let mut had_partition_error = false;
            let mut may_have_more = false;
            // Highest "lowest returned score" across partitions that filled
            // their window: no unseen chunk anywhere can score above it.
            let mut full_partition_floor: Option<f64> = None;

            for key in &keys {
                match self
                    .embedding_repo
                    .search_partition_window(
                        key,
                        &space_id,
                        &vector,
                        candidate_limit,
                        collection,
                        include_deleted,
                    )
                    .await
                {
                    Ok(window) => {
                        if window.full {
                            may_have_more = true;
                            full_partition_floor = window.raise_floor(full_partition_floor);
                        }
                        for hit in window.hits {
                            best_scores
                                .entry(hit.doc_id)
                                .and_modify(|score| {
                                    if hit.score > *score {
                                        *score = hit.score;
                                    }
                                })
                                .or_insert(hit.score);
                        }
                    }
                    Err(err) => {
                        had_partition_error = true;
                        warn!("Vector search partition {} failed: {err}", key.table_name());
                    }
                }
            }

            if best_scores.is_empty() {
                if had_partition_error {
                    warn!("Vector search failed for all partitions; falling back to text search");
                }
                let (docs, total_results) = collect_visible_text_search_results(
                    &self.doc_repo,
                    ctx,
                    &space_id,
                    &input.query,
                    input.k,
                    include_deleted,
                    input
                        .filters
                        .as_ref()
                        .and_then(|filters| filters.collection.as_deref()),
                )
                .await?;

                let items = ranked_doc_results(&ctx.principal, docs);

                let output = SearchDocsOutput {
                    total: total_results,
                    items,
                    mode: "lexical-fallback".to_string(),
                };

                let response = ToolResponse::success(output);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }

            if had_partition_error {
                return Err(McpError::ExecutionError(
                    "Semantic search is temporarily unavailable; one or more vector partitions failed"
                        .to_string(),
                ));
            }

            let mut doc_ids: Vec<(RecordId, f64)> = best_scores.into_iter().collect();
            doc_ids.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.to_string().cmp(&right.0.to_string()))
            });
            let ordered_ids: Vec<RecordId> = doc_ids.iter().map(|(id, _)| id.clone()).collect();
            let doc_scores: HashMap<RecordId, f64> = doc_ids.iter().cloned().collect();

            let docs = self
                .doc_repo
                .get_by_ids(&space_id, &ordered_ids, include_deleted)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;

            let mut doc_map: HashMap<RecordId, Doc> = docs
                .into_iter()
                .map(|doc| (doc.base.id.clone(), doc))
                .collect();
            let docs: Vec<Doc> = ordered_ids
                .into_iter()
                .filter_map(|id| doc_map.remove(&id))
                .collect();

            let docs = ctx
                .authz
                .filter_records(&ctx.principal, docs, "read:doc:metadata");

            // Without a settled check a partition that keeps filling its
            // candidate window (every large one does, since nearest-neighbour
            // search always returns the full window) doubled the window until
            // it had read the whole partition.
            let kth_visible_score = input
                .k
                .checked_sub(1)
                .and_then(|index| docs.get(index))
                .and_then(|doc| doc_scores.get(&doc.base.id).copied());
            if !may_have_more
                || elephant_storage::semantic_window_settled(
                    docs.len(),
                    input.k,
                    kth_visible_score,
                    full_partition_floor,
                )
            {
                let semantic_total = docs.len();
                let (docs, total_results, blended_lexical) = blend_semantic_with_lexical_results(
                    &self.doc_repo,
                    ctx,
                    &space_id,
                    &input.query,
                    input.k,
                    include_deleted,
                    collection,
                    docs,
                    semantic_total,
                )
                .await?;
                let items =
                    ranked_doc_results(&ctx.principal, docs.into_iter().take(input.k).collect());

                let output = SearchDocsOutput {
                    total: total_results,
                    items,
                    mode: if blended_lexical {
                        "semantic+lexical".to_string()
                    } else {
                        "semantic".to_string()
                    },
                };

                let response = ToolResponse::success(output);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }

            let next_candidate_limit = candidate_limit.saturating_mul(2);
            if next_candidate_limit == candidate_limit {
                let semantic_total = docs.len();
                let (docs, total_results, blended_lexical) = blend_semantic_with_lexical_results(
                    &self.doc_repo,
                    ctx,
                    &space_id,
                    &input.query,
                    input.k,
                    include_deleted,
                    collection,
                    docs,
                    semantic_total,
                )
                .await?;
                let items =
                    ranked_doc_results(&ctx.principal, docs.into_iter().take(input.k).collect());

                let output = SearchDocsOutput {
                    total: total_results,
                    items,
                    mode: if blended_lexical {
                        "semantic+lexical".to_string()
                    } else {
                        "semantic".to_string()
                    },
                };

                let response = ToolResponse::success(output);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            candidate_limit = next_candidate_limit;
        }
    }
}

pub(crate) async fn resolve_embedding_policy(
    repo: &SpaceConfigRepository,
    space_id: &str,
) -> McpResult<EmbeddingPolicy> {
    let policy = match repo.get_by_space_id(space_id).await {
        Ok(Some(config)) => EmbeddingPolicy::from_space_config(&config),
        Ok(None) => EmbeddingPolicy::from_env(),
        Err(err) => {
            warn!("Failed to load space_config, falling back to env: {err}");
            EmbeddingPolicy::from_env()
        }
    };

    if policy.dims == 0 {
        return Err(McpError::ExecutionError(
            "Embedding policy has invalid dimensions".to_string(),
        ));
    }

    Ok(policy)
}

pub(crate) fn build_embedding_provider(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    elephant_embeddings::provider_for_policy(policy)
}

async fn build_partition_keys(
    storage: &StorageClient,
    policy: &EmbeddingPolicy,
    principal: &elephant_policy::Principal,
) -> Vec<PartitionKey> {
    let tables = match storage.list_tables().await {
        Ok(tables) => tables,
        Err(err) => {
            warn!("Failed to list embedding partitions: {err}");
            Vec::new()
        }
    };
    let clearances: Vec<_> = principal
        .clearances
        .iter()
        .map(|clearance| (clearance.policy_id.clone(), clearance.max_level))
        .collect();

    discover_accessible_partition_keys(&tables, policy, &clearances, "default", 0)
}

fn filter_docs_by_collection(
    docs: Vec<elephant_types::records::Doc>,
    collection: Option<&str>,
) -> Vec<elephant_types::records::Doc> {
    let Some(expected) = collection else {
        return docs;
    };

    docs.into_iter()
        .filter(|doc| {
            doc.metadata
                .get("collection")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == expected)
        })
        .collect()
}

fn ranked_doc_results(
    principal: &elephant_policy::Principal,
    docs: Vec<Doc>,
) -> Vec<DocSearchResult> {
    let returned_len = docs.len();
    docs.into_iter()
        .enumerate()
        .map(|(idx, doc)| {
            let score = if returned_len <= 1 {
                1.0
            } else {
                #[allow(clippy::cast_precision_loss)]
                {
                    1.0 - (idx as f64 * 0.9 / (returned_len - 1) as f64)
                }
            };

            DocSearchResult {
                doc: redact_doc(principal, doc),
                score,
                matching_chunks: None,
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn blend_semantic_with_lexical_results(
    doc_repo: &DocRepository,
    ctx: &ToolContext,
    space_id: &str,
    query: &str,
    k: usize,
    include_deleted: bool,
    collection: Option<&str>,
    semantic_docs: Vec<Doc>,
    semantic_total: usize,
) -> McpResult<(Vec<Doc>, usize, bool)> {
    let (lexical_docs, lexical_total) = collect_visible_text_search_results(
        doc_repo,
        ctx,
        space_id,
        query,
        k,
        include_deleted,
        collection,
    )
    .await?;

    let blended = !lexical_docs.is_empty();
    let docs = merge_lexical_and_semantic_docs(lexical_docs, semantic_docs, k);
    Ok((docs, lexical_total.max(semantic_total), blended))
}

/// Reciprocal-rank-fusion constant: a document's score from one ranking is
/// `1 / (RANK_FUSION_K + rank)`, rank starting at 1.
const RANK_FUSION_K: f64 = 60.0;

/// Fuse the lexical and semantic rankings (reciprocal rank fusion).
///
/// Lexical results used to come first wholesale, so any query sharing a
/// common word with unrelated documents ("bill", "house") buried every
/// semantic hit once real embeddings existed. Fusion rewards a document both
/// rankings agree on, and keeps each ranking's leaders near the top. Exact
/// ties keep lexical-first order, so a pure exact match still leads.
fn merge_lexical_and_semantic_docs(
    lexical_docs: Vec<Doc>,
    semantic_docs: Vec<Doc>,
    k: usize,
) -> Vec<Doc> {
    if k == 0 {
        return Vec::new();
    }

    let mut fused: Vec<(f64, usize, Doc)> = Vec::new();
    let mut position: HashMap<RecordId, usize> = HashMap::new();
    for ranking in [lexical_docs, semantic_docs] {
        for (rank, doc) in ranking.into_iter().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let contribution = 1.0 / (RANK_FUSION_K + (rank + 1) as f64);
            if let Some(&index) = position.get(&doc.base.id) {
                fused[index].0 += contribution;
            } else {
                position.insert(doc.base.id.clone(), fused.len());
                let order = fused.len();
                fused.push((contribution, order, doc));
            }
        }
    }
    fused.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
    fused.into_iter().take(k).map(|(_, _, doc)| doc).collect()
}

async fn collect_visible_text_search_results(
    doc_repo: &DocRepository,
    ctx: &ToolContext,
    space_id: &str,
    query: &str,
    k: usize,
    include_deleted: bool,
    collection: Option<&str>,
) -> McpResult<(Vec<Doc>, usize)> {
    let page_limit = k.max(25);
    let mut visible_docs = Vec::new();
    let mut visible_total = 0usize;
    let mut cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_TEXT_SEARCH_PAGES {
            return Err(McpError::ExecutionError(
                "Text search exceeded the maximum page scan bound".to_string(),
            ));
        }
        let mut criteria = SearchCriteria::new()
            .with_query(query)
            .with_limit(page_limit)
            .with_space_id(space_id.to_string());

        if include_deleted {
            criteria = criteria.include_deleted();
        }
        if let Some(current_cursor) = cursor.clone() {
            criteria = criteria.with_cursor(current_cursor);
        }

        let result = doc_repo
            .search(&criteria)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let next_cursor = result.cursor.clone();
        let docs = filter_docs_by_collection(
            ctx.authz
                .filter_records(&ctx.principal, result.results, "read:doc:metadata"),
            collection,
        );

        for doc in docs {
            visible_total += 1;
            if visible_docs.len() < k {
                visible_docs.push(doc);
            }
        }

        // Stop as soon as the caller's k visible documents are in hand. Every
        // page re-runs the ranked BM25 query at a growing offset plus a count
        // query, so walking every match only to count it was quadratic in the
        // match count: a common term over the production corpus took 30-70 s.
        // The reported total is therefore the visible count seen so far, a
        // lower bound once more pages remain. The repository count is not used
        // instead because it is taken before authorization filtering.
        if !result.has_more || visible_docs.len() >= k {
            break;
        }

        let Some(next_cursor) = next_cursor else {
            return Err(McpError::ExecutionError(
                "Text search pagination reported more results without a cursor".to_string(),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(McpError::ExecutionError(
                "Text search pagination cursor repeated without making progress".to_string(),
            ));
        }
        cursor = Some(next_cursor);
    }

    Ok((visible_docs, visible_total))
}

const fn semantic_candidate_limit(k: usize) -> usize {
    if k == 0 {
        0
    } else {
        let scaled = k.saturating_mul(SEMANTIC_CANDIDATE_MULTIPLIER);
        if scaled > SEMANTIC_MIN_CANDIDATES {
            scaled
        } else {
            SEMANTIC_MIN_CANDIDATES
        }
    }
}

/// Handler for `ingest_doc` tool.
pub struct IngestDocHandler {
    source_repo: SourceRepository,
}

impl IngestDocHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            source_repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for IngestDocHandler {
    fn name(&self) -> &'static str {
        "ingest_doc"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Ingest a new document for processing.\n\n",
            "Example:\n",
            r#"{
  "doc": {
    "title": "Meeting notes",
    "text": "Alice met Bob on 2026-02-02 to discuss Project Elephant."
  }
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["doc"],
            "properties": {
                "doc": doc_payload_schema()
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(doc) = params.get_mut("doc") {
            normalize_doc_value(doc, ctx)?;
        }
        let input: IngestDocInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        match find_existing_doc_for_doc(&ctx.storage, &input.doc).await {
            Ok(Some(existing_doc)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &existing_doc, "write:doc")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
            }
            Ok(None) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &input.doc, "write:doc")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
            }
            Err(err) => {
                return Err(McpError::ExecutionError(err.to_string()));
            }
        }
        let (source_candidate, previous_source) =
            stage_source_for_manual_doc(ctx, &input.doc).await?;
        if let Err(reason) =
            authorize_persisted_manual_doc_access(ctx, &source_candidate, &input.doc).await
        {
            let response: ToolResponse<()> = ToolResponse::error(reason.to_string());
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let rollback_source = previous_source.clone();
        let (staged_source, persisted_doc) = Box::pin(persist_staged_manual_source_and_doc(
            ctx,
            &self.source_repo,
            source_candidate,
            previous_source,
            input.doc,
        ))
        .await?;
        if let Err(err) =
            enqueue_triage_source_work_item(ctx.storage.clone(), &staged_source, None).await
        {
            rollback_staged_source_and_doc(
                ctx,
                &staged_source,
                rollback_source.as_ref(),
                &persisted_doc,
            )
            .await;
            return Err(err);
        }
        let response = ToolResponse::success(redact_staged_doc(
            &ctx.principal,
            persisted_doc.doc,
            &staged_source.base.id,
        ));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `ingest_doc_simple` tool.
pub struct IngestDocSimpleHandler {
    source_repo: SourceRepository,
}

impl IngestDocSimpleHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            source_repo: SourceRepository::new(storage),
        }
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl ToolHandler for IngestDocSimpleHandler {
    fn name(&self) -> &'static str {
        "ingest_doc_simple"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Ingest a document with only title + text; all base fields are auto-generated. ",
            "Media-backed documents pass `media` as an ordered array of inline attachments (up to 32); ",
            "all attachments belong to one document and are rendered together in order, so a burst of ",
            "related frames (for example one activity segment of screen captures) should be ingested ",
            "as a single document rather than one document per frame.\n\n",
            "Example:\n",
            r#"{ "title": "Test Doc", "text": "Alex Chen founded NeuralFlow", "collection": "demo" }"#,
            "\n\nMulti-image example:\n",
            r#"{ "title": "Chat session 14:02-14:07", "media": [{ "kind": "image", "media_type": "image/jpeg", "inline_base64": "<frame 1>" }, { "kind": "image", "media_type": "image/jpeg", "inline_base64": "<frame 2>" }] }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["title"],
            "anyOf": [
                { "required": ["text"] },
                { "required": ["media"] }
            ],
            "properties": {
                "title": { "type": "string", "description": "Document title" },
                "text": { "type": "string", "description": "Document text content" },
                "media": {
                    "type": "array",
                    "items": doc_media_input_schema(),
                    "maxItems": MAX_DOC_MEDIA_ATTACHMENTS,
                    "description": "Optional ordered image/video attachments for the document; all attachments are rendered together in order"
                },
                "collection": { "type": "string", "description": "Optional collection name for ingestion" },
                "metadata": { "type": "object", "required": [], "description": "Optional metadata object" },
                "priority": { "type": "integer", "description": "Optional work queue priority for this ingestion" },
                "extraction_skill": { "type": "string", "description": "Optional extraction skill override (for example extraction/example-observations)" },
                "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint" },
                "extraction_domain": { "type": "string", "description": "Optional extraction domain hint" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: IngestDocSimpleInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let normalized_text = normalize_optional_text(input.text.clone());
        let has_media = input.has_media();
        if !has_media && normalized_text.is_none() {
            return Err(McpError::InvalidParameters(
                "ingest_doc_simple requires non-empty text or at least one media attachment"
                    .to_string(),
            ));
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let prepared_media = build_media_attachments(&ctx.storage, &space_id, &input.media)?;
        let rollback_assets = prepared_media.assets.clone();

        let base = BaseFieldsBuilder::new(&space_id)
            .for_table(TableName::Doc)
            .build();

        let mut metadata = input
            .metadata
            .map_or_else(|| Value::Object(serde_json::Map::new()), Value::Object);

        if let (Some(collection), Value::Object(map)) = (input.collection, &mut metadata) {
            map.insert("collection".to_string(), Value::String(collection));
        }
        if let Value::Object(map) = &mut metadata {
            map.insert(
                "content_type".to_string(),
                Value::String(inferred_content_type(&input.media)),
            );
            input.extraction_guidance.apply_to_metadata(map);
        }

        let doc = Doc {
            base,
            title: input.title,
            text: if has_media {
                String::new()
            } else {
                normalized_text.clone().unwrap_or_default()
            },
            sidecar_text: has_media.then_some(normalized_text).flatten(),
            media_attachments: prepared_media.attachments,
            source: None,
            rendition_key: format!("mcp-{}", Uuid::new_v4()),
            metadata,
        };

        if let Err(reason) = ctx
            .authz
            .authorize_record_write(&ctx.principal, &doc, "write:doc")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        let (source_candidate, previous_source) = stage_source_for_manual_doc(ctx, &doc).await?;
        if let Err(reason) =
            authorize_persisted_manual_doc_access(ctx, &source_candidate, &doc).await
        {
            let response: ToolResponse<()> = ToolResponse::error(reason.to_string());
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        persist_prepared_media_assets(&ctx.storage, &prepared_media.assets).await?;
        let rollback_source = previous_source.clone();
        let (staged_source, persisted_doc) = match Box::pin(persist_staged_manual_source_and_doc(
            ctx,
            &self.source_repo,
            source_candidate,
            previous_source,
            doc,
        ))
        .await
        {
            Ok(value) => value,
            Err(err) => {
                rollback_prepared_media_assets(&ctx.storage, &rollback_assets).await;
                return Err(err);
            }
        };
        if let Err(err) =
            enqueue_triage_source_work_item(ctx.storage.clone(), &staged_source, input.priority)
                .await
        {
            rollback_prepared_media_assets(&ctx.storage, &rollback_assets).await;
            rollback_staged_source_and_doc(
                ctx,
                &staged_source,
                rollback_source.as_ref(),
                &persisted_doc,
            )
            .await;
            return Err(err);
        }
        let response = ToolResponse::success(redact_staged_doc(
            &ctx.principal,
            persisted_doc.doc,
            &staged_source.base.id,
        ));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Input for `ingest_file` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestFileInput {
    /// File path on disk.
    pub path: String,
    /// Optional title override for the created document.
    #[serde(default)]
    pub title: Option<String>,
    /// Optional sidecar text metadata for media-backed documents.
    #[serde(default)]
    pub text: Option<String>,
    /// Optional content type hint (e.g., "text/markdown", "application/pdf").
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional collection name for grouping documents.
    #[serde(default)]
    pub collection: Option<String>,
    /// Optional metadata key-value pairs.
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional priority for the ingestion work item.
    #[serde(default)]
    pub priority: Option<i32>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Input for `ingest_directory` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDirectoryInput {
    /// Directory path on disk.
    pub path: String,
    /// Optional glob pattern (defaults to "**/*").
    #[serde(default)]
    pub pattern: Option<String>,
    /// Optional content type hint applied to all files (rarely needed).
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional collection name applied to all files.
    #[serde(default)]
    pub collection: Option<String>,
    /// Optional metadata applied to all files.
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional priority applied to each ingestion work item.
    #[serde(default)]
    pub priority: Option<i32>,
    /// Optional extraction guidance applied to each file.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Handler for `ingest_file` tool.
pub struct IngestFileHandler {
    source_repo: SourceRepository,
}

impl IngestFileHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            source_repo: SourceRepository::new(storage),
        }
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl ToolHandler for IngestFileHandler {
    fn name(&self) -> &'static str {
        "ingest_file"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Ingest a local file by path. Text documents and media files are normalized into the same source-first pipeline.\n",
            "In Docker deployments, files must be placed in the /ingest mount (mapped from host via ELEPHANT_INGEST_DIR).\n\n",
            "Example:\n",
            r#"{ "path": "/ingest/doc.md" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": { "type": "string", "description": "File path on disk" },
                "title": { "type": "string", "description": "Optional document title override" },
                "text": { "type": "string", "description": "Optional sidecar text metadata for media-backed files" },
                "source_type": { "type": "string", "description": "Optional content type hint (e.g. text/markdown, application/pdf)" },
                "collection": { "type": "string", "description": "Optional collection name for ingestion" },
                "metadata": { "type": "object", "required": [], "description": "Optional metadata object" },
                "priority": { "type": "integer", "description": "Optional work queue priority for this ingestion" },
                "extraction_skill": { "type": "string", "description": "Optional extraction skill override (for example extraction/custom-domain)" },
                "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint" },
                "extraction_domain": { "type": "string", "description": "Optional extraction domain hint" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: IngestFileInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let path = std::path::PathBuf::from(&input.path);
        let fs_meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| McpError::ExecutionError(format!("Failed to stat file: {e}")))?;
        if !fs_meta.is_file() {
            let response: ToolResponse<()> = ToolResponse::error("Path is not a file");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| McpError::ExecutionError(format!("Failed to read file: {e}")))?;

        let source_type = input
            .source_type
            .as_deref()
            .unwrap_or_else(|| infer_source_type_hint(&path));
        let sidecar_text = normalize_optional_text(input.text);
        let media_kind = media_kind_for_source_type(source_type);
        let extracted_text = if media_kind.is_some() {
            String::new()
        } else {
            extract_text_by_source_type(&bytes, source_type)?
        };

        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("document")
            .to_string();
        let title = input.title.unwrap_or_else(|| filename.clone());

        let hash = sha256_hex(&bytes);
        let base = BaseFieldsBuilder::new(&space_id)
            .for_table(TableName::Source)
            .build();
        let mut source = Source::new(base, SourceType::Drive, hash.clone());
        source.external_id = Some(filename.clone());
        source.path = Some(input.path.clone());
        source.metadata = json!({
            "source_type": source_type,
            "size_bytes": fs_meta.len(),
        });
        source.set_requested_ingest_mode(IngestMode::Graph);
        source.base.id = deterministic_source_id_for_ingest(&source);

        let (media_attachments, pending_assets) = if let Some(kind) = media_kind {
            let duration_ms = if kind == DocMediaKind::Video {
                probe_video_duration_ms(&input.path).ok()
            } else {
                None
            };
            let (attachment, asset) = persist_managed_media_snapshot(
                &ctx.storage,
                &space_id,
                0,
                ManagedMediaSnapshot {
                    kind,
                    media_type: source_type.to_string(),
                    duration_ms,
                    label: Some(filename.clone()),
                    bytes: bytes.clone(),
                },
            )?;
            (vec![attachment], vec![asset])
        } else {
            (Vec::new(), Vec::new())
        };
        let rollback_attachments = media_attachments.clone();

        let doc_content_type = if media_kind.is_some() {
            source_type
        } else {
            match source_type {
                "text/markdown" => "text/markdown",
                _ => "text/plain",
            }
        };

        let mut metadata = input.metadata.unwrap_or_default();
        if let Some(collection) = input.collection {
            metadata.insert("collection".to_string(), Value::String(collection));
        }
        metadata.insert(
            "content_type".to_string(),
            Value::String(doc_content_type.to_string()),
        );
        metadata.insert(
            "source_type".to_string(),
            Value::String(source_type.to_string()),
        );
        metadata.insert(
            "source_id".to_string(),
            Value::String(source.base.id.to_string()),
        );
        metadata.insert("file_path".to_string(), Value::String(input.path));
        metadata.insert(
            "size_bytes".to_string(),
            Value::Number(serde_json::Number::from(fs_meta.len())),
        );
        input.extraction_guidance.apply_to_metadata(&mut metadata);

        let doc = Doc {
            base: BaseFieldsBuilder::new(ctx.principal.space_id.clone())
                .for_table(TableName::Doc)
                .build(),
            title,
            text: extracted_text,
            sidecar_text: media_kind.and_then(|_| sidecar_text.clone()),
            media_attachments,
            source: Some(elephant_types::Link::new(source.base.id.clone())),
            rendition_key: format!("file-{}", Uuid::new_v4()),
            metadata: Value::Object(metadata),
        };

        let (source_candidate, previous_source) =
            stage_source_for_source_backed_doc(ctx, source, &doc).await?;
        if let Err(reason) =
            authorize_persisted_manual_doc_access(ctx, &source_candidate, &doc).await
        {
            let response: ToolResponse<()> = ToolResponse::error(reason.to_string());
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        persist_prepared_media_assets(&ctx.storage, &pending_assets).await?;
        let rollback_source = previous_source.clone();
        let (staged_source, persisted_doc) = match Box::pin(persist_staged_manual_source_and_doc(
            ctx,
            &self.source_repo,
            source_candidate,
            previous_source,
            doc,
        ))
        .await
        {
            Ok(value) => value,
            Err(err) => {
                rollback_managed_media_attachments(&ctx.storage, &space_id, &rollback_attachments)
                    .await;
                return Err(err);
            }
        };
        if let Err(err) =
            enqueue_triage_source_work_item(ctx.storage.clone(), &staged_source, input.priority)
                .await
        {
            rollback_staged_source_and_doc(
                ctx,
                &staged_source,
                rollback_source.as_ref(),
                &persisted_doc,
            )
            .await;
            return Err(err);
        }

        let response = ToolResponse::success(redact_staged_doc(
            &ctx.principal,
            persisted_doc.doc,
            &staged_source.base.id,
        ));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `ingest_directory` tool.
pub struct IngestDirectoryHandler;

#[async_trait]
#[allow(clippy::too_many_lines)]
impl ToolHandler for IngestDirectoryHandler {
    fn name(&self) -> &'static str {
        "ingest_directory"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Ingest all files in a directory matching a glob pattern.\n\n",
            "Example:\n",
            r#"{ "path": "/path/to/docs", "pattern": "*.md" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": { "type": "string", "description": "Directory path on disk" },
                "pattern": { "type": "string", "description": "Glob pattern (e.g. *.md, **/*.txt)", "default": "**/*" },
                "source_type": { "type": "string", "description": "Optional content type hint to apply to all files" },
                "collection": { "type": "string", "description": "Optional collection name applied to all files" },
                "metadata": { "type": "object", "required": [], "description": "Optional metadata applied to all files" },
                "priority": { "type": "integer", "description": "Optional work queue priority applied to each file" },
                "extraction_skill": { "type": "string", "description": "Optional extraction skill override applied to each file" },
                "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint applied to each file" },
                "extraction_domain": { "type": "string", "description": "Optional extraction domain hint applied to each file" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: IngestDirectoryInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let root = std::path::PathBuf::from(&input.path);
        let meta = tokio::fs::metadata(&root)
            .await
            .map_err(|e| McpError::ExecutionError(format!("Failed to stat directory: {e}")))?;
        if !meta.is_dir() {
            let response: ToolResponse<()> = ToolResponse::error("Path is not a directory");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut pattern = input.pattern.unwrap_or_else(|| "**/*".to_string());
        if !pattern.contains('/') && !pattern.contains('\\') && !pattern.starts_with("**/") {
            pattern = format!("**/{pattern}");
        }
        let glob = globset::Glob::new(&pattern)
            .map_err(|e| McpError::InvalidParameters(format!("Invalid glob pattern: {e}")))?;
        let mut builder = globset::GlobSetBuilder::new();
        builder.add(glob);
        let matcher = builder
            .build()
            .map_err(|e| McpError::ExecutionError(format!("Failed to build glob matcher: {e}")))?;

        let mut ingested: Vec<serde_json::Value> = Vec::new();
        let mut failed: Vec<serde_json::Value> = Vec::new();

        let ingest_file = IngestFileHandler::new(ctx.storage.clone());
        for entry in walkdir::WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            let rel = entry
                .path()
                .strip_prefix(&root)
                .unwrap_or_else(|_| entry.path())
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if !matcher.is_match(&rel) {
                continue;
            }

            let path = entry.path().to_string_lossy().to_string();
            let mut file_params = json!({ "path": path });
            if let Some(obj) = file_params.as_object_mut() {
                if let Some(source_type) = input.source_type.as_deref() {
                    obj.insert(
                        "source_type".to_string(),
                        Value::String(source_type.to_string()),
                    );
                }
                if let Some(collection) = input.collection.as_deref() {
                    obj.insert(
                        "collection".to_string(),
                        Value::String(collection.to_string()),
                    );
                }
                if let Some(metadata) = &input.metadata {
                    obj.insert("metadata".to_string(), Value::Object(metadata.clone()));
                }
                if let Some(priority) = input.priority {
                    obj.insert(
                        "priority".to_string(),
                        Value::Number(serde_json::Number::from(priority)),
                    );
                }
                if let Some(skill) = input.extraction_guidance.extraction_skill.as_deref() {
                    obj.insert(
                        "extraction_skill".to_string(),
                        Value::String(skill.to_string()),
                    );
                }
                if let Some(purpose) = input.extraction_guidance.extraction_purpose.as_deref() {
                    obj.insert(
                        "extraction_purpose".to_string(),
                        Value::String(purpose.to_string()),
                    );
                }
                if let Some(domain) = input.extraction_guidance.extraction_domain.as_deref() {
                    obj.insert(
                        "extraction_domain".to_string(),
                        Value::String(domain.to_string()),
                    );
                }
            }

            match ingest_file.execute(ctx, file_params).await {
                Ok(value) => {
                    let decoded: Result<ToolResponse<RedactionEnvelope<Doc>>, _> =
                        serde_json::from_value(value);
                    match decoded {
                        Ok(resp) if resp.ok => {
                            let doc_id = resp.data.as_ref().map_or_else(
                                || "unknown".to_string(),
                                |env| env.data.base.id.to_string(),
                            );
                            ingested.push(json!({ "path": rel, "doc_id": doc_id }));
                        }
                        Ok(resp) => {
                            failed.push(json!({
                                "path": rel,
                                "error": resp.error.unwrap_or_else(|| "unknown error".to_string())
                            }));
                        }
                        Err(e) => failed.push(json!({ "path": rel, "error": e.to_string() })),
                    }
                }
                Err(e) => failed.push(json!({ "path": rel, "error": e.to_string() })),
            }
        }

        let response = ToolResponse::success(json!({
            "ingested": ingested,
            "failed": failed,
        }));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Input for `ingest_url` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestUrlInput {
    /// URL to fetch.
    pub url: String,
    /// Optional title override for the created document.
    #[serde(default)]
    pub title: Option<String>,
    /// Optional sidecar text metadata for media-backed documents.
    #[serde(default)]
    pub text: Option<String>,
    /// Optional content type hint (overrides HTTP header).
    #[serde(default)]
    pub source_type: Option<String>,
}

/// Handler for `ingest_url` tool.
pub struct IngestUrlHandler {
    source_repo: SourceRepository,
    fetch_policy: elephant_pipeline::remote_source::RemoteFetchPolicy,
}

impl IngestUrlHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            source_repo: SourceRepository::new(storage),
            fetch_policy: elephant_pipeline::remote_source::RemoteFetchPolicy::ingest(),
        }
    }

    #[cfg(test)]
    const fn with_fetch_policy(
        storage: StorageClient,
        fetch_policy: elephant_pipeline::remote_source::RemoteFetchPolicy,
    ) -> Self {
        Self {
            source_repo: SourceRepository::new(storage),
            fetch_policy,
        }
    }
}

#[async_trait]
impl ToolHandler for IngestUrlHandler {
    fn name(&self) -> &'static str {
        "ingest_url"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Fetch a URL and ingest its content. Text, image, and video URLs are normalized into the same source-first pipeline.\n\n",
            "Example:\n",
            r#"{ "url": "https://example.com/doc.txt" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": { "type": "string", "description": "HTTP/HTTPS URL to fetch" },
                "title": { "type": "string", "description": "Optional document title override" },
                "text": { "type": "string", "description": "Optional sidecar text metadata for media-backed URLs" },
                "source_type": { "type": "string", "description": "Optional content type hint (overrides response header)" }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: IngestUrlInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let fetched = elephant_pipeline::remote_source::fetch_remote_source(
            &input.url,
            &elephant_pipeline::remote_source::RemoteValidators::default(),
            self.fetch_policy,
        )
        .await
        .map_err(|error| McpError::ExecutionError(error.to_string()))?;
        let elephant_pipeline::remote_source::RemoteFetchOutcome::Snapshot(snapshot) = fetched
        else {
            return Err(McpError::ExecutionError(
                "unexpected not-modified response without validators".to_string(),
            ));
        };
        let header_type = snapshot
            .content_type
            .unwrap_or_else(|| "text/plain".to_string());
        let bytes = snapshot.bytes;

        let source_type = input.source_type.unwrap_or(header_type);
        let sidecar_text = normalize_optional_text(input.text);
        let media_kind = media_kind_for_source_type(&source_type);
        let extracted = if media_kind.is_some() {
            String::new()
        } else {
            extract_text_by_source_type(&bytes, &source_type)?
        };
        let title = input.title.unwrap_or_else(|| default_url_title(&input.url));

        let hash = sha256_hex(&bytes);
        let base = BaseFieldsBuilder::new(&space_id)
            .for_table(TableName::Source)
            .build();
        let mut source = Source::new(base, SourceType::Web, hash.clone());
        source.external_id = Some(input.url.clone());
        source.uri = Some(input.url.clone());
        source.metadata = json!({ "source_type": source_type });
        source.base.id = deterministic_source_id_for_ingest(&source);

        let (media_attachments, pending_assets) = if let Some(kind) = media_kind {
            let duration_ms = if kind == DocMediaKind::Video {
                probe_video_duration_ms_from_bytes(
                    bytes.as_ref(),
                    url_extension_hint(&input.url, &source_type),
                )
                .ok()
            } else {
                None
            };
            let (attachment, asset) = persist_managed_media_snapshot(
                &ctx.storage,
                &space_id,
                0,
                ManagedMediaSnapshot {
                    kind,
                    media_type: source_type.clone(),
                    duration_ms,
                    label: Some(title.clone()),
                    bytes: bytes.clone(),
                },
            )?;
            (vec![attachment], vec![asset])
        } else {
            (Vec::new(), Vec::new())
        };
        let rollback_attachments = media_attachments.clone();

        let doc_content_type = if media_kind.is_some() || source_type.starts_with("text/") {
            source_type.as_str()
        } else {
            "text/plain"
        };

        let metadata = json!({
            "content_type": doc_content_type,
            "source_type": source_type,
            "source_id": source.base.id.to_string(),
            "url": input.url,
        });

        let doc = Doc {
            base: BaseFieldsBuilder::new(ctx.principal.space_id.clone())
                .for_table(TableName::Doc)
                .build(),
            title,
            text: extracted,
            sidecar_text: media_kind.and_then(|_| sidecar_text.clone()),
            media_attachments,
            source: Some(elephant_types::Link::new(source.base.id.clone())),
            rendition_key: format!("url-{}", Uuid::new_v4()),
            metadata,
        };

        let (source_candidate, previous_source) =
            stage_source_for_source_backed_doc(ctx, source, &doc).await?;
        if let Err(reason) =
            authorize_persisted_manual_doc_access(ctx, &source_candidate, &doc).await
        {
            let response: ToolResponse<()> = ToolResponse::error(reason.to_string());
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        persist_prepared_media_assets(&ctx.storage, &pending_assets).await?;
        let rollback_source = previous_source.clone();
        let (staged_source, persisted_doc) = match Box::pin(persist_staged_manual_source_and_doc(
            ctx,
            &self.source_repo,
            source_candidate,
            previous_source,
            doc,
        ))
        .await
        {
            Ok(value) => value,
            Err(err) => {
                rollback_managed_media_attachments(&ctx.storage, &space_id, &rollback_attachments)
                    .await;
                return Err(err);
            }
        };
        if let Err(err) =
            enqueue_triage_source_work_item(ctx.storage.clone(), &staged_source, None).await
        {
            rollback_staged_source_and_doc(
                ctx,
                &staged_source,
                rollback_source.as_ref(),
                &persisted_doc,
            )
            .await;
            return Err(err);
        }

        let response = ToolResponse::success(redact_staged_doc(
            &ctx.principal,
            persisted_doc.doc,
            &staged_source.base.id,
        ));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

fn url_extension_hint<'a>(url: &'a str, media_type: &'a str) -> Option<&'a str> {
    std::path::Path::new(url)
        .extension()
        .and_then(|value| value.to_str())
        .or_else(|| media_type.rsplit('/').next())
}

/// Handler for `extract_doc` tool.
pub struct ExtractDocHandler {
    repo: DocRepository,
    work_item_repo: WorkItemRepository,
}

impl ExtractDocHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocRepository::new(storage.clone()),
            work_item_repo: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ExtractDocHandler {
    fn name(&self) -> &'static str {
        "extract_doc"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Maintenance helper that manually replays render/build/extraction preparation for an existing document.\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string", "description": "Document ID" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ExtractDocInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let doc_id = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let doc = self
            .repo
            .get(&space_id, &doc_id)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?
            .ok_or_else(|| McpError::ExecutionError("Document not found".to_string()))?;

        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &doc, "read:doc:raw")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut registry = RendererRegistry::new();
        registry.register(Box::new(DefaultTextRenderer::new()));
        registry.register(Box::new(MultimodalRenderer::new()));
        registry.register(Box::new(PassthroughRenderer::new()));

        let content_type = doc
            .metadata
            .get("content_type")
            .and_then(Value::as_str)
            .or_else(|| {
                doc.media_attachments
                    .first()
                    .map(|item| item.media_type.as_str())
            })
            .unwrap_or("text/plain");

        let render_result = execute_render_doc(&ctx.storage, &registry, &doc, Some(content_type))
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let config = DocTreeConfig::default();
        execute_build_doc_tree(&ctx.storage, &render_result.revision_id, &config)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let idempotency_key = format!("{PLAN_EXTRACTION_KIND}:{}", render_result.revision_id);
        let existing = self
            .work_item_repo
            .find_by_idempotency_key(&space_id, &idempotency_key)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let work_item_id = if let Some(item) = existing {
            item.base.id
        } else {
            let mut base = BaseFieldsBuilder::new(space_id.clone())
                .for_table(TableName::WorkItem)
                .created_by(doc.base.created_by.clone())
                .build();
            base.security = doc.base.security.clone();
            base.provenance = doc.base.provenance.clone();
            base.subjects.clone_from(&doc.base.subjects);
            base.subjects_state = doc.base.subjects_state;

            let work_item = WorkItem {
                base,
                kind: PLAN_EXTRACTION_KIND.to_string(),
                payload: json!({
                    "revision_id": render_result.revision_id.to_string(),
                    "policy_version": "default",
                    "extractor_id": AGENTIC_EXTRACTOR_ID,
                    "allow_raw_quotes": false,
                    "ingest_mode": "graph"
                }),
                status: WorkItemStatus::Runnable,
                priority: 50,
                idempotency_key,
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };
            self.work_item_repo
                .create(work_item)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
                .base
                .id
        };

        let response = ToolResponse::success(json!({
            "doc_id": doc.base.id.to_string(),
            "revision_id": render_result.revision_id.to_string(),
            "work_item_id": work_item_id.to_string()
        }));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_doc_pipeline_status` tool.
#[allow(clippy::struct_field_names)]
pub struct GetDocPipelineStatusHandler {
    doc_repo: DocRepository,
    source_repo: SourceRepository,
    space_config_repo: SpaceConfigRepository,
    revision_repo: DocRevisionRepository,
    node_repo: DocNodeRepository,
    work_item_repo: WorkItemRepository,
    assertion_repo: AssertionRepository,
}

impl GetDocPipelineStatusHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            doc_repo: DocRepository::new(storage.clone()),
            source_repo: SourceRepository::new(storage.clone()),
            space_config_repo: SpaceConfigRepository::new(storage.clone()),
            revision_repo: DocRevisionRepository::new(storage.clone()),
            node_repo: DocNodeRepository::new(storage.clone()),
            work_item_repo: WorkItemRepository::new(storage.clone()),
            assertion_repo: AssertionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetDocPipelineStatusHandler {
    fn name(&self) -> &'static str {
        "get_doc_pipeline_status"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get document pipeline status (chunks, revisions, nodes, extraction progress).\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string", "description": "Document ID" }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetDocPipelineStatusInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let doc_id = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let Some(doc) = self
            .doc_repo
            .get(&space_id, &doc_id)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?
        else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Document not found: {}", input.id));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &doc, "read:doc")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let revisions = self
            .revision_repo
            .find_by_doc(&doc_id)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let selected_revision = revisions
            .iter()
            .find(|rev| rev.status == elephant_types::records::doc_revision::RevisionStatus::Active)
            .or_else(|| revisions.first());
        let (chunks_total, chunks_embedded, revision, nodes) =
            if let Some(revision) = selected_revision {
                let nodes = self
                    .node_repo
                    .find_by_revision(&revision.base.id)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                let embeddable: Vec<_> = nodes
                    .iter()
                    .filter(|node| {
                        matches!(
                            node.role,
                            elephant_types::records::doc_node::NodeRole::Chunk
                                | elephant_types::records::doc_node::NodeRole::Paragraph
                                | elephant_types::records::doc_node::NodeRole::Summary
                        )
                    })
                    .collect();
                (
                    embeddable.len(),
                    embeddable
                        .iter()
                        .filter(|node| node.embedding_model.is_some())
                        .count(),
                    Some(revision.base.id.clone()),
                    nodes,
                )
            } else {
                (0, 0, None, Vec::new())
            };

        let mut status = DocPipelineStatus {
            doc_id: doc_id.clone(),
            chunks: ChunkStatus {
                total: chunks_total,
                embedded: chunks_embedded,
            },
            revision: None,
            nodes_count: 0,
            extraction_state: ExtractionState::NotTriggered,
            assertions_count: 0,
            entities_created: 0,
            extraction_guidance: None,
        };

        let space_config = self
            .space_config_repo
            .get_by_space_id(&space_id)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let source = if let Some(source_link) = &doc.source {
            self.source_repo
                .get(&space_id, source_link.id())
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
        } else {
            None
        };
        status.extraction_guidance = Some(resolve_extraction_guidance_status(
            space_config.as_ref(),
            Some(&doc),
            source.as_ref(),
        ));

        if let Some(revision_id) = &revision {
            status.revision = Some(revision_id.clone());
            status.nodes_count = nodes.len();

            let assertions = self
                .assertion_repo
                .find_by_revision(&space_id, revision_id, 5000)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            status.assertions_count = assertions.len();

            let mut entities: std::collections::HashSet<RecordId> =
                std::collections::HashSet::new();
            for assertion in &assertions {
                if let Some(link) = &assertion.subject_entity {
                    entities.insert(link.id().clone());
                }
                if let Some(link) = &assertion.object_entity {
                    entities.insert(link.id().clone());
                }
            }
            status.entities_created = entities.len();
        }

        let work_items = self
            .work_item_repo
            .find_document_work(
                &space_id,
                &doc_id.to_string(),
                revision.as_ref().map(ToString::to_string).as_deref(),
                doc.source
                    .as_ref()
                    .map(|source| source.id().to_string())
                    .as_deref(),
            )
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let revision_str = revision.as_ref().map(ToString::to_string);
        let doc_id_str = doc_id.to_string();
        let rendition_key = doc.rendition_key.as_str();
        let source_id_str = doc.source.as_ref().map(|source| source.id().to_string());
        let relevant_statuses: Vec<WorkItemStatus> = work_items
            .into_iter()
            .filter(|item| {
                // Match extraction work items by revision
                let revision_match = (item.kind == PLAN_EXTRACTION_KIND
                    || item.kind == EXTRACT_ASSERTIONS_KIND)
                    && revision_str.as_deref().is_some_and(|revision_id| {
                        item.payload
                            .get("revision_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == revision_id)
                    });
                // Match render work items by doc ID
                let render_match = item.kind == RENDER_DOC_KIND
                    && item
                        .payload
                        .get("doc_id")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == doc_id_str)
                    && item
                        .payload
                        .get("rendition_key")
                        .and_then(Value::as_str)
                        .is_none_or(|value| value == rendition_key);
                let source_lifecycle_match = source_id_str
                    .as_deref()
                    .is_some_and(|source_id| work_item_matches_source_lifecycle(item, source_id));
                revision_match || render_match || source_lifecycle_match
            })
            .map(|item| item.status)
            .collect();

        status.extraction_state =
            derive_extraction_state(status.assertions_count, &relevant_statuses);
        if graph_ingest_is_still_pending(source.as_ref(), status.assertions_count) {
            status.extraction_state = match status.extraction_state {
                ExtractionState::Failed | ExtractionState::Running => status.extraction_state,
                _ => ExtractionState::Pending,
            };
        }

        let response = ToolResponse::success(status);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

fn graph_ingest_is_still_pending(source: Option<&Source>, assertions_count: usize) -> bool {
    if assertions_count > 0 {
        return false;
    }

    let Some(source) = source else {
        return false;
    };

    if source.requested_ingest_mode() != Some(IngestMode::Graph) {
        return false;
    }

    !matches!(
        source.lifecycle_status,
        SourceLifecycleStatus::GraphReady
            | SourceLifecycleStatus::Skipped
            | SourceLifecycleStatus::Rejected
    )
}

fn work_item_matches_source_lifecycle(item: &WorkItem, source_id: &str) -> bool {
    matches!(
        item.kind.as_str(),
        TRIAGE_SOURCE_KIND
            | INGEST_METADATA_KIND
            | UPGRADE_INGEST_MODE_KIND
            | VALIDATE_ARTIFACTS_KIND
            | COMMIT_ARTIFACTS_KIND
    ) && item
        .payload
        .get("source_id")
        .and_then(Value::as_str)
        .is_some_and(|value| value == source_id)
}

/// Handler for `list_doc_collections` tool.
pub struct ListDocCollectionsHandler {
    repo: DocRepository,
}

impl ListDocCollectionsHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListDocCollectionsHandler {
    fn name(&self) -> &'static str {
        "list_doc_collections"
    }

    fn description(&self) -> &'static str {
        concat!("List all document collections.\n\n", "Example:\n", r#"{}"#,)
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {}
        })
    }

    async fn execute(&self, ctx: &ToolContext, _params: Value) -> McpResult<Value> {
        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);
        if let Err(reason) = ctx
            .authz
            .evaluate(&ctx.principal, &record, "read:doc:metadata")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let collections = self
            .repo
            .get_distinct_collections_for_space(&space_id)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        // No chunk-level ABAC filtering needed since collections are doc-level
        let filtered = collections;

        let output = DocCollectionsOutput {
            collections: filtered,
            counts: None,
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_doc` tool.
pub struct GetDocHandler {
    repo: DocRepository,
}

impl GetDocHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetDocHandler {
    fn name(&self) -> &'static str {
        "get_doc"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a document by ID.\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Document ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: DocIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(doc)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &doc, "read:doc:metadata")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(redact_doc(&ctx.principal, doc));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Document not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `list_doc_chunks` tool (returns `doc_node` records).
#[allow(clippy::struct_field_names)]
pub struct ListDocChunksHandler {
    doc_repo: DocRepository,
    revision_repo: DocRevisionRepository,
    node_repo: DocNodeRepository,
}

impl ListDocChunksHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            doc_repo: DocRepository::new(storage.clone()),
            revision_repo: DocRevisionRepository::new(storage.clone()),
            node_repo: DocNodeRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListDocChunksHandler {
    fn name(&self) -> &'static str {
        "list_doc_chunks"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List all structural nodes (chunks) for a document.\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Document ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListDocChunksInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let doc = match self.doc_repo.get(&space_id, &rid).await {
            Ok(Some(d)) => d,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Document not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &doc, "read:doc:metadata")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        // Get nodes via revision
        let revisions = self
            .revision_repo
            .find_by_doc(&rid)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        let nodes = if let Some(rev) = revisions.first() {
            self.node_repo
                .find_by_revision(&rev.base.id)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
        } else {
            vec![]
        };

        let response = ToolResponse::success(nodes);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_doc_chunk` tool (returns a `doc_node` record).
pub struct GetDocChunkHandler {
    repo: DocNodeRepository,
}

impl GetDocChunkHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocNodeRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetDocChunkHandler {
    fn name(&self) -> &'static str {
        "get_doc_chunk"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a document node (chunk) by ID.\n\n",
            "Example:\n",
            r#"{ "id": "doc_node:1c0d6c6f-5e3b-4e7d-9c5c-8c4a8f3b7f2a" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Document node ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: DocIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::DocNode)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(node)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &node, "read:doc")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                // Redact text if principal only has metadata access
                let mut redacted = node;
                if ctx
                    .authz
                    .authorize_record_read(&ctx.principal, &redacted, "read:doc:raw")
                    .is_err()
                {
                    redacted.text = None;
                }

                let response = ToolResponse::success(redacted);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Document node not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `delete_doc` tool.
pub struct DeleteDocHandler {
    repo: DocRepository,
}

impl DeleteDocHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for DeleteDocHandler {
    fn name(&self) -> &'static str {
        "delete_doc"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Soft delete a document.\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Document ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: DocIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(doc)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &doc, "write:doc")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                self.repo
                    .delete(&space_id, &rid)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;

                let response = ToolResponse::success(json!({ "deleted": true, "id": input.id }));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Document not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `restore_doc` tool.
pub struct RestoreDocHandler {
    repo: DocRepository,
}

impl RestoreDocHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: DocRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RestoreDocHandler {
    fn name(&self) -> &'static str {
        "restore_doc"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Restore a soft-deleted document.\n\n",
            "Example:\n",
            r#"{ "id": "doc:3fa85f64-5717-4562-b3fc-2c963f66afa6" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Document ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: DocIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Doc)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(doc)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &doc, "write:doc")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &doc, "read:doc:metadata")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                let restored = self
                    .repo
                    .restore(&space_id, &rid)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;

                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &restored, "read:doc:metadata")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }

                let response = ToolResponse::success(redact_doc(&ctx.principal, restored));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Document not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Metadata key carrying Angel protectee attribution on docs and entities.
const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

/// Cap on docs/assertions walked per restamp selector; family-scale spaces
/// stay far below this.
const RESTAMP_MATCH_LIMIT: usize = 10_000;

/// Reads `metadata.angel_protectees` as a list of string ids (missing keys,
/// non-array values, and non-string elements yield an empty/filtered list).
///
/// Mirrors the pub(crate) helpers in `elephant-pipeline` promotion and
/// `elephant-identity` merge code (the shared attribution contract).
fn angel_protectees_from_metadata(metadata: &Value) -> Vec<String> {
    metadata
        .get(ANGEL_PROTECTEES_METADATA_KEY)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Input for the `restamp_attribution` tool.
#[derive(Debug, Deserialize)]
struct RestampAttributionInput {
    /// Match docs whose `metadata.angel_session_id` equals this value.
    #[serde(default)]
    session_id: Option<String>,
    /// Explicit document ids to re-stamp.
    #[serde(default)]
    doc_ids: Option<Vec<String>>,
    /// Match docs whose linked source uri starts with this prefix.
    #[serde(default)]
    source_uri_prefix: Option<String>,
    /// Exact replacement attribution list; empty = explicit family-wide.
    angel_protectees: Vec<String>,
}

/// Handler for `restamp_attribution` tool.
///
/// Applies an authoritative attribution correction (e.g. an Angel parent
/// correcting which child a monitoring session belonged to): matched docs get
/// `metadata.angel_protectees` replaced exactly, then entities derived from
/// those docs get their attribution recomputed from all their source docs.
pub struct RestampAttributionHandler {
    docs: DocRepository,
    revisions: DocRevisionRepository,
    assertions: AssertionRepository,
    entities: EntityRepository,
    sources: SourceRepository,
}

impl RestampAttributionHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            docs: DocRepository::new(storage.clone()),
            revisions: DocRevisionRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage.clone()),
            entities: EntityRepository::new(storage.clone()),
            sources: SourceRepository::new(storage),
        }
    }

    /// Collects the docs matched by the input selectors, deduped by id.
    ///
    /// Selectors combine as a union. Explicit `doc_ids` must resolve;
    /// `session_id` / `source_uri_prefix` may legitimately match nothing.
    async fn collect_target_docs(
        &self,
        space_id: &str,
        input: &RestampAttributionInput,
    ) -> McpResult<Result<Vec<Doc>, String>> {
        let mut docs: Vec<Doc> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        if let Some(doc_ids) = &input.doc_ids {
            for raw_id in doc_ids {
                let rid = parse_id(raw_id, TableName::Doc)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
                match self.docs.get(space_id, &rid).await {
                    Ok(Some(doc)) => {
                        if seen.insert(doc.base.id.to_string()) {
                            docs.push(doc);
                        }
                    }
                    Ok(None) => return Ok(Err(format!("Document not found: {raw_id}"))),
                    Err(e) => return Err(McpError::ExecutionError(e.to_string())),
                }
            }
        }

        if let Some(session_id) = &input.session_id {
            let matched = self
                .docs
                .find_by_angel_session_id(space_id, session_id, RESTAMP_MATCH_LIMIT)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            for doc in matched {
                if seen.insert(doc.base.id.to_string()) {
                    docs.push(doc);
                }
            }
        }

        if let Some(prefix) = &input.source_uri_prefix {
            let sources = self
                .sources
                .find_by_uri_prefix(space_id, prefix, RESTAMP_MATCH_LIMIT)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            for source in sources {
                let matched = self
                    .docs
                    .find_by_source(space_id, &source.base.id, RESTAMP_MATCH_LIMIT)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                for doc in matched {
                    if seen.insert(doc.base.id.to_string()) {
                        docs.push(doc);
                    }
                }
            }
        }

        Ok(Ok(docs))
    }

    /// Collects ids of entities whose assertion evidence traces to the docs.
    async fn collect_affected_entity_ids(
        &self,
        space_id: &str,
        docs: &[Doc],
    ) -> McpResult<Vec<RecordId>> {
        let mut entity_ids: Vec<RecordId> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for doc in docs {
            let revisions = self
                .revisions
                .find_by_doc(&doc.base.id)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            for revision in revisions {
                if revision.base.space_id != space_id {
                    continue;
                }
                let assertions = self
                    .assertions
                    .find_by_revision(space_id, &revision.base.id, RESTAMP_MATCH_LIMIT)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                for assertion in assertions {
                    for link in [&assertion.subject_entity, &assertion.object_entity]
                        .into_iter()
                        .flatten()
                    {
                        if seen.insert(link.id().to_string()) {
                            entity_ids.push(link.id().clone());
                        }
                    }
                }
            }
        }

        Ok(entity_ids)
    }

    /// Recomputes an entity's attribution as the union of
    /// `metadata.angel_protectees` across every doc referenced by the
    /// entity's assertion evidence.
    async fn recompute_entity_protectees(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        doc_protectees_cache: &mut HashMap<String, Vec<String>>,
        revision_doc_cache: &mut HashMap<String, Option<RecordId>>,
    ) -> McpResult<Vec<String>> {
        let assertions = self
            .assertions
            .find_by_resolved_entity(space_id, entity_id, RESTAMP_MATCH_LIMIT)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let mut union: Vec<String> = Vec::new();
        for assertion in &assertions {
            let revision_id = assertion.evidence.revision.id();
            let revision_key = revision_id.to_string();
            if !revision_doc_cache.contains_key(&revision_key) {
                let resolved = self
                    .revisions
                    .get_by_id(revision_id)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?
                    .filter(|revision| revision.base.space_id == space_id)
                    .map(|revision| revision.doc.id().clone());
                revision_doc_cache.insert(revision_key.clone(), resolved);
            }
            let Some(Some(doc_id)) = revision_doc_cache.get(&revision_key).cloned() else {
                continue;
            };

            let doc_key = doc_id.to_string();
            if !doc_protectees_cache.contains_key(&doc_key) {
                let protectees = self
                    .docs
                    .get(space_id, &doc_id)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?
                    .map(|doc| angel_protectees_from_metadata(&doc.metadata))
                    .unwrap_or_default();
                doc_protectees_cache.insert(doc_key.clone(), protectees);
            }
            if let Some(protectees) = doc_protectees_cache.get(&doc_key) {
                union.extend(protectees.iter().cloned());
            }
        }

        union.sort();
        union.dedup();
        Ok(union)
    }
}

#[async_trait]
impl ToolHandler for RestampAttributionHandler {
    fn name(&self) -> &'static str {
        "restamp_attribution"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Re-stamp Angel protectee attribution on documents and re-propagate it onto derived entities. ",
            "Matched docs get metadata.angel_protectees replaced with EXACTLY the given list (a correction is authoritative, not a union; an empty array marks the docs explicitly family-wide). ",
            "Entities whose assertion evidence traces to the matched docs are then recomputed: their angel_protectees becomes the union across ALL docs referenced by their assertions, and an empty union removes the key (family-wide).\n\n",
            "Doc selectors (provide at least one; matches are unioned): doc_ids (explicit ids), ",
            "session_id (docs whose metadata.angel_session_id equals it — only works when the ingesting client stamps that key; Angel's session-linked doc ingests do), ",
            "source_uri_prefix (docs whose LINKED SOURCE's uri starts with the prefix — only reaches docs staged on such a source; note Angel's docs are staged on synthetic manual sources, not its angel://… frame sources, so for Angel use doc_ids or session_id).\n\n",
            "Limitation: entity recompute follows assertion evidence spans; attribution that reached an entity via a deduplicated re-promotion (a later doc reusing an existing assertion, leaving no evidence row for that doc) is not reconstructed and may be dropped.\n\n",
            "Example:\n",
            r#"{ "session_id": "sess-42", "angel_protectees": ["child-a"] }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["angel_protectees"],
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Match docs whose metadata.angel_session_id equals this value"
                },
                "doc_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Explicit document IDs to re-stamp"
                },
                "source_uri_prefix": {
                    "type": "string",
                    "description": "Match docs staged on a source whose uri starts with this prefix (docs staged on other sources — e.g. the synthetic manual sources doc ingests mint — are not reached)"
                },
                "angel_protectees": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Exact replacement list of protectee ids; empty array = explicit family-wide"
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RestampAttributionInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let has_selector = input.session_id.is_some()
            || input.source_uri_prefix.is_some()
            || input.doc_ids.as_ref().is_some_and(|ids| !ids.is_empty());
        if !has_selector {
            let response: ToolResponse<()> = ToolResponse::error(
                "Provide at least one doc selector: session_id, doc_ids, or source_uri_prefix",
            );
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut protectees = input.angel_protectees.clone();
        protectees.sort();
        protectees.dedup();

        let docs = match self.collect_target_docs(&space_id, &input).await? {
            Ok(docs) => docs,
            Err(reason) => {
                let response: ToolResponse<()> = ToolResponse::error(reason);
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        };

        // All-or-nothing doc authorization: refuse before mutating anything.
        for doc in &docs {
            if let Err(reason) = ctx
                .authz
                .authorize_record_write(&ctx.principal, doc, "write:doc")
            {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Access denied: {reason}"));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
        }

        // Re-stamp the docs; seed the cache so recompute sees the new lists.
        let mut doc_protectees_cache: HashMap<String, Vec<String>> = HashMap::new();
        for doc in &docs {
            self.docs
                .set_angel_protectees(&space_id, &doc.base.id, &protectees)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            doc_protectees_cache.insert(doc.base.id.to_string(), protectees.clone());
        }

        // Re-propagate onto derived entities.
        let entity_ids = self.collect_affected_entity_ids(&space_id, &docs).await?;
        let mut revision_doc_cache: HashMap<String, Option<RecordId>> = HashMap::new();
        let mut entities_updated: Vec<String> = Vec::new();
        let mut entities_skipped: Vec<Value> = Vec::new();

        for entity_id in &entity_ids {
            let Some(entity) = self
                .entities
                .get(&space_id, entity_id)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
            else {
                continue;
            };
            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &entity, "write:entity")
            {
                entities_skipped.push(json!({
                    "id": entity_id.to_string(),
                    "reason": format!("Access denied: {reason}"),
                }));
                continue;
            }

            let union = self
                .recompute_entity_protectees(
                    &space_id,
                    entity_id,
                    &mut doc_protectees_cache,
                    &mut revision_doc_cache,
                )
                .await?;

            let existing = angel_protectees_from_metadata(&entity.metadata);
            let key_present = entity.metadata.get(ANGEL_PROTECTEES_METADATA_KEY).is_some();
            // Skip no-ops; an empty union with the key still present must
            // clear the key (family-wide), so it is not a no-op.
            if union == existing && (!union.is_empty() || !key_present) {
                continue;
            }

            let next = if union.is_empty() {
                None
            } else {
                Some(union.as_slice())
            };
            self.entities
                .set_angel_protectees(&space_id, entity_id, next)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
            entities_updated.push(entity_id.to_string());
        }

        let response = ToolResponse::success(json!({
            "docs_updated": docs.len(),
            "doc_ids": docs.iter().map(|doc| doc.base.id.to_string()).collect::<Vec<_>>(),
            "angel_protectees": protectees,
            "entities_updated": entities_updated.len(),
            "entity_ids": entities_updated,
            "entities_skipped": entities_skipped,
        }));
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `batch_ingest_docs` tool.
pub struct BatchIngestDocsHandler {
    source_repo: SourceRepository,
    work_items: WorkItemRepository,
}

impl BatchIngestDocsHandler {
    /// Creates a new handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            source_repo: SourceRepository::new(storage.clone()),
            work_items: WorkItemRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for BatchIngestDocsHandler {
    fn name(&self) -> &'static str {
        "batch_ingest_docs"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Batch ingest multiple documents and return a job_id for progress tracking.\n\n",
            "Example:\n",
            r#"{
  "docs": [
    {
      "title": "Daily standup",
      "text": "Status: shipped schema fixes; next up: search_all."
    },
    {
      "title": "Incident report",
      "text": "Observed suspicious domain in logs; created Entity and linked Facts."
    }
  ]
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["docs"],
            "properties": {
                "docs": {
                    "type": "array",
                    "items": doc_payload_schema(),
                    "description": "Documents to ingest"
                }
            }
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(Value::Array(docs)) = params.get_mut("docs") {
            for doc in docs {
                let _ = normalize_doc_value(doc, ctx);
            }
        }
        let input: BatchIngestDocsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        let total = input.docs.len();

        for (index, doc) in input.docs.into_iter().enumerate() {
            if let Err(reason) = ctx
                .authz
                .authorize_record_write(&ctx.principal, &doc, "write:doc")
            {
                failed.push(BatchError {
                    index,
                    error: format!("Access denied: {reason}"),
                });
                continue;
            }
            let (source_candidate, previous_source) =
                match stage_source_for_manual_doc(ctx, &doc).await {
                    Ok(value) => value,
                    Err(e) => {
                        failed.push(BatchError {
                            index,
                            error: e.to_string(),
                        });
                        continue;
                    }
                };
            if let Err(reason) =
                authorize_persisted_manual_doc_access(ctx, &source_candidate, &doc).await
            {
                failed.push(BatchError {
                    index,
                    error: reason.to_string(),
                });
                continue;
            }

            let rollback_source = previous_source.clone();
            match Box::pin(persist_staged_manual_source_and_doc(
                ctx,
                &self.source_repo,
                source_candidate,
                previous_source,
                doc,
            ))
            .await
            {
                Ok((created_source, persisted_doc)) => {
                    if let Err(reason) = ctx.authz.authorize_record_read(
                        &ctx.principal,
                        &persisted_doc.doc,
                        "read:doc:metadata",
                    ) {
                        rollback_staged_source_and_doc(
                            ctx,
                            &created_source,
                            rollback_source.as_ref(),
                            &persisted_doc,
                        )
                        .await;
                        failed.push(BatchError {
                            index,
                            error: format!("Access denied: {reason}"),
                        });
                    } else {
                        if let Err(e) = enqueue_triage_source_work_item(
                            ctx.storage.clone(),
                            &created_source,
                            None,
                        )
                        .await
                        {
                            rollback_staged_source_and_doc(
                                ctx,
                                &created_source,
                                rollback_source.as_ref(),
                                &persisted_doc,
                            )
                            .await;
                            failed.push(BatchError {
                                index,
                                error: e.to_string(),
                            });
                            continue;
                        }
                        succeeded.push(redact_staged_doc(
                            &ctx.principal,
                            persisted_doc.doc,
                            &created_source.base.id,
                        ));
                    }
                }
                Err(e) => failed.push(BatchError {
                    index,
                    error: e.to_string(),
                }),
            };
        }

        let job_status = if failed.is_empty() {
            WorkItemStatus::Done
        } else {
            WorkItemStatus::Failed
        };

        let job_payload = json!({
            "kind": "batch_ingest_docs",
            "total": total,
            "succeeded": succeeded.len(),
            "failed": failed.len(),
            "errors": failed.iter().take(25).map(|e| e.error.clone()).collect::<Vec<_>>(),
        });

        let base = BaseFieldsBuilder::new(ctx.principal.space_id.clone())
            .for_table(TableName::WorkItem)
            .build();
        let job_id = base.id.to_string();
        let job = WorkItem {
            base,
            kind: "batch_ingest_docs".to_string(),
            payload: job_payload,
            status: job_status,
            priority: 0,
            idempotency_key: job_id.clone(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let _ = self
            .work_items
            .create(job)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let output = BatchIngestDocsOutput {
            job_id,
            succeeded,
            failed,
        };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
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
    use super::*;
    use crate::authz::AuthzService;
    use elephant_pipeline::scheduler::TRIAGE_SOURCE_KIND;
    use elephant_policy::Principal;
    use elephant_types::records::SourceLifecycleStatus;
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn pdf_with_catalog_value(value: &str) -> Vec<u8> {
        use std::fmt::Write;

        let stream = "BT /F1 12 Tf 72 720 Td (Hello Elephant) Tj ET";
        let objects = [
            format!("<< /Type /Catalog /Pages 2 0 R /X {value} >>"),
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_string(),
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_string(),
            format!("<< /Length {} >>\nstream\n{stream}\nendstream", stream.len()),
        ];
        let mut pdf = "%PDF-1.4\n".to_string();
        let mut offsets = Vec::new();
        for (index, object) in objects.iter().enumerate() {
            offsets.push(pdf.len());
            writeln!(pdf, "{} 0 obj\n{object}\nendobj", index + 1).unwrap();
        }
        let xref = pdf.len();
        writeln!(pdf, "xref\n0 6\n0000000000 65535 f ").unwrap();
        for offset in offsets {
            writeln!(pdf, "{offset:010} 00000 n ").unwrap();
        }
        writeln!(
            pdf,
            "trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF"
        )
        .unwrap();
        pdf.into_bytes()
    }

    #[test]
    fn test_pdf_text_extraction_preserves_valid_content() {
        let pdf = pdf_with_catalog_value("[]");
        let text = extract_text_by_source_type(&pdf, "application/pdf").unwrap();
        assert!(text.contains("Hello Elephant"), "extracted text: {text:?}");
    }

    #[test]
    fn test_pdf_text_extraction_survives_excessive_object_nesting() {
        // RUSTSEC-2026-0187: this small input used to abort the process through
        // unbounded recursion. The parser may reject or recover malformed
        // objects, but extraction must return without aborting the process.
        let nested = format!("{}{}", "[".repeat(10_380), "]".repeat(10_380));
        let pdf = pdf_with_catalog_value(&nested);
        let _result = extract_text_by_source_type(&pdf, "application/pdf");
    }

    #[test]
    fn test_search_docs_input_defaults() {
        let json = r#"{"query": "test"}"#;
        let input: SearchDocsInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.k, 10);
    }

    #[test]
    fn test_doc_filters_defaults() {
        let filters: DocFilters = serde_json::from_str("{}").unwrap();
        assert!(!filters.include_deleted);
        assert!(filters.collection.is_none());
    }

    #[test]
    fn test_rank_fusion_lifts_agreement_without_burying_semantic_hits() {
        fn doc(title: &str) -> Doc {
            Doc {
                base: BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build(),
                title: title.to_string(),
                text: String::new(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: format!("rk-{title}"),
                metadata: serde_json::json!({}),
            }
        }
        let lexical: Vec<Doc> = (1..=5).map(|i| doc(&format!("L{i}"))).collect();
        let s1 = doc("S1");
        let both = lexical[4].clone();
        let merged = merge_lexical_and_semantic_docs(lexical, vec![s1, both], 3);
        let titles: Vec<&str> = merged.iter().map(|d| d.title.as_str()).collect();
        // L5 is in both rankings; S1 ties L1 (both rank 1) and the tie keeps
        // lexical order; the old merge returned L1, L2, L3 and dropped S1.
        assert_eq!(titles, vec!["L5", "L1", "S1"]);
    }

    #[test]
    fn test_semantic_search_blends_lexical_docs_first() {
        fn doc(title: &str) -> Doc {
            Doc {
                base: BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build(),
                title: title.to_string(),
                text: String::new(),
                sidecar_text: None,
                source: None,
                media_attachments: Vec::new(),
                rendition_key: format!("rk-{title}"),
                metadata: serde_json::json!({}),
            }
        }

        let exact = doc("context_fact category project exact");
        let semantic = doc("semantic neighbor");
        let duplicate = exact.clone();

        let merged = merge_lexical_and_semantic_docs(
            vec![exact.clone()],
            vec![semantic.clone(), duplicate],
            4,
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].base.id, exact.base.id);
        assert_eq!(merged[1].base.id, semantic.base.id);
    }

    const DOC_SCHEMA: &str = "DEFINE TABLE doc SCHEMALESS;";
    const SOURCE_SCHEMA: &str = "DEFINE TABLE source SCHEMALESS;";
    const WORK_ITEM_SCHEMA: &str = "DEFINE TABLE work_item SCHEMALESS;";
    const DOC_REVISION_SCHEMA: &str = "DEFINE TABLE doc_revision SCHEMALESS;";
    const DOC_NODE_SCHEMA: &str = "DEFINE TABLE doc_node SCHEMALESS;";
    const DOC_CHUNK_SCHEMA: &str = "DEFINE TABLE doc_chunk SCHEMALESS;";
    const ASSERTION_SCHEMA: &str = "DEFINE TABLE assertion SCHEMALESS;";

    fn env_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn make_test_principal(space_id: &str) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    #[test]
    fn test_graph_ingest_source_without_assertions_is_pending_until_graph_ready() {
        let mut source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .created_by("tester")
                .build(),
            SourceType::Email,
            "hash".to_string(),
        );
        source.set_requested_ingest_mode(IngestMode::Graph);
        source.lifecycle_status = SourceLifecycleStatus::MetadataReady;

        assert!(graph_ingest_is_still_pending(Some(&source), 0));

        source.lifecycle_status = SourceLifecycleStatus::GraphReady;
        assert!(!graph_ingest_is_still_pending(Some(&source), 0));
    }

    async fn setup_test_storage() -> StorageClient {
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99500);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = elephant_storage::StorageConfig {
            database: format!("mcp_docs_ingest_test_{db_id}"),
            ..elephant_storage::StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(DOC_SCHEMA).await.expect("schema");
        storage.query(SOURCE_SCHEMA).await.expect("schema");
        storage.query(WORK_ITEM_SCHEMA).await.expect("schema");
        storage
    }

    async fn setup_test_extraction_storage() -> StorageClient {
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99600);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = elephant_storage::StorageConfig {
            database: format!("mcp_docs_extract_test_{db_id}"),
            ..elephant_storage::StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(DOC_SCHEMA).await.expect("schema");
        storage.query(SOURCE_SCHEMA).await.expect("schema");
        storage.query(WORK_ITEM_SCHEMA).await.expect("schema");
        storage.query(DOC_REVISION_SCHEMA).await.expect("schema");
        storage.query(DOC_NODE_SCHEMA).await.expect("schema");
        storage.query(DOC_CHUNK_SCHEMA).await.expect("schema");
        storage.query(ASSERTION_SCHEMA).await.expect("schema");
        storage
    }

    #[tokio::test]
    async fn test_ingest_doc_enqueues_triage_source_work_item() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocHandler::new(storage.clone());

        let params = serde_json::json!({
            "doc": {
                "title": "Test",
                "text": "Body"
            }
        });

        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        let source_id = doc.source.expect("source link").id().clone();
        let persisted = DocRepository::new(storage.clone())
            .get("test_space", &doc.base.id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().clone()),
            Some(source_id.clone())
        );

        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");

        assert!(item.is_some(), "triage_source work item should be enqueued");
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_enqueues_triage_source_work_item() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let params = serde_json::json!({
            "title": "Test",
            "text": "Body",
            "priority": 100
        });

        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        let source_id = doc.source.expect("source link").id().clone();
        let persisted = DocRepository::new(storage.clone())
            .get("test_space", &doc.base.id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().clone()),
            Some(source_id.clone())
        );

        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");

        let item = item.expect("triage_source work item should be enqueued");
        assert_eq!(item.priority, 100);
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_applies_extraction_guidance_metadata() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Example observation",
                    "text": "Tom asks about transforms in #ml-lab.",
                    "extraction_skill": "extraction/example-observations",
                    "extraction_purpose": "extract observational evidence",
                    "extraction_domain": "example_observation"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        let doc = decoded.data.expect("doc data").data;
        let persisted = DocRepository::new(storage)
            .get("test_space", &doc.base.id)
            .await
            .expect("load doc")
            .expect("persisted doc");

        assert_eq!(
            persisted
                .metadata
                .get("extraction_skill")
                .and_then(Value::as_str),
            Some("extraction/example-observations")
        );
        assert_eq!(
            persisted
                .metadata
                .get("extraction_purpose")
                .and_then(Value::as_str),
            Some("extract observational evidence")
        );
        assert_eq!(
            persisted
                .metadata
                .get("extraction_domain")
                .and_then(Value::as_str),
            Some("example_observation")
        );
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_accepts_media_without_text() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Example frame",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/jpeg",
                        "inline_base64": "AAEC"
                    }]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        let doc = decoded.data.expect("doc data").data;

        assert_eq!(doc.text, "");
        assert_eq!(doc.sidecar_text, None);
        assert_eq!(doc.media_attachments.len(), 1);
        assert!(matches!(
            &doc.media_attachments[0].location,
            DocMediaLocation::ManagedAsset { .. }
        ));
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_persists_inline_image_as_managed_asset() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let payload = BASE64_STANDARD.encode([0_u8, 1, 2, 3, 4, 5]);
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Inline frame",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/jpeg",
                        "inline_base64": payload
                    }]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        let doc = decoded.data.expect("doc data").data;

        assert_eq!(doc.media_attachments.len(), 1);
        let attachment = &doc.media_attachments[0];
        let asset_id = match &attachment.location {
            DocMediaLocation::ManagedAsset { asset_id } => asset_id,
            other => panic!("expected managed asset, got {other:?}"),
        };
        let asset_id = RecordId::from_str(asset_id).expect("managed asset id");
        let asset_repo = MediaAssetRepository::new(storage.clone());
        let stored_asset = asset_repo
            .get_with_payload("test_space", &asset_id)
            .await
            .expect("load media asset")
            .expect("stored media asset");
        assert_eq!(stored_asset.media_type, "image/jpeg");
        assert_eq!(stored_asset.size_bytes, 6);
        assert!(stored_asset.payload_base64.is_some());
        // The metadata read must not carry the payload.
        let metadata_view = asset_repo
            .get("test_space", &asset_id)
            .await
            .expect("load media asset metadata")
            .expect("stored media asset");
        assert!(metadata_view.payload_base64.is_none());
        assert_eq!(attachment.size_bytes, Some(6));
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_multi_image_creates_ordered_assets() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let frames: Vec<Vec<u8>> = (0..3_u8).map(|index| vec![index; 4]).collect();
        let media: Vec<Value> = frames
            .iter()
            .map(|bytes| {
                serde_json::json!({
                    "kind": "image",
                    "media_type": "image/jpeg",
                    "inline_base64": BASE64_STANDARD.encode(bytes)
                })
            })
            .collect();

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Activity segment",
                    "media": media
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        let doc = decoded.data.expect("doc data").data;

        assert_eq!(doc.media_attachments.len(), 3);
        let asset_repo = MediaAssetRepository::new(storage.clone());
        for (index, attachment) in doc.media_attachments.iter().enumerate() {
            let asset_id = match &attachment.location {
                DocMediaLocation::ManagedAsset { asset_id } => asset_id,
                other => panic!("expected managed asset, got {other:?}"),
            };
            let asset_id = RecordId::from_str(asset_id).expect("managed asset id");
            let stored_asset = asset_repo
                .get_with_payload("test_space", &asset_id)
                .await
                .expect("load media asset")
                .expect("stored media asset");
            // Payload identity proves the manifest order matches the input order.
            assert_eq!(
                stored_asset.payload_base64.as_deref(),
                Some(BASE64_STANDARD.encode(&frames[index]).as_str())
            );
        }
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_rejects_media_over_attachment_cap() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let media: Vec<Value> = (0..=MAX_DOC_MEDIA_ATTACHMENTS)
            .map(|_| {
                serde_json::json!({
                    "kind": "image",
                    "media_type": "image/jpeg",
                    "inline_base64": "AAEC"
                })
            })
            .collect();

        let error = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Too many frames",
                    "media": media
                }),
            )
            .await
            .expect_err("over-cap media should be rejected");

        assert!(error
            .to_string()
            .contains(&format!("at most {MAX_DOC_MEDIA_ATTACHMENTS} attachments")));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_ingest_doc_simple_rejects_inline_media_over_configured_limit() {
        let _guard = env_lock().lock().await;
        let prior_limit = std::env::var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES").ok();
        std::env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", "4");

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestDocSimpleHandler::new(ctx.storage.clone());

        let payload = BASE64_STANDARD.encode([0_u8, 1, 2, 3, 4, 5]);
        let error = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Too large frame",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/jpeg",
                        "inline_base64": payload
                    }]
                }),
            )
            .await
            .expect_err("inline media should be rejected");

        assert!(error
            .to_string()
            .contains("inline payload exceeds configured limit of 4 bytes"));

        match prior_limit {
            Some(value) => std::env::set_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES", value),
            None => std::env::remove_var("PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES"),
        }
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_persists_inline_video_as_managed_asset() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let payload = BASE64_STANDARD.encode(b"fake-mp4-payload");
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Inline clip",
                    "text": "rolling capture",
                    "media": [{
                        "kind": "video",
                        "media_type": "video/mp4",
                        "inline_base64": payload,
                        "duration_ms": 42000
                    }]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        let doc = decoded.data.expect("doc data").data;

        assert_eq!(doc.sidecar_text.as_deref(), Some("rolling capture"));
        assert_eq!(
            doc.metadata.get("content_type"),
            Some(&Value::String("video/mp4".to_string()))
        );
        let attachment = &doc.media_attachments[0];
        let asset_id = match &attachment.location {
            DocMediaLocation::ManagedAsset { asset_id } => asset_id,
            other => panic!("expected managed asset, got {other:?}"),
        };
        let asset_id = RecordId::from_str(asset_id).expect("managed asset id");
        let stored_asset = MediaAssetRepository::new(storage.clone())
            .get_with_payload("test_space", &asset_id)
            .await
            .expect("load media asset")
            .expect("stored media asset");
        assert_eq!(stored_asset.duration_ms, Some(42_000));
        assert!(stored_asset.payload_base64.is_some());
        assert_eq!(attachment.duration_ms, Some(42_000));
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_rejects_zero_duration_inline_video() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage);

        let err = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Inline clip",
                    "media": [{
                        "kind": "video",
                        "media_type": "video/mp4",
                        "inline_base64": BASE64_STANDARD.encode(b"fake-mp4-payload"),
                        "duration_ms": 0
                    }]
                }),
            )
            .await
            .expect_err("zero-duration inline video should be rejected");

        assert!(err
            .to_string()
            .contains("duration_ms must be a positive integer"));
    }

    #[tokio::test]
    async fn test_ingest_doc_simple_invalid_media_does_not_persist_prior_assets() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocSimpleHandler::new(storage.clone());

        let err = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "title": "Mixed media",
                    "media": [
                        {
                            "kind": "image",
                            "media_type": "image/jpeg",
                            "inline_base64": BASE64_STANDARD.encode([0_u8, 1, 2, 3])
                        },
                        {
                            "kind": "video",
                            "media_type": "video/mp4",
                            "inline_base64": BASE64_STANDARD.encode(b"fake-mp4-payload")
                        }
                    ]
                }),
            )
            .await
            .expect_err("missing duration on later media item should reject request");

        assert!(err.to_string().contains("duration_ms"));

        let assets = MediaAssetRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list media assets");
        assert!(
            assets.is_empty(),
            "failed MCP media ingest should not persist orphaned assets"
        );
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_path_media_attachments() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocHandler::new(storage.clone());

        let err = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc": {
                        "title": "Rendered frame",
                        "media_attachments": [{
                            "attachment_id": "att-1",
                            "kind": "image",
                            "media_type": "image/jpeg",
                            "location": {
                                "transport": "path",
                                "path": "/tmp/elephant-example-media/frames/example.jpg"
                            }
                        }]
                    }
                }),
            )
            .await
            .expect_err("path-backed media attachments should be rejected");

        assert!(err
            .to_string()
            .contains("must not use path or uri transports"));
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_uri_media_attachments() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocHandler::new(storage.clone());

        let err = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc": {
                        "title": "Rendered clip",
                        "sidecar_text": "Caller context for this clip",
                        "media_attachments": [{
                            "attachment_id": "att-1",
                            "kind": "video",
                            "media_type": "video/mp4",
                            "duration_ms": 42000,
                            "location": {
                                "transport": "uri",
                                "uri": "https://example.com/clip.mp4"
                            }
                        }]
                    }
                }),
            )
            .await
            .expect_err("uri-backed media attachments should be rejected");

        assert!(err
            .to_string()
            .contains("must not use path or uri transports"));
    }

    #[test]
    fn test_manual_source_hash_changes_when_media_or_sidecar_changes() {
        let mut left = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Evidence".to_string(),
            text: String::new(),
            sidecar_text: Some("Observed frame".to_string()),
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Path {
                    path: "/tmp/a.jpg".to_string(),
                },
                label: None,
                size_bytes: None,
                duration_ms: None,
                sha256: Some("a".repeat(64)),
            }],
            rendition_key: "rk-left".to_string(),
            metadata: serde_json::json!({}),
        };
        let mut right = left.clone();
        right.sidecar_text = Some("Observed different frame".to_string());
        assert_ne!(manual_source_hash(&left), manual_source_hash(&right));

        right = left.clone();
        right.media_attachments[0].sha256 = Some("b".repeat(64));
        assert_ne!(manual_source_hash(&left), manual_source_hash(&right));

        left.text = "canonical text".to_string();
        right = left.clone();
        right.text = "different canonical text".to_string();
        assert_ne!(manual_source_hash(&left), manual_source_hash(&right));
    }

    #[tokio::test]
    async fn test_ingest_doc_reuses_existing_source_for_same_doc_id() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocHandler::new(storage.clone());
        let doc_id = RecordId::new_v4(TableName::Doc);

        let first = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc": {
                        "base": {
                            "id": doc_id.to_string(),
                            "space_id": "test_space"
                        },
                        "title": "First",
                        "text": "Body"
                    }
                }),
            )
            .await
            .expect("first execute");
        let first_decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(first).expect("decode first");
        let first_doc = first_decoded.data.expect("first doc").data;
        let first_source_id = first_doc.source.expect("source link").id().to_string();

        let source_id = RecordId::from_str(&first_source_id).expect("parse source id");
        let mut existing_doc = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        existing_doc.base.security.level = 4;
        DocRepository::new(storage.clone())
            .update("test_space", &doc_id, existing_doc)
            .await
            .expect("update doc security");
        let mut existing_source = SourceRepository::new(storage.clone())
            .get("test_space", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        existing_source.base.security.level = 4;
        existing_source.lifecycle_status = SourceLifecycleStatus::GraphReady;
        existing_source.current_ingest_mode = Some(IngestMode::Graph);
        SourceRepository::new(storage.clone())
            .update("test_space", &source_id, existing_source)
            .await
            .expect("promote source state");

        let second = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc": {
                        "base": {
                            "id": doc_id.to_string(),
                            "space_id": "test_space"
                        },
                        "title": "Second",
                        "text": "Updated body"
                    }
                }),
            )
            .await
            .expect("second execute");
        let second_decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(second).expect("decode second");
        let second_doc = second_decoded.data.expect("second doc").data;

        assert_eq!(
            second_doc.source.expect("source link").id().to_string(),
            first_source_id
        );
        let persisted = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Second");
        assert_eq!(persisted.base.security.level, 0);

        let persisted_source = SourceRepository::new(storage.clone())
            .get("test_space", &source_id)
            .await
            .expect("reload source")
            .expect("persisted source");
        assert_eq!(persisted_source.base.security.level, 0);
        assert_eq!(
            persisted_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(persisted_source.current_ingest_mode, None);

        let sources = SourceRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_doc_restores_soft_deleted_doc() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDocHandler::new(storage.clone());
        let doc_id = RecordId::new_v4(TableName::Doc);

        let existing = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id.clone();
                base
            },
            title: "Existing".to_string(),
            text: "Deleted doc".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "existing-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        DocRepository::new(storage.clone())
            .create(existing)
            .await
            .expect("create doc");
        DocRepository::new(storage.clone())
            .delete("test_space", &doc_id)
            .await
            .expect("delete doc");

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc": {
                        "base": {
                            "id": doc_id.to_string(),
                            "space_id": "test_space"
                        },
                        "title": "Restored",
                        "text": "Body"
                    }
                }),
            )
            .await
            .expect("restore deleted doc");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode response");
        assert!(decoded.ok, "response should be ok");

        let restored = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("restored doc");
        assert_eq!(restored.title, "Restored");
        assert!(restored.base.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_batch_ingest_docs_restores_soft_deleted_doc() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = BatchIngestDocsHandler::new(storage.clone());
        let doc_id = RecordId::new_v4(TableName::Doc);

        let existing = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id.clone();
                base
            },
            title: "Existing".to_string(),
            text: "Deleted doc".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "existing-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        DocRepository::new(storage.clone())
            .create(existing)
            .await
            .expect("create conflicting doc");
        DocRepository::new(storage.clone())
            .delete("test_space", &doc_id)
            .await
            .expect("soft-delete conflicting doc");

        let response = handler
            .execute(
                &ctx,
                json!({
                    "docs": [{
                        "title": "Test",
                        "text": "Body",
                        "id": doc_id.to_string()
                    }]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<BatchIngestDocsOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");
        let output = decoded.data.expect("batch output");
        assert_eq!(output.failed.len(), 0);
        assert_eq!(output.succeeded.len(), 1);

        let restored = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("restored doc");
        assert!(restored.base.deleted_at.is_none());

        let sources = SourceRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_file_enqueues_triage_source_work_item() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestFileHandler::new(storage.clone());

        let tmp_path =
            std::env::temp_dir().join(format!("elephant-ingest-file-{}.md", Uuid::new_v4()));
        tokio::fs::write(&tmp_path, "# Hello\n\nAlice works at Acme.\n")
            .await
            .expect("write temp file");

        let params = serde_json::json!({
            "path": tmp_path.to_string_lossy().to_string(),
            "source_type": "text/markdown",
            "collection": "sample_context",
            "metadata": {
                "sample_kind": "source_event",
                "source_id": "email-message-1"
            },
            "extraction_skill": "extraction/custom-domain",
            "extraction_domain": "email",
            "extraction_purpose": "Extract source-backed communication facts.",
            "priority": 100
        });

        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        assert_eq!(
            doc.metadata.get("collection").and_then(Value::as_str),
            Some("sample_context")
        );
        assert_eq!(
            doc.metadata.get("sample_kind").and_then(Value::as_str),
            Some("source_event")
        );
        assert_eq!(
            doc.metadata.get("extraction_skill").and_then(Value::as_str),
            Some("extraction/custom-domain")
        );
        assert_eq!(
            doc.metadata
                .get("extraction_domain")
                .and_then(Value::as_str),
            Some("email")
        );

        let source_id = doc.source.as_ref().expect("linked source").id().to_string();
        let source_record_id = parse_id(&source_id, TableName::Source).expect("source id");
        let source = SourceRepository::new(storage.clone())
            .get("test_space", &source_record_id)
            .await
            .expect("load source")
            .expect("source");
        assert_eq!(source.requested_ingest_mode(), Some(IngestMode::Graph));

        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");

        assert!(item.is_some(), "triage_source work item should be enqueued");
        assert_eq!(item.expect("work item").priority, 100);
    }

    #[tokio::test]
    async fn test_ingest_file_image_creates_media_backed_doc() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestFileHandler::new(storage.clone());

        let tmp_path =
            std::env::temp_dir().join(format!("elephant-ingest-file-image-{}.jpg", Uuid::new_v4()));
        tokio::fs::write(&tmp_path, [0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00])
            .await
            .expect("write temp image");

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "path": tmp_path.to_string_lossy().to_string()
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        assert_eq!(doc.text, "");
        assert_eq!(doc.sidecar_text, None);
        assert_eq!(doc.media_attachments.len(), 1);
        assert!(matches!(
            &doc.media_attachments[0],
            DocMediaAttachment {
                kind: DocMediaKind::Image,
                media_type,
                location: DocMediaLocation::ManagedAsset { .. },
                ..
            } if media_type == "image/jpeg"
        ));

        let source_id = doc.source.as_ref().expect("linked source").id().to_string();
        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");
        assert!(item.is_some(), "triage_source work item should be enqueued");
    }

    #[tokio::test]
    async fn test_ingest_file_video_preserves_sidecar_text_for_media_doc() {
        let _guard = env_lock().lock().await;
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestFileHandler::new(storage.clone());

        let tmp_path =
            std::env::temp_dir().join(format!("elephant-ingest-file-video-{}.mp4", Uuid::new_v4()));
        tokio::fs::write(&tmp_path, b"fake mp4 payload")
            .await
            .expect("write temp video");

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "path": tmp_path.to_string_lossy().to_string(),
                    "text": "Caller-supplied context for the clip"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        let stored = DocRepository::new(storage)
            .get("test_space", &doc.base.id)
            .await
            .expect("load stored doc")
            .expect("stored doc");
        assert_eq!(stored.text, "");
        assert_eq!(
            stored.sidecar_text.as_deref(),
            Some("Caller-supplied context for the clip")
        );
        assert_eq!(stored.media_attachments.len(), 1);
        assert!(matches!(
            &stored.media_attachments[0],
            DocMediaAttachment {
                kind: DocMediaKind::Video,
                media_type,
                location: DocMediaLocation::ManagedAsset { .. },
                ..
            } if media_type == "video/mp4"
        ));
    }

    #[tokio::test]
    async fn test_ingest_file_distinguishes_same_bytes_different_paths() {
        let _guard = env_lock().lock().await;

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestFileHandler::new(storage.clone());

        let dir = std::env::temp_dir().join(format!("elephant-ingest-file-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        let first = dir.join("frame-a.jpg");
        let second = dir.join("frame-b.jpg");
        let bytes = [0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00];
        tokio::fs::write(&first, bytes).await.expect("write first");
        tokio::fs::write(&second, bytes)
            .await
            .expect("write second");

        let first_response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "path": first.to_string_lossy().to_string(),
                    "title": "Frame A"
                }),
            )
            .await
            .expect("ingest first file");
        let second_response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "path": second.to_string_lossy().to_string(),
                    "title": "Frame B"
                }),
            )
            .await
            .expect("ingest second file");

        let first_doc: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(first_response).expect("decode first");
        let second_doc: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(second_response).expect("decode second");

        let first_source = first_doc
            .data
            .expect("first data")
            .data
            .source
            .expect("first source");
        let second_source = second_doc
            .data
            .expect("second data")
            .data
            .source
            .expect("second source");

        assert_ne!(first_source.id(), second_source.id());
    }

    #[tokio::test]
    async fn test_ingest_directory_glob_filters_files() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestDirectoryHandler;

        let dir = std::env::temp_dir().join(format!("elephant-ingest-dir-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        let a = dir.join("a.md");
        let b = dir.join("b.txt");
        tokio::fs::write(&a, "# A\n\nAlice works at Acme.\n")
            .await
            .expect("write a");
        tokio::fs::write(&b, "This should not be ingested.\n")
            .await
            .expect("write b");

        let params = serde_json::json!({
            "path": dir.to_string_lossy().to_string(),
            "pattern": "*.md"
        });
        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<serde_json::Value> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let data = decoded.data.expect("data");
        let ingested = data
            .get("ingested")
            .and_then(|v| v.as_array())
            .expect("ingested array");
        let failed = data
            .get("failed")
            .and_then(|v| v.as_array())
            .expect("failed array");

        assert_eq!(failed.len(), 0, "no failures expected");
        assert_eq!(ingested.len(), 1, "only *.md should match");
    }

    #[tokio::test]
    async fn test_ingest_url_fetches_and_enqueues_triage_source() {
        let _guard = env_lock().lock().await;

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestUrlHandler::with_fetch_policy(
            storage.clone(),
            elephant_pipeline::remote_source::RemoteFetchPolicy {
                allow_private_networks: true,
                ..elephant_pipeline::remote_source::RemoteFetchPolicy::ingest()
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.expect("read request");
            let body = "Hello from ingest_url";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(resp.as_bytes()).await.expect("write");
        });

        let url = format!("http://{addr}/doc.txt");
        let params = serde_json::json!({ "url": url });
        let response = handler.execute(&ctx, params).await.expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        let source_id = doc.source.as_ref().expect("linked source").id().to_string();
        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{source_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");
        assert!(item.is_some(), "triage_source work item should be enqueued");
    }

    #[tokio::test]
    async fn test_ingest_url_image_creates_media_backed_doc() {
        let _guard = env_lock().lock().await;

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestUrlHandler::with_fetch_policy(
            storage.clone(),
            elephant_pipeline::remote_source::RemoteFetchPolicy {
                allow_private_networks: true,
                ..elephant_pipeline::remote_source::RemoteFetchPolicy::ingest()
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.expect("read request");
            let body = [0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00];
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            socket
                .write_all(resp.as_bytes())
                .await
                .expect("write headers");
            socket.write_all(&body).await.expect("write body");
        });

        let url = format!("http://{addr}/frame.jpg");
        let response = handler
            .execute(&ctx, serde_json::json!({ "url": url }))
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        assert_eq!(doc.text, "");
        assert_eq!(doc.media_attachments.len(), 1);
        assert!(matches!(
            &doc.media_attachments[0],
            DocMediaAttachment {
                kind: DocMediaKind::Image,
                media_type,
                location: DocMediaLocation::ManagedAsset { .. },
                ..
            } if media_type == "image/jpeg"
        ));
    }

    #[tokio::test]
    async fn test_ingest_url_video_preserves_sidecar_text_for_media_doc() {
        let _guard = env_lock().lock().await;

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestUrlHandler::with_fetch_policy(
            storage.clone(),
            elephant_pipeline::remote_source::RemoteFetchPolicy {
                allow_private_networks: true,
                ..elephant_pipeline::remote_source::RemoteFetchPolicy::ingest()
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.expect("read request");
            let body = b"fake mp4 payload";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            socket
                .write_all(resp.as_bytes())
                .await
                .expect("write headers");
            socket.write_all(body).await.expect("write body");
        });

        let url = format!("http://{addr}/segment.mp4");
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "url": url,
                    "text": "Rolling example recording segment"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");

        let doc = decoded.data.expect("doc data").data;
        let stored = DocRepository::new(storage)
            .get("test_space", &doc.base.id)
            .await
            .expect("load stored doc")
            .expect("stored doc");
        assert_eq!(stored.text, "");
        assert_eq!(
            stored.sidecar_text.as_deref(),
            Some("Rolling example recording segment")
        );
        assert_eq!(stored.media_attachments.len(), 1);
        assert!(matches!(
            &stored.media_attachments[0],
            DocMediaAttachment {
                kind: DocMediaKind::Video,
                media_type,
                location: DocMediaLocation::ManagedAsset { .. },
                ..
            } if media_type == "video/mp4"
        ));
    }

    #[tokio::test]
    async fn test_ingest_url_distinguishes_same_bytes_different_urls() {
        let _guard = env_lock().lock().await;

        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestUrlHandler::with_fetch_policy(
            storage.clone(),
            elephant_pipeline::remote_source::RemoteFetchPolicy {
                allow_private_networks: true,
                ..elephant_pipeline::remote_source::RemoteFetchPolicy::ingest()
            },
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = [0u8; 2048];
                let n = socket.read(&mut buf).await.expect("read request");
                let request = String::from_utf8_lossy(&buf[..n]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                let body = [0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00];
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nX-Path: {}\r\nContent-Length: {}\r\n\r\n",
                    path,
                    body.len()
                );
                socket
                    .write_all(resp.as_bytes())
                    .await
                    .expect("write headers");
                socket.write_all(&body).await.expect("write body");
            }
        });

        let first_response = handler
            .execute(
                &ctx,
                serde_json::json!({ "url": format!("http://{addr}/frame-a.jpg") }),
            )
            .await
            .expect("ingest first url");
        let second_response = handler
            .execute(
                &ctx,
                serde_json::json!({ "url": format!("http://{addr}/frame-b.jpg") }),
            )
            .await
            .expect("ingest second url");

        let first_doc: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(first_response).expect("decode first");
        let second_doc: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(second_response).expect("decode second");

        let first_source = first_doc
            .data
            .expect("first data")
            .data
            .source
            .expect("first source");
        let second_source = second_doc
            .data
            .expect("second data")
            .data
            .source
            .expect("second source");

        assert_ne!(first_source.id(), second_source.id());
    }

    #[tokio::test]
    async fn test_extract_doc_enqueues_plan_extraction_work_item() {
        let storage = setup_test_extraction_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);

        let ingest = IngestDocSimpleHandler::new(storage.clone());
        let ingest_params = serde_json::json!({
            "title": "Test",
            "text": "Alice works at Acme."
        });
        let ingest_response = ingest.execute(&ctx, ingest_params).await.expect("ingest");
        let ingest_decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(ingest_response).expect("decode ingest");
        let doc = ingest_decoded.data.expect("doc").data;

        let handler = ExtractDocHandler::new(storage.clone());
        let extract_params = serde_json::json!({ "id": doc.base.id.to_string() });
        let extract_response = handler
            .execute(&ctx, extract_params)
            .await
            .expect("extract");
        let extract_decoded: ToolResponse<serde_json::Value> =
            serde_json::from_value(extract_response).expect("decode extract");
        assert!(extract_decoded.ok, "extract response should be ok");

        let revision_id = extract_decoded
            .data
            .as_ref()
            .and_then(|value| value.get("revision_id"))
            .and_then(|value| value.as_str())
            .expect("revision_id");

        let repo = WorkItemRepository::new(storage);
        let idempotency_key = format!("{PLAN_EXTRACTION_KIND}:{revision_id}");
        let item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item");

        assert!(
            item.is_some(),
            "plan_extraction work item should be enqueued"
        );
        let item = item.expect("plan_extraction work item");
        assert_eq!(
            item.payload.get("extractor_id").and_then(Value::as_str),
            Some(AGENTIC_EXTRACTOR_ID),
            "manual extract_doc must enqueue the active extractor"
        );
    }

    #[tokio::test]
    async fn test_get_doc_pipeline_status_reports_pending_extraction() {
        let storage = setup_test_extraction_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);

        let ingest = IngestDocSimpleHandler::new(storage.clone());
        let ingest_params = serde_json::json!({
            "title": "Test",
            "text": "Alice works at Acme."
        });
        let ingest_response = ingest.execute(&ctx, ingest_params).await.expect("ingest");
        let ingest_decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(ingest_response).expect("decode ingest");
        let doc = ingest_decoded.data.expect("doc").data;

        let extract = ExtractDocHandler::new(storage.clone());
        let extract_params = serde_json::json!({ "id": doc.base.id.to_string() });
        let _ = extract
            .execute(&ctx, extract_params)
            .await
            .expect("extract");

        let status_handler = GetDocPipelineStatusHandler::new(storage.clone());
        let status_params = serde_json::json!({ "id": doc.base.id.to_string() });
        let status_response = status_handler
            .execute(&ctx, status_params)
            .await
            .expect("status");
        let decoded: ToolResponse<DocPipelineStatus> =
            serde_json::from_value(status_response).expect("decode status");
        assert!(decoded.ok, "status response should be ok");
        let status = decoded.data.expect("status");

        assert_eq!(status.doc_id.to_string(), doc.base.id.to_string());
        assert!(
            status.revision.is_some(),
            "should have a revision after extract_doc"
        );
        assert!(
            status.nodes_count > 0,
            "should have doc nodes after extract_doc"
        );
        assert_eq!(status.extraction_state, ExtractionState::Pending);
        assert!(
            status.extraction_guidance.is_some(),
            "status should include extraction guidance diagnostics"
        );
    }

    #[tokio::test]
    async fn test_get_doc_pipeline_status_reports_pending_triage_source() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);

        let ingest = IngestDocSimpleHandler::new(storage.clone());
        let ingest_params = serde_json::json!({
            "title": "Observed frame",
            "media": [{
                "kind": "image",
                "media_type": "image/jpeg",
                "inline_base64": BASE64_STANDARD.encode([0_u8, 1, 2, 3])
            }]
        });
        let ingest_response = ingest.execute(&ctx, ingest_params).await.expect("ingest");
        let ingest_decoded: ToolResponse<RedactionEnvelope<Doc>> =
            serde_json::from_value(ingest_response).expect("decode ingest");
        let doc = ingest_decoded.data.expect("doc").data;

        let status_handler = GetDocPipelineStatusHandler::new(storage);
        let status_params = serde_json::json!({ "id": doc.base.id.to_string() });
        let status_response = status_handler
            .execute(&ctx, status_params)
            .await
            .expect("status");
        let decoded: ToolResponse<DocPipelineStatus> =
            serde_json::from_value(status_response).expect("decode status");
        let status = decoded.data.expect("status");

        assert_eq!(status.extraction_state, ExtractionState::Pending);
    }

    #[tokio::test]
    async fn test_get_doc_pipeline_status_rejects_missing_doc() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);

        let status_handler = GetDocPipelineStatusHandler::new(ctx.storage.clone());
        let status_params =
            serde_json::json!({ "id": RecordId::new_v4(TableName::Doc).to_string() });
        let status_response = status_handler
            .execute(&ctx, status_params)
            .await
            .expect("status");
        let decoded: ToolResponse<DocPipelineStatus> =
            serde_json::from_value(status_response).expect("decode status");

        assert!(!decoded.ok);
        assert!(decoded
            .error
            .unwrap_or_default()
            .contains("Document not found"));
    }

    // --- restamp_attribution ---

    const ENTITY_SCHEMA: &str = "DEFINE TABLE entity SCHEMALESS;";

    async fn setup_restamp_storage() -> StorageClient {
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99700);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = elephant_storage::StorageConfig {
            database: format!("mcp_docs_restamp_test_{db_id}"),
            ..elephant_storage::StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage.query(DOC_SCHEMA).await.expect("schema");
        storage.query(SOURCE_SCHEMA).await.expect("schema");
        storage.query(DOC_REVISION_SCHEMA).await.expect("schema");
        storage.query(ASSERTION_SCHEMA).await.expect("schema");
        storage.query(ENTITY_SCHEMA).await.expect("schema");
        storage
    }

    async fn setup_restamp_embedded_storage() -> StorageClient {
        StorageClient::new_embedded().await.expect("storage")
    }

    #[tokio::test]
    async fn test_lexical_search_stops_paging_once_k_visible_docs_are_found() {
        // A common term must not walk every match: each extra page re-runs the
        // ranked query at a larger offset. 60 matches, k = 5: one page of 25.
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc_repo = DocRepository::new(storage.clone());
        for index in 0..60 {
            doc_repo
                .create(restamp_doc(&format!("rabbit {index}"), json!({})))
                .await
                .expect("create doc");
        }
        let ctx = restamp_ctx(&storage);

        let (docs, total) = collect_visible_text_search_results(
            &doc_repo,
            &ctx,
            "test_space",
            "rabbit",
            5,
            false,
            None,
        )
        .await
        .expect("lexical search");

        assert_eq!(docs.len(), 5);
        assert_eq!(total, 25, "counting stopped after the first page");

        let (docs, total) = collect_visible_text_search_results(
            &doc_repo,
            &ctx,
            "test_space",
            "rabbit",
            40,
            false,
            None,
        )
        .await
        .expect("lexical search with a larger k");
        assert_eq!(docs.len(), 40, "the page size follows k");
        assert_eq!(total, 40);

        let (docs, total) = collect_visible_text_search_results(
            &doc_repo,
            &ctx,
            "test_space",
            "rabbit",
            100,
            false,
            None,
        )
        .await
        .expect("lexical search past the last match");
        assert_eq!(docs.len(), 60, "every match when k exceeds them");
        assert_eq!(total, 60);
    }

    fn restamp_ctx(storage: &StorageClient) -> ToolContext {
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        ToolContext::with_authz(storage.clone(), authz, principal)
    }

    fn restamp_doc(title: &str, metadata: Value) -> Doc {
        Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .created_by("tester")
                .build(),
            title: title.to_string(),
            text: format!("{title} text"),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: format!("restamp-{title}-{}", Uuid::new_v4()),
            metadata,
        }
    }

    fn restamp_revision(doc_id: &RecordId) -> elephant_types::records::DocRevision {
        use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};

        elephant_types::records::DocRevision {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::DocRevision)
                .created_by("tester")
                .build(),
            doc: elephant_types::Link::new(doc_id.clone()),
            text: "revision text".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        }
    }

    fn restamp_entity(name: &str, metadata: Value) -> elephant_types::records::entity::Entity {
        use elephant_types::records::entity::EntityType;

        let mut entity = elephant_types::records::entity::Entity::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Entity)
                .created_by("tester")
                .build(),
            EntityType::new("person"),
            name.to_string(),
        );
        entity.metadata = metadata;
        entity
    }

    fn restamp_assertion(
        revision_id: &RecordId,
        subject: &str,
        subject_entity: &RecordId,
    ) -> elephant_types::records::assertion::Assertion {
        use elephant_types::records::assertion::{
            Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
        };

        let base = BaseFieldsBuilder::new("test_space")
            .for_table(TableName::Assertion)
            .created_by("tester")
            .build();
        let assertion_key = format!("assertion-{}", base.id);
        Assertion {
            base,
            subject_text: subject.to_string(),
            predicate_raw: "knows".to_string(),
            predicate_id: None,
            object_text: Some("someone".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: elephant_types::Link::new(revision_id.clone()),
                node: None,
                start: 0,
                end: 1,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: chrono::Utc::now(),
            asserted_at: chrono::Utc::now(),
            assertion_key,
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: Some(elephant_types::Link::new(subject_entity.clone())),
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: Some("person".to_string()),
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        }
    }

    /// Core restamp flow, shared across both storage backends:
    ///
    /// - session doc restamped to exactly the corrected list
    /// - unrelated doc untouched
    /// - entity also sourced from the unrelated doc keeps its union
    /// - entity sourced only from the session doc narrows to the correction
    async fn assert_restamp_by_session_id_flow(storage: StorageClient) {
        let ctx = restamp_ctx(&storage);
        let handler = RestampAttributionHandler::new(storage.clone());

        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        // Session doc originally attributed to both children.
        let session_doc = restamp_doc(
            "session-doc",
            serde_json::json!({
                "angel_session_id": "sess-1",
                "angel_protectees": ["child-a", "child-b"]
            }),
        );
        let session_doc_id = session_doc.base.id.clone();
        doc_repo.create(session_doc).await.expect("create doc");
        let session_revision = restamp_revision(&session_doc_id);
        let session_revision_id = session_revision.base.id.clone();
        revision_repo
            .create(session_revision)
            .await
            .expect("create revision");

        // Unrelated doc attributed to child-b only.
        let other_doc = restamp_doc(
            "other-doc",
            serde_json::json!({ "angel_protectees": ["child-b"] }),
        );
        let other_doc_id = other_doc.base.id.clone();
        doc_repo.create(other_doc).await.expect("create doc");
        let other_revision = restamp_revision(&other_doc_id);
        let other_revision_id = other_revision.base.id.clone();
        revision_repo
            .create(other_revision)
            .await
            .expect("create revision");

        // Alice traces to both docs; Bob only to the session doc.
        let alice = restamp_entity(
            "Alice Chen",
            serde_json::json!({ "angel_protectees": ["child-a", "child-b"] }),
        );
        let alice_id = alice.base.id.clone();
        entity_repo.create(alice).await.expect("create alice");
        let bob = restamp_entity(
            "Bob Larsson",
            serde_json::json!({ "angel_protectees": ["child-a", "child-b"] }),
        );
        let bob_id = bob.base.id.clone();
        entity_repo.create(bob).await.expect("create bob");

        for assertion in [
            restamp_assertion(&session_revision_id, "Alice Chen", &alice_id),
            restamp_assertion(&other_revision_id, "Alice Chen", &alice_id),
            restamp_assertion(&session_revision_id, "Bob Larsson", &bob_id),
        ] {
            assertion_repo
                .create(assertion)
                .await
                .expect("create assertion");
        }

        // Parent correction: the session belonged to child-a only.
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "session_id": "sess-1",
                    "angel_protectees": ["child-a"]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<Value> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok: {decoded:?}");
        let data = decoded.data.expect("data");
        assert_eq!(data["docs_updated"], serde_json::json!(1));
        assert_eq!(data["entities_updated"], serde_json::json!(1));

        // Session doc replaced exactly; unrelated doc untouched.
        let session_doc = doc_repo
            .get("test_space", &session_doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(
            session_doc.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
        let other_doc = doc_repo
            .get("test_space", &other_doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(
            other_doc.metadata["angel_protectees"],
            serde_json::json!(["child-b"])
        );

        // Alice keeps child-b through the unrelated doc; Bob narrows.
        let alice = entity_repo
            .get("test_space", &alice_id)
            .await
            .expect("get alice")
            .expect("alice exists");
        assert_eq!(
            alice.metadata["angel_protectees"],
            serde_json::json!(["child-a", "child-b"])
        );
        let bob = entity_repo
            .get("test_space", &bob_id)
            .await
            .expect("get bob")
            .expect("bob exists");
        assert_eq!(
            bob.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
    }

    #[tokio::test]
    async fn test_restamp_attribution_by_session_id() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_storage().await;
        assert_restamp_by_session_id_flow(storage).await;
    }

    #[tokio::test]
    async fn test_restamp_attribution_by_session_id_embedded() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_embedded_storage().await;
        assert_restamp_by_session_id_flow(storage).await;
    }

    #[tokio::test]
    async fn test_restamp_attribution_family_wide_clears_entity_attribution() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_storage().await;
        let ctx = restamp_ctx(&storage);
        let handler = RestampAttributionHandler::new(storage.clone());

        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());
        let entity_repo = EntityRepository::new(storage.clone());

        let doc = restamp_doc(
            "attributed-doc",
            serde_json::json!({ "angel_protectees": ["child-a"] }),
        );
        let doc_id = doc.base.id.clone();
        doc_repo.create(doc).await.expect("create doc");
        let revision = restamp_revision(&doc_id);
        let revision_id = revision.base.id.clone();
        revision_repo
            .create(revision)
            .await
            .expect("create revision");

        let entity = restamp_entity(
            "Cara Diaz",
            serde_json::json!({ "angel_protectees": ["child-a"] }),
        );
        let entity_id = entity.base.id.clone();
        entity_repo.create(entity).await.expect("create entity");
        assertion_repo
            .create(restamp_assertion(&revision_id, "Cara Diaz", &entity_id))
            .await
            .expect("create assertion");

        // Explicit family-wide correction via doc_ids.
        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "doc_ids": [doc_id.to_string()],
                    "angel_protectees": []
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<Value> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok: {decoded:?}");

        // Doc keeps an explicit empty array recording the correction.
        let doc = doc_repo
            .get("test_space", &doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(doc.metadata["angel_protectees"], serde_json::json!([]));

        // Entity drops the key entirely so it matches every attributed_to
        // filter again (family-wide), not none of them.
        let entity = entity_repo
            .get("test_space", &entity_id)
            .await
            .expect("get entity")
            .expect("entity exists");
        assert!(
            entity.metadata.get("angel_protectees").is_none(),
            "angel_protectees key should be removed, got: {}",
            entity.metadata
        );
    }

    #[tokio::test]
    async fn test_restamp_attribution_by_source_uri_prefix() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_storage().await;
        let ctx = restamp_ctx(&storage);
        let handler = RestampAttributionHandler::new(storage.clone());

        let doc_repo = DocRepository::new(storage.clone());
        let source_repo = SourceRepository::new(storage.clone());

        // Angel L1 sources carry angel://session/{id}/frame/{n} uris.
        let mut source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .created_by("tester")
                .build(),
            SourceType::Manual,
            "restamp-hash".to_string(),
        );
        source.uri = Some("angel://session/7/frame/1".to_string());
        let source_id = source.base.id.clone();
        source_repo.create(source).await.expect("create source");

        let mut doc = restamp_doc(
            "frame-doc",
            serde_json::json!({ "angel_protectees": ["child-a", "child-b"] }),
        );
        doc.source = Some(Link::new(source_id));
        let doc_id = doc.base.id.clone();
        doc_repo.create(doc).await.expect("create doc");

        // A doc outside the session prefix must stay untouched.
        let unrelated = restamp_doc(
            "unrelated-doc",
            serde_json::json!({ "angel_protectees": ["child-a"] }),
        );
        let unrelated_id = unrelated.base.id.clone();
        doc_repo.create(unrelated).await.expect("create doc");

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "source_uri_prefix": "angel://session/7/",
                    "angel_protectees": ["child-b"]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<Value> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok: {decoded:?}");
        let data = decoded.data.expect("data");
        assert_eq!(data["docs_updated"], serde_json::json!(1));

        let doc = doc_repo
            .get("test_space", &doc_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(
            doc.metadata["angel_protectees"],
            serde_json::json!(["child-b"])
        );
        let unrelated = doc_repo
            .get("test_space", &unrelated_id)
            .await
            .expect("get doc")
            .expect("doc exists");
        assert_eq!(
            unrelated.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
    }

    #[tokio::test]
    async fn test_restamp_attribution_requires_selector() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_storage().await;
        let ctx = restamp_ctx(&storage);
        let handler = RestampAttributionHandler::new(storage);

        let response = handler
            .execute(&ctx, serde_json::json!({ "angel_protectees": ["child-a"] }))
            .await
            .expect("execute");
        let decoded: ToolResponse<Value> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded
            .error
            .unwrap_or_default()
            .contains("at least one doc selector"));
    }

    #[tokio::test]
    async fn test_restamp_attribution_no_matches_is_a_successful_noop() {
        let _guard = env_lock().lock().await;
        let storage = setup_restamp_storage().await;
        let ctx = restamp_ctx(&storage);
        let handler = RestampAttributionHandler::new(storage);

        let response = handler
            .execute(
                &ctx,
                serde_json::json!({
                    "session_id": "sess-does-not-exist",
                    "angel_protectees": ["child-a"]
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<Value> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok: {decoded:?}");
        let data = decoded.data.expect("data");
        assert_eq!(data["docs_updated"], serde_json::json!(0));
        assert_eq!(data["entities_updated"], serde_json::json!(0));
    }
}
