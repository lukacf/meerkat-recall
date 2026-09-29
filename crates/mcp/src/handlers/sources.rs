//! Source MCP tool handlers.
//!
//! Implements:
//! - `search_sources`
//! - `get_source`
//! - `create_source`
//! - `ingest_source`
//! - `ingest_source_simple`
//! - `batch_ingest_sources`
//! - `update_source`
//! - `delete_source`
//! - `restore_source`

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use tracing::warn;
use uuid::Uuid;

use elephant_storage::{
    deterministic_source_id_for_ingest, find_existing_doc_for_doc, find_existing_source_for_doc,
    find_existing_source_for_source, parse_id, persist_manual_doc_for_source_detailed,
    preview_persisted_manual_doc_for_source, rollback_persisted_manual_doc,
    rollback_persisted_source, update_existing_source_for_reingest, MediaAssetRepository,
    PersistedManualDoc, Repository, SearchCriteria, SearchRepository, SourceRepository,
    StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::mcp_simple::{CreateSourceSimpleInput, ExtractionGuidanceInput};
use elephant_types::records::doc::{Doc, DocMediaInput};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::media_asset::{
    inline_media_max_bytes, managed_asset_relative_path, MAX_DOC_MEDIA_ATTACHMENTS,
};
use elephant_types::records::source::SourceType;
use elephant_types::records::{
    apply_manual_doc_to_source, prepare_source_for_reingest,
    prepare_source_for_reingest_preserving_acl_if_placeholder, validate_manual_ingest_mode,
    validate_simple_source_fields, DocMediaAttachment, DocMediaLocation, MediaAsset, Source,
    SourceLifecycleStatus,
};
use elephant_types::{BaseFieldsBuilder, RecordId, TypeError};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{merge_base_fields, ToolContext, ToolHandler};
use elephant_pipeline::scheduler::ingest_work_kinds::enqueue_triage_source_work_item;
#[cfg(test)]
use elephant_pipeline::scheduler::ingest_work_kinds::TRIAGE_SOURCE_KIND;

const SOURCE_SEARCH_SCOPE: &str = "read:source";
const MAX_VISIBLE_SOURCE_SEARCH_PAGES: usize = 10_000;

fn sha256_hex_prefixed(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn validate_simple_source_input(name: &str, uri: Option<&str>) -> McpResult<()> {
    validate_simple_source_fields(name, uri)
        .map_err(|e: TypeError| McpError::InvalidParameters(e.to_string()))
}

fn normalize_source_value(source_value: &mut Value, ctx: &ToolContext) -> McpResult<()> {
    let obj = source_value
        .as_object_mut()
        .ok_or_else(|| McpError::InvalidParameters("source must be an object".to_string()))?;
    merge_base_fields(obj, TableName::Source, ctx)?;
    Ok(())
}

fn normalize_optional_text(text: Option<String>) -> Option<String> {
    text.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn doc_media_input_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "media_type"],
        "properties": {
            "kind": {
                "type": "string",
                "enum": ["image", "video"]
            },
            "media_type": {
                "type": "string",
                "description": "MIME media type such as image/jpeg or video/mp4"
            },
            "inline_base64": {
                "type": "string",
                "description": "Inline base64-encoded media payload"
            },
            "label": {
                "type": "string",
                "description": "Optional human-readable attachment label"
            },
            "duration_ms": {
                "type": "integer",
                "description": "Required for inline video payloads; optional otherwise"
            }
        }
    })
}

fn decode_inline_media(media: &DocMediaInput) -> McpResult<Vec<u8>> {
    let payload = media.inline_base64.as_deref().ok_or_else(|| {
        McpError::InvalidParameters("inline media payload is missing inline_base64".to_string())
    })?;
    let encoded = payload
        .split_once(',')
        .filter(|(prefix, _)| prefix.contains(";base64"))
        .map_or(payload, |(_, data)| data)
        .trim();
    BASE64_STANDARD.decode(encoded).map_err(|err| {
        McpError::InvalidParameters(format!(
            "Failed to decode inline media payload for `{}`: {err}",
            media.media_type
        ))
    })
}

fn inferred_content_type(media: &[DocMediaInput]) -> String {
    media
        .first()
        .map_or_else(|| "text/plain".to_string(), |item| item.media_type.clone())
}

fn prepare_inline_media_asset(space_id: &str, media: &DocMediaInput) -> McpResult<MediaAsset> {
    let bytes = decode_inline_media(media)?;
    prepare_managed_media_asset_bytes(space_id, media, &bytes)
}

fn prepare_managed_media_asset_bytes(
    space_id: &str,
    media: &DocMediaInput,
    bytes: &[u8],
) -> McpResult<MediaAsset> {
    let max_bytes = usize::try_from(inline_media_max_bytes(media.kind)).map_err(|_| {
        McpError::ExecutionError(
            "Configured inline media size limit exceeds platform usize".to_string(),
        )
    })?;
    if bytes.len() > max_bytes {
        return Err(McpError::InvalidParameters(format!(
            "Inline {:?} payload exceeds the {} byte limit",
            media.kind, max_bytes
        )));
    }

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let sha256 = hex::encode(hasher.finalize());
    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let relative_path = managed_asset_relative_path(&asset_id);

    let asset = MediaAsset {
        base: {
            let mut base = BaseFieldsBuilder::new(space_id.to_string())
                .for_table(TableName::MediaAsset)
                .build();
            base.id = asset_id;
            base
        },
        kind: media.kind,
        media_type: media.media_type.clone(),
        storage_path: relative_path,
        size_bytes: bytes.len() as u64,
        sha256,
        payload_base64: Some(BASE64_STANDARD.encode(bytes)),
        duration_ms: media.duration_ms,
        label: None,
    };

    Ok(asset)
}

#[derive(Debug, Clone)]
struct PreparedMediaAttachments {
    attachments: Vec<DocMediaAttachment>,
    assets: Vec<MediaAsset>,
}

type PreparedIngestSourceParts = (Source, Option<Doc>, Vec<MediaAsset>, Option<IngestMode>);

type PreparedIngestSourceSimpleParts = (
    String,
    Option<String>,
    Option<String>,
    Option<Doc>,
    Vec<MediaAsset>,
    Option<IngestMode>,
    ExtractionGuidanceInput,
);

fn validate_public_media_attachments(attachments: &[DocMediaAttachment]) -> McpResult<()> {
    for (index, attachment) in attachments.iter().enumerate() {
        match &attachment.location {
            DocMediaLocation::Path { .. } | DocMediaLocation::Uri { .. } => {
                return Err(McpError::InvalidParameters(format!(
                    "media_attachments[{index}] must not use path or uri transports on public ingest surfaces"
                )));
            }
            DocMediaLocation::ManagedAsset { .. } => {}
        }
    }
    Ok(())
}

fn build_media_attachments(
    _storage: &StorageClient,
    space_id: &str,
    media: &[DocMediaInput],
) -> McpResult<PreparedMediaAttachments> {
    if media.len() > MAX_DOC_MEDIA_ATTACHMENTS {
        return Err(McpError::InvalidParameters(format!(
            "media accepts at most {MAX_DOC_MEDIA_ATTACHMENTS} attachments per document (got {})",
            media.len()
        )));
    }
    let mut attachments = Vec::with_capacity(media.len());
    let mut assets = Vec::with_capacity(media.len());
    for (index, item) in media.iter().enumerate() {
        if item.media_type.trim().is_empty() {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}].media_type cannot be empty"
            )));
        }
        if item.transport_count() != 1 {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}] must specify exactly one media transport"
            )));
        }
        if item.path.is_some() || item.uri.is_some() {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}] must use inline_base64; public ingest does not accept path or uri media transports"
            )));
        }
        if matches!(item.kind, elephant_types::records::doc::DocMediaKind::Video)
            && item.inline_base64.is_some()
        {
            match item.duration_ms {
                Some(duration_ms) if duration_ms > 0 => {}
                _ => {
                    return Err(McpError::InvalidParameters(format!(
                        "media[{index}].duration_ms must be a positive integer for inline video payloads"
                    )));
                }
            }
        }

        let attachment = if item.inline_base64.is_some() {
            let asset = prepare_inline_media_asset(space_id, item)?;
            let attachment = DocMediaAttachment {
                attachment_id: format!("attachment-{}", Uuid::new_v4()),
                kind: asset.kind,
                media_type: asset.media_type.clone(),
                location: DocMediaLocation::ManagedAsset {
                    asset_id: asset.base.id.to_string(),
                },
                label: None,
                size_bytes: Some(asset.size_bytes),
                duration_ms: asset.duration_ms,
                sha256: Some(asset.sha256.clone()),
            };
            assets.push(asset);
            attachment
        } else {
            return Err(McpError::InvalidParameters(format!(
                "media[{index}] is missing inline_base64 payload",
            )));
        };
        attachments.push(attachment);
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
            Err(err) => {
                rollback_media_assets_by_ids(storage, &asset.base.space_id, &persisted_ids).await;
                return Err(McpError::ExecutionError(err.to_string()));
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

fn source_hash_for_staged_doc(
    name: &str,
    uri: Option<&str>,
    source_type: SourceType,
    doc: Option<&Doc>,
    pending_assets: &[MediaAsset],
) -> String {
    if doc.is_none() {
        return sha256_hex_prefixed(uri.unwrap_or(name));
    }

    let media = if pending_assets.is_empty() {
        doc.into_iter()
            .flat_map(|doc| doc.media_attachments.iter())
            .map(|attachment| {
                serde_json::json!({
                    "kind": attachment.kind,
                    "media_type": attachment.media_type.clone(),
                    "duration_ms": attachment.duration_ms,
                    "size_bytes": attachment.size_bytes,
                    "sha256": attachment.sha256.clone(),
                    "location": match &attachment.location {
                        DocMediaLocation::ManagedAsset { asset_id } => asset_id.clone(),
                        DocMediaLocation::Path { path } => path.clone(),
                        DocMediaLocation::Uri { uri } => uri.clone(),
                    },
                })
            })
            .collect::<Vec<_>>()
    } else {
        pending_assets
            .iter()
            .map(|asset| {
                serde_json::json!({
                    "kind": asset.kind,
                    "media_type": asset.media_type.clone(),
                    "duration_ms": asset.duration_ms,
                    "size_bytes": asset.size_bytes,
                    "sha256": asset.sha256.clone(),
                })
            })
            .collect::<Vec<_>>()
    };
    let payload = serde_json::json!({
        "name": name,
        "uri": uri,
        "source_type": format!("{source_type:?}"),
        "doc": doc.map(|doc| serde_json::json!({
            "title": doc.title.clone(),
            "text": doc.text.clone(),
            "sidecar_text": doc.sidecar_text.clone(),
            "rendition_key": doc.rendition_key.clone(),
            "metadata": doc.metadata.clone(),
            "media": media,
        })),
    });
    sha256_hex_prefixed(&serde_json::to_string(&payload).unwrap_or_default())
}

fn apply_space_override(space_id: &str, source: &mut Source) {
    source.base.space_id = space_id.to_string();
}

fn apply_space_override_doc(space_id: &str, doc: &mut Doc) {
    doc.base.space_id = space_id.to_string();
}

fn source_ingest_output(
    source: &Source,
    work_item_id: &RecordId,
    doc_id: Option<String>,
) -> SourceIngestOutput {
    SourceIngestOutput {
        source_id: source.base.id.to_string(),
        lifecycle_status: source.lifecycle_status,
        current_ingest_mode: source.current_ingest_mode,
        work_item_id: Some(work_item_id.to_string()),
        doc_id,
    }
}

async fn stage_source_for_ingest(
    ctx: &ToolContext,
    source: Source,
    doc: Option<&Doc>,
) -> Result<(Source, Option<Source>), McpError> {
    let existing_source = if let Some(doc) = doc {
        find_existing_source_for_doc(&ctx.storage, doc).await?
    } else {
        find_existing_source_for_source(&ctx.storage, &source).await?
    };
    if let Some(existing_source) = existing_source {
        ctx.authz
            .authorize_record_write(&ctx.principal, &existing_source, "write:source")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
        let updated_source = if doc.is_some() {
            prepare_source_for_reingest(&existing_source, source)
        } else {
            prepare_source_for_reingest_preserving_acl_if_placeholder(&existing_source, source)
        };
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

async fn rollback_staged_source_and_doc(
    ctx: &ToolContext,
    source: &Source,
    previous_source: Option<&Source>,
    staged_doc: Option<&PersistedManualDoc>,
) {
    if let Some(staged_doc) = staged_doc {
        if let Err(err) = rollback_persisted_manual_doc(&ctx.storage, staged_doc).await {
            warn!(
                doc_id = %staged_doc.doc.base.id,
                source_id = %source.base.id,
                rollback_error = %err,
                "failed to roll back staged manual doc after ingest failure"
            );
        }
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

async fn authorize_persisted_source_doc_write(
    ctx: &ToolContext,
    source: &Source,
    doc: &Doc,
) -> McpResult<()> {
    let preview = preview_persisted_manual_doc_for_source(&ctx.storage, source, doc)
        .await
        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
    ctx.authz
        .authorize_record_write(&ctx.principal, &preview, "write:doc")
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))
}

#[allow(clippy::too_many_lines)]
async fn ingest_source_record(
    ctx: &ToolContext,
    repo: &SourceRepository,
    mut source: Source,
    mut doc: Option<Doc>,
    pending_assets: &[MediaAsset],
    ingest_mode: Option<IngestMode>,
) -> McpResult<SourceIngestOutput> {
    let space_id = ctx.principal.space_id.clone();
    if space_id.is_empty() {
        return Err(McpError::ExecutionError(
            "Access denied: missing space_id".to_string(),
        ));
    }

    apply_space_override(&space_id, &mut source);
    if doc.is_none() {
        source.base.id = deterministic_source_id_for_ingest(&source);
    }

    let doc_id = if let Some(doc) = doc.as_mut() {
        apply_space_override_doc(&space_id, doc);
        if source.base.space_id != doc.base.space_id {
            return Err(McpError::InvalidParameters(format!(
                "source/doc space mismatch: source is in `{}` but doc is in `{}`",
                source.base.space_id, doc.base.space_id
            )));
        }
        match find_existing_doc_for_doc(&ctx.storage, doc).await {
            Ok(Some(existing_doc)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &existing_doc, "write:doc")
                {
                    return Err(McpError::ExecutionError(format!("Access denied: {reason}")));
                }
            }
            Ok(None) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, doc, "write:doc")
                {
                    return Err(McpError::ExecutionError(format!("Access denied: {reason}")));
                }
            }
            Err(err) => return Err(McpError::ExecutionError(err.to_string())),
        }

        Some(
            apply_manual_doc_to_source(&mut source, doc)
                .map_err(|e| McpError::ExecutionError(e.to_string()))?,
        )
    } else {
        None
    };

    let requested_mode = ingest_mode.unwrap_or_else(|| {
        if doc_id.is_some() {
            IngestMode::Graph
        } else {
            IngestMode::Metadata
        }
    });
    validate_manual_ingest_mode(requested_mode, doc_id.is_some())
        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
    source.set_requested_ingest_mode(requested_mode);
    let (source_candidate, previous_source) =
        stage_source_for_ingest(ctx, source, doc.as_ref()).await?;
    if let Some(doc_ref) = doc.as_ref() {
        authorize_persisted_source_doc_write(ctx, &source_candidate, doc_ref).await?;
    }
    let rollback_source = previous_source.clone();
    let (created, staged_doc, rollback_source_override) = if let Some(previous_source) =
        previous_source
    {
        let updated_source =
            update_existing_source_for_reingest(repo, &previous_source, source_candidate)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        if let Some(doc) = doc {
            persist_prepared_media_assets(&ctx.storage, pending_assets).await?;
            match persist_manual_doc_for_source_detailed(&ctx.storage, &updated_source, doc).await {
                Ok(staged_doc) => (updated_source, Some(staged_doc), None),
                Err(err) => {
                    rollback_prepared_media_assets(&ctx.storage, pending_assets).await;
                    if let Err(rollback_err) = rollback_persisted_source(
                        &ctx.storage,
                        &updated_source,
                        Some(&previous_source),
                    )
                    .await
                    {
                        warn!(
                            source_id = %previous_source.base.id,
                            error = %err,
                            rollback_error = %rollback_err,
                            "failed to roll back staged source after manual doc persistence error"
                        );
                    }
                    return Err(McpError::ExecutionError(err.to_string()));
                }
            }
        } else {
            (updated_source, None, None)
        }
    } else {
        let (created, rollback_source_override) = match repo.create(source_candidate.clone()).await
        {
            Ok(created) => (created, None),
            Err(elephant_storage::StorageError::Conflict(_)) => {
                let existing = repo
                    .get_with_deleted(&source_candidate.base.space_id, &source_candidate.base.id)
                    .await
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?
                    .ok_or_else(|| {
                        McpError::ExecutionError(
                            "source ingest conflict without persisted source".to_string(),
                        )
                    })?;
                ctx.authz
                    .authorize_record_write(&ctx.principal, &existing, "write:source")
                    .map_err(|reason| {
                        McpError::ExecutionError(format!("Access denied: {reason}"))
                    })?;
                let updated_source = if doc.is_some() {
                    prepare_source_for_reingest(&existing, source_candidate.clone())
                } else {
                    prepare_source_for_reingest_preserving_acl_if_placeholder(
                        &existing,
                        source_candidate.clone(),
                    )
                };
                ctx.authz
                    .authorize_record_write(&ctx.principal, &updated_source, "write:source")
                    .map_err(|reason| {
                        McpError::ExecutionError(format!("Access denied: {reason}"))
                    })?;
                let persisted =
                    update_existing_source_for_reingest(repo, &existing, updated_source)
                        .await
                        .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                (persisted, Some(existing))
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };
        if let Some(doc) = doc {
            persist_prepared_media_assets(&ctx.storage, pending_assets).await?;
            match persist_manual_doc_for_source_detailed(&ctx.storage, &created, doc).await {
                Ok(staged_doc) => (created, Some(staged_doc), rollback_source_override),
                Err(err) => {
                    rollback_prepared_media_assets(&ctx.storage, pending_assets).await;
                    match rollback_source_override
                        .as_ref()
                        .map_or_else(
                            || rollback_persisted_source(&ctx.storage, &created, None),
                            |existing_source| {
                                rollback_persisted_source(
                                    &ctx.storage,
                                    &created,
                                    Some(existing_source),
                                )
                            },
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(rollback_err) => {
                            warn!(
                                source_id = %created.base.id,
                                error = %err,
                                rollback_error = %rollback_err,
                                "failed to roll back staged source after manual doc persistence error"
                            );
                        }
                    }
                    return Err(McpError::ExecutionError(err.to_string()));
                }
            }
        } else {
            (created, None, rollback_source_override)
        }
    };
    let rollback_source = rollback_source.or(rollback_source_override);
    let work_item_id_result =
        enqueue_triage_source_work_item(&ctx.storage, &created, "mcp.ingest_source", 5).await;
    let work_item_id = match work_item_id_result {
        Ok(work_item_id) => work_item_id,
        Err(e) => {
            rollback_prepared_media_assets(&ctx.storage, pending_assets).await;
            rollback_staged_source_and_doc(
                ctx,
                &created,
                rollback_source.as_ref(),
                staged_doc.as_ref(),
            )
            .await;
            return Err(McpError::ExecutionError(e.to_string()));
        }
    };
    Ok(source_ingest_output(&created, &work_item_id, doc_id))
}

async fn count_visible_source_search_results(
    repo: &SourceRepository,
    ctx: &ToolContext,
    space_id: &str,
    query: Option<&str>,
    include_deleted: bool,
) -> McpResult<usize> {
    let mut total_visible = 0usize;
    let mut scan_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_SOURCE_SEARCH_PAGES {
            return Err(McpError::ExecutionError(
                "Source search exceeded the maximum page scan bound".to_string(),
            ));
        }

        let mut criteria = SearchCriteria::new()
            .with_limit(1)
            .with_space_id(space_id.to_string());
        if let Some(query) = query {
            criteria = criteria.with_query(query.to_string());
        }
        if include_deleted {
            criteria = criteria.include_deleted();
        }
        if let Some(cursor) = scan_cursor {
            criteria = criteria.with_cursor(cursor);
        }

        let page = repo
            .search(&criteria)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let Some(source) = page.results.into_iter().next() else {
            break;
        };

        if !ctx
            .authz
            .filter_records(&ctx.principal, vec![source], SOURCE_SEARCH_SCOPE)
            .is_empty()
        {
            total_visible = total_visible.saturating_add(1);
        }

        if !page.has_more {
            break;
        }
        let Some(next_cursor) = page.cursor else {
            return Err(McpError::ExecutionError(
                "Source search pagination reported more results without a cursor".to_string(),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(McpError::ExecutionError(
                "Source search pagination cursor repeated without making progress".to_string(),
            ));
        }
        scan_cursor = Some(next_cursor);
    }

    Ok(total_visible)
}

async fn collect_visible_source_search_results(
    repo: &SourceRepository,
    ctx: &ToolContext,
    space_id: &str,
    query: Option<&str>,
    include_deleted: bool,
    limit: usize,
    start_cursor: Option<String>,
) -> McpResult<(Vec<Source>, usize, Option<String>, bool)> {
    let total_visible =
        count_visible_source_search_results(repo, ctx, space_id, query, include_deleted).await?;
    let mut visible = Vec::new();
    let mut scan_cursor = start_cursor;
    let mut next_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_SOURCE_SEARCH_PAGES {
            return Err(McpError::ExecutionError(
                "Source search exceeded the maximum page scan bound".to_string(),
            ));
        }

        let mut criteria = SearchCriteria::new()
            .with_limit(1)
            .with_space_id(space_id.to_string());
        if let Some(query) = query {
            criteria = criteria.with_query(query.to_string());
        }
        if include_deleted {
            criteria = criteria.include_deleted();
        }
        if let Some(cursor) = scan_cursor.clone() {
            criteria = criteria.with_cursor(cursor);
        }

        let page = repo
            .search(&criteria)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let page_cursor = scan_cursor.clone();
        let result_cursor = page.cursor.clone();
        let Some(source) = page.results.into_iter().next() else {
            break;
        };

        let allowed = ctx
            .authz
            .filter_records(&ctx.principal, vec![source], SOURCE_SEARCH_SCOPE);
        if let Some(source) = allowed.into_iter().next() {
            if visible.len() < limit {
                visible.push(source);
            } else if next_cursor.is_none() {
                next_cursor = page_cursor;
            }
        }

        if !page.has_more {
            break;
        }

        let Some(next_scan_cursor) = result_cursor else {
            return Err(McpError::ExecutionError(
                "Source search pagination reported more results without a cursor".to_string(),
            ));
        };
        if !seen_cursors.insert(next_scan_cursor.clone()) {
            return Err(McpError::ExecutionError(
                "Source search pagination cursor repeated without making progress".to_string(),
            ));
        }
        scan_cursor = Some(next_scan_cursor);
    }

    let has_more = next_cursor.is_some();
    Ok((visible, total_visible, next_cursor, has_more))
}

// ============================================================================
// Input/Output Types
// ============================================================================

/// Input for `search_sources` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSourcesInput {
    /// Free-text query.
    #[serde(default)]
    pub query: Option<String>,
    /// Filters.
    #[serde(default)]
    pub filters: Option<SourceFilters>,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Search filters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceFilters {
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted sources.
    #[serde(default)]
    pub include_deleted: bool,
}

const fn default_limit() -> usize {
    50
}

/// Output for `search_sources` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSourcesOutput {
    /// Search results.
    pub items: Vec<Source>,
    /// Next page cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count (post-filter).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

/// Input for `get_source` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSourceInput {
    /// Source ID.
    pub id: String,
}

/// Input for `create_source` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSourceInput {
    /// Source record to create.
    pub source: Source,
}

/// Input for `ingest_source` tool.
/// Public input for `ingest_source`, supporting normalized source-first fields
/// and a legacy nested `doc` compatibility shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestSourceInput {
    /// Preferred normalized source-first shape.
    Normalized(Box<NormalizedIngestSourceInput>),
    /// Backward-compatible nested `doc` shape.
    Legacy(Box<LegacyIngestSourceInput>),
}

/// Preferred normalized source-first input for `ingest_source`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedIngestSourceInput {
    /// Source record to create as the provenance-first intake object.
    pub source: Source,
    /// Optional staged document title.
    #[serde(default)]
    pub title: Option<String>,
    /// Optional staged document text or sidecar text for media docs.
    #[serde(default)]
    pub text: Option<String>,
    /// Optional image/video attachments for the staged document.
    #[serde(default)]
    pub media: Vec<DocMediaInput>,
    /// Optional collection hint for the staged document.
    #[serde(default)]
    pub collection: Option<String>,
    /// Optional staged document metadata.
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, Value>>,
    /// Optional staged document rendition key.
    #[serde(default)]
    pub rendition_key: Option<String>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
    /// Optional requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
}

/// Legacy compatibility input for `ingest_source`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyIngestSourceInput {
    /// Source record to create as the provenance-first intake object.
    pub source: Source,
    /// Optional manual document payload to stage on the source.
    #[serde(default)]
    pub doc: Option<Doc>,
    /// Optional requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
}

/// Input for `ingest_source_simple` tool.
/// Public input for `ingest_source_simple`, supporting normalized source-first
/// fields and a legacy nested `doc` compatibility shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestSourceSimpleInput {
    /// Preferred normalized source-first shape.
    Normalized(Box<NormalizedIngestSourceSimpleInput>),
    /// Backward-compatible nested `doc` shape.
    Legacy(Box<LegacyIngestSourceSimpleLegacyInput>),
}

/// Preferred normalized source-first input for `ingest_source_simple`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedIngestSourceSimpleInput {
    /// Human-readable source name.
    pub name: String,
    /// Optional URI.
    #[serde(default)]
    pub uri: Option<String>,
    /// Optional source type.
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional staged document title.
    #[serde(default)]
    pub title: Option<String>,
    /// Optional staged document text or sidecar text for media docs.
    #[serde(default)]
    pub text: Option<String>,
    /// Optional image/video attachments for the staged document.
    #[serde(default)]
    pub media: Vec<DocMediaInput>,
    /// Optional collection hint for the staged document.
    #[serde(default)]
    pub collection: Option<String>,
    /// Optional staged document metadata.
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, Value>>,
    /// Optional staged document rendition key.
    #[serde(default)]
    pub rendition_key: Option<String>,
    /// Optional requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Legacy compatibility input for `ingest_source_simple`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyIngestSourceSimpleLegacyInput {
    /// Human-readable source name.
    pub name: String,
    /// Optional URI.
    #[serde(default)]
    pub uri: Option<String>,
    /// Optional source type.
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional manual document payload to stage on the source.
    #[serde(default)]
    pub doc: Option<Doc>,
    /// Optional requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Input for `batch_ingest_sources` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIngestSourcesInput {
    /// Source ingest requests to stage and enqueue.
    pub items: Vec<IngestSourceInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct NormalizedStagedDocInput {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub media: Vec<DocMediaInput>,
    #[serde(default)]
    pub collection: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    pub rendition_key: Option<String>,
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Input for `update_source` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSourceInput {
    /// Source ID.
    pub id: String,
    /// Fields to update.
    pub fields: Value,
}

/// Input for delete/restore tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIdInput {
    /// Source ID.
    pub id: String,
}

/// Output for source-first ingest tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIngestOutput {
    /// Created source ID.
    pub source_id: String,
    /// Current source lifecycle status.
    pub lifecycle_status: SourceLifecycleStatus,
    /// Highest completed ingest mode.
    #[serde(default)]
    pub current_ingest_mode: Option<IngestMode>,
    /// Enqueued triage work item ID.
    #[serde(default)]
    pub work_item_id: Option<String>,
    /// Optional staged document ID when a manual doc payload was provided.
    #[serde(default)]
    pub doc_id: Option<String>,
}

/// Output for `batch_ingest_sources`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIngestSourcesOutput {
    /// Successfully staged source ingests.
    pub succeeded: Vec<SourceIngestOutput>,
    /// Failed inputs.
    pub failed: Vec<SourceBatchError>,
}

/// Batch ingest failure entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBatchError {
    /// Item index.
    pub index: usize,
    /// Error message.
    pub error: String,
}

impl NormalizedStagedDocInput {
    const fn has_doc_fields(&self) -> bool {
        self.title.is_some()
            || self.text.is_some()
            || !self.media.is_empty()
            || self.collection.is_some()
            || self.metadata.is_some()
            || self.rendition_key.is_some()
    }

    fn into_doc(self, ctx: &ToolContext) -> McpResult<Option<(Doc, Vec<MediaAsset>)>> {
        if !self.has_doc_fields() {
            return Ok(None);
        }

        let title = self.title.ok_or_else(|| {
            McpError::InvalidParameters(
                "staged source-first document requires `title` when document fields are provided"
                    .to_string(),
            )
        })?;
        let normalized_text = normalize_optional_text(self.text);
        let has_media = !self.media.is_empty();
        if !has_media && normalized_text.is_none() {
            return Err(McpError::InvalidParameters(
                "staged source-first document requires non-empty text or at least one media attachment"
                    .to_string(),
            ));
        }

        let space_id = ctx.principal.space_id.clone();
        let prepared_media = build_media_attachments(&ctx.storage, &space_id, &self.media)?;
        let mut metadata = self.metadata.unwrap_or_default();
        if let Some(collection) = self.collection {
            metadata.insert("collection".to_string(), Value::String(collection));
        }
        metadata.insert(
            "content_type".to_string(),
            Value::String(inferred_content_type(&self.media)),
        );
        self.extraction_guidance.apply_to_metadata(&mut metadata);

        Ok(Some((
            Doc {
                base: BaseFieldsBuilder::new(space_id)
                    .for_table(TableName::Doc)
                    .build(),
                title,
                text: if has_media {
                    String::new()
                } else {
                    normalized_text.clone().unwrap_or_default()
                },
                sidecar_text: has_media.then_some(normalized_text).flatten(),
                media_attachments: prepared_media.attachments,
                source: None,
                rendition_key: self
                    .rendition_key
                    .unwrap_or_else(|| format!("mcp-{}", Uuid::new_v4())),
                metadata: Value::Object(metadata),
            },
            prepared_media.assets,
        )))
    }
}

impl IngestSourceInput {
    fn into_parts(self, ctx: &ToolContext) -> McpResult<PreparedIngestSourceParts> {
        match self {
            Self::Normalized(input) => {
                let mut source = input.source;
                if let Some(metadata) = source.metadata.as_object_mut() {
                    input.extraction_guidance.apply_to_metadata(metadata);
                }
                let prepared_doc = NormalizedStagedDocInput {
                    title: input.title,
                    text: input.text,
                    media: input.media,
                    collection: input.collection,
                    metadata: input.metadata,
                    rendition_key: input.rendition_key,
                    extraction_guidance: input.extraction_guidance,
                }
                .into_doc(ctx)?;
                let (doc, pending_assets) = match prepared_doc {
                    Some((doc, assets)) => (Some(doc), assets),
                    None => (None, Vec::new()),
                };
                Ok((source, doc, pending_assets, input.ingest_mode))
            }
            Self::Legacy(input) => {
                if let Some(doc) = &input.doc {
                    validate_public_media_attachments(&doc.media_attachments)?;
                }
                Ok((input.source, input.doc, Vec::new(), input.ingest_mode))
            }
        }
    }
}

impl IngestSourceSimpleInput {
    fn into_parts(self, ctx: &ToolContext) -> McpResult<PreparedIngestSourceSimpleParts> {
        match self {
            Self::Normalized(input) => {
                let prepared_doc = NormalizedStagedDocInput {
                    title: input.title,
                    text: input.text,
                    media: input.media,
                    collection: input.collection,
                    metadata: input.metadata,
                    rendition_key: input.rendition_key,
                    extraction_guidance: input.extraction_guidance.clone(),
                }
                .into_doc(ctx)?;
                let (doc, pending_assets) = match prepared_doc {
                    Some((doc, assets)) => (Some(doc), assets),
                    None => (None, Vec::new()),
                };
                Ok((
                    input.name,
                    input.uri,
                    input.source_type,
                    doc,
                    pending_assets,
                    input.ingest_mode,
                    input.extraction_guidance,
                ))
            }
            Self::Legacy(input) => {
                if let Some(doc) = &input.doc {
                    validate_public_media_attachments(&doc.media_attachments)?;
                }
                Ok((
                    input.name,
                    input.uri,
                    input.source_type,
                    input.doc,
                    Vec::new(),
                    input.ingest_mode,
                    input.extraction_guidance,
                ))
            }
        }
    }
}

// ============================================================================
// Tool Handlers
// ============================================================================

/// Handler for `search_sources` tool.
pub struct SearchSourcesHandler {
    repo: SourceRepository,
}

impl SearchSourcesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for SearchSourcesHandler {
    fn name(&self) -> &'static str {
        "search_sources"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search for sources by query and filters.\n\n",
            "Example:\n",
            r#"{ "query": "feed", "filters": { "include_deleted": false }, "limit": 10 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Free-text search query"
                },
                "filters": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "space_id": { "type": "string" },
                        "include_deleted": { "type": "boolean", "default": false }
                    }
                },
                "limit": {
                    "type": "integer",
                    "default": 50,
                    "maximum": 100
                },
                "cursor": {
                    "type": "string",
                    "description": "Pagination cursor"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SearchSourcesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let query = input.query.clone();
        let start_cursor = input.cursor.clone();
        let include_deleted = input
            .filters
            .as_ref()
            .is_some_and(|filters| filters.include_deleted);
        let mut criteria = SearchCriteria::new().with_limit(input.limit);

        if let Some(q) = query.clone() {
            criteria = criteria.with_query(q);
        }

        if let Some(filters) = input.filters {
            if filters.include_deleted {
                criteria = criteria.include_deleted();
            }
            if let Some(space) = filters.space_id {
                if space != ctx.principal.space_id {
                    let response: ToolResponse<()> =
                        ToolResponse::error("Access denied: access to space denied");
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
            }
        }

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        criteria = criteria.with_space_id(space_id.clone());

        if let Some(cursor) = start_cursor.clone() {
            criteria = criteria.with_cursor(cursor);
        }

        let policy_id = ctx
            .principal
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
        let record = crate::authz::AuthzService::preflight_record(&space_id, &policy_id);
        if let Err(reason) = ctx.authz.evaluate(&ctx.principal, &record, "read:source") {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let page = self
            .repo
            .search(&criteria)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let page_len = page.results.len();
        let filtered = ctx
            .authz
            .filter_records(&ctx.principal, page.results, SOURCE_SEARCH_SCOPE);
        let total_visible = count_visible_source_search_results(
            &self.repo,
            ctx,
            &space_id,
            query.as_deref(),
            include_deleted,
        )
        .await?;

        let (items, total, next_cursor, has_more) = if filtered.len() == page_len {
            (filtered, total_visible, page.cursor, page.has_more)
        } else {
            collect_visible_source_search_results(
                &self.repo,
                ctx,
                &space_id,
                query.as_deref(),
                include_deleted,
                input.limit,
                start_cursor,
            )
            .await?
        };

        let output = SearchSourcesOutput {
            items,
            next_cursor,
            has_more,
            total: Some(total),
        };

        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_source` tool.
pub struct GetSourceHandler {
    repo: SourceRepository,
}

impl GetSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetSourceHandler {
    fn name(&self) -> &'static str {
        "get_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a single source by ID.\n\n",
            "Example:\n",
            r#"{ "id": "source:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Source ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetSourceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Source)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(source)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &source, "read:source")
                {
                    let response: ToolResponse<Source> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(source);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Source not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `create_source` tool.
pub struct CreateSourceHandler {
    repo: SourceRepository,
}

impl CreateSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for CreateSourceHandler {
    fn name(&self) -> &'static str {
        "create_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a new source record.\n\n",
            "Example:\n",
            r#"{ "source": { "external_id": "My Feed", "hash": "my-feed" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["source"],
            "properties": {
                "source": {
                    "type": "object",
                    "required": [],
                    "description": "Source record to create"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(source) = params.get_mut("source") {
            normalize_source_value(source, ctx)?;
        }
        let input: CreateSourceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &input.source, "write:source")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.create(input.source).await {
            Ok(source) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &source, "read:source")
                {
                    let response: ToolResponse<Source> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(source);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `create_source_simple` tool.
pub struct CreateSourceSimpleHandler {
    repo: SourceRepository,
}

/// Handler for `ingest_source` tool.
pub struct IngestSourceHandler {
    repo: SourceRepository,
}

impl IngestSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for IngestSourceHandler {
    fn name(&self) -> &'static str {
        "ingest_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a source first, optionally stage a manual document on it, and enqueue the source-first ingest lifecycle.\n\n",
            "Example:\n",
            r#"{
  "source": { "external_id": "gmail:abc123", "source_type": "email" },
  "title": "School update",
  "text": "The school fair is on Friday.",
  "ingest_mode": "graph"
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["source"],
            "properties": {
                "source": { "type": "object", "required": [] },
                "title": { "type": "string", "description": "Optional staged document title" },
                "text": { "type": "string", "description": "Optional staged document text or sidecar text for media docs" },
                "media": {
                    "type": "array",
                    "items": doc_media_input_schema(),
                    "description": "Optional staged image/video attachments"
                },
                "collection": { "type": "string", "description": "Optional collection hint for the staged document" },
                "metadata": { "type": "object", "required": [], "description": "Optional staged document metadata" },
                "rendition_key": { "type": "string", "description": "Optional staged document rendition key" },
                "extraction_skill": { "type": "string", "description": "Optional extraction skill override" },
                "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint" },
                "extraction_domain": { "type": "string", "description": "Optional extraction domain hint" },
                "ingest_mode": {
                    "type": "string",
                    "enum": ["skip", "metadata", "retrieval", "graph"]
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(source) = params.get_mut("source") {
            normalize_source_value(source, ctx)?;
        }
        let input: IngestSourceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let (source, doc, pending_assets, ingest_mode) = input.into_parts(ctx)?;

        let output = Box::pin(ingest_source_record(
            ctx,
            &self.repo,
            source,
            doc,
            &pending_assets,
            ingest_mode,
        ))
        .await?;
        serde_json::to_value(ToolResponse::success(output))
            .map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `ingest_source_simple` tool.
pub struct IngestSourceSimpleHandler {
    repo: SourceRepository,
}

impl IngestSourceSimpleHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for IngestSourceSimpleHandler {
    fn name(&self) -> &'static str {
        "ingest_source_simple"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a simple source (name/uri/type), optionally attach a manual doc, and enqueue source-first ingestion.\n\n",
            "Example:\n",
            r#"{
  "name": "Gmail school update",
  "source_type": "email",
  "title": "School update",
  "text": "The school fair is on Friday.",
  "ingest_mode": "graph"
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string" },
                "uri": { "type": "string" },
                "source_type": { "type": "string" },
                "title": { "type": "string", "description": "Optional staged document title" },
                "text": { "type": "string", "description": "Optional staged document text or sidecar text for media docs" },
                "media": {
                    "type": "array",
                    "items": doc_media_input_schema(),
                    "description": "Optional staged image/video attachments"
                },
                "collection": { "type": "string", "description": "Optional collection hint for the staged document" },
                "metadata": { "type": "object", "required": [], "description": "Optional staged document metadata" },
                "rendition_key": { "type": "string", "description": "Optional staged document rendition key" },
                "extraction_skill": { "type": "string", "description": "Optional extraction skill override (for example extraction/example-observations)" },
                "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint" },
                "extraction_domain": { "type": "string", "description": "Optional extraction domain hint" },
                "ingest_mode": {
                    "type": "string",
                    "enum": ["skip", "metadata", "retrieval", "graph"]
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: IngestSourceSimpleInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let (name, uri, source_type_hint, doc, pending_assets, ingest_mode, extraction_guidance) =
            input.into_parts(ctx)?;
        validate_simple_source_input(&name, uri.as_deref())?;

        let source_type = match source_type_hint.as_deref() {
            Some(t) => {
                SourceType::from_str(t).map_err(|e| McpError::InvalidParameters(e.to_string()))?
            }
            None => uri.as_deref().map_or(SourceType::Manual, |uri| {
                if uri.starts_with("http://") || uri.starts_with("https://") {
                    SourceType::Web
                } else {
                    SourceType::Manual
                }
            }),
        };

        let space_id = ctx.principal.space_id.clone();
        let mut source = Source::new(
            BaseFieldsBuilder::new(space_id)
                .for_table(TableName::Source)
                .build(),
            source_type,
            source_hash_for_staged_doc(
                &name,
                uri.as_deref(),
                source_type,
                doc.as_ref(),
                &pending_assets,
            ),
        );
        source.external_id = Some(name);
        source.uri = uri;
        if let Some(metadata) = source.metadata.as_object_mut() {
            extraction_guidance.apply_to_metadata(metadata);
        }

        let output = Box::pin(ingest_source_record(
            ctx,
            &self.repo,
            source,
            doc,
            &pending_assets,
            ingest_mode,
        ))
        .await?;
        serde_json::to_value(ToolResponse::success(output))
            .map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `batch_ingest_sources` tool.
pub struct BatchIngestSourcesHandler {
    repo: SourceRepository,
}

impl BatchIngestSourcesHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for BatchIngestSourcesHandler {
    fn name(&self) -> &'static str {
        "batch_ingest_sources"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create multiple sources and enqueue source-first ingestion for each.\n\n",
            "Example:\n",
            r#"{
  "items": [
    {
      "source": { "external_id": "gmail:abc123", "source_type": "email" },
      "title": "School update",
      "text": "The school fair is on Friday.",
      "ingest_mode": "graph"
    }
  ]
}"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["items"],
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["source"],
                        "properties": {
                            "source": { "type": "object", "required": [] },
                            "title": { "type": "string", "description": "Optional staged document title" },
                            "text": { "type": "string", "description": "Optional staged document text or sidecar text for media docs" },
                            "media": {
                                "type": "array",
                                "items": doc_media_input_schema(),
                                "description": "Optional staged image/video attachments"
                            },
                            "collection": { "type": "string", "description": "Optional collection hint for the staged document" },
                            "metadata": { "type": "object", "required": [], "description": "Optional staged document metadata" },
                            "rendition_key": { "type": "string", "description": "Optional staged document rendition key" },
                            "extraction_skill": { "type": "string", "description": "Optional extraction skill override" },
                            "extraction_purpose": { "type": "string", "description": "Optional extraction purpose hint" },
                            "extraction_domain": { "type": "string", "description": "Optional extraction domain hint" },
                            "ingest_mode": {
                                "type": "string",
                                "enum": ["skip", "metadata", "retrieval", "graph"]
                            }
                        }
                    }
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let mut params = coerce_params(params, &self.input_schema());
        if let Some(items) = params.get_mut("items").and_then(Value::as_array_mut) {
            for item in items {
                if let Some(source) = item.get_mut("source") {
                    normalize_source_value(source, ctx)?;
                }
            }
        }
        let input: BatchIngestSourcesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        for (index, item) in input.items.into_iter().enumerate() {
            let parts = item.into_parts(ctx);
            match parts {
                Ok((source, doc, pending_assets, ingest_mode)) => {
                    match Box::pin(ingest_source_record(
                        ctx,
                        &self.repo,
                        source,
                        doc,
                        &pending_assets,
                        ingest_mode,
                    ))
                    .await
                    {
                        Ok(output) => succeeded.push(output),
                        Err(err) => failed.push(SourceBatchError {
                            index,
                            error: err.to_string(),
                        }),
                    }
                }
                Err(err) => failed.push(SourceBatchError {
                    index,
                    error: err.to_string(),
                }),
            }
        }

        serde_json::to_value(ToolResponse::success(BatchIngestSourcesOutput {
            succeeded,
            failed,
        }))
        .map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

impl CreateSourceSimpleHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for CreateSourceSimpleHandler {
    fn name(&self) -> &'static str {
        "create_source_simple"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a source with only a name (and optional uri/type); hash and base fields are auto-generated.\n\n",
            "Example:\n",
            r#"{ "name": "My Feed", "uri": "https://example.com/feed.xml" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string", "description": "Human-readable name" },
                "uri": { "type": "string", "description": "Optional URI for the source" },
                "source_type": { "type": "string", "description": "Optional source type (e.g. web, email, manual)" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CreateSourceSimpleInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        validate_simple_source_input(&input.name, input.uri.as_deref())?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let source_type = match input.source_type.as_deref() {
            Some(t) => {
                SourceType::from_str(t).map_err(|e| McpError::InvalidParameters(e.to_string()))?
            }
            None => input.uri.as_deref().map_or(SourceType::Manual, |uri| {
                if uri.starts_with("http://") || uri.starts_with("https://") {
                    SourceType::Web
                } else {
                    SourceType::Manual
                }
            }),
        };

        let hash_input = input.uri.as_deref().unwrap_or(&input.name);
        let hash = sha256_hex_prefixed(hash_input);

        let base = BaseFieldsBuilder::new(space_id)
            .for_table(TableName::Source)
            .build();
        let mut source = Source::new(base, source_type, hash);
        source.external_id = Some(input.name);
        source.uri = input.uri;

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &source, "write:source")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.create(source).await {
            Ok(source) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &source, "read:source")
                {
                    let response: ToolResponse<Source> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(source);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `update_source` tool.
pub struct UpdateSourceHandler {
    repo: SourceRepository,
}

impl UpdateSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UpdateSourceHandler {
    fn name(&self) -> &'static str {
        "update_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Update a source's fields.\n\n",
            "Example:\n",
            r#"{ "id": "source:550e8400-e29b-41d4-a716-446655440000", "fields": { "uri": "https://example.com" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id", "fields"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Source ID"
                },
                "fields": {
                    "type": "object",
                    "required": [],
                    "description": "Fields to update"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: UpdateSourceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Source)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let existing = match self.repo.get(&space_id, &rid).await {
            Ok(Some(source)) => source,
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Source not found: {}", input.id));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &existing, "write:source")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut source_value =
            serde_json::to_value(&existing).map_err(|e| McpError::ExecutionError(e.to_string()))?;

        if let Value::Object(ref mut map) = source_value {
            if let Value::Object(fields) = input.fields {
                for (k, v) in fields {
                    map.insert(k, v);
                }
            }
        }

        let updated: Source = serde_json::from_value(source_value)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &updated, "write:source")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.update(&space_id, &rid, updated).await {
            Ok(source) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &source, "read:source")
                {
                    let response: ToolResponse<Source> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                let response = ToolResponse::success(source);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `delete_source` tool.
pub struct DeleteSourceHandler {
    repo: SourceRepository,
}

impl DeleteSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for DeleteSourceHandler {
    fn name(&self) -> &'static str {
        "delete_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Soft delete a source.\n\n",
            "Example:\n",
            r#"{ "id": "source:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Source ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SourceIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Source)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get(&space_id, &rid).await {
            Ok(Some(source)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &source, "write:source")
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
                    ToolResponse::error(format!("Source not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

/// Handler for `restore_source` tool.
pub struct RestoreSourceHandler {
    repo: SourceRepository,
}

impl RestoreSourceHandler {
    /// Creates a new handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SourceRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for RestoreSourceHandler {
    fn name(&self) -> &'static str {
        "restore_source"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Restore a soft-deleted source.\n\n",
            "Example:\n",
            r#"{ "id": "source:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Source ID"
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SourceIdInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let rid = parse_id(&input.id, TableName::Source)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        match self.repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(source)) => {
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_write(&ctx.principal, &source, "write:source")
                {
                    let response: ToolResponse<()> =
                        ToolResponse::error(format!("Access denied: {reason}"));
                    return serde_json::to_value(response)
                        .map_err(|e| McpError::ExecutionError(e.to_string()));
                }
                if let Err(reason) =
                    ctx.authz
                        .authorize_record_read(&ctx.principal, &source, "read:source")
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

                let response = ToolResponse::success(restored);
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Ok(None) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Source not found: {}", input.id));
                serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
            }
            Err(e) => Err(McpError::ExecutionError(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::mixed_attributes_style
    )]

    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use elephant_policy::Principal;
    use elephant_storage::{DocRepository, StorageConfig, WorkItemRepository};
    use elephant_types::records::{DocMediaKind, SourceLifecycleStatus, WorkItemStatus};

    use crate::authz::AuthzService;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_source_only_reingest_staging_preserves_placeholders_and_validates_explicit_acl() {
        use elephant_policy::Clearance;
        use elephant_types::records::base::SubjectsState;
        use elephant_types::Link;

        let storage = StorageClient::new_embedded().await.expect("storage");
        let mut principal = make_test_principal("test_space");
        principal.scopes.insert("write:source".into());
        principal.clearances.push(Clearance {
            policy_id: "default".into(),
            max_level: 5,
            labels: HashSet::from(["private".into()]),
            handling_allow: HashSet::new(),
        });
        let ctx =
            ToolContext::with_authz(storage.clone(), Arc::new(AuthzService::new()), principal);
        let mut source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            "source-only-acl".into(),
        );
        source.base.id = deterministic_source_id_for_ingest(&source);
        let incoming = source.clone();
        source.base.security.level = 5;
        source.base.security.labels = vec!["private".into()];
        source.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        source.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        source.base.subjects_state = SubjectsState::Complete;
        SourceRepository::new(storage)
            .create(source.clone())
            .await
            .expect("create");

        let (prepared, _) = stage_source_for_ingest(&ctx, incoming.clone(), None)
            .await
            .expect("stage placeholders");
        assert_eq!(prepared.base.security, source.base.security);
        assert_eq!(prepared.base.provenance, source.base.provenance);
        assert_eq!(prepared.base.subjects, source.base.subjects);
        assert_eq!(prepared.base.subjects_state, source.base.subjects_state);

        let mut explicit = incoming;
        explicit.base.security.level = 3;
        explicit.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        let (prepared, _) = stage_source_for_ingest(&ctx, explicit.clone(), None)
            .await
            .expect("stage explicit");
        assert_eq!(prepared.base.security, explicit.base.security);
        assert_eq!(prepared.base.provenance, explicit.base.provenance);
        explicit.base.security.level = 9;
        let result = stage_source_for_ingest(&ctx, explicit, None).await;
        assert!(
            result.is_err(),
            "unauthorized explicit ACL must be rejected, not silently ignored"
        );
    }

    #[test]
    fn test_search_sources_defaults() {
        let req: SearchSourcesInput = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(req.query.is_none());
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

    async fn setup_test_storage() -> StorageClient {
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(99700);
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);

        let config = StorageConfig {
            database: format!("mcp_sources_ingest_test_{db_id}"),
            ..StorageConfig::default()
        };
        let storage = StorageClient::new(config).await.expect("storage");
        storage
            .query("DEFINE TABLE source SCHEMALESS;")
            .await
            .expect("source schema");
        storage
            .query("DEFINE TABLE doc SCHEMALESS;")
            .await
            .expect("doc schema");
        storage
            .query("DEFINE TABLE work_item SCHEMALESS;")
            .await
            .expect("work item schema");
        storage
    }

    #[tokio::test]
    async fn test_ingest_source_enqueues_triage_source_work_item() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceHandler::new(storage.clone());

        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Source-first doc".to_string(),
            text: "Ontology completion".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "source-first-doc".to_string(),
            metadata: json!({"collection": "default"}),
        };

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("source-first-doc"),
        );

        let response = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");
        let output = decoded.data.expect("output");

        assert_eq!(output.lifecycle_status, SourceLifecycleStatus::Discovered);
        let persisted = DocRepository::new(storage.clone())
            .get("test_space", &doc.base.id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(output.source_id.clone())
        );
        let repo = WorkItemRepository::new(storage);
        let item = repo
            .find_by_idempotency_key(
                "test_space",
                &format!("{TRIAGE_SOURCE_KIND}:{}", output.source_id),
            )
            .await
            .expect("find work item");
        assert!(item.is_some(), "triage_source work item should be enqueued");
    }

    #[tokio::test]
    async fn test_ingest_source_accepts_normalized_staged_doc_shape() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceHandler::new(storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("normalized-source-first-doc"),
        );

        let response = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "title": "Source-first multimodal doc",
                    "text": "Caller note for staged doc",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/jpeg",
                        "inline_base64": "AAEC"
                    }],
                    "collection": "observations",
                    "extraction_skill": "extraction/example-observations",
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("execute");

        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        let output = decoded.data.expect("output");
        let doc_id = RecordId::from_str(&output.doc_id.expect("doc id")).expect("parse doc id");
        let persisted = DocRepository::new(storage)
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");

        assert_eq!(persisted.title, "Source-first multimodal doc");
        assert_eq!(
            persisted.sidecar_text.as_deref(),
            Some("Caller note for staged doc")
        );
        assert_eq!(persisted.text, "");
        assert_eq!(persisted.media_attachments.len(), 1);
        assert_eq!(
            persisted
                .metadata
                .get("extraction_skill")
                .and_then(Value::as_str),
            Some("extraction/example-observations")
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_mixed_legacy_and_normalized_doc_shapes() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestSourceHandler::new(ctx.storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("mixed-source-shape"),
        );
        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Legacy title".to_string(),
            text: "Legacy text".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "legacy-doc".to_string(),
            metadata: json!({}),
        };

        let err = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "title": "Normalized title"
                }),
            )
            .await
            .expect_err("mixed ingest_source shape should be rejected");
        assert!(
            err.to_string().contains("unknown field") || err.to_string().contains("did not match")
        );
    }

    #[tokio::test]
    async fn test_ingest_source_simple_enqueues_triage_source_work_item() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "https://example.com/feed",
                    "ingest_mode": "metadata"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "response should be ok");
        let output = decoded.data.expect("output");

        let repo = WorkItemRepository::new(storage);
        let item = repo
            .find_by_idempotency_key(
                "test_space",
                &format!("{TRIAGE_SOURCE_KIND}:{}", output.source_id),
            )
            .await
            .expect("find work item");
        assert!(item.is_some(), "triage_source work item should be enqueued");
    }

    #[tokio::test]
    async fn test_ingest_source_simple_applies_extraction_guidance_metadata() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "source_type": "device_frame",
                    "ingest_mode": "metadata",
                    "extraction_skill": "extraction/example-observations",
                    "extraction_purpose": "extract observational evidence",
                    "extraction_domain": "example_observation"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        let output = decoded.data.expect("output");

        let source_id = RecordId::from_str(&output.source_id).expect("parse source id");
        let persisted = SourceRepository::new(storage)
            .get("test_space", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");

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
    async fn test_ingest_source_simple_updates_extraction_guidance_on_reuse() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let first = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "uri": "example://session/123/frame/1",
                    "source_type": "device_frame",
                    "ingest_mode": "metadata",
                    "extraction_skill": "extraction/example-observations-v1"
                }),
            )
            .await
            .expect("first execute");
        let first_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(first).expect("decode first");
        let first_output = first_decoded.data.expect("first output");

        let repo = WorkItemRepository::new(storage.clone());
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{}", first_output.source_id);
        let work_item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item")
            .expect("persisted work item");
        repo.update_status("test_space", &work_item.base.id, WorkItemStatus::Done)
            .await
            .expect("mark done");

        let second = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "uri": "example://session/123/frame/1",
                    "source_type": "device_frame",
                    "ingest_mode": "metadata",
                    "extraction_skill": "extraction/example-observations-v2",
                    "extraction_purpose": "extract richer observation graph"
                }),
            )
            .await
            .expect("second execute");
        let second_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(second).expect("decode second");
        let second_output = second_decoded.data.expect("second output");

        assert_eq!(first_output.source_id, second_output.source_id);
        assert_eq!(first_output.work_item_id, second_output.work_item_id);

        let source_id = RecordId::from_str(&second_output.source_id).expect("parse source id");
        let persisted = SourceRepository::new(storage.clone())
            .get("test_space", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        assert_eq!(
            persisted
                .metadata
                .get("extraction_skill")
                .and_then(Value::as_str),
            Some("extraction/example-observations-v2")
        );
        assert_eq!(
            persisted
                .metadata
                .get("extraction_purpose")
                .and_then(Value::as_str),
            Some("extract richer observation graph")
        );

        let reopened = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("reload work item")
            .expect("reopened work item");
        assert_eq!(reopened.status, WorkItemStatus::Runnable);
    }

    #[tokio::test]
    async fn test_ingest_source_simple_accepts_normalized_staged_doc_shape() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "source_type": "device_frame",
                    "title": "Observed frame",
                    "text": "rolling capture",
                    "media": [{
                        "kind": "video",
                        "media_type": "video/mp4",
                        "inline_base64": "AAEC",
                        "duration_ms": 10_000
                    }],
                    "extraction_skill": "extraction/example-observations",
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        let output = decoded.data.expect("output");
        let doc_id = RecordId::from_str(&output.doc_id.expect("doc id")).expect("parse doc id");
        let persisted = DocRepository::new(storage)
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");

        assert_eq!(persisted.title, "Observed frame");
        assert_eq!(persisted.sidecar_text.as_deref(), Some("rolling capture"));
        assert_eq!(persisted.media_attachments.len(), 1);
        assert_eq!(
            persisted
                .metadata
                .get("extraction_skill")
                .and_then(Value::as_str),
            Some("extraction/example-observations")
        );
    }

    #[tokio::test]
    async fn test_ingest_source_simple_accepts_browser_style_data_url_inline_media() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let payload = format!(
            "data:image/png;base64,{}",
            BASE64_STANDARD.encode([0_u8, 1, 2, 3])
        );

        let response = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "source_type": "device_frame",
                    "title": "Observed frame",
                    "text": "rolling capture",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/png",
                        "inline_base64": payload
                    }],
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("execute");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode");
        let output = decoded.data.expect("output");
        let doc_id = RecordId::from_str(&output.doc_id.expect("doc id")).expect("parse doc id");
        let persisted = DocRepository::new(storage)
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");

        assert_eq!(persisted.title, "Observed frame");
        assert_eq!(persisted.media_attachments.len(), 1);
        assert_eq!(
            persisted.metadata.get("content_type"),
            Some(&Value::String("image/png".to_string()))
        );
    }

    #[tokio::test]
    async fn test_ingest_source_simple_invalid_media_does_not_persist_prior_assets() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let err = handler
            .execute(
                &ctx,
                json!({
                    "name": "Observed frame",
                    "title": "Observed frame",
                    "media": [
                        {
                            "kind": "image",
                            "media_type": "image/png",
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

        assert!(
            err.to_string().contains("duration_ms")
                || err.to_string().contains("positive integer")
                || err.to_string().contains("media["),
            "unexpected validation error: {err}"
        );

        let assets = MediaAssetRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list media assets");
        assert!(
            assets.is_empty(),
            "failed MCP source ingest should not persist orphaned assets"
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_uri_media_transport() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceHandler::new(storage);

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("reject-uri"),
        );

        let err = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "title": "Source-first multimodal doc",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/jpeg",
                        "uri": "https://example.com/frame.jpg"
                    }],
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect_err("uri transport should be rejected");

        assert!(err.to_string().contains("must use inline_base64"));
    }

    #[tokio::test]
    async fn test_ingest_source_legacy_rejects_path_or_uri_media_attachments() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestSourceHandler::new(ctx.storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("legacy-source"),
        );
        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Unsafe legacy source".to_string(),
            text: String::new(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Uri {
                    uri: "https://example.com/frame.jpg".to_string(),
                },
                label: None,
                size_bytes: None,
                duration_ms: None,
                sha256: None,
            }],
            rendition_key: "legacy-unsafe".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect_err("legacy path/uri media attachments should be rejected");

        assert!(err.to_string().contains("must not use path or uri"));
    }

    #[tokio::test]
    async fn test_ingest_source_simple_legacy_rejects_path_or_uri_media_attachments() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestSourceSimpleHandler::new(ctx.storage.clone());

        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Unsafe legacy source simple".to_string(),
            text: String::new(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Video,
                media_type: "video/mp4".to_string(),
                location: DocMediaLocation::Path {
                    path: "/tmp/clip.mp4".to_string(),
                },
                label: None,
                size_bytes: None,
                duration_ms: Some(1000),
                sha256: None,
            }],
            rendition_key: "legacy-simple-unsafe".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = handler
            .execute(
                &ctx,
                json!({
                    "name": "legacy source simple",
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect_err("legacy path/uri media attachments should be rejected");

        assert!(err.to_string().contains("must not use path or uri"));
    }

    #[tokio::test]
    async fn test_ingest_source_simple_rejects_inline_video_without_duration() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage);

        let err = handler
            .execute(
                &ctx,
                json!({
                    "name": "Example observation stream",
                    "source_type": "device_frame",
                    "title": "Observed clip",
                    "media": [{
                        "kind": "video",
                        "media_type": "video/mp4",
                        "inline_base64": "AAEC"
                    }],
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect_err("inline video without duration should be rejected");

        assert!(err
            .to_string()
            .contains("duration_ms must be a positive integer"));
    }

    #[tokio::test]
    async fn test_ingest_source_simple_rejects_mixed_legacy_and_normalized_doc_shapes() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestSourceSimpleHandler::new(ctx.storage.clone());

        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Legacy title".to_string(),
            text: "Legacy text".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "legacy-doc".to_string(),
            metadata: json!({}),
        };

        let err = handler
            .execute(
                &ctx,
                json!({
                    "name": "Mixed source",
                    "doc": doc,
                    "title": "Normalized title"
                }),
            )
            .await
            .expect_err("mixed ingest_source_simple shape should be rejected");
        assert!(
            err.to_string().contains("unknown field") || err.to_string().contains("did not match")
        );
    }

    #[tokio::test]
    async fn test_ingest_source_simple_rejects_invalid_name_or_uri() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage);

        let blank_uri = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "   "
                }),
            )
            .await
            .expect_err("blank uri should be rejected");
        assert!(blank_uri.to_string().contains("uri cannot be empty"));

        let oversized_name = handler
            .execute(
                &ctx,
                json!({
                    "name": "x".repeat(501)
                }),
            )
            .await
            .expect_err("oversized name should be rejected");
        assert!(oversized_name
            .to_string()
            .contains("display_name cannot exceed 500"));
    }

    #[tokio::test]
    async fn test_ingest_source_simple_reuses_existing_source_and_reopens_triage() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage.clone());

        let first = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "https://example.com/feed",
                    "ingest_mode": "metadata"
                }),
            )
            .await
            .expect("first execute");
        let first_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(first).expect("decode first");
        let first_output = first_decoded.data.expect("first output");
        let repo = WorkItemRepository::new(storage.clone());
        let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{}", first_output.source_id);
        let work_item = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("find work item")
            .expect("persisted work item");
        repo.update_status("test_space", &work_item.base.id, WorkItemStatus::Done)
            .await
            .expect("mark done");

        let second = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "https://example.com/feed",
                    "ingest_mode": "metadata"
                }),
            )
            .await
            .expect("second execute");
        let second_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(second).expect("decode second");
        let second_output = second_decoded.data.expect("second output");

        assert_eq!(first_output.source_id, second_output.source_id);
        assert_eq!(first_output.work_item_id, second_output.work_item_id);

        let reopened = repo
            .find_by_idempotency_key("test_space", &idempotency_key)
            .await
            .expect("reload work item")
            .expect("reopened work item");
        assert_eq!(reopened.status, WorkItemStatus::Runnable);

        let sources = SourceRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_source_principal_space_override_allows_mismatched_payload_spaces() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = IngestSourceHandler::new(ctx.storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("source-space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc = Doc {
            base: BaseFieldsBuilder::new("doc-space")
                .for_table(TableName::Doc)
                .build(),
            title: "Doc".to_string(),
            text: "Body".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let result = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("principal space should win over payload placeholder spaces");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(result).expect("decode ingest result");
        let output = decoded.data.expect("ingest output");
        assert_eq!(output.lifecycle_status, SourceLifecycleStatus::Discovered);
        assert!(output.doc_id.is_some());
    }

    #[tokio::test]
    async fn test_ingest_source_simple_rejects_graph_without_doc() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceSimpleHandler::new(storage);

        let err = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "https://example.com/feed",
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect_err("graph ingest without doc should be rejected");
        assert!(
            err.to_string().contains("requires a staged doc"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_ingest_source_restores_soft_deleted_doc() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceHandler::new(storage.clone());
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

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("source-first-doc-conflict"),
        );
        let doc = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id;
                base
            },
            title: "Source first".to_string(),
            text: "Ontology-first ingestion".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "source-first-rk".to_string(),
            metadata: serde_json::json!({"collection": "default"}),
        };

        let response = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("ingest source should restore deleted doc");
        let decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(response).expect("decode response");
        assert!(decoded.ok, "response should be ok");
        let output = decoded.data.expect("source ingest output");
        let restored_id =
            RecordId::from_str(&output.doc_id.expect("doc id")).expect("parse doc id");
        let restored = DocRepository::new(storage.clone())
            .get("test_space", &restored_id)
            .await
            .expect("load doc")
            .expect("restored doc");
        assert_eq!(restored.title, "Source first");
        assert!(restored.base.deleted_at.is_none());

        let sources = SourceRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_ingest_source_reuses_existing_source_and_resets_lifecycle() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal.clone());
        let handler = IngestSourceHandler::new(storage.clone());
        let doc_id = RecordId::new_v4(TableName::Doc);

        let first_source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            "hash-1".to_string(),
        );
        let mut first_doc = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id.clone();
                base
            },
            title: "Doc".to_string(),
            text: "First".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        first_doc.base.security.level = 4;

        let first = handler
            .execute(
                &ctx,
                json!({
                    "source": first_source,
                    "doc": first_doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("first ingest");
        let first_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(first).expect("decode first");
        let first_output = first_decoded.data.expect("first output");
        let source_id = RecordId::from_str(&first_output.source_id).expect("parse source id");

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

        let second_source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            "hash-2".to_string(),
        );
        let mut second_doc = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id.clone();
                base
            },
            title: "Doc".to_string(),
            text: "Second".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"revision": 2}),
        };
        second_doc.base.security.level = 7;

        let second = handler
            .execute(
                &ctx,
                json!({
                    "source": second_source,
                    "doc": second_doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("second ingest");
        let second_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(second).expect("decode second");
        let second_output = second_decoded.data.expect("second output");

        assert_eq!(first_output.source_id, second_output.source_id);
        assert_eq!(
            second_output.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(second_output.current_ingest_mode, None);

        let persisted_source = SourceRepository::new(storage.clone())
            .get("test_space", &source_id)
            .await
            .expect("reload source")
            .expect("persisted source");
        assert_eq!(persisted_source.base.security.level, 7);
        assert_eq!(
            persisted_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(persisted_source.current_ingest_mode, None);

        let persisted_doc = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted_doc.base.security.level, 7);

        let sources = SourceRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_source_with_doc_restores_soft_deleted_source() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = IngestSourceHandler::new(storage.clone());
        let source_id = RecordId::new_v4(TableName::Source);
        let doc_id = RecordId::new_v4(TableName::Doc);

        let source = Source::new(
            {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Source)
                    .build();
                base.id = source_id.clone();
                base
            },
            SourceType::Manual,
            "same-hash".to_string(),
        );
        let doc = Doc {
            base: {
                let mut base = BaseFieldsBuilder::new("test_space")
                    .for_table(TableName::Doc)
                    .build();
                base.id = doc_id.clone();
                base
            },
            title: "Doc".to_string(),
            text: "First".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let first = handler
            .execute(
                &ctx,
                json!({
                    "source": source.clone(),
                    "doc": doc.clone(),
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("first ingest");
        let first_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(first).expect("decode first");
        let first_output = first_decoded.data.expect("first output");

        SourceRepository::new(storage.clone())
            .delete("test_space", &source_id)
            .await
            .expect("delete source");

        let second = handler
            .execute(
                &ctx,
                json!({
                    "source": source,
                    "doc": doc,
                    "ingest_mode": "graph"
                }),
            )
            .await
            .expect("second ingest");
        let second_decoded: ToolResponse<SourceIngestOutput> =
            serde_json::from_value(second).expect("decode second");
        let second_output = second_decoded.data.expect("second output");

        assert_eq!(first_output.source_id, second_output.source_id);
        let restored = SourceRepository::new(storage.clone())
            .get("test_space", &source_id)
            .await
            .expect("load source")
            .expect("restored source");
        assert!(restored.base.deleted_at.is_none());
        let persisted_doc = DocRepository::new(storage.clone())
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted_doc.source.as_ref().expect("source link").id(),
            &source_id
        );
    }

    #[tokio::test]
    async fn test_batch_ingest_sources_accepts_normalized_staged_doc_shape() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage.clone(), authz, principal);
        let handler = BatchIngestSourcesHandler::new(storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("batch-normalized-source"),
        );

        let response = handler
            .execute(
                &ctx,
                json!({
                    "items": [{
                        "source": source,
                        "title": "Batch staged doc",
                        "text": "batch sidecar",
                        "media": [{
                            "kind": "image",
                            "media_type": "image/png",
                            "inline_base64": "AAEC"
                        }],
                        "ingest_mode": "graph"
                    }]
                }),
            )
            .await
            .expect("execute");

        let decoded: ToolResponse<BatchIngestSourcesOutput> =
            serde_json::from_value(response).expect("decode");
        let output = decoded.data.expect("output");
        assert!(output.failed.is_empty());
        assert_eq!(output.succeeded.len(), 1);

        let doc_id = RecordId::from_str(&output.succeeded[0].doc_id.clone().expect("doc id"))
            .expect("parse doc id");
        let persisted = DocRepository::new(storage)
            .get("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Batch staged doc");
        assert_eq!(persisted.sidecar_text.as_deref(), Some("batch sidecar"));
        assert_eq!(persisted.media_attachments.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_ingest_sources_rejects_mixed_legacy_and_normalized_doc_shapes() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = BatchIngestSourcesHandler::new(ctx.storage.clone());

        let source = Source::new(
            BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Source)
                .build(),
            SourceType::Manual,
            sha256_hex_prefixed("mixed-batch-source-shape"),
        );
        let doc = Doc {
            base: BaseFieldsBuilder::new("test_space")
                .for_table(TableName::Doc)
                .build(),
            title: "Legacy title".to_string(),
            text: "Legacy text".to_string(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: None,
            rendition_key: "legacy-doc".to_string(),
            metadata: json!({}),
        };

        let err = handler
            .execute(
                &ctx,
                json!({
                    "items": [{
                        "source": source,
                        "doc": doc,
                        "title": "Normalized title"
                    }]
                }),
            )
            .await
            .expect_err("mixed batch ingest_source shape should be rejected");
        assert!(
            err.to_string().contains("unknown field") || err.to_string().contains("did not match")
        );
    }

    #[tokio::test]
    async fn test_create_source_simple_rejects_invalid_name_or_uri() {
        let storage = setup_test_storage().await;
        let principal = make_test_principal("test_space");
        let authz = Arc::new(AuthzService::new_with_skip_auth(true));
        let ctx = ToolContext::with_authz(storage, authz, principal);
        let handler = CreateSourceSimpleHandler::new(ctx.storage.clone());

        let blank_uri = handler
            .execute(
                &ctx,
                json!({
                    "name": "Ontology feed",
                    "uri": "   "
                }),
            )
            .await
            .expect_err("blank uri should be rejected");
        assert!(blank_uri.to_string().contains("uri cannot be empty"));
    }
}
