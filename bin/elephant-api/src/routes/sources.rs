//! Source API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/sources/search - Search sources
//! - GET /v1/sources/{id} - Get source by ID
//! - POST /v1/sources - Create source
//! - PUT /v1/sources/{id} - Update source
//! - DELETE /v1/sources/{id} - Soft delete source
//! - POST /v1/sources/{id}/restore - Restore soft-deleted source

use std::str::FromStr;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::warn;
use uuid::Uuid;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;
use crate::routes::ingest_guard::ensure_semantic_ingest_ready;
use crate::routes::require_space_id;
use crate::runtime::RuntimeManager;
use elephant_pipeline::scheduler::ingest_work_kinds::enqueue_triage_source_work_item;
use elephant_storage::{
    deterministic_source_id_for_ingest, find_existing_doc_for_doc, find_existing_source_for_doc,
    find_existing_source_for_source, parse_id, persist_manual_doc_for_source_detailed,
    preview_persisted_manual_doc_for_source, rollback_persisted_manual_doc,
    rollback_persisted_source, MediaAssetRepository, PaginatedResult, PersistedManualDoc,
    Repository, SearchCriteria, SourceRepository, StorageClient, StorageResult,
};
use elephant_types::ids::TableName;
use elephant_types::mcp_simple::ExtractionGuidanceInput;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::media_asset::{inline_media_max_bytes, managed_asset_relative_path};
use elephant_types::records::{
    apply_manual_doc_to_source, prepare_source_for_reingest,
    prepare_source_for_reingest_preserving_acl_if_placeholder, validate_manual_ingest_mode,
    validate_simple_source_fields, Doc, DocMediaAttachment, DocMediaInput, DocMediaKind,
    DocMediaLocation, MediaAsset, Source, SourceType,
};
use elephant_types::{BaseFieldsBuilder, RecordId};

/// Shared state for source handlers.
pub struct SourcesState {
    /// Source repository.
    pub source_repo: Arc<dyn SourceRepo>,
    /// Storage client for workflow enqueueing.
    pub storage: StorageClient,
    /// Authorization service.
    pub authz: AuthzState,
    /// Optional managed runtime for semantic-ingest readiness checks.
    pub runtime: Option<Arc<RuntimeManager>>,
}

/// Source repository interface used by the API layer.
#[async_trait::async_trait]
pub trait SourceRepo: Send + Sync {
    /// Restores only if the row still matches the authorized snapshot.
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Source,
    ) -> StorageResult<Source>;
    async fn create(&self, record: Source) -> StorageResult<Source>;
    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>>;
    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source>;
    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()>;
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Source>>;

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>>;
    async fn restore(&self, space_id: &str, id: &elephant_types::RecordId)
        -> StorageResult<Source>;
    async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source>;
    async fn hard_delete(&self, space_id: &str, id: &elephant_types::RecordId)
        -> StorageResult<()>;
}

#[async_trait::async_trait]
impl SourceRepo for SourceRepository {
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Source,
    ) -> StorageResult<Source> {
        Self::restore_guarded(self, space_id, id, authorized).await
    }

    async fn create(&self, record: Source) -> StorageResult<Source> {
        elephant_storage::Repository::create(self, record).await
    }

    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>> {
        elephant_storage::Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source> {
        elephant_storage::Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()> {
        elephant_storage::Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Source>> {
        elephant_storage::SearchRepository::search(self, criteria).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Source> {
        Self::restore(self, space_id, id).await
    }

    async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source> {
        Self::restore_and_replace_existing(self, space_id, id, record).await
    }

    async fn hard_delete(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<()> {
        Self::hard_delete(self, space_id, id).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl SourceRepo for elephant_storage::MockSourceRepository {
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Source,
    ) -> StorageResult<Source> {
        let current = Self::get_with_deleted(self, space_id, id).await?;
        let unchanged = current.is_some_and(|current| {
            elephant_storage::base_envelope_unchanged(&current.base, &authorized.base)
        });
        if !unchanged {
            return Err(elephant_storage::StorageError::Conflict(format!(
                "Source changed since it was authorized: {id}"
            )));
        }
        Self::restore(self, space_id, id).await
    }

    async fn create(&self, record: Source) -> StorageResult<Source> {
        elephant_storage::Repository::create(self, record).await
    }

    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>> {
        elephant_storage::Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source> {
        elephant_storage::Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()> {
        elephant_storage::Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Source>> {
        elephant_storage::SearchRepository::search(self, criteria).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Source>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Source> {
        Self::restore(self, space_id, id).await
    }

    async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Source,
    ) -> StorageResult<Source> {
        Self::restore_and_replace_existing(self, space_id, id, record).await
    }

    async fn hard_delete(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<()> {
        Self::hard_delete(self, space_id, id).await
    }
}

impl SourcesState {
    /// Creates new source state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self::new_with_runtime(client, authz, None)
    }

    #[must_use]
    pub fn new_with_runtime(
        client: StorageClient,
        authz: AuthzState,
        runtime: Option<Arc<RuntimeManager>>,
    ) -> Self {
        Self {
            source_repo: Arc::new(SourceRepository::new(client.clone())),
            storage: client,
            authz,
            runtime,
        }
    }

    /// Creates source state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(
        source_repo: Arc<dyn SourceRepo>,
        storage: StorageClient,
        authz: AuthzState,
    ) -> Self {
        Self {
            source_repo,
            storage,
            authz,
            runtime: None,
        }
    }
}

async fn update_existing_source_for_reingest_with_repo(
    repo: &dyn SourceRepo,
    existing: &Source,
    updated: Source,
) -> Result<Source, (StatusCode, Json<ApiError>)> {
    let space_id = existing.base.space_id.clone();
    let source_id = existing.base.id.clone();
    if existing.base.deleted_at.is_some() {
        repo.restore_and_replace_existing(&space_id, &source_id, updated)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })
    } else {
        repo.update(&space_id, &source_id, updated)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })
    }
}

/// Source search request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSearchRequest {
    /// Free-text query.
    #[serde(default)]
    pub query: Option<String>,
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted sources.
    #[serde(default)]
    pub include_deleted: bool,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

const fn default_limit() -> usize {
    50
}

/// Search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSearchResponse {
    /// Search results.
    pub results: Vec<Source>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    pub total: usize,
}

/// Source-first ingest request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SourceIngestRequest {
    /// Preferred normalized source-first request shape.
    Normalized(Box<NormalizedSourceIngestRequest>),
    /// Legacy storage-shaped request kept for compatibility.
    Legacy(Box<LegacySourceIngestRequest>),
}

/// Preferred normalized source-first ingest body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedSourceIngestRequest {
    /// Human-readable source name.
    pub name: String,
    /// Optional source URI.
    #[serde(default)]
    pub uri: Option<String>,
    /// Optional source type (e.g. `web`, `device_frame`, `manual`).
    #[serde(default)]
    pub source_type: Option<String>,
    /// Optional staged document title.
    #[serde(default)]
    pub title: Option<String>,
    /// Optional staged document text or sidecar text for media-backed docs.
    #[serde(default)]
    pub text: Option<String>,
    /// Optional typed image/video attachments for the staged document.
    #[serde(default)]
    pub media: Vec<DocMediaInput>,
    /// Optional collection hint for the staged document.
    #[serde(default)]
    pub collection: Option<String>,
    /// Optional staged document metadata.
    #[serde(default)]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional explicit rendition key for staged doc idempotency.
    #[serde(default)]
    pub rendition_key: Option<String>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
    /// Requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
    /// Whether to process synchronously.
    ///
    /// Source-first ingest is currently asynchronous only.
    #[serde(default)]
    pub sync: bool,
}

/// Legacy storage-shaped source ingest body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacySourceIngestRequest {
    /// Source data to create first-class provenance.
    pub source: Source,
    /// Optional staged manual document payload for retrieval/graph ingest.
    #[serde(default)]
    pub doc: Option<Doc>,
    /// Requested ingest depth.
    #[serde(default)]
    pub ingest_mode: Option<IngestMode>,
    /// Whether to process synchronously.
    ///
    /// Source-first ingest is currently asynchronous only.
    #[serde(default)]
    pub sync: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct NormalizedStagedDocInput {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    media: Vec<DocMediaInput>,
    #[serde(default)]
    collection: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    rendition_key: Option<String>,
    #[serde(flatten)]
    extraction_guidance: ExtractionGuidanceInput,
}

/// Source-first ingest response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIngestResult {
    /// Created source ID.
    pub source_id: String,
    /// Current lifecycle status.
    pub lifecycle_status: elephant_types::records::SourceLifecycleStatus,
    /// Highest selected/achieved ingest mode, if any.
    pub current_ingest_mode: Option<IngestMode>,
    /// Enqueued workflow item.
    pub work_item_id: Option<String>,
    /// Preallocated doc ID when manual document content was supplied.
    pub doc_id: Option<String>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;
const SOURCE_SEARCH_SCOPE: &str = "read:source";
const MAX_VISIBLE_SOURCE_SEARCH_PAGES: usize = 10_000;

fn normalize_optional_text(text: Option<String>) -> Option<String> {
    text.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

const fn media_kind_name(kind: elephant_types::records::DocMediaKind) -> &'static str {
    match kind {
        elephant_types::records::DocMediaKind::Image => "image",
        elephant_types::records::DocMediaKind::Video => "video",
    }
}

fn sha256_hex_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn inferred_content_type(media: &[DocMediaInput]) -> String {
    media
        .first()
        .map_or_else(|| "text/plain".to_string(), |item| item.media_type.clone())
}

fn normalized_source_hash(req: &NormalizedSourceIngestRequest) -> Result<String, ApiError> {
    let has_staged_doc = req.title.is_some()
        || normalize_optional_text(req.text.clone()).is_some()
        || !req.media.is_empty()
        || req.collection.is_some()
        || req.metadata.is_some()
        || req.rendition_key.is_some();
    if !has_staged_doc {
        if let Some(uri) = req.uri.as_deref() {
            return Ok(sha256_hex_prefixed(uri.as_bytes()));
        }
        return Ok(sha256_hex_prefixed(req.name.as_bytes()));
    }

    let mut media = Vec::with_capacity(req.media.len());
    for item in &req.media {
        let inline_sha256 = if item.inline_base64.is_some() {
            let bytes = decode_inline_media(item).map_err(|(_, error)| error.0)?;
            Some(sha256_hex_prefixed(&bytes))
        } else {
            None
        };
        media.push(serde_json::json!({
            "kind": item.kind,
            "media_type": item.media_type.clone(),
            "duration_ms": item.duration_ms,
            "inline_sha256": inline_sha256,
            "path": item.path.clone(),
            "uri": item.uri.clone(),
        }));
    }

    let payload = serde_json::to_vec(&serde_json::json!({
        "name": req.name,
        "uri": req.uri,
        "source_type": req.source_type,
        "title": req.title,
        "text": normalize_optional_text(req.text.clone()),
        "media": media,
        "collection": req.collection,
        "metadata": req.metadata,
        "rendition_key": req.rendition_key,
        "extraction_guidance": req.extraction_guidance,
    }))
    .map_err(|e| ApiError::internal(format!("failed to normalize source ingest payload: {e}")))?;
    Ok(sha256_hex_prefixed(&payload))
}

#[allow(clippy::result_large_err)]
fn validate_public_media_inputs(
    inputs: &[DocMediaInput],
) -> Result<(), (StatusCode, Json<ApiError>)> {
    for (index, media) in inputs.iter().enumerate() {
        if media.media_type.trim().is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "media[{index}].media_type cannot be empty"
                ))),
            ));
        }
        if media.transport_count() != 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "media[{index}] must specify exactly one media transport"
                ))),
            ));
        }
        if media.path.is_some() || media.uri.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "media[{index}] must use inline_base64; public ingest does not accept path or uri media transports"
                ))),
            ));
        }
        if matches!(media.kind, DocMediaKind::Video) && media.inline_base64.is_some() {
            match media.duration_ms {
                Some(duration_ms) if duration_ms > 0 => {}
                _ => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::validation(format!(
                            "media[{index}].duration_ms must be a positive integer for inline video payloads"
                        ))),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_public_media_attachments(
    attachments: &[DocMediaAttachment],
) -> Result<(), (StatusCode, Json<ApiError>)> {
    for (index, attachment) in attachments.iter().enumerate() {
        match &attachment.location {
            DocMediaLocation::Path { .. } | DocMediaLocation::Uri { .. } => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(format!(
                        "media_attachments[{index}] must not use path or uri transports on public ingest surfaces"
                    ))),
                ));
            }
            DocMediaLocation::ManagedAsset { .. } => {}
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn decode_inline_media(media: &DocMediaInput) -> Result<Vec<u8>, (StatusCode, Json<ApiError>)> {
    let payload = media.inline_base64.as_deref().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "inline media payload is missing inline_base64",
            )),
        )
    })?;
    let encoded = payload
        .split_once(',')
        .filter(|(prefix, _)| prefix.contains(";base64"))
        .map_or(payload, |(_, data)| data)
        .trim();
    BASE64_STANDARD.decode(encoded).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(format!(
                "invalid inline {} payload: {e}",
                media_kind_name(media.kind)
            ))),
        )
    })
}

#[derive(Debug, Clone)]
struct PreparedMediaAttachments {
    attachments: Vec<DocMediaAttachment>,
    assets: Vec<MediaAsset>,
}

#[derive(Debug, Clone)]
struct PreparedStagedDoc {
    doc: Doc,
    pending_assets: Vec<MediaAsset>,
}

#[allow(clippy::result_large_err)]
fn prepare_inline_media_asset(
    space_id: &str,
    media: &DocMediaInput,
    attachment_index: usize,
) -> Result<(DocMediaAttachment, MediaAsset), (StatusCode, Json<ApiError>)> {
    let bytes = decode_inline_media(media)?;
    prepare_managed_media_asset_bytes(space_id, media, attachment_index, &bytes)
}

#[allow(clippy::result_large_err)]
fn prepare_managed_media_asset_bytes(
    space_id: &str,
    media: &DocMediaInput,
    attachment_index: usize,
    bytes: &[u8],
) -> Result<(DocMediaAttachment, MediaAsset), (StatusCode, Json<ApiError>)> {
    let max_bytes = usize::try_from(inline_media_max_bytes(media.kind)).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(
                "configured inline media size exceeds addressable platform size",
            )),
        )
    })?;
    if bytes.len() > max_bytes {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(format!(
                "inline {} payload exceeds {} bytes",
                media_kind_name(media.kind),
                max_bytes
            ))),
        ));
    }

    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let relative_path = managed_asset_relative_path(&asset_id);

    let mut base = BaseFieldsBuilder::new(space_id.to_string())
        .for_table(TableName::MediaAsset)
        .build();
    base.id = asset_id.clone();

    let asset = MediaAsset {
        base,
        kind: media.kind,
        media_type: media.media_type.clone(),
        storage_path: relative_path,
        size_bytes: u64::try_from(bytes.len()).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal("media payload length overflow")),
            )
        })?,
        sha256: sha256_hex_prefixed(bytes),
        payload_base64: Some(BASE64_STANDARD.encode(bytes)),
        duration_ms: media.duration_ms,
        label: None,
    };
    let asset_size_bytes = asset.size_bytes;
    let asset_sha256 = asset.sha256.clone();
    Ok((
        DocMediaAttachment {
            attachment_id: format!("media-{attachment_index}"),
            kind: media.kind,
            media_type: media.media_type.clone(),
            location: DocMediaLocation::ManagedAsset {
                asset_id: asset_id.to_string(),
            },
            label: None,
            size_bytes: Some(asset_size_bytes),
            duration_ms: media.duration_ms,
            sha256: Some(asset_sha256),
        },
        asset,
    ))
}

#[allow(clippy::result_large_err)]
fn build_media_attachments(
    space_id: &str,
    inputs: &[DocMediaInput],
) -> Result<PreparedMediaAttachments, (StatusCode, Json<ApiError>)> {
    validate_public_media_inputs(inputs)?;
    let mut attachments = Vec::with_capacity(inputs.len());
    let mut assets = Vec::with_capacity(inputs.len());
    for (index, media) in inputs.iter().enumerate() {
        let (attachment, asset) = prepare_inline_media_asset(space_id, media, index)?;
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
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let repo = MediaAssetRepository::new(storage.clone());
    let mut persisted_ids = Vec::with_capacity(assets.len());
    for asset in assets {
        match repo.create(asset.clone()).await {
            Ok(_) => persisted_ids.push(asset.base.id.clone()),
            Err(e) => {
                rollback_media_assets_by_ids(storage, &asset.base.space_id, &persisted_ids).await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                ));
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
        if let Err(err) = repo.delete(space_id, asset_id).await {
            warn!(
                asset_id = %asset_id,
                error = %err,
                "failed to roll back staged media asset after source ingest failure"
            );
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

impl NormalizedStagedDocInput {
    const fn has_doc_fields(&self) -> bool {
        self.title.is_some()
            || self.text.is_some()
            || !self.media.is_empty()
            || self.collection.is_some()
            || self.metadata.is_some()
            || self.rendition_key.is_some()
    }

    #[allow(clippy::result_large_err)]
    fn into_doc(
        self,
        space_id: &str,
    ) -> Result<Option<PreparedStagedDoc>, (StatusCode, Json<ApiError>)> {
        if !self.has_doc_fields() {
            return Ok(None);
        }

        let title = self.title.ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(
                    "staged source-first document requires `title` when document fields are provided",
                )),
            )
        })?;

        let normalized_text = normalize_optional_text(self.text);
        let has_media = !self.media.is_empty();
        if !has_media && normalized_text.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(
                    "staged source-first document requires non-empty text or at least one media attachment",
                )),
            ));
        }

        let prepared_media = build_media_attachments(space_id, &self.media)?;
        let mut metadata = self.metadata.unwrap_or_default();
        if let Some(collection) = self.collection {
            metadata.insert("collection".to_string(), Value::String(collection));
        }
        metadata.insert(
            "content_type".to_string(),
            Value::String(inferred_content_type(&self.media)),
        );
        self.extraction_guidance.apply_to_metadata(&mut metadata);

        Ok(Some(PreparedStagedDoc {
            doc: Doc {
                base: BaseFieldsBuilder::new(space_id.to_string())
                    .for_table(TableName::Doc)
                    .build(),
                title,
                text: if has_media {
                    String::new()
                } else {
                    normalized_text.clone().unwrap_or_default()
                },
                sidecar_text: has_media.then_some(normalized_text).flatten(),
                source: None,
                media_attachments: prepared_media.attachments,
                metadata: Value::Object(metadata),
                rendition_key: self
                    .rendition_key
                    .unwrap_or_else(|| format!("api-{}", Uuid::new_v4())),
            },
            pending_assets: prepared_media.assets,
        }))
    }
}

impl SourceIngestRequest {
    const fn sync(&self) -> bool {
        match self {
            Self::Normalized(req) => req.sync,
            Self::Legacy(req) => req.sync,
        }
    }

    #[allow(clippy::result_large_err, clippy::type_complexity)]
    fn into_parts(
        self,
        space_id: &str,
    ) -> Result<(Source, Option<PreparedStagedDoc>, Option<IngestMode>), (StatusCode, Json<ApiError>)>
    {
        match self {
            Self::Normalized(req) => {
                validate_simple_source_fields(&req.name, req.uri.as_deref()).map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::validation(e.to_string())),
                    )
                })?;
                let source_type = match req.source_type.as_deref() {
                    Some(source_type) => SourceType::from_str(source_type).map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(ApiError::validation(e.to_string())),
                        )
                    })?,
                    None => req.uri.as_deref().map_or(SourceType::Manual, |uri| {
                        if uri.starts_with("http://") || uri.starts_with("https://") {
                            SourceType::Web
                        } else {
                            SourceType::Manual
                        }
                    }),
                };
                let hash = normalized_source_hash(&req)
                    .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, Json(error)))?;
                let base = BaseFieldsBuilder::new(space_id.to_string())
                    .for_table(TableName::Source)
                    .build();
                let mut source = Source::new(base, source_type, hash);
                source.external_id = Some(req.name);
                source.uri = req.uri;
                if let Some(metadata) = source.metadata.as_object_mut() {
                    req.extraction_guidance.apply_to_metadata(metadata);
                }
                let doc = NormalizedStagedDocInput {
                    title: req.title,
                    text: req.text,
                    media: req.media,
                    collection: req.collection,
                    metadata: req.metadata,
                    rendition_key: req.rendition_key,
                    extraction_guidance: req.extraction_guidance,
                }
                .into_doc(space_id)?;
                Ok((source, doc, req.ingest_mode))
            }
            Self::Legacy(req) => {
                if let Some(doc) = &req.doc {
                    validate_public_media_attachments(&doc.media_attachments)?;
                }
                Ok((
                    req.source,
                    req.doc.map(|doc| PreparedStagedDoc {
                        doc,
                        pending_assets: Vec::new(),
                    }),
                    req.ingest_mode,
                ))
            }
        }
    }
}

fn build_source_search_criteria(
    space_id: &str,
    query: Option<&str>,
    include_deleted: bool,
    cursor: Option<String>,
    limit: usize,
) -> SearchCriteria {
    let mut criteria = SearchCriteria::new()
        .with_limit(limit)
        .with_space_id(space_id.to_string());
    if let Some(query) = query {
        criteria = criteria.with_query(query.to_string());
    }
    if include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(cursor) = cursor {
        criteria = criteria.with_cursor(cursor);
    }
    criteria
}

async fn stage_source_for_ingest(
    state: &SourcesState,
    principal: &AuthPrincipal,
    source: Source,
    doc: Option<&Doc>,
) -> Result<(Source, Option<Source>), (StatusCode, Json<ApiError>)> {
    let existing_source = if let Some(doc) = doc {
        find_existing_source_for_doc(&state.storage, doc)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    } else {
        find_existing_source_for_source(&state.storage, &source)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    };
    if let Some(existing_source) = existing_source {
        state
            .authz
            .authorize_record_write(principal, &existing_source, "write:source")
            .map_err(|e| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError {
                        code: e.code,
                        message: e.message,
                        details: None,
                    }),
                )
            })?;
        let updated_source = if doc.is_some() {
            prepare_source_for_reingest(&existing_source, source)
        } else {
            prepare_source_for_reingest_preserving_acl_if_placeholder(&existing_source, source)
        };
        state
            .authz
            .authorize_record_write(principal, &updated_source, "write:source")
            .map_err(|e| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError {
                        code: e.code,
                        message: e.message,
                        details: None,
                    }),
                )
            })?;
        return Ok((updated_source, Some(existing_source)));
    }

    state
        .authz
        .authorize_record_write(principal, &source, "write:source")
        .map_err(|e| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: e.code,
                    message: e.message,
                    details: None,
                }),
            )
        })?;
    Ok((source, None))
}

async fn rollback_staged_source_and_doc(
    state: &SourcesState,
    source: &Source,
    previous_source: Option<&Source>,
    staged_doc: Option<&PersistedManualDoc>,
) {
    if let Some(staged_doc) = staged_doc {
        if let Err(err) = rollback_persisted_manual_doc(&state.storage, staged_doc).await {
            warn!(
                doc_id = %staged_doc.doc.base.id,
                source_id = %source.base.id,
                rollback_error = %err,
                "failed to roll back staged source doc after ingest failure"
            );
        }
    }

    match previous_source {
        Some(previous_source) => {
            if let Err(err) =
                rollback_persisted_source(&state.storage, source, Some(previous_source)).await
            {
                warn!(
                    source_id = %previous_source.base.id,
                    rollback_error = %err,
                    "failed to roll back staged source after ingest failure"
                );
            }
        }
        None => {
            if let Err(err) = rollback_persisted_source(&state.storage, source, None).await {
                warn!(
                    source_id = %source.base.id,
                    rollback_error = %err,
                    "failed to delete staged source after ingest failure"
                );
            }
        }
    }
}

async fn create_or_reuse_source_candidate(
    state: &SourcesState,
    principal: &AuthPrincipal,
    source_candidate: &Source,
    has_staged_doc: bool,
) -> Result<(Source, Option<Source>), (StatusCode, Json<ApiError>)> {
    match state.source_repo.create(source_candidate.clone()).await {
        Ok(created) => Ok((created, None)),
        Err(elephant_storage::StorageError::Conflict(_)) => {
            let existing = state
                .source_repo
                .get_with_deleted(&source_candidate.base.space_id, &source_candidate.base.id)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
                .ok_or_else(|| {
                    (
                        StatusCode::CONFLICT,
                        Json(ApiError::validation(
                            "source ingest conflict without persisted source".to_string(),
                        )),
                    )
                })?;
            state
                .authz
                .authorize_record_write(principal, &existing, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;
            let updated_source = if has_staged_doc {
                prepare_source_for_reingest(&existing, source_candidate.clone())
            } else {
                prepare_source_for_reingest_preserving_acl_if_placeholder(
                    &existing,
                    source_candidate.clone(),
                )
            };
            state
                .authz
                .authorize_record_write(principal, &updated_source, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;
            let persisted = update_existing_source_for_reingest_with_repo(
                state.source_repo.as_ref(),
                &existing,
                updated_source,
            )
            .await?;
            Ok((persisted, Some(existing)))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

async fn authorize_persisted_source_doc_write(
    state: &SourcesState,
    principal: &AuthPrincipal,
    source: &Source,
    doc: &Doc,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let preview = preview_persisted_manual_doc_for_source(&state.storage, source, doc)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    state
        .authz
        .authorize_record_write(principal, &preview, "write:doc")
        .map_err(|e| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: e.code,
                    message: e.message,
                    details: None,
                }),
            )
        })
}

async fn persist_manual_doc_with_source_rollback(
    state: &SourcesState,
    source: &Source,
    previous_source: Option<&Source>,
    doc: Doc,
) -> Result<PersistedManualDoc, (StatusCode, Json<ApiError>)> {
    match persist_manual_doc_for_source_detailed(&state.storage, source, doc).await {
        Ok(staged_doc) => Ok(staged_doc),
        Err(err) => {
            match previous_source {
                Some(previous_source) => {
                    if let Err(rollback_err) =
                        rollback_persisted_source(&state.storage, source, Some(previous_source))
                            .await
                    {
                        warn!(
                            source_id = %previous_source.base.id,
                            error = %err,
                            rollback_error = %rollback_err,
                            "failed to roll back source after manual doc persistence error"
                        );
                    }
                }
                None => {
                    if let Err(cleanup_err) =
                        rollback_persisted_source(&state.storage, source, None).await
                    {
                        warn!(
                            source_id = %source.base.id,
                            error = %err,
                            cleanup_error = %cleanup_err,
                            "failed to roll back source after manual doc persistence error"
                        );
                    }
                }
            }
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(err.to_string())),
            ))
        }
    }
}

async fn persist_staged_source_and_doc(
    state: &SourcesState,
    principal: &AuthPrincipal,
    source_candidate: Source,
    previous_source: Option<Source>,
    doc: Option<Doc>,
) -> Result<(Source, Option<PersistedManualDoc>, Option<Source>), (StatusCode, Json<ApiError>)> {
    if let Some(previous_source) = previous_source {
        let updated_source = update_existing_source_for_reingest_with_repo(
            state.source_repo.as_ref(),
            &previous_source,
            source_candidate,
        )
        .await?;
        let staged_doc = match doc {
            Some(doc) => Some(
                persist_manual_doc_with_source_rollback(
                    state,
                    &updated_source,
                    Some(&previous_source),
                    doc,
                )
                .await?,
            ),
            None => None,
        };
        Ok((updated_source, staged_doc, None))
    } else {
        let (created_source, rollback_source) =
            create_or_reuse_source_candidate(state, principal, &source_candidate, doc.is_some())
                .await?;
        let staged_doc = match doc {
            Some(doc) => Some(
                persist_manual_doc_with_source_rollback(
                    state,
                    &created_source,
                    rollback_source.as_ref(),
                    doc,
                )
                .await?,
            ),
            None => None,
        };
        Ok((created_source, staged_doc, rollback_source))
    }
}

async fn count_visible_source_search_results(
    state: &SourcesState,
    principal: &AuthPrincipal,
    space_id: &str,
    query: Option<&str>,
    include_deleted: bool,
) -> Result<usize, (StatusCode, Json<ApiError>)> {
    let mut total_visible = 0usize;
    let mut scan_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_SOURCE_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search exceeded the maximum page scan bound",
                )),
            ));
        }

        let criteria =
            build_source_search_criteria(space_id, query, include_deleted, scan_cursor, 1);
        let result = state.source_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let Some(source) = result.results.into_iter().next() else {
            break;
        };

        if !state
            .authz
            .filter_records(principal, vec![source], SOURCE_SEARCH_SCOPE)
            .is_empty()
        {
            total_visible = total_visible.saturating_add(1);
        }

        if !result.has_more {
            break;
        }

        let Some(next_cursor) = result.cursor else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_cursor);
    }

    Ok(total_visible)
}

async fn rerun_source_search_after_authz_filter(
    state: &SourcesState,
    principal: &AuthPrincipal,
    space_id: &str,
    query: Option<&str>,
    limit: usize,
    include_deleted: bool,
    start_cursor: Option<String>,
) -> Result<SourceSearchResponse, (StatusCode, Json<ApiError>)> {
    let mut visible = Vec::new();
    let total_visible =
        count_visible_source_search_results(state, principal, space_id, query, include_deleted)
            .await?;
    let mut scan_cursor = start_cursor;
    let mut next_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_SOURCE_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search exceeded the maximum page scan bound",
                )),
            ));
        }

        let criteria =
            build_source_search_criteria(space_id, query, include_deleted, scan_cursor.clone(), 1);
        let result = state.source_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let page_cursor = scan_cursor.clone();
        let result_cursor = result.cursor.clone();
        let Some(source) = result.results.into_iter().next() else {
            break;
        };

        let allowed = state
            .authz
            .filter_records(principal, vec![source], SOURCE_SEARCH_SCOPE);
        if let Some(source) = allowed.into_iter().next() {
            if visible.len() < limit {
                visible.push(source);
            } else if next_cursor.is_none() {
                next_cursor = page_cursor;
            }
        }

        if !result.has_more {
            break;
        }

        let Some(next_scan_cursor) = result_cursor else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_scan_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Source search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_scan_cursor);
    }

    let has_more = next_cursor.is_some();
    Ok(SourceSearchResponse {
        results: visible,
        cursor: next_cursor,
        has_more,
        total: total_visible,
    })
}

/// POST /v1/sources/search - Search sources.
async fn search_sources(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<SourceSearchRequest>,
) -> ApiResult<SourceSearchResponse> {
    let space_id = require_space_id(&principal)?;

    if let Some(req_space_id) = &req.space_id {
        if req_space_id != &space_id {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access to space denied")),
            ));
        }
    }

    let query = req.query.clone();
    let start_cursor = req.cursor.clone();
    let mut criteria = SearchCriteria::new()
        .with_limit(req.limit)
        .with_space_id(space_id.clone());
    if let Some(q) = query.clone() {
        criteria = criteria.with_query(q);
    }
    if req.include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(c) = start_cursor.clone() {
        criteria = criteria.with_cursor(c);
    }

    let result = state.source_repo.search(&criteria).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    let page_len = result.results.len();
    let filtered = state
        .authz
        .filter_records(&principal, result.results, SOURCE_SEARCH_SCOPE);
    let total_visible = count_visible_source_search_results(
        &state,
        &principal,
        &space_id,
        query.as_deref(),
        req.include_deleted,
    )
    .await?;

    if filtered.len() != page_len {
        let response = rerun_source_search_after_authz_filter(
            &state,
            &principal,
            &space_id,
            query.as_deref(),
            req.limit,
            req.include_deleted,
            start_cursor,
        )
        .await?;
        return Ok(Json(response));
    }

    Ok(Json(SourceSearchResponse {
        results: filtered,
        cursor: result.cursor,
        has_more: result.has_more,
        total: total_visible,
    }))
}

/// POST /v1/sources - Create source.
async fn create_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(source): Json<Source>,
) -> Result<(StatusCode, Json<Source>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut source = source;
    source.base.space_id = require_space_id(&principal)?;

    state
        .authz
        .authorize_record_write(&principal, &source, "write:source")
        .map_err(|e| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: e.code,
                    message: e.message,
                    details: None,
                }),
            )
        })?;

    match state.source_repo.create(source).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

#[allow(clippy::too_many_lines)]
async fn ingest_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<SourceIngestRequest>,
) -> Result<(StatusCode, Json<SourceIngestResult>), (StatusCode, Json<ApiError>)> {
    ensure_semantic_ingest_ready(state.runtime.as_ref()).await?;
    if req.sync() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "sync source ingest is not supported; use the asynchronous work item flow"
                    .to_string(),
            )),
        ));
    }
    let space_id = require_space_id(&principal)?;
    let (mut source, prepared_doc, ingest_mode) = req.into_parts(&space_id)?;
    let pending_assets = prepared_doc
        .as_ref()
        .map_or_else(Vec::new, |item| item.pending_assets.clone());
    let mut doc = prepared_doc.map(|item| item.doc);
    source.base.space_id.clone_from(&space_id);
    if doc.is_none() {
        source.base.id = deterministic_source_id_for_ingest(&source);
    }

    let doc_id =
        match stage_manual_doc_for_ingest(&state, &principal, &mut source, doc.as_mut()).await {
            Ok(doc_id) => doc_id,
            Err(err) => return Err(err),
        };

    let requested_mode = ingest_mode.unwrap_or_else(|| {
        if doc_id.is_some() {
            IngestMode::Graph
        } else {
            IngestMode::Metadata
        }
    });
    validate_manual_ingest_mode(requested_mode, doc_id.is_some()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    source.set_requested_ingest_mode(requested_mode);
    let (source_candidate, previous_source) =
        match stage_source_for_ingest(&state, &principal, source, doc.as_ref()).await {
            Ok(value) => value,
            Err(err) => return Err(err),
        };
    if let Some(doc_ref) = doc.as_ref() {
        authorize_persisted_source_doc_write(&state, &principal, &source_candidate, doc_ref)
            .await?;
    }

    persist_prepared_media_assets(&state.storage, &pending_assets).await?;

    let rollback_source = previous_source.clone();
    let (created, staged_doc, rollback_source_override) = match Box::pin(
        persist_staged_source_and_doc(&state, &principal, source_candidate, previous_source, doc),
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            rollback_prepared_media_assets(&state.storage, &pending_assets).await;
            return Err(err);
        }
    };
    let rollback_source = rollback_source.or(rollback_source_override);
    let work_item_id_result =
        enqueue_triage_source_work_item(&state.storage, &created, &principal.subject, 5).await;
    let work_item_id = match work_item_id_result {
        Ok(work_item_id) => work_item_id,
        Err(e) => {
            rollback_prepared_media_assets(&state.storage, &pending_assets).await;
            rollback_staged_source_and_doc(
                &state,
                &created,
                rollback_source.as_ref(),
                staged_doc.as_ref(),
            )
            .await;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ));
        }
    };

    Ok((
        StatusCode::CREATED,
        Json(SourceIngestResult {
            source_id: created.base.id.to_string(),
            lifecycle_status: created.lifecycle_status,
            current_ingest_mode: created.current_ingest_mode,
            work_item_id: Some(work_item_id.to_string()),
            doc_id,
        }),
    ))
}

async fn ingest_source_http(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    request: Result<Json<SourceIngestRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<SourceIngestResult>), (StatusCode, Json<ApiError>)> {
    let request = request.map_err(|error| {
        use axum::extract::rejection::JsonRejection;
        let status = match &error {
            JsonRejection::JsonDataError(_) | JsonRejection::JsonSyntaxError(_) => {
                StatusCode::BAD_REQUEST
            }
            other => other.status(),
        };
        (
            status,
            Json(ApiError::validation(format!(
                "invalid source ingest request: {}",
                error.body_text()
            ))),
        )
    })?;
    ingest_source(State(state), axum::Extension(principal), request).await
}

async fn stage_manual_doc_for_ingest(
    state: &SourcesState,
    principal: &AuthPrincipal,
    source: &mut Source,
    doc: Option<&mut Doc>,
) -> Result<Option<String>, (StatusCode, Json<ApiError>)> {
    let Some(doc_ref) = doc else {
        return Ok(None);
    };

    doc_ref.base.space_id = require_space_id(principal)?;
    if let Some(existing_doc) = find_existing_doc_for_doc(&state.storage, doc_ref)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
    {
        state
            .authz
            .authorize_record_write(principal, &existing_doc, "write:doc")
            .map_err(|e| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError {
                        code: e.code,
                        message: e.message,
                        details: None,
                    }),
                )
            })?;
    } else {
        state
            .authz
            .authorize_record_write(principal, doc_ref, "write:doc")
            .map_err(|e| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError {
                        code: e.code,
                        message: e.message,
                        details: None,
                    }),
                )
            })?;
    }

    let doc_id = apply_manual_doc_to_source(source, doc_ref).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    Ok(Some(doc_id))
}

/// GET /v1/sources/{id} - Get source by ID.
async fn get_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Source> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Source).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.source_repo.get(&space_id, &rid).await {
        Ok(Some(source)) => {
            state
                .authz
                .authorize_record_read(&principal, &source, "read:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;
            Ok(Json(source))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Source", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// PUT /v1/sources/{id} - Update source.
async fn update_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(source): Json<Source>,
) -> ApiResult<Source> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Source).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.source_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;
            state
                .authz
                .authorize_record_write(&principal, &source, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;

            match state.source_repo.update(&space_id, &rid, source).await {
                Ok(updated) => Ok(Json(updated)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Source", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// DELETE /v1/sources/{id} - Soft delete source.
async fn delete_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Source).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.source_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;

            match state.source_repo.delete(&space_id, &rid).await {
                Ok(()) => Ok(StatusCode::NO_CONTENT),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Source", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/sources/{id}/restore - Restore soft-deleted source.
async fn restore_source(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Source> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Source).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.source_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;
            // The response carries the restored record, so read is required too.
            state
                .authz
                .authorize_record_read(&principal, &existing, "read:source")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;

            match state
                .source_repo
                .restore_guarded(&space_id, &rid, &existing)
                .await
            {
                Ok(restored) => Ok(Json(restored)),
                Err(elephant_storage::StorageError::Conflict(_)) => Err((
                    StatusCode::CONFLICT,
                    Json(ApiError {
                        code: "CONFLICT".to_string(),
                        message: "record changed since it was authorized; retry".to_string(),
                        details: None,
                    }),
                )),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Source", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// Returns source routes.
pub fn source_routes() -> Router<Arc<SourcesState>> {
    Router::new()
        .route("/v1/sources/search", post(search_sources))
        .route("/v1/sources/ingest", post(ingest_source_http))
        .route("/v1/sources", post(create_source))
        .route("/v1/sources/{id}", get(get_source))
        .route("/v1/sources/{id}", put(update_source))
        .route("/v1/sources/{id}", delete(delete_source))
        .route("/v1/sources/{id}/restore", post(restore_source))
}

#[cfg(test)]
#[path = "sources_test.rs"]
mod integration_tests;

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
        clippy::cognitive_complexity,
        clippy::large_futures
    )]
    use super::*;
    use crate::auth::Principal as AuthPrincipal;
    use crate::authz::AuthzService;
    use elephant_storage::Repository;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Doc, SourceLifecycleStatus, SourceType};
    use elephant_types::RecordId;
    use std::str::FromStr;
    use std::sync::Arc;

    fn legacy_ingest_request(
        source: Source,
        doc: Option<Doc>,
        ingest_mode: Option<IngestMode>,
        sync: bool,
    ) -> SourceIngestRequest {
        SourceIngestRequest::Legacy(Box::new(LegacySourceIngestRequest {
            source,
            doc,
            ingest_mode,
            sync,
        }))
    }

    fn normalized_ingest_request(
        source: &Source,
        title: Option<&str>,
        text: Option<&str>,
        media: Vec<DocMediaInput>,
        collection: Option<&str>,
        ingest_mode: Option<IngestMode>,
        sync: bool,
    ) -> SourceIngestRequest {
        let source_type = serde_json::to_value(source.type_)
            .expect("serialize source type")
            .as_str()
            .expect("source type string")
            .to_string();
        SourceIngestRequest::Normalized(Box::new(NormalizedSourceIngestRequest {
            name: source
                .external_id
                .clone()
                .unwrap_or_else(|| "normalized-source".to_string()),
            uri: source.uri.clone(),
            source_type: Some(source_type),
            title: title.map(std::borrow::ToOwned::to_owned),
            text: text.map(std::borrow::ToOwned::to_owned),
            media,
            collection: collection.map(std::borrow::ToOwned::to_owned),
            metadata: None,
            rendition_key: None,
            extraction_guidance: ExtractionGuidanceInput::default(),
            ingest_mode,
            sync,
        }))
    }

    #[tokio::test]
    async fn test_source_http_rejects_invalid_json_with_structured_validation_error() {
        use axum::body::{to_bytes, Body};
        use axum::http::Request;
        use tower::ServiceExt;

        let storage = StorageClient::new_embedded().await.expect("storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let app = source_routes()
            .layer(axum::Extension(AuthPrincipal::default()))
            .with_state(state);
        for body in [
            "{",
            r#"{"name":42}"#,
            r#"{"name":"valid","sync":"invalid"}"#,
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sources/ingest")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = to_bytes(response.into_body(), 8192).await.expect("body");
            let error: Value = serde_json::from_slice(&bytes).expect("structured error");
            assert_eq!(error["code"], "VALIDATION_ERROR");
            assert!(error["message"]
                .as_str()
                .expect("message")
                .contains("invalid source ingest request"));
        }
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/ingest")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let bytes = to_bytes(response.into_body(), 8192).await.expect("body");
        let error: Value = serde_json::from_slice(&bytes).expect("structured error");
        assert_eq!(error["code"], "VALIDATION_ERROR");
    }

    #[tokio::test]
    async fn test_source_http_rejects_missing_or_blank_space_before_repository_operations() {
        use axum::body::{to_bytes, Body};
        use axum::http::Request;
        use tower::ServiceExt;

        let storage = StorageClient::new_embedded().await.expect("storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                String::new(),
                "tester".into(),
            ),
            SourceType::Manual,
            "missing-space".into(),
        );
        let source_json = serde_json::to_vec(&source).expect("source");
        for space_id in [None, Some(String::new()), Some(" \t".to_string())] {
            let app = source_routes()
                .layer(axum::Extension(AuthPrincipal {
                    space_id,
                    bypass_abac: true,
                    ..AuthPrincipal::default()
                }))
                .with_state(state.clone());
            let requests = [
                Request::builder()
                    .uri("/v1/sources/not-a-record-id")
                    .body(Body::empty())
                    .expect("read request"),
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources/search")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .expect("search request"),
                Request::builder()
                    .method("POST")
                    .uri("/v1/sources")
                    .header("content-type", "application/json")
                    .body(Body::from(source_json.clone()))
                    .expect("create request"),
            ];
            for request in requests {
                let response = app.clone().oneshot(request).await.expect("response");
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
                let bytes = to_bytes(response.into_body(), 8192).await.expect("body");
                let error: Value = serde_json::from_slice(&bytes).expect("structured error");
                assert_eq!(error["code"], "ACCESS_DENIED");
            }
        }
    }

    #[tokio::test]
    async fn test_source_only_reingest_preserves_acl_in_staging_and_create_conflict() {
        use elephant_types::records::base::SubjectsState;
        use elephant_types::Link;

        let storage = StorageClient::new_embedded().await.expect("storage");
        let state = SourcesState::new(storage.clone(), Arc::new(AuthzService::new()));
        let subject = RecordId::new_v4(TableName::Entity);
        let principal = AuthPrincipal {
            subject: "tester".into(),
            space_id: Some("default".into()),
            scopes: vec!["write:source".into()],
            security_level: 5,
            security_labels: vec!["private".into()],
            ..AuthPrincipal::default()
        };
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".into(),
                "tester".into(),
            ),
            SourceType::Manual,
            "source-only-acl".into(),
        );
        source.base.id = deterministic_source_id_for_ingest(&source);
        let incoming = source.clone();
        source.base.security.level = 5;
        source.base.security.labels = vec!["private".into()];
        source.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        source.base.subjects = vec![Link::new(subject)];
        source.base.subjects_state = SubjectsState::Complete;
        state
            .source_repo
            .create(source.clone())
            .await
            .expect("create");
        let (staged, previous) =
            stage_source_for_ingest(&state, &principal, incoming.clone(), None)
                .await
                .expect("stage");
        assert!(previous.is_some());
        assert_eq!(staged.base.security, source.base.security);
        assert_eq!(staged.base.provenance, source.base.provenance);
        assert_eq!(staged.base.subjects, source.base.subjects);

        // Force the race path: the preflight observed no record, but create finds one.
        let (persisted, previous) =
            create_or_reuse_source_candidate(&state, &principal, &incoming, false)
                .await
                .expect("reuse after conflict");
        assert!(previous.is_some());
        assert_eq!(persisted.base.security, source.base.security);
        assert_eq!(persisted.base.provenance, source.base.provenance);
        assert_eq!(persisted.base.subjects, source.base.subjects);
        assert_eq!(persisted.base.subjects_state, source.base.subjects_state);

        let mut explicit = incoming;
        explicit.base.security.level = 3;
        explicit.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        let (persisted, _) = create_or_reuse_source_candidate(&state, &principal, &explicit, false)
            .await
            .expect("explicit source update");
        assert_eq!(persisted.base.security, explicit.base.security);
        assert_eq!(persisted.base.provenance, explicit.base.provenance);

        let mut denied = explicit.clone();
        denied.base.security.level = 9;
        let result = create_or_reuse_source_candidate(&state, &principal, &denied, false).await;
        assert!(matches!(result, Err((StatusCode::FORBIDDEN, _))));
        let stored = state
            .source_repo
            .get("default", &explicit.base.id)
            .await
            .expect("read")
            .expect("source");
        assert_eq!(stored.base.security, explicit.base.security);
    }

    #[test]
    fn test_source_search_request_defaults() {
        let json = r"{}";
        let req: SourceSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
    }

    #[test]
    fn test_source_ingest_request_rejects_mixed_legacy_and_normalized_fields() {
        let json = r#"{
            "source": {
                "id": "source:00000000-0000-0000-0000-000000000000",
                "space_id": "default",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-01-01T00:00:00Z",
                "created_by": "test",
                "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "provenance": {"sources": [], "chain": []},
                "subjects_state": "complete",
                "type": "manual",
                "lifecycle_status": "discovered",
                "hash": "sha256:test",
                "metadata": {}
            },
            "title": "Normalized",
            "text": "Normalized body",
            "doc": {
                "id": "doc:00000000-0000-0000-0000-000000000000",
                "space_id": "default",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-01-01T00:00:00Z",
                "created_by": "test",
                "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "provenance": {"sources": [], "chain": []},
                "subjects_state": "complete",
                "title": "Legacy",
                "text": "Legacy body",
                "rendition_key": "legacy"
            }
        }"#;

        let decoded: Result<SourceIngestRequest, _> = serde_json::from_str(json);
        assert!(decoded.is_err(), "mixed request shapes should be rejected");
    }

    #[tokio::test]
    async fn test_ingest_source_accepts_normalized_staged_doc_shape() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(
            storage.clone(),
            Arc::new(AuthzService::new()),
        ));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: false,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let (_, Json(result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                Some("Operator reviewed the dashboard"),
                Vec::new(),
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("normalized source ingest");

        let doc_id = RecordId::from_str(&result.doc_id.expect("doc id")).expect("parse doc id");
        let persisted = elephant_storage::DocRepository::new(storage)
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Observed frame");
        assert_eq!(persisted.text, "Operator reviewed the dashboard");
        assert_eq!(
            persisted.metadata.get("collection"),
            Some(&serde_json::Value::String("frames".to_string()))
        );
        assert_eq!(
            persisted.metadata.get("content_type"),
            Some(&serde_json::Value::String("text/plain".to_string()))
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_uri_media_transport() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                None,
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    inline_base64: None,
                    path: None,
                    uri: Some("https://example.com/frame.jpg".to_string()),
                    duration_ms: None,
                }],
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("uri transport should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.message.contains("must use inline_base64"));
    }

    #[tokio::test]
    async fn test_legacy_ingest_source_rejects_path_or_uri_media_attachments() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "legacy-source".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Unsafe legacy source".to_string(),
            text: String::new(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Path {
                    path: "/tmp/frame.jpg".to_string(),
                },
                label: None,
                size_bytes: None,
                duration_ms: None,
                sha256: None,
            }],
            rendition_key: "legacy-source-unsafe".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                Some(doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("legacy path/uri media attachments should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.message.contains("must not use path or uri"));
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_inline_video_without_duration() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed clip"),
                None,
                vec![DocMediaInput {
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode(b"not-a-real-video")),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("clips"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("inline video without duration should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err
            .1
            .message
            .contains("duration_ms must be a positive integer"));
    }

    #[tokio::test]
    async fn test_ingest_source_accepts_browser_style_data_url_inline_media() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let payload = format!(
            "data:image/png;base64,{}",
            BASE64_STANDARD.encode([0_u8, 1, 2, 3])
        );

        let (status, Json(result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                None,
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/png".to_string(),
                    inline_base64: Some(payload),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("data url inline media should be accepted");

        assert_eq!(status, StatusCode::CREATED);
        assert!(result.doc_id.is_some());

        let doc_id = RecordId::from_str(&result.doc_id.expect("doc id")).expect("parse doc id");
        let persisted = elephant_storage::DocRepository::new(state.storage.clone())
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.metadata.get("content_type"),
            Some(&serde_json::Value::String("image/png".to_string()))
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_zero_duration_inline_video() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed clip"),
                None,
                vec![DocMediaInput {
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode(b"not-a-real-video")),
                    path: None,
                    uri: None,
                    duration_ms: Some(0),
                }],
                Some("clips"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("zero-duration inline video should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err
            .1
            .message
            .contains("duration_ms must be a positive integer"));
    }

    #[tokio::test]
    async fn test_ingest_source_same_name_with_distinct_content_creates_distinct_sources() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );
        source.external_id = Some("Observed frame".to_string());

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                Some("first observation"),
                Vec::new(),
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("first normalized source ingest");

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                Some("second observation"),
                Vec::new(),
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("second normalized source ingest");

        assert_ne!(
            first_result.source_id, second_result.source_id,
            "same display label but different content should not collapse to one source"
        );

        let sources = SourceRepository::new(state.storage.clone())
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 2);
    }

    #[tokio::test]
    async fn test_ingest_source_rolls_back_media_assets_when_authorization_fails() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string()],
            bypass_abac: false,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "normalized-source".to_string(),
        );

        let err = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                &source,
                Some("Observed frame"),
                None,
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/png".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode([0_u8, 1, 2, 3])),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("frames"),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("secured source/doc should reject source-first media ingest");

        assert_eq!(err.0, StatusCode::FORBIDDEN);

        let assets = MediaAssetRepository::new(state.storage.clone())
            .list("default", 10, None)
            .await
            .expect("list media assets");
        assert!(
            assets.is_empty(),
            "rejected source-first media ingest should not leak orphaned media assets"
        );
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_missing_space_id_before_cross_space_staging() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: None,
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "source-space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "doc-space".to_string(),
                "tester".to_string(),
            ),
            title: "Doc".to_string(),
            text: "Body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                Some(doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("missing space_id should fail before staging");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1 .0.message.contains("missing space_id"));
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_sync_mode() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "sync-test".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                None,
                Some(IngestMode::Metadata),
                true,
            )),
        )
        .await
        .expect_err("sync ingest should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err
            .1
            .message
            .contains("sync source ingest is not supported"));
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_missing_space_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: None,
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "missing-space".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect_err("missing space_id should be rejected");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.message.contains("missing space_id"));
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_empty_space_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("   ".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "empty-space".to_string(),
        );

        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect_err("empty space_id should be rejected");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.message.contains("missing space_id"));
    }

    #[tokio::test]
    async fn test_ingest_source_principal_space_override_allows_mismatched_payload_spaces() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "source-space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "mismatch".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "doc-space".to_string(),
                "tester".to_string(),
            ),
            title: "Doc".to_string(),
            text: "Body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let result = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                Some(doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("principal space should win over payload placeholder spaces");

        assert_eq!(result.0, StatusCode::CREATED);
        assert!(result.1.doc_id.is_some());
    }

    #[tokio::test]
    async fn test_ingest_source_reuses_existing_source_and_resets_lifecycle() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };
        let doc_id = RecordId::new_v4(TableName::Doc);

        let first_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash-1".to_string(),
        );
        let mut first_doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Doc".to_string(),
            text: "First".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        first_doc.base.security.level = 4;

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(
                first_source,
                Some(first_doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("first ingest");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        let mut existing_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        existing_source.base.security.level = 4;
        existing_source.lifecycle_status = SourceLifecycleStatus::GraphReady;
        existing_source.current_ingest_mode = Some(IngestMode::Graph);
        state
            .source_repo
            .update("default", &source_id, existing_source)
            .await
            .expect("promote source state");

        let second_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash-2".to_string(),
        );
        let mut second_doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Doc".to_string(),
            text: "Second".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"revision": 2}),
        };
        second_doc.base.security.level = 7;

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                second_source,
                Some(second_doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        assert_eq!(
            second_result.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(second_result.current_ingest_mode, None);

        let persisted_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("reload source")
            .expect("persisted source");
        assert_eq!(persisted_source.base.security.level, 7);
        assert_eq!(
            persisted_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(persisted_source.current_ingest_mode, None);

        let persisted_doc = elephant_storage::DocRepository::new(state.storage.clone())
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted_doc.base.security.level, 7);

        let sources = elephant_storage::SourceRepository::new(state.storage.clone())
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_source_updates_extraction_guidance_on_reuse() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let first_request =
            SourceIngestRequest::Normalized(Box::new(NormalizedSourceIngestRequest {
                name: "Example observation stream".to_string(),
                uri: Some("example://session/123/frame/1".to_string()),
                source_type: Some("device_frame".to_string()),
                title: None,
                text: None,
                media: Vec::new(),
                collection: None,
                metadata: None,
                rendition_key: None,
                extraction_guidance: ExtractionGuidanceInput {
                    extraction_skill: Some("extraction/example-observations-v1".to_string()),
                    extraction_purpose: None,
                    extraction_domain: None,
                },
                ingest_mode: Some(IngestMode::Metadata),
                sync: false,
            }));

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(first_request),
        )
        .await
        .expect("first ingest");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        let mut existing_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        existing_source.lifecycle_status = SourceLifecycleStatus::MetadataReady;
        existing_source.current_ingest_mode = Some(IngestMode::Metadata);
        state
            .source_repo
            .update("default", &source_id, existing_source)
            .await
            .expect("promote source state");

        let second_request =
            SourceIngestRequest::Normalized(Box::new(NormalizedSourceIngestRequest {
                name: "Example observation stream".to_string(),
                uri: Some("example://session/123/frame/1".to_string()),
                source_type: Some("device_frame".to_string()),
                title: None,
                text: None,
                media: Vec::new(),
                collection: None,
                metadata: None,
                rendition_key: None,
                extraction_guidance: ExtractionGuidanceInput {
                    extraction_skill: Some("extraction/example-observations-v2".to_string()),
                    extraction_purpose: Some("extract richer observation graph".to_string()),
                    extraction_domain: None,
                },
                ingest_mode: Some(IngestMode::Metadata),
                sync: false,
            }));

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(second_request),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        assert_eq!(
            second_result.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(second_result.current_ingest_mode, None);

        let persisted_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("reload source")
            .expect("persisted source");
        assert_eq!(
            persisted_source
                .metadata
                .get("extraction_skill")
                .and_then(serde_json::Value::as_str),
            Some("extraction/example-observations-v2")
        );
        assert_eq!(
            persisted_source
                .metadata
                .get("extraction_purpose")
                .and_then(serde_json::Value::as_str),
            Some("extract richer observation graph")
        );
        assert_eq!(
            persisted_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(persisted_source.current_ingest_mode, None);
    }

    #[tokio::test]
    async fn test_ingest_source_rejects_overwrite_of_secured_existing_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            ..AuthPrincipal::default()
        };
        let doc_id = RecordId::new_v4(TableName::Doc);

        let first_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash-1".to_string(),
        );
        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Doc".to_string(),
            text: "First".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security.level = 0;

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(
                first_source,
                Some(doc.clone()),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("first ingest");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        let mut existing_doc = elephant_storage::DocRepository::new(state.storage.clone())
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        existing_doc.base.security.level = 4;
        elephant_storage::DocRepository::new(state.storage.clone())
            .update("default", &doc_id, existing_doc)
            .await
            .expect("update doc security");
        let mut existing_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        existing_source.base.security.level = 4;
        state
            .source_repo
            .update("default", &source_id, existing_source)
            .await
            .expect("update source security");

        let second_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash-2".to_string(),
        );
        let err = ingest_source(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                second_source,
                Some(doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect_err("secured existing doc should block overwrite");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_ingest_source_without_doc_reuses_deterministic_source_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let first_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            "same-hash".to_string(),
        );

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(
                first_source,
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect("first ingest");

        let second_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            "same-hash".to_string(),
        );

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                second_source,
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        let sources = elephant_storage::SourceRepository::new(state.storage.clone())
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_source_without_doc_restores_soft_deleted_source() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            "same-hash".to_string(),
        );

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(
                source.clone(),
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect("first ingest");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        state
            .source_repo
            .delete("default", &source_id)
            .await
            .expect("delete source");

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                None,
                Some(IngestMode::Metadata),
                false,
            )),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        let restored = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("restored source");
        assert!(restored.base.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_ingest_source_with_doc_restores_soft_deleted_source() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(SourcesState::new(
            storage.clone(),
            Arc::new(AuthzService::new()),
        ));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("default".to_string()),
            scopes: vec!["write:source".to_string(), "write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let source_id = RecordId::new_v4(TableName::Source);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let source = Source::new(
            BaseFields::new(
                source_id.clone(),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "same-hash".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Doc".to_string(),
            text: "First".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let (_, Json(first_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(
                source.clone(),
                Some(doc.clone()),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("first ingest");

        state
            .source_repo
            .delete("default", &source_id)
            .await
            .expect("delete source");

        let (_, Json(second_result)) = ingest_source(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(
                source,
                Some(doc),
                Some(IngestMode::Graph),
                false,
            )),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        let restored = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("restored source");
        assert!(restored.base.deleted_at.is_none());
        let persisted_doc = elephant_storage::DocRepository::new(state.storage.clone())
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted_doc.source.as_ref().expect("source link").id(),
            &source_id
        );
    }
}
