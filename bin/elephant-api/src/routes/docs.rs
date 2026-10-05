//! Document API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/docs/search - Search documents
//! - POST /v1/docs/ingest - Ingest a new document
//! - GET /v1/docs/{id} - Get document by ID
//! - PUT /v1/docs/{id} - Update document
//! - DELETE /v1/docs/{id} - Soft delete document
//! - POST /v1/docs/{id}/restore - Restore soft-deleted document
//! - GET /v1/docs/collections - Get distinct collections
//! - POST /v1/docs/batch - Batch create/update documents

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;
use uuid::Uuid;

use elephant_embeddings::{
    discover_accessible_partition_keys, EmbeddingPolicy, EmbeddingProvider, EmbeddingProviderCache,
    PartitionKey,
};
use elephant_pipeline::scheduler::ingest_work_kinds::{
    enqueue_triage_source_work_item as enqueue_triage_source_work_item_for_source,
    INGEST_METADATA_KIND, TRIAGE_SOURCE_KIND, UPGRADE_INGEST_MODE_KIND,
};
use elephant_pipeline::scheduler::{
    resolve_extraction_guidance_status, COMMIT_ARTIFACTS_KIND, EXTRACT_ASSERTIONS_KIND,
    PLAN_EXTRACTION_KIND, RENDER_DOC_KIND, VALIDATE_ARTIFACTS_KIND,
};
use elephant_policy::RecordAttributes;
use elephant_storage::{
    find_existing_doc_for_doc, find_existing_source_for_doc, parse_id,
    persist_manual_doc_for_source_detailed, preview_persisted_manual_doc_for_source,
    rollback_persisted_manual_doc, rollback_persisted_source, update_existing_source_for_reingest,
    AssertionRepository, DocNodeRepository, DocRepository, DocRevisionRepository,
    MediaAssetRepository, NodeEmbeddingRepository, PersistedManualDoc, Repository, SearchCriteria,
    SearchRepository, SourceRepository, SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::ids::TableName;
use elephant_types::mcp_simple::ExtractionGuidanceInput;
use elephant_types::pipeline_status::{
    derive_extraction_state, ChunkStatus, DocPipelineStatus, ExtractionState,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc_node::DocNode;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::media_asset::{inline_media_max_bytes, managed_asset_relative_path};
use elephant_types::records::source::{
    apply_manual_doc_to_source, manual_doc_template_from_doc, prepare_source_for_reingest,
    ManualDocTemplate, SourceLifecycleStatus,
};
use elephant_types::records::work::WorkItemStatus;
use elephant_types::records::{Doc, Source, SourceType};
use elephant_types::records::{
    DocMediaAttachment, DocMediaInput, DocMediaKind, DocMediaLocation, MediaAsset,
};
use elephant_types::{BaseFieldsBuilder, RecordId};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::{ApiError, BatchError, BatchRequest, BatchResponse};
use crate::routes::ingest_guard::ensure_semantic_ingest_ready;
use crate::routes::require_space_id;
use crate::runtime::RuntimeManager;

/// Shared state for document handlers.
#[derive(Clone)]
pub struct DocsState {
    /// Storage client for database access.
    pub storage: StorageClient,
    /// Document repository.
    pub doc_repo: DocRepository,
    /// Partitioned embeddings repository.
    pub embedding_repo: NodeEmbeddingRepository,
    /// Space-level embedding configuration repository.
    pub space_config_repo: SpaceConfigRepository,
    /// Source repository (for extraction guidance status).
    pub source_repo: SourceRepository,
    /// Document revision repository.
    pub revision_repo: DocRevisionRepository,
    /// Document node repository.
    pub node_repo: DocNodeRepository,
    /// Work item repository (for pipeline status).
    pub work_item_repo: WorkItemRepository,
    /// Assertion repository (for pipeline status).
    pub assertion_repo: AssertionRepository,
    /// Embedding provider cache.
    pub provider_cache: EmbeddingProviderCache,
    /// Authorization service.
    pub authz: AuthzState,
    /// Optional managed runtime for semantic-ingest readiness checks.
    pub runtime: Option<Arc<RuntimeManager>>,
}

impl DocsState {
    /// Creates new document state from a storage client.
    #[must_use]
    pub fn new(client: &StorageClient, authz: AuthzState) -> Self {
        Self::new_with_runtime(client, authz, None)
    }

    #[must_use]
    pub fn new_with_runtime(
        client: &StorageClient,
        authz: AuthzState,
        runtime: Option<Arc<RuntimeManager>>,
    ) -> Self {
        let doc_repo = DocRepository::new(client.clone());
        let partition_manager = elephant_embeddings::PartitionManager::new();
        let embedding_repo = NodeEmbeddingRepository::new(client.clone(), partition_manager);
        let space_config_repo = SpaceConfigRepository::new(client.clone());
        let source_repo = SourceRepository::new(client.clone());
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let work_item_repo = WorkItemRepository::new(client.clone());
        let assertion_repo = AssertionRepository::new(client.clone());
        Self {
            storage: client.clone(),
            doc_repo,
            embedding_repo,
            space_config_repo,
            source_repo,
            revision_repo,
            node_repo,
            work_item_repo,
            assertion_repo,
            provider_cache: EmbeddingProviderCache::new(),
            authz,
            runtime,
        }
    }
}

/// Ingest request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestRequest {
    /// Normalized REST request using the planned media-aware input shape.
    Normalized(Box<NormalizedIngestRequest>),
    /// Legacy storage-shaped request kept for compatibility.
    Legacy(Box<LegacyIngestRequest>),
}

/// Normalized REST doc ingest request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedIngestRequest {
    /// Document title.
    pub title: String,
    /// Optional document text content or sidecar metadata for media docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Optional media attachments to ingest with the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<DocMediaInput>,
    /// Optional collection override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Optional metadata key-value pairs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional explicit rendition key for idempotency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendition_key: Option<String>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
    /// Optional collection override.
    #[serde(default)]
    pub sync: bool,
}

/// Legacy storage-shaped request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyIngestRequest {
    /// Document data.
    pub doc: Doc,
    /// Optional collection override.
    pub collection: Option<String>,
    /// Whether to run ingestion synchronously.
    ///
    /// Source-first doc ingest is currently asynchronous only.
    #[serde(default)]
    pub sync: bool,
}

/// Batch request item accepted by `/v1/docs/batch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchDocItem {
    /// Normalized REST request using the planned media-aware input shape.
    Normalized(Box<NormalizedBatchDocItem>),
    /// Legacy storage-shaped request body.
    Legacy(Box<Doc>),
}

/// Normalized batch item using the same public fields as single-doc ingest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedBatchDocItem {
    /// Document title.
    pub title: String,
    /// Optional document text content or sidecar metadata for media docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Optional media attachments to ingest with the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<DocMediaInput>,
    /// Optional collection override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Optional metadata key-value pairs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    /// Optional explicit rendition key for idempotency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendition_key: Option<String>,
    /// Optional extraction guidance override.
    #[serde(flatten)]
    pub extraction_guidance: ExtractionGuidanceInput,
}

/// Document search request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSearchRequest {
    /// Free-text query.
    #[serde(default)]
    pub query: Option<String>,
    /// Number of top-k similarity results (for vector search).
    #[serde(default)]
    pub k: Option<usize>,
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted documents.
    #[serde(default)]
    pub include_deleted: bool,
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Document query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct DocQuery {
    /// Include soft-deleted documents.
    #[serde(default)]
    pub include_deleted: bool,
}

const fn default_limit() -> usize {
    50
}

fn normalize_optional_text(text: Option<String>) -> Option<String> {
    text.filter(|value| !value.trim().is_empty())
}

fn decode_inline_media(index: usize, payload: &str) -> Result<Vec<u8>, ApiError> {
    let encoded = payload
        .split_once(',')
        .filter(|(prefix, _)| prefix.contains(";base64"))
        .map_or(payload, |(_, data)| data)
        .trim();

    BASE64_STANDARD.decode(encoded).map_err(|e| {
        ApiError::validation(format!(
            "media[{index}].inline_base64 is not valid base64: {e}"
        ))
    })
}

fn inferred_content_type(media: &[DocMediaInput]) -> String {
    media
        .first()
        .map_or_else(|| "text/plain".to_string(), |item| item.media_type.clone())
}

fn validate_public_media_inputs(inputs: &[DocMediaInput]) -> Result<(), ApiError> {
    for (index, media) in inputs.iter().enumerate() {
        if media.media_type.trim().is_empty() {
            return Err(ApiError::validation(format!(
                "media[{index}].media_type cannot be empty"
            )));
        }
        if media.transport_count() != 1 {
            return Err(ApiError::validation(format!(
                "media[{index}] must specify exactly one media transport"
            )));
        }
        if media.path.is_some() || media.uri.is_some() {
            return Err(ApiError::validation(format!(
                "media[{index}] must use inline_base64; public ingest does not accept path or uri media transports"
            )));
        }
        if matches!(media.kind, DocMediaKind::Video) && media.inline_base64.is_some() {
            match media.duration_ms {
                Some(duration_ms) if duration_ms > 0 => {}
                _ => {
                    return Err(ApiError::validation(format!(
                        "media[{index}].duration_ms must be a positive integer for inline video payloads"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_public_media_attachments(attachments: &[DocMediaAttachment]) -> Result<(), ApiError> {
    for (index, attachment) in attachments.iter().enumerate() {
        match &attachment.location {
            DocMediaLocation::ManagedAsset { .. } => {}
            DocMediaLocation::Path { .. } | DocMediaLocation::Uri { .. } => {
                return Err(ApiError::validation(format!(
                    "media_attachments[{index}] must not use path or uri transports on public ingest surfaces"
                )));
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct PreparedMediaAttachments {
    attachments: Vec<DocMediaAttachment>,
    assets: Vec<MediaAsset>,
}

#[derive(Debug, Clone)]
struct PreparedDoc {
    doc: Doc,
    pending_assets: Vec<MediaAsset>,
}

fn prepare_inline_media_asset(
    space_id: &str,
    media: &DocMediaInput,
    index: usize,
) -> Result<(DocMediaAttachment, MediaAsset), ApiError> {
    let payload = media.inline_base64.as_deref().ok_or_else(|| {
        ApiError::validation(format!("media[{index}] missing inline_base64 payload"))
    })?;
    let bytes = decode_inline_media(index, payload)?;
    prepare_managed_media_asset_bytes(space_id, media, index, &bytes)
}

fn prepare_managed_media_asset_bytes(
    space_id: &str,
    media: &DocMediaInput,
    index: usize,
    bytes: &[u8],
) -> Result<(DocMediaAttachment, MediaAsset), ApiError> {
    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let storage_path = managed_asset_relative_path(&asset_id);
    let size_bytes = u64::try_from(bytes.len())
        .map_err(|_| ApiError::validation(format!("media[{index}] payload is too large")))?;
    let max_inline_bytes = inline_media_max_bytes(media.kind);
    if size_bytes > max_inline_bytes {
        return Err(ApiError::validation(format!(
            "media[{index}] inline payload exceeds configured limit of {max_inline_bytes} bytes"
        )));
    }
    let sha256 = hex::encode(Sha256::digest(bytes));

    let mut asset = MediaAsset {
        base: BaseFieldsBuilder::new(space_id)
            .for_table(TableName::MediaAsset)
            .build(),
        kind: media.kind,
        media_type: media.media_type.clone(),
        storage_path,
        size_bytes,
        sha256: sha256.clone(),
        payload_base64: Some(BASE64_STANDARD.encode(bytes)),
        duration_ms: media.duration_ms,
        label: None,
    };
    asset.base.id = asset_id;
    let asset_id_string = asset.base.id.to_string();

    Ok((
        DocMediaAttachment {
            attachment_id: format!("attachment-{}", Uuid::new_v4()),
            kind: media.kind,
            media_type: media.media_type.clone(),
            location: DocMediaLocation::ManagedAsset {
                asset_id: asset_id_string,
            },
            label: None,
            size_bytes: Some(size_bytes),
            duration_ms: media.duration_ms,
            sha256: Some(sha256),
        },
        asset,
    ))
}

fn build_media_attachments(
    space_id: &str,
    inputs: &[DocMediaInput],
) -> Result<PreparedMediaAttachments, ApiError> {
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
) -> Result<(), ApiError> {
    let repo = MediaAssetRepository::new(storage.clone());
    let mut persisted_ids = Vec::with_capacity(assets.len());
    for asset in assets {
        match repo.create(asset.clone()).await {
            Ok(_) => persisted_ids.push(asset.base.id.clone()),
            Err(error) => {
                rollback_media_assets_by_ids(storage, &asset.base.space_id, &persisted_ids).await;
                return Err(ApiError::internal(format!(
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
        if let Err(err) = repo.delete(space_id, asset_id).await {
            warn!(
                asset_id = %asset_id,
                error = %err,
                "failed to roll back staged media asset after ingest failure"
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

impl NormalizedIngestRequest {
    fn into_doc(self, principal: &AuthPrincipal) -> Result<PreparedDoc, ApiError> {
        let normalized_text = normalize_optional_text(self.text.clone());
        let has_media = !self.media.is_empty();
        if !has_media && normalized_text.is_none() {
            return Err(ApiError::validation(
                "ingest requires non-empty text or at least one media attachment".to_string(),
            ));
        }

        let space_id = require_space_id(principal).map_err(|(_, err)| err.0)?;
        let prepared_media = build_media_attachments(&space_id, &self.media)?;
        let mut metadata = self
            .metadata
            .map_or_else(serde_json::Map::new, std::convert::identity);
        if let Some(collection) = self.collection {
            metadata.insert(
                "collection".to_string(),
                serde_json::Value::String(collection),
            );
        }
        metadata.insert(
            "content_type".to_string(),
            serde_json::Value::String(inferred_content_type(&self.media)),
        );
        self.extraction_guidance.apply_to_metadata(&mut metadata);

        Ok(PreparedDoc {
            doc: Doc {
                base: BaseFieldsBuilder::new(space_id)
                    .for_table(TableName::Doc)
                    .created_by(principal.subject.clone())
                    .build(),
                title: self.title,
                text: if has_media {
                    String::new()
                } else {
                    normalized_text.clone().unwrap_or_default()
                },
                sidecar_text: has_media.then_some(normalized_text).flatten(),
                source: None,
                media_attachments: prepared_media.attachments,
                rendition_key: self
                    .rendition_key
                    .unwrap_or_else(|| format!("api-{}", Uuid::new_v4())),
                metadata: serde_json::Value::Object(metadata),
            },
            pending_assets: prepared_media.assets,
        })
    }
}

impl IngestRequest {
    const fn sync(&self) -> bool {
        match self {
            Self::Normalized(req) => req.sync,
            Self::Legacy(req) => req.sync,
        }
    }

    fn into_doc(self, principal: &AuthPrincipal) -> Result<PreparedDoc, ApiError> {
        match self {
            Self::Normalized(req) => req.into_doc(principal),
            Self::Legacy(req) => {
                let LegacyIngestRequest {
                    mut doc,
                    collection,
                    sync: _,
                } = *req;
                validate_public_media_attachments(&doc.media_attachments)?;
                if let Some(collection) = collection {
                    let mut metadata = match doc.metadata {
                        serde_json::Value::Object(map) => map,
                        other => {
                            let mut map = serde_json::Map::new();
                            if !other.is_null() {
                                map.insert("_legacy_metadata".to_string(), other);
                            }
                            map
                        }
                    };
                    metadata.insert(
                        "collection".to_string(),
                        serde_json::Value::String(collection),
                    );
                    doc.metadata = serde_json::Value::Object(metadata);
                }
                Ok(PreparedDoc {
                    doc,
                    pending_assets: Vec::new(),
                })
            }
        }
    }
}

impl BatchDocItem {
    fn into_doc(self, principal: &AuthPrincipal) -> Result<PreparedDoc, ApiError> {
        match self {
            Self::Legacy(doc) => {
                validate_public_media_attachments(&doc.media_attachments)?;
                Ok(PreparedDoc {
                    doc: *doc,
                    pending_assets: Vec::new(),
                })
            }
            Self::Normalized(item) => NormalizedIngestRequest {
                title: item.title,
                text: item.text,
                media: item.media,
                collection: item.collection,
                metadata: item.metadata,
                rendition_key: item.rendition_key,
                extraction_guidance: item.extraction_guidance,
                sync: false,
            }
            .into_doc(principal),
        }
    }
}

const SEMANTIC_CANDIDATE_MULTIPLIER: usize = 8;
const SEMANTIC_MIN_CANDIDATES: usize = 24;
const TEXT_FALLBACK_CURSOR_PREFIX: &str = "text:";
const DOC_SEARCH_SCOPE: &str = "read:doc:metadata";
const MAX_VISIBLE_TEXT_SEARCH_PAGES: usize = 1024;

/// Search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSearchResponse {
    /// Search results.
    pub results: Vec<Doc>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count. Exact for text search; in semantic mode a lower bound
    /// (the search stops once the requested page is settled).
    pub total: usize,
    /// Retrieval mode used to produce these results.
    pub mode: String,
}

fn semantic_page_metadata<T>(
    results: Vec<T>,
    offset: usize,
    limit: usize,
) -> (Vec<T>, usize, Option<String>, bool) {
    let total = results.len();
    if limit == 0 {
        return (Vec::new(), total, None, false);
    }
    let page: Vec<T> = results.into_iter().skip(offset).take(limit).collect();
    let has_more = offset + page.len() < total;
    let cursor = has_more.then(|| (offset + page.len()).to_string());
    (page, total, cursor, has_more)
}

const fn semantic_empty_page_response(_offset: usize) -> DocSearchResponse {
    DocSearchResponse {
        results: Vec::new(),
        cursor: None,
        has_more: false,
        total: 0,
        mode: String::new(),
    }
}

fn semantic_text_fallback_cursor(cursor: Option<&str>) -> Option<String> {
    cursor.and_then(|value| {
        value
            .strip_prefix(TEXT_FALLBACK_CURSOR_PREFIX)
            .map(str::to_string)
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_text_search_for_semantic_fallback(
    state: &DocsState,
    principal: &AuthPrincipal,
    space_id: &str,
    query: &str,
    limit: usize,
    k: Option<usize>,
    include_deleted: bool,
    cursor: Option<String>,
) -> ApiResult<DocSearchResponse> {
    let Json(mut response) = run_text_search(
        state,
        principal,
        space_id,
        Some(query),
        limit,
        k,
        include_deleted,
        cursor,
    )
    .await?;
    response.cursor = response
        .cursor
        .map(|value| format!("{TEXT_FALLBACK_CURSOR_PREFIX}{value}"));
    response.mode = "lexical-fallback".to_string();
    Ok(Json(response))
}

fn build_text_search_criteria(
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
        criteria = criteria.with_query(query);
    }
    if include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(cursor) = cursor {
        criteria = criteria.with_cursor(cursor);
    }

    criteria
}

fn doc_search_preflight_record(space_id: &str, policy_id: &str) -> RecordAttributes {
    RecordAttributes {
        space_id: space_id.to_string(),
        security: Security {
            policy_id: policy_id.to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        },
        subjects: Vec::new(),
        subjects_state: SubjectsState::Complete,
    }
}

#[allow(clippy::result_large_err)]
fn authorize_doc_search(
    state: &DocsState,
    principal: &AuthPrincipal,
    space_id: &str,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let policy_id = principal
        .clearances
        .first()
        .map_or("default", |clearance| clearance.policy_id.as_str());
    let record = doc_search_preflight_record(space_id, policy_id);
    state
        .authz
        .evaluate(principal, &record, DOC_SEARCH_SCOPE)
        .map_err(|reason| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: "ACCESS_DENIED".to_string(),
                    message: reason.message,
                    details: None,
                }),
            )
        })
}

const fn should_rerun_text_search_after_authz(
    raw_page_len: usize,
    filtered_page_len: usize,
    _has_more: bool,
    bypass_abac: bool,
) -> bool {
    !bypass_abac && filtered_page_len < raw_page_len
}

async fn rerun_text_search_after_authz_filter(
    state: &DocsState,
    principal: &AuthPrincipal,
    space_id: &str,
    query: Option<&str>,
    limit: usize,
    include_deleted: bool,
    start_cursor: Option<String>,
) -> Result<DocSearchResponse, (StatusCode, Json<ApiError>)> {
    let mut visible = Vec::new();
    let total_visible =
        count_visible_text_search_results(state, principal, space_id, query, include_deleted)
            .await?;
    let mut scan_cursor = start_cursor;
    let mut next_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_TEXT_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Text search exceeded the maximum page scan bound",
                )),
            ));
        }
        let criteria =
            build_text_search_criteria(space_id, query, include_deleted, scan_cursor.clone(), 1);
        let result = state.doc_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let page_cursor = scan_cursor.clone();
        let result_cursor = result.cursor.clone();

        let Some(doc) = result.results.into_iter().next() else {
            break;
        };

        let allowed = state
            .authz
            .filter_records(principal, vec![doc], DOC_SEARCH_SCOPE);
        if let Some(doc) = allowed.into_iter().next() {
            if visible.len() < limit {
                visible.push(doc);
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
                    "Text search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_scan_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Text search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_scan_cursor);
    }

    let has_more = next_cursor.is_some();
    Ok(DocSearchResponse {
        results: visible,
        cursor: next_cursor,
        has_more,
        total: total_visible,
        mode: "text".to_string(),
    })
}

async fn count_visible_text_search_results(
    state: &DocsState,
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
        if pages > MAX_VISIBLE_TEXT_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Text search exceeded the maximum page scan bound",
                )),
            ));
        }
        let criteria = build_text_search_criteria(space_id, query, include_deleted, scan_cursor, 1);
        let result = state.doc_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let Some(doc) = result.results.into_iter().next() else {
            break;
        };

        if !state
            .authz
            .filter_records(principal, vec![doc], DOC_SEARCH_SCOPE)
            .is_empty()
        {
            total_visible += 1;
        }

        if !result.has_more {
            break;
        }

        let Some(next_cursor) = result.cursor else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Text search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Text search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_cursor);
    }

    Ok(total_visible)
}

const fn semantic_page_limit(limit: usize, k: Option<usize>) -> usize {
    match k {
        Some(k) if k < limit => k,
        Some(_) | None => limit,
    }
}

fn semantic_page_offset(cursor: Option<&str>) -> Result<usize, ApiError> {
    cursor.map_or_else(
        || Ok(0),
        |cursor| {
            if cursor.starts_with(TEXT_FALLBACK_CURSOR_PREFIX) {
                return Err(ApiError::validation(
                    "Text-fallback cursors are not valid semantic offsets",
                ));
            }
            cursor.parse::<usize>().map_err(|_| {
                ApiError::validation(
                    "Semantic search cursors must be numeric offsets returned by the API",
                )
            })
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticEmptyResultDisposition {
    FallbackToText,
    EmptyPage,
    ServiceUnavailable,
}

const fn semantic_empty_result_disposition(
    offset: usize,
    had_partition_error: bool,
) -> SemanticEmptyResultDisposition {
    if offset == 0 {
        return SemanticEmptyResultDisposition::FallbackToText;
    }

    if had_partition_error {
        SemanticEmptyResultDisposition::ServiceUnavailable
    } else {
        SemanticEmptyResultDisposition::EmptyPage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticExpansionAction {
    ReturnPage,
    ExpandCandidates,
    ServiceUnavailable,
}

/// `may_have_more` means a partition filled its window AND the requested page
/// is not settled yet (see `elephant_storage::semantic_window_settled`).
const fn semantic_expansion_action(
    had_partition_error: bool,
    may_have_more: bool,
) -> SemanticExpansionAction {
    if had_partition_error {
        SemanticExpansionAction::ServiceUnavailable
    } else if may_have_more {
        SemanticExpansionAction::ExpandCandidates
    } else {
        SemanticExpansionAction::ReturnPage
    }
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

fn work_item_matches_revision(
    item: &elephant_types::records::work::WorkItem,
    revision_str: &str,
) -> bool {
    matches!(
        item.kind.as_str(),
        PLAN_EXTRACTION_KIND | EXTRACT_ASSERTIONS_KIND
    ) && item
        .payload
        .get("revision_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value == revision_str)
}

fn work_item_matches_doc_render(
    item: &elephant_types::records::work::WorkItem,
    doc_id_str: &str,
    rendition_key: &str,
) -> bool {
    if item.kind != RENDER_DOC_KIND {
        return false;
    }

    let doc_matches = item
        .payload
        .get("doc_id")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value == doc_id_str);

    if !doc_matches {
        return false;
    }

    item.payload
        .get("rendition_key")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value == rendition_key)
}

fn work_item_matches_source_lifecycle(
    item: &elephant_types::records::work::WorkItem,
    source_id_str: &str,
) -> bool {
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
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value == source_id_str)
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

/// POST /v1/docs/search - Search documents.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn search_docs(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<DocSearchRequest>,
) -> ApiResult<DocSearchResponse> {
    let space_id = require_space_id(&principal)?;

    if let Some(req_space_id) = &req.space_id {
        if req_space_id != &space_id {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access to space denied")),
            ));
        }
    }

    authorize_doc_search(state.as_ref(), &principal, &space_id)?;

    if let (Some(query), Some(_)) = (req.query.clone(), req.k) {
        if let Some(text_cursor) = semantic_text_fallback_cursor(req.cursor.as_deref()) {
            return run_text_search_for_semantic_fallback(
                state.as_ref(),
                &principal,
                &space_id,
                &query,
                req.limit,
                req.k,
                req.include_deleted,
                Some(text_cursor),
            )
            .await;
        }

        let page_limit = semantic_page_limit(req.limit, req.k);
        let semantic_offset = match semantic_page_offset(req.cursor.as_deref()) {
            Ok(offset) => offset,
            Err(error) => return Err((StatusCode::BAD_REQUEST, Json(error))),
        };
        let requested_visible = semantic_offset.saturating_add(page_limit);
        let mut candidate_limit =
            semantic_candidate_limit(requested_visible.max(page_limit).max(1));
        let policy = resolve_embedding_policy(state.as_ref(), &space_id).await?;
        let provider = state
            .provider_cache
            .get_or_create(policy.clone(), build_embedding_provider)
            .await;

        let embeddings = match provider.embed(std::slice::from_ref(&query)).await {
            Ok(embeddings) => embeddings,
            Err(err) => {
                warn!("Embedding provider failed, falling back to text search: {err}");
                return run_text_search_for_semantic_fallback(
                    state.as_ref(),
                    &principal,
                    &space_id,
                    &query,
                    req.limit,
                    req.k,
                    req.include_deleted,
                    req.cursor,
                )
                .await;
            }
        };

        let Some(vector) = embeddings.into_iter().next() else {
            warn!("Embedding provider returned no vectors; falling back to text search");
            return run_text_search_for_semantic_fallback(
                state.as_ref(),
                &principal,
                &space_id,
                &query,
                req.limit,
                req.k,
                req.include_deleted,
                req.cursor,
            )
            .await;
        };

        let keys = build_partition_keys(&state.storage, &policy, &principal).await;

        loop {
            let mut best_scores: HashMap<RecordId, f64> = HashMap::new();
            let mut had_partition_error = false;
            let mut may_have_more = false;
            let mut full_partition_floor: Option<f64> = None;

            for key in &keys {
                match state
                    .embedding_repo
                    .search_partition_window(
                        key,
                        &space_id,
                        &vector,
                        candidate_limit,
                        None,
                        req.include_deleted,
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
                match semantic_empty_result_disposition(semantic_offset, had_partition_error) {
                    SemanticEmptyResultDisposition::FallbackToText => {
                        if had_partition_error {
                            warn!("Vector search failed for all partitions; falling back to text search");
                        }

                        return run_text_search_for_semantic_fallback(
                            state.as_ref(),
                            &principal,
                            &space_id,
                            &query,
                            req.limit,
                            req.k,
                            req.include_deleted,
                            req.cursor,
                        )
                        .await;
                    }
                    SemanticEmptyResultDisposition::EmptyPage => {
                        let mut response = semantic_empty_page_response(semantic_offset);
                        response.mode = "semantic".to_string();
                        return Ok(Json(response));
                    }
                    SemanticEmptyResultDisposition::ServiceUnavailable => {
                        return Err((
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(ApiError::internal(
                                "Semantic search is temporarily unavailable for paginated results; retry from the first page or try again later",
                            )),
                        ));
                    }
                }
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

            let docs = state
                .doc_repo
                .get_by_ids(&space_id, &ordered_ids, req.include_deleted)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?;

            let mut doc_map: HashMap<RecordId, Doc> = docs
                .into_iter()
                .map(|doc| (doc.base.id.clone(), doc))
                .collect();
            let ordered_docs: Vec<Doc> = ordered_ids
                .into_iter()
                .filter_map(|id| doc_map.remove(&id))
                .collect();

            let filtered = state
                .authz
                .filter_records(&principal, ordered_docs, DOC_SEARCH_SCOPE);

            // The page needs `offset + limit` documents, plus one more to know
            // whether a next page exists.
            let needed = semantic_offset.saturating_add(page_limit).saturating_add(1);
            let nth_visible_score = needed
                .checked_sub(1)
                .and_then(|index| filtered.get(index))
                .and_then(|doc| doc_scores.get(&doc.base.id).copied());
            let settled = elephant_storage::semantic_window_settled(
                filtered.len(),
                needed,
                nth_visible_score,
                full_partition_floor,
            );

            let (filtered, total, cursor, has_more) =
                semantic_page_metadata(filtered, semantic_offset, page_limit);

            match semantic_expansion_action(had_partition_error, may_have_more && !settled) {
                SemanticExpansionAction::ReturnPage => {
                    return Ok(Json(DocSearchResponse {
                        results: filtered,
                        cursor,
                        has_more,
                        total,
                        mode: "semantic".to_string(),
                    }));
                }
                SemanticExpansionAction::ServiceUnavailable => {
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(ApiError::internal(
                            "Semantic search is temporarily unavailable; one or more vector partitions failed",
                        )),
                    ));
                }
                SemanticExpansionAction::ExpandCandidates => {
                    let next_candidate_limit = candidate_limit.saturating_mul(2);
                    if next_candidate_limit == candidate_limit {
                        return Ok(Json(DocSearchResponse {
                            results: filtered,
                            cursor,
                            has_more,
                            total,
                            mode: "semantic".to_string(),
                        }));
                    }
                    candidate_limit = next_candidate_limit;
                }
            }
        }
    }

    run_text_search(
        state.as_ref(),
        &principal,
        &space_id,
        req.query.as_deref(),
        req.limit,
        req.k,
        req.include_deleted,
        req.cursor,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_text_search(
    state: &DocsState,
    principal: &AuthPrincipal,
    space_id: &str,
    query: Option<&str>,
    limit: usize,
    k: Option<usize>,
    include_deleted: bool,
    cursor: Option<String>,
) -> ApiResult<DocSearchResponse> {
    let effective_limit = semantic_page_limit(limit, k);
    let criteria = build_text_search_criteria(
        space_id,
        query,
        include_deleted,
        cursor.clone(),
        effective_limit,
    );

    let result = state.doc_repo.search(&criteria).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    let raw_page_len = result.results.len();
    let filtered = state
        .authz
        .filter_records(principal, result.results, DOC_SEARCH_SCOPE);

    if should_rerun_text_search_after_authz(
        raw_page_len,
        filtered.len(),
        result.has_more,
        principal.bypass_abac,
    ) {
        let response = rerun_text_search_after_authz_filter(
            state,
            principal,
            space_id,
            query,
            effective_limit,
            include_deleted,
            cursor,
        )
        .await?;
        return Ok(Json(response));
    }

    Ok(Json(DocSearchResponse {
        results: filtered,
        cursor: result.cursor,
        has_more: result.has_more,
        total: result.total,
        mode: "text".to_string(),
    }))
}

async fn resolve_embedding_policy(
    state: &DocsState,
    space_id: &str,
) -> Result<EmbeddingPolicy, (StatusCode, Json<ApiError>)> {
    let policy = match state.space_config_repo.get_by_space_id(space_id).await {
        Ok(Some(config)) => EmbeddingPolicy::from_space_config(&config),
        Ok(None) => EmbeddingPolicy::from_env(),
        Err(err) => {
            warn!("Failed to load space_config, falling back to env: {err}");
            EmbeddingPolicy::from_env()
        }
    };

    if policy.dims == 0 {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(
                "Embedding policy has invalid dimensions".to_string(),
            )),
        ));
    }

    Ok(policy)
}

fn build_embedding_provider(policy: &EmbeddingPolicy) -> Arc<dyn EmbeddingProvider> {
    elephant_embeddings::provider_for_policy(policy)
}

async fn build_partition_keys(
    storage: &StorageClient,
    policy: &EmbeddingPolicy,
    principal: &AuthPrincipal,
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

    discover_accessible_partition_keys(
        &tables,
        policy,
        &clearances,
        "default",
        principal.security_level,
    )
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

/// Ingest result returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResult {
    /// The document ID.
    pub doc_id: String,
    /// The source ID driving the source-first lifecycle.
    pub source_id: String,
    /// Stable lifecycle state after ingest submission.
    pub lifecycle_status: SourceLifecycleStatus,
    /// Highest successfully achieved ingest mode so far.
    pub current_ingest_mode: Option<IngestMode>,
    /// Enqueued work item ID.
    pub work_item_id: Option<String>,
    /// Whether source-first ingest work was enqueued.
    pub enqueued: bool,
}

fn build_manual_source_for_doc(doc: &Doc) -> Result<(Source, ManualDocTemplate), ApiError> {
    let mut source_base = BaseFields::new(
        RecordId::new_v4(TableName::Source),
        doc.base.space_id.clone(),
        doc.base.created_by.clone(),
    );
    source_base.security = doc.base.security.clone();
    source_base.provenance = doc.base.provenance.clone();
    source_base.subjects.clone_from(&doc.base.subjects);
    source_base.subjects_state = doc.base.subjects_state;

    let template = manual_doc_template_from_doc(doc);

    let mut source = Source::new(
        source_base,
        SourceType::Manual,
        format!("manual-doc:{}", doc.base.id),
    );
    source.external_id = Some(doc.base.id.to_string());
    source.author = Some(doc.base.created_by.clone());
    source.metadata = doc.metadata.clone();
    source
        .set_manual_doc_template(&template)
        .map_err(|e| ApiError::validation(e.to_string()))?;
    source.set_requested_ingest_mode(IngestMode::Graph);

    Ok((source, template))
}

async fn stage_source_for_manual_doc(
    state: &DocsState,
    principal: &AuthPrincipal,
    doc: &Doc,
) -> Result<(Source, Option<Source>), (StatusCode, Json<ApiError>)> {
    stage_source_for_manual_doc_with_authz(state, principal, doc, true).await
}

async fn stage_source_for_manual_doc_with_authz(
    state: &DocsState,
    principal: &AuthPrincipal,
    doc: &Doc,
    require_source_write: bool,
) -> Result<(Source, Option<Source>), (StatusCode, Json<ApiError>)> {
    if let Some(existing_doc) = find_existing_doc_for_doc(&state.storage, doc)
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
    }

    if let Some(existing_source) = find_existing_source_for_doc(&state.storage, doc)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
    {
        if require_source_write {
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
        }
        let mut updated_source = existing_source.clone();
        apply_manual_doc_to_source(&mut updated_source, doc).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(e.to_string())),
            )
        })?;
        updated_source.external_id = Some(doc.base.id.to_string());
        updated_source.set_requested_ingest_mode(IngestMode::Graph);
        let updated_source = prepare_source_for_reingest(&existing_source, updated_source);
        if require_source_write {
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
        }
        return Ok((updated_source, Some(existing_source)));
    }

    let (source, _template) =
        build_manual_source_for_doc(doc).map_err(|e| (StatusCode::BAD_REQUEST, Json(e)))?;
    if require_source_write {
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
    }
    Ok((source, None))
}

async fn authorize_persisted_manual_doc_write(
    state: &DocsState,
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

async fn rollback_staged_source_and_doc(
    state: &DocsState,
    source: &Source,
    previous_source: Option<&Source>,
    staged_doc: &PersistedManualDoc,
) {
    if let Err(err) = rollback_persisted_manual_doc(&state.storage, staged_doc).await {
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

async fn persist_staged_manual_source_and_doc(
    state: &DocsState,
    source_candidate: Source,
    previous_source: Option<Source>,
    doc: Doc,
) -> Result<(Source, PersistedManualDoc), (StatusCode, Json<ApiError>)> {
    if let Some(previous_source) = previous_source {
        let updated_source = update_existing_source_for_reingest(
            &state.source_repo,
            &previous_source,
            source_candidate,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
        match persist_manual_doc_for_source_detailed(&state.storage, &updated_source, doc).await {
            Ok(staged_doc) => Ok((updated_source, staged_doc)),
            Err(err) => {
                if let Err(rollback_err) = rollback_persisted_source(
                    &state.storage,
                    &updated_source,
                    Some(&previous_source),
                )
                .await
                {
                    warn!(
                        source_id = %previous_source.base.id,
                        error = %err,
                        rollback_error = %rollback_err,
                        "failed to roll back source after manual doc persistence error"
                    );
                }
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(err.to_string())),
                ))
            }
        }
    } else {
        let created_source = state
            .source_repo
            .create(source_candidate)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        match persist_manual_doc_for_source_detailed(&state.storage, &created_source, doc).await {
            Ok(staged_doc) => Ok((created_source, staged_doc)),
            Err(err) => {
                if let Err(cleanup_err) =
                    rollback_persisted_source(&state.storage, &created_source, None).await
                {
                    warn!(
                        source_id = %created_source.base.id,
                        error = %err,
                        cleanup_error = %cleanup_err,
                        "failed to roll back source after manual doc persistence error"
                    );
                }
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(err.to_string())),
                ))
            }
        }
    }
}

async fn stage_and_enqueue_manual_doc(
    state: &DocsState,
    principal: &AuthPrincipal,
    doc: Doc,
) -> Result<(Source, PersistedManualDoc, RecordId), (StatusCode, Json<ApiError>)> {
    let (source_candidate, previous_source) =
        stage_source_for_manual_doc(state, principal, &doc).await?;
    authorize_persisted_manual_doc_write(state, principal, &source_candidate, &doc).await?;
    let rollback_source = previous_source.clone();
    let (staged_source, staged_doc) = Box::pin(persist_staged_manual_source_and_doc(
        state,
        source_candidate,
        previous_source,
        doc,
    ))
    .await?;
    let work_item_id = match enqueue_triage_source_work_item_for_source(
        &state.storage,
        &staged_source,
        &principal.subject,
        5,
    )
    .await
    {
        Ok(work_item_id) => work_item_id,
        Err(e) => {
            rollback_staged_source_and_doc(
                state,
                &staged_source,
                rollback_source.as_ref(),
                &staged_doc,
            )
            .await;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ));
        }
    };

    Ok((staged_source, staged_doc, work_item_id))
}

async fn stage_and_enqueue_manual_doc_doc_surface(
    state: &DocsState,
    principal: &AuthPrincipal,
    doc: Doc,
) -> Result<(Source, PersistedManualDoc, RecordId), (StatusCode, Json<ApiError>)> {
    let (source_candidate, previous_source) =
        stage_source_for_manual_doc_with_authz(state, principal, &doc, false).await?;
    authorize_persisted_manual_doc_write(state, principal, &source_candidate, &doc).await?;
    let rollback_source = previous_source.clone();
    let (staged_source, staged_doc) = Box::pin(persist_staged_manual_source_and_doc(
        state,
        source_candidate,
        previous_source,
        doc,
    ))
    .await?;
    let work_item_id = match enqueue_triage_source_work_item_for_source(
        &state.storage,
        &staged_source,
        &principal.subject,
        5,
    )
    .await
    {
        Ok(work_item_id) => work_item_id,
        Err(e) => {
            rollback_staged_source_and_doc(
                state,
                &staged_source,
                rollback_source.as_ref(),
                &staged_doc,
            )
            .await;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ));
        }
    };

    Ok((staged_source, staged_doc, work_item_id))
}

/// POST /v1/docs/ingest - Ingest a new document.
#[allow(clippy::similar_names, clippy::too_many_lines)]
async fn ingest_doc(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<IngestRequest>,
) -> Result<(StatusCode, Json<IngestResult>), (StatusCode, Json<ApiError>)> {
    ensure_semantic_ingest_ready(state.runtime.as_ref()).await?;
    if req.sync() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "sync doc ingest is not supported; use the asynchronous work item flow".to_string(),
            )),
        ));
    }
    let space_id = require_space_id(&principal)?;
    let mut prepared = req
        .into_doc(&principal)
        .map_err(|error| (StatusCode::BAD_REQUEST, Json(error)))?;
    let mut doc = prepared.doc;
    doc.base.space_id.clone_from(&space_id);
    if let Some(existing_doc) = find_existing_doc_for_doc(&state.storage, &doc)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
    {
        if let Err(err) = state
            .authz
            .authorize_record_write(&principal, &existing_doc, "write:doc")
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: err.code,
                    message: err.message,
                    details: None,
                }),
            ));
        }
    } else if let Err(err) = state
        .authz
        .authorize_record_write(&principal, &doc, "write:doc")
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: err.code,
                message: err.message,
                details: None,
            }),
        ));
    }

    let rollback_assets = prepared.pending_assets.clone();
    persist_prepared_media_assets(&state.storage, &prepared.pending_assets)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, Json(error)))?;
    prepared.pending_assets.clear();

    let doc_id = doc.base.id.to_string();
    let result = stage_and_enqueue_manual_doc(&state, &principal, doc.clone()).await;
    let (staged_source, _staged_doc, work_item_id) = match result {
        Ok(value) => value,
        Err(err) => {
            rollback_prepared_media_assets(&state.storage, &rollback_assets).await;
            return Err(err);
        }
    };

    Ok((
        StatusCode::CREATED,
        Json(IngestResult {
            doc_id,
            source_id: staged_source.base.id.to_string(),
            lifecycle_status: staged_source.lifecycle_status,
            current_ingest_mode: staged_source.current_ingest_mode,
            work_item_id: Some(work_item_id.to_string()),
            enqueued: true,
        }),
    ))
}

/// GET /v1/docs/{id} - Get document by ID.
async fn get_doc(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Query(query): Query<DocQuery>,
) -> ApiResult<Doc> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    tracing::info!(
        "get_doc: space_id={}, id={}, include_deleted={}",
        space_id,
        id,
        query.include_deleted
    );
    let result = if query.include_deleted {
        state.doc_repo.get_with_deleted(&space_id, &rid).await
    } else {
        state.doc_repo.get(&space_id, &rid).await
    };

    match result {
        Ok(Some(doc)) => {
            state
                .authz
                .authorize_record_read(&principal, &doc, "read:doc")
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
            Ok(Json(doc))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// PUT /v1/docs/{id} - Update document.
async fn update_doc(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(doc): Json<Doc>,
) -> ApiResult<Doc> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.doc_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:doc")
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
                .authorize_record_write(&principal, &doc, "write:doc")
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

            match state.doc_repo.update(&space_id, &rid, doc).await {
                Ok(updated) => Ok(Json(updated)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// DELETE /v1/docs/{id} - Soft delete document.
async fn delete_doc(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.doc_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:doc")
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

            match state.doc_repo.delete(&space_id, &rid).await {
                Ok(()) => Ok(StatusCode::NO_CONTENT),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/docs/{id}/restore - Restore soft-deleted document.
async fn restore_doc(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Doc> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.doc_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:doc")
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
                .authorize_record_read(&principal, &existing, "read:doc")
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
                .doc_repo
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
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/docs/collections - Get distinct collections.
async fn get_collections(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<Vec<String>> {
    let space_id = require_space_id(&principal)?;
    match state
        .doc_repo
        .get_distinct_collections_for_space(&space_id)
        .await
    {
        Ok(collections) => Ok(Json(collections)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/docs/batch - Batch create/update documents.
async fn batch_docs(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<BatchRequest<BatchDocItem>>,
) -> ApiResult<BatchResponse<Doc>> {
    let space_id = require_space_id(&principal)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (index, item) in req.items.into_iter().enumerate() {
        let prepared = match item.into_doc(&principal) {
            Ok(doc) => doc,
            Err(error) => {
                failed.push(BatchError {
                    index,
                    error: error.message,
                });
                continue;
            }
        };
        let mut doc = prepared.doc;
        doc.base.space_id = space_id.clone();

        let pending_assets = prepared.pending_assets;
        let result = match persist_prepared_media_assets(&state.storage, &pending_assets).await {
            Ok(()) => match stage_and_enqueue_manual_doc_doc_surface(&state, &principal, doc).await
            {
                Ok((_, staged_doc, _)) => Ok(staged_doc.doc),
                Err((_, error)) => Err(error.0),
            },
            Err(error) => Err(error),
        };
        match result {
            Ok(doc) => {
                succeeded.push(doc);
            }
            Err(e) => {
                rollback_prepared_media_assets(&state.storage, &pending_assets).await;
                failed.push(BatchError {
                    index,
                    error: e.message,
                });
            }
        }
    }

    Ok(Json(BatchResponse { succeeded, failed }))
}

/// GET /v1/docs/{id}/chunks - Get document nodes (structural chunks).
async fn get_doc_chunks(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Vec<DocNode>> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    // Verify doc exists in caller's space and authorize
    let doc = state
        .doc_repo
        .get(&space_id, &rid)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id))))?;

    state
        .authz
        .authorize_record_read(&principal, &doc, "read:doc")
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

    // Get nodes via revision for this doc
    let revisions = state.revision_repo.find_by_doc(&rid).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;
    let Some(revision) = revisions.first() else {
        return Ok(Json(vec![]));
    };
    match state.node_repo.find_by_revision(&revision.base.id).await {
        Ok(nodes) => Ok(Json(nodes)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/docs/{id}/pipeline-status - Get document pipeline status.
#[allow(clippy::too_many_lines)]
async fn get_doc_pipeline_status(
    State(state): State<Arc<DocsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<DocPipelineStatus> {
    let space_id = require_space_id(&principal)?;
    let doc_id = parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    // Verify doc exists
    let doc = state
        .doc_repo
        .get(&space_id, &doc_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id))))?;

    state
        .authz
        .authorize_record_read(&principal, &doc, "read:doc")
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

    let revisions = state
        .revision_repo
        .find_by_doc(&doc_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    let selected_revision = revisions
        .iter()
        .find(|rev| rev.status == elephant_types::records::doc_revision::RevisionStatus::Active)
        .or_else(|| revisions.first());
    let (chunks_total, chunks_embedded, revision, nodes) = if let Some(revision) = selected_revision
    {
        let nodes = state
            .node_repo
            .find_by_revision(&revision.base.id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        let embeddable: Vec<_> = nodes
            .iter()
            .filter(|n| {
                matches!(
                    n.role,
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
                .filter(|n| n.embedding_model.is_some())
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

    let space_config = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    let source = if let Some(source_link) = &doc.source {
        state
            .source_repo
            .get(&space_id, source_link.id())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    } else {
        None
    };
    status.extraction_guidance = Some(resolve_extraction_guidance_status(
        space_config.as_ref(),
        Some(&doc),
        source.as_ref(),
    ));

    status.revision.clone_from(&revision);

    if let Some(revision_id) = &revision {
        status.nodes_count = nodes.len();

        // Assertions
        let assertions = state
            .assertion_repo
            .find_by_revision(&space_id, revision_id, 5000)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        status.assertions_count = assertions.len();

        // Count unique entities
        let mut entities: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
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

    // Extraction state from work items
    let work_items = state
        .work_item_repo
        .find_document_work(
            &space_id,
            &doc_id.to_string(),
            status.revision.as_ref().map(ToString::to_string).as_deref(),
            doc.source
                .as_ref()
                .map(|source| source.id().to_string())
                .as_deref(),
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    let revision_str = status.revision.as_ref().map(ToString::to_string);
    let doc_id_str = doc_id.to_string();
    let source_id_str = doc.source.as_ref().map(|source| source.id().to_string());
    let relevant_statuses: Vec<WorkItemStatus> = work_items
        .into_iter()
        .filter(|item| {
            revision_str
                .as_deref()
                .is_some_and(|value| work_item_matches_revision(item, value))
                || work_item_matches_doc_render(item, &doc_id_str, &doc.rendition_key)
                || source_id_str
                    .as_deref()
                    .is_some_and(|value| work_item_matches_source_lifecycle(item, value))
        })
        .map(|item| item.status)
        .collect();

    status.extraction_state = derive_extraction_state(status.assertions_count, &relevant_statuses);
    if graph_ingest_is_still_pending(source.as_ref(), status.assertions_count) {
        status.extraction_state = match status.extraction_state {
            ExtractionState::Failed | ExtractionState::Running => status.extraction_state,
            _ => ExtractionState::Pending,
        };
    }

    Ok(Json(status))
}

/// Returns document routes.
pub fn doc_routes() -> Router<Arc<DocsState>> {
    Router::new()
        .route("/v1/docs/search", post(search_docs))
        .route("/v1/docs/ingest", post(ingest_doc))
        .route("/v1/docs/collections", get(get_collections))
        .route("/v1/docs/batch", post(batch_docs))
        .route("/v1/docs/{id}", get(get_doc))
        .route("/v1/docs/{id}", put(update_doc))
        .route("/v1/docs/{id}", delete(delete_doc))
        .route("/v1/docs/{id}/restore", post(restore_doc))
        .route("/v1/docs/{id}/chunks", get(get_doc_chunks))
        .route(
            "/v1/docs/{id}/pipeline-status",
            get(get_doc_pipeline_status),
        )
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
        clippy::cognitive_complexity,
        clippy::large_futures
    )]
    use super::*;
    use crate::auth::Principal as ApiPrincipal;
    use crate::authz::AuthzService;
    use elephant_policy::Clearance;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::work::WorkItem;
    use elephant_types::RecordId;
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::Arc;

    fn make_work_item(kind: &str, status: WorkItemStatus, payload: serde_json::Value) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test".to_string(),
                "tester".to_string(),
            ),
            kind: kind.to_string(),
            payload,
            status,
            priority: 0,
            idempotency_key: format!("{kind}:test"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[test]
    fn test_ingest_request_defaults() {
        let json = r#"{
            "title": "test",
            "text": "test"
        }"#;
        let req: IngestRequest = serde_json::from_str(json).expect("deserialize ingest request");
        match req {
            IngestRequest::Normalized(req) => {
                assert_eq!(req.title, "test");
                assert!(!req.sync);
            }
            IngestRequest::Legacy(_) => panic!("expected normalized ingest request"),
        }
    }

    #[test]
    fn test_ingest_request_legacy_shape_still_deserializes() {
        let json = r#"{
            "doc": {
                "id": "doc:00000000-0000-0000-0000-000000000000",
                "space_id": "test",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-01-01T00:00:00Z",
                "created_by": "test",
                "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "provenance": {"sources": [], "chain": []},
                "subjects_state": "complete",
                "title": "test",
                "text": "test",
                "rendition_key": "test"
            }
        }"#;
        let req: IngestRequest = serde_json::from_str(json).expect("deserialize ingest request");
        match req {
            IngestRequest::Legacy(req) => assert_eq!(req.doc.title, "test"),
            IngestRequest::Normalized(_) => panic!("expected legacy ingest request"),
        }
    }

    #[test]
    fn test_ingest_request_rejects_mixed_legacy_and_normalized_fields() {
        let json = r#"{
            "title": "test",
            "text": "test",
            "doc": {
                "id": "doc:00000000-0000-0000-0000-000000000000",
                "space_id": "test",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-01-01T00:00:00Z",
                "created_by": "test",
                "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "provenance": {"sources": [], "chain": []},
                "subjects_state": "complete",
                "title": "legacy",
                "text": "legacy",
                "rendition_key": "legacy"
            }
        }"#;

        let decoded: Result<IngestRequest, _> = serde_json::from_str(json);
        assert!(decoded.is_err(), "mixed request shapes should be rejected");
    }

    #[test]
    fn test_doc_search_request_defaults() {
        let json = r"{}";
        let req: DocSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
    }

    fn make_api_principal(space_id: &str, scopes: &[&str]) -> ApiPrincipal {
        ApiPrincipal {
            subject: "tester".to_string(),
            space_id: Some(space_id.to_string()),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            security_level: 0,
            security_labels: Vec::new(),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: Vec::new(),
            bypass_abac: false,
        }
    }

    fn legacy_ingest_request(doc: Doc, collection: Option<&str>, sync: bool) -> IngestRequest {
        IngestRequest::Legacy(Box::new(LegacyIngestRequest {
            doc,
            collection: collection.map(std::borrow::ToOwned::to_owned),
            sync,
        }))
    }

    fn normalized_ingest_request(
        title: &str,
        text: Option<&str>,
        media: Vec<DocMediaInput>,
        collection: Option<&str>,
        sync: bool,
    ) -> IngestRequest {
        IngestRequest::Normalized(Box::new(NormalizedIngestRequest {
            title: title.to_string(),
            text: text.map(std::borrow::ToOwned::to_owned),
            media,
            collection: collection.map(std::borrow::ToOwned::to_owned),
            metadata: None,
            rendition_key: None,
            extraction_guidance: ExtractionGuidanceInput::default(),
            sync,
        }))
    }

    #[tokio::test]
    async fn test_authorize_doc_search_requires_read_scope() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = DocsState::new(&storage, Arc::new(AuthzService::new()));

        let err = authorize_doc_search(&state, &make_api_principal("default", &[]), "default")
            .expect_err("missing read:doc should be denied");
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        let body = (err.1).0;
        assert_eq!(body.code, "ACCESS_DENIED");
    }

    #[tokio::test]
    async fn test_authorize_doc_search_allows_read_scope() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = DocsState::new(&storage, Arc::new(AuthzService::new()));

        authorize_doc_search(
            &state,
            &make_api_principal("default", &["read:doc"]),
            "default",
        )
        .expect("read:doc should be allowed");
    }

    #[tokio::test]
    async fn test_authorize_doc_search_allows_metadata_scope() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = DocsState::new(&storage, Arc::new(AuthzService::new()));

        authorize_doc_search(
            &state,
            &make_api_principal("default", &["read:doc:metadata"]),
            "default",
        )
        .expect("read:doc:metadata should be allowed");
    }

    #[tokio::test]
    async fn test_ingest_doc_persists_doc_before_returning() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source", "read:doc"]);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Persist me".to_string(),
            text: "Immediate doc visibility".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "persist-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security = elephant_types::records::base::Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };

        let (status, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect("ingest doc");

        assert_eq!(status, StatusCode::CREATED);
        let persisted = state
            .doc_repo
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.base.id.to_string(), result.doc_id);
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(result.source_id)
        );
    }

    #[tokio::test]
    async fn test_ingest_doc_preserves_doc_author_on_created_source() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source", "read:doc"]);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: BaseFields::new(doc_id, "default".to_string(), "original-author".to_string()),
            title: "Delegated ingest".to_string(),
            text: "Body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "persist-doc".to_string(),
            metadata: serde_json::json!({}),
        };

        let (_, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect("ingest doc");

        let source_id = RecordId::from_str(&result.source_id).expect("parse source id");
        let source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        assert_eq!(source.base.created_by, "original-author");
        assert_eq!(source.author.as_deref(), Some("original-author"));
    }

    #[tokio::test]
    async fn test_restore_doc_requires_read_authorization() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Private Fixture".to_string(),
            text: "secret body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "private-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        state.doc_repo.create(doc).await.expect("create doc");
        state
            .doc_repo
            .delete("default", &doc_id)
            .await
            .expect("soft-delete doc");

        // Write scope alone: refused, and nothing changes.
        let denied = restore_doc(
            State(state.clone()),
            axum::Extension(make_api_principal("default", &["write:doc"])),
            Path(doc_id.to_string()),
        )
        .await;
        assert_eq!(
            denied.as_ref().err().map(|(status, _)| *status),
            Some(StatusCode::FORBIDDEN)
        );
        if let Err((_, Json(error))) = &denied {
            assert!(!error.message.contains("secret body"));
        }
        let stored = state
            .doc_repo
            .get_with_deleted("default", &doc_id)
            .await
            .expect("load doc")
            .expect("doc exists");
        assert!(stored.base.deleted_at.is_some());

        // Positive control: the same principal with read restores it.
        let allowed = restore_doc(
            State(state.clone()),
            axum::Extension(make_api_principal("default", &["write:doc", "read:doc"])),
            Path(doc_id.to_string()),
        )
        .await;
        assert!(allowed.is_ok());
    }

    #[tokio::test]
    async fn test_ingest_doc_restores_soft_deleted_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source", "read:doc"]);
        let doc_id = RecordId::new_v4(TableName::Doc);

        let existing = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Existing".to_string(),
            text: "Deleted doc".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "existing-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        state
            .doc_repo
            .create(existing)
            .await
            .expect("create conflicting doc");
        state
            .doc_repo
            .delete("default", &doc_id)
            .await
            .expect("soft-delete conflicting doc");

        let mut doc = Doc {
            base: BaseFields::new(doc_id, "default".to_string(), "tester".to_string()),
            title: "Persist me".to_string(),
            text: "This will conflict with deleted doc".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "persist-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security = elephant_types::records::base::Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };

        let (status, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect("ingest doc should restore deleted doc");

        assert_eq!(status, StatusCode::CREATED);
        let restored = state
            .doc_repo
            .get(
                "default",
                &RecordId::from_str(&result.doc_id).expect("parse doc id"),
            )
            .await
            .expect("load doc")
            .expect("restored doc");
        assert_eq!(restored.title, "Persist me");
        assert!(restored.base.deleted_at.is_none());
        let sources = state
            .source_repo
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_ingest_doc_reuses_existing_source_for_same_doc_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);
        let doc_id = RecordId::new_v4(TableName::Doc);

        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Original".to_string(),
            text: "Original body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "persist-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security = elephant_types::records::base::Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };

        let (_, Json(first_result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(doc.clone(), None, false)),
        )
        .await
        .expect("first ingest");

        let existing_doc = state
            .doc_repo
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        state
            .doc_repo
            .update("default", &doc_id, existing_doc)
            .await
            .expect("update doc security");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        let mut existing_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("load source")
            .expect("persisted source");
        existing_source.lifecycle_status = SourceLifecycleStatus::GraphReady;
        existing_source.current_ingest_mode = Some(IngestMode::Graph);
        state
            .source_repo
            .update("default", &source_id, existing_source)
            .await
            .expect("promote source state");

        doc.title = "Updated".to_string();
        doc.text = "Updated body".to_string();
        doc.metadata = serde_json::json!({"revision": 2});
        doc.base.security.level = 0;

        let (_, Json(second_result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect("second ingest");

        assert_eq!(first_result.source_id, second_result.source_id);
        assert_eq!(
            second_result.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(second_result.current_ingest_mode, None);
        let persisted = state
            .doc_repo
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Updated");
        assert_eq!(persisted.base.security.level, 0);
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(first_result.source_id.clone())
        );

        let persisted_source = state
            .source_repo
            .get("default", &source_id)
            .await
            .expect("reload source")
            .expect("source");
        assert_eq!(persisted_source.base.security.level, 0);
        assert_eq!(
            persisted_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(persisted_source.current_ingest_mode, None);

        let sources = state
            .source_repo
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_docs_preserves_doc_surface_authorization() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc"]);

        let first_doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Batch One".to_string(),
            text: "Batch body one".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "batch-one".to_string(),
            metadata: serde_json::json!({}),
        };
        let second_doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Batch Two".to_string(),
            text: "Batch body two".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "batch-two".to_string(),
            metadata: serde_json::json!({}),
        };

        let Json(response) = batch_docs(
            State(state.clone()),
            axum::Extension(principal),
            Json(BatchRequest {
                items: vec![
                    BatchDocItem::Legacy(Box::new(first_doc)),
                    BatchDocItem::Legacy(Box::new(second_doc)),
                ],
                atomic: false,
            }),
        )
        .await
        .expect("batch ingest");

        assert_eq!(response.failed.len(), 0);
        assert_eq!(response.succeeded.len(), 2);
        assert!(response.succeeded.iter().all(|doc| doc.source.is_some()));

        let sources = state
            .source_repo
            .list("default", 10, None)
            .await
            .expect("list sources");
        assert_eq!(sources.len(), 2, "batch docs should stage sources");

        let work_items = state
            .work_item_repo
            .list("default", 10, None)
            .await
            .expect("list work items");
        assert!(
            work_items
                .iter()
                .all(|item| item.kind == TRIAGE_SOURCE_KIND),
            "batch docs should enqueue only source-first triage work items",
        );
    }

    #[tokio::test]
    async fn test_ingest_doc_accepts_normalized_media_request_shape() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let (status, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                "Observed frame",
                Some("Operator sidecar"),
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode([0_u8, 1, 2, 3])),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("frames"),
                false,
            )),
        )
        .await
        .expect("normalized ingest");

        assert_eq!(status, StatusCode::CREATED);
        let persisted = state
            .doc_repo
            .get(
                "default",
                &RecordId::from_str(&result.doc_id).expect("parse doc id"),
            )
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(persisted.title, "Observed frame");
        assert_eq!(persisted.text, "");
        assert_eq!(persisted.sidecar_text.as_deref(), Some("Operator sidecar"));
        assert_eq!(persisted.media_attachments.len(), 1);
        assert_eq!(
            persisted.metadata.get("collection"),
            Some(&serde_json::Value::String("frames".to_string()))
        );
        assert_eq!(
            persisted.metadata.get("content_type"),
            Some(&serde_json::Value::String("image/jpeg".to_string()))
        );
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_zero_duration_inline_video() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                "Observed clip",
                Some("Operator sidecar"),
                vec![DocMediaInput {
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode(b"not-a-real-video")),
                    path: None,
                    uri: None,
                    duration_ms: Some(0),
                }],
                Some("clips"),
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
    async fn test_ingest_doc_rolls_back_media_assets_when_authorization_fails() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:source"]);

        let err = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                "Observed frame",
                Some("Operator sidecar"),
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode([0_u8, 1, 2, 3])),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("frames"),
                false,
            )),
        )
        .await
        .expect_err("missing write:doc should reject normalized media ingest");

        assert_eq!(err.0, StatusCode::FORBIDDEN);

        let assets = MediaAssetRepository::new(state.storage.clone())
            .list("default", 10, None)
            .await
            .expect("list media assets");
        assert!(
            assets.is_empty(),
            "rejected media ingest should not leak orphaned media assets"
        );
    }

    #[tokio::test]
    async fn test_batch_docs_accepts_normalized_request_shape() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let Json(response) = batch_docs(
            State(state.clone()),
            axum::Extension(principal),
            Json(BatchRequest {
                items: vec![
                    BatchDocItem::Normalized(Box::new(NormalizedBatchDocItem {
                        title: "Frame A".to_string(),
                        text: Some("Observed terminal".to_string()),
                        media: vec![DocMediaInput {
                            kind: DocMediaKind::Image,
                            media_type: "image/png".to_string(),
                            inline_base64: Some(BASE64_STANDARD.encode([0_u8, 1, 2, 3])),
                            path: None,
                            uri: None,
                            duration_ms: None,
                        }],
                        collection: Some("frames".to_string()),
                        metadata: None,
                        rendition_key: None,
                        extraction_guidance: ExtractionGuidanceInput::default(),
                    })),
                    BatchDocItem::Normalized(Box::new(NormalizedBatchDocItem {
                        title: "Transcript".to_string(),
                        text: Some("plain text doc".to_string()),
                        media: Vec::new(),
                        collection: None,
                        metadata: None,
                        rendition_key: None,
                        extraction_guidance: ExtractionGuidanceInput::default(),
                    })),
                ],
                atomic: false,
            }),
        )
        .await
        .expect("batch ingest");

        assert!(response.failed.is_empty());
        assert_eq!(response.succeeded.len(), 2);
        assert_eq!(
            response.succeeded[0].sidecar_text.as_deref(),
            Some("Observed terminal")
        );
        assert!(
            response.succeeded[0].source.is_some(),
            "media-backed batch docs should stage a source-first ingest"
        );
        assert!(
            response.succeeded[1].source.is_some(),
            "text-only batch docs should also stage a source-first ingest",
        );

        let work_items = state
            .work_item_repo
            .list("default", 10, None)
            .await
            .expect("list work items");
        let triage_count = work_items
            .iter()
            .filter(|item| item.kind == TRIAGE_SOURCE_KIND)
            .count();
        assert!(
            triage_count >= 2,
            "batch ingest should enqueue triage_source work for both items"
        );
    }

    #[tokio::test]
    async fn test_legacy_batch_docs_use_source_first_doc_surface() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc"]);

        let legacy_doc = Doc {
            base: BaseFieldsBuilder::new("default")
                .for_table(TableName::Doc)
                .created_by("tester")
                .build(),
            title: "Legacy batch".to_string(),
            text: "legacy batch text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "legacy-batch-direct".to_string(),
            metadata: serde_json::json!({}),
        };

        let Json(response) = batch_docs(
            State(state.clone()),
            axum::Extension(principal),
            Json(BatchRequest {
                items: vec![BatchDocItem::Legacy(Box::new(legacy_doc))],
                atomic: false,
            }),
        )
        .await
        .expect("legacy batch ingest");

        assert!(response.failed.is_empty());
        assert_eq!(response.succeeded.len(), 1);
        assert!(
            response.succeeded[0].source.is_some(),
            "legacy batch docs should now flow through the source-first doc surface"
        );

        let work_items = state
            .work_item_repo
            .list("default", 10, None)
            .await
            .expect("list work items");
        assert!(
            work_items
                .iter()
                .any(|item| item.kind == TRIAGE_SOURCE_KIND),
            "legacy batch docs should enqueue triage_source work"
        );
    }

    #[tokio::test]
    async fn test_text_only_batch_docs_enqueue_triage_with_doc_only_scope() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc"]);

        let Json(response) = batch_docs(
            State(state.clone()),
            axum::Extension(principal),
            Json(BatchRequest {
                items: vec![BatchDocItem::Normalized(Box::new(NormalizedBatchDocItem {
                    title: "Plain batch doc".to_string(),
                    text: Some("plain batch text".to_string()),
                    media: Vec::new(),
                    collection: Some("notes".to_string()),
                    metadata: None,
                    rendition_key: Some("plain-batch".to_string()),
                    extraction_guidance: ExtractionGuidanceInput::default(),
                }))],
                atomic: false,
            }),
        )
        .await
        .expect("batch ingest");

        assert!(response.failed.is_empty());
        assert_eq!(response.succeeded.len(), 1);
        assert!(
            response.succeeded[0].source.is_some(),
            "text-only batch docs should still use the source-first doc surface"
        );

        let work_items = state
            .work_item_repo
            .list("default", 10, None)
            .await
            .expect("list work items");
        assert!(
            work_items
                .iter()
                .any(|item| item.kind == TRIAGE_SOURCE_KIND),
            "text-only batch docs should enqueue triage_source work"
        );
    }

    #[tokio::test]
    async fn test_legacy_ingest_doc_preserves_top_level_collection() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let doc = Doc {
            base: BaseFieldsBuilder::new("default")
                .for_table(TableName::Doc)
                .created_by("tester")
                .build(),
            title: "Legacy doc".to_string(),
            text: "legacy text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "legacy-collection".to_string(),
            metadata: serde_json::json!({"existing":"value"}),
        };

        let (_, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, Some("legacy-frames"), false)),
        )
        .await
        .expect("legacy ingest");

        let persisted = state
            .doc_repo
            .get(
                "default",
                &RecordId::from_str(&result.doc_id).expect("parse doc id"),
            )
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.metadata.get("collection"),
            Some(&serde_json::Value::String("legacy-frames".to_string()))
        );
        assert_eq!(
            persisted.metadata.get("existing"),
            Some(&serde_json::Value::String("value".to_string()))
        );
    }

    #[tokio::test]
    async fn test_legacy_ingest_doc_preserves_scalar_metadata_when_merging_collection() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let doc = Doc {
            base: BaseFieldsBuilder::new("default")
                .for_table(TableName::Doc)
                .created_by("tester")
                .build(),
            title: "Legacy scalar metadata".to_string(),
            text: "legacy text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "legacy-scalar-metadata".to_string(),
            metadata: serde_json::Value::String("raw-tag".to_string()),
        };

        let (_, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, Some("legacy-frames"), false)),
        )
        .await
        .expect("legacy ingest");

        let persisted = state
            .doc_repo
            .get(
                "default",
                &RecordId::from_str(&result.doc_id).expect("parse doc id"),
            )
            .await
            .expect("load doc")
            .expect("persisted doc");
        assert_eq!(
            persisted.metadata.get("collection"),
            Some(&serde_json::Value::String("legacy-frames".to_string()))
        );
        assert_eq!(
            persisted.metadata.get("_legacy_metadata"),
            Some(&serde_json::Value::String("raw-tag".to_string()))
        );
    }

    #[tokio::test]
    async fn test_get_doc_pipeline_status_reports_pending_when_triage_source_is_runnable() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source", "read:doc"]);

        let (_, Json(result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(normalized_ingest_request(
                "Observed frame",
                Some("Operator sidecar"),
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    inline_base64: Some(BASE64_STANDARD.encode([0_u8, 1, 2, 3])),
                    path: None,
                    uri: None,
                    duration_ms: None,
                }],
                Some("frames"),
                false,
            )),
        )
        .await
        .expect("ingest doc");

        let Json(status) = get_doc_pipeline_status(
            State(state),
            axum::Extension(principal),
            Path(result.doc_id),
        )
        .await
        .expect("pipeline status");

        assert_eq!(status.extraction_state, ExtractionState::Pending);
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_path_or_uri_media_transports() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(normalized_ingest_request(
                "Observed frame",
                Some("Operator sidecar"),
                vec![DocMediaInput {
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    inline_base64: None,
                    path: None,
                    uri: Some("https://example.com/frame.jpg".to_string()),
                    duration_ms: None,
                }],
                Some("frames"),
                false,
            )),
        )
        .await
        .expect_err("path/uri media transport should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.message.contains("must use inline_base64"));
    }

    #[tokio::test]
    async fn test_legacy_ingest_doc_rejects_path_or_uri_media_attachments() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Unsafe legacy media".to_string(),
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
            rendition_key: "legacy-unsafe".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect_err("legacy path/uri media attachments should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.message.contains("must not use path or uri"));
    }

    #[tokio::test]
    async fn test_legacy_batch_docs_rejects_path_or_uri_media_attachments() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc"]);

        let legacy_doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Unsafe batch media".to_string(),
            text: String::new(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Video,
                media_type: "video/mp4".to_string(),
                location: DocMediaLocation::Uri {
                    uri: "https://example.com/clip.mp4".to_string(),
                },
                label: None,
                size_bytes: None,
                duration_ms: Some(1000),
                sha256: None,
            }],
            rendition_key: "legacy-batch-unsafe".to_string(),
            metadata: serde_json::json!({}),
        };

        let response = batch_docs(
            State(state),
            axum::Extension(principal),
            Json(BatchRequest {
                items: vec![BatchDocItem::Legacy(Box::new(legacy_doc))],
                atomic: true,
            }),
        )
        .await
        .expect("batch route should return a structured batch response");

        assert!(response.succeeded.is_empty());
        assert_eq!(response.failed.len(), 1);
        assert_eq!(response.failed[0].index, 0);
        assert!(response.failed[0]
            .error
            .contains("must not use path or uri"));
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_missing_space_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: None,
            scopes: vec!["write:doc".to_string(), "write:source".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Missing space".to_string(),
            text: "Should fail".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect_err("missing space_id should be rejected");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.message.contains("missing space_id"));
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_empty_space_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = AuthPrincipal {
            subject: "tester".to_string(),
            space_id: Some("   ".to_string()),
            scopes: vec!["write:doc".to_string()],
            bypass_abac: true,
            ..AuthPrincipal::default()
        };

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
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

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect_err("empty space_id should be rejected");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.message.contains("missing space_id"));
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_sync_mode() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".to_string(),
                "tester".to_string(),
            ),
            title: "Sync".to_string(),
            text: "Unsupported".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "sync-doc".to_string(),
            metadata: serde_json::json!({}),
        };

        let err = ingest_doc(
            State(state),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, true)),
        )
        .await
        .expect_err("sync ingest should be rejected");

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.message.contains("sync doc ingest is not supported"));
    }

    #[tokio::test]
    async fn test_ingest_doc_rejects_overwrite_of_secured_existing_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let state = Arc::new(DocsState::new(&storage, Arc::new(AuthzService::new())));
        let principal = make_api_principal("default", &["write:doc", "write:source"]);
        let doc_id = RecordId::new_v4(TableName::Doc);

        let mut doc = Doc {
            base: BaseFields::new(doc_id.clone(), "default".to_string(), "tester".to_string()),
            title: "Original".to_string(),
            text: "Original body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "persist-doc".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security = elephant_types::records::base::Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };

        let (_, Json(first_result)) = ingest_doc(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Json(legacy_ingest_request(doc.clone(), None, false)),
        )
        .await
        .expect("first ingest");

        let source_id = RecordId::from_str(&first_result.source_id).expect("parse source id");
        let mut existing_doc = state
            .doc_repo
            .get("default", &doc_id)
            .await
            .expect("load doc")
            .expect("persisted doc");
        existing_doc.base.security.level = 4;
        state
            .doc_repo
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

        let err = ingest_doc(
            State(state.clone()),
            axum::Extension(principal),
            Json(legacy_ingest_request(doc, None, false)),
        )
        .await
        .expect_err("secured existing doc should block overwrite");

        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_semantic_page_metadata_preserves_total_before_truncate() {
        let (page, total, cursor, has_more) = semantic_page_metadata(vec![1, 2, 3], 0, 2);
        assert_eq!(page, vec![1, 2]);
        assert_eq!(total, 3);
        assert_eq!(cursor, Some("2".to_string()));
        assert!(has_more);
    }

    #[test]
    fn test_semantic_page_metadata_supports_offset_cursor() {
        let (page, total, cursor, has_more) = semantic_page_metadata(vec![1, 2, 3], 2, 2);
        assert_eq!(page, vec![3]);
        assert_eq!(total, 3);
        assert_eq!(cursor, None);
        assert!(!has_more);
    }

    #[test]
    fn test_semantic_page_metadata_zero_limit_has_no_cursor() {
        let (page, total, cursor, has_more) = semantic_page_metadata(vec![1, 2, 3], 0, 0);
        assert!(page.is_empty());
        assert_eq!(total, 3);
        assert!(cursor.is_none());
        assert!(!has_more);
    }

    #[test]
    fn test_semantic_empty_page_response_reports_zero_total() {
        let mut response = semantic_empty_page_response(7);
        response.mode = "semantic".to_string();
        assert!(response.results.is_empty());
        assert!(response.cursor.is_none());
        assert!(!response.has_more);
        assert_eq!(response.total, 0);
        assert_eq!(response.mode, "semantic");
    }

    #[test]
    fn test_semantic_page_limit_caps_to_k_when_present() {
        assert_eq!(semantic_page_limit(50, Some(5)), 5);
        assert_eq!(semantic_page_limit(5, Some(50)), 5);
        assert_eq!(semantic_page_limit(50, None), 50);
    }

    #[test]
    fn test_semantic_candidate_limit_scales_with_requested_offset() {
        let page_limit = semantic_page_limit(50, Some(5));
        let first_page_candidates = semantic_candidate_limit(page_limit);
        let later_page_candidates = semantic_candidate_limit(40usize.saturating_add(page_limit));

        assert_eq!(first_page_candidates, 40);
        assert_eq!(later_page_candidates, 360);
        assert!(later_page_candidates > first_page_candidates);
    }

    #[test]
    fn test_semantic_empty_result_disposition_falls_back_on_first_page() {
        assert_eq!(
            semantic_empty_result_disposition(0, false),
            SemanticEmptyResultDisposition::FallbackToText
        );
        assert_eq!(
            semantic_empty_result_disposition(0, true),
            SemanticEmptyResultDisposition::FallbackToText
        );
    }

    #[test]
    fn test_semantic_empty_result_disposition_preserves_paginated_semantics() {
        assert_eq!(
            semantic_empty_result_disposition(5, false),
            SemanticEmptyResultDisposition::EmptyPage
        );
        assert_eq!(
            semantic_empty_result_disposition(5, true),
            SemanticEmptyResultDisposition::ServiceUnavailable
        );
    }

    #[test]
    fn test_semantic_page_offset_rejects_non_numeric_cursor() {
        let error = semantic_page_offset(Some("doc:not-a-semantic-offset"))
            .expect_err("non-numeric semantic cursor should be rejected");
        assert_eq!(error.code, "VALIDATION_ERROR");
        assert!(error
            .message
            .contains("Semantic search cursors must be numeric offsets"));
    }

    #[test]
    fn test_semantic_text_fallback_cursor_extracts_text_cursor() {
        assert_eq!(
            semantic_text_fallback_cursor(Some("text:5")),
            Some("5".to_string())
        );
        assert_eq!(semantic_text_fallback_cursor(Some("7")), None);
    }

    #[test]
    fn test_semantic_page_offset_rejects_text_fallback_cursor() {
        let error = semantic_page_offset(Some("text:5"))
            .expect_err("text fallback cursor should be rejected as semantic offset");
        assert_eq!(error.code, "VALIDATION_ERROR");
        assert!(error
            .message
            .contains("Text-fallback cursors are not valid semantic offsets"));
    }

    #[test]
    fn test_semantic_expansion_action_requires_unavailable_on_partition_failure() {
        assert_eq!(
            semantic_expansion_action(true, false),
            SemanticExpansionAction::ServiceUnavailable
        );
        assert_eq!(
            semantic_expansion_action(true, true),
            SemanticExpansionAction::ServiceUnavailable
        );
        assert_eq!(
            semantic_expansion_action(false, true),
            SemanticExpansionAction::ExpandCandidates
        );
        assert_eq!(
            semantic_expansion_action(false, false),
            SemanticExpansionAction::ReturnPage
        );
    }

    #[test]
    fn test_build_text_search_criteria_preserves_cursor_and_limit() {
        let criteria = build_text_search_criteria(
            "space-a",
            Some("alice"),
            true,
            Some("cursor-1".to_string()),
            7,
        );

        assert_eq!(criteria.space_id.as_deref(), Some("space-a"));
        assert_eq!(criteria.query.as_deref(), Some("alice"));
        assert_eq!(criteria.cursor.as_deref(), Some("cursor-1"));
        assert_eq!(criteria.limit, 7);
        assert!(criteria.include_deleted);
    }

    #[test]
    fn test_should_rerun_text_search_after_authz_when_current_page_was_filtered() {
        assert!(should_rerun_text_search_after_authz(10, 8, true, false));
        assert!(!should_rerun_text_search_after_authz(10, 10, false, false));
        assert!(!should_rerun_text_search_after_authz(10, 10, true, false));
        assert!(!should_rerun_text_search_after_authz(10, 10, true, true));
    }

    #[test]
    fn test_derive_extraction_state_not_triggered_when_no_work_no_assertions() {
        let state = derive_extraction_state(0, &[]);
        assert_eq!(state, ExtractionState::NotTriggered);
    }

    #[test]
    fn test_derive_extraction_state_pending_when_render_doc_runnable() {
        let item = make_work_item(
            RENDER_DOC_KIND,
            WorkItemStatus::Runnable,
            serde_json::json!({"doc_id":"doc:1","rendition_key":"rk1"}),
        );

        let statuses = vec![item.status];
        let state = derive_extraction_state(0, &statuses);
        assert_eq!(state, ExtractionState::Pending);
    }

    #[test]
    fn test_work_item_matches_doc_render_requires_current_rendition_when_present() {
        let item = make_work_item(
            RENDER_DOC_KIND,
            WorkItemStatus::Failed,
            serde_json::json!({"doc_id":"doc:1","rendition_key":"rk_old"}),
        );

        assert!(!work_item_matches_doc_render(&item, "doc:1", "rk_new"));
    }

    #[test]
    fn test_work_item_matches_doc_render_accepts_missing_rendition_key_for_back_compat() {
        let item = make_work_item(
            RENDER_DOC_KIND,
            WorkItemStatus::Failed,
            serde_json::json!({"doc_id":"doc:1"}),
        );

        assert!(work_item_matches_doc_render(&item, "doc:1", "rk_any"));
    }

    #[test]
    fn test_source_lifecycle_work_items_match_pipeline_status_source() {
        for kind in [
            TRIAGE_SOURCE_KIND,
            INGEST_METADATA_KIND,
            UPGRADE_INGEST_MODE_KIND,
        ] {
            let item = make_work_item(
                kind,
                WorkItemStatus::Runnable,
                serde_json::json!({"source_id":"source:1"}),
            );

            assert!(work_item_matches_source_lifecycle(&item, "source:1"));
            assert!(!work_item_matches_source_lifecycle(&item, "source:2"));
        }
    }
}
