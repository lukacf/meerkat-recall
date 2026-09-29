//! Relationship API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/relationships/search - Search relationships
//! - POST /v1/relationships - Create relationship
//! - POST /v1/relationships/upsert - Upsert relationship
//! - GET /v1/relationships/{id} - Get relationship by ID
//! - PUT /v1/relationships/{id} - Update relationship
//! - DELETE /v1/relationships/{id} - Soft delete relationship
//! - POST /v1/relationships/{id}/restore - Restore soft-deleted relationship
//! - POST /v1/relationships/batch - Batch operations

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::{
    parse_id, PaginatedResult, RelRepository, SearchCriteria, StorageClient, StorageError,
    StorageResult,
};
use elephant_types::ids::TableName;
use elephant_types::records::Rel;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::{ApiError, BatchError, BatchRequest, BatchResponse};
use crate::routes::require_space_id;

/// Shared state for relationship handlers.
pub struct RelationshipsState {
    /// Relationship repository.
    pub rel_repo: Arc<dyn RelRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// Relationship repository interface used by the API layer.
#[async_trait::async_trait]
pub trait RelRepo: Send + Sync {
    async fn create(&self, record: Rel) -> StorageResult<Rel>;
    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>>;
    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Rel,
    ) -> StorageResult<Rel>;
    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()>;
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Rel>>;

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>>;
    async fn restore(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<Rel>;
    async fn upsert(&self, record: Rel) -> StorageResult<Rel>;
}

#[async_trait::async_trait]
impl RelRepo for RelRepository {
    async fn create(&self, record: Rel) -> StorageResult<Rel> {
        elephant_storage::Repository::create(self, record).await
    }

    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>> {
        elephant_storage::Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Rel,
    ) -> StorageResult<Rel> {
        elephant_storage::Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()> {
        elephant_storage::Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Rel>> {
        elephant_storage::SearchRepository::search(self, criteria).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<Rel> {
        Self::restore(self, space_id, id).await
    }

    async fn upsert(&self, record: Rel) -> StorageResult<Rel> {
        Self::upsert(self, record).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl RelRepo for elephant_storage::MockRelRepository {
    async fn create(&self, record: Rel) -> StorageResult<Rel> {
        elephant_storage::Repository::create(self, record).await
    }

    async fn get(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>> {
        elephant_storage::Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        record: Rel,
    ) -> StorageResult<Rel> {
        elephant_storage::Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<()> {
        elephant_storage::Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Rel>> {
        elephant_storage::SearchRepository::search(self, criteria).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
    ) -> StorageResult<Option<Rel>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(&self, space_id: &str, id: &elephant_types::RecordId) -> StorageResult<Rel> {
        Self::restore(self, space_id, id).await
    }

    async fn upsert(&self, record: Rel) -> StorageResult<Rel> {
        Self::upsert(self, record).await
    }
}

impl RelationshipsState {
    /// Creates new relationship state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            rel_repo: Arc::new(RelRepository::new(client)),
            authz,
        }
    }

    /// Creates relationship state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(rel_repo: Arc<dyn RelRepo>, authz: AuthzState) -> Self {
        Self { rel_repo, authz }
    }
}

/// Relationship search request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelSearchRequest {
    /// Filter by relationship type.
    #[serde(default)]
    pub rel_type: Option<String>,
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted relationships.
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
pub struct RelSearchResponse {
    /// Search results.
    pub results: Vec<Rel>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    pub total: usize,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;
const REL_SEARCH_SCOPE: &str = "read:relationship";
const MAX_VISIBLE_REL_SEARCH_PAGES: usize = 10_000;

fn build_rel_search_criteria(
    space_id: &str,
    rel_type: Option<&str>,
    include_deleted: bool,
    cursor: Option<String>,
    limit: usize,
) -> SearchCriteria {
    let mut criteria = SearchCriteria::new()
        .with_limit(limit)
        .with_space_id(space_id.to_string());
    if let Some(rel_type) = rel_type {
        criteria = criteria.with_query(rel_type.to_string());
    }
    if include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(cursor) = cursor {
        criteria = criteria.with_cursor(cursor);
    }
    criteria
}

async fn count_visible_relationship_search_results(
    state: &RelationshipsState,
    principal: &AuthPrincipal,
    space_id: &str,
    rel_type: Option<&str>,
    include_deleted: bool,
) -> Result<usize, (StatusCode, Json<ApiError>)> {
    let mut total_visible = 0usize;
    let mut scan_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_REL_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Relationship search exceeded the maximum page scan bound",
                )),
            ));
        }

        let criteria =
            build_rel_search_criteria(space_id, rel_type, include_deleted, scan_cursor, 1);
        let result = state.rel_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let Some(rel) = result.results.into_iter().next() else {
            break;
        };

        if !state
            .authz
            .filter_records(principal, vec![rel], REL_SEARCH_SCOPE)
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
                    "Relationship search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Relationship search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_cursor);
    }

    Ok(total_visible)
}

async fn rerun_relationship_search_after_authz_filter(
    state: &RelationshipsState,
    principal: &AuthPrincipal,
    space_id: &str,
    rel_type: Option<&str>,
    limit: usize,
    include_deleted: bool,
    start_cursor: Option<String>,
) -> Result<RelSearchResponse, (StatusCode, Json<ApiError>)> {
    let mut visible = Vec::new();
    let total_visible = count_visible_relationship_search_results(
        state,
        principal,
        space_id,
        rel_type,
        include_deleted,
    )
    .await?;
    let mut scan_cursor = start_cursor;
    let mut next_cursor = None;
    let mut seen_cursors = std::collections::HashSet::new();
    let mut pages = 0usize;

    loop {
        pages = pages.saturating_add(1);
        if pages > MAX_VISIBLE_REL_SEARCH_PAGES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Relationship search exceeded the maximum page scan bound",
                )),
            ));
        }

        let criteria =
            build_rel_search_criteria(space_id, rel_type, include_deleted, scan_cursor.clone(), 1);
        let result = state.rel_repo.search(&criteria).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let page_cursor = scan_cursor.clone();
        let result_cursor = result.cursor.clone();
        let Some(rel) = result.results.into_iter().next() else {
            break;
        };

        let allowed = state
            .authz
            .filter_records(principal, vec![rel], REL_SEARCH_SCOPE);
        if let Some(rel) = allowed.into_iter().next() {
            if visible.len() < limit {
                visible.push(rel);
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
                    "Relationship search pagination reported more results without a cursor",
                )),
            ));
        };
        if !seen_cursors.insert(next_scan_cursor.clone()) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Relationship search pagination cursor repeated without making progress",
                )),
            ));
        }
        scan_cursor = Some(next_scan_cursor);
    }

    let has_more = next_cursor.is_some();
    Ok(RelSearchResponse {
        results: visible,
        cursor: next_cursor,
        has_more,
        total: total_visible,
    })
}

/// POST /v1/relationships/search - Search relationships.
async fn search_relationships(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<RelSearchRequest>,
) -> ApiResult<RelSearchResponse> {
    let space_id = require_space_id(&principal)?;

    if let Some(req_space_id) = &req.space_id {
        if req_space_id != &space_id {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access to space denied")),
            ));
        }
    }

    let rel_type = req.rel_type.clone();
    let start_cursor = req.cursor.clone();
    let mut criteria = SearchCriteria::new()
        .with_limit(req.limit)
        .with_space_id(space_id.clone());
    if let Some(t) = rel_type.clone() {
        criteria = criteria.with_query(t);
    }
    if req.include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(c) = start_cursor.clone() {
        criteria = criteria.with_cursor(c);
    }

    let result = state.rel_repo.search(&criteria).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    let page_len = result.results.len();
    let filtered = state
        .authz
        .filter_records(&principal, result.results, REL_SEARCH_SCOPE);

    if filtered.len() != page_len {
        return rerun_relationship_search_after_authz_filter(
            &state,
            &principal,
            &space_id,
            rel_type.as_deref(),
            req.limit,
            req.include_deleted,
            start_cursor,
        )
        .await
        .map(Json);
    }

    Ok(Json(RelSearchResponse {
        results: filtered,
        cursor: result.cursor,
        has_more: result.has_more,
        total: result.total,
    }))
}

/// POST /v1/relationships - Create relationship.
async fn create_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(rel): Json<Rel>,
) -> Result<(StatusCode, Json<Rel>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut rel = rel;
    rel.base.space_id = require_space_id(&principal)?;

    state
        .authz
        .authorize_record_write(&principal, &rel, "write:relationship")
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

    match state.rel_repo.create(rel).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StorageError::Conflict(msg)) => Err((
            StatusCode::CONFLICT,
            Json(ApiError {
                code: "CONFLICT".to_string(),
                message: msg,
                details: None,
            }),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/relationships/{id} - Get relationship by ID.
async fn get_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.rel_repo.get(&space_id, &rid).await {
        Ok(Some(rel)) => {
            state
                .authz
                .authorize_record_read(&principal, &rel, "read:relationship")
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
            Ok(Json(rel))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// PUT /v1/relationships/{id} - Update relationship.
async fn update_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(rel): Json<Rel>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.rel_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:relationship")
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
                .authorize_record_write(&principal, &rel, "write:relationship")
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

            match state.rel_repo.update(&space_id, &rid, rel).await {
                Ok(updated) => Ok(Json(updated)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// DELETE /v1/relationships/{id} - Soft delete relationship.
async fn delete_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.rel_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:relationship")
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

            match state.rel_repo.delete(&space_id, &rid).await {
                Ok(()) => Ok(StatusCode::NO_CONTENT),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/relationships/{id}/restore - Restore soft-deleted relationship.
async fn restore_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.rel_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:relationship")
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

            match state.rel_repo.restore(&space_id, &rid).await {
                Ok(restored) => Ok(Json(restored)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/relationships/upsert - Upsert relationship.
async fn upsert_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(rel): Json<Rel>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;

    if rel.base.space_id != space_id {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Space ID mismatch")),
        ));
    }

    state
        .authz
        .authorize_record_write(&principal, &rel, "write:relationship")
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

    match state.rel_repo.upsert(rel).await {
        Ok(updated) => Ok(Json(updated)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/relationships/batch - Batch operations.
async fn batch_relationships(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<BatchRequest<Rel>>,
) -> ApiResult<BatchResponse<Rel>> {
    let space_id = require_space_id(&principal)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (index, rel) in req.items.into_iter().enumerate() {
        let rid = rel.base.id.clone();

        let result = match state.rel_repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(existing)) => {
                if let Err(e) =
                    state
                        .authz
                        .authorize_record_write(&principal, &existing, "write:relationship")
                {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                if let Err(e) =
                    state
                        .authz
                        .authorize_record_write(&principal, &rel, "write:relationship")
                {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                if existing.base.deleted_at.is_some() {
                    let _ = state.rel_repo.restore(&space_id, &rid).await;
                }
                state.rel_repo.update(&space_id, &rid, rel).await
            }
            Ok(None) => {
                if let Err(e) =
                    state
                        .authz
                        .authorize_record_write(&principal, &rel, "write:relationship")
                {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                state.rel_repo.create(rel).await
            }
            Err(e) => Err(e),
        };

        match result {
            Ok(d) => succeeded.push(d),
            Err(e) => failed.push(BatchError {
                index,
                error: e.to_string(),
            }),
        }
    }

    Ok(Json(BatchResponse { succeeded, failed }))
}

/// Returns relationship routes.
pub fn rel_routes() -> Router<Arc<RelationshipsState>> {
    Router::new()
        .route("/v1/relationships/search", post(search_relationships))
        .route("/v1/relationships", post(create_relationship))
        .route("/v1/relationships/upsert", post(upsert_relationship))
        .route("/v1/relationships/batch", post(batch_relationships))
        .route("/v1/relationships/{id}", get(get_relationship))
        .route("/v1/relationships/{id}", put(update_relationship))
        .route("/v1/relationships/{id}", delete(delete_relationship))
        .route("/v1/relationships/{id}/restore", post(restore_relationship))
}

#[cfg(test)]
#[path = "relationships_test.rs"]
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
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_rel_search_request_defaults() {
        let json = r"{}";
        let req: RelSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
    }
}
