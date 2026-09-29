//! Assertion API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/assertions/search - Search assertions with filters
//! - GET /v1/assertions/{id} - Get assertion by ID
//! - POST /v1/assertions - Create manual assertion
//! - PATCH /v1/assertions/{id}/resolution - Update resolution status

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, patch, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::{parse_id, AssertionRepository, Repository, StorageClient, StorageResult};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{Assertion, ResolutionStatus};
use elephant_types::RecordId;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;
use crate::routes::require_space_id;

/// Shared state for assertion handlers.
pub struct AssertionsState {
    /// Assertion repository.
    pub assertion_repo: Arc<dyn AssertionRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// Assertion repository interface used by the API layer.
#[async_trait::async_trait]
pub trait AssertionRepo: Send + Sync {
    async fn create(&self, record: Assertion) -> StorageResult<Assertion>;
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Assertion>>;
    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Assertion,
    ) -> StorageResult<Assertion>;
    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Assertion>>;
    async fn find_by_resolved_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>>;
    async fn find_by_resolved_entity_pair(
        &self,
        space_id: &str,
        subject_entity: Option<&RecordId>,
        object_entity: Option<&RecordId>,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>>;
}

#[async_trait::async_trait]
impl AssertionRepo for AssertionRepository {
    async fn create(&self, record: Assertion) -> StorageResult<Assertion> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Assertion>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Assertion,
    ) -> StorageResult<Assertion> {
        Repository::update(self, space_id, id, record).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Assertion>> {
        Repository::list(self, space_id, limit, cursor).await
    }

    async fn find_by_resolved_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        Self::find_by_resolved_entity(self, space_id, entity_id, limit).await
    }

    async fn find_by_resolved_entity_pair(
        &self,
        space_id: &str,
        subject_entity: Option<&RecordId>,
        object_entity: Option<&RecordId>,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        Self::find_by_resolved_entity_pair(self, space_id, subject_entity, object_entity, limit)
            .await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl AssertionRepo for elephant_storage::MockAssertionRepository {
    async fn create(&self, record: Assertion) -> StorageResult<Assertion> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Assertion>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Assertion,
    ) -> StorageResult<Assertion> {
        Repository::update(self, space_id, id, record).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Assertion>> {
        Repository::list(self, space_id, limit, cursor).await
    }

    async fn find_by_resolved_entity(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        let mut items: Vec<_> = Repository::list(self, space_id, 10_000, None)
            .await?
            .into_iter()
            .filter(|item| {
                item.subject_entity
                    .as_ref()
                    .is_some_and(|link| link.id() == entity_id)
                    || item
                        .object_entity
                        .as_ref()
                        .is_some_and(|link| link.id() == entity_id)
            })
            .collect();
        if items.len() > limit {
            items.truncate(limit);
        }
        Ok(items)
    }

    async fn find_by_resolved_entity_pair(
        &self,
        space_id: &str,
        subject_entity: Option<&RecordId>,
        object_entity: Option<&RecordId>,
        limit: usize,
    ) -> StorageResult<Vec<Assertion>> {
        let mut items: Vec<_> = Repository::list(self, space_id, 10_000, None)
            .await?
            .into_iter()
            .filter(|item| {
                if let Some(entity_id) = subject_entity {
                    if item
                        .subject_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != entity_id)
                    {
                        return false;
                    }
                }
                if let Some(entity_id) = object_entity {
                    if item
                        .object_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != entity_id)
                    {
                        return false;
                    }
                }
                true
            })
            .collect();
        if items.len() > limit {
            items.truncate(limit);
        }
        Ok(items)
    }
}

impl AssertionsState {
    /// Creates new assertions state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            assertion_repo: Arc::new(AssertionRepository::new(client)),
            authz,
        }
    }

    /// Creates assertions state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(assertion_repo: Arc<dyn AssertionRepo>, authz: AuthzState) -> Self {
        Self {
            assertion_repo,
            authz,
        }
    }
}

/// Assertion search request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionSearchRequest {
    /// Free-text query across subject, predicate, object text, and object literal.
    #[serde(default)]
    pub query: Option<String>,
    /// Filter by resolution status.
    #[serde(default)]
    pub status: Option<ResolutionStatus>,
    /// Filter by source ID.
    #[serde(default)]
    pub source_id: Option<String>,
    /// Filter by predicate.
    #[serde(default)]
    pub predicate: Option<String>,
    /// Filter by resolved subject entity ID.
    #[serde(default)]
    pub subject_entity: Option<String>,
    /// Filter by resolved object entity ID.
    #[serde(default)]
    pub object_entity: Option<String>,
    /// Filter by resolved entity ID appearing as either subject or object.
    #[serde(default)]
    pub entity_id: Option<String>,
    /// Include soft-deleted assertions.
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

#[allow(clippy::result_large_err)]
fn parse_optional_filter_id(
    raw: Option<&str>,
    table: TableName,
    error_code: &str,
) -> Result<Option<RecordId>, (StatusCode, Json<ApiError>)> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            parse_id(value, table).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError {
                        code: error_code.to_string(),
                        message: e.to_string(),
                        details: None,
                    }),
                )
            })
        })
        .transpose()
}

/// Assertion search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionSearchResponse {
    /// Search results.
    pub results: Vec<Assertion>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    pub total: usize,
}

/// Resolution update request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionUpdateRequest {
    /// New resolution status.
    pub status: ResolutionStatus,
    /// Optional rationale for the status change.
    #[serde(default)]
    pub rationale: Option<String>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// POST /v1/assertions/search - Search assertions.
#[allow(clippy::too_many_lines)]
async fn search_assertions(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<AssertionSearchRequest>,
) -> ApiResult<AssertionSearchResponse> {
    let space_id = require_space_id(&principal)?;

    let query = req
        .query
        .as_deref()
        .map(str::trim)
        .filter(|q| !q.is_empty());
    let query_lower = query.map(str::to_ascii_lowercase);
    let query_terms = query_lower
        .as_deref()
        .map(assertion_search_terms)
        .unwrap_or_default();
    let source_filter = parse_optional_filter_id(
        req.source_id.as_deref(),
        TableName::Source,
        "INVALID_SOURCE_ID",
    )?;
    let subject_entity_filter = parse_optional_filter_id(
        req.subject_entity.as_deref(),
        TableName::Entity,
        "INVALID_SUBJECT_ENTITY_ID",
    )?;
    let object_entity_filter = parse_optional_filter_id(
        req.object_entity.as_deref(),
        TableName::Entity,
        "INVALID_OBJECT_ENTITY_ID",
    )?;
    let entity_filter = parse_optional_filter_id(
        req.entity_id.as_deref(),
        TableName::Entity,
        "INVALID_ENTITY_ID",
    )?;

    if req.cursor.is_none()
        && query.is_none()
        && (entity_filter.is_some()
            || subject_entity_filter.is_some()
            || object_entity_filter.is_some())
    {
        let fetch_limit = req.limit.saturating_add(1);
        let assertions = if let Some(ref entity_id) = entity_filter {
            state
                .assertion_repo
                .find_by_resolved_entity(&space_id, entity_id, fetch_limit)
                .await
        } else {
            state
                .assertion_repo
                .find_by_resolved_entity_pair(
                    &space_id,
                    subject_entity_filter.as_ref(),
                    object_entity_filter.as_ref(),
                    fetch_limit,
                )
                .await
        }
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        let filtered: Vec<Assertion> = assertions
            .into_iter()
            .filter(|a| {
                if let Some(ref status) = req.status {
                    if &a.resolution_status != status {
                        return false;
                    }
                }
                if let Some(ref predicate) = req.predicate {
                    if &a.predicate_raw != predicate {
                        return false;
                    }
                }
                if let Some(ref source_id) = source_filter {
                    if !a
                        .base
                        .provenance
                        .sources
                        .iter()
                        .any(|source| source == source_id)
                    {
                        return false;
                    }
                }
                if let Some(ref subject_entity) = subject_entity_filter {
                    if a.subject_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != subject_entity)
                    {
                        return false;
                    }
                }
                if let Some(ref object_entity) = object_entity_filter {
                    if a.object_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != object_entity)
                    {
                        return false;
                    }
                }
                true
            })
            .collect();
        let total = filtered.len();
        let mut visible = state
            .authz
            .filter_records(&principal, filtered, "read:assertion");
        let has_more = visible.len() > req.limit;
        if has_more {
            visible.truncate(req.limit);
        }
        let cursor = if has_more {
            visible.last().map(|a| a.base.id.to_string())
        } else {
            None
        };
        return Ok(Json(AssertionSearchResponse {
            results: visible,
            cursor,
            has_more,
            total,
        }));
    }

    let mut ranked_cursor = 0usize;
    let mut cursor = None;
    if query.is_some() {
        if let Some(value) = req.cursor.as_ref() {
            ranked_cursor = value.parse::<usize>().map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError {
                        code: "INVALID_CURSOR".to_string(),
                        message: format!("invalid ranked search cursor: {e}"),
                        details: None,
                    }),
                )
            })?;
        }
    } else {
        cursor = match req.cursor.as_ref() {
            Some(value) => Some(parse_id(value, TableName::Assertion).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError {
                        code: "INVALID_CURSOR".to_string(),
                        message: e.to_string(),
                        details: None,
                    }),
                )
            })?),
            None => None,
        };
    }

    let batch_size = if query.is_some() {
        2_000
    } else {
        req.limit.saturating_add(1).max(2_000)
    };
    let mut visible = Vec::new();
    let mut scored = Vec::new();
    let mut total = 0usize;

    loop {
        let assertions = state
            .assertion_repo
            .list(&space_id, batch_size, cursor.as_ref())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;

        if assertions.is_empty() {
            break;
        }

        let batch_len = assertions.len();
        cursor = assertions.last().map(|assertion| assertion.base.id.clone());

        let filtered_batch: Vec<Assertion> = assertions
            .into_iter()
            .filter(|a| {
                if let Some(ref status) = req.status {
                    if &a.resolution_status != status {
                        return false;
                    }
                }
                if let Some(ref predicate) = req.predicate {
                    if &a.predicate_raw != predicate {
                        return false;
                    }
                }
                if let Some(ref source_id) = source_filter {
                    if !a
                        .base
                        .provenance
                        .sources
                        .iter()
                        .any(|source| source == source_id)
                    {
                        return false;
                    }
                }
                if let Some(ref subject_entity) = subject_entity_filter {
                    if a.subject_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != subject_entity)
                    {
                        return false;
                    }
                }
                if let Some(ref object_entity) = object_entity_filter {
                    if a.object_entity
                        .as_ref()
                        .is_none_or(|link| link.id() != object_entity)
                    {
                        return false;
                    }
                }
                if let Some(ref entity_id) = entity_filter {
                    let is_subject = a
                        .subject_entity
                        .as_ref()
                        .is_some_and(|link| link.id() == entity_id);
                    let is_object = a
                        .object_entity
                        .as_ref()
                        .is_some_and(|link| link.id() == entity_id);
                    if !is_subject && !is_object {
                        return false;
                    }
                }
                true
            })
            .collect();

        let filtered_batch =
            state
                .authz
                .filter_records(&principal, filtered_batch, "read:assertion");

        if let Some(query_lower) = query_lower.as_deref() {
            for assertion in filtered_batch {
                let text = assertion_search_text(&assertion);
                let score = assertion_search_score(&text, query_lower, &query_terms);
                if score > 0 {
                    scored.push((score, assertion));
                }
            }
        } else {
            total = total.saturating_add(filtered_batch.len());
            for assertion in filtered_batch {
                if visible.len() < req.limit.saturating_add(1) {
                    visible.push(assertion);
                }
            }
        }

        if batch_len < batch_size {
            break;
        }
    }

    if query.is_some() {
        scored.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| right.base.created_at.cmp(&left.base.created_at))
                .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
        });
        total = scored.len();
        visible = scored
            .into_iter()
            .skip(ranked_cursor)
            .take(req.limit.saturating_add(1))
            .map(|(_, assertion)| assertion)
            .collect();
    }

    let has_more = visible.len() > req.limit;
    if has_more {
        visible.truncate(req.limit);
    }

    let next_cursor = if has_more && query.is_some() {
        Some(ranked_cursor.saturating_add(req.limit).to_string())
    } else if has_more {
        visible.last().map(|a| a.base.id.to_string())
    } else {
        None
    };

    Ok(Json(AssertionSearchResponse {
        results: visible,
        cursor: next_cursor,
        has_more,
        total,
    }))
}

fn assertion_search_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| term.len() > 1)
        .map(str::to_string)
        .collect()
}

fn assertion_search_text(assertion: &Assertion) -> String {
    let mut parts = vec![
        assertion.subject_text.as_str(),
        assertion.predicate_raw.as_str(),
    ];
    if let Some(object_text) = assertion.object_text.as_deref() {
        parts.push(object_text);
    }
    let object_literal = assertion
        .object_literal
        .as_ref()
        .map(serde_json::Value::to_string);
    if let Some(object_literal) = object_literal.as_deref() {
        parts.push(object_literal);
    }
    parts.join(" ").replace('_', " ").to_ascii_lowercase()
}

fn assertion_search_score(text: &str, query: &str, terms: &[String]) -> usize {
    if query.is_empty() {
        return 0;
    }
    let normalized_query = query.replace('_', " ");
    let phrase_score = if text.contains(&normalized_query) {
        terms.len().saturating_add(1).max(1) * 10
    } else {
        0
    };
    let matched_terms = terms
        .iter()
        .filter(|term| text.contains(term.as_str()))
        .count();
    phrase_score.saturating_add(matched_terms)
}

/// GET /v1/assertions/{id} - Get assertion by ID.
async fn get_assertion(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Assertion> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Assertion).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    match state.assertion_repo.get(&space_id, &rid).await {
        Ok(Some(assertion)) => {
            state
                .authz
                .authorize_record_read(&principal, &assertion, "read:assertion")
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
            Ok(Json(assertion))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Assertion", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/assertions - Create manual assertion.
async fn create_assertion(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(assertion): Json<Assertion>,
) -> Result<(StatusCode, Json<Assertion>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut assertion = assertion;
    assertion.base.space_id = require_space_id(&principal)?;

    state
        .authz
        .authorize_record_write(&principal, &assertion, "write:assertion")
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

    match state.assertion_repo.create(assertion).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// PATCH /v1/assertions/{id}/resolution - Update resolution status.
async fn update_resolution(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(req): Json<ResolutionUpdateRequest>,
) -> ApiResult<Assertion> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Assertion).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let mut assertion = match state.assertion_repo.get(&space_id, &rid).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Assertion", &id)),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ))
        }
    };

    state
        .authz
        .authorize_record_write(&principal, &assertion, "write:assertion")
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

    assertion.resolution_status = req.status;
    if let Some(rationale) = req.rationale {
        assertion.retry_state.abstention_reason = Some(rationale);
    }

    match state
        .assertion_repo
        .update(&space_id, &rid, assertion)
        .await
    {
        Ok(updated) => Ok(Json(updated)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// Returns assertion routes.
pub fn assertion_routes() -> Router<Arc<AssertionsState>> {
    Router::new()
        .route("/v1/assertions/search", post(search_assertions))
        .route("/v1/assertions", post(create_assertion))
        .route("/v1/assertions/{id}", get(get_assertion))
        .route("/v1/assertions/{id}/resolution", patch(update_resolution))
}

#[cfg(test)]
#[path = "assertions_test.rs"]
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
    fn test_assertion_search_request_defaults() {
        let json = r"{}";
        let req: AssertionSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
    }
}
