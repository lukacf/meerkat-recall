//! Timeline API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/timeline/search - Search events
//! - GET /v1/timeline/recent - Get recent events
//! - GET /v1/timeline/upcoming - Get upcoming events
//! - POST /v1/timeline - Create event
//! - PUT /v1/timeline/{id} - Update event
//! - DELETE /v1/timeline/{id} - Soft delete event
//! - POST /v1/timeline/{id}/restore - Restore soft-deleted event
//! - POST /v1/timeline/batch - Batch operations

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_policy::RecordAttributes;
#[cfg(test)]
use elephant_storage::MockEventRepository;
use elephant_storage::{
    parse_id, EventRepository, PaginatedResult, Repository, SearchCriteria, SearchRepository,
    StorageClient, StorageResult,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::{Event, EventStatus};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::{ApiError, BatchError, BatchRequest, BatchResponse};
use crate::routes::require_space_id;

/// Shared state for timeline handlers.
pub struct TimelineState {
    /// Event repository.
    pub event_repo: Arc<dyn TimelineEventRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// Timeline event repository interface used by the API layer.
#[async_trait::async_trait]
pub trait TimelineEventRepo: Send + Sync {
    /// Restores only if the row still matches the authorized snapshot.
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Event,
    ) -> StorageResult<Event>;
    async fn create(&self, record: Event) -> StorageResult<Event>;
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Event>>;
    async fn update(&self, space_id: &str, id: &RecordId, record: Event) -> StorageResult<Event>;
    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()>;
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Event>>;

    async fn get_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>>;
    async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<Event>>;

    async fn get_with_deleted(&self, space_id: &str, id: &RecordId)
        -> StorageResult<Option<Event>>;
    async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Event>;
}

#[async_trait::async_trait]
impl TimelineEventRepo for EventRepository {
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Event,
    ) -> StorageResult<Event> {
        Self::restore_guarded(self, space_id, id, authorized).await
    }

    async fn create(&self, record: Event) -> StorageResult<Event> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Event>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Event) -> StorageResult<Event> {
        Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Event>> {
        SearchRepository::search(self, criteria).await
    }

    async fn get_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>> {
        Self::get_recent(self, space_id, limit).await
    }

    async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<Event>> {
        Self::get_upcoming(self, space_id, days).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Event>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Event> {
        Self::restore(self, space_id, id).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl TimelineEventRepo for MockEventRepository {
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Event,
    ) -> StorageResult<Event> {
        let current = Self::get_with_deleted(self, space_id, id).await?;
        let unchanged = current.is_some_and(|current| {
            elephant_storage::base_envelope_unchanged(&current.base, &authorized.base)
        });
        if !unchanged {
            return Err(elephant_storage::StorageError::Conflict(format!(
                "Event changed since it was authorized: {id}"
            )));
        }
        Self::restore(self, space_id, id).await
    }

    async fn create(&self, record: Event) -> StorageResult<Event> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Event>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(&self, space_id: &str, id: &RecordId, record: Event) -> StorageResult<Event> {
        Repository::update(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Event>> {
        SearchRepository::search(self, criteria).await
    }

    async fn get_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>> {
        Self::get_recent(self, space_id, limit).await
    }

    async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<Event>> {
        Self::get_upcoming(self, space_id, days).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Event>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Event> {
        Self::restore(self, space_id, id).await
    }
}

impl TimelineState {
    /// Creates new timeline state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            event_repo: Arc::new(EventRepository::new(client)),
            authz,
        }
    }

    /// Creates timeline state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(event_repo: Arc<dyn TimelineEventRepo>, authz: AuthzState) -> Self {
        Self { event_repo, authz }
    }
}

/// Event search request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSearchRequest {
    /// Free-text query.
    #[serde(default)]
    pub query: Option<String>,
    /// Filter by entity IDs.
    #[serde(default)]
    pub entity_ids: Option<Vec<String>>,
    /// Filter mode for entity IDs (any or all).
    #[serde(default)]
    pub mode: Option<String>,
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted events.
    #[serde(default)]
    pub include_deleted: bool,
    /// Whether to include candidate events.
    #[serde(default)]
    pub include_candidates: bool,
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
pub struct EventSearchResponse {
    /// Search results.
    pub results: Vec<Event>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    pub total: usize,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

fn event_record_attributes(event: &Event) -> RecordAttributes {
    RecordAttributes {
        space_id: event.base.space_id.clone(),
        security: event.base.security.clone(),
        subjects: event.base.subjects.clone(),
        subjects_state: event.base.subjects_state,
    }
}

fn authorize_event(
    state: &TimelineState,
    principal: &AuthPrincipal,
    event: &Event,
    scope: &str,
) -> Result<(), ApiError> {
    state
        .authz
        .evaluate(principal, &event_record_attributes(event), scope)
        .map_err(|e| ApiError {
            code: e.code,
            message: e.message,
            details: None,
        })
}

fn filter_authorized_events(
    state: &TimelineState,
    principal: &AuthPrincipal,
    events: Vec<Event>,
    scope: &str,
) -> Vec<Event> {
    events
        .into_iter()
        .filter(|event| {
            state
                .authz
                .evaluate(principal, &event_record_attributes(event), scope)
                .is_ok()
        })
        .collect()
}

/// POST /v1/timeline/search - Search events.
async fn search_events(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<EventSearchRequest>,
) -> ApiResult<EventSearchResponse> {
    let include_candidates = req.include_candidates;
    let space_id = require_space_id(&principal)?;

    if let Some(req_space_id) = &req.space_id {
        if req_space_id != &space_id {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access to space denied")),
            ));
        }
    }

    let mut criteria = SearchCriteria::new()
        .with_limit(req.limit)
        .with_space_id(space_id);
    if let Some(q) = req.query {
        criteria = criteria.with_query(q);
    }
    if req.include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(c) = req.cursor {
        criteria = criteria.with_cursor(c);
    }
    if let Some(entities) = req.entity_ids {
        criteria = criteria.with_entities(entities);
    }
    if let Some(mode) = req.mode {
        criteria = criteria.with_mode(mode);
    }

    let result = state.event_repo.search(&criteria).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    let mut results = result.results;
    if !include_candidates {
        results.retain(|event| event.status != EventStatus::Candidate);
    }
    let filtered = filter_authorized_events(state.as_ref(), &principal, results, "read:event");
    let total = filtered.len();

    Ok(Json(EventSearchResponse {
        results: filtered,
        cursor: result.cursor,
        has_more: result.has_more,
        total,
    }))
}

/// POST /v1/timeline - Create event.
async fn create_event(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(event): Json<Event>,
) -> Result<(StatusCode, Json<Event>), (StatusCode, Json<ApiError>)> {
    let mut event = event;
    let space_id = require_space_id(&principal)?;

    // Override space_id with principal's space to prevent cross-space writes
    event.base.space_id.clone_from(&space_id);

    authorize_event(state.as_ref(), &principal, &event, "write:event")
        .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;

    match state.event_repo.create(event).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/timeline/{id} - Get event by ID.
async fn get_event(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Event> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Event).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.event_repo.get(&space_id, &rid).await {
        Ok(Some(event)) => {
            authorize_event(state.as_ref(), &principal, &event, "read:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;
            Ok(Json(event))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Event", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// PUT /v1/timeline/{id} - Update event.
async fn update_event(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(event): Json<Event>,
) -> ApiResult<Event> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Event).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.event_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            authorize_event(state.as_ref(), &principal, &existing, "write:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;
            authorize_event(state.as_ref(), &principal, &event, "write:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;

            match state.event_repo.update(&space_id, &rid, event).await {
                Ok(updated) => Ok(Json(updated)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Event", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// DELETE /v1/timeline/{id} - Soft delete event.
async fn delete_event(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Event).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.event_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            authorize_event(state.as_ref(), &principal, &existing, "write:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;

            match state.event_repo.delete(&space_id, &rid).await {
                Ok(()) => Ok(StatusCode::NO_CONTENT),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Event", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/timeline/{id}/restore - Restore soft-deleted event.
async fn restore_event(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Event> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Event).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.event_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            authorize_event(state.as_ref(), &principal, &existing, "write:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;
            // The response carries the restored event, so read is required too.
            authorize_event(state.as_ref(), &principal, &existing, "read:event")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(e)))?;

            match state
                .event_repo
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
            Json(ApiError::not_found("Event", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/timeline/recent - Get recent events.
async fn get_recent(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<ListCandidatesQuery>,
) -> ApiResult<Vec<Event>> {
    let space_id = require_space_id(&principal)?;
    match state.event_repo.get_recent(&space_id, query.limit).await {
        Ok(mut events) => {
            if !query.include_candidates {
                events.retain(|event| event.status != EventStatus::Candidate);
            }
            let filtered =
                filter_authorized_events(state.as_ref(), &principal, events, "read:event");
            Ok(Json(filtered))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// Query parameters for listing candidates.
#[derive(Debug, Clone, Deserialize)]
pub struct ListCandidatesQuery {
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Whether to include candidate events.
    #[serde(default)]
    pub include_candidates: bool,
}

/// GET /v1/timeline/upcoming - Get upcoming events.
async fn get_upcoming(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<UpcomingQuery>,
) -> ApiResult<Vec<Event>> {
    let space_id = require_space_id(&principal)?;
    match state
        .event_repo
        .get_upcoming(&space_id, query.days.unwrap_or(7))
        .await
    {
        Ok(mut events) => {
            if !query.include_candidates {
                events.retain(|event| event.status != EventStatus::Candidate);
            }
            let filtered =
                filter_authorized_events(state.as_ref(), &principal, events, "read:event");
            Ok(Json(filtered))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// Query parameters for upcoming events.
#[derive(Debug, Clone, Deserialize)]
pub struct UpcomingQuery {
    /// Time window in days.
    pub days: Option<u32>,
    /// Whether to include candidate events.
    #[serde(default)]
    pub include_candidates: bool,
}

/// POST /v1/timeline/batch - Batch operations.
async fn batch_events(
    State(state): State<Arc<TimelineState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<BatchRequest<Event>>,
) -> ApiResult<BatchResponse<Event>> {
    let space_id = require_space_id(&principal)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (index, event) in req.items.into_iter().enumerate() {
        let mut event = event;
        event.base.space_id.clone_from(&space_id);
        let rid = event.base.id.clone();

        let result = match state.event_repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(existing)) => {
                if let Err(e) =
                    authorize_event(state.as_ref(), &principal, &existing, "write:event")
                {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                if let Err(e) = authorize_event(state.as_ref(), &principal, &event, "write:event") {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                if existing.base.deleted_at.is_some() {
                    if let Err(e) = state.event_repo.restore(&space_id, &rid).await {
                        failed.push(BatchError {
                            index,
                            error: format!("restore failed: {e}"),
                        });
                        continue;
                    }
                }
                state.event_repo.update(&space_id, &rid, event).await
            }
            Ok(None) => {
                if let Err(e) = authorize_event(state.as_ref(), &principal, &event, "write:event") {
                    failed.push(BatchError {
                        index,
                        error: e.message,
                    });
                    continue;
                }
                state.event_repo.create(event).await
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

/// Returns timeline routes.
#[must_use]
#[allow(clippy::double_must_use)]
pub fn timeline_routes() -> Router<Arc<TimelineState>> {
    Router::new()
        .route("/v1/timeline/search", post(search_events))
        .route("/v1/timeline/recent", get(get_recent))
        .route("/v1/timeline/upcoming", get(get_upcoming))
        .route("/v1/timeline", post(create_event))
        .route("/v1/timeline/batch", post(batch_events))
        .route("/v1/timeline/{id}", get(get_event))
        .route("/v1/timeline/{id}", put(update_event))
        .route("/v1/timeline/{id}", delete(delete_event))
        .route("/v1/timeline/{id}/restore", post(restore_event))
}

#[cfg(test)]
#[path = "timeline_test.rs"]
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
    fn test_event_search_request_defaults() {
        let json = r"{}";
        let req: EventSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
        assert!(!req.include_candidates);
    }
}
