//! Event streaming routes for system events.
//!
//! Supports both JSON polling and Server-Sent Events (SSE) streaming.

#[cfg(test)]
#[path = "events_test.rs"]
mod integration_tests;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::stream::{self};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use elephant_outbox::StreamCursor;
use elephant_storage::{StorageClient, StorageResult, SystemEventRepository};
use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::SystemEvent;

use crate::routes::entities::ApiError;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;

/// Event streaming state.
pub struct EventsState {
    /// Repository for system events.
    pub system_event_repo: Arc<dyn SystemEventRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// System event repository interface used by the API layer.
#[async_trait::async_trait]
pub trait SystemEventRepo: Send + Sync {
    async fn get_after_seq(
        &self,
        space_id: &str,
        after_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>>;

    async fn get_before_seq(
        &self,
        space_id: &str,
        before_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>>;
}

#[async_trait::async_trait]
impl SystemEventRepo for SystemEventRepository {
    async fn get_after_seq(
        &self,
        space_id: &str,
        after_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        Self::get_after_seq(self, space_id, after_seq, limit).await
    }

    async fn get_before_seq(
        &self,
        space_id: &str,
        before_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        Self::get_before_seq(self, space_id, before_seq, limit).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl SystemEventRepo for elephant_storage::MockSystemEventRepository {
    async fn get_after_seq(
        &self,
        space_id: &str,
        after_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        Self::get_after_seq(self, space_id, after_seq, limit).await
    }

    async fn get_before_seq(
        &self,
        space_id: &str,
        before_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        Self::get_before_seq(self, space_id, before_seq, limit).await
    }
}

impl EventsState {
    /// Creates new events state.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            system_event_repo: Arc::new(SystemEventRepository::new(client)),
            authz,
        }
    }

    /// Creates events state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(system_event_repo: Arc<dyn SystemEventRepo>, authz: AuthzState) -> Self {
        Self {
            system_event_repo,
            authz,
        }
    }
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// Query parameters for event streaming.
#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    /// Cursor to resume from (base64 encoded).
    pub cursor: Option<String>,
    /// Maximum number of events to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

const fn default_limit() -> usize {
    100
}

/// Response from the stream endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamResponse {
    /// Events in this batch.
    pub events: Vec<SystemEvent>,
    /// Cursor for the next batch.
    pub next_cursor: Option<String>,
    /// Whether there are more events.
    pub has_more: bool,
}

/// Creates the events routes.
#[must_use]
#[allow(clippy::double_must_use)]
pub fn events_routes() -> Router<Arc<EventsState>> {
    Router::new()
        .route("/v1/events/stream", get(get_event_stream))
        .route("/v1/events/sse", get(get_event_stream_sse))
        .route("/v1/events/latest", get(get_latest_seq))
}

/// GET /v1/events/stream - Stream system events.
#[allow(clippy::too_many_lines)]
async fn get_event_stream(
    State(state): State<Arc<EventsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<StreamQuery>,
) -> ApiResult<StreamResponse> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: missing space_id".to_string(),
                details: None,
            }),
        )
    })?;

    let security = Security {
        policy_id: state
            .authz
            .to_abac_principal(&principal)
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone()),
        level: 0,
        labels: Vec::new(),
        handling: Vec::new(),
    };
    let record = elephant_policy::RecordAttributes {
        space_id: space_id.clone(),
        security,
        subjects: Vec::new(),
        subjects_state: SubjectsState::Complete,
    };
    state
        .authz
        .evaluate(&principal, &record, "read:event")
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

    // Parse cursor if provided
    let after_seq = if let Some(cursor_str) = &query.cursor {
        match StreamCursor::decode(cursor_str) {
            Ok(cursor) => {
                // Validate space_id matches
                if cursor.space_id != space_id {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::validation("Cursor space_id does not match query")),
                    ));
                }
                cursor.seq
            }
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(format!("Invalid cursor: {e}"))),
                ));
            }
        }
    } else {
        0
    };

    let limit = query.limit.clamp(1, 1000);
    let mut collected: Vec<SystemEvent> = Vec::new();
    let mut scan_seq = after_seq;
    let mut safety_limit = 0usize;

    while collected.len() < limit {
        safety_limit += 1;
        if safety_limit > 1000 {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Event stream pagination exceeded safety limit".to_string(),
                )),
            ));
        }

        let page = match state
            .system_event_repo
            .get_after_seq(&space_id, scan_seq, limit)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                ));
            }
        };

        if page.is_empty() {
            break;
        }

        scan_seq = page.last().map_or(scan_seq, |e| e.seq);

        let filtered = state.authz.filter_records(&principal, page, "read:event");
        for event in filtered {
            if collected.len() >= limit {
                break;
            }
            collected.push(event);
        }

        if collected.len() >= limit {
            break;
        }
    }

    let last_seq = collected.last().map(|e| e.seq);

    let has_more = if collected.len() < limit {
        false
    } else {
        match last_seq {
            None => false,
            Some(last_seq) => {
                let mut has_more_authorized = false;
                let mut scan_seq = last_seq;
                let mut safety_limit = 0usize;

                loop {
                    safety_limit += 1;
                    if safety_limit > 1000 {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiError::internal(
                                "Event stream has_more scan exceeded safety limit".to_string(),
                            )),
                        ));
                    }

                    let page = state
                        .system_event_repo
                        .get_after_seq(&space_id, scan_seq, 100)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(e.to_string())),
                            )
                        })?;

                    if page.is_empty() {
                        break;
                    }

                    scan_seq = page.last().map_or(scan_seq, |e| e.seq);
                    let filtered = state.authz.filter_records(&principal, page, "read:event");
                    if !filtered.is_empty() {
                        has_more_authorized = true;
                        break;
                    }
                }

                has_more_authorized
            }
        }
    };

    let next_cursor = if has_more {
        last_seq.and_then(|seq| StreamCursor::new(space_id.clone(), seq).encode().ok())
    } else {
        None
    };

    let events = collected;

    Ok(Json(StreamResponse {
        events,
        next_cursor,
        has_more,
    }))
}

/// Query parameters for SSE streaming.
#[derive(Debug, Clone, Deserialize)]
pub struct SseStreamQuery {
    /// Cursor to resume from (base64 encoded).
    pub cursor: Option<String>,
    /// Poll interval in milliseconds (default 1000ms).
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
}

const fn default_poll_interval() -> u64 {
    1000
}

/// GET /v1/events/sse - Server-Sent Events stream for system events.
#[allow(clippy::too_many_lines)]
async fn get_event_stream_sse(
    State(state): State<Arc<EventsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<SseStreamQuery>,
) -> impl IntoResponse {
    let Some(space_id) = principal.space_id.clone() else {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: missing space_id".to_string(),
                details: None,
            }),
        )
            .into_response();
    };

    let security = Security {
        policy_id: state
            .authz
            .to_abac_principal(&principal)
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone()),
        level: 0,
        labels: Vec::new(),
        handling: Vec::new(),
    };
    let record = elephant_policy::RecordAttributes {
        space_id: space_id.clone(),
        security,
        subjects: Vec::new(),
        subjects_state: SubjectsState::Complete,
    };
    if let Err(e) = state.authz.evaluate(&principal, &record, "read:event") {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: e.code,
                message: e.message,
                details: None,
            }),
        )
            .into_response();
    }

    // Parse initial cursor if provided and validate it matches the principal's space.
    let initial_seq = if let Some(cursor_str) = &query.cursor {
        match StreamCursor::decode(cursor_str) {
            Ok(cursor) => {
                if cursor.space_id != space_id {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiError::validation("Cursor space_id does not match query")),
                    )
                        .into_response();
                }
                cursor.seq
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(format!("Invalid cursor: {e}"))),
                )
                    .into_response();
            }
        }
    } else {
        0
    };
    let poll_interval = Duration::from_millis(query.poll_interval_ms);

    // Create the event stream
    let stream = stream::unfold(
        (
            state,
            space_id,
            initial_seq,
            initial_seq,
            poll_interval,
            principal,
        ),
        |(state, space_id, mut last_scan_seq, mut last_auth_seq, poll_interval, principal)| async move {
            loop {
                // Fetch new events after last_seq
                match state
                    .system_event_repo
                    .get_after_seq(&space_id, last_scan_seq, 100)
                    .await
                {
                    Ok(events) if !events.is_empty() => {
                        // Track the new last sequence from the UNFILTERED batch to ensure we progress
                        let new_last_scan_seq = events.last().map_or(last_scan_seq, |e| e.seq);

                        // Filter results by authorization
                        let filtered = state.authz.filter_records(&principal, events, "read:event");

                        last_scan_seq = new_last_scan_seq;

                        if filtered.is_empty() {
                            // All events in this batch filtered out, just advance and keep polling without
                            // emitting an event to avoid leaking that events occurred.
                            continue;
                        }

                        last_auth_seq = filtered.last().map_or(last_auth_seq, |e| e.seq);

                        // Create SSE event with JSON payload for authorized events
                        let payload = serde_json::json!({
                            "events": filtered,
                            "cursor": StreamCursor::new(space_id.clone(), last_auth_seq)
                                .encode()
                                .unwrap_or_default(),
                        });

                        let event = Event::default().event("events").data(payload.to_string());

                        return Some((
                            Ok::<Event, std::convert::Infallible>(event),
                            (
                                state,
                                space_id,
                                last_scan_seq,
                                last_auth_seq,
                                poll_interval,
                                principal,
                            ),
                        ));
                    }
                    Ok(_) => {
                        // No new events, wait and try again
                        sleep(poll_interval).await;
                    }
                    Err(e) => {
                        // Error fetching events, send error event
                        let event = Event::default()
                            .event("error")
                            .data(format!(r#"{{"error":"{e}"}}"#));

                        return Some((
                            Ok::<Event, std::convert::Infallible>(event),
                            (
                                state,
                                space_id,
                                last_scan_seq,
                                last_auth_seq,
                                poll_interval,
                                principal,
                            ),
                        ));
                    }
                }
            }
        },
    );

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

/// Query parameters for latest seq.
#[derive(Debug, Deserialize)]
pub struct LatestSeqQuery {
    /// Space ID to get the latest sequence number for.
    pub space_id: String,
}

/// Response with latest sequence number.
#[derive(Debug, Serialize, Deserialize)]
pub struct LatestSeqResponse {
    /// Space ID.
    pub space_id: String,
    /// Latest sequence number.
    pub seq: i64,
}

/// GET /v1/events/latest - Get latest sequence number.
async fn get_latest_seq(
    State(state): State<Arc<EventsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<LatestSeqQuery>,
) -> ApiResult<LatestSeqResponse> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: missing space_id".to_string(),
                details: None,
            }),
        )
    })?;
    if space_id != query.space_id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access to space denied".to_string(),
                details: None,
            }),
        ));
    }

    let security = Security {
        policy_id: state
            .authz
            .to_abac_principal(&principal)
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone()),
        level: 0,
        labels: Vec::new(),
        handling: Vec::new(),
    };
    let record = elephant_policy::RecordAttributes {
        space_id: space_id.clone(),
        security,
        subjects: Vec::new(),
        subjects_state: SubjectsState::Complete,
    };
    state
        .authz
        .evaluate(&principal, &record, "read:event")
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

    let mut before_seq = i64::MAX;
    let mut safety_limit = 0usize;

    let seq = loop {
        safety_limit += 1;
        if safety_limit > 1000 {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Latest seq scan exceeded safety limit".to_string(),
                )),
            ));
        }

        let page = state
            .system_event_repo
            .get_before_seq(&query.space_id, before_seq, 100)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;

        if page.is_empty() {
            break 0;
        }

        let oldest_seq_in_page = page.last().map_or(before_seq, |e| e.seq);

        let filtered = state.authz.filter_records(&principal, page, "read:event");
        if let Some(event) = filtered.first() {
            break event.seq;
        }

        before_seq = oldest_seq_in_page;
    };

    Ok(Json(LatestSeqResponse {
        space_id: query.space_id,
        seq,
    }))
}
