//! Work item REST routes.
//!
//! Implements the following endpoints:
//! - GET /v1/work-items - List work items
//! - GET /v1/work-items/{id} - Get work item by ID
//! - POST /v1/work-items/{id}/retry - Retry a failed work item

use std::str::FromStr;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::{parse_id, Repository, StorageClient, WorkItemRepository};
use elephant_types::ids::TableName;
use elephant_types::records::work::{WorkItem, WorkItemStatus};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;

/// Shared state for work item handlers.
#[derive(Clone)]
pub struct WorkItemsState {
    work_item_repo: WorkItemRepository,
    authz: AuthzState,
}

impl WorkItemsState {
    /// Creates new work items state from a storage client.
    #[must_use]
    pub const fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            work_item_repo: WorkItemRepository::new(client),
            authz,
        }
    }
}

/// Query parameters for listing work items.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkItemQuery {
    /// Filter by work item kind.
    #[serde(default)]
    pub kind: Option<String>,
    /// Filter by status (pending, runnable, running, done, failed, cancelled).
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by minimum age in seconds.
    #[serde(default)]
    pub min_age_secs: Option<i64>,
    /// Maximum results to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
}

const fn default_limit() -> usize {
    50
}

/// Work item list response.
#[derive(Debug, Clone, Serialize)]
pub struct WorkItemListResponse {
    pub results: Vec<WorkItem>,
    pub total: usize,
    pub cursor: Option<String>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// GET /v1/work-items - List work items.
async fn list_work_items(
    State(state): State<Arc<WorkItemsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<WorkItemQuery>,
) -> ApiResult<WorkItemListResponse> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access denied: missing space_id")),
        )
    })?;

    let items = if let Some(status_raw) = &query.status {
        let status = match status_raw.as_str() {
            "completed" => WorkItemStatus::Done,
            other => WorkItemStatus::from_str(other).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(e.to_string())),
                )
            })?,
        };
        if let Some(kind) = query.kind.as_deref() {
            state
                .work_item_repo
                .find_by_status_and_kind(&space_id, status, kind, query.limit)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
        } else {
            state
                .work_item_repo
                .find_by_status(&space_id, status)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
        }
    } else {
        let cursor = query
            .cursor
            .as_ref()
            .and_then(|c| parse_id(c, TableName::WorkItem).ok());
        state
            .work_item_repo
            .list(&space_id, query.limit, cursor.as_ref())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    };

    // Apply min_age filter client-side if specified
    let items = if let Some(min_age_secs) = query.min_age_secs {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(min_age_secs);
        items
            .into_iter()
            .filter(|item| item.base.created_at <= cutoff)
            .collect()
    } else {
        items
    };

    let filtered = state
        .authz
        .filter_records(&principal, items, "read:work_item");

    Ok(Json(WorkItemListResponse {
        total: filtered.len(),
        results: filtered,
        cursor: None,
    }))
}

/// GET /v1/work-items/{id} - Get work item by ID.
async fn get_work_item(
    State(state): State<Arc<WorkItemsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<WorkItem> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access denied: missing space_id")),
        )
    })?;

    let rid = parse_id(&id, TableName::WorkItem).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let item = state
        .work_item_repo
        .get(&space_id, &rid)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("WorkItem", &id)),
            )
        })?;

    state
        .authz
        .authorize_record_read(&principal, &item, "read:work_item")
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

    Ok(Json(item))
}

/// POST /v1/work-items/{id}/retry - Retry a failed work item.
async fn retry_work_item(
    State(state): State<Arc<WorkItemsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<WorkItem> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access denied: missing space_id")),
        )
    })?;

    let rid = parse_id(&id, TableName::WorkItem).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let item = state
        .work_item_repo
        .get(&space_id, &rid)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("WorkItem", &id)),
            )
        })?;

    state
        .authz
        .authorize_record_read(&principal, &item, "read:work_item")
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
        .authorize_record_write(&principal, &item, "write:work_item")
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

    if item.status != WorkItemStatus::Failed {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(format!(
                "Work item status is {:?}, only failed items can be retried",
                item.status
            ))),
        ));
    }

    let updated =
        state
            .work_item_repo
            .retry_authorized(&item)
            .await
            .map_err(|error| match error {
                error @ elephant_storage::StorageError::ErasureInProgress => {
                    ApiError::storage(&error)
                }
                elephant_storage::StorageError::Conflict(message) => (
                    StatusCode::CONFLICT,
                    Json(ApiError {
                        code: "CONFLICT".into(),
                        message,
                        details: None,
                    }),
                ),
                error => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(error.to_string())),
                ),
            })?;
    Ok(Json(updated))
}

/// Returns work item routes.
pub fn work_item_routes() -> Router<Arc<WorkItemsState>> {
    Router::new()
        .route("/v1/work-items", get(list_work_items))
        .route("/v1/work-items/{id}", get(get_work_item))
        .route("/v1/work-items/{id}/retry", post(retry_work_item))
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

    #[test]
    fn test_work_item_query_defaults() {
        let json = r"{}";
        let query: WorkItemQuery = serde_json::from_str(json).expect("deserialize");
        assert_eq!(query.limit, 50);
        assert!(query.kind.is_none());
        assert!(query.status.is_none());
        assert!(query.min_age_secs.is_none());
    }

    #[test]
    fn test_work_item_list_response_serializes() {
        let response = WorkItemListResponse {
            results: vec![],
            total: 0,
            cursor: None,
        };
        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("\"total\":0"));
    }
}
