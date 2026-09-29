//! Truth slot REST routes.
//!
//! Implements the following endpoints:
//! - GET /v1/truth-slots - List truth slots
//! - GET /v1/truth-slots/{id}/history - Get truth slot with conflict group

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::{
    parse_id, ConflictGroupRepository, Repository, StorageClient, TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::truth::{ConflictGroup, TruthSlot};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;

/// Shared state for truth slot handlers.
#[derive(Clone)]
pub struct TruthSlotsState {
    truth_slot_repo: TruthSlotRepository,
    conflict_group_repo: ConflictGroupRepository,
    authz: AuthzState,
}

impl TruthSlotsState {
    /// Creates new truth slots state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            truth_slot_repo: TruthSlotRepository::new(client.clone()),
            conflict_group_repo: ConflictGroupRepository::new(client),
            authz,
        }
    }
}

/// Query parameters for listing truth slots.
#[derive(Debug, Clone, Deserialize)]
pub struct TruthSlotQuery {
    /// Filter by subject entity ID.
    #[serde(default)]
    pub subject_entity_id: Option<String>,
    /// Filter by predicate ID.
    #[serde(default)]
    pub predicate_id: Option<String>,
    /// Only return stale slots.
    #[serde(default)]
    pub stale_only: Option<bool>,
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

/// Truth slot list response.
#[derive(Debug, Clone, Serialize)]
pub struct TruthSlotListResponse {
    pub results: Vec<TruthSlot>,
    pub total: usize,
    pub cursor: Option<String>,
}

/// Truth slot history response.
#[derive(Debug, Clone, Serialize)]
pub struct TruthSlotHistoryResponse {
    pub slot: TruthSlot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_group: Option<ConflictGroup>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// GET /v1/truth-slots - List truth slots.
async fn list_truth_slots(
    State(state): State<Arc<TruthSlotsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<TruthSlotQuery>,
) -> ApiResult<TruthSlotListResponse> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access denied: missing space_id")),
        )
    })?;

    let slots = if query.stale_only.unwrap_or(false) {
        state
            .truth_slot_repo
            .find_stale(&space_id, query.limit)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    } else if let Some(ref subject_entity_id) = query.subject_entity_id {
        state
            .truth_slot_repo
            .find_by_subject_entity(&space_id, subject_entity_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    } else {
        let cursor = query
            .cursor
            .as_ref()
            .and_then(|c| parse_id(c, TableName::TruthSlot).ok());
        state
            .truth_slot_repo
            .list(&space_id, query.limit, cursor.as_ref())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    };

    // Apply predicate filter if specified
    let slots = if let Some(ref predicate_id) = query.predicate_id {
        slots
            .into_iter()
            .filter(|s| s.predicate_id == *predicate_id)
            .collect()
    } else {
        slots
    };

    let filtered = state.authz.filter_records(&principal, slots, "read:entity");

    Ok(Json(TruthSlotListResponse {
        total: filtered.len(),
        results: filtered,
        cursor: None,
    }))
}

/// GET /v1/truth-slots/{id}/history - Get truth slot with conflict group.
async fn get_truth_slot_history(
    State(state): State<Arc<TruthSlotsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<TruthSlotHistoryResponse> {
    let space_id = principal.space_id.clone().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access denied: missing space_id")),
        )
    })?;

    let rid = parse_id(&id, TableName::TruthSlot).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let slot = state
        .truth_slot_repo
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
                Json(ApiError::not_found("TruthSlot", &id)),
            )
        })?;

    state
        .authz
        .authorize_record_read(&principal, &slot, "read:entity")
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

    let conflict_group = state
        .conflict_group_repo
        .find_unresolved_by_slot_key(&space_id, &slot.slot_key)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    Ok(Json(TruthSlotHistoryResponse {
        slot,
        conflict_group,
    }))
}

/// Returns truth slot routes.
pub fn truth_slot_routes() -> Router<Arc<TruthSlotsState>> {
    Router::new()
        .route("/v1/truth-slots", get(list_truth_slots))
        .route("/v1/truth-slots/{id}/history", get(get_truth_slot_history))
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
    fn test_truth_slot_query_defaults() {
        let json = r"{}";
        let query: TruthSlotQuery = serde_json::from_str(json).expect("deserialize");
        assert_eq!(query.limit, 50);
        assert!(query.subject_entity_id.is_none());
        assert!(query.predicate_id.is_none());
        assert!(query.stale_only.is_none());
    }

    #[test]
    fn test_truth_slot_list_response_serializes() {
        let response = TruthSlotListResponse {
            results: vec![],
            total: 0,
            cursor: None,
        };
        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("\"total\":0"));
    }
}
