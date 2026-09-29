//! Identity API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/identity/candidates - Create identity candidate
//! - GET /v1/identity/candidates - List pending identity candidates
//! - POST /v1/identity/merge - Merge two entities

use std::{collections::HashSet, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use elephant_extensions::load_runtime_reference_registry_from_env;
use elephant_identity::apply_entity_merge_rewrites;
use elephant_storage::{
    parse_id, EntityIdentityKeyRepository, EntityMergeRepository, EntityRepository,
    IdentityCandidateRepository, RelRepository, Repository, SearchCriteria, SearchRepository,
    StorageClient, SystemEventRepository,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::{
    Entity, EntityMerge, IdentityCandidate, MergeStrategy, Rel, SystemEvent,
};
use elephant_types::Link;

use crate::routes::entities::ApiError;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::require_space_id;

#[allow(dead_code)]
fn remove_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                remove_nulls(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                remove_nulls(v);
            }
        }
        _ => {}
    }
}

fn merge_security(a: &Security, b: &Security) -> Security {
    let mut merged = a.clone();
    merged.level = merged.level.max(b.level);
    merged.labels.extend(b.labels.iter().cloned());
    merged.labels.sort();
    merged.labels.dedup();
    merged.handling.extend(b.handling.iter().cloned());
    merged.handling.sort();
    merged.handling.dedup();
    merged
}

fn merge_json_values(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Object(a_obj), serde_json::Value::Object(b_obj)) => {
            let mut merged = a_obj.clone();
            for (k, b_val) in b_obj {
                match merged.get(k) {
                    Some(a_val) => {
                        let merged_val = merge_json_values(a_val, b_val);
                        merged.insert(k.clone(), merged_val);
                    }
                    None => {
                        merged.insert(k.clone(), b_val.clone());
                    }
                }
            }
            serde_json::Value::Object(merged)
        }
        (serde_json::Value::Array(a_arr), serde_json::Value::Array(b_arr)) => {
            let mut merged = a_arr.clone();
            merged.extend(b_arr.iter().cloned());
            merged.sort_by_key(std::string::ToString::to_string);
            merged.dedup();
            serde_json::Value::Array(merged)
        }
        _ => {
            if a == b {
                a.clone()
            } else {
                let mut arr = vec![a.clone(), b.clone()];
                arr.sort_by_key(std::string::ToString::to_string);
                arr.dedup();
                serde_json::Value::Array(arr)
            }
        }
    }
}

/// Identity-specific application state.
#[derive(Clone)]
pub struct IdentityState {
    /// Shared storage client (used for transactions).
    pub storage: StorageClient,
    /// Identity candidate repository.
    pub candidate_repo: IdentityCandidateRepository,
    /// Entity repository for merge operations.
    pub entity_repo: EntityRepository,
    /// Relationship repository for repointing during merge.
    pub rel_repo: RelRepository,
    /// Event repository for repointing during merge.
    pub event_repo: SystemEventRepository,
    /// Entity merge repository for lineage tracking.
    pub merge_repo: EntityMergeRepository,
    /// Identity key repository (source-of-truth for stable keys).
    #[allow(dead_code)]
    pub identity_key_repo: EntityIdentityKeyRepository,
    /// Authorization service.
    pub authz: AuthzState,
}

impl IdentityState {
    /// Creates new identity state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            storage: client.clone(),
            candidate_repo: IdentityCandidateRepository::new(client.clone()),
            entity_repo: EntityRepository::new(client.clone()),
            rel_repo: RelRepository::new(client.clone()),
            event_repo: SystemEventRepository::new(client.clone()),
            merge_repo: EntityMergeRepository::new(client.clone()),
            identity_key_repo: EntityIdentityKeyRepository::new(client),
            authz,
        }
    }
}

/// Create identity candidate request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCandidateRequest {
    /// The candidate data.
    #[serde(flatten)]
    pub candidate: IdentityCandidate,
}

/// List candidates query parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCandidatesQuery {
    /// Space ID to filter candidates.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Only return pending candidates.
    #[serde(default)]
    pub pending_only: bool,
    /// Minimum confidence threshold.
    #[serde(default)]
    pub min_confidence: Option<f64>,
    /// Maximum results to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

const fn default_limit() -> usize {
    50
}

/// Merge request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRequest {
    /// First entity to merge.
    pub entity_a: String,
    /// Second entity to merge.
    pub entity_b: String,
    /// Merge strategy to apply.
    pub strategy: MergeStrategy,
}

/// Merge response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeResponse {
    /// The surviving entity.
    pub entity: Entity,
    /// Message about the merge.
    pub message: String,
    /// Number of relationships repointed.
    pub relationships_repointed: usize,
    /// Number of events repointed.
    pub events_repointed: usize,
    /// Entity merge lineage record IDs.
    pub merge_record_ids: Vec<String>,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// POST /v1/identity/candidates - Create identity candidate.
async fn create_candidate(
    State(state): State<Arc<IdentityState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<CreateCandidateRequest>,
) -> Result<(StatusCode, Json<IdentityCandidate>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut candidate = req.candidate;
    candidate.base.space_id = require_space_id(&principal)?;

    state
        .authz
        .authorize_record_write(&principal, &candidate, "write:entity")
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
        .authorize_record_read(&principal, &candidate, "read:entity")
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

    match state.candidate_repo.create(candidate).await {
        Ok(candidate) => {
            state
                .authz
                .authorize_record_read(&principal, &candidate, "read:entity")
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

            Ok((StatusCode::CREATED, Json(candidate)))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/identity/candidates - List identity candidates.
async fn list_candidates(
    State(state): State<Arc<IdentityState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<ListCandidatesQuery>,
) -> ApiResult<Vec<IdentityCandidate>> {
    let space_id = require_space_id(&principal)?;
    if let Some(req_space_id) = &query.space_id {
        if req_space_id != &space_id {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ApiError::validation("Access to space denied")),
            ));
        }
    }

    let policy_id = state
        .authz
        .to_abac_principal(&principal)
        .clearances
        .first()
        .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());
    let record = elephant_policy::RecordAttributes {
        space_id: space_id.clone(),
        security: Security {
            policy_id,
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        },
        subjects: Vec::new(),
        subjects_state: SubjectsState::Complete,
    };
    state
        .authz
        .evaluate(&principal, &record, "read:entity")
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

    let candidates = if query.pending_only {
        state
            .candidate_repo
            .get_pending(&space_id, query.limit)
            .await
    } else if let Some(threshold) = query.min_confidence {
        state
            .candidate_repo
            .get_high_confidence(&space_id, threshold, query.limit)
            .await
    } else {
        let criteria = SearchCriteria::new()
            .with_space_id(space_id.clone())
            .with_limit(query.limit);
        state
            .candidate_repo
            .search(&criteria)
            .await
            .map(|r| r.results)
    }
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok(Json(state.authz.filter_records(
        &principal,
        candidates,
        "read:entity",
    )))
}

/// POST /v1/identity/merge - Merge two entities.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
async fn merge_entities(
    State(state): State<Arc<IdentityState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<MergeRequest>,
) -> ApiResult<MergeResponse> {
    let Some(space_id) = principal
        .space_id
        .clone()
        .filter(|space_id| !space_id.is_empty())
    else {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "FORBIDDEN".to_string(),
                message: "Access denied: missing space_id".to_string(),
                details: None,
            }),
        ));
    };
    let rid_a = parse_id(&req.entity_a, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let rid_b = parse_id(&req.entity_b, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    // Verify both entities exist
    let entity_a = match state.entity_repo.get(&space_id, &rid_a).await {
        Ok(Some(e)) => e,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Entity A", &req.entity_a)),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ))
        }
    };

    let entity_b = match state.entity_repo.get(&space_id, &rid_b).await {
        Ok(Some(e)) => e,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Entity B", &req.entity_b)),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ))
        }
    };

    // Enforce tenant invariants (docs/spec.md §7.1).
    if entity_a.base.space_id != entity_b.base.space_id {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "Cannot merge entities from different spaces",
            )),
        ));
    }

    // Require authorization to modify both entities.
    for entity in [&entity_a, &entity_b] {
        state
            .authz
            .authorize_record_write(&principal, entity, "write:entity")
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
            .authorize_record_read(&principal, entity, "read:entity")
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

    let (survivor, merged_entity) = match req.strategy {
        MergeStrategy::KeepA | MergeStrategy::Combine => (entity_a.clone(), entity_b.clone()),
        MergeStrategy::KeepB => (entity_b.clone(), entity_a.clone()),
    };

    let now = Utc::now();

    let (survivor_entity, merged_entities): (Entity, Vec<Entity>) = if req.strategy
        == MergeStrategy::Combine
    {
        let new_id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(
            new_id,
            survivor.base.space_id.clone(),
            principal.subject.clone(),
        );
        base.security = merge_security(&survivor.base.security, &merged_entity.base.security);
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let mut combined = Entity::new(base, survivor.entity_type, survivor.display_name.clone());

        for alias in survivor
            .aliases
            .iter()
            .chain(merged_entity.aliases.iter())
            .chain([&survivor.display_name, &merged_entity.display_name].into_iter())
        {
            combined.add_alias(alias.clone());
        }
        combined.identity_keys = survivor
            .identity_keys
            .iter()
            .chain(merged_entity.identity_keys.iter())
            .cloned()
            .collect();
        combined.identity_keys.sort();
        combined.identity_keys.dedup();

        combined.metadata = merge_json_values(&survivor.metadata, &merged_entity.metadata);

        combined.risk_level = match (survivor.risk_level, merged_entity.risk_level) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        combined.trust_level = match (survivor.trust_level, merged_entity.trust_level) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        combined.known_safe = survivor.known_safe || merged_entity.known_safe;

        let created = state.entity_repo.create(combined).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

        (created, vec![entity_a.clone(), entity_b.clone()])
    } else {
        let mut updated_survivor = survivor.clone();

        for alias in merged_entity
            .aliases
            .iter()
            .chain(std::iter::once(&merged_entity.display_name))
        {
            updated_survivor.add_alias(alias.clone());
        }

        updated_survivor
            .identity_keys
            .extend(merged_entity.identity_keys.iter().cloned());
        updated_survivor.identity_keys.sort();
        updated_survivor.identity_keys.dedup();

        updated_survivor.metadata =
            merge_json_values(&updated_survivor.metadata, &merged_entity.metadata);

        for handle in &merged_entity.handles {
            let Some(existing) = updated_survivor
                .handles
                .iter_mut()
                .find(|h| h.platform == handle.platform && h.username == handle.username)
            else {
                updated_survivor.handles.push(handle.clone());
                continue;
            };

            let existing_conf = existing.confidence.unwrap_or(0.0);
            let incoming_conf = handle.confidence.unwrap_or(0.0);
            if incoming_conf > existing_conf {
                existing.confidence = handle.confidence;
                existing.verified = handle.verified;
            }

            existing.first_seen = match (existing.first_seen, handle.first_seen) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (None, Some(b)) => Some(b),
                (Some(a), None) => Some(a),
                (None, None) => None,
            };
            existing.last_seen = match (existing.last_seen, handle.last_seen) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (None, Some(b)) => Some(b),
                (Some(a), None) => Some(a),
                (None, None) => None,
            };

            existing.evidence.extend(handle.evidence.iter().cloned());
            let mut seen: HashSet<RecordId> = HashSet::new();
            existing.evidence.retain(|id| seen.insert(id.clone()));
        }

        updated_survivor.base.updated_at = now;

        let rid = updated_survivor.base.id.clone();
        let updated = state
            .entity_repo
            .update(&space_id, &rid, updated_survivor)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;

        (updated, vec![merged_entity.clone()])
    };

    let mut relationships_to_repoint: Vec<Rel> = Vec::new();
    for merged in &merged_entities {
        let rid = merged.base.id.clone();
        let outgoing = state
            .rel_repo
            .find_outgoing(&space_id, &rid)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        let incoming = state
            .rel_repo
            .find_incoming(&space_id, &rid)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        relationships_to_repoint.extend(outgoing);
        relationships_to_repoint.extend(incoming);
    }

    let mut events_to_repoint: Vec<SystemEvent> = Vec::new();
    for merged in &merged_entities {
        let rid = merged.base.id.clone();
        let events = state
            .event_repo
            .find_by_entity(&space_id, &rid)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;
        events_to_repoint.extend(events);
    }

    let relationships_repointed_count = relationships_to_repoint.len();
    let events_repointed_count = events_to_repoint.len();
    let merged_entity_ids: Vec<RecordId> = merged_entities
        .iter()
        .map(|entity| entity.base.id.clone())
        .collect();

    let extension_registry = load_runtime_reference_registry_from_env().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(format!(
                "Failed to load extension reference registry: {e}"
            ))),
        )
    })?;

    apply_entity_merge_rewrites(
        &state.storage,
        &space_id,
        &survivor_entity.base.id,
        &merged_entity_ids,
        now,
        &extension_registry,
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(format!("Merge transaction failed: {e}"))),
        )
    })?;

    let mut merge_record_ids: Vec<String> = Vec::new();
    for merged in &merged_entities {
        let merge_id = RecordId::new_v4(TableName::EntityMerge);
        let mut merge_base = BaseFields::new(
            merge_id.clone(),
            survivor_entity.base.space_id.clone(),
            principal.subject.clone(),
        );

        let mut merge_security = survivor_entity.base.security.clone();
        merge_security.level = merge_security.level.max(merged.base.security.level);
        merge_security
            .labels
            .extend(merged.base.security.labels.iter().cloned());
        merge_security.labels.sort();
        merge_security.labels.dedup();
        merge_security
            .handling
            .extend(merged.base.security.handling.iter().cloned());
        merge_security.handling.sort();
        merge_security.handling.dedup();
        merge_base.security = merge_security;
        merge_base.provenance = Provenance::default();
        merge_base.subjects = vec![
            Link::new(merged.base.id.clone()),
            Link::new(survivor_entity.base.id.clone()),
        ];
        merge_base.subjects_state = SubjectsState::Complete;

        let entity_merge = EntityMerge {
            base: merge_base,
            from: Link::new(merged.base.id.clone()),
            into: Link::new(survivor_entity.base.id.clone()),
            strategy: req.strategy,
            decided_by: principal.subject.clone(),
            decided_at: now,
            run_id: None,
        };

        let merge_record = state.merge_repo.create(entity_merge).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(format!(
                    "Failed to create merge record: {e}"
                ))),
            )
        })?;

        merge_record_ids.push(merge_record.base.id.to_string());
    }

    Ok(Json(MergeResponse {
        entity: survivor_entity,
        message: format!(
            "Merge completed. {relationships_repointed_count} relationships and {events_repointed_count} events repointed."
        ),
        relationships_repointed: relationships_repointed_count,
        events_repointed: events_repointed_count,
        merge_record_ids,
    }))
}

/// Returns identity routes.
pub fn identity_routes() -> Router<Arc<IdentityState>> {
    Router::new()
        .route("/v1/identity/candidates", post(create_candidate))
        .route("/v1/identity/candidates", get(list_candidates))
        .route("/v1/identity/merge", post(merge_entities))
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
    fn test_list_candidates_query_defaults() {
        let json = r"{}";
        let query: ListCandidatesQuery = serde_json::from_str(json).expect("deserialize");
        assert_eq!(query.limit, 50);
        assert!(!query.pending_only);
    }
}
