//! Identity API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/identity/candidates - Create identity candidate
//! - GET /v1/identity/candidates - List pending identity candidates
//! - POST /v1/identity/merge - Merge two entities

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_extensions::load_runtime_reference_registry_from_env;
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    parse_id, EntityIdentityKeyRepository, EntityMergeRepository, EntityRepository,
    IdentityCandidateRepository, RelRepository, Repository, SearchCriteria, SearchRepository,
    StorageClient, SystemEventRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::{Entity, IdentityCandidate, MergeStrategy};

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

    let mut mutation = PublicMutation::begin(state.storage.clone(), &candidate.base.space_id)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?;
    for id in [candidate.entity_a.id(), candidate.entity_b.id()] {
        let entity = state
            .entity_repo
            .get(&candidate.base.space_id, id)
            .await
            .map_err(crate::public_error::legacy_storage_failure)?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::not_found("Identity target", &id.to_string())),
                )
            })?;
        state
            .authz
            .authorize_record_read(&principal, &entity, "read:entity")
            .map_err(|error| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError::validation(error.message)),
                )
            })?;
        inherit_public_envelope(&mut candidate.base, &entity.base).map_err(|error| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError::validation(error.to_string())),
            )
        })?;
        mutation.depend_on(CanonicalRecord::Entity(Box::new(entity)));
    }
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

    match mutation.create(candidate, &principal.subject).await {
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
        Err(error) => Err(crate::public_error::legacy_storage_failure(error)),
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
#[expect(
    clippy::result_large_err,
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn merge_entities(
    State(state): State<Arc<IdentityState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<MergeRequest>,
) -> ApiResult<MergeResponse> {
    use elephant_storage::entity_merge_repository::MergeSelection;
    let space_id = require_space_id(&principal)?;
    let parse = |value: &str| {
        parse_id(value, TableName::Entity).map_err(super::source_intake::storage_error)
    };
    let a = parse(&req.entity_a)?;
    let b = parse(&req.entity_b)?;
    let registry = load_runtime_reference_registry_from_env().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(error.to_string())),
        )
    })?;
    let selection = match req.strategy {
        MergeStrategy::KeepA => MergeSelection::KeepA,
        MergeStrategy::KeepB => MergeSelection::KeepB,
        MergeStrategy::Combine => MergeSelection::Combine,
    };
    let prepared = elephant_identity::prepare_entity_merge(
        &state.storage,
        &space_id,
        &a,
        &b,
        selection,
        &principal.subject,
        &registry,
    )
    .await
    .map_err(|error| match error {
        elephant_identity::IdentityError::StorageError(error) => {
            super::source_intake::storage_error(error)
        }
        other => (
            StatusCode::CONFLICT,
            Json(ApiError::validation(other.to_string())),
        ),
    })?;
    for envelope in prepared
        .authorizations()
        .map_err(super::source_intake::storage_error)?
    {
        let denied = |error: crate::authz::AuthzError| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: error.code,
                    message: error.message,
                    details: None,
                }),
            )
        };
        state
            .authz
            .authorize_record_read(&principal, &envelope, envelope.read_scope)
            .map_err(denied)?;
        state
            .authz
            .authorize_record_write(&principal, &envelope, envelope.write_scope)
            .map_err(denied)?;
    }
    let outcome = prepared
        .commit()
        .await
        .map_err(super::source_intake::storage_error)?;
    Ok(Json(MergeResponse {
        entity: outcome.survivor,
        message: format!(
            "Merge completed. {} relationships and {} events repointed.",
            outcome.relationships_repointed, outcome.events_repointed
        ),
        relationships_repointed: outcome.relationships_repointed,
        events_repointed: outcome.events_repointed,
        merge_record_ids: outcome
            .merge_record_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
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
    use elephant_types::records::base::BaseFields;
    use elephant_types::{Link, RecordId};

    async fn register_merge_space(storage: &StorageClient, space: &str) {
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: elephant_types::records::base::BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    space.into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
    }

    #[test]
    fn test_list_candidates_query_defaults() {
        let json = r"{}";
        let query: ListCandidatesQuery = serde_json::from_str(json).expect("deserialize");
        assert_eq!(query.limit, 50);
        assert!(!query.pending_only);
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture ownership, future layout, and oracle sequence."
    )]
    async fn merge_rejects_hidden_dependent_relationship_before_any_write() {
        use elephant_types::records::{entity::EntityType, rel::ClaimStatus, Rel};
        let storage = StorageClient::new_embedded().await.unwrap();
        register_merge_space(&storage, "home").await;
        let entities = EntityRepository::new(storage.clone());
        let make = |name: &str| {
            Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "home".into(),
                    "test".into(),
                ),
                EntityType::new("person"),
                name.into(),
            )
        };
        let a = entities.create(make("A")).await.unwrap();
        let b = entities.create(make("B")).await.unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "home".into(),
            "test".into(),
        );
        base.security.level = 5;
        let rel = RelRepository::new(storage.clone())
            .create(Rel {
                base,
                subject: Link::new(b.base.id.clone()),
                object: Link::new(a.base.id.clone()),
                rel_type: "knows".into(),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: vec![],
                temporal_audit: None,
            })
            .await
            .unwrap();
        let principal = AuthPrincipal {
            subject: "test".into(),
            space_id: Some("home".into()),
            spaces: Some(vec!["home".into()]),
            scopes: vec!["*".into()],
            security_level: 0,
            security_labels: vec![],
            clearances: vec![],
            subject_allowlist: None,
            purpose: vec![],
            bypass_abac: false,
        };
        let state = Arc::new(IdentityState::new(
            storage.clone(),
            Arc::new(crate::authz::AuthzService::new()),
        ));
        let error = merge_entities(
            State(state),
            axum::Extension(principal),
            Json(MergeRequest {
                entity_a: a.base.id.to_string(),
                entity_b: b.base.id.to_string(),
                strategy: MergeStrategy::KeepA,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert_eq!(entities.get("home", &a.base.id).await.unwrap().unwrap(), a);
        assert_eq!(entities.get("home", &b.base.id).await.unwrap().unwrap(), b);
        assert_eq!(
            RelRepository::new(storage.clone())
                .get("home", &rel.base.id)
                .await
                .unwrap()
                .unwrap(),
            rel
        );
        assert!(SystemEventRepository::new(storage)
            .list("home", 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}
