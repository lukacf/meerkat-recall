//! Predicate API routes.
//!
//! Implements the following endpoints:
//! - `GET /v1/predicates` - List predicates with aliases
//! - `GET /v1/predicates/{id}` - Get predicate by ID
//! - `POST /v1/predicates` - Create predicate
//! - `POST /v1/predicates/{id}/aliases` - Propose alias
//! - `PATCH /v1/predicates/{id}/aliases/{alias_id}` - Activate/deprecate alias

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, patch, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::{
    parse_id, PredicateAliasRepository, PredicateRegistryRepository, Repository, StorageClient,
    StorageResult,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasType, Cardinality, PartialResolutionPolicy, PredicateAlias,
    PredicateRegistry, PredicateSemantics, SemanticsStatus,
};
use elephant_types::records::truth::ConflictPolicy;
use elephant_types::RecordId;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;
use crate::routes::require_space_id;

/// Shared state for predicate handlers.
pub struct PredicatesState {
    /// Predicate registry repository.
    pub predicate_repo: Arc<dyn PredicateRegistryRepo>,
    /// Predicate alias repository.
    pub alias_repo: Arc<dyn PredicateAliasRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// Predicate registry repository interface used by the API layer.
#[async_trait::async_trait]
pub trait PredicateRegistryRepo: Send + Sync {
    async fn create(&self, record: PredicateRegistry) -> StorageResult<PredicateRegistry>;
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateRegistry>>;
    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateRegistry>>;
}

/// Predicate alias repository interface used by the API layer.
#[async_trait::async_trait]
pub trait PredicateAliasRepo: Send + Sync {
    async fn create(&self, record: PredicateAlias) -> StorageResult<PredicateAlias>;
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateAlias>>;
    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: PredicateAlias,
    ) -> StorageResult<PredicateAlias>;
    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateAlias>>;
}

#[async_trait::async_trait]
impl PredicateRegistryRepo for PredicateRegistryRepository {
    async fn create(&self, record: PredicateRegistry) -> StorageResult<PredicateRegistry> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateRegistry>> {
        Repository::get(self, space_id, id).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateRegistry>> {
        Repository::list(self, space_id, limit, cursor).await
    }
}

#[async_trait::async_trait]
impl PredicateAliasRepo for PredicateAliasRepository {
    async fn create(&self, record: PredicateAlias) -> StorageResult<PredicateAlias> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateAlias>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: PredicateAlias,
    ) -> StorageResult<PredicateAlias> {
        Repository::update(self, space_id, id, record).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateAlias>> {
        Repository::list(self, space_id, limit, cursor).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl PredicateRegistryRepo for elephant_storage::MockPredicateRegistryRepository {
    async fn create(&self, record: PredicateRegistry) -> StorageResult<PredicateRegistry> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateRegistry>> {
        Repository::get(self, space_id, id).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateRegistry>> {
        Repository::list(self, space_id, limit, cursor).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl PredicateAliasRepo for elephant_storage::MockPredicateAliasRepository {
    async fn create(&self, record: PredicateAlias) -> StorageResult<PredicateAlias> {
        Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateAlias>> {
        Repository::get(self, space_id, id).await
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: PredicateAlias,
    ) -> StorageResult<PredicateAlias> {
        Repository::update(self, space_id, id, record).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateAlias>> {
        Repository::list(self, space_id, limit, cursor).await
    }
}

impl PredicatesState {
    /// Creates new predicates state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            predicate_repo: Arc::new(PredicateRegistryRepository::new(client.clone())),
            alias_repo: Arc::new(PredicateAliasRepository::new(client)),
            authz,
        }
    }

    /// Creates predicates state from pre-built repositories (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repos(
        predicate_repo: Arc<dyn PredicateRegistryRepo>,
        alias_repo: Arc<dyn PredicateAliasRepo>,
        authz: AuthzState,
    ) -> Self {
        Self {
            predicate_repo,
            alias_repo,
            authz,
        }
    }
}

/// Predicate list request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateListRequest {
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Include aliases in response.
    #[serde(default = "default_include_aliases")]
    pub include_aliases: bool,
}

const fn default_limit() -> usize {
    50
}

const fn default_include_aliases() -> bool {
    true
}

/// Predicate with aliases response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateWithAliases {
    /// The predicate registry entry.
    #[serde(flatten)]
    pub predicate: PredicateRegistry,
    /// Aliases for this predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aliases: Option<Vec<PredicateAlias>>,
}

/// Predicate list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateListResponse {
    /// Predicates with aliases.
    pub results: Vec<PredicateWithAliases>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count.
    pub total: usize,
}

/// Create predicate request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePredicateRequest {
    /// Unique predicate identifier (e.g., `works_at`).
    pub predicate_id: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: Option<String>,
}

/// Propose alias request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeAliasRequest {
    /// The alias string.
    pub alias: String,
    /// Type of alias.
    #[serde(default)]
    pub alias_type: AliasType,
    /// Confidence score.
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    /// Scope of the alias.
    #[serde(default)]
    pub scope: AliasScope,
    /// Source plugin (if plugin-generated).
    #[serde(default)]
    pub source_plugin: Option<String>,
}

const fn default_confidence() -> f32 {
    0.9
}

/// Update alias status request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAliasStatusRequest {
    /// New alias status.
    pub status: AliasStatus,
}

/// Predicate hygiene alias repair request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeactivateHygieneAliasesRequest {
    /// Report matching aliases without mutating them.
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum aliases to inspect.
    #[serde(default = "default_repair_limit")]
    pub limit: usize,
}

const fn default_repair_limit() -> usize {
    10_000
}

/// Predicate hygiene alias repair response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeactivateHygieneAliasesResponse {
    /// Whether this was a dry run.
    pub dry_run: bool,
    /// Number of aliases inspected.
    pub scanned: usize,
    /// Number of active `predicate_hygiene` aliases matching the repair.
    pub matched: usize,
    /// Number of aliases updated to deprecated.
    pub deactivated: usize,
    /// Example aliases touched or that would be touched.
    pub samples: Vec<PredicateAliasRepairSample>,
}

/// Sample alias included in repair response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateAliasRepairSample {
    /// Alias record id.
    pub id: String,
    /// Raw alias string.
    pub alias: String,
    /// Canonical predicate id.
    pub predicate_id: String,
    /// Alias status before repair.
    pub previous_status: AliasStatus,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// GET /v1/predicates - List predicates.
async fn list_predicates(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    axum::extract::Query(req): axum::extract::Query<PredicateListRequest>,
) -> ApiResult<PredicateListResponse> {
    let space_id = require_space_id(&principal)?;

    let cursor = req
        .cursor
        .as_ref()
        .and_then(|c| parse_id(c, TableName::PredicateRegistry).ok());

    let predicates = state
        .predicate_repo
        .list(&space_id, req.limit + 1, cursor.as_ref())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let has_more = predicates.len() > req.limit;
    let predicates: Vec<_> = predicates.into_iter().take(req.limit).collect();

    let next_cursor = if has_more {
        predicates.last().map(|p| p.base.id.to_string())
    } else {
        None
    };

    let filtered = state
        .authz
        .filter_records(&principal, predicates, "read:predicate");

    // Fetch aliases for each predicate if requested
    let mut results = Vec::with_capacity(filtered.len());
    for pred in filtered {
        let aliases = if req.include_aliases {
            // List aliases filtering by predicate_id
            let all_aliases = state
                .alias_repo
                .list(&space_id, 100, None)
                .await
                .unwrap_or_default();
            let matching: Vec<_> = all_aliases
                .into_iter()
                .filter(|a| a.predicate_id == pred.predicate_id)
                .collect();
            Some(matching)
        } else {
            None
        };
        results.push(PredicateWithAliases {
            predicate: pred,
            aliases,
        });
    }

    let total = results.len();

    Ok(Json(PredicateListResponse {
        results,
        cursor: next_cursor,
        has_more,
        total,
    }))
}

/// GET /v1/predicates/{id} - Get predicate by ID.
async fn get_predicate(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<PredicateWithAliases> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::PredicateRegistry).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    match state.predicate_repo.get(&space_id, &rid).await {
        Ok(Some(predicate)) => {
            state
                .authz
                .authorize_record_read(&principal, &predicate, "read:predicate")
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

            // Fetch aliases for this predicate
            let all_aliases = state
                .alias_repo
                .list(&space_id, 100, None)
                .await
                .unwrap_or_default();
            let aliases: Vec<_> = all_aliases
                .into_iter()
                .filter(|a| a.predicate_id == predicate.predicate_id)
                .collect();

            Ok(Json(PredicateWithAliases {
                predicate,
                aliases: Some(aliases),
            }))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Predicate", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/predicates - Create predicate.
async fn create_predicate(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<CreatePredicateRequest>,
) -> Result<(StatusCode, Json<PredicateRegistry>), (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let id = RecordId::new_v4(TableName::PredicateRegistry);
    let mut base = BaseFields::new(id, space_id, principal.subject.clone());
    base.security.policy_id = "default".to_string();

    let semantics = if req.predicate_id == "has title" {
        Some(PredicateSemantics {
            cardinality: Cardinality::One,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
            partial_resolution_policy: PartialResolutionPolicy::AttributeFromObjectText,
            ..PredicateSemantics::default()
        })
    } else {
        None
    };
    let semantics_status = if semantics.is_some() {
        SemanticsStatus::Configured
    } else {
        SemanticsStatus::default()
    };

    let predicate = PredicateRegistry {
        base,
        predicate_id: req.predicate_id,
        description: req.description,
        status: "active".to_string(),
        semantics,
        semantics_status,
        first_seen_at: Some(chrono::Utc::now()),
        usage_count: 0,
        last_used_at: None,
    };

    state
        .authz
        .authorize_record_write(&principal, &predicate, "write:predicate")
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

    match state.predicate_repo.create(predicate).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/predicates/{id}/aliases - Propose alias.
async fn propose_alias(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(predicate_id): Path<String>,
    Json(req): Json<ProposeAliasRequest>,
) -> Result<(StatusCode, Json<PredicateAlias>), (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&predicate_id, TableName::PredicateRegistry).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    // Verify predicate exists
    let predicate = state
        .predicate_repo
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
                Json(ApiError::not_found("Predicate", &predicate_id)),
            )
        })?;

    let alias_id = RecordId::new_v4(TableName::PredicateAlias);
    let mut base = BaseFields::new(alias_id, space_id, principal.subject.clone());
    base.security.policy_id = "default".to_string();

    let alias = PredicateAlias {
        base,
        alias: req.alias,
        alias_type: req.alias_type,
        source_plugin: req.source_plugin,
        confidence: req.confidence,
        status: AliasStatus::Proposed,
        scope: req.scope,
        predicate_id: predicate.predicate_id,
        confidence_version: 1,
        usage_count: 0,
    };

    state
        .authz
        .authorize_record_write(&principal, &alias, "write:predicate")
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

    match state.alias_repo.create(alias).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// `PATCH /v1/predicates/{id}/aliases/{alias_id}` - Activate/deprecate alias.
#[allow(clippy::similar_names)]
async fn update_alias_status(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path((predicate_id, alias_id)): Path<(String, String)>,
    Json(req): Json<UpdateAliasStatusRequest>,
) -> ApiResult<PredicateAlias> {
    let space_id = require_space_id(&principal)?;
    let _pred_rid = parse_id(&predicate_id, TableName::PredicateRegistry).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let alias_rid = parse_id(&alias_id, TableName::PredicateAlias).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let mut alias = match state.alias_repo.get(&space_id, &alias_rid).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Alias", &alias_id)),
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
        .authorize_record_write(&principal, &alias, "write:predicate")
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

    alias.status = req.status;

    match state.alias_repo.update(&space_id, &alias_rid, alias).await {
        Ok(updated) => Ok(Json(updated)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// `POST /v1/predicates/aliases/deactivate-hygiene` - Deprecate generated active hygiene aliases.
async fn deactivate_hygiene_aliases(
    State(state): State<Arc<PredicatesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<DeactivateHygieneAliasesRequest>,
) -> ApiResult<DeactivateHygieneAliasesResponse> {
    let space_id = require_space_id(&principal)?;

    let limit = req.limit.clamp(1, default_repair_limit());
    let mut cursor: Option<RecordId> = None;
    let mut scanned = 0usize;
    let mut matched = 0usize;
    let mut deactivated = 0usize;
    let mut samples = Vec::new();

    loop {
        let page = state
            .alias_repo
            .list(
                &space_id,
                500.min(limit.saturating_sub(scanned)),
                cursor.as_ref(),
            )
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

        for mut alias in page {
            cursor = Some(alias.base.id.clone());
            scanned += 1;

            if alias.status != AliasStatus::Active
                || alias.source_plugin.as_deref() != Some("predicate_hygiene")
            {
                continue;
            }

            state
                .authz
                .authorize_record_write(&principal, &alias, "write:predicate")
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

            matched += 1;
            if samples.len() < 20 {
                samples.push(PredicateAliasRepairSample {
                    id: alias.base.id.to_string(),
                    alias: alias.alias.clone(),
                    predicate_id: alias.predicate_id.clone(),
                    previous_status: alias.status,
                });
            }

            if !req.dry_run {
                alias.status = AliasStatus::Deprecated;
                state
                    .alias_repo
                    .update(&space_id, &alias.base.id.clone(), alias)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiError::internal(e.to_string())),
                        )
                    })?;
                deactivated += 1;
            }

            if scanned >= limit {
                break;
            }
        }

        if scanned >= limit {
            break;
        }
    }

    Ok(Json(DeactivateHygieneAliasesResponse {
        dry_run: req.dry_run,
        scanned,
        matched,
        deactivated,
        samples,
    }))
}

/// Returns predicate routes.
pub fn predicate_routes() -> Router<Arc<PredicatesState>> {
    Router::new()
        .route(
            "/v1/predicates",
            get(list_predicates).post(create_predicate),
        )
        .route(
            "/v1/predicates/aliases/deactivate-hygiene",
            post(deactivate_hygiene_aliases),
        )
        .route("/v1/predicates/{id}", get(get_predicate))
        .route("/v1/predicates/{id}/aliases", post(propose_alias))
        .route(
            "/v1/predicates/{id}/aliases/{alias_id}",
            patch(update_alias_status),
        )
}

#[cfg(test)]
#[path = "predicates_test.rs"]
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
    fn test_predicate_list_request_defaults() {
        let json = r"{}";
        let req: PredicateListRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(req.include_aliases);
    }
}
