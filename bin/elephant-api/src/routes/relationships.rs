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

use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
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
    /// Current source authority; absent only in repository unit fixtures.
    pub storage: Option<StorageClient>,
    /// Canonical client, absent only for isolated repository mocks.
    pub mutation_storage: Option<StorageClient>,

    /// Relationship repository.
    pub rel_repo: Arc<dyn RelRepo>,
    /// Authorization service.
    pub authz: AuthzState,
}

/// Relationship repository interface used by the API layer.
#[async_trait::async_trait]
pub trait RelRepo: Send + Sync {
    /// Restores only if the row still matches the authorized snapshot.
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Rel,
    ) -> StorageResult<Rel>;
    async fn replace_authorized(
        &self,
        observed: &Rel,
        proposed: Rel,
        restore: bool,
    ) -> StorageResult<Rel>;
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
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Rel,
    ) -> StorageResult<Rel> {
        Self::restore_guarded(self, space_id, id, authorized).await
    }

    async fn replace_authorized(
        &self,
        observed: &Rel,
        proposed: Rel,
        restore: bool,
    ) -> StorageResult<Rel> {
        Self::replace_authorized(self, observed, proposed, restore).await
    }

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
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Rel,
    ) -> StorageResult<Rel> {
        let current = Self::get_with_deleted(self, space_id, id).await?;
        let unchanged = current.is_some_and(|current| {
            elephant_storage::base_envelope_unchanged(&current.base, &authorized.base)
                && current.subject == authorized.subject
                && current.object == authorized.object
        });
        if !unchanged {
            return Err(elephant_storage::StorageError::Conflict(format!(
                "Rel changed since it was authorized: {id}"
            )));
        }
        Self::restore(self, space_id, id).await
    }

    async fn replace_authorized(
        &self,
        observed: &Rel,
        proposed: Rel,
        restore: bool,
    ) -> StorageResult<Rel> {
        let current =
            Self::get_with_deleted(self, &observed.base.space_id, &observed.base.id).await?;
        if !current.is_some_and(|current| {
            current.base.updated_at == observed.base.updated_at
                && elephant_storage::base_envelope_unchanged(&current.base, &observed.base)
                && current.subject == observed.subject
                && current.object == observed.object
        }) {
            return Err(StorageError::Conflict(
                "relationship changed since authorization".into(),
            ));
        }
        if restore {
            Self::restore(self, &observed.base.space_id, &observed.base.id).await?;
        }
        elephant_storage::Repository::update(
            self,
            &observed.base.space_id,
            &observed.base.id,
            proposed,
        )
        .await
    }

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
            storage: Some(client.clone()),
            rel_repo: Arc::new(RelRepository::new(client.clone())),
            mutation_storage: Some(client),

            authz,
        }
    }

    /// Creates relationship state from a pre-built repository (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repo(rel_repo: Arc<dyn RelRepo>, authz: AuthzState) -> Self {
        Self {
            storage: None,
            rel_repo,
            authz,
            mutation_storage: None,
        }
    }
}

async fn begin_relationship_mutation(
    state: &RelationshipsState,
    space: &str,
) -> StorageResult<Option<PublicMutation>> {
    match &state.mutation_storage {
        Some(storage) => PublicMutation::begin(storage.clone(), space)
            .await
            .map(Some),
        None => Ok(None),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn authorize_relationship_endpoints(
    state: &RelationshipsState,
    principal: &AuthPrincipal,
    mutation: &mut PublicMutation,
    rel: &mut Rel,
    inherit: bool,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let Some(storage) = &state.mutation_storage else {
        return Ok(());
    };
    let repo = elephant_storage::EntityRepository::new(storage.clone());
    let original = rel.base.clone();
    let mut prepared = rel.base.clone();
    for id in [rel.subject.id(), rel.object.id()] {
        let entity = elephant_storage::Repository::get(&repo, &rel.base.space_id, id)
            .await
            .map_err(relationship_storage_error)?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::not_found(
                        "Relationship endpoint",
                        &id.to_string(),
                    )),
                )
            })?;
        state
            .authz
            .authorize_record_read(principal, &entity, "read:entity")
            .map_err(|error| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError::validation(error.message)),
                )
            })?;
        inherit_public_envelope(&mut prepared, &entity.base).map_err(|error| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError::validation(error.to_string())),
            )
        })?;
        mutation.depend_on(CanonicalRecord::Entity(Box::new(entity)));
    }
    for evidence in &rel.evidence {
        let assertion = elephant_storage::Repository::get(
            &elephant_storage::AssertionRepository::new(storage.clone()),
            &rel.base.space_id,
            evidence.assertion.id(),
        )
        .await
        .map_err(relationship_storage_error)?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(
                    "Relationship assertion is not live in selected space",
                )),
            )
        })?;
        let denied = |error: crate::authz::AuthzError| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError::validation(error.message)),
            )
        };
        state
            .authz
            .authorize_record_read(principal, &assertion, "read:assertion")
            .map_err(denied)?;
        if assertion.evidence != evidence.span {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(
                    "Relationship span differs from referenced assertion",
                )),
            ));
        }
        mutation
            .observe_relationship_assertion(rel, &assertion)
            .await
            .map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(error.to_string())),
                )
            })?;
        mutation.depend_on(CanonicalRecord::Assertion(Box::new(assertion.clone())));
        let (doc, revision, node) = mutation
            .observe_evidence(&evidence.span)
            .await
            .map_err(relationship_storage_error)?;
        state
            .authz
            .authorize_record_read(principal, &doc, "read:doc:raw")
            .map_err(denied)?;
        state
            .authz
            .authorize_record_read(principal, &revision, "read:doc:raw")
            .map_err(denied)?;
        inherit_public_envelope(&mut prepared, &assertion.base)
            .map_err(relationship_storage_error)?;
        inherit_public_envelope(&mut prepared, &doc.base).map_err(relationship_storage_error)?;
        inherit_public_envelope(&mut prepared, &revision.base)
            .map_err(relationship_storage_error)?;
        if let Some(node) = &node {
            state
                .authz
                .authorize_record_read(principal, node, "read:doc:raw")
                .map_err(denied)?;
            inherit_public_envelope(&mut prepared, &node.base)
                .map_err(relationship_storage_error)?;
        }
        if let Some(source) = &doc.source {
            if !prepared.provenance.sources.contains(source.id()) {
                prepared.provenance.sources.push(source.id().clone());
            }
        }
    }
    if inherit {
        rel.base = prepared;
    } else if original.security != prepared.security
        || original.subjects != prepared.subjects
        || original.subjects_state != prepared.subjects_state
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError::validation(
                "Relationship envelope must cover endpoint restrictions",
            )),
        ));
    }
    Ok(())
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

        if !super::current_read::filter(
            state.storage.as_ref(),
            &state.authz,
            principal,
            vec![rel],
            REL_SEARCH_SCOPE,
        )
        .await?
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

        let allowed = super::current_read::filter(
            state.storage.as_ref(),
            &state.authz,
            principal,
            vec![rel],
            REL_SEARCH_SCOPE,
        )
        .await?;
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
    let filtered = super::current_read::filter(
        state.storage.as_ref(),
        &state.authz,
        &principal,
        result.results,
        REL_SEARCH_SCOPE,
    )
    .await?;

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
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn create_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(rel): Json<Rel>,
) -> Result<(StatusCode, Json<Rel>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut rel = rel;
    rel.base.space_id = require_space_id(&principal)?;

    let mut mutation = begin_relationship_mutation(&state, &rel.base.space_id)
        .await
        .map_err(relationship_storage_error)?;
    if let Some(mutation) = &mut mutation {
        authorize_relationship_endpoints(&state, &principal, mutation, &mut rel, true).await?;
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

    state
        .authz
        .authorize_record_read(&principal, &rel, "read:relationship")
        .map_err(|e| (StatusCode::FORBIDDEN, Json(ApiError::validation(e.message))))?;
    let result = match mutation {
        Some(mutation) => mutation.create(rel, &principal.subject).await,
        None => state.rel_repo.create(rel).await,
    };
    match result {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StorageError::Conflict(msg)) => Err((
            StatusCode::CONFLICT,
            Json(ApiError {
                code: "CONFLICT".to_string(),
                message: msg,
                details: None,
            }),
        )),
        Err(e) => Err(ApiError::storage(&e)),
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
            Ok(Json(
                super::current_read::require(
                    state.storage.as_ref(),
                    &state.authz,
                    &principal,
                    rel,
                    "read:relationship",
                )
                .await?,
            ))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err(ApiError::storage(&e)),
    }
}

fn relationship_storage_error(error: StorageError) -> (StatusCode, Json<ApiError>) {
    if matches!(error, StorageError::ErasureInProgress) {
        return ApiError::storage(&error);
    }
    if let StorageError::Conflict(message) = error {
        (
            StatusCode::CONFLICT,
            Json(ApiError {
                code: "CONFLICT".into(),
                message,
                details: None,
            }),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(error.to_string())),
        )
    }
}

async fn save_authorized_relationship(
    state: &RelationshipsState,
    principal: &AuthPrincipal,
    mut mutation: Option<&mut PublicMutation>,
    mut proposed: Rel,
    observed: Option<&Rel>,
    restore: bool,
) -> ApiResult<Rel> {
    if let Some(mutation) = mutation.as_deref_mut() {
        authorize_relationship_endpoints(
            state,
            principal,
            mutation,
            &mut proposed,
            observed.is_none(),
        )
        .await?;
    }
    let denied = |message: String| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".into(),
                message,
                details: None,
            }),
        )
    };
    for record in observed.into_iter().chain(std::iter::once(&proposed)) {
        state
            .authz
            .authorize_record_read(principal, record, "read:relationship")
            .map_err(|e| denied(e.message))?;
        state
            .authz
            .authorize_record_write(principal, record, "write:relationship")
            .map_err(|e| denied(e.message))?;
    }
    let result = if let Some(existing) = observed {
        elephant_policy::authorize_ordinary_update(&existing.base, &proposed.base)
            .map_err(|e| denied(e.to_string()))?;
        // Equivalent label/subject sets are accepted by the policy. Keep the
        // authorized representation so storage can compare the envelope exactly.
        proposed.base.security.clone_from(&existing.base.security);
        proposed.base.subjects.clone_from(&existing.base.subjects);
        proposed.base.subjects_state = existing.base.subjects_state;
        match mutation {
            Some(mutation) => {
                mutation
                    .replace(existing, proposed, restore, &principal.subject)
                    .await
            }
            None => {
                state
                    .rel_repo
                    .replace_authorized(existing, proposed, restore)
                    .await
            }
        }
    } else {
        match mutation {
            Some(mutation) => mutation.create(proposed, &principal.subject).await,
            None => state.rel_repo.create(proposed).await,
        }
    };
    result.map(Json).map_err(relationship_storage_error)
}

/// PUT /v1/relationships/{id} - Update relationship.
async fn update_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(rel): Json<Rel>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let mut mutation = begin_relationship_mutation(&state, &space_id)
        .await
        .map_err(relationship_storage_error)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let existing = state
        .rel_repo
        .get(&space_id, &rid)
        .await
        .map_err(relationship_storage_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Relationship", &id)),
            )
        })?;
    save_authorized_relationship(
        &state,
        &principal,
        mutation.as_mut(),
        rel,
        Some(&existing),
        false,
    )
    .await
}

/// DELETE /v1/relationships/{id} - Soft delete relationship.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn delete_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let mutation = begin_relationship_mutation(&state, &space_id)
        .await
        .map_err(relationship_storage_error)?;
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

            let result = match &mutation {
                Some(mutation) => mutation
                    .delete(&existing, &principal.subject)
                    .await
                    .map(|_| ()),
                None => state.rel_repo.delete(&space_id, &rid).await,
            };
            match result {
                Ok(()) => Ok(StatusCode::NO_CONTENT),
                Err(e) => Err(ApiError::storage(&e)),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err(ApiError::storage(&e)),
    }
}

/// POST /v1/relationships/{id}/restore - Restore soft-deleted relationship.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn restore_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let mut mutation = begin_relationship_mutation(&state, &space_id)
        .await
        .map_err(relationship_storage_error)?;
    let rid = parse_id(&id, TableName::Rel).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.rel_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            if let Some(mutation) = &mut mutation {
                let mut proposal = existing.clone();
                authorize_relationship_endpoints(
                    &state,
                    &principal,
                    mutation,
                    &mut proposal,
                    false,
                )
                .await?;
            }
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
            // The response carries the restored record, so read is required too.
            state
                .authz
                .authorize_record_read(&principal, &existing, "read:relationship")
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

            let result = match &mutation {
                Some(mutation) => mutation.restore(&existing, &principal.subject).await,
                None => {
                    state
                        .rel_repo
                        .restore_guarded(&space_id, &rid, &existing)
                        .await
                }
            };
            match result {
                Ok(restored) => Ok(Json(restored)),
                Err(elephant_storage::StorageError::Conflict(_)) => Err((
                    StatusCode::CONFLICT,
                    Json(ApiError {
                        code: "CONFLICT".to_string(),
                        message: "record changed since it was authorized; retry".to_string(),
                        details: None,
                    }),
                )),
                Err(e) => Err(ApiError::storage(&e)),
            }
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Relationship", &id)),
        )),
        Err(e) => Err(ApiError::storage(&e)),
    }
}

/// POST /v1/relationships/upsert - Upsert relationship.
async fn upsert_relationship(
    State(state): State<Arc<RelationshipsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(rel): Json<Rel>,
) -> ApiResult<Rel> {
    let space_id = require_space_id(&principal)?;
    let mut mutation = begin_relationship_mutation(&state, &space_id)
        .await
        .map_err(relationship_storage_error)?;
    if rel.base.space_id != space_id {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Space ID mismatch")),
        ));
    }
    let existing = state
        .rel_repo
        .get_with_deleted(&space_id, &rel.base.id)
        .await
        .map_err(relationship_storage_error)?;
    save_authorized_relationship(
        &state,
        &principal,
        mutation.as_mut(),
        rel,
        existing.as_ref(),
        true,
    )
    .await
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
        let mut mutation = match begin_relationship_mutation(&state, &space_id).await {
            Ok(mutation) => mutation,
            Err(error @ elephant_storage::StorageError::ErasureInProgress) => {
                return Err(ApiError::storage(&error));
            }
            Err(error) => {
                failed.push(BatchError {
                    index,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let result = match state
            .rel_repo
            .get_with_deleted(&space_id, &rel.base.id)
            .await
        {
            Ok(existing) => {
                save_authorized_relationship(
                    &state,
                    &principal,
                    mutation.as_mut(),
                    rel,
                    existing.as_ref(),
                    true,
                )
                .await
            }
            Err(error) => Err(relationship_storage_error(error)),
        };
        match result {
            Ok(Json(record)) => succeeded.push(record),
            Err((status, Json(error))) if error.code == "NOT_READY" => {
                return Err((status, Json(error)));
            }
            Err((_, Json(error))) => failed.push(BatchError {
                index,
                error: error.message,
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
