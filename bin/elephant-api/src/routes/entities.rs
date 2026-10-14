//! Entity API routes.
//!
//! Implements the following endpoints:
//! - POST /v1/entities/search - Search entities
//! - GET /v1/entities/{id} - Get entity by ID
//! - GET /v1/entities/{id}/with-relations - Get entity with all relations
//! - GET /v1/entities/{id}/context - Get entity context bundle
//! - POST /v1/entities - Create entity
//! - PUT /v1/entities/{id} - Update entity
//! - DELETE /v1/entities/{id} - Soft delete entity
//! - POST /v1/entities/{id}/restore - Restore soft-deleted entity
//! - POST /v1/entities/upsert - Deterministic upsert
//! - POST /v1/entities/batch - Batch operations

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use elephant_storage::public_mutation::PublicMutation;
use elephant_storage::{
    parse_id, EntityRepository, EntityWithRelations, PaginatedResult, RelRepository,
    SearchCriteria, StorageClient, StorageError, StorageResult, SystemEventRepository,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{Entity, Rel, SystemEvent};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::{AuthzError, AuthzState};
use crate::routes::require_space_id;

/// Entity repository interface used by the API layer.
#[async_trait::async_trait]
pub trait EntityRepo: Send + Sync {
    /// Restores only if the row still matches the authorized snapshot.
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Entity,
    ) -> StorageResult<Entity>;
    async fn create(&self, record: Entity) -> StorageResult<Entity>;
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Entity>>;
    async fn get_many(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> StorageResult<Vec<Entity>>;
    async fn update_preserving_security(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Entity,
    ) -> StorageResult<Entity>;
    async fn restore_and_update_preserving_security(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Entity,
    ) -> StorageResult<Entity>;
    async fn replace_authorized(
        &self,
        observed: &Entity,
        proposed: Entity,
        restore: bool,
    ) -> StorageResult<Entity>;
    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()>;
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Entity>>;

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Entity>>;
    async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Entity>;
    async fn get_with_relations(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityWithRelations>>;
}

/// Relationship repository interface needed for entity context traversal.
#[async_trait::async_trait]
pub trait RelRepo: Send + Sync {
    async fn find_outgoing(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>>;
    async fn find_incoming(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>>;
}

/// Event repository interface needed for entity context.
#[async_trait::async_trait]
pub trait EventRepo: Send + Sync {
    async fn find_by_entity_since(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        since: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>>;
}

#[async_trait::async_trait]
impl EntityRepo for EntityRepository {
    async fn restore_guarded(
        &self,
        space_id: &str,
        id: &elephant_types::RecordId,
        authorized: &Entity,
    ) -> StorageResult<Entity> {
        Self::restore_guarded(self, space_id, id, authorized).await
    }

    async fn replace_authorized(
        &self,
        observed: &Entity,
        proposed: Entity,
        restore: bool,
    ) -> StorageResult<Entity> {
        Self::replace_authorized(self, observed, proposed, restore).await
    }
    async fn create(&self, record: Entity) -> StorageResult<Entity> {
        elephant_storage::Repository::create(self, record).await
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Entity>> {
        elephant_storage::Repository::get(self, space_id, id).await
    }

    async fn get_many(
        &self,
        space_id: &str,
        ids: &[RecordId],
        include_deleted: bool,
    ) -> StorageResult<Vec<Entity>> {
        Self::get_many(self, space_id, ids, include_deleted).await
    }

    async fn update_preserving_security(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Entity,
    ) -> StorageResult<Entity> {
        Self::update_preserving_security(self, space_id, id, record).await
    }

    async fn restore_and_update_preserving_security(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Entity,
    ) -> StorageResult<Entity> {
        Self::restore_and_update_preserving_security(self, space_id, id, record).await
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        elephant_storage::Repository::delete(self, space_id, id).await
    }

    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Entity>> {
        elephant_storage::SearchRepository::search(self, criteria).await
    }

    async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Entity>> {
        Self::get_with_deleted(self, space_id, id).await
    }

    async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Entity> {
        Self::restore(self, space_id, id).await
    }

    async fn get_with_relations(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityWithRelations>> {
        Self::get_with_relations(self, space_id, id).await
    }
}

#[async_trait::async_trait]
impl RelRepo for RelRepository {
    async fn find_outgoing(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>> {
        Self::find_outgoing(self, space_id, entity_id).await
    }

    async fn find_incoming(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>> {
        Self::find_incoming(self, space_id, entity_id).await
    }
}

#[async_trait::async_trait]
impl EventRepo for SystemEventRepository {
    async fn find_by_entity_since(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        since: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        Self::find_by_entity_since(self, space_id, entity_id, since, limit).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl RelRepo for elephant_storage::MockRelRepository {
    async fn find_outgoing(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>> {
        let criteria = SearchCriteria::new()
            .with_space_id(space_id)
            .with_limit(10_000);
        let page = elephant_storage::SearchRepository::search(self, &criteria).await?;
        Ok(page
            .results
            .into_iter()
            .filter(|r| r.subject.id() == entity_id)
            .collect())
    }

    async fn find_incoming(&self, space_id: &str, entity_id: &RecordId) -> StorageResult<Vec<Rel>> {
        let criteria = SearchCriteria::new()
            .with_space_id(space_id)
            .with_limit(10_000);
        let page = elephant_storage::SearchRepository::search(self, &criteria).await?;
        Ok(page
            .results
            .into_iter()
            .filter(|r| r.object.id() == entity_id)
            .collect())
    }
}

/// Shared application state for entity handlers.
#[derive(Clone)]
pub struct AppState {
    /// Current source support authority; absent only for repository unit fixtures.
    pub storage: Option<StorageClient>,
    /// Entity repository.
    pub entity_repo: Arc<dyn EntityRepo>,
    /// Relationship repository.
    pub rel_repo: Arc<dyn RelRepo>,
    /// Event repository.
    pub event_repo: Arc<dyn EventRepo>,
    /// Canonical mutation client. Absent only in isolated repository mocks.
    pub mutation_storage: Option<StorageClient>,
    /// Authorization service.
    pub authz: AuthzState,
}

impl AppState {
    /// Creates new app state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self {
            storage: Some(client.clone()),
            entity_repo: Arc::new(EntityRepository::new(client.clone())),
            rel_repo: Arc::new(RelRepository::new(client.clone())),
            event_repo: Arc::new(SystemEventRepository::new(client.clone())),
            mutation_storage: Some(client),
            authz,
        }
    }

    /// Creates app state from pre-built repositories (used by fast tests).
    #[must_use]
    #[cfg(test)]
    pub fn new_with_repos(
        entity_repo: Arc<dyn EntityRepo>,
        rel_repo: Arc<dyn RelRepo>,
        event_repo: Arc<dyn EventRepo>,
        authz: AuthzState,
    ) -> Self {
        Self {
            storage: None,
            entity_repo,
            rel_repo,
            event_repo,
            mutation_storage: None,
            authz,
        }
    }
}

async fn begin_entity_mutation(
    state: &AppState,
    space: &str,
) -> StorageResult<Option<PublicMutation>> {
    match &state.mutation_storage {
        Some(storage) => PublicMutation::begin(storage.clone(), space)
            .await
            .map(Some),
        None => Ok(None),
    }
}

async fn create_entity_atomic(
    state: &AppState,
    mutation: Option<&PublicMutation>,
    proposed: Entity,
    actor: &str,
) -> StorageResult<Entity> {
    match mutation {
        Some(mutation) => mutation.create(proposed, actor).await,
        None => state.entity_repo.create(proposed).await,
    }
}

async fn replace_entity_atomic(
    state: &AppState,
    mutation: Option<&PublicMutation>,
    observed: &Entity,
    proposed: Entity,
    restore: bool,
    actor: &str,
) -> StorageResult<Entity> {
    match mutation {
        Some(mutation) => mutation.replace(observed, proposed, restore, actor).await,
        None => {
            state
                .entity_repo
                .replace_authorized(observed, proposed, restore)
                .await
        }
    }
}

/// Entity search request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySearchRequest {
    /// Free-text search query.
    #[serde(default)]
    pub query: Option<String>,
    /// Filter by entity type.
    #[serde(default)]
    pub entity_type: Option<EntityType>,
    /// Filter by space ID.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Include soft-deleted entities.
    #[serde(default)]
    pub include_deleted: bool,
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

/// Entity search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySearchResponse {
    /// Search results.
    pub results: Vec<Entity>,
    /// Next page cursor.
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count of authorized results.
    pub total: usize,
}

/// Error response body.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiError {
    /// Error code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional structured details.
    pub details: Option<serde_json::Value>,
}

impl Serialize for ApiError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use elephant_types::public_error::ErrorCode;
        use serde::ser::SerializeStruct;
        let code = match self.code.as_str() {
            "ACCESS_DENIED" | "FORBIDDEN" => ErrorCode::ForbiddenSpace,
            "UNAUTHORIZED" => ErrorCode::Unauthorized,
            "NOT_FOUND" => ErrorCode::NotFound,
            "UNKNOWN_SPACE" => ErrorCode::UnknownSpace,
            "CONFLICT" => ErrorCode::ConflictGeneration,
            "RUNTIME_READ_ONLY" | "CAPABILITY_DISABLED" => ErrorCode::CapabilityDisabled,
            "RUNTIME_NOT_READY" | "NOT_READY" => ErrorCode::NotReady,
            "PAYLOAD_TOO_LARGE" => ErrorCode::PayloadTooLarge,
            "PROVIDER_UNAPPROVED" => ErrorCode::ProviderUnapproved,
            "RATE_LIMITED" => ErrorCode::RateLimited,
            "INTERNAL_ERROR" | "STORAGE_ERROR" => ErrorCode::Unavailable,
            _ => ErrorCode::InvalidRequest,
        };
        let mut output = serializer.serialize_struct("ApiError", 5)?;
        output.serialize_field("error_code", &code)?;
        output.serialize_field("retryable", &code.retryable())?;
        output.serialize_field("message", &self.message)?;
        output.serialize_field("code", &self.code)?;
        output.serialize_field("details", &self.details)?;
        output.end()
    }
}

impl ApiError {
    /// Creates a new validation error.
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "VALIDATION_ERROR".to_string(),
            message: message.into(),
            details: None,
        }
    }

    /// Creates a new not found error.
    #[must_use]
    pub fn not_found(resource: &str, id: &str) -> Self {
        Self {
            code: "NOT_FOUND".to_string(),
            message: format!("{resource} not found: {id}"),
            details: None,
        }
    }

    /// Project a typed storage failure through the shared public error mapper.
    #[must_use = "Use the status and public error together in the HTTP response"]
    pub fn storage(error: &StorageError) -> (StatusCode, Json<Self>) {
        crate::public_error::legacy_storage_failure_ref(error)
    }

    /// Creates a new internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        tracing::warn!(error = %message.into(), "API storage operation failed");
        Self {
            code: "INTERNAL_ERROR".to_string(),
            message: "Storage operation unavailable".into(),
            details: None,
        }
    }
}

/// Batch request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest<T> {
    /// Items to process.
    pub items: Vec<T>,
    /// Whether to fail the whole batch if one item fails.
    #[serde(default)]
    pub atomic: bool,
}

/// Batch response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponse<T> {
    /// Successful items.
    pub succeeded: Vec<T>,
    /// Failed items.
    pub failed: Vec<BatchError>,
}

/// Batch entity read request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityBatchGetRequest {
    /// Entity IDs to load.
    pub ids: Vec<String>,
    /// Include soft-deleted entities.
    #[serde(default)]
    pub include_deleted: bool,
}

/// Batch error details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchError {
    /// Index in the original request.
    pub index: usize,
    /// Error message.
    pub error: String,
}

/// Context bundle request parameters.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ContextRequest {
    /// Depth of relationship traversal (default 1).
    pub depth: Option<u32>,
    /// Time window for recent events in days (default 30).
    pub window_days: Option<u32>,
    /// Maximum number of events (default 50).
    pub event_limit: Option<usize>,
    /// Whether to include full relationship objects (default false).
    #[serde(default)]
    pub with_relations: bool,
}

/// Entity context bundle response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityContext {
    /// The target entity.
    pub entity: Entity,
    /// Outgoing relationships from the entity.
    pub outgoing_relations: Vec<serde_json::Value>,
    /// Incoming relationships to the entity.
    pub incoming_relations: Vec<serde_json::Value>,
    /// Extended relations graph if depth > 1.
    pub relations: Option<Vec<serde_json::Value>>,
    /// Recent events involving this entity.
    pub recent_events: Vec<serde_json::Value>,
    /// Relevant safety/social alerts.
    pub alerts: Option<Vec<serde_json::Value>>,
}

/// Create entity request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntityRequest {
    /// The entity data.
    #[serde(flatten)]
    pub entity: Entity,
}

/// Update entity request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEntityRequest {
    /// The entity data to update.
    #[serde(flatten)]
    pub entity: Entity,
}

/// Entity query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct EntityQuery {
    /// Include soft-deleted entities.
    #[serde(default)]
    pub include_deleted: bool,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

fn normalize_entity_query(query: Option<String>) -> Option<String> {
    query.and_then(|query| {
        let trimmed = query.trim();
        if trimmed.is_empty() || trimmed == "*" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn entity_cursor(entity: &Entity) -> String {
    let id = entity.base.id.to_string();
    id.split(':')
        .nth(1)
        .map_or_else(|| id.clone(), str::to_string)
}

/// POST /v1/entities/search - Search entities.
async fn search_entities(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<EntitySearchRequest>,
) -> ApiResult<EntitySearchResponse> {
    let space_id = require_space_id(&principal)?;

    // Cross-space search validation.
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

    if let Some(q) = normalize_entity_query(req.query) {
        criteria = criteria.with_query(q);
    }
    if let Some(t) = req.entity_type {
        criteria = criteria.with_entity_type(t);
    }
    if req.include_deleted {
        criteria = criteria.include_deleted();
    }
    if let Some(ref c) = req.cursor {
        criteria = criteria.with_cursor(c.clone());
    }

    let mut collected: Vec<Entity> = Vec::new();
    let mut total_authorized = 0usize;
    let mut scan_cursor = req.cursor.clone();
    let mut next_cursor: Option<String> = None;
    let mut safety_limit = 0usize;
    let scan_limit = req.limit.max(500);

    loop {
        safety_limit += 1;
        if safety_limit > 1000 {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(
                    "Entity search pagination exceeded safety limit".to_string(),
                )),
            ));
        }

        let mut page_criteria = criteria.clone().with_limit(scan_limit);
        if let Some(cursor) = scan_cursor.clone() {
            page_criteria = page_criteria.with_cursor(cursor);
        }

        let result = state
            .entity_repo
            .search(&page_criteria)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?;

        let filtered = super::current_read::filter(
            state.storage.as_ref(),
            &state.authz,
            &principal,
            result.results,
            "read:entity",
        )
        .await?;
        total_authorized += filtered.len();

        if collected.len() < req.limit {
            for entity in filtered {
                if collected.len() >= req.limit {
                    break;
                }
                collected.push(entity);
            }
            if collected.len() >= req.limit && next_cursor.is_none() {
                next_cursor = collected.last().map(entity_cursor);
            }
        }

        if !result.has_more || result.cursor.is_none() {
            break;
        }

        scan_cursor = result.cursor;
    }

    let has_more = total_authorized > collected.len();

    Ok(Json(EntitySearchResponse {
        results: collected,
        cursor: next_cursor,
        has_more,
        total: total_authorized,
    }))
}

/// GET /v1/entities/{id} - Get entity by ID.
async fn get_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Query(query): Query<EntityQuery>,
) -> ApiResult<Entity> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let result = if query.include_deleted {
        state.entity_repo.get_with_deleted(&space_id, &rid).await
    } else {
        state.entity_repo.get(&space_id, &rid).await
    };

    match result {
        Ok(Some(entity)) => {
            // Authorize read
            state
                .authz
                .authorize_record_read(&principal, &entity, "read:entity")
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
                    entity,
                    "read:entity",
                )
                .await?,
            ))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Entity", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// POST /v1/entities/batch-get - Read multiple entities by ID.
async fn batch_get_entities(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<EntityBatchGetRequest>,
) -> ApiResult<BatchResponse<Entity>> {
    let space_id = require_space_id(&principal)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    let mut requested = Vec::new();
    for (index, id) in req.ids.into_iter().take(500).enumerate() {
        match parse_id(&id, TableName::Entity) {
            Ok(rid) => requested.push((index, id, rid)),
            Err(e) => failed.push(BatchError {
                index,
                error: e.to_string(),
            }),
        }
    }

    let ids = requested
        .iter()
        .map(|(_, _, rid)| rid.clone())
        .collect::<Vec<_>>();
    let entities = state
        .entity_repo
        .get_many(&space_id, &ids, req.include_deleted)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    let entities = super::current_read::filter(
        state.storage.as_ref(),
        &state.authz,
        &principal,
        entities,
        "read:entity",
    )
    .await?;
    let mut by_id: HashMap<RecordId, Entity> = entities
        .into_iter()
        .map(|entity| (entity.base.id.clone(), entity))
        .collect();

    for (index, original_id, rid) in requested {
        let Some(entity) = by_id.remove(&rid) else {
            failed.push(BatchError {
                index,
                error: format!("Entity not found: {original_id}"),
            });
            continue;
        };
        if let Err(e) = state
            .authz
            .authorize_record_read(&principal, &entity, "read:entity")
        {
            failed.push(BatchError {
                index,
                error: e.message,
            });
            continue;
        }
        succeeded.push(entity);
    }

    Ok(Json(BatchResponse { succeeded, failed }))
}

/// GET /v1/entities/{id}/with-relations - Get entity with all relations.
async fn get_entity_with_relations(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<EntityWithRelations> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    match state.entity_repo.get_with_relations(&space_id, &rid).await {
        Ok(Some(mut ewr)) => {
            // Authorize entity read
            state
                .authz
                .authorize_record_read(&principal, &ewr.entity, "read:entity")
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

            // Filter relationships
            ewr.outgoing_relations = super::current_read::filter(
                state.storage.as_ref(),
                &state.authz,
                &principal,
                ewr.outgoing_relations,
                "read:relationship",
            )
            .await?;
            ewr.incoming_relations = super::current_read::filter(
                state.storage.as_ref(),
                &state.authz,
                &principal,
                ewr.incoming_relations,
                "read:relationship",
            )
            .await?;

            Ok(Json(ewr))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Entity", &id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

/// GET /v1/entities/{id}/context - Get entity context bundle.
#[allow(clippy::too_many_lines)]
async fn get_entity_context(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Query(req): Query<ContextRequest>,
) -> ApiResult<EntityContext> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let depth = req.depth.unwrap_or(1).clamp(1, 3);
    let window_days = i64::from(req.window_days.unwrap_or(30));
    let event_limit = req.event_limit.unwrap_or(50);

    // 1. Get base entity with depth-1 relations.
    let mut entity_with_rels = match state.entity_repo.get_with_relations(&space_id, &rid).await {
        Ok(Some(ewr)) => ewr,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Entity", &id)),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            ))
        }
    };

    entity_with_rels.entity = super::current_read::require(
        state.storage.as_ref(),
        &state.authz,
        &principal,
        entity_with_rels.entity,
        "read:entity",
    )
    .await?;
    // Authorize base entity.
    state
        .authz
        .authorize_record_read(&principal, &entity_with_rels.entity, "read:entity")
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

    let outgoing_relations = super::current_read::filter(
        state.storage.as_ref(),
        &state.authz,
        &principal,
        entity_with_rels.outgoing_relations.clone(),
        "read:relationship",
    )
    .await?;
    let incoming_relations = super::current_read::filter(
        state.storage.as_ref(),
        &state.authz,
        &principal,
        entity_with_rels.incoming_relations.clone(),
        "read:relationship",
    )
    .await?;

    // 2. Traversal for depth > 1.
    let relations = if depth <= 1 {
        None
    } else {
        let mut seen_entity_ids: HashSet<RecordId> = HashSet::new();
        let mut seen_rel_ids: HashSet<RecordId> = HashSet::new();
        let mut frontier: Vec<RecordId> = Vec::new();

        seen_entity_ids.insert(entity_with_rels.entity.base.id.clone());
        frontier.push(entity_with_rels.entity.base.id.clone());

        let mut rels: Vec<Rel> = Vec::new();

        for _ in 0..depth {
            if frontier.is_empty() {
                break;
            }
            let mut next_frontier: Vec<RecordId> = Vec::new();

            for entity_id in frontier {
                let outgoing = state
                    .rel_repo
                    .find_outgoing(&space_id, &entity_id)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiError::internal(e.to_string())),
                        )
                    })?;
                let incoming = state
                    .rel_repo
                    .find_incoming(&space_id, &entity_id)
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiError::internal(e.to_string())),
                        )
                    })?;

                let outgoing = super::current_read::filter(
                    state.storage.as_ref(),
                    &state.authz,
                    &principal,
                    outgoing,
                    "read:relationship",
                )
                .await?;
                let incoming = super::current_read::filter(
                    state.storage.as_ref(),
                    &state.authz,
                    &principal,
                    incoming,
                    "read:relationship",
                )
                .await?;

                for rel in outgoing.iter().chain(incoming.iter()) {
                    if seen_rel_ids.insert(rel.base.id.clone()) {
                        rels.push(rel.clone());
                    }
                }

                for rel in outgoing {
                    let other = rel.object.id().clone();
                    if seen_entity_ids.insert(other.clone()) {
                        next_frontier.push(other);
                    }
                }
                for rel in incoming {
                    let other = rel.subject.id().clone();
                    if seen_entity_ids.insert(other.clone()) {
                        next_frontier.push(other);
                    }
                }
            }

            frontier = next_frontier;
        }

        Some(
            rels.into_iter()
                .map(|r| serde_json::to_value(&r).unwrap_or_default())
                .collect(),
        )
    };

    // Fetch recent events involving this entity (defaults: window=30d, limit=50).
    let since = chrono::Utc::now() - chrono::Duration::days(window_days);
    let recent_events = match state
        .event_repo
        .find_by_entity_since(&space_id, &rid, since, event_limit)
        .await
    {
        Err(_) => vec![],
        Ok(events) => super::current_read::filter(
            state.storage.as_ref(),
            &state.authz,
            &principal,
            events,
            "read:event",
        )
        .await?
        .into_iter()
        .map(|e| serde_json::to_value(&e).unwrap_or_default())
        .collect(),
    };

    Ok(Json(EntityContext {
        entity: entity_with_rels.entity,
        outgoing_relations: outgoing_relations
            .into_iter()
            .map(|r| serde_json::to_value(&r).unwrap_or_default())
            .collect(),
        incoming_relations: incoming_relations
            .into_iter()
            .map(|r| serde_json::to_value(&r).unwrap_or_default())
            .collect(),
        relations,
        recent_events,
        alerts: None, // Would populate if signals extension is enabled
    }))
}

/// POST /v1/entities - Create entity.
async fn create_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<CreateEntityRequest>,
) -> Result<(StatusCode, Json<Entity>), (StatusCode, Json<ApiError>)> {
    // Override space_id with principal's space to prevent cross-space writes
    let mut entity = req.entity;
    entity.base.space_id = require_space_id(&principal)?;

    // Authorize write
    state
        .authz
        .authorize_record_write(&principal, &entity, "write:entity")
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
        .authorize_record_read(&principal, &entity, "read:entity")
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

    let mutation = begin_entity_mutation(&state, &entity.base.space_id)
        .await
        .map_err(|e| entity_write_error(&e))?;
    match create_entity_atomic(&state, mutation.as_ref(), entity, &principal.subject).await {
        Ok(entity) => {
            state
                .authz
                .authorize_record_read(&principal, &entity, "read:entity")
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

            Ok((StatusCode::CREATED, Json(entity)))
        }
        Err(e) => Err(entity_write_error(&e)),
    }
}

// Ordinary writes can change content, but never grant authority to reclassify it.
fn authorize_entity_write(
    state: &AppState,
    principal: &AuthPrincipal,
    entity: &Entity,
) -> Result<(), AuthzError> {
    state
        .authz
        .authorize_record_write(principal, entity, "write:entity")?;
    state
        .authz
        .authorize_record_read(principal, entity, "read:entity")
}

fn authorize_entity_replacement(
    state: &AppState,
    principal: &AuthPrincipal,
    existing: &Entity,
    updated: &Entity,
) -> Result<(), AuthzError> {
    authorize_entity_write(state, principal, existing)?;
    authorize_entity_write(state, principal, updated)?;
    if existing.base.security != updated.base.security {
        return Err(AuthzError::access_denied(
            "ordinary entity updates cannot change security metadata",
        ));
    }
    elephant_policy::authorize_ordinary_update(&existing.base, &updated.base)
        .map_err(AuthzError::access_denied)?;
    Ok(())
}

fn entity_authz_error(error: AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}

fn entity_write_error(error: &StorageError) -> (StatusCode, Json<ApiError>) {
    ApiError::storage(error)
}

/// PUT /v1/entities/{id} - Update entity without reclassifying it.
async fn update_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateEntityRequest>,
) -> ApiResult<Entity> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let mutation = begin_entity_mutation(&state, &space_id)
        .await
        .map_err(|e| entity_write_error(&e))?;
    let existing = state
        .entity_repo
        .get(&space_id, &rid)
        .await
        .map_err(|error| entity_write_error(&error))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Entity", &id)),
            )
        })?;
    authorize_entity_replacement(&state, &principal, &existing, &req.entity)
        .map_err(entity_authz_error)?;
    let updated = replace_entity_atomic(
        &state,
        mutation.as_ref(),
        &existing,
        req.entity,
        false,
        &principal.subject,
    )
    .await
    .map_err(|error| entity_write_error(&error))?;
    Ok(Json(updated))
}

/// DELETE /v1/entities/{id} - Soft delete entity.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
)]
async fn delete_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let mutation = begin_entity_mutation(&state, &space_id)
        .await
        .map_err(|e| entity_write_error(&e))?;
    match state.entity_repo.get(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:entity")
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
                None => state.entity_repo.delete(&space_id, &rid).await,
            };
            result
                .map(|()| StatusCode::NO_CONTENT)
                .map_err(|e| entity_write_error(&e))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Entity", &id)),
        )),
        Err(e) => Err(ApiError::storage(&e)),
    }
}

/// POST /v1/entities/{id}/restore - Restore soft-deleted entity.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
)]
async fn restore_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Entity> {
    let space_id = require_space_id(&principal)?;
    let rid = parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let mutation = begin_entity_mutation(&state, &space_id)
        .await
        .map_err(|e| entity_write_error(&e))?;
    match state.entity_repo.get_with_deleted(&space_id, &rid).await {
        Ok(Some(existing)) => {
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:entity")
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
                .authorize_record_read(&principal, &existing, "read:entity")
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
                        .entity_repo
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
            Json(ApiError::not_found("Entity", &id)),
        )),
        Err(e) => Err(ApiError::storage(&e)),
    }
}

/// POST /v1/entities/upsert - Deterministic upsert.
async fn upsert_entity(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(entity): Json<Entity>,
) -> ApiResult<Entity> {
    let space_id = require_space_id(&principal)?;
    authorize_entity_write(&state, &principal, &entity).map_err(entity_authz_error)?;
    let rid = entity.base.id.clone();
    let mutation = begin_entity_mutation(&state, &space_id)
        .await
        .map_err(|e| entity_write_error(&e))?;
    let existing = state
        .entity_repo
        .get_with_deleted(&space_id, &rid)
        .await
        .map_err(|error| entity_write_error(&error))?;
    let updated = if let Some(existing) = existing {
        authorize_entity_replacement(&state, &principal, &existing, &entity)
            .map_err(entity_authz_error)?;
        replace_entity_atomic(
            &state,
            mutation.as_ref(),
            &existing,
            entity,
            true,
            &principal.subject,
        )
        .await
    } else {
        create_entity_atomic(&state, mutation.as_ref(), entity, &principal.subject).await
    }
    .map_err(|error| entity_write_error(&error))?;
    Ok(Json(updated))
}

/// POST /v1/entities/batch - Batch operations.
async fn batch_entities(
    State(state): State<Arc<AppState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<BatchRequest<Entity>>,
) -> ApiResult<BatchResponse<Entity>> {
    let space_id = require_space_id(&principal)?;
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for (index, entity) in req.items.into_iter().enumerate() {
        let mutation = match begin_entity_mutation(&state, &space_id).await {
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
        let rid = entity.base.id.clone();
        let result = match state.entity_repo.get_with_deleted(&space_id, &rid).await {
            Ok(Some(existing)) => {
                if let Err(error) =
                    authorize_entity_replacement(&state, &principal, &existing, &entity)
                {
                    failed.push(BatchError {
                        index,
                        error: error.message,
                    });
                    continue;
                }
                replace_entity_atomic(
                    &state,
                    mutation.as_ref(),
                    &existing,
                    entity,
                    true,
                    &principal.subject,
                )
                .await
            }
            Ok(None) => {
                if let Err(error) = authorize_entity_write(&state, &principal, &entity) {
                    failed.push(BatchError {
                        index,
                        error: error.message,
                    });
                    continue;
                }
                create_entity_atomic(&state, mutation.as_ref(), entity, &principal.subject).await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(entity) => succeeded.push(entity),
            Err(error @ StorageError::ErasureInProgress) => {
                return Err(ApiError::storage(&error));
            }
            Err(error) => failed.push(BatchError {
                index,
                error: error.to_string(),
            }),
        }
    }
    Ok(Json(BatchResponse { succeeded, failed }))
}

/// Returns entity routes.
pub fn entity_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/entities/search", post(search_entities))
        .route("/v1/entities", post(create_entity))
        .route("/v1/entities/upsert", post(upsert_entity))
        .route("/v1/entities/batch", post(batch_entities))
        .route("/v1/entities/batch-get", post(batch_get_entities))
        .route("/v1/entities/{id}", get(get_entity))
        .route("/v1/entities/{id}", put(update_entity))
        .route("/v1/entities/{id}", delete(delete_entity))
        .route("/v1/entities/{id}/restore", post(restore_entity))
        .route(
            "/v1/entities/{id}/with-relations",
            get(get_entity_with_relations),
        )
        .route("/v1/entities/{id}/context", get(get_entity_context))
}

#[cfg(test)]
#[path = "entities_test.rs"]
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
    fn test_entity_search_request_defaults() {
        let json = r"{}";
        let req: EntitySearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.limit, 50);
        assert!(!req.include_deleted);
    }

    #[test]
    fn test_entity_query_defaults() {
        let json = r"{}";
        let query: EntityQuery = serde_json::from_str(json).expect("deserialize");
        assert!(!query.include_deleted);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope"
    )]
    async fn entity_change_publication_writes_a_semantic_outbox_record() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"default", "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete", "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        })).expect("space fixture");
        elephant_storage::Repository::create(
            &elephant_storage::SpaceConfigRepository::new(storage.clone()),
            config,
        )
        .await
        .expect("space");
        let state = AppState::new(storage.clone(), Arc::new(crate::authz::AuthzService::new()));
        let entity = Entity::new(
            elephant_types::records::base::BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "default".to_string(),
                "test".to_string(),
            ),
            EntityType::new("topic"),
            "A topic".to_string(),
        );

        let mutation = begin_entity_mutation(&state, "default")
            .await
            .expect("begin");
        create_entity_atomic(&state, mutation.as_ref(), entity.clone(), "test")
            .await
            .expect("atomic create");

        let events = SystemEventRepository::new(storage)
            .get_after_seq("default", 0, 10)
            .await
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "entity.created");
        let entity_id = entity.base.id.to_string();
        assert_eq!(
            events[0].payload["records"][0]["id"].as_str(),
            Some(entity_id.as_str())
        );
    }
}
