//! Authorized management of current per-space service grants.

use axum::{
    extract::{rejection::JsonRejection, Path, State},
    routing::get,
    Extension, Json, Router,
};
use chrono::Utc;
use elephant_storage::{
    space_grant_repository::SpaceGrantRepository, SpaceConfigRepository, StorageClient,
};
use elephant_types::space_grant::SpaceGrant;
use serde::Deserialize;
use std::{collections::BTreeSet, sync::Arc};

use crate::public_error::{ApiFailure, ErrorCode};
use crate::{auth::Principal, AuthzService};

#[derive(Clone)]
pub struct GrantsState {
    storage: StorageClient,
    authz: Arc<AuthzService>,
}
impl GrantsState {
    #[expect(
        clippy::must_use_candidate,
        reason = "Retain the existing constructor and builder annotation contract."
    )]
    pub const fn new(storage: StorageClient, authz: Arc<AuthzService>) -> Self {
        Self { storage, authz }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantRequest {
    active: bool,
    scopes: BTreeSet<String>,
    expected_generation: Option<String>,
}

async fn authorize(
    state: &GrantsState,
    principal: &Principal,
    space: &str,
) -> Result<(), ApiFailure> {
    if !principal.bypass_abac && !principal.scopes.iter().any(|scope| scope == "manage:grant") {
        return Err(ApiFailure::new(
            ErrorCode::ForbiddenSpace,
            "Grant administration requires explicit manage:grant scope",
        ));
    }
    state
        .authz
        .authorize_space_scope(principal, space, "manage:grant")?;
    let config = SpaceConfigRepository::new(state.storage.clone())
        .get_by_space_id(space)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::UnknownSpace, "Unknown space"))?;
    state
        .authz
        .authorize_record_read(principal, &config, "manage:grant")?;
    Ok(())
}

async fn get_grant(
    State(state): State<Arc<GrantsState>>,
    Extension(principal): Extension<Principal>,
    Path((space, subject)): Path<(String, String)>,
) -> Result<Json<SpaceGrant>, ApiFailure> {
    authorize(&state, &principal, &space).await?;
    let grant = SpaceGrantRepository::new(state.storage.clone())
        .get(&space, &subject)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotFound, "Service grant not found"))?;
    Ok(Json(grant))
}

async fn put_grant(
    State(state): State<Arc<GrantsState>>,
    Extension(principal): Extension<Principal>,
    Path((space, subject)): Path<(String, String)>,
    request: Result<Json<GrantRequest>, JsonRejection>,
) -> Result<Json<SpaceGrant>, ApiFailure> {
    authorize(&state, &principal, &space).await?;
    let request = request
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid service grant JSON"))?
        .0;
    if subject.trim().is_empty()
        || subject.len() > 256
        || request.scopes.len() > 128
        || request.scopes.iter().any(|scope| {
            scope.is_empty() || scope.len() > 128 || scope.chars().any(char::is_control)
        })
    {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Invalid service grant subject or scopes",
        ));
    }
    let grant = SpaceGrant {
        space_id: space,
        subject,
        generation: String::new(),
        active: request.active,
        scopes: request.scopes,
        updated_by: principal.subject,
        updated_at: Utc::now(),
    };
    Ok(Json(
        SpaceGrantRepository::new(state.storage.clone())
            .replace(grant, request.expected_generation.as_deref())
            .await?,
    ))
}

pub fn grant_routes() -> Router<Arc<GrantsState>> {
    Router::new().route(
        "/v1/spaces/{space}/grants/{subject}",
        get(get_grant).put(put_grant),
    )
}
