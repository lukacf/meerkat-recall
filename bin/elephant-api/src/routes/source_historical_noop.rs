//! Authenticated receipt-based unchanged check; never enters source intake.

use super::{require_space_id, ApiError, ApiResult, AuthPrincipal, SourcesState};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use elephant_storage::historical_adoption::{
    HistoricalAdoptionRepository, HistoricalNoopReceipt, HistoricalNoopRequest,
};
use elephant_storage::parse_id;
use elephant_storage::{DocRepository, Repository};
use elephant_types::publication::SourceAuthorizationSnapshot;
use elephant_types::{RecordId, TableName};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Full group identity from the completed offline adoption receipt. This is a
/// distinct contract from ordinary PUT and cannot certify different content.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    external_source_id: String,
    expected_generation: String,
    accepted_revision: String,
    accepted_request_fingerprint: String,
    original_commit_id: String,
    document_content_sha256: BTreeMap<RecordId, String>,
}

pub(super) async fn verify(
    State(state): State<Arc<SourcesState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<Request>,
) -> ApiResult<HistoricalNoopReceipt> {
    let request = authorize_request(&state, &principal, &id, body).await?;
    HistoricalAdoptionRepository::new(state.storage.clone())
        .verify_unchanged(&request)
        .await
        .map(Json)
        .map_err(super::super::source_intake::storage_error)
}

// The returned request owns every exact authorized image and the epoch observed
// before the first authorization. Storage rechecks them before returning a no-op.
async fn authorize_request(
    state: &SourcesState,
    principal: &AuthPrincipal,
    id: &str,
    body: Request,
) -> Result<HistoricalNoopRequest, (StatusCode, Json<ApiError>)> {
    let space = require_space_id(principal)?;
    if body.document_content_sha256.is_empty()
        || body.document_content_sha256.len()
            > elephant_storage::historical_adoption::MAX_HISTORICAL_GROUP_DOCUMENTS
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "historical group size outside supported bound",
            )),
        ));
    }
    let epoch = elephant_storage::canonical_commit::CanonicalRepository::new(state.storage.clone())
        .observe_epoch(&space)
        .await
        .map_err(super::super::source_intake::storage_error)?;
    let mut authorized_documents = BTreeMap::new();

    let id = parse_id(id, TableName::Source).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    let storage_error = super::super::source_intake::storage_error;
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
    let source = state
        .source_repo
        .get(&space, &id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Source", &id.to_string())),
            )
        })?;
    state
        .authz
        .authorize_record_read(principal, &source, "read:source")
        .map_err(denied)?;
    state
        .authz
        .authorize_record_write(principal, &source, "write:source")
        .map_err(denied)?;
    for id in body.document_content_sha256.keys() {
        if id.table() != TableName::Doc {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation("group members must be documents")),
            ));
        }
        let doc = DocRepository::new(state.storage.clone())
            .get(&space, id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::not_found("Doc", &id.to_string())),
                )
            })?;
        state
            .authz
            .authorize_record_read(principal, &doc, "read:doc:metadata")
            .map_err(denied)?;
        state
            .authz
            .authorize_record_write(principal, &doc, "write:doc")
            .map_err(denied)?;
        authorized_documents.insert(
            id.clone(),
            elephant_storage::canonical_commit::source_support_fingerprint(&doc)
                .map_err(storage_error)?,
        );
    }
    Ok(HistoricalNoopRequest {
        space_id: space,
        source_id: id,
        external_source_id: body.external_source_id,
        expected_generation: body.expected_generation,
        accepted_revision: body.accepted_revision,
        accepted_request_fingerprint: body.accepted_request_fingerprint,
        original_commit_id: body.original_commit_id,
        document_content_sha256: body.document_content_sha256,
        authorized: SourceAuthorizationSnapshot::from(&source),
        authorization_epoch: epoch,
        authorized_document_fingerprints: authorized_documents,
    })
}

#[cfg(test)]
#[path = "source_historical_noop_tests.rs"]
mod tests;
