//! An explicit current token permission closes an exact action; document
//! metadata and model output cannot grant permission to this endpoint.
use super::{require_space_id, ApiError, ApiResult, AssertionsState, AuthPrincipal};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use elephant_pipeline::scheduler::StagedActionDispositionProposal;
use elephant_storage::action_disposition::{
    ActionDisposition, ActionDispositionRepository, CloseActionInput,
};
use elephant_storage::canonical_commit::{source_support_fingerprint, CanonicalRepository};
use elephant_storage::read_eligibility::{CurrentEvidence, ReadAuthorization, ReadEligibility};
use elephant_storage::{
    parse_id, EntityRepository, Repository, StorageClient, WorkArtifactRepository,
};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::BaseFields;
use elephant_types::records::WorkArtifact;
use elephant_types::{Link, RecordId, TableName};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    #[serde(default)]
    expected_action_fingerprint: Option<String>,
    subject_entity: RecordId,
    owner_statement: EvidenceSpan,
    reason: String,
    effective_at: chrono::DateTime<chrono::Utc>,
}
fn denied(error: crate::authz::AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}
fn unavailable() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError::internal("Action disposition storage unavailable")),
    )
}
fn authorization<'a>(
    state: &'a AssertionsState,
    principal: &'a AuthPrincipal,
) -> impl Fn(&BaseFields) -> bool + Send + Sync + 'a {
    move |base| {
        let scope = match base.id.table() {
            TableName::Doc | TableName::DocRevision | TableName::DocNode => "read:doc:raw",
            TableName::Source => "read:source",
            TableName::Entity => "read:entity",
            _ => "read:assertion",
        };
        super::super::current_read::authorized(&state.authz, principal, base, scope)
    }
}
pub(super) async fn close(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
    Json(body): Json<Request>,
) -> ApiResult<ActionDisposition> {
    let space = require_space_id(&principal)?;
    let storage = state.mutation_storage.as_ref().ok_or_else(unavailable)?;
    let repository = ActionDispositionRepository::new(storage.clone());
    state
        .authz
        .authorize_space_scope(&principal, &space, "manage:action")
        .map_err(denied)?;
    let epoch = repository
        .begin(&space)
        .await
        .map_err(super::super::source_intake::storage_error)?;
    let id = parse_id(&id, TableName::Assertion).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Invalid action identity")),
        )
    })?;
    let action = state
        .assertion_repo
        .get(&space, &id)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Action", "unavailable")),
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &action, "read:assertion")
        .map_err(denied)?;
    state
        .authz
        .authorize_record_write(&principal, &action, "write:assertion")
        .map_err(denied)?;
    if let Some(expected) = &body.expected_action_fingerprint {
        let actual = elephant_storage::canonical_commit::source_support_fingerprint(&action)
            .map_err(super::super::source_intake::storage_error)?;
        if expected != &actual {
            return Err(proposal_conflict());
        }
    }
    Box::pin(repository.close(
        &space,
        epoch,
        &principal.subject,
        CloseActionInput {
            action_id: id,
            subject_entity: body.subject_entity,
            owner_statement: body.owner_statement,
            reason: body.reason,
            effective_at: body.effective_at,
        },
        &authorization(&state, &principal),
    ))
    .await
    .map(Json)
    .map_err(super::super::source_intake::storage_error)
}
pub(super) async fn get(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<Option<ActionDisposition>> {
    let space = require_space_id(&principal)?;
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:assertion")
        .map_err(denied)?;
    let storage = state.storage.as_ref().ok_or_else(unavailable)?;
    let id = parse_id(&id, TableName::Assertion).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Invalid action identity")),
        )
    })?;
    ActionDispositionRepository::new(storage.clone())
        .current(&space, &id, &authorization(&state, &principal))
        .await
        .map(Json)
        .map_err(super::super::source_intake::storage_error)
}

fn proposal_conflict() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::CONFLICT,
        Json(ApiError::validation(
            "Action proposal is unavailable or its current evidence changed",
        )),
    )
}
/// One explicitly selected immutable artifact; no all-space proposal scan.
/// Model metadata remains untrusted after this current-authorization read.
pub(super) async fn proposal(
    State(state): State<Arc<AssertionsState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path((id, artifact_id)): Path<(String, String)>,
) -> ApiResult<StagedActionDispositionProposal> {
    let space = require_space_id(&principal)?;
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:assertion")
        .map_err(denied)?;
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:doc:raw")
        .map_err(denied)?;
    Box::pin(proposal_with_storage(
        &state,
        &principal,
        &space,
        &id,
        &artifact_id,
        state.storage.as_ref().ok_or_else(unavailable)?,
    ))
    .await
}

async fn proposal_with_storage(
    state: &AssertionsState,
    principal: &AuthPrincipal,
    space: &str,
    id: &str,
    artifact_id: &str,
    storage: &StorageClient,
) -> ApiResult<StagedActionDispositionProposal> {
    let canonical = CanonicalRepository::new(storage.clone());
    let epoch = canonical
        .observe_epoch(space)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(proposal_conflict)?;
    let action_id = parse_id(id, TableName::Assertion).map_err(|_| proposal_conflict())?;
    let artifact_id =
        parse_id(artifact_id, TableName::WorkArtifact).map_err(|_| proposal_conflict())?;
    let artifacts = WorkArtifactRepository::new(storage.clone());
    let artifact = artifacts
        .get(space, &artifact_id)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(proposal_conflict)?;
    let result =
        selected_proposal(state, principal, &artifact, &action_id).ok_or_else(proposal_conflict)?;
    let auth = authorization(state, principal);
    let reader = ReadEligibility::new(storage.clone());
    let evidence =
        proposal_evidence(state, &auth, space, storage, &reader, &action_id, &result).await?;
    verify_proposal_artifact(storage, space, &artifact, &result, &evidence).await?;
    // Work metadata cannot broaden current source or evidence visibility.
    for source_id in &artifact.base.provenance.sources {
        let source = elephant_storage::SourceRepository::new(storage.clone())
            .get(space, source_id)
            .await
            .map_err(super::super::source_intake::storage_error)?
            .ok_or_else(proposal_conflict)?;
        if reader
            .project(source, &auth)
            .await
            .map_err(super::super::source_intake::storage_error)?
            .is_none()
        {
            return Err(proposal_conflict());
        }
    }
    reader
        .verify_current()
        .await
        .map_err(super::super::source_intake::storage_error)?;
    drop(reader);
    if canonical
        .observe_epoch(space)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .as_deref()
        != Some(epoch.as_str())
        || artifacts
            .get(space, &artifact_id)
            .await
            .map_err(super::super::source_intake::storage_error)?
            .as_ref()
            != Some(&artifact)
    {
        return Err(proposal_conflict());
    }
    drop(canonical);
    drop(artifacts);
    Ok(Json(result))
}

fn selected_proposal(
    state: &AssertionsState,
    principal: &AuthPrincipal,
    artifact: &WorkArtifact,
    action_id: &RecordId,
) -> Option<StagedActionDispositionProposal> {
    if artifact.base.provenance.sources.len() > 256 {
        return None;
    }
    if !super::super::current_read::authorized(
        &state.authz,
        principal,
        &artifact.base,
        "read:doc:raw",
    ) || artifact.artifact_type
        != elephant_pipeline::scheduler::extraction_work_kinds::KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE
    {
        return None;
    }
    let rows = artifact
        .data
        .get("_action_disposition_proposals")
        .and_then(serde_json::Value::as_array)
        .filter(|rows| rows.len() <= 16)?;
    let mut candidates = Vec::new();
    let mut targets = std::collections::BTreeSet::new();
    for row in rows {
        let proposal: StagedActionDispositionProposal = serde_json::from_value(row.clone()).ok()?;
        if proposal.version != 1
            || proposal.authority != "untrusted_model_proposal"
            || proposal.target_action_id.table() != TableName::Assertion
            || proposal.subject_entity_id.table() != TableName::Entity
            || proposal.reason.trim().is_empty()
            || proposal.reason.len() > 4096
            || proposal.owner_statement.start >= proposal.owner_statement.end
            || proposal.owner_statement.end - proposal.owner_statement.start > 32768
            || !targets.insert(proposal.target_action_id.clone())
        {
            return None;
        }
        if &proposal.target_action_id == action_id {
            candidates.push(proposal);
        }
    }
    candidates.pop()
}

async fn proposal_evidence(
    state: &AssertionsState,
    authorize: &ReadAuthorization<'_>,
    space: &str,
    storage: &StorageClient,
    reader: &ReadEligibility,
    action_id: &RecordId,
    result: &StagedActionDispositionProposal,
) -> Result<CurrentEvidence, (StatusCode, Json<ApiError>)> {
    let action = state
        .assertion_repo
        .get(space, action_id)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(proposal_conflict)?;
    if source_support_fingerprint(&action).map_err(super::super::source_intake::storage_error)?
        != result.target_fingerprint
        || action
            .predicate_id
            .as_deref()
            .unwrap_or(&action.predicate_raw)
            != "requires_action"
        || action.subject_entity.as_ref().map(Link::id) != Some(&result.subject_entity_id)
        || reader
            .project(action, authorize)
            .await
            .map_err(super::super::source_intake::storage_error)?
            .is_none()
    {
        return Err(proposal_conflict());
    }
    let subject = EntityRepository::new(storage.clone())
        .get(space, &result.subject_entity_id)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(proposal_conflict)?;
    if reader
        .project(subject, authorize)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .is_none()
        || ActionDispositionRepository::new(storage.clone())
            .current(space, action_id, authorize)
            .await
            .map_err(super::super::source_intake::storage_error)?
            .is_some()
    {
        return Err(proposal_conflict());
    }
    reader
        .evidence(space, &result.owner_statement, authorize)
        .await
        .map_err(super::super::source_intake::storage_error)?
        .ok_or_else(proposal_conflict)
}

async fn verify_proposal_artifact(
    storage: &StorageClient,
    space: &str,
    artifact: &WorkArtifact,
    result: &StagedActionDispositionProposal,
    evidence: &CurrentEvidence,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let revision_text = result.owner_statement.revision.id().to_string();
    let node = result
        .owner_statement
        .node
        .as_ref()
        .ok_or_else(proposal_conflict)?
        .id();
    let node_text = node.to_string();
    let source = evidence
        .doc
        .source
        .as_ref()
        .ok_or_else(proposal_conflict)?
        .id();
    let source_text = source.to_string();
    if artifact
        .data
        .get("revision_id")
        .and_then(serde_json::Value::as_str)
        != Some(revision_text.as_str())
        || artifact
            .data
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            != Some(node_text.as_str())
        || artifact
            .data
            .get("source_id")
            .and_then(serde_json::Value::as_str)
            != Some(source_text.as_str())
    {
        return Err(proposal_conflict());
    }
    elephant_pipeline::scheduler::verify_action_proposal_producer(
        storage,
        space,
        artifact,
        result.owner_statement.revision.id(),
        node,
        source,
    )
    .await
    .map_err(|_| proposal_conflict())
}

#[cfg(test)]
#[path = "source_action_disposition_tests.rs"]
mod tests;
