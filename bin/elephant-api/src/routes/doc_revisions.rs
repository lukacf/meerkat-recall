//! Lossless, authorized inspection of an exact currently published revision.

use std::io::{self, Write};
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::header,
    response::{IntoResponse, Response},
    Extension,
};
use elephant_storage::{parse_id, read_eligibility::ReadEligibility, Repository, StorageError};
use elephant_types::publication::MAX_SOURCE_TEXT_BYTES;
use elephant_types::records::{BaseFields, DocRevision};
use elephant_types::{RecordId, TableName};

use super::docs::DocsState;
use crate::auth::Principal;
use crate::public_error::{ApiFailure, ErrorCode};

// Six JSON bytes per input byte covers the worst control-character escaping.
// Additional space holds the full DTO envelope; never truncate any DTO field.
const MAX_REVISION_RESPONSE_BYTES: usize = MAX_SOURCE_TEXT_BYTES * 6 + 65_536;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
pub(super) async fn get_doc_revision(
    State(state): State<Arc<DocsState>>,
    Extension(principal): Extension<Principal>,
    Path((doc_id, revision_id)): Path<(String, String)>,
) -> Result<Response, ApiFailure> {
    let space = principal
        .space_id
        .as_deref()
        .filter(|space| elephant_types::validation::validate_space_id(space).is_ok())
        .ok_or_else(|| ApiFailure::new(ErrorCode::ForbiddenSpace, "Select a granted space"))?;
    let doc_id = typed_id(&doc_id, TableName::Doc)?;
    let revision_id = typed_id(&revision_id, TableName::DocRevision)?;
    if state
        .space_config_repo
        .get_by_space_id(space)
        .await?
        .is_none()
    {
        return Err(ApiFailure::new(ErrorCode::UnknownSpace, "Unknown space"));
    }
    let doc = state
        .doc_repo
        .get(space, &doc_id)
        .await?
        .ok_or_else(absent)?;
    let revision = state
        .revision_repo
        .get(space, &revision_id)
        .await?
        .ok_or_else(absent)?;
    if revision.doc.id() != &doc_id || revision.base.space_id != doc.base.space_id {
        return Err(absent());
    }
    for scope in ["read:doc", "read:doc:raw"] {
        state.authz.authorize_record_read(&principal, &doc, scope)?;
        state
            .authz
            .authorize_record_read(&principal, &revision, scope)?;
    }
    let reader = ReadEligibility::new(state.storage.clone());
    let authorize = |base: &BaseFields| {
        ["read:doc", "read:doc:raw"]
            .into_iter()
            .all(|scope| super::current_read::authorized(&state.authz, &principal, base, scope))
    };
    // Revision eligibility checks immutable mapping and current parent policy
    // without requiring pending mutable Doc text to equal the published render.
    let (current_doc, _) = reader
        .revision(&revision, &authorize)
        .await
        .map_err(read_failure)?
        .ok_or_else(absent)?;
    if current_doc != doc {
        return Err(changed());
    }
    let bytes = encode_revision(&revision)?;
    // Retain exact original Doc/revision values across assembly as well as the
    // reader's original source/head observations. Do not adopt a newer snapshot.
    if state.doc_repo.get(space, &doc_id).await?.as_ref() != Some(&doc)
        || state.revision_repo.get(space, &revision_id).await?.as_ref() != Some(&revision)
    {
        return Err(changed());
    }
    reader.verify_current().await.map_err(read_failure)?;
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes).into_response())
}

fn typed_id(value: &str, table: TableName) -> Result<RecordId, ApiFailure> {
    parse_id(value, table)
        .ok()
        .filter(|id| id.table() == table)
        .ok_or_else(|| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid record identifier"))
}

fn absent() -> ApiFailure {
    ApiFailure::new(ErrorCode::NotFound, "Current document revision not found")
}

fn changed() -> ApiFailure {
    ApiFailure::new(
        ErrorCode::NotReady,
        "Document publication changed during read; retry",
    )
}

fn read_failure(error: StorageError) -> ApiFailure {
    if matches!(error, StorageError::Conflict(_)) {
        changed()
    } else {
        error.into()
    }
}

fn encode_revision(revision: &DocRevision) -> Result<Vec<u8>, ApiFailure> {
    let mut output = BoundedBody {
        bytes: Vec::new(),
        overflowed: false,
    };
    serde_json::to_writer(&mut output, revision).map_err(|_| {
        if output.overflowed {
            ApiFailure::new(
                ErrorCode::PayloadTooLarge,
                "Document revision exceeds 12065536 encoded JSON bytes",
            )
        } else {
            ApiFailure::new(
                ErrorCode::Unavailable,
                "Document revision encoding unavailable",
            )
        }
    })?;
    Ok(output.bytes)
}

struct BoundedBody {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl Write for BoundedBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_REVISION_RESPONSE_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return Err(io::Error::other("document revision response limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "doc_revisions_tests.rs"]
mod tests;
