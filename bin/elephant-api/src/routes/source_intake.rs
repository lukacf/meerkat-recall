//! Shared retained HTTP source-intake authorization and queue handoff.

use axum::{http::StatusCode, Json};
use elephant_storage::source_intake::{AcceptedSourceIntake, IntakeControl, PreparedSourceIntake};
use elephant_storage::{find_existing_doc_for_doc, StorageClient, StorageError};
use elephant_types::records::{Doc, MediaAsset, Source};
use elephant_types::RecordId;

use super::entities::ApiError;
use crate::auth::Principal;
use crate::authz::AuthzState;

type IntakeError = (StatusCode, Json<ApiError>);

#[expect(
    clippy::needless_pass_by_value,
    reason = "Keep owned storage errors compatible with the existing map_err call sites"
)]
pub(super) fn storage_error(error: StorageError) -> IntakeError {
    if matches!(error, StorageError::ErasureInProgress) {
        return crate::public_error::legacy_storage_failure_ref(&error);
    }
    let status = match error {
        StorageError::Conflict(_) => StatusCode::CONFLICT,
        StorageError::DataIntegrityError(_)
        | StorageError::TypeError(_)
        | StorageError::SerializationError(_) => StatusCode::BAD_REQUEST,
        StorageError::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ApiError {
            code: if status == StatusCode::CONFLICT {
                "CONFLICT"
            } else {
                "SOURCE_INTAKE_ERROR"
            }
            .into(),
            message: error.to_string(),
            details: None,
        }),
    )
}

#[expect(
    clippy::large_futures,
    reason = "Retain the existing dispatch future layout and ownership across the original await"
)]
pub(super) async fn accept_intake(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &Principal,
    source: Source,
    doc: Option<&Doc>,
    control: IntakeControl,
    assets: &[MediaAsset],
) -> Result<(AcceptedSourceIntake, Option<RecordId>), IntakeError> {
    accept_intake_mode(
        storage, authz, principal, source, doc, control, assets, false, None, false,
    )
    .await
}

/// Retained structured source endpoint; arbitrary metadata on other endpoints
/// never grants contact publication capability.
#[expect(
    clippy::large_futures,
    reason = "Retain the existing dispatch future layout and ownership across the original await"
)]
pub(super) async fn accept_contact_intake(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &Principal,
    source: Source,
    control: IntakeControl,
) -> Result<(AcceptedSourceIntake, Option<RecordId>), IntakeError> {
    accept_intake_mode(
        storage,
        authz,
        principal,
        source,
        None,
        control,
        &[],
        false,
        None,
        true,
    )
    .await
}

#[expect(
    clippy::large_futures,
    reason = "Retain the existing dispatch future layout and ownership across the original await"
)]
pub(super) async fn restore_intake(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &Principal,
    mut source: Source,
    control: IntakeControl,
    authorized_document: Option<&Doc>,
) -> Result<(AcceptedSourceIntake, Option<RecordId>), IntakeError> {
    source.base.deleted_at = None;
    accept_intake_mode(
        storage,
        authz,
        principal,
        source,
        None,
        control,
        &[],
        true,
        authorized_document.cloned(),
        false,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "Retain the explicit source, document and authority inputs of the shared intake operation; Keep ordered authorization, source lifecycle and configuration checks in their existing operation"
)]
async fn accept_intake_mode(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &Principal,
    mut source: Source,
    doc: Option<&Doc>,
    control: IntakeControl,
    assets: &[MediaAsset],
    reactivate: bool,
    mut authorized_document: Option<Doc>,
    structured_contact: bool,
) -> Result<(AcceptedSourceIntake, Option<RecordId>), IntakeError> {
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
    if principal.space_id.as_deref() != Some(source.base.space_id.as_str())
        || source.base.space_id.trim().is_empty()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("source must belong to selected space")),
        ));
    }
    if let Some(old) = &authorized_document {
        authz
            .authorize_record_read(principal, old, "read:doc:metadata")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, old, "write:doc")
            .map_err(denied)?;
    }
    if let Some(doc) = doc {
        authz
            .authorize_record_read(principal, doc, "read:doc:metadata")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, doc, "write:doc")
            .map_err(denied)?;
        if let Some(old) = find_existing_doc_for_doc(storage, doc)
            .await
            .map_err(storage_error)?
        {
            authz
                .authorize_record_read(principal, &old, "read:doc:metadata")
                .map_err(denied)?;
            authz
                .authorize_record_write(principal, &old, "write:doc")
                .map_err(denied)?;
            authorized_document = Some(old);
        }
        if source.base.space_id != doc.base.space_id
            || source.base.security != doc.base.security
            || source.base.subjects != doc.base.subjects
            || source.base.subjects_state != doc.base.subjects_state
        {
            return Err(storage_error(StorageError::DataIntegrityError(
                "source/document security envelope mismatch".into(),
            )));
        }
        source
            .set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(doc))
            .map_err(|error| storage_error(error.into()))?;
    }
    if reactivate && control.expected_generation.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "restore requires intake.expected_generation",
            )),
        ));
    }
    let prepared = PreparedSourceIntake::prepare_with_assets(storage, source, control, assets)
        .await
        .map_err(storage_error)?;
    if let Some(old) = &prepared.previous {
        authz
            .authorize_record_read(principal, old, "read:source")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, old, "write:source")
            .map_err(denied)?;
    }
    authz
        .authorize_record_read(principal, &prepared.source, "read:source")
        .map_err(denied)?;
    authz
        .authorize_record_write(principal, &prepared.source, "write:source")
        .map_err(denied)?;
    for previous in &prepared.previous_documents {
        authz
            .authorize_record_read(principal, previous, "read:doc:metadata")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, previous, "write:doc")
            .map_err(denied)?;
    }
    if let Some(doc) = &prepared.document {
        authz
            .authorize_record_read(principal, doc, "read:doc:metadata")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, doc, "write:doc")
            .map_err(denied)?;
    }
    let prepared = match authorized_document {
        Some(document) => prepared.with_authorized_document(document),
        None => prepared,
    };
    let prepared = if reactivate {
        prepared.reactivate()
    } else {
        prepared
    };
    if let Some(receipt) = prepared
        .read_only_contact_retry(storage)
        .await
        .map_err(storage_error)?
    {
        return Ok((receipt, None));
    }
    let contact = if reactivate {
        prepared
            .prepare_restore_contact(storage)
            .await
            .map_err(storage_error)?
    } else if structured_contact {
        prepared
            .prepare_contact(storage)
            .await
            .map_err(storage_error)?
    } else {
        prepared
            .prepare_replacement_contact(storage)
            .await
            .map_err(storage_error)?
    };
    if let Some(contact) = &contact {
        for row in contact.authorizations() {
            authz
                .authorize_record_read(principal, row, row.read_scope())
                .map_err(denied)?;
            authz
                .authorize_record_write(principal, row, row.write_scope())
                .map_err(denied)?;
        }
    }
    let accepted = match &contact {
        Some(contact) => {
            prepared
                .accept_with_prepared_contact(storage, contact)
                .await
        }
        None => prepared.accept(storage).await,
    }
    .map_err(storage_error)?;
    if !accepted.needs_processing() {
        return Ok((accepted, None));
    }
    let queued = elephant_pipeline::source_queue::enqueue_accepted_source(
        storage,
        &accepted.source,
        &accepted.publication,
        &principal.subject,
    )
    .await;
    let work = match queued {
        Ok(id) => Some(id),
        Err(error) => {
            tracing::warn!(operation_id = %accepted.operation.operation_id, %error, "accepted source awaits queue recovery");
            None
        }
    };
    Ok((accepted, work))
}

pub(super) async fn retract_intake(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &Principal,
    source: &Source,
    control: IntakeControl,
    authorized_document: Option<&Doc>,
) -> Result<elephant_types::publication::OperationReceipt, IntakeError> {
    if principal.space_id.as_deref() != Some(source.base.space_id.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("source must belong to selected space")),
        ));
    }
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
    authz
        .authorize_record_read(principal, source, "read:source")
        .map_err(denied)?;
    authz
        .authorize_record_write(principal, source, "write:source")
        .map_err(denied)?;
    let prepared = elephant_storage::source_intake::PreparedSourceRetraction::prepare(
        storage,
        source,
        control,
        &principal.subject,
        authorized_document,
    )
    .await
    .map_err(storage_error)?;
    for document in &prepared.documents {
        authz
            .authorize_record_read(principal, document, "read:doc:metadata")
            .map_err(denied)?;
        authz
            .authorize_record_write(principal, document, "write:doc")
            .map_err(denied)?;
    }
    prepared.accept(storage).await.map_err(storage_error)
}

#[cfg(test)]
mod error_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn retained_intake_erasure_reservation_is_retryable_not_ready() {
        let (status, Json(error)) = storage_error(StorageError::ErasureInProgress);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let body = serde_json::to_value(error).unwrap();
        assert_eq!(body["error_code"], "not_ready");
        assert_eq!(body["retryable"], true);
        let (status, Json(error)) = storage_error(StorageError::Conflict(
            "erasure words are not a typed reservation".into(),
        ));
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(serde_json::to_value(error).unwrap()["retryable"], false);
    }
}
