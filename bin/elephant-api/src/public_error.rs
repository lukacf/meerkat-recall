//! HTTP projection of the shared public error contract.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use elephant_storage::StorageError;
pub use elephant_types::public_error::{ErrorCode, PublicError};

/// Error returned by the versioned public adapters.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ApiFailure(pub PublicError);

impl ApiFailure {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self(PublicError::new(code, message))
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.0.error_code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self.0)).into_response()
    }
}

impl From<StorageError> for ApiFailure {
    fn from(error: StorageError) -> Self {
        Self::from(&error)
    }
}

impl From<&StorageError> for ApiFailure {
    fn from(error: &StorageError) -> Self {
        match error {
            StorageError::ErasureInProgress => Self::new(
                ErrorCode::NotReady,
                "Source erasure is in progress; retry after cleanup",
            ),
            StorageError::NotFound(_) => Self::new(ErrorCode::NotFound, "Record not found"),
            StorageError::Conflict(code) if code.starts_with("unknown_space") => {
                Self::new(ErrorCode::UnknownSpace, "Unknown space")
            }
            StorageError::DataIntegrityError(code) if code == "payload_too_large" => Self::new(
                ErrorCode::PayloadTooLarge,
                "Source text exceeds 2000000 UTF-8 bytes",
            ),
            StorageError::Conflict(_) => Self::new(
                ErrorCode::ConflictGeneration,
                "Source generation or immutable revision changed; fetch its head before retrying.",
            ),
            other => {
                tracing::warn!(error = %other, "Public storage operation failed");
                Self::new(ErrorCode::Unavailable, "Storage operation unavailable")
            }
        }
    }
}

/// Preserve typed storage failures in routes retaining the legacy error envelope.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Preserve the owned legacy adapter signature and original error drop boundary."
)]
pub(crate) fn legacy_storage_failure(
    error: StorageError,
) -> (StatusCode, Json<crate::routes::entities::ApiError>) {
    legacy_storage_failure_ref(&error)
}

/// Borrowed projection for retained adapters that must inspect an error afterward.
pub(crate) fn legacy_storage_failure_ref(
    error: &StorageError,
) -> (StatusCode, Json<crate::routes::entities::ApiError>) {
    let public = ApiFailure::from(error).0;
    let status = StatusCode::from_u16(public.error_code.http_status())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let code = match public.error_code {
        ErrorCode::Unauthorized => "UNAUTHORIZED",
        ErrorCode::ForbiddenSpace => "FORBIDDEN",
        ErrorCode::UnknownSpace => "UNKNOWN_SPACE",
        ErrorCode::NotReady => "NOT_READY",
        ErrorCode::Unavailable => "STORAGE_ERROR",
        ErrorCode::CapabilityDisabled => "CAPABILITY_DISABLED",
        ErrorCode::ConflictGeneration => "CONFLICT",
        ErrorCode::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
        ErrorCode::ProviderUnapproved => "PROVIDER_UNAPPROVED",
        ErrorCode::RateLimited => "RATE_LIMITED",
        ErrorCode::InvalidRequest => "VALIDATION_ERROR",
        ErrorCode::NotFound => "NOT_FOUND",
    };
    (
        status,
        Json(crate::routes::entities::ApiError {
            code: code.into(),
            message: public.message,
            details: None,
        }),
    )
}

impl From<crate::authz::AuthzError> for ApiFailure {
    fn from(_: crate::authz::AuthzError) -> Self {
        Self::new(
            ErrorCode::ForbiddenSpace,
            "Operation not permitted in the selected space",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_status_and_body_preserve_the_shared_classification() -> Result<(), serde_json::Error>
    {
        for error in [
            StorageError::ErasureInProgress,
            StorageError::Conflict("generation changed".into()),
            StorageError::NotFound("private identifier".into()),
            StorageError::QueryError("private database query".into()),
        ] {
            let expected = match &error {
                StorageError::ErasureInProgress => ErrorCode::NotReady,
                StorageError::Conflict(_) => ErrorCode::ConflictGeneration,
                StorageError::NotFound(_) => ErrorCode::NotFound,
                _ => ErrorCode::Unavailable,
            };
            let (status, Json(body)) = legacy_storage_failure(error);
            let encoded = serde_json::to_value(body)?;
            assert_eq!(status.as_u16(), expected.http_status());
            assert_eq!(encoded["error_code"], serde_json::to_value(expected)?);
            assert_eq!(encoded["retryable"], expected.retryable());
            assert!(!encoded["message"]
                .as_str()
                .unwrap_or_default()
                .contains("private"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn reservation_is_retryable_503_and_generation_conflict_stays_409(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let response = ApiFailure::from(StorageError::ErasureInProgress).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 1024).await?;
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(
            body,
            serde_json::json!({
                "error_code": "not_ready",
                "message": "Source erasure is in progress; retry after cleanup",
                "retryable": true,
            })
        );
        let conflict = ApiFailure::from(StorageError::Conflict("generation changed".into()));
        assert_eq!(conflict.0.error_code, ErrorCode::ConflictGeneration);
        assert!(!conflict.0.retryable);
        assert_eq!(conflict.into_response().status(), StatusCode::CONFLICT);
        Ok(())
    }
}
