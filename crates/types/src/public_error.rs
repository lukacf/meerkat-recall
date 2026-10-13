//! Stable public errors shared by HTTP and MCP adapters.

use serde::{Deserialize, Serialize};

/// Machine-readable public error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Authentication is missing or invalid.
    Unauthorized,
    /// The caller cannot perform this operation in the selected space.
    ForbiddenSpace,
    /// The explicitly selected space does not exist.
    UnknownSpace,
    /// Required processing or recovery has not completed.
    NotReady,
    /// A required service is unavailable.
    Unavailable,
    /// The runtime does not permit this capability.
    CapabilityDisabled,
    /// The accepted source generation or immutable revision conflicts.
    ConflictGeneration,
    /// An input or output exceeds its published size limit.
    PayloadTooLarge,
    /// The selected space has no current approved processing binding.
    ProviderUnapproved,
    /// A bounded request budget has been exhausted.
    RateLimited,
    /// Input syntax, fields or limits are invalid.
    InvalidRequest,
    /// An authorized record is absent.
    NotFound,
}

impl ErrorCode {
    /// Whether repeating the unchanged request may succeed later.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::NotReady | Self::Unavailable | Self::RateLimited)
    }

    /// HTTP status corresponding to this classification.
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::Unauthorized => 401,
            Self::ForbiddenSpace | Self::CapabilityDisabled | Self::ProviderUnapproved => 403,
            Self::UnknownSpace | Self::NotFound => 404,
            Self::NotReady | Self::Unavailable => 503,
            Self::ConflictGeneration => 409,
            Self::PayloadTooLarge => 413,
            Self::RateLimited => 429,
            Self::InvalidRequest => 400,
        }
    }
}

/// Public error body. Internal database queries and credentials never belong here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct PublicError {
    /// Stable classification for fallback and retry decisions.
    pub error_code: ErrorCode,
    /// Bounded human-readable explanation.
    pub message: String,
    /// Whether an unchanged request may succeed later.
    pub retryable: bool,
}

impl PublicError {
    /// Constructs an error with retry behavior fixed by its classification.
    #[must_use]
    pub fn new(error_code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error_code,
            message: message.into().chars().take(512).collect(),
            retryable: error_code.retryable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_conflict_requires_reconciliation_instead_of_blind_retry() {
        let error = PublicError::new(
            ErrorCode::ConflictGeneration,
            "Fetch the current source head.",
        );
        assert_eq!(error.error_code.http_status(), 409);
        assert!(!error.retryable);
        assert_eq!(
            serde_json::to_value(error).ok(),
            Some(serde_json::json!({
                "error_code": "conflict_generation",
                "message": "Fetch the current source head.",
                "retryable": false
            }))
        );
    }
}
