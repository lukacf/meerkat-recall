//! Core MCP tool definitions.

use serde::{Deserialize, Serialize};

/// Standard MCP response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResponse<T> {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Result data (if ok).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    /// Error message (if not ok).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Additional error details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Stable classification for legacy response-envelope consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<elephant_types::public_error::ErrorCode>,
    /// Whether the unchanged refused request may be retried.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl<T> ToolResponse<T> {
    /// Creates a success response.
    #[must_use]
    pub const fn success(data: T) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
            details: None,
            error_code: None,
            retryable: None,
        }
    }

    /// Creates an error response.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        let message = message.into();
        // Preserve the established legacy refusal phrases at this compatibility
        // boundary. New handlers should return McpError::Public or error_with_code.
        let code = legacy_refusal_code(&message);
        Self::error_with_code(code, message)
    }

    /// A typed refusal without relying on the legacy message convention.
    #[must_use]
    pub fn error_with_code(
        code: elephant_types::public_error::ErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(message.into().chars().take(512).collect()),
            details: None,
            error_code: Some(code),
            retryable: Some(code.retryable()),
        }
    }

    /// Creates an error response with details.
    #[must_use]
    pub fn error_with_details(message: impl Into<String>, details: serde_json::Value) -> Self {
        let mut response = Self::error(message);
        response.details = Some(details);
        response
    }
}

/// ID response for created records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdResponse {
    /// Record ID.
    pub id: String,
}

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
    fn test_tool_response_success() {
        let response = ToolResponse::success(IdResponse {
            id: "entity:123".to_string(),
        });

        assert!(response.ok);
        assert!(response.data.is_some());
        assert!(response.error.is_none());
    }

    #[test]
    fn test_tool_response_error() {
        let response: ToolResponse<()> = ToolResponse::error("Something went wrong");

        assert!(!response.ok);
        assert!(response.data.is_none());
        assert_eq!(response.error, Some("Something went wrong".to_string()));
    }
}

fn legacy_refusal_code(message: &str) -> elephant_types::public_error::ErrorCode {
    use elephant_types::public_error::ErrorCode;
    let normalized = message.to_ascii_lowercase();
    if normalized.starts_with("access denied") || normalized.starts_with("unauthorized") {
        ErrorCode::ForbiddenSpace
    } else if normalized.contains("not found") {
        ErrorCode::NotFound
    } else if normalized.contains("already exists") || normalized.starts_with("conflict") {
        ErrorCode::ConflictGeneration
    } else {
        ErrorCode::InvalidRequest
    }
}

#[cfg(test)]
mod public_refusal_tests {
    use super::*;
    #[test]
    fn legacy_refusal_envelopes_carry_nonretryable_typed_codes() {
        let denied: ToolResponse<()> = ToolResponse::error("Access denied: missing raw scope");
        assert_eq!(
            denied.error_code,
            Some(elephant_types::public_error::ErrorCode::ForbiddenSpace)
        );
        assert_eq!(denied.retryable, Some(false));
        let success = ToolResponse::success(1);
        let encoded = serde_json::to_value(success).ok();
        assert_eq!(encoded, Some(serde_json::json!({"ok":true,"data":1})));
    }
}
