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
        }
    }

    /// Creates an error response.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(message.into()),
            details: None,
        }
    }

    /// Creates an error response with details.
    #[must_use]
    pub fn error_with_details(message: impl Into<String>, details: serde_json::Value) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(message.into()),
            details: Some(details),
        }
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
