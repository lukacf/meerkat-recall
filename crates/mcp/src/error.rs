//! Error types for the MCP crate.

use thiserror::Error;

/// Errors in MCP operations.
#[derive(Debug, Error)]
pub enum McpError {
    /// Tool not found.
    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    /// Tool execution error.
    #[error("Tool execution error: {0}")]
    ExecutionError(String),

    /// Duplicate tool name.
    #[error("Duplicate tool: {0}")]
    DuplicateTool(String),

    /// Invalid parameters.
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] elephant_storage::StorageError),

    /// Policy error.
    #[error("Policy error: {0}")]
    PolicyError(#[from] elephant_policy::PolicyError),
}

/// Result type for MCP operations.
pub type McpResult<T> = Result<T, McpError>;
