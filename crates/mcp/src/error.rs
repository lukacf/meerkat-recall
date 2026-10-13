//! Error types for the MCP crate.

use thiserror::Error;

/// Errors in MCP operations.
#[derive(Debug, Error)]
pub enum McpError {
    /// Stable public refusal or availability error.
    #[error(transparent)]
    Public(#[from] elephant_types::public_error::PublicError),
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

impl From<elephant_pipeline::PipelineError> for McpError {
    fn from(error: elephant_pipeline::PipelineError) -> Self {
        use elephant_pipeline::PipelineError;
        use elephant_storage::StorageError;
        match error {
            PipelineError::StorageError(error) => Self::StorageError(error),
            PipelineError::OutboxError(error)
                if matches!(
                    std::error::Error::source(&error)
                        .and_then(|source| source.downcast_ref::<StorageError>()),
                    Some(StorageError::ErasureInProgress)
                ) =>
            {
                Self::StorageError(StorageError::ErasureInProgress)
            }
            other => Self::ExecutionError(other.to_string()),
        }
    }
}

/// Result type for MCP operations.
pub type McpResult<T> = Result<T, McpError>;

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_pipeline::PipelineError;
    use elephant_storage::StorageError;

    #[test]
    fn pipeline_and_outbox_reservations_keep_the_storage_type() {
        for error in [
            PipelineError::StorageError(StorageError::ErasureInProgress),
            PipelineError::OutboxError(StorageError::ErasureInProgress.into()),
        ] {
            assert!(matches!(
                McpError::from(error),
                McpError::StorageError(StorageError::ErasureInProgress)
            ));
        }
        assert!(matches!(
            McpError::from(PipelineError::StorageError(StorageError::Conflict("changed".into()))),
            McpError::StorageError(StorageError::Conflict(message)) if message == "changed"
        ));
    }

    #[test]
    fn incidental_error_text_does_not_become_a_reservation() {
        let error = PipelineError::InvalidInput(
            "Source erasure is in progress; retry after cleanup".into(),
        );
        let expected = error.to_string();
        assert!(matches!(
            McpError::from(error),
            McpError::ExecutionError(message) if message == expected
        ));
    }
}
