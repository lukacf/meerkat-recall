//! Error types for the extensions crate.

use thiserror::Error;

/// Errors in extension operations.
#[derive(Debug, Error)]
pub enum ExtensionError {
    /// Invalid manifest.
    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    /// Invalid ref path.
    #[error("Invalid ref path: {0}")]
    InvalidRefPath(String),

    /// Extension not found.
    #[error("Extension not found: {0}")]
    NotFound(String),

    /// Duplicate extension.
    #[error("Duplicate extension: {0}")]
    Duplicate(String),
}

/// Result type for extension operations.
pub type ExtensionResult<T> = Result<T, ExtensionError>;
