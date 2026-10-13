//! Error types for the policy crate.

use thiserror::Error;

/// Errors that can occur in policy evaluation.
#[derive(Debug, Error)]
pub enum PolicyError {
    /// Access denied.
    #[error("Access denied: {0}")]
    AccessDenied(String),

    /// Invalid policy configuration.
    #[error("Invalid policy: {0}")]
    InvalidPolicy(String),

    /// Policy not found.
    #[error("Policy not found: {0}")]
    PolicyNotFound(String),

    /// Invalid clearance.
    #[error("Invalid clearance: {0}")]
    InvalidClearance(String),

    /// Subject scope violation.
    #[error("Subject scope violation: {0}")]
    SubjectScopeViolation(String),

    /// Registry error.
    #[error("Registry error: {0}")]
    RegistryError(String),
}

/// Result type for policy operations.
pub type PolicyResult<T> = Result<T, PolicyError>;

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
    fn test_error_display() {
        let err = PolicyError::AccessDenied("insufficient clearance".to_string());
        assert!(err.to_string().contains("insufficient clearance"));
    }
}
