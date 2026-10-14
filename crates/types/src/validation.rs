//! Validation utilities for Elephant types.
//!
//! This module provides validation functions and traits for ensuring
//! data integrity throughout the system.

use crate::error::{TypeError, TypeResult};
use crate::records::base::{BaseFields, Security, SubjectsState};

/// Trait for validating records before persistence.
pub trait Validate {
    /// Validates the record and returns any errors.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    fn validate(&self) -> TypeResult<()>;
}

/// Validates an opaque named space (1..256 UTF-8 bytes, trimmed, no controls).
///
/// # Errors
///
/// Returns an error for empty, oversized, untrimmed, or control-bearing names.
pub fn validate_space_id(space_id: &str) -> TypeResult<()> {
    if space_id.is_empty()
        || space_id.len() > 256
        || space_id.trim() != space_id
        || space_id.chars().any(char::is_control)
    {
        return Err(TypeError::ValidationError(
            "space_id must contain 1..256 UTF-8 bytes without edge whitespace or controls"
                .to_string(),
        ));
    }
    Ok(())
}

/// Validates that a `policy_id` is set when security is provided.
///
/// # Errors
///
/// Returns an error if `policy_id` is empty or if `level` is negative.
pub fn validate_security(security: &Security) -> TypeResult<()> {
    if security.policy_id.trim().is_empty() {
        return Err(TypeError::ValidationError(
            "security.policy_id cannot be empty".to_string(),
        ));
    }
    if security.level < 0 {
        return Err(TypeError::ValidationError(
            "security.level cannot be negative".to_string(),
        ));
    }
    Ok(())
}

/// Validates base fields common to all records.
///
/// # Errors
///
/// Returns an error if any of the base field validations fail.
pub fn validate_base_fields(base: &BaseFields) -> TypeResult<()> {
    validate_space_id(&base.space_id)?;
    validate_security(&base.security)?;

    // If subject_allowlist would be present (checked at API layer),
    // subjects must be populated unless explicitly allowed
    if base.subjects_state != SubjectsState::Complete && !base.subjects.is_empty() {
        // This is a warning case - subjects exist but state is not complete
        // The API layer will handle this more strictly
    }

    Ok(())
}

/// Validates an idempotency key.
///
/// # Errors
///
/// Returns an error if the key is empty or exceeds 256 characters.
pub fn validate_idempotency_key(key: &str) -> TypeResult<()> {
    if key.trim().is_empty() {
        return Err(TypeError::ValidationError(
            "idempotency_key cannot be empty".to_string(),
        ));
    }
    if key.len() > 256 {
        return Err(TypeError::ValidationError(
            "idempotency_key cannot exceed 256 characters".to_string(),
        ));
    }
    Ok(())
}

/// Validates a display name.
///
/// # Errors
///
/// Returns an error if the name is empty or exceeds 500 characters.
pub fn validate_display_name(name: &str) -> TypeResult<()> {
    if name.trim().is_empty() {
        return Err(TypeError::ValidationError(
            "display_name cannot be empty".to_string(),
        ));
    }
    if name.len() > 500 {
        return Err(TypeError::ValidationError(
            "display_name cannot exceed 500 characters".to_string(),
        ));
    }
    Ok(())
}

/// Validates a confidence score is within valid range.
///
/// # Errors
///
/// Returns an error if the value is outside `0.0..=1.0`.
pub fn validate_confidence(confidence: f64) -> TypeResult<()> {
    if !(0.0..=1.0).contains(&confidence) {
        return Err(TypeError::ValidationError(format!(
            "confidence must be between 0.0 and 1.0, got {confidence}"
        )));
    }
    Ok(())
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
    fn test_validate_space_id() {
        assert!(validate_space_id("space_1").is_ok());
        assert!(validate_space_id("").is_err());
        assert!(validate_space_id("   ").is_err());
        assert!(validate_space_id(&"a".repeat(200)).is_ok());
        assert!(validate_space_id(&"é".repeat(128)).is_ok());
        assert!(validate_space_id(&"é".repeat(129)).is_err());
        assert!(validate_space_id(" space").is_err());
        assert!(validate_space_id("space\n").is_err());
        assert!(validate_space_id("space\0name").is_err());
        // Opaque identifiers are not normalized or limited to the portable subset.
        assert!(validate_space_id("household/research:東京").is_ok());
    }

    #[test]
    fn test_validate_security() {
        let mut security = Security::default();
        assert!(validate_security(&security).is_err()); // Empty policy_id

        security.policy_id = "test_policy".to_string();
        assert!(validate_security(&security).is_ok());

        security.level = -1;
        assert!(validate_security(&security).is_err());
    }

    #[test]
    fn test_validate_idempotency_key() {
        assert!(validate_idempotency_key("idem_123").is_ok());
        assert!(validate_idempotency_key("").is_err());
        assert!(validate_idempotency_key(&"x".repeat(257)).is_err());
    }

    #[test]
    fn test_validate_display_name() {
        assert!(validate_display_name("John Doe").is_ok());
        assert!(validate_display_name("").is_err());
        assert!(validate_display_name(&"x".repeat(501)).is_err());
    }

    #[test]
    fn test_validate_confidence() {
        assert!(validate_confidence(0.0).is_ok());
        assert!(validate_confidence(0.5).is_ok());
        assert!(validate_confidence(1.0).is_ok());
        assert!(validate_confidence(-0.1).is_err());
        assert!(validate_confidence(1.1).is_err());
    }
}
