//! Helpers for required field decoding in storage repositories.

use crate::error::StorageError;

pub(crate) fn required_field<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
    value.ok_or_else(|| StorageError::QueryError(format!("Missing {field}")))
}
