//! Helpers for parsing SurrealDB responses without silent defaults.

use crate::error::{StorageError, StorageResult};

/// Parse the first result set of a response, returning a storage error on failure.
pub fn take_first<T>(response: &mut surrealdb::Response, context: &str) -> StorageResult<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("{context}: {e}")))
}
