//! Exact read-only access to existing private control rows.
//! This never enters a mutation closure (which can trigger a managed checkpoint).
use crate::{EmbeddedStore, StorageError, StorageResult};

pub async fn row(
    store: &EmbeddedStore,
    table: &str,
    space: &str,
    key: &str,
) -> StorageResult<Option<serde_json::Value>> {
    let Some(row) = store
        .extension_row_by_key(table, &format!("{table}:{key}"))
        .await?
    else {
        return Ok(None);
    };
    let actual_space = row
        .get("space_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| StorageError::DataIntegrityError("private control space missing".into()))?;
    if actual_space != space {
        return Ok(None);
    }
    Ok(Some(row))
}

pub async fn payload(
    store: &EmbeddedStore,
    table: &str,
    space: &str,
    key: &str,
) -> StorageResult<Option<String>> {
    row(store, table, space, key)
        .await?
        .map(|row| {
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| {
                    StorageError::DataIntegrityError("private control payload missing".into())
                })
        })
        .transpose()
}

/// Observe physical key existence without a space filter.
///
/// A foreign control prevents an absence proof even though scoped payload reads cannot return it.
/// `key` is the unqualified ID suffix, matching `row`/`payload` above.
pub async fn global_exists(
    client: &crate::StorageClient,
    table: &str,
    key: &str,
) -> StorageResult<bool> {
    #[derive(serde::Deserialize)]
    struct PhysicalControl {
        id: surrealdb::sql::Thing,
    }

    if let Some(store) = client.embedded_store() {
        return Ok(store
            .extension_row_by_key(table, &format!("{table}:{key}"))
            .await?
            .is_some());
    }
    let id = crate::source_publication::control_thing(table, key);
    let mut response = client
        .query_bind_checked(
            "SELECT id FROM ONLY $control;",
            crate::bindings! {"control"=>id.clone()},
        )
        .await?;
    let row: Option<PhysicalControl> = response
        .take(0)
        .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
    match row {
        Some(row) if row.id == id => Ok(true),
        Some(_) => Err(StorageError::DataIntegrityError(
            "physical control identity mismatch".into(),
        )),
        None => Ok(false),
    }
}

impl crate::source_publication::SourcePublicationRepository {
    /// Observe the exact physical head key without trusting its stored space or
    /// decoding its payload.
    ///
    /// A foreign/malformed row must prevent legacy absence.
    /// This returns no source content or authorization grant.
    ///
    /// # Errors
    /// Refuses invalid source identity, blank space, or unavailable storage.
    pub async fn head_exists_globally(
        &self,
        space: &str,
        source_id: &elephant_types::RecordId,
    ) -> StorageResult<bool> {
        if space.trim().is_empty() || source_id.table() != elephant_types::TableName::Source {
            return Err(StorageError::DataIntegrityError(
                "physical source head observation requires an explicit source and space".into(),
            ));
        }
        global_exists(
            &self.client,
            crate::source_publication::HEAD_TABLE,
            &crate::source_publication::head_key(space, source_id),
        )
        .await
    }
}

#[cfg(test)]
#[path = "private_control_read_tests.rs"]
mod tests;
