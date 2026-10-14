//! Original queue authority for retained staged embedding reindex writes.

use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};
use elephant_types::publication::AttemptFence;
use elephant_types::records::{SpaceConfig, WorkItem};

fn identity<'a>(config: &'a SpaceConfig, attempt: &AttemptFence) -> StorageResult<&'a str> {
    if config.base.space_id != attempt.space_id {
        return Err(StorageError::Conflict(
            "reindex attempt belongs to another space".into(),
        ));
    }
    config
        .processing
        .as_ref()
        .and_then(|p| p.pending_reindex.as_ref())
        .map(|p| p.id.as_str())
        .ok_or_else(|| StorageError::Conflict("no reindex pending".into()))
}

pub(crate) fn check(
    tx: &mut EmbeddedMutation<'_>,
    config: &SpaceConfig,
    attempt: &AttemptFence,
) -> StorageResult<()> {
    let expected = identity(config, attempt)?;
    let current = tx
        .get::<SpaceConfig>(&config.base.space_id, &config.base.id, false)?
        .ok_or_else(|| StorageError::Conflict("reindex space removed".into()))?;
    if current.processing != config.processing {
        return Err(StorageError::Conflict(
            "reindex configuration/content changed".into(),
        ));
    }
    crate::work_attempt_repository::check_attempt_fence(tx, attempt, chrono::Utc::now())?;
    let item = tx
        .get::<WorkItem>(&attempt.space_id, &attempt.work_item_id, false)?
        .ok_or_else(|| StorageError::Conflict("reindex claim is missing".into()))?;
    if item.kind != "reindex_space_embeddings"
        || item
            .payload
            .get("reindex_id")
            .and_then(serde_json::Value::as_str)
            != Some(expected)
    {
        return Err(StorageError::Conflict(
            "reindex original claim does not match staged profile".into(),
        ));
    }
    Ok(())
}

pub(crate) fn append(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, surrealdb::sql::Value>,
    config: &SpaceConfig,
    attempt: &AttemptFence,
) -> StorageResult<()> {
    let expected = identity(config, attempt)?;
    crate::work_attempt_repository::append_attempt_fence(sql, bindings, attempt)?;
    bindings.insert("reindex_expected_id".into(), expected.to_owned().into());
    bindings.extend(crate::bindings! {
        "reindex_guard_config" => crate::surreal::thing_from_record_id(&config.base.id),
        "reindex_guard_processing" => crate::surreal::stored_native_value(crate::surreal::json_to_surreal(serde_json::to_value(config.processing.as_ref().ok_or_else(|| StorageError::Conflict("reindex configuration missing".into()))?).map_err(|error| StorageError::SerializationError(error.to_string()))?)?),
    });
    sql.push_str("LET $reindex_current_config = (SELECT * FROM ONLY $reindex_guard_config);\nIF $reindex_current_config = NONE OR $reindex_current_config.deleted_at IS NOT NONE OR $reindex_current_config.space_id != $attempt_space OR $reindex_current_config.processing != $reindex_guard_processing { THROW 'reindex configuration/content changed'; };\n");
    sql.push_str("IF $fenced_work.kind != 'reindex_space_embeddings' OR $fenced_work.payload.reindex_id != $reindex_expected_id { THROW 'reindex original claim does not match staged profile'; };\n");
    Ok(())
}

/// Check original claim ownership before model preparation. Every subsequent
/// vector/progress transaction repeats this check inside its own write boundary.
///
/// # Errors
/// Rejects expired, superseded, foreign-space or differently targeted work.
pub async fn validate_attempt(
    client: &StorageClient,
    config: &SpaceConfig,
    attempt: &AttemptFence,
) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        return store.mutate(|tx| check(tx, config, attempt)).await;
    }
    let mut sql = "BEGIN TRANSACTION;\n".to_owned();
    let mut bindings = std::collections::BTreeMap::new();
    append(&mut sql, &mut bindings, config, attempt)?;
    sql.push_str("COMMIT TRANSACTION;");
    client.query_bind_checked(&sql, bindings).await?;
    Ok(())
}
