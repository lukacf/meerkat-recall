//! Durable bounded archival for scheduler history. Queue selection stays hot.

use crate::work_item_repository::{WorkItemDb, WorkItemRepository, WORK_ITEM_PROJECTION};
use crate::work_run_repository::{WorkRunDb, WorkRunRepository, WORK_RUN_PROJECTION};
use crate::{StorageClient, StorageError, StorageResult};
use chrono::{DateTime, Utc};
use elephant_types::records::{WorkItem, WorkItemArchive, WorkItemStatus, WorkRun, WorkRunArchive};
use elephant_types::{RecordId, TableName};

/// Stable archive identity; public references retain the original table.
#[must_use]
pub(crate) fn archive_id(id: &RecordId) -> RecordId {
    RecordId::new(
        if id.table() == TableName::WorkItem {
            TableName::WorkItemArchive
        } else {
            TableName::WorkRunArchive
        },
        id.id(),
    )
}

fn item_archive(record: WorkItem) -> WorkItemArchive {
    let mut base = record.base.clone();
    base.id = archive_id(&record.base.id);
    WorkItemArchive {
        base,
        original_id: record.base.id.clone(),
        archived_at: Utc::now(),
        record,
    }
}
fn run_archive(record: WorkRun) -> WorkRunArchive {
    let mut base = record.base.clone();
    base.id = archive_id(&record.base.id);
    WorkRunArchive {
        base,
        original_id: record.base.id.clone(),
        archived_at: Utc::now(),
        record,
    }
}

pub(crate) async fn create_item(
    client: &StorageClient,
    record: WorkItem,
) -> StorageResult<WorkItem> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                if tx
                    .get_by_id::<WorkItemArchive>(&archive_id(&record.base.id), true)?
                    .is_some()
                {
                    return Err(StorageError::Conflict(record.base.id.to_string()));
                }
                tx.create(record).map_err(Into::into)
            })
            .await;
    }
    client.query_bind_checked(
        "BEGIN TRANSACTION; LET $cold = SELECT VALUE id FROM ONLY $archive; IF $cold != NONE { THROW 'work_identity_already_archived'; }; CREATE $id CONTENT $record; COMMIT TRANSACTION;",
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id), "archive" => crate::surreal::thing_from_record_id(&archive_id(&record.base.id)), "record" => WorkItemRepository::work_item_to_value(&record)? }
    ).await?;
    Ok(record)
}

pub(crate) async fn create_run(client: &StorageClient, record: WorkRun) -> StorageResult<WorkRun> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                if tx
                    .get_by_id::<WorkRunArchive>(&archive_id(&record.base.id), true)?
                    .is_some()
                {
                    return Err(StorageError::Conflict(record.base.id.to_string()));
                }
                tx.create(record).map_err(Into::into)
            })
            .await;
    }
    client.query_bind_checked(
        "BEGIN TRANSACTION; LET $cold = SELECT VALUE id FROM ONLY $archive; IF $cold != NONE { THROW 'work_identity_already_archived'; }; CREATE $id CONTENT $record; COMMIT TRANSACTION;",
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(&record.base.id), "archive" => crate::surreal::thing_from_record_id(&archive_id(&record.base.id)), "record" => WorkRunRepository::work_run_to_value(&record)? }
    ).await?;
    Ok(record)
}

pub(crate) async fn get_item(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
) -> StorageResult<Option<WorkItem>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                if let Some(item) = tx.get::<WorkItem>(space, id, true)? {
                    return Ok(item.base.deleted_at.is_none().then_some(item));
                }
                Ok(tx
                    .get::<WorkItemArchive>(space, &archive_id(id), false)?
                    .map(|v| v.record))
            })
            .await;
    }
    let mut response = client.query_bind(
        &format!("SELECT {WORK_ITEM_PROJECTION} FROM array::concat((SELECT * FROM work_item WHERE id = $id AND space_id = $space AND deleted_at IS NONE), (SELECT VALUE record FROM work_item_archive WHERE id = $archive AND space_id = $space AND deleted_at IS NONE)) LIMIT 1"),
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "archive" => crate::surreal::thing_from_record_id(&archive_id(id)), "space" => space }
    ).await?;
    let rows: Vec<WorkItemDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Work item history: {e}")))?;
    rows.into_iter().next().map(WorkItem::try_from).transpose()
}

pub(crate) async fn get_run(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
) -> StorageResult<Option<WorkRun>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                if let Some(run) = tx.get::<WorkRun>(space, id, true)? {
                    return Ok(run.base.deleted_at.is_none().then_some(run));
                }
                Ok(tx
                    .get::<WorkRunArchive>(space, &archive_id(id), false)?
                    .map(|v| v.record))
            })
            .await;
    }
    let mut response = client.query_bind(
        &format!("SELECT {WORK_RUN_PROJECTION} FROM array::concat((SELECT * FROM work_run WHERE id = $id AND space_id = $space AND deleted_at IS NONE), (SELECT VALUE record FROM work_run_archive WHERE id = $archive AND space_id = $space AND deleted_at IS NONE)) LIMIT 1"),
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "archive" => crate::surreal::thing_from_record_id(&archive_id(id)), "space" => space }
    ).await?;
    let rows: Vec<WorkRunDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Work run history: {e}")))?;
    rows.into_iter().next().map(WorkRun::try_from).transpose()
}

// DELETE of an absent archive is a SurrealQL no-op. Snapshot-isolated engines
// may also forget a deleted hot key's version. Write both identities so a
// concurrent archive conflicts on the surviving cold row, and a concurrent
// update conflicts on the surviving hot row. This temporary archive is deleted
// in the same transaction and is never committed as another history owner.
pub(crate) const RETIRE_ARCHIVE_SQL: &str =
    "UPSERT $archive CONTENT { space_id: $space, original_id: $id, archived_at: time::now(), record: $record }; DELETE $archive;";

/// Restore and replace in the same transaction, including conditional retries.
pub(crate) async fn update_item(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    mut record: WorkItem,
    expected: Option<WorkItemStatus>,
) -> StorageResult<Option<WorkItem>> {
    record.base.id = id.clone();
    record.base.space_id = space.to_string();
    record.base.updated_at = Utc::now();
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let hot = tx.get::<WorkItem>(space, id, true)?;
                let cold = tx.get::<WorkItemArchive>(space, &archive_id(id), false)?;
                let Some(existing) = hot.as_ref().or_else(|| cold.as_ref().map(|v| &v.record))
                else {
                    return Ok(None);
                };
                if existing.base.deleted_at.is_some()
                    || expected.is_some_and(|status| existing.status != status)
                {
                    return Ok(None);
                }
                if hot.is_some() {
                    let _ = tx.replace(record.clone())?;
                } else {
                    let _ = tx.create(record.clone())?;
                }
                if cold.is_some() {
                    tx.hard_delete::<WorkItemArchive>(space, &archive_id(id))?;
                }
                Ok(Some(record))
            })
            .await;
    }
    let mut response = client.query_bind_checked(
        &format!("BEGIN TRANSACTION; LET $hot = (SELECT * FROM ONLY $id WHERE space_id = $space); LET $cold = (SELECT * FROM ONLY $archive WHERE space_id = $space); LET $old = IF $hot != NONE {{ $hot }} ELSE {{ $cold.record }}; LET $changed = IF $old != NONE AND $old.deleted_at IS NONE AND ($expected = NONE OR $old.status = $expected) {{ UPSERT $id CONTENT $record; {RETIRE_ARCHIVE_SQL} RETURN true; }} ELSE {{ RETURN false; }}; RETURN $changed; COMMIT TRANSACTION;"),
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "archive" => crate::surreal::thing_from_record_id(&archive_id(id)), "space" => space, "expected" => expected.map_or(surrealdb::sql::Value::None, |v| v.to_string().into()), "record" => WorkItemRepository::work_item_to_value(&record)? }
    ).await?;
    let changed: Option<bool> = response
        .take(response.num_statements() - 1)
        .map_err(|e| StorageError::QueryError(format!("Restore item result: {e}")))?;
    Ok(changed.unwrap_or(false).then_some(record))
}

pub(crate) async fn update_run(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    mut record: WorkRun,
) -> StorageResult<WorkRun> {
    record.base.id = id.clone();
    record.base.space_id = space.to_string();
    record.base.updated_at = Utc::now();
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let hot = tx.get::<WorkRun>(space, id, true)?;
                let cold = tx.get::<WorkRunArchive>(space, &archive_id(id), false)?;
                let old = hot
                    .as_ref()
                    .or_else(|| cold.as_ref().map(|v| &v.record))
                    .ok_or_else(|| StorageError::NotFound(id.to_string()))?;
                if old.base.deleted_at.is_some() {
                    return Err(StorageError::NotFound(id.to_string()));
                }
                if hot.is_some() {
                    let _ = tx.replace(record.clone())?;
                } else {
                    let _ = tx.create(record.clone())?;
                }
                if cold.is_some() {
                    tx.hard_delete::<WorkRunArchive>(space, &archive_id(id))?;
                }
                Ok(record)
            })
            .await;
    }
    client.query_bind_checked(
        &format!("BEGIN TRANSACTION; LET $hot = (SELECT * FROM ONLY $id WHERE space_id = $space); LET $cold = (SELECT * FROM ONLY $archive WHERE space_id = $space); LET $old = IF $hot != NONE {{ $hot }} ELSE {{ $cold.record }}; IF $old = NONE OR $old.deleted_at IS NOT NONE {{ THROW 'work_run_not_found'; }}; UPSERT $id CONTENT $record; {RETIRE_ARCHIVE_SQL} COMMIT TRANSACTION;"),
        crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "archive" => crate::surreal::thing_from_record_id(&archive_id(id)), "space" => space, "record" => WorkRunRepository::work_run_to_value(&record)? }
    ).await?;
    Ok(record)
}

/// Counts moved terminal jobs and completed attempts.
#[derive(Debug, Default, Clone, Copy)]
pub struct WorkRetentionResult {
    /// Jobs moved out of the hot queue.
    pub items_archived: usize,
    /// Execution attempts moved out of the hot table.
    pub runs_archived: usize,
}

/// Archives at most `limit` jobs and `limit` runs per pass.
pub async fn archive_terminal_work(
    client: &StorageClient,
    cutoff: DateTime<Utc>,
    limit: usize,
) -> StorageResult<WorkRetentionResult> {
    let limit = limit.min(1000);
    if limit == 0 {
        return Ok(WorkRetentionResult::default());
    }
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let mut items = tx.scan::<WorkItem>()?;
                let runs = tx.scan::<WorkRun>()?;
                let open_parents: std::collections::HashSet<_> = runs
                    .iter()
                    .filter(|run| run.completed_at.is_none())
                    .map(|run| run.work_item.id().clone())
                    .collect();
                items.sort_by_key(|item| (item.base.updated_at, item.base.id.clone()));
                let mut result = WorkRetentionResult::default();
                for item in items
                    .iter()
                    .filter(|item| {
                        eligible_item(item, cutoff) && !open_parents.contains(&item.base.id)
                    })
                    .take(limit)
                {
                    let cold = item_archive(item.clone());
                    let _ = tx.create(cold)?;
                    tx.hard_delete::<WorkItem>(&item.base.space_id, &item.base.id)?;
                    result.items_archived += 1;
                }
                let mut runs = runs;
                runs.sort_by_key(|run| (run.completed_at, run.base.id.clone()));
                for run in runs
                    .into_iter()
                    .filter(|run| {
                        run.base.deleted_at.is_none()
                            && run.completed_at.is_some_and(|at| at < cutoff)
                    })
                    .take(limit)
                {
                    let _ = tx.create(run_archive(run.clone()))?;
                    tx.hard_delete::<WorkRun>(&run.base.space_id, &run.base.id)?;
                    result.runs_archived += 1;
                }
                Ok(result)
            })
            .await;
    }
    archive_surreal(client, cutoff, limit).await
}
const fn terminal(status: WorkItemStatus) -> bool {
    matches!(
        status,
        WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
    )
}
fn eligible_item(item: &WorkItem, cutoff: DateTime<Utc>) -> bool {
    terminal(item.status)
        && item.base.updated_at < cutoff
        && item.base.deleted_at.is_none()
        && item.leased_by.is_none()
        && item.lease_expires_at.is_none()
        && !matches!(
            item.kind.as_str(),
            "managed_wiki_registration" | "managed_wiki_registration_lock"
        )
}

async fn archive_surreal(
    client: &StorageClient,
    cutoff: DateTime<Utc>,
    limit: usize,
) -> StorageResult<WorkRetentionResult> {
    // Preload only open attempts once, rather than correlating a run table scan
    // for every candidate. The NONE guard is for SurrealDB's planner, which
    // evaluates functions without a row and abandons indexes if meta::tb fails.
    // Actual records always have IDs and still check legacy string references.
    let mut response = client.query_bind(
        &format!("LET $open_work = SELECT VALUE work_item FROM work_run WHERE completed_at IS NONE; SELECT {WORK_ITEM_PROJECTION} FROM work_item WITH INDEX work_item_updated WHERE status IN ['done','failed','cancelled'] AND updated_at < $cutoff AND deleted_at IS NONE AND leased_by IS NONE AND lease_expires_at IS NONE AND kind NOT IN ['managed_wiki_registration','managed_wiki_registration_lock'] AND array::all([id NOT IN $open_work, IF id IS NONE THEN true ELSE string::concat(meta::tb(id), ':', meta::id(id)) NOT IN $open_work END]) ORDER BY updated_at, id LIMIT {limit}"),
        crate::bindings! { "cutoff" => crate::surreal::datetime_from_chrono(cutoff) }
    ).await?;
    let rows: Vec<WorkItemDb> = response
        .take(1)
        .map_err(|e| StorageError::QueryError(format!("Retention candidates: {e}")))?;
    let mut result = WorkRetentionResult::default();
    for row in rows {
        let item = WorkItem::try_from(row)?;
        let mut response = client.query_bind_checked(
            "BEGIN TRANSACTION; LET $old = (SELECT * FROM ONLY $id WHERE status = $status AND updated_at = $updated AND deleted_at IS NONE AND leased_by IS NONE AND lease_expires_at IS NONE); LET $open = array::concat((SELECT VALUE id FROM work_run WHERE work_item = $id AND completed_at IS NONE LIMIT 1), (SELECT VALUE id FROM work_run WHERE work_item = $id_string AND completed_at IS NONE LIMIT 1)); LET $moved = IF $old != NONE AND array::len($open) = 0 { CREATE $archive CONTENT { space_id: $space, original_id: $id, archived_at: time::now(), record: $old }; DELETE $id; RETURN true; } ELSE { RETURN false; }; RETURN $moved; COMMIT TRANSACTION;",
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(&item.base.id), "archive" => crate::surreal::thing_from_record_id(&archive_id(&item.base.id)), "space" => item.base.space_id, "id_string" => item.base.id.to_string(), "status" => item.status.to_string(), "updated" => crate::surreal::datetime_from_chrono(item.base.updated_at) }
        ).await?;
        let moved: Option<bool> = response
            .take(response.num_statements() - 1)
            .map_err(|e| StorageError::QueryError(format!("Archive result: {e}")))?;
        result.items_archived += usize::from(moved.unwrap_or(false));
    }
    let mut response = client.query_bind(&format!("SELECT {WORK_RUN_PROJECTION} FROM array::union((SELECT * FROM work_run WHERE completed_at < $cutoff AND type::is::datetime(completed_at) AND deleted_at IS NONE ORDER BY completed_at, id LIMIT {limit}), (SELECT * FROM work_run WHERE completed_at < $cutoff_string AND type::is::string(completed_at) AND deleted_at IS NONE ORDER BY completed_at, id LIMIT {limit})) LIMIT {limit}"), crate::bindings! { "cutoff" => crate::surreal::datetime_from_chrono(cutoff), "cutoff_string" => cutoff.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true) }).await?;
    let rows: Vec<WorkRunDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Retention run candidates: {e}")))?;
    for row in rows {
        let run = WorkRun::try_from(row)?;
        let mut response = client.query_bind_checked(
            "BEGIN TRANSACTION; LET $old = (SELECT * FROM ONLY $id WHERE <datetime>updated_at = $updated AND <option<datetime>>completed_at < $cutoff AND completed_at IS NOT NONE AND deleted_at IS NONE); LET $moved = IF $old != NONE { CREATE $archive CONTENT { space_id: $space, original_id: $id, archived_at: time::now(), record: $old }; DELETE $id; RETURN true; } ELSE { RETURN false; }; RETURN $moved; COMMIT TRANSACTION;",
            crate::bindings! { "id" => crate::surreal::thing_from_record_id(&run.base.id), "archive" => crate::surreal::thing_from_record_id(&archive_id(&run.base.id)), "space" => run.base.space_id, "updated" => crate::surreal::datetime_from_chrono(run.base.updated_at), "cutoff" => crate::surreal::datetime_from_chrono(cutoff) }
        ).await?;
        let moved: Option<bool> = response
            .take(response.num_statements() - 1)
            .map_err(|e| StorageError::QueryError(format!("Archive run result: {e}")))?;
        result.runs_archived += usize::from(moved.unwrap_or(false));
    }
    Ok(result)
}

fn with_id(
    mut value: surrealdb::sql::Value,
    id: &RecordId,
) -> StorageResult<surrealdb::sql::Value> {
    let surrealdb::sql::Value::Object(ref mut object) = value else {
        return Err(StorageError::DataIntegrityError(
            "Expected work record object".to_string(),
        ));
    };
    object.insert(
        "id".to_string(),
        crate::surreal::thing_from_record_id(id).into(),
    );
    Ok(value)
}

/// Imports an archived item without reactivating it (snapshot migration).
pub async fn import_item_archive(
    client: &StorageClient,
    archive: WorkItemArchive,
) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        store.create(archive).await?;
        return Ok(());
    }
    client.query_bind_checked("CREATE $id CONTENT { space_id: $space, original_id: $original, archived_at: $at, record: $record };", crate::bindings! { "id" => crate::surreal::thing_from_record_id(&archive.base.id), "original" => crate::surreal::thing_from_record_id(&archive.original_id), "space" => archive.base.space_id, "at" => crate::surreal::datetime_from_chrono(archive.archived_at), "record" => with_id(WorkItemRepository::work_item_to_value(&archive.record)?, &archive.original_id)? }).await?;
    Ok(())
}
/// Imports an archived attempt without moving it to the hot table.
pub async fn import_run_archive(
    client: &StorageClient,
    archive: WorkRunArchive,
) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        store.create(archive).await?;
        return Ok(());
    }
    client.query_bind_checked("CREATE $id CONTENT { space_id: $space, original_id: $original, archived_at: $at, record: $record };", crate::bindings! { "id" => crate::surreal::thing_from_record_id(&archive.base.id), "original" => crate::surreal::thing_from_record_id(&archive.original_id), "space" => archive.base.space_id, "at" => crate::surreal::datetime_from_chrono(archive.archived_at), "record" => with_id(WorkRunRepository::work_run_to_value(&archive.record)?, &archive.original_id)? }).await?;
    Ok(())
}

pub(crate) async fn list_items(
    client: &StorageClient,
    space: &str,
    limit: usize,
    cursor: Option<&RecordId>,
) -> StorageResult<Vec<WorkItem>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let mut items: std::collections::BTreeMap<_, _> = tx
                    .scan::<WorkItemArchive>()?
                    .into_iter()
                    .filter(|v| v.base.space_id == space && v.base.deleted_at.is_none())
                    .map(|v| (v.original_id, v.record))
                    .collect();
                for item in tx.scan::<WorkItem>()? {
                    if item.base.space_id == space {
                        items.insert(item.base.id.clone(), item);
                    }
                }
                Ok(items
                    .into_values()
                    .filter(|v| {
                        v.base.deleted_at.is_none() && cursor.is_none_or(|c| v.base.id > *c)
                    })
                    .take(limit)
                    .collect())
            })
            .await;
    }
    let cursor_clause = if cursor.is_some() {
        " AND id > $cursor"
    } else {
        ""
    };
    let cold_cursor = if cursor.is_some() {
        " AND original_id > $cursor"
    } else {
        ""
    };
    let query = format!("SELECT {WORK_ITEM_PROJECTION} FROM array::concat((SELECT * FROM work_item WHERE space_id = $space AND deleted_at IS NONE{cursor_clause} ORDER BY id LIMIT {limit}), (SELECT * FROM work_item_archive WHERE space_id = $space AND deleted_at IS NONE{cold_cursor} ORDER BY original_id LIMIT {limit}).record) ORDER BY id LIMIT {limit}");
    let mut response = client.query_bind(&query, crate::bindings! { "space" => space, "cursor" => cursor.map_or(surrealdb::sql::Value::None, |v| crate::surreal::thing_from_record_id(v).into()) }).await?;
    let rows: Vec<WorkItemDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Item history list: {e}")))?;
    rows.into_iter().map(WorkItem::try_from).collect()
}
pub(crate) async fn list_runs(
    client: &StorageClient,
    space: &str,
    limit: usize,
    cursor: Option<&RecordId>,
) -> StorageResult<Vec<WorkRun>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let mut rows: std::collections::BTreeMap<_, _> = tx
                    .scan::<WorkRunArchive>()?
                    .into_iter()
                    .filter(|v| v.base.space_id == space && v.base.deleted_at.is_none())
                    .map(|v| (v.original_id, v.record))
                    .collect();
                for row in tx.scan::<WorkRun>()? {
                    if row.base.space_id == space {
                        rows.insert(row.base.id.clone(), row);
                    }
                }
                Ok(rows
                    .into_values()
                    .filter(|v| {
                        v.base.deleted_at.is_none() && cursor.is_none_or(|c| v.base.id > *c)
                    })
                    .take(limit)
                    .collect())
            })
            .await;
    }
    let cursor_clause = if cursor.is_some() {
        " AND id > $cursor"
    } else {
        ""
    };
    let cold_cursor = if cursor.is_some() {
        " AND original_id > $cursor"
    } else {
        ""
    };
    let query = format!("SELECT {WORK_RUN_PROJECTION} FROM array::concat((SELECT * FROM work_run WHERE space_id = $space AND deleted_at IS NONE{cursor_clause} ORDER BY id LIMIT {limit}), (SELECT * FROM work_run_archive WHERE space_id = $space AND deleted_at IS NONE{cold_cursor} ORDER BY original_id LIMIT {limit}).record) ORDER BY id LIMIT {limit}");
    let mut response = client.query_bind(&query, crate::bindings! { "space" => space, "cursor" => cursor.map_or(surrealdb::sql::Value::None, |v| crate::surreal::thing_from_record_id(v).into()) }).await?;
    let rows: Vec<WorkRunDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Run history list: {e}")))?;
    rows.into_iter().map(WorkRun::try_from).collect()
}
pub(crate) async fn find_item_key(
    client: &StorageClient,
    space: &str,
    key: &str,
) -> StorageResult<Option<WorkItem>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let hot = tx.scan::<WorkItem>()?.into_iter().find(|v| {
                    v.base.space_id == space
                        && v.base.deleted_at.is_none()
                        && v.idempotency_key == key
                });
                if hot.is_some() {
                    return Ok(hot);
                }
                Ok(tx
                    .scan::<WorkItemArchive>()?
                    .into_iter()
                    .find(|v| {
                        v.base.space_id == space
                            && v.base.deleted_at.is_none()
                            && v.record.idempotency_key == key
                    })
                    .map(|v| v.record))
            })
            .await;
    }
    let mut response = client.query_bind(&format!("SELECT {WORK_ITEM_PROJECTION} FROM array::concat((SELECT * FROM work_item WHERE space_id = $space AND idempotency_key = $key AND deleted_at IS NONE LIMIT 1), (SELECT VALUE record FROM work_item_archive WHERE space_id = $space AND record.idempotency_key = $key AND deleted_at IS NONE LIMIT 1)) LIMIT 1"), crate::bindings! { "space" => space, "key" => key }).await?;
    let rows: Vec<WorkItemDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Item history key: {e}")))?;
    rows.into_iter().next().map(WorkItem::try_from).transpose()
}
pub(crate) async fn runs_for_item(
    client: &StorageClient,
    id: &RecordId,
) -> StorageResult<Vec<WorkRun>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let mut rows: std::collections::BTreeMap<_, _> = tx
                    .scan::<WorkRunArchive>()?
                    .into_iter()
                    .filter(|v| v.base.deleted_at.is_none() && v.record.work_item.id() == id)
                    .map(|v| (v.original_id, v.record))
                    .collect();
                for row in tx.scan::<WorkRun>()? {
                    if row.work_item.id() == id {
                        rows.insert(row.base.id.clone(), row);
                    }
                }
                let mut rows: Vec<_> = rows
                    .into_values()
                    .filter(|v| v.base.deleted_at.is_none())
                    .collect();
                rows.sort_by_key(|v| (v.attempt, v.started_at, v.base.id.clone()));
                Ok(rows)
            })
            .await;
    }
    let mut response = client.query_bind(&format!("SELECT {WORK_RUN_PROJECTION} FROM array::concat((SELECT * FROM work_run WHERE work_item = $id AND deleted_at IS NONE), (SELECT * FROM work_run WHERE work_item = $id_string AND deleted_at IS NONE), (SELECT VALUE record FROM work_run_archive WHERE record.work_item = $id AND deleted_at IS NONE), (SELECT VALUE record FROM work_run_archive WHERE record.work_item = $id_string AND deleted_at IS NONE)) ORDER BY attempt, started_at, id"), crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "id_string" => id.to_string() }).await?;
    let rows: Vec<WorkRunDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Item run history: {e}")))?;
    rows.into_iter().map(WorkRun::try_from).collect()
}

pub(crate) async fn terminal_items(
    client: &StorageClient,
    space: &str,
    status: WorkItemStatus,
    kind: Option<&str>,
    limit: Option<usize>,
) -> StorageResult<Vec<WorkItem>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let mut rows: std::collections::BTreeMap<_, _> = tx
                    .scan::<WorkItemArchive>()?
                    .into_iter()
                    .filter(|v| v.base.space_id == space && v.base.deleted_at.is_none())
                    .map(|v| (v.original_id, v.record))
                    .collect();
                for row in tx.scan::<WorkItem>()? {
                    if row.base.space_id == space {
                        rows.insert(row.base.id.clone(), row);
                    }
                }
                let mut rows: Vec<_> = rows
                    .into_values()
                    .filter(|v| {
                        v.base.deleted_at.is_none()
                            && v.status == status
                            && kind.is_none_or(|k| v.kind == k)
                    })
                    .collect();
                rows.sort_by(|a, b| {
                    b.priority
                        .cmp(&a.priority)
                        .then(a.base.created_at.cmp(&b.base.created_at))
                        .then(a.base.id.cmp(&b.base.id))
                });
                if let Some(limit) = limit {
                    rows.truncate(limit);
                }
                Ok(rows)
            })
            .await;
    }
    let hot_kind = kind.map_or("", |_| " AND kind = $kind");
    let cold_kind = kind.map_or("", |_| " AND record.kind = $kind");
    let bound = limit.map_or_else(String::new, |v| format!(" LIMIT {v}"));
    let query = format!("SELECT {WORK_ITEM_PROJECTION} FROM array::concat((SELECT * FROM work_item WHERE space_id = $space AND status = $status AND deleted_at IS NONE{hot_kind} ORDER BY priority DESC, created_at ASC{bound}), (SELECT * FROM work_item_archive WHERE space_id = $space AND record.status = $status AND deleted_at IS NONE{cold_kind} ORDER BY record.priority DESC, record.created_at ASC{bound}).record) ORDER BY priority DESC, created_at ASC{bound}");
    let mut response = client.query_bind(&query, crate::bindings! { "space" => space, "status" => status.to_string(), "kind" => kind.map_or(surrealdb::sql::Value::None, |v| v.to_string().into()) }).await?;
    let rows: Vec<WorkItemDb> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Terminal work history: {e}")))?;
    rows.into_iter().map(WorkItem::try_from).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
    use super::*;
    use crate::{Repository, WorkEdgeRepository};
    use elephant_types::records::{BaseFields, WorkEdge, WorkRunMetrics};
    use elephant_types::Link;

    fn item(status: WorkItemStatus, kind: &str) -> WorkItem {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "space".to_string(),
            "test".to_string(),
        );
        base.updated_at = Utc::now() - chrono::Duration::days(60);
        WorkItem {
            idempotency_key: base.id.to_string(),
            base,
            kind: kind.to_string(),
            status,
            payload: serde_json::json!({}),
            priority: 1,
            attempts: 2,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("retained error".to_string()),
        }
    }
    async fn indexes(client: &StorageClient) {
        if client.embedded_store().is_none() {
            client
                .query_checked(include_str!(
                    "../../migrations/sql/028_work_queue_indexes.surql"
                ))
                .await
                .unwrap();
            client
                .query_checked(include_str!(
                    "../../migrations/sql/029_work_history_archives.surql"
                ))
                .await
                .unwrap();
        }
    }
    async fn exercise(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let old = items
            .create(item(WorkItemStatus::Failed, "extract_assertions"))
            .await
            .unwrap();
        let mut child = item(WorkItemStatus::Pending, "validate_artifacts");
        child.base.updated_at = Utc::now();
        let child = items.create(child).await.unwrap();
        WorkEdgeRepository::new(client.clone())
            .create(WorkEdge {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkEdge),
                    "space".to_string(),
                    "test".to_string(),
                ),
                from: Link::new(old.base.id.clone()),
                to: Link::new(child.base.id.clone()),
                edge_type: "depends_on".to_string(),
            })
            .await
            .unwrap();
        let mut run_base = BaseFields::new(
            RecordId::new_v4(TableName::WorkRun),
            "space".to_string(),
            "test".to_string(),
        );
        run_base.updated_at = old.base.updated_at;
        let run = runs
            .create(WorkRun {
                base: run_base,
                work_item: Link::new(old.base.id.clone()),
                attempt: 2,
                started_at: old.base.updated_at,
                completed_at: Some(old.base.updated_at),
                success: false,
                error: Some("preserved".to_string()),
                metrics: WorkRunMetrics {
                    tokens_used: 123,
                    ..WorkRunMetrics::default()
                },
                worker_id: "worker".to_string(),
                worker_version: "1".to_string(),
                model_version: Some("model".to_string()),
                prompt_id: Some("prompt".to_string()),
                config_hash: None,
            })
            .await
            .unwrap();
        let cutoff = Utc::now() - chrono::Duration::days(30);
        let moved = archive_terminal_work(&client, cutoff, 1).await.unwrap();
        assert_eq!(moved.items_archived, 1);
        assert_eq!(moved.runs_archived, 1);
        assert_eq!(
            items.get("space", &old.base.id).await.unwrap().unwrap(),
            old
        );
        assert!(items.get("other", &old.base.id).await.unwrap().is_none());
        assert_eq!(
            items
                .find_by_idempotency_key("space", &old.idempotency_key)
                .await
                .unwrap()
                .unwrap()
                .base
                .id,
            old.base.id
        );
        assert_eq!(items.list("space", 10, None).await.unwrap().len(), 2);
        assert_eq!(
            items
                .find_by_status_and_kind("space", WorkItemStatus::Failed, "extract_assertions", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            runs.get("space", &run.base.id)
                .await
                .unwrap()
                .unwrap()
                .metrics
                .tokens_used,
            123
        );
        assert_eq!(runs.find_by_work_item(&old.base.id).await.unwrap().len(), 1);
        let mut promoted = child.clone();
        promoted.status = WorkItemStatus::Runnable;
        assert!(items
            .update_if_status_and_dependencies_done(
                "space",
                &child.base.id,
                promoted,
                WorkItemStatus::Pending
            )
            .await
            .unwrap()
            .is_none());
        let mut retry = old.clone();
        retry.status = WorkItemStatus::Runnable;
        let restored = items
            .update_if_status("space", &old.base.id, retry, WorkItemStatus::Failed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.status, WorkItemStatus::Runnable);
        assert_eq!(items.count_runnable().await.unwrap(), 1);
        assert_eq!(
            archive_terminal_work(&client, cutoff, 10)
                .await
                .unwrap()
                .items_archived,
            0
        );
        items
            .update_status("space", &old.base.id, WorkItemStatus::Done)
            .await
            .unwrap();
        let mut promoted = child.clone();
        promoted.status = WorkItemStatus::Runnable;
        assert!(items
            .update_if_status_and_dependencies_done(
                "space",
                &child.base.id,
                promoted,
                WorkItemStatus::Pending
            )
            .await
            .unwrap()
            .is_some());
    }
    #[tokio::test]
    async fn retention_history_and_retry_surreal() {
        exercise(StorageClient::new_memory().await.unwrap()).await;
    }
    #[tokio::test]
    async fn retention_history_and_retry_embedded() {
        exercise(StorageClient::new_embedded().await.unwrap()).await;
    }
    fn run_for(parent: &WorkItem, attempt: u32, completed: bool) -> WorkRun {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkRun),
            parent.base.space_id.clone(),
            "test".to_string(),
        );
        base.updated_at = parent.base.updated_at;
        WorkRun {
            base,
            work_item: Link::new(parent.base.id.clone()),
            attempt,
            started_at: parent.base.updated_at,
            completed_at: completed.then_some(parent.base.updated_at),
            success: completed,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "worker".to_string(),
            worker_version: "1".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        }
    }

    async fn batches(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let cutoff = Utc::now() - chrono::Duration::days(30);
        // Older protected authority and open-run items must not fill the batch.
        for kind in [
            "managed_wiki_registration",
            "managed_wiki_registration_lock",
            "open_run",
        ] {
            let mut protected = item(WorkItemStatus::Done, kind);
            protected.base.updated_at -= chrono::Duration::days(10);
            items.create(protected.clone()).await.unwrap();
            if kind == "open_run" {
                runs.create(run_for(&protected, 1, false)).await.unwrap();
            }
        }
        let parent = items
            .create(item(WorkItemStatus::Done, "extract_assertions"))
            .await
            .unwrap();
        for attempt in 1..=3 {
            runs.create(run_for(&parent, attempt, true)).await.unwrap();
        }
        let mut items_total = 0;
        let mut runs_total = 0;
        for _ in 0..4 {
            let moved = archive_terminal_work(&client, cutoff, 1).await.unwrap();
            assert!(moved.items_archived <= 1 && moved.runs_archived <= 1);
            items_total += moved.items_archived;
            runs_total += moved.runs_archived;
        }
        assert_eq!(items_total, 1);
        assert_eq!(runs_total, 3);
        assert_eq!(
            runs.find_by_work_item(&parent.base.id).await.unwrap().len(),
            3
        );
        assert!(items.create(parent.clone()).await.is_err());
        let mut cross_space_duplicate = parent.clone();
        cross_space_duplicate.base.space_id = "other".to_string();
        assert!(items.create(cross_space_duplicate).await.is_err());
        assert_eq!(
            items.get("space", &parent.base.id).await.unwrap(),
            Some(parent.clone())
        );
        items.delete("space", &parent.base.id).await.unwrap();
        assert!(items.get("space", &parent.base.id).await.unwrap().is_none());
        assert!(items
            .find_by_idempotency_key("space", &parent.idempotency_key)
            .await
            .unwrap()
            .is_none());
        assert_eq!(items.list("space", 100, None).await.unwrap().len(), 3);
    }
    #[tokio::test]
    async fn retention_bounded_batches_surreal() {
        batches(StorageClient::new_memory().await.unwrap()).await;
    }
    #[tokio::test]
    async fn retention_bounded_batches_embedded() {
        batches(StorageClient::new_embedded().await.unwrap()).await;
    }

    async fn wiki_config(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let grant = items
            .create_managed_wiki_grant("space", "owner")
            .await
            .unwrap();
        let mut config = item(WorkItemStatus::Done, "reconcile_living_wiki");
        config.payload = serde_json::json!({"authority": {"grant_id": grant, "space_id": "space"}, "interval": 30});
        items
            .create_with_managed_wiki_grant(config.clone(), &grant)
            .await
            .unwrap();
        assert_eq!(
            archive_terminal_work(&client, Utc::now() - chrono::Duration::days(30), 10)
                .await
                .unwrap()
                .items_archived,
            1
        );
        assert_eq!(
            items.find_latest_managed_wiki_work("space").await.unwrap(),
            Some(config)
        );
        assert!(items
            .managed_wiki_grant_is_active("space", &grant)
            .await
            .unwrap());
        items.revoke_managed_wiki_grants("space").await.unwrap();
        assert!(items
            .find_latest_managed_wiki_work("space")
            .await
            .unwrap()
            .is_none());
    }
    #[tokio::test]
    async fn retention_wiki_config_surreal() {
        wiki_config(StorageClient::new_memory().await.unwrap()).await;
    }
    #[tokio::test]
    async fn retention_wiki_config_embedded() {
        wiki_config(StorageClient::new_embedded().await.unwrap()).await;
    }

    #[tokio::test]
    async fn retention_snapshot_and_migration_preserve_native_fields() {
        let client = StorageClient::new_embedded().await.unwrap();
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let mut old = item(WorkItemStatus::Failed, "extract_assertions");
        old.base.security.labels = vec!["confidential".to_string()];
        old.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
        old.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        old.base.subjects_state = elephant_types::records::SubjectsState::Complete;
        let run = run_for(&old, 1, true);
        old.base.provenance.work_run_id = Some(run.base.id.clone());
        items.create(old.clone()).await.unwrap();
        runs.create(run.clone()).await.unwrap();
        archive_terminal_work(&client, Utc::now() - chrono::Duration::days(30), 10)
            .await
            .unwrap();
        let bytes = client
            .embedded_store()
            .unwrap()
            .export_snapshot_bytes()
            .await
            .unwrap();
        let restored = StorageClient::new_embedded().await.unwrap();
        restored
            .embedded_store()
            .unwrap()
            .import_snapshot_bytes(&bytes)
            .await
            .unwrap();
        assert_eq!(
            WorkItemRepository::new(restored.clone())
                .get("space", &old.base.id)
                .await
                .unwrap(),
            Some(old.clone())
        );
        let surreal = StorageClient::new_memory().await.unwrap();
        for archive in restored
            .embedded_store()
            .unwrap()
            .scan::<WorkItemArchive>()
            .await
            .unwrap()
        {
            import_item_archive(&surreal, archive).await.unwrap();
        }
        for archive in restored
            .embedded_store()
            .unwrap()
            .scan::<WorkRunArchive>()
            .await
            .unwrap()
        {
            import_run_archive(&surreal, archive).await.unwrap();
        }
        assert_eq!(
            WorkItemRepository::new(surreal.clone())
                .get("space", &old.base.id)
                .await
                .unwrap(),
            Some(old)
        );
        assert_eq!(
            WorkRunRepository::new(surreal.clone())
                .get("space", &run.base.id)
                .await
                .unwrap(),
            Some(run.clone())
        );
        assert_eq!(
            WorkRunRepository::new(surreal)
                .find_by_work_item(run.work_item.id())
                .await
                .unwrap(),
            vec![run]
        );
    }

    async fn assert_hot_only(client: &StorageClient, id: &RecordId) {
        if client.embedded_store().is_some() {
            return;
        }
        let mut rows = client
            .query_bind_checked(
                "SELECT VALUE 'present' FROM $id; SELECT VALUE 'present' FROM $archive;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "archive" => crate::surreal::thing_from_record_id(&archive_id(id))
                },
            )
            .await
            .unwrap();
        let hot: Vec<String> = rows.take(0).unwrap();
        let cold: Vec<String> = rows.take(1).unwrap();
        assert_eq!(hot, vec!["present"], "acknowledged update must stay hot");
        assert!(
            cold.is_empty(),
            "acknowledged update must leave no stale archive"
        );
    }

    async fn retry_race(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        for _ in 0..32 {
            let old = items
                .create(item(WorkItemStatus::Failed, "retry"))
                .await
                .unwrap();
            let mut next = old.clone();
            next.status = WorkItemStatus::Runnable;
            let (archived, retried) = tokio::join!(
                archive_terminal_work(&client, Utc::now() - chrono::Duration::days(30), 1),
                items.update_if_status("space", &old.base.id, next, WorkItemStatus::Failed)
            );
            archived.unwrap();
            retried.unwrap().unwrap();
            assert_hot_only(&client, &old.base.id).await;
            assert_eq!(
                items
                    .get("space", &old.base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                WorkItemStatus::Runnable
            );
            assert_eq!(
                items
                    .list("space", 100, None)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|value| value.base.id == old.base.id)
                    .count(),
                1
            );
        }
    }
    #[tokio::test]
    async fn retention_retry_race_surreal() {
        retry_race(StorageClient::new_memory().await.unwrap()).await;
    }
    #[tokio::test]
    async fn retention_retry_race_embedded() {
        retry_race(StorageClient::new_embedded().await.unwrap()).await;
    }
    async fn sibling_update_races(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let cutoff = Utc::now() - chrono::Duration::days(30);
        let parent = items
            .create(item(WorkItemStatus::Runnable, "run_parent"))
            .await
            .unwrap();
        for iteration in 0..32 {
            let old = runs
                .create(run_for(&parent, iteration + 1, true))
                .await
                .unwrap();
            let mut next = old.clone();
            next.completed_at = None;
            let (archived, updated) = if iteration % 2 == 0 {
                tokio::join!(biased;
                    archive_terminal_work(&client, cutoff, 1),
                    runs.update("space", &old.base.id, next)
                )
            } else {
                let (updated, archived) = tokio::join!(biased;
                    runs.update("space", &old.base.id, next),
                    archive_terminal_work(&client, cutoff, 1)
                );
                (archived, updated)
            };
            archived.unwrap();
            let updated = updated.unwrap();
            assert_hot_only(&client, &old.base.id).await;
            assert_eq!(
                runs.get("space", &old.base.id).await.unwrap(),
                Some(updated)
            );

            let old = items
                .create(item(WorkItemStatus::Failed, "conditional_retry"))
                .await
                .unwrap();
            let mut next = old.clone();
            next.status = WorkItemStatus::Runnable;
            let (archived, updated) = tokio::join!(
                archive_terminal_work(&client, cutoff, 1),
                items.update_if_status_and_dependencies_done(
                    "space",
                    &old.base.id,
                    next,
                    WorkItemStatus::Failed
                )
            );
            archived.unwrap();
            if let Some(updated) = updated.unwrap() {
                assert_hot_only(&client, &old.base.id).await;
                assert_eq!(
                    items.get("space", &old.base.id).await.unwrap(),
                    Some(updated)
                );
            } else {
                // This method intentionally updates hot items only. If archival
                // wins, a refusal is valid; an acknowledged stale update is not.
                assert_eq!(
                    items
                        .get("space", &old.base.id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    WorkItemStatus::Failed
                );
            }
        }
    }

    #[tokio::test]
    async fn retention_sibling_update_races_surreal() {
        sibling_update_races(StorageClient::new_memory().await.unwrap()).await;
    }

    #[tokio::test]
    async fn retention_sibling_update_races_embedded() {
        sibling_update_races(StorageClient::new_embedded().await.unwrap()).await;
    }

    async fn both_commit_orders(client: StorageClient) {
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let cutoff = Utc::now() - chrono::Duration::days(30);
        for archive_first in [false, true] {
            let old = items
                .create(item(WorkItemStatus::Failed, "ordered_retry"))
                .await
                .unwrap();
            if archive_first {
                assert_eq!(
                    archive_terminal_work(&client, cutoff, 1)
                        .await
                        .unwrap()
                        .items_archived,
                    1
                );
            }
            let mut next = old.clone();
            next.status = WorkItemStatus::Runnable;
            let updated = items.update("space", &old.base.id, next).await.unwrap();
            if !archive_first {
                assert_eq!(
                    archive_terminal_work(&client, cutoff, 1)
                        .await
                        .unwrap()
                        .items_archived,
                    0
                );
            }
            assert_hot_only(&client, &old.base.id).await;
            assert_eq!(
                items.get("space", &old.base.id).await.unwrap(),
                Some(updated)
            );

            let old_run = runs.create(run_for(&old, 1, true)).await.unwrap();
            if archive_first {
                assert_eq!(
                    archive_terminal_work(&client, cutoff, 1)
                        .await
                        .unwrap()
                        .runs_archived,
                    1
                );
            }
            let mut next_run = old_run.clone();
            next_run.completed_at = None;
            let updated_run = runs
                .update("space", &old_run.base.id, next_run)
                .await
                .unwrap();
            if !archive_first {
                assert_eq!(
                    archive_terminal_work(&client, cutoff, 1)
                        .await
                        .unwrap()
                        .runs_archived,
                    0
                );
            }
            assert_hot_only(&client, &old_run.base.id).await;
            assert_eq!(
                runs.get("space", &old_run.base.id).await.unwrap(),
                Some(updated_run)
            );
        }
    }

    #[tokio::test]
    async fn retention_both_commit_orders_surreal() {
        both_commit_orders(StorageClient::new_memory().await.unwrap()).await;
    }

    #[tokio::test]
    async fn retention_both_commit_orders_embedded() {
        both_commit_orders(StorageClient::new_embedded().await.unwrap()).await;
    }

    #[tokio::test]
    async fn retention_legacy_string_runs_and_archived_done_dependencies() {
        let client = StorageClient::new_memory().await.unwrap();
        indexes(&client).await;
        let items = WorkItemRepository::new(client.clone());
        let runs = WorkRunRepository::new(client.clone());
        let parent = items
            .create(item(WorkItemStatus::Done, "legacy"))
            .await
            .unwrap();
        let completed = run_for(&parent, 1, true);
        let open = run_for(&parent, 2, false);
        // Reproduce the previous generic JSON serializer exactly.
        for run in [&completed, &open] {
            let mut value = serde_json::to_value(run).unwrap();
            value.as_object_mut().unwrap().remove("id");
            client
                .query_bind_checked(
                    "CREATE $id CONTENT $record",
                    crate::bindings! {
                        "id" => crate::surreal::thing_from_record_id(&run.base.id),
                        "record" => crate::surreal::json_to_surreal(value).unwrap()
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            runs.find_by_work_item(&parent.base.id).await.unwrap().len(),
            2
        );
        let moved = archive_terminal_work(&client, Utc::now() - chrono::Duration::days(30), 10)
            .await
            .unwrap();
        assert_eq!(moved.items_archived, 0);
        assert_eq!(moved.runs_archived, 1);
        assert_eq!(
            runs.get("space", &completed.base.id).await.unwrap(),
            Some(completed)
        );
        let mut closed = open.clone();
        closed.completed_at = Some(Utc::now());
        runs.update("space", &open.base.id, closed).await.unwrap();
        let moved = archive_terminal_work(&client, Utc::now() - chrono::Duration::days(30), 10)
            .await
            .unwrap();
        assert_eq!(moved.items_archived, 1);
        let child = items
            .create(item(WorkItemStatus::Pending, "child"))
            .await
            .unwrap();
        let edges = WorkEdgeRepository::new(client.clone());
        let edge = edges
            .create(WorkEdge {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkEdge),
                    "space".to_string(),
                    "test".to_string(),
                ),
                from: Link::new(parent.base.id.clone()),
                to: Link::new(child.base.id.clone()),
                edge_type: "depends_on".to_string(),
            })
            .await
            .unwrap();
        let mut next = child.clone();
        next.status = WorkItemStatus::Runnable;
        assert!(items
            .update_if_status_and_dependencies_done(
                "space",
                &child.base.id,
                next,
                WorkItemStatus::Pending
            )
            .await
            .unwrap()
            .is_some());
        // Missing parents remain a barrier, even when unrelated Done history exists.
        let other = items
            .create(item(WorkItemStatus::Pending, "other_child"))
            .await
            .unwrap();
        let mut missing_edge = edge;
        missing_edge.base.id = RecordId::new_v4(TableName::WorkEdge);
        missing_edge.from = Link::new(RecordId::new_v4(TableName::WorkItem));
        missing_edge.to = Link::new(other.base.id.clone());
        edges.create(missing_edge).await.unwrap();
        let mut next = other.clone();
        next.status = WorkItemStatus::Runnable;
        assert!(items
            .update_if_status_and_dependencies_done(
                "space",
                &other.base.id,
                next,
                WorkItemStatus::Pending
            )
            .await
            .unwrap()
            .is_none());
    }
}
