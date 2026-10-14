//! Durable, payload-free event sequence floors, separate from consumer acknowledgments.
//!
//! Call these helpers inside the transaction that writes or retires events.
//! The existing checkpoint table retains one reserved producer bookkeeping row
//! per space. It never acknowledges delivery on behalf of a real consumer.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use elephant_types::records::SystemEvent;
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};

const TABLE: &str = "outbox_checkpoint";

/// Private producer identity; public checkpoint operations must reject it.
pub const RESERVED_CONSUMER: &str = "__recall_event_floor_v1";

fn invalid(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

fn key(space: &str) -> String {
    crate::outbox_checkpoint_repository::key(space, RESERVED_CONSUMER)
}

fn validate_request(space: &str, additional_floor: i64, count: usize) -> StorageResult<i64> {
    if space.is_empty() || additional_floor < 0 {
        return Err(invalid("invalid event sequence allocation scope or floor"));
    }
    i64::try_from(count).map_err(|_| invalid("outbox sequence exhausted"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredFloor {
    id: String,
    space_id: String,
    consumer: String,
    seq: i64,
    #[serde(rename = "updated_at")]
    _updated_at: DateTime<Utc>,
}

/// Return the base sequence and retain the complete batch's end in the caller's
/// original transaction. Event creation must occur in that same transaction.
pub fn reserve_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    additional_floor: i64,
    count: usize,
) -> StorageResult<i64> {
    let count = validate_request(space, additional_floor, count)?;
    let id = format!("{TABLE}:{}", key(space));
    let mut floor = additional_floor;
    let mut found = false;
    for (physical_key, row) in tx.scan_extension_rows(TABLE)? {
        let addressed = physical_key == id
            || row.get("id").and_then(serde_json::Value::as_str) == Some(id.as_str());
        let scoped = row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
            && row.get("consumer").and_then(serde_json::Value::as_str) == Some(RESERVED_CONSUMER);
        if !addressed && !scoped {
            continue;
        }
        let row: StoredFloor = serde_json::from_value(row)
            .map_err(|error| invalid(&format!("invalid reserved event sequence floor: {error}")))?;
        if found
            || physical_key != id
            || row.id != id
            || row.space_id != space
            || row.consumer != RESERVED_CONSUMER
            || row.seq < 0
        {
            return Err(invalid("reserved event sequence floor identity mismatch"));
        }
        found = true;
        floor = floor.max(row.seq);
    }
    for event in tx.scan::<SystemEvent>()? {
        if event.base.space_id == space {
            if event.seq < 0 {
                return Err(invalid("invalid stored event sequence"));
            }
            floor = floor.max(event.seq);
        }
    }
    let end = floor
        .checked_add(count)
        .ok_or_else(|| invalid("outbox sequence exhausted"))?;
    tx.upsert_extension_row(
        TABLE,
        serde_json::json!({
            "id": id, "space_id": space, "consumer": RESERVED_CONSUMER,
            "seq": end, "updated_at": Utc::now(),
        }),
    )?;
    Ok(floor)
}

/// Preserve the original event tail before destructive or sequence-lowering writes.
pub fn preserve_tx(tx: &mut EmbeddedMutation<'_>, space: &str) -> StorageResult<()> {
    reserve_tx(tx, space, 0, 0).map(|_| ())
}

/// Append allocation to an existing native transaction, returning its base
/// sequence variable. The caller must create the events before committing.
pub fn append_reserve_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    prefix: &str,
    additional_floor: i64,
    count: usize,
) -> StorageResult<String> {
    let count = validate_request(space, additional_floor, count)?;
    if prefix.is_empty()
        || !prefix.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        })
    {
        return Err(invalid("invalid event sequence SQL prefix"));
    }
    let values = [
        (format!("{prefix}_space"), Value::from(space.to_owned())),
        (
            format!("{prefix}_consumer"),
            Value::from(RESERVED_CONSUMER.to_owned()),
        ),
        (
            format!("{prefix}_id"),
            Value::from(surrealdb::sql::Thing::from((TABLE, key(space).as_str()))),
        ),
        (
            format!("{prefix}_additional"),
            Value::from(additional_floor),
        ),
        (format!("{prefix}_count"), Value::from(count)),
    ];
    for (name, value) in values {
        if bindings.insert(name, value).is_some() {
            return Err(invalid("duplicate event sequence SQL bindings"));
        }
    }
    writeln!(sql,
        "LET ${prefix}_prior = SELECT * FROM ONLY ${prefix}_id;\n\
         LET ${prefix}_owners = SELECT id FROM outbox_checkpoint WHERE space_id = ${prefix}_space AND consumer = ${prefix}_consumer;\n\
         IF array::len(${prefix}_owners) > 1 OR (array::len(${prefix}_owners) = 1 AND ${prefix}_owners[0].id != ${prefix}_id) {{ THROW 'event_sequence:identity_mismatch'; }};\n\
         IF ${prefix}_prior != NONE AND (${prefix}_prior.space_id != ${prefix}_space OR ${prefix}_prior.consumer != ${prefix}_consumer OR !type::is::int(${prefix}_prior.seq) OR ${prefix}_prior.seq < 0 OR !type::is::datetime(${prefix}_prior.updated_at) OR array::len(object::keys(${prefix}_prior)) != 5) {{ THROW 'event_sequence:invalid_floor'; }};\n\
         LET ${prefix}_retained = IF ${prefix}_prior = NONE THEN 0 ELSE ${prefix}_prior.seq END;\n\
         LET ${prefix}_invalid_live = SELECT id FROM system_event WHERE space_id = ${prefix}_space AND (!type::is::int(seq) OR seq < 0) LIMIT 1;\n\
         IF array::len(${prefix}_invalid_live) != 0 {{ THROW 'event_sequence:invalid_live_sequence'; }};\n\
         LET ${prefix}_tail = SELECT seq FROM system_event WHERE space_id = ${prefix}_space ORDER BY seq DESC LIMIT 1;\n\
         LET ${prefix}_live = IF array::len(${prefix}_tail) = 0 THEN 0 ELSE ${prefix}_tail[0].seq END;\n\
         IF !type::is::int(${prefix}_live) OR ${prefix}_live < 0 {{ THROW 'event_sequence:invalid_live_sequence'; }};\n\
         LET ${prefix}_base = math::max([${prefix}_retained, ${prefix}_live, ${prefix}_additional]);\n\
         IF ${prefix}_base > 9223372036854775807 - ${prefix}_count {{ THROW 'event_sequence:exhausted'; }};\n\
         UPSERT ${prefix}_id SET space_id = ${prefix}_space, consumer = ${prefix}_consumer, seq = ${prefix}_base + ${prefix}_count, updated_at = time::now();"
    ).map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    Ok(format!("${prefix}_base"))
}

/// Append floor preservation before event deletion in an existing transaction.
pub fn append_preserve_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    prefix: &str,
) -> StorageResult<()> {
    append_reserve_sql(sql, bindings, space, prefix, 0, 0).map(|_| ())
}

/// Allocate and create one caller-authored event atomically.
///
/// This trusted primitive adds no source publication authority or registered-space policy.
/// The caller owns any retry policy; an existing ID is not a replay certificate.
pub async fn append_system_event(
    client: &StorageClient,
    mut event: SystemEvent,
) -> StorageResult<SystemEvent> {
    #[derive(Deserialize)]
    struct AssignedSequence {
        seq: i64,
    }

    if event.base.id.table() != elephant_types::TableName::SystemEvent {
        return Err(invalid(
            "event sequence allocation requires a system event ID",
        ));
    }
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                event.seq = reserve_tx(tx, &event.base.space_id, 0, 1)?
                    .checked_add(1)
                    .ok_or_else(|| invalid("outbox sequence exhausted"))?;
                tx.create(event.clone())?;
                Ok(event)
            })
            .await;
    }
    let mut bindings = crate::bindings! {
        "append_event_id" => crate::surreal::thing_from_record_id(&event.base.id),
        "append_event_record" => crate::SystemEventRepository::system_event_to_value(&event)?,
    };
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let base = append_reserve_sql(
        &mut sql,
        &mut bindings,
        &event.base.space_id,
        "append_event_floor",
        0,
        1,
    )?;
    writeln!(sql,
        "CREATE $append_event_id CONTENT object::from_entries(array::concat(object::entries($append_event_record), [['seq', {base} + 1]]));\nRETURN {{ seq: {base} + 1 }};\nCOMMIT TRANSACTION;"
    ).map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    let mut response = client.query_bind_checked(&sql, bindings).await?;
    let result_index = response
        .num_statements()
        .checked_sub(1)
        .ok_or_else(|| invalid("missing assigned event sequence"))?;
    let assigned: Option<AssignedSequence> = response
        .take(result_index)
        .map_err(|error| invalid(&format!("invalid assigned event sequence: {error}")))?;
    event.seq = assigned
        .filter(|row| row.seq > 0)
        .ok_or_else(|| invalid("missing positive assigned event sequence"))?
        .seq;
    Ok(event)
}

/// Retire old events while retaining each affected space's sequence floor in
/// the same transaction as its deletions.
///
/// A concurrently introduced space can be handled by the next retention pass;
/// no unobserved space is deleted here.
pub async fn retire_before(client: &StorageClient, cutoff: DateTime<Utc>) -> StorageResult<usize> {
    #[derive(Deserialize)]
    struct EventSpace {
        space_id: String,
    }

    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let events = tx
                    .scan::<SystemEvent>()?
                    .into_iter()
                    .filter(|event| event.base.created_at < cutoff)
                    .collect::<Vec<_>>();
                let spaces = events
                    .iter()
                    .map(|event| event.base.space_id.clone())
                    .collect::<BTreeSet<_>>();
                for space in spaces {
                    preserve_tx(tx, &space)?;
                }
                for event in &events {
                    tx.hard_delete::<SystemEvent>(&event.base.space_id, &event.base.id)?;
                }
                Ok(events.len())
            })
            .await;
    }
    let mut response = client
        .query_bind_checked(
            "SELECT space_id FROM system_event WHERE created_at < $cutoff GROUP BY space_id",
            crate::bindings! { "cutoff" => crate::surreal::datetime_from_chrono(cutoff) },
        )
        .await?;
    let spaces: Vec<EventSpace> = response
        .take(0)
        .map_err(|error| invalid(&format!("invalid event retention space: {error}")))?;
    let mut count = 0usize;
    let mut unique_spaces = BTreeSet::new();
    unique_spaces.extend(spaces.into_iter().map(|row| row.space_id));
    for space in unique_spaces {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = crate::bindings! {
            "retention_space" => space.clone(),
            "retention_cutoff" => crate::surreal::datetime_from_chrono(cutoff),
        };
        append_preserve_sql(&mut sql, &mut bindings, &space, "retention_floor")?;
        sql.push_str("LET $retired_events = DELETE system_event WHERE space_id = $retention_space AND created_at < $retention_cutoff RETURN BEFORE;\nRETURN array::len($retired_events);\nCOMMIT TRANSACTION;\n");
        let mut response = client.query_bind_checked(&sql, bindings).await?;
        let result_index = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(|| invalid("missing event retention result"))?;
        let deleted: Option<usize> = response
            .take(result_index)
            .map_err(|error| invalid(&format!("invalid event retention result: {error}")))?;
        count = count
            .checked_add(deleted.ok_or_else(|| invalid("missing event retention count"))?)
            .ok_or_else(|| invalid("event retention count overflow"))?;
    }
    Ok(count)
}

#[cfg(test)]
#[path = "event_sequence_tests.rs"]
mod tests;
